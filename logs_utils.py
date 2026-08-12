from collections import defaultdict
from datetime import timedelta, datetime
from dateutil.parser import parse
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from shift_manager import get_shift_times_short
from config import BEIJING_TZ, LOGS_PER_PAGE


def _as_date(value):
    """把 datetime 或 date 统一转换成 date 对象"""
    if value is None:
        return None
    return value.date() if hasattr(value, "date") else value


def _time_to_minutes(t) -> int:
    return t.hour * 60 + t.minute


def _in_shift_window(ts_time, start_time, end_time, margin: int = 30) -> bool:
    """打卡时间是否落在【班次开始前margin分钟，班次结束后margin分钟】这个窗口内（环形计算，兼容跨天班次）"""
    t = _time_to_minutes(ts_time)
    s = (_time_to_minutes(start_time) - margin) % 1440
    e = (_time_to_minutes(end_time) + margin) % 1440
    if s <= e:
        return s <= t <= e
    else:
        return t >= s or t <= e


def _compute_missing_days(period_start, period_end, daily_map):
    """
    计算 [period_start, period_end) 区间内、daily_map 里完全没有记录的天数。
    period_end 是“下一周期第一天”（不含），所以实际最后一天是 period_end - 1 天。
    今天（以及今天之后）不算“缺勤”，因为今天可能还没到打卡时间。
    """
    start_date = _as_date(period_start)
    end_date = _as_date(period_end)
    if not start_date or not end_date:
        return []

    today = datetime.now(BEIJING_TZ).date()
    last_day = min(end_date - timedelta(days=1), today - timedelta(days=1))

    missing = []
    d = start_date
    while d <= last_day:
        if d not in daily_map:
            missing.append(d)
        d += timedelta(days=1)
    return missing


# ===========================
# 通用日志构建函数
# ===========================
async def build_and_send_logs(update, context, logs, target_name, key="mylogs", period_start=None, period_end=None):
    if not logs:
        reply = f"📭 {target_name} 暂无记录。"
        missing_days = _compute_missing_days(period_start, period_end, {})
        if missing_days:
            missing_str = "，".join(d.strftime("%d日") for d in missing_days)
            reply += f"\n\n🟡 休息/缺勤：{missing_str}"
        await update.message.reply_text(reply)
        return

    # 转换时区 & 排序
    logs = [(parse(ts) if isinstance(ts, str) else ts, kw, shift) for ts, kw, shift in logs]
    logs = [(ts.astimezone(BEIJING_TZ), kw, shift) for ts, kw, shift in logs]
    logs = sorted(logs, key=lambda x: x[0])

    # 只保留真正的打卡类记录（#上班打卡 / #下班打卡），
    # 过滤掉 #取消打卡 等其他审计类关键词，避免被误判为下班打卡
    logs = [(ts, kw, shift) for ts, kw, shift in logs if kw in ("#上班打卡", "#下班打卡")]

    if not logs:
        reply = f"📭 {target_name} 暂无记录。"
        missing_days = _compute_missing_days(period_start, period_end, {})
        if missing_days:
            missing_str = "，".join(d.strftime("%d日") for d in missing_days)
            reply += f"\n\n🟡 休息/缺勤：{missing_str}"
        await update.message.reply_text(reply)
        return

    # 按天组合
    daily_map = defaultdict(dict)
    i = 0
    while i < len(logs):
        ts, kw, shift = logs[i]
        date_key = ts.date()

        # 下班卡凌晨算前一天
        if kw == "#下班打卡" and ts.hour < 6:
            date_key = (ts - timedelta(days=1)).date()

        # 补卡算当天
        if shift and "（补卡）" in shift:
            date_key = ts.date()

        if kw == "#上班打卡":
            daily_map[date_key]["shift"] = shift
            daily_map[date_key]["#上班打卡"] = ts
            if shift and "（补卡）" in shift:
                daily_map[date_key]["补卡标记"] = True

            # 找可能匹配的下班卡
            j = i + 1
            while j < len(logs):
                ts2, kw2, _ = logs[j]
                if kw2 == "#下班打卡" and timedelta(0) < (ts2 - ts) <= timedelta(hours=12):
                    daily_map[date_key]["#下班打卡"] = ts2
                    break
                j += 1
            i += 1
        else:  # 下班打卡
            daily_map[date_key]["#下班打卡"] = ts
            if "shift" not in daily_map[date_key]:
                daily_map[date_key]["shift"] = shift or "未选择班次"
            i += 1

    # 过滤掉不属于本统计周期的日期：
    # I班等跨天班次的下班卡会被归到“前一天”，如果这一天本身早于查询周期的
    # 起始日期，说明它对应的上班卡不在本周期内，属于上一个周期的“漏网记录”，
    # 应当剔除，不能算作本周期缺卡/异常。
    period_start_date = _as_date(period_start)
    period_end_date = _as_date(period_end)
    if period_start_date and period_end_date:
        for day in list(daily_map.keys()):
            if day < period_start_date or day >= period_end_date:
                del daily_map[day]

    all_days = sorted(daily_map.keys())

    # ===========================
    # 统计（补卡合并到异常）
    # ===========================
    total_complete = total_abnormal = 0
    for day in all_days:
        kw_map = daily_map[day]
        shift_full = str(kw_map.get("shift") or "未选择班次")
        is_makeup = shift_full.endswith("（补卡）") or "补卡标记" in kw_map
        shift_name = shift_full.split("（")[0]

        has_up = "#上班打卡" in kw_map
        has_down = "#下班打卡" in kw_map

        if is_makeup:
            total_abnormal += 1
            continue

        if has_up:
            if shift_name in get_shift_times_short():
                start_time, _ = get_shift_times_short()[shift_name]
                if kw_map["#上班打卡"].time() > start_time:
                    total_abnormal += 1
                else:
                    total_complete += 1
            else:
                total_complete += 1
        else:
            if not has_down:
                total_abnormal += 1

        if has_down:
            if shift_name in get_shift_times_short():
                _, end_time = get_shift_times_short()[shift_name]
                down_ts = kw_map["#下班打卡"]
                if shift_name == "I班" and down_ts.date() == day:
                    total_abnormal += 1
                elif shift_name != "I班" and down_ts.time() < end_time:
                    total_abnormal += 1
                else:
                    total_complete += 1
            else:
                total_complete += 1
        else:
            if has_up:
                total_abnormal += 1

        # 签到异常：打卡时间不在【班次开始前30分钟，班次结束后30分钟】窗口内（与迟到/早退独立计数）
        if shift_name in get_shift_times_short():
            start_time, end_time = get_shift_times_short()[shift_name]
            if has_up and not _in_shift_window(kw_map["#上班打卡"].time(), start_time, end_time):
                total_abnormal += 1
            if has_down and not _in_shift_window(kw_map["#下班打卡"].time(), start_time, end_time):
                total_abnormal += 1

    # ===========================
    # 分页
    # ===========================
    pages = [all_days[i:i + LOGS_PER_PAGE] for i in range(0, len(all_days), LOGS_PER_PAGE)]

    # 默认定位到“今天”所在的那一页；如果今天还没有记录，
    # 就定位到不晚于今天的最近一天所在的页（比如查上月记录时，就是最近的那一天）
    today = datetime.now(BEIJING_TZ).date()
    default_page_index = 0
    idx_today = None
    for i, d in enumerate(all_days):
        if d <= today:
            idx_today = i
        else:
            break
    if idx_today is not None:
        default_page_index = idx_today // LOGS_PER_PAGE

    missing_days = _compute_missing_days(period_start, period_end, daily_map)

    context.user_data[f"{key}_pages"] = {
        "pages": pages,
        "daily_map": daily_map,
        "page_index": default_page_index,
        "summary": (total_complete, total_abnormal),
        "target_name": target_name,
        "missing_days": missing_days,
    }

    await send_logs_page(update, context, key)


# ===========================
# 通用发送分页内容（带秒）
# ===========================
async def send_logs_page(update, context, key="mylogs"):
    data = context.user_data.get(f"{key}_pages")
    if not data:
        msg = "⚠️ 会话已过期，请重新使用 /mylogs" if key == "mylogs" else "⚠️ 会话已过期，请重新使用 /userlogs"
        if update.callback_query:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    pages, daily_map, page_index = data["pages"], data["daily_map"], data["page_index"]
    _, total_abnormal = data["summary"]
    target_name = data.get("target_name", "本月打卡")
    missing_days = data.get("missing_days", [])

    current_page_days = pages[page_index]

    if key == "mylogs":
        reply = f"🗓️ 本月打卡情况（第 {page_index + 1}/{len(pages)} 页）：\n\n"
    elif key == "lastmonth":
        reply = f"🗓️ 上月打卡情况（第 {page_index + 1}/{len(pages)} 页）：\n\n"
    elif key == "userlogs_lastmonth":
        reply = f"🗓️ {target_name} 上月打卡记录（第 {page_index + 1}/{len(pages)} 页）：\n\n"
    else:
        reply = f"🗓️ {target_name} 本月打卡记录（第 {page_index + 1}/{len(pages)} 页）：\n\n"

    for idx, day in enumerate(current_page_days, start=1 + page_index * LOGS_PER_PAGE):
        kw_map = daily_map[day]
        shift_full = str(kw_map.get("shift") or "未选择班次")
        is_makeup = shift_full.endswith("（补卡）") or "补卡标记" in kw_map
        shift_name = shift_full.split("（")[0]

        has_up = "#上班打卡" in kw_map
        has_down = "#下班打卡" in kw_map

        has_late = has_early = False
        checkin_abnormal = checkout_abnormal = False
        if has_up and shift_name in get_shift_times_short():
            start_time, end_time = get_shift_times_short()[shift_name]
            if kw_map["#上班打卡"].time() > start_time:
                has_late = True
            if not _in_shift_window(kw_map["#上班打卡"].time(), start_time, end_time):
                checkin_abnormal = True
        if has_down and shift_name in get_shift_times_short():
            start_time, end_time = get_shift_times_short()[shift_name]
            down_ts = kw_map["#下班打卡"]
            if shift_name == "I班" and down_ts.date() == day:
                has_early = True
            elif shift_name != "I班" and down_ts.time() < end_time:
                has_early = True
            if not _in_shift_window(down_ts.time(), start_time, end_time):
                checkout_abnormal = True

        weekday_map = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        weekday_str = weekday_map[day.weekday()]

        reply += f"{idx}. {day.strftime('%m月%d日')} - {weekday_str} - {shift_name}\n"

        if has_up:
            reply += f"   └─ #上班打卡：{kw_map['#上班打卡'].strftime('%H:%M:%S')}"
            if is_makeup:
                reply += " - 补卡 🔴"
            if has_late:
                reply += " - 迟到 🔴"
            if checkin_abnormal:
                reply += " - 签到异常 🔴"
            reply += "\n"
        else:
            reply += "   └─ #上班打卡： - 缺卡 🔴\n"

        if has_down:
            down_ts = kw_map["#下班打卡"]
            next_day = down_ts.date() > day
            reply += f"   └─ #下班打卡：{down_ts.strftime('%H:%M:%S')}{'（次日）' if next_day else ''}"
            if has_early:
                reply += " - 早退 🔴"
            if checkout_abnormal:
                reply += " - 签到异常 🔴"
            reply += "\n"
        else:
            reply += "   └─ #下班打卡： - 缺卡 🔴\n"

    # ✅ 仅显示异常次数，不再显示正常次数
    reply += f"\n🔴 考勤异常（迟到/缺卡/补卡/签到异常）：{total_abnormal} 次"

    # 🟡 完全没有打卡记录的天（休息/缺勤）
    if missing_days:
        missing_str = "，".join(d.strftime("%d日") for d in missing_days)
        reply += f"\n🟡 休息/缺勤：{missing_str}"

    # 分页按钮
    buttons = []
    if page_index > 0:
        buttons.append(InlineKeyboardButton("⬅ 上一页", callback_data=f"{key}_prev"))
    if page_index < len(pages) - 1:
        buttons.append(InlineKeyboardButton("➡ 下一页", callback_data=f"{key}_next"))

    rows = [buttons] if buttons else []

    # 仅用户自己的记录显示返回按钮
    if key in ("mylogs", "lastmonth"):
        rows.append([InlineKeyboardButton("🔙 返回", callback_data="back_to_menu")])

    markup = InlineKeyboardMarkup(rows) if rows else None

    if update.callback_query:
        await update.callback_query.edit_message_text(reply, reply_markup=markup)
    else:
        await update.message.reply_text(reply, reply_markup=markup)
