from collections import defaultdict
from datetime import timedelta
from dateutil.parser import parse
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from shift_manager import get_shift_times_short
from config import BEIJING_TZ, LOGS_PER_PAGE


# ===========================
# 通用日志构建函数
# ===========================
async def build_and_send_logs(update, context, logs, target_name, key="mylogs"):
    if not logs:
        await update.message.reply_text(f"📭 {target_name} 暂无记录。")
        return

    # 转换时区 & 排序
    logs = [(parse(ts) if isinstance(ts, str) else ts, kw, shift) for ts, kw, shift in logs]
    logs = [(ts.astimezone(BEIJING_TZ), kw, shift) for ts, kw, shift in logs]
    logs = sorted(logs, key=lambda x: x[0])

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

    # ===========================
    # 分页
    # ===========================
    pages = [all_days[i:i + LOGS_PER_PAGE] for i in range(0, len(all_days), LOGS_PER_PAGE)]
    context.user_data[f"{key}_pages"] = {
        "pages": pages,
        "daily_map": daily_map,
        "page_index": 0,
        "summary": (total_complete, total_abnormal),
        "target_name": target_name
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
        if has_up and shift_name in get_shift_times_short():
            start_time, _ = get_shift_times_short()[shift_name]
            if kw_map["#上班打卡"].time() > start_time:
                has_late = True
        if has_down and shift_name in get_shift_times_short():
            _, end_time = get_shift_times_short()[shift_name]
            down_ts = kw_map["#下班打卡"]
            if shift_name == "I班" and down_ts.date() == day:
                has_early = True
            elif shift_name != "I班" and down_ts.time() < end_time:
                has_early = True

        weekday_map = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        weekday_str = weekday_map[day.weekday()]

        reply += f"{idx}. {day.strftime('%m月%d日')} - {weekday_str} - {shift_name}\n"

        if has_up:
            reply += f"   └─ #上班打卡：{kw_map['#上班打卡'].strftime('%H:%M:%S')}"
            if is_makeup:
                reply += " - 补卡 🔴"
            if has_late:
                reply += " - 迟到 🔴"
            reply += "\n"
        else:
            reply += "   └─ #上班打卡： - 缺卡 🔴\n"

        if has_down:
            down_ts = kw_map["#下班打卡"]
            next_day = down_ts.date() > day
            reply += f"   └─ #下班打卡：{down_ts.strftime('%H:%M:%S')}{'（次日）' if next_day else ''}"
            if has_early:
                reply += " - 早退 🔴"
            reply += "\n"
        else:
            reply += "   └─ #下班打卡： - 缺卡 🔴\n"

    # ✅ 仅显示异常次数，不再显示正常次数
    reply += f"\n🔴 异常（迟到/缺卡/补卡）：{total_abnormal} 次"

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
