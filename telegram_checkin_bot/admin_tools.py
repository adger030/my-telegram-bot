from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from sqlalchemy import text
import cloudinary.api
import os
from datetime import datetime, timedelta
from collections import defaultdict
from dateutil.parser import parse
from db_pg import engine, get_user_logs, get_user_logs_by_name
from config import ADMIN_IDS, BEIJING_TZ, SHIFT_TIMES, LOGS_PER_PAGE

# 提取 Cloudinary public_id
def extract_cloudinary_public_id(url: str) -> str | None:
    """
    提取 Cloudinary public_id，支持多级目录。
    e.g. https://res.cloudinary.com/demo/image/upload/v123456/folder/image.jpg
         -> folder/image
    """
    if "cloudinary.com" not in url:
        return None
    try:
        # 去掉 query 参数
        url = url.split("?")[0]
        parts = url.split("/upload/")
        if len(parts) < 2:
            return None
        path = parts[1]
        # 去掉版本号 vXXXX
        path_parts = path.split("/")
        if path_parts[0].startswith("v") and path_parts[0][1:].isdigit():
            path_parts = path_parts[1:]
        public_id_with_ext = "/".join(path_parts)
        public_id = os.path.splitext(public_id_with_ext)[0]
        return public_id
    except Exception as e:
        print(f"⚠️ public_id 提取失败: {url} -> {e}")
        return None

# 批量删除 Cloudinary
def batch_delete_cloudinary(public_ids: list, batch_size=100):
    deleted_total = 0
    for i in range(0, len(public_ids), batch_size):
        batch = public_ids[i:i + batch_size]
        try:
            response = cloudinary.api.delete_resources(batch)
            deleted = response.get("deleted", {})
            failed = response.get("failed", {})

            deleted_total += sum(1 for v in deleted.values() if v == "deleted")

            for pid, error in failed.items():
                print(f"⚠️ 删除失败: {pid} - {error}")
        except Exception as e:
            print(f"❌ 批量删除失败: {e}")
    return deleted_total

# 管理员删除命令
async def delete_range_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ 无权限！仅管理员可执行此命令。")
        return

    args = context.args
    if len(args) not in (2, 3):
        await update.message.reply_text("⚠️ 用法：/delete_range YYYY-MM-DD YYYY-MM-DD [confirm]")
        return

    start_date, end_date = args[0], args[1]
    confirm = len(args) == 3 and args[2].lower() == "confirm"

    # 查询记录
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                SELECT id, content FROM messages
                WHERE timestamp >= :start_date AND timestamp <= :end_date
            """),
            {"start_date": f"{start_date} 00:00:00", "end_date": f"{end_date} 23:59:59"}
        )
        rows = result.fetchall()

    total_count = len(rows)
    image_urls = [r[1] for r in rows if r[1] and "cloudinary.com" in r[1]]
    public_ids = [extract_cloudinary_public_id(url) for url in image_urls if extract_cloudinary_public_id(url)]

    if not confirm:
        await update.message.reply_text(
            f"🔍 预览删除范围：{start_date} 至 {end_date}\n"
            f"📄 共 {total_count} 条记录，其中 {len(public_ids)} 张图片。\n\n"
            f"要确认删除，请使用：\n`/delete_range {start_date} {end_date} confirm`",
            parse_mode="Markdown"
        )
        return

    # 删除 Cloudinary 图片
    deleted_images = batch_delete_cloudinary(public_ids)

    # 删除数据库记录
    with engine.begin() as conn:
        delete_result = conn.execute(
            text("""
                DELETE FROM messages
                WHERE timestamp >= :start_date AND timestamp <= :end_date
                RETURNING id
            """),
            {"start_date": f"{start_date} 00:00:00", "end_date": f"{end_date} 23:59:59"}
        )
        deleted_count = len(delete_result.fetchall())

    await update.message.reply_text(
        f"✅ 删除完成！\n\n"
        f"📄 数据库记录：{deleted_count}/{total_count} 条\n"
        f"🖼 Cloudinary 图片：{deleted_images}/{len(public_ids)} 张\n"
        f"📅 范围：{start_date} ~ {end_date}"
    )


async def userlogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ 无权限，仅管理员可查看他人记录。")
        return

    if not context.args:
        await update.message.reply_text("⚠️ 用法：/userlogs @用户名 或 /userlogs 中文姓名")
        return

    # 1️⃣ 解析查询对象
    raw_input = context.args[0]
    is_username = raw_input.startswith("@")
    target_key = raw_input.lstrip("@") if is_username else raw_input

    # 2️⃣ 计算本月时间范围
    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start + timedelta(days=32)).replace(day=1)

    # 3️⃣ 获取记录
    if is_username:
        logs = get_user_logs(target_key, start, end)
    else:
        logs = get_user_logs_by_name(target_key, start, end)

    if not logs:
        await update.message.reply_text(f"📭 用户 {target_key} 本月暂无打卡记录。")
        return

    # 4️⃣ 转换时区 & 排序
    logs = [(parse(ts) if isinstance(ts, str) else ts, kw, shift) for ts, kw, shift in logs]
    logs = [(ts.astimezone(BEIJING_TZ), kw, shift) for ts, kw, shift in logs]
    logs = sorted(logs, key=lambda x: x[0])

    # 5️⃣ 按天组合上下班打卡
    daily_map = defaultdict(dict)
    i = 0
    while i < len(logs):
        ts, kw, shift = logs[i]
        date_key = ts.date()
        if kw == "#下班打卡" and ts.hour < 6:
            date_key = (ts - timedelta(days=1)).date()

        if kw == "#上班打卡":
            daily_map[date_key]["shift"] = shift
            daily_map[date_key]["#上班打卡"] = ts
            j = i + 1
            while j < len(logs):
                ts2, kw2, _ = logs[j]
                if kw2 == "#下班打卡" and timedelta(0) < (ts2 - ts) <= timedelta(hours=12):
                    daily_map[date_key]["#下班打卡"] = ts2
                    break
                j += 1
            i = j if j > i else i + 1
        else:
            daily_map[date_key]["#下班打卡"] = ts
            i += 1

    # 6️⃣ 统计
    total_complete = total_abnormal = total_makeup = 0
    for day, kw_map in daily_map.items():
        shift_full = kw_map.get("shift", "未选择班次")
        is_makeup = shift_full.endswith("（补卡）")
        shift_name = shift_full.split("（")[0]
        has_up = "#上班打卡" in kw_map
        has_down = "#下班打卡" in kw_map
        has_late = has_early = False

        if is_makeup:
            total_makeup += 1

        if has_up and shift_name in SHIFT_TIMES:
            start_time, _ = SHIFT_TIMES[shift_name]
            if kw_map["#上班打卡"].time() > start_time:
                has_late = True

        if has_down and shift_name in SHIFT_TIMES:
            _, end_time = SHIFT_TIMES[shift_name]
            down_ts = kw_map["#下班打卡"]
            if shift_name == "I班" and down_ts.date() == day:
                has_early = True
            elif shift_name != "I班" and down_ts.time() < end_time:
                has_early = True

        if is_makeup:
            continue
        if has_late:
            total_abnormal += 1
        if has_early:
            total_abnormal += 1
        if not has_late and not has_early and (has_up or has_down):
            total_complete += 2 if has_up and has_down else 1

    # 7️⃣ 分页
    all_days = sorted(daily_map)
    pages = [all_days[i:i + LOGS_PER_PAGE] for i in range(0, len(all_days), LOGS_PER_PAGE)]
    context.user_data["userlogs_pages"] = {
        "pages": pages,
        "daily_map": daily_map,
        "page_index": 0,
        "summary": (total_complete, total_abnormal, total_makeup),
        "target_username": target_key,  # 无论是 username 还是 name，都记录
        "is_username": is_username      # 记录查询方式
    }

    await send_userlogs_page(update, context)  # 展示第一页


# ===========================
# 发送分页内容
# ===========================
async def send_userlogs_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data["userlogs_pages"]
    pages, daily_map, page_index = data["pages"], data["daily_map"], data["page_index"]
    total_complete, total_abnormal, total_makeup = data["summary"]
    target_username = data["target_username"]

    current_page_days = pages[page_index]
    reply = f"🗓️ {target_username} 本月打卡记录（第 {page_index+1}/{len(pages)} 页）：\n\n"

    for idx, day in enumerate(current_page_days, start=1 + page_index * LOGS_PER_PAGE):
        kw_map = daily_map[day]
        shift_full = kw_map.get("shift", "未选择班次")
        is_makeup = shift_full.endswith("（补卡）")
        shift_name = shift_full.split("（")[0]
        has_up = "#上班打卡" in kw_map
        has_down = "#下班打卡" in kw_map
        has_late = has_early = False

        if has_up and shift_name in SHIFT_TIMES:
            start_time, _ = SHIFT_TIMES[shift_name]
            if kw_map["#上班打卡"].time() > start_time:
                has_late = True

        if has_down and shift_name in SHIFT_TIMES:
            _, end_time = SHIFT_TIMES[shift_name]
            down_ts = kw_map["#下班打卡"]
            if shift_name == "I班" and down_ts.date() == day:
                has_early = True
            elif shift_name != "I班" and down_ts.time() < end_time:
                has_early = True

        reply += f"{idx}. {day.strftime('%m月%d日')} - {shift_name}\n"
        if has_up:
            reply += f"   └─ #上班打卡：{kw_map['#上班打卡'].strftime('%H:%M')}{'（补卡）' if is_makeup else ''}{'（迟到）' if has_late else ''}\n"
        if has_down:
            down_ts = kw_map["#下班打卡"]
            next_day = down_ts.date() > day
            reply += f"   └─ #下班打卡：{down_ts.strftime('%H:%M')}{'（次日）' if next_day else ''}{'（早退）' if has_early else ''}\n"

    reply += (
        f"\n🟢 正常：{total_complete} 次\n"
        f"🔴 异常（迟到/早退）：{total_abnormal} 次\n"
        f"🟡 补卡：{total_makeup} 次"
    )

    # 分页按钮
    buttons = []
    if page_index > 0:
        buttons.append(InlineKeyboardButton("⬅ 上一页", callback_data="userlogs_prev"))
    if page_index < len(pages) - 1:
        buttons.append(InlineKeyboardButton("➡ 下一页", callback_data="userlogs_next"))
    markup = InlineKeyboardMarkup([buttons]) if buttons else None

    if update.callback_query:
        await update.callback_query.edit_message_text(reply, reply_markup=markup)
    else:
        await update.message.reply_text(reply, reply_markup=markup)

# ===========================
# 分页按钮回调
# ===========================
async def userlogs_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if "userlogs_pages" not in context.user_data:
        await query.edit_message_text("⚠️ 会话已过期，请重新使用 /userlogs")
        return

    if query.data == "userlogs_prev":
        context.user_data["userlogs_pages"]["page_index"] -= 1
    elif query.data == "userlogs_next":
        context.user_data["userlogs_pages"]["page_index"] += 1

    await send_userlogs_page(update, context)
