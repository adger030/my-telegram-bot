import os
import sys
import asyncio
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dateutil.parser import parse

from config import TOKEN, KEYWORDS, ADMIN_IDS, DATA_DIR
from db_pg import init_db, save_message, get_user_logs, save_shift, get_user_name, set_user_name, get_db, sync_username
from export import export_excel, export_images
from upload_image import upload_image
from cleaner import delete_last_month_data
import shutil
import logging

# 北京时区
BEIJING_TZ = timezone(timedelta(hours=8))
WAITING_NAME = {}  # 记录等待输入姓名的用户，key 改为 user_id

SHIFT_OPTIONS = {
    "F": "F班（12:00-21:00）",
    "G": "G班（13:00-22:00）",
    "H": "H班（14:00-23:00）",
    "I": "I班（15:00-00:00）"
}

SHIFT_TIMES = {
    "F班": (datetime.strptime("12:00", "%H:%M").time(), datetime.strptime("21:00", "%H:%M").time()),
    "G班": (datetime.strptime("13:00", "%H:%M").time(), datetime.strptime("22:00", "%H:%M").time()),
    "H班": (datetime.strptime("14:00", "%H:%M").time(), datetime.strptime("23:00", "%H:%M").time()),
    "I班": (datetime.strptime("15:00", "%H:%M").time(), datetime.strptime("00:00", "%H:%M").time()),  # I班跨天
}

def extract_keyword(text: str):
    text = text.strip().replace(" ", "")
    for kw in KEYWORDS:
        if kw in text:
            return kw
    return None

def has_user_checked_keyword_today_fixed(user_id, keyword):
    now = datetime.now(BEIJING_TZ)
    if keyword == "#下班打卡" and now.hour < 6:
        ref_day = now - timedelta(days=1)
    else:
        ref_day = now
    start = ref_day.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT timestamp FROM messages
            WHERE user_id=%s AND keyword=%s
            AND timestamp >= %s AND timestamp < %s
            ORDER BY timestamp DESC
        """, (user_id, keyword, start, end))
        rows = cur.fetchall()
    for (ts,) in rows:
        ts_local = ts.astimezone(BEIJING_TZ)
        if keyword == "#下班打卡" and ts_local.hour < 6:
            continue
        return True
    return False

async def send_welcome(update_or_msg, name):
    welcome_text = (
        f"您好，{name}！\n\n"
        "📌 使用说明：\n"
        "1️⃣ 向机器人发送“#上班打卡”或“#下班打卡”并附带IP截图；\n"
        "2️⃣ 上班打卡需要选择你的班次，即可打卡成功；\n"
        "3️⃣ 若忘记上班打卡，请发送“#补卡”并附带IP截图，补卡完成才能打下班卡；\n\n"
        "IP截图必须包含以下信息\n"
        "① 设备编码：本机序列号\n"
        "② 实时IP：指定网站内显示的IP\n"
        "③ 本地时间：电脑任务栏时间（需含月、日、时、分）\n\n"
        "<a href='https://www.ipaddress.my'>点击这里查看你的IP地址</a>\n\n"
        "举个🌰，如下👇"
    )
    await update_or_msg.reply_text(welcome_text, parse_mode="HTML")
    await asyncio.sleep(1)
    await update_or_msg.reply_photo(
        photo="https://i.postimg.cc/3xRMBbT4/photo-2025-07-28-15-55-19.jpg",
        caption="#上班打卡"
    )

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    user_id = tg_user.id
    username = tg_user.username or f"user{user_id}"

    # 确保数据库迁移已完成
    from db_pg import init_db
    init_db()  # ✅ 每次启动时自动检查 user_id 主键

    # 安全调用 sync_username，避免因数据库约束异常崩溃
    try:
        sync_username(user_id, username)
    except Exception as e:
        import logging
        logging.error(f"⚠️ sync_username 失败: {e}")
        # 尝试补救：重新迁移数据库并重试
        init_db()
        sync_username(user_id, username)

    # 首次使用时要求输入姓名
    if not get_user_name(user_id):
        WAITING_NAME[user_id] = True
        await update.message.reply_text("👤 第一次打卡前请输入你的工作名：")
        return

    name = get_user_name(user_id)
    await send_welcome(update.message, name)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    tg_user = msg.from_user
    user_id = tg_user.id
    username = tg_user.username or f"user{user_id}"
    sync_username(user_id, username)  # ✅ 同步用户名
    text = msg.text.strip()

    if user_id in WAITING_NAME:
        if len(text) < 2:
            await msg.reply_text("❗ 姓名太短，请重新输入：")
            return
        try:
            set_user_name(user_id, username, text)
        except ValueError as e:
            await msg.reply_text(f"⚠️ {e}")
            return
        WAITING_NAME.pop(user_id)
        await send_welcome(update.message, text)
        return

    if not get_user_name(user_id):
        WAITING_NAME[user_id] = True
        await msg.reply_text("👤 请先输入姓名后再打卡：")
        return

    keyword = extract_keyword(text)
    if keyword:
        if keyword == "#下班打卡" and not has_user_checked_keyword_today_fixed(user_id, "#上班打卡"):
            await msg.reply_text("❗ 你今天还没打上班卡。上班时间过了？请发送“#补卡”+IP截图补卡后再打下班卡。")
            return
        if keyword == "#补卡":
            await msg.reply_text("📌 请发送“#补卡”并附IP截图完成补卡。")
            return
        await msg.reply_text("❗️请附带IP截图。")

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    tg_user = msg.from_user
    user_id = tg_user.id
    username = tg_user.username or f"user{user_id}"
    sync_username(user_id, username)
    caption = msg.caption or ""
    keyword = extract_keyword(caption)

    if not get_user_name(user_id):
        WAITING_NAME[user_id] = True
        await msg.reply_text("👤 请先输入姓名后再打卡：")
        return

    if not keyword:
        await msg.reply_text("❗ 图片必须附加关键词：#上班打卡 / #下班打卡 / #补卡")
        return

    photo = msg.photo[-1]
    file = await photo.get_file()
    if file.file_size > 1024 * 1024:
        await msg.reply_text("❗ 图片太大，不能超过1MB。")
        return
    today_str = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
    tmp_path = f"/tmp/{today_str}_{username}_{keyword}.jpg"
    await file.download_to_drive(tmp_path)
    image_url = upload_image(tmp_path)
    os.remove(tmp_path)

    name = get_user_name(user_id)
    now = datetime.now(BEIJING_TZ)

    if keyword == "#上班打卡":
        save_message(user_id, username, name, image_url, now, keyword)
        keyboard = [[InlineKeyboardButton(v, callback_data=f"shift:{k}")] for k, v in SHIFT_OPTIONS.items()]
        await msg.reply_text("请选择今天的班次：", reply_markup=InlineKeyboardMarkup(keyboard))

    elif keyword == "#补卡":
        context.user_data["makeup_data"] = {
            "user_id": user_id,
            "username": username,
            "name": name,
            "image_url": image_url,
            "date": (now - timedelta(days=1)).date() if now.hour < 6 else now.date()
        }
        keyboard = [[InlineKeyboardButton(v, callback_data=f"makeup_shift:{k}")] for k, v in SHIFT_OPTIONS.items()]
        await msg.reply_text("请选择要补卡的班次：", reply_markup=InlineKeyboardMarkup(keyboard))

    elif keyword == "#下班打卡":
        if has_user_checked_keyword_today_fixed(user_id, keyword):
            await msg.reply_text(f"⚠️ 你今天已经提交过“{keyword}”了哦！")
            return
        logs = get_user_logs(user_id, now - timedelta(days=1), now)
        last_check_in, last_shift = None, None
        for ts, kw, shift in reversed(logs):
            if kw == "#上班打卡":
                last_check_in = parse(ts) if isinstance(ts, str) else ts
                last_shift = shift.split("（")[0] if shift else None
                break
        if not last_check_in:
            await msg.reply_text("❗ 你今天还没有打上班卡，请先打卡或补卡。")
            return
        save_message(user_id, username, name, image_url, now, keyword, shift=last_shift)
        await msg.reply_text(f"✅ 下班打卡成功！班次：{last_shift or '未选择'}")

async def shift_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    tg_user = query.from_user
    user_id = tg_user.id
    shift_code = query.data.split(":")[1]
    shift_name = SHIFT_OPTIONS[shift_code]
    save_shift(user_id, shift_name)
    await query.edit_message_text(f"✅ 上班打卡成功！班次：{shift_name}")

async def handle_makeup_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    tg_user = msg.from_user
    user_id = tg_user.id
    username = tg_user.username or f"user{user_id}"
    sync_username(user_id, username)
    name = get_user_name(user_id)
    now = datetime.now(BEIJING_TZ)

    if now.hour < 6:
        ref_date = (now - timedelta(days=1)).date()
    else:
        ref_date = now.date()

    start = datetime.combine(ref_date, datetime.min.time(), tzinfo=BEIJING_TZ)
    end = start + timedelta(days=1)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT shift FROM messages
            WHERE user_id=%s AND keyword=%s AND timestamp >= %s AND timestamp < %s
        """, (user_id, "#上班打卡", start, end))
        rows = cur.fetchall()

    if rows:
        await msg.reply_text(f"⚠️ {ref_date.strftime('%m月%d日')} 已有上班打卡记录，不能重复补卡。")
        return

    keyboard = [[InlineKeyboardButton(v, callback_data=f"makeup_shift:{k}")] for k, v in SHIFT_OPTIONS.items()]
    await msg.reply_text("请选择要补卡的班次：", reply_markup=InlineKeyboardMarkup(keyboard))

    context.user_data["makeup_data"] = {
        "user_id": user_id,
        "username": username,
        "name": name,
        "date": ref_date
    }
    context.user_data.pop("awaiting_makeup", None)

async def makeup_shift_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = context.user_data.get("makeup_data")
    if not data:
        await query.edit_message_text("⚠️ 补卡信息丢失，请重新发送“#补卡”。")
        return

    shift_code = query.data.split(":")[1]
    shift_name = SHIFT_OPTIONS[shift_code]
    shift_short = shift_name.split("（")[0]
    start_time, _ = SHIFT_TIMES[shift_short]
    punch_dt = datetime.combine(data["date"], start_time, tzinfo=BEIJING_TZ)

    save_message(
        user_id=data["user_id"],
        username=data["username"],
        name=data["name"],
        content=data.get("image_url", "补卡"),
        timestamp=punch_dt,
        keyword="#上班打卡",
        shift=shift_name + "（补卡）"
    )

    await query.edit_message_text(f"✅ 补卡成功！班次：{shift_name}")
    context.user_data.pop("makeup_data", None)

async def admin_makeup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ 无权限，仅管理员可操作。")
        return

    if len(context.args) not in (3, 4):
        await update.message.reply_text(
            "⚠️ 用法：/admin_makeup @username YYYY-MM-DD 班次(F/G/H/I) [上班/下班]"
        )
        return

    username_arg, date_str, shift_code = context.args[:3]
    username_arg = username_arg.lstrip("@")
    shift_code = shift_code.upper()
    punch_type = context.args[3] if len(context.args) == 4 else "上班"

    if shift_code not in SHIFT_OPTIONS or punch_type not in ("上班", "下班"):
        await update.message.reply_text("⚠️ 班次或类型无效。")
        return

    try:
        makeup_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        await update.message.reply_text("⚠️ 日期格式错误，应为 YYYY-MM-DD")
        return

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id, name FROM users WHERE username=%s", (username_arg,))
        row = cur.fetchone()
        if not row:
            await update.message.reply_text(f"⚠️ 用户 @{username_arg} 未登记。")
            return
        user_id, name = row

    shift_name = SHIFT_OPTIONS[shift_code] + "（补卡）"
    shift_short = shift_name.split("（")[0]
    start_time, end_time = SHIFT_TIMES[shift_short]

    if punch_type == "上班":
        punch_dt = datetime.combine(makeup_date, start_time, tzinfo=BEIJING_TZ)
        keyword = "#上班打卡"
    else:
        if shift_short == "I班" and end_time == datetime.strptime("00:00", "%H:%M").time():
            punch_dt = datetime.combine(makeup_date + timedelta(days=1), end_time, tzinfo=BEIJING_TZ)
        else:
            punch_dt = datetime.combine(makeup_date, end_time, tzinfo=BEIJING_TZ)
        keyword = "#下班打卡"

    save_message(user_id, username_arg, name, f"补卡（管理员-{punch_type}）", punch_dt, keyword, shift_name)
    await update.message.reply_text(
        f"✅ 管理员补卡成功：{name}（@{username_arg}）\n"
        f"📅 日期：{makeup_date}\n🏷 班次：{shift_name}\n🔹 类型：{punch_type}\n⏰ 时间：{punch_dt.strftime('%Y-%m-%d %H:%M')}"
    )

LOGS_PER_PAGE = 7  

async def mylogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    user_id = tg_user.id
    username = tg_user.username or f"user{user_id}"
    sync_username(user_id, username)

    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start + timedelta(days=32)).replace(day=1)

    logs = get_user_logs(user_id, start, end)
    if not logs:
        await update.message.reply_text("📭 本月暂无打卡记录。")
        return

    logs = [(parse(ts) if isinstance(ts, str) else ts, kw, shift) for ts, kw, shift in logs]
    logs = [(ts.astimezone(BEIJING_TZ), kw, shift) for ts, kw, shift in logs]
    logs = sorted(logs, key=lambda x: x[0])

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
            pass
        elif has_late or has_early:
            total_abnormal += 1
        else:
            total_complete += 2 if has_up and has_down else 1

    all_days = sorted(daily_map)
    pages = [all_days[i:i + LOGS_PER_PAGE] for i in range(0, len(all_days), LOGS_PER_PAGE)]
    context.user_data["mylogs_pages"] = {
        "pages": pages,
        "daily_map": daily_map,
        "page_index": 0,
        "summary": (total_complete, total_abnormal, total_makeup)
    }

    await send_mylogs_page(update, context)

async def send_mylogs_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data["mylogs_pages"]
    pages, daily_map, page_index = data["pages"], data["daily_map"], data["page_index"]
    total_complete, total_abnormal, total_makeup = data["summary"]

    current_page_days = pages[page_index]
    reply = f"🗓️ 本月打卡情况（第 {page_index+1}/{len(pages)} 页）：\n\n"

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

    buttons = []
    if page_index > 0:
        buttons.append(InlineKeyboardButton("⬅ 上一页", callback_data="mylogs_prev"))
    if page_index < len(pages) - 1:
        buttons.append(InlineKeyboardButton("➡ 下一页", callback_data="mylogs_next"))
    markup = InlineKeyboardMarkup([buttons]) if buttons else None

    if update.callback_query:
        await update.callback_query.edit_message_text(reply, reply_markup=markup)
    else:
        await update.message.reply_text(reply, reply_markup=markup)

async def mylogs_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if "mylogs_pages" not in context.user_data:
        await query.edit_message_text("⚠️ 会话已过期，请重新使用 /mylogs")
        return

    if query.data == "mylogs_prev":
        context.user_data["mylogs_pages"]["page_index"] -= 1
    elif query.data == "mylogs_next":
        context.user_data["mylogs_pages"]["page_index"] += 1

    await send_mylogs_page(update, context)

def get_default_month_range():
    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if now.month == 12:
        end = start.replace(year=now.year + 1, month=1)
    else:
        end = start.replace(month=now.month + 1)
    return start, end

async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ 无权限，仅管理员可导出记录。")
        return
    args = context.args
    if len(args) == 2:
        try:
            start = parse(args[0]).replace(tzinfo=BEIJING_TZ, hour=0, minute=0, second=0, microsecond=0)
            end = parse(args[1]).replace(tzinfo=BEIJING_TZ, hour=23, minute=59, second=59, microsecond=999999)
        except Exception:
            await update.message.reply_text("⚠️ 日期格式错误，请使用 /export YYYY-MM-DD YYYY-MM-DD")
            return
    else:
        start, end = get_default_month_range()
    status_msg = await update.message.reply_text("⏳ 正在导出 Excel，请稍等...")
    file_path = export_excel(start, end)
    try:
        await status_msg.delete()
    except:
        pass
    if not file_path:
        await update.message.reply_text("⚠️ 指定日期内没有数据。")
        return
    if file_path.startswith("http"):
        await update.message.reply_text(f"✅ 导出完成，文件过大已上传到云端：\n{file_path}")
    else:
        await update.message.reply_document(document=open(file_path, "rb"))
        os.remove(file_path)

async def export_images_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ 无权限，仅管理员可导出记录。")
        return

    args = context.args
    if len(args) == 2:
        try:
            start = parse(args[0]).replace(tzinfo=BEIJING_TZ, hour=0, minute=0, second=0, microsecond=0)
            end = parse(args[1]).replace(tzinfo=BEIJING_TZ, hour=23, minute=59, second=59, microsecond=999999)
        except Exception:
            await update.message.reply_text("⚠️ 日期格式错误，请使用 /export_images YYYY-MM-DD YYYY-MM-DD")
            return
    else:
        start, end = get_default_month_range()

    status_msg = await update.message.reply_text("⏳ 正在导出图片，请稍等...")
    result = export_images(start, end)
    try:
        await status_msg.delete()
    except:
        pass

    if not result:
        await update.message.reply_text("⚠️ 指定日期内没有图片。")
        return

    zip_paths, export_dir = result
    if len(zip_paths) == 1:
        with open(zip_paths[0], "rb") as f:
            await update.message.reply_document(document=f)
    else:
        await update.message.reply_text(f"📦 共生成 {len(zip_paths)} 个分包，开始发送…")
        for idx, zip_path in enumerate(zip_paths, 1):
            with open(zip_path, "rb") as f:
                await update.message.reply_document(document=f, caption=f"📦 第 {idx} 包")

    for zip_path in zip_paths:
        os.remove(zip_path)
    shutil.rmtree(export_dir, ignore_errors=True)

def check_existing_instance():
    lock_file = "/tmp/bot.lock"
    if os.path.exists(lock_file):
        with open(lock_file) as f:
            pid = int(f.read())
            if os.path.exists(f"/proc/{pid}"):
                print("⚠️ 检测到已有 Bot 实例在运行，退出。")
                sys.exit(1)
    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))
    import atexit
    atexit.register(lambda: os.remove(lock_file) if os.path.exists(lock_file) else None)

def main():
    init_db()
    os.makedirs(DATA_DIR, exist_ok=True)
    scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(delete_last_month_data, CronTrigger(day=15, hour=3))
    scheduler.start()

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("mylogs", mylogs_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CommandHandler("export_images", export_images_cmd))
    app.add_handler(CommandHandler("admin_makeup", admin_makeup_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(CallbackQueryHandler(shift_callback, pattern=r"^shift:"))
    app.add_handler(CallbackQueryHandler(makeup_shift_callback, pattern=r"^makeup_shift:"))
    app.add_handler(CallbackQueryHandler(mylogs_page_callback, pattern=r"^mylogs_(prev|next)$"))
    print("🤖 Bot 正在运行...")
    app.run_polling()

if __name__ == "__main__":
    check_existing_instance()
    main()
