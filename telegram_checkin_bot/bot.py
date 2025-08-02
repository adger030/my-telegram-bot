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
from db_pg import init_db, save_message, get_user_logs, save_shift, get_user_name, set_user_name, get_db
from export import export_excel, export_images
from upload_image import upload_image
from cleaner import delete_last_month_data

# 北京时区
BEIJING_TZ = timezone(timedelta(hours=8))
WAITING_NAME = {}  # 记录等待输入姓名的用户

SHIFT_OPTIONS = {
    "F": "F班（12:00-21:00）",
    "G": "G班（13:00-22:00）",
    "H": "H班（14:00-23:00）",
    "I": "I班（15:00-00:00）"
}

def extract_keyword(text: str):
    text = text.strip().replace(" ", "")
    for kw in KEYWORDS:
        if kw in text:
            return kw
    return None

def has_user_checked_keyword_today_fixed(username, keyword):
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
            WHERE username=%s AND keyword=%s
            AND timestamp >= %s AND timestamp < %s
            ORDER BY timestamp DESC
        """, (username, keyword, start, end))
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
        "2️⃣ 上班打卡需要选择你的班次，即可打卡成功；\n\n"
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
    username = tg_user.username or f"user{tg_user.id}"
    if not get_user_name(username):
        WAITING_NAME[username] = True
        await update.message.reply_text("👤 第一次打卡前请输入你的工作名：")
        return
    name = get_user_name(username)
    await send_welcome(update.message, name)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    username = msg.from_user.username or f"user{msg.from_user.id}"
    text = msg.text.strip()

    if username in WAITING_NAME:
        if len(text) < 2:
            await msg.reply_text("❗ 姓名太短，请重新输入：")
            return
        try:
            set_user_name(username, text)
        except ValueError as e:
            await msg.reply_text(f"⚠️ {e}")
            return
        WAITING_NAME.pop(username)
        name = get_user_name(username)
        await send_welcome(update.message, name)
        return

    if not get_user_name(username):
        WAITING_NAME[username] = True
        await msg.reply_text("👤 请先输入姓名后再打卡：")
        return

    keyword = extract_keyword(text)
    if keyword:
        if keyword == "#下班打卡" and not has_user_checked_keyword_today_fixed(username, "#上班打卡"):
            await msg.reply_text("❗ 你今天还没有打上班卡呢，请先打上班卡哦～ 上班时间过了？是否要补上班卡？回复“#补卡”。")
            context.user_data["awaiting_makeup"] = True
            return
        await msg.reply_text("❗️请附带上IP截图哦。")
    elif text == "#补卡" and context.user_data.get("awaiting_makeup"):
        await handle_makeup_checkin(update, context)

async def handle_makeup_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """补上班卡功能"""
    msg = update.message
    username = msg.from_user.username or f"user{msg.from_user.id}"
    name = get_user_name(username)
    now = datetime.now(BEIJING_TZ)

    if now.hour < 6:
        timestamp = (now - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    else:
        timestamp = now.replace(hour=9, minute=0, second=0)

    keyboard = [[InlineKeyboardButton(v, callback_data=f"makeup_shift:{k}")] for k, v in SHIFT_OPTIONS.items()]
    await msg.reply_text("请选择要补卡的班次：", reply_markup=InlineKeyboardMarkup(keyboard))
    context.user_data["makeup_data"] = {"username": username, "name": name, "timestamp": timestamp}
    context.user_data.pop("awaiting_makeup", None)

async def makeup_shift_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    shift_code = query.data.split(":")[1]
    shift_name = SHIFT_OPTIONS[shift_code] + "（补卡）"
    data = context.user_data.get("makeup_data")

    if not data:
        await query.edit_message_text("⚠️ 未找到补卡数据，请重新发送“#补卡”命令。")
        return

    try:
        # 调试日志
        print(f"💾 [补卡写入数据库] 用户: {data['username']}, 班次: {shift_name}, 时间: {data['timestamp']}")
        
        # 调用 save_message 将补卡信息写入数据库
        save_message(
            username=data["username"],
            name=data["name"],
            content="补卡",  # 固定写 "补卡" 作为占位内容
            timestamp=data["timestamp"],
            keyword="#上班打卡",
            shift=shift_name
        )

        # 成功提示
        await query.edit_message_text(f"✅ 补上班卡成功！班次：{shift_name}")

    except Exception as e:
        print(f"❌ [补卡写入失败] {e}")
        await query.edit_message_text("❌ 补卡失败，数据库写入错误，请重试或联系管理员。")

    finally:
        # 清除临时补卡数据，防止重复提交
        context.user_data.pop("makeup_data", None)


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    username = msg.from_user.username or f"user{msg.from_user.id}"
    caption = msg.caption or ""
    matched_keyword = extract_keyword(caption)

    if not get_user_name(username):
        WAITING_NAME[username] = True
        await msg.reply_text("👤 请先输入姓名后再打卡：")
        return

    if not matched_keyword:
        await msg.reply_text("❗️图片必须附带打卡关键词，例如：“#上班打卡”或“#下班打卡”。")
        return

    if has_user_checked_keyword_today_fixed(username, matched_keyword):
        await msg.reply_text(f"⚠️ 你今天已经提交过“{matched_keyword}”了哦！")
        return

    if matched_keyword == "#下班打卡":
        now = datetime.now(BEIJING_TZ)
        logs = get_user_logs(username, now - timedelta(days=1), now)
        last_check_in, last_shift = None, None
        for ts, kw, shift in reversed(logs):
            if kw == "#上班打卡":
                last_check_in = parse(ts) if isinstance(ts, str) else ts
                last_shift = shift
                break
        if not last_check_in:
            await msg.reply_text("❗ 找不到上班打卡记录，下班打卡无效。是否要补上班卡？回复“#补卡”以补卡。")
            context.user_data["awaiting_makeup"] = True
            return
        last_check_in = last_check_in.astimezone(BEIJING_TZ)
        if now < last_check_in:
            await msg.reply_text("❗ 下班时间不能早于上班时间。")
            return
        if now - last_check_in > timedelta(hours=12):
            await msg.reply_text("❗ 上班打卡已超过12小时，下班打卡无效。")
            return

    photo = msg.photo[-1]
    file = await photo.get_file()
    if file.file_size > 1024 * 1024:
        await msg.reply_text("❗️ 图片太大，不能超过1MB。")
        return

    today_str = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
    tmp_path = f"/tmp/{today_str}_{username}_{matched_keyword}.jpg"
    await file.download_to_drive(tmp_path)
    image_url = upload_image(tmp_path)
    os.remove(tmp_path)
    now = datetime.now(BEIJING_TZ)
    name = get_user_name(username)

    if matched_keyword == "#上班打卡":
        save_message(username=username, name=name, content=image_url, timestamp=now, keyword=matched_keyword)
        keyboard = [[InlineKeyboardButton(v, callback_data=f"shift:{k}")] for k, v in SHIFT_OPTIONS.items()]
        await msg.reply_text("请选择今天的班次：", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        save_message(username=username, name=name, content=image_url, timestamp=now, keyword=matched_keyword, shift=last_shift)
        await msg.reply_text(f"✅ 下班打卡成功！班次：{last_shift or '未选择'}")

async def shift_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    username = query.from_user.username or f"user{query.from_user.id}"
    shift_code = query.data.split(":")[1]
    shift_name = SHIFT_OPTIONS[shift_code]
    save_shift(username, shift_name)
    await query.edit_message_text(f"✅ 上班打卡成功！班次：{shift_name}")

async def mylogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.effective_user.username or f"user{update.effective_user.id}"
    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start + timedelta(days=32)).replace(day=1)

    logs = get_user_logs(username, start, end)
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
            found_down = False
            while j < len(logs):
                ts2, kw2, _ = logs[j]
                if kw2 == "#下班打卡" and timedelta(0) < (ts2 - ts) <= timedelta(hours=12):
                    if ts2.hour < 6:
                        daily_map[ts.date()]["#下班打卡"] = ts2
                    else:
                        daily_map[date_key]["#下班打卡"] = ts2
                    found_down = True
                    break
                j += 1
            i = j if found_down else i + 1
        else:
            daily_map[date_key]["#下班打卡"] = ts
            i += 1

    reply = "🗓️ 本月打卡情况（北京时间）：\n\n"
    complete = 0
    late_count = 0
    early_count = 0
    makeup_count = 0

    for idx, day in enumerate(sorted(daily_map), start=1):
        kw_map = daily_map[day]
        shift_full = kw_map.get("shift", "未选择班次")
        is_makeup = "补卡" in shift_full
        shift_name = shift_full.split("（")[0]
        has_up = "#上班打卡" in kw_map
        has_down = "#下班打卡" in kw_map

        if is_makeup:
            makeup_count += 1

        reply += f"{idx}. {day.strftime('%m月%d日')} - {shift_name}{' 🟡（补卡）' if is_makeup else ''}\n"

        # 上班打卡
        if has_up:
            up_ts = kw_map["#上班打卡"]
            up_status = ""
            if shift_name in SHIFT_TIMES:
                start_time, _ = SHIFT_TIMES[shift_name]
                if up_ts.time() > start_time:
                    up_status = " 🔴（迟到）"
                    late_count += 1
            reply += f"   └─ #上班打卡：{up_ts.strftime('%H:%M')}{' 🟡（补卡）' if is_makeup else ''}{up_status}\n"
        else:
            reply += "   └─ ❌ 缺少上班打卡\n"

        # 下班打卡
        if has_down:
            down_ts = kw_map["#下班打卡"]
            down_status = ""
            if shift_name in SHIFT_TIMES:
                _, end_time = SHIFT_TIMES[shift_name]
                if shift_name == "I班" and down_ts.hour < 12:
                    pass  # I班跨天不判早退
                elif down_ts.time() < end_time:
                    down_status = " 🔴（早退）"
                    early_count += 1
            next_day = down_ts.date() > day
            reply += f"   └─ #下班打卡：{down_ts.strftime('%H:%M')}{'（次日）' if next_day else ''}{down_status}\n"
        else:
            reply += "   └─ ❌ 缺少下班打卡\n"

        if has_up and has_down and not is_makeup:
            complete += 1

    # ✅ 绿色标识完整打卡
    reply += (
        f"\n🟢 本月完整打卡：{complete} 天\n"
        f"🔴 迟到次数：{late_count} 次\n"
        f"🔴 早退次数：{early_count} 次\n"
        f"🟡 补卡次数：{makeup_count} 次"
    )

    await update.message.reply_text(reply)

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
    tz = BEIJING_TZ
    args = context.args
    if len(args) == 2:
        try:
            start = parse(args[0]).replace(tzinfo=tz, hour=0, minute=0, second=0, microsecond=0)
            end = parse(args[1]).replace(tzinfo=tz, hour=23, minute=59, second=59, microsecond=999999)
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
    tz = BEIJING_TZ
    args = context.args
    if len(args) == 2:
        try:
            start = parse(args[0]).replace(tzinfo=tz, hour=0, minute=0, second=0, microsecond=0)
            end = parse(args[1]).replace(tzinfo=tz, hour=23, minute=59, second=59, microsecond=999999)
        except Exception:
            await update.message.reply_text("⚠️ 日期格式错误，请使用 /export_images YYYY-MM-DD YYYY-MM-DD")
            return
    else:
        start, end = get_default_month_range()
    status_msg = await update.message.reply_text("⏳ 正在导出图片，请稍等...")
    file_path = export_images(start, end)
    try:
        await status_msg.delete()
    except:
        pass
    if not file_path:
        await update.message.reply_text("⚠️ 指定日期内没有图片。")
        return
    if file_path.startswith("http"):
        await update.message.reply_text(f"✅ 图片打包完成，文件过大已上传到云端：\n{file_path}")
    else:
        await update.message.reply_document(document=open(file_path, "rb"))
        os.remove(file_path)

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
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(CallbackQueryHandler(shift_callback, pattern=r"^shift:"))
    app.add_handler(CallbackQueryHandler(makeup_shift_callback, pattern=r"^makeup_shift:"))
    print("🤖 Bot 正在运行...")
    app.run_polling()

if __name__ == "__main__":
    check_existing_instance()
    main()
