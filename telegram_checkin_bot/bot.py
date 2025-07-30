import os
import sys
import asyncio
from datetime import datetime, timedelta, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dateutil.parser import parse
from collections import defaultdict

from config import TOKEN, KEYWORDS, ADMIN_IDS, DATA_DIR
from db_pg import init_db, has_user_checked_keyword_today, save_message, delete_old_data, get_user_logs, save_shift, get_user_name, set_user_name, get_db
from export import export_messages
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
    """从文本中提取打卡关键词"""
    text = text.strip().replace(" ", "")
    for kw in KEYWORDS:
        if kw in text:
            return kw
    return None

# ✅ 修正后的“今天是否已打卡”逻辑（支持跨天下班）
def has_user_checked_keyword_today_fixed(username, keyword):
    now = datetime.now(BEIJING_TZ)

    # 确定参考日期
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

    # 过滤掉凌晨的下班记录（它属于前一天）
    for (ts,) in rows:
        ts_local = ts.astimezone(BEIJING_TZ)
        if keyword == "#下班打卡" and ts_local.hour < 6:
            continue  # 归前一天，不算今天
        return True  # 有有效下班卡
    return False



async def send_welcome(update_or_msg, name):
    welcome_text = (
        f"您好，{name}！\n\n"
        "📌 使用说明：\n"
        "1️⃣ 向机器人发送“#上班打卡”或“#下班打卡”并附带IP截图；\n"
        "2️⃣ 上下班打卡间隔不能超过12小时，否则下班信息不录入；\n\n"
        "IP截图标准\n"
        "① 设备编码：本机序列号\n"
        "② 实时IP：指定网站内显示的IP截图\n"
        "③ 本地时间：电脑任务栏时间截图（需含月、日、时、分）\n\n"
        "<a href='https://www.ipaddress.my'>点击这里查看你的IP地址</a>\n\n"
        "举个🌰，如下👇"
    )
    await update_or_msg.reply_text(welcome_text, parse_mode="HTML")
    await asyncio.sleep(1)
    await update_or_msg.reply_photo(
        photo="https://i.postimg.cc/3xRMBbT4/photo-2025-07-28-15-55-19.jpg",
        caption="#上班打卡"
    )

# ========== 姓名登记 ==========
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    username = tg_user.username or f"user{tg_user.id}"

    if not get_user_name(username):
        WAITING_NAME[username] = True
        await update.message.reply_text("👤 第一次打卡前请输入你的工作名：")
        return
        
    name = get_user_name(username)
    await send_welcome(update.message, name)

# ========== 处理文字消息 ==========
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
            await msg.reply_text("❗ 你今天还没有打上班卡呢，赶紧去上班！")
            return
        await msg.reply_text("❗️请附带上IP截图哦。")

# ========== 处理图片打卡 ==========
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

    # 下班打卡验证
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
            await msg.reply_text("❗ 找不到上班打卡记录，下班打卡无效。")
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

# ========== 处理班次选择 ==========
async def shift_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    username = query.from_user.username or f"user{query.from_user.id}"
    shift_code = query.data.split(":")[1]
    shift_name = SHIFT_OPTIONS[shift_code]
    save_shift(username, shift_name)
    await query.edit_message_text(f"✅ 上班打卡成功！你的班次：{shift_name}")

# ========== 查看本月打卡 ==========
async def mylogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.effective_user.username or f"user{update.effective_user.id}"
    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start + timedelta(days=32)).replace(day=1)

    logs = get_user_logs(username, start, end)
    if not logs:
        await update.message.reply_text("📭 本月暂无打卡记录。")
        return

    logs = sorted(logs, key=lambda x: parse(x[0]) if isinstance(x[0], str) else x[0])
    daily_map = defaultdict(dict)

    i = 0
    while i < len(logs):
        ts, kw, shift = logs[i]
        if isinstance(ts, str): ts = parse(ts)
        ts = ts.astimezone(BEIJING_TZ)

        date_key = ts.date()
        if kw == "#下班打卡" and ts.hour < 6:
            date_key = (ts - timedelta(days=1)).date()

        if kw == "#上班打卡":
            daily_map[date_key]["shift"] = shift
            daily_map[date_key]["#上班打卡"] = ts

            j = i + 1
            while j < len(logs):
                ts2, kw2, _ = logs[j]
                if isinstance(ts2, str): ts2 = parse(ts2)
                ts2 = ts2.astimezone(BEIJING_TZ)
                if kw2 == "#下班打卡" and timedelta(0) < (ts2 - ts) <= timedelta(hours=12):
                    if ts2.hour < 6:
                        daily_map[ts.date()]["#下班打卡"] = ts2
                    else:
                        daily_map[date_key]["#下班打卡"] = ts2
                    break
                j += 1
            i = j
        else:
            daily_map[date_key]["#下班打卡"] = ts
            i += 1

    daily_map = {d: v for d, v in daily_map.items() if d.month == now.month}

    if not daily_map:
        await update.message.reply_text("📭 本月暂无打卡记录。")
        return

    reply = "🗓️ 本月打卡情况（北京时间）：\n\n"
    complete = 0
    for idx, day in enumerate(sorted(daily_map), start=1):
        kw_map = daily_map[day]
        shift_full = kw_map.get("shift", "未选择班次")
        shift = shift_full.split("（")[0]

        # 检查缺少的打卡
        has_up = "#上班打卡" in kw_map
        has_down = "#下班打卡" in kw_map

        reply += f"{idx}. {day.strftime('%m月%d日')} - {shift}\n"
        if has_up:
            reply += f"   └─ 上班打卡：{kw_map['#上班打卡'].strftime('%H:%M')}\n"
        else:
            reply += f"   └─ 缺少上班打卡\n"

        if has_down:
            reply += f"   └─ 下班打卡：{kw_map['#下班打卡'].strftime('%H:%M')}\n"
        else:
            reply += f"   └─ 缺少下班打卡\n"

        if has_up and has_down:
            complete += 1

    reply += f"\n✅ 本月完整打卡：{complete} 天"
    await update.message.reply_text(reply)

# ========== 导出数据 ==========
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
        now = datetime.now(tz)
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = (start + timedelta(days=32)).replace(day=1)

    file_path = export_messages(start, end)
    if not file_path:
        await update.message.reply_text("⚠️ 指定日期内没有数据。")
        return

    try:
        await update.message.reply_document(document=open(file_path, "rb"))
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

# ========== 主程序 ==========
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
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(CallbackQueryHandler(shift_callback, pattern=r"^shift:"))

    print("🤖 Bot 正在运行...")
    app.run_polling()

if __name__ == "__main__":
    check_existing_instance()
    main()
