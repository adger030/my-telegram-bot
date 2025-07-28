import os
import asyncio
from datetime import datetime, timedelta, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dateutil.parser import parse
from collections import defaultdict

from config import TOKEN, KEYWORDS, ADMIN_IDS, DATA_DIR
from db_pg import init_db, has_user_checked_keyword_today, save_message, delete_old_data, get_user_logs, save_shift, get_user_name, set_user_name, get_today_shift
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

# ========== 姓名登记 ==========
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    username = tg_user.username or f"user{tg_user.id}"

    if not get_user_name(username):
        WAITING_NAME[username] = True
        await update.message.reply_text("👤 欢迎使用 MS 部考勤机器人，请输入你的工作名：")
        return

    # 欢迎提示
    name = get_user_name(username)
    welcome_text = (
        f"您好，{name}！\n\n"
        "📌 使用说明：\n"
        "1️⃣ 向机器人发送“#上班打卡”或“#下班打卡”并附带IP截图；\n"
        "2️⃣ 上下班打卡间隔不能超过10小时，否则下班信息不录入；\n\n"
        "IP截图标准\n"
        "① 设备编码：本机序列号\n"
        "② 实时IP：指定网站内显示的IP截图\n"
        "③ 本地时间：电脑任务栏时间截图（需含月、日、时、分）\n\n"
        "<a href='https://www.ipaddress.my'>点击这里查看你的IP地址</a>\n\n"
        "举个🌰，如下👇"
    )
    await update.message.reply_text(welcome_text, parse_mode="HTML")
    await asyncio.sleep(1)
    await update.message.reply_photo(photo="https://ibb.co/ZzFwc5yS", caption="#上班打卡")

# ========== 处理文字消息 ==========
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    username = msg.from_user.username or f"user{msg.from_user.id}"
    text = msg.text.strip()

    # 如果在等待输入姓名
    if username in WAITING_NAME:
        if len(text) < 2:
            await msg.reply_text("❗ 姓名太短，请重新输入：")
            return
        try:
            set_user_name(username, text)  # 检查唯一性
        except ValueError as e:
            await msg.reply_text(f"⚠️ {e}")
            return  # 不移除 WAITING_NAME，继续等待用户输入新名字

        WAITING_NAME.pop(username)
        name = get_user_name(username)
        welcome_text = (
            f"您好，{name}！\n\n"
            "📌 使用说明：\n"
            "1️⃣ 向机器人发送“#上班打卡”或“#下班打卡”并附带IP截图；\n"
            "2️⃣ 上下班打卡间隔不能超过10小时，否则下班信息不录入；\n\n"
            "IP截图标准\n"
            "① 设备编码：本机序列号\n"
            "② 实时IP：指定网站内显示的IP截图\n"
            "③ 本地时间：电脑任务栏时间截图（需含月、日、时、分）\n\n"
            "<a href='https://www.ipaddress.my'>点击这里查看你的IP地址</a>\n\n"
            "举个🌰，如下👇"
        )
        await msg.reply_text(welcome_text, parse_mode="HTML")
        await asyncio.sleep(1)
        await msg.reply_photo(photo="https://ibb.co/ZzFwc5yS", caption="#上班打卡")
        return

    # 未登记姓名
    if not get_user_name(username):
        WAITING_NAME[username] = True
        await msg.reply_text("👤 请先输入姓名后再打卡：")
        return

    # 检测打卡关键词
    if extract_keyword(text):
        await msg.reply_text("❗️请附带上IP截图哦。")


# ========== 处理图片打卡 ==========
async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    username = msg.from_user.username or f"user{msg.from_user.id}"
    caption = msg.caption or ""
    matched_keyword = extract_keyword(caption)

    # 检查姓名
    if not get_user_name(username):
        WAITING_NAME[username] = True
        await msg.reply_text("👤 请先输入姓名后再打卡：")
        return

    if not matched_keyword:
        await msg.reply_text("❗️图片必须附带打卡关键词，例如：“#上班打卡”或“#下班打卡”。")
        return

    # 检查是否已打卡
    if has_user_checked_keyword_today(username, matched_keyword):
        await msg.reply_text(f"⚠️ 你今天已经提交过“{matched_keyword}”了哦！")
        return

    # 下载图片
    photo = msg.photo[-1]
    file = await photo.get_file()
    if file.file_size > 1024 * 1024:
        await msg.reply_text("❗️图片太大，不能超过1MB。")
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
        await msg.reply_text("✅ 上班打卡成功！请选择今天的班次：", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        shift = get_today_shift(username)
        save_message(username=username, name=name, content=image_url, timestamp=now, keyword=matched_keyword, shift=shift)
        await msg.reply_text(f"✅ 下班打卡成功！班次：{shift or '未选择'}")

# ========== 处理班次选择 ==========
async def shift_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    username = query.from_user.username or f"user{query.from_user.id}"
    shift_code = query.data.split(":")[1]
    shift_name = SHIFT_OPTIONS[shift_code]
    save_shift(username, shift_name)
    await query.edit_message_text(f"✅ 你的班次已记录：{shift_name}")

# ========== 查看本月打卡 ==========
async def mylogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.effective_user.username or f"user{update.effective_user.id}"
    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
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

        if kw == "#上班打卡":
            daily_map[date_key]["shift"] = shift
            daily_map[date_key]["#上班打卡"] = ts
            j = i + 1
            while j < len(logs):
                ts2, kw2, _ = logs[j]
                if isinstance(ts2, str): ts2 = parse(ts2)
                ts2 = ts2.astimezone(BEIJING_TZ)
                if kw2 == "#下班打卡" and timedelta(0) < (ts2 - ts) <= timedelta(hours=10):
                    daily_map[date_key]["#下班打卡"] = ts2
                    break
                j += 1
            i = j
        else:
            i += 1

    reply = "🗓️ 本月打卡情况（北京时间）：\n\n"
    complete = 0
    for idx, day in enumerate(sorted(daily_map), start=1):
        kw_map = daily_map[day]
        shift = kw_map.get("shift", "未选择班次")
        if "#上班打卡" in kw_map and "#下班打卡" in kw_map:
            reply += f"{idx}. {day.strftime('%m月%d日')} - {shift} - ✅ 已完成\n"
            complete += 1
        else:
            reply += f"{idx}. {day.strftime('%m月%d日')} - {shift} - 缺少打卡\n"
        for kw in ["#上班打卡", "#下班打卡"]:
            if kw in kw_map:
                reply += f"   └─ {kw}：{kw_map[kw].strftime('%H:%M')}\n"

    reply += f"\n✅ 本月完整打卡：{complete} 天"
    await update.message.reply_text(reply)

# ========== 导出数据 ==========
async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ 无权限，仅管理员可导出记录。")
        return

    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0)
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    end = next_month

    file_path = export_messages(start, end)
    if not file_path:
        await update.message.reply_text("⚠️ 指定日期内没有数据。")
        return

    await update.message.reply_document(document=open(file_path, "rb"))
    os.remove(file_path)

# ========== 主程序 ==========
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
    main()
