import os
import asyncio
from datetime import datetime, timedelta, timezone
from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ChatAction
from apscheduler.schedulers.background import BackgroundScheduler
from dateutil.parser import parse

from apscheduler.triggers.cron import CronTrigger
from cleaner import delete_last_month_data

from config import TOKEN, KEYWORDS, ADMIN_IDS, DATA_DIR
from db_pg import init_db, has_user_checked_keyword_today, save_message, delete_old_data, get_user_month_logs
from export import export_messages
from upload_image import upload_image  # ✅ 新增导入

def extract_keyword(text: str):
    for kw in KEYWORDS:
        if kw in text:
            return kw
    return None

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.from_user:
        return

    msg = update.message
    username = msg.from_user.username or f"user{msg.from_user.id}"

    if msg.chat.type != 'private':
        return
 
    text = msg.text or msg.caption or ""
    matched_keyword = next((kw for kw in KEYWORDS if kw in text), None)
    
    if not matched_keyword:
        await msg.reply_text("❗️消息中必须包含关键词，例如：“#上班打卡”或“#下班打卡”。")
        return

    if not msg.photo:
        await msg.reply_text("❗️必须附带一张图片哦（图片格式，非文件格式）。")
        return

    if has_user_checked_keyword_today(username, matched_keyword):
        await msg.reply_text(f"⚠️ 你今天已经提交过“{matched_keyword}”了哦！")
        return

    photo = msg.photo[-1]
    file = await photo.get_file()
    if file.file_size > 1024 * 1024:
        await msg.reply_text("❗️图片太大，不能超过1MB。")
        return
    beijing_tz = timezone(timedelta(hours=8))
    today_str = datetime.now(beijing_tz).strftime("%Y-%m-%d")
    tmp_path = f"/tmp/{today_str}_{username}_{matched_keyword}.jpg"
    await file.download_to_drive(tmp_path)

    # ✅ 上传到 Cloudinary，获取 URL
    image_url = upload_image(tmp_path)

    # ✅ 删除临时文件（可选）
    try:
        os.remove(tmp_path)
    except Exception as e:
        print(f"⚠️ 删除临时文件失败：{e}")

    # 存入数据库（UTC 时间）
    save_message(
        username=username,
        content=image_url,
        timestamp=datetime.now(beijing_tz),
        keyword=matched_keyword
    )
    
    await msg.reply_text("✅ 打卡成功！")

async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ 你无权限使用此指令，仅管理员可导出记录。")
        return

    args = context.args
    if len(args) != 2:
        await update.message.reply_text("请使用格式：/export YYYY-MM-DD YYYY-MM-DD")
        return

    start_date, end_date = args
    file_path = export_messages(start_date, end_date)

    if not file_path:
        await update.message.reply_text("⚠️ 指定日期内没有数据。")
        return

    await update.message.reply_document(document=open(file_path, "rb"))

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = user.first_name or user.username or "朋友"

    welcome_text= (
        f"您好，{username}！欢迎使用MS部考勤机器人\n"
        "\n"
        "📌 使用说明：\n"
        "1️⃣ 向我发送关键词“#上班打卡”或“#下班打卡”并附带你的IP截图\n"
        "2️⃣ 每个关键词每天只能提交一次哦～\n"
        "\n"
        "举个🌰，如下👇"
    )

    instruction_text = "#上班打卡\n"
    image_url = "https://ibb.co/jkPmfwGF"  # ✅ 替换为你的欢迎图

    await update.message.reply_text(welcome_text)
    await asyncio.sleep(1)
    await update.message.reply_photo(photo=image_url, caption=instruction_text)

async def mylogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = user.username or f"user{user.id}"

    logs = get_user_month_logs(username)
    if not logs:
        await update.message.reply_text("📭 本月暂无打卡记录。")
        return

    reply = "📅 本月打卡记录（北京时间）：\n\n"
    for i, (timestamp, keyword) in enumerate(logs, start=1):
        if isinstance(timestamp, str):
            utc_dt = parse(timestamp)
        else:
            utc_dt = timestamp
        beijing_dt = utc_dt.astimezone(timezone(timedelta(hours=8)))
        date_str = beijing_dt.strftime("%m月%d日 %H:%M")
        reply += f"{i}. 🕒 {date_str} ｜{keyword}\n"

    await update.message.reply_text(reply)

def main():
    init_db()
    os.makedirs(DATA_DIR, exist_ok=True)
    
    scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
    # 每月 15 日凌晨 3 点执行
    scheduler.add_job(delete_last_month_data, CronTrigger(day=15, hour=3, minute=0))
    scheduler.start()

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler('start', start_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CommandHandler("mylogs", mylogs_cmd))
    app.add_handler(MessageHandler(filters.TEXT | filters.PHOTO, handle_message))

    print("🤖 Bot 正在运行...")
    app.run_polling()

if __name__ == "__main__":
    main()
