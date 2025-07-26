import os
import asyncio
from datetime import datetime
from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ChatAction
from apscheduler.schedulers.background import BackgroundScheduler

from config import TOKEN, KEYWORDS, ADMIN_IDS, DATA_DIR
from db import init_db, has_user_checked_keyword_today, save_message, delete_old_data, get_user_month_logs
from export import export_messages


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
        await msg.reply_text("❗️必须附带一张图片。")
        return

    if has_user_checked_keyword_today(username, matched_keyword):
        await msg.reply_text(f"⚠️ 你今天已经提交过“{matched_keyword}”了哦！")
        return

    photo = msg.photo[-1]
    file = await photo.get_file()
    if file.file_size > 1024 * 1024:
        await msg.reply_text("❗️图片太大，不能超过1MB。")
        return

    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{today_str}_{username}_{matched_keyword}.jpg"
    filepath = os.path.join(DATA_DIR, filename)
    await file.download_to_drive(filepath)

    save_message(
        username=username,
        content=filepath,
        timestamp=datetime.now().isoformat(),
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

# 回复固定内容  
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = user.first_name or user.username or "朋友"

    # 文案 2：打卡说明
    welcome_text= (
        f"您好，{username}！欢迎使用MS部考勤机器人\n"
        "\n"
        "📌 使用说明：\n"
        "1️⃣ 向我发送关键词“#上班打卡”或“#下班打卡”并附带你的IP截图\n"
        "2️⃣ 每个关键词每天只能提交一次哦～\n"
        "\n"
        "举个🌰，如下👇"
    )

    # 文案 1：配图欢迎语
    instruction_text = (
        "#上班打卡\n"
    )

    image_url = "https://ibb.co/jkPmfwGF"  # ✅ 这里替换为你自己的欢迎图链接

    await update.message.reply_text(welcome_text)
    await asyncio.sleep(1)
    await update.message.reply_photo(
            photo=image_url,
            caption=instruction_text
        )
    
async def mylogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = user.username or f"user{user.id}"

    logs = get_user_month_logs(username)
    if not logs:
        await update.message.reply_text("📭 本月暂无打卡记录。")
        return

    reply = "📅 本月打卡记录：\n\n"
    for i, (timestamp, keyword) in enumerate(logs, start=1):
        date_str = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f").strftime("%m月%d日 %H:%M")
        reply += f"{i}. 🕒 {date_str} ｜{keyword}\n"

    await update.message.reply_text(reply)

def main():
    init_db()
    os.makedirs(DATA_DIR, exist_ok=True)

    scheduler = BackgroundScheduler()
    scheduler.add_job(delete_old_data, 'cron', day=15, hour=6)
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
