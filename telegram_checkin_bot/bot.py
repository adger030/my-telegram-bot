import os
import asyncio
from datetime import datetime, timedelta, timezone
from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ChatAction
from apscheduler.schedulers.background import BackgroundScheduler
from dateutil.parser import parse
from apscheduler.triggers.cron import CronTrigger
from collections import defaultdict

from cleaner import delete_last_month_data
from config import TOKEN, KEYWORDS, ADMIN_IDS, DATA_DIR
from db_pg import init_db, has_user_checked_keyword_today, save_message, delete_old_data, get_user_month_logs, get_user_logs  # 新增 get_user_logs 支持时间查询
from export import export_messages
from upload_image import upload_image

# 北京时区
BEIJING_TZ = timezone(timedelta(hours=8))

def extract_keyword(text: str):
    text = text.strip().replace(" ", "")  # 去掉空格
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
    matched_keyword = extract_keyword(text)

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

    today_str = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
    tmp_path = f"/tmp/{today_str}_{username}_{matched_keyword}.jpg"
    await file.download_to_drive(tmp_path)

    # 上传到 Cloudinary
    image_url = upload_image(tmp_path)

    # 删除临时文件
    try:
        os.remove(tmp_path)
    except Exception as e:
        print(f"⚠️ 删除临时文件失败：{e}")

    # 保存记录（使用北京时间）
    save_message(
        username=username,
        content=image_url,
        timestamp=datetime.now(BEIJING_TZ),
        keyword=matched_keyword
    )

    await msg.reply_text("✅ 打卡成功！")

async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ 你无权限使用此指令，仅管理员可导出记录。")
        return

    args = context.args
    if len(args) == 2:
        try:
            start = datetime.strptime(args[0], "%Y-%m-%d").replace(tzinfo=BEIJING_TZ)
            end = datetime.strptime(args[1], "%Y-%m-%d").replace(tzinfo=BEIJING_TZ) + timedelta(days=1)
        except ValueError:
            await update.message.reply_text("❗️日期格式错误，请使用 YYYY-MM-DD")
            return
    else:
        # 默认导出当前整月数据
        now = datetime.now(BEIJING_TZ)
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
        end = next_month

    file_path = export_messages(start, end)
    if not file_path:
        await update.message.reply_text("⚠️ 指定日期内没有数据。")
        return

    await update.message.reply_document(document=open(file_path, "rb"))
    os.remove(file_path)  # ✅ 发送后删除临时文件

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = user.first_name or user.username or "朋友"

    welcome_text = (
        f"您好，{username}！欢迎使用 MS 部考勤机器人\n"
        "\n"
        "📌 使用说明：\n"
        "1️⃣ 向我发送关键词“#上班打卡”或“#下班打卡”并附带你的IP截图；\n"
        "2️⃣ 下班打卡和上班打卡间隔不能超过10小时，否则下班信息不录入；\n"
        "3️⃣ 其他考勤问题请联系部门助理。\n"
        "\n"
        " <a href='https://www.ipaddress.my'>点击这里查看你的IP地址</a>\n"
        "\n"
        "举个🌰，如下👇"
    )

    instruction_text = "#上班打卡"
    image_url = "https://ibb.co/jkPmfwGF" 

    await update.message.reply_text(
            welcome_text,
            parse_mode="HTML"  # 指定使用 HTML 解析模式
    )
    await asyncio.sleep(1)
    await update.message.reply_photo(photo=image_url, caption=instruction_text)

REQUIRED_KEYWORDS = set(KEYWORDS)

async def mylogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = user.username or f"user{user.id}"

    # ✅ 支持跨月配对（取上个月最后一天到本月末）
    now = datetime.now(BEIJING_TZ)
    start = (now.replace(day=1) - timedelta(days=1)).replace(day=1)
    next_month = (now.replace(day=28) + timedelta(days=4)).replace(day=1)
    end = next_month

    logs = get_user_logs(username, start, end)
    if not logs:
        await update.message.reply_text("📭 本月暂无打卡记录。")
        return

    logs = sorted(logs, key=lambda x: parse(x[0]) if isinstance(x[0], str) else x[0])
    daily_map = defaultdict(dict)

    i = 0
    while i < len(logs):
        ts, kw = logs[i]
        if isinstance(ts, str):
            ts = parse(ts)
        bj_time = ts.astimezone(BEIJING_TZ)

        if kw == "#上班打卡":
            date_key = bj_time.date()
            daily_map[date_key]["#上班打卡"] = bj_time

            # 查找接下来的10小时内的#下班打卡
            j = i + 1
            while j < len(logs):
                ts2, kw2 = logs[j]
                if isinstance(ts2, str):
                    ts2 = parse(ts2)
                bj_time2 = ts2.astimezone(BEIJING_TZ)

                if kw2 == "#下班打卡" and timedelta(0) < (bj_time2 - bj_time) <= timedelta(hours=10):
                    daily_map[date_key]["#下班打卡"] = bj_time2
                    break
                j += 1
            i = j
        else:
            i += 1

    reply = "🗓️ 本月打卡情况（北京时间）：\n\n"
    complete_count = 0

    for idx, day in enumerate(sorted(daily_map), start=1):
        kw_map = daily_map[day]
        missing = REQUIRED_KEYWORDS - set(kw_map)
        date_str = day.strftime("%m月%d日")

        if not missing:
            reply += f"{idx}. {date_str} - ✅ 已完成\n"
            complete_count += 1
        else:
            missing_str = "、".join(missing)
            reply += f"{idx}. {date_str} - 缺少 {missing_str}\n"

        for kw in ["#上班打卡", "#下班打卡"]:
            if kw in kw_map:
                time_str = kw_map[kw].strftime("%H:%M")
                reply += f"   └─ {kw}：{time_str}\n"

    reply += f"\n✅ 本月完整打卡：{complete_count} 天"
    await update.message.reply_text(reply)

def main():
    init_db()
    os.makedirs(DATA_DIR, exist_ok=True)

    scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
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
