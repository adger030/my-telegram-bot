import os
import asyncio
from datetime import datetime, timedelta, timezone
from telegram import Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.constants import ChatAction
from apscheduler.schedulers.background import BackgroundScheduler
from dateutil.parser import parse
from apscheduler.triggers.cron import CronTrigger
from collections import defaultdict

from cleaner import delete_last_month_data
from config import TOKEN, KEYWORDS, ADMIN_IDS, DATA_DIR
from db_pg import init_db, has_user_checked_keyword_today, save_message, delete_old_data, get_user_month_logs, get_user_logs, save_shift, get_user_name, set_user_name
from export import export_messages
from upload_image import upload_image

# 北京时区
BEIJING_TZ = timezone(timedelta(hours=8))

# 存储等待输入姓名的用户
WAITING_NAME = {} 

# 班次选项：使用代码 -> 完整名称映射
SHIFT_OPTIONS = {
    "F": "F班（12:00-21:00）",
    "G": "G班（13:00-22:00）",
    "H": "H班（14:00-23:00）",
    "I": "I班（15:00-00:00）"
}

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

    # 如果用户在等待输入姓名
    if username in WAITING_NAME:
        name = text.strip()
        if len(name) < 2:
            await msg.reply_text("❗ 姓名太短，请重新输入：")
            return
        set_user_name(username, name)
        WAITING_NAME.pop(username)
        await msg.reply_text(f"✅ 姓名已设置为：{name}\n现在可以发送 #上班打卡 或 #下班打卡 了。")
        return

    # 如果没姓名也没走 /start，强制提示
    if not get_user_name(username):
        WAITING_NAME[username] = True
        await msg.reply_text("👤 请先输入姓名后再打卡：")
        return
    
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

    now = datetime.now(BEIJING_TZ)
    name = get_user_name(username)
    
    if matched_keyword == "#上班打卡":
        # 保存上班打卡（先不含班次）
        save_message(username=username, name=name, content=image_url, timestamp=datetime.now(BEIJING_TZ), keyword=matched_keyword)
        keyboard = [[InlineKeyboardButton(name, callback_data=f"shift:{code}")] for code, name in SHIFT_OPTIONS.items()]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await msg.reply_text("✅ 上班打卡成功！请选择今天的班次：", reply_markup=reply_markup)
    else:
        # ✅ 下班打卡时，自动继承当天上班班次
        from db_pg import get_today_shift
        shift = get_today_shift(username)
        save_message(username=username, name=name, content=image_url, timestamp=datetime.now(BEIJING_TZ), keyword=matched_keyword, shift=shift)
        await msg.reply_text(f"✅ 下班打卡成功！{shift or '未选择'}")



# 新增：处理班次选择
async def shift_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    shift_code = query.data.split(":")[1]
    shift_name = SHIFT_OPTIONS.get(shift_code, shift_code)

    username = query.from_user.username or f"user{query.from_user.id}"

    # 保存班次
    save_shift(username, shift_name)

    await query.edit_message_text(f"✅ 你的班次已记录：{shift_name}")


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

    # 检查是否已有姓名
    if not get_user_name(username):
        WAITING_NAME[username] = True
        await update.message.reply_text("👤 欢迎首次使用，请输入你的姓名（例如：张三）：")
    else:
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

    now = datetime.now(BEIJING_TZ)
    start = (now.replace(day=1) - timedelta(days=1)).replace(day=1)
    next_month = (now.replace(day=28) + timedelta(days=4)).replace(day=1)
    end = next_month

    logs = get_user_logs(username, start, end)  # 返回 (timestamp, keyword, shift)
    if not logs:
        await update.message.reply_text("📭 本月暂无打卡记录。")
        return

    logs = sorted(logs, key=lambda x: parse(x[0]) if isinstance(x[0], str) else x[0])
    daily_map = defaultdict(dict)

    i = 0
    while i < len(logs):
        ts, kw, shift = logs[i]
        if isinstance(ts, str):
            ts = parse(ts)
        bj_time = ts.astimezone(BEIJING_TZ)

        date_key = bj_time.date()
        if kw == "#上班打卡":
            daily_map[date_key]["#上班打卡"] = bj_time
            daily_map[date_key]["shift"] = shift  # 记录班次

            # 查找接下来的10小时内的下班打卡
            j = i + 1
            while j < len(logs):
                ts2, kw2, _ = logs[j]
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
        shift_name = kw_map.get("shift", "未选择班次")
        missing = REQUIRED_KEYWORDS - set(k for k in kw_map if k.startswith("#"))
        date_str = day.strftime("%m月%d日")

        if not missing:
            reply += f"{idx}. {date_str} - {shift_name} - ✅ 已完成\n"
            complete_count += 1
        else:
            missing_str = "、".join(missing)
            reply += f"{idx}. {date_str} - {shift_name} - 缺少 {missing_str}\n"

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
    app.add_handler(CallbackQueryHandler(shift_callback, pattern=r"^shift:"))

    print("🤖 Bot 正在运行...")
    app.run_polling()

if __name__ == "__main__":
    main()
