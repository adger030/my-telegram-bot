# ===========================
# 标准库
# ===========================
import os
import sys
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict
import calendar

# ===========================
# 第三方库
# ===========================
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes, ApplicationBuilder
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dateutil.parser import parse
import logging
import requests

# ===========================
# 项目内部模块
# ===========================
from config import TOKEN, KEYWORDS, ADMIN_IDS, DATA_DIR, ADMIN_USERNAMES, LOGS_PER_PAGE, BEIJING_TZ
from upload_image import upload_image
from cleaner import delete_last_month_data
from db_pg import (
    init_db, save_message, get_user_logs, save_shift, get_user_name, 
    set_user_name, get_db, transfer_user_data
)
from admin_tools import (
    delete_range_cmd, userlogs_cmd, userlogs_page_callback, transfer_cmd,
    admin_makeup_cmd, export_cmd, export_images_cmd, exportuser_cmd, userlogs_lastmonth_cmd
)
from shift_manager import (
    get_shift_options, get_shift_times, get_shift_times_short,
    list_shifts_cmd, edit_shift_cmd, delete_shift_cmd
)
from logs_utils import build_and_send_logs, send_logs_page

app = None  # 全局声明，初始为空

# ===========================
# 全局定时任务调度器
# ===========================
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

# 仅保留 WARNING 及以上的日志
logging.getLogger("httpx").setLevel(logging.WARNING)  
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)

# ===========================
# 记录需要输入姓名的用户
# ===========================
WAITING_NAME = {}  

# ===========================
# 提取关键词（例如 #上班打卡、#下班打卡 等）
# ===========================
def extract_keyword(text: str):
    text = text.strip().replace(" ", "")  # 去除空格
    for kw in KEYWORDS:
        if kw in text:
            return kw
    return None

# ===========================
# 发送欢迎信息和操作指南
# ===========================
async def send_welcome(update_or_msg, name):
    welcome_text = (
        f"您好，{name}！\n\n"
        "📌 使用说明：\n"
        "1️⃣ 向机器人发送“#上班打卡”或“#下班打卡”并附带IP截图；\n"
        "2️⃣ 上班打卡需要选择你的班次，提示打卡成功完成打卡；\n"
        "3️⃣ 若忘记上班打卡，请发送“#补卡”并附带IP截图（无法补下班卡）；\n"
        "4️⃣ 请务必在班次后1小时内完成下班打卡，超时无法打卡；\n"
	    "5️⃣ 重新发送/start指令，输入框下方展示打卡记录按钮；\n\n"
        "IP截图必须包含以下信息\n"
        "① 设备编码：本机序列号\n"
        "② 实时IP：指定网站内显示的IP\n"
        "③ 本地时间：电脑任务栏时间（需含月、日、时、分）\n\n"
        "<a href='https://www.ipaddress.my'>点击这里查看你的IP地址</a>\n\n"
    )
    await update_or_msg.reply_text(welcome_text, parse_mode="HTML")
    await asyncio.sleep(1)
    await update_or_msg.reply_photo(
        photo="https://i.postimg.cc/3xRMBbT4/photo-2025-07-28-15-55-19.jpg",
        caption="#上班打卡"
    )

# ===========================
# /start 命令：首次提示输入姓名，否则直接发送欢迎说明
# ===========================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    username = tg_user.username or f"user{tg_user.id}"
    if not get_user_name(username):  # 如果没登记过名字
        WAITING_NAME[username] = True
        await update.message.reply_text("👤 第一次打卡前请输入你的工作名（大写英文）：")
        return
    name = get_user_name(username)
    await send_welcome(update.message, name)

    # 🚀 动态按钮逻辑
    today = datetime.now(BEIJING_TZ)
    day = today.day
    _, last_day = calendar.monthrange(today.year, today.month)

    if 1 <= day <= 7:
        keyboard = [["🗓 本月打卡记录", "🗓 上月打卡记录"]]
    else:
        keyboard = [["🗓 本月打卡记录"]]

    reply_markup = ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,   # 按钮大小自适应
        one_time_keyboard=False # False 表示常驻
    )
    await update.message.reply_text("举个🌰，如上👆", reply_markup=reply_markup)
 #   await update.message.reply_sticker(
 #       sticker="CAACAgUAAxkBAAIdqWibWBP7RZ-_Gx_0UznjeAHuiz2HAAKlBwACsCjwVqRGdbv4kuN-NgQ"  # 贴纸 file_id
 #   )

# ===========================
# 处理纯文本消息
# ===========================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    username = msg.from_user.username or f"user{msg.from_user.id}"
    text = msg.text.strip()


    # 🚩 如果点击了按钮
    if text == "🗓 本月打卡记录":
        await mylogs_cmd(update, context)
        return
    elif text == "🗓 上月打卡记录":
        await lastmonth_cmd(update, context)
        return
		
    # 🚩 如果用户还没登记姓名
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
        await send_welcome(update.message, text)
        return

    # 🚩 未登记姓名
    if not get_user_name(username):
        WAITING_NAME[username] = True
        await msg.reply_text("👤 请重新输入工作姓名（英文大写）后再打卡：")
        return

    keyword = extract_keyword(text)

    if keyword:
        if keyword == "#上班打卡":
            if has_user_checked_keyword_today_fixed(username, "#上班打卡"):
                await msg.reply_text("⚠️ 今天已经打过上班卡了。")
                return
            await msg.reply_text("❗️请附带IP截图完成上班打卡。")

        elif keyword == "#补卡":
            # 🚫 已有上班卡，禁止补卡
            if has_user_checked_keyword_today_fixed(username, "#上班打卡"):
                await msg.reply_text("⚠️ 今天已有上班卡，不能再补卡。")
                return
            if has_user_checked_keyword_today_fixed(username, "#补卡"):
                await msg.reply_text("⚠️ 今天已经补过卡了。")
                return
            await msg.reply_text("📌 请发送“#补卡”并附IP截图完成补卡。")

        elif keyword == "#下班打卡":
            # 🚫 重复下班卡
            if has_user_checked_keyword_today_fixed(username, "#下班打卡"):
                await msg.reply_text("⚠️ 今天已经打过下班卡了。")
                return
            # 🚫 没有上班卡/补卡
            if not (has_user_checked_keyword_today_fixed(username, "#上班打卡") or
                    has_user_checked_keyword_today_fixed(username, "#补卡")):
                await msg.reply_text("❗ 今天还没有上班打卡，请先打卡或补卡。")
                return
            await msg.reply_text("❗️请附带IP截图完成下班打卡。")


# ===========================
# 处理带图片的打卡消息（保留原功能，新增 I班限制）
# ===========================
async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    username = msg.from_user.username or f"user{msg.from_user.id}"
    caption = msg.caption or ""
    keyword = extract_keyword(caption)

    if not get_user_name(username):
        WAITING_NAME[username] = True
        await msg.reply_text("👤 请重新输入工作姓名（英文大写）后再打卡：")
        return

    if not keyword:
        await msg.reply_text("❗ 图片必须附加关键词：#上班打卡 / #下班打卡 / #补卡")
        return

    # 下载图片（≤1MB）
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

    name = get_user_name(username)
    now = datetime.now(BEIJING_TZ)

    # ================== 根据关键词处理 ==================
    if keyword == "#上班打卡":
        # 原有：当天是否已打上班/补卡
        if has_user_checked_keyword_today_fixed(username, "#上班打卡"):
            await msg.reply_text("⚠️ 今天已经打过上班卡了。")
            return

        # 🔒 新增限制（I班跨天）：凌晨 0–6 点禁止再次打上班卡（视为前一日已上班）
        if 0 <= now.hour < 6:
            await msg.reply_text("⚠️ 已经打过上班卡，请勿重复。")
            return

        # 原有：立即保存上班卡，随后让用户选择班次
        save_message(username=username, name=name, content=image_url,
                     timestamp=now, keyword=keyword)
        keyboard = [[InlineKeyboardButton(v, callback_data=f"shift:{k}")]
                    for k, v in get_shift_options().items()]
        await msg.reply_text("请选择今天的班次：", reply_markup=InlineKeyboardMarkup(keyboard))

    elif keyword == "#补卡":
        # 原有：上班已有/补卡已有 的限制
        if has_user_checked_keyword_today_fixed(username, "#上班打卡"):
            await msg.reply_text("⚠️ 今天已有上班卡，不能再补卡。")
            return
        if has_user_checked_keyword_today_fixed(username, "#补卡"):
            await msg.reply_text("⚠️ 今天已经补过卡了。")
            return

        # 原有：凌晨补卡算前一天
        target_date = (now - timedelta(days=1)).date() if now.hour < 6 else now.date()
        context.user_data["makeup_data"] = {
            "username": username,
            "name": name,
            "image_url": image_url,
            "date": target_date
        }
        keyboard = [[InlineKeyboardButton(v, callback_data=f"makeup_shift:{k}")]
                    for k, v in get_shift_options().items()]
        await msg.reply_text("请选择要补卡的班次：", reply_markup=InlineKeyboardMarkup(keyboard))

    elif keyword == "#下班打卡":
        # 原有：当天是否已打过下班卡
        if has_user_checked_keyword_today_fixed(username, "#下班打卡"):
            await msg.reply_text("⚠️ 今天已经打过下班卡了。")
            return

        # 原有：必须先有上班卡/补卡
        if not (has_user_checked_keyword_today_fixed(username, "#上班打卡") or
                has_user_checked_keyword_today_fixed(username, "#补卡")):
            await msg.reply_text("❗ 今天还没有上班打卡，请先打卡或补卡。")
            return

        # 找到最近的上班/补卡记录
        logs = get_user_logs(username, now - timedelta(days=1), now)
        last_check_in, last_shift = None, None
        for ts, kw, shift in reversed(logs):
            if kw in ("#上班打卡", "#补卡"):
                last_check_in = parse(ts) if isinstance(ts, str) else ts
                last_shift = shift.split("（")[0] if shift else None
                break

        if not last_shift or last_shift not in get_shift_times_short():
            await msg.reply_text("⚠️ 未找到有效的班次，无法下班打卡。")
            return

        # 获取该班次的下班时间
        _, end_time = get_shift_times_short()[last_shift]
        shift_end_today = now.replace(hour=end_time.hour, minute=end_time.minute, second=0, microsecond=0)

        # 如果班次跨天（如 I班 15:00-00:00），需要把结束时间加一天
        if end_time < get_shift_times_short()[last_shift][0]:
            shift_end_today += timedelta(days=1)

        # 允许的打卡时间窗口：下班后 1 小时
        latest_allowed = shift_end_today + timedelta(hours=1)

        if now > latest_allowed:
            await msg.reply_text("⚠️ 已超过班次结束 1 小时，下班打卡无效。")
            return

        # 当日重复限制：防止同一天多次下班卡
        start_of_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        logs_today = get_user_logs(username, start_of_today, now)
        if any(kw2 == "#下班打卡" for _, kw2, _ in logs_today):
            await msg.reply_text("⚠️ 今天已经打过下班卡了。")
            return

		# 原有：保存下班卡
        save_message(username=username, name=name, content=image_url,timestamp=now, keyword=keyword, shift=last_shift)

   	    # 追加一个“仅按钮”的消息（无文字）
        buttons = [[InlineKeyboardButton("🗓 查看打卡记录", callback_data="mylogs_open")]]
        markup = InlineKeyboardMarkup(buttons)
        await msg.reply_text(f"✅ 下班打卡成功！班次：{last_shift or '未选择'}", reply_markup=markup)



# ===========================
# 选择上班班次回调
# ===========================
async def shift_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    username = query.from_user.username or f"user{query.from_user.id}"
    shift_code = query.data.split(":")[1]
    shift_name = get_shift_options()[shift_code]

    save_shift(username, shift_name)  # 保存班次

    new_text = f"✅ 上班打卡成功！班次：{shift_name}"
    if query.message.text != new_text:
        await query.edit_message_text(new_text)

# ===========================
# 检查用户当天是否已经打过指定关键词的卡（最终版）
# ===========================
def has_user_checked_keyword_today_fixed(username, keyword):
    """
    检查用户当天是否已经打过某种卡
    规则：
      - 上班卡和补卡视为同一类，只能打一次
      - 下班卡只能打一次
      - 凌晨 0-6 点的补卡/下班卡算前一天
    """
    now = datetime.now(BEIJING_TZ)

    # 关键：凌晨跨天处理
    if keyword in ("#下班打卡", "#补卡") and now.hour < 6:
        ref_day = now - timedelta(days=1)
    else:
        ref_day = now

    start = ref_day.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT keyword, timestamp FROM messages
            WHERE username=%s
              AND timestamp >= %s
              AND timestamp < %s
            ORDER BY timestamp ASC
        """, (username, start, end))
        rows = cur.fetchall()

    has_up = False   # 记录是否已有上班/补卡
    has_down = False # 记录是否已有下班

    for kw, ts in rows:
        ts_local = ts.astimezone(BEIJING_TZ)

        # 🚫 凌晨 0-6 点的补卡/下班算前一天，忽略掉
        if kw in ("#下班打卡", "#补卡") and ts_local.hour < 6:
            continue

        if kw in ("#上班打卡", "#补卡"):
            has_up = True
        elif kw == "#下班打卡":
            has_down = True

    # ---- 限制逻辑 ----
    if keyword in ("#上班打卡", "#补卡"):
        return has_up   # 只要已有上班或补卡，就禁止
    if keyword == "#下班打卡":
        return has_down # 只要已有下班，就禁止

    return False

# ===========================
# 处理补卡回调按钮（用户选择班次后执行）
# ===========================
async def makeup_shift_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()  # 先应答按钮点击事件
    data = context.user_data.get("makeup_data")  # 从上下文中取补卡信息
    if not data:
        await query.edit_message_text("⚠️ 补卡信息丢失，请重新发送“#补卡”。")
        return

    shift_code = query.data.split(":")[1]  # 从回调数据中取班次代码
    shift_name = get_shift_options()[shift_code]  # 转换为完整班次名
    shift_short = shift_name.split("（")[0]  # 提取班次简称（F班/I班等）

    # 当前时间（北京时间）
    now = datetime.now(BEIJING_TZ)

    # 🚫 时间窗口限制
    if shift_short == "I班" and (6 <= now.hour < 15):
        await query.edit_message_text("⚠️ 当前时间段禁止补 I 班（06:00-15:00 不能补卡）。")
        return
    if shift_short == "F班" and now.hour < 12:
        await query.edit_message_text("⚠️ 当前时间段禁止补 F 班（12:00 之前不能补卡）。")
        return

    # 获取班次上班时间
    start_time, _ = get_shift_times_short()[shift_short]
    punch_dt = datetime.combine(data["date"], start_time, tzinfo=BEIJING_TZ)

    # 保存补卡信息
    save_message(
        username=data["username"],
        name=data["name"],
        content=data["image_url"],  # 补卡截图 URL
        timestamp=punch_dt,
        keyword="#上班打卡",
        shift=shift_name + "（补卡）"
    )

    # 成功提示并清除上下文补卡信息
    await query.edit_message_text(f"✅ 补卡成功！班次：{shift_name}")
    context.user_data.pop("makeup_data", None)

# ===========================
# /lastmonth 命令
# ===========================
async def lastmonth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    username = tg_user.username
    fallback_username = f"user{tg_user.id}"

    now = datetime.now(BEIJING_TZ)
    year, month = (now.year, now.month - 1) if now.month > 1 else (now.year - 1, 12)

    start = datetime(year, month, 1, tzinfo=BEIJING_TZ)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=BEIJING_TZ)
    else:
        end = datetime(year, month + 1, 1, tzinfo=BEIJING_TZ)

    logs = get_user_logs(username, start, end) if username else None
    if not logs:
        logs = get_user_logs(fallback_username, start, end)

    await build_and_send_logs(update, context, logs, "上月打卡", key="lastmonth")

# ===========================
# /mylogs 命令
# ===========================
async def mylogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    username = tg_user.username
    fallback_username = f"user{tg_user.id}"

    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start + timedelta(days=32)).replace(day=1)

    logs = get_user_logs(username, start, end) if username else None
    if not logs:
        logs = get_user_logs(fallback_username, start, end)

    await build_and_send_logs(update, context, logs, "本月打卡", key="mylogs")


# ===========================
# 发送分页内容
# ===========================
async def mylogs_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if "mylogs_pages" not in context.user_data:
        await query.edit_message_text("⚠️ 会话已过期，请重新使用 /mylogs")
        return

    pages_info = context.user_data["mylogs_pages"]
    total_pages = len(pages_info["pages"])
    if query.data == "mylogs_prev" and pages_info["page_index"] > 0:
        pages_info["page_index"] -= 1
    elif query.data == "mylogs_next" and pages_info["page_index"] < total_pages - 1:
        pages_info["page_index"] += 1

    await send_logs_page(update, context, key="mylogs")

# ===========================
# 单实例检查：防止重复启动 Bot
# ===========================
def check_existing_instance():
    lock_file = "/tmp/bot.lock"
    if os.path.exists(lock_file):
        # 若锁文件存在，读取其中的 PID，检测进程是否存活
        with open(lock_file) as f:
            pid = int(f.read())
            if os.path.exists(f"/proc/{pid}"):
                print("⚠️ 检测到已有 Bot 实例在运行，退出。")
                sys.exit(1)

    # 创建锁文件，写入当前进程 PID
    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))

    # 注册退出时清理锁文件
    import atexit
    atexit.register(lambda: os.remove(lock_file) if os.path.exists(lock_file) else None)
    
# async def get_sticker_id(update, context):
#    if not update.message.sticker:
#        await update.message.reply_text("❌ 请发送一个贴纸")
#        return

#    file_id = update.message.sticker.file_id
#    await update.message.reply_text(
#        f"收到贴纸 file_id：<code>{file_id}</code>",
#        parse_mode="HTML"
#    )
        
def main():
    init_db()  
    # ✅ 初始化数据库（创建表、索引等，确保运行环境准备就绪）

    os.makedirs(DATA_DIR, exist_ok=True)  
    # ✅ 确保数据存储目录存在，用于导出文件、缓存等

    # ===========================
    # 定时任务：自动清理上个月的数据
    # ===========================
    scheduler.add_job(delete_last_month_data, CronTrigger(day=15, hour=1))
    # 每月15号凌晨1点，执行 delete_last_month_data 清理旧数据
    scheduler.start()
    # ===========================
    # 初始化 Telegram Bot 应用
    # ===========================
    global app
    app = Application.builder().token(TOKEN).build()

    # ===========================
    # ✅ 注册命令处理器（/命令）
    # ===========================

    app.add_handler(CommandHandler("list_shifts", list_shifts_cmd))      # /list_shifts：查看当前班次配置
    app.add_handler(CommandHandler("edit_shift", edit_shift_cmd))        # /edit_shift：管理员添加/修改班次
    app.add_handler(CommandHandler("delete_shift", delete_shift_cmd))    # /delete_shift：管理员删除班次
    app.add_handler(CommandHandler("start", start_cmd))                  # /start：欢迎信息 & 姓名登记
    app.add_handler(CommandHandler("mylogs", mylogs_cmd))                # /mylogs：查看本月打卡记录（分页）
    app.add_handler(CommandHandler("export", export_cmd))                # /export：导出考勤 Excel（管理员）
    app.add_handler(CommandHandler("export_images", export_images_cmd))  # /export_images：导出打卡截图 ZIP（管理员）
    app.add_handler(CommandHandler("admin_makeup", admin_makeup_cmd))    # /admin_makeup：管理员为员工补卡
    app.add_handler(CommandHandler("transfer", transfer_cmd))            # /transfer：用户数据迁移（改用户名时用）
    app.add_handler(CommandHandler("delete_range", delete_range_cmd))    # /delete_range：删除指定时间范围的打卡记录（管理员）
    app.add_handler(CommandHandler("userlogs", userlogs_cmd))            # /userlogs @username：查看指定用户的考勤记录（管理员）
    app.add_handler(CommandHandler("exportuser", exportuser_cmd)) # /exportuser 张三 2025-08-01 2025-08-25
    app.add_handler(CommandHandler("lastmonth", lastmonth_cmd))
    app.add_handler(CommandHandler("userlogs_lastmonth", userlogs_lastmonth_cmd))

    # ===========================
    # ✅ 注册消息处理器（监听非命令消息）
    # ===========================
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))  # 普通文本消息（识别打卡关键词）
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))                   # 图片消息（识别打卡截图）
    # 监听所有贴纸消息
   # app.add_handler(MessageHandler(filters.Sticker.ALL, get_sticker_id))

    # ===========================
    # ✅ 注册回调按钮处理器（InlineKeyboard）
    # ===========================
    app.add_handler(CallbackQueryHandler(shift_callback, pattern=r"^shift:"))               # 用户点击“选择上班班次”按钮
    app.add_handler(CallbackQueryHandler(makeup_shift_callback, pattern=r"^makeup_shift:")) # 用户点击“选择补卡班次”按钮
    app.add_handler(CallbackQueryHandler(mylogs_page_callback, pattern=r"^mylogs_(prev|next)$"))     # 用户点击“我的打卡记录”翻页按钮
    app.add_handler(CallbackQueryHandler(userlogs_page_callback, pattern=r"^userlogs_(prev|next)$")) # 管理员查看“指定用户打卡记录”翻页按钮
    app.add_handler(CallbackQueryHandler(mylogs_cmd, pattern="^mylogs_open$"))

    # ===========================
    # 启动 Bot
    # ===========================
    print("🤖 Bot 启动时间:", datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"))
    app.run_polling()  # 开始长轮询，持续接收 Telegram 消息


if __name__ == "__main__":
    check_existing_instance()  # ✅ 单实例检查，防止重复运行
    main()                     # ✅ 启动主函数
