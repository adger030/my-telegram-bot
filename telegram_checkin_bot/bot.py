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
from config import TOKEN, KEYWORDS, ADMIN_IDS, DATA_DIR, ADMIN_USERNAMES, LOGS_PER_PAGE
from db_pg import init_db, save_message, get_user_logs, save_shift, get_user_name, set_user_name, get_db, transfer_user_data
from upload_image import upload_image
from cleaner import delete_last_month_data
from sqlalchemy import text
import logging
from admin_tools import delete_range_cmd, userlogs_cmd, userlogs_page_callback, transfer_cmd, optimize_db, admin_makeup_cmd, export_cmd, export_images_cmd
from shift_manager import get_shift_options, get_shift_times, get_shift_times_short, list_shifts_cmd, edit_shift_cmd, delete_shift_cmd

# 仅保留 WARNING 及以上的日志
logging.getLogger("httpx").setLevel(logging.WARNING)  
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)

# ===========================
# 设置北京时区
# ===========================
BEIJING_TZ = timezone(timedelta(hours=8))
WAITING_NAME = {}  # 记录需要输入姓名的用户

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

# ===========================
# 处理纯文本消息
# ===========================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    username = msg.from_user.username or f"user{msg.from_user.id}"
    text = msg.text.strip()

    # 🚩 如果用户还没登记姓名，提示输入姓名
    if username in WAITING_NAME:
        if len(text) < 2:  # 姓名长度过短
            await msg.reply_text("❗ 姓名太短，请重新输入：")
            return
        try:
            set_user_name(username, text)  # 保存姓名
        except ValueError as e:
            await msg.reply_text(f"⚠️ {e}")
            return
        WAITING_NAME.pop(username)  # 从等待名单中移除
        await send_welcome(update.message, text)  # 发送欢迎信息
        return

    # 🚩 未登记姓名则提示先登记
    if not get_user_name(username):
        WAITING_NAME[username] = True
        await msg.reply_text("👤 请先输入姓名后再打卡：")
        return

    keyword = extract_keyword(text)  # 从消息中提取关键词

    if keyword:
        if keyword == "#上班打卡":
            # ✅ 检查是否重复打上班卡
            if has_user_checked_keyword_today_fixed(username, "#上班打卡"):
                await msg.reply_text("⚠️ 你今天已经打过上班卡了，不能重复打卡。")
                return
            await msg.reply_text("❗️请附带IP截图完成上班打卡。")

        elif keyword == "#补卡":
            # ✅ 检查是否已有上班卡，避免补卡冲突
            if has_user_checked_keyword_today_fixed(username, "#上班打卡"):
                await msg.reply_text("⚠️ 你今天已有上班打卡记录，不能再补卡。")
                return
            await msg.reply_text("📌 请发送“#补卡”并附IP截图完成补卡。")

        elif keyword == "#下班打卡":
            # ✅ 检查当天是否有上班卡
            if not has_user_checked_keyword_today_fixed(username, "#上班打卡"):
                await msg.reply_text("❗ 你今天还没打上班卡。若忘记上班卡，请补卡后再打下班卡。")
                return
            await msg.reply_text("❗️请附带IP截图完成下班打卡。")

# ===========================
# 处理带图片的打卡消息
# ===========================
async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    username = msg.from_user.username or f"user{msg.from_user.id}"
    caption = msg.caption or ""
    keyword = extract_keyword(caption)  # 从图片的文字说明提取关键词

    # 🚩 检查用户姓名是否登记
    if not get_user_name(username):
        WAITING_NAME[username] = True
        await msg.reply_text("👤 请重新输入工作姓名（英文大写）后再打卡：")
        return

    # 🚩 必须有关键词才能处理
    if not keyword:
        await msg.reply_text("❗ 图片必须附加关键词：#上班打卡 / #下班打卡 / #补卡")
        return

    # 🚩 下载图片并上传到存储（附限制：图片 ≤ 1MB）
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
        # ✅ 检查重复上班卡
        if has_user_checked_keyword_today_fixed(username, "#上班打卡"):
            await msg.reply_text("⚠️ 你今天已经打过上班卡了，不能重复打卡。")
            return

        # 保存上班卡并弹出班次选择按钮
        save_message(username=username, name=name, content=image_url, timestamp=now, keyword=keyword)
        keyboard = [[InlineKeyboardButton(v, callback_data=f"shift:{k}")] for k, v in get_shift_options().items()]
        await msg.reply_text("请选择今天的班次：", reply_markup=InlineKeyboardMarkup(keyboard))

    elif keyword == "#补卡":
        # ✅ 检查已有上班卡后禁止补卡
        if has_user_checked_keyword_today_fixed(username, "#上班打卡"):
            await msg.reply_text("⚠️ 你今天已有上班打卡记录，不能再补卡。")
            return

        # 进入补卡流程，保存补卡上下文信息
        context.user_data["makeup_data"] = {
            "username": username,
            "name": name,
            "image_url": image_url,
            "date": (now - timedelta(days=1)).date() if now.hour < 6 else now.date()  # 凌晨补卡算前一天
        }
        keyboard = [[InlineKeyboardButton(v, callback_data=f"makeup_shift:{k}")] for k, v in get_shift_options().items()]
        await msg.reply_text("请选择要补卡的班次：", reply_markup=InlineKeyboardMarkup(keyboard))

    elif keyword == "#下班打卡":
        # ✅ 检查重复下班卡
        if has_user_checked_keyword_today_fixed(username, "#下班打卡"):
            await msg.reply_text(f"⚠️ 你今天已经提交过“{keyword}”了哦！")
            return

        # ✅ 检查上班卡记录是否存在
        logs = get_user_logs(username, now - timedelta(days=1), now)
        last_check_in, last_shift = None, None
        for ts, kw, shift in reversed(logs):  # 倒序查找最近的上班打卡
            if kw == "#上班打卡":
                last_check_in = parse(ts) if isinstance(ts, str) else ts
                last_shift = shift.split("（")[0] if shift else None
                break

        if not last_check_in:
            await msg.reply_text("❗ 你今天还没有打上班卡，请先打卡或补卡。")
            return

        # 保存下班打卡
        save_message(username=username, name=name, content=image_url, timestamp=now, keyword=keyword, shift=last_shift)
        await msg.reply_text(f"✅ 下班打卡成功！班次：{last_shift or '未选择'}")

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
    await query.edit_message_text(f"✅ 上班打卡成功！班次：{shift_name}")

# ===========================
# 检查用户当天是否已经打过指定关键词的卡（修复版）
# ===========================
def has_user_checked_keyword_today_fixed(username, keyword):
    now = datetime.now(BEIJING_TZ)
    # 特殊规则：凌晨 0-6 点算前一天
    if keyword in ("#上班打卡", "#下班打卡") and now.hour < 6:
        ref_day = now - timedelta(days=1)
    else:
        ref_day = now

    # 定义查询的时间区间
    start = ref_day.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    # 查询数据库，获取当日打卡记录
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT timestamp FROM messages
            WHERE username=%s AND keyword=%s
            AND timestamp >= %s AND timestamp < %s
            ORDER BY timestamp DESC
        """, (username, keyword, start, end))
        rows = cur.fetchall()

    # 逐条验证
    for (ts,) in rows:
        ts_local = ts.astimezone(BEIJING_TZ)
        # 特殊情况：凌晨的下班卡忽略
        if keyword == "#下班打卡" and ts_local.hour < 6:
            continue
        return True
    return False

# ===========================
# 处理补上班卡的逻辑
# ===========================
async def handle_makeup_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    补上班卡功能流程：
    1. 判断日期（凌晨 0-6 点补卡算前一天）
    2. 检查该日期是否已有正常上班卡
    3. 没有则进入补卡流程：选择班次
    """
    msg = update.message
    username = msg.from_user.username or f"user{msg.from_user.id}"
    name = get_user_name(username)
    now = datetime.now(BEIJING_TZ)

    # 处理补卡参考日期（凌晨补卡算前一天）
    if now.hour < 6:
        ref_date = (now - timedelta(days=1)).date()
    else:
        ref_date = now.date()

    # 🚩 检查该日期是否已有正常上班卡
    start = datetime.combine(ref_date, datetime.min.time(), tzinfo=BEIJING_TZ)
    end = start + timedelta(days=1)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT shift FROM messages
            WHERE username=%s AND keyword=%s AND timestamp >= %s AND timestamp < %s
        """, (username, "#上班打卡", start, end))
        rows = cur.fetchall()

    # 如果已有记录，则不允许重复补卡
    if rows:
        await msg.reply_text(f"⚠️ {ref_date.strftime('%m月%d日')} 已有上班打卡记录，不能重复补卡。")
        return

    # ✅ 进入补卡流程：提示选择班次
    keyboard = [[InlineKeyboardButton(v, callback_data=f"makeup_shift:{k}")] for k, v in get_shift_options().items()]
    await msg.reply_text("请选择要补卡的班次：", reply_markup=InlineKeyboardMarkup(keyboard))

    # 记录补卡信息（日期将在后续回调中结合班次时间）
    context.user_data["makeup_data"] = {
        "username": username,
        "name": name,
        "date": ref_date
    }
    context.user_data.pop("awaiting_makeup", None)

# ===========================
# 处理补卡回调按钮（用户选择班次后执行）
# ===========================
async def makeup_shift_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()  # 先应答按钮点击事件
    data = context.user_data.get("makeup_data")  # 从上下文中取补卡信息
    if not data:
        # 若上下文中没有补卡数据，提示重新发起
        await query.edit_message_text("⚠️ 补卡信息丢失，请重新发送“#补卡”。")
        return

    shift_code = query.data.split(":")[1]  # 从回调数据中取班次代码（F/G/H/I）
    shift_name = get_shift_options()[shift_code]  # 转换为完整班次名
    shift_short = shift_name.split("（")[0]  # 提取班次简称（F班/G班/H班/I班）
    start_time, _ = get_shift_times_short()[shift_short]  # 取班次对应的上班时间
    punch_dt = datetime.combine(data["date"], start_time, tzinfo=BEIJING_TZ)  # 拼接补卡时间

    # 将补卡信息保存到数据库
    save_message(
        username=data["username"],
        name=data["name"],
        content=data["image_url"],  # 补卡时保存的截图 URL
        timestamp=punch_dt,
        keyword="#上班打卡",
        shift=shift_name + "（补卡）"
    )

    # 成功提示并清除上下文补卡信息
    await query.edit_message_text(f"✅ 补卡成功！班次：{shift_name}")
    context.user_data.pop("makeup_data", None)

# ===========================
# /mylogs 命令：查看本月打卡记录
# ===========================
async def mylogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    username = tg_user.username
    fallback_username = f"user{tg_user.id}"

    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start + timedelta(days=32)).replace(day=1)

    # 先尝试用真实 username 查，如果没有则用 user<id>
    logs = get_user_logs(username, start, end) if username else None
    if not logs:
        logs = get_user_logs(fallback_username, start, end)

    if not logs:
        await update.message.reply_text("📭 本月暂无打卡记录。")
        return

    # 转换时区 & 排序
    logs = [(parse(ts) if isinstance(ts, str) else ts, kw, shift) for ts, kw, shift in logs]  # 解析字符串时间
    logs = [(ts.astimezone(BEIJING_TZ), kw, shift) for ts, kw, shift in logs]  # 转换为北京时间
    logs = sorted(logs, key=lambda x: x[0])  # 按时间排序

    # 按天组合上下班打卡记录
    daily_map = defaultdict(dict)
    i = 0
    while i < len(logs):
        ts, kw, shift = logs[i]
        date_key = ts.date()
        if kw == "#下班打卡" and ts.hour < 6:  # 下班卡凌晨 0-6 点算前一天
            date_key = (ts - timedelta(days=1)).date()

        if kw == "#上班打卡":
            daily_map[date_key]["shift"] = shift
            daily_map[date_key]["#上班打卡"] = ts
            # 查找对应下班卡（12小时内）
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

    # ✅ 统计整月数据：正常打卡、异常（迟到/早退）、补卡
    total_complete = total_abnormal = total_makeup = 0
    for day, kw_map in daily_map.items():
        shift_full = kw_map.get("shift", "未选择班次")
        is_makeup = shift_full.endswith("（补卡）")  # 是否补卡
        shift_name = shift_full.split("（")[0]  # 去除补卡标记
        has_up = "#上班打卡" in kw_map
        has_down = "#下班打卡" in kw_map
        has_late = has_early = False

        if is_makeup:
            total_makeup += 1  # 补卡计数

        # 迟到判定：上班时间 > 班次规定时间
        if has_up and shift_name in get_shift_times_short():
            start_time, _ = get_shift_times_short()[shift_name]
            if kw_map["#上班打卡"].time() > start_time:
                has_late = True

        # 早退判定：下班时间 < 班次规定时间（I班跨天特殊判断）
        if has_down and shift_name in get_shift_times_short():
            _, end_time = get_shift_times_short()[shift_name]
            down_ts = kw_map["#下班打卡"]
            if shift_name == "I班" and down_ts.date() == day:  # I班若未跨天则早退
                has_early = True
            elif shift_name != "I班" and down_ts.time() < end_time:
                has_early = True

        # 计数逻辑
        if is_makeup:
            continue  # 补卡不计入正常/异常
        if has_late:
            total_abnormal += 1
        if has_early:
            total_abnormal += 1
        if not has_late and not has_early and (has_up or has_down):
            total_complete += 2 if has_up and has_down else 1  # 正常计次

    # 分页：每页 5 天
    all_days = sorted(daily_map)
    pages = [all_days[i:i + LOGS_PER_PAGE] for i in range(0, len(all_days), LOGS_PER_PAGE)]
    context.user_data["mylogs_pages"] = {
        "pages": pages,
        "daily_map": daily_map,
        "page_index": 0,
        "summary": (total_complete, total_abnormal, total_makeup)
    }

    await send_mylogs_page(update, context)  # 展示第一页

# ===========================
# 发送分页内容（安全版）
# ===========================
async def send_mylogs_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("mylogs_pages")
    if not data:
        # 会话过期
        if update.callback_query:
            await update.callback_query.edit_message_text("⚠️ 会话已过期，请重新使用 /mylogs")
        else:
            await update.message.reply_text("⚠️ 会话已过期，请重新使用 /mylogs")
        return

    pages, daily_map, page_index = data["pages"], data["daily_map"], data["page_index"]
    total_complete, total_abnormal, total_makeup = data["summary"]

    # ✅ 安全检查：防止索引越界
    if page_index < 0:
        page_index = 0
        data["page_index"] = 0
    elif page_index >= len(pages):
        page_index = len(pages) - 1
        data["page_index"] = page_index

    current_page_days = pages[page_index]
    reply = f"🗓️ 本月打卡情况（第 {page_index+1}/{len(pages)} 页）：\n\n"

    # 遍历当前页的每日记录
    for idx, day in enumerate(current_page_days, start=1 + page_index * LOGS_PER_PAGE):
        kw_map = daily_map[day]
        shift_full = kw_map.get("shift", "未选择班次")
        is_makeup = shift_full.endswith("（补卡）")
        shift_name = shift_full.split("（")[0]
        has_up = "#上班打卡" in kw_map
        has_down = "#下班打卡" in kw_map
        has_late = has_early = False

        # 迟到判定
        if has_up and shift_name in get_shift_times_short():
            start_time, _ = get_shift_times_short()[shift_name]
            if kw_map["#上班打卡"].time() > start_time:
                has_late = True

        # 早退判定
        if has_down and shift_name in get_shift_times_short():
            _, end_time = get_shift_times_short()[shift_name]
            down_ts = kw_map["#下班打卡"]
            if shift_name == "I班" and down_ts.date() == day:
                has_early = True
            elif shift_name != "I班" and down_ts.time() < end_time:
                has_early = True

        # 生成每日详情
        reply += f"{idx}. {day.strftime('%m月%d日')} - {shift_name}\n"
        if has_up:
            reply += f"   └─ #上班打卡：{kw_map['#上班打卡'].strftime('%H:%M')}{'（补卡）' if is_makeup else ''}{'（迟到）' if has_late else ''}\n"
        if has_down:
            down_ts = kw_map["#下班打卡"]
            next_day = down_ts.date() > day
            reply += f"   └─ #下班打卡：{down_ts.strftime('%H:%M')}{'（次日）' if next_day else ''}{'（早退）' if has_early else ''}\n"

    # 汇总信息
    reply += (
        f"\n🟢 正常：{total_complete} 次\n"
        f"🔴 异常（迟到/早退）：{total_abnormal} 次\n"
        f"🟡 补卡：{total_makeup} 次"
    )

    # 分页按钮
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

# ===========================
# 分页按钮回调（边界保护）
# ===========================
async def mylogs_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if "mylogs_pages" not in context.user_data:
        await query.edit_message_text("⚠️ 会话已过期，请重新使用 /mylogs")
        return

    pages_info = context.user_data["mylogs_pages"]
    total_pages = len(pages_info["pages"])

    # ✅ 页码安全调整
    if query.data == "mylogs_prev" and pages_info["page_index"] > 0:
        pages_info["page_index"] -= 1
    elif query.data == "mylogs_next" and pages_info["page_index"] < total_pages - 1:
        pages_info["page_index"] += 1

    await send_mylogs_page(update, context)


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

def main():
    init_db()  
    # ✅ 初始化数据库（创建表、索引等，确保运行环境准备就绪）

    os.makedirs(DATA_DIR, exist_ok=True)  
    # ✅ 确保数据存储目录存在，用于导出文件、缓存等

    # ===========================
    # 定时任务：自动清理上个月的数据
    # ===========================
    scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(delete_last_month_data, CronTrigger(day=15, hour=3))
    # 每月15号凌晨3点，执行 delete_last_month_data 清理旧数据
    scheduler.start()

    # ===========================
    # 初始化 Telegram Bot 应用
    # ===========================
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
    app.add_handler(CommandHandler("optimize", optimize_db))             # /optimize：数据库优化（管理员）
    app.add_handler(CommandHandler("delete_range", delete_range_cmd))    # /delete_range：删除指定时间范围的打卡记录（管理员）
    app.add_handler(CommandHandler("userlogs", userlogs_cmd))            # /userlogs @username：查看指定用户的考勤记录（管理员）

    # ===========================
    # ✅ 注册消息处理器（监听非命令消息）
    # ===========================
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))  # 普通文本消息（识别打卡关键词）
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))                   # 图片消息（识别打卡截图）

    # ===========================
    # ✅ 注册回调按钮处理器（InlineKeyboard）
    # ===========================
    app.add_handler(CallbackQueryHandler(shift_callback, pattern=r"^shift:"))               # 用户点击“选择上班班次”按钮
    app.add_handler(CallbackQueryHandler(makeup_shift_callback, pattern=r"^makeup_shift:")) # 用户点击“选择补卡班次”按钮
    app.add_handler(CallbackQueryHandler(mylogs_page_callback, pattern=r"^mylogs_(prev|next)$"))     # 用户点击“我的打卡记录”翻页按钮
    app.add_handler(CallbackQueryHandler(userlogs_page_callback, pattern=r"^userlogs_(prev|next)$")) # 管理员查看“指定用户打卡记录”翻页按钮

    # ===========================
    # 启动 Bot
    # ===========================
    print("🤖 Bot 正在运行...")
    app.run_polling()  # 开始长轮询，持续接收 Telegram 消息


if __name__ == "__main__":
    check_existing_instance()  # ✅ 单实例检查，防止重复运行
    main()                     # ✅ 启动主函数
