import os
import re
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import cloudinary.api
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from sqlalchemy import text
from dateutil.parser import parse

from db_pg import engine, get_user_logs, get_user_logs_by_name, get_conn, get_user_name, save_message, transfer_user_data
from config import ADMIN_IDS, BEIJING_TZ, LOGS_PER_PAGE, DATA_DIR
from export import export_excel, export_user_excel
from shift_manager import get_shift_options, get_shift_times_short
from logs_utils import build_and_send_logs, send_logs_page

# ===========================
# 管理员删除数据
# ===========================
# 提取 Cloudinary public_id
def extract_cloudinary_public_id(url: str) -> str | None:
    """
    提取 Cloudinary public_id，支持多级目录。
    e.g. https://res.cloudinary.com/demo/image/upload/v123456/folder/image.jpg
         -> folder/image
    """
    if "cloudinary.com" not in url:
        return None
    try:
        # 去掉 query 参数
        url = url.split("?")[0]
        parts = url.split("/upload/")
        if len(parts) < 2:
            return None
        path = parts[1]
        # 去掉版本号 vXXXX
        path_parts = path.split("/")
        if path_parts[0].startswith("v") and path_parts[0][1:].isdigit():
            path_parts = path_parts[1:]
        public_id_with_ext = "/".join(path_parts)
        public_id = os.path.splitext(public_id_with_ext)[0]
        return public_id
    except Exception as e:
        print(f"⚠️ public_id 提取失败: {url} -> {e}")
        return None

# 批量删除 Cloudinary
def batch_delete_cloudinary(public_ids: list, batch_size=100):
    deleted_total = 0
    for i in range(0, len(public_ids), batch_size):
        batch = public_ids[i:i + batch_size]
        try:
            response = cloudinary.api.delete_resources(batch)
            deleted = response.get("deleted", {})
            failed = response.get("failed", {})

            deleted_total += sum(1 for v in deleted.values() if v == "deleted")

            for pid, error in failed.items():
                print(f"⚠️ 删除失败: {pid} - {error}")
        except Exception as e:
            print(f"❌ 批量删除失败: {e}")
    return deleted_total

# 管理员删除命令
async def delete_range_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ 无权限！仅管理员可执行此命令。")
        return

    args = context.args
    if len(args) not in (2, 3, 4):
        await update.message.reply_text("⚠️ 用法：/delete_range YYYY-MM-DD YYYY-MM-DD [username] [confirm]")
        return

    start_date, end_date = args[0], args[1]
    username = None
    confirm = False

    # 判断参数是否有 username 或 confirm
    if len(args) == 3:
        if args[2].lower() == "confirm":
            confirm = True
        else:
            username = args[2]
    elif len(args) == 4:
        username = args[2]
        confirm = args[3].lower() == "confirm"

    # 校验日期格式
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        await update.message.reply_text("⚠️ 日期格式错误，请使用 YYYY-MM-DD")
        return

    # 查询记录
    query = """
        SELECT id, content FROM messages
        WHERE timestamp >= :start_date AND timestamp <= :end_date
    """
    params = {"start_date": f"{start_date} 00:00:00", "end_date": f"{end_date} 23:59:59"}

    if username:
        query += " AND username = :username"
        params["username"] = username

    with engine.begin() as conn:
        result = conn.execute(text(query), params)
        rows = result.fetchall()

    total_count = len(rows)
    image_urls = [r[1] for r in rows if r[1] and "cloudinary.com" in r[1]]
    public_ids = [extract_cloudinary_public_id(url) for url in image_urls if extract_cloudinary_public_id(url)]

    if not confirm:
        await update.message.reply_text(
            f"🔍 预览删除范围：{start_date} 至 {end_date}\n"
            f"👤 用户：{username or '所有用户'}\n"
            f"📄 共 {total_count} 条记录，其中 {len(public_ids)} 张图片。\n\n"
            f"要确认删除，请使用：\n`/delete_range {start_date} {end_date} {username or ''} confirm`",
            parse_mode="Markdown"
        )
        return

    # 删除 Cloudinary 图片
    deleted_images = 0
    if public_ids:
        deleted_images = batch_delete_cloudinary(public_ids)

    # 删除数据库记录
    delete_query = """
        DELETE FROM messages
        WHERE timestamp >= :start_date AND timestamp <= :end_date
    """
    if username:
        delete_query += " AND username = :username"

    with engine.begin() as conn:
        delete_result = conn.execute(text(delete_query + " RETURNING id"), params)
        deleted_count = len(delete_result.fetchall())

    await update.message.reply_text(
        f"✅ 删除完成！\n\n"
        f"👤 用户：{username or '所有用户'}\n"
        f"📄 数据库记录：{deleted_count}/{total_count} 条\n"
        f"🖼 Cloudinary 图片：{deleted_images}/{len(public_ids)} 张\n"
        f"📅 范围：{start_date} ~ {end_date}"
    )
    
# ===========================
# /userlogs_lastmonth 命令
# ===========================
async def userlogs_lastmonth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ 权限不足")
        return

    if not context.args:
        await update.message.reply_text("⚠️ 用法：/userlogs_lastmonth @用户名 或 中文姓名")
        return

    raw_input = context.args[0]
    is_username = raw_input.startswith("@")
    target_key = raw_input.lstrip("@") if is_username else raw_input

    now = datetime.now(BEIJING_TZ)
    # 上个月年月
    if now.month == 1:
        year, month = now.year - 1, 12
    else:
        year, month = now.year, now.month - 1

    # 上个月第一天、本月第一天
    first_day_prev = datetime(year, month, 1, tzinfo=BEIJING_TZ)
    first_day_this = datetime(now.year, now.month, 1, tzinfo=BEIJING_TZ)

    # 查询范围：上个月 1号 00:00 → 本月 1号 01:00
    start = first_day_prev.replace(hour=0, minute=0, second=0, microsecond=0)
    end = first_day_this.replace(hour=1, minute=0, second=0, microsecond=0)

    # 获取日志
    logs = get_user_logs(target_key, start, end) if is_username else get_user_logs_by_name(target_key, start, end)

    # ✅ 保存 key
    await build_and_send_logs(
        update,
        context,
        logs,
        f"{target_key} 上月打卡",
        key=f"userlogs_lastmonth:{target_key}"
    )


# ===========================
# /userlogs 命令（本月）
# ===========================
async def userlogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ 无权限，仅管理员可查看他人记录。")
        return

    if not context.args:
        await update.message.reply_text("⚠️ 用法：/userlogs @用户名 或 中文姓名")
        return

    raw_input = context.args[0]
    is_username = raw_input.startswith("@")
    target_key = raw_input.lstrip("@") if is_username else raw_input

    now = datetime.now(BEIJING_TZ)

    # ===== 查询范围 =====
    # 本月第一天 01:00
    first_day_this = now.replace(day=1, hour=1, minute=0, second=0, microsecond=0)
    # 下个月第一天 01:00
    first_day_next = (first_day_this + timedelta(days=32)).replace(day=1, hour=1, minute=0, second=0, microsecond=0)

    start = first_day_this
    end = first_day_next

    # 获取日志
    logs = get_user_logs(target_key, start, end) if is_username else get_user_logs_by_name(target_key, start, end)

    # ✅ 保存 key
    await build_and_send_logs(
        update,
        context,
        logs,
        f"{target_key} 本月打卡",
        key=f"userlogs:{target_key}"
    )

# ===========================
# 翻页回调（统一支持 userlogs 和 userlogs_lastmonth）
# ===========================
async def userlogs_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # 从 callback_data 拿到前缀（userlogs / userlogs_lastmonth）
    prefix = query.data.split("_")[0]  

    pages_info = context.user_data.get(f"{prefix}_pages")
    if not pages_info:
        await query.edit_message_text("⚠️ 会话已过期，请重新使用 /userlogs 或 /userlogs_lastmonth")
        return

    total_pages = len(pages_info["pages"])
    if query.data.endswith("_prev") and pages_info["page_index"] > 0:
        pages_info["page_index"] -= 1
    elif query.data.endswith("_next") and pages_info["page_index"] < total_pages - 1:
        pages_info["page_index"] += 1

    # 用 prefix 作为 key 传回去
    await send_logs_page(update, context, key=prefix)




# ===========================
# 用户数据迁移命令：/transfer <userA> <userB>
# ===========================
async def transfer_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """管理员命令：迁移 userA 的所有打卡记录到 userB"""
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ 无权限！")
        return

    if len(context.args) != 2:
        await update.message.reply_text("用法：/transfer <userA> <userB>")
        return

    user_a, user_b = context.args
    try:
        transfer_user_data(user_a, user_b)  # 执行迁移
        await update.message.reply_text(f"✅ 已将 {user_a} 的数据迁移到 {user_b}")
    except ValueError as e:
        await update.message.reply_text(f"⚠️ {e}")
    except Exception as e:
        await update.message.reply_text(f"❌ 迁移失败：{e}")

# ===========================
# 管理员补卡命令
# ===========================
async def admin_makeup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    用法：
    /admin_makeup @username YYYY-MM-DD 班次代码(F/G/H/I/...) [上班/下班]
    （在你的原代码基础上：补下班卡严格使用班次结束时间整点）
    """
    # 权限校验
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ 无权限，仅管理员可操作。")
        return

    # 参数检查
    if len(context.args) not in (3, 4):
        await update.message.reply_text(
            "⚠️ 用法：/admin_makeup @用户名 YYYY-MM-DD 班次代码 [上班/下班]\n"
            "默认补上班，若要补下班需额外指定“下班”。"
        )
        return

    username_arg, date_str, shift_code = context.args[:3]
    username = username_arg.lstrip("@")
    shift_code = shift_code.upper()
    punch_type = context.args[3] if len(context.args) == 4 else "上班"

    # 班次校验
    shift_options = get_shift_options()
    if shift_code not in shift_options:
        await update.message.reply_text(f"⚠️ 班次代码无效，可用班次：{', '.join(shift_options.keys())}")
        return
    if punch_type not in ("上班", "下班"):
        await update.message.reply_text("⚠️ 类型必须是“上班”或“下班”。")
        return

    # 日期校验
    try:
        makeup_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        await update.message.reply_text("⚠️ 日期格式错误，应为 YYYY-MM-DD")
        return

    # 用户姓名（可改为 get_user_name(username)）
    name = get_user_name(username) or username

    # 获取班次时间（从内存 map）
    shift_name = shift_options[shift_code] + "（补卡）"
    shift_short = shift_name.split("（")[0]
    shift_times_map = get_shift_times_short()
    if shift_short not in shift_times_map:
        await update.message.reply_text(f"⚠️ 班次 {shift_short} 未配置上下班时间")
        return
    start_time, end_time = shift_times_map[shift_short]  # datetime.time objects

    # helper: 构造明确的 tz-aware datetime（确保整点、秒=0、微秒=0）
    def build_shift_datetime(date_obj, time_obj, add_day=False):
        if add_day:
            date_obj = date_obj + timedelta(days=1)
        return datetime(
            date_obj.year, date_obj.month, date_obj.day,
            time_obj.hour, time_obj.minute, 0, 0,
            tzinfo=BEIJING_TZ
        )

    # 生成打卡时间（确保精确到班次时分，秒=0，微秒=0）
    if punch_type == "上班":
        punch_dt = build_shift_datetime(makeup_date, start_time, add_day=False)
        keyword = "#上班打卡"
        check_days = 1
    else:
        # 下班：若 end_time <= start_time 视为跨天，时间设为 次日 end_time
        is_cross_day = (end_time <= start_time)
        punch_dt = build_shift_datetime(makeup_date, end_time, add_day=is_cross_day)
        keyword = "#下班打卡"
        check_days = 2 if is_cross_day else 1

    # DEBUG 日志：记录班次原始时间与计算结果，便于排查偏差
    logging.info(f"[admin_makeup_cmd DEBUG] user={username} shift_short={shift_short} "
                 f"start_time={start_time.isoformat()} end_time={end_time.isoformat()} "
                 f"makeup_date={makeup_date} punch_type={punch_type} punch_dt={punch_dt.isoformat()}")

    # 检查是否已有该类型打卡（按日期范围）
    start_range = datetime.combine(makeup_date, datetime.min.time(), tzinfo=BEIJING_TZ)
    end_range = start_range + timedelta(days=check_days)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT timestamp FROM messages
                WHERE username=%s AND keyword=%s AND timestamp >= %s AND timestamp < %s
            """, (username, keyword, start_range, end_range))
            if cur.fetchone():
                await update.message.reply_text(
                    f"⚠️ {makeup_date.strftime('%m月%d日')} 已有{punch_type}打卡记录，禁止重复补卡。"
                )
                return

    # 写入数据库（save_message 会保证时区一致）
    save_message(
        username=username,
        name=name,
        content=f"补卡（管理员-{punch_type}）",
        timestamp=punch_dt,
        keyword=keyword,
        shift=shift_name
    )

    await update.message.reply_text(
        f"✅ 管理员已为 {name}（{username}）补卡：\n"
        f"📅 日期：{makeup_date}\n"
        f"🏷 班次：{shift_name}\n"
        f"🔹 类型：{punch_type}\n"
        f"⏰ 时间：{punch_dt.strftime('%Y-%m-%d %H:%M')}"
    )
    
# ===========================
# 获取默认的月份范围
# ===========================
def get_default_month_range():
    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if now.month == 12:
        end = start.replace(year=now.year + 1, month=1)  # 跨年处理
    else:
        end = start.replace(month=now.month + 1)
    return start, end
    
# ===========================
# 获取本月 1 日 06:00 至 今日 的范围
# ===========================
def get_month_to_today_range():
    now = datetime.now(BEIJING_TZ)
    # 本月1日 01:00
    start = now.replace(day=1, hour=6, minute=0, second=0, microsecond=0)
    # 今日 23:59:59.999999
    end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start, end


# ===========================
# 导出 Excel 命令：/export [YYYY-MM-DD YYYY-MM-DD]
# ===========================
async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:  # 权限检查：仅管理员可用
        await update.message.reply_text("❌ 无权限，仅管理员可导出记录。")
        return

    tz = BEIJING_TZ
    args = context.args
    if len(args) == 2:
        # ✅ 解析日期参数：导出指定日期区间
        try:
            start = parse(args[0]).replace(tzinfo=tz, hour=0, minute=0, second=0, microsecond=0)
            end = parse(args[1]).replace(tzinfo=tz, hour=23, minute=59, second=59, microsecond=999999)
        except Exception:
            await update.message.reply_text("⚠️ 日期格式错误，请使用 /export YYYY-MM-DD YYYY-MM-DD")
            return
    else:
        # ✅ 无参数则默认导出本月1日至今日
        start, end = get_month_to_today_range()

    status_msg = await update.message.reply_text("⏳ 正在导出 Excel，请稍等...")
    file_path = export_excel(start, end)  # 调用导出函数，返回文件路径或云端 URL

    # 删除状态提示消息
    try:
        await status_msg.delete()
    except:
        pass

    # ✅ 导出结果处理
    if not file_path:
        await update.message.reply_text("⚠️ 指定日期内没有数据。")
        return

    if file_path.startswith("http"):  
        # 文件过大，已上传云端
        await update.message.reply_text(f"✅ 导出完成，文件过大已上传到云端：\n{file_path}")
    else:
        # 直接发送 Excel 文件并删除临时文件
        await update.message.reply_document(document=open(file_path, "rb"))
        os.remove(file_path)

# ===========================
# /exportuser 指令
# ===========================
async def exportuser_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ 无权限，仅管理员可导出用户考勤。")
        return

    if len(context.args) not in (1, 3):
        await update.message.reply_text(
            "⚠️ 用法：\n"
            "/exportuser 姓名 起始日期 结束日期\n"
            "📌 例：/exportuser 张三 2025-08-01 2025-08-25\n"
            "👉 只输入姓名时，默认导出本月 1 日到今天"
        )
        return

    # 解析参数
    user_name = context.args[0]
    if len(context.args) == 3:
        try:
            start_datetime = datetime.strptime(context.args[1], "%Y-%m-%d")
            end_datetime = datetime.strptime(context.args[2], "%Y-%m-%d")
            end_datetime = end_datetime.replace(hour=23, minute=59, second=59)
        except ValueError:
            await update.message.reply_text("❗ 日期格式错误，请用 YYYY-MM-DD 格式")
            return
    else:
        today = datetime.today()
        start_datetime = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_datetime = today.replace(hour=23, minute=59, second=59, microsecond=0)

    status_msg = await update.message.reply_text(f"⏳ 正在导出 {user_name} 的考勤数据，请稍候...")

    # 删除状态提示消息
    try:
        await status_msg.delete()
    except:
        pass

    # 调用导出函数
    file_path = export_user_excel(user_name, start_datetime, end_datetime)
    if not file_path:
        await update.message.reply_text(f"📭 {user_name} 在指定时间内没有打卡数据。")
        return

    # 发送文件
    try:
        with open(file_path, "rb") as f:
            await update.message.reply_document(f, filename=f"{user_name}_考勤详情.xlsx")
    except Exception as e:
        await update.message.reply_text(f"❌ 导出失败：{e}")

        
# ===========================
# 在线模式导出图片链接（美化 + 搜索筛选 + 日期折叠）
# ===========================
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
        start, end = get_month_to_today_range()

    status_msg = await update.message.reply_text("⏳ 正在生成图片链接列表，请稍等...")

    # 查询数据库
    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT timestamp, keyword, name, content
            FROM messages
            WHERE timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """, conn, params=(start, end))

    if df.empty:
        await status_msg.delete()
        await update.message.reply_text("⚠️ 指定日期内没有数据。")
        return

    # 过滤图片
    photo_df = df[df["content"].str.contains(r"\.(?:jpg|jpeg|png|gif|webp)$", case=False, na=False)].copy()
    if photo_df.empty:
        await status_msg.delete()
        await update.message.reply_text("⚠️ 指定日期内没有图片。")
        return

    # 提取 public_id
    def extract_public_id(url: str) -> str | None:
        match = re.search(r'/upload/(?:v\d+/)?(.+?)\.(?:jpg|jpeg|png|gif|webp)$', url, re.IGNORECASE)
        return match.group(1) if match else None

    photo_df["public_id"] = photo_df["content"].apply(extract_public_id)
    photo_df.dropna(subset=["public_id"], inplace=True)
    if photo_df.empty:
        await status_msg.delete()
        await update.message.reply_text("⚠️ 没有有效的 Cloudinary 图片链接。")
        return

    # 构建图片URL
    photo_df["url"] = photo_df["public_id"].apply(lambda pid: cloudinary.CloudinaryImage(pid).build_url())

    # HTML 头部（样式 + 搜索 + 折叠功能）
    html_lines = [
        "<!DOCTYPE html>",
        "<html><head><meta charset='utf-8'><title>图片导出</title>",
        "<style>",
        "body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }",
        "h2 { text-align: center; color: #333; }",
        ".search-box { text-align: center; margin-bottom: 20px; }",
        "input { padding: 8px; width: 300px; border-radius: 5px; border: 1px solid #ccc; }",
        ".date-block { background: white; margin-bottom: 20px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }",
        ".date-title { font-size: 18px; padding: 10px; background: #3b81cd; color: white; cursor: pointer; border-radius: 8px 8px 0 0; }",
        ".date-title:hover { background: #0056b3; }",
        "ul { list-style-type: none; padding: 10px; margin: 0; }",
        "li { padding: 5px 0; border-bottom: 1px solid #eee; }",
        "li:last-child { border-bottom: none; }",
        "a { color: #007bff; text-decoration: none; }",
        "a:hover { text-decoration: underline; }",
        ".hidden { display: none; }",
        "</style>",
        "<script>",
        "function filterList() {",
        "  var input = document.getElementById('searchInput').value.toLowerCase();",
        "  var items = document.querySelectorAll('li');",
        "  items.forEach(function(item) {",
        "    if (item.innerText.toLowerCase().includes(input)) {",
        "      item.style.display = '';",
        "    } else {",
        "      item.style.display = 'none';",
        "    }",
        "  });",
        "}",
        "function toggleList(id) {",
        "  var el = document.getElementById(id);",
        "  if (el.classList.contains('hidden')) {",
        "    el.classList.remove('hidden');",
        "  } else {",
        "    el.classList.add('hidden');",
        "  }",
        "}",
        "</script>",
        "</head><body>",
        f"<h2>图片导出：{start.strftime('%Y-%m-%d')} 至 {end.strftime('%Y-%m-%d')}</h2>",
        "<div class='search-box'><input type='text' id='searchInput' onkeyup='filterList()' placeholder='🔍 输入关键词、姓名或时间筛选...'></div>"
    ]

    # 生成日期分组 HTML（默认收起）
    for idx, (date_str, group) in enumerate(photo_df.groupby(photo_df["timestamp"].dt.strftime("%Y-%m-%d"))):
        list_id = f"list_{idx}"
        html_lines.append(f"<div class='date-block'>")
        html_lines.append(f"<div class='date-title' onclick=\"toggleList('{list_id}')\">{date_str} ▼</div>")
        html_lines.append(f"<ul id='{list_id}' class='hidden'>")
        for _, row in group.iterrows():
            ts_local = row["timestamp"].astimezone(BEIJING_TZ).strftime('%H:%M:%S')
            keyword = row.get("keyword", "无关键词") or "无关键词"
            name = row.get("name", "未知") or "未知"
            url = row["url"]
            html_lines.append(
                f"<li>{ts_local} - {keyword} - {name} - <a href='{url}' target='_blank'>查看图片</a></li>"
            )
        html_lines.append("</ul></div>")

    html_lines.append("</body></html>")

    # 保存 HTML
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    export_dir = os.path.join(DATA_DIR, "links")
    os.makedirs(export_dir, exist_ok=True)
    html_path = os.path.join(export_dir, f"图片记录_{start_str}_{end_str}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write("\n".join(html_lines))

    try:
        await status_msg.delete()
    except:
        pass

    # 发送 HTML
    with open(html_path, "rb") as f:
        await update.message.reply_document(document=f, filename=os.path.basename(html_path))

    os.remove(html_path)
