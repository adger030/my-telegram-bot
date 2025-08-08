from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from sqlalchemy import text
import cloudinary.api
import os
from datetime import datetime, timedelta
from collections import defaultdict
from dateutil.parser import parse
from db_pg import engine, get_user_logs, get_user_logs_by_name
from config import ADMIN_IDS, BEIJING_TZ, SHIFT_TIMES, LOGS_PER_PAGE, DATA_DIR
from export import export_excel, export_images
import pandas as pd
import shutil

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
    if len(args) not in (2, 3):
        await update.message.reply_text("⚠️ 用法：/delete_range YYYY-MM-DD YYYY-MM-DD [confirm]")
        return

    start_date, end_date = args[0], args[1]
    confirm = len(args) == 3 and args[2].lower() == "confirm"

    # 查询记录
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                SELECT id, content FROM messages
                WHERE timestamp >= :start_date AND timestamp <= :end_date
            """),
            {"start_date": f"{start_date} 00:00:00", "end_date": f"{end_date} 23:59:59"}
        )
        rows = result.fetchall()

    total_count = len(rows)
    image_urls = [r[1] for r in rows if r[1] and "cloudinary.com" in r[1]]
    public_ids = [extract_cloudinary_public_id(url) for url in image_urls if extract_cloudinary_public_id(url)]

    if not confirm:
        await update.message.reply_text(
            f"🔍 预览删除范围：{start_date} 至 {end_date}\n"
            f"📄 共 {total_count} 条记录，其中 {len(public_ids)} 张图片。\n\n"
            f"要确认删除，请使用：\n`/delete_range {start_date} {end_date} confirm`",
            parse_mode="Markdown"
        )
        return

    # 删除 Cloudinary 图片
    deleted_images = batch_delete_cloudinary(public_ids)

    # 删除数据库记录
    with engine.begin() as conn:
        delete_result = conn.execute(
            text("""
                DELETE FROM messages
                WHERE timestamp >= :start_date AND timestamp <= :end_date
                RETURNING id
            """),
            {"start_date": f"{start_date} 00:00:00", "end_date": f"{end_date} 23:59:59"}
        )
        deleted_count = len(delete_result.fetchall())

    await update.message.reply_text(
        f"✅ 删除完成！\n\n"
        f"📄 数据库记录：{deleted_count}/{total_count} 条\n"
        f"🖼 Cloudinary 图片：{deleted_images}/{len(public_ids)} 张\n"
        f"📅 范围：{start_date} ~ {end_date}"
    )


async def userlogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ 无权限，仅管理员可查看他人记录。")
        return

    if not context.args:
        await update.message.reply_text("⚠️ 用法：/userlogs @用户名 或 /userlogs 中文姓名")
        return

    # 1️⃣ 解析查询对象
    raw_input = context.args[0]
    is_username = raw_input.startswith("@")
    target_key = raw_input.lstrip("@") if is_username else raw_input

    # 2️⃣ 计算本月时间范围
    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start + timedelta(days=32)).replace(day=1)

    # 3️⃣ 获取记录
    if is_username:
        logs = get_user_logs(target_key, start, end)
    else:
        logs = get_user_logs_by_name(target_key, start, end)

    if not logs:
        await update.message.reply_text(f"📭 用户 {target_key} 本月暂无打卡记录。")
        return

    # 4️⃣ 转换时区 & 排序
    logs = [(parse(ts) if isinstance(ts, str) else ts, kw, shift) for ts, kw, shift in logs]
    logs = [(ts.astimezone(BEIJING_TZ), kw, shift) for ts, kw, shift in logs]
    logs = sorted(logs, key=lambda x: x[0])

    # 5️⃣ 按天组合上下班打卡
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

    # 6️⃣ 统计
    total_complete = total_abnormal = total_makeup = 0
    for day, kw_map in daily_map.items():
        shift_full = kw_map.get("shift", "未选择班次")
        is_makeup = shift_full.endswith("（补卡）")
        shift_name = shift_full.split("（")[0]
        has_up = "#上班打卡" in kw_map
        has_down = "#下班打卡" in kw_map
        has_late = has_early = False

        if is_makeup:
            total_makeup += 1

        if has_up and shift_name in SHIFT_TIMES:
            start_time, _ = SHIFT_TIMES[shift_name]
            if kw_map["#上班打卡"].time() > start_time:
                has_late = True

        if has_down and shift_name in SHIFT_TIMES:
            _, end_time = SHIFT_TIMES[shift_name]
            down_ts = kw_map["#下班打卡"]
            if shift_name == "I班" and down_ts.date() == day:
                has_early = True
            elif shift_name != "I班" and down_ts.time() < end_time:
                has_early = True

        if is_makeup:
            continue
        if has_late:
            total_abnormal += 1
        if has_early:
            total_abnormal += 1
        if not has_late and not has_early and (has_up or has_down):
            total_complete += 2 if has_up and has_down else 1

    # 7️⃣ 分页
    all_days = sorted(daily_map)
    pages = [all_days[i:i + LOGS_PER_PAGE] for i in range(0, len(all_days), LOGS_PER_PAGE)]
    context.user_data["userlogs_pages"] = {
        "pages": pages,
        "daily_map": daily_map,
        "page_index": 0,
        "summary": (total_complete, total_abnormal, total_makeup),
        "target_username": target_key,  # 无论是 username 还是 name，都记录
        "is_username": is_username      # 记录查询方式
    }

    await send_userlogs_page(update, context)  # 展示第一页


# ===========================
# 发送分页内容
# ===========================
async def send_userlogs_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data["userlogs_pages"]
    pages, daily_map, page_index = data["pages"], data["daily_map"], data["page_index"]
    total_complete, total_abnormal, total_makeup = data["summary"]
    target_username = data["target_username"]

    current_page_days = pages[page_index]
    reply = f"🗓️ {target_username} 本月打卡记录（第 {page_index+1}/{len(pages)} 页）：\n\n"

    for idx, day in enumerate(current_page_days, start=1 + page_index * LOGS_PER_PAGE):
        kw_map = daily_map[day]
        shift_full = kw_map.get("shift", "未选择班次")
        is_makeup = shift_full.endswith("（补卡）")
        shift_name = shift_full.split("（")[0]
        has_up = "#上班打卡" in kw_map
        has_down = "#下班打卡" in kw_map
        has_late = has_early = False

        if has_up and shift_name in SHIFT_TIMES:
            start_time, _ = SHIFT_TIMES[shift_name]
            if kw_map["#上班打卡"].time() > start_time:
                has_late = True

        if has_down and shift_name in SHIFT_TIMES:
            _, end_time = SHIFT_TIMES[shift_name]
            down_ts = kw_map["#下班打卡"]
            if shift_name == "I班" and down_ts.date() == day:
                has_early = True
            elif shift_name != "I班" and down_ts.time() < end_time:
                has_early = True

        reply += f"{idx}. {day.strftime('%m月%d日')} - {shift_name}\n"
        if has_up:
            reply += f"   └─ #上班打卡：{kw_map['#上班打卡'].strftime('%H:%M')}{'（补卡）' if is_makeup else ''}{'（迟到）' if has_late else ''}\n"
        if has_down:
            down_ts = kw_map["#下班打卡"]
            next_day = down_ts.date() > day
            reply += f"   └─ #下班打卡：{down_ts.strftime('%H:%M')}{'（次日）' if next_day else ''}{'（早退）' if has_early else ''}\n"

    reply += (
        f"\n🟢 正常：{total_complete} 次\n"
        f"🔴 异常（迟到/早退）：{total_abnormal} 次\n"
        f"🟡 补卡：{total_makeup} 次"
    )

    # 分页按钮
    buttons = []
    if page_index > 0:
        buttons.append(InlineKeyboardButton("⬅ 上一页", callback_data="userlogs_prev"))
    if page_index < len(pages) - 1:
        buttons.append(InlineKeyboardButton("➡ 下一页", callback_data="userlogs_next"))
    markup = InlineKeyboardMarkup([buttons]) if buttons else None

    if update.callback_query:
        await update.callback_query.edit_message_text(reply, reply_markup=markup)
    else:
        await update.message.reply_text(reply, reply_markup=markup)

# ===========================
# 分页按钮回调
# ===========================
async def userlogs_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if "userlogs_pages" not in context.user_data:
        await query.edit_message_text("⚠️ 会话已过期，请重新使用 /userlogs")
        return

    if query.data == "userlogs_prev":
        context.user_data["userlogs_pages"]["page_index"] -= 1
    elif query.data == "userlogs_next":
        context.user_data["userlogs_pages"]["page_index"] += 1

    await send_userlogs_page(update, context)

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
# 优化数据库索引命令，限制仅管理员可用
# ===========================
async def optimize_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.username not in ADMIN_USERNAMES:
        await update.message.reply_text("❌ 你无权限执行此命令")
        return

    try:
        from db_pg import engine  # 导入已有的数据库引擎
        sql = """
        CREATE INDEX IF NOT EXISTS messages_id_idx ON messages(id);  -- 创建索引以优化查询
        CLUSTER messages USING messages_id_idx;  -- 根据索引对数据表进行物理重排（聚簇）
        """
        with engine.begin() as conn:
            conn.execute(text(sql))  # 执行 SQL

        await update.message.reply_text("✅ 数据表已按 id 进行优化")
    except Exception as e:
        await update.message.reply_text("⚠️ 执行失败，请稍后再试")
        print("CLUSTER 执行失败：", e)

# ===========================
# 管理员补卡命令
# ===========================
async def admin_makeup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    用法：
    /admin_makeup @username YYYY-MM-DD 班次(F/G/H/I) [上班/下班]
    - 默认补“上班”，若指定“下班”则补下班卡
    """
    # 🚩 权限校验
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ 无权限，仅管理员可操作。")
        return

    # 🚩 参数检查
    if len(context.args) not in (3, 4):
        await update.message.reply_text(
            "⚠️ 用法：/admin_makeup @username YYYY-MM-DD 班次(F/G/H/I) [上班/下班]\n"
            "默认补上班，若要补下班需额外指定“下班”。"
        )
        return

    # 参数解析
    username_arg, date_str, shift_code = context.args[:3]
    username = username_arg.lstrip("@")
    shift_code = shift_code.upper()
    punch_type = context.args[3] if len(context.args) == 4 else "上班"

    # 🚩 校验班次与打卡类型
    if shift_code not in SHIFT_OPTIONS:
        await update.message.reply_text("⚠️ 班次无效，请使用 F/G/H/I。")
        return
    if punch_type not in ("上班", "下班"):
        await update.message.reply_text("⚠️ 类型必须是“上班”或“下班”。")
        return

    # 🚩 日期格式验证
    try:
        makeup_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        await update.message.reply_text("⚠️ 日期格式错误，应为 YYYY-MM-DD")
        return

    # 获取用户姓名
    name = get_user_name(username)
    if not name:
        await update.message.reply_text(f"⚠️ 用户 {username} 未登记姓名，无法补卡。")
        return

    # 班次与时间处理
    shift_name = SHIFT_OPTIONS[shift_code] + "（补卡）"
    shift_short = shift_name.split("（")[0]
    start_time, end_time = SHIFT_TIMES[shift_short]

    if punch_type == "上班":
        # 上班补卡逻辑
        punch_dt = datetime.combine(makeup_date, start_time, tzinfo=BEIJING_TZ)
        keyword = "#上班打卡"

        # 检查是否已有上班卡
        start = datetime.combine(makeup_date, datetime.min.time(), tzinfo=BEIJING_TZ)
        end = start + timedelta(days=1)
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT timestamp FROM messages
                WHERE username=%s AND keyword=%s AND timestamp >= %s AND timestamp < %s
            """, (username, keyword, start, end))
            if cur.fetchone():
                await update.message.reply_text(f"⚠️ {makeup_date.strftime('%m月%d日')} 已有上班打卡记录，禁止重复补卡。")
                return

    else:  
        # 下班补卡逻辑（跨天处理 I 班）
        if shift_short == "I班" and end_time == datetime.strptime("00:00", "%H:%M").time():
            punch_dt = datetime.combine(makeup_date + timedelta(days=1), end_time, tzinfo=BEIJING_TZ)
        else:
            punch_dt = datetime.combine(makeup_date, end_time, tzinfo=BEIJING_TZ)
        keyword = "#下班打卡"

        # 检查是否已有下班卡（I班需跨天检查）
        if shift_short == "I班":
            start = datetime.combine(makeup_date, datetime.min.time(), tzinfo=BEIJING_TZ)
            end = start + timedelta(days=2)
        else:
            start = datetime.combine(makeup_date, datetime.min.time(), tzinfo=BEIJING_TZ)
            end = start + timedelta(days=1)

        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT timestamp FROM messages
                WHERE username=%s AND keyword=%s AND timestamp >= %s AND timestamp < %s
            """, (username, keyword, start, end))
            if cur.fetchone():
                await update.message.reply_text(f"⚠️ {makeup_date.strftime('%m月%d日')} 已有下班打卡记录，禁止重复补卡。")
                return

    # ✅ 写入数据库
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
        # ✅ 无参数则默认导出本月
        start, end = get_default_month_range()

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
# 在线模式导出图片链接（不依赖 _fetch_data）
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
        start, end = get_default_month_range()

    status_msg = await update.message.reply_text("⏳ 正在生成图片链接列表，请稍等...")

    # 直接从数据库查询
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

    # 筛选图片记录
    photo_df = df[df["content"].str.contains(r"\.(?:jpg|jpeg|png|gif|webp)$", case=False, na=False)].copy()
    if photo_df.empty:
        await status_msg.delete()
        await update.message.reply_text("⚠️ 指定日期内没有图片。")
        return

    # 提取 public_id 并生成 Cloudinary URL
    def extract_public_id(url: str) -> str | None:
        match = re.search(r'/upload/(?:v\d+/)?(.+?)\.(?:jpg|jpeg|png|gif|webp)$', url, re.IGNORECASE)
        return match.group(1) if match else None

    photo_df["public_id"] = photo_df["content"].apply(extract_public_id)
    photo_df.dropna(subset=["public_id"], inplace=True)
    if photo_df.empty:
        await status_msg.delete()
        await update.message.reply_text("⚠️ 没有有效的 Cloudinary 图片链接。")
        return

    photo_df["url"] = photo_df["public_id"].apply(lambda pid: cloudinary.CloudinaryImage(pid).build_url())

    # 生成 HTML
    html_lines = [
        "<html><head><meta charset='utf-8'><title>图片导出</title></head><body>",
        f"<h2>图片导出：{start.strftime('%Y-%m-%d')} 至 {end.strftime('%Y-%m-%d')}</h2>"
    ]
    for date_str, group in photo_df.groupby(photo_df["timestamp"].dt.strftime("%Y-%m-%d")):
        html_lines.append(f"<h3>{date_str}</h3><ul>")
        for _, row in group.iterrows():
            ts_local = row["timestamp"].astimezone(BEIJING_TZ).strftime('%H:%M:%S')
            keyword = row.get("keyword", "无关键词") or "无关键词"
            name = row.get("name", "未知") or "未知"
            url = row["url"]
            html_lines.append(
                f"<li>{ts_local} - {keyword} - {name} - <a href='{url}' target='_blank'>查看图片</a></li>"
            )
        html_lines.append("</ul>")
    html_lines.append("</body></html>")

    # 保存 HTML 文件
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    export_dir = os.path.join(DATA_DIR, "links")
    os.makedirs(export_dir, exist_ok=True)
    html_path = os.path.join(export_dir, f"images_links_{start_str}_{end_str}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write("\n".join(html_lines))

    try:
        await status_msg.delete()
    except:
        pass

    # 发送 HTML 文件
    with open(html_path, "rb") as f:
        await update.message.reply_document(document=f, filename=os.path.basename(html_path), caption="✅ 图片链接列表已生成")

    # 清理临时文件
    os.remove(html_path)
