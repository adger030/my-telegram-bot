# admin_tools.py
from telegram import Update
from telegram.ext import ContextTypes
from sqlalchemy import text
import cloudinary.api
from db_pg import engine
from config import ADMIN_IDS
import os

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
