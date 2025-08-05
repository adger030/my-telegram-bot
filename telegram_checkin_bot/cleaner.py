import os
from datetime import datetime, timedelta
import pytz
from sqlalchemy import text
from db_pg import engine
import cloudinary
import cloudinary.uploader

def delete_last_month_data():
    now = datetime.now(pytz.timezone("Asia/Shanghai"))
    first_day_this_month = now.replace(day=1)
    last_day_last_month = first_day_this_month - timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)

    start_str = first_day_last_month.strftime('%Y-%m-%d')
    end_str = last_day_last_month.strftime('%Y-%m-%d')

    print(f"🧹 清理数据：{start_str} - {end_str}")
    delete_messages_and_images(start_str, end_str)

def delete_messages_and_images(start_date: str, end_date: str):
    with engine.begin() as conn:
        # 1️⃣ 查询图片URL（假设图片链接存储在 content 字段并包含 cloudinary）
        result = conn.execute(
            text("""
                SELECT content FROM messages
                WHERE timestamp >= :start_date AND timestamp <= :end_date
                  AND content LIKE 'https://res.cloudinary.com/%'
            """),
            {
                "start_date": f"{start_date} 00:00:00",
                "end_date": f"{end_date} 23:59:59"
            }
        )

        image_urls = [row[0] for row in result]

        # 2️⃣ 删除 Cloudinary 图片
        for url in image_urls:
            try:
                public_id = extract_cloudinary_public_id(url)
                if public_id:
                    cloudinary.uploader.destroy(public_id)
                    print(f"🗑 已删除 Cloudinary 图片: {public_id}")
            except Exception as e:
                print(f"⚠️ 删除 Cloudinary 图片失败: {url} - {e}")

        # 3️⃣ 删除数据库记录
        conn.execute(
            text("""
                DELETE FROM messages
                WHERE timestamp >= :start_date AND timestamp <= :end_date
            """),
            {
                "start_date": f"{start_date} 00:00:00",
                "end_date": f"{end_date} 23:59:59"
            }
        )
        print(f"✅ 已删除数据库记录：{start_date} 到 {end_date}")

def extract_cloudinary_public_id(url: str):
    """提取 Cloudinary public_id"""
    if "cloudinary.com" not in url:
        return None
    parts = url.split("/")
    try:
        idx = parts.index("upload")
        public_id_with_ext = "/".join(parts[idx + 1:])
        public_id = os.path.splitext(public_id_with_ext)[0]
        return public_id
    except Exception:
        return None
