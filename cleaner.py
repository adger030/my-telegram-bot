import os
import pytz
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from db_pg import get_conn
from cloudinary import api as cloudinary_api


# ===========================
# 入口：删除上月数据（定时任务）
# ===========================
def delete_last_month_data():
    now = datetime.now(pytz.timezone("Asia/Shanghai"))
    first_day_this_month = now.replace(day=1)
    last_day_last_month = first_day_this_month - timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)

    start_str = first_day_last_month.strftime('%Y-%m-%d')
    end_str = last_day_last_month.strftime('%Y-%m-%d')

    print(f"🧹 清理数据：{start_str} - {end_str}")
    delete_messages_and_images(start_str, end_str)


# ===========================
# 主函数：批量删除指定时间区间内的消息记录与图片
# ===========================
def delete_messages_and_images(start_date: str, end_date: str, batch_size: int = 100, max_workers: int = 3):
    """
    批量删除指定日期内的  图片与数据库记录
    :param start_date: 开始日期 (YYYY-MM-DD)
    :param end_date: 结束日期 (YYYY-MM-DD)
    :param batch_size:  批量删除每次最多 100 张
    :param max_workers: 并行线程数，用于分批删除
    """
    with engine.begin() as conn:
        # 1️⃣ 查询 Cloudinary 图片 URL
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

        if not image_urls:
            print("⚠️ 指定日期内没有可删除的 Cloudinary 图片。")
        else:
            print(f"🔍 共找到 {len(image_urls)} 张图片，开始批量删除...")

            # 提取所有 public_id
            public_ids = [extract_cloudinary_public_id(url) for url in image_urls if extract_cloudinary_public_id(url)]
            print(f"📌 成功解析 {len(public_ids)} 个 Cloudinary public_id")

            start_time = time.time()

            # 2️⃣ 分批并行删除（每批最多 100 张）
            batches = [public_ids[i:i+batch_size] for i in range(0, len(public_ids), batch_size)]

            deleted_total = 0
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(batch_delete_cloudinary_images, batch): batch for batch in batches}
                for future in as_completed(futures):
                    deleted_count = future.result()
                    deleted_total += deleted_count

            elapsed = time.time() - start_time
            print(f"✅ 批量删除完成：{deleted_total}/{len(public_ids)} 张图片，耗时 {elapsed:.2f} 秒")

        # 3️⃣ 删除数据库中的 messages 记录
        result = conn.execute(
            text("""
                DELETE FROM messages
                WHERE timestamp >= :start_date AND timestamp <= :end_date
                RETURNING id
            """),
            {
                "start_date": f"{start_date} 00:00:00",
                "end_date": f"{end_date} 23:59:59"
            }
        )
        deleted_rows = len(result.fetchall())
        print(f"✅ 已删除数据库记录：{deleted_rows} 条（{start_date} 到 {end_date}）")


# ===========================
# 批量删除 Cloudinary 图片（API）
# ===========================
def batch_delete_cloudinary_images(public_id_list):
    """
    使用 Cloudinary API 一次性删除最多 100 张图片
    :param public_id_list: public_id 列表
    :return: 实际删除成功的数量
    """
    try:
        response = cloudinary.api.delete_resources(public_id_list)
        deleted = response.get("deleted", {})
        failed = response.get("failed", {})

        # 输出结果日志
        for pid, status in deleted.items():
            if status == "deleted":
                print(f"🗑 已删除图片: {pid}")
        for pid, error in failed.items():
            print(f"⚠️ 删除失败: {pid} - {error}")

        return len([s for s in deleted.values() if s == "deleted"])
    except Exception as e:
        print(f"❌ 批量删除失败: {e}")
        return 0


# ===========================
# 提取 Cloudinary public_id
# ===========================
def extract_cloudinary_public_id(url: str):
    """
    解析 Cloudinary 图片 URL 提取 public_id
    例如：
    https://res.cloudinary.com/demo/image/upload/v1691234567/folder/image.jpg
    返回 -> folder/image
    """
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
