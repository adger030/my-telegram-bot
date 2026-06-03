import os
import time
import pytz
import logging
from datetime import datetime, timedelta

from sqlalchemy import text
from db_pg import engine

import cloudinary
import cloudinary.api


# ===========================
# 日志配置
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ===========================
# 入口：删除3个月前的数据（定时任务）
# ===========================
def delete_last_3months_data():
    now = datetime.now(pytz.timezone("Asia/Shanghai"))

    # 当前月1号
    first_day_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # ⭐ 计算“3个月前的月初”
    month = first_day_this_month.month - 3
    year = first_day_this_month.year

    if month <= 0:
        month += 12
        year -= 1

    cutoff_date = datetime(year, month, 1, tzinfo=first_day_this_month.tzinfo)

    start_str = "1970-01-01"
    end_str = (cutoff_date - timedelta(seconds=1)).strftime('%Y-%m-%d')

    logger.info(f"🧹 清理3个月前数据：<= {end_str}")

    delete_messages_and_images(start_str, end_str)


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

    logger.info(f"🧹 清理数据：{start_str} - {end_str}")
    delete_messages_and_images(start_str, end_str)


# ===========================
# 入口：只删除上月图片，保留打卡数据（定时任务）
# ===========================
def delete_last_month_images():
    """
    每月执行：将上个月所有打卡记录的图片从 Cloudinary 删除，
    并将数据库 content 字段置为 NULL。
    打卡时间、班次、关键词等数据完整保留，不受影响。
    打卡数据仍由 delete_last_3months_data 按3个月周期清理。
    """
    now = datetime.now(pytz.timezone("Asia/Shanghai"))
    first_day_this_month = now.replace(day=1)
    last_day_last_month = first_day_this_month - timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)

    start_str = first_day_last_month.strftime('%Y-%m-%d')
    end_str = last_day_last_month.strftime('%Y-%m-%d')

    logger.info(f"🖼 清理上月图片（保留打卡记录）：{start_str} - {end_str}")

    # 1️⃣ 查询图片 URL
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT content FROM messages
                WHERE timestamp >= :start_date AND timestamp <= :end_date
                  AND content LIKE 'https://res.cloudinary.com/%'
            """),
            {
                "start_date": f"{start_str} 00:00:00",
                "end_date": f"{end_str} 23:59:59"
            }
        )
        image_urls = [row[0] for row in result]

    if not image_urls:
        logger.warning("⚠️ 上月没有可清理的图片。")
        return

    logger.info(f"🔍 共找到 {len(image_urls)} 张图片")

    # 2️⃣ 提取 public_id 并删除 Cloudinary 图片
    public_ids = []
    for url in image_urls:
        pid = extract_cloudinary_public_id(url)
        if pid:
            public_ids.append(pid)

    if not public_ids:
        logger.warning("⚠️ 未解析到有效 public_id")
        return

    logger.info(f"📌 成功解析 {len(public_ids)} 个 public_id")

    deleted_total = 0
    failed_ids = []
    batch_size = 100

    for i in range(0, len(public_ids), batch_size):
        batch = public_ids[i:i + batch_size]
        logger.info(f"🚀 删除图片批次 {i//batch_size + 1}，数量 {len(batch)}")
        success_count, failed_batch = delete_batch_with_retry(batch, max_retries=3)
        deleted_total += success_count
        failed_ids.extend(failed_batch)
        time.sleep(0.4)

    logger.info(f"🎯 Cloudinary 删除完成：{deleted_total}/{len(public_ids)}")

    # 3️⃣ 将已成功删除图片的 content 置为 NULL（打卡记录行保留）
    if deleted_total > 0:
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                    UPDATE messages
                    SET content = NULL
                    WHERE timestamp >= :start_date AND timestamp <= :end_date
                      AND content LIKE 'https://res.cloudinary.com/%'
                """),
                {
                    "start_date": f"{start_str} 00:00:00",
                    "end_date": f"{end_str} 23:59:59"
                }
            )
            updated_rows = result.rowcount

        logger.info(f"✅ 已清空 {updated_rows} 条记录的图片链接（打卡数据保留）")

    if failed_ids:
        logger.error(f"❌ 仍有 {len(failed_ids)} 张图片删除失败，对应记录保持不变")
        for fid in failed_ids:
            logger.error(f"   失败 public_id: {fid}")


# ===========================
# 主函数：稳定删除流程
# ===========================
def delete_messages_and_images(start_date: str, end_date: str, batch_size: int = 100, max_retries: int = 3):
    """
    稳定删除流程：
    1. 查询 public_id
    2. 顺序批量删除 Cloudinary（带重试）
    3. 删除成功后再删除数据库记录
    """

    # ===========================
    # 1️⃣ 查询图片 URL（短事务）
    # ===========================
    with engine.connect() as conn:
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
        logger.warning("⚠️ 指定日期内没有可删除的 Cloudinary 图片。")
        return

    logger.info(f"🔍 共找到 {len(image_urls)} 张图片")

    # ===========================
    # 2️⃣ 提取 public_id
    # ===========================
    public_ids = []
    for url in image_urls:
        pid = extract_cloudinary_public_id(url)
        if pid:
            public_ids.append(pid)

    if not public_ids:
        logger.warning("⚠️ 未解析到有效 public_id")
        return

    logger.info(f"📌 成功解析 {len(public_ids)} 个 public_id")

    # ===========================
    # 3️⃣ 顺序批量删除 Cloudinary
    # ===========================
    deleted_total = 0
    failed_ids = []

    start_time = time.time()

    for i in range(0, len(public_ids), batch_size):
        batch = public_ids[i:i + batch_size]
        logger.info(f"🚀 删除批次 {i//batch_size + 1}，数量 {len(batch)}")

        success_count, failed_batch = delete_batch_with_retry(batch, max_retries)

        deleted_total += success_count
        failed_ids.extend(failed_batch)

        logger.info(f"✅ 当前累计删除 {deleted_total}/{len(public_ids)}")

        # 防止 API 限流
        time.sleep(0.4)

    elapsed = time.time() - start_time
    logger.info(f"🎯 Cloudinary 删除完成：{deleted_total}/{len(public_ids)}，耗时 {elapsed:.2f} 秒")

    # ===========================
    # 4️⃣ 只有成功删除的才删数据库
    # ===========================
    if deleted_total > 0:
        with engine.begin() as conn:
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

        logger.info(f"🗑 数据库删除 {deleted_rows} 条记录")

    if failed_ids:
        logger.error(f"❌ 仍有 {len(failed_ids)} 张图片删除失败")
        for fid in failed_ids:
            logger.error(f"   失败 public_id: {fid}")


# ===========================
# 批量删除 + 重试机制
# ===========================
def delete_batch_with_retry(public_id_list, max_retries):
    attempt = 0
    failed_ids = public_id_list

    while attempt < max_retries and failed_ids:
        attempt += 1
        logger.info(f"🔁 第 {attempt} 次尝试删除 {len(failed_ids)} 张图片")

        try:
            response = cloudinary.api.delete_resources(
                failed_ids,
                resource_type="image"
            )

            deleted = response.get("deleted", {})
            failed = response.get("failed", {})

            success_ids = [pid for pid, status in deleted.items() if status == "deleted"]
            failed_ids = list(failed.keys())

            logger.info(f"   本次成功 {len(success_ids)}，失败 {len(failed_ids)}")

        except Exception as e:
            logger.error(f"❌ 删除异常: {e}")
            time.sleep(1)

    success_count = len(public_id_list) - len(failed_ids)
    return success_count, failed_ids


# ===========================
# 提取 Cloudinary public_id
# ===========================
def extract_cloudinary_public_id(url: str):
    """
    解析 Cloudinary 图片 URL 提取 public_id
    示例：
    https://res.cloudinary.com/demo/image/upload/v1691234567/folder/image.jpg
    返回 -> folder/image
    """
    if "cloudinary.com" not in url:
        return None

    try:
        parts = url.split("/")
        idx = parts.index("upload")
        public_id_with_ext = "/".join(parts[idx + 1:])
        public_id = os.path.splitext(public_id_with_ext)[0]
        return public_id
    except Exception:
        return None
