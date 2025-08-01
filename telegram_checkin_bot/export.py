import os
import pandas as pd
import pytz
import shutil
import zipfile
import requests
import logging
from datetime import datetime
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor
from config import DATA_DIR, DATABASE_URL, CLOUDINARY_UPLOAD_URL, CLOUDINARY_UPLOAD_PRESET
import cloudinary
import cloudinary.uploader

# 日志配置
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

MAX_TELEGRAM_FILE_MB = 50  # Telegram 文件限制

def export_messages(start_datetime, end_datetime):
    if not isinstance(start_datetime, datetime) or not isinstance(end_datetime, datetime):
        logging.error("❌ 参数必须为 datetime 类型")
        return None

    try:
        engine = create_engine(DATABASE_URL)
        query = "SELECT username, name, content, timestamp, keyword, shift FROM messages"
        df_iter = pd.read_sql_query(query, engine, chunksize=50000)  # 分批加载
        df = pd.concat(df_iter, ignore_index=True)
        logging.info(f"✅ 数据读取完成，共 {len(df)} 条记录")
    except Exception as e:
        logging.error(f"❌ 无法连接数据库或读取数据: {e}")
        return None

    if 'timestamp' not in df.columns:
        logging.error("❌ 数据中不含 timestamp 字段")
        return None

    # 处理缺失字段
    if 'name' not in df.columns:
        df['name'] = None
    if 'shift' not in df.columns:
        df['shift'] = None

    # 转换时区为北京时间
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True).dropna()
    df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Shanghai')

    # 过滤时间范围
    filtered = df[(df['timestamp'] >= start_datetime) & (df['timestamp'] < end_datetime)]
    if filtered.empty:
        logging.warning("⚠️ 指定日期内没有数据")
        return None

    start_str = start_datetime.strftime("%Y-%m-%d")
    end_str = (end_datetime - pd.Timedelta(seconds=1)).strftime("%Y-%m-%d")

    export_dir = os.path.join(DATA_DIR, f"export_{start_str}_{end_str}")
    os.makedirs(export_dir, exist_ok=True)

    # ✅ Excel 导出
    excel_path = os.path.join(export_dir, f"打卡记录_{start_str}_{end_str}.xlsx")
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        filtered['date'] = filtered['timestamp'].dt.strftime("%Y-%m-%d")
        for day, group_df in filtered.groupby("date"):
            slim_df = group_df[["name", "timestamp", "keyword", "shift"]].sort_values("timestamp")
            slim_df.columns = ["姓名", "打卡时间", "关键词", "班次"]
            slim_df["打卡时间"] = slim_df["打卡时间"].dt.strftime("%Y-%m-%d %H:%M:%S")
            slim_df.to_excel(writer, sheet_name=day[:31], index=False)
    logging.info(f"✅ Excel 导出完成: {excel_path}")

    # ✅ 下载图片（并行）
    image_dir = os.path.join(export_dir, "图片")
    os.makedirs(image_dir, exist_ok=True)
    photo_df = filtered[filtered["content"].str.endswith(".jpg", na=False)]

    def download_image(row):
        url = row.get("content")
        if url and url.startswith("http"):
            try:
                ts = row["timestamp"].strftime("%Y-%m-%d_%H-%M-%S")
                name = row["name"] or "匿名"
                keyword = row["keyword"] or "无关键词"
                filename = f"{ts}_{name}_{keyword}.jpg"
                save_path = os.path.join(image_dir, filename)
                response = requests.get(url, stream=True, timeout=10)
                if response.status_code == 200:
                    with open(save_path, "wb") as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
                logging.info(f"📥 下载成功: {filename}")
            except Exception as e:
                logging.warning(f"[图片下载失败] {url} - {e}")

    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(download_image, [row for _, row in photo_df.iterrows()])

    # ✅ 打包 ZIP
    zip_path = os.path.join(DATA_DIR, f"考勤统计_{start_str}_{end_str}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(export_dir):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, export_dir)
                zipf.write(full_path, arcname)
    logging.info(f"✅ 文件打包完成: {zip_path}")

    # ✅ 清理临时文件夹
    try:
        shutil.rmtree(export_dir)
    except Exception as e:
        logging.warning(f"[清理导出目录失败] {e}")

    # ✅ 检查文件大小，超过 50MB 上传到 Cloudinary
    file_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
    if file_size_mb > MAX_TELEGRAM_FILE_MB:
        logging.warning(f"⚠️ 文件超过 {MAX_TELEGRAM_FILE_MB}MB，尝试上传到 Cloudinary...")
        url = upload_to_cloudinary(zip_path)
        if url:
            logging.info(f"✅ 文件上传成功: {url}")
            return url  # 返回下载链接
        else:
            logging.error("❌ 文件上传失败")
            return None

    logging.info(f"✅ 导出完成，本地文件: {zip_path}")
    return zip_path  # 返回文件路径


def upload_to_cloudinary(file_path: str) -> str | None:
    """
    上传文件到 Cloudinary 并返回下载链接 (secure_url)
    :param file_path: 本地文件路径
    :return: 文件的 HTTPS 下载链接 (secure_url)，失败则返回 None
    """
    try:
        result = cloudinary.uploader.upload(
            file_path,
            resource_type="raw",  # raw 允许上传非图片文件，如 ZIP/Excel
            folder="telegram_exports"  # 可选：在 Cloudinary 上存储到指定文件夹
        )
        secure_url = result.get("secure_url")
        if secure_url:
            logging.info(f"✅ Cloudinary 上传成功: {secure_url}")
            return secure_url
        else:
            logging.error("❌ Cloudinary 上传未返回 secure_url")
            return None
    except Exception as e:
        logging.error(f"❌ Cloudinary 上传失败: {e}")
        return None
