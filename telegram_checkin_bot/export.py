import os
import re
import pandas as pd
import pytz
import shutil
import zipfile
import requests
import logging
from datetime import datetime
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor
from config import DATA_DIR, DATABASE_URL
import cloudinary
import cloudinary.uploader

# 日志配置
logging.basicConfig(level=logging.WARNING, format="[%(asctime)s] %(levelname)s: %(message)s")

MAX_TELEGRAM_FILE_MB = 50  # Telegram 文件限制

def safe_filename(name: str) -> str:
    """清理文件名中的非法字符"""
    return re.sub(r'[\\/*?:"<>|]', "_", str(name))

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

    # 确保必要字段存在
    if 'timestamp' not in df.columns:
        logging.error("❌ 数据中不含 timestamp 字段")
        return None
    if 'name' not in df.columns:
        df['name'] = None
    if 'shift' not in df.columns:
        df['shift'] = None

    # 处理时间字段并转换为北京时间
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"]).copy()
    df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Shanghai")

    # 过滤时间范围
    filtered = df[(df["timestamp"] >= start_datetime) & (df["timestamp"] < end_datetime)].copy()
    if filtered.empty:
        logging.warning("⚠️ 指定日期内没有数据")
        return None

    # 生成导出路径
    start_str = start_datetime.strftime("%Y-%m-%d")
    end_str = (end_datetime - pd.Timedelta(seconds=1)).strftime("%Y-%m-%d")
    export_dir = os.path.join(DATA_DIR, f"export_{start_str}_{end_str}")
    os.makedirs(export_dir, exist_ok=True)

    # ✅ Excel 导出
    filtered["date"] = filtered["timestamp"].dt.strftime("%Y-%m-%d")
    excel_path = os.path.join(export_dir, f"打卡记录_{start_str}_{end_str}.xlsx")
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        for day, group_df in filtered.groupby("date"):
            slim_df = group_df[["name", "timestamp", "keyword", "shift"]].sort_values("timestamp").copy()
            slim_df.columns = ["姓名", "打卡时间", "关键词", "班次"]
            slim_df["打卡时间"] = slim_df["打卡时间"].dt.strftime("%Y-%m-%d %H:%M:%S")
            slim_df.to_excel(writer, sheet_name=day[:31], index=False)
    logging.info(f"✅ Excel 导出完成: {excel_path}")

    # ✅ 下载图片（按天归类）
    image_dir = os.path.join(export_dir, "图片")
    os.makedirs(image_dir, exist_ok=True)
    photo_df = filtered[filtered["content"].str.endswith(".jpg", na=False)]

    def download_image(row):
        url = row.get("content")
        if url and url.startswith("http"):
            try:
                ts = row["timestamp"].strftime("%Y-%m-%d_%H-%M-%S")
                date_folder = row["timestamp"].strftime("%Y-%m-%d")
                day_dir = os.path.join(image_dir, date_folder)
                os.makedirs(day_dir, exist_ok=True)

                name = safe_filename(row["name"] or "匿名")
                keyword = safe_filename(row["keyword"] or "无关键词")
                filename = f"{ts}_{name}_{keyword}.jpg"
                save_path = os.path.join(day_dir, filename)

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

    # ✅ 检查文件大小并上传到 Cloudinary
    file_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
    if file_size_mb > MAX_TELEGRAM_FILE_MB:
        logging.warning(f"⚠️ 文件超过 {MAX_TELEGRAM_FILE_MB}MB，尝试上传到 Cloudinary...")
        url = upload_to_cloudinary(zip_path)
        if url:
            logging.info(f"✅ 文件上传成功: {url}")
            return url
        else:
            logging.error("❌ 文件上传失败")
            return None

    logging.info(f"✅ 导出完成，本地文件: {zip_path}")
    return zip_path

def upload_to_cloudinary(file_path: str) -> str | None:
    """上传文件到 Cloudinary 并返回下载链接"""
    try:
        result = cloudinary.uploader.upload(
            file_path,
            resource_type="raw",
            folder="telegram_exports",
            public_id=os.path.splitext(os.path.basename(file_path))[0]  # 用文件名作为 ID
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
