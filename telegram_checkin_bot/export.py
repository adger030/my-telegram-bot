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
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

MAX_TELEGRAM_FILE_MB = 50  # Telegram 文件大小限制（MB）
BEIJING_TZ = pytz.timezone("Asia/Shanghai")


def safe_filename(name: str) -> str:
    """清理文件名中的非法字符"""
    return re.sub(r'[\\/*?:"<>|]', "_", str(name))


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


def _fetch_data(start_datetime: datetime, end_datetime: datetime) -> pd.DataFrame:
    """从数据库读取指定时间范围的数据"""
    try:
        engine = create_engine(DATABASE_URL)
        query = """
        SELECT username, name, content, timestamp, keyword, shift 
        FROM messages 
        WHERE timestamp BETWEEN :start AND :end
        """
        df_iter = pd.read_sql_query(query, engine, params={"start": start_datetime, "end": end_datetime}, chunksize=50000)
        df = pd.concat(df_iter, ignore_index=True)
        logging.info(f"✅ 数据读取完成，共 {len(df)} 条记录")
    except Exception as e:
        logging.error(f"❌ 无法连接数据库或读取数据: {e}")
        return pd.DataFrame()

    if df.empty:
        return df

    # 转换时间为北京时间
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.tz_convert(BEIJING_TZ)
    df = df.dropna(subset=["timestamp"]).copy()
    return df


def export_excel(start_datetime: datetime, end_datetime: datetime):
    """仅导出 Excel，不包含图片"""
    df = _fetch_data(start_datetime, end_datetime)
    if df.empty:
        logging.warning("⚠️ 指定日期内没有数据")
        return None

    df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")

    start_str = start_datetime.strftime("%Y-%m-%d")
    end_str = (end_datetime - pd.Timedelta(seconds=1)).strftime("%Y-%m-%d")
    export_dir = os.path.join(DATA_DIR, f"excel_{start_str}_{end_str}")
    os.makedirs(export_dir, exist_ok=True)

    excel_path = os.path.join(export_dir, f"打卡记录_{start_str}_{end_str}.xlsx")
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for day, group_df in df.groupby("date"):
            slim_df = group_df[["name", "timestamp", "keyword", "shift"]].sort_values("timestamp").copy()
            slim_df.columns = ["姓名", "打卡时间", "关键词", "班次"]
            slim_df["打卡时间"] = slim_df["打卡时间"].dt.strftime("%Y-%m-%d %H:%M:%S")
            slim_df.to_excel(writer, sheet_name=day[:31], index=False)

    logging.info(f"✅ Excel 导出完成: {excel_path}")
    return excel_path


def export_images(start_datetime: datetime, end_datetime: datetime):
    """仅导出图片并打包 ZIP"""
    df = _fetch_data(start_datetime, end_datetime)
    if df.empty:
        logging.warning("⚠️ 指定日期内没有数据")
        return None

    photo_df = df[df["content"].str.endswith(".jpg", na=False)]
    if photo_df.empty:
        logging.warning("⚠️ 指定日期内没有图片")
        return None

    start_str = start_datetime.strftime("%Y-%m-%d")
    end_str = (end_datetime - pd.Timedelta(seconds=1)).strftime("%Y-%m-%d")
    export_dir = os.path.join(DATA_DIR, f"images_{start_str}_{end_str}")
    os.makedirs(export_dir, exist_ok=True)

    def download_image(row):
        url = row["content"]
        if url and url.startswith("http"):
            try:
                ts = row["timestamp"].strftime("%Y-%m-%d_%H-%M-%S")
                date_folder = row["timestamp"].strftime("%Y-%m-%d")
                day_dir = os.path.join(export_dir, date_folder)
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
        executor.map(download_image, photo_df.to_dict("records"))

    zip_path = os.path.join(DATA_DIR, f"图片打包_{start_str}_{end_str}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(export_dir):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, export_dir)
                zipf.write(full_path, arcname)

    shutil.rmtree(export_dir)
    logging.info(f"✅ 图片打包完成: {zip_path}")

    # 检查大小并决定是否上传到 Cloudinary
    file_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
    if file_size_mb > MAX_TELEGRAM_FILE_MB:
        logging.warning(f"⚠️ 文件超过 {MAX_TELEGRAM_FILE_MB}MB，尝试上传到 Cloudinary...")
        url = upload_to_cloudinary(zip_path)
        if url:
            os.remove(zip_path)
            return url
        else:
            return None

    return zip_path
