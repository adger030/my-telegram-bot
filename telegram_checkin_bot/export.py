import os
import re
import pandas as pd
import pytz
import shutil
import zipfile
import requests
import logging
from datetime import datetime, time
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor
from config import DATA_DIR, DATABASE_URL
import cloudinary
import cloudinary.uploader
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

# 日志配置
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

MAX_TELEGRAM_FILE_MB = 50
BEIJING_TZ = pytz.timezone("Asia/Shanghai")

# 班次时间定义
SHIFT_TIMES = {
    "F班": (time(12, 0), time(21, 0)),
    "G班": (time(13, 0), time(22, 0)),
    "H班": (time(14, 0), time(23, 0)),
    "I班": (time(15, 0), time(0, 0)),  # 跨天处理
}

def safe_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", str(name))

def upload_to_cloudinary(file_path: str) -> str | None:
    try:
        result = cloudinary.uploader.upload(
            file_path,
            resource_type="raw",
            folder="telegram_exports",
            public_id=os.path.splitext(os.path.basename(file_path))[0]
        )
        return result.get("secure_url")
    except Exception as e:
        logging.error(f"❌ Cloudinary 上传失败: {e}")
        return None

def _fetch_data(start_datetime: datetime, end_datetime: datetime) -> pd.DataFrame:
    try:
        engine = create_engine(DATABASE_URL)
        query = """
        SELECT username, name, content, timestamp, keyword, shift 
        FROM messages 
        WHERE timestamp BETWEEN %(start)s AND %(end)s
        """
        params = {
            "start": start_datetime.astimezone(pytz.UTC),
            "end": end_datetime.astimezone(pytz.UTC)
        }
        df_iter = pd.read_sql_query(query, engine, params=params, chunksize=50000)
        df = pd.concat(df_iter, ignore_index=True)
        logging.info(f"✅ 数据读取完成，共 {len(df)} 条记录")
    except Exception as e:
        logging.error(f"❌ 无法连接数据库或读取数据: {e}")
        return pd.DataFrame()

    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.tz_convert(BEIJING_TZ)
    df = df.dropna(subset=["timestamp"]).copy()
    return df

def _mark_late_early(excel_path: str):
    """标注迟到、早退（红色+班次标识）和补卡（黄色+班次标识），下班超过时间正常"""
    wb = load_workbook(excel_path)
    fill_red = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
    fill_yellow = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

    for sheet in wb.worksheets:
        for row in sheet.iter_rows(min_row=2):
            shift_cell, time_cell, keyword_cell = row[3], row[1], row[2]
            if not shift_cell.value or not time_cell.value:
                continue

            shift_text = str(shift_cell.value)
            shift_name = re.split(r'[（(]', shift_text)[0]

            # 补卡标记
            if "补卡" in shift_text:
                time_cell.fill = fill_yellow
                shift_cell.fill = fill_yellow
                if "（补卡）" not in shift_text:
                    shift_cell.value = f"{shift_text}（补卡）"
                continue

            # 迟到/早退判定
            if shift_name in SHIFT_TIMES:
                start_time, end_time = SHIFT_TIMES[shift_name]
                dt = datetime.strptime(time_cell.value, "%Y-%m-%d %H:%M:%S")

                # 迟到：上班打卡 > 开始时间
                if keyword_cell.value == "#上班打卡":
                    if dt.time() > start_time:
                        time_cell.fill = fill_red
                        shift_cell.fill = fill_red
                        if "（迟到）" not in shift_text:
                            shift_cell.value = f"{shift_text}（迟到）"

                # 早退：下班打卡 < 结束时间（超过时间正常，不提示加班）
                elif keyword_cell.value == "#下班打卡":
                    if shift_name == "I班" and dt.hour == 0:
                        continue  # I班凌晨下班正常
                    if dt.time() < end_time:
                        time_cell.fill = fill_red
                        shift_cell.fill = fill_red
                        if "（早退）" not in shift_text:
                            shift_cell.value = f"{shift_text}（早退）"

    wb.save(excel_path)


def export_excel(start_datetime: datetime, end_datetime: datetime):
    df = _fetch_data(start_datetime, end_datetime)
    if df.empty:
        logging.warning("⚠️ 指定日期内没有数据")
        return None

    # 添加日期列
    df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")

    start_str = start_datetime.strftime("%Y-%m-%d")
    end_str = (end_datetime - pd.Timedelta(seconds=1)).strftime("%Y-%m-%d")

    export_dir = os.path.join(DATA_DIR, f"excel_{start_str}_{end_str}")
    os.makedirs(export_dir, exist_ok=True)
    excel_path = os.path.join(export_dir, f"打卡记录_{start_str}_{end_str}.xlsx")

    # 格式化班次函数，避免重复添加时间段
    def format_shift(shift):
        if pd.isna(shift):
            return shift
        shift_text = str(shift)

        # 如果已存在 "（HH:MM-HH:MM）" 格式，直接返回
        if re.search(r'（\d{2}:\d{2}-\d{2}:\d{2}）', shift_text):
            return shift_text

        shift_name = shift_text.split("（")[0]  # 去掉“补卡”标记等
        if shift_name in SHIFT_TIMES:
            start, end = SHIFT_TIMES[shift_name]
            end_str = end.strftime('%H:%M')  # I班也显示00:00，不加“次日”
            return f"{shift_text}（{start.strftime('%H:%M')}-{end_str}）"
        return shift_text

    # 按日期分表写入 Excel
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for day, group_df in df.groupby("date"):
            slim_df = group_df[["name", "timestamp", "keyword", "shift"]].sort_values("timestamp").copy()
            slim_df.columns = ["姓名", "打卡时间", "关键词", "班次"]
            slim_df["打卡时间"] = slim_df["打卡时间"].dt.strftime("%Y-%m-%d %H:%M:%S")

            # 格式化班次列（如 I班 → I班（15:00-00:00））
            slim_df["班次"] = slim_df["班次"].apply(format_shift)

            slim_df.to_excel(writer, sheet_name=day[:31], index=False)

    # 标注迟到/早退和补卡
    _mark_late_early(excel_path)
    logging.info(f"✅ Excel 导出完成并标注迟到/早退: {excel_path}")
    return excel_path

def export_images(start_datetime: datetime, end_datetime: datetime):
    df = _fetch_data(start_datetime, end_datetime)
    if df.empty:
        logging.warning("⚠️ 指定日期内没有数据")
        return None

    photo_df = df[df["content"].str.contains(r"\.jpg|\.jpeg|\.png", case=False, na=False)]
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
