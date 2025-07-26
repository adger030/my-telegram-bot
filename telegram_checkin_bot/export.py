import os
import pandas as pd
import pytz
import shutil
import zipfile
import requests
from datetime import datetime
from sqlalchemy import create_engine
from config import DATA_DIR, DATABASE_URL  # DATABASE_URL 示例: postgresql://user:pass@host:port/dbname

def export_messages(start_date: str, end_date: str):
    # 建立数据库连接（使用 SQLAlchemy 引擎）
    try:
        engine = create_engine(DATABASE_URL)
        df = pd.read_sql_query("SELECT * FROM messages", engine)
    except Exception as e:
        print(f"❌ 无法连接数据库或读取数据: {e}")
        return None

    # 尝试解析 timestamp 字段
    if 'timestamp' not in df.columns:
        print("❌ 缺少 timestamp 字段")
        return None

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])

    # 设置时区：从 UTC → 北京时间
    if df['timestamp'].dt.tz is None:
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
    else:
        if df['timestamp'].dt.tz.iloc[0] is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')

    beijing_tz = pytz.timezone("Asia/Shanghai")
    df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)

    # 过滤日期范围（字符串 → datetime → 带时区）
    try:
        start_time = pd.to_datetime(start_date + " 00:00:00").tz_localize(beijing_tz)
        end_time = pd.to_datetime(end_date + " 23:59:59").tz_localize(beijing_tz)
    except Exception as e:
        print(f"❌ 日期格式错误: {e}")
        return None

    filtered = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]

    if filtered.empty:
        print("⚠️ 指定日期内没有数据。")
        return None

    # 创建导出目录
    export_dir = os.path.join(DATA_DIR, f"export_{start_date}_{end_date}")
    os.makedirs(export_dir, exist_ok=True)

    # 导出 Excel，分 sheet
    excel_path = os.path.join(export_dir, f"打卡记录_{start_date}_{end_date}.xlsx")
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        for keyword, group_df in filtered.groupby("keyword"):
            slim_df = group_df[["username", "timestamp"]].sort_values("timestamp")
            slim_df.columns = ["用户名", "打卡时间"]
            slim_df["打卡时间"] = slim_df["打卡时间"].dt.strftime("%Y-%m-%d %H:%M:%S")
            sheet_name = keyword[:31] or "打卡"  # 避免 sheet 名过长
            slim_df.to_excel(writer, sheet_name=sheet_name, index=False)

    # 下载图片（content 为 .jpg 的 Cloudinary 链接）
    image_dir = os.path.join(export_dir, "图片")
    os.makedirs(image_dir, exist_ok=True)

    photo_df = filtered[filtered["content"].str.endswith(".jpg", na=False)]
    for _, row in photo_df.iterrows():
        url = row["content"]
        if url and url.startswith("http"):
            try:
                response = requests.get(url, stream=True, timeout=10)
                if response.status_code == 200:
                    ts = row["timestamp"].strftime("%Y-%m-%d_%H-%M-%S")
                    username = row["username"] or "匿名"
                    keyword = row["keyword"] or "无关键词"
                    safe_filename = f"{ts}_{username}_{keyword}.jpg"
                    save_path = os.path.join(image_dir, safe_filename)
                    with open(save_path, "wb") as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
            except Exception as e:
                print(f"[图片下载失败] {url} - {e}")

    # 打包为 ZIP
    zip_path = os.path.join(DATA_DIR, f"考勤统计_{start_date}_{end_date}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(export_dir):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, export_dir)
                zipf.write(full_path, arcname)

    # 清理导出目录
    try:
        shutil.rmtree(export_dir)
    except Exception as e:
        print(f"[清理导出目录失败] {e}")

    return zip_path
