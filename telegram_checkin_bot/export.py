import os
import pandas as pd
import pytz
import shutil
import zipfile
import requests
from datetime import datetime
from sqlalchemy import create_engine
from config import DATA_DIR, DATABASE_URL

def export_messages(start_datetime, end_datetime):
    # ⏳ 确保参数为 datetime 类型
    if not isinstance(start_datetime, datetime) or not isinstance(end_datetime, datetime):
        print("❌ 参数必须为 datetime 类型")
        return None

    try:
        engine = create_engine(DATABASE_URL)
        df = pd.read_sql_query("SELECT * FROM messages", engine)
    except Exception as e:
        print(f"❌ 无法连接数据库或读取数据: {e}")
        return None

    if 'timestamp' not in df.columns:
        print("❌ 数据中不含 timestamp 字段")
        return None

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])

    # 时区处理
    if df['timestamp'].dt.tz is None:
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
    df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Shanghai')

    # 过滤时间范围（datetime 已经是带 tz 的）
    filtered = df[(df['timestamp'] >= start_datetime) & (df['timestamp'] < end_datetime)]

    if filtered.empty:
        print("⚠️ 指定日期内没有数据。")
        return None

    # 日期字符串用于文件名
    start_str = start_datetime.strftime("%Y-%m-%d")
    end_str = (end_datetime - pd.Timedelta(seconds=1)).strftime("%Y-%m-%d")

    # 导出路径
    export_dir = os.path.join(DATA_DIR, f"export_{start_str}_{end_str}")
    os.makedirs(export_dir, exist_ok=True)

    # Excel 导出
    excel_path = os.path.join(export_dir, f"打卡记录_{start_str}_{end_str}.xlsx")
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        for keyword, group_df in filtered.groupby("keyword"):
            slim_df = group_df[["username", "timestamp"]].sort_values("timestamp")
            slim_df.columns = ["用户名", "打卡时间"]
            slim_df["打卡时间"] = slim_df["打卡时间"].dt.strftime("%Y-%m-%d %H:%M:%S")
            sheet_name = keyword[:31] if isinstance(keyword, str) else "打卡"
            slim_df.to_excel(writer, sheet_name=sheet_name, index=False)

    # 下载图片
    image_dir = os.path.join(export_dir, "图片")
    os.makedirs(image_dir, exist_ok=True)
    photo_df = filtered[filtered["content"].str.endswith(".jpg", na=False)]

    for _, row in photo_df.iterrows():
        url = row.get("content")
        if url and url.startswith("http"):
            try:
                ts = row["timestamp"].strftime("%Y-%m-%d_%H-%M-%S")
                username = row["username"] or "匿名"
                keyword = row["keyword"] or "无关键词"
                filename = f"{ts}_{username}_{keyword}.jpg"
                save_path = os.path.join(image_dir, filename)
                response = requests.get(url, stream=True, timeout=10)
                if response.status_code == 200:
                    with open(save_path, "wb") as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
            except Exception as e:
                print(f"[图片下载失败] {url} - {e}")

    # 打包 zip
    zip_path = os.path.join(DATA_DIR, f"考勤统计_{start_str}_{end_str}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(export_dir):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, export_dir)
                zipf.write(full_path, arcname)

    # 清理
    try:
        shutil.rmtree(export_dir)
    except Exception as e:
        print(f"[清理导出目录失败] {e}")

    print(f"✅ 导出成功: {zip_path}")
    return zip_path
