import sqlite3
import pandas as pd
import os
import shutil
import zipfile
import pytz
from datetime import datetime
from config import DATA_DIR

def export_messages(start_date, end_date):
    db_path = os.path.join(DATA_DIR, "messages.db")
    if not os.path.exists(db_path):
        return None

    # 加载数据并转换时间为北京时间
    df = pd.read_sql_query("SELECT * FROM messages", sqlite3.connect(db_path))
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    beijing_tz = pytz.timezone('Asia/Shanghai')
    df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)
    df = df.dropna(subset=['timestamp'])

    # 过滤时间范围（转为北京时间）
    start_time = pd.to_datetime(start_date + " 00:00:00").tz_localize(beijing_tz)
    end_time = pd.to_datetime(end_date + " 23:59:59").tz_localize(beijing_tz)
    filtered = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]

    if filtered.empty:
        return None

    # 创建导出目录
    export_dir = os.path.join(DATA_DIR, f"export_{start_date}_{end_date}")
    os.makedirs(export_dir, exist_ok=True)

    # 导出 Excel（按 keyword 分组分 sheet）
    excel_path = os.path.join(export_dir, f"打卡记录_{start_date}_{end_date}.xlsx")
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        for keyword, group_df in filtered.groupby("keyword"):
            slim_df = group_df[["username", "timestamp"]].sort_values("timestamp")
            slim_df.columns = ["用户名", "打卡时间"]
            slim_df["打卡时间"] = slim_df["打卡时间"].dt.strftime("%Y-%m-%d %H:%M:%S")  # 格式化为北京时间字符串
            slim_df.to_excel(writer, sheet_name=keyword[:31], index=False)

    # 导出相关图片
    image_dir = os.path.join(export_dir, "图片")
    os.makedirs(image_dir, exist_ok=True)

    photo_df = filtered[filtered["content"].str.endswith(".jpg", na=False)]

    for _, row in photo_df.iterrows():
        src = row["content"]
        if os.path.exists(src):
            safe_username = row['username'] or '匿名'
            ts = row['timestamp'].strftime('%Y-%m-%d_%H-%M-%S')  # 使用北京时间命名
            filename = f"{ts}_{safe_username}_{row['keyword']}.jpg"
            dst = os.path.join(image_dir, filename)
            shutil.copy(src, dst)

    # 打包成 ZIP
    zip_path = os.path.join(DATA_DIR, f"考勤统计{start_date}_{end_date}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(export_dir):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, export_dir)
                zipf.write(full_path, arcname)

    try:
        shutil.rmtree(export_dir)
    except Exception as e:
        print(f"[清理导出目录失败] {e}")

    return zip_path
