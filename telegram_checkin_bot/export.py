import sqlite3
import pandas as pd
import os
import shutil
import zipfile
import pytz
import requests
from datetime import datetime
from config import DATA_DIR

def export_messages(start_date, end_date):
    db_path = os.path.join(DATA_DIR, "messages.db")
    if not os.path.exists(db_path):
        print("❌ 数据库不存在")
        return None

    # 加载数据
    df = pd.read_sql_query("SELECT * FROM messages", sqlite3.connect(db_path))
    
    if df.empty:
        print("❌ 数据库中没有任何记录")
        return None

    # 转换 timestamp 字段为 datetime 类型
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    # 删除无效时间
    df = df.dropna(subset=['timestamp'])

    # 如果没有时区，设为 UTC
    if df['timestamp'].dt.tz is None:
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')

    # 转为北京时间
    beijing_tz = pytz.timezone('Asia/Shanghai')
    df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)

    # 构造起止时间范围
    start_time = pd.to_datetime(start_date + " 00:00:00").tz_localize(beijing_tz)
    end_time = pd.to_datetime(end_date + " 23:59:59").tz_localize(beijing_tz)

    # 筛选记录
    filtered = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]

    # ✅ 调试信息
    print("✅ 导出调试信息")
    print("总记录数:", len(df))
    print("筛选时间段:", start_time, "~", end_time)
    print("命中记录数:", len(filtered))

    if filtered.empty:
        print("⚠️ 指定日期内没有数据")
        return None

    # 创建导出目录
    export_dir = os.path.join(DATA_DIR, f"export_{start_date}_{end_date}")
    os.makedirs(export_dir, exist_ok=True)

    # 导出 Excel 文件（按 keyword 分 sheet）
    excel_path = os.path.join(export_dir, f"打卡记录_{start_date}_{end_date}.xlsx")
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        for keyword, group_df in filtered.groupby("keyword"):
            slim_df = group_df[["username", "timestamp"]].sort_values("timestamp")
            slim_df.columns = ["用户名", "打卡时间"]
            slim_df["打卡时间"] = slim_df["打卡时间"].dt.strftime("%Y-%m-%d %H:%M:%S")
            sheet_name = keyword[:31] or "打卡"
            slim_df.to_excel(writer, sheet_name=sheet_name, index=False)

    # 下载 Cloudinary 图片
    image_dir = os.path.join(export_dir, "图片")
    os.makedirs(image_dir, exist_ok=True)

    photo_df = filtered[filtered["content"].str.endswith(".jpg", na=False)]

    for _, row in photo_df.iterrows():
        url = row["content"]
        if url and url.startswith("http"):
            try:
                response = requests.get(url, stream=True, timeout=10)
                if response.status_code == 200:
                    safe_username = row['username'] or '匿名'
                    ts = row['timestamp'].strftime('%Y-%m-%d_%H-%M-%S')
                    keyword = row['keyword'] or "无关键词"
                    filename = f"{ts}_{safe_username}_{keyword}.jpg"
                    dst = os.path.join(image_dir, filename)
                    with open(dst, 'wb') as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
            except Exception as e:
                print(f"[图片下载失败] {url} - {e}")

    # 打包为 zip 文件
    zip_path = os.path.join(DATA_DIR, f"考勤统计{start_date}_{end_date}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(export_dir):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, export_dir)
                zipf.write(full_path, arcname)

    # 清理临时导出目录
    try:
        shutil.rmtree(export_dir)
    except Exception as e:
        print(f"[清理导出目录失败] {e}")

    print(f"✅ 导出成功: {zip_path}")
    return zip_path
