import os
import shutil
import zipfile
import pandas as pd
import requests
import pytz
from datetime import datetime
from sqlalchemy import create_engine
from config import DATA_DIR, DATABASE_URL

def export_messages(start_date, end_date):
    beijing_tz = pytz.timezone('Asia/Shanghai')

    # 连接 PostgreSQL
    try:
        engine = create_engine(DATABASE_URL)
        df = pd.read_sql("SELECT * FROM messages", engine)
    except Exception as e:
        print(f"❌ 读取数据库失败: {e}")
        return None

    if df.empty:
        print("⚠️ 数据库中没有记录")
        return None

    # 时间处理
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])

    if df.empty:
        print("⚠️ 所有时间戳无效")
        return None

    # 统一为北京时间
    if df['timestamp'].dt.tz is None or df['timestamp'].dt.tz.iloc[0] is None:
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')

    df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)

    # 筛选时间范围
    try:
        start_time = pd.to_datetime(start_date + " 00:00:00").tz_localize(beijing_tz)
        end_time = pd.to_datetime(end_date + " 23:59:59").tz_localize(beijing_tz)
    except Exception as e:
        print("❌ 时间格式错误:", e)
        return None

    filtered = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]

    # 打印调试信息
    print("📊 总记录数:", len(df))
    print("📆 时间范围:", start_time, "~", end_time)
    print("📎 命中记录数:", len(filtered))

    if filtered.empty:
        print("⚠️ 指定日期内没有数据")
        return None

    # 创建导出目录
    export_dir = os.path.join(DATA_DIR, f"export_{start_date}_{end_date}")
    os.makedirs(export_dir, exist_ok=True)

    # 写入 Excel
    excel_path = os.path.join(export_dir, f"打卡记录_{start_date}_{end_date}.xlsx")
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        for keyword, group_df in filtered.groupby("keyword"):
            sheet_name = keyword.strip()[:31] if isinstance(keyword, str) and keyword.strip() else "打卡"
            slim_df = group_df[["username", "timestamp"]].sort_values("timestamp")
            slim_df.columns = ["用户名", "打卡时间"]
            slim_df["打卡时间"] = slim_df["打卡时间"].dt.strftime("%Y-%m-%d %H:%M:%S")
            slim_df.to_excel(writer, sheet_name=sheet_name, index=False)

    # 下载图片
    image_dir = os.path.join(export_dir, "图片")
    os.makedirs(image_dir, exist_ok=True)

    photo_df = filtered[filtered["content"].str.endswith(".jpg", na=False)]
    for _, row in photo_df.iterrows():
        url = row.get("content")
        if url and url.startswith("http"):
            try:
                response = requests.get(url, stream=True, timeout=10)
                if response.status_code == 200:
                    username = row.get("username") or "匿名"
                    ts = row['timestamp'].strftime('%Y-%m-%d_%H-%M-%S')
                    keyword = row.get("keyword") or "无关键词"
                    filename = f"{ts}_{username}_{keyword}.jpg"
                    filepath = os.path.join(image_dir, filename)
                    with open(filepath, 'wb') as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
            except Exception as e:
                print(f"[图片下载失败] {url} - {e}")

    # 打包 ZIP
    zip_path = os.path.join(DATA_DIR, f"考勤统计_{start_date}_{end_date}.zip")
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(export_dir):
                for file in files:
                    full_path = os.path.join(root, file)
                    arcname = os.path.relpath(full_path, export_dir)
                    zipf.write(full_path, arcname)
    except Exception as e:
        print("❌ 打包 ZIP 文件失败:", e)
        return None

    # 删除导出目录
    try:
        shutil.rmtree(export_dir)
    except Exception as e:
        print(f"[清理导出目录失败] {e}")

    print(f"✅ 导出成功: {zip_path}")
    return zip_path
