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
    """
    标注迟到（红色+班次标识）、早退（红色+班次标识）、补卡（黄色+班次标识）。
    支持跨天班次（如 I班次日下班）以及凌晨下班的正常打卡判定。
    """
    wb = load_workbook(excel_path)
    fill_red = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")      # 浅红
    fill_yellow = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")  # 浅黄

    for sheet in wb.worksheets:
        for row in sheet.iter_rows(min_row=2):  # 跳过表头
            shift_cell, time_cell, keyword_cell = row[3], row[1], row[2]

            if not shift_cell.value or not time_cell.value:
                continue

            shift_text = str(shift_cell.value).strip()
            shift_name = re.split(r'[（(]', shift_text)[0]  # 提取班次名（如 I班）

            # 时间解析：兼容 Excel datetime 对象或字符串
            if isinstance(time_cell.value, datetime):
                dt = time_cell.value
            else:
                try:
                    dt = datetime.strptime(str(time_cell.value), "%Y-%m-%d %H:%M:%S")
                except Exception:
                    continue  # 时间格式异常跳过

            # 1️⃣ 补卡标记（黄色）
            if "补卡" in shift_text:
                time_cell.fill = fill_yellow
                shift_cell.fill = fill_yellow
                if "（补卡）" not in shift_text:
                    shift_cell.value = f"{shift_text}（补卡）"
                continue

            # 2️⃣ 迟到/早退判定
            if shift_name in SHIFT_TIMES:
                start_time, end_time = SHIFT_TIMES[shift_name]

                # ---- 迟到判定 ----
                if keyword_cell.value == "#上班打卡":
                    if dt.time() > start_time:
                        time_cell.fill = fill_red
                        shift_cell.fill = fill_red
                        if "（迟到）" not in shift_text:
                            shift_cell.value = f"{shift_text}（迟到）"

                # ---- 早退判定 ----
                elif keyword_cell.value == "#下班打卡":
                    if shift_name == "I班":
                        # I班：次日 00:00 下班正常
                        if dt.hour == 0:
                            continue
                        # 当天 15:00-23:59 下班 → 早退
                        elif 15 <= dt.hour <= 23:
                            time_cell.fill = fill_red
                            shift_cell.fill = fill_red
                            if "（早退）" not in shift_text:
                                shift_cell.value = f"{shift_text}（早退）"
                    else:
                        # 其他班次：正常下班时间内判定早退
                        # 允许凌晨 0:00~1:00 正常下班（跨天）
                        if 0 <= dt.hour <= 1:
                            continue
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

    # 格式化班次函数
    def format_shift(shift):
        if pd.isna(shift):
            return shift
        shift_text = str(shift)
        if re.search(r'（\d{2}:\d{2}-\d{2}:\d{2}）', shift_text):
            return shift_text
        shift_name = shift_text.split("（")[0]
        if shift_name in SHIFT_TIMES:
            start, end = SHIFT_TIMES[shift_name]
            end_str = end.strftime('%H:%M')
            return f"{shift_text}（{start.strftime('%H:%M')}-{end_str}）"
        return shift_text

    # 写入 Excel：每个日期一个 Sheet
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for day, group_df in df.groupby("date"):
            slim_df = group_df[["name", "timestamp", "keyword", "shift"]].sort_values("timestamp").copy()
            slim_df.columns = ["姓名", "打卡时间", "关键词", "班次"]
            slim_df["打卡时间"] = slim_df["打卡时间"].dt.strftime("%Y-%m-%d %H:%M:%S")
            slim_df["班次"] = slim_df["班次"].apply(format_shift)
            slim_df.to_excel(writer, sheet_name=day[:31], index=False)

    # 标注迟到/早退和补卡
    _mark_late_early(excel_path)

    # 加载 Excel 以便后续修改
    wb = load_workbook(excel_path)

    # -------------------- 生成统计 Sheet --------------------
    stats = []
    for sheet in wb.worksheets:
        if sheet.title == "统计":
            continue
        for row in sheet.iter_rows(min_row=2, values_only=True):
            name, _, keyword, shift_text = row
            if not name or not keyword or not shift_text:
                continue
            shift_str = str(shift_text)
            if "补卡" in shift_str:
                status = "补卡"
            elif "迟到" in shift_str or "早退" in shift_str:
                status = "迟到/早退"
            else:
                status = "正常"
            stats.append({"姓名": name, "状态": status})

    stats_df = pd.DataFrame(stats)
    if not stats_df.empty:
        summary_df = stats_df.groupby(["姓名", "状态"]).size().unstack(fill_value=0).reset_index()

        # 确保列存在
        for col in ["正常", "迟到/早退", "补卡"]:
            if col not in summary_df.columns:
                summary_df[col] = 0

        # 计算“异常总数”
        summary_df["异常总数"] = summary_df["迟到/早退"] + summary_df["补卡"]

        # ✅ 按“正常打卡次数”降序排序
        summary_df = summary_df.sort_values(by="正常", ascending=False)

        # 调整列顺序
        summary_df = summary_df[["姓名", "正常", "迟到/早退", "补卡", "异常总数"]]

        # 创建统计 Sheet
        stats_sheet = wb.create_sheet("统计", 0)
        headers = ["姓名", "正常打卡", "迟到/早退", "补卡次数", "异常总数"]
        for r_idx, row in enumerate([headers] + summary_df.values.tolist(), 1):
            for c_idx, value in enumerate(row, 1):
                stats_sheet.cell(row=r_idx, column=c_idx, value=value)

        # ✅ 表头样式：加粗、居中、冻结首行
        from openpyxl.styles import Font, Alignment
        stats_sheet.freeze_panes = "A2"
        for cell in stats_sheet[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center")

    # -------------------- 所有 Sheet 自动列宽调整 --------------------
    for sheet in wb.worksheets:
        # 冻结首行并加粗居中表头
        sheet.freeze_panes = "A2"
        for cell in sheet[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center")

        # 自动列宽
        for col in sheet.columns:
            max_length = 0
            col_letter = col[0].column_letter
            for cell in col:
                try:
                    length = len(str(cell.value)) if cell.value is not None else 0
                    if length > max_length:
                        max_length = length
                except:
                    pass
            sheet.column_dimensions[col_letter].width = max_length + 2

    wb.save(excel_path)
    logging.info(f"✅ Excel 导出完成（含自动列宽、正常打卡排序、统一表头样式）: {excel_path}")
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

    # 下载图片
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

    # 分卷打包 ZIP
    zip_base = os.path.join(DATA_DIR, f"图片打包_{start_str}_{end_str}")
    zip_files = []
    part_idx = 1
    current_size = 0
    zipf = zipfile.ZipFile(f"{zip_base}_part{part_idx}.zip", "w", zipfile.ZIP_DEFLATED)

    for root, _, files in os.walk(export_dir):
        for file in files:
            full_path = os.path.join(root, file)
            arcname = os.path.relpath(full_path, export_dir)
            file_size = os.path.getsize(full_path)

            # 如果加上这个文件会超过 50MB → 关闭当前 ZIP，新建下一卷
            if current_size + file_size > MAX_TELEGRAM_FILE_MB * 1024 * 1024:
                zipf.close()
                zip_files.append(f"{zip_base}_part{part_idx}.zip")
                part_idx += 1
                zipf = zipfile.ZipFile(f"{zip_base}_part{part_idx}.zip", "w", zipfile.ZIP_DEFLATED)
                current_size = 0

            zipf.write(full_path, arcname)
            current_size += file_size

    zipf.close()
    zip_files.append(f"{zip_base}_part{part_idx}.zip")

    shutil.rmtree(export_dir)
    logging.info(f"✅ 图片分卷打包完成，共 {len(zip_files)} 卷")

    # 上传到 Cloudinary（大于 50MB 的 ZIP）
    cloud_urls = []
    for zf in zip_files:
        file_size_mb = os.path.getsize(zf) / (1024 * 1024)
        if file_size_mb > MAX_TELEGRAM_FILE_MB:
            logging.warning(f"⚠️ {zf} 超过 50MB，上传至 Cloudinary...")
            url = upload_to_cloudinary(zf)
            if url:
                cloud_urls.append(url)
                os.remove(zf)
        else:
            cloud_urls.append(zf)  # 直接本地文件返回

    return cloud_urls
