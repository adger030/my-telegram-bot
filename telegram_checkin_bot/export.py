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
    """
    从数据库读取打卡数据，兼容 user_id 改造
    """
    try:
        engine = create_engine(DATABASE_URL)
        query = """
        SELECT user_id, username, name, content, timestamp, keyword, shift 
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

            if isinstance(time_cell.value, datetime):
                dt = time_cell.value
            else:
                try:
                    dt = datetime.strptime(str(time_cell.value), "%Y-%m-%d %H:%M:%S")
                except Exception:
                    continue

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

                if keyword_cell.value == "#上班打卡":
                    if dt.time() > start_time:
                        time_cell.fill = fill_red
                        shift_cell.fill = fill_red
                        if "（迟到）" not in shift_text:
                            shift_cell.value = f"{shift_text}（迟到）"

                elif keyword_cell.value == "#下班打卡":
                    if shift_name == "I班":
                        if dt.hour == 0:
                            continue
                        elif 15 <= dt.hour <= 23:
                            time_cell.fill = fill_red
                            shift_cell.fill = fill_red
                            if "（早退）" not in shift_text:
                                shift_cell.value = f"{shift_text}（早退）"
                    else:
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

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for day, group_df in df.groupby("date"):
            slim_df = group_df[["name", "timestamp", "keyword", "shift"]].sort_values("timestamp").copy()
            slim_df.columns = ["姓名", "打卡时间", "关键词", "班次"]
            slim_df["打卡时间"] = slim_df["打卡时间"].dt.strftime("%Y-%m-%d %H:%M:%S")
            slim_df["班次"] = slim_df["班次"].apply(format_shift)
            slim_df.to_excel(writer, sheet_name=day[:31], index=False)

    _mark_late_early(excel_path)
    wb = load_workbook(excel_path)

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

        for col in ["正常", "迟到/早退", "补卡"]:
            if col not in summary_df.columns:
                summary_df[col] = 0

        summary_df["异常总数"] = summary_df["迟到/早退"] + summary_df["补卡"]
        summary_df = summary_df.sort_values(by="正常", ascending=False)
        summary_df = summary_df[["姓名", "正常", "迟到/早退", "补卡", "异常总数"]]

        stats_sheet = wb.create_sheet("统计", 0)
        headers = ["姓名", "正常打卡", "迟到/早退", "补卡", "异常总数"]
        for r_idx, row in enumerate([headers] + summary_df.values.tolist(), 1):
            for c_idx, value in enumerate(row, 1):
                stats_sheet.cell(row=r_idx, column=c_idx, value=value)

        from openpyxl.styles import Font, Alignment, PatternFill
        stats_sheet.freeze_panes = "A2"
        for cell in stats_sheet[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center")

        fill_red = PatternFill(start_color="F8CBAD", end_color="F8CBAD", fill_type="solid")
        for r_idx in range(2, stats_sheet.max_row + 1):
            abnormal = stats_sheet.cell(row=r_idx, column=5).value
            if abnormal is not None and abnormal >= 3:
                for c_idx in range(1, 6):
                    stats_sheet.cell(row=r_idx, column=c_idx).fill = fill_red

    from openpyxl.styles import Font, Alignment
    for sheet in wb.worksheets:
        sheet.freeze_panes = "A2"
        for cell in sheet[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center")
        for col in sheet.columns:
            max_length = 0
            col_letter = col[0].column_letter
            for cell in col:
                try:
                    length = len(str(cell.value)) if cell.value is not None else 0
                    if length > max_length:
                        max_length = length
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                except:
                    pass
            sheet.column_dimensions[col_letter].width = max_length + 2

    wb.save(excel_path)
    logging.info(f"✅ Excel 导出完成（含自动列宽、正常打卡排序、异常高亮、文字居中）: {excel_path}")
    return excel_path

def export_images(start_datetime: datetime, end_datetime: datetime):
    try:
        df = _fetch_data(start_datetime, end_datetime)
        if df.empty:
            logging.warning("⚠️ 指定日期内没有数据")
            return None

        photo_df = df[df["content"].str.contains(r"\.(?:jpg|jpeg|png|gif)$", case=False, na=False)].copy()
        if photo_df.empty:
            logging.warning("⚠️ 指定日期内没有图片。")
            return None

        def extract_public_id(url: str) -> str | None:
            match = re.search(r'/upload/(?:v\d+/)?(.+?)\.(?:jpg|jpeg|png|gif)$', url)
            if match:
                return match.group(1)
            logging.warning(f"⚠️ 无法解析 public_id: {url}")
            return None

        photo_df["public_id"] = photo_df["content"].apply(extract_public_id)
        public_ids = [pid for pid in photo_df["public_id"].dropna().unique() if pid.strip()]
        logging.info(f"🔍 共提取到 {len(public_ids)} 个图片 public_id")
        if not public_ids:
            logging.error("❌ 没有有效的 public_id，可能 URL 不是 Cloudinary 链接")
            return None

        start_str = start_datetime.strftime("%Y-%m-%d")
        end_str = (end_datetime - pd.Timedelta(seconds=1)).strftime("%Y-%m-%d")
        export_dir = os.path.join(DATA_DIR, f"images_{start_str}_{end_str}")
        shutil.rmtree(export_dir, ignore_errors=True)
        os.makedirs(export_dir, exist_ok=True)

        zip_paths = []
        current_zip_idx = 1
        current_zip_size = 0
        current_zip_path = os.path.join(export_dir, f"图片打包_{start_str}_{end_str}_包{current_zip_idx}.zip")
        current_zip = zipfile.ZipFile(current_zip_path, "w", zipfile.ZIP_DEFLATED)

        for idx, pid in enumerate(public_ids, 1):
            url = cloudinary.CloudinaryImage(pid).build_url()
            filename = safe_filename(f"{os.path.basename(pid)}.jpg")

            try:
                resp = requests.get(url, stream=True, timeout=15)
                resp.raise_for_status()
                content = resp.content
            except Exception as e:
                logging.warning(f"⚠️ 下载失败 {url}: {e}")
                continue

            if current_zip_size + len(content) > 40 * 1024 * 1024:
                current_zip.close()
                zip_paths.append(current_zip_path)
                logging.info(f"📦 完成 ZIP 包 {current_zip_idx}: {current_zip_path}")
                current_zip_idx += 1
                current_zip_size = 0
                current_zip_path = os.path.join(export_dir, f"图片打包_{start_str}_{end_str}_包{current_zip_idx}.zip")
                current_zip = zipfile.ZipFile(current_zip_path, "w", zipfile.ZIP_DEFLATED)

            current_zip.writestr(filename, content)
            current_zip_size += len(content)

        current_zip.close()
        zip_paths.append(current_zip_path)
        logging.info(f"✅ 图片分包导出完成，共 {len(zip_paths)} 包，目录: {export_dir}")
        return zip_paths, export_dir

    except Exception as e:
        logging.error(f"❌ export_images 失败: {e}")
        return None
