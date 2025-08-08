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
import logging
from tqdm import tqdm  # 控制台用，如果要发给用户，用 Telegram 消息更新
from telegram import Update
from telegram.ext import ContextTypes

# ===========================
# 基础配置
# ===========================
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

MAX_TELEGRAM_FILE_MB = 50  # Telegram 单文件上传限制
BEIJING_TZ = pytz.timezone("Asia/Shanghai")  # 北京时区

# 定义班次时间
SHIFT_TIMES = {
    "F班": (time(12, 0), time(21, 0)),
    "G班": (time(13, 0), time(22, 0)),
    "H班": (time(14, 0), time(23, 0)),
    "I班": (time(15, 0), time(0, 0)),  # I 班跨天处理
}

# ===========================
# 文件名安全化（去除非法字符）
# ===========================
def safe_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", str(name))

# ===========================
# 上传文件到 Cloudinary
# ===========================
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

# ===========================
# 读取数据库数据到 DataFrame
# ===========================
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
        # 分块读取（避免大数据内存溢出）
        df_iter = pd.read_sql_query(query, engine, params=params, chunksize=50000)
        df = pd.concat(df_iter, ignore_index=True)
        logging.info(f"✅ 数据读取完成，共 {len(df)} 条记录")
    except Exception as e:
        logging.error(f"❌ 无法连接数据库或读取数据: {e}")
        return pd.DataFrame()

    if df.empty:
        return df

    # 时间转为北京时区
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.tz_convert(BEIJING_TZ)
    df = df.dropna(subset=["timestamp"]).copy()
    return df

# ===========================
# Excel 内标记迟到/早退/补卡
# ===========================
def _mark_late_early(excel_path: str):
    wb = load_workbook(excel_path)
    fill_red = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")      # 浅红色填充（异常）
    fill_yellow = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")  # 浅黄色填充（补卡）

    for sheet in wb.worksheets:
        for row in sheet.iter_rows(min_row=2):  # 跳过表头
            shift_cell, time_cell, keyword_cell = row[3], row[1], row[2]

            if not shift_cell.value or not time_cell.value:
                continue

            shift_text = str(shift_cell.value).strip()
            shift_name = re.split(r'[（(]', shift_text)[0]  # 班次名（去除括号）

            # 解析时间（兼容 Excel datetime 和字符串）
            if isinstance(time_cell.value, datetime):
                dt = time_cell.value
            else:
                try:
                    dt = datetime.strptime(str(time_cell.value), "%Y-%m-%d %H:%M:%S")
                except Exception:
                    continue

            # 1️⃣ 补卡标记
            if "补卡" in shift_text:
                time_cell.fill = fill_yellow
                shift_cell.fill = fill_yellow
                if "（补卡）" not in shift_text:
                    shift_cell.value = f"{shift_text}（补卡）"
                continue

            # 2️⃣ 迟到/早退判定
            if shift_name in SHIFT_TIMES:
                start_time, end_time = SHIFT_TIMES[shift_name]

                # ---- 迟到 ----
                if keyword_cell.value == "#上班打卡" and dt.time() > start_time:
                    time_cell.fill = fill_red
                    shift_cell.fill = fill_red
                    if "（迟到）" not in shift_text:
                        shift_cell.value = f"{shift_text}（迟到）"

                # ---- 早退 ----
                elif keyword_cell.value == "#下班打卡":
                    if shift_name == "I班":
                        if dt.hour == 0:  # I班次日 00:00 正常
                            continue
                        elif 15 <= dt.hour <= 23:  # 当天提早下班
                            time_cell.fill = fill_red
                            shift_cell.fill = fill_red
                            if "（早退）" not in shift_text:
                                shift_cell.value = f"{shift_text}（早退）"
                    else:
                        if 0 <= dt.hour <= 1:  # 跨天凌晨下班正常
                            continue
                        if dt.time() < end_time:  # 提前下班
                            time_cell.fill = fill_red
                            shift_cell.fill = fill_red
                            if "（早退）" not in shift_text:
                                shift_cell.value = f"{shift_text}（早退）"

    wb.save(excel_path)

# ===========================
# 导出打卡记录 Excel
# ===========================
def export_excel(start_datetime: datetime, end_datetime: datetime):
    df = _fetch_data(start_datetime, end_datetime)
    if df.empty:
        logging.warning("⚠️ 指定日期内没有数据")
        return None

    # 生成日期列
    df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")

    start_str = start_datetime.strftime("%Y-%m-%d")
    end_str = (end_datetime - pd.Timedelta(seconds=1)).strftime("%Y-%m-%d")

    export_dir = os.path.join(DATA_DIR, f"excel_{start_str}_{end_str}")
    os.makedirs(export_dir, exist_ok=True)
    excel_path = os.path.join(export_dir, f"打卡记录_{start_str}_{end_str}.xlsx")

    # 格式化班次：自动补充班次时间
    def format_shift(shift):
        if pd.isna(shift):
            return shift
        shift_text = str(shift)
        if re.search(r'（\d{2}:\d{2}-\d{2}:\d{2}）', shift_text):
            return shift_text
        shift_name = shift_text.split("（")[0]
        if shift_name in SHIFT_TIMES:
            start, end = SHIFT_TIMES[shift_name]
            return f"{shift_text}（{start.strftime('%H:%M')}-{end.strftime('%H:%M')}）"
        return shift_text

    # 生成 Excel，每天一个 Sheet
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for day, group_df in df.groupby("date"):
            slim_df = group_df[["name", "timestamp", "keyword", "shift"]].sort_values("timestamp").copy()
            slim_df.columns = ["姓名", "打卡时间", "关键词", "班次"]
            slim_df["打卡时间"] = slim_df["打卡时间"].dt.strftime("%Y-%m-%d %H:%M:%S")
            slim_df["班次"] = slim_df["班次"].apply(format_shift)
            slim_df.to_excel(writer, sheet_name=day[:31], index=False)

    # 标记迟到/早退/补卡
    _mark_late_early(excel_path)

    # ✅ 生成“统计”Sheet：汇总正常/迟到/早退/补卡次数
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

        # 写入统计 Sheet
        stats_sheet = wb.create_sheet("统计", 0)
        headers = ["姓名", "正常打卡", "迟到/早退", "补卡", "异常总数"]
        for r_idx, row in enumerate([headers] + summary_df.values.tolist(), 1):
            for c_idx, value in enumerate(row, 1):
                stats_sheet.cell(row=r_idx, column=c_idx, value=value)

        # 样式美化：表头加粗、冻结首行、异常≥3 高亮
        from openpyxl.styles import Font, Alignment
        stats_sheet.freeze_panes = "A2"
        for cell in stats_sheet[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center")
        fill_red = PatternFill(start_color="F8CBAD", end_color="F8CBAD", fill_type="solid")
        for r_idx in range(2, stats_sheet.max_row + 1):
            if stats_sheet.cell(row=r_idx, column=5).value >= 3:
                for c_idx in range(1, 6):
                    stats_sheet.cell(row=r_idx, column=c_idx).fill = fill_red

    # 样式调整：所有 Sheet 居中、列宽自动
    from openpyxl.styles import Alignment
    for sheet in wb.worksheets:
        sheet.freeze_panes = "A2"
        for cell in sheet[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center")
        for col in sheet.columns:
            max_length = max(len(str(cell.value or "")) for cell in col)
            sheet.column_dimensions[col[0].column_letter].width = max_length + 2
            for cell in col:
                cell.alignment = Alignment(horizontal="center", vertical="center")

    wb.save(excel_path)
    logging.info(f"✅ Excel 导出完成: {excel_path}")
    return excel_path

# ===========================
# 导出图片并带进度条
# ===========================
async def export_images_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ 无权限，仅管理员可导出图片。")
        return

    export_dir = "/app/export_images"
    if os.path.exists(export_dir):
        shutil.rmtree(export_dir)
    os.makedirs(export_dir)

    # 模拟获取图片列表
    images = get_images_list()  # 这里返回所有要导出的图片路径
    total = len(images)

    if total == 0:
        await update.message.reply_text("⚠️ 没有可导出的图片。")
        return

    logging.info(f"🧹 已清理导出目录: {export_dir}")

    # 先发一个“进度消息”
    progress_msg = await update.message.reply_text(f"📦 开始导出图片 (0/{total})...")

    for idx, img_path in enumerate(images, start=1):
        shutil.copy(img_path, export_dir)  # 复制图片
        progress_text = f"📦 正在导出图片 ({idx}/{total})"
        await progress_msg.edit_text(progress_text)

    await progress_msg.edit_text(f"✅ 导出完成，共 {total} 张图片")
    logging.info(f"✅ 图片导出完成: {export_dir}")


# 模拟获取图片列表的函数
def get_images_list():
    return [
        "/app/images/img1.jpg",
        "/app/images/img2.jpg",
        "/app/images/img3.jpg"
    ]
