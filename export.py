import os
import re
import pandas as pd
import pytz
import logging
from datetime import datetime, timedelta
from config import DATA_DIR, DATABASE_URL, BEIJING_TZ
import cloudinary
import cloudinary.uploader
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from shift_manager import get_shift_times_short
from sqlalchemy import create_engine
from db_pg import get_conn 
from collections import defaultdict


# ===========================
# 基础配置
# ===========================
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

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

# 获取所有用户姓名
def get_all_user_names():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM users;")
            return [row[0] for row in cur.fetchall()]

# 导出打卡记录
def export_excel(start_datetime: datetime, end_datetime: datetime):
    from collections import defaultdict

    df = _fetch_data(start_datetime, end_datetime)
    if df.empty:
        logging.warning("⚠️ 指定日期内没有数据")
        export_dir = os.path.join(
            DATA_DIR,
            f"excel_{start_datetime:%Y-%m-%d}_{end_datetime - pd.Timedelta(seconds=1):%Y-%m-%d}"
        )
        os.makedirs(export_dir, exist_ok=True)
        excel_path = os.path.join(
            export_dir,
            f"打卡记录_{start_datetime:%Y-%m-%d}_{end_datetime - pd.Timedelta(seconds=1):%Y-%m-%d}.xlsx"
        )
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            pd.DataFrame(columns=["姓名", "打卡时间", "关键词", "班次", "备注"]).to_excel(
                writer, sheet_name="空表", index=False
            )
        return excel_path

    # ================= 时间处理 =================
    if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        try:
            df["timestamp"] = df["timestamp"].dt.tz_localize(None)
        except AttributeError:
            pass
    df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")

    start_str = start_datetime.strftime("%Y-%m-%d")
    end_str = (end_datetime - pd.Timedelta(seconds=1)).strftime("%Y-%m-%d")
    export_dir = os.path.join(DATA_DIR, f"excel_{start_str}_{end_str}")
    os.makedirs(export_dir, exist_ok=True)
    excel_path = os.path.join(export_dir, f"打卡记录_{start_str}_{end_str}.xlsx")

    all_user_names = get_all_user_names()

    def format_shift(shift):
        if pd.isna(shift):
            return shift
        shift_text = str(shift)
        if re.search(r'（\d{2}:\d{2}-\d{2}:\d{2}）', shift_text):
            return shift_text
        shift_name = shift_text.split("（")[0]
        if shift_name in get_shift_times_short():
            start, end = get_shift_times_short()[shift_name]
            return f"{shift_text}（{start.strftime('%H:%M')}-{end.strftime('%H:%M')}）"
        return shift_text

    # ================= I班跨天处理 =================
    i_shift_mask = (
        (df["keyword"] == "#下班打卡")
        & (df["shift"].notna())
        & (df["shift"].astype(str).str.startswith("I班"))
        & (df["timestamp"].dt.hour < 6)
    )
    cross_df = df[i_shift_mask].copy()
    df = df[~i_shift_mask]
    cross_df["remark"] = cross_df.get("remark", "") + "（次日）"
    cross_df["date"] = (cross_df["timestamp"] - pd.Timedelta(days=1)).dt.strftime("%Y-%m-%d")
    df = pd.concat([df, cross_df], ignore_index=True)

    # ================= 写入 Excel =================
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for day, group_df in df.groupby("date"):
            group_df = group_df.copy()
            if "remark" not in group_df.columns:
                group_df["remark"] = ""

            # 创建 sheet
            sheet_name = day[:31]
            sheet = writer.book.create_sheet(sheet_name)
            headers = ["姓名", "打卡时间", "关键词", "班次", "备注"]
            sheet.append(headers)

            for user in all_user_names:
                user_df = group_df[group_df["name"] == user]

                if user_df.empty:
                    # 🔹 当天完全没有记录，写“休息”
                    sheet.append([user, "", "", "", "（休息）"])
                    continue

                start_row = sheet.max_row + 1  # 记录姓名开始行

                # 上班记录
                up_row = user_df[user_df["keyword"] == "#上班打卡"].sort_values("timestamp").head(1)
                if not up_row.empty:
                    ts = up_row.iloc[0]["timestamp"]
                    shift = format_shift(up_row.iloc[0]["shift"])
                    remark = up_row.iloc[0].get("remark", "")
                    sheet.append([user, ts.strftime("%H:%M:%S"), "#上班打卡", shift, remark])
                else:
                    sheet.append([user, "", "#上班打卡", "", "未打上班卡"])

                # 下班记录
                down_row = user_df[user_df["keyword"] == "#下班打卡"].sort_values("timestamp").head(1)
                if not down_row.empty:
                    ts = down_row.iloc[0]["timestamp"]
                    shift = format_shift(down_row.iloc[0]["shift"])
                    remark = down_row.iloc[0].get("remark", "")
                    sheet.append([user, ts.strftime("%H:%M:%S"), "#下班打卡", shift, remark])
                else:
                    sheet.append([user, "", "#下班打卡", "", "未打下班卡"])

                end_row = sheet.max_row  # 姓名结束行

                # 🔹 合并姓名列（如果是两行以上）
                if end_row > start_row:
                    sheet.merge_cells(start_row=start_row, start_column=1, end_row=end_row, end_column=1)

    # ================= 样式处理 =================
    wb = load_workbook(excel_path)
    red_fill = PatternFill(start_color="ffc8c8", end_color="ffc8c8", fill_type="solid")        # 迟到/早退
    yellow_fill = PatternFill(start_color="fff1c8", end_color="fff1c8", fill_type="solid")     # 补卡
    blue_fill_light = PatternFill(start_color="c8eaff", end_color="c8eaff", fill_type="solid") # 未打卡
    green_fill = PatternFill(start_color="c8ffc8", end_color="c8ffc8", fill_type="solid")      # 休息
    thin_border = Border(
        left=Side(style="thin", color="000000"),
        right=Side(style="thin", color="000000"),
        top=Side(style="thin", color="000000"),
        bottom=Side(style="thin", color="000000")
    )

    for sheet in wb.worksheets:
        if sheet.title == "统计":
            continue
        for row in sheet.iter_rows(min_row=2):
            name_val, _, _, _, remark_val = [cell.value for cell in row]
            remark_val = str(remark_val or "")

            if "迟到" in remark_val or "早退" in remark_val:
                for cell in row: cell.fill = red_fill
            elif "补卡" in remark_val:
                for cell in row: cell.fill = yellow_fill
            elif "未打上班卡" in remark_val or "未打下班卡" in remark_val:
                for cell in row: cell.fill = blue_fill_light
            elif "休息" in remark_val:
                for cell in row: cell.fill = green_fill

            for cell in row:
                cell.border = thin_border
                cell.alignment = Alignment(horizontal="center", vertical="center")

    # ================= 统计表 =================
    stats = {u: {"正常": 0, "未打上班卡": 0, "未打下班卡": 0, "迟到/早退": 0, "补卡": 0} for u in all_user_names}
    for sheet in wb.worksheets:
        if sheet.title == "统计":
            continue
        df_sheet = pd.DataFrame(sheet.values)
        if df_sheet.empty or len(df_sheet.columns) < 5:
            continue
        df_sheet.columns = ["姓名", "打卡时间", "关键词", "班次", "备注"]

        for _, row in df_sheet.iterrows():
            name, kw, remark = row["姓名"], row["关键词"], str(row["备注"] or "")
            if not name or name not in stats:
                continue

            if "休息" in remark:
                continue
            if "补卡" in remark:
                stats[name]["补卡"] += 1
            elif "迟到" in remark or "早退" in remark:
                stats[name]["迟到/早退"] += 1
            elif "未打上班卡" in remark:
                stats[name]["未打上班卡"] += 1
            elif "未打下班卡" in remark:
                stats[name]["未打下班卡"] += 1
            else:
                stats[name]["正常"] += 1

    summary_df = pd.DataFrame([
        {"姓名": u, **v, "异常总数": v["未打上班卡"] + v["未打下班卡"] + v["迟到/早退"] + v["补卡"]}
        for u, v in stats.items()
    ])
    summary_df = summary_df[["姓名", "正常", "未打上班卡", "未打下班卡", "迟到/早退", "补卡", "异常总数"]]
    summary_df = summary_df.sort_values(by="正常", ascending=False)

    stats_sheet = wb.create_sheet("统计", 0)
    headers = ["姓名", "正常打卡", "未打上班卡", "未打下班卡", "迟到/早退", "补卡", "异常总数"]
    for r_idx, row in enumerate([headers] + summary_df.values.tolist(), 1):
        for c_idx, value in enumerate(row, 1):
            stats_sheet.cell(row=r_idx, column=c_idx, value=value)

    # ================= 自动列宽 + 居中 + 边框 =================
    for sheet in wb.worksheets:
        sheet.freeze_panes = "A2"
        for cell in sheet[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center", vertical="center")
        for col in sheet.columns:
            col_letter = col[0].column_letter
            max_length = max((19 if isinstance(cell.value, datetime) else len(str(cell.value or "")) for cell in col))
            for cell in col:
                cell.alignment = Alignment(horizontal="center", vertical="center")
                cell.border = thin_border
            sheet.column_dimensions[col_letter].width = min(max_length + 8, 30)

    wb.save(excel_path)
    logging.info(f"✅ Excel 导出完成: {excel_path}")
    return excel_path

