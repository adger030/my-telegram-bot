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
    df = _fetch_data(start_datetime, end_datetime)
    if df.empty:
        logging.warning("⚠️ 指定日期内没有数据")
        return None

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

    missed_days_count = {u: 0 for u in all_user_names}

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        sheet_written = False

        for day, group_df in df.groupby("date"):
            group_df = group_df.copy()
            group_df["remark"] = ""

            # 当天有上班卡的用户
            checked_users = set(
                group_df.loc[group_df["keyword"] == "#上班打卡", "name"].unique()
            )
            missed_users = []

            # 获取当天日期范围
            day_date = datetime.strptime(day, "%Y-%m-%d").date()
            day_start = datetime.combine(day_date, datetime.min.time())
            day_end = day_start + timedelta(days=1)

            for u in all_user_names:
                if u not in checked_users:
                    missed_users.append(u)
                    missed_days_count[u] += 1

            if missed_users:
                missed_df = pd.DataFrame({
                    "name": missed_users,
                    "timestamp": pd.NaT,
                    "keyword": None,
                    "shift": None,
                    "remark": "未打上班卡"
                })
                group_df = pd.concat([group_df, missed_df], ignore_index=True)

            # ===== 迟到 / 早退 / 补卡 =====
            for idx, row in group_df.iterrows():
                shift_val = row["shift"]
                keyword = row["keyword"]
                ts = row["timestamp"]

                if not shift_val or pd.isna(ts):
                    continue

                shift_text = str(shift_val).strip()
                shift_name = re.split(r'[（(]', shift_text)[0]

                if "补卡" in shift_text:
                    group_df.at[idx, "remark"] = "补卡"
                    continue

                if shift_name in get_shift_times_short():
                    start_time, end_time = get_shift_times_short()[shift_name]
                    ts_time = ts.time()

                    if keyword == "#上班打卡" and ts_time > start_time:
                        group_df.at[idx, "remark"] = "迟到"

                    elif keyword == "#下班打卡":
                        if shift_name == "I班":
                            if not (ts.hour == 0):
                                if 15 <= ts.hour <= 23:
                                    group_df.at[idx, "remark"] = "早退"
                        else:
                            if not (0 <= ts.hour <= 1):
                                if ts_time < end_time:
                                    group_df.at[idx, "remark"] = "早退"

            # === 改造点：用户+时间排序 ===
            group_df = group_df.sort_values(["name", "timestamp"], na_position="last")
            slim_df = group_df[["name", "timestamp", "keyword", "shift", "remark"]].copy()
            slim_df.columns = ["姓名", "打卡时间", "关键词", "班次", "备注"]

            slim_df["打卡时间"] = pd.to_datetime(slim_df["打卡时间"], errors="coerce").dt.tz_localize(None)
            slim_df["班次"] = slim_df["班次"].apply(format_shift)

            slim_df.to_excel(writer, sheet_name=day[:31], index=False)
            sheet_written = True

        if not sheet_written:
            pd.DataFrame(columns=["姓名", "打卡时间", "关键词", "班次", "备注"]).to_excel(
                writer, sheet_name="空表", index=False
            )

    # ===== 样式处理 =====
    wb = load_workbook(excel_path)
    red_fill = PatternFill(start_color="ffc8c8", end_color="ffc8c8", fill_type="solid")
    yellow_fill = PatternFill(start_color="fff1c8", end_color="fff1c8", fill_type="solid")
    blue_fill_light = PatternFill(start_color="c8eaff", end_color="c8eaff", fill_type="solid")

    thin_border = Border(
        left=Side(style="thin", color="000000"),
        right=Side(style="thin", color="000000"),
        top=Side(style="thin", color="000000"),
        bottom=Side(style="thin", color="000000")
    )

    from itertools import cycle
    user_fills = cycle([
        PatternFill(start_color="f9f9f9", end_color="f9f9f9", fill_type="solid"),
        PatternFill(start_color="ffffff", end_color="ffffff", fill_type="solid"),
    ])

    for sheet in wb.worksheets:
        if sheet.title == "统计":
            continue

        current_user = None
        current_fill = next(user_fills)

        for row in sheet.iter_rows(min_row=2):
            name_val = row[0].value
            remark_val = str(row[4].value or "")

            # 用户区块交替颜色
            if name_val != current_user:
                current_fill = next(user_fills)
                current_user = name_val
            for cell in row:
                cell.fill = current_fill

            # 特殊备注覆盖（迟到/早退/补卡/未打卡）
            if "迟到" in remark_val or "早退" in remark_val:
                for cell in row:
                    cell.fill = red_fill
            elif "补卡" in remark_val:
                for cell in row:
                    cell.fill = yellow_fill
            elif "未打上班卡" in remark_val:
                for cell in row:
                    cell.fill = blue_fill_light

    # ===== 统计表生成（保持原逻辑） =====
    stats = []
    for sheet in wb.worksheets:
        if sheet.title == "统计":
            continue
        for row in sheet.iter_rows(min_row=2, values_only=True):
            name, _, _, _, remark = row
            if not name:
                continue
            if remark == "未打上班卡":
                continue
            elif remark == "补卡":
                status = "补卡"
            elif remark in ("迟到", "早退"):
                status = "迟到/早退"
            else:
                status = "正常"
            stats.append({"姓名": name, "状态": status})

    stats_df = pd.DataFrame(stats)
    if not stats_df.empty:
        summary_df = stats_df.groupby(["姓名", "状态"]).size().unstack(fill_value=0).reset_index()
    else:
        summary_df = pd.DataFrame(columns=["姓名", "正常", "迟到/早退", "补卡"])

    for user in all_user_names:
        if user not in summary_df["姓名"].values:
            summary_df = pd.concat([
                summary_df,
                pd.DataFrame([{"姓名": user}])
            ], ignore_index=True)

    for col in ["正常", "迟到/早退", "补卡"]:
        if col not in summary_df.columns:
            summary_df[col] = 0
    summary_df = summary_df.fillna(0).astype({"正常": int, "迟到/早退": int, "补卡": int})

    summary_df["未打上班卡"] = summary_df["姓名"].map(missed_days_count)
    summary_df["异常总数"] = summary_df["迟到/早退"] + summary_df["补卡"]

    summary_df = summary_df[["姓名", "正常", "未打上班卡", "迟到/早退", "补卡", "异常总数"]]
    summary_df = summary_df.sort_values(by="正常", ascending=False)

    stats_sheet = wb.create_sheet("统计", 0)
    headers = ["姓名", "正常打卡", "未打上班卡", "迟到/早退", "补卡", "异常总数"]
    for r_idx, row in enumerate([headers] + summary_df.values.tolist(), 1):
        for c_idx, value in enumerate(row, 1):
            stats_sheet.cell(row=r_idx, column=c_idx, value=value)

    stats_sheet.freeze_panes = "A2"
    header_font = Font(bold=True)
    center_align = Alignment(horizontal="center")
    blue_fill = PatternFill(start_color="ffc8c8", end_color="ffc8c8", fill_type="solid")

    for cell in stats_sheet[1]:
        cell.font = header_font
        cell.alignment = center_align

    for row in stats_sheet.iter_rows(min_row=2):
        for col_idx in [6]:
            row[col_idx - 1].fill = blue_fill

    # ===== 列宽、边框 =====
    for sheet in wb.worksheets:
        sheet.freeze_panes = "A2"
        for cell in sheet[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center", vertical="center")

        for col in sheet.columns:
            col_letter = col[0].column_letter
            max_length = 0
            for cell in col:
                if isinstance(cell.value, datetime):
                    length = 19
                else:
                    length = len(str(cell.value or ""))
                max_length = max(max_length, length)
                cell.alignment = Alignment(horizontal="center", vertical="center")
                cell.border = thin_border
            sheet.column_dimensions[col_letter].width = min(max_length + 8, 30)

    wb.save(excel_path)
    logging.info(f"✅ Excel 导出完成: {excel_path}")
    return excel_path

