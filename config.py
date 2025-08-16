import os
import cloudinary
from datetime import time, timedelta, timezone
import pytz

# ===========================
# 打卡关键词配置
# ===========================
KEYWORDS = ["#上班打卡", "#下班打卡", "#补卡"]
# ✅ 定义所有机器人识别的关键词，用于消息文本匹配。

# ===========================
# 管理员配置
# ===========================
ADMIN_IDS = [6337749385, 6447602744, 6396094777, 6725923773, 6420133169, 6566783362]
# ✅ 管理员的 Telegram 用户 ID 列表，用于权限验证（如导出、补卡等管理功能）。

ADMIN_USERNAMES = ["Y_MS_KIDLAT"]
# ✅ 管理员的 Telegram 用户名列表，配合某些基于用户名的权限校验（如优化数据库）。

# ===========================
# 环境变量配置
# ===========================
TOKEN = os.getenv("TOKEN")
# ✅ Telegram Bot API Token，从环境变量读取，避免硬编码。

DATA_DIR = os.getenv("DATA_DIR", "./data")
# ✅ 数据存储目录（Excel、图片导出文件夹），默认 "./data"。

DATABASE_URL = os.getenv("DATABASE_URL")
# ✅ PostgreSQL 数据库连接 URL，从环境变量读取。

# ===========================
# Cloudinary 云存储配置
# ===========================
cloudinary.config(
    cloud_name=os.environ["cloudinary_cloud_name"],    # 云端名称（Cloudinary 控制台提供）
    api_key=os.environ["cloudinary_api_key"],          # Cloudinary API Key
    api_secret=os.environ["cloudinary_api_secret"]     # Cloudinary API Secret
)
# ✅ 初始化 Cloudinary 客户端，用于图片上传、删除、导出等操作。


# BEIJING_TZ = pytz.timezone("Asia/Shanghai")
from zoneinfo import ZoneInfo
BEIJING_TZ = ZoneInfo("Asia/Shanghai")

LOGS_PER_PAGE = 7

# ===========================
# 通用日志构建函数
# ===========================
async def build_and_send_logs(update, context, logs, target_name, key="mylogs"):
    if not logs:
        await update.message.reply_text(f"📭 {target_name} 本月暂无打卡记录。")
        return

    # 转换时区 & 排序
    logs = [(parse(ts) if isinstance(ts, str) else ts, kw, shift) for ts, kw, shift in logs]
    logs = [(ts.astimezone(BEIJING_TZ), kw, shift) for ts, kw, shift in logs]
    logs = sorted(logs, key=lambda x: x[0])

    # 按天组合
    daily_map = defaultdict(dict)
    i = 0
    while i < len(logs):
        ts, kw, shift = logs[i]
        date_key = ts.date()
        if kw == "#下班打卡" and ts.hour < 6:
            date_key = (ts - timedelta(days=1)).date()

        if kw == "#上班打卡":
            daily_map[date_key]["shift"] = shift
            daily_map[date_key]["#上班打卡"] = ts
            j = i + 1
            while j < len(logs):
                ts2, kw2, _ = logs[j]
                if kw2 == "#下班打卡" and timedelta(0) < (ts2 - ts) <= timedelta(hours=12):
                    daily_map[date_key]["#下班打卡"] = ts2
                    break
                j += 1
            i = j if j > i else i + 1
        else:
            daily_map[date_key]["#下班打卡"] = ts
            i += 1

    all_days = sorted(daily_map.keys())

    # 统计
    total_complete = total_abnormal = total_makeup = 0
    for day in all_days:
        kw_map = daily_map[day]
        shift_full = kw_map.get("shift", "未选择班次")
        is_makeup = shift_full.endswith("（补卡）")
        shift_name = shift_full.split("（")[0]
        has_up = "#上班打卡" in kw_map
        has_down = "#下班打卡" in kw_map
        has_late = has_early = False

        if is_makeup:
            total_makeup += 1

        if has_up and shift_name in get_shift_times_short():
            start_time, _ = get_shift_times_short()[shift_name]
            if kw_map["#上班打卡"].time() > start_time:
                has_late = True

        if has_down and shift_name in get_shift_times_short():
            _, end_time = get_shift_times_short()[shift_name]
            down_ts = kw_map["#下班打卡"]
            if shift_name == "I班" and down_ts.date() == day:
                has_early = True
            elif shift_name != "I班" and down_ts.time() < end_time:
                has_early = True

        if not is_makeup:
            if has_up:
                total_abnormal += 1 if has_late else 0
                total_complete += 1 if not has_late else 0
            if has_down:
                total_abnormal += 1 if has_early else 0
                total_complete += 1 if not has_early else 0

    # 分页
    pages = [all_days[i:i + LOGS_PER_PAGE] for i in range(0, len(all_days), LOGS_PER_PAGE)]
    context.user_data[f"{key}_pages"] = {
        "pages": pages,
        "daily_map": daily_map,
        "page_index": 0,
        "summary": (total_complete, total_abnormal, total_makeup),
        "target_name": target_name
    }

    if key == "mylogs":
        await send_mylogs_page(update, context)
    else:
        await send_userlogs_page(update, context)
