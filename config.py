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

RAILWAY_API_KEY = os.getenv("RAILWAY_API_KEY")
SERVICE_ID = os.getenv("SERVICE_ID")
