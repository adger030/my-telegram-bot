import os
import cloudinary

KEYWORDS = ["#上班打卡", "#下班打卡", "#补卡"]
ADMIN_IDS = [6337749385, 6447602744, 6396094777, 6725923773, 6420133169, 6566783362]
# 添加管理员用户名
ADMIN_USERNAMES = ["Y_MS_KIDLAT"]

TOKEN = os.getenv("TOKEN")
DATA_DIR = os.getenv("DATA_DIR", "./data")
DATABASE_URL = os.getenv("DATABASE_URL")

cloudinary.config(
    cloud_name=os.environ["cloudinary_cloud_name"],
    api_key=os.environ["cloudinary_api_key"],
    api_secret=os.environ["cloudinary_api_secret"]
)
