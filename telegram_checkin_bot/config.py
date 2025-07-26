TOKEN = "8462739794:AAEd1ZCbu9srOaay87FC0t-2VXSdNJCbGP0"
KEYWORDS = ["#上班打卡", "#下班打卡"]
 # Y_MS_KIDLAT，Y_MS_Rubus1，Y_MS_podhale，Y_MS_Jehoshwu，Y_MS_Menbo，Y_MS_Racheluua
ADMIN_IDS = [6337749385, 6447602744, 6396094777, 6725923773, 6420133169, 6566783362]
DATA_DIR = "data"

import cloudinary
import os

cloudinary.config(
    cloud_name=os.environ["cloudinary_cloud_name"],
    api_key=os.environ["cloudinary_api_key"],
    api_secret=os.environ["cloudinary_api_secret"]
)
