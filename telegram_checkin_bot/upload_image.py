# upload_image.py
import os
import cloudinary
import cloudinary.uploader

# ===========================
# Cloudinary 配置初始化
# ===========================
cloudinary.config(
    cloud_name=os.environ["cloudinary_cloud_name"],    # 云端名称（Cloudinary 控制台获取）
    api_key=os.environ["cloudinary_api_key"],          # API Key
    api_secret=os.environ["cloudinary_api_secret"]     # API Secret
)

# ===========================
# 上传本地图片到 Cloudinary
# ===========================
def upload_image(local_path: str) -> str:
    """
    将本地图片文件上传至 Cloudinary，并返回可公开访问的 secure_url。
    
    :param local_path: 本地图片文件路径
    :return: Cloudinary 图片的 HTTPS 访问 URL (secure_url)
    """
    response = cloudinary.uploader.upload(local_path)  # 执行上传操作
    return response["secure_url"]  # 返回图片安全链接 (https)
