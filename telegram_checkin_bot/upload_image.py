# upload_image.py
import cloudinary
import cloudinary.uploader

# 使用你的 Cloudinary 账号信息（替换下面这几项）
cloudinary.config(
    cloud_name="your_cloud_name",
    api_key="your_api_key",
    api_secret="your_api_secret"
)

def upload_image(local_path):
    response = cloudinary.uploader.upload(local_path)
    return response["secure_url"]
