import cloudinary
import cloudinary.uploader
import os

# 初始化配置
cloudinary.config(
    cloud_name=os.environ["CLOUDINARY_CLOUD_NAME"],
    api_key=os.environ["CLOUDINARY_API_KEY"],
    api_secret=os.environ["CLOUDINARY_API_SECRET"]
)

def upload_image(local_path, public_id=None):
    response = cloudinary.uploader.upload(
        local_path,
        public_id=public_id,
        folder="telegram_checkin",
        overwrite=True,
        resource_type="image"
    )
    return response['secure_url']  # 可保存这个 URL 到数据库
