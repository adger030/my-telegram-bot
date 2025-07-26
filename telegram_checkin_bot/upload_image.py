# upload_image.py
import os
import cloudinary
import cloudinary.uploader

cloudinary.config(
    cloud_name=os.environ["cloudinary_cloud_name"],
    api_key=os.environ["cloudinary_api_key"],
    api_secret=os.environ["cloudinary_api_secret"]
)

def upload_image(local_path):
    response = cloudinary.uploader.upload(local_path)
    return response["secure_url"]
