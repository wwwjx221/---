import os
import time
import pymysql
import cv2
import torch
import numpy as np
from minio import Minio
from urllib.parse import urlparse

# ------------------ 配置区域 ------------------ #
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '1025',
    'database': 'vehicle_security',
    'charset': 'utf8mb4'
}

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

MODEL_PATH = 'yolov5s.pt'
POLL_INTERVAL = 5  # 轮询时间（秒）
# --------------------------------------------- #

# 初始化 MinIO 客户端
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  
)


def load_model():
    print("[INFO] 加载 YOLOv5 模型中...")
    model = torch.hub.load('ultralytics/yolov5', 'custom', path=MODEL_PATH, force_reload=False)
    model.conf = 0.25
    print("[INFO] 模型加载完成")
    return model


def download_image_from_minio(pic_url, save_path):
    try:
        parsed = urlparse(pic_url)
        path_parts = parsed.path.strip("/").split("/", 1)
        if len(path_parts) != 2:
            print(f"[ERROR] URL 格式不正确：{pic_url}")
            return False
        bucket, object_name = path_parts
        minio_client.fget_object(bucket, object_name, save_path)
        return True
    except Exception as e:
        print(f"[ERROR] 下载图片失败：{e}")
        return False


def detect_plate_compliance(model, image_path):
    try:
        results = model(image_path)
        labels = results.pandas().xyxy[0]['name'].tolist()
        return 0 if 'license-plate' in labels else 1
    except Exception as e:
        print(f"[ERROR] 检测失败：{e}")
        return 1  # 检测失败视为合规



def process_unchecked_images(model):
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT fuid, pic_location FROM picture WHERE is_compliant IS NULL")
        rows = cursor.fetchall()

        for row in rows:
            fuid, pic_url = row
            print(f"[INFO] 处理图片 fuid={fuid}，路径={pic_url}")
            img_path = f"/tmp/{fuid}.jpg"
            if not download_image_from_minio(pic_url, img_path):
                continue

            is_compliant = detect_plate_compliance(model, img_path)

            try:
                cursor.execute(
                    "UPDATE picture SET is_compliant = %s WHERE fuid = %s",
                    (is_compliant, fuid)
                )
                conn.commit()
                print(f"[INFO] 图片 fuid={fuid} 合规性={is_compliant} 已写入数据库")
            except Exception as e:
                print(f"[ERROR] 数据写入失败：{e}")

            os.remove(img_path)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] 数据库连接或查询失败：{e}")


def main():
    model = load_model()
    print("[INFO] 图片合规性检测监听器已启动，按 Ctrl+C 可退出")
    while True:
        process_unchecked_images(model)
        time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    main()

