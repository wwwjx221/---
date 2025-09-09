import os
import time
import json
import threading
import queue
import mysql.connector
from minio import Minio
from minio.error import S3Error

# ========== 参数配置 ==========
ZEEK_OUTPUT_DIR = "/home/gch/Desktop/zeek_test/zeek_output"
CHECK_INTERVAL = 3
NUM_WORKERS = 4
HANDLED_DIRS = set()
TASK_QUEUE = queue.Queue()

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '1025',
    'database': 'vehicle_security',
    'charset': 'utf8mb4'
}

MINIO_CONFIG = {
    'endpoint': 'localhost:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin',
    'picture_bucket': 'iov-picture',
    'audio_bucket': 'iov-audio',
    'text_bucket': 'iov-text'
}

# ========== 工具函数 ==========

def connect_db():
    return mysql.connector.connect(**DB_CONFIG)

def get_minio_client():
    return Minio(
        MINIO_CONFIG['endpoint'],
        access_key=MINIO_CONFIG['access_key'],
        secret_key=MINIO_CONFIG['secret_key'],
        secure=False
    )

def upload_to_minio(bucket: str, file_path: str, object_name: str):
    minio_client = get_minio_client()

    # 自动创建桶
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    minio_client.fput_object(bucket, object_name, file_path)
    return f"http://{MINIO_CONFIG['endpoint']}/{bucket}/{object_name}"

# ========== 主处理逻辑 ==========

def process_zeek_logs(entry, conn_log_path, files_log_path, pcap_location):
    conn = connect_db()
    cursor = conn.cursor()

    # === 插入 conn.log 到 flow 表 ===
    if os.path.exists(conn_log_path):
        with open(conn_log_path, 'r') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                try:
                    record = json.loads(line)
                    ts = float(record.get('ts', 0))
                    uid = record.get('uid', '')
                    id_orig_h = record.get('id.orig_h', '')
                    id_orig_p = record.get('id.orig_p', 0)
                    id_resp_h = record.get('id.resp_h', '')
                    id_resp_p = record.get('id.resp_p', 0)
                    proto = record.get('proto', '')

                    sql = """
                        INSERT INTO flow (ts, uid, id_orig_h, id_orig_p, id_resp_h, id_resp_p, proto, pcap_location)
                        VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql, (
                        ts, uid, id_orig_h, id_orig_p,
                        id_resp_h, id_resp_p, proto,
                        pcap_location
                    ))
                except Exception as e:
                    print(f"[conn.log] 插入失败 ({entry}): {e}")
                    continue

    # === 插入 files.log 到 picture / audio /text表，并上传文件到 MinIO ===
    if os.path.exists(files_log_path):
        with open(files_log_path, 'r') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                try:
                    record = json.loads(line)
                    ts = float(record.get('ts', 0))
                    fuid = record.get('fuid', '')
                    uid = record.get('uid', '')
                    mime_type = record.get('mime_type', '')
                    extracted_file = record.get('extracted', '')

                    if not extracted_file:
                        continue

                    extracted_path = os.path.join(ZEEK_OUTPUT_DIR, entry, "extracted", extracted_file)
                    if not os.path.exists(extracted_path):
                        continue

                    object_name = f"{entry}/{extracted_file}"

                    if mime_type.startswith('image/'):
                        # 上传图片到 MinIO
                        minio_url = upload_to_minio(MINIO_CONFIG['picture_bucket'], extracted_path, object_name)
                        sql = """
                            INSERT INTO picture (fuid, uid, ts, extracted, pic_location)
                            VALUES (%s, %s, FROM_UNIXTIME(%s), %s, %s)
                        """
                        cursor.execute(sql, (fuid, uid, ts, extracted_path, minio_url))

                    elif mime_type == 'video/mp4':
                        # 上传音频到 MinIO
                        minio_url = upload_to_minio(MINIO_CONFIG['audio_bucket'], extracted_path, object_name)
                        sql = """
                            INSERT INTO audio (fuid, uid, ts, extracted, audio_location)
                            VALUES (%s, %s, FROM_UNIXTIME(%s), %s, %s)
                        """
                        cursor.execute(sql, (fuid, uid, ts, extracted_path, minio_url))
                    elif mime_type.startswith('text/') or mime_type in ['application/json', 'application/xml']:
                        minio_url = upload_to_minio(MINIO_CONFIG['text_bucket'], extracted_path, object_name)
                        sql = """
                            INSERT INTO text (fuid, uid, ts, extracted, text_location)
                            VALUES (%s, %s, FROM_UNIXTIME(%s), %s, %s)
                        """
                        cursor.execute(sql, (fuid, uid, ts, extracted_path, minio_url))

                except Exception as e:
                    print(f"[files.log] 插入失败 ({entry}): {e}")
                    continue

    conn.commit()
    cursor.close()
    conn.close()
    print(f"[✓] 目录 {entry} 数据入库与上传完成。")

# ========== 工作线程 ==========

def worker():
    while True:
        task = TASK_QUEUE.get()
        if task is None:
            break
        try:
            entry, conn_log_path, files_log_path, pcap_location = task
            process_zeek_logs(entry, conn_log_path, files_log_path, pcap_location)
        except Exception as e:
            print(f"[!] 工作线程异常: {e}")
        finally:
            TASK_QUEUE.task_done()

# ========== 主监控线程 ==========

def monitor_zeek_output():
    print(f"[*] 启动监听目录: {ZEEK_OUTPUT_DIR}")

    while True:
        for entry in os.listdir(ZEEK_OUTPUT_DIR):
            entry_path = os.path.join(ZEEK_OUTPUT_DIR, entry)
            if os.path.isdir(entry_path) and entry not in HANDLED_DIRS:
                conn_log_path = os.path.join(entry_path, "conn.log")
                files_log_path = os.path.join(entry_path, "files.log")
                if os.path.exists(conn_log_path) and os.path.exists(files_log_path):
                    pcap_location = os.path.join(ZEEK_OUTPUT_DIR, f"{entry}.pcap")
                    print(f"[+] 检测到新目录: {entry}，添加入队列")
                    TASK_QUEUE.put((entry, conn_log_path, files_log_path, pcap_location))
                    HANDLED_DIRS.add(entry)
        time.sleep(CHECK_INTERVAL)

# ========== 主程序入口 ==========

if __name__ == "__main__":
    for _ in range(NUM_WORKERS):
        threading.Thread(target=worker, daemon=True).start()

    try:
        monitor_zeek_output()
    except KeyboardInterrupt:
        print("[!] 收到中断信号，退出...")
