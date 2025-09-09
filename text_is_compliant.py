import re
import time
import pymysql
import spacy
from minio import Minio
from urllib.parse import urlparse

# ==== 数据库配置 ====
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '1025',
    'database': 'vehicle_security',
    'charset': 'utf8mb4'
}

# ==== MinIO 配置 ====
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# ==== 初始化 MinIO 客户端 ====
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# ==== 获取数据库连接 ====
def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

# ==== 读取 MinIO 对象 ====
def read_from_minio(minio_url):
    parsed = urlparse(minio_url)
    
    # 分割路径，提取 bucket 和 object_name
    path_parts = parsed.path.lstrip('/').split('/', 1)
    if len(path_parts) != 2:
        print(f"❌ URL 格式错误，无法解析 bucket 和 object：{minio_url}")
        return ""

    bucket, object_name = path_parts

    try:
        response = minio_client.get_object(bucket, object_name)
        return response.read().decode("utf-8")
    except Exception as e:
        print(f"❌ MinIO 读取失败：{minio_url}，错误：{e}")
        return ""

# ==== 合规检测逻辑 ====
def is_text_compliant(text, nlp):
    sensitive_keywords = [
        r"\d{17}[\dXx]",     # 身份证号
        r"1[3-9]\d{9}",      # 手机号
        r"国家秘密", r"机密", r"敏感词",  # 可扩展
    ]
    for pattern in sensitive_keywords:
        if re.search(pattern, text):
            return False

    # NLP 实体识别
    chinese_text = re.sub(r'[^\u4e00-\u9fa5]', '', text)
    doc = nlp(chinese_text)
    for ent in doc.ents:
        if ent.label_ in ['PERSON', 'ORG', 'GPE']:
            return False

    return True

# ==== 更新合规状态 ====
def update_is_compliant(uid, compliant):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "UPDATE text SET is_compliant = %s WHERE fuid = %s"
            cursor.execute(sql, (1 if compliant else 0, uid))
        conn.commit()
        print(f"✅ 更新：uid={uid}，is_compliant={1 if compliant else 0}")
    except Exception as e:
        print(f"❌ 数据库更新失败：{e}")
    finally:
        conn.close()

# ==== 单个任务处理 ====
def process_record(record, nlp):
    uid, location = record
    text = read_from_minio(location)
    if not text:
        update_is_compliant(uid, False)
        return

    compliant = is_text_compliant(text, nlp)
    update_is_compliant(uid, compliant)

# ==== 主轮询函数 ====
def main():
    print("🚀 启动文本合规检测服务...")

    # 提前加载 NLP 模型
    try:
        nlp = spacy.load("zh_core_web_lg")
    except Exception as e:
        print(f"❌ NLP 模型加载失败：{e}")
        return

    while True:
        print("🔄 正在查询待处理文本...")
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT fuid, text_location FROM text WHERE is_compliant IS NULL LIMIT 10")
                records = cursor.fetchall()

            if records:
                for record in records:
                    process_record(record, nlp)
            else:
                print("⏸️ 无待处理文本，休眠 5 秒...")
                time.sleep(5)
        except Exception as e:
            print(f"❌ 查询失败：{e}")
        finally:
            conn.close()

if __name__ == "__main__":
    main()
