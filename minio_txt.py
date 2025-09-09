import spacy
import re
from minio import Minio
from minio.error import S3Error

# ==== 加载中文 NLP 模型 ====
try:
    nlp = spacy.load("zh_core_web_lg")
except OSError:
    print("请先安装中文模型：python -m spacy download zh_core_web_lg")
    exit(1)

# ==== MinIO 配置 ====
MINIO_ENDPOINT = "localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "text"
OBJECT_NAME = "test.txt"  # MinIO 中的目标对象名

client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

def read_from_minio(bucket, object_name):
    """从 MinIO 中读取文件内容"""
    try:
        response = client.get_object(bucket, object_name)
        return response.read().decode("utf-8")
    except S3Error as err:
        print(f"❌ 无法从 MinIO 读取对象：{object_name}，错误：{err}")
        return ""

# ==== 原有函数保持不变 ====

def is_valid_id_card(id_card):
    """验证身份证号是否有效（简化版）"""
    if len(id_card) != 18:
        return False
    birth = id_card[6:14]
    if not (re.match(r"^\d{8}$", birth) and 1900 <= int(birth[:4]) <= 2025):
        return False
    return True

def detect_sensitive_info(text, filename=""):
    """检测文本中的敏感信息"""
    if not text:
        print(f"⚠️ 空内容，跳过 {filename}")
        return
    
    print(f"\n===== 敏感信息检测结果（文件：{filename}） =====")

    # 1. 检测身份证号
    id_pattern = re.compile(r"\d{17}[\dXx]")
    id_cards = id_pattern.findall(text)
    valid_ids = [id for id in id_cards if is_valid_id_card(id)]
    for id_card in valid_ids:
        print(f"敏感信息类型: 身份证号, 内容: {id_card}")

    # 2. 检测电话号码
    phone_pattern = re.compile(r"1[3-9]\d{9}")
    phone_numbers = phone_pattern.findall(text)
    for phone in phone_numbers:
        print(f"敏感信息类型: 电话号码, 内容: {phone}")

    # 3. 使用 spaCy 检测人名、组织机构等
    chinese_text = re.sub(r'[^\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]', '', text)
    doc = nlp(chinese_text)

    entity_type_map = {
        "PERSON": "人名",
        "ORG": "组织机构",
        "NORP": "民族/宗教/政治团体"
    }

    for ent in doc.ents:
        if ent.label_ in entity_type_map:
            print(f"敏感信息类型: {entity_type_map[ent.label_]}, 内容: {ent.text}")

# ==== 主程序入口 ====
if __name__ == "__main__":
    text = read_from_minio(BUCKET_NAME, OBJECT_NAME)
    detect_sensitive_info(text)
