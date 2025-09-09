import spacy
import re

# 加载中文模型（需提前下载：python -m spacy download zh_core_web_lg）
try:
    nlp = spacy.load("zh_core_web_lg")
except OSError:
    print("请先安装中文模型：python -m spacy download zh_core_web_lg")
    exit(1)

def read_txt_file(filepath):
    """读取文本文件内容"""
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        print(f"错误：文件 '{filepath}' 不存在")
        return ""
    except Exception as e:
        print(f"读取文件时出错：{str(e)}")
        return ""

def is_valid_id_card(id_card):
    """验证身份证号是否有效（简化版）"""
    if len(id_card) != 18:
        return False
    # 校验生日部分
    birth = id_card[6:14]
    if not (re.match(r"^\d{8}$", birth) and 1900 <= int(birth[:4]) <= 2025):
        return False
    return True

def detect_sensitive_info(filepath):
    """检测文件中的敏感信息并统一输出格式"""
    # 读取文件内容
    text = read_txt_file(filepath)
    if not text:
        return
    
    print(f"===== 敏感信息检测结果（文件：{filepath}） =====")
    
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
    
    # 3. 使用spaCy检测人名、组织机构等
    # 保留中文字符用于命名实体识别
    chinese_text = re.sub(r'[^\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]', '', text)
    doc = nlp(chinese_text)
    
    # 实体类型映射，统一输出名称
    entity_type_map = {
        "PERSON": "人名",
        "ORG": "组织机构",
        "NORP": "民族/宗教/政治团体"
    }
    
    for ent in doc.ents:
        if ent.label_ in entity_type_map:
            print(f"敏感信息类型: {entity_type_map[ent.label_]}, 内容: {ent.text}")

# 示例使用
if __name__ == "__main__":
    # 替换为你的文本文件路径
    detect_sensitive_info("test.txt")
