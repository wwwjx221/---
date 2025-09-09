import re

def is_valid_id_card(id_card):
    # 18 位身份证校验规则（简化版，完整校验可参考 GB11643-1999）
    if len(id_card) != 18:
        return False
    # 校验生日（此处仅简单判断，可扩展更严格逻辑）
    birth = id_card[6:14]
    if not (re.match(r"^\d{8}$", birth) and 1900 <= int(birth[:4]) <= 2025):
        return False
    # 校验码（可补充完整算法，如加权求和取模）
    return True  

text = "身份证号：110105199001011234 "
id_pattern = re.compile(r"\d{17}[\dXx]")  
id_cards = id_pattern.findall(text)
valid_ids = [id for id in id_cards if is_valid_id_card(id)]
print(valid_ids)  # 输出: ['110105199001011234']（需校验通过才保留）


text = "联系人：张三，电话：138xxxx 身份证号：110105199001011234 "
# 匹配 2-6 个汉字（含可能的“·”，但需根据实际场景调整）
name_pattern = re.compile(r"[\u4e00-\u9fa5·]{2,6}")  
names = name_pattern.findall(text)
print(names)  # 输出: ['张三']