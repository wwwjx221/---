import spacy
import re
# 加载中文模型（需提前下载：python -m spacy download zh_core_web_lg）
nlp = spacy.load("zh_core_web_lg")  
text = "用户李四，身份证110105199001011234，联系电话13800138000,不知道还有什么乱七八糟的doing喜，我要和北邮的" \
"王晓旭一块出去玩，wangjia 也很有意思"
def read_txt_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        content = file.read()
    return content
ttt=read_txt_file('test.txt')

#[^\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]包含中文和生僻字的正则
#[^\u4e00-\u9fff]排除所有非中文
chinese_text = re.sub(r'[^\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]', '', ttt)
print(chinese_text)
doc = nlp(chinese_text)
for ent in doc.ents:
    # 假设训练/自定义规则让模型识别 "PERSON"（姓名）、"ID_CARD"（身份证号）等
    if ent.label_ in ["PERSON", "ORG","NORP"]:  
        print(ent.text,ent.label_)



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

id_pattern = re.compile(r"\d{17}[\dXx]")  
id_cards = id_pattern.findall(text)
valid_ids = [id for id in id_cards if is_valid_id_card(id)]
print(valid_ids)  # 输出: ['110105199001011234']（需校验通过才保留）

