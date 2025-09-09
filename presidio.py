from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer
from presidio_analyzer.nlp_engine import SpacyNlpEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig  # 这里换了
import spacy
import pandas as pd

# ---------------------
# 初始化分析器和模型
# ---------------------

try:
    nlp_zh = spacy.load("zh_core_web_lg")
    nlp_en = spacy.load("en_core_web_lg")
except OSError:
    print("语言模型未找到，请先安装：")
    print("python -m spacy download zh_core_web_sm")
    print("python -m spacy download en_core_web_lg")
    exit(1)

nlp_engine = SpacyNlpEngine(models={"zh": nlp_zh, "en": nlp_en})
analyzer = AnalyzerEngine(nlp_engine=nlp_engine)

# ---------------------
# 添加自定义识别器
# ---------------------

id_pattern = Pattern(
    name="chinese_id_card",
    pattern=r"\d{17}[\dXx]|\d{15}",
    score=0.9
)
id_detector = PatternRecognizer(
    patterns=[id_pattern],
    context=["身份证", "ID", "证件"]
)

phone_pattern = Pattern(
    name="chinese_phone",
    pattern=r"1[3-9]\d{2}[-—\s]?\d{4}[-—\s]?\d{4}",
    score=0.85
)
phone_detector = PatternRecognizer(
    patterns=[phone_pattern],
    context=["手机", "电话", "号码"]
)

analyzer.registry.add_recognizer(id_detector)
analyzer.registry.add_recognizer(phone_detector)

# ---------------------
# 结构化数据检测函数
# ---------------------

def detect_sensitive_data(data, language="zh", fields=None):
    anonymizer = AnonymizerEngine()
    if isinstance(data, pd.DataFrame):
        return _detect_dataframe(data, language, fields, analyzer, anonymizer)
    elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
        return [_detect_dict(item, language, fields, analyzer, anonymizer) for item in data]
    elif isinstance(data, dict):
        return _detect_dict(data, language, fields, analyzer, anonymizer)
    else:
        raise ValueError("不支持的数据类型，支持DataFrame、列表[字典]或字典")

def _detect_dict(data_dict, language, fields, analyzer, anonymizer):
    result = {}
    for key, value in data_dict.items():
        if fields and key not in fields:
            result[key] = value
            continue
        if isinstance(value, str):
            results = analyzer.analyze(text=value, language=language)
            if results:
                anonymized_text = anonymizer.anonymize(
                    text=value,
                    analyzer_results=results,
                    anonymizers={"DEFAULT": OperatorConfig("replace", {"new_value": "***"})}
                ).text
                result[key] = anonymized_text
                result[f"{key}_sensitive_info"] = [
                    {"entity_type": res.entity_type, "start": res.start, "end": res.end}
                    for res in results
                ]
            else:
                result[key] = value
        else:
            result[key] = value
    return result

def _detect_dataframe(df, language, fields, analyzer, anonymizer):
    result_df = df.copy()
    fields_to_check = fields if fields else df.columns
    for field in fields_to_check:
        if field not in df.columns:
            continue
        sensitive_info_list = []
        anonymized_values = []
        for value in df[field]:
            if not isinstance(value, str):
                anonymized_values.append(value)
                sensitive_info_list.append(None)
                continue
            results = analyzer.analyze(text=value, language=language)
            if results:
                anonymized_text = anonymizer.anonymize(
                    text=value,
                    analyzer_results=results,
                    anonymizers={"DEFAULT": OperatorConfig("replace", {"new_value": "***"})}
                ).text
                anonymized_values.append(anonymized_text)
                sensitive_info = [
                    {"entity_type": res.entity_type, "start": res.start, "end": res.end}
                    for res in results
                ]
                sensitive_info_list.append(sensitive_info)
            else:
                anonymized_values.append(value)
                sensitive_info_list.append(None)
        result_df[field] = anonymized_values
        result_df[f"{field}_sensitive_info"] = sensitive_info_list
    return result_df

# ---------------------
# 使用示例
# ---------------------

if __name__ == "__main__":
    print("示例1: 检测单个字典")
    person_info = {
        "姓名": "张三",
        "身份证号": "110101199001011234",
        "手机号": "138-1234-5678",
        "邮箱": "zhangsan@example.com",
        "地址": "北京市朝阳区XX路123号",
        "备注": "这是一段普通文本"
    }
    detected_info = detect_sensitive_data(person_info, language="zh")
    for key, value in detected_info.items():
        print(f"{key}: {value}")
    
    print("\n" + "="*50 + "\n")
    
    print("示例2: 检测列表[字典]")
    users = [
        {
            "id": 1,
            "name": "张三",
            "phone": "13912345678",
            "id_card": "110101199001011234",
            "email": "zhangsan@example.com"
        },
        {
            "id": 2,
            "name": "李四",
            "phone": "138-1234-5678",
            "id_card": "310101199505054321",
            "email": "lisi@example.com"
        }
    ]
    detected_users = detect_sensitive_data(users, language="zh")
    for user in detected_users:
        print(user)
    
    print("\n" + "="*50 + "\n")
    
    print("示例3: 检测DataFrame")
    df = pd.DataFrame({
        "姓名": ["张三", "李四", "王五"],
        "身份证号": ["110101199001011234", "310101199505054321", "440101200010105678"],
        "手机号": ["138-1234-5678", "13912345678", "137 1234 5678"],
        "邮箱": ["zhangsan@example.com", "lisi@example.com", "wangwu@example.com"],
        "职位": ["工程师", "产品经理", "设计师"]
    })
    detected_df = detect_sensitive_data(df, language="zh")
    print(detected_df.to_csv(sep="\t", na_rep="nan"))
