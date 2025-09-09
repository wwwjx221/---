from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer
import datetime
import re
import spacy
from presidio_analyzer.nlp_engine import SpacyNlpEngine
class ChineseIDRecognizer(PatternRecognizer):
    def __init__(self):
        id_pattern = Pattern(
            regex=r"\d{17}[\dXx]",
            score=0.8,
            name="chinese_id_card"
        )
        super().__init__(
            supported_entity="CHINESE_ID_CARD",
            patterns=[id_pattern],
            context=["身份证", "证件", "ID"]
        )
    
    def validate_result(self, pattern_text: str, pattern: Pattern = None, match_start: int = 0) -> float:
        """验证身份证号码的有效性"""
        if len(pattern_text) != 18:
            return False
            
        # 验证出生日期
        birth_str = pattern_text[6:14]
        print(birth_str)
        try:
            birth_date = datetime.datetime.strptime(birth_str, "%Y%m%d")
            
            # 检查日期是否在合理范围内（1900年至今）
            current_date = datetime.date.today()
            if birth_date.year < 1900 or birth_date.date() > current_date:
                print("日期不正确")
                return False  # 日期超出合理范围
            return True
            
        except ValueError:
            print("日期错误")
            return False  # 无效日期格式


# -----------------------
# 初始化 AnalyzerEngine
# -----------------------
try:
    nlp_zh = spacy.load("zh_core_web_md")
    nlp_en = spacy.load("en_core_web_lg")
except OSError:
    print("语言模型未找到，请先安装：")
    print("python -m spacy download zh_core_web_sm")
    print("python -m spacy download en_core_web_lg")
    exit(1)
nlp_engine = SpacyNlpEngine(models={"zh": nlp_zh, "en": nlp_en})
analyzer = AnalyzerEngine(nlp_engine=nlp_engine)

# -----------------------
# 添加自定义身份证识别器
# -----------------------
id_pattern = Pattern(
    name="chinese_id_card",
    regex=r"\d{17}[\dXx]|\d{15}",  # 匹配18位或15位身份证
    score=0.9
)

id_recognizer = PatternRecognizer(
    supported_entity="CHINESE_ID_CARD",
    patterns=[id_pattern],
    context=["身份证", "证件", "ID"]
)

analyzer.registry.add_recognizer(ChineseIDRecognizer())

# -----------------------
# 要分析的文本
# -----------------------
text = """
My name is John Smith, phone number is 415-555-1234, email is john.smith@example.com,
ID is 110101199001011234, 这是我的身份证号 310101199505054321，
银行卡号 6222020200066677777。我叫王嘉旭，我喜欢和马超一起玩
"""

# -----------------------
# 进行分析
# -----------------------
results = analyzer.analyze(text=text, language="en")

# -----------------------
# 输出结果
# -----------------------
for res in results:
    print(f"Detected entity: {res.entity_type}")
    print(f"  Position: [{res.start}, {res.end}]")
    print(f"  Confidence: {res.score:.2f}")
    print(f"  Matched text: '{text[res.start:res.end]}'\n")
