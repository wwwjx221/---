import pymysql
from pymysql.cursors import DictCursor

# 数据库配置 - 请根据实际情况修改
DB_CONFIG = {
    'host': '10.21.147.42',
    'user': 'root',
    'password': '1025',
    'database': 'vehicle_security',
    'port': 3306,
    'charset': 'utf8mb4'
}

# 国家名称与中文编码的对应关系
COUNTRY_MAPPING = {
    "Afghanistan": "阿富汗",
    "Albania": "阿尔巴尼亚",
    "Algeria": "阿尔及利亚",
    "Andorra": "安道尔",
    "Angola": "安哥拉",
    "Antigua and Barbuda": "安提瓜和巴布达",
    "Argentina": "阿根廷",
    "Armenia": "亚美尼亚",
    "Australia": "澳大利亚",
    "Austria": "奥地利",
    "Azerbaijan": "阿塞拜疆",
    "Bahamas": "巴哈马",
    "Bahrain": "巴林",
    "Bangladesh": "孟加拉国",
    "Barbados": "巴巴多斯",
    "Belarus": "白俄罗斯",
    "Belgium": "比利时",
    "Belize": "伯利兹",
    "Benin": "贝宁",
    "Bhutan": "不丹",
    "Bolivia": "玻利维亚",
    "Bosnia and Herzegovina": "波斯尼亚和黑塞哥维那",
    "Botswana": "博茨瓦纳",
    "Brazil": "巴西",
    "Brunei": "文莱",
    "Bulgaria": "保加利亚",
    "Burkina Faso": "布基纳法索",
    "Burundi": "布隆迪",
    "Cabo Verde": "佛得角",
    "Cambodia": "柬埔寨",
    "Cameroon": "喀麦隆",
    "Canada": "加拿大",
    "Central African Republic": "中非共和国",
    "Chad": "乍得",
    "Chile": "智利",
    "China": "中国",
    "Colombia": "哥伦比亚",
    "Comoros": "科摩罗",
    "Congo": "刚果（布）",
    "Costa Rica": "哥斯达黎加",
    "Croatia": "克罗地亚",
    "Cuba": "古巴",
    "Cyprus": "塞浦路斯",
    "Czech Republic": "捷克共和国",
    "Denmark": "丹麦",
    "Djibouti": "吉布提",
    "Dominica": "多米尼克",
    "Dominican Republic": "多米尼加共和国",
    "Ecuador": "厄瓜多尔",
    "Egypt": "埃及",
    "El Salvador": "萨尔瓦多",
    "Equatorial Guinea": "赤道几内亚",
    "Eritrea": "厄立特里亚",
    "Estonia": "爱沙尼亚",
    "Eswatini": "斯威士兰",
    "Ethiopia": "埃塞俄比亚",
    "Fiji": "斐济",
    "Finland": "芬兰",
    "France": "法国",
    "Gabon": "加蓬",
    "Gambia": "冈比亚",
    "Georgia": "格鲁吉亚",
    "Germany": "德国",
    "Ghana": "加纳",
    "Greece": "希腊",
    "Grenada": "格林纳达",
    "Guatemala": "危地马拉",
    "Guinea": "几内亚",
    "Guinea-Bissau": "几内亚比绍",
    "Guyana": "圭亚那",
    "Haiti": "海地",
    "Honduras": "洪都拉斯",
    "Hungary": "匈牙利",
    "Iceland": "冰岛",
    "India": "印度",
    "Indonesia": "印度尼西亚",
    "Iran": "伊朗",
    "Iraq": "伊拉克",
    "Ireland": "爱尔兰",
    "Israel": "以色列",
    "Italy": "意大利",
    "Jamaica": "牙买加",
    "Japan": "日本",
    "Jordan": "约旦",
    "Kazakhstan": "哈萨克斯坦",
    "Kenya": "肯尼亚",
    "Kiribati": "基里巴斯",
    "Kuwait": "科威特",
    "Kyrgyzstan": "吉尔吉斯斯坦",
    "Laos": "老挝",
    "Latvia": "拉脱维亚",
    "Lebanon": "黎巴嫩",
    "Lesotho": "莱索托",
    "Liberia": "利比里亚",
    "Libya": "利比亚",
    "Liechtenstein": "列支敦士登",
    "Lithuania": "立陶宛",
    "Luxembourg": "卢森堡",
    "Madagascar": "马达加斯加",
    "Malawi": "马拉维",
    "Malaysia": "马来西亚",
    "Maldives": "马尔代夫",
    "Mali": "马里",
    "Malta": "马耳他",
    "Marshall Islands": "马绍尔群岛",
    "Mauritania": "毛里塔尼亚",
    "Mauritius": "毛里求斯",
    "Mexico": "墨西哥",
    "Micronesia": "密克罗尼西亚",
    "Moldova": "摩尔多瓦",
    "Monaco": "摩纳哥",
    "Mongolia": "蒙古",
    "Montenegro": "黑山",
    "Morocco": "摩洛哥",
    "Mozambique": "莫桑比克",
    "Myanmar": "缅甸",
    "Namibia": "纳米比亚",
    "Nauru": "瑙鲁",
    "Nepal": "尼泊尔",
    "Netherlands": "荷兰",
    "New Zealand": "新西兰",
    "Nicaragua": "尼加拉瓜",
    "Niger": "尼日尔",
    "Nigeria": "尼日利亚",
    "North Korea": "朝鲜",
    "North Macedonia": "北马其顿",
    "Norway": "挪威",
    "Oman": "阿曼",
    "Pakistan": "巴基斯坦",
    "Palau": "帕劳",
    "Palestine": "巴勒斯坦",
    "Panama": "巴拿马",
    "Papua New Guinea": "巴布亚新几内亚",
    "Paraguay": "巴拉圭",
    "Peru": "秘鲁",
    "Philippines": "菲律宾",
    "Poland": "波兰",
    "Portugal": "葡萄牙",
    "Qatar": "卡塔尔",
    "Romania": "罗马尼亚",
    "Russia": "俄罗斯",
    "Rwanda": "卢旺达",
    "Saint Kitts and Nevis": "圣基茨和尼维斯",
    "Saint Lucia": "圣卢西亚",
    "Saint Vincent and the Grenadines": "圣文森特和格林纳丁斯",
    "Samoa": "萨摩亚",
    "San Marino": "圣马力诺",
    "Sao Tome and Principe": "圣多美和普林西比",
    "Saudi Arabia": "沙特阿拉伯",
    "Senegal": "塞内加尔",
    "Serbia": "塞尔维亚",
    "Seychelles": "塞舌尔",
    "Sierra Leone": "塞拉利昂",
    "Singapore": "新加坡",
    "Slovakia": "斯洛伐克",
    "Slovenia": "斯洛文尼亚",
    "Solomon Islands": "所罗门群岛",
    "Somalia": "索马里",
    "South Africa": "南非",
    "South Korea": "韩国",
    "South Sudan": "南苏丹",
    "Spain": "西班牙",
    "Sri Lanka": "斯里兰卡",
    "Sudan": "苏丹",
    "Suriname": "苏里南",
    "Sweden": "瑞典",
    "Switzerland": "瑞士",
    "Syria": "叙利亚",
    "Taiwan": "中国台湾",
    "Tajikistan": "塔吉克斯坦",
    "Tanzania": "坦桑尼亚",
    "Thailand": "泰国",
    "Timor-Leste": "东帝汶",
    "Togo": "多哥",
    "Tonga": "汤加",
    "Trinidad and Tobago": "特立尼达和多巴哥",
    "Tunisia": "突尼斯",
    "Turkey": "土耳其",
    "Turkmenistan": "土库曼斯坦",
    "Tuvalu": "图瓦卢",
    "Uganda": "乌干达",
    "Ukraine": "乌克兰",
    "United Arab Emirates": "阿联酋",
    "United Kingdom": "英国",
    "United States": "美国",
    "Uruguay": "乌拉圭",
    "Uzbekistan": "乌兹别克斯坦",
    "Vanuatu": "瓦努阿图",
    "Vatican City": "梵蒂冈",
    "Venezuela": "委内瑞拉",
    "Vietnam": "越南",
    "Yemen": "也门",
    "Zambia": "赞比亚",
    "Zimbabwe": "津巴布韦",
    "Hong Kong": "中国香港",
    "Macau": "中国澳门"
}

def update_country_codes(table_name='ip_country_mapping'):
    """
    读取国家名称，翻译成中文，并更新国家编码列
    """
    connection = None
    try:
        # 建立数据库连接
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor(DictCursor)
        
        # 读取所有记录
        print(f"从表 {table_name} 中读取数据...")
        cursor.execute(f"SELECT id, country_name, country_code FROM {table_name}")
        records = cursor.fetchall()
        print(f"共读取到 {len(records)} 条记录")
        
        # 处理每条记录
        updated_count = 0
        for record in records:
            record_id = record['id']
            country_name = record['country_name']
            current_code = record['country_code']
            
            # 查找对应的中文编码
            if country_name in COUNTRY_MAPPING:
                chinese_code = COUNTRY_MAPPING[country_name]
                
                # 如果需要更新
                if chinese_code != current_code:
                    # 更新记录
                    update_sql = f"""
                    UPDATE {table_name} 
                    SET country_code = %s 
                    WHERE id = %s
                    """
                    cursor.execute(update_sql, (chinese_code, record_id))
                    updated_count += 1
                    print(f"更新记录 {record_id}: {country_name} -> {chinese_code}")
        
        # 提交事务
        connection.commit()
        print(f"更新完成，共更新 {updated_count} 条记录")
        
    except Exception as e:
        print(f"发生错误: {str(e)}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    # 请替换为实际的表名
    target_table = "ip_country_mapping"
    update_country_codes(target_table)
