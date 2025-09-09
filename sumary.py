import mysql.connector
import ipaddress
from datetime import datetime

# 连接到MySQL数据库
conn = mysql.connector.connect(
    host="10.21.147.42",
    user="root",
    password="1025",
    database="vehicle_security"
)

cursor = conn.cursor()

# 1. 查询 flow 表的数据
cursor.execute("SELECT uid, id_orig_h, id_resp_h, id_orig_p, id_resp_p, proto, pcap_location FROM flow")
flow_data = cursor.fetchall()

# 2. 查询 ip_country_mapping 表的数据
cursor.execute("SELECT network, country_name FROM ip_country_mapping")
ip_country_mapping = cursor.fetchall()

# 将网段和国家存入字典，方便后续判断
country_mapping = {}
for network, country_name in ip_country_mapping:
    country_mapping[ipaddress.ip_network(network)] = country_name

# 统计信息
total_flows = 0
inflow = 0
outflow = 0
ip_set = set()
countries_set = set()
foreign_flows = 0

# 计算流量的统计信息
for flow in flow_data:
    flow_id, id_orig_h, id_resp_h, id_orig_p, id_resp_p, proto, pcap_location = flow
    origin_country = None
    response_country = None

    # 统计IP数量
    ip_set.add(id_orig_h)
    ip_set.add(id_resp_h)

    # 判断源IP属于哪个国家
    if id_orig_h.startswith('192.168'):  # 判断是否为本机IP
        origin_country = 'Local'
    else:
        for network, country_name in country_mapping.items():
            if ipaddress.ip_address(id_orig_h) in network:
                origin_country = country_name
                break
        if origin_country is None:  # 未匹配上的归为中国
            origin_country = 'China'
    
    # 判断目的IP属于哪个国家
    if id_resp_h.startswith('192.168'):  # 判断是否为本机IP
        response_country = 'Local'
    else:
        for network, country_name in country_mapping.items():
            if ipaddress.ip_address(id_resp_h) in network:
                response_country = country_name
                break
        if response_country is None:  # 未匹配上的归为中国
            response_country = 'China'

    # 统计流入和流出
    if origin_country == 'Local':
        inflow += 1
    if response_country == 'Local':
        outflow += 1
    
    # 统计涉及的国家
    countries_set.add(origin_country)
    countries_set.add(response_country)

    # 跨境流量判断：只要源IP或目的IP不属于中国且不是本机IP
    if (origin_country != 'China' and origin_country != 'Local') or (response_country != 'China' and response_country != 'Local'):
        foreign_flows += 1
    
    total_flows += 1

# 计算跨境率
cross_rate = (foreign_flows / total_flows * 100) if total_flows > 0 else 0

# 统计数据存入 domain_risk_data
ip_count = len(ip_set)
countries_str = ', '.join(countries_set)
last_active = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
risk_summary = f"Total Flows: {total_flows}, Inflow: {inflow}, Outflow: {outflow}, Foreign Flows: {foreign_flows}"

print(risk_summary)

# 插入数据到 domain_risk_data
# cursor.execute("""
#     INSERT INTO domain_risk_data (total, inflow, outflow, ip_count, countries, cross_rate, last_active, risk_summary)
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
# """, (total_flows, inflow, outflow, ip_count, countries_str, cross_rate, last_active, risk_summary))

# 提交更改
# conn.commit()

# 关闭数据库连接
cursor.close()
conn.close()

print(f"Data inserted successfully into domain_risk_data table!")
