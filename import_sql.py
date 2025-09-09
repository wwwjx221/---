import mysql.connector
from mysql.connector import Error
import pandas as pd
import os

# 数据库配置
DB_CONFIG = {
    'host': '10.21.147.42',
    'port': 3306,
    'user': 'root',  # 替换为实际用户名
    'password': '1025',  # 替换为实际密码
    'database': 'vehicle_security',  # 替换为实际数据库名
    'charset': 'utf8mb4'
}

# 配置参数
XLSX_FILE_PATH = 'all_data.xlsx'  # XLSX文件路径
SHEET_NAME = 'Sheet1'  # Excel工作表名称（默认Sheet1）
BATCH_SIZE = 1000  # 每批次插入的记录数，可根据实际情况调整

def create_db_connection():
    """创建数据库连接"""
    connection = None
    try:
        connection = mysql.connector.connect(** DB_CONFIG)
        print("数据库连接成功")
    except Error as e:
        print(f"数据库连接错误: {e}")
    return connection

def bulk_insert_data(connection):
    """批量插入数据"""
    if not os.path.exists(XLSX_FILE_PATH):
        print(f"错误: XLSX文件 {XLSX_FILE_PATH} 不存在")
        return

    try:
        # 读取Excel文件
        df = pd.read_excel(
            XLSX_FILE_PATH,
            sheet_name=SHEET_NAME,
            usecols=['Network', 'Country Code', 'Country Name'],  # 只读取需要的列
            dtype=str  # 全部按字符串读取，避免数字格式问题
        )
        
        # 数据清洗：去除空行和空格
        df = df.dropna()
        df['Network'] = df['Network'].str.strip()
        df['Country Code'] = df['Country Code'].str.strip()
        df['Country Name'] = df['Country Name'].str.strip()
        
        total_rows = len(df)
        print(f"成功读取 {total_rows} 条数据，开始导入...")

        cursor = connection.cursor()
        total_inserted = 0
        
        # 分批次插入
        for i in range(0, total_rows, BATCH_SIZE):
            batch_df = df[i:i+BATCH_SIZE]
            # 转换为元组列表，适配executemany
            batch_data = [
                (row['Network'], row['Country Code'], row['Country Name'])
                for _, row in batch_df.iterrows()
            ]
            
            # 执行批次插入
            insert_query = """
            INSERT INTO ip_country_mapping (network, country_code, country_name)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE  -- 遇到重复的network时不更新（需表中有uk_network唯一索引）
            network = network
            """
            cursor.executemany(insert_query, batch_data)
            
            # 累计计数并打印进度
            batch_count = len(batch_data)
            total_inserted += batch_count
            print(f"已导入 {total_inserted}/{total_rows} 条数据")
        
        connection.commit()
        print(f"数据导入完成，共成功导入 {total_inserted} 条记录")
        
    except Error as e:
        print(f"插入数据错误: {e}")
        connection.rollback()
    except Exception as e:
        print(f"处理Excel文件错误: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()

if __name__ == "__main__":
    db_connection = create_db_connection()
    if db_connection:
        bulk_insert_data(db_connection)
        db_connection.close()
        print("数据库连接已关闭")
