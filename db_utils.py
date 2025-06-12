# db_utils.py (完整替换或仔细修改 create_table_if_not_exists 和 insert_data_batch 函数)
import mysql.connector
from mysql.connector import Error
import configparser
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_db_config(filename='config.ini', section='mysql'):
    parser = configparser.ConfigParser()
    parser.read(filename, encoding='utf-8') # 确保使用UTF-8读取
    db_config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return db_config

def create_connection():
    conn = None
    try:
        db_config = load_db_config()
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            logging.info("成功连接到MySQL数据库")
            return conn
    except Error as e:
        logging.error(f"连接MySQL时出错: {e}")
        return None

# 新的表名和结构
def create_table_if_not_exists(conn, table_name="recruitment_data"): # 可以自定义表名
    cursor = conn.cursor()
    # 数据库列名推荐使用英文蛇形命名法
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        internal_id INT AUTO_INCREMENT PRIMARY KEY, -- 数据库内部自增ID
        excel_id VARCHAR(100) UNIQUE NOT NULL,    -- 对应Excel的'ID'列, 设为UNIQUE
        project_name VARCHAR(255),               -- 对应Excel的'项目名称'
        character_name VARCHAR(255),             -- 对应Excel的'角色名称'
        actor_name VARCHAR(255),                 -- 对应Excel的'演员姓名'
        capture_date DATE,                       -- 对应Excel的'采集日期'
        duration_minutes INT,                    -- 对应Excel的'拍摄时长' (单位：分钟)
        imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        cursor.execute(create_table_query)
        conn.commit()
        logging.info(f"表 '{table_name}' 已检查/创建。")
    except Error as e:
        logging.error(f"创建表 '{table_name}' 时出错: {e}")
    finally:
        cursor.close()

def insert_data_batch(conn, data, table_name="recruitment_data"): # 对应表名
    if not data:
        logging.info("没有数据需要插入。")
        return 0

    cursor = conn.cursor()
    # 列的顺序必须和 excel_parser.py 中 record 元组的顺序完全一致
    # 数据库列名使用英文
    sql = f"""
    INSERT INTO {table_name} (
        excel_id, project_name, character_name, actor_name, 
        capture_date, duration_minutes
    )
    VALUES (%s, %s, %s, %s, %s, %s)  -- 6个占位符
    ON DUPLICATE KEY UPDATE          -- 如果 excel_id 已存在则更新
        project_name = VALUES(project_name),
        character_name = VALUES(character_name),
        actor_name = VALUES(actor_name),
        capture_date = VALUES(capture_date),
        duration_minutes = VALUES(duration_minutes),
        imported_at = CURRENT_TIMESTAMP;
    """
    try:
        cursor.executemany(sql, data)
        conn.commit()
        logging.info(f"成功插入/更新 {cursor.rowcount} 条记录到 '{table_name}'。")
        return cursor.rowcount
    except Error as e:
        logging.error(f"批量插入数据到 '{table_name}' 时出错: {e}")
        conn.rollback() 
        return -1
    finally:
        cursor.close()

if __name__ == '__main__':
    # 测试连接 (可选)
    # connection = create_connection()
    # if connection:
    #     create_table_if_not_exists(connection) # 测试建表
    #     # test_insert_data = [('TestID001', 'TestProject', 'TestChar', 'TestActor', '2024-01-01', 60)]
    #     # insert_data_batch(connection, test_insert_data)
    #     connection.close()
    pass