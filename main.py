import os
import logging
from db_utils import create_connection, create_table_if_not_exists, insert_data_batch
from excel_parser import parse_excel_file

# 配置日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # 输出到控制台
        # 如果需要，可以添加 logging.FileHandler("app.log") 来输出到文件
    ]
)
logger = logging.getLogger(__name__)  # 获取当前模块的logger实例

# 定义一个常量或变量来存储表名，方便全局修改和引用
TABLE_FOR_RECRUITMENT_DATA = "recruitment_data"  # 与 db_utils.py 中使用的表名保持一致


def process_excel_files_in_directory(directory_path, db_conn):
    """处理指定目录下的所有Excel文件"""
    processed_files_count = 0
    total_records_imported = 0
    found_excel_files = False

    logger.info(f"开始扫描目录 '{directory_path}' 中的Excel文件...")

    for filename in os.listdir(directory_path):
        if filename.endswith((".xlsx", ".xls")):
            found_excel_files = True
            file_path = os.path.join(directory_path, filename)
            logger.info(f"开始处理文件: {file_path}")

            # 1. 解析Excel文件
            data_from_excel = parse_excel_file(file_path)

            if not data_from_excel:
                logger.warning(f"文件 '{filename}' 没有解析到有效数据或解析失败，跳过。")
                continue

            # 2. 插入数据到数据库
            # 使用我们定义的表名常量
            rows_affected = insert_data_batch(db_conn, data_from_excel, table_name=TABLE_FOR_RECRUITMENT_DATA)

            if rows_affected > 0:
                total_records_imported += rows_affected
                logger.info(f"成功处理文件: '{filename}', 导入/更新 {rows_affected} 条记录。")
            elif rows_affected == 0:
                logger.info(f"文件 '{filename}' 中的数据可能已存在或没有新数据可更新，未进行新的插入/更新操作。")
            else:  # rows_affected == -1 表示数据库操作失败
                logger.error(f"处理文件 '{filename}' 时数据库插入/更新失败。")

            processed_files_count += 1

    if not found_excel_files:
        logger.warning(f"在目录 '{directory_path}' 中没有找到任何 .xlsx 或 .xls 文件。")

    logger.info(
        f"目录处理完成。共处理 {processed_files_count} 个Excel文件，总共导入/更新 {total_records_imported} 条记录。")


def main():
    logger.info("--- Excel数据导入MySQL程序启动 ---")

    db_connection = None  # 初始化db_connection
    try:
        # 0. 数据库连接
        db_connection = create_connection()
        if not db_connection:
            logger.error("无法连接到数据库，程序退出。")
            return  # 如果连接失败，直接退出

        # 1. 确保表存在 (使用我们定义的表名常量)
        create_table_if_not_exists(db_connection, table_name=TABLE_FOR_RECRUITMENT_DATA)

        # 2. 指定Excel文件所在的目录
        excel_directory = 'data'  # 相对于脚本的路径

        # 检查目录是否存在
        if not os.path.isdir(excel_directory):
            logger.error(f"指定的Excel目录 '{excel_directory}' 不存在或不是一个目录。请创建该目录并将Excel文件放入其中。")
            return  # 如果目录不存在，也退出

        # 3. 处理目录中的所有Excel文件
        process_excel_files_in_directory(excel_directory, db_connection)

    except Exception as e:
        logger.error(f"主程序发生未捕获的严重错误: {e}", exc_info=True)  # exc_info=True 会记录堆栈跟踪
    finally:
        if db_connection and db_connection.is_connected():
            db_connection.close()
            logger.info("数据库连接已关闭。")

    logger.info("--- 程序执行完毕 ---")


if __name__ == "__main__":
    main()