# excel_parser.py (完整替换或仔细修改 parse_excel_file 函数)
import pandas as pd
import logging
import re  # 导入正则表达式模块

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_excel_file(file_path, sheet_name=0):
    """
    解析Excel文件并返回数据列表。
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        # 将所有NaN值替换为None，这对于数据库操作更友好
        df = df.where(pd.notnull(df), None)

        # 1. 更新预期的列名 (与你Excel中的表头完全一致)
        expected_columns = [
            'ID', '项目名称', '角色名称', '演员姓名', '采集日期', '拍摄时长'
        ]

        # 检查Excel是否包含所有预期列名
        for col in expected_columns:
            if col not in df.columns:
                logging.error(f"Excel文件 '{file_path}' 缺少必需的列: '{col}'。请检查表头。")
                return []  # 如果缺少列，则不继续处理

        data_to_insert = []
        for index, row in df.iterrows():
            try:
                # 2. 数据提取和转换
                excel_id = str(row['ID']) if row['ID'] is not None else None
                project_name = str(row['项目名称']) if row['项目名称'] is not None else None
                character_name = str(row['角色名称']) if row['角色名称'] is not None else None
                actor_name = str(row['演员姓名']) if row['演员姓名'] is not None else None

                # 处理采集日期
                capture_date_str = None
                capture_date_val = row['采集日期']
                if capture_date_val is not None:
                    if isinstance(capture_date_val, pd.Timestamp):  # 如果pandas读成了Timestamp对象
                        capture_date_str = capture_date_val.strftime('%Y-%m-%d')
                    else:  # 否则尝试按字符串转换
                        try:
                            # 尝试将 "2025/6/12" 这种格式转为 "YYYY-MM-DD"
                            capture_date_str = pd.to_datetime(str(capture_date_val), format='%Y/%m/%d').strftime(
                                '%Y-%m-%d')
                        except ValueError:
                            try:
                                # 如果上面格式失败，尝试直接转换（可能已经是YYYY-MM-DD或其他可识别格式）
                                capture_date_str = pd.to_datetime(str(capture_date_val)).strftime('%Y-%m-%d')
                            except ValueError:
                                logging.warning(
                                    f"文件 '{file_path}', 行 {index + 2}: 日期 '{capture_date_val}' 格式无法解析，将设为None。")

                # 处理拍摄时长 (例如 "120min" -> 120)
                duration_minutes = None
                duration_str = row['拍摄时长']
                if duration_str is not None:
                    # 使用正则表达式提取数字部分
                    match = re.search(r'\d+', str(duration_str))
                    if match:
                        try:
                            duration_minutes = int(match.group(0))
                        except ValueError:
                            logging.warning(
                                f"文件 '{file_path}', 行 {index + 2}: 拍摄时长中的数字 '{match.group(0)}' 无法转为整数，将设为None。")
                    else:
                        logging.warning(
                            f"文件 '{file_path}', 行 {index + 2}: 拍摄时长 '{duration_str}' 中未找到数字，将设为None。")

                # 确保ID不为空
                if excel_id is None:
                    logging.warning(f"文件 '{file_path}', 行 {index + 2}: 'ID'列为空，跳过此记录: {row.to_dict()}")
                    continue

                # 3. 构建记录元组 (顺序必须与数据库表列顺序一致)
                record = (
                    excel_id,
                    project_name,
                    character_name,
                    actor_name,
                    capture_date_str,
                    duration_minutes
                )
                data_to_insert.append(record)

            except KeyError as ke:
                logging.error(
                    f"文件 '{file_path}', 行 {index + 2}: 解析时列名缺失 {ke}。检查Excel表头和代码中的expected_columns。")
            except Exception as e:
                logging.error(f"文件 '{file_path}', 行 {index + 2}: 解析时发生未知错误: {e}, 数据: {row.to_dict()}")

        logging.info(f"成功解析文件 '{file_path}'，共 {len(data_to_insert)} 条有效记录。")
        return data_to_insert

    except FileNotFoundError:
        logging.error(f"Excel文件未找到: {file_path}")
        return []
    except Exception as e:
        logging.error(f"打开或初步解析Excel文件 '{file_path}' 时出错: {e}")
        return []


if __name__ == '__main__':
    # 简单的测试 (可选)
    # 你可以创建一个临时的 test_excel.xlsx 文件来测试这个解析器
    # test_data = {
    #     'ID': ['测试01', '测试02'],
    #     '项目名称': ['项目A', '项目B'],
    #     '角色名称': ['角色X', '角色Y'],
    #     '演员姓名': ['演员1', '演员2'],
    #     '采集日期': ['2024/01/01', pd.Timestamp('2024-01-02')], # 测试不同日期格式
    #     '拍摄时长': ['60min', '90 min']
    # }
    # test_df = pd.DataFrame(test_data)
    # test_file_path = 'test_job_application.xlsx'
    # test_df.to_excel(test_file_path, index=False)
    #
    # parsed = parse_excel_file(test_file_path)
    # for p_row in parsed:
    #     print(p_row)
    #
    # import os
    # os.remove(test_file_path)
    pass