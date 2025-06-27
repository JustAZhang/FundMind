import sqlite3
import pandas as pd
from pathlib import Path
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def print_table_info(conn, table_name):
    """打印表的结构和数据"""
    try:
        # 获取表结构
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        logger.info(f"\n表 {table_name} 的结构:")
        for col in columns:
            logger.info(f"列名: {col[1]}, 类型: {col[2]}")
        
        # 获取数据条数
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logger.info(f"总记录数: {count}")
        
        # 获取最新的5条数据
        df = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY rowid DESC LIMIT 5", conn)
        if not df.empty:
            logger.info("\n最新的5条数据:")
            logger.info(df)
        else:
            logger.info("表中没有数据")
            
    except Exception as e:
        logger.error(f"读取表 {table_name} 时发生错误: {str(e)}")

def analyze_stock_database(symbol):
    """分析指定股票的数据库内容"""
    try:
        db_path = Path(f"data/db/stock_{symbol}.db")
        if not db_path.exists():
            logger.error(f"数据库文件不存在: {db_path}")
            return
        
        logger.info(f"\n分析股票 {symbol} 的数据库...")
        conn = sqlite3.connect(db_path)
        
        # 获取所有表名
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        if not tables:
            logger.info("数据库中没有表")
            return
        
        # 遍历每个表
        for table in tables:
            table_name = table[0]
            logger.info("\n" + "="*50)
            logger.info(f"分析表: {table_name}")
            print_table_info(conn, table_name)
        
        conn.close()
        
    except Exception as e:
        logger.error(f"分析数据库时发生错误: {str(e)}")

def main():
    """主函数"""
    # 测试股票代码
    symbol = "600519"  # 贵州茅台
    
    try:
        # 分析数据库
        analyze_stock_database(symbol)
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    main() 