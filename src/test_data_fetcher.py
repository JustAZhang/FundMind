import logging
from datetime import datetime
from src.data.cache import get_cache
from src.data_fetcher import load_all_data
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_database_access():
    """测试数据库访问"""
    # 1. 测试数据库文件是否存在
    test_symbol = "601318"  # 浦发银行作为测试用例
    db_path = Path(f'data/db/stock_{test_symbol}.db')
    
    logger.info(f"检查数据库文件: {db_path}")
    if not db_path.exists():
        logger.error(f"数据库文件不存在: {db_path}")
        return
    
    # 2. 测试缓存接口
    logger.info("测试缓存接口...")
    cache = get_cache()
    
    # 2.1 测试价格数据
    logger.info("获取价格数据...")
    prices = cache.get_prices(test_symbol)
    if prices:
        logger.info(f"价格数据条数: {len(prices)}")
        logger.info(f"最新价格数据: {prices[0]}")
    else:
        logger.warning("未找到价格数据")
    
    # 2.2 测试财务指标数据
    logger.info("获取财务指标数据...")
    metrics = cache.get_financial_metrics(test_symbol)
    if metrics:
        logger.info(f"财务指标数据条数: {len(metrics)}")
        logger.info(f"最新财务指标: {metrics[0]}")
    else:
        logger.warning("未找到财务指标数据")
    
    # 2.3 测试内部交易数据
    logger.info("获取内部交易数据...")
    trades = cache.get_insider_trades(test_symbol)
    if trades:
        logger.info(f"内部交易数据条数: {len(trades)}")
        logger.info(f"最新内部交易: {trades[0]}")
    else:
        logger.warning("未找到内部交易数据")
    
    # 2.4 测试新闻数据
    logger.info("获取新闻数据...")
    news = cache.get_company_news(test_symbol)
    if news:
        logger.info(f"新闻数据条数: {len(news)}")
        logger.info(f"最新新闻: {news[0]}")
    else:
        logger.warning("未找到新闻数据")
    
    # 3. 测试统一数据加载接口
    logger.info("测试统一数据加载接口...")
    try:
        all_data = load_all_data(test_symbol)
        logger.info("成功加载所有数据")
        
        # 打印每种数据类型的基本信息
        for data_type, data in all_data.items():
            if hasattr(data, 'prices'):
                logger.info(f"{data_type}: {len(data.prices)} 条记录")
            elif hasattr(data, 'financial_metrics'):
                logger.info(f"{data_type}: {len(data.financial_metrics)} 条记录")
            elif hasattr(data, 'insider_trades'):
                logger.info(f"{data_type}: {len(data.insider_trades)} 条记录")
            elif hasattr(data, 'news'):
                logger.info(f"{data_type}: {len(data.news)} 条记录")
            elif hasattr(data, 'company_facts'):
                logger.info(f"{data_type}: 公司基本信息已加载")
    except Exception as e:
        logger.error(f"加载所有数据时出错: {str(e)}")

if __name__ == "__main__":
    logger.info("开始测试数据库访问...")
    test_database_access()
    logger.info("测试完成") 