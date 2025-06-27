from typing import List, Dict, Any, Optional
from src.data_fetcher import (
    get_price_response,
    get_financial_metrics_response,
    get_insider_trade_response,
    get_company_news_response,
    get_company_facts_response
)

class Cache:
    """本地数据缓存，从 SQLite 数据库获取数据"""
    
    def get_prices(self, ticker: str) -> List[Dict[str, Any]] | None:
        """从本地数据库获取价格数据"""
        try:
            response = get_price_response(ticker)
            return [price.model_dump() for price in response.prices]
        except FileNotFoundError:
            return None
        except Exception as e:
            print(f"获取价格数据时出错: {str(e)}")
            return None

    def get_financial_metrics(self, ticker: str) -> List[Dict[str, Any]] | None:
        """从本地数据库获取财务指标数据"""
        try:
            response = get_financial_metrics_response(ticker)
            return [metric.model_dump() for metric in response.financial_metrics]
        except FileNotFoundError:
            return None
        except Exception as e:
            print(f"获取财务指标数据时出错: {str(e)}")
            return None

    def get_insider_trades(self, ticker: str) -> List[Dict[str, Any]] | None:
        """从本地数据库获取内部交易数据"""
        try:
            response = get_insider_trade_response(ticker)
            return [trade.model_dump() for trade in response.insider_trades]
        except FileNotFoundError:
            return None
        except Exception as e:
            print(f"获取内部交易数据时出错: {str(e)}")
            return None

    def get_company_news(self, ticker: str) -> List[Dict[str, Any]] | None:
        """从本地数据库获取公司新闻数据"""
        try:
            response = get_company_news_response(ticker)
            return [news.model_dump() for news in response.news]
        except FileNotFoundError:
            return None
        except Exception as e:
            print(f"获取公司新闻数据时出错: {str(e)}")
            return None

    def set_prices(self, ticker: str, data: List[Dict[str, Any]]):
        """不再需要设置缓存，数据直接存储在数据库中"""
        pass

    def set_financial_metrics(self, ticker: str, data: List[Dict[str, Any]]):
        """不再需要设置缓存，数据直接存储在数据库中"""
        pass

    def set_insider_trades(self, ticker: str, data: List[Dict[str, Any]]):
        """不再需要设置缓存，数据直接存储在数据库中"""
        pass

    def set_company_news(self, ticker: str, data: List[Dict[str, Any]]):
        """不再需要设置缓存，数据直接存储在数据库中"""
        pass

# 全局缓存实例
_cache = Cache()

def get_cache() -> Cache:
    """获取全局缓存实例"""
    return _cache
