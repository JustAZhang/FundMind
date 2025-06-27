from pathlib import Path
from typing import Dict, Any, Generator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, select, desc
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager

from src.data.models import (
    Price, PriceResponse,
    FinancialMetrics, FinancialMetricsResponse,
    InsiderTrade, InsiderTradeResponse,
    CompanyNews, CompanyNewsResponse,
    CompanyFacts, CompanyFactsResponse
)
from src.fund_loader import (
    StockBasic, StockDaily, StockFinancialMetrics,
    StockInsiderTrades, StockNews, StockValuationInfo
)

@contextmanager
def get_db_session(symbol: str) -> Generator[Session, None, None]:
    """创建并返回指定股票数据库的 SQLAlchemy Session"""
    db_path = Path(f'data/db/stock_{symbol}.db')
    if not db_path.exists():
        raise FileNotFoundError(f"数据库文件不存在: {db_path}")
    
    engine = create_engine(f'sqlite:///{db_path}')
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

def get_price_response(symbol: str) -> PriceResponse:
    """从 stock_daily 表中读取数据，返回 PriceResponse"""
    with get_db_session(symbol) as session:
        # 查询所有价格数据并按日期排序
        stmt = select(StockDaily).where(StockDaily.symbol == symbol)\
            .order_by(StockDaily.trade_date.desc())
        result = session.execute(stmt).scalars().all()
        
        prices = [
            Price(
                open=row.open,
                close=row.close,
                high=row.high,
                low=row.low,
                volume=int(row.volume),
                time=row.trade_date.strftime("%Y-%m-%d")
            )
            for row in result
        ]
        
        return PriceResponse(ticker=symbol, prices=prices)

def get_financial_metrics_response(symbol: str) -> FinancialMetricsResponse:
    """读取 stock_financial_metrics 和 stock_valuation_info 表数据，返回 FinancialMetricsResponse"""
    with get_db_session(symbol) as session:
        # 获取财务指标数据
        metrics_stmt = select(StockFinancialMetrics)\
            .where(StockFinancialMetrics.symbol == symbol)\
            .order_by(StockFinancialMetrics.report_date.desc())
        metrics_result = session.execute(metrics_stmt).scalars().all()
        
        # 获取最新估值数据
        valuation_stmt = select(StockValuationInfo)\
            .where(StockValuationInfo.symbol == symbol)\
            .order_by(StockValuationInfo.update_date.desc())\
            .limit(1)
        valuation = session.execute(valuation_stmt).scalar()
        
        financial_metrics = []
        for row in metrics_result:
            metric = FinancialMetrics(
                ticker=symbol,
                report_period=row.report_date.strftime("%Y-%m-%d"),
                period="annual",  # 假设是年度数据
                currency="CNY",   # 假设是人民币
                
                # 从财务指标表获取的数据
                earnings_per_share=row.eps,
                return_on_equity=row.roe,
                net_margin=row.net_profit_ratio,
                gross_margin=row.gross_profit_rate,
                
                # 如果有估值数据，添加估值相关指标
                market_cap=valuation.market_cap if valuation else None,
                price_to_earnings_ratio=valuation.pe_ratio if valuation else None,
                price_to_book_ratio=valuation.pb_ratio if valuation else None,
                
                # 其他字段设为 None
                enterprise_value=None,
                price_to_sales_ratio=None,
                enterprise_value_to_ebitda_ratio=None,
                enterprise_value_to_revenue_ratio=None,
                free_cash_flow_yield=None,
                peg_ratio=None,
                operating_margin=None,
                return_on_assets=None,
                return_on_invested_capital=None,
                asset_turnover=None,
                inventory_turnover=None,
                receivables_turnover=None,
                days_sales_outstanding=None,
                operating_cycle=None,
                working_capital_turnover=None,
                current_ratio=None,
                quick_ratio=None,
                cash_ratio=None,
                operating_cash_flow_ratio=None,
                debt_to_equity=None,
                debt_to_assets=None,
                interest_coverage=None,
                revenue_growth=None,
                earnings_growth=None,
                book_value_growth=None,
                earnings_per_share_growth=None,
                free_cash_flow_growth=None,
                operating_income_growth=None,
                ebitda_growth=None,
                payout_ratio=None,
                book_value_per_share=None,
                free_cash_flow_per_share=None
            )
            financial_metrics.append(metric)
        
        return FinancialMetricsResponse(financial_metrics=financial_metrics)

def get_insider_trade_response(symbol: str) -> InsiderTradeResponse:
    """读取 stock_insider_trades 表，返回 InsiderTradeResponse"""
    with get_db_session(symbol) as session:
        stmt = select(StockInsiderTrades)\
            .where(StockInsiderTrades.symbol == symbol)\
            .order_by(StockInsiderTrades.change_date.desc())
        result = session.execute(stmt).scalars().all()
        
        insider_trades = [
            InsiderTrade(
                ticker=symbol,
                name=row.holder_name,
                transaction_date=row.change_date.strftime("%Y-%m-%d"),
                transaction_shares=row.shares_changed,
                transaction_price_per_share=row.price,
                filing_date=row.update_date.strftime("%Y-%m-%d"),
                # 其他字段设为 None
                issuer=None,
                title=None,
                is_board_director=None,
                transaction_value=row.shares_changed * row.price if row.shares_changed and row.price else None,
                shares_owned_before_transaction=None,
                shares_owned_after_transaction=None,
                security_title=None
            )
            for row in result
        ]
        
        return InsiderTradeResponse(insider_trades=insider_trades)

def get_company_news_response(symbol: str) -> CompanyNewsResponse:
    """读取 stock_news 表，返回 CompanyNewsResponse"""
    with get_db_session(symbol) as session:
        stmt = select(StockNews)\
            .where(StockNews.symbol == symbol)\
            .order_by(StockNews.date.desc())
        result = session.execute(stmt).scalars().all()
        
        news = [
            CompanyNews(
                ticker=symbol,
                title=row.title,
                date=row.date.strftime("%Y-%m-%d"),
                source=row.source,
                url=row.url,
                author="",  # 数据库中没有这个字段
                sentiment=None  # 数据库中没有这个字段
            )
            for row in result
        ]
        
        return CompanyNewsResponse(news=news)

def get_company_facts_response(symbol: str) -> CompanyFactsResponse:
    """读取 stock_basic 和 stock_valuation_info 的最新数据，返回 CompanyFactsResponse"""
    with get_db_session(symbol) as session:
        # 获取基本信息
        basic_stmt = select(StockBasic).where(StockBasic.symbol == symbol)
        basic = session.execute(basic_stmt).scalar()
        
        # 获取最新估值信息
        valuation_stmt = select(StockValuationInfo)\
            .where(StockValuationInfo.symbol == symbol)\
            .order_by(StockValuationInfo.update_date.desc())\
            .limit(1)
        valuation = session.execute(valuation_stmt).scalar()
        
        if not basic:
            raise ValueError(f"未找到股票 {symbol} 的基本信息")
        
        facts = CompanyFacts(
            ticker=symbol,
            name=basic.name,
            industry=basic.industry,
            sector=None,  # 数据库中没有这个字段
            category=None,
            exchange=basic.market,
            is_active=True,
            listing_date=basic.list_date,
            location=basic.area,
            market_cap=valuation.market_cap if valuation else None,
            number_of_employees=None,
            sec_filings_url=None,
            sic_code=None,
            sic_industry=None,
            sic_sector=None,
            website_url=None,
            weighted_average_shares=int(valuation.outstanding_shares) if valuation and valuation.outstanding_shares else None
        )
        
        return CompanyFactsResponse(company_facts=facts)

def load_all_data(symbol: str) -> Dict[str, Any]:
    """返回指定股票的所有数据"""
    try:
        return {
            "prices": get_price_response(symbol),
            "financial_metrics": get_financial_metrics_response(symbol),
            "insider_trades": get_insider_trade_response(symbol),
            "news": get_company_news_response(symbol),
            "company_facts": get_company_facts_response(symbol)
        }
    except Exception as e:
        raise Exception(f"加载股票 {symbol} 的数据时发生错误: {str(e)}") 