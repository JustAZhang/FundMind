import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, UniqueConstraint, Date
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta, date
import logging
import os
from pathlib import Path
import time
import math
from typing import List
import sys
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from contextlib import contextmanager

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 创建数据库目录
DB_DIR = Path("data/db")
DB_DIR.mkdir(parents=True, exist_ok=True)

# 数据库连接
DATABASE_URL = f"sqlite:///{DB_DIR}/stock_data.db"
engine = create_engine(DATABASE_URL)
Base = declarative_base()
Session = sessionmaker(bind=engine)
SessionLocal = sessionmaker(engine)

class StockBasic(Base):
    """股票基本信息表"""
    __tablename__ = 'stock_basic'
    
    symbol = Column(String, primary_key=True)
    name = Column(String)
    industry = Column(String)
    area = Column(String)
    market = Column(String)
    list_date = Column(String)
    last_updated = Column(DateTime, default=datetime.now)

class StockDaily(Base):
    """股票每日行情表"""
    __tablename__ = 'stock_daily'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    trade_date = Column(DateTime)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    amount = Column(Float)
    
    class Config:
        unique_together = ('symbol', 'trade_date')

class StockFinancialMetrics(Base):
    """股票财务指标表"""
    __tablename__ = 'stock_financial_metrics'
    
    symbol = Column(String, primary_key=True)
    report_date = Column(Date, primary_key=True)
    roe = Column(Float)
    net_profit_ratio = Column(Float)
    gross_profit_rate = Column(Float)
    net_profits = Column(Float)
    eps = Column(Float)
    main_business_income = Column(Float)
    update_date = Column(DateTime, default=datetime.now)

class StockInsiderTrades(Base):
    """股票内部交易信息表"""
    __tablename__ = 'stock_insider_trades'
    
    symbol = Column(String, primary_key=True)
    holder_name = Column(String, primary_key=True)
    change_date = Column(Date, primary_key=True)
    change_type = Column(String)
    shares_changed = Column(Float)
    price = Column(Float)
    update_date = Column(DateTime, default=datetime.now)

class StockNews(Base):
    """股票新闻表"""
    __tablename__ = 'stock_news'
    
    symbol = Column(String, primary_key=True)
    title = Column(String, primary_key=True)
    date = Column(DateTime, primary_key=True)
    source = Column(String)
    url = Column(String)
    update_date = Column(DateTime, default=datetime.now)

class StockValuationInfo(Base):
    """股票估值信息表"""
    __tablename__ = 'stock_valuation_info'
    
    symbol = Column(String, primary_key=True)
    market_cap = Column(Float)
    total_shares = Column(Float)
    outstanding_shares = Column(Float)
    pe_ratio = Column(Float)
    pb_ratio = Column(Float)
    update_date = Column(DateTime, default=datetime.now)

def format_symbol(symbol: str) -> str:
    """将股票代码转换为 akshare 标准格式（带市场后缀）"""
    if symbol.startswith("6"):
        return symbol + ".SH"
    elif symbol.startswith(("0", "3")):
        return symbol + ".SZ"
    return symbol

class StockDataLoader:
    def __init__(self, symbol: str):
        """初始化数据加载器，为每个股票创建独立的数据库"""
        self.symbol = symbol
        
        # 创建数据目录
        db_dir = Path('data/db')
        db_dir.mkdir(parents=True, exist_ok=True)
        
        # 使用股票代码创建独立的数据库
        db_path = db_dir / f'stock_{symbol}.db'
        self.engine = create_engine(f'sqlite:///{db_path}')
        
        # 创建表
        Base.metadata.create_all(self.engine)
        
        # 创建会话
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
    
    def update_financial_metrics(self):
        """更新单个股票的财务指标"""
        try:
            logger.info(f"正在获取股票 {self.symbol} 的财务指标...")
            df = ak.stock_financial_analysis_indicator(symbol=self.symbol)
            
            for _, row in df.iterrows():
                metrics = StockFinancialMetrics(
                    symbol=self.symbol,
                    report_date=pd.to_datetime(row['报告期']),
                    roe=float(row['净资产收益率']),
                    net_profit_ratio=float(row['净利率']),
                    gross_profit_rate=float(row['毛利率']),
                    net_profits=float(row['净利润']),
                    eps=float(row['每股收益']),
                    main_business_income=float(row['主营业务收入']),
                    update_date=datetime.now()
                )
                self.session.merge(metrics)
            
            self.session.commit()
            logger.info(f"股票 {self.symbol} 财务指标更新完成")
            
        except Exception as e:
            logger.error(f"更新财务指标时发生错误: {str(e)}")
            self.session.rollback()

    def update_insider_trades(self):
        """更新单个股票的内部交易信息"""
        try:
            logger.info(f"正在获取股票 {self.symbol} 的内部交易信息...")
            
            if self.symbol.startswith('6'):
                # 上交所股票
                df = ak.stock_share_hold_change_sse(symbol=self.symbol)
                if not df.empty:
                    df['exchange'] = 'SSE'
            elif self.symbol.startswith(('0', '3')):
                # 深交所股票
                df = ak.stock_share_hold_change_szse(symbol=self.symbol)
                if not df.empty:
                    df['exchange'] = 'SZSE'
            else:
                logger.warning(f"股票代码 {self.symbol} 不符合格式")
                return
            
            if df.empty:
                logger.info(f"股票 {self.symbol} 没有内部交易数据")
                return
            
            for _, row in df.iterrows():
                holder_name = row.get('董监高姓名') or row.get('股份变动人姓名', '未知')
                shares_val = row.get("变动股份数量",0)
                shares_changed = int(float(shares_val)) if pd.notna(shares_val) else 0
                raw_price = row.get("成交均价")
                price = float(raw_price) if pd.notna(raw_price) else 0.0

                trade = StockInsiderTrades(
                    symbol=self.symbol,
                    holder_name=holder_name,
                    change_date=pd.to_datetime(row.get('变动日期')),
                    change_type=row.get('变动原因', ''),
                    shares_changed=shares_changed,
                    price=price,
                    update_date=datetime.now()
                )
                self.session.merge(trade)
            
            self.session.commit()
            logger.info(f"股票 {self.symbol} 内部交易信息更新完成")
            
        except Exception as e:
            logger.error(f"更新内部交易信息时发生错误: {str(e)}")
            self.session.rollback()

    def update_stock_news(self):
        """更新单个股票的新闻"""
        try:
            logger.info(f"正在获取股票 {self.symbol} 的新闻信息...")
            df = ak.stock_news_em(symbol=self.symbol)
            
            if df.empty:
                logger.info(f"股票 {self.symbol} 没有新闻数据")
                return
            
            # 获取30天内的新闻
            thirty_days_ago = datetime.now() - timedelta(days=30)
            
            for _, row in df.iterrows():
                date_str = row.get('时间') or row.get('日期')
                if not date_str:
                    continue
                
                news_date = pd.to_datetime(date_str)
                if news_date < thirty_days_ago:
                    continue
                    
                news = StockNews(
                    symbol=self.symbol,
                    title=row.get('标题', ''),
                    date=news_date,
                    source='东方财富',
                    url=row.get('链接', ''),
                    update_date=datetime.now()
                )
                self.session.merge(news)
            
            self.session.commit()
            logger.info(f"股票 {self.symbol} 新闻信息更新完成")
            
        except Exception as e:
            logger.error(f"更新新闻信息时发生错误: {str(e)}")
            self.session.rollback()

    def update_valuation_info(self):
        """更新单个股票的估值信息"""
        try:
            logger.info(f"正在获取股票 {self.symbol} 的估值信息...")
            
            # 处理股票代码格式
            query_symbol = self.symbol
            if query_symbol.startswith(('sh', 'sz')):
                query_symbol = query_symbol[2:]
            
            df = ak.stock_individual_info_em(symbol=query_symbol)
            
            if df is None or df.empty:
                logger.warning(f"股票 {self.symbol} 没有估值信息")
                return
            
            # 打印原始数据，帮助调试
            logger.info(f"获取到的数据列：{df.columns.tolist()}")
            logger.info(f"原始数据：\n{df}")
            
            # 确保数据框格式正确
            if 'item' not in df.columns or 'value' not in df.columns:
                logger.error(f"数据格式不正确，当前列：{df.columns}")
                return
            
            # 创建字典存储需要的值
            info_dict = {}
            for _, row in df.iterrows():
                item = row['item']
                value = row['value']
                info_dict[item] = value
            
            # 处理各个字段，添加错误处理
            try:
                market_cap = float(info_dict.get("总市值", "0").replace(",", ""))
            except:
                market_cap = 0.0
                logger.warning("转换总市值失败")
            
            try:
                total_shares = float(info_dict.get("总股本", "0").replace(",", ""))
            except:
                total_shares = 0.0
                logger.warning("转换总股本失败")
            
            try:
                outstanding_shares = float(info_dict.get("流通股本", "0").replace(",", ""))
            except:
                outstanding_shares = 0.0
                logger.warning("转换流通股本失败")
            
            try:
                pe_ratio = float(info_dict.get("市盈率(动)", "0").replace(",", ""))
            except:
                pe_ratio = 0.0
                logger.warning("转换市盈率失败")
            
            try:
                pb_ratio = float(info_dict.get("市净率", "0").replace(",", ""))
            except:
                pb_ratio = 0.0
                logger.warning("转换市净率失败")
            
            # 创建估值信息对象
            valuation = StockValuationInfo(
                symbol=self.symbol,
                market_cap=market_cap,
                total_shares=total_shares,
                outstanding_shares=outstanding_shares,
                pe_ratio=pe_ratio,
                pb_ratio=pb_ratio,
                update_date=datetime.now()
            )
            
            self.session.merge(valuation)
            self.session.commit()
            logger.info(f"股票 {self.symbol} 估值信息更新完成")
            
        except Exception as e:
            logger.error(f"更新估值信息时发生错误: {str(e)}")
            logger.error("错误详情:", exc_info=True)
            self.session.rollback()

    def update_stock_basic(self,symbol):
        """更新股票基本信息"""
        try:
            logger.info(f"正在获取股票 {self.symbol} 的基本信息...")
            
            # 获取股票基本信息
            df = ak.stock_individual_info_em(symbol=symbol)
            if df.empty:
                logger.warning(f"股票 {self.symbol} 没有基本信息")
                return
            
            # 确保数据框包含所需的列
            if "item" not in df.columns or "value" not in df.columns:
                logger.error(f"股票信息数据格式不正确: {df.columns}")
                return
            
            info = df.set_index("item")["value"]
            
            # 获取行业信息
            try:
                industry_df = ak.stock_board_industry_name_em()
                stock_industry = industry_df[industry_df['代码'] == symbol]['板块名称'].values
                industry = stock_industry[0] if len(stock_industry) > 0 else "未知"
            except Exception as e:
                logger.warning(f"获取行业信息失败: {e}")
                industry = "未知"
            
            basic = StockBasic(
                symbol=self.symbol,
                name=info.get("股票简称", ""),
                industry=industry,
                area=info.get("地区", ""),
                market="上证" if self.symbol.startswith('6') else "深证",
                list_date=info.get("上市时间", ""),
                last_updated=datetime.now()
            )
            self.session.merge(basic)
            self.session.commit()
            logger.info(f"股票 {self.symbol} 基本信息更新完成")
            
        except Exception as e:
            logger.error(f"更新基本信息时发生错误: {str(e)}")
            self.session.rollback()

    def update_stock_price(self):
        """更新股票历史价格数据"""
        try:
            logger.info(f"获取股票 {self.symbol} 的历史K线数据...")
            df = ak.stock_zh_a_hist(symbol=self.symbol, period='daily', adjust='qfq')
            if df.empty:
                logger.warning(f"{self.symbol} 无历史价格数据")
                return
            
            for _, row in df.iterrows():
                price = StockDaily(
                    symbol=self.symbol,
                    trade_date=pd.to_datetime(row['日期']),
                    open=float(row['开盘']),
                    high=float(row['最高']),
                    low=float(row['最低']),
                    close=float(row['收盘']),
                    volume=float(row['成交量']),
                    amount=float(row['成交额'])
                )
                self.session.merge(price)
            self.session.commit()
            logger.info(f"{self.symbol} 的价格数据更新完成")
        except Exception as e:
            logger.error(f"价格更新失败: {e}")
            self.session.rollback()

    def update_all_data(self):
        """更新该股票的所有数据"""
        try:
            logger.info(f"开始更新股票 {self.symbol} 的所有数据...")
            
            # 1. 先更新基本信息
            self.update_stock_basic(self.symbol)
            time.sleep(1)
            
            # 2. 更新历史价格数据
            self.update_stock_price()  # 获取所有历史价格
            time.sleep(1)
            
            # 3. 更新当日价格数据
            self.update_daily_price()  # 获取最新价格
            time.sleep(1)
            
            # 4. 更新财务指标
            try:
                self.update_financial_metrics()
            except Exception as e:
                logger.error(f"更新财务指标失败: {e}")
            time.sleep(1)
            
            # 5. 更新内部交易（放宽时间限制）
            try:
                self.update_insider_trades()
            except Exception as e:
                logger.error(f"更新内部交易失败: {e}")
            time.sleep(1)
            
            # 6. 更新新闻（放宽时间限制到30天）
            try:
                self.update_stock_news()
            except Exception as e:
                logger.error(f"更新新闻失败: {e}")
            time.sleep(1)
            
            # 7. 更新估值信息
            try:
                self.update_valuation_info()
            except Exception as e:
                logger.error(f"更新估值信息失败: {e}")
            
            logger.info(f"股票 {self.symbol} 所有数据更新完成")
            
        except Exception as e:
            logger.error(f"更新所有数据时发生错误: {str(e)}")

    def update_stock_history(self, full_update: bool = False):
        """获取股票从上市至今的所有历史数据
        
        Args:
            full_update (bool): 是否强制更新全部历史数据，默认为 False
        """
        try:
            now = datetime.now()
            thirty_days_ago = pd.Timestamp.now() - pd.Timedelta(days=30)
            logger.info(f"开始获取股票 {self.symbol} 的历史数据... full_update={full_update}")
            
            # 1. 获取历史财务指标
            try:
                logger.info("1. 获取历史财务指标...")
                has_data = False
                if not full_update:
                    # 检查是否已有数据
                    existing_data = self.session.query(StockFinancialMetrics)\
                        .filter(StockFinancialMetrics.symbol == self.symbol)\
                        .order_by(StockFinancialMetrics.report_date.desc())\
                        .first()
                    has_data = existing_data is not None
                
                df = ak.stock_financial_analysis_indicator(symbol=self.symbol)
                if not df.empty:
                    data_updated = False
                    for _, row in df.iterrows():
                        report_date = pd.to_datetime(row['报告期'])
                        
                        # 如果已有数据且不是全量更新，则只处理最新的报告
                        if has_data and not full_update and report_date <= existing_data.report_date:
                            continue
                        
                        metrics = StockFinancialMetrics(
                            symbol=self.symbol,
                            report_date=report_date,
                            roe=float(row['净资产收益率']),
                            net_profit_ratio=float(row['净利率']),
                            gross_profit_rate=float(row['毛利率']),
                            net_profits=float(row['净利润']),
                            eps=float(row['每股收益']),
                            main_business_income=float(row['主营业务收入']),
                            update_date=now
                        )
                        self.session.merge(metrics)
                        data_updated = True
                    
                    if data_updated:
                        self.session.commit()
                        logger.info("历史财务指标更新完成")
                    else:
                        logger.info("历史财务指标无需更新")
            except Exception as e:
                logger.error(f"获取历史财务指标时发生错误: {str(e)}")
                self.session.rollback()
            
            time.sleep(1)
            
            # 2. 获取历史内部交易
            try:
                logger.info("2. 获取历史内部交易...")
                has_data = False
                if not full_update:
                    # 检查是否已有数据
                    existing_trade = self.session.query(StockInsiderTrades)\
                        .filter(StockInsiderTrades.symbol == self.symbol)\
                        .order_by(StockInsiderTrades.change_date.desc())\
                        .first()
                    has_data = existing_trade is not None
                
                df = None
                if self.symbol.startswith('6'):
                    df = ak.stock_share_hold_change_sse(symbol=self.symbol)
                    if not df.empty:
                        df['exchange'] = 'SSE'
                elif self.symbol.startswith(('0', '3')):
                    df = ak.stock_share_hold_change_szse(symbol=self.symbol)
                    if not df.empty:
                        df['exchange'] = 'SZSE'
                
                if df is not None and not df.empty:
                    data_updated = False
                    for _, row in df.iterrows():
                        change_date = pd.to_datetime(row.get('变动日期'))
                        
                        # 如果已有数据且不是全量更新，则只处理新的交易记录
                        if has_data and not full_update and change_date <= existing_trade.change_date:
                            continue
                            
                        holder_name = row.get('董监高姓名') or row.get('股份变动人姓名', '未知')
                        shares_val = row.get("变动股份数量",0)
                        shares_changed = int(float(shares_val)) if pd.notna(shares_val) else 0
                        raw_price = row.get("成交均价")
                        price = float(raw_price) if pd.notna(raw_price) else 0.0

                        trade = StockInsiderTrades(
                            symbol=self.symbol,
                            holder_name=holder_name,
                            change_date=change_date,
                            change_type=row.get('变动原因', ''),
                            shares_changed=shares_changed,
                            price=price,
                            update_date=now
                        )
                        self.session.merge(trade)
                        data_updated = True
                    
                    if data_updated:
                        self.session.commit()
                        logger.info("历史内部交易更新完成")
                    else:
                        logger.info("历史内部交易无需更新")
            except Exception as e:
                logger.error(f"获取历史内部交易时发生错误: {str(e)}")
                self.session.rollback()
            
            time.sleep(1)
            
            # 3. 获取新闻（仅保留近30天）
            try:
                logger.info("3. 获取新闻数据...")
                has_data = False
                if not full_update:
                    # 检查是否已有最近的新闻数据
                    existing_news = self.session.query(StockNews)\
                        .filter(StockNews.symbol == self.symbol)\
                        .filter(StockNews.date >= thirty_days_ago)\
                        .order_by(StockNews.date.desc())\
                        .first()
                    has_data = existing_news is not None
                
                df = ak.stock_news_em(symbol=self.symbol)
                if not df.empty:
                    data_updated = False
                    for _, row in df.iterrows():
                        date_str = row.get('时间') or row.get('日期')
                        if not date_str:
                            continue
                        
                        news_date = pd.to_datetime(date_str)
                        # 只保留近30天的新闻
                        if news_date < thirty_days_ago:
                            continue
                        
                        # 如果已有数据且不是全量更新，则只处理新的新闻
                        if has_data and not full_update and news_date <= existing_news.date:
                            continue
                        
                        news = StockNews(
                            symbol=self.symbol,
                            title=row.get('标题', ''),
                            date=news_date,
                            source='东方财富',
                            url=row.get('链接', ''),
                            update_date=now
                        )
                        self.session.merge(news)
                        data_updated = True
                    
                    if data_updated:
                        self.session.commit()
                        logger.info("新闻数据更新完成")
                    else:
                        logger.info("新闻数据无需更新")
                        
                    # 删除30天前的新闻
                    self.session.query(StockNews)\
                        .filter(StockNews.symbol == self.symbol)\
                        .filter(StockNews.date < thirty_days_ago)\
                        .delete()
                    self.session.commit()
            except Exception as e:
                logger.error(f"获取新闻数据时发生错误: {str(e)}")
                self.session.rollback()
            
            time.sleep(1)
            
            # 4. 获取估值信息（仅保留近30天）
            try:
                logger.info("4. 获取估值信息...")
                has_data = False
                if not full_update:
                    # 检查是否已有最近的估值数据
                    existing_valuation = self.session.query(StockValuationInfo)\
                        .filter(StockValuationInfo.symbol == self.symbol)\
                        .filter(StockValuationInfo.update_date >= thirty_days_ago)\
                        .order_by(StockValuationInfo.update_date.desc())\
                        .first()
                    has_data = existing_valuation is not None
                
                # 如果今天还没有更新过估值信息
                if not has_data or full_update:
                    df = ak.stock_individual_info_em(symbol=self.symbol)
                    if not df.empty:
                        info = df.set_index("item")["value"]
                        
                        valuation = StockValuationInfo(
                            symbol=self.symbol,
                            market_cap=float(info.get("总市值", 0)),
                            total_shares=float(info.get("总股本", 0)),
                            outstanding_shares=float(info.get("流通股本", 0)),
                            pe_ratio=float(info.get("市盈率(动)", 0)),
                            pb_ratio=float(info.get("市净率", 0)),
                            update_date=now
                        )
                        self.session.merge(valuation)
                        self.session.commit()
                        logger.info("估值信息更新完成")
                        
                        # 删除30天前的估值数据
                        self.session.query(StockValuationInfo)\
                            .filter(StockValuationInfo.symbol == self.symbol)\
                            .filter(StockValuationInfo.update_date < thirty_days_ago)\
                            .delete()
                        self.session.commit()
                else:
                    logger.info("估值信息无需更新")
            except Exception as e:
                logger.error(f"获取估值信息时发生错误: {str(e)}")
                self.session.rollback()
            
            logger.info(f"股票 {self.symbol} 的历史数据获取完成")
            
        except Exception as e:
            logger.error(f"获取历史数据时发生错误: {str(e)}")

    def update_daily_data(self):
        """更新股票的每日数据"""
        try:
            logger.info(f"开始更新股票 {self.symbol} 的每日数据...")
            
            # 检查今天是否已经更新过
            last_update = self.session.query(StockValuationInfo.update_date)\
                .filter(StockValuationInfo.symbol == self.symbol)\
                .order_by(StockValuationInfo.update_date.desc())\
                .first()
            
            if last_update and last_update[0].date() == datetime.now().date():
                logger.info(f"股票 {self.symbol} 今日已更新，跳过更新")
                return
            
            # 1. 更新每日价格数据
            self.update_daily_price()
            time.sleep(1)
            
            # 2. 更新财务指标（如果有新的季报）
            try:
                logger.info("2. 检查更新财务指标...")
                df = ak.stock_financial_analysis_indicator(symbol=self.symbol)
                if not df.empty:
                    latest_report_date = pd.to_datetime(df.iloc[0]['报告期'])
                    
                    # 检查是否有新的财务报告
                    existing_report = self.session.query(StockFinancialMetrics)\
                        .filter(StockFinancialMetrics.symbol == self.symbol,
                                StockFinancialMetrics.report_date == latest_report_date)\
                        .first()
                    
                    if not existing_report:
                        # 有新的财务报告，更新数据
                        row = df.iloc[0]
                        metrics = StockFinancialMetrics(
                            symbol=self.symbol,
                            report_date=latest_report_date,
                            roe=float(row['净资产收益率']),
                            net_profit_ratio=float(row['净利率']),
                            gross_profit_rate=float(row['毛利率']),
                            net_profits=float(row['净利润']),
                            eps=float(row['每股收益']),
                            main_business_income=float(row['主营业务收入']),
                            update_date=datetime.now()
                        )
                        self.session.merge(metrics)
                        self.session.commit()
                        logger.info("新的财务指标已更新")
            except Exception as e:
                logger.error(f"更新财务指标时发生错误: {str(e)}")
                self.session.rollback()
            
            time.sleep(1)
            
            # 3. 更新内部交易
            try:
                logger.info("3. 更新内部交易...")
                if self.symbol.startswith('6'):
                    df = ak.stock_share_hold_change_sse(symbol=self.symbol)
                    if not df.empty:
                        df['exchange'] = 'SSE'
                elif self.symbol.startswith(('0', '3')):
                    df = ak.stock_share_hold_change_szse(symbol=self.symbol)
                    if not df.empty:
                        df['exchange'] = 'SZSE'
                
                if not df.empty:
                    today = datetime.now().date()
                    today_trades = df[pd.to_datetime(df['变动日期']).dt.date == today]
                    
                    for _, row in today_trades.iterrows():
                        holder_name = row.get('董监高姓名') or row.get('股份变动人姓名', '未知')
                        shares_val = row.get("变动股份数量",0)
                        shares_changed = int(float(shares_val)) if pd.notna(shares_val) else 0
                        raw_price = row.get("成交均价")
                        price = float(raw_price) if pd.notna(raw_price) else 0.0

                        trade = StockInsiderTrades(
                            symbol=self.symbol,
                            holder_name=holder_name,
                            change_date=pd.to_datetime(row.get('变动日期')),
                            change_type=row.get('变动原因', ''),
                            shares_changed=shares_changed,
                            price=price,
                            update_date=datetime.now()
                        )
                        self.session.merge(trade)
                    self.session.commit()
                    logger.info("今日内部交易更新完成")
            except Exception as e:
                logger.error(f"更新内部交易时发生错误: {str(e)}")
                self.session.rollback()
            
            time.sleep(1)
            
            # 4. 更新今日新闻
            try:
                logger.info("4. 更新今日新闻...")
                df = ak.stock_news_em(symbol=self.symbol)
                
                if not df.empty:
                    today = datetime.now().date()
                    today_news = df[pd.to_datetime(df['时间']).dt.date == today]
                    
                    for _, row in today_news.iterrows():
                        date_str = row.get('时间') or row.get('日期')
                        if not date_str:
                            continue
                        
                        news = StockNews(
                            symbol=self.symbol,
                            title=row.get('标题', ''),
                            date=pd.to_datetime(date_str),
                            source='东方财富',
                            url=row.get('链接', ''),
                            update_date=datetime.now()
                        )
                        self.session.merge(news)
                    self.session.commit()
                    logger.info("今日新闻更新完成")
            except Exception as e:
                logger.error(f"更新今日新闻时发生错误: {str(e)}")
                self.session.rollback()
            
            time.sleep(1)
            
            # 5. 更新估值信息
            try:
                logger.info("5. 更新估值信息...")
                df = ak.stock_individual_info_em(symbol=self.symbol)
                info = df.set_index("item")["value"]
                
                valuation = StockValuationInfo(
                    symbol=self.symbol,
                    market_cap=float(info.get("总市值", 0)),
                    total_shares=float(info.get("总股本", 0)),
                    outstanding_shares=float(info.get("流通股本", 0)),
                    pe_ratio=float(info.get("市盈率(动)", 0)),
                    pb_ratio=float(info.get("市净率", 0)),
                    update_date=datetime.now()
                )
                self.session.merge(valuation)
                self.session.commit()
                logger.info("估值信息更新完成")
            except Exception as e:
                logger.error(f"更新估值信息时发生错误: {str(e)}")
                self.session.rollback()
            
            logger.info(f"股票 {self.symbol} 的每日数据更新完成")
            
        except Exception as e:
            logger.error(f"更新每日数据时发生错误: {str(e)}")

    def update_daily_price(self):
        """更新股票最新的每日价格数据"""
        try:
            logger.info(f"获取股票 {self.symbol} 的每日价格数据...")
            
            # 获取最新的价格记录
            latest_price = self.session.query(StockDaily)\
                .filter(StockDaily.symbol == self.symbol)\
                .order_by(StockDaily.trade_date.desc())\
                .first()
            
            # 获取当日数据
            df = ak.stock_zh_a_hist(symbol=self.symbol, period='daily', adjust='qfq', start_date=datetime.now().strftime('%Y%m%d'))
            if df.empty:
                logger.info(f"{self.symbol} 今日无交易数据")
                return
            
            for _, row in df.iterrows():
                trade_date = pd.to_datetime(row['日期'])
                
                # 如果已有最新数据且日期相同，则跳过
                if latest_price and latest_price.trade_date.date() == trade_date.date():
                    continue
                    
                price = StockDaily(
                    symbol=self.symbol,
                    trade_date=trade_date,
                    open=float(row['开盘']),
                    high=float(row['最高']),
                    low=float(row['最低']),
                    close=float(row['收盘']),
                    volume=float(row['成交量']),
                    amount=float(row['成交额'])
                )
                self.session.merge(price)
            
            self.session.commit()
            logger.info(f"{self.symbol} 的每日价格数据更新完成")
        except Exception as e:
            logger.error(f"每日价格更新失败: {e}")
            self.session.rollback()

def update_daily_data(symbol: str):
    """每日更新指定股票的数据"""
    try:
        # 检查今天是否已经更新过
        db_dir = Path('data/db')
        db_path = db_dir / f'stock_{symbol}.db'
        
        if db_path.exists():
            engine = create_engine(f'sqlite:///{db_path}')
            Session = sessionmaker(bind=engine)
            session = Session()
            
            # 检查最后更新时间
            last_update = session.query(StockValuationInfo.update_date)\
                .filter(StockValuationInfo.symbol == symbol)\
                .order_by(StockValuationInfo.update_date.desc())\
                .first()
            
            if last_update and last_update[0].date() == datetime.now().date():
                logger.info(f"股票 {symbol} 今日已更新，跳过更新")
                return
        
        # 创建加载器并更新数据
        loader = StockDataLoader(symbol)
        loader.update_all_data()
        
    except Exception as e:
        logger.error(f"更新股票 {symbol} 数据时发生错误: {str(e)}")

def main():
    """主函数"""
    symbol = "601318"  # 示例股票代码
    symbol_ak = format_symbol(symbol) 
    
    try:
        logger.info(f"开始初始化股票 {symbol} 的所有历史数据...")
        loader = StockDataLoader(symbol)
        
        # 1. 创建数据库和表结构
        Base.metadata.create_all(loader.engine)
        logger.info("数据库和表结构创建完成")
        
        # 2. 获取基本信息
        logger.info("开始获取股票基本信息...")
        loader.update_stock_basic(symbol)
        time.sleep(1)
        
        # 3. 获取所有历史K线数据
        logger.info("开始获取所有历史K线数据...")
        try:
            df = ak.stock_zh_a_hist(symbol=symbol, period='daily', adjust='qfq', start_date='19900101')
            if not df.empty:
                for _, row in df.iterrows():
                    price = StockDaily(
                        symbol=symbol,
                        trade_date=pd.to_datetime(row['日期']),
                        open=float(row['开盘']),
                        high=float(row['最高']),
                        low=float(row['最低']),
                        close=float(row['收盘']),
                        volume=float(row['成交量']),
                        amount=float(row['成交额'])
                    )
                    loader.session.merge(price)
                loader.session.commit()
                logger.info(f"历史K线数据获取完成，共 {len(df)} 条记录")
        except Exception as e:
            logger.error(f"获取历史K线数据失败: {e}")
            loader.session.rollback()
        time.sleep(1)
        
        # 4. 获取所有财务指标数据
        logger.info("开始获取所有财务指标数据...")
        try:
            df = ak.stock_financial_abstract(symbol=symbol)
            print("字段名：", df.columns.tolist())
            print(df.head(3))
            if not df.empty:
                try:
                    for _, row in df.iterrows():

                        raw_date = row.get('报告期') or row.get('报告日期') or row.get('发布日期')
                        report_date = pd.to_datetime(raw_date) if raw_date else pd.Timestamp("1900-01-01")

                        metrics = StockFinancialMetrics(
                            symbol=symbol,
                            report_date=report_date,
                            roe=float(row['净资产收益率']),
                            net_profit_ratio=float(row['净利率']),
                            gross_profit_rate=float(row['毛利率']),
                            net_profits=float(row['净利润']),
                            eps=float(row['每股收益']),
                            main_business_income=float(row['主营业务收入']),
                            update_date=datetime.now()
                    )
                    loader.session.merge(metrics)
                except Exception as row_err:
                    logger.warning(f"跳过一条异常财务指标记录: {row_err}")
                loader.session.commit()
                logger.info(f"财务指标数据获取完成，共 {len(df)} 条记录")
        except Exception as e:
            logger.error(f"获取财务指标数据失败: {e}")
            loader.session.rollback()
        time.sleep(1)
        
        # 5. 获取所有内部交易数据
        logger.info("开始获取所有内部交易数据...")
        try:
            if symbol.startswith('6'):
                df = ak.stock_share_hold_change_sse(symbol=symbol)
            elif symbol.startswith(('0', '3')):
                df = ak.stock_share_hold_change_szse(symbol=symbol)
            
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    holder_name = row.get('董监高姓名') or row.get('股份变动人姓名', '未知')
                    shares_val = row.get("变动股份数量",0)
                    shares_changed = int(float(shares_val)) if pd.notna(shares_val) else 0
                    raw_price = row.get("成交均价")
                    price = float(raw_price) if pd.notna(raw_price) else 0.0

                    trade = StockInsiderTrades(
                        symbol=symbol,
                        holder_name=holder_name,
                        change_date=pd.to_datetime(row.get('变动日期')),
                        change_type=row.get('变动原因', ''),
                        shares_changed=shares_changed,
                        price=price,
                        update_date=datetime.now()
                    )
                    loader.session.merge(trade)
                loader.session.commit()
                logger.info(f"内部交易数据获取完成，共 {len(df)} 条记录")
        except Exception as e:
            logger.error(f"获取内部交易数据失败: {e}")
            loader.session.rollback()
        time.sleep(1)
        
        # 6. 获取最近一年的新闻数据
        logger.info("开始获取最近一年的新闻数据...")
        try:
            df = ak.stock_news_em(symbol=symbol_ak)
            if not df.empty:
                one_year_ago = datetime.now() - timedelta(days=365)
                for _, row in df.iterrows():
                    date_str = row.get('时间') or row.get('日期')
                    if not date_str:
                        continue
                    
                    news_date = pd.to_datetime(date_str)
                    if news_date < one_year_ago:
                        continue
                    
                    news = StockNews(
                        symbol=symbol,
                        title=row.get('标题', ''),
                        date=news_date,
                        source='东方财富',
                        url=row.get('链接', ''),
                        update_date=datetime.now()
                    )
                    loader.session.merge(news)
                loader.session.commit()
                logger.info("新闻数据获取完成")
        except Exception as e:
            logger.error(f"获取新闻数据失败: {e}")
            loader.session.rollback()
        time.sleep(1)
        
        # 7. 获取当前估值信息
        logger.info("开始获取当前估值信息...")
        try:
            query_symbol = symbol
            if query_symbol.startswith(('sh', 'sz')):
                query_symbol = query_symbol[2:]
            
            df = ak.stock_individual_info_em(symbol=query_symbol)
            if not df.empty:
                # 创建字典存储需要的值
                info_dict = {}
                for _, row in df.iterrows():
                    item = row['item']
                    value = row['value']
                    info_dict[item] = value
                
                # 处理各个字段
                try:
                    market_cap = float(info_dict.get("总市值", "0").replace(",", ""))
                except:
                    market_cap = 0.0
                
                try:
                    total_shares = float(info_dict.get("总股本", "0").replace(",", ""))
                except:
                    total_shares = 0.0
                
                try:
                    outstanding_shares = float(info_dict.get("流通股本", "0").replace(",", ""))
                except:
                    outstanding_shares = 0.0
                
                try:
                    pe_ratio = float(info_dict.get("市盈率(动)", "0").replace(",", ""))
                except:
                    pe_ratio = 0.0
                
                try:
                    pb_ratio = float(info_dict.get("市净率", "0").replace(",", ""))
                except:
                    pb_ratio = 0.0
                
                valuation = StockValuationInfo(
                    symbol=symbol,
                    market_cap=market_cap,
                    total_shares=total_shares,
                    outstanding_shares=outstanding_shares,
                    pe_ratio=pe_ratio,
                    pb_ratio=pb_ratio,
                    update_date=datetime.now()
                )
                
                loader.session.merge(valuation)
                loader.session.commit()
                logger.info("估值信息获取完成")
        except Exception as e:
            logger.error(f"获取估值信息失败: {e}")
            logger.error("错误详情:", exc_info=True)
            loader.session.rollback()
        
        logger.info(f"股票 {symbol} 的所有历史数据初始化完成")
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    main() 