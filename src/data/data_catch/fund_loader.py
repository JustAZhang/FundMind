import sqlite3
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd


class ChineseMarketDataCollector:
    """中国市场数据采集器"""

    def __init__(self, db_path: str = "chinese_market_data.db"):
        """
        初始化数据采集器
        Args:
            db_path: SQLite数据库路径
        """
        self.db_path = db_path
        self._init_database()

    def _init_database(self):
        """初始化数据库表结构"""

        # 创建数据库连接
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 创建基金净值表
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS fund_nav (
            update_time TEXT,
            source TEXT,
            fund_code TEXT,
            trade_date TEXT,
            net_asset_value REAL,
            accumulate_net_value REAL,
            daily_return REAL,
            PRIMARY KEY (fund_code, trade_date)
        )
        """
        )

        # 创建基金持仓表
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS fund_holdings (
            fund_code TEXT,
            report_date TEXT,
            industry TEXT,
            holding_ratio REAL,
            PRIMARY KEY (fund_code, report_date, industry)
        )
        """
        )

        # 创建沪深300指数表
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS hs300_daily (
            index_code TEXT,
            trade_date TEXT PRIMARY KEY,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            volume INTEGER,
            amount INTEGER
        )
        """
        )

        # 创建宏观经济指标表
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS macro_indicators (
            indicator_name TEXT,
            date TEXT,
            value REAL,
            PRIMARY KEY (indicator_name, date)
        )
        """
        )

        conn.commit()
        conn.close()

    def collect_fund_nav(self, fund_code: str):
        """
        获取基金净值数据
        Args:
            fund_code: 基金代码
        """

        # 计算过去2年的日期
        end_date = datetime.now()
        start_date = end_date - timedelta(days=2 * 365)

        try:
            # 获取基金净值数据
            fund_data = ak.fund_open_fund_info_em(fund=fund_code, indicator="单位净值走势")

            # 数据清洗和转换
            fund_data.columns = ["trade_date", "net_asset_value", "accumulate_net_value", "daily_return"]
            fund_data["fund_code"] = fund_code

            # 过滤日期范围
            fund_data["trade_date"] = pd.to_datetime(fund_data["trade_date"])
            fund_data = fund_data[(fund_data["trade_date"] >= start_date) & (fund_data["trade_date"] <= end_date)]

            # 补充更新时间和数据来源
            fund_data["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fund_data["source"] = "akshare"

            # 调整列顺序以匹配表结构
            fund_data = fund_data[
                [
                    "update_time",
                    "source",
                    "fund_code",
                    "trade_date",
                    "net_asset_value",
                    "accumulate_net_value",
                    "daily_return",
                ]
            ]

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            # 使用 INSERT OR REPLACE 避免主键冲突
            cursor.executemany(
                """
                INSERT OR REPLACE INTO fund_nav (
                    update_time,
                    source,
                    fund_code,
                    trade_date,
                    net_asset_value,
                    accumulate_net_value,
                    daily_return
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.update_time,
                        row.source,
                        row.fund_code,
                        row.trade_date.strftime("%Y-%m-%d"),
                        row.net_asset_value,
                        row.accumulate_net_value,
                        row.daily_return,
                    )
                    for row in fund_data.itertuples(index=False)
                ],
            )
            conn.commit()
            conn.close()

            print(f"成功获取基金 {fund_code} 的净值数据")

        except Exception as e:
            print(f"获取基金 {fund_code} 净值数据失败: {str(e)}")

    def collect_fund_holdings(self, fund_code: str):
        """
        获取基金持仓数据
        Args:
            fund_code: 基金代码
        """

        try:
            # 获取基金持仓数据
            holdings_data = ak.fund_portfolio_hold_em(code=fund_code)

            if holdings_data is not None and not holdings_data.empty:
                # 数据清洗和转换
                holdings_data["fund_code"] = fund_code
                holdings_data["report_date"] = datetime.now().strftime("%Y-%m-%d")

                # 保存到数据库
                conn = sqlite3.connect(self.db_path)
                holdings_data.to_sql("fund_holdings", conn, if_exists="append", index=False)
                conn.close()

                print(f"成功获取基金 {fund_code} 的持仓数据")

        except Exception as e:
            print(f"获取基金 {fund_code} 持仓数据失败: {str(e)}")

    def collect_hs300_data(self):
        """获取沪深300指数数据"""

        try:
            # 获取沪深300数据
            hs300_data = ak.stock_zh_index_daily(symbol="sh000300")

            # 过滤最近一年的数据
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)

            hs300_data["date"] = pd.to_datetime(hs300_data.index)
            hs300_data = hs300_data[(hs300_data["date"] >= start_date) & (hs300_data["date"] <= end_date)]

            # 保存到数据库
            conn = sqlite3.connect(self.db_path)
            hs300_data.to_sql("hs300_daily", conn, if_exists="replace", index=False)
            conn.close()

            print("成功获取沪深300指数数据")

        except Exception as e:
            print(f"获取沪深300指数数据失败: {str(e)}")

    def collect_macro_indicators(self):
        """获取宏观经济指标"""

        try:
            # 获取CPI数据
            cpi_data = ak.macro_china_cpi_monthly()
            cpi_data["indicator_name"] = "CPI"

            # 获取GDP数据
            gdp_data = ak.macro_china_gdp_yearly()
            gdp_data["indicator_name"] = "GDP"

            # 获取PMI数据
            pmi_data = ak.macro_china_pmi_yearly()
            pmi_data["indicator_name"] = "PMI"

            # 合并所有指标数据
            all_indicators = pd.concat([cpi_data, gdp_data, pmi_data])

            # 保存到数据库
            conn = sqlite3.connect(self.db_path)
            all_indicators.to_sql("macro_indicators", conn, if_exists="replace", index=False)
            conn.close()

            print("成功获取宏观经济指标数据")

        except Exception as e:
            print(f"获取宏观经济指标数据失败: {str(e)}")
