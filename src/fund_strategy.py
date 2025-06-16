import sqlite3
import time
from datetime import datetime, timedelta

import pandas as pd
import akshare as ak

from src.data.data_catch.fund_loader import ChineseMarketDataCollector


class FundStrategy:
    """Simple strategy for Chinese mutual funds."""

    def __init__(self, db_path: str = "chinese_market_data.db"):
        self.db_path = db_path
        self.collector = ChineseMarketDataCollector(db_path)
        self._init_portfolio()

    def _init_portfolio(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio (
                fund_code TEXT PRIMARY KEY,
                shares INTEGER,
                cost REAL,
                last_updated TEXT
            )
            """
        )
        conn.commit()
        conn.close()

    def update_data(self, fund_codes: list[str]):
        for code in fund_codes:
            self.collector.collect_fund_nav(code)

    def get_top_fund(self, lookback_days: int = 30):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql(
            "SELECT fund_code, trade_date, net_asset_value FROM fund_nav", conn
        )
        conn.close()
        if df.empty:
            return None
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        cutoff = df["trade_date"].max() - timedelta(days=lookback_days)
        df = df[df["trade_date"] >= cutoff]
        if df.empty:
            return None
        grouped = (
            df.groupby("fund_code")
            .agg(first=("net_asset_value", "first"), last=("net_asset_value", "last"))
            .assign(pct_change=lambda x: (x["last"] - x["first"]) / x["first"])
        )
        best_code = grouped["pct_change"].idxmax()
        best_change = grouped.loc[best_code, "pct_change"]
        try:
            info = ak.fund_basic_info_em(fund=best_code)
            best_name = info.loc[0, "基金简称"] if not info.empty else ""
        except Exception:
            best_name = ""
        return best_code, best_name, best_change

    def decide_hold(self, fund_code: str, short: int = 5, long: int = 20) -> bool:
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql(
            f"SELECT trade_date, net_asset_value FROM fund_nav WHERE fund_code='{fund_code}' ORDER BY trade_date",
            conn,
        )
        conn.close()
        if df.empty:
            return False
        df["ma_short"] = df["net_asset_value"].rolling(short).mean()
        df["ma_long"] = df["net_asset_value"].rolling(long).mean()
        latest = df.iloc[-1]
        return latest["ma_short"] >= latest["ma_long"]

    def buy_fund(self, fund_code: str, amount: int = 1):
        now = datetime.now().isoformat()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        row = cursor.execute(
            "SELECT shares FROM portfolio WHERE fund_code=?", (fund_code,)
        ).fetchone()
        if row:
            shares = row[0] + amount
            cursor.execute(
                "UPDATE portfolio SET shares=?, last_updated=? WHERE fund_code=?",
                (shares, now, fund_code),
            )
        else:
            cursor.execute(
                "INSERT INTO portfolio (fund_code, shares, cost, last_updated) VALUES (?, ?, ?, ?)",
                (fund_code, amount, 0, now),
            )
        conn.commit()
        conn.close()

    def sell_fund(self, fund_code: str):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM portfolio WHERE fund_code=?", (fund_code,))
        conn.commit()
        conn.close()

    def monitor(self, fund_code: str):
        if self.decide_hold(fund_code):
            print(f"Hold {fund_code}")
        else:
            print(f"Sell {fund_code}")
            self.sell_fund(fund_code)


def run_strategy(fund_codes: list[str], interval_hours: int = 24):
    strategy = FundStrategy()
    while True:
        strategy.update_data(fund_codes)
        top = strategy.get_top_fund()
        if top:
            code, name, change = top
            print(
                f"Best fund: {code} {name} {change:.2%}"
            )
            strategy.buy_fund(code)
            strategy.monitor(code)
        else:
            print("No data")
        time.sleep(interval_hours * 3600)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Chinese fund strategy")
    parser.add_argument("--funds", type=str, required=True, help="Comma separated fund codes")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    fund_codes = [f.strip() for f in args.funds.split(",")]
    if args.once:
        s = FundStrategy()
        s.update_data(fund_codes)
        top = s.get_top_fund()
        if top:
            code, name, change = top
            print(f"Best fund: {code} {name} {change:.2%}")
            s.buy_fund(code)
            s.monitor(code)
        else:
            print("No data")
    else:
        run_strategy(fund_codes)
