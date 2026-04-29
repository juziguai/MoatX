"""历史数据供给 — 从 StockData 加载回测所需数据"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from modules.stock_data import StockData


class DataFeed:
    """回测数据供给。

    preload() 一次性加载全时段数据，handle_bar 时直接切片。
    """

    def __init__(self, symbol: str, start: date, end: date):
        self.symbol = symbol
        self.start = start
        self.end = end
        self._data: pd.DataFrame | None = None
        self._sd = StockData()

    def preload(self) -> pd.DataFrame:
        """Load all daily data for the backtest period."""
        start_str = (self.start - timedelta(days=60)).strftime("%Y%m%d")  # buffer for indicators
        end_str = self.end.strftime("%Y%m%d")
        df = self._sd.get_daily(self.symbol, start_date=start_str, end_date=end_str, adjust="qfq")
        if df.empty:
            return df
        # date is in the index, promote to column
        df = df.reset_index()
        df = df[(df["date"] >= pd.Timestamp(self.start)) & (df["date"] <= pd.Timestamp(self.end))]
        df = df.sort_values("date").reset_index(drop=True)
        self._data = df
        return df

    def get_slice(self, idx: int, lookback: int = 60) -> pd.DataFrame:
        """Get data up to and including index `idx`."""
        if self._data is None:
            return pd.DataFrame()
        start = max(0, idx - lookback + 1)
        return self._data.iloc[start: idx + 1].reset_index(drop=True)

    @property
    def data(self) -> pd.DataFrame:
        return self._data if self._data is not None else pd.DataFrame()

    def __len__(self) -> int:
        return len(self._data) if self._data is not None else 0
