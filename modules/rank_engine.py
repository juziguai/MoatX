"""
rank_engine.py - MoatX 综合评分引擎
对候选股票进行技术面+基本面+资金面综合评分
"""

import pandas as pd
from typing import Optional

from modules.stock_data import StockData


class RankEngine:
    """
    综合评分引擎
    评分维度：趋势（25%）+ 估值（25%）+ 资金（25%）+ 动量（25%）
    """

    def __init__(self):
        self._spot_cache = None

    def rank(self, symbols: list, with_fundamentals: bool = True) -> pd.DataFrame:
        """
        对股票列表进行综合评分

        Args:
            symbols: 股票代码列表，如 ["600519", "000001"]
            with_fundamentals: 是否抓取财务数据（PE/PB/ROE）

        Returns:
            DataFrame: code, name, score, trend_score, val_score, flow_score, momentum_score
        """
        if not symbols:
            return pd.DataFrame()

        # 实时行情
        spot = self._get_spot(symbols)

        results = []

        for _, row in spot.iterrows():
            code = str(row.get("代码", row.get("code", "")))
            name = str(row.get("名称", row.get("name", code)))
            price = float(row.get("最新价", 0)) if pd.notna(row.get("最新价")) else 0
            pct = float(row.get("涨跌幅", 0)) if pd.notna(row.get("涨跌幅")) else 0
            turnover = float(row.get("换手率", 0)) if pd.notna(row.get("换手率")) else 0
            pe = row.get("市盈率-动态")
            pb = row.get("市净率")

            try:
                pe = float(pe) if pe not in [None, "NaN", ""] else None
            except Exception:
                pe = None
            try:
                pb = float(pb) if pb not in [None, "NaN", ""] else None
            except Exception:
                pb = None

            # 1. 趋势分（基于涨跌幅和换手率）
            trend_score = self._score_trend(pct, turnover)

            # 2. 估值分（基于PE/PB）
            val_score = self._score_valuation(pe, pb)

            # 3. 动量分（基于涨跌幅）
            momentum_score = self._score_momentum(pct)

            # 4. 资金分（换手率作为代理）
            flow_score = self._score_flow(turnover)

            # 综合分
            composite = (
                trend_score * 0.25 +
                val_score * 0.25 +
                flow_score * 0.25 +
                momentum_score * 0.25
            )

            results.append({
                "code": code,
                "name": name,
                "price": price,
                "pct_change": pct,
                "pe": pe,
                "pb": pb,
                "turnover": turnover,
                "trend_score": round(trend_score, 2),
                "val_score": round(val_score, 2),
                "flow_score": round(flow_score, 2),
                "momentum_score": round(momentum_score, 2),
                "composite": round(composite, 2),
            })

        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values("composite", ascending=False)
        return df

    def rank_all(self, limit: int = 50, with_fundamentals: bool = False) -> pd.DataFrame:
        """对全市场股票评分（取综合分最高的前N只，基于 Sina 快照）"""
        try:
            from modules.stock_data import StockData
            sd = StockData()
            spot = sd.get_spot()
            if spot.empty:
                return pd.DataFrame()
            symbols = spot["code"].astype(str).tolist()
            top_symbols = symbols[:500]
            return self.rank(top_symbols, with_fundamentals=with_fundamentals).head(limit)
        except Exception:
            return pd.DataFrame()

    # ── 评分子函数 ──────────────────────────────

    @staticmethod
    def _score_trend(pct_change: float, turnover: float) -> float:
        """趋势分：涨跌幅 + 换手率"""
        score = 50  # 基准分
        score += min(max(pct_change * 5, -30), 30)  # 涨跌贡献 [-30, +30]
        score += min(turnover * 2, 20)  # 换手率贡献 [0, 20]
        return max(0, min(100, score))

    @staticmethod
    def _score_valuation(pe: Optional[float], pb: Optional[float]) -> float:
        """估值分：PE + PB 组合打分"""
        score = 50

        if pe is not None and pe > 0:
            if pe < 10:
                score += 20
            elif pe < 20:
                score += 10
            elif pe < 40:
                score += 0
            elif pe < 70:
                score -= 15
            else:
                score -= 30

        if pb is not None and pb > 0:
            if pb < 2:
                score += 15
            elif pb < 5:
                score += 5
            elif pb < 10:
                score -= 5
            else:
                score -= 15

        return max(0, min(100, score))

    @staticmethod
    def _score_momentum(pct_change: float) -> float:
        """动量分：短期涨幅"""
        score = 50
        if pct_change > 5:
            score += 25
        elif pct_change > 2:
            score += 15
        elif pct_change > 0:
            score += 5
        elif pct_change > -2:
            score -= 5
        elif pct_change > -5:
            score -= 15
        else:
            score -= 25
        return max(0, min(100, score))

    @staticmethod
    def _score_flow(turnover: float) -> float:
        """资金活跃度分"""
        score = 50
        if turnover > 10:
            score += 30
        elif turnover > 5:
            score += 20
        elif turnover > 2:
            score += 10
        elif turnover > 0.5:
            score += 0
        else:
            score -= 15
        return max(0, min(100, score))

    def _get_spot(self, symbols: list) -> pd.DataFrame:
        """获取指定股票的实时行情（复用 StockData 的 30s Parquet 缓存）"""
        try:
            sd = StockData()
            full_spot = sd.get_spot(use_cache=True)
            if full_spot.empty:
                return pd.DataFrame()

            # 过滤目标股票
            symbol_set = set(str(s) for s in symbols)
            filtered = full_spot[full_spot["code"].isin(symbol_set)].copy()

            # 转换为 rank() 期望的列名格式
            records = []
            for _, row in filtered.iterrows():
                records.append({
                    "代码": row.get("code", ""),
                    "名称": row.get("name", ""),
                    "code": row.get("code", ""),
                    "name": row.get("name", ""),
                    "最新价": row.get("price", 0),
                    "涨跌幅": row.get("pct_change", 0),
                    "换手率": row.get("turnover", 0),
                    "市盈率-动态": row.get("pe"),
                    "市净率": row.get("pb"),
                })
            return pd.DataFrame(records)
        except Exception:
            return pd.DataFrame()
