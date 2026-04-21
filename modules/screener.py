"""
screener.py - MoatX 选股器
两阶段筛选：全市场快照 → 本地技术/基本面过滤
"""

import akshare as ak
import pandas as pd
import numpy as np
from typing import Optional, Literal


class MoatXScreener:
    """MoatX 选股器"""

    def __init__(self):
        self._spot_cache = None
        self._spot_cache_time = None

    # ─────────────────────────────────────────────
    # 全市场快照（带缓存）
    # ─────────────────────────────────────────────

    def get_spot(self) -> pd.DataFrame:
        """获取全市场实时行情（带60秒缓存）"""
        import time
        now = time.time()
        if self._spot_cache is not None and (now - self._spot_cache_time) < 60:
            return self._spot_cache
        try:
            self._spot_cache = ak.stock_zh_a_spot_em()
            self._spot_cache_time = now
            return self._spot_cache
        except Exception:
            return pd.DataFrame()

    # ─────────────────────────────────────────────
    # 全市场扫描（本地过滤，无需候选池）
    # ─────────────────────────────────────────────

    def scan_all(
        self,
        # 基本面
        pe_range: tuple = (0, 50),
        pb_range: tuple = (0, 10),
        cap_min: Optional[float] = None,
        cap_max: Optional[float] = None,
        turnover_min: Optional[float] = None,
        # 资金流向
        money_in_min: Optional[float] = None,   # 主力净流入最低占比(%)
        # 技术面（需结合日线数据）
        tech_enabled: bool = False,
        volume_ratio_min: Optional[float] = None,  # 量比最低
        pct_change_min: Optional[float] = None,     # 涨幅最低(%)
        pct_change_max: Optional[float] = None,    # 涨幅最高(%)
        # 排序
        sort_by: str = "涨跌幅",
        ascending: bool = False,
        limit: int = 50
    ) -> pd.DataFrame:
        """
        全市场扫描，直接在实时行情上过滤

        注意：ROE/毛利率等财务数据需另外接口，此处仅用PE/PB/市值/换手率/涨跌幅/量比
        如需ROE精筛，先用本方法初筛，再调用 fundamentals_filter 对候选股做深度筛选
        """
        spot = self.get_spot()
        if spot.empty:
            return pd.DataFrame()

        df = spot.copy()

        # 列名标准化
        rename_map = {
            "代码": "code",
            "名称": "name",
            "最新价": "price",
            "涨跌幅": "pct_change",
            "涨跌额": "change",
            "成交量": "volume",
            "成交额": "amount",
            "振幅": "amplitude",
            "最高": "high",
            "最低": "low",
            "今开": "open",
            "昨收": "prev_close",
            "量比": "volume_ratio",
            "换手率": "turnover",
            "市盈率-动态": "pe",
            "市净率": "pb",
            "总市值": "total_cap",
            "流通市值": "float_cap",
            "涨速": "speed",
            "5分钟涨跌": "pct_5min",
            "60日涨跌幅": "pct_60d",
            "年初至今涨跌幅": "pct_ytd",
        }
        df.rename(columns=rename_map, inplace=True)

        # 过滤
        if pe_range:
            df = df[(df["pe"] >= pe_range[0]) & (df["pe"] <= pe_range[1])]
        if pb_range:
            df = df[(df["pb"] >= pb_range[0]) & (df["pb"] <= pb_range[1])]

        if cap_min is not None:
            df = df[df["float_cap"] >= cap_min]
        if cap_max is not None:
            df = df[df["float_cap"] <= cap_max]
        if turnover_min is not None:
            df = df[df["turnover"] >= turnover_min]

        if pct_change_min is not None:
            df = df[df["pct_change"] >= pct_change_min]
        if pct_change_max is not None:
            df = df[df["pct_change"] <= pct_change_max]

        if volume_ratio_min is not None:
            df = df[df["volume_ratio"] >= volume_ratio_min]

        if sort_by in df.columns:
            df = df.sort_values(sort_by, ascending=ascending)

        return df.head(limit)

    # ─────────────────────────────────────────────
    # 板块扫描
    # ─────────────────────────────────────────────

    def scan_industry(
        self,
        industry_name: str,
        top_n: int = 10,
        conditions: Optional[dict] = None
    ) -> pd.DataFrame:
        """扫描指定行业板块内个股"""
        try:
            df = ak.stock_board_industry_cons_em(symbol=industry_name)
            rename_map = {
                "代码": "code", "名称": "name", "最新价": "price",
                "涨跌幅": "pct_change", "涨跌额": "change",
                "成交量": "volume", "成交额": "amount",
                "振幅": "amplitude", "最高": "high", "最低": "low",
                "今开": "open", "昨收": "prev_close",
                "量比": "volume_ratio", "换手率": "turnover",
                "市盈率-动态": "pe", "市净率": "pb",
                "总市值": "total_cap", "流通市值": "float_cap",
            }
            df.rename(columns=rename_map, inplace=True)

            if conditions:
                if "pe_range" in conditions:
                    df = df[(df["pe"] >= conditions["pe_range"][0]) &
                            (df["pe"] <= conditions["pe_range"][1])]
                if "pb_range" in conditions:
                    df = df[(df["pb"] >= conditions["pb_range"][0]) &
                            (df["pb"] <= conditions["pb_range"][1])]

            return df.head(top_n)
        except Exception:
            return pd.DataFrame()

    def scan_all_industries(self, limit_per: int = 3) -> pd.DataFrame:
        """扫描所有行业板块，每个板块取前limit_per只"""
        try:
            industries = ak.stock_board_industry_name_em()
            results = []
            for _, row in industries.iterrows():
                name = row.get("板块名称")
                try:
                    stocks = self.scan_industry(name, top_n=limit_per)
                    if not stocks.empty:
                        stocks["行业"] = name
                        results.append(stocks)
                except Exception:
                    continue
            if results:
                return pd.concat(results, ignore_index=True)
            return pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    # ─────────────────────────────────────────────
    # 资金流向排名
    # ─────────────────────────────────────────────

    def money_flow_rank(
        self,
        period: Literal["今日", "3日", "5日", "10日"] = "今日",
        direction: Literal["in", "out", "all"] = "in",
        limit: int = 50
    ) -> pd.DataFrame:
        """资金流向排名"""
        try:
            df = ak.stock_individual_fund_flow_rank(indicator=period)
            rename_map = {
                "代码": "code", "名称": "name", "最新价": "price",
                "今日涨跌幅": "pct_change",
                "今日主力净流入-净额": "main_net_inflow",
                "今日主力净流入-净占比": "main_net_pct",
            }
            df.rename(columns=rename_map, inplace=True)

            if direction == "in":
                df = df[df["main_net_inflow"] > 0].head(limit)
            elif direction == "out":
                df = df[df["main_net_inflow"] < 0].tail(limit).iloc[::-1]
            # else "all": return as-is sorted by eastmoney

            return df.head(limit)
        except Exception:
            return pd.DataFrame()

    # ─────────────────────────────────────────────
    # 东方财富特色筛选
    # ─────────────────────────────────────────────

    def screen_limit_up(self, date: str = None, limit: int = 50) -> pd.DataFrame:
        """
        涨停股池（东方财富）

        Args:
            date: 日期 "YYYYMMDD"，默认今日
        """
        try:
            from datetime import datetime
            if date is None:
                date = datetime.now().strftime("%Y%m%d")
            df = ak.stock_zt_pool_em(date=date)
            if df.empty:
                # 尝试昨天
                from datetime import timedelta
                date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
                df = ak.stock_zt_pool_em(date=date)
            if df.empty:
                return pd.DataFrame()
            rename_map = {
                "代码": "code", "名称": "name", "最新价": "price",
                "涨跌幅": "pct_change", "换手率": "turnover",
                "成交额": "amount", "流通市值": "float_cap",
                "涨停统计": "zt_count", "连板数": "consecutive_limit_up",
                "所属行业": "industry",
            }
            df.rename(columns=rename_map, inplace=True)
            return df.head(limit)
        except Exception:
            return pd.DataFrame()

    def screen_limit_down(self, date: str = None, limit: int = 50) -> pd.DataFrame:
        """跌停股池（东方财富）"""
        try:
            from datetime import datetime
            if date is None:
                date = datetime.now().strftime("%Y%m%d")
            df = ak.stock_zt_pool_strong_em(date=date)
            if df.empty:
                from datetime import timedelta
                date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
                df = ak.stock_zt_pool_strong_em(date=date)
            if df.empty:
                return pd.DataFrame()
            rename_map = {
                "代码": "code", "名称": "name", "最新价": "price",
                "涨跌幅": "pct_change", "换手率": "turnover",
                "成交额": "amount", "流通市值": "float_cap",
            }
            df.rename(columns=rename_map, inplace=True)
            return df.head(limit)
        except Exception:
            return pd.DataFrame()

    def screen_by_comment(
        self,
        sort_by: str = "综合得分",
        ascending: bool = False,
        limit: int = 50
    ) -> pd.DataFrame:
        """
        千股千评筛选（东方财富）

        Args:
            sort_by: 排序字段
                - "机构参与度" 机构关注程度
                - "综合得分" 技术面综合评分
                - "关注指数" 用户关注热度
                - "上升" 关注指数变化
        """
        try:
            df = ak.stock_comment_em()
            rename_map = {
                "代码": "code", "名称": "name", "最新价": "price",
                "涨跌幅": "pct_change", "换手率": "turnover",
                "市盈率": "pe", "主力成本": "main_cost",
                "机构参与度": "institutional", "综合得分": "score",
                "上升": "rise", "目前排名": "rank",
                "关注指数": "follow_index",
            }
            df.rename(columns=rename_map, inplace=True)

            if sort_by in df.columns:
                df = df.sort_values(sort_by, ascending=ascending)
            return df.head(limit)
        except Exception:
            return pd.DataFrame()

    def screen_by_sector_fund_flow(
        self,
        period: Literal["今日", "5日", "10日"] = "今日",
        sector_type: Literal["行业资金流", "概念资金流", "地域资金流"] = "行业资金流",
        top_n: int = 20
    ) -> pd.DataFrame:
        """
        板块资金流排名（东方财富）

        Returns:
            DataFrame: 板块名称、涨跌幅、主力净流入、净流入占比、领涨股
        """
        try:
            df = ak.stock_sector_fund_flow_rank(indicator=period, sector_type=sector_type)
            rename_map = {
                "名称": "sector", f"{period}涨跌幅": "pct_change",
                f"{period}主力净流入-净额": "main_net_inflow",
                f"{period}主力净流入-净占比": "main_net_pct",
                f"{period}主力净流入最大股": "top_stock",
            }
            df.rename(columns=rename_map, inplace=True)
            return df.head(top_n)
        except Exception:
            return pd.DataFrame()

    def screen_hot_sectors(self, limit: int = 20) -> pd.DataFrame:
        """
        热门板块扫描（东方财富概念板块）

        Returns:
            DataFrame: 概念板块名称、涨跌幅、资金净流入、领涨股
        """
        try:
            df = ak.stock_board_concept_name_em()
            rename_map = {
                "板块名称": "sector", "涨跌幅": "pct_change",
                "上涨家数": "rise_count", "下跌家数": "fall_count",
                "领涨股票": "top_stock", "领涨股票-涨跌幅": "top_stock_pct",
            }
            df.rename(columns=rename_map, inplace=True)
            df = df.sort_values("pct_change", ascending=False)
            return df.head(limit)
        except Exception:
            return pd.DataFrame()

    def screen_hot_stocks(
        self,
        # 综合条件
        min_institutional: float = None,
        min_score: float = None,
        min_follow: float = None,
        sort_by: str = "score",
        ascending: bool = False,
        limit: int = 50
    ) -> pd.DataFrame:
        """
        市场关注度筛选（千股千评 + 资金流综合）

        筛选市场关注度高、机构参与强的股票
        """
        try:
            # 千股千评
            comment = ak.stock_comment_em()
            rename_map = {
                "代码": "code", "名称": "name", "最新价": "price",
                "涨跌幅": "pct_change", "换手率": "turnover",
                "机构参与度": "institutional", "综合得分": "score",
                "关注指数": "follow_index",
            }
            comment.rename(columns=rename_map, inplace=True)

            # 资金流
            money = ak.stock_individual_fund_flow_rank(indicator="今日")
            money.rename(columns={
                "代码": "code",
                "今日主力净流入-净额": "main_net_inflow",
                "今日主力净流入-净占比": "main_net_pct",
            }, inplace=True)

            # 合并
            merged = comment.merge(
                money[["code", "main_net_inflow", "main_net_pct"]],
                on="code", how="left"
            )

            # 过滤
            if min_institutional is not None:
                merged = merged[merged["institutional"] >= min_institutional]
            if min_score is not None:
                merged = merged[merged["score"] >= min_score]
            if min_follow is not None:
                merged = merged[merged["follow_index"] >= min_follow]

            if sort_by in merged.columns:
                merged = merged.sort_values(sort_by, ascending=ascending)

            return merged.head(limit)
        except Exception:
            return pd.DataFrame()

    # ─────────────────────────────────────────────
    # 工具方法
    # ─────────────────────────────────────────────

    def format_screening_result(self, df: pd.DataFrame, title: str = "筛选结果") -> str:
        """将筛选结果格式化为 Markdown 表格"""
        if df.empty:
            return "**暂无符合条件的结果**"

        cols_show = ["code", "name", "price", "pct_change", "pe", "pb", "turnover", "float_cap"]
        cols_show = [c for c in cols_show if c in df.columns]

        lines = [
            f"## {title}",
            f"",
            f"| {' | '.join(cols_show)} |",
            f"| {' | '.join(['---'] * len(cols_show))} |",
        ]
        for _, row in df.iterrows():
            vals = []
            for c in cols_show:
                v = row.get(c, "")
                if isinstance(v, float):
                    # float_cap 以亿元显示
                    if c == "float_cap" and abs(v) >= 1e8:
                        v = f"{v/1e8:.2f}亿"
                    else:
                        v = f"{v:.2f}"
                vals.append(str(v))
            lines.append(f"| {' | '.join(vals)} |")

        lines.append(f"")
        lines.append(f"_共 {len(df)} 只符合条件_")
        return "\n".join(lines)
