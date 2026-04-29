"""
screener.py - MoatX 选股器
两阶段筛选：全市场快照 → 本地技术/基本面过滤
"""

import pandas as pd
import logging
from typing import Optional, Literal, Tuple

from modules.config import cfg
from modules.market_filters import filter_selection_universe

_logger = logging.getLogger(__name__)


class MoatXScreener:
    """MoatX 选股器"""

    def __init__(self) -> None:
        from modules.stock_data import StockData
        self._sd = StockData()
        self._spot_cache = None
        self._spot_cache_time = None

    # ─────────────────────────────────────────────
    # 全市场快照（Sina 数据源，带60秒内存缓存）
    # ─────────────────────────────────────────────

    def get_spot(self) -> pd.DataFrame:
        """获取全市场实时行情（Sina 数据源，带60秒内存缓存）"""
        import time
        now = time.time()
        if self._spot_cache is not None and (now - self._spot_cache_time) < 15:
            return self._spot_cache
        self._spot_cache = self._sd.get_spot()
        self._spot_cache_time = now
        return self._spot_cache

    # ─────────────────────────────────────────────
    # 全市场扫描（本地过滤，无需候选池）
    # ─────────────────────────────────────────────

    def scan_all(
        self,
        # 基本面
        pe_range: Tuple[float, float] = (0, 50),
        pb_range: Tuple[float, float] = (0, 10),
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

        # 列名标准化（兼容东方财富和新浪）
        if "代码" in df.columns:
            df.rename(columns={
                "代码": "code", "名称": "name", "最新价": "price",
                "涨跌幅": "pct_change", "涨跌额": "change", "成交量": "volume",
                "成交额": "amount", "换手率": "turnover", "市盈率-动态": "pe",
                "市净率": "pb", "总市值": "total_cap", "流通市值": "float_cap",
            }, inplace=True)

        # 检测价格数据是否有效（非交易时段 Sina 全为 0）
        price_valid = df["price"].notna().sum() > 0 and df["price"].abs().sum() > 0
        turnover_valid = df["turnover"].notna().sum() > 0 and df["turnover"].abs().sum() > 0
        df = filter_selection_universe(df, code_col="code")

        # 过滤
        if pe_range:
            df = df[(df["pe"] >= pe_range[0]) & (df["pe"] <= pe_range[1])]
        if pb_range:
            df = df[(df["pb"] >= pb_range[0]) & (df["pb"] <= pb_range[1])]

        if cap_min is not None and "float_cap" in df.columns:
            df = df[df["float_cap"] >= cap_min]
        if cap_max is not None and "float_cap" in df.columns:
            df = df[df["float_cap"] <= cap_max]
        # 换手率过滤：仅在换手率数据有效时生效
        if turnover_min is not None and turnover_valid:
            df = df[df["turnover"] >= turnover_min]

        # 涨跌幅过滤：仅在价格数据有效时生效
        if price_valid:
            if pct_change_min is not None:
                df = df[df["pct_change"] >= pct_change_min]
            if pct_change_max is not None:
                df = df[df["pct_change"] <= pct_change_max]

        if volume_ratio_min is not None and "volume_ratio" in df.columns:
            df = df[df["volume_ratio"] >= volume_ratio_min]

        # 排序：翻译常见中文列名，并降级到有效列
        sort_col_map = {"涨跌幅": "pct_change", "换手率": "turnover", "市盈率": "pe", "市净率": "pb"}
        effective_sort = sort_col_map.get(sort_by, sort_by)

        if effective_sort in df.columns:
            df = df.sort_values(effective_sort, ascending=ascending)
        elif "pct_change" in df.columns:
            df = df.sort_values("pct_change", ascending=ascending)
        elif "pe" in df.columns:
            df = df.sort_values("pe", ascending=True)

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
        """扫描指定行业板块内个股（THS 数据源）"""
        try:
            import akshare as ak
            df = ak.stock_board_industry_cons_ths(symbol=industry_name)
            if df is None or df.empty:
                return pd.DataFrame()
            rename_map = {
                "代码": "code", "名称": "name", "最新价": "price",
                "涨跌幅": "pct_change", "涨跌额": "change",
                "成交量": "volume", "成交额": "amount",
                "振幅": "amplitude", "最高": "high", "最低": "low",
                "今开": "open", "昨收": "prev_close",
                "量比": "volume_ratio", "换手率": "turnover",
                "市盈率-动态": "pe", "市净率": "pb",
            }
            df.rename(columns=rename_map, inplace=True)
            df = filter_selection_universe(df, code_col="code")

            if conditions:
                if "pe_range" in conditions:
                    df = df[(df["pe"] >= conditions["pe_range"][0]) &
                            (df["pe"] <= conditions["pe_range"][1])]
                if "pb_range" in conditions:
                    df = df[(df["pb"] >= conditions["pb_range"][0]) &
                            (df["pb"] <= conditions["pb_range"][1])]

            return df.head(top_n)
        except Exception as e:
            _logger.warning("scan_industry(%s) failed: %s", industry_name, e)
            return pd.DataFrame()

    def scan_all_industries(self, limit_per: int = 3) -> pd.DataFrame:
        """扫描所有行业板块（THS），每个板块取前 limit_per 只"""
        try:
            import akshare as ak
            industries = ak.stock_board_industry_name_ths()
            if industries is None or industries.empty:
                return pd.DataFrame()
            results = []
            for _, row in industries.iterrows():
                name = row.get("板块名称", "")
                if not name:
                    continue
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
        except Exception as e:
            _logger.warning("scan_all_industries failed: %s", e)
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
        """资金流向排名（使用 THS 行业板块数据作为代理）"""
        from modules.crawler.fundflow import get_money_flow_rank
        return get_money_flow_rank(limit=limit, use_cache=True)

    # ─────────────────────────────────────────────
    # 东方财富特色筛选（已降级）
    # ─────────────────────────────────────────────

    def screen_limit_up(self, date: str = None, limit: int = 50) -> pd.DataFrame:
        """涨停股池（Sina + Tencent 验证）"""
        try:
            df = self._sd.get_limit_up()
            df = filter_selection_universe(df, code_col="code")
            return df.head(limit) if not df.empty else df
        except Exception as e:
            _logger.warning("screen_limit_up failed: %s", e)
            return pd.DataFrame()

    def screen_limit_down(self, date: str = None, limit: int = 50) -> pd.DataFrame:
        """跌停股池（Sina + Tencent 验证）"""
        try:
            df = self._sd.get_limit_down()
            df = filter_selection_universe(df, code_col="code")
            return df.head(limit) if not df.empty else df
        except Exception as e:
            _logger.warning("screen_limit_down failed: %s", e)
            return pd.DataFrame()

    def screen_by_comment(
        self,
        sort_by: str = "综合得分",
        ascending: bool = False,
        limit: int = 50
    ) -> pd.DataFrame:
        """千股千评筛选（不再可用 — 原东方财富 push2 接口被封）"""
        _logger.warning("screen_by_comment: 千股千评数据不可用（东方财富 push2 被封）")
        return pd.DataFrame()

    def screen_by_sector_fund_flow(
        self,
        period: Literal["今日", "5日", "10日"] = "今日",
        sector_type: Literal["行业资金流", "概念资金流", "地域资金流"] = "行业资金流",
        top_n: int = 20
    ) -> pd.DataFrame:
        """板块资金流排名（不再可用 — 原东方财富 push2 接口被封）"""
        _logger.warning("screen_by_sector_fund_flow: 板块资金流数据不可用（东方财富 push2 被封）")
        return pd.DataFrame()

    def screen_hot_sectors(self, limit: int = 20) -> pd.DataFrame:
        """
        热门板块扫描（概念板块，THS 数据源）

        Returns:
            DataFrame: 概念板块名称、涨跌幅、资金净流入、领涨股
        """
        from modules.crawler import sector
        result = sector.get_concept_boards(use_cache=True)
        if not result.ok:
            _logger.warning("screen_hot_sectors failed: %s %s", result.error, result.error_detail)
            return pd.DataFrame()
        return result.data.sort_values("pct_change", ascending=False).head(limit)

    def screen_boards_by_pct_change(
        self,
        min_pct: float = 50,
        board_types: tuple[str, ...] = ("行业", "概念"),
        limit: int = 50,
    ) -> pd.DataFrame:
        """筛选板块涨跌幅。min_pct 单位为百分比点，50 表示 50%。"""
        from modules.crawler import sector
        result = sector.filter_boards_by_pct_change(
            min_pct=min_pct,
            board_types=board_types,
            use_cache=True,
        )
        if not result.ok:
            _logger.warning("screen_boards_by_pct_change failed: %s %s", result.error, result.error_detail)
            return pd.DataFrame()
        return result.data.head(limit)

    def screen_hot_stocks(
        self,
        min_institutional: float = None,
        min_score: float = None,
        min_follow: float = None,
        sort_by: str = "score",
        ascending: bool = False,
        limit: int = 50
    ) -> pd.DataFrame:
        """
        市场关注度筛选（不再可用 — 原东方财富千股千评/资金流接口被封）
        """
        _logger.warning("screen_hot_stocks: 千股千评+资金流数据不可用（东方财富 push2 被封）")
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
            "",
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

        lines.append("")
        lines.append(f"_共 {len(df)} 只符合条件_")
        return "\n".join(lines)

    def filter_by_financial_risk(self, symbols: list, max_risk: int = 40) -> dict:
        """
        批量过滤高财务风险股票（并行检测）
        Args:
            symbols: 股票代码列表
            max_risk: 风险评分上限（默认40分），超过此分数的股票将被过滤
        Returns:
            {"pass": [list of safe symbols], "fail": {symbol: risk_info}}
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from modules.stock_data import StockData

        sd = StockData()
        passed = []
        failed = {}

        def check_one(sym):
            try:
                risk = sd.check_financial_risk(sym)
                return sym, risk
            except Exception:
                return sym, {"risk_score": 0, "risk_level": "检测失败", "is_buyable": True, "red_flags": [], "warnings": []}

        with ThreadPoolExecutor(max_workers=min(len(symbols), cfg().thread_pool.risk_check_workers)) as executor:
            futures = {executor.submit(check_one, s): s for s in symbols}
            for future in as_completed(futures):
                sym, risk = future.result()
                if risk["risk_score"] >= max_risk or not risk["is_buyable"]:
                    failed[sym] = risk
                else:
                    passed.append(sym)

        return {"pass": passed, "fail": failed}
