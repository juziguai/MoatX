"""
stock_data.py - A股数据获取模块
支持：日线、周线、月线、财务数据、资金流向、龙虎榜
"""

import json
import os
import time as _time
import requests
import logging
import threading
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, List, Literal, Optional

from modules.crawler import tencent as _tencent_crawler
from modules.crawler.eastmoney import fetch_stock_info as _em_stock_info
from modules.crawler.fundflow import get_money_flow_summary
from modules.config import cfg
from modules.market_filters import filter_selection_universe
from modules.datasource import QuoteManager
from modules.risk_checker import FinancialRiskChecker
from modules.utils import _clear_all_proxy, to_full_code

_logger = logging.getLogger("moatx.stock_data")
_logger.setLevel(logging.WARNING)


def retry_on_network_error(max_retries: int = 2, base_delay: float = 1.0):
    """指数退避重试装饰器，网络异常时自动重试。"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        requests.exceptions.HTTPError) as e:
                    if attempt == max_retries:
                        _logger.warning("%s 最终失败（已重试 %d 次）: %s",
                                       func.__name__, max_retries, e)
                        raise
                    delay = base_delay * (2 ** attempt)
                    _logger.debug("%s 重试 %d/%d，%.1f秒后: %s",
                                 func.__name__, attempt + 1, max_retries, delay, e)
                    _time.sleep(delay)
        return wrapper
    return decorator


_clear_all_proxy()


def _patch_requests_no_proxy():
    """
    全局 Patch: 让所有 requests 调用都不走系统代理。
    解决 Windows 即使 ProxyEnable=0x0 也在 TCP 层劫持流量的问题。
    """
    _orig_get = requests.get
    _orig_post = requests.post
    _orig_put = requests.put
    _orig_delete = requests.delete
    _orig_session_request = requests.sessions.Session.request

    def _no_proxy_request(self, method, url, **kwargs):
        old = getattr(self, 'trust_env', None)
        self.trust_env = False
        try:
            return _orig_session_request(self, method, url, **kwargs)
        finally:
            if old is not None:
                self.trust_env = old

    def _no_proxy_get(url, **kwargs):
        kwargs.setdefault('timeout', 30)
        return _orig_get(url, **kwargs)

    def _no_proxy_post(url, **kwargs):
        kwargs.setdefault('timeout', 30)
        return _orig_post(url, **kwargs)

    def _no_proxy_put(url, **kwargs):
        kwargs.setdefault('timeout', 30)
        return _orig_put(url, **kwargs)

    def _no_proxy_delete(url, **kwargs):
        kwargs.setdefault('timeout', 30)
        return _orig_delete(url, **kwargs)

    requests.sessions.Session.request = _no_proxy_request
    requests.get = _no_proxy_get
    requests.post = _no_proxy_post
    requests.put = _no_proxy_put
    requests.delete = _no_proxy_delete


_patch_requests_no_proxy()


_REQUESTS_PATCHED = False

class StockData:
    """A股数据获取器"""

    def __init__(self, no_cache: bool = False) -> None:
        _clear_all_proxy()
        self._cache: Dict[str, Any] = {}
        self._cache_lock = threading.RLock()
        self._no_cache = no_cache

    @property
    def timeout(self) -> int:
        """统一 timeout 配置，来自 config.crawler.timeout（默认 10s）"""
        return cfg().crawler.timeout

    # ─────────────────────────────────────────────
    # 全市场实时行情（Sina 为主，30秒 Parquet 缓存）
    # ─────────────────────────────────────────────

    @retry_on_network_error(max_retries=2)
    def get_spot(self, use_cache: bool = True) -> pd.DataFrame:
        """
        获取全市场实时行情（Sina 数据源，并行抓取约 6000+ 条）。
        磁盘缓存 30 秒（Parquet 格式）。
        """
        from modules.crawler import cache as _cache

        if use_cache:
            cached = _cache.read_df_cache("spot_sina", max_age_seconds=cfg().cache.spot_seconds)
            if cached.ok and cached.data is not None:
                return cached.data

        try:
            import concurrent.futures
            session = requests.Session()
            session.trust_env = False
            base_url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"

            def fetch_page(page):
                params = {
                    "page": page, "num": 100, "sort": "symbol",
                    "asc": 1, "node": "hs_a", "_s_r_a": "page",
                }
                r = session.get(base_url, params=params, timeout=self.timeout)
                return r.json()

            all_data = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=cfg().thread_pool.sina_spot_workers) as ex:
                futures = {ex.submit(fetch_page, p): p for p in range(1, 61)}
                for fut in concurrent.futures.as_completed(futures):
                    try:
                        items = fut.result()
                        if items:
                            all_data.extend(items)
                    except Exception as e:
                        _logger.warning("get_spot page %s failed: %s", futures[fut], e)

            if not all_data:
                return pd.DataFrame()
            df = pd.DataFrame(all_data)
            df["code"] = df["symbol"].str[2:]
            keep_cols = {
                "code": "code", "name": "name", "trade": "price",
                "changepercent": "pct_change", "volume": "volume",
                "amount": "amount", "per": "pe", "pb": "pb",
                "turnoverratio": "turnover",
            }
            df = df[[c for c in keep_cols if c in df.columns]].rename(columns=keep_cols)
            for col in ["price", "pct_change", "volume", "amount", "pe", "pb", "turnover"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            _cache.write_df_cache("spot_sina", df, source="sina")
            return df
        except Exception as e:
            _logger.error("get_spot failed: %s", e)
            return pd.DataFrame()

    def get_spot_sina(self, max_pages: int = 60, use_cache: bool = True) -> pd.DataFrame:
        """
        获取全市场实时行情（新浪，备用降级源）。
        已废弃，请使用 get_spot()。
        """
        return self.get_spot(use_cache=use_cache)

    @retry_on_network_error(max_retries=2)
    def get_daily(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adjust: Literal["qfq", "hfq", ""] = "qfq"
    ) -> pd.DataFrame:
        """
        获取日线数据

        Args:
            symbol: 股票代码，如 "600519" 或 "600519.SH"
            start_date: 开始日期 "YYYYMMDD"
            end_date: 结束日期 "YYYYMMDD"
            adjust: 复权类型 qfq=前复权 hfq=后复权 ""=不复权

        Returns:
            DataFrame: date, open, high, low, close, volume, amount, turn
        """
        if symbol.endswith(".SH") or symbol.endswith(".SZ"):
            code = symbol
        else:
            # 自动补充交易所后缀
            code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"

        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        # Check warehouse cache first
        if not self._no_cache:
            try:
                from modules.db import DatabaseManager
                from modules.config import cfg as _cfg
                if _cfg().data.enable_warehouse:
                    db = DatabaseManager(_cfg().data.warehouse_path)
                    cached = db.price().load_daily(symbol, start_date, end_date, adjust)
                    if not cached.empty and cached["date"].max() >= pd.Timestamp(end_date):
                        cached.set_index("date", inplace=True)
                        return cached
            except Exception:
                pass  # fall through to network fetch

        # 主数据源：新浪（akshare），备用：腾讯财经（支持 ETF）
        df = self._get_daily_sina(code, start_date, end_date, adjust)
        if df is None or df.empty:
            df = self._get_daily_tencent(code, start_date, end_date, adjust)
        if df is None or df.empty:
            raise RuntimeError(f"获取日线数据失败 {code}: 所有数据源均不可用")

        # Save to warehouse cache
        if not self._no_cache:
            try:
                from modules.db import DatabaseManager
                from modules.config import cfg as _cfg
                if _cfg().data.enable_warehouse:
                    save_df = df.copy()
                    if "date" not in save_df.columns:
                        save_df = save_df.reset_index()
                    db = DatabaseManager(_cfg().data.warehouse_path)
                    db.price().save_daily_batch(save_df, symbol, adjust)
            except Exception:
                pass

        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        return df

    def _get_daily_tencent(self, code: str, start_date: str, end_date: str, adjust: str) -> Optional[pd.DataFrame]:
        """腾讯财经日线数据（备用源，支持 ETF）"""
        try:
            if code.endswith(".SH"):
                tc_code = f"sh{code.split('.')[0]}"
            elif code.endswith(".SZ"):
                tc_code = f"sz{code.split('.')[0]}"
            else:
                tc_code = f"sh{code}"

            url = (
                f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
                f"?_var=kline_dayqfq&param={tc_code},day,,,{20 if adjust else 20},{'qfq' if adjust == 'qfq' else ''}"
            )
            resp = requests.get(url, timeout=self.timeout)
            text = resp.text
            json_str = text[text.index("=") + 1:]
            data = json.loads(json_str)
            symbol_data = data.get("data", {}).get(tc_code, {})
            days = symbol_data.get("qfqday") or symbol_data.get("day") or []

            if not days:
                return None

            df = pd.DataFrame(
                days,
                columns=["date", "open", "close", "high", "low", "volume"]
            )
            # 腾讯数据量是手（百股），akshare用的是股，统一
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce") * 100
            df["open"] = pd.to_numeric(df["open"], errors="coerce")
            df["high"] = pd.to_numeric(df["high"], errors="coerce")
            df["low"] = pd.to_numeric(df["low"], errors="coerce")
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df["amount"] = 0.0
            df["turn"] = 0.0
            df["turnover"] = 0.0
            # 计算涨跌幅
            df["pct_change"] = df["close"].pct_change() * 100
            df = df[["date", "open", "high", "low", "close", "volume", "amount", "turn", "pct_change", "turnover"]]
            return df
        except Exception:
            return None

    def _get_daily_sina(self, code: str, start_date: str, end_date: str, adjust: str) -> Optional[pd.DataFrame]:
        """新浪财经日线数据（备用源）"""
        try:
            # 新浪需要 sh/sz 前缀
            if code.endswith(".SH"):
                sina_code = f"sh{code.split('.')[0]}"
            elif code.endswith(".SZ"):
                sina_code = f"sz{code.split('.')[0]}"
            else:
                sina_code = f"sh{code}"

            df = ak.stock_zh_a_daily(
                symbol=sina_code,
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
                adjust=adjust if adjust else ""
            )
            if df is None or df.empty:
                return None
            # 新浪接口列名已是英文：date, open, high, low, close, volume, amount, outstanding_share, turnover
            # 补充缺失的 pct_change 和 turn 列（从 close 计算近似）
            if "pct_change" not in df.columns and "close" in df.columns:
                df["pct_change"] = df["close"].pct_change() * 100
            if "turn" not in df.columns:
                df["turn"] = (df["high"] - df["low"]) / df["close"] * 100
            return df
        except Exception:
            return None

    @retry_on_network_error(max_retries=2)
    def get_daily_prices(self, symbols: List[str], count: int = 5) -> Dict[str, Any]:
        """
        批量获取日线收盘价（多股并行，Sina API）
        返回: {symbol: {date: {close, prev_close, pct_change}}}
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def fetch_one(code):
            prefix = "sh" if code.startswith(("6", "5", "9")) else "sz"
            url = (f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php"
                   f"/CN_MarketData.getKLineData?symbol={prefix}{code}&scale=240&ma=no&datalen={count}")
            r = requests.get(url, timeout=self.timeout)
            r.encoding = "utf-8"
            return code, r.json()

        results = {}
        with ThreadPoolExecutor(max_workers=min(len(symbols), 20)) as executor:
            futures = {executor.submit(fetch_one, s): s for s in symbols}
            for future in as_completed(futures):
                try:
                    code, data = future.result()
                except Exception as e:
                    _logger.warning("get_daily_prices fetch failed for %s: %s", futures[future], e)
                    continue
                if not data:
                    continue
                recs = {}
                for i, rec in enumerate(data):
                    try:
                        close = float(rec["close"])
                        day = rec["day"]
                    except (KeyError, ValueError, TypeError):
                        _logger.warning("get_daily_prices 跳过畸形记录 %s: %s", code, rec)
                        continue
                    prev = data[i - 1] if i > 0 else rec
                    try:
                        prev_close = float(prev["close"])
                    except (KeyError, ValueError, TypeError):
                        prev_close = close
                    pct = (close - prev_close) / prev_close * 100 if prev_close else 0
                    recs[day] = {"close": close, "prev_close": prev_close, "pct_change": pct}
                results[code] = recs
        return results

    @retry_on_network_error(max_retries=2)
    def get_weekly(self, symbol: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取周线数据（腾讯财经）"""
        if symbol.endswith(".SH") or symbol.endswith(".SZ"):
            code = symbol
        else:
            code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"

        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365 * 3)).strftime("%Y%m%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        try:
            if code.endswith(".SH"):
                tc_code = f"sh{code.split('.')[0]}"
            elif code.endswith(".SZ"):
                tc_code = f"sz{code.split('.')[0]}"
            else:
                tc_code = f"sh{code}"

            url = (
                f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
                f"?_var=kline_weekqfq&param={tc_code},week,,,100,qfq"
            )
            resp = requests.get(url, timeout=self.timeout)
            text = resp.text
            json_str = text[text.index("=") + 1:]
            data = json.loads(json_str)
            symbol_data = data.get("data", {}).get(tc_code, {})
            weeks = symbol_data.get("qfqweek") or symbol_data.get("week") or []

            if not weeks:
                raise RuntimeError("腾讯周线无数据")

            df = pd.DataFrame(
                weeks,
                columns=["date", "open", "close", "high", "low", "volume"]
            )
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce") * 100
            for col in ["open", "close", "high", "low"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["pct_change"] = df["close"].pct_change() * 100

            df["date"] = pd.to_datetime(df["date"])
            start = pd.to_datetime(start_date.replace("-", ""))
            end = pd.to_datetime(end_date.replace("-", ""))
            df = df[(df["date"] >= start) & (df["date"] <= end)]
            df.set_index("date", inplace=True)
            return df
        except Exception as e:
            raise RuntimeError(f"获取周线数据失败 {code}: {e}")

    def get_realtime_quote(self, symbol: str, source: str | None = None, mode: str | None = None) -> Dict[str, Any]:
        """
        获取实时行情（单股，多源交叉验证）。
        返回：当前价、涨跌幅、成交量、换手率、校验状态等。
        """
        quotes = self.get_realtime_quotes([symbol], source=source, mode=mode)
        full_code = to_full_code(symbol)
        quote = quotes.get(full_code)
        if not quote:
            raise RuntimeError(f"获取实时行情失败 {symbol}: 所有数据源均不可用")
        return quote

    @retry_on_network_error(max_retries=2)
    def get_realtime_quotes(
        self,
        symbols: List[str],
        source: str | None = None,
        mode: str | None = None,
    ) -> Dict[str, Any]:
        """
        批量获取实时行情。
        默认按 [datasource] 配置查询；可用 source 指定单一数据源，或用
        mode=single/validate 覆盖配置模式。
        """
        source_names = [source] if source else None
        return QuoteManager(source_names=source_names, mode=mode).fetch_quotes(symbols)

    def get_money_flow(self, symbol: str) -> Dict[str, Any]:
        """
        获取个股资金流向（EastMoney 数据中心，走 akshare stock_individual_fund_flow）
        """
        try:
            return get_money_flow_summary(symbol, use_cache=True)
        except Exception as exc:
            _logger.warning("get_money_flow(%s) failed: %s", symbol, exc)
            return {
                "date": "",
                "main_net_inflow": 0,
                "main_net_inflow_pct": 0,
                "super_large_net": 0,
                "large_net": 0,
                "medium_net": 0,
                "small_net": 0,
                "_note": f"资金流向数据不可用: {exc}",
            }

    def get_sector_flow(self, limit: int = 10) -> pd.DataFrame:
        """
        获取板块资金流排名（使用 THS 行业板块数据作为代理）
        """
        try:
            from modules.crawler import sector
            result = sector.get_industry_boards(use_cache=True)
            if result.ok and result.data is not None:
                return result.data.head(limit)
        except Exception as exc:
            _logger.warning("get_sector_flow failed: %s", exc)
        return pd.DataFrame()

    def get_limit_up(self) -> pd.DataFrame:
        """
        获取今日涨停股（Sina 快照 + Tencent 验证）

        两步法：
        1. 从 Sina 快照按 pct_change 筛选候选
        2. 用 Tencent 的 limit_up/limit_down 字段验证
        """
        try:
            spot = self.get_spot()
            if spot.empty:
                return pd.DataFrame()
            candidates = spot[spot["pct_change"] >= 9.0].copy()
            candidates = filter_selection_universe(candidates, code_col="code")
            if candidates.empty:
                return pd.DataFrame()
            return self._verify_limit(candidates, direction="up")
        except Exception as exc:
            _logger.warning("get_limit_up failed: %s", exc)
            return pd.DataFrame()

    def get_limit_down(self) -> pd.DataFrame:
        """获取今日跌停股（Sina 快照 + Tencent 验证）"""
        try:
            spot = self.get_spot()
            if spot.empty:
                return pd.DataFrame()
            candidates = spot[spot["pct_change"] <= -9.0].copy()
            candidates = filter_selection_universe(candidates, code_col="code")
            if candidates.empty:
                return pd.DataFrame()
            return self._verify_limit(candidates, direction="down")
        except Exception as exc:
            _logger.warning("get_limit_down failed: %s", exc)
            return pd.DataFrame()

    def _verify_limit(self, candidates: pd.DataFrame, direction: str = "up") -> pd.DataFrame:
        """用 Tencent 验证候选股是否真实涨停/跌停"""
        import requests
        codes = candidates["code"].tolist()
        # Build Tencent query directly (bypass cache to avoid long cache key)
        from modules.crawler.tencent import _build_query, _parse_tencent_response, BASE_URL
        query = _build_query(codes)
        try:
            session = requests.Session()
            session.trust_env = False
            session.proxies = {"http": None, "https": None}
            r = session.get(BASE_URL, params={"q": query}, timeout=self.timeout)
            r.encoding = "utf-8"
            if r.status_code != 200:
                return pd.DataFrame()
            quotes = _parse_tencent_response(r.text)
        except Exception:
            return pd.DataFrame()

        verified = []
        for q in quotes:
            price = q.get("price")
            limit_price = q.get("limit_up" if direction == "up" else "limit_down")
            if price and limit_price and abs(price - limit_price) < 0.05:
                verified.append({
                    "code": q.get("code"),
                    "name": q.get("name"),
                    "price": price,
                    "limit_price": limit_price,
                    "pct_change": q.get("pct_change"),
                    "amount": q.get("amount"),
                    "turnover": q.get("turnover"),
                })
        if not verified:
            return pd.DataFrame()
        return pd.DataFrame(verified).head(100)

    def get_stock_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取股票基本信息（东方财富数据中心）
        """
        result = _em_stock_info(symbol, use_cache=True)
        if not result.ok or result.data is None:
            raise RuntimeError(f"获取股票信息失败 {symbol}: {result.user_message}")
        return result.data

    def get_valuation(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """
        获取估值数据（PE/PB/ROE）
        通过同花顺财务报告计算最新估值

        Args:
            symbol: 股票代码
            current_price: 当前股价（用于计算PE/PB）

        Returns:
            dict: pe, pb, roe, eps, bvps, report_date
        """
        try:
            if not symbol.endswith((".SH", ".SZ")):
                code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"
            else:
                code = symbol

            df = ak.stock_financial_abstract_ths(symbol=code.split(".")[0])
            if df is None or df.empty:
                return None
            # 取最新一期财报
            latest = df.iloc[-1]

            eps = self._parse_number(latest.get("基本每股收益", 0))
            bvps = self._parse_number(latest.get("每股净资产", 0))
            roe_str = latest.get("净资产收益率", "0%")
            roe = float(str(roe_str).replace("%", "")) if roe_str and roe_str != "False" else 0
            report_date = str(latest.get("报告期", ""))

            pe = round(current_price / eps, 2) if eps and eps > 0 else None
            pb = round(current_price / bvps, 2) if bvps and bvps > 0 else None

            return {
                "pe": pe,
                "pb": pb,
                "roe": roe,
                "eps": eps,
                "bvps": bvps,
                "report_date": report_date
            }
        except Exception as e:
            return {"pe": None, "pb": None, "roe": None, "eps": None, "bvps": None, "report_date": "", "error": str(e)}

    @staticmethod
    def _parse_number(val) -> float:
        """解析各种格式的数字字符串"""
        if val is None or val == "False" or val == "":
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        if s == "False" or s == "":
            return 0.0
        # 处理亿、万等单位
        if "亿" in s:
            return float(s.replace("亿", "")) * 1e8
        if "万" in s:
            return float(s.replace("万", "")) * 1e4
        try:
            return float(s)
        except Exception:
            return 0.0

    def get_dividend(self, symbol: str) -> List[Dict[str, Any]]:
        """
        获取历史分红记录（巨潮）
        Returns: 近5年分红记录列表
        """
        try:
            if not symbol.endswith((".SH", ".SZ")):
                code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"
            else:
                code = symbol

            df = ak.stock_dividend_cninfo(symbol=code.split(".")[0])
            # 取最新5条
            recent = df.head(5)
            results = []
            for _, row in recent.iterrows():
                dividend = row.get("派息比例", 0)
                if dividend and dividend != "NaN":
                    try:
                        amount = float(str(dividend).replace("元", "").replace("含税", ""))
                    except Exception:
                        amount = 0
                else:
                    amount = 0
                results.append({
                    "date": str(row.get("实施方案公告日期", "")),
                    "type": str(row.get("分红类型", "")),
                    "dividend_per_share": amount,
                    "record_date": str(row.get("股权登记日", "")),
                    "ex_date": str(row.get("除权日", "")),
                    "pay_date": str(row.get("派息日", "")),
                    "description": str(row.get("实施方案分红说明", "")),
                })
            return results
        except Exception as e:
            return [{"error": str(e)}]

    def get_profit_forecast(self, symbol: str) -> Dict[str, Any]:
        """
        获取券商盈利预测（同花顺）
        Returns: 未来2-3年EPS预测（均值）
        """
        try:
            if not symbol.endswith((".SH", ".SZ")):
                code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"
            else:
                code = symbol

            df = ak.stock_profit_forecast_ths(symbol=code.split(".")[0])
            forecasts = []
            for _, row in df.iterrows():
                year = str(row.get("年度", ""))
                avg_eps = self._parse_number(row.get("均值", 0))
                num_firms = int(row.get("预测机构数", 0))
                if avg_eps > 0:
                    forecasts.append({
                        "year": year,
                        "avg_eps": avg_eps,
                        "min_eps": self._parse_number(row.get("最小值", 0)),
                        "max_eps": self._parse_number(row.get("最大值", 0)),
                        "num_firms": num_firms,
                    })
            return {"forecasts": forecasts}
        except Exception as e:
            return {"forecasts": [], "error": str(e)}

    def get_major_shareholders(self, symbol: str) -> List[Dict[str, Any]]:
        """
        获取前10大股东
        Returns: 股东名称、持股比例、股本性质
        """
        try:
            if not symbol.endswith((".SH", ".SZ")):
                code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"
            else:
                code = symbol

            df = ak.stock_main_stock_holder(stock=code.split(".")[0])
            holders = []
            for _, row in df.head(10).iterrows():
                pct = row.get("持股比例")
                try:
                    pct_val = float(pct) if pct else 0
                except Exception:
                    pct_val = 0
                holders.append({
                    "name": str(row.get("股东名称", "")),
                    "pct": pct_val,
                    "nature": str(row.get("股本性质", "")),
                    "share_count": row.get("持股数量"),
                    "截止日期": str(row.get("截至日期", "")),
                })
            return holders
        except Exception as e:
            return [{"error": str(e)}]

    def get_shareholder_changes(self, symbol: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        获取股东增减持变化（近5次）
        """
        try:
            if not symbol.endswith((".SH", ".SZ")):
                code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"
            else:
                code = symbol

            df = ak.stock_shareholder_change_ths(symbol=code.split(".")[0])
            changes = []
            for _, row in df.head(limit).iterrows():
                changes.append({
                    "date": str(row.get("公告日期", "")),
                    "holder": str(row.get("变动股东", "")),
                    "change_shares": row.get("变动数量", ""),
                    "avg_price": row.get("交易均价", ""),
                    "period": str(row.get("变动期间", "")),
                    "method": str(row.get("变动途径", "")),
                })
            return changes
        except Exception as e:
            return [{"error": str(e)}]

    def get_profit_sheet_summary(self, symbol: str) -> Dict[str, Any]:
        """
        获取利润表核心指标摘要（最新一期）
        营业收入、净利润、毛利率、净利率
        """
        try:
            if not symbol.endswith((".SH", ".SZ")):
                code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"
            else:
                code = symbol

            df = ak.stock_profit_sheet_by_report_em(symbol=code)
            if df is None or df.empty:
                return {"error": "no data"}
            latest = df.iloc[0]  # 最新一期在前面

            revenue = self._parse_number(latest.get("营业总收入", 0))
            netprofit = self._parse_number(latest.get("净利润", 0))
            # 毛利率 = (营收 - 营业成本) / 营收
            operate_cost = self._parse_number(latest.get("营业成本", 0))
            gross_margin = (revenue - operate_cost) / revenue * 100 if revenue > 0 else 0
            net_margin = netprofit / revenue * 100 if revenue > 0 else 0

            return {
                "report_date": str(latest.get("REPORT_DATE", "")),
                "revenue": revenue,
                "net_profit": netprofit,
                "gross_margin": round(gross_margin, 2),
                "net_margin": round(net_margin, 2),
                "basic_eps": self._parse_number(latest.get("BASIC_EPS", 0)),
            }
        except Exception as e:
            return {"error": str(e)}

    def get_cash_flow_summary(self, symbol: str) -> Dict[str, Any]:
        """
        获取现金流量表核心指标摘要（最新一期）
        经营现金流、投资现金流、筹资现金流、期末现金
        """
        try:
            if not symbol.endswith((".SH", ".SZ")):
                code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"
            else:
                code = symbol

            df = ak.stock_financial_cash_ths(symbol=code.split(".")[0], indicator="按报告期")
            if df is None or df.empty:
                return {"error": "no data"}
            latest = df.iloc[0]

            op_cf = self._parse_number(latest.get("经营活动产生的现金流量净额", 0))
            inv_cf = self._parse_number(latest.get("投资活动产生的现金流量净额", 0))
            fin_cf = self._parse_number(latest.get("筹资活动产生的现金流量净额", 0))
            cash_end = self._parse_number(latest.get("期末现金及现金等价物余额", 0))

            return {
                "report_date": str(latest.get("报告期", "")),
                "operating_cf": op_cf,
                "investing_cf": inv_cf,
                "financing_cf": fin_cf,
                "cash_end": cash_end,
                "free_cf": op_cf + inv_cf,  # 自由现金流近似
            }
        except Exception as e:
            return {"error": str(e)}

    # ─── 财务风险检测（委托给 FinancialRiskChecker） ─────────────────

    @retry_on_network_error(max_retries=2)
    def check_financial_risk(self, symbol: str) -> Dict[str, Any]:
        """综合财务风险检测（委托给 FinancialRiskChecker）。"""
        checker = FinancialRiskChecker(self)
        return checker.check_financial_risk(symbol)
