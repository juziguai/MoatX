"""
risk_checker.py - 财务风险检测模块
从 stock_data.py 拆分出来，职责单一：财务风险评估。
"""

from __future__ import annotations

import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

import akshare as ak
import requests

from modules.config import cfg

_logger = logging.getLogger("moatx.risk_checker")


class FinancialRiskChecker:
    """
    A股财务风险检测器。

    检测维度：ST风险、业绩雷、公告雷、债务雷、披露延迟。
    所有子检查并行执行，单检查超时由 cfg().crawler.timeout 控制。
    """

    def __init__(self, stock_data: Any) -> None:
        """
        Args:
            stock_data: StockData 实例，用于获取个股基本信息。
        """
        self._sd = stock_data
        self._timeout = cfg().crawler.timeout

    def check_financial_risk(self, symbol: str) -> dict[str, Any]:
        """
        综合财务风险检测，返回风险报告。

        Returns:
            dict: {
                symbol, risk_score (0-100), risk_level (str),
                red_flags (list), warnings (list), is_buyable (bool)
            }
        """
        risk_score = 0
        red_flags: list[str] = []
        warnings: list[str] = []

        with ThreadPoolExecutor(max_workers=cfg().thread_pool.financial_risk_workers) as executor:
            futures = {
                executor.submit(self._check_st_status, symbol): "st",
                executor.submit(self._check_earnings_forecast, symbol): "forecast",
                executor.submit(self._check_risk_notices, symbol): "notices",
                executor.submit(self._check_disclosure_delay, symbol): "disclosure",
                executor.submit(self._check_debt_ratio, symbol): "debt",
            }
            for fut in as_completed(futures, timeout=self._timeout + 1):
                name = futures[fut]
                try:
                    result = fut.result(timeout=self._timeout)
                except Exception:
                    result = {"score": 0, "warnings": [], "red_flags": []}

                if name == "st":
                    if result.get("is_st"):
                        risk_score += 40
                        red_flags.append(f"ST状态: {result.get('type')}")
                elif name == "forecast":
                    risk_score += result.get("score", 0)
                    warnings.extend(result.get("warnings", []))
                elif name == "notices":
                    risk_score += result.get("score", 0)
                    red_flags.extend(result.get("red_flags", []))
                elif name == "disclosure":
                    risk_score += result.get("score", 0)
                    warnings.extend(result.get("warnings", []))
                elif name == "debt":
                    risk_score += result.get("score", 0)
                    warnings.extend(result.get("warnings", []))

        risk_score = min(100, risk_score)

        return {
            "symbol": symbol,
            "risk_score": risk_score,
            "risk_level": self._risk_level(risk_score),
            "red_flags": red_flags,
            "warnings": warnings,
            "is_buyable": risk_score < 30,
        }

    def _risk_level(self, score: int) -> str:
        if score >= 70:
            return "极高风险"
        if score >= 50:
            return "高风险"
        if score >= 30:
            return "中等风险"
        if score >= 15:
            return "低风险"
        return "基本无风险"

    def _check_st_status(self, symbol: str) -> dict[str, Any]:
        try:
            info = self._sd.get_stock_info(symbol)
            name = info.get("name", "") if info else ""
            is_st = "ST" in name or "*ST" in name or name.startswith("S")
            st_type = None
            if "*ST" in name:
                st_type = "退市风险警示"
            elif "ST" in name:
                st_type = "特别处理"
            return {"is_st": is_st, "type": st_type, "name": name}
        except Exception:
            return {"is_st": False, "type": None, "name": symbol}

    def _check_earnings_forecast(self, symbol: str) -> dict[str, Any]:
        score = 0
        warnings: list[str] = []
        try:
            code = symbol if symbol.endswith((".SH", ".SZ")) else (
                f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"
            )
            stock = code.lower().replace(".", "")
            df = ak.stock_financial_report_sina(stock=stock, symbol="利润表")
            if df is None or df.empty:
                return {"score": 0, "warnings": []}
            df = df.head(2)
            net_profit_col = "归属于母公司所有者的净利润"
            if net_profit_col not in df.columns:
                net_profit_col = "净利润"
            for i, row in df.iterrows():
                report_date = str(row.get("报告日", ""))
                profit = self._parse_number(row.get(net_profit_col, 0))
                if profit < 0:
                    season = "最近季度" if i == 0 else "上一季度"
                    warnings.append(f"{season}亏损 {report_date}: {profit:,.0f}")
                    score += 15
                    if i == 0:
                        break
            if len(warnings) >= 2:
                score = min(score + 10, 35)
                warnings.append("连续季度亏损（业绩持续恶化）")
        except Exception as e:
            _logger.warning("_check_earnings_forecast [%s] failed: %s", symbol, e)
        return {"score": min(score, 35), "warnings": warnings}

    def _check_risk_notices(self, symbol: str, lookback_days: int = 10) -> dict[str, Any]:
        score = 0
        red_flags: list[str] = []
        try:
            keywords = sorted([
                ("可能被实施退市风险警示", 20),
                ("业绩预告更正", 15),
                ("终止上市", 25),
                ("无法表示意见", 20), ("否定意见", 25),
                ("审计发现问题", 15), ("会计差错", 10), ("虚增收入", 25),
                ("信披违规", 15), ("立案调查", 20), ("监管函", 5),
            ], key=lambda x: len(x[0]), reverse=True)
            found: set[str] = set()
            end_date = datetime.now()
            start_date = (end_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")

            session = requests.Session()
            session.trust_env = False
            code = symbol.split(".")[0]
            url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
            payload = {
                "pageNum": "1",
                "pageSize": "30",
                "column": "szse",
                "tabName": "fulltext",
                "plate": "",
                "stock": "",
                "searchkey": code,
                "secid": "",
                "category": "",
                "trade": "",
                "seDate": f"{start_date}~{end_str}",
                "sortName": "",
                "sortType": "",
                "isHLtitle": "true",
            }
            r = session.post(url, data=payload, timeout=self._timeout)
            data = r.json()
            notices = data.get("announcements") or []
            seen_titles: set[str] = set()
            for item in notices:
                title = str(item.get("announcementTitle", ""))
                title_clean = re.sub(r"<[^>]+>", "", title)
                if not title_clean or title_clean in seen_titles:
                    continue
                seen_titles.add(title_clean)
                ts = item.get("announcementTime", 0)
                if ts:
                    notice_date = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                else:
                    notice_date = ""
                matched_pts = 0
                matched_kw = ""
                for kw, pts in keywords:
                    if kw in title_clean and kw not in found:
                        if pts > matched_pts:
                            matched_pts = pts
                            matched_kw = kw
                if matched_pts > 0:
                    score += matched_pts
                    found.add(matched_kw)
                    red_flags.append(f"[{notice_date}] {title_clean[:60]}")
        except Exception as e:
            _logger.warning("_check_risk_notices [%s] failed: %s", symbol, e)
        return {"score": min(score, 40), "red_flags": red_flags}

    def _check_disclosure_delay(self, symbol: str) -> dict[str, Any]:
        score = 0
        warnings: list[str] = []
        try:
            stock_code = symbol.split(".")[0]
            df = ak.stock_financial_report_sina(stock=stock_code, symbol="利润表")
            if df is None or df.empty:
                return {"score": 0, "warnings": []}
            latest = df.iloc[0]
            revenue = self._parse_number(latest.get("营业总收入", 0) or latest.get("营业收入", 0))
            net_profit = self._parse_number(latest.get("净利润", 0))
            if revenue > 0 and revenue < 3_0000_0000 and net_profit < 0:
                score += 20
                warnings.append(f"触及ST规则（营收{revenue/1e8:.1f}亿 + 亏损）")
        except Exception as e:
            _logger.warning("_check_disclosure_delay [%s] failed: %s", symbol, e)
        return {"score": min(score, 20), "warnings": warnings}

    def _check_debt_ratio(self, symbol: str, warn_threshold: float = 0.85) -> dict[str, Any]:
        score = 0
        warnings: list[str] = []
        try:
            stock_code = symbol.split(".")[0]
            df = ak.stock_financial_report_sina(stock=stock_code, symbol="资产负债表")
            if df is None or df.empty:
                return {"score": 0, "warnings": []}
            latest = df.iloc[0]
            total_debt = self._parse_number(latest.get("负债合计", 0))
            total_assets = self._parse_number(latest.get("资产总计", 0))
            if total_assets > 0:
                debt_ratio = total_debt / total_assets
                if debt_ratio > 1.0:
                    score += 25
                    warnings.append(f"资产负债率>{debt_ratio*100:.1f}%，资不抵债")
                elif debt_ratio > warn_threshold:
                    score += 10
                    warnings.append(f"资产负债率偏高（{debt_ratio*100:.1f}%）")
        except Exception as e:
            _logger.warning("_check_debt_ratio [%s] failed: %s", symbol, e)
        return {"score": min(score, 25), "warnings": warnings}

    @staticmethod
    def _parse_number(val: Any) -> float:
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            val = val.strip().replace(",", "").replace(" ", "")
            try:
                return float(val)
            except ValueError:
                return 0.0
        return 0.0
