"""
stock_data.py - A股数据获取模块
支持：日线、周线、月线、财务数据、资金流向、龙虎榜
"""

import os
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Literal, Optional


class StockData:
    """A股数据获取器"""

    def __init__(self):
        self._clear_proxy()
        self.cache = {}

    @staticmethod
    def _clear_proxy():
        """清除代理环境变量，避免网络请求被系统代理拦截"""
        for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
                    "HTTP_PROXY", "HTTPS_PROXY"]:
            os.environ.pop(key, None)

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

        # 主数据源：东方财富（push2his），备用：新浪
        df = self._get_daily_eastmoney(code, start_date, end_date, adjust)
        if df is None or df.empty:
            df = self._get_daily_sina(code, start_date, end_date, adjust)
        if df is None or df.empty:
            raise RuntimeError(f"获取日线数据失败 {code}: 所有数据源均不可用")

        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        return df

    def _get_daily_eastmoney(self, code: str, start_date: str, end_date: str, adjust: str) -> Optional[pd.DataFrame]:
        """东方财富日线数据"""
        try:
            df = ak.stock_zh_a_hist(
                symbol=code.split(".")[0],
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            df.columns = [c.lower() for c in df.columns]
            df.rename(columns={
                "日期": "date", "开盘": "open", "收盘": "close",
                "最高": "high", "最低": "low", "成交量": "volume",
                "成交额": "amount", "振幅": "turn",
                "涨跌幅": "pct_change", "换手率": "turnover"
            }, inplace=True)
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
            # 新浪接口列名已是英文：date, open, high, low, close, volume, amount, outstanding_share, turnover
            # 补充缺失的 pct_change 和 turn 列（从 close 计算近似）
            if "pct_change" not in df.columns and "close" in df.columns:
                df["pct_change"] = df["close"].pct_change() * 100
            if "turn" not in df.columns:
                df["turn"] = (df["high"] - df["low"]) / df["close"] * 100
            return df
        except Exception:
            return None

    def get_weekly(self, symbol: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取周线数据"""
        if symbol.endswith(".SH") or symbol.endswith(".SZ"):
            code = symbol
        else:
            code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"

        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365 * 3)).strftime("%Y%m%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        df = ak.stock_zh_a_hist(
            symbol=code.split(".")[0],
            period="weekly",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"
        )
        df.columns = [c.lower() for c in df.columns]
        df.rename(columns={
            "日期": "date", "开盘": "open", "收盘": "close",
            "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount"
        }, inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        return df

    def get_realtime_quote(self, symbol: str) -> dict:
        """
        获取实时行情（单股）
        返回：当前价、涨跌幅、成交量、换手率等
        """
        try:
            if symbol.endswith(".SH") or symbol.endswith(".SZ"):
                code = symbol
            else:
                code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"

            df = ak.stock_zh_a_spot_em()
            row = df[df["代码"] == code.split(".")[0]]
            if row.empty:
                raise ValueError(f"股票 {code} 未找到")

            r = row.iloc[0]
            return {
                "code": code,
                "name": r["名称"],
                "price": r["最新价"],
                "change_pct": r["涨跌幅"],
                "volume": r["成交量"],
                "amount": r["成交额"],
                "turnover": r["换手率"],
                "pe": r["市盈率"],
                "pb": r["市净率"],
                "mkt_cap": r["总市值"],
                "float_cap": r["流通市值"],
                "high": r["最高"],
                "low": r["最低"],
                "open": r["今开"],
                "prev_close": r["昨收"]
            }
        except Exception as e:
            raise RuntimeError(f"获取实时行情失败 {symbol}: {e}")

    def get_money_flow(self, symbol: str) -> dict:
        """
        获取个股资金流向
        返回：主力净流入、超大单净流入、大单净流入等
        """
        try:
            if not symbol.endswith((".SH", ".SZ")):
                code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"
            else:
                code = symbol

            df = ak.stock_individual_fund_flow(stock=code.split(".")[0], market="sh" if code.endswith(".SH") else "sz")
            latest = df.iloc[-1]
            return {
                "date": latest["日期"],
                "main_net_inflow": latest["主力净流入净额"],
                "main_net_inflow_pct": latest["主力净流入资金占比"],
                "super_large_net": latest["超大单净流入净额"],
                "large_net": latest["大单净流入净额"],
                "medium_net": latest["中单净流入净额"],
                "small_net": latest["小单净流入净额"],
            }
        except Exception as e:
            raise RuntimeError(f"获取资金流向失败 {symbol}: {e}")

    def get_sector_flow(self, limit: int = 10) -> pd.DataFrame:
        """
        获取板块资金流排名
        Args:
            limit: 返回前N个板块
        """
        try:
            df = ak.stock_sector_fund_flow_rank(indicator="今日")
            df = df.head(limit)
            return df
        except Exception as e:
            raise RuntimeError(f"获取板块资金流失败: {e}")

    def get_limit_up(self) -> pd.DataFrame:
        """获取今日涨停股"""
        try:
            df = ak.stock_zt_pool_em(date=datetime.now().strftime("%Y%m%d"))
            return df
        except Exception:
            # 如果今日没有数据，尝试昨天
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            df = ak.stock_zt_pool_em(date=yesterday)
            return df

    def get_limit_down(self) -> pd.DataFrame:
        """获取今日跌停股"""
        try:
            df = ak.stock_zt_pool_strong_em(date=datetime.now().strftime("%Y%m%d"))
            return df
        except Exception:
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            df = ak.stock_zt_pool_strong_em(date=yesterday)
            return df

    def get_stock_info(self, symbol: str) -> dict:
        """
        获取股票基本信息
        """
        try:
            if not symbol.endswith((".SH", ".SZ")):
                code = f"{symbol}.SH" if symbol.startswith(("6", "9")) else f"{symbol}.SZ"
            else:
                code = symbol

            df = ak.stock_individual_info_em(symbol=code.split(".")[0])
            info = dict(zip(df["item"], df["value"]))
            return info
        except Exception as e:
            raise RuntimeError(f"获取股票信息失败 {symbol}: {e}")

    def get_valuation(self, symbol: str, current_price: float) -> dict:
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

    def get_dividend(self, symbol: str) -> list:
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

    def get_profit_forecast(self, symbol: str) -> dict:
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

    def get_major_shareholders(self, symbol: str) -> list:
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

    def get_shareholder_changes(self, symbol: str, limit: int = 5) -> list:
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

    def get_profit_sheet_summary(self, symbol: str) -> dict:
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

    def get_cash_flow_summary(self, symbol: str) -> dict:
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
