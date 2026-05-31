"""Cninfo (JuChao) data provider — dividends + shareholders + profit sheets.

Capabilities: DIVIDEND, MAJOR_SHAREHOLDERS, SHAREHOLDER_CHANGES, PROFIT_SHEET
"""

from __future__ import annotations

from modules.data_source import Capability, DataSource
from modules.result import Result


class CninfoProvider(DataSource):
    """Cninfo/JuChao — financial data (via akshare)."""

    @property
    def name(self) -> str:
        return "cninfo"

    def capabilities(self) -> set[Capability]:
        return {Capability.DIVIDEND, Capability.MAJOR_SHAREHOLDERS,
                Capability.SHAREHOLDER_CHANGES, Capability.PROFIT_SHEET}

    def fetch(self, capability: Capability, **params):
        import time
        t0 = time.time()
        symbol = params.get("symbol", "")
        try:
            if capability == Capability.DIVIDEND:
                data = get_dividend(symbol)
            elif capability == Capability.MAJOR_SHAREHOLDERS:
                data = get_major_shareholders(symbol)
            elif capability == Capability.SHAREHOLDER_CHANGES:
                data = get_shareholder_changes(symbol, params.get("limit", 5))
            elif capability == Capability.PROFIT_SHEET:
                data = get_profit_sheet_summary(symbol)
            else:
                return Result.fail(f"unsupported: {capability}", source=self.name)
            return Result.ok(data, source=self.name, elapsed_ms=(time.time()-t0)*1000)
        except Exception as exc:
            return Result.fail(str(exc), source=self.name, elapsed_ms=(time.time()-t0)*1000)


# --- Implementation functions (also used by stock_data.py) ---

def _ak():
    from modules.akshare_compat import import_akshare
    return import_akshare()

def _n(v):
    try: return float(v)
    except: return 0.0

def _cache_rw(symbol, key, fetch_fn):
    from modules.akshare_cache import read_cache, write_cache
    try:
        result = fetch_fn()
        write_cache(symbol, key, result)
        return result
    except Exception:
        cached = read_cache(symbol, key)
        return cached if cached is not None else [{"error": "akshare unavailable"}]


def get_dividend(symbol: str) -> list:
    def _f():
        code = symbol if symbol.endswith((".SH",".SZ")) else (f"{symbol}.SH" if symbol[0] in "69" else f"{symbol}.SZ")
        df = _ak().stock_dividend_cninfo(symbol=code.split(".")[0])
        results = []
        for _, row in df.head(5).iterrows():
            div = row.get("派息比例", 0)
            try: amt = float(str(div).replace("元","").replace("含税","")) if div and div!="NaN" else 0
            except: amt = 0
            results.append({"date": str(row.get("实施方案公告日期","")),
                           "dividend_per_share": amt, "type": str(row.get("分红类型",""))})
        return results
    return _cache_rw(symbol, "dividend", _f)


def get_profit_forecast(symbol: str) -> dict:
    def _f():
        code = symbol if symbol.endswith((".SH",".SZ")) else (f"{symbol}.SH" if symbol[0] in "69" else f"{symbol}.SZ")
        df = _ak().stock_profit_forecast_ths(symbol=code.split(".")[0])
        fcs = []
        for _, row in df.iterrows():
            eps = _n(row.get("均值",0))
            if eps > 0:
                fcs.append({"year": str(row.get("年度","")), "avg_eps": eps,
                            "min_eps": _n(row.get("最小值",0)),
                            "max_eps": _n(row.get("最大值",0)),
                            "num_firms": int(row.get("预测机构数",0))})
        return {"forecasts": fcs}
    from modules.akshare_cache import read_cache, write_cache
    try:
        result = _f(); write_cache(symbol, "profit_forecast", result); return result
    except:
        c = read_cache(symbol, "profit_forecast")
        return c if c is not None else {"forecasts": [], "error": "akshare unavailable"}


def get_major_shareholders(symbol: str) -> list:
    def _f():
        code = symbol if symbol.endswith((".SH",".SZ")) else (f"{symbol}.SH" if symbol[0] in "69" else f"{symbol}.SZ")
        df = _ak().stock_main_stock_holder(stock=code.split(".")[0])
        h = []
        for _, row in df.head(10).iterrows():
            pct = row.get("持股比例")
            try: pv = float(pct) if pct else 0
            except: pv = 0
            h.append({"name": str(row.get("股东名称","")), "pct": pv,
                      "nature": str(row.get("股本性质",""))})
        return h
    return _cache_rw(symbol, "major_shareholders", _f)


def get_shareholder_changes(symbol: str, limit: int = 5) -> list:
    def _f():
        code = symbol if symbol.endswith((".SH",".SZ")) else (f"{symbol}.SH" if symbol[0] in "69" else f"{symbol}.SZ")
        df = _ak().stock_shareholder_change_ths(symbol=code.split(".")[0])
        ch = []
        for _, row in df.head(limit).iterrows():
            ch.append({"date": str(row.get("公告日期","")),
                       "holder": str(row.get("变动股东","")),
                       "change_shares": row.get("变动数量",""),
                       "avg_price": row.get("交易均价","")})
        return ch
    return _cache_rw(symbol, "shareholder_changes", _f)


def get_profit_sheet_summary(symbol: str) -> dict:
    def _f():
        code = symbol if symbol.endswith((".SH",".SZ")) else symbol
        df = _ak().stock_profit_sheet_by_report_em(symbol=code)
        if df is None or df.empty: return {"error": "no data"}
        latest = df.iloc[0]
        rev = _n(latest.get("营业总收入",0))
        np_ = _n(latest.get("净利润",0))
        cost = _n(latest.get("营业成本",0))
        return {"report_date": str(latest.get("REPORT_DATE","")), "revenue": rev,
                "net_profit": np_, "gross_margin": round((rev-cost)/rev*100,2) if rev else 0,
                "net_margin": round(np_/rev*100,2) if rev else 0,
                "basic_eps": _n(latest.get("BASIC_EPS",0))}
    from modules.akshare_cache import read_cache, write_cache
    try:
        result = _f(); write_cache(symbol, "profit_sheet", result); return result
    except:
        c = read_cache(symbol, "profit_sheet")
        return c if c is not None else {"error": "akshare unavailable"}


def get_cash_flow_summary(symbol: str) -> dict:
    def _f():
        code = symbol if symbol.endswith((".SH",".SZ")) else (f"{symbol}.SH" if symbol[0] in "69" else f"{symbol}.SZ")
        df = _ak().stock_financial_cash_ths(symbol=code.split(".")[0], indicator="按报告期")
        if df is None or df.empty: return {"error": "no data"}
        latest = df.iloc[0]
        op = _n(latest.get("经营活动产生的现金流量净额",0))
        inv = _n(latest.get("投资活动产生的现金流量净额",0))
        fin = _n(latest.get("筹资活动产生的现金流量净额",0))
        end = _n(latest.get("期末现金及现金等价物余额",0))
        return {"report_date": str(latest.get("报告期","")), "operating_cf": op,
                "investing_cf": inv, "financing_cf": fin, "cash_end": end, "free_cf": op+inv}
    from modules.akshare_cache import read_cache, write_cache
    try:
        result = _f(); write_cache(symbol, "cash_flow", result); return result
    except:
        c = read_cache(symbol, "cash_flow")
        return c if c is not None else {"error": "akshare unavailable"}
