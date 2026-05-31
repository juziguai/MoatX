"""Sina data provider — quotes + boards + HTTP protection.

APIs: hq.sinajs.cn, vip.stock.finance.sina.com.cn
Capabilities: QUOTE, BOARD_INDUSTRY, BOARD_CONCEPT, INDEX_QUOTE
"""

from __future__ import annotations

import json, logging, time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from modules.data_source import Capability, DataSource
from modules.result import Result
from modules.sina_http import sina_get
from modules.utils import to_sina_code

_logger = logging.getLogger("moatx.ds.sina")


class SinaProvider(DataSource):
    """Sina Finance — quotes + board data."""

    @property
    def name(self) -> str:
        return "sina"

    def capabilities(self) -> set[Capability]:
        return {Capability.QUOTE, Capability.BOARD_INDUSTRY, Capability.BOARD_CONCEPT, Capability.INDEX_QUOTE}

    def fetch(self, capability: Capability, **params):
        t0 = time.time()
        try:
            if capability == Capability.QUOTE:
                return self._fetch_quotes(params.get("symbols", []), t0)
            elif capability in (Capability.BOARD_INDUSTRY, Capability.BOARD_CONCEPT):
                board_type = "industry" if capability == Capability.BOARD_INDUSTRY else "concept"
                return self._fetch_boards(board_type, params.get("use_cache", True), t0)
            elif capability == Capability.INDEX_QUOTE:
                return self._fetch_indices(params.get("codes", []), t0)
            else:
                return Result.fail(f"unsupported: {capability}", source=self.name)
        except Exception as exc:
            return Result.fail(str(exc), source=self.name, elapsed_ms=(time.time() - t0) * 1000)

    # ─── Quotes ────────────────────────────────

    def _fetch_quotes(self, symbols: list[str], t0: float) -> Result[dict]:
        if not symbols:
            return Result.ok({}, source=self.name)
        sina_codes = [to_sina_code(s) for s in symbols]
        try:
            r = sina_get(f"http://hq.sinajs.cn/list={','.join(sina_codes)}", encoding="gbk")
            if not r.text:
                return Result.fail("empty response", source=self.name)
            out = _parse_sina_quotes(r.text)
            return Result.ok(out, source=self.name, elapsed_ms=(time.time() - t0) * 1000)
        except Exception as exc:
            return Result.fail(str(exc), source=self.name, elapsed_ms=(time.time() - t0) * 1000)

    # ─── Boards ────────────────────────────────

    def _fetch_boards(self, board_type: str, use_cache: bool, t0: float) -> Result:
        from modules.crawler import cache as cc

        trade_date = cc.beijing_now().date().isoformat()
        prefix = "new_" if board_type == "industry" else "gn_"
        cache_key = cc.build_cache_key(f"sector_{board_type}_sina_v2", trade_date)

        if use_cache:
            cached = _sina_read_cache(cache_key)
            if cached.ok:
                return cached

        try:
            nodes = _sina_node_tree()
            targets = [n for n in nodes if n["node"].startswith(prefix)]
        except Exception as exc:
            stale = _sina_read_cache(cache_key, allow_stale=True)
            if stale.data is not None:
                return Result.ok(stale.data, source=self.name, warnings=["stale cache"], elapsed_ms=(time.time() - t0) * 1000)
            return Result.fail(str(exc), source=self.name)

        if not targets:
            return Result.fail("no nodes", source=self.name)

        rows = _sina_fetch_concurrent(targets, board_type, trade_date)
        if not rows:
            return Result.fail("empty rows", source=self.name)

        df = _sina_normalize(pd.DataFrame(rows))
        cc.write_json_cache(cache_key, df.to_dict(orient="records"), "sina", trade_date=trade_date)
        return Result.ok(df, source=self.name, elapsed_ms=(time.time() - t0) * 1000)

    # ─── Index Quotes ──────────────────────────

    def _fetch_indices(self, codes: list[str], t0: float) -> Result[dict]:
        if not codes:
            return Result.ok({}, source=self.name)
        try:
            r = sina_get("https://hq.sinajs.cn/list=" + ",".join(codes), encoding="gbk")
            import re
            rows = {}
            for line in r.text.strip().splitlines():
                m = re.match(r'var hq_str_([a-z]{2}\d+)="(.*)";', line.strip())
                if m:
                    parts = m.group(2).split(",")
                    rows[m.group(1)] = {
                        "name": parts[0], "open": _f(parts[1]), "prev_close": _f(parts[2]),
                        "price": _f(parts[3]), "high": _f(parts[4]), "low": _f(parts[5]),
                        "volume": _f(parts[8]), "amount": _f(parts[9]),
                    }
            return Result.ok(rows, source=self.name, elapsed_ms=(time.time() - t0) * 1000)
        except Exception as exc:
            return Result.fail(str(exc), source=self.name)


# ─── Module-level helpers ─────────────────────

def _f(v): 
    try: return float(v)
    except: return None

def _parse_sina_quotes(text: str) -> dict:
    out = {}
    for line in text.strip().splitlines():
        parts = line.strip().split(",")
        if len(parts) < 32: continue
        try:
            code = parts[0].split("=")[0].split("_")[-1] if "=" in parts[0] else ""
            name = parts[0].split('"')[1] if '"' in parts[0] else ""
            if not code: continue
            out[code] = {"code": code, "name": name, "price": _f(parts[3]),
                         "prev_close": _f(parts[2]), "high": _f(parts[4]),
                         "low": _f(parts[5]), "open": _f(parts[1]),
                         "volume": _f(parts[8]), "amount": _f(parts[9])}
            if parts[2] and parts[3]:
                out[code]["change_pct"] = round((_f(parts[3]) - _f(parts[2])) / _f(parts[2]) * 100, 2)
        except: continue
    return out

SINA_API = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php"
SINA_COLS = ["sector_type","sector","sector_code","pct_change","price","turnover","rise_count","fall_count","top_stock","top_stock_pct","source","trade_date"]

def _sina_node_tree():
    from modules.crawler import cache as cc
    cached = cc.read_json_cache("sina_node_tree", max_age_seconds=86400)
    if cached.ok and cached.data: return cached.data
    try:
        r = sina_get(f"{SINA_API}/Market_Center.getHQNodes")
        raw = json.loads(r.text.strip())
        nodes = _parse_nodes(raw)
        cc.write_json_cache("sina_node_tree", nodes, "sina")
        return nodes
    except: return []

def _parse_nodes(node):
    results = []
    children = node[1] if isinstance(node, list) and len(node) > 1 else []
    if not isinstance(children, list): return results
    for child in children:
        if not isinstance(child, list): continue
        if len(child) == 3 and isinstance(child[2], str) and child[2]:
            results.append({"name": child[0], "node": child[2]})
        elif len(child) >= 2 and isinstance(child[1], list):
            results.extend(_parse_nodes(child))
    return results

def _sina_board_data(node_code: str) -> dict:
    for attempt in range(3):
        try:
            r = sina_get(f"{SINA_API}/Market_Center.getHQNodeData", params={"page":1,"num":100,"sort":"changepercent","asc":0,"node":node_code})
            data = json.loads(r.text)
            if not isinstance(data, list) or not data:
                if attempt < 2: time.sleep(1); continue
                return {}
            break
        except:
            if attempt < 2: time.sleep(1); continue
            return {}
    changes = [float(s.get("changepercent", 0) or 0) for s in data]
    top = data[0]
    return {"pct_change": round(sum(changes)/len(changes), 3), "rise_count": sum(1 for c in changes if c>0),
            "fall_count": sum(1 for c in changes if c<0), "top_stock": top.get("name",""),
            "top_stock_pct": float(top.get("changepercent",0) or 0), "trade": float(top.get("trade",0) or 0)}

def _sina_row(node, board, stype, tdate):
    return {"sector_type": "hangye" if stype=="industry" else "gainian", "sector": node["name"],
            "sector_code": node["node"], "pct_change": board.get("pct_change"), "price": board.get("trade"),
            "turnover": pd.NA, "rise_count": board.get("rise_count"), "fall_count": board.get("fall_count"),
            "top_stock": board.get("top_stock"), "top_stock_pct": board.get("top_stock_pct"),
            "source": "sina", "trade_date": tdate}

def _sina_fetch_concurrent(nodes, stype, tdate):
    import threading
    rows, lock = [], threading.Lock()
    def _one(idx, n):
        time.sleep(0.1 * (idx % 3))
        b = _sina_board_data(n["node"])
        if b:
            with lock: rows.append(_sina_row(n, b, stype, tdate))
    with ThreadPoolExecutor(max_workers=3) as pool:
        for f in [pool.submit(_one, i, n) for i, n in enumerate(nodes)]:
            try: f.result()
            except: pass
    return rows

def _sina_normalize(df):
    out = df.copy()
    for c in SINA_COLS:
        if c not in out.columns: out[c] = pd.NA
    for c in ["pct_change","price","turnover","rise_count","fall_count","top_stock_pct"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out[SINA_COLS]

def _sina_read_cache(key, allow_stale=False):
    from modules.crawler import cache as cc
    r = cc.read_json_cache(key, max_age_seconds=None)
    if r.ok or (allow_stale and r.data is not None):
        r.data = _sina_normalize(pd.DataFrame(r.data or []))
        r.source = r.source or "sina"
        if allow_stale and not r.ok and r.error:
            r.warnings.append(f"stale: {r.error}")
    return r
