"""Fast intraday buy/no-buy decision helper.

This module is intentionally lightweight. It avoids the slow single-stock
report chain and uses only live quotes plus local daily cache/topic tags.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
import tomllib
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from modules.utils import normalize_symbol, to_sina_code, to_tencent_code

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_WAREHOUSE_DB = _PROJECT_ROOT / "data" / "warehouse.db"
_SECTOR_GRAPH = _PROJECT_ROOT / "data" / "sector_graph.toml"
_DEFAULT_WATCHLIST = _PROJECT_ROOT / "data" / "swing_watchlist_latest.json"
_TAG_CACHE: dict[str, list[str]] | None = None


def build_quick_decision(
    symbols: list[str],
    *,
    source: str = "auto",
    timeout: float = 1.2,
    include_tags: bool = True,
    include_event_factors: bool = True,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build fast intraday decisions for stock symbols."""
    started = time.perf_counter()
    codes = _unique_codes(symbols)
    quote_started = time.perf_counter()
    quotes, quote_source, quote_warning = _fetch_quotes(codes, source=source, timeout=timeout)
    quote_elapsed = time.perf_counter() - quote_started
    daily_map = _load_daily_cache(codes, db_path=db_path)
    tags_by_code = _code_tags(codes) if include_tags else {}
    event_factors = (
        _event_factors_by_code(codes, tags_by_code, db_path=db_path)
        if include_tags and include_event_factors
        else {}
    )

    rows = []
    for code in codes:
        quote = quotes.get(code, {"code": code})
        daily = daily_map.get(code, [])
        rows.append(
            _decision_for(
                code,
                quote,
                daily,
                tags_by_code.get(code, []),
                event_factors.get(code),
            )
        )

    elapsed = time.perf_counter() - started
    return {
        "engine": "quick_decision_v1",
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "summary": {
            "count": len(rows),
            "source": quote_source,
            "quote_elapsed_seconds": round(quote_elapsed, 3),
            "elapsed_seconds": round(elapsed, 3),
            "warning": quote_warning,
            "event_factor_count": sum(1 for row in event_factors.values() if row.get("status") == "active"),
        },
        "decisions": rows,
    }


def save_quick_decision(payload: dict[str, Any], *, db_path: str | Path | None = None) -> int:
    """Persist a quick decision payload and return the run id."""
    from modules.db import DatabaseManager

    db = DatabaseManager(str(_resolve_db_path(db_path)))
    try:
        return db.quick_decision().record_run(payload)
    finally:
        db.close()


def review_quick_decisions(
    symbols: list[str] | None = None,
    *,
    limit: int = 20,
    horizon_days: int = 3,
    action: str | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Evaluate stored quick decisions against later daily closes."""
    from modules.db import DatabaseManager

    codes = _unique_codes(symbols or [])
    db = DatabaseManager(str(_resolve_db_path(db_path)))
    try:
        payload = db.quick_decision().evaluate(
            limit=max(1, int(limit)),
            horizon_days=max(1, int(horizon_days)),
            symbols=codes or None,
            action=action,
        )
    finally:
        db.close()
    payload["symbols"] = codes
    return payload


def evaluate_quick_decisions(
    symbols: list[str] | None = None,
    *,
    horizons: list[int] | None = None,
    limit: int = 200,
    action: str | None = None,
    save: bool = True,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Evaluate stored quick decisions for several horizons."""
    from modules.db import DatabaseManager

    codes = _unique_codes(symbols or [])
    db = DatabaseManager(str(_resolve_db_path(db_path)))
    try:
        payload = db.quick_decision().evaluate_many(
            horizons=horizons or [1, 3, 5],
            limit=max(1, int(limit)),
            symbols=codes or None,
            action=action,
            save=save,
        )
    finally:
        db.close()
    payload["symbols"] = codes
    payload["saved_to_db"] = bool(save)
    return payload


def summarize_quick_decision_evaluations(
    *,
    horizon_days: int = 3,
    limit: int = 500,
    min_samples: int = 1,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Summarize persisted quick-decision evaluations."""
    from modules.db import DatabaseManager

    db = DatabaseManager(str(_resolve_db_path(db_path)))
    try:
        return db.quick_decision().evaluation_report(
            horizon_days=max(1, int(horizon_days)),
            limit=max(1, int(limit)),
            min_samples=max(1, int(min_samples)),
        )
    finally:
        db.close()


def _unique_codes(symbols: list[str]) -> list[str]:
    out: list[str] = []
    for raw in symbols:
        code = normalize_symbol(str(raw or ""))
        if code and code not in out:
            out.append(code)
    return out


def _fetch_quotes(
    symbols: list[str],
    *,
    source: str,
    timeout: float,
) -> tuple[dict[str, dict[str, Any]], str, str]:
    sources = [source] if source in {"sina", "tencent"} else ["sina", "tencent"]
    warnings: list[str] = []
    for item in sources:
        try:
            if item == "sina":
                quotes = _fetch_sina_quotes(symbols, timeout=timeout)
            else:
                quotes = _fetch_tencent_quotes(symbols, timeout=timeout)
            if quotes:
                return quotes, item, "; ".join(warnings)
            warnings.append(f"{item}: empty")
        except Exception as exc:
            warnings.append(f"{item}: {exc}")
    return {}, "none", "; ".join(warnings)


def _fetch_sina_quotes(symbols: list[str], *, timeout: float) -> dict[str, dict[str, Any]]:
    import re

    import requests

    if not symbols:
        return {}
    session = requests.Session()
    session.trust_env = False
    session.proxies = {"http": None, "https": None}
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn"})
    url = "http://hq.sinajs.cn/list=" + ",".join(to_sina_code(symbol) for symbol in symbols)
    resp = session.get(url, timeout=max(0.2, float(timeout)))
    resp.encoding = "gbk"
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}")
    out: dict[str, dict[str, Any]] = {}
    pattern = re.compile(r'var hq_str_([a-z]{2}\d+)="(.*)";')
    for line in resp.text.strip().splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        full_code, body = match.groups()
        parts = body.split(",")
        if len(parts) < 32 or not parts[0]:
            continue
        prev_close = _num(parts[2])
        price = _num(parts[3])
        change_pct = ((price - prev_close) / prev_close * 100) if price and prev_close else 0.0
        code = normalize_symbol(full_code)
        out[code] = {
            "code": code,
            "source_code": full_code,
            "name": parts[0],
            "open": _num(parts[1]),
            "prev_close": prev_close,
            "price": price,
            "high": _num(parts[4]),
            "low": _num(parts[5]),
            "volume": int(_num(parts[8]) or 0),
            "amount": _num(parts[9]),
            "change_pct": round(change_pct, 2),
            "quote_time": f"{parts[30]} {parts[31]}" if len(parts) > 31 else "",
        }
    return out


def _fetch_tencent_quotes(symbols: list[str], *, timeout: float) -> dict[str, dict[str, Any]]:
    import requests

    from modules.crawler.tencent import _parse_tencent_response

    if not symbols:
        return {}
    query = ",".join(to_tencent_code(symbol) for symbol in symbols)
    session = requests.Session()
    session.trust_env = False
    session.proxies = {"http": None, "https": None}
    resp = session.get("http://qt.gtimg.cn/q", params={"q": query}, timeout=max(0.2, float(timeout)))
    resp.encoding = "gbk"
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}")
    out: dict[str, dict[str, Any]] = {}
    for row in _parse_tencent_response(resp.text):
        code = normalize_symbol(str(row.get("code") or ""))
        if not code:
            continue
        out[code] = {
            "code": code,
            "name": row.get("name", ""),
            "open": row.get("open"),
            "prev_close": row.get("prev_close"),
            "price": row.get("price"),
            "high": row.get("high"),
            "low": row.get("low"),
            "volume": row.get("volume"),
            "amount": row.get("amount"),
            "turnover": row.get("turnover"),
            "change_pct": row.get("pct_change"),
            "quote_time": row.get("datetime", ""),
        }
    return out


def _load_daily_cache(
    symbols: list[str],
    *,
    days: int = 80,
    db_path: str | Path | None = None,
) -> dict[str, list[dict[str, Any]]]:
    warehouse_db = _resolve_db_path(db_path)
    if not warehouse_db.exists() or not symbols:
        return {}
    start = (date.today() - timedelta(days=days * 2)).isoformat()
    placeholders = ",".join("?" for _ in symbols)
    query = (
        "SELECT symbol, trade_date, open, high, low, close, volume, amount, turn, pct_change "
        f"FROM price_daily WHERE symbol IN ({placeholders}) AND adjust=? AND trade_date>=? "
        "ORDER BY symbol, trade_date"
    )
    params = [*symbols, "qfq", start]
    out: dict[str, list[dict[str, Any]]] = {}
    try:
        with sqlite3.connect(warehouse_db) as conn:
            conn.row_factory = sqlite3.Row
            for row in conn.execute(query, params):
                code = normalize_symbol(str(row["symbol"]))
                out.setdefault(code, []).append(
                    {
                        "date": row["trade_date"],
                        "open": _num(row["open"]),
                        "high": _num(row["high"]),
                        "low": _num(row["low"]),
                        "close": _num(row["close"]),
                        "volume": _num(row["volume"]),
                        "amount": _num(row["amount"]),
                        "turn": _num(row["turn"]),
                        "pct_change": _num(row["pct_change"]),
                    }
                )
    except Exception:
        return {}
    return out


def _resolve_db_path(db_path: str | Path | None = None) -> Path:
    if db_path:
        return Path(db_path)
    try:
        from modules.config import cfg

        return Path(cfg().data.warehouse_path)
    except Exception:
        return _WAREHOUSE_DB


def load_watchlist_symbols(path: str | Path | None = None) -> list[str]:
    """Load symbols from the latest swing watchlist."""
    watchlist_path = Path(path) if path else _DEFAULT_WATCHLIST
    if not watchlist_path.exists():
        return []
    try:
        payload = json.loads(watchlist_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    symbols: list[str] = []
    for bucket in ("positions", "candidates", "raw_candidates"):
        rows = payload.get(bucket) or []
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = row.get("symbol") or row.get("code") or row.get("stock_code")
            code = normalize_symbol(str(symbol or ""))
            if code:
                symbols.append(code)
    return _unique_codes(symbols)


def _decision_for(
    code: str,
    quote: dict[str, Any],
    daily: list[dict[str, Any]],
    tags: list[str],
    event_factor: dict[str, Any] | None = None,
) -> dict[str, Any]:
    price = _num(quote.get("price"))
    prev_close = _num(quote.get("prev_close"))
    pct = _num(quote.get("change_pct"))
    if not pct and price and prev_close:
        pct = (price / prev_close - 1.0) * 100

    metrics = _technical_metrics(daily, quote)
    warnings: list[str] = []
    reasons: list[str] = []
    action = "观察"
    recommendation = "先观察，不直接追"
    score = 50.0

    if not price:
        action = "不买"
        recommendation = "行情缺失，不能下单"
        warnings.append("未取到有效实时价格")
        score = 0.0
    else:
        score += _pct_score(pct)
        ma5_gap = metrics.get("ma5_gap_pct")
        high20_gap = metrics.get("high20_gap_pct")
        above_ma5 = metrics.get("above_ma5")
        above_ma20 = metrics.get("above_ma20")

        if pct >= 8.5:
            warnings.append("涨幅接近涨停，追高回撤风险很大")
            score -= 28
        elif pct >= 5.5:
            warnings.append("早盘涨幅过大，优先等回踩")
            score -= 15
        elif 2.0 <= pct < 5.5:
            reasons.append("盘中强度在线")
            score += 8
        elif -1.0 <= pct < 2.0:
            reasons.append("涨幅不极端，仍有观察空间")
            score += 4
        elif pct <= -3.0:
            warnings.append("当日跌幅偏大，先避开")
            score -= 18

        if ma5_gap is not None:
            if ma5_gap > 6.0:
                warnings.append(f"现价高于5日线 {ma5_gap:.1f}%，位置偏急")
                score -= 12
            elif -1.5 <= ma5_gap <= 3.5:
                reasons.append("距离5日线不远")
                score += 7
        if high20_gap is not None:
            if high20_gap > -1.0 and pct >= 3.0:
                warnings.append("贴近20日高点且已拉升，适合等确认")
                score -= 6
            elif high20_gap <= -5.0 and pct >= 0:
                reasons.append("距离20日高点仍有空间")
                score += 3
        if above_ma5:
            reasons.append("站上5日线")
            score += 4
        if above_ma20 is False:
            warnings.append("仍在20日线下方，中期趋势未完全修复")
            score -= 8

        factor_delta, factor_reason, factor_warning = _event_factor_score(event_factor)
        score += factor_delta
        if factor_reason:
            reasons.append(factor_reason)
        if factor_warning:
            warnings.append(factor_warning)

        if score >= 66 and pct < 5.5:
            action = "可轻仓观察"
            recommendation = "只适合回踩承接，不建议市价猛追"
        elif score >= 54:
            action = "观察"
            recommendation = "等回踩或尾盘确认"
        else:
            action = "不买"
            recommendation = "当前位置不适合新开仓"

    buy_zone = _buy_zone(price, prev_close, metrics, action)
    return {
        "symbol": code,
        "name": quote.get("name") or code,
        "action": action,
        "score": round(max(0.0, min(100.0, score)), 1),
        "recommendation": recommendation,
        "buy_zone": buy_zone,
        "quote": {
            "price": round(price, 3) if price else None,
            "prev_close": round(prev_close, 3) if prev_close else None,
            "change_pct": round(pct, 2),
            "open": _round(quote.get("open")),
            "high": _round(quote.get("high")),
            "low": _round(quote.get("low")),
            "amount": _round(quote.get("amount")),
            "quote_time": quote.get("quote_time", ""),
        },
        "metrics": metrics,
        "tags": tags[:8],
        "event_factor": event_factor or {},
        "reasons": reasons[:5],
        "warnings": warnings[:5],
    }


def _event_factor_score(event_factor: dict[str, Any] | None) -> tuple[float, str, str]:
    if not event_factor:
        return 0.0, "", ""
    sector = str(event_factor.get("sector") or "")
    topic = str(event_factor.get("top_topic") or "")
    score = _num(event_factor.get("factor_score"))
    direction = str(event_factor.get("direction") or "neutral")
    if event_factor.get("status") == "stale":
        updated_at = str(event_factor.get("updated_at") or "")
        return 0.0, "", f"事件因子已过期（{updated_at}），不参与加分"
    label = f"{sector}/{topic}" if topic else sector
    if direction == "bearish" and score >= 8:
        return -8.0, "", f"事件因子偏空：{label} {score:.1f}"
    if direction == "bullish":
        if score >= 18:
            return 8.0, f"事件因子强势：{label} {score:.1f}", ""
        if score >= 10:
            return 4.0, f"事件因子支持：{label} {score:.1f}", ""
    return 0.0, "", ""


def _technical_metrics(daily: list[dict[str, Any]], quote: dict[str, Any]) -> dict[str, Any]:
    rows = [row for row in daily if _num(row.get("close"))]
    price = _num(quote.get("price"))
    if price:
        today = date.today().isoformat()
        current = {
            "date": today,
            "open": _num(quote.get("open")) or price,
            "high": _num(quote.get("high")) or price,
            "low": _num(quote.get("low")) or price,
            "close": price,
            "volume": _num(quote.get("volume")),
            "amount": _num(quote.get("amount")),
        }
        if rows and str(rows[-1].get("date")) == today:
            rows[-1] = current
        else:
            rows.append(current)
    closes = [_num(row.get("close")) for row in rows if _num(row.get("close"))]
    highs = [_num(row.get("high")) or _num(row.get("close")) for row in rows if _num(row.get("close"))]
    if len(closes) < 5:
        return {"daily_bars": len(closes)}
    last = closes[-1]
    ma5 = sum(closes[-5:]) / 5
    ma10 = sum(closes[-10:]) / 10 if len(closes) >= 10 else None
    ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else None
    high20 = max(highs[-20:]) if len(highs) >= 20 else max(highs)
    return {
        "daily_bars": len(closes),
        "ma5": round(ma5, 3),
        "ma10": round(ma10, 3) if ma10 else None,
        "ma20": round(ma20, 3) if ma20 else None,
        "high20": round(high20, 3) if high20 else None,
        "ma5_gap_pct": round((last / ma5 - 1.0) * 100, 2) if ma5 else None,
        "ma20_gap_pct": round((last / ma20 - 1.0) * 100, 2) if ma20 else None,
        "high20_gap_pct": round((last / high20 - 1.0) * 100, 2) if high20 else None,
        "above_ma5": last >= ma5 if ma5 else None,
        "above_ma20": last >= ma20 if ma20 else None,
    }


def _pct_score(pct: float) -> float:
    if -0.5 <= pct <= 2.5:
        return 10
    if 2.5 < pct <= 5.5:
        return 8
    if -3.0 <= pct < -0.5:
        return 0
    if 5.5 < pct <= 8.5:
        return -4
    return -12


def _buy_zone(price: float, prev_close: float, metrics: dict[str, Any], action: str) -> str:
    if not price or action == "不买":
        return "无"
    anchors = []
    ma5 = metrics.get("ma5")
    if ma5:
        anchors.append(float(ma5))
    if prev_close:
        anchors.append(float(prev_close))
    if not anchors:
        return f"{price:.2f} 下方回踩承接"
    low = min(anchors)
    high = max(anchors)
    if high < price:
        return f"{low:.2f}-{high:.2f} 回踩不破再看"
    return f"{low:.2f}-{min(price, high):.2f} 分批观察"


def _code_tags(symbols: list[str]) -> dict[str, list[str]]:
    global _TAG_CACHE
    if _TAG_CACHE is None:
        _TAG_CACHE = _load_sector_graph_tags()
    return {code: _TAG_CACHE.get(code, []) for code in symbols}


def _load_sector_graph_tags() -> dict[str, list[str]]:
    if not _SECTOR_GRAPH.exists():
        return {}
    try:
        raw = tomllib.loads(_SECTOR_GRAPH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, list[str]] = {}
    for node in raw.get("nodes", []):
        tag = str(node.get("tag") or "").strip()
        if not tag:
            continue
        for member in node.get("members", []):
            code = normalize_symbol(str(member.get("code") or ""))
            if code:
                out.setdefault(code, [])
                if tag not in out[code]:
                    out[code].append(tag)
    return out


def _event_factors_by_code(
    symbols: list[str],
    tags_by_code: dict[str, list[str]],
    *,
    db_path: str | Path | None = None,
    max_age_days: int = 5,
) -> dict[str, dict[str, Any]]:
    sectors = sorted({tag for code in symbols for tag in tags_by_code.get(code, [])})
    warehouse_db = _resolve_db_path(db_path)
    if not sectors or not warehouse_db.exists():
        return {}
    placeholders = ",".join("?" for _ in sectors)
    query = (
        "SELECT sector, factor_score, direction, insight_count, avg_value_score, "
        f"top_topic, top_titles_json, avg_time_decay, updated_at FROM event_news_factors "
        f"WHERE sector IN ({placeholders})"
    )
    factors: dict[str, dict[str, Any]] = {}
    try:
        with sqlite3.connect(warehouse_db) as conn:
            conn.row_factory = sqlite3.Row
            for row in conn.execute(query, sectors):
                factor = dict(row)
                factor["status"] = _event_factor_status(str(factor.get("updated_at") or ""), max_age_days)
                factors[str(factor.get("sector") or "")] = factor
    except Exception:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for code in symbols:
        candidates = [factors[tag] for tag in tags_by_code.get(code, []) if tag in factors]
        if candidates:
            out[code] = max(candidates, key=lambda item: _num(item.get("factor_score")))
    return out


def _event_factor_status(updated_at: str, max_age_days: int) -> str:
    try:
        value = datetime.fromisoformat(str(updated_at).replace(" ", "T"))
    except Exception:
        return "unknown"
    age = datetime.now() - value
    return "stale" if age.days > max_age_days else "active"


def _num(value: Any) -> float:
    try:
        if value in (None, "", "-", "None"):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _round(value: Any, digits: int = 3) -> float | None:
    number = _num(value)
    return round(number, digits) if number else None


def print_quick_decision(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    print(
        "MoatX 极速盘中判断 | "
        f"行情源 {summary.get('source')} | "
        f"行情 {float(summary.get('quote_elapsed_seconds') or 0):.2f}s | "
        f"总耗时 {float(summary.get('elapsed_seconds') or 0):.2f}s"
    )
    if summary.get("warning"):
        print(f"数据提示: {summary.get('warning')}")
    if summary.get("run_id"):
        print(f"已记录: quick_decision_run_id={summary.get('run_id')}")
    for idx, row in enumerate(payload.get("decisions") or [], 1):
        quote = row.get("quote") or {}
        print(
            f"{idx}. {row.get('symbol')} {row.get('name')} | "
            f"{row.get('action')} {float(row.get('score') or 0):.1f} | "
            f"现价 {quote.get('price')} | 涨跌 {float(quote.get('change_pct') or 0):+.2f}% | "
            f"{row.get('recommendation')}"
        )
        buy_zone = row.get("buy_zone")
        if buy_zone and buy_zone != "无":
            print(f"   参考: {buy_zone}")
        tags = row.get("tags") or []
        if tags:
            print("   主题: " + "、".join(str(item) for item in tags[:5]))
        factor = row.get("event_factor") or {}
        if factor:
            status = str(factor.get("status") or "unknown")
            sector = str(factor.get("sector") or "")
            topic = str(factor.get("top_topic") or "")
            score = float(factor.get("factor_score") or 0)
            print(f"   事件: {sector}/{topic or '-'} {score:.1f} [{status}]")
        reasons = row.get("reasons") or []
        if reasons:
            print("   支持: " + "；".join(str(item) for item in reasons[:4]))
        warnings = row.get("warnings") or []
        if warnings:
            print("   风险: " + "；".join(str(item) for item in warnings[:4]))


def print_quick_decision_review(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    print(
        "MoatX 极速盘中判断复盘 | "
        f"T+{payload.get('horizon_days')} | "
        f"样本 {summary.get('evaluated')}/{summary.get('total')} | "
        f"待评估 {summary.get('pending')}"
    )
    print(
        f"平均收益 {float(summary.get('avg_forward_return_pct') or 0):+.2f}% | "
        f"买入胜率 {float(summary.get('buy_win_rate') or 0):.1f}% | "
        f"避开命中 {float(summary.get('avoid_hit_rate') or 0):.1f}%"
    )
    for item in payload.get("items") or []:
        forward = item.get("forward_return_pct")
        forward_text = "pending" if forward is None else f"{float(forward):+.2f}%"
        drawdown = item.get("max_drawdown_pct")
        drawdown_text = "-" if drawdown is None else f"{float(drawdown):+.2f}%"
        print(
            f"{item.get('id')}. {item.get('symbol')} {item.get('name')} | "
            f"{item.get('action')} {float(item.get('score') or 0):.1f} | "
            f"{item.get('outcome')} | 收益 {forward_text} | 最大回撤 {drawdown_text} | "
            f"{item.get('decision_at')} -> {item.get('exit_date') or '-'}"
        )


def print_quick_decision_evaluation(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    print(
        "MoatX 极速盘中判断自动评价 | "
        f"horizons={payload.get('horizons')} | "
        f"样本 {summary.get('evaluated')}/{summary.get('total')} | "
        f"待评估 {summary.get('pending')} | "
        f"保存 {payload.get('saved')}"
    )
    for result in payload.get("results") or []:
        item = result.get("summary") or {}
        print(
            f"T+{result.get('horizon_days')}: "
            f"样本 {item.get('evaluated')}/{item.get('total')} | "
            f"待评估 {item.get('pending')} | "
            f"保存 {result.get('saved')}"
        )


def print_quick_decision_summary(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    print(
        "MoatX 极速盘中判断评价面板 | "
        f"T+{payload.get('horizon_days')} | "
        f"样本 {summary.get('count')} | "
        f"成功率 {float(summary.get('success_rate') or 0):.1f}% | "
        f"均值 {float(summary.get('avg_forward_return_pct') or 0):+.2f}% | "
        f"回撤 {float(summary.get('avg_max_drawdown_pct') or 0):+.2f}%"
    )
    sections = [
        ("动作", payload.get("by_action") or []),
        ("分数段", payload.get("by_score_bucket") or []),
        ("主题", payload.get("by_tag") or []),
        ("事件板块", payload.get("by_event_sector") or []),
    ]
    for title, rows in sections:
        print(f"{title}:")
        for row in rows[:8]:
            print(
                f"  {row.get('key')}: n={row.get('count')} "
                f"成功率={float(row.get('success_rate') or 0):.1f}% "
                f"均值={float(row.get('avg_forward_return_pct') or 0):+.2f}% "
                f"回撤={float(row.get('avg_max_drawdown_pct') or 0):+.2f}%"
            )


def _parse_horizons(value: str | None) -> list[int]:
    if not value:
        return [1, 3, 5]
    out: list[int] = []
    for part in str(value).split(","):
        try:
            number = int(part.strip())
        except Exception:
            continue
        if number > 0 and number not in out:
            out.append(number)
    return out or [1, 3, 5]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m modules.quick_decision")
    parser.add_argument("symbols", nargs="*", help="股票代码，如 601899 000063；或 review/evaluate/summary")
    parser.add_argument("--source", choices=["auto", "sina", "tencent"], default="auto", help="行情源")
    parser.add_argument("--timeout", type=float, default=1.2, help="单源请求超时秒数")
    parser.add_argument("--no-tags", action="store_true", help="不读取本地主题图谱")
    parser.add_argument("--no-event-factors", action="store_true", help="不读取新闻事件因子")
    parser.add_argument("--watchlist", action="store_true", help="追加读取短线观察名单")
    parser.add_argument("--watchlist-file", help="观察名单 JSON 路径，默认 data/swing_watchlist_latest.json")
    parser.add_argument("--no-save", action="store_true", help="不写入决策日志")
    parser.add_argument("--review", action="store_true", help="复盘历史 quick-decision 结果")
    parser.add_argument("--limit", type=int, default=20, help="复盘条数")
    parser.add_argument("--horizon", type=int, default=3, help="复盘未来交易日数量")
    parser.add_argument("--horizons", default="1,3,5", help="自动评价 horizon 列表，如 1,3,5")
    parser.add_argument("--save-evaluation", action="store_true", help="复盘时保存评价结果")
    parser.add_argument("--min-samples", type=int, default=1, help="汇总面板最小样本数")
    parser.add_argument("--action", help="按决策动作过滤复盘，如 不买")
    parser.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    args = parser.parse_args(argv)

    mode = args.symbols[0] if args.symbols and args.symbols[0] in {"review", "evaluate", "summary", "dashboard"} else ""
    review_mode = args.review or mode == "review"
    evaluate_mode = mode == "evaluate"
    summary_mode = mode in {"summary", "dashboard"}
    review_symbols = args.symbols[1:] if mode else args.symbols
    if summary_mode:
        payload = summarize_quick_decision_evaluations(
            horizon_days=args.horizon,
            limit=args.limit,
            min_samples=args.min_samples,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_summary(payload)
        return
    if evaluate_mode or args.save_evaluation:
        payload = evaluate_quick_decisions(
            review_symbols,
            horizons=_parse_horizons(args.horizons),
            limit=args.limit,
            action=args.action,
            save=True,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_evaluation(payload)
        return
    if review_mode:
        payload = review_quick_decisions(
            review_symbols,
            limit=args.limit,
            horizon_days=args.horizon,
            action=args.action,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_review(payload)
        return
    if not args.symbols:
        if not args.watchlist:
            parser.error("请提供股票代码，或使用 --watchlist / --review")
    symbols = list(args.symbols)
    if args.watchlist:
        symbols = _unique_codes([*symbols, *load_watchlist_symbols(args.watchlist_file)])
    payload = build_quick_decision(
        symbols,
        source=args.source,
        timeout=args.timeout,
        include_tags=not args.no_tags,
        include_event_factors=not args.no_event_factors,
    )
    if args.watchlist:
        payload["summary"]["watchlist_path"] = str(Path(args.watchlist_file) if args.watchlist_file else _DEFAULT_WATCHLIST)
    if not args.no_save:
        payload["summary"]["run_id"] = save_quick_decision(payload)
    if args.as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print_quick_decision(payload)


if __name__ == "__main__":
    main()
