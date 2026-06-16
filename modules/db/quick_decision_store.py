"""Quick intraday decision persistence and review."""

from __future__ import annotations

import json
import sqlite3
from typing import Any


class QuickDecisionStore:
    """Persist quick intraday decisions and evaluate later price movement."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def record_run(self, payload: dict[str, Any]) -> int:
        summary = payload.get("summary") or {}
        decisions = list(payload.get("decisions") or [])
        generated_at = str(payload.get("generated_at") or "")
        symbols = [str(row.get("symbol") or "") for row in decisions if row.get("symbol")]

        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO quick_decision_runs
               (engine, generated_at, source, warning, symbol_count,
                quote_elapsed_seconds, elapsed_seconds, symbols_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(payload.get("engine") or "quick_decision"),
                generated_at,
                str(summary.get("source") or ""),
                str(summary.get("warning") or ""),
                int(summary.get("count") or len(decisions)),
                float(summary.get("quote_elapsed_seconds") or 0),
                float(summary.get("elapsed_seconds") or 0),
                json.dumps(symbols, ensure_ascii=False),
            ),
        )
        run_id = int(cursor.lastrowid)
        for row in decisions:
            quote = row.get("quote") or {}
            cursor.execute(
                """INSERT INTO quick_decision_rows
                   (run_id, symbol, name, action, score, price, prev_close,
                    change_pct, buy_zone, recommendation, quote_json, metrics_json,
                    event_factor_json, tags_json, reasons_json, warnings_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    str(row.get("symbol") or ""),
                    str(row.get("name") or ""),
                    str(row.get("action") or ""),
                    float(row.get("score") or 0),
                    float(quote.get("price") or 0),
                    float(quote.get("prev_close") or 0),
                    float(quote.get("change_pct") or 0),
                    str(row.get("buy_zone") or ""),
                    str(row.get("recommendation") or ""),
                    json.dumps(quote, ensure_ascii=False),
                    json.dumps(row.get("metrics") or {}, ensure_ascii=False),
                    json.dumps(row.get("event_factor") or {}, ensure_ascii=False),
                    json.dumps(row.get("tags") or [], ensure_ascii=False),
                    json.dumps(row.get("reasons") or [], ensure_ascii=False),
                    json.dumps(row.get("warnings") or [], ensure_ascii=False),
                    generated_at,
                ),
            )
        self._conn.commit()
        return run_id

    def list_rows(
        self,
        *,
        limit: int = 20,
        symbols: list[str] | None = None,
        action: str | None = None,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            conditions.append(f"r.symbol IN ({placeholders})")
            params.extend(symbols)
        if action:
            conditions.append("r.action = ?")
            params.append(action)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        query = (
            "SELECT r.*, q.engine, q.source, q.warning "
            "FROM quick_decision_rows r "
            "JOIN quick_decision_runs q ON q.id = r.run_id "
            f"{where} ORDER BY r.id DESC LIMIT ?"
        )
        params.append(max(1, int(limit)))
        cursor = self._conn.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def evaluate(
        self,
        *,
        limit: int = 20,
        horizon_days: int = 3,
        symbols: list[str] | None = None,
        action: str | None = None,
        save: bool = False,
    ) -> dict[str, Any]:
        rows = self.list_rows(limit=limit, symbols=symbols, action=action)
        items = [self._evaluate_row(row, horizon_days=horizon_days) for row in rows]
        saved = 0
        if save:
            saved = self.save_evaluations(items, horizon_days=horizon_days)
        return {
            "horizon_days": horizon_days,
            "limit": limit,
            "summary": _evaluation_summary(items),
            "saved": saved,
            "items": items,
        }

    def evaluate_many(
        self,
        *,
        horizons: list[int],
        limit: int = 200,
        symbols: list[str] | None = None,
        action: str | None = None,
        save: bool = False,
    ) -> dict[str, Any]:
        clean_horizons = _unique_positive_ints(horizons)
        results = [
            self.evaluate(
                limit=limit,
                horizon_days=horizon,
                symbols=symbols,
                action=action,
                save=save,
            )
            for horizon in clean_horizons
        ]
        return {
            "horizons": clean_horizons,
            "limit": limit,
            "saved": sum(int(result.get("saved") or 0) for result in results),
            "summary": _multi_evaluation_summary(results),
            "results": results,
        }

    def save_evaluations(self, items: list[dict[str, Any]], *, horizon_days: int) -> int:
        cursor = self._conn.cursor()
        saved = 0
        for item in items:
            decision_row_id = int(item.get("id") or 0)
            if decision_row_id <= 0:
                continue
            cursor.execute(
                """INSERT INTO quick_decision_evaluations
                   (decision_row_id, horizon_days, symbol, action, status, outcome,
                    entry_price, exit_date, exit_close, forward_return_pct, max_drawdown_pct)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(decision_row_id, horizon_days) DO UPDATE SET
                   symbol=excluded.symbol,
                   action=excluded.action,
                   status=excluded.status,
                   outcome=excluded.outcome,
                   entry_price=excluded.entry_price,
                   exit_date=excluded.exit_date,
                   exit_close=excluded.exit_close,
                   forward_return_pct=excluded.forward_return_pct,
                   max_drawdown_pct=excluded.max_drawdown_pct,
                   evaluated_at=datetime('now', 'localtime'),
                   updated_at=datetime('now', 'localtime')""",
                (
                    decision_row_id,
                    int(horizon_days),
                    str(item.get("symbol") or ""),
                    str(item.get("action") or ""),
                    str(item.get("status") or "pending"),
                    str(item.get("outcome") or "pending"),
                    _num(item.get("entry_price")),
                    str(item.get("exit_date") or ""),
                    _nullable_num(item.get("exit_close")),
                    _nullable_num(item.get("forward_return_pct")),
                    _nullable_num(item.get("max_drawdown_pct")),
                ),
            )
            saved += 1
        self._conn.commit()
        return saved

    def evaluation_report(
        self,
        *,
        horizon_days: int = 3,
        limit: int = 500,
        min_samples: int = 1,
    ) -> dict[str, Any]:
        items = self._load_evaluated_items(horizon_days=horizon_days, limit=limit)
        return {
            "horizon_days": int(horizon_days),
            "limit": int(limit),
            "min_samples": int(min_samples),
            "summary": _report_summary(items),
            "by_action": _group_report(items, lambda item: [str(item.get("action") or "未知")], min_samples),
            "by_score_bucket": _group_report(items, lambda item: [_score_bucket(_num(item.get("score")))], min_samples),
            "by_tag": _group_report(items, _item_tags, min_samples),
            "by_event_sector": _group_report(items, _item_event_sector, min_samples),
        }

    def _load_evaluated_items(self, *, horizon_days: int, limit: int) -> list[dict[str, Any]]:
        cursor = self._conn.execute(
            """SELECT e.*, r.name, r.score, r.tags_json, r.event_factor_json,
                      r.created_at AS decision_at
               FROM quick_decision_evaluations e
               JOIN quick_decision_rows r ON r.id = e.decision_row_id
               WHERE e.horizon_days = ? AND e.status = 'evaluated'
               ORDER BY e.updated_at DESC, e.id DESC
               LIMIT ?""",
            (int(horizon_days), max(1, int(limit))),
        )
        columns = [desc[0] for desc in cursor.description]
        items = []
        for row in cursor.fetchall():
            item = dict(zip(columns, row))
            item["tags"] = _json_list(item.get("tags_json"))
            item["event_factor"] = _json_dict(item.get("event_factor_json"))
            items.append(item)
        return items

    def _evaluate_row(self, row: dict[str, Any], *, horizon_days: int) -> dict[str, Any]:
        symbol = str(row.get("symbol") or "")
        decision_date = str(row.get("created_at") or "")[:10]
        entry_price = _num(row.get("price"))
        item = {
            "id": row.get("id"),
            "run_id": row.get("run_id"),
            "symbol": symbol,
            "name": row.get("name") or symbol,
            "action": row.get("action") or "",
            "score": _num(row.get("score")),
            "entry_price": entry_price,
            "decision_at": row.get("created_at") or "",
            "status": "pending",
            "outcome": "pending",
            "forward_return_pct": None,
            "max_drawdown_pct": None,
            "exit_date": "",
            "exit_close": None,
        }
        if not symbol or not decision_date or entry_price <= 0:
            item["status"] = "no_entry_price"
            item["outcome"] = "unknown"
            return item

        price_rows = self._conn.execute(
            """SELECT trade_date, close, low
               FROM price_daily
               WHERE symbol = ? AND adjust = 'qfq' AND trade_date > ?
               ORDER BY trade_date
               LIMIT ?""",
            (symbol, decision_date, max(1, int(horizon_days))),
        ).fetchall()
        if len(price_rows) < max(1, int(horizon_days)):
            return item

        exit_date, exit_close, _ = price_rows[-1]
        lows = [_num(row_low) or _num(row_close) for _, row_close, row_low in price_rows]
        forward_return = (_num(exit_close) / entry_price - 1.0) * 100
        max_drawdown = (min(lows) / entry_price - 1.0) * 100 if lows else 0.0
        item.update(
            {
                "status": "evaluated",
                "outcome": _classify_outcome(str(row.get("action") or ""), forward_return),
                "forward_return_pct": round(forward_return, 2),
                "max_drawdown_pct": round(max_drawdown, 2),
                "exit_date": exit_date,
                "exit_close": round(_num(exit_close), 3),
            }
        )
        return item


def _classify_outcome(action: str, forward_return_pct: float) -> str:
    if "不买" in action:
        return "avoided" if forward_return_pct <= 0 else "missed"
    if "可" in action or "买" in action:
        return "hit" if forward_return_pct > 0 else "miss"
    return "watch_up" if forward_return_pct > 0 else "watch_down"


def _evaluation_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    evaluated = [item for item in items if item.get("status") == "evaluated"]
    pending = len(items) - len(evaluated)
    buy_like = [item for item in evaluated if item.get("outcome") in {"hit", "miss"}]
    avoid_like = [item for item in evaluated if item.get("outcome") in {"avoided", "missed"}]
    returns = [_num(item.get("forward_return_pct")) for item in evaluated]
    return {
        "total": len(items),
        "evaluated": len(evaluated),
        "pending": pending,
        "avg_forward_return_pct": round(sum(returns) / len(returns), 2) if returns else 0.0,
        "buy_win_rate": _rate(sum(1 for item in buy_like if item.get("outcome") == "hit"), len(buy_like)),
        "avoid_hit_rate": _rate(sum(1 for item in avoid_like if item.get("outcome") == "avoided"), len(avoid_like)),
    }


def _multi_evaluation_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    summaries = [result.get("summary") or {} for result in results]
    return {
        "horizon_count": len(results),
        "total": sum(int(summary.get("total") or 0) for summary in summaries),
        "evaluated": sum(int(summary.get("evaluated") or 0) for summary in summaries),
        "pending": sum(int(summary.get("pending") or 0) for summary in summaries),
    }


def _report_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [_num(item.get("forward_return_pct")) for item in items]
    drawdowns = [_num(item.get("max_drawdown_pct")) for item in items]
    return {
        "count": len(items),
        "success_rate": _rate(sum(1 for item in items if _is_favorable(item)), len(items)),
        "avg_forward_return_pct": round(sum(returns) / len(returns), 2) if returns else 0.0,
        "avg_max_drawdown_pct": round(sum(drawdowns) / len(drawdowns), 2) if drawdowns else 0.0,
    }


def _group_report(items: list[dict[str, Any]], key_fn, min_samples: int) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        keys = key_fn(item)
        for key in keys:
            text = str(key or "").strip()
            if not text:
                continue
            groups.setdefault(text, []).append(item)
    rows = []
    for key, values in groups.items():
        if len(values) < max(1, int(min_samples)):
            continue
        summary = _report_summary(values)
        rows.append(
            {
                "key": key,
                **summary,
                "symbols": _unique_symbols(values),
            }
        )
    return sorted(rows, key=lambda row: (-int(row["count"]), -float(row["avg_forward_return_pct"]), row["key"]))


def _item_tags(item: dict[str, Any]) -> list[str]:
    tags = item.get("tags") or []
    return [str(tag) for tag in tags] or ["无标签"]


def _item_event_sector(item: dict[str, Any]) -> list[str]:
    factor = item.get("event_factor") or {}
    sector = str(factor.get("sector") or "").strip()
    status = str(factor.get("status") or "").strip()
    if not sector:
        return ["无事件因子"]
    return [f"{sector}[{status}]" if status else sector]


def _score_bucket(score: float) -> str:
    if score < 50:
        return "<50"
    if score < 60:
        return "50-59"
    if score < 70:
        return "60-69"
    return "70+"


def _is_favorable(item: dict[str, Any]) -> bool:
    return str(item.get("outcome") or "") in {"hit", "avoided", "watch_up"}


def _unique_symbols(items: list[dict[str, Any]], limit: int = 5) -> list[str]:
    symbols: list[str] = []
    for item in items:
        symbol = str(item.get("symbol") or "")
        if symbol and symbol not in symbols:
            symbols.append(symbol)
        if len(symbols) >= limit:
            break
    return symbols


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    try:
        loaded = json.loads(str(value or "[]"))
    except Exception:
        return []
    return loaded if isinstance(loaded, list) else []


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        loaded = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _unique_positive_ints(values: list[int]) -> list[int]:
    out: list[int] = []
    for value in values:
        try:
            number = int(value)
        except Exception:
            continue
        if number > 0 and number not in out:
            out.append(number)
    return out or [3]


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator * 100, 1) if denominator else 0.0


def _num(value: Any) -> float:
    try:
        if value in (None, "", "-", "None"):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _nullable_num(value: Any) -> float | None:
    if value is None:
        return None
    return _num(value)
