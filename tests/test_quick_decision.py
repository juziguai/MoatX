import json
import sqlite3
from datetime import date, datetime, timedelta

from modules.db.migrations import run_migrations
from modules.db.quick_decision_store import QuickDecisionStore
import modules.quick_decision as quick_decision
from modules.quick_decision import (
    evaluate_quick_decisions,
    load_watchlist_symbols,
    review_quick_decisions,
    save_quick_decision,
    summarize_quick_decision_evaluations,
)


def _payload(action="可轻仓观察", price=10.0, symbol="600519", score=70.0, tags=None, event_factor=None):
    return {
        "engine": "quick_decision_v1",
        "generated_at": "2026-06-01T10:00:00",
        "summary": {
            "count": 1,
            "source": "test",
            "warning": "",
            "quote_elapsed_seconds": 0.01,
            "elapsed_seconds": 0.02,
        },
        "decisions": [
            {
                "symbol": symbol,
                "name": "测试股票",
                "action": action,
                "score": score,
                "recommendation": "test",
                "buy_zone": "9.90-10.00",
                "quote": {
                    "price": price,
                    "prev_close": 9.8,
                    "change_pct": 2.04,
                },
                "metrics": {"ma5": 9.9},
                "event_factor": event_factor or {},
                "tags": tags if tags is not None else ["白酒"],
                "reasons": ["站上5日线"],
                "warnings": [],
            }
        ],
    }


def _insert_future_prices(conn):
    rows = [
        ("600519", "2026-06-02", 10.1, 10.3, 9.8, 10.2),
        ("600519", "2026-06-03", 10.2, 10.7, 10.1, 10.6),
        ("600519", "2026-06-04", 10.6, 11.0, 10.5, 10.8),
    ]
    conn.executemany(
        """INSERT INTO price_daily
           (symbol, trade_date, open, high, low, close, adjust)
           VALUES (?, ?, ?, ?, ?, ?, 'qfq')""",
        rows,
    )
    conn.commit()


def _insert_recent_prices(conn):
    rows = []
    for idx in range(8):
        trade_date = (date.today() - timedelta(days=8 - idx)).isoformat()
        close = 9.6 + idx * 0.08
        rows.append(("600519", trade_date, close - 0.03, close + 0.08, close - 0.08, close))
    conn.executemany(
        """INSERT INTO price_daily
           (symbol, trade_date, open, high, low, close, adjust)
           VALUES (?, ?, ?, ?, ?, ?, 'qfq')""",
        rows,
    )
    conn.commit()


def _insert_event_factor(conn):
    conn.execute(
        """INSERT INTO event_news_factors
           (sector, factor_score, direction, insight_count, avg_value_score,
            top_topic, top_titles_json, llm_adjustment, avg_time_decay, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "半导体",
            20.0,
            "bullish",
            4,
            72.0,
            "AI算力",
            "[]",
            1.0,
            0.95,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    conn.commit()


def test_quick_decision_store_records_and_evaluates():
    conn = sqlite3.connect(":memory:")
    try:
        run_migrations(conn)
        _insert_future_prices(conn)
        store = QuickDecisionStore(conn)

        run_id = store.record_run(_payload())
        result = store.evaluate(limit=10, horizon_days=3)

        assert run_id == 1
        assert result["summary"]["evaluated"] == 1
        assert result["summary"]["buy_win_rate"] == 100.0
        item = result["items"][0]
        assert item["outcome"] == "hit"
        assert item["forward_return_pct"] == 8.0
        assert item["max_drawdown_pct"] == -2.0
    finally:
        conn.close()


def test_quick_decision_review_uses_temporary_database(tmp_path):
    db_path = tmp_path / "warehouse.db"
    run_id = save_quick_decision(_payload(action="不买"), db_path=db_path)
    conn = sqlite3.connect(db_path)
    try:
        _insert_future_prices(conn)
    finally:
        conn.close()

    result = review_quick_decisions(["600519"], limit=5, horizon_days=3, db_path=db_path)

    assert run_id == 1
    assert result["summary"]["evaluated"] == 1
    assert result["summary"]["avoid_hit_rate"] == 0.0
    assert result["items"][0]["outcome"] == "missed"


def test_quick_decision_evaluation_persists_multiple_horizons():
    conn = sqlite3.connect(":memory:")
    try:
        run_migrations(conn)
        _insert_future_prices(conn)
        store = QuickDecisionStore(conn)
        store.record_run(_payload())

        result = store.evaluate_many(horizons=[1, 3, 5], limit=10, save=True)
        store.evaluate_many(horizons=[1, 3, 5], limit=10, save=True)

        count = conn.execute("SELECT COUNT(*) FROM quick_decision_evaluations").fetchone()[0]
        rows = conn.execute(
            "SELECT horizon_days, status FROM quick_decision_evaluations ORDER BY horizon_days"
        ).fetchall()
        assert result["saved"] == 3
        assert count == 3
        assert rows == [(1, "evaluated"), (3, "evaluated"), (5, "pending")]
    finally:
        conn.close()


def test_quick_decision_evaluate_helper_saves_to_temporary_database(tmp_path):
    db_path = tmp_path / "warehouse.db"
    save_quick_decision(_payload(), db_path=db_path)
    conn = sqlite3.connect(db_path)
    try:
        _insert_future_prices(conn)
    finally:
        conn.close()

    result = evaluate_quick_decisions(horizons=[1, 3, 5], limit=10, db_path=db_path)
    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM quick_decision_evaluations").fetchone()[0]
    finally:
        conn.close()

    assert result["saved_to_db"] is True
    assert result["saved"] == 3
    assert count == 3


def test_quick_decision_summary_groups_by_action_score_tag_and_event():
    conn = sqlite3.connect(":memory:")
    try:
        run_migrations(conn)
        _insert_future_prices(conn)
        store = QuickDecisionStore(conn)
        store.record_run(
            _payload(
                action="可轻仓观察",
                score=72.0,
                tags=["半导体", "AI"],
                event_factor={"sector": "半导体", "status": "active"},
            )
        )
        store.record_run(
            _payload(
                action="不买",
                score=42.0,
                tags=["白酒"],
                event_factor={},
            )
        )
        store.evaluate_many(horizons=[3], limit=10, save=True)

        report = store.evaluation_report(horizon_days=3, min_samples=1)

        assert report["summary"]["count"] == 2
        assert report["by_action"][0]["key"] in {"不买", "可轻仓观察"}
        assert any(row["key"] == "70+" for row in report["by_score_bucket"])
        assert any(row["key"] == "<50" for row in report["by_score_bucket"])
        assert any(row["key"] == "半导体" for row in report["by_tag"])
        assert any(row["key"] == "半导体[active]" for row in report["by_event_sector"])
    finally:
        conn.close()


def test_quick_decision_summary_helper_reads_temporary_database(tmp_path):
    db_path = tmp_path / "warehouse.db"
    save_quick_decision(
        _payload(tags=["半导体"], event_factor={"sector": "半导体", "status": "active"}),
        db_path=db_path,
    )
    conn = sqlite3.connect(db_path)
    try:
        _insert_future_prices(conn)
    finally:
        conn.close()
    evaluate_quick_decisions(horizons=[3], limit=10, db_path=db_path)

    report = summarize_quick_decision_evaluations(horizon_days=3, db_path=db_path)

    assert report["summary"]["count"] == 1
    assert report["by_tag"][0]["key"] == "半导体"


def test_quick_decision_applies_active_event_factor(tmp_path, monkeypatch):
    db_path = tmp_path / "warehouse.db"
    conn = sqlite3.connect(db_path)
    try:
        run_migrations(conn)
        _insert_recent_prices(conn)
        _insert_event_factor(conn)
    finally:
        conn.close()

    def fake_fetch_quotes(symbols, *, source, timeout):
        return (
            {
                "600519": {
                    "code": "600519",
                    "name": "测试股票",
                    "price": 10.2,
                    "prev_close": 10.0,
                    "change_pct": 2.0,
                    "open": 10.0,
                    "high": 10.3,
                    "low": 9.95,
                }
            },
            "test",
            "",
        )

    monkeypatch.setattr(quick_decision, "_fetch_quotes", fake_fetch_quotes)
    monkeypatch.setattr(quick_decision, "_code_tags", lambda symbols: {"600519": ["半导体"]})

    without_factor = quick_decision.build_quick_decision(
        ["600519"],
        include_event_factors=False,
        db_path=db_path,
    )
    with_factor = quick_decision.build_quick_decision(["600519"], db_path=db_path)

    row = with_factor["decisions"][0]
    assert row["event_factor"]["sector"] == "半导体"
    assert row["event_factor"]["status"] == "active"
    assert row["score"] > without_factor["decisions"][0]["score"]
    assert any("事件因子强势" in reason for reason in row["reasons"])


def test_load_watchlist_symbols_reads_positions_and_candidates(tmp_path):
    path = tmp_path / "watchlist.json"
    path.write_text(
        json.dumps(
            {
                "positions": [{"symbol": "600519.SH"}, {"code": "000001"}],
                "raw_candidates": [{"stock_code": "600519"}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    assert load_watchlist_symbols(path) == ["600519", "000001"]
