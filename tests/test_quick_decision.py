import json
import sqlite3
from datetime import date, datetime, timedelta

from modules.db.migrations import run_migrations
from modules.db.quick_decision_store import QuickDecisionStore
import modules.quick_decision as quick_decision
from modules.quick_decision import (
    backfill_quick_decision_samples,
    collect_sample_symbols,
    evaluate_quick_decisions,
    learn_quick_decision,
    load_watchlist_symbols,
    review_quick_decisions,
    sample_quick_decisions,
    save_quick_decision,
    summarize_quick_decision_evaluations,
)
from modules.config import QuickDecisionSettings


def _payload(
    action="可轻仓观察",
    price=10.0,
    symbol="600519",
    score=70.0,
    tags=None,
    event_factor=None,
    generated_at="2026-06-01T10:00:00",
):
    return {
        "engine": "quick_decision_v1",
        "generated_at": generated_at,
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


def _insert_future_prices_for(conn, symbol, closes):
    rows = []
    for idx, close in enumerate(closes, 2):
        rows.append((symbol, f"2026-06-{idx:02d}", close, close + 0.1, close - 0.1, close))
    conn.executemany(
        """INSERT INTO price_daily
           (symbol, trade_date, open, high, low, close, adjust)
           VALUES (?, ?, ?, ?, ?, ?, 'qfq')""",
        rows,
    )
    conn.commit()


def _insert_historical_replay_prices(conn, symbol="600519"):
    trade_dates = [
        "2026-05-20",
        "2026-05-21",
        "2026-05-22",
        "2026-05-25",
        "2026-05-26",
        "2026-05-27",
        "2026-05-28",
        "2026-05-29",
        "2026-06-01",
        "2026-06-02",
        "2026-06-03",
    ]
    rows = []
    for idx, trade_date in enumerate(trade_dates):
        close = 10.0 + idx * 0.1
        rows.append((symbol, trade_date, close - 0.05, close + 0.1, close - 0.1, close, 1000000, 10000000, 1.2, 1.0))
    conn.executemany(
        """INSERT INTO price_daily
           (symbol, trade_date, open, high, low, close, volume, amount, turn, pct_change, adjust)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'qfq')""",
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


def test_quick_decision_uses_configurable_weights(tmp_path, monkeypatch):
    db_path = tmp_path / "warehouse.db"
    conn = sqlite3.connect(db_path)
    try:
        run_migrations(conn)
        _insert_recent_prices(conn)
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
    monkeypatch.setattr(quick_decision, "_code_tags", lambda symbols: {"600519": []})
    monkeypatch.setattr(quick_decision, "_quick_settings", lambda: QuickDecisionSettings())
    default_payload = quick_decision.build_quick_decision(["600519"], db_path=db_path)

    custom_settings = QuickDecisionSettings(
        pct_neutral_bonus=25.0,
        calm_bonus=12.0,
        buy_score_threshold=90.0,
        watch_score_threshold=70.0,
    )
    monkeypatch.setattr(quick_decision, "_quick_settings", lambda: custom_settings)
    custom_payload = quick_decision.build_quick_decision(["600519"], db_path=db_path)

    assert custom_payload["decisions"][0]["score"] > default_payload["decisions"][0]["score"]
    assert custom_payload["decisions"][0]["action"] == "可轻仓观察"


def test_collect_sample_symbols_merges_sources(monkeypatch):
    monkeypatch.setattr(quick_decision, "load_watchlist_symbols", lambda path=None: ["600519", "000001"])
    monkeypatch.setattr(quick_decision, "_load_fusion_candidate_symbols", lambda **kwargs: ["000001", "300001"])
    monkeypatch.setattr(quick_decision, "_load_event_opportunity_symbols", lambda **kwargs: ["600000"])

    result = collect_sample_symbols(sources=["watchlist", "fusion", "event"], limit=4)

    assert result["symbols"] == ["600519", "000001", "300001", "600000"]
    assert result["source_counts"] == {"watchlist": 2, "fusion": 2, "event": 1}


def test_sample_quick_decisions_respects_same_day_limit(tmp_path, monkeypatch):
    db_path = tmp_path / "warehouse.db"
    today = f"{date.today().isoformat()}T10:00:00"
    save_quick_decision(_payload(generated_at=today), db_path=db_path)
    captured = {}

    monkeypatch.setattr(
        quick_decision,
        "collect_sample_symbols",
        lambda **kwargs: {
            "sources": ["watchlist"],
            "symbols": ["600519", "000001"],
            "source_counts": {"watchlist": 2},
            "warnings": [],
        },
    )

    def fake_build(symbols, **kwargs):
        captured["symbols"] = list(symbols)
        return {
            "engine": "quick_decision_v1",
            "generated_at": today,
            "summary": {
                "count": len(symbols),
                "source": "test",
                "warning": "",
                "quote_elapsed_seconds": 0.01,
                "elapsed_seconds": 0.02,
            },
            "decisions": [
                _payload(symbol=symbol, generated_at=today)["decisions"][0]
                for symbol in symbols
            ],
        }

    monkeypatch.setattr(quick_decision, "build_quick_decision", fake_build)

    result = sample_quick_decisions(max_per_symbol_per_day=1, db_path=db_path)

    assert captured["symbols"] == ["000001"]
    assert result["summary"]["sample"]["skipped_same_day_limit"] == 1
    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM quick_decision_rows").fetchone()[0]
    finally:
        conn.close()
    assert count == 2


def test_learn_quick_decision_reports_diagnostics_and_config_suggestion(tmp_path):
    db_path = tmp_path / "warehouse.db"
    save_quick_decision(
        _payload(
            symbol="000001",
            action="可轻仓观察",
            score=72.0,
            tags=["AI"],
            event_factor={"sector": "AI", "status": "active"},
        ),
        db_path=db_path,
    )
    conn = sqlite3.connect(db_path)
    try:
        _insert_future_prices_for(conn, "000001", [9.8, 9.7, 9.6])
    finally:
        conn.close()
    evaluate_quick_decisions(horizons=[3], limit=10, db_path=db_path)

    result = learn_quick_decision(horizon_days=3, min_samples=1, suggest_config=True, db_path=db_path)

    assert result["diagnostics"]["low_win_rate_actions"][0]["key"] == "可轻仓观察"
    assert result["diagnostics"]["losing_score_buckets"][0]["key"] == "70+"
    assert result["diagnostics"]["weak_event_factors"][0]["key"] == "AI[active]"
    assert "buy_score_threshold" in result["suggested_config"]["toml"]
    assert result["suggested_config"]["changes"][0]["evidence_samples"] >= 1


def test_backfill_quick_decision_samples_saves_and_evaluates(tmp_path):
    db_path = tmp_path / "warehouse.db"
    conn = sqlite3.connect(db_path)
    try:
        run_migrations(conn)
        _insert_historical_replay_prices(conn)
    finally:
        conn.close()

    result = backfill_quick_decision_samples(
        ["600519"],
        start_date="2026-05-26",
        end_date="2026-05-29",
        include_event_factors=False,
        evaluate_horizons=[3],
        db_path=db_path,
    )
    duplicate = backfill_quick_decision_samples(
        ["600519"],
        start_date="2026-05-26",
        end_date="2026-05-29",
        include_event_factors=False,
        db_path=db_path,
    )

    assert result["summary"]["source"] == "manual"
    assert result["summary"]["rows"] == 4
    assert result["summary"]["runs"] == 4
    assert result["evaluation"]["summary"]["evaluated"] == 4
    assert duplicate["summary"]["rows"] == 0
    assert duplicate["summary"]["skipped_duplicate"] == 4
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT engine, source, symbol_count FROM quick_decision_runs ORDER BY id LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert row == ("quick_decision_historical_replay_v1", "historical_replay:manual", 1)
