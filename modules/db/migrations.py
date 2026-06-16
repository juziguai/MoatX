"""Schema migration system for warehouse DB."""

from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 18

MIGRATIONS: dict[int, list[str]] = {
    1: [
        """CREATE TABLE IF NOT EXISTS price_daily (
            symbol TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            turn REAL,
            pct_change REAL,
            adjust TEXT DEFAULT 'qfq',
            PRIMARY KEY (symbol, trade_date, adjust)
        )""",
        """CREATE TABLE IF NOT EXISTS indicator_values (
            symbol TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            name TEXT NOT NULL,
            value REAL,
            PRIMARY KEY (symbol, trade_date, name)
        )""",
    ],
    2: [
        """CREATE TABLE IF NOT EXISTS backtest_strategies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            class_path TEXT NOT NULL,
            params TEXT DEFAULT '{}',
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        )""",
        """CREATE TABLE IF NOT EXISTS backtest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id INTEGER REFERENCES backtest_strategies(id),
            symbols TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            initial_capital REAL DEFAULT 100000,
            results TEXT,
            equity_curve TEXT,
            duration_ms INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        )""",
        """CREATE TABLE IF NOT EXISTS backtest_optimizations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_name TEXT NOT NULL,
            symbols TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            target_metric TEXT DEFAULT 'sharpe_ratio',
            parameter_count INTEGER DEFAULT 0,
            best_params TEXT,
            best_result TEXT,
            total_runs INTEGER DEFAULT 0,
            duration_ms INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        )""",
    ],
    3: [
        """CREATE TABLE IF NOT EXISTS task_execution_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            task_name TEXT NOT NULL,
            started_at TEXT DEFAULT (datetime('now', 'localtime')),
            finished_at TEXT,
            success INTEGER DEFAULT 0,
            output TEXT DEFAULT '',
            error TEXT DEFAULT '',
            duration_ms INTEGER DEFAULT 0
        )""",
    ],
    4: [
        """CREATE TABLE IF NOT EXISTS signal_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            price REAL,
            strategy_name TEXT DEFAULT 'moatx',
            confidence REAL DEFAULT 0,
            reason TEXT DEFAULT '',
            indicators TEXT DEFAULT '{}',
            executed INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        )""",
        """CREATE TABLE IF NOT EXISTS paper_holdings (
            symbol TEXT PRIMARY KEY,
            name TEXT DEFAULT '',
            shares INTEGER DEFAULT 0,
            avg_cost REAL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        )""",
        """CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            price REAL,
            shares INTEGER DEFAULT 0,
            value REAL DEFAULT 0,
            fee REAL DEFAULT 0,
            reason TEXT DEFAULT '',
            signal_id INTEGER,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        )""",
    ],
    5: [
        """CREATE TABLE IF NOT EXISTS task_failure_snapshot (
            task_id TEXT PRIMARY KEY,
            consecutive_failures INTEGER DEFAULT 0,
            last_failure_at TEXT,
            last_error TEXT,
            paused INTEGER DEFAULT 0,
            paused_at TEXT,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        )""",
    ],
    6: [
        """CREATE TABLE IF NOT EXISTS paper_daily_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL,
            total_value REAL NOT NULL,
            cash REAL NOT NULL,
            market_value REAL NOT NULL,
            holdings_detail TEXT DEFAULT '[]',
            positions_detail TEXT DEFAULT '[]',
            total_return_pct REAL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now', 'localtime')),
            UNIQUE(snapshot_date)
        )""",
    ],
    7: [
        """CREATE TABLE IF NOT EXISTS event_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT DEFAULT '',
            url TEXT,
            published_at TEXT,
            fetched_at TEXT NOT NULL,
            language TEXT DEFAULT 'zh',
            raw_hash TEXT NOT NULL UNIQUE,
            processed INTEGER DEFAULT 0
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_news_processed
           ON event_news(processed, id)""",
        """CREATE TABLE IF NOT EXISTS event_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            news_id INTEGER,
            event_type TEXT,
            entities_json TEXT DEFAULT '{}',
            matched_keywords TEXT DEFAULT '',
            matched_actions TEXT DEFAULT '',
            severity REAL DEFAULT 0,
            confidence REAL DEFAULT 0,
            direction TEXT DEFAULT 'neutral',
            created_at TEXT NOT NULL,
            FOREIGN KEY(news_id) REFERENCES event_news(id)
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_signals_event
           ON event_signals(event_id, created_at)""",
        """CREATE TABLE IF NOT EXISTS event_states (
            event_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            probability REAL DEFAULT 0,
            impact_strength REAL DEFAULT 0,
            status TEXT DEFAULT 'watching',
            evidence_count INTEGER DEFAULT 0,
            sources_count INTEGER DEFAULT 0,
            last_signal_at TEXT,
            updated_at TEXT NOT NULL
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_states_status
           ON event_states(status, probability)""",
        """CREATE TABLE IF NOT EXISTS event_opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT DEFAULT '',
            sector_tags TEXT DEFAULT '',
            opportunity_score REAL DEFAULT 0,
            event_score REAL DEFAULT 0,
            exposure_score REAL DEFAULT 0,
            underpricing_score REAL DEFAULT 0,
            timing_score REAL DEFAULT 0,
            risk_penalty REAL DEFAULT 0,
            recommendation TEXT DEFAULT '',
            evidence_json TEXT DEFAULT '{}',
            created_at TEXT NOT NULL
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_opportunities_event
           ON event_opportunities(event_id, opportunity_score)""",
    ],
    8: [
        """CREATE TABLE IF NOT EXISTS event_notifications (
            event_id TEXT PRIMARY KEY,
            report_hash TEXT NOT NULL,
            last_sent_at TEXT,
            cooldown_until TEXT,
            status TEXT DEFAULT 'pending',
            last_probability REAL DEFAULT 0,
            last_opportunity_score REAL DEFAULT 0,
            updated_at TEXT NOT NULL
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_notifications_cooldown
           ON event_notifications(status, cooldown_until)""",
    ],
    9: [
        """CREATE TABLE IF NOT EXISTS event_source_quality (
            source_id TEXT PRIMARY KEY,
            name TEXT DEFAULT '',
            category TEXT DEFAULT 'general',
            type TEXT DEFAULT '',
            enabled INTEGER DEFAULT 0,
            fetched INTEGER DEFAULT 0,
            inserted INTEGER DEFAULT 0,
            duplicates INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0,
            signal_hits INTEGER DEFAULT 0,
            hit_rate REAL DEFAULT 0,
            last_success_at TEXT,
            last_error TEXT DEFAULT '',
            updated_at TEXT NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS event_backtest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT DEFAULT '',
            windows_json TEXT DEFAULT '[]',
            trigger_count INTEGER DEFAULT 0,
            sample_count INTEGER DEFAULT 0,
            summary_json TEXT DEFAULT '{}',
            created_at TEXT NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS event_elasticity_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            event_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT DEFAULT '',
            trigger_date TEXT NOT NULL,
            entry_date TEXT,
            window_days INTEGER NOT NULL,
            entry_close REAL,
            exit_date TEXT,
            exit_close REAL,
            forward_return REAL DEFAULT 0,
            benchmark_return REAL DEFAULT 0,
            excess_return REAL DEFAULT 0,
            max_drawdown REAL DEFAULT 0,
            success INTEGER DEFAULT 0,
            source TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES event_backtest_runs(id)
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_elasticity_event
           ON event_elasticity_samples(event_id, symbol, trigger_date)""",
    ],
    10: [
        """ALTER TABLE event_source_quality
           ADD COLUMN quality_score REAL DEFAULT 0""",
        """ALTER TABLE event_source_quality
           ADD COLUMN reliability TEXT DEFAULT 'unknown'""",
    ],
    11: [
        """CREATE TABLE IF NOT EXISTS event_topic_memory (
            topic TEXT PRIMARY KEY,
            category TEXT DEFAULT '',
            heat REAL DEFAULT 0,
            previous_heat REAL DEFAULT 0,
            momentum REAL DEFAULT 0,
            insight_count INTEGER DEFAULT 0,
            total_insight_count INTEGER DEFAULT 0,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            sectors_json TEXT DEFAULT '[]',
            top_titles_json TEXT DEFAULT '[]',
            trend TEXT DEFAULT 'stable',
            updated_at TEXT NOT NULL
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_topic_memory_heat
           ON event_topic_memory(heat DESC, momentum DESC)""",
        """CREATE TABLE IF NOT EXISTS event_topic_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL,
            category TEXT DEFAULT '',
            heat REAL DEFAULT 0,
            insight_count INTEGER DEFAULT 0,
            sectors_json TEXT DEFAULT '[]',
            top_titles_json TEXT DEFAULT '[]',
            created_at TEXT NOT NULL
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_topic_snapshots_topic
           ON event_topic_snapshots(topic, created_at)""",
    ],
    12: [
        """CREATE TABLE IF NOT EXISTS event_llm_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            news_id INTEGER DEFAULT 0,
            title TEXT DEFAULT '',
            topic TEXT DEFAULT '',
            value_score REAL DEFAULT 0,
            llm_score REAL DEFAULT 0,
            decision TEXT DEFAULT '',
            rationale TEXT DEFAULT '',
            review_json TEXT DEFAULT '{}',
            created_at TEXT NOT NULL
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_llm_reviews_topic
           ON event_llm_reviews(topic, created_at)""",
        """CREATE INDEX IF NOT EXISTS idx_event_llm_reviews_score
           ON event_llm_reviews(llm_score DESC, created_at DESC)""",
    ],
    13: [
        """CREATE TABLE IF NOT EXISTS event_news_insights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            news_id INTEGER NOT NULL,
            source TEXT DEFAULT '',
            title TEXT DEFAULT '',
            topic TEXT NOT NULL,
            category TEXT DEFAULT '',
            value_score REAL DEFAULT 0,
            sentiment TEXT DEFAULT 'neutral',
            time_horizon TEXT DEFAULT 'mid',
            affected_sectors_json TEXT DEFAULT '[]',
            affected_stocks_json TEXT DEFAULT '[]',
            reason TEXT DEFAULT '',
            llm_score REAL DEFAULT 0,
            llm_decision TEXT DEFAULT '',
            llm_rationale TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(news_id, topic)
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_news_insights_topic
           ON event_news_insights(topic, value_score DESC)""",
        """CREATE INDEX IF NOT EXISTS idx_event_news_insights_score
           ON event_news_insights(value_score DESC, updated_at DESC)""",
        """CREATE TABLE IF NOT EXISTS event_news_topic_events (
            topic TEXT PRIMARY KEY,
            category TEXT DEFAULT '',
            heat REAL DEFAULT 0,
            confidence REAL DEFAULT 0,
            market_relevance REAL DEFAULT 0,
            direction TEXT DEFAULT 'neutral',
            insight_count INTEGER DEFAULT 0,
            affected_sectors_json TEXT DEFAULT '[]',
            latest_news_json TEXT DEFAULT '[]',
            updated_at TEXT NOT NULL
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_news_topic_events_heat
           ON event_news_topic_events(heat DESC, updated_at DESC)""",
        """CREATE TABLE IF NOT EXISTS event_news_factors (
            sector TEXT PRIMARY KEY,
            factor_score REAL DEFAULT 0,
            direction TEXT DEFAULT 'neutral',
            insight_count INTEGER DEFAULT 0,
            avg_value_score REAL DEFAULT 0,
            top_topic TEXT DEFAULT '',
            top_titles_json TEXT DEFAULT '[]',
            llm_adjustment REAL DEFAULT 1,
            updated_at TEXT NOT NULL
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_news_factors_score
           ON event_news_factors(factor_score DESC, updated_at DESC)""",
    ],
    14: [
        """ALTER TABLE event_news_factors ADD COLUMN avg_time_decay REAL DEFAULT 1""",
        """CREATE TABLE IF NOT EXISTS event_news_factor_snapshots (
            snapshot_date TEXT NOT NULL,
            sector TEXT NOT NULL,
            factor_score REAL DEFAULT 0,
            direction TEXT DEFAULT 'neutral',
            insight_count INTEGER DEFAULT 0,
            avg_value_score REAL DEFAULT 0,
            top_topic TEXT DEFAULT '',
            top_titles_json TEXT DEFAULT '[]',
            llm_adjustment REAL DEFAULT 1,
            avg_time_decay REAL DEFAULT 1,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (snapshot_date, sector)
        )""",
        """CREATE INDEX IF NOT EXISTS idx_event_news_factor_snapshots_date_score
           ON event_news_factor_snapshots(snapshot_date, factor_score DESC)""",
    ],
    15: [
        """CREATE TABLE IF NOT EXISTS source_health_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            healthy INTEGER NOT NULL DEFAULT 0,
            latency_ms REAL DEFAULT 0,
            error TEXT DEFAULT '',
            sample_count INTEGER DEFAULT 0,
            checked_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        )""",
        """CREATE INDEX IF NOT EXISTS idx_source_health_source ON source_health_log(source)""",
        """CREATE INDEX IF NOT EXISTS idx_source_health_checked ON source_health_log(checked_at)""",
    ],
    16: [
        """CREATE TABLE IF NOT EXISTS quick_decision_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            engine TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            source TEXT DEFAULT '',
            warning TEXT DEFAULT '',
            symbol_count INTEGER DEFAULT 0,
            quote_elapsed_seconds REAL DEFAULT 0,
            elapsed_seconds REAL DEFAULT 0,
            symbols_json TEXT DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        )""",
        """CREATE INDEX IF NOT EXISTS idx_quick_decision_runs_generated
           ON quick_decision_runs(generated_at DESC)""",
        """CREATE TABLE IF NOT EXISTS quick_decision_rows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT DEFAULT '',
            action TEXT DEFAULT '',
            score REAL DEFAULT 0,
            price REAL DEFAULT 0,
            prev_close REAL DEFAULT 0,
            change_pct REAL DEFAULT 0,
            buy_zone TEXT DEFAULT '',
            recommendation TEXT DEFAULT '',
            quote_json TEXT DEFAULT '{}',
            metrics_json TEXT DEFAULT '{}',
            tags_json TEXT DEFAULT '[]',
            reasons_json TEXT DEFAULT '[]',
            warnings_json TEXT DEFAULT '[]',
            created_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES quick_decision_runs(id)
        )""",
        """CREATE INDEX IF NOT EXISTS idx_quick_decision_rows_symbol
           ON quick_decision_rows(symbol, created_at DESC)""",
        """CREATE INDEX IF NOT EXISTS idx_quick_decision_rows_run
           ON quick_decision_rows(run_id)""",
    ],
    17: [
        """ALTER TABLE quick_decision_rows
           ADD COLUMN event_factor_json TEXT DEFAULT '{}'""",
    ],
    18: [
        """CREATE TABLE IF NOT EXISTS quick_decision_evaluations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_row_id INTEGER NOT NULL,
            horizon_days INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            action TEXT DEFAULT '',
            status TEXT DEFAULT 'pending',
            outcome TEXT DEFAULT 'pending',
            entry_price REAL DEFAULT 0,
            exit_date TEXT DEFAULT '',
            exit_close REAL,
            forward_return_pct REAL,
            max_drawdown_pct REAL,
            evaluated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY(decision_row_id) REFERENCES quick_decision_rows(id),
            UNIQUE(decision_row_id, horizon_days)
        )""",
        """CREATE INDEX IF NOT EXISTS idx_quick_decision_eval_symbol
           ON quick_decision_evaluations(symbol, horizon_days, updated_at DESC)""",
        """CREATE INDEX IF NOT EXISTS idx_quick_decision_eval_status
           ON quick_decision_evaluations(horizon_days, status, updated_at DESC)""",
    ],
}


def run_migrations(conn: sqlite3.Connection) -> None:
    """Run all pending migrations in order."""
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS _schema_version (version INTEGER PRIMARY KEY)"
    )
    cursor.execute("SELECT MAX(version) FROM _schema_version")
    row = cursor.fetchone()
    current_version = row[0] if row[0] is not None else 0

    for ver in range(current_version + 1, SCHEMA_VERSION + 1):
        statements = MIGRATIONS.get(ver, [])
        for stmt in statements:
            cursor.execute(stmt)
        cursor.execute("INSERT INTO _schema_version (version) VALUES (?)", (ver,))
    conn.commit()
