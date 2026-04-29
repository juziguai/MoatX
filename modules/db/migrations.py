"""Schema migration system for warehouse DB."""

from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 10

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
