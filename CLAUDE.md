# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

```powershell
# Run CLI
python -m modules.cli <subcommand>
python -m modules.cli --help

# Run scheduler
python -m modules.scheduler --list   # List tasks
python -m modules.scheduler --start  # Start (foreground)

# Run tests
pytest tests/ -m "not integration"        # Unit tests only (fast)
pytest tests/test_integration.py -v      # Integration tests (network)
pytest tests/test_xxx.py -v             # Single test file
pytest tests/ -k "test_name"            # Single test by name

# Lint (pre-commit)
ruff check modules/

# Install dev dependencies
pip install -e ".[dev]"
```

CI runs `ruff check` then `pytest -m "not integration"` on PR/push. Integration tests run daily at 08:30 UTC.

## Architecture

### Two Independent SQLite Databases

- `data/portfolio.db` — managed by `Portfolio` class directly. Holds live holdings, trades, candidates, snapshots, daily_pnl, alerts.
- `data/warehouse.db` — managed by `DatabaseManager` (WAL mode). Holds OHLCV daily data, backtest runs/optimizations, signal journal, paper holdings/trades/snapshots, task execution logs.

Schema version tracked in `_schema_version` table. Migrations live in `modules/db/migrations.py` (currently v1–v6).

### Lazy-loading Module Exports

`modules/__init__.py` uses `__getattr__` to lazily import heavy modules (StockData, IndicatorEngine, etc.). The CLI `__init__.py` re-exports specific functions — import from the full path for the complete API.

### Three-tier Quote Fallback Chain

`QuoteManager` (via `StockData.get_realtime_quotes`) tries data sources in order: **Tencent → EastMoney → Sina**. Each source only receives stocks not resolved by the previous tier.

### APScheduler Task System

Tasks are defined in `modules/scheduler.py`. Seven weekday tasks run as subprocesses. `TaskFailureTracker` in `warehouse.db` counts consecutive failures. After 3 failures, the job is paused via `scheduler.pause_job()` and a Feishu alert is sent.

### Strategy System

Strategies inherit from `ParametrizedStrategy` (`modules/strategy/base.py`). `set_params()` injects configuration. `optimizer.py` performs grid search and saves best params to `data/strategy_params.json`. `SignalEngine` reads the same JSON to apply optimized params to live signal generation.

## Key Patterns

### Database Access

All DB access through sub-stores on `DatabaseManager`:
```python
db = DatabaseManager(path)
db.price()       # OHLCV storage
db.backtest()    # Backtest runs/optimizations
db.task()        # Execution logs
db.signal()      # Signal journal + paper trading
db.failure_tracker()  # Consecutive failure tracking
```

### Adding a New Database Table

1. Add migration to `modules/db/migrations.py` (increment `SCHEMA_VERSION`)
2. Add corresponding sub-store method in `modules/db/__init__.py` if needed
3. For new store files, register in `DatabaseManager.__init__`

### Adding a New CLI Command

1. Add the command function to the appropriate file under `modules/cli/`
2. Register the parser in `modules/cli/__init__.py main()` with `sub.add_parser()` and `elif cmd == ...` dispatch
3. For tool subcommands, add to `p_tool_sub` and import from `.tool.xxx`

### Config System

`modules/config.py` — priority (highest last): hardcoded defaults → `data/moatx.toml` → `data/feishu.toml` → env vars (`MOATX_SECTION_KEY`) → runtime `config.set()`. Feishu credentials persist via `save()`.

## Critical Conventions

- **Python 3.14 docstring rule**: All docstrings in test files must be English-only. Chinese characters in docstrings cause `SyntaxError` under Python 3.14's stricter Unicode parsing.
- **`_SubprocessResult` placement**: Defined BEFORE task functions in `scheduler.py`. Python resolves type annotations at class definition time — placing it after causes `NameError`.
- **Portfolio API over direct SQL**: Scripts must use `Portfolio` / `CandidateManager` wrapper methods. Direct SQL in scripts breaks the schema contract.
- **`getattr(result, "ok", True)` pattern**: `_SubprocessResult` subprocess wrapped results may not have `.ok` — always use this fallback.
- **Windows PowerShell**: All shell commands use PowerShell syntax (not bash). Paths use backslashes or forward slashes work in Python but PowerShell-native commands use backslashes.
- **`_ensure_columns()` for schema evolution**: `CREATE TABLE IF NOT EXISTS` won't add columns to existing tables. Always follow the CREATE with `_ensure_columns(table, [("new_col", "TYPE DEFAULT ..."), ...])` in `portfolio.py:_init_db()`.
- **Scheduler CLI paths must stay in sync**: The scheduler in `modules/scheduler.py` calls `_run_module("modules.cli_portfolio", args)`. When CLI command names change (e.g. `check` → `alert check`), update the scheduler's arg lists too.
- **Datasource imports after utils extraction**: After `utils.py` was created, datasource files (`datasource.py`, `tencent.py`, `eastmoney.py`) must import functions like `to_sina_code` from `modules.utils`, not rely on them being in-scope.

---

## Karpathy Skills (Behavioral Guidelines)

> Source: forrestchang/andrej-karpathy-skills

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
