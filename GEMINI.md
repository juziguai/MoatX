# GEMINI.md

This file provides guidance to Gemini when working with code in this repository.

## 项目架构知识图谱

项目使用 Understand-Anything 生成了结构化知识图谱，位于 `.understand-anything/knowledge-graph.json`。
在开始工作前，先读取该文件可以快速理解：
- 82 个节点（模块/文件/配置）的职责和依赖关系
- 86 条边（imports/exports/contains/depends_on）描述的数据流
- 11 层架构分层（data → analysis → strategy → risk → portfolio → event → visualization → cli → backtest → infra → docs）
- 14 步 tour 导览（按顺序阅读可建立完整项目心智模型）

修改代码前，建议先查阅知识图谱了解相关模块的上下游依赖。

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
```

## Architecture

- `modules/` — Python package with CLI, data fetching, analysis, screening, backtest, risk management
- `modules/crawler/` — Multi-source data crawlers (eastmoney, sina, tencent, ths)
- `modules/db/` — SQLite persistence layer
- `modules/cli/` — Click-based CLI with subcommands
- `modules/backtest/` — Backtesting engine
- `data/` — Runtime config, database, logs (gitignored except .toml configs)
- `tests/` — pytest suite
