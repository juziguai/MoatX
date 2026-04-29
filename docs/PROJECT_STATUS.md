# MoatX 项目状态

更新时间：2026-04-26 | 阶段：Beta

## 基准目录

```
D:\Tools\AI\Claude-code\MoatX
```

## 工程化进度

| 维度 | 状态 |
|------|------|
| ruff lint | ✅ 0 警告 |
| 单元测试 | ✅ 94 passed (indicators/utils/config/portfolio/risk_checker) |
| 集成测试 | ✅ 9 passed, 3 skipped (数据源冒烟 + 风控验证) |
| 类型注解 | ✅ 核心模块完整 |
| CI/CD | ✅ GitHub Actions (ruff + pytest + 每日集成测试) |
| 风控层 | ✅ RiskController |
| 文档 | ✅ README + docs/README + PROJECT_STATUS + BETA_PLAN + known_errors |

## 评分演进

| 维度 | Alpha 初 | Alpha 末 | Beta |
|------|----------|----------|------|
| 架构设计 | 4.0 | 4.5 | 4.5 |
| 数据一致性 | 3.0 | 4.0 | 4.5 |
| 错误处理 | 3.5 | 4.0 | 4.5 |
| 性能 | 4.0 | 4.5 | 4.5 |
| 安全性 | 2.5 | 3.5 | 4.0 |
| 代码质量 | 2.5 | 3.5 | 4.0 |
| 可维护性 | 3.5 | 4.0 | 4.5 |
| **综合** | **3.0** | **4.0** | **4.3** |

## 模块清单

| 模块 | 说明 | 状态 |
|------|------|------|
| `modules/stock_data.py` | 数据获取入口（838行） | ✅ |
| `modules/risk_checker.py` | 财务风险检测（269行） | ✅ 新增 |
| `modules/utils.py` | 公共工具函数 | ✅ 新增 |
| `modules/calendar.py` | 交易日历 | ✅ 新增 |
| `modules/logger.py` | 结构化日志 | ✅ 新增 |
| `modules/risk_controller.py` | 止损/仓位/回撤检测 | ✅ |
| `modules/scheduler.py` | 定时任务调度器（7任务） | ✅ |
| `modules/backtest/` | 回测引擎 | ✅ |
| `modules/strategy/` | 策略库 + 参数优化 | ✅ |
| `modules/signal/` | 信号引擎 + 模拟交易 | ✅ |
| `modules/db/` | 数据仓库（SCHEMA_VERSION=6） | ✅ |
| `modules/crawler/` | HTTP 客户端 + 缓存 + 熔断 | ✅ |
| `modules/cli/` | CLI 入口（含 monitor 命令） | ✅ |
| `modules/cli/tool/` | diagnose/probe/signal/paper/monitor | ✅ |

## 数据库

```
data/portfolio.db     # 主库（9 表）：holdings, trades, snapshots, daily_pnl, daily_assets,
                      #   candidates, candidate_results, alert_log, risk_events
data/warehouse.db     # 数据仓库（10 表）：price_daily, indicator_values, backtest_*,
                      #   signal_journal, paper_holdings, paper_trades, paper_daily_snapshots,
                      #   task_execution_log, task_failure_snapshot
```

## CLI 入口

```powershell
python -m modules.cli <subcommand>
python -m modules.cli alert check       # 预警检查
python -m modules.cli quote             # 实时行情
python -m modules.cli monitor           # 健康监控面板
python -m modules.cli tool signal run   # 交易信号
python -m modules.cli tool paper status # 模拟交易
python -m modules.scheduler --list      # 调度任务列表
python -m modules.scheduler --start     # 启动调度器
```

## 数据源

| 数据源 | 状态 | 用途 |
|--------|------|------|
| 腾讯 `qt.gtimg.cn` | ✅ 主源 | 实时行情、日线 |
| 新浪 VIP API | ✅ 主源 | 全市场快照、日线、财务 |
| CNINFO | ✅ | 风险公告 |
| 东方财富 datacenter | ✅ | 个股 F10 |
| 同花顺 THS | ✅ | 行业/概念板块 fallback |

## 已知限制

- 非交易时段 Sina 快照价格为 0，已做兜底（返回上一交易日数据 + 标注）
- EastMoney push2 已废弃，由 THS 替代
- 调度器尚未在真实连续交易日跑过完整周期
- `data/trading_calendar.json` 和 `data/strategy_params.json` 需首次运行对应功能后生成
