# MoatX

MoatX 是一个面向 A 股的本地量化分析与事件情报工具箱。它覆盖行情查询、持仓管理、风控预警、因子评分、模拟交易、宏观事件情报、新闻源采集、板块/产业图谱、事件弹性回测和调度任务。

> 说明：MoatX 只做数据分析、情报提示、模拟交易和辅助决策；不会自动下真实交易订单。项目输出不构成投资建议。

## 当前状态

- **行情链路**：支持新浪、腾讯、东方财富等多源行情；可配置主数据源，也可开启交叉验证聚合。
- **盘中监控**：支持持仓实时估值、市场宽度、盈亏归因、模拟盘快照和事件情报摘要。
- **事件情报**：可采集新闻源，抽取宏观事件，更新事件概率，映射 A 股板块和机会标的，生成报告与推送候选。
- **选股评分**：包含估值、质量、技术、情绪、事件乘数、风控 veto、集中度惩罚等核心因子。
- **模拟交易**：支持纸面账户、信号生成、交易记录、手续费估算、日终快照和调度任务。
- **调度器**：支持候选股扫描、预警、模拟交易、事件采集/抽取/推送等任务。
- **测试与 CI**：GitHub Actions 覆盖 Python 3.10/3.11/3.12；本地当前测试为 `163 passed, 3 skipped`。

## 快速开始

### 环境要求

- Python 3.10+
- Windows PowerShell 或兼容 Shell
- 网络可访问行情/新闻源

### 安装

```powershell
cd D:\Tools\AI\Claude-code\MoatX
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -U pip
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

如果只想按旧方式安装依赖：

```powershell
python -m pip install -r requirements.txt
```

### 查看帮助

```powershell
python -m modules.cli --help
python -m modules.cli tool event --help
python -m modules.scheduler --list
```

## 常用命令

### 行情查询

```powershell
# 查询持仓行情；默认读取本地持仓
python -m modules.cli quote

# 查询个股/ETF
python -m modules.cli quote 600519 000858 510300

# 查询大盘指数
python -m modules.cli market

# 查询市场宽度：上涨/下跌/平盘家数
python -m modules.cli market --breadth

# JSON 输出
python -m modules.cli market 上证指数 深证成指 创业板指 --json
```

行情查询默认通过 `data/moatx.toml` 控制：

```toml
[datasource]
primary = "sina"          # 主数据源
mode = "validate"         # single=单源；validate=主源+校验源交叉验证
validation = ["tencent"]  # 校验源
supplement = ["eastmoney"]# 补充源
```

### 持仓与交易记录

```powershell
# 从截图导入持仓
python -m modules.cli import path\to\screenshot.jpg

# 批量扫描目录导入截图
python -m modules.cli batch-import path\to\folder

# 查看持仓
python -m modules.cli list

# 刷新持仓实时行情
python -m modules.cli refresh

# 持仓总览
python -m modules.cli summary

# 记录买卖流水
python -m modules.cli trade
```

### 风控与预警

```powershell
python -m modules.cli alert check
python -m modules.cli alert history
python -m modules.cli risk check
python -m modules.cli risk status
python -m modules.cli risk history
python -m modules.cli monitor
```

风控能力包括止损、仓位上限、单日亏损、回撤、信号日志、飞书/文件/CLI 推送等。

### 模拟交易与信号

```powershell
python -m modules.cli tool signal list
python -m modules.cli tool paper status
python -m modules.cli tool paper holdings
python -m modules.cli tool paper trades
```

模拟交易只写入本地模拟账户和信号表，不影响真实持仓/真实交易。

### 爬虫与接口探测

```powershell
python -m modules.cli tool diagnose
python -m modules.cli tool probe https://quote.eastmoney.com/sh600988.html --discover --probe-js-apis --json
```

爬虫模块用于合法网页/API 的接口发现、请求复用、字段标准化和批量并发采集。遇到登录、验证码、图形校验等反自动化机制时，项目默认不提供绕过能力，应改用公开接口、授权数据或人工配置。

## 宏观事件情报

事件情报模块负责“新闻源 → 事件信号 → 概率状态 → 产业/板块映射 → 机会标的 → 报告/推送候选”的闭环。

### 核心命令

```powershell
# 采集新闻源
python -m modules.cli tool event collect --json

# 手动注入事件文本
python -m modules.cli tool event ingest --title "伊朗威胁封锁霍尔木兹海峡" --summary "原油供给风险升高"

# 抽取事件信号
python -m modules.cli tool event extract --json

# 更新事件状态
python -m modules.cli tool event states --json

# 扫描 A 股机会
python -m modules.cli tool event opportunities --json --min-probability 0.55

# 生成事件报告
python -m modules.cli tool event report

# 盘中监控摘要：宏观事件 Top3 + 关联板块 + 机会标的
python -m modules.cli tool event summary --json

# 推送检查；默认 dry-run，不真正发送
python -m modules.cli tool event notify --json

# 真正发送推送
python -m modules.cli tool event notify --send --json

# 上下文导出，供其他模型/策略/复盘读取
python -m modules.cli tool event context --json

# 日线事件弹性回测
python -m modules.cli tool event elasticity --event-id hormuz_closure_risk --windows 1,3,5,10 --json

# 一键闭环：采集、抽取、状态、机会、报告，可选推送
python -m modules.cli tool event run --json --notify
```

### 当前信息源

已启用或可用的信息源分为五类：

- **地缘政治**：BBC 中文、RFI 中文、德国之声中文、中国新闻网国际等。
- **能源/商品**：OilPrice、期货日报模板、国家能源局模板等。
- **政策/官方**：央视网、中国人民银行、国家发改委、国家统计局等。
- **财经证券**：财联社 7x24、证券时报、上海证券报、中国新闻网财经等。
- **保留模板**：新华网、新浪、凤凰网、人民网、证监会、自定义 JSON 源等，默认禁用，待验证后开启。

配置文件：`data/event_sources.toml`

### 事件规则与产业图谱

- 事件传导配置：`data/event_transmission_map.toml`
- 旧版事件板块映射：`data/event_sector_map.toml`
- 历史事件样本：`data/event_history.toml`
- 产业/板块图谱：`data/sector_graph.toml`

首批重点方向包括石油、油服、天然气、黄金、贵金属、军工、航运、航空、半导体、芯片、信创、光伏、储能、电力、煤炭、有色等。

## 调度任务

查看任务：

```powershell
python -m modules.scheduler --list
```

后台运行：

```powershell
python -m modules.scheduler --daemon
```

查看后台状态：

```powershell
python -m modules.scheduler --status
```

当前调度覆盖：

- 候选股扫描、待验证标记、收盘验证、残留清理
- 盘中预警、持仓快照、信号生成
- 模拟盘开盘扫描、盘中监控、卖出信号、交易执行、日报
- 宏观事件新闻采集、信号抽取、状态更新、机会扫描、闭环、推送检查

任务开关位于 `data/schedule_config.toml`。

## 项目架构

```text
MoatX
├─ modules/
│  ├─ cli/                         # CLI 入口与工具命令
│  ├─ crawler/                     # 爬虫、API 探测、网页接口分析
│  ├─ db/                          # SQLite 仓储、迁移、任务日志
│  ├─ event_intelligence/          # 宏观事件情报闭环
│  ├─ signal/                      # 信号与模拟交易
│  ├─ strategy/                    # 策略库、参数优化、Walk-Forward
│  ├─ backtest/                    # 回测引擎、费用、指标、数据供给
│  ├─ stock_data.py                # A 股行情/财务/公告数据
│  ├─ datasource.py                # 多源行情聚合与交叉验证
│  ├─ market_index.py              # 大盘指数和市场宽度
│  ├─ sector_tags.py               # 行业/概念/产业标签统一入口
│  ├─ scoring_engine.py            # 核心选股因子评分
│  ├─ event_driver.py              # 事件乘数与个股事件评分
│  ├─ portfolio.py                 # 持仓、交易、快照
│  ├─ risk_controller.py           # 风控检测
│  ├─ alerter.py                   # 飞书/文件/CLI 推送
│  └─ scheduler.py                 # 任务调度器
├─ data/
│  ├─ moatx.toml                   # 主配置
│  ├─ schedule_config.toml         # 调度配置
│  ├─ event_sources.toml           # 新闻源配置
│  ├─ event_transmission_map.toml  # 事件传导规则
│  ├─ sector_graph.toml            # 产业/板块图谱
│  ├─ portfolio.db                 # 持仓主库
│  └─ warehouse.db                 # 行情/事件/信号/回测仓库
├─ docs/                           # 架构、算法、计划、评审文档
├─ scripts/                        # 监控、辅助、比赛脚本
└─ tests/                          # pytest 测试
```

完整架构图、流程图和模块说明见：

- `docs/PROJECT_ARCHITECTURE.md`
- `docs/PROJECT_STATUS.md`
- `docs/EVENT_INTELLIGENCE_IMPL_PLAN.md`
- `docs/EVENT_INTELLIGENCE_ALGORITHM.md`
- `docs/SCORING_ALGORITHM.md`

## 数据库

| 数据库 | 路径 | 主要内容 |
| --- | --- | --- |
| 主库 | `data/portfolio.db` | 持仓、交易流水、快照、候选股、预警 |
| 仓库 | `data/warehouse.db` | OHLCV、事件新闻、事件信号、事件状态、机会、推送冷却、回测、任务日志 |

## Python API 示例

```python
from modules.stock_data import StockData
from modules.indicators import IndicatorEngine
from modules.datasource import QuoteManager
from modules.event_intelligence.summary import build_event_monitor_summary

sd = StockData()
daily = sd.get_daily("600519")

ind = IndicatorEngine()
signals = ind.all_in_one(daily)

quotes = QuoteManager().fetch_quotes(["600519", "000858"])

event_summary = build_event_monitor_summary(top_events=3)
```

## 测试与质量检查

```powershell
ruff check modules/
python -m py_compile modules/config.py modules/scheduler.py modules/scoring_engine.py
python -m pytest -q
```

CI 工作流位于 `.github/workflows/ci.yml`，会在 Python 3.10/3.11/3.12 上运行 lint 与测试。

## 重要边界

- 不自动下真实交易订单。
- 不绕过验证码、登录墙、图形校验或反爬验证。
- 事件情报目前是规则系统，不是外部大模型复杂语义推理。
- 历史弹性回测用于复盘事件传导有效性，不是买卖指令。
- 产业图谱已有首版，但不是完整实时产业链数据库。
