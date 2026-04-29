# MoatX

A 股量化分析 CLI 工具，支持行情抓取、技术指标、选股筛选、持仓管理、风控预警、信号回测。

## 安装

```powershell
cd D:\Tools\AI\Claude-code\MoatX
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

依赖：akshare, pandas, numpy, requests, lxml, matplotlib, scipy, pyyaml, pytest, ruff

## CLI 入口

```powershell
python -m modules.cli --help
```

所有子命令通过 `python -m modules.cli <subcommand>` 调用。

### 实时行情

```powershell
python -m modules.cli quote              # 自动读取持仓，多源校验
python -m modules.cli quote 600519 000858 # 个股行情，多源校验
python -m modules.cli market             # 大盘指数，多源校验
python -m modules.cli market --breadth   # 全市场上涨/下跌/平盘家数
python -m modules.cli market 科创50 沪深300 --json
```

### 持仓管理

```powershell
python -m modules.cli list                # 查看持仓
python -m modules.cli config              # 查看/修改配置
python -m modules.cli refresh             # 刷新持仓实时行情
```

### 预警与风控

```powershell
python -m modules.cli check               # 运行预警检测
python -m modules.cli risk check          # 手动触发风控检查
python -m modules.cli risk status         # 查看当前风控状态
python -m modules.cli risk history        # 风控事件历史
python -m modules.cli alerts --limit 50  # 预警历史
```

### 信号与模拟交易

```powershell
python -m modules.cli signal list         # 查看信号记录
python -m modules.cli paper status        # 模拟账户状态
python -m modules.cli paper holdings      # 模拟持仓
python -m modules.cli paper trades        # 模拟交易记录
```

### 诊断工具

```powershell
python -m modules.cli diagnose            # 数据源诊断
python -m modules.cli probe-api URL      # API 探测
```

### 宏观事件情报

当前宏观事件情报模块约 95% 完成，已进入可用级收工状态：可持续采集新闻、抽取宏观事件、更新事件概率、映射 A 股机会、生成报告、执行推送冷却和调度任务；模块只输出情报与机会，不自动下单。

```powershell
python -m modules.cli tool event collect --json
python -m modules.cli tool event ingest --title "伊朗威胁封锁霍尔木兹海峡" --summary "原油供给风险升高"
python -m modules.cli tool event run --json --min-probability 1.0
python -m modules.cli tool event summary --json         # 盘中宏观事件 Top3
python -m modules.cli tool event notify --json          # dry-run，不发送
python -m modules.cli tool event notify --send --json   # 显式发送
python -m modules.cli tool event sources --json         # 查看源配置与质量
python -m modules.cli tool event context --json         # 下游模型/交易/回测上下文
python -m modules.cli tool event elasticity --event-id hormuz_closure_risk --windows 1,3,5,10 --json
python -m modules.scheduler --list                      # 查看事件调度状态
python -m modules.scheduler --daemon                    # 后台持续运行采集/抽取/推送
python -m modules.scheduler --status                    # 查看后台调度器进程
```

事件新闻源已启用 BBC 中文、RFI 中文、DW 中文、OilPrice、中国新闻网国内/国际/财经 RSS、财联社 7x24 电报、央视网新闻 JSONP、证券时报要闻 HTML、中国人民银行新闻 HTML、国家发改委新闻/政策、国家统计局数据发布、上海证券报首页、期货日报首页等 16 个源；新华、新浪、凤凰、人民网、国家能源局、证监会、自定义 JSON 作为禁用模板保留。事件调度任务已启用，`--daemon` 可在 Windows/本机后台持续运行。推送阈值在 `data/moatx.toml` 的 `[event_intelligence]` 中配置，默认 `probability >= 0.55` 或 `opportunity_score >= 75` 提醒，冷却 6 小时。盘中模拟监控会附带“宏观事件 Top3 + 关联板块 + 机会标的”。源质量已按抓取量、错误率、命中率生成 `quality_score/reliability` 和 `source_recommendation` 治理建议，报告含最新证据链。规则 NLP 已区分传闻、升级、确认、否认、缓和；抽取层默认跳过发布日期超过 14 天的旧新闻，避免历史网页误触发当前事件。事件弹性回测使用日线窗口，已内置霍尔木兹、原油、黄金、红海、俄乌、芯片制裁、关税、国内宽松历史样本，不触发自动交易。

## 项目结构

完整架构图、流程图、功能模块和数据流说明见 `docs/PROJECT_ARCHITECTURE.md`。

```
modules/
├── __init__.py              # 懒加载导出
├── __main__.py              # python -m modules 入口
├── config.py                 # 配置管理（moatx.toml + 环境变量）
├── candidate.py              # 候选股管理
├── stock_data.py             # 行情、财务、公告抓取
├── datasource.py             # 个股行情多源校验聚合
├── indicators.py             # 技术指标引擎（KDJ/RSI/BOLL/MACD 等）
├── analyzer.py              # 单股分析 + Markdown 报告
├── charts.py                 # K 线图渲染
├── screener.py               # 选股器（全市场扫描）
├── rank_engine.py            # 综合评分引擎
├── portfolio.py              # 持仓/交易/快照管理
├── alert_manager.py          # 预警检测逻辑
├── alerter.py               # 飞书/文件/CLI 推送
├── risk_controller.py        # 风控检测（止损/仓位/回撤）
├── scheduler.py              # 定时任务调度器
├── backtest/
│   ├── engine.py             # 回测引擎
│   ├── strategy.py           # 策略上下文
│   ├── fees.py               # 手续费计算
│   ├── metrics.py            # 回测指标
│   └── datafeed.py           # 回测数据供给
├── strategy/
│   ├── base.py               # ParametrizedStrategy 基类
│   ├── library.py            # 内置策略（MA Cross / MeanReversion / Breakout 等）
│   ├── optimizer.py           # 参数网格优化
│   ├── comparator.py          # 多策略对比
│   └── walkforward.py         # Walk-Forward 分析
├── signal/
│   ├── engine.py             # 信号生成引擎
│   ├── journal.py             # 信号日志
│   └── paper_trader.py       # 模拟交易
├── db/
│   ├── __init__.py           # DatabaseManager 外观类
│   ├── migrations.py         # Schema 迁移
│   ├── price_store.py        # OHLCV 行情存储
│   ├── backtest_store.py     # 回测记录存储
│   ├── signal_store.py       # 信号/模拟交易存储
│   └── task_log.py           # 调度任务日志
└── cli/
    ├── __init__.py           # CLI 入口（quote/list/check 等）
    ├── __main__.py           # python -m modules.cli 入口
    ├── alerter.py            # 飞书推送
    ├── portfolio.py          # 持仓命令
    ├── risk.py               # 风控命令
    ├── scheduler_cli.py      # 调度命令
    ├── quote.py              # 行情命令
    └── tool/
        ├── diagnose.py        # 诊断
        ├── probe.py           # API 探测
        ├── signal.py          # 信号
        └── paper.py           # 模拟交易

data/
├── portfolio.db              # 主数据库（持仓/交易/预警）
└── warehouse.db              # 数据仓库（行情/回测/信号/日志）

tests/                        # 单元测试（pytest）
scripts/                      # 辅助脚本
docs/                         # 设计文档
```

## 数据库

| 数据库 | 路径 | 内容 |
|--------|------|------|
| 主库 | `data/portfolio.db` | 持仓、交易流水、快照、候选股、预警 |
| 数据仓库 | `data/warehouse.db` | OHLCV 行情、回测记录、信号日志、任务日志 |

## 数据源架构

```
业务层 (stock_data.get_realtime_quotes)
    └─→ QuoteManager
         ├─ Tencent   ── 查询
         ├─ EastMoney ── 查询
         └─ Sina      ── 查询
              ↓
          统一字段 → 交叉校验 → 聚合输出
```

默认多源查询并返回 `validation_status/source_quotes/sources` 等校验信息；单源失败时自动降级为剩余可用源。

## 内置策略

```python
from modules.strategy.library import MovingAverageCross, BreakoutStrategy

strategy = MovingAverageCross()
strategy.fast_period = 5
strategy.slow_period = 20
strategy.position_pct = 0.8
```

支持：MovingAverageCross、MeanReversion、TrendFollowing、BreakoutStrategy、MACrossWithVolume

## Python API

```python
from modules.stock_data import StockData
from modules.indicators import IndicatorEngine
from modules.portfolio import Portfolio

sd = StockData()
df = sd.get_daily("600519")

ind = IndicatorEngine()
result = ind.all_in_one(df)

pf = Portfolio()
holdings = pf.list_holdings()
```

## 工程化

- **Lint**：ruff，0 警告
- **类型注解**：核心模块完整（config/portfolio/alert_manager/candidate/risk_controller/indicators/db）
- **CI**：GitHub Actions（Python 3.10/3.11/3.12 矩阵）
- **风控**：止损/仓位/回撤检测 + 飞书预警
- **回测**：BacktestEngine 支持多策略参数优化和 Walk-Forward 分析
