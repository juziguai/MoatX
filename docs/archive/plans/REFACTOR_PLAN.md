# MoatX CLI 重构规划

> 目标：模块化、结构化、解耦合
> 更新日期：2026-04-26
> 状态：**主体重构完成，后续改进全部落地**

---

## 一、现状问题

### 1.1 代码组织

**文件**：`modules/cli_portfolio.py`（约 790 行）

**问题**：
- 所有 argument parser 全堆在 `main()` 里（~100 行）
- 命令路由全塞在 `main()` 底部（~50 行）
- 业务逻辑和 CLI 基础设施混在一起
- `cmd_*` 函数散落在文件各处，没有按业务聚合
- 没有分包，整个项目是扁平结构

### 1.2 职责边界不清

| 问题 | 说明 |
|------|------|
| `cmd_list` 里有 SQL 查询逻辑 | `daily_pnl` 查询写在 CLI 层，应该下沉到 `Portfolio` |
| `trade` 逻辑分散 | CLI 决定买加仓/卖删仓，但 `Portfolio.record_trade()` 也知道这些逻辑 |
| `import` 截图解析在 CLI，解析结果写入在 `Portfolio` | 解析和存储没有完全分离 |

### 1.3 命令结构现状

```
持仓/交易: import batch-import list remove refresh trade
行情:      quote
预警:      check watch alerts
配置:      config
工具:      diagnose probe-api schedule signal paper
```

**问题**：`diagnose` `probe-api` `schedule` `signal` `paper` 五个工具命令和其他日常命令平级，比较散。

---

## 二、模块化原则

1. **CLI 层只负责：解析参数 → 调用业务接口 → 格式化输出**
2. **业务逻辑全部下沉到 `modules/` 下的业务类**（`Portfolio`/`StockData`/`Alerter` 等）
3. **命令分组，组内按职责聚合**
4. **子命令层级统一**（避免有的命令一级有的两级）

---

## 三、拆分方案

### 方案 A：按命令类型拆分（最保守）

```
modules/cli/
├── __init__.py          # main() + 路由注册
├── portfolio.py         # import, batch-import, list, remove, refresh, trade
├── quote.py           # quote
├── alerter.py         # alert check, alert watch, alert history
├── config.py          # config
└── tool/
    ├── __init__.py     # register_tool_commands()
    ├── diagnose.py
    ├── probe.py
    ├── signal.py
    ├── schedule.py
    └── paper.py
```

**优点**：跟现有命令分类一致，改动最小
**缺点**：`tool/` 下 5 个文件仍然散；`config` 单独拆出去显得多余

---

### 方案 B：工具命令打回 scripts/（激进）

```
modules/cli/
├── __init__.py     # main() + 路由
├── portfolio.py    # 持仓核心操作（import/list/remove/refresh/trade）
├── quote.py       # quote
├── alerter.py     # alert check/watch/history
└── config.py     # config

scripts/           # 工具类，日常不常用
├── diagnose_crawler.py   # diagnose（已有）
├── probe_api.py         # probe（已有）
├── signal_runner.py     # signal
├── schedule_runner.py   # schedule
└── paper_runner.py    # paper
```

**优点**：CLI 只留日常命令，工具归工具，职责分明
**缺点**：调用方式从 `tool probe` 变成 `python scripts/probe_api.py`，用户习惯变了

---

### 方案 C：统一两级子命令结构（最激进）

```
python -m modules.cli portfolio list
python -m modules.cli portfolio trade buy 600519 100 1500 150000
python -m modules.cli quote
python -m modules.cli alert check
python -m modules.cli tool probe https://example.com
```

**优点**：结构绝对统一，扩展方便
**缺点**：所有命令变长，用户习惯全改

---

## 四、已确认决策

### 4.1 CLI 层和业务层的边界

**确认：选项 A** —— 业务逻辑下沉到 Portfolio

- `daily_pnl` 查询 → `Portfolio.get_daily_pnl()`
- `cmd_list()` 不写任何 SQL，只调业务接口
- CLI 层只做：解析参数 → 调用业务接口 → 格式化输出

### 4.2 `trade` 的职责

**确认：选项 A** —— `Portfolio.record_trade()` 内部处理买加仓/卖删仓

- CLI 只调 `pf.record_trade()`，不感知 holdings 变化
- `record_trade()` 内部判断 action：SELL → `remove_holding`，BUY → `add_holding`

### 4.3 命令层级

**确认：保留现状**
- 一级命令：`list` `refresh` `quote` `config`
- 两级命令：`alert check` / `tool diagnose`
- 不强制统一，用户习惯已经建立

### 4.4 调用入口

**确认：选项 A** —— 保持 `python -m modules.cli_portfolio ...` 不变

### 4.5 `diagnose` 等工具的定位

**确认：保留 `tool` 子命令分组**
- 日常不常用，但有需要时统一入口方便管理
- 不打回 `scripts/`，保持调用体验一致

---

## 五、实施计划

### 第一步 ✅：业务逻辑下沉（已完成）

- `Portfolio.get_daily_pnl()` — 新增，封装当日盈亏查询
- `Portfolio.record_trade()` — 重写，内部处理 holdings 变化（买加仓/卖删仓）
- `cmd_list()` — 清理内联 SQL，改调 `pf.get_daily_pnl()`
- `cmd_trade()` — 简化为纯转发，不再处理 holdings

### 第二步 ✅：文件拆分（已完成）

```
modules/cli/
├── __init__.py          # main() + 路由注册
├── portfolio.py         # list, remove, refresh, trade, import, batch-import
├── quote.py           # quote
├── alerter.py         # alert check, watch, history
└── tool/
    ├── __init__.py     # re-export 命令函数
    ├── diagnose.py     # tool diagnose
    ├── probe.py        # tool probe
    ├── signal.py       # tool signal
    └── paper.py       # tool paper

modules/cli_portfolio.py  # 向后兼容重导出（from modules.cli import main）
```
    ├── probe.py
    ├── signal.py
    ├── schedule.py
    └── paper.py
```

### 第三步：config 独立或合并

- 如果 `config` 命令复杂，拆成 `config.py`
- 如果简单，保持在 `__init__.py` 或合并到 `portfolio.py`

---

## 六、风险评估

| 改动 | 风险 | 缓解措施 |
|------|------|----------|
| 重构目录结构 | 影响 import 路径 | 先写 `__init__.py` 导出，后改入口 |
| 改 `trade` 逻辑 | 影响现有持仓数据 | 先在测试环境验证 |
| 下沉 `daily_pnl` 查询 | 可能破坏现有显示 | 保留双轨，CLI 先查再查 Portfolio |

---

## 七、后续改进点

1. ✅ `Portfolio.list_holdings()` 返回时自动带上当日盈亏（减少 CLI 查询）
2. ✅ `check_alerts` 执行后自动写 `daily_pnl`（收盘后自动记录每日盈亏，`alert check --record-pnl`）
3. ✅ 增加 `portfolio summary` 命令输出总览（总市值、总盈亏、仓位分布）
4. ✅ `import` 截图时自动 `refresh` 更新价格（导入即刷新）
5. ✅ N+1 查询优化：`check_alerts` 并行化（ThreadPoolExecutor，max_workers=6）
6. ✅ God Class 拆解：抽出 `CandidateManager`、`AlertManager`，Portfolio 只保留核心持仓/交易逻辑

## 八、已知问题（后续处理）

- `AlertManager._detect_alerts` 与原 `Portfolio._detect_alerts` 逻辑略有差异（MACD/评分预警暂时移除），后续对齐
- 候选股 `verify_candidates` 方法在 `CandidateManager` 中已保留完整逻辑
