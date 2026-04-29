# MoatX 模拟交易实施计划

> 基于业务逻辑缺口分析 | 已有代码支撑约 60%，核心短板在卖出信号

---

## 一、改造清单总览

| # | 模块 | 内容 | 复杂度 | 状态 |
|---|------|------|--------|------|
| 1 | 配置 | `SimulationSettings` + `simulation.toml` | 低 | ✅ 已完成 |
| 2 | 卖出 | `SellSignalEngine` 止盈/止损/超期/技术卖出 | **高** | ✅ 已完成 |
| 3 | 买入 | 候选股扫描→模拟买入桥接 | 中 | ✅ 已完成 |
| 4 | 监控 | `AlertManager` 对接 paper_holdings | 中 | ✅ 已完成 |
| 5 | 调度 | 重构为模拟交易日流程 | 中 | ✅ 已完成 |
| 6 | 报告 | 日报增加投资内容 | 低 | ✅ 已完成 |

---

## 二、逐模块改造方案

### 2.1 配置：`SimulationSettings` + `simulation.toml`

**涉及文件：** `modules/config.py`（修改）, `data/simulation.toml`（新建）

**新增 dataclass：**

```python
@dataclass(frozen=True)
class SimulationSettings:
    # 买入
    max_single_position_pct: float = 0.2
    max_total_position_pct: float = 0.8
    min_buy_signal_score: int = 3
    pe_max: float = 50.0
    risk_score_max: int = 30
    initial_capital: float = 100_000.0
    # 卖出
    stop_profit_pct: float = 0.15
    stop_loss_pct: float = 0.07
    max_hold_days: int = 20
    kdj_overbought: float = 85.0
    rsi_overbought: float = 75.0

    def __post_init__(self):
        for name, val in [("max_single_position_pct", self.max_single_position_pct), ...]:
            if val < 0:
                raise ValueError(...)
```

- `MoatXConfig` 新增 `simulation: SimulationSettings` 字段
- `get_config()` 中新增 `simulation` section 合并
- `data/simulation.toml` 创建默认值

**验收标准：** `from modules.config import cfg; print(cfg().simulation.stop_loss_pct)` 输出 `0.07`

---

### 2.2 卖出信号引擎 `modules/sell_signal.py`（最大缺口）

**涉及文件：** `modules/sell_signal.py`（新建）

**对外接口：**

```python
class SellSignalEngine:
    def __init__(self, sim_cfg: SimulationSettings):
        ...

    def evaluate(self, symbol: str, holding: dict) -> SellSignal | None:
        """对一只持仓评估卖出条件，返回信号或 None"""

    def evaluate_all(self, holdings: list[dict]) -> list[SellSignal]:
        """批量评估"""
```

**`SellSignal` 数据结构：**

```python
@dataclass
class SellSignal:
    symbol: str
    reason: str           # "止盈 +15.3%" / "止损 -8.1%" / "KDJ 超买 J=92" / "持仓超期 22天"
    signal_type: str      # "stop_profit" / "stop_loss" / "technical" / "timeout"
    price: float
    entry_price: float
    hold_days: int
    pnl_pct: float
```

**四条卖出规则：**

```
1. 止盈: 当前价 / 买入均价 - 1 ≥ stop_profit_pct
   → 卖出理由: "止盈 +18.2%，持有 5 天"

2. 止损: 当前价 / 买入均价 - 1 ≤ -stop_loss_pct
   → 卖出理由: "止损 -8.5%，持有 3 天"

3. 技术卖出: 获取日线 → 计算 KDJ/RSI/MACD
   J > kdj_overbought → "KDJ 超买 J=92"
   RSI12 > rsi_overbought → "RSI 超买 82"
   MACD 死叉 → "MACD 死叉"

4. 超期卖出: hold_days > max_hold_days
   → 卖出理由: "持仓超期 25天，收益 -3.2%"
```

**注意事项：**
- `SellSignalEngine` 不依赖 `PaperTrader`，只产出信号
- 需要 `StockData.get_daily()` 获取日线用于技术指标计算
- 需要 `StockData.get_spot()` 或 `TencentSource` 获取当前价
- 如果当前价获取失败（非交易时段），跳过规则 1/2/3，仅执行规则 4

**验收标准：** 模拟持仓 5 只，至少 1 只触发卖出条件

---

### 2.3 买入桥接：候选股→模拟买入

**涉及文件：** `modules/scheduler.py`（新增 `scan_and_buy()` 函数）

**流程：**

```
Screener.scan_all(pe_range=(0, sim_cfg.pe_max))
  → filter_by_financial_risk(max_risk=sim_cfg.risk_score_max)
  → 排除已在 paper_holdings 中的股票
  → 按 buy_signal_score 排序取前 N 只
  → 每只计算买入数量：
      available_cash = initial_capital * max_total_position_pct - 当前总市值
      buy_value = min(available_cash * max_single_pct, available_cash / N)
      shares = buy_value / price（取 100 的整倍数）
  → PaperTrader._buy(symbol, price, reason=f"开盘扫描 评分={score}")
```

**关键逻辑：**
- `scan_and_buy()` 作为 scheduler 任务函数，返回买入数量
- 不重复买入已持有的股票
- 现金不足时跳过，记录到日志

---

### 2.4 `AlertManager` 对接 paper_holdings

**涉及文件：** `modules/alert_manager.py`（修改）

**当前问题：** `check_alerts()` 接收 `Portfolio.list_holdings()` 返回的 DataFrame，其中 `symbol` 列来自 holdings 表

**改造方案：** 新增方法 `check_paper_alerts()`：

```python
def check_paper_alerts(self, paper_holdings: pd.DataFrame, max_workers=6) -> list[dict]:
    """
    对 paper_holdings 运行预警检测。
    paper_holdings 来自 db.signal().all_paper_holdings()
    """
    # 将 paper_holdings 转换为 holdings 兼容格式（统一 symbol/name 列）
    normalized = paper_holdings.rename(columns={...})
    return self.check_alerts(normalized, max_workers=max_workers)
```

**原因：** `_detect_alerts()` 的分析逻辑完全适用于模拟持仓，只是输入格式不同。不需要重写，仅需适配 DataFrame 列名。

---

### 2.5 调度器重构

**涉及文件：** `modules/scheduler.py`（修改）

**替换现有 7 个任务为 6 个模拟交易任务：**

```python
TASKS: list[TaskDict] = [
    {
        "id": "scan_and_buy",
        "name": "开盘扫描+模拟买入",
        "fn": _log_task("scan_and_buy", "开盘扫描+模拟买入", scan_and_buy),
        "trigger": CronTrigger(hour=9, minute=30, day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "monitor_holdings",
        "name": "盘中监控",
        "fn": _log_task("monitor_holdings", "盘中监控", monitor_holdings),
        "trigger": IntervalTrigger(minutes=30),
        "enabled": True,
    },
    {
        "id": "generate_sell_signals",
        "name": "卖出信号",
        "fn": _log_task("generate_sell_signals", "卖出信号", generate_sell_signals),
        "trigger": CronTrigger(hour=14, minute=55, day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "execute_signals",
        "name": "执行交易",
        "fn": _log_task("execute_signals", "执行交易", execute_signals),
        "trigger": CronTrigger(hour=15, minute=0, day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "daily_snapshot",
        "name": "账户快照",
        "fn": _log_task("daily_snapshot", "账户快照", daily_snapshot),
        "trigger": CronTrigger(hour=15, minute=10, day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "daily_report",
        "name": "每日报告",
        "fn": _log_task("daily_report", "每日报告", daily_report),
        "trigger": CronTrigger(hour=15, minute=30, day_of_week="mon-fri"),
        "enabled": True,
    },
]
```

**新增 6 个任务回调函数：**

| 函数 | 核心逻辑 |
|------|----------|
| `scan_and_buy()` | 见 2.3 |
| `monitor_holdings()` | `AlertManager.check_paper_alerts()` → 写 alert_log → 飞书推送 |
| `generate_sell_signals()` | `SellSignalEngine.evaluate_all()` → 写入 signal_journal |
| `execute_signals()` | 读取 signal_journal 中未执行的信号 → `PaperTrader.execute_signal()` |
| `daily_snapshot()` | `PaperTrader.take_snapshot()` |
| `daily_report()` | 见 2.6 |

**注意：** 保留旧的 `candidate` 系列函数（`scan_candidates`/`mark_pending`/`verify_close`/`reset_pending`），暂不删除，通过 `enabled=False` 禁用。`TaskFailureTracker` 自动跳过已暂停任务。

---

### 2.6 日报增强

**涉及文件：** `scripts/daily_report.py`（修改）

**现有输出：** 任务名称 + 成功/失败 + 耗时

**新增输出：**

```markdown
# MoatX 模拟交易日报 — 2026-04-26

## 今日操作
| 时间 | 操作 | 代码 | 名称 | 价格 | 数量 | 金额 | 原因 |
|------|------|------|------|------|------|------|------|
| 09:30 | 买入 | 603382 | 海阳科技 | 26.78 | 700 | 18,746 | 开盘扫描 评分=5 |
| 15:00 | 卖出 | 600519 | 贵州茅台 | 1580 | 100 | 158,000 | 止盈 +16.2% |

## 当前持仓
| 代码 | 名称 | 成本 | 现价 | 盈亏% | 持有天数 |
|------|------|------|------|-------|----------|

## 账户概览
- 初始资金: 100,000
- 当前总资产: 103,200
- 累计收益率: +3.2%
- 可用现金: 21,000

## 预警汇总
- KDJ 超买: 2 次
- 止损预警: 1 次
```

**实现方式：**
- 从 `paper_daily_snapshots` 取最新快照
- 从 `paper_trades` 取当日交易
- 从 `signal_journal` 取当日信号
- 从 `alert_log` 取当日预警

---

## 三、实施顺序

```
Step 1: 2.1 配置    (40行，无依赖，全局可调)
  ↓
Step 2: 2.2 卖出引擎 (150行，核心缺口，独立模块)
  ↓
Step 3: 2.3 买入桥接 (60行，依赖 #1)
Step 4: 2.4 监控对接 (40行，独立)
  ↓
Step 5: 2.5 调度器   (80行，依赖 #2 #3 #4)
Step 6: 2.6 日报     (50行，独立)
```

总计约 420 行新代码 + 修改约 200 行现有代码。

---

## 四、新增文件清单

| 文件 | 说明 |
|------|------|
| `modules/sell_signal.py` | 卖出信号引擎（止盈/止损/技术/超期） |
| `data/simulation.toml` | 模拟交易参数配置 |

## 修改文件清单

| 文件 | 改动 |
|------|------|
| `modules/config.py` | 新增 `SimulationSettings` dataclass + TOML 加载 |
| `modules/scheduler.py` | 替换 7 个老任务为 6 个模拟任务 |
| `modules/alert_manager.py` | 新增 `check_paper_alerts()` |
| `scripts/daily_report.py` | 新增投资内容输出 |
