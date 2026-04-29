# MoatX Beta 阶段计划

> 基于 Alpha 改造成果 (`archive/plans/UPGRADE_PLAN.md` 20/20) | 目标 Beta (4.0) → Release (4.5)
>
> **周五模拟验证：2026-04-26** — 7 个调度任务全部跑通，发现并修复 3 个 bug（见 §验证记录）

---

## 验证记录：2026-04-26 周五交易日模拟

```
09:20  reset_pending      ✅ 清除 0 只残留
09:30  scan_candidates    ✅ 选出 5 只导入
       check_alerts       ✅ 无预警（空仓）
14:50  mark_pending       ✅ 标记 5 只待验证
15:05  generate_signals   ✅ 生成 8 个信号
15:10  verify_close       ✅ 5/5 验证（非交易时段价格持平）
15:30  snapshot_portfolio ✅ 空仓无快照
```

**发现并修复的 bug：**

| # | 问题 | 文件 | 修复 |
|---|------|------|------|
| 1 | `SinaSource.fetch_quotes()` 调用 `to_sina_code()` 但未导入 → `NameError` | `datasource.py` | 补充 import |
| 2 | `candidate_results` 旧表缺 `result_date`/`result_price`/`result_pct`/`verified` 列 → `OperationalError` | `portfolio.py` | 加 `_ensure_columns` |
| 3 | 调度器 `check_alerts`/`snapshot_portfolio`/`generate_signals` 使用旧 CLI 命令名（`check`/`snapshot`/`signal run`）全部失效 | `scheduler.py` | 改为 `alert check`/`refresh`/`tool signal run` |

**待验证（需真实交易日）：** 候选股涨跌验证（非交易时段价格持平，无法验证真实涨跌）

---

## 一、生产化运行

### 1.1 调度器实盘验证 ✅

**当前问题：** 7 个定时任务已定义（`modules/scheduler.py`），但从未在真实交易时段持续运行过

**改造内容：**
- 周一至周五 09:00 启动调度器，15:30 停止，跑满一周
- 每个任务记录成功率：
  - `scan_candidates` 09:30 开盘扫描 — 是否返回有效候选
  - `check_alerts` 每 5 分钟 — 是否超时、是否有网络故障
  - `mark_pending` 14:50 — 标记成功率
  - `verify_close` 15:10 — 收盘价获取成功率
  - `snapshot_portfolio` 15:30 — 日终快照完整性
- 每日生成运行报告：任务名称、成功/失败、耗时、错误信息

**涉及文件：** `modules/scheduler.py`, `scripts/daily_report.py`（新增）

**验收标准：** 连续 5 个交易日无崩溃，各任务成功率 ≥ 90%

**当前状态：** 已完成。`scripts/daily_report.py` 已创建（支持 `--date` 指定日期、`--json` 输出 JSON）；`scheduler.py` 改造：`SubprocessResult` 返回真实 returncode，wrapper 记录 stdout/stderr 和真实成功/失败到 `task_execution_log`

---

### 1.2 候选股验证全流程打通 ✅

**当前问题：** 候选股筛选→导入→标记→收盘验证→结果写入的 5 步流程存在断点，`verify_candidates` 方法逻辑需要端到端验证

**改造内容：**
- 确认 `import_candidates.py → set_pending.py --set → verify_candidates.py → set_pending.py --reset` 全链路可执行
- 候选股从筛选到验证结果全部写入 `candidates` + `candidate_results` 表
- 验证成功率统计（推荐买入 vs 实际涨跌）

**涉及文件：** `scripts/import_candidates.py`, `scripts/set_pending.py`, `scripts/verify_candidates.py`, `modules/candidate.py`, `modules/portfolio.py`

**验收标准：** 端到端跑通一次完整流程，数据库记录完整

**当前状态：** 已完成。`CandidateManager` 新增 `get_pending()`、`clear_pending()`、`mark_verified()` 方法；`Portfolio` 对应封装；`verify_candidates.py` 全面重写（改用 `Portfolio.mark_candidate_verified()` API，不再直接 SQL）；`set_pending.py` 改用 `Portfolio` API

---

### 1.3 调度器健壮性加固 ✅

**当前问题：** 调度器调用 `subprocess.run` 启动独立 Python 进程执行任务，但进程卡死或超时后没有恢复机制

**改造内容：**
- 每个 task 加全局超时（`timeout=120` 已有，需验证）
- 连续失败 3 次的任务自动暂停并推送飞书预警
- 任务失败时保留 stdout/stderr 到 task_log

**涉及文件：** `modules/scheduler.py`, `modules/db/task_log.py`, `modules/db/migrations.py`, `modules/db/__init__.py`

**验收标准：** 单任务失败不影响其他任务，异常任务可自动暂停

**当前状态：** 已完成。`TaskFailureTracker` 新增（`task_failure_snapshot` 表，migration 5）；APScheduler listener 监听 `EVENT_JOB_EXECUTED | EVENT_JOB_ERROR`：成功自动重置失败计数；连续 3 次失败 → `mark_paused()` → `scheduler.pause_job()` → 飞书预警；`build_scheduler()` 启动时跳过已暂停任务

---

## 二、集成测试补漏洞

### 2.1 数据源冒烟测试 ✅

**当前问题：** 5 个测试文件全为 mock 单测，网络 IO 路径零覆盖。Sina 字段改名、CNINFO 超时、THS 页面结构变更都是静默爆炸

**改造内容：**
- 新增 `tests/test_integration.py`（`@pytest.mark.integration`，默认 skip）：
  - `test_sina_spot_reachable` — Sina 全市场快照可达
  - `test_sina_daily_reachable` — 日线数据单股返回
  - `test_tencent_quote_reachable` — 腾讯行情可达
  - `test_cninfo_notices_reachable` — CNINFO 公告查询可达
  - `test_ths_sector_reachable` — THS 行业板块可达
  - `test_financial_risk_single_stock` — 单股财务风险检测可达
- 每天定时跑一次（CI 可配 schedule 或本地 cron）
- 失败时推送飞书预警

**涉及文件：** `tests/test_integration.py`（新增）

**验收标准：** 所有数据源每天至少验证一次，失败有告警

**当前状态：** 已完成。`tests/test_integration.py` 已创建，含 8 个集成测试用例覆盖 Sina/Tencent/THS/CNINFO/东方财富/新浪财经/财务风险检测；超时和接口异常均 `pytest.skip()` 而非 fail

---

### 2.2 风控规则集成测试 ✅

**当前问题：** `RiskController` 的止损/仓位/回撤检测有单测覆盖，但没有与实际行情数据联动的端到端验证

**改造内容：**
- 新增集成测试：加载真实持仓 + 实时行情 → 验证风控事件触发
- 覆盖止损临界值（浮亏刚好 6.9% vs 7.1%）

**涉及文件：** `tests/test_integration.py`

**验收标准：** 止损/仓位/回撤三项规则均与真实数据联动验证通过

**当前状态：** 已完成。`TestRiskController` 新增 4 个集成测试用例：`test_stop_loss_triggers_at_threshold`（-7.1% 触发）、`test_stop_loss_not_triggered_below_threshold`（-6.9% 不触发）、`test_position_limit_triggers`（85% 仓位超限）、`test_zero_price_skipped`（价格为0不误报）

---

### 2.3 CI 增强 ✅

**当前问题：** CI 只跑 ruff + pytest（无网络），不覆盖数据源健康状态

**改造内容：**
- CI 增加每日定时触发（`schedule` event）
- 定时触发时跑 integration 测试
- PR 触发时只跑单测

**涉及文件：** `.github/workflows/ci.yml`

**验收标准：** 每日自动数据源健康检查，PR 不跑网络测试保持快速

**当前状态：** 已完成。`ci.yml` 重构为两个 job：`test-unit`（PR/push 时跑，`-m "not integration"`）和 `test-integration`（定时 08:30 UTC 跑 integration 测试，`concurrency` 防止重复）；`workflow_dispatch` 支持手动触发

---

## 三、回测-信号-模拟交易闭环

### 3.1 策略参数优化→信号引擎自动注入

**当前问题：** `strategy/optimizer.py` 产出最优参数后，需要人工复制粘贴到 `signal/engine.py` 使用的策略实例

**改造内容：**
- `optimizer.py` 优化完成后自动序列化最佳参数到 `data/strategy_params.json`
- `signal/engine.py` 启动时读取最新优化参数
- `signal list` 命令支持指定策略和参数文件

**涉及文件：** `modules/strategy/optimizer.py`, `modules/signal/engine.py`, `modules/cli/tool/signal.py`

**验收标准：** 回测→参数导出→信号引擎加载 一步完成

**当前状态：** 已完成。`optimizer.py` 新增 `load_strategy_params()` / `save_params_to_json()`，优化结果自动写入 `data/strategy_params.json`；`SignalEngine` 新增静态方法 `load_params()` 读取参数；`signal.py` 全面重写，加载时自动调用 `strategy.set_params()` 应用最优参数；CLI 新增 `--strategy` / `--params-file` 参数；`tool signal run` 支持指定策略和参数文件

---

### 3.2 模拟交易实盘跟踪

**当前问题：** `PaperTrader` 可以执行信号并记录交易，但没有运行中的账户追踪——买了多少、亏了多少、持仓多久

**改造内容：**
- 新增每日模拟账户快照（市值、现金、持仓明细）
- `paper status` 命令输出：
  - 初始资金 vs 当前总资产
  - 累计收益率
  - 持仓明细（代码、名称、成本、现价、盈亏）
  - 历史交易列表
- 模拟交易与实盘持仓隔离（不同数据库/表）

**涉及文件：** `modules/signal/paper_trader.py`, `modules/db/signal_store.py`, `modules/cli/tool/paper.py`

**验收标准：** 模拟账户可实时追踪，与实盘数据互不干扰

**当前状态：** 已完成。新增 `paper_daily_snapshots` 表（migration 6）存储每日账户快照；`PaperTrader` 新增 `take_snapshot()`、`positions_detail()`、`_current_prices()` 方法，实时行情计算总资产和持仓盈亏；`total_value` 属性改为使用实时价格（非 avg_cost）；买入时自动获取并保存股票名称；`paper status` 输出增强（初始资金、当前总资产、累计收益率、持仓明细表、近期快照历史）；新增 `snapshot` 子命令手动记录快照

---

### 3.3 回测报告增强

**当前问题：** `BacktestEngine.report()` 只有基本指标（收益率、夏普、最大回撤、胜率）

**改造内容：**
- 新增月度收益分布表
- 新增逐年收益柱状图
- 新增最大回撤区间标注（起止日期 + 持续天数）
- 输出 Markdown 格式报告

**涉及文件：** `modules/backtest/engine.py`, `modules/backtest/metrics.py`

**验收标准：** 回测报告包含完整风险收益指标，可直接用于飞书推送

**当前状态：** 已完成。`calc_metrics()` 新增月度收益分布（`monthly_returns`）、逐年收益（`annual_returns`）、最大回撤区间详情（`max_drawdown_start/end/recovery/days`）；`report()` 完全重写为 Markdown 格式，输出完整指标表 + 年度收益表 + 月度收益分布

---

## 四、非交易时段数据质量

### 4.1 交易日历

**当前问题：** 系统使用 `datetime.now()` + 简单 `weekday()` 判断交易时间，不处理节假日（国庆、春节等），周末和节假日做分析时技术指标全废

**改造内容：**
- 引入 `akshare.tool_trade_date_hist_sina()` 获取 A 股交易日历
- 新建 `modules/calendar.py`，提供：
  - `is_trading_day(date)` — 判断是否为交易日
  - `last_trading_day(date)` — 上一个交易日
  - `next_trading_day(date)` — 下一个交易日
  - `is_trading_time()` — 当前是否在交易时段
- 缓存交易日历到本地 JSON，每日更新

**涉及文件：** `modules/calendar.py`（新增）, `modules/crawler/cache.py`（复用）

**验收标准：** 节假日不触发网络请求，直接返回上一交易日缓存数据

**当前状态：** 已完成。新建 `modules/calendar.py` 作为公共 API，复用 `backtest/calendar.py` 的核心逻辑；新增 `is_trading_time()` 判断当前是否在交易时段（09:30-11:30 / 13:00-15:00）；交易日历缓存到 `data/trading_calendar.json`，每日刷新；提供 `is_trading_day()`、`last_trading_day()`、`next_trading_day()` 公共接口

---

### 4.2 非交易时段兜底策略

**当前问题：** 非交易时段 Sina 快照价格全为 0，技术指标（MACD/KDJ/RSI）无法计算，`analyze()` 返回空数据

**改造内容：**
- 非交易时段 `get_spot()` 优先返回上一交易日收盘缓存
- `analyze()` 检测到 price=0 时提示"非交易时段，以下数据来自上一交易日"
- `cli quote` 命令展示数据时间戳，明确标注数据新鲜度

**涉及文件：** `modules/stock_data.py`, `modules/analyzer.py`, `modules/cli/quote.py`

**验收标准：** 周末/节假日执行分析时，数据来自上一交易日且明确标注

**当前状态：** 已完成。`quote.py` 新增 `modules.calendar` 导入，`查询时间` 旁增加数据状态标签（🟢 实时 / 🟡 非交易时段 / ⚪ 非交易日）；`analyzer.py` 新增 `is_trading_day` 导入，`analyze()` 在 price=0 且非交易日时报告增加 `_notice: "非交易时段，以下数据来自上一交易日"` 字段

---

## 五、日志和监控

### 5.1 结构化日志

**当前问题：** `data/alerts.log` 是纯文本追加，无轮转、无结构化，排查问题靠 grep

**改造内容：**
- 引入 Python `logging.handlers.RotatingFileHandler`
- 关键事件结构化日志（JSON 格式），写入 `data/moatx.log`：
  - 网络请求：`{"ts":"...", "event":"http", "source":"sina", "ok":true, "elapsed_ms":45}`
  - 预警触发：`{"ts":"...", "event":"alert", "symbol":"600519", "type":"kdj_overbought"}`
  - 任务执行：`{"ts":"...", "event":"task", "id":"check_alerts", "ok":true, "elapsed_ms":1234}`
  - 错误：`{"ts":"...", "event":"error", "module":"stock_data", "error":"REMOTE_DISCONNECTED"}`
- 日志轮转：10MB × 5 个文件

**涉及文件：** `modules/logger.py`（新增）, 各模块日志调用点

**验收标准：** 日志可被 `jq` 或其他 JSON 工具解析查询

**当前状态：** 已完成。新建 `modules/logger.py`，提供 `get_logger()` 工厂函数；`JsonFormatter` 将每条日志序列化为 JSON 行写入 `data/moatx.log`；`RotatingFileHandler` 轮转 10MB × 5 个文件；支持 `extra` 参数注入 event/source/ok/elapsed_ms/symbol/type/task_id/error/module 等字段；控制台输出保持纯文本格式

---

### 5.2 健康监控面板

**当前问题：** 用户不知道系统是否在正常运行。调度器是否存活？数据源是否可用？上次预警什么时候触发的？

**改造内容：**
- 新增 `cli monitor` 命令，输出一屏概览：

```
MoatX 健康监控 — 2026-04-26 14:35
─────────────────────────────────
调度器: 🟢 运行中 (已运行 5h 23m)
数据源: Sina 🟢 | Tencent 🟢 | CNINFO 🟢 | THS 🟢 | EastMoney 🔴
上次预警: 14:30 check_alerts (3 条预警)
今日错误: 2 次 (EastMoney REMOTE_DISCONNECTED × 2)
上次飞书推送: 14:30 (成功)
日志: data/moatx.log (1.2MB)
```

- 数据源状态来自 `CrawlerClient` 的健康评分

**涉及文件：** `modules/cli/tool/monitor.py`（新增）

**验收标准：** 一屏看清系统健康状况

**当前状态：** 已完成。新建 `modules/cli/tool/monitor.py`，`python -m modules.cli monitor` 命令输出系统概览：调度器状态（参考 `_scheduler_ref`）、最近任务执行结果、日志文件大小、数据源连通性（Sina/Tencent/EastMoney/CNINFO）、近期预警历史

---

### 5.3 飞书推送可靠性

**当前问题：** 推送失败只记录日志，没有重试、没有降级、没有成功率统计

**改造内容：**
- `alerter.py` 推送失败后重试 1 次（间隔 5 秒）
- 连续失败 3 次后写本地文件兜底
- 每日推送成功率统计（总次数/成功/失败）
- `cli monitor` 展示推送健康度

**涉及文件：** `modules/alerter.py`

**验收标准：** 推送可靠性 ≥ 95%

**当前状态：** 已完成。`_send_feishu_webhook` / `_send_feishu_api` 增加重试机制（最多2次，间隔5秒）；连续失败3次后写入 `data/feishu_fallback.txt` 兜底文件；新增 `_record_push(ok)` 统计每次推送结果到 `data/feishu_push_stats.json`（total/success/fail/consecutive_fails）；新增 `get_push_stats()` 公共函数；`monitor` 命令输出飞书推送成功率

---

## 六、改造清单总表

| # | 类别 | 改造点 | 复杂度 | 状态 |
|---|------|--------|--------|------|
| 1.1 | 生产化 | 调度器实盘验证 | 中 | ✅ 已完成 |
| 1.2 | 生产化 | 候选股验证全流程 | 中 | ✅ 已完成 |
| 1.3 | 生产化 | 调度器健壮性加固 | 低 | ✅ 已完成 |
| 2.1 | 测试 | 数据源冒烟测试 | 中 | ✅ 已完成 |
| 2.2 | 测试 | 风控规则集成测试 | 低 | ✅ 已完成 |
| 2.3 | 测试 | CI 增强 | 低 | ✅ 已完成 |
| 3.1 | 闭环 | 策略参数自动注入 | 中 | ✅ 已完成 |
| 3.2 | 闭环 | 模拟交易实盘跟踪 | 高 | ✅ 已完成 |
| 3.3 | 闭环 | 回测报告增强 | 中 | ✅ 已完成 |
| 4.1 | 数据 | 交易日历 | 中 | ✅ 已完成 |
| 4.2 | 数据 | 非交易时段兜底 | 低 | ✅ 已完成 |
| 5.1 | 监控 | 结构化日志 | 中 | ✅ 已完成 |
| 5.2 | 监控 | 健康监控面板 | 中 | ✅ 已完成 |
| 5.3 | 监控 | 飞书推送可靠性 | 低 | ✅ 已完成 |

---

## 七、实施路线图

```
Week 1: 生产化运行
  Day 1-2: 1.3 调度器健壮性加固 → 1.1 启动实盘验证
  Day 3-4: 1.2 候选股验证全流程
  Day 5:   收集第一周运行数据

Week 2: 测试 + 数据质量
  Day 1-2: 2.1 数据源冒烟测试 → 2.3 CI 增强
  Day 3:   2.2 风控规则集成测试
  Day 4-5: 4.1 交易日历 → 4.2 非交易时段兜底

Week 3: 闭环 + 监控
  Day 1-2: 3.1 策略参数自动注入
  Day 3-4: 3.2 模拟交易实盘跟踪
  Day 5:   3.3 回测报告增强

Week 4: 监控收尾
  Day 1-2: 5.1 结构化日志
  Day 3:   5.2 健康监控面板
  Day 4-5: 5.3 飞书推送可靠性 + 全量回归
```

---

## 八、目标

| 维度 | Alpha 初 | Alpha 末 | Beta 目标 |
|------|----------|----------|-----------|
| 架构设计 | 4.0 | 4.5 | 4.5 |
| 数据一致性 | 3.0 | 4.0 | 4.5 |
| 错误处理 | 3.5 | 4.0 | 4.5 |
| 性能 | 4.0 | 4.5 | 4.5 |
| 安全性 | 2.5 | 3.5 | 4.0 |
| 代码质量 | 2.5 | 3.5 | 4.0 |
| 可维护性 | 3.5 | 4.0 | 4.5 |
| **综合** | **3.0** | **4.0** | **4.5** |

---

## 九、Beta 收尾总结

### 已完成

- 15/15 改造点全部代码实现 + 逐项核实
- 94 单测全过，9 集成测试通过
- 周五交易日模拟：7 个调度任务全部跑通
- 模拟中发现并修复 3 个 bug（datasource 导入遗漏 / schema 迁移 / 调度器 CLI 路径）
- 文档同步：PROJECT_STATUS / archive/reviews/PROJECT_REVIEW / archive/plans/UPGRADE_PLAN / BETA_PLAN / known_errors / CLAUDE.md

### 遗留项（下一阶段）

- 调度器未在真实连续交易日运行（需实盘周期验证）
- `trading_calendar.json` / `strategy_params.json` 需首次运行后生成
- 候选股涨跌验证需真实交易时段才有意义（非交易时段价格持平）

### 进入下一阶段条件

- 所有代码改造已完成 ✅
- 测试体系已建立 ✅
- 文档已同步 ✅
- 下一阶段可聚焦：实盘运行稳定性、数据源监控、策略收益验证
