# MoatX 升级改造计划

> 基于 `docs/PROJECT_REVIEW_2026-04-26.md` | 目标 Alpha (3.0) → Beta (4.0)

---

## 一、安全加固

### 1.1 `.gitignore` 安全审查 ✅

**当前问题：** `data/feishu.toml`（飞书 webhook URL）和数据库文件可能被意外提交到 Git

**改造内容：**

```
data/feishu.toml
data/moatx.toml
data/*.db
data/*.db-*
data/.cache/
data/image/
data/alerts.log
```

**涉及文件：** `.gitignore`

**验收标准：** `git status` 不显示任何 `data/` 下敏感文件

---

### 1.2 飞书凭证存储安全 ✅

**当前问题：** 飞书 webhook URL 存储在明文 `data/feishu.toml` 中；`app_id/app_secret` 从环境变量降级到 `config.yaml` 明文读取

**改造内容：**
- `config/save()` 方法支持将飞书配置写入加密或有权限控制的路径
- `alerter.py` 的 `_get_feishu_credentials()` 增加日志脱敏（不打印 secret 明文）
- 评估是否需要将 `feishu.toml` 排除出版本控制 + 提供 `.example` 模板

**涉及文件：** `modules/config.py`, `modules/alerter.py`

**验收标准：** 飞书凭证不会以明文形式出现在 Git 历史或日志中

---

## 二、数据一致性

### 2.1 `daily_pnl` 改为纯 INSERT ✅

**当前问题：** `INSERT OR REPLACE` 导致 UNIQUE(date, symbol) 约束下覆盖同一天同一股票的历史记录，违背"数据只增不删"原则

**改造内容：**
- `insert_daily_pnl()` 改为 `INSERT`（去掉 `OR REPLACE`）
- 通过 `_migrate_table_with_check()` 去掉 `UNIQUE(date, symbol)` 约束
- `list_holdings()` 中的 `daily_pnl` JOIN 逻辑改为 `ORDER BY created_at DESC LIMIT 1`
- `record_daily_pnl()` 调用方保持兼容

**涉及文件：** `modules/portfolio.py`

**验收标准：** 同一股票同一日期多次记录不覆盖，完整历史可追溯

---

### 2.2 `snapshots` 改为纯 INSERT ✅

**当前问题：** 同 `daily_pnl`，`INSERT OR REPLACE` + `UNIQUE(date, symbol)` 覆盖历史快照

**改造内容：**
- `insert_snapshot()` 改为 `INSERT`
- 去掉 `UNIQUE(date, symbol)` 约束

**涉及文件：** `modules/portfolio.py`

**验收标准：** 同一日期同一股票多次快照不覆盖

---

### 2.3 `candidates` 验证结果独立存储 ✅

**当前问题：** `update_candidate_result()` 直接 UPDATE candidates 表，覆盖 `entry_price` 等原始推荐数据

**改造内容：**
- 新建 `candidate_results` 表：

```sql
CREATE TABLE IF NOT EXISTS candidate_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    result_date TEXT NOT NULL,
    result_price REAL,
    result_pct REAL,
    verified INTEGER DEFAULT 0,
    created_at TEXT
)
```

- `CandidateManager.update_candidate_result()` 改为 INSERT 到 `candidate_results`
- `CandidateManager.verify_candidates()` 改为 JOIN `candidate_results` 做对比
- `Portfolio._init_db()` 中加入建表语句

**涉及文件：** `modules/candidate.py`, `modules/portfolio.py`

**验收标准：** 每次验证独立插入新行，原始推荐数据不被覆盖

---

### 2.4 `record_trade` 事务原子性加固 ✅

**当前问题：** `record_trade()` 的事务只围绕单行 INSERT，但 `import_trades()` 的批量事务中间失败会回滚整个批次，逐行 commit 和批事务行为不一致

**改造内容：**
- `import_trades()` 改为逐行 commit（当前已是整体 try/rollback），但在文档中明确行为语义
- `record_trade()` 中 holdings 变更（BUY add/SEL delete）与 trade INSERT 保持同一事务内

**涉及文件：** `modules/portfolio.py`

**验收标准：** 交易写入 + 持仓变更为原子操作

---

## 三、性能优化

### 3.1 财务 API 并行化 ✅

**当前问题：** `analyzer.py:analyze()` 中 6 个财务数据获取串行执行（资金流、利润表、现金流、分红、预测、股东），单股分析延迟 ~10s+

**改造内容：**

```python
with ThreadPoolExecutor(max_workers=6) as ex:
    futures = {
        ex.submit(self.data.get_money_flow, symbol): "money_flow",
        ex.submit(self.data.get_profit_sheet_summary, symbol): "profit_sheet",
        ex.submit(self.data.get_cash_flow_summary, symbol): "cash_flow",
        ex.submit(self.data.get_dividend, symbol): "dividend",
        ex.submit(self.data.get_profit_forecast, symbol): "profit_forecast",
        ex.submit(self.data.get_major_shareholders, symbol): "major_holders",
    }
    for fut in as_completed(futures, timeout=15):
        key = futures[fut]
        try:
            results[key] = fut.result()
        except Exception:
            results[key] = {}
```

**涉及文件：** `modules/analyzer.py`

**验收标准：** 单股分析耗时从 ~10s 降到 ~3s（取决于最慢单个 API）

---

### 3.2 `refresh_holdings` 批量 SQL ✅

**当前问题：** `Portfolio.refresh_holdings()` 使用 `iterrows()` 逐行 `execute()` + `commit()`，10 只持仓产生 10 次 SQL 写入

**改造内容：**
- 收集更新参数到列表
- `executemany()` 一次批量 UPDATE
- 单次 `commit()`

**涉及文件：** `modules/portfolio.py`

**验收标准：** 持仓刷新只需 1 次数据库写入

---

### 3.3 `RankEngine` 缓存复用 ✅

**当前问题：** `RankEngine._get_spot()` 绕开缓存直接调 Tencent API，但 `MoatXScreener` 已有 15 秒内存缓存，且 `StockData.get_spot()` 有 30 秒 Parquet 磁盘缓存

**改造内容：**
- `RankEngine._get_spot()` 改为调用 `StockData.get_spot()`，复用已有缓存
- 或直接使用 `MoatXScreener.get_spot()` 的内存缓存

**涉及文件：** `modules/rank_engine.py`

**验收标准：** 评分和筛选共享同一份快照缓存，不重复请求

---

### 3.4 `AlertManager` 避免重复创建 Analyzer ✅

**当前问题：** 每次 `check_alerts()` 内部为每只股票 `new MoatXAnalyzer()`，导致 `StockData` 内部缓存被重复创建

**改造内容：**
- `AlertManager` 构造函数接受可选的 `analyzer` 参数
- 批量预警时外部注入单例，不传时内部创建（向后兼容）

```python
class AlertManager:
    def __init__(self, db, analyzer=None):
        self._analyzer = analyzer

    def check_alerts(self, holdings, max_workers=6):
        analyzer = self._analyzer or MoatXAnalyzer()
        ...
```

**涉及文件：** `modules/alert_manager.py`

**验收标准：** 批量预警检测复用同一个 Analyzer 实例

---

### 3.5 `record_daily_pnl` 批量 API 调用 ✅

**当前问题：** 先串行调 `get_daily_prices(symbols)` 再逐行 `insert_daily_pnl()`，未充分利用批量 API

**改造内容：**
- `get_daily_prices` 已支持批量，`record_daily_pnl` 改为收集所有结果后再批量 INSERT

**涉及文件：** `modules/portfolio.py`

**验收标准：** 减少单次 `commit` 调用次数

---

## 四、代码质量

### 4.1 公共工具提取 `modules/utils.py` ✅

**当前问题：** `_normalize_symbol` 在 `portfolio.py` 和 `candidate.py` 中完全重复；交易所后缀转换逻辑分散在 10+ 个函数中

**改造内容：**
- 新建 `modules/utils.py`：

```python
def normalize_symbol(symbol: str) -> str:
    """去掉 SH/SZ/BJ 后缀，只保留数字部分"""

def to_tencent_code(symbol: str) -> str:
    """转为腾讯财经格式 (sh600519 / sz000001)"""

def to_sina_code(symbol: str) -> str:
    """转为新浪财经格式"""

def to_eastmoney_secid(symbol: str, market: str = "") -> str:
    """转为东方财富 secid 格式"""
```

**替换范围：**

| 文件 | 删除的重复代码 |
|------|---------------|
| `portfolio.py:627-633` | `_normalize_symbol` |
| `candidate.py:98-104` | `_normalize_symbol` |
| `crawler/tencent.py:43-52` | `_market_prefix` |
| `crawler/eastmoney.py:39-55` | `_parse_market`, `_strip_suffix` |
| `crawler/fundflow.py:39-59` | `_parse_market`, `_strip_suffix` |
| `datasource.py` (TencentSource, SinaSource) | 内联前缀转换 |

**涉及文件：** 上述全部 + 新建 `modules/utils.py`

**验收标准：** `grep -r "def _normalize_symbol\|def _market_prefix\|def _parse_market\|def _strip_suffix" modules/` 仅在 utils.py 中存在

---

### 4.2 代理清理去重 ✅

**当前问题：** `_clear_proxy()` 在 `stock_data.py` 中定义了两次（模块级一次性调用 + 实例方法）；`_patch_requests_no_proxy()` 全局 monkey-patch 与 `CrawlerClient.trust_env=False` 功能重叠

**改造内容：**
- 保留 `_clear_all_proxy()`（模块级，一次性），迁移到 `utils.py`
- 删除 `StockData._clear_proxy()` 实例方法（仅定义，未被外部调用）
- 评估 `_patch_requests_no_proxy()` 是否可降级为可选（CrawlerClient 已逐请求设置 `trust_env=False`）

**涉及文件：** `modules/stock_data.py`, `modules/crawler/base.py`, `modules/utils.py`

**验收标准：** 代理清理逻辑只有一处定义

---

### 4.3 类型注解补充 ✅

**当前问题：** `stock_data.py`, `portfolio.py`, `screener.py` 中大量使用 `Optional[Dict[str, Any]]` 等泛型，缺乏具体类型约束

**改造内容：**
- `stock_data.py` 核心方法补充返回值 `TypedDict`
- `screener.py` 补充方法签名类型注解

**当前状态：** 已完成。`analyzer.py` 已有完整 TypedDict；`stock_data.py` 核心方法已补充 TypedDict；`screener.py` 所有公开方法均已标注 `Tuple`/`Optional`/`Literal` 参数注解和 `pd.DataFrame`/`dict` 返回注解

---

## 五、架构升级

### 5.1 `stock_data.py` 拆分 ✅

**当前问题：** 1088 行，混合数据获取（行情/日线/财务/资金流/分红/股东）+ 财务风险检测（5 个子检查），职责过重

**当前状态：** 已完成。`FinancialRiskChecker` 已拆分至 `modules/risk_checker.py`；`stock_data.py` 从 1088 行缩减至 838 行

**拆分方案：**

```
modules/stock_data.py     → 保留，纯数据获取入口
modules/risk_checker.py   → 新增，财务风险检测
```

**迁移清单：**

| 从 `stock_data.py` 移出 | 移至 `risk_checker.py` |
|--------------------------|------------------------|
| `check_financial_risk()` | 公开入口 |
| `_check_st_status()` | 子检查 |
| `_check_earnings_forecast()` | 子检查 |
| `_check_risk_notices()` | 子检查 |
| `_check_disclosure_delay()` | 子检查 |
| `_check_debt_ratio()` | 子检查 |
| `_risk_level()` | 辅助方法 |

- `analyzer.py` 改为从 `risk_checker` 导入

**涉及文件：** `modules/stock_data.py`（修改）, `modules/risk_checker.py`（新增）, `modules/analyzer.py`（修改）

**验收标准：** stock_data.py < 800 行

---

### 5.2 错误信号链完善 ✅

**当前问题：** 三层静默降级 — `analyzer.py` 6 个 `try/except` 返回空、`signal/engine.py` `evaluate()` 异常返回 None、API 封装层返回空 DataFrame 掩盖真实问题

**当前状态：** `analyzer.py`、`signal/engine.py`、`scheduler.py` 均已加 WARNING 日志

**改造内容：**
- `analyzer.py` 中 6 个 `except: return {}` → 加 `_logger.warning("获取xxx失败: %s", e)`，保留降级返回
- `signal/engine.py` 中 `except: return None` → 加 `_logger.warning()`
- `scheduler.py` 中 `_log_task` wrapper 的 warehouse 写入失败 `pass` → 加 `_logger.warning()`
- CLI 层利用 `CrawlResult.user_message` 向用户展示数据源状态

**涉及文件：** `modules/analyzer.py`, `modules/signal/engine.py`, `modules/scheduler.py`, `modules/cli/`

**验收标准：** 所有静默异常至少有一条 WARNING 级别日志；CLI 用户能感知数据源失败

---

### 5.3 `datasource.py` 抽象基类落地 ✅

**当前问题：** `QuoteSource` ABC 已定义，但 `stock_data.py` 中部分请求逻辑硬编码（如 `get_spot()` 直接构造 HTTP 请求），未通过基类调用

**当前状态：** 已完成。`get_realtime_quotes()` 已通过 `QuoteManager` 三级降级（Tencent → EastMoney → Sina）；`get_spot()` 全市场快照故意不走 QuoteManager：全市场 6000+ 股票需分页并行抓取（60 页 × 100 条），QuoteSource.fetch_quotes() 面向"指定股票列表"场景，两者路由设计有本质差异，不强求统一

---

## 六、测试体系

### 6.1 核心模块单元测试 ✅

**当前问题：** 零测试文件，`tests/` 目录不存在

**优先覆盖：**

| 优先级 | 模块 | 测试内容 |
|--------|------|----------|
| P0 | `indicators.py` | MACD 金叉/死叉、KDJ 超买超卖、RSI 边界、BOLL 轨道、均线排列 |
| P0 | `utils.py` | normalize_symbol 各边界、to_*_code 各市场 |
| P0 | `config.py` | 默认值加载、TOML 合并、环境变量覆盖、类型校验 |
| P1 | `crawler/models.py` | CrawlResult 构造、CircuitBreaker 熔断/恢复/冷却 |
| P1 | `crawler/cache.py` | 写入/读取/版本不匹配/TTL/9:30硬失效/13:00硬失效 |
| P1 | `portfolio.py` | 持仓 CRUD、add_holding 负数校验、record_trade 事务、CHECK 约束触发 |
| P2 | `analyzer.py` | 信号生成规则、趋势判断、估值判断、Buffett 视角结论 |
| P2 | `crawler/sector.py` | 字段标准化、filter_boards 过滤逻辑、fallback 链路 |
| P3 | `risk_checker.py` | ST 检测、资产负债率阈值、risk_level 分级 |

**框架：** pytest + pytest-cov（已在 `pyproject.toml` `[project.optional-dependencies] dev` 中）

**涉及文件：** `tests/` 目录（新建）、`pyproject.toml`（pytest marker 配置）

**验收标准：** P0+P1 模块测试覆盖率达到 60%+

---

### 6.2 Mock 基础设施 ✅

**当前状态：** 已完成。`tests/conftest.py` 已创建（含 `mem_db`、`sample_daily_df`、`sample_spot_df` fixtures）；pytest marker 配置已添加

**改造内容：**
- 新建 `tests/conftest.py`：

```python
import pytest
import sqlite3
import pandas as pd
import numpy as np


@pytest.fixture
def mem_db():
    """SQLite 内存数据库"""
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def sample_daily_df():
    """模拟 120 天日线数据（含 OHLCV）"""
    dates = pd.date_range("2026-01-01", periods=120, freq="B")
    np.random.seed(42)
    close = 10 + np.cumsum(np.random.randn(120) * 0.2)
    ...


@pytest.fixture
def sample_spot_df():
    """模拟全市场快照"""
    ...
```

- pytest marker 配置：

```toml
[tool.pytest.ini_options]
markers = [
    "integration: 真实网络请求，默认 skip",
    "slow: 耗时测试",
]
```

**涉及文件：** `tests/conftest.py`（新建）, `pyproject.toml`（修改）

---

### 6.3 CI 恢复 ✅

**当前状态：** 已完成。`.github/workflows/ci.yml` pytest 步骤已恢复，运行 `python -m pytest tests/ -m "not integration" --tb=short`

**改造内容：**
- `.github/workflows/ci.yml` 中恢复 pytest 步骤
- 跑 `-m "not integration"` 排除真实网络测试

**涉及文件：** `.github/workflows/ci.yml`

---

## 七、改造清单总表

| # | 类别 | 改造点 | 状态 |
|---|------|--------|------|
| 1.1 | 安全 | `.gitignore` 审查 | ✅ 已完成 |
| 1.2 | 安全 | 飞书凭证安全 | ✅ 已完成 |
| 2.1 | 数据 | `daily_pnl` INSERT 化 | ✅ 已完成 |
| 2.2 | 数据 | `snapshots` INSERT 化 | ✅ 已完成 |
| 2.3 | 数据 | `candidate_results` 独立表 | ✅ 已完成 |
| 2.4 | 数据 | `record_trade` 原子性 | ✅ 已完成 |
| 3.1 | 性能 | 财务 API 并行化 | ✅ 已完成 |
| 3.2 | 性能 | `refresh_holdings` 批量 SQL | ✅ 已完成 |
| 3.3 | 性能 | `RankEngine` 缓存复用 | ✅ 已完成 |
| 3.4 | 性能 | `AlertManager` 复用 Analyzer | ✅ 已完成 |
| 3.5 | 性能 | `record_daily_pnl` 批量 | ✅ 已完成 |
| 4.1 | 质量 | `utils.py` 公共工具提取 | ✅ 已完成 |
| 4.2 | 质量 | 代理清理去重 | ✅ 已完成 |
| 4.3 | 质量 | 类型注解补充 | ✅ 已完成 |
| 5.1 | 架构 | `stock_data.py` 拆分 | ✅ 已完成 |
| 5.2 | 架构 | 错误信号链完善 | ✅ 已完成 |
| 5.3 | 架构 | `datasource.py` ABC 落地 | ✅ 已完成 |
| 6.1 | 测试 | 核心模块单元测试 | ✅ 已完成 |
| 6.2 | 测试 | Mock 基础设施 | ✅ 已完成 |
| 6.3 | 测试 | CI 恢复 | ✅ 已完成 |

**进度：20/20 已完成，0/20 部分完成，0/20 待完成**

---

## 八、目标

| 维度 | 当前 | 已提升至 | 目标 |
|------|------|----------|------|
| 架构设计 | 4.0 | 4.0 | 4.0 |
| 数据一致性 | 3.0 | **3.5** | 4.0 |
| 错误处理 | 3.5 | **3.7** | 4.0 |
| 性能 | 4.0 | **4.3** | 4.5 |
| 安全性 | 2.5 | **3.0** | 3.5 |
| 代码质量 | 2.5 | **3.0** | 3.5 |
| 可维护性 | 3.5 | 3.5 | 4.0 |
| **综合** | **3.0** | **3.4** | **4.0** |
