# MoatX 宏观事件情报模块实施计划

> 来源模型：ChatGPT 5.5  
> 创建日期：2026-04-26  
> 依据文档：`EVENT_INTELLIGENCE_ALGORITHM.md`  
> 目标：先实现 P0 最小可用闭环，让 MoatX 能持续收集宏观/时政/能源新闻，识别事件，映射板块和个股，并生成可解释机会报告。

---

## 零、当前实现状态（2026-04-27 更新）

> 本节用于让后续参与实现/评审的模型快速理解“计划 vs 已落地 vs 待继续”的真实状态。  
> 更新来源：ChatGPT 5.5，基于当前代码验收与 Review findings。

### 0.0 收工结论

| 项目 | 结论 |
|---|---|
| 当前完成度 | 约 95%，已达到可用级宏观事件情报模块 |
| 当前阶段 | P0/P1 工程闭环收工，进入观察运行与数据资产沉淀阶段 |
| 已可用能力 | 真实新闻源采集、规则 NLP、事件状态、A 股机会、报告、推送、调度、源质量治理、旧新闻护栏、历史弹性、产业图谱 |
| 明确边界 | 不自动下单；不内置外部大模型；复杂 NLP/语义推理由后续适配器扩展 |
| 后续重点 | 持续跑源质量、淘汰低质源、补历史事件样本、继续扩产业图谱 |

### 0.1 P0 闭环落地状态

| 能力 | 当前状态 | 代码位置 | 说明 |
|---|---|---|---|
| 事件数据模型 | 已落地 | `modules/event_intelligence/models.py` | `NewsItem`、`EventSignal`、`EventState`、`TransmissionEffect`、`EventOpportunity` |
| 事件数据库 | 已落地 | `modules/db/event_store.py`、`modules/db/migrations.py` | migration version 已包含 `event_news/signals/states/opportunities` |
| 传导配置 | 已落地 | `data/event_transmission_map.toml` | 已覆盖霍尔木兹、中东冲突、原油、黄金、芯片制裁、国内宽松、红海航运、俄乌、贸易关税，并补充英文关键词以支持英文能源源 |
| 新闻源配置 | 已启用 | `data/event_sources.toml` | 已启用 BBC 中文、RFI 中文、DW 中文、OilPrice、中国新闻网国内/国际/财经 RSS、财联社、央视网、证券时报、人民银行、国家发改委新闻/政策、国家统计局、上海证券报、期货日报共 16 个源；保留新华、新浪、凤凰、人民网、国家能源局、证监会、JSON 等模板禁用 |
| 新闻采集 | 已落地 | `modules/event_intelligence/collector.py` | 支持 RSS / HTTP JSON / JSONP / HTML 列表，单源失败降级，JSON 字段与 HTML 链接规则可配置 |
| 源质量统计 | 已增强 | `event_source_quality` | 记录抓取、入库、重复、错误、信号命中率，并计算 `quality_score/reliability/source_recommendation`，`collect/sources/context/report` 可见 |
| 手动新闻注入 | 已落地 | `modules/event_intelligence/manual_ingest.py` | 支持单条标题/摘要注入，以及 UTF-8 text/JSON 文件导入 |
| 事件抽取 | 已增强 | `modules/event_intelligence/extractor.py` | 规则法：关键词 + 动作词 + 事件定义；已区分传闻、升级、确认、否认、缓和，并加入时间有效性、强度词和发布日期新鲜度护栏 |
| 概率更新 | 已增强 | `modules/event_intelligence/probability.py` | 按近期信号、来源质量、事件阶段、衰减更新 `event_states` |
| 机会扫描 | 已增强 | `modules/event_intelligence/opportunity.py` | 从事件状态 + 传导效果 + 历史弹性先验生成 A 股候选机会；每轮按事件刷新，避免重复候选累积 |
| 报告生成 | 已增强 | `modules/event_intelligence/reporter.py` | 输出 Markdown 事件状态、机会表、源质量、最新证据链、历史弹性与历史事件摘要 |
| 推送冷却 | 已落地并开启调度发送 | `modules/event_intelligence/notifier.py`、`event_notifications` | CLI 默认 dry-run；调度 `event_notify` 使用 `--send`，按事件和报告哈希冷却去重 |
| 编排服务 | 已落地 | `modules/event_intelligence/service.py` | `collect/extract/states/opportunities/report/context/run_event_cycle` |
| CLI 手动入口 | 已落地 | `modules/cli/tool/event.py`、`modules/cli/__init__.py` | 当前真实命令为 `python -m modules.cli tool event ...` |
| 源健康查看 | 已落地 | `modules/cli/tool/event.py` | `tool event sources --json` 输出全部源配置、启用状态和最近质量统计 |
| 调度器接入 | 已启用 | `modules/scheduler.py` | 事件采集/抽取/状态/机会/闭环/推送检查任务均 `enabled=true` |
| 推送接入 | 已启用 | `modules/alerter.py`、`modules/event_intelligence/notifier.py` | 手动 `tool event notify --send` 或调度 `event_notify` 会调用 Alerter |
| 下游上下文 | 已增强 | `modules/event_intelligence/context.py` | 输出 `event_context_v1`，包含源质量、证据链、历史弹性、历史事件、产业图谱版本，为自动交易、外部大模型、复杂 NLP 预留稳定只读契约 |
| 事件弹性回测 | 已落地 | `modules/event_intelligence/elasticity.py`、`data/event_history.toml` | 日线窗口统计 `T+1/T+3/T+5/T+10` 收益、胜率、回撤、超额收益；已内置霍尔木兹、原油、黄金、红海、俄乌、芯片制裁、关税、国内宽松历史触发点 |
| 产业图谱 | 已落地 | `data/sector_graph.toml`、`modules/sector_tags.py` | 配置化维护板块别名、成分股、上下游和关联事件 |

### 0.2 Review findings 处理状态

| Finding | 优先级 | 当前结论 | 落地说明 |
|---|---:|---|---|
| 批量 PE/PB 估值方向反向 | P0 | 已修复 | `modules/scoring_engine.py` 新增 `_cheapness_score()`，批量与单股估值统一为“低 PE/PB 高分” |
| 全部 veto 返回缺列 | P0 | 已修复 | `score_batch()` 全部 veto 早退前会走 `_finalize_score_output()` + `_attach_action_columns()` |
| 部分 veto 输出协议不稳定 | P1 | 已修复 | 最终返回前统一补齐 `quality/timing/sentiment/event_multiplier/total/action/suggested_weight` |
| 事件行业反查方式不可靠 | P1 | 已修复 | 已抽象 `modules/sector_tags.py` 的 `SectorTagProvider`，统一通过“行业/概念板块 → 成分股”构建标签 |
| 事件标签缺少归一化/别名匹配 | P1 | 已修复 | `SectorTagProvider.tag_matches()` 统一别名、后缀、包含匹配，测试覆盖黄金/半导体/光伏/石油等常见别名 |
| AkShare 板块成分接口缺失或 push2 不稳定 | P1 | 已兜底 | `SectorTagProvider` 对石油、油服、天然气、黄金、贵金属、军工、半导体、芯片、信创保留小型关键成分股兜底池，避免机会扫描中断 |

### 0.3 当前验收命令

```bash
pytest tests/test_event_intelligence.py tests/test_event_context.py tests/test_event_elasticity.py tests/test_sector_tags.py tests/test_event_notifier.py tests/test_event_driver.py tests/test_scoring_engine.py -q
python -m py_compile modules/config.py modules/event_intelligence/source_quality.py modules/event_intelligence/context.py modules/event_intelligence/reporter.py modules/event_intelligence/extractor.py modules/event_intelligence/collector.py modules/event_intelligence/probability.py modules/event_intelligence/opportunity.py modules/event_intelligence/elasticity.py modules/db/event_store.py modules/db/migrations.py modules/sector_tags.py modules/scheduler.py
python -m modules.cli tool event --help
python -m modules.cli tool event collect --json
python -m modules.cli tool event extract --json
python -m modules.cli tool event run --json --limit 5 --min-probability 1.0 --per-effect-limit 1
python -m modules.cli tool event notify --json
python -m modules.cli tool event summary --json --top-events 3
python -m modules.cli tool event context --json
python -m modules.cli tool event elasticity --windows 1,3,5,10 --json
python -m modules.scheduler --list
python -m modules.scheduler --daemon
python -m modules.scheduler --status
```

当前已验证：

```text
事件情报相关测试通过
事件情报 CLI 可识别
真实 RSS/JSONP/HTML 源可采集，单源失败不会拖垮整体
2026-04-27 实测：16 个启用源抓取 479 条、入库 122 条、抽取 27 条信号、无采集错误
2026-04-27 收工验收：32 个相关测试通过；真实增量采集 16 源、抓取 479、错误 0；增量抽取 3 条新闻、4 条信号
抽取层默认跳过发布日期超过 14 天的旧新闻，避免历史列表页误触发当前事件状态
context 已输出 source_quality/source_recommendation/signal_evidence/sector_graph_version
report 已输出源质量、源治理建议、最新证据链、历史弹性、历史事件样本
事件调度任务已启用，推送调度使用冷却去重
盘中监控已接入宏观事件 Top3、关联板块和机会标的
后台 scheduler 已支持 Windows/本机 daemon 模式，持续运行采集链路
事件弹性回测和产业图谱已可用于报告解释增强
```

### 0.4 收工后维护清单

1. 每日查看 `tool event sources --json`，重点关注 `watch_low_signal` 和 `disable_candidate` 源。
2. 每周复盘 `tool event elasticity --windows 1,3,5,10 --json`，补充高价值历史事件样本。
3. 继续扩充 `sector_graph.toml`，逐步替代硬编码兜底池并增强产业链关系。
4. 对推送结果做人工复盘，避免源噪声或规则误判造成无效提醒。
5. 自动交易仍不属于本模块职责，只暴露只读上下文和机会解释。

## 一、实施原则

本阶段不要追求“全自动预测战争”这种大而全目标。

P0 要先做成一个稳定闭环：

```text
新闻采集
→ 事件识别
→ 事件状态更新
→ 产业传导映射
→ 个股候选生成
→ Markdown 机会报告
```

核心原则：

| 原则 | 说明 |
|---|---|
| 先规则，后模型 | P0 使用关键词、动作词、传导配置，暂不引入复杂模型 |
| 先解释，后交易 | P0 输出可解释报告，不自动买入 |
| 先闭环，后智能 | 先把数据流、状态流、机会流跑通 |
| 先少量高价值事件 | 优先支持霍尔木兹、原油、黄金、军工、贸易制裁、政策刺激 |
| 不污染现有评分系统 | 新模块独立，后续再接入 `ScoringEngine` |

---

## 二、现有可复用能力

| 现有模块 | 可复用点 |
|---|---|
| `modules/crawler/` | HTTP 请求、缓存、API 探测、板块数据 |
| `modules/db/` | SQLite 仓库、迁移、任务日志 |
| `modules/scheduler.py` | 定时任务框架 |
| `modules/event_driver.py` | 事件→板块乘数雏形、标签匹配逻辑 |
| `modules/scoring_engine.py` | 后续个股二次过滤 |
| `modules/stock_data.py` | 行情、资金、涨停、财务风险接口 |
| `modules/alerter.py` | 后续推送通道 |
| `modules/cli/` | 命令行入口扩展方式 |

P0 不要重写这些基础设施，应尽量复用。

---

## 三、目标文件结构

新增目录：

```text
modules/event_intelligence/
```

新增文件：

```text
modules/event_intelligence/__init__.py
modules/event_intelligence/models.py
modules/event_intelligence/source_registry.py
modules/event_intelligence/collector.py
modules/event_intelligence/extractor.py
modules/event_intelligence/probability.py
modules/event_intelligence/transmission.py
modules/event_intelligence/opportunity.py
modules/event_intelligence/reporter.py
modules/event_intelligence/service.py
```

新增数据库存储：

```text
modules/db/event_store.py
```

修改文件：

```text
modules/db/__init__.py
modules/db/migrations.py
modules/scheduler.py
modules/cli/__init__.py
modules/cli/tool/__init__.py
modules/scoring_engine.py
modules/event_driver.py
docs/README.md
```

已新增/应保留测试：

```text
tests/test_event_intelligence.py
tests/test_event_driver.py
tests/test_scoring_engine.py
```

新增配置：

```text
data/event_transmission_map.toml
data/event_sources.toml
```

---

## 四、P0 模块职责

### 4.1 `models.py`

定义事件情报核心数据结构。

建议 dataclass：

```python
NewsItem
EventSignal
EventState
TransmissionEffect
EventOpportunity
```

核心字段：

```python
NewsItem:
  source
  title
  summary
  url
  published_at
  fetched_at
  raw_hash

EventSignal:
  event_id
  news_id
  event_type
  entities
  matched_keywords
  matched_actions
  severity
  confidence
  direction

EventState:
  event_id
  name
  probability
  impact_strength
  status
  last_signal_at

EventOpportunity:
  event_id
  symbol
  sector_tags
  opportunity_score
  evidence
  recommendation
```

### 4.2 `source_registry.py`

管理新闻源配置。

P0 支持两类来源：

1. RSS/JSON 新闻源。
2. 现有网页/API 可访问新闻源。

建议配置文件：

```text
data/event_sources.toml
```

示例：

```toml
[[sources]]
id = "eastmoney_global"
name = "东方财富国际财经"
type = "http_json"
url = "..."
enabled = true
weight = 0.7

[[sources]]
id = "custom_rss_energy"
name = "能源新闻 RSS"
type = "rss"
url = "..."
enabled = true
weight = 0.8
```

P0 如果暂时没有稳定 RSS，也允许先用 2~3 个 HTTP 新闻接口或搜索接口做原型。

### 4.3 `collector.py`

职责：

```text
从 source_registry 读取源
拉取新闻
标准化为 NewsItem
去重
写入 event_news 表
```

要求：

1. 必须设置超时。
2. 必须去重 URL 和标题 hash。
3. 单源失败不能影响其他源。
4. 不做复杂文本分析，只负责采集。

输出：

```python
collect_news() -> dict
```

示例返回：

```python
{
  "fetched": 120,
  "inserted": 18,
  "duplicates": 96,
  "errors": [...]
}
```

### 4.4 `extractor.py`

职责：

```text
从 event_news 读取未处理新闻
根据关键词 + 动作词 + 实体词抽取 EventSignal
写入 event_signals
```

P0 使用规则法：

```text
事件关键词：霍尔木兹、伊朗、美军、红海、OPEC、原油、黄金、制裁
动作词：封锁、威胁、袭击、部署、禁运、减产、制裁、升级、谈判破裂
资产词：原油、黄金、天然气、航运、军工
```

强度计算：

```text
severity =
  keyword_score
+ action_score
+ source_weight
+ recency_score
```

置信度计算：

```text
confidence =
  min(1.0, 0.3 + matched_keywords * 0.1 + matched_actions * 0.15 + source_weight * 0.2)
```

要求：

1. 只有关键词但没有动作词时，最多生成低强度信号。
2. 动作词越强，severity 越高。
3. 一条新闻可生成多个事件信号。

### 4.5 `probability.py`

职责：

```text
读取 event_signals
按 event_id 聚合
更新 event_states
```

P0 概率公式：

```text
probability =
  base_probability
+ recent_signal_score
+ cross_source_bonus
- decay_penalty
```

状态机：

```text
probability < 0.35        → watching
0.35 <= probability < 0.55 → watching
0.55 <= probability < 0.75 → escalating
probability >= 0.75       → confirmed
连续无新信号              → expired/resolved
```

要求：

1. 最近 24 小时信号权重大。
2. 多来源确认加分。
3. 过期事件自动衰减。

### 4.6 `transmission.py`

职责：

```text
加载 event_transmission_map.toml
把事件映射到资产、行业、概念、受益/受损方向
提供 code -> tags 反查能力
```

P0 可复用 `event_driver.py` 中已经实现的：

```text
行业/概念成分反查
标签归一化
别名匹配
```

但建议抽象成独立 provider：

```python
SectorTagProvider
```

后续 `event_driver.py`、`scoring_engine.py`、`event_intelligence` 都复用它。

P0 不强制立刻抽离，但计划中应明确这是目标。

### 4.7 `opportunity.py`

职责：

```text
根据 EventState + TransmissionEffect 生成 A 股候选机会
```

P0 候选来源：

```text
事件映射行业/概念
→ 成分股列表
→ 去除 ST / 高风险 / 低流动性
→ 计算机会分
```

P0 机会分公式：

```text
OpportunityScore =
  event_probability * 35
+ impact_strength * 20
+ exposure_score * 20
+ liquidity_score * 10
+ timing_proxy * 10
+ quality_guard * 5
- risk_penalty
```

P0 中 `timing_proxy` 可以先用：

```text
当日涨跌幅
换手率
是否涨停
板块涨幅
```

后续 P1 再接入完整 `ScoringEngine`。

### 4.8 `reporter.py`

职责：

```text
把事件机会生成 Markdown 报告
```

P0 输出示例：

```text
【宏观事件机会】霍尔木兹关闭风险升高

事件状态：
- 概率：68%
- 状态：escalating
- 影响强度：0.82

核心证据：
- 伊朗威胁封锁霍尔木兹海峡
- 美军航母进入中东

产业传导：
- 原油：利多
- 石油行业：利多
- 油服工程：利多
- 航空运输：利空

候选股票：
| 代码 | 名称 | 标签 | 机会分 | 理由 |

风险：
- 如果局势缓和，事件溢价会快速回撤
- 如果板块高开过大，不建议追高
```

### 4.9 `service.py`

编排入口。

建议函数：

```python
collect_news()
extract_events()
update_event_states()
scan_event_opportunities()
generate_event_report()
run_event_cycle()
```

P0 最小闭环：

```python
def run_event_cycle():
    collect_news()
    extract_events()
    update_event_states()
    scan_event_opportunities()
    return generate_event_report()
```

---

## 五、数据库实施计划

新增文件：

```text
modules/db/event_store.py
```

### 5.1 表结构

#### `event_news`

```sql
CREATE TABLE IF NOT EXISTS event_news (
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
);
```

#### `event_signals`

```sql
CREATE TABLE IF NOT EXISTS event_signals (
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
    created_at TEXT NOT NULL
);
```

#### `event_states`

```sql
CREATE TABLE IF NOT EXISTS event_states (
    event_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    probability REAL DEFAULT 0,
    impact_strength REAL DEFAULT 0,
    status TEXT DEFAULT 'watching',
    evidence_count INTEGER DEFAULT 0,
    sources_count INTEGER DEFAULT 0,
    last_signal_at TEXT,
    updated_at TEXT NOT NULL
);
```

#### `event_opportunities`

```sql
CREATE TABLE IF NOT EXISTS event_opportunities (
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
);
```

### 5.2 迁移方式

修改：

```text
modules/db/migrations.py
modules/db/__init__.py
```

要求：

1. 表创建幂等。
2. 不影响现有 `price/signal/task/backtest` store。
3. `DatabaseManager.event()` 返回 `EventStore`。

---

## 六、配置实施计划

### 6.1 `data/event_transmission_map.toml`

P0 至少支持：

1. 霍尔木兹关闭风险。
2. 中东冲突升级。
3. 原油供给冲击。
4. 黄金避险。
5. 芯片/半导体制裁。
6. 国内降息/降准。

示例：

```toml
[[events]]
id = "hormuz_closure_risk"
name = "霍尔木兹关闭风险"
event_types = ["geopolitical_conflict", "energy_supply_risk"]
keywords = ["霍尔木兹", "伊朗", "波斯湾", "油轮", "美军"]
actions = ["封锁", "威胁", "袭击", "部署", "禁运"]
base_probability = 0.20

[[events.effects]]
target = "石油行业"
target_type = "sector"
direction = "bullish"
impact = 0.85

[[events.effects]]
target = "航空运输"
target_type = "sector"
direction = "bearish"
impact = 0.65
```

### 6.2 `data/event_sources.toml`

P0 可以先配置占位源，但结构必须稳定。

```toml
[[sources]]
id = "eastmoney_news"
name = "东方财富资讯"
type = "http_json"
url = ""
enabled = true
weight = 0.7

[[sources]]
id = "custom_rss"
name = "自定义 RSS"
type = "rss"
url = ""
enabled = false
weight = 0.8
```

---

## 七、CLI 实施计划

修改：

```text
modules/cli/__init__.py
modules/cli/tool/event.py
```

当前真实命令入口：

```text
python -m modules.cli tool event collect
python -m modules.cli tool event ingest --title "伊朗威胁封锁霍尔木兹海峡" --summary "原油供给风险升高"
python -m modules.cli tool event ingest --file data/manual_event_news.json --source manual_file
python -m modules.cli tool event extract
python -m modules.cli tool event states
python -m modules.cli tool event opportunities
python -m modules.cli tool event report
python -m modules.cli tool event run
```

P0 必须支持：

```text
tool event run
tool event ingest
tool event report
tool event states
tool event opportunities
```

输出：

```text
report 默认输出 Markdown
run/collect/ingest/extract/states/opportunities/notify 默认输出 JSON 风格统计
--json 可强制 JSON 输出
--output 可写入 UTF-8 文件
--title/--summary/--url/--source/--published-at 支持单条手动注入
--file 支持 UTF-8 text/JSON 文件注入
--min-probability 控制机会扫描阈值
--per-effect-limit 控制单个传导目标最多候选数
```

---

## 八、调度实施计划

修改：

```text
modules/scheduler.py
```

新增任务：

| 任务 ID | 频率 | 默认 | 说明 |
|---|---|---|---|
| `event_collect_news` | 每 10 分钟 | 开启 | 拉取真实 RSS 新闻 |
| `event_extract_signals` | 每 10 分钟 | 开启 | 抽取事件信号 |
| `event_update_states` | 每 10 分钟 | 开启 | 更新事件概率 |
| `event_scan_opportunities` | 每 10 分钟 | 开启 | 生成事件机会 |
| `event_cycle` | 每 15 分钟 | 开启 | 闭环运行并 dry-run 推送候选 |
| `event_notify` | 每 15 分钟 | 开启 | 达到阈值后调用 `--send`，通过冷却表去重 |

当前启用边界：

1. 只启用事件情报链路，不启用旧实盘自动下单任务。
2. 推送由阈值和冷却表控制，避免重复轰炸。
3. 自动交易、外部大模型、复杂 NLP、历史弹性回测只开放上下文契约，不默认执行。

---

## 九、P0 实施顺序

### Step 1：数据模型和数据库

文件：

```text
modules/event_intelligence/models.py
modules/db/event_store.py
modules/db/migrations.py
modules/db/__init__.py
```

验收：

1. 数据表可创建。
2. `DatabaseManager.event()` 可用。
3. 可插入/读取新闻、信号、状态、机会。

### Step 2：传导配置和规则加载

文件：

```text
data/event_transmission_map.toml
modules/event_intelligence/transmission.py
```

验收：

1. 可加载霍尔木兹事件配置。
2. 可根据关键词找到事件定义。
3. 可输出受益/受损板块。

### Step 3：新闻源配置和采集

文件：

```text
data/event_sources.toml
modules/event_intelligence/source_registry.py
modules/event_intelligence/collector.py
```

验收：

1. 可读取 source 配置。
2. 单源失败不中断。
3. 新闻写入 `event_news` 并去重。

### Step 4：事件抽取

文件：

```text
modules/event_intelligence/extractor.py
```

验收：

1. “伊朗威胁封锁霍尔木兹海峡”能识别为 `hormuz_closure_risk`。
2. 无动作词的普通新闻只生成低强度信号或不生成信号。
3. `event_signals` 写入成功。

### Step 5：事件概率更新

文件：

```text
modules/event_intelligence/probability.py
```

验收：

1. 多条近期信号能提升 probability。
2. 多来源能提升 confidence。
3. 过期信号会衰减。
4. 状态能从 `watching` 进入 `escalating`。

### Step 6：机会扫描

文件：

```text
modules/event_intelligence/opportunity.py
```

验收：

1. 霍尔木兹事件能映射石油/油服/天然气/黄金/军工。
2. 能通过行业/概念标签找到 A 股候选。
3. 能过滤高风险/低流动性股票。
4. 能生成 `opportunity_score`。

### Step 7：报告生成和 CLI

文件：

```text
modules/event_intelligence/reporter.py
modules/event_intelligence/service.py
modules/cli/tool/event.py
modules/cli/__init__.py
```

验收：

1. `python -m modules.cli tool event run` 可跑完整闭环。
2. `python -m modules.cli tool event report` 输出 Markdown。
3. 报告包含事件概率、证据、传导链、候选股、风险说明。
4. 空新闻源、空事件、空机会时必须返回空报告，不允许异常退出。

### Step 8：调度接入

文件：

```text
modules/scheduler.py
```

验收：

1. `python -m modules.scheduler --list` 能看到事件任务。
2. 事件任务状态为开启。
3. `event_notify` 调度路径使用 `--send`，但仍受阈值和冷却表约束。

---

## 十、P0 不做的事情

为了避免范围爆炸，P0 暂不做：

1. 不自动交易。
2. 不调用外部大模型。
3. 不做复杂 NLP 分类器。
4. 不做新闻全文深度总结。
5. 已做日线事件弹性回测；暂不做分钟级事件冲击回测。
6. 不做自动学习传导权重。
7. 不默认开启自动交易；当前只开启低频事件情报采集、评分和推送。

---

## 十一、P1 增强计划

P0 跑通后再做：

1. 接入原油、黄金、美元指数等价格验证。
2. 接入板块涨跌幅和资金流，判断市场是否已经定价。
3. 接入 `ScoringEngine` 作为候选股二次过滤。
4. 已接入 `Alerter` 或飞书推送。
5. 已做事件报告冷却，防止重复推送。
6. 增加“追高风险”判断。
7. 已新增源质量统计、配置化产业图谱、日线事件弹性回测。

---

## 十二、P2 智能化计划

后续可探索：

1. 引入本地轻量分类器。
2. 引入历史相似事件案例库。
3. 已统计事件发生后板块和个股日线弹性；后续可拓展分钟级事件窗。
4. 学习事件到板块的动态权重。
5. 已对新闻源可靠性做基础评分；后续可拓展长期衰减评分。

---

## 十三、最小验收样例

### 输入新闻

```text
伊朗警告称，如果美国继续扩大制裁，不排除采取措施限制霍尔木兹海峡通行。与此同时，美军航母战斗群进入波斯湾附近海域。
```

### 期望事件信号

```text
event_id: hormuz_closure_risk
event_type: energy_supply_risk
matched_keywords: 伊朗, 美国, 霍尔木兹, 波斯湾
matched_actions: 警告, 限制, 进入
severity: >= 0.6
confidence: >= 0.5
```

### 期望事件状态

```text
status: watching 或 escalating
probability: >= 0.35
```

### 期望传导

```text
bullish: 石油行业, 油服工程, 天然气, 贵金属, 国防军工
bearish: 航空运输, 化工行业
```

### 期望报告

必须包含：

```text
事件名称
事件概率
核心证据
产业传导
候选板块
候选股票
风险提示
```

---

## 十四、给实现模型的提示

实现时请遵守：

1. 先完成 P0 闭环，不要擅自扩展到自动交易。
2. 不要破坏现有 `ScoringEngine` 和模拟交易流程。
3. 数据库迁移必须幂等。
4. 新闻源失败必须降级，不允许整个 cycle 崩溃。
5. 所有机会报告必须可解释。
6. 事件推送已按用户确认开启，但必须保留阈值、冷却、重复报告哈希保护。
