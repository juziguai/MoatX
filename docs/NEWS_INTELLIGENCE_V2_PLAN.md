# MoatX News Intelligence v2：全量新闻价值发现引擎

## 2026-04-29 落地状态

- 已完成全量新闻流规则识别：`NewsIntelligenceEngine` 从 `event_news` 读取新闻，识别 AI、算力、机器人、低空经济、能源、军工、黄金、创新药、金融地产、消费出海、并购国改等主题。
- 已完成新闻洞察持久化：`event_news_insights`、`event_news_topic_events`、`event_news_factors` 物化新闻价值、主题热度和板块因子。
- 已完成 LLM 可选增强：`llm-review` 默认 dry-run；只有 `--send` 且本地配置与环境变量齐全时才调用外部 OpenAI-compatible 模型。
- 已完成 LLM 入因子：最新 LLM 评审中 `use/watch/ignore` 会分别放大、保持或剔除对应新闻对板块因子的贡献。
- 已完成主题记忆库：`event_topic_memory` 与 `event_topic_snapshots` 追踪主题热度、动量和趋势。
- 已完成调度接入：事件闭环、新闻因子、主题记忆进入 scheduler；真实 LLM 调用不默认调度。
- 仍保持边界：不自动交易、不提交 API Key、不默认产生付费模型调用。

## 背景

当前 `event_intelligence` v1 的核心是“预设事件雷达”：先在配置里定义霍尔木兹、原油、黄金、芯片制裁等事件，再从新闻中匹配这些事件。它能解释既有宏观主题，但无法主动发现 GPT-5.5、DeepSeek V4、机器人突破、低空经济政策、创新药进展、并购重组等新主题。

v2 的目标是从“找指定新闻”升级为“读全量新闻，判断哪些有交易价值，再生成事件因子给选股算法”。

## 当前落地状态

- 已新增 `NewsIntelligenceEngine`：从 `event_news` 全量新闻流中识别主题、计算价值分、映射 A 股板块/标的。
- 已新增 `NewsFactorEngine`：把高价值新闻聚合成板块级新闻因子，输出 `sector -> factor_score`。
- 已接入 CLI：`event news`、`event news-report`、`event news-factors`。
- 已接入上下文：`event context` 输出 `news_intelligence` 和 `news_sector_factors`，供外部模型/调度/看盘模块消费。
- 已接入报告：宏观事件报告新增“新闻价值发现 / 新闻板块因子 / 高价值新闻”。
- 已接入评分：`EventDriver` 会把新闻板块因子并入 Layer 4 事件驱动分，最终影响 `ScoringEngine` 的 `event_multiplier`。
- 仍未做：复杂 NLP/外部大模型推理、长期新闻记忆库、自动交易下单。

## 总体目标

```text
全量新闻流
  ↓
去重 / 清洗 / 分源质量打分
  ↓
识别新闻主题：AI、能源、军工、黄金、地产、消费、出口、医药、机器人、低空经济……
  ↓
判断价值：是否新、是否重大、是否和 A 股资产有关、是否有政策/产业/价格/供需影响
  ↓
抽取事件：发生了什么、影响谁、利多/利空、持续多久、可信度如何
  ↓
映射板块/产业链/个股
  ↓
生成事件因子
  ↓
输入选股评分系统
```

## v1 与 v2 差异

| 维度 | v1 预设事件雷达 | v2 全量新闻价值发现 |
|---|---|---|
| 入口 | 预定义事件关键词 | 所有新闻源 |
| 主题发现 | 配置里有什么才识别什么 | 新闻自己归类成主题 |
| 新事件 | 容易漏掉 | 自动进入主题池 |
| 报告排序 | 按事件概率/机会分 | 按新闻价值/市场相关性/机会分 |
| 选股接入 | 事件命中加分 | 新闻主题热度 + 产业映射 + 时间衰减 |

## 数据对象

### NewsInsight

```json
{
  "news_id": 123,
  "source": "cls_telegraph_json",
  "title": "DeepSeek 发布 V4 预览版本",
  "topic": "AI大模型",
  "category": "technology",
  "importance": 0.90,
  "novelty": 0.85,
  "market_relevance": 0.95,
  "impact_strength": 0.88,
  "sentiment": "bullish",
  "time_horizon": "short",
  "affected_sectors": ["算力", "光模块", "AI应用", "半导体"],
  "affected_stocks": ["工业富联", "中际旭创", "新易盛"],
  "value_score": 88.7,
  "reason": "大模型能力升级可能提升算力和 AI 应用需求"
}
```

### TopicEvent

多条新闻聚合成一个主题事件：

```json
{
  "topic": "AI大模型",
  "headline": "GPT-5.5 与 DeepSeek V4 同期发布",
  "heat": 0.92,
  "confidence": 0.86,
  "market_relevance": 0.95,
  "direction": "bullish",
  "affected_sectors": ["算力", "光模块", "AI应用", "半导体"],
  "latest_news": [123, 124, 128]
}
```

## 主题分类体系 v1

首版使用规则体系，不接外部大模型：

- **AI大模型**：GPT、DeepSeek、Claude、Gemini、Qwen、Kimi、模型发布、开源模型、API、智能体。
- **算力基础设施**：GPU、算力、服务器、数据中心、液冷、CPO、光模块、HBM。
- **半导体**：芯片、光刻机、先进制程、封装、晶圆、存储、设备材料。
- **机器人**：人形机器人、具身智能、伺服、电机、减速器、传感器。
- **低空经济**：eVTOL、无人机、通航、空域、低空政策。
- **能源商品**：原油、天然气、煤炭、电力、储能、光伏、风电。
- **军工地缘**：战争、冲突、制裁、军演、导弹、航母、航运通道。
- **黄金贵金属**：黄金、白银、避险、央行购金、美元、美债。
- **医药创新药**：创新药、临床、FDA、医保、CXO、ADC。
- **金融地产政策**：降准、降息、LPR、地产政策、地方债、化债。
- **消费出海**：消费刺激、出口、关税、汇率、跨境电商。
- **并购重组国改**：并购、重组、资产注入、国企改革、市值管理。

## 价值评分

```text
value_score =
  source_quality       * 0.20
+ freshness            * 0.15
+ novelty              * 0.15
+ market_relevance     * 0.25
+ impact_strength      * 0.20
+ confidence           * 0.05
```

解释：

- `source_quality`：来自 `event_source_quality`。
- `freshness`：新闻越新越高，盘中快讯最高。
- `novelty`：标题与近期同主题重复度越低越高。
- `market_relevance`：是否能映射到 A 股板块/产业链/标的。
- `impact_strength`：政策、价格、供需、技术突破、订单、业绩、监管等影响强度。
- `confidence`：来源可信度和语义明确度。

## 产业链映射

v2 复用并扩展 `SectorTagProvider` 与 `data/sector_graph.toml`：

```text
主题 → 板块/概念 → 图谱别名 → 成分股 → 机会标的
```

首批需要补强的图谱：

- AI大模型 → 算力、光模块、AI应用、软件、传媒游戏、半导体
- 算力基础设施 → 服务器、CPO、液冷、数据中心、电力
- 机器人 → 人形机器人、减速器、伺服系统、传感器
- 低空经济 → eVTOL、无人机、航空装备、空管
- 创新药 → 创新药、CXO、医疗服务、原料药

## 选股因子接入

新闻不是直接买入信号，而是生成可解释事件因子：

```text
news_event_factor =
  topic_heat
  × source_confidence
  × market_relevance
  × sector_match
  × time_decay
  × direction
```

接入 `ScoringEngine` 时作为事件层增强：

```text
total_score =
  quality_score
+ timing_score
+ sentiment_score
+ event_multiplier
+ news_event_factor
- risk_penalty
```

## CLI 设计

```powershell
# 分析最近新闻，输出高价值新闻洞察
python -m modules.cli tool event news --json --limit 50

# 只看指定主题
python -m modules.cli tool event news --topic AI大模型 --json

# 输出主题聚合报告
python -m modules.cli tool event news-report

# 生成并持久化 v2 新闻板块因子
python -m modules.cli tool event news-factors --json
python -m modules.cli tool event topics --json
python -m modules.cli tool event topic-snapshots --topic AI大模型 --json
python -m modules.cli tool event llm-status --json
python -m modules.cli tool event llm-review --json
python -m modules.cli tool event llm-review --send --json
python -m modules.cli tool event llm-reviews --json
```

## 数据库落地

当前已进入持久化阶段，不再只做即时分析：

- `event_news_insights`：逐条新闻价值分析结果，按 `news_id + topic` 去重，保存规则分、关联板块/个股、LLM 决策和理由。
- `event_news_topic_events`：当前主题事件物化表，保存热度、置信度、关联板块和最新新闻 ID。
- `event_news_factors`：面向选股评分的板块新闻因子，保存 `sector -> factor_score`、方向、主导主题和 LLM 调整系数。
- `event_topic_memory` / `event_topic_snapshots`：长期主题记忆和热度演化追踪。
- `event_llm_reviews`：外部大模型语义评审记录；默认不调用外部模型，只有显式 `--send` 才写入真实评审。

## 分阶段实现

### Phase 1：可运行骨架

- 新增 `modules/event_intelligence/news_intelligence.py`
- 从 `event_news` 全量读取最近新闻
- 规则识别主题
- 计算价值分
- 映射板块
- CLI 输出 Top 新闻洞察

### Phase 2：主题聚合

- 多条新闻聚合为主题事件
- 去重/聚类
- 报告按主题热度排序

### Phase 3：图谱增强

- 扩展 AI、算力、机器人、低空经济、创新药产业图谱
- 支持主题到成分股候选池

### Phase 4：接入选股评分

- 生成 `news_event_factor`
- 接入 `scoring_engine.py`
- 报告解释每只股票受到哪些新闻主题影响

## 验收标准

- GPT-5.5、DeepSeek V4、AI 算力新闻能进入 `AI大模型/算力基础设施` 主题。
- 新闻报告不再只围绕预设宏观事件。
- 每条高价值新闻必须解释“为什么和 A 股有关”。
- 主题必须映射到板块/产业链，而不是只输出新闻标题。
- 选股评分可以读取新闻事件因子，但不自动下单。

## 正式盘中热点报告模板

`python -m modules.cli tool event report` 当前采用“热点速览”格式，目标是让输出像一个懂行情的盘中助理，而不是关键词复读。

### 输出样例

```text
MoatX 热点速览 | 2026-04-30 盘中

本时段扫描19个源，捕获1227条资讯。今日高热聚焦：算力基础设施（3条）、储能新能源（1条）、能源商品（1条）。AI大模型、医药创新药、机器人、军工地缘、黄金贵金属无触发阈值。

🔥 算力基础设施 · 硬件突破与政策双轮驱动

1. 麦格米特获GB300电源订单
热度 ⭐⭐⭐⭐ (84%) | 财联社 (中国)
核心看点：国产电源厂首次获得AI服务器高端平台批量配套订单，验证高端电源国产化从0到1的突破。
传导路径：GB300批量订单 ➔ 服务器电源（麦格米特(002851)等） ➔ 液冷及数据中心基础设施
选中理由：命中关键词"GB300"，自动归入"算力基础设施"模块。该新闻标志着AI服务器电源国产替代取得实质订单，可能引发资金对电源链及液冷散热环节的重新定价。
可能涉及的A股：麦格米特(002851)、欧陆通(300870)、中恒电气(002364)
一句话：电源环节国产替代加速，液冷和数据中心配套确定性抬升。
```

### 生成约束

- 顶部摘要必须说明扫描源数量、资讯数量、高热模块和无触发阈值模块。
- 单条标题必须精炼为“谁干了什么、结果怎样”，控制在 20 字左右，禁止在词语中间截断。
- 核心看点必须包含标题之外的增量事实和市场关注原因；若新闻源只有标题无正文，显式标记“全文待扩展”。
- 传导路径必须从具体触发点开始，按“触发点 ➔ 第一受益环节（代表标的）➔ 次生影响环节”输出。
- 股票映射优先使用细粒度标签，例如 `服务器电源`、`光无源器件`、`绿色算力`，不能直接套用模块通用股票池。
- 选中理由第一句保留机器命中逻辑，第二句按订单/紧缺/政策/数据等事件类型输出编辑判断，禁止退化成“出现边际变化”。
- 一句话结论用于语音播报，只做方向性描述，不出现买入、强烈看好等推荐话术。

### 防退化验收

- 麦格米特 GB300 新闻的首只标的必须是 `麦格米特(002851)`，不得回退到 `工业富联(601138)` 等通用算力池。
- 光无源器件新闻的路径起点应表达为 `800G/1.6T光模块扩产拉动`，避免“光无源器件 ➔ 光无源器件”的重复链路。
- 钠电订单新闻应归入 `储能新能源`，不应强行归入 `算力基础设施`。
- 5 条高价值新闻中至少 4 条不得出现“边际变化”。
- 报告仍然只输出情报、机会和评分因子，不产生自动下单指令。
