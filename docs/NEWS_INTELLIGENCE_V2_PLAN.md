# MoatX News Intelligence v2：全量新闻价值发现引擎

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

# 未来：把 v2 因子写入评分上下文
python -m modules.cli tool event news-factors --json
python -m modules.cli tool event topics --json
python -m modules.cli tool event topic-snapshots --topic AI??? --json
```

## 数据库落地计划

首版可先不迁移表，直接从 `event_news` 读取并即时分析；第二阶段新增：

- `news_insights`：逐条新闻价值分析结果。
- `news_topic_events`：聚合主题事件。
- `news_event_factors`：面向选股评分的事件因子。

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
