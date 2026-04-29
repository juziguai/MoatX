# MoatX 宏观事件情报驱动选股算法

> 来源模型：ChatGPT 5.5  
> 创建日期：2026-04-26  
> 目标：把 MoatX 从“普通多因子选股器”升级为“宏观事件情报驱动的 A 股机会发现系统”  
> 核心场景：提前识别国际政治、能源、军事、贸易、政策事件，并推演其对 A 股板块和个股的影响

---

## 一、算法定位

当前 `SCORING_ALGORITHM.md` 解决的是：

```text
这只股票现在质量如何？
技术位置如何？
资金情绪如何？
能不能买？
```

本算法解决的是更上层的问题：

```text
世界正在发生什么变化？
这些变化会不会影响资产价格？
会影响哪些行业和板块？
A 股里哪些股票最可能受益或受损？
市场是否还没有充分定价？
是否需要及时推送给用户？
```

因此，本算法不是替代原有评分引擎，而是成为它的上游事件引擎。

推荐关系：

```text
Event Intelligence → 发现宏观机会
Scoring Engine      → 验证个股质量、时机、风险
Alert/Push Engine   → 触发用户通知
```

---

## 二、典型目标场景

以“霍尔木兹海峡关闭风险”为例，理想系统不应等到新闻标题已经写出：

```text
霍尔木兹海峡关闭，国际油价暴涨
```

才开始反应。

它应该在更早的弱信号阶段开始累计证据：

```text
伊朗威胁封锁海峡
美军航母进入中东
油轮遇袭或保险费上涨
美国制裁升级
OPEC 供应扰动
布伦特原油异动
黄金避险上涨
航运绕行风险增加
```

当多条弱信号在时间上连续出现，系统应逐步提高事件概率：

```text
20% → 35% → 55% → 70%
```

然后推演产业影响：

```text
霍尔木兹风险上升
→ 原油供给冲击概率上升
→ 油价上行预期
→ 石油开采 / 油服 / 天然气 / 贵金属 / 军工偏利好
→ 航空 / 化工 / 物流成本偏利空
```

最后结合 A 股交易层判断：

```text
相关板块是否已经大涨？
资金是否开始流入？
相关个股是否具备流动性？
是否有 ST、财务风险、连续亏损？
是否处于可交易技术位置？
```

---

## 三、总体架构

```text
┌──────────────────────────────────────────────┐
│ Layer 0: News Collection                      │
│ 新闻/RSS/API/网页接口/公告/商品价格             │
├──────────────────────────────────────────────┤
│ Layer 1: Event Extraction                     │
│ 实体识别、事件类型、地点、国家、人物、动作       │
├──────────────────────────────────────────────┤
│ Layer 2: Event Probability                    │
│ 多源证据累计、时间衰减、事件概率更新             │
├──────────────────────────────────────────────┤
│ Layer 3: Transmission Mapping                 │
│ 事件 → 商品/资产 → 行业/概念 → A 股标的          │
├──────────────────────────────────────────────┤
│ Layer 4: Market Pricing Check                 │
│ 是否已经被市场定价，是否还有预期差               │
├──────────────────────────────────────────────┤
│ Layer 5: Stock Opportunity Scoring            │
│ 事件暴露 × 个股弹性 × 流动性 × 技术时机 - 风险    │
├──────────────────────────────────────────────┤
│ Layer 6: Push Decision                        │
│ 机会强度、置信度、时效窗口、推送冷却              │
└──────────────────────────────────────────────┘
```

---

## 四、核心评分公式

传统股票评分是：

```text
StockScore = Quality + Timing + Sentiment
```

宏观事件驱动评分应改为：

```text
OpportunityScore =
  EventProbability
× ImpactStrength
× TransmissionConfidence
× MarketUnderpricing
× StockExposure
× LiquidityScore
× TimingScore
- RiskPenalty
```

各字段含义：

| 字段 | 含义 |
|---|---|
| `EventProbability` | 事件发生或继续升级的概率 |
| `ImpactStrength` | 事件对资产价格/产业链影响强度 |
| `TransmissionConfidence` | 事件传导到 A 股板块的确定性 |
| `MarketUnderpricing` | 市场是否尚未充分反应 |
| `StockExposure` | 个股与受益/受损板块的相关度 |
| `LiquidityScore` | 个股是否足够活跃，能不能交易 |
| `TimingScore` | 技术面是否支持当前介入 |
| `RiskPenalty` | ST、财务、监管、追高、流动性风险扣分 |

建议输出范围：

```text
OpportunityScore: 0~100
```

推送阈值建议：

| 分数 | 动作 |
|---|---|
| `< 40` | 记录，不推送 |
| `40~60` | 加入观察队列 |
| `60~75` | 推送普通机会 |
| `75~90` | 推送重点机会 |
| `> 90` | 推送高优先级机会，但必须附带风险说明 |

---

## 五、Layer 0 — 新闻与情报采集

### 5.1 采集对象

新闻源应覆盖以下类别：

| 类别 | 示例 |
|---|---|
| 国际政治 | 中东、美国、伊朗、俄罗斯、欧盟、台海、南海 |
| 能源商品 | 原油、天然气、煤炭、黄金、铜、铝、锂、稀土 |
| 军事安全 | 航母、导弹、军演、油轮遇袭、港口封锁 |
| 贸易制裁 | 关税、实体清单、出口管制、芯片禁令 |
| 国内政策 | 降准、降息、房地产、消费刺激、新能源政策 |
| 公司公告 | 巨潮公告、回购、增持、业绩预告、重大合同 |
| 市场价格 | 原油、黄金、美元指数、美债、航运指数 |

### 5.2 推荐数据源类型

优先级从高到低：

1. 官方/半官方数据源。
2. 财经新闻 RSS/API。
3. 合法网页接口。
4. 公告数据源。
5. 商品/指数行情接口。
6. 搜索引擎结果摘要。

### 5.3 采集模块建议

推荐新增模块：

```text
modules/event_intelligence/
```

建议文件：

```text
modules/event_intelligence/collector.py
modules/event_intelligence/source_registry.py
modules/event_intelligence/event_store.py
modules/event_intelligence/extractor.py
modules/event_intelligence/probability.py
modules/event_intelligence/transmission.py
modules/event_intelligence/opportunity.py
modules/event_intelligence/push_policy.py
```

---

## 六、Layer 1 — 事件识别

每条新闻不应只做关键词命中，而应抽取结构化事件。

### 6.1 事件结构

```python
EventSignal = {
    "event_id": "middle_east_oil_supply_risk",
    "title": "伊朗警告将封锁霍尔木兹海峡",
    "source": "news_source",
    "published_at": "2026-04-26T10:30:00+08:00",
    "entities": {
        "countries": ["伊朗", "美国"],
        "locations": ["霍尔木兹海峡", "波斯湾"],
        "assets": ["原油", "黄金"],
        "sectors": ["石油", "油服", "航运", "军工"]
    },
    "event_type": "geopolitical_supply_risk",
    "direction": "bullish",
    "severity": 0.75,
    "confidence": 0.68,
    "raw_url": "...",
}
```

### 6.2 事件类型

建议先支持以下事件类型：

| 类型 | 说明 |
|---|---|
| `geopolitical_conflict` | 地缘冲突 |
| `energy_supply_risk` | 能源供给风险 |
| `trade_sanction` | 贸易制裁 |
| `policy_stimulus` | 政策刺激 |
| `monetary_easing` | 货币宽松 |
| `technology_restriction` | 科技封锁 |
| `natural_disaster` | 自然灾害 |
| `public_health` | 公共卫生 |
| `company_announcement` | 公司公告 |

### 6.3 事件动作词

关键词不只看名词，也要看动作。

示例：

```text
威胁、封锁、袭击、制裁、部署、演习、谈判破裂、禁运、减产、增产、降息、降准、补贴、限制、批准、出口管制
```

动作词决定事件强弱。

例如：

```text
“伊朗” + “霍尔木兹” ≠ 高风险
“伊朗” + “威胁封锁” + “霍尔木兹” = 高风险
```

---

## 七、Layer 2 — 事件概率模型

事件概率不能由单条新闻决定，应由多源信号累计。

### 7.1 概率更新

建议事件概率使用 0~1：

```text
EventProbability = BaseProbability + EvidenceBoost - DecayPenalty
```

其中：

```text
EvidenceBoost =
  source_weight
× action_strength
× entity_relevance
× freshness
× cross_source_confirmation
```

### 7.2 时间衰减

```text
0~6 小时：1.0
6~24 小时：0.8
1~3 天：0.5
3~7 天：0.25
7 天以上：0.1 或归档
```

### 7.3 多源确认

同一事件如果来自多个独立来源，置信度应显著提高：

```text
1 个来源：弱信号
2~3 个来源：中等信号
4+ 个来源：强信号
官方来源确认：直接提升权重
```

### 7.4 事件状态机

事件应有状态：

```text
watching      观察中
escalating    升级中
confirmed     已确认
pricing       市场定价中
resolved      已缓和
expired       已过期
```

霍尔木兹示例：

```text
watching → escalating → confirmed/pricing → resolved
```

---

## 八、Layer 3 — 产业传导映射

### 8.1 传导链

事件不应直接映射到股票，而应经过传导链：

```text
事件 → 资产/商品 → 行业/概念 → 个股
```

例如：

```text
霍尔木兹关闭风险
→ 原油供给收缩
→ 油价上涨
→ 石油开采、油服工程、天然气、贵金属、航运、军工
→ A 股相关个股
```

### 8.2 传导配置

建议新增配置：

```text
data/event_transmission_map.toml
```

示例：

```toml
[[events]]
id = "hormuz_closure_risk"
name = "霍尔木兹关闭风险"
event_types = ["geopolitical_conflict", "energy_supply_risk"]
keywords = ["霍尔木兹", "伊朗", "波斯湾", "油轮", "美军", "封锁"]

[[events.effects]]
asset = "原油"
direction = "bullish"
impact = 0.9

[[events.effects]]
sector = "石油行业"
direction = "bullish"
impact = 0.85

[[events.effects]]
sector = "油服工程"
direction = "bullish"
impact = 0.75

[[events.effects]]
sector = "航空运输"
direction = "bearish"
impact = 0.65
```

### 8.3 个股暴露度

每只股票需要有事件暴露度：

```text
StockExposure = 行业匹配 × 概念匹配 × 主营业务相关性 × 历史事件弹性
```

推荐字段：

```python
StockExposure = {
    "symbol": "600028",
    "tags": ["石油行业", "央企改革", "天然气"],
    "event_exposure": {
        "hormuz_closure_risk": 0.82,
        "oil_price_spike": 0.78,
    }
}
```

---

## 九、Layer 4 — 市场定价检查

事件机会的关键不只是“判断对”，还要判断市场是否已经涨完。

### 9.1 定价因子

```text
MarketUnderpricing =
  1 - MarketReaction
```

其中 `MarketReaction` 可由以下指标估算：

| 指标 | 说明 |
|---|---|
| 板块当日涨幅 | 已经涨太多则预期差下降 |
| 板块 3 日涨幅 | 防止追高 |
| 相关个股涨停数量 | 涨停潮说明市场已强反应 |
| 主力资金流入 | 越早流入越有价值 |
| 商品价格变动 | 原油/黄金等是否先动 |
| 新闻热度 | 热度过高可能已拥挤 |

### 9.2 预期差判断

```text
事件概率高 + 板块未涨 = 高预期差
事件概率高 + 板块已暴涨 = 追高风险
事件概率低 + 板块已涨 = 情绪炒作风险
```

示例：

```text
霍尔木兹风险概率 70%
石油板块 3 日涨幅仅 1.8%
原油期货已上涨 4%
→ A 股可能存在滞后定价机会
```

---

## 十、Layer 5 — 个股机会评分

### 10.1 个股候选池

事件驱动候选池不应来自涨幅榜，而应来自事件传导标签：

```text
事件相关行业/概念成分股
→ 去除 ST / 高风险 / 停牌 / 低流动性
→ 按事件暴露度与交易条件排序
```

### 10.2 事件驱动个股评分

```text
StockOpportunityScore =
  EventScore × 0.35
+ ExposureScore × 0.25
+ UnderpricingScore × 0.15
+ LiquidityScore × 0.10
+ TimingScore × 0.10
+ QualityGuard × 0.05
- RiskPenalty
```

说明：

| 因子 | 权重 | 说明 |
|---|---:|---|
| `EventScore` | 35% | 事件概率与影响强度 |
| `ExposureScore` | 25% | 个股和事件传导链相关度 |
| `UnderpricingScore` | 15% | 是否还没被市场充分定价 |
| `LiquidityScore` | 10% | 成交额、换手率、盘口活跃 |
| `TimingScore` | 10% | 技术位置是否可介入 |
| `QualityGuard` | 5% | 基本面兜底，不让垃圾股轻易入选 |
| `RiskPenalty` | 扣分 | ST、财务风险、追高、监管风险 |

这与传统长期价值评分不同：事件驱动更看重“事件强度 + 暴露度 + 预期差”。

---

## 十一、Layer 6 — 推送决策

### 11.1 推送条件

建议只有满足以下条件才推送：

```text
EventProbability >= 0.55
ImpactStrength >= 0.60
TransmissionConfidence >= 0.60
OpportunityScore >= 60
RiskPenalty 不超过阈值
最近 N 小时没有重复推送
```

### 11.2 推送内容

推送必须解释清楚，不要只发股票代码。

示例：

```text
【宏观事件机会】霍尔木兹关闭风险升高

事件判断：
- 伊朗/美军/霍尔木兹相关消息 6 小时内多源出现
- 原油供给冲击概率上升
- 事件概率：68%

受益方向：
- 石油行业、油服工程、天然气、贵金属、国防军工

候选标的：
- 600XXX：石油行业，事件暴露度 0.82，流动性良好
- 000XXX：油服工程，事件暴露度 0.76，板块未充分定价

风险：
- 若冲突缓和，事件溢价可能快速回撤
- 若板块开盘高开过大，禁止追高
```

### 11.3 推送等级

| 等级 | 条件 | 动作 |
|---|---|---|
| L1 观察 | 事件概率 40%+ | 仅记录 |
| L2 提醒 | 机会分 60+ | 普通推送 |
| L3 重点 | 机会分 75+ | 强提醒 |
| L4 风险 | 机会分高但已暴涨 | 提醒“不追高” |

---

## 十二、与现有模块关系

| 现有模块 | 新定位 |
|---|---|
| `modules/event_driver.py` | 事件乘数基础版，可逐步迁移为事件评分适配层 |
| `modules/scoring_engine.py` | 个股二次验证，不再承担宏观事件发现职责 |
| `modules/crawler/` | 新闻/API 采集基础设施 |
| `modules/alert_manager.py` | 后续接入事件推送 |
| `modules/scheduler.py` | 定时运行新闻采集、事件更新、机会扫描 |
| `data/event_sector_map.toml` | 可升级为事件传导配置 |

建议新增：

```text
modules/event_intelligence/
data/event_transmission_map.toml
data/event_intelligence.db
```

---

## 十三、数据表设计建议

### 13.1 原始新闻表

```sql
event_news(
  id INTEGER PRIMARY KEY,
  source TEXT,
  title TEXT,
  summary TEXT,
  url TEXT UNIQUE,
  published_at TEXT,
  fetched_at TEXT,
  language TEXT,
  raw_hash TEXT
)
```

### 13.2 事件信号表

```sql
event_signals(
  id INTEGER PRIMARY KEY,
  event_id TEXT,
  news_id INTEGER,
  event_type TEXT,
  entities_json TEXT,
  severity REAL,
  confidence REAL,
  direction TEXT,
  created_at TEXT
)
```

### 13.3 事件状态表

```sql
event_states(
  event_id TEXT PRIMARY KEY,
  name TEXT,
  probability REAL,
  impact_strength REAL,
  status TEXT,
  last_signal_at TEXT,
  updated_at TEXT
)
```

### 13.4 机会结果表

```sql
event_opportunities(
  id INTEGER PRIMARY KEY,
  event_id TEXT,
  symbol TEXT,
  sector_tags TEXT,
  opportunity_score REAL,
  event_score REAL,
  exposure_score REAL,
  underpricing_score REAL,
  timing_score REAL,
  risk_penalty REAL,
  recommendation TEXT,
  created_at TEXT
)
```

---

## 十四、调度流程

建议任务：

| 时间/频率 | 任务 | 说明 |
|---|---|---|
| 每 5 分钟 | `collect_news` | 拉取新闻/RSS/API |
| 每 5 分钟 | `extract_events` | 抽取事件信号 |
| 每 10 分钟 | `update_event_states` | 更新事件概率 |
| 每 10 分钟 | `scan_event_opportunities` | 生成事件机会 |
| 交易时段每 5 分钟 | `push_event_alerts` | 推送高优先级机会 |
| 收盘后 | `review_event_outcomes` | 复盘事件命中率 |

---

## 十五、实现优先级

### P0：最小可用版本

1. 建立 `event_intelligence` 模块目录。
2. 支持 3~5 个新闻源采集。
3. 实现关键词 + 动作词事件识别。
4. 实现 `event_transmission_map.toml`。
5. 实现事件概率累计和时间衰减。
6. 实现事件 → 行业/概念 → 个股候选。
7. 生成 Markdown 推送内容。

### P1：交易可用版本

1. 增加商品价格验证，如原油、黄金、美元指数。
2. 增加市场定价检查。
3. 接入 `ScoringEngine` 做个股二次过滤。
4. 接入 `AlertManager` 或飞书推送。
5. 建立事件状态数据库。

### P2：智能增强版本

1. 引入轻量文本分类器。
2. 加入事件相似案例库。
3. 统计事件发生后板块历史弹性。
4. 自动学习事件传导权重。
5. 输出“追高风险”和“预期差机会”两类推送。

---

## 十六、验收标准

### 16.1 霍尔木兹场景

系统应能完成：

1. 收集包含“伊朗、霍尔木兹、美军、油轮、封锁、原油”等内容的新闻。
2. 识别为 `geopolitical_conflict` 或 `energy_supply_risk`。
3. 将事件概率提升到观察阈值以上。
4. 映射到石油、油服、天然气、贵金属、军工等板块。
5. 找出 A 股相关个股。
6. 判断相关板块是否已经被市场充分定价。
7. 生成可解释推送。

### 16.2 推送质量

推送必须包含：

```text
事件名称
事件概率
核心证据
传导链
受益/受损板块
候选个股
机会分
风险说明
是否追高
```

### 16.3 误报控制

系统必须避免：

1. 单条新闻就高优先级推送。
2. 只有关键词没有动作词也强推。
3. 板块已经暴涨后仍提示低风险机会。
4. ST/高风险/低流动性股票进入重点候选。
5. 同一事件短时间重复轰炸用户。

---

## 十七、最终方向

MoatX 的理想形态不应只是：

```text
每天扫一批股票，算一个综合分
```

而应该是：

```text
持续理解世界变化
推演产业链影响
寻找 A 股预期差
用个股因子验证交易价值
在机会窗口还没关闭前提醒用户
```

因此，本算法应成为 MoatX 下一阶段的核心升级方向。

一句话总结：

```text
事件情报负责发现“为什么会涨”，股票评分负责判断“谁更值得买”，推送系统负责决定“什么时候告诉你”。
```

