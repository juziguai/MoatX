# ChatGPT 5.5 深度评估：评分算法设计落地报告

> 来源模型：ChatGPT 5.5  
> 评估日期：2026-04-26  
> 评估方式：静态代码核查，不运行测试、不触发网络数据源、不修改业务逻辑  
> 评估范围：`SCORING_ALGORITHM.md`、`SCORING_CRITIQUE.md` 与当前项目代码实现

---

## 一、总体结论

MoatX 评分算法已经完成了核心工程落地，不再只是文档设计。

当前项目中已经存在独立的 `ScoringEngine`，并且模拟交易主链路已经接入该评分引擎。质量分、时机分、情绪分、事件乘数、市场状态权重、组合集中度惩罚、反馈学习、买卖模拟衔接等关键模块均有代码实现。

但从严格验收角度看，当前实现仍然不是设计文档的完整等价实现。它更像是一个已经能跑的第一版多因子评分系统，而不是最终定版的稳定评分框架。

我的落地完成度判断：

| 项目 | 评估 |
|---|---|
| 核心评分引擎 | 已落地 |
| 模拟交易接入 | 已落地 |
| 四层评分结构 | 基本落地 |
| P0 批判意见修复 | 部分落地 |
| 输出协议一致性 | 未完全落地 |
| 事件驱动可靠性 | 偏弱 |
| 老评分体系统一 | 未完成 |
| 可测试性与可解释性 | 仍需增强 |

综合评价：**约 70% 落地，可进入功能测试阶段，但不建议直接作为最终选股决策依据。**

---

## 二、文档设计摘要

`SCORING_ALGORITHM.md` 的核心设计是：

1. 使用四层评分架构：
   - Layer 0：风控一票否决
   - Layer 1：质量分，满分 50
   - Layer 2：时机分，满分 35
   - Layer 3：情绪分，满分 15
   - Layer 4：事件乘数，范围约 0.6 到 1.4

2. 核心原则：
   - 不做简单一锅炖评分
   - 先风控过滤，再分层评分
   - 质量决定方向，技术决定时机
   - 信号叠加加分，信号冲突避免买入
   - 所有评分尽量可解释

3. 评分输出目标：
   - `symbol`
   - `quality`
   - `timing`
   - `sentiment`
   - `total`
   - `action`
   - `weight`

4. 买入行动分层：
   - 0：不买
   - 1-40：观察
   - 41-55：小仓试探
   - 56-70：正常买入
   - 71-85：重点买入
   - 86-100：重仓候选

`SCORING_CRITIQUE.md` 的核心批判是：

1. 不能只看个股快照，要考虑组合层面。
2. 不同市场环境下评分权重应动态变化。
3. 负 PE、负 PB 不能被排名法误判成低估。
4. 买入系统不能强，卖出系统弱。
5. 事件驱动不能停留在简单关键词。
6. A 股特有信号需要纳入。
7. 应考虑流动性、回撤、执行成本。

---

## 三、实际代码落地情况

### 3.1 核心评分引擎

当前已经存在独立文件：

```text
modules/scoring_engine.py
```

其中实现了：

- `ScoreBreakdown`
- `ScoringFeedback`
- `ScoringEngine`
- `score_batch`
- `score_single`
- `_score_quality_batch`
- `_timing_single`
- `_score_sentiment_batch`
- `_apply_event_multiplier`
- `_apply_concentration_penalty`

这说明评分算法已经有了明确的工程归宿，不再依赖旧的 `RankEngine` 或 `Analyzer` 内部零散评分。

### 3.2 模拟交易主链路接入

模拟交易文件：

```text
modules/simulation.py
```

已经在买入扫描中实例化 `ScoringEngine`，并通过 `score_batch()` 对候选股评分。

买入过滤逻辑已经使用：

```python
total >= 41
```

这与设计文档中的行动分层基本一致。

同时，模拟交易中已经根据分数占比做资金分配，而不是简单等权买入。这一点符合设计文档中“按分数加权，不按排名”的思想。

### 3.3 调度层接入

调度模块：

```text
modules/scheduler.py
```

已经存在模拟交易相关任务：

- `sim_scan_and_buy`
- `sim_generate_sell_signals`
- `sim_execute_signals`

且这些任务默认处于启用状态。

这说明评分算法已经不只是一个孤立类，而是进入了项目自动化流程。

---

## 四、分层评分落地核查

### 4.1 Layer 0：风控一票否决

状态：**基本落地**

实现位置：

```text
modules/scoring_engine.py
modules/risk_checker.py
```

当前逻辑会调用财务风险检查，若 `risk_score >= 30` 则一票否决。

ST 检查也存在。

这一层基本符合设计要求。

但存在一个细节风险：`_check_veto()` 中 ST 判断主要依赖 `check_financial_risk()` 返回的内容。如果数据源异常，异常会被吞掉并默认不否决。作为交易系统，后续建议提供“风险数据不可用时是否保守拒绝”的配置项。

### 4.2 Layer 1：质量分

状态：**部分落地，方向正确，但仍需修正**

批量评分中已经实现：

- PE 候选集排名
- PB 候选集排名
- ROE
- 毛利率
- 自由现金流
- 财务健康
- 流动性惩罚

这比早期文档批判中“只有估值分”的问题已经进步明显。

但存在三个问题：

#### 问题 1：批量质量分不是全市场排名，而是候选集排名

设计文档强调“全市场排名法”，当前 `score_batch()` 实际上是在传入候选集内部做 PE/PB 排名。

这会导致评分结果依赖候选池质量。如果候选池本身已经经过 PE 过滤，那么估值分区分度会被压缩。

#### 问题 2：`score_single()` 的 PE/PB 排名方向疑似错误

单股评分中存在如下逻辑：

```python
pe_rank = (spot["pe"] > pe).mean()
pe_pts = round((1 - pe_rank) * 15, 1)
```

如果某只股票 PE 很低，那么市场上大多数股票 PE 都大于它，`pe_rank` 会较高，最终 `(1 - pe_rank)` 反而较低。

这意味着单股评分里低 PE 可能被打低分，高 PE 反而可能被打高分。

批量评分里的方向是正确的，单股评分应与批量评分统一。

#### 问题 3：负 PE 处理偏温和

当前代码对负 PE/无 PE 做了清洗，不会参与估值加分，并附加“PE 为负”说明。

这比原始风险已经好很多。

但设计文档中的 Layer 0 规则写的是：

```text
PE <= 0 或 PB <= 0: quality_score *= 0.5
```

当前实现更接近“估值部分不给分”，并不是完整执行“质量分整体折半”。如果后续要严格贴合设计，应补齐这一点。

### 4.3 Layer 2：时机分

状态：**基本落地**

当前实现覆盖：

- MA5/MA10/MA20/MA60
- 多头排列
- MACD
- KDJ
- Bollinger
- RSI

分值结构也基本对应设计中的 35 分。

但需要注意：当前逻辑偏向技术反转/超卖，也包含多头排列加分。后续测试时应重点看两个场景：

1. 趋势强股是否被超买扣分误伤。
2. 弱势下跌股是否因 KDJ/RSI 超卖被错误抬分。

### 4.4 Layer 3：情绪分

状态：**基本落地，但数据源覆盖仍偏薄**

当前实现包含：

- 主力资金流
- 换手率
- 涨跌幅动量
- 北向持股
- 今日涨停

这已经超过最初设计中的基本情绪分，并吸收了 `SCORING_CRITIQUE.md` 中关于 A 股特有信号的部分建议。

但仍缺：

- 龙虎榜
- 融资融券
- 解禁压力
- 大宗交易
- 限售股变化
- 板块强度联动

因此情绪分目前能用，但还不是 A 股增强版的完整实现。

### 4.5 Layer 4：事件乘数

状态：**有实现，但可靠性偏弱**

当前已经存在：

```text
modules/event_driver.py
data/event_sector_map.toml
```

实现内容包括：

- 事件关键词配置
- 东方财富新闻关键词探测
- 巨潮公告标题情绪扫描
- 正负面关键词分
- 时间衰减
- 输出 -40 到 +40 的事件 boost
- 在评分中转成 `1.0 + boost / 100` 的乘数

这是一个明确可运行的事件驱动雏形。

但最大问题在行业识别：

```python
ak.stock_board_industry_cons_ths(symbol=code)
```

这个接口通常是按行业板块名查成分股，不是按股票代码反查所属行业。当前 `_infer_sector()` 大概率无法稳定得到真实行业，最终会频繁回退到“上海主板/深圳主板”。

这会导致 `event_sector_map.toml` 中配置的“石油行业、半导体、黄金、贵金属”等事件行业很难真正映射到个股。

所以 Layer 4 的结构已经落地，但效果很可能偏弱。

---

## 五、`SCORING_CRITIQUE` 批判意见落地情况

| 批判点 | 当前状态 | 评价 |
|---|---|---|
| 组合集中度 | 部分落地 | 已有行业集中度惩罚，但依赖行业映射质量 |
| 市场状态动态权重 | 已落地 | 基于沪深 300 MA20 判断牛熊中性 |
| 反馈学习 | 初步落地 | 已记录买卖评分和收益，但样本积累后才有效 |
| 仓位管理 | 部分落地 | 买入按评分加权，但没有完整加减仓系统 |
| A 股特有信号 | 部分落地 | 北向、涨停已加入，龙虎榜/融资融券等未加入 |
| 事件驱动弱关键词 | 部分改善 | 有配置和衰减，但行业映射与语义理解仍弱 |
| 执行与流动性 | 部分落地 | 有低成交额惩罚，但滑点/冲击成本不足 |
| 负 PE 陷阱 | 部分落地 | 批量估值规避负 PE，但质量整体折半未严格执行 |
| 最大回撤控制 | 部分落地 | 模拟买入前有 15% 回撤熔断 |
| 卖出侧弱 | 部分落地 | 有止盈止损、超期、技术卖出，但未接入评分衰减式卖出 |

---

## 六、当前实现中的关键风险

### 风险 1：新旧评分体系并存

项目中至少存在三套评分概念：

1. `ScoringEngine`
2. `RankEngine`
3. `Analyzer.composite_score`

其中 `ScoringEngine` 是新设计的主评分系统，但旧入口仍然存在。

如果 CLI、报告、预警、筛选、模拟交易分别使用不同评分，就会出现：

```text
同一只股票，在不同功能里分数完全不同
```

这会严重影响用户信任。

建议后续统一：

- 所有“买入候选评分”使用 `ScoringEngine`
- `RankEngine` 降级为轻量排序器或废弃
- `Analyzer.composite_score` 改名为 `technical_score`，避免冒充综合评分

### 风险 2：`score_batch()` 输出协议未完整兑现

文档要求返回：

```text
symbol, quality, timing, sentiment, total, action, weight
```

当前主要返回评分列，没有内置 `action` 和 `weight`。

虽然 `simulation.py` 中临时计算了买入权重，但这会让评分引擎和交易决策耦合不清。

建议把行动解释直接放入评分结果：

```text
action = no_buy / watch / probe / normal / heavy / max_heavy
suggested_weight = 0 / 0 / 0.05 / 0.10 / 0.15 / 0.20
```

### 风险 3：事件乘数可能“看起来有，实际上弱”

事件驱动最大价值在于：

```text
宏观事件 → 行业 → 个股 → 分数修正
```

当前“事件 → 行业”有配置，“事件 → 新闻命中”有实现，但“个股 → 行业”不稳。

这会导致 Layer 4 很难产生真实的行业传导效果。

后续应优先实现稳定的个股行业/概念映射缓存表。

### 风险 4：单股评分与批量评分口径不一致

批量评分和单股评分都存在，但估值排名方式不同。

这会带来一个严重问题：

```text
买入时 score_batch 给高分，持仓复盘时 score_single 给低分，或者反过来。
```

后续应抽出统一估值排名函数，不允许两套逻辑分叉。

### 风险 5：异常处理过于宽松

大量评分子模块出现异常时直接返回 0 或跳过。

这保证了系统不会轻易崩，但也可能掩盖数据源失效。

建议后续每只股票评分结果增加：

```text
data_quality
missing_fields
source_errors
```

否则测试时很难判断“低分是股票差，还是数据没拿到”。

---

## 七、优先修复建议

### P0：必须优先修

1. 修复 `score_single()` PE/PB 排名方向。
2. 统一 `score_batch()` 和 `score_single()` 的估值逻辑。
3. 为 `score_batch()` 增加 `action` 与 `weight` 输出。
4. 建立个股 → 行业/概念映射缓存，修复事件乘数传导。

### P1：建议下一阶段修

1. 把 `Analyzer.composite_score` 改名，避免与综合评分混淆。
2. 明确 `RankEngine` 是否废弃或改造成轻量排序器。
3. 增加评分数据质量字段。
4. 强化卖出评分：不是只靠止盈止损，还应支持评分恶化卖出。

### P2：增强项

1. 纳入龙虎榜。
2. 纳入融资融券。
3. 纳入解禁数据。
4. 纳入板块强度。
5. 事件驱动从关键词升级到轻量分类器。

---

## 八、测试建议

虽然本次没有运行测试，但后续测试应优先覆盖以下场景：

1. 负 PE 股票是否不会获得估值高分。
2. 低 PE、低 PB 股票在 `score_single()` 与 `score_batch()` 中方向一致。
3. ST 或 `risk_score >= 30` 股票是否总分为 0。
4. 强事件板块个股是否能获得事件乘数加成。
5. 同一行业已有多只持仓时，新候选股是否被降权。
6. 熊市状态下质量权重是否上升，时机权重是否下降。
7. 牛市状态下时机权重是否上升，质量权重是否下降。
8. 总分落入不同区间时，行动建议是否正确。
9. 数据源失败时是否能暴露 `source_errors` 或等价诊断字段。

---

## 九、最终判断

当前评分算法已经完成了从“设计文档”到“可执行工程模块”的关键跨越。

真正值得肯定的是：

1. 主评分引擎已经独立。
2. 模拟交易已经接入。
3. P0 批判意见不是只写在文档里，代码里确实有响应。
4. 事件驱动、反馈学习、组合惩罚都有雏形。

但它还不是一个最终稳定版本。

当前最大问题不是“没有实现”，而是：

```text
实现已经分散存在，但口径还没有完全统一；
功能已经跑通主链路，但关键细节还需要校准；
事件框架已经搭好，但行业映射这个核心传导环节偏弱。
```

我的建议是：下一阶段不要继续大规模扩功能，而是先做一次评分系统收敛：

1. 统一评分入口。
2. 修复单股/批量评分差异。
3. 补齐输出协议。
4. 修正事件行业映射。
5. 增加评分可解释与数据质量字段。

完成这些后，MoatX 的评分算法才适合进入更严肃的策略回测和模拟交易验证。

---

## 十、二轮验收发现（2026-04-26）

> 来源模型：ChatGPT 5.5  
> 验收方式：静态代码审查，不运行测试  
> 背景：其他模型已根据本报告进行一轮改造，本节记录二轮验收中仍需继续落地的问题。

### 10.1 验收结论

本轮改造不能直接通过最终验收，只能判定为“部分改造完成”。

已经看到的有效进展：

1. `ScoreBreakdown` 已补充 `action` 与 `suggested_weight` 字段。
2. `score_batch()` 已尝试补充 `action` 与 `suggested_weight` 输出。
3. `score_single()` 中 PE/PB 排名方向已经做过修正。
4. 模拟交易链路仍然接入 `ScoringEngine`。

但仍存在 3 个需要优先修复的问题，其中两个是 P0。

---

### 10.2 P0：批量估值排名方向仍然错误

位置：

```text
modules/scoring_engine.py
```

问题代码逻辑：

```python
pe_pct = float((spot_pe > pe_market).mean())
pe_pts = round((1 - pe_pct) * 15, 1)
```

问题解释：

`spot_pe > pe_market` 得到的是“市场中 PE 比当前股票更高的股票占比”。

对于低 PE 股票，这个占比会很高。

但随后代码又使用：

```python
1 - pe_pct
```

这会导致低 PE 股票反而拿低分，高 PE 股票反而可能拿高分。

这与估值分“低 PE 更优”的目标完全相反。

PB 部分也存在同类问题：

```python
pb_pct = float((spot_pb > pb_market).mean())
score += round((1 - pb_pct) * 10, 1)
```

建议修复方式二选一：

#### 方案 A：沿用当前 `>` 占比写法

```python
pe_pct = float((spot_pe > pe_market).mean())
pe_pts = round(pe_pct * 15, 1)

pb_pct = float((spot_pb > pb_market).mean())
pb_pts = round(pb_pct * 10, 1)
```

解释：比当前股票贵的股票越多，说明当前股票越便宜，应得分越高。

#### 方案 B：统一使用排名法

```python
pe_rank = spot_pe.rank(pct=True, ascending=True).iloc[spot_idx]
pe_pts = round((1 - pe_rank) * 15, 1)

pb_rank = spot_pb.rank(pct=True, ascending=True).iloc[spot_idx]
pb_pts = round((1 - pb_rank) * 10, 1)
```

建议优先采用方案 B，因为它和 `score_single()` 的修复方向更一致。

验收标准：

1. 同一市场样本中，低 PE 股票估值得分必须高于高 PE 股票。
2. 同一市场样本中，低 PB 股票估值得分必须高于高 PB 股票。
3. `score_batch()` 与 `score_single()` 的估值方向必须一致。

---

### 10.3 P0：全部风控否决时返回结果缺少关键列

位置：

```text
modules/scoring_engine.py
```

问题代码逻辑：

```python
active = df[df["vetoed"] == False].copy()
if active.empty:
    return df
```

问题解释：

如果候选股全部被风控否决，`score_batch()` 会直接返回 `df`。

此时返回结果中可能缺少：

```text
total
timing
sentiment
event_multiplier
action
suggested_weight
```

但下游模拟交易会访问：

```python
scored["total"]
```

因此在全部 veto 的情况下，可能直接触发 `KeyError`。

建议修复：

在 `active.empty` 早退前补齐标准输出列。

建议逻辑：

```python
if active.empty:
    df["quality"] = df.get("quality", 0.0)
    df["timing"] = 0.0
    df["sentiment"] = 0.0
    df["event_multiplier"] = 1.0
    df["total"] = 0.0
    df["action"] = "no_buy"
    df["suggested_weight"] = 0.0
    return df
```

更好的方式是抽一个统一 helper：

```python
def _finalize_score_output(df: pd.DataFrame) -> pd.DataFrame:
    ...
```

这样正常返回和早退返回都走同一套输出协议。

验收标准：

1. `score_batch()` 无论输入为空、全部 veto、部分 veto、无 veto，都返回稳定字段。
2. 至少包含：

```text
code
quality
timing
sentiment
event_multiplier
total
vetoed
veto_reason
action
suggested_weight
```

3. 全部 veto 时，所有股票：

```text
total = 0
action = no_buy
suggested_weight = 0
```

---

### 10.4 P1：事件行业反查仍未真正修复

位置：

```text
modules/event_driver.py
```

问题代码逻辑：

```python
df = ak.stock_board_industry_cons_ths(symbol=code)
```

问题解释：

`stock_board_industry_cons_ths()` 通常是按“行业板块名称”查询成分股，不适合作为“股票代码 → 所属行业”的反查接口。

因此当前 `_infer_sector()` 仍然很难稳定得到真实行业。

结果是事件映射配置中的：

```text
半导体
黄金
贵金属
石油行业
国防军工
光伏
储能
```

很难真正作用到个股，Layer 4 事件乘数会大幅弱化。

建议修复：

不要在 `_infer_sector()` 中逐只股票临时猜行业。

应该建立稳定缓存：

```text
code -> industries
code -> concepts
```

推荐实现方式：

1. 启动或首次调用时构建行业反查表：

```python
ak.stock_board_industry_name_ths()
ak.stock_board_industry_cons_ths(symbol=industry_name)
```

2. 构建概念反查表：

```python
ak.stock_board_concept_name_ths()
ak.stock_board_concept_cons_ths(symbol=concept_name)
```

3. `_infer_sector()` 返回多个标签，而不是单个字符串：

```python
def _infer_sectors(self, symbol: str) -> set[str]:
    return industry_tags | concept_tags
```

4. 事件匹配时只要任一标签命中 `sector_boosts` 即可加分。

建议伪代码：

```python
tags = self._infer_sectors(symbol)
for tag in tags:
    if tag in sector_boosts:
        score += sector_boosts[tag]
```

验收标准：

1. `600988`、`600760` 等股票能返回真实行业或概念标签，而不是只有“上海主板/深圳主板”。
2. `event_sector_map.toml` 中的行业/概念名称能与反查标签对齐。
3. 当宏观事件命中“半导体/黄金/石油/军工”等板块时，相关个股能实际获得事件乘数。

---

### 10.5 当前暂不强制但后续仍需处理的问题

以下问题不是本轮阻塞项，但不应遗忘：

1. `Analyzer.composite_score` 仍然存在，且报告中仍显示“综合评分”，容易和新 `ScoringEngine.total` 混淆。
2. `RankEngine` 仍然存在，定位需要明确：保留为轻量排序器，还是废弃。
3. `score_batch()` 虽然补了 `suggested_weight`，但 `simulation.py` 仍然自己按 `total` 重新计算仓位权重，没有真正使用该字段。
4. 评分结果仍缺少 `data_quality` / `source_errors` 一类诊断字段，后续测试定位数据源问题会比较困难。

---

### 10.6 给下一轮实现模型的明确任务

请优先完成以下 3 件事：

1. 修复 `score_batch()` 中 PE/PB 批量估值排名方向。
2. 修复 `score_batch()` 全部 veto 时的输出协议缺列问题。
3. 重构 `EventDriver` 的个股行业/概念反查能力，让 Layer 4 事件乘数真正能命中个股。

完成后请不要只说明“已修复”，需要提供：

1. 修改文件列表。
2. 每个问题对应的修复说明。
3. 是否影响已有模拟交易流程。
4. 如有测试，说明测试覆盖了哪些验收标准。
