# MoatX 股票评分算法设计

> 目标：将 5 个维度 15+ 指标合成一个 0-100 的综合评分，驱动模拟交易的选股和仓位决策

---

## 一、设计原则

| 原则 | 说明 |
|------|------|
| **分层过滤，不做一锅炖** | 先否决不合格的，再给合格的打分 |
| **排名优于绝对值** | PE=15 高还是低？取决于它在全市场排第几 |
| **质量定方向，技术定时机** | 基本面决定买不买，技术面决定什么时候买 |
| **信号叠加不冲突** | 多个买入信号同时触发 → 加分；买卖信号对撞 → 不买 |
| **可解释** | 每只股票的得分必须能追溯来源："PE 排前 20% +20 分，KDJ 超卖 +15 分" |

---

## 二、四层评分架构

```
┌─────────────────────────────────────────┐
│  Layer 0: 风控一票否决                  │  ST、财务风险 ≥30 → 0 分
├─────────────────────────────────────────┤
│  Layer 1: 质量分（50%）                 │  估值 + 基本面 + 股东回报
├─────────────────────────────────────────┤
│  Layer 2: 时机分（35%）                 │  技术面
├─────────────────────────────────────────┤
│  Layer 3: 情绪分（15%）                 │  资金流向 + 动量
├─────────────────────────────────────────┤
│  Layer 4: 事件乘数（×0.6~1.4）          │  新闻 + 公告 + 宏观事件
├─────────────────────────────────────────┤
│  Final: (L1+L2+L3) × L4_event_multiplier
└─────────────────────────────────────────┘
```

---

## 三、Layer 0 — 风控一票否决

**来源：** `StockData.check_financial_risk()` → `risk_score`

```
if ST or *ST → score = 0（一票否决）
if risk_score ≥ 30 → score = 0（一票否决）
if PE ≤ 0 or PB ≤ 0 → 质量分 -50%（亏损股，但允许技术面交易）
```

**现有代码覆盖：** ✅ `risk_checker.py` 5 个子检查

---

## 四、Layer 1 — 质量分（50 分）

### 4.1 估值（25 分）— 全市场排名法

**不对比绝对值，比全市场排位：**

```python
# 对全部候选股按 PE 从小到大排序
pe_rank = candidates["pe"].rank(pct=True)  # 0.0~1.0，越小越好
# 对全部候选股按 PB 从小到大排序
pb_rank = candidates["pb"].rank(pct=True)

valuation_score = ((1 - pe_rank) * 15 + (1 - pb_rank) * 10)  # 0~25
```

| 指标 | 满分 | 计算方式 |
|------|------|----------|
| PE 排名 | 15 | `(1 - pe_percentile) × 15` |
| PB 排名 | 10 | `(1 - pb_percentile) × 10` |

**为什么用排名？** PE=5 在银行业是正常值，在科技业是极度低估。排名天然处理了行业差异，不需要维护行业对照表。

### 4.2 盈利能力（15 分）

| 指标 | 满分 | 公式 |
|------|------|------|
| ROE | 10 | `ROE ≥ 20%` → 10, `ROE ≥ 10%` → 6, `ROE ≥ 5%` → 3, 负值 → 0 |
| 毛利率 | 5 | `margin ≥ 30%` → 5, `≥ 15%` → 3, `< 15%` → 0 |

**来源：** `StockData.get_profit_sheet_summary()` + `StockData.get_valuation()`

### 4.3 财务健康（10 分）

| 指标 | 满分 | 公式 |
|------|------|------|
| 资产负债率 | 5 | `ratio ≤ 40%` → 5, `≤ 60%` → 3, `≤ 80%` → 1, `> 80%` → 0 |
| 经营现金流 | 5 | `free_cf > 0` → 5，否则 → 0 |

**来源：** `StockData.get_cash_flow_summary()` + `_check_debt_ratio()`

---

## 五、Layer 2 — 时机分（35 分）

### 5.1 均线系统（10 分）

```python
ma_score = 0
if price > ma5:  ma_score += 2
if price > ma10: ma_score += 2
if price > ma20: ma_score += 3   # 中期趋势权重更高
if price > ma60: ma_score += 3
# 多头排列加分
if ma5 > ma10 > ma20: ma_score += 3   # bonus
```

**来源：** `IndicatorEngine.all_in_one()` → `ma5/ma10/ma20/ma60`

### 5.2 MACD（8 分）

```python
macd_score = 0
if dif > dea: macd_score += 3          # 多头排列
if macd_hist > 0: macd_score += 2      # 红柱
if golden_cross刚刚发生: macd_score += 3  # 金叉加分
if death_cross刚刚发生: macd_score -= 5   # 死叉大扣
```

### 5.3 KDJ（7 分）

```python
kdj_score = 0
if J < 0:   kdj_score += 7   # 深度超卖，反弹概率极高
elif J < 20: kdj_score += 5  # 超卖区
elif J < 40: kdj_score += 3  # 偏卖区
elif J > 85: kdj_score -= 5  # 超买区，追高风险
elif J > 70: kdj_score -= 2  # 偏买区
```

### 5.4 布林带 + RSI（10 分）

```python
bb_score = 0
boll_pos = (price - lower) / (upper - lower)  # 0~1

if boll_pos < 0.1:   bb_score += 5   # 触下轨，支撑位
elif boll_pos < 0.3: bb_score += 3   # 下半区
elif boll_pos > 0.9: bb_score -= 5   # 触上轨，压力位

if RSI < 30:   bb_score += 5   # 超卖反弹
elif RSI < 40: bb_score += 3   # 偏弱
elif RSI > 70: bb_score -= 5   # 超买
elif RSI > 60: bb_score -= 2
```

---

## 六、Layer 3 — 情绪加分（15 分）

### 6.1 资金流向（8 分）

```python
flow_score = 0
if main_net_inflow_pct > 0:  flow_score += 4   # 主力净流入
if main_net_inflow_pct > 5:  flow_score += 4   # 大幅流入再加
# 换手率健康度（太高=出货嫌疑，太低=无人关注）
if 3 <= turnover <= 15: flow_score += 4
elif 1 <= turnover < 3:  flow_score += 2
else: flow_score += 0  # 极高换手率不加分
```

### 6.2 动量与板块（7 分）

```python
momentum_score = 0
if 2 <= pct_change <= 5:  momentum_score += 4   # 温和上涨
elif 1 <= pct_change < 2:  momentum_score += 2
elif pct_change < -5:      momentum_score -= 3   # 暴跌减分
```

---

## 七、Layer 4 — 事件驱动加分（±20 分，乘数型）

### 7.1 为什么需要这一层

前三层（质量/时机/情绪）基于结构化数据，能回答"这只股票便宜吗""趋势好吗""资金在流入吗"。但重大市场事件到来时，这些全部失效：

```
2025 伊朗封锁霍尔木兹海峡 → 石油运输瘫痪 → 油价 3 天翻倍
  → PE 来不及反映（财报滞后 3 个月）
  → KDJ 已在高位钝化（技术面失效）
  → 主力资金才开始流入（资金面滞后 1-2 天）

唯一能抢先捕获信号的是：新闻事件 → 行业映射 → 板块打分
```

### 7.2 事件→行业映射引擎

**核心原理：关键词命中 → 行业标签 → 该行业所有股票获得情绪溢价**

```
"霍尔木兹海峡" ∩ "封锁" ∩ "伊朗"
  → keywords: [石油, 原油, 能源, 航运, 军工, 中东]
  → sectors: [石油行业, 航运港口, 国防军工, 贵金属]
  → 这些板块所有股票：事件分 +15~20

同一天另一条新闻：
"美联储降息 50bp"
  → keywords: [降息, 利率, 宽松, 流动性]
  → sectors: [房地产, 券商, 银行, 有色金属, 黄金]
  → 这些板块：事件分 +10~15
```

### 7.3 四类新闻源与处理方式

| 新闻类型 | 来源 | 处理方式 | 影响范围 |
|----------|------|----------|----------|
| **宏观事件** | 财经头条、央行公告 | 关键词→行业映射 | 板块级，影响该板块所有股票 |
| **行业政策** | 发改委、工信部公告 | 关键词→行业映射 | 板块级 |
| **个股公告** | CNINFO 已接入 | 正负面关键词扫描 | 单股级 |
| **国际市场** | 原油/黄金/汇率变动 | 阈值触发 | 板块级 |

### 7.4 个股公告情绪分析

**CNINFO 公告已经是现有能力，扩展为正负面判断：**

```python
# 正面关键词 → 加分
POSITIVE_KW = [
    ("业绩预增", 10), ("重大合同", 12), ("中标", 10), ("回购", 8),
    ("增持", 8), ("高分红", 6), ("新产品", 5), ("专利", 4),
    ("产能扩张", 7), ("战略合作", 6), ("行业龙头", 4),
]

# 负面关键词 → 减分（在 Layer 0 已处理大量负面）
NEGATIVE_KW = [
    ("减持", -8), ("质押", -6), ("诉讼", -8), ("处罚", -10),
    ("董事长辞职", -5), ("商誉减值", -12),
]
```

**来源：** `_check_risk_notices()` 已接入 CNINFO，扩展为通用公告扫描 + 情绪打分

### 7.5 事件时效衰减

新闻不是永久有效的。霍尔木兹海峡封锁第 1 天影响最大，第 10 天市场已消化。

```python
def event_decay(event_date, impact_score):
    days_ago = (today - event_date).days
    if days_ago <= 1:   return impact_score * 1.0    # 当天：100%
    elif days_ago <= 3:  return impact_score * 0.7   # 3天内：70%
    elif days_ago <= 7:  return impact_score * 0.3   # 1周：30%
    elif days_ago <= 14: return impact_score * 0.1   # 2周：10%
    else:                return 0                     # 失效
```

### 7.6 事件分对总分的修正方式

事件分不和其他分加在一起——它是**乘数修正**：

```python
# 不是: final = quality + timing + sentiment + event
# 而是: final = min(100, base_score × event_multiplier)

event_multiplier = 1.0
if sector_boost > 0:
    event_multiplier = min(1.4, 1.0 + sector_boost / 100)  # 最多 40% 溢价

final_score = min(100, base_score * event_multiplier)
```

**为什么用乘数？** 因为事件不改变股票的基本面质量，但放大了它的短期价值。一只 70 分的石油股在霍尔木兹事件下应该是 98 分，不是 90 分。

### 7.7 实现清单

| 组件 | 说明 | 复杂度 |
|------|------|--------|
| `data/event_sector_map.toml` | 关键词→行业映射表 | 低 |
| `modules/event_driver.py` | 事件驱动引擎 | 高 |
| CNINFO 公告情绪扩展 | `_check_risk_notices` → `_scan_sentiment` | 中 |
| 财经头条抓取 | 东方财富/新浪财经新闻 API | 中 |
| 事件时效衰减 | 按日期计算影响权重 | 低 |

---

## 八、综合评分公式（修正版）

```
 Layer 0: 风控否决（ST → 0分）
    ↓ 通过
 Layer 1: 质量分（0-50）
 Layer 2: 时机分（0-35）
 Layer 3: 情绪分（0-15）
    ↓ 合成基础分 0-100
 Layer 4: 事件乘数（×0.6~1.4）
    ↓
最终评分 0-100
```

```python
def score_stock(symbol, candidate_data, event_boost=0):
    # Layer 0: 风控否决
    risk = check_financial_risk(symbol)
    if risk["risk_score"] >= 30:
        return 0, "财务高风险"

    # Layer 1: 质量分（50）
    quality = (
        valuation_score(pe_rank, pb_rank) +
        profitability_score(roe, margin)  +
        health_score(debt, free_cf)
    )  # → 0~50

    # Layer 2: 时机分（35）
    timing = (
        ma_score(price, ma5, ma10, ma20, ma60) +
        macd_score(dif, dea, hist, cross)      +
        kdj_score(J)                            +
        bb_rsi_score(boll_pos, RSI)
    )  # → 0~35

    # Layer 3: 情绪分（15）
    sentiment = (
        flow_score(main_inflow_pct, turnover) +
        momentum_score(pct_change)
    )  # → 0~15

    base = quality + timing + sentiment  # 0~100

    # Layer 4: 事件乘数修正（±40%）
    multiplier = 1.0 + event_boost / 100
    final = max(0, min(100, base * multiplier))

    return final, reason
```

---

## 九、分数→行动决策

| 分数区间 | 建议行动 | 仓位 |
|----------|----------|------|
| 0 | 不买 | 0% |
| 1-40 | 仅观察 | 0% |
| 41-55 | 轻仓试探 | 单票 5% |
| 56-70 | 正常买入 | 按排名权重分配 |
| 71-85 | 重仓 | 单票上限 15% |
| 86-100 | 重仓+ | 单票上限 20% |

**仓位分配逻辑（替换当前平均主义）：**

```python
# 按分数加权，不是按排名
total_score = sum(s.score for s in candidates)
for s in candidates:
    if s.score >= 56:
        weight = s.score / total_score  # 分数越高仓位越重
        budget = available_cash * weight
        budget = min(budget, max_per_stock)  # 但有上限
        shares = (budget / price // 100) * 100
```

---

## 十、与现有代码对照

| 评分维度 | 需要的指标 | 现有代码获取方式 | 缺口 |
|----------|-----------|-----------------|------|
| PE/PB 排名 | 全市场 PE/PB | `StockData.get_spot()` 返回 `pe`/`pb` 列 | 无，可用 |
| ROE | ROE% | `StockData.get_valuation()` → `roe` | 单股查询，需批量 |
| 毛利率 | 毛利率 | `get_profit_sheet_summary()` → `gross_margin` | 单股查询慢 |
| 资产负债率 | debt_ratio | `_check_debt_ratio()` 内部计算 | 已嵌入风险检测 |
| 经营现金流 | free_cf | `get_cash_flow_summary()` → `free_cf` | 单股查询慢 |
| 均线 | ma5/10/20/60 | `IndicatorEngine.all_in_one()` | 需日线数据 |
| MACD/KDJ/RSI | dif/dea/J/rsi | 同上 | 同上 |
| 布林带 | upper/mid/lower | 同上 | 同上 |
| 资金流向 | 主力净流入占比 | `get_money_flow_summary()` | 单股查询慢 |
| 换手率 | turnover% | `get_spot()` 已含 | 无 |
| 风险评分 | risk_score | `check_financial_risk()` | ✅ 已有并行批量 |

---

## 十一、性能考量

**核心矛盾：** 质量分需要的财务数据（ROE/毛利率/现金流）是单股 API 查询，选 100 只 × 3 秒/只 = 300 秒。而时机分需要的技术指标数据（日线 OHLCV）也需要网络获取。

**解决方案 — 两阶段异步评分：**

```
阶段 1（快，不阻塞买入）：
  估值排名（PE/PB 已有 spot 数据）+ 技术指标（已有日线缓存）+ 换手率
  → 产出初筛分，选出 top 20

阶段 2（慢，对 top 20 深度分析）：
  ROE + 毛利率 + 现金流 + 资金流向
  → 产出最终质量分

阶段 3（异步，不影响当日交易）：
  对已持仓股票每日更新质量分
  → 用于卖出决策（质量分持续下降 → 减仓信号）
```

---

## 十二、评分引擎接口设计

```python
class ScoringEngine:
    def __init__(self, sim_cfg: SimulationSettings):
        self._cfg = sim_cfg
        self._sd = StockData()
        self._ind = IndicatorEngine()
        self._ranker = RankEngine()

    def score_batch(self, symbols: list[str]) -> pd.DataFrame:
        """
        批量评分，返回 DataFrame：
          symbol, quality, timing, sentiment, total, action, weight
        """

    def score_single(self, symbol: str) -> dict:
        """单股深度评分，用于卖出决策"""

    def _veto_check(self, symbol: str) -> bool:
        """Layer 0: 风控否决"""

    def _quality_score(self, df: pd.DataFrame) -> pd.Series:
        """Layer 1: 质量分"""

    def _timing_score(self, symbol: str) -> float:
        """Layer 2: 时机分（需日线数据）"""

    def _sentiment_score(self, row) -> float:
        """Layer 3: 情绪分"""
```

---

## 十三、验收标准

- 对 100 只候选股评分，0 分 ≤ 每只 ≤ 100 分
- 0 分股全是 ST 或高风险
- 80 分以上股至少满足：估值低 + 技术买入信号 + 资金流入
- 同一只股票不同交易日分数可变化（体现时机分）
- 评分结果可解释：每只股打出 `reason` 字段说明加分项
