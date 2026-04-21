---
name: moatx-analyze
description: A股量化分析 - 技术指标、K线图表、选股器、巴菲特视角
---

# MoatX 分析技能

用 MoatX 量化系统分析 A 股股票。

## 触发词

- "分析股票" + 股票代码
- "技术分析" + 股票代码
- "帮我看看" + 股票代码 + "怎么样"
- "分析" + 股票代码
- "选股" + 条件
- "画" + 股票代码 + "K线"
- "帮我选" + 条件描述

## 使用方法

```python
from modules import MoatXAnalyzer, MoatXScreener

analyzer = MoatXAnalyzer()
screener = MoatXScreener()

# 分析股票
report = analyzer.analyze("600519")
print(analyzer.format_markdown(report))

# 弹出K线图表
analyzer.chart("600519", days=120, style="dark")

# 选股
result = analyzer.screen(pe_range=(0, 30), turnover_min=5.0, pct_change_min=3.0)
```

## 分析维度

| 维度 | 内容 |
|------|------|
| 实时行情 | 现价、涨跌幅、换手率、PE/PB |
| 均线系统 | MA5/10/20/60/120/250 多头排列判断 |
| MACD | DIF、DEA、金叉死叉信号 |
| KDJ | K/D/J值、超买超卖 |
| RSI | RSI6/12/24、相对强弱 |
| 布林带 | 上下轨、当前位置 |
| 资金流向 | 主力净流入、超大单、大单、中小单 |
| 利润表 | 营业收入、净利润、毛利率、净利率 |
| 现金流量表 | 经营/投资/筹资现金流、自由现金流 |
| 历史分红 | 近5次分红方案 |
| 股东结构 | 前十大股东、持股比例 |
| 估值 | PE/PB 估值区间判断 |
| 巴菲特视角 | 安全边际、护城河自检 |

## 选股器方法

```python
# 全市场过滤（PE/换手率/涨幅/市值）
screener.scan_all(pe_range=(0, 30), turnover_min=5.0, pct_change_min=3.0)

# 资金流向排名
screener.money_flow_rank(period="今日", direction="in")

# 千股千评（机构参与度/综合得分/关注指数）
screener.screen_by_comment(sort_by="综合得分")

# 热门板块
screener.screen_hot_sectors(limit=10)

# 涨停股池
screener.screen_limit_up(limit=20)

# 行业板块扫描
screener.scan_industry("银行")

# 板块资金流排名
screener.screen_by_sector_fund_flow(period="今日", sector_type="行业资金流")
```

## 输出示例

分析完成后返回包含以下内容的字典：
- `trend` — 趋势状态（强势多头/短线空头/震荡整理）
- `signals` — 技术信号（MACD/KDJ/RSI/布林带）
- `composite_score` — 综合评分 0-100
- `buffett_view` — 巴菲特视角反思 + 操作建议
- `profit_sheet` — 利润表摘要
- `cash_flow` — 现金流量表摘要
- `dividend` — 历史分红记录
- `major_holders` — 前十大股东

## 注意

- 需要安装依赖: `pip install -r requirements.txt`
- 数据来源: akshare（东方财富主源 + 新浪财经备用）
- 自动清除代理环境变量，无需手动设置
- 股票代码格式: "600519" 或 "600519.SH"
