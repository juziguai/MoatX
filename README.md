# MoatX - A股量化分析系统

> 护城河量化分析系统 — 从数据到决策的A股技术分析工具

## 功能特性

- **数据获取** — akshare 实时/历史行情（东方财富主源 + 新浪财经备用）、财务数据、资金流向、龙虎榜、分红记录
- **技术指标** — MACD、KDJ、RSI、布林带、CCI、DMI、ATR、OBV 等 20+ 指标
- **信号系统** — 金叉/死叉、超买超卖、多头排列、均线背离
- **K线图表** — 5面板可视化（K线+均线+MACD+KDJ+RSI+成交量），支持深色/浅色主题
- **选股器** — 全市场按PE/PB/市值/换手率/涨幅过滤、千股千评、板块资金流、涨停股池
- **综合评分** — 趋势（25%）+ 估值（25%）+ 资金（25%）+ 动量（25%）
- **巴菲特视角** — 安全边际提醒、护城河自检、仓位原则

## 安装

```bash
cd D:/Tools/AI/Claude-code/MoatX
pip install -r requirements.txt
```

## 快速使用

```python
from modules import MoatXAnalyzer, MoatXScreener

analyzer = MoatXAnalyzer()

# 分析一只股票
report = analyzer.analyze("600519")  # 贵州茅台
print(analyzer.format_markdown(report))

# 弹出K线图表
analyzer.chart("600519", days=120, style="dark")

# 选股器
screener = MoatXScreener()
r = screener.scan_all(pe_range=(0, 30), turnover_min=5.0, pct_change_min=3.0)
print(screener.format_screening_result(r, "低PE高换手"))
```

## 项目结构

```
MoatX/
├── modules/
│   ├── __init__.py       # 统一导出
│   ├── stock_data.py     # 数据获取（akshare封装，自动代理容错）
│   ├── indicators.py     # 技术指标引擎（20+指标）
│   ├── analyzer.py       # 核心分析引擎 + 报告生成 + 选股入口
│   ├── charts.py         # K线图表可视化（5面板）
│   ├── screener.py       # 选股器（9种筛选维度）
│   └── rank_engine.py    # 综合评分引擎
├── skills/               # Claude Code Skill 定义
├── data/                 # 数据缓存目录（预留）
├── tests/               # 单元测试（预留）
├── requirements.txt
└── README.md
```

## 核心方法

### StockData — 数据获取

```python
sd = StockData()
df = sd.get_daily("600519", start_date="20240101")    # 日线（含双数据源自动容错）
df = sd.get_realtime_quote("600519")                     # 实时行情
d = sd.get_valuation("600519", current_price=1800)      # PE/PB/ROE估值
d = sd.get_money_flow("600519")                         # 资金流向
df = sd.get_limit_up()                                  # 今日涨停股池
d = sd.get_stock_info("600519")                         # 股票基本信息
l = sd.get_dividend("600519")                          # 历史分红
l = sd.get_major_shareholders("600519")                 # 前十大股东
d = sd.get_profit_sheet_summary("600519")              # 利润表摘要
d = sd.get_cash_flow_summary("600519")                # 现金流量表
```

### IndicatorEngine — 技术指标

```python
ind = IndicatorEngine()
df = ind.all_in_one(raw_df)  # 一次计算MACD/KDJ/RSI/布林带/CCI/DMI/ATR/OBV等20+指标
```

### MoatXAnalyzer — 核心分析

```python
analyzer = MoatXAnalyzer()

report = analyzer.analyze("600519", days=120)
# report 包含: symbol, name, price, pe, pb, roe, ma, trend, macd, kdj, rsi, boll,
#              signals, valuation, money_flow, profit_sheet, cash_flow,
#              dividend, profit_forecast, major_holders, buffett_view

print(analyzer.format_markdown(report))  # Markdown报告
analyzer.chart("600519")                  # 弹出K线图表

# 选股
result = analyzer.screen(
    pe_range=(0, 30),
    turnover_min=5.0,
    pct_change_min=3.0,
    sort_by="pct_change"
)
```

### MoatXScreener — 选股器

```python
s = MoatXScreener()

# 全市场过滤
r = s.scan_all(pe_range=(0, 30), turnover_min=5.0, pct_change_min=3.0)

# 资金流向排名
r = s.money_flow_rank(period="今日", direction="in", limit=30)

# 千股千评
r = s.screen_by_comment(sort_by="综合得分", ascending=False, limit=20)

# 热门板块
r = s.screen_hot_sectors(limit=10)

# 涨停股池
r = s.screen_limit_up(limit=20)

# 行业板块扫描
r = s.scan_industry("银行", top_n=5)

# 板块资金流
r = s.screen_by_sector_fund_flow(period="今日", sector_type="行业资金流")

# 市场关注度综合筛选
r = s.screen_hot_stocks(min_institutional=0.5, sort_by="score")

print(s.format_screening_result(r, "标题"))
```

### MoatXCharts — K线图表

```python
charts = MoatXCharts(df_with_indicators, symbol="600519")
charts.plot(save_path="600519.png", style="dark")  # 深色/浅色主题
```

## 指标覆盖

| 类别 | 指标 |
|------|------|
| 趋势 | MA5/10/20/60/120/250、多头排列判断 |
| MACD系 | DIF、DEA、MACD柱、金叉死叉 |
| KDJ系 | K、D、J值、超买超卖 |
| RSI系 | RSI6/12/24、强势弱势判断 |
| 布林带 | 上轨/中轨/下轨、当前位置 |
| 成交量 | OBV、量能均线、缩量放量判断 |
| 波动率 | ATR 平均真实波幅 |
| 顺势 | CCI、DMI/ADX/ADXR |
| 偏离率 | BIAS5/10/20 |

## 数据源容错

- 主数据源：东方财富（`stock_zh_a_hist`）
- 备用数据源：新浪财经（`stock_zh_a_daily`）
- 代理自动清除：初始化时自动清理 `HTTP_PROXY`/`HTTPS_PROXY` 环境变量

## 免责声明

本工具仅供学习研究，不构成任何投资建议。股市有风险，投资需谨慎。
