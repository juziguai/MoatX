# MoatX 爬虫/API 探测快速使用

## 1. 单接口探测

```powershell
python scripts\probe_api.py https://httpbin.org/json --sort-score
```

输出会包含：

- `status_code`：HTTP 状态码
- `response_kind`：`json` / `html` / `text`
- `score`：接口可用性评分，0-100
- `json_keys`：JSON 顶层字段
- `stock_fields`：命中的股票字段
- `challenge_detected`：是否命中验证码/风控/登录校验

## 2. 带参数探测

```powershell
python scripts\probe_api.py https://httpbin.org/get --param symbol=000001 --header "X-Test: MoatX" --sort-score
```

常用参数：

```powershell
--method POST
--header "User-Agent: MoatX"
--cookie "sid=xxx"
--param symbol=000001
--json-body-file body.json
```

## 3. 批量并发探测

准备 `urls.txt`：

```text
https://example.com/api/a
https://example.com/api/b
https://example.com/api/c
```

执行：

```powershell
python scripts\probe_api.py --file urls.txt --workers 16 --sort-score --output result.jsonl
```

## 4. 从网页发现接口并验证

```powershell
python scripts\probe_api.py https://example.com --probe-discovered --workers 8 --sort-score --output discovered.jsonl
```

说明：

- 先扫描 HTML 中疑似 URL
- 自动过滤静态资源
- 再并发探测发现到的接口

## 4.1 深度扫描外部 JS 中的 API

很多行情网站的真实 API 不在 HTML 里，而藏在外部 JS 包中。使用：

```powershell
python scripts\probe_api.py https://quote.eastmoney.com/sh600988.html --probe-js-apis --workers 10 --sort-score --min-score 80 --output data\eastmoney_sh600988_js_api_result.jsonl
```

可自动：

- 下载页面外部 `<script src="...">`
- 从 JS 中提取 `/api/...`、`push2.eastmoney.com/api/...` 等候选接口
- 根据页面 URL 推断股票 `secid`
- 补全东方财富常见行情接口候选
- 并发探测并按 `score` 排序

如果页面无法推断股票代码，可手工指定：

```powershell
python scripts\probe_api.py https://quote.eastmoney.com/sh600988.html --probe-js-apis --stock-code 600988 --market 1 --sort-score
```

只看接口语义摘要：

```powershell
python scripts\probe_api.py https://quote.eastmoney.com/sh600760.html --probe-js-apis --workers 10 --sort-score --min-score 80 --semantic-only
```

东方财富 F10 核心数据接口语义识别：

```powershell
python scripts\probe_api.py "https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/ZYZBAjaxNew?type=1&code=SH600760" --semantic-only
```

目前可识别的东方财富模块包括：

- `quote_minute`：分时走势
- `quote_ticks`：逐笔成交
- `quote_kline`：K线行情
- `quote_snapshot`：实时行情/公司核心数据
- `related_boards`：所属板块
- `period_change`：阶段涨幅
- `f10_finance`：F10财务核心指标
- `f10_company`：F10公司概况

## 5. HAR 文件分析

在浏览器 DevTools 的 Network 面板导出 HAR 后：

```powershell
python scripts\probe_api.py --har network.har --analyze-har-body --sort-score --output apis.jsonl
```

适合分析网页正常访问过程中加载过的合法接口。

## 6. Cookie 接管

支持 JSON cookie 文件：

```json
{
  "sid": "xxx",
  "token": "yyy"
}
```

执行：

```powershell
python scripts\probe_api.py https://example.com/api --cookie-file cookies.json --sort-score
```

也支持 Netscape `cookies.txt` 格式。

## 7. 风控/验证码识别与快照

```powershell
python scripts\probe_api.py --file urls.txt --workers 16 --snapshot-dir data\probe_snapshots --snapshot-challenges-only
```

结果中会标记：

- `challenge_detected`
- `challenge_type`: `captcha` / `risk_control` / `login_required`
- `challenge_reasons`

## 8. 主 CLI 入口

也可以通过 MoatX 主 CLI 使用：

```powershell
python -m modules.cli_portfolio probe-api https://httpbin.org/get --param symbol=000001 --sort-score
```

## 9. 推荐工作流

```powershell
# 1. 浏览器打开目标网站，正常操作并导出 HAR
python scripts\probe_api.py --har network.har --analyze-har-body --sort-score --output apis.jsonl

# 2. 从高分接口中挑 URL，再批量验证
python scripts\probe_api.py --file urls.txt --workers 16 --min-score 60 --sort-score --output result.csv

# 3. 如需登录态，导入自己的 Cookie
python scripts\probe_api.py --file urls.txt --cookie-file cookies.txt --workers 16 --sort-score
```

## 10. 东方财富实测查询示例

### 10.1 查询股票最近分时价

示例：实达集团 `600734`

```powershell
python scripts\probe_api.py "https://push2.eastmoney.com/api/qt/stock/trends2/get?secid=1.600734&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58&ut=fa5fd1943c7b386f172d6893dbfba10b&iscr=0&iscca=0&ndays=1" --semantic-only
```

接口语义：

- 模块：分时走势
- 字段：股票代码、股票名称、前收盘、分钟行情
- `data.trends` 格式：`时间,价格/开盘,收盘,最高,最低,成交量,成交额,均价`

### 10.2 查询指定分钟价格和成交量

示例：中航沈飞 `600760` 在 `2026-04-24 13:00` 附近。

东方财富分时数据没有 `13:00` 这一分钟，午后第一条是 `13:01`：

```text
2026-04-24 13:01,48.44,48.47,48.50,48.43,579,2805703.00,48.597
```

可解释为：

- 时间：`2026-04-24 13:01`
- 价格：`48.44`
- 成交量：`579` 手
- 成交额：`2,805,703 元`

### 10.3 查询所属大板块

示例：中航沈飞 `600760`

```powershell
python scripts\probe_api.py "https://push2.eastmoney.com/api/qt/slist/get?fltt=1&invt=2&fields=f12,f13,f14,f3,f128&secid=1.600760&ut=fa5fd1943c7b386f172d6893dbfba10b&pi=0&po=1&np=1&pz=30&spt=3" --semantic-only
```

主要板块：

- 国防军工
- 航天航空
- 航空装备Ⅱ / 航空装备Ⅲ
- 军工
- 大飞机
- 无人机
- 军民融合

### 10.4 查询公司核心数据

示例：中航沈飞 `600760`

```powershell
python scripts\probe_api.py "https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/ZYZBAjaxNew?type=1&code=SH600760" --semantic-only
```

接口语义：

- 模块：F10财务核心指标
- 字段：基本每股收益、扣非每股收益、每股净资产、营业总收入、归母净利润、ROE、资产负债率等

已实测最新记录：

- 报告期：`2025年报`
- 基本每股收益：`1.26 元`
- 营业总收入：`446.56 亿元`
- 归母净利润：`35.18 亿元`
- 加权 ROE：`18.35%`
- 资产负债率：`66.88%`

## 11. 当前边界

- 不破解验证码、不绕过风控。
- 支持识别验证码/风控/登录校验，并保存快照方便人工处理。
- 支持通过浏览器正常访问后导出的 HAR/Cookie 做合法接口分析。
