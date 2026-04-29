# 东方财富接口状态说明

更新时间：2026-04-24

本文档记录 MoatX 对东方财富行情接口的当前判断。结论先行：浏览器可以正常访问东方财富网页，但 Python 侧直接访问 EastMoney push2 批量接口仍不稳定，因此项目主线不应依赖它作为唯一行情源。

## 当前结论

| 场景 | 状态 | 说明 |
| --- | --- | --- |
| 浏览器访问 `https://quote.eastmoney.com/` | 正常 | 用户本机 Chrome 可以打开 |
| 浏览器访问个股页 `https://quote.eastmoney.com/sh600734.html` | 正常 | 页面可显示实时行情 |
| Python 访问 `82.push2.eastmoney.com/api/qt/clist/get` | 不稳定 | 常见远端断开或空响应 |
| AkShare `stock_zh_a_spot_em()` | 不稳定 | 底层依赖 EastMoney push2 |
| Chrome CDP 直接请求 push2 批量接口 | 不稳定 | 仍可能 `ERR_EMPTY_RESPONSE` |
| Sina 全市场快照 | 当前更稳 | 项目主线优先使用 |

## 观察到的错误

Python 或 Playwright/CDP 请求中曾出现：

```text
RemoteDisconnected('Remote end closed connection without response')
Connection closed abruptly
Page.goto: net::ERR_EMPTY_RESPONSE
ProxyError('Unable to connect to proxy')
```

这说明问题不是“网页是否需要登录”。当前更像是 EastMoney push2 批量接口对非典型请求链路、网络路径、代理状态或请求频率更敏感。

## 已尝试方案

### 模拟 Chrome 请求头

尝试过携带 Chrome UA、Referer、Cookie 等请求头。

结果：未稳定解决批量接口远端断开。

### 环境变量 Cookie

尝试过设置：

```powershell
$env:MOATX_EASTMONEY_COOKIE = "..."
```

结果：浏览器网页可访问，但 Python 批量接口仍可能断开。

### Chrome CDP

尝试通过用户本机 Chrome DevTools Protocol 复用浏览器环境：

```powershell
& "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="$env:TEMP\moatx-chrome"
$env:MOATX_CHROME_CDP_URL = "http://127.0.0.1:9222"
```

结果：CDP 可连接，但 push2 批量接口仍出现 `ERR_EMPTY_RESPONSE`。

### AkShare EastMoney 接口

尝试过 `ak.stock_zh_a_spot_em()`。

结果：同样受 push2 批量接口影响，不适合作为当前唯一主源。

## 接口参考

### 个股实时行情

```text
https://push2.eastmoney.com/api/qt/stock/get
```

常用参数：

| 参数 | 说明 |
| --- | --- |
| `secid` | 市场.代码，沪市常用 `1.600734`，深市常用 `0.300059` |
| `fields` | 返回字段列表 |
| `ut` | token，可为空或使用公开页面里的固定值 |

示例：

```text
https://push2.eastmoney.com/api/qt/stock/get?secid=1.600734&fields=f43,f44,f45,f46,f47,f48,f57,f58,f169,f170,f116,f115
```

### 全市场/板块列表

```text
https://82.push2.eastmoney.com/api/qt/clist/get
```

常用参数：

| 参数 | 说明 |
| --- | --- |
| `pn` | 页码 |
| `pz` | 每页数量 |
| `fid` | 排序字段 |
| `fs` | 市场过滤 |
| `fields` | 返回字段 |

该接口是当前最不稳定的部分。

## 当前项目策略

1. 全市场扫描优先使用 Sina 数据源。
2. EastMoney push2 不作为唯一主源。
3. 保留 EastMoney 文档和排障记录，后续只在证明稳定后再引入主链路。
4. 外部行情请求默认清理代理变量，避免系统代理造成额外失败。
5. 若后续继续尝试 EastMoney，应先做独立诊断脚本，不直接改主业务链路。

## 后续排障建议

如果要继续排查 EastMoney：

1. 对比浏览器 DevTools Network 中实际成功请求的完整 URL、请求头和响应。
2. 分别测试 `push2.eastmoney.com` 与 `82.push2.eastmoney.com`。
3. 降低 `pz` 和请求频率，观察是否存在频控。
4. 固定 DNS 或尝试不同网络出口，确认是否为线路问题。
5. 将诊断脚本放在 `scripts/` 下，先输出原始错误和响应头，不直接接入 `scan_all()`。
