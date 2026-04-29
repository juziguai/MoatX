# MoatX 已知错误模式

本文档记录项目中已知的错误模式、原因和解决方案。

## 1. 网络异常

### akshare 返回空 DataFrame
- 症状：`DataFrame.empty == True`
- 原因：股票代码错误 / 停牌 / 网络超时
- 处理：调用方检查 `df.empty` 后返回默认值

### requests ConnectionError
- 症状：`ConnectionError: Max retries exceeded`
- 原因：网络不稳定 / 目标站点拒绝连接
- 处理：`retry_on_network_error` 装饰器自动重试

### Sina API 返回非JSON
- 症状：`json.JSONDecodeError`
- 原因：Sina 接口频率限制或返回错误页
- 处理：`get_daily_prices` 中的 try/except 捕获并跳过

## 2. 数据异常

### 新浪日线数据畸形记录
- 症状：`KeyError: 'close'`
- 原因：新浪返回的数据结构不完整
- 位置：`modules/stock_data.py get_daily_prices`
- 状态：✅ 已修复（try/except 跳过畸形记录）

### 缓存穿透导致重复请求
- 症状：同一 symbol 短时间内重复请求
- 原因：TTL=0 或缓存未命中
- 状态：✅ 已通过 RLock 保护

## 3. 数据库异常

### CHECK 约束违反
- 症状：`CHECK constraint failed: shares >= 0`
- 原因：代码层面传入了负数
- 状态：✅ 已加防御性校验 + CHECK 约束

### 事务回滚
- 症状：`sqlite3.OperationalError: database is locked`
- 原因：多线程同时写 SQLite
- 处理：`ROLLBACK` 后重试（已在 record_trade 中实现）

## 4. 配置异常

### MOATX_* 环境变量未识别
- 原因：key 格式不对（应为全大写+下划线）
- 正确格式：`MOATX_CRAWLER_TIMEOUT`
- 调试：`python -c "from modules.config import _env_key; print(_env_key('crawler.timeout'))"`

## 5. Schema 迁移

### `_ensure_columns` 必须覆盖所有新列
- 症状：`sqlite3.OperationalError: table xxx has no column named yyy`
- 原因：`CREATE TABLE IF NOT EXISTS` 不会修改已存在的表，旧数据库缺少新增列
- 处理：在 `_init_db()` 的建表语句后追加 `self._ensure_columns("table", [("new_col", "TYPE")])`
- 位置：`modules/portfolio.py` `_init_db()` — 2026-04-26 `candidate_results` 表缺 4 列

### CLI 重构后调度器命令路径
- 症状：调度器 `_run_module("modules.cli_portfolio", ["check"])` 报 `invalid choice: 'check'`
- 原因：CLI 从 `cli_portfolio.py` 重构到 `cli/` 包后命令名变更（`check` → `alert check`，`snapshot` 不存在，`signal run` → `tool signal run`）
- 处理：同步更新 `scheduler.py` 中的命令路径
- 位置：`modules/scheduler.py:118,123,128` — 2026-04-26 修复

### `datasource.py` 导入遗漏
- 症状：`NameError: name 'to_sina_code' is not defined`
- 原因：`utils.py` 提取工具函数后，部分调用方未更新 import 语句
- 处理：`SinaSource.fetch_quotes()` 需导入 `to_sina_code`
- 位置：`modules/datasource.py:14` — 2026-04-26 修复

## 6. 已验证不复发的历史Bug

| Bug | 原因 | 修复 | 日期 |
|-----|------|------|------|
| prev_close=0 导致除零 | get_daily_prices 未检查 prev_close | 加 try/except | 2026-04 |
| 畸形新浪数据 KeyError | rec["close"] 访问不存在的 key | 同样加 try/except | 2026-04 |
| record_trade 原子性 | trade INSERT 先 commit | 事务 try/rollback | 2026-04 |
| import_trades 部分提交 | 逐行 commit | 改为整体事务 | 2026-04 |
