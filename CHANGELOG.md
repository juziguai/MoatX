# 更新日志

MoatX 各版本重要变更记录。

## 1.1.0 - 2026-05-31

### 新增
- 重建知识图谱：183 节点、441 条边，层描述、导览步骤和核心模块摘要全部中文化。
- 新增新浪概念板块采集，通过 Market_Center.getHQNodeData 的 gn_* 节点码获取 214 个概念板块。
- 新增 Market_Center.getHQNodes 节点树解析器，自动发现新浪全部行业/概念板块节点码。
- 新增 sina.fetch_concept_boards() 函数。
- 概念板块降级链更新：同花顺-新浪-本地快照。

### 变更
- 重写 sina.fetch_industry_boards()：废弃已下线的 vIndustryRank HTML 解析，改用 Market_Center.getHQNodeData API（new_* 节点码）。
- sector.py 概念板块降级链加入新浪作为第二级。
- Dashboard 配置识别 outputLanguage: zh，界面中文化。

### 修复
- 修复 knowledge-graph.json 缺少 languages/frameworks/analyzedAt/gitCommitHash 字段导致 Dashboard 校验失败。
- 修复新浪行业板块接口下架后错误分类为解析失败，现正确标记为数据源不可用。
- 修复新浪板块模块缺少 SOURCE_UNAVAILABLE 导入。

### 验证
- 三源行情链（新浪/腾讯/东财）全部正常，交叉校验通过。
- StockData.get_daily() 返回 2026-05-29 交易日 OHLCV 数据正确。
- 同花顺行业板块返回 90 个板块，涨跌分布正确。
- Node.js schema 验证通过：183 节点、441 条边、零致命错误。

## 1.0.0 - 2026-05-30

### 新增
- 新增盘中异动雷达，分钟级监控 A 股异动。
- 新增 CLI 入口：python -m modules.cli tool intraday。
- 新增单股盘中回放和股票池雷达扫描。
- 新增板块共振评分，检测同一主题下的同步异动。
- 新增 SectorTagProvider 统一运行时标签查询。
- 新增短线策略回测支持，含固定股票池和诊断归因。
- 新增短线关注列表、模拟账户、目标价、止损及次日复盘工作流。

### 变更
- 项目版本从 0.1.0 升至 1.0.0。
- 项目阶段从 Alpha 升级为生产/稳定。
- 统一 sector_graph.toml 为板块/主题主图，stock_topic_exposure.toml 为个股主题覆盖层。
- 盘中板块共振改用统一的 SectorTagProvider 替代直接读取 TOML。
- 优化近义主题匹配：电力、贵金属、芯片、半导体等。
- 优化短线评分：历史参考、风控门槛、新闻因子、主题曝光、市场确认。

### 修复
- 修复 Python 3.14 / akshare 降级行为，可选数据源失败不再阻塞主分析路径。
- 修复公告过滤，避免无关公司公告混入个股报告。
- 修复收盘后时段的市场状态判断。
- 修复板块图谱缓存隔离，不同图谱路径不再共享过期缓存。

### 验证
- 包版本解析为 1.0.0。
- 盘中雷达在 2026-05-29 电力样本池验证通过。
- 板块共振可提升非电力主题（如芯片相关个股）评分。
- 统一标签查询验证通过：四川黄金、通富微电、歌尔股份、华能国际。