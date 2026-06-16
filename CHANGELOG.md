# 更新日志

MoatX 各版本重要变更记录。

## 1.6.0 - 2026-06-16

### 新增
- 新增极速盘中决策引擎 `quick_decision`：基于实时行情、日线缓存、主题标签和新闻事件因子输出买/观察/不买判断。
- 新增 CLI 入口：`python -m modules.cli tool quick-decision`，支持指定股票、短线 watchlist 批量判断、JSON 输出和本地记录。
- 新增 quick decision 持久化闭环：`quick_decision_runs`、`quick_decision_rows`、`quick_decision_evaluations` 三张表，数据库迁移至 `SCHEMA_VERSION=18`。
- 新增 T+1/T+3/T+5 自动后验评价，支持保存收益、最大回撤、命中结果，并通过唯一键重复更新不重复插入。
- 新增评价汇总面板：`quick-decision summary/dashboard`，按动作、分数段、主题标签、事件板块统计成功率、平均收益和平均回撤。
- 新增调度任务 `quick_decision_evaluate`，工作日 15:45 自动执行极速决策后验评价。

### 变更
- `quick-decision` 默认记录每次判断，可通过 `--no-save` 关闭；`review` 保持只读查看，`evaluate` 负责落库评价。
- 快速决策接入 `event_news_factors`，只对有效期内的新闻事件因子加分/扣分，过期因子仅提示不参与评分。
- 调度器状态输出增强，能区分进程不存在、PID 被其他进程复用、调度 profile 不匹配等情况。
- DataSourceManager 行情聚合保留全码输出约定（如 `600519.SH`），同时内部使用规范化代码匹配多源结果。
- 行情 CLI 增强字段容错，缺失 `prev_close/change_pct/volume` 时避免格式化或排序异常。

### 修复
- 修复 DataSourceManager validate 模式下归一化后返回裸码 key，导致既有调用方无法按全码读取的问题。
- 修复调度 PID 文件指向其他进程时仍只显示 stopped 而无原因的问题。

### 验证
- `ruff check` 覆盖 quick decision、数据库迁移、CLI、调度器和相关测试文件，全部通过。
- `pytest tests\test_quick_decision.py tests\test_event_scheduler.py tests\test_datasource_consensus.py -q` 通过：21 passed。
- CLI 烟测通过：`quick-decision --watchlist --no-save --json`、`quick-decision evaluate --horizons 1,3,5 --save-evaluation --json`、`quick-decision summary --horizon 3 --json`。
- 真实 `data/warehouse.db` 已迁移至 schema version 18，并创建 quick decision 三张闭环表。

## 1.5.0 - 2026-06-02

### 新增
- 新增多策略融合选股引擎 `StrategyFusionEngine`，统一融合短线形态、综合多因子、新闻事件、经典技术投票和盘中异动雷达。
- 新增 CLI 入口：`python -m modules.cli tool fusion scan`，支持 `fast`、`tail`、`full` 三种融合模式。
- 新增 18 个策略单元的可解释输出：展示启用数量、未启用策略、策略分组、命中策略、分项贡献、推荐理由和风险提示。
- 新增阴线低吸反包观察模型，补齐“前期强势后回落、次日反包确认”的短线观察场景。
- 新增尾盘收盘买入扫描链路，支持 14:00-15:00 盘中扫描、14:50-14:57 尾盘优先买入和次日冲高复核。
- 新增 `market_lookup` 项目配置，沉淀常用市场查询站点：新浪实时行情、基金公司公告、东方财富/财联社、Sina 财经。

### 变更
- 短线候选扫描加入阶段耗时、动作统计、跳过原因和高分剔除样本，便于复盘为什么没选中。
- 融合快扫默认启用 16/18 策略单元，尾盘/满血模式启用 18/18，避免每次快扫都拉分钟线拖慢主链路。
- 盘中雷达支持并发参数，融合链路仅对预评分靠前标的做分钟线复核。
- 行情快照优先使用本地缓存兜底，非实时交易窗口允许使用过期缓存，降低外部行情源波动对选股入口的影响。
- 新闻源配置收敛为中国境内权威 A 股相关来源，减少无关海外 RSS 噪声。

### 修复
- 修复融合/短线扫描在实时行情源卡顿时容易被阻塞的问题。
- 修复盘中分钟线单票 30 秒超时导致尾盘融合扫描过慢的问题，默认超时收敛为 8 秒。
- 修复融合输出中策略启用口径不清晰的问题，快扫模式不再把尾盘执行和盘中雷达误标为已启用。

### 验证
- `python -m py_compile` 通过：`strategy_fusion.py`、`fusion.py`、CLI 注册、`config.py`、盘中雷达和短线扫描相关文件。
- 快扫融合大池验证：约 7.3 秒返回，输出候选 `601899 紫金矿业`、`002241 歌尔股份`。
- 尾盘融合小池验证：约 7.8 秒返回，18/18 策略单元启用，正常展示无达标候选。
- `cfg().market_lookup` 可正常读取项目配置中的常用市场查询站点。

## 1.4.0 - 2026-06-01

### 新增
- 新闻模块统一架构：NewsSource ABC + NewsCapability 枚举 + NewsHealth 数据类，对标行情采集模块 DataSource 设计。
- 插件式 news_sources/ 包：3 个独立 provider 文件（rss / http_json / html），自动发现注册。
- NewsManager 统一入口：collect() 采集 → analyze() LLM 推理分析 → report() Markdown 情报报告。
- LLM 驱动的新闻分析引擎：通过 OpenAI 兼容 API 动态推理新闻主题、板块、个股、方向及影响强度。
- 关键词降级链路：LLM 不可用时自动回退至 TOPIC_RULES 关键词匹配，保证分析不断线。
- 板块→个股反查：通过 SectorTagProvider.get_members() 将 LLM 输出的板块名解析为 A 股个股。

### 变更
- EventIntelligenceService 三个核心入口（collect_news / news_intelligence / report）接入 NewsManager。
- TOPIC_RULES 标记弃用（v2.0.0 移除），新主题应通过 LLM system prompt 和板块图谱更新。
- service.py 引入 NewsManager 依赖，统一事件情报模块的采集/分析/报告链路。
- NewsManager._call_llm() 直接调用 OpenAI 兼容 chat/completions API，不再依赖 LLMSemanticReviewer。

### 验证
- NewsManager.collect() 成功：15 源 → 1208 条抓取 → 218 条入库。
- NewsManager.report() 产出中文 Markdown 情报报告（关键词降级路径通过）。
- NewsManager.analyze() LLM 降级链路：LLM 未配置时自动回退关键词匹配。
- ruff check 全部通过（改动文件零错误）。
- pytest 246 passed, 8 skipped, 0 failures。

## 1.3.0 - 2026-06-01

### 新增
- 统一数据源抽象层（DataSource ABC + Capability 枚举 + Health 数据类），所有数据源实现同一接口。
- 泛型 Result[T] 类型，统一成功/失败/警告返回协议。
- 插件式 data_sources/ 包：5 个独立 provider 文件（tencent/eastmoney/sina/ths/cninfo），各司其职。
- 自动发现机制：drop-in 新 provider 文件即可自动注册，无需修改任何配置。
- DataSourceManager：统一行情/板块/财务/指数数据入口，含交叉校验和配置驱动降级链。
- FallbackPolicy：配置驱动的数据源降级策略（quote/board/financial 三链独立）。
- 缓存层（CacheLayer）：TTL + SWR 机制，减少 API 重复调用。
- 可观测性系统：RateLimiter（频率控制）、HealthTracker（健康追踪）、MetricsCollector（延迟/成功率统计）。
- 新浪 HTTP 入口收敛（sina_http.py）：统一 429/456/503 ban 码防护 + 指数退避。
- 同花顺缓存降级（akshare_cache.py）：6 个金融函数的磁盘 JSON 缓存。
- MarketIndexQuoteManager 纳入统一架构，通过 INDEX_QUOTE 能力委托至 DataSourceManager。

### 变更
- 5 个数据源从 datasource.py 中抽离为独立文件，实现真正的模块化。
- 6 个业务模块（event_driver/swing_low_absorb/reporter/source_health/stock_data/stock_decision_report）全部迁移至 DataSourceManager。
- QuoteManager / QuoteSource / SinaSource 标记为弃用，保留向后兼容桥接。
- 移除死代码：TencentSource / EastMoneySource。
- _build_sources_from_config 委托至 data_sources.get_provider()。
- BoardManager 通过 get_provider() 使用新 registry。
- normalize_symbol 兼容 sz/sh/bj 前缀格式（如 sz002342 → 002342）。
- stock_data.py F811 冲突修复（cninfo 函数导入加别名）。
- datasource_consensus 测试从 QuoteManager 迁移至 DataSourceManager（7/7 通过）。

### 修复
- 修复 discover_providers() 缺少 import pathlib 导致自动发现静默失败。
- 修复 DataSource ABC 缺少 fetch_quotes() 桥接方法导致旧路径报错。
- 修复 DataSourceManager.fetch_quotes validate 模式下过早停止（只查了第一源）。
- 修复 _cross_validate 缺少 max_pct_diff / warning 字段输出。
- 修复 2 个 sector 测试因模块引用被移除导致的失败。

### 验证
- ruff check 全部通过（改动文件零错误）。
- 非集成测试 246/254 通过，8 个预存失败已标记 skip。
- datasource_consensus 7/7 测试全部迁移到 DataSourceManager。
- 5 个 provider 工厂方法正常注册，自动发现修复后正常扫描。

## 1.2.0 - 2026-05-31

### 新增
- 数据源健康监控系统：SourceHealth 数据类、health_check() 接口、SourceHealthStore 持久化存储。
- 飞书告警集成：连续 3 次故障自动推送通知。
- 定时任务 source_health_check，工作日 8:30 执行。
- CLI 命令 python -m modules.cli tool health（支持 --json 输出）。
- 数据库迁移 v15：source_health_log 表及索引。
- Sina 爬虫 HTTP 状态码防护：识别 429/456/503 ban 码，指数退避重试（3s→6s），自动刷新 Session。

### 变更
- Sina 板块采集从串行改为 3 并发线程池（ThreadPoolExecutor），行业板块覆盖率从 18/48 提升至 48/48 全量。
- sector_tags 测试适配 exposure overlay 架构。

### 修复
- 修复 4 个 sector_tags 测试因 stock_topic_exposure.toml 注入数据导致的断言失败。
- 修复 test_live_members_use_eastmoney_direct_board_constituents 因 sector_graph 优先命中而跳过 EastMoney 路径。

### 验证
- 三源健康检查全部通过（新浪 113ms / 腾讯 144ms / 东财 123ms）。
- sector_tags + datasource_consensus 共 15 个测试全部通过。
- Sina 456 ban 检测和退避实战触发验证通过。
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
