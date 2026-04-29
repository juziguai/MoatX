# MoatX 文档索引

> 当前目录只保留正在使用或需要继续演进的文档。历史计划、评审和临时讨论已移动到 `docs/archive/`。

## 当前有效文档

| 文档 | 用途 |
|---|---|
| `PROJECT_ARCHITECTURE.md` | 项目总体架构、流程图、模块清单、数据流和工程边界 |
| `PROJECT_STATUS.md` | 项目当前状态、工程化进度、运行入口总览 |
| `BETA_PLAN.md` | Beta 阶段路线图与生产化验证计划 |
| `EVENT_INTELLIGENCE_ALGORITHM.md` | 宏观事件情报驱动选股算法设计 |
| `EVENT_INTELLIGENCE_IMPL_PLAN.md` | 宏观事件情报模块实施计划 |
| `SCORING_ALGORITHM.md` | 当前股票评分/选股因子算法参考 |
| `CRAWLER_USAGE.md` | 爬虫/API 探测工具使用说明 |
| `known_errors.md` | 常见错误模式与排查索引 |

## 归档目录

| 目录 | 内容 |
|---|---|
| `archive/plans/` | 已完成或阶段性过期的实施计划 |
| `archive/reviews/` | 历史评审、批判意见、模型验收报告 |
| `archive/temp/` | 临时稿、未命名讨论稿 |

## 核心算法阅读顺序

1. 先读 `PROJECT_ARCHITECTURE.md`：理解系统分层、数据流、流程图和模块边界。
2. 再读 `EVENT_INTELLIGENCE_ALGORITHM.md`：理解事件情报、产业传导和机会发现。
3. 再读 `EVENT_INTELLIGENCE_IMPL_PLAN.md`：理解 P0/P1/P2 实施拆解。
4. 再读 `SCORING_ALGORITHM.md`：理解个股质量、技术时机、情绪和风险过滤。
5. 最后读 `BETA_PLAN.md`：理解工程化落地路径。
