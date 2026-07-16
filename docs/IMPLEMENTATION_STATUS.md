# 实现状态与可信边界

更新时间：2026-07-16

## 已实现

- 深色情报终端式网页：研判队列、事件详情、信号雷达、覆盖页、方法页、移动端、键盘行选择、减少动态效果；移动端详情使用带焦点约束和关闭后焦点恢复的模态层。
- 事件详情：Event/Narrative、生命周期、结构标签、六维指标、24H/7D 结果占位、反向信号、正常时滞、方法边界、状态时间线、证据链和谱系；支持关注备注、接受/拒绝/需观察、合并/拆分、操作状态与撤销。拆分成员来自完整 Observation ID 接口，不复用截断证据卡 ID。尚无真实结果标签时明确显示 N/A。
- 待研判队列为默认首页，具有全文搜索及生命周期、事件类型、证据强度筛选；二维雷达只绘制讨论与行为均可用的事件，并提供数据表替代。
- FastAPI 核心接口：review queue、event、assessment、timeline、evidence、lineage、radar、sources、coverage、watchlist 列表/幂等新增/删除、alert rules/feed、feedback、产品研判指标、SSE，以及兼容旧路径。
- Owner / Analyst / Viewer API 权限模型，本地预览可显式关闭认证。
- 事件类型感知行为指标、MAD/Logistic 基线映射、证据掩码、低/中/高证据强度、不确定性、生命周期与多标签判定；冻结 `features-2026-07-rc2.1` 的角色、固定适用权重分母、平台家族总上限和最小样本进入聚合。Star/Fork 等 secondary intent 可在类型 Profile 上限内贡献 Behavior，但不能确认采用；安全响应使用独立标签。
- 跨平台确认不复用信号类别计数：必须至少两个实际平台家族、两个所有权去重实体且证据达到 medium；平台别名归为开发生态、模型 Hub、讨论、研究、视频和官方/媒体等家族。
- 评分与时间线按 15 分钟桶合并，当前桶不会反向污染自身历史基线；用于评分、覆盖惩罚、N/A 守卫和告警增量的是完整成员掩码，30 条证据上限只影响详情展示。
- ContentObservation 与 MetricSnapshot 分离；GitHub、Hugging Face、YouTube 等指标型连接器对同一外部对象保持稳定内容 ID，仅以采集时间追加指标快照。指标历史按采集时间重建后计算增量，每个快照记录自己的原始证据修订引用。
- RSS、Hacker News、GitHub、Hugging Face、arXiv、OpenAlex、YouTube 连接器适配器；限流重试、URL 规范化、指纹去重、DNS/重定向 SSRF 拦截和真实原始证据归档。跨来源搜索结果按 item 分片归档，避免因共享 batch 对象阻断选择性物理删除。
- 可选 BGE-M3 推理服务已经接入聚类候选链路，事件向量在进程内缓存；来源删除、标题重建和人工簇编辑可通过事件 cache tag 使缓存失效，标题哈希不一致时也拒绝复用旧向量。服务不可用时回退到 URL、实体、时间与词项证据。
- 经人工登记的 Account → Person/Organization 所有权解析；讨论度、来源多样性和协同风险均按所有权实体去重，未知所有权不做名字猜测。
- PostgreSQL + pgvector 模型、指标时间分区、评分可复现记录、评分/当前状态/Outbox 原子提交、Redis Streams 发布器、7 天/长度双裁剪、消费者组重领与 DLQ。
- 评分事件到告警规则的真实闭环：规则匹配、至少三项证据、工作区/领域日预算、四小时冷却、新增证据或状态升级、签名 Webhook、持久化投递记录和 Web 内告警接口；预算、幂等键和冷却在数据库预留事务内原子检查，Webhook 失败会释放预留。
- 连接器失败降级、处理失败持久化待重试、成功后才推进的持久化 checkpoint、24H 滚动观测与轮次耗时统计、来源候选晋级、5% 日增长上限、运行时月度成本台账与逐连接器预算降频/停机（未配置合约单价时 fail-closed）。连接器 Registry 明示发现、增量、刷新、回补、配额、成本、字段权利、删除和 72H 验收状态。
- 来源删除覆盖逐 item 原始对象、全部指标修订、成员关系、派生分数、标题和缓存；保留成员会在同一事务提高 processing revision，因此即使没有新采集也会重新评分，无保留成员的事件会被删除。
- 每条观测记录连接器 rightsPolicy；到期任务清除原始对象引用和指标修订引用，保留最小事实，并通过逐对象确认、租约和指数退避的删除队列物理删除本地/R2 对象；S3/R2 部分失败不会被确认，毒对象不阻塞其他对象，共享的未到期引用不会被误删。
- 全局行为 N/A 变更在生产认证开启时只允许 `RADAR_SYSTEM_WORKSPACE_ID` 的 Owner 执行；普通工作区反馈不会修改全局评分。后续首次出现类型有效行为事实会在同一评分周期覆盖旧 N/A。
- 人工合并/拆分采用可审计异步命令；生产认证开启时只有离线治理工作区的 Analyst/Owner 能修改全局共享拓扑，普通租户工作区返回 403。Worker 以乐观锁创建新 Event，旧 Event 标记 `supersededBy`，迁移成员并重算，保留历史评分/告警；任意工作区的关注都解析到当前有效后继；撤销提升父事件版本，阻止撤销前排队的旧命令继续执行；谱系可查且支持撤销。
- 研判漏斗埋点按工作区保存并有幂等键；当前接口只报告“详情打开→提交研判”的探索性代理。它不是强告警人工接受率，也不是从进入可研判队列开始、带值班时段排除的首次分诊 SLA，不能用于 rc2 Beta 判定。
- 评估工具输出 Precision@K、宏 F1、错误告警/日、提前量、eventType 分组、Pairwise、B-cubed 和 bootstrap 区间；双标注 Cohen's kappa 有独立实现。
- 103 项 Python 自动化用例、Ruff、前端 Lint、生产构建和 2 项 SSR/静态产品契约用例。
- 五个免密公共元数据连接器完成显式真实 smoke；该过程发现并修复 OpenAlex 空作者 ID 整批失败与异常未来发布日期污染时间线的问题。命令与运行证据独立保存，不进入确定性 CI，也不替代 72 小时 soak。

## 以录制数据运行

默认 `DEMO_MODE=true`，API 使用可重复的五类代表事件。网页优先读取 API；API 不可用时使用相同契约的本地录制样本。这使 UI、合同和评分开发不依赖付费凭证，但样本数不能代表模型效果。

## 尚未宣称完成

- 真实 120–200 活跃源和 500 候选源的运营清单。
- BGE-M3 生产推理服务、持久化分层基线与分析师反馈训练闭环；仓库不捆绑 2GB+ 模型权重，当前运行时基线缓存也不能替代 28 天时间/实体隔离校准。
- X、Bluesky、Bilibili 的生产连接；它们受授权、配额或实时流部署约束。
- 60/240 双人标注集、时间隔离校准、F1/Precision@K 的真实数值。
- 72 小时采集 soak、7 天影子运行、2,000 信源真实容量与 500 万观测数据库压测。
- Sites 身份到独立 FastAPI 的生产级联邦验证；生产认证开启时，浏览器原生 EventSource 不能携带当前 API Key，必须通过同源身份代理或改用带凭证的流客户端。
- PostgreSQL 迁移、RLS、Redis、MinIO/R2 的真实容器集成测试。本机 `docker compose config --quiet` 已通过，但 Docker Desktop 引擎未运行，不能声称数据库迁移已实际应用。
- PostgreSQL PITR、1 小时 RPO、4 小时 RTO 和备份过期删除演练。

这些项目必须保留为发布闸门，不应以演示数据或单机单测“视为通过”。

## 本轮可复现验证

```text
Python: 103 passed
Python Ruff: passed
Web: ESLint passed
Web: Vinext production build passed
Web: 2 SSR/product-contract tests passed
Python bytecode compile: passed
Python dependency check: passed
Docker Compose configuration parse: passed
Docker runtime integration: not run (local Docker engine unavailable)
```

另有非生产、CPU/内存内合成检查：10,000 信源、5,000,000 计数型观测、2,000 事件的生成与评分循环为 0.94 秒；FastAPI 内存 fixture 120 次请求 P95 为 0.909ms。它们只验证算法循环没有明显数量级错误，不包含 PostgreSQL I/O、网络、向量推理或缓存，因此不用于宣称生产容量与 P95 达标。
