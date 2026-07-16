# 实现状态与可信边界

更新时间：2026-07-17

## 已实现

- 深色情报终端式网页：研判队列、事件详情、信号雷达、信源治理、覆盖页、方法页、移动端、键盘行选择、减少动态效果；移动端详情使用带焦点约束和关闭后焦点恢复的模态层。
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
- PostgreSQL + pgvector 模型、指标时间分区、评分可复现记录、评分/当前状态/Outbox 原子提交、逐行事务化 Redis Streams 发布器、消费者组重领与 DLQ。`STREAM_OUTBOX_KINDS` 冻结 observation、metric snapshot、score、feedback、cluster edit、rescore 与 source deletion 七类消息，并由常规 publisher、恢复和裁剪共用；未登记 kind 记录失败且不进入 Stream。为避免删除 PEL 内未确认消息，V1 publisher 不执行 `MAXLEN`/时间自动裁剪；外部误删 pending ID 会由 `XAUTOCLAIM` 检出、写入 tombstone DLQ 并显式报错。独立保留工具默认 dry-run，使用与恢复相同的互斥租约拒绝活跃 publisher/consumer，基于 Redis 时钟、所有 consumer group 的 earliest-pending/last-delivered 水位和保留期计算安全边界。execute 把 dry-run ID 作为获批上限：当前安全水位后退即拒绝，只前移时仍按获批旧边界裁剪。磁盘 SQLite 账本逐条核对候选只属于冻结集合中的已发布 Outbox kind；执行期间 PostgreSQL `FOR SHARE` 事务锁住候选恢复行，Redis Lua 在单个原子操作内复核完整组状态并执行精确 `XTRIM MINID`，取消路径会等待原子命令完成后才释放 PG guard。安全删除后仍超容量只报告 `over_limit`，不越过水位强裁。XADD 成功但 PG 标记失败时保留待重试 Outbox，重复消息由稳定业务幂等键收敛；Redis Stream 全量丢失可用默认 dry-run、带批内心跳、原子检查点和正常 publisher/consumer 互斥租约的工具从已提交 Outbox 重建全部登记消息。断点续跑会以磁盘临时账本流式、精确对账 PostgreSQL 期望前缀和 Redis 去重后的完整 `outbox_id` 顺序，不能用“只保留最后检查点行”的残缺 Stream 蒙混完成；后台 PG 扫描有 240 秒 fail-closed 时限，取消路径等待线程释放 SQLite 后保留原取消异常并清理临时文件。常规 publisher 与恢复程序的 token 校验、续租和 `XADD` 已合并为单个 Redis Lua；裁剪 Lua 同样校验排他 token 后再复核组状态与 `XTRIM`，过期或被替换的 writer 不能继续写。Stream 与 state/lock/participants 键按同一 Redis Cluster hash slot 布局，协议版本 `stream-fence-2026-07-rc2.1` 写入检查点和运维报告。
- 每个 Observation revision 另存 collected/enqueued/completed/failed 时间、尝试次数和错误；合并处理多个待处理 revision 时逐条写入完成事实。Owner 只读接口报告“采集时间→评分完成”15 分钟 SLA、成熟未完成项、未来时间污染和未恢复失败，不再用连接器轮次耗时冒充端到端延迟。
- 评分事件到告警规则的真实闭环：规则匹配、至少三项证据、工作区/领域日预算、四小时冷却、新增证据或状态升级、签名 Webhook、持久化投递记录和 Web 内告警接口；预算、幂等键和冷却在数据库预留事务内原子检查。告警身份绑定稳定 `outbox_id`：in-app confirm 失败后即使事件更新/删除也恢复为 delivered；Webhook 若事件/规则快照已不可用或重试耗尽，则写入可在告警 API 查询的 `aborted + terminalReason` 审计终态后 ACK/DLQ，不宣称已送达，也不永久停在 reserved。
- 连接器失败降级、处理失败持久化待重试、成功后才推进的持久化 checkpoint、24H 滚动观测与轮次耗时统计、来源候选晋级、5% 日增长上限、运行时月度成本台账与逐连接器预算降频/停机（未配置合约单价时 fail-closed）。连接器 Registry 明示发现、增量、刷新、回补、配额、成本、字段权利、删除和 72H 验收状态。
- 每条独立内容在入库事务中自动登记候选信源；同信源同指纹复制内容不增加有效观测，但保留发现痕迹和账号/实体关联。`source-score-2026-07-rc2.2` 冻结最低样本、历史天数、容量、Asia/Shanghai 严格 floor 日增长、人工种子规则和权重；质量特征未校准时 API 与信源中心显示 N/A 而非 0。SourceScore 排行和自动晋级保持关闭，直到真实历史结果集与反馈回路校准获批；未来晋级使用离线治理 Owner、事务锁、确定性 tie-break 和 append-only 策略摘要事实。信源 API/网页使用服务端查询与分页，可覆盖 200 active + 500 candidate 以及 2,000 系统容量。
- 来源删除覆盖逐 item 原始对象、全部指标修订、成员关系、派生分数、标题和缓存；保留成员会在同一事务提高 processing revision，因此即使没有新采集也会重新评分，无保留成员的事件会被删除。
- 每条观测记录连接器 rightsPolicy；到期任务清除原始对象引用和指标修订引用，保留最小事实，并通过逐对象确认、租约和指数退避的删除队列物理删除本地/R2 对象；S3/R2 部分失败不会被确认，毒对象不阻塞其他对象，共享的未到期引用不会被误删。
- 全局行为 N/A 变更在生产认证开启时只允许 `RADAR_SYSTEM_WORKSPACE_ID` 的 Owner 执行；普通工作区反馈不会修改全局评分。后续首次出现类型有效行为事实会在同一评分周期覆盖旧 N/A。
- 人工合并/拆分采用可审计异步命令；生产认证开启时只有离线治理工作区的 Analyst/Owner 能修改全局共享拓扑，普通租户工作区返回 403。Worker 以乐观锁创建新 Event，旧 Event 标记 `supersededBy`，迁移成员并重算，保留历史评分/告警；任意工作区的关注都解析到当前有效后继；撤销提升父事件版本，阻止撤销前排队的旧命令继续执行；谱系可查且支持撤销。
- rc2 产品 KPI 的冻结策略为 `product-metrics-2026-07-rc2.9`，因验收监控与迁移证明推进至 rc2.6 于 2026-07-17 05:50（Asia/Shanghai）重新冻结；此前样本全部排除，证据窗口从该时点重启。系统在事件进入每个可研判 epoch 时原子记录 QueueEligibility，反馈必须引用当前有效 eligibility key，旧 epoch 不能污染首次分诊；强告警接受率使用已送达告警作分母和持久化反馈作判断源；错误强告警要求两个不同 Actor 复核。系统故障导致的重复告警只能由 Owner 基于两条真实投递建立不可变 MetricIncident，排除事实绑定 incident digest，不接受自由文本归因。
- 首次分诊按 Asia/Shanghai 预登记值班窗累计；前端每 5 秒上报带 attempt/segment/sequence 的认证心跳，后端忽略客户端时长汇总，只累计 `active` 状态并跨详情关闭/重开合并全部 segment。完成研判的有效 telemetry 覆盖率必须达到 95%，否则不输出达标结论；并行的服务端观测心跳墙钟不受客户端 state 缩短，只作异常护栏，不能证明前台注意力。该有效时长口径明确不防止持证 Analyst 伪报状态。`/api/v1/metrics/beta` 对四项正式指标逐项报告最低样本和 `passesTarget`，另报告墙钟护栏；样本不足时固定返回 `insufficient/null`。
- 正式人工评估采用预登记 schema v2：冻结完整本地日历、排名规则、阈值版本、bootstrap seed/迭代次数/抽样单元；每个采样时点由独立 ledger key 签名完整排序账本和 append-only 阈值跨越事实，每天 09:00±5 分钟的 Top-5 快照必须逐项等于同一账本前五名，并由 scheduler 签名后提交 snapshot commitment。Precision@5 同时要求点估计和 95% CI 下界均不低于 0.70；提前量使用版本化 crossing 事实和由 baseline collector 签名的首次发现日志。scheduler、reviewer、baseline、ledger 四类 Ed25519 密钥不得复用。
- 评估工具输出 Precision@K、宏 F1、错误告警/日、提前量、eventType 分组、Pairwise、B-cubed 和 bootstrap 区间；双标注 Cohen's kappa 有独立实现。
- 153 项默认 Python 自动化用例、11 项显式基础设施用例、Ruff、前端 Lint、生产构建和 2 项 SSR/静态产品契约用例。
- 五个免密公共元数据连接器完成显式真实 smoke；该过程发现并修复 OpenAlex 空作者 ID 整批失败与异常未来发布日期污染时间线的问题。命令与运行证据独立保存，不进入确定性 CI，也不替代 72 小时 soak。
- owner-only Sites 录制数据候选 v1 已部署成功；源码 SHA、归档哈希、访问策略与平台桌面截图均已归档。该版本明确显示 `RECORDED DEMO`，尚未连接生产 FastAPI 与身份联邦。
- `tools/acceptance_monitor.py` 可由外部调度器每 15 分钟追加哈希串联、Ed25519 签名且可检出篡改的 JSONL 样本，并分别生成 canary、72H soak 和 7 天 shadow 报告。正式模式把 policy、monitor、schema 与 keyring digest 绑定到每个样本，校验只增不减的事实账本、数据权利连续性、非零观测、至少四个连接器家族、讨论与行为覆盖、连接器健康/重复率及端到端 SLA；还要求生产 PostgreSQL、受限应用角色、强制 RLS、审计触发器、只读迁移标记、认证和稳定 instance ID 的运行时证明。内存仓库和空 keyring 固定 NO-GO，单样本 canary 固定为 `acceptanceEligible=false`。

## 以录制数据运行

默认 `DEMO_MODE=true`，API 使用可重复的五类代表事件。网页优先读取 API；API 不可用时使用相同契约的本地录制样本。这使 UI、合同和评分开发不依赖付费凭证，但样本数不能代表模型效果。

## 尚未宣称完成

- 真实 120–200 活跃源和 500 候选源的运营清单，以及 SourceScore 的经验贝叶斯收缩、探索配额和反馈回路真实校准；当前只开放候选登记与治理视图，不开放排名。
- BGE-M3 生产推理服务、持久化分层基线与分析师反馈训练闭环；仓库不捆绑 2GB+ 模型权重，当前运行时基线缓存也不能替代 28 天时间/实体隔离校准。
- X、Bluesky、Bilibili 的生产连接；它们受授权、配额或实时流部署约束。
- 60/240 双人标注集、时间隔离校准、F1/Precision@K 的真实数值。
- rc2 产品 KPI 的真实最低样本：30 条强告警、5 个有告警工作日、50 个可研判事件、50 次有效研判；Precision@5 需要完整预登记日历、scheduler 签名快照、双人独立签名复核，且点估计和 95% CI 下界均 ≥ 0.70；发现提前量需要独立 baseline collector 签名的首次发现日志。合成用例只验证计算器，不进入这些分母。
- 正式验收密钥登记与法律授权：仓库内冻结 keyring 当前故意为空，连接器 rights status 当前为 pending/blocked，因此正式 72H/7D 报告必须 fail closed；只有密钥保管人与数据权利负责人完成外部登记后才能开始正式窗口。
- 72 小时采集 soak、7 天影子运行、2,000 信源真实容量与 500 万观测数据库压测。
- Sites 身份到独立 FastAPI 的生产级联邦验证；当前 Sites 仅为录制数据私有候选。生产认证开启时，浏览器原生 EventSource 不能携带当前 API Key，必须通过同源身份代理或改用带凭证的流客户端。
- 目标生产环境的 PostgreSQL/RLS/Redis/R2 集成、跨节点中断、网络分区、进程硬终止和大规模重放演练。本地 Docker 已实际应用 `001_init_rc2.6`，并通过 RLS/trigger/只读迁移标记、并发信源去重、并发 Outbox publisher、XADD/PG 标记失败窗口、生产消费者 ACK/DLQ、空 Stream 重建、Redis stale-writer 拒绝、MinIO put/read/delete 和协调重启持久性验证；这不替代目标环境验收，也不把 at-least-once 上调为跨系统 exactly-once。
- PostgreSQL PITR、1 小时 RPO、4 小时 RTO 和备份过期删除演练。
- 生产 Redis 容量与安全保留验收：consumer-group 安全水位、Outbox 可恢复性校验、原子 stale-writer fencing、同槽键约束和拒绝强裁已在本地真实 PG/Redis 通过；进入目标环境前仍须在真实 Redis Cluster 验证槽位、故障转移、异常硬终止、网络分区、集群外管理操作、目标流量容量告警和大规模裁剪耗时。

这些项目必须保留为发布闸门，不应以演示数据或单机单测“视为通过”。

## 本轮可复现验证

```text
Python deterministic/default: 153 passed, 11 infrastructure tests skipped
Python with explicit local infrastructure: 164 passed, 7 upstream deprecation warnings
Python Ruff: passed
Web: ESLint passed
Web: Vinext production build passed
Web: 2 SSR/product-contract tests passed
Web production dependencies: 0 known vulnerabilities
Web development toolchain: 4 moderate findings remain in Drizzle CLI's legacy esbuild chain; npm only offers a breaking downgrade, so no forced remediation was applied
Sites: owner-only recorded-data deployment v1 succeeded; platform desktop screenshot inspected
Browser interaction: not run (enterprise policy blocks localhost and chatgpt.site automation)
Python bytecode compile: passed
Python dependency check: passed
Docker Compose configuration parse: passed
Docker runtime integration: passed locally (PostgreSQL/Redis/MinIO; see evidence record)
```

另有非生产、CPU/内存内合成检查：10,000 信源、5,000,000 计数型观测、2,000 事件的生成与评分循环为 0.94 秒；FastAPI 内存 fixture 120 次请求 P95 为 0.909ms。它们只验证算法循环没有明显数量级错误，不包含 PostgreSQL I/O、网络、向量推理或缓存，因此不用于宣称生产容量与 P95 达标。

本地容器的命令、版本、断言、重启持久性结果和未覆盖范围见 [基础设施集成验证记录](evidence/INFRASTRUCTURE_INTEGRATION_2026-07-17.md)。
Outbox 故障窗口、生产消费者 ACK/DLQ 和 Redis 丢失重建证据见 [Outbox/Redis 恢复验证](evidence/OUTBOX_REDIS_RECOVERY_2026-07-17.md)。
Redis Stream 安全水位、Outbox 可恢复性校验和拒绝强裁证据见 [Redis Stream 安全保留验证](evidence/REDIS_STREAM_RETENTION_2026-07-17.md)。
