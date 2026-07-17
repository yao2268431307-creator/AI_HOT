# AI 热点雷达 V1 实现状态与可信边界

更新时间：2026-07-17

候选版本：rc3（本地代码候选）

评分版本：`score-0.7.0`

阈值版本：`thresholds-2026-07-rc3`

数据库迁移：`001_init_rc3.0`

产品指标策略：`product-metrics-2026-07-rc3.2`

## 当前结论

仓库已经形成可运行、可解释、可回放的私有技术 Beta 代码候选。本地确定性测试、真实 PostgreSQL/Redis/MinIO 集成、生产构建和真实 Chromium 可访问性回归均通过；独立终审在当前仓库代码候选范围确认 `P0=0、P1=0、P2=0`。

这不等于正式 Beta 已发布。数据权利批准、目标环境不可变镜像、PITR/恢复演练、10,000 信源与 500 万观测容量证明、72 小时采集 soak、7 天影子运行和真实人工标注指标仍是外部发布闸门。任何演示数据、本地容器或内存压测都不得替代这些证据。

## 已实现

### 产品与网页

- 默认首页为待研判队列，支持搜索、时间窗、生命周期、事件类型、证据强度、平台和语言筛选。
- 二维雷达只呈现讨论度和行为趋势均适用的事件；保留最近六小时轨迹和等价数据表。
- 详情页提供两条核心曲线、差值变化、生命周期与结构标签、驱动因素、反向信号、原始证据、平台迁移、谱系、关注、忽略、纠错、合并和拆分。
- 信源中心明确展示候选/活跃生命周期、有效观测、发现原因与权利状态；SourceScore 未校准时显示 N/A，自动晋级保持 fail-closed。
- 覆盖页展示连接器健康、延迟、数据权利、总预算、逐连接器预算和逐信号家族预算。
- 桌面和移动端支持键盘导航、焦点约束与恢复、Escape 关闭、减少动态效果、图表数据表和 WCAG AA 自动检查。

### 数据与处理

- 已实现 RSS、Hacker News、GitHub、Hugging Face、arXiv、OpenAlex、YouTube，以及默认关闭、仅用于候选发现的 Bluesky 适配器。
- 统一 Observation/MetricSnapshot 契约，包含 `availableAt` 与 `availabilityBasis`，区分提供方时间和首次检测回退。
- URL 规范化、内容指纹、所有权去重、SSRF/DNS/重定向围栏、限流退避、持久 checkpoint、迟到处理和 R2/MinIO 原始证据归档已实现。
- 权利策略 `rights-2026-07-rc3.0` 对原文、摘要长度、指标类别、提供方核验和保留期逐字段执行；未批准连接器在网络与处理阶段均 fail-closed。
- GitHub、Hugging Face、OpenAlex 已移除未进入冻结 Feature Registry 的不透明或重复指标。
- 可选 BGE-M3 跨语言向量路径已接入；PostgreSQL 持久化 1024 维事件向量、模型版本和标题哈希，通过 HNSW Top-50 与最多 25 个硬匹配/近期候选收窄候选，进程内缓存保持有界，并在删除、标题变更或人工簇操作后失效重算。

### 评分、状态与解释

- 事件类型专用行为模型、MAD/Logistic 基线映射、讨论速度/加速度、跨平台传播、所有权集中、协同发布风险和覆盖不确定性已实现。
- 生命周期主状态与传播结构标签分离；数据不足不输出强结论，连接器故障不会被解释为热度下降。
- 强状态具备连续周期确认、滞回和按时间驱动的降温/休眠；评分队列按新增证据、生命周期、确认进度、关注、覆盖下降和证据强度排优先级。
- 每次评分保存 revision、输入摘要、观察 ID、基线/特征/证据/标签/聚类/身份版本与摘要、模型和阈值版本；同周期输入变化追加 revision，相同输入幂等。
- 评分回放接口使用持久化的完整输入载荷，不从当前可变状态猜测历史结论。
- LLM 不参与数值评分；摘要只能基于已入库证据生成。

### 存储、队列与运行

- PostgreSQL + pgvector 是事实提交边界；Redis Streams 用于发布与消费；R2/MinIO 保存受策略约束的原始证据。
- Transactional Outbox、稳定业务幂等键、消费者组重领、DLQ、Stream 丢失重建、安全水位裁剪和 stale-writer fencing 已实现并在本地真实基础设施验证。
- 同周期评分允许追加 revision，唯一约束基于 score input digest；真实 PostgreSQL 回归覆盖 revision 1/2、回放、`availableAt` 和 1024 维向量。
- 雷达接口限制单页默认 200、最大 500；优先级上下文由批量 PostgreSQL 查询生成，避免按事件 N+1 查询。
- 采集器在请求前按最坏重试与重定向成本执行数据库原子预留；总月度、逐连接器、逐信号家族三层预算在同一锁域校验和记账。进程崩溃留下的过期预留进入继续占额的显式对账状态，阻断 readiness，只有管理员可依据提供方账单释放或确认费用。
- `/metrics` 为 Owner-only Prometheus 文本；覆盖 HTTP 状态与 P95、连接器运行/失败/延迟/覆盖、处理与 Outbox 积压、运行组件心跳、预算利用率和待对账预留数。
- `/health/ready` 在生产模式下对数据库权限、RLS/trigger/marker、隔离删除角色、JWT、采集 Worker 每轮 Redis/R2 读写删除实探、Alert 每轮 Redis/R2 删除权限实探、五个运行组件心跳、DR 证明、预算对账和 API/Web 镜像摘要 fail-closed；公开 `/health` 与 readiness 都是最小披露，完整诊断仅限 Owner 的 `/api/v1/operations/runtime-health`。
- 生产 Compose 只接受 API/Web 的 `repository@sha256` 不可变镜像，不含本地 `build` 回退，并配置只读文件系统、loopback 端口、资源限制和 readiness healthcheck。Dockerfile 构建基座可由发布流水线参数注入；目标镜像的构建、签名和登记仍是外部发布闸门。

### 身份、安全与治理

- Owner、Analyst、Viewer 权限已实现；生产模式只接受短时 EdDSA 或 RS256 JWT，并验证 issuer、audience、iat/expiry、jti 与签名密钥版本；令牌角色声明被忽略，最终角色来自服务端工作区成员表，撤销 jti 会立即拒绝访问。
- 生产就绪要求精确的 `{api, web}` SHA-256 发布镜像清单；缺失或格式不合法时拒绝 ready。
- 全局聚类编辑和全局行为 N/A 只允许离线治理工作区授权角色操作，普通工作区不能改写共享结论。
- 原始证据清除、来源删除、指标修订删除、评分失效与重算具备可审计路径；评分历史清除只能由隔离的 `radar_deletion_worker` 按数据库事实推导范围执行，普通应用角色不能越权调用或伪造审计。
- 运维、事故响应、回滚、预算和数据权利边界见 [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md) 与 [SECURITY_AND_DATA_RIGHTS.md](SECURITY_AND_DATA_RIGHTS.md)。

## 本轮可复现验证

| 验证 | 结果 |
|---|---:|
| 默认 Python 套件 | `213 passed, 20 skipped in 23.42s` |
| 显式本地 PostgreSQL/Redis/MinIO 套件 | `231 passed, 2 skipped, 8 warnings in 27.27s` |
| Ruff | 通过 |
| Python compileall | 通过 |
| pip check | 无损坏依赖 |
| Web ESLint | 通过 |
| Vinext 生产构建 | 通过 |
| Playwright + Axe（真实 Chromium） | `2 passed` |
| Web 生产依赖审计 | `0 vulnerabilities` |
| Production Compose 配置解析 | 通过 |
| 全新 PostgreSQL 容器角色 + 迁移启动 | 通过 |
| `git diff --check` | 通过 |

8 条 warning 均来自 botocore 对 `datetime.utcnow()` 的上游弃用提示，不是测试失败。构建仍有 Vinext 路由分类提示和 Node `punycode` 弃用提示，未影响构建结果。

完整的本轮证据边界见 [RC3 本地验证记录](evidence/RC3_LOCAL_VERIFICATION_2026-07-17.md)。旧 evidence 文件保持为对应 rc2 时点的历史记录，不应用来证明 rc3 正式验收。

独立终审结论与代码完成度/正式发布资格的拆分口径见 [完成度审计](COMPLETION_AUDIT.md)。

## 尚未完成的发布闸门

1. 数据权利负责人批准正式连接器；X、Bilibili 等仍需合法凭证，Bluesky 仍为 discovery-only。
2. 在目标部署环境构建并登记 API/Web 不可变镜像摘要，配置真实 JWT、Redis、R2 和密钥保管。
3. PostgreSQL PITR、RPO ≤ 1 小时、RTO ≤ 4 小时、跨节点恢复、网络分区和进程硬终止演练。
4. 10,000 信源、500 万历史观测、2,000 活跃事件的真实数据库/网络/向量容量验证；单轮 ≤ 5 分钟、雷达缓存命中 P95 < 500ms。
5. 至少四个独立信号家族的 72 小时连续采集；95% 正常数据在 15 分钟内完成评分，重复率 < 5%。
6. 60/240 条双人时间点标注、三类核心状态宏 F1 ≥ 0.70，以及聚类抽检和置信区间证明。
7. 7 天真实影子运行、产品 KPI 最低样本、双人复核、签名 Top-5 账本和独立 baseline 首次发现日志。
8. 目标组织身份联邦、密钥轮换、生产告警接收方和真实分析师/辅助技术验收。

以上任一项未完成时，正式 Beta 均应保持 NO-GO。
