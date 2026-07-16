# AI 热点雷达 V1 · rc2 完成审计

审计基线：`AI热点雷达_V1_需求规格与执行计划_v1.0-rc2.md`  
审计日期：2026-07-17
判定原则：自动化用例只能证明代码行为；录制数据、内存压测和页面数量不能替代真实来源、人工标注、生产数据库、授权或连续运行证据。

## 状态定义

- `已证实`：存在实现、可复跑用例以及本轮通过记录。
- `待环境验证`：生产形态代码已存在，但本机缺少 PostgreSQL/Redis/R2、真实部署或浏览器辅助技术，不能给出集成结论。
- `外部闸门`：必须由数据权利人、组织管理员、分析师样本或连续时间窗口提供，不能用模拟数据替代。
- `部分完成`：本地契约或入口已完成，真实内容/模型或运营闭环尚未形成。

## 需求—证据矩阵

| rc2 要求 | 状态 | 当前证据 | Beta 前剩余动作 |
|---|---|---|---|
| 5 类 Event、生命周期主状态、多结构标签 | 已证实 | `contracts.py`、`scoring.py`；评分和生命周期回归用例 | 用冻结测试集报告真实分类型结果 |
| 事件类型专用 Behavior 和正常时滞 | 已证实 | 冻结 `feature_registry.json`；类型专用角色、窗口、MAD 与缺失掩码用例 | 用 28 天真实历史基线校准参数 |
| 播放、评论、搜索、Star 不单独确认采用 | 已证实 | `features-2026-07-rc2.1` 使用固定适用分母、平台家族总上限与最小样本；secondary intent 可受限贡献 Behavior，但 adoption 只看 primary；“高 Star + 平安装”回归用例 | 真实数据抽检 false adoption |
| 跨平台确认使用平台家族与所有权实体 | 已证实（代码） | `ScoreInput` 分离 signal/platform family；至少 2 平台家族、2 所有权实体和 medium 证据；一平台四信号类别反例用例 | 真实平台别名表与 200 条传播抽检 |
| 数据不足不输出强结论，缺失不当作 0 | 已证实 | Coverage、EvidenceMask、N/A 和连接器故障冻结用例；网页显示 N/A | 真实连接器停机演练 |
| 15 分钟采集、聚类、评分闭环 | 已证实（代码与计量）/外部时间闸门 | Worker 默认 900 秒；逐 revision 的 collected/enqueued/completed/failed 历史；Owner SLA 接口与合并 revision 回归用例 | 用真实来源运行 72H；要求 95% 在 15 分钟内完成评分且无未恢复失败 |
| 至少 4 个稳定信号家族，含讨论和行为 | 外部闸门 | 已有 RSS/HN/GitHub/HF/arXiv/OpenAlex/YouTube 适配器和覆盖页；五个免密公共元数据源完成一次真实 smoke | 用授权来源稳定运行 7 天并提交覆盖报告 |
| 120–200 活跃源、500 候选、2,000 真实容量 | 部分完成/外部闸门 | 采集事务自动登记候选；同源同指纹使用 PG 事务锁，重复内容不抬高有效观测。冻结候选治理策略采用 Asia/Shanghai 严格 floor 5% 日上限、人工种子、确定性 tie-break 与 append-only 策略摘要事实；服务端分页/查询和信源中心可覆盖完整容量。质量特征缺失显示 N/A，排行和自动晋级 fail-closed | 提供真实清单、凭证和目标数据库；完成经验贝叶斯收缩、探索配额与反馈回路校准后才可开放排名/自动晋级，并在目标 PG 上跑真实并发和容量验收 |
| 10,000 信源合成容量 | 已证实（非生产） | `tools/synthetic_load.py`：10,000 源、500 万计数事实、2,000 事件约 0.94s | 不得据此宣称 PostgreSQL P95 或生产容量 |
| 中英文事件归并 | 部分完成 | BGE-M3 接口、共同 URL/实体/时间回退和跨语言 Mock 用例 | 部署 BGE-M3，完成 200 条真实中英抽检 |
| Narrative 与 Event 分离 | 部分完成 | Event 支持可选 Narrative 关系；详情明确 Event 独立评分 | 建立真实 Narrative 归并和人工修订运营流程 |
| 待研判队列、详情、覆盖、关注、反馈 | 已证实（静态/接口）/待浏览器验证 | Vinext 构建、SSR 契约；真实 Observation 成员拆分、谱系/操作状态/撤销、总预算、关注 CRUD/筛选、详情反证/备注/纠错 | 浏览器 E2E 与真实分析师 5 分钟研判测试 |
| 24H/7D 结果与时点判断分离 | 部分完成 | 详情未有结果时显示 N/A；评估 schema 分开 outcome24h/outcome7d | 真实结果标注到达后展示结果，不得回写时点标签 |
| 产品定位随覆盖降级 | 已证实 | “开发者与研究生态信号 Beta”“中文讨论覆盖不足”及覆盖页边界 | 获得合法中文讨论源后才可调整文案 |
| 通用签名 Webhook、预算、冷却、幂等 | 已证实 | HMAC、防重放、SSRF、原子预算预留、失败释放和告警闭环用例 | 真实 Webhook 目标连通与值班演练 |
| 合并/拆分、谱系、撤销、乐观锁、关注继承 | 已证实（内存）/待环境验证（PG） | 只有离线治理工作区可改全局拓扑；新 Event、superseded、成员迁移、历史分数不改写；关注解析有效后继；撤销提升父版本；PG 失败回写设置 RLS workspace 并消费 outbox | Docker/目标 PostgreSQL 上跑跨工作区、并发、失败消费和回滚集成测试 |
| Observation 与 MetricSnapshot 分离、可重放 | 已证实 | 稳定内容 ID、追加指标事实、输入 digest、评分/阈值/窗口版本和迟到修订用例 | 30 天后用生产快照做一次真实重放对账 |
| 原始证据、删除传播和最小事实保留 | 已证实（本地对象/适配器单测）/待环境验证（R2） | Observation 强制 `r2://bucket/key`；来源删除、共享引用、缓存/embedding 失效；R2 部分删除错误检查；逐对象确认、租约和指数退避 | 真实 R2、备份和搜索索引上做部分失败/恢复演练并留审计记录 |
| 连接器发现/增量/刷新/回补/成本/权利卡 | 已证实（配置） | `connector_registry.json`、持久 checkpoint、运行成本 fail-closed | 数据负责人逐字段签字，72H 验收状态由 pending 改为 passed |
| Precision@K、宏 F1、分类型、Pairwise、B-cubed、区间 | 已证实（工具） | `tools/evaluate.py`、时间实体隔离、Cohen's kappa 和 bootstrap 用例 | 输入 60/240 双标注集并冻结报告 |
| 高影响聚类 Pairwise Precision ≥ 0.90 | 外部闸门 | 计算实现存在 | 真实冻结测试集达到门槛，否则关闭自动合并 |
| 产品 KPI：强告警接受率、错误告警、首次分诊和有效研判时长 | 已证实（采集/计算）/外部样本闸门 | 冻结 `product-metrics-2026-07-rc2.8`；反馈只能引用最新 QueueEligibility epoch；已送达告警 + 持久反馈、双 Actor 错误复核、不可变 MetricIncident 归因和值班时钟均有用例。有效研判只累计认证、服务端连续接收的 `active` 心跳，忽略客户端时长汇总，跨重开累计且要求 95% 覆盖；并保留不受 state 缩短的服务端墙钟护栏，可信边界明确不防持证内部人伪报状态 | 真实收集 30 条强告警、5 个告警工作日、50 个可研判事件和 50 次完成研判；人工评估另受签名预登记、完整快照日历和独立基线约束 |
| Owner/Analyst/Viewer 与工作区隔离 | 已证实（API key/RLS 单测）/待环境验证（联邦） | 服务器派生 workspace、RLS SQL、权限与隔离用例 | Sites/替代部署的真实身份、成员撤销和 SSE 凭证验证 |
| 外部文本、SSRF、Prompt Injection、Webhook 安全 | 已证实（代码） | URL/DNS/重定向限制、Unicode NFKC/Bidi 清理、React 转义、LLM 不参与数值评分 | 渗透测试、域名重绑定和真实出口代理验证 |
| AA、键盘、移动端、图表数据表 | 部分完成/待环境验证 | Radix Dialog、reduced-motion、断点和静态契约已通过；Sites 平台桌面截图已核验 | 企业策略禁止自动化访问 localhost/chatgpt.site；仍需 Chrome/Firefox、键盘、屏幕阅读器、对比度实测 |
| PostgreSQL Outbox、Redis、R2 一致性 | 待环境验证 | SQL、发布器、DLQ、对象存储适配器和单元用例存在 | Docker daemon/目标环境启动后跑迁移、断网和恢复演练 |
| PITR、RPO ≤1h、RTO ≤4h、Redis 丢失恢复 | 外部闸门 | 无本机生产备份环境 | 配置 WAL/PITR，执行恢复并记录实际 RPO/RTO |
| Sites 或替代部署 | 部分完成/待联邦 | owner-only 录制数据候选 v1 已部署成功，源码 SHA、归档哈希、访问人数和平台截图可追溯 | 部署独立 API/存储，完成 Sites 身份、SSE/轮询、成员撤销和回滚验证 |
| X、YouTube、中文受限源授权 | 外部闸门 | 未授权源默认关闭；YouTube 仅在 key 存在时装载 | 合同/配额/字段权利审批，不得绕过平台控制 |
| 72H soak、7 天影子运行 | 工具已证实/时间仍为外部闸门 | `acceptance_monitor.py` 追加哈希链与 Ed25519 签名 JSONL，绑定工具/schema/keyring digest，逐采样验证独立签名的完整排序账本、append-only 阈值跨越事实、Top5 精确选择、固定时间窗、单调事实账本、数据权利、非零来源和运行时证明；PG 排序/crossing 事实与数据库时钟水位来自同一只读 repeatable-read 快照。运行时证明限定 public RLS 表并精确核对触发器的表/函数 schema、事件、模式与启用状态。内存仓库、空 keyring、pending rights 和单样本 canary 均拒绝晋级 | 登记 scheduler、reviewer、baseline、ledger 四类隔离密钥和数据权利后，在生产 PostgreSQL/RLS/认证环境实际运行完整 72H/168H，并保留原始 JSONL、完整排序账本、预登记、快照、基线与签字报告 |

## 当前可复跑证明

```powershell
.\.venv\Scripts\python.exe -m pytest services\api\tests -q
ruff check services\api tools\evaluate.py
.\.venv\Scripts\python.exe -m compileall -q services\api\radar tools\evaluate.py
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe tools\live_connector_smoke.py # 非确定性、显式执行
cd web
npm.cmd run lint
npm.cmd test
```

运行态 smoke 使用 `http://127.0.0.1:8017` 与 `http://localhost:3001`，验证 health、radar、关注 CRUD、研判埋点、指标接口和网页 200。owner-only Sites 录制数据候选部署成功，证据见 [Sites 私有部署记录](evidence/SITES_PRIVATE_DEPLOYMENT_2026-07-16.md)。Docker Compose 配置可解析，但本机 Docker Desktop daemon 不可用，所以数据库迁移、RLS、Redis 与 R2 仍是明确的环境闸门。

## 当前发布结论

第二轮独立代码复审结果为：剩余 P0/P1/P2 均为“无”，当时独立复跑 `102 passed`。真实连接器 smoke 随后暴露并修复两项 OpenAlex 数据质量缺陷。第三轮复审又指出 smoke 参数无界、空响应误报 PASS 和证据措辞不精确两项 P2；现已增加 `1..10` 边界、零观测失败语义、8 个回归实例并修正文档。最终闭环复审在 `2dbbca1` 上确认当时剩余 P0/P1/P2 均为“无”，独立复跑 Python `111 passed`、Ruff、Web Lint、Vinext build、2 项渲染测试和生产依赖审计全部通过。其后新增并加固 rc2 产品 KPI、逐 revision SLA、完整签名排序/阈值跨越账本、运行时证明与长期验收监控链。最新信源治理增量的独立审计先后发现并修复并发同指纹计数、严格 5% 日上限、完整目录分页/账号实体检索、旧迁移回填和同分晋级确定性等问题；最终复核确认当前剩余 P0/P1/P2 均为“无”，独立复跑 Python `146 passed`、Ruff、compileall、pip check、Web Lint、Vinext build、2 项渲染测试、生产依赖审计、Compose 配置解析、monitor digest 和补丁检查全部通过，因此结论为**本地代码候选 GO**。

**rc2 正式 Beta 仍为 NO-GO**：当前不能宣称达到 Definition of Done，也不能进入正式私有测试。剩余阻断项不是页面或单元测试数量，而是真实数据权利、信源规模、双人标注、72H soak、7 天影子运行、生产 PostgreSQL/R2/RLS、身份联邦、可访问性和恢复证据。
