# 外部内容威胁模型与数据权利边界

更新时间：2026-07-17

## 信任边界

所有标题、正文、作者名、URL、指标、Webhook 目标和提供方响应均是不可信输入。它们只能作为数据，不能成为系统指令。V1 不调用 LLM 生成数值、状态或证据；未来若增加摘要，必须使用结构化输出、只引用已入库证据，并把外部文本放入不可执行的数据字段。

采集层执行绝对 HTTP(S)/WS(S) URL 检查、私网/loopback/reserved 地址拒绝、DNS 解析复核、逐跳重定向复核、响应大小与重试上限。Webhook 创建时和投递时均执行网络目标校验，正文有大小限制；投递使用时间戳、key id、HMAC-SHA256、五分钟防重放窗和稳定幂等键。

文本入库前执行 Unicode NFKC、双向控制字符清理和长度限制；网页由 React 转义，不把外部 HTML 注入 DOM。日志、错误和审计摘要不得包含完整 raw payload 或凭证。

## 字段级权利执行

`config/rights_policies.json` 是版本化矩阵。每项策略必须声明：允许保存的字段类别、指标政策、是否允许 raw、保留天数、摘要上限、允许的派生用途和删除范围。

运行时行为是 fail-closed：

- Registry 未达到 `active` 的连接器不发起网络请求，也不处理历史待重试内容。
- `rawPayloadAllowed=false` 的策略拒绝事实拆分；不能只依赖 UI 隐藏。
- `metricPolicy=none` 禁止任何指标；实验来源在 provider verification 前禁止指标。
- 只有冻结 Feature Registry 中登记的数值指标可持久化。GitHub watchers、Hugging Face opaque trending score、OpenAlex 重复 readers 等未登记字段不会进入 MetricSnapshot。
- 策略摘要长度在事实拆分时执行；未知策略 ID 直接失败。

来源删除清除逐 item raw、全部指标修订、成员关系、评分、标题和事件向量；仍有合法成员的事件进入 `insufficient_data` 并重新评分，无合法成员时改为不可见、零信号的审计墓碑，避免通过事件级级联删除绕过评分历史边界。共享 raw 引用只有在无其他保留事实引用后才物理删除。R2 部分失败保留待重试状态。

## 身份和隔离

生产只接受经签名验证的 EdDSA/RS256 JWT，并验证 `kid` 与签名密钥版本、算法、issuer、audience、`iat`、`exp`、`nbf`、`jti`、subject 和 workspace；最长令牌寿命为五分钟。令牌中的角色声明不参与授权，API 会从服务端 `workspace_memberships` 读取当前角色并检查 `jwt_revocations`。API key 模式只用于本地兼容，生产 readiness 拒绝它。

Owner/Analyst/Viewer 在 API 层授权；工作区事实由 PostgreSQL 强制 RLS。应用角色不是超级用户、没有 `BYPASSRLS`，迁移标记和灾备证明对应用只读。评分历史清除仅允许隔离的 `radar_deletion_worker` 执行按数据库事实推导事件范围的 `SECURITY DEFINER` 函数；该角色不能删除事件或插入清除审计，普通应用角色不能执行该函数。生产部署进一步使用 API、Scheduler、Alert 三套独立环境/Secret 注入边界，禁止以共享 `.env.production` 把删除、采集、对象存储、Webhook 与评分账本密钥扩散到无关服务。全局聚类/行为适用性治理只允许离线治理工作区。

## 明确不做

- 不绕过登录、验证码、robots、平台风控或合同限制。
- 不把单平台放大、协同风险或讨论/行为剪刀差表述为已证实的算法操控、营销投放或机器人行为。
- 不用连接器故障、缺失数据或未授权数据补零。
- 不把本地 MinIO、录制数据、合成负载或一次 smoke 当作生产合规、容量或稳定性证明。
