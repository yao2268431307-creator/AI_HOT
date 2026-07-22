# AI 热点雷达 V1：本机零订阅运行手册

## 运行边界

本模式只允许当前电脑通过 loopback 地址访问。运行时由以下组件组成：

- Web 工作台：`localhost:3210`
- FastAPI：`127.0.0.1:8017`
- 15 分钟采集/评分 Worker
- Docker 本地 PostgreSQL + pgvector
- PostgreSQL 本地连接池（默认 1–4 个连接，避免 Windows 短连接耗尽）
- `.data/evidence` 原始证据目录
- `.data/models` 本地 BGE-M3 模型

不需要 Neon、Upstash、R2、Auth0/OIDC 或任何绑定支付方式的服务。Redis 和 MinIO 仅保留在可选 `full` profile，默认不会启动。

## 启动和停止

首次运行：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\start-local.ps1
```

脚本会创建未提交到 Git 的 `.env`、启动并迁移 PostgreSQL、安装本地模型依赖、尝试下载 BGE-M3，然后隐藏启动 API、Worker 和 Web。模型准备需要数 GB 下载与磁盘空间，但不会产生 API 账单。模型文件下载失败时脚本会报警并继续启动；Worker 使用 `local_files_only`，不会在每条内容处理时反复访问模型站点。

如果只需验证程序而暂时不下载模型：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\start-local.ps1 -SkipModelSetup
```

此时模型状态显示“降级”，系统继续使用 URL、实体、标题和词法规则聚类，覆盖置信度扣减 15 分，低覆盖事件不会发送强告警。

停止：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\stop-local.ps1
```

## 零费用保护

`.env` 默认固定：

```env
RUNTIME_PROFILE=local
FREE_ONLY_MODE=true
EXTERNAL_DATA_BUDGET_RMB=0
ENABLED_CONNECTORS=rss,hackernews,github,huggingface,arxiv,bluesky
```

连接器分为 `public_no_billing`、`user_token_no_billing`、`metered` 和 `restricted`。本机模式只允许前两类；启用 OpenAlex、YouTube 等非免费类别会在任何网络请求前拒绝启动。GitHub Token 可留空，或使用用户已有的无计费 Token。

当前 RSS 登记表包含 22 个已验证可访问的中英文官方、研究与媒体 Feed。Hacker News、GitHub、Hugging Face、arXiv 和 Bluesky 会继续发现动态信源；新信源只进入候选池，不会自动晋级。某个免费上游在当前网络不可达时只将该连接器标记为 degraded，其他连接器继续采集，覆盖缺口会显示在页面上。

信源中心提供“启用、暂停、阻断、退回候选”人工审核操作，每次状态变化都要求填写依据并写入不可变审计事实。自动晋级保持关闭，容量上限为 500 个活跃信源。

## 本地数据与告警

- 原始证据按连接器和 UTC 日期保存，默认保留不超过 30 天。
- 趋势明细默认保留 180 天。
- 证据目录上限为 20GB；达到上限后，大体积正文改存可审计占位记录，数据库评分事实不删除。
- 告警通过 PostgreSQL Outbox 在 Worker 内直接处理并写入站内告警；不使用 Redis 或外部 Webhook。
- API 和 Web 无登录，因此只允许 loopback 地址和 loopback CORS 来源；任何非本机绑定会拒绝启动。

## 备份和恢复验证

创建备份：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\backup-local.ps1
```

备份前会检查事件/观测外键完整性；发现孤儿事实时拒绝生成一个无法可靠恢复的备份。备份包含 PostgreSQL dump、Feed/身份配置和证据索引，位于 `.data/backups`，只保留最近 7 份。可通过 `-Destination` 指定外接硬盘目录。

验证恢复不会覆盖当前数据库，而是创建临时数据库、恢复、检查观测/事件数量后删除临时数据库：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\verify-local-restore.ps1 -BackupFile .data\backups\<timestamp>.zip
```

同一磁盘备份不能防止整盘损坏，不应描述为正式灾备。

## 72 小时本地连续采集

确保电脑保持通电，然后执行：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\start-local-soak.ps1
```

监控器启动一个不需要管理员权限的 Windows 进程级 wake lock，运行期间阻止系统自动睡眠；合盖、断电、关机或用户主动睡眠仍会中断测试。它每 15 分钟检查运行状态、雷达、覆盖、管线延迟、重复率和零费用约束。原始样本和最终摘要分别写入：

- `.data/acceptance/local-soak.jsonl`
- `.data/acceptance/local-soak-summary.json`

该测试证明单机采集和评分稳定性，不证明公网可用性、跨机器高可用或正式灾备。

## 未覆盖平台

本机 V1 不采集 X、YouTube、OpenAlex、Bilibili、Reddit 和 Product Hunt。页面会明确显示这些缺口；缺失数据只降低覆盖置信度，不会被解释为热点降温。监控上万信源、多人权限、外部通知和公网部署属于未来版本。
