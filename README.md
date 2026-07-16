# SIGNAL//AI · AI 热点雷达

这是基于《AI热点雷达 V1 需求规格与执行计划 v1.0-rc2》形成的可运行私有 Beta 候选工程。产品把“生命周期状态”和“结构标签”分开：状态回答事件处在哪个阶段，标签解释它是否跨平台、是否已有采用或修复响应、是否存在剪刀差、协同发布或单平台集中。

当前仓库提供一个生产形态纵向切片：Vinext/React 研判工作台与信源治理中心、FastAPI 契约与 SSE、事件类型感知评分、P0/P1 连接器、PostgreSQL + pgvector 数据模型、Transactional Outbox、Redis Streams、R2/MinIO 原始证据、所有权去重、权限、预算化签名 Webhook 告警、候选信源登记与自动化用例。SourceScore 排行和自动晋级在真实结果集校准前保持 fail-closed。

产品 KPI 使用冻结的 `config/product_metric_policy.json`。`GET /api/v1/metrics/beta` 以已送达强告警、不可变 QueueEligibility、持久化人工反馈和服务端接收的版本化心跳为事实源；未达到最低样本时只返回“证据不足”，不会把演示或合成数据写成 Beta 达标。原有 `GET /api/v1/metrics/review` 仅用于探索性漏斗。

当前冻结版本为 `product-metrics-2026-07-rc2.9`，冻结时间为 2026-07-17 05:50（Asia/Shanghai）。本次因验收监控与迁移证明推进至 rc2.6 而重新冻结，rc2.9 之前的任何样本不得进入当前证据窗口。有效研判时长只累计认证 Analyst 的连续 `active` 心跳并要求至少 95% 覆盖，跨详情关闭/重开累计；客户端时长汇总被忽略，服务端心跳墙钟作为不能被 state 缩短的独立护栏。正式 Top5 必须逐项来自独立签名的完整排序账本，发现提前量使用版本化 append-only 阈值跨越事实。策略还绑定人工评估 schema、预登记 schema、四类隔离的 Ed25519 keyring 和验收 evaluator 的 SHA-256，任一变更都必须重新冻结并重新开始证据窗口。

owner-only 的 Sites 录制数据候选位于 <https://signal-ai-radar-rc2-seasun.m4gicarp.chatgpt.site>。它用于视觉和产品流程评审，不代表 FastAPI、真实数据、身份联邦或 rc2 正式 Beta 已上线；部署证据见 [Sites 私有候选记录](docs/evidence/SITES_PRIVATE_DEPLOYMENT_2026-07-16.md)。

## 工程结构

```text
web/                         Vinext / React 网页与 Sites 配置
services/api/radar/          FastAPI、评分、聚合、聚类、采集与告警
services/api/tests/          规则、接口、故障和运营约束用例
infra/postgres/001_init.sql  PostgreSQL / pgvector / Outbox 数据模型
docs/                        用例、实现边界与交付记录
docker-compose.yml           PostgreSQL、Redis、MinIO 本地基础设施
```

## 本地运行

要求 Node.js 22+ 与 Python 3.12+。

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r services\api\requirements-dev.txt
$env:PYTHONPATH="services/api"
.\.venv\Scripts\python.exe -m uvicorn radar.main:app --host 127.0.0.1 --port 8017
```

另开终端：

```powershell
cd web
npm.cmd ci --ignore-scripts
$env:NEXT_PUBLIC_API_URL="http://127.0.0.1:8017"
npm.cmd run dev
```

访问 `http://localhost:3000/`。API 文档位于 `http://127.0.0.1:8017/docs`。

若需要真实 PostgreSQL/Redis/Object Storage：

```powershell
docker compose up -d
$env:DEMO_MODE="false"
$env:DATABASE_URL="postgresql://radar_app:radar-app-local-only@localhost:5432/ai_hot"
```

采集/评分 Worker：

```powershell
$env:PYTHONPATH="services/api"
.\.venv\Scripts\python.exe -m radar.runner --once
```

计量连接器默认采用 fail-closed：除月度上限外，还必须通过 `CONNECTOR_COST_RMB_<CONNECTOR_ID>` 明确配置合约折算的人民币/请求成本；只有合同确认为零成本时才填写 `0`。Worker 每轮把实际请求数折算入当月持久台账，并在下一轮外部调用前按最坏重试成本校验余额。

告警 Worker 只处理 PostgreSQL 已提交并发布到 Redis 的评分事件；工作区必须显式列出，避免绕过 RLS：

```powershell
$env:RADAR_WORKSPACE_IDS="workspace-a"
$env:WEBHOOK_SIGNING_SECRET="replace-with-a-secret"
.\.venv\Scripts\python.exe -m radar.alert_worker
```

Redis Stream 全量丢失时，先停止调度常规 Outbox publisher 和消费者并等待当前租约退出，再按冻结时间窗从 PostgreSQL 的已提交 Outbox 重建。恢复命令会原子拒绝仍有活跃 publisher/consumer 的目标 Stream；恢复期间新启动的 publisher/consumer 也会 fail closed。命令默认仅输出 dry-run 候选统计，不写 Redis；执行模式要求 `--confirm-stream` 与目标 Stream 完全一致，且 `--until` 不得晚于 PostgreSQL 时钟。常规 publisher、恢复和裁剪共用冻结的 `STREAM_OUTBOX_KINDS`，覆盖 observation、metric snapshot、score、feedback、cluster edit、rescore 与 source deletion 七类事件；未登记 kind 会记录失败且不得进入 Stream。恢复使用稳定 `outbox_id`、互斥锁和 Redis 内续跑检查点，不改写 PostgreSQL 发布事实：

```powershell
$env:DATABASE_URL="postgresql://radar_app:...@host/database"
$env:REDIS_URL="redis://host:6379/0"
.\.venv\Scripts\python.exe tools\replay_outbox_to_redis.py `
  --since 2026-07-17T00:00:00+08:00 --until 2026-07-18T00:00:00+08:00
.\.venv\Scripts\python.exe tools\replay_outbox_to_redis.py `
  --since 2026-07-17T00:00:00+08:00 --until 2026-07-18T00:00:00+08:00 `
  --execute --confirm-stream radar:events
```

重放仍是 at-least-once；进程在 XADD 与检查点之间中断时可能再次投递同一 `outbox_id`，所以消费者的业务幂等约束不能关闭。操作与故障窗口证据见 [Outbox/Redis 恢复验证](docs/evidence/OUTBOX_REDIS_RECOVERY_2026-07-17.md)。
恢复锁存在或检查点仍为 `running` 时，常规 publisher 和 consumer 都会 fail closed。检查点同时保存 PostgreSQL `(created_at,id)` 与最后一条 Redis Stream ID；续跑不仅核对末条，还会以自动清理的磁盘临时账本精确对账 PostgreSQL 期望前缀与 Redis 去重后的完整 `outbox_id` 顺序及累计数。未完成检查点若存在前缀缺失、多余、乱序或找不到对应 Redis 行，会拒绝续跑并要求清理检查点后从完整窗口重启；已完成恢复的 Stream 后续再次丢失，则清除 completed cursor 后自动全量重建。命令完成后先启动消费者，再恢复常规 publisher；重放只覆盖历史上已标记 published 的相关事件，仍处于 pending 的 Outbox 会由常规 publisher 发布。保留 PostgreSQL Outbox 的时间范围必须覆盖所选恢复窗口，并为前缀校验预留足够临时磁盘空间。

Redis Stream 保留采用显式维护，不由 publisher 自动 `MAXLEN` 裁剪。操作前停止该 Stream 的 publisher/consumer 与 PostgreSQL Outbox 清理任务，并等待参与者租约排空；当前租约只约束应用参与者，不能替代发布链整体的跨系统 fencing。工具会拒绝活跃参与者、恢复锁、未完成恢复状态或缺少必需 consumer group。默认只 dry-run：它以 Redis 时钟计算保留期边界，取所有现存消费者组的最早 pending/last-delivered 安全水位，只把严格早于最终边界的消息列为候选，并逐个核对其 `outbox_id` 是现有恢复工具支持的已发布 Outbox kind。execute 必须回填同一次 dry-run 的 `safeTrimMinId`；执行期间 PostgreSQL `FOR SHARE` 行锁阻断候选恢复事实被删除，Redis Lua 在同一原子操作内重新核对完整消费者组状态并精确裁剪。任何无效、状态变化或不可恢复消息都会拒绝 execute；安全边界内仍超过容量上限时只报告 `over_limit`，不会强制删除：

```powershell
$env:DATABASE_URL="postgresql://radar_app:...@host/database"
$env:REDIS_URL="redis://host:6379/0"
.\.venv\Scripts\python.exe tools\trim_redis_stream.py `
  --stream radar:events --retention-hours 168 --capacity-limit 1000000 `
  --required-group radar-alerts
.\.venv\Scripts\python.exe tools\trim_redis_stream.py `
  --stream radar:events --retention-hours 168 --capacity-limit 1000000 `
  --required-group radar-alerts --execute --confirm-stream radar:events `
  --confirm-before-id 1784200000000-0
```

`--confirm-before-id` 的值必须原样复制紧邻 dry-run 输出的 `safeTrimMinId`。该值是获批的最大裁剪边界：execute 会重新计算 `calculatedSafeTrimMinId`；若安全水位后退则拒绝，若只随时间或消费进度前移，仍严格按较早的获批 ID 裁剪，不扩大删除范围。多组场景需重复传入 `--required-group`，或设置逗号分隔的 `RADAR_REQUIRED_STREAM_GROUPS`。PostgreSQL Outbox 保留期必须不短于 Redis 可恢复窗口。实现、实测断言与尚未覆盖的 Redis Cluster/目标规模边界见 [Redis Stream 安全保留验证](docs/evidence/REDIS_STREAM_RETENTION_2026-07-17.md)。

生产认证开启时，修改全局“行为不适用”只允许离线治理工作区的 Owner。请为治理身份单独配置 `RADAR_SYSTEM_WORKSPACE_ID`；普通工作区的 Analyst/Owner 反馈不会直接改写全局事件分数。

## 测试

```powershell
.\.venv\Scripts\python.exe -m pytest services\api\tests -q
ruff check services\api\radar services\api\tests
cd web
npm.cmd run lint
npm.cmd test
```

测试不调用外部平台；连接器使用录制/MockTransport 响应，避免配额、网络与授权状态让 CI 变得不确定。真实源的连通性属于部署环境 smoke test。

默认套件会跳过 11 项真实基础设施用例。启动 `docker compose` 并显式配置 `POSTGRES_INTEGRATION_DSN`、`POSTGRES_INTEGRATION_ADMIN_DSN`、`REDIS_INTEGRATION_URL` 与 `S3_INTEGRATION_*` 后，可验证实际迁移/RLS/trigger、并发去重、Outbox→Redis、consumer group、安全水位裁剪和 MinIO 对象操作。协调重启脚本还要求用 `DOCKER_INTEGRATION_CONTEXT` 指定经校验的本地 Docker context，并 fail-closed 限定 compose 的 loopback 端口 `5432/6379/9000`，不会接触远端服务：

```powershell
.\.venv\Scripts\python.exe -m pytest services\api\tests\test_postgres_integration.py services\api\tests\test_infrastructure_integration.py -q
.\.venv\Scripts\python.exe tools\infrastructure_recovery_smoke.py
```

本次实际运行结果和生产边界见 [基础设施集成验证记录](docs/evidence/INFRASTRUCTURE_INTEGRATION_2026-07-17.md)。

需要显式验证当前网络与公开元数据响应时，可运行：

```powershell
.\.venv\Scripts\python.exe tools\live_connector_smoke.py
```

该命令只执行有界读取，不进入确定性 CI，也不替代数据权利审批或 72 小时 soak。一次实际运行记录见 [真实公共连接器 smoke](docs/evidence/LIVE_CONNECTOR_SMOKE_2026-07-16.md)。

长期验收不靠手写汇总。部署后的外部调度器每 15 分钟追加一个只读样本：

```powershell
.\.venv\Scripts\python.exe tools\acceptance_monitor.py collect `
  --api-url https://your-private-api.example `
  --output .data\acceptance\shadow.jsonl `
  --evidence-window-hours 168 `
  --manual-preregistration .data\acceptance\manual-preregistration.json
```

到达完整时间窗后再生成报告：

```powershell
.\.venv\Scripts\python.exe tools\acceptance_monitor.py report `
  --input .data\acceptance\shadow.jsonl --mode soak72h

.\.venv\Scripts\python.exe tools\acceptance_monitor.py report `
  --input .data\acceptance\shadow.jsonl --mode shadow7d `
  --manual-evaluation reviewed-product-evaluation.json
```

`canary` 模式只验证接口和证据格式，固定不具备正式时长资格。72H/7D 报告会检查采样覆盖、连接器权利/连续性/重复率、逐 revision 的 15 分钟评分 SLA；7D 还要求冻结产品 KPI 和符合 `config/manual_product_evaluation.schema.json` 的独立双人评估输入。

正式采集还要求外部调度器在每次调用前注入 `ACCEPTANCE_MONITOR_SCHEDULED_AT`、`ACCEPTANCE_MONITOR_RUN_ID`、`ACCEPTANCE_MONITOR_ED25519_KEY_ID` 和 32-byte raw private key 的 Base64 值 `ACCEPTANCE_MONITOR_ED25519_PRIVATE_KEY`。API 进程另用独立的 `SCORE_LEDGER_ED25519_KEY_ID` / `SCORE_LEDGER_ED25519_PRIVATE_KEY` 签名每个采样时点的完整排序与阈值跨越账本。每日固定时点额外传入 `--manual-snapshot`，将由完整账本机械选出的 Top5 承诺绑定进同一监控样本。私钥不能写入仓库；四类公钥只登记在 `config/acceptance_monitor_public_keys.json`。

正式模式会 fail closed，只有以下条件同时成立才有资格判定：PostgreSQL、受限 `radar_app` 角色、强制 RLS、精确匹配表/函数/schema/事件/启用状态的关键审计 triggers、只读迁移标记 `001_init_rc2.6`、`AUTH_REQUIRED=true`、稳定 `RADAR_INSTANCE_ID`、数据库/应用时钟偏差不超过 5 秒、冻结 keyring 中存在分离的 scheduler/reviewer/baseline/ledger 公钥，以及所有必需连接器获得显式数据权利批准。InMemory、录制 demo、空 keyring 和 `pending/blocked` 权利状态均不能通过正式 72H/7D。

## 关键约束

- 覆盖低于 40 或不足两个独立信号家族时，只能输出“数据不足”。
- 强状态必须满足事件类型的最低证据组合并经过跨信号家族确认；模型/开发工具要求行为证据，研究或产品在行为 N/A 时不得输出“采用已确认”。
- 萌发要求至少一个适用增长指标达到 `robust Z ≥ 2`；降温要求连续两个下降周期，随后按事件类型进入休眠，新异常会以“再次活跃”回到萌发。
- 模型、开发工具、研究、产品和安全事件使用不同的行为指标与预期滞后。
- 原始指标角色来自冻结的 `config/feature_registry.json`；播放、评论、搜索和 Star 不能单独触发采用确认，安全修复响应与采用确认分开。
- 连接器故障冻结上一值并提高不确定性，不把缺数解释为降温。
- 每个事件保存评分版本、阈值版本、输入窗口摘要和驱动因素。
- Webhook 使用 HMAC-SHA256；外部 URL 经过 SSRF 安全校验。
- 同一实体控制的跨平台账号通过审核后的身份登记表去重；未确认的所有权不会靠名称自动合并。
- 告警每天最多 10 条/工作区、3 条/领域；低证据、无新增证据且无状态升级的事件不会外推。
- 告警预算、冷却和幂等键在数据库预留事务内原子检查；Webhook 失败会释放预留。
- 指标型来源对同一外部对象保持稳定内容 ID并追加指标事实；跨来源搜索结果逐 item 归档，以支持选择性物理删除。
- 默认月度数据预算为 2,000 元，达到 75%/90%/100% 时分级降频并显式惩罚覆盖。

## 生产前仍需完成

代码已具备运行与集成形态，聚类编辑执行/撤销与关注继承也已实现；但第三方授权、120–200 个真实活跃源和 500 候选源、60/240 条双人标注集、72 小时连接器 soak、7 天影子运行、2,000 信源真实容量、500 万观测 PostgreSQL 压测以及 Sites 与独立 FastAPI 的生产身份联邦，必须在真实组织环境执行。它们不能由本地模拟结果替代，详见 [实现状态](docs/IMPLEMENTATION_STATUS.md) 和 [完成审计](docs/COMPLETION_AUDIT.md)。
