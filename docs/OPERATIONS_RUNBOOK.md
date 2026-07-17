# AI 热点雷达 rc3 运维手册

更新时间：2026-07-17

本文只定义可执行的生产门槛，不把本地容器、录制数据或合成负载视为正式 Beta 证据。

## 发布前置条件

生产形态使用 PostgreSQL/pgvector 作为唯一事实源，Redis Streams 作为可重放投递层，R2/S3 作为原始证据存储。API、采集/评分 Worker、告警消费者和 Web 必须是独立进程。

发布必须同时满足：

- `DEMO_MODE=false`、`AUTH_REQUIRED=true`、`RADAR_AUTH_MODE=jwt`。
- JWT issuer、audience 和只读公钥 keyring 已登记；API 不持有身份提供方私钥。
- PostgreSQL 迁移标记精确为 `001_init_rc3.1`；`radar_app` 与隔离的 `radar_deletion_worker` 都无超级权限、无 `BYPASSRLS`。应用角色不能执行评分历史清除函数、删除事件或伪造清除审计。
- Redis、R2、稳定实例 ID、来源身份文件和已审批 RSS 清单已配置。
- 总预算、连接器预算、信号族预算均为正数 JSON 映射；计量连接器的 RMB/request 成本明确。未知成本直接暂停。
- API/Web 最终镜像和 Python/Node 基础镜像均以 digest 固定；运行时 `RADAR_RELEASE_IMAGE_DIGESTS` 与发布登记一致。
- 五个运行组件心跳正常，最近一次真实恢复演练 RPO ≤ 1 小时、RTO ≤ 4 小时且不超过 90 天。
- 所有启用连接器的权利状态已由数据负责人改为 `active`；仓库默认的 pending/blocked 状态不能晋级。

生产 Compose 文件是 [compose.production.yml](../infra/compose.production.yml)。它强制使用 API、Scheduler、Alert 三份独立环境文件，并分别挂载已复核来源身份表和 RSS 清单；示例边界见 [API](../.env.api.example)、[Scheduler](../.env.scheduler.example) 与 [Alert](../.env.alert.example)。API 不持有 Redis、R2 或平台采集凭据；Scheduler 不持有删除角色、Webhook 或评分账本私钥；Alert 不持有平台令牌、删除数据库或评分账本私钥。正式密钥由部署平台 Secret Manager 按工作负载身份注入，不得提交生产环境文件。Compose 只绑定宿主机 loopback 端口，需要组织网关负责 TLS、访问控制和限流。运行服务需要出站访问各自获准的托管依赖、官方数据提供方和 Webhook；不能把网络标记为 Docker internal 后又声称连接器可用。完整配置和正式验收步骤见 [生产配置与验收手册](PRODUCTION_CONFIGURATION.md)。

`GET /health` 仅公开版本和时钟；`GET /health/ready` 是最小化的编排 readiness。完整依赖、数据库角色和发布证明仅由 Owner 通过 `GET /api/v1/operations/runtime-health` 读取。生产任一证明缺失时 readiness 返回 503。15 分钟调度组件的心跳容忍窗口按声明周期计算，而不是错误地固定为 3 分钟。采集 Worker 在每轮采集前重新执行 Redis ping 与目标 R2 bucket 写/读/删探针；Alert 消费者每轮消费前以自身独立身份执行 Redis ping 和专用 canary 前缀的删除权限探针，代码不会主动执行写或读。任一探针失败时对应 Worker 不得继续处理，并写入失败心跳。readiness 同时要求两类 R2 权限证明，且不接受超过 30 分钟的旧探针结果。若对象存储供应商不能签发严格 delete-only 的长期凭据，应为 Alert 使用短时、受策略约束的会话凭据并注入 `R2_SESSION_TOKEN`；不得把供应商控制台的宽权限长期令牌描述为 delete-only。

## 监控与告警

`GET /metrics` 仅允许 Owner 身份，输出低基数 Prometheus 文本。建议由内部抓取器使用短期 JWT 访问。

| 信号 | 告警条件 | 级别与动作 |
|---|---|---|
| `radar_http_requests_total{status_class="5xx"}` | 5 分钟错误率 > 1% | P1；检查 API、DB 和身份依赖 |
| `radar_http_request_duration_p95_seconds` | 连续 3 个窗口 > 0.5s | P1；检查查询计划、缓存和返回量 |
| `radar_runtime_component_heartbeat_age_seconds` | 超过 readiness 的周期化阈值 | P1；重启对应组件并检查租约/队列 |
| `radar_runtime_component_last_cycle_duration_seconds{component="collector-worker"}` | > 300s | P1；暂停低优先连接器，检查聚类/评分和上游延迟 |
| `radar_observation_processing_backlog` | > 100 或持续增长 3 周期 | P1；结合 `/api/v1/operations/pipeline-sla` 查成熟未完成项 |
| `radar_outbox_backlog` | > 1,000 或持续增长 3 周期 | P1；检查 Redis、publisher fencing 和消费者组 |
| `radar_connector_runs_24h{status!="healthy"}` | 同一连接器连续失败 2 周期 | P2；单独停用，不把故障解释为热度下降 |
| `radar_connector_coverage_ratio` | < 0.6 | P2；界面降置信度，禁止强结论 |
| `radar_external_data_budget_utilization_ratio` | ≥ 0.85 | P2；系统自动降频，人工检查分类预算 |
| 同上 | ≥ 1.0 | P1；计量连接器硬暂停，禁止静默超支 |
| `radar_connector_budget_reconciliation_pending` | > 0 | P1；停止受影响计量连接器，核对提供方账单并完成显式对账 |

日志是单行 JSON，关键事件包括 `collector_cycle_completed`、`retention_cycle_completed` 和 `outbox_publish_completed`。日志平台应按 `event`、`connector_id`、`instance_id` 聚合；不得把原始外部正文、JWT 或访问密钥写入日志。

## 崩溃预算对账

计量连接器发起提供方请求前先取得一小时预算租约。进程在确认费用前崩溃时，过期租约进入 `reconciliation_required`，继续按最坏成本占用预算并使生产 readiness 失败；系统绝不自动释放额度。

数据库管理员先只读查询待处理记录，结合提供方请求日志、账单和事故时间线判断是否真实产生费用：

```sql
SELECT owner_token,connector_id,signal_family,reserved_amount_rmb,created_at,lease_until
FROM connector_budget_reservations
WHERE status='reconciliation_required'
ORDER BY created_at;
```

确认未调用提供方时，由管理员执行 `released`；确认已产生费用时执行 `reconciled_charged` 并填写不超过预留额的真实费用。理由至少八个字符，函数会追加不可变审计事实：

```sql
SELECT resolve_connector_budget_reservation(
  '<owner-token>'::uuid,
  'released',
  0,
  'provider logs confirm no request was sent'
);

SELECT resolve_connector_budget_reservation(
  '<owner-token>'::uuid,
  'reconciled_charged',
  12.50,
  'provider invoice and request log verified'
);
```

`radar_app`、`radar_deletion_worker` 和公共角色均不能执行该函数。完成后确认待对账数量为零、预算台账正确且 readiness 恢复；不得直接更新预留表或删除审计记录。

## 容量验收

`tools/synthetic_load.py` 和 `tools/api_benchmark.py` 只用于发现算法数量级退化，不能证明生产容量。

正式容量证据使用只读工具：

```powershell
python tools/production_capacity_probe.py `
  --dsn-file C:\secure\radar-capacity-reader-dsn `
  --base-url https://radar.example `
  --token-file C:\secure\radar-owner-jwt `
  --output C:\secure\capacity-evidence.json
```

工具只有在生产 readiness 通过、真实库达到 10,000 信源/5,000,000 观测/2,000 活跃事件、最近完整采集评分周期 ≤ 300 秒、120 次雷达请求 P95 ≤ 500ms 时才输出 `qualifies=true`。它不写数据库，也不生成伪造容量数据。

## 备份、恢复与 PITR

PostgreSQL 托管层必须启用连续 WAL/PITR，目标 RPO 1 小时、RTO 4 小时。每 90 天至少完成一次隔离恢复：

1. 从生产备份恢复到隔离实例，禁止连接生产 Redis、R2 和 Webhook。
2. 校验迁移标记、行数、最近 ScoreRun 输入摘要、Outbox 连续性、随机原始证据引用和 RLS。
3. 记录备份引用、恢复实例、验证摘要、实际 RPO/RTO 和操作者。
4. 由数据库管理员使用非 `radar_app` 身份向 `disaster_recovery_attestations` 追加一条记录。应用角色只有 SELECT 权限。
5. 若演练失败或超过目标，写入 `status='failed'`；readiness 必须继续拒绝生产就绪，不能手工覆盖为 passed。

Redis 丢失时以 PostgreSQL Outbox 为真源，先执行重放工具 dry-run、核对完整顺序账本，再在排他 fencing token 下执行。禁止直接裁剪含未确认 PEL 的 Stream。R2 删除和保留任务采用逐对象确认；部分失败不得确认完成。

## 变更、停机与回滚

- 单连接器：先在 Registry/部署配置中禁用，再确认不再产生新 run；其故障只降低覆盖，不写入零热度。
- 评分：每次变更必须提升 Score/Threshold/Evidence/Label 中相应版本并重新冻结产品验收策略。历史 `score_runs` 不更新。
- 聚类：人工合并/拆分走带乐观锁的命令和谱系；不要直接改事件成员表。
- 发布回滚：切回上一组 API/Web 镜像 digest；数据库迁移为前向兼容，禁止 `git reset` 或破坏性回滚事实表。
- 紧急停机：暂停 scheduler-worker 和 alert-consumer，保留 API 只读取证；不要删除 Outbox、ScoreRun、反馈或审计事实。

## 当前仍需外部完成

仓库已提供门槛和验证工具，但没有提供真实权利批准、正式 JWT 身份映射、目标环境 PITR 记录、目标规模容量结果、72 小时采集或 7 天影子运行。因此当前只能判定为本地代码候选，正式 Beta 仍是 NO-GO。
