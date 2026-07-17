# AI 热点雷达生产配置与验收手册

更新时间：2026-07-17

目标：把当前 rc3 本地代码候选部署成可开始 72 小时正式采集的生产形态。所有命令默认从仓库根目录执行；密钥只从 Secret Manager 或 root-owned 文件注入。

## 0. 管理员输入

以下对象必须由组织管理员创建，仓库不能代替：

- 私有 GitHub 仓库或组织，以及 GHCR Packages 权限；
- 两个固定到 digest 的基础镜像变量 `PYTHON_BASE_IMAGE`、`NODE_BASE_IMAGE`；
- 支持 PostgreSQL 16、pgvector、pgcrypto、PITR 的 `ai_hot` 数据库；
- TLS Redis；
- 私有 R2/S3 bucket 与 Scheduler、Alert 两套凭据；
- 能签发五分钟 JWT 的 IdP client；
- 数据权利审批工单和正式域名/TLS 网关。

其余步骤已经自动化。

## 1. 发布流水线

`.github/workflows/release.yml` 只在 `rc3-*` tag 或手工 dispatch 时发布。所有第三方 Action 固定到提交 SHA。流水线顺序为：Python/Web 测试 → immutable base 检查 → API/Web 构建（BuildKit max provenance + SBOM）→ keyless Cosign 镜像签名 → 签名复验 → 签名 release manifest。

在 GitHub repository variables 中配置：

```text
PYTHON_BASE_IMAGE=python:<reviewed-tag>@sha256:<64 hex>
NODE_BASE_IMAGE=node:<reviewed-tag>@sha256:<64 hex>
```

可选变量 `ENABLE_GITHUB_ATTESTATIONS=true` 会额外发布 GitHub 原生 provenance。GitHub 官方说明：私有/内部仓库的 Artifact Attestations 需要 Enterprise Cloud；非 Enterprise 私有仓库保持该变量未设置，Cosign 签名与签名 manifest 仍是必需门禁：[GitHub Artifact Attestations](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations)。

创建并推送发布 tag 后，从 Actions 下载：

```text
production-release-<commit>/release-manifest.json
production-release-<commit>/release-manifest.json.sha256
production-release-<commit>/release-manifest.sigstore.json
```

部署机安装 Cosign 后，用管理员批准的仓库和 commit 验证签名并生成 `/etc/ai-hot/release.env`；工具会同时验证 manifest、API 镜像和 Web 镜像的 GitHub Actions OIDC 身份：

```bash
python tools/render_verified_release.py \
  --manifest release-manifest.json \
  --checksum release-manifest.json.sha256 \
  --sigstore-bundle release-manifest.sigstore.json \
  --expected-repository <owner/repository> \
  --expected-commit <approved-full-commit-sha> \
  --public-api-url https://radar-api.example.com \
  --public-web-url https://radar.example.com \
  --output /etc/ai-hot/release.env
```

Enterprise Cloud 且启用了 GitHub 原生 attestation 时额外传 `--verify-github-attestations`。模板 `infra/deploy/release.env.example` 仅用于理解字段，不得绕过签名验证手写正式 digest。Cosign blob bundle 的验证语义见 [Sigstore 官方文档](https://docs.sigstore.dev/cosign/signing/signing_with_blobs/)。

## 2. PostgreSQL

数据库必须命名为 `ai_hot`。管理员把三份随机密码写入独立的临时 root-only 文件。为避免管理员密码出现在进程参数中，推荐把连接放入 root-only 的 libpq service 文件 `/run/secrets/ai-hot-pg-service.conf`，管理员 DSN 文件只写非敏感引用 `service=ai_hot_admin`，然后执行：

```bash
export PGSERVICEFILE=/run/secrets/ai-hot-pg-service.conf

python tools/provision_production_database.py runtime-roles \
  --admin-dsn-file /run/secrets/postgres-admin-dsn \
  --app-password-file /run/secrets/radar-app-password \
  --deletion-password-file /run/secrets/radar-deletion-password

psql service=ai_hot_admin -v ON_ERROR_STOP=1 -f infra/postgres/001_init.sql

python tools/provision_production_database.py capacity-reader \
  --admin-dsn-file /run/secrets/postgres-admin-dsn \
  --password-file /run/secrets/radar-capacity-reader-password
```

应用 DSN 仅使用 `radar_app`；`DELETION_DATABASE_URL` 仅放进 API Secret。Scheduler 和 Alert 显式清空此变量。

以 DBA 身份写入 IdP subject 与服务端角色：

```sql
INSERT INTO workspace_memberships(workspace_id,subject,role,status)
VALUES ('<workspace>','<idp-subject>','OWNER','active');
```

## 3. R2/S3 和正式来源文件

创建非公开 bucket。Scheduler 凭据需要目标 bucket 的读、写、删；Alert 使用另一身份，代码只执行删除。若使用 action-scoped 临时凭据，把 session token 注入 `R2_SESSION_TOKEN`，并在凭据到期前由 Secret Manager 轮换和重启对应工作负载。

把经审批的文件放到部署机：

```text
/etc/ai-hot/source-identities.json
/etc/ai-hot/rss-feeds.json
```

结构参考 `config/source_identities.production.example.json` 与 `config/feeds.production.example.json`。示例域名固定不能通过 formal preflight。

六类正式连接器需在 `rights_policies.json` 登记审批工单、审批文件 SHA-256、带时区审批时间，以及互相分离的数据权利/安全/产品审批主体；RSS 还要在正式 Feed 清单逐项登记发布方审批引用和证据摘要。只有把经签字材料保存在组织文档系统并完成代码审阅后，才能将 `rightsStatus` 改为 `active`。

## 4. 服务 Secret

从以下模板渲染：

- `.env.api.example` → `/etc/ai-hot/api.env`
- `.env.scheduler.example` → `/etc/ai-hot/scheduler.env`
- `.env.alert.example` → `/etc/ai-hot/alert.env`
- `infra/deploy/release.env.example` → `/etc/ai-hot/release.env`

Linux 权限：

```bash
chown root:root /etc/ai-hot/*.env /etc/ai-hot/*.json
chmod 600 /etc/ai-hot/*.env
chmod 640 /etc/ai-hot/*.json
```

API JWT keyring 是 `{"<kid>":"<PEM public key>"}`。IdP token 必须包含 `sub`、`workspaceId`、`jti`、`signingKeyVersion`，其中 `signingKeyVersion == kid`，且 `exp-iat <= 300` 秒。

Scheduler 对所有启用的 metered connector 同时配置三层预算。例如：

```env
EXTERNAL_DATA_BUDGET_RMB=2000
CONNECTOR_BUDGETS_RMB_JSON={"github":300,"huggingface":200,"openalex":300}
SIGNAL_FAMILY_BUDGETS_RMB_JSON={"behavior":500,"research":300}
CONNECTOR_COST_RMB_GITHUB=0
CONNECTOR_COST_RMB_HUGGINGFACE=0
CONNECTOR_COST_RMB_OPENALEX=0
```

只有合同确认零成本时才能把单次请求成本设为 `0`。
API 环境中的三项预算上限必须与 Scheduler 完全一致，用于覆盖页和告警展示；只有 Scheduler 持有单次请求成本与平台凭据并执行扣费。

正式六源采集还要求经批准的 `GITHUB_TOKEN` 与运维联系邮箱 `OPENALEX_MAILTO`。API、Scheduler、Alert 的 `DATABASE_URL` 必须都使用 `radar_app` 且指向同一数据库；删除 DSN 只能使用 `radar_deletion_worker`。Scheduler 与 Alert 必须使用同一个 bucket、不同的对象存储身份，Alert 正式运行必须使用带 `R2_SESSION_TOKEN` 的短时凭据。

## 5. Acceptance keyring

该步骤应在管理员批准的秘密管理工作站执行。它生成五个互不复用的 Ed25519 身份：Scheduler、Reviewer A、Reviewer B、Baseline、Score Ledger，并自动把公开 keyring 的精确摘要绑定进新的产品策略版本。

```bash
python tools/generate_acceptance_keyring.py \
  --private-output-dir /secure-transfer/ai-hot-acceptance-keys \
  --keyring-version acceptance-keys-<date>-v1 \
  --product-policy-version product-metrics-<date>-v1 \
  --frozen-at <ISO-8601-with-timezone>
```

每个 private 文件导入不同 Secret/工作负载身份。导入和回读验证完成后安全删除 `/secure-transfer`。公开的 `config/acceptance_monitor_public_keys.json` 和更新后的产品策略走代码审阅；任何旧采样不能进入新窗口。

## 6. Preflight 与部署

基础设施阶段：

```bash
python tools/production_preflight.py \
  --release-env /etc/ai-hot/release.env \
  --phase bootstrap \
  --output /var/lib/ai-hot-evidence/preflight-bootstrap.json
```

数据权利和 keyring 完成后：

```bash
python tools/production_preflight.py \
  --release-env /etc/ai-hot/release.env \
  --phase formal \
  --output /var/lib/ai-hot-evidence/preflight-formal.json
```

只有 `qualifies=true` 才部署：

```bash
docker compose -f infra/compose.production.yml --env-file /etc/ai-hot/release.env config
docker compose -f infra/compose.production.yml --env-file /etc/ai-hot/release.env pull
docker compose -f infra/compose.production.yml --env-file /etc/ai-hot/release.env up -d
```

应用端口只绑定 loopback。由组织网关终止 TLS、限制内部身份并把 Web/API 域名转发到 `127.0.0.1:3000/8017`。

## 7. PITR 与灾备证明

托管 PostgreSQL 打开连续 WAL/PITR，保留至少 7–14 天。每 90 天恢复到新的隔离实例；该实例禁止连接生产 Redis、R2 和 Webhook。

```bash
python tools/verify_restored_database.py \
  --dsn-file /run/secrets/restored-readonly-dsn \
  --minimum-sources <expected-floor> \
  --minimum-observations <expected-floor> \
  --minimum-events <expected-floor> \
  --backup-reference <provider-reference> \
  --restored-instance-id <restore-id> \
  --output /var/lib/ai-hot-evidence/restore-verification.json
```

复核报告后，由 DBA 向生产库写入不可伪造的报告摘要：

```bash
python tools/record_dr_attestation.py \
  --dsn-file /run/secrets/postgres-admin-dsn \
  --report /var/lib/ai-hot-evidence/restore-verification.json \
  --performed-at <ISO-8601> \
  --backup-reference <provider-reference> \
  --restored-instance-id <restore-id> \
  --confirm-restored-instance <restore-id> \
  --measured-rpo-seconds <seconds> \
  --measured-rto-seconds <seconds> \
  --status passed \
  --operator-subject <dba-subject>
```

超过 RPO 3600 秒或 RTO 14400 秒的记录不能标为 passed。

## 8. 容量证据

当真实生产形态库达到 10,000 sources、5,000,000 observations、2,000 active events：

```bash
python tools/production_capacity_probe.py \
  --dsn-file /run/secrets/radar-capacity-reader-dsn \
  --base-url https://radar-api.example.com \
  --token-file /run/secrets/radar-owner-jwt \
  --requests 120 \
  --output /var/lib/ai-hot-evidence/capacity.json
```

该工具要求 production readiness、最近采集周期 ≤300 秒、雷达 P95 ≤500ms，并且不写数据库。

## 9. 72 小时采集

监控器必须与应用工作负载分离。创建 `ai-hot-monitor` 系统用户，安装仓库和虚拟环境，将 `.env.acceptance.example` 渲染为 `/etc/ai-hot/acceptance-monitor.env`，再安装：

```bash
install -m 0644 infra/systemd/ai-hot-acceptance-collect.service /etc/systemd/system/
install -m 0644 infra/systemd/ai-hot-acceptance-collect.timer /etc/systemd/system/
install -d -o ai-hot-monitor -g ai-hot-monitor -m 0700 /var/lib/ai-hot-acceptance
systemctl daemon-reload
systemctl enable --now ai-hot-acceptance-collect.timer
systemctl list-timers ai-hot-acceptance-collect.timer
```

独立 IdP client 每次换取新的 Owner JWT；静态五分钟 JWT 不得保存在 env 文件中。Timer 固定 UTC 每 15 分钟运行，重复执行同一时槽不会追加第二条样本。

72H 基础设施窗口将 `ACCEPTANCE_MANUAL_PREREGISTRATION` 与 `ACCEPTANCE_MANUAL_SNAPSHOT` 留空；它们属于后续 7 天产品证据窗口，错误地指向不存在文件会让采样任务按设计失败。

观察：

```bash
journalctl -u ai-hot-acceptance-collect.service
tail -n 1 /var/lib/ai-hot-acceptance/soak72h.jsonl | jq .sampleHealthy
```

完整 72 小时需要从首尾跨满 72 小时，至少 289 个 15 分钟时点。之后执行：

```bash
python tools/acceptance_monitor.py report \
  --input /var/lib/ai-hot-acceptance/soak72h.jsonl \
  --mode soak72h \
  --output /var/lib/ai-hot-evidence/soak72h-report.json
```

策略、keyring、镜像、迁移、必需连接器权利或监控器 digest 在窗口内变化时，旧窗口作废并从零开始。

## 10. 当前不能自动完成的状态

在管理员资源和审批输入前，以下 fail-closed 状态是正确行为：

- connector rights 仍为 pending/blocked；
- acceptance public keyring 为空；
- release digest 仍是 placeholder；
- `/health/ready` 返回 503；
- 72 小时 formal report 不具备资格。

不得用示例值、录制数据或本地 MinIO 绕过这些门槛。
