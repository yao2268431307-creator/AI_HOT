# 本地基础设施集成验证记录（2026-07-17）

## 结论

本仓库的本地 Docker 基础设施已经完成真实运行态验证，不再只是配置解析或 Mock：

- PostgreSQL/pgvector 实际应用迁移 `001_init_rc2.4`；受限 `radar_app` 角色通过 RLS、审计 trigger、只读迁移标记和时钟证明。
- 两个并发 PostgreSQL 事务写入同一 `source_id + content_fingerprint` 时保留两条审计 Observation，但有效信源观测只增加一次。
- PostgreSQL Outbox 实际发布到 Redis Stream，随后在数据库事务中记录 `published_at`、`attempts=1`，payload 可从流中还原；交付语义是 at-least-once，不宣称跨系统 exactly-once。
- Redis consumer group 的读取、确认和 pending 清零通过。
- MinIO 的 S3 兼容 put/read/delete 与 Content-Type 校验通过。
- PostgreSQL、Redis、MinIO 同时执行 `docker compose restart` 后，测试事实、Stream 消息和对象均保持可读。

这份记录只证明 2026-07-17 的本地、单机、全新 Docker volume 集成链路。它不证明目标生产环境容量、跨节点故障、PITR、RPO/RTO、真实 R2、72 小时 soak 或 7 天影子运行。

## 环境

| 项目 | 实际值 |
|---|---|
| Docker Server | 29.4.3 |
| Docker Compose | v5.1.3 |
| PostgreSQL 镜像 | `pgvector/pgvector:pg16`，健康检查通过 |
| Redis 镜像 | `redis:7.4-alpine`，健康检查通过 |
| 对象存储镜像 | `minio/minio:RELEASE.2025-04-22T22-12-26Z` |
| 数据库应用角色 | `radar_app`，非 superuser，`BYPASSRLS=false` |
| 迁移标记 | `001_init_rc2.4` |

## 可复跑命令

连接字符串与本地对象存储凭证来自 `docker-compose.yml`。测试必须显式设置以下环境变量，默认测试运行不会访问本地基础设施：

```powershell
docker compose up -d
$env:DOCKER_INTEGRATION_CONTEXT="<local Docker context; e.g. desktop-linux>"
$env:POSTGRES_INTEGRATION_DSN="<restricted radar_app DSN>"
$env:POSTGRES_INTEGRATION_ADMIN_DSN="<local integration admin DSN>"
$env:REDIS_INTEGRATION_URL="redis://127.0.0.1:6379/15"
$env:S3_INTEGRATION_ENDPOINT="http://127.0.0.1:9000"
$env:S3_INTEGRATION_ACCESS_KEY="<local integration access key>"
$env:S3_INTEGRATION_SECRET_KEY="<local integration secret>"
.\.venv\Scripts\python.exe -m pytest services\api\tests -q
.\.venv\Scripts\python.exe tools\infrastructure_recovery_smoke.py
```

## 运行结果

带全部集成环境变量运行完整 API 套件：

```text
152 passed, 7 warnings in 12.62s
```

其中新增的 6 个真实基础设施用例单独运行结果为：

```text
6 passed, 7 warnings in 1.24s
```

7 条 warning 均来自已安装 botocore 内部对 `datetime.utcnow()` 的弃用提示；不影响断言或对象存储结果，后续依赖升级时消除。

运行时证明的关键结果：

```json
{
  "storageBackend": "postgresql",
  "rlsVerified": true,
  "migrationVersion": "001_init_rc2.4",
  "auditTriggersVerified": true,
  "migrationMarkerReadOnly": true,
  "databaseUser": "radar_app",
  "databaseRoleSuperuser": false,
  "databaseRoleBypassRls": false,
  "productionReady": true
}
```

服务协调重启 smoke：

```json
{
  "postgresPersisted": true,
  "redisStreamPersisted": true,
  "s3ObjectPersisted": true,
  "recoverySecondsUpperBound": 2.203,
  "passed": true
}
```

`recoverySecondsUpperBound` 从发起 `docker compose restart` 前开始计时，到三项持久事实首次全部验证成功为止；它是单次本地观察值，不是 RTO 承诺。脚本显式锁定仓库 compose 文件、`ai_hot` 项目名和经本地 endpoint 校验的 Docker context，不继承 `COMPOSE_FILE`、`DOCKER_HOST` 等覆盖变量；PostgreSQL、Redis 和 S3 endpoint 必须分别是 loopback 的 `5432/6379/9000`，远端或其他端口会在写入前 fail closed。完成后会删除本次唯一前缀的测试事实，不会删除 volume；任一清理步骤失败都会使脚本失败并报告对应后端，不会静默返回成功。

## 未被本验证覆盖的门禁

- 目标生产数据库的 500 万 Observation、2,000 活跃 Event 压测与 P95。
- WAL 归档、PITR、备份恢复、备份过期删除，以及 RPO ≤ 1 小时、RTO ≤ 4 小时。
- 网络分区、Redis 全量丢失后从 Outbox 重放、MinIO/R2 跨节点故障与部分删除恢复。
- 真实数据权利、生产身份联邦、外部告警渠道和密钥托管。
- 120–200 个授权真实源、72 小时 soak、7 天 shadow 和人工标注/产品 KPI 样本。

因此，本证据解除的是“本机 Docker 未运行、迁移和本地依赖从未实际应用”的旧风险；正式 Beta 仍保持 NO-GO。
