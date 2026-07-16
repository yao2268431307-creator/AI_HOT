# Outbox / Redis at-least-once 与恢复验证（2026-07-17）

## 结论

本地 PostgreSQL 与真实 Redis 已闭环以下消息一致性场景：

- 两个 publisher 并发处理十条正常 Outbox 时，`FOR UPDATE SKIP LOCKED` 使每条只发布和标记一次。
- XADD 成功、PostgreSQL 发布标记失败时，行事务回滚，错误在新事务中记录，Outbox 保持可重试；再次发布形成两个相同稳定 `outbox_id` 的消息，符合 at-least-once 语义。
- 生产 `RedisAlertWorker` 消费重复 score 消息时，业务幂等约束只形成一条 delivered 告警事实。
- 告警 reservation 已写入但 durable confirm 暂时失败时，Redis 消息保持 pending；投递身份绑定稳定 `outbox_id`。in-app 在事件升级/删除后仍恢复为 delivered；Webhook 无事件/规则快照或重试耗尽时写入 `aborted + terminalReason`，不宣称已送达，也不永久留下 reserved。
- poison message 首次保留 pending，达到重领上限后写入包含原 Stream ID、`outbox_id`、聚合 ID 和错误的 DLQ，再 ACK 原消息。
- `XAUTOCLAIM` 保存并推进 Redis 返回的游标；前部 poison 消息仍 pending 时，后部消息仍能进入后续处理批次。
- 删除整个 Redis Stream 后，可按冻结时间窗从 PostgreSQL 已发布 Outbox 选择性重建 `score.created` 与 `source.erased`，且不改写历史发布事实。
- 恢复命令使用互斥锁、批内心跳、原子检查点、稳定 PostgreSQL/Redis 双游标和正常处理器租约；锁被替换时检查点不会前移。续跑不仅检查末条，而是以磁盘临时账本精确去重 Redis 前缀，再由后台线程流式对账 PostgreSQL 期望前缀的完整 `outbox_id` 顺序与累计数；只保留最后检查点行、前缀多余或乱序都会拒绝。已完成恢复的 Stream 再次丢失时会清除 completed cursor 并从完整窗口重建。

这些结果证明本地单机 at-least-once 链路和空 Stream 重建机制，不证明跨系统 exactly-once、Redis Cluster 故障转移、生产网络分区或目标规模恢复时长。

## 关键实现

- `services/api/radar/outbox.py`
  - 每条待发布行使用独立 PostgreSQL 事务；XADD 后 SQL 标记失败会回滚，再用新事务记录可重试错误。
  - 正常成功清除旧 `last_error` 并增加 `attempts`。
  - `replay_batch` 只读取已发布、位于冻结时间窗且消费者实际处理的事件类型，不更新 `published_at`。
  - publisher 与 consumer 通过同一 Redis participant lease 注册；恢复锁或 `running` 状态存在时拒绝加入。
- `services/api/radar/alert_worker.py`
  - consumer participant lease 每 30 秒续租；lease 丢失时消息保持 pending，不进入正常 ACK/DLQ 分支。
  - `XAUTOCLAIM` 保存 `next_start_id`，并支持可配置的 `max_deliveries` 与 `claim_min_idle_ms`。
  - `delivered` 和 `aborted` 都是幂等终态；后者通过 `/api/v1/alerts` 暴露 terminal reason。worker 将稳定 `outbox_id`（旧消息 fallback 为 cycle/inputDigest）传作 delivery key，正常处理与 DLQ 终态复用同一身份解析。
  - `XAUTOCLAIM` 返回的 deleted pending IDs 会写 tombstone DLQ 并抛出运行错误；publisher 关闭 `MAXLEN`/时间裁剪，避免自身删除 PEL。
- `tools/replay_outbox_to_redis.py`
  - 默认 dry-run；执行要求 `--confirm-stream` 精确匹配，且 `until` 不得晚于 PostgreSQL 时钟。
  - 获取恢复锁与检查正常 participant lease 在同一个 Redis Lua 原子操作内完成。
  - 300 秒恢复锁由 30 秒后台心跳续租；“校验 lock token、写检查点、续租”由单个 Lua 脚本原子完成。
  - 检查点保存 `(created_at,id)` 和最后一条 Redis Stream ID；续跑前先验证末条，再把截止该 Stream ID 的 Redis `outbox_id` 按首次出现顺序写入自动清理的本地 SQLite 临时账本，由后台线程使用同步 psycopg server cursor 分批对账 PostgreSQL 期望前缀。这样兼容 crash duplicate，又不会把目标规模 ID 集合装入内存或阻塞恢复锁心跳。PG 连接限时 10 秒、前缀扫描限时 240 秒；取消会等待后台线程关闭 SQLite 后重抛原取消异常，不用 `WinError 32` 覆盖主错误。running 检查点对应的 Stream 消失、前缀缺失/多余/乱序、累计数错误、检查点行缺失或内容不匹配时，均要求人工清理状态并从完整窗口重启；completed 检查点对应的 Stream 再次丢失时，工具在持锁条件下原子清空旧 cursor 并自动全量重建。
  - 执行中 XADD 与检查点之间中断仍可能重复重放，依赖稳定 `outbox_id` 和业务幂等约束收敛。

## 可复跑结果

```text
Default deterministic suite: 150 passed, 10 skipped in 14.46s
Explicit PostgreSQL/Redis/MinIO suite: 160 passed, 7 warnings in 16.60s
Focused infrastructure suite: 10 passed, 7 warnings in 2.15s
Ruff: passed
```

7 条 warning 来自 botocore 内部 `datetime.utcnow()` 弃用提示，与 Redis/Outbox 断言无关。

实际 dry-run 的关键字段（时间窗与数据库时钟字段省略）：

```json
{
  "mode": "dry-run",
  "stream": "radar:events",
  "kinds": ["score.created", "source.erased"],
  "candidateCount": 0,
  "scoreEvents": 0,
  "sourceDeletions": 0
}
```

候选为 0 是因为集成用例按唯一前缀清理了临时 Outbox；这只证明 dry-run 可连接并保持只读。实际重放、双游标续跑、锁替换、两条 PG 前缀只剩末条 Stream 时的完整性拒绝、活跃处理器互斥、完成窗口与无检查点 Stream 拒绝行为均由显式容器用例使用临时发布事实验证。

## 仍保留的生产门禁

- 在 XADD 与 SQL 标记之间实际终止进程，而不只是确定性故障注入。
- PostgreSQL/Redis 网络分区、Redis Cluster 主从切换和消费者跨实例争用；participant lease 有心跳但不是跨系统 fencing token。
- 基于所有 consumer group 安全水位的生产归档/裁剪与 Redis 容量上限；当前为避免 PEL 数据损失而不自动裁剪。
- 目标数据量下的重放积压、临时磁盘空间、吞吐和恢复耗时；当前结果不能作为 RTO。
- `source.erased` 在真实 R2 上重复执行、部分删除失败和跨区域恢复。
- PostgreSQL WAL/PITR、备份恢复和 RPO ≤ 1 小时、RTO ≤ 4 小时。

因此该增量上调的是本地消息链与 Redis 空 Stream 重建证据；正式 Beta 仍维持 NO-GO。
