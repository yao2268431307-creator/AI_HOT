# Redis Stream 安全保留验证

日期：2026-07-17
范围：本地 Docker PostgreSQL 16/pgvector + Redis 7；不代表目标生产集群验收。

## 已实现的安全协议

1. 维护程序与恢复程序复用同一 Redis 互斥锁；进入前原子清理过期参与者租约并拒绝活跃 publisher/consumer、其他维护锁和 `running` 恢复状态。维护期间新参与者同样 fail closed，锁由心跳续租并用 token 所有权释放。
2. 必须存在至少一个 consumer group，且配置的必需组不得缺失。工具读取所有现存组：有 PEL 时取最早 pending ID，否则取 last-delivered ID；所有组的最早值形成消费者边界。
3. 保留期边界使用 Redis `TIME`，不信任操作机时钟。最终 `safeTrimMinId` 是保留期边界和消费者边界的较早者；`XTRIM MINID` 只删除严格小于该 ID 的消息，因此边界消息仍保留。
4. `STREAM_OUTBOX_KINDS` 冻结 observation、metric snapshot、score、feedback、cluster edit、rescore 与 source deletion 七类消息，常规 publisher、恢复和裁剪共用；未登记 kind 不得进入 Stream。dry-run 用磁盘 SQLite 精确登记所有候选行和去重后的 UUID `outbox_id`，分批查询 PostgreSQL，要求每个 ID 属于该集合且 `published_at IS NOT NULL`。无效 ID、错误 kind 或缺失发布事实均阻断 execute。
5. execute 必须提交 dry-run `safeTrimMinId` 作为获批上限，并重新计算 `calculatedSafeTrimMinId`。当前安全边界小于确认值时拒绝；边界因时间或消费进度前移时，仍以确认值建账和裁剪，删除范围不会超过已审阅计划。验证线程用 PostgreSQL `FOR SHARE` 持有所有候选行直到 Redis 操作结束，阻断并发清理或事实改写；取消期间也等待线程返回并释放连接。
6. 裁剪前再次读取所有组并确认维护租约仍有效；最终 Redis Lua 在一个原子操作内重新读取完整组集合、last-delivered、pending 数和最早 pending，任一变化都拒绝，然后执行精确而非近似的 `XTRIM MINID`。若安全裁剪后仍超过容量，只报告 `over_limit`，不强制越过消费者水位。

## 真实集成断言

`I-13` 在真实 PostgreSQL 与 Redis 上创建 5 条已发布 Outbox/Stream 消息和两个不同进度的 consumer group：

- 组 A 已读 5 条、只 ACK 第 1 条，第 2 条成为全局最早 PEL；组 B 读并 ACK 前 4 条，因此裁剪水位确实由 PEL 而非慢组 last-delivered 控制。
- 活跃 publisher 参与者租约存在时，维护程序拒绝启动。
- dry-run 把第 2 条消息判为安全边界，只列第 1 条为候选；候选 Outbox 覆盖完整。
- dry-run 后新增 `0-0` consumer group 时，原子裁剪因组集合变化拒绝且 Stream 长度不变。
- execute 不提供 dry-run 边界确认时拒绝；提供精确确认后删除 1 条，组 A 的 4 条 pending 保留，组 B 随后仍能读取第 5 条未见消息。
- 独立时间边界用例混合 2 小时前和当前消息，证明保留期而非 consumer 水位主导时，延迟 20ms 的 execute 会按 dry-run 旧边界成功裁剪；当前计算边界只前移，不会造成确认值永远失效。
- 裁剪持有 PG 行锁时，另一连接的候选 Outbox 删除因 lock timeout 被阻断。
- 另一条 Stream 使用“存在且已发布、但未登记”的 kind；dry-run 报告不可恢复，execute 拒绝且 Stream 长度保持不变。常规 publisher 对未登记 kind 同样记录失败且不写 Stream。
- 独立取消用例证明：若任务在后台 PG 获取期间取消，线程返回后 guard 会被释放；若取消发生在 Redis Lua 裁剪期间，调用方会等待原子命令完成/失败后才释放 PG guard，不提前断开命令并制造行锁窗口。

## 复跑结果

```text
Focused operational rules: 33 passed
Focused PostgreSQL/Redis integration: 7 passed
Default Python suite: 153 passed, 11 skipped
Explicit local infrastructure suite: 164 passed, 7 botocore deprecation warnings
Ruff: passed
Python compileall: passed
pip check: passed
docker compose config: passed
git diff --check: passed
Web ESLint / Vinext build / 2 rendered-product tests: passed
Web production dependency audit: 0 vulnerabilities
```

## 未覆盖与上线闸门

- 参与者互斥依赖应用 publisher/consumer 遵守租约；PG 行锁与 Redis 原子脚本关闭了本次候选删除和组状态 TOCTOU，但不等于发布链 XADD/PG 标记窗口的完整跨系统 fencing，也不能约束绕过数据库/Redis 协议的基础设施级操作。执行窗口仍应暂停 Outbox 清理任务作为运维双保险。
- 尚未在 Redis Cluster、主从故障转移、网络分区和进程硬终止窗口中验证。
- 尚未用目标消息量测量 SQLite 临时空间、Outbox 查询负载、精确裁剪时长与积压恢复时间；生产容量和告警阈值仍需压测。
- CLI 对 Redis 连接和单条命令设置 10 秒连接、30 秒 socket timeout；目标容量必须证明原子组复核与精确 `XTRIM` 在该时限内完成，不能靠放宽超时替代容量治理。
- PostgreSQL Outbox 的实际保留期必须覆盖 Redis 可恢复窗口；PITR、备份恢复和目标 RPO/RTO 仍是独立闸门。

独立审计先后复现并推动关闭组状态 TOCTOU、混合 kind 不可执行、时间边界确认失效、PG 清理竞态和取消/异常保真问题；最终只读复核结论为 P0、P1、未披露代码 P2 均无。

因此本记录只支持“安全保留逻辑在本地真实 PG/Redis 上通过”的结论，不支持“生产容量、集群故障切换或正式 Beta 已通过”。
