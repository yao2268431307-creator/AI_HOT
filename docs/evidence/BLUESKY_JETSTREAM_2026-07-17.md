# Bluesky Jetstream 实验验证 · 2026-07-17

## 结论

Bluesky 已实现为 disabled-by-default 的实验候选发现连接器，不是正式 Beta 必需连接器，也不进入未复核的强结论路径。

- Jetstream 事件只用于发现；消息本身不是自认证证据。
- AppView 返回的 AT URI、CID、作者 DID 与 Jetstream 候选三项精确一致时，raw envelope 记录 `identityVerified=true`；这只是当前快照核验，不等于 update/delete 生命周期已可靠跟踪。
- 在 durable update/delete supersession、撤回后重评分和 72H soak 通过前，所有 Bluesky Observation 均为无指标 `unverified_discovery`；处理器不创建 score run、不计独立/有效信源或 D/T/O/C，告警三证据门槛和 Webhook payload 也排除它。
- 每次未复核 envelope 使用不复用的 per-revision 对象键。已存在的稳定 Observation ID 忽略后续 discovery revision 时，新 raw ref 会进入持久删除队列；队列会 re-arm 已完成项，在 claim 和实际外部删除前都复核 observation/metric 引用。
- AppView 服务级 5xx、传输错误或无效 payload 使整轮失败并标记 degraded，不会把系统故障写成“单帖未复核”或推进 Jetstream checkpoint。

## 官方协议边界

核验使用以下 Bluesky 官方资料：

- Jetstream 说明：https://docs.bsky.app/blog/jetstream
- 当前 `bluesky-social/jetstream` 仓库：https://github.com/bluesky-social/jetstream
- 原客户端协议实现 `jetstream-legacy`：https://github.com/bluesky-social/jetstream-legacy
- AppView `app.bsky.feed.getPosts` lexicon：https://raw.githubusercontent.com/bluesky-social/atproto/main/lexicons/app/bsky/feed/getPosts.json

官方边界说明了两点：原 Jetstream 事件缺少 Firehose 的签名和 Merkle 证明，不适合直接回答高完整性的“谁说了什么”；截至本次核验，原客户端实现已经迁到 legacy 仓库，当前同名仓库是仍在演进的新 archive/server，1.0 前允许破坏性格式变化。因此连接器 Registry 明示 `experimental`、`disabled-by-default`、`blocked-on-rights-and-72h-soak`。

## 可复跑确定性用例

```powershell
.\.venv\Scripts\python.exe -m pytest services\api\tests\test_pipeline.py services\api\tests\test_operational_rules.py -q
ruff check services\api\radar services\api\tests tools
```

覆盖场景：

1. WSS URL、DNS、消息大小、消息数、空闲超时、cursor 版本与未来时间 fail closed。
2. cursor 使用 Unix 微秒并保留有限 replay overlap；无关事件仍推进高水位。
3. 精确 AppView 快照核验在 raw envelope 记录 `identityVerified=true`，但 Observation 仍为无指标 `unverified_discovery`。
4. AppView 正常缺帖/CID 不匹配保留 discovery-only 候选；AppView 5xx/无效 payload 使整轮 degraded 且 checkpoint 不动。
5. 超过 25 个候选时在 AppView batch 边界停止读取，checkpoint 不越过未处理候选；高密 replay overlap 可回退到精确 cursor，失败 attempt 不泄漏高水位。
6. PostgreSQL 通用 provenance-upgrade 回归证明原子覆盖已复核正文/指纹、指标围栏、有效信源去重和 raw deletion 排队；该回归不宣称当前 Bluesky 已走升级路径。
7. 网页明确标记“未复核候选，不参与评分”。

## 实时网络 smoke

本轮先用官方 US-East Jetstream WSS 地址做一次最小只读探测，成功收到一条 609-byte commit JSON，证明当时网络路径和基础 envelope 可读。随后运行完整有界适配器 smoke 时，US-East 连接被对端/网络重置（Windows `WinError 64`），US-East/US-West 其他公开实例出现 opening-handshake timeout；连接器按设计重试后失败，没有生成 Observation 或伪造 PASS。

可复跑命令：

```powershell
.\.venv\Scripts\python.exe tools\live_connector_smoke.py `
  --connectors bluesky `
  --bluesky-max-messages 500 `
  --bluesky-idle-timeout-seconds 5
```

这组结果同时证明“偶尔能连通”不能替代稳定性验收。当前实时端点状态仍是实验性阻断，不得把最小成功探测写成 72H soak 通过。正式晋级至少需要数据权利审批、协议格式锁定/漂移告警、目标区域连续 72 小时运行、重连/游标缺口核对和覆盖成本报告。
