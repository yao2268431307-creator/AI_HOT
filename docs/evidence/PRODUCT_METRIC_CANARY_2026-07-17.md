# rc2 产品 KPI 测量链 canary

> 历史证据说明：本文件记录的是 `rc2.1` 时代的一次本地 canary，只证明当时的 HTTP 契约与零样本保护。它已被 `product-metrics-2026-07-rc2.9` 的签名、运行时证明、完整排序/阈值跨越账本与人工预登记规则取代，不得作为当前正式 72H/7D 验收证据；当前策略必须从 2026-07-17 05:50（Asia/Shanghai）重新开始完整证据窗口。

日期：2026-07-17（Asia/Shanghai）
策略：`product-metrics-2026-07-rc2.1`
范围：本地 FastAPI + InMemoryRepository，仅验证 HTTP 契约和“证据不足”保护；不验证 PostgreSQL、RLS、Redis、R2、真实分析师样本或生产性能。

## 执行

启动独立 Uvicorn 进程于 `127.0.0.1:8021`，等待 `/health` 就绪后，连续请求 20 次：

```text
GET /api/v1/metrics/beta
```

## 结果

```json
{
  "requests": 20,
  "p95Ms": 1.27,
  "metricScope": "rc2_beta_product_metrics",
  "policyVersion": "product-metrics-2026-07-rc2.1",
  "evidenceStatus": "insufficient",
  "passesMeasuredGates": null,
  "strongAlertSample": 0,
  "triageSample": 0,
  "activeReviewSample": 0
}
```

## 判定

- HTTP canary：通过。
- 冻结策略版本：正确。
- 最低样本保护：通过；零样本没有被写成达标或失败。
- 性能：1.27ms 只代表本机内存 fixture，不得用于宣称 rc2 的 PostgreSQL/缓存 P95。
- 正式 Beta：仍为 NO-GO；必须继续收集冻结后的真实最低样本，并完成生产存储、身份、72H soak 与 7 天影子运行。

## 长期验收监控 canary

同一轮还通过 `tools/acceptance_monitor.py` 对本地 API 执行了一次 `collect` + `report --mode canary`：

```json
{
  "status": "canary_only",
  "acceptanceEligible": false,
  "passes": true,
  "hashPrefix": "sha256:a4fad384",
  "samples": 1,
  "spanHours": 0.0,
  "connectorRuns": 0,
  "pipelineSlaEvidenceStatus": "insufficient",
  "productMetricEvidenceStatus": "insufficient"
}
```

这证明监控器能读取 health、覆盖、连接器轮次、持久化重复率、采集到评分 SLA 和产品 KPI 六类只读接口，并且明确拒绝把单样本提升为 72H/7D 证据。正式验收应由外部调度器每 15 分钟执行一次 `collect`；达到完整时间窗后再运行 `report --mode soak72h` 或 `report --mode shadow7d`。
