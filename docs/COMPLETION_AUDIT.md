# AI 热点雷达 V1 完成度审计（rc3 候选）

审计日期：2026-07-17

审计对象：当前工作区 rc3 未发布候选

结论口径：代码完成度与正式发布资格分开判断

## 结论

- **本地代码候选：独立终审通过。** 当前自动化、真实本地基础设施集成、生产构建和真实浏览器回归均通过；仓库内代码候选范围未发现未关闭的 P0、P1 或 P2。
- **正式私有 Beta：NO-GO。** 数据权利、目标环境、灾备、目标规模、72 小时 soak、7 天影子运行和真实标注指标尚无合格证据。

追加说明：原独立终审覆盖 rc3.0 基础候选；其后新增的签名发布、formal preflight、管理员配置、短时 R2 凭据、迁移 `001_init_rc3.1`、灾备/容量工具与 72H systemd 调度已经完成本地确定性和真实 PostgreSQL readiness 回归，但尚未经过第二位同事的新一轮独立复核。对应证据见 [生产配置自动化本地验证记录](evidence/PRODUCTION_CONFIGURATION_LOCAL_VERIFICATION_2026-07-17.md)。

## 独立终审结论

2026-07-17 的最终聚焦复核确认 `P0=0、P1=0、P2=0`（范围仅限当前仓库代码候选）。终审重点复核了生产服务凭据隔离、数据库删除角色、每轮依赖实探、预算崩溃对账、有界向量候选，以及 Alert 独立 R2 删除身份的证明链。最后一项采用专用随机不存在 key 的真实 `DeleteObject` 探针，不要求给 Alert 身份增加读写权限；探针结果和时间写入独立心跳，readiness 同时验证采集身份与 Alert 身份的能力及新鲜度。

该结论不能替代下文列出的数据授权、正式镜像、目标部署、容量、灾备和长期运行证据。

## 需求覆盖矩阵

| 需求域 | 当前状态 | 仓库内证据 | 正式门槛 |
|---|---|---|---|
| 待研判队列、雷达、详情、信源和覆盖页 | 本地已证实 | API 契约、Vinext build、Playwright/Axe Chromium 回归 | 真实分析师与目标身份环境验收 |
| 生命周期 + 传播结构标签 | 本地已证实 | 类型专用评分、滞回、连续周期、时间驱动测试 | 真实时间点标注与宏 F1 ≥ 0.70 |
| 可解释、可复现评分 | 本地已证实 | append-only score revision、完整输入/版本摘要、回放接口、真实 PG 回归 | 生产数据窗口抽查与版本回滚演练 |
| 中英文聚类 | 代码路径已实现 | BGE-M3 接口、持久向量、URL/实体/时间回退、编辑谱系 | 200 条真实抽检准确率 ≥ 85% |
| 数据采集与证据归档 | 部分完成 | P0/P1 连接器、checkpoint、去重、MetricSnapshot、R2/MinIO | 授权数据源 72H 连续运行，重复率 < 5% |
| 数据权利 | 代码围栏已实现 | rc3 字段级策略、Feature Registry、网络/处理 fail-closed | 权利负责人签批和生产凭证 |
| 预算与成本 | 本地已证实 | 总额/连接器/信号家族台账、最坏成本预留、未知成本拒绝 | 真实合约单价与月度告警演练 |
| 告警闭环 | 本地已证实 | 三证据门槛、冷却、日预算、签名 Webhook、持久状态 | 目标接收方、15 分钟 SLA 和 7 天影子运行 |
| 权限与身份 | 本地已证实 | RBAC、EdDSA/RS256 JWT、Owner-only 运维接口 | 组织 IdP 联邦、密钥轮换和渗透/权限复核 |
| PostgreSQL/Redis/R2 | 本地真实集成已证实 | RLS、trigger、migration marker、Outbox、Stream、MinIO、重放/裁剪 | 目标托管服务、故障转移、PITR/RPO/RTO |
| 可观测性与 readiness | 本地已证实 | Prometheus 指标、结构化日志、组件心跳、503 readiness | 接入生产监控和事故演练 |
| 10,000 信源容量 | 工具已准备，未证明 | 只读 production capacity probe | 真实 10k/5m/2k 目标规模运行并达时限 |
| 产品 KPI 与人工评估 | 计算器已实现，证据不足 | rc3 冻结策略、签名监控、最低样本 fail-closed | 真实最低样本、双人复核、置信区间、7 天窗口 |

## rc3 关键修订核对

1. `score-0.7.0` / `thresholds-2026-07-rc3` 已冻结，评分持久记录包含所有复现所需版本、摘要、观察 ID 和完整回放载荷。
2. 数据库迁移为 `001_init_rc3.1`；真实 PostgreSQL 已验证迁移幂等、同周期 revision 1/2、回放、`availableAt` 与 1024 维 pgvector。
3. 雷达优先级上下文改为批量查询并限制响应页大小，消除 2,000 活跃事件时明显的 N+1 读取风险；目标 P95 仍必须实测。
4. 权利矩阵对原文、摘录、指标和提供方核验执行硬约束，未进入 Feature Registry 的指标不参与处理。
5. 预算在发起请求前以 PostgreSQL advisory lock 原子预留最坏成本，总额、连接器和信号家族相互隔离；并发 Worker 不能共同越过任一额度。
6. 生产 readiness 要求 JWT、采集身份的 Redis 与目标 R2 bucket 读写删除探测、Alert 独立身份的 Redis 与 R2 删除权限探测、五个组件心跳、DR 证明、RLS/trigger/marker 和实际运行 API/Web 镜像摘要与发布清单精确一致；两类依赖探针均按轮次执行并受 30 分钟新鲜度门禁。
7. 网页覆盖键盘、移动模态、焦点恢复、reduced motion、对比度和图表数据表，并已用真实 Chromium + Axe 回归。

## 验证结果

```text
Python default:                         213 passed, 20 skipped in 23.42s
Python + PostgreSQL/Redis/MinIO:        231 passed, 2 skipped, 8 warnings in 27.27s
Ruff:                                   passed
Python compileall:                      passed
pip check:                              no broken requirements
Web lint:                               passed
Vinext production build:                passed
Playwright + Axe on Chromium:           2 passed
npm audit --omit=dev:                   0 vulnerabilities
Production Compose config:              passed
Fresh PostgreSQL bootstrap + migration: passed
git diff --check:                       passed
```

这些结果只证明当前代码和本地真实依赖路径，不证明目标生产网络、平台配额、外部授权、长时间稳定性或生产容量。

## 正式 NO-GO 项

- 未获得所有正式数据源的权利批准和生产凭证。
- 未完成目标环境镜像构建、摘要登记、身份联邦和密钥轮换。
- 未完成 PITR、1 小时 RPO、4 小时 RTO、跨节点故障转移与硬终止恢复。
- 未完成真实 10k 信源 / 5m 观测 / 2k 活跃事件容量测试。
- 未完成 72 小时采集 soak 和 7 天影子运行。
- 未完成双人真实标注、聚类准确率、宏 F1、Precision@5 与提前量证据。
- 未达到产品 KPI 的真实最低样本；合成数据和录制演示固定不得进入分母。

## 发布建议

独立代码终审门槛已经通过；只有上方正式 NO-GO 项也逐项取得签名证据后，才可把状态从“本地代码候选”提升为“可信私有技术 Beta”。
