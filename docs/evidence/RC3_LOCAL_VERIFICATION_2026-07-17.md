# RC3 本地验证记录（2026-07-17）

## 验证对象

- 评分：`score-0.7.0`
- 阈值：`thresholds-2026-07-rc3`
- 迁移：`001_init_rc3.0`
- 产品指标策略：`product-metrics-2026-07-rc3.2`
- 权利策略：`rights-2026-07-rc3.0`

## 执行结果

| 检查 | 结果 |
|---|---:|
| 默认 Python 全套 | `213 passed, 20 skipped in 23.42s` |
| 显式 PostgreSQL/Redis/MinIO 全套 | `231 passed, 2 skipped, 8 warnings in 27.27s` |
| Ruff | 通过 |
| Python compileall | 通过 |
| pip check | 无损坏依赖 |
| Web lint | 通过 |
| Vinext production build | 通过 |
| Playwright + Axe / Chromium | `2 passed` |
| `npm audit --omit=dev` | `0 vulnerabilities` |
| Production Compose config | 使用占位 sha256 最终镜像摘要解析通过，且不存在 `build` 回退 |
| 全新 PostgreSQL 容器启动 | `000_local_roles.sql` + `001_init.sql` 从零通过，marker 为 `001_init_rc3.0` |
| `git diff --check` | 通过 |

显式基础设施套件对本地真实 PostgreSQL 应用了 rc3 迁移，并覆盖：

- 迁移重复应用的幂等性；
- 受限应用角色、RLS、审计 trigger 与只读 migration marker；
- 同周期评分 revision 1/2 和完整历史回放；
- `availableAt`/`availabilityBasis`；
- 1024 维 pgvector 事件向量写入、HNSW 近邻查询、标题哈希校验与有界候选缓存；
- 事件版本 CAS 拒绝陈旧写入，评分历史对应用角色只增不改，受限来源删除函数留审计；
- 总额/连接器/信号家族预算在请求前原子预留，并发事务不能共同越界；过期租约保持占额并进入管理员显式对账；
- 隔离删除角色、来源范围推导、不可伪造的评分历史清除审计与不可见事件墓碑；
- 每个采集周期重新执行 Redis 与 R2 写/读/删探针，Alert 每轮以独立身份执行 Redis 与 R2 删除权限探针；任一旧探针或待对账预算都使 readiness 失败；
- 雷达优先级上下文批量查询；
- Transactional Outbox、Redis Streams、消费/DLQ、恢复与安全裁剪；
- MinIO 原始证据写入、读取与删除。

真实浏览器套件使用 Chromium 验证桌面与移动路径、Tab/Escape、焦点约束和恢复、背景不可交互、reduced motion、对比度、图表等价表格和预算卡兼容回退。

## 警告说明

基础设施套件的 8 条 warning 均来自 botocore 内部 `datetime.utcnow()` 弃用提示。Vinext 构建还输出路由分类提示，Node 输出 `punycode` 弃用提示；均未导致失败，但应在依赖升级周期复核。

## 不可外推的边界

本记录不是以下项目的证明：

- 生产数据权利或平台商用授权；
- 正式镜像已经构建、签名或部署；
- 目标托管 PostgreSQL/Redis/R2 的连通性和故障转移；
- PITR、RPO/RTO、跨节点恢复、网络分区或硬终止；
- 10,000 信源、500 万观测、2,000 活跃事件的真实容量；
- 72 小时连续采集、7 天影子运行或真实产品 KPI；
- 聚类准确率、宏 F1、Precision@5 或发现提前量。

因此本记录只支持“rc3 本地代码候选”，不能支持“正式私有 Beta 已验收”。
