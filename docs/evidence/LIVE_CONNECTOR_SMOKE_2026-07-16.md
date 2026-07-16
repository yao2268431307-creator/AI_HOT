# 真实公共连接器 smoke · 2026-07-16

## 目的与边界

本次检查使用项目真实解析器读取公开元数据，只验证一次有界请求的网络、响应结构与 Observation 映射。它不写生产数据库或原始证据存储，不证明数据商用权、生产凭证、配额安全、持续可用性，也不能替代 72 小时 soak。

可复跑命令：

```powershell
.\.venv\Scripts\python.exe tools\live_connector_smoke.py
```

## 首轮结果

| 连接器 | 结果 | 观测数 | 请求数 | 延迟 | 备注 |
|---|---:|---:|---:|---:|---|
| Hacker News | PASS | 3 | 4 | 1,588ms | 官方 Firebase API |
| GitHub | PASS | 50 | 1 | 1,941ms | 未配置 Token 的公开 Search API |
| Hugging Face | PASS | 50 | 1 | 806ms | Hub 模型元数据 |
| arXiv | PASS | 50 | 1 | 1,264ms | Atom API |
| OpenAlex | FAIL | 0 | 1 | 1,657ms | 首位作者 `id=null` 触发 `.rsplit()` 空值缺陷 |

首轮失败不是网络中断，而是真实数据暴露出的解析缺陷。修订 `research.py` 使空作者或空作者 ID 映射为 `openalex:unknown`，并新增空作者与异常未来日期回归用例。

## 修复后复测

OpenAlex 空作者修复后单独复测：PASS，50 条观测、1 个请求、1,860ms。随后全量 smoke 发现一条上游记录的 `publication_date=2050-01-01`；为避免制造未来时间线，超过采集时间一天的研究日期现在回落到 `collectedAt`。本次 smoke 不归档响应；在配置了 evidence store 的生产采集路径中，原始 item 才会保留供审计。

最终全量复测（`2026-07-16T15:43:52Z`）：

| 连接器 | 结果 | 观测数 | 请求数 | 延迟 |
|---|---:|---:|---:|---:|
| Hacker News | PASS | 3 | 4 | 1,973ms |
| GitHub | PASS | 50 | 1 | 1,944ms |
| Hugging Face | PASS | 50 | 1 | 828ms |
| arXiv | PASS | 50 | 1 | 1,195ms |
| OpenAlex | PASS | 50 | 1 | 1,616ms |

OpenAlex 样例的有效发布时间为本次首次采集时间，不再保留 2050 年的异常排序值。加入 smoke 边界与空结果用例后，完整自动化回归同步为 `111 passed`。

## 当前结论

这份证据将连接器状态从“只有 MockTransport”推进到“一次真实响应已验证”，但 `config/connector_registry.json` 中的 `pending-72h-soak` 保持不变。任何连接器在真实部署前仍需权利复核、成本配置、迟到数据核对、故障恢复和连续 72 小时运行记录。

为防止 smoke 自身制造非预期负载，`--hn-max-items` 被强制限制在 `1..10`；任一连接器返回零 Observation 时记为失败，而不是把空映射误报为 PASS。
