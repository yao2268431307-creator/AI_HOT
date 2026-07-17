# 生产配置自动化本地验证记录

日期：2026-07-17

范围：本记录只证明仓库内生产配置工具、契约和确定性回归，不证明真实授权、云资源、正式镜像、容量、灾备或 72 小时运行已经完成。

冻结边界：产品策略 `product-metrics-2026-07-rc3.3`，验收器 `acceptance-monitor-v1.4`，迁移证明 `001_init_rc3.1`。验收器拒绝所有认证请求重定向，当前策略摘要与文件 SHA-256 一致。

## 已验证

- `production_preflight.py`：bootstrap 成功路径、formal 权利/keyring 阻断、相对路径、TLS、角色隔离、预算与正式六源门禁。
- `generate_acceptance_keyring.py`：五套独立 Ed25519 身份、公开 keyring 可加载、产品策略摘要绑定、成对文件失败回滚。
- `render_verified_release.py`：manifest checksum、批准仓库/commit、固定 GHCR 仓库、Cosign manifest/镜像验签命令和 release env 渲染。
- `verify_restored_database.py` / `record_dr_attestation.py`：只读恢复检查、报告摘要、备份/恢复实例绑定和 RPO/RTO 防伪规则。
- `run_scheduled_acceptance.py`：UTC 15 分钟时槽、重复时槽幂等、短时 OAuth2 token、空白 7D 附件不阻塞 72H。
- 生产 Compose 可成功解析；API/Scheduler/Alert 三份 Secret 边界、RSS/身份只读挂载和不可变镜像引用均存在。
- 发布 workflow YAML 可解析；九个第三方 Action 提交 SHA 均通过 GitHub API 验证为对应上游仓库的真实提交。

## 回归结果

```text
Python:                  227 passed, 20 skipped in 23.86s
生产工具测试:            13 passed in 0.45s
Ruff:                    passed
Python compileall:       passed
pip check:               no broken requirements
Web lint:                passed
Web rendered HTML tests: 2 passed
Vinext production build: passed
npm audit --omit=dev:    0 vulnerabilities
Production Compose:      parsed
本地真实 PG readiness:   1 passed
Workflow YAML:           parsed
git diff --check:        passed
```

Vinext 仍输出已知的动态路由分类提示，Node 仍输出上游 `punycode` 弃用提示；均未造成构建或测试失败。

## 未冒充完成的部分

- Docker Hub 在本机网络被对端重置，因此没有把一次本地容器拉取写成正式镜像证明；正式镜像必须由目标 GitHub Actions 使用管理员审核的 base digest 构建、签名并在部署机复验。
- 当前权利 Registry 仍为 pending/blocked，acceptance keyring 仍为空，属于正确的 fail-closed 状态。
- 没有生产 PostgreSQL/Redis/R2/IdP Secret，因此没有生成伪造的 formal preflight、PITR、容量或 72H 通过结果。
