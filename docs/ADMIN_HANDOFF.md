# AI 热点雷达管理员交接单

更新时间：2026-07-17

本单只列出仓库无法自行取得的组织权限和真实资源。不要把密码、API Token、私钥或完整 DSN 发到聊天中；应写入组织 Secret Manager，或部署机 `/run/secrets` 下 root-only 文件。

## 1. GitHub 与发布

- 指定目标私有仓库：`<owner>/<repository>`，并授权当前登录账户创建/推送仓库（如仓库尚不存在）。
- 允许 GitHub Actions 写入 GHCR Packages，并保留 `id-token: write`。
- 审阅并登记两个不可变基础镜像：
  - `PYTHON_BASE_IMAGE=<repository>@sha256:<digest>`
  - `NODE_BASE_IMAGE=<repository>@sha256:<digest>`
- 只有 GitHub Enterprise Cloud 私有仓库才设置 `ENABLE_GITHUB_ATTESTATIONS=true`；其他私有仓库保持未设置，使用必需的 Cosign 镜像/manifest 签名链。

## 2. 正式域名和身份

- 提供 Web 与 API 内网域名及 TLS 证书/网关，例如 `radar.example.com`、`radar-api.example.com`。
- 创建生产 IdP client：只签发 EdDSA 或 RSA ≥2048 位的五分钟 JWT；claims 必须含 `sub`、`workspaceId`、`jti`、`signingKeyVersion`。
- 创建独立的 acceptance-monitor client-credentials client，其 subject 在目标 workspace 中登记为 Owner；不要生成长期 Owner JWT。
- 确认治理 workspace ID、首批 Owner/Analyst/Viewer subject。

## 3. 托管依赖

- PostgreSQL 16：数据库名 `ai_hot`，启用 pgvector、pgcrypto、连续 WAL/PITR，保留 7–14 天；提供管理员 DSN Secret 文件。
- Redis：提供 TLS `rediss://` 地址和生产凭据。
- R2/S3：创建私有 bucket；Scheduler 身份具备目标 bucket 读/写/删。Alert 使用另一套带 session token 的短时、限定 bucket 凭据；Cloudflare R2 当前只有 Object Read & Write / Object Read 权限，因此“只删除”由独立身份、短有效期和应用代码共同约束，不得把它标成供应商原生 delete-only 权限。
- 创建生产 Linux 主机/集群和 Secret Manager 注入；端口只在 loopback 暴露，由组织网关终止 TLS。

## 4. 数据权利与成本

- 对 RSS、Hacker News、GitHub、Hugging Face、arXiv、OpenAlex 六类完成正式用途审批。
- 每类登记审批工单、签字材料 SHA-256、带时区审批时间，以及互不相同的数据权利/安全/产品审批主体。
- RSS 对每个 Feed 单独完成发布方审批；不能用连接器级审批代替。
- 提供获批 GitHub Token、OpenAlex 运维邮箱，以及真实人民币/请求成本；只有合同确认零成本才填写 `0`。

## 5. 部署与验收主机

- 部署机安装 Docker Compose、Cosign、GitHub CLI、Python 3.12 和 systemd。
- 创建 `ai-hot-monitor` 系统用户，并允许安装仓库到 `/opt/ai-hot-radar`、配置到 `/etc/ai-hot`、证据写入 `/var/lib/ai-hot-evidence` 与 `/var/lib/ai-hot-acceptance`。
- 提供用于独立 PITR 恢复的隔离数据库实例；该实例不得连接生产 Redis、R2 或 Webhook。

## 管理员完成后需要回复的非敏感信息

1. 目标 GitHub `owner/repository`，以及是否允许我创建 remote、推送分支并触发发布。
2. 选择的部署平台/主机接入方式和两个正式域名。
3. PostgreSQL、Redis、R2、IdP 资源是否已创建（只回复 Secret 引用路径，不回复值）。
4. 六类权利审批工单 ID 和审批材料在组织文档系统中的引用。
5. 是否为 GitHub Enterprise Cloud 私有仓库。

收到这些输入后，执行顺序固定为：生成隔离身份 → 提交审批 Registry → 发布并验签镜像 → 初始化数据库 → formal preflight → 部署 → PITR 演练 → 容量探针 → 开始 72H 计时。任何一步失败都不会跳过门禁。
