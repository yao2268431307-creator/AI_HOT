# 数据权利证据目录

此目录只提交审批记录、官方条款链接、字段矩阵和不可变摘要，不提交 API Token、合同原件、个人数据或原始平台内容。

正式启用步骤：

1. 复制 `APPROVAL_TEMPLATE.md` 为 `<connector-or-feed>-approval-YYYY-MM-DD.md`。
2. 数据权利、安全和产品负责人完成签字。
3. 计算审批材料 SHA-256，并将摘要写入记录；敏感合同保留在组织文档系统。
4. 在 `config/rights_policies.json` 把对应 `legalApproval` 设置为 `approved-<ticket-id>`，同时登记 `approvalEvidenceDigest`、带时区的 `approvedAt`，以及互不相同的 `dataRights`、`security`、`product` 三名 `approvalReviewers`，并提升 `policyVersion`。
5. 在 `config/connector_registry.json` 把连接器设置为 `rightsStatus=active`，并提升 `registryVersion`。
6. 通过代码审阅后部署；`production_preflight.py --phase formal` 必须通过。

RSS 还必须逐 Feed 在正式清单中登记 `approvalReference` 与审批文件 `approvalEvidenceDigest`；连接器级审批不能替代发布方逐项授权。

当前默认正式 72 小时验收要求 RSS、Hacker News、GitHub、Hugging Face、arXiv、OpenAlex 六个连接器。YouTube 和 X 不得用这六项审批替代各自的平台审批或合同。
