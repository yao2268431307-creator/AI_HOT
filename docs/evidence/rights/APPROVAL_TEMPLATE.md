# 数据源生产使用审批

> 每个连接器或 RSS 发布方复制一份。未经数据权利负责人签字，不得把 Registry 状态改为 `active`。

## 对象

- 连接器/Feed ID：
- 提供方与账户主体：
- 官方 API、Feed 或条款地址：
- 生产项目/合同/工单编号：
- 计划启用日期：
- 复审日期：

## 用途和用户

- 用途：内部 AI 热点发现、聚类、评分与分析师取证
- 使用者范围：
- 是否对外展示：否 / 是（说明）
- 是否用于模型训练：否 / 是（说明）

## 获准字段

- 提供方 ID：
- 账户/实体 ID：
- 时间戳：
- 标题：
- URL：
- 摘要最大字符数：
- 数值指标：
- 禁止字段：

## 保存与派生

- 原始响应允许保存：是 / 否
- 原始响应保留天数：
- 允许派生：去重 / 聚类 / 数值评分 / 分析师证据 / 其他
- 删除范围：raw / observation / metrics / cluster membership / vectors / cache
- 删除请求 SLA：
- 备份和恢复中的删除处理：

## 配额与成本

- 配额单位和周期：
- 最坏每轮请求数：
- 合约人民币/请求：
- 月度连接器预算：
- 限流和 Retry-After 要求：

## 风险判断

- 平台条款允许当前用途：是 / 否
- 隐私政策已覆盖：是 / 否 / 不适用
- 跨境或区域限制已评估：是 / 否 / 不适用
- 再分发和展示限制已落实：是 / 否 / 不适用
- 删除/撤回传播机制已验证：是 / 否

## 决定

- 决定：批准 / 有条件批准 / 拒绝
- 权利策略 ID：
- Registry 目标状态：`active` / `pending` / `blocked`
- `legalApproval` 引用：`approved-<ticket-id>`
- 附加条件：

## 签字

- 数据权利负责人：
- 安全负责人：
- 产品负责人：
- 日期：
- 审批证据摘要 SHA-256：

## Registry 结构化登记

```json
{
  "legalApproval": "approved-<ticket-id>",
  "approvalEvidenceDigest": "sha256:<64-lowercase-hex>",
  "approvedAt": "<ISO-8601-with-timezone>",
  "approvalReviewers": {
    "dataRights": "<subject>",
    "security": "<different-subject>",
    "product": "<different-subject>"
  }
}
```
