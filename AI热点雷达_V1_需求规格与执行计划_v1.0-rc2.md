# AI 热点雷达 V1：需求规格与执行计划

> 文档状态：评审修订稿 v1.0-rc2  
> 更新日期：2026-07-16  
> 修订依据：《AI热点雷达_V1_全面评审与修订建议》  
> 外部依赖核验日期：2026-07-16；配额、价格、可用区和产品能力仍须在 Week 0 复核  
> 目标版本：可信私有技术 Beta  
> 默认时区：Asia/Shanghai  
> 目标读者：产品、分析、数据、算法、前端、后端、测试、运维、安全与合规

---

## 0. 修订结论

外部审计整体合理，且指出的是会影响产品判断有效性的结构性问题。本修订版接受审计正文的 8 个核心 P0 问题，并逐项回答决策清单中的 10 项 P0 决策；绝大多数 P1 建议亦被采纳。

本次修订的核心变化：

1. 从“所有热点使用统一 D×T 分类”改为“事件类型专用行为模型”。
2. 从“六种互斥热点状态”改为“生命周期主状态 + 多个传播结构标签”。
3. 保留讨论与行为两条核心观察轴，但只在行为指标适用、证据充分时使用二维判断。
4. 取消有机度 O 作为确认热点的硬门槛，拆为多个传播结构指标。
5. 从“连接器数量验收”改为“独立信号家族验收”。
6. 首屏从二维雷达改为待研判队列，雷达降为探索视图。
7. V1 使用 PostgreSQL、pgvector、Redis Streams 和 R2，ClickHouse 延后到容量触发后。
8. 加入 Week 0 技术尖峰和稳定 Web 部署备选，不把上线能力绑定到单一公开测试平台。
9. 将 500 个活跃信源改为 120-200 个审核活跃信源，500 个候选信源；2,000 真实容量，10,000 合成压测。
10. 第 8 周交付物明确为“可信私有技术 Beta”，不是完整平台。
11. 将采用/响应、意图代理和纯注意力信号分层，禁止用评论、播放或搜索热度生成“采用已确认”。
12. 补全 Estimate、Coverage、Uncertainty 的计算契约和特征注册表，消除不同实现各算一套的风险。
13. 给产品 KPI 增加分母、工作时段、最低样本量和置信区间，所有数值门槛在影子运行前均视为待校准目标。
14. 修正团队资源口径为约 6.2 FTE、9 人参与。

---

## 1. 审计意见处理决定

### 1.1 P0 决策

| 编号 | 审计问题 | 决定 | 修订结果 |
|---|---|---|---|
| P0-01 | 事件类型及有效行为指标 | 接受 | V1 固定支持 5 类事件，每类单独定义行为指标、时滞和最低证据 |
| P0-02 | 六状态改为主状态 + 多标签 | 接受 | 生命周期主状态互斥，传播结构标签可多选 |
| P0-03 | 时点标签与未来结果分开 | 接受 | 标注分为 t 时点证据和 24 小时/7 天结果，按时间与实体隔离 |
| P0-04 | 评分公式与不确定性 | 接受 | 补充窗口、基线、归一化、冷启动、证据掩码和证据强度 |
| P0-05 | 取消 O 硬门槛 | 接受 | O 拆为来源多样性、原创率、所有权集中度和协同风险 |
| P0-06 | 按信号家族验收 | 接受 | 至少 4 个独立信号家族，必须同时包含讨论和行为 |
| P0-07 | Sites 与外部后端技术尖峰 | 接受 | Week 0 完成身份、外部 API、SSE/轮询和替代部署验证 |
| P0-08 | 明确团队资源 | 接受 | 明确约 6.2 FTE、9 人参与的资源模型；资源不足则延长到 12-16 周 |
| P0-09 | 中文数据路径 | 接受 | 未有合法中文讨论源时主动收缩产品定位，不以模拟覆盖替代 |
| P0-10 | Google Trends 替代 | 接受 | Trends 不进入关键路径；无行为源时只输出注意力，不输出采用确认 |

### 1.2 P1 决策

| 编号 | 审计问题 | 决定 | 修订结果 |
|---|---|---|---|
| P1-01 | Observation 与 MetricSnapshot 混合 | 接受 | 拆为不可变内容事实与追加式指标快照 |
| P1-02 | Source 过宽 | 接受 | 引入 Person、Organization、Account、Repository、Publication、Channel 和关系边 |
| P1-03 | 合并拆分谱系 | 接受 | 引入 ClusterVersion、ClusterOperation、supersededBy 和继承规则 |
| P1-04 | 30 天后重放 | 接受 | 长期保存最小事实记录、哈希和解析器版本，不长期保存受限全文 |
| P1-05 | 跨存储一致性 | 接受并进一步简化 | V1 暂缓 ClickHouse；PostgreSQL Outbox 为唯一提交边界 |
| P1-06 | 连接器预算模型 | 接受 | 每个连接器必须提供发现、增量、刷新、回补和最坏成本模型 |
| P1-07 | 外部通知 | 接受 | V1 支持通用签名 Webhook；邮件列为 P1 |
| P1-08 | 首屏待研判队列 | 接受 | 队列为默认首页，二维雷达为探索视图 |
| P1-09 | RPO 收紧到 1 小时 | 接受 | PostgreSQL 开启 PITR，核心状态 RPO 不高于 1 小时 |
| P1-10 | Vinext 降为实验选项 | 接受 | 前端契约保持框架无关；只有 Week 0 尖峰通过才使用 Sites/Vinext |

---

## 2. 产品定义

### 2.1 产品目标

AI 热点雷达帮助 AI 内容与战略团队回答三个问题：

1. 现在有哪些 AI 事件值得优先研判？
2. 事件是处于早期增长、加速扩散还是已经稳定破圈？
3. 结论由哪些独立证据支持，哪些数据仍然缺失？

产品不承诺“看见整个互联网”，也不直接判断某事件一定由营销投放或推荐算法造成。产品输出的是基于公开证据的生命周期、传播结构、行为确认和不确定性。

### 2.2 V1 成功定义

V1 必须证明：

- 少量可靠信号能够形成可复现的事件判断。
- 分析师能在 5 分钟内完成大多数候选事件的证据核验。
- 强告警数量受控，且相较人工基线具有可量化的提前量。
- 数据缺失、平台时滞和聚类不确定性不会被隐藏。
- 人工修订、删除和评分回放能够完整闭环。

### 2.3 目标用户

| 角色 | 主要任务 | 权限 |
|---|---|---|
| Owner | 管理成员、连接器、预算、告警和发布 | 全部管理权限 |
| Analyst | 研判、关注、标注、合并拆分和反馈 | 分析与编辑权限 |
| Viewer | 查看待研判队列、事件详情和共享结果 | 只读与关注权限 |

---

## 3. V1 范围与发布定位

### 3.1 V1 必须交付

- 5 类事件的事件本体与专用行为模型。
- 生命周期主状态与传播结构标签。
- 120-200 个审核活跃信源。
- 500 个候选信源。
- 至少 4 个稳定独立信号家族，其中包含讨论和行为。
- 15 分钟候选发现与分层刷新。
- 中英文内容归一化与事件归并。
- 待研判队列、事件详情、覆盖页、关注与反馈。
- 通用签名 Webhook 外部告警。
- 原始证据回链、最低证据组合、数据缺口和证据强度。
- 聚类谱系、评分版本、删除传播和历史回放。
- 2,000 信源真实容量验证，10,000 信源合成压力测试。

### 3.2 V1 不做

- 泛行业融资、政策、名人和消费级社会热点的统一评分。
- 500 个自动晋级活跃信源。
- 8 个平台全部稳定在线的形式化指标。
- 完整传播网络产品。
- 公开大众榜、SEO 站、多租户计费。
- 24 小时以上预测。
- 完整移动端编辑能力。
- 浅色主题。
- 完全自动的信源晋级、事件合并和事实裁决。

### 3.3 发布定位降级规则

若没有至少一个合法、稳定的社会化讨论家族：

- 产品不得宣传为“全网 AI 破圈雷达”。
- 产品名称下方必须显示“开发者与研究生态信号 Beta”。
- confirmed 或 cross-platform 类结论必须限制在实际覆盖的生态范围内。

若中文只有官方和媒体源，没有真实中文讨论源：

- 产品不得声称具备完整中英文破圈比较能力。
- 覆盖页必须显示“中文讨论覆盖不足”。

---

## 4. 事件类型体系

### 4.1 V1 支持的事件类型

~~~ts
type EventType =
  | "model_release"
  | "developer_tool_release"
  | "research_or_benchmark"
  | "official_product_release"
  | "security_incident";
~~~

### 4.2 类型配置

| 事件类型 | 典型事件 | 核心权威证据 | 讨论信号 | 行为或响应信号 | 正常行为时滞 | V1 最低证据组合 |
|---|---|---|---|---|---|---|
| model_release | 新模型、重大版本、权重开放 | 官方博客、模型卡、仓库、发布页 | HN、社交讨论、技术媒体 | HF 下载、依赖或集成增长、Space/衍生项目、活跃贡献者 | 0-24 小时 | 1 个权威源 + 1 个讨论家族 + 1 个模型/代码行为源 |
| developer_tool_release | 框架、Agent 工具、SDK、开源项目 | Release、仓库、官方文档 | HN、Issue、开发者讨论 | 包下载、依赖/集成项目、模板使用、活跃贡献者；Star/Fork 仅作意图代理 | 0-48 小时 | 1 个官方开发源 + 1 个独立讨论源 + 1 个响应或采用源 |
| research_or_benchmark | 论文、评测、研究突破 | arXiv/OpenAlex、作者主页、代码 | 研究者讨论、HN、技术媒体 | 独立复现、benchmark 复用、代码/数据集依赖、后续研究使用 | 6 小时-7 天 | 1 个论文源 + 1 个独立技术讨论源；采用信号可暂不适用 |
| official_product_release | API、平台、商业产品重大更新 | 官方公告、更新日志、文档 | 社区讨论、视频、媒体、搜索意图 | 获授权的一方活跃使用、API 调用、试用完成、包/集成增长 | 1-72 小时 | 1 个官方源 + 1 个独立讨论源；无独立行为源时不得输出采用确认 |
| security_incident | 漏洞、供应链风险、重大安全公告 | 官方 Advisory、CVE、修复 Release | 开发者和安全社区讨论 | 修复版本、Issue、受影响仓库、缓解措施采用 | 0-48 小时 | 1 个权威安全源 + 1 个独立讨论或修复响应源 |

### 4.3 事件类型规则

- 不适用的行为信号标记为 N/A，不计入缺失。
- 预期存在但未获得的信号标记为 missing，降低覆盖与证据强度。
- 行为时滞按事件类型和事件年龄计算。
- 每个原始指标必须标记为 `primary_adoption`、`primary_response`、`secondary_intent` 或 `attention_only`；评论、播放、搜索热度和 Star 不得单独触发 `adoption_confirmed`。
- 同一长期 Narrative 可以包含多个不同 Event，例如“模型发布”“API 开放”“价格调整”分别评分。
- 融资、政策和泛媒体事件在 V1 中只进入未支持队列，不强行使用上述模型。

---

## 5. 输出模型：生命周期主状态 + 结构标签

### 5.1 生命周期主状态

~~~ts
type LifecycleState =
  | "insufficient_data"
  | "detected"
  | "emerging"
  | "accelerating"
  | "established"
  | "cooling"
  | "dormant"
  | "noise";
~~~

主状态同一时刻只能有一个。

| 主状态 | 定义 |
|---|---|
| insufficient_data | 未满足事件类型最低证据组合，或关键连接器异常 |
| detected | 已形成可识别事件，但增长或样本尚不足 |
| emerging | 相对基线出现异常增长，证据达到中等级别 |
| accelerating | 注意力或适用行为持续加速，满足类型专用阈值 |
| established | 已跨过类型专用确认窗口，增长与证据保持稳定 |
| cooling | 曾进入 accelerating/established，当前连续下降 |
| dormant | 在类型专用休眠窗口内无有效变化，但可能重新激活 |
| noise | 覆盖充分但主要为重复、低新颖度或低强度信号 |

### 5.2 传播结构标签

~~~ts
type StructureLabel =
  | "cross_platform_confirmed"
  | "adoption_confirmed"
  | "response_confirmed"
  | "platform_concentrated"
  | "coordination_risk"
  | "attention_behavior_gap"
  | "expected_behavior_lag"
  | "official_source_led"
  | "low_source_diversity"
  | "reactivated";
~~~

标签可多选，且必须具有独立触发和解除规则。初始通用约束如下，具体阈值由 `labelPolicyVersion` 按 eventType 冻结：

| 标签 | 触发条件 | 解除条件 |
|---|---|---|
| cross_platform_confirmed | 至少 2 个平台家族、2 个所有权去重实体提供一致证据，证据强度至少 medium | 聚类修订后不再满足，或关键证据失效 |
| adoption_confirmed | 至少 1 个 `primary_adoption` 家族异常增长且满足类型最小样本，连续 2 个适用窗口 | 连续 2 个窗口低于解除阈值，或证据被回修/删除 |
| response_confirmed | 安全或响应型事件至少 1 个 `primary_response` 家族达到类型阈值 | 连续 2 个窗口低于解除阈值，或证据失效 |
| platform_concentrated | 可信增量至少 80% 来自单一平台家族且样本充足 | 单平台占比连续 2 个窗口低于 65% |
| coordination_risk | 所有权集中、近重复文案、共同目标 URL、同步发布中至少 2 项异常 | 风险特征在类型专用观察窗内均回落；历史触发仍保留在时间线 |
| attention_behavior_gap | 见 7.10，扣除正常类型时滞后残差异常 | 残差连续 2 个窗口回到解除阈值内 |
| expected_behavior_lag | 当前事件年龄仍处于类型正常时滞且 Behavior 未成熟 | 超出正常时滞、Behavior 变为 N/A，或行为证据到达 |
| official_source_led | 注意力主要由同一官方所有权实体及其账号贡献 | 独立实体贡献连续 2 个窗口超过类型阈值 |
| low_source_diversity | 覆盖充分时，独立实体数或所有权去重熵低于阈值 | 两项均连续 2 个窗口恢复 |
| reactivated | dormant 事件因新增独立证据转回 emerging | 完成一个确认窗口后解除，触发记录留在时间线 |

### 5.3 用户可见组合示例

- 加速中｜跨平台确认｜采用已确认
- 加速中｜跨平台确认｜修复响应已确认
- 加速中｜平台集中｜行为数据滞后
- 已稳定｜官方集中发布｜协同传播风险较高
- 早期增长｜来源多样性不足
- 数据不足｜中文讨论源不可用

### 5.4 “确认热点”的派生规则

“确认热点”是派生徽标，不是生命周期主状态。

初始规则：

- lifecycleState 为 accelerating 或 established。
- evidenceStrength 为 high。
- attentionEstimate 不低于类型专用阈值。
- 存在 cross_platform_confirmed。
- 若该事件类型适用行为信号，则 behaviorEstimate 达到类型专用阈值；`adoption_confirmed` 只由 `primary_adoption` 证据触发，响应型事件可改用类型专用响应标签。
- 不要求 coordinationRisk 为低。

因此一个事件可以同时是“确认热点”和“协同传播风险较高”。

---

## 6. 信号维度

### 6.1 核心维度

| 维度 | 含义 | 是否进入二维分析 |
|---|---|---|
| Attention | 独立讨论和内容消费注意力 | 横轴 |
| Behavior | 类型专用采用、使用或响应行为 | 适用时作为纵轴 |
| Diversity | 独立实体、社区和平台多样性 | 解释与标签 |
| Authority | 官方、研究或领域权威确认 | 解释与证据 |
| Coordination Risk | 集中、同步和重复传播结构风险 | 解释与标签 |
| Coverage | 预期证据的获得程度 | 结论门槛 |
| Uncertainty | 样本、基线、聚类和时滞带来的不确定性 | 证据强度 |

### 6.2 两条原始判断轴的保留方式

用户最初提出的“讨论度 × 数据趋势”仍然保留：

- 讨论度对应 Attention。
- 数据趋势改为事件类型专用 Behavior。
- Behavior 不适用或证据不足时，不绘制误导性的二维坐标。
- 默认首页不依赖二维雷达做排序。
- 雷达只展示同时具备 Attention 与 Behavior 有效估计的事件。

---

## 7. 可实现的数学定义

### 7.1 时间窗口

默认窗口：

- 15 分钟：模型发布、开发者工具、安全事件的早期发现。
- 1 小时：产品发布和研究事件的主要刷新。
- 6 小时：潜伏信号和正常时滞判断。
- 24 小时：跨平台扩散和短期结果。
- 7 天：持续性、采用和后验结果。

每个 eventType 配置 activeWindow、confirmationWindow、coolingWindow 和 dormancyWindow。

### 7.2 原始增长率

对于累积指标 x：

~~~text
r(m,w,t) = [log(1 + x_t) - log(1 + x_(t-w))] / windowHours
~~~

对于区间计数：

~~~text
r(m,w,t) = log(1 + count(m, t-w, t) / windowHours)
~~~

原始值先按平台和指标执行 P1/P99 截断，避免极端异常主导分数。

### 7.3 基线标准化

基线按以下键分层：

~~~text
eventType × platformFamily × language × eventAgeBucket × window × baselineVersion
~~~

标准化：

~~~text
robustZ = clip(
  (r - medianBaseline) / max(1.4826 × MAD, scaleFloor),
  -4,
  8
)
~~~

映射到 0-100：

~~~text
featureScore = 100 / (1 + exp[-0.9 × (robustZ - 1)])
~~~

每个进入评分的特征必须在版本化 Feature Registry 中定义：`featureId`、适用 eventType、信号类别、输入字段、单位、方向、窗口、平台家族、实体去重层级、基线键、权重、家族上限、最小样本、正常延迟、修订策略和测试向量。没有完整注册项的字段只能用于展示，不能进入评分。

多平台合并规则：

- 先在 Account 层去重，再按 Person/Organization 所有权图聚合。
- 同一底层行为被多个连接器观察到时只保留一个主观测，其他记录为佐证，不重复加分。
- 每个信号家族设置贡献上限，避免单个平台用多个相似指标占满分数。
- 累积指标发生回修时按 `sourceRevision` 重算受影响窗口；负修订不得直接解释为真实热度下降。

维度估计使用固定的适用权重分母，不把缺失特征的权重转移给已观测特征：

~~~text
applicableWeight = Σ weight_i, evidenceState_i != not_applicable
estimate = Σ(weight_i × featureScore_i × trustedObserved_i) / applicableWeight
coverage = Σ(weight_i × trustedObserved_i) / applicableWeight
~~~

其中 `trustedObserved_i` 仅在状态为 `observed` 且通过质量校验时为 1；`missing` 和 `untrusted` 不贡献分子，`not_applicable` 才从分母排除。这样缺失 60% 适用权重时，剩余信号无法被重归一化成接近满分。

### 7.4 冷启动

- 信源历史少于 28 天时使用同平台、同规模、同事件类型的 peer baseline。
- eventType 样本不足时 baselineMaturity 下降。
- baselineMaturity 低于 0.4 时只能输出低或中证据强度。
- 新信源不能因为单次极端增长获得高权威或高领先度。

### 7.5 Attention

~~~text
Attention =
  0.45 × ownershipDedupedMentionVelocity
  + 0.30 × contentEngagementVelocity
  + 0.15 × contentConsumptionVelocity
  + 0.10 × originalContentVelocity
~~~

高权威信源不直接推高 Attention。权威性单独进入 Authority；平台数、社区数和来源熵只进入 Diversity，不再重复进入 Attention。

### 7.6 Behavior

Behavior 使用 eventType 专用 Feature Profile。

#### model_release

~~~text
0.35 × modelDownloads
+ 0.30 × repositoryOrPackageAdoption
+ 0.20 × derivativeProjectsOrSpaces
+ 0.15 × activeContributorGrowth
~~~

#### developer_tool_release

~~~text
0.25 × starAndForkIntent
+ 0.30 × packageOrDependencyUsage
+ 0.25 × dependentRepositoryOrIntegrationGrowth
+ 0.20 × activeContributorAndReleaseResponse
~~~

`starAndForkIntent` 属于 `secondary_intent`，即使单项极高也不能触发 `adoption_confirmed`。

#### research_or_benchmark

~~~text
0.35 × independentReproductionActivity
+ 0.30 × codeOrDatasetDependencyUsage
+ 0.20 × benchmarkReuse
+ 0.15 × downstreamScholarlyUse
~~~

讨论、评论和媒体引用只进入 Attention。没有代码、复现或适用采用信号时，Behavior 可以为 N/A。

#### official_product_release

只根据获得授权的一方或可审计采用数据计算，初始 Profile 为：

~~~text
0.40 × activeUsageOrApiCalls
+ 0.25 × trialOrActivationCompletion
+ 0.20 × packageOrIntegrationGrowth
+ 0.15 × retainedUsage
~~~

视频消费和搜索意图只进入 Attention 或 `secondary_intent`，不得触发采用确认。缺乏独立行为源时 Behavior 为 N/A，不使用讨论量代替。

#### security_incident

~~~text
0.35 × remediationReleaseActivity
+ 0.25 × affectedRepositoryResponse
+ 0.25 × issueAndAdvisoryActivity
+ 0.15 × mitigationAdoption
~~~

### 7.7 Diversity、Authority 与 Coordination Risk

Diversity：

- 独立组织数量。
- 独立社区数量。
- 平台家族数量。
- 所有权图去重后的来源熵。

Authority：

- 官方发布。
- 论文、CVE、Release、模型卡等硬标识。
- 领域权威的独立确认。
- 权威证据一致性。

Coordination Risk：

- 同所有权实体的集中贡献。
- 高相似文案。
- 共同目标 URL。
- 五分钟内同步发布。
- 转发和引用网络集中度。

Coordination Risk 只描述传播结构，不证明营销投放、机器人或不真实用户。

### 7.8 缺失、N/A 与证据掩码

每个特征状态只能为：

~~~ts
type EvidenceState = "observed" | "missing" | "not_applicable" | "untrusted";
~~~

输出必须包含：

- evidenceMask。
- observedFeatureWeight。
- expectedFeatureWeight。
- sampleSize。
- baselineMaturity。
- clusterConfidence。

缺失项不自动把剩余特征权重放大到满分。系统可以显示“个别已观测信号增长很高”，但维度 estimate、Coverage 和证据强度必须按固定适用权重反映缺口，不能产生强告警。

### 7.9 证据强度

~~~text
evidenceQuality =
  0.30 × coverage
  + 0.20 × sampleReliability
  + 0.20 × baselineMaturity
  + 0.15 × clusterConfidence
  + 0.15 × temporalStability
~~~

各分量均为 0-1。`sampleReliability = min(1, log(1 + effectiveIndependentEntities) / log(1 + targetSampleSize))`，其中目标样本量按 eventType 和信号家族版本化；其余分量的计算与测试向量也必须进入 Feature Registry 或 Cluster Registry。

~~~text
uncertaintyIndex = clip(1 - evidenceQuality, 0, 1)
~~~

`uncertaintyIndex` 是证据不确定性指数，不是“判断为真的概率”，前端不得以百分比置信概率呈现；同时输出 `uncertaintyReasons[]`，列明覆盖、样本、基线、时滞或聚类中的主要来源。

用户可见层级：

- low：小于 0.45。
- medium：0.45-0.70。
- high：不低于 0.70。

上述分界是影子运行前的初始值，必须经可靠性检查后随 `evidencePolicyVersion` 冻结。在完成校准前，前端不展示看似精确的“80% 置信度”，只展示低、中、高证据强度。

### 7.10 剪刀差与正常时滞

~~~text
observedGap = Attention - Behavior
expectedGap = medianGap(eventType, eventAge, platformMix, baselineVersion)
gapResidual = observedGap - expectedGap
~~~

只有以下条件同时满足才添加 attention_behavior_gap：

- Behavior 适用。
- evidenceStrength 至少为 medium。
- gapResidual 的 robustZ 不低于 2。
- 连续两个评估窗口成立。

处于正常发布时滞时添加 expected_behavior_lag，不添加异常剪刀差标签。

---

## 8. 生命周期状态机

### 8.1 初始转移

~~~mermaid
stateDiagram-v2
    [*] --> insufficient_data
    insufficient_data --> detected: 最低证据组合满足
    detected --> emerging: 类型专用异常增长成立
    emerging --> accelerating: 连续增长与证据门槛成立
    accelerating --> established: 确认窗口完成
    established --> cooling: 连续下降
    cooling --> dormant: 休眠窗口无增长
    dormant --> emerging: 新证据重新激活
    detected --> noise: 覆盖充分且信号低
    emerging --> noise: 证据证伪或主要为重复
    noise --> emerging: 新独立证据出现
    detected --> insufficient_data: 覆盖跌破门槛
    emerging --> insufficient_data: 关键数据失效
~~~

### 8.2 状态门槛

具体阈值按 eventType、language、ageBucket 和 baselineVersion 配置。

全局约束：

- emerging 至少需要一个异常增长特征 robustZ 不低于 2，证据强度不低于 medium。
- accelerating 需要 Attention 或适用 Behavior 达到类型专用阈值，并连续两个周期为正增长。
- established 需要完成类型专用确认窗口，并满足最低独立证据组合。
- insufficient_data 优先级最高，但原计算结果保留为 provisional，不对用户输出强状态。
- noise 只能在 Coverage 充分时判定。

### 8.3 时间目标

- 15 分钟目标：数据进入系统并产生 detected 或 emerging 候选。
- 30-60 分钟目标：适用的快速事件进入 accelerating。
- confirmed/established 的时间取决于事件类型与正常行为时滞，不承诺统一 15 分钟确认。
- 强状态写入成功后，站内流与 Webhook 在 5 分钟内进入发送队列；采集延迟、候选发现延迟、确认延迟和通知延迟分别计量，禁止用端到端单一平均值掩盖瓶颈。

---

## 9. 信号家族与数据源

### 9.1 信号家族

| 信号家族 | 示例 | 用途 |
|---|---|---|
| 官方与权威发布 | 官方博客、Release、模型卡、论文、CVE | Authority、事件确认 |
| 开发者采用 | GitHub、Hugging Face、包和依赖关系 | Behavior |
| 研究生态 | arXiv、OpenAlex、代码复现 | Authority、Behavior |
| 社区讨论 | HN、合法社会化数据源 | Attention、Diversity |
| 视频与内容消费 | YouTube、授权 Bilibili | Attention、Behavior |
| 中文官方与媒体 | 中文官方站、合法 RSS、媒体源 | 中文事件发现 |
| 中文社会化讨论 | 获授权平台或合法供应商 | 中文 Attention、Diversity |
| 搜索与访问意图 | Google Trends Alpha 或合法替代 | Behavior，可选 |

### 9.2 Beta 发布门槛

- 至少 4 个独立信号家族稳定。
- 必须包含至少 1 个讨论家族。
- 必须包含至少 1 个行为家族。
- 任一强结论满足 eventType 的最低证据组合。
- 连接器数量不作为独立发布门槛。

### 9.3 连接器优先级

| 级别 | 数据源 | 说明 |
|---|---|---|
| P0 | 官方 RSS/博客、Hacker News、GitHub、Hugging Face、arXiv/OpenAlex | 必须形成稳定闭环 |
| P1 | YouTube | 配额尖峰通过后启用 |
| 实验性 | X、Bluesky、Bilibili、其他中文平台 | 不作为 Beta 阻断项，不进入关键结论路径 |
| 不可用 | Google Trends | 未获得 Alpha 权限时不进入任何核心公式 |

### 9.4 连接器验收卡

每个连接器必须回答：

| 项目 | 必填内容 |
|---|---|
| 业务用途 | 发现、讨论、行为、权威确认或摘要 |
| 访问方式 | 官方 API、Webhook、RSS、授权接口或允许网页 |
| 字段权利 | 采集、存储、展示、导出、embedding 和派生 |
| 发现路径 | 如何发现新对象 |
| 增量路径 | 如何获得新增内容 |
| 指标刷新 | 热、温、冷对象的刷新频率和批处理 |
| 回补策略 | 历史范围和成本 |
| 配额与费用 | 单请求、单资源、日月上限和最坏情况 |
| 删除义务 | 原始、摘要、embedding、缓存、评分和备份 |
| 质量 | 延迟、回修、删除、原创和关联账号能力 |
| 验收 | 72 小时成功率、新鲜度、重复率和覆盖贡献 |

### 9.5 特定连接器策略

#### GitHub

- 自有或已安装 App 的仓库优先 Webhook。
- 第三方仓库使用 ETag 和条件请求。
- 活跃事件升频，候选仓库低频。
- 不以朴素轮询覆盖全部仓库。

#### YouTube

- 发现与指标刷新分离。
- 使用频道上传播放列表或推送发现新视频。
- 批量查询视频指标。
- 低热度视频降频，活跃事件短期升频。
- 高成本搜索严格限额。

#### X

- 在建立 Post reads 成本模型前，不进入强状态关键路径。
- 成本模型至少包含查询数、周期数、返回数、日内唯一 Post、端点价格和高峰系数。

#### Bluesky

- Jetstream 只用于候选发现。
- 高置信证据通过 Firehose、AppView API 或原始页面复核。
- Evidence 保存 provenanceLevel。

#### arXiv/OpenAlex

- arXiv 优先 RSS/OAI-PMH 增量，API 用于补充。
- OpenAlex 用于作者、引用和主题补全，不做高频全量扫描。

### 9.6 动态外部假设

以下只是在 2026-07-16 核验过的规划输入，不是长期不变的产品事实；Week 0 必须根据实际账户、控制台、合同和地区重新确认并记录 `verifiedAt`：

| 外部能力 | 当前规划输入 | 计划处理 |
|---|---|---|
| ChatGPT Sites | 处于 public beta，账户限额可变化，部分框架、私网、数据库、后台服务和托管模式可能不受支持 | 只作为候选 Web/BFF；身份、外部 FastAPI、SSE 和替代部署必须做端到端尖峰 |
| X API | 按量计费，端点价格需从控制台获取；读取量与日内去重规则会影响成本 | 未完成真实查询模型和预算压测前保持实验性 |
| YouTube Data API | 默认日配额及扩容审计会限制搜索型发现 | 发现与指标刷新拆分，高成本搜索设硬预算 |
| GitHub REST API | 官方建议 Webhook 优先，并支持合适场景下的条件请求 | Webhook + ETag/Last-Modified + 分层刷新 |
| Hugging Face Hub | API、Resolver 和 Page 使用不同限额桶，按短窗口计算 | 连接器分别记账并按响应头自适应 |
| Google Trends API | 仍需申请 Alpha 访问 | 不进入 Beta 关键路径 |
| Bluesky Jetstream | 事件不自带密码学认证且不是正式协议稳定接口 | 仅做候选发现，高置信证据回源复核 |

---

## 10. 信源和实体模型

### 10.1 实体

~~~ts
type EntityKind =
  | "person"
  | "organization"
  | "account"
  | "repository"
  | "publication"
  | "channel";
~~~

关系：

- OwnershipEdge。
- AffiliationEdge。
- OperatesEdge。
- PublishesEdge。
- ReferencesEdge。

独立来源数量必须在 Person/Organization 层去重，不能把同一公司的博客、X、GitHub、Hugging Face 和 YouTube 当作五个独立确认。

### 10.2 信源评分

- Authority 与 Attention 分开。
- 信源领先度只使用严格早于当前事件的历史。
- SourceScore 按月或评分版本冻结。
- 低样本信源使用经验贝叶斯收缩。
- 每个周期保留 10% 探索配额给新信源。
- 新信源不会因为一次命中直接自动进入高权威等级。

### 10.3 V1 规模

- 120-200 个审核 active 信源。
- 500 个 candidate 信源。
- 2,000 信源真实容量验证。
- 10,000 信源仅用于合成压力测试。
- 自动晋级为 P2，V1 由 Analyst 批准。

---

## 11. 数据模型

### 11.1 ContentObservation

保存不可变内容事实：

~~~ts
interface ContentObservation {
  id: string;
  schemaVersion: number;
  connector: string;
  platform: string;
  externalId: string;
  accountId: string;
  entityId: string;
  publishedAt: string;
  availableAt?: string;
  collectedAt: string;
  language: "zh" | "en" | "other";
  title?: string;
  textExcerpt: string;
  canonicalUrl: string;
  contentHash: string;
  relation: "original" | "quote" | "repost" | "reply" | "unknown";
  rawRef?: string;
  parserVersion: string;
  rightsPolicyId: string;
  deletionState: "active" | "tombstoned" | "deleted";
}
~~~

### 11.2 MetricSnapshot

指标只追加，不更新原 Observation：

~~~ts
interface MetricSnapshot {
  id: string;
  subjectType: "content" | "source" | "repository" | "model" | "event";
  subjectId: string;
  metricName: string;
  value: number;
  effectiveAt: string;
  collectedAt: string;
  isEstimated: boolean;
  sourceRevision?: string;
  connector: string;
}
~~~

### 11.3 Event 与 Narrative

- Narrative：长期主题，例如“某模型家族”。
- Event：可独立评分的具体事件，例如发布、开放 API、降价或安全事故。
- Event 可以关联一个 Narrative，但评分只发生在 Event。

### 11.4 事件谱系

核心字段：

- eventId。
- clusterVersion。
- parentClusterId。
- supersededBy。
- mergeOperationId。
- splitOperationId。
- effectiveAt。

合并拆分规则：

- 原 Event ID 保留并标记 superseded。
- 新 Event 继承关注关系，但不直接继承人工标签。
- 历史评分绑定 clusterVersion，不改写。
- 当前时间序列在新版本上重算。
- 已发告警保留原版本并链接到继任事件。
- 操作支持撤销，冲突使用乐观锁。

### 11.5 最小重放事实

长期保存：

- externalId。
- 时间戳。
- URL 哈希。
- 内容哈希。
- 指标值。
- HTTP 状态和响应字段哈希。
- parserVersion。
- rightsPolicyId。
- 删除标记。
- 原始对象引用和删除记录。

受版权限制的完整载荷按条款短期删除，但最小事实允许验证采集和评分过程。

---

## 12. 聚类方案

### 12.1 多阶段候选与判定

1. 硬标识：仓库、论文 ID、模型 ID、视频 ID、CVE、官方 URL。
2. 实体和版本号匹配。
3. 时间窗口。
4. BGE-M3 向量候选召回。
5. eventType 一致性。
6. 冲突实体、不同版本和不同动作负约束。
7. 高影响或低置信候选进入 LLM/人工复核。

0.82 只作为初始候选召回阈值，不作为最终归并规则。

### 12.2 向量

- V1 使用 PostgreSQL + pgvector。
- 保存 embeddingModel、embeddingVersion 和 chunkingVersion。
- 短内容单向量，长文按段落切分并保存聚合中心。
- 模型升级采用双写新旧向量、后台重嵌入和版本切换。
- 合并拆分后重新计算事件中心，不修改原观测向量。

### 12.3 聚类评估

报告：

- Pairwise Precision/Recall。
- B-cubed Precision/Recall。
- 误合并率。
- 误拆分率。
- 跨语言误合并率。
- 同实体不同事件误合并率。

高影响事件的 Pairwise Precision 目标不低于 0.90，误合并优先于误拆分控制。

---

## 13. 系统架构

### 13.1 V1 架构

~~~mermaid
flowchart LR
    A["官方 API、RSS、Webhook、授权来源"] --> B["Connector Workers"]
    B --> C["R2 原始对象与最小事实"]
    B --> D["Redis Streams"]
    D --> E["标准化、去重、实体与聚类"]
    E --> F["PostgreSQL + pgvector"]
    F --> G["ScoringRun"]
    G --> H["事务更新当前状态 + Outbox"]
    H --> I["告警与签名 Webhook"]
    F --> J["FastAPI"]
    J --> K["Web/BFF"]
    L["人工反馈、合并拆分、删除"] --> F
~~~

### 13.2 为什么 V1 暂缓 ClickHouse

- 120-200 活跃信源和分层评分不需要立即承担双分析存储复杂度。
- PostgreSQL 分区表足以支持 V1 规模验证。
- Outbox 保留未来添加 ClickHouse 的标准出口。
- 当 15 分钟快照超过 5,000 万、查询 P95 连续超标或分析任务影响事务负载时，再引入 ClickHouse。

### 13.3 一致性

- PostgreSQL 是事件、成员关系、当前状态、告警规则和审计的唯一真源。
- 评分先创建 ScoringRun，完成后在一个事务内更新当前状态并写 Outbox。
- 告警只消费已提交的 Outbox 状态变化。
- R2 对象写入成功后才能提交对应最小事实记录。
- 未来分析存储只能消费 Outbox，不参与当前状态裁决。

### 13.4 Redis Streams 恢复

- Consumer Group 监控 Pending Entry List。
- 超过重试阈值进入 DLQ。
- 毒消息保存 traceId、schemaVersion 和 rawRef。
- Stream 目标保留 7 天，按时间、容量和所有 Consumer Group 的 earliest-pending/last-delivered 安全水位显式维护；不得由 publisher 自动强裁。
- Redis 丢失后从 PostgreSQL Outbox、连接器检查点和 R2 原始对象恢复。
- 连接器检查点在对应事件持久化后推进。
- 常规 publisher 使用参与者租约，重放与裁剪使用排他租约；每次 `XADD`/`XTRIM` 必须在同一 Redis Lua 内校验 token、续租并执行变更，过期或被替换的 writer fail closed。
- Stream、恢复状态、排他锁和参与者集合必须使用同一 Redis Cluster hash slot；围栏协议必须有版本号并写入检查点和运维报告。
- 围栏键布局或协议版本升级前必须确认旧协议无 `running` 检查点或活跃租约；不自动迁移或静默删除旧状态。
- 消息链仍按 at-least-once 设计；`XADD` 与 PostgreSQL 发布标记或重放检查点之间中断时允许重复稳定 `outbox_id`，由消费者业务幂等约束收敛。

### 13.5 评分分层

- Hot Event：15 分钟。
- Warm Event：1 小时。
- Cold/Dormant Event：每日。
- 只有新增证据、指标显著变化或人工操作时才强制重算。
- 不对全部 2,000 事件每 15 分钟无差别评分。

### 13.6 快照保留

- 15 分钟快照：90 天。
- 小时聚合：1 年。
- 日聚合：2 年。
- 原始内容观测、指标快照和评分快照分别计算容量。

---

## 14. 部署与身份技术尖峰

### 14.1 Week 0 必测

- Sites 是否可用以及当前账户限制。
- 可获得的身份字段和服务端验证方式。
- Owner/Analyst/Viewer 映射。
- 用户移出工作区后的失效。
- 外部 FastAPI 调用。
- 自定义 Header、CORS 和服务令牌。
- SSE、30 秒轮询降级和连接上限。
- 日志、限额和数据驻留要求。

### 14.2 部署决策

- 尖峰通过：可使用 Sites 作为私有 Web/BFF。
- 尖峰未通过：使用稳定 React/Next.js Web 托管，后端与 API 不改写。
- Vinext 仅在尖峰通过并接受实验性风险时使用。
- 业务逻辑不得依赖托管平台专有客户端 API。

### 14.3 服务令牌

必须包含：

- issuer。
- audience。
- subject。
- workspaceId。
- role。
- jti。
- issuedAt。
- expiresAt，最长 5 分钟。
- signingKeyVersion。

FastAPI 必须重新查询成员关系，不能信任浏览器提交的 workspaceId 或 role。

---

## 15. 安全与合规

### 15.1 外部内容威胁模型

所有标题、HTML、字幕、URL、摘要和正文都视为不可信输入。

必须实现：

- HTML 严格净化。
- URL 协议白名单。
- DNS/IP 解析后的 SSRF 防护。
- 外部图片代理限制。
- Unicode 混淆检测。
- 文本长度与嵌套限制。
- LLM 无工具权限。
- LLM 结构化输出 Schema 校验。
- 证据 URL 由程序绑定，禁止 LLM 自行生成。

### 15.2 Webhook

所有入站和出站 Webhook 必须定义：

- HMAC 签名。
- 时间戳。
- 五分钟重放窗口。
- 请求体大小限制。
- 速率限制。
- 幂等键。
- 密钥轮换。
- 重试和 DLQ。

### 15.3 工作区隔离

- V1 的原始事件和公共信源图可全局共享。
- 关注、反馈、备注、告警、成员和导出严格按 workspace 隔离。
- 人工反馈默认只影响当前工作区。
- 只有经过离线审核的全局规则变更才影响其他工作区。
- PostgreSQL 对工作区数据启用 Row-Level Security 或等价的服务端强制过滤。

### 15.4 字段级权利矩阵

每个连接器记录：

- 允许采集字段。
- 允许存储字段。
- 允许展示和导出字段。
- 允许生成 embedding 和派生指标的字段。
- 保留期限。
- 平台删除和个人删除义务。
- 内部研究、商业产品和再分发限制。
- 业务、数据、合规负责人和复核日期。

### 15.5 删除传播

删除工作流覆盖：

- R2 原始对象物理删除。
- PostgreSQL 文本片段与个人字段。
- embedding。
- 聚类成员关系。
- 搜索索引和缓存。
- 未聚合分析数据。
- 备份过期清除。

历史分数保留非个人、不可逆聚合值和删除 tombstone；如条款要求连派生值删除，则按 rightsPolicy 执行。

---

## 16. 产品页面

### 16.1 P0：首页待研判队列

默认按以下优先级排序：

1. 新增强信号。
2. 生命周期升级。
3. 新增跨平台或行为确认。
4. 关注事件变化。
5. 覆盖突然下降。
6. 低证据候选。

每行显示：

- 事件标题与类型。
- 生命周期。
- 结构标签。
- Attention 和适用 Behavior。
- 证据强度。
- 新增证据。
- 缺失信号。
- 首次发现与最近变化。

### 16.2 P0：事件详情

- 事件类型和 Narrative。
- 生命周期与结构标签。
- Attention、Behavior、Diversity、Authority、Coverage。
- evidenceMask 与缺失原因。
- 时点证据和 24 小时/7 天结果。
- 事件谱系。
- 原始证据短摘要与链接。
- 状态变更时间线。
- 关注、备注、反馈、合并和拆分。
- 反证、正常时滞和方法限制。

### 16.3 P0：覆盖与连接器

- 信号家族覆盖。
- 连接器健康、延迟、配额和预算。
- 失效数据对结论的影响。
- 当前产品定位是否需要降级。

### 16.4 P0：关注、反馈与告警

- 关注列表。
- 人工接受、拒绝、需观察。
- 通用签名 Webhook。
- 每日告警预算和领域预算。

### 16.5 P1：雷达探索

- 只展示 Attention 与 Behavior 均适用且有效的事件。
- 轨迹表示真实变化。
- 支持按 eventType 分面。
- 不作为唯一首屏或告警排序依据。

### 16.6 P1：信源中心

只有 SourceScore 冻结、循环强化测试通过后开放排行。

### 16.7 视觉范围

V1：

- 桌面深色主题。
- AA 对比度。
- 基础键盘操作。
- 图表数据表替代。
- 移动端查看、筛选、关注和确认告警。

Beta 后：

- 浅色主题。
- 完整移动编辑。
- 复杂传播图键盘交互。

---

## 17. 告警与产品价值

### 17.1 告警预算

默认每工作区：

- 强告警每天最多 10 条。
- 每领域每天最多 3 条。
- 低证据事件只进入队列，不主动推送。
- 同事件没有新增证据或状态升级时不重复告警。

### 17.2 告警内容

- 生命周期与结构标签。
- 新增证据。
- Attention 与适用 Behavior。
- 证据强度和缺失信号。
- 与上周期相比的最小有效变化。
- 事件详情链接。

### 17.3 Beta 产品目标

这些数值是待校准的 Beta 决策目标，不是未经验证的对外承诺。Week 1 先记录人工基线与处理能力，阈值必须在查看冻结测试集结果前以 `productMetricPolicyVersion` 冻结。

| 指标 | 唯一定义 | 初始目标 | 最低判定样本 |
|---|---|---|---|
| 强告警人工接受率 | 一个工作日内标记为“接受”的强告警数 / 已送达且已成熟的强告警数；仍在判断窗口内的告警右删失为 pending；拒绝、未处理均计入分母；系统故障重复件只有在系统基于两条真实投递生成不可变 incident fact 后才可排除并单列 | 不低于 60% | 至少 30 条已成熟强告警 |
| 错误强告警/日 | 经双人复核确认为证据不足、错误聚类或与目标领域无关的强告警，按实际送达工作日计 | 每工作日不超过 3 条，且错误率不高于 30% | 至少 5 个有告警工作日 |
| Precision@5 | 事前冻结纳入规则、排名版本、连续每日时点、bootstrap seed/iterations；独立 score-ledger signer 在每个采样时点签名完整排序账本，调度器只能从该账本机械提取真实 Top5 的 rank、eventId、score、scoreRunId 和账本摘要，双人期末独立标注后跨日汇总 | 点估计不低于 0.70，且 95% bootstrap 区间下界不低于 0.70（比原点估计门槛更严格） | 完整覆盖影子窗口内全部预登记快照，至少 7 个快照、35 个候选槽位 |
| 首次分诊 SLA | 在预先登记的值班时段内，从进入可研判队列到 Analyst 首次做出接受/拒绝/需观察的时长；无值班、故障和重复事件分别报告，不可事后删除 | 80% 不超过 15 分钟 | 至少 50 个可研判事件 |
| 中位有效研判用时 | 只累计认证 Analyst 发出的、服务端按连续 sequence 接收的 `active` 心跳区间；忽略客户端上报的时长汇总值，从同一 QueueEligibility 的首次打开到提交判断跨关闭/重开累计，`idle` 与等待外部数据单列；另以不受客户端 state 缩短的服务端心跳墙钟作异常护栏。该口径不声称能防止持证内部人伪报状态；有效计时必须覆盖至少 95% 的完成研判，否则证据不足 | 不超过 5 分钟 | 至少 50 次完成研判且计时覆盖率不低于 95% |
| 中位发现提前量 | 按事前冻结的机械阈值规则，将事件首次达到 Attention、Coverage 和生命周期组合门槛的事实原子写入版本化 append-only crossing 账本，并由独立 score-ledger signer 输出证据窗内全量候选清单；对完全相同的候选集合，以“独立 baseline 采集器首次记录时间 - 系统首次达到预登记早期信号门槛时间”计算；baseline 日志使用另一角色密钥签名 | 系统中位提前至少 30 分钟 | 独立签名 baseline 与至少 30 个同集事件；无需与 Precision@5 候选集合相同 |

若最低样本不足、区间过宽或人工基线不独立，该指标只能报告为“证据不足”，不能据此宣布 Beta 达标。

### 17.4 正式验收证据治理

- 正式 72H/7D 固定每 15 分钟采样；每个样本包含 scheduler run ID、计划时点和 Ed25519 签名，并形成哈希链。哈希链用于检测编辑，签名用于证明来源，两者不能互相替代。
- 产品策略同时冻结 policy、人工评估 schema、预登记 schema、验收 keyring 和 evaluator 文件摘要；任何一项变化都必须发布新策略版本并重新开始证据窗口。
- keyring 必须分离 scheduler、两名 reviewer、独立 baseline collector 和 score-ledger signer 的密钥；重复 reviewer 公钥或任意跨角色复用密钥时 fail closed。
- 每个采样时点的完整排序账本与阈值跨越事实由 score-ledger signer 签名；每日 Top5 快照必须逐项等于同一账本前五名，其账本摘要和快照签名摘要进入同一时点的监控样本。期末 reviewer 签名不能替代事前预登记、完整排序账本或每日时点承诺。
- PostgreSQL 中的排序事实、阈值跨越事实和 `generatedAt` 必须来自同一个只读 `REPEATABLE READ` 快照，并在读取事实后以数据库时钟生成水位；不得用两个独立查询或查询前的应用时钟拼接账本。
- 产品事实表、QueueEligibility、反馈、投递、incident 和交互片段为追加事实；监控器校验累计计数不下降、相同计数时摘要不变化，正式数据库由 append-only trigger 提供第二道保护。
- 正式模式只接受 PostgreSQL、强制 RLS、受限 `radar_app` 角色、只读迁移标记、关键审计 trigger、认证开启、稳定 instanceId 和数据库时钟偏差不超过 5 秒的运行时证明；InMemory/demo 永远不能通过正式验收。
- 连接器技术健康不等于数据权利批准。连接器 registry 中只有经数据负责人/法务显式批准为 `active` 的来源才可进入正式窗口；当前代码、smoke 或 monitor 报告不能替代签字审批。

---

## 18. API 输出契约

~~~ts
interface Estimate {
  value: number; // 0-100，固定适用权重分母
  applicableWeight: number;
  observedWeight: number;
  contributingFeatures: string[];
  featureProfileVersion: string;
}

interface DecisionReason {
  triggeredRules: string[];
  counterEvidence: string[];
  limitations: string[];
}

interface CoverageGap {
  featureId: string;
  state: "missing" | "untrusted";
  reason: string;
  expectedAvailableAt?: string;
}

interface EventAssessment {
  eventId: string;
  clusterVersion: number;
  eventType: EventType;
  lifecycleState: LifecycleState;
  structureLabels: StructureLabel[];
  attentionEstimate?: Estimate;
  behaviorEstimate?: Estimate;
  behaviorKind?: string;
  diversityEstimate: Estimate;
  authorityEstimate: Estimate;
  coordinationRisk: Estimate;
  coverage: number;
  evidenceStrength: "low" | "medium" | "high";
  uncertaintyIndex: number; // 0-1 的证据不确定性指数，不是概率
  uncertaintyReasons: string[];
  evidenceMask: Record<string, EvidenceState>;
  sampleSize: number;
  baselineMaturity: number;
  decisionReason: DecisionReason;
  missingEvidence: CoverageGap[];
  scoringVersion: string;
  baselineVersion: string;
  evidencePolicyVersion: string;
  labelPolicyVersion: string;
  observedAt: string;
}
~~~

主要端点：

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | /api/v1/review-queue | 待研判队列 |
| GET | /api/v1/events/{id} | 事件详情 |
| GET | /api/v1/events/{id}/assessment | 当前判断 |
| GET | /api/v1/events/{id}/timeline | 指标和状态时间线 |
| GET | /api/v1/events/{id}/evidence | 证据、反证和缺口 |
| GET | /api/v1/events/{id}/lineage | 聚类谱系 |
| POST | /api/v1/events/{id}/merge | 合并 |
| POST | /api/v1/events/{id}/split | 拆分 |
| POST | /api/v1/events/{id}/feedback | 人工反馈 |
| GET | /api/v1/coverage | 信号家族和连接器状态 |
| POST | /api/v1/watchlists | 关注 |
| POST | /api/v1/alert-rules | 告警规则 |
| GET | /api/v1/radar | 二维探索 |
| GET | /api/v1/stream | SSE，失败时轮询 |

---

## 19. 评估协议

### 19.1 两层真值

第一层：时点 t 判断，只允许使用 t 以前的数据。

- 是否异常增长。
- 是否跨平台。
- 是否存在行为确认。
- 是否平台集中。
- 是否存在协同风险。
- 覆盖是否充分。

第二层：未来结果。

- 24 小时后扩散范围。
- 7 天后持续性。
- 7 天后行为采用或响应。
- 是否回落为噪声。

早期判断和未来结果分别评估，不能混成一个标签。

### 19.2 数据集

- 第 1 周：先完成 60 个双人标注事件时点窗口，用于指南和开发；它们属于开发集首批样本，不额外叠加到总数。
- 第 8 周前：至少 240 个事件时点窗口。
- 开发、验证、冻结测试各至少 80 个窗口。
- 测试集来自更晚时间段。
- 同一 Event、Narrative、公司、产品系列或发布系列不得跨集合。
- 至少 30% 来自自然流量抽样，而不是只收集已知热点。

### 19.3 标注

- 两名分析师独立标注。
- 第三人仲裁。
- Cohen's kappa 目标不低于 0.70。
- 每种标签包含定义、正例、反例和证据不足例。

### 19.4 评估指标

分类与排序：

- Precision@K。
- False Alerts per Day。
- 中位发现提前量。
- 每个 eventType 的 Precision/Recall。
- 生命周期状态与结构标签分任务指标。
- 证据强度可靠性校准。

聚类：

- Pairwise Precision/Recall。
- B-cubed。
- 误合并率与误拆分率。
- 跨语言误合并率。

产品：

- 告警接受率。
- 平均研判时间。
- 证据点击与核验率。
- 被关注或转为行动的事件比例。

报告必须包含 bootstrap 置信区间，不能只汇报单点宏平均 F1。

---

## 20. 非功能要求

### 20.1 新鲜度

- 快速发现型 P0 连接器（RSS/HN/GitHub/Hugging Face）95% 的合格新对象在提供方可见后 15 分钟内进入候选发现。
- arXiv/OpenAlex 等批次或回修型来源按连接器 SLA 验收，初始目标为提供方可见后 60 分钟内；不得用论文 `publishedAt` 代替实际可采集时间。
- 新鲜度优先使用 `availableAt -> collectedAt`；提供方不暴露 `availableAt` 时使用首次探测时间，并在报告中标记测量局限。
- confirmed/established 不承诺统一 15 分钟。
- 连接器覆盖下降在一个评分周期内被识别。

### 20.2 性能

- 待研判队列缓存命中 P95 小于 500ms。
- 事件详情 P95 小于 1 秒。
- Hot Event 一轮评分在 5 分钟内完成。

### 20.3 容量

- 120-200 active 信源。
- 500 candidate 信源。
- 2,000 信源真实容量。
- 10,000 信源合成压测。
- 15 分钟快照保留 90 天，小时 1 年，日 2 年。

### 20.4 恢复

- PostgreSQL PITR。
- 核心状态 RPO 不高于 1 小时。
- RTO 4 小时。
- Redis 可由 Outbox、检查点和 R2 恢复。
- 告警发送记录持久化。

### 20.5 成本

2,000 元人民币只作为待验证的外部数据预算上限，不作为可行性结论。

成本必须拆分：

- 新对象发现。
- 指标刷新。
- 历史回补。
- 热点升频。
- embedding。
- LLM 摘要。
- 存储。
- 出站流量。

每个连接器设置日限额、月限额、80% 告警和 100% 降级。降级必须反映到 Coverage。

---

## 21. 资源模型

8 周交付假设以下资源在 Week 1-8 可用：

| 角色 | 人数 | 投入 | 主要责任 |
|---|---:|---:|---|
| 产品/分析负责人 | 1 | 100% | 事件本体、标注、产品决策 |
| 数据/后端工程师 | 2 | 100% | 连接器、存储、队列、API |
| 算法工程师 | 1 | 100% | 聚类、基线、评分、评估 |
| 前端工程师 | 1 | 100% | 待研判、详情、覆盖、告警 |
| QA/DevOps | 1 | 50% | 测试、部署、监控、故障演练 |
| 分析师 | 2 | 各 25% | 双标、影子运行和仲裁准备 |
| 安全/合规 | 1 | 20% | 权利矩阵、安全和删除评审 |

总投入约 6.2 FTE，共 9 人参与。该口径按表中人数乘投入比例计算；产品/分析负责人不替代两名独立标注分析师。

标注产能假设：两名分析师在 Week 1 各临时投入 50%，其余周降低投入，使 8 周平均仍约为各 25%；单人单窗口标注中位时长应不超过 15 分钟。Week 1 必须实测耗时。若中位时长超过 15 分钟、仲裁率超过 20% 或两名分析师无法临时升配，则 240 窗口目标需要增加分析师或延长排期，不能依赖无记录加班。

若核心工程资源少于 5 FTE：

- 不承诺 8 周 Beta。
- 默认延长到 12-16 周。
- 不通过取消证据、回放、安全或评估换取表面按时。

---

## 22. Week 0 与 8 周执行计划

### Week 0：开工门槛，3-5 个工作日

负责人：前端/平台、后端、安全、产品。

任务：

- Sites 身份、FastAPI、SSE/轮询端到端尖峰。
- 替代部署验证。
- 中文源合法路径确认。
- P0 连接器字段权利和成本卡。
- 团队资源和凭证确认。

退出条件：

- 身份和外部 API 可安全闭环，或已选择替代部署。
- P0 连接器无未声明的许可阻断。
- 资源模型获得项目负责人确认。

Week 0 未通过时不得进入正式 8 周承诺。

### Week 1：事件本体与评估协议

负责人：产品/分析、算法、数据。

任务：

- 冻结 5 类 eventType。
- 冻结最低证据组合、正常时滞和生命周期。
- 完成主状态与标签协议。
- 完成开发集首批 60 个双人标注事件时点窗口。
- 冻结 ContentObservation、MetricSnapshot 和谱系。

退出条件：

- 同一案例由不同分析师得到基本一致的时点标签。
- 不适用信号不会降低 Coverage。

### Week 2：三条真实链路

负责人：数据/后端。

任务：

- RSS/官方站。
- Hacker News。
- GitHub。
- R2、检查点、幂等、DLQ、删除和回放。

退出条件：

- 连续运行 72 小时。
- 重复、迟到、删除和限流测试通过。

### Week 3：开发者与研究信号

负责人：数据/后端、算法。

任务：

- Hugging Face。
- arXiv/OpenAlex。
- 类型专用 Behavior 特征。
- 请求成本和预算仪表。

退出条件：

- 至少 4 个独立信号家族可形成最低证据组合。
- 每个连接器成本可预测。

### Week 4：事件聚类 V0

负责人：算法、后端。

任务：

- 硬标识、实体、时间和向量候选。
- Event/Narrative。
- 合并拆分和事件谱系。
- 人工撤销与并发控制。

退出条件：

- 高影响样本 Pairwise Precision 不低于 0.90。
- 所有人工操作可追溯。

### Week 5：评分与覆盖 V0

负责人：算法、产品/分析。

任务：

- Attention、Behavior、Diversity、Authority、Coordination Risk、Coverage。
- 类型专用基线。
- evidenceMask、uncertainty 和 evidenceStrength。
- 正常时滞与 gapResidual。

退出条件：

- 缺失 60% 预期特征时无法产生高证据强度强告警。
- 官方同步发布案例仍可成为确认热点。

### Week 6：研判产品

负责人：前端、后端、产品。

任务：

- 待研判队列。
- 事件详情。
- 覆盖与连接器状态。
- 关注、反馈和审计。
- 桌面深色主题和移动查看。

退出条件：

- 分析师可在 5 分钟内完成一次完整研判。
- 任一结论可查看证据、反证和缺口。

### Week 7：告警与影子运行

负责人：后端、产品/分析、QA。

任务：

- 签名 Webhook。
- 告警预算、冷却和新增证据要求。
- Precision@K、False Alerts/Day 和处理时长。
- 连续 7 天影子运行开始。

退出条件：

- 强告警量可控。
- 故障不会被误判为热度下降。
- 用户不打开网页也能收到告警。

### Week 8：Beta 决策

负责人：全体。

任务：

- 时间与实体隔离测试。
- 安全、删除、恢复和容量测试。
- 产品价值指标。
- 决定是否启用 YouTube、X 或中文实验源。
- 发布或明确延后。

退出条件：

- 满足第 23 节 DoD。
- 未达标项不能用连接器或页面数量抵消。

---

## 23. Definition of Done

### 23.1 产品价值

- 强告警每天不超过 10 条。
- 第 17.3 节的接受率、错误告警、Precision@5、首次分诊和研判用时均按冻结口径采集，并达到最低样本量。
- 强告警人工接受率不低于 60%；Precision@5 点估计与 95% bootstrap 区间下界均不低于 0.70。
- 每工作日错误强告警不超过 3 条且错误率不高于 30%。
- 预登记值班时段内，80% 可研判事件在 15 分钟内完成首次分诊；认证且服务端接收的 `active` 心跳计时覆盖率不低于 95%，跨重开累计后的中位有效研判用时不超过 5 分钟，并同时保留不受 state 缩短的服务端墙钟护栏。
- 若有独立人工基线且样本达标，报告中位发现提前量；没有合格基线不得宣称“比人工更早”。
- 任一核心产品指标最低样本不足时，Beta 决策为延后或“证据不足”，不得按达标处理。
- 正式证据必须通过第 17.4 节的签名、预登记、事实连续性、生产运行时和权利审批闸门；任一闸门缺失即为 NO-GO。

### 23.2 数据与连接器

- 至少 4 个独立信号家族稳定 7 天。
- 至少一个讨论家族和一个行为家族。
- 每个强判断满足事件类型最低证据组合。
- 每个连接器具有成本、字段权利、删除和故障测试。
- 覆盖下降在一个评分周期内发现。

### 23.3 算法与评估

- 测试集按时间、实体和发布系列隔离。
- 报告 Precision@K、错误告警/日、中位提前量和 eventType 分组结果。
- 高影响聚类 Pairwise Precision 不低于 0.90。
- 报告误合并率与误拆分率。
- evidenceStrength 完成可靠性检查。
- Feature Registry、阈值、标签和证据策略在查看冻结测试结果前完成版本冻结。
- 数据、基线或聚类不足时不输出强结论。

### 23.4 工程与恢复

- PostgreSQL Outbox、告警和当前状态一致。
- 合并拆分后历史评分、关注、告警和谱系正确。
- 删除传播覆盖原始对象、摘要、embedding、缓存和索引。
- 核心状态 RPO 不高于 1 小时。
- Redis 丢失可恢复。
- Sites 不满足要求时具有已验证的替代部署。
- 2,000 信源真实容量和 10,000 信源合成压测通过。

### 23.5 产品与安全

- 待研判、事件详情、覆盖、关注和反馈完成。
- 签名 Webhook 完成。
- AA 对比度、基础键盘操作和图表数据表完成。
- 移动端可查看、筛选、关注和确认告警。
- 外部内容净化、SSRF 防护、Prompt Injection 隔离和 Webhook 验证通过。

### 23.6 合规

- 每个连接器完成字段级权利矩阵。
- 未授权来源保持关闭。
- 删除流程通过演练。
- 用户文案不把结构标签描述为事实指控。
- 产品定位与实际信号家族覆盖一致。

---

## 24. 主要风险与降级

| 风险 | 触发条件 | 降级 |
|---|---|---|
| 中文社会化源不可得 | Week 0 无合法路径 | 定位为开发者与研究生态 Beta |
| Sites 身份或 SSE 不满足 | 尖峰失败 | 切换稳定 Web 托管与轮询 |
| YouTube 配额不足 | 成本模型超限 | P1 延后，不进入关键路径 |
| X 成本不可预测 | 未完成读取量模型 | 保持实验性 |
| 评估样本不足 | 冻结集不足或泄漏 | 不发布准确率结论，延后 Beta |
| 告警疲劳 | 错误告警或总量超预算 | 提高证据门槛，只保留队列 |
| 聚类误合并过高 | 高影响 Precision 未达标 | 进入人工确认，不自动合并 |
| 资源不足 | 核心工程少于 5 FTE | 延长到 12-16 周 |

---

## 25. 启动前置条件

- Week 0 技术尖峰负责人。
- 约 6.2 FTE 资源确认。
- 两名分析师与仲裁人。
- P0 API 凭证。
- 中文来源的合法访问说明。
- PostgreSQL、Redis 和 R2 环境。
- 数据预算负责人。
- 安全与合规负责人。
- 删除请求负责人。
- 私有工作区成员名单。

---

## 26. 参考资料

- OpenAI Sites 相关能力以当前本地 Sites 插件和官方帮助中心为准：https://help.openai.com/en/articles/20001339-creating-and-managing-chatgpt-sites
- X API Usage and Billing：https://docs.x.com/x-api/fundamentals/post-cap
- YouTube Data API Quota：https://developers.google.com/youtube/v3/guides/quota_and_compliance_audits
- GitHub REST API Rate Limits：https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
- GitHub REST API Best Practices：https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api
- Hugging Face Hub Rate Limits：https://huggingface.co/docs/hub/rate-limits
- OpenAlex Developers：https://developers.openalex.org/
- arXiv API User Manual：https://info.arxiv.org/help/api/user-manual.html
- Google Trends API Alpha：https://developers.google.com/search/apis/trends
- Bluesky Jetstream：https://docs.bsky.app/blog/jetstream
- BERTrend：https://arxiv.org/abs/2411.05930

---

## 27. 最终结论

修订后的 V1 不再试图在 8 周内证明“已经监控全网”，而是证明一件更重要的事：

> 对有限但可靠的 AI 信号，系统能够按事件类型给出及时、可解释、可回放、对缺失数据诚实的判断，并真正帮助分析师更早、更快地完成研判。

当判断闭环、评估协议、连接器权利和产品价值指标成立后，再扩展到 500 个活跃信源、更多社会平台和更高容量，才是可持续的高上限路线。
