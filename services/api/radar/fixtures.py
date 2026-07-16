from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .contracts import ConnectorStatus, Evidence, EvidenceState, EventType, LifecycleState, MetricPoint, RadarEvent, StructureLabel
from .storage import InMemoryRepository


NOW = datetime.now(timezone.utc)


def ago(hours: float) -> datetime:
    return NOW - timedelta(hours=hours)


def timeline(attention: list[int], behavior: list[int]) -> list[MetricPoint]:
    count = max(len(attention), len(behavior))
    return [MetricPoint(at=ago(count - index - 1), attention=a, behavior=b) for index, (a, b) in enumerate(zip(attention, behavior, strict=True))]


def evidence(prefix: str, platforms: list[tuple[str, str, str]]) -> list[Evidence]:
    return [Evidence(
        id=f"{prefix}-e{index}", source=source, platform=platform, title=title,
        url={"Official": "https://example.com/official", "GitHub": "https://github.com/", "Hugging Face": "https://huggingface.co/", "HN": "https://news.ycombinator.com/", "arXiv": "https://arxiv.org/", "YouTube": "https://youtube.com/"}.get(platform, "https://example.com/evidence"),
        publishedAt=ago(5 - index * 0.6), kind=("official" if platform == "Official" else "behavior" if platform in {"GitHub", "Hugging Face", "YouTube"} else "research" if platform == "arXiv" else "discussion"),
        excerpt=f"{source} 提供可复核的原始观测，已完成 URL 规范化和内容指纹去重。",
    ) for index, (source, platform, title) in enumerate(platforms, 1)]


def demo_events() -> list[RadarEvent]:
    events = [
        RadarEvent(
            id="evt-open-model", narrativeId="nar-open-multimodal", narrativeTitle="开放权重多模态模型生态", title="开放权重多模态模型发布引发跨平台采用", titleEn="Open-weight multimodal model sees cross-platform adoption",
            eventType=EventType.MODEL_RELEASE, state=LifecycleState.ACCELERATING,
            labels=[StructureLabel.CROSS_PLATFORM_CONFIRMED, StructureLabel.ADOPTION_CONFIRMED, StructureLabel.OFFICIAL_SOURCE_LED],
            attention=84, behavior=78, diversity=76, authority=88, coordinationRisk=18, coverage=86, uncertainty=12, evidenceStrength="high", evidenceScore=90,
            velocity=22, gapResidual=4, firstSeen=ago(5.7), updatedAt=ago(.1), independentSources=43,
            platforms=["Official", "GitHub", "Hugging Face", "HN"],
            driver="讨论或采用行为连续两个周期保持正增长；增长来自多个独立平台。", coverageNote="四个独立信号家族均有数据。",
            timeline=timeline([28, 39, 48, 61, 73, 80, 84], [22, 31, 43, 55, 67, 73, 78]),
            evidence=evidence("model", [("Model Lab", "Official", "官方模型卡与发布说明"), ("Repository", "GitHub", "仓库采用增速异常"), ("Hub", "Hugging Face", "下载与衍生模型上升"), ("Developers", "HN", "独立讨论者扩散")]),
        ),
        RadarEvent(
            id="evt-agent-campaign", narrativeId="nar-agent-platforms", narrativeTitle="自主智能体平台生态", title="“自主智能体平台”密集宣发但开发者采用未跟进", titleEn="Agent platform campaign lacks adoption follow-through",
            eventType=EventType.DEVELOPER_TOOL_RELEASE, state=LifecycleState.EMERGING,
            labels=[StructureLabel.ATTENTION_BEHAVIOR_GAP, StructureLabel.COORDINATION_RISK, StructureLabel.LOW_SOURCE_DIVERSITY],
            attention=81, behavior=34, diversity=42, authority=58, coordinationRisk=78, coverage=79, uncertainty=21, evidenceStrength="high", evidenceScore=75,
            velocity=12, gapResidual=36, firstSeen=ago(8.2), updatedAt=ago(.3), independentSources=18,
            platforms=["Official", "RSS", "GitHub"], driver="讨论远高于同类工具的预期采用，且首轮内容发布时间与文案高度重合。",
            coverageNote="讨论与代码行为可见，付费转化不可见。", timeline=timeline([44, 58, 70, 76, 79, 81], [20, 22, 25, 28, 31, 34]),
            evidence=evidence("campaign", [("Vendor", "Official", "产品发布说明"), ("Feeds", "RSS", "同文案集中发布"), ("Repository", "GitHub", "采用低于同类基线")]),
        ),
        RadarEvent(
            id="evt-benchmark", narrativeId="nar-reasoning-evals", narrativeTitle="推理模型评测与复现", title="新推理基准结果在研究者圈层快速扩散", titleEn="Reasoning benchmark spreads through research circles",
            eventType=EventType.RESEARCH_OR_BENCHMARK, state=LifecycleState.EMERGING,
            labels=[StructureLabel.EXPECTED_BEHAVIOR_LAG, StructureLabel.OFFICIAL_SOURCE_LED],
            attention=62, behavior=41, diversity=67, authority=82, coordinationRisk=12, coverage=71, uncertainty=27, evidenceStrength="high", evidenceScore=73,
            velocity=18, gapResidual=8, firstSeen=ago(3.7), updatedAt=ago(.2), independentSources=11,
            platforms=["arXiv", "OpenAlex", "GitHub"], driver="高领先度研究信源在两个独立群体中讨论；采用行为处于正常滞后窗口。",
            coverageNote="研究与代码源可见，社交讨论覆盖有限。", timeline=timeline([18, 27, 39, 48, 56, 62], [10, 14, 21, 28, 35, 41]),
            evidence=evidence("benchmark", [("Authors", "arXiv", "论文与可复现协议"), ("Research graph", "OpenAlex", "独立作者网络扩散"), ("Reproduction", "GitHub", "复现实验开始增长")]),
        ),
        RadarEvent(
            id="evt-video-spike", narrativeId="nar-ai-video-demos", narrativeTitle="AI 视频生成演示", title="单平台 AI 视频演示播放量异常脉冲", titleEn="Single-platform AI video demo spike",
            eventType=EventType.OFFICIAL_PRODUCT_RELEASE, state=LifecycleState.DETECTED,
            labels=[StructureLabel.PLATFORM_CONCENTRATED, StructureLabel.LOW_SOURCE_DIVERSITY],
            attention=38, behavior=76, diversity=21, authority=33, coordinationRisk=30, coverage=68, uncertainty=32, evidenceStrength="medium", evidenceScore=64,
            velocity=8, gapResidual=-31, firstSeen=ago(2.6), updatedAt=ago(.1), independentSources=4,
            platforms=["YouTube"], driver="87% 行为增长集中在单平台，独立讨论者与跨平台迁移未同步增加。",
            coverageNote="视频数据完整，其他平台未发现对应增长。", timeline=timeline([17, 21, 26, 31, 35, 38], [20, 39, 58, 70, 75, 76]),
            evidence=evidence("video", [("Video", "YouTube", "播放进入推荐脉冲"), ("Cross check", "RSS", "其他平台无迁移")]),
        ),
        RadarEvent(
            id="evt-security", narrativeId="nar-inference-security", narrativeTitle="推理框架供应链安全", title="主流推理框架披露高危供应链漏洞", titleEn="Critical supply-chain issue in inference framework",
            eventType=EventType.SECURITY_INCIDENT, state=LifecycleState.ACCELERATING,
            labels=[StructureLabel.CROSS_PLATFORM_CONFIRMED, StructureLabel.OFFICIAL_SOURCE_LED, StructureLabel.ADOPTION_CONFIRMED],
            attention=74, behavior=69, diversity=71, authority=93, coordinationRisk=7, coverage=91, uncertainty=9, evidenceStrength="high", evidenceScore=94,
            velocity=29, gapResidual=2, firstSeen=ago(1.9), updatedAt=ago(.05), independentSources=27,
            platforms=["Official", "GitHub", "HN", "RSS"], driver="官方公告、补丁采用与开发者讨论同步增长，影响跨越多个生态。",
            coverageNote="安全公告、仓库事件和技术讨论均可核验。", timeline=timeline([21, 34, 48, 61, 69, 74], [16, 27, 39, 55, 63, 69]),
            evidence=evidence("security", [("Maintainer", "Official", "官方安全公告"), ("Repositories", "GitHub", "依赖项目采用补丁"), ("Developers", "HN", "维护者交叉验证")]),
        ),
    ]
    enriched: list[RadarEvent] = []
    for event in events:
        families = list(dict.fromkeys(item.kind for item in event.evidence))
        enriched.append(event.model_copy(update={
            "discussion_evidence_state": EvidenceState.OBSERVED if "discussion" in families else EvidenceState.MISSING,
            "behavior_evidence_state": EvidenceState.OBSERVED if "behavior" in families else EvidenceState.MISSING,
            "signal_families": families,
            "evidence_count": len(event.evidence),
        }))
    return enriched


def seed_repository(repository: InMemoryRepository) -> InMemoryRepository:
    for event in demo_events():
        repository.upsert_event(event)
        for item in event.evidence:
            repository.register_source_candidate(
                source_id=item.source.lower().replace(" ", "-"), display_name=item.source,
                platform=item.platform, language="other", observed_at=item.published_at,
                reason="recorded_demo_evidence",
            )
    connectors = [
        ("rss", "RSS / 官方站点", "official", "healthy", 3, 1284, 94, "142 个订阅源，正文指纹去重已启用", None, None, 0, "pending"),
        ("hn", "Hacker News", "discussion", "healthy", 2, 842, 98, "官方 API；评论作者按独立讨论者计算", None, None, 0, "pending"),
        ("github", "GitHub", "behavior", "healthy", 6, 3621, 91, "仓库事件、搜索与指标快照", 1840, 5000, 0, "pending"),
        ("hf", "Hugging Face", "behavior", "healthy", 7, 1047, 89, "Hub API；下载、点赞和衍生关系", None, None, 0, "pending"),
        ("research", "arXiv / OpenAlex", "research", "healthy", 11, 516, 86, "跨语言实体与作者网络归并", None, None, 0, "pending"),
        ("youtube", "YouTube", "behavior", "degraded", 19, 431, 58, "配额降频中；结论自动降低置信度", 7200, 10000, 0, "blocked"),
        ("x", "X Recent Search", "discussion", "paused", 0, 0, 0, "实验连接器，未配置商业数据授权", None, None, None, "blocked"),
    ]
    for connector_id, name, family, status, latency, count, coverage, note, quota_used, quota_limit, cost, rights_status in connectors:
        repository.upsert_connector(ConnectorStatus(id=connector_id, name=name, family=family, status=status, latencyMinutes=latency, observations24h=count, coverage=coverage, lastSuccess=ago(.1 if status == "healthy" else 1), note=note, quotaUsed=quota_used, quotaLimit=quota_limit, costRmbMonth=cost, rightsStatus=rights_status))
    return repository
