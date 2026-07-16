from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlsplit

from .contracts import Observation
from .embeddings import cosine_similarity
from .normalize import canonical_text, normalize_url


TOKEN_PATTERN = re.compile(r"[\w\u4e00-\u9fff]+", re.UNICODE)
ENTITY_PATTERN = re.compile(r"\b(?:GPT-?\d+(?:\.\d+)?|Claude|Gemini|Llama|Qwen|DeepSeek|Mistral|OpenAI|Anthropic|Hugging\s*Face)\b", re.I)


@dataclass(slots=True)
class ClusterCandidate:
    event_id: str
    title: str
    urls: set[str]
    entities: set[str]
    latest_at: datetime
    embedding: list[float] | None = None


@dataclass(slots=True)
class ClusterDecision:
    event_id: str | None
    score: float
    reasons: list[str]
    create_new: bool


def tokens(value: str) -> set[str]:
    return {token.lower() for token in TOKEN_PATTERN.findall(canonical_text(value)) if len(token) > 1}


def entities(value: str) -> set[str]:
    return {match.group(0).lower().replace(" ", "") for match in ENTITY_PATTERN.finditer(value)}


def jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def cluster_score(observation: Observation, candidate: ClusterCandidate, observation_embedding: list[float] | None = None) -> tuple[float, list[str]]:
    reasons: list[str] = []
    title_text = f"{observation.title or ''} {observation.text}"
    semantic = cosine_similarity(observation_embedding, candidate.embedding) if observation_embedding and candidate.embedding else max(
        jaccard(tokens(title_text), tokens(candidate.title)),
        jaccard(tokens(observation.title or ""), tokens(candidate.title)),
    )
    entity = jaccard(entities(title_text), candidate.entities)
    normalized_url = normalize_url(observation.url)
    same_url = normalized_url in candidate.urls
    same_host = any(urlsplit(url).hostname == urlsplit(normalized_url).hostname for url in candidate.urls)
    hours = abs((observation.published_at - candidate.latest_at).total_seconds()) / 3600
    temporal = max(0.0, 1.0 - hours / 72.0)
    score = semantic * 0.48 + entity * 0.22 + temporal * 0.12 + (0.18 if same_url else 0.05 if same_host else 0.0)
    if semantic >= 0.45:
        reasons.append("标题与正文语义词高度重合")
    if entity > 0:
        reasons.append("共享模型、公司或产品实体")
    if same_url:
        reasons.append("共享规范化 URL")
    elif same_host:
        reasons.append("共享来源域名")
    if hours <= 12:
        reasons.append("发布时间接近")
    return min(1.0, score), reasons


def choose_cluster(observation: Observation, candidates: list[ClusterCandidate], threshold: float = 0.62, observation_embedding: list[float] | None = None) -> ClusterDecision:
    ranked = [(candidate, *cluster_score(observation, candidate, observation_embedding)) for candidate in candidates]
    if not ranked:
        return ClusterDecision(event_id=None, score=0.0, reasons=["没有候选事件簇"], create_new=True)
    best, score, reasons = max(ranked, key=lambda item: item[1])
    return ClusterDecision(event_id=best.event_id if score >= threshold else None, score=round(score, 4), reasons=reasons, create_new=score < threshold)
