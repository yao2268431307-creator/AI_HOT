from __future__ import annotations

import hashlib
from datetime import datetime
from urllib.parse import quote

from defusedxml import ElementTree

from ..contracts import Observation
from ..normalize import content_fingerprint, language_hint, normalize_url
from .base import BaseConnector, ConnectorError, utcnow


def _text(entry: ElementTree.Element, name: str) -> str:
    for child in list(entry):
        if child.tag.rsplit("}", 1)[-1] == name:
            return " ".join((child.text or "").split())
    return ""


class ArxivConnector(BaseConnector):
    id = "arxiv"
    access_class = "public_no_billing"
    rights_policy_id = "arxiv-metadata-v1"
    platform = "arXiv"
    signal_family = "research"

    def __init__(self, query: str = "cat:cs.AI OR cat:cs.CL OR cat:cs.LG", *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.query = query

    async def collect(self) -> list[Observation]:
        url = f"https://export.arxiv.org/api/query?search_query={quote(self.query)}&sortBy=submittedDate&sortOrder=descending&max_results=50"
        response = await self.get(url)
        if len(response.content) > 5_000_000:
            raise ConnectorError("arXiv response exceeds the 5 MB safety limit")
        collected = utcnow()
        root = ElementTree.fromstring(response.content)
        entries = [node for node in list(root) if node.tag.rsplit("}", 1)[-1] == "entry"]
        observations: list[Observation] = []
        for entry in entries:
            external_url = _text(entry, "id")
            if not external_url:
                continue
            external_id = external_url.rstrip("/").rsplit("/", 1)[-1]
            item_ref = self.item_raw_ref("arxiv", external_id, collected, "xml")
            await self.archive(item_ref, ElementTree.tostring(entry, encoding="utf-8"), "application/atom+xml")
            title = _text(entry, "title")
            summary = _text(entry, "summary")
            published_raw = _text(entry, "published") or _text(entry, "updated")
            published = datetime.fromisoformat(published_raw.replace("Z", "+00:00")) if published_raw else collected
            authors = [_text(child, "name") for child in list(entry) if child.tag.rsplit("}", 1)[-1] == "author"]
            source_id = f"arxiv:{hashlib.sha1('|'.join(authors).encode()).hexdigest()[:16]}"
            canonical = normalize_url(f"https://arxiv.org/abs/{external_id}")
            observations.append(Observation(
                id=f"arxiv:{external_id}", platform=self.platform, externalId=external_id, sourceId=source_id,
                publishedAt=published, collectedAt=collected, language=language_hint(f"{title} {summary}"),
                title=title, text=summary or title, url=canonical, metrics={}, rawEvidenceRef=item_ref,
                relation="original", contentFingerprint=content_fingerprint(title, summary, canonical), signalFamily="research",
            ))
        return observations
