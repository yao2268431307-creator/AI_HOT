from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from urllib.parse import quote

from ..contracts import Observation
from ..normalize import content_fingerprint, language_hint, normalize_url
from .base import BaseConnector, utcnow


class OpenAlexConnector(BaseConnector):
    id = "openalex"
    rights_policy_id = "openalex-metadata-v1"
    platform = "OpenAlex"
    signal_family = "research"
    metered = True

    def __init__(
        self,
        search: str = "artificial intelligence",
        api_key: str | None = None,
        mailto: str | None = None,
        *args: object,
        **kwargs: object,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.search = search
        self.api_key = api_key
        self.mailto = mailto

    async def collect(self) -> list[Observation]:
        endpoint = f"https://api.openalex.org/works?search={quote(self.search)}&sort=publication_date:desc&per-page=50"
        if self.api_key:
            endpoint += f"&api_key={quote(self.api_key)}"
        if self.mailto:
            endpoint += f"&mailto={quote(self.mailto)}"
        response = await self.get(endpoint)
        collected = utcnow()
        observations: list[Observation] = []
        for work in response.json().get("results", []):
            work_id = work.get("id", "").rsplit("/", 1)[-1]
            if not work_id:
                continue
            first_authorship = (work.get("authorships") or [{}])[0] or {}
            author = first_authorship.get("author") or {}
            author_id = (author.get("id") or "unknown").rsplit("/", 1)[-1]
            item_ref = self.item_raw_ref("openalex", work_id, collected)
            await self.archive(item_ref, json.dumps(work, ensure_ascii=False).encode())
            primary = work.get("primary_location") or {}
            url = normalize_url(primary.get("landing_page_url") or work.get("doi") or work.get("id"))
            title = work.get("title") or "Untitled research work"
            concepts = [topic.get("display_name", "") for topic in (work.get("topics") or [])]
            text = f"{title} {' '.join(concepts)}"
            published = (
                datetime.fromisoformat(work["publication_date"]).replace(tzinfo=timezone.utc)
                if work.get("publication_date")
                else collected
            )
            if published > collected + timedelta(days=1):
                published = collected
            observations.append(Observation(
                id=f"openalex:{work_id}", platform=self.platform, externalId=work_id,
                sourceId=f"openalex:{author_id}",
                publishedAt=published, collectedAt=collected, language=language_hint(title), title=title, text=text, url=url,
                metrics={"citations": float(work.get("cited_by_count", 0))},
                rawEvidenceRef=item_ref, relation="original",
                contentFingerprint=content_fingerprint(title, text, url), signalFamily="research",
            ))
        return observations
