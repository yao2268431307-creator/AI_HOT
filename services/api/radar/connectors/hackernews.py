from __future__ import annotations

from datetime import datetime, timezone
import json

from ..contracts import Observation
from ..normalize import content_fingerprint, language_hint, normalize_url
from .base import BaseConnector, utcnow


class HackerNewsConnector(BaseConnector):
    id = "hackernews"
    access_class = "public_no_billing"
    rights_policy_id = "hn-official-api-v1"
    platform = "HN"
    signal_family = "discussion"
    base_url = "https://hacker-news.firebaseio.com/v0"

    def __init__(self, *args: object, max_items: int = 30, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.max_items = max_items

    async def collect(self) -> list[Observation]:
        ids = (await self.get(f"{self.base_url}/newstories.json")).json()[: self.max_items]
        collected = utcnow()
        observations: list[Observation] = []
        for item_id in ids:
            item_response = await self.get(f"{self.base_url}/item/{item_id}.json")
            item = item_response.json()
            if not item or item.get("deleted") or item.get("dead") or item.get("type") != "story":
                continue
            title = item.get("title", "")
            text = item.get("text", "") or title
            url = normalize_url(item.get("url") or f"https://news.ycombinator.com/item?id={item_id}")
            published = datetime.fromtimestamp(item.get("time", collected.timestamp()), tz=timezone.utc)
            date_path = collected.strftime("%Y/%m/%d")
            raw_ref = self.raw_ref(f"hn/{date_path}/{item_id}/{int(collected.timestamp())}.json")
            await self.archive(raw_ref, json.dumps(item, ensure_ascii=False).encode())
            observations.append(Observation(
                id=f"hn:{item_id}", platform=self.platform, externalId=str(item_id), sourceId=f"hn:{item.get('by','unknown')}",
                publishedAt=published, collectedAt=collected, language=language_hint(f"{title} {text}"), title=title,
                text=text, url=url, metrics={"score": float(item.get("score", 0)), "comments": float(item.get("descendants", 0))},
                rawEvidenceRef=raw_ref, relation="original",
                contentFingerprint=content_fingerprint(title, text, url), signalFamily="discussion",
            ))
        return observations
