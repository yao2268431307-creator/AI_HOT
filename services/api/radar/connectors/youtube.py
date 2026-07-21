from __future__ import annotations

from datetime import datetime
import json

from ..contracts import Observation
from ..normalize import content_fingerprint, language_hint, normalize_url
from .base import BaseConnector, ConnectorError, utcnow


class YouTubeConnector(BaseConnector):
    id = "youtube"
    access_class = "metered"
    rights_policy_id = "youtube-api-review-required-v1"
    platform = "YouTube"
    signal_family = "behavior"
    metered = True
    expected_requests_per_collect = 2

    def __init__(self, api_key: str, query: str = "AI model", *args: object, **kwargs: object) -> None:
        if not api_key:
            raise ConnectorError("YouTube API key is required")
        super().__init__(*args, **kwargs)
        self.api_key = api_key
        self.query = query

    async def collect(self) -> list[Observation]:
        search = await self.get("https://www.googleapis.com/youtube/v3/search", params={
            "part": "snippet", "q": self.query, "type": "video", "order": "date", "maxResults": 25, "key": self.api_key,
        })
        items = search.json().get("items", [])
        ids = [item.get("id", {}).get("videoId") for item in items if item.get("id", {}).get("videoId")]
        if not ids:
            return []
        stats = await self.get("https://www.googleapis.com/youtube/v3/videos", params={
            "part": "statistics,snippet", "id": ",".join(ids), "key": self.api_key,
        })
        collected = utcnow()
        observations: list[Observation] = []
        for video in stats.json().get("items", []):
            video_id = video["id"]
            item_ref = self.item_raw_ref("youtube", video_id, collected)
            await self.archive(item_ref, json.dumps(video, ensure_ascii=False).encode())
            snippet = video.get("snippet", {})
            statistics = video.get("statistics", {})
            title = snippet.get("title", "")
            text = snippet.get("description", "") or title
            url = normalize_url(f"https://www.youtube.com/watch?v={video_id}")
            published = datetime.fromisoformat(snippet["publishedAt"].replace("Z", "+00:00")) if snippet.get("publishedAt") else collected
            observations.append(Observation(
                id=f"youtube:{video_id}", platform=self.platform,
                externalId=video_id, sourceId=f"youtube:{snippet.get('channelId','unknown')}", publishedAt=published,
                collectedAt=collected, language=language_hint(f"{title} {text}"), title=title, text=text, url=url,
                metrics={"views": float(statistics.get("viewCount", 0)), "likes": float(statistics.get("likeCount", 0)), "comments": float(statistics.get("commentCount", 0))},
                rawEvidenceRef=item_ref, relation="original",
                contentFingerprint=content_fingerprint(title, text, url), signalFamily="behavior",
            ))
        return observations
