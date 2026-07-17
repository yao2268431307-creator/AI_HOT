from __future__ import annotations

from datetime import datetime
import json
from urllib.parse import quote

from ..contracts import Observation
from ..normalize import content_fingerprint, normalize_url
from .base import BaseConnector, utcnow


class HuggingFaceConnector(BaseConnector):
    id = "huggingface"
    rights_policy_id = "hf-hub-metadata-v1"
    platform = "Hugging Face"
    signal_family = "behavior"
    metered = True

    def __init__(self, search: str = "", *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.search = search

    async def collect(self) -> list[Observation]:
        endpoint = f"https://huggingface.co/api/models?sort=trendingScore&direction=-1&limit=50&search={quote(self.search)}"
        response = await self.get(endpoint)
        collected = utcnow()
        observations: list[Observation] = []
        for model in response.json():
            model_id = model.get("modelId") or model.get("id")
            if not model_id:
                continue
            item_ref = self.item_raw_ref("huggingface", model_id, collected)
            await self.archive(item_ref, json.dumps(model, ensure_ascii=False).encode())
            url = normalize_url(f"https://huggingface.co/{model_id}")
            published_raw = model.get("createdAt") or model.get("lastModified")
            published = datetime.fromisoformat(published_raw.replace("Z", "+00:00")) if published_raw else collected
            tags = model.get("tags") or []
            text = f"{model_id} {' '.join(tags[:20])}"
            observations.append(Observation(
                id=f"hf:{model_id}", platform=self.platform,
                externalId=model_id, sourceId=f"hf:{model_id.split('/')[0]}", publishedAt=published, collectedAt=collected,
                language="en", title=model_id, text=text, url=url,
                metrics={
                    "downloads": float(model.get("downloads", 0)),
                    "likes": float(model.get("likes", 0)),
                },
                rawEvidenceRef=item_ref, relation="original",
                contentFingerprint=content_fingerprint(model_id, text, url), signalFamily="behavior",
            ))
        return observations
