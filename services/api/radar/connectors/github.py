from __future__ import annotations

from datetime import datetime
import json

from ..contracts import Observation
from ..normalize import content_fingerprint, normalize_url
from .base import BaseConnector, utcnow


class GitHubConnector(BaseConnector):
    id = "github"
    rights_policy_id = "github-api-metadata-v1"
    platform = "GitHub"
    signal_family = "behavior"
    metered = True
    endpoint = "https://api.github.com/search/repositories"

    def __init__(self, query: str = "topic:artificial-intelligence", token: str | None = None, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.query = query
        self.token = token

    async def collect(self) -> list[Observation]:
        headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        response = await self.get(self.endpoint, params={"q": self.query, "sort": "updated", "order": "desc", "per_page": 50}, headers=headers)
        collected = utcnow()
        observations: list[Observation] = []
        for repo in response.json().get("items", []):
            item_ref = self.item_raw_ref("github", str(repo["id"]), collected)
            await self.archive(item_ref, json.dumps(repo, ensure_ascii=False).encode())
            url = normalize_url(repo["html_url"])
            title = repo["full_name"]
            text = repo.get("description") or title
            observations.append(Observation(
                id=f"github:{repo['id']}", platform=self.platform,
                externalId=str(repo["id"]), sourceId=f"github:{repo['owner']['login']}",
                publishedAt=datetime.fromisoformat(repo["updated_at"].replace("Z", "+00:00")), collectedAt=collected,
                language="en", title=title, text=text, url=url,
                metrics={"stars": float(repo.get("stargazers_count", 0)), "forks": float(repo.get("forks_count", 0)), "issues": float(repo.get("open_issues_count", 0)), "watchers": float(repo.get("subscribers_count", 0))},
                rawEvidenceRef=item_ref, relation="original",
                contentFingerprint=content_fingerprint(title, text, url), signalFamily="behavior",
            ))
        return observations
