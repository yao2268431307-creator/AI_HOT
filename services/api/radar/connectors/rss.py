from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from defusedxml import ElementTree

from ..contracts import Observation
from ..normalize import content_fingerprint, language_hint, normalize_url
from .base import BaseConnector, ConnectorError, utcnow


def _tag(element: ElementTree.Element, name: str) -> str:
    for child in list(element):
        if child.tag.rsplit("}", 1)[-1].lower() == name.lower():
            return (child.text or "").strip()
    return ""


def _published(value: str) -> datetime:
    if not value:
        return utcnow()
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return utcnow()
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class RSSConnector(BaseConnector):
    id = "rss"
    rights_policy_id = "rss-public-metadata-v1"
    platform = "RSS"
    signal_family = "official"

    def __init__(self, feeds: list[tuple[str, str] | tuple[str, str, str]], *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.feeds = feeds

    async def collect(self) -> list[Observation]:
        observations: list[Observation] = []
        failures: list[str] = []
        successful_feeds = 0
        collected_at = utcnow()
        for feed in self.feeds:
            source_id, feed_url = feed[0], feed[1]
            signal_family = feed[2] if len(feed) == 3 and feed[2] in {"discussion", "official", "research"} else "official"
            try:
                response = await self.get(feed_url)
                if len(response.content) > 5_000_000:
                    raise ConnectorError("RSS document exceeds the 5 MB safety limit")
                batch_ref = self.raw_ref(f"rss/{source_id}/{int(collected_at.timestamp())}.xml")
                await self.archive(batch_ref, response.content, response.headers.get("content-type", "application/xml"))
                root = ElementTree.fromstring(response.content)
            except Exception as exc:
                # Feeds are independent failure domains. Keep healthy feed data
                # instead of discarding the whole batch because one URL or XML
                # document is bad.
                failures.append(f"{source_id}: {str(exc)[:160]}")
                continue
            successful_feeds += 1
            entries = [node for node in root.iter() if node.tag.rsplit("}", 1)[-1].lower() in {"item", "entry"}]
            for entry in entries[:50]:
                try:
                    title = _tag(entry, "title")
                    text = _tag(entry, "description") or _tag(entry, "summary") or _tag(entry, "content")
                    url = _tag(entry, "link")
                    if not url:
                        link = next((child for child in list(entry) if child.tag.rsplit("}", 1)[-1].lower() == "link"), None)
                        url = "" if link is None else link.attrib.get("href", "")
                    if not url:
                        continue
                    url = normalize_url(url)
                    external = _tag(entry, "guid") or _tag(entry, "id") or hashlib.sha1(url.encode()).hexdigest()
                    published = _published(_tag(entry, "pubDate") or _tag(entry, "published") or _tag(entry, "updated"))
                    observations.append(Observation(
                        id=f"rss:{source_id}:{hashlib.sha1(external.encode()).hexdigest()[:16]}", platform=self.platform,
                        externalId=external, sourceId=source_id, publishedAt=published, collectedAt=collected_at,
                        language=language_hint(f"{title} {text}"), title=title or None, text=text or title, url=url,
                        metrics={}, rawEvidenceRef=batch_ref,
                        relation="original", contentFingerprint=content_fingerprint(title, text, url), signalFamily=signal_family,
                    ))
                except Exception as exc:
                    failures.append(f"{source_id}/item: {str(exc)[:160]}")
                    continue
        if self.feeds and successful_feeds == 0:
            raise ConnectorError(f"all RSS feeds failed: {'; '.join(failures[:5])}")
        return observations
