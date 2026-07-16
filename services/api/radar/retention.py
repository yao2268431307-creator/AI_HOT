from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from .evidence_store import RawEvidenceStore
from .storage import InMemoryRepository, PostgresRepository


def load_retention_days() -> dict[str, int]:
    configured = os.getenv("RIGHTS_POLICIES_PATH")
    path = Path(configured) if configured else Path(__file__).resolve().parents[3] / "config" / "rights_policies.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    values = {policy_id: int(policy["rawRetentionDays"]) for policy_id, policy in payload["policies"].items()}
    if not values or any(days < 0 for days in values.values()):
        raise ValueError("rights policy retention must be non-negative")
    return values


class RawEvidenceRetentionWorker:
    def __init__(self, repository: InMemoryRepository | PostgresRepository, objects: RawEvidenceStore, retention_days: dict[str, int] | None = None) -> None:
        self.repository = repository
        self.objects = objects
        self.retention_days = retention_days or load_retention_days()

    async def run_once(self, at: datetime | None = None) -> int:
        current = at or datetime.now(timezone.utc)
        references = self.repository.expire_raw_evidence(self.retention_days, current)
        completed = 0
        # Confirm independently. A poison object is retried with backoff and
        # cannot block unrelated deletions or be mistaken for a success.
        for reference in references:
            try:
                await self.objects.delete_many([reference])
            except Exception as exc:  # noqa: BLE001 - durable queue owns retries
                self.repository.fail_raw_evidence_deletion(reference, str(exc), current)
                continue
            self.repository.confirm_raw_evidence_deletions([reference])
            completed += 1
        return completed
