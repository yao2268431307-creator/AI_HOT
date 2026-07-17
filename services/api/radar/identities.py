from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .contracts import Observation


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    source_id: str
    account_id: str
    entity_id: str


class SourceIdentityResolver:
    """Resolve platform accounts to stable person/organisation ownership.

    Ambiguous ownership remains account-local. It is never guessed from names;
    candidate links must be supplied by a reviewed registry.
    """

    def __init__(self, identities: list[SourceIdentity] | None = None) -> None:
        self._by_source = {item.source_id: item for item in identities or []}

    @classmethod
    def from_json_file(cls, path: str | Path) -> "SourceIdentityResolver":
        rows = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(rows, list) or not rows:
            raise ValueError("source identity registry must be a non-empty JSON array")
        identities: list[SourceIdentity] = []
        source_ids: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("source identity entries must be objects")
            values = [row.get("sourceId"), row.get("accountId"), row.get("entityId")]
            if any(not isinstance(value, str) or not value.strip() for value in values):
                raise ValueError("source identity entries require non-empty sourceId/accountId/entityId")
            source_id, account_id, entity_id = (str(value).strip() for value in values)
            if source_id in source_ids:
                raise ValueError(f"duplicate source identity: {source_id}")
            source_ids.add(source_id)
            identities.append(SourceIdentity(source_id, account_id, entity_id))
        return cls(identities)

    def resolve(self, observation: Observation) -> Observation:
        identity = self._by_source.get(observation.source_id)
        if identity:
            return observation.model_copy(update={"account_id": identity.account_id, "entity_id": identity.entity_id})
        account_id = observation.account_id or observation.source_id
        # Unknown ownership is deliberately scoped to the platform account,
        # avoiding accidental merges across people with similar display names.
        entity_id = observation.entity_id or f"account-entity:{observation.platform.lower()}:{account_id}"
        return observation.model_copy(update={"account_id": account_id, "entity_id": entity_id})
