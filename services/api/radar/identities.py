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
        if not isinstance(rows, list):
            raise ValueError("source identity registry must be a JSON array")
        return cls([SourceIdentity(row["sourceId"], row["accountId"], row["entityId"]) for row in rows])

    def resolve(self, observation: Observation) -> Observation:
        identity = self._by_source.get(observation.source_id)
        if identity:
            return observation.model_copy(update={"account_id": identity.account_id, "entity_id": identity.entity_id})
        account_id = observation.account_id or observation.source_id
        # Unknown ownership is deliberately scoped to the platform account,
        # avoiding accidental merges across people with similar display names.
        entity_id = observation.entity_id or f"account-entity:{observation.platform.lower()}:{account_id}"
        return observation.model_copy(update={"account_id": account_id, "entity_id": entity_id})
