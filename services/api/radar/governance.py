from __future__ import annotations

from functools import lru_cache
import json
import os
from pathlib import Path


def connector_registry_path() -> Path:
    configured = os.getenv("CONNECTOR_REGISTRY_FILE")
    return Path(configured) if configured else Path(__file__).resolve().parents[3] / "config" / "connector_registry.json"


@lru_cache(maxsize=1)
def connector_registry() -> dict[str, dict[str, object]]:
    payload = json.loads(connector_registry_path().read_text(encoding="utf-8"))
    return {str(row["id"]): row for row in payload.get("connectors", []) if isinstance(row, dict) and row.get("id")}


def connector_rights_status(connector_id: str) -> str:
    """Return the version-controlled registry decision; missing/ambiguous entries fail closed."""
    registry = connector_registry()
    ids = ["arxiv", "openalex"] if connector_id == "research" else [connector_id]
    statuses = [registry.get(item_id, {}).get("rightsStatus") for item_id in ids]
    if statuses and all(value == "active" for value in statuses):
        return "active"
    if any(value == "blocked" for value in statuses) or any(value is None for value in statuses):
        return "blocked"
    if any(value == "experimental" for value in statuses):
        return "experimental"
    return "pending"
