from __future__ import annotations

import json
import os
from dataclasses import dataclass
from enum import IntEnum

from fastapi import Header, HTTPException


class Role(IntEnum):
    VIEWER = 1
    ANALYST = 2
    OWNER = 3


@dataclass(frozen=True, slots=True)
class Principal:
    subject: str
    role: Role
    workspace_id: str


def _keys() -> dict[str, dict[str, str]]:
    raw = os.getenv("RADAR_API_KEYS", "{}")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


async def current_principal(x_api_key: str | None = Header(default=None)) -> Principal:
    if os.getenv("AUTH_REQUIRED", "false").lower() != "true":
        return Principal("local-preview", Role.OWNER, "local-workspace")
    record = _keys().get(x_api_key or "")
    if not record:
        raise HTTPException(401, "valid workspace API key required")
    try:
        role = Role[record.get("role", "VIEWER").upper()]
    except KeyError as exc:
        raise HTTPException(403, "API key has an invalid role") from exc
    workspace_id = record.get("workspaceId")
    if not workspace_id:
        raise HTTPException(403, "API key is not assigned to a workspace")
    return Principal(record.get("subject", "workspace-user"), role, workspace_id)


def require_role(principal: Principal, minimum: Role) -> None:
    if principal.role < minimum:
        raise HTTPException(403, f"{minimum.name.title()} role required")
