from __future__ import annotations

import json
import os
import hashlib
from pathlib import Path

from .contracts import EventType


REQUIRED_EFFECTIVE_FIELDS = {
    "featureId", "eventTypes", "signalCategory", "inputMetric", "role", "unit", "direction",
    "windows", "platformFamily", "entityDedupeLevel", "baselineKey", "weight", "familyCap",
    "minimumSample", "normalDelayHours", "revisionPolicy", "testVectors",
}
ALLOWED_ROLES = {"primary_adoption", "primary_response", "secondary_intent", "attention_only"}


def registry_path() -> Path:
    configured = os.getenv("FEATURE_REGISTRY_PATH")
    return Path(configured) if configured else Path(__file__).resolve().parents[3] / "config" / "feature_registry.json"


def load_feature_registry() -> dict[str, object]:
    payload = json.loads(registry_path().read_text(encoding="utf-8"))
    if not payload.get("frozen") or not payload.get("registryVersion"):
        raise ValueError("feature registry must be versioned and frozen")
    defaults = payload.get("defaults", {})
    effective: list[dict[str, object]] = []
    seen_ids: set[str] = set()
    seen_keys: set[tuple[str, str]] = set()
    for raw in payload.get("features", []):
        item = {**defaults, **raw}
        missing = REQUIRED_EFFECTIVE_FIELDS - set(item)
        if missing:
            raise ValueError(f"feature {item.get('featureId')} is missing registry fields: {sorted(missing)}")
        feature_id = str(item["featureId"])
        if feature_id in seen_ids:
            raise ValueError(f"duplicate featureId: {feature_id}")
        seen_ids.add(feature_id)
        if item["role"] not in ALLOWED_ROLES:
            raise ValueError(f"feature {feature_id} has an invalid role")
        weight = float(item["weight"])
        family_cap = float(item["familyCap"])
        if not 0 <= weight <= 1 or not 0 <= family_cap <= 1:
            raise ValueError(f"feature {feature_id} has invalid weight/familyCap")
        for event_type in item["eventTypes"]:
            EventType(event_type)
            key = (str(event_type), str(item["inputMetric"]))
            if item["signalCategory"] == "behavior" and key in seen_keys:
                raise ValueError(f"duplicate behavior feature registration: {key}")
            if item["signalCategory"] == "behavior":
                seen_keys.add(key)
        effective.append(item)
    if not effective:
        raise ValueError("feature registry has no features")
    for event_type in EventType:
        scoring = [
            item for item in effective
            if event_type.value in item["eventTypes"]
            and item["signalCategory"] == "behavior"
            and item["role"] != "attention_only"
            and float(item["weight"]) > 0
        ]
        total_weight = sum(float(item["weight"]) for item in scoring)
        if abs(total_weight - 1.0) > 1e-9:
            raise ValueError(f"{event_type.value} behavior profile weights must sum to 1.0")
        families: dict[str, list[dict[str, object]]] = {}
        for item in scoring:
            families.setdefault(str(item["platformFamily"]), []).append(item)
        for family, items in families.items():
            caps = {float(item["familyCap"]) for item in items}
            if len(caps) != 1 or sum(float(item["weight"]) for item in items) - next(iter(caps)) > 1e-9:
                raise ValueError(f"{event_type.value}/{family} exceeds or disagrees on familyCap")
    return {**payload, "features": effective}


def feature_registry_identity() -> tuple[str, str]:
    """Return the frozen registry version and a digest of its effective rules."""
    payload = load_feature_registry()
    material = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return str(payload["registryVersion"]), f"sha256:{hashlib.sha256(material.encode()).hexdigest()}"


def behavior_metric_roles() -> dict[EventType, dict[str, str]]:
    roles: dict[EventType, dict[str, str]] = {event_type: {} for event_type in EventType}
    for item in load_feature_registry()["features"]:
        if item["signalCategory"] != "behavior":
            continue
        for raw_event_type in item["eventTypes"]:
            roles[EventType(raw_event_type)][str(item["inputMetric"])] = str(item["role"])
    if any(not values for values in roles.values()):
        raise ValueError("every supported event type requires registered behavior features")
    return roles


def behavior_feature_rules() -> dict[EventType, dict[str, dict[str, object]]]:
    rules: dict[EventType, dict[str, dict[str, object]]] = {event_type: {} for event_type in EventType}
    for item in load_feature_registry()["features"]:
        if item["signalCategory"] != "behavior":
            continue
        for raw_event_type in item["eventTypes"]:
            rules[EventType(raw_event_type)][str(item["inputMetric"])] = dict(item)
    if any(not values for values in rules.values()):
        raise ValueError("every supported event type requires registered behavior features")
    return rules


def normal_behavior_delay_hours() -> dict[EventType, tuple[float, float]]:
    """Return the effective min/max normal-delay window from the frozen registry."""
    windows: dict[EventType, list[tuple[float, float]]] = {event_type: [] for event_type in EventType}
    for item in load_feature_registry()["features"]:
        if item["signalCategory"] != "behavior" or item["role"] == "attention_only":
            continue
        raw = item["normalDelayHours"]
        if not isinstance(raw, list) or len(raw) != 2:
            raise ValueError(f"feature {item['featureId']} has invalid normalDelayHours")
        low, high = float(raw[0]), float(raw[1])
        if low < 0 or high < low:
            raise ValueError(f"feature {item['featureId']} has invalid normalDelayHours")
        for raw_event_type in item["eventTypes"]:
            windows[EventType(raw_event_type)].append((low, high))
    if any(not values for values in windows.values()):
        raise ValueError("every supported event type requires a normal behavior delay window")
    return {
        event_type: (min(value[0] for value in values), max(value[1] for value in values))
        for event_type, values in windows.items()
    }
