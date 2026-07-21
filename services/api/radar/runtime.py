from __future__ import annotations

from enum import Enum
import ipaddress
import os
from urllib.parse import urlsplit


class RuntimeProfile(str, Enum):
    DEMO = "demo"
    LOCAL = "local"
    PRODUCTION = "production"


def runtime_profile() -> RuntimeProfile:
    """Resolve the explicit profile, with one-release DEMO_MODE compatibility."""
    configured = os.getenv("RUNTIME_PROFILE", "").strip().lower()
    if configured:
        try:
            return RuntimeProfile(configured)
        except ValueError as exc:
            raise RuntimeError("RUNTIME_PROFILE must be demo, local, or production") from exc
    return (
        RuntimeProfile.PRODUCTION
        if os.getenv("DEMO_MODE", "true").lower() == "false"
        else RuntimeProfile.DEMO
    )


def _loopback_host(value: str) -> bool:
    host = value.strip().strip("[]").lower()
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def validate_local_access_configuration() -> None:
    """Fail closed when an unauthenticated local profile could be exposed."""
    if runtime_profile() is not RuntimeProfile.LOCAL:
        return
    if os.getenv("AUTH_REQUIRED", "false").lower() == "true":
        return
    api_host = os.getenv("RADAR_API_HOST", "127.0.0.1")
    web_host = os.getenv("RADAR_WEB_HOST", "127.0.0.1")
    if not _loopback_host(api_host) or not _loopback_host(web_host):
        raise RuntimeError(
            "unauthenticated local mode must bind RADAR_API_HOST and RADAR_WEB_HOST to loopback",
        )
    origins = [
        value.strip()
        for value in os.getenv(
            "CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000",
        ).split(",")
        if value.strip()
    ]
    for origin in origins:
        parts = urlsplit(origin)
        if parts.scheme not in {"http", "https"} or not parts.hostname or not _loopback_host(parts.hostname):
            raise RuntimeError("unauthenticated local mode only permits loopback CORS origins")


def free_only_mode() -> bool:
    default = "true" if runtime_profile() is RuntimeProfile.LOCAL else "false"
    return os.getenv("FREE_ONLY_MODE", default).lower() == "true"
