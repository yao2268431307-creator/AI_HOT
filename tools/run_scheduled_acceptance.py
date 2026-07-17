"""Independent 15-minute acceptance collector with short-lived OIDC credentials."""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

try:
    from tools.acceptance_monitor import append_sample, collect_sample
except ModuleNotFoundError:  # direct ``python tools/run_scheduled_acceptance.py`` execution
    from acceptance_monitor import append_sample, collect_sample


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        return None


NO_REDIRECT_OPENER = build_opener(NoRedirect)


def secret(path: Path) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError(f"secret file is empty: {path.name}")
    return value


def scheduled_slot(now: datetime | None = None) -> datetime:
    current = now or datetime.now(timezone.utc)
    current = current.astimezone(timezone.utc)
    return current.replace(minute=(current.minute // 15) * 15, second=0, microsecond=0)


def already_collected(path: Path, scheduled: datetime) -> bool:
    if not path.exists():
        return False
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return False
    try:
        latest = json.loads(lines[-1])
    except json.JSONDecodeError:
        return False
    return latest.get("scheduledAt") == scheduled.isoformat()


def fetch_client_credentials_token(
    *, endpoint: str, client_id: str, client_secret: str, scope: str,
    audience: str | None, auth_method: str, timeout: float,
) -> str:
    parts = urlsplit(endpoint)
    if (
        parts.scheme != "https" or not parts.hostname or parts.username
        or parts.password or parts.query or parts.fragment
    ):
        raise ValueError("OIDC token endpoint must be a credential-free HTTPS URL")
    form = {"grant_type": "client_credentials"}
    if scope:
        form["scope"] = scope
    if audience:
        form["audience"] = audience
    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    if auth_method == "client_secret_basic":
        encoded = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        headers["Authorization"] = f"Basic {encoded}"
    elif auth_method == "client_secret_post":
        form["client_id"] = client_id
        form["client_secret"] = client_secret
    else:
        raise ValueError("unsupported OIDC client authentication method")
    request = Request(endpoint, data=urlencode(form).encode(), headers=headers, method="POST")
    try:
        with NO_REDIRECT_OPENER.open(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"OIDC token request failed: {exc}") from exc
    token = payload.get("access_token") if isinstance(payload, dict) else None
    if not isinstance(token, str) or not token:
        raise RuntimeError("OIDC token response has no access_token")
    if payload.get("token_type", "Bearer").lower() != "bearer":
        raise RuntimeError("OIDC token response is not Bearer")
    return token


def run(args: argparse.Namespace) -> dict[str, object]:
    scheduled = scheduled_slot()
    if already_collected(args.output, scheduled):
        return {"status": "already_collected", "scheduledAt": scheduled.isoformat(), "healthy": True}
    token = fetch_client_credentials_token(
        endpoint=args.oidc_token_endpoint,
        client_id=args.oidc_client_id,
        client_secret=secret(args.oidc_client_secret_file),
        scope=args.oidc_scope,
        audience=args.oidc_audience,
        auth_method=args.oidc_auth_method,
        timeout=args.timeout,
    )
    signing_key = secret(args.signing_private_key_file)
    raw_key = base64.b64decode(signing_key, validate=True)
    if len(raw_key) != 32:
        raise ValueError("acceptance signing key must be Base64 for exactly 32 raw bytes")
    preregistration = (
        json.loads(args.manual_preregistration.read_text(encoding="utf-8"))
        if args.manual_preregistration else None
    )
    manual_snapshot = (
        json.loads(args.manual_snapshot.read_text(encoding="utf-8"))
        if args.manual_snapshot else None
    )
    previous = {
        key: os.environ.get(key)
        for key in (
            "ACCEPTANCE_MONITOR_ED25519_PRIVATE_KEY", "ACCEPTANCE_MONITOR_ED25519_KEY_ID",
            "ACCEPTANCE_MONITOR_SCHEDULED_AT", "ACCEPTANCE_MONITOR_RUN_ID",
        )
    }
    try:
        os.environ["ACCEPTANCE_MONITOR_ED25519_PRIVATE_KEY"] = signing_key
        os.environ["ACCEPTANCE_MONITOR_ED25519_KEY_ID"] = args.signing_key_id
        os.environ["ACCEPTANCE_MONITOR_SCHEDULED_AT"] = scheduled.isoformat()
        os.environ["ACCEPTANCE_MONITOR_RUN_ID"] = f"soak-{scheduled.strftime('%Y%m%dT%H%MZ')}-{uuid.uuid4().hex[:12]}"
        sample = collect_sample(
            args.api_url,
            bearer_token=token,
            timeout=args.timeout,
            evidence_window_hours=args.evidence_window_hours,
            manual_preregistration=preregistration,
            manual_snapshot=manual_snapshot,
        )
        append_sample(args.output, sample)
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    return {
        "status": "collected", "scheduledAt": scheduled.isoformat(),
        "observedAt": sample["observedAt"], "healthy": sample["sampleHealthy"],
        "errors": sample["errors"],
    }


def env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Collect one independently scheduled acceptance sample")
    result.add_argument("--api-url", default=env("RADAR_API_URL"), required=env("RADAR_API_URL") is None)
    result.add_argument("--output", type=Path, default=env("ACCEPTANCE_OUTPUT"), required=env("ACCEPTANCE_OUTPUT") is None)
    result.add_argument("--oidc-token-endpoint", default=env("OIDC_TOKEN_ENDPOINT"), required=env("OIDC_TOKEN_ENDPOINT") is None)
    result.add_argument("--oidc-client-id", default=env("OIDC_CLIENT_ID"), required=env("OIDC_CLIENT_ID") is None)
    result.add_argument("--oidc-client-secret-file", type=Path, default=env("OIDC_CLIENT_SECRET_FILE"), required=env("OIDC_CLIENT_SECRET_FILE") is None)
    result.add_argument("--oidc-scope", default=env("OIDC_SCOPE", ""))
    result.add_argument("--oidc-audience", default=env("OIDC_AUDIENCE"))
    result.add_argument("--oidc-auth-method", choices=("client_secret_basic", "client_secret_post"), default=env("OIDC_AUTH_METHOD", "client_secret_basic"))
    result.add_argument("--signing-key-id", default=env("ACCEPTANCE_SIGNING_KEY_ID"), required=env("ACCEPTANCE_SIGNING_KEY_ID") is None)
    result.add_argument("--signing-private-key-file", type=Path, default=env("ACCEPTANCE_SIGNING_PRIVATE_KEY_FILE"), required=env("ACCEPTANCE_SIGNING_PRIVATE_KEY_FILE") is None)
    result.add_argument(
        "--manual-preregistration", type=Path,
        default=env("ACCEPTANCE_MANUAL_PREREGISTRATION") or None,
    )
    result.add_argument(
        "--manual-snapshot", type=Path,
        default=env("ACCEPTANCE_MANUAL_SNAPSHOT") or None,
    )
    result.add_argument("--evidence-window-hours", type=int, default=int(env("ACCEPTANCE_EVIDENCE_WINDOW_HOURS", "72") or "72"))
    result.add_argument("--timeout", type=float, default=float(env("ACCEPTANCE_HTTP_TIMEOUT", "10") or "10"))
    return result


def main() -> int:
    payload = run(parser().parse_args())
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if payload["healthy"] is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
