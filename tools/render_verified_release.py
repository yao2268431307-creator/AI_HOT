"""Verify signed release evidence and render a non-secret Compose release file."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from typing import Callable
from urllib.parse import urlsplit


DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
COMMIT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
ISSUER = "https://token.actions.githubusercontent.com"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_checksum(manifest_path: Path, checksum_path: Path) -> None:
    parts = checksum_path.read_text(encoding="utf-8").strip().split()
    if len(parts) != 2 or parts[0] != file_sha256(manifest_path):
        raise ValueError("release manifest checksum mismatch")
    if Path(parts[1]).name != manifest_path.name:
        raise ValueError("release checksum names a different manifest")


def validate_manifest(
    manifest_path: Path,
    *,
    expected_repository: str,
    expected_commit: str,
) -> dict[str, object]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or set(payload) != {
        "schemaVersion", "commit", "repository", "api", "web", "baseImages",
    }:
        raise ValueError("release manifest has an unsupported shape")
    if payload.get("schemaVersion") != "ai-hot-release-manifest-v1":
        raise ValueError("release manifest schema is unsupported")
    if not COMMIT.fullmatch(expected_commit) or payload.get("commit") != expected_commit:
        raise ValueError("release manifest does not match the approved commit")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", expected_repository):
        raise ValueError("expected GitHub repository must be OWNER/REPOSITORY")
    if payload.get("repository") != expected_repository:
        raise ValueError("release manifest repository mismatch")
    owner = expected_repository.split("/", 1)[0].lower()
    expected_images = {
        "api": f"ghcr.io/{owner}/ai-hot-radar-api",
        "web": f"ghcr.io/{owner}/ai-hot-radar-web",
    }
    for name, expected_image in expected_images.items():
        row = payload.get(name)
        if not isinstance(row, dict) or set(row) != {"repository", "digest"}:
            raise ValueError(f"release manifest {name} image is malformed")
        if str(row.get("repository", "")).lower() != expected_image:
            raise ValueError(f"release manifest {name} repository is unexpected")
        if not DIGEST.fullmatch(str(row.get("digest", ""))):
            raise ValueError(f"release manifest {name} digest is malformed")
    bases = payload.get("baseImages")
    if not isinstance(bases, dict) or set(bases) != {"python", "node"}:
        raise ValueError("release manifest base image set is malformed")
    if any(not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", str(value)) for value in bases.values()):
        raise ValueError("release base images must be immutable repository@sha256 references")
    return payload


def workflow_identity(repository: str) -> str:
    escaped = re.escape(repository)
    return rf"^https://github\.com/{escaped}/\.github/workflows/release\.yml@refs/(heads|tags)/.+$"


def verify_supply_chain(
    manifest_path: Path,
    bundle_path: Path,
    manifest: dict[str, object],
    *,
    cosign: str,
    verify_github_attestations: bool,
    gh: str,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> None:
    repository = str(manifest["repository"])
    identity = workflow_identity(repository)
    commands = [[
        cosign, "verify-blob", str(manifest_path), "--bundle", str(bundle_path),
        "--certificate-identity-regexp", identity,
        "--certificate-oidc-issuer", ISSUER,
    ]]
    for name in ("api", "web"):
        image = manifest[name]
        assert isinstance(image, dict)
        reference = f"{image['repository']}@{image['digest']}"
        commands.append([
            cosign, "verify", reference,
            "--certificate-identity-regexp", identity,
            "--certificate-oidc-issuer", ISSUER,
        ])
        if verify_github_attestations:
            commands.append([gh, "attestation", "verify", f"oci://{reference}", "-R", repository])
    for command in commands:
        try:
            runner(command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except (OSError, subprocess.CalledProcessError) as exc:
            raise RuntimeError(f"supply-chain verification failed for {command[0]} {command[1]}") from exc


def valid_https(value: str) -> bool:
    try:
        parsed = urlsplit(value)
        return (
            parsed.scheme == "https" and bool(parsed.hostname)
            and not parsed.username and not parsed.password
            and not parsed.query and not parsed.fragment
        )
    except ValueError:
        return False


def valid_origin(value: str) -> bool:
    if not valid_https(value):
        return False
    parsed = urlsplit(value)
    return parsed.path in {"", "/"} and not parsed.query and not parsed.fragment


def render_release_env(
    manifest: dict[str, object],
    *,
    public_api_url: str,
    public_web_url: str,
    config_dir: Path,
    manifest_digest: str,
) -> str:
    if not valid_https(public_api_url) or not valid_origin(public_web_url):
        raise ValueError("public API and Web URLs must be credential-free HTTPS")
    if not DIGEST.fullmatch(manifest_digest):
        raise ValueError("manifest digest must be a lowercase SHA-256")
    api = manifest["api"]
    web = manifest["web"]
    assert isinstance(api, dict) and isinstance(web, dict)
    values = {
        "RADAR_API_IMAGE_REPOSITORY": api["repository"],
        "RADAR_API_IMAGE_DIGEST": api["digest"],
        "RADAR_WEB_IMAGE_REPOSITORY": web["repository"],
        "RADAR_WEB_IMAGE_DIGEST": web["digest"],
        "RADAR_PUBLIC_API_URL": public_api_url,
        "RADAR_PUBLIC_WEB_URL": public_web_url,
        "RADAR_RELEASE_REPOSITORY": manifest["repository"],
        "RADAR_RELEASE_COMMIT": manifest["commit"],
        "RADAR_RELEASE_MANIFEST_DIGEST": manifest_digest,
        "RADAR_API_ENV_FILE": (config_dir / "api.env").as_posix(),
        "RADAR_SCHEDULER_ENV_FILE": (config_dir / "scheduler.env").as_posix(),
        "RADAR_ALERT_ENV_FILE": (config_dir / "alert.env").as_posix(),
        "RADAR_SOURCE_IDENTITIES_FILE": (config_dir / "source-identities.json").as_posix(),
        "RADAR_RSS_FEEDS_FILE": (config_dir / "rss-feeds.json").as_posix(),
        "RADAR_API_PORT": "8017",
        "RADAR_WEB_PORT": "3000",
    }
    return "\n".join(f"{key}={value}" for key, value in values.items()) + "\n"


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Verify and render an AI Hot Radar release bundle")
    result.add_argument("--manifest", type=Path, required=True)
    result.add_argument("--checksum", type=Path, required=True)
    result.add_argument("--sigstore-bundle", type=Path, required=True)
    result.add_argument("--expected-repository", required=True)
    result.add_argument("--expected-commit", required=True)
    result.add_argument("--public-api-url", required=True)
    result.add_argument("--public-web-url", required=True)
    result.add_argument("--config-dir", type=Path, default=Path("/etc/ai-hot"))
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--cosign-bin", default="cosign")
    result.add_argument("--gh-bin", default="gh")
    result.add_argument("--verify-github-attestations", action="store_true")
    return result


def main() -> int:
    args = parser().parse_args()
    validate_checksum(args.manifest, args.checksum)
    manifest = validate_manifest(
        args.manifest,
        expected_repository=args.expected_repository,
        expected_commit=args.expected_commit,
    )
    verify_supply_chain(
        args.manifest,
        args.sigstore_bundle,
        manifest,
        cosign=args.cosign_bin,
        verify_github_attestations=args.verify_github_attestations,
        gh=args.gh_bin,
    )
    rendered = render_release_env(
        manifest,
        public_api_url=args.public_api_url,
        public_web_url=args.public_web_url,
        config_dir=args.config_dir,
        manifest_digest="sha256:" + file_sha256(args.manifest),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(args.output.name + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    os.chmod(temporary, 0o600)
    temporary.replace(args.output)
    print(json.dumps({
        "verified": True,
        "releaseManifestDigest": "sha256:" + file_sha256(args.manifest),
        "releaseEnv": str(args.output),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
