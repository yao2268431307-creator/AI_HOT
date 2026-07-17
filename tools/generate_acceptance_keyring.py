"""Generate separated Ed25519 acceptance identities and freeze their public registry.

Run this only in an administrator-approved secret-management workstation. Private
key files are written with restrictive permissions and must be imported into the
deployment secret manager, then removed from the workstation.
"""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


KEY_SPECS = (
    ("schedulerKeys", "scheduler"),
    ("reviewerKeys", "reviewer-a"),
    ("reviewerKeys", "reviewer-b"),
    ("baselineKeys", "baseline"),
    ("ledgerKeys", "score-ledger"),
)


def atomic_write(path: Path, body: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("xb") as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, mode)
    temporary.replace(path)


def replace_public_pair(
    keyring_path: Path,
    keyring_bytes: bytes,
    product_policy_path: Path,
    policy_bytes: bytes,
) -> None:
    """Replace the two public files as one recoverable administrative change."""
    originals = {
        keyring_path: keyring_path.read_bytes(),
        product_policy_path: product_policy_path.read_bytes(),
    }
    replaced: list[Path] = []
    try:
        atomic_write(keyring_path, keyring_bytes, 0o644)
        replaced.append(keyring_path)
        atomic_write(product_policy_path, policy_bytes, 0o644)
        replaced.append(product_policy_path)
    except Exception:
        # Cross-file replacement cannot be truly atomic. Restore any file that
        # was already replaced so a failed run never leaves a split binding.
        for path in reversed(replaced):
            atomic_write(path, originals[path], 0o644)
        raise


def generate(
    *, keyring_path: Path, product_policy_path: Path, private_dir: Path,
    keyring_version: str, product_policy_version: str, frozen_at: str,
) -> dict[str, object]:
    if len(keyring_version) < 8 or len(product_policy_version) < 8:
        raise ValueError("stable keyring and product policy versions are required")
    try:
        parsed_frozen = datetime.fromisoformat(frozen_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("frozen-at must be an ISO-8601 timestamp") from exc
    if parsed_frozen.tzinfo is None:
        raise ValueError("frozen-at must include a timezone")
    if private_dir.exists() and any(private_dir.iterdir()):
        raise FileExistsError("private output directory is not empty")
    current_keyring = json.loads(keyring_path.read_text(encoding="utf-8"))
    if any(current_keyring.get(group) for group in ("schedulerKeys", "reviewerKeys", "baselineKeys", "ledgerKeys")):
        raise ValueError("existing keyring is populated; rotate through a reviewed replacement procedure")
    policy = json.loads(product_policy_path.read_text(encoding="utf-8"))
    if policy.get("version") == product_policy_version:
        raise ValueError("product policy version must advance when the keyring changes")
    monitoring = policy.get("acceptanceMonitoring")
    if not isinstance(monitoring, dict):
        raise ValueError("product policy has no acceptanceMonitoring object")

    payload: dict[str, object] = {
        "schemaVersion": "acceptance-ed25519-keyring-v1",
        "keyringVersion": keyring_version,
        "frozenAt": frozen_at,
        "schedulerKeys": [],
        "reviewerKeys": [],
        "baselineKeys": [],
        "ledgerKeys": [],
    }
    private_dir.mkdir(parents=True, exist_ok=False)
    os.chmod(private_dir, 0o700)
    private_files: list[str] = []
    try:
        for group, name in KEY_SPECS:
            key_id = f"{name}-{parsed_frozen.strftime('%Y%m%d')}"
            private_key = Ed25519PrivateKey.generate()
            private_raw = private_key.private_bytes(
                serialization.Encoding.Raw,
                serialization.PrivateFormat.Raw,
                serialization.NoEncryption(),
            )
            public_raw = private_key.public_key().public_bytes(
                serialization.Encoding.Raw,
                serialization.PublicFormat.Raw,
            )
            rows = payload[group]
            assert isinstance(rows, list)
            rows.append({
                "keyId": key_id,
                "publicKeyBase64": base64.b64encode(public_raw).decode(),
                "status": "active",
            })
            filename = f"{name}.private-key-base64"
            atomic_write(private_dir / filename, base64.b64encode(private_raw) + b"\n")
            private_files.append(filename)

        keyring_bytes = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode()
        keyring_digest = "sha256:" + hashlib.sha256(keyring_bytes).hexdigest()
        policy["version"] = product_policy_version
        policy["frozenAt"] = frozen_at
        monitoring["keyringVersion"] = keyring_version
        monitoring["keyringDigest"] = keyring_digest
        monitoring["keyringFrozenAt"] = frozen_at
        notes = policy.setdefault("notes", [])
        if isinstance(notes, list):
            notes.append(
                "Acceptance keyring was populated with separated scheduler, reviewer, baseline, and ledger identities; no earlier sample belongs to this evidence window."
            )
        policy_bytes = (json.dumps(policy, ensure_ascii=False, indent=2) + "\n").encode()
        replace_public_pair(keyring_path, keyring_bytes, product_policy_path, policy_bytes)
    except Exception:
        for path in private_dir.glob("*"):
            path.unlink(missing_ok=True)
        private_dir.rmdir()
        raise
    return {
        "schemaVersion": "acceptance-key-generation-result-v1",
        "keyringVersion": keyring_version,
        "keyringDigest": keyring_digest,
        "productPolicyVersion": product_policy_version,
        "frozenAt": frozen_at,
        "privateFiles": private_files,
        "nextAction": "import every private file into a distinct secret-manager identity and securely erase this directory",
    }


def parser() -> argparse.ArgumentParser:
    root = Path(__file__).resolve().parents[1]
    result = argparse.ArgumentParser(description="Generate and freeze separated acceptance Ed25519 identities")
    result.add_argument("--keyring", type=Path, default=root / "config" / "acceptance_monitor_public_keys.json")
    result.add_argument("--product-policy", type=Path, default=root / "config" / "product_metric_policy.json")
    result.add_argument("--private-output-dir", type=Path, required=True)
    result.add_argument("--keyring-version", required=True)
    result.add_argument("--product-policy-version", required=True)
    result.add_argument("--frozen-at", default=datetime.now(timezone.utc).isoformat())
    return result


def main() -> int:
    args = parser().parse_args()
    payload = generate(
        keyring_path=args.keyring,
        product_policy_path=args.product_policy,
        private_dir=args.private_output_dir,
        keyring_version=args.keyring_version,
        product_policy_version=args.product_policy_version,
        frozen_at=args.frozen_at,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
