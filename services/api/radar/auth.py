from __future__ import annotations

import json
import os
import base64
import binascii
import math
import time
from dataclasses import dataclass
from enum import IntEnum

from fastapi import Header, HTTPException, Request
from cryptography.exceptions import InvalidSignature, UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding, rsa


class Role(IntEnum):
    VIEWER = 1
    ANALYST = 2
    OWNER = 3


@dataclass(frozen=True, slots=True)
class Principal:
    subject: str
    role: Role
    workspace_id: str


@dataclass(frozen=True, slots=True)
class VerifiedToken:
    subject: str
    workspace_id: str
    jti: str


def _keys() -> dict[str, dict[str, str]]:
    raw = os.getenv("RADAR_API_KEYS", "{}")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _decode_segment(value: str) -> bytes:
    return base64.b64decode(
        value + "=" * (-len(value) % 4), altchars=b"-_", validate=True,
    )


def jwt_configuration_ready() -> bool:
    """Check that production JWT verification has a usable trust anchor."""
    if not os.getenv("RADAR_JWT_ISSUER") or not os.getenv("RADAR_JWT_AUDIENCE"):
        return False
    try:
        keyring = json.loads(os.getenv("RADAR_JWT_PUBLIC_KEYS", ""))
        if not isinstance(keyring, dict) or not keyring:
            return False
        for key_id, pem in keyring.items():
            if not isinstance(key_id, str) or not key_id or not isinstance(pem, str):
                return False
            public_key = serialization.load_pem_public_key(pem.encode())
            if not isinstance(public_key, (rsa.RSAPublicKey, ed25519.Ed25519PublicKey)):
                return False
    except (ValueError, TypeError, json.JSONDecodeError, UnsupportedAlgorithm):
        return False
    return True


def _verified_jwt(authorization: str | None) -> VerifiedToken:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "bearer token required")
    parts = authorization[7:].split(".")
    if len(parts) != 3:
        raise HTTPException(401, "malformed bearer token")
    try:
        header = json.loads(_decode_segment(parts[0]))
        claims = json.loads(_decode_segment(parts[1]))
        signature = _decode_segment(parts[2])
        if not isinstance(header, dict) or not isinstance(claims, dict):
            raise ValueError("JWT header and claims must be objects")
        algorithm = header.get("alg")
        key_id = header.get("kid")
        if not isinstance(algorithm, str) or not isinstance(key_id, str) or not key_id:
            raise ValueError("JWT algorithm and key ID are required")
        keyring = json.loads(os.getenv("RADAR_JWT_PUBLIC_KEYS", "{}"))
        pem = keyring.get(key_id) if isinstance(keyring, dict) else None
        if not isinstance(pem, str):
            raise ValueError("unknown key ID")
        public_key = serialization.load_pem_public_key(pem.encode())
        signed = f"{parts[0]}.{parts[1]}".encode()
        if algorithm == "RS256" and isinstance(public_key, rsa.RSAPublicKey):
            public_key.verify(signature, signed, padding.PKCS1v15(), hashes.SHA256())
        elif algorithm == "EdDSA" and isinstance(public_key, ed25519.Ed25519PublicKey):
            public_key.verify(signature, signed)
        else:
            raise ValueError("unsupported JWT algorithm or key type")
        now = time.time()
        timestamp_claims = (claims["iat"], claims["exp"], claims.get("nbf", claims["iat"]))
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in timestamp_claims):
            raise ValueError("token timestamps must be numeric")
        issued_at, expires_at, not_before = (float(value) for value in timestamp_claims)
        if not all(math.isfinite(value) for value in (issued_at, expires_at, not_before)):
            raise ValueError("token timestamps must be finite")
        if expires_at <= now or not_before > now + 30 or issued_at > now + 30:
            raise ValueError("token is expired or not yet valid")
        if expires_at <= issued_at or expires_at - issued_at > 300:
            raise ValueError("token lifetime must not exceed five minutes")
        if claims.get("iss") != os.getenv("RADAR_JWT_ISSUER"):
            raise ValueError("issuer mismatch")
        audience = claims.get("aud")
        expected_audience = os.getenv("RADAR_JWT_AUDIENCE")
        audiences = [audience] if isinstance(audience, str) else audience
        if not isinstance(audiences, list) or not all(isinstance(value, str) for value in audiences):
            raise ValueError("audience must be a string or a list of strings")
        if expected_audience not in audiences:
            raise ValueError("audience mismatch")
        subject = claims["sub"]
        workspace_id = claims.get("workspaceId") or claims.get("workspace_id")
        jti = claims["jti"]
        signing_key_version = claims["signingKeyVersion"]
        if not all(isinstance(value, str) and value for value in (subject, workspace_id, jti, signing_key_version)):
            raise ValueError("subject, workspace and jti claims are required")
        if signing_key_version != key_id:
            raise ValueError("signing key version mismatch")
        return VerifiedToken(subject, workspace_id, jti)
    except (
        ValueError, KeyError, TypeError, OverflowError, json.JSONDecodeError, InvalidSignature,
        binascii.Error, UnicodeDecodeError,
    ) as exc:
        raise HTTPException(401, "invalid bearer token") from exc


async def current_principal(
    request: Request,
    x_api_key: str | None = Header(default=None), authorization: str | None = Header(default=None),
) -> Principal:
    if os.getenv("AUTH_REQUIRED", "false").lower() != "true":
        return Principal("local-preview", Role.OWNER, "local-workspace")
    if os.getenv("RADAR_AUTH_MODE", "api_keys") == "jwt":
        token = _verified_jwt(authorization)
        repository = request.app.state.repository
        if repository.is_token_revoked(token.jti):
            raise HTTPException(401, "bearer token has been revoked")
        membership_role = repository.resolve_workspace_membership(token.subject, token.workspace_id)
        if membership_role is None:
            raise HTTPException(403, "active workspace membership required")
        try:
            role = Role[str(membership_role).upper()]
        except KeyError as exc:
            raise HTTPException(403, "workspace membership has an invalid role") from exc
        return Principal(token.subject, role, token.workspace_id)
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
