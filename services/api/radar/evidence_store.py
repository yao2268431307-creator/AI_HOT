from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from threading import Lock
from typing import Protocol
from urllib.parse import urlsplit
import uuid


def evidence_key(reference: str) -> str:
    parts = urlsplit(reference)
    if parts.scheme != "r2" or not parts.netloc:
        raise ValueError("raw evidence references must use r2://bucket/key")
    key = f"{parts.netloc}/{parts.path.lstrip('/')}"
    if ".." in Path(key).parts:
        raise ValueError("raw evidence key contains traversal")
    return key


class RawEvidenceStore(Protocol):
    async def put(self, reference: str, body: bytes, content_type: str) -> None: ...
    async def get(self, reference: str) -> bytes: ...
    async def delete_many(self, references: list[str]) -> None: ...
    async def probe(self, bucket: str) -> None: ...
    async def probe_delete(self, bucket: str) -> None: ...


class LocalEvidenceStore:
    def __init__(self, root: str | Path, max_bytes: int | None = None) -> None:
        self.root = Path(root).resolve()
        self.max_bytes = max_bytes if max_bytes is not None else int(
            os.getenv("RAW_EVIDENCE_MAX_BYTES", str(20 * 1024 * 1024 * 1024)),
        )
        self._usage_cache: int | None = None
        self._usage_lock = Lock()

    def _target(self, reference: str) -> Path:
        target = (self.root / evidence_key(reference)).resolve()
        if self.root not in target.parents:
            raise ValueError("raw evidence path escaped its root")
        return target

    def usage_bytes(self) -> int:
        with self._usage_lock:
            if self._usage_cache is None:
                self._usage_cache = (
                    sum(item.stat().st_size for item in self.root.rglob("*") if item.is_file())
                    if self.root.exists() else 0
                )
            return self._usage_cache

    def _record_write(self, old_size: int, new_size: int) -> None:
        with self._usage_lock:
            if self._usage_cache is not None:
                self._usage_cache = max(0, self._usage_cache - old_size + new_size)

    def _record_delete(self, deleted_size: int) -> None:
        with self._usage_lock:
            if self._usage_cache is not None:
                self._usage_cache = max(0, self._usage_cache - deleted_size)

    def status(self) -> dict[str, object]:
        used = self.usage_bytes()
        return {
            "backend": "local_filesystem",
            "root": str(self.root),
            "usedBytes": used,
            "maxBytes": self.max_bytes,
            "capacityState": "limited" if used >= self.max_bytes else "healthy",
        }

    async def put(self, reference: str, body: bytes, content_type: str) -> None:
        target = self._target(reference)
        used = await asyncio.to_thread(self.usage_bytes)
        if used + len(body) > self.max_bytes and len(body) > 4096:
            body = json.dumps({
                "storageOmitted": True,
                "reason": "local evidence capacity limit reached",
                "originalBytes": len(body),
                "contentType": content_type,
            }).encode("utf-8")
        old_size = target.stat().st_size if target.is_file() else 0
        await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(target.write_bytes, body)
        self._record_write(old_size, len(body))

    async def get(self, reference: str) -> bytes:
        target = self._target(reference)
        if not target.is_file():
            raise FileNotFoundError(reference)
        return await asyncio.to_thread(target.read_bytes)

    async def delete_many(self, references: list[str]) -> None:
        for reference in references:
            target = self._target(reference)
            if self.root in target.parents and target.exists():
                deleted_size = target.stat().st_size
                await asyncio.to_thread(target.unlink)
                self._record_delete(deleted_size)

    async def probe(self, bucket: str) -> None:
        reference = f"r2://{bucket}/.health/{uuid.uuid4().hex}"
        await self.put(reference, b"ok", "text/plain")
        await self.delete_many([reference])

    async def probe_delete(self, bucket: str) -> None:
        """Exercise only the delete capability used by the alert consumer."""
        await self.delete_many([f"r2://{bucket}/.radar-health-delete/{uuid.uuid4().hex}"])


class S3EvidenceStore:
    """S3-compatible writer for Cloudflare R2; boto3 is imported only in production."""

    def __init__(
        self,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        session_token: str | None = None,
    ) -> None:
        import boto3

        client_options = {
            "endpoint_url": endpoint_url,
            "aws_access_key_id": access_key_id,
            "aws_secret_access_key": secret_access_key,
            "region_name": "auto",
        }
        if session_token:
            client_options["aws_session_token"] = session_token
        self.client = boto3.client("s3", **client_options)

    async def put(self, reference: str, body: bytes, content_type: str) -> None:
        parts = urlsplit(reference)
        if parts.scheme != "r2" or not parts.netloc:
            raise ValueError("raw evidence references must use r2://bucket/key")
        await asyncio.to_thread(self.client.put_object, Bucket=parts.netloc, Key=parts.path.lstrip("/"), Body=body, ContentType=content_type)

    async def get(self, reference: str) -> bytes:
        parts = urlsplit(reference)
        evidence_key(reference)
        response = await asyncio.to_thread(
            self.client.get_object, Bucket=parts.netloc, Key=parts.path.lstrip("/"),
        )
        return await asyncio.to_thread(response["Body"].read)

    async def delete_many(self, references: list[str]) -> None:
        grouped: dict[str, list[dict[str, str]]] = {}
        for reference in references:
            parts = urlsplit(reference)
            evidence_key(reference)
            if not parts.path.lstrip("/"):
                raise ValueError("raw evidence reference is missing an object key")
            grouped.setdefault(parts.netloc, []).append({"Key": parts.path.lstrip("/")})
        for bucket, objects in grouped.items():
            for start in range(0, len(objects), 1000):
                response = await asyncio.to_thread(
                    self.client.delete_objects,
                    Bucket=bucket,
                    Delete={"Objects": objects[start:start + 1000], "Quiet": True},
                )
                errors = response.get("Errors", []) if isinstance(response, dict) else []
                if errors:
                    failed = ", ".join(str(item.get("Key", "unknown")) for item in errors)
                    raise RuntimeError(f"raw evidence deletion was only partially successful: {failed}")

    async def probe(self, bucket: str) -> None:
        """Verify the exact bucket has write, read and delete permissions."""
        key = f".radar-health/{uuid.uuid4().hex}"
        try:
            await asyncio.to_thread(
                self.client.put_object, Bucket=bucket, Key=key, Body=b"radar-health", ContentType="text/plain",
            )
            response = await asyncio.to_thread(self.client.get_object, Bucket=bucket, Key=key)
            body = await asyncio.to_thread(response["Body"].read)
            if body != b"radar-health":
                raise RuntimeError("R2 probe returned unexpected bytes")
        finally:
            await asyncio.to_thread(self.client.delete_object, Bucket=bucket, Key=key)

    async def probe_delete(self, bucket: str) -> None:
        """Verify delete-only credentials without requiring write or read access."""
        key = f".radar-health-delete/{uuid.uuid4().hex}"
        await asyncio.to_thread(self.client.delete_object, Bucket=bucket, Key=key)
