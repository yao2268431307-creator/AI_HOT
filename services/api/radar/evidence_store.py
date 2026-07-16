from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Protocol
from urllib.parse import urlsplit


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
    async def delete_many(self, references: list[str]) -> None: ...


class LocalEvidenceStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()

    async def put(self, reference: str, body: bytes, content_type: str) -> None:
        target = (self.root / evidence_key(reference)).resolve()
        if self.root not in target.parents:
            raise ValueError("raw evidence path escaped its root")
        await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(target.write_bytes, body)

    async def delete_many(self, references: list[str]) -> None:
        for reference in references:
            target = (self.root / evidence_key(reference)).resolve()
            if self.root in target.parents and target.exists():
                await asyncio.to_thread(target.unlink)


class S3EvidenceStore:
    """S3-compatible writer for Cloudflare R2; boto3 is imported only in production."""

    def __init__(self, endpoint_url: str, access_key_id: str, secret_access_key: str) -> None:
        import boto3

        self.client = boto3.client("s3", endpoint_url=endpoint_url, aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, region_name="auto")

    async def put(self, reference: str, body: bytes, content_type: str) -> None:
        parts = urlsplit(reference)
        if parts.scheme != "r2" or not parts.netloc:
            raise ValueError("raw evidence references must use r2://bucket/key")
        await asyncio.to_thread(self.client.put_object, Bucket=parts.netloc, Key=parts.path.lstrip("/"), Body=body, ContentType=content_type)

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
