from __future__ import annotations

from typing import Protocol


class ObjectStore(Protocol):
    async def delete_many(self, references: list[str]) -> None: ...


class CacheInvalidator(Protocol):
    async def invalidate(self, tags: list[str]) -> None: ...


class SourceDeletionConsumer:
    """Completes deletion propagation after the database transaction commits."""

    def __init__(self, objects: ObjectStore, cache: CacheInvalidator) -> None:
        self.objects = objects
        self.cache = cache

    async def handle(self, payload: dict[str, object]) -> None:
        references = [str(value) for value in payload.get("rawEvidenceRefs", [])]
        tags = [str(value) for value in payload.get("cacheTags", [])]
        if references:
            await self.objects.delete_many(references)
        if tags:
            await self.cache.invalidate(tags)
