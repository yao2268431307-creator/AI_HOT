from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Protocol

import httpx


class EmbeddingProvider(Protocol):
    model: str
    dimensions: int

    async def embed(self, texts: list[str]) -> list[list[float]]: ...

    def status(self) -> dict[str, object]: ...


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if len(left) != len(right) or not left:
        return 0.0
    dot = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return dot / (left_norm * right_norm)


@dataclass(slots=True)
class BgeM3Provider:
    """Client for a privately deployed OpenAI-compatible BGE-M3 embedding service."""

    base_url: str
    api_key: str | None = None
    model: str = "BAAI/bge-m3"
    dimensions: int = 1024
    timeout_seconds: float = 20

    async def embed(self, texts: list[str], client: httpx.AsyncClient | None = None) -> list[list[float]]:
        if not texts:
            return []
        owned = client is None
        http = client or httpx.AsyncClient(timeout=self.timeout_seconds)
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        try:
            response = await http.post(f"{self.base_url.rstrip('/')}/v1/embeddings", headers=headers, json={"model": self.model, "input": texts, "dimensions": self.dimensions})
            response.raise_for_status()
            data = sorted(response.json().get("data", []), key=lambda item: item["index"])
            embeddings = [item["embedding"] for item in data]
            if len(embeddings) != len(texts) or any(len(vector) != self.dimensions for vector in embeddings):
                raise ValueError("embedding service returned an invalid shape")
            return embeddings
        finally:
            if owned:
                await http.aclose()

    def status(self) -> dict[str, object]:
        return {
            "backend": "remote_http",
            "model": self.model,
            "dimensions": self.dimensions,
            "state": "configured",
        }


class LocalBgeM3Provider:
    """Lazy, in-process sentence-transformers provider for the local profile."""

    def __init__(
        self,
        model: str = "BAAI/bge-m3",
        *,
        dimensions: int = 1024,
        device: str = "auto",
        batch_size: int = 8,
        cache_dir: str | Path = ".data/models/bge-m3",
        retry_cooldown_seconds: float = 3600,
        local_files_only: bool = True,
    ) -> None:
        if batch_size < 1:
            raise ValueError("embedding batch size must be positive")
        self.model = model
        self.dimensions = dimensions
        self.requested_device = device
        self.batch_size = batch_size
        self.cache_dir = Path(cache_dir).resolve()
        self.retry_cooldown_seconds = max(30.0, retry_cooldown_seconds)
        self.local_files_only = local_files_only
        self._encoder: object | None = None
        self._active_device: str | None = None
        self._last_error: str | None = None
        self._retry_after_monotonic = 0.0

    def _choose_device(self) -> str:
        if self.requested_device != "auto":
            return self.requested_device
        try:
            import torch

            if torch.cuda.is_available():
                return "cuda"
        except ImportError:
            pass
        return "cpu"

    def _load(self, *, force_cpu: bool = False) -> object:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            self._last_error = "sentence_transformers_not_installed"
            raise RuntimeError(
                "local embeddings require services/api/requirements-local-ml.txt",
            ) from exc
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        device = "cpu" if force_cpu else self._choose_device()
        try:
            encoder = SentenceTransformer(
                self.model,
                cache_folder=str(self.cache_dir),
                device=device,
                local_files_only=self.local_files_only,
            )
        except Exception as exc:
            self._last_error = type(exc).__name__
            raise
        dimension = encoder.get_sentence_embedding_dimension()
        if dimension != self.dimensions:
            self._last_error = "invalid_embedding_dimensions"
            raise RuntimeError(
                f"local embedding model returned {dimension} dimensions; expected {self.dimensions}",
            )
        self._encoder = encoder
        self._active_device = device
        self._last_error = None
        return encoder

    def _embed_sync(self, texts: list[str]) -> list[list[float]]:
        encoder = self._encoder or self._load()
        try:
            vectors = encoder.encode(
                texts,
                batch_size=self.batch_size,
                normalize_embeddings=True,
                convert_to_numpy=True,
                show_progress_bar=False,
            )
        except RuntimeError as exc:
            if self._active_device != "cuda":
                self._last_error = type(exc).__name__
                raise
            # A small GPU may load the model but fail during inference. Retry
            # once on CPU so collection remains autonomous.
            self._encoder = None
            encoder = self._load(force_cpu=True)
            vectors = encoder.encode(
                texts,
                batch_size=max(1, min(self.batch_size, 4)),
                normalize_embeddings=True,
                convert_to_numpy=True,
                show_progress_bar=False,
            )
        rows = vectors.tolist()
        if len(rows) != len(texts) or any(len(row) != self.dimensions for row in rows):
            self._last_error = "invalid_embedding_shape"
            raise RuntimeError("local embedding model returned an invalid shape")
        self._last_error = None
        return rows

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        if self._last_error and time.monotonic() < self._retry_after_monotonic:
            raise RuntimeError(f"local embedding model is cooling down after {self._last_error}")
        try:
            return await asyncio.to_thread(self._embed_sync, texts)
        except Exception as exc:
            if not self._last_error:
                self._last_error = type(exc).__name__
            self._retry_after_monotonic = time.monotonic() + self.retry_cooldown_seconds
            raise

    def status(self) -> dict[str, object]:
        retry_seconds = max(0, int(self._retry_after_monotonic - time.monotonic()))
        return {
            "backend": "local_bge_m3",
            "model": self.model,
            "dimensions": self.dimensions,
            "state": "degraded" if self._last_error else "ready" if self._encoder else "not_loaded",
            "device": self._active_device or self.requested_device,
            "lastError": self._last_error,
            "retryAfterSeconds": retry_seconds,
            "localFilesOnly": self.local_files_only,
            "observedAt": datetime.now(timezone.utc).isoformat(),
            "cacheDir": str(self.cache_dir),
        }
