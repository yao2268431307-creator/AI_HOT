from __future__ import annotations

import math
from dataclasses import dataclass

import httpx


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
