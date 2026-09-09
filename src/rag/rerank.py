"""Cross-encoder reranking via fastembed (ONNX Runtime).

A cross-encoder scores the (query, chunk) pair jointly, which is more accurate
than the bi-encoder similarity used for first-stage retrieval. Used to reorder
the fused hybrid candidates and keep the top n.
"""
from __future__ import annotations

from dataclasses import dataclass

from fastembed.rerank.cross_encoder import TextCrossEncoder

from config.settings import settings
from src.rag.chunk import Chunk

_reranker: TextCrossEncoder | None = None


def _model() -> TextCrossEncoder:
    global _reranker
    if _reranker is None:
        _reranker = TextCrossEncoder(model_name=settings.reranker_model)
    return _reranker


@dataclass(frozen=True)
class RerankedChunk:
    chunk: Chunk
    score: float


def rerank(query: str, candidates: list[Chunk], top_n: int | None = None) -> list[RerankedChunk]:
    if not candidates:
        return []
    top_n = top_n or settings.rag_rerank_top_n
    scores = list(_model().rerank(query, [c.text for c in candidates]))
    ordered = sorted(
        (RerankedChunk(c, float(s)) for c, s in zip(candidates, scores)),
        key=lambda r: r.score,
        reverse=True,
    )
    return ordered[:top_n]
