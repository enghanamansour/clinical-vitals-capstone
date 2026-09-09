"""Hybrid retrieval: dense vectors + BM25 keyword search, fused with Reciprocal
Rank Fusion.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from rank_bm25 import BM25Okapi

from config.settings import settings
from src.rag.chunk import Chunk, build_chunks
from src.rag.index import dense_search

_TOKEN = re.compile(r"[a-z0-9]+")


def _tokenize(text: str) -> list[str]:
    return _TOKEN.findall(text.lower())


class Bm25Index:
    """In-process BM25 over the chunk texts."""

    def __init__(self, chunks: list[Chunk]):
        self.chunks = chunks
        self._bm25 = BM25Okapi([_tokenize(c.text) for c in chunks])

    def search(self, query: str, top_k: int) -> list[tuple[str, float]]:
        scores = self._bm25.get_scores(_tokenize(query))
        ranked = sorted(zip(self.chunks, scores), key=lambda p: p[1], reverse=True)
        return [(c.chunk_id, float(s)) for c, s in ranked[:top_k] if s > 0]


def reciprocal_rank_fusion(
    rankings: list[list[str]], k: int = 60
) -> list[tuple[str, float]]:
    """Fuse several ranked id lists. RRF score = sum(1 / (k + rank)), rank is
    1-based within each list. Returns ids sorted by fused score, descending.
    """
    fused: dict[str, float] = {}
    for ranking in rankings:
        for rank, doc_id in enumerate(ranking, start=1):
            fused[doc_id] = fused.get(doc_id, 0.0) + 1.0 / (k + rank)
    return sorted(fused.items(), key=lambda p: p[1], reverse=True)


@dataclass(frozen=True)
class FusedHit:
    chunk_id: str
    rrf_score: float
    dense_rank: int | None
    bm25_rank: int | None


_bm25_cache: Bm25Index | None = None


def _bm25_index(chunks: list[Chunk]) -> Bm25Index:
    global _bm25_cache
    if _bm25_cache is None or _bm25_cache.chunks is not chunks:
        _bm25_cache = Bm25Index(chunks)
    return _bm25_cache


def hybrid_search(
    query: str,
    *,
    chunks: list[Chunk] | None = None,
    dense_top_k: int | None = None,
    bm25_top_k: int | None = None,
    rrf_k: int | None = None,
) -> list[FusedHit]:
    chunks = chunks or build_chunks()
    dense_top_k = dense_top_k or settings.rag_dense_top_k
    bm25_top_k = bm25_top_k or settings.rag_bm25_top_k
    rrf_k = rrf_k or settings.rag_rrf_k

    dense = dense_search(query, dense_top_k)
    bm25 = _bm25_index(chunks).search(query, bm25_top_k)

    dense_ids = [cid for cid, _ in dense]
    bm25_ids = [cid for cid, _ in bm25]
    dense_rank = {cid: i + 1 for i, cid in enumerate(dense_ids)}
    bm25_rank = {cid: i + 1 for i, cid in enumerate(bm25_ids)}

    fused = reciprocal_rank_fusion([dense_ids, bm25_ids], k=rrf_k)
    return [
        FusedHit(cid, score, dense_rank.get(cid), bm25_rank.get(cid))
        for cid, score in fused
    ]
