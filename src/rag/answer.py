"""End-to-end grounded answering: hybrid retrieval -> RRF -> cross-encoder
rerank -> an extractive answer whose every sentence carries a citation to the
guideline chunk it came from.

No text-generation model is used: the answer is assembled from the sentences of
the top reranked chunks that are most similar to the question, so it is grounded
by construction. If the best rerank score is below ``rag_min_rerank_score`` the
pipeline refuses to answer.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from config.settings import settings
from src.rag.chunk import Chunk, build_chunks
from src.rag.embed import embed_texts
from src.rag.rerank import rerank
from src.rag.search import hybrid_search

_SENT = re.compile(r"(?<=[.!?])\s+(?=[-*A-Z0-9])")
_LIST_PREFIX = re.compile(r"^\s*[-*]\s+")
_BOLD = re.compile(r"\*\*(.+?)\*\*")


def _clean(sentence: str) -> str:
    s = _LIST_PREFIX.sub("", sentence)
    s = _BOLD.sub(r"\1", s)
    return re.sub(r"\s+", " ", s).strip()


def _cos(a: list[float], b: list[float]) -> float:
    num = sum(x * y for x, y in zip(a, b))
    da = sum(x * x for x in a) ** 0.5
    db = sum(y * y for y in b) ** 0.5
    return num / (da * db) if da and db else 0.0


@dataclass
class Answer:
    query: str
    refused: bool
    answer: str
    citations: list[dict] = field(default_factory=list)
    contexts: list[dict] = field(default_factory=list)
    retrieval: list[dict] = field(default_factory=list)


def answer_question(
    query: str,
    *,
    chunks: list[Chunk] | None = None,
    rerank_pool: int = 12,
    max_sentences: int = 4,
) -> Answer:
    chunks = chunks or build_chunks()
    by_id = {c.chunk_id: c for c in chunks}

    hits = hybrid_search(query, chunks=chunks)
    retrieval = [
        {"chunk_id": h.chunk_id, "rrf_score": round(h.rrf_score, 5),
         "dense_rank": h.dense_rank, "bm25_rank": h.bm25_rank}
        for h in hits[:rerank_pool]
    ]
    candidates = [by_id[h.chunk_id] for h in hits[:rerank_pool] if h.chunk_id in by_id]
    reranked = rerank(query, candidates, top_n=settings.rag_rerank_top_n)

    if not reranked or reranked[0].score < settings.rag_min_rerank_score:
        return Answer(
            query=query, refused=True,
            answer="No sufficiently relevant guidance was retrieved for this question.",
            retrieval=retrieval,
        )

    # citation numbering follows rerank order
    cited = {r.chunk.chunk_id: i + 1 for i, r in enumerate(reranked)}
    citations = [
        {"marker": i + 1, "chunk_id": r.chunk.chunk_id, "title": r.chunk.title,
         "heading": r.chunk.heading, "source": r.chunk.source,
         "rerank_score": round(r.score, 3)}
        for i, r in enumerate(reranked)
    ]

    # draw answer sentences from the cross-encoder's top 2 chunks, plus any
    # further chunk it scored as positively relevant.
    relevant = reranked[:2] + [r for r in reranked[2:] if r.score > 0.0]

    # sentence pool from those chunks, scored against the query by cosine
    sentences: list[tuple[str, int]] = []
    for r in relevant:
        for line in re.split(r"\n+", r.chunk.text.strip()):
            for raw in _SENT.split(line):
                s = _clean(raw)
                if len(s.split()) >= 4:
                    sentences.append((s, cited[r.chunk.chunk_id]))

    q_vec, *s_vecs = embed_texts([query] + [s for s, _ in sentences])
    scored = sorted(
        ((_cos(q_vec, v), s, marker) for v, (s, marker) in zip(s_vecs, sentences)),
        key=lambda t: t[0], reverse=True,
    )

    picked: list[tuple[str, int]] = []
    seen: set[str] = set()
    for _, s, marker in scored:
        if s in seen:
            continue
        seen.add(s)
        picked.append((s, marker))
        if len(picked) >= max_sentences:
            break
    # keep the answer readable: order picked sentences by citation then original
    picked.sort(key=lambda p: p[1])

    answer_text = " ".join(f"{s} [{marker}]" for s, marker in picked)
    contexts = [
        {"chunk_id": r.chunk.chunk_id, "title": r.chunk.title, "heading": r.chunk.heading,
         "source": r.chunk.source, "text": r.chunk.text, "rerank_score": round(r.score, 3)}
        for r in reranked
    ]
    return Answer(
        query=query, refused=False, answer=answer_text,
        citations=citations, contexts=contexts, retrieval=retrieval,
    )


def explain_gold_window(patient_id: str | None = None, window_start=None) -> Answer:
    """Turn a Gold NEWS2 row into a retrieval query and answer it with citations."""
    import pandas as pd

    from src.lakehouse.gold import read_gold

    g = read_gold().to_pandas()
    if patient_id:
        g = g[g["patient_id"] == patient_id]
    if window_start is not None:
        g = g[g["window_start"] == pd.Timestamp(window_start)]
    if g.empty:
        raise ValueError("no matching Gold row")
    row = g.sort_values(["max_news2", "mean_news2"], ascending=False).iloc[0]

    on_o2 = " on oxygen" if row["pct_readings_on_oxygen"] > 0 else ""
    query = (
        f"NEWS2 {int(row['max_news2'])} ({row['worst_risk_band']} risk), "
        f"respiratory rate {int(row['max_resp_rate'])}, "
        f"SpO2 {int(row['min_spo2'])}%{on_o2}, "
        f"systolic BP {int(row['min_systolic_bp'])}, "
        f"heart rate {int(round(row['mean_heart_rate']))}, "
        f"temperature {row['max_temperature_c']:.1f}. "
        f"Recommended escalation and immediate management?"
    )
    ans = answer_question(query)
    ans.retrieval.insert(0, {"gold_patient_id": row["patient_id"],
                             "gold_window_start": str(row["window_start"]),
                             "generated_query": query})
    return ans
