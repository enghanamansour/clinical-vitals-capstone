"""RAG tests.

Pure tests (chunking, RRF) always run. The retrieval / rerank / answer tests
need the ONNX models (downloaded on first run) and a reachable Qdrant; they are
skipped automatically if Qdrant is not up.
"""
from __future__ import annotations

import pytest

from src.rag.chunk import build_chunks, chunk_document
from src.rag.search import reciprocal_rank_fusion

# --- pure: chunking -------------------------------------------------------

def test_chunker_carries_heading_and_metadata():
    doc = {
        "doc_id": "demo", "title": "Demo Guide", "source": "unit-test", "topic": "x",
        "body": "# First\nAlpha bravo charlie delta.\n\n## Second\nEcho foxtrot golf hotel.",
    }
    chunks = chunk_document(doc)
    assert {c.heading for c in chunks} == {"First", "Second"}
    assert all(c.doc_id == "demo" and c.title == "Demo Guide" for c in chunks)
    assert [c.ordinal for c in chunks] == list(range(len(chunks)))


def test_long_section_is_windowed_with_overlap():
    body = "# H\n" + " ".join(f"w{i}" for i in range(400))
    chunks = chunk_document({"doc_id": "d", "title": "d", "source": "s", "topic": "",
                             "body": body})
    assert len(chunks) >= 3
    first_tail = chunks[0].text.split()[-10:]
    assert any(w in chunks[1].text.split()[:40] for w in first_tail)


def test_real_corpus_chunks_load():
    chunks = build_chunks()
    assert len(chunks) >= 15
    assert len({c.doc_id for c in chunks}) >= 6
    assert all(c.text.strip() for c in chunks)


# --- pure: reciprocal rank fusion --------------------------------------

def test_rrf_rewards_agreement_across_rankers():
    dense = ["a", "b", "c", "d"]
    bm25 = ["c", "a", "e", "f"]
    fused = dict(reciprocal_rank_fusion([dense, bm25], k=60))
    # "a" is rank1+rank2, "c" is rank3+rank1 -> both beat items in only one list
    assert fused["a"] > fused["b"]
    assert fused["c"] > fused["e"]
    ordered = [cid for cid, _ in reciprocal_rank_fusion([dense, bm25], k=60)]
    assert ordered[0] in {"a", "c"}


def test_rrf_k_controls_head_weighting():
    r = ["x", "y", "z"]
    small_k = dict(reciprocal_rank_fusion([r], k=1))
    large_k = dict(reciprocal_rank_fusion([r], k=1000))
    assert (small_k["x"] - small_k["y"]) > (large_k["x"] - large_k["y"])


# --- integration: needs Qdrant + models ------------------------------

@pytest.fixture(scope="module")
def indexed():
    qc = pytest.importorskip("qdrant_client")
    from src.rag.index import build_index, get_client

    try:
        get_client().get_collections()
    except Exception:
        pytest.skip("Qdrant not reachable on settings.qdrant_url")
    build_index(recreate=True)
    return True


def test_hybrid_search_finds_the_right_document(indexed):
    from src.rag.search import hybrid_search

    hits = hybrid_search("When should I start screening a patient for sepsis?")
    top_docs = {h.chunk_id.split("::")[0] for h in hits[:3]}
    assert "sepsis-screening" in top_docs
    # a fused hit that both rankers returned should outrank a single-ranker hit
    both = [h for h in hits if h.dense_rank and h.bm25_rank]
    assert both and both[0].rrf_score == max(h.rrf_score for h in hits)


def test_answer_is_grounded_and_cited(indexed):
    from src.rag.answer import answer_question

    ans = answer_question("What oxygen saturation should we target in an acutely unwell adult?")
    assert not ans.refused
    assert ans.citations
    assert "[1]" in ans.answer
    assert any("94-98" in c["text"] or "94-98" in ans.answer for c in ans.contexts)
    # every citation marker in the answer maps to a real citation entry
    import re

    markers = {int(m) for m in re.findall(r"\[(\d+)\]", ans.answer)}
    assert markers.issubset({c["marker"] for c in ans.citations})


def test_irrelevant_question_is_refused(indexed):
    from src.rag.answer import answer_question

    # off-topic: the cross-encoder scores every chunk far below the default floor
    ans = answer_question("What time does the hospital gift shop close on Sundays?")
    assert ans.refused
    assert not ans.citations


def test_gold_window_links_to_cited_guidance(indexed):
    import src.lakehouse.gold as gold_mod

    try:
        gold_mod.read_gold()
    except Exception:
        pytest.skip("no Gold table built")
    from src.rag.answer import explain_gold_window

    ans = explain_gold_window()
    assert not ans.refused
    assert ans.citations
    assert ans.retrieval and "generated_query" in ans.retrieval[0]
