"""RAG command line.

    python -m src.rag.cli build
    python -m src.rag.cli ask "When should sepsis screening be started?"
    python -m src.rag.cli explain --patient P100007
"""
from __future__ import annotations

import argparse
import json
import sys

from src.rag.answer import answer_question, explain_gold_window
from src.rag.chunk import build_chunks
from src.rag.index import build_index, index_size


def _print_answer(ans) -> None:
    print("Q:", ans.query)
    print()
    if ans.refused:
        print("REFUSED:", ans.answer)
    else:
        print(ans.answer)
        print()
        print("Citations:")
        for c in ans.citations:
            print(f"  [{c['marker']}] {c['title']} - {c['heading']}  "
                  f"(rerank {c['rerank_score']})  <{c['source']}>")
    print()
    print("Retrieval (fused hybrid, top of pool):")
    for r in ans.retrieval[:8]:
        if "chunk_id" in r:
            print(f"  {r['chunk_id']:<28} rrf={r['rrf_score']}  "
                  f"dense_rank={r['dense_rank']}  bm25_rank={r['bm25_rank']}")
        else:
            print("  " + json.dumps(r))


def main() -> None:
    ap = argparse.ArgumentParser(description="RAG over the clinical guideline corpus")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("build", help="chunk corpus, embed, (re)build the Qdrant index")

    p_ask = sub.add_parser("ask", help="answer a free-text question")
    p_ask.add_argument("question")

    p_exp = sub.add_parser("explain", help="explain a Gold NEWS2 window with citations")
    p_exp.add_argument("--patient", default=None)

    args = ap.parse_args()

    if args.cmd == "build":
        chunks = build_chunks()
        n = build_index(chunks, recreate=True)
        print(f"chunks: {len(chunks)} from {len({c.doc_id for c in chunks})} documents")
        print(f"indexed points in Qdrant: {n} (collection size {index_size()})")
        return

    if args.cmd == "ask":
        _print_answer(answer_question(args.question))
        return

    if args.cmd == "explain":
        try:
            _print_answer(explain_gold_window(args.patient))
        except ValueError as exc:
            print(exc, file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
