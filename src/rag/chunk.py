"""Corpus loading and heading-aware chunking.

Each ``corpus/*.md`` file has a small ``--- ... ---`` front-matter block
(``title``, ``source``, ``topic``) followed by Markdown body text. Chunking
keeps each ``#`` / ``##`` section together when it is short enough, and slides a
fixed-size word window (with overlap) over longer sections. The originating
heading is carried on every chunk so citations can point at a section.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from pathlib import Path

from config.settings import settings

TARGET_WORDS = 130
OVERLAP_WORDS = 30

_FRONTMATTER = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)
_HEADING = re.compile(r"^(#{1,6})\s+(.*)$", re.MULTILINE)


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    doc_id: str
    title: str
    source: str
    topic: str
    heading: str
    ordinal: int
    text: str

    def as_payload(self) -> dict:
        return asdict(self)


def _parse_frontmatter(raw: str) -> tuple[dict, str]:
    m = _FRONTMATTER.match(raw)
    if not m:
        return {}, raw
    meta = {}
    for line in m.group(1).splitlines():
        if ":" in line:
            key, _, value = line.partition(":")
            meta[key.strip()] = value.strip()
    return meta, raw[m.end():]


def load_corpus(corpus_dir: Path | None = None) -> list[dict]:
    corpus_dir = Path(corpus_dir or settings.corpus_dir)
    docs = []
    for path in sorted(corpus_dir.glob("*.md")):
        if path.name.lower() == "readme.md":
            continue
        meta, body = _parse_frontmatter(path.read_text(encoding="utf-8"))
        docs.append(
            {
                "doc_id": path.stem,
                "title": meta.get("title", path.stem),
                "source": meta.get("source", "unknown"),
                "topic": meta.get("topic", ""),
                "body": body.strip(),
            }
        )
    return docs


def _split_sections(body: str) -> list[tuple[str, str]]:
    """Return (heading, section_text) pairs, in document order."""
    matches = list(_HEADING.finditer(body))
    if not matches:
        return [("", body.strip())]
    sections = []
    for i, m in enumerate(matches):
        heading = m.group(2).strip()
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        text = body[start:end].strip()
        if text:
            sections.append((heading, text))
    return sections


def _window(words: list[str]) -> list[str]:
    if len(words) <= TARGET_WORDS:
        return [" ".join(words)]
    step = TARGET_WORDS - OVERLAP_WORDS
    out = []
    for start in range(0, len(words), step):
        piece = words[start:start + TARGET_WORDS]
        if piece:
            out.append(" ".join(piece))
        if start + TARGET_WORDS >= len(words):
            break
    return out


def chunk_document(doc: dict) -> list[Chunk]:
    chunks: list[Chunk] = []
    ordinal = 0
    for heading, section in _split_sections(doc["body"]):
        for piece in _window(section.split()):
            chunks.append(
                Chunk(
                    chunk_id=f"{doc['doc_id']}::{ordinal}",
                    doc_id=doc["doc_id"],
                    title=doc["title"],
                    source=doc["source"],
                    topic=doc["topic"],
                    heading=heading,
                    ordinal=ordinal,
                    text=piece,
                )
            )
            ordinal += 1
    return chunks


def build_chunks(corpus_dir: Path | None = None) -> list[Chunk]:
    chunks: list[Chunk] = []
    for doc in load_corpus(corpus_dir):
        chunks.extend(chunk_document(doc))
    return chunks
