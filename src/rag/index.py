"""Vector index over the guideline chunks, backed by Qdrant.

``build_index`` embeds every chunk and upserts it as a Qdrant point (vector +
full chunk payload). ``dense_search`` runs cosine k-NN and returns
``(chunk_id, score)`` pairs.
"""
from __future__ import annotations

import uuid

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from config.settings import settings
from src.rag.chunk import Chunk, build_chunks
from src.rag.embed import EMBED_DIM, embed_query, embed_texts

_NAMESPACE = uuid.UUID("6f9619ff-8b86-d011-b42d-00cf4fc964ff")

_client: QdrantClient | None = None


def get_client() -> QdrantClient:
    global _client
    if _client is None:
        _client = QdrantClient(url=settings.qdrant_url, timeout=30)
    return _client


def _point_id(chunk_id: str) -> str:
    return str(uuid.uuid5(_NAMESPACE, chunk_id))


def build_index(chunks: list[Chunk] | None = None, *, recreate: bool = True) -> int:
    chunks = chunks or build_chunks()
    client = get_client()

    if recreate:
        if client.collection_exists(settings.qdrant_collection):
            client.delete_collection(settings.qdrant_collection)
        client.create_collection(
            collection_name=settings.qdrant_collection,
            vectors_config=VectorParams(size=EMBED_DIM, distance=Distance.COSINE),
        )
    elif not client.collection_exists(settings.qdrant_collection):
        client.create_collection(
            collection_name=settings.qdrant_collection,
            vectors_config=VectorParams(size=EMBED_DIM, distance=Distance.COSINE),
        )

    vectors = embed_texts([c.text for c in chunks])
    points = [
        PointStruct(id=_point_id(c.chunk_id), vector=v, payload=c.as_payload())
        for c, v in zip(chunks, vectors)
    ]
    client.upsert(collection_name=settings.qdrant_collection, points=points, wait=True)
    return len(points)


def index_size() -> int:
    return get_client().count(settings.qdrant_collection, exact=True).count


def dense_search(query: str, top_k: int) -> list[tuple[str, float]]:
    hits = get_client().search(
        collection_name=settings.qdrant_collection,
        query_vector=embed_query(query),
        limit=top_k,
        with_payload=True,
    )
    return [(h.payload["chunk_id"], float(h.score)) for h in hits]
