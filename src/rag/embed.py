"""Dense embeddings via fastembed (ONNX Runtime).

A module-level singleton holds the model so it is loaded once per process. The
first call downloads the ONNX model into ``settings.fastembed_cache_path``.
"""
from __future__ import annotations

import os

from config.settings import settings

# Must be set before fastembed / huggingface_hub are imported.
settings.fastembed_cache_path.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("FASTEMBED_CACHE_PATH", str(settings.fastembed_cache_path))
os.environ.setdefault("HF_HOME", str(settings.fastembed_cache_path / "hf"))
os.environ.setdefault("HF_HUB_DISABLE_SYMLINKS_WARNING", "1")
os.environ.setdefault("HF_HUB_DISABLE_SYMLINKS", "1")  # copy, don't symlink (Windows)

from fastembed import TextEmbedding  # noqa: E402

from src.rag.onnx_runtime import onnx_providers  # noqa: E402

EMBED_DIM = 384  # BAAI/bge-small-en-v1.5

_model: TextEmbedding | None = None


def _embedder() -> TextEmbedding:
    global _model
    if _model is None:
        _model = TextEmbedding(
            model_name=settings.embedding_model,
            providers=onnx_providers(),
        )
    return _model


def embed_texts(texts: list[str]) -> list[list[float]]:
    return [vec.tolist() for vec in _embedder().embed(list(texts))]


def embed_query(text: str) -> list[float]:
    return embed_texts([text])[0]
