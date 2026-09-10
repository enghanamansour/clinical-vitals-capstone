"""ONNX Runtime execution-provider selection for the RAG models.

fastembed runs both the dense embedding model and the cross-encoder on
``onnxruntime``. This project pins the provider here instead of letting fastembed
choose one implicitly: ONNX Runtime is used precisely because its native
libraries are Microsoft-signed and load under Windows Smart App Control (PyTorch's
are not), so the CPU provider is the one guaranteed to be present and is what the
pipeline should run on. Selecting it explicitly also keeps inference reproducible
across machines regardless of what accelerators happen to be installed.
"""
from __future__ import annotations

import onnxruntime as ort

# The provider we require. Any GPU/other providers are ignored on purpose: the
# two models are small and CPU inference is more than fast enough here.
_REQUIRED_PROVIDER = "CPUExecutionProvider"


def onnx_providers() -> list[str]:
    """Return the ONNX Runtime provider list to hand to fastembed.

    Raises ``RuntimeError`` if the signed CPU provider is not available, which
    means the onnxruntime install is broken - better to fail loudly at model
    load time than to silently fall back to something unexpected.
    """
    available = ort.get_available_providers()
    if _REQUIRED_PROVIDER not in available:
        raise RuntimeError(
            f"onnxruntime is missing {_REQUIRED_PROVIDER}; available: {available}"
        )
    return [_REQUIRED_PROVIDER]
