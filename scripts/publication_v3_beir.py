"""Frozen Publication V3 BEIR encoder and physical-layout authority."""

from __future__ import annotations

BEIR_DENSE_MODEL = "BAAI/bge-small-en-v1.5"
BEIR_DENSE_REVISION = "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a"
BEIR_DENSE_DIMENSIONS = 384
BEIR_QUERY_PREFIX = "Represent this sentence for searching relevant passages: "
BEIR_DOCUMENT_PREFIX = ""
BEIR_SPARSE_MAX_FEATURES = 250_000
BEIR_TOKENIZER = "unicode-word-casefold-v1"
BEIR_ROW_SHARD_SIZE = 32_768
BEIR_ROW_GROUP_SIZE = 8_192
BEIR_ENCODER_PACKAGES = {
    "numpy": "1.26.4",
    "sentence-transformers": "3.4.1",
    "torch": "2.5.1",
    "transformers": "4.48.3",
}


def expected_beir_metadata(dataset_id: str) -> dict[str, object]:
    return {
        "name": dataset_id,
        "kind": "beir-hybrid",
        "split": "test",
        "metric": "cosine",
        "qrels_semantics": "shared-across-all-retrieval-modes",
        "dense": {
            "backend": "sentence-transformers",
            "model": BEIR_DENSE_MODEL,
            "revision": BEIR_DENSE_REVISION,
            "dimensions": BEIR_DENSE_DIMENSIONS,
            "normalized": True,
            "query_prefix": BEIR_QUERY_PREFIX,
            "document_prefix": BEIR_DOCUMENT_PREFIX,
            "package_versions": BEIR_ENCODER_PACKAGES,
        },
        "sparse": {
            "backend": "tf-idf",
            "tokenizer": BEIR_TOKENIZER,
            "max_features": BEIR_SPARSE_MAX_FEATURES,
            "l2_normalized": True,
        },
        "text": {
            "backend": "raw-title-newline-text",
            "query_backend": "raw-text",
        },
    }
