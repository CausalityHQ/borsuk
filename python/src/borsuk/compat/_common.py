"""Shared plumbing for the drop-in compatibility adapters.

Each adapter (Pinecone, S3 Vectors, turbopuffer) is a thin shim whose surface
mimics the target SDK and whose backend is a BORSUK index. A "namespace" (or an
S3 Vectors index-within-a-bucket) maps to its own BORSUK index rooted under a
shared base URI, so isolation is physical and needs no engine support.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import borsuk

__all__ = [
    "NamespaceStore",
    "map_metric",
    "translate_turbopuffer_filter",
    "split_row",
]


def _sanitize(segment: str) -> str:
    """Percent-encode a namespace so any string is a safe URI path segment."""
    if segment == "":
        segment = "__default__"
    return quote(str(segment), safe="")


class NamespaceStore:
    """Lazily creates/opens one BORSUK index per namespace under ``base_uri``.

    ``base_uri`` is a BORSUK storage root (``file://…``, ``s3://…``, or a local
    path). Each namespace resolves to ``<base_uri>/<sanitized-namespace>``.
    Index handles are cached for the lifetime of the store.
    """

    def __init__(
        self,
        base_uri: str,
        *,
        metric: str,
        dimensions: int,
        **create_kwargs: Any,
    ) -> None:
        self.base_uri = base_uri.rstrip("/")
        self.metric = metric
        self.dimensions = dimensions
        self._create_kwargs = create_kwargs
        self._handles: dict[str, borsuk.Index] = {}

    def uri_for(self, namespace: str) -> str:
        return f"{self.base_uri}/{_sanitize(namespace)}"

    def get(self, namespace: str, *, create: bool = True) -> borsuk.Index:
        """Return the index for ``namespace``, creating it on first use.

        Opening is tried first; a missing index is created (when ``create``)."""
        key = _sanitize(namespace)
        cached = self._handles.get(key)
        if cached is not None:
            return cached
        uri = self.uri_for(namespace)
        try:
            handle = borsuk.open(uri)
        except (borsuk.BorsukError, OSError, ValueError):
            if not create:
                raise
            handle = borsuk.create(
                uri=uri,
                metric=self.metric,
                dimensions=self.dimensions,
                **self._create_kwargs,
            )
        self._handles[key] = handle
        return handle

    def namespaces(self) -> list[str]:
        """Namespaces opened or created through this store, in insertion order."""
        return list(self._handles.keys())


# ---- Metric mapping -------------------------------------------------------

# Each target service names its metrics differently; map onto BORSUK canonical
# metric strings accepted by ``borsuk.create``.
_METRIC_MAPS: dict[str, dict[str, str]] = {
    "pinecone": {
        "cosine": "cosine",
        "euclidean": "euclidean",
        "dotproduct": "inner-product",
    },
    "turbopuffer": {
        "cosine_distance": "cosine",
        "euclidean_squared": "squared-euclidean",
    },
    "s3vectors": {
        "cosine": "cosine",
        "euclidean": "euclidean",
    },
}


def map_metric(service: str, metric: str) -> str:
    """Translate a target service's metric name to a BORSUK metric string."""
    table = _METRIC_MAPS[service]
    try:
        return table[metric]
    except KeyError as exc:
        supported = ", ".join(sorted(table))
        raise ValueError(
            f"{service} metric {metric!r} is not supported; use one of: {supported}"
        ) from exc


# ---- Filter translation ---------------------------------------------------

# turbopuffer uses tuple filters, e.g. ("And", (("g", "Eq", "rock"), ...));
# BORSUK (and Pinecone/S3 Vectors) use a Mongo-style ``$``-operator dict.
_TPUF_LEAF_OPS = {
    "Eq": "$eq",
    "NotEq": "$ne",
    "Gt": "$gt",
    "Gte": "$gte",
    "Lt": "$lt",
    "Lte": "$lte",
    "In": "$in",
    "NotIn": "$nin",
    "Contains": "$contains",
}
_TPUF_LOGICAL = {"And": "$and", "Or": "$or"}


def translate_turbopuffer_filter(node: Any) -> dict:
    """Convert a turbopuffer tuple filter into a BORSUK ``$``-operator dict."""
    if node is None:
        return {}
    if not isinstance(node, (tuple, list)) or len(node) == 0:
        raise ValueError(f"invalid turbopuffer filter: {node!r}")

    head = node[0]
    if head in _TPUF_LOGICAL:
        clauses = [translate_turbopuffer_filter(child) for child in node[1]]
        return {_TPUF_LOGICAL[head]: clauses}
    if head == "Not":
        return {"$not": translate_turbopuffer_filter(node[1])}

    # Leaf: (attribute, Operator, value)
    if len(node) != 3:
        raise ValueError(f"invalid turbopuffer leaf filter: {node!r}")
    attr, op, value = node
    if op not in _TPUF_LEAF_OPS:
        supported = ", ".join(sorted(_TPUF_LEAF_OPS) + sorted(_TPUF_LOGICAL) + ["Not"])
        raise ValueError(
            f"turbopuffer operator {op!r} is not supported; use one of: {supported}"
        )
    return {str(attr): {_TPUF_LEAF_OPS[op]: value}}


# ---- Row helpers ----------------------------------------------------------


def split_row(row: dict, *, id_key: str, vector_key: str) -> tuple[str, list[float], dict]:
    """Split a turbopuffer-style row into (id, vector, metadata-attributes)."""
    row = dict(row)
    try:
        record_id = row.pop(id_key)
    except KeyError as exc:
        raise ValueError(f"row is missing required {id_key!r}: {row!r}") from exc
    try:
        vector = row.pop(vector_key)
    except KeyError as exc:
        raise ValueError(f"row is missing required {vector_key!r}: {row!r}") from exc
    return str(record_id), list(vector), row
