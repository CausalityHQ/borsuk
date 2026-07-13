"""Drop-in Pinecone client backed by BORSUK.

Mimics the Pinecone Python SDK's data-plane surface so existing code can switch
backends by changing the import and pointing at a BORSUK storage root::

    # before: from pinecone import Pinecone; pc = Pinecone(api_key=...)
    from borsuk.compat.pinecone import Pinecone
    pc = Pinecone(base_uri="file:///data/vectors", dimension=768, metric="cosine")

    index = pc.Index("products")
    index.upsert([("a", [0.1, 0.2, ...], {"genre": "rock"})], namespace="store-1")
    res = index.query(vector=[...], top_k=10, filter={"genre": {"$eq": "rock"}},
                      include_metadata=True, namespace="store-1")

The backend is a local/embedded BORSUK index — there is no network service, auth,
or server-side consistency model, and ``score`` carries BORSUK's distance.
"""

from __future__ import annotations

from typing import Any

from ._common import AttrDict, NamespaceStore, map_metric

__all__ = ["Pinecone", "Index"]

_DEFAULT_NAMESPACE = "__default__"
# Pinecone records may carry a single sparse vector alongside the dense one (for
# hybrid search). It maps to a reserved BORSUK sparse named vector.
_SPARSE_VECTOR_NAME = "__sparse__"
_SPARSE_DIMENSIONS = 1 << 31


class Pinecone:
    """Pinecone-compatible client. ``api_key`` is accepted and ignored."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_uri: str,
        dimension: int,
        metric: str = "cosine",
        **_: Any,
    ) -> None:
        self._base_uri = base_uri.rstrip("/")
        self._default_dimension = dimension
        self._default_metric = metric
        self._indexes: dict[str, Index] = {}

    def create_index(
        self,
        name: str,
        dimension: int | None = None,
        metric: str | None = None,
        spec: Any = None,
        **_: Any,
    ) -> Index:
        """Register an index. Namespaces inside it are created lazily on write."""
        return self.Index(
            name,
            dimension=dimension or self._default_dimension,
            metric=metric or self._default_metric,
        )

    def Index(  # noqa: N802 — matches Pinecone's method name
        self,
        name: str | None = None,
        host: str | None = None,
        *,
        dimension: int | None = None,
        metric: str | None = None,
    ) -> Index:
        key = name or host or "__index__"
        existing = self._indexes.get(key)
        if existing is not None:
            return existing
        store = NamespaceStore(
            f"{self._base_uri}/{key}",
            metric=map_metric("pinecone", metric or self._default_metric),
            dimensions=dimension or self._default_dimension,
            named_vectors={
                _SPARSE_VECTOR_NAME: {
                    "dimensions": _SPARSE_DIMENSIONS,
                    "metric": "inner-product",
                    "kind": "sparse",
                }
            },
        )
        index = Index(store)
        self._indexes[key] = index
        return index


def _sparse_pair(value: Any) -> dict[str, list] | None:
    """Extract `{indices, values}` from a Pinecone sparse vector (dict or object)."""
    if value is None:
        return None
    indices = (
        value.get("indices")
        if isinstance(value, dict)
        else getattr(value, "indices", None)
    )
    values = (
        value.get("values")
        if isinstance(value, dict)
        else getattr(value, "values", None)
    )
    if indices is None or values is None:
        return None
    return {"indices": [int(i) for i in indices], "values": [float(v) for v in values]}


def _coerce_vector(entry: Any) -> tuple[str, list[float], dict, dict | None]:
    """Accept Pinecone (id, values, metadata) tuples or {"id","values",...} dicts,
    plus an optional `sparse_values` on the dict form."""
    if isinstance(entry, dict):
        record_id = entry["id"]
        values = entry["values"]
        metadata = entry.get("metadata") or {}
        sparse = _sparse_pair(entry.get("sparse_values"))
    else:
        record_id = entry[0]
        values = entry[1]
        metadata = entry[2] if len(entry) > 2 and entry[2] is not None else {}
        sparse = None
    return str(record_id), list(values), dict(metadata), sparse


class Index:
    """A Pinecone index handle; data lives in namespaces underneath it."""

    def __init__(self, store: NamespaceStore) -> None:
        self._store = store

    def upsert(
        self,
        vectors: list[Any],
        namespace: str = _DEFAULT_NAMESPACE,
        **_: Any,
    ) -> dict:
        ids: list[str] = []
        values: list[list[float]] = []
        metadata: list[dict] = []
        named_vectors: list[dict | None] = []
        has_sparse = False
        for entry in vectors:
            record_id, vector, meta, sparse = _coerce_vector(entry)
            ids.append(record_id)
            values.append(vector)
            metadata.append(meta)
            if sparse is not None:
                named_vectors.append({_SPARSE_VECTOR_NAME: sparse})
                has_sparse = True
            else:
                named_vectors.append(None)
        index = self._store.get(namespace)
        # Pinecone upsert overwrites existing ids. BORSUK's native upsert does
        # this atomically (new version + suppression in one manifest), so there
        # is no delete/purge dance and reads immediately see the new record.
        index.upsert(
            values,
            ids=ids,
            metadata=metadata,
            named_vectors=named_vectors if has_sparse else None,
        )
        return AttrDict(upserted_count=len(ids))

    def query(
        self,
        vector: list[float] | None = None,
        id: str | None = None,
        top_k: int = 10,
        filter: dict | None = None,
        include_values: bool = False,
        include_metadata: bool = False,
        namespace: str = _DEFAULT_NAMESPACE,
        sparse_vector: dict | None = None,
        **_: Any,
    ) -> dict:
        index = self._store.get(namespace)
        if vector is None and sparse_vector is None:
            if id is None:
                raise ValueError("query requires vector=, sparse_vector=, or id=")
            record = index.get_record(id)
            if record is None:
                return {"matches": [], "namespace": namespace}
            vector = record[0]

        sparse = _sparse_pair(sparse_vector)
        if sparse is not None:
            # Hybrid (or sparse-only) query: fuse the dense leg with the sparse
            # named-vector leg via reciprocal-rank fusion.
            hybrid_vectors: dict[str, Any] = {_SPARSE_VECTOR_NAME: sparse}
            if vector is not None:
                hybrid_vectors[""] = list(vector)
            report = index.search_hybrid_with_report(
                vectors=hybrid_vectors,
                k=top_k,
                include_metadata=bool(include_metadata),
            )
        else:
            report = index.search_with_report(
                list(vector),
                k=top_k,
                filter=filter,
                include_metadata=bool(include_metadata),
            )
        matches = []
        for hit in report.hits:
            match = AttrDict(id=hit.id, score=hit.distance)
            if include_metadata:
                match["metadata"] = hit.metadata or {}
            if include_values:
                fetched = index.get_record(hit.id)
                match["values"] = list(fetched[0]) if fetched else []
            matches.append(match)
        return AttrDict(matches=matches, namespace=namespace)

    def fetch(
        self, ids: list[str], namespace: str = _DEFAULT_NAMESPACE, **_: Any
    ) -> dict:
        index = self._store.get(namespace)
        vectors: dict[str, Any] = {}
        for record_id in ids:
            record = index.get_record(record_id)
            if record is None:
                continue
            vectors[record_id] = AttrDict(
                id=record_id, values=list(record[0]), metadata=record[1]
            )
        return AttrDict(vectors=vectors, namespace=namespace)

    def delete(
        self,
        ids: list[str] | None = None,
        delete_all: bool = False,
        filter: dict | None = None,
        namespace: str = _DEFAULT_NAMESPACE,
        **_: Any,
    ) -> dict:
        index = self._store.get(namespace)
        if filter is not None:
            raise NotImplementedError(
                "delete by metadata filter is not supported yet; pass ids= or delete_all=True"
            )
        if delete_all:
            raise NotImplementedError(
                "delete_all requires enumerating all ids; delete by ids= for now"
            )
        if not ids:
            return {}
        index.delete([str(record_id) for record_id in ids])
        return {}

    def list_paginated(
        self,
        prefix: str | None = None,
        limit: int = 100,
        pagination_token: str | None = None,
        namespace: str = _DEFAULT_NAMESPACE,
        **_: Any,
    ) -> dict:
        """One page of up to ``limit`` vector ids, mirroring Pinecone's
        ``list_paginated``. The pagination token is an opaque cursor (here, the
        source scan offset consumed so far).
        """
        if limit is None or limit <= 0:
            raise ValueError("limit must be a positive integer")
        index = self._store.get(namespace)
        offset = int(pagination_token) if pagination_token else 0
        ids: list[str] = []
        exhausted = False
        # Scan forward and apply the prefix *before* filling the page, advancing
        # the cursor by exactly the rows consumed. This keeps a page full even
        # when matches occur past the first `limit` records, and never counts
        # non-matching ids against `limit`.
        batch = max(limit, 100)
        while len(ids) < limit:
            rows = index.list_records(offset, batch)
            if not rows:
                exhausted = True
                break
            hit_limit = False
            consumed = 0
            for record in rows:
                consumed += 1
                record_id = record[0]
                if prefix and not record_id.startswith(prefix):
                    continue
                ids.append(record_id)
                if len(ids) == limit:
                    hit_limit = True
                    break
            offset += consumed
            if hit_limit:
                break
            if len(rows) < batch:
                exhausted = True
                break
        next_token = None if exhausted else str(offset)
        return AttrDict(
            vectors=[AttrDict(id=record_id) for record_id in ids],
            pagination=AttrDict(next=next_token),
            namespace=namespace,
        )

    def list(
        self,
        prefix: str | None = None,
        limit: int = 100,
        namespace: str = _DEFAULT_NAMESPACE,
        **_: Any,
    ):
        """Generator over pages of vector ids, auto-following the cursor —
        matching the real SDK's ``for ids in index.list(...)`` usage."""
        token: str | None = None
        while True:
            page = self.list_paginated(
                prefix=prefix, limit=limit, pagination_token=token, namespace=namespace
            )
            ids = [vector["id"] for vector in page["vectors"]]
            if ids:
                yield ids
            token = page["pagination"]["next"]
            if token is None:
                break

    def describe_index_stats(self, **_: Any) -> dict:
        namespaces: dict[str, Any] = {}
        total = 0
        for namespace in self._store.namespaces():
            index = self._store.get(namespace, create=False)
            count = index.stats().records
            namespaces[namespace] = AttrDict(vector_count=count)
            total += count
        return AttrDict(
            dimension=self._store.dimensions,
            total_vector_count=total,
            namespaces=namespaces,
        )
