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
        )
        index = Index(store)
        self._indexes[key] = index
        return index


def _coerce_vector(entry: Any) -> tuple[str, list[float], dict]:
    """Accept Pinecone (id, values, metadata) tuples or {"id","values",...} dicts."""
    if isinstance(entry, dict):
        record_id = entry["id"]
        values = entry["values"]
        metadata = entry.get("metadata") or {}
    else:
        record_id = entry[0]
        values = entry[1]
        metadata = entry[2] if len(entry) > 2 and entry[2] is not None else {}
    return str(record_id), list(values), dict(metadata)


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
        for entry in vectors:
            record_id, vector, meta = _coerce_vector(entry)
            ids.append(record_id)
            values.append(vector)
            metadata.append(meta)
        index = self._store.get(namespace)
        # Pinecone upsert overwrites existing ids. BORSUK's native upsert does
        # this atomically (new version + suppression in one manifest), so there
        # is no delete/purge dance and reads immediately see the new record.
        index.upsert(values, ids=ids, metadata=metadata)
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
        **_: Any,
    ) -> dict:
        index = self._store.get(namespace)
        if vector is None:
            if id is None:
                raise ValueError("query requires either vector= or id=")
            record = index.get_record(id)
            if record is None:
                return {"matches": [], "namespace": namespace}
            vector = record[0]
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
