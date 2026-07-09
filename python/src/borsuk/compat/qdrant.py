"""Drop-in Qdrant client backed by BORSUK.

Mimics the qdrant-client data-plane surface::

    # before: from qdrant_client import QdrantClient; c = QdrantClient(path="…")
    from borsuk.compat.qdrant import QdrantClient
    c = QdrantClient(base_uri="file:///data/vectors")

    c.create_collection("docs", vectors_config={"size": 768, "distance": "Cosine"})
    c.upsert("docs", points=[{"id": "a", "vector": [0.1, 0.2, ...],
                              "payload": {"genre": "rock"}}])
    hits = c.search("docs", query_vector=[0.1, 0.2, ...], limit=10,
                    query_filter={"must": [{"key": "genre", "match": {"value": "rock"}}]},
                    with_payload=True)

Qdrant's structured ``Filter`` (must / should / must_not with FieldCondition +
match / range) is accepted in its plain-dict form and translated to BORSUK's
operator dict. Payloads map to BORSUK metadata. Local embedded backend: one
BORSUK index per collection under a base URI.
"""

from __future__ import annotations

from typing import Any

from ._common import AttrDict, NamespaceStore, map_metric

__all__ = ["QdrantClient", "ScoredPoint", "Record", "translate_qdrant_filter"]


class ScoredPoint:
    """A search hit, mirroring qdrant_client's ScoredPoint attributes."""

    __slots__ = ("id", "score", "payload", "vector")

    def __init__(
        self, id: str, score: float, payload: dict | None, vector: list[float] | None
    ) -> None:
        self.id = id
        self.score = score
        self.payload = payload
        self.vector = vector

    def __repr__(self) -> str:
        return f"ScoredPoint(id={self.id!r}, score={self.score})"


class Record:
    """A retrieved point, mirroring qdrant_client's Record attributes."""

    __slots__ = ("id", "payload", "vector")

    def __init__(
        self, id: str, payload: dict | None, vector: list[float] | None
    ) -> None:
        self.id = id
        self.payload = payload
        self.vector = vector


def _config_value(config: Any, key: str, default: Any = None) -> Any:
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


def _point_value(point: Any, key: str, default: Any = None) -> Any:
    if isinstance(point, dict):
        return point.get(key, default)
    return getattr(point, key, default)


class QdrantClient:
    """A Qdrant-compatible client. ``location``/``url`` are accepted and ignored."""

    def __init__(self, base_uri: str, **_: Any) -> None:
        self._base_uri = base_uri.rstrip("/")
        self._stores: dict[str, NamespaceStore] = {}
        self._configs: dict[str, tuple[int, str]] = {}

    def create_collection(
        self, collection_name: str, vectors_config: Any, **_: Any
    ) -> bool:
        size = int(_config_value(vectors_config, "size"))
        distance = _config_value(vectors_config, "distance", "Cosine")
        self._configs[collection_name] = (size, str(distance))
        return True

    def recreate_collection(
        self, collection_name: str, vectors_config: Any, **kw: Any
    ) -> bool:
        self._stores.pop(collection_name, None)
        return self.create_collection(collection_name, vectors_config, **kw)

    def collection_exists(self, collection_name: str, **_: Any) -> bool:
        return collection_name in self._configs

    def upsert(self, collection_name: str, points: list[Any], **_: Any) -> Any:
        index = self._index(collection_name)
        ids = [str(_point_value(p, "id")) for p in points]
        vectors = [list(_point_value(p, "vector")) for p in points]
        payloads = [dict(_point_value(p, "payload") or {}) for p in points]
        present = [i for i in ids if index.get_record(i) is not None]
        if present:
            index.delete(present)
            index.purge()
        index.add(vectors, ids=ids, metadata=payloads)
        return {"status": "completed", "operation_id": 0}

    def search(
        self,
        collection_name: str,
        query_vector: list[float],
        query_filter: Any = None,
        limit: int = 10,
        with_payload: bool = True,
        with_vectors: bool = False,
        **_: Any,
    ) -> list[ScoredPoint]:
        index = self._index(collection_name)
        report = index.search_with_report(
            list(query_vector),
            k=limit,
            filter=translate_qdrant_filter(query_filter),
            include_metadata=bool(with_payload),
        )
        hits = []
        for hit in report.hits:
            vector = None
            if with_vectors:
                record = index.get_record(hit.id)
                vector = list(record[0]) if record else None
            hits.append(
                ScoredPoint(
                    id=hit.id,
                    score=hit.distance,
                    payload=hit.metadata if with_payload else None,
                    vector=vector,
                )
            )
        return hits

    def retrieve(
        self,
        collection_name: str,
        ids: list[str],
        with_payload: bool = True,
        with_vectors: bool = False,
        **_: Any,
    ) -> list[Record]:
        index = self._index(collection_name)
        records = []
        for record_id in ids:
            record = index.get_record(str(record_id))
            if record is None:
                continue
            records.append(
                Record(
                    id=str(record_id),
                    payload=record[1] if with_payload else None,
                    vector=list(record[0]) if with_vectors else None,
                )
            )
        return records

    def scroll(
        self,
        collection_name: str,
        limit: int = 10,
        offset: int = 0,
        with_payload: bool = True,
        with_vectors: bool = False,
        **_: Any,
    ) -> tuple[list[Record], int | None]:
        index = self._index(collection_name)
        rows = index.list_records(offset, limit + 1)
        next_offset = offset + limit if len(rows) > limit else None
        records = [
            Record(
                id=rid,
                payload=meta if with_payload else None,
                vector=list(vec) if with_vectors else None,
            )
            for rid, vec, meta in rows[:limit]
        ]
        return records, next_offset

    def delete(self, collection_name: str, points_selector: Any, **_: Any) -> Any:
        index = self._index(collection_name)
        if isinstance(points_selector, dict):
            ids = points_selector.get("points", points_selector)
        else:
            ids = getattr(points_selector, "points", points_selector)
        index.delete([str(i) for i in ids])
        # Immediate-delete semantics: physically drop the rows so count() and
        # re-upserts see them gone right away.
        index.purge()
        return {"status": "completed"}

    def count(self, collection_name: str, **_: Any) -> Any:
        # qdrant-client returns a CountResult with a `.count` attribute.
        return AttrDict(count=self._index(collection_name).stats().records)

    def _index(self, collection_name: str) -> Any:
        store = self._stores.get(collection_name)
        if store is None:
            if collection_name not in self._configs:
                raise ValueError(f"collection {collection_name!r} does not exist")
            size, distance = self._configs[collection_name]
            store = NamespaceStore(
                f"{self._base_uri}/{collection_name}",
                metric=map_metric("qdrant", distance),
                dimensions=size,
            )
            self._stores[collection_name] = store
        return store.get("")


def _field_condition(cond: Any) -> dict:
    key = _point_value(cond, "key")
    match = _point_value(cond, "match")
    rng = _point_value(cond, "range")
    if match is not None:
        if _point_value(match, "any") is not None:
            return {str(key): {"$in": _point_value(match, "any")}}
        if _point_value(match, "except") is not None:
            return {str(key): {"$nin": _point_value(match, "except")}}
        return {str(key): {"$eq": _point_value(match, "value")}}
    if rng is not None:
        ops = {}
        for name, op in (
            ("gt", "$gt"),
            ("gte", "$gte"),
            ("lt", "$lt"),
            ("lte", "$lte"),
        ):
            value = _point_value(rng, name)
            if value is not None:
                ops[op] = value
        return {str(key): ops}
    raise ValueError(f"unsupported Qdrant condition: {cond!r}")


def translate_qdrant_filter(query_filter: Any) -> dict | None:
    """Convert a Qdrant ``Filter`` (dict or object) into a BORSUK operator dict."""
    if query_filter is None:
        return None
    # A plain dict without Qdrant's must/should/must_not keys is already a
    # BORSUK-style operator dict -> pass through unchanged.
    if isinstance(query_filter, dict) and not any(
        k in query_filter for k in ("must", "should", "must_not")
    ):
        return query_filter

    must = _point_value(query_filter, "must") or []
    should = _point_value(query_filter, "should") or []
    must_not = _point_value(query_filter, "must_not") or []
    clauses: list[dict] = [_field_condition(c) for c in must]
    if should:
        clauses.append({"$or": [_field_condition(c) for c in should]})
    if must_not:
        clauses.append({"$not": {"$or": [_field_condition(c) for c in must_not]}})
    if not clauses:
        return None
    return clauses[0] if len(clauses) == 1 else {"$and": clauses}
