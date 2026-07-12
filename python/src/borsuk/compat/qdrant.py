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

from collections.abc import Mapping
from typing import Any, NamedTuple

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


class _VectorSpec(NamedTuple):
    dimensions: int
    distance: str


class _CollectionConfig(NamedTuple):
    primary_name: str
    primary: _VectorSpec
    named_vectors: dict[str, _VectorSpec]


_SPARSE_NOT_SUPPORTED = (
    "BORSUK's Qdrant compatibility adapter supports named dense vectors only; "
    "sparse vectors are not implemented"
)


def _config_value(config: Any, key: str, default: Any = None) -> Any:
    if isinstance(config, Mapping):
        return config.get(key, default)
    return getattr(config, key, default)


def _point_value(point: Any, key: str, default: Any = None) -> Any:
    if isinstance(point, Mapping):
        return point.get(key, default)
    return getattr(point, key, default)


def _distance_name(distance: Any) -> str:
    return str(getattr(distance, "value", distance))


def _is_sparse_vector(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, Mapping):
        return "indices" in value and "values" in value
    return hasattr(value, "indices") and hasattr(value, "values")


def _is_sparse_config(config: Any) -> bool:
    if config is None:
        return False
    if _is_sparse_vector(config):
        return True
    if "sparse" in type(config).__name__.lower():
        return True
    if isinstance(config, Mapping):
        return "index" in config and "size" not in config
    return hasattr(config, "index") and not hasattr(config, "size")


def _reject_sparse(value: Any) -> None:
    if value:
        raise NotImplementedError(_SPARSE_NOT_SUPPORTED)


def _dense_vector(value: Any) -> list[float]:
    if _is_sparse_vector(value):
        raise NotImplementedError(_SPARSE_NOT_SUPPORTED)
    return list(value)


def _single_vector_config(config: Any) -> _VectorSpec:
    if _is_sparse_config(config):
        raise NotImplementedError(_SPARSE_NOT_SUPPORTED)
    size = int(_config_value(config, "size"))
    distance = _distance_name(_config_value(config, "distance", "Cosine"))
    return _VectorSpec(dimensions=size, distance=distance)


def _collection_config(vectors_config: Any) -> _CollectionConfig:
    if isinstance(vectors_config, Mapping) and not any(
        key in vectors_config for key in ("size", "distance")
    ):
        if not vectors_config:
            raise ValueError("vectors_config must contain at least one vector")
        items = list(vectors_config.items())
        for _, config in items:
            if _is_sparse_config(config):
                raise NotImplementedError(_SPARSE_NOT_SUPPORTED)
        primary_name, primary_config = items[0]
        named_vectors = {
            str(name): _single_vector_config(config) for name, config in items[1:]
        }
        return _CollectionConfig(
            primary_name=str(primary_name),
            primary=_single_vector_config(primary_config),
            named_vectors=named_vectors,
        )

    return _CollectionConfig(
        primary_name="",
        primary=_single_vector_config(vectors_config),
        named_vectors={},
    )


def _as_borsuk_named_vector(config: _CollectionConfig, name: Any) -> str:
    if name is None:
        return ""
    vector_name = str(name)
    if vector_name == "" or vector_name == config.primary_name:
        return ""
    return vector_name


def _split_point_vector(
    vector: Any, config: _CollectionConfig
) -> tuple[list[float], dict[str, list[float]] | None]:
    if _is_sparse_vector(vector):
        raise NotImplementedError(_SPARSE_NOT_SUPPORTED)
    if not isinstance(vector, Mapping):
        return _dense_vector(vector), None

    if config.primary_name:
        known_names = {config.primary_name, *config.named_vectors}
        unknown = sorted(str(name) for name in vector if str(name) not in known_names)
        if unknown:
            raise ValueError(
                "point vector contains unknown named vector(s): " + ", ".join(unknown)
            )
        try:
            primary = _dense_vector(vector[config.primary_name])
        except KeyError as exc:
            raise ValueError(
                f"point vector is missing primary named vector {config.primary_name!r}"
            ) from exc
        named_vectors = {
            str(name): _dense_vector(value)
            for name, value in vector.items()
            if str(name) != config.primary_name
        }
        return primary, named_vectors or None

    if "" in vector:
        return _dense_vector(vector[""]), None
    if len(vector) == 1:
        return _dense_vector(next(iter(vector.values()))), None
    raise ValueError(
        "unnamed Qdrant collections accept plain vectors or a single unnamed vector"
    )


def _query_vector_and_name(
    query_vector: Any, using: Any, config: _CollectionConfig
) -> tuple[list[float], str]:
    vector_name = using
    vector = query_vector

    if _is_sparse_vector(vector):
        raise NotImplementedError(_SPARSE_NOT_SUPPORTED)
    if isinstance(vector, Mapping):
        if "name" in vector and "vector" in vector:
            vector_name = vector_name if vector_name is not None else vector["name"]
            vector = vector["vector"]
        elif len(vector) == 1:
            vector_name, vector = next(iter(vector.items()))
        else:
            raise ValueError(f"unsupported Qdrant query vector: {query_vector!r}")
    elif hasattr(vector, "name") and hasattr(vector, "vector"):
        vector_name = vector_name if vector_name is not None else vector.name
        vector = vector.vector

    return _dense_vector(vector), _as_borsuk_named_vector(config, vector_name)


class QdrantClient:
    """A Qdrant-compatible client. ``location``/``url`` are accepted and ignored."""

    def __init__(self, base_uri: str, **_: Any) -> None:
        self._base_uri = base_uri.rstrip("/")
        self._stores: dict[str, NamespaceStore] = {}
        self._configs: dict[str, _CollectionConfig] = {}

    def create_collection(
        self, collection_name: str, vectors_config: Any, **kw: Any
    ) -> bool:
        _reject_sparse(kw.get("sparse_vectors_config"))
        _reject_sparse(kw.get("sparse_vectors"))
        self._configs[collection_name] = _collection_config(vectors_config)
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
        config = self._configs[collection_name]
        ids = [str(_point_value(p, "id")) for p in points]
        vectors: list[list[float]] = []
        named_vectors: list[dict[str, list[float]] | None] = []
        has_named_vectors = False
        for point in points:
            vector, named = _split_point_vector(_point_value(point, "vector"), config)
            vectors.append(vector)
            named_vectors.append(named)
            has_named_vectors = has_named_vectors or named is not None
        payloads = [dict(_point_value(p, "payload") or {}) for p in points]
        # Qdrant upsert overwrites an existing point; BORSUK's native upsert
        # replaces every representation (dense + named vectors) atomically.
        index.upsert(
            vectors,
            ids=ids,
            metadata=payloads,
            named_vectors=named_vectors if has_named_vectors else None,
        )
        return {"status": "completed", "operation_id": 0}

    def search(
        self,
        collection_name: str,
        query_vector: Any,
        query_filter: Any = None,
        limit: int = 10,
        with_payload: bool = True,
        with_vectors: bool = False,
        **kw: Any,
    ) -> list[ScoredPoint]:
        index = self._index(collection_name)
        query, vector_name = _query_vector_and_name(
            query_vector, kw.get("using"), self._configs[collection_name]
        )
        report = index.search_with_report(
            query,
            k=limit,
            filter=translate_qdrant_filter(query_filter),
            vector=vector_name,
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

    def query_points(
        self,
        collection_name: str,
        query: Any,
        query_filter: Any = None,
        limit: int = 10,
        with_payload: bool = True,
        with_vectors: bool = False,
        using: Any = None,
        **kw: Any,
    ) -> Any:
        if query_filter is None:
            query_filter = kw.get("filter")
        return AttrDict(
            points=self.search(
                collection_name,
                query_vector=query,
                query_filter=query_filter,
                limit=limit,
                with_payload=with_payload,
                with_vectors=with_vectors,
                using=using,
            )
        )

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
            config = self._configs[collection_name]
            create_kwargs: dict[str, Any] = {}
            if config.named_vectors:
                create_kwargs["named_vectors"] = {
                    name: {
                        "dimensions": spec.dimensions,
                        "metric": map_metric("qdrant", spec.distance),
                    }
                    for name, spec in config.named_vectors.items()
                }
            store = NamespaceStore(
                f"{self._base_uri}/{collection_name}",
                metric=map_metric("qdrant", config.primary.distance),
                dimensions=config.primary.dimensions,
                **create_kwargs,
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
