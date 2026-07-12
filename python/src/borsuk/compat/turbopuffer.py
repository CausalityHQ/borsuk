"""Drop-in turbopuffer client backed by BORSUK.

Mimics the turbopuffer Python SDK's namespace surface::

    # before: import turbopuffer; tpuf = turbopuffer.Turbopuffer(region=...)
    from borsuk.compat.turbopuffer import Turbopuffer
    tpuf = Turbopuffer(base_uri="file:///data/vectors", dimension=768)

    ns = tpuf.namespace("products")
    ns.write(upsert_rows=[{"id": 1, "vector": [0.1, 0.2, ...], "genre": "rock"}],
             distance_metric="cosine_distance")
    res = ns.query(rank_by=("vector", "ANN", [0.1, 0.2, ...]), top_k=10,
                   filters=("And", (("genre", "Eq", "rock"),)),
                   include_attributes=["genre"])

turbopuffer stores the vector inline as ``vector`` and every other row key as a
filterable attribute; the adapter maps those to BORSUK metadata. Filters use
turbopuffer's tuple syntax and are translated to BORSUK's operator dict.
"""

from __future__ import annotations

from typing import Any

from ._common import (
    AttrDict,
    NamespaceStore,
    map_metric,
    split_row,
    translate_turbopuffer_filter,
)

__all__ = ["Turbopuffer", "Namespace"]


class Turbopuffer:
    """turbopuffer-compatible client. ``api_key``/``region`` are accepted only."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_uri: str,
        dimension: int,
        region: str | None = None,
        default_distance_metric: str = "cosine_distance",
        **_: Any,
    ) -> None:
        self._base_uri = base_uri.rstrip("/")
        self._dimension = dimension
        self._default_metric = default_distance_metric
        self._namespaces: dict[str, Namespace] = {}

    def namespace(self, name: str) -> Namespace:
        existing = self._namespaces.get(name)
        if existing is not None:
            return existing
        namespace = Namespace(
            self._base_uri,
            name=name,
            dimension=self._dimension,
            default_metric=self._default_metric,
        )
        self._namespaces[name] = namespace
        return namespace


class Namespace:
    """A turbopuffer namespace handle backed by one BORSUK index."""

    def __init__(
        self,
        base_uri: str,
        *,
        name: str,
        dimension: int,
        default_metric: str,
    ) -> None:
        self._name = name
        self._dimension = dimension
        self._default_metric = default_metric
        self._store: NamespaceStore | None = None
        self._base_uri = base_uri

    def _index(self, distance_metric: str | None = None) -> Any:
        if self._store is None:
            metric = map_metric("turbopuffer", distance_metric or self._default_metric)
            self._store = NamespaceStore(
                f"{self._base_uri}/{self._name}",
                metric=metric,
                dimensions=self._dimension,
            )
        return self._store.get("")

    def write(
        self,
        upsert_rows: list[dict] | None = None,
        deletes: list[Any] | None = None,
        delete_by_filter: Any = None,
        distance_metric: str | None = None,
        **_: Any,
    ) -> dict:
        index = self._index(distance_metric)
        if delete_by_filter is not None:
            raise NotImplementedError(
                "delete_by_filter is not supported yet; pass deletes=[ids]"
            )
        if deletes:
            index.delete([str(record_id) for record_id in deletes])
        if upsert_rows:
            ids: list[str] = []
            values: list[list[float]] = []
            metadata: list[dict] = []
            for row in upsert_rows:
                record_id, vector, attrs = split_row(
                    row, id_key="id", vector_key="vector"
                )
                ids.append(record_id)
                values.append(vector)
                metadata.append(attrs)
            # Upserts overwrite existing ids atomically via BORSUK's native
            # upsert (no delete/purge dance).
            index.upsert(values, ids=ids, metadata=metadata)
            return {"rows_affected": len(ids)}
        return {"rows_affected": 0}

    def query(
        self,
        rank_by: tuple,
        top_k: int = 10,
        filters: Any = None,
        include_attributes: Any = None,
        **_: Any,
    ) -> list[dict]:
        if not (
            isinstance(rank_by, (tuple, list))
            and len(rank_by) == 3
            and rank_by[1] == "ANN"
        ):
            raise ValueError(
                'rank_by must be ("vector", "ANN", <query vector>); other ranks are unsupported'
            )
        query_vector = list(rank_by[2])
        index = self._index()
        include_metadata = include_attributes is not None
        report = index.search_with_report(
            query_vector,
            k=top_k,
            filter=translate_turbopuffer_filter(filters) or None,
            include_metadata=include_metadata,
        )
        wanted: set[str] | None = None
        if isinstance(include_attributes, (list, tuple, set)):
            wanted = {str(attr) for attr in include_attributes}
        results = []
        for hit in report.hits:
            row = AttrDict(id=hit.id, dist=hit.distance)
            for key, value in (hit.metadata or {}).items():
                if wanted is None or key in wanted:
                    row[key] = value
            results.append(row)
        return results
