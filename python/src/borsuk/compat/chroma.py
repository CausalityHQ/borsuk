"""Drop-in Chroma client backed by BORSUK.

Mimics chromadb's collection surface::

    # before: import chromadb; client = chromadb.PersistentClient(path="…")
    from borsuk.compat.chroma import Client
    client = Client(base_uri="file:///data/vectors", dimensions=768)

    col = client.get_or_create_collection("docs")
    col.add(ids=["a"], embeddings=[[0.1, 0.2, ...]],
            metadatas=[{"genre": "rock"}], documents=["hello"])
    col.query(query_embeddings=[[0.1, 0.2, ...]], n_results=10,
              where={"genre": "rock"}, include=["metadatas", "documents", "distances"])

Chroma's ``where`` filter already uses a Mongo-style operator dict, so it maps to
BORSUK directly. Documents are stored under a reserved metadata key. This is a
local, embedded backend — one BORSUK index per collection under a base URI.
"""

from __future__ import annotations

from typing import Any

from ._common import NamespaceStore, map_metric

__all__ = ["Client", "Collection"]

# Chroma stores an optional text document per record; keep it in metadata under a
# reserved key so it round-trips without a separate column.
_DOCUMENT_KEY = "__document__"


def _space_to_metric(metadata: dict | None) -> str:
    space = (metadata or {}).get("hnsw:space", "l2")
    return map_metric("chroma", space)


class Client:
    """Chroma-compatible client. Collections are BORSUK indexes under base_uri."""

    def __init__(self, *, base_uri: str, dimensions: int, **_: Any) -> None:
        self._base_uri = base_uri.rstrip("/")
        self._dimensions = dimensions
        self._collections: dict[str, Collection] = {}

    def create_collection(
        self, name: str, metadata: dict | None = None, **_: Any
    ) -> Collection:
        if name in self._collections:
            raise ValueError(f"collection {name!r} already exists")
        return self._make(name, metadata)

    def get_collection(self, name: str, **_: Any) -> Collection:
        if name not in self._collections:
            raise ValueError(f"collection {name!r} does not exist")
        return self._collections[name]

    def get_or_create_collection(
        self, name: str, metadata: dict | None = None, **_: Any
    ) -> Collection:
        return self._collections.get(name) or self._make(name, metadata)

    def delete_collection(self, name: str, **_: Any) -> None:
        self._collections.pop(name, None)

    def _make(self, name: str, metadata: dict | None) -> Collection:
        store = NamespaceStore(
            f"{self._base_uri}/{name}",
            metric=_space_to_metric(metadata),
            dimensions=self._dimensions,
        )
        collection = Collection(name, store)
        self._collections[name] = collection
        return collection


class Collection:
    def __init__(self, name: str, store: NamespaceStore) -> None:
        self.name = name
        self._store = store

    def _index(self) -> Any:
        return self._store.get("")

    def add(
        self,
        ids: list[str],
        embeddings: list[list[float]],
        metadatas: list[dict] | None = None,
        documents: list[str] | None = None,
        **_: Any,
    ) -> None:
        index = self._index()
        prepared = _merge_documents(ids, metadatas, documents)
        # Chroma upsert overwrites existing ids; use BORSUK's native atomic upsert.
        index.upsert(
            [list(v) for v in embeddings], ids=[str(i) for i in ids], metadata=prepared
        )

    # Chroma uses ``upsert`` as an alias of add-with-overwrite.
    upsert = add

    def query(
        self,
        query_embeddings: list[list[float]],
        n_results: int = 10,
        where: dict | None = None,
        include: list[str] | None = None,
        **_: Any,
    ) -> dict:
        index = self._index()
        include = include or ["metadatas", "documents", "distances"]
        want_meta = "metadatas" in include or "documents" in include
        result: dict[str, list] = {"ids": []}
        for field in ("distances", "metadatas", "documents", "embeddings"):
            if field in include:
                result[field] = []
        for query in query_embeddings:
            report = index.search_with_report(
                list(query), k=n_results, filter=where, include_metadata=want_meta
            )
            result["ids"].append([hit.id for hit in report.hits])
            if "distances" in result:
                result["distances"].append([hit.distance for hit in report.hits])
            if "metadatas" in result:
                result["metadatas"].append(
                    [_strip_document(hit.metadata) for hit in report.hits]
                )
            if "documents" in result:
                result["documents"].append(
                    [(hit.metadata or {}).get(_DOCUMENT_KEY) for hit in report.hits]
                )
            if "embeddings" in result:
                result["embeddings"].append(
                    [_vector_of(index, hit.id) for hit in report.hits]
                )
        return result

    def get(
        self,
        ids: list[str] | None = None,
        where: dict | None = None,
        limit: int | None = None,
        offset: int = 0,
        include: list[str] | None = None,
        **_: Any,
    ) -> dict:
        index = self._index()
        include = include or ["metadatas", "documents"]
        rows: list[tuple[str, list[float], dict]]
        if ids is not None:
            rows = []
            for record_id in ids:
                record = index.get_record(str(record_id))
                if record is not None:
                    rows.append((str(record_id), record[0], record[1]))
        else:
            rows = [
                (rid, vec, meta)
                for rid, vec, meta in index.list_records(offset, limit or 1_000_000)
            ]
        # Chroma applies `where` on get too (evaluated here over listed rows).
        if where is not None:
            rows = [r for r in rows if _matches_where(r[2], where)]
        out: dict[str, list] = {"ids": [r[0] for r in rows]}
        if "metadatas" in include:
            out["metadatas"] = [_strip_document(r[2]) for r in rows]
        if "documents" in include:
            out["documents"] = [r[2].get(_DOCUMENT_KEY) for r in rows]
        if "embeddings" in include:
            out["embeddings"] = [r[1] for r in rows]
        return out

    def peek(self, limit: int = 10, **_: Any) -> dict:
        return self.get(limit=limit)

    def delete(
        self, ids: list[str] | None = None, where: dict | None = None, **_: Any
    ) -> None:
        index = self._index()
        if ids is None:
            raise NotImplementedError(
                "delete by `where` is not supported yet; pass ids"
            )
        index.delete([str(i) for i in ids])
        # Immediate-delete semantics: physically drop the rows so count() and
        # re-adds see them gone right away.
        index.purge()

    def count(self) -> int:
        return self._index().stats().records


def _merge_documents(
    ids: list[str], metadatas: list[dict] | None, documents: list[str] | None
) -> list[dict]:
    merged: list[dict] = []
    for i in range(len(ids)):
        meta = dict(metadatas[i]) if metadatas and metadatas[i] else {}
        if documents and i < len(documents) and documents[i] is not None:
            meta[_DOCUMENT_KEY] = documents[i]
        merged.append(meta)
    return merged


def _strip_document(metadata: dict | None) -> dict:
    if not metadata:
        return {}
    return {k: v for k, v in metadata.items() if k != _DOCUMENT_KEY}


def _vector_of(index: Any, record_id: str) -> list[float]:
    record = index.get_record(record_id)
    return list(record[0]) if record else []


def _matches_where(metadata: dict, where: dict) -> bool:
    # Minimal Chroma-style where evaluation for get() post-filtering.
    for key, cond in where.items():
        if key in ("$and", "$or"):
            results = [_matches_where(metadata, c) for c in cond]
            if key == "$and" and not all(results):
                return False
            if key == "$or" and not any(results):
                return False
            continue
        value = metadata.get(key)
        if isinstance(cond, dict):
            for op, operand in cond.items():
                if op == "$eq" and value != operand:
                    return False
                if op == "$ne" and value == operand:
                    return False
                if op == "$in" and value not in operand:
                    return False
                if op == "$nin" and value in operand:
                    return False
                if op == "$gt" and not (value is not None and value > operand):
                    return False
                if op == "$gte" and not (value is not None and value >= operand):
                    return False
                if op == "$lt" and not (value is not None and value < operand):
                    return False
                if op == "$lte" and not (value is not None and value <= operand):
                    return False
        elif value != cond:
            return False
    return True
