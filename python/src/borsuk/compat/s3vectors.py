"""Drop-in Amazon S3 Vectors client backed by BORSUK.

Mimics the ``boto3.client("s3vectors")`` data-plane surface::

    # before: import boto3; s3v = boto3.client("s3vectors")
    from borsuk.compat.s3vectors import client
    s3v = client("s3vectors", base_uri="file:///data/vectors")

    s3v.create_vector_bucket(vectorBucketName="media")
    s3v.create_index(vectorBucketName="media", indexName="movies",
                     dimension=768, distanceMetric="cosine")
    s3v.put_vectors(vectorBucketName="media", indexName="movies",
                    vectors=[{"key": "star-wars", "data": {"float32": emb},
                              "metadata": {"genre": "scifi"}}])
    s3v.query_vectors(vectorBucketName="media", indexName="movies",
                      queryVector={"float32": emb}, topK=3,
                      filter={"genre": "scifi"}, returnMetadata=True)

A vector's id is called ``key`` here, and vector payloads are nested as
``{"float32": [...]}``, matching the AWS API. The backend is a local BORSUK
index rooted at ``<base_uri>/<bucket>/<index>``; there is no IAM or S3 service.
"""

from __future__ import annotations

from typing import Any

from ._common import NamespaceStore, map_metric

__all__ = ["client", "S3VectorsClient"]


def client(service_name: str = "s3vectors", *, base_uri: str, **_: Any) -> S3VectorsClient:
    """Factory mirroring ``boto3.client("s3vectors", ...)``."""
    if service_name != "s3vectors":
        raise ValueError(f"unsupported service {service_name!r}; expected 's3vectors'")
    return S3VectorsClient(base_uri=base_uri)


class _IndexConfig:
    __slots__ = ("dimension", "metric", "non_filterable")

    def __init__(self, dimension: int, metric: str, non_filterable: list[str]) -> None:
        self.dimension = dimension
        self.metric = metric
        self.non_filterable = non_filterable


class S3VectorsClient:
    """An S3 Vectors-compatible client. Buckets and indexes are local roots."""

    def __init__(self, *, base_uri: str) -> None:
        self._base_uri = base_uri.rstrip("/")
        self._buckets: set[str] = set()
        self._indexes: dict[tuple[str, str], _IndexConfig] = {}
        self._stores: dict[tuple[str, str], NamespaceStore] = {}

    # ---- Control plane ----------------------------------------------------

    def create_vector_bucket(self, vectorBucketName: str, **_: Any) -> dict:
        self._buckets.add(vectorBucketName)
        return {}

    def list_vector_buckets(self, **_: Any) -> dict:
        return {"vectorBuckets": [{"vectorBucketName": name} for name in sorted(self._buckets)]}

    def create_index(
        self,
        vectorBucketName: str,
        indexName: str,
        dimension: int,
        distanceMetric: str = "cosine",
        dataType: str = "float32",
        metadataConfiguration: dict | None = None,
        **_: Any,
    ) -> dict:
        self._buckets.add(vectorBucketName)
        non_filterable = list((metadataConfiguration or {}).get("nonFilterableMetadataKeys", []))
        self._indexes[(vectorBucketName, indexName)] = _IndexConfig(
            dimension=dimension,
            metric=map_metric("s3vectors", distanceMetric),
            non_filterable=non_filterable,
        )
        return {}

    def list_indexes(self, vectorBucketName: str, **_: Any) -> dict:
        return {
            "indexes": [
                {"vectorBucketName": bucket, "indexName": index}
                for (bucket, index) in sorted(self._indexes)
                if bucket == vectorBucketName
            ]
        }

    def get_index(self, vectorBucketName: str, indexName: str, **_: Any) -> dict:
        config = self._require_config(vectorBucketName, indexName)
        return {
            "index": {
                "vectorBucketName": vectorBucketName,
                "indexName": indexName,
                "dimension": config.dimension,
            }
        }

    # ---- Data plane -------------------------------------------------------

    def put_vectors(
        self,
        vectorBucketName: str,
        indexName: str,
        vectors: list[dict],
        **_: Any,
    ) -> dict:
        index = self._store(vectorBucketName, indexName).get("")
        ids: list[str] = []
        values: list[list[float]] = []
        metadata: list[dict] = []
        for vector in vectors:
            ids.append(str(vector["key"]))
            values.append(list(vector["data"]["float32"]))
            metadata.append(dict(vector.get("metadata") or {}))
        present = [key for key in ids if index.get_record(key) is not None]
        if present:
            index.delete(present)
            index.purge()
        index.add(values, ids=ids, metadata=metadata)
        return {}

    def query_vectors(
        self,
        vectorBucketName: str,
        indexName: str,
        queryVector: dict,
        topK: int = 10,
        filter: dict | None = None,
        returnMetadata: bool = False,
        returnDistance: bool = False,
        **_: Any,
    ) -> dict:
        index = self._store(vectorBucketName, indexName).get("")
        report = index.search_with_report(
            list(queryVector["float32"]),
            k=topK,
            filter=filter,
            include_metadata=bool(returnMetadata),
        )
        vectors = []
        for hit in report.hits:
            entry: dict[str, Any] = {"key": hit.id}
            if returnDistance:
                entry["distance"] = hit.distance
            if returnMetadata:
                entry["metadata"] = hit.metadata or {}
            vectors.append(entry)
        return {"vectors": vectors}

    def get_vectors(
        self,
        vectorBucketName: str,
        indexName: str,
        keys: list[str],
        returnData: bool = False,
        returnMetadata: bool = False,
        **_: Any,
    ) -> dict:
        index = self._store(vectorBucketName, indexName).get("")
        vectors = []
        for key in keys:
            record = index.get_record(str(key))
            if record is None:
                continue
            entry: dict[str, Any] = {"key": str(key)}
            if returnData:
                entry["data"] = {"float32": list(record[0])}
            if returnMetadata:
                entry["metadata"] = record[1]
            vectors.append(entry)
        return {"vectors": vectors}

    def delete_vectors(
        self,
        vectorBucketName: str,
        indexName: str,
        keys: list[str],
        **_: Any,
    ) -> dict:
        index = self._store(vectorBucketName, indexName).get("")
        index.delete([str(key) for key in keys])
        return {}

    # ---- Internals --------------------------------------------------------

    def _require_config(self, bucket: str, index: str) -> _IndexConfig:
        try:
            return self._indexes[(bucket, index)]
        except KeyError as exc:
            raise ValueError(
                f"index {index!r} in bucket {bucket!r} does not exist; call create_index first"
            ) from exc

    def _store(self, bucket: str, index: str) -> NamespaceStore:
        key = (bucket, index)
        store = self._stores.get(key)
        if store is not None:
            return store
        config = self._require_config(bucket, index)
        store = NamespaceStore(
            f"{self._base_uri}/{bucket}/{index}",
            metric=config.metric,
            dimensions=config.dimension,
        )
        self._stores[key] = store
        return store
