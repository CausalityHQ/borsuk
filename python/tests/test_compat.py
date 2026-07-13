"""Drop-in adapter tests: each emulated SDK surface must round-trip data through
BORSUK — create, upsert with metadata, filtered query, fetch/get, delete."""

import tempfile
import unittest
from pathlib import Path

from borsuk.compat._common import map_metric, translate_turbopuffer_filter
from borsuk.compat.chroma import Client as ChromaClient
from borsuk.compat.pinecone import Pinecone
from borsuk.compat.qdrant import QdrantClient, translate_qdrant_filter
from borsuk.compat.s3vectors import client as s3vectors_client
from borsuk.compat.turbopuffer import Turbopuffer


def base_uri(tmp: str) -> str:
    return Path(tmp).as_uri()


class PineconeAdapterTest(unittest.TestCase):
    def test_upsert_query_fetch_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pc = Pinecone(
                api_key="ignored", base_uri=base_uri(tmp), dimension=2, metric="cosine"
            )
            index = pc.Index("songs")
            index.upsert(
                [
                    ("a", [1.0, 0.0], {"genre": "rock", "year": 1975}),
                    {
                        "id": "b",
                        "values": [0.0, 1.0],
                        "metadata": {"genre": "jazz", "year": 1999},
                    },
                    ("c", [1.0, 0.1], {"genre": "rock", "year": 2001}),
                ],
                namespace="store-1",
            )

            res = index.query(
                vector=[1.0, 0.0],
                top_k=10,
                filter={"genre": {"$eq": "rock"}},
                include_metadata=True,
                include_values=True,
                namespace="store-1",
            )
            ids = [match["id"] for match in res["matches"]]
            self.assertEqual(set(ids), {"a", "c"})
            self.assertTrue(
                all(match["metadata"]["genre"] == "rock" for match in res["matches"])
            )
            self.assertEqual(len(res["matches"][0]["values"]), 2)
            # Response reads the same by attribute or by key, like the real SDK.
            self.assertEqual([m.id for m in res.matches], ids)
            self.assertEqual(res.matches[0].metadata["genre"], "rock")
            self.assertEqual(res.namespace, "store-1")

            fetched = index.fetch(["a"], namespace="store-1")
            self.assertEqual(fetched["vectors"]["a"]["metadata"]["year"], 1975)
            self.assertEqual(fetched.vectors["a"].id, "a")

            self.assertEqual(index.upsert([("z", [3.0, 3.0], {})]).upserted_count, 1)

            # Namespaces are isolated.
            self.assertEqual(
                index.query(vector=[1.0, 0.0], namespace="other")["matches"], []
            )

            index.delete(ids=["a"], namespace="store-1")
            after = index.query(
                vector=[1.0, 0.0], filter={"genre": "rock"}, namespace="store-1"
            )
            self.assertEqual([m["id"] for m in after["matches"]], ["c"])

            stats = index.describe_index_stats()
            self.assertEqual(stats["dimension"], 2)

    def test_upsert_overwrites_existing_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pc = Pinecone(base_uri=base_uri(tmp), dimension=2, metric="euclidean")
            index = pc.Index("i")
            index.upsert([("x", [0.0, 0.0], {"v": 1})])
            index.upsert([("x", [9.0, 9.0], {"v": 2})])
            fetched = index.fetch(["x"])
            self.assertEqual(fetched["vectors"]["x"]["metadata"]["v"], 2)

    def test_sparse_values_hybrid_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pc = Pinecone(base_uri=base_uri(tmp), dimension=2, metric="euclidean")
            idx = pc.Index("docs")
            idx.upsert(
                [
                    {
                        "id": "a",
                        "values": [1.0, 0.0],
                        "sparse_values": {"indices": [5, 7], "values": [1.0, 2.0]},
                        "metadata": {"k": "a"},
                    },
                    {
                        "id": "b",
                        "values": [0.0, 1.0],
                        "sparse_values": {"indices": [5, 9], "values": [3.0, 1.0]},
                        "metadata": {"k": "b"},
                    },
                ]
            )
            # Sparse-only query: term 7 is in "a" only.
            res = idx.query(
                sparse_vector={"indices": [7], "values": [1.0]},
                top_k=5,
                include_metadata=True,
            )
            self.assertEqual([m["id"] for m in res["matches"]], ["a"])
            self.assertEqual(res["matches"][0]["metadata"]["k"], "a")
            # Hybrid: dense near "a" + shared sparse term 5 -> "a" ranks first.
            res2 = idx.query(
                vector=[1.0, 0.0],
                sparse_vector={"indices": [5], "values": [1.0]},
                top_k=5,
            )
            self.assertEqual(res2["matches"][0]["id"], "a")


class PineconeContractTest(unittest.TestCase):
    """Pin the documented Pinecone data-plane contract, not just a happy path:
    query-by-id, the full ``$``-operator filter set, top_k limiting, default
    include flags, and describe_index_stats counts. These are the behaviors a real
    Pinecone application depends on when it 'changes the import'."""

    def _index(self, tmp: str):
        pc = Pinecone(base_uri=base_uri(tmp), dimension=2, metric="euclidean")
        index = pc.Index("catalog")
        index.upsert(
            [
                ("a", [0.0, 0.0], {"genre": "rock", "year": 1971, "live": True}),
                ("b", [1.0, 0.0], {"genre": "jazz", "year": 1985, "live": False}),
                ("c", [2.0, 0.0], {"genre": "rock", "year": 1999, "live": False}),
                ("d", [3.0, 0.0], {"genre": "folk", "year": 2010, "live": True}),
            ]
        )
        return index

    def test_query_by_id_uses_that_records_vector(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = self._index(tmp)
            # Pinecone query(id=...) ranks by the stored vector of that id, so the
            # id itself comes back as the nearest match (distance 0).
            res = index.query(id="a", top_k=2)
            self.assertEqual(res["matches"][0]["id"], "a")
            # A missing id yields no matches rather than an error.
            self.assertEqual(index.query(id="missing", top_k=2)["matches"], [])

    def test_top_k_limits_result_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = self._index(tmp)
            self.assertEqual(len(index.query(vector=[0.0, 0.0], top_k=2)["matches"]), 2)

    def test_include_flags_default_off(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = self._index(tmp)
            match = index.query(vector=[0.0, 0.0], top_k=1)["matches"][0]
            # Pinecone omits metadata/values unless explicitly requested.
            self.assertNotIn("metadata", match)
            self.assertNotIn("values", match)

    def _ids(self, index, filter_: dict) -> set:
        res = index.query(vector=[0.0, 0.0], top_k=10, filter=filter_)
        return {m["id"] for m in res["matches"]}

    def test_comparison_and_set_filter_operators(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = self._index(tmp)
            self.assertEqual(self._ids(index, {"year": {"$gte": 1999}}), {"c", "d"})
            self.assertEqual(self._ids(index, {"year": {"$lt": 1985}}), {"a"})
            self.assertEqual(self._ids(index, {"year": {"$gt": 1985}}), {"c", "d"})
            self.assertEqual(self._ids(index, {"genre": {"$ne": "rock"}}), {"b", "d"})
            self.assertEqual(
                self._ids(index, {"genre": {"$in": ["rock", "folk"]}}), {"a", "c", "d"}
            )
            self.assertEqual(
                self._ids(index, {"genre": {"$nin": ["rock", "folk"]}}), {"b"}
            )
            self.assertEqual(self._ids(index, {"live": {"$eq": True}}), {"a", "d"})

    def test_multiple_filter_keys_are_conjunctive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = self._index(tmp)
            # Two keys in one filter dict AND together, as Pinecone specifies.
            self.assertEqual(
                self._ids(index, {"genre": "rock", "year": {"$gte": 1999}}), {"c"}
            )

    def test_describe_index_stats_counts_namespaces(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pc = Pinecone(base_uri=base_uri(tmp), dimension=2, metric="euclidean")
            index = pc.Index("catalog")
            index.upsert([("a", [0.0, 0.0], {})], namespace="ns-1")
            index.upsert(
                [("b", [1.0, 0.0], {}), ("c", [2.0, 0.0], {})], namespace="ns-2"
            )
            stats = index.describe_index_stats()
            self.assertEqual(stats["dimension"], 2)
            self.assertEqual(stats["total_vector_count"], 3)
            self.assertEqual(stats["namespaces"]["ns-1"]["vector_count"], 1)
            self.assertEqual(stats["namespaces"]["ns-2"]["vector_count"], 2)

    def test_delete_removes_only_targeted_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = self._index(tmp)
            index.delete(ids=["a", "c"])
            remaining = {
                m["id"] for m in index.query(vector=[0.0, 0.0], top_k=10)["matches"]
            }
            self.assertEqual(remaining, {"b", "d"})

    def test_delete_by_filter_raises_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = self._index(tmp)
            # Unsupported control-plane-ish calls fail loudly, not silently.
            with self.assertRaises(NotImplementedError):
                index.delete(filter={"genre": "rock"})
            with self.assertRaises(NotImplementedError):
                index.delete(delete_all=True)

    def test_list_and_list_paginated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = self._index(tmp)  # ids a, b, c, d
            # list_paginated returns a page of ids plus a forward cursor.
            page = index.list_paginated(limit=2)
            self.assertEqual(len(page["vectors"]), 2)
            self.assertIsNotNone(page["pagination"]["next"])
            # list() auto-follows the cursor across every id.
            seen = [record_id for ids in index.list(limit=2) for record_id in ids]
            self.assertEqual(set(seen), {"a", "b", "c", "d"})
            # A prefix narrows the enumeration.
            index.upsert([("zzz-1", [0.0, 0.0], {})])
            zzz = [record_id for ids in index.list(prefix="zzz") for record_id in ids]
            self.assertEqual(zzz, ["zzz-1"])

    def test_list_paginated_prefix_filters_before_paginating(self) -> None:
        # A prefix that matches only records past the first `limit` must still be
        # found in a single page — the prefix is applied before the page fills,
        # not after slicing, so `limit` never counts non-matching ids.
        with tempfile.TemporaryDirectory() as tmp:
            pc = Pinecone(base_uri=base_uri(tmp), dimension=2, metric="euclidean")
            index = pc.Index("c")
            index.upsert([(f"a{i}", [float(i), 0.0], {}) for i in range(10)])
            index.upsert([("target", [0.0, 1.0], {})])
            page = index.list_paginated(prefix="target", limit=2)
            self.assertEqual([v["id"] for v in page["vectors"]], ["target"])

    def test_list_rejects_nonpositive_limit(self) -> None:
        # limit<=0 must raise, not hand back a non-advancing cursor that would
        # make list() loop forever.
        with tempfile.TemporaryDirectory() as tmp:
            index = self._index(tmp)
            with self.assertRaises(ValueError):
                index.list_paginated(limit=0)
            with self.assertRaises(ValueError):
                next(iter(index.list(limit=0)))


class S3VectorsAdapterTest(unittest.TestCase):
    def test_put_query_get_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s3v = s3vectors_client("s3vectors", base_uri=base_uri(tmp))
            s3v.create_vector_bucket(vectorBucketName="media")
            s3v.create_index(
                vectorBucketName="media",
                indexName="movies",
                dimension=2,
                distanceMetric="cosine",
            )
            s3v.put_vectors(
                vectorBucketName="media",
                indexName="movies",
                vectors=[
                    {
                        "key": "star-wars",
                        "data": {"float32": [1.0, 0.0]},
                        "metadata": {"genre": "scifi"},
                    },
                    {
                        "key": "casablanca",
                        "data": {"float32": [0.0, 1.0]},
                        "metadata": {"genre": "drama"},
                    },
                ],
            )

            res = s3v.query_vectors(
                vectorBucketName="media",
                indexName="movies",
                queryVector={"float32": [1.0, 0.0]},
                topK=5,
                filter={"genre": "scifi"},
                returnMetadata=True,
                returnDistance=True,
            )
            self.assertEqual([v["key"] for v in res["vectors"]], ["star-wars"])
            self.assertEqual(res["vectors"][0]["metadata"]["genre"], "scifi")
            self.assertIn("distance", res["vectors"][0])

            got = s3v.get_vectors(
                vectorBucketName="media",
                indexName="movies",
                keys=["star-wars"],
                returnData=True,
                returnMetadata=True,
            )
            self.assertEqual(got["vectors"][0]["data"]["float32"], [1.0, 0.0])

            s3v.delete_vectors(
                vectorBucketName="media", indexName="movies", keys=["star-wars"]
            )
            after = s3v.query_vectors(
                vectorBucketName="media",
                indexName="movies",
                queryVector={"float32": [1.0, 0.0]},
                topK=5,
            )
            self.assertEqual([v["key"] for v in after["vectors"]], ["casablanca"])

    def test_query_missing_index_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s3v = s3vectors_client("s3vectors", base_uri=base_uri(tmp))
            with self.assertRaises(ValueError):
                s3v.query_vectors(
                    vectorBucketName="nope",
                    indexName="nope",
                    queryVector={"float32": [1.0, 0.0]},
                )


class S3VectorsContractTest(unittest.TestCase):
    """Pin the S3 Vectors data-plane contract: PutVectors overwrite-by-key, topK,
    the filter operator set, return flags defaulting off, partial GetVectors, and
    bucket/index isolation."""

    def _client(self, tmp: str):
        s3v = s3vectors_client("s3vectors", base_uri=base_uri(tmp))
        s3v.create_vector_bucket(vectorBucketName="media")
        s3v.create_index(
            vectorBucketName="media",
            indexName="movies",
            dimension=2,
            distanceMetric="euclidean",
        )
        s3v.put_vectors(
            vectorBucketName="media",
            indexName="movies",
            vectors=[
                {
                    "key": "a",
                    "data": {"float32": [0.0, 0.0]},
                    "metadata": {"genre": "scifi", "year": 1977},
                },
                {
                    "key": "b",
                    "data": {"float32": [1.0, 0.0]},
                    "metadata": {"genre": "drama", "year": 1999},
                },
                {
                    "key": "c",
                    "data": {"float32": [2.0, 0.0]},
                    "metadata": {"genre": "scifi", "year": 2014},
                },
            ],
        )
        return s3v

    def _keys(self, s3v, index: str = "movies", **query) -> set:
        res = s3v.query_vectors(
            vectorBucketName="media",
            indexName=index,
            queryVector={"float32": [0.0, 0.0]},
            topK=10,
            **query,
        )
        return {v["key"] for v in res["vectors"]}

    def test_put_vectors_overwrites_by_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s3v = self._client(tmp)
            s3v.put_vectors(
                vectorBucketName="media",
                indexName="movies",
                vectors=[
                    {
                        "key": "a",
                        "data": {"float32": [9.0, 9.0]},
                        "metadata": {"genre": "moved"},
                    }
                ],
            )
            got = s3v.get_vectors(
                vectorBucketName="media",
                indexName="movies",
                keys=["a"],
                returnData=True,
                returnMetadata=True,
            )
            self.assertEqual(got["vectors"][0]["data"]["float32"], [9.0, 9.0])
            self.assertEqual(got["vectors"][0]["metadata"]["genre"], "moved")

    def test_topk_limits_and_return_flags_default_off(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s3v = self._client(tmp)
            res = s3v.query_vectors(
                vectorBucketName="media",
                indexName="movies",
                queryVector={"float32": [0.0, 0.0]},
                topK=2,
            )
            self.assertEqual(len(res["vectors"]), 2)
            self.assertNotIn("metadata", res["vectors"][0])
            self.assertNotIn("distance", res["vectors"][0])

    def test_filter_operator_set(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s3v = self._client(tmp)
            self.assertEqual(self._keys(s3v, filter={"genre": "scifi"}), {"a", "c"})
            self.assertEqual(self._keys(s3v, filter={"year": {"$gte": 2000}}), {"c"})
            self.assertEqual(self._keys(s3v, filter={"genre": {"$ne": "scifi"}}), {"b"})
            self.assertEqual(
                self._keys(s3v, filter={"year": {"$in": [1977, 1999]}}), {"a", "b"}
            )

    def test_get_vectors_skips_missing_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s3v = self._client(tmp)
            got = s3v.get_vectors(
                vectorBucketName="media",
                indexName="movies",
                keys=["a", "missing", "c"],
                returnData=True,
            )
            self.assertEqual([v["key"] for v in got["vectors"]], ["a", "c"])

    def test_delete_removes_only_targeted_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s3v = self._client(tmp)
            s3v.delete_vectors(
                vectorBucketName="media", indexName="movies", keys=["a", "c"]
            )
            self.assertEqual(self._keys(s3v), {"b"})

    def test_control_plane_listing_and_index_isolation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s3v = self._client(tmp)
            s3v.create_index(
                vectorBucketName="media",
                indexName="shows",
                dimension=2,
                distanceMetric="euclidean",
            )
            s3v.put_vectors(
                vectorBucketName="media",
                indexName="shows",
                vectors=[{"key": "z", "data": {"float32": [0.0, 0.0]}}],
            )
            names = {
                i["indexName"]
                for i in s3v.list_indexes(vectorBucketName="media")["indexes"]
            }
            self.assertEqual(names, {"movies", "shows"})
            self.assertEqual(
                [
                    b["vectorBucketName"]
                    for b in s3v.list_vector_buckets()["vectorBuckets"]
                ],
                ["media"],
            )
            self.assertEqual(
                s3v.get_index(vectorBucketName="media", indexName="movies")["index"][
                    "dimension"
                ],
                2,
            )
            # Each index is a separate BORSUK root: "shows" holds only its own key.
            self.assertEqual(self._keys(s3v, index="shows"), {"z"})


class TurbopufferAdapterTest(unittest.TestCase):
    def test_write_and_query_with_tuple_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tpuf = Turbopuffer(base_uri=base_uri(tmp), dimension=2)
            ns = tpuf.namespace("products")
            ns.write(
                upsert_rows=[
                    {
                        "id": "1",
                        "vector": [1.0, 0.0],
                        "category": "animal",
                        "public": 1,
                    },
                    {"id": "2", "vector": [0.0, 1.0], "category": "plant", "public": 1},
                    {
                        "id": "3",
                        "vector": [1.0, 0.1],
                        "category": "animal",
                        "public": 0,
                    },
                ],
                distance_metric="cosine_distance",
            )

            results = ns.query(
                rank_by=("vector", "ANN", [1.0, 0.0]),
                top_k=10,
                filters=("And", (("category", "Eq", "animal"), ("public", "Eq", 1))),
                include_attributes=["category"],
            )
            self.assertEqual([row["id"] for row in results], ["1"])
            self.assertEqual(results[0]["category"], "animal")

            ns.write(deletes=["1"])
            after = ns.query(
                rank_by=("vector", "ANN", [1.0, 0.0]),
                filters=("category", "Eq", "animal"),
            )
            self.assertEqual([row["id"] for row in after], ["3"])


class TurbopufferContractTest(unittest.TestCase):
    """Pin the turbopuffer data-plane contract: write overwrite-by-id, top_k, the
    tuple filter operators, include_attributes projection, deletes, and namespace
    isolation."""

    def _ns(self, tmp: str, name: str = "products"):
        tpuf = Turbopuffer(base_uri=base_uri(tmp), dimension=2)
        ns = tpuf.namespace(name)
        ns.write(
            upsert_rows=[
                {"id": "a", "vector": [0.0, 0.0], "category": "animal", "rank": 1},
                {"id": "b", "vector": [1.0, 0.0], "category": "plant", "rank": 2},
                {"id": "c", "vector": [2.0, 0.0], "category": "animal", "rank": 3},
            ],
            distance_metric="euclidean_squared",
        )
        return tpuf, ns

    def _ids(self, ns, **query) -> set:
        rows = ns.query(rank_by=("vector", "ANN", [0.0, 0.0]), top_k=10, **query)
        return {row["id"] for row in rows}

    def test_write_overwrites_by_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _tpuf, ns = self._ns(tmp)
            ns.write(
                upsert_rows=[{"id": "a", "vector": [9.0, 9.0], "category": "moved"}]
            )
            rows = ns.query(
                rank_by=("vector", "ANN", [9.0, 9.0]),
                top_k=1,
                include_attributes=["category"],
            )
            self.assertEqual(rows[0]["id"], "a")
            self.assertEqual(rows[0]["category"], "moved")

    def test_top_k_limits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _tpuf, ns = self._ns(tmp)
            rows = ns.query(rank_by=("vector", "ANN", [0.0, 0.0]), top_k=2)
            self.assertEqual(len(rows), 2)

    def test_tuple_filter_operators(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _tpuf, ns = self._ns(tmp)
            self.assertEqual(
                self._ids(ns, filters=("category", "Eq", "animal")), {"a", "c"}
            )
            self.assertEqual(
                self._ids(ns, filters=("category", "NotEq", "animal")), {"b"}
            )
            self.assertEqual(self._ids(ns, filters=("rank", "Gte", 2)), {"b", "c"})
            self.assertEqual(self._ids(ns, filters=("rank", "In", [1, 3])), {"a", "c"})
            self.assertEqual(
                self._ids(
                    ns,
                    filters=("And", (("category", "Eq", "animal"), ("rank", "Lt", 3))),
                ),
                {"a"},
            )
            self.assertEqual(
                self._ids(ns, filters=("Not", ("category", "Eq", "animal"))), {"b"}
            )

    def test_include_attributes_projects_only_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _tpuf, ns = self._ns(tmp)
            rows = ns.query(
                rank_by=("vector", "ANN", [0.0, 0.0]),
                top_k=1,
                include_attributes=["category"],
            )
            self.assertIn("category", rows[0])
            self.assertNotIn("rank", rows[0])

    def test_delete_and_namespace_isolation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tpuf, ns = self._ns(tmp)
            other = tpuf.namespace("other")
            other.write(
                upsert_rows=[{"id": "z", "vector": [0.0, 0.0]}],
                distance_metric="euclidean_squared",
            )
            ns.write(deletes=["a"])
            self.assertEqual(self._ids(ns), {"b", "c"})
            self.assertEqual(self._ids(other), {"z"})


class ChromaAdapterTest(unittest.TestCase):
    def test_add_query_get_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = ChromaClient(base_uri=base_uri(tmp), dimensions=2)
            col = client.get_or_create_collection("docs", metadata={"hnsw:space": "l2"})
            col.add(
                ids=["a", "b", "c"],
                embeddings=[[0.0, 0.0], [1.0, 0.0], [0.0, 1.0]],
                metadatas=[{"genre": "rock"}, {"genre": "rock"}, {"genre": "jazz"}],
                documents=["alpha", "beta", "gamma"],
            )
            self.assertEqual(col.count(), 3)

            res = col.query(
                query_embeddings=[[0.0, 0.0]],
                n_results=3,
                where={"genre": "rock"},
                include=["metadatas", "documents", "distances"],
            )
            self.assertEqual(set(res["ids"][0]), {"a", "b"})
            self.assertIn("alpha", res["documents"][0])
            # The reserved document key is not leaked into returned metadata.
            self.assertNotIn("__document__", res["metadatas"][0][0])

            got = col.get(ids=["c"])
            self.assertEqual(got["ids"], ["c"])
            self.assertEqual(got["documents"], ["gamma"])

            everything = col.get()
            self.assertEqual(set(everything["ids"]), {"a", "b", "c"})

            col.delete(ids=["a"])
            self.assertEqual(col.count(), 2)


class QdrantAdapterTest(unittest.TestCase):
    def test_upsert_search_scroll_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = QdrantClient(base_uri=base_uri(tmp))
            client.create_collection(
                "docs", vectors_config={"size": 2, "distance": "Euclid"}
            )
            client.upsert(
                "docs",
                points=[
                    {
                        "id": "1",
                        "vector": [0.0, 0.0],
                        "payload": {"genre": "rock", "year": 1975},
                    },
                    {
                        "id": "2",
                        "vector": [1.0, 0.0],
                        "payload": {"genre": "rock", "year": 2001},
                    },
                    {
                        "id": "3",
                        "vector": [0.0, 1.0],
                        "payload": {"genre": "jazz", "year": 1999},
                    },
                ],
            )
            self.assertEqual(client.count("docs")["count"], 3)
            self.assertEqual(client._index("docs").stats().named_vectors, [])

            hits = client.search(
                "docs",
                query_vector=[0.0, 0.0],
                query_filter={
                    "must": [
                        {"key": "genre", "match": {"value": "rock"}},
                        {"key": "year", "range": {"gte": 2000}},
                    ]
                },
                limit=5,
                with_payload=True,
            )
            self.assertEqual([h.id for h in hits], ["2"])
            self.assertEqual(hits[0].payload["genre"], "rock")

            fetched = client.retrieve("docs", ids=["3"], with_vectors=True)
            self.assertEqual(fetched[0].vector, [0.0, 1.0])

            records, next_offset = client.scroll("docs", limit=2, offset=0)
            self.assertEqual(len(records), 2)
            self.assertEqual(next_offset, 2)

            client.delete("docs", points_selector={"points": ["1"]})
            self.assertEqual(client.count("docs")["count"], 2)

    def test_named_dense_vectors_search_each_vector(self) -> None:
        class NamedVector:
            def __init__(self, name: str, vector: list[float]) -> None:
                self.name = name
                self.vector = vector

        with tempfile.TemporaryDirectory() as tmp:
            client = QdrantClient(base_uri=base_uri(tmp))
            client.create_collection(
                "docs",
                vectors_config={
                    "image": {"size": 2, "distance": "Euclid"},
                    "text": {"size": 2, "distance": "Euclid"},
                },
            )
            client.upsert(
                "docs",
                points=[
                    {
                        "id": "a",
                        "vector": {"image": [0.0, 0.0], "text": [9.0, 0.0]},
                        "payload": {"kind": "primary-nearest"},
                    },
                    {
                        "id": "b",
                        "vector": {"image": [1.0, 0.0], "text": [0.0, 0.0]},
                        "payload": {"kind": "text-nearest"},
                    },
                    {
                        "id": "c",
                        "vector": {"image": [2.0, 0.0], "text": [0.1, 0.0]},
                        "payload": {"kind": "text-runner-up"},
                    },
                ],
            )

            self.assertEqual(client._index("docs").stats().named_vectors, ["text"])

            image_hits = client.search(
                "docs",
                query_vector=NamedVector("image", [0.0, 0.0]),
                limit=3,
                with_payload=True,
            )
            self.assertEqual([hit.id for hit in image_hits], ["a", "b", "c"])
            self.assertEqual(image_hits[0].payload["kind"], "primary-nearest")

            text_hits = client.search(
                "docs",
                query_vector=[0.0, 0.0],
                using="text",
                limit=3,
                with_payload=False,
            )
            self.assertEqual([hit.id for hit in text_hits], ["b", "c", "a"])
            self.assertIsNone(text_hits[0].payload)

            text_response = client.query_points(
                "docs",
                query=[0.0, 0.0],
                using="text",
                limit=2,
                with_payload=True,
            )
            self.assertEqual([point.id for point in text_response.points], ["b", "c"])
            self.assertEqual(text_response.points[0].payload["kind"], "text-nearest")

    def test_sparse_vectors_upsert_and_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = QdrantClient(base_uri=base_uri(tmp))
            client.create_collection(
                "hybrid",
                vectors_config={"dense": {"size": 2, "distance": "Cosine"}},
                sparse_vectors_config={"text": {}},
            )
            client.upsert(
                "hybrid",
                points=[
                    {
                        "id": "a",
                        "vector": {
                            "dense": [1.0, 0.0],
                            "text": {"indices": [5, 7], "values": [1.0, 2.0]},
                        },
                        "payload": {"kind": "a"},
                    },
                    {
                        "id": "b",
                        "vector": {
                            "dense": [0.0, 1.0],
                            "text": {"indices": [5, 9], "values": [3.0, 1.0]},
                        },
                        "payload": {"kind": "b"},
                    },
                ],
            )
            # Term 5 is in both; term 7 only in "a".
            both = client.query_points(
                "hybrid", query={"indices": [5], "values": [1.0]}, using="text", limit=5
            )
            self.assertEqual({p.id for p in both.points}, {"a", "b"})
            only_a = client.search(
                "hybrid",
                query_vector={"indices": [7], "values": [1.0]},
                using="text",
                limit=5,
                with_payload=True,
            )
            self.assertEqual([p.id for p in only_a], ["a"])
            self.assertEqual(only_a[0].payload["kind"], "a")
            # The dense leg still works alongside the sparse one.
            dense = client.search(
                "hybrid", query_vector=[1.0, 0.0], using="dense", limit=1
            )
            self.assertEqual(dense[0].id, "a")


try:
    import langchain_core  # noqa: F401

    _HAS_LANGCHAIN = True
except ImportError:
    _HAS_LANGCHAIN = False


class QdrantContractTest(unittest.TestCase):
    """Pin the Qdrant data-plane contract: upsert overwrite-by-id, limit, the
    must/should/must_not + match/range filter forms, retrieve with vectors,
    scroll pagination, count, delete, and collection existence."""

    def _client(self, tmp: str):
        client = QdrantClient(base_uri=base_uri(tmp))
        client.create_collection(
            "docs", vectors_config={"size": 2, "distance": "Euclid"}
        )
        client.upsert(
            "docs",
            points=[
                {
                    "id": "1",
                    "vector": [0.0, 0.0],
                    "payload": {"genre": "rock", "year": 1971},
                },
                {
                    "id": "2",
                    "vector": [1.0, 0.0],
                    "payload": {"genre": "jazz", "year": 1985},
                },
                {
                    "id": "3",
                    "vector": [2.0, 0.0],
                    "payload": {"genre": "rock", "year": 1999},
                },
                {
                    "id": "4",
                    "vector": [3.0, 0.0],
                    "payload": {"genre": "folk", "year": 2010},
                },
            ],
        )
        return client

    def _ids(self, client, query_filter) -> set:
        hits = client.search(
            "docs", query_vector=[0.0, 0.0], query_filter=query_filter, limit=10
        )
        return {hit.id for hit in hits}

    def test_upsert_overwrites_by_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = self._client(tmp)
            client.upsert(
                "docs",
                points=[
                    {"id": "1", "vector": [9.0, 9.0], "payload": {"genre": "moved"}}
                ],
            )
            # The overwrite is visible in live-state reads: id "1" now has the new
            # vector and payload, and remains a single live point (no duplicate).
            got = client.retrieve(
                "docs", ids=["1"], with_vectors=True, with_payload=True
            )
            self.assertEqual(len(got), 1)
            self.assertEqual(got[0].vector, [9.0, 9.0])
            self.assertEqual(got[0].payload["genre"], "moved")
            records, _ = client.scroll("docs", limit=10, offset=0)
            ids = [record.id for record in records]
            self.assertEqual(
                ids.count("1"), 1, "upsert must not create a duplicate live point"
            )
            self.assertEqual(set(ids), {"1", "2", "3", "4"})

    def test_limit_caps_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = self._client(tmp)
            self.assertEqual(
                len(client.search("docs", query_vector=[0.0, 0.0], limit=2)), 2
            )

    def test_filter_must_should_mustnot_match_range(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = self._client(tmp)
            # must: genre == rock AND year >= 1990
            self.assertEqual(
                self._ids(
                    client,
                    {
                        "must": [
                            {"key": "genre", "match": {"value": "rock"}},
                            {"key": "year", "range": {"gte": 1990}},
                        ]
                    },
                ),
                {"3"},
            )
            # should: jazz OR folk
            self.assertEqual(
                self._ids(
                    client,
                    {
                        "should": [
                            {"key": "genre", "match": {"value": "jazz"}},
                            {"key": "genre", "match": {"value": "folk"}},
                        ]
                    },
                ),
                {"2", "4"},
            )
            # must_not: not rock
            self.assertEqual(
                self._ids(
                    client, {"must_not": [{"key": "genre", "match": {"value": "rock"}}]}
                ),
                {"2", "4"},
            )
            # match.any -> $in
            self.assertEqual(
                self._ids(
                    client,
                    {"must": [{"key": "genre", "match": {"any": ["rock", "folk"]}}]},
                ),
                {"1", "3", "4"},
            )

    def test_scroll_paginates_without_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = self._client(tmp)
            first, next_offset = client.scroll("docs", limit=2, offset=0)
            self.assertEqual(len(first), 2)
            self.assertEqual(next_offset, 2)
            second, _ = client.scroll("docs", limit=2, offset=next_offset)
            self.assertEqual(len(second), 2)
            seen = {record.id for record in first} | {record.id for record in second}
            self.assertEqual(seen, {"1", "2", "3", "4"})

    def test_delete_and_collection_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = self._client(tmp)
            self.assertTrue(client.collection_exists("docs"))
            self.assertFalse(client.collection_exists("missing"))
            client.delete("docs", points_selector={"points": ["1", "2"]})
            self.assertEqual(client.count("docs")["count"], 2)
            remaining, _ = client.scroll("docs", limit=10, offset=0)
            self.assertEqual({record.id for record in remaining}, {"3", "4"})


@unittest.skipUnless(_HAS_LANGCHAIN, "langchain-core not installed")
class LangchainAdapterTest(unittest.TestCase):
    def test_vector_store_add_and_search(self) -> None:
        import hashlib

        from borsuk.compat.langchain import BorsukVectorStore
        from langchain_core.embeddings import Embeddings

        class FakeEmbeddings(Embeddings):
            dim = 64

            def _vec(self, text: str) -> list[float]:
                vector = [0.0] * self.dim
                for token in text.lower().split():
                    digest = hashlib.blake2b(token.encode(), digest_size=8).digest()
                    vector[int.from_bytes(digest[:4], "big") % self.dim] += 1.0
                norm = sum(x * x for x in vector) ** 0.5 or 1.0
                return [x / norm for x in vector]

            def embed_documents(self, texts):
                return [self._vec(t) for t in texts]

            def embed_query(self, text):
                return self._vec(text)

        with tempfile.TemporaryDirectory() as tmp:
            store = BorsukVectorStore.from_texts(
                ["the cat sat on the mat", "borsuk stores vectors in object storage"],
                FakeEmbeddings(),
                uri=base_uri(tmp),
                metadatas=[{"src": "a"}, {"src": "b"}],
            )
            docs = store.similarity_search("where does borsuk keep vectors", k=1)
            self.assertEqual(
                docs[0].page_content, "borsuk stores vectors in object storage"
            )
            self.assertEqual(docs[0].metadata, {"src": "b"})
            # Works as a LangChain retriever.
            retrieved = store.as_retriever(search_kwargs={"k": 1}).invoke("cat")
            self.assertEqual(retrieved[0].page_content, "the cat sat on the mat")


class TranslationTest(unittest.TestCase):
    def test_turbopuffer_filter_translation(self) -> None:
        self.assertEqual(
            translate_turbopuffer_filter(("genre", "Eq", "rock")),
            {"genre": {"$eq": "rock"}},
        )
        self.assertEqual(
            translate_turbopuffer_filter(
                ("And", (("g", "In", ["a", "b"]), ("Not", ("y", "Lt", 2000))))
            ),
            {"$and": [{"g": {"$in": ["a", "b"]}}, {"$not": {"y": {"$lt": 2000}}}]},
        )
        with self.assertRaises(ValueError):
            translate_turbopuffer_filter(("g", "Glob", "r*"))

    def test_qdrant_filter_translation(self) -> None:
        self.assertEqual(
            translate_qdrant_filter(
                {"must": [{"key": "genre", "match": {"value": "rock"}}]}
            ),
            {"genre": {"$eq": "rock"}},
        )
        self.assertEqual(
            translate_qdrant_filter(
                {
                    "must": [{"key": "year", "range": {"gte": 2000, "lt": 2010}}],
                    "must_not": [{"key": "genre", "match": {"any": ["pop"]}}],
                }
            ),
            {
                "$and": [
                    {"year": {"$gte": 2000, "$lt": 2010}},
                    {"$not": {"$or": [{"genre": {"$in": ["pop"]}}]}},
                ]
            },
        )
        # A BORSUK-style dict passes straight through.
        self.assertEqual(
            translate_qdrant_filter({"genre": {"$eq": "rock"}}),
            {"genre": {"$eq": "rock"}},
        )
        self.assertIsNone(translate_qdrant_filter(None))

    def test_metric_mapping(self) -> None:
        self.assertEqual(map_metric("pinecone", "dotproduct"), "inner-product")
        self.assertEqual(
            map_metric("turbopuffer", "euclidean_squared"), "squared-euclidean"
        )
        self.assertEqual(map_metric("s3vectors", "cosine"), "cosine")
        self.assertEqual(map_metric("chroma", "l2"), "euclidean")
        self.assertEqual(map_metric("qdrant", "Dot"), "inner-product")
        with self.assertRaises(ValueError):
            map_metric("pinecone", "hamming")


if __name__ == "__main__":
    unittest.main()
