"""Drop-in adapter tests: each emulated SDK surface must round-trip data through
BORSUK — create, upsert with metadata, filtered query, fetch/get, delete."""

import tempfile
import unittest
from pathlib import Path

from borsuk.compat._common import map_metric, translate_turbopuffer_filter
from borsuk.compat.pinecone import Pinecone
from borsuk.compat.s3vectors import client as s3vectors_client
from borsuk.compat.turbopuffer import Turbopuffer


def base_uri(tmp: str) -> str:
    return Path(tmp).as_uri()


class PineconeAdapterTest(unittest.TestCase):
    def test_upsert_query_fetch_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pc = Pinecone(api_key="ignored", base_uri=base_uri(tmp), dimension=2, metric="cosine")
            index = pc.Index("songs")
            index.upsert(
                [
                    ("a", [1.0, 0.0], {"genre": "rock", "year": 1975}),
                    {"id": "b", "values": [0.0, 1.0], "metadata": {"genre": "jazz", "year": 1999}},
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
            self.assertTrue(all(match["metadata"]["genre"] == "rock" for match in res["matches"]))
            self.assertEqual(len(res["matches"][0]["values"]), 2)

            fetched = index.fetch(["a"], namespace="store-1")
            self.assertEqual(fetched["vectors"]["a"]["metadata"]["year"], 1975)

            # Namespaces are isolated.
            self.assertEqual(index.query(vector=[1.0, 0.0], namespace="other")["matches"], [])

            index.delete(ids=["a"], namespace="store-1")
            after = index.query(vector=[1.0, 0.0], filter={"genre": "rock"}, namespace="store-1")
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


class S3VectorsAdapterTest(unittest.TestCase):
    def test_put_query_get_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s3v = s3vectors_client("s3vectors", base_uri=base_uri(tmp))
            s3v.create_vector_bucket(vectorBucketName="media")
            s3v.create_index(
                vectorBucketName="media", indexName="movies", dimension=2, distanceMetric="cosine"
            )
            s3v.put_vectors(
                vectorBucketName="media",
                indexName="movies",
                vectors=[
                    {"key": "star-wars", "data": {"float32": [1.0, 0.0]}, "metadata": {"genre": "scifi"}},
                    {"key": "casablanca", "data": {"float32": [0.0, 1.0]}, "metadata": {"genre": "drama"}},
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

            s3v.delete_vectors(vectorBucketName="media", indexName="movies", keys=["star-wars"])
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
                    vectorBucketName="nope", indexName="nope", queryVector={"float32": [1.0, 0.0]}
                )


class TurbopufferAdapterTest(unittest.TestCase):
    def test_write_and_query_with_tuple_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tpuf = Turbopuffer(base_uri=base_uri(tmp), dimension=2)
            ns = tpuf.namespace("products")
            ns.write(
                upsert_rows=[
                    {"id": "1", "vector": [1.0, 0.0], "category": "animal", "public": 1},
                    {"id": "2", "vector": [0.0, 1.0], "category": "plant", "public": 1},
                    {"id": "3", "vector": [1.0, 0.1], "category": "animal", "public": 0},
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

    def test_metric_mapping(self) -> None:
        self.assertEqual(map_metric("pinecone", "dotproduct"), "inner-product")
        self.assertEqual(map_metric("turbopuffer", "euclidean_squared"), "squared-euclidean")
        self.assertEqual(map_metric("s3vectors", "cosine"), "cosine")
        with self.assertRaises(ValueError):
            map_metric("pinecone", "hamming")


if __name__ == "__main__":
    unittest.main()
