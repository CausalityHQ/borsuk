"""BORSUK cookbook: every retrieval mode and the ways to mix them.

This runs end-to-end (it is exercised in CI), so every snippet here is guaranteed
to work against the current build. It covers:

* dense vector search,
* versioned upsert (overwrite by id),
* metadata filtering,
* full-text BM25 search,
* sparse (lexical / SPLADE-style) named vectors,
* hybrid fusion (dense + sparse + text with RRF and weighted fusion),
* a retrieve-then-rerank RAG pattern,
* query cost / explain.

Each section builds its own tiny index so the parts are independent and
copy-pasteable.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import borsuk


def _index(**kwargs):
    root = tempfile.mkdtemp(prefix="borsuk-cookbook-")
    index = borsuk.create(uri=Path(root).as_uri(), **kwargs)
    return index, root


def dense_search_and_upsert() -> None:
    """Nearest-neighbour search, then overwrite a record in place with upsert."""
    index, root = _index(metric=borsuk.VectorMetricName.EUCLIDEAN, dimensions=2)
    try:
        index.add([[0.0, 0.0], [1.0, 0.0], [0.0, 1.0]], ids=["a", "b", "c"])
        assert index.search_ids([0.1, 0.0], k=2) == ["a", "b"]

        # upsert replaces "a" in place (add would reject the existing id).
        index.upsert([[0.0, 9.0]], ids=["a"])
        # "a" is now far from the origin; "b" is nearest there, and there is only
        # ever one "a".
        near_origin = index.search_ids([0.0, 0.0], k=3)
        assert near_origin[0] == "b"
        assert near_origin.count("a") == 1
        print("dense + upsert:", near_origin)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def metadata_filtering() -> None:
    """Filter search by typed metadata (Pinecone-style operator dicts)."""
    index, root = _index(metric=borsuk.VectorMetricName.EUCLIDEAN, dimensions=2)
    try:
        index.add(
            [[0.0, 0.0], [0.1, 0.0], [0.2, 0.0]],
            ids=["a", "b", "c"],
            metadata=[{"genre": "comedy"}, {"genre": "drama"}, {"genre": "comedy"}],
        )
        report = index.search_with_report(
            [0.0, 0.0],
            k=5,
            filter={"genre": {"$eq": "comedy"}},
            include_metadata=True,
        )
        ids = [hit.id for hit in report.hits]
        assert ids == ["a", "c"], ids
        print("filtered (genre=comedy):", ids)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def full_text_bm25() -> None:
    """Lexical full-text ranking with BM25 over per-record text."""
    index, root = _index(
        metric=borsuk.VectorMetricName.EUCLIDEAN, dimensions=2, text=True
    )
    try:
        index.add(
            [[0.0, 0.0], [1.0, 0.0], [2.0, 0.0]],
            ids=["a", "b", "c"],
            text=[
                "the quick brown fox",
                "a needle in a haystack",
                "needle needle everywhere",
            ],
        )
        ids = index.search_text("needle", k=2)
        assert set(ids) == {"b", "c"}, ids
        print("bm25 'needle':", ids)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def sparse_lexical_named_vector() -> None:
    """A sparse (inverted-index) named vector for huge-vocabulary lexical search.

    Nothing is densified, so the vocabulary can be millions of terms while each
    record carries only its non-zeros.
    """
    index, root = _index(
        metric=borsuk.VectorMetricName.EUCLIDEAN,
        dimensions=2,
        named_vectors={
            "lexical": {
                "dimensions": 100_000,
                "metric": "inner-product",
                "kind": "sparse",
            }
        },
    )
    try:
        index.add(
            [[0.0, 0.0], [1.0, 0.0]],
            ids=["a", "b"],
            named_vectors=[
                {"lexical": {"indices": [5, 7], "values": [1.0, 2.0]}},
                {"lexical": {"indices": [5, 9], "values": [3.0, 1.0]}},
            ],
        )
        # Term 5 is in both; term 7 only in "a".
        assert set(index.search_sparse_named("lexical", [5], [1.0], k=5)) == {"a", "b"}
        assert index.search_sparse_named("lexical", [7], [1.0], k=5) == ["a"]
        print(
            "sparse lexical (term 7):", index.search_sparse_named("lexical", [7], [1.0])
        )
    finally:
        shutil.rmtree(root, ignore_errors=True)


def hybrid_fusion() -> None:
    """Fuse a dense vector leg with a BM25 text leg (RRF, then weighted)."""
    index, root = _index(
        metric=borsuk.VectorMetricName.EUCLIDEAN, dimensions=2, text=True
    )
    try:
        index.add(
            [[0.0, 0.0], [1.0, 0.0], [0.0, 1.0]],
            ids=["a", "b", "c"],
            text=["red apple", "green apple pie", "blue sky"],
        )
        # Reciprocal-rank fusion of the vector query and the text query.
        rrf = index.search_hybrid(
            vectors={"": [0.0, 0.0]}, text="apple", k=3, fusion="rrf"
        )
        print("hybrid rrf:", rrf)
        # Weighted fusion, leaning on the text leg.
        weighted = index.search_hybrid(
            vectors={"": [0.0, 0.0]},
            text="apple",
            k=3,
            fusion="weighted",
            weights={"": 0.2, "@text": 1.0},
        )
        print("hybrid weighted (text-heavy):", weighted)
        assert set(rrf) and set(weighted)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def rag_retrieve_then_rerank() -> None:
    """A RAG-style retrieve -> rerank -> context pipeline.

    Retrieve a wide candidate set, rerank it with your own scorer (here a simple
    metadata-priority stand-in for a cross-encoder), and keep the best few.
    """
    index, root = _index(metric=borsuk.VectorMetricName.EUCLIDEAN, dimensions=2)
    try:
        index.add(
            [[0.0, 0.0], [0.1, 0.0], [0.2, 0.0], [0.3, 0.0]],
            ids=["a", "b", "c", "d"],
            metadata=[
                {"priority": 1},
                {"priority": 4},
                {"priority": 3},
                {"priority": 2},
            ],
        )
        # 1) Retrieve a wide candidate set with metadata.
        report = index.search_with_report([0.0, 0.0], k=4, include_metadata=True)
        candidates = [(hit.id, hit.metadata) for hit in report.hits]
        # 2) Rerank with your model (here: descending priority).
        reranked = sorted(candidates, key=lambda c: -c[1]["priority"])
        # 3) Keep the top-2 as LLM context.
        context_ids = [cid for cid, _ in reranked[:2]]
        assert context_ids == ["b", "c"], context_ids
        print("rag reranked top-2:", context_ids)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def query_cost_explain() -> None:
    """Ask what a query cost: requests, bytes, cache, latency, and dollars."""
    index, root = _index(metric=borsuk.VectorMetricName.EUCLIDEAN, dimensions=2)
    try:
        index.add([[float(i), 0.0] for i in range(20)])
        plan = index.explain([0.0, 0.0], k=5)
        print(
            "explain:",
            {
                "hits": len(plan["hits"]),
                "get_requests": plan["get_requests"],
                "bytes_read": plan["bytes_read"],
                "estimated_cost_usd": plan["estimated_cost_usd"],
                "cache_hit_ratio": plan["cache_hit_ratio"],
            },
        )
        assert plan["estimated_cost_usd"] >= 0.0
    finally:
        shutil.rmtree(root, ignore_errors=True)


def main() -> None:
    dense_search_and_upsert()
    metadata_filtering()
    full_text_bm25()
    sparse_lexical_named_vector()
    hybrid_fusion()
    rag_retrieve_then_rerank()
    query_cost_explain()
    print("cookbook ok")


if __name__ == "__main__":
    main()
