from __future__ import annotations

from array import array
from typing import TYPE_CHECKING, Sequence

import borsuk

if TYPE_CHECKING:
    from borsuk import (
        CanonicalLeafMode,
        CanonicalVectorMetric,
        LeafMode,
        MinkowskiMetric,
        RecordId,
        SearchModeName,
        VectorMetric,
    )


def typed_config_values() -> None:
    metric: VectorMetric = borsuk.VectorMetricName.COSINE
    minkowski: MinkowskiMetric = borsuk.minkowski_metric(3)
    metric = minkowski
    mode: SearchModeName | borsuk.SearchMode = borsuk.SearchMode.APPROX
    leaf_mode: LeafMode = borsuk.LeafModeName.VAMANA_PQ
    pq_leaf_mode: LeafMode = borsuk.LeafModeName.PQ_SCAN
    hybrid_leaf_mode: LeafMode = borsuk.LeafModeName.HYBRID

    names: Sequence[str] = borsuk.leaf_mode_names()
    distance: float = borsuk.vector_distance(metric, [1.0, 0.0], [0.0, 1.0])

    assert mode == "approx"
    assert leaf_mode == "vamana-pq"
    assert pq_leaf_mode == "pq-scan"
    assert hybrid_leaf_mode == "hybrid"
    assert "pq-scan" in names
    assert "vamana-pq" in names
    assert "hybrid" in names
    assert distance >= 0.0


def typed_index_methods(index: borsuk.Index) -> None:
    ids: list[RecordId] = index.add([[0.0, 0.0]], ids=["a"])
    byte_ids: list[RecordId] = index.add([[2.0, 0.0]], ids=[b"\x00\x9f\xff\x07"])
    vector_buffer = array("f", [1.0, 0.0])
    query_buffer = array("f", [0.0, 0.0])
    query_batch_buffer = array("f", [0.0, 0.0, 1.0, 0.0])

    buffer_ids: list[RecordId] = index.add_buffer(vector_buffer, ids=["b"])
    search_ids: list[str] = index.search_ids([0.0, 0.0], k=1)
    search_id_bytes: list[bytes] = index.search_id_bytes([0.0, 0.0], k=1)
    vectors: list[list[float]] = index.search_vectors([0.0, 0.0], k=1)
    buffer_search_ids: list[str] = index.search_ids_buffer(query_buffer, k=1)
    buffer_vectors: list[list[float]] = index.search_vectors_buffer(query_buffer, k=1)
    batch_ids: list[list[str]] = index.search_ids_batch([[0.0, 0.0]], k=1)
    batch_id_bytes: list[list[bytes]] = index.search_id_bytes_batch([[0.0, 0.0]], k=1)
    batch_vectors: list[list[list[float]]] = index.search_vectors_batch([[0.0, 0.0]], k=1)
    batch_buffer_ids: list[list[str]] = index.search_ids_batch_buffer(query_batch_buffer, k=1)
    batch_buffer_vectors: list[list[list[float]]] = index.search_vectors_batch_buffer(
        query_batch_buffer,
        k=1,
    )
    report: borsuk.SearchReport = index.search_with_report_buffer(query_buffer, k=1)
    batch_reports: list[borsuk.SearchReport] = index.search_batch_with_report_buffer(
        query_batch_buffer,
        k=1,
    )
    report_leaf_mode: CanonicalLeafMode = report.leaf_mode
    stats_metric: CanonicalVectorMetric | MinkowskiMetric = index.stats().metric
    vector: list[float] | None = index.get_vector("a")
    byte_vector: list[float] | None = index.get_vector(b"\x00\x9f\xff\x07")

    assert ids
    assert byte_ids
    assert buffer_ids
    assert search_ids
    assert search_id_bytes
    assert vectors
    assert buffer_search_ids
    assert buffer_vectors
    assert batch_ids
    assert batch_id_bytes
    assert batch_vectors
    assert batch_buffer_ids
    assert batch_buffer_vectors
    assert report.segments_total >= 0
    assert report_leaf_mode in {"flat-scan", "sq-scan", "pq-scan", "graph", "vamana-pq", "hybrid"}
    assert stats_metric
    assert batch_reports
    assert vector is None or len(vector) == 2
    assert byte_vector is None or len(byte_vector) == 2
