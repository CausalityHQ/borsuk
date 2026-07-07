"""Native Python API for BORSUK.

The implementation is provided by the Rust/PyO3 extension module
``borsuk._borsuk``. There is intentionally no subprocess or CLI fallback in the
runtime API.
"""

from collections.abc import Buffer, Sequence
from enum import Enum
from itertools import islice
from math import isfinite
from typing import Any, Literal, NewType, TypeAlias

from ._borsuk import (
    AddReport,
    BorsukError,
    CompactionReport,
    GarbageCollectionReport,
    Hit,
    Index,
    IndexStats,
    RebuildReport,
    RequestCounts,
    SearchReport,
    create as _create,
    leaf_mode_names as _leaf_mode_names,
    open as _open,
    tie_aware_recall_at_k as _tie_aware_recall_at_k,
    vector_distance as _vector_distance,
    vector_metric_names as _vector_metric_names,
)


class VectorMetricName(str, Enum):
    EUCLIDEAN = "euclidean"
    SQUARED_EUCLIDEAN = "squared-euclidean"
    COSINE = "cosine"
    INNER_PRODUCT = "inner-product"
    ANGULAR = "angular"
    MANHATTAN = "manhattan"
    GOWER = "gower"
    CHEBYSHEV = "chebyshev"
    CANBERRA = "canberra"
    BRAY_CURTIS = "bray-curtis"
    CORRELATION = "correlation"
    HAMMING = "hamming"
    JACCARD = "jaccard"
    DICE = "dice"
    SIMPLE_MATCHING = "simple-matching"
    RUSSELL_RAO = "russell-rao"
    ROGERS_TANIMOTO = "rogers-tanimoto"
    SOKAL_SNEATH = "sokal-sneath"
    YULE = "yule"
    HELLINGER = "hellinger"
    CHI_SQUARE = "chi-square"
    KULLBACK_LEIBLER = "kullback-leibler"
    JEFFREYS = "jeffreys"
    JENSEN_SHANNON = "jensen-shannon"
    BHATTACHARYYA = "bhattacharyya"
    WASSERSTEIN = "wasserstein"
    DYNAMIC_TIME_WARPING = "dynamic-time-warping"
    RUZICKA = "ruzicka"
    SQUARED_CHORD = "squared-chord"
    WAVE_HEDGES = "wave-hedges"
    LORENTZIAN = "lorentzian"
    CLARK = "clark"


class SearchMode(str, Enum):
    EXACT = "exact"
    APPROX = "approx"


class LeafModeName(str, Enum):
    FLAT_SCAN = "flat-scan"
    SQ_SCAN = "sq-scan"
    PQ_SCAN = "pq-scan"
    GRAPH = "graph"
    VAMANA_PQ = "vamana-pq"
    HYBRID = "hybrid"


MinkowskiMetric = NewType("MinkowskiMetric", str)
Float32Buffer = Buffer
RecordId: TypeAlias = str | bytes | int


CanonicalVectorMetric: TypeAlias = Literal[
    "euclidean",
    "squared-euclidean",
    "cosine",
    "inner-product",
    "angular",
    "manhattan",
    "gower",
    "chebyshev",
    "canberra",
    "bray-curtis",
    "correlation",
    "hamming",
    "jaccard",
    "dice",
    "simple-matching",
    "russell-rao",
    "rogers-tanimoto",
    "sokal-sneath",
    "yule",
    "hellinger",
    "chi-square",
    "kullback-leibler",
    "jeffreys",
    "jensen-shannon",
    "bhattacharyya",
    "wasserstein",
    "dynamic-time-warping",
    "ruzicka",
    "squared-chord",
    "wave-hedges",
    "lorentzian",
    "clark",
]
VectorMetricAlias: TypeAlias = Literal[
    "l2",
    "sqeuclidean",
    "l2-squared",
    "innerproduct",
    "ip",
    "dot",
    "dot-product",
    "angle",
    "l1",
    "gower-distance",
    "linf",
    "l-infinity",
    "braycurtis",
    "simplematching",
    "matching",
    "smc",
    "russellrao",
    "rogerstanimoto",
    "sokalsneath",
    "chisquare",
    "chi2",
    "kullbackleibler",
    "kl",
    "kl-divergence",
    "jeffreys-divergence",
    "jensenshannon",
    "js",
    "js-distance",
    "bhattacharyya-distance",
    "earth-mover",
    "earthmover",
    "emd",
    "dynamictimewarping",
    "dtw",
    "weighted-jaccard",
    "weightedjaccard",
    "squaredchord",
    "wavehedges",
]
VectorMetric: TypeAlias = CanonicalVectorMetric | VectorMetricAlias | MinkowskiMetric | VectorMetricName
SearchModeName: TypeAlias = Literal["exact", "approx"]
CanonicalLeafMode: TypeAlias = Literal[
    "flat-scan",
    "sq-scan",
    "pq-scan",
    "graph",
    "vamana-pq",
    "hybrid",
]
SearchTerminationReason: TypeAlias = Literal[
    "complete",
    "exact-pruned",
    "epsilon",
    "max-segments",
    "max-bytes",
    "max-latency",
]
RecallGuarantee: TypeAlias = Literal["exact", "budget-complete", "degraded"]
LeafModeAlias: TypeAlias = Literal[
    "flat",
    "flatscan",
    "sq",
    "sqscan",
    "scalar-scan",
    "scalar-quantized-scan",
    "pq",
    "pqscan",
    "product-quantized-scan",
    "local-graph",
    "segment-graph",
    "vamana",
    "vamanapq",
    "vamana_pq",
    "diskann",
    "diskann-pq",
    "auto",
    "stored",
    "stored-leaf",
    "segment-leaf",
]
LeafMode: TypeAlias = CanonicalLeafMode | LeafModeAlias | LeafModeName


Hit.__annotations__ = {
    "id": str,
    "id_bytes": bytes,
    "distance": float,
}
IndexStats.__annotations__ = {
    "metric": CanonicalVectorMetric | MinkowskiMetric,
    "dimensions": int,
    "segment_max_vectors": int,
    "ram_budget_bytes": int | None,
    "manifest_version": int,
    "routing_max_level": int,
    "routing_page_fanout": int,
    "routing_leaf_pages": int,
    "routing_pages": int,
    "segments": int,
    "records": int,
    "segment_bytes": int,
    "graph_bytes": int,
    "resident_bytes_estimate": int,
}
RequestCounts.__annotations__ = {
    "gets": int,
    "puts": int,
    "deletes": int,
    "heads": int,
    "lists": int,
    "total": int,
}
AddReport.__annotations__ = {
    "segments_written": int,
    "graph_payloads_written": int,
    "manifest_tables_written": int,
    "routing_pages_written": int,
    "total_bytes_written": int,
    "bytes_per_vector": float,
    "requests": RequestCounts,
}
SearchReport.__annotations__ = {
    "hits": list[Hit],
    "leaf_mode": CanonicalLeafMode,
    "termination_reason": SearchTerminationReason,
    "recall_guarantee": RecallGuarantee,
    "segments_total": int,
    "segments_searched": int,
    "segments_skipped": int,
    "routing_page_indexes_read": int,
    "routing_pages_read": int,
    "bytes_read": int,
    "prefetched_bytes_unused": int,
    "graph_bytes_read": int,
    "object_cache_hits": int,
    "object_cache_misses": int,
    "cache_repairs": int,
    "records_considered": int,
    "records_scored": int,
    "graph_candidates_added": int,
    "resident_bytes_estimate": int,
    "elapsed_ms": int,
    "requests": RequestCounts,
}
CompactionReport.__annotations__ = {
    "compacted": bool,
    "source_level": int,
    "target_level": int,
    "segments_read": int,
    "segments_written": int,
    "records_rewritten": int,
    "routing_page_indexes_read": int,
    "routing_pages_read": int,
    "routing_page_indexes_written": int,
    "routing_pages_written": int,
    "graph_payloads_read": int,
    "graph_bytes_read": int,
    "bytes_read": int,
    "bytes_written": int,
    "object_cache_hits": int,
    "object_cache_misses": int,
    "manifest_version": int,
}
GarbageCollectionReport.__annotations__ = {
    "dry_run": bool,
    "objects_scanned": int,
    "objects_deleted": int,
    "routing_objects_deleted": int,
    "tables_deleted": int,
    "routing_page_indexes_read": int,
    "routing_pages_read": int,
    "bytes_read": int,
    "bytes_reclaimable": int,
    "bytes_reclaimed": int,
    "object_cache_hits": int,
    "object_cache_misses": int,
    "candidates": list[str],
}
RebuildReport.__annotations__ = {
    "compaction": CompactionReport,
    "garbage_collection": GarbageCollectionReport,
}


def _enum_value(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def _validate_optional_search_string(value: Any, field_name: str) -> str:
    value = _enum_value(value)
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string when set")
    return value


def _vector_rows(vectors: Sequence[Sequence[float]]) -> list[list[float]]:
    return [list(vector) for vector in vectors]


def _ids_are_all_strings(ids: Sequence[RecordId]) -> bool:
    return all(isinstance(id, str) for id in ids)


def _ids_contain_integers(ids: Sequence[RecordId]) -> bool:
    return any(isinstance(id, int) and not isinstance(id, bool) for id in ids)


def _integer_id_bytes(id: int) -> bytes:
    if isinstance(id, bool) or id < 0:
        raise ValueError("integer record ids must be non-negative")

    value = id
    chunks: list[int] = []
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            byte |= 0x80
        chunks.append(byte)
        if not value:
            return bytes(chunks)


def _id_bytes(id: RecordId) -> bytes:
    if isinstance(id, str):
        return id.encode("utf-8")
    if isinstance(id, int):
        return _integer_id_bytes(id)
    return bytes(id)


def _id_bytes_list(ids: Sequence[RecordId]) -> list[bytes]:
    return [_id_bytes(id) for id in ids]


def _search_kwargs(
    *,
    mode: SearchModeName | SearchMode,
    leaf_mode: LeafMode | LeafModeName,
    eps: float | None,
    max_segments: int | None,
    max_bytes: int | str | None,
    max_latency_ms: int | None,
    routing_page_overfetch: int | None,
    max_candidates_per_segment: int | None,
    guaranteed_recall: bool,
) -> dict[str, Any]:
    return {
        "mode": _validate_optional_search_string(mode, "mode"),
        "leaf_mode": _validate_optional_search_string(leaf_mode, "leaf_mode"),
        "eps": eps,
        "max_segments": _validate_optional_search_int(max_segments, "max_segments"),
        "max_bytes": _validate_optional_search_bytes(max_bytes),
        "max_latency_ms": _validate_optional_search_int(max_latency_ms, "max_latency_ms"),
        "routing_page_overfetch": _validate_optional_search_int(
            routing_page_overfetch,
            "routing_page_overfetch",
        ),
        "max_candidates_per_segment": _validate_optional_search_int(
            max_candidates_per_segment,
            "max_candidates_per_segment",
        ),
        "guaranteed_recall": _validate_bool(guaranteed_recall, "guaranteed_recall"),
    }


def _validate_optional_search_int(value: int | None, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer when set")
    return value


def _validate_required_int(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer when set")
    return value


def _validate_bool(value: bool, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean when set")
    return value


def _validate_non_negative_number(value: float, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a non-negative finite number")
    value = float(value)
    if not isfinite(value) or value < 0:
        raise ValueError(f"{field} must be a non-negative finite number")
    return value


def _validate_optional_search_bytes(value: int | str | None) -> int | str | None:
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("max_bytes must be an integer when set")
    return value


def _validate_optional_ram_budget(value: int | str | None) -> str | None:
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("ram_budget must be an integer when set")
    return f"{value}B"


def _validate_optional_cache_max_bytes(value: int | str | None) -> str | None:
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("cache_max_bytes must be an integer when set")
    return f"{value}B"


def _validate_search_k(k: int) -> int:
    if isinstance(k, bool) or not isinstance(k, int):
        raise ValueError("k must be an integer")
    return k


def create(
    *,
    uri: str,
    metric: VectorMetric,
    dim: int | None = None,
    dimensions: int | None = None,
    segment_size: int | None = None,
    segment_max_vectors: int | None = None,
    routing_page_fanout: int | None = None,
    graph_neighbors: int | None = None,
    ram_budget: int | str | None = None,
    cache_dir: str | None = None,
) -> Index:
    return _create(
        uri=uri,
        metric=_enum_value(metric),
        dim=_validate_optional_search_int(dim, "dim"),
        dimensions=_validate_optional_search_int(dimensions, "dimensions"),
        segment_size=_validate_optional_search_int(segment_size, "segment_size"),
        segment_max_vectors=_validate_optional_search_int(
            segment_max_vectors,
            "segment_max_vectors",
        ),
        routing_page_fanout=_validate_optional_search_int(
            routing_page_fanout,
            "routing_page_fanout",
        ),
        graph_neighbors=_validate_optional_search_int(
            graph_neighbors,
            "graph_neighbors",
        ),
        ram_budget=_validate_optional_ram_budget(ram_budget),
        cache_dir=cache_dir,
    )


def open(
    uri: str,
    cache_dir: str | None = None,
    ram_budget: int | str | None = None,
    resident_routing: bool = False,
    cache_max_bytes: int | str | None = None,
) -> Index:
    return _open(
        uri,
        cache_dir=cache_dir,
        ram_budget=_validate_optional_ram_budget(ram_budget),
        resident_routing=_validate_bool(resident_routing, "resident_routing"),
        cache_max_bytes=_validate_optional_cache_max_bytes(cache_max_bytes),
    )


def leaf_mode_names() -> list[CanonicalLeafMode]:
    return _leaf_mode_names()


def _validate_recall_k(k: int) -> int:
    if isinstance(k, bool) or not isinstance(k, int):
        raise ValueError("k must be an integer")
    if k <= 0:
        raise ValueError("k must be greater than zero")
    return k


def recall_at_k(exact_ids: Sequence[RecordId], actual_ids: Sequence[RecordId], k: int) -> float:
    k = _validate_recall_k(k)

    exact_top = {_id_bytes(id) for id in islice(exact_ids, k)}
    if not exact_top:
        return 0.0

    actual_top = {_id_bytes(id) for id in islice(actual_ids, k)}
    return len(exact_top.intersection(actual_top)) / len(exact_top)


def tie_aware_recall_at_k(
    exact_distances: Sequence[float],
    actual_distances: Sequence[float],
    k: int,
) -> float:
    k = _validate_recall_k(k)
    return _tie_aware_recall_at_k(list(exact_distances), list(actual_distances), k)


def vector_distance(
    metric: VectorMetric,
    left: Sequence[float],
    right: Sequence[float],
) -> float:
    return _vector_distance(_enum_value(metric), list(left), list(right))


def vector_metric_names() -> list[CanonicalVectorMetric]:
    return _vector_metric_names()


_index_add = Index.add
_index_add_with_report = Index.add_with_report
_index_add_id_bytes = Index.add_id_bytes
_index_add_buffer = Index.add_buffer
_index_add_buffer_id_bytes = Index.add_buffer_id_bytes
_index_stats = Index.stats
_index_search_ids = Index.search_ids
_index_search_id_bytes = Index.search_id_bytes
_index_search_vectors = Index.search_vectors
_index_get_vector = Index.get_vector
_index_get_vector_by_id = Index.get_vector_by_id
_index_search_ids_buffer = Index.search_ids_buffer
_index_search_id_bytes_buffer = Index.search_id_bytes_buffer
_index_search_vectors_buffer = Index.search_vectors_buffer
_index_search_ids_batch = Index.search_ids_batch
_index_search_id_bytes_batch = Index.search_id_bytes_batch
_index_search_vectors_batch = Index.search_vectors_batch
_index_search_ids_batch_buffer = Index.search_ids_batch_buffer
_index_search_id_bytes_batch_buffer = Index.search_id_bytes_batch_buffer
_index_search_vectors_batch_buffer = Index.search_vectors_batch_buffer
_index_search_with_report = Index.search_with_report
_index_search_with_report_buffer = Index.search_with_report_buffer
_index_search_batch_with_report = Index.search_batch_with_report
_index_search_batch_with_report_buffer = Index.search_batch_with_report_buffer
_index_compact = Index.compact
_index_rebuild = Index.rebuild
_index_gc_obsolete_segments = Index.gc_obsolete_segments


def _annotated_index_add(
    self: Index,
    vectors: Sequence[Sequence[float]],
    ids: Sequence[RecordId] | None = None,
) -> list[RecordId]:
    rows = _vector_rows(vectors)
    if ids is None:
        return _index_add(self, rows, None)
    ids_list = list(ids)
    if _ids_are_all_strings(ids_list):
        return _index_add(self, rows, ids_list)
    added = _index_add_id_bytes(self, rows, _id_bytes_list(ids_list))
    return ids_list if _ids_contain_integers(ids_list) else added


def _annotated_index_add_with_report(
    self: Index,
    vectors: Sequence[Sequence[float]],
    ids: Sequence[str] | None = None,
) -> tuple[list[str], AddReport]:
    rows = _vector_rows(vectors)
    if ids is None:
        return _index_add_with_report(self, rows, None)
    ids_list = list(ids)
    if not _ids_are_all_strings(ids_list):
        raise ValueError("add_with_report ids must be strings")
    return _index_add_with_report(self, rows, ids_list)


def _annotated_index_add_buffer(
    self: Index,
    vectors: Float32Buffer,
    ids: Sequence[RecordId] | None = None,
) -> list[RecordId]:
    if ids is None:
        return _index_add_buffer(self, vectors, None)
    ids_list = list(ids)
    if _ids_are_all_strings(ids_list):
        return _index_add_buffer(self, vectors, ids_list)
    added = _index_add_buffer_id_bytes(self, vectors, _id_bytes_list(ids_list))
    return ids_list if _ids_contain_integers(ids_list) else added


def _annotated_index_stats(self: Index) -> IndexStats:
    return _index_stats(self)


def _annotated_index_search_ids(
    self: Index,
    query: Sequence[float],
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[str]:
    return _index_search_ids(
        self,
        list(query),
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_id_bytes(
    self: Index,
    query: Sequence[float],
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[bytes]:
    return _index_search_id_bytes(
        self,
        list(query),
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_vectors(
    self: Index,
    query: Sequence[float],
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[list[float]]:
    return _index_search_vectors(
        self,
        list(query),
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_get_vector(self: Index, id: RecordId) -> list[float] | None:
    if isinstance(id, str):
        return _index_get_vector(self, id)
    return _index_get_vector_by_id(self, _id_bytes(id))


def _annotated_index_search_ids_buffer(
    self: Index,
    query: Float32Buffer,
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[str]:
    return _index_search_ids_buffer(
        self,
        query,
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_id_bytes_buffer(
    self: Index,
    query: Float32Buffer,
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[bytes]:
    return _index_search_id_bytes_buffer(
        self,
        query,
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_vectors_buffer(
    self: Index,
    query: Float32Buffer,
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[list[float]]:
    return _index_search_vectors_buffer(
        self,
        query,
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_ids_batch(
    self: Index,
    queries: Sequence[Sequence[float]],
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[list[str]]:
    return _index_search_ids_batch(
        self,
        _vector_rows(queries),
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_id_bytes_batch(
    self: Index,
    queries: Sequence[Sequence[float]],
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[list[bytes]]:
    return _index_search_id_bytes_batch(
        self,
        _vector_rows(queries),
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_vectors_batch(
    self: Index,
    queries: Sequence[Sequence[float]],
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[list[list[float]]]:
    return _index_search_vectors_batch(
        self,
        _vector_rows(queries),
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_ids_batch_buffer(
    self: Index,
    queries: Float32Buffer,
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[list[str]]:
    return _index_search_ids_batch_buffer(
        self,
        queries,
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_id_bytes_batch_buffer(
    self: Index,
    queries: Float32Buffer,
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[list[bytes]]:
    return _index_search_id_bytes_batch_buffer(
        self,
        queries,
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_vectors_batch_buffer(
    self: Index,
    queries: Float32Buffer,
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[list[list[float]]]:
    return _index_search_vectors_batch_buffer(
        self,
        queries,
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_with_report(
    self: Index,
    query: Sequence[float],
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> SearchReport:
    return _index_search_with_report(
        self,
        list(query),
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_with_report_buffer(
    self: Index,
    query: Float32Buffer,
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> SearchReport:
    return _index_search_with_report_buffer(
        self,
        query,
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_batch_with_report(
    self: Index,
    queries: Sequence[Sequence[float]],
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[SearchReport]:
    return _index_search_batch_with_report(
        self,
        _vector_rows(queries),
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_search_batch_with_report_buffer(
    self: Index,
    queries: Float32Buffer,
    k: int = 10,
    mode: SearchModeName | SearchMode = "exact",
    leaf_mode: LeafMode | LeafModeName = "graph",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
) -> list[SearchReport]:
    return _index_search_batch_with_report_buffer(
        self,
        queries,
        k=_validate_search_k(k),
        **_search_kwargs(
            mode=mode,
            leaf_mode=leaf_mode,
            eps=eps,
            max_segments=max_segments,
            max_bytes=max_bytes,
            max_latency_ms=max_latency_ms,
            routing_page_overfetch=routing_page_overfetch,
            max_candidates_per_segment=max_candidates_per_segment,
            guaranteed_recall=guaranteed_recall,
        ),
    )


def _annotated_index_compact(
    self: Index,
    *,
    source_level: int = 0,
    target_level: int = 1,
    max_segments: int | None = None,
    all_matching: bool = False,
    min_segments: int = 2,
    target_segment_max_vectors: int | None = None,
) -> CompactionReport:
    return _index_compact(
        self,
        source_level=_validate_required_int(source_level, "source_level"),
        target_level=_validate_required_int(target_level, "target_level"),
        max_segments=_validate_optional_search_int(max_segments, "max_segments"),
        all_matching=_validate_bool(all_matching, "all_matching"),
        min_segments=_validate_required_int(min_segments, "min_segments"),
        target_segment_max_vectors=_validate_optional_search_int(
            target_segment_max_vectors,
            "target_segment_max_vectors",
        ),
    )


def _annotated_index_rebuild(
    self: Index,
    *,
    source_level: int = 0,
    target_level: int = 1,
    min_segments: int = 1,
    target_segment_max_vectors: int | None = None,
    delete_obsolete: bool = False,
) -> RebuildReport:
    return _index_rebuild(
        self,
        source_level=_validate_required_int(source_level, "source_level"),
        target_level=_validate_required_int(target_level, "target_level"),
        min_segments=_validate_required_int(min_segments, "min_segments"),
        target_segment_max_vectors=_validate_optional_search_int(
            target_segment_max_vectors,
            "target_segment_max_vectors",
        ),
        delete_obsolete=_validate_bool(delete_obsolete, "delete_obsolete"),
    )


def _annotated_index_gc_obsolete_segments(
    self: Index,
    *,
    dry_run: bool = True,
    min_age_seconds: float = 86_400.0,
) -> GarbageCollectionReport:
    return _index_gc_obsolete_segments(
        self,
        dry_run=_validate_bool(dry_run, "dry_run"),
        min_age_seconds=_validate_non_negative_number(min_age_seconds, "min_age_seconds"),
    )


Index.add = _annotated_index_add
Index.add_with_report = _annotated_index_add_with_report
Index.add_buffer = _annotated_index_add_buffer
Index.stats = _annotated_index_stats
Index.search_ids = _annotated_index_search_ids
Index.search_id_bytes = _annotated_index_search_id_bytes
Index.search_vectors = _annotated_index_search_vectors
Index.get_vector = _annotated_index_get_vector
Index.search_ids_buffer = _annotated_index_search_ids_buffer
Index.search_id_bytes_buffer = _annotated_index_search_id_bytes_buffer
Index.search_vectors_buffer = _annotated_index_search_vectors_buffer
Index.search_ids_batch = _annotated_index_search_ids_batch
Index.search_id_bytes_batch = _annotated_index_search_id_bytes_batch
Index.search_vectors_batch = _annotated_index_search_vectors_batch
Index.search_ids_batch_buffer = _annotated_index_search_ids_batch_buffer
Index.search_id_bytes_batch_buffer = _annotated_index_search_id_bytes_batch_buffer
Index.search_vectors_batch_buffer = _annotated_index_search_vectors_batch_buffer
Index.search_with_report = _annotated_index_search_with_report
Index.search_with_report_buffer = _annotated_index_search_with_report_buffer
Index.search_batch_with_report = _annotated_index_search_batch_with_report
Index.search_batch_with_report_buffer = _annotated_index_search_batch_with_report_buffer
Index.compact = _annotated_index_compact
Index.rebuild = _annotated_index_rebuild
Index.gc_obsolete_segments = _annotated_index_gc_obsolete_segments


def minkowski_metric(p: float) -> MinkowskiMetric:
    power = float(p)
    if not isfinite(power) or power < 1.0:
        raise ValueError("Minkowski power must be greater than or equal to 1")
    return MinkowskiMetric(f"minkowski:{power:g}")


__all__ = [
    "AddReport",
    "BorsukError",
    "CanonicalLeafMode",
    "CanonicalVectorMetric",
    "CompactionReport",
    "Float32Buffer",
    "GarbageCollectionReport",
    "Hit",
    "Index",
    "IndexStats",
    "LeafMode",
    "LeafModeAlias",
    "LeafModeName",
    "MinkowskiMetric",
    "RecallGuarantee",
    "RecordId",
    "RebuildReport",
    "RequestCounts",
    "SearchModeName",
    "SearchTerminationReason",
    "SearchReport",
    "SearchMode",
    "VectorMetric",
    "VectorMetricAlias",
    "VectorMetricName",
    "create",
    "leaf_mode_names",
    "minkowski_metric",
    "open",
    "recall_at_k",
    "tie_aware_recall_at_k",
    "vector_distance",
    "vector_metric_names",
]
