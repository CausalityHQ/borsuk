"""Native Python API for BORSUK.

The implementation is provided by the Rust/PyO3 extension module
``borsuk._borsuk``. There is intentionally no subprocess or CLI fallback in the
runtime API.
"""

from collections.abc import Buffer, Mapping, Sequence
from enum import Enum
from itertools import islice
from math import isfinite
from typing import Any, Literal, NewType, TypeAlias

from ._borsuk import (
    AddReport,
    BorsukError,
    CompactionReport,
    DeleteReport,
    GarbageCollectionReport,
    Hit,
    IncrementalReport,
    Index,
    IndexStats,
    PurgeReport,
    RebuildReport,
    RequestCounts,
    SearchReport,
    WarmReport,
)
from ._borsuk import (
    create as _create,
)
from ._borsuk import (
    leaf_mode_names as _leaf_mode_names,
)
from ._borsuk import (
    open as _open,
)
from ._borsuk import (
    tie_aware_recall_at_k as _tie_aware_recall_at_k,
)
from ._borsuk import (
    vector_distance as _vector_distance,
)
from ._borsuk import (
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
    SRHT_PQ_SCAN = "srht-pq-scan"
    FAST_TURBOQUANT_MSE_SCAN = "fast-turboquant-mse-scan"
    FAST_TURBOQUANT_SCAN = "fast-turboquant-scan"
    GRAPH = "graph"
    VAMANA_PQ = "vamana-pq"
    HYBRID = "hybrid"


MinkowskiMetric = NewType("MinkowskiMetric", str)
Float32Buffer = Buffer
RecordId: TypeAlias = str | bytes | int
SparseVectorInput: TypeAlias = tuple[Sequence[int], Sequence[float]]
SparseRecordInput: TypeAlias = (
    SparseVectorInput | Mapping[str, Sequence[int] | Sequence[float]]
)
HybridFusion: TypeAlias = Literal["rrf", "weighted"]
LeafCapability: TypeAlias = Literal["pq-scan-only", "graph-enabled"]
CacheExecutionPolicy: TypeAlias = Literal["scan", "graph", "auto"]
GlobalScanCodec: TypeAlias = Literal[
    "pq-scan",
    "srht-pq-scan",
    "fast-turboquant-mse-scan",
    "fast-turboquant-scan",
]
VectorElementType: TypeAlias = Literal[
    "float32",
    "float16",
    "bfloat16",
    "float8-e4m3fn",
    "float8-e5m2",
    "fp8",
    "int8",
    "binary",
]


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
VectorMetric: TypeAlias = (
    CanonicalVectorMetric | VectorMetricAlias | MinkowskiMetric | VectorMetricName
)
SearchModeName: TypeAlias = Literal["exact", "approx"]
CanonicalLeafMode: TypeAlias = Literal[
    "flat-scan",
    "sq-scan",
    "pq-scan",
    "srht-pq-scan",
    "fast-turboquant-mse-scan",
    "fast-turboquant-scan",
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
# A named-vector spec: `{"dimensions": int, "metric": VectorMetric, "kind"?:
# "dense" | "sparse" | "late-interaction", "element_type"?: ...}`.
NamedVectorSpecInput: TypeAlias = Mapping[str, int | VectorMetric | str]
NamedVectorInput: TypeAlias = (
    Sequence[float]
    | Sequence[Sequence[float]]
    | Mapping[str, Sequence[int] | Sequence[float]]
)
NamedVectorRecordInput: TypeAlias = Mapping[str, NamedVectorInput]
HybridVectorInput: TypeAlias = Mapping[str, NamedVectorInput]


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
    "text": bool,
    "named_vectors": list[str],
    "sparse_encoded_vectors": int,
    "dense_encoded_vectors": int,
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
    "prepared_positioned_bytes": int,
    "collection_resident_bytes": int,
    "retained_bytes": int,
    "retained_capacity_bytes": int,
    "retained_peak_bytes": int,
    "transient_bytes": int,
    "transient_capacity_bytes": int,
    "transient_peak_bytes": int,
}
WarmReport.__annotations__ = {
    "segments_loaded": int,
    "segments_total": int,
    "segments_resident": int,
    "graphs_resident": int,
    "coverage_complete": bool,
    "bytes_resident": int,
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
    "decoded_cache_hits": int,
    "decoded_cache_bytes_read": int,
    "object_cache_hits": int,
    "object_cache_misses": int,
    "disk_cache_bytes_read": int,
    "backing_bytes_read": int,
    "disk_cache_reads": int,
    "backing_reads": int,
    "cache_repairs": int,
    "records_considered": int,
    "records_scored": int,
    "graph_candidates_added": int,
    "global_graph_chunks_searched": int,
    "global_scan_chunks_searched": int,
    "global_base_approximate_us": int,
    "global_base_head_admission_us": int,
    "global_base_head_fetch_us": int,
    "global_base_head_decode_admission_us": int,
    "global_base_head_decode_us": int,
    "global_base_exact_admission_us": int,
    "global_base_exact_fetch_us": int,
    "global_base_exact_read_us_max": int,
    "global_base_exact_read_us_sum": int,
    "global_base_exact_reads_over_20ms": int,
    "global_base_exact_reads_over_30ms": int,
    "global_base_exact_reads_over_50ms": int,
    "global_base_exact_reads_over_100ms": int,
    "global_base_exact_cpu_us": int,
    "global_base_exact_rerank_us": int,
    "resident_bytes_estimate": int,
    "prepared_positioned_bytes": int,
    "collection_resident_bytes": int,
    "retained_bytes": int,
    "retained_capacity_bytes": int,
    "retained_peak_bytes": int,
    "transient_bytes": int,
    "transient_capacity_bytes": int,
    "transient_peak_bytes": int,
    "elapsed_ms": int,
    "requests": RequestCounts,
    "rows_evaluated": int,
    "rows_passed_filter": int,
    "segments_pruned_by_filter": int,
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
    "transaction_states_remaining": int,
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


def _sparse_parts(
    indices: Sequence[int],
    values: Sequence[float],
) -> tuple[list[int], list[float]]:
    normalized_indices: list[int] = []
    for index in indices:
        if isinstance(index, bool) or not isinstance(index, int):
            raise ValueError("sparse indices must be integers")
        if index < 0 or index > 0xFFFFFFFF:
            raise ValueError("sparse indices must fit in u32")
        normalized_indices.append(index)
    try:
        normalized_values = [float(value) for value in values]
    except (TypeError, ValueError) as exc:
        raise ValueError("sparse values must be numbers") from exc
    return normalized_indices, normalized_values


def _normalize_sparse_entry(entry: Any) -> tuple[list[int], list[float]] | None:
    if entry is None:
        return None
    if (
        isinstance(entry, Sequence)
        and not isinstance(entry, (str, bytes, bytearray))
        and len(entry) == 2
    ):
        return _sparse_parts(entry[0], entry[1])
    if isinstance(entry, Mapping):
        try:
            return _sparse_parts(entry["indices"], entry["values"])
        except KeyError as exc:
            raise ValueError("sparse entries must provide indices and values") from exc
    try:
        return _sparse_parts(entry["indices"], entry["values"])
    except (KeyError, TypeError, AttributeError):
        pass
    if hasattr(entry, "indices") and hasattr(entry, "values"):
        return _sparse_parts(entry.indices, entry.values)
    raise ValueError(
        "sparse entries must be None, (indices, values), or provide indices and values"
    )


def _normalize_sparse_list(
    sparse: Sequence[SparseRecordInput | None] | None,
) -> list[tuple[list[int], list[float]] | None] | None:
    if sparse is None:
        return None
    return [_normalize_sparse_entry(entry) for entry in sparse]


NativeNamedVectorEntry: TypeAlias = tuple[
    str,
    list[float] | None,
    tuple[list[int], list[float]] | None,
    list[list[float]] | None,
]


def _normalize_named_vector_value(value: Any) -> NativeNamedVectorEntry:
    if isinstance(value, Mapping):
        sparse = _normalize_sparse_entry(value)
        if sparse is None:
            raise ValueError("named vector sparse entries cannot be None")
        return ("", None, sparse, None)
    if hasattr(value, "indices") and hasattr(value, "values"):
        sparse = _normalize_sparse_entry(value)
        if sparse is None:
            raise ValueError("named vector sparse entries cannot be None")
        return ("", None, sparse, None)
    if isinstance(value, (str, bytes, bytearray)):
        raise ValueError("named vector dense entries must be numeric sequences")
    try:
        values = list(value)
        if (
            values
            and isinstance(values[0], Sequence)
            and not isinstance(values[0], (str, bytes, bytearray))
        ):
            return ("", None, None, [list(token) for token in values])
        return ("", values, None, None)
    except TypeError as exc:
        raise ValueError(
            "named vector entries must be dense sequences or provide indices and values"
        ) from exc


def _normalize_named_vector_record(
    entry: NamedVectorRecordInput | None,
) -> list[NativeNamedVectorEntry] | None:
    if entry is None:
        return None
    if not isinstance(entry, Mapping):
        raise ValueError("named vector records must be dicts or None")
    normalized: list[NativeNamedVectorEntry] = []
    for name, value in entry.items():
        if not isinstance(name, str):
            raise ValueError("named vector names must be strings")
        _, dense, sparse, multi = _normalize_named_vector_value(value)
        normalized.append((name, dense, sparse, multi))
    return normalized


def _normalize_named_vector_list(
    named_vectors: Sequence[NamedVectorRecordInput | None] | None,
) -> list[list[NativeNamedVectorEntry] | None] | None:
    if named_vectors is None:
        return None
    return [_normalize_named_vector_record(entry) for entry in named_vectors]


def _normalize_hybrid_vectors(
    vectors: HybridVectorInput | None,
) -> list[NativeNamedVectorEntry] | None:
    if vectors is None:
        return None
    if not isinstance(vectors, Mapping):
        raise ValueError("hybrid vectors must be a dict")
    normalized: list[NativeNamedVectorEntry] = []
    for name, value in vectors.items():
        if not isinstance(name, str):
            raise ValueError("hybrid vector names must be strings")
        _, dense, sparse, multi = _normalize_named_vector_value(value)
        normalized.append((name, dense, sparse, multi))
    return normalized


def _normalize_text_list(text: Sequence[str | None] | None) -> list[str | None] | None:
    if text is None:
        return None
    rows = list(text)
    for row in rows:
        if row is not None and not isinstance(row, str):
            raise ValueError("text entries must be strings or None")
    return rows


def _validate_optional_payload_lengths(
    rows: Sequence[Sequence[float]],
    sparse: Sequence[object] | None,
    text: Sequence[str | None] | None,
    named_vectors: Sequence[object] | None,
) -> None:
    if sparse is not None and len(sparse) != len(rows):
        raise ValueError(
            f"sparse length {len(sparse)} must match vectors length {len(rows)}"
        )
    if text is not None and len(text) != len(rows):
        raise ValueError(
            f"text length {len(text)} must match vectors length {len(rows)}"
        )
    if named_vectors is not None and len(named_vectors) != len(rows):
        raise ValueError(
            f"named_vectors length {len(named_vectors)} must match vectors length {len(rows)}"
        )


def _normalize_weights(
    weights: Mapping[str, float] | None,
) -> list[tuple[str, float]] | None:
    if weights is None:
        return None
    if not isinstance(weights, Mapping):
        raise ValueError("weights must be a dict when set")
    normalized: list[tuple[str, float]] = []
    for name, weight in weights.items():
        if not isinstance(name, str):
            raise ValueError("weights names must be strings")
        try:
            normalized.append((name, float(weight)))
        except (TypeError, ValueError) as exc:
            raise ValueError("weights must contain numbers") from exc
    return normalized


def _validate_text_query(text: str) -> str:
    if not isinstance(text, str):
        raise ValueError("text must be a string")
    return text


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
        "max_latency_ms": _validate_optional_search_int(
            max_latency_ms, "max_latency_ms"
        ),
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


def _validate_global_pq_code_bytes(value: int | None) -> int | None:
    value = _validate_optional_search_int(value, "global_pq_code_bytes")
    if value is not None and (value <= 0 or value > 256 or value & (value - 1)):
        raise ValueError(
            "global_pq_code_bytes must be a power of two in 1..=256 when set"
        )
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


def _normalize_named_vector_specs(
    named_vectors: Mapping[str, NamedVectorSpecInput] | None,
) -> list[tuple[str, int, str, str, str]] | None:
    if named_vectors is None:
        return None
    if not isinstance(named_vectors, Mapping):
        raise ValueError("named_vectors must be a dict when set")
    normalized: list[tuple[str, int, str, str, str]] = []
    for name, spec in named_vectors.items():
        if not isinstance(name, str):
            raise ValueError("named vector names must be strings")
        if not isinstance(spec, Mapping):
            raise ValueError("named vector specs must be dicts")
        try:
            dimensions = _validate_required_int(spec["dimensions"], "dimensions")
            metric = _enum_value(spec["metric"])
        except KeyError as exc:
            raise ValueError(
                "named vector specs must provide dimensions and metric"
            ) from exc
        if not isinstance(metric, str):
            raise ValueError("named vector metric must be a string")
        # Optional "kind": dense, sparse, or late-interaction.
        kind = spec.get("kind", "dense")
        if kind not in ("dense", "sparse", "late-interaction"):
            raise ValueError(
                "named vector kind must be 'dense', 'sparse', or 'late-interaction'"
            )
        element_type = spec.get("element_type", "float32")
        if element_type not in (
            "float32",
            "float16",
            "bfloat16",
            "float8-e4m3fn",
            "float8-e5m2",
            "fp8",
            "int8",
            "binary",
        ):
            raise ValueError(
                "named vector element_type must be float32, float16, bfloat16, "
                "float8-e4m3fn, float8-e5m2, fp8, int8, or binary"
            )
        normalized.append((name, dimensions, metric, kind, element_type))
    return normalized


def create(
    *,
    uri: str,
    metric: VectorMetric,
    vector_element_type: VectorElementType = "float32",
    dim: int | None = None,
    dimensions: int | None = None,
    segment_size: int | None = None,
    segment_max_vectors: int | None = None,
    routing_page_fanout: int | None = None,
    graph_neighbors: int | None = None,
    leaf_capability: LeafCapability = "pq-scan-only",
    global_scan_codec: GlobalScanCodec = "srht-pq-scan",
    global_pq_layout: str = "adaptive",
    global_pq_code_bytes: int | None = None,
    turboquant_bits: int = 0,
    turboquant_qjl_bits: int = 0,
    turboquant_shards: int = 1,
    ram_budget: int | str | None = None,
    cache_dir: str | None = None,
    text: bool = False,
    named_vectors: Mapping[str, NamedVectorSpecInput] | None = None,
) -> Index:
    """Create an index.

    ``turboquant_bits=0`` selects the codec-specific qualified default: eight
    bits for ``fast-turboquant-scan`` and four for the MSE control codec. The
    resolved concrete value is persisted in the index manifest.
    """
    return _create(
        uri=uri,
        metric=_enum_value(metric),
        vector_element_type=vector_element_type,
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
        leaf_capability=leaf_capability,
        global_scan_codec=global_scan_codec,
        global_pq_layout=global_pq_layout,
        global_pq_code_bytes=_validate_global_pq_code_bytes(global_pq_code_bytes),
        turboquant_bits=_validate_required_int(turboquant_bits, "turboquant_bits"),
        turboquant_qjl_bits=_validate_required_int(
            turboquant_qjl_bits, "turboquant_qjl_bits"
        ),
        turboquant_shards=_validate_required_int(
            turboquant_shards, "turboquant_shards"
        ),
        ram_budget=_validate_optional_ram_budget(ram_budget),
        cache_dir=cache_dir,
        text=_validate_bool(text, "text"),
        named_vectors=_normalize_named_vector_specs(named_vectors),
    )


def open(
    uri: str,
    cache_dir: str | None = None,
    ram_budget: int | str | None = None,
    resident_routing: bool = False,
    cache_max_bytes: int | str | None = None,
    preload: bool = False,
    max_active_searches: int = 8,
    max_waiting_searches: int = 16,
    leaf_read_width: int = 32,
    max_inflight_leaf_reads: int = 48,
    exact_read_max_physical_amplification: int = 1,
    max_parallel_decode_rank_tasks: int = 1,
) -> Index:
    max_active_searches = _validate_required_int(
        max_active_searches, "max_active_searches"
    )
    max_waiting_searches = _validate_required_int(
        max_waiting_searches, "max_waiting_searches"
    )
    leaf_read_width = _validate_required_int(leaf_read_width, "leaf_read_width")
    max_inflight_leaf_reads = _validate_required_int(
        max_inflight_leaf_reads, "max_inflight_leaf_reads"
    )
    max_parallel_decode_rank_tasks = _validate_required_int(
        max_parallel_decode_rank_tasks, "max_parallel_decode_rank_tasks"
    )
    exact_read_max_physical_amplification = _validate_required_int(
        exact_read_max_physical_amplification,
        "exact_read_max_physical_amplification",
    )
    if max_active_searches <= 0:
        raise ValueError("max_active_searches must be greater than zero")
    if max_waiting_searches < 0:
        raise ValueError("max_waiting_searches must be non-negative")
    if leaf_read_width <= 0:
        raise ValueError("leaf_read_width must be greater than zero")
    if max_inflight_leaf_reads <= 0:
        raise ValueError("max_inflight_leaf_reads must be greater than zero")
    if max_parallel_decode_rank_tasks <= 0:
        raise ValueError("max_parallel_decode_rank_tasks must be greater than zero")
    if not 1 <= exact_read_max_physical_amplification <= 5:
        raise ValueError(
            "exact_read_max_physical_amplification must be between 1 and 5"
        )
    return _open(
        uri,
        cache_dir=cache_dir,
        ram_budget=_validate_optional_ram_budget(ram_budget),
        resident_routing=_validate_bool(resident_routing, "resident_routing"),
        cache_max_bytes=_validate_optional_cache_max_bytes(cache_max_bytes),
        preload=_validate_bool(preload, "preload"),
        max_active_searches=max_active_searches,
        max_waiting_searches=max_waiting_searches,
        leaf_read_width=leaf_read_width,
        max_inflight_leaf_reads=max_inflight_leaf_reads,
        max_parallel_decode_rank_tasks=max_parallel_decode_rank_tasks,
        exact_read_max_physical_amplification=exact_read_max_physical_amplification,
    )


def leaf_mode_names() -> list[CanonicalLeafMode]:
    return _leaf_mode_names()


def _validate_recall_k(k: int) -> int:
    if isinstance(k, bool) or not isinstance(k, int):
        raise ValueError("k must be an integer")
    if k <= 0:
        raise ValueError("k must be greater than zero")
    return k


def recall_at_k(
    exact_ids: Sequence[RecordId], actual_ids: Sequence[RecordId], k: int
) -> float:
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
_index_upsert = Index.upsert
_index_add_with_report = Index.add_with_report
_index_add_id_bytes = Index.add_id_bytes
_index_add_buffer = Index.add_buffer
_index_add_buffer_id_bytes = Index.add_buffer_id_bytes
_index_stats = Index.stats
_index_refresh = Index.refresh
_index_warm = Index.warm
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
_index_search_text = Index.search_text
_index_search_text_with_report = Index.search_text_with_report
_index_search_late_interaction = Index.search_late_interaction
_index_search_hybrid = Index.search_hybrid
_index_search_hybrid_with_report = Index.search_hybrid_with_report
_index_compact = Index.compact
_index_rebuild = Index.rebuild
_index_gc_obsolete_segments = Index.gc_obsolete_segments


def _annotated_index_upsert(
    self: Index,
    vectors: Sequence[Sequence[float]],
    ids: Sequence[str],
    metadata: Sequence[dict] | None = None,
    sparse: Sequence[SparseRecordInput | None] | None = None,
    text: Sequence[str | None] | None = None,
    named_vectors: Sequence[NamedVectorRecordInput | None] | None = None,
) -> list[RecordId]:
    """Insert or replace records by id (MVCC upsert).

    Existing ids are overwritten atomically — reads immediately see only the new
    record, and the superseded version is reclaimed by the next compaction. Ids
    are required (an upsert without ids is meaningless).
    """
    rows = _vector_rows(vectors)
    meta_list = list(metadata) if metadata is not None else None
    sparse_list = _normalize_sparse_list(sparse)
    text_list = _normalize_text_list(text)
    named_vector_list = _normalize_named_vector_list(named_vectors)
    _validate_optional_payload_lengths(rows, sparse_list, text_list, named_vector_list)
    return _index_upsert(
        self, rows, list(ids), meta_list, sparse_list, text_list, named_vector_list
    )


def _annotated_index_refresh(self: Index) -> bool:
    """Advance this handle to the latest atomically published snapshot.

    Returns ``True`` when the snapshot advanced and ``False`` when the handle
    was already current. Until refresh succeeds, reads keep using the handle's
    previously pinned snapshot.
    """
    return _index_refresh(self)


def _annotated_index_add(
    self: Index,
    vectors: Sequence[Sequence[float]],
    ids: Sequence[RecordId] | None = None,
    metadata: Sequence[dict] | None = None,
    sparse: Sequence[SparseRecordInput | None] | None = None,
    text: Sequence[str | None] | None = None,
    named_vectors: Sequence[NamedVectorRecordInput | None] | None = None,
) -> list[RecordId]:
    rows = _vector_rows(vectors)
    meta_list = list(metadata) if metadata is not None else None
    sparse_list = _normalize_sparse_list(sparse)
    text_list = _normalize_text_list(text)
    named_vector_list = _normalize_named_vector_list(named_vectors)
    _validate_optional_payload_lengths(rows, sparse_list, text_list, named_vector_list)
    if ids is None:
        if meta_list is not None:
            raise ValueError("metadata requires explicit ids")
        return _index_add(
            self, rows, None, None, sparse_list, text_list, named_vector_list
        )
    ids_list = list(ids)
    if _ids_are_all_strings(ids_list):
        return _index_add(
            self, rows, ids_list, meta_list, sparse_list, text_list, named_vector_list
        )
    if meta_list is not None:
        raise ValueError("metadata is only supported with string ids")
    if (
        sparse_list is not None
        or text_list is not None
        or named_vector_list is not None
    ):
        raise ValueError(
            "sparse, text, and named_vectors are only supported with string ids"
        )
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


def _annotated_index_warm(self: Index) -> WarmReport:
    """Eagerly load all active segments into RAM.

    Returns the number of newly loaded segments and the estimated decoded bytes
    resident for all active segments. Repeated calls are idempotent.
    """
    return _index_warm(self)


def _annotated_index_search_ids(
    self: Index,
    query: Sequence[float],
    k: int = 10,
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
    filter: dict | None = None,
    vector: str = "",
) -> list[str]:
    return _index_search_ids(
        self,
        list(query),
        k=_validate_search_k(k),
        filter=filter,
        vector=_validate_optional_search_string(vector, "vector"),
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
    vector: str = "",
) -> list[list[float]]:
    return _index_search_vectors(
        self,
        list(query),
        k=_validate_search_k(k),
        vector=_validate_optional_search_string(vector, "vector"),
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
    eps: float | None = None,
    max_segments: int | None = None,
    max_bytes: int | str | None = None,
    max_latency_ms: int | None = None,
    routing_page_overfetch: int | None = None,
    max_candidates_per_segment: int | None = None,
    guaranteed_recall: bool = False,
    filter: dict | None = None,
    include_metadata: bool = False,
    vector: str = "",
) -> SearchReport:
    return _index_search_with_report(
        self,
        list(query),
        k=_validate_search_k(k),
        filter=filter,
        include_metadata=include_metadata,
        vector=_validate_optional_search_string(vector, "vector"),
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


def _annotated_index_search_text(
    self: Index,
    text: str,
    k: int = 10,
) -> list[str]:
    return _index_search_text(
        self,
        _validate_text_query(text),
        k=_validate_search_k(k),
    )


def _annotated_index_search_late_interaction(
    self: Index,
    name: str,
    query_tokens: Sequence[Sequence[float]],
    k: int = 10,
) -> list[str]:
    return _index_search_late_interaction(
        self,
        _validate_optional_search_string(name, "name"),
        _vector_rows(query_tokens),
        k=_validate_search_k(k),
    )


def _annotated_index_search_text_with_report(
    self: Index,
    text: str,
    k: int = 10,
    include_metadata: bool = False,
) -> SearchReport:
    return _index_search_text_with_report(
        self,
        _validate_text_query(text),
        k=_validate_search_k(k),
        include_metadata=_validate_bool(include_metadata, "include_metadata"),
    )


def _annotated_index_search_hybrid(
    self: Index,
    *,
    vectors: HybridVectorInput | None = None,
    text: str | None = None,
    k: int = 10,
    fusion: HybridFusion = "rrf",
    rrf_k: int = 60,
    weights: Mapping[str, float] | None = None,
) -> list[str]:
    return _index_search_hybrid(
        self,
        vectors=_normalize_hybrid_vectors(vectors),
        text=_validate_text_query(text) if text is not None else None,
        k=_validate_search_k(k),
        fusion=_validate_optional_search_string(fusion, "fusion"),
        rrf_k=_validate_required_int(rrf_k, "rrf_k"),
        weights=_normalize_weights(weights),
    )


def _annotated_index_search_hybrid_with_report(
    self: Index,
    *,
    vectors: HybridVectorInput | None = None,
    text: str | None = None,
    k: int = 10,
    fusion: HybridFusion = "rrf",
    rrf_k: int = 60,
    weights: Mapping[str, float] | None = None,
    include_metadata: bool = False,
) -> SearchReport:
    return _index_search_hybrid_with_report(
        self,
        vectors=_normalize_hybrid_vectors(vectors),
        text=_validate_text_query(text) if text is not None else None,
        k=_validate_search_k(k),
        fusion=_validate_optional_search_string(fusion, "fusion"),
        rrf_k=_validate_required_int(rrf_k, "rrf_k"),
        weights=_normalize_weights(weights),
        include_metadata=_validate_bool(include_metadata, "include_metadata"),
    )


def _annotated_index_search_with_report_buffer(
    self: Index,
    query: Float32Buffer,
    k: int = 10,
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
    mode: SearchModeName | SearchMode = "approx",
    leaf_mode: LeafMode | LeafModeName = "srht-pq-scan",
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
        min_age_seconds=_validate_non_negative_number(
            min_age_seconds, "min_age_seconds"
        ),
    )


Index.add = _annotated_index_add
Index.upsert = _annotated_index_upsert
Index.add_with_report = _annotated_index_add_with_report
Index.add_buffer = _annotated_index_add_buffer
Index.stats = _annotated_index_stats
Index.refresh = _annotated_index_refresh
Index.warm = _annotated_index_warm
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
Index.search_text = _annotated_index_search_text
Index.search_text_with_report = _annotated_index_search_text_with_report
Index.search_late_interaction = _annotated_index_search_late_interaction
Index.search_hybrid = _annotated_index_search_hybrid
Index.search_hybrid_with_report = _annotated_index_search_hybrid_with_report
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
    "CacheExecutionPolicy",
    "CanonicalLeafMode",
    "CanonicalVectorMetric",
    "CompactionReport",
    "Float32Buffer",
    "GarbageCollectionReport",
    "GlobalScanCodec",
    "HybridFusion",
    "HybridVectorInput",
    "Hit",
    "Index",
    "IndexStats",
    "LeafCapability",
    "LeafMode",
    "LeafModeAlias",
    "LeafModeName",
    "MinkowskiMetric",
    "NamedVectorInput",
    "NamedVectorRecordInput",
    "NamedVectorSpecInput",
    "DeleteReport",
    "PurgeReport",
    "IncrementalReport",
    "RecallGuarantee",
    "RecordId",
    "RebuildReport",
    "RequestCounts",
    "SearchModeName",
    "SearchTerminationReason",
    "SearchReport",
    "SearchMode",
    "SparseRecordInput",
    "SparseVectorInput",
    "VectorMetric",
    "VectorElementType",
    "VectorMetricAlias",
    "VectorMetricName",
    "WarmReport",
    "create",
    "leaf_mode_names",
    "minkowski_metric",
    "open",
    "recall_at_k",
    "tie_aware_recall_at_k",
    "vector_distance",
    "vector_metric_names",
]
