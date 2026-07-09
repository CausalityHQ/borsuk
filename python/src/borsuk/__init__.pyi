from collections.abc import Buffer, Mapping, Sequence
from enum import Enum
from typing import Literal, NewType, TypeAlias

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
MinkowskiMetric = NewType("MinkowskiMetric", str)
Float32Buffer: TypeAlias = Buffer
RecordId: TypeAlias = str | bytes | int
SparseVectorInput: TypeAlias = tuple[Sequence[int], Sequence[float]]
SparseRecordInput: TypeAlias = (
    SparseVectorInput | Mapping[str, Sequence[int] | Sequence[float]]
)
HybridFusion: TypeAlias = Literal["rrf", "weighted"]

class BorsukError(RuntimeError):
    code: str

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

VectorMetric: TypeAlias = (
    CanonicalVectorMetric | VectorMetricAlias | MinkowskiMetric | VectorMetricName
)
SearchModeName: TypeAlias = Literal["exact", "approx"]
CanonicalLeafMode: TypeAlias = Literal[
    "flat-scan", "sq-scan", "pq-scan", "graph", "vamana-pq", "hybrid"
]
SearchTerminationReason: TypeAlias = Literal[
    "complete", "exact-pruned", "epsilon", "max-segments", "max-bytes", "max-latency"
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

class Hit:
    id: str
    id_bytes: bytes
    distance: float
    metadata: dict | None
    def __repr__(self) -> str: ...

class IndexStats:
    metric: CanonicalVectorMetric | MinkowskiMetric
    dimensions: int
    segment_max_vectors: int
    ram_budget_bytes: int | None
    text: bool
    sparse_encoded_vectors: int
    dense_encoded_vectors: int
    manifest_version: int
    routing_max_level: int
    routing_page_fanout: int
    routing_leaf_pages: int
    routing_pages: int
    segments: int
    records: int
    segment_bytes: int
    graph_bytes: int
    resident_bytes_estimate: int
    def __repr__(self) -> str: ...

class RequestCounts:
    gets: int
    puts: int
    deletes: int
    heads: int
    lists: int
    total: int
    def __repr__(self) -> str: ...

class AddReport:
    segments_written: int
    graph_payloads_written: int
    manifest_tables_written: int
    routing_pages_written: int
    total_bytes_written: int
    bytes_per_vector: float
    requests: RequestCounts
    def __repr__(self) -> str: ...

class SearchReport:
    hits: list[Hit]
    leaf_mode: CanonicalLeafMode
    termination_reason: SearchTerminationReason
    recall_guarantee: RecallGuarantee
    segments_total: int
    segments_searched: int
    segments_skipped: int
    routing_page_indexes_read: int
    routing_pages_read: int
    bytes_read: int
    prefetched_bytes_unused: int
    graph_bytes_read: int
    object_cache_hits: int
    object_cache_misses: int
    cache_repairs: int
    records_considered: int
    records_scored: int
    graph_candidates_added: int
    resident_bytes_estimate: int
    elapsed_ms: int
    requests: RequestCounts
    rows_evaluated: int
    rows_passed_filter: int
    segments_pruned_by_filter: int
    def __repr__(self) -> str: ...

class CompactionReport:
    compacted: bool
    source_level: int
    target_level: int
    segments_read: int
    segments_written: int
    records_rewritten: int
    routing_page_indexes_read: int
    routing_pages_read: int
    routing_page_indexes_written: int
    routing_pages_written: int
    graph_payloads_read: int
    graph_bytes_read: int
    bytes_read: int
    bytes_written: int
    object_cache_hits: int
    object_cache_misses: int
    manifest_version: int
    def __repr__(self) -> str: ...

class GarbageCollectionReport:
    dry_run: bool
    objects_scanned: int
    objects_deleted: int
    routing_objects_deleted: int
    tables_deleted: int
    routing_page_indexes_read: int
    routing_pages_read: int
    bytes_read: int
    bytes_reclaimable: int
    bytes_reclaimed: int
    object_cache_hits: int
    object_cache_misses: int
    candidates: list[str]
    def __repr__(self) -> str: ...

class RebuildReport:
    compaction: CompactionReport
    garbage_collection: GarbageCollectionReport
    def __repr__(self) -> str: ...

class DeleteReport:
    deleted: int
    total_tombstoned: int
    published: bool
    requests: RequestCounts
    def __repr__(self) -> str: ...

class PurgeReport:
    segments_rewritten: int
    records_purged: int
    tombstones_cleared: int
    published: bool
    requests: RequestCounts
    def __repr__(self) -> str: ...

class IncrementalReport:
    splits: int
    merges: int
    segments_created: int
    segments_removed: int
    records_moved: int
    published: bool
    requests: RequestCounts
    def __repr__(self) -> str: ...

class Index:
    def __init__(self, uri: str) -> None: ...
    def delete(self, ids: Sequence[str]) -> DeleteReport: ...
    def purge(self) -> PurgeReport: ...
    def maintain(
        self,
        *,
        max_segment_vectors: int | None = None,
        max_segment_radius: float | None = None,
        min_segment_vectors: int | None = None,
        max_operations: int | None = None,
    ) -> IncrementalReport: ...
    def add(
        self,
        vectors: Sequence[Sequence[float]],
        ids: Sequence[RecordId] | None = None,
        metadata: Sequence[dict] | None = None,
        sparse: Sequence[SparseRecordInput | None] | None = None,
        text: Sequence[str | None] | None = None,
    ) -> list[RecordId]: ...
    def add_with_report(
        self,
        vectors: Sequence[Sequence[float]],
        ids: Sequence[str] | None = None,
    ) -> tuple[list[str], AddReport]: ...
    def add_buffer(
        self,
        vectors: Float32Buffer,
        ids: Sequence[RecordId] | None = None,
    ) -> list[RecordId]: ...
    def stats(self) -> IndexStats: ...
    def search_ids(
        self,
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
        prefetch_depth: int | None = None,
        filter: dict | None = None,
    ) -> list[str]: ...
    def get_record(self, id: str) -> tuple[list[float], dict] | None: ...
    def list_records(
        self, offset: int = 0, limit: int = 100
    ) -> list[tuple[str, list[float], dict]]: ...
    def search_id_bytes(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[bytes]: ...
    def search_vectors(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[list[float]]: ...
    def get_vector(self, id: RecordId) -> list[float] | None: ...
    def search_ids_buffer(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[str]: ...
    def search_id_bytes_buffer(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[bytes]: ...
    def search_vectors_buffer(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[list[float]]: ...
    def search_ids_batch(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[list[str]]: ...
    def search_id_bytes_batch(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[list[bytes]]: ...
    def search_vectors_batch(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[list[list[float]]]: ...
    def search_ids_batch_buffer(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[list[str]]: ...
    def search_id_bytes_batch_buffer(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[list[bytes]]: ...
    def search_vectors_batch_buffer(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[list[list[float]]]: ...
    def search_with_report(
        self,
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
        prefetch_depth: int | None = None,
        filter: dict | None = None,
        include_metadata: bool = False,
    ) -> SearchReport: ...
    def search_text(
        self,
        text: str,
        k: int = 10,
    ) -> list[str]: ...
    def search_text_with_report(
        self,
        text: str,
        k: int = 10,
        include_metadata: bool = False,
    ) -> SearchReport: ...
    def search_hybrid(
        self,
        *,
        dense: Sequence[float] | None = None,
        text: str | None = None,
        k: int = 10,
        fusion: HybridFusion = "rrf",
        rrf_k: int = 60,
        weights: tuple[float, float] | None = None,
    ) -> list[str]: ...
    def search_hybrid_with_report(
        self,
        *,
        dense: Sequence[float] | None = None,
        text: str | None = None,
        k: int = 10,
        fusion: HybridFusion = "rrf",
        rrf_k: int = 60,
        weights: tuple[float, float] | None = None,
        include_metadata: bool = False,
    ) -> SearchReport: ...
    def search_with_report_buffer(
        self,
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
        prefetch_depth: int | None = None,
    ) -> SearchReport: ...
    def search_batch_with_report(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[SearchReport]: ...
    def search_batch_with_report_buffer(
        self,
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
        prefetch_depth: int | None = None,
    ) -> list[SearchReport]: ...
    def compact(
        self,
        *,
        source_level: int = 0,
        target_level: int = 1,
        max_segments: int | None = None,
        all_matching: bool = False,
        min_segments: int = 2,
        target_segment_max_vectors: int | None = None,
        target_segment_max_radius: float | None = None,
    ) -> CompactionReport: ...
    def rebuild(
        self,
        *,
        source_level: int = 0,
        target_level: int = 1,
        min_segments: int = 1,
        target_segment_max_vectors: int | None = None,
        delete_obsolete: bool = False,
    ) -> RebuildReport: ...
    def gc_obsolete_segments(
        self,
        *,
        dry_run: bool = True,
        min_age_seconds: float = 86_400.0,
    ) -> GarbageCollectionReport: ...

def create(
    *,
    uri: str,
    metric: VectorMetric | VectorMetricName,
    dim: int | None = None,
    dimensions: int | None = None,
    segment_size: int | None = None,
    segment_max_vectors: int | None = None,
    routing_page_fanout: int | None = None,
    graph_neighbors: int | None = None,
    ram_budget: int | str | None = None,
    cache_dir: str | None = None,
    text: bool = False,
) -> Index: ...
def open(
    uri: str,
    cache_dir: str | None = None,
    ram_budget: int | str | None = None,
    resident_routing: bool = False,
    cache_max_bytes: int | str | None = None,
) -> Index: ...
def leaf_mode_names() -> list[CanonicalLeafMode]: ...
def minkowski_metric(p: float) -> MinkowskiMetric: ...
def recall_at_k(
    exact_ids: Sequence[RecordId], actual_ids: Sequence[RecordId], k: int
) -> float: ...
def tie_aware_recall_at_k(
    exact_distances: Sequence[float],
    actual_distances: Sequence[float],
    k: int,
) -> float: ...
def vector_distance(
    metric: VectorMetric | VectorMetricName,
    left: Sequence[float],
    right: Sequence[float],
) -> float: ...
def vector_metric_names() -> list[CanonicalVectorMetric]: ...
