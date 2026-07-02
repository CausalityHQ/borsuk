from enum import Enum
from typing import Any, Literal, Sequence, TypeAlias

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
VectorMetric: TypeAlias = CanonicalVectorMetric | VectorMetricAlias

CanonicalStringMetric: TypeAlias = Literal[
    "levenshtein",
    "normalized-levenshtein",
    "damerau-levenshtein",
    "normalized-damerau-levenshtein",
    "optimal-string-alignment",
    "hamming",
    "jaro",
    "jaro-winkler",
    "sorensen-dice",
]
StringMetricAlias: TypeAlias = Literal[
    "edit",
    "edit-distance",
    "normalized-edit",
    "normalized-edit-distance",
    "damerau",
    "normalized-damerau",
    "osa",
    "jarowinkler",
    "sorensendice",
    "dice",
]
StringMetric: TypeAlias = CanonicalStringMetric | StringMetricAlias
SearchModeName: TypeAlias = Literal["exact", "approx"]
PayloadRefs: TypeAlias = Sequence[str | None]

class BorsukError(RuntimeError): ...

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

class StringMetricName(str, Enum):
    LEVENSHTEIN = "levenshtein"
    NORMALIZED_LEVENSHTEIN = "normalized-levenshtein"
    DAMERAU_LEVENSHTEIN = "damerau-levenshtein"
    NORMALIZED_DAMERAU_LEVENSHTEIN = "normalized-damerau-levenshtein"
    OPTIMAL_STRING_ALIGNMENT = "optimal-string-alignment"
    HAMMING = "hamming"
    JARO = "jaro"
    JARO_WINKLER = "jaro-winkler"
    SORENSEN_DICE = "sorensen-dice"

class SearchMode(str, Enum):
    EXACT = "exact"
    APPROX = "approx"

class Hit:
    id: str
    distance: float
    payload_ref: str | None
    def __repr__(self) -> str: ...

class IndexStats:
    metric: str
    dimensions: int
    segment_max_vectors: int
    ram_budget_bytes: int | None
    manifest_version: int
    segments: int
    records: int
    segment_bytes: int
    graph_bytes: int
    resident_bytes_estimate: int
    def __repr__(self) -> str: ...

class SearchReport:
    hits: list[Hit]
    segments_total: int
    segments_searched: int
    segments_skipped: int
    bytes_read: int
    graph_bytes_read: int
    object_cache_hits: int
    object_cache_misses: int
    records_considered: int
    records_scored: int
    graph_candidates_added: int
    resident_bytes_estimate: int
    elapsed_ms: int
    def __repr__(self) -> str: ...

class CompactionReport:
    compacted: bool
    source_level: int
    target_level: int
    segments_read: int
    segments_written: int
    records_rewritten: int
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
    bytes_reclaimable: int
    bytes_reclaimed: int
    candidates: list[str]
    def __repr__(self) -> str: ...

class Index:
    def __init__(self, uri: str) -> None: ...
    def add(
        self,
        ids: Sequence[str],
        vectors: Sequence[Sequence[float]],
        payload_refs: PayloadRefs | None = None,
    ) -> None: ...
    def add_buffer(
        self,
        ids: Sequence[str],
        vectors: Any,
        payload_refs: PayloadRefs | None = None,
    ) -> None: ...
    def stats(self) -> IndexStats: ...
    def search(
        self,
        query: Sequence[float],
        k: int = 10,
        mode: SearchModeName | SearchMode = "exact",
        eps: float | None = None,
        max_segments: int | None = None,
        max_bytes: int | str | None = None,
        max_latency_ms: int | None = None,
        max_candidates_per_segment: int | None = None,
    ) -> list[Hit]: ...
    def search_buffer(
        self,
        query: Any,
        k: int = 10,
        mode: SearchModeName | SearchMode = "exact",
        eps: float | None = None,
        max_segments: int | None = None,
        max_bytes: int | str | None = None,
        max_latency_ms: int | None = None,
        max_candidates_per_segment: int | None = None,
    ) -> list[Hit]: ...
    def search_batch(
        self,
        queries: Sequence[Sequence[float]],
        k: int = 10,
        mode: SearchModeName | SearchMode = "exact",
        eps: float | None = None,
        max_segments: int | None = None,
        max_bytes: int | str | None = None,
        max_latency_ms: int | None = None,
        max_candidates_per_segment: int | None = None,
    ) -> list[list[Hit]]: ...
    def search_batch_buffer(
        self,
        queries: Any,
        k: int = 10,
        mode: SearchModeName | SearchMode = "exact",
        eps: float | None = None,
        max_segments: int | None = None,
        max_bytes: int | str | None = None,
        max_latency_ms: int | None = None,
        max_candidates_per_segment: int | None = None,
    ) -> list[list[Hit]]: ...
    def search_with_report(
        self,
        query: Sequence[float],
        k: int = 10,
        mode: SearchModeName | SearchMode = "exact",
        eps: float | None = None,
        max_segments: int | None = None,
        max_bytes: int | str | None = None,
        max_latency_ms: int | None = None,
        max_candidates_per_segment: int | None = None,
    ) -> SearchReport: ...
    def search_with_report_buffer(
        self,
        query: Any,
        k: int = 10,
        mode: SearchModeName | SearchMode = "exact",
        eps: float | None = None,
        max_segments: int | None = None,
        max_bytes: int | str | None = None,
        max_latency_ms: int | None = None,
        max_candidates_per_segment: int | None = None,
    ) -> SearchReport: ...
    def search_batch_with_report(
        self,
        queries: Sequence[Sequence[float]],
        k: int = 10,
        mode: SearchModeName | SearchMode = "exact",
        eps: float | None = None,
        max_segments: int | None = None,
        max_bytes: int | str | None = None,
        max_latency_ms: int | None = None,
        max_candidates_per_segment: int | None = None,
    ) -> list[SearchReport]: ...
    def search_batch_with_report_buffer(
        self,
        queries: Any,
        k: int = 10,
        mode: SearchModeName | SearchMode = "exact",
        eps: float | None = None,
        max_segments: int | None = None,
        max_bytes: int | str | None = None,
        max_latency_ms: int | None = None,
        max_candidates_per_segment: int | None = None,
    ) -> list[SearchReport]: ...
    def compact(
        self,
        *,
        source_level: int = 0,
        target_level: int = 1,
        max_segments: int | None = None,
        min_segments: int = 2,
        target_segment_max_vectors: int | None = None,
    ) -> CompactionReport: ...
    def gc_obsolete_segments(self, *, dry_run: bool = True) -> GarbageCollectionReport: ...

def create(
    *,
    uri: str,
    metric: VectorMetric | VectorMetricName,
    dim: int | None = None,
    dimensions: int | None = None,
    segment_size: int = 4096,
    segment_max_vectors: int | None = None,
    ram_budget: str | None = None,
    cache_dir: str | None = None,
) -> Index: ...
def open(uri: str, cache_dir: str | None = None, ram_budget: str | None = None) -> Index: ...
def recall_at_k(exact_ids: Sequence[str], actual_ids: Sequence[str], k: int) -> float: ...
def string_distance(metric: StringMetric | StringMetricName, left: str, right: str) -> float: ...
def string_metric_names() -> list[CanonicalStringMetric]: ...
def vector_distance(
    metric: VectorMetric | VectorMetricName,
    left: Sequence[float],
    right: Sequence[float],
) -> float: ...
def vector_metric_names() -> list[CanonicalVectorMetric]: ...
