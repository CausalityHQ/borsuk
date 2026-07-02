"""Native Python API for BORSUK.

The implementation is provided by the Rust/PyO3 extension module
``borsuk._borsuk``. There is intentionally no subprocess or CLI fallback in the
runtime API.
"""

from enum import Enum
from typing import Any

from ._borsuk import (
    CompactionReport,
    BorsukError,
    GarbageCollectionReport,
    Hit,
    Index,
    IndexStats,
    SearchReport,
    create as _create,
    open,
    recall_at_k,
    string_distance as _string_distance,
    string_metric_names,
    vector_distance as _vector_distance,
    vector_metric_names,
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


def _enum_value(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def create(
    *,
    uri: str,
    metric: str | VectorMetricName,
    dim: int | None = None,
    dimensions: int | None = None,
    segment_size: int = 4096,
    segment_max_vectors: int | None = None,
    ram_budget: str | None = None,
    cache_dir: str | None = None,
) -> Index:
    return _create(
        uri=uri,
        metric=_enum_value(metric),
        dim=dim,
        dimensions=dimensions,
        segment_size=segment_size,
        segment_max_vectors=segment_max_vectors,
        ram_budget=ram_budget,
        cache_dir=cache_dir,
    )


def string_distance(metric: str | StringMetricName, left: str, right: str) -> float:
    return _string_distance(_enum_value(metric), left, right)


def vector_distance(metric: str | VectorMetricName, left: list[float], right: list[float]) -> float:
    return _vector_distance(_enum_value(metric), left, right)


__all__ = [
    "BorsukError",
    "CompactionReport",
    "GarbageCollectionReport",
    "Hit",
    "Index",
    "IndexStats",
    "SearchReport",
    "SearchMode",
    "StringMetricName",
    "VectorMetricName",
    "create",
    "open",
    "recall_at_k",
    "string_distance",
    "string_metric_names",
    "vector_distance",
    "vector_metric_names",
]
