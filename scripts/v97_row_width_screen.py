#!/usr/bin/env python3
"""G1 row-width quality screen for the native object-storage ANN router."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

DIMENSIONS = 768
PAGE_ROWS = 256
SUMMARY_CODES_PER_PAGE = 2
SUMMARY_CODE_BYTES = 16
THREE_GIB = 3 * 1024**3


@dataclass(frozen=True, slots=True)
class PqSpec:
    """One preregistered G1 routing representation."""

    name: str
    subspaces: int
    centroid_bits: int
    row_bytes: int
    row_codes_present: bool = True


@dataclass(frozen=True, slots=True)
class ObjectIdentity:
    """Immutable local object bound to its registered S3 identity."""

    uri: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        if (
            not self.uri.startswith("s3://")
            or len(self.sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.sha256)
            or self.bytes <= 0
        ):
            raise ValueError("object identity differs")


@dataclass(frozen=True, slots=True)
class ScreenAuthority:
    """Registered identities and query-blind training declaration."""

    source_commit: str
    critique_result_sha256: str
    page_map_sha256: str
    dimensions: int
    seed: int
    identities: Mapping[str, ObjectIdentity]

    def __post_init__(self) -> None:
        if (
            len(self.source_commit) != 40
            or any(character not in "0123456789abcdef" for character in self.source_commit)
            or len(self.critique_result_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.critique_result_sha256
            )
            or len(self.page_map_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.page_map_sha256
            )
            or self.dimensions <= 0
            or set(self.identities)
            != {"source", "queries", "truth", "generation", "base", "delta"}
        ):
            raise ValueError("screen authority differs")


PQ16X8 = PqSpec("pq16x8", 16, 8, 16)
PQ24X8 = PqSpec("pq24x8", 24, 8, 24)
PQ32X8 = PqSpec("pq32x8", 32, 8, 32)
PQ32X4 = PqSpec("pq32x4", 32, 4, 16)
SUMMARY_ONLY_PQ16X8 = PqSpec(
    "summary-only-pq16x8", 16, 8, 0, row_codes_present=False
)


@dataclass(frozen=True, slots=True)
class ResidentProjection:
    """Complete preregistered resident-memory worksheet."""

    rows: int
    pages: int
    row_codes_bytes: int
    summary_codes_bytes: int
    row_codebook_bytes: int
    summary_codebook_bytes: int
    sq8_parameter_bytes: int
    mutation_entries: int
    mutation_directory_bytes: int
    resident_delta_rows: int
    resident_delta_bytes: int
    page_directory_reserve_bytes: int
    response_buffers_bytes: int
    planner_workspace_bytes: int
    runtime_reserve_bytes: int
    total_bytes: int
    budget_bytes: int
    eligible: bool


@dataclass(frozen=True, order=True, slots=True)
class PageKey:
    """One physical page within one immutable run object."""

    object_role: str
    ordinal: int

    def __post_init__(self) -> None:
        if self.object_role not in ("base", "delta") or self.ordinal < 0:
            raise ValueError("page key differs")


@dataclass(frozen=True, slots=True)
class RoutedPage:
    """Authenticated physical range charged by the G1 planner."""

    key: PageKey
    offset: int
    encoded_bytes: int


@dataclass(frozen=True, slots=True)
class SelectionEvidence:
    """One query's independently derivable containment evidence."""

    hit_ids: tuple[int, ...]
    hits: int
    hits10: int
    recall10_ppm: int
    recall100_ppm: int


@dataclass(frozen=True, slots=True)
class ScreenInputs:
    """Authenticated in-memory inputs to the bounded G1 comparison."""

    source_ids: np.ndarray
    vectors: np.ndarray
    queries: np.ndarray
    truth_ids: np.ndarray
    page_by_id: Mapping[int, PageKey]
    pages: Mapping[PageKey, RoutedPage]
    row_order_by_page: Mapping[PageKey, Sequence[int]]
    seed: int
    neighbors: int
    max_gets: int
    max_bytes: int
    summary_page_limit: int = 128
    shortlist_rows: int = 512
    training_rows: int = 100_000
    training_iterations: int = 10


@dataclass(frozen=True, slots=True)
class ScreenResult:
    """Per-query evidence emitted by the five-arm bounded screen."""

    query_count: int
    exact_f32_samples: tuple[dict[str, object], ...]
    arms: Mapping[str, tuple[dict[str, object], ...]]
    artifact_identities: Mapping[str, Mapping[str, object]]


def _sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _authenticate_object(
    role: str, path: pathlib.Path, identity: ObjectIdentity
) -> None:
    if (
        not path.is_file()
        or path.stat().st_size != identity.bytes
        or _sha256_file(path) != identity.sha256
    ):
        raise ValueError(f"{role} identity differs")


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _array_identity(value: np.ndarray) -> dict[str, object]:
    array = np.ascontiguousarray(value)
    body = array.tobytes(order="C")
    return {
        "bytes": len(body),
        "dtype": array.dtype.name,
        "sha256": hashlib.sha256(body).hexdigest(),
        "shape": list(array.shape),
    }


def _read_page_run(
    path: pathlib.Path,
    run: Mapping[str, object],
    *,
    role: str,
    dimensions: int,
    identity: ObjectIdentity,
) -> tuple[dict[int, PageKey], dict[PageKey, RoutedPage], dict[PageKey, tuple[int, ...]]]:
    registered = run.get("object")
    if registered != {
        "bytes": identity.bytes,
        "sha256": identity.sha256,
        "uri": identity.uri,
    }:
        raise ValueError(f"{role} generation binding differs")
    pages_raw = run.get("pages")
    if not isinstance(pages_raw, list) or not pages_raw:
        raise ValueError(f"{role} page roster differs")
    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("sequence", pa.uint64(), nullable=False),
            pa.field("state", pa.uint8(), nullable=False),
            pa.field(
                "code",
                pa.list_(
                    pa.field("element", pa.uint8(), nullable=False), dimensions
                ),
                nullable=False,
            ),
        ]
    )
    page_by_id: dict[int, PageKey] = {}
    pages: dict[PageKey, RoutedPage] = {}
    rows_by_page: dict[PageKey, tuple[int, ...]] = {}
    source = pa.memory_map(str(path), "r")
    try:
        for expected_ordinal, raw in enumerate(pages_raw):
            if (
                not isinstance(raw, dict)
                or set(raw) != {"bytes", "offset", "page", "rows"}
                or any(type(raw[field]) is not int for field in raw)
                or raw["page"] != expected_ordinal
                or raw["bytes"] <= 0
                or raw["offset"] < 0
                or raw["rows"] <= 0
                or raw["offset"] + raw["bytes"] > identity.bytes
            ):
                raise ValueError(f"{role} page roster differs")
            key = PageKey(role, expected_ordinal)
            body = source.read_at(raw["bytes"], raw["offset"])
            table = ipc.open_stream(body).read_all()
            if table.schema != expected_schema or table.num_rows != raw["rows"]:
                raise ValueError(f"{role} page schema differs")
            ids = tuple(int(value) for value in table.column("id").to_pylist())
            states = table.column("state").to_pylist()
            if len(set(ids)) != len(ids) or any(state != 0 for state in states):
                raise ValueError(f"{role} page rows differ")
            for row_id in ids:
                if row_id in page_by_id:
                    raise ValueError(f"{role} page rows differ")
                page_by_id[row_id] = key
            pages[key] = RoutedPage(
                key=key,
                offset=raw["offset"],
                encoded_bytes=raw["bytes"],
            )
            rows_by_page[key] = ids
    finally:
        source.close()
    return page_by_id, pages, rows_by_page


def load_screen_inputs(
    *,
    source: pathlib.Path,
    queries: pathlib.Path,
    truth: pathlib.Path,
    generation: pathlib.Path,
    base: pathlib.Path,
    delta: pathlib.Path,
    identities: Mapping[str, ObjectIdentity],
    dimensions: int,
    neighbors: int,
    query_count: int,
    seed: int,
) -> ScreenInputs:
    """Authenticate and load the frozen base+delta G1 generation."""

    paths = {
        "source": source,
        "queries": queries,
        "truth": truth,
        "generation": generation,
        "base": base,
        "delta": delta,
    }
    if set(identities) != set(paths) or dimensions <= 0 or neighbors <= 0 or query_count <= 0:
        raise ValueError("screen input identity roster differs")
    for role, path in paths.items():
        _authenticate_object(role, path, identities[role])

    generation_body = generation.read_bytes()
    manifest = json.loads(generation_body)
    if (
        _canonical_json_bytes(manifest) != generation_body
        or not isinstance(manifest, dict)
        or manifest.get("dimensions") != dimensions
        or not isinstance(manifest.get("runs"), list)
    ):
        raise ValueError("generation authority differs")
    base_runs = [run for run in manifest["runs"] if run.get("kind") == "base"]
    delta_runs = [run for run in manifest["runs"] if run.get("kind") == "delta"]
    if len(base_runs) != 1 or len(delta_runs) != 1:
        raise ValueError("generation run roster differs")
    base_map, base_pages, base_rows = _read_page_run(
        base,
        base_runs[0],
        role="base",
        dimensions=dimensions,
        identity=identities["base"],
    )
    delta_map, delta_pages, delta_rows = _read_page_run(
        delta,
        delta_runs[0],
        role="delta",
        dimensions=dimensions,
        identity=identities["delta"],
    )
    if set(base_map).intersection(delta_map):
        raise ValueError("generation rows overlap")
    page_by_id = {**base_map, **delta_map}
    pages = {**base_pages, **delta_pages}
    row_order_by_page = {**base_rows, **delta_rows}

    source_table = pq.read_table(source)
    source_schema = pa.schema(
        [
            pa.field("feature_row_id", pa.uint64(), nullable=False),
            pa.field(
                "embedding",
                pa.list_(pa.field("item", pa.float32(), nullable=False), dimensions),
                nullable=False,
            ),
        ]
    )
    if source_table.schema != source_schema:
        raise ValueError("source schema differs")
    source_ids = np.asarray(
        source_table.column("feature_row_id").combine_chunks().to_numpy(),
        dtype=np.int64,
    )
    source_column = source_table.column("embedding").combine_chunks()
    vectors = np.asarray(source_column.values.to_numpy(), dtype=np.float32).reshape(
        -1, dimensions
    )
    if (
        source_column.null_count
        or not np.isfinite(vectors).all()
        or len(set(int(value) for value in source_ids)) != source_ids.size
        or set(int(value) for value in source_ids) != set(page_by_id)
    ):
        raise ValueError("source authority differs")

    query_table = pq.read_table(queries)
    query_schema = pa.schema(
        [
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field("feature_row_id", pa.uint64(), nullable=False),
            pa.field(
                "embedding",
                pa.list_(pa.field("item", pa.float32(), nullable=False), dimensions),
                nullable=False,
            ),
        ]
    )
    if query_table.schema != query_schema or query_table.num_rows != query_count:
        raise ValueError("query schema differs")
    query_ordinals = np.asarray(
        query_table.column("query_ordinal").combine_chunks().to_numpy(),
        dtype=np.uint32,
    )
    query_column = query_table.column("embedding").combine_chunks()
    query_vectors = np.asarray(
        query_column.values.to_numpy(), dtype=np.float32
    ).reshape(-1, dimensions)
    if (
        query_column.null_count
        or not np.array_equal(query_ordinals, np.arange(query_count, dtype=np.uint32))
        or not np.isfinite(query_vectors).all()
    ):
        raise ValueError("query authority differs")

    truth_table = pq.read_table(truth)
    truth_schema = pa.schema(
        [
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field("rank", pa.uint16(), nullable=False),
            pa.field("feature_row_id", pa.uint64(), nullable=False),
            pa.field("squared_distance", pa.float64(), nullable=False),
        ]
    )
    if (
        truth_table.schema != truth_schema
        or truth_table.num_rows != query_count * neighbors
    ):
        raise ValueError("truth schema differs")
    truth_ordinals = np.asarray(
        truth_table.column("query_ordinal").combine_chunks().to_numpy(),
        dtype=np.uint32,
    )
    truth_ranks = np.asarray(
        truth_table.column("rank").combine_chunks().to_numpy(), dtype=np.uint16
    )
    truth_unsigned = np.asarray(
        truth_table.column("feature_row_id").combine_chunks().to_numpy(),
        dtype=np.uint64,
    )
    truth_distances = np.asarray(
        truth_table.column("squared_distance").combine_chunks().to_numpy(),
        dtype=np.float64,
    ).reshape(query_count, neighbors)
    if (
        np.any(truth_unsigned > np.iinfo(np.int64).max)
        or not np.array_equal(
            truth_ordinals,
            np.repeat(np.arange(query_count, dtype=np.uint32), neighbors),
        )
        or not np.array_equal(
            truth_ranks,
            np.tile(np.arange(neighbors, dtype=np.uint16), query_count),
        )
        or not np.isfinite(truth_distances).all()
        or np.any(truth_distances < 0)
        or np.any(truth_distances[:, 1:] < truth_distances[:, :-1])
    ):
        raise ValueError("truth authority differs")
    truth_ids = truth_unsigned.astype(np.int64, copy=False).reshape(
        query_count, neighbors
    )
    if any(
        len(set(int(value) for value in row)) != neighbors for row in truth_ids
    ) or any(int(value) not in page_by_id for value in truth_ids.reshape(-1)):
        raise ValueError("truth authority differs")
    return ScreenInputs(
        source_ids=source_ids,
        vectors=np.ascontiguousarray(vectors),
        queries=np.ascontiguousarray(query_vectors),
        truth_ids=np.ascontiguousarray(truth_ids),
        page_by_id=page_by_id,
        pages=pages,
        row_order_by_page=row_order_by_page,
        seed=seed,
        neighbors=neighbors,
        max_gets=32,
        max_bytes=16 * 1024**2,
    )


def page_map_sha256(inputs: ScreenInputs) -> str:
    """Hash the complete tier-aware page roster and row membership."""

    document = [
        {
            "bytes": inputs.pages[key].encoded_bytes,
            "object_role": key.object_role,
            "offset": inputs.pages[key].offset,
            "ordinal": key.ordinal,
            "row_ids": [int(row_id) for row_id in inputs.row_order_by_page[key]],
        }
        for key in sorted(inputs.pages)
    ]
    return hashlib.sha256(_canonical_json_bytes(document)).hexdigest()


def pack_pq4(codes: np.ndarray) -> np.ndarray:
    """Pack even/odd four-bit subspaces into low/high nibbles."""

    values = np.asarray(codes)
    if (
        values.ndim != 2
        or values.shape[1] == 0
        or values.shape[1] % 2
        or not np.issubdtype(values.dtype, np.integer)
        or np.any(values < 0)
        or np.any(values > 15)
    ):
        raise ValueError("4-bit PQ codes differ")
    unsigned = values.astype(np.uint8, copy=False)
    return np.ascontiguousarray(unsigned[:, 0::2] | (unsigned[:, 1::2] << 4))


def unpack_pq4(packed: np.ndarray, subspaces: int) -> np.ndarray:
    """Decode the canonical low-even/high-odd nibble layout."""

    values = np.asarray(packed)
    if (
        values.ndim != 2
        or not np.issubdtype(values.dtype, np.integer)
        or subspaces <= 0
        or subspaces % 2
        or values.shape[1] * 2 != subspaces
        or np.any(values < 0)
        or np.any(values > 255)
    ):
        raise ValueError("packed 4-bit PQ shape differs")
    unsigned = values.astype(np.uint8, copy=False)
    codes = np.empty((values.shape[0], subspaces), dtype=np.uint8)
    codes[:, 0::2] = unsigned & 0x0F
    codes[:, 1::2] = unsigned >> 4
    return codes


def score_packed_pq4(packed: np.ndarray, tables: np.ndarray) -> np.ndarray:
    """Score packed rows using an ascending-subspace float32 ADC sum."""

    lookup = np.asarray(tables)
    if (
        lookup.ndim != 2
        or lookup.shape[0] == 0
        or lookup.shape[0] % 2
        or lookup.shape[1] != 16
        or lookup.dtype != np.float32
        or not np.isfinite(lookup).all()
    ):
        raise ValueError("4-bit PQ score table differs")
    codes = unpack_pq4(packed, lookup.shape[0])
    scores = np.zeros(codes.shape[0], dtype=np.float32)
    for subspace in range(lookup.shape[0]):
        scores += lookup[subspace, codes[:, subspace]]
    return scores


def adc_scores(
    query: np.ndarray,
    books: np.ndarray,
    codes: np.ndarray,
    spec: PqSpec,
) -> np.ndarray:
    """Score rows with the native ascending-subspace float32 ADC order."""

    vector = np.asarray(query)
    centroids = np.asarray(books)
    expected_centroids = 1 << spec.centroid_bits
    if (
        vector.ndim != 1
        or vector.dtype != np.float32
        or not np.isfinite(vector).all()
        or centroids.ndim != 3
        or centroids.dtype != np.float32
        or centroids.shape[0] != spec.subspaces
        or centroids.shape[1] != expected_centroids
        or centroids.shape[2] * spec.subspaces != vector.size
        or not np.isfinite(centroids).all()
    ):
        raise ValueError("PQ ADC authority differs")
    if spec.centroid_bits == 4:
        decoded = unpack_pq4(codes, spec.subspaces)
    else:
        decoded = np.asarray(codes)
        if (
            decoded.ndim != 2
            or decoded.shape[1] != spec.subspaces
            or decoded.dtype != np.uint8
        ):
            raise ValueError("PQ row-code authority differs")
    scores = np.zeros(decoded.shape[0], dtype=np.float32)
    width = centroids.shape[2]
    for subspace in range(spec.subspaces):
        lo = subspace * width
        hi = lo + width
        delta = centroids[subspace] - vector[lo:hi]
        table = np.einsum("ij,ij->i", delta, delta, dtype=np.float32)
        scores += table[decoded[:, subspace]]
    if not np.isfinite(scores).all():
        raise ValueError("PQ ADC scores are non-finite")
    return scores


def page_block_means(
    vectors_by_page: Mapping[PageKey, np.ndarray],
) -> tuple[tuple[PageKey, ...], np.ndarray]:
    """Build the native two contiguous means for every physical page."""

    if not vectors_by_page:
        raise ValueError("page-summary input is empty")
    keys: list[PageKey] = []
    means: list[np.ndarray] = []
    dimensions: int | None = None
    for key in sorted(vectors_by_page):
        page = np.asarray(vectors_by_page[key])
        if (
            page.ndim != 2
            or page.shape[0] == 0
            or page.dtype != np.float32
            or not np.isfinite(page).all()
            or (dimensions is not None and page.shape[1] != dimensions)
        ):
            raise ValueError("page-summary vectors differ")
        dimensions = page.shape[1]
        split = (page.shape[0] + 1) // 2
        first = page[:split]
        second = page[split:] if split < page.shape[0] else page[:1]
        keys.extend((key, key))
        means.extend(
            (
                first.mean(axis=0, dtype=np.float32),
                second.mean(axis=0, dtype=np.float32),
            )
        )
    return tuple(keys), np.ascontiguousarray(np.stack(means), dtype=np.float32)


def fit_pq(
    training_vectors: np.ndarray,
    spec: PqSpec,
    *,
    seed: int,
    sample_rows: int = 100_000,
    iterations: int = 10,
) -> np.ndarray:
    """Fit one deterministic query-blind product quantizer."""

    vectors = np.asarray(training_vectors)
    centroids_per_subspace = 1 << spec.centroid_bits
    if (
        vectors.ndim != 2
        or vectors.dtype != np.float32
        or vectors.shape[1] % spec.subspaces
        or vectors.shape[0] < centroids_per_subspace
        or sample_rows < centroids_per_subspace
        or iterations <= 0
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("PQ training authority differs")
    generator = np.random.default_rng(seed)
    take = min(vectors.shape[0], sample_rows)
    sample = np.ascontiguousarray(
        vectors[generator.choice(vectors.shape[0], take, replace=False)]
    )
    width = vectors.shape[1] // spec.subspaces
    books = np.empty(
        (spec.subspaces, centroids_per_subspace, width), dtype=np.float32
    )
    for subspace in range(spec.subspaces):
        lo = subspace * width
        hi = lo + width
        training = np.ascontiguousarray(sample[:, lo:hi])
        centroids = training[
            generator.choice(training.shape[0], centroids_per_subspace, replace=False)
        ].copy()
        for _ in range(iterations):
            norms = np.einsum("ij,ij->i", centroids, centroids, dtype=np.float32)
            assignment = np.empty(training.shape[0], dtype=np.int32)
            for start in range(0, training.shape[0], 8192):
                stop = min(start + 8192, training.shape[0])
                block = training[start:stop]
                assignment[start:stop] = np.argmin(
                    norms[None, :] - np.float32(2.0) * (block @ centroids.T),
                    axis=1,
                )
            counts = np.bincount(assignment, minlength=centroids_per_subspace)
            order = np.argsort(assignment, kind="stable")
            starts = np.concatenate(([0], np.cumsum(counts)[:-1]))
            occupied = counts > 0
            centroids[occupied] = (
                np.add.reduceat(training[order], starts[occupied], axis=0)
                / counts[occupied, None]
            )
        books[subspace] = centroids
    return books


def encode_pq(vectors: np.ndarray, books: np.ndarray, spec: PqSpec) -> np.ndarray:
    """Encode rows and return their canonical persistent representation."""

    values = np.asarray(vectors)
    centroids = np.asarray(books)
    centroid_count = 1 << spec.centroid_bits
    if (
        values.ndim != 2
        or values.dtype != np.float32
        or centroids.ndim != 3
        or centroids.dtype != np.float32
        or centroids.shape[0] != spec.subspaces
        or centroids.shape[1] != centroid_count
        or centroids.shape[2] * spec.subspaces != values.shape[1]
        or not np.isfinite(values).all()
        or not np.isfinite(centroids).all()
    ):
        raise ValueError("PQ encoding authority differs")
    codes = np.empty((values.shape[0], spec.subspaces), dtype=np.uint8)
    width = centroids.shape[2]
    for subspace in range(spec.subspaces):
        lo = subspace * width
        hi = lo + width
        book = centroids[subspace]
        norms = np.einsum("ij,ij->i", book, book, dtype=np.float32)
        for start in range(0, values.shape[0], 8192):
            stop = min(start + 8192, values.shape[0])
            block = values[start:stop, lo:hi]
            codes[start:stop, subspace] = np.argmin(
                norms[None, :] - np.float32(2.0) * (block @ book.T), axis=1
            )
    return pack_pq4(codes) if spec.centroid_bits == 4 else codes


def top_k_total_order(scores: np.ndarray, ids: np.ndarray, count: int) -> np.ndarray:
    """Return top-k row indices under the global `(score, id)` order."""

    distances = np.asarray(scores)
    row_ids = np.asarray(ids)
    if (
        distances.ndim != 1
        or row_ids.ndim != 1
        or distances.shape != row_ids.shape
        or not np.issubdtype(distances.dtype, np.floating)
        or not np.issubdtype(row_ids.dtype, np.integer)
        or not np.isfinite(distances).all()
        or len(set(int(value) for value in row_ids)) != row_ids.size
        or not 0 < count <= row_ids.size
    ):
        raise ValueError("rank input differs")
    if count == distances.size:
        selected = np.arange(distances.size, dtype=np.int64)
    else:
        boundary = np.partition(distances, count - 1)[count - 1]
        below = np.flatnonzero(distances < boundary)
        tied = np.flatnonzero(distances == boundary)
        tied = tied[np.argsort(row_ids[tied], kind="stable")]
        selected = np.concatenate((below, tied[: count - below.size]))
    order = np.lexsort((row_ids[selected], distances[selected]))
    return selected[order].astype(np.int64, copy=False)


def select_budgeted_pages(
    ranked_pages: Iterable[PageKey],
    pages: Mapping[PageKey, RoutedPage],
    *,
    max_gets: int,
    max_bytes: int,
) -> tuple[PageKey, ...]:
    """Select first-ranked distinct pages within exact GET and byte caps."""

    if max_gets <= 0 or max_bytes <= 0 or not pages:
        raise ValueError("page budget differs")
    selected: list[PageKey] = []
    seen: set[PageKey] = set()
    encoded_bytes = 0
    for key in ranked_pages:
        page = pages.get(key)
        if page is None:
            raise ValueError("ranked page is not registered")
        if (
            page.key != key
            or page.offset < 0
            or page.encoded_bytes <= 0
        ):
            raise ValueError("registered page range differs")
        if key in seen:
            continue
        seen.add(key)
        if encoded_bytes + page.encoded_bytes > max_bytes:
            continue
        selected.append(key)
        encoded_bytes += page.encoded_bytes
        if len(selected) == max_gets:
            break
    return tuple(selected)


def evaluate_selected_pages(
    *,
    selected_pages: Iterable[PageKey],
    truth_ids: np.ndarray,
    page_by_id: Mapping[int, PageKey],
    neighbors: int,
) -> SelectionEvidence:
    """Derive recall evidence solely from truth and selected membership."""

    truth = np.asarray(truth_ids)
    selected = tuple(selected_pages)
    if (
        truth.ndim != 1
        or not np.issubdtype(truth.dtype, np.integer)
        or truth.size != neighbors
        or neighbors <= 0
        or len(set(selected)) != len(selected)
        or len(set(int(value) for value in truth)) != truth.size
        or any(int(value) not in page_by_id for value in truth)
    ):
        raise ValueError("page-selection evidence differs")
    selected_set = set(selected)
    hits = tuple(
        int(row_id)
        for row_id in truth
        if page_by_id[int(row_id)] in selected_set
    )
    cutoff = min(10, neighbors)
    hits10 = sum(
        page_by_id[int(row_id)] in selected_set for row_id in truth[:cutoff]
    )
    return SelectionEvidence(
        hit_ids=hits,
        hits=len(hits),
        hits10=hits10,
        recall10_ppm=hits10 * 1_000_000 // cutoff,
        recall100_ppm=len(hits) * 1_000_000 // neighbors,
    )


def _sample_evidence(
    *,
    query_ordinal: int,
    selected_pages: Sequence[PageKey],
    truth_ids: np.ndarray,
    page_by_id: Mapping[int, PageKey],
    pages: Mapping[PageKey, RoutedPage],
    neighbors: int,
) -> dict[str, object]:
    evidence = evaluate_selected_pages(
        selected_pages=selected_pages,
        truth_ids=truth_ids,
        page_by_id=page_by_id,
        neighbors=neighbors,
    )
    cutoff = min(10, neighbors)
    selected_set = set(selected_pages)
    return {
        "bytes": sum(pages[key].encoded_bytes for key in selected_pages),
        "gets": len(selected_pages),
        "hit10_ids": [
            int(row_id)
            for row_id in truth_ids[:cutoff]
            if page_by_id[int(row_id)] in selected_set
        ],
        "hit_ids": list(evidence.hit_ids),
        "hits10": evidence.hits10,
        "hits": evidence.hits,
        "query": query_ordinal,
        "ranges": [
            {
                "bytes": pages[key].encoded_bytes,
                "object_role": key.object_role,
                "offset": pages[key].offset,
                "ordinal": key.ordinal,
            }
            for key in selected_pages
        ],
        "recall10_ppm": evidence.recall10_ppm,
        "recall100_ppm": evidence.recall100_ppm,
        "selected_pages": [
            {"object_role": key.object_role, "ordinal": key.ordinal}
            for key in selected_pages
        ],
        "truth_ids": [int(row_id) for row_id in truth_ids],
    }


def _rank_pages(page_scores: np.ndarray, page_keys: Sequence[PageKey]) -> tuple[PageKey, ...]:
    scores = np.asarray(page_scores)
    if (
        scores.ndim != 1
        or scores.size != len(page_keys)
        or not np.isfinite(scores).all()
        or len(set(page_keys)) != len(page_keys)
    ):
        raise ValueError("page ranking authority differs")
    roles = np.fromiter(
        (0 if key.object_role == "base" else 1 for key in page_keys),
        dtype=np.int8,
    )
    ordinals = np.fromiter((key.ordinal for key in page_keys), dtype=np.int64)
    order = np.lexsort((ordinals, roles, scores))
    return tuple(page_keys[int(index)] for index in order)


def evaluate_width_arms(inputs: ScreenInputs) -> ScreenResult:
    """Evaluate the registered five arms behind one bounded summary fence."""

    source_ids = np.asarray(inputs.source_ids)
    vectors = np.asarray(inputs.vectors)
    queries = np.asarray(inputs.queries)
    truth_ids = np.asarray(inputs.truth_ids)
    if (
        source_ids.ndim != 1
        or not np.issubdtype(source_ids.dtype, np.integer)
        or vectors.ndim != 2
        or vectors.dtype != np.float32
        or vectors.shape[0] != source_ids.size
        or queries.ndim != 2
        or queries.dtype != np.float32
        or queries.shape[1] != vectors.shape[1]
        or truth_ids.shape != (queries.shape[0], inputs.neighbors)
        or not np.issubdtype(truth_ids.dtype, np.integer)
        or not np.isfinite(vectors).all()
        or not np.isfinite(queries).all()
        or len(set(int(value) for value in source_ids)) != source_ids.size
        or inputs.max_gets <= 0
        or inputs.max_bytes <= 0
        or inputs.summary_page_limit <= 0
        or inputs.shortlist_rows <= 0
    ):
        raise ValueError("screen input authority differs")
    row_ids = tuple(int(value) for value in source_ids)
    if set(row_ids) != set(inputs.page_by_id):
        raise ValueError("screen page membership differs")
    if set(inputs.pages) != set(inputs.row_order_by_page):
        raise ValueError("screen page roster differs")
    ordered_ids = tuple(
        int(row_id)
        for key in sorted(inputs.row_order_by_page)
        for row_id in inputs.row_order_by_page[key]
    )
    if (
        len(ordered_ids) != len(set(ordered_ids))
        or set(ordered_ids) != set(row_ids)
        or any(inputs.page_by_id[row_id] != key for key in inputs.row_order_by_page for row_id in inputs.row_order_by_page[key])
        or any(inputs.pages[key].key != key for key in inputs.pages)
    ):
        raise ValueError("screen page membership differs")

    position_by_id = {row_id: position for position, row_id in enumerate(row_ids)}
    vectors_by_page = {
        key: np.ascontiguousarray(
            vectors[[position_by_id[int(row_id)] for row_id in rows]]
        )
        for key, rows in inputs.row_order_by_page.items()
    }
    summary_keys, summary_vectors = page_block_means(vectors_by_page)
    base_summary_positions = [
        position
        for position, key in enumerate(summary_keys)
        if key.object_role == "base"
    ]
    summary_books = fit_pq(
        np.ascontiguousarray(summary_vectors[base_summary_positions]),
        PQ16X8,
        seed=inputs.seed ^ 0x53554D4D,
        sample_rows=inputs.training_rows,
        iterations=inputs.training_iterations,
    )
    summary_codes = encode_pq(summary_vectors, summary_books, PQ16X8)
    artifact_identities: dict[str, Mapping[str, object]] = {
        "summary-router": {
            "codebook": _array_identity(summary_books),
            "codes": _array_identity(summary_codes),
        }
    }
    page_keys = tuple(sorted(inputs.pages))
    if summary_keys != tuple(key for key in page_keys for _ in range(2)):
        raise ValueError("page-summary order differs")

    base_positions = np.asarray(
        [
            position
            for position, row_id in enumerate(row_ids)
            if inputs.page_by_id[row_id].object_role == "base"
        ],
        dtype=np.int64,
    )
    base_vectors = np.ascontiguousarray(vectors[base_positions])
    exact_samples: list[dict[str, object]] = []
    summary_samples: list[dict[str, object]] = []
    fences: list[tuple[np.ndarray, tuple[PageKey, ...]]] = []
    for query_ordinal, query in enumerate(queries):
        summary_scores = adc_scores(query, summary_books, summary_codes, PQ16X8)
        ranked_pages = _rank_pages(
            summary_scores.reshape(-1, 2).min(axis=1), page_keys
        )
        fence = ranked_pages[: min(inputs.summary_page_limit, len(ranked_pages))]
        candidate_ids = np.asarray(
            [
                int(row_id)
                for key in fence
                for row_id in inputs.row_order_by_page[key]
            ],
            dtype=np.int64,
        )
        candidate_positions = np.asarray(
            [position_by_id[int(row_id)] for row_id in candidate_ids], dtype=np.int64
        )
        fences.append((candidate_positions, fence))
        delta = vectors[candidate_positions] - query
        exact_scores = np.einsum("ij,ij->i", delta, delta, dtype=np.float32)
        exact_order = top_k_total_order(
            exact_scores,
            candidate_ids,
            min(inputs.shortlist_rows, candidate_ids.size),
        )
        exact_selected = select_budgeted_pages(
            (inputs.page_by_id[int(candidate_ids[index])] for index in exact_order),
            inputs.pages,
            max_gets=inputs.max_gets,
            max_bytes=inputs.max_bytes,
        )
        exact_samples.append(
            _sample_evidence(
                query_ordinal=query_ordinal,
                selected_pages=exact_selected,
                truth_ids=truth_ids[query_ordinal],
                page_by_id=inputs.page_by_id,
                pages=inputs.pages,
                neighbors=inputs.neighbors,
            )
        )
        summary_selected = select_budgeted_pages(
            ranked_pages,
            inputs.pages,
            max_gets=inputs.max_gets,
            max_bytes=inputs.max_bytes,
        )
        summary_samples.append(
            _sample_evidence(
                query_ordinal=query_ordinal,
                selected_pages=summary_selected,
                truth_ids=truth_ids[query_ordinal],
                page_by_id=inputs.page_by_id,
                pages=inputs.pages,
                neighbors=inputs.neighbors,
            )
        )

    arms: dict[str, tuple[dict[str, object], ...]] = {}
    for spec in (PQ16X8, PQ24X8, PQ32X8, PQ32X4):
        books = fit_pq(
            base_vectors,
            spec,
            seed=inputs.seed,
            sample_rows=inputs.training_rows,
            iterations=inputs.training_iterations,
        )
        codes = encode_pq(vectors, books, spec)
        artifact_identities[spec.name] = {
            "codebook": _array_identity(books),
            "codes": _array_identity(codes),
        }
        samples: list[dict[str, object]] = []
        for query_ordinal, query in enumerate(queries):
            candidate_positions, _ = fences[query_ordinal]
            candidate_ids = source_ids[candidate_positions]
            scores = adc_scores(query, books, codes[candidate_positions], spec)
            order = top_k_total_order(
                scores,
                candidate_ids,
                min(inputs.shortlist_rows, candidate_ids.size),
            )
            selected = select_budgeted_pages(
                (inputs.page_by_id[int(candidate_ids[index])] for index in order),
                inputs.pages,
                max_gets=inputs.max_gets,
                max_bytes=inputs.max_bytes,
            )
            samples.append(
                _sample_evidence(
                    query_ordinal=query_ordinal,
                    selected_pages=selected,
                    truth_ids=truth_ids[query_ordinal],
                    page_by_id=inputs.page_by_id,
                    pages=inputs.pages,
                    neighbors=inputs.neighbors,
                )
            )
        arms[spec.name] = tuple(samples)
    arms[SUMMARY_ONLY_PQ16X8.name] = tuple(summary_samples)
    return ScreenResult(
        query_count=queries.shape[0],
        exact_f32_samples=tuple(exact_samples),
        arms=arms,
        artifact_identities=artifact_identities,
    )


def _aggregate_samples(samples: Sequence[Mapping[str, object]]) -> dict[str, object]:
    recall10 = [int(sample["recall10_ppm"]) for sample in samples]
    recall100 = [int(sample["recall100_ppm"]) for sample in samples]
    gets = [int(sample["gets"]) for sample in samples]
    encoded_bytes = [int(sample["bytes"]) for sample in samples]
    if not samples:
        raise ValueError("screen samples are empty")
    ordered = sorted(recall100)
    p05_index = max(0, (len(ordered) * 5 + 99) // 100 - 1)
    average10 = sum(recall10) // len(samples)
    average100 = sum(recall100) // len(samples)
    p05 = ordered[p05_index]
    maximum_gets = max(gets)
    maximum_bytes = max(encoded_bytes)
    return {
        "average_recall10_ppm": average10,
        "average_recall100_ppm": average100,
        "p05_recall100_ppm": p05,
        "worst_recall100_ppm": ordered[0],
        "max_gets_per_query": maximum_gets,
        "max_bytes_per_query": maximum_bytes,
        "quality_gate_passed": (
            average10 >= 960_000 and average100 >= 975_000 and p05 >= 900_000
        ),
        "resource_gate_passed": maximum_gets <= 32 and maximum_bytes <= 16 * 1024**2,
    }


def _producer_paired_interval(
    challenger: Sequence[int],
    control: Sequence[int],
    *,
    seed: int,
    resamples: int,
    statistic: str,
) -> list[int]:
    left = np.asarray(challenger, dtype=np.int64)
    right = np.asarray(control, dtype=np.int64)
    generator = np.random.default_rng(seed)
    differences = np.empty(resamples, dtype=np.float64)
    p05_index = max(0, (left.size * 5 + 99) // 100 - 1)
    for start in range(0, resamples, 256):
        stop = min(start + 256, resamples)
        draws = generator.integers(0, left.size, size=(stop - start, left.size))
        left_draws = left[draws]
        right_draws = right[draws]
        if statistic == "mean":
            differences[start:stop] = (left_draws - right_draws).mean(axis=1)
        else:
            differences[start:stop] = (
                np.partition(left_draws, p05_index, axis=1)[:, p05_index]
                - np.partition(right_draws, p05_index, axis=1)[:, p05_index]
            )
    lower, upper = np.quantile(differences, [0.025, 0.975], method="nearest")
    return [int(np.rint(lower)), int(np.rint(upper))]


def screen_result_document(
    result: ScreenResult,
    *,
    authority: ScreenAuthority,
    neighbors: int,
    max_gets: int,
    max_bytes: int,
    bootstrap_seed: int,
    bootstrap_resamples: int,
) -> dict[str, object]:
    """Build the producer's canonical-friendly, independently checked claims."""

    if (
        result.query_count <= 0
        or neighbors <= 0
        or max_gets != 32
        or max_bytes != 16 * 1024**2
        or bootstrap_resamples != 10_000
    ):
        raise ValueError("screen result configuration differs")
    exact_samples = list(result.exact_f32_samples)
    exact_aggregate = _aggregate_samples(exact_samples)
    control = result.arms[PQ16X8.name]
    control10 = [int(sample["recall10_ppm"]) for sample in control]
    control100 = [int(sample["recall100_ppm"]) for sample in control]
    specs = {
        spec.name: spec
        for spec in (PQ16X8, PQ24X8, PQ32X8, PQ32X4, SUMMARY_ONLY_PQ16X8)
    }
    arm_documents: list[dict[str, object]] = []
    aggregates: dict[str, dict[str, object]] = {}
    paired_r100_ci: dict[str, list[int]] = {}
    for name, samples_tuple in result.arms.items():
        samples = list(samples_tuple)
        aggregate = _aggregate_samples(samples)
        aggregates[name] = aggregate
        recall10 = [int(sample["recall10_ppm"]) for sample in samples]
        recall100 = [int(sample["recall100_ppm"]) for sample in samples]
        spec = specs[name]
        average_recall100_ci = _producer_paired_interval(
            recall100,
            control100,
            seed=bootstrap_seed,
            resamples=bootstrap_resamples,
            statistic="mean",
        )
        paired_r100_ci[name] = average_recall100_ci
        arm_documents.append(
            {
                "aggregate": aggregate,
                "name": name,
                "paired_vs_pq16": {
                    "average_recall10_ppm": _producer_paired_interval(
                        recall10,
                        control10,
                        seed=bootstrap_seed,
                        resamples=bootstrap_resamples,
                        statistic="mean",
                    ),
                    "average_recall100_ppm": average_recall100_ci,
                    "p05_recall100_ppm": _producer_paired_interval(
                        recall100,
                        control100,
                        seed=bootstrap_seed,
                        resamples=bootstrap_resamples,
                        statistic="p05",
                    ),
                },
                "projection": asdict(project_resident_bytes_100m(spec)),
                "row_bytes": spec.row_bytes,
                "samples": samples,
            }
        )
    eligible = [
        name
        for name, aggregate in aggregates.items()
        if bool(exact_aggregate["quality_gate_passed"])
        and bool(exact_aggregate["resource_gate_passed"])
        and bool(aggregate["quality_gate_passed"])
        and bool(aggregate["resource_gate_passed"])
        and project_resident_bytes_100m(specs[name]).eligible
        and paired_r100_ci[name][1] >= 0
    ]
    winner = None
    if eligible:
        winner = min(
            eligible,
            key=lambda name: (
                specs[name].row_bytes,
                -int(aggregates[name]["p05_recall100_ppm"]),
                -int(aggregates[name]["average_recall10_ppm"]),
                name,
            ),
        )
    return {
        "arms": arm_documents,
        "artifacts": result.artifact_identities,
        "authority": {
            "critique_result_sha256": authority.critique_result_sha256,
            "dimensions": authority.dimensions,
            "identities": {
                role: asdict(identity)
                for role, identity in sorted(authority.identities.items())
            },
            "page_map_sha256": authority.page_map_sha256,
            "seed": authority.seed,
            "source_commit": authority.source_commit,
            "training_declaration": "source-only-base-tier",
        },
        "bootstrap_resamples": bootstrap_resamples,
        "bootstrap_seed": bootstrap_seed,
        "exact_f32": {"aggregate": exact_aggregate, "samples": exact_samples},
        "max_bytes": max_bytes,
        "max_gets": max_gets,
        "neighbors": neighbors,
        "queries": result.query_count,
        "schema": "borsuk-v97-row-width-screen-v1",
        "winner": winner,
    }


def run_screen(
    inputs: ScreenInputs,
    *,
    identities: Mapping[str, ObjectIdentity],
    source_commit: str,
    critique_result_sha256: str,
    output: pathlib.Path,
) -> dict[str, object]:
    """Run one local immutable screen and write canonical result bytes."""

    result = evaluate_width_arms(inputs)
    authority = ScreenAuthority(
        source_commit=source_commit,
        critique_result_sha256=critique_result_sha256,
        page_map_sha256=page_map_sha256(inputs),
        dimensions=inputs.vectors.shape[1],
        seed=inputs.seed,
        identities=identities,
    )
    document = screen_result_document(
        result,
        authority=authority,
        neighbors=inputs.neighbors,
        max_gets=inputs.max_gets,
        max_bytes=inputs.max_bytes,
        bootstrap_seed=inputs.seed,
        bootstrap_resamples=10_000,
    )
    output.write_bytes(_canonical_json_bytes(document))
    return document


def parse_screen_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the exact development-only G1 producer surface."""

    parser = argparse.ArgumentParser()
    for role in ("source", "queries", "truth", "generation", "base", "delta"):
        parser.add_argument(f"--{role}", type=pathlib.Path, required=True)
        parser.add_argument(f"--{role}-uri", required=True)
        parser.add_argument(f"--{role}-sha256", required=True)
        parser.add_argument(f"--{role}-bytes", type=int, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--critique-result-sha256", required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--dimensions", type=int, default=768)
    parser.add_argument("--neighbors", type=int, default=100)
    parser.add_argument("--query-count", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=7216)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_screen_args(argv)
    identities = {
        role: ObjectIdentity(
            uri=getattr(args, f"{role}_uri"),
            sha256=getattr(args, f"{role}_sha256"),
            bytes=getattr(args, f"{role}_bytes"),
        )
        for role in ("source", "queries", "truth", "generation", "base", "delta")
    }
    inputs = load_screen_inputs(
        source=args.source,
        queries=args.queries,
        truth=args.truth,
        generation=args.generation,
        base=args.base,
        delta=args.delta,
        identities=identities,
        dimensions=args.dimensions,
        neighbors=args.neighbors,
        query_count=args.query_count,
        seed=args.seed,
    )
    document = run_screen(
        inputs,
        identities=identities,
        source_commit=args.source_commit,
        critique_result_sha256=args.critique_result_sha256,
        output=args.output,
    )
    print(
        json.dumps(
            {
                "output_bytes": args.output.stat().st_size,
                "output_sha256": _sha256_file(args.output),
                "winner": document["winner"],
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def project_resident_bytes_100m(spec: PqSpec) -> ResidentProjection:
    """Return the full 100M resident projection for one G1 arm."""

    rows = 100_000_000
    pages = (rows + PAGE_ROWS - 1) // PAGE_ROWS
    if (
        spec.subspaces <= 0
        or DIMENSIONS % spec.subspaces
        or spec.centroid_bits not in (4, 8)
        or spec.row_bytes < 0
        or spec.row_codes_present != (spec.row_bytes > 0)
    ):
        raise ValueError("PQ projection spec differs")
    centroids = 1 << spec.centroid_bits
    row_codes_bytes = rows * spec.row_bytes
    summary_codes_bytes = pages * SUMMARY_CODES_PER_PAGE * SUMMARY_CODE_BYTES
    row_codebook_bytes = centroids * DIMENSIONS * 4 if spec.row_codes_present else 0
    summary_codebook_bytes = 256 * DIMENSIONS * 4
    sq8_parameter_bytes = 2 * DIMENSIONS * 4
    mutation_entries = 1_000_000
    mutation_directory_bytes = mutation_entries * 96
    resident_delta_rows = 100_000
    resident_delta_bytes = resident_delta_rows * (DIMENSIONS + 72)
    page_directory_reserve_bytes = 128 * 1024**2
    response_buffers_bytes = 16 * 16 * 1024**2
    planner_workspace_bytes = 128_000_000
    runtime_reserve_bytes = 536_870_912
    total_bytes = sum(
        (
            row_codes_bytes,
            summary_codes_bytes,
            row_codebook_bytes,
            summary_codebook_bytes,
            sq8_parameter_bytes,
            mutation_directory_bytes,
            resident_delta_bytes,
            page_directory_reserve_bytes,
            response_buffers_bytes,
            planner_workspace_bytes,
            runtime_reserve_bytes,
        )
    )
    return ResidentProjection(
        rows=rows,
        pages=pages,
        row_codes_bytes=row_codes_bytes,
        summary_codes_bytes=summary_codes_bytes,
        row_codebook_bytes=row_codebook_bytes,
        summary_codebook_bytes=summary_codebook_bytes,
        sq8_parameter_bytes=sq8_parameter_bytes,
        mutation_entries=mutation_entries,
        mutation_directory_bytes=mutation_directory_bytes,
        resident_delta_rows=resident_delta_rows,
        resident_delta_bytes=resident_delta_bytes,
        page_directory_reserve_bytes=page_directory_reserve_bytes,
        response_buffers_bytes=response_buffers_bytes,
        planner_workspace_bytes=planner_workspace_bytes,
        runtime_reserve_bytes=runtime_reserve_bytes,
        total_bytes=total_bytes,
        budget_bytes=THREE_GIB,
        eligible=total_bytes < THREE_GIB,
    )


if __name__ == "__main__":
    main()
