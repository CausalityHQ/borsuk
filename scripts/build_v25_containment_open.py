#!/usr/bin/env python3
"""Research-only conversion of immutable V24 evidence into V25 Parquet inputs."""

from __future__ import annotations

import dataclasses
import hashlib
import heapq
import json
import math
import pathlib
import struct
import sys
from collections.abc import Iterable

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

_U64_MASK = (1 << 64) - 1


@dataclasses.dataclass(frozen=True)
class V25OpenSelection:
    dataset_ordinals: tuple[int, ...]
    query_source_ordinals: tuple[int, ...]
    dataset_ordinals_sha256: str


@dataclasses.dataclass(frozen=True)
class RegisteredV24Input:
    role: str
    path: pathlib.Path
    uri: str
    sha256: str
    encoded_bytes: int
    generation: str


@dataclasses.dataclass(frozen=True)
class V25OpenBuildRequest:
    construction: RegisteredV24Input
    page_rows: RegisteredV24Input
    output_dir: pathlib.Path
    output_uri_prefix: str
    output_generation: str
    source_row_count: int
    cohort_count: int
    pseudoquery_count: int
    page_count: int
    cohort_seed: int
    pseudoquery_seed: int
    output_row_group_size: int


@dataclasses.dataclass(frozen=True)
class V25OpenOutputIdentity:
    role: str
    uri: str
    digest_algorithm: str
    digest: str
    encoded_bytes: int
    generation: str


@dataclasses.dataclass(frozen=True)
class V25OpenBuildReceipt:
    selection: V25OpenSelection
    outputs: dict[str, V25OpenOutputIdentity]


def _splitmix64(value: int) -> int:
    value = (value + 0x9E3779B97F4A7C15) & _U64_MASK
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & _U64_MASK
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & _U64_MASK
    return value ^ (value >> 31)


def _smallest_ranked_ordinals(
    ordinals: range | tuple[int, ...], count: int, seed: int
) -> list[int]:
    heap: list[tuple[int, int, int]] = []
    for ordinal in ordinals:
        rank = _splitmix64(ordinal ^ seed)
        candidate = (-rank, -ordinal, ordinal)
        if len(heap) < count:
            heapq.heappush(heap, candidate)
        elif candidate > heap[0]:
            heapq.heapreplace(heap, candidate)
    return sorted(
        (item[2] for item in heap),
        key=lambda ordinal: (_splitmix64(ordinal ^ seed), ordinal),
    )


def select_v25_open_rows(
    *,
    source_row_count: int,
    cohort_count: int,
    pseudoquery_count: int,
    cohort_seed: int,
    pseudoquery_seed: int,
) -> V25OpenSelection:
    """Select and densely remap one query-independent open-screen cohort."""

    if (
        source_row_count <= 0
        or cohort_count <= 0
        or cohort_count > source_row_count
        or pseudoquery_count <= 0
        or pseudoquery_count >= cohort_count
        or cohort_seed < 0
        or cohort_seed > _U64_MASK
        or pseudoquery_seed < 0
        or pseudoquery_seed > _U64_MASK
        or cohort_seed == pseudoquery_seed
    ):
        raise ValueError("V25 open split authority differs")

    dataset_ordinals = tuple(
        _smallest_ranked_ordinals(range(source_row_count), cohort_count, cohort_seed)
    )
    query_dataset_ordinals = _smallest_ranked_ordinals(
        dataset_ordinals, pseudoquery_count, pseudoquery_seed
    )
    local_by_dataset = {
        dataset_ordinal: source_ordinal
        for source_ordinal, dataset_ordinal in enumerate(dataset_ordinals)
    }
    query_source_ordinals = tuple(
        local_by_dataset[dataset_ordinal] for dataset_ordinal in query_dataset_ordinals
    )
    digest = hashlib.sha256()
    for ordinal in dataset_ordinals:
        digest.update(struct.pack("<Q", ordinal))
    return V25OpenSelection(
        dataset_ordinals=dataset_ordinals,
        query_source_ordinals=query_source_ordinals,
        dataset_ordinals_sha256=digest.hexdigest(),
    )


def _sha256_file(path: pathlib.Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    encoded_bytes = 0
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            encoded_bytes += len(block)
            digest.update(block)
    return encoded_bytes, digest.hexdigest()


def _authenticate_input(value: RegisteredV24Input, role: str) -> None:
    if (
        value.role != role
        or not value.uri.startswith("s3://")
        or value.uri.endswith("/")
        or "/../" in value.uri
        or len(value.sha256) != 64
        or any(character not in "0123456789abcdef" for character in value.sha256)
        or value.encoded_bytes <= 0
        or not value.generation
        or _sha256_file(value.path) != (value.encoded_bytes, value.sha256)
    ):
        raise ValueError(f"V25 open {role} authority differs")


def _vector_type() -> pa.FixedSizeListType:
    return pa.list_(pa.field("element", pa.float32(), nullable=False), 96)


def _construction_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("source_ordinal", pa.uint64(), nullable=False),
            pa.field("vector", _vector_type(), nullable=False),
        ]
    )


def _page_rows_schema(construction_sha256: str, generation: str) -> pa.Schema:
    return pa.schema(
        [
            pa.field("page_ordinal", pa.uint32(), nullable=False),
            pa.field("replica", pa.bool_(), nullable=False),
            pa.field("record_id", pa.string(), nullable=False),
            pa.field("vector", _vector_type(), nullable=False),
        ],
        metadata={
            b"construction_rows_sha256": construction_sha256.encode(),
            b"generation": generation.encode(),
        },
    )


def _fixed_vectors(values: np.ndarray) -> pa.FixedSizeListArray:
    flat = pa.array(values.astype(np.float32, copy=False).reshape(-1), type=pa.float32())
    return pa.FixedSizeListArray.from_arrays(flat, type=_vector_type())


def _normalize_vectors(values: np.ndarray) -> np.ndarray:
    vectors = np.asarray(values, dtype=np.float32)
    if vectors.ndim != 2 or vectors.shape[1] != 96 or not np.isfinite(vectors).all():
        raise ValueError("V25 open vector values differ")
    vectors64 = vectors.astype(np.float64)
    squared_norms = np.zeros(vectors.shape[0], dtype=np.float64)
    for dimension in range(96):
        squared_norms += vectors64[:, dimension] * vectors64[:, dimension]
    norms = np.sqrt(squared_norms)
    if not np.isfinite(norms).all() or np.any(norms == 0.0):
        raise ValueError("V25 open vector values differ")
    return (vectors64 / norms[:, None]).astype(np.float32)


def _batch_vectors(column: pa.Array) -> np.ndarray:
    if (
        not isinstance(column, pa.FixedSizeListArray)
        or column.offset != 0
        or column.null_count != 0
        or column.values.null_count != 0
    ):
        raise ValueError("V25 open vector column differs")
    return column.values.to_numpy(zero_copy_only=False).reshape(-1, 96)


def _matches_historical_f16_projection(
    observed: np.ndarray, normalized_construction: np.ndarray
) -> bool:
    expected = normalized_construction.astype(np.float16)
    lower = np.nextafter(expected, np.float16(-math.inf)).astype(np.float32)
    upper = np.nextafter(expected, np.float16(math.inf)).astype(np.float32)
    expected32 = expected.astype(np.float32)
    representable = observed.astype(np.float16).astype(np.float32) == observed
    return bool(
        representable.all()
        and ((observed == expected32) | (observed == lower) | (observed == upper)).all()
    )


def _read_selected_construction(
    request: V25OpenBuildRequest, selection: V25OpenSelection
) -> tuple[np.ndarray, np.ndarray]:
    parquet = pq.ParquetFile(request.construction.path)
    if (
        parquet.schema_arrow != _construction_schema()
        or parquet.metadata.num_rows != request.source_row_count
    ):
        raise ValueError("V25 open construction schema differs")
    selected = np.asarray(selection.dataset_ordinals, dtype=np.uint64)
    sorted_order = np.argsort(selected)
    sorted_selected = selected[sorted_order]
    output = np.empty((request.cohort_count, 96), dtype=np.float32)
    found = np.zeros(request.cohort_count, dtype=np.bool_)
    expected_source = 0
    for batch in parquet.iter_batches(batch_size=65_536):
        source = batch.column(0).to_numpy(zero_copy_only=False)
        if (
            batch.num_columns != 2
            or any(column.null_count for column in batch.columns)
            or not np.array_equal(
                source,
                np.arange(expected_source, expected_source + batch.num_rows, dtype=np.uint64),
            )
        ):
            raise ValueError("V25 open construction inventory differs")
        vectors = _batch_vectors(batch.column(1))
        positions = np.searchsorted(sorted_selected, source)
        matched = (positions < sorted_selected.size) & (
            sorted_selected[np.minimum(positions, sorted_selected.size - 1)] == source
        )
        if np.any(matched):
            local = sorted_order[positions[matched]]
            if np.any(found[local]):
                raise ValueError("V25 open construction selection repeats")
            output[local] = vectors[matched]
            found[local] = True
        expected_source += batch.num_rows
    if expected_source != request.source_row_count or not found.all():
        raise ValueError("V25 open construction inventory differs")
    return output, _normalize_vectors(output)


def _read_selected_pages(
    request: V25OpenBuildRequest,
    selection: V25OpenSelection,
    construction_vectors: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    parquet = pq.ParquetFile(request.page_rows.path)
    if not parquet.schema_arrow.equals(
        _page_rows_schema(
            request.construction.sha256, request.construction.generation
        ),
        check_metadata=True,
    ):
        raise ValueError("V25 open page schema differs")
    selected = np.asarray(selection.dataset_ordinals, dtype=np.uint64)
    sorted_order = np.argsort(selected)
    sorted_selected = selected[sorted_order]
    primary = np.full(request.cohort_count, np.iinfo(np.uint32).max, dtype=np.uint32)
    replica = np.full(request.cohort_count, np.iinfo(np.uint32).max, dtype=np.uint32)
    primary_vectors = np.empty((request.cohort_count, 96), dtype=np.float32)
    replica_vectors = np.empty((request.cohort_count, 96), dtype=np.float32)
    all_primary = np.zeros(request.source_row_count, dtype=np.bool_)
    all_replica = np.zeros(request.source_row_count, dtype=np.bool_)
    for batch in parquet.iter_batches(batch_size=65_536):
        if batch.num_columns != 4 or any(column.null_count for column in batch.columns):
            raise ValueError("V25 open page batch differs")
        page = batch.column(0).to_numpy(zero_copy_only=False)
        is_replica = batch.column(1).to_numpy(zero_copy_only=False)
        try:
            dataset = pc.cast(batch.column(2), pa.uint64(), safe=True).to_numpy(
                zero_copy_only=False
            )
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as error:
            raise ValueError("V25 open page record ID differs") from error
        if not np.all(
            pc.equal(batch.column(2), pc.cast(pa.array(dataset), pa.string()))
            .to_numpy(zero_copy_only=False)
        ):
            raise ValueError("V25 open page record ID differs")
        vectors = _batch_vectors(batch.column(3))
        if (
            np.any(dataset >= request.source_row_count)
            or np.any(page >= request.page_count)
            or not np.isfinite(vectors).all()
            or np.any(np.all(vectors == 0.0, axis=1))
        ):
            raise ValueError("V25 open page values differ")
        for replica_flag, seen in ((False, all_primary), (True, all_replica)):
            ordinals = dataset[is_replica == replica_flag]
            if (
                np.unique(ordinals).size != ordinals.size
                or np.any(seen[ordinals.astype(np.intp)])
            ):
                raise ValueError("V25 open page relation repeats")
            seen[ordinals.astype(np.intp)] = True
        positions = np.searchsorted(sorted_selected, dataset)
        matched = (positions < sorted_selected.size) & (
            sorted_selected[np.minimum(positions, sorted_selected.size - 1)] == dataset
        )
        for row in np.flatnonzero(matched):
            local = int(sorted_order[positions[row]])
            target = replica if is_replica[row] else primary
            if target[local] != np.iinfo(np.uint32).max:
                raise ValueError("V25 open selected page relation repeats")
            target[local] = page[row]
            (replica_vectors if is_replica[row] else primary_vectors)[local] = vectors[row]
    if not all_primary.all() or np.any(all_replica & ~all_primary):
        raise ValueError("V25 open page inventory differs")
    if np.any(primary == np.iinfo(np.uint32).max) or np.any(primary == replica):
        raise ValueError("V25 open selected page relation differs")
    replicated = replica != np.iinfo(np.uint32).max
    if not np.array_equal(primary_vectors[replicated], replica_vectors[replicated]):
        raise ValueError("V25 open replica vector differs")
    if not _matches_historical_f16_projection(
        primary_vectors, construction_vectors
    ) or not _matches_historical_f16_projection(
        replica_vectors[replicated], construction_vectors[replicated]
    ):
        raise ValueError("V25 open page construction vector differs")
    return primary, replica


def _exact_oracle_pages(assignments: Iterable[tuple[int, ...]], budget: int) -> list[int]:
    masks: dict[int, int] = {}
    for neighbor, pages in enumerate(assignments):
        for page in pages:
            masks[page] = masks.get(page, 0) | (1 << neighbor)
    states: dict[tuple[int, int], tuple[int, ...]] = {(0, 0): ()}
    maximum = min(budget, len(masks))
    for page, mask in sorted(masks.items()):
        for (covered, count), pages in list(states.items()):
            if count == maximum:
                continue
            key = (covered | mask, count + 1)
            candidate = (*pages, page)
            if key not in states or candidate < states[key]:
                states[key] = candidate
    return list(
        max(
            states.items(),
            key=lambda item: (
                item[0][0].bit_count(),
                tuple(-page for page in item[1]),
            ),
        )[1]
    )


def _fixed_order_cosine_distances(
    vectors64: np.ndarray, query64: np.ndarray
) -> np.ndarray:
    if vectors64.ndim != 2 or vectors64.shape[1] != 96 or query64.shape != (96,):
        raise ValueError("V25 open exact truth vector shape differs")
    similarities = np.zeros(vectors64.shape[0], dtype=np.float64)
    for dimension in range(96):
        similarities += vectors64[:, dimension] * query64[dimension]
    return 1.0 - similarities


def _exact_truth(
    vectors: np.ndarray,
    primary: np.ndarray,
    replica: np.ndarray,
    query_sources: tuple[int, ...],
) -> tuple[list[list[int]], list[list[int]], list[list[int]], list[list[int]]]:
    neighbor_rows: list[list[int]] = []
    primary_rows: list[list[int]] = []
    replica_rows: list[list[int]] = []
    oracle_rows: list[list[int]] = []
    ordinals = np.arange(vectors.shape[0], dtype=np.uint64)
    vectors64 = vectors.astype(np.float64)
    maximum = np.iinfo(np.uint32).max
    for query_source in query_sources:
        own_pages = {int(primary[query_source])}
        if replica[query_source] != maximum:
            own_pages.add(int(replica[query_source]))
        distances = _fixed_order_cosine_distances(
            vectors64, vectors64[query_source]
        )
        own_page_array = np.fromiter(sorted(own_pages), dtype=np.uint32)
        forbidden = (
            (ordinals == query_source)
            | np.isin(primary, own_page_array)
            | np.isin(replica, own_page_array)
        )
        distances[forbidden] = math.inf
        order = np.lexsort((ordinals, distances))[:10]
        if order.size != 10 or not np.isfinite(distances[order]).all():
            raise ValueError("V25 open exact truth inventory differs")
        neighbors = [int(value) for value in order]
        primaries = [int(primary[value]) for value in order]
        replicas = [int(replica[value]) for value in order]
        assignments = [
            tuple(sorted({p, r})) if r != maximum else (p,)
            for p, r in zip(primaries, replicas, strict=True)
        ]
        neighbor_rows.append(neighbors)
        primary_rows.append(primaries)
        replica_rows.append(replicas)
        oracle_rows.append(_exact_oracle_pages(assignments, 8))
    return neighbor_rows, primary_rows, replica_rows, oracle_rows


def _write_parquet(table: pa.Table, path: pathlib.Path, row_group_size: int) -> None:
    pq.write_table(
        table,
        path,
        compression="zstd",
        version="2.6",
        row_group_size=row_group_size,
        use_dictionary=False,
        write_statistics=True,
    )


def _output_identity(
    role: str, path: pathlib.Path, uri_prefix: str, generation: str
) -> V25OpenOutputIdentity:
    encoded_bytes, digest = _sha256_file(path)
    return V25OpenOutputIdentity(
        role=role,
        uri=f"{uri_prefix}{path.name}",
        digest_algorithm="sha256",
        digest=digest,
        encoded_bytes=encoded_bytes,
        generation=generation,
    )


def build_v25_open_inputs(request: V25OpenBuildRequest) -> V25OpenBuildReceipt:
    """Convert authenticated immutable V24 tables into a clean V25 cohort."""

    if (
        request.construction.generation != request.page_rows.generation
        or not request.output_generation
        or not request.output_uri_prefix.startswith("s3://")
        or not request.output_uri_prefix.endswith("/")
        or "/../" in request.output_uri_prefix
        or request.page_count <= 0
        or request.output_row_group_size <= 0
        or request.output_dir.exists()
    ):
        raise ValueError("V25 open build authority differs")
    _authenticate_input(request.construction, "construction-rows-parquet")
    _authenticate_input(request.page_rows, "page-rows-parquet")
    selection = select_v25_open_rows(
        source_row_count=request.source_row_count,
        cohort_count=request.cohort_count,
        pseudoquery_count=request.pseudoquery_count,
        cohort_seed=request.cohort_seed,
        pseudoquery_seed=request.pseudoquery_seed,
    )
    _construction_vectors, vectors = _read_selected_construction(request, selection)
    primary, replica = _read_selected_pages(
        request, selection, vectors
    )
    query_sources = selection.query_source_ordinals
    truth = _exact_truth(vectors, primary, replica, query_sources)

    request.output_dir.mkdir()
    try:
        source_ordinals = np.arange(request.cohort_count, dtype=np.uint64)
        tables = {
            "construction-rows-parquet": pa.Table.from_arrays(
                [pa.array(source_ordinals), _fixed_vectors(vectors)],
                schema=_construction_schema(),
            ),
            "page-assignments-parquet": pa.table(
                {
                    "source_ordinal": pa.array(source_ordinals),
                    "primary_page": pa.array(primary, type=pa.uint32()),
                    "replica_page": pa.array(replica, type=pa.uint32()),
                },
                schema=pa.schema(
                    [
                        pa.field("source_ordinal", pa.uint64(), nullable=False),
                        pa.field("primary_page", pa.uint32(), nullable=False),
                        pa.field("replica_page", pa.uint32(), nullable=False),
                    ]
                ),
            ),
            "pseudoqueries-parquet": pa.Table.from_arrays(
                [
                    pa.array(range(request.pseudoquery_count), type=pa.uint32()),
                    pa.array(query_sources, type=pa.uint64()),
                    _fixed_vectors(vectors[list(query_sources)]),
                ],
                schema=pa.schema(
                    [
                        pa.field("query_ordinal", pa.uint32(), nullable=False),
                        pa.field("source_ordinal", pa.uint64(), nullable=False),
                        pa.field("vector", _vector_type(), nullable=False),
                    ]
                ),
            ),
            "source-map-parquet": pa.table(
                {
                    "source_ordinal": pa.array(source_ordinals),
                    "dataset_ordinal": pa.array(
                        selection.dataset_ordinals, type=pa.uint64()
                    ),
                },
                schema=pa.schema(
                    [
                        pa.field("source_ordinal", pa.uint64(), nullable=False),
                        pa.field("dataset_ordinal", pa.uint64(), nullable=False),
                    ]
                ),
            ),
        }
        neighbors, truth_primary, truth_replica, oracle = truth
        truth_schema = pa.schema(
            [
                pa.field("query_ordinal", pa.uint32(), nullable=False),
                pa.field(
                    "neighbor_source_ordinals",
                    pa.list_(pa.field("element", pa.uint64(), nullable=False), 10),
                    nullable=False,
                ),
                pa.field(
                    "primary_pages",
                    pa.list_(pa.field("element", pa.uint32(), nullable=False), 10),
                    nullable=False,
                ),
                pa.field(
                    "replica_pages",
                    pa.list_(pa.field("element", pa.uint32(), nullable=False), 10),
                    nullable=False,
                ),
                pa.field(
                    "oracle_pages",
                    pa.list_(pa.field("element", pa.uint32(), nullable=False), 8),
                    nullable=False,
                ),
            ]
        )
        def fixed(rows: list[list[int]], kind: pa.DataType, width: int) -> pa.Array:
            numpy_kind = np.uint64 if kind == pa.uint64() else np.uint32
            padded = [
                [*row, *([np.iinfo(numpy_kind).max] * (width - len(row)))]
                for row in rows
            ]
            if any(len(row) > width for row in rows):
                raise ValueError("V25 open fixed-list width differs")
            return pa.FixedSizeListArray.from_arrays(
                pa.array(np.asarray(padded, dtype=numpy_kind).reshape(-1), type=kind),
                type=pa.list_(pa.field("element", kind, nullable=False), width),
            )
        tables["truth-parquet"] = pa.Table.from_arrays(
            [
                pa.array(range(request.pseudoquery_count), type=pa.uint32()),
                fixed(neighbors, pa.uint64(), 10),
                fixed(truth_primary, pa.uint32(), 10),
                fixed(truth_replica, pa.uint32(), 10),
                fixed(oracle, pa.uint32(), 8),
            ],
            schema=truth_schema,
        )
        file_names = {
            "construction-rows-parquet": "construction-rows.parquet",
            "page-assignments-parquet": "page-assignments.parquet",
            "pseudoqueries-parquet": "pseudoqueries.parquet",
            "source-map-parquet": "source-map.parquet",
            "truth-parquet": "truth.parquet",
        }
        outputs: dict[str, V25OpenOutputIdentity] = {}
        for role, table in tables.items():
            path = request.output_dir / file_names[role]
            _write_parquet(table, path, request.output_row_group_size)
            outputs[role] = _output_identity(
                role, path, request.output_uri_prefix, request.output_generation
            )
        receipt_value = {
            "cohort_seed": request.cohort_seed,
            "inputs": [dataclasses.asdict(request.construction), dataclasses.asdict(request.page_rows)],
            "outputs": {role: dataclasses.asdict(value) for role, value in outputs.items()},
            "pseudoquery_seed": request.pseudoquery_seed,
            "schema": "borsuk-v25-open-conversion-receipt-v1",
            "selected_dataset_ordinals_sha256": selection.dataset_ordinals_sha256,
        }
        for value in receipt_value["inputs"]:
            value["path"] = pathlib.Path(value["path"]).name
        (request.output_dir / "conversion-receipt.json").write_bytes(
            json.dumps(receipt_value, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        return V25OpenBuildReceipt(selection=selection, outputs=outputs)
    except Exception:
        for path in request.output_dir.iterdir():
            path.unlink()
        request.output_dir.rmdir()
        raise


@dataclasses.dataclass(frozen=True)
class _Cli:
    manifest: pathlib.Path
    input_dir: pathlib.Path
    output_dir: pathlib.Path


def _parse_cli(arguments: list[str]) -> _Cli:
    values: dict[str, pathlib.Path] = {}
    convert = False
    execute = False
    index = 0
    while index < len(arguments):
        flag = arguments[index]
        if flag in {"--manifest", "--input-dir", "--output-dir"}:
            if flag in values or index + 1 >= len(arguments) or not arguments[index + 1]:
                raise ValueError("V25 open CLI value differs")
            values[flag] = pathlib.Path(arguments[index + 1])
            index += 2
        elif flag == "--convert-open-inputs":
            if convert:
                raise ValueError("V25 open CLI acknowledgement repeats")
            convert = True
            index += 1
        elif flag == "--execute":
            if execute:
                raise ValueError("V25 open CLI acknowledgement repeats")
            execute = True
            index += 1
        else:
            raise ValueError(f"V25 open CLI flag is unknown: {flag}")
    if set(values) != {"--manifest", "--input-dir", "--output-dir"}:
        raise ValueError("V25 open CLI required path differs")
    if not convert or not execute:
        raise ValueError("V25 open CLI execution acknowledgement differs")
    return _Cli(
        manifest=values["--manifest"],
        input_dir=values["--input-dir"],
        output_dir=values["--output-dir"],
    )


def _manifest_input(
    value: object, role: str, input_dir: pathlib.Path
) -> RegisteredV24Input:
    if not isinstance(value, dict) or set(value) != {
        "encoded_bytes",
        "file_name",
        "generation",
        "role",
        "sha256",
        "uri",
    }:
        raise ValueError("V25 open CLI input manifest differs")
    file_name = value["file_name"]
    path = pathlib.Path(file_name) if type(file_name) is str else pathlib.Path(".")
    if path.name != file_name or file_name in {"", ".", ".."}:
        raise ValueError("V25 open CLI input file differs")
    target = input_dir / path
    target.lstat()
    if not target.is_file() or target.is_symlink():
        raise ValueError("V25 open CLI input type differs")
    registered = RegisteredV24Input(
        role=value["role"],
        path=target,
        uri=value["uri"],
        sha256=value["sha256"],
        encoded_bytes=value["encoded_bytes"],
        generation=value["generation"],
    )
    if (
        type(registered.role) is not str
        or type(registered.uri) is not str
        or type(registered.sha256) is not str
        or type(registered.encoded_bytes) is not int
        or type(registered.generation) is not str
        or registered.role != role
    ):
        raise ValueError("V25 open CLI input authority differs")
    return registered


def _run_cli(arguments: list[str]) -> bytes:
    cli = _parse_cli(arguments)
    input_dir = cli.input_dir.resolve(strict=True)
    if not input_dir.is_dir():
        raise ValueError("V25 open CLI input directory differs")
    manifest_path = cli.manifest.resolve(strict=True)
    if (
        manifest_path.parent != input_dir
        or manifest_path.is_symlink()
        or not manifest_path.is_file()
    ):
        raise ValueError("V25 open CLI manifest path differs")
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    if (
        not isinstance(manifest, dict)
        or set(manifest)
        != {
            "cohort_count",
            "cohort_seed",
            "construction",
            "output_generation",
            "output_row_group_size",
            "output_uri_prefix",
            "page_count",
            "page_rows",
            "pseudoquery_count",
            "pseudoquery_seed",
            "schema",
            "source_row_count",
        }
        or manifest_bytes
        != json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        or manifest["schema"] != "borsuk-v25-open-build-manifest-v1"
    ):
        raise ValueError("V25 open CLI manifest differs")
    numeric = (
        "cohort_count",
        "cohort_seed",
        "output_row_group_size",
        "page_count",
        "pseudoquery_count",
        "pseudoquery_seed",
        "source_row_count",
    )
    if any(type(manifest[field]) is not int for field in numeric) or any(
        type(manifest[field]) is not str
        for field in ("output_generation", "output_uri_prefix")
    ):
        raise ValueError("V25 open CLI manifest types differ")
    construction = _manifest_input(
        manifest["construction"], "construction-rows-parquet", input_dir
    )
    page_rows = _manifest_input(manifest["page_rows"], "page-rows-parquet", input_dir)
    expected_names = {
        manifest_path.name,
        construction.path.name,
        page_rows.path.name,
    }
    if {path.name for path in input_dir.iterdir()} != expected_names:
        raise ValueError("V25 open CLI input inventory differs")
    build_v25_open_inputs(
        V25OpenBuildRequest(
            construction=construction,
            page_rows=page_rows,
            output_dir=cli.output_dir,
            output_uri_prefix=manifest["output_uri_prefix"],
            output_generation=manifest["output_generation"],
            source_row_count=manifest["source_row_count"],
            cohort_count=manifest["cohort_count"],
            pseudoquery_count=manifest["pseudoquery_count"],
            page_count=manifest["page_count"],
            cohort_seed=manifest["cohort_seed"],
            pseudoquery_seed=manifest["pseudoquery_seed"],
            output_row_group_size=manifest["output_row_group_size"],
        )
    )
    return (cli.output_dir / "conversion-receipt.json").read_bytes()


def _main() -> int:
    try:
        output = _run_cli(sys.argv[1:])
        sys.stdout.buffer.write(output)
    except (json.JSONDecodeError, OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
