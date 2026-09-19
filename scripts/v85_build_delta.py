#!/usr/bin/env python3
"""Build deterministic query-visible V85 base and delta Arrow artifacts."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import pathlib
from collections import defaultdict
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

SCHEMA = "borsuk-v85-generation-v1"
RECEIPT_SCHEMA = "borsuk-v85-build-receipt-v1"


@dataclasses.dataclass(frozen=True)
class BuildRequest:
    """Construction capability; deliberately contains no queries or ground truth."""

    source: pathlib.Path
    output: pathlib.Path
    uri_prefix: str
    base_rows: int
    delta_rows: int
    dimensions: int
    page_rows: int
    router_cells: int
    base_runs: int
    delta_runs: int
    seed: int


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _identity(uri: str, body: bytes) -> dict[str, Any]:
    return {"bytes": len(body), "sha256": _sha256(body), "uri": uri}


def _vector_type(dimensions: int) -> pa.DataType:
    return pa.list_(pa.field("element", pa.float32(), nullable=False), dimensions)


def _read_source(request: BuildRequest) -> tuple[np.ndarray, np.ndarray, str]:
    table = pq.read_table(request.source)
    expected = pa.schema(
        [
            pa.field("embedding", _vector_type(request.dimensions), nullable=False),
        ]
    )
    if table.schema != expected:
        raise ValueError("source Parquet schema differs")
    if table.num_rows != request.base_rows + request.delta_rows:
        raise ValueError("source row count differs")
    ids = np.arange(table.num_rows, dtype=np.int64)
    vectors = np.asarray(
        table.column("embedding").combine_chunks().values.to_numpy(), dtype=np.float32
    ).reshape(-1, request.dimensions)
    if not np.isfinite(vectors).all():
        raise ValueError("source vector is non-finite")
    if len(set(int(value) for value in ids)) != len(ids):
        raise ValueError("source IDs are not unique")
    return ids, vectors, hashlib.sha256(request.source.read_bytes()).hexdigest()


def canonicalize_evaluation(
    query_source: pathlib.Path,
    truth_source: pathlib.Path,
    query_output: pathlib.Path,
    truth_output: pathlib.Path,
    *,
    dimensions: int,
    neighbors: int,
    query_limit: int,
) -> None:
    """Convert frozen ReLAION evaluation inputs to the strict reader schemas."""

    if dimensions <= 0 or neighbors <= 0 or query_limit <= 0:
        raise ValueError("evaluation shape differs")
    query_table = pq.read_table(query_source)
    expected_query = pa.schema(
        [pa.field("embedding", _vector_type(dimensions), nullable=False)]
    )
    if query_table.schema != expected_query or query_table.num_rows < query_limit:
        raise ValueError("query source authority differs")
    query_values = np.asarray(
        query_table.column("embedding").combine_chunks().values.to_numpy(),
        dtype=np.float32,
    ).reshape(-1, dimensions)[:query_limit]
    if not np.isfinite(query_values).all():
        raise ValueError("query source is non-finite")
    canonical_queries = pa.Table.from_arrays(
        [
            pa.array(np.arange(query_limit, dtype=np.uint32), type=pa.uint32()),
            pa.FixedSizeListArray.from_arrays(
                pa.array(query_values.reshape(-1), type=pa.float32()), dimensions
            ),
        ],
        schema=pa.schema(
            [
                pa.field("query", pa.uint32(), nullable=False),
                pa.field("vector", _vector_type(dimensions), nullable=False),
            ]
        ),
    )

    truth_table = pq.read_table(truth_source, columns=["feature_row_id"])
    truth_column = truth_table.column("feature_row_id").combine_chunks()
    wanted = query_limit * neighbors
    if (
        truth_column.type != pa.uint64()
        or truth_column.null_count != 0
        or len(truth_column) < wanted
    ):
        raise ValueError("truth source authority differs")
    truth_values = np.asarray(truth_column.to_numpy(), dtype=np.uint64)[:wanted]
    if np.any(truth_values > np.iinfo(np.int64).max):
        raise ValueError("truth ID is not representable")
    canonical_truth = pa.Table.from_arrays(
        [
            pa.array(np.arange(query_limit, dtype=np.uint32), type=pa.uint32()),
            pa.FixedSizeListArray.from_arrays(
                pa.array(truth_values.astype(np.int64, copy=False), type=pa.int64()),
                neighbors,
            ),
        ],
        schema=pa.schema(
            [
                pa.field("query", pa.uint32(), nullable=False),
                pa.field(
                    "neighbors",
                    pa.list_(pa.field("element", pa.int64(), nullable=False), neighbors),
                    nullable=False,
                ),
            ]
        ),
    )
    pq.write_table(canonical_queries, query_output)
    pq.write_table(canonical_truth, truth_output)


def _fit_router(base: np.ndarray, cells: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    if cells <= 0 or cells > len(base):
        raise ValueError("router cell count differs")
    generator = np.random.default_rng(seed)
    initial = np.sort(generator.choice(len(base), size=cells, replace=False))
    centroids = np.array(base[initial], dtype=np.float32, copy=True)
    assignments = np.zeros(len(base), dtype=np.int64)
    for _ in range(6):
        assignments = _assign_cells(base, centroids)
        for cell in range(cells):
            members = base[assignments == cell]
            if len(members):
                centroids[cell] = members.mean(axis=0, dtype=np.float64).astype(np.float32)
    return centroids, assignments


def _assign_cells(vectors: np.ndarray, centroids: np.ndarray) -> np.ndarray:
    assignments = np.empty(len(vectors), dtype=np.int64)
    centroid_norms = np.einsum("ij,ij->i", centroids, centroids, optimize=True)
    for start in range(0, len(vectors), 8_192):
        stop = min(start + 8_192, len(vectors))
        block = vectors[start:stop]
        distances = (
            np.einsum("ij,ij->i", block, block, optimize=True)[:, None]
            + centroid_norms[None, :]
            - np.float32(2.0) * (block @ centroids.T)
        )
        assignments[start:stop] = np.argmin(distances, axis=1)
    return assignments


def _page_schema(dimensions: int) -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("sequence", pa.uint64(), nullable=False),
            pa.field("state", pa.uint8(), nullable=False),
            pa.field("vector", _vector_type(dimensions), nullable=False),
        ]
    )


def _page_stream(ids: np.ndarray, sequence: int, vectors: np.ndarray) -> bytes:
    dimensions = vectors.shape[1]
    schema = _page_schema(dimensions)
    vector_array = pa.FixedSizeListArray.from_arrays(
        pa.array(np.ascontiguousarray(vectors).reshape(-1), type=pa.float32()), dimensions
    )
    table = pa.Table.from_arrays(
        [
            pa.array(ids, type=pa.int64()),
            pa.array(np.full(len(ids), sequence, dtype=np.uint64), type=pa.uint64()),
            pa.array(np.zeros(len(ids), dtype=np.uint8), type=pa.uint8()),
            vector_array,
        ],
        schema=schema,
    )
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _write_ipc_file(path: pathlib.Path, table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    body = sink.getvalue().to_pybytes()
    path.write_bytes(body)
    return body


def _emit_run(
    output: pathlib.Path,
    uri_prefix: str,
    name: str,
    run_id: int,
    generation: int,
    kind: str,
    pages: dict[int, list[int]],
    ids: np.ndarray,
    vectors: np.ndarray,
    sequence: int,
) -> tuple[dict[str, Any], dict[int, tuple[int, int]]]:
    body = bytearray()
    page_authorities = []
    row_locations: dict[int, tuple[int, int]] = {}
    next_row = 0
    for page in sorted(pages):
        indices = np.asarray(sorted(pages[page], key=lambda index: int(ids[index])), dtype=np.int64)
        stream = _page_stream(ids[indices], sequence, vectors[indices])
        offset = len(body)
        body.extend(stream)
        page_authorities.append(
            {"bytes": len(stream), "offset": offset, "page": page, "rows": len(indices)}
        )
        for local_index, source_index in enumerate(indices):
            row_locations[int(ids[source_index])] = (run_id, next_row + local_index)
        next_row += len(indices)
    encoded = bytes(body)
    (output / name).write_bytes(encoded)
    uri = f"{uri_prefix.rstrip('/')}/{name}"
    return (
        {
            "generation": generation,
            "kind": kind,
            "object": _identity(uri, encoded),
            "pages": page_authorities,
            "run_id": run_id,
        },
        row_locations,
    )


def _validate_request(request: BuildRequest) -> None:
    if (
        request.base_rows <= 0
        or request.delta_rows <= 0
        or request.dimensions <= 0
        or request.page_rows <= 0
        or request.router_cells <= 0
        or request.base_runs <= 0
        or request.delta_runs <= 0
        or request.delta_runs > request.delta_rows
        or not request.uri_prefix.startswith("s3://")
    ):
        raise ValueError("build request differs")
    if request.output.exists() and any(request.output.iterdir()):
        raise ValueError("output directory is not empty")


def build_delta_artifacts(request: BuildRequest) -> dict[str, Any]:
    """Build one deterministic immutable generation and return its receipt."""

    _validate_request(request)
    request.output.mkdir(parents=True, exist_ok=True)
    ids, vectors, source_sha256 = _read_source(request)
    base_vectors = np.ascontiguousarray(vectors[: request.base_rows])
    training_sha256 = _sha256(base_vectors.tobytes())
    centroids, base_cells = _fit_router(base_vectors, request.router_cells, request.seed)

    cell_pages: list[list[int]] = []
    base_page_members: dict[int, list[int]] = {}
    next_page = 0
    for cell in range(request.router_cells):
        members = np.flatnonzero(base_cells == cell)
        ordered = sorted((int(index) for index in members), key=lambda index: int(ids[index]))
        pages = []
        for start in range(0, len(ordered), request.page_rows):
            page = next_page
            next_page += 1
            pages.append(page)
            base_page_members[page] = ordered[start : start + request.page_rows]
        if not pages:
            raise ValueError("router produced an empty cell")
        cell_pages.append(pages)

    router_schema = pa.schema(
        [
            pa.field("cell", pa.uint32(), nullable=False),
            pa.field("centroid", _vector_type(request.dimensions), nullable=False),
            pa.field("first_page", pa.uint32(), nullable=False),
            pa.field("page_count", pa.uint32(), nullable=False),
        ]
    )
    router_table = pa.Table.from_arrays(
        [
            pa.array(np.arange(request.router_cells, dtype=np.uint32)),
            pa.FixedSizeListArray.from_arrays(
                pa.array(centroids.reshape(-1), type=pa.float32()), request.dimensions
            ),
            pa.array([pages[0] for pages in cell_pages], type=pa.uint32()),
            pa.array([len(pages) for pages in cell_pages], type=pa.uint32()),
        ],
        schema=router_schema,
    )
    router_body = _write_ipc_file(request.output / "router.arrow", router_table)

    runs = []
    for run_index in range(request.base_runs):
        pages = {
            page: members
            for page, members in base_page_members.items()
            if page % request.base_runs == run_index
        }
        if not pages:
            continue
        run, _ = _emit_run(
            request.output,
            request.uri_prefix,
            f"base-{run_index:03d}.arrow",
            run_index,
            0,
            "base",
            pages,
            ids,
            vectors,
            1,
        )
        runs.append(run)

    delta_vectors = vectors[request.base_rows :]
    delta_cells = _assign_cells(delta_vectors, centroids)
    delta_source_indices = np.arange(request.base_rows, len(ids), dtype=np.int64)
    run_splits = np.array_split(delta_source_indices, request.delta_runs)
    mutation_locations: dict[int, tuple[int, int]] = {}
    for delta_index, source_indices in enumerate(run_splits):
        run_id = request.base_runs + delta_index
        pages: dict[int, list[int]] = defaultdict(list)
        for source_index in source_indices:
            relative = int(source_index) - request.base_rows
            cell = int(delta_cells[relative])
            page_choices = cell_pages[cell]
            page = page_choices[int(ids[source_index]) % len(page_choices)]
            pages[page].append(int(source_index))
        run, locations = _emit_run(
            request.output,
            request.uri_prefix,
            f"delta-{delta_index:03d}.arrow",
            run_id,
            1,
            "delta",
            dict(pages),
            ids,
            vectors,
            2,
        )
        runs.append(run)
        mutation_locations.update(locations)

    mutation_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("sequence", pa.uint64(), nullable=False),
            pa.field("state", pa.uint8(), nullable=False),
            pa.field("run_id", pa.uint32(), nullable=True),
            pa.field("row", pa.uint32(), nullable=True),
        ]
    )
    mutation_ids = sorted(mutation_locations)
    mutation_table = pa.Table.from_arrays(
        [
            pa.array(mutation_ids, type=pa.int64()),
            pa.array([2] * len(mutation_ids), type=pa.uint64()),
            pa.array([0] * len(mutation_ids), type=pa.uint8()),
            pa.array([mutation_locations[row_id][0] for row_id in mutation_ids], type=pa.uint32()),
            pa.array([mutation_locations[row_id][1] for row_id in mutation_ids], type=pa.uint32()),
        ],
        schema=mutation_schema,
    )
    mutation_body = _write_ipc_file(request.output / "mutations.arrow", mutation_table)

    manifest = {
        "base_horizon": request.base_rows,
        "dimensions": request.dimensions,
        "generation": 1,
        "mutation_directory": _identity(
            f"{request.uri_prefix.rstrip('/')}/mutations.arrow", mutation_body
        ),
        "neighbors": 100,
        "page_rows": request.page_rows,
        "previous_generation_sha256": "0" * 64,
        "router": _identity(f"{request.uri_prefix.rstrip('/')}/router.arrow", router_body),
        "runs": runs,
        "schema": SCHEMA,
        "source_split": f"relaion-{request.base_rows + request.delta_rows}-base{request.base_rows}-delta{request.delta_rows}",
    }
    manifest_body = _canonical_bytes(manifest)
    (request.output / "generation.json").write_bytes(manifest_body)

    receipt = {
        "artifacts": [
            _identity(f"{request.uri_prefix.rstrip('/')}/{path.name}", path.read_bytes())
            for path in sorted(request.output.iterdir())
            if path.is_file()
        ],
        "base_rows": request.base_rows,
        "delta_rows": request.delta_rows,
        "generation_sha256": _sha256(manifest_body),
        "schema": RECEIPT_SCHEMA,
        "source_sha256": source_sha256,
        "training_rows": request.base_rows,
        "training_sha256": training_sha256,
    }
    (request.output / "receipt.json").write_bytes(_canonical_bytes(receipt))
    return receipt


def _parse_args() -> BuildRequest:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--uri-prefix", required=True)
    parser.add_argument("--base-rows", type=int, required=True)
    parser.add_argument("--delta-rows", type=int, required=True)
    parser.add_argument("--dimensions", type=int, required=True)
    parser.add_argument("--page-rows", type=int, required=True)
    parser.add_argument("--router-cells", type=int, required=True)
    parser.add_argument("--base-runs", type=int, required=True)
    parser.add_argument("--delta-runs", type=int, required=True)
    parser.add_argument("--seed", type=int, default=85)
    return BuildRequest(**vars(parser.parse_args()))


if __name__ == "__main__":
    print(json.dumps(build_delta_artifacts(_parse_args()), separators=(",", ":"), sort_keys=True))
