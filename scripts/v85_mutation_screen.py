#!/usr/bin/env python3
"""Exercise 500 replacements and 500 tombstones through real V85 artifacts."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import pathlib
from collections import defaultdict
from typing import Any

import numpy as np
import pyarrow as pa

from scripts.v85_build_delta import (
    BuildRequest,
    CompactionRequest,
    _canonical_bytes,
    _emit_run,
    _identity,
    _load_mutation_rows,
    _read_page_stream,
    _write_ipc_file,
    build_delta_artifacts,
    compact_delta_artifacts,
)
from scripts.v85_qualification import validate_mutation_screen


@dataclasses.dataclass(frozen=True)
class MutationScreenRequest:
    """Bounded construction capability for the mutation correctness screen."""

    source: pathlib.Path
    work: pathlib.Path
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


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _code_sha256(code: np.ndarray) -> str:
    return _sha256(np.ascontiguousarray(code, dtype=np.uint8).tobytes())


def _artifact_rows(
    root: pathlib.Path,
    manifest: dict[str, Any],
    kinds: set[str],
) -> dict[int, dict[str, Any]]:
    rows: dict[int, dict[str, Any]] = {}
    dimensions = manifest["dimensions"]
    for run in manifest["runs"]:
        if run["kind"] not in kinds:
            continue
        path = root / pathlib.PurePosixPath(run["object"]["uri"]).name
        body = path.read_bytes()
        if (
            len(body) != run["object"]["bytes"]
            or _sha256(body) != run["object"]["sha256"]
        ):
            raise ValueError("mutation screen run identity differs")
        run_row = 0
        for page in run["pages"]:
            start = page["offset"]
            stop = start + page["bytes"]
            table = _read_page_stream(body[start:stop], dimensions)
            if table.num_rows != page["rows"]:
                raise ValueError("mutation screen page rows differ")
            codes = np.asarray(
                table.column("code").combine_chunks().values.to_numpy(),
                dtype=np.uint8,
            ).reshape(-1, dimensions)
            identifiers = table.column("id").to_pylist()
            sequences = table.column("sequence").to_pylist()
            states = table.column("state").to_pylist()
            for index in range(table.num_rows):
                identifier = int(identifiers[index])
                if identifier in rows:
                    raise ValueError("mutation screen duplicate physical row")
                rows[identifier] = {
                    "code": codes[index].copy(),
                    "page": int(page["page"]),
                    "row": run_row + index,
                    "run_id": int(run["run_id"]),
                    "sequence": int(sequences[index]),
                    "state": int(states[index]),
                }
            run_row += table.num_rows
    return rows


def _mutation_table(
    rows: dict[int, tuple[int, int, int | None, int | None]],
) -> pa.Table:
    identifiers = sorted(rows)
    return pa.Table.from_arrays(
        [
            pa.array(identifiers, type=pa.int64()),
            pa.array([rows[row][0] for row in identifiers], type=pa.uint64()),
            pa.array([rows[row][1] for row in identifiers], type=pa.uint8()),
            pa.array([rows[row][2] for row in identifiers], type=pa.uint32()),
            pa.array([rows[row][3] for row in identifiers], type=pa.uint32()),
        ],
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("sequence", pa.uint64(), nullable=False),
                pa.field("state", pa.uint8(), nullable=False),
                pa.field("run_id", pa.uint32(), nullable=True),
                pa.field("row", pa.uint32(), nullable=True),
            ]
        ),
    )


def run_mutation_screen(request: MutationScreenRequest) -> dict[str, Any]:
    """Build, mutate, compact, and validate one exact 1,000-operation trace."""

    if (
        not request.source.is_file()
        or request.work.exists()
        or request.output.exists()
        or not request.uri_prefix.startswith("s3://")
        or request.base_rows < 1_000
        or request.delta_rows <= 0
    ):
        raise ValueError("mutation screen request differs")
    built = request.work / "built"
    built_uri = f"{request.uri_prefix.rstrip('/')}/built"
    build_delta_artifacts(
        BuildRequest(
            source=request.source,
            output=built,
            uri_prefix=built_uri,
            base_rows=request.base_rows,
            delta_rows=request.delta_rows,
            dimensions=request.dimensions,
            page_rows=request.page_rows,
            router_cells=request.router_cells,
            base_runs=request.base_runs,
            delta_runs=request.delta_runs,
            seed=request.seed,
        )
    )
    generation_path = built / "generation.json"
    before_body = generation_path.read_bytes()
    manifest = json.loads(before_body)
    base_rows = _artifact_rows(built, manifest, {"base"})
    mutation_ids = sorted(base_rows)[:1_000]
    if len(mutation_ids) != 1_000:
        raise ValueError("mutation screen base evidence differs")
    replacement_ids = mutation_ids[:500]
    tombstone_ids = mutation_ids[500:]

    replacement_codes = np.stack([base_rows[row]["code"] for row in replacement_ids])
    replacement_codes[:, 0] = replacement_codes[:, 0] + np.uint8(1)
    replacement_id_array = np.asarray(replacement_ids, dtype=np.int64)
    replacement_pages: dict[int, list[int]] = defaultdict(list)
    for index, row_id in enumerate(replacement_ids):
        replacement_pages[base_rows[row_id]["page"]].append(index)
    replacement_run_id = max(run["run_id"] for run in manifest["runs"]) + 1
    replacement_run, replacement_locations = _emit_run(
        built,
        built_uri,
        "delta-replacements.arrow",
        replacement_run_id,
        manifest["generation"] + 1,
        "delta",
        dict(replacement_pages),
        replacement_id_array,
        replacement_codes,
        2,
    )

    old_mutation_path = built / pathlib.PurePosixPath(
        manifest["mutation_directory"]["uri"]
    ).name
    mutations = _load_mutation_rows(old_mutation_path.read_bytes())
    for row_id in replacement_ids:
        run_id, row = replacement_locations[row_id]
        mutations[row_id] = (2, 0, run_id, row)
    for row_id in tombstone_ids:
        mutations[row_id] = (2, 1, None, None)
    mutation_path = built / "mutations-g2.arrow"
    mutation_body = _write_ipc_file(mutation_path, _mutation_table(mutations))

    manifest["generation"] += 1
    manifest["previous_generation_sha256"] = _sha256(before_body)
    manifest["mutation_directory"] = _identity(
        f"{built_uri}/mutations-g2.arrow", mutation_body
    )
    manifest["runs"].append(replacement_run)
    after_path = built / "generation-g2.json"
    after_body = _canonical_bytes(manifest)
    after_path.write_bytes(after_body)

    compacted = request.work / "compacted"
    compact_delta_artifacts(
        CompactionRequest(
            generation=after_path,
            output=compacted,
            uri_prefix=f"{request.uri_prefix.rstrip('/')}/compacted",
            delta_run_ids=tuple(
                run["run_id"] for run in manifest["runs"] if run["kind"] == "delta"
            ),
        )
    )
    compacted_body = (compacted / "generation.json").read_bytes()
    compacted_manifest = json.loads(compacted_body)
    compacted_rows = _artifact_rows(compacted, compacted_manifest, {"delta"})
    directory = _load_mutation_rows(mutation_body)

    cases = []
    for row_id in mutation_ids:
        old = base_rows[row_id]
        sequence, state, run_id, row = directory[row_id]
        old_witness = {
            "code_sha256": _code_sha256(old["code"]),
            "row": old["row"],
            "run_id": old["run_id"],
            "sequence": old["sequence"],
            "state": "live",
        }
        if row_id in replacement_locations:
            replacement_index = replacement_ids.index(row_id)
            replacement_code = replacement_codes[replacement_index]
            newest_witness = {
                "code_sha256": _code_sha256(replacement_code),
                "row": row,
                "run_id": run_id,
                "sequence": 2,
                "state": "live",
            }
            survivor = compacted_rows.get(row_id)
            if survivor is None:
                raise ValueError("mutation screen replacement was lost")
            compacted_sequence: int | None = survivor["sequence"]
            compacted_code_sha256: str | None = _code_sha256(survivor["code"])
            kind = "replacement"
            physical_rows = [old_witness, newest_witness]
            latest_state = "live"
        else:
            if row_id in compacted_rows:
                raise ValueError("mutation screen tombstone was resurrected")
            compacted_sequence = None
            compacted_code_sha256 = None
            kind = "tombstone"
            physical_rows = [old_witness]
            latest_state = "tombstone"
        cases.append(
            {
                "compacted_code_sha256": compacted_code_sha256,
                "compacted_sequence": compacted_sequence,
                "directory": {
                    "row": row,
                    "run_id": run_id,
                    "sequence": sequence,
                    "state": latest_state,
                },
                "id": row_id,
                "kind": kind,
                "physical_rows": physical_rows,
                "writes": [
                    {"sequence": 1, "state": "live"},
                    {"sequence": 2, "state": latest_state},
                ],
            }
        )

    receipt = {
        "after_generation_sha256": _sha256(after_body),
        "before_generation_sha256": _sha256(before_body),
        "cases": cases,
        "claim_eligible": False,
        "compacted_generation_sha256": _sha256(compacted_body),
        "evidence_kind": "semantic-local-artifact-screen",
        "schema": "borsuk-v85-mutation-screen-v1",
    }
    validate_mutation_screen(receipt)
    request.output.write_bytes(_canonical_bytes(receipt))
    return receipt
