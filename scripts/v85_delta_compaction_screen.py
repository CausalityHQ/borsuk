#!/usr/bin/env python3
"""Run the fixed 100k V85 fragmentation and compaction semantic screen."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess
from dataclasses import dataclass
from typing import Any

from scripts.v85_qualification import validate_delta_compaction_screen

ROWS = 100_000
BASE_ROWS = 90_000
DELTA_ROWS = 10_000
QUERY_COUNT = 32
PAGE_BUDGET = 512
RUN_COUNTS = (1, 10, 100)


@dataclass(frozen=True)
class ScreenRequest:
    source: pathlib.Path
    queries: pathlib.Path
    truth: pathlib.Path
    binary: pathlib.Path
    work: pathlib.Path
    output: pathlib.Path
    uri_prefix: str


def _sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _identity_arguments(
    role: str, path: pathlib.Path, uri: str
) -> list[str]:
    return [
        f"--{role}",
        str(path),
        uri,
        _sha256(path),
        str(path.stat().st_size),
    ]


def _path_for_uri(
    identity: dict[str, Any], roots: tuple[pathlib.Path, ...]
) -> pathlib.Path:
    if (
        not isinstance(identity, dict)
        or set(identity) != {"bytes", "sha256", "uri"}
        or type(identity["bytes"]) is not int
        or identity["bytes"] <= 0
        or not isinstance(identity["sha256"], str)
        or len(identity["sha256"]) != 64
        or not isinstance(identity["uri"], str)
    ):
        raise ValueError("artifact identity differs")
    name = pathlib.PurePosixPath(identity["uri"]).name
    matches = [root / name for root in roots if (root / name).is_file()]
    matching = [
        path
        for path in matches
        if path.stat().st_size == identity["bytes"]
        and _sha256(path) == identity["sha256"]
    ]
    if len(matching) != 1:
        raise ValueError(f"artifact path for {identity['uri']} differs")
    return matching[0]


def _truth_rows(path: pathlib.Path) -> list[list[int]]:
    import pyarrow.parquet as pq

    table = pq.read_table(path)
    if table.num_rows != QUERY_COUNT or table.column_names != ["query", "neighbors"]:
        raise ValueError("semantic screen truth authority differs")
    ordinals = table.column("query").to_pylist()
    if ordinals != list(range(QUERY_COUNT)):
        raise ValueError("semantic screen truth ordinals differ")
    rows = table.column("neighbors").to_pylist()
    if any(len(row) != 100 or len(set(row)) != 100 for row in rows):
        raise ValueError("semantic screen truth rows differ")
    return rows


def _zip_equal_lengths(left: list[Any], right: list[Any]) -> Any:
    if len(left) != len(right):
        raise ValueError("semantic screen reader sample count differs")
    return ((left[index], right[index]) for index in range(len(left)))


def _run_reader(
    request: ScreenRequest,
    generation_path: pathlib.Path,
    artifact_roots: tuple[pathlib.Path, ...],
    truth_rows: list[list[int]],
) -> dict[str, Any]:
    manifest = json.loads(generation_path.read_bytes())
    generation_uri = f"{request.uri_prefix.rstrip('/')}/{generation_path.parent.name}/generation.json"
    arguments = [str(request.binary)]
    arguments += _identity_arguments(
        "generation", generation_path, generation_uri
    )
    for role in ("router", "mutation_directory"):
        identity = manifest[role]
        path = _path_for_uri(identity, artifact_roots)
        cli_role = "mutations" if role == "mutation_directory" else role
        arguments += _identity_arguments(cli_role, path, identity["uri"])
    for run in manifest["runs"]:
        identity = run["object"]
        path = _path_for_uri(identity, artifact_roots)
        arguments += _identity_arguments("run", path, identity["uri"])
    arguments += _identity_arguments(
        "queries", request.queries, f"{request.uri_prefix.rstrip('/')}/queries.parquet"
    )
    arguments += _identity_arguments(
        "truth", request.truth, f"{request.uri_prefix.rstrip('/')}/truth.parquet"
    )
    arguments += ["--page-budget", str(PAGE_BUDGET)]
    completed = subprocess.run(
        arguments,
        check=True,
        capture_output=True,
        text=False,
        timeout=600,
    )
    result = json.loads(completed.stdout)
    samples = result.get("samples")
    if not isinstance(samples, list) or len(samples) != len(truth_rows):
        raise ValueError("semantic screen reader sample count differs")
    for sample, truth_ids in _zip_equal_lengths(samples, truth_rows):
        sample["truth_ids"] = truth_ids
    return result


def _write_and_validate_receipt(
    receipt: dict[str, Any], output: pathlib.Path
) -> None:
    output.write_text(
        json.dumps(receipt, separators=(",", ":"), sort_keys=True) + "\n"
    )
    validate_delta_compaction_screen(receipt)


def run_screen(request: ScreenRequest) -> dict[str, Any]:
    from scripts.v85_build_delta import (
        BuildRequest,
        CompactionRequest,
        build_delta_artifacts,
        compact_delta_artifacts,
    )

    if (
        not request.source.is_file()
        or not request.queries.is_file()
        or not request.truth.is_file()
        or not request.binary.is_file()
        or not request.uri_prefix.startswith("s3://")
        or request.work.exists()
        or request.output.exists()
    ):
        raise ValueError("semantic screen request differs")
    request.work.mkdir(parents=True)
    truth_rows = _truth_rows(request.truth)
    results = []
    roots: dict[int, pathlib.Path] = {}
    for runs in RUN_COUNTS:
        root = request.work / f"runs-{runs:03d}"
        build_delta_artifacts(
            BuildRequest(
                source=request.source,
                output=root,
                uri_prefix=f"{request.uri_prefix.rstrip('/')}/runs-{runs:03d}",
                base_rows=BASE_ROWS,
                delta_rows=DELTA_ROWS,
                dimensions=768,
                page_rows=256,
                router_cells=256,
                base_runs=1,
                delta_runs=runs,
                seed=85,
            )
        )
        roots[runs] = root
        result = _run_reader(
            request,
            root / "generation.json",
            (root,),
            truth_rows,
        )
        results.append({"result": result, "runs": runs})

    hundred = roots[100]
    manifest = json.loads((hundred / "generation.json").read_bytes())
    run_ids = tuple(
        run["run_id"] for run in manifest["runs"] if run["kind"] == "delta"
    )
    compacted_root = request.work / "compacted"
    compaction = compact_delta_artifacts(
        CompactionRequest(
            generation=hundred / "generation.json",
            output=compacted_root,
            uri_prefix=f"{request.uri_prefix.rstrip('/')}/compacted",
            delta_run_ids=run_ids,
        )
    )
    compacted_result = _run_reader(
        request,
        compacted_root / "generation.json",
        (hundred, compacted_root),
        truth_rows,
    )
    receipt = {
        "base_rows": BASE_ROWS,
        "claim_eligible": False,
        "compaction": {
            "amplification_ppm": compaction["amplification_ppm"],
            "input_runs": compaction["input_runs"],
            "logical_live_bytes": compaction["logical_live_bytes"],
            "read_bytes": compaction["read_bytes"],
            "result": compacted_result,
            "write_bytes": compaction["write_bytes"],
        },
        "delta_rows": DELTA_ROWS,
        "evidence_kind": "semantic-local-artifact-screen",
        "page_budget": PAGE_BUDGET,
        "query_count": QUERY_COUNT,
        "results": results,
        "rows": ROWS,
        "schema": "borsuk-v85-delta-compaction-screen-v1",
    }
    _write_and_validate_receipt(receipt, request.output)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=pathlib.Path, required=True)
    parser.add_argument("--queries", type=pathlib.Path, required=True)
    parser.add_argument("--truth", type=pathlib.Path, required=True)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--work", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--uri-prefix", required=True)
    args = parser.parse_args()
    run_screen(ScreenRequest(**vars(args)))


if __name__ == "__main__":
    main()
