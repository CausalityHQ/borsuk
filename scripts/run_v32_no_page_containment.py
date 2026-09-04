#!/usr/bin/env python3
"""Fail fast on V32 truth containment without reading any page body."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from argparse import ArgumentParser
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

QUERY_COUNT = 32
RECALL_K = 10


@dataclass(frozen=True)
class LocalArtifact:
    path: Path
    sha256: str
    encoded_bytes: int


@dataclass(frozen=True)
class V32ContainmentPlan:
    qualifier: Path
    manifest: LocalArtifact
    artifact_dir: Path
    query: LocalArtifact
    logical_sources: LocalArtifact
    truth: LocalArtifact
    source_rows: int
    query_start: int
    query_count: int


def _digest(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _validate_artifact(artifact: LocalArtifact) -> None:
    if (
        not artifact.path.is_absolute()
        or not _digest(artifact.sha256)
        or type(artifact.encoded_bytes) is not int
        or artifact.encoded_bytes <= 0
    ):
        raise ValueError("V32 containment artifact authority differs")


def _read_truth(
    plan: V32ContainmentPlan, truth_bytes: bytes
) -> tuple[tuple[int, ...], ...]:
    _validate_artifact(plan.manifest)
    _validate_artifact(plan.query)
    _validate_artifact(plan.truth)
    if (
        not plan.qualifier.is_absolute()
        or not plan.artifact_dir.is_absolute()
        or plan.source_rows != 1_000_000
        or type(plan.query_start) is not int
        or plan.query_start < 0
        or plan.query_count != QUERY_COUNT
        or type(truth_bytes) is not bytes
        or len(truth_bytes) != plan.truth.encoded_bytes
        or hashlib.sha256(truth_bytes).hexdigest() != plan.truth.sha256
    ):
        raise ValueError("V32 containment truth byte authority differs")
    table = pq.read_table(pa.BufferReader(truth_bytes))
    if table.schema.names != ["neighbors_id"] or table.num_rows < plan.query_start + QUERY_COUNT:
        raise ValueError("V32 containment truth schema differs")
    field = table.schema.field("neighbors_id")
    item = (
        field.type.value_field
        if pa.types.is_list(field.type) or pa.types.is_fixed_size_list(field.type)
        else None
    )
    if (
        field.nullable
        or item is None
        or item.nullable
        or not (pa.types.is_int32(item.type) or pa.types.is_int64(item.type))
    ):
        raise ValueError("V32 containment truth schema differs")
    rows = table.column("neighbors_id").slice(plan.query_start, QUERY_COUNT).to_pylist()
    result: list[tuple[int, ...]] = []
    for row in rows:
        if (
            type(row) is not list
            or len(row) < RECALL_K
            or any(
                type(logical) is not int or not 0 <= logical < plan.source_rows
                for logical in row[:RECALL_K]
            )
            or len(set(row[:RECALL_K])) != RECALL_K
        ):
            raise ValueError("V32 containment truth membership differs")
        result.append(tuple(row[:RECALL_K]))
    return tuple(result)


def _read_logical_sources(plan: V32ContainmentPlan) -> tuple[int, ...]:
    _validate_artifact(plan.logical_sources)
    payload = plan.logical_sources.path.read_bytes()
    if (
        len(payload) != plan.logical_sources.encoded_bytes
        or hashlib.sha256(payload).hexdigest() != plan.logical_sources.sha256
    ):
        raise ValueError("V32 containment logical-source byte authority differs")
    try:
        table = pa.ipc.open_file(pa.BufferReader(payload)).read_all()
    except pa.ArrowInvalid as error:
        raise ValueError("V32 containment logical-source Arrow differs") from error
    expected_schema = pa.schema(
        [pa.field("source_ordinal", pa.uint64(), nullable=False)]
    )
    if table.schema != expected_schema or table.num_rows != plan.source_rows:
        raise ValueError("V32 containment logical-source Arrow differs")
    sources = table.column("source_ordinal").to_pylist()
    source_to_logical = [-1] * plan.source_rows
    for logical, source in enumerate(sources):
        if (
            type(source) is not int
            or not 0 <= source < plan.source_rows
            or source_to_logical[source] != -1
        ):
            raise ValueError("V32 containment logical-source permutation differs")
        source_to_logical[source] = logical
    if any(logical < 0 for logical in source_to_logical):
        raise ValueError("V32 containment logical-source permutation differs")
    return tuple(source_to_logical)


def _commands(
    plan: V32ContainmentPlan,
    truth: tuple[tuple[int, ...], ...],
    source_to_logical: tuple[int, ...],
) -> tuple[tuple[str, ...], ...]:
    common = (
        str(plan.qualifier),
        "--execute",
        "--manifest",
        str(plan.manifest.path),
        "--manifest-sha256",
        plan.manifest.sha256,
        "--manifest-bytes",
        str(plan.manifest.encoded_bytes),
        "--artifact-dir",
        str(plan.artifact_dir),
        "--query-parquet",
        str(plan.query.path),
        "--query-sha256",
        plan.query.sha256,
        "--query-bytes",
        str(plan.query.encoded_bytes),
        "--query-count",
        "1",
        "--root-beam",
        "8",
        "--leaf-beam",
        "64",
        "--candidate-depth",
        "12288",
        "--page-count",
        "16",
        "--k",
        "10",
    )
    return tuple(
        common
        + (
            "--query-start",
            str(plan.query_start + offset),
            "--diagnose-logicals",
            ",".join(str(source_to_logical[source]) for source in sources),
        )
        for offset, sources in enumerate(truth)
    )


def build_v32_containment_commands(
    plan: V32ContainmentPlan, truth_bytes: bytes
) -> tuple[tuple[str, ...], ...]:
    """Build one no-page diagnostic invocation per frozen query."""

    truth = _read_truth(plan, truth_bytes)
    return _commands(plan, truth, _read_logical_sources(plan))


def _diagnostics(
    payload: bytes, query_ordinal: int, truth: tuple[int, ...]
) -> tuple[list[dict[str, object]], dict[str, int]]:
    if type(payload) is not bytes or not payload.endswith(b"\n") or b"\n" in payload[:-1]:
        raise ValueError("V32 containment diagnostic canonical bytes differ")
    value = json.loads(payload)
    expected = (
        json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
    if (
        payload != expected
        or type(value) is not dict
        or set(value)
        != {
            "claim_eligible",
            "diagnostics",
            "page_body_reads",
            "query_ordinal",
            "routing",
            "schema_version",
        }
        or value["claim_eligible"] is not False
        or value["schema_version"] != 3
        or value["page_body_reads"] != 0
        or value["query_ordinal"] != query_ordinal
        or type(value["diagnostics"]) is not list
        or len(value["diagnostics"]) != RECALL_K
    ):
        raise ValueError("V32 containment diagnostic authority differs")
    diagnostics = value["diagnostics"]
    expected_keys = {
        "candidate_rank",
        "first_unique_page_rank",
        "leaf_ordinal",
        "logical",
        "page_ordinal",
        "reciprocal_rank_selected",
        "stage",
    }
    stages = {"leaf-frontier", "candidate-retention", "page-reducer", "selected-page"}
    for item in diagnostics:
        optional_ranks = (
            (item.get("candidate_rank"), item.get("first_unique_page_rank"))
            if type(item) is dict
            else ()
        )
        if (
            type(item) is not dict
            or set(item) != expected_keys
            or type(item["logical"]) is not int
            or type(item["leaf_ordinal"]) is not int
            or type(item["page_ordinal"]) is not int
            or item["logical"] < 0
            or item["leaf_ordinal"] < 0
            or item["page_ordinal"] < 0
            or type(item["reciprocal_rank_selected"]) is not bool
            or item["stage"] not in stages
            or any(
                rank is not None and (type(rank) is not int or rank < 0)
                for rank in optional_ranks
            )
            or (item["stage"] == "selected-page" and item["candidate_rank"] is None)
        ):
            raise ValueError("V32 containment diagnostic value differs")
    if {item["logical"] for item in diagnostics} != set(truth):
        raise ValueError("V32 containment truth binding differs")
    routing = value["routing"]
    routing_keys = {
        "candidates_retained",
        "codes_scanned",
        "leaves_scored",
        "pages_considered",
        "roots_scored",
        "selected_page_bytes",
        "selected_pages",
    }
    if (
        type(routing) is not dict
        or set(routing) != routing_keys
        or any(type(item) is not int or item < 0 for item in routing.values())
        or routing["candidates_retained"] != 12_288
        or not 12_288 <= routing["codes_scanned"] <= 1_000_000
        or routing["leaves_scored"] == 0
        or routing["roots_scored"] == 0
        or not 16 <= routing["pages_considered"] <= 12_288
        or routing["selected_pages"] != 16
        or routing["selected_page_bytes"] == 0
    ):
        raise ValueError("V32 containment routing work differs")
    return diagnostics, routing


def run_v32_no_page_containment(
    plan: V32ContainmentPlan,
    truth_bytes: bytes,
    *,
    invoke: Callable[[tuple[str, ...]], bytes],
) -> bytes:
    """Run and independently reduce the page-free scale containment gate."""

    truth = _read_truth(plan, truth_bytes)
    source_to_logical = _read_logical_sources(plan)
    commands = _commands(plan, truth, source_to_logical)
    samples = []
    routing_work = []
    losses: Counter[str] = Counter()
    for offset, (command, logicals) in enumerate(zip(commands, truth, strict=True)):
        query_ordinal = plan.query_start + offset
        diagnostics, routing = _diagnostics(
            invoke(command),
            query_ordinal,
            tuple(source_to_logical[source] for source in logicals),
        )
        routing_work.append(routing)
        hits = sum(item["stage"] == "selected-page" for item in diagnostics)
        losses.update(
            str(item["stage"])
            for item in diagnostics
            if item["stage"] != "selected-page"
        )
        samples.append({"hits": hits, "query_ordinal": query_ordinal})
    total_hits = sum(sample["hits"] for sample in samples)
    aggregate = total_hits * 1_000_000 // (QUERY_COUNT * RECALL_K)
    minimum = min(sample["hits"] for sample in samples) * 1_000_000 // RECALL_K
    perfect = sum(sample["hits"] == RECALL_K for sample in samples)
    failed = [] if total_hits == QUERY_COUNT * RECALL_K else ["perfect-containment"]
    maximum_selected_page_bytes = max(
        work["selected_page_bytes"] for work in routing_work
    )
    if maximum_selected_page_bytes > 3_145_728:
        failed.append("selected-page-bytes")
    value = {
        "aggregate_containment_ppm": aggregate,
        "claim_eligible": False,
        "failed_gates": failed,
        "losses_by_stage": dict(sorted(losses.items())),
        "manifest_sha256": plan.manifest.sha256,
        "maximum_codes_scanned": max(
            work["codes_scanned"] for work in routing_work
        ),
        "maximum_selected_page_bytes": maximum_selected_page_bytes,
        "minimum_containment_ppm": minimum,
        "logical_sources_sha256": plan.logical_sources.sha256,
        "page_body_reads": 0,
        "perfect_queries": perfect,
        "query_count": QUERY_COUNT,
        "query_sha256": plan.query.sha256,
        "samples": samples,
        "schema_version": 1,
        "selected_page_hits": total_hits,
        "source_rows": plan.source_rows,
        "status": "passed" if not failed else "failed",
        "truth_sha256": plan.truth.sha256,
    }
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _invoke(command: tuple[str, ...]) -> bytes:
    completed = subprocess.run(command, check=False, capture_output=True)
    if completed.returncode != 0:
        detail = completed.stderr.decode(errors="replace").strip()
        raise RuntimeError(
            f"V32 containment qualifier failed ({completed.returncode}): {detail}"
        )
    return completed.stdout


def main(arguments: list[str] | None = None) -> int:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", required=True)
    parser.add_argument("--qualifier", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--manifest-bytes", type=int, required=True)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--query-parquet", type=Path, required=True)
    parser.add_argument("--query-sha256", required=True)
    parser.add_argument("--query-bytes", type=int, required=True)
    parser.add_argument("--logical-sources-arrow", type=Path, required=True)
    parser.add_argument("--logical-sources-sha256", required=True)
    parser.add_argument("--logical-sources-bytes", type=int, required=True)
    parser.add_argument("--truth-parquet", type=Path, required=True)
    parser.add_argument("--truth-sha256", required=True)
    parser.add_argument("--truth-bytes", type=int, required=True)
    parser.add_argument("--source-rows", type=int, required=True)
    parser.add_argument("--query-start", type=int, required=True)
    parser.add_argument("--query-count", type=int, required=True)
    args = parser.parse_args(arguments)
    plan = V32ContainmentPlan(
        qualifier=args.qualifier,
        manifest=LocalArtifact(
            args.manifest, args.manifest_sha256, args.manifest_bytes
        ),
        artifact_dir=args.artifact_dir,
        query=LocalArtifact(
            args.query_parquet, args.query_sha256, args.query_bytes
        ),
        logical_sources=LocalArtifact(
            args.logical_sources_arrow,
            args.logical_sources_sha256,
            args.logical_sources_bytes,
        ),
        truth=LocalArtifact(
            args.truth_parquet, args.truth_sha256, args.truth_bytes
        ),
        source_rows=args.source_rows,
        query_start=args.query_start,
        query_count=args.query_count,
    )
    truth = args.truth_parquet.read_bytes()
    sys.stdout.buffer.write(
        run_v32_no_page_containment(plan, truth, invoke=_invoke)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
