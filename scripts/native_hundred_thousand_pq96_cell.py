#!/usr/bin/env python3
"""Truth-separated paired PQ96/source ReLAION-100k range screen."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import platform
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import read_geometric_router_parquet
from scripts.native_hundred_thousand_sign96_cell import (
    _read_plans,
    _source_authority,
)
from scripts.native_one_million_data_range_query import plan_ranked_pages
from scripts.native_one_million_page_priority import rank_selected_pages
from scripts.native_one_million_source_priority import source_distance_batch
from scripts.native_page_microcluster_cell import FROZEN_INPUTS, _read_queries_truth
from scripts.native_pq96_groups import (
    GROUP_PAGES,
    fit_source_pq96,
    open_pq96_groups,
    score_pq96,
    source_rows_digest,
    write_pq96_groups,
)
from scripts.native_rotated_sign96 import BATCH_ROWS
from scripts.native_rotated_two_bit_cell import PRIOR_PAGES, PRIOR_TREE
from scripts.recount_native_one_million_data_range import recount_range_masks
from scripts.validate_native_geometric_layout_result import _read_geometric_queries

SCHEMA = "borsuk-hundred-thousand-pq96-range-v1"
SOURCE_SEAL_FILE = "pq96-source-seal.json"
PLANS_FILE = "pq96-plans.json"
PLAN_SEAL_FILE = "pq96-plan-seal.json"


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _scorer_identity() -> dict[str, str]:
    blas = np.__config__.show(mode="dicts")["Build Dependencies"]["blas"]
    return {
        "numpy": np.__version__,
        "blas_name": str(blas["name"]),
        "blas_version": str(blas["version"]),
        "openblas_threads": os.environ.get("OPENBLAS_NUM_THREADS", ""),
        "omp_threads": os.environ.get("OMP_NUM_THREADS", ""),
        "source_arithmetic": "float32-to-float64-squared-l2-norm-plus-dot-clamp-zero-v1",
        "pq96_arithmetic": "v97-ascending-subspace-float32-adc-v1",
        "machine": platform.machine(),
        "cpu_flags_sha256": _cpu_flags_sha256(),
    }


def _cpu_flags_sha256() -> str:
    cpuinfo = Path("/proc/cpuinfo").read_text()
    flags = next(
        (line.partition(":")[2].split() for line in cpuinfo.splitlines()
         if line.startswith(("flags", "Features"))),
        [],
    )
    return hashlib.sha256(" ".join(sorted(flags)).encode()).hexdigest()


def run_construct(
    root: Path, *, expected_rows: int = 100_000, expected_pages: int = 166,
) -> dict[str, object]:
    """Train and seal PQ96 before query or truth capability is available."""
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("PQ96 construction must be source-only")
    _, vectors, _, ordinals, counts, order_sha = _source_authority(
        root, expected_rows, expected_pages,
    )
    books = fit_source_pq96(vectors, expected_rows=expected_rows)
    stream_sha = source_rows_digest(vectors)
    physical_by_source = np.empty(expected_rows, dtype=np.int64)
    physical_by_source[np.asarray(ordinals, dtype=np.intp)] = np.arange(expected_rows)
    batches = (
        (physical_by_source[first:first + 4096], vectors[first:first + 4096])
        for first in range(0, expected_rows, 4096)
    )
    written = write_pq96_groups(
        root / "pq96", books, counts, batches,
        source_sha256=FROZEN_INPUTS.layout.source.sha256,
        layout_sha256=FROZEN_INPUTS.membership.sha256,
        physical_order_sha256=order_sha,
        source_rows_sha256=stream_sha,
    )
    seal = {
        "schema": SCHEMA + "-source-seal",
        "source": dataclasses.asdict(FROZEN_INPUTS.layout.source),
        "membership": dataclasses.asdict(FROZEN_INPUTS.membership),
        "prior_physical_order_sha256": order_sha,
        "source_rows_sha256": stream_sha,
        "rows": expected_rows, "pages": expected_pages,
        "pq96_seal_sha256": written.seal_sha256,
        "pq96_groups_sha256": written.seal["groups_sha256"],
        "pq96_model_sha256": written.seal["model_sha256"],
        "machine": platform.machine(),
        "cpu_flags_sha256": _cpu_flags_sha256(),
    }
    (root / SOURCE_SEAL_FILE).write_bytes(_canonical(seal))
    return seal


def _read_source_seal(root: Path, rows: int, pages: int, order_sha: str) -> dict[str, object]:
    body = (root / SOURCE_SEAL_FILE).read_bytes()
    seal = json.loads(body)
    codec_body = (root / "pq96" / "seal.json").read_bytes()
    codec = json.loads(codec_body)
    if (
        body != _canonical(seal)
        or set(seal) != {
            "schema", "source", "membership", "prior_physical_order_sha256",
            "source_rows_sha256", "rows", "pages", "pq96_seal_sha256",
            "pq96_groups_sha256", "pq96_model_sha256",
            "machine", "cpu_flags_sha256",
        }
        or seal.get("schema") != SCHEMA + "-source-seal"
        or seal.get("source") != dataclasses.asdict(FROZEN_INPUTS.layout.source)
        or seal.get("membership") != dataclasses.asdict(FROZEN_INPUTS.membership)
        or seal.get("prior_physical_order_sha256") != order_sha
        or seal.get("rows") != rows or seal.get("pages") != pages
        or hashlib.sha256(codec_body).hexdigest() != seal.get("pq96_seal_sha256")
        or seal.get("source_rows_sha256") != codec.get("source_rows_sha256")
        or seal.get("pq96_groups_sha256") != codec.get("groups_sha256")
        or seal.get("pq96_model_sha256") != codec.get("model_sha256")
    ):
        raise ValueError("PQ96 source seal differs")
    return seal


def run_plan(
    root: Path, *, query_count: int = 1000, expected_rows: int = 100_000,
    expected_pages: int = 166, maximum_gets: int = 32,
    maximum_bytes: int = 16_777_216,
) -> dict[str, object]:
    """Use the same fixed selected rows; derive PQ and source plans separately."""
    if (root / "truth.parquet").exists():
        raise ValueError("PQ96 plan must be truth-free")
    _, vectors, membership, ordinals, counts, order_sha = _source_authority(
        root, expected_rows, expected_pages,
    )
    source_seal = _read_source_seal(root, expected_rows, expected_pages, order_sha)
    reader = open_pq96_groups(
        root / "pq96", source_sha256=FROZEN_INPUTS.layout.source.sha256,
        layout_sha256=FROZEN_INPUTS.membership.sha256,
        physical_order_sha256=order_sha,
        seal_sha256=source_seal["pq96_seal_sha256"],
    )
    if reader.page_row_counts != counts:
        raise ValueError("PQ96 code/layout page counts differ")
    records = np.concatenate(
        [reader.group_records(index) for index in range(len(reader.ranges))]
    )
    physical_vectors = vectors[np.asarray(ordinals, dtype=np.intp)]
    row_pages = np.repeat(np.arange(len(counts), dtype=np.uint32), counts)
    row_groups = (row_pages // GROUP_PAGES).astype(np.uint32)
    router = read_geometric_router_parquet(
        root / "tree.parquet", root / "pages.parquet", FROZEN_INPUTS.layout,
        membership, PRIOR_TREE, PRIOR_PAGES,
    )
    page_lengths = {"base": tuple(int(page.encoded_page_bytes) for page in router.pages)}
    if len(page_lengths["base"]) != expected_pages:
        raise ValueError("PQ96 page layout differs")
    queries = _read_geometric_queries(
        root / "queries.parquet", FROZEN_INPUTS.queries, 768,
    )
    prior_plans = _read_plans(root)
    if len(queries) != query_count or len(prior_plans) != query_count:
        raise ValueError("PQ96 query cohort differs")
    samples: list[dict[str, object]] = []
    for ordinal, (query, prior) in enumerate(zip(queries, prior_plans, strict=True)):
        if prior["query_ordinal"] != ordinal:
            raise ValueError("PQ96 prior query order differs")
        selected = tuple(prior["selected_groups"])
        if expected_rows == 100_000 and len(selected) != 32:
            raise ValueError("PQ96 fixed OPQ8 group count differs")
        positions = np.flatnonzero(np.isin(row_groups, selected))
        if not len(positions):
            raise ValueError("PQ96 selected rows empty")
        pq_scores = score_pq96(query, reader, records[positions])
        source_scores = np.empty(len(positions), dtype=np.float64)
        for first in range(0, len(positions), BATCH_ROWS):
            last = min(first + BATCH_ROWS, len(positions))
            source_scores[first:last] = source_distance_batch(
                query[None, :], physical_vectors[positions[first:last]]
            )[0]
        plans = {
            arm: plan_ranked_pages(
                rank_selected_pages(
                    scores, row_pages[positions], row_groups[positions],
                    selected, len(counts),
                ),
                page_lengths, maximum_gets=maximum_gets, maximum_bytes=maximum_bytes,
            )
            for arm, scores in (("pq96", pq_scores), ("source", source_scores))
        }
        errors = pq_scores.astype(np.float64) - source_scores
        absolute = np.abs(errors)
        samples.append({
            "query_ordinal": ordinal, "selected_groups": list(selected),
            "projected_code_gets": len(selected),
            "projected_code_bytes": sum(reader.ranges[group][3] for group in selected),
            "score_error": {
                "signed_mean": float(np.mean(errors)),
                "absolute_p50": float(np.quantile(absolute, 0.50)),
                "absolute_p95": float(np.quantile(absolute, 0.95)),
                "absolute_p99": float(np.quantile(absolute, 0.99)),
                "absolute_max": float(np.max(absolute)),
            },
            **plans,
        })
    plans_doc = {
        "schema": SCHEMA + "-plans",
        "source_seal_sha256": _sha(root / SOURCE_SEAL_FILE),
        "opq_plans_sha256": _sha(root / "opq" / "plans.json"),
        "query_identity": dataclasses.asdict(FROZEN_INPUTS.queries),
        "scorer": _scorer_identity(), "samples": samples,
    }
    body = _canonical(plans_doc)
    (root / PLANS_FILE).write_bytes(body)
    (root / PLAN_SEAL_FILE).write_bytes(_canonical({
        "schema": SCHEMA + "-plan-seal",
        "source_seal_sha256": plans_doc["source_seal_sha256"],
        "plans_sha256": hashlib.sha256(body).hexdigest(),
        "opq_plans_sha256": plans_doc["opq_plans_sha256"],
        "query_identity": plans_doc["query_identity"],
        "scorer": plans_doc["scorer"],
    }))
    return plans_doc


def _sealed_plans(root: Path, query_count: int) -> dict[str, object]:
    body = (root / PLANS_FILE).read_bytes()
    plans = json.loads(body)
    seal_body = (root / PLAN_SEAL_FILE).read_bytes()
    seal = json.loads(seal_body)
    prior = _read_plans(root)
    if (
        body != _canonical(plans) or seal_body != _canonical(seal)
        or plans.get("schema") != SCHEMA + "-plans"
        or plans.get("source_seal_sha256") != _sha(root / SOURCE_SEAL_FILE)
        or plans.get("opq_plans_sha256") != _sha(root / "opq" / "plans.json")
        or plans.get("query_identity") != dataclasses.asdict(FROZEN_INPUTS.queries)
        or len(plans.get("samples", [])) != query_count
        or len(prior) != query_count
        or seal != {
            "schema": SCHEMA + "-plan-seal",
            "source_seal_sha256": plans["source_seal_sha256"],
            "plans_sha256": hashlib.sha256(body).hexdigest(),
            "opq_plans_sha256": plans["opq_plans_sha256"],
            "query_identity": plans["query_identity"],
            "scorer": plans["scorer"],
        }
    ):
        raise ValueError("PQ96 plan seal differs")
    for ordinal, (sample, historical) in enumerate(zip(plans["samples"], prior, strict=True)):
        if (
            sample.get("query_ordinal") != ordinal
            or sample.get("selected_groups") != historical["selected_groups"]
        ):
            raise ValueError("PQ96 fixed group plan differs")
    return plans


def run_evaluate(
    root: Path, out: Path, *, query_count: int = 1000,
    expected_rows: int = 100_000, expected_pages: int = 166,
) -> dict[str, object]:
    """Open truth only after PQ/source plans are sealed."""
    _, _, membership, _, counts, order_sha = _source_authority(
        root, expected_rows, expected_pages,
    )
    _read_source_seal(root, expected_rows, expected_pages, order_sha)
    plans = _sealed_plans(root, query_count)
    _, truth = _read_queries_truth(root, FROZEN_INPUTS)
    if len(truth) != query_count:
        raise ValueError("PQ96 truth cohort differs")
    owners = {row.stable_id: row.page_ordinal for row in membership}
    truth_pages = []
    for neighbors in truth:
        if (
            len(neighbors) != 100 or len(set(neighbors)) != 100
            or any(item not in owners for item in neighbors)
        ):
            raise ValueError("PQ96 truth owners differ")
        truth_pages.append([owners[item] for item in neighbors])
    router = read_geometric_router_parquet(
        root / "tree.parquet", root / "pages.parquet", FROZEN_INPUTS.layout,
        membership, PRIOR_TREE, PRIOR_PAGES,
    )
    lengths = {"base": tuple(int(page.encoded_page_bytes) for page in router.pages)}
    if len(lengths["base"]) != len(counts):
        raise ValueError("PQ96 truth page layout differs")
    samples, metrics = recount_range_masks(
        plans["samples"], truth_pages, lengths, arms=("pq96", "source"),
    )
    paired_deltas = []
    for sample, plan in zip(samples, plans["samples"], strict=True):
        pq_plan, source_plan = plan["pq96"], plan["source"]
        paired_deltas.append({
            "query_ordinal": sample["query_ordinal"],
            "gt100_delta": sample["pq96"]["hits_at_100"] - sample["source"]["hits_at_100"],
            "gt10_delta": sample["pq96"]["hits_at_10"] - sample["source"]["hits_at_10"],
            "target_page_symmetric_difference": len(
                {tuple(x) for x in pq_plan["target_pages"]}
                ^ {tuple(x) for x in source_plan["target_pages"]}
            ),
            "included_page_symmetric_difference": len(
                {tuple(x) for x in pq_plan["included_pages"]}
                ^ {tuple(x) for x in source_plan["included_pages"]}
            ),
            "interval_symmetric_difference": len(
                {tuple(x) for x in pq_plan["ranges"]}
                ^ {tuple(x) for x in source_plan["ranges"]}
            ),
            "data_get_delta": pq_plan["gets"] - source_plan["gets"],
        })
    projection = {
        "total_code_gets": sum(item["projected_code_gets"] for item in plans["samples"]),
        "total_code_bytes": sum(item["projected_code_bytes"] for item in plans["samples"]),
        "maximum_code_gets": max(item["projected_code_gets"] for item in plans["samples"]),
        "maximum_code_bytes": max(item["projected_code_bytes"] for item in plans["samples"]),
        "model_bytes": (root / "pq96" / "model.bin").stat().st_size,
    }
    pq = metrics["pq96"]
    source = metrics["source"]
    quality_advance = (
        source["gt100_hits"] >= 98_000
        and pq["gt100_hits"] >= source["gt100_hits"] - 300
        and pq["p05_gt100_hits"] >= 90
        and pq["sub90_queries"] <= source["sub90_queries"] + 10
        and pq["maximum_gets"] <= 32
        and pq["maximum_bytes"] <= 16_777_216
    )
    out.mkdir(parents=True, exist_ok=True)
    evidence = {
        "schema": SCHEMA + "-evidence",
        "plans_sha256": _sha(root / PLANS_FILE),
        "samples": samples, "metrics": metrics,
        "paired_deltas": paired_deltas, "code_projection": projection,
    }
    evidence_body = _canonical(evidence)
    (out / "pq96-evidence.json").write_bytes(evidence_body)
    result = {
        "schema": SCHEMA + "-result",
        "plans_sha256": evidence["plans_sha256"],
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "quality_advance_candidate": quality_advance,
        "resource_gate_pending": True,
        "metrics": metrics, "code_projection": projection,
    }
    (out / "pq96-result.json").write_bytes(_canonical(result))
    return result


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("construct", "plan", "evaluate"))
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--out", type=Path, default=Path("evaluation"))
    args = parser.parse_args(argv)
    if args.phase == "construct":
        run_construct(args.root)
    elif args.phase == "plan":
        run_plan(args.root)
    else:
        run_evaluate(args.root, args.out)


if __name__ == "__main__":
    main()
