#!/usr/bin/env python3
"""Independent recomputation for the native ANN G1 row-width screen."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass

import numpy as np


@dataclass(frozen=True, slots=True)
class ArmEvidenceSummary:
    average_recall10_ppm: int
    average_recall100_ppm: int
    p05_recall100_ppm: int
    worst_recall100_ppm: int
    max_gets_per_query: int
    max_bytes_per_query: int
    quality_gate_passed: bool
    resource_gate_passed: bool


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{label} differs")
    return value


def _page_key(value: object) -> tuple[str, int]:
    if not isinstance(value, dict) or set(value) != {"object_role", "ordinal"}:
        raise ValueError("selected page differs")
    role = value["object_role"]
    ordinal = value["ordinal"]
    if role not in ("base", "delta") or type(ordinal) is not int or ordinal < 0:
        raise ValueError("selected page differs")
    return role, ordinal


def _nearest_rank(values: Sequence[int], numerator: int, denominator: int) -> int:
    ordered = sorted(values)
    index = max(0, (len(ordered) * numerator + denominator - 1) // denominator - 1)
    return int(ordered[index])


def validate_arm_evidence(
    samples: object,
    *,
    page_by_id: Mapping[int, tuple[str, int]],
    page_bytes: Mapping[tuple[str, int], int],
    page_ranges: Mapping[tuple[str, int], tuple[int, int]],
    expected_queries: int,
    neighbors: int,
    max_gets: int,
    max_bytes: int,
) -> ArmEvidenceSummary:
    """Reconstruct every sample and aggregate from page membership."""

    if (
        not isinstance(samples, list)
        or len(samples) != expected_queries
        or expected_queries <= 0
        or neighbors <= 0
        or max_gets <= 0
        or max_bytes <= 0
    ):
        raise ValueError("arm sample count differs")
    expected_keys = {
        "bytes",
        "gets",
        "hit10_ids",
        "hit_ids",
        "hits10",
        "hits",
        "query",
        "ranges",
        "recall10_ppm",
        "recall100_ppm",
        "selected_pages",
        "truth_ids",
    }
    recalls10: list[int] = []
    recalls100: list[int] = []
    gets_values: list[int] = []
    bytes_values: list[int] = []
    for query, sample in enumerate(samples):
        if not isinstance(sample, dict) or set(sample) != expected_keys:
            raise ValueError("arm sample schema differs")
        if _integer(sample["query"], "query ordinal") != query:
            raise ValueError("query ordinals differ")
        truth = sample["truth_ids"]
        selected_raw = sample["selected_pages"]
        ranges_raw = sample["ranges"]
        if (
            not isinstance(truth, list)
            or len(truth) != neighbors
            or any(type(row_id) is not int for row_id in truth)
            or len(set(truth)) != neighbors
            or not isinstance(selected_raw, list)
            or not isinstance(ranges_raw, list)
        ):
            raise ValueError("sample truth or pages differ")
        selected = tuple(_page_key(value) for value in selected_raw)
        if len(set(selected)) != len(selected) or any(
            page not in page_bytes for page in selected
        ):
            raise ValueError("selected page differs")
        expected_ranges = [
            {
                "bytes": page_ranges[page][1],
                "object_role": page[0],
                "offset": page_ranges[page][0],
                "ordinal": page[1],
            }
            for page in selected
        ]
        if ranges_raw != expected_ranges:
            raise ValueError("sample range evidence differs")
        if any(row_id not in page_by_id for row_id in truth):
            raise ValueError("truth page membership differs")
        selected_set = set(selected)
        expected_hits = [
            row_id for row_id in truth if page_by_id[row_id] in selected_set
        ]
        cutoff = min(10, neighbors)
        expected_hits10 = [
            row_id for row_id in truth[:cutoff] if page_by_id[row_id] in selected_set
        ]
        recall10_ppm = len(expected_hits10) * 1_000_000 // cutoff
        recall100_ppm = len(expected_hits) * 1_000_000 // neighbors
        if (
            sample["hit_ids"] != expected_hits
            or sample["hit10_ids"] != expected_hits10
            or _integer(sample["hits"], "sample hits") != len(expected_hits)
            or _integer(sample["hits10"], "sample hits10")
            != len(expected_hits10)
            or _integer(sample["recall10_ppm"], "sample recall10")
            != recall10_ppm
            or _integer(sample["recall100_ppm"], "sample recall100")
            != recall100_ppm
        ):
            raise ValueError("sample hit evidence differs")
        expected_gets = len(selected)
        expected_bytes = sum(page_bytes[page] for page in selected)
        if (
            _integer(sample["gets"], "sample GETs") != expected_gets
            or _integer(sample["bytes"], "sample bytes") != expected_bytes
        ):
            raise ValueError("sample resource evidence differs")
        recalls10.append(recall10_ppm)
        recalls100.append(recall100_ppm)
        gets_values.append(expected_gets)
        bytes_values.append(expected_bytes)

    average10 = sum(recalls10) // expected_queries
    average100 = sum(recalls100) // expected_queries
    p05 = _nearest_rank(recalls100, 5, 100)
    worst = min(recalls100)
    maximum_gets = max(gets_values)
    maximum_bytes = max(bytes_values)
    return ArmEvidenceSummary(
        average_recall10_ppm=average10,
        average_recall100_ppm=average100,
        p05_recall100_ppm=p05,
        worst_recall100_ppm=worst,
        max_gets_per_query=maximum_gets,
        max_bytes_per_query=maximum_bytes,
        quality_gate_passed=(
            average10 >= 960_000 and average100 >= 975_000 and p05 >= 900_000
        ),
        resource_gate_passed=maximum_gets <= max_gets and maximum_bytes <= max_bytes,
    )


def paired_bootstrap_interval(
    challenger: Sequence[int],
    control: Sequence[int],
    *,
    seed: int,
    resamples: int,
    statistic: str,
) -> tuple[int, int]:
    """Return one deterministic paired percentile interval in ppm."""

    left = np.asarray(challenger, dtype=np.int64)
    right = np.asarray(control, dtype=np.int64)
    if (
        left.ndim != 1
        or left.shape != right.shape
        or left.size == 0
        or resamples < 100
        or statistic not in ("mean", "p05")
    ):
        raise ValueError("paired bootstrap input differs")
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
    lower, upper = np.quantile(
        differences, [0.025, 0.975], method="nearest"
    )
    return int(np.rint(lower)), int(np.rint(upper))


_ARM_SPECS = {
    "pq16x8": (16, 8, 16, True),
    "pq24x8": (24, 8, 24, True),
    "pq32x8": (32, 8, 32, True),
    "pq32x4": (32, 4, 16, True),
    "summary-only-pq16x8": (16, 8, 0, False),
}


def choose_screen_winner(
    summaries: Mapping[str, ArmEvidenceSummary],
    *,
    projections: Mapping[str, bool],
    paired_r100_ci: Mapping[str, tuple[int, int]],
    exact_gate_passed: bool,
) -> str | None:
    """Apply point gates, CI kills, width, and frozen secondary tie-breaks."""

    if (
        set(summaries) != set(projections)
        or set(summaries) != set(paired_r100_ci)
        or any(name not in _ARM_SPECS for name in summaries)
    ):
        raise ValueError("winner evidence roster differs")
    eligible = [
        name
        for name, summary in summaries.items()
        if exact_gate_passed
        and summary.quality_gate_passed
        and summary.resource_gate_passed
        and projections[name]
        and paired_r100_ci[name][1] >= 0
    ]
    if not eligible:
        return None
    return min(
        eligible,
        key=lambda name: (
            _ARM_SPECS[name][2],
            -summaries[name].p05_recall100_ppm,
            -summaries[name].average_recall10_ppm,
            name,
        ),
    )


def _resident_projection(name: str) -> dict[str, object]:
    subspaces, centroid_bits, row_bytes, row_codes_present = _ARM_SPECS[name]
    if 768 % subspaces:
        raise ValueError("resident projection arm differs")
    rows = 100_000_000
    pages = (rows + 255) // 256
    row_codes_bytes = rows * row_bytes
    summary_codes_bytes = pages * 2 * 16
    row_codebook_bytes = (1 << centroid_bits) * 768 * 4 if row_codes_present else 0
    summary_codebook_bytes = 256 * 768 * 4
    sq8_parameter_bytes = 2 * 768 * 4
    mutation_entries = 1_000_000
    mutation_directory_bytes = mutation_entries * 96
    resident_delta_rows = 100_000
    resident_delta_bytes = resident_delta_rows * (768 + 72)
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
    budget_bytes = 3 * 1024**3
    return {
        "rows": rows,
        "pages": pages,
        "row_codes_bytes": row_codes_bytes,
        "summary_codes_bytes": summary_codes_bytes,
        "row_codebook_bytes": row_codebook_bytes,
        "summary_codebook_bytes": summary_codebook_bytes,
        "sq8_parameter_bytes": sq8_parameter_bytes,
        "mutation_entries": mutation_entries,
        "mutation_directory_bytes": mutation_directory_bytes,
        "resident_delta_rows": resident_delta_rows,
        "resident_delta_bytes": resident_delta_bytes,
        "page_directory_reserve_bytes": page_directory_reserve_bytes,
        "response_buffers_bytes": response_buffers_bytes,
        "planner_workspace_bytes": planner_workspace_bytes,
        "runtime_reserve_bytes": runtime_reserve_bytes,
        "total_bytes": total_bytes,
        "budget_bytes": budget_bytes,
        "eligible": total_bytes < budget_bytes,
    }


def _recall_vector(samples: Sequence[object], field: str) -> list[int]:
    values: list[int] = []
    for sample in samples:
        if not isinstance(sample, dict):
            raise ValueError("arm sample schema differs")
        values.append(_integer(sample.get(field), field))
    return values


def validate_screen_result(
    result: object,
    *,
    page_by_id: Mapping[int, tuple[str, int]],
    page_bytes: Mapping[tuple[str, int], int],
    page_ranges: Mapping[tuple[str, int], tuple[int, int]],
    expected_authority: object | None = None,
    expected_truth_ids: Sequence[Sequence[int]] | None = None,
) -> dict[str, object]:
    """Independently recompute the complete G1 screen and winner."""

    expected_keys = {
        "arms",
        "artifacts",
        "authority",
        "bootstrap_resamples",
        "bootstrap_seed",
        "exact_f32",
        "max_bytes",
        "max_gets",
        "neighbors",
        "queries",
        "schema",
        "shortlist_rows",
        "summary_page_limit",
        "winner",
    }
    if not isinstance(result, dict) or set(result) != expected_keys:
        raise ValueError("screen result schema differs")
    if result["schema"] != "borsuk-v97-row-width-screen-v2":
        raise ValueError("screen result schema differs")
    if (
        _integer(result["summary_page_limit"], "summary page limit") != 1024
        or _integer(result["shortlist_rows"], "shortlist rows") != 8192
    ):
        raise ValueError("screen routing limits differ")
    artifacts = result["artifacts"]
    expected_artifact_roles = {
        "summary-router",
        "pq16x8",
        "pq24x8",
        "pq32x8",
        "pq32x4",
    }
    if not isinstance(artifacts, dict) or set(artifacts) != expected_artifact_roles:
        raise ValueError("screen artifact identity differs")
    for artifact in artifacts.values():
        if not isinstance(artifact, dict) or set(artifact) != {"codebook", "codes"}:
            raise ValueError("screen artifact identity differs")
        for role, identity in artifact.items():
            if not isinstance(identity, dict) or set(identity) != {
                "bytes",
                "dtype",
                "sha256",
                "shape",
            }:
                raise ValueError("screen artifact identity differs")
            dtype = identity["dtype"]
            shape = identity["shape"]
            item_bytes = 4 if dtype == "float32" else 1 if dtype == "uint8" else 0
            if (
                type(identity["bytes"]) is not int
                or identity["bytes"] <= 0
                or not isinstance(shape, list)
                or not shape
                or any(type(value) is not int or value <= 0 for value in shape)
                or identity["bytes"] != item_bytes * int(np.prod(shape))
                or not isinstance(identity["sha256"], str)
                or len(identity["sha256"]) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in identity["sha256"]
                )
                or (role == "codebook" and dtype != "float32")
                or (role == "codes" and dtype != "uint8")
            ):
                raise ValueError("screen artifact identity differs")
    authority = result["authority"]
    authority_keys = {
        "critique_result_sha256",
        "dimensions",
        "identities",
        "page_map_sha256",
        "seed",
        "source_commit",
        "training_declaration",
    }
    if (
        not isinstance(authority, dict)
        or set(authority) != authority_keys
        or authority["training_declaration"] != "source-only-base-tier"
        or type(authority["dimensions"]) is not int
        or authority["dimensions"] <= 0
        or type(authority["seed"]) is not int
        or not isinstance(authority["source_commit"], str)
        or len(authority["source_commit"]) != 40
        or any(
            character not in "0123456789abcdef"
            for character in authority["source_commit"]
        )
    ):
        raise ValueError("screen authority differs")
    for digest_role in ("critique_result_sha256", "page_map_sha256"):
        digest = authority[digest_role]
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ValueError("screen authority differs")
    identities = authority["identities"]
    if not isinstance(identities, dict) or set(identities) != {
        "source",
        "queries",
        "truth",
        "generation",
        "base",
        "delta",
    }:
        raise ValueError("screen authority differs")
    for identity in identities.values():
        if (
            not isinstance(identity, dict)
            or set(identity) != {"bytes", "sha256", "uri"}
            or type(identity["bytes"]) is not int
            or identity["bytes"] <= 0
            or not isinstance(identity["uri"], str)
            or not identity["uri"].startswith("s3://")
            or not isinstance(identity["sha256"], str)
            or len(identity["sha256"]) != 64
            or any(
                character not in "0123456789abcdef"
                for character in identity["sha256"]
            )
        ):
            raise ValueError("screen authority differs")
    if expected_authority is not None and authority != expected_authority:
        raise ValueError("screen authority differs")
    queries = _integer(result["queries"], "screen queries")
    neighbors = _integer(result["neighbors"], "screen neighbors")
    max_gets = _integer(result["max_gets"], "screen GET budget")
    max_bytes = _integer(result["max_bytes"], "screen byte budget")
    seed = _integer(result["bootstrap_seed"], "bootstrap seed")
    resamples = _integer(result["bootstrap_resamples"], "bootstrap resamples")
    if resamples != 10_000:
        raise ValueError("bootstrap resamples differ")

    exact = result["exact_f32"]
    if not isinstance(exact, dict) or set(exact) != {"aggregate", "samples"}:
        raise ValueError("exact diagnostic schema differs")
    exact_summary = validate_arm_evidence(
        exact["samples"],
        page_by_id=page_by_id,
        page_bytes=page_bytes,
        page_ranges=page_ranges,
        expected_queries=queries,
        neighbors=neighbors,
        max_gets=max_gets,
        max_bytes=max_bytes,
    )
    if exact["aggregate"] != asdict(exact_summary):
        raise ValueError("exact diagnostic aggregate differs")
    exact_truth = [sample["truth_ids"] for sample in exact["samples"]]
    if expected_truth_ids is not None and exact_truth != [
        [int(row_id) for row_id in row] for row in expected_truth_ids
    ]:
        raise ValueError("registered truth differs")

    arms = result["arms"]
    if not isinstance(arms, list) or [arm.get("name") for arm in arms if isinstance(arm, dict)] != list(_ARM_SPECS):
        raise ValueError("screen arm roster differs")
    control_samples = arms[0].get("samples")
    if not isinstance(control_samples, list):
        raise ValueError("screen arm roster differs")
    control_recall10 = _recall_vector(control_samples, "recall10_ppm")
    control_recall100 = _recall_vector(control_samples, "recall100_ppm")
    summaries: dict[str, ArmEvidenceSummary] = {}
    paired_r100_ci: dict[str, tuple[int, int]] = {}
    for arm in arms:
        if not isinstance(arm, dict) or set(arm) != {
            "aggregate",
            "name",
            "paired_vs_pq16",
            "projection",
            "row_bytes",
            "samples",
        }:
            raise ValueError("screen arm schema differs")
        name = arm["name"]
        if name not in _ARM_SPECS:
            raise ValueError("screen arm roster differs")
        expected_row_bytes = _ARM_SPECS[name][2]
        if _integer(arm["row_bytes"], "arm row bytes") != expected_row_bytes:
            raise ValueError("arm row bytes differ")
        summary = validate_arm_evidence(
            arm["samples"],
            page_by_id=page_by_id,
            page_bytes=page_bytes,
            page_ranges=page_ranges,
            expected_queries=queries,
            neighbors=neighbors,
            max_gets=max_gets,
            max_bytes=max_bytes,
        )
        if arm["aggregate"] != asdict(summary):
            raise ValueError("arm aggregate differs")
        if [sample["truth_ids"] for sample in arm["samples"]] != exact_truth:
            raise ValueError("arm truth differs")
        if arm["projection"] != _resident_projection(name):
            raise ValueError("resident projection differs")
        recalls10 = _recall_vector(arm["samples"], "recall10_ppm")
        recalls100 = _recall_vector(arm["samples"], "recall100_ppm")
        expected_ci = {
            "average_recall10_ppm": list(
                paired_bootstrap_interval(
                    recalls10,
                    control_recall10,
                    seed=seed,
                    resamples=resamples,
                    statistic="mean",
                )
            ),
            "average_recall100_ppm": list(
                paired_bootstrap_interval(
                    recalls100,
                    control_recall100,
                    seed=seed,
                    resamples=resamples,
                    statistic="mean",
                )
            ),
            "p05_recall100_ppm": list(
                paired_bootstrap_interval(
                    recalls100,
                    control_recall100,
                    seed=seed,
                    resamples=resamples,
                    statistic="p05",
                )
            ),
        }
        if arm["paired_vs_pq16"] != expected_ci:
            raise ValueError("paired confidence interval differs")
        summaries[name] = summary
        paired_r100_ci[name] = tuple(expected_ci["average_recall100_ppm"])

    winner = choose_screen_winner(
        summaries,
        projections={
            name: bool(_resident_projection(name)["eligible"]) for name in summaries
        },
        paired_r100_ci=paired_r100_ci,
        exact_gate_passed=(
            exact_summary.quality_gate_passed and exact_summary.resource_gate_passed
        ),
    )
    if result["winner"] != winner:
        raise ValueError("screen winner differs")
    return {
        "exact_f32": asdict(exact_summary),
        "arms": {name: asdict(summary) for name, summary in summaries.items()},
        "winner": winner,
    }


def rescore_screen_result(
    result_path: pathlib.Path,
    *,
    expected_sha256: str,
    page_by_id: Mapping[int, tuple[str, int]],
    page_bytes: Mapping[tuple[str, int], int],
    page_ranges: Mapping[tuple[str, int], tuple[int, int]],
    expected_authority: object,
    expected_truth_ids: Sequence[Sequence[int]] | None = None,
    output: pathlib.Path,
) -> dict[str, object]:
    """Authenticate canonical producer bytes and write independent evidence."""

    body = result_path.read_bytes()
    if (
        len(expected_sha256) != 64
        or hashlib.sha256(body).hexdigest() != expected_sha256
    ):
        raise ValueError("screen result identity differs")
    result = json.loads(body)
    if (
        json.dumps(result, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        != body
    ):
        raise ValueError("screen result canonical bytes differ")
    summary = validate_screen_result(
        result,
        page_by_id=page_by_id,
        page_bytes=page_bytes,
        page_ranges=page_ranges,
        expected_authority=expected_authority,
        expected_truth_ids=expected_truth_ids,
    )
    receipt = {
        "arms": summary["arms"],
        "exact_f32": summary["exact_f32"],
        "result_sha256": expected_sha256,
        "schema": "borsuk-v97-row-width-rescore-v2",
        "winner": summary["winner"],
    }
    output.write_bytes(
        json.dumps(receipt, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )
    return receipt


def parse_rescore_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    for role in ("source", "queries", "truth", "generation", "base", "delta"):
        parser.add_argument(f"--{role}", type=pathlib.Path, required=True)
        parser.add_argument(f"--{role}-uri", required=True)
        parser.add_argument(f"--{role}-sha256", required=True)
        parser.add_argument(f"--{role}-bytes", type=int, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--critique-result-sha256", required=True)
    parser.add_argument("--bootstrap-resamples", type=int, required=True)
    parser.add_argument("--result", type=pathlib.Path, required=True)
    parser.add_argument("--result-sha256", required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--dimensions", type=int, default=768)
    parser.add_argument("--neighbors", type=int, default=100)
    parser.add_argument("--query-count", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=7216)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    from scripts.v97_row_width_screen import (
        ObjectIdentity,
        ScreenAuthority,
        load_screen_inputs,
        page_map_sha256,
    )

    args = parse_rescore_args(argv)
    if args.bootstrap_resamples != 10_000:
        raise ValueError("bootstrap resamples differ")
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
    authority = ScreenAuthority(
        source_commit=args.source_commit,
        critique_result_sha256=args.critique_result_sha256,
        page_map_sha256=page_map_sha256(inputs),
        dimensions=args.dimensions,
        seed=args.seed,
        identities=identities,
    )
    expected_authority = {
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
    }
    receipt = rescore_screen_result(
        args.result,
        expected_sha256=args.result_sha256,
        page_by_id={
            row_id: (key.object_role, key.ordinal)
            for row_id, key in inputs.page_by_id.items()
        },
        page_bytes={
            (key.object_role, key.ordinal): page.encoded_bytes
            for key, page in inputs.pages.items()
        },
        page_ranges={
            (key.object_role, key.ordinal): (page.offset, page.encoded_bytes)
            for key, page in inputs.pages.items()
        },
        expected_authority=expected_authority,
        expected_truth_ids=inputs.truth_ids,
        output=args.output,
    )
    print(
        json.dumps(
            {
                "output_bytes": args.output.stat().st_size,
                "output_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
                "winner": receipt["winner"],
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
