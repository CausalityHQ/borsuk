"""Fixed-width, expansion-aware source-score diagnostic for sealed V116."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.v124_source_tier_precision import (
    load_truth,
    mapped_nominees,
    rank,
    sha256,
    unit,
)

ROWS = 1_000_000
DIMENSIONS = 768
QUERIES = 1_000
NOMINEES = 512
WIDE = 512
RETURN = 100
V124_NOMINEE_CAPTURE = 96_849
V124_NOMINEE_EXACT = 96_813


def lines(path: Path):
    with path.open() as handle:
        for line in handle:
            yield json.loads(line)


def validate_replay_prefix(
    request: dict, sealed: dict, wide: dict, ordinal: int
) -> None:
    """A widened Rust replay may only extend the sealed top-100."""
    if any(item.get("query_ordinal") != ordinal for item in (request, sealed, wide)):
        raise ValueError(f"V126 query ordinal differs at {ordinal}")
    for field in (
        "nominees",
        "score_bits",
        "primary",
        "page_votes",
        "ranges",
        "plan_bytes",
        "plan_score",
        "baseline_ranges",
        "baseline_bytes",
    ):
        if sealed.get(field) != wide.get(field):
            raise ValueError(f"V126 sealed {field} differs at {ordinal}")
    for field in ("nominees", "baseline_ranges"):
        if request.get(field) != wide.get(field):
            raise ValueError(f"V126 request {field} differs at {ordinal}")
    for field in ("returned_ids", "baseline_returned_ids"):
        old = sealed.get(field)
        extended = wide.get(field)
        if (
            type(old) is not list
            or len(old) != RETURN
            or type(extended) is not list
            or len(extended) != WIDE
            or len(set(extended)) != WIDE
            or extended[:RETURN] != old
        ):
            raise ValueError(f"V126 sealed {field} prefix differs at {ordinal}")
    for field, byte_field in (
        ("ranges", "plan_bytes"),
        ("baseline_ranges", "baseline_bytes"),
    ):
        pairs = wide[field]
        if (
            type(pairs) is not list
            or not 1 <= len(pairs) <= 32
            or any(
                type(pair) is not list
                or len(pair) != 2
                or type(pair[0]) is not int
                or type(pair[1]) is not int
                or pair[0] < 0
                or pair[1] <= pair[0]
                for pair in pairs
            )
        ):
            raise ValueError(f"V126 {field} interval differs at {ordinal}")
        if sum(end - start for start, end in pairs) != wide[byte_field]:
            raise ValueError(f"V126 {field} byte count differs at {ordinal}")
    if wide["plan_bytes"] > 16_777_216 or wide["baseline_bytes"] > 16_777_216:
        raise ValueError(f"V126 physical byte cap differs at {ordinal}")


def score_union(
    source: np.ndarray,
    ids_to_rows: dict[int, int],
    union_ids: set[int],
    query: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Same source scorer for candidate and capped-control fixed unions."""
    ids = np.asarray(sorted(union_ids), dtype=np.int64)
    if ids.size < RETURN or any(int(value) not in ids_to_rows for value in ids):
        raise ValueError("V126 union source IDs differ")
    ordinals = np.fromiter(
        (ids_to_rows[int(value)] for value in ids), dtype=np.int64, count=ids.size
    )
    selected = source[ordinals]
    exact = unit(selected.astype(np.float64)) @ query
    fp16 = unit(selected.astype(np.float16).astype(np.float64)) @ query
    return rank(ids, exact, RETURN), rank(ids, fp16, RETURN)


def statistics(values: list[int]) -> dict[str, int]:
    if len(values) != QUERIES:
        raise ValueError("V126 statistic row count differs")
    ordered = sorted(values)
    return {
        "total": sum(values),
        "p05": ordered[49],
        "median": ordered[499],
        "p95": ordered[949],
        "max": ordered[-1],
        "sub90": sum(value < 90 for value in values),
    }


def preflight(requests_path: Path, sealed_path: Path, wide_path: Path) -> None:
    count = 0
    for ordinal, items in enumerate(
        zip(lines(requests_path), lines(sealed_path), lines(wide_path), strict=True)
    ):
        if ordinal >= QUERIES:
            raise ValueError("V126 preflight request count exceeds 1000")
        validate_replay_prefix(*items, ordinal)
        count += 1
    if count != QUERIES:
        raise ValueError("V126 preflight request count differs")


def evaluate(
    *,
    source_path: Path,
    layout_path: Path,
    requests_path: Path,
    sealed_path: Path,
    wide_path: Path,
    truth_path: Path,
    evidence_path: Path,
    summary_path: Path,
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pq.read_table(source_path, columns=["feature_row_id", "embedding"])
    embedding = table.schema.field("embedding").type
    if (
        table.num_rows != ROWS
        or not pa.types.is_fixed_size_list(embedding)
        or embedding.list_size != DIMENSIONS
        or embedding.value_type != pa.float32()
    ):
        raise ValueError("V126 source geometry differs")
    source_ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if (
        not np.issubdtype(source_ids.dtype, np.integer)
        or source_ids.min() < 0
        or source_ids.max() > np.iinfo(np.int64).max
        or np.unique(source_ids).size != ROWS
    ):
        raise ValueError("V126 source IDs differ")
    source = np.asarray(
        table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False),
        dtype=np.float32,
    ).reshape(ROWS, DIMENSIONS)
    if not np.isfinite(source).all():
        raise ValueError("V126 source coordinates differ")
    layout = np.load(layout_path, mmap_mode="r", allow_pickle=False)
    if (
        layout.shape != (ROWS,)
        or layout.dtype not in (np.dtype("int32"), np.dtype("int64"))
        or layout.min() != 0
        or layout.max() != ROWS - 1
        or np.unique(layout).size != ROWS
    ):
        raise ValueError("V126 layout permutation differs")
    gold = load_truth(truth_path, source_ids)
    ids_to_rows = {int(value): ordinal for ordinal, value in enumerate(source_ids)}
    counts = {
        arm: {
            metric: []
            for metric in (
                "capture",
                "exact_hits",
                "fp16_hits",
                "fp16_disagreement",
                "union_size",
                "sq8_hits",
                "gets",
                "planned_bytes",
            )
        }
        for arm in ("candidate", "control")
    }
    nominee_capture, nominee_exact = [], []
    paired_exact_wins = [0, 0, 0]
    with evidence_path.open("x") as evidence:
        for ordinal, (request, sealed, wide, truth) in enumerate(
            zip(
                lines(requests_path),
                lines(sealed_path),
                lines(wide_path),
                gold,
                strict=True,
            )
        ):
            if ordinal >= QUERIES:
                raise ValueError("V126 request count exceeds 1000")
            validate_replay_prefix(request, sealed, wide, ordinal)
            physical = request.get("nominees")
            if (
                type(physical) is not list
                or len(physical) != NOMINEES
                or len(set(physical)) != NOMINEES
                or any(
                    type(value) is not int or value < 0 or value >= ROWS
                    for value in physical
                )
            ):
                raise ValueError(f"V126 nominee roster differs at {ordinal}")
            query = np.asarray(request.get("query"), dtype=np.float64)
            if (
                query.shape != (DIMENSIONS,)
                or not np.isfinite(query).all()
                or np.linalg.norm(query) <= 0
            ):
                raise ValueError(f"V126 query differs at {ordinal}")
            query /= np.linalg.norm(query)
            source_ordinals, nominee_ids = mapped_nominees(layout, source_ids, physical)
            nominee_set = set(map(int, nominee_ids))
            truth_set = set(map(int, truth[:RETURN]))
            nominee_capture.append(len(nominee_set & truth_set))
            nominee_rank = rank(
                nominee_ids,
                unit(source[source_ordinals].astype(np.float64)) @ query,
                RETURN,
            )
            nominee_exact.append(len(set(map(int, nominee_rank)) & truth_set))
            row = {
                "query_ordinal": ordinal,
                "nominee_capture": nominee_capture[-1],
                "nominee_exact_hits": nominee_exact[-1],
                "arms": {},
            }
            for arm, field in (
                ("candidate", "returned_ids"),
                ("control", "baseline_returned_ids"),
            ):
                ranges_field = "ranges" if arm == "candidate" else "baseline_ranges"
                bytes_field = "plan_bytes" if arm == "candidate" else "baseline_bytes"
                union = nominee_set | set(wide[field])
                exact, fp16 = score_union(source, ids_to_rows, union, query)
                exact_ids = set(map(int, exact))
                fp16_ids = set(map(int, fp16))
                values = {
                    "union_ids": sorted(union),
                    "capture": len(union & truth_set),
                    "exact_returned_ids": exact.tolist(),
                    "exact_hits": len(exact_ids & truth_set),
                    "fp16_returned_ids": fp16.tolist(),
                    "fp16_hits": len(fp16_ids & truth_set),
                    "fp16_disagreement": len(exact_ids - fp16_ids),
                    "sq8_returned_ids": sealed[field],
                    "sq8_hits": len(set(sealed[field]) & truth_set),
                    "gets": len(wide[ranges_field]),
                    "planned_bytes": wide[bytes_field],
                }
                row["arms"][arm] = values
                for metric in counts[arm]:
                    counts[arm][metric].append(
                        len(union) if metric == "union_size" else values[metric]
                    )
            candidate_hits = row["arms"]["candidate"]["exact_hits"]
            control_hits = row["arms"]["control"]["exact_hits"]
            paired_exact_wins[
                (candidate_hits > control_hits) + 2 * (candidate_hits < control_hits)
            ] += 1
            evidence.write(
                json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
            )
    if (
        sum(nominee_capture) != V124_NOMINEE_CAPTURE
        or sum(nominee_exact) != V124_NOMINEE_EXACT
    ):
        raise ValueError("V126 V124 nominee reproduction differs")
    if (
        sum(counts["candidate"]["sq8_hits"]) != 99_208
        or sum(counts["control"]["sq8_hits"]) != 98_618
    ):
        raise ValueError("V126 V116 SQ8 baseline reproduction differs")
    summary = {
        "schema": "borsuk-v126-expansion-source-development-v1",
        "dataset": "ReLAION-1M",
        "split": "validation-1000-already-used",
        "rows": ROWS,
        "dimensions": DIMENSIONS,
        "query_count": QUERIES,
        "wide_sq8_top_k": WIDE,
        "nominee_width": NOMINEES,
        "source_sha256": sha256(source_path),
        "layout_sha256": sha256(layout_path),
        "requests_sha256": sha256(requests_path),
        "sealed_replay_sha256": sha256(sealed_path),
        "wide_replay_sha256": sha256(wide_path),
        "truth_sha256": sha256(truth_path),
        "evidence_sha256": sha256(evidence_path),
        "nominee_capture_total": sum(nominee_capture),
        "nominee_exact_hits_total": sum(nominee_exact),
        "candidate_exact_better_queries": paired_exact_wins[1],
        "equal_queries": paired_exact_wins[0],
        "control_exact_better_queries": paired_exact_wins[2],
        "arms": {
            arm: {metric: statistics(series) for metric, series in metrics.items()}
            for arm, metrics in counts.items()
        },
        "physical_caps_validated": True,
        "live_s3_measured": False,
    }
    candidate = summary["arms"]["candidate"]
    control = summary["arms"]["control"]
    summary["qualifies_fixed_expansion_diagnostic"] = (
        candidate["exact_hits"]["total"] >= 99_000
        and candidate["exact_hits"]["total"] >= control["exact_hits"]["total"]
        and candidate["exact_hits"]["p05"] >= 90
        and candidate["exact_hits"]["sub90"] <= control["exact_hits"]["sub90"]
    )
    summary["fp16_within_50_hits"] = (
        candidate["exact_hits"]["total"] - candidate["fp16_hits"]["total"] <= 50
    )
    summary_path.write_text(
        json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight", action="store_true")
    for name in (
        "source",
        "layout",
        "requests",
        "sealed",
        "wide",
        "truth",
        "evidence",
        "summary",
    ):
        parser.add_argument(
            "--" + name, type=Path, required=name in ("requests", "sealed", "wide")
        )
    args = parser.parse_args()
    if args.preflight:
        preflight(args.requests, args.sealed, args.wide)
        return
    if any(
        getattr(args, name) is None
        for name in ("source", "layout", "truth", "evidence", "summary")
    ):
        parser.error(
            "full evaluation requires source, layout, truth, evidence, summary"
        )
    evaluate(
        source_path=args.source,
        layout_path=args.layout,
        requests_path=args.requests,
        sealed_path=args.sealed,
        wide_path=args.wide,
        truth_path=args.truth,
        evidence_path=args.evidence,
        summary_path=args.summary,
    )


if __name__ == "__main__":
    main()
