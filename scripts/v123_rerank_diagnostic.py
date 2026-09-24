"""Postmortem FP16 and float32 rerank of sealed V121 candidates."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np

ROWS = 9_990_000
DIMENSIONS = 96
WIDTHS = (0, 100, 256, 512, 1024, 1600)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(4 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def candidate_ids(layout: np.ndarray, nominees: list[int],
                  returned: list[int], width: int) -> np.ndarray:
    """Map physical router ordinals to source IDs, then form a stable union."""
    physical = np.asarray(nominees, dtype=np.int64)
    if (physical.ndim != 1 or physical.size == 0
            or np.any(physical < 0) or np.any(physical >= layout.size)
            or len(set(map(int, physical))) != physical.size
            or width < 0):
        raise ValueError("diagnostic nominee or width differs")
    source_ids = np.asarray(returned[:width], dtype=np.int64)
    if (np.any(source_ids < 0) or np.any(source_ids >= layout.size)
            or len(set(map(int, source_ids))) != source_ids.size):
        raise ValueError("diagnostic returned IDs differ")
    return np.unique(np.concatenate((layout[physical], source_ids)))


def rank_source(source: np.ndarray, ids: np.ndarray, query: np.ndarray,
                top_k: int, *, half: bool) -> list[int]:
    """Score selected normalized source vectors with float32 or rounded FP16."""
    if (source.ndim != 2 or source.dtype != np.float32
            or ids.ndim != 1 or ids.dtype != np.int64
            or query.shape != (source.shape[1],) or query.dtype != np.float32
            or not 0 < top_k <= ids.size or len(set(map(int, ids))) != ids.size
            or np.any(ids < 0) or np.any(ids >= len(source))
            or not np.isfinite(query).all()):
        raise ValueError("diagnostic source scoring geometry differs")
    vectors = source[ids]
    if half:
        vectors = vectors.astype(np.float16).astype(np.float32)
    scores = vectors @ query
    if not np.isfinite(scores).all():
        raise ValueError("diagnostic source score is nonfinite")
    ordering = np.lexsort((ids, -scores))[:top_k]
    return [int(item) for item in ids[ordering]]


def _rows(path: Path):
    with path.open() as source:
        for line in source:
            yield json.loads(line)


def evaluate(source_path: Path | None, layout_path: Path, built_manifest_path: Path,
             requests_path: Path, sealed_path: Path, wide_path: Path,
             truth_path: Path, evidence_path: Path, summary_path: Path,
             *, capture_only: bool = False,
             capture_evidence_path: Path | None = None) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    built = json.loads(built_manifest_path.read_text())
    if (built.get("rows") != ROWS or built.get("dimensions") != DIMENSIONS
            or (source_path is not None and built.get("source_sha256") != _sha256(source_path))
            or built.get("layout_sha256") != _sha256(layout_path)):
        raise ValueError("diagnostic source or layout binding differs")
    layout = np.load(layout_path, mmap_mode="r", allow_pickle=False)
    if layout.shape != (ROWS,) or layout.dtype != np.int64:
        raise ValueError("diagnostic layout geometry differs")
    source = None
    if not capture_only:
        if source_path is None:
            raise ValueError("source is required for rerank")
        table = pq.read_table(source_path, columns=["feature_row_id", "embedding"])
        field = table.schema.field("embedding")
        if (table.num_rows != ROWS or not pa.types.is_fixed_size_list(field.type)
                or field.type.list_size != DIMENSIONS
                or field.type.value_type != pa.float32()):
            raise ValueError("diagnostic source geometry differs")
        source_ids = table["feature_row_id"].combine_chunks().to_numpy(
            zero_copy_only=False,
        )
        if not np.array_equal(source_ids, np.arange(ROWS, dtype=source_ids.dtype)):
            raise ValueError("diagnostic source IDs differ")
        source = np.asarray(table["embedding"].combine_chunks().values.to_numpy(
            zero_copy_only=False,
        ), dtype=np.float32).reshape(ROWS, DIMENSIONS)
    gold = pq.read_table(truth_path, columns=["neighbors_id"])[
        "neighbors_id"].slice(0, 1_000).to_pylist()
    if (len(gold) != 1_000 or any(len(row) < 100 or
            any(type(item) is not int or item < 0 or item >= ROWS for item in row[:100])
            for row in gold)):
        raise ValueError("diagnostic GT count differs")

    series = {arm: {str(width): {metric: [] for metric in
        ("effective_width", "sq8_capture", "union_capture", "f32_hits", "f16_hits", "f16_f32_disagreement")}
        for width in WIDTHS} for arm in ("candidate", "baseline")}
    nominee_capture = []
    with evidence_path.open("x") as evidence:
        iterator = zip(_rows(requests_path), _rows(sealed_path),
                       _rows(wide_path), gold, strict=True)
        for ordinal, (request, sealed, wide, truth) in enumerate(iterator):
            if (ordinal >= 1_000 or request.get("query_ordinal") != ordinal
                    or sealed.get("query_ordinal") != ordinal
                    or wide.get("query_ordinal") != ordinal
                    or request.get("nominees") != sealed.get("nominees")
                    or request.get("nominees") != wide.get("nominees")
                    or sealed.get("primary") != wide.get("primary")
                    or sealed.get("ranges") != wide.get("ranges")
                    or sealed.get("baseline_ranges") != wide.get("baseline_ranges")
                    or sealed.get("plan_bytes") != wide.get("plan_bytes")
                    or sealed.get("baseline_bytes") != wide.get("baseline_bytes")
                    or len(sealed.get("ranges", [])) > 32
                    or len(sealed.get("baseline_ranges", [])) > 32
                    or sealed.get("plan_bytes", 16_777_217) > 16_777_216
                    or sealed.get("baseline_bytes", 16_777_217) > 16_777_216
                    or len(set(truth[:100])) != 100
                    or "baseline_ranges" not in sealed
                    or "baseline_returned_ids" not in sealed
                    or "baseline_bytes" not in sealed):
                raise ValueError(f"diagnostic sealed replay parity differs at {ordinal}")
            query = np.asarray(request["query"], dtype=np.float32)
            if (query.shape != (DIMENSIONS,) or not np.isfinite(query).all()
                    or not np.isclose(np.linalg.norm(query), 1.0, atol=1e-4)):
                raise ValueError(f"diagnostic query differs at {ordinal}")
            nominees = request["nominees"]
            if len(nominees) != 512:
                raise ValueError(f"diagnostic nominee count differs at {ordinal}")
            truth_set = set(map(int, truth[:100]))
            nominee_sources = set(map(int, layout[np.asarray(nominees, np.int64)]))
            nominee_hit = len(truth_set & nominee_sources)
            nominee_capture.append(nominee_hit)
            record = {"query_ordinal": ordinal, "nominee_capture": nominee_hit,
                      "arms": {}}
            for arm, key, original_key, byte_key in (
                ("candidate", "returned_ids", "returned_ids", "plan_bytes"),
                ("baseline", "baseline_returned_ids", "baseline_returned_ids",
                 "baseline_bytes"),
            ):
                if key not in wide:
                    raise ValueError(f"diagnostic {arm} returned IDs missing at {ordinal}")
                returned = wide[key]
                expected_width = min(1_600, sealed[byte_key] // (DIMENSIONS + 12))
                if (len(returned) != expected_width
                        or len(set(returned)) != len(returned)
                        or returned[:100] != sealed[original_key]
                        or any(type(item) is not int or item < 0 or item >= ROWS
                               for item in returned)):
                    raise ValueError(f"diagnostic {arm} top-100 parity differs at {ordinal}")
                record["arms"][arm] = {}
                for width in WIDTHS:
                    union = candidate_ids(layout, nominees, returned, width)
                    values = {
                        "effective_width": min(width, len(returned)),
                        "sq8_capture": len(truth_set & set(returned[:width])),
                        "union_capture": len(truth_set & set(map(int, union))),
                    }
                    if source is not None:
                        top_f32 = rank_source(source, union, query, 100, half=False)
                        top_f16 = rank_source(source, union, query, 100, half=True)
                        values.update({
                            "f32_hits": len(truth_set & set(top_f32)),
                            "f16_hits": len(truth_set & set(top_f16)),
                            "f16_f32_disagreement": 100 - len(set(top_f32) & set(top_f16)),
                        })
                        if values["f32_hits"] > values["union_capture"] or \
                                values["f16_hits"] > values["union_capture"]:
                            raise ValueError("diagnostic exact rank exceeds capture")
                    record["arms"][arm][str(width)] = values
                    for name, value in values.items():
                        series[arm][str(width)][name].append(value)
            evidence.write(json.dumps(record, sort_keys=True,
                                      separators=(",", ":")) + "\n")
    if len(nominee_capture) != 1_000:
        raise ValueError("diagnostic query count differs")
    if capture_evidence_path is not None:
        for captured, reranked in zip(_rows(capture_evidence_path),
                                      _rows(evidence_path), strict=True):
            if (captured["query_ordinal"] != reranked["query_ordinal"]
                    or captured["nominee_capture"] != reranked["nominee_capture"]):
                raise ValueError("capture and rerank query evidence differs")
            for arm in ("candidate", "baseline"):
                for width in map(str, WIDTHS):
                    for metric in ("effective_width", "sq8_capture", "union_capture"):
                        if captured["arms"][arm][width][metric] != \
                                reranked["arms"][arm][width][metric]:
                            raise ValueError("capture and rerank frontier differs")
    out = {arm: {} for arm in series}
    for arm, widths in series.items():
        for width, metrics in widths.items():
            out[arm][width] = {name + "_total": sum(values)
                               for name, values in metrics.items() if values}
            out[arm][width]["effective_width_min"] = min(metrics["effective_width"])
            out[arm][width]["effective_width_max"] = max(metrics["effective_width"])
            for precision in ("f16", "f32"):
                hits = metrics[f"{precision}_hits"]
                if hits:
                    out[arm][width][f"{precision}_p05_hits"] = sorted(hits)[49]
                    out[arm][width][f"{precision}_sub90_queries"] = sum(value < 90 for value in hits)
    paired = {}
    if not capture_only:
        for width in map(str, WIDTHS):
            paired[width] = {}
            for precision in ("f16", "f32"):
                left = series["candidate"][width][f"{precision}_hits"]
                right = series["baseline"][width][f"{precision}_hits"]
                paired[width][precision] = {
                    "candidate_better": sum(a > b for a, b in zip(left, right)),
                    "equal": sum(a == b for a, b in zip(left, right)),
                    "baseline_better": sum(a < b for a, b in zip(left, right)),
                }
    result = out["candidate"]["512"]
    baseline = out["baseline"]["512"]
    supports_followup = (not capture_only and result["f16_hits_total"] >= 99_000
                 and result["f16_p05_hits"] >= 90
                 and abs(result["f32_hits_total"] - result["f16_hits_total"]) <= 50
                 and result["f16_hits_total"] >= baseline["f16_hits_total"]
                 and result["f16_p05_hits"] >= baseline["f16_p05_hits"]
                 and result["f16_sub90_queries"] <= baseline["f16_sub90_queries"])
    summary_path.write_text(json.dumps({
        "schema": "borsuk-v123-rerank-postmortem-v2",
        "phase": "capture" if capture_only else "rerank",
        "baseline": "shared-nominee-hybrid-control-ranges",
        "dataset": "deep-image-96-angular", "split": "test-first-1000-postmortem",
        "query_count": 1_000, "rows": ROWS, "dimensions": DIMENSIONS,
        "widths": list(WIDTHS), "nominee_capture_total": sum(nominee_capture),
        "arms": out, "paired_queries": paired,
        "supports_followup": supports_followup,
        "requests_sha256": _sha256(requests_path),
        "sealed_replay_sha256": _sha256(sealed_path),
        "wide_replay_sha256": _sha256(wide_path),
        "source_sha256": _sha256(source_path) if source_path is not None else None,
        "layout_sha256": _sha256(layout_path),
        "built_manifest_sha256": _sha256(built_manifest_path),
        "truth_sha256": _sha256(truth_path),
        "evidence_sha256": _sha256(evidence_path),
        "live_s3_measured": False, "local_fp16_tier_measured": False,
    }, sort_keys=True, separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("capture", "rerank"), default="rerank")
    parser.add_argument("--source", type=Path)
    parser.add_argument("--capture-evidence", type=Path)
    for name in ("layout", "built_manifest", "requests",
                 "sealed_replay", "wide_replay", "truth", "evidence", "summary"):
        parser.add_argument(f"--{name.replace('_', '-')}", required=True, type=Path)
    args = parser.parse_args()
    evaluate(args.source, args.layout, args.built_manifest, args.requests,
             args.sealed_replay, args.wide_replay, args.truth, args.evidence,
             args.summary, capture_only=args.phase == "capture",
             capture_evidence_path=args.capture_evidence)


if __name__ == "__main__":
    main()
