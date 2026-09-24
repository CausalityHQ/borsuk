"""Development-only cross-corpus source-tier precision and interval study."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np

WIDTH = 512
RETURN = 100
QUERIES = 1_000
SLACK = 1e-12


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(4 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def rank(ids: np.ndarray, scores: np.ndarray, count: int = RETURN) -> np.ndarray:
    if (ids.ndim != 1 or scores.shape != ids.shape or ids.dtype != np.int64
            or not np.isfinite(scores).all() or not 0 < count <= ids.size):
        raise ValueError("source-tier ranking inputs differ")
    return ids[np.lexsort((ids, -scores))[:count]]


def unit(vectors: np.ndarray) -> np.ndarray:
    norm = np.linalg.norm(vectors, axis=1)
    if not np.isfinite(norm).all() or np.any(norm <= 0):
        raise ValueError("source-tier decoded norm differs")
    return vectors / norm[:, None]


def decode_int16(vectors: np.ndarray) -> np.ndarray:
    scale = np.max(np.abs(vectors), axis=1) / 32767.0
    if not np.isfinite(scale).all() or np.any(scale <= 0):
        raise ValueError("source-tier int16 scale differs")
    codes = np.rint(vectors / scale[:, None]).clip(-32767, 32767).astype(np.int16)
    return codes.astype(np.float64) * scale[:, None]


def mapped_nominees(layout: np.ndarray, source_ids: np.ndarray,
                    physical: list[int]) -> tuple[np.ndarray, np.ndarray]:
    """Resolve physical ordinals through layout and source row identities."""
    ordinals = np.asarray(layout[np.asarray(physical, dtype=np.int64)],
                          dtype=np.int64)
    ids = np.asarray(source_ids[ordinals], dtype=np.int64)
    if np.unique(ids).size != len(physical):
        raise ValueError("source-tier mapped nominee IDs differ")
    return ordinals, ids


def certified_refinement(
    ids: np.ndarray, exact: np.ndarray, approx: np.ndarray,
    delta: np.ndarray,
) -> tuple[int, np.ndarray, float]:
    """Retain every possible exact top-100 under sound score intervals."""
    if (ids.size != WIDTH or exact.shape != ids.shape
            or approx.shape != ids.shape or delta.shape != ids.shape
            or not np.isfinite(exact).all() or not np.isfinite(approx).all()
            or not np.isfinite(delta).all() or np.any(delta < 0)
            or np.any(np.abs(exact - approx) > delta)):
        raise ValueError("source-tier score interval is unsound")
    lower = approx - delta
    upper = approx + delta
    threshold = float(np.partition(lower, -RETURN)[-RETURN])
    eligible = upper >= threshold
    refined = rank(ids[eligible], exact[eligible])
    if not np.array_equal(refined, rank(ids, exact)):
        raise ValueError("source-tier certified refinement differs")
    return int(np.count_nonzero(eligible)), refined, threshold


def _lines(path: Path):
    with path.open() as handle:
        for line in handle:
            yield json.loads(line)


def load_truth(path: Path, source_ids: np.ndarray) -> list[list[int]]:
    """Accept list-form or flat 100-neighbor rows, preserving source IDs."""
    import pyarrow.parquet as pq

    schema = pq.read_schema(path)
    if "neighbors_id" in schema.names:
        truth = pq.read_table(path, columns=["neighbors_id"])[
            "neighbors_id"].slice(0, QUERIES).to_pylist()
    elif "feature_row_id" in schema.names:
        values = pq.read_table(path, columns=["feature_row_id"])[
            "feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
        if values.size != QUERIES * RETURN:
            raise ValueError("source-tier flat GT count differs")
        truth = values.reshape(QUERIES, RETURN).tolist()
    else:
        raise ValueError("source-tier GT schema differs")
    if (len(truth) != QUERIES or any(len(item) < RETURN
            or len(set(item[:RETURN])) != RETURN
            or any(type(value) is not int or value < 0
                   for value in item[:RETURN]) for item in truth)):
        raise ValueError("source-tier GT roster differs")
    flat = np.asarray([value for row in truth for value in row[:RETURN]],
                      dtype=np.int64)
    sorted_ids = np.sort(source_ids.astype(np.int64))
    positions = np.searchsorted(sorted_ids, flat)
    if np.any(positions >= sorted_ids.size) or not np.array_equal(
        sorted_ids[np.minimum(positions, sorted_ids.size - 1)], flat,
    ):
        raise ValueError("source-tier GT ID is absent from source")
    return truth


def evaluate(
    *, cohort: str, rows: int, dimensions: int, source_path: Path,
    layout_path: Path, requests_path: Path, truth_path: Path,
    evidence_path: Path, summary_path: Path,
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pq.read_table(source_path, columns=["feature_row_id", "embedding"])
    embedding = table.schema.field("embedding").type
    if (table.num_rows != rows or not pa.types.is_fixed_size_list(embedding)
            or embedding.list_size != dimensions
            or embedding.value_type != pa.float32()):
        raise ValueError("source-tier source schema differs")
    source_ids = table["feature_row_id"].combine_chunks().to_numpy(
        zero_copy_only=False,
    )
    if (not np.issubdtype(source_ids.dtype, np.integer)
            or source_ids.min() < 0 or source_ids.max() > np.iinfo(np.int64).max
            or np.unique(source_ids).size != rows):
        raise ValueError("source-tier source IDs differ")
    source = np.asarray(table["embedding"].combine_chunks().values.to_numpy(
        zero_copy_only=False,
    ), dtype=np.float32).reshape(rows, dimensions)
    if not np.isfinite(source).all():
        raise ValueError("source-tier source coordinates differ")
    source_norm = np.linalg.norm(source, axis=1)
    if not np.isfinite(source_norm).all() or np.any(source_norm <= 0):
        raise ValueError("source-tier source norm differs")
    layout = np.load(layout_path, mmap_mode="r", allow_pickle=False)
    if (layout.shape != (rows,) or layout.dtype not in (np.dtype("int32"), np.dtype("int64"))
            or layout.min() != 0 or layout.max() != rows - 1
            or np.unique(layout).size != rows):
        raise ValueError("source-tier layout permutation differs")
    gold = load_truth(truth_path, source_ids)

    values: dict[str, list[int | float]] = {name: [] for name in (
        "capture", "reference_hits", "fp16_hits", "int16_hits",
        "fp16_disagreement", "int16_disagreement", "fp16_refine",
        "int16_refine", "fp16_net_loss", "int16_net_loss", "margin",
    )}
    with evidence_path.open("x") as evidence:
        for ordinal, (request, truth) in enumerate(zip(
            _lines(requests_path), gold, strict=True,
        )):
            physical = request.get("nominees")
            if (ordinal >= QUERIES or request.get("query_ordinal") != ordinal
                    or type(physical) is not list or len(physical) != WIDTH
                    or len(set(physical)) != WIDTH
                    or any(type(value) is not int or value < 0 or value >= rows
                           for value in physical)):
                raise ValueError(f"source-tier nominee roster differs at {ordinal}")
            query = np.asarray(request.get("query"), dtype=np.float64)
            if (query.shape != (dimensions,) or not np.isfinite(query).all()
                    or np.linalg.norm(query) <= 0):
                raise ValueError(f"source-tier query differs at {ordinal}")
            query /= np.linalg.norm(query)
            source_ordinals, ids = mapped_nominees(layout, source_ids, physical)
            selected = source[source_ordinals]
            original = unit(selected.astype(np.float64))
            exact = original @ query
            reference = rank(ids, exact)
            truth_set = set(map(int, truth[:RETURN]))
            order = np.lexsort((ids, -exact))
            margin = float(exact[order[RETURN - 1]] - exact[order[RETURN]])
            result = {"query_ordinal": ordinal,
                      "capture": len(truth_set & set(map(int, ids))),
                      "reference_ids": reference.tolist(),
                      "reference_hits": len(truth_set & set(map(int, reference))),
                      "margin": margin, "arms": {}}
            for name, decoded in (
                ("fp16", selected.astype(np.float16).astype(np.float64)),
                ("int16", decode_int16(selected.astype(np.float64))),
            ):
                normalized = unit(decoded)
                approx = normalized @ query
                delta = np.linalg.norm(normalized - original, axis=1) + SLACK
                refine_count, refined, threshold = certified_refinement(
                    ids, exact, approx, delta,
                )
                returned = rank(ids, approx)
                returned_set = set(map(int, returned))
                reference_set = set(map(int, reference))
                arm = {"returned_ids": returned.tolist(),
                       "hits": len(truth_set & returned_set),
                       "disagreement": len(reference_set - returned_set),
                       "refine_count": refine_count,
                       "threshold": threshold,
                       "max_delta": float(delta.max()),
                       "certified_reference_ids": refined.tolist()}
                result["arms"][name] = arm
                values[f"{name}_hits"].append(arm["hits"])
                values[f"{name}_disagreement"].append(arm["disagreement"])
                values[f"{name}_refine"].append(refine_count)
                values[f"{name}_net_loss"].append(
                    result["reference_hits"] - arm["hits"])
            for key in ("capture", "reference_hits", "margin"):
                values[key].append(result[key])
            evidence.write(json.dumps(result, sort_keys=True,
                                      separators=(",", ":")) + "\n")
    if len(values["capture"]) != QUERIES:
        raise ValueError("source-tier query count differs")
    summary: dict[str, object] = {
        "schema": "borsuk-v124-source-tier-precision-v1",
        "cohort": cohort, "rows": rows, "dimensions": dimensions,
        "split": "already-used-development", "query_count": QUERIES,
        "source_sha256": sha256(source_path),
        "layout_sha256": sha256(layout_path),
        "requests_sha256": sha256(requests_path),
        "truth_sha256": sha256(truth_path),
        "evidence_sha256": sha256(evidence_path),
        "modeled_float64_kernel": True, "live_serving_measured": False,
    }
    for key, series in values.items():
        ordered = sorted(series)
        summary[key + "_total"] = sum(series)
        summary[key + "_p05"] = ordered[49]
        summary[key + "_p50"] = ordered[499]
        summary[key + "_p95"] = ordered[949]
        summary[key + "_max"] = ordered[-1]
    summary_path.write_text(json.dumps(summary, sort_keys=True,
                                       separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cohort", required=True)
    parser.add_argument("--rows", required=True, type=int)
    parser.add_argument("--dimensions", required=True, type=int)
    for name in ("source", "layout", "requests", "truth", "evidence", "summary"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    evaluate(cohort=args.cohort, rows=args.rows, dimensions=args.dimensions,
             source_path=args.source, layout_path=args.layout,
             requests_path=args.requests, truth_path=args.truth,
             evidence_path=args.evidence, summary_path=args.summary)


if __name__ == "__main__":
    main()
