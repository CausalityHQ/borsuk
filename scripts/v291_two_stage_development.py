"""One frozen local centroid -> two-bit -> SQ8 CoHere development replay."""

import argparse
import json
import math
import tarfile
from pathlib import Path

import numpy as np

from scripts.native_rotated_two_bit_codes import (
    BATCH_ROWS, PACKED_BYTES, _fit_records, decode_levels, rotate_rows,
)
from scripts.v284_page_primary_dev64 import (
    ARCHIVE_SHA, ROOT_SHA_FILE_SHA, covered_pages, sha,
)
from scripts.v285_exact_page_rank_bound import (
    LAYOUT_SHA, RAW_SHA, REQUESTS_SHA, TRUTH_SHA,
)

SQ8_SHA = "301696df05ca03122951b66ad8a9bedb5d5f1e675c6fc66f6019abbce3fcda58"
SEED = 20260923


def rust_sq8_scores(fetched, query, low, step):
    """Match score_nominees' sequential f32 rounding, including shift."""
    shift = np.float32(0)
    qnorm = np.float32(0)
    weights = query * step
    inner = np.zeros(len(fetched), dtype=np.float32)
    for coordinate in range(768):
        shift = np.float32(shift + np.float32(query[coordinate] * low[coordinate]))
        qnorm = np.float32(qnorm + np.float32(query[coordinate] * query[coordinate]))
        inner += fetched["code"][:, coordinate].astype(np.float32) * weights[coordinate]
    shift = np.float32(shift - np.float32(qnorm / np.float32(2)))
    return fetched["norm"] - np.float32(2) * (inner + shift)


def evaluate(raw, archive, root_sha, layout, sq8_path, requests, truth, candidates_path=None):
    assert [sha(p.read_bytes()) for p in (raw, archive, root_sha, layout, sq8_path, requests, truth)] == [
        RAW_SHA, ARCHIVE_SHA, ROOT_SHA_FILE_SHA, LAYOUT_SHA, SQ8_SHA, REQUESTS_SHA, TRUTH_SHA]
    with tarfile.open(archive, "r:gz") as source:
        root_raw = source.extractfile("generation/manifest.json").read()
        assert sha(root_raw) == root_sha.read_text().strip()
        root = json.loads(root_raw)
        blob = source.extractfile("generation/centroids.bin").read()
        assert sha(blob) == root["centroids_sha256"]
        router_raw = source.extractfile("generation/router/manifest.json").read()
        assert sha(router_raw) == root["router_manifest_sha256"]
        router = json.loads(router_raw)
        coefficients = {}
        for name in ("low", "step"):
            body = source.extractfile(f"generation/router/{name}.bin").read()
            assert sha(body) == router["sections"][name]["sha256"]
            assert len(body) == router["sections"][name]["bytes"]
            coefficients[name] = np.frombuffer(body, dtype="<f4").copy()
    assert blob[:8] == b"BORSUCP1" and int.from_bytes(blob[8:16], "little") == 100_000
    assert [int.from_bytes(blob[i:i+4], "little") for i in (16, 20, 24)] == [768, 32, 256]
    centers = np.frombuffer(blob, dtype="<f2", offset=32).astype(np.float32).reshape(3125, 768)
    assert np.isfinite(centers).all()
    center_norms = np.einsum("ij,ij->i", centers, centers)
    vectors = np.memmap(raw, dtype="<f4", mode="r", shape=(100_000, 768))
    order = np.load(layout, allow_pickle=False)
    assert order.shape == (100_000,) and np.array_equal(np.sort(order), np.arange(100_000))
    mean = np.asarray(np.mean(vectors, axis=0, dtype=np.float64), dtype=np.float32)
    low, step = coefficients["low"], coefficients["step"]
    assert low.shape == step.shape == (768,) and np.isfinite(low).all()
    assert np.isfinite(step).all() and (step > 0).all()
    records = np.empty((100_000, 200), dtype=np.uint8)
    for first in range(0, 100_000, BATCH_ROWS):
        last = min(first + BATCH_ROWS, 100_000)
        rows = np.asarray(vectors[order[first:last]], dtype=np.float64)
        assert np.isfinite(rows).all()
        records[first:last] = _fit_records(rows - mean.astype(np.float64), rotation_seed=SEED)
    sq8_type = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (768,))])
    sq8 = np.memmap(sq8_path, dtype=sq8_type, mode="r", shape=(100_000,))
    assert np.array_equal(sq8["id"], order) and np.isfinite(sq8["norm"]).all()
    restored = low + sq8["code"][:256].astype(np.float32) * step
    assert np.allclose(np.einsum("ij,ij->i", restored, restored), sq8["norm"][:256],
                       rtol=1e-5, atol=1e-4)
    req = [json.loads(line) for line in requests.read_text().splitlines()]
    gt = np.fromfile(truth, dtype="<u4").reshape(64, 100)
    assert len(req) == 64 and [q["query_ordinal"] for q in req] == list(range(64))
    assert (gt < 100_000).all()
    q = np.asarray([item["query"] for item in req], dtype=np.float32)
    assert q.shape == (64, 768) and np.isfinite(q).all()
    q_rot = rotate_rows(q.astype(np.float64), rotation_seed=SEED).astype(np.float32)
    m_rot = rotate_rows(mean[None, :].astype(np.float64), rotation_seed=SEED)[0].astype(np.float32)
    candidate_count = math.ceil(8 * math.sqrt(391))
    routed = None
    if candidates_path is not None:
        assert sha(candidates_path.read_bytes()) == "c24e53f8e740ad569331c774f46ef3161873e3a765d20a345290af50a1b974d0"
        routed = [json.loads(line) for line in candidates_path.read_text().splitlines()]
        assert len(routed) == 64 and [r["query_ordinal"] for r in routed] == list(range(64))
    page_of = np.empty(100_000, dtype=np.int32)
    page_of[order] = np.arange(100_000) // 256
    candidate_hits, fetched_hits, returned_hits, get_counts, byte_counts, scored_rows = [], [], [], [], [], []
    plans = []
    for ordinal, (query, neighbors) in enumerate(zip(q, gt)):
        if routed is None:
            distances = center_norms - 2 * (centers @ query)
            page_distances = np.minimum.reduceat(distances, np.arange(0, 3125, 8))
            candidates = np.lexsort((np.arange(391), page_distances))[:candidate_count]
        else:
            candidates = np.asarray(routed[ordinal]["pages"], dtype=np.int64)
            assert len(candidates) == candidate_count and np.unique(candidates).size == candidate_count
            assert (candidates >= 0).all() and (candidates < 391).all()
        candidate_hits.append(int(np.isin(page_of[neighbors], candidates).sum()))
        lengths = [min(256, 100_000-p*256) for p in candidates]
        starts = np.cumsum([0]+lengths[:-1])
        rows = np.concatenate([np.arange(p*256, min((p+1)*256, 100_000)) for p in candidates])
        scored_rows.append(len(rows))
        part = records[rows]
        levels = decode_levels(part[:, :PACKED_BYTES]).astype(np.float32)
        scale = np.frombuffer(part[:, PACKED_BYTES:PACKED_BYTES+4].tobytes(), dtype="<f4")
        assert np.isfinite(scale).all() and (scale > 0).all()
        norm2 = float(mean @ mean) + 2*scale*(levels @ m_rot) + np.einsum("ij,ij->i", levels, levels)*scale*scale
        assert np.isfinite(norm2).all() and (norm2 > 0).all()
        score = (float(query @ mean) + scale*(levels @ q_rot[ordinal])) / np.sqrt(norm2)
        page_scores = np.maximum.reduceat(score, starts)
        ranking = candidates[np.lexsort((candidates, -page_scores))]
        selected = set()
        for page in ranking:
            tentative = selected | {int(page)}
            cover, count = covered_pages(tentative)
            if len(cover) <= 84 and count <= 32:
                selected = tentative
        cover, count = covered_pages(selected)
        fetched_hits.append(int(np.isin(page_of[neighbors], list(cover)).sum()))
        get_counts.append(count)
        byte_counts.append(sum(min(256, 100_000-256*p)*780 for p in cover))
        physical = np.concatenate([np.arange(p*256, min((p+1)*256, 100_000)) for p in sorted(cover)])
        fetched = sq8[physical]
        sq8_scores = rust_sq8_scores(fetched, query, low, step)
        best = fetched["id"][np.lexsort((fetched["id"], sq8_scores))[:100]]
        returned_hits.append(int(np.isin(best, neighbors).sum()))
        plans.append({"query_ordinal": ordinal, "pages": sorted(cover),
                      "fetched_gt_hits": fetched_hits[-1],
                      "returned_gt_hits": returned_hits[-1]})
    p05 = lambda hits: sorted(hits)[math.ceil(.05 * 64)-1]
    return {"schema": "borsuk-v291-two-stage-development-v1",
            "dataset": "CoHere first100k D768 cosine k100", "split": "development0-63",
            "queries": 64, "candidate_pages": candidate_count,
            "mean_candidate_gt_hits": sum(candidate_hits)/64, "p05_candidate_gt_hits": p05(candidate_hits),
            "mean_fetched_gt_hits": sum(fetched_hits)/64, "p05_fetched_gt_hits": p05(fetched_hits),
            "mean_returned_gt_hits": sum(returned_hits)/64, "p05_returned_gt_hits": p05(returned_hits),
            "max_gets": max(get_counts), "max_planned_bytes": max(byte_counts),
            "max_coded_rows_scored": max(scored_rows),
            "advance": sum(fetched_hits)/64 >= 98.9 and p05(fetched_hits) >= 96 and
            sum(returned_hits)/64 >= 98 and p05(returned_hits) >= 95 and
            max(get_counts) <= 32 and max(byte_counts) <= 16_777_216}, plans


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("raw", "archive", "root-sha", "layout", "sq8", "requests", "truth", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--plans-output", type=Path)
    args = parser.parse_args()
    result, plans = evaluate(args.raw, args.archive, args.root_sha, args.layout,
                             args.sq8, args.requests, args.truth)
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    if args.plans_output is not None:
        args.plans_output.write_text("".join(json.dumps(p, sort_keys=True, separators=(",", ":")) + "\n"
                                            for p in plans))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
