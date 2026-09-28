"""Exact sequential-f32 SQ8 scores for sealed frozen V296 physical plans."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import numpy as np
from scripts.v291_two_stage_development import rust_sq8_scores


def checked(path, digest):
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest() != digest:
        raise ValueError(f"input digest differs: {path.name}")
    return body


def evaluate(config_path, config_sha, sq8_path, truth_path, truth_sha, plans_path, plans_sha):
    config = json.loads(checked(config_path, config_sha))
    root = Path(config["root"])
    manifest = json.loads(checked(root / "manifest.json", config["root_sha"]))
    if (manifest["rows"], manifest["dimensions"], manifest["page_rows"]) != (100000, 768, 256):
        raise ValueError("generation geometry")
    router = json.loads(checked(root / "router/manifest.json", manifest["router_manifest_sha256"]))
    coefficients = {}
    for name in ("low", "step"):
        section = router["sections"][name]
        body = checked(root / f"router/{name}.bin", section["sha256"])
        if len(body) != section["bytes"] or len(body) != 3072:
            raise ValueError("coefficient geometry")
        coefficients[name] = np.frombuffer(body, dtype="<f4")
    if not np.isfinite(coefficients["low"]).all() or not np.isfinite(coefficients["step"]).all() or (coefficients["step"] <= 0).any():
        raise ValueError("coefficient values")
    sq8_type = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (768,))])
    body = checked(sq8_path, manifest["sq8_object_sha256"])
    if len(body) != 78000000:
        raise ValueError("SQ8 geometry")
    sq8 = np.frombuffer(body, dtype=sq8_type)
    if not np.array_equal(np.sort(sq8["id"]), np.arange(100000)) or not np.isfinite(sq8["norm"]).all():
        raise ValueError("source IDs or norm")
    page_of = np.empty(100000, dtype=np.int64)
    page_of[sq8["id"]] = np.arange(100000) // 256
    truth_raw = checked(truth_path, truth_sha)
    if len(truth_raw) != 400000:
        raise ValueError("truth geometry")
    truth = np.frombuffer(truth_raw, dtype="<u4").reshape(1000, 100)
    if (truth >= 100000).any():
        raise ValueError("truth ID")
    requests = [json.loads(line) for line in checked(Path(config["requests"]), config["requests_sha"]).splitlines()]
    if len(requests) != 1000 or [row["query_ordinal"] for row in requests] != list(range(1000)):
        raise ValueError("request roster")
    plans = [json.loads(line) for line in checked(plans_path, plans_sha).splitlines()]
    first, count = config["first"], config["count"]
    if (first, count) not in ((0, 64), (256, 64), (256, 744)):
        raise ValueError("frozen split")
    roster = [(ordinal, arm) for ordinal in range(first, first + count) for arm in ("graph", "flat")]
    if [(p["query_ordinal"], p["arm"]) for p in plans] != roster:
        raise ValueError("plan roster")
    samples = []
    for plan in plans:
        ordinal = plan["query_ordinal"]
        query = np.asarray(requests[ordinal]["query"], dtype=np.float32)
        if query.shape != (768,) or not np.isfinite(query).all():
            raise ValueError("query geometry")
        pages = np.asarray(plan["pages"], dtype=np.int64)
        candidates = np.asarray(plan["candidate_pages"], dtype=np.int64)
        for selected in (pages, candidates):
            if selected.ndim != 1 or len(selected) == 0 or (selected < 0).any() or (selected >= 391).any() or not (np.diff(selected) > 0).all():
                raise ValueError("page roster")
        gets = 1 + int((np.diff(pages) > 1).sum())
        physical = np.concatenate([np.arange(p * 256, min((p + 1) * 256, 100000)) for p in pages])
        planned_bytes = len(physical) * 780
        code_rows = sum(min(256, 100000 - p * 256) for p in candidates)
        if len(candidates) != 159 or len(pages) > 84 or gets > 32 or planned_bytes > 16777216 or gets != plan["planned_gets"] or planned_bytes != plan["planned_bytes"] or code_rows != plan["coded_rows_scored"]:
            raise ValueError("physical budget or accounting")
        neighbors = truth[ordinal]
        if np.unique(neighbors).size != 100:
            raise ValueError("truth uniqueness")
        fetched = sq8[physical]
        scores = rust_sq8_scores(fetched, query, coefficients["low"], coefficients["step"])
        if not np.isfinite(scores).all():
            raise ValueError("nonfinite score")
        ids = fetched["id"][np.lexsort((fetched["id"], scores))[:100]]
        samples.append({**plan,
            "candidate_gt_hits": int(np.isin(page_of[neighbors], candidates).sum()),
            "fetched_gt_hits": int(np.isin(fetched["id"], neighbors).sum()),
            "returned_gt_hits": int(np.isin(ids, neighbors).sum())})
    p05 = lambda values: sorted(values)[math.ceil(.05 * count) - 1]
    arms = {}
    for arm in ("graph", "flat"):
        rows = [row for row in samples if row["arm"] == arm]
        metrics = {f"{stat}_{layer}_gt_hits": reducer([row[f"{layer}_gt_hits"] for row in rows])
            for stat, reducer in (("mean", lambda values: sum(values) / count), ("p05", p05))
            for layer in ("candidate", "fetched", "returned")}
        arms[arm] = {**metrics, "max_gets": max(row["planned_gets"] for row in rows),
            "max_planned_bytes": max(row["planned_bytes"] for row in rows),
            "max_coded_rows_scored": max(row["coded_rows_scored"] for row in rows),
            "max_centroid_evaluations": max(row["centroid_evaluations"] for row in rows)}
    graph, flat = arms["graph"], arms["flat"]
    passes = graph["mean_returned_gt_hits"] >= 98 and graph["p05_returned_gt_hits"] >= 95 and graph["mean_returned_gt_hits"] >= flat["mean_returned_gt_hits"] - .5
    return {"schema": "borsuk-v296-validation-score-v1", "dataset": config["dataset"],
        "metric": "cosine", "rows": 100000, "dimensions": 768, "k": 100,
        "first": first, "queries": count, "arms": arms, "screen_pass": passes,
        "qualification": False, "config_sha256": config_sha, "plans_sha256": plans_sha,
        "truth_sha256": truth_sha}, samples


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("config", "sq8", "truth", "plans", "output", "samples-output"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("config-sha", "truth-sha", "plans-sha"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    result, samples = evaluate(args.config, args.config_sha, args.sq8, args.truth,
                               args.truth_sha, args.plans, args.plans_sha)
    args.samples_output.write_text("".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in samples))
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
