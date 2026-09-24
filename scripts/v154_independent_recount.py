"""Independently recount terminal-closed ReLAION plans and CPU summaries."""

from __future__ import annotations

import bisect
import hashlib
import json
import math
import struct

BUCKET = "borsuk-bench-453182569524-euc1"
V146 = ("research/v146-centroid-score/"
        "ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0/"
        "runs/v146-20260924T111500Z/a0001/artifacts/")
SCORES_SHA = "9c4d5c2942f5e01729a8871628648146e2d055e373a4d972e53435c6a98b4395"
CENTROID_SHA = "07ad4736d8eb6867d46523de71ed61d82220dabc923f3c88ead1cd13eb776651"
QUERY_SHA = "4ef4734ff40aeda8b8e99ec01ca993be46e8ad9c337b5410259eeb171f57acfd"
ROUTING_SHA = "2545c5762d04d28c91941607ae1801a8aa7d5a927f4a02aa92b28d43b1cdf1cf"
REQUEST_SHA = "c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9"
PRIMARY_SHA = "71bfdf71f293ca1d23f58694866b3ba52ee8ee95ed9b02e676a2a8bf031f3162"
ROWS = 1_000_000
DIMS = 768
PAGE_COUNT = 3907
UNIT_COUNT = 31250
ROW_BYTES = 780


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def records(raw: bytes) -> list[dict]:
    values = [json.loads(line) for line in raw.splitlines()]
    if len(values) != 1000:
        raise ValueError("V154 row count differs")
    return values


def p95(values: list[int]) -> float:
    return sorted(values)[949] / 1e6


def cover(pages: list[int]) -> tuple[list[list[int]], int]:
    runs: list[list[int]] = []
    for page in pages:
        if runs and runs[-1][1] == page:
            runs[-1][1] += 1
        else:
            runs.append([page, page + 1])
    bridges = max(0, len(runs) - 32)
    gaps = sorted((runs[i + 1][0] - runs[i][1], i)
                  for i in range(len(runs) - 1))
    joined = {index for _, index in gaps[:bridges]}
    merged = [runs[0].copy()]
    for index, run in enumerate(runs[1:]):
        if index in joined:
            merged[-1][1] = run[1]
        else:
            merged.append(run.copy())
    ranges = [[start * 256 * ROW_BYTES, min(end * 256, ROWS) * ROW_BYTES]
              for start, end in merged]
    return ranges, sum(end - start for start, end in ranges)


def replay(scores: dict[int, float], primary: list[int]) -> dict:
    primary_pages = list(dict.fromkeys(row // 256 for row in primary))
    primary_set = set(primary_pages)
    target = min(PAGE_COUNT, 4 * len(primary_pages))
    selected: list[int] = []
    ordered = primary_pages + [page for page, _ in sorted(scores.items(),
        key=lambda item: (item[1], item[0])) if page not in primary_set]
    for page in ordered:
        bisect.insort(selected, page)
        _, charged = cover(selected)
        if charged > 16_777_216:
            selected.remove(page)
            continue
        if len(selected) >= target:
            break
    ranges, charged = cover(selected)
    return {"selected_pages": selected, "ranges": ranges,
            "planned_bytes": charged, "gets": len(ranges),
            "target_pages": target, "target_shortfall": target - len(selected)}


def recount(s3: object, prefix: str, decision: dict,
            authenticated: dict[str, bytes]) -> None:
    del prefix
    seal = json.loads(authenticated["fresh/manifest.json"])
    if (seal["schema"] != "borsuk-v154-relaion-seal-v1"
            or seal["requests_sha256"] != REQUEST_SHA
            or seal["primary_sha256"] != PRIMARY_SHA
            or seal["query_count"] != 1000
            or seal["queries_sha256"] != QUERY_SHA
            or seal["routing_sha256"] != ROUTING_SHA
            or digest(authenticated["fresh/queries.jsonl"]) != QUERY_SHA
            or digest(authenticated["fresh/routing.jsonl"]) != ROUTING_SHA):
        raise ValueError("V154 input seal differs")
    queries = records(authenticated["fresh/queries.jsonl"])
    routing = records(authenticated["fresh/routing.jsonl"])
    science = records(authenticated["science.jsonl"])
    summary = json.loads(authenticated["science.summary.json"])
    graph = authenticated["science.graph.bin"]
    if (summary["schema"] != "borsuk-v154-relaion-page-graph-plan-v1"
            or summary["rows"] != ROWS or summary["dimensions"] != DIMS
            or summary["queries"] != 1000 or graph[:8] != b"BORSUKG1"
            or summary["graph_sha256"] != digest(graph)
            or summary["graph_bytes"] != len(graph)
            or struct.unpack_from("<Q8I", graph, 8)[:5]
            != (ROWS, DIMS, 32, 256, UNIT_COUNT)
            or graph[48:80] != bytes.fromhex(CENTROID_SHA)):
        raise ValueError("V154 summary or graph geometry differs")
    reference = s3.get_object(Bucket=BUCKET,
                              Key=V146 + "relaion.rust-scores.bin")["Body"].read()
    if len(reference) != 15_628_000 or digest(reference) != SCORES_SHA:
        raise ValueError("V146 flat score reference differs")
    times = {arm: [] for arm in ("flat", "v150", "v151", "v152")}
    phases = {phase: [] for phase in ("search_ns", "score_ns", "plan_ns")}
    flat_score_ns, flat_plan_ns = [], []
    flat_even, flat_odd, cached_even, cached_odd = [], [], [], []
    bytes_total = {arm: 0 for arm in times}
    gets_total = {arm: 0 for arm in times}
    work = {arm: {name: [] for name in ("graph", "exact", "union", "total", "shortfall")}
            for arm in ("v151", "v152")}
    new_unit_work = []
    capture = {arm: 0 for arm in ("v151", "v152")}
    flat_selected_pages_total = 0
    flat_target_shortfall_queries = 0
    all_primary = paired_primary = all_caps = True
    flat_primary_shortfall_queries = 0
    max_diff = 0.0
    for ordinal, (query, route, record) in enumerate(zip(queries, routing, science)):
        if any(row["query_ordinal"] != ordinal or row["source_query_ordinal"] != ordinal
               for row in (query, route, record)):
            raise ValueError("V154 query identity differs")
        primary = route["primary"]
        primary_pages = {row // 256 for row in primary}
        p = len(primary_pages)
        if record["primary_pages"] != sorted(primary_pages):
            raise ValueError("V154 primary pages differ")
        flat_scores = record["flat_scores"]
        if len(flat_scores) != PAGE_COUNT or any(not math.isfinite(x) for x in flat_scores):
            raise ValueError("V154 flat scores differ")
        expected_scores = struct.unpack_from(f"<{PAGE_COUNT}f", reference,
                                             ordinal * PAGE_COUNT * 4)
        if max(abs(a - b) for a, b in zip(flat_scores, expected_scores)) > 1e-6:
            raise ValueError("V154 flat score reference parity differs")
        candidates = {"flat": dict(enumerate(flat_scores))}
        plans = {"flat": record["flat_plan"]}
        for arm in ("v150", "v151", "v152"):
            observed = record[arm]
            scored = observed["scored_pages"]
            candidates[arm] = dict(scored)
            plans[arm] = observed["plan"]
            evaluated = observed["evaluated_units"]
            if (len(evaluated) != len(set(evaluated))
                    or len(evaluated) != observed["unit_evaluations"]
                    or len(evaluated) > 16 * p
                    or any(unit < 0 or unit >= UNIT_COUNT for unit in evaluated)
                    or len(scored) != len(candidates[arm])):
                raise ValueError(f"V154 {arm} bounded work differs")
            if arm in ("v151", "v152") and len(scored) > 5 * p:
                raise ValueError("V154 page roster exceeds 5P")
            for page, score in scored:
                if not 0 <= page < PAGE_COUNT or not math.isfinite(score):
                    raise ValueError("V154 sparse score differs")
                max_diff = max(max_diff, abs(score - flat_scores[page]))
            if sum(observed[k] for k in phases) != observed["elapsed_ns"]:
                raise ValueError("V154 phase timing differs")
            times[arm].append(observed["elapsed_ns"])
        if (record["v151"]["evaluated_units"] != record["v152"]["evaluated_units"]
                or record["v151"]["provisional_pages"] != record["v152"]["provisional_pages"]
                or [page for page, _ in record["v151"]["scored_pages"]]
                != [page for page, _ in record["v152"]["scored_pages"]]
                or len(record["v152"]["cached_graph_squared"])
                != len(record["v152"]["evaluated_units"])):
            raise ValueError("V154 cached graph roster or score cache differs")
        if record["flat_score_ns"] + record["flat_plan_ns"] != record["flat_elapsed_ns"]:
            raise ValueError("V154 flat phase timing differs")
        times["flat"].append(record["flat_elapsed_ns"])
        flat_score_ns.append(record["flat_score_ns"])
        flat_plan_ns.append(record["flat_plan_ns"])
        for arm in times:
            expected = replay(candidates[arm], primary)
            if plans[arm] != expected:
                raise ValueError(f"V154 {arm} planner replay differs: {ordinal}")
            all_primary &= primary_pages.issubset(plans[arm]["selected_pages"])
            all_caps &= plans[arm]["gets"] <= 32 and plans[arm]["planned_bytes"] <= 16_777_216
            bytes_total[arm] += plans[arm]["planned_bytes"]
            gets_total[arm] += plans[arm]["gets"]
        flat_target_shortfall_queries += plans["flat"]["target_shortfall"] > 0
        flat_selected_pages_total += len(plans["flat"]["selected_pages"])
        flat_selected = set(plans["flat"]["selected_pages"])
        for arm in ("v151", "v152"):
            observed = record[arm]
            exact = {unit for page, _ in observed["scored_pages"]
                     for unit in range(page * 8, min((page + 1) * 8, UNIT_COUNT))}
            evaluated = set(observed["evaluated_units"])
            values = work[arm]
            values["graph"].append(len(evaluated))
            values["exact"].append(len(exact))
            values["union"].append(len(exact | evaluated))
            values["total"].append(len(evaluated) + (
                observed["newly_scored_units"] if arm == "v152" else len(exact)))
            values["shortfall"].append(plans[arm]["target_shortfall"])
            capture[arm] += len(flat_selected & set(plans[arm]["selected_pages"]))
            if (observed["exact_page_unit_evaluations"] != len(exact)
                    or observed["distinct_scored_units"] != len(exact | evaluated)
                    or observed["total_unit_distance_computations"] != values["total"][-1]):
                raise ValueError(f"V154 {arm} unit work summary differs")
        new_unit_work.append(record["v152"]["newly_scored_units"])
        flat_retained = primary_pages & set(plans["flat"]["selected_pages"])
        flat_primary_shortfall_queries += len(flat_retained) < p
        retention_matches = all(
            primary_pages & set(plans[arm]["selected_pages"]) == flat_retained
            for arm in ("v150", "v151", "v152")
        )
        paired_primary &= retention_matches
        if record["primary_retained"] != all(
                primary_pages.issubset(plans[arm]["selected_pages"])
                for arm in times):
            raise ValueError("V154 absolute primary flag differs")
        if record["paired_primary_retention"] != retention_matches:
            raise ValueError("V154 paired primary flag differs")
        for phase in phases:
            phases[phase].append(record["v152"][phase])
        (flat_even if ordinal % 2 == 0 else flat_odd).append(record["flat_elapsed_ns"])
        (cached_even if ordinal % 2 == 0 else cached_odd).append(record["v152"]["elapsed_ns"])
    expected_fields = {"flat_p50_ms": sorted(times["flat"])[499] / 1e6,
                       "flat_p95_ms": p95(times["flat"]),
                       "flat_p99_ms": sorted(times["flat"])[989] / 1e6,
                       "flat_score_p95_ms": p95(flat_score_ns),
                       "flat_plan_p95_ms": p95(flat_plan_ns),
                       "flat_target_shortfall_queries": flat_target_shortfall_queries,
                       "v150_p95_ms": p95(times["v150"]),
                       "v151_p95_ms": p95(times["v151"]),
                       "v152_p50_ms": sorted(times["v152"])[499] / 1e6,
                       "v152_p95_ms": p95(times["v152"]),
                       "v152_p99_ms": sorted(times["v152"])[989] / 1e6,
                       "v152_search_p95_ms": p95(phases["search_ns"]),
                       "v152_score_p95_ms": p95(phases["score_ns"]),
                       "v152_plan_p95_ms": p95(phases["plan_ns"]),
                       "flat_planned_bytes_total": bytes_total["flat"],
                       "flat_gets_total": gets_total["flat"],
                       "v151_planned_bytes_total": bytes_total["v151"],
                       "v152_planned_bytes_total": bytes_total["v152"],
                       "v152_gets_total": gets_total["v152"],
                       "flat_selected_pages_total": flat_selected_pages_total,
                       "v151_selected_flat_page_capture": capture["v151"],
                       "v152_selected_flat_page_capture": capture["v152"],
                       "all_primary_retained": all_primary,
                       "paired_primary_retention": paired_primary,
                       "flat_primary_shortfall_queries": flat_primary_shortfall_queries,
                       "all_plan_caps": all_caps}
    for arm in ("v151", "v152"):
        values = work[arm]
        for name in ("graph", "exact", "union", "total"):
            field = {"graph": "graph_work", "exact": "exact_page_work",
                     "union": "distinct_union_work", "total": "total_unit_work"}[name]
            expected_fields[f"{arm}_{field}_p95"] = sorted(values[name])[949]
        expected_fields[f"{arm}_target_shortfall_p95"] = sorted(values["shortfall"])[949]
        expected_fields[f"{arm}_target_shortfall_queries"] = sum(
            n > 0 for n in values["shortfall"])
    expected_fields["v152_new_unit_work_p95"] = sorted(new_unit_work)[949]
    for field, expected in expected_fields.items():
        if summary[field] != expected:
            raise ValueError(f"V154 summary differs: {field}")
    if (abs(summary["max_abs_page_score_difference"] - max_diff) > 1e-6
            or summary["flat_even_p95_ms"] != sorted(flat_even)[474] / 1e6
            or summary["flat_odd_p95_ms"] != sorted(flat_odd)[474] / 1e6
            or summary["v152_even_p95_ms"] != sorted(cached_even)[474] / 1e6
            or summary["v152_odd_p95_ms"] != sorted(cached_odd)[474] / 1e6):
        raise ValueError("V154 score or parity summary differs")
    if not paired_primary:
        verdict = "reject-primary-parity"
    elif not all_caps:
        verdict = "reject-caps"
    elif max_diff > 0.0001:
        verdict = "reject-score-parity"
    elif (p95(times["v152"]) >= p95(times["flat"])
          or sorted(cached_even)[474] >= sorted(flat_even)[474]
          or sorted(cached_odd)[474] >= sorted(flat_odd)[474]):
        verdict = "reject-cpu"
    else:
        verdict = "pass-cpu"
    if decision != {"schema": "borsuk-v154-decision-v1",
                    "verdict": verdict}:
        raise ValueError("V154 decision recount differs")
