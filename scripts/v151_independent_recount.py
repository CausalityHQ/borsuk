"""Independent postterminal V151 plan, work, and returned-quality recount."""

from __future__ import annotations

import hashlib
import json
import math
import struct

from scripts.launch_v150_unit_centroid_graph_spot import replay_plan

BUCKET = "borsuk-bench-453182569524-euc1"
V122_SQ8 = ("research/v122-deep-image-100k/"
            "afe07cb5a9ba8518263375595f589639fdf3f4f1/"
            "runs/v122-20260924T011355Z/a0001/artifacts/built/sq8.bin")
SQ8_SHA = "c20dcb8058d2409791c6c584d9f078491d4c239acbe7c19350be7757d533e8df"
GRAPH_SHA = "647799e92c32fa724aa764a4af2053377e7267ef7d968936adcbb2cc6de966dc"
CENTROID_KEY = ("research/v146-centroid-score/"
                "ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0/"
                "runs/v146-20260924T111500Z/a0001/artifacts/deep.centroids.bin")
CENTROID_SHA = "9f8a924ebeacd3c512365934c49c9fc42705a1bde1098ec99c981fb1a7fe3f12"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def rows(data: bytes) -> list[dict]:
    result = [json.loads(line) for line in data.splitlines()]
    if len(result) != 1000:
        raise ValueError("V151 record count differs")
    return result


def recount(s3: object, prefix: str, decision: dict,
            authenticated: dict[str, bytes]) -> None:
    del prefix
    manifest = json.loads(authenticated["fresh/manifest.json"])
    if (manifest["schema"] != "borsuk-v151-fresh-inputs-v1"
            or manifest["source_query_start"] != 3000
            or manifest["query_count"] != 1000
            or manifest["truth_read_by_router"] is not False
            or digest(authenticated["fresh/queries.jsonl"]) != manifest["queries_sha256"]
            or digest(authenticated["fresh/routing.jsonl"]) != manifest["routing_sha256"]
            or digest(authenticated["fresh/truth.npy"]) != manifest["truth_sha256"]):
        raise ValueError("V151 fresh input seal differs")
    queries = rows(authenticated["fresh/queries.jsonl"])
    routing = rows(authenticated["fresh/routing.jsonl"])
    plans = rows(authenticated["science.jsonl"])
    replayed = rows(authenticated["replay.jsonl"])
    evidence = rows(authenticated["evidence.jsonl"])
    plan_summary = json.loads(authenticated["science.summary.json"])
    summary = json.loads(authenticated["returned.summary.json"])
    replay_seal = json.loads(authenticated["replay.seal.json"])
    audit = json.loads(authenticated["returned.audit.json"])
    if (replay_seal["schema"] != "borsuk-v151-returned-seal-v1"
            or replay_seal["truth_opened"] is not False
            or replay_seal["plans_sha256"] != digest(authenticated["science.jsonl"])
            or replay_seal["replay_sha256"] != digest(authenticated["replay.jsonl"])
            or summary["replay_sha256"] != replay_seal["replay_sha256"]
            or summary["truth_sha256"] != manifest["truth_sha256"]
            or summary["evidence_sha256"] != digest(authenticated["evidence.jsonl"])
            or summary["plan_summary_sha256"] != digest(authenticated["science.summary.json"])):
        raise ValueError("V151 returned-ID seal differs")
    if (audit["schema"] != "borsuk-v151-returned-audit-v1"
            or audit["replay_sha256"] != replay_seal["replay_sha256"]
            or audit["queries_sha256"] != manifest["queries_sha256"]
            or audit["routing_sha256"] != manifest["routing_sha256"]
            or audit["source_sha256"] != manifest["source_sha256"]
            or audit["sq8_sha256"] != manifest["sq8_sha256"]
            or audit["checked_arm_queries"] != 3000
            or audit["truth_opened"] is not False):
        raise ValueError("V151 independent returned audit differs")
    graph = authenticated["science.graph.bin"]
    if (digest(graph) != GRAPH_SHA or graph[:8] != b"BORSUKG1"
            or struct.unpack_from("<Q8I", graph, 8)[:5] != (100000, 96, 32, 256, 3125)
            or plan_summary["graph_sha256"] != GRAPH_SHA
            or plan_summary["graph_bytes"] != len(graph)):
        raise ValueError("V151 graph artifact differs")
    centroid_blob = s3.get_object(Bucket=BUCKET, Key=CENTROID_KEY)["Body"].read()
    if (len(centroid_blob) != 600032 or digest(centroid_blob) != CENTROID_SHA
            or centroid_blob[:8] != b"BORSUCP1"
            or struct.unpack_from("<Q3I", centroid_blob, 8) != (100000, 96, 32, 256)):
        raise ValueError("V151 centroid source differs")
    centers = [struct.unpack_from("<96e", centroid_blob, 32 + 192 * unit)
               for unit in range(3125)]
    sq8 = s3.get_object(Bucket=BUCKET, Key=V122_SQ8)["Body"].read()
    if len(sq8) != 10_800_000 or digest(sq8) != SQ8_SHA:
        raise ValueError("V122 SQ8 source map differs")
    inverse = [-1] * 100000
    for position in range(100000):
        source_id = struct.unpack_from("<q", sq8, position * 108)[0]
        if not 0 <= source_id < 100000 or inverse[source_id] != -1:
            raise ValueError("SQ8 ID map differs")
        inverse[source_id] = position
    truth = authenticated["fresh/truth.npy"]
    if truth[:8] != b"\x93NUMPY\x01\x00":
        raise ValueError("fresh GT NPY version differs")
    header_size = struct.unpack_from("<H", truth, 8)[0]
    header = truth[10:10 + header_size].decode()
    if "'<i8'" not in header or "(1000, 100)" not in header or len(truth) != 10 + header_size + 800000:
        raise ValueError("fresh GT NPY geometry differs")
    gold = struct.unpack_from("<100000q", truth, 10 + header_size)
    arm_values = {arm: {field: [] for field in ("source", "sq8", "physical", "bytes", "gets")}
                  for arm in ("flat", "v150", "v151")}
    replay_cpu = {arm: {"sq8_ns": [], "source_ns": []}
                  for arm in ("flat", "v150", "v151")}
    cpu = {arm: [] for arm in ("flat", "v150", "v151")}
    all_primary = all_caps = True
    max_score_diff = 0.0
    flat_bytes = new_bytes = flat_selected = new_capture = 0
    new_work = []
    new_shortfall = []
    exact_work, union_work, total_work = [], [], []
    for ordinal, (query, route, plan, replay, proof) in enumerate(
        zip(queries, routing, plans, replayed, evidence)
    ):
        if any(record.get("query_ordinal") != ordinal
               or record.get("source_query_ordinal") != 3000 + ordinal
               for record in (query, route, plan, replay)) or proof.get("query_ordinal") != ordinal:
            raise ValueError("V151 query identity differs")
        roster = route["primary"]
        nominees = route["nominees"]
        if (len(roster) != 100 or len(nominees) != 512
                or len(set(nominees)) != 512
                or any(not isinstance(row, int) or not 0 <= row < 100000
                       for row in roster + nominees)):
            raise ValueError("V151 fresh router roster differs")
        primary_pages = {row // 256 for row in roster}
        p = len(primary_pages)
        if plan["primary_pages"] != sorted(primary_pages) or plan["primary_distinct_pages"] != p:
            raise ValueError("V151 primary pages differ")
        flat_scores = plan["flat_scores"]
        if len(flat_scores) != 391 or any(not math.isfinite(score) for score in flat_scores):
            raise ValueError("V151 flat score matrix differs")
        query_vector = query["query"]
        if (len(query_vector) != 96
                or any(not isinstance(value, (int, float)) or not math.isfinite(value)
                       for value in query_vector)):
            raise ValueError("V151 query vector differs")
        scored_unit_cache = {}
        def unit_distance(unit: int) -> float:
            if unit not in scored_unit_cache:
                center = centers[unit]
                scored_unit_cache[unit] = math.sqrt(sum(
                    (a - b) * (a - b) for a, b in zip(query_vector, center)))
            return scored_unit_cache[unit]
        arm_plans = {"flat": plan["flat_plan"],
                     "v150": plan["v150"]["plan"],
                     "v151": plan["v151"]["plan"]}
        candidates = {"flat": dict(enumerate(flat_scores)),
                      "v150": dict(plan["v150"]["scored_pages"]),
                      "v151": dict(plan["v151"]["scored_pages"])}
        for arm in ("v150", "v151"):
            scored_pages = plan[arm]["scored_pages"]
            if (len(scored_pages) != len(candidates[arm])
                    or any(not isinstance(page, int) or not 0 <= page < 391
                           or not math.isfinite(score)
                           for page, score in scored_pages)):
                raise ValueError(f"V151 {arm} scored page roster differs")
        for arm in arm_plans:
            expected = replay_plan(candidates[arm], roster)
            observed = arm_plans[arm]
            if (observed["selected_pages"] != expected["selected_pages"]
                    or observed["ranges"] != expected["ranges"]
                    or observed["planned_bytes"] != expected["planned_bytes"]
                    or observed["target_shortfall"] != expected["target_shortfall"]
                    or observed["gets"] != len(expected["ranges"])):
                raise ValueError(f"V151 {arm} planner replay differs")
            all_primary &= primary_pages.issubset(observed["selected_pages"])
            all_caps &= observed["gets"] <= 32 and observed["planned_bytes"] <= 16_777_216
        for arm in ("v150", "v151"):
            record = plan[arm]
            evaluated = record["evaluated_units"]
            if (len(evaluated) != len(set(evaluated))
                    or record["unit_evaluations"] != len(evaluated)
                    or len(evaluated) > 16 * p
                    or any(not isinstance(unit, int) or not 0 <= unit < 3125 for unit in evaluated)
                    or (record["work_exhausted"] and len(evaluated) != 16 * p)):
                raise ValueError(f"V151 {arm} work differs")
            if arm == "v151":
                expected_seeds = {unit for page in primary_pages
                                  for unit in range(page * 8, min((page + 1) * 8, 3125))}
                if (not expected_seeds.issubset(evaluated)
                        or len(record["provisional_pages"]) > 4 * p
                        or len({page for page, _ in record["provisional_pages"]})
                        != len(record["provisional_pages"])):
                    raise ValueError("V151 page-diverse seeds or result differ")
                scored = set(candidates[arm])
                if scored != primary_pages | {page for page, _ in record["provisional_pages"]}:
                    raise ValueError("V151 exact page candidates differ")
                observed_minima = {}
                for unit in evaluated:
                    page = unit // 8
                    if page not in primary_pages:
                        observed_minima[page] = min(
                            observed_minima.get(page, math.inf), unit_distance(unit))
                expected_provisional = sorted(observed_minima.items(),
                                              key=lambda item: (item[1], item[0]))[:4 * p]
                if ([page for page, _ in expected_provisional]
                        != [page for page, _ in record["provisional_pages"]]
                        or any(abs(expected - actual) > 0.001
                               for (_, expected), (_, actual) in
                               zip(expected_provisional, record["provisional_pages"]))):
                    raise ValueError("V151 provisional page ranking differs")
            for page, score in record["scored_pages"]:
                expected_score = min(unit_distance(unit) for unit in
                                     range(page * 8, min((page + 1) * 8, 3125)))
                if abs(score - expected_score) > 0.001:
                    raise ValueError("V151 independent centroid page score differs")
                max_score_diff = max(max_score_diff, abs(score - flat_scores[page]))
        cpu["flat"].append(plan["flat_elapsed_ns"])
        cpu["v150"].append(plan["v150"]["elapsed_ns"])
        cpu["v151"].append(plan["v151"]["elapsed_ns"])
        flat_bytes += arm_plans["flat"]["planned_bytes"]
        new_bytes += arm_plans["v151"]["planned_bytes"]
        flat_selected += len(arm_plans["flat"]["selected_pages"])
        new_capture += len(set(arm_plans["v151"]["selected_pages"])
                           & set(arm_plans["flat"]["selected_pages"]))
        new_work.append(plan["v151"]["unit_evaluations"])
        new_shortfall.append(arm_plans["v151"]["target_shortfall"])
        for arm in ("v150", "v151"):
            record = plan[arm]
            exact_units = {unit for page in candidates[arm]
                           for unit in range(page * 8, min((page + 1) * 8, 3125))}
            union = exact_units | set(record["evaluated_units"])
            if (record["exact_page_unit_evaluations"] != len(exact_units)
                    or record["distinct_scored_units"] != len(union)
                    or record["total_unit_distance_computations"]
                    != len(exact_units) + len(record["evaluated_units"])):
                raise ValueError(f"V151 {arm} unit work differs")
            if arm == "v151":
                exact_work.append(len(exact_units))
                union_work.append(len(union))
                total_work.append(len(exact_units) + len(record["evaluated_units"]))
        truth_ids = gold[ordinal * 100:(ordinal + 1) * 100]
        if len(set(truth_ids)) != 100 or any(not 0 <= source < 100000 for source in truth_ids):
            raise ValueError("V151 GT row differs")
        truth_set = set(truth_ids)
        for arm in arm_values:
            returned = replay["arms"][arm]
            source_ids = returned["source_top100_ids"]
            sq8_ids = returned["sq8_top100_ids"]
            if (len(source_ids) != 100 or len(set(source_ids)) != 100
                    or len(sq8_ids) != 100 or len(set(sq8_ids)) != 100
                    or any(not 0 <= source < 100000 for source in source_ids + sq8_ids)
                    or returned["ranges"] != arm_plans[arm]["ranges"]
                    or returned["planned_bytes"] != arm_plans[arm]["planned_bytes"]
                    or returned["gets"] != arm_plans[arm]["gets"]):
                raise ValueError("V151 sealed returned ID or route differs")
            source_hits = len(set(source_ids) & truth_set)
            sq8_hits = len(set(sq8_ids) & truth_set)
            physical_hits = sum(
                any(first <= inverse[source] * 108 < last for first, last in returned["ranges"])
                for source in truth_ids
            )
            if (sq8_hits > physical_hits
                    or proof["arms"][arm] != {
                        "source_hits": source_hits,
                        "sq8_hits": sq8_hits,
                        "physical_hits": physical_hits,
                    }):
                raise ValueError("V151 returned quality recount differs")
            for field, value in (("source", source_hits), ("sq8", sq8_hits),
                                 ("physical", physical_hits),
                                 ("bytes", returned["planned_bytes"]),
                                 ("gets", returned["gets"])):
                arm_values[arm][field].append(value)
            for field in ("sq8_ns", "source_ns"):
                value = returned[field]
                if not isinstance(value, int) or value < 0:
                    raise ValueError("V151 replay timing differs")
                replay_cpu[arm][field].append(value)
    p95 = {arm: sorted(values)[949] / 1e6 for arm, values in cpu.items()}
    if (plan_summary["all_primary_retained"] is not all_primary
            or plan_summary["all_plan_caps"] is not all_caps
            or abs(plan_summary["max_abs_page_score_difference"] - max_score_diff) > 1e-7
            or plan_summary["flat_planned_bytes_total"] != flat_bytes
            or plan_summary["v151_planned_bytes_total"] != new_bytes
            or plan_summary["flat_selected_pages_total"] != flat_selected
            or plan_summary["v151_selected_flat_page_capture"] != new_capture
            or plan_summary["v151_graph_work_p95"] != sorted(new_work)[949]
            or plan_summary["v151_exact_page_work_p95"] != sorted(exact_work)[949]
            or plan_summary["v151_distinct_union_work_p95"] != sorted(union_work)[949]
            or plan_summary["v151_total_unit_work_p95"] != sorted(total_work)[949]
            or plan_summary["v151_target_shortfall_queries"] != sum(
                value > 0 for value in new_shortfall)
            or plan_summary["v151_target_shortfall_p95"] != sorted(new_shortfall)[949]
            or any(abs(plan_summary[f"{arm}_p95_ms"] - value) > 1e-9
                   for arm, value in p95.items())):
        raise ValueError("V151 plan summary recount differs")
    for arm, fields in arm_values.items():
        actual = summary["arms"][arm]
        source_hits = sorted(fields["source"])
        if (actual["source_hits"] != sum(source_hits)
                or actual["sq8_hits"] != sum(fields["sq8"])
                or actual["physical_hits"] != sum(fields["physical"])
                or actual["source_p05_hits"] != source_hits[49]
                or actual["source_sub90_queries"] != sum(hits < 90 for hits in source_hits)
                or actual["planned_bytes_total"] != sum(fields["bytes"])
                or actual["gets_total"] != sum(fields["gets"])):
                raise ValueError("V151 returned summary recount differs")
        for field, label in (("sq8_ns", "sq8_p95_ms"),
                             ("source_ns", "source_p95_ms")):
            if abs(actual[label] - sorted(replay_cpu[arm][field])[949] / 1e6) > 1e-9:
                raise ValueError("V151 replay timing summary differs")
    flat = summary["arms"]["flat"]
    candidate = summary["arms"]["v151"]
    paired = [new - old for new, old in zip(arm_values["v151"]["source"],
                                             arm_values["flat"]["source"])]
    expected_paired = {"wins": sum(delta > 0 for delta in paired),
                       "ties": sum(delta == 0 for delta in paired),
                       "losses": sum(delta < 0 for delta in paired),
                       "mean_delta_hits": sum(paired) / 1000}
    if summary["v151_vs_flat_paired_source_hits"] != expected_paired:
        raise ValueError("V151 paired source outcome recount differs")
    passed = (all_primary and all_caps and max_score_diff <= 0.0001
              and candidate["source_hits"] + 100 >= flat["source_hits"]
              and candidate["source_p05_hits"] >= 98
              and candidate["source_sub90_queries"] == 0
              and p95["v151"] < p95["flat"]
              and candidate["planned_bytes_total"] <= flat["planned_bytes_total"]
              and candidate["gets_total"] <= flat["gets_total"])
    if (summary["passes_frozen_gate"] is not passed
            or decision["verdict"] != ("pass" if passed else "reject")):
        raise ValueError("V151 frozen decision recount differs")
