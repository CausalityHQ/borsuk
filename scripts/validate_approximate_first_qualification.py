#!/usr/bin/env python3
"""Fail-closed evaluator for terminal approximate-first paired evidence."""

import argparse
import hashlib
import json
import math
import re
import traceback
from collections import defaultdict
from pathlib import Path


class ValidationError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise ValidationError(message)


def finite(value, field):
    require(isinstance(value, (int, float)) and math.isfinite(value), f"invalid {field}")
    return float(value)


def integer(value, field):
    require(isinstance(value, int) and not isinstance(value, bool) and value >= 0, f"invalid {field}")
    return value


def percentile(values, fraction):
    require(values, "percentile over empty values")
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def sign_test_p(wins, losses):
    trials = wins + losses
    if trials == 0 or wins <= losses:
        return 1.0
    return sum(math.comb(trials, count) for count in range(wins, trials + 1)) / (2**trials)


def marker_rows(path):
    values = {}
    for line in path.read_text().splitlines():
        key, separator, value = line.partition("=")
        require(separator, f"malformed completion marker line: {line}")
        values[key] = value
    require(values.get("schema_version") == "1", "completion marker schema mismatch")
    require(values.get("rows", "").isdigit(), "completion marker rows missing")
    return int(values["rows"])


def arm(row, name, expected_mode, truth, k):
    value = row.get(name)
    require(isinstance(value, dict), f"missing {name} arm")
    require(value.get("mode") == expected_mode, f"{name} mode mismatch")
    engine = value.get("execution_engine")
    require(isinstance(engine, str) and engine, f"invalid {name} execution engine")
    require(("approximate-first" in engine) == (name == "treatment"), f"{name} execution engine mismatch")
    ids = value.get("ordered_ids")
    require(isinstance(ids, list) and len(ids) == k and len(set(ids)) == k, f"invalid {name} ordered ids")
    stored_recall = finite(value.get("recall_at_10"), f"{name} recall")
    computed_recall = len(set(ids) & set(truth)) / k
    require(abs(stored_recall - computed_recall) <= 1e-6, f"{name} recall does not reconcile")
    fields = (
        "latency_ms", "storage_gets", "storage_heads", "backing_reads",
        "backing_bytes_read", "decoded_cache_hits", "decoded_cache_bytes_read",
        "disk_cache_reads", "disk_cache_bytes_read", "bytes_read",
        "global_identity_rows_resolved", "global_exact_vectors_fetched",
        "global_base_approximate_us", "global_base_exact_rerank_us",
        "global_delta_approximate_us", "global_delta_exact_rerank_us",
        "global_delta_wait_us", "collection_resident_bytes", "retained_bytes",
        "retained_capacity_bytes", "retained_peak_bytes", "transient_bytes",
        "transient_capacity_bytes", "transient_peak_bytes",
    )
    numeric = {field: finite(value.get(field), f"{name}.{field}") for field in fields}
    require(all(number >= 0 for number in numeric.values()), f"negative {name} telemetry")
    require(
        numeric["bytes_read"]
        == numeric["disk_cache_bytes_read"] + numeric["backing_bytes_read"],
        f"{name} byte telemetry does not reconcile",
    )
    require(
        numeric["backing_reads"] <= numeric["storage_gets"] + numeric["storage_heads"],
        f"{name} backing reads exceed storage requests",
    )
    require(numeric["global_identity_rows_resolved"] > 0, f"{name} resolved no identities")
    if name == "control":
        require(numeric["global_exact_vectors_fetched"] > 0, "control fetched no exact vectors")
    return {"recall": stored_recall, **numeric}


def evaluate_point(rows, manifest, nprobe, candidates):
    control_values = []
    treatment_values = []
    for row in rows:
        truth = row.get("ground_truth_ids")
        k = manifest["k"]
        require(isinstance(truth, list) and len(truth) == k and len(set(truth)) == k, "invalid ground truth ids")
        control_values.append(arm(row, "control", manifest["control"], truth, k))
        treatment_values.append(arm(row, "treatment", manifest["treatment"], truth, k))

    control_latency = [value["latency_ms"] for value in control_values]
    treatment_latency = [value["latency_ms"] for value in treatment_values]
    treatment_recall = [value["recall"] for value in treatment_values]
    control_recall = [value["recall"] for value in control_values]
    control_reads = sum(value["backing_reads"] for value in control_values)
    treatment_reads = sum(value["backing_reads"] for value in treatment_values)
    control_bytes = sum(value["backing_bytes_read"] for value in control_values)
    treatment_bytes = sum(value["backing_bytes_read"] for value in treatment_values)
    require(control_reads > 0 and control_bytes > 0, "control backing I/O must be positive")
    read_reduction = 1.0 - treatment_reads / control_reads
    byte_reduction = 1.0 - treatment_bytes / control_bytes
    require(len(control_latency) == len(treatment_latency), "paired latency lengths differ")
    paired_differences = [
        control - treatment
        for control, treatment in zip(control_latency, treatment_latency)  # noqa: B905
    ]
    wins = sum(value > 0 for value in paired_differences)
    losses = sum(value < 0 for value in paired_differences)
    p_value = sign_test_p(wins, losses)
    control_p95 = percentile(control_latency, 0.95)
    treatment_p95 = percentile(treatment_latency, 0.95)
    p95_improvement = control_p95 - treatment_p95
    mean_treatment_recall = sum(treatment_recall) / len(treatment_recall)
    p05_treatment_recall = percentile(treatment_recall, 0.05)
    failures = []
    if sum(control_recall) / len(control_recall) < manifest["required_mean_recall_at_10"]:
        failures.append("control mean recall")
    if percentile(control_recall, 0.05) < manifest["required_p05_query_recall_at_10"]:
        failures.append("control p05 recall")
    if mean_treatment_recall < manifest["required_mean_recall_at_10"]:
        failures.append("treatment mean recall")
    if p05_treatment_recall < manifest["required_p05_query_recall_at_10"]:
        failures.append("treatment p05 recall")
    if max(value["global_exact_vectors_fetched"] for value in treatment_values) > manifest["maximum_treatment_exact_vectors"]:
        failures.append("treatment exact vectors")
    if max(value["global_base_exact_rerank_us"] + value["global_delta_exact_rerank_us"] for value in treatment_values) > manifest["maximum_treatment_exact_rerank_us"]:
        failures.append("treatment exact rerank time")
    if max(value["disk_cache_reads"] for value in control_values + treatment_values) > manifest["maximum_disk_cache_reads"]:
        failures.append("disk cache reads")
    if max(value["decoded_cache_hits"] for value in control_values + treatment_values) > 0:
        failures.append("decoded cache hits")
    if read_reduction < manifest["minimum_backing_read_reduction_fraction"]:
        failures.append("backing read reduction")
    if byte_reduction < manifest["minimum_backing_byte_reduction_fraction"]:
        failures.append("backing byte reduction")
    if p95_improvement < manifest["minimum_p95_latency_improvement_ms"]:
        failures.append("p95 latency improvement")
    if p_value > manifest["maximum_one_sided_sign_test_p"]:
        failures.append("paired sign test")
    if treatment_p95 > manifest["maximum_treatment_p95_ms"]:
        failures.append("treatment p95 ceiling")
    return {
        "nprobe": nprobe,
        "max_candidates": candidates,
        "queries": len(rows),
        "control_mean_recall_at_10": sum(control_recall) / len(control_recall),
        "treatment_mean_recall_at_10": mean_treatment_recall,
        "treatment_p05_recall_at_10": p05_treatment_recall,
        "control_p95_ms": control_p95,
        "treatment_p50_ms": percentile(treatment_latency, 0.50),
        "treatment_p95_ms": treatment_p95,
        "p95_improvement_ms": p95_improvement,
        "backing_read_reduction_fraction": read_reduction,
        "backing_byte_reduction_fraction": byte_reduction,
        "faster_pairs": wins,
        "slower_pairs": losses,
        "one_sided_sign_test_p": p_value,
        "treatment_transient_peak_bytes": max(value["transient_peak_bytes"] for value in treatment_values),
        "failures": failures,
        "accepted": not failures,
    }


def validate(root, manifest_path, *, completed_after_evaluator_failure=False):
    root = Path(root)
    manifest = json.loads(Path(manifest_path).read_text())
    complete = root / "APPROXIMATE_FIRST_PAIRS_COMPLETE"
    require(complete.is_file(), "completion marker is absent; measurement artifact is ineligible for inspection")
    require(not (root / "bench_approximate_first_pairs.jsonl.incomplete").exists(), "incomplete artifact is present")
    failures = list(root.glob("*FAILED*")) + list(root.glob("*FAILURE*"))
    if completed_after_evaluator_failure:
        require(failures, "recovery mode requires a campaign failure marker")
        require(
            (root / "APPROXIMATE_FIRST_QUALIFICATION_FAILED").is_file(),
            "recovery mode requires the qualification failure marker",
        )
    else:
        require(not failures, "campaign failure marker is present")
    identity_path = root / "qualification_identity.json"
    require(identity_path.is_file(), "qualification identity is absent")
    identity = json.loads(identity_path.read_text())
    require(identity.get("source_tree_clean") is True, "source tree was not clean")
    require(identity.get("origin_main_ancestor") is True, "source was not based on origin/main")
    for field in ("source_commit", "manifest_sha256", "dataset_descriptor_sha256", "binary_sha256"):
        require(re.fullmatch(r"[0-9a-f]{40}" if field == "source_commit" else r"[0-9a-f]{64}", str(identity.get(field, ""))) is not None, f"invalid identity {field}")
    archive_sha = identity.get("source_archive_sha256")
    require(
        archive_sha is None or re.fullmatch(r"[0-9a-f]{64}", str(archive_sha)) is not None,
        "invalid identity source_archive_sha256",
    )
    manifest_sha = hashlib.sha256(Path(manifest_path).read_bytes()).hexdigest()
    require(identity["manifest_sha256"] == manifest_sha, "manifest identity drift")
    require(identity["dataset_descriptor_sha256"] == manifest["dataset_descriptor_sha256"], "dataset identity drift")
    expected_rows = manifest["queries"] * len(manifest["nprobes"]) * len(manifest["max_candidates"])
    require(marker_rows(complete) == expected_rows, "completion marker row count mismatch")
    artifact = root / "bench_approximate_first_pairs.jsonl"
    require(artifact.is_file(), "terminal JSONL artifact is absent")
    rows = [json.loads(line) for line in artifact.read_text().splitlines()]
    require(len(rows) == expected_rows, "JSONL row count mismatch")
    grouped = defaultdict(list)
    seen = set()
    source_order = {}
    for row in rows:
        require(row.get("schema_version") == manifest["artifact_schema_version"], "row schema mismatch")
        require(row.get("query_seed") == manifest["query_seed"], "query seed drift")
        require(row.get("scan_codec") == manifest["scan_codec"], "scan codec drift")
        require(row.get("cache_execution") == manifest["cache_execution"], "cache execution drift")
        nprobe = integer(row.get("nprobe"), "nprobe")
        candidates = integer(row.get("max_candidates"), "max_candidates")
        require(nprobe in manifest["nprobes"] and candidates in manifest["max_candidates"], "undeclared search point")
        sample = integer(row.get("sample_index"), "sample_index")
        require(sample < manifest["queries"], "sample index out of range")
        key = (nprobe, candidates, sample)
        require(key not in seen, "duplicate paired row")
        seen.add(key)
        source = integer(row.get("query_source_index"), "query_source_index")
        if sample in source_order:
            require(source_order[sample] == source, "query identity differs between search points")
        source_order[sample] = source
        expected_order = "control,treatment" if sample % 2 == 0 else "treatment,control"
        require(row.get("arm_order") == expected_order, "arm order drift")
        grouped[(nprobe, candidates)].append(row)
    require(set(source_order) == set(range(manifest["queries"])), "query sequence is incomplete")
    require(len(set(source_order.values())) == manifest["queries"], "query source identities are not unique")
    points = []
    for nprobe in manifest["nprobes"]:
        for candidates in manifest["max_candidates"]:
            point_rows = sorted(grouped[(nprobe, candidates)], key=lambda row: row["sample_index"])
            require(len(point_rows) == manifest["queries"], "search point is incomplete")
            points.append(evaluate_point(point_rows, manifest, nprobe, candidates))
    eligible = [point for point in points if point["accepted"]]
    selected = min(
        eligible,
        key=lambda point: (
            point["treatment_p95_ms"],
            point["treatment_p50_ms"],
            -point["backing_read_reduction_fraction"],
            -point["backing_byte_reduction_fraction"],
            point["treatment_transient_peak_bytes"],
            -point["treatment_mean_recall_at_10"],
        ),
        default=None,
    )
    return {
        "protocol": manifest["protocol"],
        "recovery_mode": completed_after_evaluator_failure,
        "identity": identity,
        "accepted": selected is not None,
        "selected": selected,
        "points": points,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--decision", type=Path)
    parser.add_argument("--completed-after-evaluator-failure", action="store_true")
    args = parser.parse_args()
    try:
        decision = validate(
            args.root,
            args.manifest,
            completed_after_evaluator_failure=args.completed_after_evaluator_failure,
        )
    except ValidationError as error:
        print(f"invalid: {error}")
        return 2
    except Exception:  # noqa: BLE001 - unexpected evaluator defects are infrastructure failures
        traceback.print_exc()
        return 2
    encoded = json.dumps(decision, indent=2, sort_keys=True) + "\n"
    if args.decision:
        args.decision.write_text(encoded)
    print(encoded, end="")
    return 0 if decision["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
