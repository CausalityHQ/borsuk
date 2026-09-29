"""Independently verify the CLOSED fresh 1M prospective native and HTTP run."""

from collections import Counter
import gzip
import hashlib
import json
import math
from pathlib import Path
import struct
import sys

import boto3


ROOT = Path("docs/research/native-union-20260928")


def sha(body):
    return hashlib.sha256(body).hexdigest()


def percentile(values, p):
    values = sorted(values)
    x = (len(values) - 1) * p
    lo, hi = math.floor(x), math.ceil(x)
    return values[lo] + (values[hi] - values[lo]) * (x - lo)


def main(attempt, family="rank16"):
    if family not in ("rank16", "cohere"):
        raise ValueError("unknown fresh dataset family")
    campaign = "fresh-" + family + "-confirm936"
    directory = ROOT / campaign / attempt
    config_bytes = (ROOT / (campaign + "-config.json")).read_bytes()
    config = json.loads(config_bytes)
    launch = json.loads((directory / "aws-launch.json").read_text())
    close = json.loads((directory / "aws-closeout.json").read_text())
    s3_session = boto3.Session(profile_name="causality", region_name=config["region"])
    s3, ec2 = s3_session.client("s3"), s3_session.client("ec2")
    bucket, prefix = config["bucket"], launch["prefix"]

    def s3_bytes(key):
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    terminal_bytes = s3_bytes(prefix + "/terminal.json")
    reservation_bytes = s3_bytes(prefix + "/reservation.json")
    terminal = json.loads(terminal_bytes)
    reservation = json.loads(reservation_bytes)
    assert terminal_bytes == (directory / "aws-terminal.json").read_bytes()
    assert reservation == json.loads((directory / "aws-reservation.json").read_text())
    assert terminal["schema"] == reservation["schema"] == "borsuk-fresh-" + family + "-1m-confirm936-spot-v1"
    assert terminal["status"] == "complete" and terminal["exit_code"] == 0
    assert terminal["instance_id"] == launch["instance_id"] == close["instance_id"]
    assert terminal["source_base_commit"] == reservation["source_base_commit"] == launch["source_base_commit"]
    assert terminal["source_archive_sha256"] == reservation["source_archive_sha256"] == launch["source_archive_sha256"]
    assert reservation["config_sha256"] == sha(config_bytes)
    archive = s3_bytes("research/native-library-check/sources/" + launch["source_archive_sha256"] + ".tar.gz")
    assert sha(archive) == launch["source_archive_sha256"]
    for name, ident in terminal["artifacts"].items():
        body = s3_bytes(prefix + "/artifacts/" + name)
        assert len(body) == ident["bytes"] and sha(body) == ident["sha256"], name
        assert gzip.decompress((directory / (name + ".gz")).read_bytes()) == body, name

    def artifact(name):
        return gzip.decompress((directory / ("screen/" + name + ".gz")).read_bytes())

    def sealed_range(ident, start, length):
        head = s3.head_object(Bucket=bucket, Key=ident["key"])
        assert head["ContentLength"] == ident["bytes"] and head["Metadata"]["sha256"] == ident["sha256"]
        response = s3.get_object(Bucket=bucket, Key=ident["key"],
                                 Range=f"bytes={start}-{start + length - 1}", IfMatch=head["ETag"])
        body = response["Body"].read()
        assert len(body) == length
        assert response["ContentRange"] == f"bytes {start}-{start + length - 1}/{ident['bytes']}"
        return body

    first, count = config["first"], config["count"]
    assert (first, count) == (64, 936)
    raw = sealed_range(config["sealed"]["queries.raw"], first * 768 * 4, count * 768 * 4)
    truth_body = sealed_range(config["sealed"]["truth.u32"], first * 100 * 4, count * 100 * 4)
    original_body = sealed_range(config["sealed"]["requests.jsonl"], config["requests_byte_start"],
                                 config["sealed"]["requests.jsonl"]["bytes"] - config["requests_byte_start"])
    requests_body = artifact("requests936.jsonl")
    requests = [json.loads(line) for line in requests_body.splitlines()]
    original = [json.loads(line) for line in original_body.splitlines()]
    truth = [struct.unpack_from("<100I", truth_body, q * 400) for q in range(count)]
    assert len(requests) == len(original) == count
    for q, (request, source) in enumerate(zip(requests, original)):
        vector = struct.unpack_from("<768f", raw, q * 768 * 4)
        assert request["query_ordinal"] == q and request["source_query_ordinal"] == source["query_ordinal"] == first + q
        assert request["query"] == source["query"] == list(vector)
        assert len(set(truth[q])) == 100 and max(truth[q]) < config["rows"]

    quality = json.loads(artifact("native-quality.json"))
    decision = json.loads(artifact("decision.json"))
    if family == "cohere":
        assert quality["dataset"] == "CoHere FIRST1M D768 cosine"
    assert quality["split"] == config["query_split"] and quality["root_sha256"] == config["root_sha256"]
    assert quality["opened_original_ordinals"] == [64, 999] and not quality["only_dev64_ranges_read"]
    assert quality["query_prefix_sha256"] == sha(raw)
    assert quality["requests_prefix_sha256"] == sha(original_body)
    assert quality["truth_prefix_sha256"] == sha(truth_body)
    assert not quality["qualification"] and decision["native_quality"] == quality["native_quality"]
    refs = {}
    for k in (10, 100):
        records = [json.loads(line) for line in artifact(f"reference-k{k}.jsonl").splitlines()]
        assert len(records) == count + 2 and records[0]["root_sha256"] == config["root_sha256"]
        assert records[0]["top_k"] == k and records[0]["declared_panel_count"] == count and records[-1]["count"] == count
        refs[k] = records[1:-1]
        hits = 0
        for q, row in enumerate(refs[k]):
            assert row["query_ordinal"] == q and len(row["ids"]) == len(set(row["ids"])) == k
            assert len(row["ranges"]) == row["submitted_gets"] <= 32
            assert row["planned_bytes"] == row["verified_bytes"] <= 16_773_120 and row["failed_gets"] == 0
            hits += len(set(row["ids"]) & set(truth[q][:k]))
        assert quality["native_quality"][str(k)] == {
            "hits": hits, "denominator": count * k, "recall": hits / (count * k)}

    native_pass = quality["native_quality"]["10"]["recall"] >= .95
    assert native_pass == decision["http_measured"]
    runs = []
    total_gets = total_bytes = total_success = 0
    for rep, result in enumerate(decision.get("runs", [])):
        k = config["setting_order"][rep]
        assert result == json.loads(artifact(f"run{rep}-k{k}/result.json"))
        assert result["rep"] == rep and result["k"] == k and result["offered_count"] == count
        assert result["split"] == config["query_split"] and result["identity_parity_valid"]
        assert result["namespace_ready_ms"] > 0
        samples = [json.loads(line) for line in artifact(f"run{rep}-k{k}/http.jsonl").splitlines()]
        assert len(samples) == count
        assert dict(Counter(row["outcome"] for row in samples)) == result["outcomes"]
        assert sum(result["outcomes"].values()) == count
        assert result["offered_qps"] == 8 and result["scheduled_duration_ns"] == count * 125_000_000
        assert result["elapsed_including_drain_ns"] >= result["scheduled_duration_ns"]
        hits = gets = verified_bytes = failed_gets = 0
        timings = []
        for q, row in enumerate(samples):
            assert row["query_ordinal"] == q
            assert row["integrity_error"] is None
            counters = row.get("response", {})
            gets += counters.get("submitted_gets") or 0
            verified_bytes += counters.get("verified_bytes") or 0
            failed_gets += counters.get("failed_gets") or 0
            if row["outcome"] != "success":
                continue
            assert row["physical_counters_complete"]
            assert row["status"] == 200 and row["response"]["authority"] == result["authority"]
            response = row["response"]
            for field in ("ids", "ranges", "planned_bytes", "submitted_gets", "verified_bytes", "failed_gets"):
                assert response[field] == refs[k][q][field]
            assert row["returned_hits"] == len(set(response["ids"]) & set(truth[q][:k]))
            hits += row["returned_hits"]
            timings.append((row["completed_ns"] - row["started_ns"]) / 1e6)
        assert result["successful_count"] == len(timings) and result["mean_offered_recall"] == hits / (count * k)
        assert result["achieved_successful_qps"] == len(timings) * 1e9 / result["elapsed_including_drain_ns"]
        assert result["known_submitted_gets"] == gets and result["known_verified_bytes"] == verified_bytes
        assert result["known_failed_gets"] == failed_gets
        assert result["physical_counters_complete"] == all(row["physical_counters_complete"] for row in samples)
        if timings:
            for name, p in (("p50", .5), ("p90", .9), ("p95", .95), ("p99", .99)):
                assert abs(result["successful_incoming_http_ms"][name] - percentile(timings, p)) < 1e-9
        gate = (len(timings) == count and hits / (count * k) >= .95
                and result["successful_incoming_http_ms"]["p90"] < 444
                and result["achieved_successful_qps"] >= 8) if k == 10 and timings else False
        if k == 10:
            assert result["development_gate_passed"] == gate
        runs.append({"rep": rep, "k": k, "hits": hits, "denominator": count * k,
                     "outcomes": result["outcomes"], "incoming_http_ms": result["successful_incoming_http_ms"],
                     "successful_qps": result["achieved_successful_qps"],
                     "namespace_ready_ms": result["namespace_ready_ms"],
                     "data_gets": gets, "verified_data_bytes": verified_bytes})
        total_success += len(timings)
        total_gets += gets
        total_bytes += verified_bytes
    k10_runs = [run for run in decision.get("runs", []) if run["k"] == 10]
    passed = native_pass and len(k10_runs) == 2 and all(run["development_gate_passed"] for run in k10_runs)
    assert decision["decision"].startswith("GO " if passed else "FAIL ")
    if not native_pass:
        assert not decision["http_measured"] and not runs
    else:
        assert decision["fresh_cohort_used"] and not decision["qualification"]
        assert not decision["matched_control_http_measured"] and not decision["matched_vendor_measured"]

    cgroup = json.loads(artifact("cgroup.json"))["cgroup"]
    assert int(cgroup["memory.max"]) == 8 * 1024**3
    assert int(cgroup["memory.peak"]) < int(cgroup["memory.max"])
    assert int(cgroup["memory.swap.peak"]) == 0 and "oom_kill 0" in cgroup["memory.events"]
    instance = ec2.describe_instances(InstanceIds=[launch["instance_id"]])["Reservations"][0]["Instances"][0]
    assert instance["State"]["Name"] == close["state"] == "terminated"
    assert instance["InstanceLifecycle"] == "spot"
    report = {"valid_measurement": True, "decision": decision["decision"], "prospective_confirmation": True,
              "qualification": False, "matched_vendor_measured": False, "matched_control_http_measured": False,
              "instance_id": launch["instance_id"], "state": "terminated", "terminal_sha256": sha(terminal_bytes),
              "reservation_sha256": sha(reservation_bytes), "source_archive_sha256": sha(archive),
              "config_sha256": sha(config_bytes), "native_quality": quality["native_quality"],
              "runs": runs, "all_offered_requests": count * len(runs), "all_successful_requests": total_success,
              "physical_data_gets": total_gets, "verified_data_bytes": total_bytes,
              "cgroup_peak_bytes": int(cgroup["memory.peak"]), "swap_peak_bytes": 0,
              "instance_elapsed_seconds": close["observed_elapsed_s"],
              "compute_cost_estimate_usd": close["compute_cost_estimate_usd"], "cost_scope": close["cost_status"]}
    (directory / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("valid_measurement", "decision", "native_quality",
                                               "all_successful_requests", "physical_data_gets", "cgroup_peak_bytes")}))


if __name__ == "__main__":
    family = "cohere" if len(sys.argv) > 1 and sys.argv[1] == "cohere" else "rank16"
    offset = 2 if family == "cohere" else 1
    main(sys.argv[offset] if len(sys.argv) > offset else "a0001", family)
