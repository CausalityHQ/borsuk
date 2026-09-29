"""Verify CLOSED fresh dev64 identities and integer/timing reductions only."""

import gzip
import hashlib
import json
import math
from pathlib import Path
import struct
import sys

import boto3


ROOT = Path("docs/research/native-union-20260928")
BUCKET = "borsuk-bench-453182569524-euc1"


def sha(body):
    return hashlib.sha256(body).hexdigest()


def percentile(values, p):
    values = sorted(values)
    x = (len(values) - 1) * p
    low, high = math.floor(x), math.ceil(x)
    return values[low] + (values[high] - values[low]) * (x - low)


def main(attempt, family="rank16"):
    if family not in ("rank16", "cohere"):
        raise ValueError("unknown fresh dataset family")
    campaign = "fresh-" + family + "-dev64"
    directory = ROOT / campaign / attempt
    launch = json.loads((directory / "aws-launch.json").read_text())
    config = json.loads((ROOT / (campaign + "-config.json")).read_text())
    session = boto3.Session(profile_name="causality", region_name="eu-central-1")
    s3, ec2 = session.client("s3"), session.client("ec2")
    prefix = launch["prefix"]
    terminal_bytes = s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"].read()
    assert terminal_bytes == (directory / "aws-terminal.json").read_bytes()
    terminal = json.loads(terminal_bytes)
    reservation_bytes = s3.get_object(Bucket=BUCKET, Key=prefix + "/reservation.json")["Body"].read()
    reservation = json.loads(reservation_bytes)
    assert reservation == json.loads((directory / "aws-reservation.json").read_text())
    assert terminal["status"] == "complete" and terminal["exit_code"] == 0
    assert terminal["instance_id"] == launch["instance_id"]
    assert terminal["source_base_commit"] == reservation["source_base_commit"] == launch["source_base_commit"]
    assert terminal["source_archive_sha256"] == reservation["source_archive_sha256"] == launch["source_archive_sha256"]
    assert reservation["config_sha256"] == sha((ROOT / (campaign + "-config.json")).read_bytes())
    archive = s3.get_object(Bucket=BUCKET, Key="research/native-library-check/sources/"
                            + launch["source_archive_sha256"] + ".tar.gz")["Body"].read()
    assert sha(archive) == launch["source_archive_sha256"]
    for name, ident in terminal["artifacts"].items():
        body = s3.get_object(Bucket=BUCKET, Key=prefix + "/artifacts/" + name)["Body"].read()
        assert len(body) == ident["bytes"] and sha(body) == ident["sha256"], name
        assert gzip.decompress((directory / (name + ".gz")).read_bytes()) == body
    def artifact(name):
        return gzip.decompress((directory / ("screen/" + name + ".gz")).read_bytes())
    quality = json.loads(artifact("native-quality.json"))
    decision = json.loads(artifact("decision.json"))
    cgroup = json.loads(artifact("cgroup.json"))["cgroup"]
    assert quality["split"] == config["query_split"]
    assert quality["root_sha256"] == config["root_sha256"]
    assert quality["only_dev64_ranges_read"] and not quality["qualification"]
    assert decision["decision"] == ("GO fresh CoHere1M " + config["query_split"] if family == "cohere" else "GO fresh ReLAION1M development only")
    assert decision["fresh_cohort_used"] and decision["http_measured"]
    assert not decision["qualification"] and not decision["matched_control_http_measured"] and not decision["matched_vendor_measured"]

    def sealed_prefix(ident, length):
        head = s3.head_object(Bucket=BUCKET, Key=ident["key"])
        assert head["ContentLength"] == ident["bytes"] and head["Metadata"]["sha256"] == ident["sha256"]
        response = s3.get_object(Bucket=BUCKET, Key=ident["key"], Range=f"bytes=0-{length-1}",
                                 IfMatch=head["ETag"])
        body = response["Body"].read()
        assert len(body) == length and response["ContentRange"] == f"bytes 0-{length-1}/{ident['bytes']}"
        return body

    query_prefix = sealed_prefix(config["sealed"]["queries.raw"], 64 * 768 * 4)
    request_body = artifact("requests64.jsonl")
    assert request_body == sealed_prefix(config["sealed"]["requests.jsonl"], len(request_body))
    truth_body = sealed_prefix(config["sealed"]["truth.u32"], 64 * 100 * 4)
    assert quality["query_prefix_sha256"] == sha(query_prefix)
    assert quality["requests_prefix_sha256"] == sha(request_body)
    assert quality["truth_prefix_sha256"] == sha(truth_body)
    truth = [struct.unpack_from("<100I", truth_body, q * 400) for q in range(64)]
    requests = [json.loads(row) for row in request_body.splitlines()]
    assert [r["query_ordinal"] for r in requests] == list(range(64))
    refs = {}
    for k in (10, 100):
        records = [json.loads(row) for row in artifact(f"reference-k{k}.jsonl").splitlines()]
        assert len(records) == 66 and records[0]["root_sha256"] == config["root_sha256"]
        assert records[0]["top_k"] == k and records[-1]["count"] == 64
        refs[k] = records[1:-1]
        hits = 0
        for q, row in enumerate(refs[k]):
            assert row["query_ordinal"] == q and len(row["ids"]) == len(set(row["ids"])) == k
            assert len(row["ranges"]) == row["submitted_gets"] <= 32
            assert row["planned_bytes"] == row["verified_bytes"] <= 16_773_120 and row["failed_gets"] == 0
            hits += len(set(row["ids"]) & set(truth[q][:k]))
        assert quality["native_quality"][str(k)] == {
            "hits": hits, "denominator": 64 * k, "recall": hits / (64 * k)}
    assert decision["native_quality"] == quality["native_quality"]
    assert quality["native_quality"]["10"]["recall"] >= .95
    assert len(decision["runs"]) == 4
    all_gets = all_bytes = 0
    runs = []
    for rep, k in enumerate(config["setting_order"]):
        result = json.loads(artifact(f"run{rep}-k{k}/result.json"))
        assert result == decision["runs"][rep] and result["rep"] == rep and result["k"] == k
        assert result["split"] == config["query_split"] and result["fresh_cohort_used"]
        assert result["successful_count"] == 64 and result["outcomes"] == {"success": 64}
        assert result["identity_parity_valid"] and result["physical_counters_complete"]
        assert result["namespace_ready_ms"] > 0
        if k == 10:
            assert result["development_gate_passed"]
        samples = [json.loads(row) for row in artifact(f"run{rep}-k{k}/http.jsonl").splitlines()]
        assert len(samples) == 64
        hits = gets = verified_bytes = 0
        timings = []
        for q, row in enumerate(samples):
            assert row["query_ordinal"] == q and row["outcome"] == "success" and row["status"] == 200
            response = row["response"]
            assert response["authority"] == result["authority"]
            for field in ("ids", "ranges", "planned_bytes", "submitted_gets", "verified_bytes", "failed_gets"):
                assert response[field] == refs[k][q][field]
            assert row["returned_hits"] == len(set(response["ids"]) & set(truth[q][:k]))
            assert row["physical_counters_complete"] and row["integrity_error"] is None
            hits += row["returned_hits"]
            gets += response["submitted_gets"]
            verified_bytes += response["verified_bytes"]
            timings.append((row["completed_ns"] - row["started_ns"]) / 1e6)
        assert result["mean_offered_recall"] == hits / (64 * k)
        assert result["known_submitted_gets"] == gets
        assert result["known_verified_bytes"] == verified_bytes
        assert result["known_failed_gets"] == 0
        assert result["achieved_successful_qps"] >= 8
        for name, p in (("p50", .5), ("p90", .9), ("p95", .95), ("p99", .99)):
            assert abs(result["successful_incoming_http_ms"][name] - percentile(timings, p)) < 1e-9
        if k == 10:
            assert hits >= 608 and result["successful_incoming_http_ms"]["p90"] < 444
        all_gets += gets
        all_bytes += verified_bytes
        runs.append({"rep": rep, "k": k, "hits": hits, "denominator": 64 * k,
                     "incoming_http_ms": result["successful_incoming_http_ms"],
                     "successful_qps": result["achieved_successful_qps"],
                     "namespace_ready_ms": result["namespace_ready_ms"],
                     "data_gets": gets, "verified_data_bytes": verified_bytes})
    assert int(cgroup["memory.max"]) == 8 * 1024**3
    assert int(cgroup["memory.peak"]) < int(cgroup["memory.max"])
    assert int(cgroup["memory.swap.peak"]) == 0 and "oom_kill 0" in cgroup["memory.events"]
    instance = ec2.describe_instances(InstanceIds=[launch["instance_id"]])["Reservations"][0]["Instances"][0]
    assert instance["State"]["Name"] == "terminated" and instance["InstanceLifecycle"] == "spot"
    close = json.loads((directory / "aws-closeout.json").read_text())
    assert close["instance_id"] == launch["instance_id"] and close["state"] == "terminated"
    report = {"valid_measurement": True, "decision": decision["decision"],
              "qualification": False, "matched_vendor_measured": False,
              "matched_control_http_measured": False, "fresh_dev64_only": True,
              "prospective_queries_opened": False, "instance_id": launch["instance_id"],
              "state": "terminated", "terminal_sha256": sha(terminal_bytes),
              "reservation_sha256": sha(reservation_bytes), "source_archive_sha256": sha(archive),
              "config_sha256": reservation["config_sha256"], "native_quality": quality["native_quality"],
              "runs": runs, "all_offered_requests": 256, "all_successful_requests": 256,
              "physical_data_gets": all_gets, "verified_data_bytes": all_bytes,
              "cgroup_peak_bytes": int(cgroup["memory.peak"]), "swap_peak_bytes": 0,
              "instance_elapsed_seconds": close["observed_elapsed_s"],
              "compute_cost_estimate_usd": close["compute_cost_estimate_usd"],
              "cost_scope": close["cost_status"]}
    (directory / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("valid_measurement", "decision",
                                               "native_quality", "physical_data_gets",
                                               "verified_data_bytes", "cgroup_peak_bytes")}))


if __name__ == "__main__":
    family = "cohere" if len(sys.argv) > 1 and sys.argv[1] == "cohere" else "rank16"
    offset = 2 if family == "cohere" else 1
    main(sys.argv[offset] if len(sys.argv) > offset else ("a0001" if family == "cohere" else "a0002"), family)
