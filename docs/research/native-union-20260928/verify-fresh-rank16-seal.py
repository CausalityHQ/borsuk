"""Verify CLOSED rank16 construction metadata and sealed S3 object identities."""

import gzip
import hashlib
import json
from pathlib import Path
import sys

import boto3


ROOT = Path("docs/research/native-union-20260928")
BUCKET = "borsuk-bench-453182569524-euc1"


def sha(body):
    return hashlib.sha256(body).hexdigest()


def main(attempt):
    directory = ROOT / "fresh-rank16-seal" / attempt
    launch = json.loads((directory / "aws-launch.json").read_text())
    prefix = launch["prefix"]
    session = boto3.Session(profile_name="causality", region_name="eu-central-1")
    s3, ec2 = session.client("s3"), session.client("ec2")

    def original(key, path):
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        assert path.read_bytes() == body, key
        return body

    terminal_bytes = original(prefix + "/terminal.json", directory / "aws-terminal.json")
    reservation_bytes = s3.get_object(Bucket=BUCKET, Key=prefix + "/reservation.json")["Body"].read()
    terminal, reservation = json.loads(terminal_bytes), json.loads(reservation_bytes)
    assert reservation == json.loads((directory / "aws-reservation.json").read_text())
    assert terminal["status"] == "complete" and terminal["exit_code"] == 0
    assert terminal["instance_id"] == launch["instance_id"]
    assert terminal["source_base_commit"] == reservation["source_base_commit"] == launch["source_base_commit"]
    assert terminal["source_archive_sha256"] == reservation["source_archive_sha256"] == launch["source_archive_sha256"]
    assert reservation["config_sha256"] == sha((ROOT / "fresh-rank16-seal-config.json").read_bytes())
    archive = s3.get_object(Bucket=BUCKET, Key="research/native-library-check/sources/"
                            + launch["source_archive_sha256"] + ".tar.gz")["Body"].read()
    assert sha(archive) == launch["source_archive_sha256"]
    for name, ident in terminal["artifacts"].items():
        body = s3.get_object(Bucket=BUCKET, Key=prefix + "/artifacts/" + name)["Body"].read()
        assert len(body) == ident["bytes"] and sha(body) == ident["sha256"], name
        assert gzip.decompress((directory / (name + ".gz")).read_bytes()) == body
    decision = json.loads(gzip.decompress((directory / "screen/decision.json.gz").read_bytes()))
    cgroup = json.loads(gzip.decompress((directory / "screen/cgroup.json.gz").read_bytes()))
    config = json.loads((ROOT / "fresh-rank16-seal-config.json").read_text())
    panel = json.loads((ROOT / "fresh-rank16-provisional-ids.json").read_text())
    registry = json.loads(Path(config["registry_path"]).read_text())
    ranked = sorted(registry, key=lambda shard: (
        hashlib.sha256(b"borsuk-v36-screen-object-v1" + shard["path"].encode()
                       + shard["encoded_bytes"].to_bytes(8, "little")).digest(),
        shard["path"].encode()))
    expected_sources = [{"rank": rank, "path": ranked[rank]["path"],
                         "bytes": ranked[rank]["encoded_bytes"],
                         "sha256": ranked[rank]["sha256"]}
                        for rank in sorted({r["source_rank"] for r in panel["selected"]})]
    assert decision["authenticated_query_sources"] == expected_sources
    assert decision["decision"] == "PASS sealed query and exact-GT construction only"
    assert decision["queries"] == 1000 and decision["gt_k"] == 100
    assert decision["panel_sha256"] == config["panel_sha256"] == reservation["panel_sha256"]
    assert decision["indexed_source_sha256"] == config["source_parquet"]["sha256"]
    assert decision["indexed_raw_sha256"] == config["source_raw_sha256"]
    assert decision["oracle_self_check"] and not decision["qualification"] and not decision["ann_quality_measured"]
    for name, expected_size in (("queries.raw", 3_072_000), ("truth.u32", 400_000),
                                ("requests.jsonl", None)):
        ident = decision["artifacts"][name]
        assert ident["key"] == prefix + "/sealed/" + name
        if expected_size is not None:
            assert ident["bytes"] == expected_size
        head = s3.head_object(Bucket=BUCKET, Key=ident["key"])
        assert head["ContentLength"] == ident["bytes"] and head["Metadata"]["sha256"] == ident["sha256"]
        body = s3.get_object(Bucket=BUCKET, Key=ident["key"])["Body"].read()
        assert len(body) == ident["bytes"] and sha(body) == ident["sha256"]
    assert int(cgroup["memory.max"]) == 24 * 1024**3
    assert int(cgroup["memory.peak"]) < int(cgroup["memory.max"])
    assert int(cgroup["memory.swap.peak"]) == 0
    assert "oom_kill 0" in cgroup["memory.events"]
    instance = ec2.describe_instances(InstanceIds=[launch["instance_id"]])["Reservations"][0]["Instances"][0]
    assert instance["State"]["Name"] == "terminated" and instance["InstanceLifecycle"] == "spot"
    assert instance["InstanceType"] == "c7g.4xlarge"
    close = json.loads((directory / "aws-closeout.json").read_text())
    assert close["instance_id"] == launch["instance_id"] and close["state"] == "terminated"
    report = {"valid_construction": True, "qualification": False, "ann_quality_measured": False,
              "attempt": attempt, "instance_id": launch["instance_id"], "state": "terminated",
              "terminal_sha256": sha(terminal_bytes), "reservation_sha256": sha(reservation_bytes),
              "source_archive_sha256": sha(archive), "config_sha256": reservation["config_sha256"],
              "panel_sha256": decision["panel_sha256"], "authenticated_query_shards": len(expected_sources),
              "sealed_artifacts": decision["artifacts"],
              "memory_peak_bytes": int(cgroup["memory.peak"]), "swap_peak_bytes": 0,
              "oracle_wall_seconds": decision["wall_seconds"],
              "instance_elapsed_seconds": close["observed_elapsed_s"],
              "compute_cost_estimate_usd": close["compute_cost_estimate_usd"],
              "cost_scope": close["cost_status"]}
    (directory / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("valid_construction", "state",
                                             "authenticated_query_shards",
                                             "memory_peak_bytes", "compute_cost_estimate_usd")}))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "a0001")
