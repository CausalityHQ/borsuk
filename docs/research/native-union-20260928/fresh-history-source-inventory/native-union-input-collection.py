"""Bind CLOSED native-union reservations to exact historical config input bytes.

Reads only terminal/reservation JSON and Git config blobs, never measurements,
query vectors, ground truth or source Parquet bodies.
"""

import hashlib
import json
from pathlib import Path
import subprocess

import boto3


ROOT = Path(__file__).resolve().parent.parent
PREFIX = "research/native-union/20260928/"
BUCKET = "borsuk-bench-453182569524-euc1"


def sha(body):
    return hashlib.sha256(body).hexdigest()


def config_versions():
    versions = {}
    for path in ROOT.glob("*config.json"):
        relative = path.relative_to(ROOT.parents[2])
        bodies = [("HEAD", path.read_bytes())]
        commits = subprocess.run(["git", "log", "--all", "--format=%H", "--", str(relative)],
                                 capture_output=True, text=True, check=True).stdout.splitlines()
        for commit in commits:
            shown = subprocess.run(["git", "show", f"{commit}:{relative}"], capture_output=True)
            if shown.returncode == 0:
                bodies.append((commit, shown.stdout))
        for commit, body in bodies:
            versions.setdefault(sha(body), {"path": str(relative), "commit": commit,
                                           "config": json.loads(body)})
    return versions


def source_refs(value, path=""):
    refs = []
    if isinstance(value, dict):
        if "sha256" in value and ("key" in value or "uri" in value):
            if any(tag in path.lower() for tag in ("request", "query", "source", "chunks")):
                refs.append({"role": path, "key_or_uri": value.get("key", value.get("uri")),
                             "sha256": value["sha256"]})
        for key, child in value.items():
            refs.extend(source_refs(child, path + "/" + key))
    elif isinstance(value, list):
        for ordinal, child in enumerate(value):
            refs.extend(source_refs(child, path + "/" + str(ordinal)))
    return refs


def main():
    versions = config_versions()
    s3 = boto3.Session(profile_name="causality").client("s3", region_name="eu-central-1")
    terminals = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        terminals.extend(item["Key"] for item in page.get("Contents", [])
                         if item["Key"].endswith("/terminal.json"))
    assert len(terminals) >= 50
    records = []
    for key in sorted(terminals):
        root = key.removesuffix("terminal.json")
        terminal_bytes = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        reservation_bytes = s3.get_object(Bucket=BUCKET, Key=root + "reservation.json")["Body"].read()
        terminal, reservation = json.loads(terminal_bytes), json.loads(reservation_bytes)
        assert terminal["status"] in ("complete", "failed")
        requested = reservation.get("config_sha256")
        version = versions.get(requested)
        refs = source_refs(version["config"]) if version else []
        invalid_source_proof = None
        if key == PREFIX + "fresh-a0001/terminal.json":
            identity = terminal["artifacts"]["screen/decision.json"]
            body = s3.get_object(Bucket=BUCKET, Key=root + "artifacts/screen/decision.json")["Body"].read()
            decision = json.loads(body)
            assert sha(body) == identity["sha256"] and len(body) == identity["bytes"]
            assert decision["decision"] == "INVALID prospective cohort source mismatch"
            assert decision["sealed"] is False and decision["qualification"] is False
            invalid_source_proof = {"decision_sha256": sha(body), "sealed": False,
                                    "decision": decision["decision"]}
        records.append({"terminal_key": key, "terminal_sha256": sha(terminal_bytes),
                        "terminal_status": terminal["status"],
                        "reservation_key": root + "reservation.json",
                        "reservation_sha256": sha(reservation_bytes),
                        "scope": reservation.get("scope"),
                        "config_sha256": requested,
                        "config_path": version["path"] if version else None,
                        "config_commit": version["commit"] if version else None,
                        "invalid_source_proof": invalid_source_proof,
                        "source_query_refs": refs})
    report = {"metadata_only": True, "query_gt_or_sealed_bodies_opened": False,
              "complete_prior_query_audit": False, "terminal_count": len(records),
              "config_bound_count": sum(bool(row["config_path"]) for row in records),
              "unresolved_config_count": sum(bool(row["config_sha256"] and not row["config_path"])
                                             for row in records),
              "records": records}
    (ROOT / "fresh-native-union-runtime-inputs.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("terminal_count", "config_bound_count",
                                             "unresolved_config_count")}))


if __name__ == "__main__":
    main()
