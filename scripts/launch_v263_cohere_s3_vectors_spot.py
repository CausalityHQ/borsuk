#!/usr/bin/env python3
"""Run the frozen CoHere first1M S3 Vectors comparator once on Spot."""

import fcntl
import hashlib
import json
import subprocess

import boto3

from scripts.launch_v157_primary_feasibility_spot import BUCKET, REGION, archive_source, missing, put_if_absent
from scripts.launch_matched_s3_vectors_1m_spot import (
    ObjectIdentity, SpotTarget, build_plan, claim_launched_attempt,
    claim_reserved_attempt, ensure_unstarted, launch_one_spot, monitor_and_terminate,
)
from scripts.launch_v223_authenticated_graph_http_spot import terminate_confirmed

V261_PREFIX = ("research/v261-cohere-dual-graph-1m/"
               "2dee58896e42f84d74e2653dc0960e19d6defc63/runs/a0001")
V261_SHA = "00c7d4803354f15d5f62bea6e53fd0672a0f5fc91cc482e1fa4b20b8951a9f82"
INPUTS = {
    "source": ("vectors.raw", 3_072_000_000,
               "6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005"),
    "queries": ("requests.jsonl", 15_369_495,
                "86d9406486a2bb27aa2e603f019e078dd3ecaed47f79ec685558ba3536433812"),
    "truth": ("truth.u32", 400_000,
              "62e14eba043fafb8d8ec7c833d7d320c5d823c549683d15e5eacdff365a87f39"),
}


def get(s3, key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def launch():
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"],
                      check=False).returncode:
        raise ValueError("source is not a fast-forward descendant of origin/main")
    session = boto3.Session(profile_name="causality", region_name=REGION)
    s3, ec2 = session.client("s3"), session.client("ec2")
    prior_raw = get(s3, V261_PREFIX + "/terminal.json")
    prior = json.loads(prior_raw)
    if sha(prior_raw) != V261_SHA or prior["status"] != "complete":
        raise ValueError("V261 terminal differs")
    identities = {}
    for role, (name, size, digest) in INPUTS.items():
        if prior["artifacts"][name] != {"bytes": size, "sha256": digest}:
            raise ValueError(f"V261 {role} identity differs")
        identities[role] = ObjectIdentity(
            role, f"s3://{BUCKET}/{V261_PREFIX}/artifacts/{name}", digest, size)
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    archive = archive_source(commit)
    archive_sha = sha(archive)
    archive_key = f"research/v263-cohere-s3-vectors-1m/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v263-cohere-s3-vectors-1m/{commit}/runs/a0001"
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    quote = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    plan = build_plan(
        profile="causality", source_commit=commit,
        source_archive=ObjectIdentity("source_archive", f"s3://{BUCKET}/{archive_key}",
                                      archive_sha, len(archive)),
        inputs=identities, output_prefix=f"s3://{BUCKET}/{prefix}",
        image_id="ami-06121aa3085b6f918", image_architecture="x86_64",
        security_group_id="sg-0b1fd3e4fbde4af0d",
        instance_profile_arn="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
        targets=(SpotTarget("eu-central-1c", "subnet-0a12dbed0ca6fac25"),),
        spot_price_usd_per_hour_micros=round(float(quote["SpotPrice"]) * 1_000_000),
        vector_bucket="borsuk-v263-cohere-" + commit[:12],
        instance_type="c7i.4xlarge", split="prior_used_test", query_workers=8,
        query_order="ordinal", metric="cosine", input_format="cohere_raw",
    )
    ensure_unstarted(plan, s3_client=s3)
    claim_reserved_attempt(plan, s3_client=s3)
    instance_id = launch_one_spot(plan, ec2_client=ec2)
    print(json.dumps({"instance_id": instance_id, "prefix": prefix}), flush=True)
    try:
        claim_launched_attempt(plan, s3_client=s3, instance_id=instance_id)
        terminal = monitor_and_terminate(plan, s3_client=s3, ec2_client=ec2,
                                         instance_id=instance_id)
    finally:
        terminate_confirmed(ec2, instance_id)
    if terminal.status != "complete":
        raise RuntimeError(f"V263 terminal status {terminal.status}")
    evidence = {}
    for role, identity in terminal.evidence.items():
        key = identity.uri.removeprefix(f"s3://{BUCKET}/")
        body = get(s3, key)
        if len(body) != identity.bytes or sha(body) != identity.sha256:
            raise ValueError(f"V263 {role} readback differs")
        evidence[role] = json.loads(body) if role in {"result", "resources", "cleanup"} else None
    result = evidence["result"]
    if (result["schema"] != "borsuk-matched-s3-vectors-cohere-1m-v1"
            or result["vectors"] != 1_000_000 or result["dimensions"] != 768
            or result["metric"] != "cosine" or result["top_k"] != 100
            or result["query_workers"] != 8 or result["query_order"] != "ordinal"
            or result["samples_sha256"] != terminal.evidence["samples"].sha256
            or evidence["cleanup"]["bucket_deleted"] is not True):
        raise ValueError("V263 result identity differs")
    output = {"source_commit": commit, "instance_id": instance_id,
              "terminal_sha256": sha(get(s3, prefix + "/terminal.json")),
              "result_sha256": terminal.evidence["result"].sha256,
              "samples_sha256": terminal.evidence["samples"].sha256,
              "passes": result["passes"], "upload_seconds": result["upload_seconds"],
              "upload_logical_bytes": result["upload_logical_bytes"],
              "put_requests": result["put_requests"], "resources": evidence["resources"],
              "cleanup": evidence["cleanup"]}
    body = json.dumps(output, sort_keys=True, separators=(",", ":")).encode()
    put_if_absent(prefix + "/closeout.json", body)
    if get(s3, prefix + "/closeout.json") != body:
        raise ValueError("V263 closeout readback differs")
    print(json.dumps({"closeout_sha256": sha(body), **output}, sort_keys=True), flush=True)


if __name__ == "__main__":
    with open("/tmp/borsuk-cohere-s3-vectors-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch()
