#!/usr/bin/env python3
"""Run one immutable two-host ReLAION-1M graph HTTP gate on Spot."""

import argparse
import hashlib
import io
import json
import subprocess
import tarfile
import time

import boto3
from botocore.exceptions import ClientError

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, PROFILE_ARN, REGION, SECURITY_GROUP, SUBNET,
    archive_source, missing, put_if_absent,
)
from scripts.launch_v219_reachable_graph_1m_spot import describe_state

SCHEMA = "borsuk-v223-authenticated-graph-http-spot-v1"
TERMINAL_SCHEMA = "borsuk-v223-authenticated-graph-http-terminal-v1"
ROOT_SHA = "c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf"
V219_TERMINAL_SHA = "782fe56ee77a7f16c39e77a6012da63d899e201a694984d919952e894cc90180"
V219_TERMINAL_KEY = ("research/v219-reachable-graph-1m/"
                     "008ab6fbc50e6293e0599a33993c619702109bd9/runs/a0002/terminal.json")
V221_PREFIX = ("research/v221-s3-validation-1m/"
               "a1ed538f8e3386d70be2f41a43f44f74325ea047/runs/a0001/")
V222_CLOSEOUT_KEY = ("research/v222-external-graph-http-1m/"
                     "1eee3ac3e8663e95a735c9f61297d5259b28b623/runs/a0001/closeout.json")
V222_CLOSEOUT_SHA = "81aba13fb991aa18868d607531cc3380d833fe1c5bf1e3d16bfcaa80ab4651e4"
V222_PREFIX = V222_CLOSEOUT_KEY.rsplit("/", 1)[0]
ARTIFACTS = {
    "server": {"ready.json", "server-resources.json", "build.log", "install.log",
               "run-closed.log"},
    "client": {"first.raw.jsonl", "first.summary.json", "first.quality.json",
               "first.time", "repeat.raw.jsonl", "repeat.summary.json",
               "repeat.quality.json", "repeat.time", "install.log", "run-closed.log"},
}
WALL_SECONDS = 7200


def bootstrap(role: str, commit: str, archive_sha: str, archive_key: str,
              prefix: str, server_ip: str = "", server_id: str = "",
              mutation_stride: int = 0, delta_encoding: str = "",
              collection_uri: str = "", collection_sha: str = "",
              root_sha: str = ROOT_SHA, generation_uri: str = "",
              cohere_dual: bool = False) -> str:
    if role not in ARTIFACTS:
        raise ValueError("unknown V223 role")
    script = f"""#!/bin/bash
set -euo pipefail
systemd-run --unit=v223-{role}-hard-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v223-authenticated-graph-http
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
export BORSUK_V223_ROLE='{role}'
export BORSUK_V223_BUCKET='{BUCKET}'
export BORSUK_V223_PREFIX='{prefix}'
export BORSUK_V223_SOURCE_COMMIT='{commit}'
export BORSUK_V223_ARCHIVE_SHA='{archive_sha}'
export BORSUK_V223_ROOT_SHA='{root_sha}'
export BORSUK_V223_SERVER_IP='{server_ip}'
export BORSUK_V223_SERVER_ID='{server_id}'
export BORSUK_V223_MUTATION_STRIDE='{mutation_stride if mutation_stride else ""}'
export BORSUK_V223_DELTA_ENCODING='{delta_encoding}'
export BORSUK_V223_COLLECTION_URI='{collection_uri}'
export BORSUK_V223_COLLECTION_SHA='{collection_sha}'
export BORSUK_V223_GENERATION_URI='{generation_uri}'
export BORSUK_V223_COHERE_DUAL='{'1' if cohere_dual else ''}'
exec bash repo/scripts/run_v223_authenticated_graph_http.sh
"""
    if len(script.encode()) > 16_384:
        raise ValueError("V223 user data exceeds EC2 limit")
    return script


def spot_request(role: str, prefix: str, user_data: str) -> dict:
    return {
        "ClientToken": "v223-" + hashlib.sha256(f"{prefix}/{role}".encode()).hexdigest()[:48],
        "ImageId": "ami-06121aa3085b6f918", "InstanceType": "c7i.4xlarge",
        "MinCount": 1, "MaxCount": 1, "IamInstanceProfile": {"Arn": PROFILE_ARN},
        "NetworkInterfaces": [{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                               "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        "InstanceMarketOptions": {"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        "InstanceInitiatedShutdownBehavior": "terminate",
        "BlockDeviceMappings": [{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True, "VolumeSize": 40,
            "VolumeType": "gp3"}}],
        "TagSpecifications": [{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": "borsuk-v223-authenticated-graph-http"},
            {"Key": "BorsukAttempt", "Value": prefix.rsplit("/", 1)[-1]},
            {"Key": "BorsukRole", "Value": role}]}],
        "UserData": user_data,
    }


def read_marker(s3, ec2, key: str, instance_id: str, seconds: int) -> bytes:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404", "NotFound"}:
                raise
        if describe_state(ec2, instance_id) in {"terminated", "shutting-down"}:
            raise RuntimeError(f"V223 {instance_id} stopped before {key}")
        time.sleep(10)
    raise TimeoutError(f"V223 {instance_id} exceeded marker deadline for {key}")


def terminal(s3, role: str, prefix: str, raw: bytes, instance_id: str,
             commit: str, archive_sha: str, mutation_stride: int = 0,
             delta_encoding: str = "", collection_uri: str = "",
             root_sha: str = ROOT_SHA, generation_uri: str = "",
             cohere_dual: bool = False) -> dict:
    value = json.loads(raw)
    artifacts = set(ARTIFACTS[role])
    if role == "server" and mutation_stride:
        artifacts.add("health.json")
    if role == "server" and (collection_uri or generation_uri):
        artifacts.add("hydrate.json")
    if role == "server" and generation_uri:
        artifacts.add("publish.json")
    if (value.get("schema") != TERMINAL_SCHEMA or value.get("role") != role
            or value.get("status") not in {"complete", "failed", "interrupted"}
            or value.get("instance_id") != instance_id
            or value.get("source_commit") != commit
            or value.get("source_archive_sha256") != archive_sha
            or value.get("generation_root_sha256") != root_sha
            or value.get("mutation_stride", 0) != mutation_stride
            or value.get("delta_encoding", "") != delta_encoding
            or value.get("collection_uri", "") != collection_uri
            or value.get("generation_uri", "") != generation_uri
            or value.get("cohere_dual", False) != cohere_dual
            or not set(value.get("artifacts", {})).issubset(artifacts)
            or (value["status"] == "complete" and (
                value.get("exit_code") != 0 or set(value["artifacts"]) != artifacts))):
        raise ValueError(f"V223 {role} terminal differs: {value}")
    for name, identity in value["artifacts"].items():
        data = s3.get_object(Bucket=BUCKET,
            Key=f"{prefix}/{role}/artifacts/{name}")["Body"].read()
        if len(data) != identity["bytes"] or hashlib.sha256(data).hexdigest() != identity["sha256"]:
            raise ValueError(f"V223 {role} artifact differs: {name}")
    if role == "client" and value["status"] == "complete":
        for label in ("first", "repeat"):
            body = s3.get_object(Bucket=BUCKET,
                Key=f"{prefix}/client/sealed/{label}.raw.jsonl")["Body"].read()
            if hashlib.sha256(body).hexdigest() != value["artifacts"][f"{label}.raw.jsonl"]["sha256"]:
                raise ValueError(f"V223 sealed {label} raw differs")
    value["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
    return value


def summaries(s3, prefix: str, receipt: dict) -> list[dict]:
    result = []
    for label in ("first", "repeat"):
        name = f"{label}.summary.json"
        raw = s3.get_object(Bucket=BUCKET,
            Key=f"{prefix}/client/artifacts/{name}")["Body"].read()
        identity = receipt["artifacts"][name]
        if len(raw) != identity["bytes"] or hashlib.sha256(raw).hexdigest() != identity["sha256"]:
            raise ValueError(f"{prefix} {name} differs")
        value = json.loads(raw)
        result.append({key: value[key] for key in
            ("pass_label", "p50_ns", "p90_ns", "p95_ns", "p99_ns", "qps", "raw_sha256")})
    return result


def terminate_confirmed(ec2, instance_id: str) -> None:
    for retry in range(5):
        try:
            if describe_state(ec2, instance_id) != "terminated":
                ec2.terminate_instances(InstanceIds=[instance_id])
                ec2.get_waiter("instance_terminated").wait(
                    InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 24})
            return
        except Exception as error:
            if retry == 4:
                raise RuntimeError(f"V223 cleanup unconfirmed for {instance_id}") from error
            time.sleep(5)


def launch(attempt: str, s3_terminal_sha: str) -> None:
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if len(s3_terminal_sha) != 64 or any(c not in "0123456789abcdef" for c in s3_terminal_sha):
        raise ValueError("V221 terminal SHA differs")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"],
                      check=False).returncode:
        raise ValueError("source is not fast-forward descendant of origin/main")
    session = boto3.Session(profile_name="causality", region_name=REGION)
    s3, ec2 = session.client("s3"), session.client("ec2")
    v219 = s3.get_object(Bucket=BUCKET, Key=V219_TERMINAL_KEY)["Body"].read()
    v221 = s3.get_object(Bucket=BUCKET, Key=V221_PREFIX + "terminal.json")["Body"].read()
    v221_receipt = json.loads(v221)
    if (hashlib.sha256(v219).hexdigest() != V219_TERMINAL_SHA
            or json.loads(v219)["status"] != "complete"
            or hashlib.sha256(v221).hexdigest() != s3_terminal_sha
            or v221_receipt["status"] != "complete"
            or v221_receipt["claim_eligible"] is not True):
        raise ValueError("V219/V221 predecessor terminal differs")
    result_identity = v221_receipt["evidence"]["result"]
    result = s3.get_object(Bucket=BUCKET,
        Key=V221_PREFIX + "evidence/result.json")["Body"].read()
    measured = json.loads(result)
    if (len(result) != result_identity["bytes"]
            or hashlib.sha256(result).hexdigest() != result_identity["sha256"]
            or measured["schema"] != "borsuk-matched-s3-vectors-relaion-1m-v2"
            or measured["split"] != "validation" or measured["metric"] != "cosine"
            or measured["query_workers"] != 8 or measured["query_order"] != "ordinal"):
        raise ValueError("V221 comparator result identity differs")
    v222 = s3.get_object(Bucket=BUCKET, Key=V222_CLOSEOUT_KEY)["Body"].read()
    v222_closeout = json.loads(v222)
    if (hashlib.sha256(v222).hexdigest() != V222_CLOSEOUT_SHA
            or v222_closeout.get("gate_pass") is not True):
        raise ValueError("V222 predecessor closeout differs")
    v222_client_raw = s3.get_object(Bucket=BUCKET,
        Key=V222_PREFIX + "/client/terminal.json")["Body"].read()
    if hashlib.sha256(v222_client_raw).hexdigest() != v222_closeout["client_terminal_sha256"]:
        raise ValueError("V222 client terminal differs")
    v222_summaries = summaries(s3, V222_PREFIX, json.loads(v222_client_raw))
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    group = ec2.describe_security_groups(GroupIds=[SECURITY_GROUP])["SecurityGroups"][0]
    if not any(rule.get("IpProtocol") == "tcp"
               and isinstance(rule.get("FromPort"), int)
               and isinstance(rule.get("ToPort"), int)
               and rule["FromPort"] <= 8080 <= rule["ToPort"]
               and any(peer.get("GroupId") == SECURITY_GROUP
               for peer in rule.get("UserIdGroupPairs", []))
               for rule in group["IpPermissions"]):
        raise ValueError("V223 peer-only TCP/8080 security group rule missing")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        if not {"scripts/run_v223_authenticated_graph_http.sh",
                "docs/research/v223-authenticated-graph-http-1m-prereg.md",
                "docs/research/v223-relaion-1m-generation.json"}.issubset(
                    source.getnames()):
            raise ValueError("V223 source archive lacks worker or prereg")
        root = source.extractfile("docs/research/v223-relaion-1m-generation.json").read()
        if hashlib.sha256(root).hexdigest() != ROOT_SHA:
            raise ValueError("V223 source archive root differs")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v223-authenticated-graph-http-1m/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v223-authenticated-graph-http-1m/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("V223 attempt already reserved")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("V223 source archive length differs")
    spot = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    spot_quote = float(spot["SpotPrice"])
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA, "source_commit": commit, "source_archive_sha256": archive_sha,
        "v219_terminal_sha256": V219_TERMINAL_SHA,
        "v221_terminal_sha256": s3_terminal_sha,
        "v222_closeout_sha256": V222_CLOSEOUT_SHA,
        "generation_root_sha256": ROOT_SHA,
        "dataset": "ReLAION-1M D768", "split": "validation-1000-already-used",
        "k": 100, "concurrency": 8, "transport": "VPC peer HTTP/1.1",
        "cache_state": "resident after authenticated hydration; no response cache",
        "passes": ["first_pass", "immediate_repeat"],
        "hardware": "two c7i.4xlarge Spot peers in eu-central-1c",
        "spot_quote_usd_per_hour_per_host": spot_quote,
        "spot_quote_timestamp": spot["Timestamp"].isoformat(),
        "gate": "both passes exact V219 IDs;99664 GT100 hits;p05=98;p95<100ms;p99<150ms;QPS>=100;server RSS<=3GiB",
        "interruption_policy": "discard both hosts and restart complete cell under new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    ids = {}
    launch_epochs = {}
    try:
        server_script = bootstrap("server", commit, archive_sha, archive_key, prefix)
        server_launch = ec2.run_instances(**spot_request("server", prefix, server_script))[
            "Instances"][0]
        server_id = server_launch["InstanceId"]
        ids["server"] = server_id
        launch_epochs["server"] = server_launch["LaunchTime"].timestamp()
        put_if_absent(prefix + "/server/launch.json", json.dumps({
            "schema": "borsuk-v223-authenticated-graph-http-launch-v1",
            "role": "server", "instance_id": server_id, "source_commit": commit,
            "launch_epoch": launch_epochs["server"],
            "spot_quote_usd_per_hour": spot_quote,
        }, sort_keys=True).encode())
        print(json.dumps({"role": "server", "instance_id": server_id,
                          "output_prefix": prefix}), flush=True)
        ready_raw = read_marker(s3, ec2, prefix + "/server/ready.json", server_id, 1800)
        ready = json.loads(ready_raw)
        if (ready.get("schema") != "borsuk-v223-authenticated-graph-http-ready-v1"
                or ready.get("instance_id") != server_id
                or ready.get("generation_root_sha256") != ROOT_SHA
                or ready.get("source_commit") != commit):
            raise ValueError("V223 server readiness identity differs")
        server_ip = ready["private_ip"]
        if len(server_ip.split(".")) != 4 or not all(part.isdigit() and 0 <= int(part) <= 255
                                                       for part in server_ip.split(".")):
            raise ValueError("V223 server private IP differs")
        actual_ip = ec2.describe_instances(InstanceIds=[server_id])[
            "Reservations"][0]["Instances"][0]["PrivateIpAddress"]
        if server_ip != actual_ip:
            raise ValueError("V223 ready IP differs from EC2 identity")
        client_script = bootstrap("client", commit, archive_sha, archive_key, prefix,
                                  server_ip, server_id)
        client_launch = ec2.run_instances(**spot_request("client", prefix, client_script))[
            "Instances"][0]
        client_id = client_launch["InstanceId"]
        ids["client"] = client_id
        launch_epochs["client"] = client_launch["LaunchTime"].timestamp()
        put_if_absent(prefix + "/client/launch.json", json.dumps({
            "schema": "borsuk-v223-authenticated-graph-http-launch-v1",
            "role": "client", "instance_id": client_id, "server_instance_id": server_id,
            "source_commit": commit, "launch_epoch": launch_epochs["client"],
            "spot_quote_usd_per_hour": spot_quote,
        }, sort_keys=True).encode())
        print(json.dumps({"role": "client", "instance_id": client_id,
                          "server_private_ip": server_ip}), flush=True)
        client_raw = read_marker(s3, ec2, prefix + "/client/terminal.json", client_id, 1200)
        client = terminal(s3, "client", prefix, client_raw, client_id, commit, archive_sha)
        if client.get("server_instance_id") != server_id:
            raise ValueError("V223 client bound wrong server")
        terminate_confirmed(ec2, client_id)
        server_raw = read_marker(s3, ec2, prefix + "/server/terminal.json", server_id, 300)
        server = terminal(s3, "server", prefix, server_raw, server_id, commit, archive_sha)
        terminate_confirmed(ec2, server_id)
        print(json.dumps({"server": server, "client": client,
                          "final_states": "terminated", "artifact_replay": "pass"},
                         sort_keys=True), flush=True)
        if client["status"] != "complete" or server["status"] != "complete":
            raise RuntimeError("V223 measurement incomplete; preserve negative artifacts")
        qualities = [json.loads(s3.get_object(Bucket=BUCKET,
            Key=f"{prefix}/client/artifacts/{label}.quality.json")["Body"].read())
            for label in ("first", "repeat")]
        resources = json.loads(s3.get_object(Bucket=BUCKET,
            Key=f"{prefix}/server/artifacts/server-resources.json")["Body"].read())
        gate_pass = (all(row["pass"] for row in qualities)
                     and resources["peak_rss_bytes"] <= 3 * 1024 ** 3)
        estimate = sum((value["worker_finished_epoch"] - launch_epochs[role])
                       * spot_quote / 3600 for role, value in
                       (("server", server), ("client", client)))
        closeout = {"schema": "borsuk-v223-authenticated-graph-http-closeout-v1",
                    "source_commit": commit, "source_archive_sha256": archive_sha,
                    "generation_root_sha256": ROOT_SHA,
                    "v222_closeout_sha256": V222_CLOSEOUT_SHA,
                    "server_instance_id": server_id,
                    "client_instance_id": client_id,
                    "server_terminal_sha256": server["terminal_sha256"],
                    "client_terminal_sha256": client["terminal_sha256"],
                    "spot_quote_usd_per_hour_per_host": spot_quote,
                    "estimated_compute_usd_to_terminal": estimate,
                    "quality": qualities, "server_peak_rss_bytes": resources["peak_rss_bytes"],
                    "gate_pass": gate_pass,
                    "v222_baseline_summaries": v222_summaries,
                    "v223_summaries": summaries(s3, prefix, client),
                    "comparison": "V223 library loader versus V222 example loader; matched request and peer shape, run differences descriptive"}
        put_if_absent(prefix + "/closeout.json",
                      json.dumps(closeout, sort_keys=True).encode())
        print(json.dumps(closeout, sort_keys=True), flush=True)
    except Exception as error:
        states = {}
        for role, instance_id in ids.items():
            try:
                row = ec2.describe_instances(InstanceIds=[instance_id])[
                    "Reservations"][0]["Instances"][0]
                states[role] = {"instance_id": instance_id,
                                "state": row["State"]["Name"],
                                "reason": row.get("StateTransitionReason", ""),
                                "spot_notice": not missing(
                                    s3, f"{prefix}/{role}/spot-interruption.json")}
            except Exception as snapshot_error:
                states[role] = {"instance_id": instance_id,
                                "snapshot_error": str(snapshot_error)}
        try:
            put_if_absent(prefix + "/failure.json", json.dumps({
                "schema": "borsuk-v223-authenticated-graph-http-failure-v1",
                "source_commit": commit, "error": str(error), "instances": states,
                "interruption_policy": "discard complete cell and restart new attempt",
            }, sort_keys=True).encode())
        except Exception:
            pass
        raise
    finally:
        for instance_id in ids.values():
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    parser.add_argument("--s3-terminal-sha", required=True)
    args = parser.parse_args()
    launch(args.attempt, args.s3_terminal_sha)
