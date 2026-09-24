#!/usr/bin/env python3
"""Launch and authenticate one frozen V150 unit-centroid graph Spot attempt."""

from __future__ import annotations

import argparse
import bisect
import base64
import gzip
import hashlib
import io
import json
import math
import shlex
import struct
import subprocess
import tarfile
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

BUCKET = "borsuk-bench-453182569524-euc1"
REGION = "eu-central-1"
TAG = "borsuk-v150-unit-centroid-graph"
RUNNER = "scripts/run_v150_unit_centroid_graph_remote.sh"
INPUTS = (
    ("research/v122-deep-image-100k/afe07cb5a9ba8518263375595f589639fdf3f4f1/runs/v122-20260924T011355Z/a0001/artifacts/queries.jsonl", 2_041_773),
    ("research/v140-budgeted-page/3213ca34cf6a8c32824dec6967b39eb06dd5fba6/runs/v140-20260924T092255Z/a0001/terminal.json", 1_738),
    ("research/v140-budgeted-page/3213ca34cf6a8c32824dec6967b39eb06dd5fba6/runs/v140-20260924T092255Z/a0001/artifacts/deep.raw.jsonl", 3_757_457),
    ("research/v146-centroid-score/ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0/runs/v146-20260924T111500Z/a0001/terminal.json", 2_844),
    ("research/v146-centroid-score/ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0/runs/v146-20260924T111500Z/a0001/artifacts/deep.centroids.bin", 600_032),
    ("research/v146-centroid-score/ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0/runs/v146-20260924T111500Z/a0001/artifacts/deep.rust-scores.bin", 1_564_000),
)


@dataclass(frozen=True)
class Plan:
    source_commit: str
    archive_uri: str
    archive_sha256: str
    archive_bytes: int
    output_prefix: str
    wall_seconds: int = 5_400
    image_id: str = "ami-06121aa3085b6f918"
    instance_type: str = "c7i.8xlarge"
    subnet_id: str = "subnet-0a12dbed0ca6fac25"
    security_group_id: str = "sg-0b1fd3e4fbde4af0d"
    instance_profile_arn: str = (
        "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
    )


def location(uri: str) -> tuple[str, str]:
    bucket, slash, key = uri.removeprefix("s3://").partition("/")
    if bucket != BUCKET or not slash or not key:
        raise ValueError("S3 location differs")
    return bucket, key.rstrip("/")


def validate(plan: Plan) -> None:
    for value, size in ((plan.source_commit, 40), (plan.archive_sha256, 64)):
        if len(value) != size or any(char not in "0123456789abcdef" for char in value):
            raise ValueError("source identity differs")
    archive_bucket, archive_key = location(plan.archive_uri)
    output_bucket, output_key = location(plan.output_prefix)
    attempt = output_key.rsplit("/", 1)[-1]
    if (
        archive_bucket != output_bucket
        or plan.source_commit not in archive_key
        or plan.archive_sha256 not in archive_key
        or not output_key.startswith(
            f"research/v150-unit-centroid-graph/{plan.source_commit}/runs/"
        )
        or not (len(attempt) == 5 and attempt.startswith("a") and attempt[1:].isdigit())
        or plan.archive_bytes <= 0
        or plan.wall_seconds != 5_400
        or plan.instance_type != "c7i.8xlarge"
    ):
        raise ValueError("immutable V150 plan differs")


def user_data(plan: Plan) -> str:
    validate(plan)
    exports = "\n".join(
        (
            f"export V150_SOURCE_COMMIT={plan.source_commit}",
            f"export V150_ARCHIVE_SHA256={plan.archive_sha256}",
            f"export V150_OUTPUT_PREFIX={shlex.quote(plan.output_prefix)}",
            f"export V150_WALL_SECONDS={plan.wall_seconds}",
        )
    )
    return f"""#!/bin/bash
set -euo pipefail
systemd-run --unit=v150-hard-stop --on-active={plan.wall_seconds}s /usr/sbin/shutdown -h now
root=/mnt/v150-unit-centroid-graph
mkdir -p "$root" && cd "$root"
{exports}
bootstrap_failure() {{
  code=$?
  trap - EXIT
  if [ "$code" -ne 0 ]; then
    token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \\
      http://169.254.169.254/latest/api/token || true)
    instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \\
      http://169.254.169.254/latest/meta-data/instance-id || true)
    INSTANCE_ID="$instance_id" EXIT_CODE="$code" python3 - <<'PY' >terminal.json
import json,os
print(json.dumps({{"schema":"borsuk-v150-unit-centroid-graph-spot-v1",
  "source_commit":os.environ["V150_SOURCE_COMMIT"],
  "source_archive_sha256":os.environ["V150_ARCHIVE_SHA256"],
  "instance_id":os.environ["INSTANCE_ID"],"exit_code":int(os.environ["EXIT_CODE"]),
  "phase":"bootstrap","status":"failed","artifacts":{{}}}},
  sort_keys=True,separators=(',',':')))
PY
    aws s3 cp terminal.json "$V150_OUTPUT_PREFIX/terminal.json" --only-show-errors || true
    shutdown -h now || true
  fi
}}
trap bootstrap_failure EXIT
aws s3 cp {shlex.quote(plan.archive_uri)} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "{plan.archive_bytes}" ]
printf '%s  source.tar.gz\\n' {plan.archive_sha256} | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
exec bash repo/{RUNNER}
"""


def launch_spec(plan: Plan) -> dict:
    token = "v150-" + hashlib.sha256(plan.output_prefix.encode()).hexdigest()[:48]
    return {
        "ClientToken": token,
        "ImageId": plan.image_id,
        "InstanceType": plan.instance_type,
        "MinCount": 1,
        "MaxCount": 1,
        "IamInstanceProfile": {"Arn": plan.instance_profile_arn},
        "NetworkInterfaces": [{
            "AssociatePublicIpAddress": True,
            "DeviceIndex": 0,
            "Groups": [plan.security_group_id],
            "SubnetId": plan.subnet_id,
        }],
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {
                "InstanceInterruptionBehavior": "terminate",
                "SpotInstanceType": "one-time",
            },
        },
        "InstanceInitiatedShutdownBehavior": "terminate",
        "BlockDeviceMappings": [{
            "DeviceName": "/dev/xvda",
            "Ebs": {"DeleteOnTermination": True, "Encrypted": True,
                    "VolumeSize": 120, "VolumeType": "gp3"},
        }],
        "TagSpecifications": [{
            "ResourceType": "instance",
            "Tags": [{"Key": "Name", "Value": TAG},
                     {"Key": "BorsukAttempt", "Value": plan.output_prefix.rsplit("/", 1)[-1]}],
        }],
        "UserData": base64.b64encode(user_data(plan).encode()).decode(),
    }


def missing(s3: object, key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return True
        raise
    return False


def check_source_and_inputs(s3: object, plan: Plan) -> None:
    subprocess.run(["git", "merge-base", "--is-ancestor", plan.source_commit,
                    "origin/main"], check=True)
    bucket, key = location(plan.archive_uri)
    response = s3.get_object(Bucket=bucket, Key=key)
    sha = hashlib.sha256()
    count = 0
    archive = io.BytesIO()
    for block in response["Body"].iter_chunks(chunk_size=4 * 1024 * 1024):
        sha.update(block)
        count += len(block)
        archive.write(block)
    if count != plan.archive_bytes or sha.hexdigest() != plan.archive_sha256:
        raise ValueError("source archive differs")
    expected_tree = subprocess.run(
        ["git", "archive", "--format=tar", plan.source_commit],
        check=True, capture_output=True,
    ).stdout
    if gzip.decompress(archive.getvalue()) != expected_tree:
        raise ValueError("source archive is not the named Git commit")
    archive.seek(0)
    with tarfile.open(fileobj=archive, mode="r:gz") as source:
        if (RUNNER not in source.getnames()
                or "crates/borsuk/src/bin/v150_unit_centroid_graph.rs" not in source.getnames()
                or "crates/borsuk/src/budgeted_page_rank.rs" not in source.getnames()
                or "crates/borsuk/src/centroid_hnsw.rs" not in source.getnames()
                or "crates/borsuk/src/unit_centroid_pages.rs" not in source.getnames()
                or "crates/borsuk/src/unit_centroid_graph.rs" not in source.getnames()
                or "crates/borsuk/src/lib.rs" not in source.getnames()
                or "crates/borsuk/Cargo.toml" not in source.getnames()
                or "Cargo.lock" not in source.getnames()
                or "docs/research/inputs/v138-deep-primary.jsonl" not in source.getnames()):
            raise ValueError("source archive lacks V150 runner or evaluator")
    for key, size in INPUTS:
        if s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] != size:
            raise ValueError(f"frozen input length differs: {key}")


def reserve(plan: Plan) -> None:
    receipt = json.dumps({
        "schema": "borsuk-v150-unit-centroid-graph-reservation-v1",
        "source_commit": plan.source_commit,
        "source_archive_sha256": plan.archive_sha256,
        "output_prefix": plan.output_prefix,
    }, sort_keys=True, separators=(",", ":")).encode()
    with tempfile.NamedTemporaryFile() as body:
        body.write(receipt)
        body.flush()
        bucket, key = location(plan.output_prefix)
        subprocess.run([
            "aws", "s3api", "put-object", "--profile", "causality",
            "--region", REGION, "--bucket", bucket,
            "--key", key + "/reservation.json", "--body", body.name,
            "--if-none-match", "*", "--output", "json",
        ], check=True, stdout=subprocess.DEVNULL)


def replay_plan(scores: dict[int, float], roster: list[int]) -> dict:
    primary = list(dict.fromkeys(row // 256 for row in roster))
    target = min(391, 4 * len(primary))
    selected: list[int] = []
    final_ranges: list[list[int]] = []
    final_bytes = 0

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
        for i, run in enumerate(runs[1:]):
            if i in joined:
                merged[-1][1] = run[1]
            else:
                merged.append(run.copy())
        ranges = [[start * 256 * 108, min(end * 256, 100000) * 108]
                  for start, end in merged]
        return ranges, sum(end - start for start, end in ranges)

    order = primary + [page for page, _ in sorted(scores.items(),
                       key=lambda item: (item[1], item[0])) if page not in primary]
    for page in order:
        bisect.insort(selected, page)
        ranges, charged = cover(selected)
        if charged > 16_777_216:
            selected.remove(page)
            continue
        final_ranges, final_bytes = ranges, charged
        if len(selected) >= target:
            break
    return {"selected_pages": selected, "ranges": final_ranges,
            "planned_bytes": final_bytes, "target_shortfall": target - len(selected)}


def recount_science(s3: object, prefix: str, decision: dict,
                    authenticated: dict[str, bytes]) -> None:
    def artifact(name: str) -> bytes:
        return authenticated[name]

    summary = json.loads(artifact("science.summary.json"))
    records = [json.loads(line) for line in artifact("science.jsonl").splitlines()]
    if (len(records) != 1000 or summary.get("schema") != "borsuk-v150-unit-centroid-graph-v1"
            or summary.get("rows") != 100000 or summary.get("dimensions") != 96
            or summary.get("query_count") != 1000 or summary.get("m") != 16
            or summary.get("m0") != 32 or summary.get("ef_construction") != 64
            or summary.get("graph_bytes") != len(artifact("science.graph.bin"))):
        raise ValueError("V150 science geometry/count differs")
    centroid_key = next(key for key, _ in INPUTS if key.endswith("/deep.centroids.bin"))
    centroids = s3.get_object(Bucket=BUCKET, Key=centroid_key)["Body"].read()
    if (len(centroids) != 600_032 or hashlib.sha256(centroids).hexdigest()
            != "9f8a924ebeacd3c512365934c49c9fc42705a1bde1098ec99c981fb1a7fe3f12"):
        raise ValueError("V146 centroid reference differs")
    graph = artifact("science.graph.bin")
    if len(graph) < 80 or graph[:8] != b"BORSUKG1":
        raise ValueError("V150 graph format differs")
    rows, dims, unit_rows, page_rows, nodes, entry, m, m0, ef = struct.unpack_from("<Q8I", graph, 8)
    if ((rows, dims, unit_rows, page_rows, nodes, m, m0, ef)
            != (100000, 96, 32, 256, 3125, 16, 32, 64)
            or entry >= nodes or graph[48:80] != hashlib.sha256(centroids).digest()):
        raise ValueError("V150 graph header differs")
    cursor = 80
    heights = []
    adjacency = []
    for node in range(nodes):
        if cursor + 2 > len(graph):
            raise ValueError("V150 graph tower truncated")
        height = struct.unpack_from("<H", graph, cursor)[0]
        cursor += 2
        if not 1 <= height <= 64:
            raise ValueError("V150 graph height differs")
        tower = []
        for level in range(height):
            if cursor + 2 > len(graph):
                raise ValueError("V150 graph layer truncated")
            count = struct.unpack_from("<H", graph, cursor)[0]
            cursor += 2
            if count > (32 if level == height - 1 else 16) or cursor + 4 * count > len(graph):
                raise ValueError("V150 graph degree differs")
            ids = struct.unpack_from(f"<{count}I", graph, cursor)
            cursor += 4 * count
            if len(ids) != len(set(ids)) or any(neighbor == node or neighbor >= nodes for neighbor in ids):
                raise ValueError("V150 graph neighbor differs")
            tower.append(ids)
        if not tower[-1]:
            raise ValueError("V150 graph base layer empty")
        heights.append(height)
        adjacency.append(tower)
    if cursor != len(graph) or heights[entry] != max(heights):
        raise ValueError("V150 graph extent differs")
    for tower in adjacency:
        for level, ids in enumerate(tower):
            if any(heights[neighbor] < len(tower) - level for neighbor in ids):
                raise ValueError("V150 graph edge level differs")
    query_key = next(key for key, _ in INPUTS if key.endswith("/queries.jsonl"))
    query_bytes = s3.get_object(Bucket=BUCKET, Key=query_key)["Body"].read()
    if (len(query_bytes) != 2_041_773 or hashlib.sha256(query_bytes).hexdigest()
            != "dd95571cc7c333f331b8c4c0b55070b366b389508b0e195e0e56d5982177da99"):
        raise ValueError("V122 query reference differs")
    queries = [json.loads(line) for line in query_bytes.splitlines()]
    if len(queries) != 1000:
        raise ValueError("V122 query count differs")
    flat_key = next(key for key, _ in INPUTS if key.endswith("/deep.rust-scores.bin"))
    flat = s3.get_object(Bucket=BUCKET, Key=flat_key)["Body"].read()
    if (len(flat) != 1_564_000 or hashlib.sha256(flat).hexdigest()
            != "341436ca66c24b0e247cf2a8e40b606d7d983cbd7b170ba4e2740f390cf0807c"):
        raise ValueError("V146 flat reference differs")
    references = struct.unpack("<391000f", flat)
    v140_key = next(key for key, _ in INPUTS if key.endswith("/deep.raw.jsonl"))
    v140_bytes = s3.get_object(Bucket=BUCKET, Key=v140_key)["Body"].read()
    if hashlib.sha256(v140_bytes).hexdigest() != "e2a1b31161bb2190cb7dc36d0601c50449c6d6aba0cc1a4985c032d713bc54d5":
        raise ValueError("V140 raw baseline differs")
    v140_records = [json.loads(line) for line in v140_bytes.splitlines()]
    if len(v140_records) != 1000:
        raise ValueError("V140 raw baseline count differs")
    roster_bytes = (Path(__file__).resolve().parents[1]
                    / "docs/research/inputs/v138-deep-primary.jsonl").read_bytes()
    if hashlib.sha256(roster_bytes).hexdigest() != "04cdbea9079837806059799d9a90c2f579526de100b7743f83a3794a15c0d6e3":
        raise ValueError("V138 primary roster differs")
    rosters = [json.loads(line) for line in roster_bytes.splitlines()]
    if len(rosters) != 1000:
        raise ValueError("V138 primary roster count differs")
    evaluations = []
    exact_evaluations = []
    distinct_evaluations = []
    total_evaluations = []
    combined_ns = []
    flat_ns = []
    capture = control_capture = reference_count = 0
    all_primary = all_caps = all_shortfall = True
    max_diff = 0.0
    for ordinal, record in enumerate(records):
        if (record["query_ordinal"] != ordinal
                or record["source_query_ordinal"] != ordinal + 9000):
            raise ValueError("V150 query ordinal differs")
        visited = record["visited_pages"]
        visited_set = {item[0] for item in visited}
        if len(visited_set) != len(visited):
            raise ValueError("V150 duplicate visited page")
        query_diff = 0.0
        for page, score in visited:
            if not (isinstance(page, int) and 0 <= page < 391
                    and math.isfinite(score)):
                raise ValueError("V150 visited page differs")
            query_diff = max(query_diff, abs(score - references[ordinal * 391 + page]))
        if abs(query_diff - record["query_max_abs_score_difference"]) > 1e-7:
            raise ValueError("V150 score difference recount differs")
        max_diff = max(max_diff, query_diff)
        roster_record = rosters[ordinal]
        if (roster_record["query_ordinal"] != ordinal
                or roster_record["source_query_ordinal"] != ordinal + 9000):
            raise ValueError("V138 primary identity differs")
        roster = roster_record["primary"]
        primary_set = {row // 256 for row in roster}
        p = record["primary_distinct_pages"]
        baseline_record = v140_records[ordinal]
        baseline_selected = set(baseline_record["variants"]["4"]["selected_pages"])
        if (baseline_record["query_ordinal"] != ordinal
                or p != len(primary_set)
                or record["primary_pages"] != sorted(primary_set)
                or record["baseline_selected_pages"] != sorted(baseline_selected)
                or record["baseline_selected_count"] != len(baseline_selected)
                or record["baseline_target_shortfall"]
                != baseline_record["variants"]["4"]["target_shortfall"]):
            raise ValueError("V150 baseline or primary geometry differs")
        cap = min(391 - p, 4 * p)
        unit_budget = 16 * p
        nearest_budget = 8 * p
        units = record["returned_units"]
        unit_ids = [item[0] for item in units]
        if (len(unit_ids) != len(set(unit_ids)) or len(unit_ids) > nearest_budget
                or any(not isinstance(unit, int) or not 0 <= unit < 3125
                       or not math.isfinite(distance) or distance < 0
                       for unit, distance in units)):
            raise ValueError("V150 visited unit record differs")
        if units != sorted(units, key=lambda item: (item[1], item[0])):
            raise ValueError("V150 returned unit order differs")
        query_record = queries[ordinal]
        if (query_record["query_ordinal"] != ordinal
                or query_record["source_query_ordinal"] != ordinal + 9000):
            raise ValueError("V122 query identity differs")
        query = query_record["query"]
        for unit, distance in units:
            center = struct.unpack_from("<96e", centroids, 32 + unit * 192)
            expected = math.sqrt(sum((left - right) ** 2 for left, right in zip(query, center)))
            if abs(expected - distance) > 1e-3:
                raise ValueError("V150 returned unit distance differs")
        expected_pages = set(primary_set)
        for unit in unit_ids:
            if len(expected_pages) < p + cap:
                expected_pages.add(unit // 8)
        if expected_pages != visited_set:
            raise ValueError("V150 unit-to-page admission differs")
        evaluated = record["evaluated_units"]
        if (len(evaluated) != len(set(evaluated))
                or any(not isinstance(unit, int) or not 0 <= unit < 3125
                       for unit in evaluated)
                or not set(unit_ids).issubset(set(evaluated))
                or record["unit_evaluations"] != len(evaluated)):
            raise ValueError("V150 evaluated unit recount differs")
        if record["work_exhausted"] and len(evaluated) != unit_budget:
            raise ValueError("V150 exhaustion recount differs")
        exact_units = {unit for page in visited_set
                       for unit in range(page * 8, min((page + 1) * 8, 3125))}
        if (record["exact_score_evaluations"] != len(exact_units)
                or record["distinct_scored_units"] != len(exact_units | set(evaluated))
                or record["total_unit_distance_computations"]
                != len(evaluated) + len(exact_units)):
            raise ValueError("V150 total scoring work recount differs")
        control_set = set(primary_set)
        for page in range(391):
            if len(control_set) == len(visited_set):
                break
            control_set.add(page)
        selected_set = set(record["selected_pages"])
        control_selected_set = set(record["control_selected_pages"])
        actual = replay_plan(dict(visited), roster)
        control = replay_plan({page: references[ordinal * 391 + page]
                               for page in control_set}, roster)
        if (record["selected_pages"] != actual["selected_pages"]
                or record["ranges"] != actual["ranges"]
                or record["planned_bytes"] != actual["planned_bytes"]
                or record["target_shortfall"] != actual["target_shortfall"]
                or record["control_selected_pages"] != control["selected_pages"]):
            raise ValueError("V150 planner replay differs")
        if (record["additional_page_budget"] != cap
                or record["nearest_unit_budget"] != nearest_budget
                or record["unit_evaluation_budget"] != unit_budget
                or len(visited) > p + cap
                or record["unit_evaluations"] > unit_budget
                or record["unit_evaluations"] < len(unit_ids)
                or record["control_visited_pages"] != len(visited)
                or len(control_set) != len(visited_set)
                or len(selected_set) != len(record["selected_pages"])
                or len(control_selected_set) != len(record["control_selected_pages"])
                or not selected_set.issubset(visited_set)
                or not control_selected_set.issubset(control_set)):
            raise ValueError("V150 search budget recount differs")
        evaluations.append(record["unit_evaluations"])
        exact_evaluations.append(record["exact_score_evaluations"])
        distinct_evaluations.append(record["distinct_scored_units"])
        total_evaluations.append(record["total_unit_distance_computations"])
        combined_ns.append(record["search_and_planner_ns"])
        flat_ns.append(record["paired_flat_score_and_planner_ns"])
        retained = primary_set.issubset(visited_set) and primary_set.issubset(selected_set)
        if record["all_primary_retained"] is not retained:
            raise ValueError("V150 primary inclusion recount differs")
        all_primary &= retained
        ranges = record["ranges"]
        range_ok = (len(ranges) == record["gets"]
                    and all(0 <= start < end <= 100000 * 108
                            for start, end in ranges)
                    and sum(end - start for start, end in ranges)
                    == record["planned_bytes"])
        query_caps = (range_ok and record["gets"] <= 32
                      and record["planned_bytes"] <= 16_777_216)
        if record["plan_caps_hold"] is not query_caps:
            raise ValueError("V150 plan cap recount differs")
        all_caps &= query_caps
        shortfall = record["target_shortfall"] <= record["baseline_target_shortfall"]
        if record["shortfall_no_worse"] is not shortfall:
            raise ValueError("V150 shortfall recount differs")
        all_shortfall &= shortfall
        query_capture = len(selected_set & baseline_selected)
        query_control_capture = len(control_selected_set & baseline_selected)
        if (record["selected_baseline_capture"] != query_capture
                or record["control_selected_baseline_capture"] != query_control_capture):
            raise ValueError("V150 page capture recount differs")
        capture += query_capture
        control_capture += query_control_capture
        reference_count += len(baseline_selected)
    if reference_count <= 0:
        raise ValueError("V150 empty baseline capture denominator")
    fraction = capture / reference_count
    control_fraction = control_capture / reference_count
    p95_cpu_ms = sorted(combined_ns)[949] / 1e6
    p95_flat_ms = sorted(flat_ns)[949] / 1e6
    verdict = ("pass" if all_primary and all_caps and all_shortfall
               and max_diff <= 0.0001
               and sorted(distinct_evaluations)[949] < 1562
               and p95_cpu_ms < p95_flat_ms
               and fraction >= 0.95 and fraction >= control_fraction + 0.05
               else "reject")
    if (summary.get("verdict") != verdict or decision.get("verdict") != verdict
            or summary.get("all_primary_retained") is not all_primary
            or summary.get("all_plan_caps") is not all_caps
            or summary.get("shortfall_no_worse") is not all_shortfall
            or summary.get("unit_evaluations_p95") != sorted(evaluations)[949]
            or summary.get("exact_score_evaluations_p95") != sorted(exact_evaluations)[949]
            or summary.get("distinct_scored_units_p95") != sorted(distinct_evaluations)[949]
            or summary.get("total_unit_distance_computations_p95") != sorted(total_evaluations)[949]
            or abs(summary.get("search_and_planner_p95_ms", -1) - p95_cpu_ms) > 1e-9
            or abs(summary.get("paired_flat_score_and_planner_p95_ms", -1) - p95_flat_ms) > 1e-9
            or summary.get("selected_baseline_capture") != capture
            or summary.get("control_selected_baseline_capture") != control_capture
            or summary.get("baseline_selected_count") != reference_count
            or abs(summary.get("max_abs_visited_score_difference", -1) - max_diff) > 1e-7):
        raise ValueError("V150 scientific verdict recount differs")


def launch_and_monitor(plan: Plan) -> dict:
    validate(plan)
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    _, prefix = location(plan.output_prefix)
    if not missing(s3, prefix + "/reservation.json") or not missing(
        s3, prefix + "/terminal.json"
    ):
        raise ValueError("immutable attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V150 worker already active")
    check_source_and_inputs(s3, plan)
    reserve(plan)
    instance_id = ec2.run_instances(**launch_spec(plan))["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id": instance_id, "output_prefix": plan.output_prefix},
                     sort_keys=True), flush=True)
    deadline = time.monotonic() + plan.wall_seconds + 1_800
    try:
        while time.monotonic() < deadline:
            if not missing(s3, prefix + "/terminal.json"):
                raw = s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"].read()
                terminal = json.loads(raw)
                if (terminal.get("schema") != "borsuk-v150-unit-centroid-graph-spot-v1"
                        or terminal.get("source_commit") != plan.source_commit
                        or terminal.get("source_archive_sha256") != plan.archive_sha256
                        or terminal.get("instance_id") != instance_id
                        or (terminal.get("status") == "complete" and (
                            terminal.get("phase") != "complete"
                            or terminal.get("exit_code") != 0
                            or terminal.get("interrupted") is not False))):
                    raise ValueError("V150 terminal identity differs")
                terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
                for _ in range(24):
                    state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
                    if state == "terminated":
                        break
                    time.sleep(5)
                else:
                    ec2.terminate_instances(InstanceIds=[instance_id])
                    ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
                terminal["instance_state"] = "terminated"
                required = {"install.log", "core-check-resources.txt",
                            "core-test-resources.txt", "core-test.log",
                            "gate-build-resources.txt", "download.log",
                            "science-resources.txt", "science.jsonl",
                            "science.summary.json", "science.graph.bin",
                            "science.cgroup-memory.txt", "decision.json", "worker.log"}
                if terminal.get("status") == "complete":
                    decision = json.loads(s3.get_object(
                        Bucket=BUCKET, Key=prefix + "/artifacts/decision.json")["Body"].read())
                    if (decision.get("schema") != "borsuk-v150-decision-v1"
                            or decision.get("verdict") not in {"pass", "reject"}):
                        raise ValueError("V150 decision differs")
                    artifacts = terminal.get("artifacts", {})
                    if not required.issubset(artifacts):
                        raise ValueError("V150 complete terminal lacks required artifacts")
                    terminal["decision"] = decision
                artifacts = terminal.get("artifacts", {})
                authenticated = {}
                for name, metadata in artifacts.items():
                    body = s3.get_object(
                        Bucket=BUCKET, Key=prefix + "/artifacts/" + name)["Body"]
                    digest = hashlib.sha256()
                    length = 0
                    chunks = []
                    for block in body.iter_chunks(chunk_size=4 * 1024 * 1024):
                        digest.update(block)
                        length += len(block)
                        chunks.append(block)
                    if length != metadata["bytes"] or digest.hexdigest() != metadata["sha256"]:
                        raise ValueError(f"V150 artifact differs: {name}")
                    authenticated[name] = b"".join(chunks)
                if terminal.get("status") == "complete":
                    recount_science(s3, prefix, terminal["decision"], authenticated)
                terminal["authenticated_artifact_count"] = len(artifacts)
                print(json.dumps(terminal, sort_keys=True), flush=True)
                return terminal
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state == "terminated":
                raise RuntimeError("V150 instance terminated without terminal marker")
            time.sleep(15)
        raise TimeoutError(f"V150 terminal missing after worker deadline: {instance_id}")
    except BaseException:
        try:
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state != "terminated":
                ec2.terminate_instances(InstanceIds=[instance_id])
                ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
        except Exception:
            pass
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--archive-uri", required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--archive-bytes", required=True, type=int)
    parser.add_argument("--output-prefix", required=True)
    args = parser.parse_args()
    terminal = launch_and_monitor(Plan(
        source_commit=args.source_commit,
        archive_uri=args.archive_uri,
        archive_sha256=args.archive_sha256,
        archive_bytes=args.archive_bytes,
        output_prefix=args.output_prefix,
    ))
    if terminal.get("status") != "complete" or terminal.get("exit_code") != 0:
        raise RuntimeError("V150 terminal reports an infrastructure failure")


if __name__ == "__main__":
    main()
