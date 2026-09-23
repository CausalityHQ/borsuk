#!/usr/bin/env python3
"""Launch one immutable rotated two-bit router decision on Causality Spot."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import time
from collections.abc import Sequence

from scripts.launch_native_geometric_layout_spot import (
    DEFAULT_TARGETS,
    FROZEN_QUERIES,
    FROZEN_SOURCE,
    FROZEN_TRUTH,
    FrozenInput,
    SourceArchiveIdentity,
    SpotLayoutPlan,
    _atomic_put,
    _q,
    _s3_location,
    _terminate_and_wait,
)
from scripts.launch_native_geometric_layout_spot import (
    build_launch_specs as geometric_launch_specs,
)
from scripts.launch_native_geometric_layout_spot import (
    build_plan as geometric_build_plan,
)

PRIOR_MEMBERSHIP = FrozenInput(
    role="geometric-membership",
    uri=(
        "s3://borsuk-bench-453182569524-euc1/research/native-geometric-router/"
        "67c88488fb17a9f02715c6d262a22225cf950de5/"
        "runs/relaion-100k-dev1000-a0001/artifacts/membership.parquet"
    ),
    sha256="f72b80f1341bd64599e51a69e627f0b9a2280be4f5235a6c5995fef8eacd866c",
    encoded_bytes=762_442,
    rows=100_000,
)
PRIOR_TREE = FrozenInput(
    role="geometric-router-tree",
    uri=(
        "s3://borsuk-bench-453182569524-euc1/research/native-geometric-router/"
        "67c88488fb17a9f02715c6d262a22225cf950de5/"
        "runs/relaion-100k-dev1000-a0001/artifacts/tree.parquet"
    ),
    sha256="e38f7e7385147eef0bd101aa510fb15579edbd07d7516505d70eb17b7326c974",
    encoded_bytes=123_743,
    rows=165,
)
PRIOR_PAGES = FrozenInput(
    role="geometric-page-representatives",
    uri=(
        "s3://borsuk-bench-453182569524-euc1/research/native-geometric-router/"
        "67c88488fb17a9f02715c6d262a22225cf950de5/"
        "runs/relaion-100k-dev1000-a0001/artifacts/pages.parquet"
    ),
    sha256="f7c3c5b39a6a0e7c7a3fd05082599283d94c3960e26a33d90c862303506a7476",
    encoded_bytes=476_403,
    rows=166,
)
PRIOR_EVIDENCE = FrozenInput(
    role="page-centered-group-evidence",
    uri=(
        "s3://borsuk-bench-453182569524-euc1/research/native-page-centered-groups/"
        "fd3b14ba84c863fede148cd9d8beea80f67dd409/"
        "runs/relaion-100k-dev1000-a0001/artifacts/evidence.json"
    ),
    sha256="24c0f36d947206141b536dd0e6d7536e29e1b56bf3c60700394546fb1e1d20d9",
    encoded_bytes=4_497_706,
    rows=1_000,
)


def build_plan(**values: object) -> SpotLayoutPlan:
    attempt = values.get("attempt", 1)
    if type(attempt) is not int or not 1 <= attempt <= 99:
        raise ValueError("rotated two-bit attempt differs")
    actual_prefix = values.get("output_prefix")
    if type(actual_prefix) is not str:
        raise ValueError("rotated two-bit output prefix differs")
    adjusted = dict(values)
    adjusted["attempt"] = 1
    adjusted["output_prefix"] = actual_prefix.replace(f"a{attempt:04d}", "a0001")
    base = geometric_build_plan(**adjusted)
    plan = dataclasses.replace(base, attempt=attempt, output_prefix=actual_prefix)
    expected = (
        "s3://borsuk-bench-453182569524-euc1/research/native-rotated-two-bit/"
        + plan.source_commit
        + f"/runs/relaion-100k-dev1000-a{attempt:04d}"
    )
    if plan.output_prefix.rstrip("/") != expected:
        raise ValueError("rotated two-bit immutable output prefix differs")
    return plan


def worker_script(plan: SpotLayoutPlan) -> str:
    """Build user data with a hard query/truth capability boundary."""
    script = """#!/bin/bash
set -euo pipefail
root=/mnt/native-rotated-two-bit
output=@OUTPUT@
phase=bootstrap
status=failed
started=$(date +%s)
MAXIMUM_RSS_BYTES=@RSS@
broker_pid=
mkdir -p "$root" && cd "$root"
exec 2>worker-stderr.log
run_capped() {
  setsid "$@" & pid=$!
  peak_bytes=0
  while kill -0 "$pid" 2>/dev/null; do
    rss_bytes=$(ps -eo pid=,pgid=,rss= | awk -v pgid="$pid" -v broker="${broker_pid:-0}" '$2 == pgid || $1 == broker { total += $3 } END { printf "%.0f", total * 1024 }')
    if [ "$rss_bytes" -gt "$peak_bytes" ]; then peak_bytes=$rss_bytes; fi
    if [ "$rss_bytes" -gt "$MAXIMUM_RSS_BYTES" ]; then
      kill -TERM -- "-$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      return 137
    fi
    sleep 1
  done
  rc=0; wait "$pid" || rc=$?
  if [ "$phase" = evaluate ]; then printf '%s\n' "$peak_bytes" > evaluate-combined-peak.txt; fi
  return "$rc"
}
check_peak_rss() {
  local peak_kib swaps
  peak_kib=$(awk -F: 'index($1,"Maximum resident set size") {gsub(/[[:space:]]/,"",$2); print $2}' "$1")
  swaps=$(awk -F: 'index($1,"Swaps") {gsub(/[[:space:]]/,"",$2); print $2}' "$1")
  [[ "$peak_kib" =~ ^[0-9]+$ ]] || return 1
  [[ "$swaps" =~ ^[0-9]+$ ]] || return 1
  (( swaps == 0 )) || return 1
  (( peak_kib * 1024 <= MAXIMUM_RSS_BYTES ))
}
terminal() {
    rc=$?; trap - EXIT; set +e; ended=$(date +%s)
    if [ -n "$broker_pid" ]; then kill -TERM "$broker_pid" 2>/dev/null || true; wait "$broker_pid" 2>/dev/null || true; fi
    if [ "$status" != complete ]; then
      aws s3 cp worker-stderr.log "$output/diagnostics/worker-stderr.log" --only-show-errors || true
      for resource in construct-resources.txt evaluate-resources.txt validate-resources.txt; do
        if [ -f "$resource" ]; then aws s3 cp "$resource" "$output/diagnostics/$resource" --only-show-errors || true; fi
      done
    fi
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token 2>/dev/null || true)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || true)
  STATUS="$status" PHASE="$phase" EXIT_CODE="$rc" STARTED="$started" ENDED="$ended" SOURCE_COMMIT=@COMMIT@ SOURCE_ARCHIVE_URI=@ARCHIVE_URI@ SOURCE_ARCHIVE_SHA256=@ARCHIVE_SHA@ SOURCE_ARCHIVE_BYTES=@ARCHIVE_BYTES@ REQUIREMENTS_SHA256=@REQUIREMENTS_SHA@ INSTANCE_ID="$instance_id" OUTPUT_PREFIX="$output" ATTEMPT=@ATTEMPT@ python3 - <<'PY'
import hashlib,json,os,pathlib
def ident(path,role):
 body=pathlib.Path(path).read_bytes()
 return {"encoded_bytes":len(body),"role":role,"sha256":hashlib.sha256(body).hexdigest(),"uri":os.environ["OUTPUT_PREFIX"]+"/artifacts/"+path}
files=(("mean","mean.bin"),("groups","groups.bin"),("code-seal","seal.json"),("sealed","sealed.json"),("evidence","evidence.json"),("result","result.json"),("validation","validation.json"),("broker-audit","broker-audit.json"),("evaluate-combined-peak","evaluate-combined-peak.txt"),("construct-resources","construct-resources.txt"),("evaluate-resources","evaluate-resources.txt"),("validate-resources","validate-resources.txt"))
complete=os.environ["STATUS"]=="complete" and int(os.environ["EXIT_CODE"])==0
value={"artifacts":{role:ident(path,role) for role,path in files} if complete else {},"attempt":int(os.environ["ATTEMPT"]),"claim_eligible":False,"elapsed_seconds":int(os.environ["ENDED"])-int(os.environ["STARTED"]),"exit_code":int(os.environ["EXIT_CODE"]),"instance_id":os.environ.get("INSTANCE_ID",""),"phase":os.environ["PHASE"],"schema":"borsuk-rotated-two-bit-terminal-v1","source_commit":os.environ["SOURCE_COMMIT"],"source_archive":{"uri":os.environ["SOURCE_ARCHIVE_URI"],"sha256":os.environ["SOURCE_ARCHIVE_SHA256"],"encoded_bytes":int(os.environ["SOURCE_ARCHIVE_BYTES"])},"requirements_sha256":os.environ["REQUIREMENTS_SHA256"],"status":os.environ["STATUS"]}
pathlib.Path("terminal.json").write_text(json.dumps(value,sort_keys=True,separators=(",",":"))+"\\n")
PY
  aws s3 cp terminal.json "$output/terminal.json" --only-show-errors || true
  rm -f source.parquet membership.parquet queries.parquet truth.parquet tree.parquet pages.parquet prior-evidence.json
  sudo shutdown -h now || true
  exit "$rc"
}
trap terminal EXIT
trap 'exit 143' TERM INT
phase=install
dnf install -y -q python3.12 python3.12-pip tar gzip time util-linux >install.log 2>&1
swapoff -a
awk '$1 == "SwapTotal:" {exit ($2 != 0)}' /proc/meminfo
phase=source
aws s3 cp @ARCHIVE_URI@ source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = @ARCHIVE_BYTES@ ]
printf '%s  source.tar.gz\n' @ARCHIVE_SHA@ | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
[ "$(cat repo/.borsuk-source-commit)" = @COMMIT@ ]
printf '%s  repo/scripts/requirements-format-bench.txt\n' @REQUIREMENTS_SHA@ | sha256sum -c -
chmod -R a+rX "$root/repo"
python3.12 -m venv .venv
.venv/bin/python -m pip install --disable-pip-version-check --quiet -r repo/scripts/requirements-format-bench.txt
aws s3 cp @SOURCE_URI@ source.parquet --only-show-errors
[ "$(stat -c%s source.parquet)" = @SOURCE_BYTES@ ]
printf '%s  source.parquet\n' @SOURCE_SHA@ | sha256sum -c -
aws s3 cp @MEMBERSHIP_URI@ membership.parquet --only-show-errors
[ "$(stat -c%s membership.parquet)" = @MEMBERSHIP_BYTES@ ]
printf '%s  membership.parquet\n' @MEMBERSHIP_SHA@ | sha256sum -c -
phase=construct
run_capped timeout @WALL@ unshare --net --fork env -i PATH="$PATH" PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=32 OMP_NUM_THREADS=32 /usr/bin/time -v -o construct-resources.txt "$root/.venv/bin/python" -m scripts.native_rotated_two_bit_cell construct --root "$root" --output-prefix "$output"
phase=seal
chmod 0444 source.parquet membership.parquet mean.bin groups.bin seal.json sealed.json
aws s3 cp mean.bin "$output/artifacts/mean.bin" --only-show-errors
aws s3 cp groups.bin "$output/artifacts/groups.bin" --only-show-errors
aws s3 cp seal.json "$output/artifacts/seal.json" --only-show-errors
aws s3 cp sealed.json "$output/artifacts/sealed.json" --only-show-errors
aws s3 cp construct-resources.txt "$output/artifacts/construct-resources.txt" --only-show-errors
phase=evaluate
aws s3 cp @PRIOR_EVIDENCE_URI@ prior-evidence.json --only-show-errors
[ "$(stat -c%s prior-evidence.json)" = @PRIOR_EVIDENCE_BYTES@ ]
printf '%s  prior-evidence.json\n' @PRIOR_EVIDENCE_SHA@ | sha256sum -c -
aws s3 cp @TREE_URI@ tree.parquet --only-show-errors
[ "$(stat -c%s tree.parquet)" = @TREE_BYTES@ ]
printf '%s  tree.parquet\n' @TREE_SHA@ | sha256sum -c -
aws s3 cp @PAGES_URI@ pages.parquet --only-show-errors
[ "$(stat -c%s pages.parquet)" = @PAGES_BYTES@ ]
printf '%s  pages.parquet\n' @PAGES_SHA@ | sha256sum -c -
aws s3 cp @QUERIES_URI@ queries.parquet --only-show-errors
[ "$(stat -c%s queries.parquet)" = @QUERIES_BYTES@ ]
printf '%s  queries.parquet\n' @QUERIES_SHA@ | sha256sum -c -
aws s3 cp @TRUTH_URI@ truth.parquet --only-show-errors
[ "$(stat -c%s truth.parquet)" = @TRUTH_BYTES@ ]
printf '%s  truth.parquet\n' @TRUTH_SHA@ | sha256sum -c -
chmod 0444 tree.parquet pages.parquet queries.parquet truth.parquet prior-evidence.json
mkdir evaluation && chown nobody:nobody evaluation
env PYTHONPATH="$root/repo" "$root/.venv/bin/python" -m scripts.native_rotated_two_bit_range_broker --root "$root" --output-prefix "$output" --socket "$root/broker.sock" --audit "$root/broker-audit.json" & broker_pid=$!
for attempt in $(seq 1 100); do
  [ -S "$root/broker.sock" ] && break
  kill -0 "$broker_pid" 2>/dev/null
  sleep 0.1
done
[ -S "$root/broker.sock" ]
run_capped /usr/bin/time -v -o evaluate-resources.txt timeout @WALL@ unshare --net --fork setpriv --reuid=nobody --regid=nobody --clear-groups env -i PATH="$PATH" PYTHONPATH="$root/repo" BORSUK_RANGE_BROKER_SOCKET="$root/broker.sock" AWS_EC2_METADATA_DISABLED=true OPENBLAS_NUM_THREADS=32 OMP_NUM_THREADS=32 "$root/.venv/bin/python" -m scripts.native_rotated_two_bit_cell evaluate --root "$root" --out "$root/evaluation" --output-prefix "$output" --source-archive-uri @ARCHIVE_URI@ --source-archive-sha256 @ARCHIVE_SHA@ --source-archive-bytes @ARCHIVE_BYTES@ --requirements-sha256 @REQUIREMENTS_SHA@
kill -TERM "$broker_pid"
wait "$broker_pid"
broker_pid=
mv evaluation/evidence.json evidence.json
mv evaluation/result.json result.json
rmdir evaluation
python3 - <<'PY'
import json,pathlib
audit=json.loads(pathlib.Path('broker-audit.json').read_bytes())
samples=json.loads(pathlib.Path('evidence.json').read_bytes())['samples']
assert audit['gets']==sum(sample['code_gets'] for sample in samples)
assert audit['bytes']==sum(sample['code_bytes'] for sample in samples)
PY
aws s3 cp evidence.json "$output/artifacts/evidence.json" --only-show-errors
aws s3 cp result.json "$output/artifacts/result.json" --only-show-errors
aws s3 cp broker-audit.json "$output/artifacts/broker-audit.json" --only-show-errors
aws s3 cp evaluate-combined-peak.txt "$output/artifacts/evaluate-combined-peak.txt" --only-show-errors
aws s3 cp evaluate-resources.txt "$output/artifacts/evaluate-resources.txt" --only-show-errors
phase=validate
run_capped /usr/bin/time -v -o validate-resources.txt timeout @WALL@ env PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=32 OMP_NUM_THREADS=32 "$root/.venv/bin/python" -m scripts.native_rotated_two_bit_cell validate --root "$root" --out "$root" --output-prefix "$output" --source-commit @COMMIT@ --source-archive-uri @ARCHIVE_URI@ --source-archive-sha256 @ARCHIVE_SHA@ --source-archive-bytes @ARCHIVE_BYTES@ --requirements-sha256 @REQUIREMENTS_SHA@
aws s3 cp validation.json "$output/artifacts/validation.json" --only-show-errors
aws s3 cp validate-resources.txt "$output/artifacts/validate-resources.txt" --only-show-errors
phase=resource-gate
for resource in construct-resources.txt evaluate-resources.txt validate-resources.txt; do
  check_peak_rss "$resource"
done
(( $(cat evaluate-combined-peak.txt) <= MAXIMUM_RSS_BYTES ))
awk '$1 == "SwapTotal:" {exit ($2 != 0)}' /proc/meminfo
status=complete
phase=complete
"""
    replacements = {
        "@OUTPUT@": _q(plan.output_prefix.rstrip("/")),
        "@RSS@": str(plan.maximum_rss_bytes),
        "@COMMIT@": _q(plan.source_commit),
        "@ATTEMPT@": str(plan.attempt),
        "@ARCHIVE_URI@": _q(plan.source_archive.uri),
        "@ARCHIVE_BYTES@": str(plan.source_archive.encoded_bytes),
        "@ARCHIVE_SHA@": _q(plan.source_archive.sha256),
        "@REQUIREMENTS_SHA@": _q(plan.requirements_sha256),
        "@SOURCE_URI@": _q(plan.source.uri),
        "@SOURCE_BYTES@": str(plan.source.encoded_bytes),
        "@SOURCE_SHA@": _q(plan.source.sha256),
        "@MEMBERSHIP_URI@": _q(PRIOR_MEMBERSHIP.uri),
        "@MEMBERSHIP_BYTES@": str(PRIOR_MEMBERSHIP.encoded_bytes),
        "@MEMBERSHIP_SHA@": _q(PRIOR_MEMBERSHIP.sha256),
        "@TREE_URI@": _q(PRIOR_TREE.uri),
        "@TREE_BYTES@": str(PRIOR_TREE.encoded_bytes),
        "@TREE_SHA@": _q(PRIOR_TREE.sha256),
        "@PAGES_URI@": _q(PRIOR_PAGES.uri),
        "@PAGES_BYTES@": str(PRIOR_PAGES.encoded_bytes),
        "@PAGES_SHA@": _q(PRIOR_PAGES.sha256),
        "@PRIOR_EVIDENCE_URI@": _q(PRIOR_EVIDENCE.uri),
        "@PRIOR_EVIDENCE_BYTES@": str(PRIOR_EVIDENCE.encoded_bytes),
        "@PRIOR_EVIDENCE_SHA@": _q(PRIOR_EVIDENCE.sha256),
        "@QUERIES_URI@": _q(plan.queries.uri),
        "@QUERIES_BYTES@": str(plan.queries.encoded_bytes),
        "@QUERIES_SHA@": _q(plan.queries.sha256),
        "@TRUTH_URI@": _q(plan.truth.uri),
        "@TRUTH_BYTES@": str(plan.truth.encoded_bytes),
        "@TRUTH_SHA@": _q(plan.truth.sha256),
        "@WALL@": str(plan.wall_seconds),
    }
    for marker, value in replacements.items():
        script = script.replace(marker, value)
    if len(script.encode()) > 16_384:
        raise ValueError("rotated two-bit Spot user data exceeds EC2 limit")
    return script


def build_launch_specs(plan: SpotLayoutPlan) -> list[dict[str, object]]:
    specs = geometric_launch_specs(plan)
    user_data = worker_script(plan)
    for spec in specs:
        zone = spec["NetworkInterfaces"][0]["SubnetId"]
        token = hashlib.sha256(
            f"rotated two-bit:{plan.source_commit}:{zone}:a{plan.attempt:04d}".encode()
        ).hexdigest()[:32]
        spec["ClientToken"] = "native-rotated-two-bit-" + token
        spec["UserData"] = user_data
        spec["TagSpecifications"][0]["Tags"][0]["Value"] = (
            "borsuk-native-rotated-two-bit"
        )
        spec["TagSpecifications"][0]["Tags"][1]["Value"] = f"a{plan.attempt:04d}"
    return specs


def _validate_terminal_bytes(
    body: bytes, plan: SpotLayoutPlan, instance_id: str
) -> dict[str, object]:
    try:
        terminal = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("rotated two-bit terminal JSON differs") from error
    canonical = (
        json.dumps(terminal, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    if (
        body != canonical
        or type(terminal) is not dict
        or set(terminal)
        != {
            "artifacts",
            "attempt",
            "claim_eligible",
            "elapsed_seconds",
            "exit_code",
            "instance_id",
            "phase",
            "schema",
            "source_commit",
            "source_archive",
            "requirements_sha256",
            "status",
        }
    ):
        raise ValueError("rotated two-bit terminal canonical bytes differ")
    if (
        terminal["schema"] != "borsuk-rotated-two-bit-terminal-v1"
        or terminal["attempt"] != plan.attempt
        or terminal["claim_eligible"] is not False
        or terminal["source_commit"] != plan.source_commit
        or terminal["requirements_sha256"] != plan.requirements_sha256
        or terminal["instance_id"] != instance_id
        or type(terminal["elapsed_seconds"]) is not int
        or terminal["elapsed_seconds"] < 0
        or type(terminal["exit_code"]) is not int
    ):
        raise ValueError("rotated two-bit terminal authority differs")
    if terminal["source_archive"] != dataclasses.asdict(plan.source_archive):
        raise ValueError("rotated two-bit source archive differs")
    complete = (
        terminal["status"] == terminal["phase"] == "complete"
        and terminal["exit_code"] == 0
    )
    if not complete:
        if (
            terminal["status"] != "failed"
            or terminal["exit_code"] == 0
            or terminal["artifacts"] != {}
        ):
            raise ValueError("rotated two-bit failed terminal differs")
        return terminal
    names = {
        "mean": "mean.bin",
        "groups": "groups.bin",
        "code-seal": "seal.json",
        "sealed": "sealed.json",
        "evidence": "evidence.json",
        "result": "result.json",
        "validation": "validation.json",
        "broker-audit": "broker-audit.json",
        "evaluate-combined-peak": "evaluate-combined-peak.txt",
        "construct-resources": "construct-resources.txt",
        "evaluate-resources": "evaluate-resources.txt",
        "validate-resources": "validate-resources.txt",
    }
    artifacts = terminal["artifacts"]
    if type(artifacts) is not dict or set(artifacts) != set(names):
        raise ValueError("rotated two-bit terminal artifact roster differs")
    for role, name in names.items():
        identity = artifacts[role]
        if (
            type(identity) is not dict
            or set(identity) != {"role", "uri", "sha256", "encoded_bytes"}
            or identity["role"] != role
            or identity["uri"] != plan.output_prefix.rstrip("/") + "/artifacts/" + name
            or type(identity["encoded_bytes"]) is not int
            or identity["encoded_bytes"] <= 0
            or type(identity["sha256"]) is not str
            or len(identity["sha256"]) != 64
            or any(
                character not in "0123456789abcdef" for character in identity["sha256"]
            )
        ):
            raise ValueError("rotated two-bit terminal artifact identity differs")
    return terminal


def _instance_state(ec2_client: object, instance_id: str) -> str | None:
    """Return None while a newly launched instance is propagating in EC2."""
    try:
        response = ec2_client.describe_instances(InstanceIds=[instance_id])
    except Exception as error:
        code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
        if code == "InvalidInstanceID.NotFound":
            return None
        raise
    return response["Reservations"][0]["Instances"][0]["State"]["Name"]


def launch_and_monitor(plan: SpotLayoutPlan) -> dict[str, object]:
    import boto3

    session = boto3.Session(profile_name=plan.profile, region_name="eu-central-1")
    ec2 = session.client("ec2")
    s3 = session.client("s3")
    bucket, prefix = _s3_location(plan.output_prefix)
    existing = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/", MaxKeys=1)
    if existing.get("KeyCount", 0) or existing.get("Contents"):
        raise ValueError("rotated two-bit immutable attempt already exists")
    reservation = {
        "schema": "borsuk-rotated-two-bit-reservation-v1",
        "attempt": plan.attempt,
        "source_commit": plan.source_commit,
        "source_archive": dataclasses.asdict(plan.source_archive),
        "requirements_sha256": plan.requirements_sha256,
        "prior_membership_sha256": PRIOR_MEMBERSHIP.sha256,
    }
    _atomic_put(
        s3,
        bucket=bucket,
        key=f"{prefix}/reservation.json",
        body=(
            json.dumps(reservation, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode(),
    )
    instance_id = None
    capacity_markers = ("InsufficientInstanceCapacity", "MaxSpotInstanceCountExceeded")
    for spec in build_launch_specs(plan):
        try:
            response = ec2.run_instances(**spec)
        except Exception as error:
            if any(marker in str(error) for marker in capacity_markers):
                continue
            raise
        instance_id = response["Instances"][0]["InstanceId"]
        break
    if instance_id is None:
        raise RuntimeError(
            "rotated two-bit Spot capacity unavailable; no instance launched"
        )
    deadline = time.monotonic() + plan.wall_seconds + 900
    try:
        while True:
            try:
                response = s3.get_object(Bucket=bucket, Key=f"{prefix}/terminal.json")
            except Exception as error:
                code = str(
                    getattr(error, "response", {}).get("Error", {}).get("Code", "")
                )
                if code not in {"404", "NoSuchKey", "NotFound"}:
                    raise
                state = _instance_state(ec2, instance_id)
                if state in {"terminated", "shutting-down", "stopped"}:
                    raise RuntimeError(
                        f"rotated two-bit instance {instance_id} ended without terminal"
                    ) from error
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "rotated two-bit terminal deadline exceeded"
                    ) from error
                time.sleep(15)
                continue
            body = response["Body"].read()
            return _validate_terminal_bytes(body, plan, instance_id)
    finally:
        _terminate_and_wait(ec2, instance_id)


def parse_args(argv: Sequence[str] | None = None) -> SpotLayoutPlan:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-archive-uri", required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--source-archive-bytes", type=int, required=True)
    parser.add_argument("--requirements-sha256", required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--attempt", type=int, default=1)
    parser.add_argument("--image-id", default="ami-06121aa3085b6f918")
    parser.add_argument("--security-group-id", default="sg-0b1fd3e4fbde4af0d")
    parser.add_argument(
        "--instance-profile-arn",
        default="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
    )
    args = parser.parse_args(argv)
    return build_plan(
        profile="causality",
        source_commit=args.source_commit,
        source_archive=SourceArchiveIdentity(
            args.source_archive_uri,
            args.source_archive_sha256,
            args.source_archive_bytes,
        ),
        source=FROZEN_SOURCE,
        queries=FROZEN_QUERIES,
        truth=FROZEN_TRUTH,
        requirements_sha256=args.requirements_sha256,
        output_prefix=args.output_prefix,
        attempt=args.attempt,
        image_id=args.image_id,
        security_group_id=args.security_group_id,
        instance_profile_arn=args.instance_profile_arn,
        targets=DEFAULT_TARGETS,
    )


def main(argv: Sequence[str] | None = None) -> None:
    terminal = launch_and_monitor(parse_args(argv))
    print(json.dumps(terminal, sort_keys=True, separators=(",", ":")))
    if terminal["status"] != "complete":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
