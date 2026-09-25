#!/usr/bin/env python3
"""One frozen Spot attempt for V239 graph versus flat PQ containment."""

import argparse
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

import boto3

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, REGION, archive_source, missing, put_if_absent,
)
from scripts.launch_v218_reachable_graph_100k_spot import INPUTS, LATE, downloads
from scripts.launch_v224_graph_store_s3_spot import (
    instance_request, terminal_marker, terminate_confirmed,
)

PARENT = ("research/v218-reachable-graph-100k/"
          "ce317cac8d1eb0a1b8a8610f0a3756b96090514c/runs/a0001/")
PARENT_TERMINAL_SHA = "cf44ecb95a9d1cc07ac895a222a407c37db090c499f15a887e887d74c8492efd"
ARTIFACTS = ("raw.jsonl", "summary.json", "quality.json", "run-closed.log", "build.log", "install.log", "run.time")


def bootstrap(commit, archive_sha, archive_key, prefix, inputs, truth):
    template = r'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v239-hard-stop --on-active=7200s /usr/sbin/shutdown -h now
root=/mnt/v239-graph-containment-100k
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  for name in @@ARTIFACTS@@; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://@@BUCKET@@/@@PREFIX@@/artifacts/$name" --only-show-errors || code=96
    fi
  done
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os,time
from pathlib import Path
artifacts={}
for name in @@ARTIFACTS_PY@@:
    path=Path(name)
    if path.is_file():
        h=hashlib.sha256()
        with path.open('rb') as source:
            for block in iter(lambda: source.read(1024*1024),b''):
                h.update(block)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':h.hexdigest()}
code=int(os.environ['EXIT_CODE']);phase=os.environ['PHASE']
print(json.dumps({'schema':'borsuk-v239-graph-containment-100k-terminal-v1',
  'source_commit':'@@COMMIT@@','source_archive_sha256':'@@ARCHIVE_SHA@@',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'worker_finished_epoch':time.time(),'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json 's3://@@BUCKET@@/@@PREFIX@@/terminal.json' --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://@@BUCKET@@/@@ARCHIVE_KEY@@' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '@@ARCHIVE_SHA@@' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q python3.12 python3.12-pip gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
phase=inputs
@@INPUT_DOWNLOADS@@
phase=build
cd repo
"$CARGO_HOME/bin/cargo" build --release --locked -p borsuk --bin v239_graph_containment_100k --jobs 6 >"$root/build.log" 2>&1
"$CARGO_HOME/bin/cargo" test --release --locked -p borsuk --lib graph_ties_keep_smallest_physical_ordinals --jobs 6 >>"$root/build.log" 2>&1
cd "$root"
phase=candidates
/usr/bin/time -v -o run.time "$CARGO_TARGET_DIR/release/v239_graph_containment_100k" \
  prepare.json build-summary.json plane.bin graph.bin new-to-old.u32 books.raw codes.raw \
  requests.jsonl raw.jsonl summary.json
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/raw.jsonl' \
  --body raw.jsonl --if-none-match '*' --no-cli-pager >/dev/null
raw_sha=$(sha256sum raw.jsonl | cut -d ' ' -f1)
remote_sha=$(aws s3 cp 's3://@@BUCKET@@/@@PREFIX@@/sealed/raw.jsonl' - \
  --only-show-errors | sha256sum | cut -d ' ' -f1)
[ "$raw_sha" = "$remote_sha" ]
phase=truth
@@TRUTH_DOWNLOAD@@
.venv/bin/python -m scripts.v239_score_graph_containment_100k \
  --raw raw.jsonl --summary summary.json --truth truth.parquet --output quality.json
phase=complete
'''
    values = {
        "ARTIFACTS": " ".join(ARTIFACTS), "ARTIFACTS_PY": repr(ARTIFACTS),
        "BUCKET": BUCKET, "PREFIX": prefix, "COMMIT": commit,
        "ARCHIVE_SHA": archive_sha, "ARCHIVE_KEY": archive_key,
        "INPUT_DOWNLOADS": downloads(inputs), "TRUTH_DOWNLOAD": downloads(truth),
    }
    for key, value in values.items():
        template = template.replace("@@" + key + "@@", value)
    if "@@" in template:
        raise ValueError("unresolved V239 worker placeholder")
    return template


def read_verified(s3, key, expected):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"]
    sha = hashlib.sha256()
    data = bytearray()
    for block in body.iter_chunks(chunk_size=1024 * 1024):
        sha.update(block)
        data.extend(block)
    if sha.hexdigest() != expected:
        raise ValueError(f"artifact digest differs: {key}")
    return bytes(data)


def download_verified(s3, key, path, expected_sha, expected_bytes):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"]
    sha = hashlib.sha256()
    length = 0
    with path.open("wb") as output:
        for block in body.iter_chunks(chunk_size=1024 * 1024):
            output.write(block)
            sha.update(block)
            length += len(block)
    if length != expected_bytes or sha.hexdigest() != expected_sha:
        raise ValueError(f"artifact identity differs: {key}")


def launch(attempt):
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"],
                      check=False).returncode:
        raise ValueError("source is not a fast-forward descendant of origin/main")
    session = boto3.Session(profile_name="causality", region_name=REGION)
    s3, ec2 = session.client("s3"), session.client("ec2")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    parent = json.loads(read_verified(s3, PARENT + "terminal.json", PARENT_TERMINAL_SHA))
    if parent.get("status") != "complete":
        raise ValueError("V218 parent not closed")
    chosen = [row for row in INPUTS if row[0] in {
        "prepare.json", "plane.bin", "new-to-old.u32", "books.raw", "codes.raw", "requests.jsonl"}]
    if len(chosen) != 6:
        raise ValueError("V218 input roster differs")
    extra = [(name, PARENT + "artifacts/" + name,
              parent["artifacts"][name]["bytes"], parent["artifacts"][name]["sha256"])
             for name in ("build-summary.json", "graph.bin")]
    truth = tuple(row for row in LATE if row[0] == "truth.parquet")
    if len(truth) != 1:
        raise ValueError("truth authority differs")
    archive = archive_source(commit)
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v239-graph-containment-100k/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v239-graph-containment-100k/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    quote = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    spot_price = float(quote["SpotPrice"])
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": "borsuk-v239-graph-containment-100k-reservation-v1",
        "source_commit": commit, "source_archive_sha256": archive_sha,
        "parent_terminal_sha256": PARENT_TERMINAL_SHA,
        "dataset": "ReLAION-100k D768", "queries": "0:256 development, 256:1000 method-held-out",
        "k": 100, "ef": 4096, "shortlist_prefixes": [512, 1024, 2048, 4096],
        "gate": "smallest graph prefix with >=99.6% GT100 and p05>=98 on each split has p95 minimum SQ8 page bytes <=16MiB",
        "hardware": "c7i.4xlarge Spot eu-central-1c",
        "spot_quote_usd_per_hour": spot_price,
        "spot_quote_timestamp": quote["Timestamp"].isoformat(),
        "interruption_policy": "discard interrupted cell; restart under new prefix",
    }, sort_keys=True).encode())
    instance_id = None
    terminated = False
    try:
        request = instance_request(prefix, bootstrap(commit, archive_sha, archive_key,
                                                     prefix, chosen + extra, truth))
        request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v239-graph-containment-100k"
        row = ec2.run_instances(**request)["Instances"][0]
        instance_id = row["InstanceId"]
        launched = row["LaunchTime"].timestamp()
        put_if_absent(prefix + "/launch.json", json.dumps({
            "instance_id": instance_id, "launch_epoch": launched,
            "source_commit": commit, "spot_quote_usd_per_hour": spot_price,
        }, sort_keys=True).encode())
        print(json.dumps({"instance_id": instance_id, "prefix": prefix}), flush=True)
        raw = terminal_marker(s3, ec2, prefix + "/terminal.json", instance_id)
        terminate_confirmed(ec2, instance_id)
        terminated = True
        terminal = json.loads(raw)
        terminal_sha = hashlib.sha256(raw).hexdigest()
        if (terminal.get("schema") != "borsuk-v239-graph-containment-100k-terminal-v1"
                or terminal.get("instance_id") != instance_id
                or terminal.get("source_commit") != commit
                or terminal.get("source_archive_sha256") != archive_sha
                or not set(terminal.get("artifacts", {})).issubset(ARTIFACTS)):
            raise ValueError("terminal authority differs")
        for name, identity in terminal["artifacts"].items():
            body = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/artifacts/{name}")["Body"]
            sha = hashlib.sha256()
            length = 0
            for block in body.iter_chunks(chunk_size=1024 * 1024):
                sha.update(block)
                length += len(block)
            if length != identity["bytes"] or sha.hexdigest() != identity["sha256"]:
                raise ValueError(f"terminal artifact differs: {name}")
        if terminal["status"] != "complete" or set(terminal["artifacts"]) != set(ARTIFACTS):
            raise RuntimeError("V239 cell failed or Spot interrupted")
        quality = json.loads(read_verified(s3, f"{prefix}/artifacts/quality.json",
                                           terminal["artifacts"]["quality.json"]["sha256"]))
        with tempfile.TemporaryDirectory(prefix="borsuk-v239-replay-") as directory:
            root = Path(directory)
            raw_path, summary_path, truth_path = (root / name for name in
                                                   ("raw.jsonl", "summary.json", "truth.parquet"))
            download_verified(s3, prefix + "/sealed/raw.jsonl", raw_path,
                              terminal["artifacts"]["raw.jsonl"]["sha256"],
                              terminal["artifacts"]["raw.jsonl"]["bytes"])
            download_verified(s3, prefix + "/artifacts/summary.json", summary_path,
                              terminal["artifacts"]["summary.json"]["sha256"],
                              terminal["artifacts"]["summary.json"]["bytes"])
            download_verified(s3, truth[0][1], truth_path, truth[0][3], truth[0][2])
            replay_path = root / "quality-replay.json"
            subprocess.run(["uv", "run", "--no-project", "--with", "numpy==2.5.0",
                            "--with", "pyarrow==24.0.0", "python", "-m",
                            "scripts.v239_score_graph_containment_100k",
                            "--raw", str(raw_path), "--summary", str(summary_path),
                            "--truth", str(truth_path), "--output", str(replay_path)],
                           check=True)
            if json.loads(replay_path.read_text()) != quality:
                raise ValueError("quality result differs from sealed independent replay")
        closeout = {"schema": "borsuk-v239-graph-containment-100k-closeout-v1",
                    "source_commit": commit, "source_archive_sha256": archive_sha,
                    "instance_id": instance_id, "terminal_sha256": terminal_sha,
                    "quality": quality, "gate_pass": quality.get("gate_pass") is True,
                    "estimated_compute_usd_to_terminal":
                        (terminal["worker_finished_epoch"] - launched) * spot_price / 3600}
        put_if_absent(prefix + "/closeout.json", json.dumps(closeout, sort_keys=True).encode())
        print(json.dumps(closeout, sort_keys=True), flush=True)
        if not closeout["gate_pass"]:
            raise RuntimeError("V239 physical-page or containment gate failed")
    finally:
        if instance_id is not None and not terminated:
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    launch(parser.parse_args().attempt)
