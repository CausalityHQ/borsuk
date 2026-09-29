"""Launch one bounded rank16 exact-GT construction cell on Causality Spot."""

import fcntl
import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import time

import boto3

from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, PROFILE_ARN, REGION, SECURITY_GROUP, missing, put_if_absent,
)


ROOT = Path("docs/research/native-union-20260928")
CONFIG = ROOT / "fresh-rank16-seal-config.json"
SCHEMA = "borsuk-v36-rank16-fresh-1m-seal-spot-v1"
TAG = "borsuk-rank16-fresh-1m-seal"
SUBNETS = {"subnet-034528fbd6977848f", "subnet-0a12dbed0ca6fac25",
           "subnet-00243d923761c047c"}


def sha(body):
    return hashlib.sha256(body).hexdigest()


def main(attempt, subnet):
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subnet not in SUBNETS:
        raise ValueError("unregistered subnet")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("commit the frozen construction source before launch")
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "merge-base", "--is-ancestor", "HEAD", "origin/main"], check=True)
    config = json.loads(CONFIG.read_text())
    for name, expected in config["code_sha256"].items():
        if sha(Path(name).read_bytes()) != expected:
            raise ValueError(f"frozen construction code differs: {name}")
    if (sha(Path(config["panel_path"]).read_bytes()) != config["panel_sha256"]
            or sha(Path(config["registry_path"]).read_bytes()) != config["registry_sha256"]):
        raise ValueError("frozen panel or registry differs")
    archive = io.BytesIO()
    raw = subprocess.run(["git", "archive", "--format=tar", "HEAD"],
                         capture_output=True, check=True).stdout
    with gzip.GzipFile(filename="", mode="wb", fileobj=archive, mtime=0) as zipped:
        zipped.write(raw)
    source = archive.getvalue()
    with tarfile.open(fileobj=io.BytesIO(source), mode="r:gz") as tar:
        if not {str(CONFIG), "scripts/seal_v36_rank16_fresh_1m.py",
                str(ROOT / "fresh-rank16-seal-preregister.md")}.issubset(tar.getnames()):
            raise ValueError("frozen construction source archive differs")
    archive_sha = sha(source)
    archive_key = f"research/native-library-check/sources/{archive_sha}.tar.gz"
    prefix = f"research/native-union/20260928/fresh-rank16-seal-{attempt}"
    out = ROOT / "fresh-rank16-seal" / attempt
    out.mkdir(parents=True, exist_ok=True)
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered; inspect original")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(r["Instances"] for r in active["Reservations"]):
        raise ValueError("BORSUK Spot worker already active")
    az = ec2.describe_subnets(SubnetIds=[subnet])["Subnets"][0]["AvailabilityZone"]
    quote = ec2.describe_spot_price_history(
        InstanceTypes=["c7g.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone=az, MaxResults=1)["SpotPriceHistory"][0]
    if float(quote["SpotPrice"]) > 0.60:
        raise ValueError("Spot quote exceeds preregistered cap")
    if missing(s3, archive_key):
        put_if_absent(archive_key, source)
    elif sha(s3.get_object(Bucket=BUCKET, Key=archive_key)["Body"].read()) != archive_sha:
        raise ValueError("existing source archive differs")
    runner.WALL_SECONDS = 3600
    runner.SCHEMA = SCHEMA
    runner.ARTIFACTS += ("cpu.txt", "environment.txt", "screen/decision.json",
                         "screen/cgroup.json")
    body = runner.user_data(commit, archive_sha, archive_key, prefix)
    body = body.replace("v174-relaid-bind-compile", "rank16-fresh-1m-seal")
    body = body.replace("'source_commit':", "'source_base_commit':")
    start, stop = body.index("phase=install"), body.index("phase=complete", body.index("phase=install"))
    config_sha = sha(CONFIG.read_bytes())
    check = f'''phase=install
dnf install -y -q python3.12 python3.12-pip util-linux time
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
lscpu >cpu.txt
.venv/bin/python -c 'import numpy,pyarrow,platform; print(numpy.__version__,pyarrow.__version__,platform.platform())' >environment.txt
phase=construct
systemd-run --unit=rank16-seal --wait --pipe -p MemoryMax=24G -p MemorySwapMax=0 -p RuntimeMaxSec=3300 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=OPENBLAS_NUM_THREADS=8 --setenv=OMP_NUM_THREADS=8 --setenv=MKL_NUM_THREADS=8 --setenv=AWS_MAX_ATTEMPTS=1 \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 3000 \\
 bash -c 'set -e; ulimit -v 29360128; exec taskset -c 0-7 "$1" "$2/repo/scripts/seal_v36_rank16_fresh_1m.py" "$2/repo/docs/research/native-union-20260928/fresh-rank16-seal-config.json" "$3" "$2/repo" "$2/screen" "$4"' _ \\
 "$root/.venv/bin/python" "$root" '{config_sha}' '{prefix}' >test.log 2>&1
test -s "$root/screen/decision.json"
test -s "$root/screen/cgroup.json"
'''
    body = body[:start] + check + body[stop:]
    subprocess.run(["bash", "-n"], input=body, text=True, check=True)
    if len(body.encode()) > 16384:
        raise ValueError("EC2 user data limit")
    reservation = {"schema": SCHEMA, "attempt": attempt, "source_base_commit": commit,
                   "source_archive_sha256": archive_sha, "config_sha256": config_sha,
                   "panel_sha256": config["panel_sha256"], "profile": "causality",
                   "region": REGION, "availability_zone": az, "instance_type": "c7g.4xlarge",
                   "spot_price_observed_usd_per_hour": quote["SpotPrice"],
                   "spot_quote_timestamp": quote["Timestamp"].isoformat(),
                   "spot_max_usd_per_hour": "0.60", "wall_seconds": 3600,
                   "compute_cost_cap_usd": 0.60, "ebs_s3_transfer_allowance_usd": 0.50,
                   "measurement_memory_max_bytes": 24 * 1024**3, "swap_max_bytes": 0,
                   "interruption_policy": "Discard incomplete cell; no automatic retry",
                   "scope": "Rank16 frozen ReLAION1M external queries plus exhaustive f64 GT100; no ANN quality"}
    (out / "aws-user-data.sh").write_text(body)
    (out / "aws-reservation.json").write_text(json.dumps(reservation, indent=2) + "\n")
    put_if_absent(prefix + "/reservation.json", json.dumps(reservation, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="rank16-seal-" + attempt + "-" + archive_sha[:32],
        ImageId="ami-03748c04dc81412c6", InstanceType="c7g.4xlarge",
        MinCount=1, MaxCount=1, IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": subnet}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time",
            "MaxPrice": "0.60"}}, InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True, "VolumeSize": 80,
            "VolumeType": "gp3"}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": TAG}, {"Key": "BorsukAttempt", "Value": attempt}]}],
        UserData=body)
    instance = receipt["Instances"][0]["InstanceId"]
    launch = {"instance_id": instance, "prefix": prefix, "source_archive_sha256": archive_sha,
              "source_base_commit": commit, "bucket": BUCKET}
    (out / "aws-launch.json").write_text(json.dumps(launch, indent=2) + "\n")
    print(json.dumps(launch), flush=True)
    started = time.monotonic()
    terminal = None
    try:
        while time.monotonic() - started < 3900:
            if not missing(s3, prefix + "/terminal.json"):
                raw = s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"].read()
                terminal = json.loads(raw)
                for key, expected in (("schema", SCHEMA), ("instance_id", instance),
                                      ("source_archive_sha256", archive_sha),
                                      ("source_base_commit", commit)):
                    if terminal.get(key) != expected:
                        raise ValueError(f"terminal {key} differs")
                (out / "aws-terminal.json").write_bytes(raw)
                for name, ident in terminal["artifacts"].items():
                    if name not in runner.ARTIFACTS:
                        raise ValueError("unexpected terminal artifact")
                    data = s3.get_object(Bucket=BUCKET, Key=prefix + "/artifacts/" + name)["Body"].read()
                    if len(data) != ident["bytes"] or sha(data) != ident["sha256"]:
                        raise ValueError("terminal artifact differs")
                    path = out / (name + ".gz")
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(gzip.compress(data, mtime=0))
                (out / "aws-terminal.sha256").write_text(sha(raw) + "  aws-terminal.json\n")
                print(json.dumps({"terminal_status": terminal["status"],
                                  "exit_code": terminal["exit_code"]}), flush=True)
                break
            state = ec2.describe_instances(InstanceIds=[instance])["Reservations"][0]["Instances"][0]["State"]["Name"]
            print(json.dumps({"instance_id": instance, "state": state,
                              "elapsed_s": round(time.monotonic() - started)}), flush=True)
            if state in ("terminated", "shutting-down"):
                raise RuntimeError("worker closed without terminal; do not duplicate")
            time.sleep(20)
        else:
            raise TimeoutError("construction worker exceeded wall cap")
    finally:
        ec2.terminate_instances(InstanceIds=[instance])
        ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance])
        close = {"instance_id": instance, "state": "terminated",
                 "observed_elapsed_s": round(time.monotonic() - started),
                 "compute_cost_estimate_usd": round((time.monotonic() - started) / 3600
                                                    * float(quote["SpotPrice"]), 4),
                 "cost_status": "estimate only; excludes EBS/S3 and is not an invoice"}
        (out / "aws-closeout.json").write_text(json.dumps(close, indent=2) + "\n")
        print(json.dumps(close), flush=True)
    if terminal is None or terminal["status"] != "complete" or terminal["exit_code"]:
        raise RuntimeError("construction terminal failed; inspect original evidence")
    decision = json.loads(gzip.decompress((out / "screen/decision.json.gz").read_bytes()))
    for ident in decision["artifacts"].values():
        data = s3.get_object(Bucket=BUCKET, Key=ident["key"])["Body"].read()
        if len(data) != ident["bytes"] or sha(data) != ident["sha256"]:
            raise ValueError("sealed artifact differs")
    print(json.dumps({"sealed_artifacts_verified": 3, "decision": decision["decision"]}), flush=True)


if __name__ == "__main__":
    attempt = sys.argv[1] if len(sys.argv) > 1 else "a0001"
    subnet = sys.argv[2] if len(sys.argv) > 2 else "subnet-0a12dbed0ca6fac25"
    with open("/tmp/borsuk-rank16-fresh-seal.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        main(attempt, subnet)
