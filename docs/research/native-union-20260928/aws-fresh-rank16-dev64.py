"""One frozen fresh 1M development or prospective native/HTTP worker."""

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

sys.path.insert(0, str(Path.cwd()))
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, PROFILE_ARN, REGION, SECURITY_GROUP, missing, put_if_absent,
)


ROOT = Path("docs/research/native-union-20260928")
SUBNETS = {"subnet-034528fbd6977848f", "subnet-0a12dbed0ca6fac25",
           "subnet-00243d923761c047c"}


def sha(body):
    return hashlib.sha256(body).hexdigest()


def main(attempt, subnet, mode="dev64", family="rank16"):
    if mode not in ("dev64", "confirm936"):
        raise ValueError("unknown fresh panel mode")
    if family not in ("rank16", "cohere"):
        raise ValueError("unknown fresh dataset family")
    confirm = mode == "confirm936"
    campaign = "fresh-" + family + "-" + mode
    config_path = ROOT / (campaign + "-config.json")
    schema = "borsuk-fresh-" + family + "-1m-" + mode + "-spot-v1"
    tag = "borsuk-" + campaign
    wall = 3600 if confirm else 1800
    process = 3000 if confirm else 1500
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subnet not in SUBNETS:
        raise ValueError("unregistered subnet")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("commit frozen fresh development source before launch")
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "merge-base", "--is-ancestor", "HEAD", "origin/main"], check=True)
    config = json.loads(config_path.read_text())
    if config["schema"] != "borsuk-fresh-" + family + "-1m-" + mode + "-v1":
        raise ValueError("fresh protocol config differs")
    for name, expected in config["code_sha256"].items():
        if sha(Path(name).read_bytes()) != expected:
            raise ValueError(f"frozen source/protocol differs: {name}")
    seal_path = ROOT / ("fresh-" + family + "-seal/a0001/verification.json")
    sealed_proof = json.loads(seal_path.read_text())
    expected_raw = ("6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005"
                    if family == "cohere" else
                    "a3eac4dedae5006843ea2ad243ec590440fe5b983ca8cd969dbc92b3e3406c33")
    if (sha(seal_path.read_bytes()) != config["sealed_construction_verification_sha256"]
            or not sealed_proof["valid_construction"]
            or sealed_proof["state"] != ("local-complete" if family == "cohere" else "terminated")
            or sealed_proof["sealed_artifacts"] != config["sealed"]
            or config["source_raw_sha256"] != expected_raw):
        raise ValueError("fresh query/source construction authority differs")
    for folder, flag, digest_key in (
        ("native-reference-panel/a0002", "valid_check", "qualified_reference_verification_sha256"),
        ("four-slot-1m-offered-http/a0001", "valid_measurement", "qualified_http_verification_sha256"),
    ):
        path = ROOT / folder / "verification.json"
        proof = json.loads(path.read_text())
        if sha(path.read_bytes()) != config[digest_key] or not proof[flag] or proof["state"] != "terminated":
            raise ValueError("qualified native/binary authority differs")
    if (sha(Path("crates/borsuk/src/bin/two_bit_plan_demo.rs").read_bytes())
            != "ce9502a86c503bf085942fb4294731fac8432ddcb5f1e52cdf21c68397fd5f02"
            or sha(Path("crates/borsuk/examples/two_bit_http.rs").read_bytes())
            != "2adc14246cda4f7ff2318a985ba61b27e4e7e6db865be67260b0252e4b0d9e49"):
        raise ValueError("current native source differs from qualified binaries")
    if family == "cohere":
        source_path = ROOT / "cohere-source-1m/a0001/verification.json"
        source = json.loads(source_path.read_text())
        if (sha(source_path.read_bytes()) != config["source_construction_verification_sha256"]
                or not source["valid_source_construction"] or source["state"] != "local-complete"
                or not source["remote_bodies_independently_hashed"]
                or source["query_or_truth_used"] or source["quality_measured"]
                or source["source_raw_sha256"] != expected_raw
                or source["root_sha256"] != config["root_sha256"]
                or sealed_proof["root_sha256"] != config["root_sha256"]):
            raise ValueError("closed CoHere source authority differs")
        expected_generation = {**source["generation_artifacts"],
                               "generation/canonical.bin": source["canonical_artifact"]}
    else:
        current = json.loads((ROOT / "current-1m-offered-http-config.json").read_text())
        expected_generation = {name: ident for name, ident in current["artifacts"].items()
                               if name.startswith("generation/")}
    if (config["generation_artifacts"] != expected_generation
            or config["root_manifest"] != expected_generation["generation/manifest.json"]):
        raise ValueError("closed generation artifact roster differs")
    if confirm:
        prior_attempt = config["closed_dev64_attempt"] if family == "cohere" else "a0002"
        if len(prior_attempt) != 5 or not prior_attempt.startswith("a") or not prior_attempt[1:].isdigit():
            raise ValueError("invalid closed development attempt")
        prior = ROOT / ("fresh-" + family + "-dev64/" + prior_attempt)
        proof = json.loads((prior / "verification.json").read_text())
        terminal = json.loads((prior / "aws-terminal.json").read_text())
        if (not proof["valid_measurement"] or proof["state"] != "terminated"
                or sha((prior / "verification.json").read_bytes()) != config["closed_dev64_verification_sha256"]
                or sha((prior / "aws-terminal.json").read_bytes()) != config["closed_dev64_terminal_sha256"]
                or config["requests_byte_start"] != terminal["artifacts"]["screen/requests64.jsonl"]["bytes"]):
            raise ValueError("frozen prospective suffix boundary differs")
        if family == "cohere":
            dev_path = ROOT / "fresh-cohere-dev64-config.json"
            dev = json.loads(dev_path.read_text())
            if (proof["config_sha256"] != sha(dev_path.read_bytes())
                    or proof["decision"] != "GO fresh CoHere1M " + dev["query_split"]
                    or proof["prospective_queries_opened"]
                    or any(config[key] != dev[key] for key in
                           ("root_sha256", "root_manifest", "source_raw_sha256", "sealed",
                            "binaries", "generation_artifacts", "code_sha256", "gates",
                            "rows", "dimensions", "offered_qps", "setting_order"))):
                raise ValueError("prospective candidate differs from closed development GO")
    archive = io.BytesIO()
    raw = subprocess.run(["git", "archive", "--format=tar", "HEAD"],
                         capture_output=True, check=True).stdout
    with gzip.GzipFile(filename="", mode="wb", fileobj=archive, mtime=0) as zipped:
        zipped.write(raw)
    source = archive.getvalue()
    with tarfile.open(fileobj=io.BytesIO(source), mode="r:gz") as tar:
        if not {str(config_path), str(ROOT / (("fresh-cohere-confirm936-preregister.md" if confirm else "fresh-cohere-1m-preregister.md") if family == "cohere" else campaign + "-preregister.md")),
                "scripts/run_native_fresh_rank16_dev64.py"}.issubset(tar.getnames()):
            raise ValueError("frozen development archive differs")
    archive_sha = sha(source)
    archive_key = f"research/native-library-check/sources/{archive_sha}.tar.gz"
    prefix = f"research/native-union/{'20260929' if family == 'cohere' else '20260928'}/{campaign}-{attempt}"
    out = ROOT / campaign / attempt
    out.mkdir(parents=True, exist_ok=True)
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered; inspect original")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(r["Instances"] for r in active["Reservations"]):
        raise ValueError("BORSUK worker already active")
    stopped = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [tag]},
        {"Name": "instance-state-name", "Values": ["stopped"]},
    ])
    if any(r["Instances"] for r in stopped["Reservations"]):
        raise ValueError("original fresh development worker stopped; inspect it")
    az = ec2.describe_subnets(SubnetIds=[subnet])["Subnets"][0]["AvailabilityZone"]
    quote = ec2.describe_spot_price_history(
        InstanceTypes=["c7g.2xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone=az, MaxResults=1)["SpotPriceHistory"][0]
    if float(quote["SpotPrice"]) > .30:
        raise ValueError("Spot quote exceeds preregistered cap")
    if missing(s3, archive_key):
        put_if_absent(archive_key, source)
    elif sha(s3.get_object(Bucket=BUCKET, Key=archive_key)["Body"].read()) != archive_sha:
        raise ValueError("existing source archive differs")
    runner.WALL_SECONDS = wall
    runner.SCHEMA = schema
    artifacts = ["cpu.txt", "environment.txt", "screen/decision.json", "screen/cgroup.json",
                 "screen/native-quality.json", f"screen/requests{config['count']}.jsonl",
                 "screen/reference-k10.jsonl", "screen/reference-k100.jsonl",
                 "screen/reference-k10.log", "screen/reference-k10.time",
                 "screen/reference-k100.log", "screen/reference-k100.time"]
    for rep, k in enumerate(config["setting_order"]):
        artifacts.extend(f"screen/run{rep}-k{k}/{name}" for name in
                         ("http.jsonl", "result.json", "server.log", "server.time",
                          "server-closeout.json"))
    runner.ARTIFACTS += tuple(artifacts)
    body = runner.user_data(commit, archive_sha, archive_key, prefix)
    body = body.replace("v174-relaid-bind-compile", campaign)
    body = body.replace("'source_commit':", "'source_base_commit':")
    start, stop = body.index("phase=install"), body.index("phase=complete", body.index("phase=install"))
    config_sha = sha(config_path.read_bytes())
    check = f'''phase=install
dnf install -y -q python3.12 python3.12-pip util-linux time
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3
lscpu >cpu.txt
.venv/bin/python -c 'import numpy,platform; print(numpy.__version__,platform.platform())' >environment.txt
mkdir binaries
'''
    for name, ident in config["binaries"].items():
        check += f'''aws s3 cp 's3://{BUCKET}/{ident["key"]}' binaries/{name} --only-show-errors
echo '{ident["sha256"]}  binaries/{name}' | sha256sum -c -
[ "$(stat -c %s binaries/{name})" = '{ident["bytes"]}' ]
chmod 755 binaries/{name}
'''
    check += f'''phase=measure
systemd-run --unit={campaign} --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec={process+30} -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 {process} \\
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 "$1" "$2/repo/scripts/run_native_fresh_rank16_dev64.py" "$2/repo/{config_path}" "$3" "$2/screen" "$2/binaries" "$4"' _ \\
 "$root/.venv/bin/python" "$root" '{config_sha}' '{prefix}' >test.log 2>&1
test -s "$root/screen/decision.json"
test -s "$root/screen/native-quality.json"
test -s "$root/screen/cgroup.json"
'''
    body = body[:start] + check + body[stop:]
    subprocess.run(["bash", "-n"], input=body, text=True, check=True)
    if len(body.encode()) > 16384:
        raise ValueError("EC2 user data limit")
    reservation = {"schema": schema, "attempt": attempt, "source_base_commit": commit,
                   "source_archive_sha256": archive_sha, "config_sha256": config_sha,
                   "sealed_construction_terminal_sha256": sealed_proof["terminal_sha256"],
                   "profile": "causality", "region": REGION, "availability_zone": az,
                   "instance_type": "c7g.2xlarge", "spot_price_observed_usd_per_hour": quote["SpotPrice"],
                   "spot_quote_timestamp": quote["Timestamp"].isoformat(),
                   "spot_max_usd_per_hour": "0.30", "compute_cost_cap_usd": wall / 3600 * .30,
                   "ebs_s3_allowance_usd": .30 if confirm else .15, "wall_seconds": wall,
                   "measurement_memory_max_bytes": 8 * 1024**3, "swap_max_bytes": 0,
                   "interruption_policy": "Discard interrupted cell; no automatic retry",
                   "scope": (f"Frozen {family} prospective64-999 native R10/R100 then conditional four-slot HTTP; no tuning" if confirm else
                             f"Frozen {family} fresh dev64 native R10/R100 then conditional four-slot HTTP; prospective untouched")}
    (out / "aws-user-data.sh").write_text(body)
    (out / "aws-reservation.json").write_text(json.dumps(reservation, indent=2) + "\n")
    put_if_absent(prefix + "/reservation.json", json.dumps(reservation, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken=campaign + "-" + attempt + "-" + archive_sha[:28],
        ImageId="ami-03748c04dc81412c6", InstanceType="c7g.2xlarge",
        MinCount=1, MaxCount=1, IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": subnet}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time",
            "MaxPrice": "0.30"}}, InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True, "VolumeSize": 80,
            "VolumeType": "gp3"}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": tag}, {"Key": "BorsukAttempt", "Value": attempt}]}],
        UserData=body)
    instance = receipt["Instances"][0]["InstanceId"]
    launch = {"instance_id": instance, "prefix": prefix, "source_archive_sha256": archive_sha,
              "source_base_commit": commit, "bucket": BUCKET}
    (out / "aws-launch.json").write_text(json.dumps(launch, indent=2) + "\n")
    print(json.dumps(launch), flush=True)
    started = time.monotonic()
    terminal = None
    try:
        while time.monotonic() - started < wall + 300:
            if not missing(s3, prefix + "/terminal.json"):
                raw = s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"].read()
                terminal = json.loads(raw)
                for key, expected in (("schema", schema), ("instance_id", instance),
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
            raise TimeoutError("fresh development worker exceeded wall cap")
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
        raise RuntimeError("fresh development terminal failed; inspect original evidence")


if __name__ == "__main__":
    family = "cohere" if len(sys.argv) > 1 and sys.argv[1] == "cohere" else "rank16"
    if family == "cohere":
        sys.argv.pop(1)
    mode = "confirm936" if len(sys.argv) > 1 and sys.argv[1] == "confirm936" else "dev64"
    offset = 2 if mode == "confirm936" else 1
    attempt = sys.argv[offset] if len(sys.argv) > offset else "a0001"
    subnet = sys.argv[offset + 1] if len(sys.argv) > offset + 1 else "subnet-0a12dbed0ca6fac25"
    with open("/tmp/borsuk-fresh-rank16-worker.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        main(attempt, subnet, mode, family)
