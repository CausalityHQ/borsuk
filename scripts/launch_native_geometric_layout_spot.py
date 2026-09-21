#!/usr/bin/env python3
"""Launch one immutable native-geometric-layout screen on Causality Spot."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import shlex
import time
from collections.abc import Sequence
from typing import Literal


@dataclasses.dataclass(frozen=True, slots=True)
class FrozenInput:
    role: str
    uri: str
    sha256: str
    encoded_bytes: int
    rows: int


FROZEN_SOURCE = FrozenInput(
    role="source",
    uri=(
        "s3://borsuk-bench-453182569524-euc1/research/"
        "v85-pq16-page-nomination/24383d853474a19702d18d2de700bee3618167f5/"
        "100k-a0023/attempt/inputs/source-100k.parquet"
    ),
    sha256="a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d",
    encoded_bytes=145_121_661,
    rows=100_000,
)
FROZEN_QUERIES = FrozenInput(
    role="queries",
    uri=(
        "s3://borsuk-bench-453182569524-euc1/research/"
        "v85-competitive-rescore/fb976932ecd4076e2f76a7cb7e7aa7efe01e9a2d/"
        "runs/v85-100k-dev1000-20260920T094401Z-fb976932/a0001/inputs/"
        "queries.parquet"
    ),
    sha256="4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac",
    encoded_bytes=1_544_342,
    rows=1_000,
)
FROZEN_TRUTH = FrozenInput(
    role="truth",
    uri=(
        "s3://borsuk-bench-453182569524-euc1/research/"
        "v85-competitive-rescore/fb976932ecd4076e2f76a7cb7e7aa7efe01e9a2d/"
        "runs/v85-100k-dev1000-20260920T094401Z-fb976932/a0001/inputs/"
        "truth-100k.parquet"
    ),
    sha256="ab8bfae34f753512f352581218596fc0f043354f8168192c856278b3ab5a0ce7",
    encoded_bytes=512_093,
    rows=100_000,
)


@dataclasses.dataclass(frozen=True, slots=True)
class SourceArchiveIdentity:
    uri: str
    sha256: str
    encoded_bytes: int


@dataclasses.dataclass(frozen=True, slots=True)
class SpotTarget:
    availability_zone: str
    subnet_id: str


DEFAULT_TARGETS = (
    SpotTarget("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
    SpotTarget("eu-central-1b", "subnet-00243d923761c047c"),
    SpotTarget("eu-central-1a", "subnet-034528fbd6977848f"),
)


@dataclasses.dataclass(frozen=True, slots=True)
class SpotLayoutPlan:
    profile: str
    source_commit: str
    source_archive: SourceArchiveIdentity
    source: FrozenInput
    queries: FrozenInput
    truth: FrozenInput
    requirements_sha256: str
    output_prefix: str
    image_id: str
    security_group_id: str
    instance_profile_arn: str
    targets: tuple[SpotTarget, ...]
    attempt: int = 1
    market: Literal["spot"] = "spot"
    instance_type: str = "c7i.8xlarge"
    wall_seconds: int = 7_200
    maximum_rss_bytes: int = 3 * 1024**3
    volume_gib: int = 100


def _sha256(value: str) -> bool:
    return (
        len(value) == 64
        and value.lower() == value
        and all(character in "0123456789abcdef" for character in value)
    )


def _coerce_archive(value: object) -> SourceArchiveIdentity:
    if isinstance(value, SourceArchiveIdentity):
        return value
    if type(value) is dict:
        return SourceArchiveIdentity(**value)
    raise ValueError("layout source archive differs")


def _coerce_input(value: object) -> FrozenInput:
    if isinstance(value, FrozenInput):
        return value
    if type(value) is dict:
        return FrozenInput(**value)
    raise ValueError("layout frozen input differs")


def _coerce_targets(value: object) -> tuple[SpotTarget, ...]:
    if type(value) not in (tuple, list):
        raise ValueError("layout Spot targets differ")
    return tuple(
        item if isinstance(item, SpotTarget) else SpotTarget(**item) for item in value
    )


def build_plan(**values: object) -> SpotLayoutPlan:
    concrete = dict(values)
    concrete["source_archive"] = _coerce_archive(concrete.get("source_archive"))
    concrete["source"] = _coerce_input(concrete.get("source"))
    concrete["queries"] = _coerce_input(concrete.get("queries"))
    concrete["truth"] = _coerce_input(concrete.get("truth"))
    concrete["targets"] = _coerce_targets(concrete.get("targets"))
    try:
        plan = SpotLayoutPlan(**concrete)
    except TypeError as error:
        raise ValueError("layout Spot plan differs") from error
    if (
        plan.profile != "causality"
        or len(plan.source_commit) != 40
        or not all(character in "0123456789abcdef" for character in plan.source_commit)
        or not plan.source_archive.uri.startswith("s3://")
        or not _sha256(plan.source_archive.sha256)
        or plan.source_archive.encoded_bytes <= 0
        or plan.source != FROZEN_SOURCE
        or plan.queries != FROZEN_QUERIES
        or plan.truth != FROZEN_TRUTH
        or not _sha256(plan.requirements_sha256)
        or not plan.output_prefix.startswith("s3://")
        or plan.source_commit not in plan.output_prefix
        or plan.attempt != 1
        or plan.market != "spot"
        or plan.instance_type != "c7i.8xlarge"
        or plan.wall_seconds != 7_200
        or plan.maximum_rss_bytes != 3 * 1024**3
        or plan.volume_gib != 100
        or not plan.image_id.startswith("ami-")
        or not plan.security_group_id.startswith("sg-")
        or not plan.instance_profile_arn.startswith("arn:aws:iam::")
        or plan.targets != DEFAULT_TARGETS
        or any(
            "1m" in item.uri.lower()
            or "10m" in item.uri.lower()
            or "100m" in item.uri.lower()
            for item in (plan.source, plan.queries, plan.truth)
        )
    ):
        raise ValueError("layout Spot plan differs")
    return plan


def _q(value: object) -> str:
    return shlex.quote(str(value))


def _validate_terminal_bytes(
    body: bytes,
    source_commit: str,
    instance_id: str,
    output_prefix: str,
) -> dict[str, object]:
    try:
        terminal = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("layout terminal JSON differs") from error
    canonical = json.dumps(terminal, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    if body != canonical or type(terminal) is not dict or set(terminal) != {
        "artifacts",
        "claim_eligible",
        "elapsed_seconds",
        "exit_code",
        "instance_id",
        "phase",
        "schema",
        "source_commit",
        "status",
    }:
        raise ValueError("layout terminal canonical bytes differ")
    artifact_names = {
        "membership": "membership.parquet",
        "tree": "tree.parquet",
        "pages": "pages.parquet",
        "evidence": "evidence.parquet",
        "result": "result.json",
        "validation": "validation.json",
        "construct-resources": "construct-resources.txt",
        "evaluate-resources": "evaluate-resources.txt",
        "validate-resources": "validate-resources.txt",
    }
    artifacts = terminal["artifacts"]
    if type(artifacts) is not dict or set(artifacts) != set(artifact_names):
        raise ValueError("layout terminal artifact roster differs")
    for role, name in artifact_names.items():
        identity = artifacts[role]
        if (
            type(identity) is not dict
            or set(identity) != {"encoded_bytes", "role", "sha256", "uri"}
            or identity["role"] != role
            or type(identity["encoded_bytes"]) is not int
            or identity["encoded_bytes"] <= 0
            or not _sha256(identity["sha256"])
            or identity["uri"] != f"{output_prefix.rstrip('/')}/artifacts/{name}"
        ):
            raise ValueError("layout terminal artifact identity differs")
    if (
        terminal["schema"] != "borsuk-native-geometric-layout-terminal-v1"
        or terminal["claim_eligible"] is not False
        or type(terminal["elapsed_seconds"]) is not int
        or terminal["elapsed_seconds"] < 0
        or type(terminal["exit_code"]) is not int
        or terminal["exit_code"] != 0
        or type(terminal["instance_id"]) is not str
        or terminal["instance_id"] != instance_id
        or terminal["phase"] != "complete"
        or terminal["source_commit"] != source_commit
        or terminal["status"] != "complete"
    ):
        raise ValueError("layout terminal authority differs")
    return terminal


def worker_script(plan: SpotLayoutPlan) -> str:
    """Build the single-cell geometric-router worker program."""
    script = f"""#!/bin/bash
set -euo pipefail
root=/mnt/native-geometric-layout
output={_q(plan.output_prefix.rstrip('/'))}
phase=bootstrap
status=failed
started=$(date +%s)
MAXIMUM_RSS_BYTES={_q(plan.maximum_rss_bytes)}
mkdir -p "$root" && cd "$root"
run_capped() {{
  setsid "$@" & pid=$!
  while kill -0 "$pid" 2>/dev/null; do
    rss_bytes=$(ps -eo pgid=,rss= | awk -v pgid="$pid" '$1 == pgid {{ total += $2 }} END {{ printf "%.0f", total * 1024 }}')
    if [ "${{rss_bytes:-0}}" -gt "$MAXIMUM_RSS_BYTES" ]; then
      kill -TERM -- "-$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; return 137
    fi
    sleep 1
  done
  rc=0; wait "$pid" || rc=$?; return "$rc"
}}
terminal() {{
  rc=$?; trap - EXIT; ended=$(date +%s)
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token 2>/dev/null || true)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || true)
  STATUS="$status" PHASE="$phase" EXIT_CODE="$rc" STARTED="$started" ENDED="$ended" SOURCE_COMMIT={_q(plan.source_commit)} INSTANCE_ID="$instance_id" OUTPUT_PREFIX="$output" python3 - <<'PY'
import hashlib,json,os,pathlib
def ident(path,role):
 d=pathlib.Path(path).read_bytes(); return {{"encoded_bytes":len(d),"role":role,"sha256":hashlib.sha256(d).hexdigest(),"uri":os.environ["OUTPUT_PREFIX"]+"/artifacts/"+path}}
files=(("membership","membership.parquet"),("tree","tree.parquet"),("pages","pages.parquet"),("evidence","evidence.parquet"),("result","result.json"),("validation","validation.json"),("construct-resources","construct-resources.txt"),("evaluate-resources","evaluate-resources.txt"),("validate-resources","validate-resources.txt"))
complete=os.environ["STATUS"]=="complete" and int(os.environ["EXIT_CODE"])==0
v={{"artifacts":{{r:ident(p,r) for r,p in files}} if complete else {{}},"claim_eligible":False,"elapsed_seconds":int(os.environ["ENDED"])-int(os.environ["STARTED"]),"exit_code":int(os.environ["EXIT_CODE"]),"instance_id":os.environ.get("INSTANCE_ID",""),"phase":os.environ["PHASE"],"schema":"borsuk-native-geometric-layout-terminal-v1","source_commit":os.environ["SOURCE_COMMIT"],"status":os.environ["STATUS"]}}
pathlib.Path("terminal.json").write_text(json.dumps(v,sort_keys=True,separators=(",",":"))+"\\n")
PY
  aws s3 cp terminal.json "$output/terminal.json" --only-show-errors || true
  rm -f source.parquet queries.parquet truth.parquet membership.parquet tree.parquet pages.parquet evidence.parquet result.json sealed-router.json authority.json construct.py evaluate.py validate.py
  sudo shutdown -h now || true; exit "$rc"
}}
trap terminal EXIT
phase=install
dnf install -y -q python3.12 python3.12-pip tar gzip time util-linux >install.log 2>&1
phase=source
aws s3 cp {_q(plan.source_archive.uri)} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = {_q(plan.source_archive.encoded_bytes)} ]
printf '%s  source.tar.gz\n' {_q(plan.source_archive.sha256)} | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
[ "$(cat repo/.borsuk-source-commit)" = {_q(plan.source_commit)} ]
printf '%s  repo/scripts/requirements-format-bench.txt\n' {_q(plan.requirements_sha256)} | sha256sum -c -
python3.12 -m venv .venv
.venv/bin/python -m pip install --disable-pip-version-check --quiet -r repo/scripts/requirements-format-bench.txt
aws s3 cp {_q(plan.source.uri)} source.parquet --only-show-errors
[ "$(stat -c%s source.parquet)" = {_q(plan.source.encoded_bytes)} ]
printf '%s  source.parquet\n' {_q(plan.source.sha256)} | sha256sum -c -
SOURCE_URI={_q(plan.source.uri)} SOURCE_SHA={_q(plan.source.sha256)} SOURCE_BYTES={_q(plan.source.encoded_bytes)} .venv/bin/python - <<'PY'
import json,os,pathlib
v={{"dimensions":768,"maximum_page_bytes":491520,"maximum_page_rows":65535,"method":"balanced-two-means-480k","metric":"l2","rows":100000,"schema":"borsuk-native-geometric-layout-authority-v1","seed":20260921,"source":{{"encoded_bytes":int(os.environ["SOURCE_BYTES"]),"role":"source","sha256":os.environ["SOURCE_SHA"],"uri":os.environ["SOURCE_URI"]}}}}
pathlib.Path("authority.json").write_text(json.dumps(v,sort_keys=True,separators=(",",":"))+"\\n")
PY
cat >construct.py <<'PY'
import dataclasses,json,os,pathlib
from scripts.native_geometric_layout_screen import _source_arrays,construct_geometric_router,layout_authority_from_dict,write_geometric_router_parquet,write_membership_parquet
a=layout_authority_from_dict(json.loads(pathlib.Path("authority.json").read_text())); ids,x=_source_arrays(pathlib.Path("source.parquet"),a); m,r=construct_geometric_router(a,ids,x)
mi=dataclasses.replace(write_membership_parquet(pathlib.Path("membership.parquet"),a,m),role="geometric-membership",uri=os.environ["OUTPUT_PREFIX"]+"/artifacts/membership.parquet")
ti,pi=write_geometric_router_parquet(pathlib.Path("tree.parquet"),pathlib.Path("pages.parquet"),a,m,r); ti=dataclasses.replace(ti,uri=os.environ["OUTPUT_PREFIX"]+"/artifacts/tree.parquet"); pi=dataclasses.replace(pi,uri=os.environ["OUTPUT_PREFIX"]+"/artifacts/pages.parquet")
v={{"membership":dataclasses.asdict(mi),"pages":dataclasses.asdict(pi),"schema":"borsuk-native-geometric-router-seal-v1","tree":dataclasses.asdict(ti)}}; pathlib.Path("sealed-router.json").write_text(json.dumps(v,sort_keys=True,separators=(",",":"))+"\\n")
PY
phase=construct
run_capped timeout {_q(plan.wall_seconds)} unshare --net --fork env -i PATH="$PATH" PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=32 OMP_NUM_THREADS=32 OUTPUT_PREFIX="$output" /usr/bin/time -v -o construct-resources.txt "$root/.venv/bin/python" construct.py
phase=seal
chmod 0444 membership.parquet tree.parquet pages.parquet sealed-router.json
aws s3 cp membership.parquet "$output/artifacts/membership.parquet" --only-show-errors
aws s3 cp tree.parquet "$output/artifacts/tree.parquet" --only-show-errors
aws s3 cp pages.parquet "$output/artifacts/pages.parquet" --only-show-errors
aws s3 cp sealed-router.json "$output/artifacts/sealed-router.json" --only-show-errors
aws s3 cp construct-resources.txt "$output/artifacts/construct-resources.txt" --only-show-errors
phase=evaluate
export OPENBLAS_NUM_THREADS=32
export OMP_NUM_THREADS=32
aws s3 cp {_q(plan.queries.uri)} queries.parquet --only-show-errors
[ "$(stat -c%s queries.parquet)" = {_q(plan.queries.encoded_bytes)} ] && printf '%s  queries.parquet\n' {_q(plan.queries.sha256)} | sha256sum -c -
aws s3 cp {_q(plan.truth.uri)} truth.parquet --only-show-errors
[ "$(stat -c%s truth.parquet)" = {_q(plan.truth.encoded_bytes)} ] && printf '%s  truth.parquet\n' {_q(plan.truth.sha256)} | sha256sum -c -
chmod 0444 source.parquet queries.parquet truth.parquet membership.parquet tree.parquet pages.parquet sealed-router.json
mkdir evaluation && chown nobody:nobody evaluation
cat >evaluate.py <<'PY'
import dataclasses,json,os,pathlib
from scripts.native_geometric_layout_screen import ArtifactIdentity,EvaluationLimits,_ground_truth,_source_arrays,evaluate_geometric_router,geometric_router_result_bytes,layout_authority_from_dict,read_geometric_router_parquet,read_membership_parquet,write_geometric_router_evidence
from scripts.validate_native_geometric_layout_result import _read_geometric_queries
a=layout_authority_from_dict(json.loads(pathlib.Path("authority.json").read_text())); ids,x=_source_arrays(pathlib.Path("source.parquet"),a); s=json.loads(pathlib.Path("sealed-router.json").read_text()); mi=ArtifactIdentity(**s["membership"]); ti=ArtifactIdentity(**s["tree"]); pi=ArtifactIdentity(**s["pages"]); m=read_membership_parquet(pathlib.Path("membership.parquet"),a,ids); r=read_geometric_router_parquet(pathlib.Path("tree.parquet"),pathlib.Path("pages.parquet"),a,m,ti,pi)
qi=ArtifactIdentity("queries",os.environ["QUERIES_URI"],os.environ["QUERIES_SHA"],int(os.environ["QUERIES_BYTES"])); gi=ArtifactIdentity("truth",os.environ["TRUTH_URI"],os.environ["TRUTH_SHA"],int(os.environ["TRUTH_BYTES"])); q=_read_geometric_queries(pathlib.Path("queries.parquet"),qi,a.dimensions); g=_ground_truth(pathlib.Path("truth.parquet")); limits=EvaluationLimits(32,16777216); e=evaluate_geometric_router(r,m,ids,x,q,g,leaf_frontier=128,limits=limits); ei=dataclasses.replace(write_geometric_router_evidence(pathlib.Path("evaluation/evidence.parquet"),e),uri=os.environ["OUTPUT_PREFIX"]+"/artifacts/evidence.parquet"); pathlib.Path("evaluation/result.json").write_bytes(geometric_router_result_bytes(authority=a,queries=qi,truth=gi,membership=mi,tree=ti,pages=pi,evidence=ei,evaluation=e,leaf_frontier=128,limits=limits))
PY
run_capped /usr/bin/time -v -o evaluate-resources.txt timeout {_q(plan.wall_seconds)} setpriv --reuid=nobody --regid=nobody --clear-groups env PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=32 OMP_NUM_THREADS=32 OUTPUT_PREFIX="$output" QUERIES_URI={_q(plan.queries.uri)} QUERIES_SHA={_q(plan.queries.sha256)} QUERIES_BYTES={_q(plan.queries.encoded_bytes)} TRUTH_URI={_q(plan.truth.uri)} TRUTH_SHA={_q(plan.truth.sha256)} TRUTH_BYTES={_q(plan.truth.encoded_bytes)} "$root/.venv/bin/python" evaluate.py
mv evaluation/evidence.parquet evidence.parquet; mv evaluation/result.json result.json; rmdir evaluation
aws s3 cp evidence.parquet "$output/artifacts/evidence.parquet" --only-show-errors
aws s3 cp result.json "$output/artifacts/result.json" --only-show-errors
aws s3 cp evaluate-resources.txt "$output/artifacts/evaluate-resources.txt" --only-show-errors
phase=assemble
cat >validate.py <<'PY'
import hashlib,json,os,pathlib
from scripts.native_geometric_layout_screen import ArtifactIdentity,EvaluationLimits,LayoutMethod
from scripts.validate_native_geometric_layout_result import GeometricRouterScreenAuthority,GeometricRouterValidationPaths,validate_geometric_router_result
def ident(p,r):
 d=pathlib.Path(p).read_bytes(); return ArtifactIdentity(r,os.environ["OUTPUT_PREFIX"]+"/artifacts/"+p,hashlib.sha256(d).hexdigest(),len(d))
s=json.loads(pathlib.Path("sealed-router.json").read_text()); e=ident("evidence.parquet","geometric-router-evidence"); result=ident("result.json","result")
a=GeometricRouterScreenAuthority("borsuk-native-geometric-router-screen-authority-v1",ArtifactIdentity("source",os.environ["SOURCE_URI"],os.environ["SOURCE_SHA"],int(os.environ["SOURCE_BYTES"])),ArtifactIdentity("queries",os.environ["QUERIES_URI"],os.environ["QUERIES_SHA"],int(os.environ["QUERIES_BYTES"])),ArtifactIdentity("truth",os.environ["TRUTH_URI"],os.environ["TRUTH_SHA"],int(os.environ["TRUTH_BYTES"])),ArtifactIdentity(**s["membership"]),ArtifactIdentity(**s["tree"]),ArtifactIdentity(**s["pages"]),e,result,100000,768,"l2",20260921,LayoutMethod.TWO_MEANS_480K,65535,491520,128,EvaluationLimits(32,16777216))
p=GeometricRouterValidationPaths(*(pathlib.Path(x) for x in ("source.parquet","queries.parquet","truth.parquet","membership.parquet","tree.parquet","pages.parquet","evidence.parquet","result.json"))); decision=validate_geometric_router_result(p,a); v={{"claim_eligible":False,"decision":decision,"schema":"borsuk-native-geometric-router-validation-v1","source_commit":os.environ["SOURCE_COMMIT"]}}; pathlib.Path("validation.json").write_text(json.dumps(v,sort_keys=True,separators=(",",":"))+"\\n")
PY
phase=validate
run_capped /usr/bin/time -v -o validate-resources.txt timeout {_q(plan.wall_seconds)} env OUTPUT_PREFIX="$output" SOURCE_URI={_q(plan.source.uri)} SOURCE_SHA={_q(plan.source.sha256)} SOURCE_BYTES={_q(plan.source.encoded_bytes)} QUERIES_URI={_q(plan.queries.uri)} QUERIES_SHA={_q(plan.queries.sha256)} QUERIES_BYTES={_q(plan.queries.encoded_bytes)} TRUTH_URI={_q(plan.truth.uri)} TRUTH_SHA={_q(plan.truth.sha256)} TRUTH_BYTES={_q(plan.truth.encoded_bytes)} SOURCE_COMMIT={_q(plan.source_commit)} PYTHONPATH="$root/repo" .venv/bin/python validate.py
aws s3 cp validation.json "$output/artifacts/validation.json" --only-show-errors
aws s3 cp validate-resources.txt "$output/artifacts/validate-resources.txt" --only-show-errors
status=complete
phase=complete
"""
    if len(script.encode()) > 16_384:
        raise ValueError("layout Spot user data exceeds EC2 limit")
    return script


def build_launch_specs(plan: SpotLayoutPlan) -> list[dict[str, object]]:
    user_data = worker_script(plan)
    specs = []
    for target in plan.targets:
        token = hashlib.sha256(
            f"native-geometric:{plan.source_commit}:{target.availability_zone}:a0001".encode()
        ).hexdigest()[:47]
        specs.append(
            {
                "BlockDeviceMappings": [
                    {
                        "DeviceName": "/dev/xvda",
                        "Ebs": {
                            "DeleteOnTermination": True,
                            "Encrypted": True,
                            "VolumeSize": plan.volume_gib,
                            "VolumeType": "gp3",
                        },
                    }
                ],
                "ClientToken": "native-geometric-" + token,
                "IamInstanceProfile": {"Arn": plan.instance_profile_arn},
                "ImageId": plan.image_id,
                "InstanceInitiatedShutdownBehavior": "terminate",
                "InstanceMarketOptions": {
                    "MarketType": "spot",
                    "SpotOptions": {
                        "InstanceInterruptionBehavior": "terminate",
                        "SpotInstanceType": "one-time",
                    },
                },
                "InstanceType": plan.instance_type,
                "MaxCount": 1,
                "MinCount": 1,
                "NetworkInterfaces": [
                    {
                        "AssociatePublicIpAddress": True,
                        "DeviceIndex": 0,
                        "Groups": [plan.security_group_id],
                        "SubnetId": target.subnet_id,
                    }
                ],
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Name", "Value": "borsuk-native-geometric-layout"},
                            {"Key": "BorsukAttempt", "Value": "a0001"},
                        ],
                    }
                ],
                "UserData": user_data,
            }
        )
    return specs


def _s3_location(uri: str) -> tuple[str, str]:
    bucket, separator, key = uri.removeprefix("s3://").partition("/")
    if not separator or not bucket or not key:
        raise ValueError("layout S3 URI differs")
    return bucket, key.rstrip("/")


def _atomic_put(s3_client: object, *, bucket: str, key: str, body: bytes) -> None:
    event_name = "before-sign.s3.PutObject"
    event_id = f"native-geometric-{hashlib.sha256(key.encode()).hexdigest()[:16]}"

    def add_precondition(request: object, **_: object) -> None:
        request.headers["If-None-Match"] = "*"

    s3_client.meta.events.register_first(
        event_name, add_precondition, unique_id=event_id
    )
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
    finally:
        s3_client.meta.events.unregister(event_name, unique_id=event_id)


def _terminate_and_wait(ec2_client: object, instance_id: str) -> None:
    ec2_client.terminate_instances(InstanceIds=[instance_id])
    ec2_client.get_waiter("instance_terminated").wait(
        InstanceIds=[instance_id],
        WaiterConfig={"Delay": 5, "MaxAttempts": 60},
    )


def launch_and_monitor(plan: SpotLayoutPlan) -> dict[str, object]:
    import boto3

    session = boto3.Session(profile_name=plan.profile, region_name="eu-central-1")
    ec2 = session.client("ec2")
    s3 = session.client("s3")
    bucket, prefix = _s3_location(plan.output_prefix)
    for name in ("reservation.json", "terminal.json"):
        try:
            s3.head_object(Bucket=bucket, Key=f"{prefix}/{name}")
        except Exception as error:
            code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                continue
            raise
        raise ValueError("layout immutable attempt already exists")
    reservation = json.dumps(
        {
            "attempt": 1,
            "schema": "borsuk-native-geometric-layout-reservation-v1",
            "source_commit": plan.source_commit,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode() + b"\n"
    _atomic_put(
        s3,
        bucket=bucket,
        key=f"{prefix}/reservation.json",
        body=reservation,
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
        raise RuntimeError("layout Spot capacity unavailable")
    deadline = time.monotonic() + plan.wall_seconds + 900
    try:
        while True:
            try:
                response = s3.get_object(Bucket=bucket, Key=f"{prefix}/terminal.json")
            except Exception as error:
                code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
                if code not in {"404", "NoSuchKey", "NotFound"}:
                    raise
                if time.monotonic() >= deadline:
                    raise TimeoutError("layout Spot terminal deadline exceeded") from error
                time.sleep(15)
                continue
            body = response["Body"].read()
            return _validate_terminal_bytes(
                body,
                plan.source_commit,
                instance_id,
                plan.output_prefix,
            )
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
            uri=args.source_archive_uri,
            sha256=args.source_archive_sha256,
            encoded_bytes=args.source_archive_bytes,
        ),
        source=FROZEN_SOURCE,
        queries=FROZEN_QUERIES,
        truth=FROZEN_TRUTH,
        requirements_sha256=args.requirements_sha256,
        output_prefix=args.output_prefix,
        image_id=args.image_id,
        security_group_id=args.security_group_id,
        instance_profile_arn=args.instance_profile_arn,
        targets=DEFAULT_TARGETS,
    )


def main(argv: Sequence[str] | None = None) -> None:
    terminal = launch_and_monitor(parse_args(argv))
    print(json.dumps(terminal, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
