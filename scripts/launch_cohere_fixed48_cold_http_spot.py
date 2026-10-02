#!/usr/bin/env python3
"""One retained fixed48 cold HTTP gate: aNNNN | --self-check | --stage-configs-check.

Root freezes executor source, then config.json, controller-config.json and
asset-manifest.json beneath ROOT. contract() exposes the exact freeze API.
Internal worker entry: --stage REPO OUTPUT PREFIX. No replacement or build.
"""
import ast
import base64
from contextlib import contextmanager
import fcntl
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from unittest.mock import patch

if not __debug__:
    raise RuntimeError("authority checks require assertions enabled")
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_cohere_fixed48_cold_http as runtime

science, driver, library = runtime.science, runtime.driver, runtime.library
ROOT = runtime.BASE / "cold-http"
CONFIG, NAME = ROOT / "config.json", ""
CONTROL, MANIFEST = ROOT / "controller-config.json", ROOT / "asset-manifest.json"
OWN = "scripts/launch_cohere_fixed48_cold_http_spot.py"
MODULE = OWN[:-3].replace("/", ".")
SCHEMA = "borsuk-cohere-fixed48-cold-http-spot-v1"
PREFIX = "research/semantic-router/20261002/fixed48-cold-"
TOKEN_PREFIX, TAG = "fixed48-cold-", "borsuk-fixed48-cold-http"
WALL, WORKER_SECONDS, SERVICE_SECONDS = 3000, 2340, 2400
MEMORY, SCRATCH = 12 << 30, 16 << 30
WORK_ROOT = Path("/mnt/cohere-fixed48-cold")
INSTANCE_TYPE, IMAGE_ID = science.INSTANCE_TYPE, science.IMAGE_ID
ROOT_DEVICE_NAME, SUBNET = science.ROOT_DEVICE_NAME, science.SUBNET
REGION, BUCKET = science.REGION, science.BUCKET
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, .50
AWSCLI_VERSION, AWSCLI_SHA256 = science.AWSCLI_VERSION, science.AWSCLI_SHA256
LOCAL_BYTES, MAX_BODY_BYTES = 1 << 20, 64 << 20
DIAGNOSTIC_FILES = ("failure.json", "cold-closure.json", "screen/summary.json", "screen/cleanup.json", "profile.log")
DIAGNOSTIC_BYTES = 4096
encoded, sha, artifact, write = science.encoded, science.sha, science.artifact, science.write
regular_path, read_json = science.regular_path, science.read_json
archive_digest, lifecycle, pin = science.archive_digest, science.lifecycle, science.pin
VERIFICATION = runtime.BASE / "cold-runtime-verification"
RUNTIME_FILES = ("config.json", "source-qualification.json", "sdk-ledger.jsonl", "input-hashes.json",
    "publisher-requests.jsonl", "request-derivative.json", "original-generation-root.json", "transport-delta.json",
    "publication.log", "publication-resources.txt", "publication-reference.jsonl", "publication.json",
    "sealed-reference-k10.jsonl", "records.jsonl", "resources.json", "cleanup.json", "summary.json", "scientific-reference.json")
CONTROLLER_FILES = ("config.json", "controller-config.json", "asset-manifest.json", "source-qualification.json",
    "bootstrap-staging.json", "runtime-abi.json", "cpu.txt", "tool-versions.json", "run-closed.log",
    "profile.log", "profile-resources.txt", "profile-cgroup.json", "staging.json", "cold-closure.json", "failure.json",
    "controller-sdk-ledger.jsonl")
NATIVE_FILES = ("source-qualification.json", "config.json", "native-source-manifest.json", "source-before.json",
    "source-after.json", "workspace-receipt.json", "test.log", "test-resources.txt", "workspace-cgroup.json",
    "cpu.txt", "rustc-version.txt", "cargo-version.txt", "run-closed.log", "binaries/two_bit_http",
    "binaries/check_semantic_router_scorer", "binaries/two_bit_plan_demo", "aws-reservation.json",
    "aws-closeout.json", "aws-terminal.json", "collection-replay.json", "root-verification.json")
ARTIFACTS = (*CONTROLLER_FILES, *("native/" + n for n in NATIVE_FILES),
    *("screen/" + n for n in RUNTIME_FILES), "screen/COMPLETE.json")
TERMINAL_IDENTITIES = ("config_sha256", "controller_config_sha256", "asset_manifest_sha256",
    "code_identity_sha256", "runtime_code_identity_sha256", "proofs_identity_sha256", "assets_identity_sha256",
    "artifact_roster_sha256", "native_source_identity_sha256", "binary_sha256", "publisher_sha256",
    "campaign_schema", "awscli_version", "awscli_sha256")


def code_closure(repo):
    """Declared runtime closure plus every repository Python import, recursively."""
    pending, found = list((*runtime.CODE, OWN)), set()
    while pending:
        name = pending.pop()
        if name in found:
            continue
        path = regular_path(repo / name)
        assert path.is_file(), "missing controller source: " + name
        found.add(name)
        if path.suffix != ".py":
            continue
        for node in ast.walk(ast.parse(path.read_bytes(), filename=name)):
            modules = []
            if isinstance(node, ast.ImportFrom) and node.module == "scripts":
                modules = ["scripts." + a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            elif isinstance(node, ast.Import):
                modules = [a.name for a in node.names]
            for module in modules:
                if module.startswith("scripts."):
                    relative = module.replace(".", "/") + ".py"
                    assert (repo / relative).is_file(), "unresolved repository import: " + module
                    pending.append(relative)
    return tuple(sorted(found))


CODE = code_closure(Path(__file__).resolve().parents[1])


def contract():
    return dict(schema=SCHEMA + "-contract", root=str(ROOT), config=str(CONFIG), controller_config=str(CONTROL),
        asset_manifest=str(MANIFEST), controller_schema=SCHEMA, runtime_schema=runtime.SCHEMA,
        controller_fields=["schema", "authority_pending", "execution_source", "code_sha256", "runtime_config"],
        runtime_config_pointer=dict(path=str(CONFIG), bytes="positive integer <=1MiB", sha256="SHA256"),
        runtime_fixed=runtime.FIXED,
        runtime_additional_fields=["bucket", "namespace_prefix", "code_sha256", "execution_source", "asset_manifest", "proofs", "resources"],
        source_environment=["BORSUK_COLD_SOURCE_COMMIT", "BORSUK_COLD_ARCHIVE_SHA256"],
        resource_environment=dict.fromkeys(runtime.retained.THREAD_ENV, "2") | {"AWS_MAX_ATTEMPTS": "1"},
        controller_code=list(CODE), runtime_code=list(runtime.CODE), runtime_outputs=list(RUNTIME_FILES),
        controller_code_additions=sorted(set(CODE) - set(runtime.CODE)), controller_outputs=list(CONTROLLER_FILES),
        native_outputs=list(NATIVE_FILES),
        runtime_marker="COMPLETE.json", artifacts=list(ARTIFACTS), terminal_identities=list(TERMINAL_IDENTITIES),
        resources=dict(runtime.HOST, publication_limit_seconds=900, cold_limit_seconds=900,
            service_limit_seconds=SERVICE_SECONDS, output_reserve_bytes="root-frozen positive integer <=256MiB"),
        namespace_prefix=PREFIX + "aNNNN/serving", instance_type=INSTANCE_TYPE, image_id=IMAGE_ID,
        machine_limit_seconds=WALL, worker_limit_seconds=WORKER_SECONDS, compute_cap_usd=COMPUTE_CAP,
        ebs_s3_allowance_usd=.15, cli="aNNNN | --self-check | --stage-configs-check", worker_cli="--stage REPO OUTPUT PREFIX",
        api=["contract", "qualify", "preflight", "stage_configs", "stage_configs_check", "user_data", "poll", "stage", "offline_reduce", "validate_closed", "collect", "main", "self_check"],
        source_freeze="All CODE matches pushed archive; three JSON authorities absent from executor archive and committed afterwards.",
        cold_http_measured=False, launch_authorized=False)


def root_verification(repo):
    root = read_json(repo / VERIFICATION / "root-verification.json")
    assert root["schema"] == "borsuk-fixed48-retained-cold-root-verification-v1"
    assert root["owned_source"] == dict(path=runtime.OWN, **artifact(repo / runtime.OWN)), "unqualified runtime source"
    assert root["historical_proofs_authenticated"] == len(runtime.PROOF_PATHS) == 20
    assert root["asset_identities_and_sources_authenticated"] == 38 and root["executor_closure_paths"] == len(runtime.CODE) == 74
    assert root["self_check"]["exit_status"] == 0 and root["self_check"]["queries"] == 64 and root["self_check"]["scenarios"] == 8
    assert root["native_or_network_execution"] is root["native_rebuilt"] is root["full_workspace_test_execution"] is root["launch_authorized"] is False
    assert root["cold_http_measurement"] == "UNMEASURED"
    for name, identity in root["artifacts"].items():
        driver.safe_key(name)
        assert artifact(regular_path(repo / VERIFICATION / name)) == pin(identity, LOCAL_BYTES), "root verification body drift"
    qualified = read_json(repo / VERIFICATION / "worker-contract.json")
    assert set(qualified["current_executor_code_roster"]) == set(runtime.CODE)
    assert set(qualified["output_roster"]) == {*RUNTIME_FILES, "COMPLETE.json"}, "runtime18 contract"
    assert qualified["proof_paths"] == {n: str(p) for n, p in runtime.PROOF_PATHS.items()}
    return {str(VERIFICATION / n): artifact(repo / VERIFICATION / n)
        for n in ("root-verification.json", *root["artifacts"])}


def qualify(base=Path("."), config_path=None, config_sha=None):
    repo = regular_path(base)
    path = regular_path(repo / CONFIG if config_path is None else config_path)
    assert artifact(path)["bytes"] <= LOCAL_BYTES
    digest = artifact(path)["sha256"]
    assert config_sha is None or digest == config_sha, "config drift"
    control = read_json(repo / CONTROL)
    assert set(control) == {"schema", "authority_pending", "execution_source", "code_sha256", "runtime_config"}
    assert control["schema"] == SCHEMA and control["authority_pending"] is False, "controller freeze pending"
    assert science.read_repo(repo, control["runtime_config"], CONFIG) == path.read_bytes()
    code = {n: artifact(regular_path(repo / n))["sha256"] for n in code_closure(repo)}
    assert code_closure(repo) == CODE and control["code_sha256"] == code, "exact controller closure drift"
    config = read_json(path)
    assert config["execution_source"] == control["execution_source"], "source authorities differ"
    source = config["execution_source"]
    assert set(source) == {"commit", "archive_sha256"} and re.fullmatch("[0-9a-f]{40}", source["commit"])
    assert re.fullmatch("[0-9a-f]{64}", source["archive_sha256"]), "source freeze pending"
    with patch.dict(os.environ, BORSUK_COLD_SOURCE_COMMIT=source["commit"], BORSUK_COLD_ARCHIVE_SHA256=source["archive_sha256"]):
        _, assets, historical = runtime.qualify(path, digest, repo)
    assert config["asset_manifest"]["path"] == str(MANIFEST), "asset manifest destination"
    assert set(assets) == {*('qualification/' + n for n in NATIVE_FILES),
        *('generation/' + n for n in runtime.retained.GENERATION_FILES), "sq8.bin", *('panel/' + n for n in runtime.PANEL_FILES)}
    assert set(assets) == set(json.loads(science.read_repo(repo, config["asset_manifest"]))["assets"]) and len(assets) == 38
    assert re.fullmatch(re.escape(PREFIX) + "a[0-9]{4}/serving", config["namespace_prefix"]), "one fresh owned serving namespace"
    limits = config["resources"]
    assert all(limits[n] == v for n, v in (('publication_limit_seconds', 900), ('cold_limit_seconds', 900), ('service_limit_seconds', SERVICE_SECONDS)))
    assert 0 < limits["output_reserve_bytes"] <= 256 << 20, "bounded output reserve"
    original = read_json(repo / science.QUALIFICATION / "source-qualification.json")
    native_source = driver.qualification.worker.source_hashes(repo)
    assert original["source_sha256"] == native_source and len(native_source) == 399
    assert driver.qualification.worker.source_identity(native_source) == driver.SOURCE_ID, "unchanged399 source"
    assert historical["scientific_decision"]["scientific_status"] == "GO", "scientific disposition admission"
    verified = root_verification(repo)
    return dict(config_path=str(CONFIG), config_sha256=digest, controller_config_sha256=artifact(repo / CONTROL)["sha256"],
        asset_manifest_sha256=config["asset_manifest"]["sha256"], source_archive_commit=source["commit"],
        source_archive_sha256=source["archive_sha256"], code_identity_sha256=sha(encoded(code)),
        runtime_code_identity_sha256=sha(encoded(config["code_sha256"])), proofs_identity_sha256=sha(encoded(config["proofs"])),
        assets_identity_sha256=sha(encoded(assets)), artifact_roster_sha256=sha(encoded(ARTIFACTS)),
        native_source_identity_sha256=driver.SOURCE_ID, binary_sha256=assets["qualification/binaries/two_bit_http"]["sha256"],
        publisher_sha256=assets["qualification/binaries/two_bit_plan_demo"]["sha256"], campaign_schema=SCHEMA,
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256, artifacts=list(ARTIFACTS), assets=assets,
        authority_paths=[str(CONFIG), str(CONTROL), str(MANIFEST)], root_verification=verified,
        output_reserve_bytes=limits["output_reserve_bytes"], namespace_prefix=config["namespace_prefix"])


def preflight(base=Path("."), collection_out=None):
    repo = regular_path(base)
    owned = None
    if collection_out is not None:
        out = regular_path(collection_out)
        assert re.fullmatch("a[0-9]{4}", out.name) and out == regular_path(repo / ROOT / out.name), "unowned output"
        owned = str(out.relative_to(repo)) + "/"
    status = subprocess.check_output(["git", "status", "--porcelain", "-z", "--untracked-files=all"], cwd=repo, text=True)
    assert all(owned and x.startswith("?? ") and x[3:].startswith(owned) for x in status.split("\0") if x), "dirty frozen source"
    proof = qualify(repo)
    source = proof["source_archive_commit"]
    for a, b in ((source, "HEAD"), (source, "origin/main"), ("HEAD", "origin/main")):
        subprocess.run(["git", "merge-base", "--is-ancestor", a, b], cwd=repo, check=True)
    for name in proof["authority_paths"]:
        assert (repo / name).read_bytes() == subprocess.check_output(["git", "show", "HEAD:" + name], cwd=repo), "uncommitted authority"
        assert subprocess.run(["git", "cat-file", "-e", source + ":" + name], cwd=repo,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0, "source must precede JSON freeze"
    for name in CODE:
        assert subprocess.check_output(["git", "show", source + ":" + name], cwd=repo) == (repo / name).read_bytes(), "executor source drift: " + name
    assert archive_digest(source, repo) == proof["source_archive_sha256"], "archive source drift"
    return proof


def config_key(digest):
    assert re.fullmatch("[0-9a-f]{64}", digest)
    return "research/semantic-router/20261002/fixed48-cold-configs/" + digest + ".json"


def stage_configs(proof):
    """Reuse only fully authenticated immutable JSON; never overwrite or retry."""
    from botocore.config import Config
    from botocore.exceptions import ClientError

    body = encoded(proof)
    authorities = [(pin(dict(bytes=len(body), sha256=sha(body)), LOCAL_BYTES), body)]
    for name, digest in zip(proof["authority_paths"],
            (proof[n] for n in ("config_sha256", "controller_config_sha256", "asset_manifest_sha256")), strict=True):
        identity = pin(artifact(Path(name)), LOCAL_BYTES)
        assert identity["sha256"] == digest, "frozen authority drift"
        body = Path(name).read_bytes()
        assert dict(bytes=len(body), sha256=sha(body)) == identity, "local authority drift"
        authorities.append((identity, body))
    s3 = lifecycle()[0].boto3.Session(profile_name="causality", region_name=REGION).client("s3",
        config=Config(retries={"total_max_attempts": 1}, connect_timeout=5, read_timeout=5))
    try:
        for identity, body in authorities:
            key = config_key(identity["sha256"])
            try:
                response = s3.get_object(Bucket=BUCKET, Key=key)
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404", "NotFound"}:
                    raise
                try:
                    s3.put_object(Bucket=BUCKET, Key=key, Body=body, IfNoneMatch="*")
                except ClientError as error:
                    if error.response.get("Error", {}).get("Code") != "PreconditionFailed":
                        raise
                    response = s3.get_object(Bucket=BUCKET, Key=key)
                else:
                    continue
            stream = response["Body"]
            try:
                assert response["ContentLength"] == identity["bytes"], "remote authority length"
                remote = bytearray()
                while True:
                    chunk = stream.read(identity["bytes"] + 1 - len(remote))
                    if not chunk:
                        break
                    remote.extend(chunk)
                    assert len(remote) <= identity["bytes"], "remote authority overflow"
                assert dict(bytes=len(remote), sha256=sha(remote)) == identity, "remote authority SHA/length"
                assert remote == body, "remote authority body differs"
            finally:
                stream.close()
    finally:
        s3.close()


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert (commit, archive_sha) == (qualification["source_archive_commit"], qualification["source_archive_sha256"])
    assert re.fullmatch(re.escape(PREFIX) + "a[0-9]{4}", prefix) and qualification["namespace_prefix"] == prefix + "/serving"
    driver.safe_key(archive_key)
    assert artifact(CONFIG)["sha256"] == qualification["config_sha256"]
    _, bootstrap = lifecycle()
    adapter = {k: qualification[k] for k in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG), native_binary={"key": "unused"}, native_publisher={"key": "unused"})
    with patch.multiple(bootstrap, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS, TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), patch.object(bootstrap, "_offered", return_value=False):
        body = bootstrap.user_data(commit, archive_sha, archive_key, prefix, adapter)
    source_line = f"aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors\n"
    assert body.count(source_line) == 1, "source bootstrap hook drift"
    source_command = (f"systemd-run --quiet --wait --pipe -p MemoryMax={MEMORY} -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=300 -p WorkingDirectory=\"$root\" "
        f"python3.12 - {shlex.quote(BUCKET)} {shlex.quote(archive_key)} {archive_sha} {SCRATCH} <<'SOURCE'\n" + science.BOOTSTRAP_STAGE + "SOURCE\n")
    body = body.replace(source_line, source_command)
    early = {k: qualification[k] for k in (*TERMINAL_IDENTITIES, "source_archive_commit", "source_archive_sha256")}
    early["proof_sha256"] = sha(encoded(qualification))
    early64 = base64.b64encode(gzip.compress(encoded(early), mtime=0)).decode()
    install = f'''phase=install
python3.12 -m venv --system-site-packages "$root/venv"
lscpu >cpu.txt
phase=cold
systemd-run --unit=cohere-fixed48-cold --wait --pipe -p MemoryMax={MEMORY} -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec={SERVICE_SECONDS} -p WorkingDirectory="$root" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C {' '.join('--setenv='+n+'=2' for n in runtime.retained.THREAD_ENV)} \\
 --setenv=BORSUK_COLD_SOURCE_COMMIT={commit} --setenv=BORSUK_COLD_ARCHIVE_SHA256={archive_sha} \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 {WORKER_SECONDS} \\
 taskset -c 4-5 "$root/venv/bin/python" -m {MODULE} --stage "$root/repo" "$root" {prefix} >profile.log 2>&1
'''
    start, end = body.index("phase=install\n"), body.index("phase=complete\n")
    body = body[:start] + install + body[end:]
    fields = {k: qualification[k] for k in TERMINAL_IDENTITIES}
    assert body.count(repr(fields)[1:-1]) == 1
    body = body.replace(repr(fields)[1:-1], "**{k:v for k,v in json.loads(Path('source-qualification.json').read_text()).items() if k != 'proof_sha256'}")
    body = body.replace("'source_archive_sha256':'" + archive_sha + "',", "", 1)
    body = body.replace(", 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256')", "")
    body = body.replace("/mnt/native-semantic-router-cold", str(WORK_ROOT))
    body = body.replace("python3-boto3 python3.12", "python3-boto3 python3.12 python3.12-venv")
    body = body.replace("exec >run.log 2>&1\n", "exec >run.log 2>&1\n" +
        f"python3 -c 'import base64,gzip; from pathlib import Path; Path(\"source-qualification.json\").write_bytes(gzip.decompress(base64.b64decode(\"{early64}\",validate=True)))'\n", 1)
    reserve = '''import os,shutil,sys,zipfile
from pathlib import Path
root=Path(sys.argv[1]); limit=int(sys.argv[2]); reserve=int(sys.argv[3]); seen=set(); used=0
for base,ds,fs in os.walk(root,followlinks=False):
 for name in fs:
  p=Path(base)/name
  if p.is_symlink(): continue
  s=p.stat(); key=s.st_dev,s.st_ino
  if key not in seen: used+=max(s.st_size,s.st_blocks*512); seen.add(key)
if len(sys.argv)>4:
 with zipfile.ZipFile(sys.argv[4]) as z:
  reserve+=3*sum(((m.file_size+4095)//4096)*4096 for m in z.infolist())
assert used+reserve<=limit and shutil.disk_usage(root).free>=reserve,'bootstrap whole-worker reserve'
'''
    marker = "phase=awscli-download\n"
    assert body.count(marker) == 1
    body = body.replace(marker, "cat >whole-reserve.py <<'RESERVE'\n" + reserve + "RESERVE\n" +
        f'python3 whole-reserve.py "$root" {SCRATCH} {bootstrap.AWSCLI_BYTES}\n' + marker, 1)
    body = body.replace("phase=awscli-install\n", f'phase=awscli-install\npython3 whole-reserve.py "$root" {SCRATCH} 16777216 awscliv2.zip\n')
    # Keep the AWSCLI payload/install/cache under the inventoried worker root.
    body = body.replace("export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1", 'export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1\nexport PATH="$root/bin:$PATH"\nmkdir -p "$root/apt"')
    body = body.replace("apt-get -qq", 'apt-get -o Dir::Cache::archives="$root/apt" -qq')
    body = body.replace("--output awscliv2.zip", f"--max-filesize {bootstrap.AWSCLI_BYTES} --output awscliv2.zip")
    body = body.replace("./aws/install\n", './aws/install --install-dir "$root/aws-cli" --bin-dir "$root/bin"\n')
    body = body.replace("if path.is_file():", "if path.is_file() and (name != 'screen/COMPLETE.json' or os.environ['EXIT_CODE'] == '0'):", 1)
    body = body.replace('if [ -f "$name" ]; then', 'if [ -f "$name" ] && { [ "$name" != screen/COMPLETE.json ] || [ "$code" = 0 ]; }; then')
    body = body.replace('timeout --kill-after=5 60 aws s3 cp "$name"',
        f'systemd-run --quiet --wait --pipe -p MemoryMax={MEMORY} -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=65 --setenv=AWS_MAX_ATTEMPTS=1 timeout --kill-after=5 60 aws s3 cp "$root/$name"')
    body = body.replace('timeout --kill-after=5 60 aws s3 cp terminal.json', 'timeout --kill-after=5 60 aws s3 cp "$root/terminal.json"')
    # systemd does not inherit the installing shell's worker-local PATH.
    body = body.replace(" aws s3 cp ", ' "$root/bin/aws" s3 cp ')
    body = body.replace("command -v aws >/dev/null", 'test -x "$root/bin/aws"')
    body = body.replace("cli_version=$(aws --version)", 'cli_version=$("$root/bin/aws" --version)')
    assert body.count('"$root/bin/aws" s3 cp ') == 2 and "60 aws s3 cp " not in body, "upload CLI hook drift"
    diagnostics = f'''diagnostics={{}}
if int(os.environ['EXIT_CODE']):
    for name in {DIAGNOSTIC_FILES!r}:
        if name in artifacts:
            offset=max(0,artifacts[name]['bytes']-{DIAGNOSTIC_BYTES})
            with Path(name).open('rb') as source:
                source.seek(offset); body=source.read({DIAGNOSTIC_BYTES})
            diagnostics[name]={{'offset':offset,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'body_base64':base64.b64encode(body).decode()}}
'''
    assert body.count("code=int(os.environ['EXIT_CODE'])\n") == 1
    body = body.replace("import hashlib,json,os\n", "import base64,hashlib,json,os\n", 1)
    body = body.replace("code=int(os.environ['EXIT_CODE'])\n", diagnostics + "code=int(os.environ['EXIT_CODE'])\n", 1)
    body = body.replace("'artifacts':artifacts}", "'artifacts':artifacts,'failure_diagnostics':diagnostics}", 1)
    # Every generated Python fragment, including the terminal, is syntax checked.
    for fragment in re.findall(r"<<'([A-Z]+)'[^\n]*\n(.*?)\n\1\n", body, re.S):
        compile(fragment[1], "<cold-" + fragment[0] + ">", "exec")
    subprocess.run(["bash", "-n"], input=body, text=True, check=True)
    assert len(body.encode()) <= 16384, "EC2 user data cap"
    assert not any(n in body for n in ("cargo ", "rustup", "unused", "pip install", "--publish"))
    return body


def poll(ec2, s3, prefix, instance_id, started):
    shared, _ = lifecycle()
    with patch.object(shared, "WALL", WALL):
        return shared.poll(ec2, s3, prefix, instance_id, started)


@contextmanager
def inventory(out, reserve, report):
    with science.scratch_monitor(out, report) as check:
        check()
        assert science.scratch_bytes(out) + reserve <= SCRATCH, "whole-worker admission reserve"
        assert shutil.disk_usage(out).free >= reserve, "whole-worker disk headroom"
        yield check


def native_abi(out, assets):
    """Use existing Ubuntu/glibc/ldd admission before the publisher or HTTP."""
    _, bootstrap = lifecycle()
    abi = out / "abi"
    (abi / "binaries").mkdir(parents=True)
    try:
        pins = {n: artifact(assets / "qualification/binaries" / n) for n in ("two_bit_http", "two_bit_plan_demo")}
        requirements = {}
        for name in pins:
            target = abi / "binaries" / name
            os.link(assets / "qualification/binaries" / name, target)
            target.chmod(0o500)
            requirements[name] = bootstrap._required_glibc(target)
        write(abi / "source-qualification.json", dict(runtime_os=bootstrap.RUNTIME_OS, runtime_glibc=bootstrap.RUNTIME_GLIBC,
            required_glibc=requirements, binary_bytes=pins["two_bit_http"]["bytes"], binary_sha256=pins["two_bit_http"]["sha256"],
            publisher_bytes=pins["two_bit_plan_demo"]["bytes"], publisher_sha256=pins["two_bit_plan_demo"]["sha256"]))
        return bootstrap._runtime_abi(abi)
    finally:
        if (abi / "runtime-abi.json").exists():
            write(out / "runtime-abi.json", (abi / "runtime-abi.json").read_bytes())
        shutil.rmtree(abi)


def stage(repo, out, prefix):
    repo, out = regular_path(repo), regular_path(out)
    assert out == WORK_ROOT and repo == out / "repo", "exact whole-worker root"
    assert re.fullmatch(re.escape(PREFIX) + "a[0-9]{4}", prefix)
    started, before = time.monotonic(), runtime.snapshot()
    staging = dict(closed=False, peak_scratch_bytes=0, shared_hardlinks_accounted=True)
    closure = dict(closed=False, runtime_invocations=0, build_invocations=0, scientific_scorer_invocations=0,
        oracle_invocations=0, process_cleanup=False, replay_passed=False, execution_status="FAIL", scientific_status="INVALID")
    failure = dict(status="pending", replacement_allowed=False)
    old_term, old_alarm = signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGALRM)
    def interrupted(signum, frame):
        raise InterruptedError("worker interrupted: " + str(signum))
    signal.signal(signal.SIGTERM, interrupted); signal.signal(signal.SIGALRM, interrupted)
    signal.setitimer(signal.ITIMER_REAL, WORKER_SECONDS)
    s3 = None
    try:
        runtime.check_cgroup(before, before, runtime.HOST)
        import boto3
        from botocore.config import Config
        s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"total_max_attempts": 1}, connect_timeout=5, read_timeout=5))
        early = read_json(out / "source-qualification.json")
        deadline = started + WORKER_SECONDS
        with inventory(out, 8 << 20, staging), patch.object(science, "WORK_ROOT", out):
            # Separate small authorities are streamed, fsynced and authenticated.
            with (out / "controller-sdk-ledger.jsonl").open("xb") as ledger:
                def download(digest, target):
                    with runtime.sdk_operation(s3, "head_object", BUCKET, config_key(digest), ledger) as (response, row):
                        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
                        identity = pin(dict(bytes=response["ContentLength"], sha256=digest), LOCAL_BYTES)
                    return runtime.fetch(s3, dict(bucket=BUCKET, key=config_key(digest)), identity, ledger,
                        lambda reserve=0: whole_admission(out, reserve, staging), deadline, target)
                download(early["proof_sha256"], out / "frozen-qualification.json")
                proof = read_json(out / "frozen-qualification.json")
                assert all(early[k] == proof[k] for k in (*TERMINAL_IDENTITIES, "source_archive_commit", "source_archive_sha256"))
                for name, digest in (("config.json", proof["config_sha256"]), ("controller-config.json", proof["controller_config_sha256"]), ("asset-manifest.json", proof["asset_manifest_sha256"])):
                    download(digest, out / name)
                    destination = repo / ROOT / name
                    assert not destination.exists(), "authority present in executor archive"
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    write(destination, (out / name).read_bytes())
            assert qualify(repo, out / "config.json", proof["config_sha256"]) == proof, "worker authority drift"
            assert proof["namespace_prefix"] == prefix + "/serving"
            assert os.environ.get("BORSUK_COLD_SOURCE_COMMIT") == proof["source_archive_commit"]
            assert os.environ.get("BORSUK_COLD_ARCHIVE_SHA256") == proof["source_archive_sha256"]
            assert all(os.environ.get(n) == "2" for n in runtime.retained.THREAD_ENV) and os.environ.get("AWS_MAX_ATTEMPTS") == "1"
            write(out / "source-qualification.json", proof)
            import platform
            release = platform.freedesktop_os_release()
            assert platform.machine() == "x86_64" and (release["ID"], release["VERSION_ID"]) == ("ubuntu", "24.04")
            write(out / "tool-versions.json", dict(python=sys.version, architecture=platform.machine(), os_release=release,
                thread_environment=dict.fromkeys(runtime.retained.THREAD_ENV, "2"), aws_max_attempts=1))
            config = read_json(out / "config.json")
            reserve = sum(p["bytes"] for p in proof["assets"].values()) + proof["output_reserve_bytes"]
            whole_admission(out, reserve, staging)
            original_publish, original_observe = runtime.publish_generation, runtime.observe
            def publish(*args):
                assets = args[2] / "assets"
                native_abi(out, assets)
                for name in NATIVE_FILES:
                    target = out / "native" / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    source = assets / "qualification" / name
                    assert artifact(source) == library.identity(proof["assets"]["qualification/" + name])
                    os.link(source, target)
                return original_publish(*args)
            closure["runtime_invocations"] = 1
            with patch.object(runtime, "publish_generation", side_effect=publish), patch.object(runtime, "observe",
                    side_effect=lambda output, limits, report, deadline: original_observe(out, limits, report, deadline)):
                summary = runtime.run(out / "config.json", proof["config_sha256"], repo, out / "screen")
            reduced = offline_reduce(out / "screen", config, proof, repo)
            assert reduced == summary, "worker offline reducer mismatch"
            closure.update(closed=True, process_cleanup=True, replay_passed=True, execution_status="SUCCESS", scientific_status=summary["status"])
            staging["closed"] = True
            failure["status"] = "complete"
    except BaseException as error:
        failure.update(status="failed", error_type=type(error).__name__, error=str(error))
    finally:
        try:
            if s3 is not None:
                s3.close()
        except BaseException as error:
            closure.update(closed=False, execution_status="FAIL")
            failure.update(status="failed", sdk_cleanup_error=str(error))
        # Runtime normally removes its scratch itself; interruption before that
        # point still has one owner and one terminal cleanup record.
        try:
            if (out / "screen/scratch").exists():
                shutil.rmtree(out / "screen/scratch")
            assert not (out / "screen/scratch").exists()
        except BaseException as error:
            closure.update(closed=False, process_cleanup=False, execution_status="FAIL")
            failure.update(status="failed", scratch_cleanup_error=str(error))
        closure["wall_seconds"] = time.monotonic() - started
        counters = dict(before=before, after=runtime.snapshot(), closed=True)
        try:
            runtime.check_cgroup(before, counters["after"], runtime.HOST, drained=True)
        except BaseException as error:
            closure.update(closed=False, process_cleanup=False, execution_status="FAIL")
            failure.update(status="failed", resource_error=str(error))
        write(out / "profile-cgroup.json", counters)
        write(out / "staging.json", staging); write(out / "cold-closure.json", closure); write(out / "failure.json", failure)
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGTERM, old_term); signal.signal(signal.SIGALRM, old_alarm)
    return closure


def whole_admission(out, reserve, report):
    usage = science.scratch_usage(out)
    used = max(usage["unique_inode_bytes"], usage["physical_allocated_bytes"])
    report.update(scratch_usage=usage, peak_scratch_bytes=max(report["peak_scratch_bytes"], used))
    assert used + reserve <= SCRATCH and shutil.disk_usage(out).free >= reserve, "whole-worker stream reserve"


def offline_reduce(screen, config, proof, repo):
    """Only closed, authenticated records; replay arithmetic and native telemetry."""
    summary, marker = read_json(screen / "summary.json"), read_json(screen / "COMPLETE.json")
    assert marker["schema"] == runtime.SCHEMA + "-complete" and marker["config_sha256"] == proof["config_sha256"]
    assert set(marker["files"]) == set(RUNTIME_FILES), "closed runtime18 roster"
    for name, identity in marker["files"].items():
        assert artifact(regular_path(screen / name)) == identity, "closed runtime body drift: " + name
    assert marker["status"] == summary["status"] and marker["passed"] is summary["quality_gate_passed"]
    assets = proof["assets"]
    assert read_json(screen / "input-hashes.json") == assets
    # Authenticate the existing sealed panel, never construct new truth.
    panel = repo / runtime.SCIENTIFIC / "screen"
    for name in ("requests.jsonl", "records.jsonl", "truth.i64"):
        assert artifact(panel / name) == library.identity(assets["panel/" + name]), "closed scientific panel body"
    derived, references, truths = runtime.panel_inputs(panel)
    assert (screen / "publisher-requests.jsonl").read_bytes() == derived
    expected = [dict(r, ids=r["ids"][:10]) for r in references]
    assert (screen / "sealed-reference-k10.jsonl").read_bytes() == b"".join(encoded(r) + b"\n" for r in expected)
    publication = read_json(screen / "publication.json")
    arm = publication["arm"]
    assert arm["indexes"] == {"10": config["namespace_prefix"]}
    root = read_json(screen / "original-generation-root.json")
    assert artifact(screen / "original-generation-root.json") == library.identity(assets["generation/manifest.json"])
    assert publication["publication_via_production_library"] is True and publication["process"]["exit_status"] == 0
    assert publication["process"]["process_cleanup"] is True
    assert publication["manifest"] == runtime.transport_manifest(root, config["namespace_prefix"], publication["sq8"]["etag"])
    serving = publication["manifest"]
    assert arm["authority"] == dict(root_sha256=sha(encoded(serving) + b"\n"), generation=1, control_epoch=1)
    head_body = base64.b64decode(publication["head_body_base64"], validate=True)
    head = json.loads(head_body)
    assert head == dict(schema="borsuk-two-bit-head-v2", generation=1, epoch=1,
        root_sha256=arm["authority"]["root_sha256"], mutation=None, fence=None)
    assert arm["head_file"] == dict(bytes=len(head_body), sha256=sha(head_body))
    for name in library.STARTUP:
        identity = (dict(bytes=len(encoded(serving) + b"\n"), sha256=sha(encoded(serving) + b"\n")) if name == "manifest.json"
            else library.identity(assets["generation/" + name]))
        assert arm["metadata_files"][name] == identity["bytes"] and arm["metadata_sha256"][name] == identity["sha256"]
    assert arm["leaf_object"] == library.identity(assets["generation/router/leaves.bin"])
    assert publication["validation"] == library.publication_reference(screen / "publication-reference.jsonl", arm, references)
    models = dict(http=runtime.native_budget_model(arm["metadata_files"], retained_root=True),
        publisher_local=runtime.native_budget_model(arm["metadata_files"], slots=1, eager=True),
        publisher_remote=runtime.native_budget_model(arm["metadata_files"], slots=1))
    assert publication["native_budget_models"] == models
    assert models["http"]["modeled_remote_payload_bytes"] <= config["resources"]["native_memory_bytes"]
    assert max(models[n]["modeled_remote_payload_bytes"] for n in ("publisher_local", "publisher_remote")) <= config["resources"]["publisher_memory_bytes"]
    assert publication["resources"] == runtime.telemetry.resources((screen / "publication-resources.txt").read_text(), config["resources"]["publisher_memory_bytes"])
    scientific = read_json(screen / "scientific-reference.json")
    for name, field in (("requests.jsonl", "requests"), ("records.jsonl", "records"), ("truth.i64", "truth")):
        assert scientific[field] == library.identity(assets["panel/" + name])
    hits10 = sum(len(set(r["ids"][:10]) & set(t[:10])) for r, t in zip(references, truths))
    hits100 = sum(len(set(r["ids"]) & set(t)) for r, t in zip(references, truths))
    assert scientific["returned_hits10"] == hits10 and scientific["recall_at_10"] == hits10 / 640 and scientific["denominator10"] == 640
    assert scientific["returned_hits100"] == hits100 and scientific["recall_at_100"] == hits100 / 6400 and scientific["denominator100"] == 6400
    assert scientific["measurement_rerun"] is False
    derivative = read_json(screen / "request-derivative.json")
    assert derivative == dict(original=library.identity(assets["panel/requests.jsonl"]), derivative=artifact(screen / "publisher-requests.jsonl"), only_ordinal_key_changed=True, f32_bits_equal=True)
    records = [json.loads(line) for line in (screen / "records.jsonl").read_bytes().splitlines()]
    for row, reference, truth, request in zip(records, expected, truths, derived.splitlines()):
        assert row["outcome"] == "success", "closed cold execution failure"
        assert row["returned_hits"] == runtime.validate_query(row["response"], arm, reference, truth)
        assert row["accounting"] == library.transport(row["native_header"], row["response"], arm, wave_objects=8, root_reuse=True)
        assert row["resources"] == runtime.telemetry.resources(row["native_time_log"], config["resources"]["native_memory_bytes"])
        body = library.http_request(json.loads(request)["query"], arm["authority"])
        assert row["request_sha256"] == sha(body) and row["request_bytes"] == len(body)
        assert row["expected_authority"] == arm["authority"] and row["native_process_started"] is True
        runtime.check_cgroup(row["cgroup_before"], row["cgroup_after"], config["resources"], drained=True)
    reduced = runtime.reduce_records(records, records[0]["started_ns"], records[-1]["terminal_ns"])
    # The outer panel interval includes small bookkeeping before/after calls.
    reduced["serial_full_span_ns"] = summary["serial_full_span_ns"]
    assert reduced["serial_full_span_ns"] >= records[-1]["terminal_ns"] - records[0]["started_ns"] > 0
    assert reduced["serial_full_span_ns"] <= config["resources"]["cold_limit_seconds"] * 1_000_000_000
    reduced["serial_full_span_completions_per_second"] = 64 * 1e9 / reduced["serial_full_span_ns"]
    assert reduced == summary, "offline reducer result drift"
    cleanup, resources = read_json(screen / "cleanup.json"), read_json(screen / "resources.json")
    assert cleanup["valid"] is cleanup["scratch_removed"] is cleanup["process_cleanup"] is True
    assert cleanup["build_invocations"] == cleanup["scientific_scorer_invocations"] == cleanup["oracle_invocations"] == 0
    assert cleanup["publication_invocations"] == 1 and cleanup["cold_invocations"] == 64
    assert cleanup["remote_namespace_retained"] == config["namespace_prefix"]
    assert resources["build_invocations"] == resources["scientific_scorer_invocations"] == resources["oracle_invocations"] == 0
    assert resources["publication_invocations"] == 1 and resources["cold_invocations"] == 64
    assert resources["cgroup"]["closed"] is True
    runtime.check_cgroup(resources["cgroup"]["before"], resources["cgroup"]["after"], config["resources"], drained=True)
    assert 0 <= resources["peak_scratch_bytes"] <= SCRATCH and resources["process_peak_rss_bytes"] <= MEMORY
    source = read_json(screen / "source-qualification.json")
    assert source == dict(execution_source=config["execution_source"], current_executor_sha256=config["code_sha256"],
        original_qualification_source_identity=driver.SOURCE_ID, actual_full_workspace_execution=False,
        historical_science=config["proofs"]["historical_validation"], native_rebuilt=False,
        build_invocations=0, scientific_scorer_invocations=0, oracle_invocations=0)
    sdk_rows = [json.loads(line) for line in (screen / "sdk-ledger.jsonl").read_bytes().splitlines()]
    assert sdk_rows and all(r["sdk_http_dispatch_attempts"] == 1 and r["retry_attempts"] == 0 and r["error"] is None for r in sdk_rows)
    return reduced


def validate_closed(out, proof, terminal, files):
    complete = terminal["status"] == terminal["phase"] == "complete" and terminal["exit_code"] == 0
    assert type(terminal["exit_code"]) is type(terminal["original_exit_code"]) is int
    assert 0 <= terminal["exit_code"] <= 255 and 0 <= terminal["original_exit_code"] <= 255
    assert terminal["status"] == ("complete" if complete else "failed")
    if not complete:
        assert terminal["exit_code"] != 0
        return False
    assert terminal["original_exit_code"] == 0 and set(files) == set(ARTIFACTS), "complete body roster"
    for name, field in (("config.json", "config_sha256"), ("controller-config.json", "controller_config_sha256"), ("asset-manifest.json", "asset_manifest_sha256")):
        assert files[name]["sha256"] == proof[field]
    assert read_json(out / "source-qualification.json") == proof
    config = read_json(out / "config.json")
    assert config["execution_source"] == dict(commit=proof["source_archive_commit"], archive_sha256=proof["source_archive_sha256"])
    assert files["screen/config.json"]["sha256"] == proof["config_sha256"]
    marker = read_json(out / "screen/COMPLETE.json")
    assert marker["files"] == {n: files["screen/" + n] for n in RUNTIME_FILES}
    for name in NATIVE_FILES:
        assert files["native/" + name] == library.identity(proof["assets"]["qualification/" + name]), "native closure body"
    q = dict(directory=str(out / "native"), files={n: files["native/" + n] for n in NATIVE_FILES})
    native = driver.native_authority(dict(qualification=q), Path.cwd())
    assert native["native_qualification_passed"] is True and native["source_identity_sha256"] == driver.SOURCE_ID
    _, bootstrap = lifecycle()
    abi = read_json(out / "runtime-abi.json")
    bootstrap._validate_runtime_abi(dict(runtime_os=bootstrap.RUNTIME_OS, runtime_glibc=bootstrap.RUNTIME_GLIBC,
        required_glibc=abi["required_glibc"], binary_bytes=files["native/binaries/two_bit_http"]["bytes"], binary_sha256=proof["binary_sha256"],
        publisher_bytes=files["native/binaries/two_bit_plan_demo"]["bytes"], publisher_sha256=proof["publisher_sha256"]), abi)
    cold = read_json(out / "cold-closure.json")
    assert cold["closed"] is cold["process_cleanup"] is cold["replay_passed"] is True
    assert cold["execution_status"] == "SUCCESS" and cold["runtime_invocations"] == 1
    assert cold["build_invocations"] == cold["scientific_scorer_invocations"] == cold["oracle_invocations"] == 0
    assert 0 <= cold["wall_seconds"] <= WORKER_SECONDS
    summary = offline_reduce(out / "screen", config, proof, Path.cwd())
    assert cold["scientific_status"] == summary["status"] in ("PASS", "FAIL")
    counters = read_json(out / "profile-cgroup.json")
    assert counters["closed"] is True
    runtime.check_cgroup(counters["before"], counters["after"], config["resources"], drained=True)
    staging = read_json(out / "staging.json")
    assert staging["closed"] is staging["shared_hardlinks_accounted"] is True
    assert 0 <= staging["peak_scratch_bytes"] <= SCRATCH
    assert max(staging["scratch_usage"]["unique_inode_bytes"], staging["scratch_usage"]["physical_allocated_bytes"]) <= SCRATCH
    boot = read_json(out / "bootstrap-staging.json")
    assert boot["source_authenticated"] is True and boot["source_archive_sha256"] == proof["source_archive_sha256"]
    assert boot["scratch_limit_bytes"] == SCRATCH and boot["scratch_before_extract_bytes"] + boot["source_repository_reserve_bytes"] <= SCRATCH
    versions = read_json(out / "tool-versions.json")
    assert versions["architecture"] == "x86_64" and versions["python"].startswith("3.12.")
    assert versions["os_release"]["ID"] == "ubuntu" and versions["os_release"]["VERSION_ID"] == "24.04"
    assert versions["thread_environment"] == dict.fromkeys(runtime.retained.THREAD_ENV, "2") and versions["aws_max_attempts"] == 1
    timing = (out / "profile-resources.txt").read_text()
    assert 0 <= int(timing.split("Maximum resident set size (kbytes): ", 1)[1].splitlines()[0]) * 1024 <= MEMORY
    assert int(timing.split("Exit status: ", 1)[1].splitlines()[0]) == 0
    assert read_json(out / "failure.json")["status"] == "complete"
    sdk = [json.loads(line) for line in (out / "controller-sdk-ledger.jsonl").read_bytes().splitlines()]
    assert len(sdk) == 8 and [r["operation"] for r in sdk] == ["head_object", "get_object"] * 4
    assert all(r["sdk_http_dispatch_attempts"] == 1 and r["retry_attempts"] == 0 and r["error"] is None for r in sdk)
    return True


def collect(s3, prefix, out, instance_id, commit, digest):
    out = regular_path(out)
    assert re.fullmatch(re.escape(PREFIX) + "a[0-9]{4}", prefix)
    assert out == regular_path(ROOT / prefix.removeprefix(PREFIX)), "unowned collection"
    launch, close, reservation = (read_json(out / n) for n in ("aws-launch.json", "aws-closeout.json", "aws-reservation.json"))
    assert close["state"] == "terminated" and close["nodes"] == launch["nodes"], "terminate and wait BEFORE collection"
    assert instance_id == launch["instance_id"] in {n["instance_id"] for n in close["nodes"].values()}
    assert launch["prefix"] == prefix
    proof = reservation["qualification"]
    assert proof == preflight(collection_out=out), "collection freeze drift"
    assert reservation["schema"] == SCHEMA and reservation["config_sha256"] == proof["config_sha256"]
    for value in (launch, reservation):
        assert value["source_commit"] == commit and value["source_archive_sha256"] == digest
    receipts, failure_context = {}, {}
    try:
        with s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"] as stream:
            raw = stream.read(LOCAL_BYTES + 1)
        assert 0 < len(raw) <= LOCAL_BYTES, "bounded terminal"
        terminal = runtime.retained.primitives.decode(raw); write(out / "aws-terminal.json", raw)
        assert terminal["schema"] == SCHEMA and terminal["instance_id"] == instance_id
        assert terminal["source_commit"] == commit == proof["source_archive_commit"]
        assert terminal["source_archive_sha256"] == digest == proof["source_archive_sha256"]
        for name in TERMINAL_IDENTITIES:
            assert terminal[name] == proof[name], "terminal identity: " + name
        files = terminal["artifacts"]
        assert set(files) <= set(ARTIFACTS), "unexpected terminal body"
        for identity in files.values():
            assert type(identity) is dict and set(identity) == {"bytes", "sha256"}
            assert type(identity["bytes"]) is int and 0 <= identity["bytes"] <= MAX_BODY_BYTES
            assert re.fullmatch("[0-9a-f]{64}", identity["sha256"])
        assert sum(p["bytes"] for p in files.values()) <= 256 << 20, "bounded full collection"
        if terminal["status"] == "failed":
            assert validate_closed(out, proof, terminal, files) is False
            diagnostics = terminal.get("failure_diagnostics", {})
            assert type(diagnostics) is dict and set(diagnostics) <= set(DIAGNOSTIC_FILES)
            for name, diagnostic in diagnostics.items():
                assert name in files and type(diagnostic) is dict
                assert set(diagnostic) == {"offset", "bytes", "sha256", "body_base64"}
                assert type(diagnostic["offset"]) is type(diagnostic["bytes"]) is int
                assert 0 <= diagnostic["bytes"] <= DIAGNOSTIC_BYTES
                assert diagnostic["offset"] == max(0, files[name]["bytes"] - DIAGNOSTIC_BYTES)
                assert diagnostic["offset"] + diagnostic["bytes"] == files[name]["bytes"]
                assert type(diagnostic["body_base64"]) is str and len(diagnostic["body_base64"]) <= 4 * ((DIAGNOSTIC_BYTES + 2) // 3)
                body = base64.b64decode(diagnostic["body_base64"], validate=True)
                assert len(body) == diagnostic["bytes"] and sha(body) == diagnostic["sha256"], "terminal diagnostic authentication"
                if diagnostic["offset"] == 0:
                    assert dict(bytes=len(body), sha256=sha(body)) == files[name], "terminal diagnostic body identity"
            failure_context = dict(original_exit_code=terminal["original_exit_code"], exit_code=terminal["exit_code"],
                phase=terminal["phase"], failure_diagnostics=diagnostics, terminal_authority=dict(instance_id=instance_id,
                    config_sha256=proof["config_sha256"], source_commit=commit, source_archive_sha256=digest))
            write(out / "terminal-failure-diagnostics.json", failure_context)
        for name, identity in files.items():
            target = regular_path(out / name); target.parent.mkdir(parents=True, exist_ok=True)
            part = target.with_name(target.name + ".part"); part.unlink(missing_ok=True)
            count, hashed = 0, hashlib.sha256()
            try:
                with s3.get_object(Bucket=BUCKET, Key=prefix + "/artifacts/" + name)["Body"] as stream, part.open("xb") as body:
                    for chunk in iter(lambda: stream.read(LOCAL_BYTES), b""):
                        count += len(chunk); assert count <= identity["bytes"], "collection length overflow: " + name
                        hashed.update(chunk); body.write(chunk)
                    body.flush(); os.fsync(body.fileno())
                assert dict(bytes=count, sha256=hashed.hexdigest()) == identity, "collection body authentication: " + name
                if target.exists():
                    assert artifact(target) == identity, "local body drift"
                else:
                    os.rename(part, target); driver.sync_directory(target.parent)
            finally:
                part.unlink(missing_ok=True)
            receipts[name] = dict(identity, bucket=BUCKET, key=prefix + "/artifacts/" + name, full_body_stream_verified=True, local_body=True)
            write(out / "collection-progress.json", dict(complete=False, files=receipts))
        complete = validate_closed(out, proof, terminal, files)
        write(out / "collection-receipt.json", dict(schema=SCHEMA + "-collection", complete=complete,
            instance_id=instance_id, state="terminated", files=receipts, whole_body_verification=True,
            execution_status="SUCCESS" if complete else "FAIL", scientific_status=read_json(out / "screen/summary.json")["status"] if complete else "INVALID",
            config_sha256=proof["config_sha256"], source_commit=commit, source_archive_sha256=digest, **failure_context))
    except BaseException as error:
        failed = dict(error_type=type(error).__name__, error=str(error), authenticated_files=receipts,
            execution_status="FAIL", scientific_status="INVALID", **failure_context)
        write(out / "collection-error.json", failed)
        # shared.main writes its own error summary; this ledger must survive it.
        write(out / "collection-authenticated-failure.json", failed)
        raise
    return terminal


def main(attempt):
    assert re.fullmatch("a[0-9]{4}", attempt)
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        proof = preflight()
        assert proof["namespace_prefix"] == PREFIX + attempt + "/serving"
        with patch.dict(os.environ, AWS_MAX_ATTEMPTS="1"):
            stage_configs(proof)
            return lifecycle()[0].main(attempt, campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


def stage_configs_check():
    """Source-bound offline check of immutable reuse, races and closed failures."""
    import tempfile
    from botocore.exceptions import ClientError
    from unittest.mock import Mock

    shared, _ = lifecycle()
    def error(code, operation):
        return ClientError(dict(Error=dict(Code=code)), operation)
    class Stream(io.BytesIO):
        def read(self, size=-1):
            assert 0 < size <= LOCAL_BYTES + 1
            if self.failed:
                raise OSError("stream failed")
            return super().read(min(size, 7))
    class S3:
        def __init__(self, objects=None, mode="ok"):
            self.objects, self.mode = dict(objects or {}), mode
            self.puts, self.gets, self.streams, self.closed = [], [], [], False
        def get_object(self, Bucket, Key):
            assert Bucket == BUCKET
            self.gets.append(Key)
            if self.mode == "access":
                raise error("AccessDenied", "GetObject")
            if self.mode == "transport":
                raise OSError("transport failed")
            if Key not in self.objects or self.mode == "race-missing" or (self.mode.startswith("race") and len(self.gets) == 1):
                raise error("NoSuchKey", "GetObject")
            body = self.objects[Key]
            stream = Stream(body[:-1] if self.mode == "truncated" else body + b"x" if self.mode == "overflow" else body)
            stream.failed = self.mode == "stream-failure"
            self.streams.append(stream)
            return dict(Body=stream, ContentLength=len(body) + (self.mode == "length"))
        def put_object(self, Bucket, Key, Body, IfNoneMatch):
            assert Bucket == BUCKET and IfNoneMatch == "*"
            self.puts.append(Key)
            if self.mode == "put-failure":
                raise error("AccessDenied", "PutObject")
            if self.mode.startswith("race"):
                self.objects[Key] = Body if self.mode == "race-ok" else Body + b"tamper"
            if Key in self.objects:
                raise error("PreconditionFailed", "PutObject")
            self.objects[Key] = Body
        def close(self):
            self.closed = True
    with tempfile.TemporaryDirectory(prefix="fixed48-authority-check-") as tmp:
        paths = [Path(tmp) / n for n in ("config.json", "controller-config.json", "asset-manifest.json")]
        for i, path in enumerate(paths):
            write(path, dict(authority=i))
        proof = dict(authority_paths=list(map(str, paths)), **{field: artifact(path)["sha256"]
            for field, path in zip(("config_sha256", "controller_config_sha256", "asset_manifest_sha256"), paths)})
        def run(sdk, rejected=False, twice=False, local_failure=False):
            session = Mock(); session.client.return_value = sdk
            with patch.object(shared.boto3, "Session", return_value=session) as factory, \
                    patch.object(shared.peer, "put_if_absent", side_effect=lambda key, body:
                        sdk.put_object(Bucket=BUCKET, Key=key, Body=body, IfNoneMatch="*")):
                try:
                    stage_configs(proof)
                    if twice:
                        stage_configs(proof)
                except (AssertionError, ClientError, OSError):
                    assert rejected
                else:
                    assert not rejected
            assert sdk.closed is bool(factory.call_count) and all(stream.closed for stream in sdk.streams)
            assert factory.call_count == (0 if local_failure else 2 if twice else 1)
            if factory.call_count:
                factory.assert_called_with(profile_name="causality", region_name=REGION)
                options = session.client.call_args.kwargs["config"]
                assert options.retries["total_max_attempts"] == 1
                assert options.connect_timeout == options.read_timeout == 5
        initial = S3(); run(initial, twice=True)
        assert len(initial.objects) == len(initial.puts) == 4 and len(initial.gets) == 8
        for mode in ("tamper", "length", "truncated", "overflow", "stream-failure", "access", "transport", "put-failure", "race-ok", "race-tamper", "race-missing"):
            sdk = S3({} if mode == "put-failure" or mode.startswith("race") else initial.objects, mode)
            if mode == "tamper":
                key = config_key(proof["asset_manifest_sha256"]); sdk.objects[key] = b"x" * len(sdk.objects[key])
            run(sdk, rejected=mode != "race-ok")
            assert len(sdk.puts) == (4 if mode == "race-ok" else int(mode in ("put-failure", "race-tamper", "race-missing")))
            assert len(sdk.gets) == (8 if mode == "race-ok" else 4 if mode == "tamper" else 2 if mode.startswith("race") else 1)
        proof["config_sha256"] = "0" * 64
        drift = S3(initial.objects); run(drift, rejected=True, local_failure=True)
        assert not drift.gets and not drift.puts
        paths[0].write_bytes(b"x" * (LOCAL_BYTES + 1))
        run(S3(), rejected=True, local_failure=True)
    return dict(passed=True, source_sha256=artifact(Path(__file__))["sha256"], scenarios=14,
        native_or_cloud_execution=False)


def self_check():
    """Bounded real metadata admission, generated shell and mocked lifecycle."""
    import copy
    from contextlib import ExitStack, redirect_stdout
    from datetime import datetime, timezone
    import resource
    import tempfile
    from types import SimpleNamespace
    from unittest.mock import Mock

    resource.setrlimit(resource.RLIMIT_AS, (256 << 20, 256 << 20))
    signal.alarm(55)
    started, checks = time.monotonic(), 0
    module, repo = sys.modules[__name__], Path(__file__).resolve().parents[1]
    def rejected(call):
        nonlocal checks
        try:
            call()
        except (AssertionError, ValueError, OSError, RuntimeError, subprocess.CalledProcessError):
            checks += 1
            return
        raise AssertionError("negative check admitted")

    proofs = {n: dict(path=str(p), **artifact(repo / p)) for n, p in runtime.PROOF_PATHS.items()}
    original_proofs = runtime.read_proofs(repo, proofs)
    assets, historical = runtime.historical_assets(repo, original_proofs)
    verified = root_verification(repo)
    assert len(assets) == 38 and len(proofs) == 20 and len(CODE) == 80
    assert set(runtime.CODE) < set(CODE) and len(RUNTIME_FILES) == 18 and len(ARTIFACTS) == 56
    assert historical["scientific_decision"]["scientific_status"] == "GO"
    checks += 1
    with tempfile.TemporaryDirectory(prefix="fixed48-cold-controller-check-") as tmp, patch.object(lifecycle()[0].boto3, "Session", side_effect=AssertionError("cloud forbidden")) as forbidden:
        work = Path(tmp)
        authority = work / "authority"; authority.mkdir()
        path, control_path, manifest_path = (authority / n for n in ("config.json", "controller-config.json", "asset-manifest.json"))
        source = dict(commit="a" * 40, archive_sha256="b" * 64)
        manifest = dict(schema="borsuk-fixed48-retained-cold-assets-v1", authority_pending=False, assets=assets)
        write(manifest_path, manifest)
        config = dict(runtime.FIXED, bucket=BUCKET, namespace_prefix=PREFIX + "a0001/serving", execution_source=source,
            code_sha256={n: artifact(repo / n)["sha256"] for n in runtime.CODE}, proofs=proofs,
            asset_manifest=dict(path=str(MANIFEST), **artifact(manifest_path)), resources=dict(runtime.HOST,
                publication_limit_seconds=900, cold_limit_seconds=900, service_limit_seconds=SERVICE_SECONDS, output_reserve_bytes=64 << 20))
        write(path, config)
        control = dict(schema=SCHEMA, authority_pending=False, execution_source=source,
            code_sha256={n: artifact(repo / n)["sha256"] for n in CODE}, runtime_config=dict(path=str(CONFIG), **artifact(path)))
        write(control_path, control)
        real_read_repo, real_read_json, real_artifact = science.read_repo, read_json, artifact
        def local_json(name):
            return real_read_json(control_path if Path(name) == repo / CONTROL else name)
        def local_pointer(base, pointer, expected=None):
            selected = {str(CONFIG): path, str(MANIFEST): manifest_path}.get(pointer["path"])
            if selected is None:
                return real_read_repo(base, pointer, expected)
            assert set(pointer) == {"path", "bytes", "sha256"} and (expected is None or pointer["path"] == str(expected))
            assert artifact(selected) == {k: pointer[k] for k in ("bytes", "sha256")}
            return selected.read_bytes()
        def local_artifact(name):
            return real_artifact(control_path if Path(name) == repo / CONTROL else name)
        with patch.object(module, "read_json", side_effect=local_json), patch.object(module, "artifact", side_effect=local_artifact), patch.object(science, "read_repo", side_effect=local_pointer):
            proof = qualify(repo, path)
            assert proof["root_verification"] == verified and proof["assets"] == assets
            checks += 1
            for name, value in (("authority_pending", True), ("code_sha256", {}), ("execution_source", {})):
                write(control_path, dict(control, **{name: value})); rejected(lambda: qualify(repo, path))
            write(control_path, control)
            rejected(lambda: qualify(repo, path, "0" * 64))
            for name, value in (("authority_pending", True), ("namespace_prefix", "foreign/serving"), ("code_sha256", {})):
                write(path, dict(config, **{name: value}))
                write(control_path, dict(control, runtime_config=dict(path=str(CONFIG), **artifact(path))))
                rejected(lambda: qualify(repo, path))
            write(path, config); write(control_path, control)
            changed = copy.deepcopy(manifest); changed["assets"]["sq8.bin"]["source"]["key"] += "-tampered"
            write(manifest_path, changed)
            altered = dict(config, asset_manifest=dict(path=str(MANIFEST), **artifact(manifest_path)))
            write(path, altered); write(control_path, dict(control, runtime_config=dict(path=str(CONFIG), **artifact(path))))
            rejected(lambda: qualify(repo, path))
            write(manifest_path, manifest); write(path, config); write(control_path, control)
            # Rebound proof SHA still cannot change completed scientific semantics.
            bad_proofs = dict(original_proofs)
            changed = json.loads(bad_proofs["historical_validation"]); changed["scientific_status"] = "FAIL"
            bad_proofs["historical_validation"] = encoded(changed)
            rejected(lambda: runtime.historical_assets(repo, bad_proofs))
            with patch.object(driver.qualification.worker, "source_hashes", return_value={}):
                rejected(lambda: qualify(repo, path))

        # Actual Git checks source-before-config, clean state and origin backing.
        git_repo = work / "git"; git_repo.mkdir()
        def git(*args):
            return subprocess.check_output(["git", *args], cwd=git_repo, stderr=subprocess.PIPE)
        git("init", "-q"); (git_repo / "source.py").write_bytes(b"frozen\n")
        git("add", "."); git("-c", "core.hooksPath=/dev/null", "commit", "-qm", "Synthetic source")
        revision = git("rev-parse", "HEAD").decode().strip()
        for n in proof["authority_paths"]:
            target = git_repo / n; target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(b"frozen authority\n")
        git("add", "."); git("-c", "core.hooksPath=/dev/null", "commit", "-qm", "Synthetic authority")
        git("update-ref", "refs/remotes/origin/main", git("rev-parse", "HEAD").decode().strip())
        git_proof = dict(proof, source_archive_commit=revision, source_archive_sha256=archive_digest(revision, git_repo))
        with patch.object(module, "CODE", ("source.py",)), patch.object(module, "qualify", return_value=git_proof):
            assert preflight(git_repo) == git_proof
            own = git_repo / ROOT / "a0001"; own.mkdir(); (own / "receipt").write_bytes(b"owned\n")
            assert preflight(git_repo, own) == git_proof
            rejected(lambda: preflight(git_repo))
            (git_repo / "foreign").write_bytes(b"foreign"); rejected(lambda: preflight(git_repo, own)); (git_repo / "foreign").unlink()
            (git_repo / "source.py").write_bytes(b"changed\n"); rejected(lambda: preflight(git_repo, own)); (git_repo / "source.py").write_bytes(b"frozen\n")
            with patch.object(module, "archive_digest", return_value="0" * 64):
                rejected(lambda: preflight(git_repo, own))
            with patch.object(module, "qualify", return_value=dict(git_proof, source_archive_commit=git("rev-parse", "HEAD").decode().strip())):
                rejected(lambda: preflight(git_repo, own))
            git("update-ref", "refs/remotes/origin/main", revision)
            rejected(lambda: preflight(git_repo, own))
        checks += 1

        # Generate from the actual shared bootstrap; execute its terminal Python
        # and scoped upload shell with local stubs, including an upload failure.
        frozen = work / "frozen"; (frozen / ROOT).mkdir(parents=True)
        (frozen / CONFIG).write_bytes(path.read_bytes())
        previous_cwd = Path.cwd(); os.chdir(frozen)
        try:
            body = user_data(source["commit"], source["archive_sha256"], "source/frozen.tar.gz", PREFIX + "a0001", proof)
        finally:
            os.chdir(previous_cwd)
        user_data_bytes = len(body.encode())
        assert all(s in body for s in ("--on-active=3000s", "MemoryMax=12884901888", "MemorySwapMax=0", "CPUQuota=200%", "TasksMax=512", "RuntimeMaxSec=2400", "taskset -c 4-5"))
        assert all("--setenv=" + n + "=2" in body for n in runtime.retained.THREAD_ENV)
        assert "--setenv=BORSUK_COLD_SOURCE_COMMIT=" + source["commit"] in body
        finish = body[body.index("finish() {\n"):body.index("trap finish EXIT\n")]
        for mode in ("success", "scientific-fail", "runtime-failed", "upload-failed", "runtime-upload-failed"):
            dest = work / mode; dest.mkdir()
            for n in ARTIFACTS:
                target = dest / n; target.parent.mkdir(parents=True, exist_ok=True); write(target, b"closed fixture\n")
            write(dest / "source-qualification.json", proof); write(dest / "run.log", b"closed log\n")
            runtime_failed = mode in ("runtime-failed", "runtime-upload-failed")
            write(dest / "failure.json", dict(status="failed" if runtime_failed else "complete",
                error_type="RuntimeError", error="synthetic prepublication failure"))
            write(dest / "screen/summary.json", dict(status="EXECUTION_FAILED", error="synthetic runtime cause"))
            write(dest / "profile.log", b"prefix" + b"x" * 8192 + b"runtime log tail\n")
            cli = dest / "bin/aws"; cli.parent.mkdir()
            cli.write_text('''#!/bin/bash
root="${0%/bin/aws}"
test -f "$3" || exit 44
printf '%s\\n' "$4" >> "$root/uploads"
if [[ "$MODE" = *upload-failed ]] && [[ "$4" = */artifacts/* ]]; then exit 55; fi
''')
            cli.chmod(0o700)
            stub = '''curl() { case "$*" in */api/token*) echo token;; *) echo i-owned;; esac; }
shutdown() { :; }
systemd-run() { while [ "$1" != timeout ]; do shift; done; (cd /; env -i PATH=/usr/bin:/bin MODE="$MODE" "$@"); }
'''
            script = (stub + "root=" + shlex.quote(str(dest)) + "; phase=" + ("cold" if runtime_failed else "complete") +
                "; export MODE=" + mode + "; export ARTIFACT_NAMES=" + shlex.quote(" ".join(ARTIFACTS)) + '; cd "$root"\n' +
                finish.replace("/dev/ttyS0", str(dest / "serial")) + "\n(exit " + ("7" if runtime_failed else "0") + "); finish\n")
            result = subprocess.run(["bash", "-c", script], capture_output=True, cwd="/", timeout=10,
                env=dict(os.environ, PATH=str(cli.parent) + ":/usr/bin:/bin"))
            terminal = read_json(dest / "terminal.json")
            assert terminal["exit_code"] == result.returncode == {"runtime-failed": 7, "upload-failed": 96, "runtime-upload-failed": 96}.get(mode, 0), (mode, result.stderr)
            assert terminal["original_exit_code"] == (7 if runtime_failed else 0)
            uploads = (dest / "uploads").read_text().splitlines()
            assert bool([n for n in uploads if n.endswith("/screen/COMPLETE.json")]) == (mode in ("success", "scientific-fail"))
            for key in TERMINAL_IDENTITIES:
                assert terminal[key] == proof[key]
            if terminal["exit_code"]:
                diagnostics = terminal["failure_diagnostics"]
                assert base64.b64decode(diagnostics["failure.json"]["body_base64"]) == (dest / "failure.json").read_bytes()
                assert base64.b64decode(diagnostics["screen/summary.json"]["body_base64"]) == (dest / "screen/summary.json").read_bytes()
                assert base64.b64decode(diagnostics["profile.log"]["body_base64"]).endswith(b"runtime log tail\n")
                assert diagnostics["profile.log"]["bytes"] == 4096
            if mode == "runtime-upload-failed":
                shell_failure = terminal
            checks += 1

        # Shared.main remains the sole owner of ACKs, fsync and same-ID teardown.
        shared, _ = lifecycle()
        for mode in ("success", "fsync", "multi-ack", "multi-ack-fsync", "interrupt", "interruption", "wait"):
            ec2, s3, session = Mock(), Mock(), Mock(); session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {"Reservations": []}
            ec2.describe_subnets.return_value = {"Subnets": [{"AvailabilityZone": "synthetic-az"}]}
            ec2.describe_spot_price_history.return_value = {"SpotPriceHistory": [{"SpotPrice": "0.1", "Timestamp": datetime.now(timezone.utc)}]}
            ids = ["i-owned", "i-extra"] if mode.startswith("multi-ack") else ["i-owned"]
            ec2.run_instances.return_value = {"Instances": [{"InstanceId": n} for n in ids]}
            events = []
            ec2.terminate_instances.side_effect = lambda **kwargs: events.append("terminate")
            def waited(**kwargs):
                events.append("wait")
                if mode == "wait":
                    raise RuntimeError("wait failed")
            ec2.get_waiter.return_value.wait.side_effect = waited
            def collected(*args):
                assert events == ["terminate", "wait"]; events.append("collect")
                return dict(status="complete", phase="complete", exit_code=0, artifacts=dict.fromkeys(ARTIFACTS))
            launch_proof = dict(proof, source_archive_sha256=sha(gzip.compress(b"synthetic archive", mtime=0)))
            with patch.object(module, "ROOT", work / ("launch-" + mode)), patch.object(module, "preflight", return_value=launch_proof), \
                    patch.object(module, "stage_configs"), patch.object(module, "user_data", return_value="synthetic"), \
                    patch.object(shared.boto3, "Session", return_value=session), \
                    patch.object(subprocess, "check_output", side_effect=["", "a" * 40, b"synthetic archive"]), patch.object(subprocess, "run"), \
                    patch.object(shared.peer, "missing", return_value=True), patch.object(shared.peer, "put_if_absent"), \
                    patch.object(module, "poll", side_effect={"interrupt": KeyboardInterrupt(), "interruption": RuntimeError("Spot interruption")}.get(mode)), \
                    patch.object(module, "collect", side_effect=collected) as collector, \
                    patch.object(os, "fsync", side_effect=OSError("fsync") if mode.endswith("fsync") else None), redirect_stdout(io.StringIO()):
                try:
                    main("a0001")
                except (OSError, RuntimeError, KeyboardInterrupt):
                    assert mode not in ("success", "multi-ack")
                else:
                    assert mode in ("success", "multi-ack")
            ec2.run_instances.assert_called_once()
            ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            if mode == "wait":
                collector.assert_not_called()
            else:
                assert events == ["terminate", "wait", "collect"]
            call = ec2.run_instances.call_args.kwargs
            assert call["ImageId"] == IMAGE_ID and call["InstanceType"] == "c7i.2xlarge"
            assert call["BlockDeviceMappings"] == [dict(DeviceName="/dev/sda1", Ebs=dict(DeleteOnTermination=True, Encrypted=True, VolumeSize=80, VolumeType="gp3"))]
            assert call["InstanceMarketOptions"]["SpotOptions"]["MaxPrice"] == "0.50"
            reservation = read_json(work / ("launch-" + mode) / "a0001/aws-reservation.json")
            assert reservation["wall_seconds"] == WALL and reservation["compute_cap_usd"] == .50 and reservation["ebs_s3_allowance_usd"] == .15
        checks += 1

        # Execute the shared source stream/admission code against a tiny tar.
        import tarfile
        tar = io.BytesIO()
        with tarfile.open(fileobj=tar, mode="w") as archive:
            member = tarfile.TarInfo("scripts/source.py"); member.size = 5
            archive.addfile(member, io.BytesIO(b"code\n"))
        archive_body = gzip.compress(tar.getvalue(), mtime=0)
        class SourceS3:
            def get_object(self, **kwargs):
                return dict(Body=io.BytesIO(archive_body))
        for mode in ("source-ok", "source-tamper", "source-scratch"):
            destination = work / mode; destination.mkdir()
            arguments = ["-", BUCKET, "source", "0" * 64 if mode == "source-tamper" else sha(archive_body), str(1 if mode == "source-scratch" else SCRATCH)]
            oldcwd = Path.cwd(); os.chdir(destination)
            try:
                with patch.object(shared.boto3, "client", return_value=SourceS3()), patch.object(sys, "argv", arguments):
                    if mode == "source-ok":
                        exec(compile(science.BOOTSTRAP_STAGE, "<shared-source-stage>", "exec"), {})
                        assert read_json(destination / "bootstrap-staging.json")["source_authenticated"] is True
                        checks += 1
                    else:
                        rejected(lambda: exec(compile(science.BOOTSTRAP_STAGE, "<shared-source-stage>", "exec"), {}))
            finally:
                os.chdir(oldcwd)

        # The existing ABI helper sees authenticated hardlinks and mocked ldd.
        abi_out = work / "abi-test"; abi_out.mkdir()
        native_assets = abi_out / "assets"
        for name in ("two_bit_http", "two_bit_plan_demo"):
            target = native_assets / "qualification/binaries" / name
            target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes((name + " fixture").encode())
        _, bootstrap = lifecycle()
        with patch.object(bootstrap.platform, "freedesktop_os_release", return_value=dict(ID="ubuntu", VERSION_ID="24.04")), \
                patch.object(bootstrap.platform, "machine", return_value="x86_64"), patch.object(os, "confstr", return_value="glibc 2.39"), \
                patch.object(bootstrap, "_required_glibc", return_value="2.35"), \
                patch.object(subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout="libc.so.6 => /lib/libc.so.6", stderr="")):
            assert native_abi(abi_out, native_assets)["qualified"] is True
            assert not (abi_out / "abi").exists()
            with patch.object(bootstrap, "_required_glibc", return_value="2.40"):
                rejected(lambda: native_abi(abi_out, native_assets))
            with patch.object(subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout="libc.so.6 => not found", stderr="")):
                rejected(lambda: native_abi(abi_out, native_assets))
        checks += 1

        # Capture the existing runtime's closed synthetic success fixture. Its
        # own SDK/native process seams remain mocked; no live vectors or native.
        fixture, fixture_repo = work / "runtime", work / "runtime-repo"
        fixture_repo.mkdir()
        captured = {}
        original_run = runtime.run
        def capture(*args):
            result = original_run(*args)
            if result["status"] == "PASS":
                shutil.copytree(args[3], fixture)
                panel = fixture_repo / runtime.SCIENTIFIC / "screen"; panel.mkdir(parents=True)
                for name in ("requests.jsonl", "records.jsonl", "truth.i64"):
                    shutil.copyfile(Path(args[0]).parent / "fixture/panel" / name, panel / name)
                captured["assets"] = read_json(fixture / "input-hashes.json")
            return result
        with patch.object(runtime, "run", side_effect=capture), redirect_stdout(io.StringIO()):
            runtime.self_check()
        fixture_config = read_json(fixture / "config.json")
        fixture_proof = dict(proof, config_sha256=artifact(fixture / "config.json")["sha256"], assets=captured["assets"])
        assert offline_reduce(fixture, fixture_config, fixture_proof, fixture_repo)["status"] == "PASS"
        checks += 1
        original_record = (fixture / "records.jsonl").read_bytes()
        tampered = [json.loads(n) for n in original_record.splitlines()]; tampered[0]["returned_hits"] = 0
        write(fixture / "records.jsonl", b"".join(encoded(n) + b"\n" for n in tampered))
        marker = read_json(fixture / "COMPLETE.json"); marker["files"]["records.jsonl"] = artifact(fixture / "records.jsonl")
        write(fixture / "COMPLETE.json", marker)
        rejected(lambda: offline_reduce(fixture, fixture_config, fixture_proof, fixture_repo))
        write(fixture / "records.jsonl", original_record); marker["files"]["records.jsonl"] = artifact(fixture / "records.jsonl"); write(fixture / "COMPLETE.json", marker)

        # Run the controller's real worker orchestration around a mocked runtime.
        # SDK events, small-body streams, cgroup closure and failure ledgers are real.
        class Events:
            def __init__(self):
                self.handlers = {}
            def register(self, event, callback, unique_id):
                self.handlers[event, unique_id] = callback
            def unregister(self, event, unique_id):
                self.handlers.pop((event, unique_id))
            def emit(self, event, **kwargs):
                for (name, unused), callback in list(self.handlers.items()):
                    if name == event:
                        callback(**kwargs)
        class WorkerSDK:
            def __init__(self, objects):
                self.objects, self.closed = objects, False
                self.meta = SimpleNamespace(events=Events())
            def response(self, **kwargs):
                self.meta.events.emit("before-send.s3")
                self.meta.events.emit("needs-retry.s3", response=(SimpleNamespace(status_code=200), {}))
                return dict(ResponseMetadata=dict(HTTPStatusCode=200, RetryAttempts=0), **kwargs)
            def head_object(self, Bucket, Key):
                return self.response(ContentLength=len(self.objects[Key]))
            def get_object(self, Bucket, Key):
                return self.response(Body=io.BytesIO(self.objects[Key]), ContentLength=len(self.objects[Key]))
            def close(self):
                self.closed = True
        cgroup = read_json(fixture / "resources.json")["cgroup"]["before"]
        for mode in ("stage-ok", "stage-runtime-failed", "stage-body-tamper", "stage-scratch"):
            stage_out = work / mode; stage_out.mkdir(); stage_repo = stage_out / "repo"; stage_repo.mkdir()
            selected = dict(fixture_config, namespace_prefix=PREFIX + "a0001/serving")
            selected_body = encoded(selected) + b"\n"
            control_body, manifest_body = control_path.read_bytes(), manifest_path.read_bytes()
            stage_proof = dict(proof, config_sha256=sha(selected_body), controller_config_sha256=sha(control_body),
                asset_manifest_sha256=sha(manifest_body), assets=captured["assets"])
            objects = {config_key(sha(b)): b for b in (encoded(stage_proof), selected_body, control_body, manifest_body)}
            if mode == "stage-body-tamper":
                objects[config_key(sha(selected_body))] = selected_body + b"tamper"
            sdk = WorkerSDK(objects)
            write(stage_out / "source-qualification.json", dict(**{k: stage_proof[k] for k in (*TERMINAL_IDENTITIES, "source_archive_commit", "source_archive_sha256")}, proof_sha256=sha(encoded(stage_proof))))
            def mock_runtime(config_path, digest, repo_path, output):
                assert artifact(config_path)["sha256"] == digest
                assert repo_path == stage_repo and output == stage_out / "screen"
                output.mkdir()
                native = output / "scratch/assets/qualification"
                for name in NATIVE_FILES:
                    target = native / name; target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(("qualified fixture " + name).encode())
                runtime.publish_generation(None, selected, output / "scratch", output, None, None, None, time.monotonic() + 1)
                shutil.rmtree(output / "scratch")
                if mode == "stage-runtime-failed":
                    raise RuntimeError("synthetic runtime failure")
                return dict(status="FAIL")
            with ExitStack() as stack:
                stack.enter_context(patch.object(module, "WORK_ROOT", stage_out))
                stack.enter_context(patch.object(module, "qualify", return_value=stage_proof))
                stack.enter_context(patch.object(module, "native_abi", return_value=dict(qualified=True)))
                stack.enter_context(patch.object(runtime, "snapshot", return_value=cgroup))
                stack.enter_context(patch.object(runtime, "publish_generation", return_value={}))
                runner = stack.enter_context(patch.object(runtime, "run", side_effect=mock_runtime))
                stack.enter_context(patch.object(module, "offline_reduce", return_value=dict(status="FAIL")))
                stack.enter_context(patch.object(shared.boto3, "client", return_value=sdk))
                stack.enter_context(patch.dict(os.environ, dict.fromkeys(runtime.retained.THREAD_ENV, "2") |
                    dict(AWS_MAX_ATTEMPTS="1", BORSUK_COLD_SOURCE_COMMIT=source["commit"], BORSUK_COLD_ARCHIVE_SHA256=source["archive_sha256"])))
                stack.enter_context(patch.object(bootstrap.platform, "freedesktop_os_release", return_value=dict(ID="ubuntu", VERSION_ID="24.04")))
                if mode == "stage-scratch":
                    stack.enter_context(patch.object(module, "SCRATCH", 1))
                closed = stage(stage_repo, stage_out, PREFIX + "a0001")
            assert sdk.closed and not sdk.meta.events.handlers
            assert closed["closed"] is (mode == "stage-ok")
            if mode == "stage-ok":
                assert closed["execution_status"] == "SUCCESS" and closed["scientific_status"] == "FAIL"
                runner.assert_called_once()
                assert len([p for p in (stage_out / "native").rglob("*") if p.is_file()]) == len(NATIVE_FILES)
            else:
                assert read_json(stage_out / "failure.json")["status"] == "failed"
            assert not (stage_out / "screen/scratch").exists()
        checks += 1

        # Stream-authenticated terminal collection and partial failure ledger.
        class Stream(io.BytesIO):
            def read(self, size=-1):
                assert 0 < size <= LOCAL_BYTES + 1
                return super().read(size)
        class S3:
            def __init__(self, bodies):
                self.bodies, self.calls = bodies, []
            def get_object(self, **kwargs):
                assert kwargs["Bucket"] == BUCKET
                self.calls.append(kwargs["Key"])
                return dict(Body=Stream(self.bodies[kwargs["Key"]]))
        dest = work / "collection/a0001"; dest.mkdir(parents=True)
        bodies = {"screen/" + n: (fixture / n).read_bytes() for n in (*RUNTIME_FILES, "COMPLETE.json")}
        for n in (*CONTROLLER_FILES, *("native/" + n for n in NATIVE_FILES)):
            bodies[n] = b"closed synthetic body\n"
        proof = dict(fixture_proof)
        collected_config = dict(fixture_config, execution_source=source)
        bodies["config.json"] = bodies["screen/config.json"] = encoded(collected_config) + b"\n"
        proof["config_sha256"] = sha(bodies["config.json"])
        bodies["controller-config.json"] = encoded(control) + b"\n"
        proof["controller_config_sha256"] = sha(bodies["controller-config.json"])
        bodies["asset-manifest.json"] = encoded(dict(manifest, assets=proof["assets"])) + b"\n"
        proof["asset_manifest_sha256"] = sha(bodies["asset-manifest.json"])
        bodies["source-qualification.json"] = encoded(proof) + b"\n"
        source_qualification = json.loads(bodies["screen/source-qualification.json"])
        source_qualification["execution_source"] = source
        bodies["screen/source-qualification.json"] = encoded(source_qualification) + b"\n"
        for name in NATIVE_FILES:
            bodies["native/" + name] = ("qualified fixture " + name).encode()
        proof["binary_sha256"] = sha(bodies["native/binaries/two_bit_http"])
        proof["publisher_sha256"] = sha(bodies["native/binaries/two_bit_plan_demo"])
        bodies["source-qualification.json"] = encoded(proof) + b"\n"
        abi = read_json(abi_out / "runtime-abi.json")
        abi.update(qualified=True, required_glibc=dict.fromkeys(("two_bit_http", "two_bit_plan_demo"), "2.35"),
            binaries={n: library.identity(proof["assets"]["qualification/binaries/" + n]) for n in ("two_bit_http", "two_bit_plan_demo")},
            ldd={n: dict(returncode=0, stdout="libc.so.6 => /lib/libc.so.6", stderr="") for n in ("two_bit_http", "two_bit_plan_demo")})
        bodies["runtime-abi.json"] = encoded(abi) + b"\n"
        bodies["bootstrap-staging.json"] = encoded(dict(source_authenticated=True, source_archive_sha256=source["archive_sha256"],
            scratch_limit_bytes=SCRATCH, scratch_before_extract_bytes=1, source_repository_reserve_bytes=1)) + b"\n"
        bodies["cold-closure.json"] = encoded(dict(closed=True, process_cleanup=True, replay_passed=True, runtime_invocations=1,
            build_invocations=0, scientific_scorer_invocations=0, oracle_invocations=0, execution_status="SUCCESS", scientific_status="PASS", wall_seconds=1)) + b"\n"
        bodies["profile-cgroup.json"] = encoded(dict(before=cgroup, after=cgroup, closed=True)) + b"\n"
        bodies["staging.json"] = encoded(dict(closed=True, shared_hardlinks_accounted=True, peak_scratch_bytes=1,
            scratch_usage=dict(unique_inode_bytes=1, physical_allocated_bytes=1))) + b"\n"
        bodies["tool-versions.json"] = encoded(dict(architecture="x86_64", python="3.12.3", os_release=dict(ID="ubuntu", VERSION_ID="24.04"),
            thread_environment=dict.fromkeys(runtime.retained.THREAD_ENV, "2"), aws_max_attempts=1)) + b"\n"
        bodies["profile-resources.txt"] = b"Maximum resident set size (kbytes): 1\nExit status: 0\n"
        bodies["failure.json"] = encoded(dict(status="complete")) + b"\n"
        bodies["controller-sdk-ledger.jsonl"] = b"".join(encoded(dict(operation=operation,
            sdk_http_dispatch_attempts=1, retry_attempts=0, error=None)) + b"\n" for operation in ["head_object", "get_object"] * 4)
        marker = json.loads(bodies["screen/COMPLETE.json"])
        marker.update(config_sha256=proof["config_sha256"], files={n: dict(bytes=len(bodies["screen/" + n]), sha256=sha(bodies["screen/" + n])) for n in RUNTIME_FILES})
        bodies["screen/COMPLETE.json"] = encoded(marker) + b"\n"
        bodies["profile.log"] = b"x" * (LOCAL_BYTES + 7)
        terminal = dict(schema=SCHEMA, status="complete", phase="complete", exit_code=0, original_exit_code=0,
            instance_id="i-owned", source_commit=source["commit"], source_archive_sha256=source["archive_sha256"],
            **{n: proof[n] for n in TERMINAL_IDENTITIES}, artifacts={n: dict(bytes=len(b), sha256=sha(b)) for n, b in bodies.items()})
        launch = dict(instance_id="i-owned", nodes={"0": dict(instance_id="i-owned")}, prefix=PREFIX + "a0001",
            source_commit=source["commit"], source_archive_sha256=source["archive_sha256"])
        write(dest / "aws-launch.json", launch); write(dest / "aws-closeout.json", dict(state="terminated", nodes=launch["nodes"]))
        write(dest / "aws-reservation.json", dict(schema=SCHEMA, qualification=proof, config_sha256=proof["config_sha256"],
            source_commit=source["commit"], source_archive_sha256=source["archive_sha256"]))
        cloud = {PREFIX + "a0001/artifacts/" + n: b for n, b in bodies.items()}
        cloud[PREFIX + "a0001/terminal.json"] = encoded(terminal)
        transport = S3(cloud)
        oldcwd = Path.cwd(); os.chdir(fixture_repo)
        with patch.object(module, "ROOT", dest.parent), patch.object(module, "preflight", return_value=proof), \
                patch.object(driver, "native_authority", return_value=dict(native_qualification_passed=True, source_identity_sha256=driver.SOURCE_ID)):
            assert collect(transport, PREFIX + "a0001", dest, "i-owned", source["commit"], source["archive_sha256"]) == terminal
            assert read_json(dest / "collection-receipt.json")["complete"] is True
            assert len(read_json(dest / "collection-receipt.json")["files"]) == len(ARTIFACTS)
            versions_body = (dest / "tool-versions.json").read_bytes()
            changed = json.loads(versions_body); changed["thread_environment"].pop("BLIS_NUM_THREADS")
            write(dest / "tool-versions.json", changed)
            rejected(lambda: validate_closed(dest, proof, terminal, terminal["artifacts"]))
            write(dest / "tool-versions.json", versions_body)
            changed = copy.deepcopy(terminal); changed["artifacts"].pop("screen/scientific-reference.json")
            rejected(lambda: validate_closed(dest, proof, changed, changed["artifacts"]))
            write(dest / "aws-closeout.json", dict(state="running", nodes=launch["nodes"])); transport.calls.clear()
            rejected(lambda: collect(transport, PREFIX + "a0001", dest, "i-owned", source["commit"], source["archive_sha256"])); assert not transport.calls
            write(dest / "aws-closeout.json", dict(state="terminated", nodes=launch["nodes"]))
            cloud[PREFIX + "a0001/artifacts/profile.log"] += b"tamper"
            rejected(lambda: collect(transport, PREFIX + "a0001", dest, "i-owned", source["commit"], source["archive_sha256"]))
            assert read_json(dest / "collection-error.json")["authenticated_files"]
            cloud[PREFIX + "a0001/artifacts/profile.log"] = bodies["profile.log"]
            changed = dict(terminal, code_identity_sha256="0" * 64); cloud[PREFIX + "a0001/terminal.json"] = encoded(changed)
            rejected(lambda: collect(transport, PREFIX + "a0001", dest, "i-owned", source["commit"], source["archive_sha256"]))
            assert read_json(dest / "collection-authenticated-failure.json")["execution_status"] == "FAIL"
        failed = dict(terminal, status="failed", phase="cold", exit_code=7, original_exit_code=7, artifacts={"cpu.txt": terminal["artifacts"]["cpu.txt"]})
        cloud[PREFIX + "a0001/terminal.json"] = encoded(failed)
        with patch.object(module, "ROOT", dest.parent), patch.object(module, "preflight", return_value=proof):
            assert collect(transport, PREFIX + "a0001", dest, "i-owned", source["commit"], source["archive_sha256"])["exit_code"] == 7
            assert read_json(dest / "collection-receipt.json")["complete"] is False
            assert read_json(dest / "collection-receipt.json")["execution_status"] == "FAIL"
            assert read_json(dest / "collection-receipt.json")["scientific_status"] == "INVALID"
            # The original runtime error remains available even if every S3
            # artifact upload failed. Use the real generated-shell diagnostics.
            failed = dict(terminal, status="failed", phase="cold", exit_code=96, original_exit_code=7,
                failure_diagnostics=shell_failure["failure_diagnostics"],
                artifacts={n: shell_failure["artifacts"][n] for n in shell_failure["failure_diagnostics"]})
            missing = S3({PREFIX + "a0001/terminal.json": encoded(failed)})
            try:
                collect(missing, PREFIX + "a0001", dest, "i-owned", source["commit"], source["archive_sha256"])
            except KeyError:
                pass
            else:
                raise AssertionError("absent artifact admitted")
            authenticated = read_json(dest / "collection-authenticated-failure.json")
            assert authenticated["execution_status"] == "FAIL" and authenticated["scientific_status"] == "INVALID"
            assert authenticated["original_exit_code"] == 7 and authenticated["exit_code"] == 96
            assert authenticated["failure_diagnostics"] == shell_failure["failure_diagnostics"]
            assert b"synthetic prepublication failure" in base64.b64decode(authenticated["failure_diagnostics"]["failure.json"]["body_base64"])
            assert read_json(dest / "terminal-failure-diagnostics.json")["failure_diagnostics"] == authenticated["failure_diagnostics"]
            for kind in ("hash", "length", "identity"):
                altered = copy.deepcopy(failed)
                diagnostic = altered["failure_diagnostics"]["failure.json"]
                if kind == "hash":
                    diagnostic["sha256"] = "0" * 64
                elif kind == "length":
                    diagnostic["bytes"] = DIAGNOSTIC_BYTES + 1
                else:
                    altered["artifacts"]["failure.json"]["sha256"] = "0" * 64
                bad = S3({PREFIX + "a0001/terminal.json": encoded(altered)})
                rejected(lambda: collect(bad, PREFIX + "a0001", dest, "i-owned", source["commit"], source["archive_sha256"]))
                assert bad.calls == [PREFIX + "a0001/terminal.json"]
        os.chdir(oldcwd)
        checks += 1
        forbidden.assert_not_called()
    assert root_verification(repo) == verified and runtime.read_proofs(repo, proofs) == original_proofs
    signal.alarm(0)
    assert time.monotonic() - started < 55 and "numpy" not in sys.modules and "pyarrow" not in sys.modules
    return dict(schema=SCHEMA + "-self-check", passed=True, checks=checks, controller_code_paths=len(CODE),
        runtime_code_paths=len(runtime.CODE), proof_count=len(proofs), asset_count=len(assets), runtime_bodies=len(RUNTIME_FILES),
        artifacts=len(ARTIFACTS), user_data_bytes=user_data_bytes, elapsed_seconds=time.monotonic() - started,
        peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, synthetic=True,
        native_or_cloud_execution=False, cold_http_measured=False, launch_authorized=False)


if __name__ == "__main__":
    try:
        if sys.argv[1:] == ["--self-check"]:
            print(json.dumps(self_check(), sort_keys=True))
        elif sys.argv[1:] == ["--stage-configs-check"]:
            print(json.dumps(stage_configs_check(), sort_keys=True))
        elif sys.argv[1:2] == ["--stage"]:
            assert len(sys.argv) == 5, "--stage REPO OUTPUT PREFIX"
            sys.exit(0 if stage(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4])["closed"] else 2)
        else:
            assert len(sys.argv) == 2, "usage: aNNNN | --self-check"
            with open("/tmp/borsuk-fixed48-cold-http-spot-launch.lock", "a+") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                main(sys.argv[1])
    except (Exception, KeyboardInterrupt) as error:
        print(type(error).__name__ + ": " + str(error), file=sys.stderr)
        sys.exit(2)
