#!/usr/bin/env python3
"""One fixed48 campaign: aNNNN | --stage REPO OUTPUT PREFIX | --self-check.

Root freezes source BEFORE the separate campaign/driver/asset JSON authorities.
Original retained/native bodies are authenticated transports, never rebuilt.
The strict67-file driver runs and replays in separate Python interpreters.
"""
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
import signal
import subprocess
import sys
import threading
import time
from unittest.mock import patch

if not __debug__:
    raise RuntimeError("authority checks require assertions enabled")
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import launch_cohere_semantic_artifact_reproduction_spot as previous
from scripts import run_cohere_fixed48_fresh_falsifier as driver

ROOT = Path(driver.BASE) / "scientific-execution" / "python-replay-a0002"
CONFIG, NAME = ROOT / "config.json", ""
OWN = "scripts/launch_cohere_fixed48_fresh_scientific_spot.py"
MODULE = OWN[:-3].replace("/", ".")
SCHEMA = "borsuk-cohere-fixed48-fresh-scientific-spot-v1"
PREFIX = "research/semantic-router/20261002/fixed48-scientific-"
TOKEN_PREFIX, TAG = "fixed48-science-", "borsuk-fixed48-fresh-scientific"
WALL, WORKER_SECONDS, SERVICE_SECONDS = 4500, 3600, 3660
MEMORY, SCRATCH = 12 << 30, 16 << 30
WORK_ROOT = Path("/mnt/cohere-fixed48-scientific")
RETAINED = Path(driver.BASE) / "artifact-reproduction/a0002"
QUALIFICATION = Path(driver.BASE) / "implementation-gates/a0001"
INSTANCE_TYPE, IMAGE_ID = previous.INSTANCE_TYPE, previous.IMAGE_ID
ROOT_DEVICE_NAME, SUBNET = previous.ROOT_DEVICE_NAME, previous.SUBNET
REGION, BUCKET = previous.REGION, previous.BUCKET
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, .625
AWSCLI_VERSION, AWSCLI_SHA256 = previous.AWSCLI_VERSION, previous.AWSCLI_SHA256
CODE = tuple(sorted(set((*previous.CODE, *driver.CODE, OWN))))
CONTROLLER_FILES = ("config.json", "driver-config.json", "asset-manifest.json", "source-qualification.json",
    "bootstrap-staging.json", "staging.json", "transport-budget.json", "runtime-abi.json", "cpu.txt", "tool-versions.json", "run-closed.log", "profile.log",
    "profile-resources.txt", "driver-process.log", "driver-replay.log", "driver-replay.json",
    "profile-cgroup.json", "scientific-closure.json", "failure.json")
TERMINAL_IDENTITIES = ("config_sha256", "driver_config_sha256", "asset_manifest_sha256", "code_identity_sha256",
    "refs_identity_sha256", "artifact_roster_sha256", "scorer_binary_sha256", "campaign_schema",
    "awscli_version", "awscli_sha256")
ARTIFACTS = ()  # Bound per launch to the authoritative driver roster.
LOCAL_BYTES, MAX_BODY_BYTES = 1 << 20, 16 << 30
encoded, sha, artifact, write = previous.encoded, previous.sha, previous.artifact, previous.write
regular_path, read_json = driver.regular_path, driver.read_json
archive_digest, lifecycle = previous.archive_digest, previous.lifecycle


def pin(value, cap=MAX_BODY_BYTES):
    assert type(value) is dict and set(value) == {"bytes", "sha256"}, "exact body pin"
    assert type(value["bytes"]) is int and 0 < value["bytes"] <= cap, "body cap"
    assert type(value["sha256"]) is str and re.fullmatch("[0-9a-f]{64}", value["sha256"]), "body SHA"
    return value


def read_repo(repo, pointer, expected=None):
    assert type(pointer) is dict and set(pointer) == {"path", "bytes", "sha256"}
    driver.safe_key(pointer["path"])
    if expected is not None: assert pointer["path"] == str(expected), "config destination differs"
    identity = pin({k: pointer[k] for k in ("bytes", "sha256")}, LOCAL_BYTES)
    path = regular_path(repo / pointer["path"])
    assert artifact(path) == identity, "repo body identity: " + pointer["path"]
    return path.read_bytes()


def original_assets(repo):
    """Read only original small receipts; no large/native bodies are hydrated."""
    collected = read_json(repo / RETAINED / "collection-receipt.json")
    root = read_json(repo / RETAINED / "root-verification.json")
    terminal = read_json(repo / RETAINED / "aws-terminal.json")
    assert collected["complete"] is collected["whole_body_verification"] is True
    assert collected["state"] == root["instance_state_verified"] == "terminated" and root["qualified"] is True
    assert collected["retained_body_count"] == root["retained_bodies"] == 30
    assert root["terminal_artifacts"] == len(collected["files"]) == 44
    assert root["original_collector_full_stream_authentication"] is True
    assert terminal["status"] == terminal["phase"] == "complete" and terminal["exit_code"] == terminal["original_exit_code"] == 0
    assert terminal["instance_id"] == collected["instance_id"]
    assert {n: {k: p[k] for k in ("bytes", "sha256")} for n, p in collected["files"].items()} == terminal["artifacts"]
    assets = {}
    for name in (*driver.retained.RETAINED_FILES, "COMPLETE.json"):
        body = collected["files"]["screen/" + name]
        identity = pin({k: body[k] for k in ("bytes", "sha256")})
        assert body["full_body_stream_verified"] is True and body["bucket"] == BUCKET
        assets["retained/" + name] = dict(**identity, source=dict(bucket=body["bucket"], key=body["key"]),
                                        repo_path=str(RETAINED / "screen" / name))
    old = read_json(repo / QUALIFICATION / "aws-terminal.json")
    launch = read_json(repo / QUALIFICATION / "aws-launch.json")
    native_root = read_json(repo / QUALIFICATION / "root-verification.json")
    assert old["status"] == old["phase"] == "complete" and old["exit_code"] == old["original_exit_code"] == 0
    assert native_root["qualified"] is True and native_root["instance_state_verified"] == "terminated"
    assert native_root["actual_full_workspace_execution"] is False and native_root["source_identity_sha256"] == driver.SOURCE_ID
    assert old["instance_id"] == launch["instance_id"]
    with driver.qualification.execution_mode(fixed48=True):
        names = (*driver.qualification.ARTIFACTS, "aws-reservation.json", "aws-closeout.json", "aws-terminal.json",
                 "collection-replay.json", "root-verification.json")
        assert len(set(names)) == 21 and len(old["artifacts"]) == 16
        for name in names:
            path = QUALIFICATION / name
            identity = pin(old["artifacts"][name] if name in old["artifacts"] else artifact(repo / path), 16 << 20)
            assets["qualification/" + name] = dict(**identity,
                source=dict(bucket=BUCKET, key=launch["prefix"] + "/artifacts/" + name), repo_path=str(path))
    return assets


# Only driver imports exist in this interpreter. Qualification pointers are
# checked here; their full original bodies are authenticated on the worker.
INSPECT = r"""
import json,sys
from pathlib import Path
repo=Path(sys.argv[3]); sys.path.insert(0,str(repo))
from scripts import run_cohere_fixed48_fresh_falsifier as h
c=h.read_config(Path(sys.argv[1]),sys.argv[2],repo)
old=h.offline.decode(h.retained.archived.read_ref(repo,h.retained.FIXED['archived_config']))
a=h.panel_authority(c,repo,old)
p=h.read_json(repo/h.BASE/'implementation-gates/a0001/source-qualification.json')
s=h.qualification.worker.source_hashes(repo)
h.require(s==p['source_sha256'] and len(s)==399 and h.qualification.worker.source_identity(s)==h.SOURCE_ID,'current399 source mismatch')
r=h.read_json(repo/h.BASE/'artifact-reproduction/a0002/screen/generation/manifest.json')
h.retained.read_config(repo/h.BASE/'artifact-reproduction/a0002/screen/config.json',c['retained']['config']['sha256'],repo,historical_metadata_replay=True)
shards=a['metadata_authority']['ordered_train_shards']
indexes={x['shard_ordinal'] for x in a['panel']['selected']}
indexes.update(i for i,x in enumerate(shards) if any(x['source_start']<hi and x['source_end_exclusive']>lo for lo,hi in ((1000000,1002000),(1005000,1006000))))
objects=[shards[i] for i in sorted(indexes)]+[old['registered_test'],old['consumed_queries']]
acquisition={x['key']:{k:x[k] for k in ('bytes','sha256')} for x in objects}
h.require(len(acquisition)==len(objects),'unique acquisition object roster')
print(json.dumps(dict(native_source_identity_sha256=h.SOURCE_ID,scorer_binary_sha256=c['qualification']['files']['binaries/check_semantic_router_scorer']['sha256'],selected_sha256=a['panel']['selected_sha256'],largest_shard_bytes=max(x['bytes'] for x in objects),acquisition_objects=acquisition,driver_outputs=sorted(h.output_roster(c,r))),sort_keys=True))
"""


def qualify(base=Path("."), config_path=None, config_sha=None):
    repo = regular_path(base)
    path = regular_path(repo / CONFIG if config_path is None else config_path)
    config = read_json(path)
    assert set(config) == {"schema", "authority_pending", "execution_source", "code_sha256", "driver_config", "asset_manifest"}
    assert config["schema"] == SCHEMA and config["authority_pending"] is False, "config freeze pending"
    digest = artifact(path)["sha256"]
    assert config_sha is None or digest == config_sha, "config changed"
    source = config["execution_source"]
    assert type(source) is dict and set(source) == {"commit", "archive_sha256"}
    assert re.fullmatch("[0-9a-f]{40}", source["commit"]) and re.fullmatch("[0-9a-f]{64}", source["archive_sha256"]), "source freeze pending"
    code = {n: artifact(regular_path(repo / n))["sha256"] for n in CODE}
    assert config["code_sha256"] == code, "exact controller source closure"
    dc_body = read_repo(repo, config["driver_config"], ROOT / "driver-config.json")
    manifest_body = read_repo(repo, config["asset_manifest"], ROOT / "asset-manifest.json")
    dc, manifest = json.loads(dc_body), json.loads(manifest_body)
    assert dc["execution_source"] == source and dc["code_sha256"] == {n: code[n] for n in driver.CODE}, "driver source freeze differs"
    assert dc["qualification"]["directory"] == str(WORK_ROOT / "assets/qualification")
    r = dc["retained"]
    assert r["directory"] == str(WORK_ROOT / "assets/retained")
    assert r["config"]["path"] == str(WORK_ROOT / "assets/retained/config.json")
    assert r["complete"]["path"] == str(WORK_ROOT / "assets/retained/COMPLETE.json")
    assert set(manifest) == {"schema", "authority_pending", "assets"}
    assert manifest["schema"] == "borsuk-fixed48-scientific-assets-v1" and manifest["authority_pending"] is False, "asset freeze pending"
    originals = original_assets(repo)
    assert set(manifest["assets"]) == set(originals), "exact original52 assets required"
    for name, entry in manifest["assets"].items():
        assert set(entry) == {"bytes", "sha256", "source"}
        identity = pin({k: entry[k] for k in ("bytes", "sha256")})
        assert identity == {k: originals[name][k] for k in ("bytes", "sha256")}, "original asset identity: " + name
        transport = entry["source"]
        if set(transport) == {"repo_path"}:
            assert transport["repo_path"] == originals[name]["repo_path"] and entry["bytes"] <= LOCAL_BYTES
            assert artifact(regular_path(repo / transport["repo_path"])) == identity, "small repo asset differs"
        else:
            assert transport == originals[name]["source"], "original S3 pointer differs"
            driver.safe_key(transport["key"])
        if name.startswith("qualification/"):
            assert dc["qualification"]["files"][name.removeprefix("qualification/")] == identity
    assert set(dc["qualification"]["files"]) == {n.removeprefix("qualification/") for n in originals if n.startswith("qualification/")}
    for key, name in (("config", "config.json"), ("complete", "COMPLETE.json")):
        assert {k: r[key][k] for k in ("bytes", "sha256")} == {k: originals["retained/" + name][k] for k in ("bytes", "sha256")}
    inspected = json.loads(subprocess.check_output([sys.executable, "-c", INSPECT, str(repo / config["driver_config"]["path"]),
        config["driver_config"]["sha256"], str(repo)], cwd=repo, timeout=45))
    outputs = inspected["driver_outputs"]
    assert len(outputs) == 50 and all(driver.safe_key(n) for n in outputs), "authoritative driver50 roster"
    staging_gets = 2 + sum("bucket" in x["source"] for x in manifest["assets"].values())
    prospective = staging_gets + len(inspected["acquisition_objects"])
    assert prospective <= 256, "prospective combined request cap"
    artifacts = [*CONTROLLER_FILES, *("screen/" + n for n in outputs), "screen/COMPLETE.json"]
    return dict(config_path=str(CONFIG), config_sha256=digest, source_archive_commit=source["commit"],
        source_archive_sha256=source["archive_sha256"], driver_config=config["driver_config"],
        asset_manifest=config["asset_manifest"], driver_config_sha256=config["driver_config"]["sha256"],
        asset_manifest_sha256=config["asset_manifest"]["sha256"], code_identity_sha256=sha(encoded(code)),
        refs_identity_sha256=sha(encoded(driver.REFS)), artifact_roster_sha256=sha(encoded(artifacts)),
        artifacts=artifacts, staging_gets=staging_gets, prospective_dispatches=prospective,
        acquisition_objects=inspected["acquisition_objects"], driver_output_reserve_bytes=64 << 20, largest_shard_bytes=inspected["largest_shard_bytes"], scorer_binary_sha256=inspected["scorer_binary_sha256"], campaign_schema=SCHEMA,
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)


def preflight(base=Path("."), collection_out=None):
    repo = regular_path(base)
    owned = None
    if collection_out is not None:
        out = regular_path(collection_out)
        assert re.fullmatch("a[0-9]{4}", out.name) and out == regular_path(repo / ROOT / out.name), "unowned collection output"
        owned = str(out.relative_to(repo)) + "/"
    status = subprocess.check_output(["git", "status", "--porcelain", "-z", "--untracked-files=all"], cwd=repo, text=True)
    assert all(owned and entry.startswith("?? ") and entry[3:].startswith(owned) for entry in status.split("\0") if entry), "dirty frozen source"
    proof = qualify(repo)
    for path in (CONFIG, ROOT / "driver-config.json", ROOT / "asset-manifest.json"):
        assert (repo / path).read_bytes() == subprocess.check_output(["git", "show", "HEAD:" + str(path)], cwd=repo), "uncommitted config"
    source = proof["source_archive_commit"]
    for a, b in ((source, "HEAD"), (source, "origin/main"), ("HEAD", "origin/main")):
        subprocess.run(["git", "merge-base", "--is-ancestor", a, b], cwd=repo, check=True)
    for path in (CONFIG, ROOT / "driver-config.json", ROOT / "asset-manifest.json"):
        assert subprocess.run(["git", "cat-file", "-e", source + ":" + str(path)], cwd=repo,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0, "source must precede config freeze"
    for n in CODE:
        assert subprocess.check_output(["git", "show", source + ":" + n], cwd=repo) == (repo / n).read_bytes(), "frozen code drift: " + n
    assert archive_digest(source, repo) == proof["source_archive_sha256"], "source archive drift"
    return proof


def config_key(digest):
    assert re.fullmatch("[0-9a-f]{64}", digest)
    return "research/semantic-router/20261002/fixed48-scientific-configs/" + digest + ".json"


BOOTSTRAP_STAGE = r"""import boto3,hashlib,json,os,sys,tarfile
from pathlib import Path
from botocore.config import Config
root=Path.cwd(); limit=int(sys.argv[4])
def size():
 seen=set(); total=0
 for base,ds,fs in os.walk(root):
  for n in fs:
   p=Path(base)/n
   if p.is_symlink(): continue
   x=p.stat(); k=x.st_dev,x.st_ino
   if k not in seen: total+=max(x.st_size,x.st_blocks*512); seen.add(k)
 return total
available=((limit-size())//4096)*4096; assert available>0
s3=boto3.client('s3',region_name='eu-central-1',config=Config(retries={'total_max_attempts':1},connect_timeout=10,read_timeout=30))
h=hashlib.sha256(); count=0
with s3.get_object(Bucket=sys.argv[1],Key=sys.argv[2])['Body'] as body,Path('source.tar.gz').open('xb') as out:
 for chunk in iter(lambda:body.read(1048576),b''):
  count+=len(chunk); assert count<=available,'source staging scratch bound'
  h.update(chunk);out.write(chunk)
 out.flush();os.fsync(out.fileno())
assert h.hexdigest()==sys.argv[3],'source archive authentication'
reserve=0
with tarfile.open('source.tar.gz',mode='r|gz') as archive:
 for m in archive:
  assert not m.name.startswith('/') and '..' not in Path(m.name).parts
  if m.isfile(): reserve+=((m.size+4095)//4096)*4096
assert size()+reserve+(16<<20)<=limit,'source extraction scratch reserve'
Path('bootstrap-staging.json').write_text(json.dumps(dict(source_archive_bytes=count,source_archive_sha256=h.hexdigest(),source_repository_reserve_bytes=reserve,scratch_before_extract_bytes=size(),scratch_limit_bytes=limit,source_authenticated=True,source_gets=1,scope='source archive/repo/AWSCLI bootstrap staging beneath worker root; install and uploads separately observed'))+'\n')
"""


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert (commit, archive_sha) == (qualification["source_archive_commit"], qualification["source_archive_sha256"])
    assert re.fullmatch(re.escape(PREFIX) + "a[0-9]{4}", prefix)
    driver.safe_key(archive_key)
    assert qualification["config_path"] == str(CONFIG) and qualification["campaign_schema"] == SCHEMA
    config_body = Path(CONFIG).read_bytes()
    assert sha(config_body) == qualification["config_sha256"]
    _, bootstrap = lifecycle()
    artifacts = tuple(qualification["artifacts"])
    upload = (*artifacts[:-1], "screen/failure.json", "screen/failure-resources.json", "screen/failure-cleanup.json", artifacts[-1])
    adapter = {k: qualification[k] for k in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG), native_binary={"key": "unused"}, native_publisher={"key": "unused"})
    with patch.multiple(bootstrap, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=upload, TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), patch.object(bootstrap, "_offered", return_value=False):
        body = bootstrap.user_data(commit, archive_sha, archive_key, prefix, adapter)
    source_command = (f"systemd-run --quiet --wait --pipe -p MemoryMax={MEMORY} -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=300 -p WorkingDirectory=\"$root\" "
        f"python3.12 - {shlex.quote(BUCKET)} {shlex.quote(archive_key)} {archive_sha} {SCRATCH} <<'SOURCE'\n" + BOOTSTRAP_STAGE + "SOURCE\n")
    source_line = f"aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors\n"
    assert body.count(source_line) == 1, "source bootstrap hook drift"
    body = body.replace(source_line, source_command)
    early_proof = {k: qualification[k] for k in (*TERMINAL_IDENTITIES, "source_archive_commit", "source_archive_sha256")}
    proof64 = base64.b64encode(gzip.compress(encoded(early_proof), mtime=0)).decode()
    proof_sha = sha(encoded(qualification))
    install = f"""phase=install
aws s3 cp s3://{BUCKET}/{config_key(proof_sha)} source-qualification.json --only-show-errors
printf '%s  source-qualification.json\\n' '{proof_sha}' | sha256sum -c -
aws s3 cp s3://{BUCKET}/{config_key(qualification['config_sha256'])} config.json --only-show-errors
printf '%s  config.json\\n' '{qualification['config_sha256']}' | sha256sum -c -
python3.12 -m venv --system-site-packages "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0
lscpu >cpu.txt
phase=scientific
systemd-run --unit=cohere-fixed48-scientific --wait --pipe -p MemoryMax={MEMORY} -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec={SERVICE_SECONDS} -p WorkingDirectory="$root" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C {' '.join('--setenv='+n+'=2' for n in driver.retained.THREAD_ENV)} \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 {WORKER_SECONDS} \\
 "$root/venv/bin/python" -m {MODULE} --stage "$root/repo" "$root" {prefix} >profile.log 2>&1
"""
    start, end = body.index("phase=install\n"), body.index("phase=complete\n")
    body = body[:start] + install + body[end:]
    fields = {k: qualification[k] for k in TERMINAL_IDENTITIES}
    assert body.count(repr(fields)[1:-1]) == 1
    body = body.replace(repr(fields)[1:-1], "**json.loads(Path('source-qualification.json').read_text())")
    body = body.replace("'source_archive_sha256':'" + archive_sha + "',", "", 1)
    body = body.replace("/mnt/native-semantic-router-cold", str(WORK_ROOT))
    body = body.replace("python3-boto3 python3.12", "python3-boto3 python3.12 python3.12-venv")
    body = body.replace(", 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256')", "")
    early = f"python3 -c 'import base64,gzip; from pathlib import Path; Path(\"source-qualification.json\").write_bytes(gzip.decompress(base64.b64decode(\"{proof64}\",validate=True)))'\n"
    body = body.replace("exec >run.log 2>&1\n", "exec >run.log 2>&1\n" + early, 1)
    body = body.replace("if path.is_file():", "if path.is_file() and (name != 'screen/COMPLETE.json' or os.environ['EXIT_CODE'] == '0'):", 1)
    body = body.replace('if [ -f "$name" ]; then', 'if [ -f "$name" ] && { [ "$name" != screen/COMPLETE.json ] || [ "$code" = 0 ]; }; then')
    body = body.replace('timeout --kill-after=5 60 aws s3 cp "$name"',
        f'systemd-run --quiet --wait --pipe -p MemoryMax={MEMORY} -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=135 --setenv=AWS_MAX_ATTEMPTS=1 timeout --kill-after=5 120 aws s3 cp "$root/$name"')
    body = body.replace('timeout --kill-after=5 60 aws s3 cp terminal.json', 'timeout --kill-after=5 60 aws s3 cp "$root/terminal.json"')
    terminal_script = body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split("\nPY\n", 1)[0]
    compile(terminal_script, "<scientific-terminal>", "exec")
    subprocess.run(["bash", "-n"], input=body, text=True, check=True)
    assert len(body.encode()) <= 16384, "EC2 user data cap"
    assert all(n not in body for n in ("cargo", "rustup", "unused", "--publish", "two_bit_http"))
    return body


def stage_configs(proof):
    """Already frozen JSON only; put_if_absent never replaces an authority."""
    c = read_json(CONFIG)
    proof_body = encoded(proof)
    assert len(proof_body) <= LOCAL_BYTES
    lifecycle()[0].peer.put_if_absent(config_key(sha(proof_body)), proof_body)
    for path in (CONFIG, ROOT / "driver-config.json", ROOT / "asset-manifest.json"):
        body = path.read_bytes()
        pointer = ({"bytes": len(body), "sha256": sha(body)} if path == CONFIG else
                   {k: c["driver_config" if path.name == "driver-config.json" else "asset_manifest"][k] for k in ("bytes", "sha256")})
        assert {"bytes": len(body), "sha256": sha(body)} == pin(pointer, LOCAL_BYTES)
        lifecycle()[0].peer.put_if_absent(config_key(pointer["sha256"]), body)


def poll(ec2, s3, prefix, instance_id, started):
    shared, _ = lifecycle()
    with patch.object(shared, "WALL", WALL):
        return shared.poll(ec2, s3, prefix, instance_id, started)


def scratch_usage(root):
    unique, allocated, logical, seen, aliases = 0, 0, 0, set(), 0
    for base, dirs, names in os.walk(root, followlinks=False):
        for name in names:
            path = Path(base) / name
            try: stat = path.stat(follow_symlinks=False)
            except FileNotFoundError: continue  # Owned extractor released its shard.
            if path.is_symlink(): continue
            logical += stat.st_size
            key = stat.st_dev, stat.st_ino
            if key in seen:
                aliases += 1; continue
            seen.add(key); unique += stat.st_size; allocated += stat.st_blocks * 512
    return dict(unique_inode_bytes=unique, physical_allocated_bytes=allocated,
                logical_path_bytes=logical, unique_inodes=len(seen), hardlink_aliases=aliases)


def scratch_bytes(root):
    usage = scratch_usage(root)
    return max(usage["unique_inode_bytes"], usage["physical_allocated_bytes"])


@contextmanager
def scratch_monitor(root, report):
    stopped, errors = threading.Event(), []
    def check():
        usage = scratch_usage(root)
        report["scratch_usage"] = usage
        size = max(usage["unique_inode_bytes"], usage["physical_allocated_bytes"])
        report["peak_scratch_bytes"] = max(report["peak_scratch_bytes"], size)
        assert size <= SCRATCH, "whole-worker scratch limit"
    def watch():
        while not stopped.wait(.5):
            try: check()
            except BaseException as error:
                errors.append(error); os.kill(os.getpid(), signal.SIGTERM); return
    check()
    thread = threading.Thread(target=watch, daemon=True); thread.start()
    try:
        yield check
        if errors: raise errors[0]
    finally:
        stopped.set(); thread.join(timeout=5)
        assert not thread.is_alive(), "scratch observer cleanup"
        check()


def download(s3, source, target, identity, repo, deadline):
    """Bounded stream/authentication completes BEFORE final rename."""
    pin(identity)
    assert scratch_bytes(WORK_ROOT) + identity["bytes"] <= SCRATCH, "staging scratch reserve"
    target = regular_path(target)
    assert not target.exists(), "fresh staged body required"
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_name(target.name + ".part"); assert not part.exists()
    stream = (regular_path(repo / source["repo_path"]).open("rb") if "repo_path" in source else
              s3.get_object(Bucket=source["bucket"], Key=source["key"])["Body"])
    hashed, count = hashlib.sha256(), 0
    try:
        with stream, part.open("xb") as out:
            for chunk in iter(lambda: stream.read(LOCAL_BYTES), b""):
                assert time.monotonic() < deadline, "staging deadline"
                count += len(chunk); assert count <= identity["bytes"], "staging length overflow"
                hashed.update(chunk); out.write(chunk)
            out.flush(); os.fsync(out.fileno())
        assert {"bytes": count, "sha256": hashed.hexdigest()} == identity, "staging body authentication"
        os.rename(part, target); driver.sync_directory(target.parent)
    finally: part.unlink(missing_ok=True)
    return identity


def spend(path, scope):
    with path.open("r+") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        value = json.load(stream)
        assert sum(value["dispatches"].values()) < 256, "combined acquisition request cap"
        value["dispatches"][scope] = value["dispatches"].get(scope, 0) + 1
        stream.seek(0); stream.truncate(); stream.write(json.dumps(value, sort_keys=True))
        stream.flush(); os.fsync(stream.fileno())


def run_cli(repo, out, proof, replay, seconds):
    assert seconds > 0, "whole-worker deadline"
    args = [sys.executable, str(repo / driver.OWN)]
    if replay: args.append("--replay")
    args += [str(out / "driver-config.json"), proof["driver_config_sha256"], str(repo), str(out / "screen")]
    return previous.prior.run_process(args, out / ("driver-replay.log" if replay else "driver-process.log"), seconds)


def stage(repo, out, prefix):
    repo, out = regular_path(repo), regular_path(out)
    assert out == WORK_ROOT and not out.is_relative_to(repo), "exact external worker output"
    assert re.fullmatch(re.escape(PREFIX) + "a[0-9]{4}", prefix)
    started = time.monotonic(); deadline = started + WORKER_SECONDS
    old_term, old_alarm = signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGALRM)
    counters = dict(closed=False, before=None, after=None)
    staging = dict(closed=False, files={}, peak_scratch_bytes=0, shared_hardlinks_accounted=True)
    closure = dict(closed=False, driver_invocations=0, build_invocations=0, driver_exit_code=None,
        process_cleanup=False, replay_passed=False, scientific_status="INVALID", prefix=prefix, wall_seconds=0)
    failure = dict(status="pending", replacement_allowed=False)
    budget = out / "transport-budget.json"
    def interrupted(signum, frame): raise InterruptedError("worker interrupted: " + str(signum))
    signal.signal(signal.SIGTERM, interrupted); signal.signal(signal.SIGALRM, interrupted)
    signal.setitimer(signal.ITIMER_REAL, WORKER_SECONDS)
    try:
        proof = read_json(out / "source-qualification.json")
        assert artifact(out / "config.json")["sha256"] == proof["config_sha256"]
        config = read_json(out / "config.json")
        assert config["schema"] == SCHEMA and config["authority_pending"] is False
        counters["before"] = previous.prior.capture_cgroup()
        with patch.object(previous.prior, "MEMORY", MEMORY):
            previous.prior.validate_cgroup(dict(closed=True, before=counters["before"], after=counters["before"]))
        assert not budget.exists(), "fresh transport ledger"
        write(budget, dict(dispatches={}, prospective_dispatches=proof["prospective_dispatches"],
            scope="worker SDK config2/assets GETs plus unchanged driver Accounting actual dispatches; bootstrap/install/uploads separate",
            cap=256, driver_actual_dispatches_verified=False))
        import boto3
        from botocore.config import Config
        s3 = boto3.Session(region_name=REGION).client("s3", config=Config(
            retries={"total_max_attempts": 1}, connect_timeout=10, read_timeout=30))
        s3.meta.events.register("before-send.s3", lambda request, **kwargs: spend(budget, "staging_" + request.method))
        with scratch_monitor(out, staging) as checkpoint:
            for field in ("driver_config", "asset_manifest"):
                pointer = config[field]
                download(s3, dict(bucket=BUCKET, key=config_key(pointer["sha256"])), out / Path(pointer["path"]).name,
                         pin({k: pointer[k] for k in ("bytes", "sha256")}, LOCAL_BYTES), repo, deadline)
            # These separate authorities are deliberately absent from the archive.
            for field in ("driver_config", "asset_manifest"):
                path = regular_path(repo / config[field]["path"])
                assert not path.exists(), "configuration was included in source archive"
                path.parent.mkdir(parents=True, exist_ok=True); write(path, (out / path.name).read_bytes())
            assert qualify(repo, out / "config.json", proof["config_sha256"]) == proof, "worker freeze drift"
            manifest = read_json(out / "asset-manifest.json")
            assert not (out / "assets").exists() and not (out / "screen").exists(), "fresh worker assets/output"
            # All large aliases are hardlinks. Reserve the sole Parquet plus
            # new vectors/scorer/16MiB diagnostics/metadata/debug receipts.
            reserve = sum(x["bytes"] for x in manifest["assets"].values()) + proof["largest_shard_bytes"] + proof["driver_output_reserve_bytes"]
            assert scratch_bytes(out) + reserve <= SCRATCH, "whole frozen staging/driver scratch reserve"
            write(out / "tool-versions.json", previous.prior.tools())
            for name, entry in sorted(manifest["assets"].items(), key=lambda item: item[0].endswith("COMPLETE.json")):
                staging["files"][name] = download(s3, entry["source"], out / "assets" / name,
                    {k: entry[k] for k in ("bytes", "sha256")}, repo, deadline)
                checkpoint(); write(out / "staging.json", staging)
            assert read_json(budget)["dispatches"] == {"staging_GET": proof["staging_gets"]}, "staging dispatch roster"
            staging["closed"] = True
            binary = out / "assets/qualification/binaries/check_semantic_router_scorer"
            previous.runtime_abi(binary.read_bytes(), artifact(binary), out)
            staging["largest_shard_reserve_bytes"] = proof["largest_shard_bytes"]
            staging["driver_output_reserve_bytes"] = proof["driver_output_reserve_bytes"]
            assert scratch_bytes(out) + proof["largest_shard_bytes"] + proof["driver_output_reserve_bytes"] <= SCRATCH, "largest Parquet scratch reserve"
            closure.update(config_sha256=proof["config_sha256"], driver_config_sha256=proof["driver_config_sha256"],
                           execution_source=config["execution_source"], driver_invocations=1)
            result = run_cli(repo, out, proof, False, deadline-time.monotonic())
            closure.update(driver_exit_code=result["exit_status"], process_cleanup=result["process_cleanup"])
            assert type(result["exit_status"]) is int and result["exit_status"] == 0 and result["process_cleanup"] is True, "driver failed"
            actual = read_json(out / "screen/resources.json")["http_request_dispatch_attempts"]
            transport = read_json(budget)
            assert actual == dict(GET=len(proof["acquisition_objects"]), HEAD=0, PUT=0), "frozen acquisition roster dispatches"
            assert transport["dispatches"] == {"staging_GET": proof["staging_gets"]}
            transport["driver_actual_dispatches"] = actual
            assert sum(transport["dispatches"].values()) + sum(actual.values()) <= 256
            transport["driver_actual_dispatches_verified"] = True; write(budget, transport)
            replay = run_cli(repo, out, proof, True, deadline-time.monotonic())
            assert replay["exit_status"] == 0 and replay["process_cleanup"] is True, "driver replay failed"
            decision = read_json(out / "driver-replay.log")
            assert decision == read_json(out / "screen/decision.json") and decision["execution_status"] == "SUCCESS"
            assert decision["scientific_status"] in ("GO", "FAIL") and decision["build_invocations"] == 0
            write(out / "driver-replay.json", dict(passed=True, execution_status="SUCCESS", result=decision,
                config_sha256=proof["driver_config_sha256"], complete=artifact(out / "screen/COMPLETE.json"),
                replay_exit_code=0, process_cleanup=True))
            counters.update(after=previous.prior.capture_cgroup(), closed=True)
            with patch.object(previous.prior, "MEMORY", MEMORY): previous.prior.validate_cgroup(counters)
            closure.update(closed=True, replay_passed=True, scientific_status=decision["scientific_status"])
            failure.update(status="complete", driver_exit_code=0)
    except BaseException as error:
        failure.update(status="failed", error_type=type(error).__name__, error=str(error))
        if isinstance(error, (KeyboardInterrupt, SystemExit)): raise
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGTERM, old_term); signal.signal(signal.SIGALRM, old_alarm)
        if counters["before"] is not None and counters["after"] is None: counters["after"] = previous.prior.capture_cgroup()
        closure["wall_seconds"] = time.monotonic()-started
        write(out / "staging.json", staging); write(out / "profile-cgroup.json", counters)
        write(out / "scientific-closure.json", closure); write(out / "failure.json", failure)
    return closure


def validate_closed(out, proof, terminal, files):
    assert type(terminal["exit_code"]) is type(terminal["original_exit_code"]) is int
    assert 0 <= terminal["exit_code"] <= 255 and 0 <= terminal["original_exit_code"] <= 255
    complete = terminal["phase"] == "complete" and terminal["exit_code"] == 0
    assert terminal["status"] == ("complete" if complete else "failed"), "false terminal completion"
    if not complete:
        assert terminal["exit_code"] != 0
        return False
    assert terminal["original_exit_code"] == 0 and set(files) == set(proof["artifacts"]), "full completed roster"
    bootstrap = read_json(out / "bootstrap-staging.json")
    assert bootstrap["source_authenticated"] is True and bootstrap["source_archive_sha256"] == proof["source_archive_sha256"]
    assert bootstrap["scratch_limit_bytes"] == SCRATCH and bootstrap["scratch_before_extract_bytes"] + bootstrap["source_repository_reserve_bytes"] <= SCRATCH
    assert files["config.json"]["sha256"] == proof["config_sha256"]
    assert files["driver-config.json"]["sha256"] == files["screen/config.json"]["sha256"] == proof["driver_config_sha256"]
    assert files["asset-manifest.json"]["sha256"] == proof["asset_manifest_sha256"]
    assert read_json(out / "source-qualification.json") == proof
    dc = read_json(out / "driver-config.json")
    marker, decision, replay = (read_json(out / n) for n in ("screen/COMPLETE.json", "screen/decision.json", "driver-replay.json"))
    assert marker["schema"] == driver.SCHEMA + "-complete" and marker["execution_status"] == "SUCCESS"
    assert marker["config_sha256"] == proof["driver_config_sha256"]
    expected = {n.removeprefix("screen/"): p for n, p in files.items() if n.startswith("screen/") and n != "screen/COMPLETE.json"}
    assert marker["files"] == expected and marker["roster_sha256"] == driver.retained.archived.prior.value_sha(expected)
    assert replay["passed"] is replay["process_cleanup"] is True and replay["replay_exit_code"] == 0
    assert replay["execution_status"] == "SUCCESS" and replay["config_sha256"] == proof["driver_config_sha256"]
    assert replay["complete"] == files["screen/COMPLETE.json"] and replay["result"] == decision
    assert decision["execution_status"] == "SUCCESS" and decision["scientific_status"] == marker["scientific_status"]
    assert decision["scientific_status"] in ("GO", "FAIL") and decision["build_invocations"] == 0
    assert decision["scorer_invocations"] == decision["oracle_invocations"] == 1
    assert decision["cleanup_complete"] is decision["resource_gate_passed"] is decision["native_qualification_passed"] is True
    closure, staging = read_json(out / "scientific-closure.json"), read_json(out / "staging.json")
    assert closure["closed"] is closure["process_cleanup"] is closure["replay_passed"] is True
    assert closure["driver_invocations"] == 1 and closure["build_invocations"] == closure["driver_exit_code"] == 0
    assert closure["scientific_status"] == decision["scientific_status"] and 0 <= closure["wall_seconds"] <= WORKER_SECONDS
    assert closure["config_sha256"] == proof["config_sha256"] and closure["driver_config_sha256"] == proof["driver_config_sha256"]
    assert closure["execution_source"] == dc["execution_source"]
    assert staging["largest_shard_reserve_bytes"] == proof["largest_shard_bytes"]
    assert staging["driver_output_reserve_bytes"] == proof["driver_output_reserve_bytes"]
    assert staging["scratch_usage"]["unique_inode_bytes"] <= SCRATCH and staging["scratch_usage"]["physical_allocated_bytes"] <= SCRATCH
    assert staging["closed"] is staging["shared_hardlinks_accounted"] is True and 0 <= staging["peak_scratch_bytes"] <= SCRATCH
    transport = read_json(out / "transport-budget.json")
    assert transport["driver_actual_dispatches_verified"] is True and transport["cap"] == 256
    actual = read_json(out / "screen/resources.json")["http_request_dispatch_attempts"]
    assert transport["driver_actual_dispatches"] == actual == dict(GET=len(proof["acquisition_objects"]), HEAD=0, PUT=0)
    assert transport["dispatches"] == {"staging_GET": proof["staging_gets"]}
    assert sum(transport["dispatches"].values()) + sum(actual.values()) <= 256
    assert transport["prospective_dispatches"] == proof["prospective_dispatches"]
    manifest = read_json(out / "asset-manifest.json")
    assert staging["files"] == {n: {k: p[k] for k in ("bytes", "sha256")} for n, p in manifest["assets"].items()}
    for name, count in (("resources.json", 1), ("measurement-resources.json", 0)):
        driver.check_resources(read_json(out / "screen" / name), dc, count)
    cleanup = read_json(out / "screen/cleanup.json")
    assert cleanup["process_cleanup"] is cleanup["native_scratch_empty"] is True
    measurement, scorer = read_json(out / "screen/measurement-receipt.json"), read_json(out / "screen/scorer-config.json")
    inputs = {k: measurement[k] for k in ("scorer_config", "measurements", "order")}
    driver.offline.validate_receipt(measurement, scorer, inputs)
    driver.offline.validate_measurements((out / "screen/records.jsonl").read_bytes(), scorer, inputs, measurement)
    for field, name in (("scorer_config", "scorer-config.json"), ("measurements", "records.jsonl"),
                        ("order", "source-order.u64"), ("requests", "requests.jsonl")):
        assert {k: measurement[field][k] for k in ("bytes", "sha256")} == files["screen/" + name], "measurement body binding"
    assert measurement["binary_sha256"] == proof["scorer_binary_sha256"]
    assert read_json(out / "screen/measurement-cleanup.json") == cleanup
    offline = read_json(out / "screen/offline-result.json")
    for field in ("execution_status", "scientific_status", "first_crossing_stage", "failure_class", "eligible_for_cold_measurement"):
        assert offline[field] == decision[field], "offline scientific classification"
    oracle = read_json(out / "screen/oracle.json")
    assert oracle["passed"] is oracle["oracle_self_check_passed"] is True and oracle["ground_truth_constructions"] == 1
    for field, name in (("measurement_receipt", "measurement-receipt.json"), ("measurement_sequence", "measurement-sequence.json"),
                        ("truth_u32", "truth.u32"), ("truth_i64", "truth.i64"), ("queries_raw", "queries.raw"), ("source_raw", "source.raw")):
        assert {k: oracle[field][k] for k in ("bytes", "sha256")} == files["screen/" + name], "oracle body binding"
    import struct
    narrow, wide = (out / "screen/truth.u32").read_bytes(), (out / "screen/truth.i64").read_bytes()
    assert len(narrow) == 25600 and len(wide) == 51200
    assert all(a == b for (a,), (b,) in zip(struct.iter_unpack("<I", narrow), struct.iter_unpack("<q", wide))), "truth widening"

    with patch.object(previous.prior, "MEMORY", MEMORY): previous.prior.validate_cgroup(read_json(out / "profile-cgroup.json"))
    abi = read_json(out / "runtime-abi.json")
    assert abi["qualified"] is True and abi["builder"]["sha256"] == proof["scorer_binary_sha256"]
    versions = read_json(out / "tool-versions.json")
    assert versions["versions"] == previous.prior.VERSIONS and versions["threads"] == 2 and versions["aws_max_attempts"] == 1
    assert versions["architecture"] == "x86_64" and versions["os_release"]["ID"] == "ubuntu" and versions["os_release"]["VERSION_ID"] == "24.04"
    assert versions["thread_environment"] == dict.fromkeys(driver.retained.THREAD_ENV, "2") and versions["python"].startswith("3.12.")
    timing = (out / "profile-resources.txt").read_text()
    assert 0 <= int(timing.split("Maximum resident set size (kbytes): ", 1)[1].splitlines()[0])*1024 <= MEMORY
    assert int(timing.split("Exit status: ", 1)[1].splitlines()[0]) == 0
    assert read_json(out / "failure.json")["status"] == "complete"
    return True


def collect(s3, prefix, out, instance_id, commit, digest):
    out = regular_path(out)
    assert re.fullmatch(re.escape(PREFIX) + "a[0-9]{4}", prefix)
    assert out == regular_path(ROOT / prefix.removeprefix(PREFIX)), "unowned collection output"
    launch, close, reservation = (read_json(out / n) for n in ("aws-launch.json", "aws-closeout.json", "aws-reservation.json"))
    assert close["state"] == "terminated" and close["nodes"] == launch["nodes"], "terminate and wait BEFORE collect"
    assert instance_id == launch["instance_id"] in {n["instance_id"] for n in close["nodes"].values()}
    assert launch["prefix"] == prefix
    proof = reservation["qualification"]
    assert proof == preflight(collection_out=out), "frozen collection authority drift"
    for record in (launch, reservation): assert record["source_commit"] == commit and record["source_archive_sha256"] == digest
    assert reservation["schema"] == SCHEMA and reservation["config_sha256"] == proof["config_sha256"]
    with s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"] as stream: raw = stream.read(LOCAL_BYTES+1)
    assert 0 < len(raw) <= LOCAL_BYTES, "bounded terminal"
    terminal = driver.retained.primitives.decode(raw); write(out / "aws-terminal.json", raw)
    assert terminal["schema"] == SCHEMA and terminal["instance_id"] == instance_id
    assert terminal["source_commit"] == commit == proof["source_archive_commit"]
    assert terminal["source_archive_sha256"] == digest == proof["source_archive_sha256"]
    for k in TERMINAL_IDENTITIES: assert terminal[k] == proof[k], "terminal identity: " + k
    allowed = {*proof["artifacts"], "screen/failure.json", "screen/failure-resources.json", "screen/failure-cleanup.json"}
    files = terminal["artifacts"]; assert set(files) <= allowed, "unexpected terminal body"
    receipts = {}
    try:
        for name, identity in files.items():
            assert type(identity) is dict and set(identity) == {"bytes", "sha256"}
            assert type(identity["bytes"]) is int and 0 <= identity["bytes"] <= MAX_BODY_BYTES
            assert re.fullmatch("[0-9a-f]{64}", identity["sha256"])
            stream = s3.get_object(Bucket=BUCKET, Key=prefix + "/artifacts/" + name)["Body"]
            hashed, count, body = hashlib.sha256(), 0, bytearray()
            local = identity["bytes"] <= LOCAL_BYTES or name == "screen/records.jsonl"
            assert name != "screen/records.jsonl" or identity["bytes"] <= driver.offline.CAPS["measurements"]
            with stream:
                for chunk in iter(lambda: stream.read(LOCAL_BYTES), b""):
                    count += len(chunk); assert count <= identity["bytes"], "collection length overflow: " + name
                    hashed.update(chunk)
                    if local: body.extend(chunk)
            assert {"bytes": count, "sha256": hashed.hexdigest()} == identity, "body authentication: " + name
            if local:
                target = regular_path(out / name); target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists(): assert artifact(target) == identity, "local body drift"
                else: write(target, bytes(body))
            receipts[name] = dict(**identity, bucket=BUCKET, key=prefix + "/artifacts/" + name,
                full_body_stream_verified=True, local_body=local)
            write(out / "collection-progress.json", dict(complete=False, files=receipts))
        complete = validate_closed(out, proof, terminal, files)
        write(out / "collection-receipt.json", dict(schema=SCHEMA + "-collection", complete=complete,
            instance_id=instance_id, state="terminated", files=receipts, whole_body_verification=True,
            large_bodies_retained_in_s3=True, execution_status="SUCCESS" if complete else "FAIL",
            scientific_status=read_json(out / "screen/decision.json")["scientific_status"] if complete else "INVALID",
            config_sha256=proof["config_sha256"], source_commit=commit, source_archive_sha256=digest))
    except BaseException as error:
        write(out / "collection-error.json", dict(error_type=type(error).__name__, error=str(error),
            authenticated_files=receipts, execution_status="FAIL", scientific_status="INVALID"))
        raise
    return terminal


def main(attempt):
    assert re.fullmatch("a[0-9]{4}", attempt)
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        proof = preflight()  # No cloud session before source/config admission.
        stage_configs(proof)
        shared, _ = lifecycle()
        with patch.object(sys.modules[__name__], "ARTIFACTS", tuple(proof["artifacts"])):
            return shared.main(attempt, campaign=sys.modules[__name__])
    finally: os.chdir(before)


def self_check():
    """Real generated shell/transport/lifecycle, bounded synthetic driver bodies."""
    import copy
    from contextlib import redirect_stdout
    from datetime import datetime, timezone
    import resource
    import shutil
    import tempfile
    from types import ModuleType, SimpleNamespace
    from unittest.mock import Mock

    started = time.monotonic(); checks = 0
    resource.setrlimit(resource.RLIMIT_AS, (256 << 20, 256 << 20)); signal.alarm(55)
    module, repo = sys.modules[__name__], Path(__file__).resolve().parents[1]
    def rejected(call):
        nonlocal checks
        try: call()
        except (AssertionError, ValueError, OSError, RuntimeError, subprocess.CalledProcessError): checks += 1; return
        raise AssertionError("negative check admitted")
    sdk, botocore, exceptions, sdk_config = (ModuleType(n) for n in ("boto3", "botocore", "botocore.exceptions", "botocore.config"))
    class SDKError(Exception):
        def __init__(self, **kwargs): super().__init__("synthetic SDK")
    for n in ("ClientError", "EndpointConnectionError", "ReadTimeoutError"): setattr(exceptions, n, SDKError)
    sdk.Session = Mock(side_effect=AssertionError("cloud forbidden"))
    botocore.exceptions = exceptions; sdk_config.Config = lambda **kwargs: SimpleNamespace(**kwargs)
    class Stream(io.BytesIO):
        def read(self, size=-1):
            assert 0 < size <= LOCAL_BYTES+1, "bounded S3 read"
            return super().read(size)
    class S3:
        def __init__(self, bodies):
            self.bodies, self.calls, self.hook = bodies, [], None
            self.meta = SimpleNamespace(events=SimpleNamespace(register=self.register))
        def register(self, name, callback): self.hook = callback
        def get_object(self, **kwargs):
            assert kwargs["Bucket"] == BUCKET
            self.calls.append(kwargs["Key"])
            if self.hook: self.hook(SimpleNamespace(method="GET"))
            return {"Body": Stream(self.bodies[kwargs["Key"]])}
    with tempfile.TemporaryDirectory(prefix="fixed48-controller-check-") as temporary, patch.dict(sys.modules,
        {"boto3": sdk, "botocore": botocore, "botocore.exceptions": exceptions, "botocore.config": sdk_config}):
        work = Path(temporary); frozen = work / "frozen"; frozen.mkdir()
        originals = original_assets(repo); assert len(originals) == 52; checks += 1
        for n in CODE:
            p = frozen / n; p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes((repo / n).read_bytes())
        manifest = dict(schema="borsuk-fixed48-scientific-assets-v1", authority_pending=False, assets={})
        for n, entry in originals.items():
            # Match root's finite 47-S3 + 5-bridge-repo transport contract.
            bridge = n.startswith("qualification/") and n.removeprefix("qualification/") not in read_json(repo / QUALIFICATION / "aws-terminal.json")["artifacts"]
            source = {"repo_path": entry["repo_path"]} if bridge else entry["source"]
            manifest["assets"][n] = dict(**{k: entry[k] for k in ("bytes", "sha256")}, source=source)
            if bridge:
                target = frozen / source["repo_path"]; target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((repo / source["repo_path"]).read_bytes())
        source = dict(commit="a"*40, archive_sha256="b"*64)
        dc = dict(driver.EXPECTED, refs=driver.REFS, code_sha256={n: artifact(repo / n)["sha256"] for n in driver.CODE},
            execution_source=source, generation_prefix="synthetic/fixed48", qualification=dict(
                directory=str(WORK_ROOT / "assets/qualification"), files={n.removeprefix("qualification/"):
                    {k: p[k] for k in ("bytes", "sha256")} for n, p in originals.items() if n.startswith("qualification/")}),
            retained=dict(directory=str(WORK_ROOT / "assets/retained"), **{k: dict(path=str(WORK_ROOT / "assets/retained" / n),
                **{x: originals["retained/" + n][x] for x in ("bytes", "sha256")}) for k, n in (("config", "config.json"), ("complete", "COMPLETE.json"))}))
        root = frozen / ROOT; root.mkdir(parents=True)
        write(root / "driver-config.json", dc); write(root / "asset-manifest.json", manifest)
        config = dict(schema=SCHEMA, authority_pending=False, execution_source=source,
            code_sha256={n: artifact(repo / n)["sha256"] for n in CODE},
            **{field: dict(path=str(ROOT / name), **artifact(root / name)) for field, name in
               (("driver_config", "driver-config.json"), ("asset_manifest", "asset-manifest.json"))})
        path = root / "config.json"; write(path, config)
        real_output = subprocess.check_output
        def inspect(args, **kwargs):
            assert args[2] == INSPECT
            return real_output([*args[:-1], str(repo)], cwd=repo, timeout=45)
        with patch.object(module, "original_assets", return_value=originals), patch.object(subprocess, "check_output", side_effect=inspect):
            proof = qualify(frozen)
            assert proof["staging_gets"] == 49 and len(proof["acquisition_objects"]) == 67 and proof["prospective_dispatches"] == 116
            checks += 1
            for field, value in (("authority_pending", True), ("execution_source", None), ("code_sha256", {})):
                write(path, dict(config, **{field: value})); rejected(lambda: qualify(frozen))
            write(path, config); rejected(lambda: qualify(frozen, config_sha="0"*64))
            changed = copy.deepcopy(manifest); changed["authority_pending"] = True
            write(root / "asset-manifest.json", changed)
            rejected(lambda: qualify(frozen)); write(root / "asset-manifest.json", manifest)
            # Rebind outer SHA; semantic transport tampering still fails.
            changed = copy.deepcopy(manifest); next(iter(changed["assets"].values()))["source"]["key"] += "-tampered"
            write(root / "asset-manifest.json", changed)
            bad = copy.deepcopy(config); bad["asset_manifest"].update(artifact(root / "asset-manifest.json")); write(path, bad)
            rejected(lambda: qualify(frozen))
            write(root / "asset-manifest.json", manifest); write(path, config)
        for draft_name in ("borsuk-fixed48-scientific-outer-draft-config.json",):
            draft = Path("/tmp") / draft_name
            if draft.exists():
                before = artifact(draft); rejected(lambda:qualify(repo,draft))
                assert artifact(draft)==before
        # Actual Git admits ONLY this attempt's untracked collection output.
        git_repo = work / "git"; git_repo.mkdir()
        def git(*args): return real_output(["git", *args], cwd=git_repo, stderr=subprocess.PIPE)
        git("init", "-q")
        tiny = "source.py"; (git_repo / tiny).write_bytes(b"frozen source\n")
        (git_repo / ROOT).mkdir(parents=True)
        tracked = git_repo / ROOT / "a0001/tracked.log"; tracked.parent.mkdir(); tracked.write_bytes(b"original\n")
        git("add", "."); git("-c", "core.hooksPath=/dev/null", "commit", "-qm", "Synthetic frozen inputs")
        revision = git("rev-parse", "HEAD").decode().strip()
        for n in ("config.json", "driver-config.json", "asset-manifest.json"):
            (git_repo / ROOT / n).write_bytes((root / n).read_bytes())
        git("add", "."); git("-c", "core.hooksPath=/dev/null", "commit", "-qm", "Synthetic separate config freeze")
        git("update-ref", "refs/remotes/origin/main", git("rev-parse", "HEAD").decode().strip())
        git_proof = dict(proof, source_archive_commit=revision, source_archive_sha256=archive_digest(revision, git_repo))
        with patch.object(module, "CODE", (tiny,)), patch.object(module, "qualify", return_value=git_proof):
            assert preflight(git_repo) == git_proof
            own = git_repo / ROOT / "a0001/collection.log"; own.write_bytes(b"new result\n")
            assert preflight(git_repo, own.parent) == git_proof; rejected(lambda: preflight(git_repo))
            (git_repo / "foreign").write_bytes(b"foreign"); rejected(lambda: preflight(git_repo, own.parent)); (git_repo / "foreign").unlink()
            tracked.write_bytes(b"changed"); rejected(lambda: preflight(git_repo, own.parent)); tracked.write_bytes(b"original\n")
            with patch.object(module, "archive_digest", return_value="0"*64): rejected(lambda: preflight(git_repo, own.parent))
            with patch.object(module, "qualify", return_value=dict(git_proof, source_archive_commit=git("rev-parse", "HEAD").decode().strip())):
                rejected(lambda:preflight(git_repo,own.parent))
            rejected(lambda: preflight(git_repo, work / "foreign/a0001")); checks += 2
        shared, _ = lifecycle()
        # CONFIG's exact relative name is retained while CWD switches to frozen.
        cwd = Path.cwd(); os.chdir(frozen)
        try: body = user_data("a"*40, "b"*64, "sources/synthetic.tar.gz", PREFIX + "a0001", proof)
        finally: os.chdir(cwd)
        assert len(body.encode()) <= 16384
        user_data_bytes = len(body.encode())
        assert all(t in body for t in ("--on-active=4500s", "MemoryMax=12884901888", "MemorySwapMax=0", "CPUQuota=200%", "TasksMax=512", "RuntimeMaxSec=3660", "timeout --signal=TERM --kill-after=30 3600"))
        compile(BOOTSTRAP_STAGE, "<bootstrap-staging>", "exec")
        for n in driver.retained.THREAD_ENV: assert "--setenv=" + n + "=2" in body
        checks += 1
        # Execute the actual generated Python source transport against a tiny
        # tar stream, including source tampering and pre-extraction reserve.
        import tarfile
        tar_body = io.BytesIO()
        with tarfile.open(fileobj=tar_body, mode="w") as archive:
            member = tarfile.TarInfo("scripts/frozen.py"); member.size = 5
            archive.addfile(member, io.BytesIO(b"code\n"))
        archive_body = gzip.compress(tar_body.getvalue(), mtime=0)
        for mode in ("source-ok", "source-tamper", "source-scratch"):
            destination = work / mode; destination.mkdir()
            transport = S3({"source": archive_body})
            args = ["-", BUCKET, "source", "0"*64 if mode=="source-tamper" else sha(archive_body),
                str(1 if mode=="source-scratch" else SCRATCH)]
            oldcwd=Path.cwd(); os.chdir(destination)
            try:
                with patch.object(sdk,"client",return_value=transport,create=True), patch.object(sys,"argv",args):
                    if mode=="source-ok":
                        exec(compile(BOOTSTRAP_STAGE,"<generated-source-stage>","exec"),{})
                        assert read_json(destination/"bootstrap-staging.json")["source_authenticated"] is True
                    else: rejected(lambda:exec(compile(BOOTSTRAP_STAGE,"<generated-source-stage>","exec"),{}))
            finally: os.chdir(oldcwd)
            checks+=1
        # Execute the ACTUAL generated terminal and scoped uploads from default /.
        finish = body[body.index("finish() {\n"):body.index("trap finish EXIT\n")]
        upload = (*proof["artifacts"][:-1], "screen/failure.json", "screen/failure-resources.json", "screen/failure-cleanup.json", "screen/COMPLETE.json")
        for mode in ("success", "scientific-fail", "driver-failed", "upload-failed"):
            dest = work / ("finish-" + mode); dest.mkdir()
            for n in proof["artifacts"]:
                target = dest / n; target.parent.mkdir(parents=True, exist_ok=True); write(target, b"synthetic body\n")
            write(dest / "source-qualification.json", proof); write(dest / "run.log", b"original bootstrap log\n")
            stub = '''curl() { case "$*" in */api/token*) echo token;; *) echo i-synthetic;; esac; }
shutdown() { :; }
timeout() { shift 2; "$@"; }
systemd-run() { while [ "$1" != timeout ]; do shift; done; (cd /; "$@"); }
aws() { test -f "$3" || return 44; printf '%s\n' "$4" >> "$root/uploads"; if [ "$MODE" = upload-failed ] && [[ "$4" = */artifacts/profile.log ]]; then return 55; fi; }
'''
            script = (stub + "root=" + shlex.quote(str(dest)) + "; phase=" + ("scientific" if mode == "driver-failed" else "complete") +
                "; export MODE=" + mode + "; export ARTIFACT_NAMES=" + shlex.quote(" ".join(upload)) + '; cd "$root"\n' +
                finish.replace("/dev/ttyS0", str(dest / "serial")) + "\n(exit " + ("7" if mode == "driver-failed" else "0") + "); finish\n")
            result = subprocess.run(["bash", "-c", script], capture_output=True, cwd="/", timeout=10)
            terminal = read_json(dest / "terminal.json")
            assert terminal["exit_code"] == result.returncode == {"driver-failed": 7, "upload-failed": 96}.get(mode, 0), (mode, result.stderr)
            assert terminal["original_exit_code"] == (7 if mode == "driver-failed" else 0)
            uploads = (dest / "uploads").read_text().splitlines()
            assert bool([x for x in uploads if x.endswith("/artifacts/screen/COMPLETE.json")]) == (mode in ("success", "scientific-fail"))
            if mode in ("success", "scientific-fail"): assert uploads[-2].endswith("/screen/COMPLETE.json") and uploads[-1].endswith("/terminal.json")
            checks += 1
        # Execute the shared ownership/fsync/cleanup implementation without cloud.
        with redirect_stdout(io.StringIO()): shared.self_check(lifecycle_only=True)
        for mode in ("success", "fsync", "multi-ack", "multi-ack-fsync", "interrupt", "interruption", "wait"):
            ec2, s3, session = Mock(), Mock(), Mock(); session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {"Reservations": []}
            ec2.describe_subnets.return_value = {"Subnets": [{"AvailabilityZone": "synthetic-az"}]}
            ec2.describe_spot_price_history.return_value = {"SpotPriceHistory": [{"SpotPrice": "0.1", "Timestamp": datetime.now(timezone.utc)}]}
            ids = ["i-original", "i-extra"] if mode.startswith("multi-ack") else ["i-original"]
            ec2.run_instances.return_value = {"Instances": [{"InstanceId": n} for n in ids]}
            events = []; ec2.terminate_instances.side_effect = lambda **kwargs: events.append("terminate")
            def waited(**kwargs):
                events.append("wait")
                if mode == "wait": raise RuntimeError("wait failed")
            ec2.get_waiter.return_value.wait.side_effect = waited
            def collected(*args):
                assert events == ["terminate", "wait"]; events.append("collect")
                return dict(status="complete", phase="complete", exit_code=0, artifacts=dict.fromkeys(proof["artifacts"]))
            launch_proof = dict(proof, source_archive_sha256=sha(gzip.compress(b"synthetic archive", mtime=0)))
            with patch.object(module, "ROOT", work / ("launch-" + mode)), patch.object(module, "preflight", return_value=launch_proof), \
                patch.object(module, "stage_configs"), patch.object(module, "user_data", return_value="synthetic"), \
                patch.object(shared.boto3, "Session", return_value=session), \
                patch.object(subprocess, "check_output", side_effect=["", "a"*40, b"synthetic archive"]), patch.object(subprocess, "run"), \
                patch.object(shared.peer, "missing", return_value=True), patch.object(shared.peer, "put_if_absent"), \
                patch.object(module, "poll", side_effect={"interrupt": KeyboardInterrupt(), "interruption": RuntimeError("Spot interruption")}.get(mode)), \
                patch.object(module, "collect", side_effect=collected) as collector, \
                patch.object(os, "fsync", side_effect=OSError("fsync") if mode.endswith("fsync") else None), redirect_stdout(io.StringIO()):
                try: main("a0001")
                except (OSError, RuntimeError, KeyboardInterrupt): assert mode not in ("success", "multi-ack")
                else: assert mode in ("success", "multi-ack")
            ec2.run_instances.assert_called_once(); ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            if mode == "wait": collector.assert_not_called()
            else: assert events == ["terminate", "wait", "collect"]
            reservation = read_json(work / ("launch-" + mode) / "a0001/aws-reservation.json")
            assert reservation["wall_seconds"] == 4500 and reservation["compute_cap_usd"] == .625 and reservation["ebs_s3_allowance_usd"] == .15
            assert ec2.run_instances.call_args.kwargs["BlockDeviceMappings"] == [{"DeviceName": "/dev/sda1", "Ebs": {"DeleteOnTermination": True, "Encrypted": True, "VolumeSize": 80, "VolumeType": "gp3"}}]
            checks += 1
        # Real authenticated S3/repo staging, tamper/length/deadline/scratch guards.
        testroot = work / "downloads"; testroot.mkdir(); payload = b"bounded asset\n"
        with patch.object(module, "WORK_ROOT", testroot):
            transport = S3({"body": payload})
            identity = dict(bytes=len(payload), sha256=sha(payload))
            download(transport, dict(bucket=BUCKET, key="body"), testroot / "good", identity, repo, time.monotonic()+5)
            assert (testroot / "good").read_bytes() == payload
            for bad in (dict(bytes=len(payload), sha256="0"*64), dict(bytes=1, sha256=sha(payload))):
                rejected(lambda: download(transport, dict(bucket=BUCKET, key="body"), testroot / "bad", bad, repo, time.monotonic()+5))
                assert not (testroot / "bad").exists() and not (testroot / "bad.part").exists()
            rejected(lambda: download(transport, dict(bucket=BUCKET, key="body"), testroot / "expired", identity, repo, 0))
            with patch.object(module, "SCRATCH", 1): rejected(lambda: download(transport, dict(bucket=BUCKET, key="body"), testroot / "big", identity, repo, time.monotonic()+5))
            os.link(testroot / "good", testroot / "alias")
            usage = scratch_usage(testroot)
            assert usage["hardlink_aliases"] == 1 and usage["logical_path_bytes"] == 2*usage["unique_inode_bytes"]
            checks += 2
        counters = dict(cgroup="/synthetic", observer_pid=123, process_ids=[123], **{
            "memory.max": str(MEMORY), "memory.peak": "1234", "memory.swap.max": "0", "memory.swap.peak": "0",
            "memory.events": "max 0\noom 0\noom_kill 0", "memory.swap.events": "max 0\nfail 0", "cpu.max": "200000 100000",
            "cpu.stat": "usage_usec 10", "pids.max": "512", "pids.current": "1", "pids.events": "max 0"})
        versions = dict(versions=previous.prior.VERSIONS, architecture="x86_64", os_release=dict(ID="ubuntu", VERSION_ID="24.04"),
            threads=2, aws_max_attempts=1, python="3.12.0", thread_environment=dict.fromkeys(driver.retained.THREAD_ENV, "2"))
        # The actual stage starts one separate fake driver process; replay is a
        # second CLI, never another scorer/build/acquisition process.
        stub_driver = '''import json,sys
from pathlib import Path
args=sys.argv[1:]; replay=args[:1]==['--replay']
if replay: args.pop(0)
assert len(args)==4
out=Path(args[3]); events=out.parent/'invocations'
with events.open('a') as f:f.write('replay\\n' if replay else 'driver\\n')
if not replay:
 out.mkdir(); d=dict(execution_status='SUCCESS',scientific_status='FAIL',build_invocations=0)
 (out/'decision.json').write_text(json.dumps(d)); (out/'COMPLETE.json').write_text('{}')
 (out/'resources.json').write_text(json.dumps(dict(http_request_dispatch_attempts=dict(GET=0,HEAD=0,PUT=0))))
print((out/'decision.json').read_text())
'''
        for mode in ("success", "asset-tamper", "pending", "driver-failed", "interrupt", "scratch"):
            dest = work / ("stage-" + mode); dest.mkdir(); fake_repo = dest / "repo"; (fake_repo / "scripts").mkdir(parents=True)
            (fake_repo / driver.OWN).write_text(stub_driver)
            assets = {"retained/COMPLETE.json": b"{}", "qualification/binaries/check_semantic_router_scorer": b"fake-native-not-executed"}
            mf = dict(schema="borsuk-fixed48-scientific-assets-v1", authority_pending=False,
                assets={n: dict(bytes=len(b), sha256=sha(b), source=dict(bucket=BUCKET, key=n)) for n,b in assets.items()})
            dc_fake = b"{}\n"; mf_body = encoded(mf)+b"\n"
            cfg = dict(schema=SCHEMA, authority_pending=mode=="pending", execution_source=source,
                driver_config=dict(path=str(ROOT / "driver-config.json"), bytes=len(dc_fake), sha256=sha(dc_fake)),
                asset_manifest=dict(path=str(ROOT / "asset-manifest.json"), bytes=len(mf_body), sha256=sha(mf_body)))
            write(dest / "config.json", cfg)
            fake_proof = dict(proof, config_sha256=artifact(dest / "config.json")["sha256"], driver_config_sha256=sha(dc_fake),
                asset_manifest_sha256=sha(mf_body), staging_gets=4, acquisition_objects={}, prospective_dispatches=4, largest_shard_bytes=1)
            write(dest / "source-qualification.json", fake_proof)
            bodies = {config_key(sha(dc_fake)): dc_fake, config_key(sha(mf_body)): mf_body, **assets}
            if mode == "asset-tamper": bodies["retained/COMPLETE.json"] = b"bad"
            transport = S3(bodies); session = SimpleNamespace(client=lambda *args, **kwargs: transport)
            real_run = run_cli
            def ran(*args):
                if mode == "interrupt": raise KeyboardInterrupt()
                if mode == "driver-failed": return dict(exit_status=7, process_cleanup=True)
                return real_run(*args)
            with patch.object(module, "WORK_ROOT", dest), patch.object(module, "qualify", return_value=fake_proof), \
                patch.object(previous.prior, "capture_cgroup", return_value=counters), patch.object(previous.prior, "tools", return_value=versions), \
                patch.object(previous, "runtime_abi"), patch.object(sdk, "Session", return_value=session), \
                patch.object(module, "run_cli", side_effect=ran), patch.object(module, "SCRATCH", 1 if mode=="scratch" else SCRATCH):
                try: closure = stage(fake_repo, dest, PREFIX + "a0001")
                except KeyboardInterrupt: assert mode == "interrupt"
                else:
                    assert closure["closed"] == (mode=="success"), (mode, read_json(dest / "failure.json"))
                    if mode == "success":
                        assert closure["scientific_status"] == "FAIL" and (dest / "invocations").read_text() == "driver\nreplay\n"
            if mode in ("asset-tamper", "pending", "scratch"): assert not (dest / "invocations").exists()
            if mode=="driver-failed": assert read_json(dest / "scientific-closure.json")["driver_exit_code"] == 7
            checks += 1
        # Actual original process-group timeout cleanup, with a sleeping
        # stdlib child instead of a native executable.
        try:
            previous.prior.run_process([sys.executable,"-c","import time; print('owned child',flush=True); time.sleep(30)"],work/"timeout.log",.05)
        except subprocess.TimeoutExpired: checks+=1
        else: raise AssertionError("owned process timeout admitted")
        assert (work/"timeout.log").is_file()
        # Capture a real production-format synthetic driver FAIL from its own
        # self-check, in a clean interpreter that has no controller imports.
        fixture = work / "driver-fixture"
        capture = r'''import shutil,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from scripts import run_cohere_fixed48_fresh_falsifier as h
original=h.replay; captured=[]
def replay(*args):
 result=original(*args)
 if result['scientific_status']=='FAIL' and not captured:
  shutil.copytree(args[3],sys.argv[2]); captured.append(True)
 return result
h.replay=replay
print(h.self_check())
assert captured
'''
        subprocess.run([sys.executable, "-c", capture, str(repo), str(fixture)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        dc_fixture = read_json(fixture / "config.json")
        decision = read_json(fixture / "decision.json")
        assert decision["scientific_status"] == "FAIL"; checks += 1
        dest = work / "collection/a0001"; dest.mkdir(parents=True)
        proof2 = dict(proof, driver_config_sha256=artifact(fixture / "config.json")["sha256"],
            scorer_binary_sha256=read_json(fixture / "measurement-receipt.json")["binary_sha256"])
        marker = read_json(fixture / "COMPLETE.json")
        proof2["artifacts"] = [*CONTROLLER_FILES, *("screen/" + n for n in sorted(marker["files"])), "screen/COMPLETE.json"]
        proof2["artifact_roster_sha256"] = sha(encoded(proof2["artifacts"]))
        proof2.update(staging_gets=0, acquisition_objects={}, prospective_dispatches=0)
        bodies = {"screen/" + str(p.relative_to(fixture)): p.read_bytes() for p in fixture.rglob("*") if p.is_file()}
        for n in CONTROLLER_FILES: bodies[n] = b"synthetic closed log\n"
        def body(n, value): bodies[n] = encoded(value)+b"\n" if not isinstance(value, bytes) else value
        body("config.json", encoded(config)+b"\n"); proof2["config_sha256"] = sha(bodies["config.json"])
        body("driver-config.json", bodies["screen/config.json"])
        body("asset-manifest.json", manifest); proof2["asset_manifest_sha256"] = sha(bodies["asset-manifest.json"])
        body("source-qualification.json", proof2)
        body("bootstrap-staging.json", dict(source_authenticated=True, source_archive_sha256="b"*64,
            scratch_limit_bytes=SCRATCH, scratch_before_extract_bytes=1, source_repository_reserve_bytes=1))
        body("driver-replay.json", dict(passed=True, process_cleanup=True, execution_status="SUCCESS", replay_exit_code=0,
            result=decision, config_sha256=proof2["driver_config_sha256"], complete=dict(bytes=len(bodies["screen/COMPLETE.json"]), sha256=sha(bodies["screen/COMPLETE.json"]))))
        body("scientific-closure.json", dict(closed=True, process_cleanup=True, replay_passed=True, driver_invocations=1,
            build_invocations=0, driver_exit_code=0, scientific_status="FAIL", wall_seconds=.1,
            config_sha256=proof2["config_sha256"], driver_config_sha256=proof2["driver_config_sha256"], execution_source=dc_fixture["execution_source"]))
        body("staging.json", dict(closed=True, shared_hardlinks_accounted=True, peak_scratch_bytes=1,
            largest_shard_reserve_bytes=proof2["largest_shard_bytes"], driver_output_reserve_bytes=proof2["driver_output_reserve_bytes"], scratch_usage=dict(unique_inode_bytes=1,physical_allocated_bytes=1),
            files={n: {k:p[k] for k in ("bytes","sha256")} for n,p in manifest["assets"].items()}))
        body("transport-budget.json", dict(driver_actual_dispatches_verified=True, cap=256, dispatches={"staging_GET":0},
            driver_actual_dispatches=dict(GET=0,HEAD=0,PUT=0), prospective_dispatches=0))
        body("runtime-abi.json", dict(qualified=True, builder=dict(sha256=proof2["scorer_binary_sha256"])))
        body("tool-versions.json", versions); body("profile-cgroup.json", dict(closed=True,before=counters,after=counters))
        body("profile-resources.txt", b"Maximum resident set size (kbytes): 1\nExit status: 0\n")
        body("failure.json", dict(status="complete"))
        terminal = dict(schema=SCHEMA, instance_id="i-owned", source_commit="a"*40, source_archive_sha256="b"*64,
            status="complete", phase="complete", exit_code=0, original_exit_code=0,
            **{k:proof2[k] for k in TERMINAL_IDENTITIES}, artifacts={n:dict(bytes=len(b),sha256=sha(b)) for n,b in bodies.items()})
        launch = dict(instance_id="i-owned", nodes={"0":dict(instance_id="i-owned")}, prefix=PREFIX+"a0001", source_commit="a"*40, source_archive_sha256="b"*64)
        for n,v in (("aws-launch.json",launch),("aws-closeout.json",dict(state="terminated",nodes=launch["nodes"])),
            ("aws-reservation.json",dict(schema=SCHEMA,qualification=proof2,config_sha256=proof2["config_sha256"],source_commit="a"*40,source_archive_sha256="b"*64))): write(dest/n,v)
        cloud = {PREFIX+"a0001/artifacts/"+n:b for n,b in bodies.items()}
        cloud[PREFIX+"a0001/terminal.json"] = encoded(terminal)
        transport = S3(cloud)
        with patch.object(module,"ROOT",dest.parent), patch.object(module,"preflight",return_value=proof2):
            assert collect(transport,PREFIX+"a0001",dest,"i-owned","a"*40,"b"*64)==terminal
            assert read_json(dest/"collection-receipt.json")["scientific_status"]=="FAIL"
            assert not read_json(dest/"collection-receipt.json")["files"]["screen/source-order.u64"]["local_body"]
            checks += 1
            write(dest/"aws-closeout.json",dict(state="running",nodes=launch["nodes"])); transport.calls.clear()
            rejected(lambda:collect(transport,PREFIX+"a0001",dest,"i-owned","a"*40,"b"*64)); assert not transport.calls
            write(dest/"aws-closeout.json",dict(state="terminated",nodes=launch["nodes"]))
            cloud[PREFIX+"a0001/artifacts/cpu.txt"] += b"tampered"
            rejected(lambda:collect(transport,PREFIX+"a0001",dest,"i-owned","a"*40,"b"*64))
            assert read_json(dest/"collection-error.json")["execution_status"]=="FAIL"
            cloud[PREFIX+"a0001/artifacts/cpu.txt"] = bodies["cpu.txt"]
            incomplete = dict(terminal,status="failed",phase="scientific",exit_code=7,original_exit_code=7,artifacts={"cpu.txt":terminal["artifacts"]["cpu.txt"]})
            cloud[PREFIX+"a0001/terminal.json"] = encoded(incomplete)
            assert collect(transport,PREFIX+"a0001",dest,"i-owned","a"*40,"b"*64)["exit_code"]==7
            assert read_json(dest/"collection-receipt.json")["complete"] is False
            forged = dict(terminal,code_identity_sha256="0"*64); cloud[PREFIX+"a0001/terminal.json"]=encoded(forged)
            rejected(lambda:collect(transport,PREFIX+"a0001",dest,"i-owned","a"*40,"b"*64))
            checks += 1
        result = subprocess.run([sys.executable,"-O",str(repo/OWN),"--self-check"],capture_output=True,timeout=5)
        assert result.returncode!=0 and b"authority checks" in result.stderr; checks += 1
        sdk.Session.assert_not_called()
    signal.alarm(0)
    assert "numpy" not in sys.modules and "pyarrow" not in sys.modules
    assert time.monotonic()-started < 55
    return dict(schema=SCHEMA+"-self-check",passed=True,checks=checks,elapsed_seconds=time.monotonic()-started,
        peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,synthetic=True,
        native_or_real_vectors_or_truth_executed=False,user_data_bytes=user_data_bytes,launch_authority=False)


if __name__ == "__main__":
    try:
        if sys.argv[1:] == ["--help"]: print(__doc__)
        elif sys.argv[1:] == ["--self-check"]: print(json.dumps(self_check(), sort_keys=True))
        elif sys.argv[1:2] == ["--stage"]:
            assert len(sys.argv) == 5, "--stage REPO OUTPUT PREFIX"
            sys.exit(0 if stage(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4])["closed"] else 2)
        else:
            assert len(sys.argv) == 2, "usage: aNNNN | --stage REPO OUTPUT PREFIX | --self-check"
            with open("/tmp/borsuk-fixed48-scientific-spot-launch.lock", "a+") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB); main(sys.argv[1])
    except (Exception, KeyboardInterrupt) as error:
        print(type(error).__name__ + ": " + str(error), file=sys.stderr); sys.exit(2)
