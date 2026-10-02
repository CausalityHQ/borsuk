#!/usr/bin/env python3
"""Disposable infrastructure canary; never a scientific/performance gate.

Root freezes CODE, then the separate CONFIG. --contract describes that seam.
The shared Spot launcher alone owns ACKs, termination and wait-before-collect.
"""
import base64
from contextlib import contextmanager
import fcntl
import gzip
import hashlib
import inspect
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
import tempfile
import time
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import launch_cohere_fixed48_cold_http_spot as cold

runtime, science = cold.runtime, cold.science
ROOT = runtime.BASE / 'disposable-canary'
CONFIG, NAME = ROOT / 'config.json', ''
OWN = 'scripts/launch_cohere_fixed48_canary_spot.py'
MODULE = OWN[:-3].replace('/', '.')
SCHEMA = 'borsuk-fixed48-disposable-canary-v1'
PREFIX, TOKEN_PREFIX, TAG = ('research/semantic-router/20261002/fixed48-canary-',
    'fixed48-canary-', 'borsuk-fixed48-disposable-canary')
WORK_ROOT = Path('/mnt/cohere-fixed48-canary')
WALL, WORKER_SECONDS, SERVICE_SECONDS = 900, 600, 660
MEMORY, SCRATCH = 2 << 30, 2 << 30
REGION, BUCKET = cold.REGION, cold.BUCKET
INSTANCE_TYPE, IMAGE_ID, ROOT_DEVICE_NAME, SUBNET = (cold.INSTANCE_TYPE, cold.IMAGE_ID, cold.ROOT_DEVICE_NAME, cold.SUBNET)
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP, ALLOWANCE = .50, .15, .10
LIMITS = dict(machine_limit_seconds=WALL, worker_limit_seconds=WORKER_SECONDS,
    service_limit_seconds=SERVICE_SECONDS, shared_memory_bytes=MEMORY, swap_bytes=0,
    cpu_quota_percent=200, tasks_max=512, scratch_bytes=SCRATCH, requests_max=64,
    small_body_bytes=1 << 20, compute_cap_usd=COMPUTE_CAP, ebs_s3_allowance_usd=ALLOWANCE)
MANIFEST = dict(path=str(cold.MANIFEST), bytes=13203,
    sha256='ce4b1356fe069d66b547ffa3b0a5bfed53aa2988f1949deb7e4eba6e60228fef')
SMALL = 'generation/manifest.json'
CODE = tuple(sorted(set(cold.CODE) | {OWN}))
ARTIFACTS = ('config.json', 'source-qualification.json', 'bootstrap-staging.json',
    'bootstrap-sdk-ledger.jsonl', 'sdk-ledger.jsonl', 'asset-heads.json', 'small-body.json',
    'imports.json', 'cli.json', 'cli.log', 'resources.json', 'cleanup.json', 'summary.json',
    'failure.json', 'profile.log', 'profile-resources.txt')
IDENTITIES = ('config_sha256', 'code_identity_sha256', 'asset_manifest_sha256', 'artifact_roster_sha256')
SDK_VERSION = '1.40.72'
FIELDS = ('schema', 'authority_pending', 'execution_source', 'code_sha256', 'asset_manifest', 'resources', 'small_asset')
encoded, sha, artifact, write = cold.encoded, cold.sha, cold.artifact, cold.write
regular_path, read_json, lifecycle = cold.regular_path, cold.read_json, cold.lifecycle


def contract():
    return dict(schema='borsuk-fixed48-disposable-canary-contract-v1', root=str(ROOT), config=str(CONFIG),
        config_fields=list(FIELDS), config_schema=SCHEMA,
        execution_source=dict(commit='40 lowercase hex; clean pushed source commit',
            archive_sha256='64 lowercase hex of gzip.compress(git archive --format=tar COMMIT,mtime=0)'),
        asset_manifest=MANIFEST, small_asset=SMALL, resources=LIMITS, CODE=list(CODE), ARTIFACTS=list(ARTIFACTS),
        API=['contract', 'qualify', 'preflight', 'user_data', 'stage', 'collect', 'main', 'self_check'],
        cli='aNNNN | --contract | --self-check | --stage REPO OUTPUT PREFIX', result='INFRA_GO / FAIL, never scientific GO',
        source_freeze='Canary config absent from source archive and committed separately afterwards. Existing cold asset manifest is pinned independently; no source/config self-reference.',
        real_remote=['source/config SHA authentication', 'Python controller/runtime imports',
            'boto3 no-retry HEAD of every manifest S3 key', 'five repository bridges SHA authentication',
            'one generation manifest GET/full SHA', 'controller/runtime noargs usage CLI', 'cleanup/resource checks'],
        sdk_versions=dict(boto3=SDK_VERSION, botocore=SDK_VERSION), sdk_required_api='PutObject.IfNoneMatch',
        mocked_local=['S3 SDK', 'EC2 Spot lifecycle'], native_ann_calls=0, performance_measured=False, launch_authorized=False)


def qualify(base=Path('.'), config_path=None, config_sha=None):
    repo = regular_path(base).resolve()
    path = regular_path(repo / CONFIG if config_path is None else config_path)
    identity = artifact(path)
    assert 0 < identity['bytes'] <= 1 << 20 and (config_sha is None or config_sha == identity['sha256']), 'config identity'
    config = read_json(path)
    assert set(config) == set(FIELDS) and config['schema'] == SCHEMA and config['authority_pending'] is False, 'config freeze'
    assert encoded(config['resources']) == encoded(LIMITS) and config['small_asset'] == SMALL, 'bounded infrastructure protocol'
    source = config['execution_source']
    assert set(source) == {'commit', 'archive_sha256'}
    assert re.fullmatch('[0-9a-f]{40}', source['commit']) and re.fullmatch('[0-9a-f]{64}', source['archive_sha256']), 'source freeze'
    code = {n: artifact(regular_path(repo / n))['sha256'] for n in CODE}
    assert config['code_sha256'] == code, 'exact code closure'
    assert config['asset_manifest'] == MANIFEST, 'original manifest pin'
    manifest = json.loads(science.read_repo(repo, MANIFEST))
    assert manifest['schema'] == 'borsuk-fixed48-retained-cold-assets-v1' and manifest['authority_pending'] is False
    assets = manifest['assets']
    assert len(assets) == 38 and set(assets) == {*('qualification/' + n for n in cold.NATIVE_FILES),
        *('generation/' + n for n in runtime.retained.GENERATION_FILES), 'sq8.bin', *('panel/' + n for n in runtime.PANEL_FILES)}, 'exact38 roster'
    bridges, remote = {}, {}
    for name, entry in assets.items():
        cold.driver.safe_key(name)
        assert set(entry) == {'bytes', 'sha256', 'source'}
        science.pin({n: entry[n] for n in ('bytes', 'sha256')})
        transport = entry['source']
        if set(transport) == {'repo_path'}:
            body = science.read_repo(repo, dict(path=transport['repo_path'], bytes=entry['bytes'], sha256=entry['sha256']))
            bridges[name] = dict(bytes=len(body), sha256=sha(body), path=transport['repo_path'], authenticated=True)
        else:
            assert {'bucket', 'key'} <= set(transport) <= {'bucket', 'key', 'version_id', 'etag'}
            assert transport['bucket'] == BUCKET
            cold.driver.safe_key(transport['key'])
            for field in ('version_id', 'etag'):
                if field in transport:
                    assert isinstance(transport[field], str) and transport[field]
            key = transport['bucket'], transport['key']
            assert key not in remote, 'distinct remote bodies'
            remote[key] = name
    assert len(bridges) == 5 and len(remote) == 33, 'five bridges / thirty-three S3 keys'
    assert assets[SMALL]['bytes'] == 34656 and assets[SMALL]['bytes'] <= LIMITS['small_body_bytes']
    root_gates = cold.root_verification(repo)
    panel_path = runtime.BASE / 'raw-query-admission-verification/real-panel-verification.json'
    panel = read_json(repo / panel_path)
    assert panel['schema'] == 'borsuk-fixed48-real-panel-admission-v1' and panel['exit_status'] == 0
    assert panel['real_authenticated_queries'] == 64 and panel['raw_f32_and_values_preserved'] is True
    assert panel['source_sha256'] == code[runtime.OWN] and panel['native_or_network_execution'] is panel['measurement'] is False
    gates = {str(panel_path): artifact(repo / panel_path), **root_gates}
    for name, pin in panel['artifacts'].items():
        cold.driver.safe_key(name)
        assert artifact(regular_path(repo / panel_path.parent / name)) == pin, 'real-panel gate drift'
        gates[str(panel_path.parent / name)] = pin
    return dict(config_path=str(CONFIG), config_bytes=identity['bytes'], config_sha256=identity['sha256'],
        source_archive_commit=source['commit'], source_archive_sha256=source['archive_sha256'],
        code_identity_sha256=sha(encoded(code)), asset_manifest_sha256=MANIFEST['sha256'],
        artifact_roster_sha256=sha(encoded(ARTIFACTS)), campaign_schema=SCHEMA,
        admission_gates=gates, assets=assets, repository_bridges=bridges)


def preflight(base=Path('.'), collection_out=None):
    repo = regular_path(base).resolve()
    owned = None
    if collection_out is not None:
        out = regular_path(collection_out).resolve()
        assert re.fullmatch('a[0-9]{4}', out.name) and out == repo / ROOT / out.name, 'owned collection output'
        owned = str(out.relative_to(repo)) + '/'
    status = subprocess.check_output(['git', 'status', '--porcelain', '-z', '--untracked-files=all'], cwd=repo, text=True)
    assert all(owned and row.startswith('?? ') and row[3:].startswith(owned) for row in status.split('\0') if row), 'dirty frozen source'
    proof = qualify(repo)
    source = proof['source_archive_commit']
    for a, b in ((source, 'HEAD'), (source, 'origin/main'), ('HEAD', 'origin/main')):
        subprocess.run(['git', 'merge-base', '--is-ancestor', a, b], cwd=repo, check=True)
    assert (repo / CONFIG).read_bytes() == subprocess.check_output(['git', 'show', 'HEAD:' + str(CONFIG)], cwd=repo), 'uncommitted config'
    assert subprocess.run(['git', 'cat-file', '-e', source + ':' + str(CONFIG)], cwd=repo,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0, 'source archive must precede config freeze'
    for name in (*CODE, MANIFEST['path'], *proof['admission_gates'], *(v['path'] for v in proof['repository_bridges'].values())):
        assert (repo / name).read_bytes() == subprocess.check_output(['git', 'show', source + ':' + name], cwd=repo), 'frozen source/body drift: ' + name
    assert science.archive_digest(source, repo) == proof['source_archive_sha256'], 'source archive drift'
    return proof


def config_key(digest):
    assert re.fullmatch('[0-9a-f]{64}', digest)
    return 'research/semantic-router/20261002/fixed48-canary-configs/' + digest + '.json'



def sdk_guard(model=None):
    """Read the installed service model without credentials or network calls."""
    import boto3, botocore, botocore.session
    model = botocore.session.get_session().get_service_model('s3') if model is None else model
    assert 'IfNoneMatch' in model.operation_model('PutObject').input_shape.members, 'SDK lacks PutObject.IfNoneMatch'
    return dict(boto3=boto3.__version__, botocore=botocore.__version__, conditional_put=True, network_calls=0)


def bootstrap(mode, root, proof, archive_key, prefix, names):
    """Standalone authenticated bootstrap/terminal, embedded before repo imports."""
    import hashlib, json, os, shutil, sys, tarfile, time, urllib.request
    from pathlib import Path
    import boto3
    from botocore.config import Config
    root = Path(root)
    capability = sdk_guard()
    assert capability['boto3'] == capability['botocore'] == '1.40.72', 'pinned canary SDK'
    s3 = boto3.client('s3', region_name='eu-central-1', config=Config(
        retries={'total_max_attempts': 1}, connect_timeout=5, read_timeout=10))
    bucket = 'borsuk-bench-453182569524-euc1'
    def digest(body):
        return hashlib.sha256(body).hexdigest()
    def dump(path, value):
        with (root / path).open('w') as out:
            json.dump(value, out, sort_keys=True); out.write('\n'); out.flush(); os.fsync(out.fileno())
    def size():
        total, seen = 0, set()
        for base, ds, fs in os.walk(root, followlinks=False):
            for n in fs:
                p = Path(base) / n
                if p.is_symlink(): continue  # venv executables; archive links are rejected separately
                st = p.stat(); key = st.st_dev, st.st_ino
                if key not in seen:
                    seen.add(key); total += max(st.st_size, st.st_blocks * 512)
        assert total <= 2147483648, 'bootstrap scratch cap'
        return total
    ledger = root / 'bootstrap-sdk-ledger.jsonl'
    def call(operation, key, **kwargs):
        row = dict(operation=operation, key=key, bucket=bucket, attempts=0, retries=0, error=None)
        token = 'canary-' + str(time.monotonic_ns())
        def sent(**unused):
            row['attempts'] += 1
        s3.meta.events.register('before-send.s3', sent, unique_id=token)
        try:
            response = getattr(s3, operation)(Bucket=bucket, Key=key, **kwargs)
            row.update(status=response['ResponseMetadata']['HTTPStatusCode'],
                retries=response['ResponseMetadata'].get('RetryAttempts', 0))
            assert row['attempts'] == 1 and row['retries'] == 0 and row['status'] == 200, 'bootstrap SDK attempt/status'
            return response
        except BaseException as error:
            row['error'] = type(error).__name__
            raise
        finally:
            s3.meta.events.unregister('before-send.s3', unique_id=token)
            with ledger.open('ab') as out:
                out.write((json.dumps(row, sort_keys=True) + '\n').encode()); out.flush(); os.fsync(out.fileno())
    def get(key, path, length, expected):
        count, h = 0, hashlib.sha256()
        response = call('get_object', key)
        try:
            assert response['ContentLength'] == length
            with path.open('xb') as out:
                while True:
                    chunk = response['Body'].read(min(1048576, length + 1 - count))
                    if not chunk: break
                    count += len(chunk); assert count <= length
                    h.update(chunk); out.write(chunk)
                out.flush(); os.fsync(out.fileno())
            assert count == length and h.hexdigest() == expected, 'bootstrap body authentication'
        finally:
            response['Body'].close()
    try:
        if mode == 'setup':
            used = size()
            head = call('head_object', archive_key)
            length = head['ContentLength']
            assert 0 < length <= 2147483648 - used - 16777216, 'archive admission'
            get(archive_key, root / 'source.tar.gz', length, proof['source_archive_sha256'])
            reserve, seen = 0, set()
            with tarfile.open(root / 'source.tar.gz', mode='r|gz') as archive:
                for m in archive:
                    assert m.name and not m.name.startswith('/') and '..' not in Path(m.name).parts and m.name not in seen
                    assert (m.isfile() or m.isdir()) and m.size >= 0, 'source archive special file/link/size'
                    seen.add(m.name)
                    if m.isfile(): reserve += ((m.size + 4095) // 4096) * 4096
            assert size() + reserve + 16777216 <= 2147483648 and shutil.disk_usage(root).free >= reserve + 16777216, 'source extraction reserve'
            repo = root / 'repo'; repo.mkdir()
            with tarfile.open(root / 'source.tar.gz', mode='r|gz') as archive:
                for m in archive:
                    path = repo / m.name
                    if m.isdir(): path.mkdir(parents=True, exist_ok=True)
                    else:
                        path.parent.mkdir(parents=True, exist_ok=True)
                        with archive.extractfile(m) as stream, path.open('xb') as out:
                            shutil.copyfileobj(stream, out, 1048576)
                        path.chmod(m.mode & 0o777)
            (root / 'source.tar.gz').unlink()
            key = 'research/semantic-router/20261002/fixed48-canary-configs/' + proof['config_sha256'] + '.json'
            get(key, root / 'config.json', proof['config_bytes'], proof['config_sha256'])
            config = json.loads((root / 'config.json').read_bytes())
            assert config['execution_source'] == dict(commit=proof['source_archive_commit'], archive_sha256=proof['source_archive_sha256'])
            assert digest(json.dumps(config['code_sha256'], sort_keys=True, separators=(',', ':')).encode()) == proof['code_identity_sha256']
            for name, pin in config['code_sha256'].items():
                assert not Path(name).is_absolute() and '..' not in Path(name).parts
                assert digest((repo / name).read_bytes()) == pin, 'code authentication before imports'
            destination = repo / proof['config_path']
            assert not destination.exists(), 'self-referencing source/config'
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / 'config.json', destination)
            dump('source-qualification.json', proof)
            dump('bootstrap-staging.json', dict(source_archive_bytes=length, source_archive_sha256=proof['source_archive_sha256'],
                source_authenticated=True, source_extraction_bytes=reserve, source_archive_removed=True,
                config_authenticated=True, code_authenticated_before_import=True, sdk=capability, scratch_bytes=size()))
            return
        assert mode == 'finish'
        original = int(os.environ['CANARY_EXIT_CODE']); code = original
        temporary, cleanup_error = root / 'canary-temp', None
        try:
            (root / 'source.tar.gz').unlink(missing_ok=True)
            if temporary.exists(): shutil.rmtree(temporary)
        except Exception as error:
            code = 96; cleanup_error = type(error).__name__
        if not (root / 'failure.json').exists():
            dump('failure.json', dict(status='failed', phase=os.environ.get('CANARY_PHASE'), original_exit_code=original, replacement_allowed=False))
        if not (root / 'cleanup.json').exists():
            dump('cleanup.json', dict(temporary_files_removed=not temporary.exists() and not (root / 'source.tar.gz').exists(), sdk_closed=True, process_cleanup=False, worker_entered=False))
        instance = ''
        try:
            request = urllib.request.Request('http://169.254.169.254/latest/api/token', method='PUT',
                headers={'X-aws-ec2-metadata-token-ttl-seconds': '60'})
            with urllib.request.urlopen(request, timeout=3) as response: token = response.read(4096).decode()
            request = urllib.request.Request('http://169.254.169.254/latest/meta-data/instance-id',
                headers={'X-aws-ec2-metadata-token': token})
            with urllib.request.urlopen(request, timeout=3) as response: instance = response.read(4096).decode()
        except Exception: code = 96
        files, uploads = {}, []
        # The SDK ledger is uploaded last; its own PUT is counted in terminal.
        ordered = [n for n in names if n != 'bootstrap-sdk-ledger.jsonl'] + ['bootstrap-sdk-ledger.jsonl']
        for name in ordered:
            path = root / name
            if not path.is_file(): continue
            try:
                body = path.read_bytes()
                assert len(body) <= 1048576, 'terminal artifact cap'
                call('put_object', prefix + '/artifacts/' + name, Body=body, IfNoneMatch='*')
                files[name] = dict(bytes=len(body), sha256=digest(body))
                uploads.append(dict(name=name, status=200, attempts=1, retries=0))
            except Exception as error:
                code = 96; uploads.append(dict(name=name, error=type(error).__name__))
        stage_rows = []
        if (root / 'sdk-ledger.jsonl').is_file():
            stage_rows = [json.loads(r) for r in (root / 'sdk-ledger.jsonl').read_bytes().splitlines()]
        boot_rows = [json.loads(r) for r in ledger.read_bytes().splitlines()] if ledger.exists() else []
        requests = len(stage_rows) + len(boot_rows) + 1
        if requests > 64: code = 96
        terminal = dict(schema=proof['campaign_schema'], instance_id=instance,
            source_commit=proof['source_archive_commit'], source_archive_sha256=proof['source_archive_sha256'],
            **{n: proof[n] for n in ('config_sha256', 'code_identity_sha256', 'asset_manifest_sha256', 'artifact_roster_sha256')},
            phase='complete' if code == 0 else os.environ.get('CANARY_PHASE', 'failed'),
            status='complete' if code == 0 else 'failed', original_exit_code=original, exit_code=code,
            infrastructure_status='INFRA_GO' if code == 0 else 'FAIL', performance_measured=False,
            scientific_status='UNMEASURED', artifacts=files, uploads=uploads, cleanup_error=cleanup_error,
            resource_ledger=dict(s3_requests_including_terminal_put=requests, request_cap=64,
                scope='bootstrap + readonly worker SDK + artifact/terminal uploads; excludes root lifecycle/collection',
                limits=proof['resources']), terminal_put_response='confirmed only by parent collection')
        dump('terminal.json', terminal)
        print('BORSUK_TERMINAL ' + json.dumps(terminal, sort_keys=True), flush=True)
        call('put_object', prefix + '/terminal.json', Body=(root / 'terminal.json').read_bytes(), IfNoneMatch='*')
        return code
    finally:
        s3.close()


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert (commit, archive_sha) == (qualification['source_archive_commit'], qualification['source_archive_sha256'])
    assert re.fullmatch(re.escape(PREFIX) + 'a[0-9]{4}', prefix)
    cold.driver.safe_key(archive_key)
    proof = {k: qualification[k] for k in ('config_path', 'config_bytes', 'source_archive_commit', 'source_archive_sha256', 'campaign_schema', *IDENTITIES)}
    proof['resources'] = LIMITS
    packed = base64.b64encode(gzip.compress(encoded(proof), mtime=0)).decode()
    program = inspect.getsource(sdk_guard) + inspect.getsource(bootstrap) + '\nimport base64,gzip,json,sys\n' + \
        f'proof=json.loads(gzip.decompress(base64.b64decode({packed!r})))\n' + \
        f'sys.exit(bootstrap(sys.argv[1], {str(WORK_ROOT)!r}, proof, {archive_key!r}, {prefix!r}, {ARTIFACTS!r}) or 0)\n'
    body = f'''#!/bin/bash
set -Eeuo pipefail
root={shlex.quote(str(WORK_ROOT))}
mkdir -p "$root"
cd "$root"
shutdown -h +15
cat >bootstrap.py <<'BOOTSTRAP'
{program}BOOTSTRAP
cat >worker.sh <<'WORKER'
#!/bin/bash
set -Eeuo pipefail
cd {shlex.quote(str(WORK_ROOT))}
phase=bootstrap
finish() {{
  code=$?
  trap - EXIT TERM
  set +e
  sdk_python="$PWD/venv/bin/python"
  test -x "$sdk_python" || sdk_python=python3.12
  CANARY_EXIT_CODE="$code" CANARY_PHASE="$phase" "$sdk_python" bootstrap.py finish >>terminal-upload.log 2>&1
  result=$?
  cat terminal-upload.log >/dev/ttyS0 2>/dev/null || true
  shutdown -h now || true
  exit "$result"
}}
trap finish EXIT
trap 'exit 143' TERM
export AWS_MAX_ATTEMPTS=1 AWS_METADATA_SERVICE_NUM_ATTEMPTS=1 DEBIAN_FRONTEND=noninteractive LC_ALL=C
mkdir -p apt
exec >profile.log 2>&1
phase=install
if ! python3.12 -m venv "$PWD/venv"; then
  timeout --kill-after=5 60 apt-get -o Dir::Cache::archives="$PWD/apt" -qq -o Acquire::Retries=0 -o DPkg::Lock::Timeout=15 update
  timeout --kill-after=5 90 apt-get -o Dir::Cache::archives="$PWD/apt" -qq -y -o Acquire::Retries=0 -o DPkg::Lock::Timeout=15 install python3.12-venv
  python3.12 -m venv "$PWD/venv"
fi
export TMPDIR="$PWD/pip-temp"
mkdir -p "$TMPDIR"
timeout --kill-after=5 90 "$PWD/venv/bin/python" -m pip install --disable-pip-version-check --no-cache-dir --only-binary=:all: --retries=0 --timeout=10 boto3=={SDK_VERSION} botocore=={SDK_VERSION}
rm -rf "$TMPDIR"
phase=source
"$PWD/venv/bin/python" bootstrap.py setup
phase=canary
export PYTHONPATH="$PWD/repo" PYTHONDONTWRITEBYTECODE=1
/usr/bin/time -v -o profile-resources.txt timeout --signal=TERM --kill-after=5 {WORKER_SECONDS} \\
  "$PWD/venv/bin/python" -m {MODULE} --stage "$PWD/repo" "$PWD" {shlex.quote(prefix)}
phase=complete
WORKER
systemd-run --unit=cohere-fixed48-canary --wait --pipe -p MemoryMax={MEMORY} -p MemorySwapMax=0 \\
 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec={SERVICE_SECONDS} -p KillMode=control-group \\
 -p WorkingDirectory="$root" /bin/bash "$root/worker.sh"
'''
    compile(program, '<canary-bootstrap>', 'exec')
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    assert len(body.encode()) < 16384, 'EC2 user data cap'
    assert not any(n in body for n in ('cargo ', 'rustup', '--publish', '--self-check'))
    return body


def head_assets(s3, assets, ledger, out, check):
    rows, seen = [], set()
    for name, entry in sorted(assets.items()):
        source = entry['source']
        if 'repo_path' in source: continue
        key = source['bucket'], source['key']
        if key in seen: continue
        seen.add(key); check()
        kwargs = {'VersionId': source['version_id']} if 'version_id' in source else {}
        with runtime.sdk_operation(s3, 'head_object', *key, ledger, **kwargs) as (head, row):
            assert head['ResponseMetadata']['HTTPStatusCode'] == 200 and head['ContentLength'] == entry['bytes'], 'asset HEAD size/status: ' + name
            for pin, field in (('version_id', 'VersionId'), ('etag', 'ETag')):
                if pin in source: assert head.get(field) == source[pin], 'asset HEAD pinned ' + pin
            rows.append(dict(name=name, bucket=key[0], key=key[1], bytes=head['ContentLength'],
                etag=head.get('ETag'), version_id=head.get('VersionId'), metadata=head.get('Metadata', {}),
                declared_sha256=entry['sha256'], sha_authenticated=False,
                authentication='HEAD size + declared version/ETag when present; body SHA not claimed'))
        write(out / 'asset-heads.json', dict(heads=rows, distinct_keys=len(rows), complete=False))
    assert len(rows) == 33, 'all33 remote keys admitted'
    write(out / 'asset-heads.json', dict(heads=rows, distinct_keys=len(rows), complete=True))
    return rows


def cli_smoke(repo, out, check):
    rows = []
    with (out / 'cli.log').open('xb') as log:
        for module, status in ((cold.MODULE, 2), (runtime.OWN[:-3].replace('/', '.'), 1)):
            check()
            command = [sys.executable, '-m', module]
            with subprocess.Popen(command, cwd=repo, env=dict(os.environ, PYTHONPATH=str(repo),
                    PYTHONDONTWRITEBYTECODE='1', AWS_MAX_ATTEMPTS='1'), stdout=log, stderr=log) as child:
                try:
                    code = child.wait(timeout=20)
                except BaseException:
                    child.kill(); child.wait(); raise
            assert code == status, 'CLI usage exit: ' + module
            rows.append(dict(module=module, argv=[], returncode=code, expected_usage_exit=status, real=True, scientific_invocation=False))
        log.flush(); os.fsync(log.fileno())
    body = (out / 'cli.log').read_bytes()
    assert 0 < len(body) <= 1 << 20 and body.count(b'usage:') >= 2 and b'ModuleNotFoundError' not in body, 'actual CLI usage smoke'
    write(out / 'cli.json', dict(calls=rows, complete=True, logs=artifact(out / 'cli.log')))
    return rows


def stage(repo, out, prefix):
    repo, out = regular_path(repo).resolve(), regular_path(out).resolve()
    assert out == WORK_ROOT and repo == out / 'repo', 'owned whole-worker root'
    assert re.fullmatch(re.escape(PREFIX) + 'a[0-9]{4}', prefix)
    started = time.monotonic()
    resources, summary = {}, dict(infrastructure_status='FAIL', scientific_status='UNMEASURED',
        performance_measured=False, native_ann_calls=0, scientific_calls=0, closed=False)
    cleanup = dict(temporary_files_removed=False, sdk_closed=False, process_cleanup=False)
    failure = dict(status='pending', replacement_allowed=False)
    temporary, s3 = out / 'canary-temp', None
    old_term, old_alarm = signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGALRM)
    def interrupted(signum, frame):
        raise InterruptedError('worker interrupted: ' + str(signum))
    signal.signal(signal.SIGTERM, interrupted); signal.signal(signal.SIGALRM, interrupted)
    signal.setitimer(signal.ITIMER_REAL, WORKER_SECONDS)
    try:
        import boto3
        from botocore.config import Config
        capability = sdk_guard()
        cold_capability = runtime.sdk_guard()
        assert {k: cold_capability[k] for k in capability} == capability, 'cold runtime SDK parity'
        assert capability['boto3'] == capability['botocore'] == SDK_VERSION, 'worker SDK version'
        s3 = boto3.client('s3', region_name=REGION, config=Config(retries={'total_max_attempts': 1}, connect_timeout=5, read_timeout=5))
        with runtime.observe(out, LIMITS, resources, started + WORKER_SECONDS) as check:
            proof = qualify(repo, out / 'config.json')
            early = read_json(out / 'source-qualification.json')
            assert all(proof[k] == early[k] for k in ('source_archive_commit', 'source_archive_sha256', 'config_bytes', *IDENTITIES)), 'bootstrap/worker identities'
            write(out / 'source-qualification.json', dict(early, admission_gates=proof['admission_gates']))
            write(out / 'imports.json', dict(real=True, modules=[MODULE, cold.MODULE, runtime.OWN[:-3].replace('/', '.'), 'boto3', 'botocore.config'],
                python=sys.version, sdk=capability, cold_runtime_sdk=cold_capability, files={MODULE:artifact(repo / OWN), cold.MODULE:artifact(repo / cold.OWN),
                    runtime.OWN:artifact(repo / runtime.OWN)}, complete=True))
            temporary.mkdir()
            with (out / 'sdk-ledger.jsonl').open('xb') as ledger:
                heads = head_assets(s3, proof['assets'], ledger, out, check)
                pin = proof['assets'][SMALL]
                selected = next(r for r in heads if r['name'] == SMALL)
                assert pin['bytes'] <= LIMITS['small_body_bytes']
                # Shared fetch owns stream SHA, .part removal, fsync and rename.
                body = runtime.fetch(s3, pin['source'], pin, ledger, check, started + WORKER_SECONDS,
                    temporary / 'manifest.json', etag=selected['etag'])
                assert body == {k: pin[k] for k in ('bytes', 'sha256')}
                write(out / 'small-body.json', dict(name=SMALL, **body, authenticated=True, full_body=True,
                    selected_asset_gets=1, large_asset_gets=0, repository_bridges=proof['repository_bridges']))
            sdk = [json.loads(r) for r in (out / 'sdk-ledger.jsonl').read_bytes().splitlines()]
            assert len(sdk) == 34 and [r['operation'] for r in sdk] == ['head_object'] * 33 + ['get_object']
            assert all(r['sdk_http_dispatch_attempts'] == 1 and r['retry_attempts'] == 0 and r['error'] is None for r in sdk)
            assert sum(r['consumed_response_bytes'] for r in sdk) == pin['bytes']
            cli = cli_smoke(repo, out, check)
            shutil.rmtree(temporary)
            cleanup['temporary_files_removed'] = not temporary.exists()
            s3.close(); s3 = None; cleanup['sdk_closed'] = True
            check()
            summary.update(infrastructure_status='INFRA_GO', real_remote_api_operations=['HEAD', 'GET'],
                readonly_sdk_requests=len(sdk), head_keys=len(heads), repo_bridges=5, small_asset_gets=1, cli=cli,
                identities={k: proof[k] for k in (*IDENTITIES, 'source_archive_commit', 'source_archive_sha256')})
        cleanup['process_cleanup'] = True
        failure['status'] = 'complete'
    except BaseException as error:
        summary['infrastructure_status'] = 'FAIL'
        failure.update(status='failed', error_type=type(error).__name__, error=str(error))
    finally:
        try:
            try:
                if temporary.exists(): shutil.rmtree(temporary)
                cleanup['temporary_files_removed'] = not temporary.exists()
            finally:
                if s3 is not None: s3.close(); cleanup['sdk_closed'] = True
            if resources.get('cgroup', {}).get('closed'):
                before, after = resources['cgroup']['before'], resources['cgroup']['after']
                runtime.check_cgroup(before, after, LIMITS, drained=True)
                cleanup['process_cleanup'] = True
        except BaseException as error:
            summary['infrastructure_status'] = 'FAIL'
            failure.update(status='failed', cleanup_error_type=type(error).__name__, cleanup_error=str(error))
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGTERM, old_term); signal.signal(signal.SIGALRM, old_alarm)
        summary['closed'] = all(cleanup.values())
        if not summary['closed']: summary['infrastructure_status'] = 'FAIL'
        resources.update(wall_seconds=time.monotonic() - started, limits=LIMITS, source_scope='entire worker root including authenticated repository')
        write(out / 'resources.json', resources); write(out / 'cleanup.json', cleanup)
        write(out / 'failure.json', failure); write(out / 'summary.json', summary)
    return summary


def poll(ec2, s3, prefix, instance_id, started):
    while time.monotonic() - started < WALL:
        if not lifecycle()[0].peer.missing(s3, prefix + '/terminal.json'): return
        state = ec2.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0]['State']['Name']
        assert state not in ('terminated', 'shutting-down'), 'Spot worker ended without terminal; no replacement'
        time.sleep(10)
    raise TimeoutError('canary machine deadline; no replacement')


def validate_closed(out, proof, terminal):
    assert terminal['infrastructure_status'] in ('INFRA_GO', 'FAIL')
    assert terminal['performance_measured'] is False and terminal['scientific_status'] == 'UNMEASURED'
    assert terminal['resource_ledger']['limits'] == LIMITS
    assert 0 < terminal['resource_ledger']['s3_requests_including_terminal_put'] <= 64
    if terminal['infrastructure_status'] == 'FAIL':
        assert terminal['status'] == 'failed' and terminal['exit_code'] != 0
        return False
    assert terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == terminal['original_exit_code'] == 0
    assert set(terminal['artifacts']) == set(ARTIFACTS), 'complete artifact roster'
    summary, cleanup, resources = (read_json(out / n) for n in ('summary.json', 'cleanup.json', 'resources.json'))
    assert summary['infrastructure_status'] == 'INFRA_GO' and summary['closed'] is True
    assert summary['scientific_status'] == 'UNMEASURED' and summary['performance_measured'] is False
    assert summary['native_ann_calls'] == summary['scientific_calls'] == 0
    assert cleanup == dict(temporary_files_removed=True, sdk_closed=True, process_cleanup=True), 'cleanup gate'
    assert resources['limits'] == LIMITS and resources['wall_seconds'] <= WORKER_SECONDS
    assert resources['peak_scratch_bytes'] <= SCRATCH and resources['cgroup']['closed'] is True
    runtime.check_cgroup(resources['cgroup']['before'], resources['cgroup']['after'], LIMITS, drained=True)
    assert read_json(out / 'failure.json')['status'] == 'complete'
    assert artifact(out / 'config.json') == dict(bytes=proof['config_bytes'], sha256=proof['config_sha256'])
    early = read_json(out / 'source-qualification.json')
    assert all(early[n] == proof[n] for n in (*IDENTITIES, 'source_archive_commit', 'source_archive_sha256'))
    boot = read_json(out / 'bootstrap-staging.json')
    assert boot['source_authenticated'] is boot['config_authenticated'] is boot['code_authenticated_before_import'] is boot['source_archive_removed'] is True
    assert boot['source_archive_sha256'] == proof['source_archive_sha256'] and boot['scratch_bytes'] <= SCRATCH
    imports = read_json(out / 'imports.json')
    assert imports['real'] is imports['complete'] is True
    assert imports['sdk'] == dict(boto3=SDK_VERSION, botocore=SDK_VERSION, conditional_put=True, network_calls=0)
    assert boot['sdk'] == imports['sdk']
    assert {k: imports['cold_runtime_sdk'][k] for k in imports['sdk']} == imports['sdk']
    assert imports['cold_runtime_sdk']['python_executable'], 'cold runtime interpreter evidence'
    for module, path in ((MODULE, OWN), (cold.MODULE, cold.OWN), (runtime.OWN, runtime.OWN)):
        assert imports['files'][module]['sha256'] == artifact(Path(path))['sha256'], 'import identity'
    head = read_json(out / 'asset-heads.json')
    assert head['complete'] is True and head['distinct_keys'] == len(head['heads']) == 33
    expected = {n: a for n, a in proof['assets'].items() if 'key' in a['source']}
    assert {r['name'] for r in head['heads']} == set(expected)
    for row in head['heads']:
        entry = expected[row['name']]
        assert (row['bucket'], row['key'], row['bytes'], row['declared_sha256']) == (entry['source']['bucket'], entry['source']['key'], entry['bytes'], entry['sha256'])
        assert row['sha_authenticated'] is False
        for pin, field in (('version_id', 'version_id'), ('etag', 'etag')):
            if pin in entry['source']: assert row[field] == entry['source'][pin]
    small = read_json(out / 'small-body.json')
    assert small['name'] == SMALL and {n: small[n] for n in ('bytes', 'sha256')} == {n: proof['assets'][SMALL][n] for n in ('bytes', 'sha256')}
    assert small['authenticated'] is small['full_body'] is True and small['selected_asset_gets'] == 1 and small['large_asset_gets'] == 0
    assert small['repository_bridges'] == proof['repository_bridges']
    cli = read_json(out / 'cli.json')
    assert cli['complete'] is True and cli['logs'] == artifact(out / 'cli.log')
    assert [(r['module'], r['argv'], r['returncode']) for r in cli['calls']] == [(cold.MODULE, [], 2), (runtime.OWN[:-3].replace('/', '.'), [], 1)]
    assert all(r['real'] is True and r['scientific_invocation'] is False for r in cli['calls'])
    sdk = [json.loads(r) for r in (out / 'sdk-ledger.jsonl').read_bytes().splitlines()]
    assert len(sdk) == 34 and [r['operation'] for r in sdk] == ['head_object'] * 33 + ['get_object']
    assert {(r['bucket'], r['key']) for r in sdk[:-1]} == {(a['source']['bucket'], a['source']['key']) for a in expected.values()}
    assert (sdk[-1]['bucket'], sdk[-1]['key']) == (proof['assets'][SMALL]['source']['bucket'], proof['assets'][SMALL]['source']['key'])
    assert all(r['sdk_http_dispatch_attempts'] == 1 and r['retry_attempts'] == 0 and r['error'] is None and r['http_status'] == 200 for r in sdk)
    assert sum(r['consumed_response_bytes'] for r in sdk) == proof['assets'][SMALL]['bytes']
    boot_sdk = [json.loads(r) for r in (out / 'bootstrap-sdk-ledger.jsonl').read_bytes().splitlines()]
    assert all(r['attempts'] == 1 and r['retries'] == 0 and r['error'] is None and r['status'] == 200 for r in boot_sdk)
    assert [r['operation'] for r in boot_sdk[:3]] == ['head_object', 'get_object', 'get_object']
    assert boot_sdk[0]['key'] == boot_sdk[1]['key'] == 'research/native-library-check/sources/' + proof['source_archive_sha256'] + '.tar.gz'
    assert boot_sdk[2]['key'] == config_key(proof['config_sha256'])
    assert len(boot_sdk) == 3 + len(ARTIFACTS) - 1
    assert all(r['operation'] == 'put_object' for r in boot_sdk[3:])
    assert {r['key'].rsplit('/artifacts/', 1)[-1] for r in boot_sdk[3:]} == set(ARTIFACTS) - {'bootstrap-sdk-ledger.jsonl'}
    assert terminal['resource_ledger']['s3_requests_including_terminal_put'] == len(sdk) + len(boot_sdk) + 2
    assert {r['name'] for r in terminal['uploads']} == set(ARTIFACTS)
    assert all(r.get('status') == 200 and r.get('attempts') == 1 and r.get('retries') == 0 for r in terminal['uploads'])
    return True


def collect(s3, prefix, out, instance_id, commit, digest):
    out = regular_path(out).resolve()
    assert re.fullmatch(re.escape(PREFIX) + 'a[0-9]{4}', prefix) and out == (ROOT / prefix.removeprefix(PREFIX)).resolve(), 'owned collection'
    launch, close, reservation = (read_json(out / n) for n in ('aws-launch.json', 'aws-closeout.json', 'aws-reservation.json'))
    assert close['state'] == 'terminated' and close['nodes'] == launch['nodes'], 'terminate and wait BEFORE collect'
    assert instance_id == launch['instance_id'] in {n['instance_id'] for n in close['nodes'].values()}
    assert launch['prefix'] == prefix
    proof = reservation['qualification']
    assert proof == preflight(collection_out=out), 'collection source freeze'
    assert reservation['ebs_s3_allowance_usd'] == ALLOWANCE and reservation['compute_cap_usd'] == COMPUTE_CAP
    receipts = {}
    def fetch(key, pin=None):
        response = s3.get_object(Bucket=BUCKET, Key=key)
        try:
            body = response['Body'].read((1 << 20) + 1)
            assert 0 <= len(body) <= 1 << 20 and response['ContentLength'] == len(body), 'bounded collection body'
            assert response['ResponseMetadata']['HTTPStatusCode'] == 200 and response['ResponseMetadata'].get('RetryAttempts', 0) == 0
            if pin is not None: assert dict(bytes=len(body), sha256=sha(body)) == pin, 'collection SHA/length'
            return body
        finally:
            response['Body'].close()
    try:
        raw = fetch(prefix + '/terminal.json')
        terminal = runtime.retained.primitives.decode(raw); write(out / 'aws-terminal.json', raw)
        assert terminal['schema'] == SCHEMA and terminal['instance_id'] == instance_id
        for value in (terminal, launch, reservation):
            assert value['source_commit'] == commit == proof['source_archive_commit']
            assert value['source_archive_sha256'] == digest == proof['source_archive_sha256']
        assert all(terminal[n] == proof[n] for n in IDENTITIES), 'terminal identities'
        files = terminal['artifacts']
        assert set(files) <= set(ARTIFACTS) and sum(p['bytes'] for p in files.values()) <= len(ARTIFACTS) << 20
        for name, pin in files.items():
            assert set(pin) == {'bytes', 'sha256'} and type(pin['bytes']) is int and 0 <= pin['bytes'] <= 1 << 20
            assert re.fullmatch('[0-9a-f]{64}', pin['sha256'])
            body = fetch(prefix + '/artifacts/' + name, pin)
            write(out / name, body)
            receipts[name] = dict(pin, full_body_stream_verified=True)
        passed = validate_closed(out, proof, terminal)
        write(out / 'collection-receipt.json', dict(complete=passed, state='terminated', files=receipts,
            infrastructure_status='INFRA_GO' if passed else 'FAIL', scientific_status='UNMEASURED', performance_measured=False))
        return terminal
    except BaseException as error:
        write(out / 'collection-authentication-failure.json', dict(infrastructure_status='FAIL',
            scientific_status='UNMEASURED', error_type=type(error).__name__, error=str(error), authenticated_files=receipts))
        raise


@contextmanager
def lifecycle_adapter():
    """Apply canary policy without duplicating shared ACK/termination ownership."""
    shared, _ = lifecycle()
    from botocore.config import Config
    real_session, real_dumps = shared.boto3.Session, json.dumps
    def session(*args, **kwargs):
        value = real_session(*args, **kwargs)
        real_client = value.client
        def client(service, *a, **kw):
            kw['config'] = Config(retries={'total_max_attempts': 1}, connect_timeout=5, read_timeout=10)
            result = real_client(service, *a, **kw)
            if service == 'ec2':
                run = result.run_instances
                def launch(**params):
                    params['MetadataOptions'] = dict(HttpTokens='required', HttpEndpoint='enabled', HttpPutResponseHopLimit=1)
                    return run(**params)
                result.run_instances = launch
            return result
        value.client = client
        return value
    def dumps(value, *args, **kwargs):
        if isinstance(value, dict) and value.get('schema') == SCHEMA and 'ebs_s3_allowance_usd' in value:
            value = dict(value, ebs_s3_allowance_usd=ALLOWANCE)
        return real_dumps(value, *args, **kwargs)
    with patch.object(shared.boto3, 'Session', side_effect=session), patch.object(shared.json, 'dumps', side_effect=dumps), \
            patch.dict(os.environ, AWS_MAX_ATTEMPTS='1', AWS_METADATA_SERVICE_NUM_ATTEMPTS='1'):
        yield shared


def stage_config(proof):
    from botocore.config import Config
    from botocore.exceptions import ClientError
    sdk_guard()
    body = CONFIG.read_bytes()
    assert dict(bytes=len(body), sha256=sha(body)) == dict(bytes=proof['config_bytes'], sha256=proof['config_sha256'])
    s3 = lifecycle()[0].boto3.Session(profile_name='causality', region_name=REGION).client('s3',
        config=Config(retries={'total_max_attempts': 1}, connect_timeout=5, read_timeout=5))
    try:
        key = config_key(proof['config_sha256'])
        try: response = s3.get_object(Bucket=BUCKET, Key=key)
        except ClientError as error:
            if error.response.get('Error', {}).get('Code') not in {'NoSuchKey', '404', 'NotFound'}: raise
            s3.put_object(Bucket=BUCKET, Key=key, Body=body, IfNoneMatch='*')
        else:
            try:
                remote = response['Body'].read((1 << 20) + 1)
                assert remote == body and response['ContentLength'] == len(body), 'immutable config drift'
            finally: response['Body'].close()
    finally: s3.close()


def main(attempt):
    assert re.fullmatch('a[0-9]{4}', attempt)
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        proof = preflight()
        stage_config(proof)
        with lifecycle_adapter() as shared:
            return shared.main(attempt, campaign=sys.modules[__name__])
    finally: os.chdir(before)


def self_check():
    """Actual source/metadata; fake SDK/EC2 only. No native/build/cloud activity."""
    import copy
    import tarfile
    from contextlib import redirect_stdout
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from unittest.mock import Mock
    import boto3
    from botocore.exceptions import ClientError
    module = sys.modules[__name__]
    repo = Path(__file__).resolve().parents[1]
    config = dict(schema=SCHEMA, authority_pending=False, execution_source=dict(commit='a' * 40, archive_sha256='b' * 64),
        code_sha256={n: artifact(repo / n)['sha256'] for n in CODE}, asset_manifest=MANIFEST, resources=LIMITS, small_asset=SMALL)
    checks = []
    def rejected(action):
        try: action()
        except (AssertionError, FileNotFoundError, RuntimeError, OSError): return
        raise AssertionError('negative case accepted')
    with tempfile.TemporaryDirectory(prefix='fixed48-canary-check-') as tmp, \
            patch.object(boto3, 'Session', side_effect=AssertionError('cloud forbidden')) as cloud:
        work = Path(tmp)
        config_path = work / 'config.json'; write(config_path, config)
        proof = qualify(repo, config_path)
        assert len(proof['assets']) == 38 and len(proof['repository_bridges']) == 5
        for key, value in (('resources', dict(LIMITS, requests_max=65)), ('small_asset', 'sq8.bin'),
                ('asset_manifest', dict(MANIFEST, sha256='0' * 64)), ('authority_pending', True),
                ('code_sha256', dict(config['code_sha256'], **{OWN:'0' * 64}))):
            write(config_path, dict(config, **{key:value})); rejected(lambda: qualify(repo, config_path))
        write(config_path, config); rejected(lambda: qualify(repo, config_path, '0' * 64))
        checks.append('actual-source/gates/manifest/config-tamper')
        assert sdk_guard()['conditional_put'] is True
        from botocore.model import ServiceModel
        from botocore.session import get_session
        old_model = copy.deepcopy(get_session().get_component('data_loader').load_service_model('s3', 'service-2'))
        shape = old_model['operations']['PutObject']['input']['shape']
        old_model['shapes'][shape]['members'].pop('IfNoneMatch')
        rejected(lambda: sdk_guard(ServiceModel(old_model)))
        checks.append('real-installed-SDK-conditional-PUT-model/missing-capability-rejected')

        class Events:
            def __init__(self): self.handlers = {}
            def register(self, event, handler, unique_id): self.handlers[unique_id] = (event, handler)
            def unregister(self, event, unique_id): self.handlers.pop(unique_id)
            def send(self):
                for event, handler in list(self.handlers.values()):
                    if event == 'before-send.s3': handler()
        class S3:
            def __init__(self, mode='success'):
                self.meta = SimpleNamespace(events=Events())
                self.mode, self.calls, self.closed, self.streams, self.bodies = mode, [], False, [], {}
            def response(self, operation, key):
                self.meta.events.send(); self.calls.append((operation, key))
                return dict(ResponseMetadata=dict(HTTPStatusCode=200, RetryAttempts=0))
            def head_object(self, Bucket, Key, **kwargs):
                response = self.response('head_object', Key)
                if self.mode == 'missing': raise FileNotFoundError('missing S3 key')
                if Key in self.bodies:
                    response.update(ContentLength=len(self.bodies[Key]), ETag='"canary"', Metadata={})
                    return response
                entry = next(a for a in proof['assets'].values() if a['source'].get('key') == Key)
                response.update(ContentLength=entry['bytes'] + (1 if self.mode == 'size' else 0), ETag='"canary"', Metadata={})
                if 'version_id' in entry['source']: response['VersionId'] = entry['source']['version_id']
                return response
            def get_object(self, Bucket, Key, **kwargs):
                response = self.response('get_object', Key)
                body = self.bodies.get(Key)
                if body is None:
                    selected = proof['assets'][SMALL]
                    assert Key == selected['source']['key'], 'large/body GET forbidden'
                    body = (repo / runtime.BASE / 'artifact-reproduction/a0002/screen/generation/manifest.json').read_bytes()
                    if self.mode == 'tamper': body = b'!' + body[1:]
                stream = io.BytesIO(body); self.streams.append(stream)
                response.update(Body=stream, ContentLength=len(body), ETag='"canary"')
                return response
            def put_object(self, Bucket, Key, Body, **kwargs):
                response = self.response('put_object', Key)
                if self.mode == 'upload-fail': raise OSError('upload failure')
                self.bodies[Key] = bytes(Body)
                return response
            def close(self): self.closed = True

        for mode in ('success', 'missing', 'size', 'tamper'):
            out = work / ('heads-' + mode); out.mkdir()
            sdk = S3(mode)
            with (out / 'ledger').open('xb') as ledger:
                if mode in ('missing', 'size'):
                    rejected(lambda: head_assets(sdk, proof['assets'], ledger, out, lambda:None))
                    assert len(sdk.calls) == 1
                else:
                    rows = head_assets(sdk, proof['assets'], ledger, out, lambda:None)
                    assert len(rows) == 33 and all(not r['sha_authenticated'] for r in rows)
                    pin = proof['assets'][SMALL]
                    action = lambda: runtime.fetch(sdk, pin['source'], pin, ledger, lambda *args:None,
                        time.monotonic() + 10, out / 'small', etag='"canary"')
                    if mode == 'tamper': rejected(action)
                    else: assert action() == {n: pin[n] for n in ('bytes', 'sha256')}
                    assert len(sdk.calls) == 34 and all(s.closed for s in sdk.streams)
                    assert not (out / 'small.part').exists()
        pinned = copy.deepcopy(proof['assets']); pinned[SMALL]['source']['etag'] = '"different"'
        out = work / 'etag'; out.mkdir()
        with (out / 'ledger').open('xb') as ledger:
            rejected(lambda: head_assets(S3(), pinned, ledger, out, lambda:None))
        checks.append('all33-heads/no-SHA-metadata/size/missing/ETag/smallbody-tamper/part-cleanup')

        shell = user_data('a' * 40, 'b' * 64, 'research/native-library-check/sources/' + 'b' * 64 + '.tar.gz', PREFIX + 'a0001', proof)
        assert len(shell.encode()) < 16384 and '--stage' in shell and 'RuntimeMaxSec=660' in shell
        assert all(v in shell for v in ('MemoryMax=2147483648', 'MemorySwapMax=0', 'CPUQuota=200%', 'TasksMax=512'))
        checks.append('generated-Bash/Python-syntax/bounded-resources')

        # Both CLI commands are real Python usage exits from the current code.
        out = work / 'cli'; out.mkdir(); cli_smoke(repo, out, lambda:None)
        checks.append('actual-controller/runtime-noargs-CLI')

        def snap():
            return dict(cgroup='/synthetic', **{'memory.max':str(MEMORY), 'memory.peak':'10000',
                'memory.swap.max':'0', 'memory.swap.peak':'0', 'cpu.max':'200000 100000',
                'pids.max':'512', 'pids.current':'1', 'memory.events':'oom 0\noom_kill 0\nmax 0\n',
                'memory.swap.events':'max 0\n', 'pids.events':'max 0\n', 'process_ids':[os.getpid()]})
        before = snap(); runtime.check_cgroup(before, snap(), LIMITS, drained=True)
        for name, value in (('memory.max', str(MEMORY + 1)), ('memory.swap.peak', '1'), ('pids.max', '513'), ('memory.events', 'oom 1\n')):
            changed = dict(before, **{name:value}); rejected(lambda: runtime.check_cgroup(before, changed, LIMITS, drained=True))
        checks.append('real-cgroup-validator/rejected-resource-drift')

        for mode in ('success', 'missing', 'tamper', 'cli', 'cleanup'):
            out = work / ('worker-' + mode); out.mkdir(); (out / 'repo').mkdir()
            for name in (OWN, cold.OWN, runtime.OWN):
                target = out / 'repo' / name; target.parent.mkdir(parents=True, exist_ok=True); shutil.copyfile(repo / name, target)
            write(out / 'source-qualification.json', dict(proof, resources=LIMITS))
            sdk = S3('missing' if mode == 'missing' else 'tamper' if mode == 'tamper' else 'success')
            real_rmtree = shutil.rmtree
            def remove(path, *a, **kw):
                if mode == 'cleanup' and Path(path).name == 'canary-temp': raise OSError('cleanup rejected')
                return real_rmtree(path, *a, **kw)
            def cli_fake(*args):
                if mode == 'cli': raise AssertionError('CLI failure')
                return []
            with patch.object(module, 'WORK_ROOT', out), patch.object(module, 'qualify', return_value=proof), \
                    patch.object(boto3, 'client', return_value=sdk), patch.object(runtime, 'snapshot', side_effect=snap), \
                    patch.object(module, 'cli_smoke', side_effect=cli_fake), patch.object(shutil, 'rmtree', side_effect=remove):
                result = stage(out / 'repo', out, PREFIX + 'a0001')
            assert result['infrastructure_status'] == ('INFRA_GO' if mode == 'success' else 'FAIL')
            assert result['scientific_status'] == 'UNMEASURED' and sdk.closed, (mode, read_json(out / 'failure.json'))
            assert (out / 'failure.json').is_file() and (out / 'resources.json').is_file()
            assert (out / 'canary-temp').exists() == (mode == 'cleanup')
        checks.append('executable-worker/success/missing/tamper/CLI/cleanup-failure')

        # Shared main owns every ACK, fsync failure, teardown and collection.
        shared, _ = lifecycle()
        for mode in ('success', 'multi-ack', 'fsync', 'poll', 'wait'):
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {'Reservations':[]}
            ec2.describe_subnets.return_value = {'Subnets':[{'AvailabilityZone':'synthetic-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory':[dict(SpotPrice='0.1', Timestamp=datetime.now(timezone.utc))]}
            ids = ['i-owned', 'i-extra'] if mode == 'multi-ack' else ['i-owned']
            ec2.run_instances.return_value = {'Instances':[{'InstanceId':n} for n in ids]}
            launch_call = ec2.run_instances
            events = []
            ec2.terminate_instances.side_effect = lambda **kwargs: events.append('terminate')
            def waited(**kwargs):
                events.append('wait')
                if mode == 'wait': raise RuntimeError('wait failed')
            ec2.get_waiter.return_value.wait.side_effect = waited
            def collected(*args):
                assert events == ['terminate', 'wait']; events.append('collect')
                return dict(status='complete', phase='complete', exit_code=0, artifacts=dict.fromkeys(ARTIFACTS))
            launch_proof = dict(proof, source_archive_sha256=sha(gzip.compress(b'synthetic archive', mtime=0)))
            with patch.object(module, 'ROOT', work / ('launch-' + mode)), patch.object(module, 'preflight', return_value=launch_proof), \
                    patch.object(module, 'stage_config'), patch.object(module, 'user_data', return_value='synthetic'), \
                    patch.object(shared.boto3, 'Session', return_value=session), \
                    patch.object(subprocess, 'check_output', side_effect=['', 'a'*40, b'synthetic archive']), patch.object(subprocess, 'run'), \
                    patch.object(shared.peer, 'missing', return_value=True), patch.object(shared.peer, 'put_if_absent'), \
                    patch.object(module, 'poll', side_effect=RuntimeError('poll failed') if mode == 'poll' else None), \
                    patch.object(module, 'collect', side_effect=collected) as collector, \
                    patch.object(os, 'fsync', side_effect=OSError('fsync') if mode == 'fsync' else None), redirect_stdout(io.StringIO()):
                if mode in ('success', 'multi-ack'): main('a0001')
                else: rejected(lambda: main('a0001'))
            ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            if mode == 'wait': collector.assert_not_called()
            else: assert events == ['terminate', 'wait', 'collect']
            params = launch_call.call_args.kwargs
            assert params['MetadataOptions']['HttpTokens'] == 'required' and params['ImageId'] == IMAGE_ID
            assert params['BlockDeviceMappings'] == [dict(DeviceName=ROOT_DEVICE_NAME, Ebs=dict(DeleteOnTermination=True, Encrypted=True, VolumeSize=80, VolumeType='gp3'))]
            reservation = read_json(work / ('launch-' + mode) / 'a0001/aws-reservation.json')
            assert reservation['compute_cap_usd'] == .15 and reservation['ebs_s3_allowance_usd'] == .10
        checks.append('shared-lifecycle/all-ACK/fsync/terminate-wait-before-collect/IMDSv2/no-replacement')
        # Run the embedded source/config admission code, not a bootstrap mock.
        for mode in ('success', 'source-tamper', 'config-tamper', 'code-tamper', 'self-reference', 'link'):
            out = work / ('bootstrap-' + mode); out.mkdir()
            if mode == 'success': (out / 'venv-python').symlink_to(sys.executable)
            content = b'print(1)\n'
            raw = io.BytesIO()
            with tarfile.open(fileobj=raw, mode='w') as tar:
                member = tarfile.TarInfo('scripts/source.py')
                body = b'print(2)\n' if mode == 'code-tamper' else content
                member.size = len(body); tar.addfile(member, io.BytesIO(body))
                if mode == 'self-reference':
                    member = tarfile.TarInfo(str(CONFIG)); member.size = 2; tar.addfile(member, io.BytesIO(b'{}'))
                if mode == 'link':
                    member = tarfile.TarInfo('escape'); member.type = tarfile.SYMTYPE; member.linkname = '/tmp'; tar.addfile(member)
            archive = gzip.compress(raw.getvalue(), mtime=0)
            binding = dict(commit='a'*40, archive_sha256=sha(archive))
            fixture = dict(execution_source=binding, code_sha256={'scripts/source.py':sha(content)})
            config_body = encoded(fixture)
            boot_proof = dict(proof, source_archive_sha256=sha(archive), config_sha256=sha(config_body), config_bytes=len(config_body),
                code_identity_sha256=sha(encoded(fixture['code_sha256'])), resources=LIMITS)
            archive_key = 'research/native-library-check/sources/' + sha(archive) + '.tar.gz'
            sdk = S3()
            sdk.bodies = {archive_key:archive, config_key(sha(config_body)):config_body}
            if mode == 'source-tamper': sdk.bodies[archive_key] = b'!' + archive[1:]
            if mode == 'config-tamper': sdk.bodies[config_key(sha(config_body))] = b'!' + config_body[1:]
            with patch.object(boto3, 'client', return_value=sdk):
                action = lambda: bootstrap('setup', out, boot_proof, archive_key, PREFIX+'a0001', ARTIFACTS)
                if mode == 'success':
                    action(); assert read_json(out / 'bootstrap-staging.json')['code_authenticated_before_import'] is True
                    assert not (out / 'source.tar.gz').exists()
                else: rejected(action)
            assert sdk.closed and all(stream.closed for stream in sdk.streams)
        checks.append('actual-embedded-bootstrap/source-config-code-tamper/self-reference/special-file-rejection')

        # Failure terminal upload remains real implementation, with SDK + IMDS mocked.
        out = work / 'terminal-failure'; out.mkdir(); (out / 'source.tar.gz').write_bytes(b'partial')
        (out / 'canary-temp').mkdir()
        sdk = S3()
        import urllib.request
        boot_proof = dict(proof, resources=LIMITS)
        with patch.object(boto3, 'client', return_value=sdk), patch.object(urllib.request, 'urlopen',
                side_effect=[io.BytesIO(b'token'), io.BytesIO(b'i-owned')]), \
                patch.dict(os.environ, CANARY_EXIT_CODE='7', CANARY_PHASE='source'), redirect_stdout(io.StringIO()):
            status = bootstrap('finish', out, boot_proof, 'source-key', PREFIX+'a0001', ARTIFACTS)
        terminal = json.loads(sdk.bodies[PREFIX+'a0001/terminal.json'])
        assert status == terminal['exit_code'] == terminal['original_exit_code'] == 7
        assert terminal['infrastructure_status'] == 'FAIL' and terminal['scientific_status'] == 'UNMEASURED'
        assert sdk.closed and not (out / 'source.tar.gz').exists() and not (out / 'canary-temp').exists()
        assert terminal['resource_ledger']['s3_requests_including_terminal_put'] <= 64
        checks.append('failure-terminal-upload/source-temp-cleanup/request-ledger')
        # Collect a terminal failure only after the shared durable closeout.
        with patch.object(module, 'ROOT', work / 'collection'), patch.object(module, 'preflight', return_value=proof):
            dest = module.ROOT / 'a0001'; dest.mkdir(parents=True)
            nodes = {'0':dict(instance_id='i-owned')}
            launch = dict(instance_id='i-owned', nodes=nodes, prefix=PREFIX+'a0001', source_commit='a'*40, source_archive_sha256='b'*64)
            write(dest / 'aws-launch.json', launch)
            write(dest / 'aws-closeout.json', dict(state='terminated', nodes=nodes))
            write(dest / 'aws-reservation.json', dict(source_commit='a'*40, source_archive_sha256='b'*64,
                qualification=proof, compute_cap_usd=COMPUTE_CAP, ebs_s3_allowance_usd=ALLOWANCE))
            collected = collect(sdk, PREFIX+'a0001', dest, 'i-owned', 'a'*40, 'b'*64)
            assert collected['exit_code'] == 7 and read_json(dest / 'collection-receipt.json')['infrastructure_status'] == 'FAIL'
            write(dest / 'aws-closeout.json', dict(state='running', nodes=nodes))
            calls = len(sdk.calls)
            rejected(lambda: collect(sdk, PREFIX+'a0001', dest, 'i-owned', 'a'*40, 'b'*64))
            assert len(sdk.calls) == calls
            write(dest / 'aws-closeout.json', dict(state='terminated', nodes=nodes))
            key = PREFIX+'a0001/artifacts/failure.json'
            sdk.bodies[key] = b'!' + sdk.bodies[key][1:]
            rejected(lambda: collect(sdk, PREFIX+'a0001', dest, 'i-owned', 'a'*40, 'b'*64))
            assert read_json(dest / 'collection-authentication-failure.json')['infrastructure_status'] == 'FAIL'
        checks.append('actual-collector/failure-preserved/closeout-required/body-tamper-rejected')

        # Finish still uploads a failed terminal when temporary cleanup itself fails.
        out = work / 'terminal-cleanup-failure'; out.mkdir(); (out / 'canary-temp').mkdir()
        sdk = S3()
        with patch.object(boto3, 'client', return_value=sdk), patch.object(urllib.request, 'urlopen',
                side_effect=[io.BytesIO(b'token'), io.BytesIO(b'i-owned')]), \
                patch.object(shutil, 'rmtree', side_effect=OSError('cleanup failure')), \
                patch.dict(os.environ, CANARY_EXIT_CODE='7', CANARY_PHASE='canary'), redirect_stdout(io.StringIO()):
            status = bootstrap('finish', out, boot_proof, 'source-key', PREFIX+'a0001', ARTIFACTS)
        terminal = json.loads(sdk.bodies[PREFIX+'a0001/terminal.json'])
        assert status == terminal['exit_code'] == 96 and terminal['cleanup_error'] == 'OSError' and sdk.closed
        checks.append('terminal-survives-cleanup-failure')

        # Authenticate the complete successful worker/terminal/collector chain.
        out = work / 'worker-success'
        write(out / 'config.json', config)
        cli_smoke(repo, out, lambda:None)
        write(out / 'bootstrap-staging.json', dict(source_authenticated=True, config_authenticated=True,
            code_authenticated_before_import=True, source_archive_removed=True, source_archive_sha256='b'*64,
            sdk=sdk_guard(), scratch_bytes=10000))
        write(out / 'profile.log', b'canary local integration fixture\n')
        write(out / 'profile-resources.txt', b'remote process time mocked for local integration\n')
        source_key = 'research/native-library-check/sources/' + 'b'*64 + '.tar.gz'
        with (out / 'bootstrap-sdk-ledger.jsonl').open('wb') as ledger:
            for operation, key in (('head_object', source_key), ('get_object', source_key), ('get_object', config_key(proof['config_sha256']))):
                ledger.write(encoded(dict(operation=operation, key=key, bucket=BUCKET, attempts=1, retries=0, error=None, status=200)) + b'\n')
        sdk = S3()
        with patch.object(boto3, 'client', return_value=sdk), patch.object(urllib.request, 'urlopen',
                side_effect=[io.BytesIO(b'token'), io.BytesIO(b'i-owned')]), \
                patch.dict(os.environ, CANARY_EXIT_CODE='0', CANARY_PHASE='complete'), redirect_stdout(io.StringIO()):
            status = bootstrap('finish', out, dict(proof, resources=LIMITS), source_key, PREFIX+'a0001', ARTIFACTS)
        terminal = json.loads(sdk.bodies[PREFIX+'a0001/terminal.json'])
        assert status == 0 and len(terminal['artifacts']) == 16
        with patch.object(module, 'ROOT', work / 'success-collection'), patch.object(module, 'preflight', return_value=proof):
            dest = module.ROOT / 'a0001'; dest.mkdir(parents=True)
            write(dest / 'aws-launch.json', launch)
            write(dest / 'aws-closeout.json', dict(state='terminated', nodes=nodes))
            write(dest / 'aws-reservation.json', dict(source_commit='a'*40, source_archive_sha256='b'*64,
                qualification=proof, compute_cap_usd=COMPUTE_CAP, ebs_s3_allowance_usd=ALLOWANCE))
            assert collect(sdk, PREFIX+'a0001', dest, 'i-owned', 'a'*40, 'b'*64)['infrastructure_status'] == 'INFRA_GO'
            assert read_json(dest / 'collection-receipt.json')['complete'] is True
            bad = copy.deepcopy(terminal); bad['resource_ledger']['s3_requests_including_terminal_put'] += 1
            rejected(lambda: validate_closed(dest, proof, bad))
            write(dest / 'cleanup.json', dict(temporary_files_removed=True, sdk_closed=True, process_cleanup=False))
            rejected(lambda: validate_closed(dest, proof, terminal))
        checks.append('actual-full-success-terminal-collector/request-count/cleanup-rejected')
        cloud.assert_not_called()
    return dict(self_check=True, checks=checks, infrastructure_claim='local verification only; disposable remote gate still required',
        source_code_identity_sha256=sha(encoded(config['code_sha256'])), real=['local code/proof/manifest identities', 'controller/runtime CLI usage exits', 'shared validators'],
        mocked=['S3 SDK', 'EC2 lifecycle', 'remote cgroup snapshots', 'worker qualification/CLI wiring; each checked separately on actual source'], native_calls=0, network_calls=0, performance_measured=False)


if __name__ == '__main__':
    try:
        if sys.argv[1:] == ['--self-check']:
            print(json.dumps(self_check(), sort_keys=True))
        elif sys.argv[1:] == ['--contract']:
            print(json.dumps(contract(), indent=2))
        elif sys.argv[1:2] == ['--stage']:
            assert len(sys.argv) == 5, '--stage REPO OUTPUT PREFIX'
            sys.exit(0 if stage(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4])['infrastructure_status'] == 'INFRA_GO' else 2)
        else:
            assert len(sys.argv) == 2, 'usage: aNNNN | --contract | --self-check'
            with open('/tmp/borsuk-fixed48-canary-spot-launch.lock', 'a+') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                main(sys.argv[1])
    except (Exception, KeyboardInterrupt) as error:
        print(type(error).__name__ + ': ' + str(error), file=sys.stderr)
        sys.exit(2)
