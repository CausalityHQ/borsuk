#!/usr/bin/env python3
"""One retained generation: aNNNN | --stage REPO OUTPUT PREFIX | --self-check.

Root freezes CODE/archive first, then commits only CONFIG (exact helper schema).
No launch until both are origin-backed. Config is separately embedded/authenticated;
workers need no Git. Collection streams every body after owned terminate+wait,
retaining small receipts locally and explicit large-body S3 pointers. Helper
--replay works after downloading all screen bodies; local SQ8 ETag is provenance,
not an S3 ETag. No second build, query, GT, search, or replacement is authorized.
"""
import base64
import fcntl
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import signal
import struct
import subprocess
import sys
import time
import zlib
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import reproduce_cohere_semantic_artifacts as h
from scripts import launch_cohere_top32_coverage_spot as prior

ROOT = Path(h.BASE) / 'fixed48/artifact-reproduction'
CONFIG, NAME = ROOT / 'config.json', ''
OWN = 'scripts/launch_cohere_semantic_artifact_reproduction_spot.py'
MODULE = OWN[:-3].replace('/', '.')
SCHEMA = 'borsuk-cohere-semantic-artifact-reproduction-spot-v1'
PREFIX = 'research/semantic-router/20261002/fixed48-artifact-reproduction-'
TOKEN_PREFIX, TAG = 'fixed48-artifact-', 'borsuk-fixed48-artifact-reproduction'
WALL, WORKER_SECONDS, SERVICE_SECONDS = 3600, 1800, 1860
MEMORY, SCRATCH = 12 << 30, 16 << 30
INSTANCE_TYPE, IMAGE_ID = prior.INSTANCE_TYPE, prior.IMAGE_ID
ROOT_DEVICE_NAME, SUBNET = prior.ROOT_DEVICE_NAME, prior.SUBNET
REGION, BUCKET = prior.REGION, prior.BUCKET
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, .50
AWSCLI_VERSION, AWSCLI_SHA256 = prior.AWSCLI_VERSION, prior.AWSCLI_SHA256
CODE = tuple(sorted(set((*h.CODE, *prior.CODE, OWN))))
CONTROLLER_FILES = ('config.json', 'source-qualification.json', 'archived-builder-assurance.json',
    'runtime-abi.json', 'cpu.txt', 'tool-versions.json', 'run-closed.log', 'profile.log',
    'profile-resources.txt', 'helper-process.log', 'profile-cgroup.json', 'reproduction-closure.json', 'failure.json')
ARTIFACTS = (*CONTROLLER_FILES, *('screen/' + n for n in h.RETAINED_FILES), 'screen/COMPLETE.json')
UPLOAD_FILES = (*ARTIFACTS, 'screen/failure.json', 'screen/failure-resources.json')
TERMINAL_IDENTITIES = ('config_sha256', 'code_identity_sha256', 'refs_identity_sha256',
    'artifact_roster_sha256', 'builder_assurance_sha256', 'builder_binary_sha256',
    'campaign_schema', 'awscli_version', 'awscli_sha256')
encoded, sha, artifact, write = prior.encoded, prior.sha, prior.artifact, prior.write
LOCAL_BYTES = 1 << 20
MAX_BODY_BYTES = 16 << 30


def qualify(base=Path('.'), config_path=None, config_sha=None):
    base = Path(base).resolve()
    path = base / CONFIG if config_path is None else Path(config_path)
    path = h.regular_path(path)
    assert path.is_file() and path.stat().st_size <= LOCAL_BYTES, 'bounded config'
    body = path.read_bytes()
    digest = sha(body) if config_sha is None else config_sha
    assert isinstance(h.primitives.decode(body).get('execution_source'), dict), 'source freeze pending'
    config = h.read_config(path, digest, base)
    _, binary, assurance = h.authorities(config, base)  # Read-only, no corpus/native call.
    code = {n: artifact(h.regular_path(base / n))['sha256'] for n in CODE}
    assert all(code[n] == d for n, d in config['code_sha256'].items())
    return dict(config_path=str(CONFIG), config_sha256=digest,
        source_archive_commit=config['execution_source']['commit'],
        source_archive_sha256=config['execution_source']['archive_sha256'],
        code_identity_sha256=sha(encoded(code)), refs_identity_sha256=sha(encoded(config['refs'])),
        artifact_roster_sha256=sha(encoded(ARTIFACTS)), builder_assurance_sha256=sha(encoded(assurance)),
        builder_binary_sha256=sha(binary), campaign_schema=SCHEMA,
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)


def archive_digest(commit, base):
    """Stream exactly gzip.compress(git archive, mtime=0), including Python's header."""
    digest = hashlib.sha256(gzip.compress(b'', mtime=0)[:10])
    compressor, crc, length = zlib.compressobj(9, zlib.DEFLATED, -15), 0, 0
    with subprocess.Popen(['git', 'archive', '--format=tar', commit], cwd=base,
                          stdout=subprocess.PIPE) as process:
        try:
            for chunk in iter(lambda: process.stdout.read(1 << 20), b''):
                crc, length = zlib.crc32(chunk, crc), length + len(chunk)
                digest.update(compressor.compress(chunk))
            assert process.wait(timeout=30) == 0, 'code archive failed'
        except BaseException:
            process.kill(); process.wait(); raise
    digest.update(compressor.flush())
    digest.update(struct.pack('<II', crc & 0xffffffff, length & 0xffffffff))
    return digest.hexdigest()


def preflight(base=Path('.'), collection_out=None):
    base = Path(base).resolve()
    owned_prefix = None
    if collection_out is not None:
        collection_out = h.regular_path(collection_out)
        assert re.fullmatch(r'a[0-9]{4}', collection_out.name)
        assert collection_out == h.regular_path(base / ROOT / NAME / collection_out.name), 'unowned collection output'
        if collection_out.is_relative_to(base):
            owned_prefix = str(collection_out.relative_to(base)) + '/'
    status = subprocess.check_output(['git', 'status', '--porcelain', '-z', '--untracked-files=all'], cwd=base, text=True)
    assert all(owned_prefix and entry.startswith('?? ') and entry[3:].startswith(owned_prefix)
               for entry in status.split('\0') if entry), 'dirty source'
    body = (base / CONFIG).read_bytes()
    assert body == subprocess.check_output(['git', 'show', 'HEAD:' + str(CONFIG)], cwd=base), 'uncommitted config'
    proof = qualify(base, config_sha=sha(body))
    source = proof['source_archive_commit']
    for revision, ancestor in ((source, 'HEAD'), (source, 'origin/main'), ('HEAD', 'origin/main')):
        subprocess.run(['git', 'merge-base', '--is-ancestor', revision, ancestor], cwd=base, check=True)
    for name in CODE:
        frozen = subprocess.check_output(['git', 'show', source + ':' + name], cwd=base)
        assert frozen == h.regular_path(base / name).read_bytes(), 'frozen code blob drift: ' + name
    assert archive_digest(source, base) == proof['source_archive_sha256'], 'frozen archive drift'
    return proof


def lifecycle():
    return prior.lifecycle()


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert commit == qualification['source_archive_commit'] and archive_sha == qualification['source_archive_sha256'], 'archive/config source drift'
    assert re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha)
    assert re.fullmatch('[a-zA-Z0-9/._-]+', archive_key) and '..' not in archive_key.split('/')
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', prefix)
    assert qualification['config_path'] == str(CONFIG) and qualification['campaign_schema'] == SCHEMA
    body_config = Path(CONFIG).read_bytes()
    assert sha(body_config) == qualification['config_sha256'], 'config changed since preflight'
    assert json.loads(body_config)['execution_source'] == dict(commit=commit, archive_sha256=archive_sha)
    _, bootstrap = lifecycle()
    adapter = {k: qualification[k] for k in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG), native_binary={'key': 'unused'}, native_publisher={'key': 'unused'})
    with patch.multiple(bootstrap, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=UPLOAD_FILES,
                        TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), patch.object(bootstrap, '_offered', return_value=False):
        body = bootstrap.user_data(commit, archive_sha, archive_key, prefix, adapter)
    config64 = base64.b64encode(gzip.compress(body_config, mtime=0)).decode()
    proof64 = base64.b64encode(gzip.compress(encoded(qualification), mtime=0)).decode()
    env = ' '.join('--setenv=' + n + '=2' for n in h.THREAD_ENV)
    command = f'''phase=install
python3.12 - <<'CONFIG'
import base64,gzip,hashlib,json
from pathlib import Path
body=gzip.decompress(base64.b64decode('{config64}',validate=True))
assert hashlib.sha256(body).hexdigest()=='{qualification['config_sha256']}'
Path('config.json').write_bytes(body)
CONFIG
python3.12 -m venv --system-site-packages "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0
aws configure set default.s3.max_concurrent_requests 2
aws configure set default.s3.multipart_chunksize 64MB
lscpu >cpu.txt
phase=reproduction
systemd-run --unit=cohere-artifact-reproduction --wait --pipe -p MemoryMax={MEMORY} -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec={SERVICE_SECONDS} -p WorkingDirectory="$root" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C {env} \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 {WORKER_SECONDS} \\
 "$root/venv/bin/python" -m {MODULE} --stage "$root/repo" "$root" {prefix} >profile.log 2>&1
for name in $ARTIFACT_NAMES; do
 case "$name" in screen/failure.json|screen/failure-resources.json) continue;; esac
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
'''
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    body = body[:start] + command + body[end:]
    fields = {k: qualification[k] for k in TERMINAL_IDENTITIES}
    assert body.count(repr(fields)[1:-1]) == 1
    body = body.replace(repr(fields)[1:-1], "**json.loads(Path('source-qualification.json').read_text())")
    body = body.replace("'source_archive_sha256':'" + archive_sha + "',", '', 1)
    body = body.replace('/mnt/native-semantic-router-cold', '/mnt/cohere-artifact-reproduction')
    body = body.replace('python3-boto3 python3.12', 'python3-boto3 python3.12 python3.12-venv')
    body = body.replace(", 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256')", '')
    body = body.replace('if path.is_file():', 'if path.is_file() and (name != "screen/COMPLETE.json" or os.environ["EXIT_CODE"] == "0"):', 1)
    # COMPLETE is uploaded after every other regular body, including failures.
    shell_quote = __import__('shlex').quote
    members = (*h.INPUT_FILES, 'builder', 'generation/{' + ','.join(h.GENERATION_FILES) + '}',
               *h.ARTIFACTS, 'failure.json', 'failure-resources.json', 'COMPLETE.json')
    roster = "export ARTIFACT_NAMES=\"$(printf '%s ' " + ' '.join(CONTROLLER_FILES) + ' screen/{' + ','.join(members) + '})"'
    body = body.replace('export ARTIFACT_NAMES=' + shell_quote(' '.join(UPLOAD_FILES)), roster)
    destination = 's3://' + BUCKET + '/' + prefix
    body = body.replace(destination, '$BORSUK_OUTPUT')
    body = body.replace('phase=bootstrap\n', 'phase=bootstrap\nBORSUK_OUTPUT=' + shell_quote(destination) + '\n', 1)
    body = body.replace('if [ -f "$name" ]; then', 'if [ -f "$name" ] && { [ "$name" != screen/COMPLETE.json ] || [ "$code" = 0 ]; }; then')
    body = body.replace('timeout --kill-after=5 60 aws s3 cp "$name"',
        f'systemd-run --quiet --wait --pipe -p MemoryMax={MEMORY} -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=135 --setenv=AWS_MAX_ATTEMPTS=1 timeout --kill-after=5 120 aws s3 cp "$root/$name"')
    marker = 'exec >run.log 2>&1\n'
    assert body.count(marker) == 1
    body = body.replace(marker, marker + f"python3 -c 'import base64,gzip; from pathlib import Path; Path(\"source-qualification.json\").write_bytes(gzip.decompress(base64.b64decode(\"{proof64}\",validate=True)))'\n" + "printf '%s\\n' '{\"status\":\"pending\",\"replacement_allowed\":false}' >failure.json\n")
    marker = '  cp run.log run-closed.log || code=96\n'
    assert body.count(marker) == 1
    body = body.replace(marker, '''  if [ "$phase" != complete ] || [ "$original_code" != 0 ]; then
    FAILURE_CODE="$original_code" FAILURE_PHASE="$phase" python3 - <<'FAILURE' || code=96
import json,os
from pathlib import Path
p=Path('failure.json'); value=json.loads(p.read_text()) if p.exists() else {}
value.update(status='failed',replacement_allowed=False,bootstrap_phase=os.environ['FAILURE_PHASE'],bootstrap_exit_code=int(os.environ['FAILURE_CODE']))
p.write_text(json.dumps(value,sort_keys=True,separators=(',',':'))+'\\n')
FAILURE
  fi
''' + marker)
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split('\nPY\n', 1)[0], '<terminal>', 'exec')
    assert len(body.encode()) <= 16384, 'EC2 user-data limit'
    assert not any(n in body for n in ('rustup', 'cargo', 'unused', '--publish', 'two_bit_http'))
    return body


def poll(ec2, s3, prefix, instance_id, started):
    shared, _ = lifecycle()
    with patch.object(shared, 'WALL', WALL):
        return shared.poll(ec2, s3, prefix, instance_id, started)


def runtime_abi(binary, pin, out):
    _, bootstrap = lifecycle()
    path = out / 'abi-builder'
    report = dict(qualified=False, builder=pin)
    try:
        assert dict(bytes=len(binary), sha256=sha(binary)) == pin
        write(path, binary); path.chmod(0o500)
        required = bootstrap._required_glibc(path)
        libc, version = os.confstr('CS_GNU_LIBC_VERSION').split()
        result = subprocess.run(['ldd', '-r', str(path)], capture_output=True, text=True,
                                timeout=15, env=dict(os.environ, LC_ALL='C'))
        report.update(required_glibc=required, libc=libc, glibc_version=version,
            returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
        assert libc == 'glibc' and bootstrap._version(required) <= bootstrap._version(version)
        text = (result.stdout + result.stderr).lower()
        assert result.returncode == 0 and text.strip() and not any(n in text for n in ('not found', 'undefined symbol', 'unresolved'))
        report['qualified'] = True
    finally:
        path.unlink(missing_ok=True)
        write(out / 'runtime-abi.json', report)
    return report


def stage(repo, out, prefix):
    repo, out = Path(repo).resolve(), h.regular_path(out)
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', prefix)
    counters = dict(closed=False, before=None, after=None)
    closure = dict(closed=False, helper_invocations=0, helper_exit_code=None, process_cleanup=False,
                   prefix=prefix, query_or_truth_used=False, wall_seconds=0)
    status = dict(status='pending', replacement_allowed=False)
    started, previous = time.monotonic(), signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, h.archived.prior.terminate)
    write(out / 'failure.json', status)
    try:
        recorded = h.read_json(out / 'source-qualification.json')
        proof = qualify(repo, out / 'config.json', recorded['config_sha256'])
        assert proof == recorded, 'worker frozen source/config differs'
        config = h.read_config(out / 'config.json', proof['config_sha256'], repo)
        _, binary, assurance = h.authorities(config, repo)
        write(out / 'archived-builder-assurance.json', assurance)
        closure.update(config_sha256=proof['config_sha256'], execution_source=config['execution_source'])
        versions = prior.tools()
        versions['thread_environment'] = {n: os.environ.get(n) for n in h.THREAD_ENV}
        assert set(versions['thread_environment'].values()) == {'2'}
        write(out / 'tool-versions.json', versions)
        counters['before'] = prior.capture_cgroup()
        with patch.object(prior, 'MEMORY', MEMORY):
            prior.validate_cgroup(dict(closed=True, before=counters['before'], after=counters['before']))
        runtime_abi(binary, {k: config['builder'][k] for k in ('bytes', 'sha256')}, out)
        assert not (out / 'screen').exists(), 'fresh output required'
        command = [sys.executable, str(repo / h.OWN), str(out / 'config.json'),
                   proof['config_sha256'], str(repo), str(out / 'screen')]
        closure.update(helper_invocations=1, command=command)
        result = prior.run_process(command, out / 'helper-process.log', WORKER_SECONDS - (time.monotonic() - started))
        closure.update(helper_exit_code=result['exit_status'], process_cleanup=result['process_cleanup'])
        assert type(result['exit_status']) is int and result['exit_status'] == 0 and result['process_cleanup'] is True, 'helper failed'
        counters.update(after=prior.capture_cgroup(), closed=True)
        with patch.object(prior, 'MEMORY', MEMORY):
            prior.validate_cgroup(counters)
        h.replay(out / 'config.json', proof['config_sha256'], repo, out / 'screen')
        closure['closed'] = True
        status.update(status='complete', helper_exit_code=0)
    except BaseException as error:
        status.update(status='failed', error_type=type(error).__name__, error=str(error),
                      helper_exit_code=closure['helper_exit_code'])
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
    finally:
        signal.signal(signal.SIGTERM, previous)
        closure['wall_seconds'] = time.monotonic() - started
        if counters['before'] is not None and counters['after'] is None:
            counters['after'] = prior.capture_cgroup()
        write(out / 'profile-cgroup.json', counters)
        write(out / 'reproduction-closure.json', closure)
        write(out / 'failure.json', status)
    return closure


def bounded_json(path):
    return h.read_json(path)


def validate_closed(out, proof, terminal, files):
    assert terminal['phase'] in ('bootstrap', 'apt-update', 'apt-install', 'awscli-download', 'awscli-install', 'source-download', 'install', 'reproduction', 'complete')
    complete = terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['status'] == ('complete' if complete else 'failed'), 'false terminal completion'
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    assert 0 <= terminal['exit_code'] <= 255 and 0 <= terminal['original_exit_code'] <= 255
    if not complete:
        assert terminal['exit_code'] != 0, 'failed exit must be nonzero'
        return False
    assert terminal['original_exit_code'] == 0 and set(files) == set(ARTIFACTS), 'complete body roster'
    assert files['config.json']['sha256'] == proof['config_sha256']
    assert files['screen/config.json'] == files['config.json']
    assert bounded_json(out / 'source-qualification.json') == proof
    config = bounded_json(out / 'config.json')
    assert config['execution_source'] == dict(commit=terminal['source_commit'], archive_sha256=terminal['source_archive_sha256'])
    marker = bounded_json(out / 'screen/COMPLETE.json')
    assert marker['schema'] == h.SCHEMA + '-complete' and marker['passed'] is True
    assert marker['config_sha256'] == proof['config_sha256'] and marker['build_invocations'] == 1
    assert marker['process_cleanup'] is marker['retained'] is True
    assert marker['query_or_truth_used'] is marker['quality_measured'] is marker['cold_http_measured'] is False
    retained = {n: files['screen/' + n] for n in h.RETAINED_FILES}
    assert marker['files'] == retained and marker['roster_sha256'] == h.archived.prior.value_sha(retained)
    assert {k: marker['new_root'][k] for k in ('bytes', 'sha256')} == retained['generation/manifest.json']
    assert retained['builder']['sha256'] == proof['builder_binary_sha256']
    for field, name in h.INPUT_FIELDS.items():
        assert retained[name] == {k: config['corpus'][field][k] for k in ('bytes', 'sha256')}
    plane = bounded_json(out / 'screen/generation/plane/manifest.json')
    for name, pin in config['payloads'].items():
        actual = retained['generation/' + name]
        assert all(actual[k] == v for k, v in pin.items() if k != 'sha256_from'), 'historical payload: ' + name
        if 'sha256_from' in pin:
            assert actual['sha256'] == plane['page_digest_sha256']
    h.check_head(bounded_json(out / 'screen/local-sq8-head.json'), config)
    assert bounded_json(out / 'screen/source-qualification.json') == bounded_json(out / 'archived-builder-assurance.json')
    closure = bounded_json(out / 'reproduction-closure.json')
    assert closure['closed'] is closure['process_cleanup'] is True and closure['helper_invocations'] == 1
    assert closure['helper_exit_code'] == 0 and closure['config_sha256'] == proof['config_sha256']
    assert closure['execution_source'] == config['execution_source'] and closure['query_or_truth_used'] is False
    assert 0 <= closure['wall_seconds'] <= WORKER_SECONDS
    failure = bounded_json(out / 'failure.json')
    assert failure['status'] == 'complete' and failure['helper_exit_code'] == 0
    with patch.object(prior, 'MEMORY', MEMORY):
        prior.validate_cgroup(bounded_json(out / 'profile-cgroup.json'))
    h.check_resources(bounded_json(out / 'screen/resources.json'), config)
    build = bounded_json(out / 'screen/build-resources.json')
    assert build['exit_status'] == 0 and build['process_cleanup'] is True and build['process_peak_rss_kib'] * 1024 <= MEMORY
    abi = bounded_json(out / 'runtime-abi.json')
    assert abi['qualified'] is True and abi['builder'] == {k: config['builder'][k] for k in ('bytes', 'sha256')}
    versions = bounded_json(out / 'tool-versions.json')
    assert versions['versions'] == prior.VERSIONS and versions['architecture'] == 'x86_64'
    assert versions['os_release']['ID'] == 'ubuntu' and versions['os_release']['VERSION_ID'] == '24.04'
    assert versions['threads'] == 2 and versions['aws_max_attempts'] == 1
    assert versions['python'].startswith('3.12.') and versions['thread_environment'] == dict.fromkeys(h.THREAD_ENV, '2')
    assert sha(encoded(bounded_json(out / 'archived-builder-assurance.json'))) == proof['builder_assurance_sha256']
    timing = (out / 'profile-resources.txt').read_text()
    assert 0 <= int(timing.split('Maximum resident set size (kbytes): ', 1)[1].splitlines()[0]) * 1024 <= MEMORY
    assert int(timing.split('Exit status: ', 1)[1].splitlines()[0]) == 0
    return True


def collect(s3, prefix, out, instance_id, commit, digest):
    out = h.regular_path(out)
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', prefix)
    assert out == h.regular_path(ROOT / NAME / prefix.removeprefix(PREFIX)), 'unowned collection output'
    launch, close, reservation = (bounded_json(out / n) for n in ('aws-launch.json', 'aws-closeout.json', 'aws-reservation.json'))
    assert close['state'] == 'terminated' and close['nodes'] == launch['nodes'], 'owned termination wait required'
    assert instance_id == launch['instance_id'] in {n['instance_id'] for n in close['nodes'].values()}
    assert launch['prefix'] == prefix
    for record in (launch, reservation):
        assert record['source_commit'] == commit and record['source_archive_sha256'] == digest
    proof = reservation['qualification']
    assert proof == preflight(collection_out=out), 'collection frozen authority drift'
    assert reservation['config_sha256'] == proof['config_sha256'], 'reservation config drift'
    assert proof['source_archive_commit'] == commit and proof['source_archive_sha256'] == digest
    stream = s3.get_object(Bucket=BUCKET, Key=prefix + '/terminal.json')['Body']
    with stream:
        raw = stream.read(LOCAL_BYTES + 1)
    assert len(raw) <= LOCAL_BYTES, 'bounded terminal'
    terminal = h.primitives.decode(raw)
    write(out / 'aws-terminal.json', raw)
    assert terminal['schema'] == reservation['schema'] == SCHEMA and terminal['instance_id'] == instance_id
    assert terminal['source_commit'] == commit and terminal['source_archive_sha256'] == digest
    for key in TERMINAL_IDENTITIES:
        assert terminal[key] == proof[key], 'terminal identity: ' + key
    assert set(terminal['artifacts']) <= set(UPLOAD_FILES), 'unexpected body roster'
    receipts, files = {}, terminal['artifacts']
    for name, pin in files.items():
        assert set(pin) == {'bytes', 'sha256'} and type(pin['bytes']) is int and 0 <= pin['bytes'] <= MAX_BODY_BYTES
        assert re.fullmatch('[0-9a-f]{64}', pin['sha256'])
        stream = s3.get_object(Bucket=BUCKET, Key=prefix + '/artifacts/' + name)['Body']
        count, hashed, small = 0, hashlib.sha256(), bytearray()
        with stream:
            for chunk in iter(lambda: stream.read(1 << 20), b''):
                count += len(chunk)
                assert count <= pin['bytes'], 'body length overflow: ' + name
                hashed.update(chunk)
                if pin['bytes'] <= LOCAL_BYTES:
                    small.extend(chunk)
        assert count == pin['bytes'] and hashed.hexdigest() == pin['sha256'], 'body identity: ' + name
        path = h.regular_path(out / name)
        if pin['bytes'] <= LOCAL_BYTES:
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                assert artifact(path) == pin, 'local receipt drift: ' + name
            else:
                write(path, bytes(small))
        receipts[name] = dict(**pin, bucket=BUCKET, key=prefix + '/artifacts/' + name,
            full_body_stream_verified=True, local_body=pin['bytes'] <= LOCAL_BYTES)
        write(out / 'collection-progress.json', dict(complete=False, files=receipts))
    complete = validate_closed(out, proof, terminal, files)
    write(out / 'collection-receipt.json', dict(schema=SCHEMA + '-collection', complete=complete,
        instance_id=instance_id, state='terminated', config_sha256=proof['config_sha256'],
        source_commit=commit, source_archive_sha256=digest, files=receipts,
        retained_body_count=sum('screen/' + n in files for n in h.RETAINED_FILES),
        whole_body_verification=True, large_bodies_retained_in_s3=True,
        helper_replay='Download every screen/ body to one regular directory; use helper --replay CONFIG SHA REPO DIRECTORY'))
    return terminal


def main(attempt):
    assert re.fullmatch(r'a[0-9]{4}', attempt)
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        preflight()  # No SDK/session before authority admission.
        shared, _ = lifecycle()
        return shared.main(attempt, campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


def self_check():
    """<=256MiB/55s; real metadata and tiny retained fixture, mocked cloud/native."""
    import copy
    from contextlib import redirect_stdout
    from datetime import datetime, timezone
    import resource
    import shutil
    import tempfile
    from types import ModuleType
    from unittest.mock import Mock

    started = time.monotonic()
    signal.alarm(55)
    assert resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024 < 256 << 20
    module, repo = sys.modules[__name__], Path(__file__).resolve().parents[1]
    draft = Path('/tmp/borsuk-fixed48-artifact-root-draft-config.json')
    before_draft = artifact(draft) if draft.exists() else None
    def rejected(call):
        try:
            call()
        except (AssertionError, ValueError, OSError, RuntimeError, subprocess.CalledProcessError):
            return
        raise AssertionError('negative check admitted')
    sdk, botocore, exceptions = (ModuleType(n) for n in ('boto3', 'botocore', 'botocore.exceptions'))
    class SDKError(Exception):
        def __init__(self, **kwargs):
            super().__init__('mock SDK')
    for name in ('ClientError', 'EndpointConnectionError', 'ReadTimeoutError'):
        setattr(exceptions, name, SDKError)
    sdk.Session = Mock(side_effect=AssertionError('cloud forbidden'))
    botocore.exceptions = exceptions
    with tempfile.TemporaryDirectory(prefix='artifact-controller-check-') as temporary, \
            patch.dict(sys.modules, {'boto3': sdk, 'botocore': botocore, 'botocore.exceptions': exceptions}):
        work = Path(temporary)
        config_path = work / 'frozen-config.json'
        refs = {n: h.primitives.decode(h.archived.read_ref(repo, p)) for n, p in h.FIXED.items() if n != 'preregister'}
        config = dict(h.EXPECTED, refs=h.FIXED,
            code_sha256={n: artifact(repo / n)['sha256'] for n in h.CODE},
            corpus=refs['archived_config']['corpus'], builder=refs['archived_config']['builder'],
            payloads=h.expected_payloads(refs), execution_source=dict(commit='a'*40, archive_sha256='b'*64),
            sq8_object_key='synthetic-controller/objects/' + refs['archived_config']['corpus']['sq8']['sha256'])
        write(config_path, config)
        with patch.object(module, 'CONFIG', config_path):
            proof = qualify(repo)
            for key, value in (('authority_pending', True), ('execution_source', None), ('code_sha256', {})):
                write(config_path, dict(config, **{key: value}))
                rejected(lambda: qualify(repo))
            write(config_path, config)
            rejected(lambda: qualify(repo, config_sha='0'*64))
            assert sha(config_path.read_bytes()) == proof['config_sha256']
            frozen = {n: (repo / n).read_bytes() for n in CODE}
            def git_read(args, **kwargs):
                if args[1] == 'status': return ''
                if args[1] == 'show':
                    revision, name = args[2].split(':', 1)
                    return config_path.read_bytes() if revision == 'HEAD' else frozen[name]
                raise AssertionError(args)
            with patch.object(subprocess, 'check_output', side_effect=git_read), \
                    patch.object(subprocess, 'run') as ancestry, patch.object(module, 'archive_digest', return_value='b'*64):
                assert preflight(repo) == proof
                assert ancestry.call_count == 3
                name = CODE[-1]; saved = frozen[name]; frozen[name] = b'changed source'
                rejected(lambda: preflight(repo)); frozen[name] = saved
                with patch.object(module, 'archive_digest', return_value='c'*64):
                    rejected(lambda: preflight(repo))
                with patch.object(subprocess, 'check_output', side_effect=lambda a, **k: 'dirty' if a[1]=='status' else git_read(a, **k)):
                    rejected(lambda: preflight(repo))
                with patch.object(subprocess, 'check_output', side_effect=lambda a, **k: b'changed config' if a[1]=='show' and a[2].startswith('HEAD:') else git_read(a, **k)):
                    rejected(lambda: preflight(repo))
                with patch.object(subprocess, 'run', side_effect=subprocess.CalledProcessError(1, 'git ancestry')):
                    rejected(lambda: preflight(repo))
            # Real Git status and collection admission; only historical reference
            # loading is cached, using the authorities authenticated above.
            authority = h.authorities(config, repo)
            source_repo = work / 'source-repo'; source_repo.mkdir()
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=source_repo, stderr=subprocess.PIPE)
            git('init', '-q')
            for name in CODE:
                target = source_repo/name; target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((repo/name).read_bytes())
            owned = source_repo/ROOT/'a0001'; owned.mkdir(parents=True)
            tracked = owned/'tracked.log'; tracked.write_bytes(b'frozen tracked log')
            git('add', '.')
            git('-c', 'core.hooksPath=/dev/null', 'commit', '-qm', 'Synthetic frozen source')
            source_commit = git('rev-parse', 'HEAD').decode().strip()
            source_digest = archive_digest(source_commit, source_repo)
            source_config = source_repo/ROOT/'config.json'
            write(source_config, dict(config, execution_source=dict(commit=source_commit, archive_sha256=source_digest)))
            git('add', str(ROOT/'config.json'))
            git('-c', 'core.hooksPath=/dev/null', 'commit', '-qm', 'Synthetic separate config freeze')
            git('update-ref', 'refs/remotes/origin/main', 'HEAD')
            config_commit = git('rev-parse', 'HEAD').decode().strip()
            before = Path.cwd()
            try:
                os.chdir(source_repo)
                with patch.object(module, 'CONFIG', ROOT/'config.json'), \
                        patch.object(h, '__file__', str(source_repo/h.OWN)), patch.object(h, 'authorities', return_value=authority):
                    source_proof = preflight()
                    launch = dict(source_commit=source_commit, source_archive_sha256=source_digest,
                        instance_id='i-original', nodes={'0':dict(instance_id='i-original')}, prefix=PREFIX+'a0001')
                    terminal = dict(**launch, **{k:source_proof[k] for k in TERMINAL_IDENTITIES},
                        schema=SCHEMA, status='failed', phase='complete', exit_code=96, original_exit_code=0, artifacts={})
                    for name, value in (('aws-launch.json', launch),
                            ('aws-closeout.json', dict(state='terminated', nodes=launch['nodes'])),
                            ('aws-reservation.json', dict(schema=SCHEMA, qualification=source_proof,
                                config_sha256=source_proof['config_sha256'], source_commit=source_commit, source_archive_sha256=source_digest))):
                        write(owned/name, value)
                    s3 = Mock(); s3.get_object.side_effect=lambda **k: {'Body':io.BytesIO(encoded(terminal))}
                    collect_args = (s3, launch['prefix'], owned, 'i-original', source_commit, source_digest)
                    rejected(lambda: preflight())  # launch still refuses its dirty output
                    assert collect(*collect_args)['exit_code'] == 96
                    assert bounded_json(owned/'collection-receipt.json')['complete'] is False
                    # Every authority/path rejection happens before a remote fetch.
                    def refused():
                        s3.reset_mock(); rejected(lambda: collect(*collect_args)); s3.get_object.assert_not_called()
                    for path in (tracked, source_repo/OWN, source_config):
                        saved = path.read_bytes(); path.write_bytes(saved+b'changed'); refused(); path.write_bytes(saved)
                    # Clean, committed drift must still fail original code/config
                    # binding; a blanket clean-tree exemption cannot admit it.
                    for path in (source_repo/OWN, source_config):
                        if path == source_config:
                            changed = bounded_json(path)
                            changed['sq8_object_key'] = 'changed/objects/' + config['corpus']['sq8']['sha256']
                            write(path, changed)
                        else:
                            path.write_bytes(path.read_bytes()+b'\n# committed controller drift\n')
                        git('add', str(path.relative_to(source_repo)))
                        git('-c', 'core.hooksPath=/dev/null', 'commit', '-qm', 'Synthetic committed drift')
                        git('update-ref', 'refs/remotes/origin/main', 'HEAD')
                        refused()
                        git('reset', '--hard', config_commit)
                        git('update-ref', 'refs/remotes/origin/main', 'HEAD')
                    for name in ('a0002/unrelated.log', 'a0001-other/lookalike.log'):
                        path = source_repo/ROOT/name; path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(b'unrelated'); refused(); path.unlink()
                    reservation_path = owned/'aws-reservation.json'
                    saved = reservation_path.read_bytes(); reservation = bounded_json(reservation_path)
                    reservation['qualification']['refs_identity_sha256'] = '0'*64
                    write(reservation_path, reservation); refused(); write(reservation_path, saved)
                    s3.reset_mock()
                    rejected(lambda: collect(s3, PREFIX+'a0002', owned, 'i-original', source_commit, source_digest))
                    s3.get_object.assert_not_called()
                    outside = work/'outside/a0001'; outside.mkdir(parents=True)
                    for name in ('aws-launch.json', 'aws-closeout.json', 'aws-reservation.json'):
                        shutil.copyfile(owned/name, outside/name)
                    shutil.rmtree(owned)  # remove owned untracked files; restore tracked source
                    owned.mkdir(); tracked.write_bytes(b'frozen tracked log')
                    with patch.object(module, 'ROOT', outside.parent):
                        assert collect(s3, launch['prefix'], outside, 'i-original', source_commit, source_digest)['exit_code'] == 96
            finally:
                os.chdir(before)
            # Streaming gzip identity agrees with the shared launcher's exact bytes.
            payload = b'tiny frozen source\n' * 1000
            process = Mock(); process.stdout = io.BytesIO(payload); process.wait.return_value = 0
            process.__enter__ = Mock(return_value=process); process.__exit__ = Mock(return_value=False)
            with patch.object(subprocess, 'Popen', return_value=process):
                assert archive_digest('a'*40, repo) == sha(gzip.compress(payload, mtime=0))
            shared, _ = lifecycle()
            body = user_data('a'*40, 'b'*64, 'source/key', PREFIX+'a0001', proof)
            assert len(body.encode()) <= 16384
            roster_script = body.split('phase=bootstrap\n', 1)[1].split('finish() {\n', 1)[0]
            names = subprocess.check_output(['bash', '-c', roster_script + '\nprintf "%s" "$ARTIFACT_NAMES"'], text=True).split()
            assert names == [n for n in UPLOAD_FILES if n != 'screen/COMPLETE.json'] + ['screen/COMPLETE.json']
            # Root's separate config commit may preserve indented original bytes.
            realistic = dict(config, execution_source=dict(commit=sha(b'realistic commit')[:40], archive_sha256=sha(b'realistic archive')))
            realistic_body = (json.dumps(realistic, indent=2) + '\n').encode()
            production_path = ROOT / 'config.json'
            realistic_proof = dict(proof, config_path=str(production_path), config_sha256=sha(realistic_body),
                source_archive_commit=realistic['execution_source']['commit'], source_archive_sha256=realistic['execution_source']['archive_sha256'])
            read_bytes = Path.read_bytes
            with patch.object(module, 'CONFIG', production_path), \
                    patch.object(Path, 'read_bytes', lambda p: realistic_body if p == production_path else read_bytes(p)):
                realistic_data = user_data(realistic_proof['source_archive_commit'], realistic_proof['source_archive_sha256'],
                    'research/native-library-check/sources/' + realistic_proof['source_archive_sha256'] + '.tar.gz', PREFIX+'a0001', realistic_proof)
                assert len(realistic_data.encode()) <= 16384
            assert all(n in body for n in ('--on-active=3600s', 'MemoryMax=12884901888', 'MemorySwapMax=0', 'CPUQuota=200%', 'TasksMax=512', 'RuntimeMaxSec=1860', 'timeout --signal=TERM --kill-after=30 1800'))
            assert all('--setenv='+n+'=2' in body for n in h.THREAD_ENV)
            rejected(lambda: user_data('c'*40, 'b'*64, 'source/key', PREFIX+'a0001', proof))
            rejected(lambda: user_data('a'*40, 'c'*64, 'source/key', PREFIX+'a0001', proof))
            script = body.split("python3.12 - <<'CONFIG'\n", 1)[1].split('\nCONFIG\n', 1)[0]
            embedded = work / 'embedded'; embedded.mkdir()
            subprocess.run([sys.executable, '-c', script], cwd=embedded, check=True)
            assert (embedded/'config.json').read_bytes() == config_path.read_bytes()
            early = body.split('exec >run.log 2>&1\n',1)[1].split('\n',1)[0]
            subprocess.run(['bash','-c',early], cwd=embedded, check=True)
            assert bounded_json(embedded/'source-qualification.json') == proof
            packed = base64.b64encode(gzip.compress(b'changed config', mtime=0)).decode()
            bad_script = re.sub(r"body=gzip.decompress\(base64.b64decode\('[^']+'", "body=gzip.decompress(base64.b64decode('"+packed+"'", script)
            assert subprocess.run([sys.executable, '-c', bad_script], cwd=embedded, capture_output=True).returncode != 0
            write(config_path, dict(config, authority_pending=True))
            rejected(lambda: user_data('a'*40, 'b'*64, 'source/key', PREFIX+'a0001', proof)); write(config_path, config)
            # Execute actual terminal hashing and finish uploads with shell cloud stubs.
            finish = body[body.index('finish() {\n'):body.index('trap finish EXIT\n')]
            for mode in ('success', 'helper-failed', 'upload-failed'):
                destination = work / ('bootstrap-'+mode); destination.mkdir()
                for name in ARTIFACTS:
                    path = destination/name; path.parent.mkdir(parents=True, exist_ok=True); write(path, b'body\n')
                write(destination/'run.log', b'closed bootstrap\n')
                write(destination/'source-qualification.json', proof)
                write(destination/'failure.json', dict(status='complete', helper_exit_code=0))
                stub = '''curl() { case "$*" in */api/token*) echo mock-token;; *) echo i-synthetic;; esac; }
shutdown() { :; }
timeout() { shift 2; "$@"; }
systemd-run() { while [ "$1" != timeout ]; do shift; done; (cd /; "$@"); }
aws() { test -f "$3" || return 44; cmp "$3" "$root/${4##*/artifacts/}" 2>/dev/null || { [ "$4" = "$BORSUK_OUTPUT/terminal.json" ] && cmp "$3" "$root/terminal.json"; } || return 45; printf '%s\\n' "$4" >>"$root/uploads"; if [ "$MODE" = upload-failed ] && [[ "$4" = */artifacts/profile.log ]]; then return 55; fi; }
'''
                run = 'BORSUK_OUTPUT=s3://'+BUCKET+'/'+PREFIX+'a0001; root='+str(destination)+'; phase='+('reproduction' if mode=='helper-failed' else 'complete')+'; export MODE='+mode+'; export ARTIFACT_NAMES='+__import__('shlex').quote(' '.join(n for n in UPLOAD_FILES if n != 'screen/COMPLETE.json')+' screen/COMPLETE.json')+'; cd "$root"\n'
                script_finish = stub+run+finish.replace('/dev/ttyS0', str(destination/'serial'))+'\n(exit '+('7' if mode=='helper-failed' else '0')+'); finish\n'
                finished = subprocess.run(['bash', '-c', script_finish], capture_output=True, timeout=10, cwd='/')
                terminal = bounded_json(destination/'terminal.json')
                assert terminal['exit_code'] == finished.returncode == {'success':0, 'helper-failed':7, 'upload-failed':96}[mode], (mode, terminal['exit_code'], finished.returncode, finished.stderr)
                assert terminal['original_exit_code'] == (7 if mode=='helper-failed' else 0)
                assert terminal['status'] == ('complete' if mode=='success' else 'failed')
                uploads = (destination/'uploads').read_text().splitlines()
                marker_upload = [n for n in uploads if n.endswith('/artifacts/screen/COMPLETE.json')]
                assert bool(marker_upload) == (mode=='success')
                assert ('screen/COMPLETE.json' in terminal['artifacts']) == (mode=='success')
                if mode=='success':
                    assert uploads[-2].endswith('/artifacts/screen/COMPLETE.json') and uploads[-1].endswith('/terminal.json')
                    assert set(terminal['artifacts']) == set(ARTIFACTS)
            sdk.Session.assert_not_called()
            # Reuse actual shared ACK, fsync, interruption and termination paths.
            with redirect_stdout(io.StringIO()): shared.self_check(lifecycle_only=True)
            launch_proof = dict(proof, source_archive_sha256=sha(gzip.compress(b'synthetic archive',mtime=0)))
            for mode in ('success', 'fsync', 'multi-ack', 'interrupt', 'interruption', 'wait'):
                ec2, s3, session = Mock(), Mock(), Mock(); session.client.side_effect = [ec2, s3]
                ec2.describe_instances.return_value = {'Reservations': []}
                ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'mock-az'}]}
                ec2.describe_spot_price_history.return_value = {'SpotPriceHistory': [{'SpotPrice': '0.1', 'Timestamp': datetime.now(timezone.utc)}]}
                ids = ['i-original', 'i-extra'] if mode=='multi-ack' else ['i-original']
                ec2.run_instances.return_value = {'Instances': [{'InstanceId': n} for n in ids]}
                events = []; ec2.terminate_instances.side_effect = lambda **k: events.append('terminate')
                def wait(**kwargs):
                    events.append('wait')
                    if mode=='wait': raise RuntimeError('wait failed')
                ec2.get_waiter.return_value.wait.side_effect = wait
                def collected(*args):
                    assert events == ['terminate', 'wait']; events.append('collect')
                    assert args[-2:] == ('a'*40, launch_proof['source_archive_sha256'])
                    return dict(status='complete', phase='complete', exit_code=0, artifacts=dict.fromkeys(ARTIFACTS, {}))
                error = {'interrupt': KeyboardInterrupt(), 'interruption': RuntimeError('Spot interruption')}.get(mode)
                with patch.object(module, 'ROOT', work/mode), patch.object(module, 'preflight', return_value=launch_proof), \
                        patch.object(module, 'user_data', return_value='synthetic user data'), \
                        patch.object(shared.boto3, 'Session', return_value=session), \
                        patch.object(subprocess, 'check_output', side_effect=['', 'a'*40, b'synthetic archive']), \
                        patch.object(subprocess, 'run'), patch.object(shared.peer, 'missing', return_value=True), \
                        patch.object(shared.peer, 'put_if_absent'), patch.object(module, 'poll', side_effect=error), \
                        patch.object(module, 'collect', side_effect=collected) as collector, \
                        patch.object(os, 'fsync', side_effect=OSError('fsync failed') if mode=='fsync' else None), redirect_stdout(io.StringIO()):
                    try: main('a0001')
                    except (OSError, RuntimeError, KeyboardInterrupt): assert mode not in ('success', 'multi-ack')
                    else: assert mode in ('success', 'multi-ack')
                ec2.run_instances.assert_called_once()
                ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
                ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
                if mode=='wait': collector.assert_not_called()
                else: assert events == ['terminate', 'wait', 'collect']
                args = ec2.run_instances.call_args.kwargs
                assert args['ImageId']==IMAGE_ID and args['InstanceType']==INSTANCE_TYPE
                assert args['BlockDeviceMappings']==[{'DeviceName':ROOT_DEVICE_NAME,'Ebs':{'DeleteOnTermination':True,'Encrypted':True,'VolumeSize':80,'VolumeType':'gp3'}}]
                assert args['InstanceMarketOptions']['SpotOptions']['MaxPrice']=='0.50'
                reservation = bounded_json(work/mode/'a0001/aws-reservation.json')
                assert reservation['wall_seconds']==3600 and reservation['compute_cap_usd']==.50 and reservation['ebs_s3_allowance_usd']==.15
            counters = dict(cgroup='/synthetic', observer_pid=1234, process_ids=[1234], **{
                'memory.max':str(MEMORY), 'memory.peak':'1234', 'memory.swap.max':'0', 'memory.swap.peak':'0',
                'memory.events':'max 0\noom 0\noom_kill 0', 'memory.swap.events':'max 0\nfail 0',
                'cpu.max':'200000 100000', 'cpu.stat':'usage_usec 10', 'pids.max':'512', 'pids.current':'1', 'pids.events':'max 0'})
            versions = dict(versions=prior.VERSIONS, architecture='x86_64', os_release=dict(ID='ubuntu',VERSION_ID='24.04'),
                threads=2, aws_max_attempts=1, python='3.12.0', thread_environment=dict.fromkeys(h.THREAD_ENV, '2'))
            for mode in ('success', 'nonzero', 'cleanup', 'interrupt', 'abi'):
                destination = work/('stage-'+mode); destination.mkdir()
                write(destination/'config.json',config_path.read_bytes()); write(destination/'source-qualification.json',proof)
                def prepared(*args):
                    if mode=='interrupt': raise KeyboardInterrupt()
                    screen = destination/'screen'; screen.mkdir(); write(screen/'build.log', b'preserved partial log\n')
                    write(args[1], b'one mocked helper\n')
                    return dict(exit_status=7 if mode=='nonzero' else 0, process_cleanup=mode!='cleanup')
                with patch.dict(os.environ, dict.fromkeys(h.THREAD_ENV, '2')), \
                        patch.object(prior, 'tools', return_value=versions), patch.object(prior, 'capture_cgroup', return_value=counters), \
                        patch.object(module, 'runtime_abi', side_effect=ValueError('incompatible builder') if mode=='abi' else None), \
                        patch.object(prior, 'run_process', side_effect=prepared) as helper_call, patch.object(h, 'replay'):
                    try: closure = stage(repo,destination,PREFIX+'a0001')
                    except KeyboardInterrupt: assert mode=='interrupt'
                    else:
                        assert closure['closed'] == (mode=='success'), (mode, closure, bounded_json(destination/'failure.json'))
                        if mode=='nonzero': assert closure['helper_exit_code']==7
                assert helper_call.call_count == (0 if mode=='abi' else 1)
                if mode!='abi':
                    assert helper_call.call_args.args[0][1]==str(repo/h.OWN)
                    assert 0 < helper_call.call_args.args[2] <= WORKER_SECONDS
                if mode in ('nonzero','cleanup'): assert (destination/'screen/build.log').exists()
                assert bounded_json(destination/'failure.json')['status']==('complete' if mode=='success' else 'failed')
            from scripts import run_native_semantic_1m_quality as native
            process = Mock(pid=1234)
            with patch.object(native.subprocess, 'Popen', return_value=process), patch.object(native.os, 'killpg') as killed:
                process.wait.side_effect = [subprocess.TimeoutExpired('mock',1),0,0]
                try: prior.run_process(['mock'],work/'timeout.log',1)
                except subprocess.TimeoutExpired: pass
                else: raise AssertionError('timeout admitted')
                assert killed.call_args_list[0].args==(1234,signal.SIGTERM) and killed.call_args_list[-1].args==(1234,signal.SIGKILL)
                assert process.wait.call_count==3

        # Collect the helper's actual tiny production-format fixture while its
        # scoped fake native/download seams are still active; replay stays real.
        actual_replay, collected_once = h.replay, []
        def fixture_replay(path, digest, base, screen):
            marker = actual_replay(path, digest, base, screen)
            if collected_once: return marker
            collected_once.append(True)
            destination = work/'collected/a0001'; destination.mkdir(parents=True); shutil.copytree(screen,destination/'screen')
            fixture_config = json.loads(Path(path).read_bytes())
            fixture_proof = dict(proof, config_sha256=digest,
                builder_binary_sha256=fixture_config['builder']['sha256'],
                builder_assurance_sha256=sha(encoded(bounded_json(screen/'source-qualification.json'))))
            source = dict(source_commit=fixture_config['execution_source']['commit'], source_archive_sha256=fixture_config['execution_source']['archive_sha256'])
            write(destination/'config.json',Path(path).read_bytes()); write(destination/'source-qualification.json',fixture_proof)
            write(destination/'archived-builder-assurance.json',bounded_json(screen/'source-qualification.json'))
            write(destination/'runtime-abi.json',dict(qualified=True,builder=fixture_config['builder']))
            write(destination/'tool-versions.json',versions); write(destination/'cpu.txt',b'synthetic x86\n')
            for name in ('run-closed.log','profile.log'): write(destination/name,b'closed log\n')
            write(destination/'helper-process.log',b'x'*(LOCAL_BYTES+17))
            write(destination/'profile-resources.txt',b'Maximum resident set size (kbytes): 42\nExit status: 0\n')
            write(destination/'profile-cgroup.json',dict(closed=True,before=counters,after=counters))
            write(destination/'reproduction-closure.json',dict(closed=True,process_cleanup=True,helper_invocations=1,
                helper_exit_code=0,config_sha256=digest,execution_source=fixture_config['execution_source'],query_or_truth_used=False,wall_seconds=.01))
            write(destination/'failure.json',dict(status='complete',helper_exit_code=0))
            launch = dict(**source,instance_id='i-original',nodes={'0':dict(instance_id='i-original')},prefix=PREFIX+'a0001')
            terminal = dict(**source,**{k:fixture_proof[k] for k in TERMINAL_IDENTITIES},schema=SCHEMA,
                instance_id='i-original',status='complete',phase='complete',exit_code=0,original_exit_code=0,
                artifacts={n:artifact(destination/n) for n in ARTIFACTS})
            for name,value in (('aws-launch.json',launch),('aws-closeout.json',dict(state='terminated',nodes=launch['nodes'])),
                    ('aws-reservation.json',dict(schema=SCHEMA,qualification=fixture_proof,config_sha256=digest,**source))): write(destination/name,value)
            bodies = {n:(destination/n).read_bytes() for n in ARTIFACTS}; s3 = Mock()
            class Stream(io.BytesIO):
                def read(self, size=-1):
                    assert 0 < size <= LOCAL_BYTES+1, 'unbounded S3 read'
                    return super().read(size)
            def get(**kwargs):
                key=kwargs['Key']; return {'Body':Stream(encoded(terminal) if key.endswith('/terminal.json') else bodies[key.split('/artifacts/',1)[1]])}
            s3.get_object.side_effect=get
            with patch.object(module,'ROOT',destination.parent), patch.object(module,'preflight',return_value=fixture_proof):
                assert collect(s3,launch['prefix'],destination,'i-original',source['source_commit'],source['source_archive_sha256'])['exit_code']==0
                assert s3.get_object.call_count==len(ARTIFACTS)+1
                receipt=bounded_json(destination/'collection-receipt.json')
                assert receipt['complete'] and receipt['retained_body_count']==30
                assert receipt['files']['helper-process.log']['local_body'] is False
                assert actual_replay(destination/'config.json',digest,base,destination/'screen')['passed']
                for key in ('source_commit','config_sha256','artifact_roster_sha256','instance_id'):
                    saved=terminal[key]; terminal[key]='c'*len(saved)
                    rejected(lambda:collect(s3,launch['prefix'],destination,'i-original',source['source_commit'],source['source_archive_sha256'])); terminal[key]=saved
                for name in h.RETAINED_FILES:
                    name='screen/'+name; saved=bodies[name]; bodies[name]=b'tampered retained body'
                    rejected(lambda:collect(s3,launch['prefix'],destination,'i-original',source['source_commit'],source['source_archive_sha256'])); bodies[name]=saved
                missing='screen/source.raw'; saved=terminal['artifacts'].pop(missing)
                rejected(lambda:collect(s3,launch['prefix'],destination,'i-original',source['source_commit'],source['source_archive_sha256'])); terminal['artifacts'][missing]=saved
                saved_get=s3.get_object.side_effect
                def absent(**kwargs):
                    if kwargs['Key'].endswith('/screen/generation/canonical.bin'): raise FileNotFoundError('missing retained S3 body')
                    return saved_get(**kwargs)
                s3.get_object.side_effect=absent
                rejected(lambda:collect(s3,launch['prefix'],destination,'i-original',source['source_commit'],source['source_archive_sha256'])); s3.get_object.side_effect=saved_get
                original_terminal=copy.deepcopy(terminal)
                terminal.update(status='failed',phase='reproduction',exit_code=7,original_exit_code=7,
                    artifacts={n:terminal['artifacts'][n] for n in ('run-closed.log','profile.log')})
                assert collect(s3,launch['prefix'],destination,'i-original',source['source_commit'],source['source_archive_sha256'])['exit_code']==7
                assert bounded_json(destination/'collection-receipt.json')['complete'] is False
                terminal.clear(); terminal.update(original_terminal)
                for state,nodes in (('running',launch['nodes']),('terminated',{'0':dict(instance_id='i-unowned')})):
                    write(destination/'aws-closeout.json',dict(state=state,nodes=nodes)); s3.reset_mock()
                    rejected(lambda:collect(s3,launch['prefix'],destination,'i-original',source['source_commit'],source['source_archive_sha256'])); s3.get_object.assert_not_called()
                # Actual closed resource and COMPLETE checks, independent of SHA transport.
                terminal=original_terminal
                report=bounded_json(destination/'screen/resources.json'); report['passed']=False
                resource_path=destination/'screen/resources.json'; resource_path.chmod(0o600); write(resource_path,report)
                rejected(lambda:validate_closed(destination,fixture_proof,terminal,terminal['artifacts']))
            return marker
        with patch.object(h,'replay',side_effect=fixture_replay),redirect_stdout(io.StringIO()): h.self_check()
        assert collected_once
    if before_draft is not None: assert artifact(draft)==before_draft
    signal.alarm(0)
    assert time.monotonic()-started<55 and resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024 <= 256<<20
    print(f'PASS config/source/archive and embedded config; real Git owned-output collection/clean committed drift; service-cwd shell/finish marker-last/failure; ONE helper/ABI failure/cleanup; ACK/fsync/multiACK/interrupt/terminate-wait; all30 stream tamper/missing/failed/resource rejection and actual retained replay. Indented production-shape user data {len(realistic_data.encode())}/16384 bytes. {time.monotonic()-started:.2f}/55s; {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024}/268435456 RSS bytes. Cloud/data/native UNRUN.')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    elif sys.argv[1:2] == ['--stage']:
        assert len(sys.argv) == 5
        closed = stage(*sys.argv[2:])
        sys.exit(0 if closed['closed'] else (closed['helper_exit_code'] or 1))
    else:
        assert len(sys.argv) == 2, 'aNNNN | --stage REPO OUTPUT PREFIX | --self-check'
        with open('/tmp/borsuk-fixed48-artifact-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(sys.argv[1])
