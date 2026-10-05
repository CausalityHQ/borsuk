"""One frozen native packing diagnostic; root owns config, transport and launch.

CLI: aNNNN | --self-check | --replay OUT. Remote --remote is bootstrap-only.
No compiler, query runner, packing algorithm, retries or replacement instances.
"""
import argparse
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import stat
import subprocess
import sys
import time

if not __debug__:
    raise RuntimeError('diagnostic requires assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path('docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/packing-diagnostic')
CONFIG, NAME = ROOT/'config.json', ''
SCHEMA = 'borsuk-fine-pack-diagnostic-spot-v1'
PREFIX = 'research/hierarchical-cells/20261005/fine-pack-diagnostic-'
TOKEN_PREFIX, TAG = 'fine-pack-diagnostic-', 'borsuk-fine-pack-diagnostic'
MODULE = 'scripts.launch_fine_pack_diagnostic'
WALL, COMPUTE_CAP, SPOT_MAX_USD_PER_HOUR = 900, .15, .60
INSTANCE_TYPE, IMAGE_ID = 'c7i.2xlarge', 'ami-0b8a830d6339a9758'
ROOT_DEVICE_NAME, SUBNET = '/dev/sda1', 'subnet-034528fbd6977848f'
REGION, BUCKET = 'eu-central-1', 'borsuk-bench-453182569524-euc1'
REMOTE_ROOT = Path('/mnt/fine-pack-diagnostic')
BINARY_NAME = 'binaries/hierarchical_semantic_cells'
BINARY_PIN = dict(bytes=7860472, sha256='a4ea1d88223dbcf73b714ca2f75b9556c9d4f1389333391c1ac619a8bb9b4f45')
BINARY_KEY = 'research/semantic-router/20261005/fine-sq8-packing-binary-repair-a0001/artifacts/'+BINARY_NAME
NATIVE_COMMIT = '71f77b3a30ea7f364c7a9d7e82188151a25a6be4'
SOURCE_ID = '13286db011c6f0699f74b0e10e9d340e0ac565346a8c8640bcf5e4b64990b36d'
DIAGNOSTIC_SOURCES = {
    'fine_sq8_groups.rs': 'a66c728c9bc708e542b57a8c8dc4efa681a705c2731717ad3eda2d52546d5027',
    'resident_vector_graph.rs': 'c62459b45caeaaa798b16b24b88180ad6d4dc5bdd554cd804e6d6b354eb3b79d',
    'bin/hierarchical_semantic_cells.rs': 'f636602726e30f8f5c4d67f557546318ffb75f091da30c7a131a1247d39a6bd3'}
CAPS = dict(cpu_threads=1, memory_bytes=256*1024**2, swap_bytes=0,
            deadline_seconds=300, operations=500000000, output_bytes=2*1024**2)
FIXED = dict(region=REGION, bucket=BUCKET, instance_type=INSTANCE_TYPE,
             image_id=IMAGE_ID, subnet_id=SUBNET, root_device_name=ROOT_DEVICE_NAME,
             machine_limit_seconds=WALL, compute_cap_usd=COMPUTE_CAP,
             spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR, ebs_s3_allowance_usd=.15,
             native_cpu_affinity=[0], native_caps=CAPS, tasks_max=32,
             automatic_retry=False, quality_or_performance_claim=False)
# The lifecycle's import closure is frozen too; the remote supervisor imports
# only this file and the SDK. Root supplies the minimal archive roster.
CODE = tuple(sorted('scripts/'+n+'.py' for n in (
    'launch_fine_pack_diagnostic', 'launch_native_metadata_ranges_cold_spot',
    'launch_native_startup_profile_spot', 'launch_native_peer_1m_spot',
    'launch_v174_relaid_bind_compile_spot', 'launch_v157_primary_feasibility_spot',
    'check_native_metadata_ranges_build', 'check_native_metadata_ranges_stats',
    'check_native_startup_build', 'check_native_startup_stats',
    'run_native_metadata_ranges_cold', 'run_native_cold_first_query',
    'run_native_peer_1m_worker', 'run_native_peer_offered_http',
    'run_native_union_offered_http', 'run_native_union_http', 'rest_coexistence_load')))
EVIDENCE = ROOT.parent/'packing-implementation-gates/binary-repair/a0001'
RECEIPTS = tuple(dict(path=str(EVIDENCE/name), bytes=size, sha256=digest) for name, size, digest in (
    ('parent-verification.json', 14424, 'c13efa7743a6d9e9f9d21b0e9e5ec56b3b5058484b2d4eca71ec7c817c6132bd'),
    ('aws-terminal.json', 3128, '3dca512d1b9c89e571944e792efc36d91a66e02c6e5195c78b2810098ea8855e'),
    ('aws-closeout.json', 136, '1d3cce4ac98066fe088ed543e0a34fefa34f8557d935edd63f5f4cf9b5558fd2'),
    ('workspace-receipt.json', 62788, '697db113bf61402d8a6c776fbbd7c94bb12538faec904c2f23ab7bf611a02333'),
    ('source-qualification.json', 410667, 'ffae1c458c116584539b234825a314d2fab71d90f593cd64d19c8849593c56d9')))
INPUT_PINS = (
    ('/mnt/hierarchical-100k/screen/fine/layouts/relaion/manifest.json', 22721, '0a2a2dff6a7abc48143d08eb491a150bbed03957b81b0261c2b8a0434612afec'),
    ('/mnt/hierarchical-100k/screen/fine/layouts/relaion/graph.bin', 26712462, 'f817d7f4fbdd640f66590f58f9e07e9218753fbec4517ba95fc405a78d612325'),
    ('/mnt/hierarchical-100k/screen/fine/layouts/cohere/manifest.json', 22815, 'ea1a792a841014ef08ed805d2872403739ba778ea8b8d3a9e5eab1b7f831c8bb'),
    ('/mnt/hierarchical-100k/screen/fine/layouts/cohere/graph.bin', 26844914, '4afd895f2e684f833437f4e6d4270c4713bbeb54fe67b50919f796e95f896e2e'),
    ('/mnt/hierarchical-100k/packing-original-seal.json', 1109, '374cbd0e8700c85c2b4229468c3a9fa20283deb969b661386fb0078024b9de62'),
    ('/mnt/hierarchical-100k/packing-prefix.jsonl', 1665668, '7abcf7830e10d214999b26fb499cebf925e16b8a88a931ddb9f981ad3d28d025'))
ARTIFACTS = ('config.json', 'native-config.json', 'source-qualification.json',
    'stage-receipt.json', 'native-exit.json', 'resources.json', 'cleanup.json',
    'cpu.txt', 'runtime-abi.json', 'run-closed.log', 'native.log', BINARY_NAME,
    'screen/report.json', 'screen/report.permutations.json', 'screen/report.prefix.jsonl')
ROSTER_SHA = hashlib.sha256(json.dumps(ARTIFACTS, separators=(',', ':')).encode()).hexdigest()
CGROUP_FILES = ('memory.max', 'memory.peak', 'memory.swap.max', 'memory.swap.peak',
                'memory.events', 'memory.swap.events', 'cpu.max', 'cpu.stat',
                'pids.max', 'pids.current', 'pids.events', 'cgroup.procs', 'cgroup.events')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)+'\n').encode()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def decode(body):
    def pairs(rows):
        result = {}
        for key, value in rows:
            require(key not in result, 'duplicate JSON key: '+key)
            result[key] = value
        return result
    return json.loads(body, object_pairs_hook=pairs,
                      parse_constant=lambda n: require(False, 'invalid JSON number: '+n))


def pin(body):
    return dict(bytes=len(body), sha256=sha(body))


def regular(path):
    path = Path(path).absolute()
    require(path.resolve() == path, 'symlink or noncanonical path: '+str(path))
    fd = os.open(path, os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        require(stat.S_ISREG(os.fstat(fd).st_mode), 'regular file required: '+str(path))
        return os.fdopen(fd, 'rb')
    except BaseException:
        os.close(fd)
        raise


def file_pin(path):
    digest, size = hashlib.sha256(), 0
    with regular(path) as source:
        for chunk in iter(lambda: source.read(65536), b''):
            digest.update(chunk); size += len(chunk)
    return dict(bytes=size, sha256=digest.hexdigest())


def read(path, limit=4*1024**2):
    with regular(path) as source:
        body = source.read(limit+1)
    require(len(body) <= limit, 'file cap: '+str(path))
    return body


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write(path, body):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    require(path.absolute().resolve() == path.absolute(), 'write path symlink')
    with path.open('xb') as output:
        output.write(body); output.flush(); os.fsync(output.fileno())
    sync_directory(path.parent)


def body_pin(value):
    require(type(value) is dict and set(value) == {'bytes', 'sha256'}, 'artifact identity fields')
    require(type(value['bytes']) is int and 0 <= value['bytes'] <= 64*1024**2, 'artifact byte cap')
    require(type(value['sha256']) is str and re.fullmatch('[0-9a-f]{64}', value['sha256']), 'artifact digest')
    return value


def object_key(key):
    require(type(key) is str and re.fullmatch(r'[A-Za-z0-9_./-]{1,1024}', key)
            and not key.startswith('/') and all(p not in ('', '.', '..') for p in key.split('/')), 'object key')


def native_inputs(native):
    return [a for p in native['panels'] for a in (p['root'], p['graph'])]+[native['original_seal'], native['prefix']]


def validate_config(config, base=None):
    require(set(config) == {'schema', 'authority_pending', 'fixed', 'native_config',
        'native_config_sha256', 'binary', 'inputs', 'native_qualification', 'code_sha256',
        'source_archive_paths', 'source_archive_paths_sha256'}, 'frozen config fields')
    require(config['schema'] == SCHEMA and config['authority_pending'] is False, 'root must freeze authority')
    require(encoded(config['fixed']) == encoded(FIXED), 'fixed resource/method contract')
    native = config['native_config']
    require(set(native) == {'schema', 'caps', 'panels', 'original_seal', 'prefix'}
            and native['schema'] == 'borsuk-fine-pack-config-v1'
            and encoded(native['caps']) == encoded(CAPS), 'native config contract')
    require(len(native['panels']) == 2, 'two panels')
    for p, dataset in zip(native['panels'], ('relaion', 'cohere')):
        require(set(p) == {'dataset', 'identity', 'root', 'graph'} and p['dataset'] == dataset, 'native panel')
        identity = p['identity']
        require(set(identity) == {'rows', 'dimensions', 'generation', 'source', 'layout', 'pq'}
                and type(identity['rows']) is int and identity['rows'] == 100000
                and type(identity['dimensions']) is int and identity['dimensions'] == 768
                and type(identity['generation']) is int and 0 <= identity['generation'] < 2**64, 'panel geometry')
        for name in ('source', 'layout', 'pq'):
            require(type(identity[name]) is list and len(identity[name]) == 32
                    and all(type(v) is int and 0 <= v <= 255 for v in identity[name]), 'layout identity')
    require(sha(encoded(native)) == config['native_config_sha256'], 'native config digest')
    require(len(config['inputs']) == 6, 'six input bodies only')
    for descriptor, transport, (path, size, digest) in zip(native_inputs(native), config['inputs'], INPUT_PINS):
        require(descriptor == dict(path=path, bytes=size, sha256=digest), 'historical input pin')
        require(set(transport) == {'destination', 'key', 'bytes', 'sha256'}
                and transport['destination'] == path and body_pin({k:transport[k] for k in ('bytes', 'sha256')})
                == dict(bytes=size, sha256=digest), 'exact native input transport')
        object_key(transport['key'])
    require(len({i['key'] for i in config['inputs']}) == 6, 'distinct input objects')
    require(config['binary'] == dict(BINARY_PIN, key=BINARY_KEY), 'qualified executable pin')
    require(config['native_qualification'] == list(RECEIPTS), 'qualified original receipts')
    require(set(config['code_sha256']) == set(CODE), 'code closure')
    for name, digest in config['code_sha256'].items():
        body_pin(dict(bytes=0, sha256=digest))
        if base is not None:
            require(file_pin(Path(base)/name)['sha256'] == digest, 'code drift: '+name)
    # Root fills this roster after integration. Neither config nor archive hash
    # is embedded in itself; qualification and terminal bind their actual bytes.
    paths = sorted([str(CONFIG), *CODE, *(r['path'] for r in RECEIPTS)])
    require(config['source_archive_paths'] == paths
            and config['source_archive_paths_sha256'] == sha(json.dumps(paths, separators=(',', ':')).encode()), 'minimal archive closure')
    if base is not None:
        for receipt in RECEIPTS:
            require(file_pin(Path(base)/receipt['path']) == {k:receipt[k] for k in ('bytes', 'sha256')}, 'qualification drift')
    return native


def preflight(base=Path('.')):
    raw = read(base/CONFIG)
    config = decode(raw)
    validate_config(config, base)
    return dict(config_path=str(CONFIG), config_sha256=sha(raw), campaign_schema=SCHEMA,
                artifact_roster_sha256=ROSTER_SHA, native_config_sha256=config['native_config_sha256'],
                binary=config['binary'], native_source_commit=NATIVE_COMMIT, source_identity_sha256=SOURCE_ID,
                code_sha256=config['code_sha256'], native_qualification=config['native_qualification'],
                source_archive_paths=config['source_archive_paths'],
                source_archive_paths_sha256=config['source_archive_paths_sha256'])


def lifecycle():
    from scripts import launch_native_metadata_ranges_cold_spot as shared
    return shared


def poll(ec2, s3, prefix, instance_id, started):
    shared = lifecycle()
    original = shared.WALL
    try:
        shared.WALL = WALL
        shared.poll(ec2, s3, prefix, instance_id, started)
    finally:
        shared.WALL = original


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    require(re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha), 'source archive pins')
    require(re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', prefix), 'attempt prefix')
    object_key(archive_key)
    require(qualification['config_path'] == str(CONFIG), 'bootstrap config path')
    # SDK bootstrap, IMDSv2, one request attempt, terminal last; no toolchain.
    body = f'''#!/bin/bash
set -euo pipefail
export AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard AWS_DEFAULT_REGION={REGION}
systemd-run --unit=fine-pack-stop --on-active={WALL}s /usr/sbin/shutdown -h now
root={REMOTE_ROOT}
mkdir "$root"
cd "$root"
trap '/usr/sbin/shutdown -h now || true' EXIT
exec >run.log 2>&1
test "$(uname -m)" = x86_64
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3.12 python3.12-venv tar gzip
python3.12 -m venv "$root/venv"
python="$root/venv/bin/python"
"$python" -m pip install --retries 0 --timeout 15 --no-cache-dir --disable-pip-version-check --only-binary=:all: --no-deps boto3==1.40.72 botocore==1.40.72 jmespath==1.0.1 s3transfer==0.14.0 python-dateutil==2.9.0.post0 six==1.17.0 urllib3==2.6.3
"$python" - <<'PY'
import boto3, hashlib, tarfile
import botocore.session
from pathlib import Path
from botocore.config import Config
assert boto3.__version__=='1.40.72' and botocore.__version__=='1.40.72'
assert 'IfNoneMatch' in botocore.session.get_session().get_service_model('s3').operation_model('PutObject').input_shape.members
s3=boto3.client('s3',region_name='{REGION}',config=Config(retries={{'total_max_attempts':1}}))
response=s3.get_object(Bucket='{BUCKET}',Key='{archive_key}')
digest=hashlib.sha256()
with response['Body'] as source, open('source.tar.gz','xb') as output:
    while chunk:=source.read(65536):
        digest.update(chunk); output.write(chunk)
assert digest.hexdigest()=='{archive_sha}'
Path('repo').mkdir()
with tarfile.open('source.tar.gz','r:gz') as archive:
    archive.extractall('repo',filter='data')
PY
export PYTHONPATH="$root/repo"
"$python" -m {MODULE} --remote "$root/repo" "$root" '{commit}' '{archive_sha}' '{prefix}' '{qualification['config_sha256']}'
'''
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    require(len(body.encode()) < 16384, 'EC2 userdata cap')
    return body


def download(s3, descriptor, destination):
    response = s3.get_object(Bucket=BUCKET, Key=descriptor['key'])
    expected = body_pin({k:descriptor[k] for k in ('bytes', 'sha256')})
    with response['Body'] as source:
        require(response['ContentLength'] == expected['bytes'], 'transport ContentLength')
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        require(destination.absolute().resolve() == destination.absolute(), 'staging symlink')
        digest, total = hashlib.sha256(), 0
        with destination.open('xb') as output:
            while chunk := source.read(min(65536, expected['bytes']-total+1)):
                total += len(chunk)
                require(total <= expected['bytes'], 'transport body exceeds descriptor')
                digest.update(chunk); output.write(chunk)
            require(dict(bytes=total, sha256=digest.hexdigest()) == expected, 'transport body pin')
            output.flush(); os.fsync(output.fileno())
    sync_directory(destination.parent)


def sdk_guard():
    import boto3
    import botocore.session
    require(boto3.__version__ == botocore.__version__ == '1.40.72', 'pinned SDK interpreter')
    members = botocore.session.get_session().get_service_model('s3').operation_model('PutObject').input_shape.members
    require('IfNoneMatch' in members, 'SDK lacks immutable PutObject.IfNoneMatch')
    return dict(boto3=boto3.__version__, botocore=botocore.__version__, put_object_if_none_match=True)


def runtime_abi():
    release = dict(line.split('=',1) for line in Path('/etc/os-release').read_text().splitlines() if '=' in line)
    return dict(machine=platform.machine(), python=list(sys.version_info[:2]),
                os={k:release[k].strip('"') for k in ('ID','VERSION_ID')},
                libc=list(platform.libc_ver()), sdk=sdk_guard())


def validate_abi(abi):
    require(abi == dict(machine='x86_64', python=[3,12], os=dict(ID='ubuntu',VERSION_ID='24.04'),
        libc=['glibc','2.39'], sdk=dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True)), 'selected x86 Ubuntu/Python/native ABI and SDK')


def stage(s3, config, root):
    validate_config(config)
    download(s3, config['binary'], root/BINARY_NAME)
    (root/BINARY_NAME).chmod(0o500)
    write(root/'native-config.json', encoded(config['native_config']))
    for descriptor in config['inputs']:
        download(s3, descriptor, descriptor['destination'])
    result = dict(binary=file_pin(root/BINARY_NAME), native_config=file_pin(root/'native-config.json'),
                  inputs={d['destination']:file_pin(d['destination']) for d in config['inputs']},
                  exact_six_inputs=True, compiler_used=False)
    write(root/'stage-receipt.json', encoded(result))
    return result


def cgroup_snapshot(group):
    return dict(path=str(group), observer_pid=os.getpid(),
                files={name:(group/name).read_text() for name in CGROUP_FILES})


def events(body):
    return {k:int(v) for k, v in (line.split() for line in body.splitlines())}


def validate_resources(resource):
    require(resource['closed'] is True and resource['deadline_exceeded'] is False, 'resource closure/deadline')
    before, after = resource['before'], resource['after']
    require(before['path'] == after['path'], 'same original cgroup')
    for snapshot in (before, after):
        f = snapshot['files']
        require(set(f) == set(CGROUP_FILES), 'mandatory kernel resource counters')
        require(int(f['memory.max']) == CAPS['memory_bytes']
                and 0 <= int(f['memory.peak']) <= CAPS['memory_bytes'], 'native kernel memory cap/peak')
        require(int(f['memory.swap.max']) == int(f['memory.swap.peak']) == 0, 'native swap')
        quota, period = map(int, f['cpu.max'].split())
        require(quota == period and period > 0, 'CPU1 quota')
        require(int(f['pids.max']) == FIXED['tasks_max'] and int(f['pids.current']) == 0
                and not f['cgroup.procs'].strip() and events(f['cgroup.events'])['populated'] == 0, 'native descendants drained')
        require(events(f['cpu.stat'])['usage_usec'] >= 0, 'CPU evidence')
        for key in ('memory.events', 'memory.swap.events', 'pids.events'):
            counters = events(f[key])
            required = ('oom', 'oom_kill', 'oom_group_kill') if key == 'memory.events' else tuple(counters)
            require(counters and all(counters.get(k, -1) == 0 for k in required), 'resource failure: '+key)
    for key in ('memory.events', 'memory.swap.events', 'pids.events', 'cpu.stat'):
        a, b = events(before['files'][key]), events(after['files'][key])
        require(set(a) == set(b) and all(b[k] >= a[k] for k in a), 'resource counter regression')
    require(resource['cpu_affinity'] == [0] and resource['supervisor_outside_native_cgroup'] is True
            and resource['process_attached_before_exec'] is True
            and resource['descendants_remaining_after_exit'] is False
            and 0 <= resource['elapsed_seconds'] <= CAPS['deadline_seconds'], 'original native resource authority')


def create_group(group):
    for name, value in (('memory.max', str(CAPS['memory_bytes'])), ('memory.swap.max', '0'),
                        ('cpu.max', '100000 100000'), ('pids.max', str(FIXED['tasks_max'])),
                        ('memory.oom.group', '1')):
        (group/name).write_text(value)


def drain_group(group):
    if (group/'cgroup.procs').read_text().strip():
        (group/'cgroup.kill').write_text('1')
    deadline = time.monotonic()+5
    while events((group/'cgroup.events').read_text())['populated']:
        require(time.monotonic() < deadline, 'cgroup drain deadline')
        time.sleep(.05)


def supervise(config, root, *, run_id, group=None):
    """Popen owns the original executable; the observer stays outside its cap."""
    group = group or Path('/sys/fs/cgroup')/('borsuk-fine-pack-'+str(os.getpid()))
    command = [str(root/BINARY_NAME), 'check-fine-pack', str(root/'native-config.json'),
               config['native_config_sha256'], str(root/'screen/report.json')]
    (root/'screen').mkdir(exist_ok=False)
    receipt = dict(command=command, process_exit_code=None, process_started=False,
                   config_sha256=config['native_config_sha256'], binary=config['binary'], run_id=run_id)
    resource = dict(closed=False, deadline_exceeded=False, before=None, after=None,
                    cpu_affinity=[0], process_attached_before_exec=False,
                    supervisor_outside_native_cgroup=False, elapsed_seconds=0,
                    descendants_remaining_after_exit=True)
    cleanup = dict(drain_complete=False, cleanup_complete=False, output_durable=False)
    process, created, error = None, False, None
    started = time.monotonic()
    old_term = signal.getsignal(signal.SIGTERM)
    def interrupted(signum, frame):
        raise InterruptedError('original supervisor interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        group.mkdir(exist_ok=False); created = True; create_group(group)
        resource['before'] = cgroup_snapshot(group)
        require(not resource['before']['files']['cgroup.procs'].strip(), 'fresh native cgroup')
        observer = Path('/proc/self/cgroup').read_text()
        require(str(group).removeprefix('/sys/fs/cgroup') not in observer, 'supervisor outside native cap')
        resource['supervisor_outside_native_cgroup'] = True
        fd = os.open(group/'cgroup.procs', os.O_WRONLY)
        def attach():
            os.sched_setaffinity(0, {0})
            os.write(fd, str(os.getpid()).encode())
        environment = dict(os.environ, BORSUK_CPU_THREADS='1', RAYON_NUM_THREADS='1',
                           TOKIO_WORKER_THREADS='1', OMP_NUM_THREADS='1', AWS_MAX_ATTEMPTS='1')
        try:
            with (root/'native.log').open('xb') as log:
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                    env=environment, preexec_fn=attach, pass_fds=(fd,), start_new_session=True)
                receipt.update(process_started=True, process_id=process.pid)
                resource['process_attached_before_exec'] = True
                while process.poll() is None:
                    resource['elapsed_seconds'] = time.monotonic()-started
                    if resource['elapsed_seconds'] >= CAPS['deadline_seconds']:
                        resource['deadline_exceeded'] = True
                        raise TimeoutError('native 300s deadline')
                    require(log.tell() <= 4*1024**2, 'native log cap')
                    time.sleep(.05)
                receipt['process_exit_code'] = process.wait()
                resource['descendants_remaining_after_exit'] = bool((group/'cgroup.procs').read_text().strip())
                log.flush(); os.fsync(log.fileno())
        finally:
            os.close(fd)
    except BaseException as failure:
        error = failure
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        resource['elapsed_seconds'] = time.monotonic()-started
        try:
            if created:
                drain_group(group)
                cleanup['drain_complete'] = True
                if process is not None:
                    receipt['process_exit_code'] = process.wait(timeout=5)
                resource['after'] = cgroup_snapshot(group); resource['closed'] = True
                group.rmdir(); cleanup['cleanup_complete'] = True
            for name in ('native.log', 'screen/report.json', 'screen/report.permutations.json', 'screen/report.prefix.jsonl'):
                path = root/name
                if path.exists():
                    with regular(path) as output:
                        os.fsync(output.fileno())
            sync_directory(root/'screen'); sync_directory(root)
            cleanup['output_durable'] = True
        except BaseException as failure:
            error = error or failure
            if process is not None and process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                receipt['process_exit_code'] = process.wait(timeout=5)
        if error is not None:
            receipt['error'] = dict(type=type(error).__name__, message=str(error))
        write(root/'native-exit.json', encoded(receipt))
        write(root/'resources.json', encoded(resource))
        write(root/'cleanup.json', encoded(cleanup))
        signal.signal(signal.SIGTERM, old_term)
    return receipt


def validate_result(root, config):
    validate_config(config)
    validate_abi(decode(read(root/'runtime-abi.json')))
    require(file_pin(root/'native-config.json') == pin(encoded(config['native_config'])), 'native config bytes')
    require(file_pin(root/BINARY_NAME) == BINARY_PIN, 'executed binary bytes')
    exit_receipt = decode(read(root/'native-exit.json'))
    resource, cleanup = (decode(read(root/name)) for name in ('resources.json', 'cleanup.json'))
    require(type(exit_receipt['process_exit_code']) is int and exit_receipt['process_exit_code'] == 0
            and exit_receipt['process_started'] is True and 'error' not in exit_receipt, 'original native exit zero')
    command = exit_receipt['command']
    expected_root = Path(command[0]).parent.parent
    require(command == [str(expected_root/BINARY_NAME), 'check-fine-pack', str(expected_root/'native-config.json'),
                config['native_config_sha256'], str(expected_root/'screen/report.json')]
            and exit_receipt['config_sha256'] == config['native_config_sha256']
            and exit_receipt['binary'] == config['binary'], 'original executable/config/command binding')
    validate_resources(resource)
    require(all(cleanup.get(k) is True for k in ('drain_complete', 'cleanup_complete', 'output_durable')), 'native cleanup/durability')
    stage_receipt = decode(read(root/'stage-receipt.json'))
    require(stage_receipt == dict(binary=BINARY_PIN, native_config=pin(encoded(config['native_config'])),
            inputs={d['destination']:{k:d[k] for k in ('bytes', 'sha256')} for d in config['inputs']},
            exact_six_inputs=True, compiler_used=False), 'authenticated original staging')
    body = read(root/'screen/report.json', CAPS['output_bytes'])
    report = decode(body)
    require(report['schema'] == 'borsuk-fine-pack-diagnostic-v1' and report['complete'] is True
            and type(report['queries']) is int and report['queries'] == 128
            and report['config_sha256'] == config['native_config_sha256']
            and report['diagnostic_source_sha256'] == DIAGNOSTIC_SOURCES, 'native complete report binding')
    for key, expected in (('standalone_authority', False), ('requires_matching_supervisor_exit_receipt', True),
            ('quality_or_performance_claim', False), ('truth_opened', False), ('requests_opened', False),
            ('sq8_canonical_pq_bodies_opened', False)):
        require(report[key] is expected, 'native report scope: '+key)
    details = report['details']
    require(details['original_seal'] == config['native_config']['original_seal']
            and details['original_seal_bytes_sha256'] == INPUT_PINS[4][2]
            and details['caps'] == CAPS and 0 <= details['operations'] <= CAPS['operations']
            and 0 <= details['wall_ms'] <= 300000, 'native method/resources binding')
    for key, name in (('permutation_seal', 'screen/report.permutations.json'), ('prefix', 'screen/report.prefix.jsonl')):
        require(details[key] == dict(file_pin(root/name), path=str(expected_root/name)), 'native companion pin: '+key)
    require(file_pin(root/'screen/report.prefix.jsonl') == {k:config['native_config']['prefix'][k] for k in ('bytes', 'sha256')}, 'consumed prefix')
    permutation = decode(read(root/'screen/report.permutations.json', CAPS['output_bytes']))
    native = config['native_config']
    require(permutation['schema'] == 'borsuk-fine-pack-permutations-v1'
            and permutation['config_sha256'] == config['native_config_sha256']
            and permutation['diagnostic_source_sha256'] == DIAGNOSTIC_SOURCES
            and permutation['original_seal'] == native['original_seal']
            and permutation['nomination_prefix'] == native['prefix']
            and permutation['panels'] == native['panels'] and permutation['group_rows'] == 16
            and permutation['groups_per_pack'] == 42 and permutation['query_blind'] is True
            and permutation['original_trace_source_identity_sha256'] == details['original_trace_source_identity_sha256'], 'joint permutation binding')
    require(len(permutation['old_to_new']) == len(permutation['permutation_sha256_le_u32']) == 2, 'both permutations')
    for mapping, digest in zip(permutation['old_to_new'], permutation['permutation_sha256_le_u32']):
        require(len(mapping) == 6250 and all(type(v) is int for v in mapping)
                and sorted(mapping) == list(range(6250)), 'group bijection')
        require(sha(b''.join(v.to_bytes(4, 'little') for v in mapping)) == digest, 'permutation LE32 hash')
    replay = details['replay']
    require(len(replay['queries']) == 128, 'all 128 plans completed')
    passes, maxima = [0, 0], [0, 0]
    for index, q in enumerate(replay['queries']):
        panel = index//64
        require(q['dataset'] == native['panels'][panel]['dataset'] and q['ordinal'] == index%64
                and q['all_nominees_retained'] is True and 1 <= q['nominees'] <= 1024, '128 frozen plan identities')
        ranges = q['ranges']
        require(1 <= len(ranges) <= 32, 'explicit cover cap32')
        end, count = 0, 0
        for r in ranges:
            require(set(r) == {'start', 'end'} and all(type(r[k]) is int for k in r)
                    and end <= r['start'] < r['end'] <= 100000*780
                    and r['start']%780 == r['end']%780 == 0, 'cover range geometry')
            end = r['end']; count += r['end']-r['start']
        fits = count <= 16*1024**2
        require(type(q['planned_bytes']) is int and q['planned_bytes'] == count and q['fits'] is fits, 'cover bytes/disposition')
        passes[panel] += fits; maxima[panel] = max(maxima[panel], count)
    survived = passes == [64, 64]
    require(replay['per_panel_passes'] == passes and replay['per_panel_max_bytes'] == maxima
            and replay['survived'] is survived and report['status'] == ('SURVIVED_NECESSARY_LOCALITY' if survived else 'REJECT'), 'completed locality disposition')
    require(sum(file_pin(root/n)['bytes'] for n in ARTIFACTS if n.startswith('screen/')) <= CAPS['output_bytes'], 'native total output cap')
    return dict(status=report['status'], valid_diagnostic=True, report_sha256=sha(body),
                native_config_sha256=config['native_config_sha256'], binary=BINARY_PIN,
                per_panel_passes=passes, per_panel_max_bytes=maxima, quality_or_performance_claim=False)


def publish(s3, root, prefix, terminal):
    """Retain every present raw body, read back, then publish the terminal."""
    artifacts = {}
    for name in ARTIFACTS:
        path = root/name
        if path.exists():
            body = read(path, 64*1024**2)
            ident = pin(body)
            key = prefix+'/artifacts/'+name
            s3.put_object(Bucket=BUCKET, Key=key, Body=body, IfNoneMatch='*')
            response = s3.get_object(Bucket=BUCKET, Key=key)
            with response['Body'] as source:
                require(pin(source.read(ident['bytes']+1)) == ident, 'upload readback: '+name)
            artifacts[name] = ident
    terminal['artifacts'] = artifacts
    raw = encoded(terminal)
    write(root/'terminal.json', raw)
    s3.put_object(Bucket=BUCKET, Key=prefix+'/terminal.json', Body=raw, IfNoneMatch='*')
    return terminal


def remote(repo, root, commit, archive_sha, prefix, config_sha):
    import urllib.request
    import boto3
    from botocore.config import Config
    require(re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', prefix), 'remote prefix')
    sdk_guard()
    s3 = boto3.client('s3', region_name=REGION, config=Config(retries={'total_max_attempts':1}))
    request = urllib.request.Request('http://169.254.169.254/latest/api/token', method='PUT',
                                    headers={'X-aws-ec2-metadata-token-ttl-seconds':'60'})
    with urllib.request.urlopen(request, timeout=5) as response:
        token = response.read(4096).decode()
    request = urllib.request.Request('http://169.254.169.254/latest/meta-data/instance-id',
                                    headers={'X-aws-ec2-metadata-token':token})
    with urllib.request.urlopen(request, timeout=5) as response:
        instance_id = response.read(128).decode()
    terminal = dict(schema=SCHEMA, source_commit=commit, source_archive_sha256=archive_sha,
        instance_id=instance_id, prefix=prefix, config_sha256=config_sha,
        status='failed', phase='execution', exit_code=96, original_exit_code=None,
        disposition='INVALID', artifact_roster_sha256=ROSTER_SHA)
    try:
        abi = runtime_abi(); write(root/'runtime-abi.json', encoded(abi)); validate_abi(abi)
        config_body = read(repo/CONFIG)
        require(sha(config_body) == config_sha, 'root config authentication')
        config = decode(config_body); validate_config(config, repo)
        proof = preflight(repo)
        write(root/'config.json', config_body); write(root/'source-qualification.json', encoded(proof))
        write(root/'cpu.txt', subprocess.check_output(['lscpu']))
        stage(s3, config, root)
        receipt = supervise(config, root, run_id=prefix+'/'+instance_id)
        terminal['original_exit_code'] = receipt['process_exit_code']
        result = validate_result(root, config)
        terminal.update(status='complete', phase='complete', exit_code=0, disposition=result['status'],
                        result=result, qualification=proof)
    except BaseException as error:
        terminal['error'] = dict(type=type(error).__name__, message=str(error))
    finally:
        if (root/'native-exit.json').exists():
            terminal['original_exit_code'] = decode(read(root/'native-exit.json'))['process_exit_code']
        write(root/'run-closed.log', read(root/'run.log'))
        publish(s3, root, prefix, terminal)
    return terminal['exit_code']


def replay(out):
    terminal = decode(read(out/'aws-terminal.json'))
    reservation, launch, close = (decode(read(out/name)) for name in ('aws-reservation.json', 'aws-launch.json', 'aws-closeout.json'))
    require(close['state'] == 'terminated' and close['nodes'] == launch['nodes']
            == {'0':{'instance_id':launch['instance_id']}}, 'same acknowledged instance terminated/waited')
    require(terminal['schema'] == reservation['schema'] == SCHEMA
            and terminal['instance_id'] == launch['instance_id'] and terminal['prefix'] == launch['prefix']
            and re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', terminal['prefix']), 'terminal attempt/instance identity')
    for key in ('source_commit', 'source_archive_sha256'):
        require(terminal[key] == reservation[key] == launch[key], 'terminal source binding: '+key)
    require(terminal['config_sha256'] == reservation['config_sha256']
            and terminal['artifact_roster_sha256'] == ROSTER_SHA, 'terminal frozen config/roster')
    require(set(terminal['artifacts']) <= set(ARTIFACTS), 'terminal artifact roster')
    for name, identity in terminal['artifacts'].items():
        body_pin(identity)
        require(file_pin(out/name) == identity, 'collected full artifact hash: '+name)
        if (out/(name+'.gz')).exists():
            with regular(out/(name+'.gz')) as compressed, gzip.GzipFile(fileobj=compressed) as source:
                require(pin(source.read(identity['bytes']+1)) == identity, 'transport gzip hash: '+name)
    if terminal['status'] != 'complete':
        return dict(valid_diagnostic=False, status='INVALID', original_exit_code=terminal['original_exit_code'])
    require(terminal['phase'] == 'complete' and type(terminal['exit_code']) is int
            and type(terminal['original_exit_code']) is int
            and terminal['exit_code'] == terminal['original_exit_code'] == 0
            and set(terminal['artifacts']) == set(ARTIFACTS), 'complete original terminal/roster')
    raw = read(out/'config.json')
    require(sha(raw) == terminal['config_sha256'], 'collected config hash')
    config = decode(raw); validate_config(config)
    proof = decode(read(out/'source-qualification.json'))
    require(proof == reservation['qualification'] == terminal['qualification']
            and proof['config_sha256'] == sha(raw) and proof['code_sha256'] == config['code_sha256']
            and proof['native_qualification'] == config['native_qualification'], 'original qualification binding')
    result = validate_result(out, config)
    require(decode(read(out/'native-exit.json'))['run_id'] == terminal['prefix']+'/'+terminal['instance_id'], 'original run ID')
    require(result == terminal['result'] and terminal['disposition'] == result['status'], 'original result receipt')
    return result


def collect(s3, prefix, out, instance_id, commit, digest):
    response = s3.get_object(Bucket=BUCKET, Key=prefix+'/terminal.json')
    with response['Body'] as source:
        raw = source.read(4*1024**2+1)
    require(len(raw) <= 4*1024**2, 'terminal cap')
    write(out/'aws-terminal.json', raw)
    terminal = decode(raw)
    require(terminal['instance_id'] == instance_id and terminal['source_commit'] == commit
            and terminal['source_archive_sha256'] == digest and terminal['prefix'] == prefix
            and terminal['schema'] == SCHEMA and set(terminal['artifacts']) <= set(ARTIFACTS), 'original terminal identity')
    for name, identity in terminal['artifacts'].items():
        body_pin(identity)
        response = s3.get_object(Bucket=BUCKET, Key=prefix+'/artifacts/'+name)
        with response['Body'] as source:
            body = source.read(identity['bytes']+1)
        require(pin(body) == identity, 'remote full artifact hash: '+name)
        write(out/name, body); write(out/(name+'.gz'), gzip.compress(body, mtime=0))
    result = replay(out)
    write(out/'collection-replay.json', encoded(result))
    return terminal


def self_check():
    """Real fake-native original processes; cgroup, SDK, IMDS and AWS mocked.

    Run under CPU1/256MiB/swap0/120s. No corpus, graph, native ANN or network.
    """
    import copy
    import io
    import tempfile
    from datetime import datetime, timezone
    from unittest.mock import Mock, patch
    from contextlib import ExitStack
    module = sys.modules[__name__]
    shared = lifecycle()
    # Actual installed service model, no client/credentials/network.
    require(sdk_guard() == dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True), 'real installed immutable SDK model')
    import botocore.session
    old_model=Mock()
    old_model.get_service_model.return_value.operation_model.return_value.input_shape.members={}
    with patch.object(botocore.session,'get_session',return_value=old_model):
        try:
            sdk_guard()
        except ValueError as error:
            require('IfNoneMatch' in str(error),'old SDK model rejection')
        else:
            raise AssertionError('old model without immutable PUT accepted')
    old_path=Path('/tmp/borsuk-canary-botocore-1.34.46-s3.json')
    official_old_checked=False
    if old_path.exists():
        from botocore.model import ServiceModel
        old_body=read(old_path)
        require(sha(old_body)=='0e0760c9ae10e9f8af267b0d9708fefabbaec023370f0044bbecc51565f05699','authenticated official old service model')
        service=ServiceModel(decode(old_body),service_name='s3')
        session=Mock();session.get_service_model.return_value=service
        with patch.object(botocore.session,'get_session',return_value=session):
            try:sdk_guard()
            except ValueError as error:require('IfNoneMatch' in str(error),'official old model refused')
            else:raise AssertionError('official old SDK model accepted')
        official_old_checked=True
    fake = '''#!/usr/bin/env python3
import hashlib,json,os,sys,time
from pathlib import Path
_,mode,config_path,config_sha,out=sys.argv
assert mode=='check-fine-pack'
c=json.loads(Path(config_path).read_bytes());p=Path(out)
def sha(b):return hashlib.sha256(b).hexdigest()
def emit(path,value):
 b=json.dumps(value,sort_keys=True,separators=(',',':')).encode()
 with path.open('xb') as f:f.write(b);f.flush();os.fsync(f.fileno())
 return {'path':str(path),'bytes':len(b),'sha256':sha(b)}
sources=SOURCES
trace='0'*64
maps=[list(range(6250)),list(reversed(range(6250)))]
perms=emit(p.with_suffix('.permutations.json'),{'schema':'borsuk-fine-pack-permutations-v1',
 'config_sha256':config_sha,'diagnostic_source_sha256':sources,'old_to_new':maps,
 'permutation_sha256_le_u32':[sha(b''.join(v.to_bytes(4,'little') for v in m)) for m in maps],
 'original_trace_source_identity_sha256':trace,'original_seal':c['original_seal'],
 'nomination_prefix':c['prefix'],'panels':c['panels'],'group_rows':16,'groups_per_pack':42,'query_blind':True})
prefix=p.with_suffix('.prefix.jsonl');body=Path(c['prefix']['path']).read_bytes()
with prefix.open('xb') as f:f.write(body);f.flush();os.fsync(f.fileno())
prefix_pin={'path':str(prefix),'bytes':len(body),'sha256':sha(body)}
reject=os.environ.get('FINE_PACK_FAKE')=='reject'
queries=[]
for panel in c['panels']:
 for ordinal in range(64):
  size=780*22000 if reject and ordinal==0 else 780
  queries.append({'dataset':panel['dataset'],'ordinal':ordinal,'nominees':1,'all_nominees_retained':True,
   'ranges':[{'start':0,'end':size}],'planned_bytes':size,'fits':size<=16*1024**2})
emit(p,{'schema':'borsuk-fine-pack-diagnostic-v1','complete':True,'queries':128,
 'status':'REJECT' if reject else 'SURVIVED_NECESSARY_LOCALITY','config_sha256':config_sha,
 'diagnostic_source_sha256':sources,'standalone_authority':False,'requires_matching_supervisor_exit_receipt':True,
 'quality_or_performance_claim':False,'truth_opened':False,'requests_opened':False,'sq8_canonical_pq_bodies_opened':False,
 'details':{'original_seal':c['original_seal'],'original_seal_bytes_sha256':c['original_seal']['sha256'],
 'original_trace_source_identity_sha256':trace,'caps':c['caps'],'operations':1,'wall_ms':1,
 'prefix':prefix_pin,'permutation_seal':perms,'replay':{'queries':queries,'survived':not reject,
 'per_panel_passes':[63,63] if reject else [64,64],'per_panel_max_bytes':[780*22000]*2 if reject else [780,780]}}})
fd=os.open(p.parent,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
if os.environ.get('FINE_PACK_FAKE')=='deadline':time.sleep(10)
sys.exit(17 if os.environ.get('FINE_PACK_FAKE')=='nonzero' else 0)
'''.replace('SOURCES', repr(DIAGNOSTIC_SOURCES)).encode()
    fake_pin = pin(fake)
    def failure(call, label):
        try:
            call()
        except (ValueError, FileNotFoundError, FileExistsError, OSError, KeyError, AssertionError):
            return
        raise AssertionError('accepted '+label)
    with tempfile.TemporaryDirectory(prefix='fine-pack-check-') as tmp, ExitStack() as stack:
        base = Path(tmp)
        inputs = base/'inputs'
        inputs.mkdir()
        input_pins = []
        for i in range(6):
            path, body = inputs/str(i), ('synthetic body '+str(i)+'\n').encode()
            write(path, body); input_pins.append((str(path), len(body), sha(body)))
        stack.enter_context(patch.object(module, 'INPUT_PINS', tuple(input_pins)))
        stack.enter_context(patch.object(module, 'BINARY_PIN', fake_pin))
        descriptors = [dict(path=p, bytes=n, sha256=h) for p,n,h in input_pins]
        native = dict(schema='borsuk-fine-pack-config-v1', caps=copy.deepcopy(CAPS),
            panels=[dict(dataset=dataset, root=descriptors[i*2], graph=descriptors[i*2+1],
                identity=dict(rows=100000, dimensions=768, generation=i+1, source=[0]*32, layout=[1]*32, pq=[2]*32))
                for i,dataset in enumerate(('relaion','cohere'))], original_seal=descriptors[4], prefix=descriptors[5])
        paths = sorted([str(CONFIG), *CODE, *(r['path'] for r in RECEIPTS)])
        config = dict(schema=SCHEMA, authority_pending=False, fixed=copy.deepcopy(FIXED), native_config=native,
            native_config_sha256=sha(encoded(native)), binary=dict(fake_pin, key=BINARY_KEY),
            inputs=[dict(destination=d['path'], key='synthetic/'+str(i), bytes=d['bytes'], sha256=d['sha256']) for i,d in enumerate(descriptors)],
            native_qualification=list(RECEIPTS), code_sha256={n:file_pin(n)['sha256'] for n in CODE},
            source_archive_paths=paths, source_archive_paths_sha256=sha(json.dumps(paths,separators=(',',':')).encode()))
        validate_config(config)
        failures = 0
        for key, value in [('authority_pending',True), ('native_config_sha256','0'*64), ('extra',True),
                           ('source_archive_paths_sha256','0'*64), ('native_qualification',[])]:
            bad=copy.deepcopy(config);bad[key]=value
            failure(lambda:validate_config(bad), 'config '+key); failures+=1
        bad=copy.deepcopy(config);bad['fixed']['native_caps']['memory_bytes']*=2
        failure(lambda:validate_config(bad), 'resource tuning'); failures+=1
        bad=copy.deepcopy(config);bad['inputs'][0]['destination']='/tmp/forbidden-requests'
        failure(lambda:validate_config(bad), 'foreign input'); failures+=1
        for body in (b'{"same":1,"same":2}',b'{"value":NaN}'):
            failure(lambda:decode(body), 'JSON duplicate/nonfinite'); failures+=1
        raw = encoded(config)
        proof = dict(config_path=str(CONFIG), config_sha256=sha(raw), campaign_schema=SCHEMA,
            artifact_roster_sha256=ROSTER_SHA, native_config_sha256=config['native_config_sha256'],
            binary=config['binary'], native_source_commit=NATIVE_COMMIT, source_identity_sha256=SOURCE_ID,
            code_sha256=config['code_sha256'], native_qualification=config['native_qualification'],
            source_archive_paths=paths, source_archive_paths_sha256=config['source_archive_paths_sha256'])
        userdata = user_data('a'*40,'b'*64,'synthetic/archive.tar.gz',PREFIX+'a0001',proof)
        require('total_max_attempts' in userdata and 'shutdown' in userdata and 'rustup' not in userdata
                and 'cargo' not in userdata, 'SDK/no-compiler bootstrap')
        compile(userdata.split("<<'PY'\n",1)[1].split('\nPY\n',1)[0], '<bootstrap>', 'exec')
        # Download executes the actual SDK body/auth/fsync/no-overwrite path.
        class Body(io.BytesIO):
            def read(self, n=-1):
                require(0 < n <= 64*1024**2+1, 'bounded SDK read')
                return super().read(n)
        bodies={'synthetic/'+str(i):read(inputs/str(i)) for i in range(6)}
        bodies[BINARY_KEY]=fake
        store={}
        s3=Mock()
        def get_object(**kwargs):
            require(kwargs['Bucket']==BUCKET, 'SDK bucket')
            body=(store if kwargs['Key'] in store else bodies)[kwargs['Key']]
            return dict(Body=Body(body), ContentLength=len(body))
        def put_object(**kwargs):
            require(kwargs['IfNoneMatch']=='*', 'immutable marker/artifact writes')
            require(kwargs['Key'] not in store, 'S3 no overwrite')
            store[kwargs['Key']]=kwargs['Body']
        s3.get_object.side_effect=get_object;s3.put_object.side_effect=put_object
        download(s3, config['binary'], base/'download')
        require(file_pin(base/'download')==fake_pin, 'download identity')
        failure(lambda:download(s3,config['binary'],base/'download'),'download overwrite');failures+=1
        for suffix, body in [('short',fake[:-1]),('long',fake+b'x'),('tamper',b'x'+fake[1:])]:
            bodies[BINARY_KEY]=body
            failure(lambda:download(s3,config['binary'],base/suffix),'transport '+suffix);failures+=1
        bodies[BINARY_KEY]=fake
        # Real original processes; only cgroup pseudo-files are substituted.
        files=dict(zip(CGROUP_FILES, (str(CAPS['memory_bytes']),'4096','0','0',
            'low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n',
            'high 0\nmax 0\nfail 0\n','100000 100000','usage_usec 1\nuser_usec 1\nsystem_usec 0\n',
            str(FIXED['tasks_max']),'0','max 0\n','','populated 0\nfrozen 0\n')))
        run_id=PREFIX+'a0001/i-original'
        def fixture(name, mode='success', deadline=False):
            root=base/name;root.mkdir()
            write(root/'config.json',raw);write(root/'source-qualification.json',encoded(proof))
            write(root/'native-config.json',encoded(native));write(root/BINARY_NAME,fake);(root/BINARY_NAME).chmod(0o700)
            write(root/'stage-receipt.json',encoded(dict(binary=fake_pin,native_config=pin(encoded(native)),
                inputs={d['destination']:{k:d[k] for k in ('bytes','sha256')} for d in config['inputs']},
                exact_six_inputs=True,compiler_used=False)))
            write(root/'cpu.txt',b'synthetic cpu\n');write(root/'run-closed.log',b'')
            write(root/'runtime-abi.json',encoded(dict(machine='x86_64',python=[3,12],
                os=dict(ID='ubuntu',VERSION_ID='24.04'),libc=['glibc','2.39'],
                sdk=dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True))))
            group=base/(name+'-cgroup')
            def create(g):
                (g/'cgroup.procs').write_text('')
            def snapshot(g):
                return dict(path=str(g),observer_pid=os.getpid(),files=copy.deepcopy(files))
            owned=[]
            def drain(g):
                if owned and owned[0].poll() is None:
                    os.killpg(owned[0].pid,signal.SIGKILL)
                (g/'cgroup.procs').unlink()
            real_popen=subprocess.Popen
            def spawn(command, **kwargs):
                p=real_popen(command,**kwargs)
                owned.append(p)
                # Synthetic cgroup cannot remove exited PIDs as a kernel does.
                (group/'cgroup.procs').write_text('')
                return p
            with patch.object(module,'create_group',side_effect=create), \
                 patch.object(module,'cgroup_snapshot',side_effect=snapshot), \
                 patch.object(module,'drain_group',side_effect=drain), \
                 patch.object(subprocess,'Popen',side_effect=spawn), \
                 patch.dict(os.environ,FINE_PACK_FAKE=mode), \
                 patch.dict(CAPS,deadline_seconds=.2 if deadline else 300):
                receipt=supervise(config,root,run_id=run_id,group=group)
            require(not group.exists(),'synthetic cgroup cleanup')
            return root,receipt
        root, receipt=fixture('original')
        require(receipt['process_exit_code']==0 and validate_result(root,config)['status']=='SURVIVED_NECESSARY_LOCALITY', 'original fake process closure')
        require(file_pin(root/'native.log')['bytes']==0,'silent exit0 log allowed')
        rejected, receipt=fixture('rejected','reject')
        require(receipt['process_exit_code']==0 and validate_result(rejected,config)['status']=='REJECT','completed all128 rejection')
        nonzero, receipt=fixture('nonzero','nonzero')
        require(receipt['process_exit_code']==17 and decode(read(nonzero/'screen/report.json'))['complete'] is True,'real original exit17')
        failure(lambda:validate_result(nonzero,config),'PASS body original exit17');failures+=1
        timeout, receipt=fixture('timeout','deadline',True)
        require(receipt['process_exit_code']==-signal.SIGKILL and decode(read(timeout/'resources.json'))['deadline_exceeded'] is True,'real deadline process killed')
        failure(lambda:validate_result(timeout,config),'deadline PASS body');failures+=1
        failure(lambda:supervise(config,root,run_id=run_id,group=base/'never-created'),'output overwrite');failures+=1
        def tamper(name, change):
            path=root/name;original=read(path);path.write_bytes(change(original))
            try:failure(lambda:validate_result(root,config),'tamper '+name)
            finally:path.write_bytes(original)
        def changed(body, callback):
            value=decode(body);callback(value);return encoded(value)
        for name, change in (
            ('native-exit.json',lambda b:changed(b,lambda d:d.update(process_exit_code=17))),
            ('cleanup.json',lambda b:changed(b,lambda d:d.update(cleanup_complete=False))),
            ('cleanup.json',lambda b:changed(b,lambda d:d.update(output_durable=False))),
            ('resources.json',lambda b:changed(b,lambda d:d.update(closed=False))),
            ('resources.json',lambda b:changed(b,lambda d:d.update(descendants_remaining_after_exit=True))),
            ('resources.json',lambda b:changed(b,lambda d:d['after']['files'].update({'memory.events':'oom 1\noom_kill 1\noom_group_kill 0\n'}))),
            ('resources.json',lambda b:changed(b,lambda d:d['after']['files'].update({'memory.peak':str(257*1024**2)}))),
            ('resources.json',lambda b:changed(b,lambda d:d['after']['files'].update({'memory.swap.peak':'1'}))),
            ('resources.json',lambda b:changed(b,lambda d:d['after']['files'].update({'cgroup.procs':'999'}))),
            ('stage-receipt.json',lambda b:changed(b,lambda d:d.update(exact_six_inputs=False))),
            ('native-config.json',lambda b:b+b' '),
            (BINARY_NAME,lambda b:b+b' '),
            ('screen/report.json',lambda b:changed(b,lambda d:d.update(config_sha256='0'*64))),
            ('screen/report.json',lambda b:changed(b,lambda d:d.update(queries=127))),
            ('screen/report.permutations.json',lambda b:changed(b,lambda d:d['old_to_new'][0].__setitem__(0,1))),
            ('screen/report.prefix.jsonl',lambda b:b+b'x')):
            tamper(name,change);failures+=1
        for name in ('screen/report.json','screen/report.permutations.json','screen/report.prefix.jsonl','resources.json','cleanup.json','native-exit.json'):
            path=root/name;hidden=path.with_name(path.name+'.hidden');path.rename(hidden)
            try:failure(lambda:validate_result(root,config),'missing '+name);failures+=1
            finally:hidden.rename(path)
        # Joint seal tamper, even if its report descriptor is rehashed.
        original_perms=read(root/'screen/report.permutations.json');original_report=read(root/'screen/report.json')
        perm=decode(original_perms);perm['old_to_new'][0][0]=1
        body=encoded(perm);(root/'screen/report.permutations.json').write_bytes(body)
        report=decode(original_report);report['details']['permutation_seal'].update(pin(body))
        (root/'screen/report.json').write_bytes(encoded(report))
        failure(lambda:validate_result(root,config),'rehashed non-bijection');failures+=1
        (root/'screen/report.permutations.json').write_bytes(original_perms);(root/'screen/report.json').write_bytes(original_report)
        prefix=PREFIX+'a0001'
        result=validate_result(root,config)
        terminal=dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,instance_id='i-original',
            prefix=prefix,config_sha256=sha(raw),status='complete',phase='complete',exit_code=0,original_exit_code=0,
            disposition=result['status'],artifact_roster_sha256=ROSTER_SHA,qualification=proof,result=result)
        publish(s3,root,prefix,terminal)
        require(s3.put_object.call_args.kwargs['Key']==prefix+'/terminal.json','marker published last')
        require(set(terminal['artifacts'])==set(ARTIFACTS),'complete raw artifact roster')
        def controller_receipts(out):
            out.mkdir()
            write(out/'aws-reservation.json',encoded(dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,
                config_sha256=sha(raw),qualification=proof)))
            nodes={'0':{'instance_id':'i-original'}}
            write(out/'aws-launch.json',encoded(dict(instance_id='i-original',nodes=nodes,prefix=prefix,
                source_commit='a'*40,source_archive_sha256='b'*64)))
            write(out/'aws-closeout.json',encoded(dict(state='terminated',nodes=nodes)))
        out=base/'collected';controller_receipts(out)
        collect(s3,prefix,out,'i-original','a'*40,'b'*64)
        require(replay(out)==result,'full hash/raw/gzip original collection replay')
        failure(lambda:collect(s3,prefix,out,'i-original','a'*40,'b'*64),'collection overwrite');failures+=1
        path=out/'screen/report.json';original=read(path);path.write_bytes(original+b' ')
        failure(lambda:replay(out),'collected tamper');failures+=1;path.write_bytes(original)
        key=prefix+'/artifacts/native.log';store[key]=b'tampered'
        damaged=base/'damaged';controller_receipts(damaged)
        failure(lambda:collect(s3,prefix,damaged,'i-original','a'*40,'b'*64),'remote fullhash');failures+=1
        store[key]=b''
        # SDK/upload failure leaves no misleading terminal; raw failures remain.
        failed_prefix=PREFIX+'a0002'
        failed_terminal=dict(terminal,status='failed',phase='execution',exit_code=96,original_exit_code=17,disposition='INVALID',prefix=failed_prefix)
        publish(s3,nonzero,failed_prefix,failed_terminal)
        require(failed_terminal['original_exit_code']==17 and failed_terminal['artifacts']['screen/report.json']['bytes']>0,'failure body preserved')
        reads_before=s3.get_object.call_count
        with patch.object(s3,'get_object',return_value=dict(Body=Body(b'wrong'),ContentLength=5)):
            failure(lambda:publish(s3,rejected,PREFIX+'a0003',copy.deepcopy(terminal)),'upload readback');failures+=1
        require(PREFIX+'a0003/terminal.json' not in store and s3.get_object.call_count>=reads_before,'no marker after failed upload')
        # Same shared ACK ownership, fsync, termination and waiter path used in production.
        import boto3
        campaign=Mock(ROOT=base,NAME='lifecycle',SCHEMA=SCHEMA,WALL=WALL,ARTIFACTS=ARTIFACTS,
            PREFIX=PREFIX,TOKEN_PREFIX=TOKEN_PREFIX,TAG=TAG,SUBNET=SUBNET,INSTANCE_TYPE=INSTANCE_TYPE,
            IMAGE_ID=IMAGE_ID,ROOT_DEVICE_NAME=ROOT_DEVICE_NAME,SPOT_MAX_USD_PER_HOUR=SPOT_MAX_USD_PER_HOUR,COMPUTE_CAP=COMPUTE_CAP)
        campaign.preflight.return_value=dict(config_sha256='c'*64)
        campaign.user_data.return_value='#!/bin/bash\nexit 0\n'
        for mode in ('success','poll','launch-fsync','launch-upload','collect','extra-ack'):
            campaign.ROOT=base/('lifecycle-'+mode);campaign.poll.reset_mock();campaign.collect.reset_mock()
            ec2=Mock();aws_s3=Mock();session=Mock()
            session.client.side_effect=lambda service:ec2 if service=='ec2' else aws_s3
            ec2.describe_instances.return_value={'Reservations':[]}
            ec2.describe_subnets.return_value={'Subnets':[{'AvailabilityZone':'mock'}]}
            ec2.describe_spot_price_history.return_value={'SpotPriceHistory':[{'SpotPrice':'.01','Timestamp':datetime.now(timezone.utc)}]}
            ack=['i-original','i-extra'] if mode=='extra-ack' else ['i-original']
            ec2.run_instances.return_value={'Instances':[{'InstanceId':i} for i in ack]}
            campaign.poll.side_effect=OSError('mock observation') if mode=='poll' else None
            campaign.collect.side_effect=OSError('mock collection') if mode=='collect' else None
            campaign.collect.return_value=dict(status='complete',phase='complete',exit_code=0,artifacts={n:pin(b'') for n in ARTIFACTS})
            chronology=[]
            ec2.terminate_instances.side_effect=lambda **kw:chronology.append(('terminate',kw['InstanceIds']))
            ec2.get_waiter.return_value.wait.side_effect=lambda **kw:chronology.append(('wait',kw['InstanceIds']))
            real_fsync=os.fsync
            def fsync(fd):
                if mode=='launch-fsync':raise OSError('mock launch persistence')
                return real_fsync(fd)
            def upload(key,body):
                if mode=='launch-upload' and key.endswith('/launch.json'):raise OSError('mock launch upload')
            with patch.object(boto3,'Session',return_value=session), \
                 patch.object(shared.subprocess,'check_output',side_effect=['','a'*40]), \
                 patch.object(shared,'source_archive',return_value=b'tiny synthetic archive'), \
                 patch.object(shared.peer,'missing',return_value=True), \
                 patch.object(shared.peer,'put_if_absent',side_effect=upload), \
                 patch.object(shared.os,'fsync',side_effect=fsync):
                if mode in ('success','extra-ack'):shared.main('a0001',campaign=campaign)
                else:failure(lambda:shared.main('a0001',campaign=campaign),'lifecycle '+mode);failures+=1
            require(ec2.run_instances.call_count==1,'one original launch, no retry')
            require(chronology==[('terminate',ack),('wait',ack)],'every ACK same-ID terminate/wait')
            kwargs=ec2.run_instances.call_args.kwargs
            require(kwargs['InstanceType']==INSTANCE_TYPE and kwargs['InstanceMarketOptions']['MarketType']=='spot'
                    and kwargs['InstanceMarketOptions']['SpotOptions']['MaxPrice']=='0.60','bounded c7i Spot launch')
            if mode!='launch-fsync':
                require(decode(read(campaign.ROOT/'lifecycle/a0001/aws-launch.json'))['nodes']=={str(i):{'instance_id':v} for i,v in enumerate(ack)},'durable every-ACK receipt')
        print(f'PASS fine-pack: real fake-native exit0/REJECT/exit17/deadline; {failures} refusals; fullhash/readback/marker-last/no-overwrite; every-ACK same-ID terminate/wait; actual SDK model positive/official_old_negative={official_old_checked}; cgroup/SDK transport/AWS MOCKED; no ANN/graph/corpus/network')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('attempt', nargs='?')
    parser.add_argument('--self-check', action='store_true')
    parser.add_argument('--replay', type=Path)
    parser.add_argument('--remote', nargs=6)
    args = parser.parse_args()
    require(sum((args.attempt is not None, args.self_check, args.replay is not None, args.remote is not None)) == 1, 'one CLI mode')
    if args.self_check:
        self_check(); return 0
    if args.replay:
        print(json.dumps(replay(args.replay), sort_keys=True)); return 0
    if args.remote:
        repo, root, commit, digest, prefix, config_sha = args.remote
        return remote(Path(repo), Path(root), commit, digest, prefix, config_sha)
    require(re.fullmatch(r'a[0-9]{4}', args.attempt), 'attempt must be aNNNN')
    sdk_guard()
    os.environ['AWS_MAX_ATTEMPTS'] = '1'
    os.environ['AWS_RETRY_MODE'] = 'standard'
    with open('/tmp/borsuk-fine-pack-diagnostic.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        lifecycle().main(args.attempt, campaign=sys.modules[__name__])
    return 0


if __name__ == '__main__':
    sys.exit(main())
