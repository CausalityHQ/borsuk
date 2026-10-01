"""Launch one frozen FIRST100k semantic/graph cold HTTP Spot experiment.

Authority pointers use {path, bytes, sha256, key}; archived receipt/log pointers
may also use archived_path/archived_sha256 for gzip transport. Root supplies the
config, standalone native_proof, completed native_assurance and exact 399-file
native_source_manifest. This controller never qualifies or rebuilds native code.
"""

import fcntl
import gzip
import io
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
from contextlib import contextmanager
from shlex import quote
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('qualification requires Python assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import launch_native_metadata_ranges_cold_spot as shared
from scripts import launch_native_peer_1m_spot as peer
from scripts import launch_native_startup_profile_spot as startup
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts.check_native_startup_build import source_hashes, source_identity

ROOT = Path('docs/research/performance-architecture-20260930/semantic-cold')
CONFIG = ROOT / 'config.json'
NAME = ''
SCHEMA = 'borsuk-native-semantic-router-cold-spot-v1'
PREFIX = 'research/semantic-router/20261001/cold-'
TOKEN_PREFIX = 'semantic-router-cold-'
TAG = 'borsuk-semantic-router-cold'
WALL = 3600
INSTANCE_TYPE = 'm7i.2xlarge'
IMAGE_ID = 'ami-06121aa3085b6f918'
SUBNET = peer.SUBNET
SPOT_MAX_USD_PER_HOUR = COMPUTE_CAP = .50
BINARY_SHA = 'c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533'
BINARY_BYTES = 16191384
GATES = ('affected-final', 'release-final', 'clippy-final', 'test-build-final', 'full-workspace-final')
EXTRAS = ('scripts/launch_native_semantic_router_cold_spot.py',
          'scripts/launch_native_metadata_ranges_cold_spot.py',
          'scripts/launch_native_startup_profile_spot.py', 'scripts/launch_native_peer_1m_spot.py',
          'scripts/launch_v174_relaid_bind_compile_spot.py', 'scripts/launch_v157_primary_feasibility_spot.py',
          'scripts/check_native_startup_build.py')
AUTHORITY_FILES = ('native-assurance.json', 'boundary-check.json', 'native-source-manifest.json',
                   'native-source.tar.gz', 'qualified-source.tar.gz',
                   *(f'assurance/{gate}.{suffix}' for gate in GATES for suffix in ('json', 'log')))
ARTIFACTS = ('source-qualification.json', 'binaries/two_bit_http', 'cpu.txt', 'run-closed.log',
             'profile.log', 'profile-resources.txt', 'profile-cgroup.json',
             'screen/records.jsonl', 'screen/summary.json', 'screen/config.json', 'screen/qualification.json',
             *AUTHORITY_FILES)


def _worker():
    # The runtime is integrated independently; a missing module is a launch blocker.
    return importlib.import_module('scripts.run_native_semantic_router_cold')


@contextmanager
def _cwd(base):
    previous = Path.cwd()
    os.chdir(base)
    try:
        yield
    finally:
        os.chdir(previous)


def _identity(body):
    return dict(bytes=len(body), sha256=peer.sha(body))


def _read(base, pointer):
    assert type(pointer['bytes']) is int and pointer['bytes'] > 0
    assert len(pointer['sha256']) == 64 and all(c in '0123456789abcdef' for c in pointer['sha256'])
    if 'key' in pointer:
        assert isinstance(pointer['key'], str) and pointer['key'] and '\n' not in pointer['key']
    if 'archived_path' in pointer:
        body = (base / pointer['archived_path']).read_bytes()
        if 'archived_sha256' in pointer:
            assert peer.sha(body) == pointer['archived_sha256']
        # Receipt gzip is transport; a qualified .tar.gz is itself the artifact.
        if pointer['archived_path'].endswith('.gz') and not pointer['path'].endswith('.gz'):
            body = gzip.decompress(body)
    else:
        body = (base / pointer['path']).read_bytes()
    assert _identity(body) == {key: pointer[key] for key in ('bytes', 'sha256')}
    return body


def _archive_sources(body):
    with tarfile.open(fileobj=io.BytesIO(body), mode='r:gz') as archive:
        members = [m for m in archive if m.isfile() and
                   (m.name.endswith('.rs') or Path(m.name).name in ('Cargo.toml', 'Cargo.lock'))]
        assert len(members) == len({m.name for m in members}), 'duplicate source archive entries'
        assert all(not Path(m.name).is_absolute() and '..' not in Path(m.name).parts for m in members)
        return {m.name: peer.sha(archive.extractfile(m).read()) for m in members}


def _qualify(base, binary_override=None):
    base = Path(base).resolve()
    body = (base / CONFIG).read_bytes()
    config = json.loads(body)
    expected = dict(schema='borsuk-native-semantic-router-cold-v1', architecture='x86_64',
        region=peer.REGION, bucket=peer.BUCKET, count=64, k=10, ann_queries=512,
        dataset_order=['ReLAION', 'CoHere'], blocks=['control0', 'candidate1', 'candidate2', 'control3'],
        client_cpu_affinity=[4, 5], native_cpu_affinity=[0, 1, 2, 3],
        worker_limit_seconds=3000, machine_limit_seconds=WALL, instance_type=INSTANCE_TYPE,
        image_id=IMAGE_ID, subnet_id=SUBNET, spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR,
        compute_cap_usd=COMPUTE_CAP, ebs_s3_allowance_usd=.15, profile_memory_bytes=8*1024**3,
        native_rlimit_as_bytes=4*1024**3, profile_swap_bytes=0, native_memory_bytes=536870912,
        credential_protocol='instance-imdsv2')
    assert all(config[key] == value for key, value in expected.items()), 'fixed protocol/infrastructure'
    worker = _worker()
    with _cwd(base):
        worker.validate_config(config)
    assert set(config['code_sha256']) == set(worker.CODE)
    assert set(config['controller_code_sha256']) == set(EXTRAS)
    code = dict(config['code_sha256'], **config['controller_code_sha256'])
    for name, digest in code.items():
        assert peer.sha((base/name).read_bytes()) == digest, name
    binary_pointer = config['native_binary']
    assert {key: binary_pointer[key] for key in ('bytes', 'sha256')} == config['binary'] == dict(bytes=BINARY_BYTES, sha256=BINARY_SHA)
    binary = Path(binary_override).read_bytes() if binary_override is not None else _read(base, binary_pointer)
    assert _identity(binary) == config['binary']
    files = {name: _read(base, config[key]) for key, name in (
        ('native_assurance', 'native-assurance.json'), ('native_proof', 'boundary-check.json'),
        ('native_source_manifest', 'native-source-manifest.json'))}
    manifest = json.loads(files['native-source-manifest.json'])
    identities = source_hashes(base)
    identity = source_identity(identities)
    assert manifest['schema'] == 'borsuk-native-semantic-router-source-manifest-v1'
    assert identities == manifest['source_sha256']
    assert len(identities) == manifest['source_file_count'] == config['native_source_file_count'] == 399
    assert identity == manifest['source_identity_sha256'] == config['native_source_identity_sha256']
    files['native-source.tar.gz'] = _read(base, manifest['native_source_archive'])
    assert _archive_sources(files['native-source.tar.gz']) == identities, 'complete qualified source archive'
    proof = json.loads(files['boundary-check.json'])
    assert config['qualification_sha256'] == peer.sha(files['boundary-check.json'])
    assert proof['qualified'] is True and type(proof['green_status']) is type(proof['release_status']) is int
    assert proof['green_status'] == proof['release_status'] == 0
    assert proof['binary_sha256'] == BINARY_SHA
    assert proof['source_file_count'] == 399 and proof['source_identity_sha256'] == identity
    assert proof['compiled_native_sha256'] and all(identities.get(name) == digest
        for name, digest in proof['compiled_native_sha256'].items())
    assurance = json.loads(files['native-assurance.json'])
    assert assurance['full_workspace_test_execution'] is True, 'completed full workspace execution required'
    assert {k: assurance['binaries']['two_bit_http'][k] for k in ('bytes', 'sha256')} == config['binary']
    files['qualified-source.tar.gz'] = _read(base, assurance['source_archive'])
    qualified = _archive_sources(files['qualified-source.tar.gz'])
    assert qualified and all(identities.get(name) == digest for name, digest in qualified.items())
    for name in GATES:
        gate = assurance['gates'][name]
        assert type(gate['exit_status']) is int and gate['exit_status'] == 0, name
        receipt_body, log = _read(base, gate['receipt']), _read(base, gate['log'])
        receipt = json.loads(receipt_body)
        assert type(receipt['exit_status']) is int and receipt['exit_status'] == 0
        assert receipt['source_unchanged'] is True and receipt['command'] == gate['command']
        if name == 'full-workspace-final':
            assert receipt['artifacts']['test.log'] == _identity(log)
        else:
            assert receipt['log_sha256'] == peer.sha(log)
        for path, digest in receipt['source_sha256'].items():
            matches = [d for n, d in identities.items() if n == path or Path(n).name == path]
            assert matches == [digest], 'receipt source identity: ' + path
        if name == 'full-workspace-final':
            assert receipt['source_sha256'] == identities
            assert receipt['source_identity_sha256'] == identity and receipt['source_file_count'] == 399
            assert 'test' in receipt['command'] and '--workspace' in receipt['command']
            assert '--no-run' not in receipt['command'], 'compilation is not full-suite execution'
        files[f'assurance/{name}.json'], files[f'assurance/{name}.log'] = receipt_body, log
    assert set(files) == set(AUTHORITY_FILES)
    qualification = dict(config_path=str(CONFIG), config_sha256=peer.sha(body), campaign_schema=SCHEMA,
        native_source_commit=manifest['native_source_commit'], source_identity_sha256=identity,
        source_file_count=399, code_sha256=code, binary_sha256=BINARY_SHA, binary_bytes=BINARY_BYTES,
        qualification_sha256=config['qualification_sha256'], native_rebuilt=False,
        current_full_suite_pass_claim=True, full_workspace_test_execution=True,
        native_source_archive_sha256=manifest['native_source_archive']['sha256'],
        native_source_manifest_sha256=peer.sha(files['native-source-manifest.json']),
        native_assurance_sha256=peer.sha(files['native-assurance.json']),
        native_binary=binary_pointer, authority_artifacts={n: _identity(b) for n, b in files.items()},
        artifact_roster_sha256=peer.sha(json.dumps(ARTIFACTS, separators=(',', ':')).encode()))
    return qualification, files


def preflight(base=Path('.')):
    return _qualify(base)[0]


def _stage(repo, out):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    expected = json.loads((out/'source-qualification.json').read_bytes())
    actual, files = _qualify(repo, out/'binaries/two_bit_http')
    assert actual == expected, 'remote qualification differs from local authority'
    for name, body in files.items():
        path = out/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    (out/'binaries/two_bit_http').chmod(0o755)


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert len(commit) == 40 and len(archive_sha) == 64
    with patch.multiple(runner, WALL_SECONDS=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS):
        body = runner.user_data(commit, archive_sha, archive_key, prefix)
    body = body.replace('v174-relaid-bind-compile', 'native-semantic-router-cold')
    # The bootstrap creates the trap and authenticates the campaign archive.
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    proof = json.dumps(qualification, sort_keys=True, separators=(',', ':')).encode()
    import base64
    encoded = base64.b64encode(gzip.compress(proof, mtime=0)).decode()
    binary_key = quote('s3://' + peer.BUCKET + '/' + qualification['native_binary']['key'])
    command = f'''phase=install
dnf install -y -q tar gzip time util-linux python3.12
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("{encoded}")))'
phase=binary-qualification
lscpu >cpu.txt
test "$(uname -m)" = x86_64
mkdir binaries
aws s3 cp {binary_key} binaries/two_bit_http --only-show-errors
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_semantic_router_cold_spot --stage "$root/repo" "$root"
phase=profile
systemd-run --unit=native-semantic-router-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3030 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 3000 \\
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_semantic_router_cold {CONFIG} {qualification['config_sha256']} "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
'''
    command += '\n'.join('test -s "$root/' + name + '"' for name in ARTIFACTS) + '\n'
    body = body[:start] + command + body[end:]
    # Terminal identities are emitted by the bootstrap alongside its byte roster.
    marker = "'source_archive_sha256':'" + archive_sha + "',"
    terminal_fields = {key: qualification[key] for key in ('config_sha256', 'qualification_sha256',
        'binary_sha256', 'binary_bytes', 'native_source_commit', 'source_identity_sha256',
        'source_file_count', 'artifact_roster_sha256', 'native_source_archive_sha256',
        'native_source_manifest_sha256', 'native_assurance_sha256')}
    assert body.count(marker) == 1, 'bootstrap terminal identity hook changed'
    body = body.replace(marker, marker + repr(terminal_fields)[1:-1] + ',')
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    assert len(body.encode()) < 16384
    return body


def poll(ec2, s3, prefix, instance_id, started):
    with patch.object(startup, 'WALL', WALL):
        return startup.poll(ec2, s3, prefix, instance_id, started)


def collect(s3, prefix, out, instance_id, commit, digest):
    out = Path(out)
    reservation = json.loads((out/'aws-reservation.json').read_bytes())
    closed = json.loads((out/'aws-closeout.json').read_bytes())
    assert closed['state'] == 'terminated' and instance_id in {n['instance_id'] for n in closed['nodes'].values()}
    qualification = reservation['qualification']
    raw = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/terminal.json')['Body'].read()
    (out/'aws-terminal.json').write_bytes(raw)
    terminal = json.loads(raw)
    assert terminal['schema'] == SCHEMA and terminal['instance_id'] == instance_id
    assert terminal['source_commit'] == reservation['source_commit'] == commit
    assert terminal['source_archive_sha256'] == reservation['source_archive_sha256'] == digest
    for key in ('config_sha256', 'qualification_sha256', 'binary_sha256', 'binary_bytes',
                'native_source_commit', 'source_identity_sha256', 'source_file_count', 'artifact_roster_sha256',
                'native_source_archive_sha256', 'native_source_manifest_sha256', 'native_assurance_sha256'):
        assert terminal[key] == qualification[key], key
    assert set(terminal['artifacts']) <= set(ARTIFACTS)
    files = {}
    for name, identity in terminal['artifacts'].items():
        data = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/artifacts/'+name)['Body'].read()
        assert _identity(data) == identity, name
        path = out/(name+'.gz')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(gzip.compress(data, mtime=0))
        files[name] = data
    if terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == 0:
        assert set(files) == set(ARTIFACTS), 'complete artifact roster'
        assert json.loads(files['source-qualification.json']) == qualification
        for name, identity in qualification['authority_artifacts'].items():
            assert _identity(files[name]) == identity, name
        assert _identity(files['binaries/two_bit_http']) == dict(bytes=qualification['binary_bytes'], sha256=qualification['binary_sha256'])
        assert peer.sha(files['screen/config.json']) == qualification['config_sha256']
        assert peer.sha(files['boundary-check.json']) == peer.sha(files['screen/qualification.json']) == qualification['qualification_sha256']
    return terminal


def main(attempt):
    return shared.main(attempt, campaign=sys.modules[__name__])


def lifecycle_self_check():
    """Exercise this campaign through the actual shared lifecycle, including wait."""
    from datetime import datetime, timezone
    import tempfile
    from unittest.mock import Mock
    from botocore.exceptions import ReadTimeoutError, EndpointConnectionError
    module = sys.modules[__name__]
    tokens = []
    for failure in ('success', 'fsync', 'upload', 'poll', 'interruption', 'interrupt', 'multi-ack', 'multi-ack-fsync'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {'Reservations': []}
            ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'synthetic-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory': [
                {'SpotPrice': '.1', 'Timestamp': datetime.now(timezone.utc)}]}
            ids = ['i-owned', 'i-extra'] if failure.startswith('multi-ack') else ['i-owned']
            ec2.run_instances.return_value = {'Instances': [{'InstanceId': n} for n in ids]}
            events = []
            ec2.terminate_instances.side_effect = lambda **kw: events.append('terminate')
            ec2.get_waiter.return_value.wait.side_effect = lambda **kw: events.append('wait')
            def collected(*args):
                assert events == ['terminate', 'wait']
                events.append('collect')
                return dict(status='complete', phase='complete', exit_code=0, artifacts={n: {} for n in ARTIFACTS})
            error = {'poll': ReadTimeoutError(endpoint_url='synthetic'),
                     'interruption': RuntimeError('interrupted'), 'interrupt': KeyboardInterrupt()}.get(failure)
            writes = [None, None, OSError('upload')] if failure == 'upload' else [None]*3
            attempt = 'a' + str(len(tokens)+1).zfill(4)
            with patch.object(module, 'ROOT', Path(tmp)), patch.object(module, 'preflight', return_value={'config_sha256': 'a'*64}), \
                    patch.object(module, 'user_data', return_value='synthetic'), patch.object(module, 'poll', side_effect=error), \
                    patch.object(module, 'collect', side_effect=collected), patch.object(shared.boto3, 'Session', return_value=session), \
                    patch.object(shared.subprocess, 'check_output', side_effect=['', '0'*40, b'archive']), \
                    patch.object(peer, 'missing', return_value=True), patch.object(peer, 'put_if_absent', side_effect=writes), \
                    patch.object(shared.os, 'fsync', side_effect=OSError('persist') if failure.endswith('fsync') else None) as fsync:
                try:
                    main(attempt)
                except (OSError, RuntimeError, ReadTimeoutError, KeyboardInterrupt):
                    assert failure not in ('success', 'multi-ack'), 'unexpected lifecycle failure'
                else:
                    assert failure in ('success', 'multi-ack'), 'failure swallowed'
                fsync.assert_called_once()
                shared.boto3.Session.assert_called_once_with(profile_name='causality', region_name=peer.REGION)
            assert events == ['terminate', 'wait', 'collect']
            ec2.run_instances.assert_called_once()
            ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            args = ec2.run_instances.call_args.kwargs
            assert args['InstanceType'] == INSTANCE_TYPE and args['ImageId'] == IMAGE_ID
            assert args['InstanceMarketOptions'] == {'MarketType': 'spot', 'SpotOptions': {
                'InstanceInterruptionBehavior': 'terminate', 'SpotInstanceType': 'one-time', 'MaxPrice': '0.50'}}
            assert args['BlockDeviceMappings'][0]['Ebs'] == dict(DeleteOnTermination=True, Encrypted=True, VolumeSize=80, VolumeType='gp3')
            assert args['NetworkInterfaces'][0]['AssociatePublicIpAddress'] is True
            assert args['IamInstanceProfile'] == {'Arn': peer.PROFILE_ARN}
            assert args['ClientToken'].startswith(TOKEN_PREFIX) and len(args['ClientToken']) <= 64
            tokens.append(args['ClientToken'])
            out = Path(tmp)/attempt
            for name in ('aws-launch.json', 'aws-closeout.json'):
                assert json.loads((out/name).read_bytes())['nodes'] == {str(i): dict(instance_id=n) for i, n in enumerate(ids)}
            reservation = json.loads((out/'aws-reservation.json').read_bytes())
            assert reservation['instance_type'] == INSTANCE_TYPE and reservation['image_id'] == IMAGE_ID
            assert reservation['compute_cap_usd'] == .50 and reservation['wall_seconds'] == WALL
            assert reservation['ebs_s3_allowance_usd'] == .15 and reservation['total_cost_measured'] is False
    assert len(tokens) == len(set(tokens))
    ec2, s3 = Mock(), Mock()
    ec2.describe_instances.side_effect = [ReadTimeoutError(endpoint_url='synthetic'),
        {'Reservations': [{'Instances': [{'State': {'Name': 'running'}}]}]}]
    with patch.object(peer, 'missing', side_effect=[EndpointConnectionError(endpoint_url='synthetic'), True, True, False]), \
            patch.object(startup.time, 'sleep'):
        poll(ec2, s3, 'synthetic', 'i-owned', startup.time.monotonic())
    assert ec2.describe_instances.call_count == 2
    assert all(c.kwargs == {'InstanceIds': ['i-owned']} for c in ec2.describe_instances.call_args_list)
    ec2.run_instances.assert_not_called()
    ec2.describe_instances.side_effect = None
    ec2.describe_instances.return_value = {'Reservations': [{'Instances': [{'State': {'Name': 'terminated'}}]}]}
    with patch.object(peer, 'missing', return_value=True), patch.object(startup.time, 'sleep'):
        try:
            poll(ec2, s3, 'synthetic', 'i-owned', startup.time.monotonic())
        except RuntimeError:
            pass
        else:
            raise AssertionError('interruption accepted')
    with patch.object(startup.time, 'monotonic', return_value=WALL+301):
        try:
            poll(ec2, s3, 'synthetic', 'i-owned', 0)
        except TimeoutError:
            pass
        else:
            raise AssertionError('wall cap ignored')


def collection_self_check():
    import tempfile
    from unittest.mock import Mock
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        files = {n: b'synthetic artifact\n' for n in ARTIFACTS}
        binary = files['binaries/two_bit_http']
        config = files['screen/config.json']
        proof = files['boundary-check.json']
        files['screen/qualification.json'] = proof
        qualification = dict(config_sha256=peer.sha(config), qualification_sha256=peer.sha(proof),
            binary_sha256=peer.sha(binary), binary_bytes=len(binary), native_source_commit='2'*40,
            source_identity_sha256='3'*64, source_file_count=399, artifact_roster_sha256='4'*64,
            native_source_archive_sha256='5'*64, native_source_manifest_sha256='6'*64, native_assurance_sha256='7'*64,
            authority_artifacts={n: _identity(files[n]) for n in AUTHORITY_FILES})
        files['source-qualification.json'] = json.dumps(qualification).encode()
        terminal = dict(schema=SCHEMA, instance_id='i-owned', source_commit='0'*40,
            source_archive_sha256='1'*64, status='complete', phase='complete', exit_code=0,
            artifacts={n: _identity(b) for n, b in files.items()}, **{k:v for k,v in qualification.items() if k != 'authority_artifacts'})
        (out/'aws-reservation.json').write_text(json.dumps(dict(source_commit='0'*40,
            source_archive_sha256='1'*64, qualification=qualification)))
        (out/'aws-closeout.json').write_text(json.dumps(dict(state='terminated', nodes={'0': dict(instance_id='i-owned')})))
        def fetched(**kwargs):
            key = kwargs['Key']
            return {'Body': io.BytesIO(json.dumps(current).encode() if key.endswith('/terminal.json')
                else files[key.split('/artifacts/')[1]])}
        s3 = Mock()
        s3.get_object.side_effect = fetched
        for change in (None, 'source', 'config', 'proof', 'binary', 'roster', 'sha', 'bytes', 'unknown'):
            current = json.loads(json.dumps(terminal))
            if change in ('source', 'config', 'proof', 'binary'):
                key = {'source': 'source_identity_sha256', 'config': 'config_sha256',
                       'proof': 'qualification_sha256', 'binary': 'binary_sha256'}[change]
                current[key] = 'f'*64
            elif change == 'roster':
                del current['artifacts']['screen/records.jsonl']
            elif change in ('sha', 'bytes'):
                current['artifacts']['screen/summary.json']['sha256' if change == 'sha' else 'bytes'] = 'f'*64 if change == 'sha' else 1
            elif change == 'unknown':
                current['artifacts']['../escape'] = _identity(b'bad')
            try:
                result = collect(s3, 'synthetic', out, 'i-owned', '0'*40, '1'*64)
            except AssertionError:
                assert change is not None
            else:
                assert change is None and result == terminal, 'changed terminal accepted'


def self_check():
    """Only synthetic bodies and mocked AWS; no native or cloud execution."""
    import tempfile
    from types import SimpleNamespace
    module = sys.modules[__name__]
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        # A missing authority must fail before any cloud client is constructed.
        with patch.object(shared.boto3, 'Session') as aws:
            try:
                preflight(base)
            except FileNotFoundError:
                pass
            else:
                raise AssertionError('missing authority accepted')
            aws.assert_not_called()
        binary = b'synthetic binary; never executable'
        binary_sha = peer.sha(binary)
        def put(name, body):
            if not isinstance(body, bytes):
                body = json.dumps(body, sort_keys=True, separators=(',', ':')).encode()
            path = base / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            return dict(path=name, bytes=len(body), sha256=peer.sha(body), key='synthetic/' + name)
        for n in range(399):
            put(f'crates/test{n}.rs', f'// synthetic {n}\n'.encode())
        identities = source_hashes(base)
        source_sha = source_identity(identities)
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode='w:gz') as tar:
            for name in identities:
                tar.add(base / name, arcname=name)
        manifest = dict(schema='borsuk-native-semantic-router-source-manifest-v1',
            source_file_count=399, source_identity_sha256=source_sha,
            native_source_commit='0'*40, source_sha256=identities,
            native_source_archive=put('native-source.tar.gz', archive.getvalue()))
        gates = {}
        for name in GATES:
            log = put('receipts/' + name + '.log', b'test result: ok. 1 passed; 0 failed;\n')
            receipt = dict(exit_status=0, source_unchanged=True, command=['cargo', 'test', '--workspace'],
                source_sha256=identities, source_identity_sha256=source_sha, source_file_count=399,
                log_sha256=log['sha256'])
            if name == 'full-workspace-final':
                del receipt['log_sha256']
                receipt['artifacts'] = {'test.log': {k:log[k] for k in ('bytes', 'sha256')}}
            gates[name] = dict(exit_status=0, command=receipt['command'],
                receipt=put('receipts/' + name + '.json', receipt), log=log)
        assurance = dict(full_workspace_test_execution=True, gates=gates,
            source_archive=dict(manifest['native_source_archive'], path='/missing/qualified-source.tar.gz',
                                archived_path=manifest['native_source_archive']['path']),
            binaries={'two_bit_http': dict(bytes=len(binary), sha256=binary_sha)})
        proof = dict(qualified=True, green_status=0, release_status=0,
            binary_sha256=binary_sha, source_file_count=399, source_identity_sha256=source_sha,
            compiled_native_sha256=dict(list(identities.items())[:2]))
        runtime = SimpleNamespace(CODE=('scripts/synthetic-runtime.py',), validate_config=lambda _: None)
        for name in (*runtime.CODE, *EXTRAS):
            put(name, b'# synthetic controller/runtime closure\n')
        config = dict(schema='borsuk-native-semantic-router-cold-v1', architecture='x86_64',
            region=peer.REGION, bucket=peer.BUCKET, count=64, k=10, ann_queries=512,
            dataset_order=['ReLAION', 'CoHere'], blocks=['control0', 'candidate1', 'candidate2', 'control3'],
            client_cpu_affinity=[4, 5], native_cpu_affinity=[0, 1, 2, 3],
            worker_limit_seconds=3000, machine_limit_seconds=3600,
            instance_type=INSTANCE_TYPE, image_id=IMAGE_ID, subnet_id=SUBNET,
            spot_max_usd_per_hour=.50, compute_cap_usd=.50, ebs_s3_allowance_usd=.15,
            profile_memory_bytes=8*1024**3, native_rlimit_as_bytes=4*1024**3,
            profile_swap_bytes=0, native_memory_bytes=536870912, credential_protocol='instance-imdsv2',
            native_source_file_count=399, native_source_identity_sha256=source_sha,
            native_binary=put('binary', binary), native_assurance=put('assurance.json', assurance),
            native_source_manifest=put('manifest.json', manifest), native_proof=put('proof.json', proof),
            code_sha256={n: peer.sha((base/n).read_bytes()) for n in runtime.CODE},
            controller_code_sha256={n: peer.sha((base/n).read_bytes()) for n in EXTRAS})
        config['binary'] = dict(bytes=len(binary), sha256=binary_sha)
        config['qualification_sha256'] = config['native_proof']['sha256']
        def freeze(value):
            put(str(CONFIG), value)
        freeze(config)
        with patch.object(module, '_worker', return_value=runtime), \
                patch.multiple(module, BINARY_SHA=binary_sha, BINARY_BYTES=len(binary)):
            qualification = preflight(base)
            assert qualification['source_file_count'] == 399 and qualification['native_rebuilt'] is False
            assert qualification['current_full_suite_pass_claim'] is True
            stage = base/'stage'
            (stage/'binaries').mkdir(parents=True)
            (stage/'binaries/two_bit_http').write_bytes(binary)
            (stage/'source-qualification.json').write_text(json.dumps(qualification))
            _stage(base, stage)
            assert all(_identity((stage/n).read_bytes()) == ident
                       for n, ident in qualification['authority_artifacts'].items())
            raw = b'synthetic archived log\n'
            pointer = put('archived.log.gz', gzip.compress(raw, mtime=0))
            pointer.update(path='missing.log', archived_path=pointer['path'], archived_sha256=pointer['sha256'], **_identity(raw))
            assert _read(base, pointer) == raw
            try:
                _read(base, dict(pointer, archived_sha256='f'*64))
            except AssertionError:
                pass
            else:
                raise AssertionError('bad gzip transport accepted')
            for key, value in [('architecture', 'aarch64'), ('ann_queries', 256),
                               ('worker_limit_seconds', 3001), ('native_memory_bytes', 1073741824),
                               ('native_source_file_count', 398),
                               ('qualification_sha256', 'f'*64), ('controller_code_sha256', {})]:
                freeze(dict(config, **{key: value}))
                try:
                    preflight(base)
                except AssertionError:
                    pass
                else:
                    raise AssertionError('changed authority accepted: ' + key)
            freeze(config)
            original = (base/'assurance.json').read_bytes()
            for changed in (dict(assurance, full_workspace_test_execution=False),
                            dict(assurance, gates=dict(gates, **{'full-workspace-final': dict(gates['full-workspace-final'], exit_status=1)}))):
                broken = dict(config, native_assurance=put('assurance.json', changed))
                freeze(broken)
                try:
                    preflight(base)
                except AssertionError:
                    pass
                else:
                    raise AssertionError('pending/failed full suite accepted')
            put('assurance.json', original)
            freeze(config)
            path = base / next(iter(identities))
            original = path.read_bytes()
            path.write_bytes(original + b'// changed\n')
            try:
                preflight(base)
            except AssertionError:
                pass
            else:
                raise AssertionError('changed source accepted')
            path.write_bytes(original)
            body = user_data('0'*40, '1'*64, 'sources/synthetic', PREFIX+'a0001', qualification)
            assert len(body.encode()) < 16384
            for marker in ('--on-active=3600s', 'MemoryMax=8G', 'MemorySwapMax=0',
                           'RuntimeMaxSec=3030', 'ulimit -v 4194304', 'taskset -c 4-5',
                           'PYTHONPATH="$root/repo"', '-m scripts.run_native_semantic_router_cold',
                           '--kill-after=30 3000'):
                assert marker in body, marker
            assert 'rustup' not in body and 'cargo build' not in body
            terminal_script = body.split("python3 - <<'PY' >terminal.json\n")[1].split('\nPY\n')[0]
            terminal = json.loads(subprocess.check_output([sys.executable, '-c', terminal_script],
                cwd=stage, env=dict(os.environ, INSTANCE_ID='i-synthetic', EXIT_CODE='0', PHASE='complete')))
            assert terminal['schema'] == SCHEMA and terminal['binary_sha256'] == binary_sha
            assert terminal['source_identity_sha256'] == source_sha
            assert terminal['qualification_sha256'] == qualification['qualification_sha256']
    lifecycle_self_check()
    collection_self_check()
    print('semantic cold controller self-check PASS; cloud/native UNRUN')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    elif len(sys.argv) == 4 and sys.argv[1] == '--stage':
        _stage(sys.argv[2], sys.argv[3])
    else:
        assert len(sys.argv) == 2, 'usage: python3 -m scripts.launch_native_semantic_router_cold_spot aNNNN'
        with open('/tmp/borsuk-native-semantic-router-cold-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(sys.argv[1])
