"""One cold first-query Spot worker; close the owned ID before collecting bodies."""
import fcntl
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import boto3
from botocore.exceptions import EndpointConnectionError, ReadTimeoutError
from scripts import launch_native_startup_profile_spot as startup
from scripts import launch_native_peer_1m_spot as peer
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts.check_native_startup_build import FOCUSED_ARM, RUNTIME, source_hashes, source_identity

ROOT = peer.ROOT
CONFIG = ROOT / 'cold-first-query-config.json'
SCHEMA = 'borsuk-native-cold-first-query-spot-v1'
WALL = 1800
FROZEN = ROOT / 'arm-sha-startup/a0001'
NATIVE_COMMIT = 'd32d472293d7300be77eb5f1e87cf69892cc504b'
BINARY_SHA = '3565d27ffbe7a88e2a4c6d0c982ef795072d944e1dab8f11720a9bf9b31556af'
CODE = ('scripts/run_native_cold_first_query.py', 'scripts/check_native_startup_stats.py',
        'scripts/run_native_peer_1m_worker.py', 'scripts/run_native_peer_offered_http.py',
        'scripts/run_native_union_offered_http.py', 'scripts/run_native_union_http.py',
        'scripts/rest_coexistence_load.py', 'scripts/launch_native_cold_first_query_spot.py',
        'scripts/check_native_startup_build.py')
FROZEN_FILES = ('binaries/two_bit_http', 'boundary-check.json', 'compiled-source.json',
                'source-qualification.json', 'rustc-version.txt', 'cargo-version.txt',
                'cpuinfo.txt', 'arm-feature-tree.txt', 'x86-feature-tree.txt', 'run-closed.log',
                *('compiled-source/' + name for name in FOCUSED_ARM))
ARTIFACTS = ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt',
             'source-qualification.json', 'boundary-check.json', 'compiled-source.json',
             'binaries/two_bit_http', 'profile.log', 'profile-resources.txt',
             'profile-cgroup.json', 'screen/summary.json',
             'screen/relaion-records.jsonl', 'screen/cohere-records.jsonl')


def preflight(base=Path('.')):
    config_body = (base / CONFIG).read_bytes()
    config = json.loads(config_body)
    assert config['schema'] == 'borsuk-native-cold-first-query-v1'
    assert (config['region'], config['bucket'], config['count'], config['k']) == (peer.REGION, peer.BUCKET, 64, 10)
    assert config['dataset_order'] == ['ReLAION', 'CoHere']
    assert config['client_cpu_affinity'] == [4, 5] and config['native_cpu_affinity'] == [0, 1, 2, 3]
    assert config['worker_limit_seconds'] == 1200 and config['machine_limit_seconds'] == WALL
    assert config['namespace_connect_deadline_seconds'] == 45 and config['native_process_limit_seconds'] == 60
    assert config['query_payload_timeout_seconds'] == 5
    assert config['previous_observed_development_panel'] is True
    assert config['namespace_cold_start_included'] is True and config['application_sq8_cache'] is False
    assert config['s3_service_cache'] == 'uncontrolled' and config['transport'] == 'loopback plain HTTP'
    assert config['matched_vendor_measured'] is False and config['offered_or_saturation_qps_measured'] is False
    assert config['gates'] == dict(recall_at_10_minimum=.95, all_calls_success=True,
        source_scorer_ordered_id_physical_parity=True,
        cold_start_to_first_http_response_p90_ms_exclusive_maximum=444)
    assert set(config['code_sha256']) == set(CODE[:-2])
    for name, digest in config['code_sha256'].items():
        assert peer.sha((base / name).read_bytes()) == digest, name
    code = dict(config['code_sha256'], **{name: peer.sha((base / name).read_bytes()) for name in CODE[-2:]})
    frozen = config['frozen_native_qualification']
    assert frozen == dict(path=str(FROZEN / 'verification.json'),
        sha256='b98e17328df1d83b583f06b4d1f0f36a77a159893bb41ff25c8accc6724a7e58',
        source_commit=NATIVE_COMMIT,
        source_archive_sha256='a37b33402a82bf9653ed6542b7a523fd46d64307d449ff3d575d4c572e751ace',
        terminal_sha256='4e898e7a59a5a044112c805ae8c0ef8ca51d3183e8a6ad32330e3da580fcd6be')
    verified_body = (base / frozen['path']).read_bytes()
    assert peer.sha(verified_body) == frozen['sha256']
    verified = json.loads(verified_body)
    assert verified['valid_diagnostic'] and verified['diagnostic_gate_passed'] and verified['state'] == 'terminated'
    terminal_body = (base / FROZEN / 'aws-terminal.json').read_bytes()
    assert peer.sha(terminal_body) == frozen['terminal_sha256'] == verified['terminal_sha256']
    terminal = json.loads(terminal_body)
    assert terminal['schema'] == startup.SCHEMA_ARM and terminal['phase'] == terminal['status'] == 'complete'
    assert terminal['exit_code'] == 0 and terminal['instance_id'] == verified['instance_id']
    for key in ('source_commit', 'source_archive_sha256'):
        assert terminal[key] == verified[key] == frozen[key]
    close = json.loads((base / FROZEN / 'aws-closeout.json').read_bytes())
    assert close['state'] == 'terminated' and close['nodes'] == {'0': {'instance_id': verified['instance_id']}}
    reservation = json.loads((base / FROZEN / 'aws-reservation.json').read_bytes())
    assert reservation['source_commit'] == frozen['source_commit']
    assert reservation['source_archive_sha256'] == frozen['source_archive_sha256']
    assert set(terminal['artifacts']) == set(startup.ARTIFACTS_ARM)
    bodies = {}
    for name, ident in terminal['artifacts'].items():
        body = gzip.decompress((base / FROZEN / (name + '.gz')).read_bytes())
        assert len(body) == ident['bytes'] and peer.sha(body) == ident['sha256'], name
        if name in FROZEN_FILES:
            bodies[name] = body
    boundary = json.loads(bodies['boundary-check.json'])
    qualified = json.loads(bodies['source-qualification.json'])
    assert boundary['qualified'] and boundary['no_corpus_query']
    assert boundary['green_status'] == boundary['release_status'] == 0
    assert boundary['current_full_suite_pass_claim'] is False
    identities = source_hashes(base)
    assert len(identities) == boundary['source_file_count'] == qualified['source_file_count'] == 395
    assert source_identity(identities) == boundary['source_identity_sha256'] == qualified['source_identity_sha256']
    compiled = json.loads(bodies['compiled-source.json'])
    assert set(compiled) == set(FOCUSED_ARM)
    assert compiled == boundary['compiled_native_sha256'] == qualified['compiled_native_sha256'] == verified['compiled_native_sha256']
    for name, digest in compiled.items():
        assert identities[name] == peer.sha(bodies['compiled-source/' + name]) == digest, name
    assert boundary['compiled_http_sha256'] == compiled[RUNTIME[0]]
    assert boundary['binary_sha256'] == peer.sha(bodies['binaries/two_bit_http']) == BINARY_SHA
    assert config['binary'] == dict(terminal['artifacts']['binaries/two_bit_http'],
        key='research/native-union/20260929/arm-sha-startup-a0001/artifacts/binaries/two_bit_http')
    old_config = (base / qualified['config_path']).read_bytes()
    assert peer.sha(old_config) == verified['config_sha256'] == qualified['config_sha256'] == reservation['config_sha256']
    for name, digest in qualified['code_sha256'].items():
        assert peer.sha((base / name).read_bytes()) == digest, name
    peers = json.loads((base / ROOT / 'peer-1m-config.json').read_bytes())
    assert len(config['items']) == 2
    for item, old, metadata in zip(config['items'], peers['items'], json.loads(old_config)['items']):
        assert {key: value for key, value in item.items() if key != 'metadata_files'} == old
        assert (item['rows'], item['dimensions']) == (1000000, 768)
        assert item['authority'] == metadata['authority'] and item['indexes']['10'] == metadata['index']
        assert item['metadata_files'] == metadata['metadata_files']
        body = (base / item['closed_dev_verification_path']).read_bytes()
        assert peer.sha(body) == item['closed_dev_verification_sha256']
        dev = json.loads(body)
        assert dev['valid_measurement'] and dev['state'] == 'terminated'
    return dict(config_sha256=peer.sha(config_body), code_sha256=code,
        frozen_native_qualification=frozen,
        frozen_binary_artifacts={name: terminal['artifacts'][name] for name in FROZEN_FILES},
        source_identity_sha256=source_identity(identities), source_file_count=len(identities),
        compiled_native_sha256=compiled, current_full_suite_pass_claim=False,
        native_rebuilt=False, native_source_commit=NATIVE_COMMIT,
        historical_assurance_scope=boundary['historical_assurance_scope'])


def extract_frozen(out):
    """Run on the archived checkout; never download or rebuild the native binary."""
    out = Path(out)
    proof = json.loads((out / 'source-qualification.json').read_bytes())
    assert preflight() == proof
    for name in ('binaries/two_bit_http', 'boundary-check.json', 'compiled-source.json'):
        body = gzip.decompress((FROZEN / (name + '.gz')).read_bytes())
        target = out / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    (out / 'binaries/two_bit_http').chmod(0o755)
    print(json.dumps(dict(native_rebuilt=False, binary_sha256=BINARY_SHA,
        native_source_commit=NATIVE_COMMIT, qualification=proof), sort_keys=True))


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS = WALL, SCHEMA, ARTIFACTS
    body = runner.user_data(commit, archive_sha, archive_key, prefix).replace('v174-relaid-bind-compile', 'native-cold-first-query')
    start, end = body.index('phase=install\n'), body.index('phase=complete')
    proof = json.dumps(qualification, sort_keys=True, separators=(',', ':'))
    command = f'''phase=install
dnf install -y -q tar gzip time python3.12
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
cat >source-qualification.json <<'QUALIFICATION'
{proof}
QUALIFICATION
phase=binary-qualification
lscpu >cpu.txt
/usr/bin/time -v -o test-resources.txt bash -c 'cd repo; PYTHONPATH=. python3.12 -m scripts.launch_native_cold_first_query_spot --extract-frozen "$1"' _ "$root" >test.log 2>&1
test -s boundary-check.json && test -s binaries/two_bit_http
phase=profile
systemd-run --unit=native-cold-first-query --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1230 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1200 \\
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 scripts/run_native_cold_first_query.py {CONFIG} {qualification['config_sha256']} "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
'''
    command += '\n'.join('test -s "$root/' + name + '"' for name in ARTIFACTS if name.startswith('screen/')) + '\n'
    body = body[:start] + command + body[end:]
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    assert len(body.encode()) < 16384
    return body


def poll(ec2, s3, prefix, instance_id, started):
    original = startup.WALL
    startup.WALL = WALL
    try:
        startup.poll(ec2, s3, prefix, instance_id, started)
    finally:
        startup.WALL = original


def collect(s3, prefix, out, instance_id, commit, digest):
    raw = s3.get_object(Bucket=peer.BUCKET, Key=prefix + '/terminal.json')['Body'].read()
    (out / 'aws-terminal.json').write_bytes(raw)
    terminal = json.loads(raw)
    assert terminal['schema'] == SCHEMA and terminal['instance_id'] == instance_id
    assert terminal['source_commit'] == commit and terminal['source_archive_sha256'] == digest
    assert set(terminal['artifacts']) <= set(ARTIFACTS)
    for name, ident in terminal['artifacts'].items():
        data = s3.get_object(Bucket=peer.BUCKET, Key=prefix + '/artifacts/' + name)['Body'].read()
        assert len(data) == ident['bytes'] and peer.sha(data) == ident['sha256'], name
        path = out / (name + '.gz')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(gzip.compress(data, mtime=0))
    return terminal


def main(attempt):
    assert len(attempt) == 5 and attempt[0] == 'a' and attempt[1:].isdigit()
    assert not subprocess.check_output(['git', 'status', '--porcelain'], text=True).strip()
    proof = preflight()
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    archive = gzip.compress(subprocess.check_output(['git', 'archive', '--format=tar', 'HEAD']), mtime=0)
    digest = peer.sha(archive)
    key = 'research/native-library-check/sources/' + digest + '.tar.gz'
    prefix = 'research/native-union/20260929/cold-first-query-' + attempt
    body = user_data(commit, digest, key, prefix, proof)
    session = boto3.Session(profile_name='causality', region_name=peer.REGION)
    ec2, s3 = session.client('ec2'), session.client('s3')
    assert peer.missing(s3, prefix + '/reservation.json') and peer.missing(s3, prefix + '/terminal.json')
    active = ec2.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': ['borsuk-*']},
        {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping', 'stopped']}])
    assert not any(row['Instances'] for row in active['Reservations'])
    az = ec2.describe_subnets(SubnetIds=[peer.SUBNET])['Subnets'][0]['AvailabilityZone']
    quote = ec2.describe_spot_price_history(InstanceTypes=['c7g.2xlarge'], ProductDescriptions=['Linux/UNIX'],
        AvailabilityZone=az, MaxResults=1)['SpotPriceHistory'][0]
    assert float(quote['SpotPrice']) <= .30
    if peer.missing(s3, key):
        peer.put_if_absent(key, archive)
    else:
        assert peer.sha(s3.get_object(Bucket=peer.BUCKET, Key=key)['Body'].read()) == digest
    reservation = dict(schema=SCHEMA, source_commit=commit, source_archive_sha256=digest,
        wall_seconds=WALL, instance_type='c7g.2xlarge', availability_zone=az,
        spot_price_observed_usd_per_hour=quote['SpotPrice'], spot_quote_timestamp=quote['Timestamp'].isoformat(),
        spot_max_usd_per_hour=.30, compute_cap_usd=.15, ebs_s3_allowance_usd=.15,
        qualification=proof, config_sha256=proof['config_sha256'],
        interruption_policy='Discard interrupted worker; no automatic replacement')
    out = ROOT / 'cold-first-query' / attempt
    out.mkdir(parents=True, exist_ok=False)
    (out / 'aws-reservation.json').write_text(json.dumps(reservation, indent=2) + '\n')
    (out / 'aws-user-data.sh').write_text(body)
    peer.put_if_absent(prefix + '/reservation.json', json.dumps(reservation, sort_keys=True).encode())
    nodes = {}
    started = time.monotonic()
    try:
        receipt = ec2.run_instances(ClientToken='cold-first-' + peer.sha(prefix.encode())[:48], ImageId='ami-03748c04dc81412c6',
            InstanceType='c7g.2xlarge', MinCount=1, MaxCount=1, IamInstanceProfile={'Arn': peer.PROFILE_ARN},
            NetworkInterfaces=[{'AssociatePublicIpAddress': True, 'DeviceIndex': 0, 'Groups': [peer.SECURITY_GROUP], 'SubnetId': peer.SUBNET}],
            InstanceMarketOptions={'MarketType': 'spot', 'SpotOptions': {'InstanceInterruptionBehavior': 'terminate', 'SpotInstanceType': 'one-time', 'MaxPrice': '0.30'}},
            InstanceInitiatedShutdownBehavior='terminate', BlockDeviceMappings=[{'DeviceName': '/dev/xvda', 'Ebs': {'DeleteOnTermination': True, 'Encrypted': True, 'VolumeSize': 80, 'VolumeType': 'gp3'}}],
            TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': 'borsuk-cold-first-query'}]}], UserData=body)
        # Record all ACKed IDs before receipt persistence can fail.
        for index, row in enumerate(receipt['Instances']):
            nodes[str(index)] = dict(instance_id=row['InstanceId'])
        node = next(iter(nodes.values()))
        launch = dict(**node, prefix=prefix, source_commit=commit, source_archive_sha256=digest)
        with (out / 'aws-launch.json').open('x') as receipt_file:
            receipt_file.write(json.dumps(launch, indent=2) + '\n')
            receipt_file.flush()
            os.fsync(receipt_file.fileno())
        peer.put_if_absent(prefix + '/launch.json', json.dumps(launch, sort_keys=True).encode())
        print(json.dumps(launch), flush=True)
        poll(ec2, s3, prefix, node['instance_id'], started)
    finally:
        startup.terminate_owned(ec2, nodes)
        close = dict(nodes=nodes, state='terminated', observed_elapsed_s=round(time.monotonic() - started))
        (out / 'aws-closeout.json').write_text(json.dumps(close, indent=2) + '\n')
    terminal = collect(s3, prefix, out, node['instance_id'], commit, digest)
    assert terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    assert set(terminal['artifacts']) == set(ARTIFACTS)
    print(json.dumps(dict(complete=True, instance_id=node['instance_id'], state='terminated')), flush=True)


def self_check():
    """Mock AWS only: ACK ownership, transient observations, no replacement."""
    from unittest.mock import Mock, patch
    import tempfile
    from datetime import datetime, timezone
    module = sys.modules[__name__]
    ec2, s3 = Mock(), Mock()
    ec2.describe_instances.side_effect = [ReadTimeoutError(endpoint_url='mock'),
        {'Reservations': [{'Instances': [{'State': {'Name': 'running'}}]}]}]
    with patch.object(peer, 'missing', side_effect=[EndpointConnectionError(endpoint_url='mock'), True, True, False]), patch.object(time, 'sleep'):
        poll(ec2, s3, 'mock', 'i-original', time.monotonic())
    assert ec2.describe_instances.call_count == 2
    assert all(call.kwargs == {'InstanceIds': ['i-original']} for call in ec2.describe_instances.call_args_list)
    ec2.run_instances.assert_not_called()
    ec2.describe_instances.side_effect = None
    ec2.describe_instances.return_value = {'Reservations': [{'Instances': [{'State': {'Name': 'terminated'}}]}]}
    with patch.object(peer, 'missing', return_value=True), patch.object(time, 'sleep'):
        try:
            poll(ec2, s3, 'mock', 'i-original', time.monotonic())
        except RuntimeError:
            pass
        else:
            raise AssertionError('interruption accepted')
    ec2.run_instances.assert_not_called()
    with patch.object(time, 'monotonic', return_value=WALL + 301):
        try:
            poll(ec2, s3, 'mock', 'i-original', 0)
        except TimeoutError:
            pass
        else:
            raise AssertionError('wall cap ignored')
    owned = {'0': {'instance_id': 'i-original'}}
    ec2.terminate_instances.side_effect = [EndpointConnectionError(endpoint_url='mock'), {}]
    with patch.object(time, 'sleep'):
        startup.terminate_owned(ec2, owned)
    assert ec2.terminate_instances.call_count == 2
    ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=['i-original'])
    for failure in ('fsync', 'launch-upload', 'poll', 'interruption', 'interrupt'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {'Reservations': []}
            ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'mock-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory': [
                {'SpotPrice': '0.1', 'Timestamp': datetime.now(timezone.utc)}]}
            ec2.run_instances.return_value = {'Instances': [{'InstanceId': 'i-original'}]}
            writes = [None, None, OSError('upload')] if failure == 'launch-upload' else [None, None, None]
            error = {'interruption': RuntimeError('worker interrupted'), 'interrupt': KeyboardInterrupt()}.get(failure, ReadTimeoutError(endpoint_url='mock'))
            with patch.object(module, 'ROOT', Path(tmp)), patch.object(boto3, 'Session', return_value=session), patch.object(subprocess, 'check_output', side_effect=['', '0'*40, b'archive']), patch.object(module, 'preflight', return_value={'config_sha256': '1'*64}), patch.object(module, 'user_data', return_value='mock'), patch.object(peer, 'missing', return_value=True), patch.object(peer, 'put_if_absent', side_effect=writes), patch.object(os, 'fsync', side_effect=OSError('persist') if failure == 'fsync' else None), patch.object(module, 'poll', side_effect=error):
                try:
                    main('a0001')
                except (OSError, ReadTimeoutError, RuntimeError, KeyboardInterrupt):
                    pass
                else:
                    raise AssertionError('failure swallowed')
            ec2.run_instances.assert_called_once()
            ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-original'])
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=['i-original'])
            close = json.loads((Path(tmp)/'cold-first-query/a0001/aws-closeout.json').read_text())
            assert close['state'] == 'terminated' and close['nodes'] == owned
    with tempfile.TemporaryDirectory() as tmp, patch.object(module, 'CONFIG', Path(tmp)/'config.json'):
        CONFIG.write_text('{}')
        proof = dict(config_sha256=peer.sha(CONFIG.read_bytes()), frozen_native_qualification={'path': str(FROZEN/'verification.json')})
        body = user_data('0'*40, '1'*64, 'sources/mock', 'mock', proof)
        assert '--on-active=1800s' in body and 'RuntimeMaxSec=1230' in body
        assert 'MemoryMax=8G' in body and 'MemorySwapMax=0' in body
        assert 'ulimit -v 4194304' in body and 'taskset -c 4-5 python3.12' in body
        assert 'taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup' in body
        assert 'TOKIO_WORKER_THREADS=4' in body and 'BORSUK_NATIVE_MEMORY_BYTES=1073741824' in body
        assert 'rustup' not in body and 'cargo build' not in body and 'cargo test' not in body
        assert '--signal=TERM --kill-after=30 1200' in body
        assert 'run_native_cold_first_query.py' in body and len(body.encode()) < 16384
    # When integrated with the parent's worker/config, exercise the real frozen
    # authority and prove changed campaign/source/binary metadata is rejected.
    if CONFIG.exists():
        proof = preflight()
        assert proof['source_file_count'] == 395 and len(proof['compiled_native_sha256']) == 8
        assert set(proof['code_sha256']) == set(CODE) and proof['native_rebuilt'] is False
        original = json.loads(CONFIG.read_bytes())
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out/'source-qualification.json').write_text(json.dumps(proof))
            extract_frozen(out)
            assert peer.sha((out/'binaries/two_bit_http').read_bytes()) == BINARY_SHA
            assert (out/'binaries/two_bit_http').stat().st_mode & 0o111
            for name in ('boundary-check.json', 'compiled-source.json'):
                assert (out/name).read_bytes() == gzip.decompress((FROZEN/(name+'.gz')).read_bytes())
            config_path = out/'bad-config.json'
            for key, value in [('count', 63), ('k', 100), ('client_cpu_affinity', [0, 1]),
                    ('binary', dict(original['binary'], sha256='0'*64)),
                    ('frozen_native_qualification', dict(original['frozen_native_qualification'], source_commit='0'*40)),
                    ('items', [dict(original['items'][0], metadata_files={}), original['items'][1]])]:
                config_path.write_text(json.dumps(dict(original, **{key: value})))
                with patch.object(module, 'CONFIG', config_path):
                    try:
                        preflight()
                    except AssertionError:
                        pass
                    else:
                        raise AssertionError('changed metadata accepted: ' + key)
    print('cold first-query controller synthetic checks PASS')


if __name__ == '__main__':
    if sys.argv[1:2] == ['--extract-frozen']:
        assert len(sys.argv) == 3
        extract_frozen(sys.argv[2])
    elif sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        assert len(sys.argv) == 2, 'usage: python3 -m scripts.launch_native_cold_first_query_spot aNNNN'
        with open('/tmp/borsuk-native-cold-first-query-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(sys.argv[1])
