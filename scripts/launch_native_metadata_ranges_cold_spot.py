"""One reviewed metadata-range build and direct ABBA cold Spot campaign."""
import fcntl
import gzip
import json
import os
import re
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
from scripts import run_native_metadata_ranges_cold as worker

ROOT = peer.ROOT
CONFIG = ROOT / 'metadata-ranges-config.json'
SCHEMA = 'borsuk-native-metadata-ranges-cold-spot-v1'
WALL = 4200
FROZEN = ROOT / 'arm-sha-startup/a0001'
NATIVE_COMMIT = 'd32d472293d7300be77eb5f1e87cf69892cc504b'
BINARY_SHA = '3565d27ffbe7a88e2a4c6d0c982ef795072d944e1dab8f11720a9bf9b31556af'
STAGE = 'crates/borsuk/src/object_native_generation.rs'
CONTROL_IDENTITY = 'c2151132c6df9000f74181a0e52e0131d148f71653c450d2c4f752ac91f8580b'
CODE = (*worker.CODE, 'scripts/launch_native_metadata_ranges_cold_spot.py',
        'scripts/check_native_metadata_ranges_build.py')
FROZEN_FILES = ('binaries/two_bit_http', 'boundary-check.json', 'compiled-source.json',
                'source-qualification.json', 'rustc-version.txt', 'cargo-version.txt',
                'cpuinfo.txt', 'arm-feature-tree.txt', 'x86-feature-tree.txt', 'run-closed.log',
                *('compiled-source/' + name for name in FOCUSED_ARM))
ARTIFACTS = ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt',
    'source-qualification.json', 'boundary-check.json', 'boundary-cgroup.json',
    'compiled-source.json', 'binaries/two_bit_http', 'object-native.log',
    'generation.log', 'http.log', 'source.log', 'release.log', 'rustc-version.txt',
    'cargo-version.txt', 'cpuinfo.txt', 'arm-feature-tree.txt', 'x86-feature-tree.txt',
    'profile.log', 'profile-resources.txt', 'profile-cgroup.json', 'screen/summary.json',
    *('screen/block' + str(block) + '-records.jsonl' for block in range(4)),
    *('compiled-source/' + name for name in FOCUSED_ARM),
    *('control/' + name for name in FROZEN_FILES))


def preflight(base=Path('.')):
    config_body = (base / CONFIG).read_bytes()
    config = json.loads(config_body)
    assert config['schema'] == 'borsuk-native-metadata-ranges-cold-v1'
    assert (config['region'], config['bucket'], config['count'], config['k']) == (peer.REGION, peer.BUCKET, 64, 10)
    assert config['dataset_order'] == ['ReLAION', 'CoHere']
    assert config['ann_queries'] == 256
    assert config['blocks'] == [dict(arm=arm, begin=begin, end=end) for arm, begin, end in worker.BLOCKS]
    assert config['client_cpu_affinity'] == [4, 5] and config['native_cpu_affinity'] == [0, 1, 2, 3]
    assert config['worker_limit_seconds'] == 1500 and config['machine_limit_seconds'] == WALL
    assert config['namespace_connect_deadline_seconds'] == 45 and config['native_process_limit_seconds'] == 60
    assert config['query_payload_timeout_seconds'] == 5
    assert config['previous_observed_development_panel'] is True
    assert config['namespace_cold_start_included'] is True and config['application_sq8_cache'] is False
    assert config['s3_service_cache'] == 'uncontrolled' and config['transport'] == 'loopback plain HTTP'
    assert config['matched_vendor_measured'] is False and config['offered_or_saturation_qps_measured'] is False
    assert config['gates'] == dict(recall_at_10_minimum=.95, all_calls_success=True,
        source_scorer_ordered_id_physical_parity=True,
        candidate_cold_p90_lower_than_control=True,
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
    reviewed = config['reviewed_native_delta_sha256']
    assert set(reviewed) == {STAGE}
    assert identities[STAGE] == reviewed[STAGE] != qualified['compiled_native_sha256'][STAGE]
    baseline = dict(identities, **{STAGE: qualified['compiled_native_sha256'][STAGE]})
    assert len(identities) == boundary['source_file_count'] == qualified['source_file_count'] == 395
    assert source_identity(baseline) == boundary['source_identity_sha256'] == qualified['source_identity_sha256'] == CONTROL_IDENTITY
    compiled = json.loads(bodies['compiled-source.json'])
    assert set(compiled) == set(FOCUSED_ARM)
    assert compiled == boundary['compiled_native_sha256'] == qualified['compiled_native_sha256'] == verified['compiled_native_sha256']
    for name, digest in compiled.items():
        assert baseline[name] == peer.sha(bodies['compiled-source/' + name]) == digest, name
    assert boundary['compiled_http_sha256'] == compiled[RUNTIME[0]]
    assert boundary['binary_sha256'] == peer.sha(bodies['binaries/two_bit_http']) == BINARY_SHA
    assert config['control_binary'] == dict(terminal['artifacts']['binaries/two_bit_http'],
        key='research/native-union/20260929/arm-sha-startup-a0001/artifacts/binaries/two_bit_http')
    old_config = (base / qualified['config_path']).read_bytes()
    assert peer.sha(old_config) == verified['config_sha256'] == qualified['config_sha256'] == reservation['config_sha256']
    for name, digest in qualified['code_sha256'].items():
        assert peer.sha((base / name).read_bytes()) == digest, name
    original_cold = json.loads((base / ROOT / 'cold-first-query-config.json').read_text())
    assert config['items'] == original_cold['items']
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
        compiled_native_sha256={name: identities[name] for name in FOCUSED_ARM},
        reviewed_native_delta_sha256=reviewed, control_source_identity_sha256=CONTROL_IDENTITY,
        control_compiled_native_sha256=compiled, current_full_suite_pass_claim=False,
        native_rebuilt=True, control_source_commit=NATIVE_COMMIT,
        artifact_roster_sha256=peer.sha(json.dumps(ARTIFACTS, separators=(',', ':')).encode()),
        config_path=str(CONFIG), campaign_schema=SCHEMA,
        historical_assurance_scope=boundary['historical_assurance_scope'])


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS = WALL, SCHEMA, ARTIFACTS
    body = runner.user_data(commit, archive_sha, archive_key, prefix).replace('v174-relaid-bind-compile', 'native-metadata-ranges-cold')
    roster = json.dumps(ARTIFACTS, separators=(',', ':'))
    body = body.replace(' '.join(ARTIFACTS), "$(python3 -c 'import json; print(\" \".join(json.load(open(\"artifact-roster.json\"))))')")
    body = body.replace(repr(ARTIFACTS), 'json.loads(Path("artifact-roster.json").read_text())')
    body = body.replace('phase=bootstrap', "cat >artifact-roster.json <<'ROSTER'\n" + roster + "\nROSTER\nphase=bootstrap")
    body = body.replace('gcc gcc-c++ cmake perl tar gzip time', 'gcc gcc-c++ cmake perl tar gzip time python3-devel python3.12 pkgconf-pkg-config')
    body = body.replace('export RUSTUP_HOME=', 'python3.12 -m ensurepip\npython3.12 -m pip install -q boto3\nexport RUSTUP_HOME=')
    start, end = body.index('phase=test\n'), body.index('phase=complete')
    proof = json.dumps(qualification, sort_keys=True, separators=(',', ':'))
    command = f'''cat >source-qualification.json <<'QUALIFICATION'
{proof}
QUALIFICATION
phase=binary-qualification
lscpu >cpu.txt
systemd-run --unit=metadata-ranges-build --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 \\
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 2400 \\
 taskset -c 0-3 python3.12 "$root/repo/scripts/check_native_metadata_ranges_build.py" "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
test -s boundary-check.json && test -s binaries/two_bit_http
phase=profile
systemd-run --unit=native-metadata-ranges-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1530 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1500 \\
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 scripts/run_native_metadata_ranges_cold.py {CONFIG} {qualification['config_sha256']} "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/control/binaries/two_bit_http" "$1/control/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
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


def main(attempt, campaign=None):
    campaign = sys.modules[__name__] if campaign is None else campaign
    assert len(attempt) == 5 and attempt[0] == 'a' and attempt[1:].isdigit()
    token_prefix = getattr(campaign, 'TOKEN_PREFIX', 'metadata-ranges-')
    assert token_prefix.isascii() and 0 < len(token_prefix) < 64
    assert not subprocess.check_output(['git', 'status', '--porcelain'], text=True).strip()
    proof = campaign.preflight()
    commit = proof.get('source_archive_commit', subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip())
    assert re.fullmatch(r'[0-9a-f]{40}', commit)
    if 'source_archive_commit' in proof:
        subprocess.run(['git', 'merge-base', '--is-ancestor', commit, 'origin/main'], check=True)
    archive = gzip.compress(subprocess.check_output(['git', 'archive', '--format=tar', commit]), mtime=0)
    digest = peer.sha(archive)
    key = 'research/native-library-check/sources/' + digest + '.tar.gz'
    prefix = getattr(campaign, 'PREFIX', 'research/native-union/20260930/metadata-ranges-cold-') + attempt
    body = campaign.user_data(commit, digest, key, prefix, proof)
    session = boto3.Session(profile_name='causality', region_name=peer.REGION)
    ec2, s3 = session.client('ec2'), session.client('s3')
    assert peer.missing(s3, prefix + '/reservation.json') and peer.missing(s3, prefix + '/terminal.json')
    active = ec2.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': ['borsuk-*']},
        {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping']}])
    assert not any(row['Instances'] for row in active['Reservations'])
    subnet = getattr(campaign, 'SUBNET', peer.SUBNET)
    instance_type = getattr(campaign, 'INSTANCE_TYPE', 'c7g.2xlarge')
    image_id = getattr(campaign, 'IMAGE_ID', 'ami-03748c04dc81412c6')
    root_device_name = getattr(campaign, 'ROOT_DEVICE_NAME', '/dev/xvda')
    spot_max = getattr(campaign, 'SPOT_MAX_USD_PER_HOUR', .30)
    assert isinstance(spot_max, (int, float)) and 0 < spot_max <= 1
    az = ec2.describe_subnets(SubnetIds=[subnet])['Subnets'][0]['AvailabilityZone']
    quote = ec2.describe_spot_price_history(InstanceTypes=[instance_type], ProductDescriptions=['Linux/UNIX'],
        AvailabilityZone=az, MaxResults=1)['SpotPriceHistory'][0]
    assert float(quote['SpotPrice']) <= spot_max
    if peer.missing(s3, key):
        peer.put_if_absent(key, archive)
    else:
        assert peer.sha(s3.get_object(Bucket=peer.BUCKET, Key=key)['Body'].read()) == digest
    del archive
    reservation = dict(schema=campaign.SCHEMA, source_commit=commit, source_archive_sha256=digest,
        wall_seconds=campaign.WALL, instance_type=instance_type, image_id=image_id, root_device_name=root_device_name, availability_zone=az, subnet_id=subnet,
        spot_price_observed_usd_per_hour=quote['SpotPrice'], spot_quote_timestamp=quote['Timestamp'].isoformat(),
        spot_max_usd_per_hour=spot_max, compute_cap_usd=getattr(campaign, 'COMPUTE_CAP', .35), ebs_s3_allowance_usd=.15,
        total_cost_measured=False,
        cost_scope=f'{campaign.WALL}s at capped Spot rate; boot/termination overhead and EBS/S3 allowance estimated',
        qualification=proof, config_sha256=proof['config_sha256'],
        interruption_policy='Discard interrupted worker; no automatic replacement')
    out = campaign.ROOT / getattr(campaign, 'NAME', 'metadata-ranges-cold') / attempt
    out.mkdir(parents=True, exist_ok=False)
    (out / 'aws-reservation.json').write_text(json.dumps(reservation, indent=2) + '\n')
    (out / 'aws-user-data.sh').write_text(body)
    peer.put_if_absent(prefix + '/reservation.json', json.dumps(reservation, sort_keys=True).encode())
    nodes = {}
    started = time.monotonic()
    try:
        receipt = ec2.run_instances(ClientToken=token_prefix + peer.sha(prefix.encode())[:min(48, 64-len(token_prefix))], ImageId=image_id,
            InstanceType=instance_type, MinCount=1, MaxCount=1, IamInstanceProfile={'Arn': peer.PROFILE_ARN},
            NetworkInterfaces=[{'AssociatePublicIpAddress': True, 'DeviceIndex': 0, 'Groups': [peer.SECURITY_GROUP], 'SubnetId': subnet}],
            InstanceMarketOptions={'MarketType': 'spot', 'SpotOptions': {'InstanceInterruptionBehavior': 'terminate', 'SpotInstanceType': 'one-time', 'MaxPrice': f'{spot_max:.2f}'}},
            InstanceInitiatedShutdownBehavior='terminate', BlockDeviceMappings=[{'DeviceName': root_device_name, 'Ebs': {'DeleteOnTermination': True, 'Encrypted': True, 'VolumeSize': 80, 'VolumeType': 'gp3'}}],
            TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': getattr(campaign, 'TAG', 'borsuk-metadata-ranges-cold')}]}], UserData=body)
        # Record all ACKed IDs before receipt persistence can fail.
        for index, row in enumerate(receipt['Instances']):
            nodes[str(index)] = dict(instance_id=row['InstanceId'])
        node = next(iter(nodes.values()))
        launch = dict(**node, nodes=nodes, prefix=prefix, source_commit=commit, source_archive_sha256=digest)
        with (out / 'aws-launch.json').open('x') as receipt_file:
            receipt_file.write(json.dumps(launch, indent=2) + '\n')
            receipt_file.flush()
            os.fsync(receipt_file.fileno())
        peer.put_if_absent(prefix + '/launch.json', json.dumps(launch, sort_keys=True).encode())
        print(json.dumps(launch), flush=True)
        campaign.poll(ec2, s3, prefix, node['instance_id'], started)
    finally:
        failure = sys.exc_info()[0] is not None
        startup.terminate_owned(ec2, nodes)
        close = dict(nodes=nodes, state='terminated', observed_elapsed_s=round(time.monotonic() - started))
        (out / 'aws-closeout.json').write_text(json.dumps(close, indent=2) + '\n')
        if failure and nodes:
            try:
                campaign.collect(s3, prefix, out, node['instance_id'], commit, digest)
            except Exception as error:
                (out / 'collection-error.json').write_text(json.dumps(dict(error_type=type(error).__name__, error=str(error))) + '\n')
    terminal = campaign.collect(s3, prefix, out, node['instance_id'], commit, digest)
    assert terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    assert set(terminal['artifacts']) == set(campaign.ARTIFACTS)
    print(json.dumps(dict(complete=True, instance_id=node['instance_id'], state='terminated')), flush=True)


def self_check(lifecycle_only=False):
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
    for failure in ('fsync', 'launch-upload', 'poll', 'interruption', 'interrupt', 'success', 'multi-ack', 'archive-source'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {'Reservations': []}
            ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'mock-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory': [
                {'SpotPrice': '0.1', 'Timestamp': datetime.now(timezone.utc)}]}
            instance_ids = ['i-original','i-extra'] if failure == 'multi-ack' else ['i-original']
            expected_owned = {str(i):dict(instance_id=node) for i,node in enumerate(instance_ids)}
            ec2.run_instances.return_value = {'Instances': [{'InstanceId': node} for node in instance_ids]}
            writes = [None, None, OSError('upload')] if failure == 'launch-upload' else [None, None, None]
            error = {'interruption': RuntimeError('worker interrupted'), 'interrupt': KeyboardInterrupt(), 'success': None, 'archive-source': None}.get(failure, ReadTimeoutError(endpoint_url='mock'))
            events = []
            ec2.terminate_instances.side_effect = lambda **kw: events.append('terminate')
            ec2.get_waiter.return_value.wait.side_effect = lambda **kw: events.append('wait')
            def collected(*args):
                assert events == ['terminate', 'wait']
                events.append('collect')
                return dict(status='complete', phase='complete', exit_code=0,
                    artifacts={name: {} for name in ARTIFACTS})
            with patch.object(module, 'ROOT', Path(tmp)), patch.object(boto3, 'Session', return_value=session), patch.object(subprocess, 'check_output', side_effect=['', '0'*40, b'archive']), patch.object(module, 'preflight', return_value=dict(config_sha256='1'*64, **({'source_archive_commit':'2'*40} if failure == 'archive-source' else {}))), patch.object(subprocess, 'run') as git_run, patch.object(module, 'user_data', return_value='mock'), patch.object(peer, 'missing', return_value=True), patch.object(peer, 'put_if_absent', side_effect=writes), patch.object(os, 'fsync', side_effect=OSError('persist') if failure == 'fsync' else None), patch.object(module, 'poll', side_effect=error), patch.object(module, 'collect', side_effect=collected):
                try:
                    main('a0001')
                except (OSError, ReadTimeoutError, RuntimeError, KeyboardInterrupt):
                    pass
                else:
                    assert failure in ('success', 'archive-source'), 'failure swallowed'
            if failure == 'archive-source':
                git_run.assert_called_once_with(['git', 'merge-base', '--is-ancestor', '2'*40, 'origin/main'], check=True)
                assert json.loads((Path(tmp)/'metadata-ranges-cold/a0001/aws-reservation.json').read_text())['source_commit'] == '2'*40
            else:
                git_run.assert_not_called()
            ec2.run_instances.assert_called_once()
            assert ec2.run_instances.call_args.kwargs['BlockDeviceMappings'] == [{'DeviceName': '/dev/xvda', 'Ebs': {'DeleteOnTermination': True, 'Encrypted': True, 'VolumeSize': 80, 'VolumeType': 'gp3'}}]
            assert json.loads((Path(tmp)/'metadata-ranges-cold/a0001/aws-reservation.json').read_text())['root_device_name'] == '/dev/xvda'
            ec2.terminate_instances.assert_called_once_with(InstanceIds=instance_ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=instance_ids)
            persisted = json.loads((Path(tmp)/'metadata-ranges-cold/a0001/aws-launch.json').read_bytes())
            assert persisted['nodes'] == expected_owned
            close = json.loads((Path(tmp)/'metadata-ranges-cold/a0001/aws-closeout.json').read_text())
            assert close['state'] == 'terminated' and close['nodes'] == expected_owned
            assert events == ['terminate', 'wait', 'collect']
    if lifecycle_only:
        print('PASS shared ACK persistence/fsync/multi-ACK/cleanup-before-collection')
        return
    with tempfile.TemporaryDirectory() as tmp, patch.object(module, 'CONFIG', Path(tmp)/'config.json'):
        CONFIG.write_text('{}')
        proof = dict(config_sha256=peer.sha(CONFIG.read_bytes()), frozen_native_qualification={'path': str(FROZEN/'verification.json')})
        body = user_data('0'*40, '1'*64, 'sources/mock', 'mock', proof)
        assert '--on-active=4200s' in body and 'RuntimeMaxSec=1530' in body
        assert 'MemoryMax=8G' in body and 'MemorySwapMax=0' in body
        assert 'ulimit -v 4194304' in body and 'taskset -c 4-5 python3.12' in body
        assert 'taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup' in body
        assert 'TOKIO_WORKER_THREADS=4' in body and 'BORSUK_NATIVE_MEMORY_BYTES=1073741824' in body
        assert 'rustup' in body and 'check_native_metadata_ranges_build.py' in body
        assert 'RuntimeMaxSec=2430' in body and 'MemoryMax=10G' in body
        assert '30 2400' in body and '$1/control/binaries/two_bit_http' in body
        assert '--signal=TERM --kill-after=30 1500' in body
        assert 'run_native_metadata_ranges_cold.py' in body and len(body.encode()) < 16384
    # Authenticate the real closed control without native execution. Only the
    # candidate stage digest is synthetic; every other source/file is real.
    if FROZEN.exists():
        original = json.loads((ROOT / 'cold-first-query-config.json').read_bytes())
        identities = source_hashes(Path('.'))
        identities[STAGE] = json.loads(gzip.decompress((FROZEN/'compiled-source.json.gz').read_bytes()))[STAGE]
        assert source_identity(identities) == CONTROL_IDENTITY
        candidate = dict(identities, **{STAGE: '1'*64})
        config = dict(original, schema='borsuk-native-metadata-ranges-cold-v1',
            control_binary=original['binary'], worker_limit_seconds=1500,
            machine_limit_seconds=WALL, ann_queries=256,
            blocks=[dict(arm=arm, begin=begin, end=end) for arm, begin, end in worker.BLOCKS],
            gates=dict(original['gates'], candidate_cold_p90_lower_than_control=True),
            reviewed_native_delta_sha256={STAGE: candidate[STAGE]},
            code_sha256={name: peer.sha(Path(name).read_bytes()) for name in worker.CODE})
        del config['binary']
        with tempfile.TemporaryDirectory() as tmp, patch.object(module, 'CONFIG', Path(tmp)/'config.json'), patch.object(module, 'source_hashes', return_value=candidate):
            CONFIG.write_text(json.dumps(config))
            proof = preflight()
            assert proof['source_file_count'] == 395 and len(proof['compiled_native_sha256']) == 8
            assert set(proof['code_sha256']) == set(CODE) and proof['native_rebuilt'] is True
            body = user_data('0'*40, '1'*64, 'sources/mock', 'mock', proof)
            assert len(body.encode()) < 16384
            for key, value in [('count', 63), ('ann_queries', 255),
                ('blocks', list(reversed(config['blocks']))),
                ('control_binary', dict(config['control_binary'], sha256='0'*64)),
                ('reviewed_native_delta_sha256', {STAGE: '0'*64}),
                ('items', [dict(config['items'][0], metadata_files={}), config['items'][1]])]:
                CONFIG.write_text(json.dumps(dict(config, **{key: value})))
                try:
                    preflight()
                except AssertionError:
                    pass
                else:
                    raise AssertionError('changed authority accepted: ' + key)
            CONFIG.write_text(json.dumps(config))
            changed = dict(candidate, **{'Cargo.toml': '0'*64})
            with patch.object(module, 'source_hashes', return_value=changed):
                try:
                    preflight()
                except AssertionError:
                    pass
                else:
                    raise AssertionError('unreviewed native delta accepted')
        # Run the actual qualification helper with a fake Cargo, not a build.
        from scripts import check_native_metadata_ranges_build as build
        from scripts import launch_native_metadata_ranges_cold_spot as build_controller
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as tmp:
            repo, out = Path(tmp)/'repo', Path(tmp)/'out'
            out.mkdir()
            for name in FOCUSED_ARM:
                path = repo/name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(Path(name).read_bytes() + (b'// synthetic delta\n' if name == STAGE else b''))
            (repo/FROZEN).parent.mkdir(parents=True, exist_ok=True)
            (repo/FROZEN).symlink_to(FROZEN.resolve(), target_is_directory=True)
            reviewed = {STAGE: peer.sha((repo/STAGE).read_bytes())}
            build_identities = dict(identities, **reviewed)
            build_proof = dict(proof, reviewed_native_delta_sha256=reviewed,
                source_identity_sha256=source_identity(build_identities),
                compiled_native_sha256={name: build_identities[name] for name in FOCUSED_ARM})
            (out/'source-qualification.json').write_text(json.dumps(build_proof))
            def features(*args):
                for name in ('rustc-version.txt', 'cargo-version.txt', 'arm-feature-tree.txt', 'x86-feature-tree.txt'):
                    (out/name).write_bytes(gzip.decompress((FROZEN/(name+'.gz')).read_bytes()))
                return {'arm_asm_selected': True}
            def fake_cargo(args, stdout, **kwargs):
                assert '--locked' in args and '--release' in args and args[0] == 'fake-cargo'
                assert args[args.index('--jobs')+1] == '4'
                if args[1] == 'test':
                    text = 'test result: ok. 1 passed; 0 failed;\n'
                    text += '\n'.join('test '+name+' ... ok' for name in build.SOURCE_TESTS)
                    stdout.write(text)
                else:
                    path = out/'target/release/examples/two_bit_http'
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(b'fake-built-binary')
            with patch.object(build_controller, 'preflight', return_value=build_proof), patch.object(build, 'source_hashes', return_value=build_identities), patch.object(build, 'feature_checks', side_effect=features), patch.object(build, 'capture_cgroup'), patch.object(subprocess, 'run', side_effect=fake_cargo) as cargo, contextlib.redirect_stdout(io.StringIO()):
                build.main('fake-cargo', repo, out)
            assert cargo.call_count == 5
            report = json.loads((out/'boundary-check.json').read_text())
            assert report['qualified'] and report['focused_tests'] == ['object-native', 'generation', 'http', 'source']
            assert report['reviewed_native_delta_sha256'] == reviewed
            assert report['source_identity_sha256'] == source_identity(build_identities)
            assert report['compiled_native_sha256'] == build_proof['compiled_native_sha256']
            assert report['compiled_native_sha256'][STAGE] == reviewed[STAGE]
            assert peer.sha((out/'binaries/two_bit_http').read_bytes()) == report['binary_sha256']
            assert (out/'control/boundary-check.json').read_bytes() == gzip.decompress((FROZEN/'boundary-check.json.gz').read_bytes())
            assert peer.sha((out/'control/binaries/two_bit_http').read_bytes()) == BINARY_SHA
            assert all((out/'compiled-source'/name).read_bytes() == (repo/name).read_bytes() for name in FOCUSED_ARM)
        print('real frozen authority and synthetic source-delta checks PASS; user-data bytes=' + str(len(body.encode())))
    print('metadata ranges controller synthetic checks PASS')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        assert len(sys.argv) == 2, 'usage: python3 -m scripts.launch_native_metadata_ranges_cold_spot aNNNN'
        with open('/tmp/borsuk-native-metadata-ranges-cold-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(sys.argv[1])
