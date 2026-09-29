"""One immutable startup profiling worker; never replace an owned launch."""
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
from scripts.check_native_startup_build import RUNTIME, FOCUSED
from scripts import launch_native_peer_1m_spot as peer
from scripts import launch_v174_relaid_bind_compile_spot as runner

ROOT = peer.ROOT
SCHEMA = 'borsuk-native-startup-profile-spot-v1'
CONFIG = ROOT / 'startup-profile-config.json'
WALL = 3600
ARTIFACTS = ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt',
    'object-native.log', 'generation.log', 'http.log', 'release.log',
    'boundary-check.json', 'boundary-cgroup.json', 'compiled-source.json', 'source-qualification.json',
    'binaries/two_bit_http', 'profile.log', 'profile-resources.txt',
    'profile-cgroup.json', 'screen/summary.json',
    *('compiled-source/' + name for name in FOCUSED),
    *(f'screen/cell{cell}-{suffix}' for cell in range(6)
      for suffix in ('server.log', 'server.time', 'close.json', 'profile.json')))


def preflight(base=Path('.')):
    root = base / ROOT
    proof_body = (root / 'source-completion-integration/a0002/verification.json').read_bytes()
    proof = json.loads(proof_body)
    assert proof['valid_check'] and proof['state'] == 'terminated'
    changed = {name for name, digest in proof['compiled_native_sha256'].items()
               if '/src/bin/' not in name and peer.sha((base / name).read_bytes()) != digest}
    assert changed == set(FOCUSED), sorted(changed)
    config_body = (base / CONFIG).read_bytes()
    config = json.loads(config_body)
    assert config['schema'] == 'borsuk-native-startup-profile-v1'
    assert config['region'] == peer.REGION and config['bucket'] == peer.BUCKET
    assert config['dataset_order'] == ['ReLAION', 'CoHere'] * 3
    assert [item['dataset'] for item in config['items']] == ['ReLAION', 'CoHere']
    assert config['ann_queries'] == 0 and config['first_query_measured'] is False
    assert config['matched_vendor_measured'] is False
    for name, digest in config['code_sha256'].items():
        assert peer.sha((base / name).read_bytes()) == digest, name
    for item, total in zip(config['items'], (256694403, 256694456)):
        files = {'manifest.json': 33910 if item['dataset'] == 'ReLAION' else 33963,
            'page_manifest.json': 280, 'page_digests.bin': 125024, 'centroids.bin': 48000032,
            'graph.bin': 4265768, 'diverse_graph.bin': 4265768, 'plane/manifest.json': 549,
            'plane/mean.bin': 3072, 'plane/records.bin': 200000000}
        assert item['metadata_files'] == files and sum(files.values()) == total
        old_name = 'fresh-rank16-dev64-config.json' if item['dataset'] == 'ReLAION' else 'fresh-cohere-dev64-config.json'
        old = json.loads((root / old_name).read_text())
        assert item['authority'] == dict(root_sha256=old['root_sha256'], generation=1, control_epoch=1)
        prefix = ('research/native-union/20260928/fresh-rank16-dev64-a0002' if item['dataset'] == 'ReLAION'
                  else 'research/native-union/20260929/fresh-cohere-dev64-a0002')
        assert item['index'] == prefix + '/indexes/' + item['dataset'].lower() + '/k10'
        body = (base / item['closed_dev_verification_path']).read_bytes()
        assert peer.sha(body) == item['closed_dev_verification_sha256']
        closed = json.loads(body)
        assert closed['valid_measurement'] and closed['state'] == 'terminated'
    return dict(config_sha256=peer.sha(config_body), code_sha256=dict(config['code_sha256'], **{name: peer.sha((base / name).read_bytes()) for name in
            ('scripts/launch_native_startup_profile_spot.py', 'scripts/check_native_startup_build.py')}),
        compiled_native_sha256={name: peer.sha((base / name).read_bytes()) for name in FOCUSED},
        prior_assurance_sha256=peer.sha(proof_body), changed_runtime_files=list(RUNTIME),
        changed_test_files=[FOCUSED[-1]], prior_unaffected_assurance_tests=2696,
        current_full_suite_pass_claim=False, items=config['items'])


def user_data(commit, archive_sha, archive_key, prefix, qualification=None):
    runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS = WALL, SCHEMA, ARTIFACTS
    body = runner.user_data(commit, archive_sha, archive_key, prefix).replace('v174-relaid-bind-compile', 'native-startup-profile')
    body = body.replace('gcc gcc-c++ cmake perl tar gzip time', 'gcc gcc-c++ cmake perl tar gzip time python3-devel python3.12 pkgconf-pkg-config')
    start, end = body.index('phase=test\n'), body.index('phase=complete')
    body = body[:start] + '''phase=example-qualification
lscpu >cpu.txt
systemd-run --unit=startup-profile --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 \\
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 2400 \\
 taskset -c 0-3 python3 "$root/repo/scripts/check_native_startup_build.py" "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
test -s boundary-check.json && test -s binaries/two_bit_http
''' + body[end:]
    frozen = json.dumps(qualification or {}, sort_keys=True)
    body = body.replace('phase=example-qualification', "cat >source-qualification.json <<'QUALIFICATION'\n" + frozen + "\nQUALIFICATION\nphase=example-qualification")
    body = body.replace('phase=complete', profile_commands() + 'phase=complete')
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    assert len(body.encode()) < 16384
    return body


def main(attempt):
    assert len(attempt) == 5 and attempt[0] == 'a' and attempt[1:].isdigit()
    assert not subprocess.check_output(['git', 'status', '--porcelain'], text=True).strip()
    subprocess.run(['git', 'fetch', 'origin', 'main'], check=True)
    subprocess.run(['git', 'merge-base', '--is-ancestor', 'HEAD', 'origin/main'], check=True)
    proof = preflight()
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    archive = gzip.compress(subprocess.check_output(['git', 'archive', '--format=tar', 'HEAD']), mtime=0)
    digest = peer.sha(archive)
    key = 'research/native-library-check/sources/' + digest + '.tar.gz'
    prefix = 'research/native-union/20260929/startup-profile-' + attempt
    body = user_data(commit, digest, key, prefix, proof)
    session = boto3.Session(profile_name='causality', region_name=peer.REGION)
    ec2, s3 = session.client('ec2'), session.client('s3')
    assert peer.missing(s3, prefix + '/reservation.json') and peer.missing(s3, prefix + '/terminal.json')
    active = ec2.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': ['borsuk-*']},
        {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping']}])
    assert not any(row['Instances'] for row in active['Reservations'])
    az = ec2.describe_subnets(SubnetIds=[peer.SUBNET])['Subnets'][0]['AvailabilityZone']
    quote = ec2.describe_spot_price_history(InstanceTypes=['c7g.2xlarge'], ProductDescriptions=['Linux/UNIX'],
        AvailabilityZone=az, MaxResults=1)['SpotPriceHistory'][0]
    assert float(quote['SpotPrice']) <= .30
    if peer.missing(s3, key):
        peer.put_if_absent(key, archive)
    else:
        assert peer.sha(s3.get_object(Bucket=peer.BUCKET, Key=key)['Body'].read()) == digest
    reservation = dict(schema=SCHEMA, source_commit=commit, source_archive_sha256=digest, wall_seconds=WALL,
        instance_type='c7g.2xlarge', availability_zone=az, spot_price_observed_usd_per_hour=quote['SpotPrice'],
        spot_quote_timestamp=quote['Timestamp'].isoformat(), compute_cap_usd=.30, ebs_s3_allowance_usd=.15,
        qualification=proof, config_sha256=proof['config_sha256'],
        interruption_policy='Discard interrupted build; no automatic replacement')
    out = ROOT / 'startup-profile' / attempt
    out.mkdir(parents=True, exist_ok=False)
    (out / 'aws-reservation.json').write_text(json.dumps(reservation, indent=2) + '\n')
    (out / 'aws-user-data.sh').write_text(body)
    peer.put_if_absent(prefix + '/reservation.json', json.dumps(reservation, sort_keys=True).encode())
    nodes = {}
    started = time.monotonic()
    try:
        receipt = ec2.run_instances(ClientToken='startup-' + peer.sha(prefix.encode())[:48], ImageId='ami-03748c04dc81412c6',
            InstanceType='c7g.2xlarge', MinCount=1, MaxCount=1, IamInstanceProfile={'Arn': peer.PROFILE_ARN},
            NetworkInterfaces=[{'AssociatePublicIpAddress': True, 'DeviceIndex': 0, 'Groups': [peer.SECURITY_GROUP], 'SubnetId': peer.SUBNET}],
            InstanceMarketOptions={'MarketType': 'spot', 'SpotOptions': {'InstanceInterruptionBehavior': 'terminate', 'SpotInstanceType': 'one-time', 'MaxPrice': '0.30'}},
            InstanceInitiatedShutdownBehavior='terminate', BlockDeviceMappings=[{'DeviceName': '/dev/xvda', 'Ebs': {'DeleteOnTermination': True, 'Encrypted': True, 'VolumeSize': 80, 'VolumeType': 'gp3'}}],
            TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': 'borsuk-startup-profile'}]}], UserData=body)
        # Own every ACK before parsing secondary fields or persisting receipts.
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
        terminate_owned(ec2, nodes)
        close = dict(nodes=nodes, state='terminated', observed_elapsed_s=round(time.monotonic() - started))
        (out / 'aws-closeout.json').write_text(json.dumps(close, indent=2) + '\n')
    terminal_body = s3.get_object(Bucket=peer.BUCKET, Key=prefix + '/terminal.json')['Body'].read()
    (out / 'aws-terminal.json').write_bytes(terminal_body)
    terminal = json.loads(terminal_body)
    assert terminal['schema'] == SCHEMA and terminal['instance_id'] == node['instance_id']
    assert terminal['source_commit'] == commit and terminal['source_archive_sha256'] == digest
    for name, ident in terminal['artifacts'].items():
        assert name in ARTIFACTS
        data = s3.get_object(Bucket=peer.BUCKET, Key=prefix + '/artifacts/' + name)['Body'].read()
        assert len(data) == ident['bytes'] and peer.sha(data) == ident['sha256']
        path = out / (name + '.gz'); path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(gzip.compress(data, mtime=0))
    assert terminal['status'] == 'complete' and terminal['exit_code'] == 0
    assert set(ARTIFACTS) == set(terminal['artifacts'])
    print(json.dumps(dict(complete=True, instance_id=node['instance_id'], state='terminated')), flush=True)


def profile_commands():
    return f'''phase=profile
systemd-run --unit=native-startup-profile --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=630 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 600 \\
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 0-3 python3.12 scripts/run_native_startup_profile.py {CONFIG} {peer.sha(CONFIG.read_bytes())} "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
'''


def poll(ec2, s3, prefix, instance_id, started):
    while time.monotonic() - started < WALL + 300:
        try:
            if not peer.missing(s3, prefix + '/terminal.json'):
                return
            state = ec2.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0]['State']['Name']
            if state in ('terminated', 'shutting-down'):
                raise RuntimeError('worker interrupted without terminal; no replacement')
            print(json.dumps(dict(instance_id=instance_id, state=state)), flush=True)
        except (EndpointConnectionError, ReadTimeoutError) as error:
            # Observation errors provide no evidence of terminal state.
            print(json.dumps(dict(instance_id=instance_id, observation_error=type(error).__name__)), flush=True)
        time.sleep(20)
    raise TimeoutError('startup profile wall exceeded; no replacement')


def terminate_owned(ec2, nodes):
    while nodes:
        try:
            peer.terminate_owned(ec2, nodes)
            return
        except (EndpointConnectionError, ReadTimeoutError) as error:
            print(json.dumps(dict(cleanup_retry=type(error).__name__, nodes=nodes)), flush=True)
            time.sleep(20)


def self_check():
    """No AWS: polling observations cannot replace or lose owned instances."""
    from unittest.mock import Mock, patch
    import tempfile
    ec2, s3 = Mock(), Mock()
    owned = {'0': {'instance_id': 'i-original'}}
    state = {'Reservations': [{'Instances': [{'State': {'Name': 'running'}}]}]}
    ec2.describe_instances.side_effect = [ReadTimeoutError(endpoint_url='mock'), state]
    with patch.object(peer, 'missing', side_effect=[EndpointConnectionError(endpoint_url='mock'), True, True, False]), patch.object(time, 'sleep'):
        poll(ec2, s3, 'synthetic', 'i-original', time.monotonic())
    assert ec2.describe_instances.call_count == 2
    assert all(call.kwargs == {'InstanceIds': ['i-original']} for call in ec2.describe_instances.call_args_list)
    ec2.run_instances.assert_not_called()
    ec2.terminate_instances.side_effect = [EndpointConnectionError(endpoint_url='mock'), {}]
    with patch.object(time, 'sleep'):
        terminate_owned(ec2, owned)
    ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=['i-original'])
    assert ec2.terminate_instances.call_count == 2 and owned['0']['instance_id'] == 'i-original'
    with patch.object(peer, 'missing', return_value=True), patch.object(time, 'sleep'):
        ec2.describe_instances.side_effect = None
        ec2.describe_instances.return_value = {'Reservations': [{'Instances': [{'State': {'Name': 'terminated'}}]}]}
        try:
            poll(ec2, s3, 'synthetic', 'i-original', time.monotonic())
        except RuntimeError:
            pass
        else:
            raise AssertionError('interruption accepted')
    with patch.object(time, 'monotonic', return_value=WALL + 301):
        try:
            poll(ec2, s3, 'synthetic', 'i-original', 0)
        except TimeoutError:
            pass
        else:
            raise AssertionError('deadline ignored')
    with tempfile.TemporaryDirectory() as tmp, patch.object(sys.modules[__name__], 'CONFIG', Path(tmp) / 'config.json'):
        CONFIG.write_text('{}')
        body = user_data('0' * 40, '1' * 64, 'sources/mock', 'synthetic')
        assert '--on-active=3600s' in body and 'MemoryMax=10G' in body and 'MemoryMax=8G' in body
        assert 'RuntimeMaxSec=2430' in body and 'RuntimeMaxSec=630' in body
        assert 'ulimit -v 4194304' in body and 'run_native_startup_profile.py' in body
        assert 'MemorySwapMax=0' in body and 'taskset -c 0-3' in body
    # Failure after ACK (durable launch upload or polling) must always close that ID.
    for failure in ('launch-upload', 'poll'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {'Reservations': []}
            ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'synthetic-az'}]}
            from datetime import datetime, timezone
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory': [
                {'SpotPrice': '0.1', 'Timestamp': datetime.now(timezone.utc)}]}
            ec2.run_instances.return_value = {'Instances': [{'InstanceId': 'i-original'}]}
            writes = [None, None, OSError('launch upload failed')] if failure == 'launch-upload' else [None, None, None]
            with patch.object(sys.modules[__name__], 'ROOT', Path(tmp)), patch.object(boto3, 'Session', return_value=session), patch.object(subprocess, 'check_output', side_effect=['', '0'*40, b'archive']), patch.object(subprocess, 'run'), patch.object(sys.modules[__name__], 'preflight', return_value={'config_sha256': '1'*64}), patch.object(sys.modules[__name__], 'user_data', return_value='synthetic'), patch.object(peer, 'missing', return_value=True), patch.object(peer, 'put_if_absent', side_effect=writes), patch.object(sys.modules[__name__], 'poll', side_effect=ReadTimeoutError(endpoint_url='mock')):
                try:
                    main('a0001')
                except (OSError, ReadTimeoutError):
                    pass
                else:
                    raise AssertionError('failure swallowed')
            ec2.run_instances.assert_called_once()
            ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-original'])
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=['i-original'])
            close = json.loads((Path(tmp) / 'startup-profile/a0001/aws-closeout.json').read_text())
            assert close['state'] == 'terminated' and close['nodes']['0']['instance_id'] == 'i-original'
    build_self_check()
    preflight_self_check()
    print('startup controller synthetic checks PASS')


def build_self_check():
    from scripts import check_native_startup_build as build
    from unittest.mock import patch
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        repo, out = Path(tmp) / 'repo', Path(tmp) / 'out'
        out.mkdir()
        for name in FOCUSED:
            source = repo / name
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text('synthetic source')
        commands = []
        def cargo(args, **kwargs):
            commands.append(args)
            kwargs['stdout'].write('test result: ok. 1 passed; 0 failed;\n')
            if args[1] == 'build':
                binary = out / 'target/release/examples/two_bit_http'
                binary.parent.mkdir(parents=True)
                binary.write_bytes(b'synthetic executable')
        with patch.object(build.subprocess, 'run', side_effect=cargo), patch.object(build, 'capture_cgroup'):
            build.main('synthetic-cargo', repo, out)
        assert len(commands) == 4 and all('--locked' in args and '--release' in args for args in commands)
        assert commands[0][-2:] == ['--lib', 'object_native_generation::tests']
        assert commands[1][-2:] == ['--test', 'two_bit_generation']
        assert commands[2][-2:] == ['--example', 'two_bit_http']
        assert commands[3][1] == 'build'
        boundary = json.loads((out / 'boundary-check.json').read_text())
        assert boundary['qualified'] and boundary['current_full_suite_pass_claim'] is False
        assert boundary['binary_sha256'] == peer.sha(b'synthetic executable')
        assert set(boundary['compiled_native_sha256']) == set(FOCUSED)


def preflight_self_check():
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        def write(name, body):
            path = base / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        for name in FOCUSED:
            write(name, b'changed source')
        unchanged = 'crates/borsuk/src/lib.rs'
        write(unchanged, b'unchanged source')
        roster = dict.fromkeys(FOCUSED, peer.sha(b'old source'))
        roster[unchanged] = peer.sha(b'unchanged source')
        write(ROOT / 'source-completion-integration/a0002/verification.json', json.dumps(
            dict(valid_check=True, state='terminated', compiled_native_sha256=roster)).encode())
        for name in ('scripts/launch_native_startup_profile_spot.py', 'scripts/check_native_startup_build.py'):
            write(name, Path(name).read_bytes())
        code = 'scripts/run_native_startup_profile.py'
        write(code, b'synthetic worker')
        config = dict(schema='borsuk-native-startup-profile-v1', region=peer.REGION, bucket=peer.BUCKET,
            dataset_order=['ReLAION', 'CoHere']*3, ann_queries=0, first_query_measured=False,
            matched_vendor_measured=False, code_sha256={code: peer.sha(b'synthetic worker')}, items=[])
        for dataset, date, family in [('ReLAION', '20260928', 'fresh-rank16-dev64'),
                                      ('CoHere', '20260929', 'fresh-cohere-dev64')]:
            root_hash = peer.sha(dataset.encode())
            write(ROOT / (family + '-config.json'), json.dumps(dict(root_sha256=root_hash)).encode())
            closed_path = str(ROOT / family / 'a0002/verification.json')
            closed = json.dumps(dict(valid_measurement=True, state='terminated')).encode()
            write(closed_path, closed)
            files = {'manifest.json': 33910 if dataset == 'ReLAION' else 33963,
                'page_manifest.json': 280, 'page_digests.bin': 125024, 'centroids.bin': 48000032,
                'graph.bin': 4265768, 'diverse_graph.bin': 4265768, 'plane/manifest.json': 549,
                'plane/mean.bin': 3072, 'plane/records.bin': 200000000}
            config['items'].append(dict(dataset=dataset, authority=dict(root_sha256=root_hash,
                generation=1, control_epoch=1), metadata_files=files,
                index=f'research/native-union/{date}/{family}-a0002/indexes/{dataset.lower()}/k10',
                closed_dev_verification_path=closed_path, closed_dev_verification_sha256=peer.sha(closed)))
        write(CONFIG, json.dumps(config).encode())
        assert preflight(base)['changed_runtime_files'] == list(RUNTIME)
        for failure in ('metadata', 'unaffected', 'code'):
            if failure == 'metadata':
                config['items'][0]['metadata_files']['manifest.json'] += 1
                write(CONFIG, json.dumps(config).encode())
            else:
                write(unchanged if failure == 'unaffected' else code, b'tampered')
            try:
                preflight(base)
            except AssertionError:
                pass
            else:
                raise AssertionError('preflight accepted ' + failure)
            config['items'][0]['metadata_files']['manifest.json'] = 33910
            write(CONFIG, json.dumps(config).encode())
            write(unchanged, b'unchanged source')
            write(code, b'synthetic worker')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        with open('/tmp/borsuk-startup-profile.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(sys.argv[1] if len(sys.argv) > 1 else 'a0001')
