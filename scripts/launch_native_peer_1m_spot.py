"""One owned two-Spot peer campaign; terminate each role on its terminal."""
import fcntl
import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import time

import boto3
from botocore.exceptions import EndpointConnectionError, ReadTimeoutError

from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts.launch_v157_primary_feasibility_spot import BUCKET, REGION, PROFILE_ARN, SECURITY_GROUP, missing, put_if_absent

ROOT = Path('docs/research/native-union-20260928')
CONFIG = ROOT / 'peer-1m-config.json'
WALL = 1800
SCHEMA = 'borsuk-native-peer-1m-role-v1'
SUBNET = 'subnet-0a12dbed0ca6fac25'


def sha(body):
    return hashlib.sha256(body).hexdigest()


def qualified_binary(config):
    identity = config.get('http_build_verification')
    if not identity:
        raise ValueError('private-listener build qualification required; historical binary is loopback only')
    body = Path(identity['path']).read_bytes()
    assert sha(body) == identity['sha256']
    proof = json.loads(body)
    assert proof['valid_check'] and proof['state'] == 'terminated' and proof['no_corpus_query']
    assert proof['qualification'] == 'private-listener HTTP example only'
    assert proof['library_assurance_reused'] == 2696
    assert proof['compiled_http_sha256'] == sha(Path('crates/borsuk/examples/two_bit_http.rs').read_bytes())
    return proof['binary']


def preflight(config):
    assert config['schema'] == 'borsuk-native-peer-1m-v1'
    assert config['count'] == 64 and config['offered_qps'] == config['workers'] == 8
    assert config['setting_order'] == [10, 100, 100, 10]
    assert config['gates'] == dict(mean_offered_recall_at_10_minimum=.95,
        incoming_http_p90_ms_exclusive_maximum=444, successful_qps_minimum=8, all_offered_success=True)
    assert [item['dataset'] for item in config['items']] == ['ReLAION', 'CoHere']
    assert config['namespace_cold_start_included'] is False and config['metadata_resident'] is True
    assert config['application_sq8_cache'] is False
    assert config['worker_sha256'] == sha(Path('scripts/run_native_peer_1m_worker.py').read_bytes())
    for name, expected in config['code_sha256'].items():
        assert sha(Path(name).read_bytes()) == expected
    for item in config['items']:
        path = Path(item['closed_dev_verification_path'])
        proof = json.loads(path.read_text())
        assert sha(path.read_bytes()) == item['closed_dev_verification_sha256']
        assert proof['valid_measurement'] and proof['state'] == 'terminated'
        assert proof['native_quality']['10']['recall'] >= .95
        assert (item['rows'], item['dimensions']) == (1000000, 768)
        assert item['authority']['generation'] == item['authority']['control_epoch'] == 1
        original = json.loads((ROOT / ('fresh-rank16-dev64-config.json' if item['dataset'] == 'ReLAION'
                                      else 'fresh-cohere-dev64-config.json')).read_text())
        assert item['query_split'] == original['query_split']
        assert item['authority']['root_sha256'] == original['root_sha256']
        terminal_body = (path.parent / 'aws-terminal.json').read_bytes()
        assert sha(terminal_body) == proof['terminal_sha256']
        terminal = json.loads(terminal_body)
        prefix = ('research/native-union/20260928/fresh-rank16-dev64-a0002' if item['dataset'] == 'ReLAION'
                  else 'research/native-union/20260929/fresh-cohere-dev64-a0002')
        label = item['dataset'].lower()
        assert item['indexes'] == {str(k): f'{prefix}/indexes/{label}/k{k}' for k in (10, 100)}
        for name, artifact in [('requests', 'requests64.jsonl'), ('reference-k10', 'reference-k10.jsonl'),
                               ('reference-k100', 'reference-k100.jsonl')]:
            assert item['inputs'][name] == dict(terminal['artifacts']['screen/' + artifact],
                                                key=f'{prefix}/artifacts/screen/{artifact}')
        quality_body = gzip.decompress((path.parent / 'screen/native-quality.json.gz').read_bytes())
        assert sha(quality_body) == terminal['artifacts']['screen/native-quality.json']['sha256']
        quality = json.loads(quality_body)
        assert item['inputs']['truth'] == dict(original['sealed']['truth.u32'], range_start=0,
            range_bytes=64 * 100 * 4, range_sha256=quality['truth_prefix_sha256'])
    binary = qualified_binary(config)
    assert config['binary'] == binary


def artifacts(role):
    names = ['test.log', 'test-resources.txt', 'run-closed.log', 'screen/summary.json', 'cpu.txt', 'environment.txt']
    if role == 'server':
        names.append('binaries/two_bit_http')
    for cell in range(8):
        names.append(f'screen/ready{cell}.json')
        if role == 'server':
            names += [f'screen/cell{cell}-server.log', f'screen/cell{cell}-server.time',
                      f'screen/close{cell}.json', f'screen/closed{cell}.json']
        else:
            names += [f'screen/config{cell}.json', f'screen/done{cell}.json',
                      f'screen/cell{cell}/result.json', f'screen/cell{cell}/http.jsonl']
    return tuple(names)


def user_data(role, commit, archive_sha, archive_key, prefix, config):
    runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS = WALL, SCHEMA, artifacts(role)
    body = runner.user_data(commit, archive_sha, archive_key, prefix + '/' + role)
    body = body.replace('v174-relaid-bind-compile', 'native-peer-1m')
    start = body.index('phase=install')
    stop = body.index('phase=complete', start)
    install = '''phase=install
dnf install -y -q python3.12 util-linux time
lscpu >cpu.txt
python3.12 -c 'import platform; print(platform.platform())' >environment.txt
mkdir binaries
'''
    if role == 'server':
        ident = config['binary']
        install += f'''aws s3 cp 's3://{BUCKET}/{ident['key']}' binaries/two_bit_http --only-show-errors
echo '{ident['sha256']}  binaries/two_bit_http' | sha256sum -c -
[ "$(stat -c %s binaries/two_bit_http)" = '{ident['bytes']}' ]
chmod 755 binaries/two_bit_http
'''
    limit, address, cpus = ('8G', 4194304, '0-3') if role == 'server' else ('512M', 1048576, '0-1')
    install += f'''phase=measure
systemd-run --unit=native-peer-{role} --wait --pipe -p MemoryMax={limit} -p MemorySwapMax=0 -p RuntimeMaxSec=1430 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 1400 \\
 bash -c 'set -e; ulimit -v {address}; exec taskset -c {cpus} python3.12 scripts/run_native_peer_1m_worker.py {role} {CONFIG} {sha(CONFIG.read_bytes())} {prefix} "$1/screen" "$1/binaries/two_bit_http"' _ "$root" >test.log 2>&1
test -s "$root/screen/summary.json"
'''
    body = body[:start] + install + body[stop:]
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    assert len(body.encode()) <= 16384
    return body


def terminate_owned(ec2, nodes):
    if nodes:
        ids = [node['instance_id'] for node in nodes.values()]
        ec2.terminate_instances(InstanceIds=ids)
        ec2.get_waiter('instance_terminated').wait(InstanceIds=ids)


def main(attempt):
    assert len(attempt) == 5 and attempt[0] == 'a' and attempt[1:].isdigit()
    assert not subprocess.check_output(['git', 'status', '--porcelain'], text=True).strip()
    subprocess.run(['git', 'fetch', 'origin', 'main'], check=True)
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    subprocess.run(['git', 'merge-base', '--is-ancestor', 'HEAD', 'origin/main'], check=True)
    config = json.loads(CONFIG.read_text())
    preflight(config)
    raw = subprocess.check_output(['git', 'archive', '--format=tar', 'HEAD'])
    source = gzip.compress(raw, mtime=0)
    archive_sha = sha(source)
    archive_key = 'research/native-library-check/sources/' + archive_sha + '.tar.gz'
    prefix = 'research/native-union/20260929/peer-1m-' + attempt
    bodies = {role: user_data(role, commit, archive_sha, archive_key, prefix, config) for role in ['server', 'client']}
    session = boto3.Session(profile_name='causality', region_name=REGION)
    ec2, s3 = session.client('ec2'), session.client('s3')
    assert missing(s3, prefix + '/reservation.json') and missing(s3, prefix + '/terminal.json')
    active = ec2.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': ['borsuk-*']},
        {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping']}])
    assert not any(row['Instances'] for row in active['Reservations']), 'inspect original BORSUK worker'
    group = ec2.describe_security_groups(GroupIds=[SECURITY_GROUP])['SecurityGroups'][0]
    assert any(p['IpProtocol'] == 'tcp' and p.get('FromPort') == p.get('ToPort') == 8080
        and any(pair['GroupId'] == SECURITY_GROUP for pair in p.get('UserIdGroupPairs', [])) for p in group['IpPermissions'])
    az = ec2.describe_subnets(SubnetIds=[SUBNET])['Subnets'][0]['AvailabilityZone']
    quote = ec2.describe_spot_price_history(InstanceTypes=['c7g.2xlarge'], ProductDescriptions=['Linux/UNIX'],
        AvailabilityZone=az, MaxResults=1)['SpotPriceHistory'][0]
    assert float(quote['SpotPrice']) <= .30
    if missing(s3, archive_key):
        put_if_absent(archive_key, source)
    else:
        assert sha(s3.get_object(Bucket=BUCKET, Key=archive_key)['Body'].read()) == archive_sha
    reservation = dict(schema='borsuk-native-peer-1m-spot-v1', source_commit=commit,
        source_archive_sha256=archive_sha, config_sha256=sha(CONFIG.read_bytes()), attempt=attempt,
        roles=['server', 'client'], spot_price_observed_usd_per_hour=quote['SpotPrice'],
        spot_quote_timestamp=quote['Timestamp'].isoformat(), availability_zone=az,
        instance_type='c7g.2xlarge', wall_seconds=WALL, compute_cost_cap_usd=.30,
        ebs_s3_allowance_usd=.30, interruption_policy='Discard interrupted cell; no automatic replacement')
    out = ROOT / 'peer-1m' / attempt
    out.mkdir(parents=True, exist_ok=False)
    (out / 'aws-reservation.json').write_text(json.dumps(reservation, indent=2) + '\n')
    for role, body in bodies.items():
        (out / (role + '-user-data.sh')).write_text(body)
    put_if_absent(prefix + '/reservation.json', json.dumps(reservation, sort_keys=True).encode())
    nodes, terminals = {}, {}
    started = time.monotonic()
    try:
        for role in ['server', 'client']:
            receipt = ec2.run_instances(ClientToken='peer-' + sha((prefix + role + archive_sha).encode())[:48],
                ImageId='ami-03748c04dc81412c6', InstanceType='c7g.2xlarge', MinCount=1, MaxCount=1,
                IamInstanceProfile={'Arn': PROFILE_ARN},
                NetworkInterfaces=[{'AssociatePublicIpAddress': True, 'DeviceIndex': 0,
                    'Groups': [SECURITY_GROUP], 'SubnetId': SUBNET}],
                InstanceMarketOptions={'MarketType': 'spot', 'SpotOptions': {'InstanceInterruptionBehavior': 'terminate',
                    'SpotInstanceType': 'one-time', 'MaxPrice': '0.30'}},
                InstanceInitiatedShutdownBehavior='terminate',
                BlockDeviceMappings=[{'DeviceName': '/dev/xvda', 'Ebs': {'DeleteOnTermination': True,
                    'Encrypted': True, 'VolumeSize': 80 if role == 'server' else 20, 'VolumeType': 'gp3'}}],
                TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': 'borsuk-peer-1m-' + role},
                    {'Key': 'BorsukAttempt', 'Value': attempt}]}], UserData=bodies[role])
            node = receipt['Instances'][0]
            nodes[role] = dict(instance_id=node['InstanceId'])
            nodes[role]['private_ip'] = node['PrivateIpAddress']
            (out / 'launch-progress.json').write_text(json.dumps(nodes, indent=2) + '\n')
            print(json.dumps(dict(role=role, **nodes[role])), flush=True)
        launch = dict(**nodes, prefix=prefix, bucket=BUCKET, config_sha256=reservation['config_sha256'],
            source_commit=commit, source_archive_sha256=archive_sha)
        (out / 'aws-launch.json').write_text(json.dumps(launch, indent=2) + '\n')
        put_if_absent(prefix + '/launch.json', json.dumps(launch, sort_keys=True).encode())
        while time.monotonic() - started < WALL + 300:
            try:
                for role, node in nodes.items():
                    if role in terminals:
                        continue
                    key = prefix + '/' + role + '/terminal.json'
                    if not missing(s3, key):
                        body = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
                        terminal = json.loads(body)
                        assert terminal['schema'] == SCHEMA and terminal['instance_id'] == node['instance_id']
                        assert terminal['source_commit'] == commit and terminal['source_archive_sha256'] == archive_sha
                        terminals[role] = terminal
                        (out / (role + '-terminal.json')).write_bytes(body)
                        ec2.terminate_instances(InstanceIds=[node['instance_id']])
                        if terminal['status'] != 'complete' or terminal['exit_code'] != 0:
                            raise RuntimeError('role failed; inspect original closed evidence')
                    else:
                        state = ec2.describe_instances(InstanceIds=[node['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']
                        if state in ('terminated', 'shutting-down'):
                            raise RuntimeError('role closed without terminal; discard interrupted cell')
                if len(terminals) == 2:
                    break
                print(json.dumps(dict(active_roles=[role for role in nodes if role not in terminals],
                                      elapsed_s=round(time.monotonic() - started))), flush=True)
            except (EndpointConnectionError, ReadTimeoutError) as error:
                print(json.dumps(dict(observation_error=type(error).__name__)), flush=True)
            time.sleep(20)
        else:
            raise TimeoutError('peer cluster exceeded hard wall')
    finally:
        terminate_owned(ec2, nodes)
        elapsed = time.monotonic() - started
        close = dict(nodes=nodes, state='terminated', observed_elapsed_s=round(elapsed),
            compute_cost_estimate_usd=round(elapsed / 3600 * float(quote['SpotPrice']) * len(nodes), 4),
            cost_scope='estimate only, conservatively same duration for both roles; excludes EBS/S3, not invoice')
        (out / 'aws-closeout.json').write_text(json.dumps(close, indent=2) + '\n')
        print(json.dumps(close), flush=True)
        for role in nodes:
            key = prefix + '/' + role + '/terminal.json'
            if missing(s3, key):
                continue
            body = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
            (out / (role + '-terminal.json')).write_bytes(body)
            terminal = json.loads(body)
            for name, ident in terminal['artifacts'].items():
                assert name in artifacts(role)
                data = s3.get_object(Bucket=BUCKET, Key=prefix + '/' + role + '/artifacts/' + name)['Body'].read()
                assert len(data) == ident['bytes'] and sha(data) == ident['sha256']
                path = out / role / (name + '.gz')
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(gzip.compress(data, mtime=0))
    assert len(terminals) == 2
    print(json.dumps(dict(terminal_status='complete', roles=2)), flush=True)


if __name__ == '__main__':
    with open('/tmp/borsuk-peer-1m-worker.lock', 'a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        main(sys.argv[1] if len(sys.argv) > 1 else 'a0001')
