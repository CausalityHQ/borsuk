"""One bounded Spot qualification of the private-listener HTTP example only."""
import fcntl
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

import boto3
from scripts import launch_native_peer_1m_spot as peer
from scripts import launch_v174_relaid_bind_compile_spot as runner

ROOT = peer.ROOT
SCHEMA = 'borsuk-peer-http-build-v1'
WALL = 2700
ARTIFACTS = ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt',
             'green.log', 'release.log', 'http.fixed.rs', 'boundary-check.json',
             'boundary-cgroup.json', 'listener.log', 'binaries/two_bit_http')


def preflight():
    proof = json.loads((ROOT / 'source-completion-integration/a0002/verification.json').read_text())
    assert proof['valid_check'] and proof['state'] == 'terminated'
    changed = [name for name, digest in proof['compiled_native_sha256'].items()
               if '/src/bin/' not in name and peer.sha(Path(name).read_bytes()) != digest]
    assert changed == ['crates/borsuk/examples/two_bit_http.rs'], changed
    source = Path(changed[0]).read_text()
    assert source.count('const QUERY_SLOTS: usize = 4;') == 1
    assert 'ip.is_private()' in source
    subprocess.run([sys.executable, 'scripts/check_http_peer_listener.py'], check=True)
    return proof


def user_data(commit, archive_sha, archive_key, prefix):
    runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS = WALL, SCHEMA, ARTIFACTS
    body = runner.user_data(commit, archive_sha, archive_key, prefix).replace('v174-relaid-bind-compile', 'native-peer-http-build')
    body = body.replace('gcc gcc-c++ cmake perl tar gzip time', 'gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config')
    start, end = body.index('phase=test\n'), body.index('phase=complete')
    body = body[:start] + '''phase=example-qualification
lscpu >cpu.txt
systemd-run --unit=peer-http-build --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 \\
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 2400 \\
 taskset -c 0-3 python3 "$root/repo/scripts/check_http_peer_build.py" "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
cd repo
PATH="$CARGO_HOME/bin:$PATH" python3 scripts/check_http_peer_listener.py >"$root/listener.log" 2>&1
cd "$root"
test -s boundary-check.json && test -s binaries/two_bit_http
''' + body[end:]
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
    prefix = 'research/native-union/20260929/peer-http-build-' + attempt
    body = user_data(commit, digest, key, prefix)
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
        spot_quote_timestamp=quote['Timestamp'].isoformat(), compute_cap_usd=.225, ebs_s3_allowance_usd=.15,
        changed_native_files=['crates/borsuk/examples/two_bit_http.rs'], library_assurance_reused=2696,
        prior_assurance_sha256=peer.sha((ROOT / 'source-completion-integration/a0002/verification.json').read_bytes()),
        interruption_policy='Discard interrupted build; no automatic replacement')
    out = ROOT / 'peer-http-build' / attempt
    out.mkdir(parents=True, exist_ok=False)
    (out / 'aws-reservation.json').write_text(json.dumps(reservation, indent=2) + '\n')
    (out / 'aws-user-data.sh').write_text(body)
    peer.put_if_absent(prefix + '/reservation.json', json.dumps(reservation, sort_keys=True).encode())
    nodes = {}
    started = time.monotonic()
    try:
        receipt = ec2.run_instances(ClientToken='peerbuild-' + digest[:48], ImageId='ami-03748c04dc81412c6',
            InstanceType='c7g.2xlarge', MinCount=1, MaxCount=1, IamInstanceProfile={'Arn': peer.PROFILE_ARN},
            NetworkInterfaces=[{'AssociatePublicIpAddress': True, 'DeviceIndex': 0, 'Groups': [peer.SECURITY_GROUP], 'SubnetId': peer.SUBNET}],
            InstanceMarketOptions={'MarketType': 'spot', 'SpotOptions': {'InstanceInterruptionBehavior': 'terminate', 'SpotInstanceType': 'one-time', 'MaxPrice': '0.30'}},
            InstanceInitiatedShutdownBehavior='terminate', BlockDeviceMappings=[{'DeviceName': '/dev/xvda', 'Ebs': {'DeleteOnTermination': True, 'Encrypted': True, 'VolumeSize': 80, 'VolumeType': 'gp3'}}],
            TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': 'borsuk-peer-http-build'}]}], UserData=body)
        node = nodes['build'] = dict(instance_id=receipt['Instances'][0]['InstanceId'])
        launch = dict(**node, prefix=prefix, source_commit=commit, source_archive_sha256=digest)
        (out / 'aws-launch.json').write_text(json.dumps(launch, indent=2) + '\n')
        print(json.dumps(launch), flush=True)
        while time.monotonic() - started < WALL + 300:
            if not peer.missing(s3, prefix + '/terminal.json'):
                break
            state = ec2.describe_instances(InstanceIds=[node['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']
            if state in ('terminated', 'shutting-down'):
                raise RuntimeError('build interrupted without terminal')
            print(json.dumps(dict(state=state, elapsed_s=round(time.monotonic() - started))), flush=True)
            time.sleep(20)
        else:
            raise TimeoutError('build wall exceeded')
    finally:
        peer.terminate_owned(ec2, nodes)
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
    print(json.dumps(dict(complete=True, instance_id=node['instance_id'], state='terminated')), flush=True)


if __name__ == '__main__':
    with open('/tmp/borsuk-peer-http-build.lock', 'a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        main(sys.argv[1] if len(sys.argv) > 1 else 'a0001')
