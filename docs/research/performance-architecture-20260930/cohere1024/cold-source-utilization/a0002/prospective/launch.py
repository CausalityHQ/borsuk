"""Minimal launch glue for the frozen, single qualification/evidence attempt."""
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess

root = Path('/data/target/borsuk-cold-membership-native/source-utilization-a0002')
repo = Path('/home/rb/worktrees/borsuk-prod-ready-v9')
prefix_docs = 'docs/research/performance-architecture-20260930/cohere1024/cold-source-utilization/a0002/prospective/'
protocol = json.loads((root / 'protocol.json').read_text())
commit = (root / 'protocol-commit.txt').read_text().strip()
env = dict(os.environ, AWS_MAX_ATTEMPTS='1')
def git(*args):
    return subprocess.check_output(['/usr/bin/git', *args], cwd=repo)
def aws(*args):
    q = subprocess.run(['aws', '--profile', 'causality', '--region', 'eu-central-1', *args], capture_output=True, text=True, timeout=55, env=env)
    assert q.returncode == 0, q.stderr
    return json.loads(q.stdout) if q.stdout.strip() else {}
assert git('rev-parse', 'HEAD').decode().strip() == commit == git('rev-parse', 'origin/main').decode().strip()
for name in [*protocol['inputs'], 'protocol.json', 'user-data.sh', 'bundle.json', 'local-admission-canary.json', 'root-source-review.json']:
    if name == 'B1.jsonl':
        # Closed raw metadata stays in immutable S3 evidence; its body pin is
        # committed in the protocol/config rather than duplicating 11MB in Git.
        continue
    assert git('show', commit + ':' + prefix_docs + name) == (root / name).read_bytes(), name
assert git('show', commit + ':' + prefix_docs + 'launch.py') == Path(__file__).read_bytes()
assert protocol['status'] == 'FROZEN_REVIEWED_ADMITTED_NATIVE_QUALIFICATION_AND_METADATA_REPLAY_ONLY'
assert protocol['paid_launch_authorized'] is True and protocol['ann_run'] is False and protocol['performance_claim'] is False
assert protocol['build_seconds'] == 7200 and protocol['gate_unit_seconds'] == 7440 and protocol['machine_seconds'] == 9000
assert protocol['build_memory_bytes'] == 8589934592 and protocol['build_cpu'] == 2 and protocol['build_jobs'] == 1 and protocol['swap'] == 0 and protocol['pids'] == 512
assert protocol['replay'] == {'cpu': 1, 'memory_bytes': 268435456, 'swap': 0, 'pids': 128, 'seconds': 120, 'network': 'AF_UNIX', 'input_label': 'B1', 'historical_sq8_query_byte_cap': 16773120}
assert json.loads((root / 'local-admission-canary.json').read_text())['status'] == 'PASS_BOUNDED_STATIC_ADMISSION_AND_SYNTHETIC_GLUE'
assert json.loads((root / 'root-source-review.json').read_text())['status'] == 'REQUIRED_REVIEW_REPAIRS_INDEPENDENTLY_CHECKED_NATIVE_UNRUN'
assert not (root / 'launch.json').exists() and not (root / 'launch-attempt.json').exists()
assert aws('sts', 'get-caller-identity')['Account'] == '453182569524'
quote_body = aws('ec2', 'describe-spot-price-history', '--instance-types', 'c7i.2xlarge', '--product-descriptions', 'Linux/UNIX', '--availability-zone', 'eu-central-1c', '--max-items', '1')
quote = quote_body['SpotPriceHistory'][0]
assert float(quote['SpotPrice']) <= .50 and .50 * 9000 / 3600 <= 1.50
(root / 'spot-quote.json').write_text(json.dumps(quote_body, indent=2) + '\n')
name = 'borsuk-cold-source-utilization-a0002'
assert not any(r.get('Instances') for r in aws('ec2', 'describe-instances', '--filters', 'Name=tag:Name,Values=' + name)['Reservations'])
# The exact historical terminal/termination proof path is supplied in the freeze.
prior_pin = protocol['prior_closed_proof']
proof_path = Path(prior_pin['path'])
proof_body = proof_path.read_bytes()
assert len(proof_body) == prior_pin['bytes'] and hashlib.sha256(proof_body).hexdigest() == prior_pin['sha256']
assert json.loads(proof_body)['terminated_and_waited'] is True
assert protocol['no_live_owned_job_at_freeze'] is True
bucket, prefix = protocol['bucket'], protocol['prefix']
for n, pin in protocol['inputs'].items():
    data = (root / n).read_bytes()
    assert len(data) == pin['bytes'] and hashlib.sha256(data).hexdigest() == pin['sha256'], n
    aws('s3', 'cp', str(root / n), 's3://' + bucket + '/' + prefix + '/inputs/' + n, '--only-show-errors')
    assert aws('s3api', 'head-object', '--bucket', bucket, '--key', prefix + '/inputs/' + n)['ContentLength'] == len(data)
source = protocol['source']
data = (root / 'source.tar.gz').read_bytes()
assert len(data) == source['archive_bytes'] and hashlib.sha256(data).hexdigest() == source['archive_sha256']
key = 'research/native-library-check/sources/' + source['archive_sha256'] + '.tar.gz'
aws('s3', 'cp', str(root / 'source.tar.gz'), 's3://' + bucket + '/' + key, '--only-show-errors')
assert aws('s3api', 'head-object', '--bucket', bucket, '--key', key)['ContentLength'] == len(data)
userdata = (root / 'user-data.sh').read_bytes()
assert len(userdata) == protocol['userdata']['bytes'] and hashlib.sha256(userdata).hexdigest() == protocol['userdata']['sha256']
request = json.loads((root.parent / 'page-auth-gate-a0001/run-instances.json').read_text())
request['NetworkInterfaces'][0]['SubnetId'] = 'subnet-0a12dbed0ca6fac25'
request['ClientToken'] = name
request['UserData'] = base64.b64encode(userdata).decode()
request['TagSpecifications'][0]['Tags'] = [{'Key': 'Name', 'Value': name}, {'Key': 'Purpose', 'Value': 'exact-rust-qualification-and-closed-metadata-costs'}]
assert sum(m.get('Ebs', {}).get('VolumeSize', 0) for m in request['BlockDeviceMappings']) == 80
assert request['InstanceType'] == 'c7i.2xlarge' and request['InstanceMarketOptions']['MarketType'] == 'spot' and request['InstanceInitiatedShutdownBehavior'] == 'terminate'
(root / 'run-instances.json').write_text(json.dumps(request, indent=2) + '\n')
attempt = {'protocol_commit': commit, 'source': source['candidate'], 'quote': quote, 'compute_cap_usd': 1.50, 'ancillary_cap_usd': .15, 'client_token': name, 'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'ann_run': False, 'performance_claim': False}
with (root / 'launch-attempt.json').open('x') as stream:
    stream.write(json.dumps(attempt, indent=2) + '\n')
launch = aws('ec2', 'run-instances', '--cli-input-json', 'file://' + str(root / 'run-instances.json'))
(root / 'launch.json').write_text(json.dumps(launch, indent=2) + '\n')
assert len(launch['Instances']) == 1
instance = launch['Instances'][0]['InstanceId']
(root / 'active-job.json').write_text(json.dumps({'status': 'LAUNCHED', 'instance_id': instance, 'protocol_commit': commit, 'prefix': prefix, 'bucket': bucket, 'performance_claim': False}, indent=2) + '\n')
print(instance, flush=True)
