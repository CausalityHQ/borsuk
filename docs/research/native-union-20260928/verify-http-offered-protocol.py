"""Independent CLOSED synthetic offered HTTP source/artifact/cleanup verifier."""
import gzip
import hashlib
import io
import json
import sys
import tarfile
from pathlib import Path
import boto3

out = Path(sys.argv[1])
root = out.parents[1]
launch = json.loads((out / 'aws-launch.json').read_text())
reservation = json.loads((out / 'aws-reservation.json').read_text())
raw = (out / 'aws-terminal.json').read_bytes()
terminal = json.loads(raw)
close = json.loads((out / 'aws-closeout.json').read_text())
assert terminal['schema'] == 'borsuk-native-http-offered-protocol-v1'
assert hashlib.sha256(raw).hexdigest() == (out / 'aws-terminal.sha256').read_text().split()[0]
assert all(terminal[key] == launch[key] for key in ['instance_id', 'source_archive_sha256', 'source_base_commit'])
assert terminal['status'] == 'complete' and terminal['exit_code'] == 0
assert close['instance_id'] == launch['instance_id'] and close['state'] == 'terminated'
session = boto3.Session(profile_name='causality', region_name='eu-central-1')
assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name'] == 'terminated'
s3 = session.client('s3')
def get(name):
    body = gzip.decompress((out / (name + '.gz')).read_bytes())
    ident = terminal['artifacts'][name]
    assert len(body) == ident['bytes'] and hashlib.sha256(body).hexdigest() == ident['sha256']
    return body
for name in terminal['artifacts']:
    get(name)
archive = s3.get_object(Bucket=launch['bucket'], Key='research/native-library-check/sources/' + launch['source_archive_sha256'] + '.tar.gz')['Body'].read()
assert hashlib.sha256(archive).hexdigest() == launch['source_archive_sha256']
with tarfile.open(fileobj=io.BytesIO(archive), mode='r:gz') as tar:
    frozen = {m.name: tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()}
prior = json.loads((root / 'source-completion-integration/a0002/verification.json').read_text())
boundary = json.loads((root / 'http-topk-authority/a0002/verification.json').read_text())
assert prior['valid_check'] and prior['full_assurance']['passed'] == 2696 and boundary['valid_check']
expected = dict(prior['compiled_native_sha256'])
expected['crates/borsuk/examples/two_bit_http.rs'] = boundary['compiled_http_sha256']
assert len(expected) == reservation['existing_native_files_verified_unchanged'] == 395
for name, digest in expected.items():
    assert hashlib.sha256(frozen[name]).hexdigest() == digest and Path(name).read_bytes() == frozen[name], name
check = json.loads(get('protocol-check.json'))
assert check['passed'] and check['checks'] == len(check['cases']) == 5
assert check['synthetic_protocol_only'] and not check['dataset_quality_measured']
assert check['ann_queries'] == 0 and not check['full_native_assurance_repeated']
assert len(check['source_sha256']) == 4
for name, digest in check['source_sha256'].items():
    assert hashlib.sha256(frozen[name]).hexdigest() == digest and Path(name).read_bytes() == frozen[name], name
cgroup = check['cgroup']
assert int(cgroup['memory.swap.peak']) == 0
assert all(int(row.split()[1]) == 0 for row in cgroup['memory.events'].splitlines() if row.split()[0] in ['oom', 'oom_kill'])
assert reservation['reused_library_terminal_sha256'] == prior['terminal_sha256']
assert reservation['reused_http_boundary_terminal_sha256'] == boundary['terminal_sha256']
report = dict(valid_check=True, state='terminated', instance_id=launch['instance_id'],
              source_archive_sha256=launch['source_archive_sha256'], terminal_sha256=hashlib.sha256(raw).hexdigest(),
              native_files_unchanged=395, protocol_source_files_verified=4,
              artifacts_verified=len(terminal['artifacts']), checks_passed=5,
              unchanged_library_assurance_reused=2696, ann_queries=0,
              dataset_quality_measured=False, cgroup=cgroup)
(out / 'verification.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
