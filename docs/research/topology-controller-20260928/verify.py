"""Reconcile a closed AWS correctness cell; never execute a query or test."""
import gzip, hashlib, io, json, re, sys, tarfile
from pathlib import Path
import boto3
out = Path(sys.argv[1])
launch = json.loads((out/'aws-launch.json').read_text())
terminal_raw = (out/'aws-terminal.json').read_bytes()
terminal = json.loads(terminal_raw)
close = json.loads((out/'aws-closeout.json').read_text())
assert close['instance_id'] == launch['instance_id'] == terminal['instance_id']
session = boto3.Session(profile_name='causality', region_name='eu-central-1')
state = session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']
assert state == 'terminated'
assert hashlib.sha256(terminal_raw).hexdigest() == (out/'aws-terminal.sha256').read_text().split()[0]
for name, ident in terminal['artifacts'].items():
    data = gzip.decompress((out/(name+'.gz')).read_bytes())
    assert len(data) == ident['bytes'] and hashlib.sha256(data).hexdigest() == ident['sha256']
sha = launch['source_archive_sha256']
archive = session.client('s3').get_object(Bucket=launch['bucket'], Key=f'research/native-library-check/sources/{sha}.tar.gz')['Body'].read()
assert hashlib.sha256(archive).hexdigest() == sha == terminal['source_archive_sha256']
matched, changed = [], []
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path = Path(member.name)
        if path.suffix == '.rs' or path.name in ('Cargo.toml','Cargo.lock','native_two_bit_topology.py','test_native_two_bit_topology.py','native_two_bit_cosine_development.py','v291_two_stage_development.py'):
            same = path.is_file() and path.read_bytes() == tar.extractfile(member).read()
            (matched if same else changed).append(member.name)
log = gzip.decompress((out/'test.log.gz').read_bytes()).decode()
counts = [tuple(map(int, x)) for x in re.findall(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)]
result = dict(instance_id=launch['instance_id'],state=state,source_archive_sha256=sha,terminal_sha256=hashlib.sha256(terminal_raw).hexdigest(),artifact_count=len(terminal['artifacts']),source_files_matched=len(matched),source_files_changed=changed,test_summaries=counts)
assert not changed and terminal['status'] == 'complete' and terminal['exit_code'] == 0
if '/review-red/' in str(out):
    assert '12 topology review regressions rejected incorrectly' in log
    assert log.count('review regression: ') == 12
elif '/red/' in str(out):
    assert '6 topology controller checks not implemented' in log
    assert log.count(': topology controller not implemented') == 6
else:
    assert 'six topology controller checks passed' in log
    if '/review-green/' in str(out):
        assert 'twelve topology review regressions passed' in log
(out/'verification.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k != "test_summaries"}))
