"""Closed HTTP boundary source/artifact/RED-GREEN/termination verification."""
import gzip,hashlib,io,json,re,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1];launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0] and terminal['schema']=='borsuk-native-http-check-v1'
assert all(terminal[k]==launch[k] for k in ['source_archive_sha256','source_base_commit','instance_id']) and close['state']=='terminated' and close['instance_id']==launch['instance_id']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3');assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    p=out/(name+'.gz');b=gzip.decompress(p.read_bytes()) if p.exists() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read();ident=terminal['artifacts'][name];assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
for name in terminal['artifacts']:get(name)
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read();assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256'];matched=0;native=0
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name);is_native=path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']
        if is_native or member.name in [str(root/'http-plan.md'),'scripts/check_two_bit_http_boundary.py']:
            assert path.read_bytes()==tar.extractfile(member).read(),member.name;matched+=1;native+=is_native
assert native==393 and reservation['existing_native_files_verified_except_declared_http_and_send_fix']==392
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=native,source_files_matched=matched,artifacts_verified=len(terminal['artifacts']),qualification=False,no_ann_query_run=True)
if terminal['status']!='complete' or terminal['exit_code']!=0:
    report.update(valid_check=False,phase=terminal['phase'],exit_code=terminal['exit_code']);(out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
check=json.loads(get('boundary-check.json'));source=Path('crates/borsuk/examples/two_bit_http.rs').read_bytes();assert check['original_example_sha256']==hashlib.sha256(source).hexdigest() and check['red_example_sha256']==hashlib.sha256(source.replace(b'request.control_epoch != authority.control_epoch',b'false')).hexdigest()
assert check['red_status']!=0 and check['green_passed'] and check['no_ann_query_run'] and check['full_workspace_repeated'] and not check['qualified']
assert 'request_identity_geometry_and_nonqueued_admission ... FAILED' in get('red.log').decode() and 'test result: ok. 1 passed; 0 failed;' in get('green.log').decode() and 'Finished `release` profile' in get('release.log').decode()
assert check['binary_sha256']==terminal['artifacts']['binaries/two_bit_http']['sha256'] and check['binary_bytes']==terminal['artifacts']['binaries/two_bit_http']['bytes']
cgroup=json.loads(get('boundary-cgroup.json'));assert int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill'])
full=get('full.log').decode();summaries=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',full)
assert len(summaries)==148 and sum(int(r[0]) for r in summaries)==2690 and sum(int(r[1]) for r in summaries)==0 and sum(int(r[2]) for r in summaries)==26
focused=get('focused.log').decode();assert 'bounded_range_query_future_is_send ... ok' in focused and '0 failed;' in focused
report.update(valid_check=True,boundary=check,cgroup=cgroup,core_full_assurance_reused=False,full_assurance=dict(passed=2690,failed=0,ignored=26,targets=148))
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
