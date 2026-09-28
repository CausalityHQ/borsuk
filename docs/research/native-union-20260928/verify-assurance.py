"""Authenticate terminal assurance receipts and source; no build/tests locally."""
import gzip,hashlib,io,json,re,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert terminal['source_archive_sha256']==launch['source_archive_sha256'] and terminal['source_base_commit']==launch['source_base_commit']
assert terminal['status']=='complete' and terminal['exit_code']==0
assert close['state']=='terminated' and close['instance_id']==terminal['instance_id']==launch['instance_id']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3')
assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    p=out/(name+'.gz');b=gzip.decompress(p.read_bytes()) if p.exists() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read()
    ident=terminal['artifacts'][name];assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
for name in terminal['artifacts']:get(name)
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read();assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256']
matched=[];native=0
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name);is_native=path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']
        if is_native or member.name in ['docs/research/native-union-20260928/plan.md','docs/research/native-union-20260928/aws-green.py']:
            assert path.read_bytes()==tar.extractfile(member).read(),member.name;matched.append(member.name);native+=is_native
assert native==392
focused=get('focused.log').decode()
assert all('Running tests/'+name+'.rs' in focused for name in ['two_bit_generation','two_bit_application_ids','two_bit_gc_delayed_delete'])
full=get('full.log').decode();matches=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',full)
assert matches and all(int(x[1])==0 for x in matches) and 'Finished `test` profile' in full
assert len(matches)>=147 and 'test two_bit_build::tests::union_generation_has_two_authenticated_graphs ... ok' in full
assert 'test tests::frozen_union_has_unique_bounded_pages ... ok' in full
assert 'Finished `release` profile' in get('release.log').decode()
assert all(name in terminal['artifacts'] for name in ['binaries/build_two_bit_generation','binaries/two_bit_plan_demo'])
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),source_files_matched=len(matched),native_files_matched=native,artifacts_verified=len(terminal['artifacts']),full_assurance=dict(passed=sum(int(x[0]) for x in matches),failed=0,ignored=sum(int(x[2]) for x in matches),targets=len(matches)),focused_summary=[list(map(int,x)) for x in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',focused)],binaries={name:terminal['artifacts']['binaries/'+name] for name in ['build_two_bit_generation','two_bit_plan_demo']},qualification=False)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
