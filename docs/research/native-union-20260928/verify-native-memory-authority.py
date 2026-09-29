"""Independent CLOSED source-precision RED authority proof; no numerical execution."""
import gzip,hashlib,io,json,re,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1]
launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert terminal['schema']=='borsuk-native-native-memory-authority-v1'
assert all(terminal[k]==launch[k] for k in ['source_archive_sha256','source_base_commit','instance_id'])
assert close['state']=='terminated' and close['instance_id']==launch['instance_id']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3')
assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
 p=out/(name+'.gz');b=gzip.decompress(p.read_bytes()) if p.exists() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read()
 ident=terminal['artifacts'][name];assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
for name in terminal['artifacts']:get(name)
def archive(receipt):
 b=s3.get_object(Bucket=receipt['bucket'],Key='research/native-library-check/sources/'+receipt['source_archive_sha256']+'.tar.gz')['Body'].read()
 assert hashlib.sha256(b).hexdigest()==receipt['source_archive_sha256']
 with tarfile.open(fileobj=io.BytesIO(b),mode='r:gz') as tar:
  return {m.name:tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()}
frozen=archive(launch);prior=root/'source-precision-integration/a0003';proof=json.loads((prior/'verification.json').read_text());old_launch=json.loads((prior/'aws-launch.json').read_text());old=archive(old_launch)
assert proof['valid_check'] and proof['state']=='terminated' and proof['full_assurance']==dict(passed=2694,failed=0,ignored=26,targets=145)
assert hashlib.sha256((prior/'aws-terminal.json').read_bytes()).hexdigest()==proof['terminal_sha256']==reservation['reused_previous_assurance_terminal_sha256']
native=[n for n in frozen if Path(n).suffix=='.rs' or Path(n).name in ['Cargo.toml','Cargo.lock']]
old_native=set(proof['compiled_native_sha256']);helper='crates/borsuk/src/native_development_memory.rs'
assert len(native)==395 and set(native)==old_native|{helper}
changed=sorted(n for n in old_native if old[n]!=frozen[n]);assert changed==['crates/borsuk/examples/two_bit_http.rs','crates/borsuk/src/bin/two_bit_plan_demo.rs']
for n in native:assert Path(n).read_bytes()==frozen[n],n
assert terminal['status']=='complete' and terminal['exit_code']==0
check=json.loads(get('boundary-check.json'));assert check['red_status']==101 and check['green_passed'] and check['no_corpus_query'] and not check['full_workspace_repeated'] and not check['qualified']
assert check['helper_sha256']==hashlib.sha256(frozen[helper]).hexdigest()
red_source=frozen[helper].replace(b'.unwrap_or("1073741824")',b'.map(|_| "1073741824").unwrap_or("1073741824")',1)
assert check['red_helper_sha256']==hashlib.sha256(red_source).hexdigest()
name='native_development_memory::tests::explicit_admission_preserves_default_and_rejects_invalid_bounds'
red=get('red.log').decode();green=get('green.log').decode()
assert name+' ... FAILED' in red and 'left: Ok(1073741824)' in red and 'right: Ok(1342177280)' in red
assert green.count(name+' ... ok')==2 and green.count('test result: ok. 1 passed; 0 failed;')==2
assert 'Finished `release` profile' in get('release.log').decode() and set(check['binaries'])=={'two_bit_plan_demo','two_bit_http'}
for name,identity in check['binaries'].items():assert identity==terminal['artifacts']['binaries/'+name]
cgroup=json.loads(get('boundary-cgroup.json'));assert int(cgroup['memory.swap.peak'])==0 and all(int(line.split()[1])==0 for line in cgroup['memory.events'].splitlines() if line.split()[0] in ['oom','oom_kill'])
report=dict(valid_check=True,instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=395,unchanged_previous_native_files=392,artifacts_verified=len(terminal['artifacts']),qualification=False,no_corpus_query=True,library_full_assurance_reused=proof['full_assurance'],library_full_assurance_terminal_sha256=proof['terminal_sha256'],full_workspace_repeated=False,focused_native_tests_passed=2,boundary=check,cgroup=cgroup,compiled_native_sha256={n:hashlib.sha256(frozen[n]).hexdigest() for n in native})
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k!='compiled_native_sha256'}))
