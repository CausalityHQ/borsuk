"""Independent CLOSED expected missing-row RED; archive and metadata only."""
import gzip,hashlib,io,json,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1];launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);reservation=json.loads((out/'aws-reservation.json').read_text());close=json.loads((out/'aws-closeout.json').read_text())
assert terminal['schema']=='borsuk-native-source-completion-red-v1' and hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert all(terminal[k]==launch[k] for k in ['source_archive_sha256','source_base_commit','instance_id']) and close['instance_id']==launch['instance_id'] and close['state']=='terminated'
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3');assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
 b=gzip.decompress((out/(name+'.gz')).read_bytes());i=terminal['artifacts'][name];assert len(b)==i['bytes'] and hashlib.sha256(b).hexdigest()==i['sha256'];return b
for name in terminal['artifacts']:get(name)
def archive(receipt):
 b=s3.get_object(Bucket=receipt['bucket'],Key='research/native-library-check/sources/'+receipt['source_archive_sha256']+'.tar.gz')['Body'].read();assert hashlib.sha256(b).hexdigest()==receipt['source_archive_sha256']
 with tarfile.open(fileobj=io.BytesIO(b),mode='r:gz') as t:return {m.name:t.extractfile(m).read() for m in t.getmembers() if m.isfile()}
frozen=archive(launch);prior=root/'walk-source-integration/a0001';l=json.loads((prior/'aws-launch.json').read_text());proof=json.loads((prior/'verification.json').read_text());old=archive(l);assert proof['valid_check'] and proof['state']=='terminated' and proof['full_assurance']['passed']==2693
assert hashlib.sha256((prior/'aws-terminal.json').read_bytes()).hexdigest()==proof['terminal_sha256']==reservation['reused_previous_assurance_terminal_sha256']
gen='crates/borsuk/src/two_bit_generation.rs';old[gen]=gzip.decompress((prior/'generation.fixed.rs.gz').read_bytes());assert hashlib.sha256(old[gen]).hexdigest()==proof['compiled_source_sha256']
native=[n for n in frozen if Path(n).suffix=='.rs' or Path(n).name in ['Cargo.toml','Cargo.lock']];helper='crates/borsuk/src/native_development_memory.rs';assert len(native)==395 and helper in native
changed=sorted(n for n in native if n!=helper and old[n]!=frozen[n]);assert changed==['crates/borsuk/examples/two_bit_http.rs','crates/borsuk/src/bin/two_bit_plan_demo.rs',gen]
memory=json.loads((root/'native-memory-authority/a0001/verification.json').read_text());assert memory['valid_check'] and memory['focused_native_tests_passed']==2
for n in [helper]+changed[:2]:assert hashlib.sha256(frozen[n]).hexdigest()==memory['compiled_native_sha256'][n]
anchor=b'#[cfg(test)]\nmod source_walk_tests';assert frozen[gen].split(anchor)[0]==old[gen].split(anchor)[0]
if terminal['status']!='complete' or terminal['exit_code']!=0:
 log=get('red.log').decode();assert 'error[E0271]' in log and '<i32 as Mul>::Output == usize' in log and 'could not compile `borsuk`' in log
 report=dict(valid_check=False,instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=395,artifacts_verified=len(terminal['artifacts']),qualification=False,no_corpus_query=True,production_body_unchanged=True,reason='Focused test closure integer inference compile failure; no expected RED executed')
 (out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
for n in native:assert Path(n).read_bytes()==frozen[n],n
config=json.loads(frozen[str(root/'source-completion-red-config.json')]);assert len(config['baseline_native_sha256'])==395
for n,digest in config['baseline_native_sha256'].items():assert hashlib.sha256(old[gen] if n==gen else frozen[n]).hexdigest()==digest,n
assert hashlib.sha256(frozen[gen].split(anchor)[0]).hexdigest()==config['production_prefix_sha256']
assert terminal['status']=='complete' and terminal['exit_code']==0
check=json.loads(get('boundary-check.json'));assert check==dict(qualified=False,no_corpus_query=True,full_workspace_repeated=False,red_status=101,production_prefix_sha256=config['production_prefix_sha256'],generation_sha256=hashlib.sha256(frozen[gen]).hexdigest())
name='source_walk_tests::bounded_completion_recovers_unvisited_rows_without_duplicate_or_extra_work';log=get('red.log').decode();assert name+' ... FAILED' in log and 'left: (0, 1.0)' in log and 'right: (160, 10.0)' in log and 'test result: FAILED. 0 passed; 1 failed;' in log
cgroup=json.loads(get('boundary-cgroup.json'));assert int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill'])
report=dict(valid_check=True,instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=395,artifacts_verified=len(terminal['artifacts']),qualification=False,no_corpus_query=True,production_body_unchanged=True,boundary=check,cgroup=cgroup)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
