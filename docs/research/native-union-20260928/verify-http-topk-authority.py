"""Independent CLOSED HTTP top-k source/RED/GREEN/cleanup verifier; no kernels."""
import gzip,hashlib,io,json,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1];launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);reservation=json.loads((out/'aws-reservation.json').read_text());close=json.loads((out/'aws-closeout.json').read_text())
assert terminal['schema']=='borsuk-native-http-topk-authority-v1' and hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert all(terminal[k]==launch[k] for k in ['source_archive_sha256','source_base_commit','instance_id']) and close['instance_id']==launch['instance_id'] and close['state']=='terminated'
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3');assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    p=out/(name+'.gz');b=gzip.decompress(p.read_bytes()) if p.exists() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read();i=terminal['artifacts'][name];assert len(b)==i['bytes'] and hashlib.sha256(b).hexdigest()==i['sha256'];return b
for name in terminal['artifacts']:get(name)
assert terminal['status']=='complete' and terminal['exit_code']==0
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read();assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256']
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as t:frozen={m.name:t.extractfile(m).read() for m in t.getmembers() if m.isfile() and (Path(m.name).suffix=='.rs' or Path(m.name).name in ['Cargo.toml','Cargo.lock'])}
assert len(frozen)==395
prior=root/'source-completion-integration/a0002';proof=json.loads((prior/'verification.json').read_text());assert proof['valid_check'] and proof['state']=='terminated' and proof['full_assurance']==dict(passed=2696,failed=0,ignored=26,targets=145)
assert hashlib.sha256((prior/'aws-terminal.json').read_bytes()).hexdigest()==proof['terminal_sha256']==reservation['reused_previous_assurance_terminal_sha256']
example='crates/borsuk/examples/two_bit_http.rs'
assert [n for n,d in proof['compiled_native_sha256'].items() if hashlib.sha256(frozen[n]).hexdigest()!=d]==[example]
check=json.loads(get('boundary-check.json'));assert check['qualified'] and check['red_status']==101 and check['green_status']==check['release_status']==0 and check['accepted_k']==[10,100] and check['library_assurance_reused']==2696 and check['no_corpus_query'] and not check['full_workspace_repeated']
original=frozen[example];assert hashlib.sha256(original).hexdigest()==check['original_http_sha256'];assert original.count(b'if request.k != 100')==1;fixed=get('http.fixed.rs');assert fixed==original.replace(b'if request.k != 100',b'if !matches!(request.k, 10 | 100)') and hashlib.sha256(fixed).hexdigest()==check['compiled_http_sha256']
assert terminal['artifacts']['binaries/two_bit_http']['sha256']==check['binary_sha256']
for name,b in frozen.items():assert Path(name).read_bytes() in ([b,fixed] if name==example else [b]),name
name='tests::request_identity_geometry_and_nonqueued_admission';red=get('red.log').decode();green=get('green.log').decode();assert name+' ... FAILED' in red and 'left: Err(400)' in red and 'right: Ok(())' in red and 'could not compile' not in red
assert name+' ... ok' in green and 'test result: ok. 2 passed; 0 failed;' in green
cgroup=json.loads(get('boundary-cgroup.json'));assert int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill'])
report=dict(valid_check=True,instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=395,artifacts_verified=len(terminal['artifacts']),qualified_http_top_k=[10,100],unchanged_library_assurance_reused=2696,focused_native_tests_passed=2,compiled_http_sha256=check['compiled_http_sha256'],binary_sha256=check['binary_sha256'],no_corpus_query=True,full_workspace_repeated=False,cgroup=cgroup)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
