"""Independent CLOSED source-precision RED authority proof; no numerical execution."""
import gzip,hashlib,io,json,re,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1]
launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert terminal['schema']=='borsuk-native-source-precision-red-v1'
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
frozen=archive(launch);prior=root/'walk-source-integration/a0001';old_launch=json.loads((prior/'aws-launch.json').read_text());proof=json.loads((prior/'verification.json').read_text());old=archive(old_launch)
prior_raw=(prior/'aws-terminal.json').read_bytes();assert hashlib.sha256(prior_raw).hexdigest()==proof['terminal_sha256']==reservation['reused_previous_assurance_terminal_sha256'] and proof['valid_check'] and proof['state']=='terminated'
generation='crates/borsuk/src/two_bit_generation.rs';codec='crates/borsuk/src/rotated_two_bit.rs'
old[generation]=gzip.decompress((prior/'generation.fixed.rs.gz').read_bytes());assert hashlib.sha256(old[generation]).hexdigest()==proof['compiled_source_sha256']
native=[n for n in frozen if Path(n).suffix=='.rs' or Path(n).name in ['Cargo.toml','Cargo.lock']]
assert len(native)==394 and sorted(n for n in native if old[n]!=frozen[n])==[codec]
assert frozen[codec].split(b'\n#[cfg(test)]\nmod precision_tests {',1)[0].rstrip(b'\n')==old[codec].rstrip(b'\n')
for n in native:assert Path(n).read_bytes()==frozen[n],n
assert terminal['status']=='complete' and terminal['exit_code']==0
check=json.loads(get('boundary-check.json'))
assert check['red_status']==101 and check['red_verified'] and check['no_corpus_query'] and not check['full_workspace_repeated'] and not check['qualified']
assert check['original_source_sha256']==hashlib.sha256(frozen[codec]).hexdigest()
red=get('red.log').decode();name='rotated_two_bit::precision_tests::three_bit_records_match_scalar_cosine_and_admit_exact_lookup_scratch'
assert name+' ... FAILED' in red and 'three-bit record geometry' in red and 'left: 200' in red and 'right: 296' in red
assert 'test result: FAILED. 0 passed; 1 failed;' in red
cgroup=json.loads(get('boundary-cgroup.json'));assert int(cgroup['memory.swap.peak'])==0 and all(int(line.split()[1])==0 for line in cgroup['memory.events'].splitlines() if line.split()[0] in ['oom','oom_kill'])
report=dict(valid_check=True,instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=len(native),artifacts_verified=len(terminal['artifacts']),qualification=False,no_corpus_query=True,production_body_unchanged=True,boundary=check,cgroup=cgroup)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
