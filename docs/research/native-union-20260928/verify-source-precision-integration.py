"""Independent CLOSED source-frontier authority proof; no numerical execution."""
import gzip,hashlib,io,json,re,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1]
launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert terminal['schema']=='borsuk-native-source-precision-integration-v1'
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
assert len(native)==394 and set(native)=={n for n in old if Path(n).suffix=='.rs' or Path(n).name in ['Cargo.toml','Cargo.lock']}
changed=sorted(n for n in native if old[n]!=frozen[n]);assert changed==reservation['changed_native_files'] and len(changed)==9
for n in native:assert Path(n).read_bytes()==frozen[n],n
red_dir=root/'source-precision-red/a0003';red_proof=json.loads((red_dir/'verification.json').read_text())
assert red_proof['valid_check'] and red_proof['state']=='terminated' and red_proof['production_body_unchanged']
assert hashlib.sha256((red_dir/'aws-terminal.json').read_bytes()).hexdigest()==red_proof['terminal_sha256']==reservation['prerequisite_red_terminal_sha256']
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=len(native),artifacts_verified=len(terminal['artifacts']),qualification=False,no_corpus_query=True)
required=['boundary-check.json','boundary-cgroup.json','green.log','full.log','release.log','binaries/two_bit_plan_demo','binaries/two_bit_http','binaries/build_two_bit_generation']
missing=[n for n in required if n not in terminal['artifacts']]
if missing or terminal['status']!='complete' or terminal['exit_code']!=0:
 report.update(valid_check=False,missing_artifacts=missing,phase=terminal['phase'],exit_code=terminal['exit_code'])
 (out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
check=json.loads(get('boundary-check.json'));assert check['green_passed'] and check['no_corpus_query'] and check['full_workspace_repeated'] and not check['qualified']
assert check['native_source_sha256']=={n:hashlib.sha256(frozen[n]).hexdigest() for n in native if n.endswith('.rs')}
green=get('green.log').decode();assert 'three_bit_records_match_scalar_cosine_and_admit_exact_lookup_scratch ... ok' in get('full.log').decode()
for suite in ['two_bit_generation','two_bit_application_ids']:assert 'Running tests/'+suite+'.rs' in green
assert all(failed=='0' for _,failed,_ in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',green))
assert 'Finished `release` profile' in get('release.log').decode()
assert set(check['binaries'])=={'two_bit_plan_demo','two_bit_http','build_two_bit_generation'}
for name,identity in check['binaries'].items():assert identity==terminal['artifacts']['binaries/'+name]
cgroup=json.loads(get('boundary-cgroup.json'));assert int(cgroup['memory.swap.peak'])==0 and all(int(line.split()[1])==0 for line in cgroup['memory.events'].splitlines() if line.split()[0] in ['oom','oom_kill'])
def top_level_roster(data):
    current=None;blocks={}
    for line in data.decode().splitlines():
        if 'Running ' in line:
            current=line.strip();assert current not in blocks;blocks[current]=[]
        if current:blocks[current].append(line)
    roster={};bench={}
    for target,lines in blocks.items():
        text='\n'.join(lines);summaries=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',text)
        if summaries:roster[target]=tuple(map(int,summaries[-1]))
        else:
            assert 'Running benches/local_search.rs ' in target
            checks=re.findall(r'Testing ([^\n]+)\nSuccess',text);assert len(checks)==12
            bench[target]=checks
    return roster,bench
def logical(values):
    result={re.sub(r'-[0-9a-f]{16}(?=\))','',key):value for key,value in values.items()}
    assert len(result)==len(values)
    return result

roster,bench=map(logical,top_level_roster(get('full.log')))
old_full=gzip.decompress((prior/'full.log.gz').read_bytes());old_terminal=json.loads(prior_raw);ident=old_terminal['artifacts']['full.log'];assert len(old_full)==ident['bytes'] and hashlib.sha256(old_full).hexdigest()==ident['sha256']
old_roster,old_bench=map(logical,top_level_roster(old_full));assert set(roster)==set(old_roster) and bench==old_bench
core=[k for k in old_roster if k.startswith('Running unittests src/lib.rs ') and re.search(r'deps/borsuk\)',k)];assert len(core)==1
for target,values in old_roster.items():assert roster[target]==tuple(v+(1 if target==core[0] and i==0 else 0) for i,v in enumerate(values)),target
counts=dict(passed=sum(v[0] for v in roster.values()),failed=sum(v[1] for v in roster.values()),ignored=sum(v[2] for v in roster.values()),targets=len(roster))
assert counts==dict(passed=2694,failed=0,ignored=26,targets=145),counts
report.update(valid_check=True,boundary=check,cgroup=cgroup,full_assurance=counts,cargo_executed_targets=len(roster)+len(bench),benchmark_smoke_checks=12,exact_previous_roster_plus_one_codec_test=True,compiled_native_sha256={n:hashlib.sha256(frozen[n]).hexdigest() for n in native},compiled_generation_sha256=hashlib.sha256(frozen[generation]).hexdigest(),compiled_codec_sha256=hashlib.sha256(frozen[codec]).hexdigest())
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
