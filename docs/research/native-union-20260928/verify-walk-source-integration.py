"""Independent closed construction transformation, tests, binaries and cleanup proof."""
import ast,gzip,hashlib,io,json,re,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1];launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0] and terminal['schema']=='borsuk-native-walk-source-integration-v1'
assert all(terminal[k]==launch[k] for k in ['source_archive_sha256','source_base_commit','instance_id']) and close['state']=='terminated' and close['instance_id']==launch['instance_id']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3');assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    p=out/(name+'.gz');b=gzip.decompress(p.read_bytes()) if p.exists() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read();ident=terminal['artifacts'][name];assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
for name in terminal['artifacts']:get(name)
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read()
assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256']
native=0
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    worker=tar.extractfile('scripts/check_walk_source_integration.py').read()
    literals={n.targets[0].id:ast.literal_eval(n.value) for n in ast.parse(worker).body if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name) and n.targets[0].id in ['anchor','replacement','plan_anchor','plan_replacement']}
    assert set(literals)=={'anchor','replacement','plan_anchor','plan_replacement'}
    for member in tar.getmembers():
        path=Path(member.name)
        if path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']:
            expected=tar.extractfile(member).read()
            if member.name=='crates/borsuk/src/two_bit_generation.rs':
                original=expected;assert original.count(literals['anchor'])==original.count(literals['plan_anchor'])==1
                fixed=original.replace(literals['anchor'],literals['replacement']).replace(literals['plan_anchor'],literals['plan_replacement'])
                assert path.read_bytes() in [original,fixed]
            else: assert path.read_bytes()==expected,member.name
            native+=1
assert native==394 and reservation['existing_native_files_verified_except_declared_regression']==394
previous=root/'walk-source-replay/a0001'
prior_raw=(previous/'aws-terminal.json').read_bytes();proof=json.loads((previous/'verification.json').read_text())
assert hashlib.sha256(prior_raw).hexdigest()==reservation['reused_previous_assurance_terminal_sha256']==proof['terminal_sha256']
assert proof['valid_measurement'] and proof['decision']=='GO early source-code development only'
prior_launch=json.loads((previous/'aws-launch.json').read_text())
prior_archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+prior_launch['source_archive_sha256']+'.tar.gz')['Body'].read()
assert hashlib.sha256(prior_archive).hexdigest()==prior_launch['source_archive_sha256']
count=0
with tarfile.open(fileobj=io.BytesIO(prior_archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name)
        if path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']:
            expected=tar.extractfile(member).read();current=original if member.name=='crates/borsuk/src/two_bit_generation.rs' else path.read_bytes()
            if member.name=='crates/borsuk/src/two_bit_generation.rs':
                start=current.index(b'// Source nomination over bounded graph walks; no corpus-sized query allocation.');end=current.index(b'impl TwoBitGeneration {',start)
                current=current[:start]+current[end:];start=current.index(b'\n#[cfg(test)]\nmod source_walk_tests {');current=current[:start]
            assert current==expected,member.name
            count+=1
assert count==394
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=native,artifacts_verified=len(terminal['artifacts']),qualification=False,no_corpus_query=True,previous_native_files_verified_except_declared_regression=count)
required=['generation.fixed.rs','boundary-check.json','boundary-cgroup.json','red.log','green.log','full.log','release.log','binaries/two_bit_plan_demo','binaries/two_bit_http']
missing=[name for name in required if name not in terminal['artifacts']]
if missing or terminal['status']!='complete' or terminal['exit_code']!=0:
    report.update(valid_check=False,missing_artifacts=missing,phase=terminal['phase'],exit_code=terminal['exit_code'])
    (out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
assert get('generation.fixed.rs')==fixed
check=json.loads(get('boundary-check.json'))
assert check['original_source_sha256']==hashlib.sha256(original).hexdigest() and check['fixed_source_sha256']==hashlib.sha256(fixed).hexdigest()
assert check['red_status']!=0 and check['green_passed'] and check['no_corpus_query'] and check['full_workspace_repeated'] and not check['qualified']
red=get('red.log').decode();green=get('green.log').decode()
assert 'source_nomination_precedes_centroid_cutoff_and_scores_units_once ... FAILED' in red and 'walked source nomination not integrated' in red
assert 'source_nomination_precedes_centroid_cutoff_and_scores_units_once ... ok' in green and 'source_nomination_handles_one_graph_partial_rows_and_rejects_invalid_walks ... ok' in green
assert 'Running tests/two_bit_generation.rs' in green and all(failed=='0' for _,failed,_ in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',green))
assert 'Finished `release` profile' in get('release.log').decode()
assert set(check['binaries'])=={'two_bit_plan_demo','two_bit_http'}
for name,identity in check['binaries'].items():assert identity==terminal['artifacts']['binaries/'+name]
cgroup=json.loads(get('boundary-cgroup.json'))
assert int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill'])
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
roster,bench=top_level_roster(get('full.log'))
prior=root/'locality-source-check/a0001';prior_terminal=json.loads((prior/'aws-terminal.json').read_text());prior_full=gzip.decompress((prior/'full.log.gz').read_bytes())
assert len(prior_full)==prior_terminal['artifacts']['full.log']['bytes'] and hashlib.sha256(prior_full).hexdigest()==prior_terminal['artifacts']['full.log']['sha256']
old_roster,old_bench=top_level_roster(prior_full)
# Cargo adds the new binary to integration-test compile environments; executable
# fingerprints change even for unchanged targets. Match logical source/stem.
def logical(values):
    result={re.sub(r'-[0-9a-f]{16}(?=\))','',key):value for key,value in values.items()}
    assert len(result)==len(values)
    return result
roster,bench,old_roster,old_bench=map(logical,[roster,bench,old_roster,old_bench])

added=set(roster)-set(old_roster)
assert len(added)==1 and next(iter(added)).startswith('Running unittests src/bin/two_bit_walk_nomination.rs ') and roster[next(iter(added))]==(2,0,0)
assert set(old_roster)<=set(roster) and bench==old_bench
core=[k for k in old_roster if k.startswith('Running unittests src/lib.rs ') and re.search(r'deps/borsuk\)',k)];assert len(core)==1
for target,values in old_roster.items():assert roster[target]==tuple(v+(2 if target==core[0] and i==0 else 0) for i,v in enumerate(values)),target
counts=dict(passed=sum(v[0] for v in roster.values()),failed=sum(v[1] for v in roster.values()),ignored=sum(v[2] for v in roster.values()),targets=len(roster))
assert counts==dict(passed=2693,failed=0,ignored=26,targets=145),counts
report.update(valid_check=True,boundary=check,cgroup=cgroup,full_assurance=counts,cargo_executed_targets=len(roster)+len(bench),benchmark_smoke_checks=12,exact_previous_roster_plus_two_core_and_new_binary=True,roster_note='Logical Cargo source/executable stems; adding binary target changes integration-test executable fingerprints',compiled_source_sha256=hashlib.sha256(fixed).hexdigest())
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
