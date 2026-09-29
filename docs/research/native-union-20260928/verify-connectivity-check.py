"""Independent closed construction transformation, tests, binaries and cleanup proof."""
import ast,gzip,hashlib,io,json,re,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1];launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0] and terminal['schema']=='borsuk-native-connectivity-check-v1'
assert all(terminal[k]==launch[k] for k in ['source_archive_sha256','source_base_commit','instance_id']) and close['state']=='terminated' and close['instance_id']==launch['instance_id']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3');assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    p=out/(name+'.gz');b=gzip.decompress(p.read_bytes()) if p.exists() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read();ident=terminal['artifacts'][name];assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
for name in terminal['artifacts']:get(name)
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read();assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256'];matched=0;native=0;original=None
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    worker=tar.extractfile('scripts/check_unit_graph_connectivity.py').read()
    literals={n.targets[0].id:ast.literal_eval(n.value) for n in ast.parse(worker).body if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name) and n.targets[0].id in ['anchor','replacement']}
    assert set(literals)=={'anchor','replacement'}
    for member in tar.getmembers():
        path=Path(member.name);is_native=path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']
        if is_native or member.name in [str(root/'connectivity-preregister.md'),'scripts/check_unit_graph_connectivity.py']:
            expected=tar.extractfile(member).read()
            if member.name=='crates/borsuk/src/unit_centroid_graph.rs':
                original=expected;assert original.count(literals['anchor'])==1;fixed=original.replace(literals['anchor'],literals['replacement']);assert path.read_bytes() in [original,fixed]
            else:assert path.read_bytes()==expected,member.name
            matched+=1;native+=is_native
assert native==393 and original and reservation['existing_native_files_verified_except_declared_regression']==393
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=native,source_files_matched=matched,artifacts_verified=len(terminal['artifacts']),qualification=False,no_corpus_query=True)
if terminal['status']!='complete' or terminal['exit_code']!=0:
    report.update(valid_check=False,phase=terminal['phase'],exit_code=terminal['exit_code']);(out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
assert get('unit_centroid_graph.fixed.rs')==fixed
check=json.loads(get('boundary-check.json'));assert check['original_graph_sha256']==hashlib.sha256(original).hexdigest() and check['fixed_graph_sha256']==hashlib.sha256(fixed).hexdigest() and check['red_status']!=0 and check['green_passed'] and check['no_corpus_query'] and check['full_workspace_repeated'] and not check['qualified']
red=get('red.log').decode();green=get('green.log').decode();assert 'duplicate_centroids_keep_seeded_discovery_reachable_and_bounded ... FAILED' in red and any(s in red for s in ['frontier ended before work cap','discovery lost required page coverage','directed graph disconnected']);assert 'duplicate_centroids_keep_seeded_discovery_reachable_and_bounded ... ok' in green and '0 failed;' in green
assert '0 failed;' in get('focused.log').decode() and 'Finished `release` profile' in get('release.log').decode()
for name,identity in check['binaries'].items():assert identity==terminal['artifacts']['binaries/'+name]
assert set(check['binaries'])=={'build_two_bit_generation','two_bit_plan_demo','two_bit_http','build_sq8_source'}
cgroup=json.loads(get('boundary-cgroup.json'));assert int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill'])
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
full=get('full.log');roster,bench=top_level_roster(full)
previous=root/'http-check/a0002';prior_raw=(previous/'aws-terminal.json').read_bytes();prior=json.loads(prior_raw)
assert hashlib.sha256(prior_raw).hexdigest()==reservation['reused_previous_assurance_terminal_sha256']
prior_full=gzip.decompress((previous/'full.log.gz').read_bytes());assert len(prior_full)==prior['artifacts']['full.log']['bytes'] and hashlib.sha256(prior_full).hexdigest()==prior['artifacts']['full.log']['sha256']
old_roster,old_bench=top_level_roster(prior_full);assert set(roster)==set(old_roster) and bench==old_bench
core=[k for k in roster if k.startswith('Running unittests src/lib.rs ') and re.search(r'deps/borsuk-[0-9a-f]+\)',k)];assert len(core)==1
for target,values in roster.items():assert values==tuple(v+(1 if target==core[0] and i==0 else 0) for i,v in enumerate(old_roster[target])),target
counts=dict(passed=sum(r[0] for r in roster.values()),failed=sum(r[1] for r in roster.values()),ignored=sum(r[2] for r in roster.values()),targets=len(roster));assert counts==dict(passed=2687,failed=0,ignored=26,targets=144),counts
raw_summaries=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',full.decode())
report.update(cargo_executed_targets=len(roster)+len(bench),benchmark_smoke_checks=12,exact_previous_top_level_roster_plus_one=True,raw_summary_passes=sum(int(r[0]) for r in raw_summaries),raw_summary_count=len(raw_summaries),count_note='Top-level harness totals; nested subprocess stdout summaries can interleave and are not separate Cargo targets')
report.update(valid_check=True,boundary=check,cgroup=cgroup,full_assurance=counts,compiled_graph_sha256=hashlib.sha256(fixed).hexdigest(),declared_source_transformation=True)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
