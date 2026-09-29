"""Independent closed construction transformation, tests, binaries and cleanup proof."""
import ast,gzip,hashlib,io,json,re,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1];launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0] and terminal['schema']=='borsuk-native-walk-source-check-v1'
assert all(terminal[k]==launch[k] for k in ['source_archive_sha256','source_base_commit','instance_id']) and close['state']=='terminated' and close['instance_id']==launch['instance_id']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3');assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    p=out/(name+'.gz');b=gzip.decompress(p.read_bytes()) if p.exists() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read();ident=terminal['artifacts'][name];assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
for name in terminal['artifacts']:get(name)
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read()
assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256']
native=0
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    worker=tar.extractfile('scripts/check_walk_source_nomination.py').read()
    literals={n.targets[0].id:ast.literal_eval(n.value) for n in ast.parse(worker).body if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name) and n.targets[0].id in ['anchor','replacement']}
    assert set(literals)=={'anchor','replacement'}
    for member in tar.getmembers():
        path=Path(member.name)
        if path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']:
            expected=tar.extractfile(member).read()
            if member.name=='crates/borsuk/src/bin/two_bit_walk_nomination.rs':
                original=expected;assert original.count(literals['anchor'])==1
                fixed=original.replace(literals['anchor'],literals['replacement'])
                assert path.read_bytes() in [original,fixed]
            else: assert path.read_bytes()==expected,member.name
            native+=1
assert native==394 and reservation['existing_native_files_verified_except_declared_regression']==393
previous=root/'locality-source-check/a0001'
prior_raw=(previous/'aws-terminal.json').read_bytes()
assert hashlib.sha256(prior_raw).hexdigest()==reservation['reused_previous_assurance_terminal_sha256']
proof=json.loads((previous/'verification.json').read_text())
assert proof['valid_check'] and proof['full_assurance']==dict(passed=2689,failed=0,ignored=26,targets=144)
prior_launch=json.loads((previous/'aws-launch.json').read_text())
prior_archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+prior_launch['source_archive_sha256']+'.tar.gz')['Body'].read()
assert hashlib.sha256(prior_archive).hexdigest()==prior_launch['source_archive_sha256']
fixes={'crates/borsuk/src/centroid_hnsw.rs':('centroid_hnsw.fixed.rs','compiled_graph_sha256'),'crates/borsuk/src/source_order.rs':('source_order.fixed.rs','compiled_source_sha256'),'crates/borsuk/examples/build_sq8_source.rs':('build_sq8_source.fixed.rs','compiled_example_sha256')}
count=0
with tarfile.open(fileobj=io.BytesIO(prior_archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name)
        if path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']:
            expected=tar.extractfile(member).read()
            if member.name in fixes:
                name,key=fixes[member.name];expected=gzip.decompress((previous/(name+'.gz')).read_bytes())
                assert hashlib.sha256(expected).hexdigest()==proof[key]
            assert path.read_bytes()==expected,member.name
            count+=1
assert count==393
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=native,artifacts_verified=len(terminal['artifacts']),qualification=False,no_corpus_query=True,reused_unchanged_shared_native_files=393,reused_full_assurance=proof['full_assurance'])
required=['walk_nomination.fixed.rs','boundary-check.json','boundary-cgroup.json','red.log','green.log','release.log','binaries/two_bit_walk_nomination']
missing=[name for name in required if name not in terminal['artifacts']]
if missing or terminal['status']!='complete' or terminal['exit_code']!=0:
    report.update(valid_check=False,missing_artifacts=missing,phase=terminal['phase'],exit_code=terminal['exit_code'])
    (out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
assert get('walk_nomination.fixed.rs')==fixed
check=json.loads(get('boundary-check.json'))
assert check['original_source_sha256']==hashlib.sha256(original).hexdigest() and check['fixed_source_sha256']==hashlib.sha256(fixed).hexdigest()
assert check['red_status']!=0 and check['green_passed'] and check['no_corpus_query'] and not check['full_workspace_repeated'] and not check['qualified']
red=get('red.log').decode();green=get('green.log').decode()
assert 'early_source_max_replaces_centroid_cutoff_and_reuses_duplicates ... FAILED' in red and 'walk source roster not implemented' in red
assert 'early_source_max_replaces_centroid_cutoff_and_reuses_duplicates ... ok' in green and 'malformed_walks_fail_before_scoring_and_nonfinite_scores_fail ... ok' in green
assert re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',green)==[('2','0','0')]
assert 'Finished `release` profile' in get('release.log').decode()
assert set(check['binaries'])=={'two_bit_walk_nomination'} and check['binaries']['two_bit_walk_nomination']==terminal['artifacts']['binaries/two_bit_walk_nomination']
cgroup=json.loads(get('boundary-cgroup.json'))
assert int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill'])
report.update(valid_check=True,boundary=check,cgroup=cgroup,narrow_tests=dict(passed=2,failed=0,ignored=0),compiled_source_sha256=hashlib.sha256(fixed).hexdigest())
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
