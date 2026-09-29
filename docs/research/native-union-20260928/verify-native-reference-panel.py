"""Independent CLOSED reference CLI/source/binary/cleanup verifier; no kernels."""
import gzip,hashlib,io,json,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1]
launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw)
reservation=json.loads((out/'aws-reservation.json').read_text());close=json.loads((out/'aws-closeout.json').read_text())
assert terminal['schema']=='borsuk-native-reference-panel-v1'
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert all(terminal[k]==launch[k] for k in ['instance_id','source_archive_sha256','source_base_commit'])
assert close['instance_id']==launch['instance_id'] and close['state']=='terminated'
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3')
assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    p=out/(name+'.gz')
    b=gzip.decompress(p.read_bytes()) if p.exists() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read()
    ident=terminal['artifacts'][name]
    assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256']
    return b
for name in terminal['artifacts']:get(name)
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read()
assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256']
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    frozen={m.name:tar.extractfile(m).read() for m in tar.getmembers() if m.isfile() and (Path(m.name).suffix=='.rs' or Path(m.name).name in ['Cargo.toml','Cargo.lock'])}
assert len(frozen)==395
prior=json.loads((root/'source-completion-integration/a0002/verification.json').read_text())
boundary=json.loads((root/'http-topk-authority/a0002/verification.json').read_text())
assert prior['valid_check'] and boundary['valid_check'] and prior['full_assurance']['passed']==2696
expected=dict(prior['compiled_native_sha256']);expected['crates/borsuk/examples/two_bit_http.rs']=boundary['compiled_http_sha256']
changed=[name for name,digest in expected.items() if hashlib.sha256(frozen[name]).hexdigest()!=digest]
assert changed==reservation['changed_native_files']==['crates/borsuk/src/bin/two_bit_plan_demo.rs']
for name,data in frozen.items():assert Path(name).read_bytes()==data,name
if terminal['status']=='failed':
    assert terminal['exit_code']==1 and terminal['phase']=='reference-check'
    tests=get('tests.log').decode(); release=get('release.log').decode(); failure=get('test.log').decode()
    assert 'test result: ok. 3 passed; 0 failed;' in tests
    for name in ['declared_panel_options_are_bounded_and_explicit','live_scope_is_development_only','explicit_admission_preserves_default_and_rejects_invalid_bounds']:
        assert name+' ... ok' in tests
    assert 'Finished `release` profile' in release
    assert "assert 'test result: ok. 2 passed; 0 failed;'" in failure and 'AssertionError' in failure
    assert 'reference-check.json' not in terminal['artifacts'] and 'binaries/two_bit_plan_demo' not in terminal['artifacts']
    cgroup=json.loads(get('reference-cgroup.json'))
    assert int(cgroup['memory.swap.peak'])==0 and all(int(row.split()[1])==0 for row in cgroup['memory.events'].splitlines() if row.split()[0] in ['oom','oom_kill'])
    report=dict(valid_check=False,engineering_invalid=True,state='terminated',instance_id=launch['instance_id'],source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=395,unchanged_native_files=394,artifacts_verified=len(terminal['artifacts']),focused_native_tests_passed=3,release_build_completed=True,compiled_source_sha256=hashlib.sha256(frozen[changed[0]]).hexdigest(),binary_retained=False,cli_preopen_rejection_checks_completed=False,unchanged_library_assurance_reused=2696,no_corpus_query=True,full_workspace_repeated=False,reason='Checker expected2 tests, but included native_development_memory module adds third passing test; assertion after release build prevented binary retention and CLI checks. Fixed checker count3/test assertion placement; no paid rerun before final reload resume.',cgroup=cgroup)
    (out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
assert terminal['status']=='complete' and terminal['exit_code']==0
check=json.loads(get('reference-check.json'))
assert check['qualified'] and check['focused_tests_passed']==3 and len(check['cli_preopen_rejection_checks'])==5
assert check['focused_tests_reused']==3 and check['focused_tests_executed']==0
reuse=json.loads(get('reused-tests.json'))
partial=root/'native-reference-panel/a0001';partial_proof=json.loads((partial/'verification.json').read_text());partial_raw=(partial/'aws-terminal.json').read_bytes();partial_terminal=json.loads(partial_raw)
assert partial_proof['engineering_invalid'] and partial_proof['focused_native_tests_passed']==3 and partial_proof['state']=='terminated'
assert hashlib.sha256(partial_raw).hexdigest()==partial_proof['terminal_sha256']==reuse['terminal_sha256']==reservation['reused_test_terminal_sha256']
assert reuse['source_archive_sha256']==partial_proof['source_archive_sha256']==reservation['reused_test_source_archive_sha256']
assert reuse['compiled_source_sha256']==partial_proof['compiled_source_sha256']==check['compiled_source_sha256']
assert hashlib.sha256(get('tests.log')).hexdigest()==reuse['tests_sha256']==partial_terminal['artifacts']['tests.log']['sha256']
assert get('tests.log')==gzip.decompress((partial/'tests.log.gz').read_bytes())
assert reuse['passed']==reservation['reused_native_tests']==3
assert all(r['returncode']==1 and 'Error:' in r['stderr'] for r in check['cli_preopen_rejection_checks'])
assert check['unchanged_library_assurance_reused']==2696 and check['no_corpus_query'] and not check['full_workspace_repeated']
assert hashlib.sha256(frozen[changed[0]]).hexdigest()==check['compiled_source_sha256']
ident=terminal['artifacts']['binaries/two_bit_plan_demo']
assert ident['bytes']==check['binary_bytes'] and ident['sha256']==check['binary_sha256']
log=get('tests.log').decode()
assert 'declared_panel_options_are_bounded_and_explicit ... ok' in log
assert 'live_scope_is_development_only ... ok' in log and 'test result: ok. 3 passed; 0 failed;' in log
cgroup=json.loads(get('reference-cgroup.json'))
assert int(cgroup['memory.swap.peak'])==0 and all(int(row.split()[1])==0 for row in cgroup['memory.events'].splitlines() if row.split()[0] in ['oom','oom_kill'])
assert reservation['reused_library_terminal_sha256']==prior['terminal_sha256'] and reservation['reused_http_boundary_terminal_sha256']==boundary['terminal_sha256']
report=dict(valid_check=True,state='terminated',instance_id=launch['instance_id'],source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=395,unchanged_native_files=394,artifacts_verified=len(terminal['artifacts']),focused_tests_passed=3,focused_tests_reused=3,focused_tests_executed=0,cli_preopen_rejection_checks=5,compiled_source_sha256=check['compiled_source_sha256'],binary_sha256=check['binary_sha256'],unchanged_library_assurance_reused=2696,no_corpus_query=True,full_workspace_repeated=False,cgroup=cgroup)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
