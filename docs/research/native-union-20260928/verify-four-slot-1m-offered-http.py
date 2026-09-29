"""Independent CLOSED current1M source/identity/integer quality/HTTP timing audit."""
import gzip,hashlib,io,json,math,re,struct,sys,tarfile
from collections import Counter
from pathlib import Path
import boto3

out=Path(sys.argv[1]);root=out.parents[1]
def sha(body):return hashlib.sha256(body).hexdigest()
def load(p):return json.loads(p.read_text())
launch=load(out/'aws-launch.json');reservation=load(out/'aws-reservation.json');raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=load(out/'aws-closeout.json')
assert terminal['schema']=='borsuk-four-slot-1m-offered-http-dev-v1'
assert sha(raw)==(out/'aws-terminal.sha256').read_text().split()[0]
assert all(terminal[k]==launch[k] for k in ['instance_id','source_archive_sha256','source_base_commit'])
assert close['instance_id']==launch['instance_id'] and close['state']=='terminated'
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3')
assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    ident=terminal['artifacts'][name];path=out/(name+'.gz')
    body=gzip.decompress(path.read_bytes()) if path.exists() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read()
    assert len(body)==ident['bytes'] and sha(body)==ident['sha256'];return body
def obj(name):return json.loads(get(name))
for name in terminal['artifacts']:get(name)
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read();assert sha(archive)==launch['source_archive_sha256']
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:frozen={m.name:tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()}
config_name=str(root/'four-slot-1m-offered-http-config.json');assert Path(config_name).read_bytes()==frozen[config_name] and sha(frozen[config_name])==reservation['config_sha256'];config=json.loads(frozen[config_name])
for name,digest in config['dependencies'].items():assert sha(frozen[name])==digest and Path(name).read_bytes()==frozen[name],name
for name in ['aws-four-slot-1m-offered-http.py','four-slot-1m-offered-http-preregister.md']:
    path=root/name;assert path.read_bytes()==frozen[str(path)]
proofs={}
for folder,ident in config['authorities'].items():
    for name,digest in ident.items():assert sha((root/folder/name).read_bytes())==digest==sha(frozen[str(root/folder/name)])
    proofs[folder]=load(root/folder/'verification.json');assert proofs[folder]['state']=='terminated' and (proofs[folder].get('valid_check') or proofs[folder].get('valid_measurement'))
expected=dict(proofs['source-completion-integration/a0002']['compiled_native_sha256']);expected['crates/borsuk/examples/two_bit_http.rs']=proofs['two-slot-1m-offered-http/a0001']['compiled_http_sha256'];expected['crates/borsuk/src/bin/two_bit_plan_demo.rs']=proofs['native-reference-panel/a0002']['compiled_source_sha256']
assert len(expected)==395 and reservation['changed_native_files']==['crates/borsuk/examples/two_bit_http.rs']
http_path='crates/borsuk/examples/two_bit_http.rs';original=frozen[http_path];fixed=original.replace(b'const QUERY_SLOTS: usize = 2;',b'const QUERY_SLOTS: usize = 4;');assert original.count(b'const QUERY_SLOTS: usize = 2;')==1 and sha(fixed)==reservation['expected_compiled_http_sha256']
expected[http_path]=sha(original);assert expected==reservation['compiled_native_sha256']
for name,digest in expected.items():
    assert sha(frozen[name])==digest,name
    assert Path(name).read_bytes() in ([original,fixed] if name==http_path else [frozen[name]]),name
checker='scripts/check_http_four_slot_authority.py';assert Path(checker).read_bytes()==frozen[checker]
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=sha(raw),native_files_matched=395,artifacts_verified=len(terminal['artifacts']),full_library_assurance_reused=2696,offered_protocol_checks_reused=5,qualification=False,fresh_cohort_used=False,matched_control_http_measured=False,matched_vendor_measured=False,compute_cost_estimate_usd=close['compute_cost_estimate_usd'],compute_cost_status=close['cost_status'])
if terminal['status']!='complete' or terminal['exit_code']!=0 or 'screen/decision.json' not in terminal['artifacts']:
    report.update(valid_measurement=False,engineering_invalid=True,phase=terminal['phase'],exit_code=terminal['exit_code']);(out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
reuse=obj('reuse.json');assert reuse['authorities']==config['authorities'] and reuse['full_library_tests_reused']==2696 and reuse['offered_protocol_checks_reused']==5 and reuse['actual_native_references_reused']
boundary=obj('boundary-check.json');assert boundary['qualified'] and boundary['query_slots']==4 and boundary['red_status']==101 and boundary['green_status']==boundary['release_status']==0 and boundary['library_assurance_reused']==2696 and not boundary['full_workspace_repeated']
assert boundary['original_http_sha256']==sha(original) and boundary['compiled_http_sha256']==sha(fixed)==sha(get('http.fixed.rs')) and boundary['binary_sha256']==sha(get('binaries/two_bit_http'))
red=get('red.log').decode();green=get('green.log').decode();assert 'left: 2' in red and 'right: 4' in red and 'could not compile' not in red and 'test result: ok. 2 passed; 0 failed;' in green
compiler=obj('boundary-cgroup.json');assert int(compiler['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in compiler['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill'])
prior=proofs['current-1m-offered-http/a0001'];assert prior['valid_measurement'] and prior['decision']=='FAIL current1M consumed development offered8QPS' and prior['native_quality']['10']['hits']==635
prior_terminal=load(root/'current-1m-offered-http/a0001/aws-terminal.json');prior_launch=load(root/'current-1m-offered-http/a0001/aws-launch.json')
for role in ['reference-k10.jsonl','reference-k100.jsonl','native-quality.json']:
    assert config['artifacts'][role]==dict(key=prior_launch['prefix']+'/artifacts/screen/'+role,**prior_terminal['artifacts']['screen/'+role])
assert config['closed_index_prefix']==prior_launch['prefix']+'/indexes/relaion' and config['query_slots']==4
def input_body(role):
    ident=config['artifacts'][role];body=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read();assert len(body)==ident['bytes'] and sha(body)==ident['sha256'];return body
closed=json.loads(input_body('closed-quality.json'));gt=struct.unpack('<6400I',input_body('truth.u32'));truth=[gt[q*100:(q+1)*100] for q in range(64)]
original=input_body('original-requests').splitlines(keepends=True);assert len(original)==1000 and get('screen/requests64.jsonl')==b''.join(original[:64]);requests=[json.loads(x) for x in original[:64]]
assert closed['decision']=='KILL 1M consumed development quality' and closed['returned_hits']==dict(control=6288,candidate=6318) and closed['flat_hits']==6363
assert closed['root_sha256']['candidate']==config['root_sha256']==config['artifacts']['generation/manifest.json']['sha256']
authority=dict(root_sha256=config['root_sha256'],generation=1,control_epoch=1)
refs={};quality={}
for k in [100,10]:
    records=[json.loads(x) for x in get('screen/reference-k'+str(k)+'.jsonl').splitlines()];assert len(records)==66
    assert all(records[0][key]==value for key,value in authority.items()) and records[0]['top_k']==k and records[0]['declared_panel_count']==64 and records[-1]['count']==64 and records[-1]['top_k']==k
    refs[k]=records[1:-1];plan=''.join(json.dumps({key:r[key] for key in ['query_ordinal','ranges','planned_bytes']},sort_keys=True,separators=(',',':'))+'\n' for r in refs[k]);assert sha(plan.encode())==closed['plans_sha256']['candidate']
    for q,row in enumerate(refs[k]):
        assert row['query_ordinal']==q and row['ids']==closed['expected_ids']['candidate'][q][:k] and len(set(row['ids']))==k
        assert 0<len(row['ranges'])==row['submitted_gets']<=32 and row['planned_bytes']==row['verified_bytes']<=16773120 and row['failed_gets']==0
        assert all(0<=a<b<=780000000 and a%780==b%780==0 for a,b in row['ranges']) and all(x[1]<y[0] for x,y in zip(row['ranges'],row['ranges'][1:])) and sum(b-a for a,b in row['ranges'])==row['planned_bytes']
    hits=sum(len(set(row['ids'])&set(truth[q][:k])) for q,row in enumerate(refs[k]));quality[str(k)]=dict(hits=hits,denominator=64*k,recall=hits/(64*k),actual_native_serving=True,offline_ordered_id_plan_parity=True)
assert quality['100']['hits']==6318
native=obj('screen/native-quality.json');assert native['native_quality']==quality and native['requests64_sha256']==sha(get('screen/requests64.jsonl')) and not native['fresh_cohort_used'] and not native['qualification']
assert native['original_strict_diagnostic_preserved']==closed['decision']
def near(a,b):assert math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-8),(a,b)
def quantile(values,p):
    values=sorted(values);i=(len(values)-1)*p;lo=math.floor(i);hi=math.ceil(i);return values[lo]+(values[hi]-values[lo])*(i-lo)
def tails(values):return {name:quantile(values,p) if values else None for name,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]}
decision=obj('screen/decision.json');runs=decision['runs'];assert 1<=len(runs)<=4 and decision['query_slots']==4 and decision['actual_native_references_reused']
summary=[];failures=Counter()
for rep,r in enumerate(runs):
    k=config['setting_order'][rep];base='screen/run'+str(rep)+'-k'+str(k)+'/'
    assert r==obj(base+'result.json') and r['rep']==rep and r['k']==k and r['authority']==authority and r['offered_count']==64 and r['offered_qps']==8 and r['workers']==8 and r['timeout_seconds']==5
    assert not r['fresh_cohort_used'] and not r['matched_control_http_measured'] and not r['server_admission_measured'] and r['metadata_resident'] and not r['application_sq8_cache']
    samples=[json.loads(x) for x in get(base+'http.jsonl').splitlines()];assert len(samples)==64;epoch=samples[0]['scheduled_ns'];success=[]
    for q,s in enumerate(samples):
        assert s['query_ordinal']==q and s['scheduled_ns']==epoch+q*125000000 and s['dispatched_ns']>=s['scheduled_ns'] and s['completed_ns']>=s['dispatched_ns'] and s['server_admission_ns'] is None and s['integrity_error'] is None
        if s['started_ns'] is not None:
            assert s['dispatched_ns']<=s['started_ns']<=s['completed_ns'];body=json.dumps(dict(query=requests[q]['query'],k=k,**authority),separators=(',',':'),allow_nan=False).encode();assert s['request_sha256']==sha(body) and s['request_bytes']==len(body)
        if s['outcome']=='success':
            assert s['status']==200 and s['physical_counters_complete'];response=s['response'];assert response['authority']==authority and all(response[key]==refs[k][q][key] for key in ['ids','ranges','planned_bytes','submitted_gets','verified_bytes','failed_gets']);assert s['returned_hits']==len(set(response['ids'])&set(truth[q][:k]));success.append(s)
        elif s['outcome']=='rejected_503':assert s['status']==503 and s['physical_counters_complete']
        elif s['outcome']=='client_capacity_drop':assert s['status'] is None and s['started_ns'] is None and s['physical_counters_complete']
        else:assert s['outcome'] in ['timeout','transport_error','http_error'] and not s['physical_counters_complete']
    outcomes=dict(Counter(s['outcome'] for s in samples));assert outcomes==r['outcomes'] and r['successful_count']==len(success) and r['identity_parity_valid']
    elapsed=max(8000000000,max(s['completed_ns'] for s in samples)-epoch);assert r['scheduled_duration_ns']==8000000000 and r['elapsed_including_drain_ns']==elapsed;near(r['achieved_successful_qps'],len(success)*1e9/elapsed)
    hits=sum(s['returned_hits'] for s in success);near(r['mean_offered_recall'],hits/(64*k))
    if success:near(r['mean_successful_recall'],hits/(len(success)*k))
    else:assert r['mean_successful_recall'] is None
    assert r['successful_completions_in_window']==sum(s['completed_ns']<epoch+8000000000 for s in success)
    for field,values in [('successful_incoming_http_ms',[(s['completed_ns']-s['started_ns'])/1e6 for s in success]),('successful_scheduled_to_completion_ms',[(s['completed_ns']-s['scheduled_ns'])/1e6 for s in success]),('all_offered_terminal_ms',[(s['completed_ns']-s['scheduled_ns'])/1e6 for s in samples]),('dispatch_lag_ms',[(s['dispatched_ns']-s['scheduled_ns'])/1e6 for s in samples]),('client_queue_delay_ms',[(s['started_ns']-s['dispatched_ns'])/1e6 for s in samples if s['started_ns'] is not None])]:
        for label,value in tails(values).items():
            if value is None:assert r[field][label] is None
            else:near(r[field][label],value)
    for field,key in [('known_submitted_gets','submitted_gets'),('known_verified_bytes','verified_bytes'),('known_failed_gets','failed_gets')]:assert r[field]==sum(s.get('response',{}).get(key) or 0 for s in samples)
    assert r['physical_counters_complete']==all(s['physical_counters_complete'] for s in samples)
    gate=len(success)==64 and r['mean_offered_recall']>=.95 and r['successful_incoming_http_ms']['p90']<444 and r['achieved_successful_qps']>=8;assert r['development_gate_passed']==gate
    if k==10 and not gate:assert rep==len(runs)-1
    stop=obj(base+'server-closeout.json');assert stop['intentional_stop'] and stop['returncode']!=0
    rss=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',get(base+'server.time').decode()).group(1))
    summary.append(dict(rep=rep,k=k,successful_count=len(success),outcomes=outcomes,mean_offered_recall=r['mean_offered_recall'],mean_successful_recall=r['mean_successful_recall'],incoming_http_ms=r['successful_incoming_http_ms'],all_offered_terminal_ms=r['all_offered_terminal_ms'],successful_qps=r['achieved_successful_qps'],process_max_rss_kib=rss,known_gets=r['known_submitted_gets'],known_verified_bytes=r['known_verified_bytes'],physical_counters_complete=r['physical_counters_complete'],development_gate_passed=gate))
    if k==10:failures.update({o:n for o,n in outcomes.items() if o!='success'})
k10=[r for r in runs if r['k']==10];passed=len(k10)==2 and all(r['development_gate_passed'] for r in k10)
assert decision['decision']==('GO current1M consumed development offered8QPS only' if passed else 'FAIL current1M consumed development offered8QPS') and decision['native_quality']==quality and decision['failure_outcomes']==dict(failures)
assert decision['original_strict_diagnostic_preserved']==closed['decision'] and not any(decision[k] for k in ['fresh_cohort_used','qualification','matched_control_http_measured','matched_vendor_measured','lifecycle_cost_measured'])
assert decision['all_physical_query_counters_complete']==all(r['physical_counters_complete'] for r in runs)
resource=obj('screen/cgroup.json');cg=resource['cgroup'];assert int(cg['memory.max'])==8589934592 and int(cg['memory.swap.max'])==int(cg['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cg['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill'])
assert resource['address_space_limit']==[4294967296,4294967296] and resource['cpu_affinity']==[0,1,2,3]
report.update(valid_measurement=True,engineering_invalid=False,decision=decision['decision'],native_quality=quality,runs=summary,original_strict_diagnostic_preserved=closed['decision'],cgroup=resource,matched_offline_recall100_control=6288/6400,matched_offline_flat_sq8_recall100=6363/6400,compiled_http_sha256=sha(fixed),binary_sha256=boundary['binary_sha256'],focused_native_tests_passed=2,query_slots=4,actual_native_references_reused=True,compiler_cgroup=compiler)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k not in ['cgroup','compiler_cgroup']}))
