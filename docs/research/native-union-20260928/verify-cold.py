"""Verify closed source/receipts and reduce recorded counters/GT sets; no scoring."""
import gzip,hashlib,io,json,math,statistics,struct,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1]
launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert terminal['source_archive_sha256']==launch['source_archive_sha256'] and terminal['source_base_commit']==launch['source_base_commit']
assert close['state']=='terminated' and close['instance_id']==terminal['instance_id']==launch['instance_id']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3')
assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    b=gzip.decompress((out/(name+'.gz')).read_bytes());ident=terminal['artifacts'][name]
    assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
def obj(name):return json.loads(get(name))
for name in terminal['artifacts']:get(name)
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read();assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256']
matched=[];native=0
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name);is_native=path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']
        if is_native or member.name in [str(root/x) for x in ['cold-config.json','cold-preregister.md']]+['scripts/run_native_union_cold.py']:
            assert path.read_bytes()==tar.extractfile(member).read(),member.name;matched.append(member.name);native+=is_native
assert native==392
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=native,source_files_matched=len(matched),artifacts_verified=len(terminal['artifacts']),qualification=False)
if terminal['status']!='complete' or terminal['exit_code']!=0:
    report.update(valid_measurement=False,phase=terminal['phase'],exit_code=terminal['exit_code'])
    (out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
config=json.loads((root/'cold-config.json').read_text());assert hashlib.sha256((root/'cold-config.json').read_bytes()).hexdigest()==reservation['config_sha256']
reuse=obj('reuse.json');assert reuse['native_files_matched']==392 and reuse['no_compile_or_test_rerun'] and reuse['source_archive_sha256']==reservation['reused_native_source_archive_sha256'] and reuse['terminal_sha256']==reservation['reused_terminal_sha256']
decision=obj('screen/decision.json');rows=[]
def quantile(values,p):
    v=sorted(values);i=(len(v)-1)*p;lo=math.floor(i);hi=math.ceil(i);return v[lo]+(v[hi]-v[lo])*(i-lo)
def near(a,b):assert math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-8),(a,b)
for item in config['items'][:len(decision['results'])]:
    name=item['name'];result=obj('screen/'+name+'/result.json');paired=[json.loads(s) for s in get('screen/'+name+'/native-paired.jsonl').splitlines()];binding=obj('screen/'+name+'/binding.json');plans={arm:[] for arm in ['control','candidate']}
    assert len(paired)==128 and result['native_library_search_calls']==256 and not result['incoming_service_http_measured']
    for q in range(64):
        for r in paired[q*2:q*2+2]:
            assert r['query_ordinal']==q;plans[r['arm']].append({k:r[k] for k in ['query_ordinal','ranges','planned_bytes']})
    for arm in plans:
        digest=hashlib.sha256(''.join(json.dumps(p,sort_keys=True,separators=(',',':'))+'\n' for p in plans[arm]).encode()).hexdigest();assert digest==item['expected_'+('control' if arm=='control' else 'union')+'_plans_sha256']
        body=get('screen/'+name+'/native-'+arm+'/manifest.json');assert hashlib.sha256(body).hexdigest()==binding[arm+'_root_sha256'];assert json.loads(body)['schema']=='borsuk-two-bit-generation-v4'
    ident=item['artifacts']['truth'];gt=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read();assert len(gt)==ident['bytes'] and hashlib.sha256(gt).hexdigest()==ident['sha256'];truth=struct.unpack('<100000I',gt)
    recorded_ids={};runs=[]
    for rep,arm in enumerate(config['arm_order']):
        path='screen/'+name+'/run'+str(rep)+'-'+arm;records=[json.loads(s) for s in get(path+'/live.jsonl').splitlines()];run=obj(path+'/result.json');assert len(records)==66
        start,summary=records[0],records[-1];assert start['root_sha256']==binding[arm+'_root_sha256'] and start['generation']==1 and start['control_epoch']==1 and summary['count']==64
        hits=[]
        for q,r in enumerate(records[1:-1]):
            assert {k:r[k] for k in ['query_ordinal','ranges','planned_bytes']}==plans[arm][q]
            assert len(r['ids'])==len(set(r['ids']))==100 and r['submitted_gets']==len(r['ranges'])<=32 and r['verified_bytes']==r['planned_bytes']<=16773120 and r['failed_gets']==0
            if (arm,q) in recorded_ids:assert recorded_ids[(arm,q)]==r['ids']
            recorded_ids[(arm,q)]=r['ids'];hits.append(len(set(r['ids'])&set(truth[q*100:(q+1)*100])))
        index=0 if arm=='control' else 1;assert sum(hits)==item['expected_returned_hits'][index]==run['returned_hits'];assert sorted(hits)[3]==item['expected_p05_hits'][index]
        for field,key in [('complete_library_call_latency_ms','query_wall_ns'),('process_cpu_ms','query_process_cpu_ns')]:
            for label,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]:near(run[field][label],quantile([r[key]/1e6 for r in records[1:-1]],p))
        near(run['serial_observed_qps'],64e9/summary['measurement_wall_ns']);assert run['data_get_attempts']==sum(r['submitted_gets'] for r in records[1:-1]) and run['verified_data_bytes']==sum(r['verified_bytes'] for r in records[1:-1])
        runs.append(run)
    medians={arm:{p:statistics.median([r['complete_library_call_latency_ms'][p] for r in runs if r['arm']==arm]) for p in ['p50','p90','p95','p99']} for arm in plans}
    for arm in plans:
        for p in medians[arm]:near(medians[arm][p],result['arm_median_complete_call_ms'][arm][p])
    assert result['decision'].startswith('GO')==(medians['candidate']['p90']<=250 and medians['candidate']['p95']<=400)
    rows.append(dict(dataset=name,split=result['split'],returned_hits=item['expected_returned_hits'],arm_median_complete_call_ms=medians,arm_median_serial_qps={arm:statistics.median([r['serial_observed_qps'] for r in runs if r['arm']==arm]) for arm in plans},decision=result['decision']))
cgroup=obj('screen/cgroup.json');assert int(cgroup['memory.swap.peak'])==0 and all(int(line.split()[1])==0 for line in cgroup['memory.events'].splitlines() if line.split()[0] in ['oom','oom_kill'])
report.update(valid_measurement=True,rows=rows,cgroup=cgroup,decision=decision['decision'],native_search_calls=256*len(rows))
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
