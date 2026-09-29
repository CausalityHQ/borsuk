"""Closed layout receipts/source/GT-set verification; no query/scoring kernel."""
import gzip,hashlib,io,json,math,struct,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1];launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0] and all(terminal[k]==launch[k] for k in ['source_archive_sha256','source_base_commit','instance_id']) and close['state']=='terminated' and close['instance_id']==launch['instance_id']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3');assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    p=out/(name+'.gz');b=gzip.decompress(p.read_bytes()) if p.exists() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read();ident=terminal['artifacts'][name];assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
def obj(name):return json.loads(get(name))
for name in terminal['artifacts']:get(name)
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read();assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256'];matched=0;native=0
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name);is_native=path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']
        if is_native or member.name in [str(root/x) for x in ['layout-config.json','layout-preregister.md']]+['scripts/run_native_union_layout_transfer.py']:
            assert path.read_bytes()==tar.extractfile(member).read(),member.name;matched+=1;native+=is_native
assert native==392
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=native,source_files_matched=matched,artifacts_verified=len(terminal['artifacts']),qualification=False)
if terminal['status']!='complete' or terminal['exit_code']!=0:
    report.update(valid_measurement=False,phase=terminal['phase'],exit_code=terminal['exit_code']);(out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
config=json.loads((root/'layout-config.json').read_text());assert hashlib.sha256((root/'layout-config.json').read_bytes()).hexdigest()==reservation['config_sha256'];helper=obj('helper.json');assert helper['source_archive_sha256']==launch['source_archive_sha256'] and helper['sha256']==terminal['artifacts']['binaries/build_sq8_source']['sha256'] and helper['bytes']==terminal['artifacts']['binaries/build_sq8_source']['bytes'];assert 'Finished `release` profile' in get('compile.log').decode()
decision=obj('screen/decision.json');rows=[]
def quantile(v,p):
    v=sorted(v);i=(len(v)-1)*p;lo=math.floor(i);hi=math.ceil(i);return v[lo]+(v[hi]-v[lo])*(i-lo)
def near(a,b):assert math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-8),(a,b)
for item in config['items'][:len(decision['results'])]:
    base='screen/'+item['name']+'/';result=obj(base+'result.json');binding=obj(base+'binding.json');assert binding['control_root_sha256']==item['control_root_sha256'] and binding['per_id_sq8_and_two_bit_payload_exact'] and binding['coefficient_f32_bits_exact']
    records=[json.loads(s) for s in get(base+'paired.jsonl').splitlines()];assert len(records)==128;plans={a:[] for a in ['control','candidate']}
    for q in range(64):
        for r in records[q*2:q*2+2]:
            a=r['arm'];assert r['query_ordinal']==q and r['root_sha256']==binding[a+'_root_sha256'];plans[a].append({k:r[k] for k in ['query_ordinal','ranges','planned_bytes']});sample=result['samples'][a][q]
            for stage,field in [('candidate','candidate_hits'),('nominated','nominated_hits'),('fetched','fetched_hits'),('returned','returned_hits'),('flat','flat_hits')]:assert len(set(sample['stages'][stage]))==len(sample['stages'][stage])==sample[field]
            assert set(sample['stages']['nominated'])<=set(sample['stages']['candidate']) and set(sample['stages']['nominated'])<=set(sample['stages']['fetched']) and set(sample['stages']['returned'])<=set(sample['stages']['fetched']) and sample['gets']==len(r['ranges'])<=32 and sample['bytes']==r['planned_bytes']<=16773120
    for a in plans:
        normal=''.join(json.dumps(p,sort_keys=True,separators=(',',':'))+'\n' for p in plans[a]);assert hashlib.sha256(normal.encode()).hexdigest()==result['normal_plans_sha256'][a];assert result['returned_hits'][a]==sum(s['returned_hits'] for s in result['samples'][a]) and result['p05'][a]==sorted(s['returned_hits'] for s in result['samples'][a])[3]
    assert result['normal_plans_sha256']['control']==item['expected_control_plans_sha256'] and result['returned_hits']['control']==item['control_returned_hits'] and result['new_exhaustive_sq8_kernel_calls']==0 and result['cached_sort_queries']==128
    if result['physical_cold_measured']:
        ident=item['artifacts']['truth'];truth=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read();assert len(truth)==ident['bytes'] and hashlib.sha256(truth).hexdigest()==ident['sha256'];gt=struct.unpack('<100000I',truth)
        for run in result['runs']:
            a=run['arm'];path=base+'run'+str(run['rep'])+'-'+a+'/';live=[json.loads(s) for s in get(path+'live.jsonl').splitlines()];assert len(live)==66 and live[0]['root_sha256']==binding[a+'_root_sha256'] and live[0]['generation']==1 and live[0]['control_epoch']==1 and live[-1]['count']==64
            for q,r in enumerate(live[1:-1]):
                assert {k:r[k] for k in ['query_ordinal','ranges','planned_bytes']}==plans[a][q] and r['submitted_gets']==len(r['ranges'])<=32 and r['verified_bytes']==r['planned_bytes']<=16773120 and r['failed_gets']==0
                assert len(r['ids'])==len(set(r['ids']))==100 and set(r['ids'])&set(gt[q*100:(q+1)*100])==set(result['samples'][a][q]['stages']['returned'])
            for label,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]:near(run['complete_library_call_ms'][label],quantile([r['query_wall_ns']/1e6 for r in live[1:-1]],p))
            near(run['serial_observed_qps'],64e9/live[-1]['measurement_wall_ns'])
    rows.append(dict(dataset=item['name'],returned_hits=result['returned_hits'],p05=result['p05'],decision=result['decision'],physical_cold_measured=result['physical_cold_measured'],arm_median_complete_call_ms=result.get('arm_median_complete_call_ms')))
cgroup=obj('screen/cgroup.json');assert int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill']);report.update(valid_measurement=True,rows=rows,cgroup=cgroup,helper=helper,decision=decision['decision'],fresh_cohort_used=False)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
