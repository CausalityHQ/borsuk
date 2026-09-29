"""Closed incoming HTTP source/identity/GT-set/counter/timing/cleanup verification."""
import gzip,hashlib,io,json,math,re,struct,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1];launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0] and terminal['schema']=='borsuk-native-http-v1'
assert all(terminal[k]==launch[k] for k in ['source_archive_sha256','source_base_commit','instance_id']) and close['state']=='terminated' and close['instance_id']==launch['instance_id']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3');assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    b=gzip.decompress((out/(name+'.gz')).read_bytes());ident=terminal['artifacts'][name];assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
def obj(name):return json.loads(get(name))
for name in terminal['artifacts']:get(name)
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read();assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256'];matched=0;native=0
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name);is_native=path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']
        if is_native or member.name in [str(root/x) for x in ['http-config.json','http-preregister.md']]+['scripts/run_native_union_http.py']:
            assert path.read_bytes()==tar.extractfile(member).read(),member.name;matched+=1;native+=is_native
assert native==393
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=native,source_files_matched=matched,artifacts_verified=len(terminal['artifacts']),qualification=False)
if terminal['status']!='complete' or terminal['exit_code']!=0:
    report.update(valid_measurement=False,phase=terminal['phase'],exit_code=terminal['exit_code']);(out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
config=json.loads((root/'http-config.json').read_text());assert hashlib.sha256((root/'http-config.json').read_bytes()).hexdigest()==reservation['config_sha256']
def remote(ident):
    b=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read();assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
def quantile(v,p):
    v=sorted(v);i=(len(v)-1)*p;lo=math.floor(i);hi=math.ceil(i);return v[lo]+(v[hi]-v[lo])*(i-lo)
def near(a,b):assert math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-8),(a,b)
decision=obj('screen/decision.json');rows=[];calls=0
for item,result in zip(config['items'],decision['results']):
    base='screen/'+item['name']+'/';assert obj(base+'result.json')==result and obj(base+'bad-head.json')==dict(returncode=1,wrong_epoch_rejected=True,ann_queries=0)
    assert 'trusted head authority mismatch' in get(base+'bad-head.log').decode()
    gt=struct.unpack('<100000I',remote(item['artifacts']['truth']));refs={a:[json.loads(s) for s in remote(item['artifacts']['reference-'+a]).splitlines()] for a in ['control','candidate']}
    assert [(r['rep'],r['arm']) for r in result['runs']]==list(enumerate(config['arm_order']))
    for run in result['runs']:
        a=run['arm'];path=base+'run'+str(run['rep'])+'-'+a+'/';authority=dict(root_sha256=item['root_sha256'][a],generation=1,control_epoch=1)
        assert run==obj(path+'result.json') and run['authority']==authority and run['count']==64
        boundary=obj(path+'boundary.json');assert boundary==dict(root=409,generation=409,epoch=409,zero=400,unknown=422,body_cap=413,pressure=503,released=400,valid_ann_probes=0)
        live=[json.loads(s) for s in get(path+'http.jsonl').splitlines()];assert len(live)==64
        startup=[json.loads(s) for s in get(path+'server.log').splitlines() if s.startswith(b'{')];assert len(startup)==1 and startup[0]['phase']=='ready' and startup[0]['authority']==authority
        cleanup=obj(path+'server-closeout.json');assert cleanup['intentional_stop'] and cleanup['returncode']!=0
        rss=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',get(path+'server.time').decode()).group(1));assert rss>0
        for q,r in enumerate(live):
            assert r['query_ordinal']==q and r['authority']==authority and all(r[k]==refs[a][q+1][k] for k in ['ids','ranges','planned_bytes','submitted_gets','verified_bytes','failed_gets'])
            assert len(r['ids'])==len(set(r['ids']))==100 and r['returned_hits']==len(set(r['ids'])&set(gt[q*100:(q+1)*100])) and r['submitted_gets']==len(r['ranges'])<=32 and r['verified_bytes']==r['planned_bytes']<=16773120 and r['failed_gets']==0
        assert run['returned_hits']==sum(r['returned_hits'] for r in live)==item['returned_hits'][a] and run['p05']==sorted(r['returned_hits'] for r in live)[3]==item['p05'][a] and run['data_get_attempts']==sum(r['submitted_gets'] for r in live) and run['verified_bytes']==sum(r['verified_bytes'] for r in live)
        for label,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]:near(run['incoming_http_ms'][label],quantile([r['http_wall_ns']/1e6 for r in live],p))
        near(run['serial_observed_qps'],64e9/run['measurement_wall_ns']);calls+=64
        run['process_max_rss_kib']=rss
    for a in ['control','candidate']:
        for label in ['p50','p90','p95','p99']:near(result['arm_median_incoming_http_ms'][a][label],quantile([r['incoming_http_ms'][label] for r in result['runs'] if r['arm']==a],.5))
        near(result['arm_median_serial_qps'][a],quantile([r['serial_observed_qps'] for r in result['runs'] if r['arm']==a],.5))
    rows.append(result)
resources=obj('screen/cgroup.json');cgroup=resources['cgroup'];assert resources['address_space_limit']==[4294967296,4294967296] and resources['cpu_affinity']==[0,1,2,3] and int(cgroup['memory.max'])==8589934592 and int(cgroup['memory.swap.max'])==int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill'])
report.update(valid_measurement=True,decision=decision['decision'],rows=rows,actual_native_http_queries=calls,resources=resources,incoming_http_measured=True,loopback=True,concurrent_qps_measured=False,fresh_cohort_used=False,matched_vendor_measured=False)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k!='rows'}))
