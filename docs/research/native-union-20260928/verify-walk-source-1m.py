"""Independent CLOSED first1M source/GT-set/HTTP metadata checks, no ANN kernels."""
import gzip,hashlib,io,json,math,re,struct,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1];launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert terminal['schema']=='borsuk-native-walk-source-1m-dev-v1' and hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert all(terminal[k]==launch[k] for k in ['source_archive_sha256','source_base_commit','instance_id']) and close['state']=='terminated' and close['instance_id']==launch['instance_id']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3');assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    p=out/(name+'.gz');data=gzip.decompress(p.read_bytes()) if p.exists() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read();ident=terminal['artifacts'][name];assert len(data)==ident['bytes'] and hashlib.sha256(data).hexdigest()==ident['sha256'];return data
def obj(name):return json.loads(get(name))
for name,ident in terminal['artifacts'].items():
    if (out/(name+'.gz')).exists():get(name)
    else:
        stream=s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'];digest=hashlib.sha256();size=0
        for chunk in iter(lambda:stream.read(4<<20),b''):digest.update(chunk);size+=len(chunk)
        assert size==ident['bytes'] and digest.hexdigest()==ident['sha256']
config_path=root/'walk-source-1m-config.json';config=json.loads(config_path.read_text());assert hashlib.sha256(config_path.read_bytes()).hexdigest()==reservation['config_sha256']
for name,digest in config['dependencies'].items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read();assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256'];native=matched=0
names=[str(root/x) for x in ['walk-source-1m-config.json','walk-source-1m-preregister.md']]+['scripts/run_native_walk_source_1m.py']+list(config['dependencies'])
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name);is_native=path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']
        if is_native or member.name in names:assert path.read_bytes()==tar.extractfile(member).read(),member.name;native+=is_native;matched+=1
assert native==394 and reservation['new_native_files_matched']==394
proof=json.loads((root/'walk-source-integration/a0001/verification.json').read_text());assert proof['valid_check'] and proof['full_assurance']==dict(passed=2693,failed=0,ignored=26,targets=145) and proof['cargo_executed_targets']==146
assert hashlib.sha256(Path('crates/borsuk/src/two_bit_generation.rs').read_bytes()).hexdigest()==proof['compiled_source_sha256']==reservation['replay_compiled_source_sha256']
assert hashlib.sha256((root/'walk-source-integration/a0001/aws-terminal.json').read_bytes()).hexdigest()==proof['terminal_sha256']==reservation['replay_authority_terminal_sha256']
previous=json.loads((root/'walk-source-http/a0002/verification.json').read_text());assert previous['valid_measurement'] and previous['terminal_sha256']==reservation['predecessor_http_terminal_sha256']
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=native,source_files_matched=matched,artifacts_verified=len(terminal['artifacts']),qualification=False,fresh_cohort_used=False)
if terminal['status']!='complete' or terminal['exit_code']!=0 or 'screen/decision.json' not in terminal['artifacts']:
    report.update(valid_measurement=False,reason='Incomplete/failed terminal',phase=terminal['phase'],exit_code=terminal['exit_code']);(out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
assert obj('screen/self-check.json')['passed']
base='screen/relaion/';decision=obj('screen/decision.json');result=obj(base+'result.json');assert decision['results']==[result] and decision['decision']==result['decision'] and decision['scale_run'] and not decision['qualification'] and not decision['fresh_cohort_used']
binding=obj(base+'binding.json');recipe=binding['source_recipe'];root_manifest=obj(base+'generation/manifest.json');root_sha=hashlib.sha256(get(base+'generation/manifest.json')).hexdigest();assert root_sha==binding['root_sha256']==result['root_sha256']
assert root_manifest['schema']=='borsuk-two-bit-generation-v4' and recipe['rows']==1000000 and recipe['recipe']=='borsuk-hierarchical-extents-chacha8-v3' and recipe['source_cell_order']=='nearest-unvisited-layer0-entry-ordinal-fallback-v1' and recipe['query_or_truth_used'] is False and recipe['source_cell_target_rows']==256 and recipe['sampling_cell_target_rows']==1024 and recipe['samples_per_sampling_cell']==64
assert binding['raw_sha256']==config['raw_sha256'] and binding['normalized_sha256']==config['normalized_sha256'] and binding['same_root_source_scorer_control'] and not binding['query_or_truth_used_for_construction']
for name,field in [('page_manifest.json','page_manifest_sha256'),('plane/manifest.json','plane_manifest_sha256'),('centroids.bin','centroids_sha256'),('graph.bin','graph_sha256'),('diverse_graph.bin','diverse_graph_sha256')]:assert terminal['artifacts'][base+'generation/'+name]['sha256']==root_manifest[field]
page=obj(base+'generation/page_manifest.json');assert page['rows']==1000000 and page['dimensions']==768 and page['object_sha256']==root_manifest['sq8_object_sha256']
head=s3.head_object(Bucket=config['bucket'],Key=root_manifest['sq8_object_key']);assert head['ETag']==root_manifest['sq8_etag'] and head['ContentLength']==780000000 and head['Metadata']['sha256']==root_manifest['sq8_object_sha256']
stream=s3.get_object(Bucket=config['bucket'],Key=root_manifest['sq8_object_key'])['Body'];digest=hashlib.sha256();size=0
for chunk in iter(lambda:stream.read(4<<20),b''):digest.update(chunk);size+=len(chunk)
assert size==780000000 and digest.hexdigest()==root_manifest['sq8_object_sha256']
order_bytes=get(base+'order.u64');assert len(order_bytes)==8000000 and hashlib.sha256(order_bytes).hexdigest()==binding['source_order_sha256']==recipe['order_sha256'];order=struct.unpack('<1000000Q',order_bytes);assert len(set(order))==1000000 and min(order)==0 and max(order)==999999
positions={i:p for p,i in enumerate(order)};truth_bytes=get(base+'truth.u32');assert len(truth_bytes)==25600;truth=struct.unpack('<6400I',truth_bytes);oracle=obj(base+'oracle.json');assert oracle['truth_sha256']==hashlib.sha256(truth_bytes).hexdigest() and oracle['dtype']=='float64' and oracle['queries']==64 and oracle['rows']==1000000 and oracle['exact_block_sort_merge']
assert result['rows']==1000000 and result['split']=='consumed external development0-63' and not result['fresh_cohort_used'] and not result['qualification'] and len(result['scores_by_id_sha256'])==64
plans={a:[json.loads(s) for s in get(base+'plan-'+a+'.jsonl').splitlines()] for a in ['control','candidate']};decomposition={}
for arm,records in plans.items():
    assert len(records)==64 and len(result['samples'][arm])==64 and len(result['expected_ids'][arm])==64;loss={k:0 for k in ['gt_walk_pool','gt_roster','gt_nomination','gt_physical','gt_ranking','flat_discovery','flat_nomination','flat_physical','flat_ranking']};gets=bytes_=calls=0
    for q,r in enumerate(records):
        assert r['query_ordinal']==q;gt=set(truth[q*100:(q+1)*100]);assert len(gt)==100 and all(i<1000000 for i in gt)
        ranges=r['ranges'];assert 0<len(ranges)<=32 and all(0<=a<b<=780000000 and a%780==b%780==0 for a,b in ranges) and all(x[1]<y[0] for x,y in zip(ranges,ranges[1:])) and sum(b-a for a,b in ranges)==r['planned_bytes']<=16773120
        assert len(r['discoveries'])==2;units=set()
        for discovery in r['discoveries']:
            evaluated=discovery['walk_evaluated_units'];assert 0<len(evaluated)==len(set(evaluated))<=1272 and all(0<=u<31250 for u in evaluated)
            assert not discovery['walk_work_exhausted'] or len(evaluated)==1272
            assert set(range(discovery['seed_page']*8,min((discovery['seed_page']+1)*8,31250)))<=set(evaluated);units.update(evaluated)
        assert len(units)*32<=81408;calls+=len(units)*32
        sample=result['samples'][arm][q];stage={k:set(v) for k,v in sample['stages'].items()};assert all(len(v)==len(stage[k]) and stage[k]<=gt for k,v in sample['stages'].items())
        expected_d={i for i in gt if positions[i]//256 in r['ranked_candidate_pages']};expected_n={i for i in gt if positions[i]//256 in r['selected_pages']};expected_f={i for i in gt if any(a//780<=positions[i]<b//780 for a,b in ranges)}
        assert stage['candidate']==expected_d and stage['nominated']==expected_n and stage['fetched']==expected_f and stage['returned']==gt&set(result['expected_ids'][arm][q]) and sample['returned_hits']==len(stage['returned'])
        assert sample['gets']==len(ranges) and sample['bytes']==r['planned_bytes'];gets+=len(ranges);bytes_+=r['planned_bytes'];walk_gt={i for i in gt if positions[i]//32 in units}
        for key,value in [('gt_walk_pool',len(gt-walk_gt)),('gt_roster',len(walk_gt-stage['candidate'])),('gt_nomination',len(stage['candidate']-stage['nominated'])),('gt_physical',len(stage['nominated']-stage['fetched'])),('gt_ranking',len(stage['fetched']-stage['returned'])),('flat_discovery',len(stage['flat']-stage['candidate'])),('flat_nomination',len((stage['flat']&stage['candidate'])-stage['nominated'])),('flat_physical',len((stage['flat']&stage['nominated'])-stage['fetched'])),('flat_ranking',len((stage['flat']&stage['fetched'])-stage['returned']))]:loss[key]+=value
    normal=''.join(json.dumps({k:r[k] for k in ['query_ordinal','ranges','planned_bytes']},sort_keys=True,separators=(',',':'))+'\n' for r in records)
    assert hashlib.sha256(normal.encode()).hexdigest()==result['plans_sha256'][arm]
    assert result['returned_hits'][arm]==sum(s['returned_hits'] for s in result['samples'][arm]) and result['p05'][arm]==sorted(s['returned_hits'] for s in result['samples'][arm])[3]
    decomposition[arm]=dict(losses=loss,planned_gets=gets,planned_bytes=bytes_,source_row_evaluations=calls)
assert result['flat_hits']==sum(len(s['stages']['flat']) for s in result['samples']['control'])==sum(len(s['stages']['flat']) for s in result['samples']['candidate'])
passed=result['returned_hits']['candidate']/64>=98 and result['p05']['candidate']>=95 and result['flat_hits']-result['returned_hits']['candidate']<=32 and result['returned_hits']['candidate']>=result['returned_hits']['control']
def near(a,b):assert math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-8),(a,b)
def quantile(values,p):
    values=sorted(values);i=(len(values)-1)*p;lo=math.floor(i);hi=math.ceil(i);return values[lo]+(values[hi]-values[lo])*(i-lo)
if not passed:assert result['decision']=='KILL 1M consumed development quality' and not result['physical_cold_measured']
if result['physical_cold_measured']:
    assert passed and [(r['rep'],r['arm']) for r in result['runs']]==list(enumerate(config['arm_order']));refs={a:[json.loads(s) for s in get(base+'native-'+a+'/live.jsonl').splitlines()] for a in plans}
    for arm,records in refs.items():
        assert len(records)==66 and records[0]['root_sha256']==root_sha and records[0]['generation']==records[0]['control_epoch']==1 and records[-1]['count']==64
        for q,r in enumerate(records[1:-1]):assert r['ids']==result['expected_ids'][arm][q] and all(r[k]==plans[arm][q][k] for k in ['query_ordinal','ranges','planned_bytes']) and r['submitted_gets']==len(r['ranges'])<=32 and r['verified_bytes']==r['planned_bytes']<=16773120 and r['failed_gets']==0
    for run in result['runs']:
        arm=run['arm'];path=base+'run'+str(run['rep'])+'-'+arm+'/';assert run==obj(path+'result.json');live=[json.loads(s) for s in get(path+'http.jsonl').splitlines()];assert len(live)==64 and run['count']==64 and run['authority']==dict(root_sha256=root_sha,generation=1,control_epoch=1)
        assert obj(path+'boundary.json')==dict(root=409,generation=409,epoch=409,zero=400,unknown=422,body_cap=413,pressure=503,released=400,valid_ann_probes=0)
        assert obj(path+'server-closeout.json')['intentional_stop'] and obj(path+'server-closeout.json')['returncode']!=0
        for q,r in enumerate(live):assert r['query_ordinal']==q and r['authority']==run['authority'] and all(r[k]==refs[arm][q+1][k] for k in ['ids','ranges','planned_bytes','submitted_gets','verified_bytes','failed_gets']) and r['returned_hits']==len(set(r['ids'])&set(truth[q*100:(q+1)*100]))
        assert run['returned_hits']==sum(r['returned_hits'] for r in live)==result['returned_hits'][arm] and run['p05']==sorted(r['returned_hits'] for r in live)[3]==result['p05'][arm] and run['data_get_attempts']==sum(r['submitted_gets'] for r in live) and run['verified_bytes']==sum(r['verified_bytes'] for r in live)
        for label,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]:near(run['incoming_http_ms'][label],quantile([r['http_wall_ns']/1e6 for r in live],p))
        near(run['serial_observed_qps'],64e9/run['measurement_wall_ns']);run['process_max_rss_kib']=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',get(path+'server.time').decode()).group(1))
    for arm in plans:
        for label in ['p50','p90','p95','p99']:near(result['arm_median_incoming_http_ms'][arm][label],quantile([r['incoming_http_ms'][label] for r in result['runs'] if r['arm']==arm],.5))
        near(result['arm_median_serial_qps'][arm],quantile([r['serial_observed_qps'] for r in result['runs'] if r['arm']==arm],.5))
    tails=result['arm_median_incoming_http_ms']['candidate'];assert result['decision']==('GO 1M consumed development HTTP envelope only' if tails['p90']<=250 and tails['p95']<=400 else 'KILL 1M consumed development HTTP envelope')
cgroup=obj('screen/cgroup.json');assert int(cgroup['memory.max'])==8589934592 and int(cgroup['memory.swap.max'])==int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill'])
report.update(valid_measurement=True,result=result,decomposition=decomposition,cgroup=cgroup)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k!='result'}))
