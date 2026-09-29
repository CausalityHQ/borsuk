"""Closed layout receipts/source/GT-set verification; no query/scoring kernel."""
import gzip,hashlib,io,json,math,struct,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1];launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert terminal['schema']=='borsuk-native-source-precision-http-v1'
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
        if is_native or member.name in [str(root/x) for x in ['source-precision-http-config.json','source-precision-http-preregister.md']]+['scripts/run_native_source_precision_http.py','scripts/run_native_union_http.py','scripts/run_native_union_cold.py','scripts/run_native_cold.py','scripts/native_two_bit_cosine_development.py','scripts/v291_two_stage_development.py']:
            assert path.read_bytes()==tar.extractfile(member).read(),member.name;matched+=1;native+=is_native
assert native==394
authority=root/'source-precision-integration/a0003';proof=json.loads((authority/'verification.json').read_text());authority_raw=(authority/'aws-terminal.json').read_bytes()
assert proof['valid_check'] and proof['full_assurance']==dict(passed=2694,failed=0,ignored=26,targets=145) and proof['cargo_executed_targets']==146
assert hashlib.sha256(authority_raw).hexdigest()==proof['terminal_sha256']==reservation['candidate_authority_terminal_sha256']
for name,digest in proof['compiled_native_sha256'].items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest,name
assert proof['compiled_generation_sha256']==reservation['candidate_compiled_generation_sha256']
assert proof['compiled_native_sha256']['crates/borsuk/src/unit_centroid_graph.rs']==reservation['candidate_compiled_graph_sha256']
control=root/'walk-source-integration/a0001';cp=json.loads((control/'verification.json').read_text())
assert cp['valid_check'] and cp['full_assurance']==dict(passed=2693,failed=0,ignored=26,targets=145) and cp['state']=='terminated'
assert hashlib.sha256((control/'aws-terminal.json').read_bytes()).hexdigest()==cp['terminal_sha256']==reservation['control_authority_terminal_sha256']
assert reservation['new_native_files_matched']==394
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=native,source_files_matched=matched,artifacts_verified=len(terminal['artifacts']),qualification=False)
missing=[name for name in ['screen/decision.json','screen/cgroup.json','screen/relaion/result.json','test-resources.txt'] if name not in terminal['artifacts']]
if missing:
    report.update(valid_measurement=False,reason='Incomplete required measurement artifacts',missing_artifacts=missing,reported_status=terminal['status'],reported_exit_code=terminal['exit_code']);(out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
if terminal['status']!='complete' or terminal['exit_code']!=0:
    report.update(valid_measurement=False,phase=terminal['phase'],exit_code=terminal['exit_code']);(out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
config=json.loads((root/'source-precision-http-config.json').read_text());assert hashlib.sha256(Path('scripts/run_native_source_precision_http.py').read_bytes()).hexdigest()==config['controller_sha256']==reservation['controller_sha256'];assert hashlib.sha256((root/'source-precision-http-config.json').read_bytes()).hexdigest()==reservation['config_sha256'];helper=obj('helper.json');assert helper['source_archive_sha256']==reservation['reused_native_source_archive_sha256'] and helper['reused_binary'] and helper['native_source_matches_compiled_transformation'] and helper['sha256']==terminal['artifacts']['binaries/build_sq8_source']['sha256'] and helper['bytes']==terminal['artifacts']['binaries/build_sq8_source']['bytes']
for name,digest in (config['scorer_hashes']|config['controller_dependencies']).items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest
decision=obj('screen/decision.json');rows=[]
assert 1<=len(decision['results'])<=2 and [r['dataset'] for r in decision['results']]==[i['name'] for i in config['items'][:len(decision['results'])]]
assert decision['decision']==(next((r['decision'] for r in decision['results'] if r['decision'].startswith('KILL')),decision['results'][-1]['decision']))
assert all(obj('screen/'+r['dataset']+'/quality.json')['decision'].startswith('GO') for r in decision['results'][:-1]) and not decision['qualification'] and not decision['fresh_cohort_used'] and not decision['scale_run']
def quantile(v,p):
    v=sorted(v);i=(len(v)-1)*p;lo=math.floor(i);hi=math.ceil(i);return v[lo]+(v[hi]-v[lo])*(i-lo)
def near(a,b):assert math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-8),(a,b)
for item in config['items'][:len(decision['results'])]:
    base='screen/'+item['name']+'/';result=obj(base+'result.json');binding=obj(base+'binding.json');assert binding['control_root_sha256']==item['control_root_sha256'] and binding['per_id_sq8_payload_exact'] and binding['source_precision_bits']==dict(control=2,candidate=3) and binding['coefficient_f32_bits_exact']
    assert result==decision['results'][len(rows)] and result['source_recipe']==binding['source_only_recipe']
    recipe=result['source_recipe'];assert recipe['recipe']==config['source_recipe']=='borsuk-hierarchical-extents-chacha8-v3' and recipe['source_cell_order']=='nearest-unvisited-layer0-entry-ordinal-fallback-v1' and recipe['source_cell_target_rows']==256 and recipe['sampling_cell_target_rows']==1024 and recipe['samples_per_sampling_cell']==64 and recipe['rows']==100000 and recipe['query_or_truth_used'] is False
    if 'closed-manifest.json' in item['artifacts']:
        ident=item['artifacts']['closed-manifest.json'];expected=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read()
        assert len(expected)==ident['bytes'] and hashlib.sha256(expected).hexdigest()==ident['sha256'] and get(base+'candidate/manifest.json')==expected
    records=[json.loads(s) for s in get(base+'actual-paired.jsonl').splitlines()];assert len(records)==128;plans={a:[] for a in ['control','candidate']}
    ident=item['artifacts']['closed-paired.jsonl'];closed=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read()
    assert len(closed)==ident['bytes'] and hashlib.sha256(closed).hexdigest()==ident['sha256'];closed=[json.loads(line) for line in closed.splitlines()];assert len(closed)==128
    current=[next(row for row in closed[q*2:q*2+2] if row['arm']=='candidate') for q in range(64)]
    actual=[next(row for row in records[q*2:q*2+2] if row['arm']=='control') for q in range(64)]
    assert all(all(r[k]==old[k] for k in ['query_ordinal','root_sha256','ranges','planned_bytes','discoveries','ranked_candidate_pages','selected_pages','primary_page']) for r,old in zip(actual,current))
    core=[json.loads(line) for line in get(base+'core-plan.jsonl').splitlines()];assert len(core)==64
    actual_candidate=[next(row for row in records[q*2:q*2+2] if row['arm']=='candidate') for q in range(64)]
    assert all(all(r[k]==new[k] for k in ['query_ordinal','ranges','planned_bytes','discoveries','ranked_candidate_pages','selected_pages','primary_page']) for r,new in zip(core,actual_candidate))
    assert binding['candidate_root_sha256'] in get(base+'core-plan.time').decode() and all(r['root_sha256']==binding['candidate_root_sha256'] for r in actual_candidate)
    assert binding['control_root_sha256']==item['control_root_sha256'] and binding['candidate_root_sha256']!=item['control_root_sha256']
    assert binding['control_root_sha256'] in get(base+'control-plan.time').decode()
    def retained_control(name):
        ident=item['artifacts']['control/'+name];body=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read()
        assert len(body)==ident['bytes'] and hashlib.sha256(body).hexdigest()==ident['sha256'];return body
    candidate_root=obj(base+'candidate/manifest.json');candidate_plane=obj(base+'candidate/plane/manifest.json');old_plane=json.loads(retained_control('plane/manifest.json'))
    assert hashlib.sha256(get(base+'candidate/manifest.json')).hexdigest()==binding['candidate_root_sha256']
    assert candidate_root['schema']=='borsuk-two-bit-generation-v5' and candidate_plane['schema']=='borsuk-rotated-three-bit-plane-v1'
    assert old_plane['record_bytes']==200 and candidate_plane['record_bytes']==296
    for field in ['rows','dimensions','seed','source_sha256','sq8_sha256','source_order_sha256','mean_sha256','query_or_truth_used']:assert candidate_plane[field]==old_plane[field]
    for name in ['centroids.bin','graph.bin','diverse_graph.bin','page_manifest.json','page_digests.bin','canonical.bin','plane/mean.bin']:assert get(base+'candidate/'+name)==retained_control(name),name
    assert hashlib.sha256(get(base+'candidate/plane/manifest.json')).hexdigest()==candidate_root['plane_manifest_sha256']
    assert hashlib.sha256(get(base+'candidate/plane/records.bin')).hexdigest()==candidate_plane['records_sha256']
    assert len(get(base+'candidate/plane/records.bin'))==100000*296
    assert hashlib.sha256(get(base+'candidate/plane/mean.bin')).hexdigest()==candidate_plane['mean_sha256']


    ident=item['artifacts']['truth'];truth=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read();assert len(truth)==ident['bytes'] and hashlib.sha256(truth).hexdigest()==ident['sha256'];gt=struct.unpack('<100000I',truth)
    ident=item['artifacts']['sq8.bin'];old_sq8=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read();assert len(old_sq8)==ident['bytes'] and hashlib.sha256(old_sq8).hexdigest()==ident['sha256']
    old_ids=[struct.unpack_from('<q',old_sq8,pos*780)[0] for pos in range(100000)];del old_sq8
    encoded_order=get(base+'order.u64');assert hashlib.sha256(encoded_order).hexdigest()==binding['source_order_sha256'];new_ids=struct.unpack('<100000Q',encoded_order)
    assert set(old_ids)==set(new_ids)==set(range(100000)) and len(set(old_ids))==len(set(new_ids))==100000
    orders={'control':old_ids,'candidate':new_ids};positions={arm:{i:pos for pos,i in enumerate(ids)} for arm,ids in orders.items()}
    discovery_work={arm:[] for arm in orders};discovery_loss={arm:dict(gt_walk_pool=0,gt_source_roster=0,flat_gt_walk_pool=0,flat_gt_source_roster=0,seed_evaluated_gt=0) for arm in orders}

    for q in range(64):
        for r in records[q*2:q*2+2]:
            a=r['arm'];assert r['query_ordinal']==q and r['root_sha256']==binding[a+'_root_sha256'];plans[a].append({k:r[k] for k in ['query_ordinal','ranges','planned_bytes']});sample=result['samples'][a][q]
            for stage,field in [('candidate','candidate_hits'),('nominated','nominated_hits'),('fetched','fetched_hits'),('returned','returned_hits'),('flat','flat_hits')]:assert len(set(sample['stages'][stage]))==len(sample['stages'][stage])==sample[field]
            query_gt=set(gt[q*100:(q+1)*100]);stages={key:set(value) for key,value in sample['stages'].items()};assert all(values<=query_gt for values in stages.values())
            expected_discovered={i for i in query_gt if positions[a][i]//256 in r['ranked_candidate_pages']};expected_nominated={i for i in query_gt if positions[a][i]//256 in r['selected_pages']}
            physically_present={i for i in query_gt if any(begin//780<=positions[a][i]<end//780 for begin,end in r['ranges'])}
            assert stages['candidate']==expected_discovered and stages['nominated']==expected_nominated and stages['fetched']==physically_present
            assert len(r['discoveries'])==2
            walk_pages={unit//8 for discovery in r['discoveries'] for unit in discovery['walk_evaluated_units']}|{discovery['seed_page'] for discovery in r['discoveries']}
            seed_pages={unit//8 for discovery in r['discoveries'] for unit in discovery['seed_evaluated_units']}
            walk_gt={i for i in query_gt if positions[a][i]//256 in walk_pages};seed_gt={i for i in query_gt if positions[a][i]//256 in seed_pages};flat_gt=stages['flat']
            assert stages['candidate']<=walk_gt
            for key,value in [('gt_walk_pool',len(query_gt-walk_gt)),('gt_source_roster',len(walk_gt-stages['candidate'])),('flat_gt_walk_pool',len(flat_gt-walk_gt)),('flat_gt_source_roster',len((flat_gt&walk_gt)-stages['candidate'])),('seed_evaluated_gt',len(seed_gt))]:discovery_loss[a][key]+=value
            for discovery in r['discoveries']:
                evaluated=discovery['walk_evaluated_units'];assert 0<len(evaluated)==len(set(evaluated))<=1272 and all(0<=unit<3125 for unit in evaluated)
                assert not discovery['walk_work_exhausted'] or len(evaluated)==1272
                assert set(range(discovery['seed_page']*8,min((discovery['seed_page']+1)*8,3125)))<=set(evaluated)
                pages={unit//8 for unit in evaluated}|{discovery['seed_page']};assert len(pages)>=159
                discovery_work[a].append(len(evaluated))
            assert set(sample['stages']['nominated'])<=set(sample['stages']['candidate']) and set(sample['stages']['nominated'])<=set(sample['stages']['fetched']) and set(sample['stages']['returned'])<=set(sample['stages']['fetched']) and sample['gets']==len(r['ranges'])<=32 and sample['bytes']==r['planned_bytes']<=16773120
    for a in plans:
        normal=''.join(json.dumps(p,sort_keys=True,separators=(',',':'))+'\n' for p in plans[a]);assert hashlib.sha256(normal.encode()).hexdigest()==result['normal_plans_sha256'][a];assert result['returned_hits'][a]==sum(s['returned_hits'] for s in result['samples'][a]) and result['p05'][a]==sorted(s['returned_hits'] for s in result['samples'][a])[3]
    assert result['normal_plans_sha256']['control']==item['expected_control_plans_sha256'] and result['returned_hits']['control']==item['control_returned_hits'] and result['new_exhaustive_sq8_kernel_calls']==0 and result['cached_sort_queries']==128
    assert result['returned_hits']['control']==item['control_returned_hits'] and sum(x['flat_hits'] for x in result['samples']['control'])==item['control_flat_hits']
    hits=result['returned_hits']['candidate'];passed=hits/64>=98 and result['p05']['candidate']>=95 and item['control_flat_hits']-hits<=32 and hits>=item['control_returned_hits']
    if not passed:assert result['decision']=='KILL actual core quality' and not result['physical_cold_measured']
    if passed and not result['physical_cold_measured']:assert result['decision']=='GO actual core development quality only'
    if result['physical_cold_measured']:
        refs={arm:[json.loads(s) for s in get(base+'native-'+arm+'/live.jsonl').splitlines()] for arm in ['control','candidate']}
        assert all(len(records)==66 and records[0]['root_sha256']==binding[arm+'_root_sha256'] and records[0]['generation']==records[0]['control_epoch']==1 and records[-1]['count']==64 for arm,records in refs.items())
        assert [(r['rep'],r['arm']) for r in result['runs']]==list(enumerate(config['arm_order']))
        for arm,records in refs.items():
            for q,r in enumerate(records[1:-1]):
                assert r['ids']==result['expected_ids'][arm][q] and {k:r[k] for k in ['query_ordinal','ranges','planned_bytes']}==plans[arm][q]
                assert set(r['ids'])&set(gt[q*100:(q+1)*100])==set(result['samples'][arm][q]['stages']['returned'])
                assert r['submitted_gets']==len(r['ranges'])<=32 and r['verified_bytes']==r['planned_bytes']<=16773120 and r['failed_gets']==0
        for run in result['runs']:
            arm=run['arm'];path=base+'run'+str(run['rep'])+'-'+arm+'/';authority=dict(root_sha256=binding[arm+'_root_sha256'],generation=1,control_epoch=1)
            assert run==obj(path+'result.json') and run['authority']==authority and run['count']==64
            assert obj(path+'boundary.json')==dict(root=409,generation=409,epoch=409,zero=400,unknown=422,body_cap=413,pressure=503,released=400,valid_ann_probes=0)
            live=[json.loads(s) for s in get(path+'http.jsonl').splitlines()];assert len(live)==64
            startup=[json.loads(s) for s in get(path+'server.log').splitlines() if s.startswith(b'{')];assert len(startup)==1 and startup[0]['phase']=='ready' and startup[0]['authority']==authority
            cleanup=obj(path+'server-closeout.json');assert cleanup['intentional_stop'] and cleanup['returncode']!=0
            import re
            rss=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',get(path+'server.time').decode()).group(1));assert rss>0
            for q,r in enumerate(live):
                assert r['query_ordinal']==q and r['authority']==authority and all(r[k]==refs[arm][q+1][k] for k in ['ids','ranges','planned_bytes','submitted_gets','verified_bytes','failed_gets'])
                assert len(r['ids'])==len(set(r['ids']))==100 and r['returned_hits']==len(set(r['ids'])&set(gt[q*100:(q+1)*100])) and r['submitted_gets']==len(r['ranges'])<=32 and r['verified_bytes']==r['planned_bytes']<=16773120 and r['failed_gets']==0
            assert run['returned_hits']==sum(r['returned_hits'] for r in live)==result['returned_hits'][arm] and run['p05']==sorted(r['returned_hits'] for r in live)[3]==result['p05'][arm]
            assert run['data_get_attempts']==sum(r['submitted_gets'] for r in live) and run['verified_bytes']==sum(r['verified_bytes'] for r in live)
            for label,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]:near(run['incoming_http_ms'][label],quantile([r['http_wall_ns']/1e6 for r in live],p))
            near(run['serial_observed_qps'],64e9/run['measurement_wall_ns'])
            run['process_max_rss_kib']=rss
        for arm in refs:
            for label in ['p50','p90','p95','p99']:near(result['arm_median_incoming_http_ms'][arm][label],quantile([r['incoming_http_ms'][label] for r in result['runs'] if r['arm']==arm],.5))
            near(result['arm_median_serial_qps'][arm],quantile([r['serial_observed_qps'] for r in result['runs'] if r['arm']==arm],.5))
        tail_pass=result['arm_median_incoming_http_ms']['candidate']['p90']<=250 and result['arm_median_incoming_http_ms']['candidate']['p95']<=400
        assert result['decision']==('GO actual core and HTTP development envelope only' if tail_pass else 'KILL actual core HTTP envelope')
    decomposition={}
    for arm in orders:
        samples=result['samples'][arm];counts={key:sum(sample[key] for sample in samples) for key in ['candidate_hits','nominated_hits','fetched_hits','returned_hits','flat_hits','gets','bytes']}
        losses=dict(gt_discovery=6400-counts['candidate_hits'],gt_nomination=counts['candidate_hits']-counts['nominated_hits'],gt_physical=counts['nominated_hits']-counts['fetched_hits'],gt_quantization_or_ranking=counts['fetched_hits']-counts['returned_hits'],flat_gt_discovery=0,flat_gt_nomination=0,flat_gt_physical=0,flat_gt_ranking=0,returned_outside_flat=0)
        losses['gt_physical']=0;losses['gt_physical_spill']=0
        for sample in samples:
            d,n,f,t,z=(set(sample['stages'][key]) for key in ['candidate','nominated','fetched','returned','flat'])
            losses['gt_physical']+=len(n-f);losses['gt_physical_spill']+=len(f-n)
            for key,value in [('flat_gt_discovery',len(z-d)),('flat_gt_nomination',len((z&d)-n)),('flat_gt_physical',len((z&n)-f)),('flat_gt_ranking',len((z&f)-t)),('returned_outside_flat',len(t-z))]:losses[key]+=value
        decomposition[arm]=dict(counts=counts,losses=losses,discovery_loss=discovery_loss[arm],graph_walks_verified=len(discovery_work[arm]),work_evaluations_cap=1272,work_evaluations_min=min(discovery_work[arm]),work_evaluations_max=max(discovery_work[arm]),naturally_ended_walks=sum(n<1272 for n in discovery_work[arm]))
    rows.append(dict(dataset=item['name'],returned_hits=result['returned_hits'],p05=result['p05'],decision=result['decision'],physical_cold_measured=result['physical_cold_measured'],decomposition=decomposition,arm_median_incoming_http_ms=result.get('arm_median_incoming_http_ms'),arm_median_serial_qps=result.get('arm_median_serial_qps'),runs=result.get('runs')))
cgroup=obj('screen/cgroup.json');assert int(cgroup['memory.max'])==8589934592 and int(cgroup['memory.swap.max'])==int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill']);report.update(valid_measurement=True,rows=rows,cgroup=cgroup,helper=helper,decision=decision['decision'],fresh_cohort_used=False)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
