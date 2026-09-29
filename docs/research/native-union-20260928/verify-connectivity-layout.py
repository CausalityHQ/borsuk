"""Closed layout receipts/source/GT-set verification; no query/scoring kernel."""
import gzip,hashlib,io,json,math,struct,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1];launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert terminal['schema']=='borsuk-native-connectivity-layout-v1'
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
        if is_native or member.name in [str(root/x) for x in ['layout-config.json','connectivity-layout-preregister.md']]+['scripts/run_native_union_layout_transfer.py']:
            assert path.read_bytes()==tar.extractfile(member).read(),member.name;matched+=1;native+=is_native
assert native==393
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=native,source_files_matched=matched,artifacts_verified=len(terminal['artifacts']),qualification=False)
if terminal['status']!='complete' or terminal['exit_code']!=0:
    report.update(valid_measurement=False,phase=terminal['phase'],exit_code=terminal['exit_code']);(out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
config=json.loads((root/'layout-config.json').read_text());assert hashlib.sha256((root/'layout-config.json').read_bytes()).hexdigest()==reservation['config_sha256'];helper=obj('helper.json');assert helper['source_archive_sha256']==reservation['reused_native_source_archive_sha256'] and helper['reused_binary'] and helper['native_source_matches_compiled_transformation'] and helper['sha256']==terminal['artifacts']['binaries/build_sq8_source']['sha256'] and helper['bytes']==terminal['artifacts']['binaries/build_sq8_source']['bytes']
decision=obj('screen/decision.json');rows=[]
def quantile(v,p):
    v=sorted(v);i=(len(v)-1)*p;lo=math.floor(i);hi=math.ceil(i);return v[lo]+(v[hi]-v[lo])*(i-lo)
def near(a,b):assert math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-8),(a,b)
for item in config['items'][:len(decision['results'])]:
    base='screen/'+item['name']+'/';result=obj(base+'result.json');binding=obj(base+'binding.json');assert binding['control_root_sha256']==item['control_root_sha256'] and binding['per_id_sq8_and_two_bit_payload_exact'] and binding['coefficient_f32_bits_exact']
    records=[json.loads(s) for s in get(base+'paired.jsonl').splitlines()];assert len(records)==128;plans={a:[] for a in ['control','candidate']}
    ident=item['artifacts']['truth'];truth=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read();assert len(truth)==ident['bytes'] and hashlib.sha256(truth).hexdigest()==ident['sha256'];gt=struct.unpack('<100000I',truth)
    ident=item['artifacts']['sq8.bin'];old_sq8=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read();assert len(old_sq8)==ident['bytes'] and hashlib.sha256(old_sq8).hexdigest()==ident['sha256']
    old_ids=[struct.unpack_from('<q',old_sq8,pos*780)[0] for pos in range(100000)];del old_sq8
    encoded_order=get(base+'order.u64');assert hashlib.sha256(encoded_order).hexdigest()==binding['source_order_sha256'];new_ids=struct.unpack('<100000Q',encoded_order)
    assert set(old_ids)==set(new_ids)==set(range(100000)) and len(set(old_ids))==len(set(new_ids))==100000
    orders={'control':old_ids,'candidate':new_ids};positions={arm:{i:pos for pos,i in enumerate(ids)} for arm,ids in orders.items()}
    discovery_work={arm:[] for arm in orders};discovery_loss={arm:dict(gt_walk_pool=0,gt_centroid_roster=0,flat_gt_walk_pool=0,flat_gt_centroid_roster=0,seed_evaluated_gt=0) for arm in orders}

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
            for key,value in [('gt_walk_pool',len(query_gt-walk_gt)),('gt_centroid_roster',len(walk_gt-stages['candidate'])),('flat_gt_walk_pool',len(flat_gt-walk_gt)),('flat_gt_centroid_roster',len((flat_gt&walk_gt)-stages['candidate'])),('seed_evaluated_gt',len(seed_gt))]:discovery_loss[a][key]+=value
            for discovery in r['discoveries']:
                evaluated=discovery['walk_evaluated_units'];assert len(evaluated)==len(set(evaluated))==1272 and all(0<=unit<3125 for unit in evaluated)
                pages={unit//8 for unit in evaluated}|{discovery['seed_page']};assert len(pages)>=159
                discovery_work[a].append(len(evaluated))
            assert set(sample['stages']['nominated'])<=set(sample['stages']['candidate']) and set(sample['stages']['nominated'])<=set(sample['stages']['fetched']) and set(sample['stages']['returned'])<=set(sample['stages']['fetched']) and sample['gets']==len(r['ranges'])<=32 and sample['bytes']==r['planned_bytes']<=16773120
    for a in plans:
        normal=''.join(json.dumps(p,sort_keys=True,separators=(',',':'))+'\n' for p in plans[a]);assert hashlib.sha256(normal.encode()).hexdigest()==result['normal_plans_sha256'][a];assert result['returned_hits'][a]==sum(s['returned_hits'] for s in result['samples'][a]) and result['p05'][a]==sorted(s['returned_hits'] for s in result['samples'][a])[3]
    assert result['normal_plans_sha256']['control']==item['expected_control_plans_sha256'] and result['returned_hits']['control']==item['control_returned_hits'] and result['new_exhaustive_sq8_kernel_calls']==0 and result['cached_sort_queries']==128
    assert result['returned_hits']['control']==item['control_returned_hits'] and sum(x['flat_hits'] for x in result['samples']['control'])==item['control_flat_hits']
    hits=result['returned_hits']['candidate'];passed=hits/64>=98 and result['p05']['candidate']>=95 and item['control_flat_hits']-hits<=32 and hits>=item['control_returned_hits']
    assert passed==result['physical_cold_measured']
    if not passed:assert result['decision']=='KILL source-order quality'
    if result['physical_cold_measured']:
        ident=item['artifacts']['truth'];truth=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read();assert len(truth)==ident['bytes'] and hashlib.sha256(truth).hexdigest()==ident['sha256'];gt=struct.unpack('<100000I',truth)
        for run in result['runs']:
            a=run['arm'];path=base+'run'+str(run['rep'])+'-'+a+'/';live=[json.loads(s) for s in get(path+'live.jsonl').splitlines()];assert len(live)==66 and live[0]['root_sha256']==binding[a+'_root_sha256'] and live[0]['generation']==1 and live[0]['control_epoch']==1 and live[-1]['count']==64
            for q,r in enumerate(live[1:-1]):
                assert {k:r[k] for k in ['query_ordinal','ranges','planned_bytes']}==plans[a][q] and r['submitted_gets']==len(r['ranges'])<=32 and r['verified_bytes']==r['planned_bytes']<=16773120 and r['failed_gets']==0
                assert len(r['ids'])==len(set(r['ids']))==100 and set(r['ids'])&set(gt[q*100:(q+1)*100])==set(result['samples'][a][q]['stages']['returned'])
            for label,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]:near(run['complete_library_call_ms'][label],quantile([r['query_wall_ns']/1e6 for r in live[1:-1]],p))
            near(run['serial_observed_qps'],64e9/live[-1]['measurement_wall_ns'])
    if result['physical_cold_measured']:
        medians={arm:{label:quantile([r['complete_library_call_ms'][label] for r in result['runs'] if r['arm']==arm],.5) for label in ['p50','p90','p95','p99']} for arm in plans}
        for arm in medians:
            for label in medians[arm]:near(medians[arm][label],result['arm_median_complete_call_ms'][arm][label])
        tail_pass=medians['candidate']['p90']<=250 and medians['candidate']['p95']<=400
        assert result['decision']==('GO source-order development and cold envelope only' if tail_pass else 'KILL source-order cold envelope')
    decomposition={}
    for arm in orders:
        samples=result['samples'][arm];counts={key:sum(sample[key] for sample in samples) for key in ['candidate_hits','nominated_hits','fetched_hits','returned_hits','flat_hits','gets','bytes']}
        losses=dict(gt_discovery=6400-counts['candidate_hits'],gt_nomination=counts['candidate_hits']-counts['nominated_hits'],gt_physical=counts['nominated_hits']-counts['fetched_hits'],gt_quantization_or_ranking=counts['fetched_hits']-counts['returned_hits'],flat_gt_discovery=0,flat_gt_nomination=0,flat_gt_physical=0,flat_gt_ranking=0,returned_outside_flat=0)
        for sample in samples:
            d,n,f,t,z=(set(sample['stages'][key]) for key in ['candidate','nominated','fetched','returned','flat'])
            for key,value in [('flat_gt_discovery',len(z-d)),('flat_gt_nomination',len((z&d)-n)),('flat_gt_physical',len((z&n)-f)),('flat_gt_ranking',len((z&f)-t)),('returned_outside_flat',len(t-z))]:losses[key]+=value
        decomposition[arm]=dict(counts=counts,losses=losses,discovery_loss=discovery_loss[arm],graph_walks_verified=len(discovery_work[arm]),work_evaluations_per_walk=1272)
    rows.append(dict(dataset=item['name'],returned_hits=result['returned_hits'],p05=result['p05'],decision=result['decision'],physical_cold_measured=result['physical_cold_measured'],decomposition=decomposition,arm_median_complete_call_ms=result.get('arm_median_complete_call_ms')))
cgroup=obj('screen/cgroup.json');assert int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill']);report.update(valid_measurement=True,rows=rows,cgroup=cgroup,helper=helper,decision=decision['decision'],fresh_cohort_used=False)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
