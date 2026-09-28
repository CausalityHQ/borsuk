"""Closed-artifact identity and GT-set reduction only; never run an ANN query."""
import gzip,hashlib,io,json,struct,sys,tarfile
from pathlib import Path
import boto3

out=Path(sys.argv[1])
launch=json.loads((out/'aws-launch.json').read_text())
raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw)
close=json.loads((out/'aws-closeout.json').read_text())
assert close['instance_id']==terminal['instance_id']==launch['instance_id'] and close['state']=='terminated'
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3')
assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert terminal['status']=='complete' and terminal['exit_code']==0
assert terminal['source_archive_sha256']==launch['source_archive_sha256'] and terminal['source_base_commit']==launch['source_base_commit']

def get(name):
    local=out/(name+'.gz')
    data=gzip.decompress(local.read_bytes()) if local.is_file() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read()
    ident=terminal['artifacts'][name]
    assert len(data)==ident['bytes'] and hashlib.sha256(data).hexdigest()==ident['sha256']
    return data

def value(name):return json.loads(get(name))
verified=[]
for name in terminal['artifacts']:
    if (out/(name+'.gz')).is_file():get(name);verified.append(name)
archive=s3.get_object(Bucket=launch['bucket'],Key=f"research/native-library-check/sources/{launch['source_archive_sha256']}.tar.gz")['Body'].read()
assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256']
matched=[]
selected={'Cargo.toml','Cargo.lock','native_two_bit_cosine_development.py','v291_two_stage_development.py','native_two_bit_topology.py','test_native_two_bit_topology.py','run_native_two_bit_topology.py','test_native_two_bit_topology_runner.py'}
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name)
        if path.suffix=='.rs' or path.name in selected or member.name in ['docs/research/topology-screen-20260928/config.json','docs/research/topology-screen-20260928/preregister.md','docs/research/topology-screen-20260928/aws-launch.py']:
            assert path.is_file() and path.read_bytes()==tar.extractfile(member).read(),member.name
            matched.append(member.name)
config=json.loads(Path('docs/research/topology-screen-20260928/config.json').read_text())
reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(Path('docs/research/topology-screen-20260928/config.json').read_bytes()).hexdigest()==reservation['config_sha256']
adapter_log=get('adapter-check.log').decode()
assert 'test tests::graph_root_preserves_unrelated_numeric_tokens ... ok' in adapter_log
assert 'test graph_variant_adapter_preserves_components_and_rejects_untrusted_roots ... ok' in adapter_log
assert 'seven topology controller checks passed' in get('metadata-check.log').decode()
assert 'twelve topology review regressions passed' in get('metadata-check.log').decode()
assert 'topology range/scorer adapter checks passed' in get('metadata-check.log').decode()
decision=value('screen/decision.json');executed=decision['executed_datasets']
assert executed in [['relaion'],['relaion','cohere']] and not decision['validation_or_scale_run'] and not decision['qualification']
if len(executed)==1:assert decision['decision']=='KILL'
rows=[]
for item in config['items'][:len(executed)]:
    name=item['name'];prefix=f'screen/{name}/'
    result=value(prefix+'result.json');authority=value(prefix+'authority.json')
    control=value(prefix+'control/manifest.json');candidate=value(prefix+'candidate/manifest.json')
    assert hashlib.sha256(get(prefix+'control/manifest.json')).hexdigest()==result['roots']['control']==authority['current_root_sha256']
    assert hashlib.sha256(get(prefix+'candidate/manifest.json')).hexdigest()==result['roots']['candidate']
    assert result['roots']['control']!=result['roots']['candidate'] and control['graph_sha256']!=candidate['graph_sha256']
    restored=get(prefix+'candidate/manifest.json').decode()
    for key in ['graph_sha256','graph_resident_bytes']:
        old=json.dumps(candidate[key],separators=(',',':'));new=json.dumps(control[key],separators=(',',':'))
        token='"'+key+'":'+old
        assert restored.count(token)==1
        restored=restored.replace(token,'"'+key+'":'+new,1)
    assert restored.encode()==get(prefix+'control/manifest.json')

    assert json.dumps({k:v for k,v in control.items() if k not in ['graph_sha256','graph_resident_bytes']},sort_keys=True)==json.dumps({k:v for k,v in candidate.items() if k not in ['graph_sha256','graph_resident_bytes']},sort_keys=True)
    for arm,root in [('control',control),('candidate',candidate)]:
        assert root['schema']=='borsuk-two-bit-generation-v3' and root['base_epoch']==0
        for file,key in [('graph.bin','graph_sha256'),('centroids.bin','centroids_sha256'),('page_manifest.json','page_manifest_sha256'),('plane/manifest.json','plane_manifest_sha256')]:
            assert hashlib.sha256(get(prefix+arm+'/'+file)).hexdigest()==root[key]
    assert authority['historical_root_fingerprint']==item['historical_root_sha256'] and authority['source_control_scorer_parity']
    assert terminal['artifacts'][prefix+'sq8.bin']['sha256']==item['sq8_sha256']
    assert terminal['artifacts'][prefix+'normalized.f32']['sha256']==item['normalized_sha256']
    assert terminal['artifacts'][prefix+'order.u64']['sha256']==item['order_sha256']
    assert terminal['artifacts'][prefix+'scores.npy']['sha256']==authority['score_cache_sha256']
    assert terminal['artifacts'][prefix+'control/canonical.bin']==dict(bytes=control['canonical']['bytes'],sha256=control['canonical']['sha256'])
    assert hashlib.sha256(get(prefix+'preflight-plans.jsonl')).hexdigest()==item['historical_plans_sha256']
    baseline=json.loads(Path(item['historical_score_file']).read_text())
    assert hashlib.sha256(Path(item['historical_score_file']).read_bytes()).hexdigest()==item['historical_score_sha256']
    assert authority['preflight']==baseline['samples']
    plans=[json.loads(line) for line in get(prefix+'paired-plans.jsonl').splitlines()]
    preflight=[json.loads(line) for line in get(prefix+'preflight-plans.jsonl').splitlines()]
    sets=value(prefix+'decomposition.json');assert len(plans)==len(sets)
    truth_bytes=s3.get_object(Bucket=config['bucket'],Key=item['inputs']['truth']['key'])['Body'].read()
    assert len(truth_bytes)==400000 and hashlib.sha256(truth_bytes).hexdigest()==item['inputs']['truth']['sha256']
    truth=struct.unpack('<100000I',truth_bytes)
    order_bytes=s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+prefix+'order.u64')['Body'].read()
    assert len(order_bytes)==800000 and hashlib.sha256(order_bytes).hexdigest()==item['order_sha256']
    order=struct.unpack('<100000Q',order_bytes);assert sorted(order)==list(range(100000))
    position={identity:i for i,identity in enumerate(order)}
    by_arm=dict(control=[],candidate=[])
    for index,(plan,decomp) in enumerate(zip(plans,sets)):
        i=index//2;arm=(['control','candidate'] if i%2==0 else ['candidate','control'])[index%2]
        assert (plan['query_ordinal'],plan['arm'],plan['root_sha256'])==(i,arm,result['roots'][arm])
        assert (decomp['query_ordinal'],decomp['arm'])==(i,arm)
        if arm=='control':assert {k:plan[k] for k in ['query_ordinal','ranges','planned_bytes']}==preflight[i]
        gt=set(truth[i*100:(i+1)*100]);assert len(gt)==100
        seed=set(plan['seed_evaluated_units']);walk=set(plan['walk_evaluated_units'])
        assert len(seed)==len(plan['seed_evaluated_units'])<=128 and len(walk)==len(plan['walk_evaluated_units'])<=1272
        assert len(set(plan['ranked_candidate_pages']))==159 and plan['seed_page'] in plan['ranked_candidate_pages'] and plan['primary_page']==plan['ranked_candidate_pages'][0]
        expected=dict(seed={identity for identity in gt if position[identity]//32 in seed},walk={identity for identity in gt if position[identity]//32 in walk})
        expected['visited']=expected['seed']|expected['walk']
        expected['candidate']={identity for identity in gt if position[identity]//256 in plan['ranked_candidate_pages']}
        expected['nominated']={identity for identity in gt if position[identity]//256 in plan['selected_pages']}
        expected['physical']={identity for identity in gt if any(start//780<=position[identity]<end//780 for start,end in plan['ranges'])}
        for stage,ids in expected.items():assert set(decomp['stages'][stage])==ids and len(decomp['stages'][stage])==len(ids)
        assert set(decomp['stages']['returned'])<=expected['physical'] and set(decomp['stages']['flat'])<=gt
        sample=dict(query_ordinal=i,candidate_hits=len(expected['candidate']),fetched_hits=len(expected['physical']),returned_hits=len(decomp['stages']['returned']),flat_hits=len(decomp['stages']['flat']),gets=len(plan['ranges']),bytes=sum(end-start for start,end in plan['ranges']))
        assert sample['bytes']==plan['planned_bytes']<=16773120 and sample['gets']<=32
        assert sample==result['samples'][arm][len(by_arm[arm])];by_arm[arm].append(sample)
    if result['quality_complete']:
        assert len(plans)==128
        totals={arm:{key:sum(s[key] for s in samples) for key in ['candidate_hits','fetched_hits','returned_hits','flat_hits']} for arm,samples in by_arm.items()}
        floor=dict(relaion=(6379,6346),cohere=(6392,6342))[name]
        candidate_totals=totals['candidate'];control_totals=totals['control']
        passed=(candidate_totals['candidate_hits']>control_totals['candidate_hits'] and candidate_totals['fetched_hits']>=floor[0] and candidate_totals['returned_hits']>=max(floor[1],control_totals['returned_hits']) and candidate_totals['flat_hits']-candidate_totals['returned_hits']<=32 and sorted(s['returned_hits'] for s in by_arm['candidate'])[3]>=95)
        assert (result['decision']=='GO')==passed
        rows.append(dict(dataset=name,baseline=control_totals,candidate=candidate_totals,returned_delta_pp=(candidate_totals['returned_hits']-control_totals['returned_hits'])/64,cause=result['cause'],decision=result['decision']))
    else:
        assert result['decision']=='KILL' and len(plans)<128 and get(prefix+'paired.stderr').decode().strip()=='Error: Invalid("candidate geometry")'
        next_arm=(['control','candidate'] if (len(plans)//2)%2==0 else ['candidate','control'])[len(plans)%2]
        assert next_arm=='candidate'
        rows.append(dict(dataset=name,decision='KILL',cause=result['cause'],partial_records=len(plans)))
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),source_files_matched=len(matched),locally_verified_artifacts=len(verified),artifact_count=len(terminal['artifacts']),convergence_rows=rows)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report))
