"""Closed metadata, truth-set and clock reduction only; no query/scoring execution."""
import gzip,hashlib,io,json,math,struct,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw)
assert terminal['status']=='complete' and terminal['exit_code']==0
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert terminal['instance_id']==launch['instance_id']==json.loads((out/'aws-closeout.json').read_text())['instance_id']
assert json.loads((out/'aws-closeout.json').read_text())['state']=='terminated'
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3');bucket=launch['bucket']
assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    p=out/(name+'.gz');b=gzip.decompress(p.read_bytes()) if p.exists() else s3.get_object(Bucket=bucket,Key=launch['prefix']+'/artifacts/'+name)['Body'].read()
    ident=terminal['artifacts'][name];assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
for name in terminal['artifacts']:get(name)
assert b'test tests::frozen_union_has_unique_bounded_pages ... ok' in get('unit-check.log')
archive=s3.get_object(Bucket=bucket,Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read()
assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256']==terminal['source_archive_sha256']
matched=[]
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        p=Path(member.name)
        if p.suffix=='.rs' or p.name in ['Cargo.toml','Cargo.lock'] or member.name in ['scripts/run_union_nomination.py','docs/research/union-nomination-20260928/config.json','docs/research/union-nomination-20260928/preregister.md','docs/research/union-nomination-20260928/plan.md','docs/research/union-nomination-20260928/aws-green.py']:
            assert p.read_bytes()==tar.extractfile(member).read(),str(p);matched.append(member.name)
config=json.loads(Path('docs/research/union-nomination-20260928/config.json').read_text());decision=json.loads(get('screen/decision.json'))
assert hashlib.sha256(Path('docs/research/union-nomination-20260928/config.json').read_bytes()).hexdigest()==json.loads((out/'aws-reservation.json').read_text())['config_sha256']
old_dir=Path('docs/research/topology-screen-20260928/a0003');old_terminal=json.loads((old_dir/'aws-terminal.json').read_text());old_launch=json.loads((old_dir/'aws-launch.json').read_text())
assert hashlib.sha256((old_dir/'aws-terminal.json').read_bytes()).hexdigest()==config['source_terminal_sha256']
def asset(identity):
    b=s3.get_object(Bucket=bucket,Key=identity['key'])['Body'].read();assert len(b)==identity['bytes'] and hashlib.sha256(b).hexdigest()==identity['sha256'];return b
rows=[]
for item,result in zip(config['items'],decision['results']):
    name=item['name'];assert result==json.loads(get('screen/'+name+'/result.json')) and result['dataset']==name
    truth=struct.unpack('<100000I',asset(item['artifacts']['truth']))
    role='screen/'+name+'/order.u64';identity=dict(old_terminal['artifacts'][role],key=old_launch['prefix']+'/artifacts/'+role)
    order=struct.unpack('<100000Q',asset(identity));assert sorted(order)==list(range(100000));position={identity:i for i,identity in enumerate(order)}
    records=[json.loads(s) for s in get('screen/'+name+'/nomination.jsonl').splitlines()];decomp=json.loads(get('screen/'+name+'/decomposition.json'))
    assert len(records)==len(decomp)==128
    plans=[json.loads(s) for s in asset(item['artifacts']['preflight-plans.jsonl']).splitlines()]
    traces=[json.loads(s) for s in asset(item['artifacts']['paired-plans.jsonl']).splitlines()]
    old_decomp=json.loads(gzip.decompress((old_dir/('screen/'+name+'/decomposition.json.gz')).read_bytes()))
    old_sets={(d['query_ordinal'],d['arm']):d['stages'] for d in old_decomp}
    samples={'control':[],'union':[]}
    for index,(r,d) in enumerate(zip(records,decomp)):
        q=index//2;arm=(['control','union'] if q%2==0 else ['union','control'])[index%2]
        assert (r['query_ordinal'],r['arm'])==(q,arm)==(d['query_ordinal'],d['arm'])
        wanted=set(next(t for t in traces[q*2:q*2+2] if t['arm']=='control')['ranked_candidate_pages'])
        if arm=='union':wanted.update(next(t for t in traces[q*2:q*2+2] if t['arm']=='candidate')['ranked_candidate_pages'])
        assert len(r['ranked_candidate_pages'])==len(wanted) and set(r['ranked_candidate_pages'])==wanted
        if arm=='control':assert {k:r[k] for k in ['query_ordinal','ranges','planned_bytes']}==plans[q]
        gt=set(truth[q*100:(q+1)*100]);assert len(gt)==100
        physical=set();previous=-1
        for start,end in r['ranges']:
            assert type(start) is int and type(end) is int and 0<=start<end<=78000000 and start%199680==0 and (end%199680==0 or end==78000000) and start>previous
            previous=end;physical.update(range(start//199680,(end-1)//199680+1))
        assert len(r['ranges'])<=32 and r['planned_bytes']==sum(e-s for s,e in r['ranges'])<=16773120
        assert set(r['selected_pages'])<=wanted and set(r['selected_pages'])<=physical
        ids=d['returned_ids'];assert len(ids)==len(set(ids))==100 and all(type(i) is int and 0<=i<100000 and position[i]//256 in physical for i in ids)
        expected=dict(candidate=sorted(g for g in gt if position[g]//256 in wanted),nominated=sorted(g for g in gt if position[g]//256 in set(r['selected_pages'])),physical=sorted(g for g in gt if position[g]//256 in physical),returned=sorted(gt&set(ids)))
        for phase,values in expected.items():assert d['stages'][phase]==values
        assert d['stages']['flat']==old_sets[q,'control']['flat']
        if arm=='control':
            for phase in ['candidate','nominated','physical','returned']:assert d['stages'][phase]==old_sets[q,'control'][phase]
        sample=dict(query_ordinal=q,candidate_hits=len(expected['candidate']),fetched_hits=len(expected['physical']),returned_hits=len(expected['returned']),flat_hits=len(d['stages']['flat']),gets=len(r['ranges']),bytes=r['planned_bytes'])
        samples[arm].append(sample)
    assert samples==result['samples']
    for arm,ss in samples.items():
        for field in ['candidate_hits','fetched_hits','returned_hits','flat_hits']:assert result['metrics'][arm][field]==dict(total=sum(s[field] for s in ss),mean=sum(s[field] for s in ss)/64,p05=sorted(s[field] for s in ss)[3])
        for field,values in result['timings_ms_nomination_only'][arm].items():
            times=sorted(r[field]/1e6 for r in records if r['arm']==arm)
            for label,p in zip(['p50','p90','p95','p99'],[.5,.9,.95,.99]):
                at=63*p;lo=math.floor(at);hi=math.ceil(at);v=times[lo]+(times[hi]-times[lo])*(at-lo);assert math.isclose(values[label],v,rel_tol=1e-12)
    total=lambda arm,f:sum(s[f] for s in samples[arm]);fetch,ret={'relaion':(6379,6346),'cohere':(6392,6342)}[name]
    checks=dict(positive_discovery=total('union','candidate_hits')>total('control','candidate_hits'),fetched_floor=total('union','fetched_hits')>=fetch,returned_floor=total('union','returned_hits')>=ret,no_returned_regression=total('union','returned_hits')>=total('control','returned_hits'),flat_gap=total('union','flat_hits')-total('union','returned_hits')<=32,p05=sorted(s['returned_hits'] for s in samples['union'])[3]>=95)
    assert result['decision']==('GO' if all(checks.values()) else 'KILL')
    rows.append(dict(dataset=name,split='consumed development0-63',decision=result['decision'],checks=checks,metrics=result['metrics'],nomination_only_ms=result['timings_ms_nomination_only']))
    if result['decision']=='KILL':assert result==decision['results'][-1]
assert not decision['qualification'] and not decision['validation_or_scale_run']
report=dict(instance_id=launch['instance_id'],state='terminated',source_files_matched=len(matched),artifacts_verified=len(terminal['artifacts']),source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),convergence_rows=rows,ordered_id_scoring_parity_scope='AWS cached SQ8 sorting only; local verification checks GT sets/counts, not reranking',qualification=False)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
