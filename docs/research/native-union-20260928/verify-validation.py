"""Authenticate closed744 evidence and recompute recorded GT/stage counts, no scoring."""
import gzip,hashlib,io,json,math,struct,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1]
launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert all(terminal[k]==launch[k] for k in ['source_archive_sha256','source_base_commit','instance_id']) and terminal['schema']=='borsuk-native-union-validation-v1'
assert close['state']=='terminated' and close['instance_id']==launch['instance_id']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3')
assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    b=gzip.decompress((out/(name+'.gz')).read_bytes());ident=terminal['artifacts'][name];assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
def obj(name):return json.loads(get(name))
for name in terminal['artifacts']:get(name)
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read();assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256']
matched=[];native=0
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name);is_native=path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']
        if is_native or member.name in [str(root/x) for x in ['validation-config.json','validation-preregister.md']]+['scripts/run_native_union_validation.py','scripts/run_native_union_cold.py']:
            assert path.read_bytes()==tar.extractfile(member).read(),member.name;matched.append(member.name);native+=is_native
assert native==392
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=native,source_files_matched=len(matched),artifacts_verified=len(terminal['artifacts']),qualification=False)
if terminal['status']!='complete' or terminal['exit_code']!=0:
    report.update(valid_measurement=False,phase=terminal['phase'],exit_code=terminal['exit_code']);(out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
config=json.loads((root/'validation-config.json').read_text());assert hashlib.sha256((root/'validation-config.json').read_bytes()).hexdigest()==reservation['config_sha256']
reuse=obj('reuse.json');assert reuse['native_files_matched']==392 and reuse['no_compile_or_test_rerun'] and reuse['source_archive_sha256']==reservation['reused_native_source_archive_sha256'] and reuse['terminal_sha256']==reservation['reused_terminal_sha256']
def remote(ident):
    b=s3.get_object(Bucket=config['bucket'],Key=ident['key'])['Body'].read();assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
def near(a,b):assert math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-10),(a,b)
decision=obj('screen/decision.json');rows=[]
for item in config['items'][:len(decision['results'])]:
    name=item['name'];base='screen/'+name+'/';binding=obj(base+'binding.json');assert binding['source_roots']==item['roots'];result=obj(base+'result.json');decomposition=obj(base+'decomposition.json');assert len(decomposition)==744 and result['queries']==744 and result['source_roots']==item['roots']
    truth=struct.unpack('<100000I',remote(item['artifacts']['truth']));sq8=remote(item['artifacts']['sq8.bin']);order=[struct.unpack_from('<q',sq8,i*780)[0] for i in range(100000)];assert sorted(order)==list(range(100000));unit_of={i:p//32 for p,i in enumerate(order)};page_of={i:p//256 for p,i in enumerate(order)}
    plans={arm:[json.loads(s) for s in get(base+'validation-'+arm+'.jsonl').splitlines()] for arm in ['control','candidate']};counts={arm:{k:[] for k in result['metrics'][arm]} for arm in plans}
    for arm in plans:
        assert item['roots'][arm] in get(base+'validation-'+arm+'.time').decode()
        assert len(plans[arm])==744 and hashlib.sha256(get(base+'dev-'+arm+'.jsonl')).hexdigest()==item['expected_'+('control' if arm=='control' else 'union')+'_plans_sha256']
        normal=''.join(json.dumps({k:r[k] for k in ['query_ordinal','ranges','planned_bytes']},sort_keys=True,separators=(',',':'))+'\n' for r in plans[arm]);assert hashlib.sha256(normal.encode()).hexdigest()==result['normal_plans_sha256'][arm]
    for offset,q in enumerate(range(256,1000)):
        gt=set(truth[q*100:(q+1)*100]);assert len(gt)==100;stage_pair={}
        for arm in plans:
            r=plans[arm][offset];sample=result['samples'][arm][offset];assert r['query_ordinal']==sample['query_ordinal']==q
            assert len(r['discoveries'])==(1 if arm=='control' else 2);seed,walk=set(),set()
            for d in r['discoveries']:
                for field,cap,target in [('seed_evaluated_units',128,seed),('walk_evaluated_units',1272,walk)]:
                    values=d[field];assert len(values)==len(set(values))<=cap and all(type(i) is int and 0<=i<3125 for i in values);target.update(values)
            fetched=set()
            for i,(start,end) in enumerate(r['ranges']):
                assert 0<=start<end<=78000000 and start%199680==0 and (end%199680==0 or end==78000000) and (i==0 or r['ranges'][i-1][1]<start);fetched.update(order[start//780:end//780])
            assert len(r['ranges'])==sample['gets']<=32 and sum(e-s for s,e in r['ranges'])==sample['bytes']==r['planned_bytes']<=16773120
            returned,flat=[sample[k] for k in ['returned_ids','flat_ids']];assert len(set(returned))==len(returned)==len(set(flat))==len(flat)==100 and set(returned)<=fetched
            states=dict(seed={i for i in gt if unit_of[i] in seed},walk={i for i in gt if unit_of[i] in walk},candidate={i for i in gt if page_of[i] in r['ranked_candidate_pages']},nominated={i for i in gt if page_of[i] in r['selected_pages']},physical=gt&fetched,returned=gt&set(returned),flat=gt&set(flat));states['visited']=states['seed']|states['walk']
            assert states['nominated']<=states['candidate'] and states['nominated']<=states['physical'] and states['returned']<=states['physical'];assert {k:sorted(v) for k,v in states.items()}==decomposition[offset]['stages'][arm]
            for k,v in states.items():assert sample[k+'_hits']==len(v);counts[arm][k+'_hits'].append(len(v))
            stage_pair[arm]=states
        assert result['samples']['control'][offset]['flat_ids']==result['samples']['candidate'][offset]['flat_ids']
        assert {k:dict(gained=sorted(stage_pair['candidate'][k]-stage_pair['control'][k]),lost=sorted(stage_pair['control'][k]-stage_pair['candidate'][k])) for k in stage_pair['control']}==decomposition[offset]['gained_lost']
    for arm in counts:
        for field,values in counts[arm].items():
            metric=result['metrics'][arm][field];assert metric['total']==sum(values) and metric['p05']==sorted(values)[37];near(metric['mean'],sum(values)/744)
    m=result['metrics']['candidate'];c=result['metrics']['control'];passed=m['returned_hits']['mean']>=98 and m['returned_hits']['p05']>=95 and m['flat_hits']['mean']-m['returned_hits']['mean']<=.5 and m['returned_hits']['total']>=c['returned_hits']['total'] and m['candidate_hits']['total']>c['candidate_hits']['total'];assert result['decision'].startswith('GO')==passed
    rows.append(dict(dataset=name,split=result['split'],metrics=result['metrics'],stage_loss=result['stage_loss'],decision=result['decision']))
assert decision['results']==[obj('screen/'+r['dataset']+'/result.json') for r in rows] and decision['decision']==rows[-1]['decision'];assert len(rows)==2 or rows[-1]['decision'].startswith('KILL')
cgroup=obj('screen/cgroup.json');assert int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill'])
report.update(valid_measurement=True,rows=rows,decision=decision['decision'],cgroup=cgroup,physical_reads_measured=False,new_exhaustive_queries=746*len(rows),native_planner_calls=1616*len(rows))
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
