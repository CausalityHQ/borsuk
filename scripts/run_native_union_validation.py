"""Frozen consumed744 method-validation, same v4 binaries/source/scorer, no serving claim."""
import json,math,subprocess,sys,time
from pathlib import Path
import numpy as np
from scripts.run_native_union_cold import native,plan,plan_sha,sha,write
from scripts.native_two_bit_cosine_development import normalize
from scripts.v291_two_stage_development import rust_sq8_scores

def score(sq8,query,low,step):
    # Same row-independent helper/coordinate rounding; bounded2048-row working set.
    return np.concatenate([rust_sq8_scores(sq8[i:i+2048],query,low,step) for i in range(0,len(sq8),2048)])

def evaluate(item,directory,config,binaries):
    directory.mkdir()
    for role,ident in item['artifacts'].items():
        p=directory/role;p.parent.mkdir(parents=True,exist_ok=True)
        subprocess.run(['aws','s3','cp','s3://'+config['bucket']+'/'+ident['key'],str(p),'--only-show-errors'],check=True)
        if p.stat().st_size!=ident['bytes'] or sha(p)!=ident['sha256']:raise ValueError('frozen input '+role)
    roots={arm:json.loads((directory/arm/'manifest.json').read_text()) for arm in ['control','candidate']}
    for arm in roots:
        if sha(directory/arm/'manifest.json')!=item['roots'][arm] or roots[arm]['schema']!='borsuk-two-bit-generation-v4':raise ValueError('v4 root')
    control,candidate=[dict(roots[arm]) for arm in ['control','candidate']]
    for field in ['diverse_graph_sha256','diverse_graph_resident_bytes']:del control[field],candidate[field]
    if control!=candidate or roots['control']['graph_sha256']!=roots['control']['diverse_graph_sha256']:raise ValueError('matched root intervention')
    requests=[json.loads(s) for s in (directory/'requests').read_text().splitlines()]
    if len(requests)!=1000 or [r['query_ordinal'] for r in requests]!=list(range(1000)):raise ValueError('requests')
    truth=np.fromfile(directory/'truth',dtype='<u4').reshape(1000,100)
    if (truth>=100000).any() or any(len(set(row.tolist()))!=100 for row in truth):raise ValueError('GT sets')
    sq8=np.memmap(directory/'sq8.bin',mode='r',dtype=[('id','<i8'),('norm','<f4'),('code','u1',(768,))])
    if sq8.shape!=(100000,) or not np.array_equal(np.sort(sq8['id']),np.arange(100000)) or not np.isfinite(sq8['norm']).all() or not (sq8['norm']>0).all():raise ValueError('signed source IDs/norms')
    low,step=[np.asarray(roots['control'][k],dtype=np.float32) for k in ['low','step']]
    if roots['control']['sq8_object_sha256']!=item['artifacts']['sq8.bin']['sha256'] or any(x.shape!=(768,) or not np.isfinite(x).all() for x in [low,step]) or not (step>0).all():raise ValueError('scorer authority')
    cache=np.load(directory/'scores.npy',mmap_mode='r',allow_pickle=False)
    if cache.shape!=(64,100000) or cache.dtype!=np.dtype('<f4'):raise ValueError('closed cache')
    for q in [0,63]:
        if not np.array_equal(score(sq8,normalize(requests[q]['query']),low,step),cache[q]):raise ValueError('bounded scorer exact closed cache parity')
    write(directory/'binding.json',dict(source_roots=item['roots'],native_binary_sha256=sha(binaries/'two_bit_plan_demo'),trace_root_authority='authenticated input root SHA and exact recorded native CLI; ordinary trace has no root field'))
    plans={}
    for arm in roots:
        args=[binaries/'two_bit_plan_demo',directory/arm,item['roots'][arm],directory/'requests',item['artifacts']['requests']['sha256']]
        native(directory,'dev-'+arm,[*args,directory/('dev-'+arm+'.jsonl'),0,64])
        if sha(directory/('dev-'+arm+'.jsonl'))!=item['expected_'+('control' if arm=='control' else 'union')+'_plans_sha256']:raise ValueError('unchanged dev plan SHA')
        native(directory,'validation-'+arm,[*args,directory/('validation-'+arm+'.jsonl'),256,744,'--trace'],cap=360)
        plans[arm]=[json.loads(s) for s in (directory/('validation-'+arm+'.jsonl')).read_text().splitlines()]
        if len(plans[arm])!=744 or [r['query_ordinal'] for r in plans[arm]]!=list(range(256,1000)):raise ValueError('trace roster')
    unit_of=np.empty(100000,dtype=np.int64);page_of=np.empty(100000,dtype=np.int64)
    unit_of[sq8['id']]=np.arange(100000)//32;page_of[sq8['id']]=np.arange(100000)//256
    samples={arm:[] for arm in roots};decomposition=[];scoring_start=time.monotonic()
    for offset,q in enumerate(range(256,1000)):
        values=score(sq8,normalize(requests[q]['query']),low,step)
        if not np.isfinite(values).all():raise ValueError('finite scores')
        flat=sq8['id'][np.lexsort((sq8['id'],values))[:100]].tolist();gt=set(truth[q].tolist());stages={}
        for arm in roots:
            r=plans[arm][offset]
            if len(r['discoveries'])!=(1 if arm=='control' else 2):raise ValueError('graph count')
            seed,walk=set(),set()
            for discovery in r['discoveries']:
                for field,cap,target in [('seed_evaluated_units',128,seed),('walk_evaluated_units',1272,walk)]:
                    ids=discovery[field]
                    if len(ids)>cap or len(ids)!=len(set(ids)) or any(type(i) is not int or not 0<=i<3125 for i in ids):raise ValueError('graph work bounds')
                    target.update(ids)
            ranked,selected=r['ranked_candidate_pages'],r['selected_pages']
            if len(ranked)>318 or len(ranked)!=len(set(ranked)) or len(selected)!=len(set(selected)) or any(type(p) is not int or not 0<=p<391 for p in ranked+selected) or not set(selected)<=set(ranked):raise ValueError('nomination geometry')
            ranges=r['ranges']
            if not 0<len(ranges)<=32 or any(not 0<=s<e<=78000000 or s%199680 or (e%199680 and e!=78000000) for s,e in ranges) or any(ranges[i][1]>=ranges[i+1][0] for i in range(len(ranges)-1)) or sum(e-s for s,e in ranges)!=r['planned_bytes'] or r['planned_bytes']>16773120:raise ValueError('fixed physical plan')
            positions=np.concatenate([np.arange(s//780,e//780) for s,e in ranges]);ids=sq8['id'][positions];returned=ids[np.lexsort((ids,values[positions]))[:100]].tolist()
            states=dict(seed={i for i in gt if int(unit_of[i]) in seed},walk={i for i in gt if int(unit_of[i]) in walk},candidate={i for i in gt if int(page_of[i]) in ranked},nominated={i for i in gt if int(page_of[i]) in selected},physical=gt&set(ids.tolist()),returned=gt&set(returned),flat=gt&set(flat))
            states['visited']=states['seed']|states['walk']
            if not states['nominated']<=states['candidate'] or not states['nominated']<=states['physical'] or not states['returned']<=states['physical']:raise ValueError('stage authority')
            stages[arm]=states;samples[arm].append(dict(query_ordinal=q,**{k+'_hits':len(v) for k,v in states.items()},gets=len(ranges),bytes=r['planned_bytes'],returned_ids=returned,flat_ids=flat))
        decomposition.append(dict(query_ordinal=q,stages={arm:{k:sorted(v) for k,v in states.items()} for arm,states in stages.items()},gained_lost={k:dict(gained=sorted(stages['candidate'][k]-stages['control'][k]),lost=sorted(stages['control'][k]-stages['candidate'][k])) for k in stages['control']}))
    metrics={arm:{field:dict(total=sum(s[field] for s in values),mean=sum(s[field] for s in values)/744,p05=sorted(s[field] for s in values)[math.ceil(.05*744)-1]) for field in [key+'_hits' for key in ['seed','walk','visited','candidate','nominated','physical','returned','flat']]} for arm,values in samples.items()}
    m=metrics['candidate'];c=metrics['control'];passed=m['returned_hits']['mean']>=98 and m['returned_hits']['p05']>=95 and m['flat_hits']['mean']-m['returned_hits']['mean']<=.5 and m['returned_hits']['total']>=c['returned_hits']['total'] and m['candidate_hits']['total']>c['candidate_hits']['total']
    loss=dict(discovery=74400-m['candidate_hits']['total'],nomination=m['candidate_hits']['total']-m['nominated_hits']['total'],physical_gap=0,extra_fetched_gt=m['physical_hits']['total']-m['nominated_hits']['total'],fetched_not_returned=m['physical_hits']['total']-m['returned_hits']['total'],exhaustive_sq8_missing=74400-m['flat_hits']['total'])
    write(directory/'decomposition.json',decomposition)
    result=dict(dataset=item['name'],rows=100000,dimensions=768,metric='cosine',k=100,split='consumed method-validation256-999',queries=744,decision='GO method-validation only' if passed else 'KILL fixed quality gate',qualification=False,metrics=metrics,samples=samples,stage_loss=loss,scoring_wall_seconds=time.monotonic()-scoring_start,source_roots=item['roots'],normal_plans_sha256={arm:plan_sha([plan(r) for r in plans[arm]]) for arm in roots},native_planner_calls=1616,new_exhaustive_sq8_kernel_calls=746,physical_reads_measured=False)
    write(directory/'result.json',result);return result

def main():
    path,digest,repo,out,binaries,prefix=sys.argv[1:];repo,out,binaries=map(Path,[repo,out,binaries]);config=json.loads(Path(path).read_text())
    if sha(path)!=digest or config['schema']!='borsuk-native-union-validation-v1' or (config['first'],config['count'],config['batch_rows'])!=(256,744,2048) or config['gate']!=dict(min_mean_recall=98,min_p05=95,max_flat_deficit_pp=.5,max_gets=32,max_bytes=16773120) or [i['name'] for i in config['items']]!=['relaion','cohere'] or np.__version__!='2.3.3':raise ValueError('frozen config')
    for name,digest in config['scorer_hashes'].items():
        if sha(repo/name)!=digest:raise ValueError('scorer bytes')
    out.mkdir();results=[]
    for item in config['items']:
        result=evaluate(item,out/item['name'],config,binaries);results.append(result)
        if result['decision'].startswith('KILL'):break
    write(out/'decision.json',dict(decision=results[-1]['decision'],results=results,qualification=False,live_or_scale_run=False))

if __name__=='__main__':
    try:main()
    finally:
        group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
        if len(sys.argv)==7 and Path(sys.argv[4]).is_dir() and (group/'memory.peak').exists():write(Path(sys.argv[4])/'cgroup.json',{k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()})
