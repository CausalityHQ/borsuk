"""Replay closed discovery, source-nominate once, enforce unchanged development gates."""
import hashlib,json,struct,subprocess,sys
from pathlib import Path
import numpy as np
from scripts.native_two_bit_topology import quality_gate,validate_paired_roster,PAGE_BYTES,OBJECT_BYTES

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(4<<20),b''):h.update(b)
    return h.hexdigest()

def write(path,value):
    with Path(path).open('x') as f:json.dump(value,f,sort_keys=True,indent=2,allow_nan=False);f.write('\n')

def evaluate(item,directory,config,binary):
    directory.mkdir()
    for role,identity in item['artifacts'].items():
        p=directory/role;p.parent.mkdir(parents=True,exist_ok=True)
        subprocess.run(['aws','s3','cp','s3://'+config['bucket']+'/'+identity['key'],str(p),'--only-show-errors'],check=True)
        if p.stat().st_size!=identity['bytes'] or sha(p)!=identity['sha256']:raise ValueError('closed asset '+role)
    traces=[json.loads(s) for s in (directory/'paired-plans.jsonl').read_text().splitlines()]
    validate_paired_roster(traces,item['roots'])
    args=[binary,directory/'control',item['roots']['control'],directory/'candidate',item['roots']['candidate'],directory/'requests',item['artifacts']['requests']['sha256'],directory/'paired-plans.jsonl',item['artifacts']['paired-plans.jsonl']['sha256'],directory/'nomination.jsonl']
    with (directory/'nomination.log').open('x') as f:
        subprocess.run(['/usr/bin/time','-v','-o',str(directory/'nomination.time'),'timeout','--signal=TERM','--kill-after=10','300',*map(str,args)],check=True,stdout=f,stderr=subprocess.STDOUT)
    records=[json.loads(s) for s in (directory/'nomination.jsonl').read_text().splitlines()]
    expected=[(i,arm) for i in range(64) for arm in (['control','union'] if i%2==0 else ['union','control'])]
    if len(records)!=128 or [(r['query_ordinal'],r['arm']) for r in records]!=expected:raise ValueError('native roster')
    plans=[json.loads(s) for s in (directory/'preflight-plans.jsonl').read_text().splitlines()]
    authority=json.loads((directory/'authority.json').read_text())['preflight']
    scores=np.load(directory/'scores.npy',mmap_mode='r',allow_pickle=False)
    if scores.shape!=(64,100000) or scores.dtype!=np.dtype('<f4'):raise ValueError('score cache geometry')
    sq8=np.memmap(directory/'sq8.bin',mode='r',dtype=[('id','<i8'),('norm','<f4'),('code','u1',(768,))])
    if sq8.shape!=(100000,) or not np.array_equal(np.sort(sq8['id']),np.arange(100000)):raise ValueError('signed ID geometry')
    page_of=np.empty(100000,dtype=np.int64);page_of[sq8['id']]=np.arange(100000)//256
    truth=np.array(struct.unpack('<100000I',(directory/'truth').read_bytes()),dtype=np.int64).reshape(1000,100)
    samples={'control':[],'union':[]};decomposition=[];normal=[]
    for r in records:
        i,arm=r['query_ordinal'],r['arm'];gt=set(truth[i].tolist())
        if len(gt)!=100 or any(not 0<=g<100000 for g in gt):raise ValueError('truth identity')
        pair={t['arm']:t for t in traces[i*2:i*2+2]}
        candidates=r['ranked_candidate_pages'];selected=r['selected_pages']
        wanted=set(pair['control']['ranked_candidate_pages'])
        if arm=='union':wanted.update(pair['candidate']['ranked_candidate_pages'])
        if len(candidates)!=len(set(candidates)) or set(candidates)!=wanted or r['primary_page']!=candidates[0] or not set(selected)<=wanted:raise ValueError('discovery/nomination authority')
        fields={k:r[k] for k in ['query_ordinal','ranges','planned_bytes']}
        if arm=='control':
            if fields!=plans[i] or candidates!=pair['control']['ranked_candidate_pages']:raise ValueError('native control nomination parity')
            normal.append(fields)
        ranges=r['ranges'];previous=-1;positions=[];physical_pages=set()
        if not 0<len(ranges)<=32:raise ValueError('range count')
        for start,end in ranges:
            if type(start) is not int or type(end) is not int or not 0<=start<end<=OBJECT_BYTES or start%PAGE_BYTES or (end%PAGE_BYTES and end!=OBJECT_BYTES) or start<=previous:raise ValueError('range geometry')
            previous=end;positions.append(np.arange(start//780,end//780));physical_pages.update(range(start//PAGE_BYTES,(end-1)//PAGE_BYTES+1))
        if r['planned_bytes']!=sum(e-s for s,e in ranges) or r['planned_bytes']>16773120 or not set(selected)<=physical_pages:raise ValueError('physical cap/nomination')
        positions=np.concatenate(positions);ids=sq8['id'][positions]
        returned=ids[np.lexsort((ids,scores[i,positions]))[:100]].tolist()
        flat=sq8['id'][np.lexsort((sq8['id'],scores[i]))[:100]].tolist()
        stages=dict(candidate=sorted(g for g in gt if page_of[g] in wanted),nominated=sorted(g for g in gt if page_of[g] in set(selected)),physical=sorted(g for g in gt if page_of[g] in physical_pages),returned=sorted(gt&set(returned)),flat=sorted(gt&set(flat)))
        sample=dict(query_ordinal=i,candidate_hits=len(stages['candidate']),fetched_hits=len(stages['physical']),returned_hits=len(stages['returned']),flat_hits=len(stages['flat']),gets=len(ranges),bytes=r['planned_bytes'])
        if arm=='control' and {k:v for k,v in sample.items() if k!='candidate_hits'}!=authority[i]:raise ValueError('cached control quality parity')
        samples[arm].append(sample);decomposition.append(dict(query_ordinal=i,arm=arm,stages=stages,returned_ids=returned))
        if any(type(r[k]) is not int or r[k]<0 for k in ['nomination_wall_ns','nomination_process_cpu_ns']):raise ValueError('clock')
    # Exact serialized normal-plan identity, as emitted by the original native demo.
    text=''.join(json.dumps(p,sort_keys=True,separators=(',',':'))+'\n' for p in normal)
    if hashlib.sha256(text.encode()).hexdigest()!=item['artifacts']['preflight-plans.jsonl']['sha256']:raise ValueError('whole control plan hash')
    passed=quality_gate(item['name'],samples['control'],samples['union'])
    metrics={arm:{field:dict(total=sum(s[field] for s in rows),mean=sum(s[field] for s in rows)/64,p05=sorted(s[field] for s in rows)[3]) for field in ['candidate_hits','fetched_hits','returned_hits','flat_hits']} for arm,rows in samples.items()}
    timings={arm:{k:dict(zip(['p50','p90','p95','p99'],np.quantile([r[k]/1e6 for r in records if r['arm']==arm],[.5,.9,.95,.99]).tolist())) for k in ['nomination_wall_ns','nomination_process_cpu_ns']} for arm in samples}
    write(directory/'decomposition.json',decomposition)
    result=dict(dataset=item['name'],split='consumed development0-63',rows=100000,dimensions=768,k=100,metric='cosine',decision='GO' if passed else 'KILL',metrics=metrics,samples=samples,timings_ms_nomination_only=timings,native_nomination_calls=128,shared_query_preparations=64,new_graph_queries=0,new_exhaustive_sq8_kernel_calls=0,cached_sq8_sort_calls=256,logical_ranges_not_physical_gets=True,qualification=False)
    write(directory/'result.json',result);return result

def main():
    config_path,digest,repo,out,binary=sys.argv[1:];repo,out=Path(repo),Path(out)
    if sha(config_path)!=digest:raise ValueError('config identity')
    config=json.loads(Path(config_path).read_text())
    if config['schema']!='borsuk-union-nomination-v1' or [i['name'] for i in config['items']]!=['relaion','cohere'] or (config['rows'],config['dimensions'],config['k'],config['first'],config['count'],config['metric'])!=(100000,768,100,0,64,'cosine') or np.__version__!='2.3.3':raise ValueError('frozen scope')
    for name,digest in config['scorer_hashes'].items():
        if sha(repo/name)!=digest:raise ValueError('frozen scorer')
    if sha(repo/'scripts/native_two_bit_topology.py')!=config['helper_sha256']:raise ValueError('frozen gates')
    out.mkdir();results=[]
    for item in config['items']:
        result=evaluate(item,out/item['name'],config,binary);results.append(result)
        if result['decision']=='KILL':break
    write(out/'decision.json',dict(decision='KILL' if results[-1]['decision']=='KILL' else 'GO nomination only',results=results,validation_or_scale_run=False,qualification=False))

if __name__=='__main__':
    try:main()
    finally:
        group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
        if len(sys.argv)==6 and Path(sys.argv[4]).is_dir() and (group/'memory.peak').exists():write(Path(sys.argv[4])/'cgroup.json',{k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()})
