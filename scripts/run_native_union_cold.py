"""Frozen same-binary ABBA client-cold S3 comparison; no vendor qualification."""
import hashlib,json,struct,subprocess,sys
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scripts.run_native_cold import sha,write
from scripts.native_two_bit_topology import quality_gate

METADATA=['manifest.json','page_manifest.json','page_digests.bin','centroids.bin','graph.bin','diverse_graph.bin','plane/manifest.json','plane/mean.bin','plane/records.bin']

def native(directory,phase,args,cap=300):
    with (directory/(phase+'.log')).open('x') as log:
        subprocess.run(['/usr/bin/time','-v','-o',str(directory/(phase+'.time')),'timeout','--signal=TERM','--kill-after=10',str(cap),*map(str,args)],check=True,stdout=log,stderr=subprocess.STDOUT)

def plan(r):return {k:r[k] for k in ['query_ordinal','ranges','planned_bytes']}
def plan_sha(plans):return hashlib.sha256(''.join(json.dumps(p,sort_keys=True,separators=(',',':'))+'\n' for p in plans).encode()).hexdigest()

def evaluate(item,directory,config,repo,binaries,prefix):
    directory.mkdir()
    for role,ident in item['artifacts'].items():
        p=directory/role;p.parent.mkdir(parents=True,exist_ok=True)
        subprocess.run(['aws','s3','cp','s3://'+config['bucket']+'/'+ident['key'],str(p),'--only-show-errors'],check=True)
        if p.stat().st_size!=ident['bytes'] or sha(p)!=ident['sha256']:raise ValueError('closed asset '+role)
    original=json.loads((directory/'control/manifest.json').read_text())
    raw=directory/'source'
    if item['name']=='relaion':
        raw=directory/'vectors.raw';remaining=100000
        with raw.open('xb') as f:
            for batch in pq.ParquetFile(directory/'source').iter_batches(batch_size=8192,columns=['embedding']):
                column=batch.column(0)
                if not pa.types.is_fixed_size_list(column.type) or column.type.list_size!=768 or column.type.value_type!=pa.float32() or column.null_count or column.values.null_count:raise ValueError('source parquet geometry')
                rows=min(remaining,len(column));values=np.asarray(column.values.to_numpy(zero_copy_only=False),dtype='<f4').reshape(len(column),768)[:rows]
                if not np.isfinite(values).all():raise ValueError('finite source')
                f.write(values.tobytes());remaining-=rows
                if not remaining:break
        if remaining:raise ValueError('source rows')
    if raw.stat().st_size!=307200000 or sha(raw)!=item['raw_sha256']:raise ValueError('original raw source identity')
    sq8_key=prefix+'/data/'+item['name']+'/objects/'+original['sq8_object_sha256']
    subprocess.run(['aws','s3api','put-object','--bucket',config['bucket'],'--key',sq8_key,'--body',str(directory/'sq8.bin'),'--if-none-match','*','--metadata','sha256='+original['sq8_object_sha256']],check=True,stdout=subprocess.DEVNULL)
    head=json.loads(subprocess.check_output(['aws','s3api','head-object','--bucket',config['bucket'],'--key',sq8_key]))
    if head['ContentLength']!=78000000 or head['Metadata']['sha256']!=original['sq8_object_sha256']:raise ValueError('SQ8 HEAD identity')
    build=dict(raw=str(raw),raw_sha256=item['raw_sha256'],sq8=str(directory/'sq8.bin'),sq8_sha256=original['sq8_object_sha256'],rows=100000,dimensions=768,generation=1,base_epoch=0,low=original['low'],step=original['step'],sq8_object_key=sq8_key,sq8_etag=head['ETag'])
    write(directory/'builder.json',build)
    candidate=directory/'native-candidate'
    native(directory,'build',[binaries/'build_two_bit_generation',directory/'builder.json',sha(directory/'builder.json'),268435456,candidate])
    body=(candidate/'manifest.json').read_bytes();candidate_sha=sha(candidate/'manifest.json');root=json.loads(body)
    if root['schema']!='borsuk-two-bit-generation-v4' or root['sq8_object_key']!=sq8_key or root['sq8_etag']!=head['ETag']:raise ValueError('new native root authority')
    for field in ['low','step']:
        if struct.pack('<768f',*root[field])!=struct.pack('<768f',*original[field]):raise ValueError('SQ8 coefficient bits')
    for name,ident in item['expected_components'].items():
        p=candidate/name
        if p.stat().st_size!=ident['bytes'] or sha(p)!=ident['sha256']:raise ValueError('native component parity '+name)
    control=directory/'native-control';control.mkdir();text=body.decode()
    for field,replacement in [('diverse_graph_sha256',root['graph_sha256']),('diverse_graph_resident_bytes',root['graph_resident_bytes'])]:
        old='"'+field+'":'+json.dumps(root[field],separators=(',',':'));new='"'+field+'":'+json.dumps(replacement,separators=(',',':'))
        if text.count(old)!=1:raise ValueError('control binding token')
        text=text.replace(old,new,1)
    for name in METADATA[1:]+['canonical.bin']:
        p=control/name;p.parent.mkdir(parents=True,exist_ok=True);p.hardlink_to(candidate/('graph.bin' if name=='diverse_graph.bin' else name))
    (control/'manifest.json').write_text(text);control_sha=sha(control/'manifest.json')
    write(directory/'binding.json',dict(control_root_sha256=control_sha,candidate_root_sha256=candidate_sha,sq8_object_key=sq8_key,sq8_etag=head['ETag'],canonical_object_key=root['canonical']['object_key'],only_control_graph_identity_changed=True))
    request_sha=item['artifacts']['requests']['sha256'];binary=binaries/'two_bit_plan_demo'
    native(directory,'paired',[binary,control,control_sha,directory/'requests',request_sha,directory/'native-paired.jsonl',0,64,'--trace','--paired',candidate,candidate_sha])
    paired=[json.loads(s) for s in (directory/'native-paired.jsonl').read_text().splitlines()]
    if len(paired)!=128:raise ValueError('native paired roster')
    frozen=[json.loads(s) for s in (directory/'paired-plans.jsonl').read_text().splitlines()]
    nomination=[json.loads(s) for s in (directory/'union/nomination.jsonl').read_text().splitlines()]
    plans={arm:[] for arm in ['control','candidate']}
    for q in range(64):
        old={r['arm']:r for r in frozen[q*2:q*2+2]};union=next(r for r in nomination[q*2:q*2+2] if r['arm']=='union')
        for position,r in enumerate(paired[q*2:q*2+2]):
            arm=(['control','candidate'] if q%2==0 else ['candidate','control'])[position]
            if (r['query_ordinal'],r['arm'],r['root_sha256'])!=(q,arm,control_sha if arm=='control' else candidate_sha):raise ValueError('paired ordinal/arm/root')
            expected=old['control'] if arm=='control' else union
            if any(r[k]!=expected[k] for k in ['ranges','planned_bytes','ranked_candidate_pages','selected_pages','primary_page']):raise ValueError('native nomination parity')
            wanted=[old['control']] if arm=='control' else [old['control'],old['candidate']]
            if len(r['discoveries'])!=len(wanted):raise ValueError('native discovery count')
            for discovery,previous in zip(r['discoveries'],wanted):
                if any(discovery[k]!=previous[k] for k in ['seed_page','seed_evaluated_units','walk_evaluated_units','seed_work_exhausted','walk_work_exhausted']):raise ValueError('native graph work parity')
            plans[arm].append(plan(r))
    if plan_sha(plans['control'])!=item['expected_control_plans_sha256'] or plan_sha(plans['candidate'])!=item['expected_union_plans_sha256']:raise ValueError('whole native plan hashes')
    scores=np.load(directory/'scores.npy',mmap_mode='r',allow_pickle=False)
    if scores.shape!=(64,100000) or scores.dtype!=np.dtype('<f4'):raise ValueError('score cache geometry')
    sq8=np.memmap(directory/'sq8.bin',mode='r',dtype=[('id','<i8'),('norm','<f4'),('code','u1',(768,))]);truth=np.frombuffer((directory/'truth').read_bytes(),dtype='<u4').reshape(1000,100)
    if sq8.shape!=(100000,) or not np.array_equal(np.sort(sq8['id']),np.arange(100000)):raise ValueError('signed ID roster')
    expected_ids={arm:[] for arm in plans}
    for arm in plans:
        for q,p in enumerate(plans[arm]):
            positions=np.concatenate([np.arange(s//780,e//780) for s,e in p['ranges']]);ids=sq8['id'][positions]
            expected_ids[arm].append(ids[np.lexsort((ids,scores[q,positions]))[:100]].tolist())
    reference=json.loads((directory/'union/result.json').read_text())['samples'];runs=[]
    for rep,arm in enumerate(config['arm_order']):
        run=directory/('run'+str(rep)+'-'+arm);run.mkdir();local=control if arm=='control' else candidate;root_sha=control_sha if arm=='control' else candidate_sha
        native(run,'live',[binary,local,root_sha,directory/'requests',request_sha,run/'live.jsonl',0,64,'--live-s3',config['bucket'],'eu-central-1',prefix+'/runs/'+item['name']+'/'+str(rep)+'-'+arm])
        records=[json.loads(s) for s in (run/'live.jsonl').read_text().splitlines()]
        if len(records)!=66 or records[0]['phase']!='startup' or records[-1]['phase']!='summary' or records[0]['root_sha256']!=root_sha or records[0]['generation']!=1 or records[0]['control_epoch']!=1 or records[-1]['count']!=64:raise ValueError('native live authority/roster')
        samples=[]
        for q,r in enumerate(records[1:-1]):
            if r['phase']!='query' or plan(r)!=plans[arm][q] or r['ids']!=expected_ids[arm][q]:raise ValueError('native returned-ID/plan parity')
            p=plans[arm][q]
            if r['submitted_gets']!=len(p['ranges']) or r['verified_bytes']!=p['planned_bytes'] or r['failed_gets'] or r['submitted_gets']>32 or r['verified_bytes']>16773120:raise ValueError('actual physical accounting')
            gt=set(truth[q].tolist());sample=reference['control' if arm=='control' else 'union'][q]
            if len(gt)!=100 or len(set(r['ids'])&gt)!=sample['returned_hits']:raise ValueError('native GT parity')
            samples.append(sample)
        if sum(s['returned_hits'] for s in samples)!=item['expected_returned_hits'][0 if arm=='control' else 1] or sorted(s['returned_hits'] for s in samples)[3]!=item['expected_p05_hits'][0 if arm=='control' else 1]:raise ValueError('quality parity')
        percentiles=lambda key:dict(zip(['p50','p90','p95','p99'],np.quantile([r[key]/1e6 for r in records[1:-1]],[.5,.9,.95,.99]).tolist()))
        result=dict(rep=rep,arm=arm,returned_hits=sum(s['returned_hits'] for s in samples),samples=samples,complete_library_call_latency_ms=percentiles('query_wall_ns'),process_cpu_ms=percentiles('query_process_cpu_ns'),serial_observed_qps=64/(records[-1]['measurement_wall_ns']/1e9),measurement_wall_ns=records[-1]['measurement_wall_ns'],data_get_attempts=sum(r['submitted_gets'] for r in records[1:-1]),verified_data_bytes=sum(r['verified_bytes'] for r in records[1:-1]),failed_data_gets=0,startup=records[0])
        write(run/'result.json',result);runs.append(result)
    if not quality_gate(item['name'],reference['control'],reference['union']):raise ValueError('frozen quality gates')
    medians={arm:{key:float(np.median([r['complete_library_call_latency_ms'][key] for r in runs if r['arm']==arm])) for key in ['p50','p90','p95','p99']} for arm in plans}
    passed=medians['candidate']['p90']<=250 and medians['candidate']['p95']<=400
    result=dict(dataset=item['name'],split='consumed development0-63',rows=100000,dimensions=768,k=100,metric='cosine',decision='GO native development envelope only' if passed else 'KILL native latency envelope',runs=runs,arm_median_complete_call_ms=medians,native_library_search_calls=256,native_offline_planner_calls=128,cached_sq8_sort_parity_queries=128,new_exhaustive_sq8_kernel_calls=0,qualification=False,cache_state='no client SQ8 cache; router resident; connections reused within process; S3 server cache uncontrolled',incoming_service_http_measured=False)
    write(directory/'result.json',result);return result

def main():
    config_path,digest,repo,out,binaries,prefix=sys.argv[1:];repo,out,binaries=map(Path,[repo,out,binaries])
    if sha(config_path)!=digest:raise ValueError('config identity')
    config=json.loads(Path(config_path).read_text())
    if config['schema']!='borsuk-native-union-cold-v1' or config['arm_order']!=['control','candidate','candidate','control'] or [i['name'] for i in config['items']]!=['relaion','cohere'] or (config['rows'],config['dimensions'],config['k'],config['first'],config['count'],config['metric'])!=(100000,768,100,0,64,'cosine') or np.__version__!='2.3.3' or pa.__version__!='24.0.0':raise ValueError('frozen scope/dependencies')
    for name,digest in config['scorer_hashes'].items():
        if sha(repo/name)!=digest:raise ValueError('frozen scorer')
    out.mkdir();results=[]
    for item in config['items']:
        result=evaluate(item,out/item['name'],config,repo,binaries,prefix);results.append(result)
        if result['decision'].startswith('KILL'):break
    write(out/'decision.json',dict(decision=results[-1]['decision'],results=results,qualification=False,validation_or_scale_run=False))

if __name__=='__main__':
    try:main()
    finally:
        group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
        if len(sys.argv)==7 and Path(sys.argv[4]).is_dir() and (group/'memory.peak').exists():write(Path(sys.argv[4])/'cgroup.json',{k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()})
