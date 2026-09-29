"""Same-root source-priority quality and incoming HTTP against current integrated core."""
import json,struct,subprocess,sys
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
from scripts.run_native_union_cold import native,plan,plan_sha,sha,write
from scripts.run_native_union_http import run as http_run

def evaluate(item,directory,config,binaries,prefix):
    directory.mkdir()
    for role,ident in item['artifacts'].items():
        p=directory/role;p.parent.mkdir(parents=True,exist_ok=True)
        subprocess.run(['aws','s3','cp','s3://'+config['bucket']+'/'+ident['key'],str(p),'--only-show-errors'],check=True)
        if p.stat().st_size!=ident['bytes'] or sha(p)!=ident['sha256']:raise ValueError('frozen input '+role)
    control=directory/'control';root=json.loads((control/'manifest.json').read_text());raw=directory/'source'
    if item['name']=='relaion':
        raw=directory/'source.raw';remaining=100000
        with raw.open('xb') as f:
            for batch in pq.ParquetFile(directory/'source').iter_batches(batch_size=8192,columns=['embedding']):
                col=batch.column(0)
                if not pa.types.is_fixed_size_list(col.type) or col.type.list_size!=768 or col.type.value_type!=pa.float32() or col.null_count or col.values.null_count:raise ValueError('original source geometry')
                values=np.asarray(col.values.slice(col.offset*768,len(col)*768).to_numpy(zero_copy_only=False),dtype='<f4').reshape(len(col),768)[:remaining]
                if not np.isfinite(values).all():raise ValueError('finite original source')
                f.write(values.tobytes());remaining-=len(values)
                if not remaining:break
        if remaining:raise ValueError('source rows')
    if raw.stat().st_size!=307200000 or sha(raw)!=item['raw_sha256'] or sha(control/'manifest.json')!=item['control_root_sha256']:raise ValueError('source/control identity')
    fit=binaries/'build_sq8_source';normalized=directory/'normalized.raw';order=directory/'order.u64';new_sq8=directory/'candidate-sq8.bin'
    native(directory,'normalize',[fit,'normalize',raw,item['raw_sha256'],100000,768,1073741824,normalized])
    if 'closed-builder.json' not in item['artifacts']:native(directory,'hier-fit',[fit,'hier-fit',normalized,sha(normalized),100000,768,1073741824,order])
    recipe=json.loads((directory/'hier-fit.log').read_text());values=np.fromfile(order,dtype='<u8')
    if values.shape!=(100000,) or not np.array_equal(np.sort(values),np.arange(100000)) or recipe['order_sha256']!=sha(order) or recipe['query_or_truth_used'] is not False or recipe['recipe']!=config['source_recipe'] or recipe['extents'][0][0]!=0 or recipe['extents'][-1][1]!=100000 or any(not 0<b-a<=1024 for a,b in recipe['extents']) or any(recipe['extents'][i][1]!=recipe['extents'][i+1][0] for i in range(len(recipe['extents'])-1)):raise ValueError('source-only hierarchical order authority')
    native(directory,'sq8',[fit,normalized,sha(normalized),768,order,sha(order),1073741824,new_sq8])
    sq8_receipt=json.loads((directory/'sq8.log').read_text())
    for field in ['low','step']:
        if struct.pack('<768f',*sq8_receipt[field])!=struct.pack('<768f',*root[field]):raise ValueError('source coefficient parity')
    dtype=np.dtype([('id','<i8'),('norm','<f4'),('code','u1',(768,))]);old=np.memmap(directory/'sq8.bin',mode='r',dtype=dtype);candidate_sq8=np.memmap(new_sq8,mode='r',dtype=dtype)
    if old.shape!=(100000,) or candidate_sq8.shape!=(100000,):raise ValueError('SQ8 geometry')
    if not np.array_equal(candidate_sq8['id'],values) or not np.array_equal(np.sort(old['id']),np.arange(100000)) or old[np.argsort(old['id'])].tobytes()!=candidate_sq8[np.argsort(candidate_sq8['id'])].tobytes():raise ValueError('per-ID exact SQ8 payload parity')
    sq8_sha=sha(new_sq8)
    if sq8_sha!=sq8_receipt['sq8_sha256']:raise ValueError('new source SQ8 identity')
    if 'closed-builder.json' in item['artifacts']:
        build=json.loads((directory/'closed-builder.json').read_text())
        if build['sq8_sha256']!=sq8_sha or build['low']!=sq8_receipt['low'] or build['step']!=sq8_receipt['step']:raise ValueError('exact old builder payload')
        sq8_key=build['sq8_object_key']
        head=json.loads(subprocess.check_output(['aws','s3api','head-object','--bucket',config['bucket'],'--key',sq8_key]))
        if head['ContentLength']!=78000000 or head['Metadata']['sha256']!=sq8_sha or head['ETag']!=build['sq8_etag']:raise ValueError('exact retained object HEAD')
        build.update(raw=str(raw),sq8=str(new_sq8))
    else:
        sq8_key=prefix+'/data/'+item['name']+'/objects/'+sq8_sha
        subprocess.run(['aws','s3api','put-object','--bucket',config['bucket'],'--key',sq8_key,'--body',str(new_sq8),'--if-none-match','*','--metadata','sha256='+sq8_sha],check=True,stdout=subprocess.DEVNULL)
        head=json.loads(subprocess.check_output(['aws','s3api','head-object','--bucket',config['bucket'],'--key',sq8_key]))
        if head['ContentLength']!=78000000 or head['Metadata']['sha256']!=sq8_sha:raise ValueError('new CoHere object HEAD')
        build=dict(raw=str(raw),raw_sha256=item['raw_sha256'],sq8=str(new_sq8),sq8_sha256=sq8_sha,rows=100000,dimensions=768,generation=1,base_epoch=0,low=sq8_receipt['low'],step=sq8_receipt['step'],sq8_object_key=sq8_key,sq8_etag=head['ETag'])
    candidate=directory/'candidate';write(directory/'builder.json',build)
    native(directory,'build',[binaries/'build_two_bit_generation',directory/'builder.json',sha(directory/'builder.json'),268435456,candidate])
    candidate_sha=sha(candidate/'manifest.json');new_root=json.loads((candidate/'manifest.json').read_text())
    if new_root['schema']!='borsuk-two-bit-generation-v4' or new_root['sq8_object_key']!=sq8_key or new_root['sq8_etag']!=head['ETag'] or sha(candidate/'plane/mean.bin')!=sha(control/'plane/mean.bin'):raise ValueError('native candidate binding/mean parity')
    old_records=np.memmap(control/'plane/records.bin',mode='r',dtype=np.dtype('V200'));new_records=np.memmap(candidate/'plane/records.bin',mode='r',dtype=np.dtype('V200'))
    if old_records[np.argsort(old['id'])].tobytes()!=new_records[np.argsort(candidate_sq8['id'])].tobytes():raise ValueError('per-ID exact source two-bit row parity')
    write(directory/'binding.json',dict(control_root_sha256=item['control_root_sha256'],candidate_root_sha256=candidate_sha,per_id_sq8_and_two_bit_payload_exact=True,coefficient_f32_bits_exact=True,source_order_sha256=sha(order),source_only_recipe=recipe,sq8_object_key=sq8_key,sq8_etag=head['ETag']))
    if 'closed-manifest.json' in item['artifacts'] and (candidate/'manifest.json').read_bytes()!=(directory/'closed-manifest.json').read_bytes():raise ValueError('exact reconstructed root')
    binary=binaries/'old_two_bit_plan_demo';request_sha=item['artifacts']['requests']['sha256']
    native(directory,'paired',[binary,control,item['control_root_sha256'],directory/'requests',request_sha,directory/'paired.jsonl',0,64,'--trace','--paired',candidate,candidate_sha])
    records=[json.loads(s) for s in (directory/'paired.jsonl').read_text().splitlines()]
    if len(records)!=128:raise ValueError('paired roster')
    closed=[json.loads(line) for line in (directory/'closed-paired.jsonl').read_text().splitlines()]
    parity=['query_ordinal','root_sha256','ranges','planned_bytes','discoveries','ranked_candidate_pages','selected_pages','primary_page']
    expected=[next(row for row in closed[q*2:q*2+2] if row['arm']=='candidate') for q in range(64)]
    if len(closed)!=128 or any(any(r[k]!=expected[q][k] for k in parity) for q in range(64) for r in records[q*2:q*2+2]):raise ValueError('exact closed CURRENT integrated-core control parity')
    native(directory,'core-plan',[binaries/'two_bit_plan_demo',candidate,candidate_sha,directory/'requests',request_sha,directory/'core-plan.jsonl',0,64,'--trace'])
    candidate_plans=[json.loads(s) for s in (directory/'core-plan.jsonl').read_text().splitlines()]
    if len(candidate_plans)!=64:raise ValueError('actual core plan roster')
    for q,r in enumerate(candidate_plans):
        if r['query_ordinal']!=q:raise ValueError('actual core ordinal')
        r.update(arm='candidate',root_sha256=candidate_sha)
        records[q*2+(1 if q%2==0 else 0)]=r
    write(directory/'preflight.json',dict(old_control_binary=True,new_candidate_binary=True,exact_current_control=True,candidate_root_sha256=candidate_sha))
    with (directory/'actual-paired.jsonl').open('x') as f:
        for r in records:f.write(json.dumps(r)+'\n')
    reference=[json.loads(s) for s in (directory/'reference-paired.jsonl').read_text().splitlines()]
    old_scores=np.load(directory/'scores.npy',mmap_mode='r',allow_pickle=False)
    if old_scores.shape!=(64,100000) or old_scores.dtype!=np.dtype('<f4'):raise ValueError('cache authority')
    cache_order=np.memmap(directory/'cache-sq8.bin',mode='r',dtype=dtype)
    if cache_order.shape!=(100000,) or not np.array_equal(np.sort(cache_order['id']),np.arange(100000)):raise ValueError('cached score physical ID authority')
    by_id=np.empty_like(old_scores);by_id[:,cache_order['id']]=old_scores;truth=np.fromfile(directory/'truth',dtype='<u4').reshape(1000,100)
    samples={a:[] for a in ['control','candidate']};expected_ids={a:[] for a in samples};plans={a:[] for a in samples}
    for q in range(64):
        old_trace=next(r for r in reference[q*2:q*2+2] if r['arm']=='candidate')
        for position,r in enumerate(records[q*2:q*2+2]):
            arm=(['control','candidate'] if q%2==0 else ['candidate','control'])[position]
            if r['query_ordinal']!=q or r['arm']!=arm or r['root_sha256']!=(item['control_root_sha256'] if arm=='control' else candidate_sha):raise ValueError('paired identity')
            if arm=='control' and any(r[k]!=old_trace[k] for k in ['ranges','planned_bytes','discoveries','ranked_candidate_pages','selected_pages','primary_page']):raise ValueError('exact current union control trace parity')
            physical=old if arm=='control' else candidate_sq8;p=plan(r);positions=np.concatenate([np.arange(s//780,e//780) for s,e in p['ranges']]);ids=physical['id'][positions];scores=by_id[q,ids];returned=ids[np.lexsort((ids,scores))[:100]].tolist();gt=set(truth[q].tolist());flat_ids=old['id'][np.lexsort((old['id'],by_id[q,old['id']]))[:100]]
            pages={int(i):pos//256 for pos,i in enumerate(physical['id'])};candidate_gt={i for i in gt if pages[i] in r['ranked_candidate_pages']};nominated_gt={i for i in gt if pages[i] in r['selected_pages']};fetched_gt=gt&set(ids.tolist());returned_gt=gt&set(returned);flat_gt=gt&set(flat_ids.tolist())
            if not nominated_gt<=candidate_gt or not nominated_gt<=fetched_gt or not returned_gt<=fetched_gt or not 0<len(p['ranges'])<=32 or p['planned_bytes']>16773120:raise ValueError('stage/physical authority')
            samples[arm].append(dict(query_ordinal=q,candidate_hits=len(candidate_gt),nominated_hits=len(nominated_gt),fetched_hits=len(fetched_gt),returned_hits=len(returned_gt),flat_hits=len(flat_gt),gets=len(p['ranges']),bytes=p['planned_bytes'],stages=dict(candidate=sorted(candidate_gt),nominated=sorted(nominated_gt),fetched=sorted(fetched_gt),returned=sorted(returned_gt),flat=sorted(flat_gt))))
            expected_ids[arm].append(returned);plans[arm].append(p)
    if plan_sha(plans['control'])!=item['expected_control_plans_sha256'] or sum(s['returned_hits'] for s in samples['control'])!=item['control_returned_hits'] or sum(s['flat_hits'] for s in samples['control'])!=item['control_flat_hits']:raise ValueError('current control metrics/plan authority')
    hits=sum(s['returned_hits'] for s in samples['candidate']);p05=sorted(s['returned_hits'] for s in samples['candidate'])[3];passed=hits/64>=98 and p05>=95 and item['control_flat_hits']-hits<=32 and hits>=item['control_returned_hits']
    result=dict(dataset=item['name'],decision='GO actual core development quality only' if passed else 'KILL actual core quality',samples=samples,returned_hits={a:sum(s['returned_hits'] for s in samples[a]) for a in samples},p05={a:sorted(s['returned_hits'] for s in samples[a])[3] for a in samples},normal_plans_sha256={a:plan_sha(p) for a,p in plans.items()},qualification=False,expected_ids=expected_ids,source_recipe=recipe,per_id_source_and_scorer_parity=True,new_exhaustive_sq8_kernel_calls=0,cached_sort_queries=128,physical_cold_measured=False)
    write(directory/'quality.json',result)
    return result

def main():
    path,digest,repo,out,binaries,prefix=sys.argv[1:];repo,out,binaries=map(Path,[repo,out,binaries]);config=json.loads(Path(path).read_text())
    if sha(path)!=digest or config['schema']!='borsuk-native-union-layout-transfer-v1' or config['arm_order']!=['control','candidate','candidate','control'] or [i['name'] for i in config['items']]!=['relaion','cohere'] or np.__version__!='2.3.3' or pa.__version__!='24.0.0':raise ValueError('frozen layout scope')
    for name,digest in (config['scorer_hashes']|config['controller_dependencies']).items():
        if sha(repo/name)!=digest:raise ValueError('frozen scorer')
    out.mkdir();results=[]
    for item in config['items']:
        result=evaluate(item,out/item['name'],config,binaries,prefix);results.append(result)
        if result['decision'].startswith('KILL'):break
    if all(r['decision'].startswith('GO') for r in results):
        for item,result in zip(config['items'],results):
            live_references={};directory=out/item['name'];binding=json.loads((directory/'binding.json').read_text());truth=np.fromfile(directory/'truth',dtype='<u4').reshape(1000,100)
            for arm in ['control','candidate']:
                live=directory/('native-'+arm);live.mkdir();binary=binaries/('old_two_bit_plan_demo' if arm=='control' else 'two_bit_plan_demo')
                index=prefix+'/indexes/'+item['name']+'/'+arm
                root_path=directory/arm;root_sha=binding[arm+'_root_sha256']
                native(live,'live',[binary,root_path,root_sha,directory/'requests',item['artifacts']['requests']['sha256'],live/'live.jsonl',0,64,'--live-s3',config['bucket'],config['region'],index])
                records=[json.loads(s) for s in (live/'live.jsonl').read_text().splitlines()]
                if len(records)!=66 or records[0]['root_sha256']!=root_sha or records[0]['generation']!=1 or records[0]['control_epoch']!=1 or records[-1]['count']!=64:raise ValueError('native reference head/roster')
                plans=[{k:r[k] for k in ['query_ordinal','ranges','planned_bytes']} for r in records[1:-1]]
                if plan_sha(plans)!=result['normal_plans_sha256'][arm]:raise ValueError('live core plan parity')
                for q,r in enumerate(records[1:-1]):
                    gt=set(truth[q].tolist())
                    if r['ids']!=result['expected_ids'][arm][q] or set(r['ids'])&gt!=set(result['samples'][arm][q]['stages']['returned']) or r['submitted_gets']!=len(r['ranges']) or r['verified_bytes']!=r['planned_bytes'] or r['failed_gets']:raise ValueError('native reference scorer/physical parity')
                live_references[arm]=records
            http_item=dict(item,root_sha256={a:binding[a+'_root_sha256'] for a in ['control','candidate']},index_prefix={a:prefix+'/indexes/'+item['name']+'/'+a for a in ['control','candidate']},returned_hits=result['returned_hits'],p05=result['p05'])
            requests=[json.loads(s) for s in (directory/'requests').read_text().splitlines()];gt=np.fromfile(directory/'truth',dtype='<u4').tolist()
            runs=[http_run(http_item,arm,rep,config,directory,binaries/('old_two_bit_http' if arm=='control' else 'two_bit_http'),requests,live_references[arm],gt,prefix) for rep,arm in enumerate(config['arm_order'])]
            from scripts.run_native_union_http import quantile
            medians={arm:{label:quantile([r['incoming_http_ms'][label] for r in runs if r['arm']==arm],.5) for label in ['p50','p90','p95','p99']} for arm in ['control','candidate']}
            result.update(runs=runs,arm_median_incoming_http_ms=medians,arm_median_serial_qps={arm:quantile([r['serial_observed_qps'] for r in runs if r['arm']==arm],.5) for arm in ['control','candidate']},physical_cold_measured=True,incoming_service_http_measured=True,decision='GO actual core and HTTP development envelope only' if medians['candidate']['p90']<=250 and medians['candidate']['p95']<=400 else 'KILL actual core HTTP envelope')
            if result['decision'].startswith('KILL'):break
    for item,result in zip(config['items'],results):write(out/item['name']/'result.json',result)
    write(out/'decision.json',dict(decision=results[-1]['decision'] if all(r['decision'].startswith('GO') for r in results) else next(r['decision'] for r in results if r['decision'].startswith('KILL')),results=results,qualification=False,fresh_cohort_used=False,scale_run=False))

if __name__=='__main__':
    try:main()
    finally:
        group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
        if len(sys.argv)==7 and Path(sys.argv[4]).is_dir() and (group/'memory.peak').exists():write(Path(sys.argv[4])/'cgroup.json',{k:(group/k).read_text() for k in ['memory.max','memory.peak','memory.swap.max','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()})
