"""Exact closed source-v3 reconstruction and one bounded early source-code replay."""
import json,struct,subprocess,sys
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
from scripts.run_native_union_cold import native,plan,plan_sha,sha,write

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
    build=json.loads((directory/'closed-builder.json').read_text())
    if build['sq8_sha256']!=sq8_sha or build['low']!=sq8_receipt['low'] or build['step']!=sq8_receipt['step']:raise ValueError('exact old builder payload')
    sq8_key=build['sq8_object_key']
    head=json.loads(subprocess.check_output(['aws','s3api','head-object','--bucket',config['bucket'],'--key',sq8_key]))
    if head['ContentLength']!=78000000 or head['Metadata']['sha256']!=sq8_sha or head['ETag']!=build['sq8_etag']:raise ValueError('exact retained object HEAD')
    candidate=directory/'candidate'
    build.update(raw=str(raw),sq8=str(new_sq8));write(directory/'builder.json',build)
    native(directory,'build',[binaries/'build_two_bit_generation',directory/'builder.json',sha(directory/'builder.json'),268435456,candidate])
    candidate_sha=sha(candidate/'manifest.json');new_root=json.loads((candidate/'manifest.json').read_text())
    if new_root['schema']!='borsuk-two-bit-generation-v4' or new_root['sq8_object_key']!=sq8_key or new_root['sq8_etag']!=head['ETag'] or sha(candidate/'plane/mean.bin')!=sha(control/'plane/mean.bin'):raise ValueError('native candidate binding/mean parity')
    old_records=np.memmap(control/'plane/records.bin',mode='r',dtype=np.dtype('V200'));new_records=np.memmap(candidate/'plane/records.bin',mode='r',dtype=np.dtype('V200'))
    if old_records[np.argsort(old['id'])].tobytes()!=new_records[np.argsort(candidate_sq8['id'])].tobytes():raise ValueError('per-ID exact source two-bit row parity')
    write(directory/'binding.json',dict(control_root_sha256=item['control_root_sha256'],candidate_root_sha256=candidate_sha,per_id_sq8_and_two_bit_payload_exact=True,coefficient_f32_bits_exact=True,source_order_sha256=sha(order),source_only_recipe=recipe,sq8_object_key=sq8_key,sq8_etag=head['ETag']))
    if (candidate/'manifest.json').read_bytes()!=(directory/'closed-manifest.json').read_bytes():raise ValueError('exact reconstructed root')
    binary=binaries/'two_bit_plan_demo';request_sha=item['artifacts']['requests']['sha256']
    native(directory,'paired',[binary,control,item['control_root_sha256'],directory/'requests',request_sha,directory/'paired.jsonl',0,64,'--trace','--paired',candidate,candidate_sha])
    records=[json.loads(s) for s in (directory/'paired.jsonl').read_text().splitlines()]
    closed=[json.loads(s) for s in (directory/'closed-paired.jsonl').read_text().splitlines()]
    if len(records)!=128 or len(closed)!=128:raise ValueError('paired roster')
    parity=['query_ordinal','arm','root_sha256','ranges','planned_bytes','discoveries','ranked_candidate_pages','selected_pages','primary_page']
    if any(any(r[k]!=old[k] for k in parity) for r,old in zip(records,closed)):raise ValueError('exact original candidate/control preflight plans')
    write(directory/'preflight.json',dict(exact_closed_root=True,exact_closed_paired_traces=True,candidate_root_sha256=candidate_sha,paired_queries=128))
    native(directory,'replay',[binaries/'two_bit_walk_nomination',candidate,candidate_sha,item['control_root_sha256'],directory/'requests',request_sha,directory/'paired.jsonl',sha(directory/'paired.jsonl'),directory/'replay.jsonl'])
    replay=[json.loads(s) for s in (directory/'replay.jsonl').read_text().splitlines()]
    if len(replay)!=64:raise ValueError('replay roster')
    for q,r in enumerate(replay):
        if r['query_ordinal']!=q or r['root_sha256']!=candidate_sha or not 0<r['source_rows_scored']<=81408 or len(r['ranked_candidate_pages'])>318:raise ValueError('replay bounds')
        pos=q*2+(1 if q%2==0 else 0)
        records[pos]=r
    reference=[json.loads(s) for s in (directory/'reference-paired.jsonl').read_text().splitlines()]
    old_scores=np.load(directory/'scores.npy',mmap_mode='r',allow_pickle=False)
    if old_scores.shape!=(64,100000) or old_scores.dtype!=np.dtype('<f4'):raise ValueError('cache authority')
    by_id=np.empty_like(old_scores);by_id[:,old['id']]=old_scores;truth=np.fromfile(directory/'truth',dtype='<u4').reshape(1000,100)
    samples={a:[] for a in ['control','candidate']};expected_ids={a:[] for a in samples};plans={a:[] for a in samples}
    for q in range(64):
        old_trace=next(r for r in reference[q*2:q*2+2] if r['arm']=='candidate')
        for position,r in enumerate(records[q*2:q*2+2]):
            arm=(['control','candidate'] if q%2==0 else ['candidate','control'])[position]
            if r['query_ordinal']!=q or r['arm']!=arm or r['root_sha256']!=(item['control_root_sha256'] if arm=='control' else candidate_sha):raise ValueError('paired identity')
            if arm=='control' and any(r[k]!=old_trace[k] for k in ['ranges','planned_bytes','discoveries','ranked_candidate_pages','selected_pages','primary_page']):raise ValueError('exact current union control trace parity')
            physical=old if arm=='control' else candidate_sq8;p=plan(r);positions=np.concatenate([np.arange(s//780,e//780) for s,e in p['ranges']]);ids=physical['id'][positions];scores=by_id[q,ids];returned=ids[np.lexsort((ids,scores))[:100]].tolist();gt=set(truth[q].tolist());flat_ids=old['id'][np.lexsort((old['id'],old_scores[q]))[:100]]
            pages={int(i):pos//256 for pos,i in enumerate(physical['id'])};candidate_gt={i for i in gt if pages[i] in r['ranked_candidate_pages']};nominated_gt={i for i in gt if pages[i] in r['selected_pages']};fetched_gt=gt&set(ids.tolist());returned_gt=gt&set(returned);flat_gt=gt&set(flat_ids.tolist())
            if not nominated_gt<=candidate_gt or not nominated_gt<=fetched_gt or not returned_gt<=fetched_gt or not 0<len(p['ranges'])<=32 or p['planned_bytes']>16773120:raise ValueError('stage/physical authority')
            samples[arm].append(dict(query_ordinal=q,candidate_hits=len(candidate_gt),nominated_hits=len(nominated_gt),fetched_hits=len(fetched_gt),returned_hits=len(returned_gt),flat_hits=len(flat_gt),gets=len(p['ranges']),bytes=p['planned_bytes'],stages=dict(candidate=sorted(candidate_gt),nominated=sorted(nominated_gt),fetched=sorted(fetched_gt),returned=sorted(returned_gt),flat=sorted(flat_gt))))
            expected_ids[arm].append(returned);plans[arm].append(p)
    if plan_sha(plans['control'])!=item['expected_control_plans_sha256'] or sum(s['returned_hits'] for s in samples['control'])!=item['control_returned_hits'] or sum(s['flat_hits'] for s in samples['control'])!=item['control_flat_hits']:raise ValueError('current control metrics/plan authority')
    hits=sum(s['returned_hits'] for s in samples['candidate']);p05=sorted(s['returned_hits'] for s in samples['candidate'])[3];passed=hits/64>=98 and p05>=95 and item['control_flat_hits']-hits<=32 and hits>=item['control_returned_hits']
    result=dict(dataset=item['name'],decision='GO early source-code development only' if passed else 'KILL early source-code quality',samples=samples,returned_hits={a:sum(s['returned_hits'] for s in samples[a]) for a in samples},p05={a:sorted(s['returned_hits'] for s in samples[a])[3] for a in samples},normal_plans_sha256={a:plan_sha(p) for a,p in plans.items()},qualification=False,source_recipe=recipe,per_id_source_and_scorer_parity=True,new_exhaustive_sq8_kernel_calls=0,cached_sort_queries=128,physical_cold_measured=False)
    write(directory/'quality.json',result)
    write(directory/'result.json',result);return result

def main():
    path,digest,repo,out,binaries,prefix=sys.argv[1:];repo,out,binaries=map(Path,[repo,out,binaries]);config=json.loads(Path(path).read_text())
    if sha(path)!=digest or config['schema']!='borsuk-native-union-layout-transfer-v1' or config['arm_order']!=['control','candidate','candidate','control'] or [i['name'] for i in config['items']]!=['relaion'] or np.__version__!='2.3.3' or pa.__version__!='24.0.0':raise ValueError('frozen layout scope')
    for name,digest in config['scorer_hashes'].items():
        if sha(repo/name)!=digest:raise ValueError('frozen scorer')
    out.mkdir();results=[]
    for item in config['items']:
        result=evaluate(item,out/item['name'],config,binaries,prefix);results.append(result)
        if result['decision'].startswith('KILL'):break
    write(out/'decision.json',dict(decision=results[-1]['decision'],results=results,qualification=False,fresh_cohort_used=False,scale_run=False))

if __name__=='__main__':
    try:main()
    finally:
        group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
        if len(sys.argv)==7 and Path(sys.argv[4]).is_dir() and (group/'memory.peak').exists():write(Path(sys.argv[4])/'cgroup.json',{k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()})
