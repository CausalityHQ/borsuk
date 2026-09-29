"""AWS-only first1M development falsifier; same-root old/new planner, no fresh claim."""
import atexit,json,os,struct,subprocess,sys,time
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scripts.run_native_union_cold import native,plan,plan_sha,sha,write
from scripts.run_native_union_http import run as http_run,quantile
from scripts.native_two_bit_cosine_development import normalize
from scripts.v291_two_stage_development import rust_sq8_scores


def best(ids,scores,k=100):
    assert len(ids)==len(scores) and np.isfinite(scores).all()
    order=np.lexsort((ids,scores))[:k]
    return ids[order],scores[order]


def oracle(raw,queries,rows):
    # Complete block sorts and exact top100 merges; no approximate neighbor search.
    query=np.asarray(queries,dtype=np.float32).astype(np.float64);query/=np.sqrt(np.sum(query*query,axis=1))[:,None]
    retained=[(np.empty(0,dtype=np.int64),np.empty(0,dtype=np.float64)) for _ in query]
    with Path(raw).open('rb') as source:
        for start in range(0,rows,8192):
            count=min(8192,rows-start);body=source.read(count*768*4);assert len(body)==count*768*4
            block=np.frombuffer(body,dtype='<f4').reshape(count,768).astype(np.float64)
            block/=np.sqrt(np.sum(block*block,axis=1))[:,None]
            distances=1-query@block.T
            assert np.isfinite(distances).all() and (distances>=-1e-12).all() and (distances<=2+1e-12).all()
            ids=np.arange(start,start+len(block),dtype=np.int64)
            for q,scores in enumerate(distances):
                old_ids,old_scores=retained[q];new_ids,new_scores=best(ids,scores)
                retained[q]=best(np.concatenate((old_ids,new_ids)),np.concatenate((old_scores,new_scores)))
        assert source.read(1)==b''

    return np.asarray([ids for ids,_ in retained],dtype='<u4')


def self_check():
    ids=np.array([8,2,5,1]);scores=np.array([.2,.1,.1,.3])
    assert best(ids,scores,3)[0].tolist()==[2,5,8]
    left=best(ids[:2],scores[:2],2);right=best(ids[2:],scores[2:],2)
    assert best(np.concatenate((left[0],right[0])),np.concatenate((left[1],right[1])),3)[0].tolist()==[2,5,8]
    assert normalize([1.0]+[0.0]*767).tolist()==[1.0]+[0.0]*767
    import math
    x=np.array([[1.,2.,3.],[-2.,1.,4.],[1.,0.,0.]],dtype=np.float64);q=np.array([[3.,2.,1.],[-1.,0.,2.]])
    matrix=1-(q/np.sqrt(np.sum(q*q,axis=1))[:,None])@(x/np.sqrt(np.sum(x*x,axis=1))[:,None]).T
    scalar=np.array([[1-sum(float(a)*float(b) for a,b in zip(y,z))/(math.sqrt(sum(float(a)**2 for a in y))*math.sqrt(sum(float(b)**2 for b in z))) for z in x] for y in q])
    assert np.max(np.abs(matrix-scalar))<1e-12
    import tempfile
    padded=np.zeros((3,768),dtype='<f4');padded[:,:3]=x;queries=np.zeros((2,768));queries[:,:3]=q
    with tempfile.TemporaryDirectory() as directory:
        raw=Path(directory)/'raw';padded.tofile(raw)
        assert oracle(raw,queries,3).tolist()==[np.lexsort((np.arange(3),r)).tolist() for r in scalar]


def main():
    config_path,digest,repo,out,binaries,prefix=sys.argv[1:];repo,out,binaries=map(Path,[repo,out,binaries]);config=json.loads(Path(config_path).read_text())
    assert sha(config_path)==digest and config['schema']=='borsuk-native-walk-source-1m-dev-v1'
    assert (config['rows'],config['dimensions'],config['k'],config['first'],config['count'])==(1000000,768,100,0,64)
    assert config['arm_order']==['control','candidate','candidate','control'] and config['fresh_cohort_used'] is False
    assert np.__version__=='2.3.3' and pa.__version__=='24.0.0' and sorted(os.sched_getaffinity(0))==[0,1,2,3]
    for name,digest in config['dependencies'].items():assert sha(repo/name)==digest
    out.mkdir();self_check();write(out/'self-check.json',dict(passed=True,top100_merge_signed_id_ties=True,actual_streaming_oracle_fixture=True,scalar_matrix_f64_parity=True))
    def resources():
        group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
        write(out/'cgroup.json',{k:(group/k).read_text() for k in ['memory.max','memory.peak','memory.swap.max','memory.swap.peak','memory.events','cpu.stat']})
    atexit.register(resources);pa.set_cpu_count(4);pa.set_io_thread_count(2)
    directory=out/'relaion';directory.mkdir();rows=config['rows']
    for role,ident in config['artifacts'].items():
        path=directory/role;subprocess.run(['aws','s3','cp','s3://'+config['bucket']+'/'+ident['key'],str(path),'--only-show-errors'],check=True)
        assert path.stat().st_size==ident['bytes'] and sha(path)==ident['sha256']
    raw=directory/'source.raw';pf=pq.ParquetFile(directory/'source.parquet');assert pf.metadata.num_rows==rows
    with raw.open('xb') as stream:
        for batch in pf.iter_batches(batch_size=8192,columns=['embedding']):
            col=batch.column(0);assert pa.types.is_fixed_size_list(col.type) and col.type.list_size==768 and col.type.value_type==pa.float32() and col.null_count==col.values.null_count==0
            values=np.asarray(col.values.slice(col.offset*768,len(col)*768).to_numpy(zero_copy_only=False),dtype='<f4').reshape(len(col),768)
            assert np.isfinite(values).all() and (values!=0).any(axis=1).all();stream.write(values.tobytes())
    assert raw.stat().st_size==3072000000 and sha(raw)==config['raw_sha256']
    order=directory/'order.u64';sq8_path=directory/'sq8.bin'
    recipe=json.loads((directory/'hier-fit.log').read_text());receipt=json.loads((directory/'sq8.log').read_text());values=np.fromfile(order,dtype='<u8')
    assert len(values)==rows and np.array_equal(np.sort(values),np.arange(rows,dtype=np.uint64))
    assert recipe['order_sha256']==sha(order) and recipe['recipe']=='borsuk-hierarchical-extents-chacha8-v3' and recipe['query_or_truth_used'] is False and recipe['source_cell_target_rows']==256 and recipe['sampling_cell_target_rows']==1024 and recipe['samples_per_sampling_cell']==64 and recipe['source_cell_order']=='nearest-unvisited-layer0-entry-ordinal-fallback-v1'
    extents=recipe['extents'];assert extents[0][0]==0 and extents[-1][1]==rows and all(0<b-a<=1024 for a,b in extents) and all(x[1]==y[0] for x,y in zip(extents,extents[1:]))
    build=json.loads((directory/'closed-builder.json').read_text());binding=json.loads((directory/'closed-binding.json').read_text());sq8_sha=sha(sq8_path);key=build['sq8_object_key']
    assert receipt['sq8_sha256']==sq8_sha==build['sq8_sha256'] and receipt['rows']==rows and sq8_path.stat().st_size==780000000
    assert binding['source_order_sha256']==sha(order) and binding['raw_sha256']==config['raw_sha256'] and binding['normalized_sha256']==config['normalized_sha256']
    head=json.loads(subprocess.check_output(['aws','s3api','head-object','--bucket',config['bucket'],'--key',key]));assert head['ContentLength']==780000000 and head['Metadata']['sha256']==sq8_sha and head['ETag']==build['sq8_etag']
    build.update(raw=str(raw),sq8=str(sq8_path));write(directory/'builder.json',build)
    generation=directory/'generation';native(directory,'build',[binaries/'build_two_bit_generation',directory/'builder.json',sha(directory/'builder.json'),2147483648,generation],600)
    root_sha=sha(generation/'manifest.json');root=json.loads((generation/'manifest.json').read_text());assert root['schema']=='borsuk-two-bit-generation-v4' and json.loads((generation/'page_manifest.json').read_text())['rows']==rows and root['sq8_object_key']==key and root['sq8_etag']==head['ETag']
    assert (generation/'manifest.json').read_bytes()==(directory/'closed-manifest.json').read_bytes()
    for name,ident in config['closed_generation_components'].items():assert (generation/name).stat().st_size==ident['bytes'] and sha(generation/name)==ident['sha256']
    write(directory/'binding.json',dict(closed_construction_reused=True,root_sha256=root_sha,source_recipe=recipe,source_order_sha256=sha(order),raw_sha256=sha(raw),normalized_sha256=config['normalized_sha256'],same_root_source_scorer_control=True,query_or_truth_used_for_construction=False))
    requests=[json.loads(s) for s in (directory/'requests').read_text().splitlines()];assert len(requests)==1000 and [q['query_ordinal'] for q in requests]==list(range(1000))
    started=time.monotonic();truth=oracle(raw,[r['query'] for r in requests[:64]],rows);truth.tofile(directory/'truth.u32')
    write(directory/'oracle.json',dict(dtype='float64',distance='1-dot of f64-normalized original f32',tie='signed source ordinal ascending',queries=64,rows=rows,truth_sha256=sha(directory/'truth.u32'),wall_s=time.monotonic()-started,exact_block_sort_merge=True))
    dtype=np.dtype([('id','<i8'),('norm','<f4'),('code','u1',(768,))]);sq8=np.memmap(sq8_path,dtype=dtype,mode='r',shape=(rows,));assert np.array_equal(sq8['id'],values)
    positions=np.empty(rows,dtype=np.int64);positions[sq8['id']]=np.arange(rows,dtype=np.int64)
    low,step=(np.asarray(root[k],dtype=np.float32) for k in ['low','step']);assert np.array_equal(low,np.asarray(receipt['low'],dtype=np.float32)) and np.array_equal(step,np.asarray(receipt['step'],dtype=np.float32))
    by_id=np.empty(rows,dtype='<f4');plans={};samples={};expected={};scores_identity=[];score_start=time.monotonic()
    for arm,binary in [('control','old_two_bit_plan_demo'),('candidate','two_bit_plan_demo')]:
        native(directory,'plan-'+arm,[binaries/binary,generation,root_sha,directory/'requests',config['artifacts']['requests']['sha256'],directory/('plan-'+arm+'.jsonl'),0,64,'--trace'],300)
        plans[arm]=[json.loads(s) for s in (directory/('plan-'+arm+'.jsonl')).read_text().splitlines()];assert len(plans[arm])==64
        samples[arm]=[];expected[arm]=[]
    for q in range(64):
        if time.monotonic()-score_start>900:raise TimeoutError('fixed quality phase envelope')
        query=normalize(requests[q]['query']);assert query.shape==(768,)
        for start in range(0,rows,8192):
            chunk=sq8[start:start+8192];by_id[chunk['id']]=rust_sq8_scores(chunk,query,low,step)
        assert np.isfinite(by_id).all();import hashlib
        scores_identity.append(hashlib.sha256(by_id.tobytes()).hexdigest());flat=best(np.arange(rows,dtype=np.int64),by_id)[0];gt=set(map(int,truth[q]));flat_gt=gt&set(map(int,flat))
        for arm in plans:
            r=plans[arm][q];assert r['query_ordinal']==q and 0<len(r['ranges'])<=32 and r['planned_bytes']<=16773120
            physical=np.concatenate([np.arange(a//780,b//780) for a,b in r['ranges']]);ids=sq8['id'][physical];returned=best(ids,by_id[ids])[0].tolist();expected[arm].append(returned)
            stage=dict(candidate=sorted(i for i in gt if positions[i]//256 in r['ranked_candidate_pages']),nominated=sorted(i for i in gt if positions[i]//256 in r['selected_pages']),fetched=sorted(gt&set(map(int,ids))),returned=sorted(gt&set(returned)),flat=sorted(flat_gt))
            assert set(stage['nominated'])<=set(stage['candidate']) and set(stage['nominated'])<=set(stage['fetched'])
            samples[arm].append(dict(query_ordinal=q,stages=stage,returned_hits=len(stage['returned']),gets=len(r['ranges']),bytes=r['planned_bytes']))
    hits={a:sum(s['returned_hits'] for s in x) for a,x in samples.items()};p05={a:sorted(s['returned_hits'] for s in x)[3] for a,x in samples.items()};flat_hits=sum(len(s['stages']['flat']) for s in samples['control'])
    passed=hits['candidate']/64>=98 and p05['candidate']>=95 and flat_hits-hits['candidate']<=32 and hits['candidate']>=hits['control']
    result=dict(dataset='relaion',rows=rows,split='consumed external development0-63',returned_hits=hits,p05=p05,flat_hits=flat_hits,samples=samples,expected_ids=expected,plans_sha256={a:plan_sha([plan(r) for r in records]) for a,records in plans.items()},root_sha256=root_sha,scores_by_id_sha256=scores_identity,scoring_wall_s=time.monotonic()-score_start,decision='GO 1M consumed development quality only' if passed else 'KILL 1M consumed development quality',physical_cold_measured=False,fresh_cohort_used=False,qualification=False)
    write(directory/'quality.json',result)
    if passed:
        refs={};item=dict(name='relaion',root_sha256={a:root_sha for a in plans},index_prefix={},returned_hits=hits,p05=p05)
        for arm,binary in [('control','old_two_bit_plan_demo'),('candidate','two_bit_plan_demo')]:
            live=directory/('native-'+arm);live.mkdir();item['index_prefix'][arm]=prefix+'/runs/relaion/'+arm
            native(live,'live',[binaries/binary,generation,root_sha,directory/'requests',config['artifacts']['requests']['sha256'],live/'live.jsonl',0,64,'--live-s3',config['bucket'],config['region'],item['index_prefix'][arm]],300)
            refs[arm]=[json.loads(s) for s in (live/'live.jsonl').read_text().splitlines()];assert len(refs[arm])==66
            for q,r in enumerate(refs[arm][1:-1]):assert r['ids']==expected[arm][q] and plan(r)==plan(plans[arm][q]) and r['submitted_gets']==len(r['ranges'])<=32 and r['verified_bytes']==r['planned_bytes']<=16773120 and r['failed_gets']==0
        runs=[http_run(item,a,rep,config,directory,binaries/('old_two_bit_http' if a=='control' else 'two_bit_http'),requests,refs[a],truth.reshape(-1).tolist(),prefix) for rep,a in enumerate(config['arm_order'])]
        tails={a:{label:quantile([r['incoming_http_ms'][label] for r in runs if r['arm']==a],.5) for label in ['p50','p90','p95','p99']} for a in plans};qps={a:quantile([r['serial_observed_qps'] for r in runs if r['arm']==a],.5) for a in plans}
        result.update(runs=runs,arm_median_incoming_http_ms=tails,arm_median_serial_qps=qps,physical_cold_measured=True,decision='GO 1M consumed development HTTP envelope only' if tails['candidate']['p90']<=250 and tails['candidate']['p95']<=400 else 'KILL 1M consumed development HTTP envelope')
    write(directory/'result.json',result);write(out/'decision.json',dict(decision=result['decision'],results=[result],fresh_cohort_used=False,qualification=False,scale_run=True))

if __name__=='__main__':main()
