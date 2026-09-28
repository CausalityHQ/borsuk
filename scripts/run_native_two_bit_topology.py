"""Frozen ReLAION-first topology screen; offline plans, never vendor qualification."""
import hashlib
import json
import resource
import struct
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scripts.native_two_bit_cosine_development import normalize, rust_sq8_scores
from scripts.native_two_bit_topology import (
    historical_control_fingerprint, validate_root_pair, validate_paired_roster,
    stage_sets, quality_gate, classify_failure, PAGE_BYTES, OBJECT_BYTES,
)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(4 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def checked(path, expected):
    if sha(path) != expected:
        raise ValueError(f'identity mismatch: {path}')
    return Path(path)


def write(path, value):
    with Path(path).open('x') as stream:
        json.dump(value,stream,sort_keys=True,indent=2,allow_nan=False)
        stream.write('\n')


def run(directory, phase, args, allow_failure=False):
    with (directory/(phase+'.stdout')).open('x') as out, (directory/(phase+'.stderr')).open('x') as err:
        result = subprocess.run(['/usr/bin/time','-v','-o',str(directory/(phase+'.time')),
            'timeout','--signal=TERM','--kill-after=10','300',*map(str,args)],stdout=out,stderr=err)
    if result.returncode and not allow_failure:
        raise RuntimeError(f'{phase} exited {result.returncode}')
    return result.returncode,(directory/(phase+'.stdout')).read_text(),(directory/(phase+'.stderr')).read_text()


def plan_fields(plan):
    return {k:plan[k] for k in ['query_ordinal','ranges','planned_bytes']}


def physical_rows(plan):
    ranges = plan['ranges']
    if not 0 < len(ranges) <= 32:
        raise ValueError('range count')
    previous = -1
    for start,end in ranges:
        if type(start) is not int or type(end) is not int or not 0 <= start < end <= OBJECT_BYTES or start % PAGE_BYTES or (end % PAGE_BYTES and end != OBJECT_BYTES) or start <= previous:
            raise ValueError('physical range geometry')
        previous = end
    size=sum(end-start for start,end in ranges)
    if type(plan['planned_bytes']) is not int or size != plan['planned_bytes'] or size > 16773120:
        raise ValueError('physical byte budget')
    return np.concatenate([np.arange(start//780,end//780) for start,end in ranges])


def scored(plan, sq8, scores, truth, flat):
    physical = physical_rows(plan)
    fetched = sq8[physical]
    returned = fetched['id'][np.lexsort((fetched['id'],scores[physical]))[:100]].tolist()
    if len(returned) != 100:
        raise ValueError('returned roster')
    gt=set(truth)
    sample=dict(query_ordinal=plan['query_ordinal'],fetched_hits=len(gt.intersection(fetched['id'].tolist())),
        returned_hits=len(gt.intersection(returned)),flat_hits=len(gt.intersection(flat)),gets=len(plan['ranges']),bytes=plan['planned_bytes'])
    return sample,returned


def evaluate(item, directory, repo, binaries, config):
    directory.mkdir()
    for role,identity in item['inputs'].items():
        path=directory/role
        subprocess.run(['aws','s3','cp','s3://'+config['bucket']+'/'+identity['key'],str(path),'--only-show-errors'],check=True)
        checked(path,identity['sha256'])
        if path.stat().st_size != identity['bytes']:
            raise ValueError('input byte identity')
    raw=directory/'vectors.raw'
    if item['name']=='relaion':
        remaining=100000
        with raw.open('xb') as output:
            for batch in pq.ParquetFile(directory/'source').iter_batches(batch_size=8192,columns=['embedding']):
                column=batch.column(0)
                if not pa.types.is_fixed_size_list(column.type) or column.type.list_size != 768 or column.type.value_type != pa.float32() or column.null_count or column.values.null_count:
                    raise ValueError('raw parquet geometry')
                rows=min(remaining,len(column))
                values=np.asarray(column.values.to_numpy(zero_copy_only=False),dtype='<f4').reshape(len(column),768)[:rows]
                if not np.isfinite(values).all(): raise ValueError('raw finite')
                output.write(values.tobytes());remaining-=rows
                if not remaining: break
        if remaining: raise ValueError('source rows')
    else:
        raw=directory/'source'
    checked(raw,item['raw_sha256'])
    if raw.stat().st_size != 307200000: raise ValueError('raw geometry')
    normalized,order,sq8_path=(directory/n for n in ['normalized.f32','order.u64','sq8.bin'])
    tool=binaries/'examples/build_sq8_source'
    run(directory,'normalize',[tool,'normalize',raw,item['raw_sha256'],100000,768,268435456,normalized])
    checked(normalized,item['normalized_sha256'])
    run(directory,'fit',[tool,'fit',normalized,item['normalized_sha256'],100000,768,268435456,order])
    checked(order,item['order_sha256'])
    _,encoded,_=run(directory,'encode',[tool,normalized,item['normalized_sha256'],768,order,item['order_sha256'],268435456,sq8_path])
    encoding=json.loads(encoded);checked(sq8_path,item['sq8_sha256'])
    old_builder=json.loads(checked(repo/item['historical_builder_file'],item['historical_builder_sha256']).read_text())
    for key in ['low','step']:
        if json.dumps(encoding[key]) != json.dumps(old_builder[key]): raise ValueError('SQ8 coefficients changed')
    builder=dict(old_builder,raw=str(raw),sq8=str(sq8_path),base_epoch=0)
    write(directory/'builder.json',builder)
    control=directory/'control'
    _,root_sha,_=run(directory,'build',[binaries/'build_two_bit_generation',directory/'builder.json',sha(directory/'builder.json'),268435456,control])
    root_sha=root_sha.strip()
    body=checked(control/'manifest.json',root_sha).read_bytes()
    root=json.loads(body)
    plane_body=checked(control/'plane/manifest.json',root['plane_manifest_sha256']).read_bytes()
    if historical_control_fingerprint(body,plane_body,item['order_sha256']) != item['historical_root_sha256']: raise ValueError('historical query-component fingerprint')
    canonical=root['canonical']
    if (canonical['rows'],canonical['dimensions'],canonical['bytes']) != (100000,768,308000000): raise ValueError('canonical geometry')
    checked(control/'canonical.bin',canonical['sha256'])
    page=json.loads(checked(control/'page_manifest.json',root['page_manifest_sha256']).read_text())
    if (page['rows'],page['dimensions'],page['page_rows']) != (100000,768,256): raise ValueError('page authority geometry')
    centers=checked(control/'centroids.bin',root['centroids_sha256']).read_bytes()
    if struct.unpack_from('<QIII',centers,8) != (100000,768,32,256): raise ValueError('centroid geometry')
    requests=[json.loads(line) for line in (directory/'requests').read_text().splitlines()]
    if len(requests)!=1000 or any(type(r['query_ordinal']) is not int or r['query_ordinal'] != i for i,r in enumerate(requests)): raise ValueError('request roster')
    truth=np.fromfile(directory/'truth',dtype='<u4').reshape(1000,100)[:64]
    if (truth>=100000).any() or any(np.unique(row).size!=100 for row in truth): raise ValueError('GT authority')
    dtype=np.dtype([('id','<i8'),('norm','<f4'),('code','u1',(768,))])
    if sq8_path.stat().st_size != OBJECT_BYTES: raise ValueError('SQ8 geometry')
    sq8=np.memmap(sq8_path,mode='r',dtype=dtype,shape=(100000,))
    if not np.array_equal(np.sort(sq8['id']),np.arange(100000)) or not np.isfinite(sq8['norm']).all() or not (sq8['norm']>0).all(): raise ValueError('SQ8 signed-ID/norm authority')
    plans_path=directory/'preflight-plans.jsonl'
    run(directory,'preflight',[binaries/'two_bit_plan_demo',control,root_sha,directory/'requests',item['inputs']['requests']['sha256'],plans_path,0,64])
    checked(plans_path,item['historical_plans_sha256'])
    plans=[json.loads(line) for line in plans_path.read_text().splitlines()]
    old_score=json.loads(checked(repo/item['historical_score_file'],item['historical_score_sha256']).read_text())
    if len(plans)!=64 or len(old_score['samples'])!=64: raise ValueError('control panel size')
    low,step=(np.asarray(root[key],dtype=np.float32) for key in ['low','step'])
    scores=np.empty((64,100000),dtype=np.float32);flats=[];preflight=[]
    scoring_start=time.monotonic();usage_start=resource.getrusage(resource.RUSAGE_SELF)
    for i,plan in enumerate(plans):
        if set(plan)!= {'query_ordinal','ranges','planned_bytes'} or plan['query_ordinal']!=i: raise ValueError('normal control plan shape')
        query=normalize(requests[i]['query'])
        if query.shape!=(768,): raise ValueError('query geometry')
        scores[i]=rust_sq8_scores(sq8,query,low,step)
        if not np.isfinite(scores[i]).all(): raise ValueError('SQ8 score finite')
        flat=sq8['id'][np.lexsort((sq8['id'],scores[i]))[:100]].tolist();flats.append(flat)
        sample,_=scored(plan,sq8,scores[i],truth[i].tolist(),flat)
        if sample != old_score['samples'][i]: raise ValueError(f'control scorer parity ordinal{i}')
        preflight.append(sample)
    usage_end=resource.getrusage(resource.RUSAGE_SELF)
    write(directory/'scoring.json',dict(wall_seconds=time.monotonic()-scoring_start,
        user_seconds=usage_end.ru_utime-usage_start.ru_utime,system_seconds=usage_end.ru_stime-usage_start.ru_stime,
        process_cumulative_max_rss_kib_before=usage_start.ru_maxrss,process_cumulative_max_rss_kib_after=usage_end.ru_maxrss,
        full_sq8_kernel_calls=64,cache_payload_bytes=scores.nbytes,scope='Python mirror, exhaustive and preflight scoring; not Rust serving'))
    np.save(directory/'scores.npy',scores,allow_pickle=False)
    write(directory/'authority.json',dict(current_root_sha256=root_sha,historical_root_fingerprint=item['historical_root_sha256'],
        control_plans_sha256=sha(plans_path),score_cache_sha256=sha(directory/'scores.npy'),source_identities=item,preflight=preflight,
        source_control_scorer_parity=True,source_scorer_hashes=config['source_scorer_hashes']))
    candidate=directory/'candidate'
    _,built,_=run(directory,'graph',[binaries/'build_two_bit_graph_variant',control,root_sha,candidate])
    build=json.loads(built);candidate_sha=build['root_sha256']
    candidate_root=json.loads(checked(candidate/'manifest.json',candidate_sha).read_text())
    validate_root_pair(root,candidate_root)
    paired_path=directory/'paired-plans.jsonl'
    code,_,stderr=run(directory,'paired',[binaries/'two_bit_plan_demo',control,root_sha,directory/'requests',item['inputs']['requests']['sha256'],paired_path,0,64,'--trace','--paired',candidate,candidate_sha],allow_failure=True)
    records=[json.loads(line) for line in paired_path.read_text().splitlines()]
    roots=dict(control=root_sha,candidate=candidate_sha)
    validate_paired_roster(records,roots,partial=bool(code))
    unit_of=np.empty(100000,dtype=np.int64);page_of=np.empty(100000,dtype=np.int64)
    unit_of[sq8['id']]=np.arange(100000)//32;page_of[sq8['id']]=np.arange(100000)//256
    samples=dict(control=[],candidate=[]);stages={};decomposition=[]
    for record in records:
        i,arm=record['query_ordinal'],record['arm']
        if arm=='control' and plan_fields(record)!=plans[i]: raise ValueError('paired control plan parity')
        pages=record['ranked_candidate_pages']
        if len(pages)!=159 or record['primary_page']!=pages[0] or record['seed_page'] not in pages: raise ValueError('trace candidate/seed/primary geometry')
        if any(type(record[k]) is not bool for k in ['seed_work_exhausted','walk_work_exhausted']): raise ValueError('trace exhaustion flags')
        sample,returned=scored(record,sq8,scores[i],truth[i].tolist(),flats[i])
        sets=stage_sets(truth[i].tolist(),unit_of,page_of,record,returned,flats[i])
        sample['candidate_hits']=len(sets['candidate'])
        if arm=='control' and {k:v for k,v in sample.items() if k!='candidate_hits'}!=preflight[i]: raise ValueError('paired control score parity')
        if any(type(record[k]) is not int or record[k]<0 for k in ['route_wall_ns','route_process_cpu_ns']): raise ValueError('route clocks')
        samples[arm].append(sample);stages[i,arm]=sets
        decomposition.append(dict(query_ordinal=i,arm=arm,stages={k:sorted(v) for k,v in sets.items()},
            distinct_visited_units=len(set(record['seed_evaluated_units'])|set(record['walk_evaluated_units'])),
            seed_page=record['seed_page'],primary_page=record['primary_page']))
    write(directory/'decomposition.json',decomposition)
    if code:
        if classify_failure(records,roots,code,stderr)!='scientific-kill': raise RuntimeError(f'invalid paired runtime exit{code}')
        result=dict(dataset=item['name'],decision='KILL',cause='candidate discovery geometry',partial_records=len(records),samples=samples,quality_complete=False)
    else:
        passed=quality_gate(item['name'],samples['control'],samples['candidate'])
        gains=[dict(query_ordinal=i,stages={k:dict(gained=sorted(stages[i,'candidate'][k]-stages[i,'control'][k]),lost=sorted(stages[i,'control'][k]-stages[i,'candidate'][k])) for k in stages[i,'control']}) for i in range(64)]
        write(directory/'gained-lost.json',gains)
        metrics={arm:{key:dict(total=sum(s[key] for s in values),mean=sum(s[key] for s in values)/64,p05=sorted(s[key] for s in values)[3]) for key in ['candidate_hits','fetched_hits','returned_hits','flat_hits']} for arm,values in samples.items()}
        timings={arm:{split:{key:dict(zip(['p50','p90','p95','p99'],np.quantile([r[key]/1e6 for r in records if r['arm']==arm and r['query_ordinal']>=first],[.5,.9,.95,.99]).tolist())) for key in ['route_wall_ns','route_process_cpu_ns']} for split,first in [('all64',0),('ordinal1-63',1)]} for arm in roots}
        cause='none' if passed else ('discovery' if metrics['candidate']['candidate_hits']['total']<=metrics['control']['candidate_hits']['total'] or metrics['candidate']['fetched_hits']['total']<dict(relaion=6379,cohere=6392)[item['name']] else 'nomination/scoring/returned quality')
        result=dict(dataset=item['name'],decision='GO' if passed else 'KILL',cause=cause,quality_complete=True,metrics=metrics,timings_ms_exploratory=timings,samples=samples)
    result.update(rows=100000,dimensions=768,metric='cosine',k=100,split='consumed development0-63',roots=roots,graph_build=build['build'],
        logical_ranges_not_physical_gets=True,score_kernel_calls=64,native_planner_calls=64+len(records)+(1 if code else 0),qualification=False)
    write(directory/'result.json',result)
    return result


def main():
    config_path,config_sha,repo,directory,binaries=sys.argv[1:]
    repo,directory,binaries=map(lambda p:Path(p).resolve(),[repo,directory,binaries])
    config=json.loads(checked(config_path,config_sha).read_text())
    if config['schema']!='borsuk-topology-mechanism-screen-v1' or [i['name'] for i in config['items']]!=['relaion','cohere'] or (config['rows'],config['dimensions'],config['first'],config['count'],config['k'],config['metric'])!=(100000,768,0,64,100,'cosine'): raise ValueError('frozen screen')
    if np.__version__!=config['numpy_version'] or pa.__version__!=config['pyarrow_version']: raise ValueError('scorer dependency versions')
    for name,digest in config['source_scorer_hashes'].items(): checked(repo/name,digest)
    directory.mkdir()
    results=[]
    for item in config['items']:
        result=evaluate(item,directory/item['name'],repo,binaries,config);results.append(result)
        if result['decision']=='KILL': break
    write(directory/'decision.json',dict(decision='KILL' if results[-1]['decision']=='KILL' else 'GO development mechanism only',results=results,
        executed_datasets=[r['dataset'] for r in results],validation_or_scale_run=False,qualification=False,config_sha256=config_sha))


if __name__=='__main__':
    try:
        main()
    finally:
        # Accounted cgroup memory includes file cache/kernel; it is not RSS.
        group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
        if len(sys.argv)==6 and Path(sys.argv[4]).is_dir() and (group/'memory.peak').exists():
            write(Path(sys.argv[4])/'cgroup.json',{k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()})
