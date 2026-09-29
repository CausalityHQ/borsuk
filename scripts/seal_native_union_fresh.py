"""Source-parity prerequisite and sealed disjoint cohort; never inspect ANN quality."""
import json,subprocess,sys,time
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scripts.run_native_cold import sha,write

def download(bucket,key,path,ident):
    subprocess.run(['aws','s3','cp','s3://'+bucket+'/'+key,str(path),'--only-show-errors'],check=True)
    if path.stat().st_size!=ident['bytes'] or sha(path)!=ident['sha256']:raise ValueError('immutable input identity')

def source(item,directory,config):
    directory.mkdir();raw=directory/'source.raw';queries=directory/'queries.raw';count=0
    with raw.open('xb') as index,queries.open('xb') as query:
        for ordinal,chunk in enumerate(item['chunks']):
            if count!=chunk['row_start']:raise ValueError('canonical shard order')
            p=directory/('train'+str(ordinal)+'.parquet');uri=chunk['uri'];prefix='s3://'+config['bucket']+'/'
            if not uri.startswith(prefix):raise ValueError('source bucket authority')
            download(config['bucket'],uri[len(prefix):],p,chunk)
            parquet=pq.ParquetFile(p)
            if parquet.metadata.num_rows!=chunk['rows']:raise ValueError('source shard row count')
            for batch in parquet.iter_batches(batch_size=8192,columns=[item['column']]):
                column=batch.column(0)
                if not pa.types.is_fixed_size_list(column.type) or column.type.list_size!=768 or column.type.value_type!=pa.float32() or column.null_count or column.values.null_count:raise ValueError('source shard geometry')
                values=np.asarray(column.values.slice(column.offset*768,len(column)*768).to_numpy(zero_copy_only=False),dtype='<f4').reshape(len(column),768)[:max(0,101000-count)]
                if not np.isfinite(values).all() or not (values!=0).any(axis=1).all():raise ValueError('source finite nonzero')
                first=max(0,100000-count);index.write(values[:min(len(values),first)].tobytes())
                start=max(0,100000-count);end=min(len(values),101000-count)
                if end>start:query.write(values[start:end].tobytes())
                count+=len(values)
                if count==101000:break
            if count==101000:break
    if raw.stat().st_size!=307200000 or queries.stat().st_size!=3072000:raise ValueError('source/query coverage')
    return dict(dataset=item['name'],source_raw_sha256=sha(raw),expected_source_raw_sha256=item['source_raw_sha256'],source_parity=sha(raw)==item['source_raw_sha256'],query_source_raw_sha256=sha(queries),source_query_intervals_disjoint=True,query_count=1000)

def seal(item,directory,config,prefix):
    started=time.monotonic();raw=np.memmap(directory/'source.raw',mode='r',dtype='<f4',shape=(100000,768));qraw=np.memmap(directory/'queries.raw',mode='r',dtype='<f4',shape=(1000,768));source=np.asarray(raw,dtype=np.float64);source/=np.sqrt(np.einsum('ij,ij->i',source,source))[:,None];queries=np.asarray(qraw,dtype=np.float64);queries/=np.sqrt(np.einsum('ij,ij->i',queries,queries))[:,None]
    if not np.isfinite(source).all() or not np.isfinite(queries).all():raise ValueError('oracle normalization')
    # Runnable oracle check: scalar f64 cosine agrees with matrix path on fixed cells.
    check=queries[:2]@source[:3].T
    for q in range(2):
        for row in range(3):
            expected=float(np.dot(np.asarray(qraw[q],dtype=np.float64),np.asarray(raw[row],dtype=np.float64)))/(float(np.linalg.norm(np.asarray(qraw[q],dtype=np.float64)))*float(np.linalg.norm(np.asarray(raw[row],dtype=np.float64))))
            if abs(check[q,row]-expected)>1e-12:raise ValueError('oracle scalar parity')
    ids=np.arange(100000,dtype=np.int64);truth=np.empty((1000,100),dtype='<u4');min_margin=float('inf');ties=0
    for first in range(0,1000,16):
        cosine=queries[first:first+16]@source.T
        if not np.isfinite(cosine).all() or (np.abs(cosine)>1+1e-12).any():raise ValueError('oracle score bounds')
        for offset,scores in enumerate(cosine):
            order=np.lexsort((ids,1-scores))[:101];truth[first+offset]=order[:100];margin=float(scores[order[99]]-scores[order[100]]);min_margin=min(min_margin,margin);ties+=margin==0
    truth.tofile(directory/'truth.u32')
    with (directory/'requests.jsonl').open('x') as f:
        for q in range(1000):f.write(json.dumps(dict(query_ordinal=q,query=qraw[q].tolist()),separators=(',',':'),allow_nan=False)+'\n')
    artifacts={}
    for name in ['queries.raw','requests.jsonl','truth.u32']:
        p=directory/name;ident=dict(bytes=p.stat().st_size,sha256=sha(p),key=prefix+'/sealed/'+item['name']+'/'+name)
        subprocess.run(['aws','s3api','put-object','--bucket',config['bucket'],'--key',ident['key'],'--body',str(p),'--if-none-match','*','--metadata','sha256='+ident['sha256']],check=True,stdout=subprocess.DEVNULL);artifacts[name]=ident
    result=dict(dataset=item['name'],source_parity=True,source_raw_sha256=item['source_raw_sha256'],query_source_first=100000,query_source_count=1000,query_cohort=config['query_cohort'],oracle=config['oracle'],exhaustive_oracle_queries=1000,ann_queries_or_scoring=0,quality_peek=False,oracle_scalar_check=True,gt_cutoff_exact_ties=ties,minimum_100_101_cosine_margin=min_margin,artifacts=artifacts,oracle_wall_seconds=time.monotonic()-started,qualification=False)
    write(directory/'cohort.json',result);return result

def main():
    path,digest,repo,out,binaries,prefix=sys.argv[1:];repo,out=map(Path,[repo,out]);config=json.loads(Path(path).read_text())
    if sha(path)!=digest or config['schema']!='borsuk-native-union-fresh-seal-v1' or config['dimensions']!=768 or config['k']!=100 or [i['name'] for i in config['items']]!=['relaion','cohere'] or np.__version__!='2.3.3' or pa.__version__!='24.0.0' or config['native_quality_peek_allowed'] is not False:raise ValueError('frozen source/cohort scope')
    for name,digest in config['scorer_hashes'].items():
        if sha(repo/name)!=digest:raise ValueError('frozen library scorer source')
    out.mkdir();parity=[]
    for item in config['items']:
        result=source(item,out/item['name'],config);parity.append(result);write(out/item['name']/'source-parity.json',result)
        if not result['source_parity']:
            write(out/'decision.json',dict(decision='INVALID prospective cohort source mismatch',source_parity=parity,sealed=False,qualification=False));return
    cohorts=[seal(item,out/item['name'],config,prefix) for item in config['items']]
    write(out/'decision.json',dict(decision='PASS source parity and sealed cohort construction only',source_parity=parity,cohorts=cohorts,sealed=True,qualification=False,ann_quality_measured=False))

if __name__=='__main__':
    try:main()
    finally:
        group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
        if len(sys.argv)==7 and Path(sys.argv[4]).is_dir() and (group/'memory.peak').exists():write(Path(sys.argv[4])/'cgroup.json',{k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()})
