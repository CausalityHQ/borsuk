"""One client-cold nearest-baseline S3 serving cell; never vendor qualification."""
import hashlib,json,struct,subprocess,sys
from pathlib import Path
import numpy as np


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(4<<20),b''):h.update(block)
    return h.hexdigest()


def write(path,value):
    with Path(path).open('x') as out:json.dump(value,out,sort_keys=True,indent=2,allow_nan=False);out.write('\n')


def main():
    config_path,config_sha,repo,directory,binary,prefix=sys.argv[1:]
    repo,directory=Path(repo),Path(directory)
    if sha(config_path)!=config_sha:raise ValueError('configuration identity')
    config=json.loads(Path(config_path).read_text())
    if config['schema']!='borsuk-native-cold-baseline-v1' or (config['rows'],config['dimensions'],config['k'],config['first'],config['count'])!=(100000,768,100,0,64):raise ValueError('frozen development scope')
    for name,digest in config['scorer_hashes'].items():
        if sha(repo/name)!=digest:raise ValueError('frozen scorer changed')
    directory.mkdir()
    for name,identity in config['artifacts'].items():
        path=directory/name;path.parent.mkdir(parents=True,exist_ok=True)
        subprocess.run(['aws','s3','cp','s3://'+config['bucket']+'/'+identity['key'],str(path),'--only-show-errors'],check=True)
        if path.stat().st_size!=identity['bytes'] or sha(path)!=identity['sha256']:raise ValueError('closed input identity '+name)
    body=(directory/'control/manifest.json').read_bytes()
    if hashlib.sha256(body).hexdigest()!=config['control_root_sha256']:raise ValueError('closed root identity')
    original=json.loads(body)
    sq8_key=prefix+'/objects/'+original['sq8_object_sha256']
    # Unique preregistered namespace; no replacement or multipart approximation.
    subprocess.run(['aws','s3api','put-object','--bucket',config['bucket'],'--key',sq8_key,
        '--body',str(directory/'sq8.bin'),'--if-none-match','*','--metadata','sha256='+original['sq8_object_sha256']],
        check=True,stdout=subprocess.DEVNULL)
    head=json.loads(subprocess.check_output(['aws','s3api','head-object','--bucket',config['bucket'],'--key',sq8_key]))
    if head['ContentLength']!=78000000 or head['Metadata']['sha256']!=original['sq8_object_sha256']:raise ValueError('published SQ8 identity')
    text=body.decode()
    replacements=[(original['sq8_object_key'],sq8_key),(original['sq8_etag'],head['ETag']),
        (original['canonical']['object_key'],prefix+'/objects/'+original['canonical']['sha256'])]
    for before,after in replacements:
        token=json.dumps(before,separators=(',',':'))
        if text.count(token)!=1:raise ValueError('serving binding token')
        text=text.replace(token,json.dumps(after,separators=(',',':')),1)
    bound=json.loads(text)
    restored=dict(bound);restored['canonical']=dict(bound['canonical'])
    for key in ['sq8_object_key','sq8_etag']:restored[key]=original[key]
    restored['canonical']['object_key']=original['canonical']['object_key']
    if json.dumps(restored,sort_keys=True)!=json.dumps(original,sort_keys=True):raise ValueError('non-serving root fields changed')
    (directory/'control/manifest.json').write_text(text)
    bound_sha=sha(directory/'control/manifest.json')
    write(directory/'binding.json',dict(original_root_sha256=config['control_root_sha256'],bound_root_sha256=bound_sha,prefix=prefix,
        sq8_object_key=sq8_key,sq8_etag=head['ETag'],only_serving_bindings_changed=True))
    args=[binary,directory/'control',bound_sha,directory/'requests',config['artifacts']['requests']['sha256']]
    with (directory/'preflight.log').open('x') as log:
        subprocess.run(['timeout','--signal=TERM','--kill-after=10','120',*map(str,args),str(directory/'normal-plans.jsonl'),'0','64'],check=True,stdout=log,stderr=subprocess.STDOUT)
    if sha(directory/'normal-plans.jsonl')!=config['artifacts']['preflight-plans.jsonl']['sha256']:raise ValueError('serving-bound normal plan parity')
    with (directory/'live.log').open('x') as log:
        subprocess.run(['/usr/bin/time','-v','-o',str(directory/'live.time'),'timeout','--signal=TERM','--kill-after=10','300',
            *map(str,args),str(directory/'live.jsonl'),'0','64','--live-s3',config['bucket'],'eu-central-1',prefix],check=True,stdout=log,stderr=subprocess.STDOUT)
    records=[json.loads(line) for line in (directory/'live.jsonl').read_text().splitlines()]
    if len(records)!=66 or records[0]['phase']!='startup' or records[-1]['phase']!='summary':raise ValueError('live record roster')
    startup,summary=records[0],records[-1];queries=records[1:-1]
    if startup['root_sha256']!=bound_sha or startup['generation']!=1 or startup['control_epoch']!=1:raise ValueError('native publication/read/open authority')
    plans=[json.loads(line) for line in (directory/'preflight-plans.jsonl').read_text().splitlines()]
    authority=json.loads((directory/'authority.json').read_text())
    scores=np.load(directory/'scores.npy',mmap_mode='r',allow_pickle=False)
    if scores.shape!=(64,100000) or scores.dtype!=np.dtype('<f4'):raise ValueError('score cache geometry')
    sq8=np.memmap(directory/'sq8.bin',mode='r',dtype=[('id','<i8'),('norm','<f4'),('code','u1',(768,))])
    truth=struct.unpack('<100000I',(directory/'truth').read_bytes());samples=[]
    for i,(record,plan) in enumerate(zip(queries,plans)):
        if record['phase']!='query' or record['query_ordinal']!=i or {k:record[k] for k in ['query_ordinal','ranges','planned_bytes']}!=plan:raise ValueError('live plan parity')
        if record['submitted_gets']!=len(plan['ranges']) or record['verified_bytes']!=plan['planned_bytes'] or record['failed_gets']!=0 or record['submitted_gets']>32 or record['verified_bytes']>16773120:raise ValueError('physical read accounting/caps')
        positions=np.concatenate([np.arange(start//780,end//780) for start,end in plan['ranges']])
        expected=sq8['id'][positions][np.lexsort((sq8['id'][positions],scores[i,positions]))[:100]].tolist()
        if record['ids']!=expected:raise ValueError('native returned-ID parity ordinal'+str(i))
        hits=len(set(expected)&set(truth[i*100:(i+1)*100]))
        if hits!=authority['preflight'][i]['returned_hits']:raise ValueError('returned count parity')
        samples.append(dict(query_ordinal=i,returned_hits=hits))
    values=[r['query_wall_ns']/1e6 for r in queries]
    result=dict(decision='PASS serving integration only',qualification=False,dataset=config['dataset'],split='consumed development0-63',k=100,
        returned_hits=sum(s['returned_hits'] for s in samples),returned_recall_percent=sum(s['returned_hits'] for s in samples)/64,
        p05_returned_hits=sorted(s['returned_hits'] for s in samples)[3],samples=samples,
        complete_library_call_latency_ms=dict(zip(['p50','p90','p95','p99'],np.quantile(values,[.5,.9,.95,.99]).tolist())),
        serial_observed_qps=64/(summary['measurement_wall_ns']/1e9),measurement_wall_ns=summary['measurement_wall_ns'],
        data_get_attempts=sum(r['submitted_gets'] for r in queries),verified_data_bytes=sum(r['verified_bytes'] for r in queries),failed_data_gets=0,
        root_sha256=bound_sha,startup=startup,cache_state='client SQ8 cache absent; router resident; connections reused; S3 server cache uncontrolled',
        scope=config['scope'],query_scoring_kernel_calls=0,score_cache_sort_parity_queries=64)
    write(directory/'result.json',result)


if __name__=='__main__':
    try:main()
    finally:
        group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
        if len(sys.argv)==7 and Path(sys.argv[4]).is_dir() and (group/'memory.peak').exists():
            write(Path(sys.argv[4])/'cgroup.json',{k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()})
