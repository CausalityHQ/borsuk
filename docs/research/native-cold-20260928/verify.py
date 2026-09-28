"""Closed native serving evidence: hashes, GT reduction and clocks; no queries."""
import gzip,hashlib,io,json,math,struct,sys,tarfile
from pathlib import Path
import boto3

out=Path(sys.argv[1]);launch=json.loads((out/'aws-launch.json').read_text())
terminal_raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(terminal_raw)
close=json.loads((out/'aws-closeout.json').read_text())
assert terminal['status']=='complete' and terminal['exit_code']==0
assert close['state']=='terminated' and close['instance_id']==terminal['instance_id']==launch['instance_id']
assert hashlib.sha256(terminal_raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert terminal['source_archive_sha256']==launch['source_archive_sha256'] and terminal['source_base_commit']==launch['source_base_commit']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3')
assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'

def get(name):
    local=out/(name+'.gz')
    data=gzip.decompress(local.read_bytes()) if local.is_file() else s3.get_object(Bucket=launch['bucket'],Key=launch['prefix']+'/artifacts/'+name)['Body'].read()
    identity=terminal['artifacts'][name]
    assert len(data)==identity['bytes'] and hashlib.sha256(data).hexdigest()==identity['sha256']
    return data

verified=[]
for name in terminal['artifacts']:
    get(name);verified.append(name)
assert 'test tests::live_scope_is_development_only ... ok' in get('unit-check.log').decode()
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read()
assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256']
matched=[]
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name)
        if path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock'] or member.name in ['scripts/run_native_cold.py','docs/research/native-cold-20260928/config.json','docs/research/native-cold-20260928/plan.md','docs/research/native-cold-20260928/preregister.md','docs/research/native-cold-20260928/aws-green.py']:
            assert path.read_bytes()==tar.extractfile(member).read(),str(path);matched.append(member.name)
config=json.loads(Path('docs/research/native-cold-20260928/config.json').read_text())
reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(Path('docs/research/native-cold-20260928/config.json').read_bytes()).hexdigest()==reservation['config_sha256']
binding=json.loads(get('live/binding.json'));result=json.loads(get('live/result.json'))
body=get('live/control/manifest.json');root=json.loads(body)
assert hashlib.sha256(body).hexdigest()==result['root_sha256']==binding['bound_root_sha256']
assert binding['original_root_sha256']==config['control_root_sha256'] and binding['prefix']==reservation['serving_namespace']
identity=config['artifacts']['control/manifest.json']
old=s3.get_object(Bucket=config['bucket'],Key=identity['key'])['Body'].read()
assert len(old)==identity['bytes'] and hashlib.sha256(old).hexdigest()==identity['sha256']==config['control_root_sha256']
original=json.loads(old);restored=body.decode()
for before,after in [(root['sq8_object_key'],original['sq8_object_key']),(root['sq8_etag'],original['sq8_etag']),(root['canonical']['object_key'],original['canonical']['object_key'])]:
    token=json.dumps(before,separators=(',',':'));assert restored.count(token)==1
    restored=restored.replace(token,json.dumps(after,separators=(',',':')),1)
assert restored.encode()==old
head_bytes=s3.get_object(Bucket=config['bucket'],Key=binding['prefix']+'/head.json')['Body'].read();assert 0<len(head_bytes)<=4096
head=json.loads(head_bytes)
assert head['root_sha256']==binding['bound_root_sha256'] and head['generation']==1 and head['epoch']==1 and head['schema']=='borsuk-two-bit-head-v2' and head['mutation'] is None and head['fence'] is None
remote_prefix=binding['prefix']+'/generations/'+binding['bound_root_sha256']
assert s3.get_object(Bucket=config['bucket'],Key=remote_prefix+'/manifest.json')['Body'].read()==body
for name,key in [('page_manifest.json','page_manifest_sha256'),('graph.bin','graph_sha256'),('centroids.bin','centroids_sha256'),('plane/manifest.json','plane_manifest_sha256')]:
    data=s3.get_object(Bucket=config['bucket'],Key=remote_prefix+'/'+name)['Body'].read()
    assert hashlib.sha256(data).hexdigest()==root[key]
for key,expected in [(root['sq8_object_key'],78000000),(root['canonical']['object_key'],308000000)]:
    meta=s3.head_object(Bucket=config['bucket'],Key=key);assert meta['ContentLength']==expected
    if key==root['sq8_object_key']:assert meta['ETag']==root['sq8_etag'] and meta['Metadata']['sha256']==root['sq8_object_sha256']
assert hashlib.sha256(get('live/normal-plans.jsonl')).hexdigest()==config['artifacts']['preflight-plans.jsonl']['sha256']
records=[json.loads(line) for line in get('live/live.jsonl').splitlines()]
assert len(records)==66 and records[0]['phase']=='startup' and records[-1]['phase']=='summary'
assert records[0]==result['startup'] and records[0]['root_sha256']==result['root_sha256']
assert records[-1]['count']==64 and records[-1]['measurement_wall_ns']==result['measurement_wall_ns']
plans=[json.loads(line) for line in get('live/normal-plans.jsonl').splitlines()]
truth_identity=config['artifacts']['truth'];truth_bytes=s3.get_object(Bucket=config['bucket'],Key=truth_identity['key'])['Body'].read()
assert len(truth_bytes)==truth_identity['bytes'] and hashlib.sha256(truth_bytes).hexdigest()==truth_identity['sha256']
truth=struct.unpack('<100000I',truth_bytes)
authority_identity=config['artifacts']['authority.json'];authority_bytes=s3.get_object(Bucket=config['bucket'],Key=authority_identity['key'])['Body'].read()
assert hashlib.sha256(authority_bytes).hexdigest()==authority_identity['sha256']
authority=json.loads(authority_bytes);samples=[]
for i,(record,plan) in enumerate(zip(records[1:-1],plans)):
    assert record['phase']=='query' and record['query_ordinal']==i
    assert {k:record[k] for k in ['query_ordinal','ranges','planned_bytes']}==plan
    assert type(record['submitted_gets']) is int and record['submitted_gets']==len(plan['ranges'])<=32
    assert record['verified_bytes']==plan['planned_bytes']==sum(b-a for a,b in plan['ranges'])<=16773120 and record['failed_gets']==0
    assert len(record['ids'])==len(set(record['ids']))==100 and all(type(v) is int and 0<=v<100000 for v in record['ids'])
    assert all(type(record[k]) is int and record[k]>0 for k in ['query_wall_ns','query_process_cpu_ns'])
    hits=len(set(record['ids'])&set(truth[i*100:(i+1)*100]));assert hits==authority['preflight'][i]['returned_hits']
    samples.append(dict(query_ordinal=i,returned_hits=hits))
assert samples==result['samples'] and sum(x['returned_hits'] for x in samples)==result['returned_hits']==6346
assert result['returned_recall_percent']==99.15625 and result['p05_returned_hits']==97
assert not result['qualification'] and result['decision']=='PASS serving integration only'
# This original worker field counts Python-wrapper kernels, not native search.
assert result['query_scoring_kernel_calls']==0 and result['score_cache_sort_parity_queries']==64

def percentiles(values):
    values=sorted(values);answer={}
    for label,q in zip(['p50','p90','p95','p99'],[.5,.9,.95,.99]):
        at=(len(values)-1)*q;low=math.floor(at);high=math.ceil(at)
        answer[label]=values[low]+(values[high]-values[low])*(at-low)
    return answer

latency=percentiles([r['query_wall_ns']/1e6 for r in records[1:-1]])
for key,value in latency.items():assert math.isclose(result['complete_library_call_latency_ms'][key],value,rel_tol=1e-12)
qps=64/(records[-1]['measurement_wall_ns']/1e9);assert result['serial_observed_qps']==qps
assert result['data_get_attempts']==sum(r['submitted_gets'] for r in records[1:-1])
assert result['verified_data_bytes']==sum(r['verified_bytes'] for r in records[1:-1])
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],source_files_matched=len(matched),terminal_sha256=hashlib.sha256(terminal_raw).hexdigest(),verified_artifacts=len(verified),published_head_root_verified=True,gt_reduction_verified=True,returned_hits=6346,returned_recall_percent=99.15625,complete_library_call_latency_ms=latency,query_process_cpu_ms=percentiles([r['query_process_cpu_ns']/1e6 for r in records[1:-1]]),serial_observed_qps=qps,data_get_attempts=result['data_get_attempts'],verified_data_bytes=result['verified_data_bytes'],new_exhaustive_sq8_kernel_calls=0,native_search_calls=len(records[1:-1]),qualification=False)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
