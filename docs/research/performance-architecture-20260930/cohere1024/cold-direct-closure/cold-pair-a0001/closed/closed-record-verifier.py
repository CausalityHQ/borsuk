"""Closed native-record/resource audit only. No corpus, query or truth decoding/scoring."""
import json,hashlib,subprocess,math
from pathlib import Path
r=Path('/data/target/borsuk-cold-membership-native/direct-closure-cold-pair-a0001');p=json.loads((r/'protocol.json').read_text());t=json.loads((r/'terminal.json').read_text());col=r/'collected';i=json.loads((r/'launch.json').read_text())['Instances'][0]['InstanceId']
assert t['instance_id']==i and t['binary_source_commit']==p['candidate'] and t['qualified_source_identity_sha256']==p['qualification']['native_source_identity_sha256']
assert t['exit']==t['original_exit']==t['native_exit']==0 and t['performance_claim'] is False
assert json.loads((r/'wait.json').read_text())['exit']==0
for line in (r/'artifacts.sha256').read_text().splitlines():
 h,path=line.split('  ',1);assert '..' not in Path(path).parts;assert hashlib.sha256((col/path.removeprefix('./')).read_bytes()).hexdigest()==h,path
for name in ['A.config.json','B.config.json','stage.sh']:
 assert (col/name).read_bytes()==(r/name).read_bytes();assert hashlib.sha256((r/name).read_bytes()).hexdigest()==p['inputs'][name]['sha256']
for name,value in {'cpu.max':'100000 100000','memory.max':'536870912','memory.swap.max':'0','pids.max':'256'}.items():assert (col/(name+'.before')).read_text().strip()==value
assert int((col/'memory.peak.after').read_text())<=536870912
assert (col/'memory.swap.peak.after').read_text().strip()=='0'
events=dict(x.split() for x in (col/'memory.events.after').read_text().splitlines());assert events['oom']==events['oom_kill']=='0'
assert not (col/'scratch-files.txt').read_text().strip()
assert (col/'final-exit').read_text().strip()=='0'
for name in ['pins-before.txt','pins-after.txt','A.pins-before.txt','A.pins-after.txt','B.pins-before.txt','B.pins-after.txt']:
 lines=(col/name).read_text().splitlines();assert len(lines)==6 and all(s.endswith(': OK') for s in lines),name
for arm in ['A','B']:assert (col/(arm+'.native-exit')).read_text().strip()=='0'
state=(col/'systemd-after.txt').read_text();assert 'MainPID=0' in state and 'ActiveState=inactive' in state
expected={name:hashlib.sha256(subprocess.check_output(['git','show',p['candidate']+':'+path])).hexdigest() for name,path in [('runner_source_sha256','crates/borsuk/src/bin/check_cohere_native_baseline.rs'),('generation_source_sha256','crates/borsuk/src/two_bit_generation.rs'),('router_source_sha256','crates/borsuk/src/semantic_unit_router.rs'),('codec_source_sha256','crates/borsuk/src/rotated_two_bit.rs'),('source_plane_source_sha256','crates/borsuk/src/two_bit_source.rs')]}
def audit(arm):
 path=col/(arm+'.jsonl');cfg=json.loads((r/(arm+'.config.json')).read_text());queries=[];recalls=[];prefix=hashlib.sha256();offset=0;sealed=None;summary=None;charges={n:{k:0 for k in ['submitted_gets','verified_bytes','failed_gets']} for n in ['router','source','sq8']}
 with path.open('rb') as f:
  for raw in f:
   assert len(raw)<=65536 and raw.endswith(b'\n');d=json.loads(raw);phase=d['phase']
   if phase=='identity':
    assert offset==0 and d['schema']=='borsuk-cohere-native-baseline-result-v3';assert d['binary_sha256']==p['binary']['sha256'] and d['config_sha256']==p['inputs'][arm+'.config.json']['sha256'];assert all(d[k]==v for k,v in expected.items());assert d['serving']==cfg['serving']
   if phase=='bound_inputs':
    for k in ['rows','count','dimensions','k','metric','generation_root_sha256','generation_prefix','fetch_parallelism']:assert d[k]==cfg[k],k
    assert d['requests_sha256']==cfg['requests']['sha256'] and d['truth_sha256']==cfg['truth']['sha256']
   if phase=='query':
    assert sealed is None and len(queries)<1000 and d['ordinal']==len(queries) and d['truth_opened'] is False
    assert d['returned_count']==10 and d['underfill'] is False and d['serving']==cfg['serving'];assert len({h['id'] for h in d['returned']})==10
    assert d['source_nomination_skipped']==(arm=='B') and d['planning_scope']==('direct_cover_only' if arm=='B' else 'source_nomination_and_cover')
    for n in charges:
     for k in charges[n]:charges[n][k]+=d['charges'][n][k]
    assert d['charges']['router']=={'submitted_gets':0,'verified_bytes':0,'failed_gets':0}
    if arm=='B':assert d['charges']['source']=={'submitted_gets':0,'verified_bytes':0,'failed_gets':0};assert d['charges']['sq8']['submitted_gets']<=32 and d['charges']['sq8']['verified_bytes']<=52428800
    assert d['query_wall_ns']>0 and d['query_process_cpu_ns']>=0;queries.append(d)
   if phase=='all_queries_sealed':
    assert sealed is None and len(queries)==1000 and d['count']==1000 and d['truth_opened'] is False
    assert d['prefix_bytes']==offset and d['prefix_sha256']==prefix.hexdigest();assert d['requires_successful_sync'] is True and d['requires_successful_directory_sync'] is True
    prefix.update(raw);offset+=len(raw);sealed={'prefix_bytes':d['prefix_bytes'],'prefix_sha256':d['prefix_sha256'],'sealed_bytes':offset,'sealed_sha256':prefix.hexdigest()};continue
   if phase=='recall':
    assert sealed is not None and d['ordinal']==len(recalls);assert 0<=d['hits10']<=10;recalls.append(d['hits10'])
   if phase=='terminal':
    assert summary is None;summary=d['summary'];assert summary['status']=='MEASURED' and summary['complete'] is True and summary['queries']==1000 and summary['underfilled_queries']==0
   if sealed is None:prefix.update(raw);offset+=len(raw)
 assert summary is not None and sealed is not None and len(recalls)==1000
 assert all(summary[k]==v for k,v in sealed.items());assert summary['charges']==charges
 assert summary['recall_numerator']==summary['total_hits10']==sum(recalls) and summary['recall_denominator']==10000
 assert summary['query_wall_ns']==sum(q['query_wall_ns'] for q in queries) and summary['query_process_cpu_ns']==sum(q['query_process_cpu_ns'] for q in queries)
 assert summary['all_queries_sealed'] is True and summary['requests_sha256']==cfg['requests']['sha256'] and summary['truth_sha256']==cfg['truth']['sha256']
 return queries,recalls,summary
A,ar,at=audit('A');B,br,bt=audit('B')
references=[('B1.jsonl',10777291,'29da105428503267e73e9e23e1a61bef9967ccdc9fb4f43ef64b23d306ba0cbb'),('B2.jsonl',10777322,'095a0487e1b93112b0c6d4c838d127acadedcb7a49878691208d8f32dc1cd3f5')]
for name,size,sha in references:
 path=r.parent/'membership-abba-a0001'/'collected'/name;assert path.stat().st_size==size;h=hashlib.sha256()
 with path.open('rb') as f:
  for block in iter(lambda:f.read(1048576),b''):h.update(block)
 assert h.hexdigest()==sha
 with path.open() as f:
  n=0
  for line in f:
   q=json.loads(line)
   if q['phase']=='query':assert q['ordinal']==n and q['returned']==A[n]['returned'] and q['charges']==A[n]['charges'] and q['trace']==A[n]['trace'];n+=1
 assert n==1000
assert hashlib.sha256((r.parent/'source-utilization-a0002'/'collected'/'source-utilization.json').read_bytes()).hexdigest()=='0e9d9ba9fb913d9debb2d787b9403092f3079cc87c10760e332d0ec949d67987'
pred=json.loads((r.parent/'source-utilization-a0002'/'collected'/'source-utilization.json').read_text());assert pred['complete'] is True and pred['count']==1000
for q,model in zip(B,pred['queries']):
 assert q['ordinal']==model['ordinal'];assert q['charges']['sq8']['submitted_gets']==model['direct_closure_sq8_gets'] and q['charges']['sq8']['verified_bytes']==model['direct_closure_sq8_bytes'],'INVALID_PLAN_PARITY'
 assert q['plan']['planned_bytes']==model['direct_closure_sq8_bytes']
deltas=[b-a for a,b in zip(ar,br)]
def metrics(queries,hits,summary,arm):
 times=sorted(q['query_wall_ns']/1e6 for q in queries)
 qs={f'p{int(prob*100)}_ms':times[math.ceil(prob*len(times))-1] for prob in [.5,.9,.95,.99]}
 qs.update(recall_at_10=sum(hits)/10000,recall_p05=sorted(hits)[math.ceil(.05*len(hits))-1]/10,serial_query_QPS=1e9*len(queries)/sum(q['query_wall_ns'] for q in queries),query_wall_seconds=sum(q['query_wall_ns'] for q in queries)/1e9,query_process_CPU_seconds=sum(q['query_process_cpu_ns'] for q in queries)/1e9)
 qs['stage_scope_seconds']={name:sum(q['stages'][name]['end_ns']-q['stages'][name]['start_ns'] for q in queries)/1e9 for name in ['discovery','source','planning','sq8']}
 transport={key:0 for key in ['attempts','transport_failures','stream_failures','consumed_payload_bytes','dropped_error_bodies']};methods=[0]*10;statuses={}
 for query in queries:
  before=query['transport']['before'];after=query['transport']['after']
  for key in transport:
   delta=after[key]-before[key];assert delta>=0;transport[key]+=delta
  assert len(before['method_counts'])==len(after['method_counts'])==10
  for index,(start,end) in enumerate(zip(before['method_counts'],after['method_counts'])):
   assert end>=start;methods[index]+=end-start
  old=dict(before['status_counts']);new=dict(after['status_counts'])
  for status in set(old)|set(new):
   delta=new.get(status,0)-old.get(status,0);assert delta>=0;statuses[str(status)]=statuses.get(str(status),0)+delta
 transport['method_counts']=methods;transport['status_counts']=statuses;transport['attempts_minus_logical_GETs']=transport['attempts']-sum(c['submitted_gets'] for c in summary['charges'].values());qs['native_query_transport']=transport
 qs['charges']=summary['charges'];qs['transport_scope']='cumulative process native HTTP boundaries, not wire/billed S3';qs['native_time_receipt']=arm+'.time';qs['modelled_direct_memory']=summary.get('direct_memory')
 return qs
ma=metrics(A,ar,at,'A');mb=metrics(B,br,bt,'B');ratios={'p90_B_over_A':mb['p90_ms']/ma['p90_ms'],'p95_B_over_A':mb['p95_ms']/ma['p95_ms'],'serial_QPS_B_over_A':mb['serial_query_QPS']/ma['serial_query_QPS']}
quality=sum(br)>=9723;speed=ratios['p90_B_over_A']<=1 and ratios['p95_B_over_A']<=1 and ratios['serial_QPS_B_over_A']>=1
assert p['performance_method']['quantiles']=='nearest-rank ceil(p*N), no interpolation'
result={'status':'COLD_PAIR_OBSERVED_GATE_PASS' if quality and speed else 'QUALITY_REJECT' if not quality else 'COLD_PAIR_PERFORMANCE_REJECT','candidate':p['candidate'],'instance_id':i,'terminated':True,'protocol_sha256':hashlib.sha256((r/'protocol.json').read_bytes()).hexdigest(),'A':ma,'B':mb,'ratios':ratios,'kernel_cgroup_peak_bytes':int((col/'memory.peak.after').read_text()),'all1000_baseline_ordered_ID_scorebit_trace_charge_parity_with_historical_B1_B2':True,'direct_GET_byte_closed_prediction_parity_all1000':True,'sealed_before_truth_both':True,'queries_worse':sum(d<0 for d in deltas),'queries_better':sum(d>0 for d in deltas),'per_query_hit_delta':deltas,'application_cache':'off','backend_cache':'uncontrolled','temporal_order_bias':'A then B on same host; no strict backend-cold claim','vendor_win_claim':False,'total_lifecycle_cost_win_claim':False,'scope':'one frozen 100k/1000/D1024/cosine/k10 paired query latency/throughput falsifier'}
(r/'independent-verification.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k!='per_query_hit_delta'}))
