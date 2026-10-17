from pathlib import Path
import json,hashlib,math
p=Path('/mnt/borsuk-workspace/evidence/leaf-prefix40-abba-run-a0001')
o=Path('/mnt/borsuk-workspace/evidence/leaf-prefix40-abba-closed-a0001')
assert (p/'terminal').read_text().strip()=='ABBA_LATENCY_SCREEN_CLOSED'
assert (p/'manager.exit').read_text().strip()=='0'
o.mkdir()
def dig(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def stats(a):
 s=sorted(a);n=len(s)
 return dict(count=n,p50_ms=s[math.ceil(.50*n)-1]/1e6,p90_ms=s[math.ceil(.90*n)-1]/1e6,p95_ms=s[math.ceil(.95*n)-1]/1e6,p99_ms=s[math.ceil(.99*n)-1]/1e6,total_ns=sum(s))
rows={};reports={};times={};stages={};cells=[]
for name,leaf in [('A1',48),('B1',40),('B2',40),('A2',48)]:
 cell=f'{name}.leaf{leaf}'
 for suffix in ['exit','native.exit','timeout.exit','drain.exit','cleanup.exit','reader.exit']:assert (p/f'{cell}.{suffix}').read_text().strip()=='0'
 rp=p/f'{cell}.reader-report.json';r=json.loads(rp.read_bytes());t=r['native_terminal']
 assert r['complete'] and r['status']=='MEASURED' and r['identity']['semantic_leaf_limit']==leaf
 assert t['all_queries_sealed'] and t['complete'] and r['statistics']['count']==1000
 assert t['queries']==1000 and t['recall_denominator']==10000 and t['underfilled_queries']==0
 h=hashlib.sha256();qs={};hits={};ts={};ss={k:[] for k in ['discovery','source','planning','sq8']}
 with (p/f'{cell}.jsonl').open('rb') as f:
  for raw in f:
   h.update(raw);v=json.loads(raw)
   if v.get('phase')=='query':
    i=v['ordinal'];assert i not in qs
    qs[i]=dig({k:v[k] for k in ['returned','returned_physical_ordinals','plan','trace','charges','sum','serving','source_nomination_skipped','planning_scope']})
    ts[i]=v['query_wall_ns']
    for k in ss:
     z=v['stages'][k];assert z['end_ns']>=z['start_ns'];ss[k].append(z['end_ns']-z['start_ns'])
   elif v.get('phase')=='recall':
    i=v['ordinal'];assert i not in hits;hits[i]=v['hits10']
 assert h.hexdigest()==r['result_sha256'] and set(qs)==set(hits)==set(ts)==set(range(1000))
 assert sum(hits.values())==t['recall_numerator']
 assert sum(ts.values())==t['query_wall_ns']
 canonical=json.loads((Path(__file__).parent/'canonical'/f'leaf{leaf}.json').read_bytes())
 assert qs=={int(k):v for k,v in canonical['queries'].items()}
 assert hits=={int(k):v for k,v in canonical['hits'].items()}
 assert t['recall_numerator']==canonical['recall_numerator']
 assert t['charges']==canonical['charges'] and t['sum']==canonical['sum']
 rows[name]=(qs,hits);times[name]=ts
 reports[name]=dict(cell=name,leaf_limit=leaf,result_sha256=r['result_sha256'],reader_report_sha256=hashlib.sha256(rp.read_bytes()).hexdigest(),config_sha256=r['identity']['config_sha256'],recall_hits=t['recall_numerator'],recall_denominator=10000,recall_at_10=t['mean_recall10'],statistics=r['statistics'],max_query_ms=max(ts.values())/1e6,query_cpu_ns=t['query_process_cpu_ns'],query_wall_ns=t['query_wall_ns'],observed_process_peak_bytes=t['observed_process_peak_bytes'],logical_charges=t['charges'],physical_transport=t['transport_last_boundary'],process_cpu_ns=t['process_cpu_ns'],process_wall_ns=t['process_wall_ns'],logical_gets=t['sum']['submitted_gets'],logical_verified_bytes=t['sum']['verified_bytes'],stage_descriptive={k:stats(v) for k,v in ss.items()})
 cells.append(reports[name])
same={}
for a,b in [('A1','A2'),('B1','B2')]:
 same[a+'_'+b]=dict(canonical_equal_queries=sum(rows[a][0][i]==rows[b][0][i] for i in range(1000)),recall_rows_equal=rows[a][1]==rows[b][1],logical_charges_equal=reports[a]['logical_charges']==reports[b]['logical_charges'])
 assert same[a+'_'+b]==dict(canonical_equal_queries=1000,recall_rows_equal=True,logical_charges_equal=True)
pairs=[]
for a,b in [('A1','B1'),('A2','B2')]:
 ar=reports[a];br=reports[b]
 pairs.append(dict(control=a,candidate=b,p90_ratio=br['statistics']['p90_ns']/ar['statistics']['p90_ns'],p95_ratio=br['statistics']['p95_ns']/ar['statistics']['p95_ns'],wall_ratio=br['query_wall_ns']/ar['query_wall_ns'],cpu_ratio=br['query_cpu_ns']/ar['query_cpu_ns'],logical_byte_ratio=br['logical_verified_bytes']/ar['logical_verified_bytes'],logical_get_ratio=br['logical_gets']/ar['logical_gets']))
d=[rows['B1'][1][i]-rows['A1'][1][i] for i in range(1000)]
doc=dict(schema='borsuk-semantic-leaf-prefix-abba-screen-v1',source_revision='a0463ed097296c19539d05529ddad615be760fdd',delivery_base='beeb3c8218698ccf0c569b3980112bf1b62c0184',instance='i-00bf80e4d5592efc8',order=['A1','B1','B2','A2'],cells=cells,same_arm_parity=same,temporal_pair_ratios=pairs,recall_delta=dict(net_hits=sum(d),percentage_points=sum(d)/100,queries_improved=sum(x>0 for x in d),queries_worsened=sum(x<0 for x in d),queries_same=sum(x==0 for x in d)),pooled_descriptive_48=stats(list(times['A1'].values())+list(times['A2'].values())),pooled_descriptive_40=stats(list(times['B1'].values())+list(times['B2'].values())),independent_queries=1000,repetitions_per_arm=2,pooled_2000_independent=False,cache_app='OFF',cache_backend='UNKNOWN',sdk_context={'recall_percent':96.17,'p95_ms':206,'descriptive_only':True},default_promoted=False,universal_cold_claim=False,concurrent_qps_claim=False,competitor_win_claim=False)
with (o/'summary.json').open('x') as f:json.dump(doc,f,sort_keys=True,indent=2);f.write('\n')
print(json.dumps(doc,sort_keys=True))
