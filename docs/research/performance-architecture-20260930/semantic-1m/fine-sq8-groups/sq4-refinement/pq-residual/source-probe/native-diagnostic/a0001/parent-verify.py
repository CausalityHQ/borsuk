import json,hashlib,struct,sys
from pathlib import Path
sys.path.insert(0,'/home/rb/worktrees/borsuk-prod-ready-v9')
from scripts import launch_fine_pack_diagnostic as c
c.configure_pq_residual_source()
out=Path('/home/rb/worktrees/borsuk-pq-residual-source-probe-launch-a0001')/c.ROOT/'a0001'
replay=c.replay(out)
t=json.loads((out/'aws-terminal.json').read_bytes());report=json.loads((out/'screen/report.json').read_bytes())
for n,p in t['artifacts'].items():
 b=(out/n).read_bytes();assert len(b)==p['bytes'] and hashlib.sha256(b).hexdigest()==p['sha256'],n
results=json.loads((out/'screen/report.pq-residual-results.json').read_bytes());selection=json.loads((out/'screen/report.pq-residual-selections.json').read_bytes());anchors=json.loads((out/'screen/report.pq-residual-anchors.json').read_bytes())
assert len(results)==len(anchors)==128
summary=[]
for panel_index,dataset in enumerate(['relaion','cohere']):
 cohort=selection['panels'][panel_index]['cohort'];assert len(cohort)==4096 and [r['logical'] for r in cohort]==[j*100000//4096 for j in range(4096)]
 members={r['id']:r['physical'] for r in cohort};assert len(members)==4096
 overlaps=[]
 for ordinal in range(64):
  i=panel_index*64+ordinal;r=results[i];a=anchors[i]
  assert r['dataset']==a['dataset']==dataset and r['anchor_cohort_index']==a['cohort_index']==64*ordinal
  assert r['anchor_logical']==a['row']['logical']==cohort[64*ordinal]['logical'] and a['row']==cohort[64*ordinal]
  assert r['query_sha256']==a['prepared_sha256']==hashlib.sha256(b''.join(struct.pack('<I',b) for b in a['prepared_bits'])).hexdigest()
  sets=[]
  for arm in ['native','candidate']:
   rows=r[arm];assert len(rows)==100;ids={v['id'] for v in rows};assert len(ids)==100 and a['row']['id'] not in ids
   assert all(members[v['id']]==v['physical'] for v in rows)
   sets.append(ids)
  overlaps.append(len(sets[0]&sets[1]))
 actual={'dataset':dataset,'overlaps':overlaps,'p05':sorted(overlaps)[3],'sum':sum(overlaps),'passed':sum(overlaps)>=6336 and sorted(overlaps)[3]>=98}
 assert actual==report['details']['panels'][panel_index]
 summary.append(dict(dataset=dataset,source_top100_overlap_mean=sum(overlaps)/6400,p05_overlap_hits=sorted(overlaps)[3],sum_hits=sum(overlaps),passed=actual['passed']))
assert report['status']==('SURVIVED_SOURCE_NEIGHBORHOODS' if all(p['passed'] for p in summary) else 'REJECT')==replay['status']
resources=json.loads((out/'resources.json').read_bytes());assert resources['closed'] and not resources['descendants_remaining_after_exit']
proof={'schema':'borsuk-pq-residual-source-parent-verification-v1','verified':True,'valid_diagnostic':True,'status':report['status'],'source_commit':t['source_commit'],'native_source_commit':t['qualification']['native_source_commit'],'instance_id':t['instance_id'],'artifact_count':len(t['artifacts']),'all_artifacts_independently_hashed':True,'sealed_results_independently_recounted':128,'summary':summary,'native_process_wall_seconds':resources['elapsed_seconds'],'native_memory_peak_bytes':int(resources['after']['files']['memory.peak']),'native_swap_peak_bytes':int(resources['after']['files']['memory.swap.peak']),'original_native_exit_status':0,'same_instance_terminated':True,'requests_opened':False,'truth_opened':False,'quality_or_performance_claim':False,'next_action':'stop rejected arm; no matched GT refinement or parameter retuning'}
Path('/tmp/borsuk-pq-source-a0001-parent-verification.json').write_text(json.dumps(proof,indent=2,sort_keys=True)+'\n');print(json.dumps(proof,sort_keys=True))
