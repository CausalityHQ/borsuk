import json,gzip,hashlib,tempfile,sys
from pathlib import Path
root=Path('/home/rb/worktrees/borsuk-prod-ready-v9');sys.path.insert(0,str(root))
from scripts import run_native_semantic_router_cold as w
from scripts import launch_native_semantic_router_cold_spot as ctl
base=root/ctl.ROOT/'offered/a0003';terminal=json.loads((base/'aws-terminal.json').read_bytes())
bodies={}
for n,i in terminal['artifacts'].items():
 b=gzip.decompress((base/(n+'.gz')).read_bytes());assert len(b)==i['bytes'] and hashlib.sha256(b).hexdigest()==i['sha256'],n;bodies[n]=b
assert len(bodies)==108
config=json.loads(bodies['screen/config.json']);digest=hashlib.sha256(bodies['screen/config.json']).hexdigest();assert digest==terminal['config_sha256'];w.validate_config(config)
saved=json.loads(bodies['screen/summary.json'])
with tempfile.TemporaryDirectory() as tmp:
 out=Path(tmp)
 for n,b in bodies.items():
  p=out/n;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
 w.validate_runtime(config,out/'binaries/two_bit_http',out/'screen/qualification.json')
 def fetch(bucket,identity,path):
  b=path.read_bytes();sha=w.input_sha(identity);size=identity.get('range_bytes',identity['bytes']);assert len(b)==size and hashlib.sha256(b).hexdigest()==sha
  return dict(path=str(path),bytes=size,sha256=sha)
 panels=w.prepare(config,out/'screen',fetch=fetch)
 records=[];saved_cells=[]
 for index,rate,dataset,arm in w.offered_order():
  n='screen/'+w.offered_name(index,dataset,arm);body=bodies[n];rows=[json.loads(x) for x in body.splitlines()]
  assert body==''.join(w.encoded(r)+'\n' for r in rows).encode();records.extend(rows)
  saved_cells.append(json.loads(bodies[n.replace('-records.jsonl','-summary.json')]))
 actual=w.reduce_offered(records,panels,config)
 for cell,saved_cell in zip(actual['cells'],saved_cells):assert saved_cell==w.closed_cell_summary(cell,config,digest)
 for name,value in actual.items():assert saved[name]==value,name
 for dataset,p in panels.items():
  for name,x in p['inputs'].items():
   s=saved['inputs'][dataset]['common'][name];assert (x['bytes'],x['sha256'])==(s['bytes'],s['sha256'])
  for arm,a in p['arms'].items():
   for name,x in a['inputs'].items():
    s=saved['inputs'][dataset]['arms'][arm][name];assert (x['bytes'],x['sha256'])==(s['bytes'],s['sha256'])
 assert saved['binary_sha256']==config['binary']['sha256'] and saved['qualification_sha256']==config['qualification_sha256']
 profile=json.loads(bodies['profile-cgroup.json']);assert ctl._profile_resources(profile)
 failed=[r for r in records if r['outcome']=='failed'];assert len(failed)==1
 failure=failed[0]
 result=dict(schema='borsuk-native-semantic-offered-a3-independent-replay-v1',source_commit=terminal['source_commit'],source_archive_sha256=terminal['source_archive_sha256'],config_sha256=digest,terminal_sha256=hashlib.sha256((base/'aws-terminal.json').read_bytes()).hexdigest(),authenticated_artifacts=len(bodies),records=len(records),saved_all_cells_and_summary_parity=True,input_bodies_authenticated=True,input_paths='operational remote paths preserved; byte/hash identities compared to independently authenticated local copies',execution_status='FAIL preserved',original_exit_status=1,scientific_qualification='FAIL; global fatal under frozen classifier',failed_call={k:failure[k] for k in ['dataset','arm','rate_index','offered_qps','query_ordinal','http_status','failure_kind','error_type','error','cleanup_confirmed','telemetry_validation_errors']},failed_response=failure['response'],summary=actual,profile_cgroup=profile)
 (base/'independent-verification.json').write_text(json.dumps(result,indent=2)+'\n')
 print('ALL108/1536raw ledger/source+scorer+transport+resource/24markers/summaryparity verified; originalFAIL retained')
 print('attained',actual['largest_passing_tested_offered_qps'],'successful',actual['ann_calls_successful'],'failed',actual['failed_calls'],'aborted',actual['aborted_calls'])
