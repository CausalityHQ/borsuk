import importlib.util,json,pathlib,hashlib,time
root=pathlib.Path('/home/rb/worktrees/borsuk-prod-ready-v9')
script=pathlib.Path('/tmp/borsuk-global-leaf-reducer-review-31232bdd/reduce_hierarchical_global_leaf_coverage.py')
spec=importlib.util.spec_from_file_location('coverage_review',script);mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
base=root/'docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/paired100k'
def pin(p):
 b=p.read_bytes();return dict(path=str(p),bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
audit=json.loads((base/'a0001/independent-coverage-audit.json').read_text()); config={'historical_control':{'terminal':pin(base/'a0001/aws-terminal.json'),'datasets':{}},'datasets':{}};sealed={}
for name in mod.DATASETS:
 cfg=base/f'a0001/screen/measurement/{name}-diagnose.json';diag=base/f'a0001/screen/measurement/{name}-diagnostic.jsonl';old=json.loads(cfg.read_text());config['historical_control']['datasets'][name]=dict(diagnostic_config=pin(cfg),diagnostic=pin(diag));config['datasets'][name]={k:old[k] for k in ['candidate_root','requests','truth']};events=[json.loads(x) for x in diag.read_text().splitlines()];rows=[]
 for event in events[1:65]:
  t=event['trace'];r=dict(ordinal=event['ordinal'],policy=mod.POLICIES[0],primary_ids=sorted(t['primary_ids']),covered_ids=sorted(t['covered_ids']));rows.extend([r,{}])
 sealed[name]=dict(rows=rows)
reads=[];orig=mod.read_artifact
def read(desc,cap,retain=True):
 assert not any(desc['sha256']==config['datasets'][n]['truth']['sha256'] for n in mod.DATASETS),'GT opened';reads.append(desc['path']);return orig(desc,cap,retain)
mod.read_artifact=read;start=time.monotonic();mod.original_control_parity(config,sealed,audit)
row=sealed['relaion']['rows'][0];oldids=row['covered_ids'];replacement=next(x for x in range(100000) if x not in set(oldids));row['covered_ids']=sorted(oldids[1:]+[replacement])
try:mod.original_control_parity(config,sealed,audit)
except ValueError as e: rejected=str(e)
else:raise AssertionError('same-size changed control accepted')
result=dict(schema='borsuk-global-leaf-real-original-control-admission-v1',source_sha256=pin(script)['sha256'],actual_archived_datasets=list(mod.DATASETS),actual_original_queries_compared=128,truth_body_opens=0,nomination_execution=False,synthetic_new_rows_copied_from_authenticated_original=True,original_schema_admission=True,same_size_control_mutation_rejected=rejected,wall_seconds=time.monotonic()-start)
pathlib.Path('/tmp/borsuk-global-leaf-original-control-admission-20261003.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
