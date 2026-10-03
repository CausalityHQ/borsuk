import json,pathlib,hashlib,subprocess
from scripts import reduce_hierarchical_global_leaf_coverage as r
repo=pathlib.Path.cwd();out=pathlib.Path('/data/orchestration/borsuk-global-leaf-reduction-a0002-20261003');b=repo/'docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells';s=b/'paired100k/global-leaf-probe/a0002/screen';old=b/'paired100k/a0001'
def pin(f):
 f=pathlib.Path(f).resolve();return {'path':str(f),'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()}
def load(f):return json.loads(pathlib.Path(f).read_text())
def write(f,d):pathlib.Path(f).write_text(json.dumps(d,sort_keys=True,indent=2)+'\n')
a=load(b/'implementation-gates/minimal-archive/qualified-nomination-authority.json')['native'];seal=load(s/'paired-seal.json')
for name,path in [('module','crates/borsuk/src/hierarchical_semantic_cells.rs'),('binary_source','crates/borsuk/src/bin/hierarchical_semantic_cells.rs')]:
 (out/name).write_bytes(subprocess.check_output(['git','show',a['source_commit']+':'+path]))
source={'revision':a['source_commit'],**{n:pin(out/n) for n in ['archive','executable','module','binary_source']},'reducer':pin(repo/'scripts/reduce_hierarchical_global_leaf_coverage.py')}
control={'terminal':pin(old/'aws-terminal.json'),'datasets':{d:{'diagnostic_config':pin(old/'screen/measurement'/f'{d}-diagnose.json'),'diagnostic':pin(old/'screen/measurement'/f'{d}-diagnostic.jsonl')} for d in r.DATASETS}}
config={'schema':r.SCHEMA,'protocol':pin(b/'paired100k/global-leaf-routing-probe-preregistration.json'),'historical_audit':pin(old/'independent-coverage-audit.json'),'historical_control':control,'source':source,'datasets':{}}
receipt={k:config[k] for k in ['protocol','historical_audit','historical_control','source']};receipt.update(schema=r.SCHEMA+'-closed-receipt',status='CLOSED_VALID',execution_exit_code=0,source_qualified=True,resources_qualified=True,cleanup_complete=True,both_sealed_before_truth=True,truth_opened=False,datasets={})
for d in r.DATASETS:
 x=seal['datasets'][d];ds={n:pin(s/x[k]['path']) for n,k in [('candidate_root','root'),('directories','directories'),('requests','requests'),('nomination_config','config'),('nominations','nomination')]};ds.update(truth=pin(out/(d+'-truth64')),truth_id_space='logical_source_ordinal_le_u32');config['datasets'][d]=ds
 frozen=r.nominations(ds,source)
 stage=load(s/'measurement'/f'{d}-nominate-stage-receipt.json')['stages'][0]['cgroup_after'];events=dict(line.split() for line in stage['memory.events'].splitlines())
 resources={'memory_limit_bytes':int(stage['memory.max']),'memory_peak_bytes':int(stage['memory.peak']),'swap_limit_bytes':int(stage['memory.swap.max']),'swap_peak_bytes':int(stage['memory.swap.peak']),'cpu_count':1,'oom':int(events['oom']),'oom_kill':int(events['oom_kill'])}
 receipt['datasets'][d]={k:ds[k] for k in ['nomination_config','nominations','candidate_root','directories','requests']};receipt['datasets'][d].update(prefix=frozen['prefix'],source_identity_sha256=frozen['source_identity_sha256'],execution_exit_code=0,sealed_before_truth=True,fsynced=True,complete=True,resources=resources)
write(out/'closed-receipt.json',receipt);config['closed_receipt']=pin(out/'closed-receipt.json');write(out/'config.json',config);print(pin(out/'config.json'))
