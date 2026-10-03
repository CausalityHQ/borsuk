import json,os,subprocess,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from scripts import run_hierarchical_global_leaf_probe as probe
from scripts import launch_hierarchical_cells_100k_spot as launcher
from scripts import prepare_hierarchical_cells_100k as local
repo=Path(sys.argv[1]); results=[]
cgroup_path=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('::',1)[1].lstrip('/')
def snapshot():
 return dict(path=str(cgroup_path),cpu_affinity=sorted(os.sched_getaffinity(0)),**{n:(cgroup_path/n).read_text().strip() for n in ('memory.max','memory.swap.max','cpu.max','memory.peak','memory.swap.peak','memory.events')})
before=snapshot()
assert before['memory.max']=='268435456' and before['memory.swap.max']=='0' and before['cpu.max']=='100000 100000' and before['cpu_affinity']==[0],before
commands=[
 [sys.executable,'scripts/run_hierarchical_global_leaf_probe.py','--self-check'],
 [sys.executable,'scripts/launch_hierarchical_cells_100k_spot.py','--global-leaf-probe','--self-check'],
 [sys.executable,'scripts/run_hierarchical_global_leaf_probe.py','--self-check-qualification',str(repo)],
 [sys.executable,'-m','py_compile','scripts/run_hierarchical_global_leaf_probe.py','scripts/launch_hierarchical_cells_100k_spot.py'],
 ['git','diff','--check'],
]
for i,command in enumerate(commands):
 run=subprocess.run(command,cwd=repo,capture_output=True,text=True)
 log=Path(f'/tmp/borsuk-global-leaf-staging-check-{i}.log');log.write_text(run.stdout+run.stderr)
 results.append(dict(command=command,exit_status=run.returncode,log=str(log)))
 print(json.dumps(results[-1]),flush=True)
 if run.returncode:print(run.stdout+run.stderr);raise SystemExit(run.returncode)
config_pin=local.identity(repo/launcher.PROBE_CONFIG)
config=local.read_json(config_pin,512<<10)
roles={role:probe.qualify_role(role,config['roles'][role],repo) for role in probe.ROLE_NAMES}
evidence=probe.original_evidence(repo,config)
summary=dict(frozen_config=config_pin,
 roles={role:dict(native_source_count=len(value['source_sha256']),support_source_count=len(value['source_archive_support_sha256']),source_identity_sha256=value['source_identity_sha256'],source_commit=value['source_commit']) for role,value in roles.items()},
 datasets=sorted(evidence['items']),archive_or_binary_or_corpus_bodies_read=False,network=False)
print('PASS actual frozen metadata '+json.dumps(summary,sort_keys=True),flush=True)
try:launcher.probe_qualify(repo)
except ValueError as error:
 assert str(error)=='probe controller source drift',str(error)
 print('PASS immutable frozen launcher rejects repaired controller hashes; root refreeze required',flush=True)
else:raise AssertionError('old frozen controller identity accepted repaired code')
after=snapshot()
assert after['memory.swap.peak']=='0' and int(after['memory.peak'])<=268435456 and 'oom 0' in after['memory.events'],after
print('PASS enforced cgroup '+json.dumps(after,sort_keys=True),flush=True)
Path('/tmp/borsuk-global-leaf-staging-assurance.json').write_text(json.dumps(dict(checks=results,frozen_metadata=summary,frozen_source_drift_rejected=True,cgroup_before=before,cgroup_after=after),sort_keys=True,indent=2)+'\n')
