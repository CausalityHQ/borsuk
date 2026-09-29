"""One actual planner integration RED/GREEN/full/release, no corpus queries."""
import atexit,hashlib,json,subprocess,sys
from pathlib import Path
cargo,repo,out=sys.argv[1:];repo,out=Path(repo),Path(out)
def resources():
 group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
 (out/'boundary-cgroup.json').write_text(json.dumps({k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},indent=2)+'\n')
atexit.register(resources)
digest=lambda b:hashlib.sha256(b).hexdigest()
base=[cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--jobs','4']
with (out/'green.log').open('w') as log:
 for target in [['--lib','source_walk_tests'],['--test','two_bit_generation'],['--test','two_bit_application_ids'],['--bin','two_bit_plan_demo','native_development_memory::tests'],['--example','two_bit_http','native_development_memory::tests']]:
  subprocess.run(base+target,check=True,stdout=log,stderr=subprocess.STDOUT)
with (out/'full.log').open('w') as log:subprocess.run([cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'--workspace','--all-targets','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
with (out/'release.log').open('w') as log:subprocess.run([cargo,'build','--release','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--bin','two_bit_plan_demo','--bin','build_two_bit_generation','--example','two_bit_http','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
(out/'binaries').mkdir();binaries={}
for name,sub in [('two_bit_plan_demo',''),('build_two_bit_generation',''),('two_bit_http','examples')]:
 binary=out/'binaries'/name;binary.write_bytes((out/'target/release'/sub/name).read_bytes());binary.chmod(0o755);binaries[name]=dict(bytes=binary.stat().st_size,sha256=digest(binary.read_bytes()))
native={str(p.relative_to(repo)):digest(p.read_bytes()) for p in repo.rglob('*.rs') if 'target' not in p.parts}
result=dict(qualified=False,no_corpus_query=True,full_workspace_repeated=True,green_passed=True,binaries=binaries,native_source_sha256=native)
(out/'boundary-check.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
