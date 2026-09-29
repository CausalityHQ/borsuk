"""AWS-only runner memory RED/GREEN/release; unchanged library gate reused."""
import atexit,hashlib,json,subprocess,sys
from pathlib import Path
cargo,repo,out=sys.argv[1:];repo,out=Path(repo),Path(out)
def resources():
 group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
 (out/'boundary-cgroup.json').write_text(json.dumps({k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},indent=2)+'\n')
atexit.register(resources)
p=repo/'crates/borsuk/src/native_development_memory.rs';original=p.read_bytes();anchor=b'.unwrap_or("1073741824")';assert original.count(anchor)==1
red=original.replace(anchor,b'.map(|_| "1073741824").unwrap_or("1073741824")',1)
base=[cargo,'test','--release','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--jobs','4']
name='native_development_memory::tests::explicit_admission_preserves_default_and_rejects_invalid_bounds'
p.write_bytes(red)
try:
 with (out/'red.log').open('w') as log:status=subprocess.run(base+['--bin','two_bit_plan_demo',name],stdout=log,stderr=subprocess.STDOUT).returncode
finally:p.write_bytes(original)
log=(out/'red.log').read_text();assert status==101 and name+' ... FAILED' in log and 'left: Ok(1073741824)' in log and 'right: Ok(1342177280)' in log,'Expected ignored-caller-bound RED'
with (out/'green.log').open('w') as log:
 for target in [['--bin','two_bit_plan_demo'],['--example','two_bit_http']]:subprocess.run(base+target+[name],check=True,stdout=log,stderr=subprocess.STDOUT)
with (out/'release.log').open('w') as log:subprocess.run([cargo,'build','--release','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--bin','two_bit_plan_demo','--example','two_bit_http','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
(out/'binaries').mkdir();binaries={}
for name,sub in [('two_bit_plan_demo',''),('two_bit_http','examples')]:
 binary=out/'binaries'/name;binary.write_bytes((out/'target/release'/sub/name).read_bytes());binary.chmod(0o755);binaries[name]=dict(bytes=binary.stat().st_size,sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
assert p.read_bytes()==original
result=dict(qualified=False,no_corpus_query=True,full_workspace_repeated=False,red_status=status,green_passed=True,binaries=binaries,helper_sha256=hashlib.sha256(original).hexdigest(),red_helper_sha256=hashlib.sha256(red).hexdigest())
(out/'boundary-check.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
