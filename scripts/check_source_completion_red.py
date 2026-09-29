"""AWS-only focused native RED; no full suite or corpus."""
import atexit,hashlib,json,subprocess,sys
from pathlib import Path
cargo,repo,out=sys.argv[1:];repo,out=Path(repo),Path(out)
def resources():
 group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
 (out/'boundary-cgroup.json').write_text(json.dumps({k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},indent=2)+'\n')
atexit.register(resources)
r=repo/'docs/research/native-union-20260928';config=json.loads((r/'source-completion-red-config.json').read_text());p=repo/'crates/borsuk/src/two_bit_generation.rs';original=p.read_bytes()
assert hashlib.sha256(original.split(b'#[cfg(test)]\nmod source_walk_tests')[0]).hexdigest()==config['production_prefix_sha256']
name='source_walk_tests::bounded_completion_recovers_unvisited_rows_without_duplicate_or_extra_work'
with (out/'red.log').open('w') as log:
 status=subprocess.run([cargo,'test','--release','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--jobs','4','--lib',name],stdout=log,stderr=subprocess.STDOUT).returncode
log=(out/'red.log').read_text();assert status==101 and name+' ... FAILED' in log and 'left: (0, 1.0)' in log and 'right: (160, 10.0)' in log,'Expected unvisited-source-row RED'
assert original==p.read_bytes()
result=dict(qualified=False,no_corpus_query=True,full_workspace_repeated=False,red_status=status,production_prefix_sha256=config['production_prefix_sha256'],generation_sha256=hashlib.sha256(original).hexdigest())
(out/'boundary-check.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
