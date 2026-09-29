"""AWS-only expected codec geometry RED; no corpus or full assurance."""
import atexit,hashlib,json,subprocess,sys
from pathlib import Path
cargo,repo,out=sys.argv[1:];repo,out=Path(repo),Path(out)
def resources():
 group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
 (out/'boundary-cgroup.json').write_text(json.dumps({k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},indent=2)+'\n')
atexit.register(resources)
p=repo/'crates/borsuk/src/rotated_two_bit.rs';original=p.read_bytes()
name='rotated_two_bit::precision_tests::three_bit_records_match_scalar_cosine_and_admit_exact_lookup_scratch'
command=[cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--jobs','4','--lib',name]
with (out/'red.log').open('w') as log:
 status=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT).returncode
red=(out/'red.log').read_text()
assert status==101 and name+' ... FAILED' in red and 'three-bit record geometry' in red and 'left: 200' in red and 'right: 296' in red,'Expected two-bit record geometry RED'
assert p.read_bytes()==original
result=dict(qualified=False,no_corpus_query=True,full_workspace_repeated=False,original_source_sha256=hashlib.sha256(original).hexdigest(),red_status=status,red_verified=True,command=command)
(out/'boundary-check.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
