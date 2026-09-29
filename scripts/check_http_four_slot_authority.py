"""AWS-only affected four-slot HTTP example RED/GREEN and compiled-source authority."""
import atexit, hashlib, json, subprocess, sys
from pathlib import Path
cargo,repo,out=sys.argv[1:];repo,out=Path(repo),Path(out)
def resources():
    group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    (out/'boundary-cgroup.json').write_text(json.dumps({k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},indent=2)+'\n')
atexit.register(resources)
p=repo/'crates/borsuk/examples/two_bit_http.rs';original=p.read_bytes()
common=[cargo,'--manifest-path',str(repo/'Cargo.toml')]
args=['--release','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--jobs','4']
name='tests::request_identity_geometry_and_nonqueued_admission'
def run(label,command):
    with (out/(label+'.log')).open('x') as log:
        return subprocess.run(command,stdout=log,stderr=subprocess.STDOUT).returncode
red=run('red',[cargo,'test',*args,'--example','two_bit_http',name]);log=(out/'red.log').read_text()
assert red==101 and name+' ... FAILED' in log and 'left: 2' in log and 'right: 4' in log and 'could not compile' not in log
old=b'const QUERY_SLOTS: usize = 2;';new=b'const QUERY_SLOTS: usize = 4;';assert original.count(old)==1
fixed=original.replace(old,new);p.write_bytes(fixed)
assert run('green',[cargo,'test',*args,'--example','two_bit_http'])==0
assert '0 failed;' in (out/'green.log').read_text()
assert run('release',[cargo,'build',*args,'--example','two_bit_http'])==0
assert p.read_bytes()==fixed
(out/'http.fixed.rs').write_bytes(fixed)
(out/'binaries').mkdir(exist_ok=True);binary=(out/'target/release/examples/two_bit_http').read_bytes();(out/'binaries/two_bit_http').write_bytes(binary)
result=dict(qualified=True,no_corpus_query=True,full_workspace_repeated=False,red_status=red,green_status=0,release_status=0,original_http_sha256=hashlib.sha256(original).hexdigest(),compiled_http_sha256=hashlib.sha256(fixed).hexdigest(),binary_sha256=hashlib.sha256(binary).hexdigest(),accepted_k=[10,100],query_slots=4,library_assurance_reused=2696)
(out/'boundary-check.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
