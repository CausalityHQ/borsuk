"""AWS-only narrow HTTP example check, authority mutation RED then original GREEN."""
import atexit,hashlib,json,subprocess,sys
from pathlib import Path
cargo,repo,out=sys.argv[1:];repo,out=Path(repo),Path(out)
def resources():
    group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    (out/'boundary-cgroup.json').write_text(json.dumps({k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},indent=2)+'\n')
atexit.register(resources)
p=repo/'crates/borsuk/examples/two_bit_http.rs';original=p.read_bytes();digest=lambda b:hashlib.sha256(b).hexdigest()
cmd=[cargo,'test','--release','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--example','two_bit_http','--jobs','4']
red=original.replace(b'request.control_epoch != authority.control_epoch',b'false')
assert red!=original
try:
    p.write_bytes(red)
    with (out/'red.log').open('w') as log:status=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT).returncode
    text=(out/'red.log').read_text()
    assert status!=0 and 'request_identity_geometry_and_nonqueued_admission ... FAILED' in text, 'expected failed epoch assertion, not compilation error'
finally:p.write_bytes(original)
assert p.read_bytes()==original
with (out/'green.log').open('w') as log:subprocess.run(cmd,check=True,stdout=log,stderr=subprocess.STDOUT)
subprocess.run([str(Path(cargo).parent/'rustfmt'),'--check','--edition','2024',str(p)],check=True)
with (out/'focused.log').open('w') as log:subprocess.run([cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--lib','sq8_s3_range::tests','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
with (out/'full.log').open('w') as log:subprocess.run([cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'--workspace','--all-targets','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
with (out/'release.log').open('w') as log:subprocess.run([cargo,'build','--release','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--example','two_bit_http','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
(out/'binaries').mkdir();binary=out/'binaries/two_bit_http';binary.write_bytes((out/'target/release/examples/two_bit_http').read_bytes());binary.chmod(0o755)
result=dict(qualified=False,original_example_sha256=digest(original),red_example_sha256=digest(red),red_status=status,green_passed=True,binary_bytes=binary.stat().st_size,binary_sha256=digest(binary.read_bytes()),full_workspace_repeated=True,no_ann_query_run=True)
(out/'boundary-check.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
