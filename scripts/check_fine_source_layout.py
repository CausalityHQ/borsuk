"""Frozen AWS RED then declared construction repair, focused GREEN and one full gate."""
import atexit,hashlib,json,subprocess,sys
from pathlib import Path
cargo,repo,out=sys.argv[1:];repo,out=Path(repo),Path(out)
def resources():
    group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    (out/'boundary-cgroup.json').write_text(json.dumps({k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},indent=2)+'\n')
atexit.register(resources)
p=repo/'crates/borsuk/src/source_order.rs';example=repo/'crates/borsuk/examples/build_sq8_source.rs';original_example=example.read_bytes();original=p.read_bytes();digest=lambda b:hashlib.sha256(b).hexdigest()
cmd=[cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--lib','source_order::tests','--jobs','4']
with (out/'red.log').open('w') as log:status=subprocess.run(cmd+['--','hierarchical_group_target_keeps_separate_source_modes'],stdout=log,stderr=subprocess.STDOUT).returncode
text=(out/'red.log').read_text()
assert status!=0 and 'hierarchical_group_target_keeps_separate_source_modes ... FAILED' in text and 'coarse source groups merge distinct modes' in text, 'expected source grouping assertion RED, not compiler or geometry failure'
anchor=b'    let cells = rows.div_ceil(1024);\n    let sample_rows = rows.min(cells.checked_mul(64).ok_or(bad("layout geometry"))?);'
replacement=b"""    let cells = rows.div_ceil(256);
    // Hold the original global source-only reservoir fixed while refining groups.
    let sample_rows = rows.min(
        rows.div_ceil(1024)
            .checked_mul(64)
            .ok_or(bad("layout geometry"))?,
    );"""
assert original.count(anchor)==1
fixed=original.replace(anchor,replacement);p.write_bytes(fixed)
example_anchor=b'            receipt["recipe"] = "borsuk-hierarchical-extents-chacha8-v1".into();'
example_replacement=b"""            receipt["recipe"] = "borsuk-hierarchical-extents-chacha8-v2".into();
            receipt["source_cell_target_rows"] = 256.into();
            receipt["sampling_cell_target_rows"] = 1024.into();
            receipt["samples_per_sampling_cell"] = 64.into();"""
assert original_example.count(example_anchor)==1
fixed_example=original_example.replace(example_anchor,example_replacement);example.write_bytes(fixed_example)
(out/'source_order.fixed.rs').write_bytes(fixed);(out/'build_sq8_source.fixed.rs').write_bytes(fixed_example)
with (out/'green.log').open('w') as log:subprocess.run(cmd,check=True,stdout=log,stderr=subprocess.STDOUT)
subprocess.run([str(Path(cargo).parent/'rustfmt'),'--check','--edition','2024',str(p),str(example)],check=True)
with (out/'full.log').open('w') as log:subprocess.run([cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'--workspace','--all-targets','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
with (out/'release.log').open('w') as log:subprocess.run([cargo,'build','--release','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--bin','build_two_bit_generation','--bin','two_bit_plan_demo','--example','two_bit_http','--example','build_sq8_source','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
(out/'binaries').mkdir();binaries={}
for name,sub in [('build_two_bit_generation',''),('two_bit_plan_demo',''),('two_bit_http','examples'),('build_sq8_source','examples')]:
    binary=out/'binaries'/name;binary.write_bytes((out/'target/release'/sub/name).read_bytes());binary.chmod(0o755);binaries[name]=dict(bytes=binary.stat().st_size,sha256=digest(binary.read_bytes()))
# Public CLI receipt check uses synthetic source only; no ANN query or GT.
import math,struct
fixture=out/'fixture.f32';data=b''.join(struct.pack('<2f',math.cos(2*math.pi*(row%16)/16),math.sin(2*math.pi*(row%16)/16)) for row in range(4096));fixture.write_bytes(data)
order=out/'fixture-order.u64'
with (out/'recipe.log').open('w') as log:subprocess.run([str(out/'binaries/build_sq8_source'),'hier-fit',str(fixture),digest(data),'4096','2',str(16<<20),str(order)],check=True,stdout=log,stderr=subprocess.STDOUT)
receipt=json.loads((out/'recipe.log').read_text());assert receipt['recipe']=='borsuk-hierarchical-extents-chacha8-v2' and receipt['source_cell_target_rows']==256 and receipt['sampling_cell_target_rows']==1024 and receipt['samples_per_sampling_cell']==64 and receipt['query_or_truth_used'] is False and len(receipt['extents'])==16
ids=struct.unpack('<4096Q',order.read_bytes());assert set(ids)==set(range(4096)) and digest(order.read_bytes())==receipt['order_sha256']
for first,last in receipt['extents']:assert last-first==256 and len({i%16 for i in ids[first:last]})==1
(out/'recipe-check.json').write_text(json.dumps(dict(passed=True,source_sha256=digest(data),order_sha256=digest(order.read_bytes()),receipt=receipt,corpus_used=False),indent=2)+'\n')
result=dict(qualified=False,original_source_sha256=digest(original),fixed_source_sha256=digest(fixed),original_example_sha256=digest(original_example),fixed_example_sha256=digest(fixed_example),red_status=status,green_passed=True,binaries=binaries,full_workspace_repeated=True,no_corpus_query=True)
(out/'boundary-check.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
