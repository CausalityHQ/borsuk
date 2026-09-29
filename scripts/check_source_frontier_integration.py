"""One actual planner integration RED/GREEN/full/release, no corpus queries."""
import atexit,hashlib,json,subprocess,sys
from pathlib import Path
cargo,repo,out=sys.argv[1:];repo,out=Path(repo),Path(out)
def resources():
 group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
 (out/'boundary-cgroup.json').write_text(json.dumps({k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},indent=2)+'\n')
atexit.register(resources)
p=repo/'crates/borsuk/src/unit_centroid_graph.rs';original=p.read_bytes();digest=lambda b:hashlib.sha256(b).hexdigest()
base=[cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--jobs','4']
anchor=b'        let mut scores = HashMap::with_capacity(max_evaluations.min(self.node_count()));'
start=original.index(b'    fn search_pages_seeded_impl(')
assert original[start:].count(anchor)==1
red_source=original[:start]+original[start:].replace(anchor,b'        let mut priority: Option<&mut dyn FnMut(usize) -> Result<f64, UnitCentroidGraphError>> = None;\n'+anchor,1)
p.write_bytes(red_source)
try:
 with (out/'red.log').open('w') as log:
  status=subprocess.run(base+['--lib','unit_centroid_graph::tests::source_frontier_finds_target_under_the_same_work_cap'],stdout=log,stderr=subprocess.STDOUT).returncode
finally: p.write_bytes(original)
red=(out/'red.log').read_text();assert status!=0 and 'source_frontier_finds_target_under_the_same_work_cap ... FAILED' in red and 'left: [0, 1, 2]' in red and 'right: [0, 1, 3]' in red,'Expected centroid-priority assertion RED'
(out/'graph.fixed.rs').write_bytes(original)
subprocess.run([str(Path(cargo).parent/'rustfmt'),'--check','--edition','2024',str(p),str(repo/'crates/borsuk/src/two_bit_generation.rs')],check=True)
with (out/'green.log').open('w') as log:
 for target in [['--lib','unit_centroid_graph::tests'],['--lib','two_bit_generation::source_walk_tests'],['--test','two_bit_generation']]:
  subprocess.run(base+target,check=True,stdout=log,stderr=subprocess.STDOUT)
with (out/'full.log').open('w') as log:subprocess.run([cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'--workspace','--all-targets','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
with (out/'release.log').open('w') as log:subprocess.run([cargo,'build','--release','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--bin','two_bit_plan_demo','--bin','build_two_bit_generation','--example','two_bit_http','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
(out/'binaries').mkdir();binaries={}
for name,sub in [('two_bit_plan_demo',''),('build_two_bit_generation',''),('two_bit_http','examples')]:
 binary=out/'binaries'/name;binary.write_bytes((out/'target/release'/sub/name).read_bytes());binary.chmod(0o755);binaries[name]=dict(bytes=binary.stat().st_size,sha256=digest(binary.read_bytes()))
result=dict(qualified=False,no_corpus_query=True,full_workspace_repeated=True,original_source_sha256=digest(original),fixed_source_sha256=digest(original),centroid_red_source_sha256=digest(red_source),generation_source_sha256=digest((repo/'crates/borsuk/src/two_bit_generation.rs').read_bytes()),red_status=status,green_passed=True,binaries=binaries)
(out/'boundary-check.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
