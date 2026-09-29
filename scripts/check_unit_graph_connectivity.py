"""Frozen AWS RED then declared construction repair, focused GREEN and one full gate."""
import atexit,hashlib,json,subprocess,sys
from pathlib import Path
cargo,repo,out=sys.argv[1:];repo,out=Path(repo),Path(out)
def resources():
    group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    (out/'boundary-cgroup.json').write_text(json.dumps({k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},indent=2)+'\n')
atexit.register(resources)
p=repo/'crates/borsuk/src/unit_centroid_graph.rs';original=p.read_bytes();digest=lambda b:hashlib.sha256(b).hexdigest()
cmd=[cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--lib','unit_centroid_graph::tests','--jobs','4']
with (out/'red.log').open('w') as log:status=subprocess.run(cmd+['--','duplicate_centroids_keep_seeded_discovery_reachable_and_bounded'],stdout=log,stderr=subprocess.STDOUT).returncode
text=(out/'red.log').read_text()
assert status!=0 and 'duplicate_centroids_keep_seeded_discovery_reachable_and_bounded ... FAILED' in text and ('frontier ended before work cap' in text or 'discovery lost required page coverage' in text or 'directed graph disconnected' in text), 'expected reachability assertion RED, not compiler failure'
anchor=b'        let adjacency = build(&centers, M, M0, EF_CONSTRUCTION, 64)\n            .ok_or(UnitCentroidGraphError::InvalidGeometry)?;'
replacement=b"""        let mut adjacency = build(&centers, M, M0, EF_CONSTRUCTION, 64)
            .ok_or(UnitCentroidGraphError::InvalidGeometry)?;
        // A directed base-layer cycle prevents pruning from isolating a seeded
        // walk. Keep the existing degree and evaluation bounds; higher layers
        // remain untouched. Construction quality needs separate qualification.
        let nodes = adjacency.neighbours.len();
        for (node, tower) in adjacency.neighbours.iter_mut().enumerate() {
            let next = ((node + 1) % nodes) as u32;
            let base = tower.last_mut().expect("base layer");
            if !base.contains(&next) {
                if base.len() == M0 {
                    base.pop();
                }
                base.push(next);
            }
        }"""
assert original.count(anchor)==1
fixed=original.replace(anchor,replacement);p.write_bytes(fixed)
# The unchanged source outside the declared construction block is retained.
(out/'unit_centroid_graph.fixed.rs').write_bytes(fixed)
with (out/'green.log').open('w') as log:subprocess.run(cmd,check=True,stdout=log,stderr=subprocess.STDOUT)
with (out/'focused.log').open('w') as log:subprocess.run([cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--test','two_bit_generation','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
subprocess.run([str(Path(cargo).parent/'rustfmt'),'--check','--edition','2024',str(p)],check=True)
with (out/'full.log').open('w') as log:subprocess.run([cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'--workspace','--all-targets','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
with (out/'release.log').open('w') as log:subprocess.run([cargo,'build','--release','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--bin','build_two_bit_generation','--bin','two_bit_plan_demo','--example','two_bit_http','--example','build_sq8_source','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
(out/'binaries').mkdir();binaries={}
for name,sub in [('build_two_bit_generation',''),('two_bit_plan_demo',''),('two_bit_http','examples'),('build_sq8_source','examples')]:
    binary=out/'binaries'/name;binary.write_bytes((out/'target/release'/sub/name).read_bytes());binary.chmod(0o755);binaries[name]=dict(bytes=binary.stat().st_size,sha256=digest(binary.read_bytes()))
result=dict(qualified=False,original_graph_sha256=digest(original),fixed_graph_sha256=digest(fixed),red_status=status,green_passed=True,binaries=binaries,full_workspace_repeated=True,no_corpus_query=True)
(out/'boundary-check.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
