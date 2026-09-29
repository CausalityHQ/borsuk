"""Frozen AWS RED then declared construction repair, focused GREEN and one full gate."""
import atexit,hashlib,json,subprocess,sys
from pathlib import Path
cargo,repo,out=sys.argv[1:];repo,out=Path(repo),Path(out)
def resources():
    group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    (out/'boundary-cgroup.json').write_text(json.dumps({k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},indent=2)+'\n')
atexit.register(resources)
graph=repo/'crates/borsuk/src/centroid_hnsw.rs';original_graph=graph.read_bytes();p=repo/'crates/borsuk/src/source_order.rs';example=repo/'crates/borsuk/examples/build_sq8_source.rs';original_example=example.read_bytes();original=p.read_bytes();digest=lambda b:hashlib.sha256(b).hexdigest()
cmd=[cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--lib','centroid_hnsw::tests::source_cell_order_follows_nearest_unvisited_edges','--jobs','4']
with (out/'red.log').open('w') as log:status=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT).returncode
text=(out/'red.log').read_text()
assert status!=0 and 'source_cell_order_follows_nearest_unvisited_edges ... FAILED' in text and 'source cells still use numeric order' in text, 'expected source locality assertion RED, not compiler or geometry failure'
anchor=b'    if cells > 1 && router.is_none() {\n        return Err(bad("hierarchical centroid graph"));\n    }\n    input.seek(SeekFrom::Start(0))?;'
replacement=b'    if cells > 1 && router.is_none() {\n        return Err(bad("hierarchical centroid graph"));\n    }\n    let mut ranks = vec![0; cells];\n    if let Some(router) = &router {\n        for (rank, cell) in router.layer0_nearest_order().into_iter().enumerate() {\n            ranks[cell as usize] = rank;\n        }\n    }\n    input.seek(SeekFrom::Start(0))?;'
graph_anchor=b'    /// Source-cell ordering; qualified separately from query routing.\n    pub(crate) fn layer0_nearest_order(&self) -> Vec<u32> {\n        (0..self.vectors.len() as u32).collect()\n    }'
graph_replacement=b'    /// Source-only adjacency-local chain: O(edges * dimensions + nodes),\n    /// with O(nodes) output/seen storage; no global quadratic centroid chain.\n    pub(crate) fn layer0_nearest_order(&self) -> Vec<u32> {\n        let n = self.vectors.len();\n        let mut order = Vec::with_capacity(n);\n        let mut seen = vec![false; n];\n        let mut current = self.entry;\n        let mut fallback = 0;\n        for _ in 0..n {\n            order.push(current);\n            seen[current as usize] = true;\n            if order.len() == n {\n                break;\n            }\n            current = Self::layer_neighbours(&self.neighbours, current, 0)\n                .iter()\n                .filter(|&&node| !seen[node as usize])\n                .map(|&node| Candidate {\n                    distance: squared_distance(\n                        &self.vectors[current as usize],\n                        &self.vectors[node as usize],\n                    ),\n                    node,\n                })\n                .min()\n                .map_or_else(\n                    || {\n                        while seen[fallback] {\n                            fallback += 1;\n                        }\n                        fallback as u32\n                    },\n                    |candidate| candidate.node,\n                );\n        }\n        order\n    }'
assert original.count(anchor)==1
assert original.count(b'            rank: cell,')==1
fixed=original.replace(anchor,replacement).replace(b'            rank: cell,',b'            rank: ranks[cell],');p.write_bytes(fixed)
assert original_graph.count(graph_anchor)==1
fixed_graph=original_graph.replace(graph_anchor,graph_replacement);graph.write_bytes(fixed_graph)
(out/'centroid_hnsw.fixed.rs').write_bytes(fixed_graph)
example_anchor=b'            receipt["recipe"] = "borsuk-hierarchical-extents-chacha8-v2".into();'
example_replacement=b'            receipt["recipe"] = "borsuk-hierarchical-extents-chacha8-v3".into();\n            receipt["source_cell_order"] =\n                "nearest-unvisited-layer0-entry-ordinal-fallback-v1".into();'
assert original_example.count(example_anchor)==1
fixed_example=original_example.replace(example_anchor,example_replacement);example.write_bytes(fixed_example)
(out/'source_order.fixed.rs').write_bytes(fixed);(out/'build_sq8_source.fixed.rs').write_bytes(fixed_example)
with (out/'green.log').open('w') as log:
    subprocess.run(cmd,check=True,stdout=log,stderr=subprocess.STDOUT)
    subprocess.run([('source_order::tests' if arg=='centroid_hnsw::tests::source_cell_order_follows_nearest_unvisited_edges' else arg) for arg in cmd],check=True,stdout=log,stderr=subprocess.STDOUT)
subprocess.run([str(Path(cargo).parent/'rustfmt'),'--check','--edition','2024',str(p),str(example),str(graph)],check=True)
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
receipt=json.loads((out/'recipe.log').read_text());assert receipt['recipe']=='borsuk-hierarchical-extents-chacha8-v3' and receipt['source_cell_order']=='nearest-unvisited-layer0-entry-ordinal-fallback-v1' and receipt['source_cell_target_rows']==256 and receipt['sampling_cell_target_rows']==1024 and receipt['samples_per_sampling_cell']==64 and receipt['query_or_truth_used'] is False and len(receipt['extents'])==16
ids=struct.unpack('<4096Q',order.read_bytes());assert set(ids)==set(range(4096)) and digest(order.read_bytes())==receipt['order_sha256']
for first,last in receipt['extents']:assert last-first==256 and len({i%16 for i in ids[first:last]})==1
(out/'recipe-check.json').write_text(json.dumps(dict(passed=True,source_sha256=digest(data),order_sha256=digest(order.read_bytes()),receipt=receipt,corpus_used=False),indent=2)+'\n')
result=dict(qualified=False,original_graph_sha256=digest(original_graph),fixed_graph_sha256=digest(fixed_graph),original_source_sha256=digest(original),fixed_source_sha256=digest(fixed),original_example_sha256=digest(original_example),fixed_example_sha256=digest(fixed_example),red_status=status,green_passed=True,binaries=binaries,full_workspace_repeated=True,no_corpus_query=True)
(out/'boundary-check.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
