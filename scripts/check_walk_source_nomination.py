"""Frozen isolated native replay primitive RED/GREEN/release; no shared Rust changes."""
import atexit,hashlib,json,subprocess,sys
from pathlib import Path
cargo,repo,out=sys.argv[1:];repo,out=Path(repo),Path(out)
def resources():
 group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
 (out/'boundary-cgroup.json').write_text(json.dumps({k:(group/k).read_text() for k in ['memory.peak','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},indent=2)+'\n')
atexit.register(resources)
p=repo/'crates/borsuk/src/bin/two_bit_walk_nomination.rs';original=p.read_bytes();digest=lambda b:hashlib.sha256(b).hexdigest()
cmd=[cargo,'test','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--bin','two_bit_walk_nomination','--jobs','4']
with (out/'red.log').open('w') as log:status=subprocess.run(cmd+['early_source_max_replaces_centroid_cutoff_and_reuses_duplicates'],stdout=log,stderr=subprocess.STDOUT).returncode
red=(out/'red.log').read_text();assert status!=0 and 'early_source_max_replaces_centroid_cutoff_and_reuses_duplicates ... FAILED' in red and 'walk source roster not implemented' in red,'Expected primitive assertion RED, not compilation'
anchor=b'''fn reroster(
    primary: &[usize],
    walks: &[Vec<usize>],
    mut score: impl FnMut(usize) -> Result<f64, Box<dyn Error>>,
) -> Result<Roster, Box<dyn Error>> {
    let _ = (&primary, &walks, &mut score);
    Err("walk source roster not implemented".into())
}'''
replacement=b'fn reroster(\n    primary: &[usize],\n    walks: &[Vec<usize>],\n    mut score: impl FnMut(usize) -> Result<f64, Box<dyn Error>>,\n) -> Result<Roster, Box<dyn Error>> {\n    if primary.len() != 2 || walks.len() != 2 {\n        return Err("frozen graph roster".into());\n    }\n    for (&seed, units) in primary.iter().zip(walks) {\n        if seed >= 391\n            || units.len() != 1272\n            || units.iter().any(|&unit| unit >= 3125)\n            || units.iter().copied().collect::<BTreeSet<_>>().len() != 1272\n            || (seed * 8..((seed + 1) * 8).min(3125)).any(|unit| !units.contains(&unit))\n        {\n            return Err("frozen walk geometry".into());\n        }\n    }\n    let mut memo = BTreeMap::new();\n    let mut global_pages = BTreeMap::<usize, f64>::new();\n    let mut selected = BTreeSet::new();\n    let mut per_graph = Vec::new();\n    for (&seed, units) in primary.iter().zip(walks) {\n        let mut pages = BTreeMap::<usize, f64>::new();\n        for &unit in units {\n            let value = if let Some(&value) = memo.get(&unit) {\n                value\n            } else {\n                let value = score(unit)?;\n                if !value.is_finite() {\n                    return Err("nonfinite source score".into());\n                }\n                memo.insert(unit, value);\n                value\n            };\n            for scores in [&mut pages, &mut global_pages] {\n                scores\n                    .entry(unit / 8)\n                    .and_modify(|old| *old = old.max(value))\n                    .or_insert(value);\n            }\n        }\n        let mut ranked = pages.into_iter().collect::<Vec<_>>();\n        ranked.sort_unstable_by(|&(lp, l), &(rp, r)| r.total_cmp(&l).then(lp.cmp(&rp)));\n        let mut roster = vec![seed];\n        roster.extend(\n            ranked\n                .iter()\n                .filter(|&&(page, _)| page != seed)\n                .take(158)\n                .map(|&(page, _)| page),\n        );\n        if roster.len() != 159 {\n            return Err("source roster geometry".into());\n        }\n        selected.extend(roster.iter().copied());\n        per_graph.push(roster);\n    }\n    let mut ranked = selected\n        .into_iter()\n        .map(|page| (page, global_pages[&page]))\n        .collect::<Vec<_>>();\n    ranked.sort_unstable_by(|&(lp, l), &(rp, r)| r.total_cmp(&l).then(lp.cmp(&rp)));\n    Ok(Roster {\n        ranked,\n        per_graph,\n        unit_scores: memo.into_iter().collect(),\n    })\n}'
assert original.count(anchor)==1;fixed=original.replace(anchor,replacement);p.write_bytes(fixed)
subprocess.run([str(Path(cargo).parent/'rustfmt'),'--edition','2024','--check',str(p)],check=True)
fixed=p.read_bytes();(out/'walk_nomination.fixed.rs').write_bytes(fixed)
with (out/'green.log').open('w') as log:subprocess.run(cmd,check=True,stdout=log,stderr=subprocess.STDOUT)
with (out/'release.log').open('w') as log:subprocess.run([cargo,'build','--release','--locked','--manifest-path',str(repo/'Cargo.toml'),'--target-dir',str(out/'target'),'-p','borsuk','--bin','two_bit_walk_nomination','--jobs','4'],check=True,stdout=log,stderr=subprocess.STDOUT)
(out/'binaries').mkdir();binary=out/'binaries/two_bit_walk_nomination';binary.write_bytes((out/'target/release/two_bit_walk_nomination').read_bytes());binary.chmod(0o755)
result=dict(qualified=False,no_corpus_query=True,full_workspace_repeated=False,original_source_sha256=digest(original),fixed_source_sha256=digest(fixed),red_status=status,green_passed=True,binaries={'two_bit_walk_nomination':dict(bytes=binary.stat().st_size,sha256=digest(binary.read_bytes()))})
(out/'boundary-check.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
