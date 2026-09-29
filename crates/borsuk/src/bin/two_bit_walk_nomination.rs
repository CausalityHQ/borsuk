//! Bounded closed-walk source nomination falsifier; never a serving default.
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
};

#[derive(Debug, serde::Serialize)]
struct Roster {
    ranked: Vec<(usize, f64)>,
    per_graph: Vec<Vec<usize>>,
    unit_scores: Vec<(usize, f64)>,
}
fn reroster(
    primary: &[usize],
    walks: &[Vec<usize>],
    mut score: impl FnMut(usize) -> Result<f64, Box<dyn Error>>,
) -> Result<Roster, Box<dyn Error>> {
    if primary.len() != 2 || walks.len() != 2 {
        return Err("frozen graph roster".into());
    }
    for (&seed, units) in primary.iter().zip(walks) {
        if seed >= 391
            || units.len() != 1272
            || units.iter().any(|&unit| unit >= 3125)
            || units.iter().copied().collect::<BTreeSet<_>>().len() != 1272
            || (seed * 8..((seed + 1) * 8).min(3125)).any(|unit| !units.contains(&unit))
        {
            return Err("frozen walk geometry".into());
        }
    }
    let mut memo = BTreeMap::new();
    let mut global_pages = BTreeMap::<usize, f64>::new();
    let mut selected = BTreeSet::new();
    let mut per_graph = Vec::new();
    for (&seed, units) in primary.iter().zip(walks) {
        let mut pages = BTreeMap::<usize, f64>::new();
        for &unit in units {
            let value = if let Some(&value) = memo.get(&unit) {
                value
            } else {
                let value = score(unit)?;
                if !value.is_finite() {
                    return Err("nonfinite source score".into());
                }
                memo.insert(unit, value);
                value
            };
            for scores in [&mut pages, &mut global_pages] {
                scores
                    .entry(unit / 8)
                    .and_modify(|old| *old = old.max(value))
                    .or_insert(value);
            }
        }
        let mut ranked = pages.into_iter().collect::<Vec<_>>();
        ranked.sort_unstable_by(|&(lp, l), &(rp, r)| r.total_cmp(&l).then(lp.cmp(&rp)));
        let mut roster = vec![seed];
        roster.extend(
            ranked
                .iter()
                .filter(|&&(page, _)| page != seed)
                .take(158)
                .map(|&(page, _)| page),
        );
        if roster.len() != 159 {
            return Err("source roster geometry".into());
        }
        selected.extend(roster.iter().copied());
        per_graph.push(roster);
    }
    let mut ranked = selected
        .into_iter()
        .map(|page| (page, global_pages[&page]))
        .collect::<Vec<_>>();
    ranked.sort_unstable_by(|&(lp, l), &(rp, r)| r.total_cmp(&l).then(lp.cmp(&rp)));
    Ok(Roster {
        ranked,
        per_graph,
        unit_scores: memo.into_iter().collect(),
    })
}

fn read(path: &std::path::Path, digest: &str, cap: u64) -> Result<Vec<u8>, Box<dyn Error>> {
    use std::io::Read;
    let mut body = Vec::new();
    std::fs::File::open(path)?
        .take(cap + 1)
        .read_to_end(&mut body)?;
    use sha2::{Digest, Sha256};
    if body.len() as u64 > cap || format!("{:x}", Sha256::digest(&body)) != digest {
        return Err("input identity or cap".into());
    }
    Ok(body)
}
#[derive(serde::Deserialize)]
struct Discovery {
    seed_page: usize,
    walk_evaluated_units: Vec<usize>,
}
#[derive(serde::Deserialize)]
struct Trace {
    query_ordinal: usize,
    arm: String,
    root_sha256: String,
    discoveries: Vec<Discovery>,
}
#[derive(serde::Deserialize)]
struct Request {
    query_ordinal: usize,
    query: Vec<f32>,
}
fn main() -> Result<(), Box<dyn Error>> {
    use borsuk::{
        budgeted_page_rank::choose_budgeted_pages_sparse,
        two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits},
        two_bit_source::TwoBitPlane,
    };
    use std::{io::Write, path::Path, time::Instant};
    let a = std::env::args().collect::<Vec<_>>();
    if a.len() != 9 {
        return Err(
            "usage: ROOT ROOT_SHA CONTROL_SHA REQUESTS REQUESTS_SHA TRACE TRACE_SHA OUTPUT".into(),
        );
    }
    let root = Path::new(&a[1]);
    let generation = TwoBitGeneration::open(
        root,
        &a[2],
        TwoBitGenerationLimits {
            max_memory_bytes: 1_073_741_824,
            max_active_queries: 1,
            max_query_bytes: 16773120,
            max_query_gets: 32,
            max_parallel_gets: 32,
            max_query_scratch_bytes: 400000,
            already_pinned_bytes: 0,
        },
    )?;
    if generation.rows() != 100000 {
        return Err("frozen rows".into());
    }
    drop(generation);
    let manifest: serde_json::Value =
        serde_json::from_slice(&read(&root.join("manifest.json"), &a[2], 65536)?)?;
    let plane = TwoBitPlane::open(
        &root.join("plane"),
        manifest["plane_manifest_sha256"]
            .as_str()
            .ok_or("plane identity")?,
        manifest["sq8_object_sha256"]
            .as_str()
            .ok_or("SQ8 identity")?,
        268435456,
    )?;
    if plane.receipt().rows != 100000 || plane.receipt().dimensions != 768 {
        return Err("frozen plane geometry".into());
    }
    let traces = read(Path::new(&a[6]), &a[7], 16 << 20)?;
    let traces = std::str::from_utf8(&traces)?
        .lines()
        .map(serde_json::from_str::<Trace>)
        .collect::<Result<Vec<_>, _>>()?;
    let requests = read(Path::new(&a[4]), &a[5], 64 << 20)?;
    let requests = std::str::from_utf8(&requests)?
        .lines()
        .map(serde_json::from_str::<Request>)
        .collect::<Result<Vec<_>, _>>()?;
    if traces.len() != 128
        || requests.len() != 1000
        || requests
            .iter()
            .enumerate()
            .any(|(q, r)| r.query_ordinal != q || r.query.len() != 768)
    {
        return Err("request/trace roster".into());
    }
    // Validate all paired identities before preparing or scoring any query.
    for q in 0..64 {
        for (position, t) in traces[q * 2..q * 2 + 2].iter().enumerate() {
            let control = (q % 2 == 0) == (position == 0);
            if t.query_ordinal != q
                || t.arm != if control { "control" } else { "candidate" }
                || t.root_sha256 != a[if control { 3 } else { 2 }]
                || t.discoveries.len() != 2
            {
                return Err("paired authority/order".into());
            }
        }
    }
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&a[8])?;
    for q in 0..64 {
        let trace = traces[q * 2..q * 2 + 2]
            .iter()
            .find(|t| t.arm == "candidate")
            .ok_or("candidate trace")?;
        let primary = trace
            .discoveries
            .iter()
            .map(|d| d.seed_page)
            .collect::<Vec<_>>();
        let walks = trace
            .discoveries
            .iter()
            .map(|d| d.walk_evaluated_units.clone())
            .collect::<Vec<_>>();
        let prepared = plane.prepare_query(&requests[q].query, 400000)?;
        let start = Instant::now();
        let cpu = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
        let roster = reroster(&primary, &walks, |unit| {
            let mut maximum = f64::NEG_INFINITY;
            for row in unit * 32..(unit + 1) * 32 {
                maximum = maximum.max(prepared.score(plane.record(row).ok_or("source row")?)?);
            }
            Ok(maximum)
        })?;
        let order = roster
            .ranked
            .iter()
            .enumerate()
            .map(|(rank, &(page, _))| (page, rank as f32))
            .collect::<Vec<_>>();
        let plan = choose_budgeted_pages_sparse(
            &order,
            &[roster.ranked[0].0 * 256],
            100000,
            768,
            391,
            32,
            16773120,
        )
        .map_err(|e| format!("physical admission: {e:?}"))?;
        let after = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
        let cpu_ns = (after.tv_sec as i128 - cpu.tv_sec as i128) * 1_000_000_000
            + after.tv_nsec as i128
            - cpu.tv_nsec as i128;
        writeln!(
            output,
            "{}",
            serde_json::json!({"query_ordinal":q,"arm":"candidate","root_sha256":a[2],"ranges":plan.ranges.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>(),"planned_bytes":plan.planned_bytes,"selected_pages":plan.selected_pages,"ranked_candidate_pages":roster.ranked.iter().map(|&(p,_)|p).collect::<Vec<_>>(),"primary_page":roster.ranked[0].0,"per_graph_pages":roster.per_graph,"source_unit_scores":roster.unit_scores,"source_rows_scored":roster.unit_scores.len()*32,"nomination_wall_ns":start.elapsed().as_nanos(),"nomination_process_cpu_ns":cpu_ns,"incoming_service_http_measured":false})
        )?;
    }
    output.sync_all()?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn walk() -> Vec<usize> {
        let mut units = (0..1272).collect::<Vec<_>>();
        units[1271] = 1280;
        units
    }
    #[test]
    fn early_source_max_replaces_centroid_cutoff_and_reuses_duplicates() {
        let a = walk();
        let mut b = a.clone();
        b.reverse();
        let mut calls = 0;
        let result = reroster(&[0, 0], &[a, b], |unit| {
            calls += 1;
            Ok(if unit == 1280 { 10. } else { 1. })
        })
        .expect("walk source roster not implemented");
        assert_eq!(calls, 1272);
        assert_eq!(result.unit_scores.len() * 32, 40704);
        assert_eq!(result.ranked.len(), 159);
        assert_eq!(result.ranked[0], (160, 10.));
        assert!(!result.ranked.iter().any(|&(p, _)| p == 158));
        assert_eq!(result.per_graph[0], result.per_graph[1]);
        assert_eq!(result.ranked[1], (0, 1.));
    }
    #[test]
    fn malformed_walks_fail_before_scoring_and_nonfinite_scores_fail() {
        let good = walk();
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("input");
        std::fs::write(&file, b"abc").unwrap();
        let digest = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        assert_eq!(read(&file, digest, 3).unwrap(), b"abc");
        assert!(read(&file, digest, 2).is_err());
        assert!(read(&file, &"0".repeat(64), 3).is_err());
        let mut outside = good.clone();
        outside[0] = 3125;
        let mut missing_primary = good.clone();
        missing_primary[5] = 1281;
        for bad in [good[..1271].to_vec(), outside, missing_primary] {
            assert!(
                reroster(&[0, 0], &[good.clone(), bad], |_| panic!(
                    "invalid input scored"
                ))
                .is_err()
            );
        }
        let mut duplicate = good.clone();
        duplicate[1] = duplicate[2];
        assert!(
            reroster(&[0, 0], &[good.clone(), duplicate], |_| panic!(
                "duplicate input scored"
            ))
            .is_err()
        );
        assert!(
            reroster(&[391, 0], &[good.clone(), good.clone()], |_| panic!(
                "invalid primary scored"
            ))
            .is_err()
        );
        assert!(reroster(&[0, 0], &[good.clone(), good], |_| Ok(f64::NAN)).is_err());
    }
}
