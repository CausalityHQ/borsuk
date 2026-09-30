//! Closed-discovery union nomination falsifier; never a serving default.
use std::{collections::BTreeSet, error::Error};
fn union_pages(a: &[usize], b: &[usize]) -> Result<Vec<usize>, Box<dyn Error>> {
    for pages in [a, b] {
        if pages.len() != 159
            || pages.iter().any(|&p| p >= 391)
            || pages.iter().copied().collect::<BTreeSet<_>>().len() != 159
        {
            return Err("frozen candidate geometry".into());
        }
    }
    Ok(a.iter()
        .chain(b)
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}
#[cfg(not(test))]
fn main() -> Result<(), Box<dyn Error>> {
    replay::run()
}
#[cfg(not(test))]
mod replay {
    use super::*;
    use borsuk::{
        budgeted_page_rank::choose_budgeted_pages_sparse,
        two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits},
        two_bit_source::TwoBitPlane,
    };
    use serde::Deserialize;
    use sha2::{Digest, Sha256};
    use std::{fs, io::Write, path::Path, time::Instant};
    #[derive(Deserialize)]
    struct Trace {
        query_ordinal: usize,
        arm: String,
        root_sha256: String,
        ranked_candidate_pages: Vec<usize>,
    }
    fn read(path: &Path, digest: &str, cap: u64) -> Result<Vec<u8>, Box<dyn Error>> {
        if fs::metadata(path)?.len() > cap {
            return Err("input size cap".into());
        }
        let body = fs::read(path)?;
        if body.len() as u64 > cap || format!("{:x}", Sha256::digest(&body)) != digest {
            return Err("input identity".into());
        }
        Ok(body)
    }
    fn root_common(body: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        let value: serde_json::Value = serde_json::from_slice(body)?;
        let mut text = std::str::from_utf8(body)?.to_owned();
        for field in ["graph_sha256", "graph_resident_bytes"] {
            let key = format!("\"{field}\":");
            let start = text.find(&key).ok_or("graph field")? + key.len();
            let tail = &text[start..];
            let token = serde_json::to_string(&value[field])?;
            let space = tail.len() - tail.trim_start().len();
            if !tail[space..].starts_with(&token) {
                return Err("graph token".into());
            }
            text.replace_range(start + space..start + space + token.len(), "0");
        }
        Ok(text.into_bytes())
    }
    fn cpu() -> u128 {
        let t = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
        (t.tv_sec as u128) * 1_000_000_000 + t.tv_nsec as u128
    }
    pub fn run() -> Result<(), Box<dyn Error>> {
        let a = std::env::args().collect::<Vec<_>>();
        if a.len() != 10 {
            return Err(
                "usage: ROOT ROOT_SHA OTHER OTHER_SHA REQUESTS REQUESTS_SHA TRACE TRACE_SHA OUTPUT"
                    .into(),
            );
        }
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 1_073_741_824,
            max_active_queries: 1,
            max_query_bytes: 16773120,
            max_query_gets: 32,
            max_parallel_gets: 32,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 16,
            max_query_scratch_bytes: 400000,
            already_pinned_bytes: 0,
        };
        for (root, digest) in [(&a[1], &a[2]), (&a[3], &a[4])] {
            let generation = TwoBitGeneration::open(Path::new(root), digest, limits)?;
            drop(generation);
        }
        let r = read(&Path::new(&a[1]).join("manifest.json"), &a[2], 65536)?;
        let s = read(&Path::new(&a[3]).join("manifest.json"), &a[4], 65536)?;
        if root_common(&r)? != root_common(&s)? {
            return Err("non-graph root bytes changed".into());
        }
        let root: serde_json::Value = serde_json::from_slice(&r)?;
        let plane = TwoBitPlane::open(
            &Path::new(&a[1]).join("plane"),
            root["plane_manifest_sha256"]
                .as_str()
                .ok_or("plane digest")?,
            root["sq8_object_sha256"].as_str().ok_or("sq8 digest")?,
            268435456,
        )?;
        if plane.receipt().rows != 100000 || plane.receipt().dimensions != 768 {
            return Err("frozen plane geometry".into());
        }
        let traces = read(Path::new(&a[7]), &a[8], 16 << 20)?;
        let traces = std::str::from_utf8(&traces)?
            .lines()
            .map(serde_json::from_str::<Trace>)
            .collect::<Result<Vec<_>, _>>()?;
        if traces.len() != 128 {
            return Err("trace roster".into());
        }
        let requests = read(Path::new(&a[5]), &a[6], 64 << 20)?;
        let requests = std::str::from_utf8(&requests)?
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        if requests.len() != 1000
            || requests
                .iter()
                .enumerate()
                .any(|(i, r)| r["query_ordinal"].as_u64() != Some(i as u64))
        {
            return Err("request roster".into());
        }
        let mut out = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&a[9])?;
        for q in 0..64 {
            let pair = &traces[q * 2..q * 2 + 2];
            let mut pages = [Vec::new(), Vec::new()];
            for (position, t) in pair.iter().enumerate() {
                let control = (q % 2 == 0) == (position == 0);
                let index = if control { 0 } else { 1 };
                if t.query_ordinal != q
                    || t.arm != if control { "control" } else { "candidate" }
                    || t.root_sha256 != a[if control { 2 } else { 4 }]
                {
                    return Err("paired trace authority/order".into());
                }
                pages[index] = t.ranked_candidate_pages.clone();
            }
            let union = union_pages(&pages[0], &pages[1])?;
            let query = requests[q]["query"]
                .as_array()
                .ok_or("query vector")?
                .iter()
                .map(|v| v.as_f64().map(|v| v as f32).ok_or("query coordinate"))
                .collect::<Result<Vec<_>, _>>()?;
            let prepared = plane.prepare_query(&query, 400000)?;
            let mut arms = vec![("control", pages[0].clone()), ("union", union)];
            if q % 2 == 1 {
                arms.reverse();
            }
            for (arm, candidates) in arms {
                let wall = Instant::now();
                let cpu_start = cpu();
                let mut ranked = Vec::with_capacity(candidates.len());
                for &p in &candidates {
                    let mut score = f64::NEG_INFINITY;
                    for row in p * 256..((p + 1) * 256).min(100000) {
                        score = score.max(prepared.score(plane.record(row).ok_or("plane row")?)?);
                    }
                    ranked.push((p, score));
                }
                ranked.sort_unstable_by(|&(lp, l), &(rp, r)| r.total_cmp(&l).then(lp.cmp(&rp)));
                let primary = ranked[0].0;
                let order = ranked
                    .iter()
                    .enumerate()
                    .map(|(rank, &(p, _))| (p, rank as f32))
                    .collect::<Vec<_>>();
                let plan = choose_budgeted_pages_sparse(
                    &order,
                    &[primary * 256],
                    100000,
                    768,
                    391,
                    32,
                    16773120,
                )
                .map_err(|e| format!("physical admission: {e:?}"))?;
                let wall_ns = wall.elapsed().as_nanos();
                let cpu_ns = cpu() - cpu_start;
                writeln!(
                    out,
                    "{}",
                    serde_json::json!({"query_ordinal":q,"arm":arm,"ranges":plan.ranges.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>(),"planned_bytes":plan.planned_bytes,"ranked_candidate_pages":ranked.iter().map(|&(p,_)|p).collect::<Vec<_>>(),"primary_page":primary,"selected_pages":plan.selected_pages,"nomination_wall_ns":wall_ns,"nomination_process_cpu_ns":cpu_ns})
                )?;
            }
        }
        out.sync_all()?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frozen_union_has_unique_bounded_pages() {
        let a = (0..159).collect::<Vec<_>>();
        let b = (100..259).collect::<Vec<_>>();
        assert_eq!(
            union_pages(&a, &b).expect("union candidates not implemented"),
            (0..259).collect::<Vec<_>>()
        );
        let mut duplicate = a.clone();
        duplicate[1] = duplicate[0];
        assert!(union_pages(&duplicate, &b).is_err());
        let mut outside = a.clone();
        outside[0] = 391;
        assert!(union_pages(&outside, &b).is_err());
        assert!(union_pages(&a[..158], &b).is_err());
    }
}
