//! Exact Rust PQ arithmetic replay of the frozen V287 page-ranking diagnostic.

use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fs;
use std::io::{self, BufRead};

use borsuk::{pq64_nominee::Pq64Error, pq64_router_artifact::load_source_router};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn pq<T>(result: Result<T, Pq64Error>) -> Result<T, io::Error> {
    result.map_err(|error| io::Error::other(format!("PQ score: {error:?}")))
}

fn check(path: &str, expected: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    if format!("{:x}", Sha256::digest(&bytes)) != expected {
        return Err(invalid("input digest differs").into());
    }
    Ok(bytes)
}

fn cover(selected: &BTreeSet<usize>) -> (BTreeSet<usize>, usize) {
    let pages = selected.iter().copied().collect::<Vec<_>>();
    let mut gaps = pages
        .windows(2)
        .enumerate()
        .filter_map(|(i, pair)| {
            (pair[1] > pair[0] + 1).then_some((pair[1] - pair[0] - 1, pair[0], i))
        })
        .collect::<Vec<_>>();
    let bridge_count = (gaps.len() + 1).saturating_sub(32);
    gaps.sort_unstable();
    let mut covered = selected.clone();
    for &(_, _, i) in gaps.iter().take(bridge_count) {
        covered.extend(pages[i] + 1..pages[i + 1]);
    }
    (covered, gaps.len() + 1 - bridge_count)
}

fn fetched(
    scores: &[f32],
    truth: &[usize],
    id_to_page: &HashMap<usize, usize>,
) -> Result<(usize, usize, usize), Box<dyn Error>> {
    let mut ranking = (0..scores.len()).collect::<Vec<_>>();
    ranking.sort_unstable_by(|&a, &b| scores[b].total_cmp(&scores[a]).then(a.cmp(&b)));
    let mut selected = BTreeSet::new();
    for page in ranking {
        selected.insert(page);
        let (covered, gets) = cover(&selected);
        if covered.len() > 84 || gets > 32 {
            selected.remove(&page);
        }
    }
    let (covered, gets) = cover(&selected);
    let hits = truth
        .iter()
        .filter(|id| {
            id_to_page
                .get(id)
                .is_some_and(|page| covered.contains(page))
        })
        .count();
    let bytes = covered
        .iter()
        .map(|page| (100_000 - page * 256).min(256) * 780)
        .sum();
    Ok((hits, gets, bytes))
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 7 {
        return Err(
            "usage: v287_exact_pq_page ROUTER ROUTER_SHA SQ8 REQUESTS GT_U32 OUTPUT".into(),
        );
    }
    let artifact = load_source_router(std::path::Path::new(&args[1]), &args[2])?;
    if artifact.router.rows() != 100_000
        || artifact.router.dimensions() != 768
        || artifact.router.page_rows() != 256
    {
        return Err(invalid("router geometry").into());
    }
    let sq8 = check(&args[3], &artifact.sq8_sha256)?;
    let requests = check(
        &args[4],
        "1de0122f73d1b72e54498640b9701ce6d156b513629596447580c85fac302ba4",
    )?;
    let truth = check(
        &args[5],
        "f6630d0edf06539752c3fbf129ae01e58d3a3cf7b6aefa4decaa9c979e8ba355",
    )?;
    if sq8.len() != 78_000_000 || truth.len() != 25_600 {
        return Err(invalid("input geometry").into());
    }
    let mut id_to_page = HashMap::with_capacity(100_000);
    for (row, bytes) in sq8.chunks_exact(780).enumerate() {
        let id = usize::try_from(i64::from_le_bytes(bytes[..8].try_into()?))?;
        if id_to_page.insert(id, row / 256).is_some() {
            return Err(invalid("duplicate SQ8 ID").into());
        }
    }
    let cosine = pq(artifact.router.cosine_view())?;
    let mut hits = [Vec::new(), Vec::new()];
    let mut max_gets = 0;
    let mut max_bytes = 0;
    for (ordinal, line) in requests.as_slice().lines().enumerate() {
        let request: Value = serde_json::from_str(&line?)?;
        if request["query_ordinal"].as_u64() != Some(ordinal as u64) {
            return Err(invalid("query ordinal").into());
        }
        let query = serde_json::from_value::<Vec<f32>>(request["query"].clone())?;
        let expected = truth[ordinal * 400..(ordinal + 1) * 400]
            .chunks_exact(4)
            .map(|word| u32::from_le_bytes(word.try_into().unwrap()) as usize)
            .collect::<Vec<_>>();
        let euclidean = pq(artifact.router.prepare_query(&query))?;
        let cosine_query = pq(cosine.prepare_query(&query))?;
        let mut scores = [vec![f32::NEG_INFINITY; 391], vec![f32::NEG_INFINITY; 391]];
        for row in 0..100_000 {
            scores[0][row / 256] = scores[0][row / 256].max(-pq(euclidean.score_row(row))?);
            scores[1][row / 256] = scores[1][row / 256].max(pq(cosine_query.score_row(row))?);
        }
        for arm in 0..2 {
            let (count, gets, bytes) = fetched(&scores[arm], &expected, &id_to_page)?;
            hits[arm].push(count);
            max_gets = max_gets.max(gets);
            max_bytes = max_bytes.max(bytes);
        }
    }
    if hits.iter().any(|arm| arm.len() != 64) || max_gets > 32 || max_bytes > 16_777_216 {
        return Err(invalid("roster or physical cap").into());
    }
    let mut arms = serde_json::Map::new();
    for (name, mut arm) in ["euclidean", "cosine"].into_iter().zip(hits) {
        let mean = arm.iter().sum::<usize>() as f64 / 64.0;
        arm.sort_unstable();
        let p05 = arm[3];
        arms.insert(
            name.to_owned(),
            json!({
                "mean_fetched_gt_hits": mean, "p05_fetched_gt_hits": p05,
                "advance": mean >= 98.9 && p05 >= 96,
            }),
        );
    }
    let result = json!({
        "schema": "borsuk-v287-rust-pq-page-bound-v1",
        "dataset": "CoHere first100k D768 cosine k100", "split": "development0-63",
        "queries": 64, "max_gets": max_gets, "max_planned_bytes": max_bytes,
        "arms": arms,
    });
    fs::write(&args[6], format!("{}\n", result))?;
    println!("{result}");
    Ok(())
}
