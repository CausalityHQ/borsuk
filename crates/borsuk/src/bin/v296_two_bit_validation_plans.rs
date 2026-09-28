//! Frozen bounded-centroid/two-bit physical plans and paired flat control.
use borsuk::{
    budgeted_page_rank::{BudgetedPagePlan, choose_budgeted_pages_sparse},
    object_native_generation::{ObjectNativeGeneration, ObjectNativeLimits},
    rotated_two_bit::{PreparedTwoBit, RotatedTwoBitCodec},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{error::Error, fs, io, path::Path, time::Instant};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inputs {
    dataset: String,
    root: String,
    root_sha: String,
    mean: String,
    mean_sha: String,
    records: String,
    records_sha: String,
    requests: String,
    requests_sha: String,
    first: usize,
    count: usize,
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn checked(path: &str, digest: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    if format!("{:x}", Sha256::digest(&bytes)) != digest {
        return Err(invalid("input identity").into());
    }
    Ok(bytes)
}
fn plan(
    prepared: &PreparedTwoBit,
    records: &[u8],
    pages: &[usize],
) -> Result<BudgetedPagePlan, Box<dyn Error>> {
    if pages.len() != 159
        || pages.iter().any(|&page| page >= 391)
        || pages.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid("candidate geometry").into());
    }
    let mut ranked = Vec::with_capacity(pages.len());
    for &page in pages {
        let first = page * 256;
        let end = ((page + 1) * 256).min(100_000);
        let mut score = f64::NEG_INFINITY;
        for row in records[first * 200..end * 200].chunks_exact(200) {
            score = score.max(prepared.score(row)?);
        }
        ranked.push((page, score));
    }
    ranked.sort_unstable_by(|&(left_page, left), &(right_page, right)| {
        right.total_cmp(&left).then(left_page.cmp(&right_page))
    });
    let primary = [ranked[0].0 * 256];
    let order = ranked
        .iter()
        .enumerate()
        .map(|(rank, &(page, _))| (page, rank as f32))
        .collect::<Vec<_>>();
    choose_budgeted_pages_sparse(&order, &primary, 100_000, 768, 391, 32, 84 * 256 * 780)
        .map_err(|_| invalid("page admission").into())
}
fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 5 {
        return Err("usage: v296_two_bit_validation_plans CONFIG CONFIG_SHA PLANS RESULT".into());
    }
    let input: Inputs = serde_json::from_slice(&checked(&args[1], &args[2])?)?;
    if !((input.first == 0 && input.count == 64)
        || (input.first == 256 && matches!(input.count, 64 | 744)))
    {
        return Err(invalid("frozen query split").into());
    }
    let generation = ObjectNativeGeneration::open(
        Path::new(&input.root),
        &input.root_sha,
        ObjectNativeLimits {
            max_memory_bytes: 2 * 1024 * 1024 * 1024,
            max_active_queries: 1,
            max_query_bytes: 16_777_216,
            max_query_gets: 32,
            max_parallel_gets: 32,
            max_router_regions: 391,
            max_router_shortlist: 512,
            already_pinned_bytes: 0,
        },
    )
    .map_err(|_| invalid("generation authentication"))?;
    if generation.pages().rows() != 100_000
        || generation.pages().dimensions() != 768
        || generation.centroids().unit_rows() != 32
        || generation.centroids().page_rows() != 256
    {
        return Err(invalid("generation geometry").into());
    }
    let mean = checked(&input.mean, &input.mean_sha)?;
    let records = checked(&input.records, &input.records_sha)?;
    if mean.len() != 3072 || records.len() != 20_000_000 {
        return Err(invalid("code geometry").into());
    }
    let mean = mean
        .chunks_exact(4)
        .map(|word| f32::from_le_bytes(word.try_into().unwrap()))
        .collect::<Vec<_>>();
    let codec = RotatedTwoBitCodec::new(&mean, 20260923)?;
    let requests = checked(&input.requests, &input.requests_sha)?;
    let requests = std::str::from_utf8(&requests)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    if requests.len() != 1000
        || requests
            .iter()
            .enumerate()
            .any(|(ordinal, request)| request["query_ordinal"].as_u64() != Some(ordinal as u64))
    {
        return Err(invalid("query roster").into());
    }
    let mut samples = Vec::with_capacity(input.count * 2);
    let started = Instant::now();
    for ordinal in input.first..input.first + input.count {
        let query: Vec<f32> = serde_json::from_value(requests[ordinal]["query"].clone())?;
        let prepared = codec.prepare_query(&query, 400_000)?;
        let seed = generation
            .graph()
            .search(generation.centroids(), &query, 1, 128)?;
        let seed_page = seed.units.first().ok_or_else(|| invalid("no seed"))?.0 / 8;
        let found = generation.graph().search_pages_seeded(
            generation.centroids(),
            &query,
            &[seed_page],
            158,
            1272,
        )?;
        if seed.unit_evaluations > 128 || found.unit_evaluations > 1272 {
            return Err(invalid("centroid work cap").into());
        }
        let mut graph_pages = vec![seed_page];
        graph_pages.extend(found.pages.iter().map(|&(page, _)| page));
        graph_pages.sort_unstable();
        let scores = generation.centroids().score_pages(&query)?;
        let mut flat_pages = (0..391).collect::<Vec<_>>();
        flat_pages.sort_unstable_by(|&left, &right| {
            scores[left]
                .total_cmp(&scores[right])
                .then(left.cmp(&right))
        });
        flat_pages.truncate(159);
        flat_pages.sort_unstable();
        for (arm, candidates, unit_evaluations) in [
            (
                "graph",
                graph_pages,
                seed.unit_evaluations + found.unit_evaluations,
            ),
            ("flat", flat_pages, generation.centroids().unit_count()),
        ] {
            let selected = plan(&prepared, &records, &candidates)?;
            let pages = selected
                .ranges
                .iter()
                .flat_map(|range| range.start / (256 * 780)..range.end.div_ceil(256 * 780))
                .collect::<Vec<_>>();
            if pages.len() > 84 || selected.ranges.len() > 32 || selected.planned_bytes > 16_777_216
            {
                return Err(invalid("physical budget").into());
            }
            let code_rows = candidates
                .iter()
                .map(|&page| (100_000 - page * 256).min(256))
                .sum::<usize>();
            samples.push(json!({"query_ordinal":ordinal,"arm":arm,"candidate_pages":candidates,"pages":pages,"planned_gets":selected.ranges.len(),"planned_bytes":selected.planned_bytes,"coded_rows_scored":code_rows,"centroid_evaluations":unit_evaluations}));
        }
    }
    let raw = samples
        .iter()
        .map(|sample| format!("{sample}\n"))
        .collect::<String>();
    let result = json!({"schema":"borsuk-v296-validation-plans-v1","dataset":input.dataset,"metric":"cosine","dimensions":768,"rows":100_000,"k":100,"first":input.first,"queries":input.count,"config_sha256":args[2],"plans_sha256":format!("{:x}",Sha256::digest(raw.as_bytes())),"functional_replay_wall_us":started.elapsed().as_micros()});
    fs::write(&args[3], raw)?;
    fs::write(&args[4], format!("{result}\n"))?;
    println!("{result}");
    Ok(())
}
