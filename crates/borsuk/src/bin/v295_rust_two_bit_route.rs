//! Frozen V293 candidate pages scored by the public Rust two-bit codec.
use borsuk::{
    budgeted_page_rank::choose_budgeted_pages_sparse, rotated_two_bit::RotatedTwoBitCodec,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{error::Error, fs, io, path::Path, time::Instant};

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn checked(path: &Path, digest: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    if format!("{:x}", Sha256::digest(&bytes)) != digest {
        return Err(invalid("input identity").into());
    }
    Ok(bytes)
}
fn lines(bytes: &[u8]) -> Result<Vec<Value>, Box<dyn Error>> {
    std::str::from_utf8(bytes)?
        .lines()
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}
fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 9 {
        return Err("usage: v295_rust_two_bit_route MEAN RECORDS RECORDS_SHA REQUESTS CANDIDATES QUERY_COUNT PLANS RESULT".into());
    }
    let count = args[6].parse::<usize>()?;
    if count != 4 && count != 64 {
        return Err(invalid("query count").into());
    }
    let mean = checked(
        Path::new(&args[1]),
        "b50ffae3e16cc5e155ceee3adffcac0dc04d6812b0a1bb8fc01cbd259b58440a",
    )?;
    let records = checked(Path::new(&args[2]), &args[3])?;
    if mean.len() != 3072 || records.len() != 20_000_000 {
        return Err(invalid("source geometry").into());
    }
    let mean = mean
        .chunks_exact(4)
        .map(|word| f32::from_le_bytes(word.try_into().unwrap()))
        .collect::<Vec<_>>();
    let codec = RotatedTwoBitCodec::new(&mean, 20260923)?;
    let requests = lines(&checked(
        Path::new(&args[4]),
        "1de0122f73d1b72e54498640b9701ce6d156b513629596447580c85fac302ba4",
    )?)?;
    let candidates = lines(&checked(
        Path::new(&args[5]),
        "c24e53f8e740ad569331c774f46ef3161873e3a765d20a345290af50a1b974d0",
    )?)?;
    let reference = lines(&checked(
        Path::new("docs/research/v293-plans.jsonl"),
        "00190ff7ed017b2150d1415f36077be774c0e59075ec0eb0e691f81511ae3aa4",
    )?)?;
    if requests.len() != 64 || candidates.len() != 64 || reference.len() != 64 {
        return Err(invalid("roster").into());
    }
    let started = Instant::now();
    let mut plans = Vec::new();
    let mut differences = Vec::new();
    let mut max_gets = 0;
    let mut max_bytes = 0;
    let mut max_rows = 0;
    for ordinal in 0..count {
        for roster in [&requests, &candidates, &reference] {
            if roster[ordinal]["query_ordinal"].as_u64() != Some(ordinal as u64) {
                return Err(invalid("ordinal").into());
            }
        }
        let query: Vec<f32> = serde_json::from_value(requests[ordinal]["query"].clone())?;
        let prepared = codec.prepare_query(&query, 400_000)?;
        let pages: Vec<usize> = serde_json::from_value(candidates[ordinal]["pages"].clone())?;
        if pages.len() != 159
            || pages.iter().any(|&page| page >= 391)
            || pages.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(invalid("candidate pages").into());
        }
        let mut ranked = Vec::with_capacity(pages.len());
        let mut scored_rows = 0;
        for page in pages {
            let first = page * 256;
            let end = ((page + 1) * 256).min(100_000);
            let mut score = f64::NEG_INFINITY;
            for record in records[first * 200..end * 200].chunks_exact(200) {
                score = score.max(prepared.score(record)?);
            }
            ranked.push((page, score));
            scored_rows += end - first;
        }
        ranked.sort_unstable_by(|&(left_page, left), &(right_page, right)| {
            right.total_cmp(&left).then(left_page.cmp(&right_page))
        });
        let primary = [ranked[0].0 * 256];
        let ranked = ranked
            .iter()
            .enumerate()
            .map(|(rank, &(page, _))| (page, rank as f32))
            .collect::<Vec<_>>();
        let plan =
            choose_budgeted_pages_sparse(&ranked, &primary, 100_000, 768, 391, 32, 84 * 256 * 780)
                .map_err(|_| invalid("page admission"))?;
        let pages = plan
            .ranges
            .iter()
            .flat_map(|range| range.start / (256 * 780)..range.end.div_ceil(256 * 780))
            .collect::<Vec<_>>();
        if pages.len() > 84
            || plan.ranges.len() > 32
            || plan.planned_bytes > 16_777_216
            || scored_rows > 40_704
        {
            return Err(invalid("budget").into());
        }
        let old_pages: Vec<usize> = serde_json::from_value(reference[ordinal]["pages"].clone())?;
        let mut result = json!({"query_ordinal":ordinal,"pages":pages});
        if pages == old_pages {
            result["fetched_gt_hits"] = reference[ordinal]["fetched_gt_hits"].clone();
            result["returned_gt_hits"] = reference[ordinal]["returned_gt_hits"].clone();
        } else {
            differences.push(ordinal);
        }
        plans.push(result);
        max_gets = max_gets.max(plan.ranges.len());
        max_bytes = max_bytes.max(plan.planned_bytes);
        max_rows = max_rows.max(scored_rows);
    }
    let plans_raw = plans
        .iter()
        .map(|plan| format!("{plan}\n"))
        .collect::<String>();
    let result = json!({"schema":"borsuk-v295-rust-two-bit-route-v1", "dataset":"CoHere first100k D768 cosine k100", "split":format!("development0-{}", count - 1), "queries":count,
        "all_page_sets_equal":differences.is_empty(),"page_set_differences":differences,
        "max_gets":max_gets,"max_planned_bytes":max_bytes,"max_coded_rows_scored":max_rows,
        "records_sha256":args[3],"plans_sha256":format!("{:x}",Sha256::digest(plans_raw.as_bytes())),
        "functional_replay_wall_us":started.elapsed().as_micros()});
    fs::write(&args[7], plans_raw)?;
    fs::write(&args[8], format!("{result}\n"))?;
    println!("{result}");
    Ok(())
}
