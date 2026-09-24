//! Replay the production page-admission module against frozen V140 plans.

#[path = "../../../../crates/borsuk/src/budgeted_page_rank.rs"]
mod budgeted_page_rank;

use budgeted_page_rank::choose_budgeted_pages;
use serde_json::{Value, json};
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::time::Instant;

fn rows(path: &str) -> Result<Vec<Value>, Box<dyn Error>> {
    let mut result = Vec::new();
    for line in BufReader::new(File::open(path)?).lines() {
        result.push(serde_json::from_str(&line?)?);
    }
    if result.len() != 1000 {
        return Err(format!("frozen query count differs: {path}").into());
    }
    Ok(result)
}

fn unsigned(value: &Value) -> Result<usize, Box<dyn Error>> {
    value
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| "nonnegative integer differs".into())
}

fn quantile(values: &[u128], index: usize) -> u128 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[index]
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 8 {
        return Err(
            "usage: v145_page_parity COHORT ROWS DIM SCORES PRIMARY V140_RAW SUMMARY".into(),
        );
    }
    let cohort = &args[1];
    let rows_count = args[2].parse::<usize>()?;
    let dimensions = args[3].parse::<usize>()?;
    let page_count = rows_count.div_ceil(256);
    let score_bytes = fs::read(&args[4])?;
    let expected_len = 1000usize
        .checked_mul(page_count)
        .and_then(|value| value.checked_mul(4))
        .ok_or("score matrix length overflow")?;
    if score_bytes.len() != expected_len {
        return Err("score matrix byte count differs".into());
    }
    let primary = rows(&args[5])?;
    let frozen = rows(&args[6])?;
    let mut durations = Vec::with_capacity(1000);
    let mut total_bytes = 0usize;
    let mut total_gets = 0usize;
    let mut parity_count = 0usize;
    let mut output = BufWriter::new(io::stdout().lock());
    for ordinal in 0..1000 {
        if unsigned(&primary[ordinal]["query_ordinal"])? != ordinal
            || unsigned(&frozen[ordinal]["query_ordinal"])? != ordinal
        {
            return Err(format!("query ordinal differs: {ordinal}").into());
        }
        let first = ordinal * page_count * 4;
        let scores = score_bytes[first..first + page_count * 4]
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<_>>();
        let physical = primary[ordinal]["primary"]
            .as_array()
            .ok_or("primary roster differs")?
            .iter()
            .map(unsigned)
            .collect::<Result<Vec<_>, _>>()?;
        if physical.len() != 100 {
            return Err("primary width differs".into());
        }
        let start = Instant::now();
        let plan = choose_budgeted_pages(
            &scores, &physical, rows_count, dimensions, 4, 32, 16_777_216,
        )
        .map_err(|error| format!("page planner failed at {ordinal}: {error:?}"))?;
        let elapsed = start.elapsed().as_nanos();
        durations.push(elapsed);
        let expected = &frozen[ordinal]["variants"]["4"];
        let ranges = plan
            .ranges
            .iter()
            .map(|range| [range.start, range.end])
            .collect::<Vec<_>>();
        let agrees = expected["selected_pages"] == json!(plan.selected_pages)
            && expected["ranges"] == json!(ranges)
            && expected["planned_bytes"] == json!(plan.planned_bytes)
            && expected["gets"] == json!(ranges.len())
            && expected["target_shortfall"] == json!(plan.target_shortfall)
            && expected["primary_pages_retained"] == json!(plan.primary_pages_retained);
        serde_json::to_writer(
            &mut output,
            &json!({
                "query_ordinal": ordinal,
                "selected_pages": plan.selected_pages,
                "ranges": ranges,
                "planned_bytes": plan.planned_bytes,
                "gets": plan.ranges.len(),
                "target_shortfall": plan.target_shortfall,
                "primary_pages_retained": plan.primary_pages_retained,
                "planner_ns": elapsed,
                "matches_v140": agrees,
            }),
        )?;
        output.write_all(b"\n")?;
        if !agrees {
            output.flush()?;
            return Err(format!("V140 beta4 page-plan parity differs at {ordinal}").into());
        }
        parity_count += 1;
        total_bytes += plan.planned_bytes;
        total_gets += plan.ranges.len();
    }
    output.flush()?;
    let summary = json!({
        "schema": "borsuk-v145-production-page-parity-v1",
        "cohort": cohort,
        "query_count": parity_count,
        "rows": rows_count,
        "dimensions": dimensions,
        "beta": 4,
        "matches_v140": parity_count == 1000,
        "total_planned_bytes": total_bytes,
        "total_gets": total_gets,
        "planner_p50_ms": quantile(&durations, 499) as f64 / 1e6,
        "planner_p95_ms": quantile(&durations, 949) as f64 / 1e6,
        "planner_p99_ms": quantile(&durations, 989) as f64 / 1e6,
    });
    fs::write(&args[7], format!("{}\n", serde_json::to_string(&summary)?))?;
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
