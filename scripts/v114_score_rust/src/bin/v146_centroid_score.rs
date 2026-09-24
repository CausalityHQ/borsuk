//! Replay production f16 centroid scoring and page admission against V140.

#[path = "../../../../crates/borsuk/src/budgeted_page_rank.rs"]
mod budgeted_page_rank;
#[path = "../../../../crates/borsuk/src/unit_centroid_pages.rs"]
mod unit_centroid_pages;

use budgeted_page_rank::choose_budgeted_pages;
use serde_json::{Value, json};
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::time::Instant;
use unit_centroid_pages::UnitCentroidPages;

fn records(path: &str) -> Result<Vec<Value>, Box<dyn Error>> {
    let records = BufReader::new(File::open(path)?)
        .lines()
        .map(|line| Ok(serde_json::from_str(&line?)?))
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    if records.len() != 1000 {
        return Err(format!("frozen record count differs: {path}").into());
    }
    Ok(records)
}

fn f32_file(path: &str, expected_count: usize) -> Result<Vec<f32>, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    if bytes.len() != expected_count.checked_mul(4).ok_or("f32 length overflow")? {
        return Err(format!("f32 byte count differs: {path}").into());
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
        .collect())
}

fn unsigned(value: &Value) -> Result<usize, Box<dyn Error>> {
    value
        .as_u64()
        .and_then(|number| usize::try_from(number).ok())
        .ok_or_else(|| "nonnegative integer differs".into())
}

fn percentile(values: &[u128], index: usize) -> f64 {
    let mut values = values.to_vec();
    values.sort_unstable();
    values[index] as f64 / 1e6
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 14 {
        return Err("usage: v146_centroid_score COHORT ROWS DIM QUERIES PRIMARY SQ8 LOW STEP V145_SCORES V140_RAW CENTERS_OUT SCORES_OUT SUMMARY".into());
    }
    let cohort = &args[1];
    let rows = args[2].parse::<usize>()?;
    let dimensions = args[3].parse::<usize>()?;
    let page_count = rows.div_ceil(256);
    let queries = records(&args[4])?;
    let primary = records(&args[5])?;
    let low = f32_file(&args[7], dimensions)?;
    let step = f32_file(&args[8], dimensions)?;
    let reference = f32_file(&args[9], 1000 * page_count)?;
    let frozen = records(&args[10])?;
    let build_started = Instant::now();
    let centers = UnitCentroidPages::build_from_sq8_reader(
        &mut BufReader::new(File::open(&args[6])?),
        rows,
        dimensions,
        32,
        256,
        &low,
        &step,
    )?;
    let build_ms = build_started.elapsed().as_secs_f64() * 1000.0;
    fs::write(&args[11], &centers)?;
    let scorer = UnitCentroidPages::decode(&centers)?;
    if (
        scorer.rows(),
        scorer.dimensions(),
        scorer.unit_rows(),
        scorer.page_rows(),
    ) != (rows, dimensions, 32, 256)
    {
        return Err("decoded centroid geometry differs".into());
    }
    let mut score_ns = Vec::with_capacity(1000);
    let mut combined_ns = Vec::with_capacity(1000);
    let mut max_abs_score_diff = 0.0f32;
    let mut mismatches = 0usize;
    let mut first_mismatch = None;
    let mut output = BufWriter::new(io::stdout().lock());
    let mut score_output = BufWriter::new(File::create(&args[12])?);
    for ordinal in 0..1000 {
        if unsigned(&queries[ordinal]["query_ordinal"])? != ordinal
            || unsigned(&primary[ordinal]["query_ordinal"])? != ordinal
            || unsigned(&frozen[ordinal]["query_ordinal"])? != ordinal
        {
            return Err(format!("query ordinal differs: {ordinal}").into());
        }
        if cohort == "deep-image-96-angular-random100k"
            && (unsigned(&queries[ordinal]["source_query_ordinal"])? != 9000 + ordinal
                || unsigned(&primary[ordinal]["source_query_ordinal"])? != 9000 + ordinal)
        {
            return Err(format!("D96 source query ordinal differs: {ordinal}").into());
        }
        let query = queries[ordinal]["query"]
            .as_array()
            .ok_or("query vector differs")?
            .iter()
            .map(|value| {
                value
                    .as_f64()
                    .map(|x| x as f32)
                    .ok_or("query scalar differs")
            })
            .collect::<Result<Vec<_>, _>>()?;
        let physical = primary[ordinal]["primary"]
            .as_array()
            .ok_or("primary roster differs")?
            .iter()
            .map(unsigned)
            .collect::<Result<Vec<_>, _>>()?;
        if query.len() != dimensions || physical.len() != 100 {
            return Err(format!("frozen query/primary width differs: {ordinal}").into());
        }
        let started = Instant::now();
        let scores = scorer.score_pages(&query)?;
        let scoring_elapsed = started.elapsed().as_nanos();
        let plan = choose_budgeted_pages(&scores, &physical, rows, dimensions, 4, 32, 16_777_216)
            .map_err(|error| format!("page plan failed at {ordinal}: {error:?}"))?;
        let combined_elapsed = started.elapsed().as_nanos();
        for score in &scores {
            score_output.write_all(&score.to_le_bytes())?;
        }
        score_ns.push(scoring_elapsed);
        combined_ns.push(combined_elapsed);
        let reference_scores = &reference[ordinal * page_count..(ordinal + 1) * page_count];
        let query_max_abs_diff = scores
            .iter()
            .zip(reference_scores)
            .map(|(&actual, &reference)| (actual - reference).abs())
            .fold(0.0_f32, f32::max);
        max_abs_score_diff = max_abs_score_diff.max(query_max_abs_diff);
        let ranges = plan
            .ranges
            .iter()
            .map(|range| [range.start, range.end])
            .collect::<Vec<_>>();
        let expected = &frozen[ordinal]["variants"]["4"];
        let agrees = expected["selected_pages"] == json!(plan.selected_pages)
            && expected["ranges"] == json!(ranges)
            && expected["planned_bytes"] == json!(plan.planned_bytes)
            && expected["gets"] == json!(ranges.len())
            && expected["target_shortfall"] == json!(plan.target_shortfall)
            && expected["primary_pages_retained"] == json!(plan.primary_pages_retained);
        if !agrees {
            mismatches += 1;
            first_mismatch.get_or_insert(ordinal);
        }
        serde_json::to_writer(
            &mut output,
            &json!({
                "query_ordinal": ordinal,
                "score_max_abs_diff": query_max_abs_diff,
                "scoring_ns": scoring_elapsed,
                "scoring_and_planner_ns": combined_elapsed,
                "matches_v140": agrees,
                "selected_pages": plan.selected_pages,
                "ranges": ranges,
                "planned_bytes": plan.planned_bytes,
                "gets": plan.ranges.len(),
                "target_shortfall": plan.target_shortfall,
                "primary_pages_retained": plan.primary_pages_retained,
            }),
        )?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    score_output.flush()?;
    let summary = json!({
        "schema": "borsuk-v146-centroid-score-v1",
        "cohort": cohort,
        "rows": rows,
        "dimensions": dimensions,
        "query_count": 1000,
        "centroid_format": "BORSUCP1",
        "centroid_blob_bytes": centers.len(),
        "resident_array_bytes": scorer.resident_array_bytes(),
        "centroid_build_ms": build_ms,
        "exact_page_plan_matches": 1000 - mismatches,
        "first_page_plan_mismatch": first_mismatch,
        "max_abs_score_diff": max_abs_score_diff,
        "scoring_p50_ms": percentile(&score_ns, 499),
        "scoring_p95_ms": percentile(&score_ns, 949),
        "scoring_p99_ms": percentile(&score_ns, 989),
        "scoring_and_planner_p50_ms": percentile(&combined_ns, 499),
        "scoring_and_planner_p95_ms": percentile(&combined_ns, 949),
        "scoring_and_planner_p99_ms": percentile(&combined_ns, 989),
    });
    fs::write(&args[13], format!("{}\n", serde_json::to_string(&summary)?))?;
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
