//! GT-free V150 semantic unit-centroid graph screen on the frozen D96 cohort.

use borsuk::budgeted_page_rank::{choose_budgeted_pages, choose_budgeted_pages_sparse};
use borsuk::unit_centroid_graph::UnitCentroidGraph;
use borsuk::unit_centroid_pages::UnitCentroidPages;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::time::Instant;

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

fn unsigned(value: &Value) -> Result<usize, Box<dyn Error>> {
    value
        .as_u64()
        .and_then(|number| usize::try_from(number).ok())
        .ok_or_else(|| "nonnegative integer differs".into())
}

fn percentile_usize(values: &[usize], index: usize) -> usize {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[index]
}

fn percentile_ms(values: &[u128], index: usize) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[index] as f64 / 1e6
}

fn flat_baseline_ns(
    scorer: &UnitCentroidPages,
    query: &[f32],
    roster: &[usize],
    rows: usize,
) -> Result<u128, Box<dyn Error>> {
    let started = Instant::now();
    let scores = scorer.score_pages(query)?;
    let _plan = choose_budgeted_pages(
        &scores,
        roster,
        rows,
        scorer.dimensions(),
        4,
        32,
        16_777_216,
    )
    .map_err(|error| format!("paired flat plan failed: {error:?}"))?;
    Ok(started.elapsed().as_nanos())
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 9 {
        return Err("usage: v150_unit_centroid_graph QUERIES PRIMARY CENTROIDS FLAT_SCORES V140_RAW GRAPH_OUT SUMMARY_OUT ROWS".into());
    }
    let rows = args[8].parse::<usize>()?;
    let queries = records(&args[1])?;
    let primary = records(&args[2])?;
    let frozen = records(&args[5])?;
    let centroid_blob = fs::read(&args[3])?;
    let scorer = UnitCentroidPages::decode(&centroid_blob)?;
    let dimensions = scorer.dimensions();
    if (
        scorer.rows(),
        dimensions,
        scorer.unit_rows(),
        scorer.page_rows(),
    ) != (rows, 96, 32, 256)
    {
        return Err("frozen scorer geometry differs".into());
    }
    let page_count = rows.div_ceil(256);
    let reference_bytes = fs::read(&args[4])?;
    if reference_bytes.len() != 1000 * page_count * 4 {
        return Err("frozen flat score matrix length differs".into());
    }
    let reference = reference_bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
    if reference.iter().any(|value| !value.is_finite()) {
        return Err("frozen flat score matrix is nonfinite".into());
    }
    let build_start = Instant::now();
    let graph = UnitCentroidGraph::build(&scorer, &centroid_blob)?;
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
    let graph_bytes = graph.encode()?;
    fs::write(&args[6], &graph_bytes)?;
    let graph = UnitCentroidGraph::decode(&graph_bytes, &centroid_blob, &scorer)?;

    let mut output = BufWriter::new(io::stdout().lock());
    let mut evals = Vec::with_capacity(1000);
    let mut exact_evals = Vec::with_capacity(1000);
    let mut distinct_evals = Vec::with_capacity(1000);
    let mut total_evals = Vec::with_capacity(1000);
    let mut search_ns = Vec::with_capacity(1000);
    let mut scored_page_counts = Vec::with_capacity(1000);
    let mut combined_ns = Vec::with_capacity(1000);
    let mut flat_ns = Vec::with_capacity(1000);
    let mut max_abs_score_difference = 0.0f32;
    let mut all_primary_retained = true;
    let mut all_plan_caps = true;
    let mut exhausted = 0usize;
    let mut selected_capture = 0usize;
    let mut selected_reference = 0usize;
    let mut control_capture = 0usize;
    let mut shortfall_no_worse = true;
    for ordinal in 0..1000 {
        if unsigned(&queries[ordinal]["query_ordinal"])? != ordinal
            || unsigned(&primary[ordinal]["query_ordinal"])? != ordinal
            || unsigned(&frozen[ordinal]["query_ordinal"])? != ordinal
            || unsigned(&queries[ordinal]["source_query_ordinal"])? != ordinal + 9000
            || unsigned(&primary[ordinal]["source_query_ordinal"])? != ordinal + 9000
        {
            return Err(format!("frozen query identity differs: {ordinal}").into());
        }
        let query = queries[ordinal]["query"]
            .as_array()
            .ok_or("query vector differs")?
            .iter()
            .map(|value| {
                value
                    .as_f64()
                    .map(|number| number as f32)
                    .ok_or("query scalar differs")
            })
            .collect::<Result<Vec<_>, _>>()?;
        let roster = primary[ordinal]["primary"]
            .as_array()
            .ok_or("primary roster differs")?
            .iter()
            .map(unsigned)
            .collect::<Result<Vec<_>, _>>()?;
        if query.len() != dimensions || roster.len() != 100 {
            return Err(format!("frozen query/primary width differs: {ordinal}").into());
        }
        let primary_pages = roster.iter().map(|row| row / 256).collect::<BTreeSet<_>>();
        let p = primary_pages.len();
        let flat_before = if ordinal % 2 == 0 {
            Some(flat_baseline_ns(&scorer, &query, &roster, rows)?)
        } else {
            None
        };
        let additional_budget = (4 * p).min(page_count.saturating_sub(p));
        let nearest_units = 8 * p;
        let unit_budget = 16 * p;
        let started = Instant::now();
        let found = graph.search(&scorer, &query, nearest_units, unit_budget)?;
        let mut candidate_pages = primary_pages.clone();
        let mut returned_units = Vec::with_capacity(found.units.len());
        for &(unit, distance) in &found.units {
            returned_units.push((unit, distance));
            if candidate_pages.len() < p + additional_budget {
                candidate_pages.insert(unit / (scorer.page_rows() / scorer.unit_rows()));
            }
        }
        let scored_pages = candidate_pages
            .iter()
            .map(|&page| Ok((page, scorer.score_page(&query, page)?)))
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
        let search_elapsed = started.elapsed().as_nanos();
        let plan = choose_budgeted_pages_sparse(
            &scored_pages,
            &roster,
            rows,
            dimensions,
            4,
            32,
            16_777_216,
        )
        .map_err(|error| format!("sparse plan failed at {ordinal}: {error:?}"))?;
        let combined_elapsed = started.elapsed().as_nanos();
        let flat_elapsed = match flat_before {
            Some(elapsed) => elapsed,
            None => flat_baseline_ns(&scorer, &query, &roster, rows)?,
        };
        let mut exact_units = BTreeSet::new();
        for &page in &candidate_pages {
            for unit in page * 8..((page + 1) * 8).min(scorer.unit_count()) {
                exact_units.insert(unit);
            }
        }
        let exact_score_evaluations = exact_units.len();
        let graph_units = found
            .evaluated_units
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let distinct_scored_units = exact_units.union(&graph_units).count();
        let reference_scores = &reference[ordinal * page_count..(ordinal + 1) * page_count];
        let query_max_abs_score_difference = scored_pages
            .iter()
            .map(|&(page, score)| (score - reference_scores[page]).abs())
            .fold(0.0f32, f32::max);
        max_abs_score_difference = max_abs_score_difference.max(query_max_abs_score_difference);
        let retained = primary_pages
            .iter()
            .all(|page| plan.selected_pages.binary_search(page).is_ok());
        let budget_ok = found.unit_evaluations <= unit_budget
            && found.units.len() <= nearest_units
            && scored_pages.len() <= p + additional_budget
            && plan.ranges.len() <= 32
            && plan.planned_bytes <= 16_777_216;
        all_primary_retained &= retained;
        all_plan_caps &= budget_ok;
        exhausted += usize::from(found.work_exhausted);
        evals.push(found.unit_evaluations);
        exact_evals.push(exact_score_evaluations);
        distinct_evals.push(distinct_scored_units);
        total_evals.push(found.unit_evaluations + exact_score_evaluations);
        scored_page_counts.push(scored_pages.len());
        search_ns.push(search_elapsed);
        combined_ns.push(combined_elapsed);
        flat_ns.push(flat_elapsed);
        let baseline_pages = frozen[ordinal]["variants"]["4"]["selected_pages"]
            .as_array()
            .ok_or("V140 selected pages differ")?
            .iter()
            .map(unsigned)
            .collect::<Result<BTreeSet<_>, _>>()?;
        selected_capture += plan
            .selected_pages
            .iter()
            .filter(|page| baseline_pages.contains(page))
            .count();
        selected_reference += baseline_pages.len();
        let baseline_shortfall = unsigned(&frozen[ordinal]["variants"]["4"]["target_shortfall"])?;
        let query_shortfall_no_worse = plan.target_shortfall <= baseline_shortfall;
        shortfall_no_worse &= query_shortfall_no_worse;
        let mut control_pages = primary_pages.iter().copied().collect::<BTreeSet<_>>();
        let secondary_count = scored_pages.len() - p;
        for page in 0..page_count {
            if control_pages.len() == p + secondary_count {
                break;
            }
            control_pages.insert(page);
        }
        let control_scores = control_pages
            .into_iter()
            .map(|page| (page, reference_scores[page]))
            .collect::<Vec<_>>();
        let control_plan = choose_budgeted_pages_sparse(
            &control_scores,
            &roster,
            rows,
            dimensions,
            4,
            32,
            16_777_216,
        )
        .map_err(|error| format!("control plan failed at {ordinal}: {error:?}"))?;
        let query_control_capture = control_plan
            .selected_pages
            .iter()
            .filter(|page| baseline_pages.contains(page))
            .count();
        control_capture += query_control_capture;
        serde_json::to_writer(
            &mut output,
            &json!({
                "query_ordinal": ordinal,
                "source_query_ordinal": ordinal + 9000,
                "primary_distinct_pages": p,
                "additional_page_budget": additional_budget,
                "nearest_unit_budget": nearest_units,
                "unit_evaluation_budget": unit_budget,
                "unit_evaluations": found.unit_evaluations,
                "work_exhausted": found.work_exhausted,
                "returned_units": returned_units,
                "evaluated_units": found.evaluated_units,
                "exact_score_evaluations": exact_score_evaluations,
                "distinct_scored_units": distinct_scored_units,
                "total_unit_distance_computations": found.unit_evaluations + exact_score_evaluations,
                "visited_pages": scored_pages,
                "query_max_abs_score_difference": query_max_abs_score_difference,
                "search_ns": search_elapsed,
                "search_and_planner_ns": combined_elapsed,
                "paired_flat_score_and_planner_ns": flat_elapsed,
                "all_primary_retained": retained,
                "plan_caps_hold": budget_ok,
                "selected_pages": plan.selected_pages,
                "primary_pages": primary_pages,
                "ranges": plan.ranges.iter().map(|range| [range.start, range.end]).collect::<Vec<_>>(),
                "planned_bytes": plan.planned_bytes,
                "gets": plan.ranges.len(),
                "target_shortfall": plan.target_shortfall,
                "selected_baseline_capture": plan.selected_pages.iter().filter(|page| baseline_pages.contains(page)).count(),
                "baseline_selected_count": baseline_pages.len(),
                "baseline_selected_pages": baseline_pages,
                "control_selected_baseline_capture": query_control_capture,
                "control_selected_pages": control_plan.selected_pages,
                "control_visited_pages": control_scores.len(),
                "baseline_target_shortfall": baseline_shortfall,
                "shortfall_no_worse": query_shortfall_no_worse,
            }),
        )?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    let capture_fraction = selected_capture as f64 / selected_reference as f64;
    let control_fraction = control_capture as f64 / selected_reference as f64;
    let verdict = if all_primary_retained
        && all_plan_caps
        && shortfall_no_worse
        && max_abs_score_difference <= 0.0001
        && percentile_usize(&distinct_evals, 949) < scorer.unit_count() / 2
        && percentile_ms(&combined_ns, 949) < percentile_ms(&flat_ns, 949)
        && capture_fraction >= 0.95
        && capture_fraction >= control_fraction + 0.05
    {
        "pass"
    } else {
        "reject"
    };
    let summary = json!({
        "schema": "borsuk-v150-unit-centroid-graph-v1",
        "cohort": "deep-image-96-angular-random100k",
        "split": "publication-test ordinals 9000-9999, previously used",
        "rows": rows,
        "dimensions": dimensions,
        "query_count": 1000,
        "m": 16,
        "m0": 32,
        "ef_construction": 64,
        "graph_bytes": graph_bytes.len(),
        "graph_build_ms": build_ms,
        "centroid_resident_array_bytes": scorer.resident_array_bytes(),
        "all_primary_retained": all_primary_retained,
        "all_plan_caps": all_plan_caps,
        "work_exhausted_count": exhausted,
        "max_abs_visited_score_difference": max_abs_score_difference,
        "unit_evaluations_p50": percentile_usize(&evals, 499),
        "unit_evaluations_p95": percentile_usize(&evals, 949),
        "unit_evaluations_p99": percentile_usize(&evals, 989),
        "exact_score_evaluations_p95": percentile_usize(&exact_evals, 949),
        "distinct_scored_units_p95": percentile_usize(&distinct_evals, 949),
        "total_unit_distance_computations_p95": percentile_usize(&total_evals, 949),
        "scored_pages_p95": percentile_usize(&scored_page_counts, 949),
        "flat_unit_evaluations": scorer.unit_count(),
        "paired_flat_score_and_planner_p95_ms": percentile_ms(&flat_ns, 949),
        "search_p50_ms": percentile_ms(&search_ns, 499),
        "search_p95_ms": percentile_ms(&search_ns, 949),
        "search_p99_ms": percentile_ms(&search_ns, 989),
        "search_and_planner_p50_ms": percentile_ms(&combined_ns, 499),
        "search_and_planner_p95_ms": percentile_ms(&combined_ns, 949),
        "search_and_planner_p99_ms": percentile_ms(&combined_ns, 989),
        "selected_baseline_capture": selected_capture,
        "baseline_selected_count": selected_reference,
        "selected_baseline_capture_fraction": capture_fraction,
        "control_selected_baseline_capture": control_capture,
        "control_selected_baseline_capture_fraction": control_fraction,
        "shortfall_no_worse": shortfall_no_worse,
        "verdict": verdict,
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
