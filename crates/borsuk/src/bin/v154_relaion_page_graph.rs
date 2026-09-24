//! ReLAION-1M GT-blind paired flat and graph page-plan scale diagnostic.
#![recursion_limit = "256"]

use borsuk::budgeted_page_rank::{
    BudgetedPagePlan, choose_budgeted_pages, choose_budgeted_pages_sparse,
};
use borsuk::unit_centroid_graph::UnitCentroidGraph;
use borsuk::unit_centroid_pages::UnitCentroidPages;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::time::Instant;

const ROWS: usize = 1_000_000;
const DIMS: usize = 768;
const QUERY_START: usize = 0;

fn records(path: &str) -> Result<Vec<Value>, Box<dyn Error>> {
    let values = BufReader::new(File::open(path)?)
        .lines()
        .map(|line| Ok(serde_json::from_str(&line?)?))
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    if values.len() != 1000 {
        return Err("fresh query count differs".into());
    }
    Ok(values)
}

fn numbers(value: &Value) -> Result<Vec<usize>, Box<dyn Error>> {
    value
        .as_array()
        .ok_or("integer list differs")?
        .iter()
        .map(|entry| {
            entry
                .as_u64()
                .and_then(|number| usize::try_from(number).ok())
                .ok_or_else(|| "integer differs".into())
        })
        .collect()
}

fn percentile_usize(values: &[usize], ordinal: usize) -> usize {
    let mut values = values.to_vec();
    values.sort_unstable();
    values[ordinal]
}

fn percentile_ms(values: &[u128], ordinal: usize) -> f64 {
    let mut values = values.to_vec();
    values.sort_unstable();
    values[ordinal] as f64 / 1e6
}

fn plan_json(plan: &BudgetedPagePlan) -> Value {
    json!({
        "selected_pages": plan.selected_pages,
        "ranges": plan.ranges.iter().map(|range| [range.start, range.end]).collect::<Vec<_>>(),
        "gets": plan.ranges.len(),
        "planned_bytes": plan.planned_bytes,
        "target_pages": plan.target_pages,
        "target_shortfall": plan.target_shortfall,
    })
}

struct FlatArm {
    scores: Vec<f32>,
    plan: BudgetedPagePlan,
    score_ns: u128,
    plan_ns: u128,
    elapsed_ns: u128,
}

fn flat_arm(
    scorer: &UnitCentroidPages,
    query: &[f32],
    primary: &[usize],
) -> Result<FlatArm, Box<dyn Error>> {
    let started = Instant::now();
    let scores = scorer.score_pages(query)?;
    let score_ns = started.elapsed().as_nanos();
    let plan = choose_budgeted_pages(&scores, primary, ROWS, DIMS, 4, 32, 16_777_216)
        .map_err(|error| format!("flat plan: {error:?}"))?;
    let elapsed_ns = started.elapsed().as_nanos();
    Ok(FlatArm {
        scores,
        plan,
        score_ns,
        plan_ns: elapsed_ns - score_ns,
        elapsed_ns,
    })
}

struct GraphArm {
    scored_pages: Vec<(usize, f32)>,
    provisional_pages: Vec<(usize, f32)>,
    evaluated_units: Vec<usize>,
    unit_evaluations: usize,
    cached_graph_squared: Vec<f32>,
    newly_scored_units: usize,
    work_exhausted: bool,
    plan: BudgetedPagePlan,
    search_ns: u128,
    score_ns: u128,
    plan_ns: u128,
    elapsed_ns: u128,
}

fn old_arm(
    graph: &UnitCentroidGraph,
    scorer: &UnitCentroidPages,
    query: &[f32],
    primary: &[usize],
    primary_pages: &BTreeSet<usize>,
) -> Result<GraphArm, Box<dyn Error>> {
    let p = primary_pages.len();
    let started = Instant::now();
    let found = graph.search(scorer, query, 8 * p, 16 * p)?;
    let search_ns = started.elapsed().as_nanos();
    let mut pages = primary_pages.clone();
    for &(unit, _) in &found.units {
        if pages.len() < p + 4 * p {
            pages.insert(unit / 8);
        }
    }
    let scored_pages = pages
        .iter()
        .map(|&page| Ok((page, scorer.score_page(query, page)?)))
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let score_ns = started.elapsed().as_nanos() - search_ns;
    let plan = choose_budgeted_pages_sparse(&scored_pages, primary, ROWS, DIMS, 4, 32, 16_777_216)
        .map_err(|error| format!("V150 sparse plan: {error:?}"))?;
    let elapsed_ns = started.elapsed().as_nanos();
    Ok(GraphArm {
        scored_pages,
        provisional_pages: found
            .units
            .into_iter()
            .map(|(unit, score)| (unit / 8, score))
            .collect(),
        evaluated_units: found.evaluated_units,
        unit_evaluations: found.unit_evaluations,
        cached_graph_squared: Vec::new(),
        newly_scored_units: pages
            .iter()
            .map(|&page| (page * 8..((page + 1) * 8).min((ROWS + 31) / 32)).count())
            .sum(),
        work_exhausted: found.work_exhausted,
        plan,
        search_ns,
        score_ns,
        plan_ns: elapsed_ns - search_ns - score_ns,
        elapsed_ns,
    })
}

fn new_arm(
    graph: &UnitCentroidGraph,
    scorer: &UnitCentroidPages,
    query: &[f32],
    primary: &[usize],
    primary_pages: &BTreeSet<usize>,
) -> Result<GraphArm, Box<dyn Error>> {
    let p = primary_pages.len();
    let started = Instant::now();
    let page_roster = primary_pages.iter().copied().collect::<Vec<_>>();
    let found = graph.search_pages_seeded(scorer, query, &page_roster, 4 * p, 16 * p)?;
    let search_ns = started.elapsed().as_nanos();
    let mut pages = primary_pages.clone();
    pages.extend(found.pages.iter().map(|&(page, _)| page));
    let scored_pages = pages
        .iter()
        .map(|&page| Ok((page, scorer.score_page(query, page)?)))
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let score_ns = started.elapsed().as_nanos() - search_ns;
    let plan = choose_budgeted_pages_sparse(&scored_pages, primary, ROWS, DIMS, 4, 32, 16_777_216)
        .map_err(|error| format!("V151 sparse plan: {error:?}"))?;
    let elapsed_ns = started.elapsed().as_nanos();
    Ok(GraphArm {
        scored_pages,
        provisional_pages: found.pages,
        evaluated_units: found.evaluated_units,
        unit_evaluations: found.unit_evaluations,
        cached_graph_squared: Vec::new(),
        newly_scored_units: pages
            .iter()
            .map(|&page| (page * 8..((page + 1) * 8).min((ROWS + 31) / 32)).count())
            .sum(),
        work_exhausted: found.work_exhausted,
        plan,
        search_ns,
        score_ns,
        plan_ns: elapsed_ns - search_ns - score_ns,
        elapsed_ns,
    })
}

fn cached_arm(
    graph: &UnitCentroidGraph,
    scorer: &UnitCentroidPages,
    query: &[f32],
    primary: &[usize],
    primary_pages: &BTreeSet<usize>,
) -> Result<GraphArm, Box<dyn Error>> {
    let p = primary_pages.len();
    let started = Instant::now();
    let page_roster = primary_pages.iter().copied().collect::<Vec<_>>();
    let found =
        graph.search_pages_seeded_with_scores(scorer, query, &page_roster, 4 * p, 16 * p)?;
    let search_ns = started.elapsed().as_nanos();
    let mut pages = primary_pages.clone();
    pages.extend(found.pages.iter().map(|&(page, _)| page));
    let page_roster = pages.iter().copied().collect::<Vec<_>>();
    let (scored_pages, newly_scored_units) =
        scorer.score_pages_sparse_cached(query, &page_roster, &found.evaluated_scores)?;
    let score_ns = started.elapsed().as_nanos() - search_ns;
    let plan = choose_budgeted_pages_sparse(&scored_pages, primary, ROWS, DIMS, 4, 32, 16_777_216)
        .map_err(|error| format!("V152 sparse plan: {error:?}"))?;
    let elapsed_ns = started.elapsed().as_nanos();
    let cached_graph_squared = found
        .evaluated_units
        .iter()
        .map(|&unit| found.evaluated_scores[&(unit as u32)])
        .collect();
    Ok(GraphArm {
        scored_pages,
        provisional_pages: found.pages,
        evaluated_units: found.evaluated_units,
        unit_evaluations: found.unit_evaluations,
        cached_graph_squared,
        newly_scored_units,
        work_exhausted: found.work_exhausted,
        plan,
        search_ns,
        score_ns,
        plan_ns: elapsed_ns - search_ns - score_ns,
        elapsed_ns,
    })
}

fn graph_json(arm: &GraphArm) -> Value {
    let exact_units = arm
        .scored_pages
        .iter()
        .flat_map(|&(page, _)| {
            (page * 8..((page + 1) * 8).min((ROWS + 31) / 32)).collect::<Vec<_>>()
        })
        .collect::<BTreeSet<_>>();
    let union_units = exact_units
        .iter()
        .copied()
        .chain(arm.evaluated_units.iter().copied())
        .collect::<BTreeSet<_>>();
    json!({
        "scored_pages": arm.scored_pages,
        "provisional_pages": arm.provisional_pages,
        "evaluated_units": arm.evaluated_units,
        "unit_evaluations": arm.unit_evaluations,
        "exact_page_unit_evaluations": exact_units.len(),
        "distinct_scored_units": union_units.len(),
        "total_unit_distance_computations": arm.unit_evaluations + arm.newly_scored_units,
        "cached_graph_squared": arm.cached_graph_squared,
        "newly_scored_units": arm.newly_scored_units,
        "search_ns": arm.search_ns,
        "score_ns": arm.score_ns,
        "plan_ns": arm.plan_ns,
        "work_exhausted": arm.work_exhausted,
        "elapsed_ns": arm.elapsed_ns,
        "plan": plan_json(&arm.plan),
    })
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 6 {
        return Err(
            "usage: v154_relaion_page_graph QUERIES ROUTING CENTROIDS GRAPH_OUT SUMMARY_OUT".into(),
        );
    }
    let queries = records(&args[1])?;
    let routing = records(&args[2])?;
    let centroid_blob = fs::read(&args[3])?;
    let scorer = UnitCentroidPages::decode(&centroid_blob)?;
    if (
        scorer.rows(),
        scorer.dimensions(),
        scorer.unit_rows(),
        scorer.page_rows(),
    ) != (ROWS, DIMS, 32, 256)
    {
        return Err("frozen centroid geometry differs".into());
    }
    let build_start = Instant::now();
    let graph_bytes = UnitCentroidGraph::build(&scorer, &centroid_blob)?.encode()?;
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
    let graph_sha = format!("{:x}", Sha256::digest(&graph_bytes));
    fs::write(&args[4], &graph_bytes)?;
    let graph = UnitCentroidGraph::decode(&graph_bytes, &centroid_blob, &scorer)?;
    let mut output = BufWriter::new(io::stdout().lock());
    let (mut flat_ns, mut old_ns, mut new_ns) = (
        Vec::with_capacity(1000),
        Vec::with_capacity(1000),
        Vec::with_capacity(1000),
    );
    let mut cached_ns = Vec::with_capacity(1000);
    let (mut flat_score_ns, mut flat_plan_ns) =
        (Vec::with_capacity(1000), Vec::with_capacity(1000));
    let mut flat_target_shortfall_queries = 0usize;
    let (mut cached_search_ns, mut cached_score_ns, mut cached_plan_ns) = (
        Vec::with_capacity(1000),
        Vec::with_capacity(1000),
        Vec::with_capacity(1000),
    );
    let (mut flat_even_ns, mut flat_odd_ns, mut cached_even_ns, mut cached_odd_ns) = (
        Vec::with_capacity(500),
        Vec::with_capacity(500),
        Vec::with_capacity(500),
        Vec::with_capacity(500),
    );
    let mut cached_bytes = 0usize;
    let (mut flat_gets, mut cached_gets) = (0usize, 0usize);
    let mut cached_total_work = Vec::with_capacity(1000);
    let mut cached_missing_work = Vec::with_capacity(1000);
    let mut cached_graph_work = Vec::with_capacity(1000);
    let mut cached_exact_work = Vec::with_capacity(1000);
    let mut cached_union_work = Vec::with_capacity(1000);
    let mut cached_shortfall = Vec::with_capacity(1000);
    let mut cached_capture = 0usize;
    let (mut flat_bytes, mut new_bytes, mut new_work) = (0usize, 0usize, Vec::with_capacity(1000));
    let (mut exact_work, mut union_work, mut total_work) = (
        Vec::with_capacity(1000),
        Vec::with_capacity(1000),
        Vec::with_capacity(1000),
    );
    let (mut flat_pages, mut new_capture) = (0usize, 0usize);
    let mut new_shortfall = Vec::with_capacity(1000);
    let (mut all_primary, mut paired_primary, mut all_caps, mut max_score_diff) =
        (true, true, true, 0.0f32);
    let mut flat_primary_shortfall_queries = 0usize;
    for ordinal in 0..1000 {
        let query_record = &queries[ordinal];
        let route_record = &routing[ordinal];
        if (
            query_record["query_ordinal"].as_u64(),
            route_record["query_ordinal"].as_u64(),
        ) != (Some(ordinal as u64), Some(ordinal as u64))
            || (
                query_record["source_query_ordinal"].as_u64(),
                route_record["source_query_ordinal"].as_u64(),
            ) != (
                Some((QUERY_START + ordinal) as u64),
                Some((QUERY_START + ordinal) as u64),
            )
        {
            return Err(format!("fresh query identity differs: {ordinal}").into());
        }
        let query = query_record["query"]
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
        let primary = numbers(&route_record["primary"])?;
        let nominees = numbers(&route_record["nominees"])?;
        if query.len() != DIMS
            || primary.len() != 100
            || nominees.len() != 512
            || primary.iter().any(|&row| row >= ROWS)
            || nominees.iter().any(|&row| row >= ROWS)
        {
            return Err(format!("fresh route geometry differs: {ordinal}").into());
        }
        let primary_pages = primary.iter().map(|row| row / 256).collect::<BTreeSet<_>>();
        let (flat, old, new, cached) = if ordinal % 2 == 0 {
            let flat = flat_arm(&scorer, &query, &primary)?;
            let cached = cached_arm(&graph, &scorer, &query, &primary, &primary_pages)?;
            let new = new_arm(&graph, &scorer, &query, &primary, &primary_pages)?;
            let old = old_arm(&graph, &scorer, &query, &primary, &primary_pages)?;
            (flat, old, new, cached)
        } else {
            let cached = cached_arm(&graph, &scorer, &query, &primary, &primary_pages)?;
            let flat = flat_arm(&scorer, &query, &primary)?;
            let old = old_arm(&graph, &scorer, &query, &primary, &primary_pages)?;
            let new = new_arm(&graph, &scorer, &query, &primary, &primary_pages)?;
            (flat, old, new, cached)
        };
        if new.evaluated_units != cached.evaluated_units
            || new.provisional_pages != cached.provisional_pages
            || new.unit_evaluations != cached.unit_evaluations
            || new.work_exhausted != cached.work_exhausted
            || new
                .scored_pages
                .iter()
                .map(|&(page, _)| page)
                .collect::<Vec<_>>()
                != cached
                    .scored_pages
                    .iter()
                    .map(|&(page, _)| page)
                    .collect::<Vec<_>>()
        {
            return Err(format!("V152 changed V151 graph roster: {ordinal}").into());
        }
        let diff = old
            .scored_pages
            .iter()
            .chain(new.scored_pages.iter())
            .chain(cached.scored_pages.iter())
            .map(|&(page, score)| (score - flat.scores[page]).abs())
            .fold(0.0f32, f32::max);
        max_score_diff = max_score_diff.max(diff);
        let primary_kept = [&flat.plan, &old.plan, &new.plan, &cached.plan]
            .iter()
            .all(|plan| {
                primary_pages
                    .iter()
                    .all(|page| plan.selected_pages.binary_search(page).is_ok())
            });
        let flat_retained = primary_pages
            .iter()
            .filter(|page| flat.plan.selected_pages.binary_search(page).is_ok())
            .copied()
            .collect::<Vec<_>>();
        let retention_matches_flat = [&old.plan, &new.plan, &cached.plan].iter().all(|plan| {
            primary_pages
                .iter()
                .filter(|page| plan.selected_pages.binary_search(page).is_ok())
                .copied()
                .collect::<Vec<_>>()
                == flat_retained
        });
        let caps = [&flat.plan, &old.plan, &new.plan, &cached.plan]
            .iter()
            .all(|plan| plan.ranges.len() <= 32 && plan.planned_bytes <= 16_777_216)
            && old.unit_evaluations <= 16 * primary_pages.len()
            && new.unit_evaluations <= 16 * primary_pages.len()
            && new.scored_pages.len() <= 5 * primary_pages.len()
            && cached.unit_evaluations <= 16 * primary_pages.len()
            && cached.scored_pages.len() <= 5 * primary_pages.len();
        all_primary &= primary_kept;
        paired_primary &= retention_matches_flat;
        flat_primary_shortfall_queries += usize::from(flat_retained.len() < primary_pages.len());
        all_caps &= caps;
        flat_ns.push(flat.elapsed_ns);
        flat_score_ns.push(flat.score_ns);
        flat_plan_ns.push(flat.plan_ns);
        flat_target_shortfall_queries += usize::from(flat.plan.target_shortfall > 0);
        old_ns.push(old.elapsed_ns);
        new_ns.push(new.elapsed_ns);
        cached_ns.push(cached.elapsed_ns);
        cached_search_ns.push(cached.search_ns);
        cached_score_ns.push(cached.score_ns);
        cached_plan_ns.push(cached.plan_ns);
        if ordinal % 2 == 0 {
            flat_even_ns.push(flat.elapsed_ns);
            cached_even_ns.push(cached.elapsed_ns);
        } else {
            flat_odd_ns.push(flat.elapsed_ns);
            cached_odd_ns.push(cached.elapsed_ns);
        }
        flat_bytes += flat.plan.planned_bytes;
        new_bytes += new.plan.planned_bytes;
        cached_bytes += cached.plan.planned_bytes;
        flat_gets += flat.plan.ranges.len();
        cached_gets += cached.plan.ranges.len();
        cached_total_work.push(cached.unit_evaluations + cached.newly_scored_units);
        cached_missing_work.push(cached.newly_scored_units);
        cached_graph_work.push(cached.unit_evaluations);
        cached_shortfall.push(cached.plan.target_shortfall);
        let cached_exact_units = cached
            .scored_pages
            .iter()
            .flat_map(|&(page, _)| page * 8..((page + 1) * 8).min((ROWS + 31) / 32))
            .collect::<BTreeSet<_>>();
        cached_exact_work.push(cached_exact_units.len());
        cached_union_work.push(
            cached_exact_units
                .iter()
                .copied()
                .chain(cached.evaluated_units.iter().copied())
                .collect::<BTreeSet<_>>()
                .len(),
        );
        new_work.push(new.unit_evaluations);
        new_shortfall.push(new.plan.target_shortfall);
        let exact_units = new
            .scored_pages
            .iter()
            .flat_map(|&(page, _)| page * 8..((page + 1) * 8).min((ROWS + 31) / 32))
            .collect::<BTreeSet<_>>();
        exact_work.push(exact_units.len());
        union_work.push(
            exact_units
                .iter()
                .copied()
                .chain(new.evaluated_units.iter().copied())
                .collect::<BTreeSet<_>>()
                .len(),
        );
        total_work.push(new.unit_evaluations + exact_units.len());
        flat_pages += flat.plan.selected_pages.len();
        new_capture += new
            .plan
            .selected_pages
            .iter()
            .filter(|page| flat.plan.selected_pages.binary_search(page).is_ok())
            .count();
        cached_capture += cached
            .plan
            .selected_pages
            .iter()
            .filter(|page| flat.plan.selected_pages.binary_search(page).is_ok())
            .count();
        serde_json::to_writer(
            &mut output,
            &json!({
                "query_ordinal": ordinal,
                "source_query_ordinal": QUERY_START + ordinal,
                "primary_pages": primary_pages,
                "primary_distinct_pages": primary_pages.len(),
                "flat_scores": flat.scores,
                "flat_elapsed_ns": flat.elapsed_ns,
                "flat_score_ns": flat.score_ns,
                "flat_plan_ns": flat.plan_ns,
                "flat_plan": plan_json(&flat.plan),
                "v150": graph_json(&old),
                "v151": graph_json(&new),
                "v152": graph_json(&cached),
                "primary_retained": primary_kept,
                "paired_primary_retention": retention_matches_flat,
                "plan_caps_hold": caps,
                "max_abs_page_score_difference": diff,
            }),
        )?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    let summary = json!({
        "schema": "borsuk-v154-relaion-page-graph-plan-v1",
        "split": "ReLAION-1M-validation-1000-already-used",
        "rows": ROWS, "dimensions": DIMS, "queries": 1000,
        "graph_sha256": graph_sha, "graph_bytes": graph_bytes.len(),
        "graph_build_ms": build_ms,
        "flat_p50_ms": percentile_ms(&flat_ns, 499),
        "flat_p95_ms": percentile_ms(&flat_ns, 949),
        "flat_p99_ms": percentile_ms(&flat_ns, 989),
        "flat_score_p95_ms": percentile_ms(&flat_score_ns, 949),
        "flat_plan_p95_ms": percentile_ms(&flat_plan_ns, 949),
        "flat_target_shortfall_queries": flat_target_shortfall_queries,
        "v150_p95_ms": percentile_ms(&old_ns, 949),
        "v151_p95_ms": percentile_ms(&new_ns, 949),
        "v152_p50_ms": percentile_ms(&cached_ns, 499),
        "v152_p95_ms": percentile_ms(&cached_ns, 949),
        "v152_p99_ms": percentile_ms(&cached_ns, 989),
        "v152_search_p95_ms": percentile_ms(&cached_search_ns, 949),
        "v152_score_p95_ms": percentile_ms(&cached_score_ns, 949),
        "v152_plan_p95_ms": percentile_ms(&cached_plan_ns, 949),
        "flat_even_p95_ms": percentile_ms(&flat_even_ns, 474),
        "flat_odd_p95_ms": percentile_ms(&flat_odd_ns, 474),
        "v152_even_p95_ms": percentile_ms(&cached_even_ns, 474),
        "v152_odd_p95_ms": percentile_ms(&cached_odd_ns, 474),
        "v152_total_unit_work_p95": percentile_usize(&cached_total_work, 949),
        "v152_new_unit_work_p95": percentile_usize(&cached_missing_work, 949),
        "v152_graph_work_p95": percentile_usize(&cached_graph_work, 949),
        "v152_exact_page_work_p95": percentile_usize(&cached_exact_work, 949),
        "v152_distinct_union_work_p95": percentile_usize(&cached_union_work, 949),
        "v152_target_shortfall_queries": cached_shortfall.iter().filter(|&&n| n > 0).count(),
        "v152_target_shortfall_p95": percentile_usize(&cached_shortfall, 949),
        "v151_graph_work_p95": percentile_usize(&new_work, 949),
        "v151_exact_page_work_p95": percentile_usize(&exact_work, 949),
        "v151_distinct_union_work_p95": percentile_usize(&union_work, 949),
        "v151_total_unit_work_p95": percentile_usize(&total_work, 949),
        "v151_target_shortfall_queries": new_shortfall.iter().filter(|&&n| n > 0).count(),
        "v151_target_shortfall_p95": percentile_usize(&new_shortfall, 949),
        "flat_planned_bytes_total": flat_bytes,
        "flat_gets_total": flat_gets,
        "v151_planned_bytes_total": new_bytes,
        "v152_planned_bytes_total": cached_bytes,
        "v152_gets_total": cached_gets,
        "v152_selected_flat_page_capture": cached_capture,
        "flat_selected_pages_total": flat_pages,
        "v151_selected_flat_page_capture": new_capture,
        "all_primary_retained": all_primary,
        "paired_primary_retention": paired_primary,
        "flat_primary_shortfall_queries": flat_primary_shortfall_queries,
        "all_plan_caps": all_caps,
        "max_abs_page_score_difference": max_score_diff,
        "gt_opened": false,
    });
    fs::write(&args[5], format!("{}\n", serde_json::to_string(&summary)?))?;
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
