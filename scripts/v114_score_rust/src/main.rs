//! V114 research gate adapter around the production exact SQ8 mirror.

#[path = "../../../crates/borsuk/src/exact_sq8_mirror.rs"]
mod exact_sq8_mirror;
#[path = "../../../crates/borsuk/src/exact_sq8_nominee.rs"]
mod exact_sq8_nominee;
#[path = "../../../crates/borsuk/src/pq64_nominee.rs"]
mod pq64_nominee;
#[path = "../../../crates/borsuk/src/pq64_router_artifact.rs"]
mod pq64_router_artifact;
#[path = "../../../crates/borsuk/src/returned_sq8.rs"]
mod returned_sq8;
#[path = "../../../crates/borsuk/src/sq8_page_authority.rs"]
mod sq8_page_authority;
#[path = "../../../crates/borsuk/src/serving_generation.rs"]
mod serving_generation;
#[path = "../../../crates/borsuk/src/physical_interval.rs"]
mod physical_interval;

use exact_sq8_mirror::{ExactSq8Mirror, MirrorManifest, Placement};
use exact_sq8_nominee::{Sq8Geometry, primary_ordinals};
use pq64_router_artifact::load_source_router;
use returned_sq8::{ReturnedRange, rank_returned_ranges};
use sha2::{Digest, Sha256};
use physical_interval::{IntervalGeometry, IntervalPlan, PlanError,
    normalize_budget_lattice, plan_weighted_intervals};
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::Instant;

fn parse_manifest(value: &serde_json::Value) -> Result<MirrorManifest, String> {
    let integer = |path: &[&str]| -> Result<u64, String> {
        let mut current = value;
        for part in path {
            current = current
                .get(part)
                .ok_or_else(|| format!("missing {}", path.join(".")))?;
        }
        current
            .as_u64()
            .ok_or_else(|| format!("invalid {}", path.join(".")))
    };
    let string = |key: &str| -> Result<String, String> {
        value
            .get(key)
            .and_then(|item| item.as_str())
            .map(str::to_owned)
            .ok_or_else(|| format!("invalid {key}"))
    };
    let floats = |key: &str| -> Result<Vec<f32>, String> {
        serde_json::from_value(
            value
                .get(key)
                .cloned()
                .ok_or_else(|| format!("missing {key}"))?,
        )
        .map_err(|error| format!("invalid {key}: {error}"))
    };
    let size = |path: &[&str]| -> Result<usize, String> {
        usize::try_from(integer(path)?).map_err(|_| format!("large {}", path.join(".")))
    };
    Ok(MirrorManifest {
        format_version: u32::try_from(integer(&["format_version"])?)
            .map_err(|error| error.to_string())?,
        generation: integer(&["generation"])?,
        max_nominees: size(&["max_nominees"])?,
        geometry: Sq8Geometry {
            rows: size(&["geometry", "rows"])?,
            dimensions: size(&["geometry", "dimensions"])?,
        },
        object_sha256: string("object_sha256")?,
        block_digest_sha256: string("block_digest_sha256")?,
        low: floats("low")?,
        step: floats("step")?,
    })
}

fn route_primary(
    primary: &[usize],
    nominees: &[usize],
    rows: usize,
    dimensions: usize,
) -> Result<(Vec<(usize, u32)>, IntervalPlan), PlanError> {
    let final_rows = rows % 256;
    let final_rows = if final_rows == 0 { 256 } else { final_rows };
    if rows == 0 || dimensions == 0 {
        return Err(PlanError::InvalidGeometry);
    }
    let unit_bytes = dimensions
        .checked_add(12)
        .and_then(|width| width.checked_mul(32))
        .ok_or(PlanError::ArithmeticOverflow)?;
    let primary_set = primary.iter().copied().collect::<HashSet<_>>();
    let nominee_set = nominees.iter().copied().collect::<HashSet<_>>();
    if primary_set.len() != primary.len()
        || nominee_set.len() != nominees.len()
        || !primary_set.is_subset(&nominee_set)
    {
        return Err(PlanError::InvalidWeights);
    }
    let primary_weight = nominees.len().checked_add(1)
        .and_then(|count| u32::try_from(count).ok())
        .ok_or(PlanError::ArithmeticOverflow)?;
    let mut weights = BTreeMap::<usize, u32>::new();
    for &ordinal in nominees {
        if ordinal >= rows {
            return Err(PlanError::InvalidWeights);
        }
        let page_weight = weights.entry(ordinal / 256).or_default();
        *page_weight = page_weight
            .checked_add(if primary_set.contains(&ordinal) {
                primary_weight
            } else {
                1
            })
            .ok_or(PlanError::ArithmeticOverflow)?;
    }
    let votes = weights.into_iter().collect::<Vec<_>>();
    let plan = plan_weighted_intervals(
        normalize_budget_lattice(IntervalGeometry {
            page_count: rows.div_ceil(256),
            full_page_units: 8,
            last_page_bytes: final_rows
                .checked_mul(dimensions + 12)
                .ok_or(PlanError::ArithmeticOverflow)?,
            unit_bytes,
            max_gets: 32,
            max_units: 16_777_216 / unit_bytes,
        })?,
        &votes,
    )?;
    Ok((votes, plan))
}

fn process_request(
    value: &serde_json::Value,
    ram: &ExactSq8Mirror,
    disk: &ExactSq8Mirror,
    rows: usize,
    dimensions: usize,
) -> Result<serde_json::Value, String> {
    let number = |key: &str| -> Result<usize, String> {
        value
            .get(key)
            .and_then(|item| item.as_u64())
            .and_then(|item| usize::try_from(item).ok())
            .ok_or_else(|| format!("invalid {key}"))
    };
    let vector = |key: &str| -> Result<Vec<f32>, String> {
        serde_json::from_value(
            value
                .get(key)
                .cloned()
                .ok_or_else(|| format!("missing {key}"))?,
        )
        .map_err(|error| format!("invalid {key}: {error}"))
    };
    let query_ordinal = number("query_ordinal")?;
    let primary_count = number("primary_count")?;
    let query = vector("query")?;
    let nominees: Vec<usize> =
        serde_json::from_value(value.get("nominees").cloned().ok_or("missing nominees")?)
            .map_err(|error| format!("invalid nominees: {error}"))?;
    if query.len() != dimensions || primary_count == 0 || primary_count > nominees.len() {
        return Err("invalid query geometry".to_owned());
    }
    let ram_scores = ram
        .score(&nominees, &query)
        .map_err(|error| error.to_string())?;
    let file_scores = disk
        .score(&nominees, &query)
        .map_err(|error| error.to_string())?;
    if ram_scores.len() != file_scores.len()
        || ram_scores.iter().zip(&file_scores).any(|(a, b)| {
            a.ordinal != b.ordinal || a.id != b.id || a.score.to_bits() != b.score.to_bits()
        })
    {
        return Err("RAM/file exact SQ8 scores differ".to_owned());
    }
    let ram_primary = primary_ordinals(&ram_scores, primary_count)
        .map_err(|error| format!("invalid RAM primary: {error:?}"))?;
    let file_primary = primary_ordinals(&file_scores, primary_count)
        .map_err(|error| format!("invalid file primary: {error:?}"))?;
    let (votes, plan) = route_primary(&ram_primary, &nominees, rows, dimensions)
        .map_err(|error| error.to_string())?;
    let ranges = plan
        .ranges
        .iter()
        .map(|range| [range.start, range.end])
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "generation": ram.generation(),
        "query_ordinal": query_ordinal,
        "ram_primary": ram_primary,
        "file_primary": file_primary,
        "score_bits": ram_scores.iter().map(|entry| entry.score.to_bits()).collect::<Vec<_>>(),
        "page_votes": votes,
        "ranges": ranges,
        "plan_bytes": plan.bytes,
        "plan_score": plan.score,
    }))
}

fn run_nominate(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() != 7 {
        return Err("usage: borsuk-v114-score-gate nominate ROUTER MANIFEST_SHA REQUESTS REGIONS SHORTLIST".into());
    }
    let router = load_source_router(Path::new(&args[2]), &args[3])?;
    let regions = args[5].parse::<usize>()?;
    let shortlist = args[6].parse::<usize>()?;
    let mut output = BufWriter::new(io::stdout().lock());
    let input = BufReader::new(File::open(&args[4])?);
    for (ordinal, line) in input.lines().enumerate() {
        let request = serde_json::from_str::<serde_json::Value>(&line?)?;
        if request["query_ordinal"] != ordinal {
            return Err("nomination query ordinals are not contiguous".into());
        }
        let query: Vec<f32> = serde_json::from_value(request["query"].clone())?;
        let nominees = router.router.nominate(&query, regions, shortlist)
            .map_err(|error| io::Error::other(format!("nomination failed: {error:?}")))?;
        serde_json::to_writer(&mut output, &serde_json::json!({
            "query_ordinal": ordinal, "nominees": nominees,
        }))?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    Ok(())
}

/// Apply the same native returned-row arithmetic to either physical plan.
fn score_bounded_ranges(
    object: &[u8], geometry: Sq8Geometry, pairs: &[[usize; 2]],
    query: &[f32], low: &[f32], step: &[f32], top_k: usize,
) -> Result<(Vec<i64>, usize), String> {
    if pairs.is_empty() || pairs.len() > 32 {
        return Err("returned plan GET cap differs".to_owned());
    }
    let mut total = 0usize;
    let mut ranges = Vec::with_capacity(pairs.len());
    for &[start, end] in pairs {
        let bytes = object.get(start..end)
            .ok_or("returned plan range exceeds object")?;
        total = total.checked_add(bytes.len())
            .ok_or("returned plan byte count overflows")?;
        if total > 16_777_216 {
            return Err("returned plan byte cap differs".to_owned());
        }
        ranges.push(ReturnedRange { start, bytes });
    }
    let returned = rank_returned_ranges(
        geometry, &ranges, query, low, step, top_k, 16_777_216,
    ).map_err(|error| format!("invalid returned score: {error:?}"))?;
    Ok((returned.iter().map(|entry| entry.id).collect(), total))
}

fn replay_top_k(args: &[String]) -> Result<usize, Box<dyn Error>> {
    if args.len() != 6 && args.len() != 7 {
        return Err("usage: borsuk-v114-score-gate replay-returned MANIFEST OBJECT SIDECAR REQUESTS [TOP_K]".into());
    }
    let top_k = args.get(6).map(|raw| raw.parse::<usize>()).transpose()?.unwrap_or(100);
    if !(100..=1_600).contains(&top_k) {
        return Err("replay top-K must be between 100 and 1600".into());
    }
    Ok(top_k)
}

fn replay_width(requested: usize, available: usize, explicit_wide: bool) -> usize {
    if explicit_wide { requested.min(available) } else { requested }
}

fn run_replay_returned(args: &[String]) -> Result<(), Box<dyn Error>> {
    let top_k = replay_top_k(args)?;
    let explicit_wide = args.len() == 7;
    let manifest = parse_manifest(&serde_json::from_str::<serde_json::Value>(
        &fs::read_to_string(&args[2])?,
    )?)
    .map_err(io::Error::other)?;
    let mirror = ExactSq8Mirror::open(
        Path::new(&args[3]), Path::new(&args[4]),
        manifest.clone(), Placement::Ram,
    )?;
    let object = fs::read(&args[3])?;
    if format!("{:x}", Sha256::digest(&object)) != manifest.object_sha256 {
        return Err("replay SQ8 object changed after mirror opening".into());
    }
    let mut output = BufWriter::new(io::stdout().lock());
    for (ordinal, line) in BufReader::new(File::open(&args[5])?).lines().enumerate() {
        let request = serde_json::from_str::<serde_json::Value>(&line?)?;
        let query_ordinal = request["query_ordinal"].as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .ok_or("invalid replay ordinal")?;
        if query_ordinal != ordinal {
            return Err("replay query ordinal differs".into());
        }
        let query: Vec<f32> = serde_json::from_value(request["query"].clone())?;
        let nominees: Vec<usize> = serde_json::from_value(request["nominees"].clone())?;
        let primary_count = request["primary_count"].as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .ok_or("invalid replay primary count")?;
        if query.len() != manifest.geometry.dimensions
            || primary_count != 100 || nominees.len() != 512
        {
            return Err("replay request geometry differs".into());
        }
        let scored = mirror.score(&nominees, &query)?;
        let primary = primary_ordinals(&scored, primary_count)
            .map_err(|error| io::Error::other(format!("invalid primary: {error:?}")))?;
        let (votes, plan) = route_primary(
            &primary, &nominees, manifest.geometry.rows, manifest.geometry.dimensions,
        )?;
        let plan_pairs = plan.ranges.iter().map(|range| [range.start, range.end])
            .collect::<Vec<_>>();
        let row_bytes = manifest.geometry.dimensions
            .checked_add(12).ok_or("replay row width overflows")?;
        let candidate_rows = plan.bytes / row_bytes;
        let (returned_ids, returned_bytes) = score_bounded_ranges(
            &object, manifest.geometry, &plan_pairs, &query,
            &manifest.low, &manifest.step,
            replay_width(top_k, candidate_rows, explicit_wide),
        ).map_err(io::Error::other)?;
        if returned_bytes != plan.bytes {
            return Err("planned returned bytes differ".into());
        }
        let mut result = serde_json::json!({
            "query_ordinal": ordinal,
            "nominees": nominees,
            "score_bits": scored.iter().map(|entry| entry.score.to_bits()).collect::<Vec<_>>(),
            "primary": primary,
            "page_votes": votes,
            "ranges": plan_pairs,
            "plan_bytes": plan.bytes,
            "plan_score": plan.score,
            "returned_ids": returned_ids,
        });
        if let Some(raw_ranges) = request.get("baseline_ranges") {
            let baseline_pairs: Vec<[usize; 2]> = serde_json::from_value(raw_ranges.clone())?;
            let baseline_rows = baseline_pairs.iter().try_fold(
                0usize, |sum, pair| -> Result<usize, Box<dyn Error>> {
                    let length = pair[1].checked_sub(pair[0])
                        .ok_or("baseline range ends before start")?;
                    if length % row_bytes != 0 {
                        return Err("baseline range is not row aligned".into());
                    }
                    Ok(sum.checked_add(length / row_bytes)
                        .ok_or("baseline row count overflows")?)
                },
            )?;
            let (baseline_ids, baseline_bytes) = score_bounded_ranges(
                &object, manifest.geometry, &baseline_pairs, &query,
                &manifest.low, &manifest.step,
                replay_width(top_k, baseline_rows, explicit_wide),
            ).map_err(io::Error::other)?;
            result["baseline_ranges"] = serde_json::to_value(baseline_pairs)?;
            result["baseline_bytes"] = serde_json::json!(baseline_bytes);
            result["baseline_returned_ids"] = serde_json::json!(baseline_ids);
        }
        serde_json::to_writer(&mut output, &result)?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    Ok(())
}

/// Score three already-sealed physical plans without rerouting or loading GT.
fn run_replay_fixed_ranges(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() != 9 {
        return Err("usage: borsuk-v114-score-gate replay-fixed-ranges MANIFEST OBJECT SIDECAR REQUESTS SEALED V140 TOP_K".into());
    }
    let top_k = args[8].parse::<usize>()?;
    if !(100..=1600).contains(&top_k) {
        return Err("replay top-K must be between 100 and 1600".into());
    }
    let manifest = parse_manifest(&serde_json::from_str::<serde_json::Value>(
        &fs::read_to_string(&args[2])?,
    )?)
    .map_err(io::Error::other)?;
    let _mirror = ExactSq8Mirror::open(
        Path::new(&args[3]), Path::new(&args[4]),
        manifest.clone(), Placement::Ram,
    )?;
    let object = fs::read(&args[3])?;
    if format!("{:x}", Sha256::digest(&object)) != manifest.object_sha256 {
        return Err("replay SQ8 object changed after mirror opening".into());
    }
    let requests = BufReader::new(File::open(&args[5])?);
    let sealed = BufReader::new(File::open(&args[6])?);
    let plans = BufReader::new(File::open(&args[7])?);
    let mut output = BufWriter::new(io::stdout().lock());
    let mut count = 0usize;
    for (ordinal, triple) in requests.lines().zip(sealed.lines()).zip(plans.lines()).enumerate() {
        let ((request, old), plan) = triple;
        let request: serde_json::Value = serde_json::from_str(&request?)?;
        let old: serde_json::Value = serde_json::from_str(&old?)?;
        let plan: serde_json::Value = serde_json::from_str(&plan?)?;
        if [
            request["query_ordinal"].as_u64(),
            old["query_ordinal"].as_u64(),
            plan["query_ordinal"].as_u64(),
        ] != [Some(ordinal as u64); 3] {
            return Err(format!("fixed-range query ordinal differs: {ordinal}").into());
        }
        if request["nominees"] != old["nominees"]
            || request["baseline_ranges"] != old["baseline_ranges"]
        {
            return Err(format!("fixed-range roster differs: {ordinal}").into());
        }
        let query: Vec<f32> = serde_json::from_value(request["query"].clone())?;
        if query.len() != manifest.geometry.dimensions {
            return Err("fixed-range query dimensions differ".into());
        }
        let beta = &plan["variants"]["4"];
        let ranges: [(&str, serde_json::Value); 3] = [
            ("beta4", beta["ranges"].clone()),
            ("broad", old["ranges"].clone()),
            ("control", old["baseline_ranges"].clone()),
        ];
        let mut arms = serde_json::Map::new();
        for (name, raw) in ranges {
            let pairs: Vec<[usize; 2]> = serde_json::from_value(raw)?;
            let started = Instant::now();
            let (ids, bytes) = score_bounded_ranges(
                &object, manifest.geometry, &pairs, &query,
                &manifest.low, &manifest.step, top_k,
            ).map_err(io::Error::other)?;
            let expected = match name {
                "beta4" => beta["planned_bytes"].as_u64(),
                "broad" => old["plan_bytes"].as_u64(),
                _ => old["baseline_bytes"].as_u64(),
            };
            if expected != Some(bytes as u64)
                || (name == "beta4" && beta["gets"].as_u64() != Some(pairs.len() as u64))
            {
                return Err(format!("fixed-range byte charge differs: {name}, {ordinal}").into());
            }
            if name != "beta4" {
                let historical = if name == "broad" {
                    &old["returned_ids"]
                } else {
                    &old["baseline_returned_ids"]
                };
                let historical: Vec<i64> = serde_json::from_value(historical.clone())?;
                if historical.len() != 100
                    || ids.iter().take(100).collect::<HashSet<_>>()
                        != historical.iter().collect::<HashSet<_>>()
                {
                    return Err(format!("V116 fixed-range SQ8 parity differs: {name}, {ordinal}").into());
                }
            }
            arms.insert(name.to_owned(), serde_json::json!({
                "ranges": pairs, "planned_bytes": bytes, "gets": pairs.len(),
                "sq8_top512_ids": ids,
                "sq8_ns": started.elapsed().as_nanos(),
            }));
        }
        serde_json::to_writer(&mut output, &serde_json::json!({
            "query_ordinal": ordinal, "arms": arms,
        }))?;
        output.write_all(b"\n")?;
        count += 1;
    }
    if count != 1000 {
        return Err(format!("fixed-range replay count differs: {count}").into());
    }
    output.flush()?;
    Ok(())
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).is_some_and(|value| value == "nominate") {
        return run_nominate(&args);
    }
    if args.get(1).is_some_and(|value| value == "replay-returned") {
        return run_replay_returned(&args);
    }
    if args.get(1).is_some_and(|value| value == "replay-fixed-ranges") {
        return run_replay_fixed_ranges(&args);
    }
    if args.len() != 5 {
        return Err("usage: v114_exact_local_score MANIFEST OBJECT SIDECAR REQUESTS".into());
    }
    let manifest = parse_manifest(&serde_json::from_str::<serde_json::Value>(
        &fs::read_to_string(&args[1])?,
    )?)
    .map_err(io::Error::other)?;
    let rows = manifest.geometry.rows;
    let dimensions = manifest.geometry.dimensions;
    let ram = ExactSq8Mirror::open(
        Path::new(&args[2]),
        Path::new(&args[3]),
        manifest.clone(),
        Placement::Ram,
    )?;
    let disk = ExactSq8Mirror::open(
        Path::new(&args[2]),
        Path::new(&args[3]),
        manifest,
        Placement::File,
    )?;
    let mut output = BufWriter::new(io::stdout().lock());
    let input = BufReader::new(File::open(&args[4])?);
    for (ordinal, line) in input.lines().enumerate() {
        let request = serde_json::from_str::<serde_json::Value>(&line?)?;
        let result =
            process_request(&request, &ram, &disk, rows, dimensions).map_err(io::Error::other)?;
        if result["query_ordinal"] != ordinal {
            return Err("query ordinals are not contiguous".into());
        }
        serde_json::to_writer(&mut output, &result)?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("V114 exact local score: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::fs;

    #[test]
    fn replay_top_k_defaults_to_production_100_and_bounds_diagnostic_width() {
        let args = vec!["bin", "replay-returned", "manifest", "object", "sidecar",
                        "requests"].into_iter().map(str::to_owned).collect::<Vec<_>>();
        assert_eq!(replay_top_k(&args).unwrap(), 100);
        let mut diagnostic = args.clone();
        diagnostic.push("1600".to_owned());
        assert_eq!(replay_top_k(&diagnostic).unwrap(), 1600);
        diagnostic[6] = "1601".to_owned();
        assert!(replay_top_k(&diagnostic).is_err());
        diagnostic[6] = "99".to_owned();
        assert!(replay_top_k(&diagnostic).is_err());
        assert_eq!(replay_width(100, 99, false), 100);
        assert_eq!(replay_width(1_600, 768, true), 768);
    }

    #[test]
    fn page_votes_and_plan_charge_the_short_final_page_exactly() {
        let (votes, plan) =
            route_primary(&[0, 256, 300], &[0, 1, 256, 257, 300], 416, 768).unwrap();
        assert_eq!(votes, vec![(0, 7), (1, 13)]);
        assert_eq!(plan.ranges, vec![0..324_480]);
        assert_eq!(plan.bytes, 324_480);
    }

    #[test]
    fn route_accepts_a_partial_32_row_final_unit() {
        let (_, plan) = route_primary(&[272], &[272], 273, 768).unwrap();
        assert_eq!(plan.ranges, vec![256 * 780..273 * 780]);
        assert_eq!(plan.bytes, 17 * 780);
    }

    #[test]
    fn rust_planner_tie_prefers_less_physical_io() {
        let plan = plan_weighted_intervals(
            IntervalGeometry {
                page_count: 4,
                full_page_units: 1,
                last_page_bytes: 1,
                unit_bytes: 1,
                max_gets: 1,
                max_units: 2,
            },
            &[(0, 1), (1, 1), (3, 2)],
        )
        .unwrap();
        assert_eq!(plan.score, 2);
        assert_eq!(plan.ranges, vec![3..4]);
    }

    #[test]
    fn manifest_parser_requires_exact_numeric_geometry() {
        let valid = serde_json::json!({
            "format_version": 1, "generation": 7, "max_nominees": 2,
            "geometry": {"rows": 2, "dimensions": 4},
            "object_sha256": "a".repeat(64),
            "block_digest_sha256": "b".repeat(64),
            "low": [0.0, 0.0, 0.0, 0.0],
            "step": [1.0, 1.0, 1.0, 1.0],
        });
        assert_eq!(parse_manifest(&valid).unwrap().geometry.rows, 2);
        let mut invalid = valid;
        invalid["geometry"]["rows"] = serde_json::json!(2.5);
        assert!(parse_manifest(&invalid).is_err());
    }

    #[test]
    fn paired_ranges_use_one_bounded_returned_scorer() {
        let mut object = Vec::new();
        for id in [30i64, 10, 20] {
            object.extend_from_slice(&id.to_le_bytes());
            object.extend_from_slice(&(id as f32).to_le_bytes());
            object.push(0);
        }
        let geometry = Sq8Geometry { rows: 3, dimensions: 1 };
        let scored = score_bounded_ranges(
            &object, geometry, &[[0, 26]], &[0.0], &[0.0], &[1.0], 2,
        ).unwrap();
        assert_eq!(scored.0, vec![10, 30]);
        assert_eq!(scored.1, 26);
        assert!(score_bounded_ranges(
            &object, geometry, &[[0, 27]], &[0.0], &[0.0], &[1.0], 2,
        ).is_err());
    }

    #[test]
    fn request_scores_both_placements_and_routes_one_short_page() {
        let root = std::env::temp_dir().join(format!("v114-cli-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        let object = root.join("sq8.bin");
        let sidecar = root.join("blocks.sha256");
        let mut bytes = Vec::new();
        for id in 0i64..32 {
            bytes.extend_from_slice(&id.to_le_bytes());
            bytes.extend_from_slice(&(id as f32).to_le_bytes());
            bytes.extend_from_slice(&[0u8; 4]);
        }
        fs::write(&object, &bytes).unwrap();
        let block_digest = Sha256::digest(&bytes);
        fs::write(&sidecar, block_digest).unwrap();
        let manifest = MirrorManifest {
            format_version: 1,
            generation: 7,
            max_nominees: 2,
            geometry: Sq8Geometry {
                rows: 32,
                dimensions: 4,
            },
            object_sha256: format!("{:x}", Sha256::digest(&bytes)),
            block_digest_sha256: format!("{:x}", Sha256::digest(block_digest)),
            low: vec![0.0; 4],
            step: vec![1.0; 4],
        };
        let ram = exact_sq8_mirror::ExactSq8Mirror::open(
            &object,
            &sidecar,
            manifest.clone(),
            exact_sq8_mirror::Placement::Ram,
        )
        .unwrap();
        let disk = exact_sq8_mirror::ExactSq8Mirror::open(
            &object,
            &sidecar,
            manifest,
            exact_sq8_mirror::Placement::File,
        )
        .unwrap();
        let request = serde_json::json!({
            "query_ordinal": 0, "query": [0.0, 0.0, 0.0, 0.0],
            "nominees": [7, 2], "primary_count": 1,
        });
        let result = process_request(&request, &ram, &disk, 32, 4).unwrap();
        assert_eq!(result["ram_primary"], serde_json::json!([2]));
        assert_eq!(result["generation"], serde_json::json!(7));
        assert_eq!(result["file_primary"], serde_json::json!([2]));
        assert_eq!(
            result["score_bits"],
            serde_json::json!([1088421888, 1073741824])
        );
        assert_eq!(result["page_votes"], serde_json::json!([[0, 4]]));
        assert_eq!(result["ranges"], serde_json::json!([[0, 512]]));
        fs::remove_dir_all(root).unwrap();
    }
}
