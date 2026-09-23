//! V114 research gate adapter around the production exact SQ8 mirror.

#[path = "../../../crates/borsuk/src/exact_sq8_mirror.rs"]
mod exact_sq8_mirror;
#[path = "../../../crates/borsuk/src/exact_sq8_nominee.rs"]
mod exact_sq8_nominee;
#[path = "../../../crates/borsuk/src/physical_interval.rs"]
mod physical_interval;

use exact_sq8_mirror::{ExactSq8Mirror, MirrorManifest, Placement};
use exact_sq8_nominee::{Sq8Geometry, primary_ordinals};
use physical_interval::{IntervalGeometry, IntervalPlan, PlanError, plan_weighted_intervals};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;

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
    rows: usize,
    dimensions: usize,
) -> Result<(Vec<(usize, u32)>, IntervalPlan), PlanError> {
    let final_rows = rows % 256;
    let final_rows = if final_rows == 0 { 256 } else { final_rows };
    if rows == 0 || dimensions == 0 || final_rows % 32 != 0 {
        return Err(PlanError::InvalidGeometry);
    }
    let unit_bytes = dimensions
        .checked_add(12)
        .and_then(|width| width.checked_mul(32))
        .ok_or(PlanError::ArithmeticOverflow)?;
    let mut weights = BTreeMap::<usize, u32>::new();
    for &ordinal in primary {
        if ordinal >= rows {
            return Err(PlanError::InvalidWeights);
        }
        *weights.entry(ordinal / 256).or_default() += 1;
    }
    let votes = weights.into_iter().collect::<Vec<_>>();
    let plan = plan_weighted_intervals(
        IntervalGeometry {
            page_count: rows.div_ceil(256),
            full_page_units: 8,
            last_page_units: final_rows / 32,
            unit_bytes,
            max_gets: 32,
            max_units: 16_777_216 / unit_bytes,
        },
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
    let (votes, plan) =
        route_primary(&ram_primary, rows, dimensions).map_err(|error| error.to_string())?;
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

fn run() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
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
    fn page_votes_and_plan_charge_the_short_final_page_exactly() {
        let (votes, plan) = route_primary(&[0, 256, 300], 416, 768).unwrap();
        assert_eq!(votes, vec![(0, 1), (1, 2)]);
        assert_eq!(plan.ranges, vec![0..324_480]);
        assert_eq!(plan.bytes, 324_480);
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
        assert_eq!(result["page_votes"], serde_json::json!([[0, 1]]));
        assert_eq!(result["ranges"], serde_json::json!([[0, 512]]));
        fs::remove_dir_all(root).unwrap();
    }
}
