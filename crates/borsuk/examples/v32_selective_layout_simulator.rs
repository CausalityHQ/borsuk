//! Metadata-only V32 selective object-layout simulator.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_LEAF_ROWS: u32 = 1_024;
const MAX_SELECTED_LEAVES: usize = 256;
const MAX_OBJECT_ROWS: u32 = 8_192;
const MAX_OBJECT_PARENTS: usize = 32;
const MAX_OBJECT_RANGES: usize = 128;
const MAX_OBJECT_BYTES: u64 = 524_288;
const OBJECT_FIXED_BYTES: u64 = 4_096;
const PARENT_BYTES: u64 = 208;
const RANGE_BYTES: u64 = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V32LayoutArm {
    leaves_per_object: u8,
    wave_width: u16,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V32LayoutLeaf {
    high_rows: u32,
    parent: u32,
    root: u32,
    rows: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V32LayoutQuery {
    selected_leaves: Vec<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V32LayoutSimulationInput {
    dataset: String,
    layout_sha256: String,
    leaves: Vec<V32LayoutLeaf>,
    queries: Vec<V32LayoutQuery>,
    query_cohort_sha256: String,
    schema: String,
    source_archive_sha256: String,
    source_commit: String,
}

#[derive(Debug, Clone, Serialize)]
struct V32LayoutArmResult {
    admission_eligible: bool,
    arm: V32LayoutArm,
    leaf_rows_max: u32,
    leaf_rows_min: u32,
    maximum_byte_amplification_ppm: u64,
    maximum_concurrent_bytes: u64,
    maximum_fetched_bytes: u64,
    maximum_objects: u32,
    maximum_selected_bytes: u64,
    maximum_selected_rows: u64,
    maximum_waves: u32,
}

#[derive(Debug, Clone)]
struct V32LayoutSimulationResult {
    arms: Vec<V32LayoutArmResult>,
    leaf_row_histogram: [u64; 6],
    nondominated: BTreeSet<V32LayoutArm>,
}

#[derive(Serialize)]
struct V32LayoutSimulationReceipt<'a> {
    arms: &'a [V32LayoutArmResult],
    dataset: &'a str,
    input_sha256: &'a str,
    leaf_row_histogram: [u64; 6],
    layout_sha256: &'a str,
    nondominated: &'a BTreeSet<V32LayoutArm>,
    query_cohort_sha256: &'a str,
    schema: &'static str,
    source_archive_sha256: &'a str,
    source_commit: &'a str,
}

#[derive(Debug, Clone)]
struct ProjectedObject {
    first_leaf: usize,
    leaf_count: usize,
    bytes: u64,
}

fn leaf_payload_bytes(leaf: V32LayoutLeaf) -> Result<u64, String> {
    let bitmap_words = u64::from(leaf.rows).div_ceil(32);
    let base_rows = leaf
        .rows
        .checked_sub(leaf.high_rows)
        .ok_or_else(|| "high-row count exceeds leaf rows".to_owned())?;
    RANGE_BYTES
        .checked_add(bitmap_words.checked_mul(4).ok_or("bitmap bytes overflow")?)
        .and_then(|value| value.checked_add(u64::from(base_rows) * 24))
        .and_then(|value| value.checked_add(u64::from(leaf.high_rows) * 48))
        .ok_or_else(|| "leaf bytes overflow".to_owned())
}

fn projected_object_bytes(leaves: &[V32LayoutLeaf]) -> Result<u64, String> {
    let parents = leaves
        .iter()
        .map(|leaf| leaf.parent)
        .collect::<BTreeSet<_>>();
    leaves.iter().try_fold(
        OBJECT_FIXED_BYTES
            + u64::try_from(parents.len()).map_err(|_| "parent count overflow")? * PARENT_BYTES,
        |total, leaf| {
            total
                .checked_add(leaf_payload_bytes(*leaf)?)
                .ok_or_else(|| "object bytes overflow".to_owned())
        },
    )
}

fn pack_objects(
    leaves: &[V32LayoutLeaf],
    leaves_per_object: usize,
) -> Result<Vec<ProjectedObject>, String> {
    let mut objects = Vec::new();
    let mut first = 0;
    while first < leaves.len() {
        let root = leaves[first].root;
        let mut end = first;
        while end < leaves.len() && end - first < leaves_per_object && leaves[end].root == root {
            let candidate = &leaves[first..=end];
            let rows = candidate.iter().try_fold(0_u32, |total, leaf| {
                total.checked_add(leaf.rows).ok_or("object rows overflow")
            })?;
            let parents = candidate
                .iter()
                .map(|leaf| leaf.parent)
                .collect::<BTreeSet<_>>()
                .len();
            let bytes = projected_object_bytes(candidate)?;
            if rows > MAX_OBJECT_ROWS
                || parents > MAX_OBJECT_PARENTS
                || candidate.len() > MAX_OBJECT_RANGES
                || bytes > MAX_OBJECT_BYTES
            {
                break;
            }
            end += 1;
        }
        if end == first {
            return Err("one routing leaf exceeds an object bound".to_owned());
        }
        objects.push(ProjectedObject {
            first_leaf: first,
            leaf_count: end - first,
            bytes: projected_object_bytes(&leaves[first..end])?,
        });
        first = end;
    }
    Ok(objects)
}

fn simulate_arm(
    input: &V32LayoutSimulationInput,
    arm: V32LayoutArm,
) -> Result<V32LayoutArmResult, String> {
    let objects = pack_objects(&input.leaves, usize::from(arm.leaves_per_object))?;
    let mut largest_object_bytes = objects
        .iter()
        .map(|object| object.bytes)
        .collect::<Vec<_>>();
    largest_object_bytes.sort_unstable_by(|left, right| right.cmp(left));
    let largest_256_bytes = largest_object_bytes
        .into_iter()
        .take(MAX_SELECTED_LEAVES)
        .try_fold(0_u64, |total, bytes| {
            total.checked_add(bytes).ok_or("admission bytes overflow")
        })?;
    let mut maximum_objects = 0_u32;
    let mut maximum_waves = 0_u32;
    let mut maximum_fetched_bytes = 0_u64;
    let mut maximum_selected_bytes = 0_u64;
    let mut maximum_byte_amplification_ppm = 0_u64;
    let mut maximum_concurrent_bytes = 0_u64;
    let mut maximum_selected_rows = 0_u64;

    for query in &input.queries {
        let selected = query
            .selected_leaves
            .iter()
            .map(|leaf| *leaf as usize)
            .collect::<BTreeSet<_>>();
        if selected.is_empty()
            || selected.len() != query.selected_leaves.len()
            || selected.len() > MAX_SELECTED_LEAVES
            || selected.iter().any(|leaf| *leaf >= input.leaves.len())
        {
            return Err("selected leaf authority differs".to_owned());
        }
        let selected_rows = selected.iter().try_fold(0_u64, |total, leaf| {
            total
                .checked_add(u64::from(input.leaves[*leaf].rows))
                .ok_or("selected rows overflow")
        })?;
        if selected_rows > 262_144 {
            return Err("selected row bound differs".to_owned());
        }
        let selected_bytes = selected.iter().try_fold(0_u64, |total, leaf| {
            total
                .checked_add(leaf_payload_bytes(input.leaves[*leaf])?)
                .ok_or_else(|| "selected bytes overflow".to_owned())
        })?;
        let fetched = objects
            .iter()
            .filter(|object| {
                (object.first_leaf..object.first_leaf + object.leaf_count)
                    .any(|leaf| selected.contains(&leaf))
            })
            .collect::<Vec<_>>();
        let fetched_bytes = fetched.iter().try_fold(0_u64, |total, object| {
            total
                .checked_add(object.bytes)
                .ok_or("fetched bytes overflow")
        })?;
        let waves = fetched.len().div_ceil(usize::from(arm.wave_width));
        let concurrent = fetched
            .chunks(usize::from(arm.wave_width))
            .map(|wave| wave.iter().map(|object| object.bytes).sum::<u64>())
            .max()
            .unwrap_or(0);
        let amplification = fetched_bytes
            .checked_mul(1_000_000)
            .ok_or("amplification overflow")?
            .div_ceil(selected_bytes.max(1));
        maximum_objects =
            maximum_objects.max(u32::try_from(fetched.len()).map_err(|_| "object count overflow")?);
        maximum_waves = maximum_waves.max(u32::try_from(waves).map_err(|_| "wave count overflow")?);
        maximum_fetched_bytes = maximum_fetched_bytes.max(fetched_bytes);
        maximum_selected_bytes = maximum_selected_bytes.max(selected_bytes);
        maximum_byte_amplification_ppm = maximum_byte_amplification_ppm.max(amplification);
        maximum_concurrent_bytes = maximum_concurrent_bytes.max(concurrent);
        maximum_selected_rows = maximum_selected_rows.max(selected_rows);
    }

    let admission_eligible = maximum_objects <= 256
        && maximum_fetched_bytes <= 64 * 1024 * 1024
        && maximum_concurrent_bytes <= 64 * 1024 * 1024
        && maximum_selected_rows <= 262_144
        && largest_256_bytes <= 64 * 1024 * 1024;
    Ok(V32LayoutArmResult {
        admission_eligible,
        arm,
        maximum_objects,
        maximum_waves,
        maximum_fetched_bytes,
        maximum_selected_bytes,
        maximum_byte_amplification_ppm,
        maximum_concurrent_bytes,
        maximum_selected_rows,
        leaf_rows_min: input.leaves.iter().map(|leaf| leaf.rows).min().unwrap_or(0),
        leaf_rows_max: input.leaves.iter().map(|leaf| leaf.rows).max().unwrap_or(0),
    })
}

fn simulate_v32_selective_layouts(
    input: &V32LayoutSimulationInput,
) -> Result<V32LayoutSimulationResult, String> {
    if input.leaves.is_empty() || input.queries.is_empty() {
        return Err("layout simulation input is empty".to_owned());
    }
    for (index, leaf) in input.leaves.iter().enumerate() {
        if leaf.rows == 0 || leaf.rows > MAX_LEAF_ROWS || leaf.high_rows > leaf.rows {
            return Err("routing leaf shape differs".to_owned());
        }
        if index > 0 {
            let previous = input.leaves[index - 1];
            if leaf.root < previous.root
                || leaf.parent < previous.parent
                || (leaf.parent == previous.parent && leaf.root != previous.root)
            {
                return Err("routing leaf order differs".to_owned());
            }
        }
    }
    let mut leaf_row_histogram = [0_u64; 6];
    for leaf in &input.leaves {
        let bucket = match leaf.rows {
            1..=32 => 0,
            33..=64 => 1,
            65..=128 => 2,
            129..=256 => 3,
            257..=512 => 4,
            513..=1_024 => 5,
            _ => return Err("routing leaf histogram differs".to_owned()),
        };
        leaf_row_histogram[bucket] = leaf_row_histogram[bucket]
            .checked_add(1)
            .ok_or_else(|| "routing leaf histogram overflows".to_owned())?;
    }
    let mut arms = Vec::with_capacity(15);
    for leaves_per_object in [1, 2, 4] {
        for wave_width in [16, 32, 64, 128, 256] {
            arms.push(simulate_arm(
                input,
                V32LayoutArm {
                    leaves_per_object,
                    wave_width,
                },
            )?);
        }
    }
    let nondominated = arms
        .iter()
        .filter(|candidate| candidate.admission_eligible)
        .filter(|candidate| {
            !arms.iter().any(|other| {
                other.admission_eligible
                    && other.maximum_objects <= candidate.maximum_objects
                    && other.maximum_waves <= candidate.maximum_waves
                    && other.maximum_fetched_bytes <= candidate.maximum_fetched_bytes
                    && other.maximum_concurrent_bytes <= candidate.maximum_concurrent_bytes
                    && (other.maximum_objects < candidate.maximum_objects
                        || other.maximum_waves < candidate.maximum_waves
                        || other.maximum_fetched_bytes < candidate.maximum_fetched_bytes
                        || other.maximum_concurrent_bytes < candidate.maximum_concurrent_bytes)
            })
        })
        .map(|result| result.arm)
        .collect();
    Ok(V32LayoutSimulationResult {
        arms,
        leaf_row_histogram,
        nondominated,
    })
}

fn lowercase_hex(value: &str, bytes: usize) -> bool {
    value.len() == bytes * 2
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn canonical_v32_layout_simulation_receipt(
    input_bytes: &[u8],
    expected_sha256: &str,
) -> Result<Vec<u8>, String> {
    if !lowercase_hex(expected_sha256, 32) || sha256(input_bytes) != expected_sha256 {
        return Err("layout simulation input digest differs".to_owned());
    }
    let input: V32LayoutSimulationInput =
        serde_json::from_slice(input_bytes).map_err(|error| error.to_string())?;
    let mut canonical_input = serde_json::to_vec(&input).map_err(|error| error.to_string())?;
    canonical_input.push(b'\n');
    if canonical_input != input_bytes {
        return Err("layout simulation input is not canonical".to_owned());
    }
    if input.schema != "borsuk-v32-selective-layout-simulation-input-v1"
        || !lowercase_hex(&input.layout_sha256, 32)
        || !lowercase_hex(&input.query_cohort_sha256, 32)
        || !lowercase_hex(&input.source_archive_sha256, 32)
        || !lowercase_hex(&input.source_commit, 20)
        || input.dataset.is_empty()
    {
        return Err("layout simulation input authority differs".to_owned());
    }
    let result = simulate_v32_selective_layouts(&input)?;
    let receipt = V32LayoutSimulationReceipt {
        arms: &result.arms,
        dataset: &input.dataset,
        input_sha256: expected_sha256,
        leaf_row_histogram: result.leaf_row_histogram,
        layout_sha256: &input.layout_sha256,
        nondominated: &result.nondominated,
        query_cohort_sha256: &input.query_cohort_sha256,
        schema: "borsuk-v32-selective-layout-simulation-receipt-v1",
        source_archive_sha256: &input.source_archive_sha256,
        source_commit: &input.source_commit,
    };
    let mut bytes = serde_json::to_vec(&receipt).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn run_cli(args: Vec<String>) -> Result<Vec<u8>, String> {
    let mut args = args.into_iter();
    let _program = args
        .next()
        .ok_or_else(|| "program name is missing".to_owned())?;
    let mut input = None::<PathBuf>;
    let mut input_sha256 = None::<String>;
    let mut execute = false;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--input" => {
                if input.is_some() {
                    return Err("duplicate --input".to_owned());
                }
                input = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| "--input value is missing".to_owned())?,
                ));
            }
            "--input-sha256" => {
                if input_sha256.is_some() {
                    return Err("duplicate --input-sha256".to_owned());
                }
                input_sha256 = Some(
                    args.next()
                        .ok_or_else(|| "--input-sha256 value is missing".to_owned())?,
                );
            }
            "--execute-layout-simulation" => {
                if execute {
                    return Err("duplicate --execute-layout-simulation".to_owned());
                }
                execute = true;
            }
            _ => return Err(format!("unknown argument `{flag}`")),
        }
    }
    if !execute {
        return Err("--execute-layout-simulation is required".to_owned());
    }
    let input = input.ok_or_else(|| "--input is required".to_owned())?;
    let expected_sha256 = input_sha256.ok_or_else(|| "--input-sha256 is required".to_owned())?;
    let length = std::fs::metadata(&input)
        .map_err(|error| error.to_string())?
        .len();
    if length == 0 || length > 128 * 1024 * 1024 {
        return Err("layout simulation input length differs".to_owned());
    }
    let bytes = std::fs::read(input).map_err(|error| error.to_string())?;
    canonical_v32_layout_simulation_receipt(&bytes, &expected_sha256)
}

fn main() {
    match run_cli(std::env::args().collect()) {
        Ok(receipt) => {
            if let Err(error) = std::io::stdout().write_all(&receipt) {
                eprintln!("{error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        V32LayoutArm, V32LayoutLeaf, V32LayoutQuery, V32LayoutSimulationInput,
        canonical_v32_layout_simulation_receipt, leaf_payload_bytes, run_cli,
        simulate_v32_selective_layouts,
    };
    use sha2::{Digest, Sha256};

    fn sha256(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn coherent_input() -> V32LayoutSimulationInput {
        V32LayoutSimulationInput {
            dataset: "synthetic-96".to_owned(),
            layout_sha256: "1".repeat(64),
            leaves: vec![
                V32LayoutLeaf {
                    root: 0,
                    parent: 0,
                    rows: 100,
                    high_rows: 0,
                },
                V32LayoutLeaf {
                    root: 0,
                    parent: 0,
                    rows: 200,
                    high_rows: 100,
                },
                V32LayoutLeaf {
                    root: 0,
                    parent: 1,
                    rows: 300,
                    high_rows: 300,
                },
                V32LayoutLeaf {
                    root: 0,
                    parent: 1,
                    rows: 400,
                    high_rows: 0,
                },
                V32LayoutLeaf {
                    root: 1,
                    parent: 2,
                    rows: 500,
                    high_rows: 250,
                },
                V32LayoutLeaf {
                    root: 1,
                    parent: 2,
                    rows: 600,
                    high_rows: 600,
                },
                V32LayoutLeaf {
                    root: 1,
                    parent: 3,
                    rows: 700,
                    high_rows: 0,
                },
                V32LayoutLeaf {
                    root: 1,
                    parent: 3,
                    rows: 800,
                    high_rows: 400,
                },
            ],
            queries: vec![
                V32LayoutQuery {
                    selected_leaves: vec![0, 2, 4, 6],
                },
                V32LayoutQuery {
                    selected_leaves: vec![1, 3, 5, 7],
                },
            ],
            query_cohort_sha256: "2".repeat(64),
            schema: "borsuk-v32-selective-layout-simulation-input-v1".to_owned(),
            source_archive_sha256: "3".repeat(64),
            source_commit: "4".repeat(40),
        }
    }

    #[test]
    fn v32_selective_layout_simulator_compares_complete_preregistered_ladder() {
        assert_eq!(
            leaf_payload_bytes(V32LayoutLeaf {
                high_rows: 100,
                parent: 0,
                root: 0,
                rows: 100,
            })
            .unwrap(),
            4_848
        );
        let result = simulate_v32_selective_layouts(&coherent_input()).unwrap();
        assert_eq!(result.arms.len(), 15);
        assert!(result.arms.iter().all(|arm| arm.admission_eligible));
        assert_eq!(result.leaf_row_histogram, [0, 0, 1, 1, 3, 3]);

        let one_16 = result
            .arms
            .iter()
            .find(|arm| {
                arm.arm
                    == V32LayoutArm {
                        leaves_per_object: 1,
                        wave_width: 16,
                    }
            })
            .unwrap();
        assert_eq!(one_16.maximum_objects, 4);
        assert_eq!(one_16.maximum_waves, 1);
        assert_eq!(one_16.maximum_selected_rows, 2_000);
        assert!(one_16.maximum_selected_bytes > 0);
        assert_eq!(one_16.leaf_rows_min, 100);
        assert_eq!(one_16.leaf_rows_max, 800);

        let four_16 = result
            .arms
            .iter()
            .find(|arm| {
                arm.arm
                    == V32LayoutArm {
                        leaves_per_object: 4,
                        wave_width: 16,
                    }
            })
            .unwrap();
        assert_eq!(four_16.maximum_objects, 2);
        assert_eq!(four_16.maximum_waves, 1);
        assert!(four_16.maximum_fetched_bytes > one_16.maximum_fetched_bytes);
        assert!(four_16.maximum_byte_amplification_ppm > 1_000_000);

        assert!(result.nondominated.contains(&V32LayoutArm {
            leaves_per_object: 1,
            wave_width: 16,
        }));
        assert!(result.nondominated.contains(&V32LayoutArm {
            leaves_per_object: 4,
            wave_width: 256,
        }));
    }

    #[test]
    fn v32_selective_layout_simulator_rejects_authority_and_bound_drift() {
        let mut cases = Vec::new();

        let mut empty_leaf = coherent_input();
        empty_leaf.leaves[0].rows = 0;
        cases.push(empty_leaf);

        let mut oversized_leaf = coherent_input();
        oversized_leaf.leaves[0].rows = 1_025;
        cases.push(oversized_leaf);

        let mut impossible_high = coherent_input();
        impossible_high.leaves[0].high_rows = 101;
        cases.push(impossible_high);

        let mut root_reversal = coherent_input();
        root_reversal.leaves[4].root = 0;
        root_reversal.leaves[3].root = 1;
        cases.push(root_reversal);

        let mut duplicate = coherent_input();
        duplicate.queries[0].selected_leaves = vec![0, 2, 2];
        cases.push(duplicate);

        let mut empty_selection = coherent_input();
        empty_selection.queries[0].selected_leaves.clear();
        cases.push(empty_selection);

        let mut parent_crosses_roots = coherent_input();
        parent_crosses_roots.leaves[1].root = 1;
        parent_crosses_roots.leaves[2].root = 1;
        parent_crosses_roots.leaves[3].root = 1;
        cases.push(parent_crosses_roots);

        let mut unknown = coherent_input();
        unknown.queries[0].selected_leaves = vec![0, 8];
        cases.push(unknown);

        let mut too_many = coherent_input();
        too_many.leaves = (0..257)
            .map(|parent| V32LayoutLeaf {
                root: 0,
                parent,
                rows: 1_024,
                high_rows: 1_024,
            })
            .collect();
        too_many.queries = vec![V32LayoutQuery {
            selected_leaves: (0..257).collect(),
        }];
        cases.push(too_many);

        for invalid in cases {
            assert!(simulate_v32_selective_layouts(&invalid).is_err());
        }
    }

    #[test]
    fn v32_selective_layout_simulator_authenticates_canonical_input_and_output() {
        let input = concat!(
            "{\"dataset\":\"deep-image-96\",",
            "\"layout_sha256\":\"1111111111111111111111111111111111111111111111111111111111111111\",",
            "\"leaves\":[{\"high_rows\":0,\"parent\":0,\"root\":0,\"rows\":100}],",
            "\"queries\":[{\"selected_leaves\":[0]}],",
            "\"query_cohort_sha256\":\"2222222222222222222222222222222222222222222222222222222222222222\",",
            "\"schema\":\"borsuk-v32-selective-layout-simulation-input-v1\",",
            "\"source_archive_sha256\":\"3333333333333333333333333333333333333333333333333333333333333333\",",
            "\"source_commit\":\"4444444444444444444444444444444444444444\"}\n",
        );
        let digest = sha256(input.as_bytes());
        let first = canonical_v32_layout_simulation_receipt(input.as_bytes(), &digest).unwrap();
        let second = canonical_v32_layout_simulation_receipt(input.as_bytes(), &digest).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.last(), Some(&b'\n'));
        let value: serde_json::Value = serde_json::from_slice(&first).unwrap();
        assert_eq!(
            value["schema"],
            "borsuk-v32-selective-layout-simulation-receipt-v1"
        );
        assert_eq!(value["input_sha256"], digest);
        assert_eq!(
            value["source_commit"],
            "4444444444444444444444444444444444444444"
        );
        assert_eq!(value["arms"].as_array().unwrap().len(), 15);
        assert_eq!(value["nondominated"].as_array().unwrap().len(), 15);

        assert!(
            canonical_v32_layout_simulation_receipt(input.as_bytes(), &"0".repeat(64)).is_err()
        );
        let mut noncanonical = input.as_bytes().to_vec();
        noncanonical.insert(1, b' ');
        let noncanonical_digest = sha256(&noncanonical);
        assert!(
            canonical_v32_layout_simulation_receipt(&noncanonical, &noncanonical_digest).is_err()
        );
    }

    #[test]
    fn v32_selective_layout_simulator_cli_requires_explicit_authenticated_execution() {
        let input = concat!(
            "{\"dataset\":\"deep-image-96\",",
            "\"layout_sha256\":\"1111111111111111111111111111111111111111111111111111111111111111\",",
            "\"leaves\":[{\"high_rows\":0,\"parent\":0,\"root\":0,\"rows\":100}],",
            "\"queries\":[{\"selected_leaves\":[0]}],",
            "\"query_cohort_sha256\":\"2222222222222222222222222222222222222222222222222222222222222222\",",
            "\"schema\":\"borsuk-v32-selective-layout-simulation-input-v1\",",
            "\"source_archive_sha256\":\"3333333333333333333333333333333333333333333333333333333333333333\",",
            "\"source_commit\":\"4444444444444444444444444444444444444444\"}\n",
        );
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("input.json");
        std::fs::write(&path, input).unwrap();
        let digest = sha256(input.as_bytes());
        let args = vec![
            "v32_selective_layout_simulator".to_owned(),
            "--input".to_owned(),
            path.display().to_string(),
            "--input-sha256".to_owned(),
            digest,
            "--execute-layout-simulation".to_owned(),
        ];
        let receipt = run_cli(args.clone()).unwrap();
        assert!(receipt.ends_with(b"\n"));

        let mut missing_execute = args.clone();
        missing_execute.pop();
        assert!(run_cli(missing_execute).is_err());
        let mut duplicate = args.clone();
        duplicate.extend(["--input".to_owned(), path.display().to_string()]);
        assert!(run_cli(duplicate).is_err());
        let mut storage_flag = args;
        storage_flag.push("--s3-bucket".to_owned());
        assert!(run_cli(storage_flag).is_err());
    }
}
