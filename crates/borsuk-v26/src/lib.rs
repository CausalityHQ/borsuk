//! Fail-fast contracts for the prerelease BORSUK V26 page layout.

#![allow(
    missing_docs,
    reason = "unpublished internal prerelease contract crate; not a compatibility surface"
)]

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

mod local;
mod tree;

pub use local::{
    V26LayoutBuildOutput, V26LayoutBuildRequest, V26LayoutEvaluationRequest, V26LocalObjectPath,
    evaluate_v26_layout_oracle, run_v26_layout_build, v26_construction_schema,
    v26_page_assignments_schema, v26_query_schema, v26_source_map_schema, v26_tree_schema,
    v26_truth_schema, validate_v26_layout_build_output,
};

pub use tree::{
    V26ConstructionRow, V26Node, V26RowPages, V26Tree, build_v26_dual_tree_layout,
    validate_v26_dual_tree_layout,
};

const V26_LAYOUT_SCHEMA: &str = "borsuk-v26-dual-tree-layout-v1";
const V26_PRIMARY_SEED: u64 = 0x5632_362d_5452_4545;
const V26_REPLICA_SEED: u64 = 0x5632_362d_5245_504c;
const V26_PAGE_CAPACITY: u32 = 704;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Error(String);

impl std::fmt::Display for V26Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for V26Error {}

pub type Result<T> = std::result::Result<T, V26Error>;

fn invalid(message: &str) -> V26Error {
    V26Error(message.to_owned())
}

pub fn exact_v26_layout_oracle_pages(
    assignments: &[Vec<u32>],
    page_budget: usize,
) -> Result<Vec<u32>> {
    if assignments.len() != 10
        || page_budget != 8
        || assignments.iter().any(|pages| {
            pages.is_empty() || pages.len() > 2 || pages.windows(2).any(|pair| pair[0] >= pair[1])
        })
    {
        return Err(invalid("V26 truth assignments differ"));
    }
    let mut page_masks = BTreeMap::<u32, u16>::new();
    for (neighbor, pages) in assignments.iter().enumerate() {
        for page in pages {
            *page_masks.entry(*page).or_default() |= 1_u16 << neighbor;
        }
    }
    let maximum_pages = page_budget.min(page_masks.len());
    let mut states = vec![None::<([u32; 8], usize)>; 1 << assignments.len()];
    states[0] = Some(([0; 8], 0));
    for (page, mask) in page_masks {
        for covered in (0..states.len()).rev() {
            let Some((mut pages, count)) = states[covered] else {
                continue;
            };
            if count == maximum_pages {
                continue;
            }
            let combined = covered | usize::from(mask);
            pages[count] = page;
            let next_count = count + 1;
            if states[combined]
                .as_ref()
                .is_none_or(|(prior, prior_count)| {
                    next_count < *prior_count
                        || (next_count == *prior_count
                            && pages[..next_count] < prior[..*prior_count])
                })
            {
                states[combined] = Some((pages, next_count));
            }
        }
    }
    states
        .into_iter()
        .enumerate()
        .filter_map(|(mask, pages)| pages.map(|pages| (mask.count_ones(), pages)))
        .max_by(
            |(left_hits, (left_pages, left_count)), (right_hits, (right_pages, right_count))| {
                left_hits
                    .cmp(right_hits)
                    .then_with(|| right_count.cmp(left_count))
                    .then_with(|| right_pages[..*right_count].cmp(&left_pages[..*left_count]))
            },
        )
        .map(|(_, (pages, count))| pages[..count].to_vec())
        .filter(|pages| !pages.is_empty())
        .ok_or_else(|| invalid("V26 layout oracle differs"))
}

fn v26_layout_hits(assignments: &[Vec<u32>], selected_pages: &[u32]) -> u32 {
    assignments
        .iter()
        .filter(|pages| {
            pages
                .iter()
                .any(|page| selected_pages.binary_search(page).is_ok())
        })
        .count() as u32
}

fn v26_ppm(numerator: u64, denominator: u64) -> Result<u64> {
    numerator
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_div(denominator))
        .ok_or_else(|| invalid("V26 metric arithmetic differs"))
}

fn exact_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ObjectIdentity {
    pub role: String,
    pub uri: String,
    pub digest_algorithm: String,
    pub digest: String,
    pub encoded_bytes: u64,
    pub generation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutAuthority {
    pub schema: String,
    pub generation: String,
    pub source_commit: String,
    pub source_archive_sha256: String,
    pub construction_rows: V26ObjectIdentity,
    pub source_map: V26ObjectIdentity,
    pub primary_seed: u64,
    pub replica_seed: u64,
    pub page_capacity: u32,
    pub expected_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutReceipt {
    pub authority: V26LayoutAuthority,
    pub inputs: Vec<V26ObjectIdentity>,
    pub outputs: Vec<V26ObjectIdentity>,
    pub row_count: u64,
    pub leaves_per_tree: u32,
    pub page_count: u32,
    pub projection_steps: u64,
    pub worker_count: u32,
    pub elapsed_ns: u64,
    pub cpu_ns: u64,
    pub peak_rss_bytes: u64,
    pub peak_psi_full_avg10_milli_percent: u64,
    pub swap_start_bytes: u64,
    pub swap_end_bytes: u64,
    pub query_role_opens: u64,
    pub page_body_reads: u64,
    pub claim_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26QueryTruth {
    pub query_ordinal: u32,
    pub neighbor_source_ordinals: Vec<u64>,
    pub ground_truth_page_assignments: Vec<Vec<u32>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutSample {
    pub query_ordinal: u32,
    pub selected_pages: Vec<u32>,
    pub hits: u32,
    pub recall_ppm: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum V26Disposition {
    AuthorityStop,
    LayoutRejected,
    RankReducerRejected,
    TreeRouterRejected,
    BoundedLayoutCandidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutResult {
    pub schema: String,
    pub query_count: u32,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub disposition: V26Disposition,
    pub page_body_reads: u64,
    pub claim_eligible: bool,
}

pub(crate) fn projected_steps(rows: u64, leaves: u64) -> Result<u64> {
    if leaves <= 1 {
        return Ok(0);
    }
    let left_leaves = leaves / 2;
    let right_leaves = leaves - left_leaves;
    let left_rows = (rows - right_leaves).min(
        left_leaves
            .checked_mul(u64::from(V26_PAGE_CAPACITY))
            .ok_or_else(|| invalid("V26 projection work overflows"))?,
    );
    let right_rows = rows - left_rows;
    let own = rows
        .checked_mul(16 * 96)
        .ok_or_else(|| invalid("V26 projection work overflows"))?;
    let left = projected_steps(left_rows, left_leaves)?;
    let right = projected_steps(right_rows, right_leaves)?;
    own.checked_add(left)
        .and_then(|partial| partial.checked_add(right))
        .ok_or_else(|| invalid("V26 projection work overflows"))
}

fn validate_identity(
    identity: &V26ObjectIdentity,
    expected_role: &str,
    generation: &str,
) -> Result<()> {
    if identity.role != expected_role
        || identity.generation != generation
        || identity.digest_algorithm != "sha256"
        || !exact_lower_hex(&identity.digest, 64)
        || identity.encoded_bytes == 0
        || !identity.uri.starts_with("s3://")
    {
        return Err(invalid("V26 object identity differs"));
    }
    Ok(())
}

fn validate_receipt(receipt: &V26LayoutReceipt) -> Result<()> {
    let authority = &receipt.authority;
    if authority.schema != V26_LAYOUT_SCHEMA
        || authority.generation.is_empty()
        || !exact_lower_hex(&authority.source_commit, 40)
        || !exact_lower_hex(&authority.source_archive_sha256, 64)
        || authority.primary_seed != V26_PRIMARY_SEED
        || authority.replica_seed != V26_REPLICA_SEED
        || authority.page_capacity != V26_PAGE_CAPACITY
        || authority.expected_rows == 0
        || receipt.row_count != authority.expected_rows
    {
        return Err(invalid("V26 layout authority differs"));
    }
    validate_identity(
        &authority.construction_rows,
        "construction-parquet",
        &authority.generation,
    )?;
    validate_identity(
        &authority.source_map,
        "source-map-parquet",
        &authority.generation,
    )?;
    let leaves = receipt.row_count.div_ceil(u64::from(V26_PAGE_CAPACITY));
    let leaves_u32 = u32::try_from(leaves).map_err(|_| invalid("V26 page count overflows"))?;
    if receipt.leaves_per_tree != leaves_u32
        || receipt.page_count
            != leaves_u32
                .checked_mul(2)
                .ok_or_else(|| invalid("V26 page count overflows"))?
        || receipt.projection_steps
            != projected_steps(receipt.row_count, leaves)?
                .checked_mul(2)
                .ok_or_else(|| invalid("V26 projection work overflows"))?
        || receipt.worker_count == 0
        || receipt.elapsed_ns == 0
        || receipt.cpu_ns == 0
        || receipt.peak_rss_bytes == 0
        || receipt.peak_psi_full_avg10_milli_percent > 500
        || receipt.swap_end_bytes != receipt.swap_start_bytes
        || receipt.query_role_opens != 0
        || receipt.page_body_reads != 0
        || receipt.claim_eligible
    {
        return Err(invalid("V26 layout receipt differs"));
    }

    let input_roles = [
        "construction-parquet",
        "layout-manifest",
        "source-map-parquet",
    ];
    let output_roles = [
        "page-assignments-parquet",
        "primary-tree-parquet",
        "replica-tree-parquet",
    ];
    if receipt.inputs.len() != input_roles.len() || receipt.outputs.len() != output_roles.len() {
        return Err(invalid("V26 object inventory differs"));
    }
    if receipt.inputs[0] != authority.construction_rows || receipt.inputs[2] != authority.source_map
    {
        return Err(invalid("V26 construction input authority differs"));
    }
    for (identity, role) in receipt.inputs.iter().zip(input_roles) {
        validate_identity(identity, role, &authority.generation)?;
    }
    for (identity, role) in receipt.outputs.iter().zip(output_roles) {
        validate_identity(identity, role, &authority.generation)?;
    }
    let mut uris = BTreeSet::new();
    if receipt
        .inputs
        .iter()
        .chain(&receipt.outputs)
        .any(|identity| !uris.insert(identity.uri.as_str()))
    {
        return Err(invalid("V26 object URI roles overlap"));
    }
    Ok(())
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect(),
        ),
        scalar => scalar,
    }
}

pub fn canonical_v26_layout_receipt_bytes(receipt: &V26LayoutReceipt) -> Result<Vec<u8>> {
    validate_receipt(receipt)?;
    let value = serde_json::to_value(receipt)
        .map_err(|error| V26Error(format!("V26 layout receipt serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| V26Error(format!("V26 layout receipt serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn canonical_v26_layout_result_bytes(
    result: &V26LayoutResult,
    truths: &[V26QueryTruth],
    samples: &[V26LayoutSample],
) -> Result<Vec<u8>> {
    if result.schema != "borsuk-v26-layout-result-v1"
        || result.query_count != 512
        || truths.len() != 512
        || samples.len() != truths.len()
        || result.page_body_reads != 0
        || result.claim_eligible
    {
        return Err(invalid("V26 layout result authority differs"));
    }
    let mut total_hits = 0_u64;
    let mut minimum_recall = 1_000_000_u64;
    for (query_index, (truth, sample)) in truths.iter().zip(samples).enumerate() {
        if usize::try_from(truth.query_ordinal).ok() != Some(query_index)
            || sample.query_ordinal != truth.query_ordinal
            || truth.neighbor_source_ordinals.len() != 10
            || truth
                .neighbor_source_ordinals
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != 10
            || truth.ground_truth_page_assignments.len() != 10
        {
            return Err(invalid("V26 layout truth authority differs"));
        }
        let selected = exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, 8)?;
        let hits = v26_layout_hits(&truth.ground_truth_page_assignments, &selected);
        let recall = v26_ppm(u64::from(hits), 10)?;
        if sample.selected_pages != selected || sample.hits != hits || sample.recall_ppm != recall {
            return Err(invalid("V26 layout sample differs"));
        }
        total_hits = total_hits
            .checked_add(u64::from(hits))
            .ok_or_else(|| invalid("V26 metric arithmetic differs"))?;
        minimum_recall = minimum_recall.min(recall);
    }
    let aggregate = v26_ppm(total_hits, truths.len() as u64 * 10)?;
    let expected_disposition = if aggregate >= 995_000 && minimum_recall >= 800_000 {
        V26Disposition::BoundedLayoutCandidate
    } else {
        V26Disposition::LayoutRejected
    };
    if result.aggregate_recall_ppm != aggregate
        || result.minimum_query_recall_ppm != minimum_recall
        || result.disposition != expected_disposition
    {
        return Err(invalid("V26 layout result metrics differ"));
    }
    let value = serde_json::json!({"result": result, "samples": samples});
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| V26Error(format!("V26 layout result serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{
        V26ConstructionRow, V26Disposition, V26LayoutAuthority, V26LayoutReceipt, V26LayoutResult,
        V26LayoutSample, V26ObjectIdentity, V26QueryTruth, build_v26_dual_tree_layout,
        canonical_v26_layout_receipt_bytes, canonical_v26_layout_result_bytes,
        exact_v26_layout_oracle_pages, validate_v26_dual_tree_layout,
    };

    const PRIMARY_SEED: u64 = 0x5632_362d_5452_4545;
    const REPLICA_SEED: u64 = 0x5632_362d_5245_504c;

    fn row(source_ordinal: u64) -> V26ConstructionRow {
        let mut vector = [0.0_f32; 96];
        for (dimension, coordinate) in vector.iter_mut().enumerate() {
            let raw = ((source_ordinal * 37 + dimension as u64 * 17) % 257) as i32 - 128;
            *coordinate = raw as f32 / 128.0;
        }
        V26ConstructionRow {
            source_ordinal,
            vector,
        }
    }

    fn authority(expected_rows: u64) -> V26LayoutAuthority {
        V26LayoutAuthority {
            schema: "borsuk-v26-dual-tree-layout-v1".to_owned(),
            generation: "v26-test-generation".to_owned(),
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            construction_rows: identity("construction-parquet", '3', 1024),
            source_map: identity("source-map-parquet", '4', 512),
            primary_seed: PRIMARY_SEED,
            replica_seed: REPLICA_SEED,
            page_capacity: 704,
            expected_rows,
        }
    }

    fn identity(role: &str, marker: char, encoded_bytes: u64) -> V26ObjectIdentity {
        V26ObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://v26-test/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: marker.to_string().repeat(64),
            encoded_bytes,
            generation: "v26-test-generation".to_owned(),
        }
    }

    #[test]
    fn v26_tree_balances_aligned_leaves_and_is_byte_deterministic() {
        // Break caught: unstable worker scheduling, an unaligned split, or page-range overlap.
        let rows = (0..1_409).map(row).collect::<Vec<_>>();
        let authority = authority(rows.len() as u64);

        let one = build_v26_dual_tree_layout(&authority, &rows).unwrap();
        let repeated = build_v26_dual_tree_layout(&authority, &rows).unwrap();
        assert_eq!(
            serde_json::to_vec(&one).unwrap(),
            serde_json::to_vec(&repeated).unwrap()
        );

        let (primary, replica, assignments) = one;
        assert_eq!(assignments.len(), 1_409);
        assert_eq!(
            assignments
                .iter()
                .map(|assignment| assignment.source_ordinal)
                .collect::<BTreeSet<_>>(),
            (0..1_409).collect()
        );

        let primary_counts = assignments.iter().fold(BTreeMap::new(), |mut counts, row| {
            *counts.entry(row.primary_page).or_insert(0_usize) += 1;
            counts
        });
        let replica_counts = assignments.iter().fold(BTreeMap::new(), |mut counts, row| {
            *counts.entry(row.replica_page).or_insert(0_usize) += 1;
            counts
        });
        assert_eq!(
            primary_counts.keys().copied().collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            replica_counts.keys().copied().collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert!(primary_counts.values().all(|count| *count <= 704));
        assert!(replica_counts.values().all(|count| *count <= 704));
        assert!(
            assignments
                .iter()
                .all(|assignment| assignment.primary_page != assignment.replica_page)
        );
        assert_eq!(primary.seed, PRIMARY_SEED);
        assert_eq!(replica.seed, REPLICA_SEED);
        assert!(primary.nodes.iter().all(|node| node.threshold.is_finite()));
        assert!(replica.nodes.iter().all(|node| node.threshold.is_finite()));
        validate_v26_dual_tree_layout(&authority, &primary, &replica, &assignments).unwrap();

        let mut invalid = assignments.clone();
        invalid[0].replica_page = invalid[0].primary_page;
        assert!(validate_v26_dual_tree_layout(&authority, &primary, &replica, &invalid).is_err());
    }

    #[test]
    fn v26_tree_records_zero_gap_plateaus_without_losing_assignment_authority() {
        // Break caught: an unrecorded score plateau makes later best-first routing ambiguous.
        let rows = (0..705)
            .map(|source_ordinal| V26ConstructionRow {
                source_ordinal,
                vector: [0.125; 96],
            })
            .collect::<Vec<_>>();
        let authority = authority(rows.len() as u64);
        let (primary, replica, assignments) =
            build_v26_dual_tree_layout(&authority, &rows).unwrap();
        assert_eq!(primary.nodes[0].split_gap, 0.0);
        assert_eq!(replica.nodes[0].split_gap, 0.0);
        assert_eq!(assignments.len(), 705);
        validate_v26_dual_tree_layout(&authority, &primary, &replica, &assignments).unwrap();
    }

    fn receipt() -> V26LayoutReceipt {
        let authority = authority(1_409);
        V26LayoutReceipt {
            inputs: vec![
                authority.construction_rows.clone(),
                identity("layout-manifest", '5', 900),
                authority.source_map.clone(),
            ],
            authority,
            outputs: vec![
                identity("page-assignments-parquet", '6', 30_000),
                identity("primary-tree-parquet", '7', 4_000),
                identity("replica-tree-parquet", '8', 4_000),
            ],
            row_count: 1_409,
            leaves_per_tree: 3,
            page_count: 6,
            projection_steps: 6_494_208,
            worker_count: 4,
            elapsed_ns: 2_000_000,
            cpu_ns: 6_000_000,
            peak_rss_bytes: 32 * 1024 * 1024,
            peak_psi_full_avg10_milli_percent: 0,
            swap_start_bytes: 0,
            swap_end_bytes: 0,
            query_role_opens: 0,
            page_body_reads: 0,
            claim_eligible: false,
        }
    }

    #[test]
    fn v26_tree_layout_receipt_recomputes_counts_work_and_identities() {
        // Break caught: accepting incomplete authority, hidden evaluation I/O, or false counts.
        let valid = receipt();
        let bytes = canonical_v26_layout_receipt_bytes(&valid).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert!(!bytes[..bytes.len() - 1].contains(&b'\n'));

        type ReceiptMutation = Box<dyn Fn(&mut V26LayoutReceipt)>;
        let mut mutations: Vec<ReceiptMutation> = vec![
            Box::new(|value| value.authority.schema.push_str("-drift")),
            Box::new(|value| value.authority.source_commit = "a".repeat(39)),
            Box::new(|value| value.authority.source_archive_sha256 = "g".repeat(64)),
            Box::new(|value| value.authority.primary_seed ^= 1),
            Box::new(|value| value.authority.replica_seed ^= 1),
            Box::new(|value| value.authority.page_capacity = 703),
            Box::new(|value| value.row_count -= 1),
            Box::new(|value| value.leaves_per_tree -= 1),
            Box::new(|value| value.page_count -= 1),
            Box::new(|value| value.projection_steps = 0),
            Box::new(|value| value.worker_count = 0),
            Box::new(|value| value.elapsed_ns = 0),
            Box::new(|value| value.cpu_ns = 0),
            Box::new(|value| value.peak_rss_bytes = 0),
            Box::new(|value| value.peak_psi_full_avg10_milli_percent = 501),
            Box::new(|value| value.swap_end_bytes = 1),
            Box::new(|value| value.query_role_opens = 1),
            Box::new(|value| value.page_body_reads = 1),
            Box::new(|value| value.claim_eligible = true),
            Box::new(|value| value.inputs[0].role = "pseudoqueries-parquet".to_owned()),
            Box::new(|value| value.inputs[0].digest_algorithm = "blake3".to_owned()),
            Box::new(|value| value.inputs[0].digest = "A".repeat(64)),
            Box::new(|value| value.inputs[0].encoded_bytes = 0),
            Box::new(|value| value.outputs.swap(0, 1)),
            Box::new(|value| value.outputs[0].uri = value.inputs[0].uri.clone()),
        ];
        for mutate in mutations.drain(..) {
            let mut candidate = valid.clone();
            mutate(&mut candidate);
            assert!(canonical_v26_layout_receipt_bytes(&candidate).is_err());
        }
    }

    #[test]
    fn v26_layout_oracle_uses_both_pages_and_prefers_shorter_lexicographic_cover() {
        // Break caught: redundant pages displace the shortest complete two-copy cover.
        let assignments = (1_u32..=10).map(|page| vec![0, page]).collect::<Vec<_>>();
        assert_eq!(
            exact_v26_layout_oracle_pages(&assignments, 8).unwrap(),
            vec![0]
        );

        let assignments = vec![
            vec![0, 8],
            vec![1, 8],
            vec![2, 9],
            vec![3, 9],
            vec![4, 10],
            vec![5, 10],
            vec![6, 11],
            vec![7, 11],
            vec![12, 13],
            vec![14, 15],
        ];
        assert_eq!(
            exact_v26_layout_oracle_pages(&assignments, 8).unwrap(),
            vec![8, 9, 10, 11, 12, 14]
        );
    }

    #[test]
    fn v26_layout_oracle_result_recomputes_samples_gates_and_disposition() {
        // Break caught: a claimed layout result drifts from its per-query truth authority.
        let truths = (0_u32..512)
            .map(|query_ordinal| {
                let ground_truth_page_assignments = if query_ordinal < 13 {
                    (0_u32..10).map(|page| vec![page]).collect::<Vec<_>>()
                } else {
                    (0_u32..10)
                        .map(|page| vec![0, page + 1])
                        .collect::<Vec<_>>()
                };
                V26QueryTruth {
                    query_ordinal,
                    neighbor_source_ordinals: (0_u64..10)
                        .map(|neighbor| u64::from(query_ordinal) * 10 + neighbor)
                        .collect(),
                    ground_truth_page_assignments,
                }
            })
            .collect::<Vec<_>>();
        let samples = truths
            .iter()
            .map(|truth| {
                let selected_pages =
                    exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, 8).unwrap();
                let hits = if truth.query_ordinal < 13 { 8 } else { 10 };
                V26LayoutSample {
                    query_ordinal: truth.query_ordinal,
                    selected_pages,
                    hits,
                    recall_ppm: u64::from(hits) * 100_000,
                }
            })
            .collect::<Vec<_>>();
        let valid = V26LayoutResult {
            schema: "borsuk-v26-layout-result-v1".to_owned(),
            query_count: 512,
            aggregate_recall_ppm: 994_921,
            minimum_query_recall_ppm: 800_000,
            disposition: V26Disposition::LayoutRejected,
            page_body_reads: 0,
            claim_eligible: false,
        };
        let bytes = canonical_v26_layout_result_bytes(&valid, &truths, &samples).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        type ResultMutation = Box<dyn Fn(&mut V26LayoutResult, &mut Vec<V26LayoutSample>)>;
        let mut mutations: Vec<ResultMutation> = vec![
            Box::new(|result, _| result.query_count = 511),
            Box::new(|result, _| result.aggregate_recall_ppm += 1),
            Box::new(|result, _| result.minimum_query_recall_ppm += 1),
            Box::new(|result, _| result.disposition = V26Disposition::BoundedLayoutCandidate),
            Box::new(|result, _| result.page_body_reads = 1),
            Box::new(|result, _| result.claim_eligible = true),
            Box::new(|_, rows| rows[0].query_ordinal = 1),
            Box::new(|_, rows| rows[0].selected_pages.swap(0, 1)),
            Box::new(|_, rows| rows[0].hits += 1),
            Box::new(|_, rows| rows[0].recall_ppm += 1),
        ];
        for mutation in mutations.drain(..) {
            let mut result = valid.clone();
            let mut rows = samples.clone();
            mutation(&mut result, &mut rows);
            assert!(canonical_v26_layout_result_bytes(&result, &truths, &rows).is_err());
        }
    }
}

#[cfg(test)]
mod local_schema_tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::{
        v26_construction_schema, v26_page_assignments_schema, v26_source_map_schema,
        v26_tree_schema,
    };

    #[test]
    fn v26_layout_local_schema_contracts_are_exact_and_nonnullable() {
        // Break caught: cross-language field/type/order/nullability drift.
        let vector = DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
        );
        assert_eq!(
            v26_construction_schema(),
            Schema::new(vec![
                Field::new("source_ordinal", DataType::UInt64, false),
                Field::new("vector", vector, false),
            ])
        );
        assert_eq!(
            v26_source_map_schema(),
            Schema::new(vec![
                Field::new("source_ordinal", DataType::UInt64, false),
                Field::new("dataset_ordinal", DataType::UInt64, false),
            ])
        );
        assert_eq!(
            v26_tree_schema(),
            Schema::new(vec![
                Field::new("node_ordinal", DataType::UInt32, false),
                Field::new("left", DataType::UInt32, true),
                Field::new("right", DataType::UInt32, true),
                Field::new("direction_ordinal", DataType::UInt8, false),
                Field::new("threshold", DataType::Float32, false),
                Field::new("split_gap", DataType::Float32, false),
                Field::new("leaf_page", DataType::UInt32, true),
            ])
        );
        assert_eq!(
            v26_page_assignments_schema(),
            Schema::new(vec![
                Field::new("source_ordinal", DataType::UInt64, false),
                Field::new("primary_page", DataType::UInt32, false),
                Field::new("replica_page", DataType::UInt32, false),
            ])
        );
    }
}
