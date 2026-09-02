//! Fail-fast contracts for the prerelease BORSUK V26 page layout.

#![allow(
    missing_docs,
    reason = "unpublished internal prerelease contract crate; not a compatibility surface"
)]

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

mod tree;

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

fn projected_steps(rows: u64, leaves: u64) -> Result<u64> {
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{
        V26ConstructionRow, V26LayoutAuthority, V26LayoutReceipt, V26ObjectIdentity,
        build_v26_dual_tree_layout, canonical_v26_layout_receipt_bytes,
        validate_v26_dual_tree_layout,
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
        V26LayoutReceipt {
            authority: authority(1_409),
            inputs: vec![
                identity("construction-parquet", '3', 540_000),
                identity("layout-manifest", '4', 900),
                identity("source-map-parquet", '5', 20_000),
            ],
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
}
