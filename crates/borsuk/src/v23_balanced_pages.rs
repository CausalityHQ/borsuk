use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{BorsukError, Result};

const MANIFEST_SCHEMA: &str = "borsuk-v23-balanced-page-manifest-v1";
const RECEIPT_SCHEMA: &str = "borsuk-v23-balanced-page-receipt-v1";
const DIMENSIONS: u64 = 96;
const SUPERCELL_TARGET_ROWS: u64 = 12_288;
const PRIMARY_ROWS_PER_PAGE: u64 = 384;
const TOP_SUPERCELLS: u64 = 96;
const SELECTED_PAGES: u64 = 8;
const MAX_SUPERCELLS: u64 = 8_192;
const MAX_PAGES_PER_SUPERCELL: u64 = 64;
const RUNTIME_RESERVE_BYTES: u64 = 850 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23BalancedArmConfig {
    pub(crate) name: String,
    pub(crate) amplification_ppm: u64,
    pub(crate) replicas_per_page: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23BalancedIdentity {
    pub(crate) role: String,
    pub(crate) uri: String,
    pub(crate) digest_algorithm: String,
    pub(crate) digest: String,
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23BalancedManifest {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) dataset_id: String,
    pub(crate) rows: u64,
    pub(crate) dimensions: u32,
    pub(crate) supercell_target_rows: u64,
    pub(crate) primary_rows_per_page: u16,
    pub(crate) top_supercells: u16,
    pub(crate) selected_pages: u8,
    pub(crate) arms: Vec<V23BalancedArmConfig>,
    pub(crate) ordered_inputs: Vec<V23BalancedIdentity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23BalancedStop {
    Authority,
    Resource,
    Determinism,
    Progress,
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23BalancedReceipt {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) manifest_sha256: String,
    pub(crate) ordered_inputs: Vec<V23BalancedIdentity>,
    pub(crate) outputs: Vec<V23BalancedIdentity>,
    pub(crate) stop: Option<V23BalancedStop>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23BalancedProjection {
    pub(crate) supercells: u64,
    pub(crate) maximum_pages: u64,
    pub(crate) maximum_scored_dimensions: u64,
    pub(crate) serving_bytes: u64,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(format!("V23 balanced page {message}"))
}

fn valid_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn validate_v23_balanced_identity(identity: &V23BalancedIdentity) -> Result<()> {
    if identity.role.is_empty()
        || !identity.uri.starts_with("s3://")
        || identity.digest_algorithm != "sha256"
        || !valid_lower_hex(&identity.digest, 64)
        || identity.encoded_bytes == 0
    {
        return Err(invalid("object identity differs"));
    }
    Ok(())
}

fn validate_identity_list(identities: &[V23BalancedIdentity]) -> Result<()> {
    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for identity in identities {
        validate_v23_balanced_identity(identity)?;
        if !roles.insert(identity.role.as_str()) || !uris.insert(identity.uri.as_str()) {
            return Err(invalid("object identity duplicates"));
        }
    }
    Ok(())
}

fn expected_arms() -> [V23BalancedArmConfig; 3] {
    [
        V23BalancedArmConfig {
            name: "amp-1125".to_owned(),
            amplification_ppm: 1_125_000,
            replicas_per_page: 48,
        },
        V23BalancedArmConfig {
            name: "amp-1250".to_owned(),
            amplification_ppm: 1_250_000,
            replicas_per_page: 96,
        },
        V23BalancedArmConfig {
            name: "amp-1500".to_owned(),
            amplification_ppm: 1_500_000,
            replicas_per_page: 192,
        },
    ]
}

pub(crate) fn validate_v23_balanced_manifest(manifest: &V23BalancedManifest) -> Result<()> {
    if manifest.schema != MANIFEST_SCHEMA
        || manifest.claim_eligible
        || !valid_lower_hex(&manifest.source_commit, 40)
        || !valid_lower_hex(&manifest.source_archive_sha256, 64)
        || manifest.dataset_id != "deep-image-96"
        || manifest.rows == 0
        || u64::from(manifest.dimensions) != DIMENSIONS
        || manifest.supercell_target_rows != SUPERCELL_TARGET_ROWS
        || u64::from(manifest.primary_rows_per_page) != PRIMARY_ROWS_PER_PAGE
        || u64::from(manifest.top_supercells) != TOP_SUPERCELLS
        || u64::from(manifest.selected_pages) != SELECTED_PAGES
        || manifest.arms.as_slice() != expected_arms()
    {
        return Err(invalid("manifest constants differ"));
    }
    if manifest
        .ordered_inputs
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<Vec<_>>()
        != ["source-shard-manifest", "f16-control"]
    {
        return Err(invalid("construction input roles differ"));
    }
    validate_identity_list(&manifest.ordered_inputs)?;
    project_v23_balanced_shape(manifest.rows)?;
    Ok(())
}

pub(crate) fn project_v23_balanced_shape(rows: u64) -> Result<V23BalancedProjection> {
    if rows == 0 {
        return Err(invalid("row count is zero"));
    }
    let targets = rows.div_ceil(SUPERCELL_TARGET_ROWS);
    let supercells = targets
        .checked_next_power_of_two()
        .ok_or_else(|| invalid("supercell projection overflows"))?
        .min(MAX_SUPERCELLS);
    let maximum_pages = rows
        .div_ceil(PRIMARY_ROWS_PER_PAGE)
        .checked_add(supercells - 1)
        .ok_or_else(|| invalid("page projection overflows"))?;
    let supercell_dimensions = supercells
        .checked_mul(DIMENSIONS)
        .ok_or_else(|| invalid("supercell work projection overflows"))?;
    let page_dimensions = TOP_SUPERCELLS
        .checked_mul(MAX_PAGES_PER_SUPERCELL)
        .and_then(|value| value.checked_mul(DIMENSIONS))
        .ok_or_else(|| invalid("page work projection overflows"))?;
    let maximum_scored_dimensions = supercell_dimensions
        .checked_add(page_dimensions)
        .ok_or_else(|| invalid("query work projection overflows"))?;
    let serving_bytes = supercells
        .checked_mul(DIMENSIONS * 2)
        .and_then(|value| value.checked_add(maximum_pages.checked_mul(DIMENSIONS * 2 + 64)?))
        .and_then(|value| value.checked_add(RUNTIME_RESERVE_BYTES))
        .ok_or_else(|| invalid("serving memory projection overflows"))?;
    Ok(V23BalancedProjection {
        supercells,
        maximum_pages,
        maximum_scored_dimensions,
        serving_bytes,
    })
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json_value(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

pub(crate) fn canonical_v23_balanced_receipt_bytes(
    receipt: &V23BalancedReceipt,
) -> Result<Vec<u8>> {
    if receipt.schema != RECEIPT_SCHEMA
        || receipt.claim_eligible
        || !valid_lower_hex(&receipt.manifest_sha256, 64)
        || receipt.ordered_inputs.is_empty()
        || (receipt.stop.is_some() && !receipt.outputs.is_empty())
    {
        return Err(invalid("receipt authority differs"));
    }
    validate_identity_list(&receipt.ordered_inputs)?;
    validate_identity_list(&receipt.outputs)?;
    let value = serde_json::to_value(receipt)
        .map_err(|error| invalid(&format!("receipt serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("canonical JSON failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        V23BalancedArmConfig, V23BalancedIdentity, V23BalancedManifest, V23BalancedReceipt,
        V23BalancedStop, canonical_v23_balanced_receipt_bytes, project_v23_balanced_shape,
        validate_v23_balanced_manifest,
    };

    fn sha256(byte: u8) -> String {
        std::iter::repeat_n(format!("{byte:02x}"), 32).collect()
    }

    fn identity(role: &str, byte: u8) -> V23BalancedIdentity {
        V23BalancedIdentity {
            role: role.to_owned(),
            uri: format!("s3://borsuk-v23-eu-west-1/frozen/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: sha256(byte),
            encoded_bytes: 4096,
        }
    }

    fn manifest_fixture(rows: u64) -> V23BalancedManifest {
        V23BalancedManifest {
            schema: "borsuk-v23-balanced-page-manifest-v1".to_owned(),
            claim_eligible: false,
            source_commit: sha256(0x11).chars().take(40).collect(),
            source_archive_sha256: sha256(0x12),
            dataset_id: "deep-image-96".to_owned(),
            rows,
            dimensions: 96,
            supercell_target_rows: 12_288,
            primary_rows_per_page: 384,
            top_supercells: 96,
            selected_pages: 8,
            arms: vec![
                V23BalancedArmConfig {
                    name: "amp-1125".to_owned(),
                    amplification_ppm: 1_125_000,
                    replicas_per_page: 48,
                },
                V23BalancedArmConfig {
                    name: "amp-1250".to_owned(),
                    amplification_ppm: 1_250_000,
                    replicas_per_page: 96,
                },
                V23BalancedArmConfig {
                    name: "amp-1500".to_owned(),
                    amplification_ppm: 1_500_000,
                    replicas_per_page: 192,
                },
            ],
            ordered_inputs: vec![
                identity("source-shard-manifest", 0x21),
                identity("f16-control", 0x22),
            ],
        }
    }

    #[test]
    fn v23_balanced_authority_rejects_identity_shape_and_role_drift() {
        let valid = manifest_fixture(100_000_000);
        validate_v23_balanced_manifest(&valid).unwrap();

        let mut mutations = Vec::new();
        let mut changed = valid.clone();
        changed.claim_eligible = true;
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.dimensions = 95;
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.arms[0].replicas_per_page = 49;
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.ordered_inputs[0].digest_algorithm = "blake3".to_owned();
        mutations.push(changed);
        let mut changed = valid.clone();
        changed
            .ordered_inputs
            .push(identity("official-query-parquet", 0x23));
        mutations.push(changed);

        for mutation in mutations {
            assert!(validate_v23_balanced_manifest(&mutation).is_err());
        }
    }

    #[test]
    fn v23_balanced_authority_projection_is_exact_at_100m() {
        let projection = project_v23_balanced_shape(100_000_000).unwrap();
        assert_eq!(projection.supercells, 8_192);
        assert_eq!(projection.maximum_pages, 268_608);
        assert_eq!(projection.maximum_scored_dimensions, 1_376_256);
        assert!(projection.serving_bytes < 3 * 1024 * 1024 * 1024);
        assert!(project_v23_balanced_shape(0).is_err());
    }

    #[test]
    fn v23_balanced_authority_receipt_is_claim_ineligible_and_canonical() {
        let receipt = V23BalancedReceipt {
            schema: "borsuk-v23-balanced-page-receipt-v1".to_owned(),
            claim_eligible: false,
            manifest_sha256: sha256(0x31),
            ordered_inputs: manifest_fixture(100_000_000).ordered_inputs,
            outputs: vec![identity("supercells-parquet", 0x32)],
            stop: None,
        };
        let bytes = canonical_v23_balanced_receipt_bytes(&receipt).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        let mut changed = receipt.clone();
        changed.claim_eligible = true;
        assert!(canonical_v23_balanced_receipt_bytes(&changed).is_err());
        let mut changed = receipt;
        changed.stop = Some(V23BalancedStop::Resource);
        changed
            .outputs
            .push(identity("partial-scientific-output", 0x33));
        assert!(canonical_v23_balanced_receipt_bytes(&changed).is_err());
    }
}
