use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const MANIFEST_SCHEMA: &str = "borsuk-v23-rabitq-manifest-v1";
const RECEIPT_SCHEMA: &str = "borsuk-v23-rabitq-receipt-v1";
pub(crate) const V23_RABITQ_MIN_ALIGNMENT: f32 = 0.102_062_07;
const CONSTRUCTION_INPUT_ROLES: [&str; 3] = ["tree-receipt", "incidence-tree", "page-roster"];
const CONSTRUCTION_OUTPUT_ROLES: [&str; 5] = [
    "row-codes",
    "leaf-offsets",
    "centroids",
    "rotation",
    "f16-control",
];
const EVALUATION_INPUT_ROLES: [&str; 9] = [
    "construction-receipt",
    "incidence-tree",
    "row-codes",
    "leaf-offsets",
    "centroids",
    "rotation",
    "f16-control",
    "d2-report",
    "query-parquet",
];
const EVALUATION_OUTPUT_ROLES: [&str; 1] = ["screen-result"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23RaBitQPhase {
    Construction,
    Development,
    Holdout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23RaBitQRunMode {
    Preflight(V23RaBitQPhase),
    Execute(V23RaBitQPhase),
}

impl V23RaBitQRunMode {
    fn phase(self) -> V23RaBitQPhase {
        match self {
            Self::Preflight(phase) | Self::Execute(phase) => phase,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23RaBitQObjectIdentity {
    pub(crate) role: String,
    pub(crate) uri: String,
    pub(crate) sha256: String,
    pub(crate) blake3: Option<String>,
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23RaBitQManifest {
    pub(crate) schema: String,
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) index_id: String,
    pub(crate) dataset_id: String,
    pub(crate) rotation_seed_hex: String,
    pub(crate) expected_pages: u32,
    pub(crate) expected_source_occurrences: u64,
    pub(crate) expected_unique_rows: u64,
    pub(crate) page_namespace_uri_prefix: Option<String>,
    pub(crate) run_mode: V23RaBitQRunMode,
    pub(crate) registered_inputs: Vec<V23RaBitQObjectIdentity>,
    pub(crate) output_uri_prefix: String,
    pub(crate) output_roles: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23RaBitQTerminalStatus {
    Complete,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23RaBitQReceipt {
    pub(crate) schema: String,
    pub(crate) manifest_sha256: String,
    pub(crate) manifest: V23RaBitQManifest,
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) index_id: String,
    pub(crate) dataset_id: String,
    pub(crate) run_mode: V23RaBitQRunMode,
    pub(crate) inputs: Vec<V23RaBitQObjectIdentity>,
    pub(crate) outputs: Vec<V23RaBitQObjectIdentity>,
    pub(crate) terminal_status: V23RaBitQTerminalStatus,
    pub(crate) stop_reason: Option<String>,
    pub(crate) claim_eligible: bool,
}

impl V23RaBitQReceipt {
    pub(crate) fn complete(
        manifest: &V23RaBitQManifest,
        manifest_bytes: &[u8],
        outputs: Vec<V23RaBitQObjectIdentity>,
    ) -> Self {
        Self {
            schema: RECEIPT_SCHEMA.to_string(),
            manifest_sha256: format!("{:x}", Sha256::digest(manifest_bytes)),
            manifest: manifest.clone(),
            source_commit: manifest.source_commit.clone(),
            source_archive_sha256: manifest.source_archive_sha256.clone(),
            index_id: manifest.index_id.clone(),
            dataset_id: manifest.dataset_id.clone(),
            run_mode: manifest.run_mode,
            inputs: manifest.registered_inputs.clone(),
            outputs,
            terminal_status: V23RaBitQTerminalStatus::Complete,
            stop_reason: None,
            claim_eligible: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23RaBitQServingProjection {
    pub(crate) row_bytes: u64,
    pub(crate) leaf_offset_bytes: u64,
    pub(crate) centroid_bytes: u64,
    pub(crate) tree_bytes: u64,
    pub(crate) rotation_bytes: u64,
    pub(crate) reserve_bytes: u64,
    pub(crate) total_bytes: u64,
    pub(crate) ceiling_bytes: u64,
    pub(crate) headroom_bytes: u64,
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_identity(identity: &V23RaBitQObjectIdentity) -> Result<()> {
    if identity.role.is_empty()
        || !identity.uri.starts_with("s3://")
        || !is_lower_hex(&identity.sha256, 64)
        || identity.blake3.is_some()
        || identity.encoded_bytes == 0
    {
        return Err(BorsukError::InvalidStorage(
            "V23 RaBitQ object identity differs".to_string(),
        ));
    }
    Ok(())
}

fn validate_roles(values: &[V23RaBitQObjectIdentity], expected: &[&str]) -> Result<()> {
    if values.len() != expected.len()
        || values
            .iter()
            .zip(expected)
            .any(|(value, role)| value.role != *role)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 RaBitQ manifest roles differ".to_string(),
        ));
    }
    values.iter().try_for_each(validate_identity)
}

pub(crate) fn validate_v23_rabitq_manifest(value: &V23RaBitQManifest) -> Result<()> {
    if value.schema != MANIFEST_SCHEMA
        || !is_lower_hex(&value.source_commit, 40)
        || !is_lower_hex(&value.source_archive_sha256, 64)
        || !is_lower_hex(&value.rotation_seed_hex, 64)
        || value.expected_pages < 8
        || value.expected_unique_rows == 0
        || value.expected_source_occurrences < value.expected_unique_rows
        || value.expected_source_occurrences > value.expected_unique_rows.saturating_mul(2)
        || value.index_id.is_empty()
        || value.dataset_id.is_empty()
    {
        return Err(BorsukError::InvalidStorage(
            "V23 RaBitQ manifest authority differs".to_string(),
        ));
    }
    let (inputs, outputs): (&[&str], &[&str]) = match value.run_mode.phase() {
        V23RaBitQPhase::Construction => {
            let Some(prefix) = value.page_namespace_uri_prefix.as_deref() else {
                return Err(BorsukError::InvalidStorage(
                    "V23 RaBitQ page namespace authority differs".to_string(),
                ));
            };
            if !prefix.starts_with("s3://") || !prefix.ends_with('/') {
                return Err(BorsukError::InvalidStorage(
                    "V23 RaBitQ page namespace authority differs".to_string(),
                ));
            }
            (&CONSTRUCTION_INPUT_ROLES, &CONSTRUCTION_OUTPUT_ROLES)
        }
        V23RaBitQPhase::Development | V23RaBitQPhase::Holdout => {
            if value.page_namespace_uri_prefix.is_some() {
                return Err(BorsukError::InvalidStorage(
                    "V23 RaBitQ page namespace authority differs".to_string(),
                ));
            }
            (&EVALUATION_INPUT_ROLES, &EVALUATION_OUTPUT_ROLES)
        }
    };
    validate_roles(&value.registered_inputs, inputs)?;
    if !value.output_uri_prefix.starts_with("s3://")
        || !value.output_uri_prefix.ends_with('/')
        || value.output_roles.len() != outputs.len()
        || value
            .output_roles
            .iter()
            .zip(outputs)
            .any(|(value, expected)| value != expected)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 RaBitQ manifest output authority differs".to_string(),
        ));
    }
    let mut uris = BTreeSet::new();
    if value.registered_inputs.iter().any(|identity| {
        !uris.insert(identity.uri.as_str())
            || identity.uri.starts_with(&value.output_uri_prefix)
            || value.output_uri_prefix.starts_with(&identity.uri)
    }) {
        return Err(BorsukError::InvalidStorage(
            "V23 RaBitQ object URIs overlap".to_string(),
        ));
    }
    if value
        .page_namespace_uri_prefix
        .as_deref()
        .is_some_and(|prefix| {
            value.output_uri_prefix.starts_with(prefix)
                || prefix.starts_with(&value.output_uri_prefix)
                || value.registered_inputs.iter().any(|identity| {
                    identity.uri.starts_with(prefix) || prefix.starts_with(&identity.uri)
                })
        })
    {
        return Err(BorsukError::InvalidStorage(
            "V23 RaBitQ page namespace URI overlaps authority".to_string(),
        ));
    }
    Ok(())
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 RaBitQ JSON serialization failed: {error}"))
    })?;
    let value = crate::v23_incidence::canonical_json_value(value);
    let mut bytes = serde_json::to_vec(&value).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 RaBitQ canonical JSON failed: {error}"))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn canonical_v23_rabitq_manifest_bytes(value: &V23RaBitQManifest) -> Result<Vec<u8>> {
    validate_v23_rabitq_manifest(value)?;
    canonical_json_bytes(value)
}

fn validate_receipt(value: &V23RaBitQReceipt) -> Result<()> {
    validate_v23_rabitq_manifest(&value.manifest)?;
    let manifest_bytes = canonical_v23_rabitq_manifest_bytes(&value.manifest)?;
    if value.schema != RECEIPT_SCHEMA
        || value.manifest_sha256 != format!("{:x}", Sha256::digest(&manifest_bytes))
        || value.source_commit != value.manifest.source_commit
        || value.source_archive_sha256 != value.manifest.source_archive_sha256
        || value.index_id != value.manifest.index_id
        || value.dataset_id != value.manifest.dataset_id
        || value.run_mode != value.manifest.run_mode
        || value.inputs != value.manifest.registered_inputs
        || value.claim_eligible
    {
        return Err(BorsukError::InvalidStorage(
            "V23 RaBitQ receipt binding differs".to_string(),
        ));
    }
    let expected_outputs = value
        .manifest
        .output_roles
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    validate_roles(&value.outputs, &expected_outputs)?;
    if value.outputs.iter().any(|output| {
        output
            .uri
            .strip_prefix(&value.manifest.output_uri_prefix)
            .is_none_or(|name| name.is_empty() || name.contains('/'))
    }) {
        return Err(BorsukError::InvalidStorage(
            "V23 RaBitQ receipt output URI differs".to_string(),
        ));
    }
    match (value.terminal_status, value.stop_reason.as_deref()) {
        (V23RaBitQTerminalStatus::Complete, None) => Ok(()),
        (V23RaBitQTerminalStatus::Failed | V23RaBitQTerminalStatus::Stopped, Some(reason))
            if !reason.is_empty() =>
        {
            Ok(())
        }
        _ => Err(BorsukError::InvalidStorage(
            "V23 RaBitQ terminal state differs".to_string(),
        )),
    }
}

pub(crate) fn canonical_v23_rabitq_receipt_bytes(value: &V23RaBitQReceipt) -> Result<Vec<u8>> {
    validate_receipt(value)?;
    canonical_json_bytes(value)
}

pub(crate) fn read_v23_rabitq_receipt(bytes: &[u8]) -> Result<V23RaBitQReceipt> {
    let value: V23RaBitQReceipt = serde_json::from_slice(bytes).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 RaBitQ receipt JSON differs: {error}"))
    })?;
    if canonical_v23_rabitq_receipt_bytes(&value)? != bytes {
        return Err(BorsukError::InvalidStorage(
            "V23 RaBitQ receipt canonical bytes differ".to_string(),
        ));
    }
    Ok(value)
}

pub(crate) fn project_v23_rabitq_serving_bytes(rows: u64) -> Result<V23RaBitQServingProjection> {
    const CEILING: u64 = 3 * 1024 * 1024 * 1024;
    if rows == 0 {
        return Err(BorsukError::InvalidStorage(
            "V23 RaBitQ row count differs".to_string(),
        ));
    }
    let row_bytes = rows.checked_mul(28).ok_or_else(|| {
        BorsukError::InvalidStorage("V23 RaBitQ serving projection overflow".to_string())
    })?;
    let leaf_offset_bytes = 65_537 * 8;
    let centroid_bytes = 65_536 * 96 * 2;
    let tree_bytes = 40_369_836;
    let rotation_bytes = 96 * 96 * 4;
    let reserve_bytes = 64 * 1024 * 1024;
    let total_bytes = row_bytes
        .checked_add(
            leaf_offset_bytes + centroid_bytes + tree_bytes + rotation_bytes + reserve_bytes,
        )
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V23 RaBitQ serving projection overflow".to_string())
        })?;
    if total_bytes > CEILING {
        return Err(BorsukError::InvalidStorage(
            "V23 RaBitQ serving projection exceeds ceiling".to_string(),
        ));
    }
    Ok(V23RaBitQServingProjection {
        row_bytes,
        leaf_offset_bytes,
        centroid_bytes,
        tree_bytes,
        rotation_bytes,
        reserve_bytes,
        total_bytes,
        ceiling_bytes: CEILING,
        headroom_bytes: CEILING - total_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        V23RaBitQManifest, V23RaBitQObjectIdentity, V23RaBitQPhase, V23RaBitQReceipt,
        V23RaBitQRunMode, V23RaBitQTerminalStatus, canonical_v23_rabitq_manifest_bytes,
        canonical_v23_rabitq_receipt_bytes, project_v23_rabitq_serving_bytes,
        read_v23_rabitq_receipt, validate_v23_rabitq_manifest,
    };

    fn identity(role: &str, marker: u8) -> V23RaBitQObjectIdentity {
        V23RaBitQObjectIdentity {
            role: role.to_string(),
            uri: format!("s3://borsuk-v23-rabitq/{role}-{marker:02x}"),
            sha256: format!("{marker:02x}").repeat(32),
            blake3: None,
            encoded_bytes: u64::from(marker) + 1,
        }
    }

    fn construction_manifest() -> V23RaBitQManifest {
        V23RaBitQManifest {
            schema: "borsuk-v23-rabitq-manifest-v1".to_string(),
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            index_id: "index-bcda7bb66812e162d45077e6".to_string(),
            dataset_id: "deep-image-96".to_string(),
            rotation_seed_hex: "3".repeat(64),
            expected_pages: 28_282,
            expected_source_occurrences: 19_980_000,
            expected_unique_rows: 9_990_000,
            page_namespace_uri_prefix: Some(
                "s3://borsuk-v23-rabitq/frozen-attempt/pages/".to_string(),
            ),
            run_mode: V23RaBitQRunMode::Execute(V23RaBitQPhase::Construction),
            registered_inputs: vec![
                identity("tree-receipt", 1),
                identity("incidence-tree", 2),
                identity("page-roster", 3),
            ],
            output_uri_prefix: "s3://borsuk-v23-rabitq/construction-fixture/".to_string(),
            output_roles: vec![
                "row-codes".to_string(),
                "leaf-offsets".to_string(),
                "centroids".to_string(),
                "rotation".to_string(),
                "f16-control".to_string(),
            ],
        }
    }

    #[test]
    fn v23_rabitq_authority_rejects_role_schema_digest_and_phase_drift() {
        let manifest = construction_manifest();
        validate_v23_rabitq_manifest(&manifest).unwrap();
        let bytes = canonical_v23_rabitq_manifest_bytes(&manifest).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut mutations = Vec::new();
        let mut value = manifest.clone();
        value.schema.push_str("-unknown");
        mutations.push(value);
        let mut value = manifest.clone();
        value.registered_inputs.pop();
        mutations.push(value);
        let mut value = manifest.clone();
        value.output_roles.push("screen-result".to_string());
        mutations.push(value);
        let mut value = manifest.clone();
        value.registered_inputs[0].sha256 = "g".repeat(64);
        mutations.push(value);
        let mut value = manifest.clone();
        value.registered_inputs[0].sha256.clear();
        value.registered_inputs[0].blake3 = Some("a".repeat(64));
        mutations.push(value);
        let mut value = manifest.clone();
        value.registered_inputs[0].encoded_bytes = 0;
        mutations.push(value);
        let mut value = manifest.clone();
        value.output_uri_prefix = value.registered_inputs[0].uri.clone();
        mutations.push(value);
        let mut value = manifest.clone();
        value.run_mode = V23RaBitQRunMode::Execute(V23RaBitQPhase::Development);
        mutations.push(value);
        let mut value = manifest.clone();
        value.page_namespace_uri_prefix = None;
        mutations.push(value);
        let mut value = manifest.clone();
        value.page_namespace_uri_prefix = Some(value.registered_inputs[2].uri.clone());
        mutations.push(value);
        for mutation in mutations {
            assert!(validate_v23_rabitq_manifest(&mutation).is_err());
        }

        let mut json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("unknown".to_string(), true.into());
        assert!(serde_json::from_value::<V23RaBitQManifest>(json).is_err());
    }

    #[test]
    fn v23_rabitq_authority_receipt_binds_manifest_inputs_outputs_and_terminal_state() {
        let manifest = construction_manifest();
        let manifest_bytes = canonical_v23_rabitq_manifest_bytes(&manifest).unwrap();
        let outputs = manifest
            .output_roles
            .iter()
            .enumerate()
            .map(|(ordinal, role)| V23RaBitQObjectIdentity {
                role: role.clone(),
                uri: format!("{}{role}", manifest.output_uri_prefix),
                sha256: format!("{:02x}", ordinal + 10).repeat(32),
                blake3: None,
                encoded_bytes: ordinal as u64 + 1,
            })
            .collect::<Vec<_>>();
        let receipt = V23RaBitQReceipt::complete(&manifest, &manifest_bytes, outputs);
        let bytes = canonical_v23_rabitq_receipt_bytes(&receipt).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(read_v23_rabitq_receipt(&bytes).unwrap(), receipt);

        let mut mutation = receipt.clone();
        mutation.outputs[0].sha256 = "g".repeat(64);
        assert!(canonical_v23_rabitq_receipt_bytes(&mutation).is_err());
        let mut mutation = receipt.clone();
        mutation.terminal_status = V23RaBitQTerminalStatus::Stopped;
        assert!(canonical_v23_rabitq_receipt_bytes(&mutation).is_err());
        let mut mutation = receipt.clone();
        mutation.source_commit = "f".repeat(40);
        assert!(canonical_v23_rabitq_receipt_bytes(&mutation).is_err());
        let mut mutation = receipt.clone();
        mutation.index_id.push_str("-other");
        assert!(canonical_v23_rabitq_receipt_bytes(&mutation).is_err());
        let mut mutation = receipt.clone();
        mutation.dataset_id.push_str("-other");
        assert!(canonical_v23_rabitq_receipt_bytes(&mutation).is_err());
    }

    #[test]
    fn v23_rabitq_authority_projects_exact_100m_resident_bytes() {
        let value = project_v23_rabitq_serving_bytes(100_000_000).unwrap();
        assert_eq!(value.row_bytes, 2_800_000_000);
        assert_eq!(value.leaf_offset_bytes, 524_296);
        assert_eq!(value.centroid_bytes, 12_582_912);
        assert_eq!(value.tree_bytes, 40_369_836);
        assert_eq!(value.rotation_bytes, 36_864);
        assert_eq!(value.reserve_bytes, 67_108_864);
        assert_eq!(value.total_bytes, 2_920_622_772);
        assert_eq!(value.ceiling_bytes, 3_221_225_472);
        assert_eq!(value.headroom_bytes, 300_602_700);
        assert!(project_v23_rabitq_serving_bytes(0).is_err());
    }
}
