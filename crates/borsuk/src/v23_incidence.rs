use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{BorsukError, Result};

const V23_INCIDENCE_RECEIPT_SCHEMA: &str = "borsuk-v23-incidence-receipt-v1";
const V23_INCIDENCE_MANIFEST_SCHEMA: &str = "borsuk-v23-incidence-manifest-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23IncidencePhase {
    TreeTraining,
    PostingConstruction,
    DevelopmentEvaluation,
    HoldoutBinding,
    HoldoutEvaluation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23FmaBackend {
    Aarch64NeonFma,
    X86AvxFma,
    ScalarControl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23IncidenceStopClass {
    AuthorityStop,
    ResourceStop,
    DeterminismStop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceObjectIdentity {
    pub(crate) role: String,
    pub(crate) uri: String,
    pub(crate) digest_algorithm: String,
    pub(crate) digest: String,
    pub(crate) encoded_bytes: u64,
    pub(crate) generation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceCapabilityProbes {
    pub(crate) network_namespace_changed: bool,
    pub(crate) host_canary_denied: bool,
    pub(crate) network_canary_denied: bool,
    pub(crate) allowlisted_inputs_opened: bool,
    pub(crate) output_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceAlgorithm {
    pub(crate) dimensions: u32,
    pub(crate) reservoir_rows: u32,
    pub(crate) tree_depth: u8,
    pub(crate) leaf_count: u32,
    pub(crate) lloyd_iterations: u8,
    pub(crate) posting_caps: [u16; 3],
    pub(crate) probe_counts: [u16; 3],
    pub(crate) selection_width: u8,
    pub(crate) aggregate_recall_ppm: u64,
    pub(crate) minimum_query_recall_ppm: u64,
    pub(crate) oracle_attainment_ppm: u64,
}

impl V23IncidenceAlgorithm {
    const REGISTERED: Self = Self {
        dimensions: 96,
        reservoir_rows: 2_097_152,
        tree_depth: 16,
        leaf_count: 65_536,
        lloyd_iterations: 4,
        posting_caps: [512, 1024, 2048],
        probe_counts: [32, 64, 128],
        selection_width: 8,
        aggregate_recall_ppm: 975_000,
        minimum_query_recall_ppm: 800_000,
        oracle_attainment_ppm: 995_000,
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceManifest {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) phase: V23IncidencePhase,
    pub(crate) parent_receipt_sha256: Option<String>,
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) index_id: String,
    pub(crate) dataset_id: String,
    pub(crate) algorithm: V23IncidenceAlgorithm,
    pub(crate) ordered_inputs: Vec<V23IncidenceObjectIdentity>,
}

impl V23IncidenceCapabilityProbes {
    fn all_passed(&self) -> bool {
        self.network_namespace_changed
            && self.host_canary_denied
            && self.network_canary_denied
            && self.allowlisted_inputs_opened
            && self.output_writable
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23IncidenceReceipt {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) phase: V23IncidencePhase,
    pub(crate) parent_receipt_sha256: Option<String>,
    pub(crate) executable_sha256: String,
    pub(crate) fma_backend: V23FmaBackend,
    pub(crate) network_namespace_inode: u64,
    pub(crate) ordered_mounts: Vec<V23IncidenceObjectIdentity>,
    pub(crate) probes: V23IncidenceCapabilityProbes,
    pub(crate) outputs: Vec<V23IncidenceObjectIdentity>,
    pub(crate) stop: Option<V23IncidenceStopClass>,
}

fn valid_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_role_algorithm(role: &str, algorithm: &str) -> bool {
    let sha256_role = matches!(
        role,
        "construction-manifest"
            | "page-roster"
            | "query-parquet"
            | "neighbors-parquet"
            | "dataset-meta"
            | "d2-report"
            | "parent-receipt"
            | "preflight-receipt"
            | "executable"
            | "latency-samples"
    ) || role.starts_with("training-shard-");
    let blake3_role = matches!(
        role,
        "incidence-tree" | "incidence-postings-one" | "incidence-postings-two"
    ) || role.starts_with("page-body-");
    (sha256_role && algorithm == "sha256") || (blake3_role && algorithm == "blake3")
}

fn validate_object_identity(identity: &V23IncidenceObjectIdentity) -> Result<()> {
    if identity.role.is_empty()
        || identity.uri.is_empty()
        || identity.generation.is_empty()
        || identity.encoded_bytes == 0
        || !valid_role_algorithm(&identity.role, &identity.digest_algorithm)
        || !valid_lower_hex(&identity.digest, 64)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence object identity differs".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_v23_incidence_identity(
    observed: &V23IncidenceObjectIdentity,
    registered: &V23IncidenceObjectIdentity,
) -> Result<()> {
    validate_object_identity(observed)?;
    validate_object_identity(registered)?;
    if observed != registered {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence registered object identity differs".to_string(),
        ));
    }
    Ok(())
}

fn validate_identity_list(identities: &[V23IncidenceObjectIdentity]) -> Result<()> {
    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for identity in identities {
        validate_object_identity(identity)?;
        if !roles.insert(identity.role.as_str()) || !uris.insert(identity.uri.as_str()) {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence object identities are duplicated".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_receipt(receipt: &V23IncidenceReceipt) -> Result<()> {
    let parent_is_valid = match receipt.phase {
        V23IncidencePhase::TreeTraining => receipt.parent_receipt_sha256.is_none(),
        _ => receipt
            .parent_receipt_sha256
            .as_deref()
            .is_some_and(|digest| valid_lower_hex(digest, 64)),
    };
    if receipt.schema != V23_INCIDENCE_RECEIPT_SCHEMA
        || receipt.claim_eligible
        || !parent_is_valid
        || !valid_lower_hex(&receipt.executable_sha256, 64)
        || receipt.fma_backend == V23FmaBackend::ScalarControl
        || receipt.network_namespace_inode == 0
        || receipt.ordered_mounts.is_empty()
        || !receipt.probes.all_passed()
        || (receipt.stop.is_none() && receipt.outputs.is_empty())
        || (receipt.stop.is_some() && !receipt.outputs.is_empty())
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence receipt authority differs".to_string(),
        ));
    }
    validate_identity_list(&receipt.ordered_mounts)?;
    validate_identity_list(&receipt.outputs)?;
    let mounted_roles = receipt
        .ordered_mounts
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<BTreeSet<_>>();
    let mounted_uris = receipt
        .ordered_mounts
        .iter()
        .map(|identity| identity.uri.as_str())
        .collect::<BTreeSet<_>>();
    if receipt
        .outputs
        .iter()
        .any(|identity| mounted_roles.contains(identity.role.as_str()))
        || receipt
            .outputs
            .iter()
            .any(|identity| mounted_uris.contains(identity.uri.as_str()))
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence receipt inputs and outputs overlap".to_string(),
        ));
    }
    Ok(())
}

fn validate_manifest(manifest: &V23IncidenceManifest) -> Result<()> {
    let parent_is_valid = match manifest.phase {
        V23IncidencePhase::TreeTraining => manifest.parent_receipt_sha256.is_none(),
        _ => manifest
            .parent_receipt_sha256
            .as_deref()
            .is_some_and(|digest| valid_lower_hex(digest, 64)),
    };
    if manifest.schema != V23_INCIDENCE_MANIFEST_SCHEMA
        || manifest.claim_eligible
        || !parent_is_valid
        || !valid_lower_hex(&manifest.source_commit, 40)
        || !valid_lower_hex(&manifest.source_archive_sha256, 64)
        || manifest.index_id.is_empty()
        || manifest.dataset_id.is_empty()
        || manifest.algorithm != V23IncidenceAlgorithm::REGISTERED
        || manifest.ordered_inputs.is_empty()
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence manifest authority differs".to_string(),
        ));
    }
    validate_identity_list(&manifest.ordered_inputs)
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

pub(crate) fn canonical_v23_incidence_receipt_bytes(
    receipt: &V23IncidenceReceipt,
) -> Result<Vec<u8>> {
    validate_receipt(receipt)?;
    let value = serde_json::to_value(receipt).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence receipt serialization failed: {error}"
        ))
    })?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 incidence canonical JSON failed: {error}"))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn canonical_v23_incidence_manifest_bytes(
    manifest: &V23IncidenceManifest,
) -> Result<Vec<u8>> {
    validate_manifest(manifest)?;
    let value = serde_json::to_value(manifest).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "V23 incidence manifest serialization failed: {error}"
        ))
    })?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!("V23 incidence canonical JSON failed: {error}"))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        V23FmaBackend, V23IncidenceAlgorithm, V23IncidenceCapabilityProbes, V23IncidenceManifest,
        V23IncidenceObjectIdentity, V23IncidencePhase, V23IncidenceReceipt,
        canonical_v23_incidence_manifest_bytes, canonical_v23_incidence_receipt_bytes,
        validate_v23_incidence_identity,
    };

    fn object(role: &str, algorithm: &str, digest: &str) -> V23IncidenceObjectIdentity {
        V23IncidenceObjectIdentity {
            role: role.to_string(),
            uri: format!("file:///authority/{role}"),
            digest_algorithm: algorithm.to_string(),
            digest: digest.to_string(),
            encoded_bytes: 17,
            generation: "generation-0001".to_string(),
        }
    }

    fn receipt_fixture() -> V23IncidenceReceipt {
        V23IncidenceReceipt {
            schema: "borsuk-v23-incidence-receipt-v1".to_string(),
            claim_eligible: false,
            phase: V23IncidencePhase::TreeTraining,
            parent_receipt_sha256: None,
            executable_sha256: "11".repeat(32),
            fma_backend: V23FmaBackend::Aarch64NeonFma,
            network_namespace_inode: 91,
            ordered_mounts: vec![object("construction-manifest", "sha256", &"22".repeat(32))],
            probes: V23IncidenceCapabilityProbes {
                network_namespace_changed: true,
                host_canary_denied: true,
                network_canary_denied: true,
                allowlisted_inputs_opened: true,
                output_writable: true,
            },
            outputs: vec![object("incidence-tree", "blake3", &"33".repeat(32))],
            stop: None,
        }
    }

    fn manifest_fixture() -> V23IncidenceManifest {
        V23IncidenceManifest {
            schema: "borsuk-v23-incidence-manifest-v1".to_string(),
            claim_eligible: false,
            phase: V23IncidencePhase::TreeTraining,
            parent_receipt_sha256: None,
            source_commit: "66".repeat(20),
            source_archive_sha256: "77".repeat(32),
            index_id: "index-fixture".to_string(),
            dataset_id: "deep-image-96".to_string(),
            algorithm: V23IncidenceAlgorithm {
                dimensions: 96,
                reservoir_rows: 2_097_152,
                tree_depth: 16,
                leaf_count: 65_536,
                lloyd_iterations: 4,
                posting_caps: [512, 1024, 2048],
                probe_counts: [32, 64, 128],
                selection_width: 8,
                aggregate_recall_ppm: 975_000,
                minimum_query_recall_ppm: 800_000,
                oracle_attainment_ppm: 995_000,
            },
            ordered_inputs: vec![object("training-shard-0000", "sha256", &"88".repeat(32))],
        }
    }

    #[test]
    fn v23_incidence_authority_rejects_role_digest_length_and_phase_drift() {
        let registered = object("construction-manifest", "sha256", &"44".repeat(32));
        assert!(validate_v23_incidence_identity(&registered, &registered).is_ok());

        let mut changed = registered.clone();
        changed.role = "query-parquet".to_string();
        assert!(validate_v23_incidence_identity(&changed, &registered).is_err());

        let mut changed = registered.clone();
        changed.digest = "45".repeat(32);
        assert!(validate_v23_incidence_identity(&changed, &registered).is_err());

        let mut changed = registered.clone();
        changed.encoded_bytes += 1;
        assert!(validate_v23_incidence_identity(&changed, &registered).is_err());

        let mut changed = registered.clone();
        changed.digest_algorithm = "blake3".to_string();
        assert!(validate_v23_incidence_identity(&changed, &registered).is_err());

        let wrong_registered_algorithm =
            object("construction-manifest", "blake3", &"46".repeat(32));
        assert!(
            validate_v23_incidence_identity(
                &wrong_registered_algorithm,
                &wrong_registered_algorithm,
            )
            .is_err()
        );

        let unknown_role = object("unregistered-role", "sha256", &"47".repeat(32));
        assert!(validate_v23_incidence_identity(&unknown_role, &unknown_role).is_err());
    }

    #[test]
    fn v23_incidence_authority_receipt_binds_capability_backend_parent_and_canonical_bytes() {
        let receipt = receipt_fixture();
        let bytes = canonical_v23_incidence_receipt_bytes(&receipt).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        let mut changed = receipt.clone();
        changed.claim_eligible = true;
        assert!(canonical_v23_incidence_receipt_bytes(&changed).is_err());

        let mut changed = receipt.clone();
        changed.fma_backend = V23FmaBackend::ScalarControl;
        assert!(canonical_v23_incidence_receipt_bytes(&changed).is_err());

        let mut changed = receipt.clone();
        changed.probes.network_canary_denied = false;
        assert!(canonical_v23_incidence_receipt_bytes(&changed).is_err());

        let mut changed = receipt;
        changed.parent_receipt_sha256 = Some("55".repeat(32));
        assert!(canonical_v23_incidence_receipt_bytes(&changed).is_err());

        let mut changed = receipt_fixture();
        changed.outputs[0].role = changed.ordered_mounts[0].role.clone();
        assert!(canonical_v23_incidence_receipt_bytes(&changed).is_err());

        let mut changed = receipt_fixture();
        changed.outputs[0].uri = changed.ordered_mounts[0].uri.clone();
        assert!(canonical_v23_incidence_receipt_bytes(&changed).is_err());
    }

    #[test]
    fn v23_incidence_authority_manifest_binds_constants_inputs_and_canonical_bytes() {
        let manifest = manifest_fixture();
        let bytes = canonical_v23_incidence_manifest_bytes(&manifest).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut changed = manifest.clone();
        changed.algorithm.posting_caps = [512, 1024, 1024];
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        changed
            .ordered_inputs
            .push(changed.ordered_inputs[0].clone());
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest;
        changed.phase = V23IncidencePhase::HoldoutEvaluation;
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());
    }
}
