use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const V23_INCIDENCE_RECEIPT_SCHEMA: &str = "borsuk-v23-incidence-receipt-v1";
const V23_INCIDENCE_MANIFEST_SCHEMA: &str = "borsuk-v23-incidence-manifest-v1";
const V23_INCIDENCE_SOURCE_COMMIT: &str = "c339a546f8f9370cb2e6e9fb3b0fd4bdefa3cb05";
const V23_INCIDENCE_SOURCE_ARCHIVE_SHA256: &str =
    "77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d";
const V23_INCIDENCE_INDEX_ID: &str = "index-bcda7bb66812e162d45077e6";
const V23_INCIDENCE_DATASET_ID: &str = "deep-image-96";

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
pub(crate) enum V23IncidenceRunMode {
    Preflight,
    Execute,
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
#[serde(tag = "authority_kind", rename_all = "kebab-case")]
pub(crate) enum V23IncidenceInputAuthority {
    DatasetMeta {
        identity: V23IncidenceObjectIdentity,
        physical_schema: String,
        dimensions: u32,
        metric: String,
        train_rows: u64,
        test_rows: u64,
        neighbors_per_query: u32,
    },
    TrainingShard {
        identity: V23IncidenceObjectIdentity,
        ordinal_start: u64,
        ordinal_end: u64,
        physical_schema: String,
        dimensions: u32,
        metric: String,
        rows: u64,
    },
}

impl V23IncidenceInputAuthority {
    fn identity(&self) -> &V23IncidenceObjectIdentity {
        match self {
            Self::DatasetMeta { identity, .. } | Self::TrainingShard { identity, .. } => identity,
        }
    }
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
    pub(crate) ordered_inputs: Vec<V23IncidenceInputAuthority>,
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
    pub(crate) run_mode: V23IncidenceRunMode,
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
    let parent_is_valid = match (receipt.phase, receipt.run_mode) {
        (V23IncidencePhase::TreeTraining, V23IncidenceRunMode::Preflight) => {
            receipt.parent_receipt_sha256.is_none()
        }
        _ => receipt
            .parent_receipt_sha256
            .as_deref()
            .is_some_and(|digest| valid_lower_hex(digest, 64)),
    };
    let result_shape_is_valid = match (receipt.run_mode, receipt.stop) {
        (_, Some(_)) => receipt.outputs.is_empty(),
        (V23IncidenceRunMode::Preflight, None) => receipt.outputs.is_empty(),
        (V23IncidenceRunMode::Execute, None) => !receipt.outputs.is_empty(),
    };
    if receipt.schema != V23_INCIDENCE_RECEIPT_SCHEMA
        || receipt.claim_eligible
        || !parent_is_valid
        || !valid_lower_hex(&receipt.executable_sha256, 64)
        || receipt.fma_backend == V23FmaBackend::ScalarControl
        || receipt.network_namespace_inode == 0
        || receipt.ordered_mounts.is_empty()
        || !receipt.probes.all_passed()
        || !result_shape_is_valid
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
        || manifest.source_commit != V23_INCIDENCE_SOURCE_COMMIT
        || manifest.source_archive_sha256 != V23_INCIDENCE_SOURCE_ARCHIVE_SHA256
        || manifest.index_id != V23_INCIDENCE_INDEX_ID
        || manifest.dataset_id != V23_INCIDENCE_DATASET_ID
        || manifest.algorithm != V23IncidenceAlgorithm::REGISTERED
        || manifest.ordered_inputs.is_empty()
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence manifest authority differs".to_string(),
        ));
    }
    validate_manifest_inputs(manifest)
}

fn validate_manifest_inputs(manifest: &V23IncidenceManifest) -> Result<()> {
    if manifest.phase != V23IncidencePhase::TreeTraining || manifest.ordered_inputs.len() < 2 {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence manifest inputs differ".to_string(),
        ));
    }
    let V23IncidenceInputAuthority::DatasetMeta {
        identity,
        physical_schema,
        dimensions,
        metric,
        train_rows,
        test_rows,
        neighbors_per_query,
    } = &manifest.ordered_inputs[0]
    else {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence manifest input order differs".to_string(),
        ));
    };
    validate_object_identity(identity)?;
    if identity.role != "dataset-meta"
        || physical_schema != "deep-image-meta-v1"
        || *dimensions != 96
        || metric != "cosine"
        || *train_rows != 9_990_000
        || *test_rows != 10_000
        || *neighbors_per_query != 100
    {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence dataset authority differs".to_string(),
        ));
    }

    let mut identities = Vec::with_capacity(manifest.ordered_inputs.len());
    identities.push(identity.clone());
    let mut expected_start = 0_u64;
    for (index, input) in manifest.ordered_inputs[1..].iter().enumerate() {
        let V23IncidenceInputAuthority::TrainingShard {
            identity,
            ordinal_start,
            ordinal_end,
            physical_schema,
            dimensions,
            metric,
            rows,
        } = input
        else {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence training authority differs".to_string(),
            ));
        };
        let expected_role = format!("training-shard-{index:04}");
        if identity.role != expected_role
            || *ordinal_start != expected_start
            || *ordinal_end <= *ordinal_start
            || *rows != ordinal_end - ordinal_start
            || physical_schema != "emb:fixed-size-list<element:f32;96>:non-null"
            || *dimensions != 96
            || metric != "cosine"
        {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence training authority differs".to_string(),
            ));
        }
        validate_object_identity(identity)?;
        expected_start = *ordinal_end;
        identities.push(identity.clone());
    }
    if expected_start != *train_rows {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence training ordinal authority differs".to_string(),
        ));
    }
    validate_identity_list(&identities)
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

fn validate_object_bytes(identity: &V23IncidenceObjectIdentity, bytes: &[u8]) -> Result<()> {
    let digest = match identity.digest_algorithm.as_str() {
        "sha256" => format!("{:x}", Sha256::digest(bytes)),
        "blake3" => blake3::hash(bytes).to_hex().to_string(),
        _ => {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence digest algorithm differs".to_string(),
            ));
        }
    };
    if identity.encoded_bytes != bytes.len() as u64 || identity.digest != digest {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence object bytes differ".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn canonical_v23_incidence_receipt_bytes(
    receipt: &V23IncidenceReceipt,
    parent_receipt_bytes: Option<&[u8]>,
    output_bytes: &[(&str, &[u8])],
) -> Result<Vec<u8>> {
    validate_receipt(receipt)?;
    match (
        receipt.parent_receipt_sha256.as_deref(),
        parent_receipt_bytes,
    ) {
        (None, None) => {}
        (Some(expected), Some(bytes)) if format!("{:x}", Sha256::digest(bytes)) == expected => {}
        _ => {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence parent receipt bytes differ".to_string(),
            ));
        }
    }
    if receipt.outputs.len() != output_bytes.len() {
        return Err(BorsukError::InvalidStorage(
            "V23 incidence output count differs".to_string(),
        ));
    }
    for (identity, (role, bytes)) in receipt.outputs.iter().zip(output_bytes) {
        if identity.role != *role {
            return Err(BorsukError::InvalidStorage(
                "V23 incidence output order differs".to_string(),
            ));
        }
        validate_object_bytes(identity, bytes)?;
    }
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
    use sha2::{Digest, Sha256};

    use super::{
        V23FmaBackend, V23IncidenceAlgorithm, V23IncidenceCapabilityProbes,
        V23IncidenceInputAuthority, V23IncidenceManifest, V23IncidenceObjectIdentity,
        V23IncidencePhase, V23IncidenceReceipt, V23IncidenceRunMode,
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
        let parent_receipt_sha256 = format!("{:x}", Sha256::digest(b"preflight-receipt"));
        let tree_digest = blake3::hash(b"tree-output").to_hex().to_string();
        V23IncidenceReceipt {
            schema: "borsuk-v23-incidence-receipt-v1".to_string(),
            claim_eligible: false,
            phase: V23IncidencePhase::TreeTraining,
            run_mode: V23IncidenceRunMode::Execute,
            parent_receipt_sha256: Some(parent_receipt_sha256),
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
            outputs: vec![V23IncidenceObjectIdentity {
                encoded_bytes: 11,
                ..object("incidence-tree", "blake3", &tree_digest)
            }],
            stop: None,
        }
    }

    fn canonical_receipt(receipt: &V23IncidenceReceipt) -> crate::Result<Vec<u8>> {
        let parent_bytes = receipt
            .parent_receipt_sha256
            .as_ref()
            .map(|_| b"preflight-receipt".as_slice());
        let outputs = if receipt.outputs.is_empty() {
            Vec::new()
        } else {
            vec![("incidence-tree", b"tree-output".as_slice())]
        };
        canonical_v23_incidence_receipt_bytes(receipt, parent_bytes, &outputs)
    }

    fn manifest_fixture() -> V23IncidenceManifest {
        V23IncidenceManifest {
            schema: "borsuk-v23-incidence-manifest-v1".to_string(),
            claim_eligible: false,
            phase: V23IncidencePhase::TreeTraining,
            parent_receipt_sha256: None,
            source_commit: "c339a546f8f9370cb2e6e9fb3b0fd4bdefa3cb05".to_string(),
            source_archive_sha256:
                "77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d".to_string(),
            index_id: "index-bcda7bb66812e162d45077e6".to_string(),
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
            ordered_inputs: vec![
                V23IncidenceInputAuthority::DatasetMeta {
                    identity: object("dataset-meta", "sha256", &"88".repeat(32)),
                    physical_schema: "deep-image-meta-v1".to_string(),
                    dimensions: 96,
                    metric: "cosine".to_string(),
                    train_rows: 9_990_000,
                    test_rows: 10_000,
                    neighbors_per_query: 100,
                },
                V23IncidenceInputAuthority::TrainingShard {
                    identity: object("training-shard-0000", "sha256", &"89".repeat(32)),
                    ordinal_start: 0,
                    ordinal_end: 9_990_000,
                    physical_schema: "emb:fixed-size-list<element:f32;96>:non-null".to_string(),
                    dimensions: 96,
                    metric: "cosine".to_string(),
                    rows: 9_990_000,
                },
            ],
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
        let bytes = canonical_receipt(&receipt).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        let mut changed = receipt.clone();
        changed.claim_eligible = true;
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt.clone();
        changed.fma_backend = V23FmaBackend::ScalarControl;
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt.clone();
        changed.probes.network_canary_denied = false;
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt;
        changed.parent_receipt_sha256 = None;
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt_fixture();
        changed.run_mode = V23IncidenceRunMode::Preflight;
        changed.parent_receipt_sha256 = None;
        changed.outputs.clear();
        assert!(canonical_receipt(&changed).is_ok());

        changed
            .outputs
            .push(object("incidence-tree", "blake3", &"33".repeat(32)));
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt_fixture();
        changed.parent_receipt_sha256 = Some("56".repeat(32));
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt_fixture();
        changed.run_mode = V23IncidenceRunMode::Preflight;
        changed.parent_receipt_sha256 = Some("55".repeat(32));
        changed.outputs.clear();
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt_fixture();
        changed.outputs[0].role = changed.ordered_mounts[0].role.clone();
        assert!(canonical_receipt(&changed).is_err());

        let mut changed = receipt_fixture();
        changed.outputs[0].uri = changed.ordered_mounts[0].uri.clone();
        assert!(canonical_receipt(&changed).is_err());

        let receipt = receipt_fixture();
        assert!(
            canonical_v23_incidence_receipt_bytes(
                &receipt,
                Some(b"wrong-parent"),
                &[("incidence-tree", b"tree-output")],
            )
            .is_err()
        );
        assert!(
            canonical_v23_incidence_receipt_bytes(
                &receipt,
                Some(b"preflight-receipt"),
                &[("incidence-tree", b"wrong-tree")],
            )
            .is_err()
        );
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
        changed.source_commit = "66".repeat(20);
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        changed.index_id = "index-drift".to_string();
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        changed
            .ordered_inputs
            .push(changed.ordered_inputs[0].clone());
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        let V23IncidenceInputAuthority::TrainingShard { dimensions, .. } =
            &mut changed.ordered_inputs[1]
        else {
            panic!("fixture input differs");
        };
        *dimensions = 95;
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        let V23IncidenceInputAuthority::TrainingShard {
            ordinal_end, rows, ..
        } = &mut changed.ordered_inputs[1]
        else {
            panic!("fixture input differs");
        };
        *ordinal_end -= 1;
        *rows -= 1;
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest.clone();
        changed.ordered_inputs.swap(0, 1);
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());

        let mut changed = manifest;
        changed.phase = V23IncidencePhase::HoldoutEvaluation;
        assert!(canonical_v23_incidence_manifest_bytes(&changed).is_err());
    }
}
