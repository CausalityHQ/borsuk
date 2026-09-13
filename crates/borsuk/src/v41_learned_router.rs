use crate::error::{BorsukError, Result};
use crate::v38_boundary_spill::v38_v40_owner_rows_from_artifacts;
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, RecordBatch, UInt32Array,
    UInt64Array,
    builder::{ListBuilder, UInt32Builder},
};
use arrow_ipc::{
    MessageHeader, MetadataVersion,
    convert::fb_to_schema,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use borsuk_fma::FusedDot64;
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    io::Cursor,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::Arc,
};

const V41_QUERY_DIMENSIONS: usize = 768;
const V41_DEVELOPMENT_QUERY_COUNT: usize = 1_000;
const V41_MAXIMUM_TRAINING_QUERIES: usize = 800;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn valid_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_s3_uri(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("s3://") else {
        return false;
    };
    let Some((bucket, key)) = rest.split_once('/') else {
        return false;
    };
    !bucket.is_empty()
        && !key.is_empty()
        && !bucket.contains(['?', '#'])
        && !key.contains(['?', '#'])
}

pub(crate) fn v41_marginal_targets_from_v38_artifacts(
    relation_bytes: &[u8],
    posting_summary_bytes: &[u8],
    source_rows: usize,
    posting_count: u32,
    maximum_rows_per_posting: u32,
    gt_feature_ids: &[u64],
    selected: &[u32],
) -> Result<Vec<f32>> {
    let owners = v38_v40_owner_rows_from_artifacts(
        relation_bytes,
        posting_summary_bytes,
        source_rows,
        posting_count,
        maximum_rows_per_posting,
    )?;
    borsuk_v41::v41_marginal_targets(&owners, gt_feature_ids, selected, posting_count)
        .map_err(|error| invalid(&error.to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V41LocalRunMode {
    AuditDevelopmentValidation,
    PartitionDevelopment,
    Preflight,
    TrainDiagnostic,
    SelectDiagnostic,
    EvaluateDiagnostic,
    TrainFinal,
    SelectValidation,
    EvaluateValidation,
    AuditHoldout,
    SelectHoldout,
    EvaluateHoldout,
}

impl V41LocalRunMode {
    fn input_roles(self) -> &'static [&'static str] {
        match self {
            Self::AuditDevelopmentValidation => {
                &["freeze-receipt", "development-query", "validation-query"]
            }
            Self::PartitionDevelopment => &["development-query", "development-gt", "split-audit"],
            Self::Preflight => &["preflight-authority", "source-archive"],
            Self::TrainDiagnostic => &[
                "split-audit",
                "training-partition-receipt",
                "training-query",
                "training-gt",
                "spill-relation",
                "spill-postings",
                "source-archive",
                "binary",
            ],
            Self::SelectDiagnostic => &[
                "diagnostic-query-receipt",
                "diagnostic-query",
                "diagnostic-model",
                "source-archive",
                "binary",
            ],
            Self::EvaluateDiagnostic => &[
                "diagnostic-evaluation-receipt",
                "diagnostic-selection",
                "diagnostic-gt",
                "spill-relation",
                "spill-postings",
            ],
            Self::TrainFinal => &[
                "split-audit",
                "development-query",
                "development-gt",
                "spill-relation",
                "spill-postings",
                "diagnostic-result",
                "source-archive",
                "binary",
            ],
            Self::SelectValidation => &["validation-query", "model", "source-archive", "binary"],
            Self::EvaluateValidation => &[
                "validation-selection",
                "validation-gt",
                "spill-relation",
                "spill-postings",
            ],
            Self::AuditHoldout => &["split-audit", "holdout-query", "validation-result"],
            Self::SelectHoldout => &[
                "holdout-audit",
                "holdout-query",
                "model",
                "source-archive",
                "binary",
            ],
            Self::EvaluateHoldout => &[
                "holdout-audit",
                "holdout-selection",
                "holdout-gt",
                "spill-relation",
                "spill-postings",
            ],
        }
    }

    fn output_roles(self) -> &'static [&'static str] {
        match self {
            Self::AuditDevelopmentValidation => &["split-audit"],
            Self::PartitionDevelopment => &[
                "training-query",
                "training-gt",
                "diagnostic-query",
                "diagnostic-gt",
                "partition-manifest",
                "training-partition-receipt",
                "diagnostic-query-receipt",
                "diagnostic-evaluation-receipt",
            ],
            Self::Preflight => &["preflight-samples", "preflight-result"],
            Self::TrainDiagnostic => &["diagnostic-model", "training-state", "training-result"],
            Self::SelectDiagnostic => &["diagnostic-selection"],
            Self::EvaluateDiagnostic => &["diagnostic-result"],
            Self::TrainFinal => &["model", "training-state", "training-result"],
            Self::SelectValidation => &["validation-selection"],
            Self::EvaluateValidation => &["validation-result"],
            Self::AuditHoldout => &["holdout-audit"],
            Self::SelectHoldout => &["holdout-selection"],
            Self::EvaluateHoldout => &["holdout-result"],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V41LocalArtifact {
    role: String,
    path: PathBuf,
    uri: String,
    sha256: String,
    blake3: String,
    encoded_bytes: u64,
}

impl V41LocalArtifact {
    pub(crate) fn try_new(
        role: String,
        path: PathBuf,
        uri: String,
        sha256: String,
        blake3: String,
        encoded_bytes: u64,
    ) -> Result<Self> {
        if !valid_token(&role)
            || path.as_os_str().is_empty()
            || !valid_s3_uri(&uri)
            || !valid_digest(&sha256)
            || !valid_digest(&blake3)
            || encoded_bytes == 0
        {
            return Err(invalid("V41 local artifact identity differs"));
        }
        Ok(Self {
            role,
            path,
            uri,
            sha256,
            blake3,
            encoded_bytes,
        })
    }

    pub(crate) fn role(&self) -> &str {
        &self.role
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V41LocalOutput {
    role: String,
    path: PathBuf,
}

impl V41LocalOutput {
    pub(crate) fn try_new(role: String, path: PathBuf) -> Result<Self> {
        if !valid_token(&role) || path.as_os_str().is_empty() {
            return Err(invalid("V41 local output identity differs"));
        }
        Ok(Self { role, path })
    }

    pub(crate) fn role(&self) -> &str {
        &self.role
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V41LocalRunRequest {
    mode: V41LocalRunMode,
    inputs: Vec<V41LocalArtifact>,
    outputs: Vec<V41LocalOutput>,
    workers: u32,
}

impl V41LocalRunRequest {
    pub(crate) fn try_new(
        mode: V41LocalRunMode,
        inputs: Vec<V41LocalArtifact>,
        outputs: Vec<V41LocalOutput>,
        workers: u32,
    ) -> Result<Self> {
        let input_roles = inputs
            .iter()
            .map(V41LocalArtifact::role)
            .collect::<Vec<_>>();
        let output_roles = outputs.iter().map(V41LocalOutput::role).collect::<Vec<_>>();
        let mut paths = BTreeSet::new();
        let mut uris = BTreeSet::new();
        if input_roles != mode.input_roles()
            || output_roles != mode.output_roles()
            || !matches!(workers, 1 | 2 | 4 | 8 | 16 | 32)
            || inputs.iter().any(|input| !paths.insert(input.path()))
            || outputs.iter().any(|output| !paths.insert(output.path()))
            || inputs.iter().any(|input| !uris.insert(input.uri.as_str()))
        {
            return Err(invalid("V41 local phase capability differs"));
        }
        Ok(Self {
            mode,
            inputs,
            outputs,
            workers,
        })
    }

    pub(crate) fn input_roles(&self) -> Vec<&str> {
        self.inputs.iter().map(V41LocalArtifact::role).collect()
    }

    pub(crate) fn input_artifacts(&self) -> &[V41LocalArtifact] {
        &self.inputs
    }
}

fn validate_queries(queries: &[(u32, Vec<f32>)]) -> Result<Vec<[u8; 32]>> {
    let mut fingerprints = Vec::with_capacity(queries.len());
    for (index, (ordinal, vector)) in queries.iter().enumerate() {
        if usize::try_from(*ordinal).ok() != Some(index)
            || vector.len() != V41_QUERY_DIMENSIONS
            || vector.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("V41 query role differs"));
        }
        let mut hash = Sha256::new();
        for value in vector {
            hash.update(value.to_bits().to_le_bytes());
        }
        fingerprints.push(hash.finalize().into());
    }
    Ok(fingerprints)
}

fn fingerprint_set_sha256(fingerprints: &[[u8; 32]]) -> String {
    let mut ordered = fingerprints.to_vec();
    ordered.sort_unstable();
    let mut hash = Sha256::new();
    for fingerprint in ordered {
        hash.update(fingerprint);
    }
    format!("{:x}", hash.finalize())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V41QuerySplitAudit {
    left_role: String,
    right_role: String,
    left_rows: u32,
    right_rows: u32,
    left_vector_bits_sha256: String,
    right_vector_bits_sha256: String,
    left_fingerprints: Vec<[u8; 32]>,
    right_fingerprints: Vec<[u8; 32]>,
}

impl V41QuerySplitAudit {
    pub(crate) fn left_rows(&self) -> u32 {
        self.left_rows
    }

    pub(crate) fn right_rows(&self) -> u32 {
        self.right_rows
    }

    pub(crate) fn left_vector_bits_sha256(&self) -> &str {
        &self.left_vector_bits_sha256
    }

    pub(crate) fn right_vector_bits_sha256(&self) -> &str {
        &self.right_vector_bits_sha256
    }
}

pub(crate) fn audit_v41_query_roles(
    left_role: &str,
    left: &[(u32, Vec<f32>)],
    right_role: &str,
    right: &[(u32, Vec<f32>)],
) -> Result<V41QuerySplitAudit> {
    if !matches!(
        (left_role, right_role),
        ("development-query", "validation-query")
            | ("development-query", "holdout-query")
            | ("validation-query", "holdout-query")
    ) || left.is_empty()
        || right.is_empty()
    {
        return Err(invalid("V41 query split roles differ"));
    }
    let mut left_fingerprints = validate_queries(left)?;
    let mut right_fingerprints = validate_queries(right)?;
    left_fingerprints.sort_unstable();
    right_fingerprints.sort_unstable();
    let left_set = left_fingerprints.iter().copied().collect::<BTreeSet<_>>();
    if right_fingerprints
        .iter()
        .any(|fingerprint| left_set.contains(fingerprint))
    {
        return Err(invalid("V41 cross-role query vector is duplicated"));
    }
    Ok(V41QuerySplitAudit {
        left_role: left_role.to_owned(),
        right_role: right_role.to_owned(),
        left_rows: u32::try_from(left.len())
            .map_err(|_| invalid("V41 query split row count differs"))?,
        right_rows: u32::try_from(right.len())
            .map_err(|_| invalid("V41 query split row count differs"))?,
        left_vector_bits_sha256: fingerprint_set_sha256(&left_fingerprints),
        right_vector_bits_sha256: fingerprint_set_sha256(&right_fingerprints),
        left_fingerprints,
        right_fingerprints,
    })
}

pub(crate) fn audit_v41_holdout_query_role(
    prior: &V41QuerySplitAudit,
    holdout: &[(u32, Vec<f32>)],
) -> Result<V41QuerySplitAudit> {
    if prior.left_role != "development-query" || prior.right_role != "validation-query" {
        return Err(invalid("V41 holdout predecessor audit differs"));
    }
    let mut holdout_fingerprints = validate_queries(holdout)?;
    holdout_fingerprints.sort_unstable();
    let prior_fingerprints = prior
        .left_fingerprints
        .iter()
        .chain(&prior.right_fingerprints)
        .copied()
        .collect::<BTreeSet<_>>();
    if holdout_fingerprints
        .iter()
        .any(|fingerprint| prior_fingerprints.contains(fingerprint))
    {
        return Err(invalid("V41 holdout query vector is duplicated"));
    }
    Ok(V41QuerySplitAudit {
        left_role: "development-validation-query-set".to_owned(),
        right_role: "holdout-query".to_owned(),
        left_rows: prior
            .left_rows
            .checked_add(prior.right_rows)
            .ok_or_else(|| invalid("V41 query split row count differs"))?,
        right_rows: u32::try_from(holdout.len())
            .map_err(|_| invalid("V41 query split row count differs"))?,
        left_vector_bits_sha256: fingerprint_set_sha256(
            &prior_fingerprints.iter().copied().collect::<Vec<_>>(),
        ),
        right_vector_bits_sha256: fingerprint_set_sha256(&holdout_fingerprints),
        left_fingerprints: prior_fingerprints.into_iter().collect(),
        right_fingerprints: holdout_fingerprints,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V41DevelopmentSplit {
    training_ordinals: Vec<u32>,
    diagnostic_ordinals: Vec<u32>,
}

impl V41DevelopmentSplit {
    pub(crate) fn training_ordinals(&self) -> &[u32] {
        &self.training_ordinals
    }

    pub(crate) fn diagnostic_ordinals(&self) -> &[u32] {
        &self.diagnostic_ordinals
    }
}

pub(crate) fn split_v41_development_queries(
    queries: &[(u32, Vec<f32>)],
) -> Result<V41DevelopmentSplit> {
    if queries.len() != V41_DEVELOPMENT_QUERY_COUNT {
        return Err(invalid("V41 development query count differs"));
    }
    validate_queries(queries)?;
    let mut groups = BTreeMap::<[u8; 32], Vec<u32>>::new();
    for (ordinal, vector) in queries {
        let mut hash = Sha256::new();
        hash.update(b"borsuk-v41-development-split-v1");
        for value in vector {
            hash.update(value.to_bits().to_le_bytes());
        }
        groups
            .entry(hash.finalize().into())
            .or_default()
            .push(*ordinal);
    }
    let mut groups = groups.into_iter().collect::<Vec<_>>();
    groups.sort_unstable_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1[0].cmp(&right.1[0]))
    });
    let mut training_ordinals = Vec::with_capacity(V41_MAXIMUM_TRAINING_QUERIES);
    let mut diagnostic_ordinals = Vec::new();
    for (_, ordinals) in groups {
        if training_ordinals.len() + ordinals.len() <= V41_MAXIMUM_TRAINING_QUERIES {
            training_ordinals.extend(ordinals);
        } else {
            diagnostic_ordinals.extend(ordinals);
        }
    }
    training_ordinals.sort_unstable();
    diagnostic_ordinals.sort_unstable();
    if training_ordinals.is_empty()
        || diagnostic_ordinals.is_empty()
        || training_ordinals.len() + diagnostic_ordinals.len() != queries.len()
    {
        return Err(invalid("V41 development split differs"));
    }
    Ok(V41DevelopmentSplit {
        training_ordinals,
        diagnostic_ordinals,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V41ArtifactIdentity {
    role: String,
    uri: String,
    sha256: String,
    blake3: String,
    encoded_bytes: u64,
}

impl V41ArtifactIdentity {
    pub(crate) fn try_new(
        role: String,
        uri: String,
        sha256: String,
        blake3: String,
        encoded_bytes: u64,
    ) -> Result<Self> {
        if !valid_token(&role)
            || !valid_s3_uri(&uri)
            || !valid_digest(&sha256)
            || !valid_digest(&blake3)
            || encoded_bytes == 0
        {
            return Err(invalid("V41 artifact identity differs"));
        }
        Ok(Self {
            role,
            uri,
            sha256,
            blake3,
            encoded_bytes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V41PartitionChildAuthority {
    uri: String,
    sha256: String,
    blake3: String,
    encoded_bytes: u64,
    parent_query: V41ArtifactIdentity,
    parent_gt: V41ArtifactIdentity,
    split_sha256: String,
}

impl V41PartitionChildAuthority {
    pub(crate) fn try_new(
        uri: String,
        sha256: String,
        blake3: String,
        encoded_bytes: u64,
        parent_query: V41ArtifactIdentity,
        parent_gt: V41ArtifactIdentity,
        split_sha256: String,
    ) -> Result<Self> {
        if !valid_s3_uri(&uri)
            || !valid_digest(&sha256)
            || !valid_digest(&blake3)
            || !valid_digest(&split_sha256)
            || encoded_bytes == 0
            || parent_query.role != "development-query"
            || parent_gt.role != "development-gt"
        {
            return Err(invalid("V41 partition child authority differs"));
        }
        Ok(Self {
            uri,
            sha256,
            blake3,
            encoded_bytes,
            parent_query,
            parent_gt,
            split_sha256,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V41DevelopmentPartitionChild {
    role: String,
    authority: V41PartitionChildAuthority,
    original_query_ordinals: Vec<u32>,
}

impl V41DevelopmentPartitionChild {
    pub(crate) fn try_new(
        role: String,
        authority: V41PartitionChildAuthority,
        original_query_ordinals: Vec<u32>,
    ) -> Result<Self> {
        if !matches!(
            role.as_str(),
            "training-query" | "training-gt" | "diagnostic-query" | "diagnostic-gt"
        ) || original_query_ordinals.is_empty()
            || !original_query_ordinals
                .windows(2)
                .all(|window| window[0] < window[1])
        {
            return Err(invalid("V41 partition child differs"));
        }
        Ok(Self {
            role,
            authority,
            original_query_ordinals,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V41DevelopmentPartitionManifest {
    children: Vec<V41DevelopmentPartitionChild>,
}

impl V41DevelopmentPartitionManifest {
    pub(crate) fn try_new(children: Vec<V41DevelopmentPartitionChild>) -> Result<Self> {
        let roles = children
            .iter()
            .map(|child| child.role.as_str())
            .collect::<Vec<_>>();
        if roles
            != [
                "training-query",
                "training-gt",
                "diagnostic-query",
                "diagnostic-gt",
            ]
            || children[0].original_query_ordinals != children[1].original_query_ordinals
            || children[2].original_query_ordinals != children[3].original_query_ordinals
        {
            return Err(invalid("V41 partition child roles differ"));
        }
        Ok(Self { children })
    }

    pub(crate) fn children(&self) -> &[V41DevelopmentPartitionChild] {
        &self.children
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V41DevelopmentPartitionProjection {
    role: String,
    manifest_sha256: String,
    children: Vec<V41ProjectedPartitionChild>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct V41ProjectedPartitionChild {
    role: String,
    uri: String,
    sha256: String,
    blake3: String,
    encoded_bytes: u64,
    original_query_ordinals: Vec<u32>,
}

impl From<&V41DevelopmentPartitionChild> for V41ProjectedPartitionChild {
    fn from(child: &V41DevelopmentPartitionChild) -> Self {
        Self {
            role: child.role.clone(),
            uri: child.authority.uri.clone(),
            sha256: child.authority.sha256.clone(),
            blake3: child.authority.blake3.clone(),
            encoded_bytes: child.authority.encoded_bytes,
            original_query_ordinals: child.original_query_ordinals.clone(),
        }
    }
}

impl V41DevelopmentPartitionProjection {
    pub(crate) fn child_roles(&self) -> Vec<&str> {
        self.children
            .iter()
            .map(|child| child.role.as_str())
            .collect()
    }
}

pub(crate) fn project_v41_development_partition(
    manifest: &V41DevelopmentPartitionManifest,
    manifest_sha256: &str,
    role: &str,
) -> Result<V41DevelopmentPartitionProjection> {
    if !valid_digest(manifest_sha256) {
        return Err(invalid("V41 partition projection authority differs"));
    }
    let children = match role {
        "training-partition-receipt" => manifest.children[0..2]
            .iter()
            .map(V41ProjectedPartitionChild::from)
            .collect(),
        "diagnostic-query-receipt" => manifest.children[2..3]
            .iter()
            .map(V41ProjectedPartitionChild::from)
            .collect(),
        "diagnostic-evaluation-receipt" => manifest.children[3..4]
            .iter()
            .map(V41ProjectedPartitionChild::from)
            .collect(),
        _ => return Err(invalid("V41 partition projection role differs")),
    };
    Ok(V41DevelopmentPartitionProjection {
        role: role.to_owned(),
        manifest_sha256: manifest_sha256.to_owned(),
        children,
    })
}

pub(crate) fn validate_v41_development_partition(
    manifest: &V41DevelopmentPartitionManifest,
    total_queries: u32,
) -> Result<()> {
    let children = &manifest.children;
    let first = &children[0];
    if children.iter().any(|child| {
        child.authority.parent_query != first.authority.parent_query
            || child.authority.parent_gt != first.authority.parent_gt
            || child.authority.split_sha256 != first.authority.split_sha256
    }) || children[0].original_query_ordinals != children[1].original_query_ordinals
        || children[2].original_query_ordinals != children[3].original_query_ordinals
    {
        return Err(invalid("V41 partition binding differs"));
    }
    let mut ordinals = children[0].original_query_ordinals.clone();
    ordinals.extend_from_slice(&children[2].original_query_ordinals);
    ordinals.sort_unstable();
    if ordinals != (0..total_queries).collect::<Vec<_>>() {
        return Err(invalid("V41 partition ordinal coverage differs"));
    }
    let mut uris = BTreeSet::new();
    let mut digests = BTreeSet::new();
    if children.iter().any(|child| {
        !uris.insert(child.authority.uri.as_str())
            || !digests.insert((
                child.authority.sha256.as_str(),
                child.authority.blake3.as_str(),
                child.authority.encoded_bytes,
            ))
    }) {
        return Err(invalid("V41 partition child identity differs"));
    }
    Ok(())
}

pub(crate) fn validate_v41_development_partition_against(
    manifest: &V41DevelopmentPartitionManifest,
    expected_parent_query: &V41ArtifactIdentity,
    expected_parent_gt: &V41ArtifactIdentity,
    expected_split_sha256: &str,
    expected_training_ordinals: &[u32],
    expected_diagnostic_ordinals: &[u32],
) -> Result<()> {
    let total_queries = expected_training_ordinals
        .len()
        .checked_add(expected_diagnostic_ordinals.len())
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| invalid("V41 partition expected row count differs"))?;
    validate_v41_development_partition(manifest, total_queries)?;
    if !valid_digest(expected_split_sha256)
        || manifest.children.iter().any(|child| {
            child.authority.parent_query != *expected_parent_query
                || child.authority.parent_gt != *expected_parent_gt
                || child.authority.split_sha256 != expected_split_sha256
        })
        || manifest.children[0].original_query_ordinals != expected_training_ordinals
        || manifest.children[1].original_query_ordinals != expected_training_ordinals
        || manifest.children[2].original_query_ordinals != expected_diagnostic_ordinals
        || manifest.children[3].original_query_ordinals != expected_diagnostic_ordinals
    {
        return Err(invalid("V41 partition expected authority differs"));
    }
    Ok(())
}

const V41_MODEL_WIDTH: usize = 64;
const V41_MODEL_QUERY_DIMENSIONS: usize = 768;
const V41_MODEL_MANIFEST_SCHEMA: &str = "borsuk-v41-model-manifest-v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V41ModelIdentity {
    role: String,
    uri: String,
    sha256: String,
    blake3: String,
    encoded_bytes: u64,
}

impl From<&V41ArtifactIdentity> for V41ModelIdentity {
    fn from(identity: &V41ArtifactIdentity) -> Self {
        Self {
            role: identity.role.clone(),
            uri: identity.uri.clone(),
            sha256: identity.sha256.clone(),
            blake3: identity.blake3.clone(),
            encoded_bytes: identity.encoded_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V41ModelAuthority {
    backend: String,
    page_count: u32,
    source: V41ArtifactIdentity,
    source_archive: V41ArtifactIdentity,
    index: V41ArtifactIdentity,
    training_split_sha256: String,
}

impl V41ModelAuthority {
    pub(crate) fn try_new(
        backend: String,
        page_count: u32,
        source: V41ArtifactIdentity,
        source_archive: V41ArtifactIdentity,
        index: V41ArtifactIdentity,
        training_split_sha256: String,
    ) -> Result<Self> {
        let detected = FusedDot64::detect()
            .map_err(|_| invalid("V41 fused model backend is unavailable"))?
            .backend()
            .as_str();
        if backend != detected
            || page_count == 0
            || source.role != "source"
            || source_archive.role != "source-archive"
            || index.role != "index"
            || !valid_digest(&training_split_sha256)
        {
            return Err(invalid("V41 model authority differs"));
        }
        Ok(Self {
            backend,
            page_count,
            source,
            source_archive,
            index,
            training_split_sha256,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V41ModelManifest {
    schema: String,
    backend: String,
    page_count: u32,
    source: V41ModelIdentity,
    source_archive: V41ModelIdentity,
    index: V41ModelIdentity,
    training_split_sha256: String,
    tensor_shapes: BTreeMap<String, Vec<u64>>,
    model_sha256: String,
}

pub(crate) struct V41EncodedModel {
    arrow_bytes: Vec<u8>,
    manifest_bytes: Vec<u8>,
}

impl V41EncodedModel {
    pub(crate) fn arrow_bytes(&self) -> &[u8] {
        &self.arrow_bytes
    }

    pub(crate) fn manifest_bytes(&self) -> &[u8] {
        &self.manifest_bytes
    }
}

fn v41_model_tensor_shapes(page_count: u32) -> BTreeMap<String, Vec<u64>> {
    BTreeMap::from([
        ("b".to_owned(), vec![64]),
        ("page_bias".to_owned(), vec![u64::from(page_count)]),
        (
            "page_embeddings".to_owned(),
            vec![u64::from(page_count), 64],
        ),
        ("w_q".to_owned(), vec![64, 768]),
        ("w_s".to_owned(), vec![64, 64]),
    ])
}

fn v41_model_schema(page_count: u32) -> Result<Schema> {
    let page_count = usize::try_from(page_count)
        .map_err(|_| invalid("V41 model page count conversion differs"))?;
    let field = |name: &str, length: usize| -> Result<Field> {
        let length =
            i32::try_from(length).map_err(|_| invalid("V41 model tensor length overflows"))?;
        Ok(Field::new(
            name,
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                length,
            ),
            false,
        ))
    };
    Ok(Schema::new(vec![
        field("w_q", V41_MODEL_WIDTH * V41_MODEL_QUERY_DIMENSIONS)?,
        field("b", V41_MODEL_WIDTH)?,
        field("w_s", V41_MODEL_WIDTH * V41_MODEL_WIDTH)?,
        field("page_embeddings", page_count * V41_MODEL_WIDTH)?,
        field("page_bias", page_count)?,
    ]))
}

fn v41_tensor_array(field: &Field, values: &[f32]) -> Result<Arc<dyn Array>> {
    let (child, length) = match field.data_type() {
        DataType::FixedSizeList(child, length) => (Arc::clone(child), *length),
        _ => return Err(invalid("V41 model Arrow schema differs")),
    };
    Ok(Arc::new(FixedSizeListArray::try_new(
        child,
        length,
        Arc::new(Float32Array::from(values.to_vec())),
        None,
    )?))
}

fn v41_model_manifest(authority: &V41ModelAuthority, model_sha256: String) -> V41ModelManifest {
    V41ModelManifest {
        schema: V41_MODEL_MANIFEST_SCHEMA.to_owned(),
        backend: authority.backend.clone(),
        page_count: authority.page_count,
        source: (&authority.source).into(),
        source_archive: (&authority.source_archive).into(),
        index: (&authority.index).into(),
        training_split_sha256: authority.training_split_sha256.clone(),
        tensor_shapes: v41_model_tensor_shapes(authority.page_count),
        model_sha256,
    }
}

fn v41_canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    fn canonical(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(values) => {
                let mut values = values.into_iter().collect::<Vec<_>>();
                values.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                serde_json::Value::Object(
                    values
                        .into_iter()
                        .map(|(key, value)| (key, canonical(value)))
                        .collect(),
                )
            }
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.into_iter().map(canonical).collect())
            }
            value => value,
        }
    }
    let value = serde_json::to_value(value)
        .map_err(|_| invalid("V41 model manifest serialization differs"))?;
    let mut bytes = serde_json::to_vec(&canonical(value))
        .map_err(|_| invalid("V41 model manifest serialization differs"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn encode_v41_model(
    model: &borsuk_v41::V41ResidualModel,
    authority: &V41ModelAuthority,
) -> Result<V41EncodedModel> {
    if model.page_count()
        != usize::try_from(authority.page_count)
            .map_err(|_| invalid("V41 model page count conversion differs"))?
    {
        return Err(invalid("V41 model page count differs"));
    }
    let schema = Arc::new(v41_model_schema(authority.page_count)?);
    let tensors = model.tensors();
    let columns = [
        tensors.w_q,
        tensors.bias,
        tensors.w_s,
        tensors.page_embeddings,
        tensors.page_bias,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, values)| v41_tensor_array(schema.field(index), values))
    .collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut arrow_bytes = Vec::new();
    let mut writer = FileWriter::try_new_with_options(&mut arrow_bytes, schema.as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    let model_sha256 = format!("{:x}", Sha256::digest(&arrow_bytes));
    let manifest_bytes = v41_canonical_json_bytes(&v41_model_manifest(authority, model_sha256))?;
    Ok(V41EncodedModel {
        arrow_bytes,
        manifest_bytes,
    })
}

fn v41_parse_arrow_message(stored: &[u8]) -> Result<arrow_ipc::Message<'_>> {
    let invalid_arrow = || invalid("V41 model Arrow framing differs");
    if stored.len() < 4 {
        return Err(invalid_arrow());
    }
    let continuation = stored[..4] == [255; 4];
    let prefix = if continuation { 8 } else { 4 };
    if stored.len() < prefix {
        return Err(invalid_arrow());
    }
    let length_offset = if continuation { 4 } else { 0 };
    let length = usize::try_from(u32::from_le_bytes(
        stored[length_offset..length_offset + 4]
            .try_into()
            .map_err(|_| invalid_arrow())?,
    ))
    .map_err(|_| invalid_arrow())?;
    let end = prefix.checked_add(length).ok_or_else(invalid_arrow)?;
    if end != stored.len() {
        return Err(invalid_arrow());
    }
    arrow_ipc::root_as_message(&stored[prefix..end]).map_err(|_| invalid_arrow())
}

fn validate_v41_arrow_file(
    bytes: &[u8],
    expected_schema: &Schema,
    maximum_values: usize,
) -> Result<()> {
    let invalid_arrow = || invalid("V41 model Arrow framing differs");
    if bytes.len() < 18 || !bytes.starts_with(b"ARROW1") || !bytes.ends_with(b"ARROW1") {
        return Err(invalid_arrow());
    }
    let trailer = bytes.len() - 10;
    let footer_length = usize::try_from(u32::from_le_bytes(
        bytes[trailer..trailer + 4]
            .try_into()
            .map_err(|_| invalid_arrow())?,
    ))
    .map_err(|_| invalid_arrow())?;
    let footer_start = trailer
        .checked_sub(footer_length)
        .ok_or_else(invalid_arrow)?;
    let footer =
        arrow_ipc::root_as_footer(&bytes[footer_start..trailer]).map_err(|_| invalid_arrow())?;
    if footer
        .dictionaries()
        .is_some_and(|dictionaries| !dictionaries.is_empty())
    {
        return Err(invalid_arrow());
    }
    if footer.version() != MetadataVersion::V5 {
        return Err(invalid_arrow());
    }
    let footer_schema = footer.schema().ok_or_else(invalid_arrow)?;
    let decoded_footer_schema = catch_unwind(AssertUnwindSafe(|| fb_to_schema(footer_schema)))
        .map_err(|_| invalid_arrow())?;
    if &decoded_footer_schema != expected_schema {
        return Err(invalid_arrow());
    }
    let blocks = footer.recordBatches().ok_or_else(invalid_arrow)?;
    if blocks.len() != 1 {
        return Err(invalid_arrow());
    }
    let block = blocks.get(0);
    let offset = usize::try_from(block.offset()).map_err(|_| invalid_arrow())?;
    let metadata = usize::try_from(block.metaDataLength()).map_err(|_| invalid_arrow())?;
    let body = usize::try_from(block.bodyLength()).map_err(|_| invalid_arrow())?;
    let referenced_end = offset
        .checked_add(metadata)
        .and_then(|value| value.checked_add(body))
        .ok_or_else(invalid_arrow)?;
    if offset < 16
        || metadata < 8
        || bytes.get(referenced_end..footer_start) != Some(&[255, 255, 255, 255, 0, 0, 0, 0])
    {
        return Err(invalid_arrow());
    }

    let schema_message = v41_parse_arrow_message(&bytes[8..offset])?;
    if schema_message.header_type() != MessageHeader::Schema || schema_message.bodyLength() != 0 {
        return Err(invalid_arrow());
    }
    let leading_schema = schema_message
        .header_as_schema()
        .ok_or_else(invalid_arrow)?;
    let decoded_leading_schema = catch_unwind(AssertUnwindSafe(|| fb_to_schema(leading_schema)))
        .map_err(|_| invalid_arrow())?;
    if decoded_leading_schema != decoded_footer_schema {
        return Err(invalid_arrow());
    }

    let metadata_end = offset.checked_add(metadata).ok_or_else(invalid_arrow)?;
    let body_end = metadata_end.checked_add(body).ok_or_else(invalid_arrow)?;
    if body_end != referenced_end {
        return Err(invalid_arrow());
    }
    let batch_message = v41_parse_arrow_message(&bytes[offset..metadata_end])?;
    if batch_message.header_type() != MessageHeader::RecordBatch
        || usize::try_from(batch_message.bodyLength()).ok() != Some(body)
    {
        return Err(invalid_arrow());
    }
    let batch = batch_message
        .header_as_record_batch()
        .ok_or_else(invalid_arrow)?;
    if batch.length() != 1
        || batch.compression().is_some()
        || batch.variadicBufferCounts().is_some()
    {
        return Err(invalid_arrow());
    }
    let nodes = batch.nodes().ok_or_else(invalid_arrow)?;
    if nodes.len() != 10
        || nodes.iter().any(|node| {
            node.length() < 0
                || node.null_count() != 0
                || usize::try_from(node.length()).map_or(true, |length| length > maximum_values)
        })
    {
        return Err(invalid_arrow());
    }
    let buffers = batch.buffers().ok_or_else(invalid_arrow)?;
    if buffers.len() != 15
        || buffers.iter().any(|buffer| {
            buffer.offset() < 0
                || buffer.length() < 0
                || usize::try_from(buffer.offset())
                    .ok()
                    .zip(usize::try_from(buffer.length()).ok())
                    .and_then(|(offset, length)| offset.checked_add(length))
                    .is_none_or(|end| end > body)
        })
    {
        return Err(invalid_arrow());
    }
    Ok(())
}

fn v41_tensor_values(batch: &RecordBatch, index: usize) -> Result<Vec<f32>> {
    let list = batch
        .column(index)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V41 model Arrow tensor type differs"))?;
    if list.null_count() != 0 || list.len() != 1 {
        return Err(invalid("V41 model Arrow tensor nullability differs"));
    }
    let values = list.value(0);
    let values = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V41 model Arrow tensor value type differs"))?;
    if values.null_count() != 0 {
        return Err(invalid("V41 model Arrow tensor value nullability differs"));
    }
    Ok(values.values().to_vec())
}

pub(crate) fn decode_v41_model(
    arrow_bytes: &[u8],
    manifest_bytes: &[u8],
    authority: &V41ModelAuthority,
) -> Result<borsuk_v41::V41ResidualModel> {
    let raw_bytes = borsuk_v41::v41_parameter_bytes(u64::from(authority.page_count))
        .map_err(|error| invalid(&format!("V41 model parameter bytes differ: {error}")))?;
    let arrow_length =
        u64::try_from(arrow_bytes.len()).map_err(|_| invalid("V41 model Arrow length differs"))?;
    if arrow_length < raw_bytes
        || arrow_length
            > raw_bytes
                .checked_add(131_072)
                .ok_or_else(|| invalid("V41 model Arrow bound overflows"))?
        || manifest_bytes.is_empty()
        || manifest_bytes.len() > 8_192
    {
        return Err(invalid("V41 model artifact bound differs"));
    }
    let expected_schema = v41_model_schema(authority.page_count)?;
    let maximum_values =
        usize::try_from(raw_bytes / 4).map_err(|_| invalid("V41 model value bound differs"))?;
    validate_v41_arrow_file(arrow_bytes, &expected_schema, maximum_values)?;
    let manifest: V41ModelManifest = serde_json::from_slice(manifest_bytes)
        .map_err(|_| invalid("V41 model manifest differs"))?;
    if v41_canonical_json_bytes(&manifest)? != manifest_bytes
        || manifest != v41_model_manifest(authority, format!("{:x}", Sha256::digest(arrow_bytes)))
    {
        return Err(invalid("V41 model manifest authority differs"));
    }
    let mut reader = catch_unwind(AssertUnwindSafe(|| {
        FileReader::try_new(Cursor::new(arrow_bytes), None)
    }))
    .map_err(|_| invalid("V41 model Arrow reader panicked"))??;
    if reader.schema().as_ref() != &expected_schema || reader.num_batches() != 1 {
        return Err(invalid("V41 model Arrow schema differs"));
    }
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| invalid("V41 model Arrow batch differs"))?;
    if reader.next().is_some() || batch.num_rows() != 1 || batch.num_columns() != 5 {
        return Err(invalid("V41 model Arrow batch count differs"));
    }
    borsuk_v41::V41ResidualModel::try_new(
        v41_tensor_values(&batch, 0)?,
        v41_tensor_values(&batch, 1)?,
        v41_tensor_values(&batch, 2)?,
        v41_tensor_values(&batch, 3)?,
        v41_tensor_values(&batch, 4)?,
    )
    .map_err(|error| invalid(&format!("V41 decoded model differs: {error}")))
}

const V41_TRAINING_STATE_SCHEMA: &str = "borsuk-v41-training-state-v1";
const V41_TRAINING_STATE_ROW_GROUP_ROWS: usize = 4_096;

fn v41_training_state_schema() -> Arc<Schema> {
    let list = DataType::List(Arc::new(Field::new("element", DataType::UInt32, false)));
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("epoch", DataType::UInt32, false),
            Field::new("state_ordinal", DataType::UInt32, false),
            Field::new("query_ordinal", DataType::UInt32, false),
            Field::new("rollout_step", DataType::UInt32, false),
            Field::new("selected_page", DataType::UInt32, false),
            Field::new("ordered_prefix", list.clone(), false),
            Field::new("ascending_membership", list, false),
            Field::new("nonzero_target_pages", DataType::UInt32, false),
            Field::new("target_sum_bits", DataType::UInt32, false),
            Field::new("loss_present", DataType::Boolean, false),
            Field::new("loss_bits", DataType::UInt32, false),
            Field::new("optimizer_step_before", DataType::UInt64, false),
            Field::new("optimizer_step_after", DataType::UInt64, false),
            Field::new("numerical_stop_count", DataType::UInt32, false),
        ],
        HashMap::from([("schema".to_owned(), V41_TRAINING_STATE_SCHEMA.to_owned())]),
    ))
}

fn v41_u32_lists<'a>(values: impl Iterator<Item = &'a [u32]>) -> ArrayRef {
    let child = Arc::new(Field::new("element", DataType::UInt32, false));
    let mut builder = ListBuilder::new(UInt32Builder::new()).with_field(child);
    for value in values {
        builder.values().append_slice(value);
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn v41_training_state_batch(records: &[borsuk_v41::V41TrainingRecord]) -> Result<RecordBatch> {
    let schema = v41_training_state_schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt32Array::from_iter_values(
                records.iter().map(|record| record.epoch),
            )),
            Arc::new(UInt32Array::from_iter_values(
                records.iter().map(|record| record.state_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                records.iter().map(|record| record.query_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                records.iter().map(|record| record.rollout_step),
            )),
            Arc::new(UInt32Array::from_iter_values(
                records.iter().map(|record| record.selected_page),
            )),
            v41_u32_lists(
                records
                    .iter()
                    .map(|record| record.ordered_prefix.as_slice()),
            ),
            v41_u32_lists(
                records
                    .iter()
                    .map(|record| record.ascending_membership.as_slice()),
            ),
            Arc::new(UInt32Array::from_iter_values(
                records.iter().map(|record| record.nonzero_target_pages),
            )),
            Arc::new(UInt32Array::from_iter_values(
                records.iter().map(|record| record.target_sum_bits),
            )),
            Arc::new(BooleanArray::from(
                records
                    .iter()
                    .map(|record| record.loss_bits.is_some())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from_iter_values(
                records
                    .iter()
                    .map(|record| record.loss_bits.unwrap_or(0.0_f32.to_bits())),
            )),
            Arc::new(UInt64Array::from_iter_values(
                records.iter().map(|record| record.optimizer_step_before),
            )),
            Arc::new(UInt64Array::from_iter_values(
                records.iter().map(|record| record.optimizer_step_after),
            )),
            Arc::new(UInt32Array::from_iter_values(
                records.iter().map(|record| record.numerical_stop_count),
            )),
        ],
    )
    .map_err(Into::into)
}

fn v41_validate_training_record(
    record: &borsuk_v41::V41TrainingRecord,
    prior: Option<(u32, u32)>,
) -> Result<()> {
    let key_is_next = match prior {
        None => (record.epoch, record.state_ordinal) == (0, 0),
        Some((epoch, state)) => {
            (record.epoch == epoch && state.checked_add(1) == Some(record.state_ordinal))
                || (epoch.checked_add(1) == Some(record.epoch) && record.state_ordinal == 0)
        }
    };
    let mut membership = record.ordered_prefix.clone();
    membership.sort_unstable();
    let target_sum = f32::from_bits(record.target_sum_bits);
    let loss = record.loss_bits.map(f32::from_bits);
    if !key_is_next
        || record.rollout_step > 20
        || record.ordered_prefix.len() != record.rollout_step as usize
        || record.ordered_prefix.iter().collect::<BTreeSet<_>>().len()
            != record.ordered_prefix.len()
        || record.ordered_prefix.contains(&record.selected_page)
        || record.ascending_membership != membership
        || record
            .ascending_membership
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || !target_sum.is_finite()
        || target_sum.to_bits() == (-0.0_f32).to_bits()
        || target_sum < 0.0
        || (target_sum == 0.0) != (record.nonzero_target_pages == 0)
        || (target_sum == 0.0) != record.loss_bits.is_none()
        || loss.is_some_and(|value| {
            !value.is_finite() || value.to_bits() == (-0.0_f32).to_bits() || value < 0.0
        })
        || record
            .optimizer_step_after
            .checked_sub(record.optimizer_step_before)
            .is_none_or(|advance| advance > 1)
        || (record.loss_bits.is_some()
            && record.optimizer_step_before.checked_add(1) != Some(record.optimizer_step_after))
        || record.numerical_stop_count != 0
    {
        return Err(invalid("V41 training-state record differs"));
    }
    Ok(())
}

pub(crate) struct V41TrainingStateParquetSink {
    path: PathBuf,
    writer: Option<ArrowWriter<fs::File>>,
    buffered: Vec<borsuk_v41::V41TrainingRecord>,
    prior: Option<(u32, u32)>,
}

impl V41TrainingStateParquetSink {
    pub(crate) fn try_new(path: &Path) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|source| BorsukError::Io {
                path: path.to_owned(),
                source,
            })?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(false)
            .set_max_row_group_row_count(Some(V41_TRAINING_STATE_ROW_GROUP_ROWS))
            .build();
        let writer = ArrowWriter::try_new(file, v41_training_state_schema(), Some(properties))?;
        Ok(Self {
            path: path.to_owned(),
            writer: Some(writer),
            buffered: Vec::with_capacity(V41_TRAINING_STATE_ROW_GROUP_ROWS),
            prior: None,
        })
    }

    fn flush(&mut self) -> Result<()> {
        if self.buffered.is_empty() {
            return Ok(());
        }
        let batch = v41_training_state_batch(&self.buffered)?;
        self.writer
            .as_mut()
            .ok_or_else(|| invalid("V41 training-state writer is closed"))?
            .write(&batch)?;
        self.buffered.clear();
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        self.flush()?;
        self.writer
            .take()
            .ok_or_else(|| invalid("V41 training-state writer is closed"))?
            .close()?;
        fs::OpenOptions::new()
            .write(true)
            .open(&self.path)
            .and_then(|file| file.sync_all())
            .map_err(|source| BorsukError::Io {
                path: self.path.clone(),
                source,
            })?;
        Ok(())
    }
}

impl borsuk_v41::V41TrainingSink for V41TrainingStateParquetSink {
    fn record(&mut self, record: borsuk_v41::V41TrainingRecord) -> borsuk_v41::Result<()> {
        v41_validate_training_record(&record, self.prior)
            .map_err(|error| borsuk_v41::V41Error::new(error.to_string()))?;
        self.prior = Some((record.epoch, record.state_ordinal));
        self.buffered.push(record);
        if self.buffered.len() == V41_TRAINING_STATE_ROW_GROUP_ROWS {
            self.flush()
                .map_err(|error| borsuk_v41::V41Error::new(error.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        V41ArtifactIdentity, V41DevelopmentPartitionChild, V41DevelopmentPartitionManifest,
        V41LocalArtifact, V41LocalOutput, V41LocalRunMode, V41LocalRunRequest, V41ModelAuthority,
        V41ModelManifest, V41PartitionChildAuthority, V41TrainingStateParquetSink,
        audit_v41_holdout_query_role, audit_v41_query_roles, decode_v41_model, encode_v41_model,
        project_v41_development_partition, split_v41_development_queries, v41_canonical_json_bytes,
        v41_marginal_targets_from_v38_artifacts, v41_parse_arrow_message,
        validate_v41_development_partition, validate_v41_development_partition_against,
    };
    use crate::v38_boundary_spill::{
        V38SpillRecord, encode_v38_posting_summary_parquet, encode_v38_spill_relation_parquet,
        summarize_v38_spill_relation,
    };
    use sha2::{Digest, Sha256};
    use std::{collections::BTreeMap, path::PathBuf};

    const SHA_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SHA_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SHA_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const SHA_D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

    fn artifact(role: &str) -> V41LocalArtifact {
        V41LocalArtifact::try_new(
            role.to_owned(),
            PathBuf::from(format!("/tmp/v41-{role}")),
            format!("s3://borsuk-test/v41/{role}"),
            SHA_A.to_owned(),
            SHA_B.to_owned(),
            64,
        )
        .unwrap()
    }

    fn output(role: &str) -> V41LocalOutput {
        V41LocalOutput::try_new(
            role.to_owned(),
            PathBuf::from(format!("/tmp/v41-output-{role}")),
        )
        .unwrap()
    }

    fn query(ordinal: u32, marker: u32) -> (u32, Vec<f32>) {
        let mut values = vec![0.0_f32; 768];
        values[0] = f32::from_bits(marker.max(1));
        values[1] = ordinal as f32;
        (ordinal, values)
    }

    fn replace_once(bytes: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
        assert_eq!(needle.len(), replacement.len());
        let offset = bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("registered token must occur in encoded artifact");
        let mut changed = bytes.to_vec();
        changed[offset..offset + needle.len()].copy_from_slice(replacement);
        changed
    }

    fn model_identity(role: &str, digest: &str) -> V41ArtifactIdentity {
        V41ArtifactIdentity::try_new(
            role.to_owned(),
            format!("s3://borsuk-test/v41/{role}"),
            digest.to_owned(),
            digest.to_owned(),
            64,
        )
        .unwrap()
    }

    #[test]
    fn v41_model_arrow_round_trips_and_rejects_physical_drift() {
        let page_count = 24_usize;
        let model = borsuk_v41::V41ResidualModel::try_new(
            vec![0.25; 64 * 768],
            vec![0.5; 64],
            vec![0.75; 64 * 64],
            vec![1.0; page_count * 64],
            vec![1.25; page_count],
        )
        .unwrap();
        let backend = if cfg!(target_arch = "aarch64") {
            "aarch64-neon-fma"
        } else {
            "x86-avx-fma"
        };
        let authority = V41ModelAuthority::try_new(
            backend.to_owned(),
            u32::try_from(page_count).unwrap(),
            model_identity("source", SHA_A),
            model_identity("source-archive", SHA_B),
            model_identity("index", SHA_C),
            SHA_D.to_owned(),
        )
        .unwrap();
        let encoded = encode_v41_model(&model, &authority).unwrap();
        assert!(encoded.manifest_bytes().starts_with(b"{\"backend\":"));
        assert!(encoded.manifest_bytes().ends_with(b"\n"));
        assert_eq!(
            decode_v41_model(encoded.arrow_bytes(), encoded.manifest_bytes(), &authority).unwrap(),
            model
        );

        let trailer = encoded.arrow_bytes().len() - 10;
        let footer_length = usize::try_from(u32::from_le_bytes(
            encoded.arrow_bytes()[trailer..trailer + 4]
                .try_into()
                .unwrap(),
        ))
        .unwrap();
        let footer =
            arrow_ipc::root_as_footer(&encoded.arrow_bytes()[trailer - footer_length..trailer])
                .unwrap();
        let record_offset =
            usize::try_from(footer.recordBatches().unwrap().get(0).offset()).unwrap();
        let mut schema_with_premature_eos = encoded.arrow_bytes()[8..record_offset].to_vec();
        schema_with_premature_eos.extend_from_slice(&[0; 8]);
        assert!(v41_parse_arrow_message(&schema_with_premature_eos).is_err());

        let mut trailing = encoded.arrow_bytes().to_vec();
        trailing.push(0);
        assert!(decode_v41_model(&trailing, encoded.manifest_bytes(), &authority).is_err());

        let wrong_name = replace_once(encoded.arrow_bytes(), b"w_q", b"x_q");
        let mut renamed_manifest: V41ModelManifest =
            serde_json::from_slice(encoded.manifest_bytes()).unwrap();
        renamed_manifest.model_sha256 = format!("{:x}", Sha256::digest(&wrong_name));
        let renamed_manifest = v41_canonical_json_bytes(&renamed_manifest).unwrap();
        assert!(decode_v41_model(&wrong_name, &renamed_manifest, &authority).is_err());

        let wrong_backend = if backend == "aarch64-neon-fma" {
            replace_once(
                encoded.manifest_bytes(),
                b"aarch64-neon-fma",
                b"xarch64-neon-fma",
            )
        } else {
            replace_once(encoded.manifest_bytes(), b"x86-avx-fma", b"x86-fma-fma")
        };
        assert!(decode_v41_model(encoded.arrow_bytes(), &wrong_backend, &authority).is_err());

        let wrong_page_count = replace_once(
            encoded.manifest_bytes(),
            b"\"page_count\":24",
            b"\"page_count\":25",
        );
        assert!(decode_v41_model(encoded.arrow_bytes(), &wrong_page_count, &authority).is_err());
    }

    #[test]
    fn v41_authority_rejects_identity_schema_and_generation_drift() {
        assert!(
            V41LocalArtifact::try_new(
                "development-query".to_owned(),
                PathBuf::from("/tmp/v41-development-query"),
                "s3://borsuk-test/v41/development-query".to_owned(),
                "not-a-digest".to_owned(),
                SHA_B.to_owned(),
                64,
            )
            .is_err()
        );
        assert!(
            V41LocalArtifact::try_new(
                "development-query".to_owned(),
                PathBuf::from("/tmp/v41-development-query"),
                "s3://borsuk-test/v41/development-query".to_owned(),
                SHA_A.to_owned(),
                SHA_B.to_owned(),
                0,
            )
            .is_err()
        );

        let mut inputs = vec![artifact("preflight-authority"), artifact("source-archive")];
        let outputs = vec![output("preflight-samples"), output("preflight-result")];
        assert!(
            V41LocalRunRequest::try_new(
                V41LocalRunMode::Preflight,
                inputs.clone(),
                outputs.clone(),
                32,
            )
            .is_ok()
        );
        inputs[1] = inputs[0].clone();
        assert!(
            V41LocalRunRequest::try_new(V41LocalRunMode::Preflight, inputs, outputs, 32,).is_err()
        );
    }

    #[test]
    fn v41_authority_modes_admit_only_phase_local_capabilities() {
        for mode in [
            V41LocalRunMode::AuditDevelopmentValidation,
            V41LocalRunMode::PartitionDevelopment,
            V41LocalRunMode::Preflight,
            V41LocalRunMode::TrainDiagnostic,
            V41LocalRunMode::SelectDiagnostic,
            V41LocalRunMode::EvaluateDiagnostic,
            V41LocalRunMode::TrainFinal,
            V41LocalRunMode::SelectValidation,
            V41LocalRunMode::EvaluateValidation,
            V41LocalRunMode::AuditHoldout,
            V41LocalRunMode::SelectHoldout,
            V41LocalRunMode::EvaluateHoldout,
        ] {
            let request = V41LocalRunRequest::try_new(
                mode,
                mode.input_roles()
                    .iter()
                    .map(|role| artifact(role))
                    .collect(),
                mode.output_roles()
                    .iter()
                    .map(|role| output(role))
                    .collect(),
                1,
            )
            .unwrap();
            assert_eq!(request.input_roles(), mode.input_roles());
        }

        let audit = V41LocalRunRequest::try_new(
            V41LocalRunMode::AuditDevelopmentValidation,
            vec![
                artifact("freeze-receipt"),
                artifact("development-query"),
                artifact("validation-query"),
            ],
            vec![output("split-audit")],
            1,
        )
        .unwrap();
        assert_eq!(
            audit.input_roles(),
            vec!["freeze-receipt", "development-query", "validation-query"]
        );

        let selection = V41LocalRunRequest::try_new(
            V41LocalRunMode::SelectValidation,
            vec![
                artifact("validation-query"),
                artifact("model"),
                artifact("source-archive"),
                artifact("binary"),
            ],
            vec![output("validation-selection")],
            32,
        )
        .unwrap();
        assert!(!selection.input_roles().contains(&"validation-gt"));
        let mut leaked = selection.input_artifacts().to_vec();
        leaked.push(artifact("validation-gt"));
        assert!(
            V41LocalRunRequest::try_new(
                V41LocalRunMode::SelectValidation,
                leaked,
                vec![output("validation-selection")],
                32,
            )
            .is_err()
        );

        let evaluation = V41LocalRunRequest::try_new(
            V41LocalRunMode::EvaluateValidation,
            vec![
                artifact("validation-selection"),
                artifact("validation-gt"),
                artifact("spill-relation"),
                artifact("spill-postings"),
            ],
            vec![output("validation-result")],
            32,
        )
        .unwrap();
        assert!(!evaluation.input_roles().contains(&"validation-query"));
        assert!(!evaluation.input_roles().contains(&"model"));

        let training = V41LocalRunRequest::try_new(
            V41LocalRunMode::TrainDiagnostic,
            vec![
                artifact("split-audit"),
                artifact("training-partition-receipt"),
                artifact("training-query"),
                artifact("training-gt"),
                artifact("spill-relation"),
                artifact("spill-postings"),
                artifact("source-archive"),
                artifact("binary"),
            ],
            vec![
                output("diagnostic-model"),
                output("training-state"),
                output("training-result"),
            ],
            32,
        )
        .unwrap();
        assert!(!training.input_roles().contains(&"partition-manifest"));
        assert!(!training.input_roles().contains(&"diagnostic-query"));
        assert!(!training.input_roles().contains(&"diagnostic-gt"));

        let diagnostic_selection = V41LocalRunRequest::try_new(
            V41LocalRunMode::SelectDiagnostic,
            vec![
                artifact("diagnostic-query-receipt"),
                artifact("diagnostic-query"),
                artifact("diagnostic-model"),
                artifact("source-archive"),
                artifact("binary"),
            ],
            vec![output("diagnostic-selection")],
            32,
        )
        .unwrap();
        assert!(
            !diagnostic_selection
                .input_roles()
                .contains(&"partition-manifest")
        );
        assert!(
            !diagnostic_selection
                .input_roles()
                .contains(&"diagnostic-gt")
        );
    }

    #[test]
    fn v41_authority_split_audits_reject_cross_role_vector_duplicates() {
        let development = vec![query(0, 11), query(1, 12)];
        let validation = vec![query(0, 21), query(1, 22)];
        let audit = audit_v41_query_roles(
            "development-query",
            &development,
            "validation-query",
            &validation,
        )
        .unwrap();
        assert_eq!(audit.left_rows(), 2);
        assert_eq!(audit.right_rows(), 2);
        assert_ne!(
            audit.left_vector_bits_sha256(),
            audit.right_vector_bits_sha256()
        );

        let duplicate = vec![query(0, 21), development[1].clone()];
        assert!(
            audit_v41_query_roles(
                "development-query",
                &development,
                "validation-query",
                &duplicate,
            )
            .is_err()
        );

        let prior = audit_v41_query_roles(
            "development-query",
            &development,
            "validation-query",
            &validation,
        )
        .unwrap();
        assert!(audit_v41_holdout_query_role(&prior, &vec![query(0, 31)]).is_ok());
        assert!(audit_v41_holdout_query_role(&prior, &vec![development[0].clone()]).is_err());
        assert!(audit_v41_holdout_query_role(&prior, &vec![validation[0].clone()]).is_err());
        let wrong_predecessor = audit_v41_query_roles(
            "development-query",
            &development,
            "holdout-query",
            &vec![query(0, 41)],
        )
        .unwrap();
        assert!(audit_v41_holdout_query_role(&wrong_predecessor, &vec![query(0, 51)]).is_err());
        assert!(
            audit_v41_query_roles(
                "validation-query",
                &validation,
                "holdout-query",
                &vec![validation[0].clone()],
            )
            .is_err()
        );
        assert!(
            audit_v41_query_roles(
                "development-query",
                &development,
                "holdout-query",
                &vec![query(0, 31)],
            )
            .is_ok()
        );
        assert!(
            audit_v41_query_roles(
                "development-query",
                &development,
                "holdout-query",
                &vec![development[0].clone()],
            )
            .is_err()
        );
    }

    #[test]
    fn v41_authority_development_split_is_duplicate_safe_and_order_exact() {
        let mut queries = (0..1_000)
            .map(|ordinal| query(ordinal, ordinal + 100))
            .collect::<Vec<_>>();
        queries[900].1 = queries[799].1.clone();
        let split = split_v41_development_queries(&queries).unwrap();
        assert_eq!(
            split.training_ordinals().len() + split.diagnostic_ordinals().len(),
            1_000
        );
        assert!(split.training_ordinals().len() <= 800);
        let in_training = |ordinal| split.training_ordinals().contains(&ordinal);
        assert_eq!(in_training(799), in_training(900));
        assert!(
            split
                .training_ordinals()
                .windows(2)
                .all(|window| window[0] < window[1])
        );
        assert!(
            split
                .diagnostic_ordinals()
                .windows(2)
                .all(|window| window[0] < window[1])
        );
        assert_eq!(split_v41_development_queries(&queries).unwrap(), split);

        let mut groups = BTreeMap::<[u8; 32], Vec<u32>>::new();
        for (ordinal, vector) in &queries {
            let mut hash = Sha256::new();
            hash.update(b"borsuk-v41-development-split-v1");
            for value in vector {
                hash.update(value.to_bits().to_le_bytes());
            }
            groups
                .entry(hash.finalize().into())
                .or_default()
                .push(*ordinal);
        }
        let mut expected_training = Vec::new();
        let mut expected_diagnostic = Vec::new();
        for (_, ordinals) in groups {
            if expected_training.len() + ordinals.len() <= 800 {
                expected_training.extend(ordinals);
            } else {
                expected_diagnostic.extend(ordinals);
            }
        }
        expected_training.sort_unstable();
        expected_diagnostic.sort_unstable();
        assert_eq!(split.training_ordinals(), expected_training);
        assert_eq!(split.diagnostic_ordinals(), expected_diagnostic);

        queries.swap(1, 2);
        assert!(split_v41_development_queries(&queries).is_err());
    }

    fn child(role: &str, sha256: &str, ordinals: Vec<u32>) -> V41DevelopmentPartitionChild {
        V41DevelopmentPartitionChild::try_new(
            role.to_owned(),
            V41PartitionChildAuthority::try_new(
                format!("s3://borsuk-test/v41/{role}"),
                sha256.to_owned(),
                SHA_B.to_owned(),
                128,
                parent_identity("development-query", SHA_C),
                parent_identity("development-gt", SHA_D),
                SHA_A.to_owned(),
            )
            .unwrap(),
            ordinals,
        )
        .unwrap()
    }

    fn parent_identity(role: &str, sha256: &str) -> V41ArtifactIdentity {
        V41ArtifactIdentity::try_new(
            role.to_owned(),
            format!("s3://borsuk-test/v41/parent-{role}"),
            sha256.to_owned(),
            SHA_B.to_owned(),
            256,
        )
        .unwrap()
    }

    #[test]
    fn v41_authority_partition_children_bind_parent_ordinals_and_manifest() {
        let manifest = V41DevelopmentPartitionManifest::try_new(vec![
            child("training-query", SHA_A, vec![0, 2]),
            child("training-gt", SHA_B, vec![0, 2]),
            child("diagnostic-query", SHA_C, vec![1, 3]),
            child("diagnostic-gt", SHA_D, vec![1, 3]),
        ])
        .unwrap();
        validate_v41_development_partition(&manifest, 4).unwrap();
        validate_v41_development_partition_against(
            &manifest,
            &parent_identity("development-query", SHA_C),
            &parent_identity("development-gt", SHA_D),
            SHA_A,
            &[0, 2],
            &[1, 3],
        )
        .unwrap();

        let training_projection =
            project_v41_development_partition(&manifest, SHA_A, "training-partition-receipt")
                .unwrap();
        assert_eq!(
            training_projection.child_roles(),
            ["training-query", "training-gt"]
        );
        let query_projection =
            project_v41_development_partition(&manifest, SHA_A, "diagnostic-query-receipt")
                .unwrap();
        assert_eq!(query_projection.child_roles(), ["diagnostic-query"]);
        let evaluation_projection =
            project_v41_development_partition(&manifest, SHA_A, "diagnostic-evaluation-receipt")
                .unwrap();
        assert_eq!(evaluation_projection.child_roles(), ["diagnostic-gt"]);

        let mut missing = manifest.children().to_vec();
        missing.pop();
        assert!(V41DevelopmentPartitionManifest::try_new(missing).is_err());

        let drifted = V41DevelopmentPartitionManifest::try_new(vec![
            child("training-query", SHA_A, vec![0, 2]),
            child("training-gt", SHA_B, vec![0, 3]),
            child("diagnostic-query", SHA_C, vec![1, 3]),
            child("diagnostic-gt", SHA_D, vec![1, 3]),
        ]);
        assert!(drifted.is_err());

        let coherently_substituted = V41DevelopmentPartitionManifest::try_new(vec![
            child("training-query", SHA_A, vec![0, 1]),
            child("training-gt", SHA_B, vec![0, 1]),
            child("diagnostic-query", SHA_C, vec![2, 3]),
            child("diagnostic-gt", SHA_D, vec![2, 3]),
        ])
        .unwrap();
        assert!(
            validate_v41_development_partition_against(
                &coherently_substituted,
                &parent_identity("development-query", SHA_C),
                &parent_identity("development-gt", SHA_D),
                SHA_A,
                &[0, 2],
                &[1, 3],
            )
            .is_err()
        );

        let mut substituted_parent = manifest.clone();
        for child in &mut substituted_parent.children {
            child.authority.parent_query = parent_identity("development-query", SHA_D);
        }
        assert!(
            validate_v41_development_partition_against(
                &substituted_parent,
                &parent_identity("development-query", SHA_C),
                &parent_identity("development-gt", SHA_D),
                SHA_A,
                &[0, 2],
                &[1, 3],
            )
            .is_err()
        );
    }

    #[test]
    fn v41_target_adapter_reuses_v38_owner_decoder() {
        let mut records = Vec::new();
        let mut alternate_local = [25_u32; 4];
        for source_ordinal in 0_u64..100 {
            let primary = u32::try_from(source_ordinal % 4).unwrap();
            records.push(V38SpillRecord {
                source_ordinal,
                feature_row_id: 10_000 + source_ordinal,
                posting_ordinal: primary,
                owner_role: 0,
                posting_local_ordinal: u32::try_from(source_ordinal / 4).unwrap(),
                alternate_violation_bits: None,
            });
            if source_ordinal % 5 == 0 {
                let alternate = (primary + 1) % 4;
                let local = &mut alternate_local[usize::try_from(alternate).unwrap()];
                records.push(V38SpillRecord {
                    source_ordinal,
                    feature_row_id: 10_000 + source_ordinal,
                    posting_ordinal: alternate,
                    owner_role: 1,
                    posting_local_ordinal: *local,
                    alternate_violation_bits: Some(0.0_f32.to_bits()),
                });
                *local += 1;
            }
        }
        let relation = encode_v38_spill_relation_parquet(&records).unwrap();
        let summaries = summarize_v38_spill_relation(&records, 4, 30).unwrap();
        let postings = encode_v38_posting_summary_parquet(&summaries).unwrap();
        let gt = (10_000_u64..10_100).collect::<Vec<_>>();

        assert_eq!(
            v41_marginal_targets_from_v38_artifacts(&relation, &postings, 100, 4, 30, &gt, &[],)
                .unwrap(),
            vec![0.30; 4]
        );
        assert!(
            v41_marginal_targets_from_v38_artifacts(
                &relation,
                &postings[..postings.len() - 1],
                100,
                4,
                30,
                &gt,
                &[],
            )
            .is_err()
        );
    }

    #[test]
    fn v41_training_state_parquet_is_nonnull_ordered_and_row_group_bounded() {
        use arrow_array::{Array, ListArray, UInt32Array, UInt64Array};
        use arrow_schema::{DataType, Field, Schema};
        use borsuk_v41::{V41TrainingRecord, V41TrainingSink};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::{collections::HashMap, fs::File, sync::Arc};

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("training-state.parquet");
        let mut sink = V41TrainingStateParquetSink::try_new(&path).unwrap();
        fn require_training_sink(_: &mut impl V41TrainingSink) {}
        require_training_sink(&mut sink);
        for row in 0_u32..4_100 {
            let epoch = row / 2_050;
            let state_ordinal = row % 2_050;
            let rollout_step = state_ordinal % 21;
            let prefix = (0..rollout_step).collect::<Vec<_>>();
            sink.record(V41TrainingRecord {
                epoch,
                state_ordinal,
                query_ordinal: state_ordinal / 21,
                rollout_step,
                selected_page: rollout_step,
                ordered_prefix: prefix.clone(),
                ascending_membership: prefix,
                nonzero_target_pages: 1,
                target_sum_bits: 1.0_f32.to_bits(),
                loss_bits: Some(0.5_f32.to_bits()),
                optimizer_step_before: u64::from(row / 64),
                optimizer_step_after: u64::from(row / 64) + 1,
                numerical_stop_count: 0,
            })
            .unwrap();
        }
        sink.finish().unwrap();

        let list = DataType::List(Arc::new(Field::new("element", DataType::UInt32, false)));
        let expected_schema = Schema::new_with_metadata(
            vec![
                Field::new("epoch", DataType::UInt32, false),
                Field::new("state_ordinal", DataType::UInt32, false),
                Field::new("query_ordinal", DataType::UInt32, false),
                Field::new("rollout_step", DataType::UInt32, false),
                Field::new("selected_page", DataType::UInt32, false),
                Field::new("ordered_prefix", list.clone(), false),
                Field::new("ascending_membership", list, false),
                Field::new("nonzero_target_pages", DataType::UInt32, false),
                Field::new("target_sum_bits", DataType::UInt32, false),
                Field::new("loss_present", DataType::Boolean, false),
                Field::new("loss_bits", DataType::UInt32, false),
                Field::new("optimizer_step_before", DataType::UInt64, false),
                Field::new("optimizer_step_after", DataType::UInt64, false),
                Field::new("numerical_stop_count", DataType::UInt32, false),
            ],
            HashMap::from([(
                "schema".to_owned(),
                "borsuk-v41-training-state-v1".to_owned(),
            )]),
        );
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        assert_eq!(builder.schema().as_ref(), &expected_schema);
        assert_eq!(
            builder
                .metadata()
                .row_groups()
                .iter()
                .map(|group| group.num_rows())
                .collect::<Vec<_>>(),
            vec![4_096, 4]
        );
        let batches = builder
            .with_batch_size(4_096)
            .build()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            4_100
        );
        assert!(
            batches
                .iter()
                .flat_map(|batch| batch.columns())
                .all(|column| column.null_count() == 0)
        );
        let first = &batches[0];
        assert_eq!(
            first
                .column(1)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(0),
            0
        );
        assert_eq!(
            first
                .column(12)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(63),
            1
        );
        let prefixes = first
            .column(5)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(prefixes.value(20).len(), 20);
        assert_eq!(prefixes.values().null_count(), 0);

        let rejected_path = directory.path().join("rejected.parquet");
        let mut rejected = V41TrainingStateParquetSink::try_new(&rejected_path).unwrap();
        let record = V41TrainingRecord {
            epoch: 0,
            state_ordinal: 0,
            query_ordinal: 0,
            rollout_step: 0,
            selected_page: 0,
            ordered_prefix: Vec::new(),
            ascending_membership: Vec::new(),
            nonzero_target_pages: 0,
            target_sum_bits: 0.0_f32.to_bits(),
            loss_bits: None,
            optimizer_step_before: 0,
            optimizer_step_after: 0,
            numerical_stop_count: 0,
        };
        rejected.record(record.clone()).unwrap();
        assert!(rejected.record(record).is_err());
    }
}
