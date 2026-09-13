use crate::error::{BorsukError, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
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

#[cfg(test)]
mod tests {
    use super::{
        V41ArtifactIdentity, V41DevelopmentPartitionChild, V41DevelopmentPartitionManifest,
        V41LocalArtifact, V41LocalOutput, V41LocalRunMode, V41LocalRunRequest,
        V41PartitionChildAuthority, audit_v41_holdout_query_role, audit_v41_query_roles,
        project_v41_development_partition, split_v41_development_queries,
        validate_v41_development_partition, validate_v41_development_partition_against,
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
}
