use std::{collections::BTreeSet, fs, path::PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    v23_incidence_eval::{
        read_v23_incidence_development_queries, read_v23_incidence_development_truth,
    },
    v23_incidence_tree::{V23IncidenceTrainingShape, decode_incidence_tree},
    v23_rabitq::{
        V23RaBitQManifest, V23RaBitQObjectIdentity, V23RaBitQPhase, V23RaBitQRunMode,
        canonical_v23_rabitq_manifest_bytes, validate_v23_rabitq_manifest,
    },
    v23_rabitq_arrow::{
        V23RaBitQGeometryBytes, V23RaBitQGeometryIdentities, read_v23_rabitq_f16_control,
        read_v23_rabitq_geometry, read_v23_rabitq_row_planes,
    },
    v23_rabitq_build::read_v23_rabitq_build_receipt,
    v23_rabitq_eval::{
        V23RaBitQDevelopmentRequest, canonical_v23_rabitq_screen_result_bytes,
        detected_v23_rabitq_backend, evaluate_v23_rabitq_development,
    },
};

const INPUT_ROLES: [&str; 9] = [
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

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_string())
}

fn lower_hex(value: &str, width: usize) -> bool {
    value.len() == width
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct V23RaBitQLocalObjectIdentity {
    pub role: String,
    pub uri: String,
    pub sha256: String,
    pub encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V23RaBitQLocalArtifactPaths {
    pub manifest: PathBuf,
    pub construction_receipt: PathBuf,
    pub incidence_tree: PathBuf,
    pub row_codes: PathBuf,
    pub leaf_offsets: PathBuf,
    pub centroids: PathBuf,
    pub rotation: PathBuf,
    pub f16_control: PathBuf,
    pub d2_report: PathBuf,
    pub query_parquet: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V23RaBitQLocalRunRequest {
    pub paths: V23RaBitQLocalArtifactPaths,
    pub manifest_identity: V23RaBitQLocalObjectIdentity,
    pub registered_inputs: Vec<V23RaBitQLocalObjectIdentity>,
    pub execute_development: bool,
}

impl V23RaBitQLocalObjectIdentity {
    fn validate(&self, role: &str) -> Result<()> {
        if self.role != role
            || !self.uri.starts_with("s3://")
            || self.uri.trim_start_matches("s3://").is_empty()
            || !lower_hex(&self.sha256, 64)
            || self.encoded_bytes == 0
        {
            return Err(invalid("V23 RaBitQ local object identity differs"));
        }
        Ok(())
    }

    fn internal(&self) -> V23RaBitQObjectIdentity {
        V23RaBitQObjectIdentity {
            role: self.role.clone(),
            uri: self.uri.clone(),
            sha256: self.sha256.clone(),
            blake3: None,
            encoded_bytes: self.encoded_bytes,
        }
    }
}

impl V23RaBitQLocalArtifactPaths {
    fn ordered(&self) -> [&PathBuf; 10] {
        [
            &self.manifest,
            &self.construction_receipt,
            &self.incidence_tree,
            &self.row_codes,
            &self.leaf_offsets,
            &self.centroids,
            &self.rotation,
            &self.f16_control,
            &self.d2_report,
            &self.query_parquet,
        ]
    }

    fn input_path(&self, role: &str) -> Result<&PathBuf> {
        match role {
            "construction-receipt" => Ok(&self.construction_receipt),
            "incidence-tree" => Ok(&self.incidence_tree),
            "row-codes" => Ok(&self.row_codes),
            "leaf-offsets" => Ok(&self.leaf_offsets),
            "centroids" => Ok(&self.centroids),
            "rotation" => Ok(&self.rotation),
            "f16-control" => Ok(&self.f16_control),
            "d2-report" => Ok(&self.d2_report),
            "query-parquet" => Ok(&self.query_parquet),
            _ => Err(invalid("V23 RaBitQ local role differs")),
        }
    }
}

fn read_authenticated(
    path: &PathBuf,
    identity: &V23RaBitQLocalObjectIdentity,
    role: &str,
) -> Result<Vec<u8>> {
    identity.validate(role)?;
    if !path.is_absolute() || !path.is_file() {
        return Err(invalid("V23 RaBitQ local artifact path differs"));
    }
    let bytes = fs::read(path).map_err(|error| {
        invalid(&format!(
            "V23 RaBitQ local artifact read failed for {}: {error}",
            path.display()
        ))
    })?;
    if identity.encoded_bytes != bytes.len() as u64
        || identity.sha256 != format!("{:x}", Sha256::digest(&bytes))
    {
        return Err(invalid("V23 RaBitQ local artifact byte authority differs"));
    }
    Ok(bytes)
}

fn validate_request(request: &V23RaBitQLocalRunRequest) -> Result<Vec<V23RaBitQObjectIdentity>> {
    if !request.execute_development || request.registered_inputs.len() != INPUT_ROLES.len() {
        return Err(invalid("V23 RaBitQ local execution was not authorized"));
    }
    request.manifest_identity.validate("manifest")?;
    let mut paths = BTreeSet::new();
    if request
        .paths
        .ordered()
        .iter()
        .any(|path| !path.is_absolute() || !paths.insert(path.as_path()))
    {
        return Err(invalid("V23 RaBitQ local artifact paths overlap"));
    }
    let mut uris = BTreeSet::new();
    if !uris.insert(request.manifest_identity.uri.as_str()) {
        return Err(invalid("V23 RaBitQ local object URIs overlap"));
    }
    let mut internal = Vec::with_capacity(INPUT_ROLES.len());
    for (identity, role) in request.registered_inputs.iter().zip(INPUT_ROLES) {
        identity.validate(role)?;
        if !uris.insert(identity.uri.as_str()) {
            return Err(invalid("V23 RaBitQ local object URIs overlap"));
        }
        internal.push(identity.internal());
    }
    Ok(internal)
}

#[doc(hidden)]
pub fn run_v23_rabitq_local_request(request: V23RaBitQLocalRunRequest) -> Result<Vec<u8>> {
    let inputs = validate_request(&request)?;
    let manifest_bytes = read_authenticated(
        &request.paths.manifest,
        &request.manifest_identity,
        "manifest",
    )?;
    let manifest: V23RaBitQManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| invalid(&format!("V23 RaBitQ manifest JSON differs: {error}")))?;
    validate_v23_rabitq_manifest(&manifest)?;
    if canonical_v23_rabitq_manifest_bytes(&manifest)? != manifest_bytes
        || manifest.run_mode != V23RaBitQRunMode::Execute(V23RaBitQPhase::Development)
        || manifest.registered_inputs != inputs
    {
        return Err(invalid("V23 RaBitQ local manifest binding differs"));
    }

    let mut role_bytes = Vec::with_capacity(INPUT_ROLES.len());
    for (identity, role) in request.registered_inputs.iter().zip(INPUT_ROLES) {
        role_bytes.push(read_authenticated(
            request.paths.input_path(role)?,
            identity,
            role,
        )?);
    }
    let receipt = read_v23_rabitq_build_receipt(&role_bytes[0])?;
    if receipt.outputs != inputs[2..7] {
        return Err(invalid("V23 RaBitQ construction receipt binding differs"));
    }
    let tree = decode_incidence_tree(&role_bytes[1])?;
    if tree.shape != V23IncidenceTrainingShape::PRODUCTION {
        return Err(invalid("V23 RaBitQ incidence tree shape differs"));
    }
    let rows = read_v23_rabitq_row_planes(&role_bytes[2], &inputs[2])?;
    let geometry = read_v23_rabitq_geometry(
        &V23RaBitQGeometryBytes {
            leaf_offsets: std::mem::take(&mut role_bytes[3]),
            centroids: std::mem::take(&mut role_bytes[4]),
            rotation: std::mem::take(&mut role_bytes[5]),
        },
        &V23RaBitQGeometryIdentities {
            leaf_offsets: inputs[3].clone(),
            centroids: inputs[4].clone(),
            rotation: inputs[5].clone(),
        },
        receipt.source_rows,
    )?;
    let exact_rows =
        read_v23_rabitq_f16_control(&role_bytes[6], &inputs[6], rows.sign_codes.len())?;
    let truth = read_v23_incidence_development_truth(&role_bytes[7])?;
    let queries = read_v23_incidence_development_queries(&role_bytes[8])?;
    let result = evaluate_v23_rabitq_development(V23RaBitQDevelopmentRequest {
        source_commit: manifest.source_commit,
        source_archive_sha256: manifest.source_archive_sha256,
        index_id: manifest.index_id,
        inputs: &inputs,
        tree: &tree,
        geometry: &geometry,
        rows: &rows,
        exact_rows: &exact_rows,
        queries: &queries,
        truth: &truth,
        backend: detected_v23_rabitq_backend()?,
    })?;
    canonical_v23_rabitq_screen_result_bytes(&result, &inputs)
}
