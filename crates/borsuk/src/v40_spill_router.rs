use crate::error::{BorsukError, Result};
use crate::v37_relation_router::{
    V37BalancedTree, V37FeatureGroundTruth, select_v37_tree_postings_with_limit,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const V40_MAXIMUM_FRONTIER_POSTINGS: usize = 64;
const V40_MAXIMUM_NODE_POPS: usize = 1_024;
const V40_DIRECT_QUERY_COUNT: u64 = 1_000;
const V40_DIRECT_SELECTED_POSTINGS: usize = 21;

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
/// One capability-separated local V40 direct-router phase.
#[doc(hidden)]
pub enum V40LocalRunMode {
    /// Select postings from query vectors without ground truth access.
    SelectDirect,
    /// Evaluate sealed selections with ground truth but no query-vector access.
    EvaluateDirect,
}

impl V40LocalRunMode {
    fn input_roles(self) -> &'static [&'static str] {
        match self {
            Self::SelectDirect => &["v37-authority", "ownership-tree", "development-query"],
            Self::EvaluateDirect => &[
                "v38-ceiling-authority",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
                "development-ground-truth",
                "direct-selection-result",
                "direct-selection",
            ],
        }
    }

    fn output_roles(self) -> &'static [&'static str] {
        match self {
            Self::SelectDirect => &["direct-selection"],
            Self::EvaluateDirect => &["direct-result"],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One authenticated local input whose URI is evidence, not a network capability.
#[doc(hidden)]
pub struct V40LocalArtifact {
    role: String,
    path: PathBuf,
    uri: String,
    sha256: String,
    blake3: String,
    encoded_bytes: u64,
}

impl V40LocalArtifact {
    /// Construct one strict phase-local input identity.
    pub fn try_new(
        role: String,
        path: PathBuf,
        uri: String,
        sha256: String,
        blake3: String,
        encoded_bytes: u64,
    ) -> Result<Self> {
        if role.is_empty()
            || path.as_os_str().is_empty()
            || !valid_s3_uri(&uri)
            || !valid_digest(&sha256)
            || !valid_digest(&blake3)
            || encoded_bytes == 0
        {
            return Err(BorsukError::InvalidStorage(
                "V40 local artifact identity differs".to_owned(),
            ));
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

    /// Return the exact phase-local role.
    pub fn role(&self) -> &str {
        &self.role
    }

    /// Return the local path without opening it.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One explicit create-only V40 output.
#[doc(hidden)]
pub struct V40LocalOutput {
    role: String,
    path: PathBuf,
}

impl V40LocalOutput {
    /// Construct one strict output identity.
    pub fn try_new(role: String, path: PathBuf) -> Result<Self> {
        if role.is_empty() || path.as_os_str().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "V40 local output identity differs".to_owned(),
            ));
        }
        Ok(Self { role, path })
    }

    /// Return the output role.
    pub fn role(&self) -> &str {
        &self.role
    }

    /// Return the create-only output path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Exact local inputs and output for one V40 direct phase.
#[doc(hidden)]
pub struct V40LocalRunRequest {
    mode: V40LocalRunMode,
    inputs: Vec<V40LocalArtifact>,
    outputs: Vec<V40LocalOutput>,
    workers: u32,
}

impl V40LocalRunRequest {
    /// Validate exact roles, worker values, and path/URI separation.
    pub fn try_new(
        mode: V40LocalRunMode,
        inputs: Vec<V40LocalArtifact>,
        outputs: Vec<V40LocalOutput>,
        workers: u32,
    ) -> Result<Self> {
        let input_roles = inputs
            .iter()
            .map(V40LocalArtifact::role)
            .collect::<Vec<_>>();
        let output_roles = outputs.iter().map(V40LocalOutput::role).collect::<Vec<_>>();
        let mut paths = BTreeSet::new();
        let mut uris = BTreeSet::new();
        if input_roles != mode.input_roles()
            || output_roles != mode.output_roles()
            || !matches!(workers, 1 | 2 | 4 | 8 | 16 | 32)
            || inputs.iter().any(|input| !paths.insert(input.path()))
            || outputs.iter().any(|output| !paths.insert(output.path()))
            || inputs.iter().any(|input| !uris.insert(input.uri.as_str()))
        {
            return Err(BorsukError::InvalidStorage(
                "V40 local phase capability differs".to_owned(),
            ));
        }
        Ok(Self {
            mode,
            inputs,
            outputs,
            workers,
        })
    }

    /// Return input roles in their authoritative order.
    pub fn input_roles(&self) -> Vec<&str> {
        self.inputs.iter().map(V40LocalArtifact::role).collect()
    }

    /// Return output roles in their authoritative order.
    pub fn output_roles(&self) -> Vec<&str> {
        self.outputs.iter().map(V40LocalOutput::role).collect()
    }

    /// Return this request's phase.
    pub fn mode(&self) -> V40LocalRunMode {
        self.mode
    }

    /// Return the frozen worker count.
    pub fn workers(&self) -> u32 {
        self.workers
    }
}

#[derive(Debug)]
pub(crate) struct V40AuthenticatedLocalInputs {
    files: Vec<File>,
}

pub(crate) fn authenticate_v40_local_request(
    request: &V40LocalRunRequest,
) -> Result<V40AuthenticatedLocalInputs> {
    const HASH_BUFFER_BYTES: usize = 1_048_576;
    let invalid =
        || BorsukError::InvalidStorage("V40 local input authentication differs".to_owned());
    let mut canonical_inputs = BTreeSet::new();
    let mut file_ids = BTreeSet::new();
    let mut files = Vec::with_capacity(request.inputs.len());
    for input in &request.inputs {
        let before = fs::symlink_metadata(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        if before.file_type().is_symlink()
            || !before.file_type().is_file()
            || before.len() != input.encoded_bytes
        {
            return Err(invalid());
        }
        let file = File::open(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        let opened = file.metadata().map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        if opened.dev() != before.dev()
            || opened.ino() != before.ino()
            || !file_ids.insert((opened.dev(), opened.ino()))
            || !canonical_inputs.insert(fs::canonicalize(&input.path).map_err(|source| {
                BorsukError::Io {
                    path: input.path.clone(),
                    source,
                }
            })?)
        {
            return Err(invalid());
        }
        let mut reader = BufReader::with_capacity(HASH_BUFFER_BYTES, file);
        let mut sha256 = Sha256::new();
        let mut blake3 = blake3::Hasher::new();
        let mut observed_bytes = 0_u64;
        let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
        loop {
            let read = reader.read(&mut buffer).map_err(|source| BorsukError::Io {
                path: input.path.clone(),
                source,
            })?;
            if read == 0 {
                break;
            }
            observed_bytes = observed_bytes
                .checked_add(read as u64)
                .ok_or_else(invalid)?;
            sha256.update(&buffer[..read]);
            blake3.update(&buffer[..read]);
        }
        let after = fs::symlink_metadata(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        if observed_bytes != input.encoded_bytes
            || format!("{:x}", sha256.finalize()) != input.sha256
            || blake3.finalize().to_hex().as_str() != input.blake3
            || after.file_type().is_symlink()
            || after.dev() != before.dev()
            || after.ino() != before.ino()
            || after.len() != before.len()
            || after.mtime() != before.mtime()
            || after.mtime_nsec() != before.mtime_nsec()
            || after.ctime() != before.ctime()
            || after.ctime_nsec() != before.ctime_nsec()
        {
            return Err(invalid());
        }
        files.push(reader.into_inner());
    }

    let mut canonical_outputs = BTreeSet::new();
    for output in &request.outputs {
        match fs::symlink_metadata(&output.path) {
            Ok(_) => return Err(invalid()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(BorsukError::Io {
                    path: output.path.clone(),
                    source,
                });
            }
        }
        let parent = output.path.parent().ok_or_else(invalid)?;
        let parent_metadata = fs::symlink_metadata(parent).map_err(|source| BorsukError::Io {
            path: parent.to_owned(),
            source,
        })?;
        if parent_metadata.file_type().is_symlink() || !parent_metadata.file_type().is_dir() {
            return Err(invalid());
        }
        let output_name = output.path.file_name().ok_or_else(invalid)?;
        let canonical = fs::canonicalize(parent)
            .map_err(|source| BorsukError::Io {
                path: parent.to_owned(),
                source,
            })?
            .join(output_name);
        if canonical_inputs.contains(&canonical) || !canonical_outputs.insert(canonical) {
            return Err(invalid());
        }
    }
    Ok(V40AuthenticatedLocalInputs { files })
}

fn v40_local_input<'a>(
    request: &'a V40LocalRunRequest,
    role: &str,
) -> Result<&'a V40LocalArtifact> {
    request
        .inputs
        .iter()
        .find(|input| input.role == role)
        .ok_or_else(|| BorsukError::InvalidStorage("V40 local input role differs".to_owned()))
}

fn v40_authenticated_input_file(
    request: &V40LocalRunRequest,
    authenticated: &V40AuthenticatedLocalInputs,
    role: &str,
) -> Result<File> {
    let index = request
        .inputs
        .iter()
        .position(|input| input.role == role)
        .ok_or_else(|| BorsukError::InvalidStorage("V40 local input role differs".to_owned()))?;
    let mut file = authenticated
        .files
        .get(index)
        .ok_or_else(|| BorsukError::InvalidStorage("V40 authenticated input differs".to_owned()))?
        .try_clone()
        .map_err(|source| BorsukError::Io {
            path: request.inputs[index].path.clone(),
            source,
        })?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| BorsukError::Io {
            path: request.inputs[index].path.clone(),
            source,
        })?;
    Ok(file)
}

fn read_v40_authenticated_input(
    request: &V40LocalRunRequest,
    authenticated: &V40AuthenticatedLocalInputs,
    role: &str,
) -> Result<Vec<u8>> {
    let input = v40_local_input(request, role)?;
    let capacity = usize::try_from(input.encoded_bytes).map_err(|_| {
        BorsukError::InvalidStorage("V40 local input exceeds address space".to_owned())
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    v40_authenticated_input_file(request, authenticated, role)?
        .take(input.encoded_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
    if bytes.len() != capacity {
        return Err(BorsukError::InvalidStorage(
            "V40 authenticated input length differs".to_owned(),
        ));
    }
    Ok(bytes)
}

fn v40_canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, v40_canonical_json(value)))
                    .collect(),
            )
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(v40_canonical_json).collect())
        }
        value => value,
    }
}

fn v40_selection_receipt_bytes(
    request: &V40LocalRunRequest,
    selections: &[V40DirectSelectionRecord],
    output_bytes: &[u8],
) -> Result<Vec<u8>> {
    let inputs = request
        .inputs
        .iter()
        .map(|input| {
            serde_json::json!({
                "blake3": input.blake3,
                "encoded_bytes": input.encoded_bytes,
                "role": input.role,
                "sha256": input.sha256,
                "uri": input.uri,
            })
        })
        .collect::<Vec<_>>();
    let output = request.outputs.first().ok_or_else(|| {
        BorsukError::InvalidStorage("V40 direct selection output differs".to_owned())
    })?;
    let total_node_pops = selections.iter().try_fold(0_u64, |total, selection| {
        total.checked_add(u64::from(selection.node_pops))
    });
    let total_node_pops = total_node_pops.ok_or_else(|| {
        BorsukError::InvalidStorage("V40 direct selection work overflows".to_owned())
    })?;
    let value = serde_json::json!({
        "artifact": {
            "blake3": blake3::hash(output_bytes).to_hex().to_string(),
            "encoded_bytes": output_bytes.len(),
            "role": output.role,
            "sha256": format!("{:x}", Sha256::digest(output_bytes)),
            "uri": format!("file://{}", output.path.display()),
        },
        "claim_eligible": false,
        "evidence": {
            "fma_backend": selections[0].fma_backend,
            "maximum_node_pops": V40_MAXIMUM_NODE_POPS,
            "query_count": selections.len(),
            "selected_postings": V40_DIRECT_SELECTED_POSTINGS,
            "total_node_pops": total_node_pops,
        },
        "inputs": inputs,
        "mode": "select-direct",
        "schema": "borsuk-v40-local-result-v1",
    });
    let mut bytes = serde_json::to_vec(&v40_canonical_json(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!("V40 result serialization failed: {error}"))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V40SelectionReceiptArtifact {
    blake3: String,
    encoded_bytes: u64,
    role: String,
    sha256: String,
    uri: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V40SelectionReceiptEvidence {
    fma_backend: String,
    maximum_node_pops: u64,
    query_count: u64,
    selected_postings: u64,
    total_node_pops: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V40SelectionReceipt {
    artifact: V40SelectionReceiptArtifact,
    claim_eligible: bool,
    evidence: V40SelectionReceiptEvidence,
    inputs: Vec<V40SelectionReceiptArtifact>,
    mode: String,
    schema: String,
}

fn valid_v40_local_file_uri(value: &str) -> bool {
    value
        .strip_prefix("file://")
        .is_some_and(|path| path.starts_with('/') && path.len() > 1 && !path.contains(['?', '#']))
}

fn parse_v40_selection_receipt_bytes(bytes: &[u8], selection: &V40LocalArtifact) -> Result<String> {
    let invalid =
        || BorsukError::InvalidStorage("V40 direct selection receipt authority differs".to_owned());
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let mut canonical = serde_json::to_vec(&v40_canonical_json(value)).map_err(|_| invalid())?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err(invalid());
    }
    let receipt: V40SelectionReceipt = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let artifact = &receipt.artifact;
    let evidence = &receipt.evidence;
    let input_roles = receipt
        .inputs
        .iter()
        .map(|input| input.role.as_str())
        .collect::<Vec<_>>();
    let input_uris = receipt
        .inputs
        .iter()
        .map(|input| input.uri.as_str())
        .collect::<BTreeSet<_>>();
    let maximum_total_node_pops = evidence
        .query_count
        .checked_mul(evidence.maximum_node_pops)
        .ok_or_else(invalid)?;
    if receipt.schema != "borsuk-v40-local-result-v1"
        || receipt.claim_eligible
        || receipt.mode != "select-direct"
        || artifact.role != "direct-selection"
        || !valid_v40_local_file_uri(&artifact.uri)
        || artifact.sha256 != selection.sha256
        || artifact.blake3 != selection.blake3
        || artifact.encoded_bytes != selection.encoded_bytes
        || evidence.query_count != V40_DIRECT_QUERY_COUNT
        || evidence.selected_postings != V40_DIRECT_SELECTED_POSTINGS as u64
        || evidence.maximum_node_pops != V40_MAXIMUM_NODE_POPS as u64
        || evidence.total_node_pops < evidence.query_count
        || evidence.total_node_pops > maximum_total_node_pops
        || !matches!(
            evidence.fma_backend.as_str(),
            "aarch64-neon-fma" | "x86-avx-fma"
        )
        || input_roles != ["v37-authority", "ownership-tree", "development-query"]
        || input_uris.len() != receipt.inputs.len()
        || receipt.inputs.iter().any(|input| {
            !valid_s3_uri(&input.uri)
                || !valid_digest(&input.sha256)
                || !valid_digest(&input.blake3)
                || input.encoded_bytes == 0
        })
    {
        return Err(invalid());
    }
    Ok(evidence.fma_backend.clone())
}

fn publish_v40_output(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
    output.write_all(bytes).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    output.sync_all().map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })
}

fn v40_direct_selection_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("selection_rank", DataType::UInt32, false),
        Field::new("posting_ordinal", DataType::UInt32, false),
        Field::new("node_pops", DataType::UInt32, false),
        Field::new("scored_internal_nodes", DataType::UInt32, false),
        Field::new("fma_backend", DataType::Utf8, false),
    ])
}

pub(crate) fn load_v40_projected_queries(
    path: &Path,
    expected_queries: u64,
) -> Result<Vec<Vec<f32>>> {
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    load_v40_projected_queries_file(file, path, expected_queries)
}

pub(crate) fn load_v40_projected_queries_file(
    file: File,
    display_path: &Path,
    expected_queries: u64,
) -> Result<Vec<Vec<f32>>> {
    if expected_queries == 0 {
        return Err(BorsukError::InvalidStorage(
            "V40 direct query count differs".to_owned(),
        ));
    }
    let capacity = usize::try_from(expected_queries).map_err(|_| {
        BorsukError::InvalidStorage("V40 direct query count exceeds address space".to_owned())
    })?;
    let projection = crate::v36_funnel_geometry::build_v36_srht192_control()?;
    let mut projected_queries = Vec::with_capacity(capacity);
    crate::v36_prefix_dataset::scan_v36_prefix_query_parquet_file(
        file,
        display_path,
        expected_queries,
        |batch| {
            for row in crate::v36_prefix_dataset::v36_prefix_query_rows_from_batch(
                &batch,
                0,
                batch.num_rows(),
            )? {
                let projected =
                    crate::v35_projection::project_v35_query_simd(&projection, &row.embedding)?;
                projected_queries.push(
                    projected
                        .coordinates()
                        .iter()
                        .map(|value| {
                            let value = *value as f32;
                            if value == 0.0 { 0.0 } else { value }
                        })
                        .collect(),
                );
            }
            Ok(())
        },
    )?;
    if projected_queries.len() != capacity {
        return Err(BorsukError::InvalidStorage(
            "V40 direct projected query count differs".to_owned(),
        ));
    }
    Ok(projected_queries)
}

fn validate_v40_direct_selections(
    records: &[V40DirectSelectionRecord],
    selected_postings: usize,
    expected_backend: Option<&str>,
) -> Result<()> {
    let invalid =
        || BorsukError::InvalidStorage("V40 direct selection authority differs".to_owned());
    if records.is_empty() || selected_postings == 0 {
        return Err(invalid());
    }
    for (query, record) in records.iter().enumerate() {
        let expected_query = u32::try_from(query).map_err(|_| invalid())?;
        let postings = record
            .posting_ordinals
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if record.query_ordinal != expected_query
            || record.posting_ordinals.len() != selected_postings
            || postings.len() != selected_postings
            || record.node_pops == 0
            || record.scored_internal_nodes == 0
            || record.scored_internal_nodes > record.node_pops
            || !matches!(
                record.fma_backend.as_str(),
                "aarch64-neon-fma" | "x86-avx-fma"
            )
            || expected_backend.is_some_and(|backend| record.fma_backend != backend)
        {
            return Err(invalid());
        }
    }
    if records
        .windows(2)
        .any(|pair| pair[0].fma_backend != pair[1].fma_backend)
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn encode_v40_direct_selections_parquet(
    records: &[V40DirectSelectionRecord],
    selected_postings: usize,
) -> Result<Vec<u8>> {
    validate_v40_direct_selections(records, selected_postings, None)?;
    let rows = records
        .len()
        .checked_mul(selected_postings)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V40 direct selection row count overflows".to_owned())
        })?;
    let mut queries = Vec::with_capacity(rows);
    let mut ranks = Vec::with_capacity(rows);
    let mut postings = Vec::with_capacity(rows);
    let mut node_pops = Vec::with_capacity(rows);
    let mut scores = Vec::with_capacity(rows);
    let mut backends = Vec::with_capacity(rows);
    for record in records {
        for (rank, posting) in record.posting_ordinals.iter().copied().enumerate() {
            queries.push(record.query_ordinal);
            ranks.push(u32::try_from(rank).map_err(|_| {
                BorsukError::InvalidStorage("V40 direct selection rank overflows".to_owned())
            })?);
            postings.push(posting);
            node_pops.push(record.node_pops);
            scores.push(record.scored_internal_nodes);
            backends.push(record.fma_backend.clone());
        }
    }
    let schema = Arc::new(v40_direct_selection_schema());
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(queries)),
            Arc::new(UInt32Array::from(ranks)),
            Arc::new(UInt32Array::from(postings)),
            Arc::new(UInt32Array::from(node_pops)),
            Arc::new(UInt32Array::from(scores)),
            Arc::new(StringArray::from(backends)),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(4_096))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

pub(crate) fn decode_v40_direct_selections_parquet(
    bytes: &[u8],
    expected_queries: u32,
    selected_postings: usize,
    expected_backend: &str,
) -> Result<Vec<V40DirectSelectionRecord>> {
    let invalid = || BorsukError::InvalidStorage("V40 direct selection Parquet differs".to_owned());
    if expected_queries == 0 || selected_postings == 0 {
        return Err(invalid());
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    if builder.schema().as_ref() != &v40_direct_selection_schema() {
        return Err(invalid());
    }
    let expected_rows = usize::try_from(expected_queries)
        .map_err(|_| invalid())?
        .checked_mul(selected_postings)
        .ok_or_else(invalid)?;
    let mut records = Vec::with_capacity(expected_queries as usize);
    let mut observed_rows = 0_usize;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.schema().as_ref() != &v40_direct_selection_schema()
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid());
        }
        let queries = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let ranks = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let postings = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let node_pops = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let scores = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let backends = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(invalid)?;
        for row in 0..batch.num_rows() {
            let flat_row = observed_rows.checked_add(row).ok_or_else(invalid)?;
            let query = flat_row / selected_postings;
            let rank = flat_row % selected_postings;
            if queries.value(row) as usize != query
                || ranks.value(row) as usize != rank
                || backends.value(row) != expected_backend
            {
                return Err(invalid());
            }
            if rank == 0 {
                records.push(V40DirectSelectionRecord {
                    query_ordinal: queries.value(row),
                    posting_ordinals: Vec::with_capacity(selected_postings),
                    node_pops: node_pops.value(row),
                    scored_internal_nodes: scores.value(row),
                    fma_backend: backends.value(row).to_owned(),
                });
            }
            let record = records.get_mut(query).ok_or_else(invalid)?;
            if record.node_pops != node_pops.value(row)
                || record.scored_internal_nodes != scores.value(row)
                || record.fma_backend != backends.value(row)
            {
                return Err(invalid());
            }
            record.posting_ordinals.push(postings.value(row));
        }
        observed_rows = observed_rows
            .checked_add(batch.num_rows())
            .ok_or_else(invalid)?;
    }
    if observed_rows != expected_rows || records.len() != expected_queries as usize {
        return Err(invalid());
    }
    validate_v40_direct_selections(
        records.as_slice(),
        selected_postings,
        Some(expected_backend),
    )?;
    Ok(records)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40TreeFrontier {
    pub(crate) posting_ordinals: Vec<u32>,
    pub(crate) node_pops: u32,
    pub(crate) scored_internal_nodes: u32,
    pub(crate) fma_backend: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V40EvaluationSpec {
    pub(crate) selected_postings: u32,
    pub(crate) gt_neighbors: u32,
    pub(crate) aggregate_gate_ppm: u32,
    pub(crate) minimum_gate_ppm: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40DirectSelectionRecord {
    pub(crate) query_ordinal: u32,
    pub(crate) posting_ordinals: Vec<u32>,
    pub(crate) node_pops: u32,
    pub(crate) scored_internal_nodes: u32,
    pub(crate) fma_backend: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40DirectSample {
    pub(crate) query_ordinal: u32,
    pub(crate) selected_postings: Vec<u32>,
    pub(crate) node_pops: u32,
    pub(crate) scored_internal_nodes: u32,
    pub(crate) hits: u32,
    pub(crate) recall_ppm: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40DirectEvaluation {
    pub(crate) samples: Vec<V40DirectSample>,
    pub(crate) total_hits: u64,
    pub(crate) aggregate_recall_ppm: u32,
    pub(crate) minimum_recall_ppm: u32,
    pub(crate) passed: bool,
    pub(crate) disposition: String,
}

pub(crate) fn select_v40_tree_frontier(
    tree: &V37BalancedTree,
    expected_backend: &str,
    query: &[f32],
    posting_limit: usize,
    maximum_node_pops: usize,
) -> Result<V40TreeFrontier> {
    if posting_limit == 0
        || posting_limit > V40_MAXIMUM_FRONTIER_POSTINGS
        || maximum_node_pops == 0
        || maximum_node_pops > V40_MAXIMUM_NODE_POPS
    {
        return Err(BorsukError::InvalidStorage(
            "V40 tree frontier authority differs".to_owned(),
        ));
    }
    let selection = select_v37_tree_postings_with_limit(
        tree,
        expected_backend,
        maximum_node_pops,
        posting_limit,
        query,
    )?;
    Ok(V40TreeFrontier {
        posting_ordinals: selection.selected_postings,
        node_pops: selection.node_visits,
        scored_internal_nodes: selection.scored_internal_nodes,
        fma_backend: selection.fma_backend,
    })
}

pub(crate) fn select_v40_direct_queries(
    tree: &V37BalancedTree,
    expected_backend: &str,
    queries: &[Vec<f32>],
    selected_postings: usize,
    maximum_node_pops: usize,
) -> Result<Vec<V40DirectSelectionRecord>> {
    let invalid = || BorsukError::InvalidStorage("V40 direct query authority differs".to_owned());
    if queries.is_empty() {
        return Err(invalid());
    }
    let mut records = Vec::with_capacity(queries.len());
    for (query_ordinal, query) in queries.iter().enumerate() {
        let frontier = select_v40_tree_frontier(
            tree,
            expected_backend,
            query,
            selected_postings,
            maximum_node_pops,
        )?;
        records.push(V40DirectSelectionRecord {
            query_ordinal: u32::try_from(query_ordinal).map_err(|_| invalid())?,
            posting_ordinals: frontier.posting_ordinals,
            node_pops: frontier.node_pops,
            scored_internal_nodes: frontier.scored_internal_nodes,
            fma_backend: frontier.fma_backend,
        });
    }
    validate_v40_direct_selections(&records, selected_postings, Some(expected_backend))?;
    Ok(records)
}

/// Run one authenticated local V40 direct phase without any storage client.
#[doc(hidden)]
pub fn run_v40_local_request(request: V40LocalRunRequest) -> Result<Vec<u8>> {
    if request.mode != V40LocalRunMode::SelectDirect {
        return Err(BorsukError::InvalidStorage(
            "V40 direct evaluation runner is not yet available".to_owned(),
        ));
    }
    let authenticated = authenticate_v40_local_request(&request)?;
    let authority_bytes = read_v40_authenticated_input(&request, &authenticated, "v37-authority")?;
    let binding = crate::v37_relation_router::v37_v40_selection_binding(&authority_bytes)?;
    if binding.workers != request.workers {
        return Err(BorsukError::InvalidStorage(
            "V40 direct worker authority differs".to_owned(),
        ));
    }
    let tree_input = v40_local_input(&request, "ownership-tree")?;
    let tree_bytes = read_v40_authenticated_input(&request, &authenticated, "ownership-tree")?;
    let tree = crate::v37_relation_router::decode_v37_tree_arrow(
        &tree_bytes,
        tree_input.encoded_bytes,
        &tree_input.sha256,
        &tree_input.blake3,
    )?;
    let corpus_rows = tree
        .leaf_populations
        .iter()
        .try_fold(0_u64, |total, population| total.checked_add(*population))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V40 direct tree population overflows".to_owned())
        })?;
    if tree.dimensions as u64 != binding.dimensions
        || tree.seed != binding.tree_seed
        || tree.fma_backend != binding.fma_backend
        || tree.leaf_populations.len() as u64 != binding.leaf_count
        || corpus_rows != binding.corpus_rows
    {
        return Err(BorsukError::InvalidStorage(
            "V40 direct tree binding differs".to_owned(),
        ));
    }
    let query_input = v40_local_input(&request, "development-query")?;
    let queries = load_v40_projected_queries_file(
        v40_authenticated_input_file(&request, &authenticated, "development-query")?,
        &query_input.path,
        V40_DIRECT_QUERY_COUNT,
    )?;
    let maximum_node_pops = V40_MAXIMUM_NODE_POPS.min(tree.nodes.len());
    let selections = select_v40_direct_queries(
        &tree,
        &binding.fma_backend,
        &queries,
        V40_DIRECT_SELECTED_POSTINGS,
        maximum_node_pops,
    )?;
    let output_bytes =
        encode_v40_direct_selections_parquet(&selections, V40_DIRECT_SELECTED_POSTINGS)?;
    let output = request.outputs.first().ok_or_else(|| {
        BorsukError::InvalidStorage("V40 direct selection output differs".to_owned())
    })?;
    publish_v40_output(&output.path, &output_bytes)?;
    v40_selection_receipt_bytes(&request, &selections, &output_bytes)
}

pub(crate) fn evaluate_v40_direct_recall(
    spec: &V40EvaluationSpec,
    owners: &[(u64, u32, Option<u32>)],
    selections: &[V40DirectSelectionRecord],
    truth: &[V37FeatureGroundTruth],
    expected_backend: &str,
) -> Result<V40DirectEvaluation> {
    let invalid =
        || BorsukError::InvalidStorage("V40 direct evaluation authority differs".to_owned());
    if spec.selected_postings == 0
        || spec.gt_neighbors == 0
        || spec.aggregate_gate_ppm > 1_000_000
        || spec.minimum_gate_ppm > 1_000_000
        || owners.is_empty()
        || selections.is_empty()
        || selections.len() != truth.len()
        || !matches!(expected_backend, "aarch64-neon-fma" | "x86-avx-fma")
    {
        return Err(invalid());
    }

    let mut ownership = BTreeMap::new();
    let mut known_postings = BTreeSet::new();
    for &(feature_id, primary, alternate) in owners {
        if alternate == Some(primary)
            || ownership.insert(feature_id, (primary, alternate)).is_some()
        {
            return Err(invalid());
        }
        known_postings.insert(primary);
        if let Some(posting) = alternate {
            known_postings.insert(posting);
        }
    }
    if ownership
        .keys()
        .copied()
        .ne(owners.iter().map(|owner| owner.0))
    {
        return Err(invalid());
    }

    let selected_count = usize::try_from(spec.selected_postings).map_err(|_| invalid())?;
    let truth_count = usize::try_from(spec.gt_neighbors).map_err(|_| invalid())?;
    let mut samples = Vec::with_capacity(selections.len());
    let mut total_hits = 0_u64;
    let mut minimum_recall_ppm = 1_000_000_u32;

    for (expected_query, (selection, ground_truth)) in selections.iter().zip(truth).enumerate() {
        let expected_query = u32::try_from(expected_query).map_err(|_| invalid())?;
        let selected: BTreeSet<_> = selection.posting_ordinals.iter().copied().collect();
        let truth_ids: BTreeSet<_> = ground_truth.feature_row_ids.iter().copied().collect();
        if selection.query_ordinal != expected_query
            || ground_truth.query_ordinal != expected_query
            || selection.posting_ordinals.len() != selected_count
            || selected.len() != selected_count
            || !selected.is_subset(&known_postings)
            || selection.node_pops == 0
            || selection.scored_internal_nodes == 0
            || selection.scored_internal_nodes > selection.node_pops
            || selection.fma_backend != expected_backend
            || ground_truth.feature_row_ids.len() != truth_count
            || truth_ids.len() != truth_count
        {
            return Err(invalid());
        }

        let mut hits = 0_u32;
        for feature_id in &ground_truth.feature_row_ids {
            let (primary, alternate) = ownership.get(feature_id).ok_or_else(invalid)?;
            if selected.contains(primary)
                || alternate.is_some_and(|posting| selected.contains(&posting))
            {
                hits = hits.checked_add(1).ok_or_else(invalid)?;
            }
        }
        let recall_ppm = u32::try_from(
            u64::from(hits).checked_mul(1_000_000).ok_or_else(invalid)?
                / u64::from(spec.gt_neighbors),
        )
        .map_err(|_| invalid())?;
        total_hits = total_hits
            .checked_add(u64::from(hits))
            .ok_or_else(invalid)?;
        minimum_recall_ppm = minimum_recall_ppm.min(recall_ppm);
        samples.push(V40DirectSample {
            query_ordinal: expected_query,
            selected_postings: selection.posting_ordinals.clone(),
            node_pops: selection.node_pops,
            scored_internal_nodes: selection.scored_internal_nodes,
            hits,
            recall_ppm,
        });
    }

    let possible_hits = u64::try_from(selections.len())
        .map_err(|_| invalid())?
        .checked_mul(u64::from(spec.gt_neighbors))
        .ok_or_else(invalid)?;
    let aggregate_recall_ppm =
        u32::try_from(total_hits.checked_mul(1_000_000).ok_or_else(invalid)? / possible_hits)
            .map_err(|_| invalid())?;
    let passed = aggregate_recall_ppm >= spec.aggregate_gate_ppm
        && minimum_recall_ppm >= spec.minimum_gate_ppm;
    Ok(V40DirectEvaluation {
        samples,
        total_hits,
        aggregate_recall_ppm,
        minimum_recall_ppm,
        passed,
        disposition: if passed {
            "direct-passed"
        } else {
            "direct-failed"
        }
        .to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use std::{fs, fs::File, path::PathBuf, sync::Arc};

    use super::super::v37_relation_router::{
        V37BalancedNode, V37BalancedTree, score_v37_hyperplane_fused,
    };
    use super::{
        V40DirectSelectionRecord, V40EvaluationSpec, V40LocalArtifact, V40LocalOutput,
        V40LocalRunMode, V40LocalRunRequest, authenticate_v40_local_request,
        decode_v40_direct_selections_parquet, encode_v40_direct_selections_parquet,
        evaluate_v40_direct_recall, load_v40_projected_queries, load_v40_projected_queries_file,
        parse_v40_selection_receipt_bytes, select_v40_direct_queries, select_v40_tree_frontier,
    };
    use crate::v35_projection::project_v35_query_simd;
    use crate::v36_funnel_geometry::build_v36_srht192_control;
    use crate::v36_prefix_dataset::{v36_prefix_query_schema, write_v36_prefix_query_parquet};
    use crate::v37_relation_router::V37FeatureGroundTruth;
    use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch, UInt32Array, UInt64Array};
    use arrow_schema::{DataType, Field};
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    fn four_leaf_tree() -> V37BalancedTree {
        let normal = vec![1.0_f32, 0.0, 0.0];
        let leaf = |posting_ordinal| V37BalancedNode {
            normal: Vec::new(),
            boundary_score_bits: 0.0_f32.to_bits(),
            boundary_source_ordinal: 0,
            left_node: None,
            right_node: None,
            posting_ordinal: Some(posting_ordinal),
            population: 1,
        };
        let branch = |population, left_node, right_node| V37BalancedNode {
            normal: normal.clone(),
            boundary_score_bits: 0.0_f32.to_bits(),
            boundary_source_ordinal: 0,
            left_node: Some(left_node),
            right_node: Some(right_node),
            posting_ordinal: None,
            population,
        };
        let backend = score_v37_hyperplane_fused(&[0.0, 0.0, 0.0], &[1.0, 0.0, 0.0])
            .unwrap()
            .1
            .to_owned();
        V37BalancedTree {
            dimensions: 3,
            seed: 40,
            fma_backend: backend,
            nodes: vec![
                branch(4, 1, 4),
                branch(2, 2, 3),
                leaf(0),
                leaf(1),
                branch(2, 5, 6),
                leaf(2),
                leaf(3),
            ],
            leaf_populations: vec![1; 4],
            assignments: Vec::new(),
        }
    }

    #[test]
    fn v40_tree_frontier_matches_exhaustive_order_and_fails_closed() {
        let tree = four_leaf_tree();
        let frontier =
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[1.0, 0.0, 0.0], 4, 7).unwrap();
        assert_eq!(frontier.posting_ordinals, vec![3, 0, 1, 2]);
        assert_eq!(frontier.node_pops, 7);
        assert_eq!(frontier.scored_internal_nodes, 3);
        assert_eq!(frontier.fma_backend, tree.fma_backend);

        assert!(
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[1.0, 0.0, 0.0], 5, 7).is_err()
        );
        assert!(
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[1.0, 0.0, 0.0], 4, 6).is_err()
        );
    }

    #[test]
    fn v40_tree_frontier_handles_equal_margins_signed_zero_and_bad_inputs() {
        let tree = four_leaf_tree();
        let frontier =
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[-0.0, 0.0, 0.0], 4, 7).unwrap();
        assert_eq!(frontier.posting_ordinals, vec![0, 1, 2, 3]);

        assert!(select_v40_tree_frontier(&tree, "scalar-control", &[0.0; 3], 4, 7).is_err());
        assert!(
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[f32::NAN, 0.0, 0.0], 4, 7)
                .is_err()
        );
        assert!(select_v40_tree_frontier(&tree, &tree.fma_backend, &[0.0; 3], 0, 7).is_err());
        assert!(select_v40_tree_frontier(&tree, &tree.fma_backend, &[0.0; 3], 4, 0).is_err());
    }

    fn direct_evaluation_fixture() -> (
        V40EvaluationSpec,
        Vec<(u64, u32, Option<u32>)>,
        Vec<V40DirectSelectionRecord>,
        Vec<V37FeatureGroundTruth>,
    ) {
        let spec = V40EvaluationSpec {
            selected_postings: 2,
            gt_neighbors: 4,
            aggregate_gate_ppm: 750_000,
            minimum_gate_ppm: 750_000,
        };
        let owners = vec![
            (10, 0, Some(1)),
            (11, 2, Some(1)),
            (12, 3, Some(0)),
            (13, 4, None),
            (20, 2, Some(0)),
            (21, 3, None),
            (22, 4, Some(5)),
            (23, 0, None),
        ];
        let selections = vec![
            V40DirectSelectionRecord {
                query_ordinal: 0,
                posting_ordinals: vec![0, 1],
                node_pops: 5,
                scored_internal_nodes: 3,
                fma_backend: "aarch64-neon-fma".to_owned(),
            },
            V40DirectSelectionRecord {
                query_ordinal: 1,
                posting_ordinals: vec![0, 3],
                node_pops: 6,
                scored_internal_nodes: 4,
                fma_backend: "aarch64-neon-fma".to_owned(),
            },
        ];
        let truth = vec![
            V37FeatureGroundTruth {
                query_ordinal: 0,
                feature_row_ids: vec![10, 11, 12, 13],
            },
            V37FeatureGroundTruth {
                query_ordinal: 1,
                feature_row_ids: vec![20, 21, 22, 23],
            },
        ];
        (spec, owners, selections, truth)
    }

    #[test]
    fn v40_direct_evaluation_counts_two_owner_hits_once_and_enforces_gates() {
        let (spec, owners, selections, truth) = direct_evaluation_fixture();
        let result =
            evaluate_v40_direct_recall(&spec, &owners, &selections, &truth, "aarch64-neon-fma")
                .unwrap();
        assert_eq!(result.total_hits, 6);
        assert_eq!(result.aggregate_recall_ppm, 750_000);
        assert_eq!(result.minimum_recall_ppm, 750_000);
        assert_eq!(result.samples[0].hits, 3);
        assert_eq!(result.samples[0].recall_ppm, 750_000);
        assert_eq!(result.samples[1].hits, 3);
        assert!(result.passed);
        assert_eq!(result.disposition, "direct-passed");
    }

    #[test]
    fn v40_direct_evaluation_rejects_selection_truth_and_backend_drift() {
        let (spec, owners, selections, truth) = direct_evaluation_fixture();

        let mut duplicated = selections.clone();
        duplicated[0].posting_ordinals = vec![0, 0];
        assert!(
            evaluate_v40_direct_recall(&spec, &owners, &duplicated, &truth, "aarch64-neon-fma")
                .is_err()
        );

        let mut skipped_query = selections.clone();
        skipped_query[1].query_ordinal = 2;
        assert!(
            evaluate_v40_direct_recall(&spec, &owners, &skipped_query, &truth, "aarch64-neon-fma")
                .is_err()
        );

        let mut unknown_truth = truth.clone();
        unknown_truth[0].feature_row_ids[0] = 99;
        assert!(
            evaluate_v40_direct_recall(
                &spec,
                &owners,
                &selections,
                &unknown_truth,
                "aarch64-neon-fma"
            )
            .is_err()
        );

        assert!(
            evaluate_v40_direct_recall(&spec, &owners, &selections, &truth, "x86-avx-fma").is_err()
        );
    }

    fn local_artifact(role: &str) -> V40LocalArtifact {
        V40LocalArtifact::try_new(
            role.to_owned(),
            PathBuf::from(format!("/tmp/v40-{role}")),
            format!("s3://fixture/v40/{role}"),
            "1".repeat(64),
            "2".repeat(64),
            17,
        )
        .unwrap()
    }

    fn local_output(role: &str) -> V40LocalOutput {
        V40LocalOutput::try_new(role.to_owned(), PathBuf::from(format!("/tmp/v40-{role}"))).unwrap()
    }

    #[test]
    fn v40_authority_direct_modes_separate_query_and_truth_capabilities() {
        let selection_inputs = ["v37-authority", "ownership-tree", "development-query"]
            .map(local_artifact)
            .to_vec();
        let selection = V40LocalRunRequest::try_new(
            V40LocalRunMode::SelectDirect,
            selection_inputs,
            vec![local_output("direct-selection")],
            16,
        )
        .unwrap();
        assert_eq!(
            selection.input_roles(),
            vec!["v37-authority", "ownership-tree", "development-query"]
        );
        assert_eq!(selection.output_roles(), vec!["direct-selection"]);

        let evaluation_inputs = [
            "v38-ceiling-authority",
            "v38-construction-result",
            "spill-relation",
            "spill-postings",
            "development-ground-truth",
            "direct-selection-result",
            "direct-selection",
        ]
        .map(local_artifact)
        .to_vec();
        let evaluation = V40LocalRunRequest::try_new(
            V40LocalRunMode::EvaluateDirect,
            evaluation_inputs,
            vec![local_output("direct-result")],
            1,
        )
        .unwrap();
        assert_eq!(evaluation.output_roles(), vec!["direct-result"]);
        assert!(!evaluation.input_roles().contains(&"development-query"));
        assert!(
            !selection
                .input_roles()
                .contains(&"development-ground-truth")
        );
    }

    #[test]
    fn v40_authority_direct_modes_reject_identity_role_and_path_drift() {
        assert!(
            V40LocalArtifact::try_new(
                "ownership-tree".to_owned(),
                PathBuf::from("/tmp/tree"),
                "file:///tmp/tree".to_owned(),
                "1".repeat(64),
                "2".repeat(64),
                17,
            )
            .is_err()
        );
        assert!(
            V40LocalArtifact::try_new(
                "ownership-tree".to_owned(),
                PathBuf::from("/tmp/tree"),
                "s3://fixture/tree".to_owned(),
                "1".repeat(63),
                "2".repeat(64),
                17,
            )
            .is_err()
        );

        let mut inputs = ["v37-authority", "ownership-tree", "development-query"]
            .map(local_artifact)
            .to_vec();
        inputs.swap(0, 1);
        assert!(
            V40LocalRunRequest::try_new(
                V40LocalRunMode::SelectDirect,
                inputs,
                vec![local_output("direct-selection")],
                16,
            )
            .is_err()
        );

        let mut overlap = ["v37-authority", "ownership-tree", "development-query"]
            .map(local_artifact)
            .to_vec();
        overlap[1] = overlap[0].clone();
        assert!(
            V40LocalRunRequest::try_new(
                V40LocalRunMode::SelectDirect,
                overlap,
                vec![local_output("direct-selection")],
                16,
            )
            .is_err()
        );

        let inputs = ["v37-authority", "ownership-tree", "development-query"]
            .map(local_artifact)
            .to_vec();
        assert!(
            V40LocalRunRequest::try_new(
                V40LocalRunMode::SelectDirect,
                inputs,
                vec![
                    V40LocalOutput::try_new(
                        "direct-selection".to_owned(),
                        PathBuf::from("/tmp/v40-ownership-tree"),
                    )
                    .unwrap()
                ],
                3,
            )
            .is_err()
        );
    }

    fn local_artifact_bytes(root: &std::path::Path, role: &str, bytes: &[u8]) -> V40LocalArtifact {
        let path = root.join(role);
        fs::write(&path, bytes).unwrap();
        V40LocalArtifact::try_new(
            role.to_owned(),
            path,
            format!("s3://fixture/v40/{role}"),
            format!("{:x}", Sha256::digest(bytes)),
            blake3::hash(bytes).to_hex().to_string(),
            bytes.len() as u64,
        )
        .unwrap()
    }

    #[test]
    fn v40_direct_artifact_authentication_rejects_byte_and_output_drift() {
        let root = tempdir().unwrap();
        let inputs = [
            ("v37-authority", b"authority".as_slice()),
            ("ownership-tree", b"tree".as_slice()),
            ("development-query", b"query".as_slice()),
        ]
        .map(|(role, bytes)| local_artifact_bytes(root.path(), role, bytes))
        .to_vec();
        let request = V40LocalRunRequest::try_new(
            V40LocalRunMode::SelectDirect,
            inputs,
            vec![
                V40LocalOutput::try_new(
                    "direct-selection".to_owned(),
                    root.path().join("selection.parquet"),
                )
                .unwrap(),
            ],
            16,
        )
        .unwrap();
        assert!(authenticate_v40_local_request(&request).is_ok());

        fs::write(root.path().join("ownership-tree"), b"drift").unwrap();
        assert!(authenticate_v40_local_request(&request).is_err());

        fs::write(root.path().join("ownership-tree"), b"tree").unwrap();
        fs::write(root.path().join("selection.parquet"), b"occupied").unwrap();
        assert!(authenticate_v40_local_request(&request).is_err());
    }

    #[test]
    fn v40_direct_artifact_selection_parquet_round_trips_and_fails_closed() {
        let (_, _, selections, _) = direct_evaluation_fixture();
        let bytes = encode_v40_direct_selections_parquet(&selections, 2).unwrap();
        assert_eq!(
            decode_v40_direct_selections_parquet(&bytes, 2, 2, "aarch64-neon-fma").unwrap(),
            selections
        );
        assert!(decode_v40_direct_selections_parquet(&bytes, 2, 3, "aarch64-neon-fma").is_err());
        assert!(
            decode_v40_direct_selections_parquet(
                &bytes[..bytes.len() - 1],
                2,
                2,
                "aarch64-neon-fma"
            )
            .is_err()
        );

        let mut reordered = selections.clone();
        reordered.swap(0, 1);
        assert!(encode_v40_direct_selections_parquet(&reordered, 2).is_err());
        let mut backend_drift = selections;
        backend_drift[1].fma_backend = "x86-avx-fma".to_owned();
        assert!(encode_v40_direct_selections_parquet(&backend_drift, 2).is_err());
    }

    #[test]
    fn v40_direct_selection_receipt_binds_artifact_and_backend() {
        let root = tempdir().unwrap();
        let selections = (0..1_000_u32)
            .map(|query_ordinal| V40DirectSelectionRecord {
                query_ordinal,
                posting_ordinals: (0..21_u32).collect(),
                node_pops: 41,
                scored_internal_nodes: 20,
                fma_backend: "aarch64-neon-fma".to_owned(),
            })
            .collect::<Vec<_>>();
        let selection_bytes = encode_v40_direct_selections_parquet(&selections, 21).unwrap();
        let output_path = root.path().join("selection.parquet");
        let request = V40LocalRunRequest::try_new(
            V40LocalRunMode::SelectDirect,
            ["v37-authority", "ownership-tree", "development-query"]
                .map(local_artifact)
                .to_vec(),
            vec![
                V40LocalOutput::try_new("direct-selection".to_owned(), output_path.clone())
                    .unwrap(),
            ],
            4,
        )
        .unwrap();
        let receipt =
            super::v40_selection_receipt_bytes(&request, &selections, &selection_bytes).unwrap();
        let selection = V40LocalArtifact::try_new(
            "direct-selection".to_owned(),
            output_path,
            "s3://fixture/v40/direct-selection".to_owned(),
            format!("{:x}", Sha256::digest(&selection_bytes)),
            blake3::hash(&selection_bytes).to_hex().to_string(),
            u64::try_from(selection_bytes.len()).unwrap(),
        )
        .unwrap();

        assert_eq!(
            parse_v40_selection_receipt_bytes(&receipt, &selection).unwrap(),
            "aarch64-neon-fma"
        );
        assert!(
            parse_v40_selection_receipt_bytes(&receipt[..receipt.len() - 1], &selection).is_err()
        );
    }

    #[test]
    fn v40_direct_query_loader_reuses_frozen_srht_projection() {
        let root = tempdir().unwrap();
        let path = root.path().join("queries.parquet");
        let first = (0..768)
            .map(|index| (index as f32 + 1.0) / 1_024.0)
            .collect::<Vec<_>>();
        let second = first.iter().map(|value| -*value).collect::<Vec<_>>();
        let values = first.iter().chain(&second).copied().collect::<Vec<_>>();
        let embeddings = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            768,
            Arc::new(Float32Array::from(values)),
            None,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(v36_prefix_query_schema()),
            vec![
                Arc::new(UInt32Array::from(vec![0, 1])),
                Arc::new(UInt64Array::from(vec![10, 11])),
                Arc::new(embeddings),
            ],
        )
        .unwrap();
        write_v36_prefix_query_parquet(&path, [batch]).unwrap();

        let observed = load_v40_projected_queries(&path, 2).unwrap();
        let projection = build_v36_srht192_control().unwrap();
        let expected = [first, second]
            .iter()
            .map(|query| {
                project_v35_query_simd(&projection, query)
                    .unwrap()
                    .coordinates()
                    .iter()
                    .map(|value| {
                        let value = *value as f32;
                        if value == 0.0 { 0.0 } else { value }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(observed, expected);
        assert!(load_v40_projected_queries(&path, 1).is_err());
    }

    #[test]
    fn v40_direct_query_loader_consumes_the_authenticated_open_inode() {
        let root = tempdir().unwrap();
        let path = root.path().join("queries.parquet");
        let values = (0..2 * 768)
            .map(|index| (index as f32 + 1.0) / 2_048.0)
            .collect::<Vec<_>>();
        let embeddings = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            768,
            Arc::new(Float32Array::from(values)),
            None,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(v36_prefix_query_schema()),
            vec![
                Arc::new(UInt32Array::from(vec![0, 1])),
                Arc::new(UInt64Array::from(vec![10, 11])),
                Arc::new(embeddings),
            ],
        )
        .unwrap();
        write_v36_prefix_query_parquet(&path, [batch]).unwrap();
        let file = File::open(&path).unwrap();
        fs::remove_file(&path).unwrap();

        let observed = load_v40_projected_queries_file(file, &path, 2).unwrap();
        assert_eq!(observed.len(), 2);
    }

    #[test]
    fn v40_direct_query_selector_seals_backend_and_work_evidence() {
        let tree = four_leaf_tree();
        let selections = select_v40_direct_queries(
            &tree,
            &tree.fma_backend,
            &[vec![1.0, 0.0, 0.0], vec![-1.0, 0.0, 0.0]],
            2,
            7,
        )
        .unwrap();
        assert_eq!(selections.len(), 2);
        assert_eq!(selections[0].query_ordinal, 0);
        assert_eq!(selections[0].posting_ordinals, vec![3, 0]);
        assert_eq!(selections[1].query_ordinal, 1);
        assert_eq!(selections[1].posting_ordinals, vec![0, 1]);
        assert!(
            selections
                .iter()
                .all(|selection| selection.fma_backend == tree.fma_backend)
        );
        assert!(select_v40_direct_queries(&tree, &tree.fma_backend, &[], 2, 7).is_err());
    }
}
