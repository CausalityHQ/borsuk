use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, BooleanArray, FixedSizeListArray, Float32Array, RecordBatch, StringArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    v24_witness::{V24ObjectIdentity, V24SourceRow, validate_v24_identity},
    v24_witness_graph::{
        V24WitnessGraph, V24WitnessSampler, build_v24_witness_graph, read_v24_witness_graph,
        read_v24_witnesses, write_v24_witness_graph, write_v24_witnesses,
    },
    v24_witness_postings::{
        V24PostingPage, V24PostingPageRow, build_v24_witness_postings, write_v24_witness_postings,
    },
};

const V24_LOCAL_MANIFEST_SCHEMA: &str = "borsuk-v24-local-manifest-v1";
const V24_TRAINING_RESULT_SCHEMA: &str = "borsuk-v24-training-result-v1";
const CONSTRUCTION_ROWS_FILE: &str = "construction-rows.parquet";
const WITNESSES_FILE: &str = "witnesses.arrow";
const WITNESS_GRAPH_FILE: &str = "witness-graph.arrow";
const RESULT_FILE: &str = "result.json";
const PAGE_ROWS_FILE: &str = "page-rows.parquet";
const POSTINGS_FILE: &str = "witness-postings.arrow";
const POSTING_SCRATCH_DIR: &str = ".posting-scratch";

/// One offline V24 scientific phase.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V24LocalPhase {
    /// Construct deterministic witnesses and their graph.
    TrainWitnesses,
    /// Stream page rows and construct witness-to-page postings.
    BuildPostings,
    /// Evaluate the preregistered development ladder.
    EvaluateDevelopment,
    /// Bind sealed holdout truth without exposing it to construction.
    BindHoldout,
    /// Evaluate one sealed cell on holdout.
    EvaluateHoldout,
}

/// Strict local-file request for one V24 phase.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V24LocalRunRequest {
    /// Canonical phase manifest.
    pub manifest: PathBuf,
    /// Complete authenticated phase input directory.
    pub input_dir: PathBuf,
    /// Empty output directory owned by this phase.
    pub output_dir: PathBuf,
    /// Exactly one phase to execute.
    pub phase: V24LocalPhase,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V24TrainingManifest {
    schema: String,
    claim_eligible: bool,
    generation: String,
    phase: String,
    seed: u64,
    source_row_count: u64,
    witness_count: u64,
    inputs: Vec<V24ObjectIdentity>,
    output_uris: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V24PostingManifest {
    schema: String,
    claim_eligible: bool,
    generation: String,
    phase: String,
    source_row_count: u64,
    witness_count: u64,
    inputs: Vec<V24ObjectIdentity>,
    output_uris: BTreeMap<String, String>,
}

/// Canonical claim-ineligible output of the witness-training phase.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V24TrainingResult {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) phase: String,
    pub(crate) generation: String,
    pub(crate) seed: u64,
    pub(crate) source_row_count: u64,
    pub(crate) witness_count: u64,
    pub(crate) inputs: Vec<V24ObjectIdentity>,
    pub(crate) outputs: Vec<V24ObjectIdentity>,
}

/// Canonical claim-ineligible output of the posting-construction phase.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V24PostingResult {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) phase: String,
    pub(crate) generation: String,
    pub(crate) source_row_count: u64,
    pub(crate) witness_count: u64,
    pub(crate) unique_source_rows: u64,
    pub(crate) physical_source_rows: u64,
    pub(crate) inputs: Vec<V24ObjectIdentity>,
    pub(crate) outputs: Vec<V24ObjectIdentity>,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn validate_request(request: &V24LocalRunRequest) -> Result<()> {
    if !request.manifest.is_file()
        || !request.input_dir.is_dir()
        || !request.output_dir.is_dir()
        || fs::read_dir(&request.output_dir)
            .map_err(|source| BorsukError::Io {
                path: request.output_dir.clone(),
                source,
            })?
            .next()
            .is_some()
    {
        return Err(invalid("V24 local request path authority differs"));
    }
    Ok(())
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    fn canonicalize(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.into_iter().map(canonicalize).collect())
            }
            serde_json::Value::Object(values) => serde_json::Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, canonicalize(value)))
                    .collect(),
            ),
            scalar => scalar,
        }
    }

    let value = serde_json::to_value(value)
        .map_err(|error| invalid(&format!("V24 local JSON serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonicalize(value))
        .map_err(|error| invalid(&format!("V24 local JSON serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn sha256_identity(role: &str, uri: &str, generation: &str, bytes: &[u8]) -> V24ObjectIdentity {
    V24ObjectIdentity {
        role: role.to_owned(),
        uri: uri.to_owned(),
        digest_algorithm: "sha256".to_owned(),
        digest: format!("{:x}", Sha256::digest(bytes)),
        encoded_bytes: bytes.len() as u64,
        generation: generation.to_owned(),
    }
}

fn sha256_file_identity(
    path: &Path,
    role: &str,
    uri: &str,
    generation: &str,
) -> Result<V24ObjectIdentity> {
    use std::io::Read;

    let mut file = fs::File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut hasher = Sha256::new();
    let mut encoded_bytes = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        encoded_bytes = encoded_bytes
            .checked_add(read as u64)
            .ok_or_else(|| invalid("V24 local input length overflows"))?;
    }
    Ok(V24ObjectIdentity {
        role: role.to_owned(),
        uri: uri.to_owned(),
        digest_algorithm: "sha256".to_owned(),
        digest: format!("{:x}", hasher.finalize()),
        encoded_bytes,
        generation: generation.to_owned(),
    })
}

fn exact_directory_file(input_dir: &Path, expected_name: &str) -> Result<PathBuf> {
    let mut names = fs::read_dir(input_dir)
        .map_err(|source| BorsukError::Io {
            path: input_dir.to_owned(),
            source,
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name())
                .map_err(|source| BorsukError::Io {
                    path: input_dir.to_owned(),
                    source,
                })
        })
        .collect::<Result<Vec<_>>>()?;
    names.sort();
    if names != [expected_name] {
        return Err(invalid("V24 local input inventory differs"));
    }
    Ok(input_dir.join(expected_name))
}

fn construction_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
            ),
            false,
        ),
    ])
}

fn page_rows_schema() -> Schema {
    Schema::new(vec![
        Field::new("page_ordinal", DataType::UInt32, false),
        Field::new("replica", DataType::Boolean, false),
        Field::new("record_id", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
            ),
            false,
        ),
    ])
}

struct V24PageRows {
    batches: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    row: usize,
    buffered: Option<(u32, bool, V24PostingPageRow)>,
    last_page: Option<u32>,
}

impl V24PageRows {
    fn open(path: &Path, source_row_count: u64) -> Result<Self> {
        let file = fs::File::open(path).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let physical_rows = builder.metadata().file_metadata().num_rows();
        if builder.schema().as_ref() != &page_rows_schema()
            || physical_rows <= 0
            || u64::try_from(physical_rows).ok().is_none_or(|rows| {
                rows < source_row_count || rows > source_row_count.saturating_mul(2)
            })
        {
            return Err(invalid("V24 page-row Parquet authority differs"));
        }
        Ok(Self {
            batches: builder.build()?,
            batch: None,
            row: 0,
            buffered: None,
            last_page: None,
        })
    }

    fn next_row(&mut self) -> Result<Option<(u32, bool, V24PostingPageRow)>> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.num_rows()
            {
                if batch.num_columns() != 4
                    || batch
                        .columns()
                        .iter()
                        .any(|column| column.null_count() != 0)
                {
                    return Err(invalid("V24 page-row Parquet batch differs"));
                }
                let page = batch.columns()[0]
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| invalid("V24 page-row page column differs"))?
                    .value(self.row);
                let replica = batch.columns()[1]
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| invalid("V24 page-row replica column differs"))?
                    .value(self.row);
                let record_id = batch.columns()[2]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("V24 page-row record ID column differs"))?
                    .value(self.row)
                    .as_bytes()
                    .to_vec();
                let vectors = batch.columns()[3]
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| invalid("V24 page-row vector column differs"))?;
                let values = vectors
                    .values()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| invalid("V24 page-row vector child differs"))?;
                let vector = values.values()[self.row * 96..(self.row + 1) * 96]
                    .try_into()
                    .unwrap();
                self.row += 1;
                return Ok(Some((
                    page,
                    replica,
                    V24PostingPageRow { record_id, vector },
                )));
            }
            self.batch = match self.batches.next() {
                Some(batch) => Some(batch?),
                None => return Ok(None),
            };
            self.row = 0;
        }
    }
}

impl Iterator for V24PageRows {
    type Item = Result<V24PostingPage>;

    fn next(&mut self) -> Option<Self::Item> {
        let first = match self.buffered.take() {
            Some(row) => row,
            None => match self.next_row() {
                Ok(Some(row)) => row,
                Ok(None) => return None,
                Err(error) => return Some(Err(error)),
            },
        };
        if self.last_page.is_some_and(|last| first.0 <= last) {
            return Some(Err(invalid("V24 page-row order differs")));
        }
        let page_ordinal = first.0;
        let mut primary_rows = Vec::new();
        let mut replica_rows = Vec::new();
        if first.1 {
            replica_rows.push(first.2);
        } else {
            primary_rows.push(first.2);
        }
        loop {
            match self.next_row() {
                Ok(Some(row)) if row.0 == page_ordinal => {
                    if row.1 {
                        replica_rows.push(row.2);
                    } else {
                        primary_rows.push(row.2);
                    }
                }
                Ok(Some(row)) if row.0 > page_ordinal => {
                    self.buffered = Some(row);
                    break;
                }
                Ok(Some(_)) => return Some(Err(invalid("V24 page-row order differs"))),
                Ok(None) => break,
                Err(error) => return Some(Err(error)),
            }
        }
        self.last_page = Some(page_ordinal);
        Some(Ok(V24PostingPage {
            page_ordinal,
            primary_rows,
            replica_rows,
        }))
    }
}

fn sample_training_rows(
    path: &Path,
    expected_rows: u64,
    witness_count: usize,
    seed: u64,
) -> Result<Vec<crate::v24_witness_graph::V24Witness>> {
    let file = fs::File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    if builder.schema().as_ref() != &construction_schema()
        || builder.metadata().file_metadata().num_rows() != i64::try_from(expected_rows).unwrap()
    {
        return Err(invalid("V24 construction Parquet authority differs"));
    }
    let mut sampler = V24WitnessSampler::new(witness_count, seed)?;
    let mut next_ordinal = 0_u64;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 2
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V24 construction Parquet batch differs"));
        }
        let ordinals = batch.columns()[0]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V24 construction ordinal column differs"))?;
        let vectors = batch.columns()[1]
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V24 construction vector column differs"))?;
        let values = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V24 construction vector child differs"))?;
        for row in 0..batch.num_rows() {
            if ordinals.value(row) != next_ordinal {
                return Err(invalid("V24 construction source order differs"));
            }
            let vector: [f32; 96] = values.values()[row * 96..(row + 1) * 96]
                .try_into()
                .unwrap();
            sampler.consider(V24SourceRow {
                source_ordinal: next_ordinal,
                vector,
            })?;
            next_ordinal += 1;
        }
    }
    if next_ordinal != expected_rows {
        return Err(invalid("V24 construction row count differs"));
    }
    sampler.finish()
}

fn write_owned_file(output_dir: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let temporary = output_dir.join(format!(".{name}.tmp"));
    let final_path = output_dir.join(name);
    let result = (|| -> Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|source| BorsukError::Io {
                path: temporary.clone(),
                source,
            })?;
        file.write_all(bytes).map_err(|source| BorsukError::Io {
            path: temporary.clone(),
            source,
        })?;
        file.sync_all().map_err(|source| BorsukError::Io {
            path: temporary.clone(),
            source,
        })?;
        fs::rename(&temporary, &final_path).map_err(|source| BorsukError::Io {
            path: final_path.clone(),
            source,
        })?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn cleanup_training_outputs(output_dir: &Path) {
    for name in [WITNESSES_FILE, WITNESS_GRAPH_FILE, RESULT_FILE] {
        let _ = fs::remove_file(output_dir.join(name));
        let _ = fs::remove_file(output_dir.join(format!(".{name}.tmp")));
    }
}

fn run_training(request: &V24LocalRunRequest) -> Result<Vec<u8>> {
    let manifest_bytes = fs::read(&request.manifest).map_err(|source| BorsukError::Io {
        path: request.manifest.clone(),
        source,
    })?;
    let manifest: V24TrainingManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| invalid(&format!("V24 training manifest differs: {error}")))?;
    if canonical_json_bytes(&manifest)? != manifest_bytes
        || manifest.schema != V24_LOCAL_MANIFEST_SCHEMA
        || manifest.claim_eligible
        || manifest.phase != "witness-training"
        || manifest.generation.is_empty()
        || manifest.source_row_count == 0
        || manifest.witness_count < 2
        || manifest.witness_count > u64::from(u32::MAX)
        || manifest.witness_count > manifest.source_row_count
        || manifest.inputs.len() != 1
        || manifest.output_uris.len() != 2
        || manifest
            .output_uris
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != ["witness-graph", "witnesses-arrow"]
    {
        return Err(invalid("V24 training manifest authority differs"));
    }
    let path = exact_directory_file(&request.input_dir, CONSTRUCTION_ROWS_FILE)?;
    let registered_input = &manifest.inputs[0];
    let observed_input = sha256_file_identity(
        &path,
        "construction-rows-parquet",
        &registered_input.uri,
        &manifest.generation,
    )?;
    validate_v24_identity(&observed_input, registered_input)?;

    let witnesses = sample_training_rows(
        &path,
        manifest.source_row_count,
        usize::try_from(manifest.witness_count).unwrap(),
        manifest.seed,
    )?;
    let witness_bytes = write_v24_witnesses(&witnesses)?;
    let graph = build_v24_witness_graph(&witnesses, manifest.seed)?;
    let graph_bytes = write_v24_witness_graph(&graph)?;
    let outputs = vec![
        sha256_identity(
            "witness-graph",
            &manifest.output_uris["witness-graph"],
            &manifest.generation,
            &graph_bytes,
        ),
        sha256_identity(
            "witnesses-arrow",
            &manifest.output_uris["witnesses-arrow"],
            &manifest.generation,
            &witness_bytes,
        ),
    ];
    for identity in &outputs {
        validate_v24_identity(identity, identity)?;
    }
    let result = V24TrainingResult {
        schema: V24_TRAINING_RESULT_SCHEMA.to_owned(),
        claim_eligible: false,
        phase: manifest.phase,
        generation: manifest.generation,
        seed: manifest.seed,
        source_row_count: manifest.source_row_count,
        witness_count: manifest.witness_count,
        inputs: manifest.inputs,
        outputs,
    };
    let result_bytes = canonical_json_bytes(&result)?;
    let write_result = (|| -> Result<()> {
        write_owned_file(&request.output_dir, WITNESSES_FILE, &witness_bytes)?;
        write_owned_file(&request.output_dir, WITNESS_GRAPH_FILE, &graph_bytes)?;
        write_owned_file(&request.output_dir, RESULT_FILE, &result_bytes)?;
        Ok(())
    })();
    if let Err(error) = write_result {
        cleanup_training_outputs(&request.output_dir);
        return Err(error);
    }
    Ok(result_bytes)
}

fn run_posting_construction(request: &V24LocalRunRequest) -> Result<Vec<u8>> {
    let manifest_bytes = fs::read(&request.manifest).map_err(|source| BorsukError::Io {
        path: request.manifest.clone(),
        source,
    })?;
    let manifest: V24PostingManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| invalid(&format!("V24 posting manifest differs: {error}")))?;
    let roles = manifest
        .inputs
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<Vec<_>>();
    if canonical_json_bytes(&manifest)? != manifest_bytes
        || manifest.schema != V24_LOCAL_MANIFEST_SCHEMA
        || manifest.claim_eligible
        || manifest.phase != "posting-construction"
        || manifest.generation.is_empty()
        || manifest.source_row_count == 0
        || manifest.witness_count < 2
        || manifest.witness_count > u64::from(u32::MAX)
        || roles != ["witness-graph", "witnesses-arrow", "page-rows-parquet"]
        || manifest.output_uris.len() != 1
        || manifest
            .output_uris
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != ["witness-postings"]
    {
        return Err(invalid("V24 posting manifest authority differs"));
    }

    let mut names = fs::read_dir(&request.input_dir)
        .map_err(|source| BorsukError::Io {
            path: request.input_dir.clone(),
            source,
        })?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|source| BorsukError::Io {
            path: request.input_dir.clone(),
            source,
        })?;
    names.sort();
    if names != [PAGE_ROWS_FILE, WITNESS_GRAPH_FILE, WITNESSES_FILE] {
        return Err(invalid("V24 local input inventory differs"));
    }

    let registered = |role: &str| {
        manifest
            .inputs
            .iter()
            .find(|identity| identity.role == role)
            .unwrap()
    };
    for (role, name) in [
        ("witness-graph", WITNESS_GRAPH_FILE),
        ("witnesses-arrow", WITNESSES_FILE),
        ("page-rows-parquet", PAGE_ROWS_FILE),
    ] {
        let authority = registered(role);
        let observed = sha256_file_identity(
            &request.input_dir.join(name),
            role,
            &authority.uri,
            &manifest.generation,
        )?;
        validate_v24_identity(&observed, authority)?;
    }

    let expected_witnesses = usize::try_from(manifest.witness_count).unwrap();
    let witness_path = request.input_dir.join(WITNESSES_FILE);
    let witness_bytes = fs::read(&witness_path).map_err(|source| BorsukError::Io {
        path: witness_path,
        source,
    })?;
    read_v24_witnesses(
        &witness_bytes,
        registered("witnesses-arrow"),
        expected_witnesses,
    )?;
    let graph_path = request.input_dir.join(WITNESS_GRAPH_FILE);
    let graph_bytes = fs::read(&graph_path).map_err(|source| BorsukError::Io {
        path: graph_path,
        source,
    })?;
    let graph: V24WitnessGraph = read_v24_witness_graph(
        &graph_bytes,
        registered("witness-graph"),
        expected_witnesses,
    )?;
    let pages = V24PageRows::open(
        &request.input_dir.join(PAGE_ROWS_FILE),
        manifest.source_row_count,
    )?;
    let scratch = request.output_dir.join(POSTING_SCRATCH_DIR);
    fs::create_dir(&scratch).map_err(|source| BorsukError::Io {
        path: scratch.clone(),
        source,
    })?;
    let plane_result =
        build_v24_witness_postings(&graph, manifest.source_row_count, pages, &scratch);
    let cleanup_result = fs::remove_dir(&scratch).map_err(|source| BorsukError::Io {
        path: scratch.clone(),
        source,
    });
    let plane = match (plane_result, cleanup_result) {
        (Ok(plane), Ok(())) => plane,
        (Err(error), _) | (Ok(_), Err(error)) => return Err(error),
    };
    if plane.unique_source_rows() != manifest.source_row_count {
        return Err(invalid("V24 posting unique source count differs"));
    }

    let posting_bytes = write_v24_witness_postings(&plane)?;
    let output = sha256_identity(
        "witness-postings",
        &manifest.output_uris["witness-postings"],
        &manifest.generation,
        &posting_bytes,
    );
    validate_v24_identity(&output, &output)?;
    let result = V24PostingResult {
        schema: "borsuk-v24-posting-result-v1".to_owned(),
        claim_eligible: false,
        phase: manifest.phase,
        generation: manifest.generation,
        source_row_count: manifest.source_row_count,
        witness_count: manifest.witness_count,
        unique_source_rows: plane.unique_source_rows(),
        physical_source_rows: plane.physical_source_rows(),
        inputs: manifest.inputs,
        outputs: vec![output],
    };
    let result_bytes = canonical_json_bytes(&result)?;
    let write_result = (|| -> Result<()> {
        write_owned_file(&request.output_dir, POSTINGS_FILE, &posting_bytes)?;
        write_owned_file(&request.output_dir, RESULT_FILE, &result_bytes)?;
        Ok(())
    })();
    if let Err(error) = write_result {
        for name in [POSTINGS_FILE, RESULT_FILE] {
            let _ = fs::remove_file(request.output_dir.join(name));
            let _ = fs::remove_file(request.output_dir.join(format!(".{name}.tmp")));
        }
        return Err(error);
    }
    Ok(result_bytes)
}

/// Execute one offline V24 phase after validating its complete local boundary.
///
#[doc(hidden)]
pub fn run_v24_local_request(request: V24LocalRunRequest) -> Result<Vec<u8>> {
    validate_request(&request)?;
    match request.phase {
        V24LocalPhase::TrainWitnesses => run_training(&request),
        V24LocalPhase::BuildPostings => run_posting_construction(&request),
        V24LocalPhase::EvaluateDevelopment
        | V24LocalPhase::BindHoldout
        | V24LocalPhase::EvaluateHoldout => {
            Err(invalid("V24 local phase execution is not yet wired"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use arrow_array::{
        ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, RecordBatch, StringArray,
        UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use serde_json::json;
    use sha2::{Digest, Sha256};

    use super::{
        V24LocalPhase, V24LocalRunRequest, V24PostingResult, V24TrainingResult,
        run_v24_local_request,
    };
    use crate::{
        v24_witness::V24ObjectIdentity,
        v24_witness_graph::{read_v24_witness_graph, read_v24_witnesses},
        v24_witness_postings::read_v24_witness_postings,
    };

    fn write_construction_rows(path: &std::path::Path, rows: u64) {
        let child = Arc::new(Field::new("element", DataType::Float32, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::clone(&child), 96),
                false,
            ),
        ]));
        let mut values = Vec::with_capacity(usize::try_from(rows).unwrap() * 96);
        for source in 0..rows {
            let mut vector = [0.0_f32; 96];
            vector[usize::try_from(source % 96).unwrap()] = 1.0;
            vector[usize::try_from((source * 17 + 3) % 96).unwrap()] += 0.125;
            values.extend_from_slice(&vector);
        }
        let vectors =
            FixedSizeListArray::try_new(child, 96, Arc::new(Float32Array::from(values)), None)
                .unwrap();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from_iter_values(0..rows)),
            Arc::new(vectors),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn input_identity(path: &std::path::Path) -> V24ObjectIdentity {
        file_identity(
            path,
            "construction-rows-parquet",
            "s3://borsuk-v24/construction-rows.parquet",
        )
    }

    fn file_identity(path: &std::path::Path, role: &str, uri: &str) -> V24ObjectIdentity {
        let bytes = fs::read(path).unwrap();
        V24ObjectIdentity {
            role: role.to_owned(),
            uri: uri.to_owned(),
            digest_algorithm: "sha256".to_owned(),
            digest: format!("{:x}", Sha256::digest(&bytes)),
            encoded_bytes: bytes.len() as u64,
            generation: "generation-v24-training-fixture".to_owned(),
        }
    }

    fn write_page_rows(path: &std::path::Path, rows: u64) {
        let child = Arc::new(Field::new("element", DataType::Float32, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("page_ordinal", DataType::UInt32, false),
            Field::new("replica", DataType::Boolean, false),
            Field::new("record_id", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::clone(&child), 96),
                false,
            ),
        ]));
        let source_order = (0..16)
            .flat_map(|page| (0..rows).filter(move |source| source % 16 == page))
            .collect::<Vec<_>>();
        let mut values = Vec::with_capacity(usize::try_from(rows).unwrap() * 96);
        for source in &source_order {
            let mut vector = [0.0_f32; 96];
            vector[usize::try_from(source % 96).unwrap()] = 1.0;
            vector[usize::try_from((source * 17 + 3) % 96).unwrap()] += 0.125;
            values.extend_from_slice(&vector);
        }
        let vectors =
            FixedSizeListArray::try_new(child, 96, Arc::new(Float32Array::from(values)), None)
                .unwrap();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from_iter_values(
                source_order
                    .iter()
                    .map(|source| u32::try_from(source % 16).unwrap()),
            )),
            Arc::new(BooleanArray::from(vec![
                false;
                usize::try_from(rows).unwrap()
            ])),
            Arc::new(StringArray::from_iter_values(
                source_order.iter().map(u64::to_string),
            )),
            Arc::new(vectors),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_posting_manifest(path: &std::path::Path, inputs: &[V24ObjectIdentity]) {
        let manifest = json!({
            "claim_eligible": false,
            "generation": "generation-v24-training-fixture",
            "inputs": inputs,
            "output_uris": {
                "witness-postings": "s3://borsuk-v24/witness-postings.arrow"
            },
            "phase": "posting-construction",
            "schema": "borsuk-v24-local-manifest-v1",
            "source_row_count": 257,
            "witness_count": 17
        });
        let mut bytes = serde_json::to_vec(&manifest).unwrap();
        bytes.push(b'\n');
        fs::write(path, bytes).unwrap();
    }

    fn write_manifest(path: &std::path::Path, input: &V24ObjectIdentity) {
        let manifest = json!({
            "claim_eligible": false,
            "generation": "generation-v24-training-fixture",
            "inputs": [input],
            "output_uris": {
                "witness-graph": "s3://borsuk-v24/witness-graph.arrow",
                "witnesses-arrow": "s3://borsuk-v24/witnesses.arrow"
            },
            "phase": "witness-training",
            "schema": "borsuk-v24-local-manifest-v1",
            "seed": 1311768467463790320_u64,
            "source_row_count": 257,
            "witness_count": 17
        });
        let mut bytes = serde_json::to_vec(&manifest).unwrap();
        bytes.push(b'\n');
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn v24_witness_local_training_authenticates_parquet_and_emits_graph_result() {
        let root = std::env::temp_dir().join(format!("borsuk-v24-local-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let input_dir = root.join("input");
        let output_dir = root.join("output");
        fs::create_dir_all(&input_dir).unwrap();
        fs::create_dir_all(&output_dir).unwrap();
        let construction = input_dir.join("construction-rows.parquet");
        write_construction_rows(&construction, 257);
        let input = input_identity(&construction);
        let manifest = root.join("manifest.json");
        write_manifest(&manifest, &input);

        let request = V24LocalRunRequest {
            manifest: manifest.clone(),
            input_dir: input_dir.clone(),
            output_dir: output_dir.clone(),
            phase: V24LocalPhase::TrainWitnesses,
        };
        let result_bytes = run_v24_local_request(request.clone()).unwrap();
        assert_eq!(result_bytes.last(), Some(&b'\n'));
        assert_eq!(
            fs::read(output_dir.join("result.json")).unwrap(),
            result_bytes
        );
        let result: V24TrainingResult = serde_json::from_slice(&result_bytes).unwrap();
        assert_eq!(result.source_row_count, 257);
        assert_eq!(result.witness_count, 17);
        assert_eq!(result.inputs, vec![input.clone()]);
        assert_eq!(result.outputs.len(), 2);

        let witness_bytes = fs::read(output_dir.join("witnesses.arrow")).unwrap();
        let graph_bytes = fs::read(output_dir.join("witness-graph.arrow")).unwrap();
        let witness_identity = result
            .outputs
            .iter()
            .find(|identity| identity.role == "witnesses-arrow")
            .unwrap();
        let graph_identity = result
            .outputs
            .iter()
            .find(|identity| identity.role == "witness-graph")
            .unwrap();
        assert_eq!(
            read_v24_witnesses(&witness_bytes, witness_identity, 17)
                .unwrap()
                .len(),
            17
        );
        assert_eq!(
            read_v24_witness_graph(&graph_bytes, graph_identity, 17)
                .unwrap()
                .node_count(),
            17
        );

        fs::remove_dir_all(&output_dir).unwrap();
        fs::create_dir(&output_dir).unwrap();
        let mut changed = fs::read(&construction).unwrap();
        let midpoint = changed.len() / 2;
        changed[midpoint] ^= 1;
        fs::write(&construction, changed).unwrap();
        assert!(run_v24_local_request(request).is_err());
        assert!(fs::read_dir(&output_dir).unwrap().next().is_none());

        fs::remove_file(construction).unwrap();
        fs::remove_file(manifest).unwrap();
        fs::remove_dir(input_dir).unwrap();
        fs::remove_dir(output_dir).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn v24_witness_local_postings_authenticate_parquet_and_emit_arrow_result() {
        let root =
            std::env::temp_dir().join(format!("borsuk-v24-local-postings-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let training_input = root.join("training-input");
        let training_output = root.join("training-output");
        let posting_input = root.join("posting-input");
        let posting_output = root.join("posting-output");
        for directory in [
            &training_input,
            &training_output,
            &posting_input,
            &posting_output,
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        let construction = training_input.join("construction-rows.parquet");
        write_construction_rows(&construction, 257);
        let training_manifest = root.join("training-manifest.json");
        write_manifest(&training_manifest, &input_identity(&construction));
        run_v24_local_request(V24LocalRunRequest {
            manifest: training_manifest,
            input_dir: training_input,
            output_dir: training_output.clone(),
            phase: V24LocalPhase::TrainWitnesses,
        })
        .unwrap();

        for name in ["witnesses.arrow", "witness-graph.arrow"] {
            fs::rename(training_output.join(name), posting_input.join(name)).unwrap();
        }
        fs::remove_file(training_output.join("result.json")).unwrap();
        let page_rows = posting_input.join("page-rows.parquet");
        write_page_rows(&page_rows, 257);
        let inputs = vec![
            file_identity(
                &posting_input.join("witness-graph.arrow"),
                "witness-graph",
                "s3://borsuk-v24/witness-graph.arrow",
            ),
            file_identity(
                &posting_input.join("witnesses.arrow"),
                "witnesses-arrow",
                "s3://borsuk-v24/witnesses.arrow",
            ),
            file_identity(
                &page_rows,
                "page-rows-parquet",
                "s3://borsuk-v24/page-rows.parquet",
            ),
        ];
        let posting_manifest = root.join("posting-manifest.json");
        write_posting_manifest(&posting_manifest, &inputs);
        let result_bytes = run_v24_local_request(V24LocalRunRequest {
            manifest: posting_manifest,
            input_dir: posting_input,
            output_dir: posting_output.clone(),
            phase: V24LocalPhase::BuildPostings,
        })
        .unwrap();
        let result: V24PostingResult = serde_json::from_slice(&result_bytes).unwrap();
        assert_eq!(result.inputs, inputs);
        assert_eq!(result.outputs.len(), 1);
        assert_eq!(result.unique_source_rows, 257);
        assert_eq!(result.physical_source_rows, 257);
        let posting_bytes = fs::read(posting_output.join("witness-postings.arrow")).unwrap();
        let plane = read_v24_witness_postings(&posting_bytes, &result.outputs[0], 17).unwrap();
        assert_eq!(plane.unique_source_rows(), 257);
        assert_eq!(plane.physical_source_rows(), 257);
        assert_eq!(
            fs::read(posting_output.join("result.json")).unwrap(),
            result_bytes
        );

        fs::remove_dir_all(root).unwrap();
    }
}
