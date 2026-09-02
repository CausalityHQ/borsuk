use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
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
    v24_witness::{
        V24ObjectIdentity, V24SourceRow, parse_v24_decimal_source_ordinal, validate_v24_identity,
    },
    v24_witness_eval::{
        V24_SELECTOR_WARMUP_SAMPLES, V24Cell, V24Disposition, V24Evaluation, V24EvaluationScope,
        V24QuerySample, V24QueryTruth, V24Result, canonical_v24_result_bytes, classify_v24_ladder,
        evaluate_v24_cell, evaluate_v24_exact_control, exact_v24_oracle_pages,
        fuse_v24_posting_plane,
    },
    v24_witness_graph::{
        V24DistanceBackend, V24Witness, V24WitnessGraph, V24WitnessSampler, V24WitnessSearch,
        build_v24_witness_graph_with_progress, normalize_v24_witness_vector,
        read_v24_witness_graph, read_v24_witnesses, v24_scientific_distance_backend,
        write_v24_witness_graph, write_v24_witnesses,
    },
    v24_witness_postings::{
        V24PostingPage, V24PostingPageRow, V24PostingPlane,
        build_v24_witness_postings_with_progress, read_v24_witness_postings,
        v24_posting_total_work_units, write_v24_witness_postings,
    },
    v24_witness_pseudoquery::{
        V24PseudoqueryEvidenceOutput, V24PseudoqueryPageRow, V24PseudoqueryPassAuthority,
        V24PseudoqueryPassReceipt, bind_v24_pseudoquery_pages_with_progress,
        bind_v24_pseudoquery_pass_receipt_authority, bind_v24_pseudoquery_result_authority,
        build_v24_pseudoquery_evidence_with_progress, canonical_v24_pseudoquery_pass_receipt_bytes,
        canonical_v24_pseudoquery_result_bytes, evaluate_v24_pseudoquery_result_with_progress,
        scan_v24_pseudoquery_truth_with_progress, select_v24_pseudoqueries_with_progress,
        validate_v24_pseudoquery_pass_receipt, write_v24_pseudoquery_evidence_parquet,
    },
};

const V24_LOCAL_MANIFEST_SCHEMA: &str = "borsuk-v24-local-manifest-v1";
const V24_TRAINING_RESULT_SCHEMA: &str = "borsuk-v24-training-result-v1";
const V24_HOLDOUT_BINDING_SCHEMA: &str = "borsuk-v24-holdout-binding-v1";
const CONSTRUCTION_ROWS_FILE: &str = "construction-rows.parquet";
const WITNESSES_FILE: &str = "witnesses.arrow";
const WITNESS_GRAPH_FILE: &str = "witness-graph.arrow";
const RESULT_FILE: &str = "result.json";
const TRAINING_RESULT_FILE: &str = "training-result.json";
const PAGE_ROWS_FILE: &str = "page-rows.parquet";
const POSTINGS_FILE: &str = "witness-postings.arrow";
const POSTING_RESULT_FILE: &str = "posting-result.json";
const PSEUDOQUERY_EVIDENCE_FILE: &str = "pseudoquery-evidence.parquet";
const PSEUDOQUERY_PASS_RECEIPT_FILE: &str = "pseudoquery-pass-receipt.json";
const POSTING_SCRATCH_DIR: &str = ".posting-scratch";
const QUERIES_FILE: &str = "queries.parquet";
const NEIGHBORS_FILE: &str = "neighbors.parquet";
const DEVELOPMENT_RESULT_FILE: &str = "development-result.json";
const HOLDOUT_BINDING_FILE: &str = "holdout-binding.json";
const PROGRESS_FILE: &str = "progress.json";
const V24_SERVING_BYTES: u64 = 1_644_167_168;
const V24_SOURCE_PROGRESS_ROWS: u64 = 1_048_576;
const V24_DEVELOPMENT_LATENCY_SAMPLES: u64 = 10_000;
const V24_DEVELOPMENT_PROGRESS_SAMPLES: u64 = 1_024;

/// One offline V24 scientific phase.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V24LocalPhase {
    /// Construct deterministic witnesses and their graph.
    TrainWitnesses,
    /// Stream page rows and construct witness-to-page postings.
    BuildPostings,
    /// Run the query-independent corpus-uniform catastrophe screen.
    EvaluatePseudoqueries,
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
    construction_rows_digest: String,
    parent_result_sha256: String,
    inputs: Vec<V24ObjectIdentity>,
    output_uris: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V24PseudoqueryManifest {
    schema: String,
    claim_eligible: bool,
    generation: String,
    phase: String,
    seed: u64,
    source_row_count: u64,
    witness_count: u64,
    pseudoquery_count: u64,
    page_count: u32,
    physical_source_rows: u64,
    inputs: Vec<V24ObjectIdentity>,
    output_uris: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V24DevelopmentManifest {
    schema: String,
    claim_eligible: bool,
    generation: String,
    phase: String,
    page_count: u32,
    query_count: u32,
    witness_count: u64,
    pseudoquery_count: u32,
    pseudoquery_split_seed: u64,
    serving_bytes: u64,
    inputs: Vec<V24ObjectIdentity>,
    output_uris: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V24HoldoutManifest {
    schema: String,
    claim_eligible: bool,
    generation: String,
    phase: String,
    page_count: u32,
    query_count: u32,
    witness_count: u64,
    serving_bytes: u64,
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
    pub(crate) distance_backend: V24DistanceBackend,
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
    pub(crate) construction_rows_digest: String,
    pub(crate) parent_result_sha256: String,
    pub(crate) distance_backend: V24DistanceBackend,
    pub(crate) unique_source_rows: u64,
    pub(crate) physical_source_rows: u64,
    pub(crate) inputs: Vec<V24ObjectIdentity>,
    pub(crate) outputs: Vec<V24ObjectIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V24HoldoutBinding {
    schema: String,
    claim_eligible: bool,
    generation: String,
    page_count: u32,
    query_count: u32,
    witness_count: u64,
    serving_bytes: u64,
    selected_cell: V24Cell,
    development_result_sha256: String,
    identities: Vec<V24ObjectIdentity>,
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

#[derive(Debug, Serialize)]
struct V24ProgressSnapshot<'a> {
    completed_units: u64,
    phase: &'a str,
    sequence: u64,
    total_units: u64,
}

struct V24ProgressWriter {
    output_dir: PathBuf,
    phase: &'static str,
    sequence: u64,
    completed_units: u64,
    total_units: u64,
    committed: bool,
}

impl V24ProgressWriter {
    fn start(output_dir: &Path, phase: &'static str, total_units: u64) -> Result<Self> {
        if total_units == 0 {
            return Err(invalid("V24 progress total differs"));
        }
        let writer = Self {
            output_dir: output_dir.to_owned(),
            phase,
            sequence: 0,
            completed_units: 0,
            total_units,
            committed: false,
        };
        writer.write()?;
        Ok(writer)
    }

    fn advance(&mut self, completed_units: u64) -> Result<()> {
        if completed_units <= self.completed_units || completed_units > self.total_units {
            return Err(invalid("V24 progress completed work differs"));
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid("V24 progress sequence overflows"))?;
        self.completed_units = completed_units;
        self.write()
    }

    fn write(&self) -> Result<()> {
        let bytes = canonical_json_bytes(&V24ProgressSnapshot {
            completed_units: self.completed_units,
            phase: self.phase,
            sequence: self.sequence,
            total_units: self.total_units,
        })?;
        let temporary = self.output_dir.join(format!(".{PROGRESS_FILE}.tmp"));
        let final_path = self.output_dir.join(PROGRESS_FILE);
        let result = (|| -> Result<()> {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .map_err(|source| BorsukError::Io {
                    path: temporary.clone(),
                    source,
                })?;
            file.write_all(&bytes).map_err(|source| BorsukError::Io {
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
            fs::File::open(&self.output_dir)
                .and_then(|directory| directory.sync_all())
                .map_err(|source| BorsukError::Io {
                    path: self.output_dir.clone(),
                    source,
                })
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for V24ProgressWriter {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(self.output_dir.join(PROGRESS_FILE));
            let _ = fs::remove_file(self.output_dir.join(format!(".{PROGRESS_FILE}.tmp")));
        }
    }
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

fn exact_directory_files(input_dir: &Path, expected_names: &[&str]) -> Result<()> {
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
    let mut expected = expected_names
        .iter()
        .map(std::ffi::OsString::from)
        .collect::<Vec<_>>();
    expected.sort();
    if names != expected {
        return Err(invalid("V24 local input inventory differs"));
    }
    Ok(())
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

struct V24ConstructionRows {
    batches: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    row: usize,
    next_ordinal: u64,
    expected_rows: u64,
}

impl V24ConstructionRows {
    fn open(path: &Path, expected_rows: u64) -> Result<Self> {
        let file = fs::File::open(path).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        if expected_rows == 0
            || builder.schema().as_ref() != &construction_schema()
            || builder.metadata().file_metadata().num_rows()
                != i64::try_from(expected_rows)
                    .map_err(|_| invalid("V24 construction row count exceeds i64"))?
        {
            return Err(invalid("V24 construction Parquet authority differs"));
        }
        Ok(Self {
            batches: builder.build()?,
            batch: None,
            row: 0,
            next_ordinal: 0,
            expected_rows,
        })
    }
}

impl Iterator for V24ConstructionRows {
    type Item = Result<V24SourceRow>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.num_rows()
            {
                if batch.num_columns() != 2
                    || batch
                        .columns()
                        .iter()
                        .any(|column| column.null_count() != 0)
                {
                    return Some(Err(invalid("V24 construction Parquet batch differs")));
                }
                let ordinals = match batch.columns()[0].as_any().downcast_ref::<UInt64Array>() {
                    Some(ordinals) => ordinals,
                    None => return Some(Err(invalid("V24 construction ordinal column differs"))),
                };
                let vectors = match batch.columns()[1]
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                {
                    Some(vectors) => vectors,
                    None => return Some(Err(invalid("V24 construction vector column differs"))),
                };
                let values = match vectors.values().as_any().downcast_ref::<Float32Array>() {
                    Some(values) => values,
                    None => return Some(Err(invalid("V24 construction vector child differs"))),
                };
                if vectors.offset() != 0
                    || vectors.value_length() != 96
                    || values.null_count() != 0
                    || values.len() != batch.num_rows() * 96
                {
                    return Some(Err(invalid("V24 construction vector layout differs")));
                }
                let source_ordinal = ordinals.value(self.row);
                if source_ordinal != self.next_ordinal {
                    return Some(Err(invalid("V24 construction source order differs")));
                }
                let vector = values.values()[self.row * 96..(self.row + 1) * 96]
                    .try_into()
                    .unwrap();
                self.row += 1;
                self.next_ordinal += 1;
                return Some(Ok(V24SourceRow {
                    source_ordinal,
                    vector,
                }));
            }
            self.batch = match self.batches.next() {
                Some(Ok(batch)) => Some(batch),
                Some(Err(error)) => return Some(Err(error.into())),
                None if self.next_ordinal == self.expected_rows => return None,
                None => return Some(Err(invalid("V24 construction row count differs"))),
            };
            self.row = 0;
        }
    }
}

fn page_rows_schema(construction_rows_digest: &str, generation: &str) -> Schema {
    Schema::new_with_metadata(
        vec![
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
        ],
        HashMap::from([
            (
                "construction_rows_sha256".to_owned(),
                construction_rows_digest.to_owned(),
            ),
            ("generation".to_owned(), generation.to_owned()),
        ]),
    )
}

fn query_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
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

fn truth_schema() -> Schema {
    let child = Arc::new(Field::new("element", DataType::UInt32, false));
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new(
            "primary_pages",
            DataType::FixedSizeList(Arc::clone(&child), 10),
            false,
        ),
        Field::new(
            "replica_pages",
            DataType::FixedSizeList(Arc::clone(&child), 10),
            false,
        ),
        Field::new("oracle_pages", DataType::FixedSizeList(child, 8), false),
    ])
}

fn read_development_queries(path: &Path, query_count: usize) -> Result<Vec<[f32; 96]>> {
    let file = fs::File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    if builder.schema().as_ref() != &query_schema()
        || usize::try_from(builder.metadata().file_metadata().num_rows()).ok() != Some(query_count)
    {
        return Err(invalid("V24 development query Parquet authority differs"));
    }
    let mut queries = Vec::with_capacity(query_count);
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 2
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V24 development query batch differs"));
        }
        let ordinals = batch.columns()[0]
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V24 development query ordinal differs"))?;
        let vectors = batch.columns()[1]
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V24 development query vector differs"))?;
        let values = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V24 development query vector child differs"))?;
        for row in 0..batch.num_rows() {
            if usize::try_from(ordinals.value(row)).ok() != Some(queries.len()) {
                return Err(invalid("V24 development query order differs"));
            }
            let vector = values.values()[row * 96..(row + 1) * 96]
                .try_into()
                .unwrap();
            queries.push(normalize_v24_witness_vector(&vector)?);
        }
    }
    if queries.len() != query_count {
        return Err(invalid("V24 development query count differs"));
    }
    Ok(queries)
}

fn read_development_truth(
    path: &Path,
    query_count: usize,
    page_count: usize,
) -> Result<Vec<V24QueryTruth>> {
    let file = fs::File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    if builder.schema().as_ref() != &truth_schema()
        || usize::try_from(builder.metadata().file_metadata().num_rows()).ok() != Some(query_count)
    {
        return Err(invalid("V24 development truth Parquet authority differs"));
    }
    let mut truth = Vec::with_capacity(query_count);
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 4
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V24 development truth batch differs"));
        }
        let ordinals = batch.columns()[0]
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V24 development truth ordinal differs"))?;
        let lists = [1_usize, 2, 3]
            .map(|column| {
                batch.columns()[column]
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| invalid("V24 development truth list differs"))
            })
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let values = lists
            .iter()
            .map(|list| {
                list.values()
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| invalid("V24 development truth child differs"))
            })
            .collect::<Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            if usize::try_from(ordinals.value(row)).ok() != Some(truth.len()) {
                return Err(invalid("V24 development truth order differs"));
            }
            let primary = &values[0].values()[row * 10..(row + 1) * 10];
            let replica = &values[1].values()[row * 10..(row + 1) * 10];
            let mut assignments = Vec::with_capacity(10);
            for (&primary, &replica) in primary.iter().zip(replica) {
                if usize::try_from(primary).map_or(true, |page| page >= page_count)
                    || replica != u32::MAX
                        && (usize::try_from(replica).map_or(true, |page| page >= page_count)
                            || replica == primary)
                {
                    return Err(invalid("V24 development neighbor page differs"));
                }
                let mut pages = vec![primary];
                if replica != u32::MAX {
                    pages.push(replica);
                }
                pages.sort_unstable();
                assignments.push(pages);
            }
            let padded_oracle = &values[2].values()[row * 8..(row + 1) * 8];
            let oracle_len = padded_oracle
                .iter()
                .position(|page| *page == u32::MAX)
                .unwrap_or(8);
            let oracle_pages = padded_oracle[..oracle_len].to_vec();
            if oracle_pages.is_empty()
                || padded_oracle[oracle_len..]
                    .iter()
                    .any(|page| *page != u32::MAX)
                || oracle_pages.windows(2).any(|pair| pair[0] >= pair[1])
                || oracle_pages
                    .iter()
                    .any(|page| usize::try_from(*page).map_or(true, |page| page >= page_count))
            {
                return Err(invalid("V24 development oracle pages differ"));
            }
            let exact_oracle = exact_v24_oracle_pages(&assignments, 8)?;
            truth.push(V24QueryTruth {
                query_ordinal: ordinals.value(row),
                ground_truth_page_assignments: assignments,
                oracle_pages: exact_oracle,
            });
        }
    }
    if truth.len() != query_count {
        return Err(invalid("V24 development truth count differs"));
    }
    Ok(truth)
}

struct V24PageRows {
    batches: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    row: usize,
    buffered: Option<(u32, bool, V24PostingPageRow)>,
    last_page: Option<u32>,
}

impl V24PageRows {
    fn open(
        path: &Path,
        source_row_count: u64,
        construction_rows_digest: &str,
        generation: &str,
    ) -> Result<Self> {
        let file = fs::File::open(path).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let physical_rows = builder.metadata().file_metadata().num_rows();
        if builder.schema().as_ref() != &page_rows_schema(construction_rows_digest, generation)
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

struct V24PseudoqueryPageRows {
    rows: V24PageRows,
}

impl Iterator for V24PseudoqueryPageRows {
    type Item = Result<V24PseudoqueryPageRow>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rows.next_row() {
            Ok(Some((page_ordinal, replica, row))) => Some(
                parse_v24_decimal_source_ordinal(&row.record_id).map(|source_ordinal| {
                    V24PseudoqueryPageRow {
                        page_ordinal,
                        replica,
                        source_ordinal,
                    }
                }),
            ),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

fn sample_training_rows(
    path: &Path,
    expected_rows: u64,
    witness_count: usize,
    seed: u64,
    mut progress: impl FnMut(u64) -> Result<()>,
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
            if next_ordinal.is_multiple_of(V24_SOURCE_PROGRESS_ROWS) {
                progress(next_ordinal)?;
            }
        }
    }
    if next_ordinal != expected_rows {
        return Err(invalid("V24 construction row count differs"));
    }
    if !next_ordinal.is_multiple_of(V24_SOURCE_PROGRESS_ROWS) {
        progress(next_ordinal)?;
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
    for name in [
        WITNESSES_FILE,
        WITNESS_GRAPH_FILE,
        RESULT_FILE,
        PROGRESS_FILE,
    ] {
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

    let total_units = manifest
        .source_row_count
        .checked_add(manifest.witness_count)
        .ok_or_else(|| invalid("V24 training progress total overflows"))?;
    let mut progress =
        V24ProgressWriter::start(&request.output_dir, "witness-training", total_units)?;
    let witnesses = sample_training_rows(
        &path,
        manifest.source_row_count,
        usize::try_from(manifest.witness_count).unwrap(),
        manifest.seed,
        |completed_rows| progress.advance(completed_rows),
    )?;
    let witness_bytes = write_v24_witnesses(&witnesses)?;
    let graph =
        build_v24_witness_graph_with_progress(&witnesses, manifest.seed, |completed_nodes| {
            progress.advance(manifest.source_row_count + completed_nodes)
        })?;
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
        distance_backend: graph.distance_backend(),
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
    progress.commit();
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
        || roles
            != [
                "training-result",
                "witness-graph",
                "witnesses-arrow",
                "page-rows-parquet",
            ]
        || manifest.construction_rows_digest.len() != 64
        || manifest.parent_result_sha256.len() != 64
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
    if names
        != [
            PAGE_ROWS_FILE,
            TRAINING_RESULT_FILE,
            WITNESS_GRAPH_FILE,
            WITNESSES_FILE,
        ]
    {
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
        ("training-result", TRAINING_RESULT_FILE),
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

    let training_result_path = request.input_dir.join(TRAINING_RESULT_FILE);
    let training_result_bytes =
        fs::read(&training_result_path).map_err(|source| BorsukError::Io {
            path: training_result_path,
            source,
        })?;
    let training_result: V24TrainingResult = serde_json::from_slice(&training_result_bytes)
        .map_err(|error| invalid(&format!("V24 training result differs: {error}")))?;
    let registered_training = registered("training-result");
    if canonical_json_bytes(&training_result)? != training_result_bytes
        || training_result.schema != V24_TRAINING_RESULT_SCHEMA
        || training_result.claim_eligible
        || training_result.phase != "witness-training"
        || training_result.generation != manifest.generation
        || training_result.source_row_count != manifest.source_row_count
        || training_result.witness_count != manifest.witness_count
        || training_result.inputs.len() != 1
        || training_result.inputs[0].role != "construction-rows-parquet"
        || training_result.inputs[0].digest_algorithm != "sha256"
        || training_result.inputs[0].digest != manifest.construction_rows_digest
        || training_result.outputs
            != [
                registered("witness-graph").clone(),
                registered("witnesses-arrow").clone(),
            ]
        || registered_training.digest != manifest.parent_result_sha256
        || training_result.distance_backend != v24_scientific_distance_backend()?
    {
        return Err(invalid("V24 posting parent authority differs"));
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
    if graph.distance_backend() != training_result.distance_backend {
        return Err(invalid("V24 posting graph backend differs"));
    }
    let pages = V24PageRows::open(
        &request.input_dir.join(PAGE_ROWS_FILE),
        manifest.source_row_count,
        &manifest.construction_rows_digest,
        &manifest.generation,
    )?;
    let total_units =
        v24_posting_total_work_units(manifest.source_row_count, manifest.witness_count)?;
    let mut progress =
        V24ProgressWriter::start(&request.output_dir, "posting-construction", total_units)?;
    let scratch = request.output_dir.join(POSTING_SCRATCH_DIR);
    fs::create_dir(&scratch).map_err(|source| BorsukError::Io {
        path: scratch.clone(),
        source,
    })?;
    let plane_result = build_v24_witness_postings_with_progress(
        &graph,
        manifest.source_row_count,
        pages,
        &scratch,
        |completed_units| progress.advance(completed_units),
    );
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
        construction_rows_digest: manifest.construction_rows_digest,
        parent_result_sha256: manifest.parent_result_sha256,
        distance_backend: training_result.distance_backend,
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
    progress.commit();
    Ok(result_bytes)
}

fn select_development_pages(
    search: &V24WitnessSearch<'_>,
    plane: &V24PostingPlane,
    query: &[f32; 96],
    cell: V24Cell,
    page_count: usize,
    scalar_control: bool,
    exact_control: bool,
) -> Result<Vec<u32>> {
    let selected = usize::try_from(cell.selected_witnesses).unwrap();
    let ef = if exact_control {
        plane.witness_count()
    } else {
        usize::try_from(cell.ef_search).unwrap()
    };
    let ranked = if scalar_control {
        search.search_scalar_control(query, selected, ef)?
    } else {
        search.search(query, selected, ef)?
    };
    let mut pages = fuse_v24_posting_plane(&ranked, plane, cell, page_count)?;
    pages.sort_unstable();
    Ok(pages)
}

fn development_samples(
    pages: &[Vec<u32>],
    truth: &[V24QueryTruth],
    cell: V24Cell,
) -> Result<Vec<V24QuerySample>> {
    pages
        .iter()
        .zip(truth)
        .map(|(pages, truth)| {
            let selected = pages
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>();
            let oracle_pages = exact_v24_oracle_pages(
                &truth.ground_truth_page_assignments,
                usize::try_from(cell.page_budget).unwrap(),
            )?;
            let oracle = oracle_pages
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>();
            let hits = truth
                .ground_truth_page_assignments
                .iter()
                .filter(|assignments| assignments.iter().any(|page| selected.contains(page)))
                .count();
            let oracle_hits = truth
                .ground_truth_page_assignments
                .iter()
                .filter(|assignments| assignments.iter().any(|page| oracle.contains(page)))
                .count();
            Ok(V24QuerySample {
                query_ordinal: truth.query_ordinal,
                page_ordinals: pages.clone(),
                hits: u32::try_from(hits).map_err(|_| invalid("V24 development hits overflow"))?,
                oracle_hits: u32::try_from(oracle_hits)
                    .map_err(|_| invalid("V24 development oracle hits overflow"))?,
                recall_ppm: u64::try_from(hits).unwrap() * 100_000,
            })
        })
        .collect()
}

fn evaluate_development_cell(
    search: &V24WitnessSearch<'_>,
    plane: &V24PostingPlane,
    queries: &[[f32; 96]],
    truth: &[V24QueryTruth],
    cell: V24Cell,
    page_count: usize,
    mut progress: impl FnMut(u64) -> Result<()>,
) -> Result<V24Evaluation> {
    let pages = queries
        .iter()
        .map(|query| select_development_pages(search, plane, query, cell, page_count, false, false))
        .collect::<Result<Vec<_>>>()?;
    let query_units = u64::try_from(queries.len()).unwrap();
    progress(query_units)?;
    let scalar_pages = queries
        .iter()
        .map(|query| select_development_pages(search, plane, query, cell, page_count, true, false))
        .collect::<Result<Vec<_>>>()?;
    progress(query_units * 2)?;
    let samples = development_samples(&pages, truth, cell)?;
    for iteration in 0..V24_SELECTOR_WARMUP_SAMPLES {
        let query = &queries[usize::try_from(iteration).unwrap() % queries.len()];
        let selected =
            select_development_pages(search, plane, query, cell, page_count, false, false)?;
        std::hint::black_box(selected);
    }
    progress(query_units * 2 + V24_SELECTOR_WARMUP_SAMPLES)?;
    let mut latency_ns =
        Vec::with_capacity(usize::try_from(V24_DEVELOPMENT_LATENCY_SAMPLES).unwrap());
    for iteration in 0..V24_DEVELOPMENT_LATENCY_SAMPLES {
        let iteration = usize::try_from(iteration).unwrap();
        let query = &queries[iteration % queries.len()];
        let start = Instant::now();
        let selected =
            select_development_pages(search, plane, query, cell, page_count, false, false)?;
        std::hint::black_box(selected);
        latency_ns.push(u64::try_from(start.elapsed().as_nanos().max(1)).unwrap_or(u64::MAX));
        let completed_samples = u64::try_from(iteration + 1).unwrap();
        if completed_samples.is_multiple_of(V24_DEVELOPMENT_PROGRESS_SAMPLES)
            || completed_samples == V24_DEVELOPMENT_LATENCY_SAMPLES
        {
            progress(query_units * 2 + V24_SELECTOR_WARMUP_SAMPLES + completed_samples)?;
        }
    }
    evaluate_v24_cell(
        cell,
        samples,
        truth,
        page_count,
        latency_ns,
        V24_SERVING_BYTES,
        scalar_pages,
    )
}

fn evaluate_exact_control(
    search: &V24WitnessSearch<'_>,
    plane: &V24PostingPlane,
    queries: &[[f32; 96]],
    truth: &[V24QueryTruth],
    cell: V24Cell,
    page_count: usize,
    mut progress: impl FnMut(u64) -> Result<()>,
) -> Result<crate::v24_witness_eval::V24ExactControl> {
    let pages = queries
        .iter()
        .map(|query| select_development_pages(search, plane, query, cell, page_count, false, true))
        .collect::<Result<Vec<_>>>()?;
    let query_units = u64::try_from(queries.len()).unwrap();
    progress(query_units)?;
    let scalar_pages = queries
        .iter()
        .map(|query| select_development_pages(search, plane, query, cell, page_count, true, true))
        .collect::<Result<Vec<_>>>()?;
    progress(query_units * 2)?;
    evaluate_v24_exact_control(
        cell,
        development_samples(&pages, truth, cell)?,
        truth,
        page_count,
        scalar_pages,
    )
}

fn development_cell_work_units(query_count: u64) -> Result<u64> {
    query_count
        .checked_mul(2)
        .and_then(|units| units.checked_add(V24_SELECTOR_WARMUP_SAMPLES))
        .and_then(|units| units.checked_add(V24_DEVELOPMENT_LATENCY_SAMPLES))
        .ok_or_else(|| invalid("V24 development progress total overflows"))
}

fn run_development_evaluation(request: &V24LocalRunRequest) -> Result<Vec<u8>> {
    let manifest_bytes = fs::read(&request.manifest).map_err(|source| BorsukError::Io {
        path: request.manifest.clone(),
        source,
    })?;
    let manifest: V24DevelopmentManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| invalid(&format!("V24 development manifest differs: {error}")))?;
    let roles = manifest
        .inputs
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<Vec<_>>();
    if canonical_json_bytes(&manifest)? != manifest_bytes
        || manifest.schema != V24_LOCAL_MANIFEST_SCHEMA
        || manifest.claim_eligible
        || manifest.phase != "development-evaluation"
        || manifest.generation.is_empty()
        || manifest.page_count < 8
        || manifest.query_count != 32
        || manifest.witness_count < 32
        || manifest.pseudoquery_count == 0
        || manifest.serving_bytes != V24_SERVING_BYTES
        || roles
            != [
                "pseudoquery-pass-receipt",
                "witness-graph",
                "witness-postings",
                "query-parquet",
                "neighbors-parquet",
            ]
        || manifest.output_uris.len() != 1
        || manifest
            .output_uris
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != ["development-result"]
        || !manifest.output_uris["development-result"].starts_with("s3://")
    {
        return Err(invalid("V24 development manifest authority differs"));
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
    if names
        != [
            NEIGHBORS_FILE,
            PSEUDOQUERY_PASS_RECEIPT_FILE,
            QUERIES_FILE,
            WITNESS_GRAPH_FILE,
            POSTINGS_FILE,
        ]
    {
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
        ("pseudoquery-pass-receipt", PSEUDOQUERY_PASS_RECEIPT_FILE),
        ("witness-graph", WITNESS_GRAPH_FILE),
        ("witness-postings", POSTINGS_FILE),
        ("query-parquet", QUERIES_FILE),
        ("neighbors-parquet", NEIGHBORS_FILE),
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
    let graph_path = request.input_dir.join(WITNESS_GRAPH_FILE);
    let graph_bytes = fs::read(&graph_path).map_err(|source| BorsukError::Io {
        path: graph_path,
        source,
    })?;
    let graph = read_v24_witness_graph(
        &graph_bytes,
        registered("witness-graph"),
        expected_witnesses,
    )?;
    let pass_receipt_path = request.input_dir.join(PSEUDOQUERY_PASS_RECEIPT_FILE);
    let pass_receipt_bytes = fs::read(&pass_receipt_path).map_err(|source| BorsukError::Io {
        path: pass_receipt_path,
        source,
    })?;
    let pass_receipt: V24PseudoqueryPassReceipt = serde_json::from_slice(&pass_receipt_bytes)
        .map_err(|error| invalid(&format!("V24 pseudoquery pass receipt differs: {error}")))?;
    if canonical_json_bytes(&pass_receipt)? != pass_receipt_bytes {
        return Err(invalid("V24 pseudoquery pass receipt bytes differ"));
    }
    validate_v24_pseudoquery_pass_receipt(
        &pass_receipt,
        V24PseudoqueryPassAuthority {
            graph: registered("witness-graph"),
            postings: registered("witness-postings"),
            generation: &manifest.generation,
            witness_count: manifest.witness_count,
            distance_backend: graph.distance_backend(),
            split_seed: manifest.pseudoquery_split_seed,
            pseudoquery_count: manifest.pseudoquery_count,
        },
    )?;
    let posting_path = request.input_dir.join(POSTINGS_FILE);
    let posting_bytes = fs::read(&posting_path).map_err(|source| BorsukError::Io {
        path: posting_path,
        source,
    })?;
    let plane = read_v24_witness_postings(
        &posting_bytes,
        registered("witness-postings"),
        expected_witnesses,
    )?;
    let query_count = usize::try_from(manifest.query_count).unwrap();
    let page_count = usize::try_from(manifest.page_count).unwrap();
    let queries = read_development_queries(&request.input_dir.join(QUERIES_FILE), query_count)?;
    let truth = read_development_truth(
        &request.input_dir.join(NEIGHBORS_FILE),
        query_count,
        page_count,
    )?;
    let search = V24WitnessSearch::new(&graph)?;
    let registered_cells = V24Cell::registered_ladder()
        .into_iter()
        .filter(|cell| usize::try_from(cell.page_budget).unwrap() <= page_count)
        .collect::<Vec<_>>();
    let query_units = u64::try_from(query_count).unwrap();
    let cell_units = development_cell_work_units(query_units)?;
    let total_units = u64::try_from(registered_cells.len())
        .unwrap()
        .checked_mul(cell_units)
        .and_then(|units| units.checked_add(query_units * 2))
        .ok_or_else(|| invalid("V24 development progress total overflows"))?;
    let mut progress =
        V24ProgressWriter::start(&request.output_dir, "development-evaluation", total_units)?;
    let mut evaluated_cells = Vec::new();
    for (cell_index, cell) in registered_cells.into_iter().enumerate() {
        let offset = u64::try_from(cell_index).unwrap() * cell_units;
        let evaluation = evaluate_development_cell(
            &search,
            &plane,
            &queries,
            &truth,
            cell,
            page_count,
            |completed_units| progress.advance(offset + completed_units),
        )?;
        let passed = evaluation.passed;
        evaluated_cells.push(evaluation);
        if passed {
            break;
        }
    }
    let serving = evaluated_cells
        .last()
        .cloned()
        .ok_or_else(|| invalid("V24 development ladder is empty"))?;
    let exact_control = if serving.passed {
        None
    } else {
        let offset = u64::try_from(evaluated_cells.len()).unwrap() * cell_units;
        Some(evaluate_exact_control(
            &search,
            &plane,
            &queries,
            &truth,
            serving.cell,
            page_count,
            |completed_units| progress.advance(offset + completed_units),
        )?)
    };
    let disposition = classify_v24_ladder(
        serving.passed,
        exact_control
            .as_ref()
            .is_some_and(|control| control.quality.passed),
        false,
    );
    let result = V24Result {
        schema: "borsuk-v24-witness-result-v1".to_owned(),
        claim_eligible: false,
        evaluation_scope: V24EvaluationScope::Development,
        distance_backend: v24_scientific_distance_backend()?,
        identities: manifest.inputs,
        evaluated_cells,
        serving,
        exact_control,
        disposition,
        page_integration_passed: false,
        page_body_reads: 0,
    };
    let result_bytes = canonical_v24_result_bytes(&result, &result.identities, &truth, page_count)?;
    write_owned_file(&request.output_dir, RESULT_FILE, &result_bytes)?;
    progress.commit();
    Ok(result_bytes)
}

fn run_holdout_binding(request: &V24LocalRunRequest) -> Result<Vec<u8>> {
    let manifest_bytes = fs::read(&request.manifest).map_err(|source| BorsukError::Io {
        path: request.manifest.clone(),
        source,
    })?;
    let manifest: V24HoldoutManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| invalid(&format!("V24 holdout binding manifest differs: {error}")))?;
    let roles = manifest
        .inputs
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<Vec<_>>();
    if canonical_json_bytes(&manifest)? != manifest_bytes
        || manifest.schema != V24_LOCAL_MANIFEST_SCHEMA
        || manifest.claim_eligible
        || manifest.phase != "holdout-binding"
        || manifest.generation.is_empty()
        || manifest.page_count < 8
        || manifest.query_count != 32
        || manifest.witness_count < 32
        || manifest.serving_bytes != V24_SERVING_BYTES
        || roles != ["development-result", "query-parquet", "neighbors-parquet"]
        || manifest
            .output_uris
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != ["holdout-truth"]
        || !manifest.output_uris["holdout-truth"].starts_with("s3://")
    {
        return Err(invalid("V24 holdout binding manifest authority differs"));
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
    if names != [DEVELOPMENT_RESULT_FILE, NEIGHBORS_FILE, QUERIES_FILE] {
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
        ("development-result", DEVELOPMENT_RESULT_FILE),
        ("query-parquet", QUERIES_FILE),
        ("neighbors-parquet", NEIGHBORS_FILE),
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
    let query_count = usize::try_from(manifest.query_count).unwrap();
    let page_count = usize::try_from(manifest.page_count).unwrap();
    let mut progress = V24ProgressWriter::start(
        &request.output_dir,
        "holdout-binding",
        u64::try_from(query_count).unwrap(),
    )?;
    let queries = read_development_queries(&request.input_dir.join(QUERIES_FILE), query_count)?;
    let truth = read_development_truth(
        &request.input_dir.join(NEIGHBORS_FILE),
        query_count,
        page_count,
    )?;
    if queries.len() != truth.len() {
        return Err(invalid("V24 holdout query/truth cardinality differs"));
    }
    let development_path = request.input_dir.join(DEVELOPMENT_RESULT_FILE);
    let development_bytes = fs::read(&development_path).map_err(|source| BorsukError::Io {
        path: development_path,
        source,
    })?;
    let development: V24Result = serde_json::from_slice(&development_bytes)
        .map_err(|error| invalid(&format!("V24 development result differs: {error}")))?;
    if canonical_json_bytes(&development)? != development_bytes
        || development.schema != "borsuk-v24-witness-result-v1"
        || development.claim_eligible
        || development.evaluation_scope != V24EvaluationScope::Development
        || development.distance_backend != v24_scientific_distance_backend()?
        || !development.serving.passed
        || development.evaluated_cells.last() != Some(&development.serving)
        || development.exact_control.is_some()
        || development.disposition != V24Disposition::PageIntegrationRejected
        || development.page_integration_passed
        || development.page_body_reads != 0
    {
        return Err(invalid("V24 sealed development result differs"));
    }
    let binding = V24HoldoutBinding {
        schema: V24_HOLDOUT_BINDING_SCHEMA.to_owned(),
        claim_eligible: false,
        generation: manifest.generation,
        page_count: manifest.page_count,
        query_count: manifest.query_count,
        witness_count: manifest.witness_count,
        serving_bytes: manifest.serving_bytes,
        selected_cell: development.serving.cell,
        development_result_sha256: registered("development-result").digest.clone(),
        identities: manifest.inputs,
    };
    let bytes = canonical_json_bytes(&binding)?;
    progress.advance(u64::try_from(query_count).unwrap())?;
    write_owned_file(&request.output_dir, HOLDOUT_BINDING_FILE, &bytes)?;
    progress.commit();
    Ok(bytes)
}

fn run_holdout_evaluation(request: &V24LocalRunRequest) -> Result<Vec<u8>> {
    let manifest_bytes = fs::read(&request.manifest).map_err(|source| BorsukError::Io {
        path: request.manifest.clone(),
        source,
    })?;
    let manifest: V24HoldoutManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| invalid(&format!("V24 holdout evaluation manifest differs: {error}")))?;
    let roles = manifest
        .inputs
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<Vec<_>>();
    if canonical_json_bytes(&manifest)? != manifest_bytes
        || manifest.schema != V24_LOCAL_MANIFEST_SCHEMA
        || manifest.claim_eligible
        || manifest.phase != "holdout-evaluation"
        || manifest.generation.is_empty()
        || manifest.page_count < 8
        || manifest.query_count != 32
        || manifest.witness_count < 32
        || manifest.serving_bytes != V24_SERVING_BYTES
        || roles
            != [
                "holdout-truth",
                "witness-graph",
                "witness-postings",
                "query-parquet",
                "neighbors-parquet",
            ]
        || manifest
            .output_uris
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != ["holdout-result"]
        || !manifest.output_uris["holdout-result"].starts_with("s3://")
    {
        return Err(invalid("V24 holdout evaluation manifest authority differs"));
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
    if names
        != [
            HOLDOUT_BINDING_FILE,
            NEIGHBORS_FILE,
            QUERIES_FILE,
            WITNESS_GRAPH_FILE,
            POSTINGS_FILE,
        ]
    {
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
        ("holdout-truth", HOLDOUT_BINDING_FILE),
        ("witness-graph", WITNESS_GRAPH_FILE),
        ("witness-postings", POSTINGS_FILE),
        ("query-parquet", QUERIES_FILE),
        ("neighbors-parquet", NEIGHBORS_FILE),
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
    let binding_path = request.input_dir.join(HOLDOUT_BINDING_FILE);
    let binding_bytes = fs::read(&binding_path).map_err(|source| BorsukError::Io {
        path: binding_path,
        source,
    })?;
    let binding: V24HoldoutBinding = serde_json::from_slice(&binding_bytes)
        .map_err(|error| invalid(&format!("V24 holdout binding differs: {error}")))?;
    let binding_role = |role: &str| {
        binding
            .identities
            .iter()
            .find(|identity| identity.role == role)
    };
    if canonical_json_bytes(&binding)? != binding_bytes
        || binding.schema != V24_HOLDOUT_BINDING_SCHEMA
        || binding.claim_eligible
        || binding.generation != manifest.generation
        || binding.page_count != manifest.page_count
        || binding.query_count != manifest.query_count
        || binding.witness_count != manifest.witness_count
        || binding.serving_bytes != manifest.serving_bytes
        || !binding.selected_cell.is_registered()
        || binding.development_result_sha256.len() != 64
        || binding.identities.len() != 3
        || binding_role("query-parquet") != Some(registered("query-parquet"))
        || binding_role("neighbors-parquet") != Some(registered("neighbors-parquet"))
    {
        return Err(invalid("V24 holdout binding authority differs"));
    }
    let expected_witnesses = usize::try_from(manifest.witness_count).unwrap();
    let graph_path = request.input_dir.join(WITNESS_GRAPH_FILE);
    let graph_bytes = fs::read(&graph_path).map_err(|source| BorsukError::Io {
        path: graph_path,
        source,
    })?;
    let graph = read_v24_witness_graph(
        &graph_bytes,
        registered("witness-graph"),
        expected_witnesses,
    )?;
    let posting_path = request.input_dir.join(POSTINGS_FILE);
    let posting_bytes = fs::read(&posting_path).map_err(|source| BorsukError::Io {
        path: posting_path,
        source,
    })?;
    let plane = read_v24_witness_postings(
        &posting_bytes,
        registered("witness-postings"),
        expected_witnesses,
    )?;
    let query_count = usize::try_from(manifest.query_count).unwrap();
    let page_count = usize::try_from(manifest.page_count).unwrap();
    let queries = read_development_queries(&request.input_dir.join(QUERIES_FILE), query_count)?;
    let truth = read_development_truth(
        &request.input_dir.join(NEIGHBORS_FILE),
        query_count,
        page_count,
    )?;
    let search = V24WitnessSearch::new(&graph)?;
    let query_units = u64::try_from(query_count).unwrap();
    let cell_units = development_cell_work_units(query_units)?;
    let total_units = cell_units
        .checked_add(query_units * 2)
        .ok_or_else(|| invalid("V24 holdout progress total overflows"))?;
    let mut progress =
        V24ProgressWriter::start(&request.output_dir, "holdout-evaluation", total_units)?;
    let serving = evaluate_development_cell(
        &search,
        &plane,
        &queries,
        &truth,
        binding.selected_cell,
        page_count,
        |completed_units| progress.advance(completed_units),
    )?;
    let exact_control = if serving.passed {
        None
    } else {
        Some(evaluate_exact_control(
            &search,
            &plane,
            &queries,
            &truth,
            binding.selected_cell,
            page_count,
            |completed_units| progress.advance(cell_units + completed_units),
        )?)
    };
    let disposition = classify_v24_ladder(
        serving.passed,
        exact_control
            .as_ref()
            .is_some_and(|control| control.quality.passed),
        false,
    );
    let result = V24Result {
        schema: "borsuk-v24-witness-result-v1".to_owned(),
        claim_eligible: false,
        evaluation_scope: V24EvaluationScope::Holdout,
        distance_backend: v24_scientific_distance_backend()?,
        identities: manifest.inputs,
        evaluated_cells: vec![serving.clone()],
        serving,
        exact_control,
        disposition,
        page_integration_passed: false,
        page_body_reads: 0,
    };
    let bytes = canonical_v24_result_bytes(&result, &result.identities, &truth, page_count)?;
    write_owned_file(&request.output_dir, RESULT_FILE, &bytes)?;
    progress.commit();
    Ok(bytes)
}

fn run_pseudoquery_evaluation(request: &V24LocalRunRequest) -> Result<Vec<u8>> {
    const INPUTS: [(&str, &str); 5] = [
        ("posting-result", POSTING_RESULT_FILE),
        ("witness-graph", WITNESS_GRAPH_FILE),
        ("witness-postings", POSTINGS_FILE),
        ("construction-rows-parquet", CONSTRUCTION_ROWS_FILE),
        ("page-rows-parquet", PAGE_ROWS_FILE),
    ];
    let manifest_bytes = fs::read(&request.manifest).map_err(|source| BorsukError::Io {
        path: request.manifest.clone(),
        source,
    })?;
    let manifest: V24PseudoqueryManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| invalid(&format!("V24 pseudoquery manifest differs: {error}")))?;
    let roles = manifest
        .inputs
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<Vec<_>>();
    if canonical_json_bytes(&manifest)? != manifest_bytes
        || manifest.schema != V24_LOCAL_MANIFEST_SCHEMA
        || manifest.claim_eligible
        || manifest.phase != "pseudoquery-evaluation"
        || manifest.generation.is_empty()
        || manifest.source_row_count <= 10
        || manifest.witness_count < 128
        || manifest.witness_count > u64::from(u32::MAX)
        || manifest.pseudoquery_count == 0
        || manifest.pseudoquery_count > u64::from(u32::MAX)
        || manifest.witness_count + manifest.pseudoquery_count > manifest.source_row_count
        || manifest.page_count < 64
        || manifest.physical_source_rows < manifest.source_row_count
        || manifest.physical_source_rows > manifest.source_row_count.saturating_mul(2)
        || roles != INPUTS.map(|(role, _)| role)
        || manifest.output_uris.len() != 3
        || manifest
            .output_uris
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != [
                "pseudoquery-evidence",
                "pseudoquery-pass-receipt",
                "pseudoquery-result",
            ]
        || manifest
            .output_uris
            .values()
            .any(|uri| !uri.starts_with("s3://") || uri.ends_with('/') || uri.contains("/../"))
        || manifest.output_uris["pseudoquery-evidence"]
            == manifest.output_uris["pseudoquery-result"]
        || manifest.output_uris["pseudoquery-evidence"]
            == manifest.output_uris["pseudoquery-pass-receipt"]
        || manifest.output_uris["pseudoquery-pass-receipt"]
            == manifest.output_uris["pseudoquery-result"]
    {
        return Err(invalid("V24 pseudoquery manifest authority differs"));
    }
    exact_directory_files(&request.input_dir, &INPUTS.map(|(_, name)| name))?;
    let registered = |role: &str| {
        manifest
            .inputs
            .iter()
            .find(|identity| identity.role == role)
            .unwrap()
    };
    for (role, name) in INPUTS {
        let authority = registered(role);
        let observed = sha256_file_identity(
            &request.input_dir.join(name),
            role,
            &authority.uri,
            &manifest.generation,
        )?;
        validate_v24_identity(&observed, authority)?;
    }

    let posting_result_path = request.input_dir.join(POSTING_RESULT_FILE);
    let posting_result_bytes =
        fs::read(&posting_result_path).map_err(|source| BorsukError::Io {
            path: posting_result_path,
            source,
        })?;
    let posting_result: V24PostingResult = serde_json::from_slice(&posting_result_bytes)
        .map_err(|error| invalid(&format!("V24 pseudoquery posting result differs: {error}")))?;
    let posting_input = |role: &str| {
        posting_result
            .inputs
            .iter()
            .find(|identity| identity.role == role)
    };
    if canonical_json_bytes(&posting_result)? != posting_result_bytes
        || posting_result.schema != "borsuk-v24-posting-result-v1"
        || posting_result.claim_eligible
        || posting_result.phase != "posting-construction"
        || posting_result.generation != manifest.generation
        || posting_result.source_row_count != manifest.source_row_count
        || posting_result.witness_count != manifest.witness_count
        || posting_result.unique_source_rows != manifest.source_row_count
        || posting_result.physical_source_rows != manifest.physical_source_rows
        || posting_result.distance_backend != v24_scientific_distance_backend()?
        || posting_result.inputs.len() != 4
        || posting_result.outputs != [registered("witness-postings").clone()]
        || posting_input("witness-graph") != Some(registered("witness-graph"))
        || posting_input("page-rows-parquet") != Some(registered("page-rows-parquet"))
        || posting_result.construction_rows_digest != registered("construction-rows-parquet").digest
    {
        return Err(invalid("V24 pseudoquery posting authority differs"));
    }

    let expected_witnesses = usize::try_from(manifest.witness_count).unwrap();
    let graph_path = request.input_dir.join(WITNESS_GRAPH_FILE);
    let graph_bytes = fs::read(&graph_path).map_err(|source| BorsukError::Io {
        path: graph_path,
        source,
    })?;
    let graph = read_v24_witness_graph(
        &graph_bytes,
        registered("witness-graph"),
        expected_witnesses,
    )?;
    if graph.distance_backend() != posting_result.distance_backend {
        return Err(invalid("V24 pseudoquery graph backend differs"));
    }
    let mut witnesses = vec![None; expected_witnesses];
    for (source_ordinal, witness_ordinal) in graph.source_index() {
        let slot = usize::try_from(witness_ordinal).unwrap();
        let vector = *graph
            .witness_vector(witness_ordinal)
            .ok_or_else(|| invalid("V24 pseudoquery witness vector differs"))?;
        if slot >= witnesses.len()
            || witnesses[slot]
                .replace(V24Witness {
                    witness_ordinal,
                    source_ordinal,
                    vector,
                })
                .is_some()
        {
            return Err(invalid("V24 pseudoquery witness inventory differs"));
        }
    }
    let witnesses = witnesses
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| invalid("V24 pseudoquery witness inventory differs"))?;

    let posting_path = request.input_dir.join(POSTINGS_FILE);
    let posting_bytes = fs::read(&posting_path).map_err(|source| BorsukError::Io {
        path: posting_path,
        source,
    })?;
    let plane = read_v24_witness_postings(
        &posting_bytes,
        registered("witness-postings"),
        expected_witnesses,
    )?;
    if plane.unique_source_rows() != manifest.source_row_count
        || plane.physical_source_rows() != manifest.physical_source_rows
    {
        return Err(invalid("V24 pseudoquery posting counts differ"));
    }

    let cells = u64::try_from(V24Cell::registered_ladder().len()).unwrap();
    let total_units = manifest
        .source_row_count
        .checked_mul(2)
        .and_then(|units| units.checked_add(manifest.physical_source_rows))
        .and_then(|units| units.checked_add(cells * 2))
        .and_then(|units| units.checked_add(manifest.pseudoquery_count))
        .ok_or_else(|| invalid("V24 pseudoquery progress total overflows"))?;
    let mut progress =
        V24ProgressWriter::start(&request.output_dir, "pseudoquery-evaluation", total_units)?;
    let construction_path = request.input_dir.join(CONSTRUCTION_ROWS_FILE);
    let split = select_v24_pseudoqueries_with_progress(
        V24ConstructionRows::open(&construction_path, manifest.source_row_count)?,
        &witnesses,
        usize::try_from(manifest.pseudoquery_count).unwrap(),
        manifest.source_row_count,
        manifest.seed,
        |completed| progress.advance(completed),
    )?;
    let split_units = manifest.source_row_count;
    let truth = scan_v24_pseudoquery_truth_with_progress(
        &split,
        V24ConstructionRows::open(&construction_path, manifest.source_row_count)?,
        manifest.source_row_count,
        posting_result.distance_backend,
        |completed| progress.advance(split_units + completed),
    )?;
    let truth_units = split_units + manifest.source_row_count;
    let pages = V24PageRows::open(
        &request.input_dir.join(PAGE_ROWS_FILE),
        manifest.source_row_count,
        &posting_result.construction_rows_digest,
        &manifest.generation,
    )?;
    let page_truth = bind_v24_pseudoquery_pages_with_progress(
        &truth,
        V24PseudoqueryPageRows { rows: pages },
        manifest.source_row_count,
        manifest.physical_source_rows,
        manifest.page_count,
        |completed| progress.advance(truth_units + completed),
    )?;
    let page_units = truth_units + manifest.physical_source_rows;
    let search = V24WitnessSearch::new(&graph)?;
    let evidence = build_v24_pseudoquery_evidence_with_progress(
        &split,
        &page_truth,
        usize::try_from(manifest.page_count).unwrap(),
        |query, cell| {
            select_development_pages(
                &search,
                &plane,
                query,
                cell,
                usize::try_from(manifest.page_count).unwrap(),
                false,
                false,
            )
        },
        |completed| progress.advance(page_units + completed),
    )?;
    let evidence_units = page_units + cells;
    let base_result = evaluate_v24_pseudoquery_result_with_progress(
        &split,
        &page_truth,
        &evidence,
        usize::try_from(manifest.page_count).unwrap(),
        posting_result.distance_backend,
        |completed| progress.advance(evidence_units + completed),
    )?;
    let evidence_path = request.output_dir.join(PSEUDOQUERY_EVIDENCE_FILE);
    let finish = (|| -> Result<Vec<u8>> {
        let evidence_identity = write_v24_pseudoquery_evidence_parquet(
            V24PseudoqueryEvidenceOutput {
                path: &evidence_path,
                uri: &manifest.output_uris["pseudoquery-evidence"],
                generation: &manifest.generation,
            },
            &base_result,
            &split,
            &page_truth,
            &evidence,
            usize::try_from(manifest.page_count).unwrap(),
        )?;
        let result = bind_v24_pseudoquery_result_authority(
            base_result,
            manifest.inputs.clone(),
            evidence_identity.clone(),
        )?;
        let result_bytes = canonical_v24_pseudoquery_result_bytes(
            &result,
            &manifest.inputs,
            &evidence_identity,
            &split,
            &page_truth,
            &evidence,
            usize::try_from(manifest.page_count).unwrap(),
        )?;
        write_owned_file(&request.output_dir, RESULT_FILE, &result_bytes)?;
        if result.passed {
            let result_identity = sha256_identity(
                "pseudoquery-result",
                &manifest.output_uris["pseudoquery-result"],
                &manifest.generation,
                &result_bytes,
            );
            let receipt =
                bind_v24_pseudoquery_pass_receipt_authority(&result, result_identity.clone())?;
            let receipt_bytes =
                canonical_v24_pseudoquery_pass_receipt_bytes(&receipt, &result, &result_identity)?;
            write_owned_file(
                &request.output_dir,
                PSEUDOQUERY_PASS_RECEIPT_FILE,
                &receipt_bytes,
            )?;
        }
        Ok(result_bytes)
    })();
    let result_bytes = match finish {
        Ok(bytes) => bytes,
        Err(error) => {
            let _ = fs::remove_file(&evidence_path);
            let _ = fs::remove_file(request.output_dir.join(RESULT_FILE));
            let _ = fs::remove_file(request.output_dir.join(format!(".{RESULT_FILE}.tmp")));
            let _ = fs::remove_file(request.output_dir.join(PSEUDOQUERY_PASS_RECEIPT_FILE));
            let _ = fs::remove_file(
                request
                    .output_dir
                    .join(format!(".{PSEUDOQUERY_PASS_RECEIPT_FILE}.tmp")),
            );
            return Err(error);
        }
    };
    progress.commit();
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
        V24LocalPhase::EvaluatePseudoqueries => run_pseudoquery_evaluation(&request),
        V24LocalPhase::EvaluateDevelopment => run_development_evaluation(&request),
        V24LocalPhase::BindHoldout => run_holdout_binding(&request),
        V24LocalPhase::EvaluateHoldout => run_holdout_evaluation(&request),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, sync::Arc};

    use arrow_array::{
        ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, RecordBatch, StringArray,
        UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use half::f16;
    use parquet::arrow::ArrowWriter;
    use serde_json::json;
    use sha2::{Digest, Sha256};

    use super::{
        V24LocalPhase, V24LocalRunRequest, V24PostingResult, V24TrainingResult,
        exact_v24_oracle_pages, run_v24_local_request,
    };

    #[test]
    fn v24_exact_truth_recomputes_eight_page_cover_with_lexicographic_ties() {
        let assignments = (0_u32..10)
            .map(|neighbor| vec![neighbor * 2, neighbor * 2 + 1])
            .collect::<Vec<_>>();
        assert_eq!(
            exact_v24_oracle_pages(&assignments, 8).unwrap(),
            vec![0, 2, 4, 6, 8, 10, 12, 14]
        );

        let shared = (0_u32..10)
            .map(|neighbor| vec![neighbor / 2, 10 + neighbor / 2])
            .collect::<Vec<_>>();
        assert_eq!(
            exact_v24_oracle_pages(&shared, 8).unwrap(),
            vec![0, 1, 2, 3, 4]
        );

        let root = std::env::temp_dir().join(format!(
            "borsuk-v24-input-oracle-is-not-authority-{}",
            std::process::id()
        ));
        let _ = fs::remove_file(&root);
        write_development_truth_with_oracle(&root, 15);
        let truth = super::read_development_truth(&root, 32, 16).unwrap();
        assert!(truth.iter().all(|query| query.oracle_pages == [0]));
        fs::remove_file(root).unwrap();
    }
    use crate::{
        metric::unit_l2_normalized,
        v24_witness::V24ObjectIdentity,
        v24_witness_eval::{V24Disposition, V24Result},
        v24_witness_graph::{
            read_v24_witness_graph, read_v24_witnesses, v24_scientific_distance_backend,
        },
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

    fn write_page_rows(path: &std::path::Path, rows: u64, construction_digest: &str) {
        let child = Arc::new(Field::new("element", DataType::Float32, false));
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("page_ordinal", DataType::UInt32, false),
                Field::new("replica", DataType::Boolean, false),
                Field::new("record_id", DataType::Utf8, false),
                Field::new(
                    "vector",
                    DataType::FixedSizeList(Arc::clone(&child), 96),
                    false,
                ),
            ],
            HashMap::from([
                (
                    "construction_rows_sha256".to_owned(),
                    construction_digest.to_owned(),
                ),
                (
                    "generation".to_owned(),
                    "generation-v24-training-fixture".to_owned(),
                ),
            ]),
        ));
        let source_order = (0..16)
            .flat_map(|page| (0..rows).filter(move |source| source % 16 == page))
            .collect::<Vec<_>>();
        let mut values = Vec::with_capacity(usize::try_from(rows).unwrap() * 96);
        for source in &source_order {
            let mut vector = [0.0_f32; 96];
            vector[usize::try_from(source % 96).unwrap()] = 1.0;
            vector[usize::try_from((source * 17 + 3) % 96).unwrap()] += 0.125;
            values.extend(
                unit_l2_normalized(&vector)
                    .into_iter()
                    .map(f16::from_f32)
                    .map(f32::from),
            );
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

    fn write_posting_manifest(
        path: &std::path::Path,
        inputs: &[V24ObjectIdentity],
        witness_count: u64,
        construction_rows_digest: &str,
        parent_result_sha256: &str,
    ) {
        let manifest = json!({
            "claim_eligible": false,
            "construction_rows_digest": construction_rows_digest,
            "generation": "generation-v24-training-fixture",
            "inputs": inputs,
            "output_uris": {
                "witness-postings": "s3://borsuk-v24/witness-postings.arrow"
            },
            "phase": "posting-construction",
            "parent_result_sha256": parent_result_sha256,
            "schema": "borsuk-v24-local-manifest-v1",
            "source_row_count": 257,
            "witness_count": witness_count
        });
        let mut bytes = serde_json::to_vec(&manifest).unwrap();
        bytes.push(b'\n');
        fs::write(path, bytes).unwrap();
    }

    fn write_evaluation_page_rows(path: &std::path::Path, rows: u64, construction_digest: &str) {
        let child = Arc::new(Field::new("element", DataType::Float32, false));
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("page_ordinal", DataType::UInt32, false),
                Field::new("replica", DataType::Boolean, false),
                Field::new("record_id", DataType::Utf8, false),
                Field::new(
                    "vector",
                    DataType::FixedSizeList(Arc::clone(&child), 96),
                    false,
                ),
            ],
            HashMap::from([
                (
                    "construction_rows_sha256".to_owned(),
                    construction_digest.to_owned(),
                ),
                (
                    "generation".to_owned(),
                    "generation-v24-training-fixture".to_owned(),
                ),
            ]),
        ));
        let order = (0..rows)
            .map(|source| (0_u32, false, source))
            .chain((1_u32..16).flat_map(|page| {
                (0..rows)
                    .filter(move |source| 1 + source % 15 == u64::from(page))
                    .map(move |source| (page, true, source))
            }))
            .collect::<Vec<_>>();
        let mut values = Vec::with_capacity(order.len() * 96);
        for (_, _, source) in &order {
            let mut vector = [0.0_f32; 96];
            vector[usize::try_from(source % 96).unwrap()] = 1.0;
            vector[usize::try_from((source * 17 + 3) % 96).unwrap()] += 0.125;
            values.extend(
                unit_l2_normalized(&vector)
                    .into_iter()
                    .map(f16::from_f32)
                    .map(f32::from),
            );
        }
        let vectors =
            FixedSizeListArray::try_new(child, 96, Arc::new(Float32Array::from(values)), None)
                .unwrap();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from_iter_values(
                order.iter().map(|(page, _, _)| *page),
            )),
            Arc::new(BooleanArray::from(
                order
                    .iter()
                    .map(|(_, replica, _)| *replica)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(
                order.iter().map(|(_, _, source)| source.to_string()),
            )),
            Arc::new(vectors),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_development_queries(path: &std::path::Path) {
        let child = Arc::new(Field::new("element", DataType::Float32, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("query_ordinal", DataType::UInt32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::clone(&child), 96),
                false,
            ),
        ]));
        let mut values = Vec::with_capacity(32 * 96);
        for query in 0_u32..32 {
            let mut vector = [0.0_f32; 96];
            vector[usize::try_from(query % 96).unwrap()] = 1.0;
            vector[usize::try_from((query * 17 + 3) % 96).unwrap()] += 0.125;
            values.extend_from_slice(&vector);
        }
        let vectors =
            FixedSizeListArray::try_new(child, 96, Arc::new(Float32Array::from(values)), None)
                .unwrap();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt32Array::from_iter_values(0_u32..32)) as ArrayRef,
                Arc::new(vectors),
            ],
        )
        .unwrap();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_development_truth_with_oracle(path: &std::path::Path, oracle_page: u32) {
        let child = Arc::new(Field::new("element", DataType::UInt32, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("query_ordinal", DataType::UInt32, false),
            Field::new(
                "primary_pages",
                DataType::FixedSizeList(Arc::clone(&child), 10),
                false,
            ),
            Field::new(
                "replica_pages",
                DataType::FixedSizeList(Arc::clone(&child), 10),
                false,
            ),
            Field::new(
                "oracle_pages",
                DataType::FixedSizeList(Arc::clone(&child), 8),
                false,
            ),
        ]));
        let fixed = |width: i32, values: Vec<u32>| {
            FixedSizeListArray::try_new(
                Arc::clone(&child),
                width,
                Arc::new(UInt32Array::from(values)),
                None,
            )
            .unwrap()
        };
        let primary = fixed(10, vec![0_u32; 32 * 10]);
        let replica = fixed(10, vec![u32::MAX; 32 * 10]);
        let oracle = fixed(
            8,
            (0..32)
                .flat_map(|_| std::iter::once(oracle_page).chain([u32::MAX; 7]))
                .collect(),
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt32Array::from_iter_values(0_u32..32)) as ArrayRef,
                Arc::new(primary),
                Arc::new(replica),
                Arc::new(oracle),
            ],
        )
        .unwrap();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_development_truth(path: &std::path::Path) {
        write_development_truth_with_oracle(path, 0);
    }

    fn write_development_manifest(path: &std::path::Path, inputs: &[V24ObjectIdentity]) {
        let manifest = json!({
            "claim_eligible": false,
            "generation": "generation-v24-training-fixture",
            "inputs": inputs,
            "output_uris": {
                "development-result": "s3://borsuk-v24/development-result.json"
            },
            "page_count": 16,
            "phase": "development-evaluation",
            "pseudoquery_count": 8,
            "pseudoquery_split_seed": 1311768467463790320_u64,
            "query_count": 32,
            "schema": "borsuk-v24-local-manifest-v1",
            "serving_bytes": 1_644_167_168_u64,
            "witness_count": 32
        });
        let mut bytes = serde_json::to_vec(&manifest).unwrap();
        bytes.push(b'\n');
        fs::write(path, bytes).unwrap();
    }

    fn write_pseudoquery_pass_receipt(
        path: &std::path::Path,
        ordered_inputs: &[V24ObjectIdentity],
        witness_count: u32,
    ) {
        assert_eq!(
            ordered_inputs
                .iter()
                .map(|identity| identity.role.as_str())
                .collect::<Vec<_>>(),
            [
                "posting-result",
                "witness-graph",
                "witness-postings",
                "construction-rows-parquet",
                "page-rows-parquet",
            ]
        );
        let object = |role: &str, uri: &str, digest_byte: &str| V24ObjectIdentity {
            role: role.to_owned(),
            uri: uri.to_owned(),
            digest_algorithm: "sha256".to_owned(),
            digest: digest_byte.repeat(32),
            encoded_bytes: 17,
            generation: "generation-v24-training-fixture".to_owned(),
        };
        let receipt = json!({
            "benchmark_query_reads": 0,
            "claim_eligible": false,
            "distance_backend": v24_scientific_distance_backend().unwrap(),
            "evidence": object(
                "pseudoquery-evidence",
                "s3://borsuk-v24/pseudoquery-evidence.parquet",
                "71",
            ),
            "generation": "generation-v24-training-fixture",
            "ordered_inputs": ordered_inputs,
            "page_body_reads": 0,
            "passed": true,
            "pseudoquery_count": 8,
            "result": object(
                "pseudoquery-result",
                "s3://borsuk-v24/pseudoquery-result.json",
                "72",
            ),
            "schema": "borsuk-v24-pseudoquery-pass-receipt-v1",
            "source_ordinals_sha256": "73".repeat(32),
            "split_seed": 1311768467463790320_u64,
            "witness_count": witness_count,
        });
        let mut bytes = serde_json::to_vec(&receipt).unwrap();
        bytes.push(b'\n');
        fs::write(path, bytes).unwrap();
    }

    fn write_pseudoquery_manifest(path: &std::path::Path, inputs: &[V24ObjectIdentity]) {
        let manifest = json!({
            "claim_eligible": false,
            "generation": "generation-v24-training-fixture",
            "inputs": inputs,
            "output_uris": {
                "pseudoquery-evidence": "s3://borsuk-v24/pseudoquery-evidence.parquet",
                "pseudoquery-pass-receipt": "s3://borsuk-v24/pseudoquery-pass-receipt.json",
                "pseudoquery-result": "s3://borsuk-v24/pseudoquery-result.json"
            },
            "page_count": 64,
            "phase": "pseudoquery-evaluation",
            "physical_source_rows": 514,
            "pseudoquery_count": 8,
            "schema": "borsuk-v24-local-manifest-v1",
            "seed": 1311768467463790320_u64,
            "source_row_count": 257,
            "witness_count": 128
        });
        let mut bytes = serde_json::to_vec(&manifest).unwrap();
        bytes.push(b'\n');
        fs::write(path, bytes).unwrap();
    }

    fn write_holdout_manifest(
        path: &std::path::Path,
        phase: &str,
        inputs: &[V24ObjectIdentity],
        output_role: &str,
    ) {
        let manifest = json!({
            "claim_eligible": false,
            "generation": "generation-v24-training-fixture",
            "inputs": inputs,
            "output_uris": {
                (output_role): format!("s3://borsuk-v24/{output_role}.json")
            },
            "page_count": 16,
            "phase": phase,
            "query_count": 32,
            "schema": "borsuk-v24-local-manifest-v1",
            "serving_bytes": 1_644_167_168_u64,
            "witness_count": 32
        });
        let mut bytes = serde_json::to_vec(&manifest).unwrap();
        bytes.push(b'\n');
        fs::write(path, bytes).unwrap();
    }

    fn write_manifest(path: &std::path::Path, input: &V24ObjectIdentity) {
        write_training_manifest(path, input, 17);
    }

    fn write_training_manifest(
        path: &std::path::Path,
        input: &V24ObjectIdentity,
        witness_count: u64,
    ) {
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
            "witness_count": witness_count
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
        assert_eq!(
            result.distance_backend,
            v24_scientific_distance_backend().unwrap()
        );
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
    fn v24_witness_local_progress_reports_training_source_and_graph_work() {
        let root = std::env::temp_dir().join(format!(
            "borsuk-v24-local-training-progress-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let input_dir = root.join("input");
        let output_dir = root.join("output");
        fs::create_dir_all(&input_dir).unwrap();
        fs::create_dir_all(&output_dir).unwrap();
        let construction = input_dir.join("construction-rows.parquet");
        write_construction_rows(&construction, 257);
        let manifest = root.join("manifest.json");
        write_manifest(&manifest, &input_identity(&construction));

        run_v24_local_request(V24LocalRunRequest {
            manifest,
            input_dir,
            output_dir: output_dir.clone(),
            phase: V24LocalPhase::TrainWitnesses,
        })
        .unwrap();
        let progress_bytes = fs::read(output_dir.join("progress.json")).unwrap();
        let progress: serde_json::Value = serde_json::from_slice(&progress_bytes).unwrap();
        assert_eq!(
            progress_bytes,
            super::canonical_json_bytes(&progress).unwrap(),
            "progress must be canonical newline JSON"
        );
        assert_eq!(
            progress,
            json!({
                "completed_units": 274,
                "phase": "witness-training",
                "sequence": 2,
                "total_units": 274
            })
        );

        fs::remove_dir_all(root).unwrap();
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
        let training_result_path = posting_input.join("training-result.json");
        fs::rename(training_output.join("result.json"), &training_result_path).unwrap();
        let training_result_bytes = fs::read(&training_result_path).unwrap();
        let training_result: V24TrainingResult =
            serde_json::from_slice(&training_result_bytes).unwrap();
        let construction_digest = training_result.inputs[0].digest.clone();
        let parent_result_sha256 = format!("{:x}", Sha256::digest(&training_result_bytes));
        let page_rows = posting_input.join("page-rows.parquet");
        write_page_rows(&page_rows, 257, &construction_digest);
        let inputs = vec![
            file_identity(
                &training_result_path,
                "training-result",
                "s3://borsuk-v24/training-result.json",
            ),
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
        write_posting_manifest(
            &posting_manifest,
            &inputs,
            17,
            &construction_digest,
            &parent_result_sha256,
        );
        let result_bytes = run_v24_local_request(V24LocalRunRequest {
            manifest: posting_manifest,
            input_dir: posting_input,
            output_dir: posting_output.clone(),
            phase: V24LocalPhase::BuildPostings,
        })
        .unwrap();
        let result: V24PostingResult = serde_json::from_slice(&result_bytes).unwrap();
        assert_eq!(result.inputs, inputs);
        assert_eq!(result.construction_rows_digest, construction_digest);
        assert_eq!(result.parent_result_sha256, parent_result_sha256);
        assert_eq!(result.distance_backend, training_result.distance_backend);
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
        let progress_bytes = fs::read(posting_output.join("progress.json")).unwrap();
        let progress: serde_json::Value = serde_json::from_slice(&progress_bytes).unwrap();
        assert_eq!(
            progress_bytes,
            super::canonical_json_bytes(&progress).unwrap()
        );
        assert_eq!(
            progress,
            json!({
                "completed_units": 530,
                "phase": "posting-construction",
                "sequence": 3,
                "total_units": 530
            })
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn v24_witness_local_pseudoquery_authenticates_corpus_only_inputs() {
        let root = std::env::temp_dir().join(format!(
            "borsuk-v24-local-pseudoquery-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let training_input = root.join("training-input");
        let training_output = root.join("training-output");
        let posting_input = root.join("posting-input");
        let posting_output = root.join("posting-output");
        let pseudoquery_input = root.join("pseudoquery-input");
        let pseudoquery_output = root.join("pseudoquery-output");
        for directory in [
            &training_input,
            &training_output,
            &posting_input,
            &posting_output,
            &pseudoquery_input,
            &pseudoquery_output,
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        let construction = training_input.join("construction-rows.parquet");
        write_construction_rows(&construction, 257);
        let training_manifest = root.join("training-manifest.json");
        write_training_manifest(&training_manifest, &input_identity(&construction), 128);
        run_v24_local_request(V24LocalRunRequest {
            manifest: training_manifest,
            input_dir: training_input.clone(),
            output_dir: training_output.clone(),
            phase: V24LocalPhase::TrainWitnesses,
        })
        .unwrap();
        for name in ["witnesses.arrow", "witness-graph.arrow"] {
            fs::rename(training_output.join(name), posting_input.join(name)).unwrap();
        }
        let training_result_path = posting_input.join("training-result.json");
        fs::rename(training_output.join("result.json"), &training_result_path).unwrap();
        let training_result_bytes = fs::read(&training_result_path).unwrap();
        let training_result: V24TrainingResult =
            serde_json::from_slice(&training_result_bytes).unwrap();
        let construction_digest = training_result.inputs[0].digest.clone();
        let parent_result_sha256 = format!("{:x}", Sha256::digest(&training_result_bytes));
        let page_rows = posting_input.join("page-rows.parquet");
        write_evaluation_page_rows(&page_rows, 257, &construction_digest);
        let posting_inputs = vec![
            file_identity(
                &training_result_path,
                "training-result",
                "s3://borsuk-v24/training-result.json",
            ),
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
        write_posting_manifest(
            &posting_manifest,
            &posting_inputs,
            128,
            &construction_digest,
            &parent_result_sha256,
        );
        let posting_result_bytes = run_v24_local_request(V24LocalRunRequest {
            manifest: posting_manifest,
            input_dir: posting_input.clone(),
            output_dir: posting_output.clone(),
            phase: V24LocalPhase::BuildPostings,
        })
        .unwrap();

        fs::write(
            pseudoquery_input.join("posting-result.json"),
            &posting_result_bytes,
        )
        .unwrap();
        fs::copy(
            posting_input.join("witness-graph.arrow"),
            pseudoquery_input.join("witness-graph.arrow"),
        )
        .unwrap();
        fs::copy(
            posting_output.join("witness-postings.arrow"),
            pseudoquery_input.join("witness-postings.arrow"),
        )
        .unwrap();
        fs::copy(
            &construction,
            pseudoquery_input.join("construction-rows.parquet"),
        )
        .unwrap();
        fs::copy(&page_rows, pseudoquery_input.join("page-rows.parquet")).unwrap();
        let inputs = vec![
            file_identity(
                &pseudoquery_input.join("posting-result.json"),
                "posting-result",
                "s3://borsuk-v24/posting-result.json",
            ),
            file_identity(
                &pseudoquery_input.join("witness-graph.arrow"),
                "witness-graph",
                "s3://borsuk-v24/witness-graph.arrow",
            ),
            file_identity(
                &pseudoquery_input.join("witness-postings.arrow"),
                "witness-postings",
                "s3://borsuk-v24/witness-postings.arrow",
            ),
            file_identity(
                &pseudoquery_input.join("construction-rows.parquet"),
                "construction-rows-parquet",
                "s3://borsuk-v24/construction-rows.parquet",
            ),
            file_identity(
                &pseudoquery_input.join("page-rows.parquet"),
                "page-rows-parquet",
                "s3://borsuk-v24/page-rows.parquet",
            ),
        ];
        let manifest = root.join("pseudoquery-manifest.json");
        write_pseudoquery_manifest(&manifest, &inputs);
        let rejected_output = root.join("pseudoquery-rejected-output");
        fs::create_dir(&rejected_output).unwrap();
        let rejected_manifest = root.join("pseudoquery-rejected-manifest.json");
        let mut rejected: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
        rejected["output_uris"]["pseudoquery-result"] = json!("s3://borsuk-v24/../escape");
        let mut rejected_bytes = serde_json::to_vec(&rejected).unwrap();
        rejected_bytes.push(b'\n');
        fs::write(&rejected_manifest, rejected_bytes).unwrap();
        let error = run_v24_local_request(V24LocalRunRequest {
            manifest: rejected_manifest,
            input_dir: pseudoquery_input.clone(),
            output_dir: rejected_output.clone(),
            phase: V24LocalPhase::EvaluatePseudoqueries,
        })
        .unwrap_err();
        assert!(error.to_string().contains("manifest authority differs"));
        assert_eq!(fs::read_dir(rejected_output).unwrap().count(), 0);
        let result_bytes = run_v24_local_request(V24LocalRunRequest {
            manifest,
            input_dir: pseudoquery_input,
            output_dir: pseudoquery_output.clone(),
            phase: V24LocalPhase::EvaluatePseudoqueries,
        })
        .unwrap();
        let result: crate::v24_witness_pseudoquery::V24PseudoqueryResult =
            serde_json::from_slice(&result_bytes).unwrap();
        assert_eq!(result.ordered_inputs, inputs);
        assert_eq!(result.selected_cell, None);
        assert!(result.passed);
        assert_eq!(result.benchmark_query_reads, 0);
        assert_eq!(result.page_body_reads, 0);
        assert_eq!(
            result.evidence.as_ref().unwrap().role,
            "pseudoquery-evidence"
        );
        assert!(
            pseudoquery_output
                .join("pseudoquery-evidence.parquet")
                .is_file()
        );
        assert_eq!(
            fs::read(pseudoquery_output.join("result.json")).unwrap(),
            result_bytes
        );
        let receipt_bytes =
            fs::read(pseudoquery_output.join("pseudoquery-pass-receipt.json")).unwrap();
        let receipt: serde_json::Value = serde_json::from_slice(&receipt_bytes).unwrap();
        assert_eq!(receipt["schema"], "borsuk-v24-pseudoquery-pass-receipt-v1");
        assert_eq!(receipt["passed"], true);
        assert!(receipt.get("cells").is_none());
        assert!(
            !String::from_utf8(receipt_bytes)
                .unwrap()
                .contains("aggregate_recall")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn v24_witness_local_development_evaluates_first_passing_cell_without_page_reads() {
        let root = std::env::temp_dir().join(format!(
            "borsuk-v24-local-development-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let training_input = root.join("training-input");
        let training_output = root.join("training-output");
        let posting_input = root.join("posting-input");
        let posting_output = root.join("posting-output");
        let development_input = root.join("development-input");
        let development_output = root.join("development-output");
        let holdout_bind_input = root.join("holdout-bind-input");
        let holdout_bind_output = root.join("holdout-bind-output");
        let holdout_eval_input = root.join("holdout-eval-input");
        let holdout_eval_output = root.join("holdout-eval-output");
        for directory in [
            &training_input,
            &training_output,
            &posting_input,
            &posting_output,
            &development_input,
            &development_output,
            &holdout_bind_input,
            &holdout_bind_output,
            &holdout_eval_input,
            &holdout_eval_output,
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        let construction = training_input.join("construction-rows.parquet");
        write_construction_rows(&construction, 257);
        let training_manifest = root.join("training-manifest.json");
        write_training_manifest(&training_manifest, &input_identity(&construction), 32);
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
        let training_result_path = posting_input.join("training-result.json");
        fs::rename(training_output.join("result.json"), &training_result_path).unwrap();
        let training_result_bytes = fs::read(&training_result_path).unwrap();
        let training_result: V24TrainingResult =
            serde_json::from_slice(&training_result_bytes).unwrap();
        let construction_digest = training_result.inputs[0].digest.clone();
        let parent_result_sha256 = format!("{:x}", Sha256::digest(&training_result_bytes));
        let page_rows = posting_input.join("page-rows.parquet");
        write_evaluation_page_rows(&page_rows, 257, &construction_digest);
        let posting_inputs = vec![
            file_identity(
                &training_result_path,
                "training-result",
                "s3://borsuk-v24/training-result.json",
            ),
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
        write_posting_manifest(
            &posting_manifest,
            &posting_inputs,
            32,
            &construction_digest,
            &parent_result_sha256,
        );
        let posting_result_bytes = run_v24_local_request(V24LocalRunRequest {
            manifest: posting_manifest,
            input_dir: posting_input.clone(),
            output_dir: posting_output.clone(),
            phase: V24LocalPhase::BuildPostings,
        })
        .unwrap();
        assert_eq!(
            fs::read(posting_output.join("result.json")).unwrap(),
            posting_result_bytes
        );

        fs::copy(
            posting_input.join("witness-graph.arrow"),
            development_input.join("witness-graph.arrow"),
        )
        .unwrap();
        fs::rename(
            posting_output.join("witness-postings.arrow"),
            development_input.join("witness-postings.arrow"),
        )
        .unwrap();
        let queries = development_input.join("queries.parquet");
        let truth = development_input.join("neighbors.parquet");
        write_development_queries(&queries);
        write_development_truth(&truth);
        let screen_inputs = vec![
            file_identity(
                &posting_output.join("result.json"),
                "posting-result",
                "s3://borsuk-v24/posting-result.json",
            ),
            file_identity(
                &development_input.join("witness-graph.arrow"),
                "witness-graph",
                "s3://borsuk-v24/witness-graph.arrow",
            ),
            file_identity(
                &development_input.join("witness-postings.arrow"),
                "witness-postings",
                "s3://borsuk-v24/witness-postings.arrow",
            ),
            file_identity(
                &construction,
                "construction-rows-parquet",
                "s3://borsuk-v24/construction-rows.parquet",
            ),
            file_identity(
                &page_rows,
                "page-rows-parquet",
                "s3://borsuk-v24/page-rows.parquet",
            ),
        ];
        let pass_receipt = development_input.join("pseudoquery-pass-receipt.json");
        write_pseudoquery_pass_receipt(&pass_receipt, &screen_inputs, 32);
        let inputs = vec![
            file_identity(
                &pass_receipt,
                "pseudoquery-pass-receipt",
                "s3://borsuk-v24/pseudoquery-pass-receipt.json",
            ),
            file_identity(
                &development_input.join("witness-graph.arrow"),
                "witness-graph",
                "s3://borsuk-v24/witness-graph.arrow",
            ),
            file_identity(
                &development_input.join("witness-postings.arrow"),
                "witness-postings",
                "s3://borsuk-v24/witness-postings.arrow",
            ),
            file_identity(&queries, "query-parquet", "s3://borsuk-v24/queries.parquet"),
            file_identity(
                &truth,
                "neighbors-parquet",
                "s3://borsuk-v24/neighbors.parquet",
            ),
        ];
        let development_manifest = root.join("development-manifest.json");
        write_development_manifest(&development_manifest, &inputs);
        fs::rename(
            &pass_receipt,
            development_input.join("pseudoquery-pass-receipt.missing"),
        )
        .unwrap();
        let missing_receipt_output = root.join("development-missing-receipt-output");
        fs::create_dir(&missing_receipt_output).unwrap();
        let error = run_v24_local_request(V24LocalRunRequest {
            manifest: development_manifest.clone(),
            input_dir: development_input.clone(),
            output_dir: missing_receipt_output,
            phase: V24LocalPhase::EvaluateDevelopment,
        })
        .unwrap_err();
        assert!(error.to_string().contains("input inventory"));
        fs::rename(
            development_input.join("pseudoquery-pass-receipt.missing"),
            &pass_receipt,
        )
        .unwrap();
        let valid_receipt_bytes = fs::read(&pass_receipt).unwrap();
        let mut drifted_receipt: serde_json::Value =
            serde_json::from_slice(&valid_receipt_bytes).unwrap();
        drifted_receipt["ordered_inputs"][1]["digest"] = json!("74".repeat(32));
        let mut drifted_bytes = serde_json::to_vec(&drifted_receipt).unwrap();
        drifted_bytes.push(b'\n');
        fs::write(&pass_receipt, drifted_bytes).unwrap();
        let mut drifted_inputs = inputs.clone();
        drifted_inputs[0] = file_identity(
            &pass_receipt,
            "pseudoquery-pass-receipt",
            "s3://borsuk-v24/pseudoquery-pass-receipt.json",
        );
        let drifted_manifest = root.join("development-drifted-manifest.json");
        write_development_manifest(&drifted_manifest, &drifted_inputs);
        let drifted_output = root.join("development-drifted-output");
        fs::create_dir(&drifted_output).unwrap();
        let error = run_v24_local_request(V24LocalRunRequest {
            manifest: drifted_manifest,
            input_dir: development_input.clone(),
            output_dir: drifted_output,
            phase: V24LocalPhase::EvaluateDevelopment,
        })
        .unwrap_err();
        assert!(error.to_string().contains("pass receipt"));
        fs::write(&pass_receipt, valid_receipt_bytes).unwrap();
        for (field, value) in [
            ("pseudoquery_count", json!(1)),
            ("pseudoquery_split_seed", json!(7)),
        ] {
            let manifest_bytes = fs::read(&development_manifest).unwrap();
            let mut manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
            manifest[field] = value;
            let mut bytes = serde_json::to_vec(&manifest).unwrap();
            bytes.push(b'\n');
            let drifted_manifest = root.join(format!("development-{field}-manifest.json"));
            fs::write(&drifted_manifest, bytes).unwrap();
            let drifted_output = root.join(format!("development-{field}-output"));
            fs::create_dir(&drifted_output).unwrap();
            let error = run_v24_local_request(V24LocalRunRequest {
                manifest: drifted_manifest,
                input_dir: development_input.clone(),
                output_dir: drifted_output,
                phase: V24LocalPhase::EvaluateDevelopment,
            })
            .unwrap_err();
            assert!(error.to_string().contains("pass receipt"));
        }
        let result_bytes = run_v24_local_request(V24LocalRunRequest {
            manifest: development_manifest,
            input_dir: development_input.clone(),
            output_dir: development_output.clone(),
            phase: V24LocalPhase::EvaluateDevelopment,
        })
        .unwrap();
        let result: V24Result = serde_json::from_slice(&result_bytes).unwrap();
        assert_eq!(result.identities, inputs);
        assert_eq!(
            result.distance_backend,
            v24_scientific_distance_backend().unwrap()
        );
        assert_eq!(result.serving.cell.page_budget, 8);
        assert!(result.serving.passed);
        assert!(result.exact_control.is_none());
        assert!(result.serving.scalar_simd_pages_equal);
        assert_eq!(result.disposition, V24Disposition::PageIntegrationRejected);
        assert_eq!(result.page_body_reads, 0);
        assert_eq!(
            fs::read(development_output.join("result.json")).unwrap(),
            result_bytes
        );
        let progress_bytes = fs::read(development_output.join("progress.json")).unwrap();
        let progress: serde_json::Value = serde_json::from_slice(&progress_bytes).unwrap();
        assert_eq!(
            progress_bytes,
            super::canonical_json_bytes(&progress).unwrap()
        );
        assert_eq!(
            progress,
            json!({
                "completed_units": 11_088,
                "phase": "development-evaluation",
                "sequence": 13,
                "total_units": 598_816
            })
        );

        fs::copy(
            development_output.join("result.json"),
            holdout_bind_input.join("development-result.json"),
        )
        .unwrap();
        fs::copy(
            development_input.join("queries.parquet"),
            holdout_bind_input.join("queries.parquet"),
        )
        .unwrap();
        fs::copy(
            development_input.join("neighbors.parquet"),
            holdout_bind_input.join("neighbors.parquet"),
        )
        .unwrap();
        let bind_inputs = vec![
            file_identity(
                &holdout_bind_input.join("development-result.json"),
                "development-result",
                "s3://borsuk-v24/development-result.json",
            ),
            file_identity(
                &holdout_bind_input.join("queries.parquet"),
                "query-parquet",
                "s3://borsuk-v24/holdout-queries.parquet",
            ),
            file_identity(
                &holdout_bind_input.join("neighbors.parquet"),
                "neighbors-parquet",
                "s3://borsuk-v24/holdout-neighbors.parquet",
            ),
        ];
        let bind_manifest = root.join("holdout-bind-manifest.json");
        write_holdout_manifest(
            &bind_manifest,
            "holdout-binding",
            &bind_inputs,
            "holdout-truth",
        );
        let binding_bytes = run_v24_local_request(V24LocalRunRequest {
            manifest: bind_manifest,
            input_dir: holdout_bind_input,
            output_dir: holdout_bind_output.clone(),
            phase: V24LocalPhase::BindHoldout,
        })
        .unwrap();
        assert_eq!(
            fs::read(holdout_bind_output.join("holdout-binding.json")).unwrap(),
            binding_bytes
        );
        let bind_progress_bytes = fs::read(holdout_bind_output.join("progress.json")).unwrap();
        let bind_progress: serde_json::Value =
            serde_json::from_slice(&bind_progress_bytes).unwrap();
        assert_eq!(
            bind_progress,
            json!({
                "completed_units": 32,
                "phase": "holdout-binding",
                "sequence": 1,
                "total_units": 32
            })
        );

        for name in ["witness-graph.arrow", "witness-postings.arrow"] {
            fs::copy(development_input.join(name), holdout_eval_input.join(name)).unwrap();
        }
        for name in ["queries.parquet", "neighbors.parquet"] {
            fs::copy(development_input.join(name), holdout_eval_input.join(name)).unwrap();
        }
        fs::copy(
            holdout_bind_output.join("holdout-binding.json"),
            holdout_eval_input.join("holdout-binding.json"),
        )
        .unwrap();
        let holdout_inputs = vec![
            file_identity(
                &holdout_eval_input.join("holdout-binding.json"),
                "holdout-truth",
                "s3://borsuk-v24/holdout-truth.json",
            ),
            file_identity(
                &holdout_eval_input.join("witness-graph.arrow"),
                "witness-graph",
                "s3://borsuk-v24/witness-graph.arrow",
            ),
            file_identity(
                &holdout_eval_input.join("witness-postings.arrow"),
                "witness-postings",
                "s3://borsuk-v24/witness-postings.arrow",
            ),
            file_identity(
                &holdout_eval_input.join("queries.parquet"),
                "query-parquet",
                "s3://borsuk-v24/holdout-queries.parquet",
            ),
            file_identity(
                &holdout_eval_input.join("neighbors.parquet"),
                "neighbors-parquet",
                "s3://borsuk-v24/holdout-neighbors.parquet",
            ),
        ];
        let holdout_manifest = root.join("holdout-eval-manifest.json");
        write_holdout_manifest(
            &holdout_manifest,
            "holdout-evaluation",
            &holdout_inputs,
            "holdout-result",
        );
        let holdout_bytes = run_v24_local_request(V24LocalRunRequest {
            manifest: holdout_manifest,
            input_dir: holdout_eval_input,
            output_dir: holdout_eval_output.clone(),
            phase: V24LocalPhase::EvaluateHoldout,
        })
        .unwrap();
        let holdout: V24Result = serde_json::from_slice(&holdout_bytes).unwrap();
        assert_eq!(holdout.evaluated_cells.len(), 1);
        assert_eq!(holdout.serving.cell, result.serving.cell);
        assert_eq!(holdout.page_body_reads, 0);
        assert_eq!(
            fs::read(holdout_eval_output.join("result.json")).unwrap(),
            holdout_bytes
        );
        let holdout_progress_bytes = fs::read(holdout_eval_output.join("progress.json")).unwrap();
        let holdout_progress: serde_json::Value =
            serde_json::from_slice(&holdout_progress_bytes).unwrap();
        assert_eq!(
            holdout_progress,
            json!({
                "completed_units": 11_088,
                "phase": "holdout-evaluation",
                "sequence": 13,
                "total_units": 11_152
            })
        );

        fs::remove_dir_all(root).unwrap();
    }
}
