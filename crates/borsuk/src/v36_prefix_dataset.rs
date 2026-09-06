use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader as ArrowFileReader,
    writer::{FileWriter as ArrowFileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, ObjectStoreScheme};
use parquet::{
    arrow::{ArrowSchemaConverter, ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
    schema::types::SchemaDescriptor,
};
use rayon::{ThreadPoolBuilder, prelude::*};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result, V36ArtifactIdentity, V36PrefixFreezeAuthority,
    V36PrefixFreezeExecutionAuthority, V36PrefixFreezeReceipt, V36PrefixPopulationAuthority,
    V36PrefixRegisteredSourceObject, V36PrefixRoleAuthority, V36PrefixSourceObject,
    bind_v36_prefix_population_authority, canonical_v36_prefix_freeze_authority_bytes,
    canonical_v36_prefix_freeze_execution_authority_bytes,
    canonical_v36_prefix_freeze_receipt_bytes, canonical_v36_prefix_population_authority_bytes,
    canonical_v36_prefix_source_registry_bytes, validate_v36_prefix_freeze_authority,
    validate_v36_prefix_freeze_execution_authority, validate_v36_prefix_population_authority,
};

const DIMENSIONS: usize = 768;
const GT_NEIGHBORS: usize = 100;
const DISTINCT_CANDIDATES: usize = 1_100_000;
const CORPUS_ROWS: usize = 1_000_000;
const PARQUET_ROW_GROUP_ROWS: usize = 8_192;
const IDENTITY_RUN_FORMAT: &str = "borsuk-v36-prefix-identity-run-v1";

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn temporary_output(path: &Path) -> Result<tempfile::NamedTempFile> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("V36 prefix output path has no parent"))?;
    tempfile::NamedTempFile::new_in(parent).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })
}

fn publish_output(temporary: tempfile::NamedTempFile, path: &Path) -> Result<()> {
    temporary
        .as_file()
        .sync_all()
        .map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
    temporary.persist(path).map_err(|error| BorsukError::Io {
        path: path.to_owned(),
        source: error.error,
    })?;
    Ok(())
}

fn parquet_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(PARQUET_ROW_GROUP_ROWS))
        .set_data_page_size_limit(1024 * 1024)
        .build()
}

fn validate_parquet_descriptor(actual: &SchemaDescriptor, expected: &Schema) -> Result<()> {
    let expected = ArrowSchemaConverter::new().convert(expected)?;
    if actual != &expected {
        return Err(invalid("V36 prefix Parquet physical schema differs"));
    }
    Ok(())
}

fn digest_bytes(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("V36 prefix digest differs"));
    }
    let mut decoded = [0_u8; 32];
    for (index, chunk) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let pair = std::str::from_utf8(chunk).map_err(|_| invalid("V36 prefix digest differs"))?;
        decoded[index] =
            u8::from_str_radix(pair, 16).map_err(|_| invalid("V36 prefix digest differs"))?;
    }
    if decoded.iter().all(|byte| *byte == 0) {
        return Err(invalid("V36 prefix digest differs"));
    }
    Ok(decoded)
}

fn validate_embedding(embedding: &[f32]) -> Result<()> {
    if embedding.len() != DIMENSIONS
        || embedding.iter().any(|value| !value.is_finite())
        || embedding.iter().all(|value| *value == 0.0)
    {
        return Err(invalid("V36 prefix embedding differs"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
/// One physical input row before duplicate resolution and role selection.
pub struct V36PrefixInputRow {
    /// Signed physical feature ID; negative IDs are invalid.
    pub feature_row_id: i64,
    /// Position of the complete object in the registered sampled order.
    pub selected_object_ordinal: u16,
    /// Zero-based physical row offset inside that object.
    pub row_offset: u64,
    /// Exact source-domain f32 vector.
    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Compact identity retained after a streamed row passes vector validation.
pub struct V36PrefixRowIdentity {
    /// Unsigned logical feature ID.
    pub feature_row_id: u64,
    /// Corpus ordinal, absent until query removal and source ordering finish.
    pub source_ordinal: Option<u64>,
    /// Position of the complete object in the registered sampled order.
    pub selected_object_ordinal: u16,
    /// Zero-based physical row offset inside that object.
    pub row_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete-object first-occurrence evidence stored in one Arrow IPC identity run.
pub struct V36PrefixIdentityRun {
    /// Physical rows validated in the complete source object.
    pub physical_rows: u64,
    /// Global first occurrences contributed by this object in row-offset order.
    pub rows: Vec<V36PrefixRowIdentity>,
    /// Position of the complete object in registered sample order.
    pub selected_object_ordinal: u16,
    /// Exact complete source object authenticated before the run was committed.
    pub source: V36PrefixSourceObject,
}

fn v36_prefix_identity_run_schema(run: &V36PrefixIdentityRun, row_count: usize) -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("feature_row_id", DataType::UInt64, false),
            Field::new("row_offset", DataType::UInt64, false),
        ],
        HashMap::from([
            ("format".to_owned(), IDENTITY_RUN_FORMAT.to_owned()),
            (
                "selected_object_ordinal".to_owned(),
                run.selected_object_ordinal.to_string(),
            ),
            ("physical_rows".to_owned(), run.physical_rows.to_string()),
            ("rows".to_owned(), row_count.to_string()),
            ("source_blake3".to_owned(), run.source.blake3.clone()),
            (
                "source_encoded_bytes".to_owned(),
                run.source.encoded_bytes.to_string(),
            ),
            ("source_path".to_owned(), run.source.path.clone()),
            (
                "source_sample_sha256".to_owned(),
                run.source.sample_sha256.clone(),
            ),
            ("source_sha256".to_owned(), run.source.sha256.clone()),
            ("source_uri".to_owned(), run.source.uri.clone()),
        ]),
    )
}

fn v36_prefix_object_sample_sha256(path: &str, encoded_bytes: u64) -> String {
    let mut sample = Sha256::new();
    sample.update(b"borsuk-v36-screen-object-v1");
    sample.update(path.as_bytes());
    sample.update(encoded_bytes.to_le_bytes());
    format!("{:x}", sample.finalize())
}

fn validate_v36_prefix_identity_run(run: &V36PrefixIdentityRun) -> Result<()> {
    let mut feature_ids = HashSet::with_capacity(run.rows.len());
    if run.selected_object_ordinal >= 16
        || run.physical_rows == 0
        || run.rows.len() as u64 > run.physical_rows
        || run.source.path.is_empty()
        || run.source.uri.is_empty()
        || run.source.encoded_bytes == 0
        || digest_bytes(&run.source.sha256).is_err()
        || digest_bytes(&run.source.blake3).is_err()
        || digest_bytes(&run.source.sample_sha256).is_err()
        || run.source.sample_sha256
            != v36_prefix_object_sample_sha256(&run.source.path, run.source.encoded_bytes)
        || run.rows.iter().any(|row| {
            row.selected_object_ordinal != run.selected_object_ordinal
                || row.source_ordinal.is_some()
                || row.row_offset >= run.physical_rows
                || !feature_ids.insert(row.feature_row_id)
        })
        || run
            .rows
            .windows(2)
            .any(|pair| pair[0].row_offset >= pair[1].row_offset)
    {
        return Err(invalid("V36 prefix identity-run rows differ"));
    }
    Ok(())
}

/// Encode the first-occurrence identities contributed by one complete source object.
pub fn encode_v36_prefix_identity_run(run: &V36PrefixIdentityRun) -> Result<Vec<u8>> {
    validate_v36_prefix_identity_run(run)?;
    let schema = Arc::new(v36_prefix_identity_run_schema(run, run.rows.len()));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(
                run.rows
                    .iter()
                    .map(|row| row.feature_row_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                run.rows
                    .iter()
                    .map(|row| row.row_offset)
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    let mut writer = ArrowFileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    Ok(bytes)
}

/// Authenticate and decode one complete immutable source-object identity run.
pub fn decode_v36_prefix_identity_run(
    bytes: &[u8],
    registered: &V36ArtifactIdentity,
    source: &V36PrefixSourceObject,
    selected_object_ordinal: u16,
) -> Result<V36PrefixIdentityRun> {
    let expected_role = format!("population-identity-run-{selected_object_ordinal:04}");
    let sha256 = format!("{:x}", Sha256::digest(bytes));
    let blake3 = blake3::hash(bytes).to_hex().to_string();
    let content_addressed = url::Url::parse(&registered.uri)
        .ok()
        .filter(|uri| uri.scheme() == "s3" && uri.host_str().is_some())
        .and_then(|uri| uri.path().rsplit('/').next().map(str::to_owned))
        .is_some_and(|name| name.starts_with(&format!("{sha256}-")));
    if selected_object_ordinal >= 16
        || registered.role != expected_role
        || registered.encoded_bytes != bytes.len() as u64
        || registered.sha256 != sha256
        || registered.blake3 != blake3
        || !content_addressed
    {
        return Err(invalid("V36 prefix identity-run artifact differs"));
    }

    let mut reader = ArrowFileReader::try_new(std::io::Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let row_count = schema
        .metadata()
        .get("rows")
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| invalid("V36 prefix identity-run schema differs"))?;
    let physical_rows = schema
        .metadata()
        .get("physical_rows")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| invalid("V36 prefix identity-run schema differs"))?;
    let expected_run = V36PrefixIdentityRun {
        physical_rows,
        rows: Vec::with_capacity(row_count),
        selected_object_ordinal,
        source: source.clone(),
    };
    if schema.as_ref() != &v36_prefix_identity_run_schema(&expected_run, row_count)
        || reader.num_batches() != 1
    {
        return Err(invalid("V36 prefix identity-run schema differs"));
    }
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| invalid("V36 prefix identity-run batch is missing"))?;
    if reader.next().is_some() || batch.num_rows() != row_count {
        return Err(invalid("V36 prefix identity-run batches differ"));
    }
    let feature_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 prefix identity-run feature IDs differ"))?;
    let row_offsets = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 prefix identity-run row offsets differ"))?;
    let rows = feature_ids
        .values()
        .iter()
        .zip(row_offsets.values())
        .map(|(&feature_row_id, &row_offset)| V36PrefixRowIdentity {
            feature_row_id,
            source_ordinal: None,
            selected_object_ordinal,
            row_offset,
        })
        .collect::<Vec<_>>();
    let run = V36PrefixIdentityRun {
        physical_rows,
        rows,
        selected_object_ordinal,
        source: source.clone(),
    };
    validate_v36_prefix_identity_run(&run)?;
    Ok(run)
}

/// Reconstruct the exact completed population prefix from authenticated identity runs.
pub fn restore_v36_prefix_population(
    runs: &[V36PrefixIdentityRun],
    distinct_candidates: usize,
) -> Result<V36PrefixObjectPrefixScan> {
    let restored = restore_v36_prefix_population_state(runs, distinct_candidates)?;
    let (cutoff_object_ordinal, cutoff_row_offset) = restored
        .cutoff
        .ok_or(BorsukError::V36PrefixSourceInsufficient)?;
    Ok(V36PrefixObjectPrefixScan {
        consumed_objects: restored.consumed_objects,
        cutoff_object_ordinal,
        cutoff_row_offset,
        distinct_rows_observed: restored.distinct_rows_observed,
        duplicate_rows: restored.duplicate_rows,
        physical_rows: restored.physical_rows,
        unique_rows: restored.unique_rows,
    })
}

/// Reconstruct resumable population state at a complete source-object boundary.
pub fn restore_v36_prefix_population_state(
    runs: &[V36PrefixIdentityRun],
    distinct_candidates: usize,
) -> Result<V36PrefixRestoredPopulation> {
    if runs.is_empty() || runs.len() > 16 || distinct_candidates == 0 {
        return Err(invalid("V36 prefix identity-run replay limits differ"));
    }
    let mut source_paths = BTreeSet::new();
    let mut feature_ids = HashSet::with_capacity(distinct_candidates);
    let mut consumed_objects = Vec::with_capacity(runs.len());
    let mut unique_rows = Vec::with_capacity(distinct_candidates);
    let mut physical_rows = 0_u64;
    let mut cutoff = None;
    for (ordinal, run) in runs.iter().enumerate() {
        if cutoff.is_some() {
            return Err(invalid("V36 prefix identity run follows cutoff object"));
        }
        validate_v36_prefix_identity_run(run)?;
        if usize::from(run.selected_object_ordinal) != ordinal
            || !source_paths.insert(run.source.path.as_str())
        {
            return Err(invalid("V36 prefix identity-run sequence differs"));
        }
        physical_rows = physical_rows
            .checked_add(run.physical_rows)
            .ok_or_else(|| invalid("V36 prefix identity-run physical rows overflow"))?;
        for row in &run.rows {
            if !feature_ids.insert(row.feature_row_id) {
                return Err(invalid("V36 prefix identity-run global ID repeats"));
            }
            if unique_rows.len() < distinct_candidates {
                unique_rows.push(row.clone());
                if unique_rows.len() == distinct_candidates {
                    cutoff = Some((run.selected_object_ordinal, row.row_offset));
                }
            }
        }
        consumed_objects.push(run.source.clone());
    }
    let distinct_rows_observed = u64::try_from(feature_ids.len()).unwrap_or(u64::MAX);
    let duplicate_rows = physical_rows
        .checked_sub(distinct_rows_observed)
        .ok_or_else(|| invalid("V36 prefix identity-run duplicate rows underflow"))?;
    Ok(V36PrefixRestoredPopulation {
        consumed_objects,
        cutoff,
        distinct_rows_observed,
        duplicate_rows,
        next_object_ordinal: runs
            .len()
            .try_into()
            .map_err(|_| invalid("V36 prefix identity-run ordinal overflows"))?,
        physical_rows,
        unique_rows,
    })
}

#[derive(Debug, Clone, PartialEq)]
/// One valid unique row after materialization ordering.
pub struct V36PrefixMaterializedRow {
    /// Unsigned logical feature ID.
    pub feature_row_id: u64,
    /// Corpus ordinal, absent for a query row.
    pub source_ordinal: Option<u64>,
    /// Exact source-domain f32 vector.
    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
/// One query row with its role-local ordinal.
pub struct V36PrefixQueryRow {
    /// Zero-based ordinal within a query role.
    pub query_ordinal: u32,
    /// Unsigned logical feature ID.
    pub feature_row_id: u64,
    /// Exact source-domain f32 vector.
    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
/// Disjoint role selection plus the query-excluded corpus.
pub struct V36PrefixRoleSplit {
    /// Development queries.
    pub development: Vec<V36PrefixRowIdentity>,
    /// Validation queries.
    pub validation: Vec<V36PrefixRowIdentity>,
    /// Once-opened sealed holdout queries.
    pub sealed_holdout: Vec<V36PrefixRowIdentity>,
    /// Timing-only queries with no exact-GT obligation.
    pub performance: Vec<V36PrefixRowIdentity>,
    /// Remaining rows ordered by the registered source score.
    pub corpus: Vec<V36PrefixRowIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One registered source object with its query-independent sample score.
pub struct V36PrefixRankedSourceObject {
    /// Complete encoded length.
    pub encoded_bytes: u64,
    /// Registered path.
    pub path: String,
    /// Query-independent sample digest.
    pub sample_sha256: String,
    /// Complete-object SHA-256.
    pub sha256: String,
    /// Immutable object URI.
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Local immutable inputs for one bounded V36 prefix-freeze attempt.
pub struct V36PrefixFreezeRequest {
    /// Pre-freeze scientific authority path.
    pub authority: PathBuf,
    /// Executable whose exact bytes are bound by the attempt authority.
    pub executable: PathBuf,
    /// Attempt lifecycle and provenance authority path.
    pub execution_authority: PathBuf,
    /// Empty output directory owned by this attempt.
    pub output: PathBuf,
    /// Empty encrypted scratch directory owned by this attempt.
    pub scratch: PathBuf,
    /// Exact source-code archive evidence path.
    pub source_archive: PathBuf,
    /// Complete registered source-object list path.
    pub source_registry: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Authenticated local state produced before any source-object network access.
pub struct V36PrefixFreezePreflight {
    /// Validated pre-freeze scientific authority.
    pub authority: V36PrefixFreezeAuthority,
    /// Validated lifecycle and provenance authority.
    pub execution_authority: V36PrefixFreezeExecutionAuthority,
    /// Complete source registry in its canonical encoded order.
    pub registry: Vec<V36PrefixRegisteredSourceObject>,
    /// Query-independently ranked complete objects.
    pub ranked_objects: Vec<V36PrefixRankedSourceObject>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete bounded source-prefix scan evidence before role selection.
pub struct V36PrefixObjectPrefixScan {
    /// Complete authenticated objects through the cutoff object.
    pub consumed_objects: Vec<crate::V36PrefixSourceObject>,
    /// Selected-object ordinal containing the target distinct row.
    pub cutoff_object_ordinal: u16,
    /// Physical row offset of the target distinct row.
    pub cutoff_row_offset: u64,
    /// Distinct IDs observed across every complete consumed object.
    pub distinct_rows_observed: u64,
    /// Physical rows whose ID repeated an earlier occurrence.
    pub duplicate_rows: u64,
    /// Physical rows scanned across every complete consumed object.
    pub physical_rows: u64,
    /// First-occurrence identities for the exact requested distinct prefix.
    pub unique_rows: Vec<V36PrefixRowIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Authenticated population progress at any complete source-object boundary.
pub struct V36PrefixRestoredPopulation {
    /// Complete source objects incorporated in ranked order.
    pub consumed_objects: Vec<crate::V36PrefixSourceObject>,
    /// Registered cutoff, absent while more complete objects are required.
    pub cutoff: Option<(u16, u64)>,
    /// Distinct IDs observed through the complete object prefix.
    pub distinct_rows_observed: u64,
    /// Duplicate physical rows observed through the complete object prefix.
    pub duplicate_rows: u64,
    /// First registered object ordinal not yet incorporated.
    pub next_object_ordinal: u16,
    /// Physical rows observed through the complete object prefix.
    pub physical_rows: u64,
    /// First occurrences retained up to the requested distinct-row target.
    pub unique_rows: Vec<V36PrefixRowIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One durable population boundary emitted after a complete authenticated object.
pub struct V36PrefixPopulationCommit {
    /// Cutoff position once the requested distinct prefix has been reached.
    pub cutoff: Option<(u16, u64)>,
    /// Distinct IDs observed through this complete object.
    pub distinct_rows: u64,
    /// Duplicate physical rows observed through this complete object.
    pub duplicate_rows: u64,
    /// Physical rows observed through this complete object.
    pub physical_rows: u64,
    /// Complete-object first-occurrence evidence.
    pub run: V36PrefixIdentityRun,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Strict role-separated Parquet outputs from one prefix population.
pub struct V36PrefixRoleParquetPaths {
    /// Development queries.
    pub development: PathBuf,
    /// Performance queries.
    pub performance: PathBuf,
    /// Sealed-holdout queries.
    pub sealed_holdout: PathBuf,
    /// Canonically ordered corpus.
    pub source: PathBuf,
    /// Validation queries.
    pub validation: PathBuf,
}

#[derive(Debug, Clone, PartialEq)]
/// One exact GT@100 row.
pub struct V36PrefixGtNeighbor {
    /// Query ordinal.
    pub query_ordinal: u32,
    /// Zero-based neighbor rank.
    pub rank: u16,
    /// Unsigned logical feature ID.
    pub feature_row_id: u64,
    /// Exact binary64 squared-L2 distance.
    pub squared_distance: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// One query role authorized to receive exact GT@100.
pub enum V36PrefixQualityRole {
    /// Development queries.
    Development,
    /// Validation queries.
    Validation,
    /// Once-opened sealed holdout queries.
    SealedHoldout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One quality-query Parquet input and exact-GT output role.
pub struct V36PrefixGtParquetJob {
    /// Exact number of queries in this role.
    pub expected_queries: u32,
    /// Destination exact-GT Parquet path.
    pub output: PathBuf,
    /// Source query Parquet path.
    pub query: PathBuf,
    /// Closed quality role.
    pub role: V36PrefixQualityRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Bounded work evidence from one corpus-outer exact-GT pass.
pub struct V36PrefixGtRunStats {
    /// Quality queries updated for every source row.
    pub quality_queries: u32,
    /// Complete source scans performed.
    pub source_scans: u32,
    /// Source rows processed in canonical order.
    pub source_rows: u64,
}

/// Rank a complete authenticated registry by the frozen sample rule.
pub fn rank_v36_prefix_source_objects(
    authority: &V36PrefixFreezeAuthority,
    registry: &[V36PrefixRegisteredSourceObject],
) -> Result<Vec<V36PrefixRankedSourceObject>> {
    validate_v36_prefix_freeze_authority(authority, registry)?;
    let mut paths = BTreeSet::new();
    let mut uris = BTreeSet::new();
    let mut ranked = registry
        .iter()
        .map(|object| {
            if object.encoded_bytes == 0
                || object.path.is_empty()
                || !paths.insert(object.path.as_str())
                || !uris.insert(object.uri.as_str())
            {
                return Err(invalid("V36 prefix source registry differs"));
            }
            digest_bytes(&object.sha256)?;
            let mut hasher = Sha256::new();
            hasher.update(b"borsuk-v36-screen-object-v1");
            hasher.update(object.path.as_bytes());
            hasher.update(object.encoded_bytes.to_le_bytes());
            Ok(V36PrefixRankedSourceObject {
                encoded_bytes: object.encoded_bytes,
                path: object.path.clone(),
                sample_sha256: format!("{:x}", hasher.finalize()),
                sha256: object.sha256.clone(),
                uri: object.uri.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_by(|left, right| {
        (&left.sample_sha256, &left.path).cmp(&(&right.sample_sha256, &right.path))
    });
    Ok(ranked)
}

/// Validate one streamed physical row and retain only its compact identity.
pub fn validate_v36_prefix_input_row(row: &V36PrefixInputRow) -> Result<V36PrefixRowIdentity> {
    if row.feature_row_id < 0 {
        return Err(invalid("V36 prefix feature ID differs"));
    }
    validate_embedding(&row.embedding)?;
    Ok(V36PrefixRowIdentity {
        feature_row_id: u64::try_from(row.feature_row_id)
            .map_err(|_| invalid("V36 prefix feature ID differs"))?,
        source_ordinal: None,
        selected_object_ordinal: row.selected_object_ordinal,
        row_offset: row.row_offset,
    })
}

/// Keep the first sampled-object occurrence per ID without retaining vectors.
pub fn deduplicate_v36_prefix_row_identities(
    mut rows: Vec<V36PrefixRowIdentity>,
) -> Result<Vec<V36PrefixRowIdentity>> {
    rows.sort_by_key(|row| (row.selected_object_ordinal, row.row_offset));
    let mut physical = BTreeSet::new();
    let mut feature_ids = BTreeSet::new();
    let mut unique = Vec::new();
    for row in rows {
        if row.source_ordinal.is_some()
            || !physical.insert((row.selected_object_ordinal, row.row_offset))
        {
            return Err(invalid("V36 prefix physical row differs"));
        }
        if feature_ids.insert(row.feature_row_id) {
            unique.push(row);
        }
    }
    Ok(unique)
}

/// Validate membership against independently authenticated complete-object evidence.
pub fn validate_v36_prefix_cutoff_membership(
    rows: &[V36PrefixRowIdentity],
    consumed_objects: usize,
    distinct_candidates: usize,
) -> Result<()> {
    if consumed_objects == 0
        || rows.len() < distinct_candidates
        || distinct_candidates == 0
        || rows
            .iter()
            .any(|row| usize::from(row.selected_object_ordinal) >= consumed_objects)
        || usize::from(rows[distinct_candidates - 1].selected_object_ordinal)
            != consumed_objects - 1
    {
        return Err(invalid("V36 prefix consumed-object membership differs"));
    }
    Ok(())
}

fn score(seed: &[u8; 32], source_identity: &[u8; 32], feature_row_id: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(seed);
    hasher.update(source_identity);
    hasher.update(feature_row_id.to_le_bytes());
    hasher.finalize().into()
}

fn source_score(source_identity: &[u8; 32], feature_row_id: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(source_identity);
    hasher.update(feature_row_id.to_le_bytes());
    hasher.finalize().into()
}

/// Compute one frozen prefix-population query score for audit and mutation tests.
pub fn v36_prefix_query_score_sha256(
    seed_label: &str,
    source_identity_sha256: &str,
    feature_row_id: u64,
) -> Result<String> {
    let seed: [u8; 32] = Sha256::digest(seed_label.as_bytes()).into();
    let source_identity = digest_bytes(source_identity_sha256)?;
    Ok(score(&seed, &source_identity, feature_row_id)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// Compute one frozen prefix-population corpus score for audit and mutation tests.
pub fn v36_prefix_source_score_sha256(
    source_identity_sha256: &str,
    feature_row_id: u64,
) -> Result<String> {
    let source_identity = digest_bytes(source_identity_sha256)?;
    Ok(source_score(&source_identity, feature_row_id)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// Validate the exact population-specific query-role authority.
pub fn validate_v36_prefix_role_authority(roles: &[V36PrefixRoleAuthority]) -> Result<()> {
    let expected_names = ["development", "validation", "sealed-holdout", "performance"];
    let expected_labels = [
        "borsuk-v36-prefix-screen-development-query-v1",
        "borsuk-v36-prefix-screen-validation-query-v1",
        "borsuk-v36-prefix-screen-sealed-holdout-query-v1",
        "borsuk-v36-prefix-screen-performance-query-v1",
    ];
    let expected_rows = [1_000_u64, 1_000, 1_000, 10_000];
    if roles.len() != expected_names.len() {
        return Err(invalid("V36 prefix query role authority differs"));
    }
    let mut seeds = BTreeSet::new();
    for (((role, expected_name), expected_label), expected_rows) in roles
        .iter()
        .zip(expected_names)
        .zip(expected_labels)
        .zip(expected_rows)
    {
        if role.role != expected_name
            || role.seed_label != expected_label
            || role.rows != expected_rows
            || role.seed_sha256 != format!("{:x}", Sha256::digest(role.seed_label.as_bytes()))
            || !seeds.insert(role.seed_sha256.as_str())
        {
            return Err(invalid("V36 prefix query role authority differs"));
        }
    }
    Ok(())
}

/// Select disjoint query roles in priority order, then order the corpus.
pub fn select_v36_prefix_roles(
    rows: Vec<V36PrefixRowIdentity>,
    population: &V36PrefixPopulationAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<V36PrefixRoleSplit> {
    validate_v36_prefix_population_authority(population, source_registry)?;
    validate_v36_prefix_role_authority(&population.roles)?;
    let source_identity = digest_bytes(&population.ordered_source_manifest_sha256)?;
    let mut unique = deduplicate_v36_prefix_row_identities(rows)?;
    let consumed_objects = population.consumed_objects.len();
    validate_v36_prefix_cutoff_membership(&unique, consumed_objects, DISTINCT_CANDIDATES)?;
    unique.truncate(DISTINCT_CANDIDATES);
    let mut remaining = unique;
    let mut selected = Vec::with_capacity(4);
    for role in &population.roles {
        let seed = digest_bytes(&role.seed_sha256)?;
        let mut ranked = remaining
            .into_iter()
            .map(|row| (score(&seed, &source_identity, row.feature_row_id), row))
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            (&left.0, left.1.feature_row_id).cmp(&(&right.0, right.1.feature_row_id))
        });
        let count =
            usize::try_from(role.rows).map_err(|_| invalid("V36 prefix query count overflows"))?;
        if ranked.len() < count {
            return Err(invalid("V36 prefix query population is insufficient"));
        }
        let rest = ranked.split_off(count);
        selected.push(ranked.into_iter().map(|(_, row)| row).collect::<Vec<_>>());
        remaining = rest.into_iter().map(|(_, row)| row).collect();
    }
    let mut corpus = remaining
        .into_iter()
        .map(|row| (source_score(&source_identity, row.feature_row_id), row))
        .collect::<Vec<_>>();
    corpus.sort_by(|left, right| {
        (&left.0, left.1.feature_row_id).cmp(&(&right.0, right.1.feature_row_id))
    });
    if corpus.len() < CORPUS_ROWS {
        return Err(invalid("V36 prefix corpus size differs"));
    }
    corpus.truncate(CORPUS_ROWS);
    let corpus = corpus
        .into_iter()
        .enumerate()
        .map(|(ordinal, (_, mut row))| {
            row.source_ordinal = Some(u64::try_from(ordinal).unwrap());
            row
        })
        .collect();
    let [development, validation, sealed_holdout, performance] = selected
        .try_into()
        .map_err(|_| invalid("V36 prefix query roles differ"))?;
    Ok(V36PrefixRoleSplit {
        development,
        validation,
        sealed_holdout,
        performance,
        corpus,
    })
}

fn vector_field() -> Field {
    Field::new(
        "embedding",
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, false)),
            DIMENSIONS as i32,
        ),
        false,
    )
}

fn registered_input_schema() -> Schema {
    Schema::new(vec![
        Field::new("url", DataType::Utf8, true),
        Field::new("natural_score", DataType::Float32, true),
        Field::new("feature_row_id", DataType::Int64, true),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIMENSIONS as i32,
            ),
            true,
        ),
    ])
}

fn sha256_file(path: &Path) -> Result<(u64, String)> {
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut bytes = 0_u64;
    let mut hasher = Sha256::new();
    loop {
        let read = reader.read(&mut buffer).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(u64::try_from(read).unwrap())
            .ok_or_else(|| invalid("V36 prefix registered object length overflows"))?;
        hasher.update(&buffer[..read]);
    }
    Ok((bytes, format!("{:x}", hasher.finalize())))
}

fn authenticate_file(path: &Path, expected: &V36ArtifactIdentity) -> Result<()> {
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut bytes = 0_u64;
    let mut sha256 = Sha256::new();
    let mut blake3 = blake3::Hasher::new();
    loop {
        let read = reader.read(&mut buffer).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(u64::try_from(read).unwrap())
            .ok_or_else(|| invalid("V36 prefix local input length overflows"))?;
        sha256.update(&buffer[..read]);
        blake3.update(&buffer[..read]);
    }
    if bytes != expected.encoded_bytes
        || format!("{:x}", sha256.finalize()) != expected.sha256
        || blake3.finalize().to_hex().as_str() != expected.blake3
    {
        return Err(invalid("V36 prefix local input authority differs"));
    }
    Ok(())
}

fn read_file(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })
}

fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut temporary = temporary_output(path)?;
    temporary
        .write_all(bytes)
        .map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
    publish_output(temporary, path)
}

fn acquire_v36_prefix_object(
    runtime: &tokio::runtime::Runtime,
    object: &V36PrefixRankedSourceObject,
    scratch: &Path,
    ordinal: usize,
) -> Result<PathBuf> {
    if ordinal >= 16 || !scratch.is_dir() || object.encoded_bytes == 0 {
        return Err(invalid("V36 prefix object acquisition request differs"));
    }
    digest_bytes(&object.sha256)?;
    let uri = url::Url::parse(&object.uri)
        .map_err(|_| invalid("V36 prefix registered object URI differs"))?;
    let (scheme, location) = ObjectStoreScheme::parse(&uri)
        .map_err(|_| invalid("V36 prefix registered object URI differs"))?;
    let store: Box<dyn ObjectStore> = match scheme {
        ObjectStoreScheme::Http => {
            let origin = uri.origin().ascii_serialization();
            Box::new(
                object_store::http::HttpBuilder::new()
                    .with_url(origin)
                    .with_retry(object_store::RetryConfig {
                        max_retries: 0,
                        ..Default::default()
                    })
                    .with_client_options(
                        object_store::ClientOptions::new()
                            .with_allow_http(uri.scheme() == "http")
                            .with_timeout_disabled()
                            .with_read_timeout(std::time::Duration::from_secs(120)),
                    )
                    .build()?,
            )
        }
        ObjectStoreScheme::Local => object_store::parse_url(&uri)?.0,
        _ => return Err(invalid("V36 prefix registered object scheme differs")),
    };
    let output = scratch.join(format!("source-{ordinal:04}.parquet"));
    if output.exists() {
        return Err(invalid("V36 prefix acquired object path already exists"));
    }
    let mut temporary =
        tempfile::NamedTempFile::new_in(scratch).map_err(|source| BorsukError::Io {
            path: scratch.to_owned(),
            source,
        })?;
    let (encoded_bytes, sha256) = runtime.block_on(async {
        let result = store.get(&location).await?;
        if result.meta.size != object.encoded_bytes {
            return Err(invalid("V36 prefix registered object length differs"));
        }
        let mut stream = result.into_stream();
        let mut encoded_bytes = 0_u64;
        let mut sha256 = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            encoded_bytes = encoded_bytes
                .checked_add(u64::try_from(chunk.len()).unwrap())
                .ok_or_else(|| invalid("V36 prefix acquired object length overflows"))?;
            if encoded_bytes > object.encoded_bytes {
                return Err(invalid("V36 prefix registered object length differs"));
            }
            temporary
                .write_all(&chunk)
                .map_err(|source| BorsukError::Io {
                    path: output.clone(),
                    source,
                })?;
            sha256.update(&chunk);
        }
        Ok((encoded_bytes, format!("{:x}", sha256.finalize())))
    })?;
    if encoded_bytes != object.encoded_bytes || sha256 != object.sha256 {
        return Err(invalid("V36 prefix registered object authority differs"));
    }
    temporary
        .as_file()
        .sync_all()
        .map_err(|source| BorsukError::Io {
            path: output.clone(),
            source,
        })?;
    temporary
        .persist_noclobber(&output)
        .map_err(|error| BorsukError::Io {
            path: output.clone(),
            source: error.error,
        })?;
    Ok(output)
}

struct V36PrefixAcquiredObjects {
    paths: Vec<PathBuf>,
}

impl V36PrefixAcquiredObjects {
    fn cleanup(mut self) -> Result<()> {
        while let Some(path) = self.paths.pop() {
            if let Err(source) = fs::remove_file(&path) {
                self.paths.push(path.clone());
                return Err(BorsukError::Io { path, source });
            }
        }
        Ok(())
    }
}

impl Drop for V36PrefixAcquiredObjects {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
    }
}

fn output_identity(
    role: &str,
    filename: &str,
    output_prefix: &str,
    path: &Path,
) -> Result<V36ArtifactIdentity> {
    let (encoded_bytes, sha256) = sha256_file(path)?;
    Ok(V36ArtifactIdentity {
        blake3: blake3_file(path)?,
        encoded_bytes,
        role: role.to_owned(),
        sha256,
        uri: format!("{output_prefix}{filename}"),
    })
}

/// Execute one complete bounded V36 diagnostic population freeze locally.
pub fn run_v36_prefix_freeze(request: V36PrefixFreezeRequest) -> Result<()> {
    let preflight = load_v36_prefix_freeze_preflight(&request)?;
    let runtime =
        tokio::runtime::Runtime::new().map_err(|_| invalid("V36 prefix object runtime differs"))?;
    let mut acquired = V36PrefixAcquiredObjects { paths: Vec::new() };
    let scan = scan_v36_prefix_object_prefix(
        &preflight.ranked_objects,
        usize::from(preflight.authority.object_cap),
        preflight.authority.source_byte_cap,
        usize::try_from(preflight.authority.distinct_candidates)
            .map_err(|_| invalid("V36 prefix distinct row count overflows"))?,
        |ordinal, object| {
            let path = acquire_v36_prefix_object(&runtime, object, &request.scratch, ordinal)?;
            acquired.paths.push(path.clone());
            Ok(path)
        },
    )?;
    let population = bind_v36_prefix_population_authority(
        &preflight.authority,
        scan.consumed_objects.clone(),
        &preflight.registry,
    )?;
    let split = select_v36_prefix_roles(scan.unique_rows, &population, &preflight.registry)?;
    let paths = materialize_v36_prefix_role_parquets(
        &acquired.paths,
        &preflight.ranked_objects[..acquired.paths.len()],
        &split,
        &request.scratch,
        &request.output,
    )?;
    let gt_paths = [
        request.output.join("development-gt100.parquet"),
        request.output.join("validation-gt100.parquet"),
        request.output.join("sealed-holdout-gt100.parquet"),
    ];
    let gt_jobs = [
        V36PrefixGtParquetJob {
            expected_queries: u32::try_from(split.development.len())
                .map_err(|_| invalid("V36 prefix development count overflows"))?,
            output: gt_paths[0].clone(),
            query: paths.development.clone(),
            role: V36PrefixQualityRole::Development,
        },
        V36PrefixGtParquetJob {
            expected_queries: u32::try_from(split.validation.len())
                .map_err(|_| invalid("V36 prefix validation count overflows"))?,
            output: gt_paths[1].clone(),
            query: paths.validation.clone(),
            role: V36PrefixQualityRole::Validation,
        },
        V36PrefixGtParquetJob {
            expected_queries: u32::try_from(split.sealed_holdout.len())
                .map_err(|_| invalid("V36 prefix holdout count overflows"))?,
            output: gt_paths[2].clone(),
            query: paths.sealed_holdout.clone(),
            role: V36PrefixQualityRole::SealedHoldout,
        },
    ];
    let source_feature_ids = split
        .corpus
        .iter()
        .map(|row| row.feature_row_id)
        .collect::<Vec<_>>();
    let gt_stats = write_v36_prefix_gt100_roles_from_parquets(
        &paths.source,
        &source_feature_ids,
        &gt_jobs,
        usize::from(population.workspace_count),
    )?;
    if gt_stats.source_scans != 1
        || gt_stats.source_rows != population.corpus_rows
        || gt_stats.quality_queries
            != u32::try_from(
                split.development.len() + split.validation.len() + split.sealed_holdout.len(),
            )
            .map_err(|_| invalid("V36 prefix quality query count overflows"))?
    {
        return Err(invalid("V36 prefix exact truth execution differs"));
    }

    let population_path = request.output.join("population-authority.json");
    write_atomic_bytes(
        &population_path,
        &canonical_v36_prefix_population_authority_bytes(&population, &preflight.registry)?,
    )?;
    let artifact_paths = [
        (
            "population-authority",
            "population-authority.json",
            &population_path,
        ),
        ("source", "source.parquet", &paths.source),
        (
            "development-query",
            "development-query.parquet",
            &paths.development,
        ),
        (
            "development-gt100",
            "development-gt100.parquet",
            &gt_paths[0],
        ),
        (
            "validation-query",
            "validation-query.parquet",
            &paths.validation,
        ),
        ("validation-gt100", "validation-gt100.parquet", &gt_paths[1]),
        (
            "sealed-holdout-query",
            "sealed-holdout-query.parquet",
            &paths.sealed_holdout,
        ),
        (
            "sealed-holdout-gt100",
            "sealed-holdout-gt100.parquet",
            &gt_paths[2],
        ),
        (
            "performance-query",
            "performance-query.parquet",
            &paths.performance,
        ),
    ];
    let outputs = artifact_paths
        .into_iter()
        .map(|(role, filename, path)| {
            output_identity(
                role,
                filename,
                &preflight.execution_authority.output_prefix,
                path,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let receipt = V36PrefixFreezeReceipt {
        claim_eligible: false,
        cutoff_object_ordinal: scan.cutoff_object_ordinal,
        cutoff_row_offset: scan.cutoff_row_offset,
        distinct_rows_observed: scan.distinct_rows_observed,
        duplicate_rows: scan.duplicate_rows,
        execution_authority_sha256: format!(
            "{:x}",
            Sha256::digest(canonical_v36_prefix_freeze_execution_authority_bytes(
                &preflight.execution_authority,
            )?)
        ),
        freeze_authority_sha256: format!(
            "{:x}",
            Sha256::digest(canonical_v36_prefix_freeze_authority_bytes(
                &preflight.authority,
                &preflight.registry,
            )?)
        ),
        outputs,
        physical_rows: scan.physical_rows,
        population,
        schema: "borsuk-v36-prefix-freeze-receipt-v1".to_owned(),
        source_archive_sha256: preflight
            .execution_authority
            .inputs
            .iter()
            .find(|input| input.role == "source-archive")
            .ok_or_else(|| invalid("V36 prefix source archive authority differs"))?
            .sha256
            .clone(),
        source_registry_sha256: format!(
            "{:x}",
            Sha256::digest(canonical_v36_prefix_source_registry_bytes(
                &preflight.authority,
                &preflight.registry,
            )?)
        ),
    };
    write_atomic_bytes(
        &request.output.join("freeze-receipt.json"),
        &canonical_v36_prefix_freeze_receipt_bytes(
            &receipt,
            &preflight.authority,
            &preflight.execution_authority,
            &preflight.registry,
        )?,
    )?;
    acquired.cleanup()?;
    Ok(())
}

/// Authenticate every local attempt input before any source-object network access.
pub fn load_v36_prefix_freeze_preflight(
    request: &V36PrefixFreezeRequest,
) -> Result<V36PrefixFreezePreflight> {
    if request.output == request.scratch
        || !request.output.is_dir()
        || !request.scratch.is_dir()
        || request
            .output
            .read_dir()
            .map_err(|source| BorsukError::Io {
                path: request.output.clone(),
                source,
            })?
            .next()
            .is_some()
        || request
            .scratch
            .read_dir()
            .map_err(|source| BorsukError::Io {
                path: request.scratch.clone(),
                source,
            })?
            .next()
            .is_some()
    {
        return Err(invalid("V36 prefix attempt directories differ"));
    }
    let execution_bytes = read_file(&request.execution_authority)?;
    let execution_authority: V36PrefixFreezeExecutionAuthority =
        serde_json::from_slice(&execution_bytes)
            .map_err(|_| invalid("V36 prefix execution authority JSON differs"))?;
    validate_v36_prefix_freeze_execution_authority(&execution_authority)?;
    if canonical_v36_prefix_freeze_execution_authority_bytes(&execution_authority)?
        != execution_bytes
    {
        return Err(invalid("V36 prefix execution authority bytes differ"));
    }
    let input = |role: &str| {
        execution_authority
            .inputs
            .iter()
            .find(|input| input.role == role)
            .ok_or_else(|| invalid("V36 prefix execution input role differs"))
    };
    authenticate_file(&request.executable, input("binary")?)?;
    authenticate_file(&request.authority, input("freeze-authority")?)?;
    authenticate_file(&request.source_archive, input("source-archive")?)?;
    authenticate_file(&request.source_registry, input("source-registry")?)?;

    let authority_bytes = read_file(&request.authority)?;
    let authority: V36PrefixFreezeAuthority = serde_json::from_slice(&authority_bytes)
        .map_err(|_| invalid("V36 prefix freeze authority JSON differs"))?;
    let registry_bytes = read_file(&request.source_registry)?;
    let registry: Vec<V36PrefixRegisteredSourceObject> = serde_json::from_slice(&registry_bytes)
        .map_err(|_| invalid("V36 prefix source registry JSON differs"))?;
    validate_v36_prefix_freeze_authority(&authority, &registry)?;
    if canonical_v36_prefix_freeze_authority_bytes(&authority, &registry)? != authority_bytes
        || crate::canonical_v36_prefix_source_registry_bytes(&authority, &registry)?
            != registry_bytes
    {
        return Err(invalid("V36 prefix local authority bytes differ"));
    }
    let ranked_objects = rank_v36_prefix_source_objects(&authority, &registry)?;
    Ok(V36PrefixFreezePreflight {
        authority,
        execution_authority,
        registry,
        ranked_objects,
    })
}

/// Authenticate and stream one complete registered raw source object.
pub fn scan_v36_prefix_registered_input_parquet<F>(
    path: &Path,
    object: &V36PrefixRankedSourceObject,
    selected_object_ordinal: u16,
    mut consume: F,
) -> Result<u64>
where
    F: FnMut(V36PrefixInputRow) -> Result<()>,
{
    digest_bytes(&object.sha256)?;
    digest_bytes(&object.sample_sha256)?;
    if object.path.is_empty()
        || object.uri.is_empty()
        || selected_object_ordinal >= 16
        || object.sample_sha256
            != v36_prefix_object_sample_sha256(&object.path, object.encoded_bytes)
    {
        return Err(invalid("V36 prefix registered object identity differs"));
    }
    let (encoded_bytes, sha256) = sha256_file(path)?;
    if encoded_bytes != object.encoded_bytes || sha256 != object.sha256 {
        return Err(invalid("V36 prefix registered object authority differs"));
    }
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let expected_schema = registered_input_schema();
    validate_parquet_descriptor(builder.parquet_schema(), &expected_schema)?;
    if builder.schema().as_ref() != &expected_schema {
        return Err(invalid("V36 prefix registered object schema differs"));
    }
    let mut row_offset = 0_u64;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.schema().as_ref() != &expected_schema
            || batch.num_columns() != 4
            || batch.num_rows() == 0
        {
            return Err(invalid("V36 prefix registered object batch differs"));
        }
        let ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("V36 prefix registered object ID column differs"))?;
        let embeddings = batch
            .column(3)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V36 prefix registered object embedding column differs"))?;
        let values = embeddings
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V36 prefix registered object embedding child differs"))?;
        if ids.null_count() != 0
            || embeddings.null_count() != 0
            || values.null_count() != 0
            || values.len() != batch.num_rows() * DIMENSIONS
        {
            return Err(invalid("V36 prefix registered object gated null differs"));
        }
        for row in 0..batch.num_rows() {
            let start = row * DIMENSIONS;
            let input = V36PrefixInputRow {
                feature_row_id: ids.value(row),
                selected_object_ordinal,
                row_offset,
                embedding: values.values()[start..start + DIMENSIONS].to_vec(),
            };
            validate_v36_prefix_input_row(&input)?;
            consume(input)?;
            row_offset = row_offset
                .checked_add(1)
                .ok_or_else(|| invalid("V36 prefix registered object row count overflows"))?;
        }
    }
    if row_offset == 0 {
        return Err(invalid("V36 prefix registered object is empty"));
    }
    Ok(row_offset)
}

fn blake3_file(path: &Path) -> Result<String> {
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut hasher = blake3::Hasher::new();
    loop {
        let read = reader.read(&mut buffer).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn scan_v36_prefix_object_prefix_from_state<F, C>(
    ranked_objects: &[V36PrefixRankedSourceObject],
    object_cap: usize,
    byte_cap: u64,
    distinct_candidates: usize,
    restored: Option<V36PrefixRestoredPopulation>,
    mut acquire: F,
    mut commit: C,
) -> Result<V36PrefixObjectPrefixScan>
where
    F: FnMut(usize, &V36PrefixRankedSourceObject) -> Result<PathBuf>,
    C: FnMut(&V36PrefixPopulationCommit) -> Result<()>,
{
    if object_cap == 0
        || object_cap > 16
        || byte_cap == 0
        || distinct_candidates == 0
        || ranked_objects.is_empty()
    {
        return Err(invalid("V36 prefix source scan limits differ"));
    }
    let restored = restored.unwrap_or(V36PrefixRestoredPopulation {
        consumed_objects: Vec::new(),
        cutoff: None,
        distinct_rows_observed: 0,
        duplicate_rows: 0,
        next_object_ordinal: 0,
        physical_rows: 0,
        unique_rows: Vec::new(),
    });
    let start = usize::from(restored.next_object_ordinal);
    if start > object_cap
        || start > ranked_objects.len()
        || restored
            .consumed_objects
            .iter()
            .zip(ranked_objects)
            .any(|(source, ranked)| {
                source.encoded_bytes != ranked.encoded_bytes
                    || source.path != ranked.path
                    || source.sample_sha256 != ranked.sample_sha256
                    || source.sha256 != ranked.sha256
                    || source.uri != ranked.uri
            })
    {
        return Err(invalid("V36 prefix restored source authority differs"));
    }
    let mut consumed_objects = restored.consumed_objects;
    let mut seen = restored
        .unique_rows
        .iter()
        .map(|row| row.feature_row_id)
        .collect::<HashSet<_>>();
    if seen.len() != restored.unique_rows.len()
        || u64::try_from(seen.len()).unwrap_or(u64::MAX) != restored.distinct_rows_observed
    {
        return Err(invalid("V36 prefix restored identity authority differs"));
    }
    let mut unique_rows = restored.unique_rows;
    let mut physical_rows = restored.physical_rows;
    let mut encoded_bytes = consumed_objects.iter().try_fold(0_u64, |total, object| {
        total
            .checked_add(object.encoded_bytes)
            .ok_or_else(|| invalid("V36 prefix source scan bytes overflow"))
    })?;
    let mut cutoff = restored.cutoff;
    for (ordinal, object) in ranked_objects
        .iter()
        .enumerate()
        .take(object_cap)
        .skip(start)
    {
        encoded_bytes = encoded_bytes
            .checked_add(object.encoded_bytes)
            .ok_or_else(|| invalid("V36 prefix source scan bytes overflow"))?;
        if encoded_bytes > byte_cap {
            break;
        }
        let path = acquire(ordinal, object)?;
        let selected_object_ordinal = u16::try_from(ordinal)
            .map_err(|_| invalid("V36 prefix source object ordinal overflows"))?;
        let mut object_seen = HashSet::new();
        let mut object_identities = Vec::new();
        let object_rows = scan_v36_prefix_registered_input_parquet(
            &path,
            object,
            selected_object_ordinal,
            |row| {
                let identity = validate_v36_prefix_input_row(&row)?;
                if !seen.contains(&identity.feature_row_id)
                    && object_seen.insert(identity.feature_row_id)
                {
                    object_identities.push(identity);
                }
                Ok(())
            },
        )?;
        if object_rows == 0 {
            return Err(invalid("V36 prefix source object is empty"));
        }
        let source = crate::V36PrefixSourceObject {
            blake3: blake3_file(&path)?,
            encoded_bytes: object.encoded_bytes,
            path: object.path.clone(),
            sample_sha256: object.sample_sha256.clone(),
            sha256: object.sha256.clone(),
            uri: object.uri.clone(),
        };
        physical_rows = physical_rows
            .checked_add(object_rows)
            .ok_or_else(|| invalid("V36 prefix physical rows overflow"))?;
        for identity in &object_identities {
            if !seen.insert(identity.feature_row_id) {
                return Err(invalid("V36 prefix provisional identity commit differs"));
            }
            if unique_rows.len() < distinct_candidates {
                unique_rows.push(identity.clone());
                if unique_rows.len() == distinct_candidates {
                    cutoff = Some((selected_object_ordinal, identity.row_offset));
                }
            }
        }
        consumed_objects.push(source.clone());
        let distinct_rows = u64::try_from(seen.len()).unwrap_or(u64::MAX);
        let duplicate_rows = physical_rows
            .checked_sub(distinct_rows)
            .ok_or_else(|| invalid("V36 prefix duplicate rows underflow"))?;
        let boundary = V36PrefixPopulationCommit {
            cutoff,
            distinct_rows,
            duplicate_rows,
            physical_rows,
            run: V36PrefixIdentityRun {
                physical_rows: object_rows,
                rows: object_identities,
                selected_object_ordinal,
                source,
            },
        };
        validate_v36_prefix_identity_run(&boundary.run)?;
        commit(&boundary)?;
        if cutoff.is_some() {
            break;
        }
    }
    let (cutoff_object_ordinal, cutoff_row_offset) =
        cutoff.ok_or(BorsukError::V36PrefixSourceInsufficient)?;
    validate_v36_prefix_cutoff_membership(
        &unique_rows,
        consumed_objects.len(),
        distinct_candidates,
    )?;
    let distinct_rows_observed = u64::try_from(seen.len()).unwrap_or(u64::MAX);
    Ok(V36PrefixObjectPrefixScan {
        consumed_objects,
        cutoff_object_ordinal,
        cutoff_row_offset,
        distinct_rows_observed,
        duplicate_rows: physical_rows
            .checked_sub(distinct_rows_observed)
            .ok_or_else(|| invalid("V36 prefix duplicate rows underflow"))?,
        physical_rows,
        unique_rows,
    })
}

/// Scan authenticated objects and emit a durable boundary only after each completes.
pub fn scan_v36_prefix_object_prefix_checkpointed<F, C>(
    ranked_objects: &[V36PrefixRankedSourceObject],
    object_cap: usize,
    byte_cap: u64,
    distinct_candidates: usize,
    acquire: F,
    commit: C,
) -> Result<V36PrefixObjectPrefixScan>
where
    F: FnMut(usize, &V36PrefixRankedSourceObject) -> Result<PathBuf>,
    C: FnMut(&V36PrefixPopulationCommit) -> Result<()>,
{
    scan_v36_prefix_object_prefix_from_state(
        ranked_objects,
        object_cap,
        byte_cap,
        distinct_candidates,
        None,
        acquire,
        commit,
    )
}

/// Resume a source scan strictly after an authenticated complete-object prefix.
pub fn scan_v36_prefix_object_prefix_resumed<F, C>(
    ranked_objects: &[V36PrefixRankedSourceObject],
    object_cap: usize,
    byte_cap: u64,
    distinct_candidates: usize,
    prior_runs: &[V36PrefixIdentityRun],
    acquire: F,
    commit: C,
) -> Result<V36PrefixObjectPrefixScan>
where
    F: FnMut(usize, &V36PrefixRankedSourceObject) -> Result<PathBuf>,
    C: FnMut(&V36PrefixPopulationCommit) -> Result<()>,
{
    let restored = restore_v36_prefix_population_state(prior_runs, distinct_candidates)?;
    scan_v36_prefix_object_prefix_from_state(
        ranked_objects,
        object_cap,
        byte_cap,
        distinct_candidates,
        Some(restored),
        acquire,
        commit,
    )
}

/// Scan authenticated complete objects through a distinct-ID cutoff.
pub fn scan_v36_prefix_object_prefix<F>(
    ranked_objects: &[V36PrefixRankedSourceObject],
    object_cap: usize,
    byte_cap: u64,
    distinct_candidates: usize,
    acquire: F,
) -> Result<V36PrefixObjectPrefixScan>
where
    F: FnMut(usize, &V36PrefixRankedSourceObject) -> Result<PathBuf>,
{
    scan_v36_prefix_object_prefix_checkpointed(
        ranked_objects,
        object_cap,
        byte_cap,
        distinct_candidates,
        acquire,
        |_| Ok(()),
    )
}

/// Exact physical schema of a V36 prefix source table.
pub fn v36_prefix_source_schema() -> Schema {
    Schema::new(vec![
        Field::new("feature_row_id", DataType::UInt64, false),
        vector_field(),
    ])
}

/// Exact physical schema of a V36 prefix query table.
pub fn v36_prefix_query_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("feature_row_id", DataType::UInt64, false),
        vector_field(),
    ])
}

/// Exact physical schema of a V36 prefix exact-GT table.
pub fn v36_prefix_gt100_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("rank", DataType::UInt16, false),
        Field::new("feature_row_id", DataType::UInt64, false),
        Field::new("squared_distance", DataType::Float64, false),
    ])
}

fn validate_expected_feature_ids(expected: &[u64]) -> Result<()> {
    if expected.is_empty() {
        return Err(invalid("V36 prefix source membership is empty"));
    }
    let mut sorted = expected.to_vec();
    sorted.sort_unstable();
    if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("V36 prefix source membership overlaps"));
    }
    Ok(())
}

fn validate_source_batch(
    batch: &RecordBatch,
    expected_feature_ids: &[u64],
    next_ordinal: &mut usize,
) -> Result<()> {
    if batch.schema().as_ref() != &v36_prefix_source_schema()
        || batch.num_rows() == 0
        || batch.num_columns() != 2
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V36 prefix source Parquet batch differs"));
    }
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 prefix source Parquet ID column differs"))?;
    let embeddings = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V36 prefix source Parquet embedding column differs"))?;
    let values = embeddings
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V36 prefix source Parquet embedding child differs"))?;
    if values.null_count() != 0 || values.len() != batch.num_rows() * DIMENSIONS {
        return Err(invalid("V36 prefix source Parquet embedding shape differs"));
    }
    for row in 0..batch.num_rows() {
        if expected_feature_ids.get(*next_ordinal).copied() != Some(ids.value(row)) {
            return Err(invalid("V36 prefix source Parquet membership differs"));
        }
        let start = row * DIMENSIONS;
        validate_embedding(&values.values()[start..start + DIMENSIONS])?;
        *next_ordinal = next_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("V36 prefix source Parquet row count overflows"))?;
    }
    Ok(())
}

const MATERIALIZATION_BUCKET_ROWS: usize = 8_192;
const MATERIALIZATION_RECORD_BYTES: usize = 16 + DIMENSIONS * 4;

fn write_materialization_record<W: Write>(
    writer: &mut W,
    path: &Path,
    ordinal: usize,
    row: &V36PrefixMaterializedRow,
) -> Result<()> {
    validate_embedding(&row.embedding)?;
    let mut record = [0_u8; MATERIALIZATION_RECORD_BYTES];
    record[..8].copy_from_slice(
        &u64::try_from(ordinal)
            .map_err(|_| invalid("V36 prefix materialization ordinal overflows"))?
            .to_le_bytes(),
    );
    record[8..16].copy_from_slice(&row.feature_row_id.to_le_bytes());
    for (encoded, value) in record[16..]
        .as_chunks_mut::<4>()
        .0
        .iter_mut()
        .zip(&row.embedding)
    {
        *encoded = value.to_bits().to_le_bytes();
    }
    writer.write_all(&record).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })
}

fn materialization_rows(split: &V36PrefixRoleSplit) -> [&[V36PrefixRowIdentity]; 5] {
    [
        &split.corpus,
        &split.development,
        &split.validation,
        &split.sealed_holdout,
        &split.performance,
    ]
}

fn read_materialization_bucket(path: &Path) -> Result<Vec<(u64, V36PrefixMaterializedRow)>> {
    let bytes = read_file(path)?;
    if bytes.is_empty() || bytes.len() % MATERIALIZATION_RECORD_BYTES != 0 {
        return Err(invalid("V36 prefix materialization spool differs"));
    }
    let mut rows = Vec::with_capacity(bytes.len() / MATERIALIZATION_RECORD_BYTES);
    for record in bytes.as_chunks::<MATERIALIZATION_RECORD_BYTES>().0 {
        let ordinal = u64::from_le_bytes(record[..8].try_into().unwrap());
        let feature_row_id = u64::from_le_bytes(record[8..16].try_into().unwrap());
        let mut embedding = Vec::with_capacity(DIMENSIONS);
        for component in record[16..].as_chunks::<4>().0 {
            embedding.push(f32::from_bits(u32::from_le_bytes(*component)));
        }
        validate_embedding(&embedding)?;
        rows.push((
            ordinal,
            V36PrefixMaterializedRow {
                feature_row_id,
                source_ordinal: Some(ordinal),
                embedding,
            },
        ));
    }
    rows.sort_by_key(|(ordinal, _)| *ordinal);
    Ok(rows)
}

fn materialized_batch(
    rows: &[(u64, V36PrefixMaterializedRow)],
    query: bool,
) -> Result<RecordBatch> {
    let ordinals = rows
        .iter()
        .map(|(ordinal, _)| u32::try_from(*ordinal))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| invalid("V36 prefix materialization ordinal overflows"))?;
    let ids = rows
        .iter()
        .map(|(_, row)| row.feature_row_id)
        .collect::<Vec<_>>();
    let values = rows
        .iter()
        .flat_map(|(_, row)| row.embedding.iter().copied())
        .collect::<Vec<_>>();
    let embeddings = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        DIMENSIONS as i32,
        Arc::new(Float32Array::from(values)),
        None,
    )?;
    let mut columns = Vec::<ArrayRef>::new();
    let schema = if query {
        columns.push(Arc::new(UInt32Array::from(ordinals)));
        columns.push(Arc::new(UInt64Array::from(ids)));
        v36_prefix_query_schema()
    } else {
        columns.push(Arc::new(UInt64Array::from(ids)));
        v36_prefix_source_schema()
    };
    columns.push(Arc::new(embeddings));
    Ok(RecordBatch::try_new(Arc::new(schema), columns)?)
}

fn write_spooled_output(
    path: &Path,
    rows: &[V36PrefixRowIdentity],
    buckets: &[PathBuf],
    query: bool,
) -> Result<()> {
    let schema = if query {
        v36_prefix_query_schema()
    } else {
        v36_prefix_source_schema()
    };
    let mut temporary = temporary_output(path)?;
    let mut writer = ArrowWriter::try_new(
        temporary.as_file_mut(),
        Arc::new(schema),
        Some(parquet_writer_properties()),
    )?;
    let expected = rows
        .iter()
        .map(|row| row.feature_row_id)
        .collect::<Vec<_>>();
    let mut source_ordinal = 0_usize;
    let mut query_ordinal = 0_u64;
    let mut query_ids = BTreeSet::new();
    for bucket in buckets {
        let decoded = read_materialization_bucket(bucket)?;
        let batch = materialized_batch(&decoded, query)?;
        if query {
            validate_query_batch(&batch, &mut query_ordinal, &mut query_ids)?;
        } else {
            validate_source_batch(&batch, &expected, &mut source_ordinal)?;
        }
        writer.write(&batch)?;
    }
    if (query && query_ordinal != rows.len() as u64) || (!query && source_ordinal != rows.len()) {
        return Err(invalid("V36 prefix materialization row count differs"));
    }
    writer.close()?;
    publish_output(temporary, path)
}

/// Materialize canonical source and query Parquet using bounded ordinal buckets.
pub fn materialize_v36_prefix_role_parquets(
    source_paths: &[PathBuf],
    ranked_objects: &[V36PrefixRankedSourceObject],
    split: &V36PrefixRoleSplit,
    scratch: &Path,
    output: &Path,
) -> Result<V36PrefixRoleParquetPaths> {
    if source_paths.len() != ranked_objects.len()
        || source_paths.is_empty()
        || !scratch.is_dir()
        || !output.is_dir()
    {
        return Err(invalid("V36 prefix materialization inputs differ"));
    }
    let role_rows = materialization_rows(split);
    if role_rows.iter().any(|rows| rows.is_empty()) {
        return Err(invalid("V36 prefix materialization role is empty"));
    }
    let mut destinations = HashMap::new();
    for (role, rows) in role_rows.iter().enumerate() {
        for (ordinal, row) in rows.iter().enumerate() {
            if (role == 0 && row.source_ordinal != Some(ordinal as u64))
                || (role != 0 && row.source_ordinal.is_some())
                || destinations
                    .insert(
                        (row.selected_object_ordinal, row.row_offset),
                        (role, ordinal, row.feature_row_id),
                    )
                    .is_some()
            {
                return Err(invalid("V36 prefix materialization membership differs"));
            }
        }
    }
    let mut spool_paths = Vec::new();
    let mut spools = Vec::new();
    for (role, rows) in role_rows.iter().enumerate() {
        let mut role_paths = Vec::new();
        let mut role_files = Vec::new();
        for bucket in 0..rows.len().div_ceil(MATERIALIZATION_BUCKET_ROWS) {
            let path = scratch.join(format!("role-{role}-bucket-{bucket:06}.bin"));
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|source| BorsukError::Io {
                    path: path.clone(),
                    source,
                })?;
            role_paths.push(path);
            role_files.push(BufWriter::new(file));
        }
        spool_paths.push(role_paths);
        spools.push(role_files);
    }
    for (object_ordinal, (path, object)) in source_paths.iter().zip(ranked_objects).enumerate() {
        scan_v36_prefix_registered_input_parquet(
            path,
            object,
            object_ordinal
                .try_into()
                .map_err(|_| invalid("V36 prefix materialization object ordinal overflows"))?,
            |row| {
                if let Some((role, ordinal, feature_row_id)) =
                    destinations.remove(&(row.selected_object_ordinal, row.row_offset))
                {
                    if feature_row_id != u64::try_from(row.feature_row_id).unwrap_or(u64::MAX) {
                        return Err(invalid("V36 prefix materialization feature ID differs"));
                    }
                    let file = &mut spools[role][ordinal / MATERIALIZATION_BUCKET_ROWS];
                    write_materialization_record(
                        file,
                        &spool_paths[role][ordinal / MATERIALIZATION_BUCKET_ROWS],
                        ordinal,
                        &V36PrefixMaterializedRow {
                            feature_row_id,
                            source_ordinal: Some(ordinal as u64),
                            embedding: row.embedding,
                        },
                    )?;
                }
                Ok(())
            },
        )?;
    }
    if !destinations.is_empty() {
        return Err(invalid("V36 prefix materialization row is missing"));
    }
    for (role, role_spools) in spools.iter_mut().enumerate() {
        for (bucket, spool) in role_spools.iter_mut().enumerate() {
            spool.flush().map_err(|source| BorsukError::Io {
                path: spool_paths[role][bucket].clone(),
                source,
            })?;
        }
    }
    drop(spools);
    let names = [
        "source.parquet",
        "development-query.parquet",
        "validation-query.parquet",
        "sealed-holdout-query.parquet",
        "performance-query.parquet",
    ];
    let mut outputs = Vec::new();
    for (role, (rows, name)) in role_rows.iter().zip(names).enumerate() {
        let path = output.join(name);
        write_spooled_output(&path, rows, &spool_paths[role], role != 0)?;
        outputs.push(path);
    }
    for role in spool_paths {
        for path in role {
            fs::remove_file(&path).map_err(|source| BorsukError::Io { path, source })?;
        }
    }
    let [source, development, validation, sealed_holdout, performance] = outputs
        .try_into()
        .map_err(|_| invalid("V36 prefix materialization outputs differ"))?;
    Ok(V36PrefixRoleParquetPaths {
        development,
        performance,
        sealed_holdout,
        source,
        validation,
    })
}

/// Write validated source batches without retaining the complete corpus in RAM.
pub fn write_v36_prefix_source_parquet<I>(
    path: &Path,
    expected_feature_ids: &[u64],
    batches: I,
) -> Result<()>
where
    I: IntoIterator<Item = RecordBatch>,
{
    validate_expected_feature_ids(expected_feature_ids)?;
    let mut temporary = temporary_output(path)?;
    let mut writer = ArrowWriter::try_new(
        temporary.as_file_mut(),
        Arc::new(v36_prefix_source_schema()),
        Some(parquet_writer_properties()),
    )?;
    let mut next_ordinal = 0_usize;
    for batch in batches {
        validate_source_batch(&batch, expected_feature_ids, &mut next_ordinal)?;
        writer.write(&batch)?;
    }
    if next_ordinal != expected_feature_ids.len() {
        return Err(invalid("V36 prefix source Parquet row count differs"));
    }
    writer.close()?;
    publish_output(temporary, path)?;
    Ok(())
}

/// Stream and validate a complete source Parquet artifact one batch at a time.
pub fn scan_v36_prefix_source_parquet<F>(
    path: &Path,
    expected_feature_ids: &[u64],
    mut consume: F,
) -> Result<()>
where
    F: FnMut(RecordBatch) -> Result<()>,
{
    validate_expected_feature_ids(expected_feature_ids)?;
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    validate_parquet_descriptor(builder.parquet_schema(), &v36_prefix_source_schema())?;
    if builder.schema().as_ref() != &v36_prefix_source_schema() {
        return Err(invalid("V36 prefix source Parquet physical schema differs"));
    }
    let mut next_ordinal = 0_usize;
    for batch in builder.build()? {
        let batch = batch?;
        validate_source_batch(&batch, expected_feature_ids, &mut next_ordinal)?;
        consume(batch)?;
    }
    if next_ordinal != expected_feature_ids.len() {
        return Err(invalid("V36 prefix source Parquet row count differs"));
    }
    Ok(())
}

fn validate_query_batch(
    batch: &RecordBatch,
    next_ordinal: &mut u64,
    feature_ids: &mut BTreeSet<u64>,
) -> Result<()> {
    if batch.schema().as_ref() != &v36_prefix_query_schema()
        || batch.num_rows() == 0
        || batch.num_columns() != 3
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V36 prefix query Parquet batch differs"));
    }
    let ordinals = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::UInt32Array>()
        .ok_or_else(|| invalid("V36 prefix query Parquet ordinal column differs"))?;
    let ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 prefix query Parquet ID column differs"))?;
    let embeddings = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V36 prefix query Parquet embedding column differs"))?;
    let values = embeddings
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V36 prefix query Parquet embedding child differs"))?;
    if values.null_count() != 0 || values.len() != batch.num_rows() * DIMENSIONS {
        return Err(invalid("V36 prefix query Parquet embedding shape differs"));
    }
    for row in 0..batch.num_rows() {
        if u64::from(ordinals.value(row)) != *next_ordinal || !feature_ids.insert(ids.value(row)) {
            return Err(invalid("V36 prefix query Parquet ordering differs"));
        }
        let start = row * DIMENSIONS;
        validate_embedding(&values.values()[start..start + DIMENSIONS])?;
        *next_ordinal = next_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("V36 prefix query Parquet row count overflows"))?;
    }
    Ok(())
}

/// Write validated query batches without retaining all query vectors in RAM.
pub fn write_v36_prefix_query_parquet<I>(path: &Path, batches: I) -> Result<()>
where
    I: IntoIterator<Item = RecordBatch>,
{
    let mut temporary = temporary_output(path)?;
    let mut writer = ArrowWriter::try_new(
        temporary.as_file_mut(),
        Arc::new(v36_prefix_query_schema()),
        Some(parquet_writer_properties()),
    )?;
    let mut feature_ids = BTreeSet::new();
    let mut next_ordinal = 0_u64;
    for batch in batches {
        validate_query_batch(&batch, &mut next_ordinal, &mut feature_ids)?;
        writer.write(&batch)?;
    }
    if next_ordinal == 0 {
        return Err(invalid("V36 prefix query Parquet is empty"));
    }
    writer.close()?;
    publish_output(temporary, path)?;
    Ok(())
}

/// Stream and validate a complete query Parquet artifact.
pub fn scan_v36_prefix_query_parquet<F>(
    path: &Path,
    expected_rows: u64,
    mut consume: F,
) -> Result<()>
where
    F: FnMut(RecordBatch) -> Result<()>,
{
    if expected_rows == 0 {
        return Err(invalid("V36 prefix query Parquet row count differs"));
    }
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    validate_parquet_descriptor(builder.parquet_schema(), &v36_prefix_query_schema())?;
    if builder.schema().as_ref() != &v36_prefix_query_schema() {
        return Err(invalid("V36 prefix query Parquet physical schema differs"));
    }
    let mut feature_ids = BTreeSet::new();
    let mut next_ordinal = 0_u64;
    for batch in builder.build()? {
        let batch = batch?;
        validate_query_batch(&batch, &mut next_ordinal, &mut feature_ids)?;
        consume(batch)?;
    }
    if next_ordinal != expected_rows {
        return Err(invalid("V36 prefix query Parquet row count differs"));
    }
    Ok(())
}

#[derive(Default)]
struct GtValidationState {
    next_query: u32,
    next_rank: u16,
    prior: Option<(f64, u64)>,
    feature_ids: BTreeSet<u64>,
    rows: u64,
}

fn validate_gt_batch(batch: &RecordBatch, state: &mut GtValidationState) -> Result<()> {
    if batch.schema().as_ref() != &v36_prefix_gt100_schema()
        || batch.num_rows() == 0
        || batch.num_columns() != 4
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V36 prefix GT Parquet batch differs"));
    }
    let queries = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::UInt32Array>()
        .ok_or_else(|| invalid("V36 prefix GT query column differs"))?;
    let ranks = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::UInt16Array>()
        .ok_or_else(|| invalid("V36 prefix GT rank column differs"))?;
    let ids = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 prefix GT ID column differs"))?;
    let distances = batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .ok_or_else(|| invalid("V36 prefix GT distance column differs"))?;
    for row in 0..batch.num_rows() {
        let distance = distances.value(row);
        let id = ids.value(row);
        if queries.value(row) != state.next_query
            || ranks.value(row) != state.next_rank
            || !distance.is_finite()
            || distance < 0.0
            || !state.feature_ids.insert(id)
            || state
                .prior
                .is_some_and(|prior| prior.0.total_cmp(&distance).then(prior.1.cmp(&id)).is_gt())
        {
            return Err(invalid("V36 prefix GT ordering differs"));
        }
        state.rows = state
            .rows
            .checked_add(1)
            .ok_or_else(|| invalid("V36 prefix GT row count overflows"))?;
        if usize::from(state.next_rank) + 1 == GT_NEIGHBORS {
            state.next_query = state
                .next_query
                .checked_add(1)
                .ok_or_else(|| invalid("V36 prefix GT query count overflows"))?;
            state.next_rank = 0;
            state.prior = None;
            state.feature_ids.clear();
        } else {
            state.next_rank += 1;
            state.prior = Some((distance, id));
        }
    }
    Ok(())
}

/// Write validated exact-GT batches.
pub fn write_v36_prefix_gt100_parquet<I>(path: &Path, batches: I) -> Result<()>
where
    I: IntoIterator<Item = RecordBatch>,
{
    let mut temporary = temporary_output(path)?;
    let mut writer = ArrowWriter::try_new(
        temporary.as_file_mut(),
        Arc::new(v36_prefix_gt100_schema()),
        Some(parquet_writer_properties()),
    )?;
    let mut state = GtValidationState::default();
    for batch in batches {
        validate_gt_batch(&batch, &mut state)?;
        writer.write(&batch)?;
    }
    if state.rows == 0 || state.next_rank != 0 {
        return Err(invalid("V36 prefix GT row count differs"));
    }
    writer.close()?;
    publish_output(temporary, path)?;
    Ok(())
}

/// Stream and validate exact GT@100 for an exact query count.
pub fn scan_v36_prefix_gt100_parquet<F>(
    path: &Path,
    expected_queries: u32,
    mut consume: F,
) -> Result<()>
where
    F: FnMut(RecordBatch) -> Result<()>,
{
    if expected_queries == 0 {
        return Err(invalid("V36 prefix GT query count differs"));
    }
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    validate_parquet_descriptor(builder.parquet_schema(), &v36_prefix_gt100_schema())?;
    if builder.schema().as_ref() != &v36_prefix_gt100_schema() {
        return Err(invalid("V36 prefix GT physical schema differs"));
    }
    let mut state = GtValidationState::default();
    for batch in builder.build()? {
        let batch = batch?;
        validate_gt_batch(&batch, &mut state)?;
        consume(batch)?;
    }
    if state.next_query != expected_queries || state.next_rank != 0 {
        return Err(invalid("V36 prefix GT query count differs"));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct RankedNeighbor {
    distance: f64,
    feature_row_id: u64,
}

impl PartialEq for RankedNeighbor {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits()
            && self.feature_row_id == other.feature_row_id
    }
}

impl Eq for RankedNeighbor {}

impl PartialOrd for RankedNeighbor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedNeighbor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then(self.feature_row_id.cmp(&other.feature_row_id))
    }
}

fn squared_l2(left: &[f32], right: &[f32]) -> f64 {
    left.iter().zip(right).fold(0.0_f64, |sum, (left, right)| {
        let delta = f64::from(*left) - f64::from(*right);
        let squared = delta * delta;
        sum + squared
    })
}

/// Bounded exact-GT state that consumes the corpus in source-ordinal batches.
pub struct V36PrefixGtAccumulator {
    queries: Vec<V36PrefixQueryRow>,
    query_ids: BTreeSet<u64>,
    corpus_ids: BTreeSet<u64>,
    heaps: Vec<BinaryHeap<RankedNeighbor>>,
    next_source_ordinal: u64,
}

impl V36PrefixGtAccumulator {
    /// Create a GT tile accumulator for one quality-query role.
    pub fn new(_role: V36PrefixQualityRole, queries: Vec<V36PrefixQueryRow>) -> Result<Self> {
        let first_ordinal = queries.first().map(|row| row.query_ordinal);
        if queries.is_empty()
            || queries.iter().enumerate().any(|(ordinal, row)| {
                first_ordinal
                    .and_then(|first| first.checked_add(u32::try_from(ordinal).unwrap_or(u32::MAX)))
                    != Some(row.query_ordinal)
                    || validate_embedding(&row.embedding).is_err()
            })
        {
            return Err(invalid("V36 prefix exact truth query input differs"));
        }
        let query_ids = queries
            .iter()
            .map(|query| query.feature_row_id)
            .collect::<BTreeSet<_>>();
        if query_ids.len() != queries.len() {
            return Err(invalid("V36 prefix exact truth query membership differs"));
        }
        let heaps = (0..queries.len())
            .map(|_| BinaryHeap::with_capacity(GT_NEIGHBORS))
            .collect();
        Ok(Self {
            queries,
            query_ids,
            corpus_ids: BTreeSet::new(),
            heaps,
            next_source_ordinal: 0,
        })
    }

    /// Absorb one validated, source-ordered corpus batch.
    pub fn absorb(&mut self, corpus: &[V36PrefixMaterializedRow]) -> Result<()> {
        for row in corpus {
            if row.source_ordinal != Some(self.next_source_ordinal)
                || validate_embedding(&row.embedding).is_err()
                || self.query_ids.contains(&row.feature_row_id)
                || !self.corpus_ids.insert(row.feature_row_id)
            {
                return Err(invalid("V36 prefix exact truth corpus input differs"));
            }
            for (query, heap) in self.queries.iter().zip(&mut self.heaps) {
                let candidate = RankedNeighbor {
                    distance: squared_l2(&row.embedding, &query.embedding),
                    feature_row_id: row.feature_row_id,
                };
                if heap.len() < GT_NEIGHBORS {
                    heap.push(candidate);
                } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                    heap.pop();
                    heap.push(candidate);
                }
            }
            self.next_source_ordinal = self
                .next_source_ordinal
                .checked_add(1)
                .ok_or_else(|| invalid("V36 prefix exact truth corpus size overflows"))?;
        }
        Ok(())
    }

    /// Finish one complete GT tile in `(query_ordinal,rank)` order.
    pub fn finish(self) -> Result<Vec<V36PrefixGtNeighbor>> {
        if self.next_source_ordinal < GT_NEIGHBORS as u64
            || self.heaps.iter().any(|heap| heap.len() != GT_NEIGHBORS)
        {
            return Err(invalid("V36 prefix exact truth corpus is insufficient"));
        }
        let mut truth = Vec::with_capacity(self.queries.len() * GT_NEIGHBORS);
        for (query, heap) in self.queries.iter().zip(self.heaps) {
            let mut neighbors = heap.into_vec();
            neighbors.sort();
            truth.extend(neighbors.into_iter().enumerate().map(|(rank, neighbor)| {
                V36PrefixGtNeighbor {
                    query_ordinal: query.query_ordinal,
                    rank: u16::try_from(rank).unwrap(),
                    feature_row_id: neighbor.feature_row_id,
                    squared_distance: neighbor.distance,
                }
            }));
        }
        Ok(truth)
    }
}

/// Exact binary64 no-explicit-FMA GT@100 for quality queries.
pub fn exact_v36_prefix_gt100(
    role: V36PrefixQualityRole,
    corpus: &[V36PrefixMaterializedRow],
    queries: &[V36PrefixQueryRow],
) -> Result<Vec<V36PrefixGtNeighbor>> {
    let mut accumulator = V36PrefixGtAccumulator::new(role, queries.to_vec())?;
    accumulator.absorb(corpus)?;
    accumulator.finish()
}

fn v36_prefix_query_rows_from_batch(
    batch: &RecordBatch,
    offset: usize,
    rows: usize,
) -> Result<Vec<V36PrefixQueryRow>> {
    let ordinals = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V36 prefix query Parquet ordinal column differs"))?;
    let ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 prefix query Parquet ID column differs"))?;
    let embeddings = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V36 prefix query Parquet embedding column differs"))?;
    let values = embeddings
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V36 prefix query Parquet embedding child differs"))?;
    (offset..offset + rows)
        .map(|row| {
            let start = row * DIMENSIONS;
            let embedding = values.values()[start..start + DIMENSIONS].to_vec();
            validate_embedding(&embedding)?;
            Ok(V36PrefixQueryRow {
                query_ordinal: ordinals.value(row),
                feature_row_id: ids.value(row),
                embedding,
            })
        })
        .collect()
}

fn v36_prefix_source_rows_from_batch(
    batch: &RecordBatch,
    next_source_ordinal: &mut u64,
) -> Result<Vec<V36PrefixMaterializedRow>> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 prefix source Parquet ID column differs"))?;
    let embeddings = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V36 prefix source Parquet embedding column differs"))?;
    let values = embeddings
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V36 prefix source Parquet embedding child differs"))?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let start = row * DIMENSIONS;
        let embedding = values.values()[start..start + DIMENSIONS].to_vec();
        validate_embedding(&embedding)?;
        rows.push(V36PrefixMaterializedRow {
            feature_row_id: ids.value(row),
            source_ordinal: Some(*next_source_ordinal),
            embedding,
        });
        *next_source_ordinal = next_source_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("V36 prefix source Parquet row count overflows"))?;
    }
    Ok(rows)
}

fn v36_prefix_gt_batch(rows: &[V36PrefixGtNeighbor]) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(v36_prefix_gt100_schema()),
        vec![
            Arc::new(UInt32Array::from(
                rows.iter().map(|row| row.query_ordinal).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt16Array::from(
                rows.iter().map(|row| row.rank).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|row| row.feature_row_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|row| row.squared_distance)
                    .collect::<Vec<_>>(),
            )),
        ],
    )?)
}

struct V36PrefixGtQueryState {
    heap: BinaryHeap<RankedNeighbor>,
    query: V36PrefixQueryRow,
    role: usize,
}

/// Compute all quality-role GT@100 with one corpus scan and bounded state.
pub fn write_v36_prefix_gt100_roles_from_parquets(
    source_path: &Path,
    expected_source_feature_ids: &[u64],
    jobs: &[V36PrefixGtParquetJob],
    worker_threads: usize,
) -> Result<V36PrefixGtRunStats> {
    if jobs.len() != 3
        || worker_threads == 0
        || worker_threads > 16
        || jobs[0].role != V36PrefixQualityRole::Development
        || jobs[1].role != V36PrefixQualityRole::Validation
        || jobs[2].role != V36PrefixQualityRole::SealedHoldout
        || jobs.iter().any(|job| job.expected_queries == 0)
    {
        return Err(invalid("V36 prefix exact truth job set differs"));
    }
    let mut paths = BTreeSet::from([source_path]);
    if jobs
        .iter()
        .flat_map(|job| [&job.query, &job.output])
        .any(|path| !paths.insert(path))
    {
        return Err(invalid("V36 prefix exact truth paths overlap"));
    }
    let mut states = Vec::new();
    let mut query_ids = BTreeSet::new();
    for (role, job) in jobs.iter().enumerate() {
        scan_v36_prefix_query_parquet(&job.query, u64::from(job.expected_queries), |batch| {
            let rows = v36_prefix_query_rows_from_batch(&batch, 0, batch.num_rows())?;
            for query in rows {
                if !query_ids.insert(query.feature_row_id) {
                    return Err(invalid("V36 prefix exact truth query membership differs"));
                }
                states.push(V36PrefixGtQueryState {
                    heap: BinaryHeap::with_capacity(GT_NEIGHBORS),
                    query,
                    role,
                });
            }
            Ok(())
        })?;
    }
    let quality_queries = u32::try_from(states.len())
        .map_err(|_| invalid("V36 prefix exact truth query count overflows"))?;
    let expected_quality_queries = jobs.iter().try_fold(0_u32, |total, job| {
        total
            .checked_add(job.expected_queries)
            .ok_or_else(|| invalid("V36 prefix exact truth query count overflows"))
    })?;
    if quality_queries != expected_quality_queries {
        return Err(invalid("V36 prefix exact truth query count differs"));
    }
    let pool = ThreadPoolBuilder::new()
        .num_threads(worker_threads)
        .build()
        .map_err(|_| invalid("V36 prefix exact truth worker pool differs"))?;
    let mut source_rows = 0_u64;
    scan_v36_prefix_source_parquet(source_path, expected_source_feature_ids, |source_batch| {
        let rows = v36_prefix_source_rows_from_batch(&source_batch, &mut source_rows)?;
        if rows
            .iter()
            .any(|row| query_ids.contains(&row.feature_row_id))
        {
            return Err(invalid("V36 prefix exact truth corpus contains a query"));
        }
        pool.install(|| {
            states.par_iter_mut().for_each(|state| {
                for row in &rows {
                    let candidate = RankedNeighbor {
                        distance: squared_l2(&row.embedding, &state.query.embedding),
                        feature_row_id: row.feature_row_id,
                    };
                    if state.heap.len() < GT_NEIGHBORS {
                        state.heap.push(candidate);
                    } else if state.heap.peek().is_some_and(|worst| candidate < *worst) {
                        state.heap.pop();
                        state.heap.push(candidate);
                    }
                }
            });
        });
        Ok(())
    })?;
    if source_rows < GT_NEIGHBORS as u64
        || states.iter().any(|state| state.heap.len() != GT_NEIGHBORS)
    {
        return Err(invalid("V36 prefix exact truth corpus is insufficient"));
    }
    let mut truth_by_role = [Vec::new(), Vec::new(), Vec::new()];
    for state in states {
        let mut neighbors = state.heap.into_vec();
        neighbors.sort();
        truth_by_role[state.role].extend(neighbors.into_iter().enumerate().map(
            |(rank, neighbor)| V36PrefixGtNeighbor {
                query_ordinal: state.query.query_ordinal,
                rank: u16::try_from(rank).unwrap(),
                feature_row_id: neighbor.feature_row_id,
                squared_distance: neighbor.distance,
            },
        ));
    }
    for (job, truth) in jobs.iter().zip(truth_by_role) {
        write_v36_prefix_gt100_parquet(&job.output, [v36_prefix_gt_batch(&truth)?])?;
    }
    Ok(V36PrefixGtRunStats {
        quality_queries,
        source_scans: 1,
        source_rows,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        io::{BufWriter, Write},
        rc::Rc,
        sync::{
            Arc as StdArc,
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
        },
    };

    use axum::{Router, body::Body, http::Response, routing::get};

    use super::*;

    struct CountingWriter {
        calls: Rc<Cell<usize>>,
    }

    impl Write for CountingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.calls.set(self.calls.get() + 1);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn v36_prefix_dataset_spool_writer_coalesces_record_writes() {
        let calls = Rc::new(Cell::new(0));
        let mut writer = BufWriter::with_capacity(
            8 * 1024,
            CountingWriter {
                calls: Rc::clone(&calls),
            },
        );
        let row = V36PrefixMaterializedRow {
            feature_row_id: 7,
            source_ordinal: Some(0),
            embedding: vec![1.0; DIMENSIONS],
        };
        for ordinal in 0..32 {
            write_materialization_record(
                &mut writer,
                Path::new("counting-spool.bin"),
                ordinal,
                &row,
            )
            .unwrap();
        }
        writer.flush().unwrap();
        assert!(calls.get() <= 16, "underlying writes={}", calls.get());
    }

    #[test]
    fn v36_prefix_dataset_acquires_one_complete_authenticated_object() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("registered.bin");
        fs::write(&source, b"registered object bytes").unwrap();
        let bytes = fs::read(&source).unwrap();
        let object = V36PrefixRankedSourceObject {
            encoded_bytes: bytes.len().try_into().unwrap(),
            path: "data/registered.parquet".into(),
            sample_sha256: "1".repeat(64),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            uri: url::Url::from_file_path(&source).unwrap().to_string(),
        };
        let scratch = directory.path().join("scratch");
        fs::create_dir(&scratch).unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let acquired = acquire_v36_prefix_object(&runtime, &object, &scratch, 0).unwrap();
        assert_eq!(fs::read(acquired).unwrap(), bytes);

        let bad_scratch = directory.path().join("bad-scratch");
        fs::create_dir(&bad_scratch).unwrap();
        let mut drifted = object;
        drifted.sha256 = "f".repeat(64);
        assert!(acquire_v36_prefix_object(&runtime, &drifted, &bad_scratch, 0).is_err());
        assert!(bad_scratch.read_dir().unwrap().next().is_none());
    }

    #[test]
    fn v36_prefix_dataset_http_failure_never_resumes_a_partial_object() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let calls = StdArc::new(AtomicUsize::new(0));
        let handler_calls = StdArc::clone(&calls);
        let app = Router::new().route(
            "/object",
            get(move || {
                handler_calls.fetch_add(1, AtomicOrdering::SeqCst);
                async {
                    Response::builder()
                        .header("content-length", "64")
                        .header("etag", "\"registered-etag\"")
                        .header("last-modified", "Sun, 06 Sep 2026 00:00:00 GMT")
                        .body(Body::from("partial"))
                        .unwrap()
                }
            }),
        );
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = runtime.spawn(async move { axum::serve(listener, app).await.unwrap() });
        let directory = tempfile::tempdir().unwrap();
        let object = V36PrefixRankedSourceObject {
            encoded_bytes: 64,
            path: "data/registered.parquet".into(),
            sample_sha256: "1".repeat(64),
            sha256: "2".repeat(64),
            uri: format!("http://{address}/object"),
        };
        assert!(acquire_v36_prefix_object(&runtime, &object, directory.path(), 0).is_err());
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);
        assert!(directory.path().read_dir().unwrap().next().is_none());
        server.abort();
    }

    #[test]
    fn v36_prefix_dataset_checked_cleanup_cannot_report_false_success() {
        let directory = tempfile::tempdir().unwrap();
        let acquired = directory.path().join("source-0000.parquet");
        fs::write(&acquired, b"complete").unwrap();
        V36PrefixAcquiredObjects {
            paths: vec![acquired],
        }
        .cleanup()
        .unwrap();
        assert!(directory.path().read_dir().unwrap().next().is_none());

        let not_a_file = directory.path().join("source-0001.parquet");
        fs::create_dir(&not_a_file).unwrap();
        assert!(
            V36PrefixAcquiredObjects {
                paths: vec![not_a_file],
            }
            .cleanup()
            .is_err()
        );
    }
}
