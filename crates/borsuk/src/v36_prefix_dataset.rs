use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{ArrowSchemaConverter, ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
    schema::types::SchemaDescriptor,
};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result, V36ArtifactIdentity, V36PrefixFreezeAuthority,
    V36PrefixFreezeExecutionAuthority, V36PrefixPopulationAuthority,
    V36PrefixRegisteredSourceObject, V36PrefixRoleAuthority,
    canonical_v36_prefix_freeze_authority_bytes,
    canonical_v36_prefix_freeze_execution_authority_bytes, validate_v36_prefix_freeze_authority,
    validate_v36_prefix_freeze_execution_authority, validate_v36_prefix_population_authority,
};

const DIMENSIONS: usize = 768;
const GT_NEIGHBORS: usize = 100;
const DISTINCT_CANDIDATES: usize = 1_100_000;
const CORPUS_ROWS: usize = 1_000_000;
const PARQUET_ROW_GROUP_ROWS: usize = 8_192;

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
    let mut sample = Sha256::new();
    sample.update(b"borsuk-v36-screen-object-v1");
    sample.update(object.path.as_bytes());
    sample.update(object.encoded_bytes.to_le_bytes());
    if object.path.is_empty()
        || object.uri.is_empty()
        || selected_object_ordinal >= 16
        || object.sample_sha256 != format!("{:x}", sample.finalize())
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

/// Scan authenticated complete objects through a distinct-ID cutoff.
pub fn scan_v36_prefix_object_prefix<F>(
    ranked_objects: &[V36PrefixRankedSourceObject],
    object_cap: usize,
    byte_cap: u64,
    distinct_candidates: usize,
    mut acquire: F,
) -> Result<V36PrefixObjectPrefixScan>
where
    F: FnMut(usize, &V36PrefixRankedSourceObject) -> Result<PathBuf>,
{
    if object_cap == 0
        || object_cap > 16
        || byte_cap == 0
        || distinct_candidates == 0
        || ranked_objects.is_empty()
    {
        return Err(invalid("V36 prefix source scan limits differ"));
    }
    let mut consumed_objects = Vec::new();
    let mut seen = HashSet::with_capacity(distinct_candidates);
    let mut unique_rows = Vec::with_capacity(distinct_candidates);
    let mut physical_rows = 0_u64;
    let mut encoded_bytes = 0_u64;
    let mut cutoff = None;
    for (ordinal, object) in ranked_objects.iter().take(object_cap).enumerate() {
        encoded_bytes = encoded_bytes
            .checked_add(object.encoded_bytes)
            .ok_or_else(|| invalid("V36 prefix source scan bytes overflow"))?;
        if encoded_bytes > byte_cap {
            break;
        }
        let path = acquire(ordinal, object)?;
        let selected_object_ordinal = u16::try_from(ordinal)
            .map_err(|_| invalid("V36 prefix source object ordinal overflows"))?;
        let object_rows = scan_v36_prefix_registered_input_parquet(
            &path,
            object,
            selected_object_ordinal,
            |row| {
                physical_rows = physical_rows
                    .checked_add(1)
                    .ok_or_else(|| invalid("V36 prefix physical rows overflow"))?;
                let identity = validate_v36_prefix_input_row(&row)?;
                if seen.insert(identity.feature_row_id) && unique_rows.len() < distinct_candidates {
                    unique_rows.push(identity);
                    if unique_rows.len() == distinct_candidates {
                        cutoff = Some((selected_object_ordinal, row.row_offset));
                    }
                }
                Ok(())
            },
        )?;
        if object_rows == 0 {
            return Err(invalid("V36 prefix source object is empty"));
        }
        consumed_objects.push(crate::V36PrefixSourceObject {
            blake3: blake3_file(&path)?,
            encoded_bytes: object.encoded_bytes,
            path: object.path.clone(),
            sample_sha256: object.sample_sha256.clone(),
            sha256: object.sha256.clone(),
            uri: object.uri.clone(),
        });
        if cutoff.is_some() {
            break;
        }
    }
    let (cutoff_object_ordinal, cutoff_row_offset) =
        cutoff.ok_or_else(|| invalid("V36 prefix source is insufficient"))?;
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
            role_files.push(file);
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
                    file.write_all(&(ordinal as u64).to_le_bytes())
                        .and_then(|_| file.write_all(&feature_row_id.to_le_bytes()))
                        .map_err(|source| BorsukError::Io {
                            path: spool_paths[role][ordinal / MATERIALIZATION_BUCKET_ROWS].clone(),
                            source,
                        })?;
                    for value in row.embedding {
                        file.write_all(&value.to_bits().to_le_bytes())
                            .map_err(|source| BorsukError::Io {
                                path: spool_paths[role][ordinal / MATERIALIZATION_BUCKET_ROWS]
                                    .clone(),
                                source,
                            })?;
                    }
                }
                Ok(())
            },
        )?;
    }
    if !destinations.is_empty() {
        return Err(invalid("V36 prefix materialization row is missing"));
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
        if queries.is_empty()
            || queries.iter().enumerate().any(|(ordinal, row)| {
                row.query_ordinal != u32::try_from(ordinal).unwrap()
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
