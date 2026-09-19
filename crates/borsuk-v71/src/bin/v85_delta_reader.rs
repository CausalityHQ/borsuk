//! Authenticated local V85 delta runner over cross-language artifacts.

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fmt, fs,
    io::{Cursor, Write},
    path::PathBuf,
    sync::Arc,
};

use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array, UInt8Array, UInt64Array};
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_schema::{DataType, Field, Schema};
use futures_util::{StreamExt, TryStreamExt, stream};
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use sha2::{Digest, Sha256};
use url::Url;

type ReaderResult<T> = Result<T, ReaderError>;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReaderError(String);

impl ReaderError {
    fn authority(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ReaderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ReaderError {}

/// One exact local payload and its immutable object-store identity.
#[derive(Clone, Debug)]
struct LocalArtifactIdentity {
    role: String,
    path: PathBuf,
    uri: String,
    sha256: String,
    bytes: u64,
}

/// Explicit authority plus every immutable run required by one V85 screen.
#[derive(Clone, Debug)]
struct LocalArtifactRequest {
    generation: LocalArtifactIdentity,
    router: LocalArtifactIdentity,
    mutations: LocalArtifactIdentity,
    runs: Vec<LocalArtifactIdentity>,
    queries: LocalArtifactIdentity,
    truth: LocalArtifactIdentity,
    page_budget: usize,
}

#[derive(Clone, Debug)]
struct RemoteArtifactIdentity {
    uri: String,
    sha256: String,
    bytes: u64,
}

#[derive(Clone, Debug)]
struct RemoteArtifactRequest {
    generation: LocalArtifactIdentity,
    router: LocalArtifactIdentity,
    mutations: LocalArtifactIdentity,
    runs: Vec<RemoteArtifactIdentity>,
    queries: LocalArtifactIdentity,
    truth: LocalArtifactIdentity,
    page_budget: usize,
    range_concurrency: usize,
}

#[derive(Clone, Debug)]
enum ExecutionRequest {
    Local(LocalArtifactRequest),
    Remote(RemoteArtifactRequest),
}

impl LocalArtifactRequest {
    fn identities(&self) -> Vec<&LocalArtifactIdentity> {
        let mut identities = Vec::with_capacity(5 + self.runs.len());
        identities.extend([&self.generation, &self.router, &self.mutations]);
        identities.extend(self.runs.iter());
        identities.extend([&self.queries, &self.truth]);
        identities
    }

    #[cfg(test)]
    fn identity_mut(&mut self, index: usize) -> &mut LocalArtifactIdentity {
        match index {
            0 => &mut self.generation,
            1 => &mut self.router,
            2 => &mut self.mutations,
            index if index < 3 + self.runs.len() => &mut self.runs[index - 3],
            index if index == 3 + self.runs.len() => &mut self.queries,
            index if index == 4 + self.runs.len() => &mut self.truth,
            _ => panic!("artifact identity index differs"),
        }
    }
}

fn authenticate_local_artifacts(request: &LocalArtifactRequest) -> ReaderResult<Vec<Vec<u8>>> {
    if request.runs.is_empty() {
        return Err(ReaderError::authority("artifact runs are empty"));
    }
    let identities = request.identities();
    let mut bodies = Vec::with_capacity(identities.len());
    let mut uris = std::collections::BTreeSet::new();
    for (index, identity) in identities.into_iter().enumerate() {
        let expected_role = match index {
            0 => "generation",
            1 => "router",
            2 => "mutations",
            index if index < 3 + request.runs.len() => "run",
            index if index == 3 + request.runs.len() => "queries",
            _ => "truth",
        };
        if identity.role != expected_role
            || !identity.uri.starts_with("s3://")
            || !uris.insert(identity.uri.as_str())
            || identity.sha256.len() != 64
            || !identity
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || identity.bytes == 0
        {
            return Err(ReaderError::authority("artifact identity differs"));
        }
        let body = fs::read(&identity.path)
            .map_err(|error| ReaderError::authority(format!("artifact read failed: {error}")))?;
        if u64::try_from(body.len()).ok() != Some(identity.bytes)
            || format!("{:x}", Sha256::digest(&body)) != identity.sha256
        {
            return Err(ReaderError::authority("artifact payload identity differs"));
        }
        bodies.push(body);
    }
    Ok(bodies)
}

fn authenticate_local_identities(
    identities: &[&LocalArtifactIdentity],
) -> ReaderResult<Vec<Vec<u8>>> {
    let mut bodies = Vec::with_capacity(identities.len());
    let mut uris = std::collections::BTreeSet::new();
    for identity in identities {
        if !identity.uri.starts_with("s3://")
            || !uris.insert(identity.uri.as_str())
            || identity.sha256.len() != 64
            || !identity
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || identity.bytes == 0
        {
            return Err(ReaderError::authority("artifact identity differs"));
        }
        let body = fs::read(&identity.path)
            .map_err(|error| ReaderError::authority(format!("artifact read failed: {error}")))?;
        if u64::try_from(body.len()).ok() != Some(identity.bytes)
            || format!("{:x}", Sha256::digest(&body)) != identity.sha256
        {
            return Err(ReaderError::authority("artifact payload identity differs"));
        }
        bodies.push(body);
    }
    Ok(bodies)
}

#[derive(Clone, Debug)]
struct DecodedRow {
    id: i64,
    sequence: u64,
    vector: Vec<f32>,
    page: u32,
    run_id: u32,
    row: u32,
    bytes: u64,
}

fn page_schema(dimensions: i32) -> Schema {
    let child = Arc::new(Field::new("element", DataType::Float32, false));
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("state", DataType::UInt8, false),
        Field::new("vector", DataType::FixedSizeList(child, dimensions), false),
    ])
}

async fn read_planned_page_streams(
    store: &dyn ObjectStore,
    reads: &[borsuk_v71::delta::PageRead],
    dimensions: i32,
    range_concurrency: usize,
) -> ReaderResult<Vec<DecodedRow>> {
    if dimensions <= 0 || range_concurrency == 0 {
        return Err(ReaderError::authority("page read shape differs"));
    }
    let expected_schema = page_schema(dimensions);
    let mut decoded = Vec::new();
    let mut bodies = stream::iter(reads.iter().cloned())
        .map(|read| async move {
            let url = Url::parse(&read.uri)
                .map_err(|error| ReaderError::authority(format!("page URI differs: {error}")))?;
            let path = ObjectPath::from(url.path().trim_start_matches('/'));
            let end = read
                .offset
                .checked_add(read.bytes)
                .ok_or_else(|| ReaderError::authority("page range overflows"))?;
            let body = store
                .get_opts(
                    &path,
                    GetOptions {
                        range: Some(GetRange::Bounded(read.offset..end)),
                        ..GetOptions::default()
                    },
                )
                .await
                .map_err(|error| ReaderError::authority(format!("page range GET failed: {error}")))?
                .bytes()
                .await
                .map_err(|error| ReaderError::authority(format!("page body failed: {error}")))?;
            Ok::<_, ReaderError>((read, body))
        })
        .buffer_unordered(range_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    bodies.sort_by_key(|(read, _)| (read.run_id, read.page, read.offset));
    for (read, body) in bodies {
        let expected_bytes = usize::try_from(read.bytes)
            .map_err(|_| ReaderError::authority("page length is not addressable"))?;
        if body.len() != expected_bytes {
            return Err(ReaderError::authority("page range length differs"));
        }
        let mut reader = StreamReader::try_new(Cursor::new(body), None)
            .map_err(|error| ReaderError::authority(format!("page IPC differs: {error}")))?;
        if reader.schema().as_ref() != &expected_schema {
            return Err(ReaderError::authority("page Arrow schema differs"));
        }
        let mut page_rows = 0u32;
        for batch in &mut reader {
            let batch = batch
                .map_err(|error| ReaderError::authority(format!("page batch differs: {error}")))?;
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ReaderError::authority("page ID type differs"))?;
            let sequences = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ReaderError::authority("page sequence type differs"))?;
            let states = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| ReaderError::authority("page state type differs"))?;
            let vectors = batch
                .column(3)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| ReaderError::authority("page vector type differs"))?;
            let values = vectors
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| ReaderError::authority("page vector value type differs"))?;
            if batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
                || values.null_count() != 0
            {
                return Err(ReaderError::authority("page contains nulls"));
            }
            for row in 0..batch.num_rows() {
                let begin = row * usize::try_from(dimensions).unwrap();
                let vector =
                    values.values()[begin..begin + usize::try_from(dimensions).unwrap()].to_vec();
                if !vector.iter().all(|value| value.is_finite()) || states.value(row) != 0 {
                    return Err(ReaderError::authority("page row authority differs"));
                }
                decoded.push(DecodedRow {
                    id: ids.value(row),
                    sequence: sequences.value(row),
                    vector,
                    page: read.page,
                    run_id: read.run_id,
                    row: read
                        .row_offset
                        .checked_add(u32::try_from(row).map_err(|_| {
                            ReaderError::authority("page row ordinal is not addressable")
                        })?)
                        .ok_or_else(|| ReaderError::authority("page row ordinal overflows"))?,
                    bytes: if row == 0 { read.bytes } else { 0 },
                });
            }
            page_rows = page_rows
                .checked_add(u32::try_from(batch.num_rows()).map_err(|_| {
                    ReaderError::authority("page batch row count is not addressable")
                })?)
                .ok_or_else(|| ReaderError::authority("page row count overflows"))?;
        }
        if page_rows != read.rows {
            return Err(ReaderError::authority("page row count differs"));
        }
    }
    Ok(decoded)
}

#[derive(Clone, Debug)]
struct QuerySample {
    query: u32,
    hits: u32,
    neighbors: u32,
    requests: u32,
    bytes: u64,
    latency_ns: u64,
    result_ids: Vec<i64>,
}

#[derive(Serialize)]
struct CanonicalSample {
    bytes: u64,
    hits: u32,
    latency_ns: u64,
    neighbors: u32,
    query: u32,
    recall_ppm: u64,
    requests: u32,
    result_ids: Vec<i64>,
}

#[derive(Serialize)]
struct CanonicalResult {
    aggregate_recall_ppm: u64,
    generation: u64,
    samples: Vec<CanonicalSample>,
    total_bytes: u64,
    total_requests: u64,
    worst_recall_ppm: u64,
}

fn canonical_result_bytes(samples: &[QuerySample], generation: u64) -> ReaderResult<Vec<u8>> {
    if generation == 0 || samples.is_empty() {
        return Err(ReaderError::authority("result authority differs"));
    }
    let mut canonical = Vec::with_capacity(samples.len());
    let mut total_hits = 0u64;
    let mut total_neighbors = 0u64;
    let mut total_requests = 0u64;
    let mut total_bytes = 0u64;
    let mut worst = u64::MAX;
    for (index, sample) in samples.iter().enumerate() {
        if usize::try_from(sample.query).ok() != Some(index)
            || sample.neighbors == 0
            || sample.hits > sample.neighbors
            || sample.result_ids.len() != usize::try_from(sample.neighbors).unwrap_or(usize::MAX)
            || sample
                .result_ids
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != sample.result_ids.len()
            || sample.requests == 0
            || sample.latency_ns == 0
        {
            return Err(ReaderError::authority("query sample differs"));
        }
        let recall = u64::from(sample.hits) * 1_000_000 / u64::from(sample.neighbors);
        total_hits += u64::from(sample.hits);
        total_neighbors += u64::from(sample.neighbors);
        total_requests += u64::from(sample.requests);
        total_bytes = total_bytes
            .checked_add(sample.bytes)
            .ok_or_else(|| ReaderError::authority("result bytes overflow"))?;
        worst = worst.min(recall);
        canonical.push(CanonicalSample {
            bytes: sample.bytes,
            hits: sample.hits,
            latency_ns: sample.latency_ns,
            neighbors: sample.neighbors,
            query: sample.query,
            recall_ppm: recall,
            requests: sample.requests,
            result_ids: sample.result_ids.clone(),
        });
    }
    let result = CanonicalResult {
        aggregate_recall_ppm: total_hits * 1_000_000 / total_neighbors,
        generation,
        samples: canonical,
        total_bytes,
        total_requests,
        worst_recall_ppm: worst,
    };
    let mut bytes = serde_json::to_vec(&result)
        .map_err(|error| ReaderError::authority(format!("result serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn remote_artifact_matches(expected: (&str, &str, u64), actual: &RemoteArtifactIdentity) -> bool {
    expected == (actual.uri.as_str(), actual.sha256.as_str(), actual.bytes)
}

fn object_path(uri: &str) -> ReaderResult<ObjectPath> {
    let url = Url::parse(uri)
        .map_err(|error| ReaderError::authority(format!("object URI differs: {error}")))?;
    Ok(ObjectPath::from(url.path().trim_start_matches('/')))
}

fn router_schema(dimensions: i32) -> Schema {
    Schema::new(vec![
        Field::new("cell", DataType::UInt32, false),
        Field::new(
            "centroid",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                dimensions,
            ),
            false,
        ),
        Field::new("first_page", DataType::UInt32, false),
        Field::new("page_count", DataType::UInt32, false),
    ])
}

fn read_router(body: &[u8], dimensions: i32) -> ReaderResult<Vec<(Vec<f32>, u32, u32)>> {
    let mut reader = FileReader::try_new(Cursor::new(body), None)
        .map_err(|error| ReaderError::authority(format!("router IPC differs: {error}")))?;
    if reader.schema().as_ref() != &router_schema(dimensions) {
        return Err(ReaderError::authority("router Arrow schema differs"));
    }
    let mut rows = Vec::new();
    for batch in &mut reader {
        let batch = batch
            .map_err(|error| ReaderError::authority(format!("router batch differs: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(ReaderError::authority("router contains nulls"));
        }
        let cells = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .ok_or_else(|| ReaderError::authority("router cell type differs"))?;
        let centroids = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| ReaderError::authority("router centroid type differs"))?;
        let values = centroids
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| ReaderError::authority("router centroid value type differs"))?;
        let first_pages = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .ok_or_else(|| ReaderError::authority("router first-page type differs"))?;
        let page_counts = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .ok_or_else(|| ReaderError::authority("router page-count type differs"))?;
        let width = usize::try_from(dimensions)
            .map_err(|_| ReaderError::authority("router dimensions differ"))?;
        for row in 0..batch.num_rows() {
            if usize::try_from(cells.value(row)).ok() != Some(rows.len()) {
                return Err(ReaderError::authority("router cells are not ordered"));
            }
            let centroid = values.values()[row * width..(row + 1) * width].to_vec();
            if !centroid.iter().all(|value| value.is_finite()) || page_counts.value(row) == 0 {
                return Err(ReaderError::authority("router row authority differs"));
            }
            rows.push((centroid, first_pages.value(row), page_counts.value(row)));
        }
    }
    if rows.is_empty() {
        return Err(ReaderError::authority("router is empty"));
    }
    Ok(rows)
}

fn mutation_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("state", DataType::UInt8, false),
        Field::new("run_id", DataType::UInt32, true),
        Field::new("row", DataType::UInt32, true),
    ])
}

fn read_mutations(body: &[u8]) -> ReaderResult<borsuk_v71::delta::MutationDirectory> {
    use borsuk_v71::delta::{MutationDirectory, MutationRecord, MutationState};

    let mut reader = FileReader::try_new(Cursor::new(body), None)
        .map_err(|error| ReaderError::authority(format!("mutation IPC differs: {error}")))?;
    if reader.schema().as_ref() != &mutation_schema() {
        return Err(ReaderError::authority("mutation Arrow schema differs"));
    }
    let mut records = Vec::new();
    let mut previous_id = None;
    for batch in &mut reader {
        let batch = batch
            .map_err(|error| ReaderError::authority(format!("mutation batch differs: {error}")))?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| ReaderError::authority("mutation ID type differs"))?;
        let sequences = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| ReaderError::authority("mutation sequence type differs"))?;
        let states = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| ReaderError::authority("mutation state type differs"))?;
        let run_ids = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .ok_or_else(|| ReaderError::authority("mutation run type differs"))?;
        let rows = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .ok_or_else(|| ReaderError::authority("mutation row type differs"))?;
        if ids.null_count() != 0 || sequences.null_count() != 0 || states.null_count() != 0 {
            return Err(ReaderError::authority("mutation required field is null"));
        }
        for index in 0..batch.num_rows() {
            let id = ids.value(index);
            if previous_id.is_some_and(|prior| id <= prior) {
                return Err(ReaderError::authority(
                    "mutation IDs are not strictly ordered",
                ));
            }
            previous_id = Some(id);
            let state = match states.value(index) {
                0 if !run_ids.is_null(index) && !rows.is_null(index) => MutationState::Live {
                    run_id: run_ids.value(index),
                    row: rows.value(index),
                },
                1 if run_ids.is_null(index) && rows.is_null(index) => MutationState::Tombstone,
                _ => return Err(ReaderError::authority("mutation state differs")),
            };
            records.push(MutationRecord {
                id,
                sequence: sequences.value(index),
                state,
            });
        }
    }
    MutationDirectory::try_from_entries(records)
        .map_err(|error| ReaderError::authority(format!("mutation directory differs: {error}")))
}

fn read_parquet_batches(body: &[u8]) -> ReaderResult<(Arc<Schema>, Vec<arrow_array::RecordBatch>)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(body))
        .map_err(|error| ReaderError::authority(format!("Parquet metadata differs: {error}")))?;
    let schema = builder.schema().clone();
    let reader = builder
        .build()
        .map_err(|error| ReaderError::authority(format!("Parquet reader failed: {error}")))?;
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ReaderError::authority(format!("Parquet batch differs: {error}")))?;
    Ok((schema, batches))
}

fn read_queries(body: &[u8], dimensions: i32) -> ReaderResult<Vec<Vec<f32>>> {
    let expected = Arc::new(Schema::new(vec![
        Field::new("query", DataType::UInt32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                dimensions,
            ),
            false,
        ),
    ]));
    let (schema, batches) = read_parquet_batches(body)?;
    if schema != expected {
        return Err(ReaderError::authority("query Parquet schema differs"));
    }
    let width = usize::try_from(dimensions)
        .map_err(|_| ReaderError::authority("query dimensions differ"))?;
    let mut queries = Vec::new();
    for batch in batches {
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .ok_or_else(|| ReaderError::authority("query ordinal type differs"))?;
        let vectors = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| ReaderError::authority("query vector type differs"))?;
        let values = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| ReaderError::authority("query value type differs"))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
            || values.null_count() != 0
        {
            return Err(ReaderError::authority("query contains nulls"));
        }
        for row in 0..batch.num_rows() {
            if usize::try_from(ordinals.value(row)).ok() != Some(queries.len()) {
                return Err(ReaderError::authority("query ordinals differ"));
            }
            let vector = values.values()[row * width..(row + 1) * width].to_vec();
            if !vector.iter().all(|value| value.is_finite()) {
                return Err(ReaderError::authority("query vector is non-finite"));
            }
            queries.push(vector);
        }
    }
    if queries.is_empty() {
        return Err(ReaderError::authority("queries are empty"));
    }
    Ok(queries)
}

fn read_truth(body: &[u8], neighbors: i32) -> ReaderResult<Vec<Vec<i64>>> {
    let expected = Arc::new(Schema::new(vec![
        Field::new("query", DataType::UInt32, false),
        Field::new(
            "neighbors",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Int64, false)),
                neighbors,
            ),
            false,
        ),
    ]));
    let (schema, batches) = read_parquet_batches(body)?;
    if schema != expected {
        return Err(ReaderError::authority("truth Parquet schema differs"));
    }
    let width =
        usize::try_from(neighbors).map_err(|_| ReaderError::authority("truth width differs"))?;
    let mut truth = Vec::new();
    for batch in batches {
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .ok_or_else(|| ReaderError::authority("truth ordinal type differs"))?;
        let lists = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| ReaderError::authority("truth neighbors type differs"))?;
        let values = lists
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| ReaderError::authority("truth neighbor value type differs"))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
            || values.null_count() != 0
        {
            return Err(ReaderError::authority("truth contains nulls"));
        }
        for row in 0..batch.num_rows() {
            if usize::try_from(ordinals.value(row)).ok() != Some(truth.len()) {
                return Err(ReaderError::authority("truth ordinals differ"));
            }
            truth.push(values.values()[row * width..(row + 1) * width].to_vec());
        }
    }
    Ok(truth)
}

fn squared_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}

async fn execute_with_store(
    authority_bodies: &[Vec<u8>],
    router_authority: &LocalArtifactIdentity,
    mutation_authority: &LocalArtifactIdentity,
    runs: &[RemoteArtifactIdentity],
    page_budget: usize,
    range_concurrency: usize,
    store: &dyn ObjectStore,
) -> ReaderResult<Vec<u8>> {
    use borsuk_v71::delta::{Candidate, GenerationManifest, merge_candidates, plan_page_reads};

    if page_budget == 0 || range_concurrency == 0 || authority_bodies.len() != 5 {
        return Err(ReaderError::authority("execution shape differs"));
    }
    let generation = GenerationManifest::from_canonical_bytes(&authority_bodies[0])
        .map_err(|error| ReaderError::authority(format!("generation differs: {error}")))?;
    if !remote_artifact_matches(
        generation.router_identity(),
        &RemoteArtifactIdentity {
            uri: router_authority.uri.clone(),
            sha256: router_authority.sha256.clone(),
            bytes: router_authority.bytes,
        },
    ) || !remote_artifact_matches(
        generation.mutation_directory_identity(),
        &RemoteArtifactIdentity {
            uri: mutation_authority.uri.clone(),
            sha256: mutation_authority.sha256.clone(),
            bytes: mutation_authority.bytes,
        },
    ) || generation.runs().len() != runs.len()
    {
        return Err(ReaderError::authority("generation object binding differs"));
    }
    for run in generation.runs() {
        let matches = runs
            .iter()
            .filter(|identity| remote_artifact_matches(run.object_identity(), identity))
            .count();
        if matches != 1 {
            return Err(ReaderError::authority("generation run binding differs"));
        }
    }
    let dimensions = i32::try_from(generation.dimensions())
        .map_err(|_| ReaderError::authority("generation dimensions are not addressable"))?;
    let router = read_router(&authority_bodies[1], dimensions)?;
    let mutations = read_mutations(&authority_bodies[2])?;
    let queries = read_queries(&authority_bodies[3], dimensions)?;
    let truth = read_truth(
        &authority_bodies[4],
        i32::try_from(generation.neighbors())
            .map_err(|_| ReaderError::authority("neighbor count is not addressable"))?,
    )?;
    if queries.len() != truth.len() {
        return Err(ReaderError::authority("query and truth counts differ"));
    }
    let run_kinds = generation
        .runs()
        .iter()
        .map(|run| (run.run_id(), run.kind()))
        .collect::<BTreeMap<_, _>>();
    let mut samples = Vec::with_capacity(queries.len());
    for (query_index, query) in queries.iter().enumerate() {
        let started = std::time::Instant::now();
        let mut scored_pages = Vec::<(f32, u32)>::new();
        for (centroid, first_page, page_count) in &router {
            let score = squared_distance(query, centroid);
            for offset in 0..*page_count {
                scored_pages.push((
                    score,
                    first_page
                        .checked_add(offset)
                        .ok_or_else(|| ReaderError::authority("router page range overflows"))?,
                ));
            }
        }
        scored_pages.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
        let mut selected = scored_pages
            .into_iter()
            .take(page_budget)
            .map(|(_, page)| page)
            .collect::<Vec<_>>();
        selected.sort_unstable();
        selected.dedup();
        let reads = plan_page_reads(&selected, &generation)
            .map_err(|error| ReaderError::authority(format!("page plan differs: {error}")))?;
        let rows = read_planned_page_streams(store, &reads, dimensions, range_concurrency).await?;
        let decoded_bytes = rows.iter().map(|row| row.bytes).sum::<u64>();
        if decoded_bytes != reads.iter().map(|read| read.bytes).sum::<u64>()
            || rows.iter().any(|row| !selected.contains(&row.page))
        {
            return Err(ReaderError::authority("decoded page evidence differs"));
        }
        let mut base = Vec::new();
        let mut delta = Vec::new();
        for row in rows {
            let candidate = Candidate {
                id: row.id,
                sequence: row.sequence,
                distance: squared_distance(query, &row.vector),
                run_id: row.run_id,
                row: row.row,
            };
            match run_kinds.get(&row.run_id).copied() {
                Some("base") => base.push(candidate),
                Some("delta") => delta.push(candidate),
                _ => return Err(ReaderError::authority("candidate run differs")),
            }
        }
        let merged = merge_candidates(
            base.into_iter(),
            delta.into_iter(),
            &mutations,
            usize::try_from(generation.neighbors()).unwrap(),
        )
        .map_err(|error| ReaderError::authority(format!("candidate merge differs: {error}")))?;
        let expected = &truth[query_index];
        let hits = merged
            .iter()
            .filter(|candidate| expected.contains(&candidate.id))
            .count();
        samples.push(QuerySample {
            query: u32::try_from(query_index)
                .map_err(|_| ReaderError::authority("query index is not addressable"))?,
            hits: u32::try_from(hits)
                .map_err(|_| ReaderError::authority("hit count is not addressable"))?,
            neighbors: generation.neighbors(),
            requests: u32::try_from(reads.len())
                .map_err(|_| ReaderError::authority("request count is not addressable"))?,
            bytes: reads.iter().map(|read| read.bytes).sum(),
            latency_ns: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            result_ids: merged.iter().map(|candidate| candidate.id).collect(),
        });
    }
    canonical_result_bytes(&samples, generation.generation())
}

async fn execute_local(request: &LocalArtifactRequest) -> ReaderResult<Vec<u8>> {
    let bodies = authenticate_local_artifacts(request)?;
    let queries_index = 3 + request.runs.len();
    let authority_bodies = vec![
        bodies[0].clone(),
        bodies[1].clone(),
        bodies[2].clone(),
        bodies[queries_index].clone(),
        bodies[queries_index + 1].clone(),
    ];
    let store = object_store::memory::InMemory::new();
    let runs = request
        .runs
        .iter()
        .enumerate()
        .map(|(index, run)| {
            let body = &bodies[3 + index];
            (run, body)
        })
        .map(|(run, body)| async {
            store
                .put(
                    &object_path(&run.uri)?,
                    bytes::Bytes::copy_from_slice(body).into(),
                )
                .await
                .map_err(|error| {
                    ReaderError::authority(format!("run local store failed: {error}"))
                })?;
            Ok::<_, ReaderError>(RemoteArtifactIdentity {
                uri: run.uri.clone(),
                sha256: run.sha256.clone(),
                bytes: run.bytes,
            })
        });
    let mut remote_runs = Vec::with_capacity(request.runs.len());
    for run in runs {
        remote_runs.push(run.await?);
    }
    execute_with_store(
        &authority_bodies,
        &request.router,
        &request.mutations,
        &remote_runs,
        request.page_budget,
        1,
        &store,
    )
    .await
}

async fn execute_remote_with_store(
    request: &RemoteArtifactRequest,
    store: &dyn ObjectStore,
) -> ReaderResult<Vec<u8>> {
    if request.runs.is_empty() || request.range_concurrency == 0 {
        return Err(ReaderError::authority("remote execution shape differs"));
    }
    let authority_bodies = authenticate_local_identities(&[
        &request.generation,
        &request.router,
        &request.mutations,
        &request.queries,
        &request.truth,
    ])?;
    for run in &request.runs {
        if !run.uri.starts_with("s3://")
            || run.sha256.len() != 64
            || !run
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || run.bytes == 0
        {
            return Err(ReaderError::authority("remote run identity differs"));
        }
        let metadata = store
            .head(&object_path(&run.uri)?)
            .await
            .map_err(|error| ReaderError::authority(format!("remote run HEAD failed: {error}")))?;
        if metadata.size != run.bytes {
            return Err(ReaderError::authority("remote run length differs"));
        }
    }
    execute_with_store(
        &authority_bodies,
        &request.router,
        &request.mutations,
        &request.runs,
        request.page_budget,
        request.range_concurrency,
        store,
    )
    .await
}

fn parse_args(arguments: Vec<String>) -> ReaderResult<ExecutionRequest> {
    let mut iterator = arguments.into_iter();
    let _program = iterator.next();
    let mut identities = BTreeMap::<String, LocalArtifactIdentity>::new();
    let mut runs = Vec::<LocalArtifactIdentity>::new();
    let mut remote_runs = Vec::<RemoteArtifactIdentity>::new();
    let mut page_budget = None;
    let mut range_concurrency = None;
    while let Some(flag) = iterator.next() {
        if flag == "--page-budget" {
            if page_budget.is_some() {
                return Err(ReaderError::authority("page budget is duplicated"));
            }
            page_budget = Some(
                iterator
                    .next()
                    .ok_or_else(|| ReaderError::authority("page budget is missing"))?
                    .parse::<usize>()
                    .map_err(|_| ReaderError::authority("page budget differs"))?,
            );
            continue;
        }
        if flag == "--range-concurrency" {
            if range_concurrency.is_some() {
                return Err(ReaderError::authority("range concurrency is duplicated"));
            }
            range_concurrency = Some(
                iterator
                    .next()
                    .ok_or_else(|| ReaderError::authority("range concurrency is missing"))?
                    .parse::<usize>()
                    .map_err(|_| ReaderError::authority("range concurrency differs"))?,
            );
            continue;
        }
        let role = flag
            .strip_prefix("--")
            .ok_or_else(|| ReaderError::authority("CLI flag differs"))?;
        if role == "run" {
            let path = PathBuf::from(
                iterator
                    .next()
                    .ok_or_else(|| ReaderError::authority("artifact path is missing"))?,
            );
            let uri = iterator
                .next()
                .ok_or_else(|| ReaderError::authority("artifact URI is missing"))?;
            let sha256 = iterator
                .next()
                .ok_or_else(|| ReaderError::authority("artifact SHA-256 is missing"))?;
            let bytes = iterator
                .next()
                .ok_or_else(|| ReaderError::authority("artifact length is missing"))?
                .parse::<u64>()
                .map_err(|_| ReaderError::authority("artifact length differs"))?;
            runs.push(LocalArtifactIdentity {
                role: role.to_string(),
                path,
                uri,
                sha256,
                bytes,
            });
            continue;
        }
        if role == "run-s3" {
            let uri = iterator
                .next()
                .ok_or_else(|| ReaderError::authority("artifact URI is missing"))?;
            let sha256 = iterator
                .next()
                .ok_or_else(|| ReaderError::authority("artifact SHA-256 is missing"))?;
            let bytes = iterator
                .next()
                .ok_or_else(|| ReaderError::authority("artifact length is missing"))?
                .parse::<u64>()
                .map_err(|_| ReaderError::authority("artifact length differs"))?;
            remote_runs.push(RemoteArtifactIdentity { uri, sha256, bytes });
            continue;
        }
        if !matches!(
            role,
            "generation" | "router" | "mutations" | "queries" | "truth"
        ) || identities.contains_key(role)
        {
            return Err(ReaderError::authority("artifact CLI role differs"));
        }
        let path = PathBuf::from(
            iterator
                .next()
                .ok_or_else(|| ReaderError::authority("artifact path is missing"))?,
        );
        let uri = iterator
            .next()
            .ok_or_else(|| ReaderError::authority("artifact URI is missing"))?;
        let sha256 = iterator
            .next()
            .ok_or_else(|| ReaderError::authority("artifact SHA-256 is missing"))?;
        let bytes = iterator
            .next()
            .ok_or_else(|| ReaderError::authority("artifact length is missing"))?
            .parse::<u64>()
            .map_err(|_| ReaderError::authority("artifact length differs"))?;
        identities.insert(
            role.to_string(),
            LocalArtifactIdentity {
                role: role.to_string(),
                path,
                uri,
                sha256,
                bytes,
            },
        );
    }
    let mut take = |role: &str| {
        identities
            .remove(role)
            .ok_or_else(|| ReaderError::authority(format!("{role} artifact is missing")))
    };
    let generation = take("generation")?;
    let router = take("router")?;
    let mutations = take("mutations")?;
    let queries = take("queries")?;
    let truth = take("truth")?;
    let page_budget =
        page_budget.ok_or_else(|| ReaderError::authority("page budget is missing"))?;
    if !identities.is_empty()
        || page_budget == 0
        || (!runs.is_empty() && !remote_runs.is_empty())
        || (remote_runs.is_empty() && range_concurrency.is_some())
        || (!remote_runs.is_empty() && range_concurrency.is_none())
        || range_concurrency == Some(0)
    {
        return Err(ReaderError::authority("CLI authority differs"));
    }
    if remote_runs.is_empty() {
        Ok(ExecutionRequest::Local(LocalArtifactRequest {
            generation,
            router,
            mutations,
            runs,
            queries,
            truth,
            page_budget,
        }))
    } else {
        Ok(ExecutionRequest::Remote(RemoteArtifactRequest {
            generation,
            router,
            mutations,
            runs: remote_runs,
            queries,
            truth,
            page_budget,
            range_concurrency: range_concurrency.expect("validated"),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let request = parse_args(env::args().collect())?;
    let result = match request {
        ExecutionRequest::Local(request) => execute_local(&request).await?,
        ExecutionRequest::Remote(request) => {
            let first = request
                .runs
                .first()
                .ok_or_else(|| ReaderError::authority("remote runs are empty"))?;
            let store = object_store::aws::AmazonS3Builder::from_env()
                .with_url(&first.uri)
                .build()?;
            execute_remote_with_store(&request, &store).await?
        }
    };
    std::io::stdout().write_all(&result)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use arrow_array::{
        ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, UInt8Array,
        UInt32Array, UInt64Array,
    };
    use arrow_ipc::writer::{FileWriter, StreamWriter};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use parquet::arrow::ArrowWriter;
    use parquet::{basic::Compression, file::properties::WriterProperties};
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    use super::{
        ExecutionRequest, LocalArtifactIdentity, LocalArtifactRequest, QuerySample,
        RemoteArtifactIdentity, RemoteArtifactRequest, authenticate_local_artifacts,
        canonical_result_bytes, execute_local, execute_remote_with_store, parse_args,
        read_planned_page_streams,
    };
    use borsuk_v71::delta::PageRead;

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn identity(root: &TempDir, role: &str, body: &[u8]) -> LocalArtifactIdentity {
        let path = root.path().join(format!("{role}.bin"));
        fs::write(&path, body).unwrap();
        LocalArtifactIdentity {
            role: role.into(),
            path,
            uri: format!("s3://fixture/{role}"),
            sha256: digest(body),
            bytes: u64::try_from(body.len()).unwrap(),
        }
    }

    fn request(root: &TempDir) -> LocalArtifactRequest {
        let mut base_run = identity(root, "base-run", b"base");
        base_run.role = "run".into();
        let mut delta_run = identity(root, "delta-run", b"delta");
        delta_run.role = "run".into();
        LocalArtifactRequest {
            generation: identity(root, "generation", b"generation\n"),
            router: identity(root, "router", b"router"),
            mutations: identity(root, "mutations", b"mutations"),
            runs: vec![base_run, delta_run],
            queries: identity(root, "queries", b"queries"),
            truth: identity(root, "truth", b"truth"),
            page_budget: 2,
        }
    }

    #[test]
    fn local_artifacts_authenticate_all_seven_roles_before_semantic_use() {
        let root = TempDir::new().unwrap();
        let baseline = request(&root);
        let authenticated = authenticate_local_artifacts(&baseline).unwrap();
        assert_eq!(authenticated.len(), 7);

        for role in 0..baseline.identities().len() {
            let mut drift = baseline.clone();
            drift.identity_mut(role).sha256 = "0".repeat(64);
            assert!(authenticate_local_artifacts(&drift).is_err(), "role {role}");
        }
    }

    fn page_stream(
        ids: &[i64],
        sequences: &[u64],
        states: &[u8],
        vectors: &[f32],
        dimensions: i32,
    ) -> Vec<u8> {
        let child = Arc::new(Field::new("element", DataType::Float32, false));
        let vector = FixedSizeListArray::try_new(
            child.clone(),
            dimensions,
            Arc::new(Float32Array::from(vectors.to_vec())),
            None,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("sequence", DataType::UInt64, false),
            Field::new("state", DataType::UInt8, false),
            Field::new("vector", DataType::FixedSizeList(child, dimensions), false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
                Arc::new(UInt64Array::from(sequences.to_vec())),
                Arc::new(UInt8Array::from(states.to_vec())),
                Arc::new(vector),
            ],
        )
        .unwrap();
        let mut body = Vec::new();
        let mut writer = StreamWriter::try_new(&mut body, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        body
    }

    #[tokio::test]
    async fn page_reader_uses_only_registered_ranges_and_rejects_schema_drift() {
        let store = InMemory::new();
        let first = page_stream(&[1], &[1], &[0], &[1.0, 0.0], 2);
        let second = page_stream(&[2], &[2], &[0], &[0.0, 1.0], 2);
        let mut parent = vec![0x55; 13];
        let first_offset = parent.len();
        parent.extend_from_slice(&first);
        parent.extend_from_slice(&[0x66; 11]);
        let second_offset = parent.len();
        parent.extend_from_slice(&second);
        parent.extend_from_slice(&[0x77; 7]);
        store
            .put(&Path::from("runs/base.arrow"), Bytes::from(parent).into())
            .await
            .unwrap();
        let reads = vec![
            PageRead {
                run_id: 0,
                page: 1,
                row_offset: 0,
                uri: "memory:///runs/base.arrow".into(),
                offset: u64::try_from(first_offset).unwrap(),
                bytes: u64::try_from(first.len()).unwrap(),
                rows: 1,
            },
            PageRead {
                run_id: 0,
                page: 3,
                row_offset: 1,
                uri: "memory:///runs/base.arrow".into(),
                offset: u64::try_from(second_offset).unwrap(),
                bytes: u64::try_from(second.len()).unwrap(),
                rows: 1,
            },
        ];
        let decoded = read_planned_page_streams(&store, &reads, 2, 2)
            .await
            .unwrap();
        assert_eq!(decoded.iter().map(|row| row.id).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(
            decoded.iter().map(|row| row.page).collect::<Vec<_>>(),
            [1, 3]
        );
        assert_eq!(
            decoded.iter().map(|row| row.bytes).sum::<u64>(),
            u64::try_from(first.len() + second.len()).unwrap()
        );

        let wrong = page_stream(&[3], &[3], &[0], &[1.0, 2.0, 3.0, 4.0], 4);
        store
            .put(
                &Path::from("runs/wrong.arrow"),
                Bytes::from(wrong.clone()).into(),
            )
            .await
            .unwrap();
        let wrong_read = [PageRead {
            run_id: 1,
            page: 4,
            row_offset: 0,
            uri: "memory:///runs/wrong.arrow".into(),
            offset: 0,
            bytes: u64::try_from(wrong.len()).unwrap(),
            rows: 1,
        }];
        assert!(
            read_planned_page_streams(&store, &wrong_read, 2, 2)
                .await
                .is_err()
        );
    }

    #[test]
    fn canonical_result_recomputes_samples_and_aggregates() {
        let samples = vec![
            QuerySample {
                query: 0,
                hits: 2,
                neighbors: 2,
                requests: 3,
                bytes: 120,
                latency_ns: 11,
                result_ids: vec![1, 2],
            },
            QuerySample {
                query: 1,
                hits: 1,
                neighbors: 2,
                requests: 4,
                bytes: 160,
                latency_ns: 17,
                result_ids: vec![3, 4],
            },
        ];
        let bytes = canonical_result_bytes(&samples, 7).unwrap();
        assert!(bytes.ends_with(b"\n"));
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["samples"][0]["result_ids"], serde_json::json!([1, 2]));
        assert_eq!(value["samples"][1]["result_ids"], serde_json::json!([3, 4]));
        assert_eq!(value["aggregate_recall_ppm"], 750_000);
        assert_eq!(value["worst_recall_ppm"], 500_000);
        assert_eq!(value["total_requests"], 7);
        assert_eq!(value["total_bytes"], 280);
        assert_eq!(value["generation"], 7);
        assert_eq!(
            serde_json::to_vec(&value).unwrap().as_slice(),
            &bytes[..bytes.len() - 1]
        );
    }

    fn ipc_file(batch: &RecordBatch) -> Vec<u8> {
        let mut body = Vec::new();
        let mut writer = FileWriter::try_new(&mut body, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        body
    }

    fn parquet_file(batch: &RecordBatch) -> Vec<u8> {
        let properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer =
            ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties)).unwrap();
        writer.write(batch).unwrap();
        writer.into_inner().unwrap()
    }

    fn list_f32(values: &[f32], dimensions: i32) -> ArrayRef {
        Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(Field::new("element", DataType::Float32, false)),
                dimensions,
                Arc::new(Float32Array::from(values.to_vec())),
                None,
            )
            .unwrap(),
        )
    }

    fn exact_local_request(root: &TempDir) -> LocalArtifactRequest {
        let base_zero = page_stream(&[1, 2], &[1, 1], &[0, 0], &[1.0, 0.0, 0.9, 0.0], 2);
        let base_one = page_stream(&[3, 4], &[1, 1], &[0, 0], &[0.0, 1.0, 0.0, 0.9], 2);
        let mut base = base_zero.clone();
        base.extend_from_slice(&base_one);
        let delta_zero = page_stream(&[5], &[2], &[0], &[0.95, 0.0], 2);
        let delta_one = page_stream(&[6], &[2], &[0], &[0.0, 0.95], 2);

        let router_schema = Arc::new(Schema::new(vec![
            Field::new("cell", DataType::UInt32, false),
            Field::new(
                "centroid",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    2,
                ),
                false,
            ),
            Field::new("first_page", DataType::UInt32, false),
            Field::new("page_count", DataType::UInt32, false),
        ]));
        let router = ipc_file(
            &RecordBatch::try_new(
                router_schema,
                vec![
                    Arc::new(UInt32Array::from(vec![0, 1])),
                    list_f32(&[1.0, 0.0, 0.0, 1.0], 2),
                    Arc::new(UInt32Array::from(vec![0, 1])),
                    Arc::new(UInt32Array::from(vec![1, 1])),
                ],
            )
            .unwrap(),
        );
        let mutation_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("sequence", DataType::UInt64, false),
            Field::new("state", DataType::UInt8, false),
            Field::new("run_id", DataType::UInt32, true),
            Field::new("row", DataType::UInt32, true),
        ]));
        let mutations = ipc_file(
            &RecordBatch::try_new(
                mutation_schema,
                vec![
                    Arc::new(Int64Array::from(vec![5, 6])),
                    Arc::new(UInt64Array::from(vec![2, 2])),
                    Arc::new(UInt8Array::from(vec![0, 0])),
                    Arc::new(UInt32Array::from(vec![1, 2])),
                    Arc::new(UInt32Array::from(vec![0, 0])),
                ],
            )
            .unwrap(),
        );
        let query_schema = Arc::new(Schema::new(vec![
            Field::new("query", DataType::UInt32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    2,
                ),
                false,
            ),
        ]));
        let queries = parquet_file(
            &RecordBatch::try_new(
                query_schema,
                vec![
                    Arc::new(UInt32Array::from(vec![0, 1])),
                    list_f32(&[1.0, 0.0, 0.0, 1.0], 2),
                ],
            )
            .unwrap(),
        );
        let truth_schema = Arc::new(Schema::new(vec![
            Field::new("query", DataType::UInt32, false),
            Field::new(
                "neighbors",
                DataType::FixedSizeList(Arc::new(Field::new("element", DataType::Int64, false)), 2),
                false,
            ),
        ]));
        let truth_neighbors = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Int64, false)),
            2,
            Arc::new(Int64Array::from(vec![1, 5, 3, 6])),
            None,
        )
        .unwrap();
        let truth = parquet_file(
            &RecordBatch::try_new(
                truth_schema,
                vec![
                    Arc::new(UInt32Array::from(vec![0, 1])),
                    Arc::new(truth_neighbors),
                ],
            )
            .unwrap(),
        );

        let router_identity = identity(root, "router", &router);
        let mutations_identity = identity(root, "mutations", &mutations);
        let base_identity = identity(root, "base-run", &base);
        let delta_zero_identity = identity(root, "delta-run-0", &delta_zero);
        let delta_one_identity = identity(root, "delta-run-1", &delta_one);
        let queries_identity = identity(root, "queries", &queries);
        let truth_identity = identity(root, "truth", &truth);
        let object = |artifact: &LocalArtifactIdentity| serde_json::json!({"bytes": artifact.bytes, "sha256": artifact.sha256, "uri": artifact.uri});
        let manifest = serde_json::json!({
            "base_horizon": 4,
            "dimensions": 2,
            "generation": 1,
            "mutation_directory": object(&mutations_identity),
            "neighbors": 2,
            "page_rows": 2,
            "previous_generation_sha256": "0".repeat(64),
            "router": object(&router_identity),
            "runs": [
                {"generation": 0, "kind": "base", "object": object(&base_identity), "pages": [
                    {"bytes": base_zero.len(), "offset": 0, "page": 0, "rows": 2},
                    {"bytes": base_one.len(), "offset": base_zero.len(), "page": 1, "rows": 2}
                ], "run_id": 0},
                {"generation": 1, "kind": "delta", "object": object(&delta_zero_identity), "pages": [
                    {"bytes": delta_zero.len(), "offset": 0, "page": 0, "rows": 1}
                ], "run_id": 1},
                {"generation": 1, "kind": "delta", "object": object(&delta_one_identity), "pages": [
                    {"bytes": delta_one.len(), "offset": 0, "page": 1, "rows": 1}
                ], "run_id": 2}
            ],
            "schema": "borsuk-v85-generation-v1",
            "source_split": "synthetic-4-base-2-delta"
        });
        let mut manifest_body = serde_json::to_vec(&manifest).unwrap();
        manifest_body.push(b'\n');
        let mut base_identity = base_identity;
        base_identity.role = "run".into();
        let mut delta_zero_identity = delta_zero_identity;
        delta_zero_identity.role = "run".into();
        let mut delta_one_identity = delta_one_identity;
        delta_one_identity.role = "run".into();
        LocalArtifactRequest {
            generation: identity(root, "generation", &manifest_body),
            router: router_identity,
            mutations: mutations_identity,
            runs: vec![base_identity, delta_zero_identity, delta_one_identity],
            queries: queries_identity,
            truth: truth_identity,
            page_budget: 2,
        }
    }

    #[test]
    fn remote_cli_requires_explicit_s3_runs_and_bounded_io_concurrency() {
        // Break caught: a purported S3 qualification silently accepts local run
        // paths, mixes local and remote runs, or permits unbounded I/O fan-out.
        let root = TempDir::new().unwrap();
        let local = exact_local_request(&root);
        let mut arguments = vec!["v85_delta_reader".to_string()];
        for (flag, identity) in [
            ("--generation", &local.generation),
            ("--router", &local.router),
            ("--mutations", &local.mutations),
            ("--queries", &local.queries),
            ("--truth", &local.truth),
        ] {
            arguments.extend([
                flag.to_string(),
                identity.path.display().to_string(),
                identity.uri.clone(),
                identity.sha256.clone(),
                identity.bytes.to_string(),
            ]);
        }
        for run in &local.runs {
            arguments.extend([
                "--run-s3".to_string(),
                run.uri.clone(),
                run.sha256.clone(),
                run.bytes.to_string(),
            ]);
        }
        arguments.extend([
            "--page-budget".to_string(),
            "2".to_string(),
            "--range-concurrency".to_string(),
            "8".to_string(),
        ]);

        let parsed = parse_args(arguments.clone()).unwrap();
        let ExecutionRequest::Remote(remote) = parsed else {
            panic!("remote CLI selected a local execution path")
        };
        assert_eq!(remote.range_concurrency, 8);
        assert_eq!(remote.runs.len(), 3);

        let mut mixed = arguments;
        mixed.extend([
            "--run".to_string(),
            local.runs[0].path.display().to_string(),
            local.runs[0].uri.clone(),
            local.runs[0].sha256.clone(),
            local.runs[0].bytes.to_string(),
        ]);
        assert!(parse_args(mixed).is_err());

        let mut unbounded = vec!["v85_delta_reader".to_string()];
        unbounded.extend(["--range-concurrency".to_string(), "0".to_string()]);
        assert!(parse_args(unbounded).is_err());
    }

    #[tokio::test]
    async fn remote_reader_uses_registered_ranges_without_local_run_files() {
        // Break caught: remote execution authenticates by downloading or opening
        // complete run files locally instead of issuing only registered ranges.
        let root = TempDir::new().unwrap();
        let local = exact_local_request(&root);
        let store = InMemory::new();
        let mut runs = Vec::new();
        for run in &local.runs {
            let body = fs::read(&run.path).unwrap();
            let uri = url::Url::parse(&run.uri).unwrap();
            store
                .put(
                    &Path::from(uri.path().trim_start_matches('/')),
                    Bytes::from(body).into(),
                )
                .await
                .unwrap();
            runs.push(RemoteArtifactIdentity {
                uri: run.uri.clone(),
                sha256: run.sha256.clone(),
                bytes: run.bytes,
            });
            fs::remove_file(&run.path).unwrap();
        }
        let request = RemoteArtifactRequest {
            generation: local.generation,
            router: local.router,
            mutations: local.mutations,
            runs,
            queries: local.queries,
            truth: local.truth,
            page_budget: local.page_budget,
            range_concurrency: 2,
        };

        let body = execute_remote_with_store(&request, &store).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["aggregate_recall_ppm"], 1_000_000);
        assert_eq!(value["total_requests"], 8);
        assert_eq!(value["samples"][0]["result_ids"], serde_json::json!([1, 5]));
        assert_eq!(value["samples"][1]["result_ids"], serde_json::json!([3, 6]));

        let mut authority_drift = request;
        authority_drift.router.uri = "s3://fixture/unbound-router".into();
        assert!(
            execute_remote_with_store(&authority_drift, &store)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn local_runner_recomputes_perfect_recall_from_authenticated_sparse_ranges() {
        let root = TempDir::new().unwrap();
        let request = exact_local_request(&root);
        let bytes = execute_local(&request).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["aggregate_recall_ppm"], 1_000_000);
        assert_eq!(value["worst_recall_ppm"], 1_000_000);
        assert_eq!(value["total_requests"], 8);
        assert_eq!(value["generation"], 1);
    }

    #[test]
    fn cli_requires_seven_explicit_artifact_identities_and_no_storage_flags() {
        let mut arguments = vec!["v85_delta_reader".to_string()];
        for role in ["generation", "router", "mutations", "queries", "truth"] {
            arguments.extend([
                format!("--{role}"),
                format!("/tmp/{role}"),
                format!("s3://fixture/{role}"),
                "1".repeat(64),
                "10".to_string(),
            ]);
        }
        for run in ["base-run", "delta-run"] {
            arguments.extend([
                "--run".to_string(),
                format!("/tmp/{run}"),
                format!("s3://fixture/{run}"),
                "1".repeat(64),
                "10".to_string(),
            ]);
        }
        arguments.extend(["--page-budget".into(), "2".into()]);
        let ExecutionRequest::Local(request) = parse_args(arguments.clone()).unwrap() else {
            panic!("local CLI selected a remote execution path")
        };
        assert_eq!(request.identities().len(), 7);
        assert_eq!(request.page_budget, 2);

        arguments.extend(["--bucket".into(), "forbidden".into()]);
        assert!(parse_args(arguments).is_err());
    }

    #[test]
    fn cli_accepts_repeated_explicit_runs_for_fragmented_and_compacted_generations() {
        // Break caught: the reader hard-codes one base plus one delta object and
        // therefore cannot measure 10/100-run snapshots before compaction.
        let mut arguments = vec!["v85_delta_reader".to_string()];
        for role in ["generation", "router", "mutations", "queries", "truth"] {
            arguments.extend([
                format!("--{role}"),
                format!("/tmp/{role}"),
                format!("s3://fixture/{role}"),
                "1".repeat(64),
                "10".to_string(),
            ]);
        }
        for run in ["base-000", "delta-000", "delta-001"] {
            arguments.extend([
                "--run".to_string(),
                format!("/tmp/{run}.arrow"),
                format!("s3://fixture/{run}.arrow"),
                "2".repeat(64),
                "20".to_string(),
            ]);
        }
        arguments.extend(["--page-budget".into(), "2".into()]);

        let ExecutionRequest::Local(request) = parse_args(arguments).unwrap() else {
            panic!("local CLI selected a remote execution path")
        };
        assert_eq!(request.runs.len(), 3);
        assert_eq!(request.identities().len(), 8);
    }
}
