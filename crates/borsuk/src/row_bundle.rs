//! Pure construction and validation for bounded canonical materialized row bundles.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    ops::Range,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use arrow_array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int32Array, Int64Array, ListArray,
    RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    builder::{BinaryBuilder, ListBuilder},
    types::{Int32Type, Int64Type, UInt64Type},
};
use arrow_buffer::Buffer;
use arrow_ipc::{
    Block, CompressionType, MetadataVersion,
    convert::fb_to_schema,
    reader::{FileDecoder, read_footer_length},
    root_as_footer,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    basic::{Compression, ZstdLevel},
    errors::{ParquetError, Result as ParquetResult},
    file::{
        properties::{WriterProperties, WriterVersion},
        reader::{ChunkReader, Length},
    },
    schema::types::ColumnPath,
};

use crate::{
    BorsukError, Result,
    format::{
        PositionedRouteAssignmentKind, PositionedRoutePlanRow, PositionedRouteProjectionKind,
        validate_positioned_route_plan,
    },
    mutation::{MutationOperation, MutationStamp, MutationState, MutationVersion},
};

const ROW_BUNDLE_FORMAT_VERSION: u16 = 1;
const DIRECTORY_FORMAT_VERSION: u16 = 1;
const DIRECTORY_PARTITIONS: usize = 256;
const REQUIRED_ROW_COLUMNS: usize = 12;
const ROW_BUNDLE_ZSTD_LEVEL: i32 = 3;
const ROW_BUNDLE_BLOOM_FPP: f64 = 0.01;
const ROW_BUNDLE_DATA_PAGE_BYTES: usize = 1024 * 1024;
const FORMAT_MAX_BUNDLE_BYTES: u64 = 128 * 1024 * 1024;
const FORMAT_MAX_ROW_GROUP_BYTES: u64 = 8 * 1024 * 1024;
const FORMAT_MAX_DIRECTORY_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
const FORMAT_MAX_DIRECTORY_BATCH_BYTES: u64 = 1024 * 1024;
const FORMAT_MAX_AUTHORITY_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SUMMARY_ROWS_PER_SHARD: usize = 1_024;
const MAX_RUN_ROWS: usize = crate::positioned_log::MAX_APPEND_ROWS as usize;
const MAX_RUN_BUNDLES: usize = MAX_RUN_ROWS;
const MAX_RUN_SUMMARIES: usize = MAX_RUN_ROWS;
const MAX_SUMMARY_ROOT_SHARDS: usize = MAX_RUN_SUMMARIES;
const MAX_ROSTER_ROWS: usize = MAX_RUN_BUNDLES + MAX_SUMMARY_ROOT_SHARDS + 1;
pub(crate) const MAX_ACTIVE_ROW_BUNDLE_LEVELS: usize = 16;
const MAX_ACTIVE_DIRECTORY_LEVELS: usize = 16;
const MAX_DIRECTORY_ROOT_RUNS: usize = DIRECTORY_PARTITIONS * MAX_ACTIVE_DIRECTORY_LEVELS;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RowBundlePackOptions {
    pub(crate) target_bundle_bytes: u64,
    pub(crate) hard_max_bundle_bytes: u64,
    pub(crate) target_row_group_bytes: u64,
    pub(crate) hard_max_row_group_bytes: u64,
    pub(crate) max_row_groups_per_bundle: usize,
    pub(crate) summary_rows_per_shard: usize,
}

impl RowBundlePackOptions {
    #[allow(
        dead_code,
        reason = "wired into materialization at the Task 4 atomic switch"
    )]
    pub(crate) const fn production() -> Self {
        Self {
            target_bundle_bytes: 64 * 1024 * 1024,
            hard_max_bundle_bytes: 128 * 1024 * 1024,
            target_row_group_bytes: 4 * 1024 * 1024,
            hard_max_row_group_bytes: 8 * 1024 * 1024,
            max_row_groups_per_bundle: 16,
            summary_rows_per_shard: 1_024,
        }
    }

    fn validate(self) -> Result<()> {
        if self.hard_max_bundle_bytes > FORMAT_MAX_BUNDLE_BYTES
            || self.hard_max_row_group_bytes > FORMAT_MAX_ROW_GROUP_BYTES
        {
            return invalid("row-bundle writer options exceed a v1 format hard cap");
        }
        if self.target_bundle_bytes == 0
            || self.target_bundle_bytes > self.hard_max_bundle_bytes
            || self.target_row_group_bytes == 0
            || self.target_row_group_bytes > self.hard_max_row_group_bytes
            || self.hard_max_row_group_bytes > self.hard_max_bundle_bytes
            || self.max_row_groups_per_bundle == 0
            || self.max_row_groups_per_bundle > 16
            || self.summary_rows_per_shard == 0
            || self.summary_rows_per_shard > MAX_SUMMARY_ROWS_PER_SHARD
        {
            return invalid("row-bundle writer settings are outside the pinned layout bounds");
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CanonicalRowBatch {
    batch: RecordBatch,
}

impl CanonicalRowBatch {
    pub(crate) fn try_new(batch: RecordBatch) -> Result<Self> {
        validate_canonical_row_schema(batch.schema().as_ref())?;
        if batch.num_rows() == 0 {
            return invalid("canonical row batch must not be empty");
        }
        Ok(Self { batch })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactRef {
    pub(crate) path: String,
    pub(crate) checksum: String,
    pub(crate) encoded_bytes: u64,
}

impl ArtifactRef {
    fn validate(&self, extension: &str) -> Result<()> {
        if self.path.is_empty() || !self.path.ends_with(extension) {
            return invalid("row-bundle artifact path has the wrong extension");
        }
        validate_checksum(&self.checksum)?;
        if self.encoded_bytes == 0 {
            return invalid("row-bundle artifact must not be empty");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthenticatedRange {
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) checksum: String,
}

impl AuthenticatedRange {
    fn checked(&self, file_len: u64) -> Result<Range<u64>> {
        validate_checksum(&self.checksum)?;
        if self.length == 0 {
            return invalid("authenticated range must not be empty");
        }
        let end = self
            .offset
            .checked_add(self.length)
            .ok_or_else(|| BorsukError::InvalidStorage("authenticated range overflows".into()))?;
        if end > file_len {
            return invalid("authenticated range escapes its immutable object");
        }
        Ok(self.offset..end)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct VerifiedRange {
    offset: u64,
    end: u64,
    bytes: Bytes,
}

impl VerifiedRange {
    pub(crate) fn new(
        expected: &AuthenticatedRange,
        fetched_offset: u64,
        bytes: Bytes,
    ) -> Result<Self> {
        if fetched_offset != expected.offset || bytes.len() as u64 != expected.length {
            return invalid("fetched range does not match its authenticated bounds");
        }
        let end = expected
            .offset
            .checked_add(expected.length)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("authenticated range overflows".to_string())
            })?;
        verify_checksum(&expected.checksum, &bytes, "authenticated range")?;
        Ok(Self {
            offset: fetched_offset,
            end,
            bytes,
        })
    }

    fn range(&self) -> Range<u64> {
        self.offset..self.end
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BoundedChunkReader {
    file_len: u64,
    ranges: Vec<VerifiedRange>,
}

impl BoundedChunkReader {
    pub(crate) fn new(file_len: u64, mut ranges: Vec<VerifiedRange>) -> Result<Self> {
        if file_len == 0 || ranges.is_empty() {
            return invalid("bounded chunk reader needs a file length and verified ranges");
        }
        ranges.sort_by_key(|range| range.offset);
        for range in &ranges {
            if range.range().end > file_len {
                return invalid("verified range escapes bounded chunk reader file length");
            }
        }
        for pair in ranges.windows(2) {
            if pair[0].range().end > pair[1].range().start {
                return invalid("verified ranges overlap");
            }
        }
        Ok(Self { file_len, ranges })
    }

    fn containing(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let length = u64::try_from(length)
            .map_err(|_| ParquetError::General("bounded read length exceeds u64".into()))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| ParquetError::General("bounded read range overflows".into()))?;
        let range = self
            .ranges
            .iter()
            .find(|range| start >= range.range().start && end <= range.range().end)
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "Parquet requested unauthenticated range {start}..{end}"
                ))
            })?;
        let local_start = usize::try_from(start - range.offset)
            .map_err(|_| ParquetError::General("bounded read offset exceeds usize".into()))?;
        let local_end = usize::try_from(end - range.offset)
            .map_err(|_| ParquetError::General("bounded read end exceeds usize".into()))?;
        Ok(range.bytes.slice(local_start..local_end))
    }
}

impl Length for BoundedChunkReader {
    fn len(&self) -> u64 {
        self.file_len
    }
}

impl ChunkReader for BoundedChunkReader {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        let range = self
            .ranges
            .iter()
            .find(|range| start >= range.range().start && start < range.range().end)
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "Parquet requested unauthenticated reader offset {start}"
                ))
            })?;
        let local = usize::try_from(start - range.offset)
            .map_err(|_| ParquetError::General("bounded reader offset exceeds usize".into()))?;
        Ok(Cursor::new(range.bytes.slice(local..)))
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        self.containing(start, length)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RowGroupSummary {
    pub(crate) row_group: u32,
    pub(crate) modality: String,
    pub(crate) projection_kind: u8,
    pub(crate) assignment_kind: u8,
    pub(crate) assignment_checksum: [u8; 32],
    pub(crate) routing_epoch: Option<u64>,
    pub(crate) min_cell_ordinal: Option<u32>,
    pub(crate) max_cell_ordinal: Option<u32>,
    pub(crate) min_record_id: Vec<u8>,
    pub(crate) max_record_id: Vec<u8>,
    pub(crate) first_record_id: Vec<u8>,
    pub(crate) last_record_id: Vec<u8>,
    pub(crate) row_count: u64,
    pub(crate) data: AuthenticatedRange,
    pub(crate) record_id_bloom: Option<AuthenticatedRange>,
    pub(crate) page_indexes: Vec<AuthenticatedRange>,
    pub(crate) min_stamp: MutationStamp,
    pub(crate) max_stamp: MutationStamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RowBundleRef {
    pub(crate) artifact: ArtifactRef,
    pub(crate) footer: AuthenticatedRange,
    pub(crate) row_groups: Vec<RowGroupSummary>,
}

#[derive(Clone, Debug)]
pub(crate) struct PackedRowBundle {
    #[cfg(test)]
    pub(crate) bytes: Bytes,
    pub(crate) reference: RowBundleRef,
}

pub(crate) trait RowBundleObjectSink {
    fn emit(&mut self, artifact: &ArtifactRef, bytes: Bytes) -> Result<()>;
}

struct EncodedRowBundle {
    bytes: Bytes,
    reference: RowBundleRef,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum RowBundleRole {
    Bundle,
    SummaryShard,
    SummaryRoot,
}

impl RowBundleRole {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Bundle => "row_bundle",
            Self::SummaryShard => "row_bundle_summary_shard",
            Self::SummaryRoot => "row_bundle_summary_root",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RosterEntry {
    pub(crate) role: RowBundleRole,
    pub(crate) path: String,
    pub(crate) checksum: String,
    pub(crate) encoded_bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct PackedArtifact {
    #[cfg(test)]
    pub(crate) bytes: Vec<u8>,
    pub(crate) reference: ArtifactRef,
}

#[derive(Clone, Debug)]
pub(crate) struct PackedRoster {
    #[cfg(test)]
    pub(crate) bytes: Vec<u8>,
    pub(crate) artifacts: Vec<RosterEntry>,
    pub(crate) role_bytes: BTreeMap<RowBundleRole, u64>,
    reference: ArtifactRef,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RowBundleRunRef {
    pub(crate) level: u8,
    pub(crate) summary_root: ArtifactRef,
    pub(crate) roster: ArtifactRef,
    pub(crate) bundle_count: u64,
    pub(crate) summary_count: u64,
    pub(crate) role_bytes: BTreeMap<String, u64>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ConstructionMetrics {
    pub(crate) peak_staged_bytes: u64,
    pub(crate) directory_writes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct PackedRowBundleRun {
    pub(crate) bundles: Vec<PackedRowBundle>,
    pub(crate) summary_shards: Vec<PackedArtifact>,
    pub(crate) root: PackedArtifact,
    pub(crate) roster: PackedRoster,
    pub(crate) run_ref: RowBundleRunRef,
    pub(crate) metrics: ConstructionMetrics,
}

impl PackedRowBundleRun {
    pub(crate) fn summary_row_count(&self) -> usize {
        self.bundles
            .iter()
            .map(|bundle| bundle.reference.row_groups.len())
            .sum()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StagedRowBundleGeneration {
    pub(crate) active_runs: Vec<RowBundleRunRef>,
    pub(crate) directory_root: ArtifactRef,
    pub(crate) root: PackedArtifact,
    pub(crate) metrics: ConstructionMetrics,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RowBundleGenerationRef {
    pub(crate) active_runs: Vec<RowBundleRunRef>,
    pub(crate) directory_root: ArtifactRef,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SummaryBoundary {
    modality: String,
    projection_kind: u8,
    assignment_kind: u8,
    assignment_checksum: [u8; 32],
    routing_epoch: Option<u64>,
    cell_ordinal: Option<u32>,
    record_id: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SummaryShardRef {
    first: SummaryBoundary,
    last: SummaryBoundary,
    pub(crate) artifact: ArtifactRef,
    pub(crate) row_count: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DirectoryPackOptions {
    pub(crate) hard_max_object_bytes: u64,
    pub(crate) target_batch_bytes: u64,
    pub(crate) hard_max_batch_bytes: u64,
}

impl DirectoryPackOptions {
    #[allow(
        dead_code,
        reason = "wired into materialization at the Task 4 atomic switch"
    )]
    pub(crate) const fn production() -> Self {
        Self {
            hard_max_object_bytes: 64 * 1024 * 1024,
            target_batch_bytes: 512 * 1024,
            hard_max_batch_bytes: 1024 * 1024,
        }
    }

    fn validate(self) -> Result<()> {
        if self.hard_max_object_bytes > FORMAT_MAX_DIRECTORY_OBJECT_BYTES
            || self.hard_max_batch_bytes > FORMAT_MAX_DIRECTORY_BATCH_BYTES
        {
            return invalid("ID-directory writer options exceed a v1 format hard cap");
        }
        if self.hard_max_object_bytes == 0
            || self.target_batch_bytes == 0
            || self.target_batch_bytes > self.hard_max_batch_bytes
            || self.hard_max_batch_bytes >= self.hard_max_object_bytes
        {
            return invalid("ID-directory writer settings are outside the pinned bounds");
        }
        Ok(())
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub(crate) struct DirectoryPartition(u8);

impl DirectoryPartition {
    pub(crate) fn for_record_id(record_id: &[u8]) -> Self {
        Self(blake3::hash(record_id).as_bytes()[0])
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DirectoryRow {
    pub(crate) record_id: Vec<u8>,
    pub(crate) routing_epoch: u64,
    pub(crate) cell_ordinal: u32,
    pub(crate) state: MutationState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DirectoryBatchRef {
    min_record_id: Vec<u8>,
    max_record_id: Vec<u8>,
    row_count: u64,
    range: AuthenticatedRange,
    metadata_length: i32,
    body_length: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DirectoryRunRef {
    pub(crate) partition: DirectoryPartition,
    pub(crate) level: u8,
    pub(crate) artifact: ArtifactRef,
    footer: AuthenticatedRange,
    batches: Vec<DirectoryBatchRef>,
}

#[derive(Clone, Debug)]
pub(crate) struct PackedDirectoryPartitionRun {
    pub(crate) bytes: Bytes,
    pub(crate) reference: DirectoryRunRef,
}

#[derive(Clone, Debug)]
pub(crate) struct PackedDirectoryRoot {
    pub(crate) bytes: Vec<u8>,
    pub(crate) reference: ArtifactRef,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DirectoryOwnerState {
    pub(crate) routing_epoch: u64,
    pub(crate) cell_ordinal: u32,
    pub(crate) state: MutationState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DirectoryLookup {
    Found(DirectoryOwnerState),
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MaterializedAssignmentAuthority {
    Catalog {
        modality: String,
        projection_kind: u8,
        assignment_checksum: [u8; 32],
        owner: DirectoryOwnerState,
    },
    Analyzer {
        modality: String,
        projection_kind: u8,
        assignment_checksum: [u8; 32],
        state: MutationState,
    },
}

/// Exact authenticated assignment identity used for one materialized-row read.
/// Catalog reads carry the stable ID-directory owner; analyzer reads carry no
/// fabricated cell and must instead provide their expected analyzer identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaterializedLookupAuthority(MaterializedAssignmentAuthority);

impl MaterializedLookupAuthority {
    pub(crate) fn catalog(
        modality: &str,
        projection_kind: PositionedRouteProjectionKind,
        assignment_checksum: [u8; 32],
        owner: DirectoryOwnerState,
    ) -> Result<Self> {
        if modality.is_empty() || assignment_checksum == [0; 32] || owner.routing_epoch == 0 {
            return invalid("catalog materialized lookup authority is incomplete");
        }
        Ok(Self(MaterializedAssignmentAuthority::Catalog {
            modality: modality.to_string(),
            projection_kind: projection_kind_code(projection_kind),
            assignment_checksum,
            owner,
        }))
    }

    pub(crate) fn analyzer(
        modality: &str,
        projection_kind: PositionedRouteProjectionKind,
        assignment_checksum: [u8; 32],
        state: MutationState,
    ) -> Result<Self> {
        if modality.is_empty() || assignment_checksum == [0; 32] {
            return invalid("analyzer materialized lookup authority is incomplete");
        }
        Ok(Self(MaterializedAssignmentAuthority::Analyzer {
            modality: modality.to_string(),
            projection_kind: projection_kind_code(projection_kind),
            assignment_checksum,
            state,
        }))
    }

    fn modality(&self) -> &str {
        match &self.0 {
            MaterializedAssignmentAuthority::Catalog { modality, .. }
            | MaterializedAssignmentAuthority::Analyzer { modality, .. } => modality,
        }
    }

    fn projection_kind(&self) -> u8 {
        match &self.0 {
            MaterializedAssignmentAuthority::Catalog {
                projection_kind, ..
            }
            | MaterializedAssignmentAuthority::Analyzer {
                projection_kind, ..
            } => *projection_kind,
        }
    }

    fn assignment_kind(&self) -> u8 {
        match &self.0 {
            MaterializedAssignmentAuthority::Catalog { .. } => 0,
            MaterializedAssignmentAuthority::Analyzer { .. } => 1,
        }
    }

    fn assignment_checksum(&self) -> [u8; 32] {
        match &self.0 {
            MaterializedAssignmentAuthority::Catalog {
                assignment_checksum,
                ..
            }
            | MaterializedAssignmentAuthority::Analyzer {
                assignment_checksum,
                ..
            } => *assignment_checksum,
        }
    }

    fn routing_epoch(&self) -> Option<u64> {
        match &self.0 {
            MaterializedAssignmentAuthority::Catalog { owner, .. } => Some(owner.routing_epoch),
            MaterializedAssignmentAuthority::Analyzer { .. } => None,
        }
    }

    fn cell_ordinal(&self) -> Option<u32> {
        match &self.0 {
            MaterializedAssignmentAuthority::Catalog { owner, .. } => Some(owner.cell_ordinal),
            MaterializedAssignmentAuthority::Analyzer { .. } => None,
        }
    }

    fn state(&self) -> MutationState {
        match &self.0 {
            MaterializedAssignmentAuthority::Catalog { owner, .. } => owner.state,
            MaterializedAssignmentAuthority::Analyzer { state, .. } => *state,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OpenedRowBundleRun {
    run: RowBundleRunRef,
    roster: Vec<RosterEntry>,
    shards: Vec<SummaryShardRef>,
}

#[derive(Clone, Debug)]
pub(crate) struct OpenedRowBundleGeneration {
    pub(crate) generation: RowBundleGenerationRef,
    pub(crate) active_runs: Vec<OpenedRowBundleRun>,
    pub(crate) directory_runs: Vec<DirectoryRunRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ObjectRangeRequest {
    pub(crate) path: String,
    pub(crate) range: Range<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct CompactedRowBundles {
    pub(crate) row_bundles: PackedRowBundleRun,
    pub(crate) directory_runs: Vec<DirectoryRunRef>,
    pub(crate) metrics: ConstructionMetrics,
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(BorsukError::InvalidStorage(message.to_string()))
}

fn validate_checksum(checksum: &str) -> Result<()> {
    if checksum.len() != 64
        || !checksum
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid("checksum is not lowercase BLAKE3 hex");
    }
    Ok(())
}

fn verify_checksum(expected: &str, bytes: &[u8], role: &str) -> Result<()> {
    validate_checksum(expected)?;
    let actual = blake3::hash(bytes).to_hex().to_string();
    if actual != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "{role} checksum mismatch: expected {expected}, got {actual}"
        )));
    }
    Ok(())
}

fn checked_usize(value: u64, role: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| BorsukError::InvalidStorage(format!("{role} exceeds usize")))
}

fn validate_canonical_row_schema(schema: &Schema) -> Result<()> {
    let required = [
        Field::new("row_bundle_format", DataType::UInt16, false),
        Field::new("modality", DataType::Utf8, false),
        Field::new("projection_kind", DataType::UInt8, false),
        Field::new("assignment_kind", DataType::UInt8, false),
        Field::new("assignment_checksum", DataType::FixedSizeBinary(32), false),
        Field::new("routing_epoch", DataType::UInt64, true),
        Field::new("cell_ordinal", DataType::UInt32, true),
        Field::new("record_id", DataType::Binary, false),
        Field::new("projected_ordinal", DataType::UInt32, false),
        Field::new("mutation_hlc", DataType::UInt64, false),
        Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
    ];
    if schema.fields().len() < REQUIRED_ROW_COLUMNS
        || schema
            .fields()
            .iter()
            .take(REQUIRED_ROW_COLUMNS)
            .zip(required.iter())
            .any(|(actual, expected)| actual.as_ref() != expected)
    {
        return invalid("canonical row bundle schema is not exact");
    }
    if schema
        .fields()
        .iter()
        .skip(REQUIRED_ROW_COLUMNS)
        .any(|field| {
            required
                .iter()
                .any(|required| field.name() == required.name())
        })
    {
        return invalid("canonical row bundle schema repeats a system column");
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct CanonicalRowKey {
    modality: String,
    projection_kind: u8,
    assignment_kind: u8,
    assignment_checksum: [u8; 32],
    routing_epoch: Option<u64>,
    cell_ordinal: Option<u32>,
    record_id: Vec<u8>,
    projected_ordinal: u32,
    hlc: u64,
    writer: [u8; 16],
    digest: [u8; 32],
}

impl CanonicalRowKey {
    fn stamp(&self) -> MutationStamp {
        MutationStamp::new(
            MutationVersion::from_parts(self.hlc, self.writer),
            self.digest,
        )
    }
}

fn canonical_row_key(batch: &RecordBatch, row: usize) -> Result<CanonicalRowKey> {
    let versions = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .expect("canonical schema");
    if versions.is_null(row) || versions.value(row) != ROW_BUNDLE_FORMAT_VERSION {
        return invalid("canonical row bundle format marker is unsupported");
    }
    let modality = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("canonical schema");
    let projection_kind = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("canonical schema");
    let assignment_kind = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("canonical schema");
    let checksum = batch
        .column(4)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("canonical schema");
    let routing_epoch = batch
        .column(5)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("canonical schema");
    let cell = batch
        .column(6)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("canonical schema");
    let ids = batch
        .column(7)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("canonical schema");
    let projected = batch
        .column(8)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("canonical schema");
    let hlc = batch
        .column(9)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("canonical schema");
    let writer = batch
        .column(10)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("canonical schema");
    let digest = batch
        .column(11)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("canonical schema");
    if [0, 1, 2, 3, 4, 7, 8, 9, 10, 11]
        .into_iter()
        .any(|column| batch.column(column).is_null(row))
    {
        return invalid("canonical row bundle system columns must not contain nulls");
    }
    let checksum: [u8; 32] = checksum.value(row).try_into().map_err(|_| {
        BorsukError::InvalidStorage("canonical catalog checksum width changed".into())
    })?;
    let writer: [u8; 16] = writer.value(row).try_into().map_err(|_| {
        BorsukError::InvalidStorage("canonical mutation writer width changed".into())
    })?;
    let digest: [u8; 32] = digest.value(row).try_into().map_err(|_| {
        BorsukError::InvalidStorage("canonical mutation digest width changed".into())
    })?;
    let modality = modality.value(row).to_string();
    let record_id = ids.value(row).to_vec();
    let projection_kind = projection_kind.value(row);
    let assignment_kind = assignment_kind.value(row);
    let routing_epoch = (!routing_epoch.is_null(row)).then(|| routing_epoch.value(row));
    let cell_ordinal = (!cell.is_null(row)).then(|| cell.value(row));
    let valid_assignment = match assignment_kind {
        0 => routing_epoch.is_some_and(|epoch| epoch > 0) && cell_ordinal.is_some(),
        1 => routing_epoch.is_none() && cell_ordinal.is_none(),
        _ => false,
    };
    if modality.is_empty()
        || record_id.is_empty()
        || checksum == [0; 32]
        || projection_kind > 4
        || !valid_assignment
    {
        return invalid("canonical row bundle identity is empty or invalid");
    }
    Ok(CanonicalRowKey {
        modality,
        projection_kind,
        assignment_kind,
        assignment_checksum: checksum,
        routing_epoch,
        cell_ordinal,
        record_id,
        projected_ordinal: projected.value(row),
        hlc: hlc.value(row),
        writer,
        digest,
    })
}

fn projection_kind_code(kind: PositionedRouteProjectionKind) -> u8 {
    match kind {
        PositionedRouteProjectionKind::Primary => 0,
        PositionedRouteProjectionKind::Dense => 1,
        PositionedRouteProjectionKind::Sparse => 2,
        PositionedRouteProjectionKind::Text => 3,
        PositionedRouteProjectionKind::LateInteraction => 4,
    }
}

fn assignment_kind_code(kind: PositionedRouteAssignmentKind) -> u8 {
    match kind {
        PositionedRouteAssignmentKind::Catalog => 0,
        PositionedRouteAssignmentKind::Analyzer => 1,
    }
}

fn validated_row_keys(
    batches: &[CanonicalRowBatch],
    route_plan: &[PositionedRoutePlanRow],
) -> Result<Vec<Vec<CanonicalRowKey>>> {
    if batches.is_empty() {
        return invalid("canonical row packer needs at least one batch");
    }
    if route_plan.len() > MAX_RUN_ROWS {
        return invalid("canonical row packer route plan exceeds the positioned append row bound");
    }
    validate_positioned_route_plan(route_plan)?;
    let mut routes = BTreeMap::<(String, Vec<u8>, u32), &PositionedRoutePlanRow>::new();
    for route in route_plan.iter().filter(|route| route.record_id.is_some()) {
        let id = route.record_id.as_ref().expect("filtered assignment");
        let ordinal = route.projected_ordinal.ok_or_else(|| {
            BorsukError::InvalidStorage("route-plan data row lost its ordinal".into())
        })?;
        if routes
            .insert((route.modality.clone(), id.clone(), ordinal), route)
            .is_some()
        {
            return invalid("route plan repeats a canonical bundle row identity");
        }
    }

    let mut all = Vec::with_capacity(batches.len());
    let expected_routes = routes.len();
    let mut previous = None::<CanonicalRowKey>;
    let mut used = BTreeSet::new();
    for canonical in batches {
        validate_canonical_row_schema(canonical.batch.schema().as_ref())?;
        let mut keys = Vec::with_capacity(canonical.batch.num_rows());
        let mut batch_identity = None::<(String, u8, u8, [u8; 32], Option<u64>)>;
        for row in 0..canonical.batch.num_rows() {
            let key = canonical_row_key(&canonical.batch, row)?;
            let identity = (
                key.modality.clone(),
                key.projection_kind,
                key.assignment_kind,
                key.assignment_checksum,
                key.routing_epoch,
            );
            if batch_identity
                .as_ref()
                .is_some_and(|expected| expected != &identity)
            {
                return invalid(
                    "one canonical row batch must have one modality, catalog, and routing epoch",
                );
            }
            batch_identity.get_or_insert(identity);
            if previous.as_ref().is_some_and(|left| left >= &key) {
                return invalid("canonical row bundle rows are not globally ordered");
            }
            let route_key = (
                key.modality.clone(),
                key.record_id.clone(),
                key.projected_ordinal,
            );
            let route = routes.get(&route_key).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "canonical row is missing from its authenticated route plan".into(),
                )
            })?;
            if projection_kind_code(route.projection_kind) != key.projection_kind
                || assignment_kind_code(route.assignment.kind) != key.assignment_kind
                || route.assignment.checksum != key.assignment_checksum
                || route.assignment.routing_epoch != key.routing_epoch
                || route.cell_ordinal != key.cell_ordinal
                || route.stamp != key.stamp()
            {
                return invalid("canonical row disagrees with its authenticated route plan");
            }
            if !used.insert(route_key) {
                return invalid("canonical row bundle repeats one routed row");
            }
            previous = Some(key.clone());
            keys.push(key);
        }
        all.push(keys);
    }
    if used.len() != expected_routes {
        return invalid("canonical row bundles do not cover every route-plan data row");
    }
    Ok(all)
}

pub(crate) fn pack_canonical_row_bundles_to_sink(
    batches: &[CanonicalRowBatch],
    route_plan: &[PositionedRoutePlanRow],
    target_level: u8,
    options: RowBundlePackOptions,
    sink: &mut dyn RowBundleObjectSink,
) -> Result<PackedRowBundleRun> {
    options.validate()?;
    if usize::from(target_level) >= MAX_ACTIVE_ROW_BUNDLE_LEVELS {
        return invalid("row-bundle target level exceeds the active level cap");
    }
    let keys = validated_row_keys(batches, route_plan)?;
    let mut bundles = Vec::new();
    let mut peak_staged_bytes = 0_u64;
    for (canonical, keys) in batches.iter().zip(keys.iter()) {
        let row_memory = canonical
            .batch
            .get_array_memory_size()
            .checked_div(canonical.batch.num_rows())
            .unwrap_or(1)
            .max(1);
        let estimated_rows = checked_usize(options.target_bundle_bytes, "bundle target")?
            .checked_div(row_memory)
            .unwrap_or(1)
            .max(1);
        let mut start = 0_usize;
        while start < canonical.batch.num_rows() {
            let mut rows = estimated_rows
                .min(canonical.batch.num_rows() - start)
                .max(1);
            let packed = loop {
                match encode_row_bundle(
                    canonical.batch.slice(start, rows),
                    &keys[start..start + rows],
                    options,
                ) {
                    Ok(bundle)
                        if bundle.bytes.len() as u64 > options.target_bundle_bytes && rows > 1 =>
                    {
                        rows = rows.div_ceil(2);
                    }
                    Ok(bundle) => break bundle,
                    Err(error) if rows > 1 => {
                        rows = rows.div_ceil(2);
                        if rows == 0 {
                            return Err(error);
                        }
                    }
                    Err(error) => return Err(error),
                }
            };
            if packed.bytes.len() as u64 > options.hard_max_bundle_bytes {
                return invalid(
                    "one canonical row produces an oversized bundle above the hard cap",
                );
            }
            peak_staged_bytes = peak_staged_bytes.max(packed.bytes.len() as u64);
            #[cfg(test)]
            let retained = packed.bytes.clone();
            sink.emit(&packed.reference.artifact, packed.bytes)?;
            bundles.push(PackedRowBundle {
                #[cfg(test)]
                bytes: retained,
                reference: packed.reference,
            });
            start += rows;
        }
    }
    build_packed_run(bundles, target_level, options, peak_staged_bytes, sink)
}

#[cfg(test)]
#[derive(Default)]
struct CollectingRowBundleSink {
    objects: BTreeMap<String, Bytes>,
}

#[cfg(test)]
impl RowBundleObjectSink for CollectingRowBundleSink {
    fn emit(&mut self, artifact: &ArtifactRef, bytes: Bytes) -> Result<()> {
        if self.objects.insert(artifact.path.clone(), bytes).is_some() {
            return invalid("row-bundle collecting sink saw a duplicate object path");
        }
        Ok(())
    }
}

#[cfg(test)]
fn pack_canonical_row_bundles(
    batches: &[CanonicalRowBatch],
    route_plan: &[PositionedRoutePlanRow],
    options: RowBundlePackOptions,
) -> Result<PackedRowBundleRun> {
    let mut sink = CollectingRowBundleSink::default();
    pack_canonical_row_bundles_to_sink(batches, route_plan, 0, options, &mut sink)
}

fn encode_row_bundle(
    batch: RecordBatch,
    keys: &[CanonicalRowKey],
    options: RowBundlePackOptions,
) -> Result<EncodedRowBundle> {
    let estimated_bytes = batch.get_array_memory_size() as u64;
    let desired_groups = usize::try_from(
        estimated_bytes
            .div_ceil(options.target_row_group_bytes)
            .clamp(1, options.max_row_groups_per_bundle as u64),
    )
    .map_err(|_| BorsukError::InvalidStorage("row-group count exceeds usize".into()))?;
    let rows_per_group = batch.num_rows().div_ceil(desired_groups).max(1);
    let zstd = ZstdLevel::try_new(ROW_BUNDLE_ZSTD_LEVEL)?;
    let record_id_path = ColumnPath::from("record_id");
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(zstd))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_data_page_size_limit(ROW_BUNDLE_DATA_PAGE_BYTES)
        .set_dictionary_page_size_limit(ROW_BUNDLE_DATA_PAGE_BYTES)
        .set_bloom_filter_enabled(false)
        .set_column_bloom_filter_enabled(record_id_path.clone(), true)
        .set_column_bloom_filter_fpp(record_id_path.clone(), ROW_BUNDLE_BLOOM_FPP)
        .set_column_bloom_filter_ndv(record_id_path, batch.num_rows() as u64)
        .build();
    let mut bytes = Vec::new();
    let mut row_ranges = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties))?;
        let mut start = 0;
        while start < batch.num_rows() {
            let rows = rows_per_group.min(batch.num_rows() - start);
            writer.write(&batch.slice(start, rows))?;
            writer.flush()?;
            row_ranges.push(start..start + rows);
            start += rows;
        }
        writer.close()?;
    }
    let bytes = Bytes::from(bytes);
    if bytes.len() as u64 > options.hard_max_bundle_bytes {
        return invalid("encoded row bundle exceeds the hard byte cap");
    }
    let reference = parse_completed_row_bundle(&bytes, keys, &row_ranges, options)?;
    Ok(EncodedRowBundle { bytes, reference })
}

fn authenticated_range(bytes: &[u8], range: Range<u64>) -> Result<AuthenticatedRange> {
    let start = checked_usize(range.start, "authenticated range start")?;
    let end = checked_usize(range.end, "authenticated range end")?;
    let selected = bytes.get(start..end).ok_or_else(|| {
        BorsukError::InvalidStorage("authenticated range escapes completed object".into())
    })?;
    Ok(AuthenticatedRange {
        offset: range.start,
        length: range.end - range.start,
        checksum: blake3::hash(selected).to_hex().to_string(),
    })
}

fn parse_completed_row_bundle(
    bytes: &Bytes,
    keys: &[CanonicalRowKey],
    row_ranges: &[Range<usize>],
    options: RowBundlePackOptions,
) -> Result<RowBundleRef> {
    let file_len = bytes.len() as u64;
    if bytes.len() < 8 {
        return invalid("completed Parquet row bundle is shorter than its footer trailer");
    }
    if &bytes[bytes.len() - 4..] != b"PAR1" {
        return invalid("completed Parquet row bundle has no trailing PAR1 magic");
    }
    let metadata_len = u32::from_le_bytes(
        bytes[bytes.len() - 8..bytes.len() - 4]
            .try_into()
            .map_err(|_| {
                BorsukError::InvalidStorage("Parquet footer length is truncated".into())
            })?,
    ) as u64;
    let footer_start = file_len.checked_sub(metadata_len + 8).ok_or_else(|| {
        BorsukError::InvalidStorage("Parquet footer length exceeds object".into())
    })?;
    let footer = authenticated_range(bytes, footer_start..file_len)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?;
    let metadata = builder.metadata();
    if metadata.num_row_groups() != row_ranges.len()
        || metadata.num_row_groups() > options.max_row_groups_per_bundle
    {
        return invalid("completed row bundle violates its row-group count bound");
    }
    let schema_descr = metadata.file_metadata().schema_descr();
    let record_id_column = (0..schema_descr.num_columns())
        .find(|index| schema_descr.column(*index).name() == "record_id")
        .ok_or_else(|| BorsukError::InvalidStorage("row bundle lost record_id column".into()))?;
    let mut summaries = Vec::with_capacity(row_ranges.len());
    for (ordinal, (row_group, key_range)) in
        metadata.row_groups().iter().zip(row_ranges).enumerate()
    {
        if row_group.num_rows() < 0 || row_group.num_rows() as usize != key_range.len() {
            return invalid("completed row-group row count disagrees with canonical input");
        }
        let mut data_start = u64::MAX;
        let mut data_end = 0_u64;
        let mut page_indexes = Vec::new();
        for column in row_group.columns() {
            if column.file_path().is_some() {
                return invalid("external Parquet file_path is forbidden in row bundles");
            }
            let start = column
                .dictionary_page_offset()
                .unwrap_or_else(|| column.data_page_offset());
            let length = column.compressed_size();
            if start < 0 || length <= 0 {
                return invalid("row-group column has an invalid physical range");
            }
            let start = start as u64;
            let end = start.checked_add(length as u64).ok_or_else(|| {
                BorsukError::InvalidStorage("row-group column range overflows".into())
            })?;
            if end > footer_start {
                return invalid("row-group data overlaps its authenticated footer");
            }
            data_start = data_start.min(start);
            data_end = data_end.max(end);
            for (offset, length) in [
                (column.column_index_offset(), column.column_index_length()),
                (column.offset_index_offset(), column.offset_index_length()),
            ] {
                if let (Some(offset), Some(length)) = (offset, length) {
                    if offset < 0 || length <= 0 {
                        return invalid("Parquet page index range is invalid");
                    }
                    page_indexes.push(authenticated_range(
                        bytes,
                        offset as u64..offset as u64 + length as u64,
                    )?);
                }
            }
        }
        let data = authenticated_range(bytes, data_start..data_end)?;
        if data.length > options.hard_max_row_group_bytes {
            return invalid("encoded row-group span exceeds its hard byte cap");
        }
        let id_column = row_group.column(record_id_column);
        let record_id_bloom = match (
            id_column.bloom_filter_offset(),
            id_column.bloom_filter_length(),
        ) {
            (Some(offset), Some(length)) if offset >= 0 && length > 0 => Some(authenticated_range(
                bytes,
                offset as u64..offset as u64 + length as u64,
            )?),
            (None, None) => None,
            _ => return invalid("record_id bloom range is incomplete or invalid"),
        };
        let group_keys = &keys[key_range.clone()];
        let first = group_keys.first().ok_or_else(|| {
            BorsukError::InvalidStorage("completed row group has no canonical keys".into())
        })?;
        if group_keys.iter().any(|key| {
            key.modality != first.modality
                || key.projection_kind != first.projection_kind
                || key.assignment_kind != first.assignment_kind
                || key.assignment_checksum != first.assignment_checksum
                || key.routing_epoch != first.routing_epoch
        }) {
            return invalid("one row group crosses a modality or assignment identity");
        }
        let min_record_id = group_keys
            .iter()
            .map(|key| key.record_id.as_slice())
            .min()
            .expect("non-empty keys")
            .to_vec();
        let max_record_id = group_keys
            .iter()
            .map(|key| key.record_id.as_slice())
            .max()
            .expect("non-empty keys")
            .to_vec();
        let last = group_keys.last().expect("non-empty keys");
        let min_stamp_key = group_keys
            .iter()
            .min_by_key(|key| (key.hlc, key.writer))
            .expect("non-empty keys");
        let max_stamp_key = group_keys
            .iter()
            .max_by_key(|key| (key.hlc, key.writer))
            .expect("non-empty keys");
        summaries.push(RowGroupSummary {
            row_group: u32::try_from(ordinal)
                .map_err(|_| BorsukError::InvalidStorage("row-group ordinal exceeds u32".into()))?,
            modality: first.modality.clone(),
            projection_kind: first.projection_kind,
            assignment_kind: first.assignment_kind,
            assignment_checksum: first.assignment_checksum,
            routing_epoch: first.routing_epoch,
            min_cell_ordinal: group_keys.iter().filter_map(|key| key.cell_ordinal).min(),
            max_cell_ordinal: group_keys.iter().filter_map(|key| key.cell_ordinal).max(),
            min_record_id,
            max_record_id,
            first_record_id: first.record_id.clone(),
            last_record_id: last.record_id.clone(),
            row_count: group_keys.len() as u64,
            data,
            record_id_bloom,
            page_indexes,
            min_stamp: min_stamp_key.stamp(),
            max_stamp: max_stamp_key.stamp(),
        });
    }
    let checksum = blake3::hash(bytes).to_hex().to_string();
    Ok(RowBundleRef {
        artifact: ArtifactRef {
            path: format!("row-bundles/{checksum}.parquet"),
            checksum,
            encoded_bytes: file_len,
        },
        footer,
        row_groups: summaries,
    })
}

fn write_parquet_batch(batch: RecordBatch) -> Result<Vec<u8>> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(
            ROW_BUNDLE_ZSTD_LEVEL,
        )?))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_data_page_size_limit(ROW_BUNDLE_DATA_PAGE_BYTES)
        .set_dictionary_page_size_limit(ROW_BUNDLE_DATA_PAGE_BYTES)
        .set_bloom_filter_enabled(false)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

fn decode_exact_parquet_artifact(
    artifact: &ArtifactRef,
    bytes: Bytes,
    expected_schema: &SchemaRef,
    role: &str,
    max_rows: usize,
) -> Result<RecordBatch> {
    artifact.validate(".parquet")?;
    if artifact.encoded_bytes > FORMAT_MAX_AUTHORITY_OBJECT_BYTES {
        return invalid("Parquet authority exceeds the v1 format hard cap");
    }
    if bytes.len() as u64 != artifact.encoded_bytes {
        return invalid("Parquet authority length disagrees with its artifact reference");
    }
    verify_checksum(&artifact.checksum, &bytes, role)?;
    let decoded = catch_unwind(AssertUnwindSafe(|| -> Result<Vec<RecordBatch>> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
        if builder.schema().as_ref() != expected_schema.as_ref() {
            return invalid("Parquet authority schema or nullability changed");
        }
        if builder.metadata().row_groups().iter().any(|group| {
            group
                .columns()
                .iter()
                .any(|column| column.file_path().is_some())
        }) {
            return invalid("external Parquet file_path is forbidden in authority artifacts");
        }
        let metadata_rows = builder.metadata().file_metadata().num_rows();
        let metadata_rows = usize::try_from(metadata_rows).map_err(|_| {
            BorsukError::InvalidStorage(format!("{role} footer row count is invalid"))
        })?;
        if max_rows == 0
            || metadata_rows == 0
            || metadata_rows > max_rows
            || builder.metadata().num_row_groups() != 1
        {
            return Err(BorsukError::InvalidStorage(format!(
                "{role} footer row count exceeds its explicit row cap"
            )));
        }
        Ok(builder
            .with_batch_size(max_rows)
            .build()?
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }))
    .map_err(|_| {
        BorsukError::InvalidStorage(format!("{role} panicked during Parquet interpretation"))
    })??;
    if decoded.len() != 1 || decoded[0].num_rows() > max_rows {
        return invalid("Parquet authority must decode as exactly one record batch");
    }
    Ok(decoded.into_iter().next().expect("checked one batch"))
}

fn artifact_ref_for_bytes(prefix: &str, bytes: &[u8]) -> Result<ArtifactRef> {
    if bytes.is_empty() || bytes.len() as u64 > FORMAT_MAX_AUTHORITY_OBJECT_BYTES {
        return invalid("Parquet authority exceeds the v1 format hard cap");
    }
    let checksum = blake3::hash(bytes).to_hex().to_string();
    Ok(ArtifactRef {
        path: format!("{prefix}/{checksum}.parquet"),
        checksum,
        encoded_bytes: bytes.len() as u64,
    })
}

fn emit_packed_artifact(
    prefix: &str,
    bytes: Vec<u8>,
    sink: &mut dyn RowBundleObjectSink,
) -> Result<PackedArtifact> {
    let reference = artifact_ref_for_bytes(prefix, &bytes)?;
    #[cfg(test)]
    let retained = bytes.clone();
    sink.emit(&reference, Bytes::from(bytes))?;
    Ok(PackedArtifact {
        reference,
        #[cfg(test)]
        bytes: retained,
    })
}

fn summary_shard_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("bundle_path", DataType::Utf8, false),
        Field::new("bundle_checksum", DataType::Utf8, false),
        Field::new("bundle_bytes", DataType::UInt64, false),
        Field::new("footer_offset", DataType::UInt64, false),
        Field::new("footer_length", DataType::UInt64, false),
        Field::new("footer_checksum", DataType::Utf8, false),
        Field::new("row_group", DataType::UInt32, false),
        Field::new("modality", DataType::Utf8, false),
        Field::new("projection_kind", DataType::UInt8, false),
        Field::new("assignment_kind", DataType::UInt8, false),
        Field::new("assignment_checksum", DataType::FixedSizeBinary(32), false),
        Field::new("routing_epoch", DataType::UInt64, true),
        Field::new("min_cell_ordinal", DataType::UInt32, true),
        Field::new("max_cell_ordinal", DataType::UInt32, true),
        Field::new("min_record_id", DataType::Binary, false),
        Field::new("max_record_id", DataType::Binary, false),
        Field::new("first_record_id", DataType::Binary, false),
        Field::new("last_record_id", DataType::Binary, false),
        Field::new("row_count", DataType::UInt64, false),
        Field::new("data_offset", DataType::UInt64, false),
        Field::new("data_length", DataType::UInt64, false),
        Field::new("data_checksum", DataType::Utf8, false),
        Field::new("bloom_offset", DataType::UInt64, true),
        Field::new("bloom_length", DataType::UInt64, true),
        Field::new("bloom_checksum", DataType::Utf8, true),
        Field::new(
            "page_index_offsets",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true))),
            false,
        ),
        Field::new(
            "page_index_lengths",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true))),
            false,
        ),
        Field::new(
            "page_index_checksums",
            DataType::List(Arc::new(Field::new_list_field(DataType::Binary, true))),
            false,
        ),
        Field::new("min_mutation_hlc", DataType::UInt64, false),
        Field::new("min_mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("min_mutation_digest", DataType::FixedSizeBinary(32), false),
        Field::new("max_mutation_hlc", DataType::UInt64, false),
        Field::new("max_mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("max_mutation_digest", DataType::FixedSizeBinary(32), false),
    ]))
}

fn encode_summary_shard(rows: &[(&RowBundleRef, &RowGroupSummary)]) -> Result<Vec<u8>> {
    let mut checksum_lists = ListBuilder::new(BinaryBuilder::new());
    for (_, summary) in rows {
        for range in &summary.page_indexes {
            checksum_lists
                .values()
                .append_value(range.checksum.as_bytes());
        }
        checksum_lists.append(true);
    }
    let batch = RecordBatch::try_new(
        summary_shard_schema(),
        vec![
            Arc::new(UInt16Array::from_iter_values(
                rows.iter().map(|_| ROW_BUNDLE_FORMAT_VERSION),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(bundle, _)| bundle.artifact.path.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|(bundle, _)| bundle.artifact.checksum.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|(bundle, _)| bundle.artifact.encoded_bytes),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|(bundle, _)| bundle.footer.offset),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|(bundle, _)| bundle.footer.length),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|(bundle, _)| bundle.footer.checksum.as_str()),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|(_, summary)| summary.row_group),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(_, summary)| summary.modality.as_str()),
            )),
            Arc::new(UInt8Array::from_iter_values(
                rows.iter().map(|(_, summary)| summary.projection_kind),
            )),
            Arc::new(UInt8Array::from_iter_values(
                rows.iter().map(|(_, summary)| summary.assignment_kind),
            )),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                rows.iter().map(|(_, summary)| summary.assignment_checksum),
            )?),
            Arc::new(UInt64Array::from_iter(
                rows.iter().map(|(_, summary)| summary.routing_epoch),
            )),
            Arc::new(UInt32Array::from_iter(
                rows.iter().map(|(_, summary)| summary.min_cell_ordinal),
            )),
            Arc::new(UInt32Array::from_iter(
                rows.iter().map(|(_, summary)| summary.max_cell_ordinal),
            )),
            Arc::new(BinaryArray::from_iter_values(
                rows.iter()
                    .map(|(_, summary)| summary.min_record_id.as_slice()),
            )),
            Arc::new(BinaryArray::from_iter_values(
                rows.iter()
                    .map(|(_, summary)| summary.max_record_id.as_slice()),
            )),
            Arc::new(BinaryArray::from_iter_values(
                rows.iter()
                    .map(|(_, summary)| summary.first_record_id.as_slice()),
            )),
            Arc::new(BinaryArray::from_iter_values(
                rows.iter()
                    .map(|(_, summary)| summary.last_record_id.as_slice()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|(_, summary)| summary.row_count),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|(_, summary)| summary.data.offset),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|(_, summary)| summary.data.length),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|(_, summary)| summary.data.checksum.as_str()),
            )),
            Arc::new(UInt64Array::from_iter(rows.iter().map(|(_, summary)| {
                summary.record_id_bloom.as_ref().map(|range| range.offset)
            }))),
            Arc::new(UInt64Array::from_iter(rows.iter().map(|(_, summary)| {
                summary.record_id_bloom.as_ref().map(|range| range.length)
            }))),
            Arc::new(StringArray::from_iter(rows.iter().map(|(_, summary)| {
                summary
                    .record_id_bloom
                    .as_ref()
                    .map(|range| range.checksum.as_str())
            }))),
            Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>(
                rows.iter().map(|(_, summary)| {
                    Some(
                        summary
                            .page_indexes
                            .iter()
                            .map(|range| Some(range.offset))
                            .collect::<Vec<_>>(),
                    )
                }),
            )),
            Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>(
                rows.iter().map(|(_, summary)| {
                    Some(
                        summary
                            .page_indexes
                            .iter()
                            .map(|range| Some(range.length))
                            .collect::<Vec<_>>(),
                    )
                }),
            )),
            Arc::new(checksum_lists.finish()),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter()
                    .map(|(_, summary)| summary.min_stamp.version().hlc()),
            )),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                rows.iter()
                    .map(|(_, summary)| summary.min_stamp.version().writer()),
            )?),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                rows.iter().map(|(_, summary)| summary.min_stamp.digest()),
            )?),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter()
                    .map(|(_, summary)| summary.max_stamp.version().hlc()),
            )),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                rows.iter()
                    .map(|(_, summary)| summary.max_stamp.version().writer()),
            )?),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                rows.iter().map(|(_, summary)| summary.max_stamp.digest()),
            )?),
        ],
    )?;
    write_parquet_batch(batch)
}

fn decoded_u64_list(array: &ListArray, row: usize, role: &str) -> Result<Vec<u64>> {
    if array.is_null(row) {
        return Err(BorsukError::InvalidStorage(format!("{role} list is null")));
    }
    let values = array.value(row);
    let values = values
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact list schema");
    if values.null_count() != 0 {
        return Err(BorsukError::InvalidStorage(format!(
            "{role} list contains nulls"
        )));
    }
    Ok(values.values().to_vec())
}

fn decoded_i32_list(array: &ListArray, row: usize, role: &str) -> Result<Vec<i32>> {
    if array.is_null(row) {
        return Err(BorsukError::InvalidStorage(format!("{role} list is null")));
    }
    let values = array.value(row);
    let values = values
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("exact list schema");
    if values.null_count() != 0 {
        return Err(BorsukError::InvalidStorage(format!(
            "{role} list contains nulls"
        )));
    }
    Ok(values.values().to_vec())
}

fn decoded_i64_list(array: &ListArray, row: usize, role: &str) -> Result<Vec<i64>> {
    if array.is_null(row) {
        return Err(BorsukError::InvalidStorage(format!("{role} list is null")));
    }
    let values = array.value(row);
    let values = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("exact list schema");
    if values.null_count() != 0 {
        return Err(BorsukError::InvalidStorage(format!(
            "{role} list contains nulls"
        )));
    }
    Ok(values.values().to_vec())
}

fn decoded_binary_list(array: &ListArray, row: usize, role: &str) -> Result<Vec<Vec<u8>>> {
    if array.is_null(row) {
        return Err(BorsukError::InvalidStorage(format!("{role} list is null")));
    }
    let values = array.value(row);
    let values = values
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("exact list schema");
    if values.null_count() != 0 {
        return Err(BorsukError::InvalidStorage(format!(
            "{role} list contains nulls"
        )));
    }
    Ok(values.iter().flatten().map(<[u8]>::to_vec).collect())
}

fn decoded_checksum_list(array: &ListArray, row: usize) -> Result<Vec<String>> {
    if array.is_null(row) {
        return invalid("page-index checksum list is null");
    }
    let values = array.value(row);
    let values = values
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("exact list schema");
    if values.null_count() != 0 {
        return invalid("page-index checksum list contains nulls");
    }
    values
        .iter()
        .map(|value| {
            let value = value.expect("checked non-null");
            std::str::from_utf8(value)
                .map(str::to_string)
                .map_err(|_| BorsukError::InvalidStorage("page-index checksum is not UTF-8".into()))
        })
        .collect()
}

fn summary_boundary(summary: &RowGroupSummary, first: bool) -> SummaryBoundary {
    SummaryBoundary {
        modality: summary.modality.clone(),
        projection_kind: summary.projection_kind,
        assignment_kind: summary.assignment_kind,
        assignment_checksum: summary.assignment_checksum,
        routing_epoch: summary.routing_epoch,
        cell_ordinal: if first {
            summary.min_cell_ordinal
        } else {
            summary.max_cell_ordinal
        },
        record_id: if first {
            summary.first_record_id.clone()
        } else {
            summary.last_record_id.clone()
        },
    }
}

fn validate_summary_shard_non_overlap(summaries: &[RowGroupSummary]) -> Result<()> {
    let mut previous_last = None::<SummaryBoundary>;
    for summary in summaries {
        let first = summary_boundary(summary, true);
        let last = summary_boundary(summary, false);
        if first > last
            || previous_last
                .as_ref()
                .is_some_and(|previous| previous >= &first)
        {
            return invalid("row-bundle summary rows overlap or reverse canonical bounds");
        }
        previous_last = Some(last);
    }
    Ok(())
}

pub(crate) fn decode_summary_shard(
    artifact: &ArtifactRef,
    bytes: Bytes,
) -> Result<Vec<RowBundleRef>> {
    let decoded = decode_exact_parquet_artifact(
        artifact,
        bytes,
        &summary_shard_schema(),
        "row-bundle summary shard",
        MAX_SUMMARY_ROWS_PER_SHARD,
    )?;
    if decoded.num_rows() == 0 {
        return invalid("row-bundle summary shard decoded no rows");
    }
    let required = [
        0_usize, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19, 20, 21, 22, 26, 27, 28, 29,
        30, 31, 32, 33, 34,
    ];
    if required
        .into_iter()
        .any(|column| decoded.column(column).null_count() != 0)
    {
        return invalid("row-bundle summary shard contains null required authority");
    }
    let format = decoded
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .expect("exact schema");
    let paths = decoded
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let checksums = decoded
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let bundle_bytes = decoded
        .column(3)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let footer_offsets = decoded
        .column(4)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let footer_lengths = decoded
        .column(5)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let footer_checksums = decoded
        .column(6)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let row_groups = decoded
        .column(7)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("exact schema");
    let modalities = decoded
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let projection = decoded
        .column(9)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("exact schema");
    let assignment = decoded
        .column(10)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("exact schema");
    let assignment_checksums = decoded
        .column(11)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("exact schema");
    let epochs = decoded
        .column(12)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let minimum_cells = decoded
        .column(13)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("exact schema");
    let maximum_cells = decoded
        .column(14)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("exact schema");
    let minimum_ids = decoded
        .column(15)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("exact schema");
    let maximum_ids = decoded
        .column(16)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("exact schema");
    let first_ids = decoded
        .column(17)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("exact schema");
    let last_ids = decoded
        .column(18)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("exact schema");
    let row_counts = decoded
        .column(19)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let data_offsets = decoded
        .column(20)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let data_lengths = decoded
        .column(21)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let data_checksums = decoded
        .column(22)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let bloom_offsets = decoded
        .column(23)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let bloom_lengths = decoded
        .column(24)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let bloom_checksums = decoded
        .column(25)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let page_offsets = decoded
        .column(26)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exact schema");
    let page_lengths = decoded
        .column(27)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exact schema");
    let page_checksums = decoded
        .column(28)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exact schema");
    let minimum_hlc = decoded
        .column(29)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let minimum_writers = decoded
        .column(30)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("exact schema");
    let minimum_digests = decoded
        .column(31)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("exact schema");
    let maximum_hlc = decoded
        .column(32)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let maximum_writers = decoded
        .column(33)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("exact schema");
    let maximum_digests = decoded
        .column(34)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("exact schema");

    let mut bundles = Vec::<RowBundleRef>::new();
    let mut bundle_positions = BTreeMap::<String, usize>::new();
    let mut ordered_summaries = Vec::with_capacity(decoded.num_rows());
    for row in 0..decoded.num_rows() {
        if format.value(row) != ROW_BUNDLE_FORMAT_VERSION || row_counts.value(row) == 0 {
            return invalid("row-bundle summary row has invalid format or count");
        }
        let route_nullable = (
            !epochs.is_null(row),
            !minimum_cells.is_null(row),
            !maximum_cells.is_null(row),
        );
        let assignment_kind = assignment.value(row);
        let valid_route = match assignment_kind {
            0 => route_nullable == (true, true, true) && epochs.value(row) > 0,
            1 => route_nullable == (false, false, false),
            _ => false,
        };
        let bloom_nullable = (
            !bloom_offsets.is_null(row),
            !bloom_lengths.is_null(row),
            !bloom_checksums.is_null(row),
        );
        if !valid_route || !matches!(bloom_nullable, (true, true, true) | (false, false, false)) {
            return invalid("row-bundle summary nullable assignment authority is inconsistent");
        }
        let page_offsets = decoded_u64_list(page_offsets, row, "page-index offset")?;
        let page_lengths = decoded_u64_list(page_lengths, row, "page-index length")?;
        let page_checksums = decoded_checksum_list(page_checksums, row)?;
        if page_offsets.len() != page_lengths.len() || page_offsets.len() != page_checksums.len() {
            return invalid("page-index authenticated range lists disagree");
        }
        let page_indexes = page_offsets
            .into_iter()
            .zip(page_lengths)
            .zip(page_checksums)
            .map(|((offset, length), checksum)| AuthenticatedRange {
                offset,
                length,
                checksum,
            })
            .collect::<Vec<_>>();
        let min_stamp = MutationStamp::new(
            MutationVersion::from_parts(
                minimum_hlc.value(row),
                minimum_writers.value(row).try_into().map_err(|_| {
                    BorsukError::InvalidStorage("minimum mutation writer width changed".into())
                })?,
            ),
            minimum_digests.value(row).try_into().map_err(|_| {
                BorsukError::InvalidStorage("minimum mutation digest width changed".into())
            })?,
        );
        let max_stamp = MutationStamp::new(
            MutationVersion::from_parts(
                maximum_hlc.value(row),
                maximum_writers.value(row).try_into().map_err(|_| {
                    BorsukError::InvalidStorage("maximum mutation writer width changed".into())
                })?,
            ),
            maximum_digests.value(row).try_into().map_err(|_| {
                BorsukError::InvalidStorage("maximum mutation digest width changed".into())
            })?,
        );
        let summary = RowGroupSummary {
            row_group: row_groups.value(row),
            modality: modalities.value(row).to_string(),
            projection_kind: projection.value(row),
            assignment_kind,
            assignment_checksum: assignment_checksums.value(row).try_into().map_err(|_| {
                BorsukError::InvalidStorage("summary assignment checksum width changed".into())
            })?,
            routing_epoch: route_nullable.0.then(|| epochs.value(row)),
            min_cell_ordinal: route_nullable.1.then(|| minimum_cells.value(row)),
            max_cell_ordinal: route_nullable.2.then(|| maximum_cells.value(row)),
            min_record_id: minimum_ids.value(row).to_vec(),
            max_record_id: maximum_ids.value(row).to_vec(),
            first_record_id: first_ids.value(row).to_vec(),
            last_record_id: last_ids.value(row).to_vec(),
            row_count: row_counts.value(row),
            data: AuthenticatedRange {
                offset: data_offsets.value(row),
                length: data_lengths.value(row),
                checksum: data_checksums.value(row).to_string(),
            },
            record_id_bloom: bloom_nullable.0.then(|| AuthenticatedRange {
                offset: bloom_offsets.value(row),
                length: bloom_lengths.value(row),
                checksum: bloom_checksums.value(row).to_string(),
            }),
            page_indexes,
            min_stamp,
            max_stamp,
        };
        let first = summary_boundary(&summary, true);
        let last = summary_boundary(&summary, false);
        validate_summary_boundary(&first)?;
        validate_summary_boundary(&last)?;
        if first > last
            || summary.min_record_id.is_empty()
            || summary.min_record_id > summary.max_record_id
            || summary.first_record_id < summary.min_record_id
            || summary.first_record_id > summary.max_record_id
            || summary.last_record_id < summary.min_record_id
            || summary.last_record_id > summary.max_record_id
            || summary
                .min_cell_ordinal
                .zip(summary.max_cell_ordinal)
                .is_some_and(|(minimum, maximum)| minimum > maximum)
            || summary.min_stamp.version() > summary.max_stamp.version()
        {
            return invalid("row-bundle summary bounds or ordering are invalid");
        }
        ordered_summaries.push(summary.clone());
        let bundle_ref = RowBundleRef {
            artifact: ArtifactRef {
                path: paths.value(row).to_string(),
                checksum: checksums.value(row).to_string(),
                encoded_bytes: bundle_bytes.value(row),
            },
            footer: AuthenticatedRange {
                offset: footer_offsets.value(row),
                length: footer_lengths.value(row),
                checksum: footer_checksums.value(row).to_string(),
            },
            row_groups: Vec::new(),
        };
        bundle_ref.artifact.validate(".parquet")?;
        bundle_ref
            .footer
            .checked(bundle_ref.artifact.encoded_bytes)?;
        summary.data.checked(bundle_ref.artifact.encoded_bytes)?;
        if let Some(bloom) = &summary.record_id_bloom {
            bloom.checked(bundle_ref.artifact.encoded_bytes)?;
        }
        for range in &summary.page_indexes {
            range.checked(bundle_ref.artifact.encoded_bytes)?;
        }
        let position = if let Some(position) = bundle_positions.get(&bundle_ref.artifact.path) {
            *position
        } else {
            let position = bundles.len();
            bundle_positions.insert(bundle_ref.artifact.path.clone(), position);
            bundles.push(bundle_ref.clone());
            position
        };
        let existing = &mut bundles[position];
        if existing.artifact != bundle_ref.artifact || existing.footer != bundle_ref.footer {
            return invalid("summary shard repeats inconsistent bundle authority");
        }
        if existing
            .row_groups
            .last()
            .is_some_and(|prior| prior.row_group >= summary.row_group)
        {
            return invalid("summary shard repeats or reorders a bundle row group");
        }
        existing.row_groups.push(summary);
    }
    validate_summary_shard_non_overlap(&ordered_summaries)?;
    for bundle in &bundles {
        validate_row_bundle_reference(bundle)?;
    }
    Ok(bundles)
}

fn summary_root_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("first_modality", DataType::Utf8, false),
        Field::new("first_projection_kind", DataType::UInt8, false),
        Field::new("first_assignment_kind", DataType::UInt8, false),
        Field::new(
            "first_assignment_checksum",
            DataType::FixedSizeBinary(32),
            false,
        ),
        Field::new("first_routing_epoch", DataType::UInt64, true),
        Field::new("first_cell_ordinal", DataType::UInt32, true),
        Field::new("first_record_id", DataType::Binary, false),
        Field::new("last_modality", DataType::Utf8, false),
        Field::new("last_projection_kind", DataType::UInt8, false),
        Field::new("last_assignment_kind", DataType::UInt8, false),
        Field::new(
            "last_assignment_checksum",
            DataType::FixedSizeBinary(32),
            false,
        ),
        Field::new("last_routing_epoch", DataType::UInt64, true),
        Field::new("last_cell_ordinal", DataType::UInt32, true),
        Field::new("last_record_id", DataType::Binary, false),
        Field::new("shard_path", DataType::Utf8, false),
        Field::new("shard_checksum", DataType::Utf8, false),
        Field::new("shard_rows", DataType::UInt64, false),
        Field::new("shard_bytes", DataType::UInt64, false),
    ]))
}

fn encode_summary_root(
    shards: &[PackedArtifact],
    summaries: &[(&RowBundleRef, &RowGroupSummary)],
    rows_per_shard: usize,
) -> Result<Vec<u8>> {
    let first = summaries.chunks(rows_per_shard).map(|chunk| chunk[0].1);
    let last = summaries
        .chunks(rows_per_shard)
        .map(|chunk| chunk[chunk.len() - 1].1);
    let first = first.collect::<Vec<_>>();
    let last = last.collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        summary_root_schema(),
        vec![
            Arc::new(UInt16Array::from_iter_values(
                shards.iter().map(|_| ROW_BUNDLE_FORMAT_VERSION),
            )),
            Arc::new(StringArray::from_iter_values(
                first.iter().map(|summary| summary.modality.as_str()),
            )),
            Arc::new(UInt8Array::from_iter_values(
                first.iter().map(|summary| summary.projection_kind),
            )),
            Arc::new(UInt8Array::from_iter_values(
                first.iter().map(|summary| summary.assignment_kind),
            )),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                first.iter().map(|summary| summary.assignment_checksum),
            )?),
            Arc::new(UInt64Array::from_iter(
                first.iter().map(|summary| summary.routing_epoch),
            )),
            Arc::new(UInt32Array::from_iter(
                first.iter().map(|summary| summary.min_cell_ordinal),
            )),
            Arc::new(BinaryArray::from_iter_values(
                first
                    .iter()
                    .map(|summary| summary.first_record_id.as_slice()),
            )),
            Arc::new(StringArray::from_iter_values(
                last.iter().map(|summary| summary.modality.as_str()),
            )),
            Arc::new(UInt8Array::from_iter_values(
                last.iter().map(|summary| summary.projection_kind),
            )),
            Arc::new(UInt8Array::from_iter_values(
                last.iter().map(|summary| summary.assignment_kind),
            )),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                last.iter().map(|summary| summary.assignment_checksum),
            )?),
            Arc::new(UInt64Array::from_iter(
                last.iter().map(|summary| summary.routing_epoch),
            )),
            Arc::new(UInt32Array::from_iter(
                last.iter().map(|summary| summary.max_cell_ordinal),
            )),
            Arc::new(BinaryArray::from_iter_values(
                last.iter().map(|summary| summary.last_record_id.as_slice()),
            )),
            Arc::new(StringArray::from_iter_values(
                shards.iter().map(|shard| shard.reference.path.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                shards.iter().map(|shard| shard.reference.checksum.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                summaries
                    .chunks(rows_per_shard)
                    .map(|chunk| chunk.len() as u64),
            )),
            Arc::new(UInt64Array::from_iter_values(
                shards.iter().map(|shard| shard.reference.encoded_bytes),
            )),
        ],
    )?;
    write_parquet_batch(batch)
}

fn validate_summary_boundary(boundary: &SummaryBoundary) -> Result<()> {
    let valid_assignment = match boundary.assignment_kind {
        0 => {
            boundary.routing_epoch.is_some_and(|epoch| epoch > 0) && boundary.cell_ordinal.is_some()
        }
        1 => boundary.routing_epoch.is_none() && boundary.cell_ordinal.is_none(),
        _ => false,
    };
    if boundary.modality.is_empty()
        || boundary.projection_kind > 4
        || boundary.assignment_checksum == [0; 32]
        || boundary.record_id.is_empty()
        || !valid_assignment
    {
        return invalid("row-bundle summary boundary is empty or has invalid assignment authority");
    }
    Ok(())
}

pub(crate) fn decode_summary_root(
    artifact: &ArtifactRef,
    bytes: Bytes,
) -> Result<Vec<SummaryShardRef>> {
    let decoded = decode_exact_parquet_artifact(
        artifact,
        bytes,
        &summary_root_schema(),
        "row-bundle run summary root",
        MAX_SUMMARY_ROOT_SHARDS,
    )?;
    if decoded.num_rows() == 0 {
        return invalid("row-bundle run summary root decoded no shards");
    }
    let required = [0_usize, 1, 2, 3, 4, 7, 8, 9, 10, 11, 14, 15, 16, 17, 18];
    if required
        .into_iter()
        .any(|column| decoded.column(column).null_count() != 0)
    {
        return invalid("row-bundle run summary root contains null required authority");
    }
    let format = decoded
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .expect("exact schema");
    let first_modalities = decoded
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let first_projection = decoded
        .column(2)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("exact schema");
    let first_assignment = decoded
        .column(3)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("exact schema");
    let first_checksums = decoded
        .column(4)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("exact schema");
    let first_epochs = decoded
        .column(5)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let first_cells = decoded
        .column(6)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("exact schema");
    let first_ids = decoded
        .column(7)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("exact schema");
    let last_modalities = decoded
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let last_projection = decoded
        .column(9)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("exact schema");
    let last_assignment = decoded
        .column(10)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("exact schema");
    let last_checksums = decoded
        .column(11)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("exact schema");
    let last_epochs = decoded
        .column(12)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let last_cells = decoded
        .column(13)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("exact schema");
    let last_ids = decoded
        .column(14)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("exact schema");
    let paths = decoded
        .column(15)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let checksums = decoded
        .column(16)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let row_counts = decoded
        .column(17)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let encoded_bytes = decoded
        .column(18)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let mut shards = Vec::with_capacity(decoded.num_rows());
    for row in 0..decoded.num_rows() {
        if format.value(row) != ROW_BUNDLE_FORMAT_VERSION || row_counts.value(row) == 0 {
            return invalid("row-bundle summary root has invalid format or row count");
        }
        let first = SummaryBoundary {
            modality: first_modalities.value(row).to_string(),
            projection_kind: first_projection.value(row),
            assignment_kind: first_assignment.value(row),
            assignment_checksum: first_checksums.value(row).try_into().map_err(|_| {
                BorsukError::InvalidStorage("first summary checksum width changed".into())
            })?,
            routing_epoch: (!first_epochs.is_null(row)).then(|| first_epochs.value(row)),
            cell_ordinal: (!first_cells.is_null(row)).then(|| first_cells.value(row)),
            record_id: first_ids.value(row).to_vec(),
        };
        let last = SummaryBoundary {
            modality: last_modalities.value(row).to_string(),
            projection_kind: last_projection.value(row),
            assignment_kind: last_assignment.value(row),
            assignment_checksum: last_checksums.value(row).try_into().map_err(|_| {
                BorsukError::InvalidStorage("last summary checksum width changed".into())
            })?,
            routing_epoch: (!last_epochs.is_null(row)).then(|| last_epochs.value(row)),
            cell_ordinal: (!last_cells.is_null(row)).then(|| last_cells.value(row)),
            record_id: last_ids.value(row).to_vec(),
        };
        validate_summary_boundary(&first)?;
        validate_summary_boundary(&last)?;
        if first > last {
            return invalid("row-bundle summary shard bounds are reversed");
        }
        let shard = SummaryShardRef {
            first,
            last,
            artifact: ArtifactRef {
                path: paths.value(row).to_string(),
                checksum: checksums.value(row).to_string(),
                encoded_bytes: encoded_bytes.value(row),
            },
            row_count: row_counts.value(row),
        };
        validate_authority_artifact(&shard.artifact, "row-bundle summary shard")?;
        shards.push(shard);
    }
    validate_summary_root_non_overlap(&shards)?;
    Ok(shards)
}

fn validate_summary_root_non_overlap(shards: &[SummaryShardRef]) -> Result<()> {
    if shards.is_empty()
        || shards.iter().any(|shard| shard.first > shard.last)
        || shards.windows(2).any(|pair| pair[0].last >= pair[1].first)
    {
        return invalid("row-bundle summary root shard bounds overlap or reverse");
    }
    Ok(())
}

fn roster_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("checksum", DataType::Utf8, false),
        Field::new("encoded_bytes", DataType::UInt64, false),
    ]))
}

fn encode_roster(entries: &[RosterEntry]) -> Result<Vec<u8>> {
    let batch = RecordBatch::try_new(
        roster_schema(),
        vec![
            Arc::new(UInt16Array::from_iter_values(
                entries.iter().map(|_| ROW_BUNDLE_FORMAT_VERSION),
            )),
            Arc::new(StringArray::from_iter_values(
                entries.iter().map(|entry| entry.role.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                entries.iter().map(|entry| entry.path.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                entries.iter().map(|entry| entry.checksum.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                entries.iter().map(|entry| entry.encoded_bytes),
            )),
        ],
    )?;
    write_parquet_batch(batch)
}

pub(crate) fn decode_roster(artifact: &ArtifactRef, bytes: Bytes) -> Result<Vec<RosterEntry>> {
    let decoded = decode_exact_parquet_artifact(
        artifact,
        bytes,
        &roster_schema(),
        "row-bundle run roster",
        MAX_ROSTER_ROWS,
    )?;
    if decoded.num_rows() == 0
        || (0..decoded.num_columns()).any(|column| decoded.column(column).null_count() != 0)
    {
        return invalid("row-bundle run roster is empty or contains null authority");
    }
    let format = decoded
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .expect("exact schema");
    let roles = decoded
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let paths = decoded
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let checksums = decoded
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let encoded_bytes = decoded
        .column(4)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let mut entries = Vec::with_capacity(decoded.num_rows());
    for row in 0..decoded.num_rows() {
        if format.value(row) != ROW_BUNDLE_FORMAT_VERSION {
            return invalid("row-bundle roster format marker is unsupported");
        }
        let role = match roles.value(row) {
            "row_bundle" => RowBundleRole::Bundle,
            "row_bundle_summary_shard" => RowBundleRole::SummaryShard,
            "row_bundle_summary_root" => RowBundleRole::SummaryRoot,
            _ => return invalid("row-bundle roster contains an unknown role"),
        };
        let reference = ArtifactRef {
            path: paths.value(row).to_string(),
            checksum: checksums.value(row).to_string(),
            encoded_bytes: encoded_bytes.value(row),
        };
        reference.validate(".parquet")?;
        entries.push(RosterEntry {
            role,
            path: reference.path,
            checksum: reference.checksum,
            encoded_bytes: reference.encoded_bytes,
        });
    }
    if entries
        .windows(2)
        .any(|pair| (pair[0].role, pair[0].path.as_str()) >= (pair[1].role, pair[1].path.as_str()))
    {
        return invalid("row-bundle roster is not uniquely ordered by role and path");
    }
    Ok(entries)
}

fn build_packed_run(
    bundles: Vec<PackedRowBundle>,
    target_level: u8,
    options: RowBundlePackOptions,
    mut peak_staged_bytes: u64,
    sink: &mut dyn RowBundleObjectSink,
) -> Result<PackedRowBundleRun> {
    if bundles.is_empty() || bundles.len() > MAX_RUN_BUNDLES {
        return invalid("row-bundle run has no bundles or exceeds its positioned row bound");
    }
    let summaries = bundles
        .iter()
        .flat_map(|bundle| {
            bundle
                .reference
                .row_groups
                .iter()
                .map(|summary| (&bundle.reference, summary))
        })
        .collect::<Vec<_>>();
    if summaries.is_empty() || summaries.len() > MAX_RUN_SUMMARIES {
        return invalid("row-bundle run has no summaries or exceeds its positioned row bound");
    }
    let mut summary_shards = Vec::new();
    for rows in summaries.chunks(options.summary_rows_per_shard) {
        let bytes = encode_summary_shard(rows)?;
        peak_staged_bytes = peak_staged_bytes.max(bytes.len() as u64);
        let artifact = emit_packed_artifact("row-bundle-summary-shards", bytes, sink)?;
        summary_shards.push(artifact);
    }
    if summary_shards.len() > MAX_SUMMARY_ROOT_SHARDS {
        return invalid("row-bundle run exceeds its summary-root shard bound");
    }
    let root_bytes =
        encode_summary_root(&summary_shards, &summaries, options.summary_rows_per_shard)?;
    peak_staged_bytes = peak_staged_bytes.max(root_bytes.len() as u64);
    let root = emit_packed_artifact("row-bundle-run-roots", root_bytes, sink)?;
    let mut entries = bundles
        .iter()
        .map(|bundle| RosterEntry {
            role: RowBundleRole::Bundle,
            path: bundle.reference.artifact.path.clone(),
            checksum: bundle.reference.artifact.checksum.clone(),
            encoded_bytes: bundle.reference.artifact.encoded_bytes,
        })
        .chain(summary_shards.iter().map(|shard| RosterEntry {
            role: RowBundleRole::SummaryShard,
            path: shard.reference.path.clone(),
            checksum: shard.reference.checksum.clone(),
            encoded_bytes: shard.reference.encoded_bytes,
        }))
        .collect::<Vec<_>>();
    entries.push(RosterEntry {
        role: RowBundleRole::SummaryRoot,
        path: root.reference.path.clone(),
        checksum: root.reference.checksum.clone(),
        encoded_bytes: root.reference.encoded_bytes,
    });
    if entries.len() > MAX_ROSTER_ROWS {
        return invalid("row-bundle run roster exceeds its positioned artifact bound");
    }
    entries.sort_by(|left, right| {
        (left.role, left.path.as_str()).cmp(&(right.role, right.path.as_str()))
    });
    let mut role_bytes = BTreeMap::<RowBundleRole, u64>::new();
    for entry in &entries {
        *role_bytes.entry(entry.role).or_default() = role_bytes
            .get(&entry.role)
            .copied()
            .unwrap_or_default()
            .checked_add(entry.encoded_bytes)
            .ok_or_else(|| BorsukError::InvalidStorage("roster role bytes overflow".into()))?;
    }
    let roster_bytes = encode_roster(&entries)?;
    peak_staged_bytes = peak_staged_bytes.max(roster_bytes.len() as u64);
    let roster_artifact = emit_packed_artifact("row-bundle-rosters", roster_bytes, sink)?;
    let roster = PackedRoster {
        #[cfg(test)]
        bytes: roster_artifact.bytes.clone(),
        artifacts: entries,
        role_bytes: role_bytes.clone(),
        reference: roster_artifact.reference.clone(),
    };
    let run_ref = RowBundleRunRef {
        level: target_level,
        summary_root: root.reference.clone(),
        roster: roster.reference.clone(),
        bundle_count: bundles.len() as u64,
        summary_count: summaries.len() as u64,
        role_bytes: role_bytes
            .into_iter()
            .map(|(role, bytes)| (role.as_str().to_string(), bytes))
            .collect(),
    };
    Ok(PackedRowBundleRun {
        bundles,
        summary_shards,
        root,
        roster,
        run_ref,
        metrics: ConstructionMetrics {
            peak_staged_bytes,
            ..ConstructionMetrics::default()
        },
    })
}

fn generation_root_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("directory_root_path", DataType::Utf8, false),
        Field::new("directory_root_checksum", DataType::Utf8, false),
        Field::new("directory_root_bytes", DataType::UInt64, false),
        Field::new("level", DataType::UInt8, false),
        Field::new("summary_root_path", DataType::Utf8, false),
        Field::new("summary_root_checksum", DataType::Utf8, false),
        Field::new("summary_root_bytes", DataType::UInt64, false),
        Field::new("roster_path", DataType::Utf8, false),
        Field::new("roster_checksum", DataType::Utf8, false),
        Field::new("roster_bytes", DataType::UInt64, false),
        Field::new("bundle_count", DataType::UInt64, false),
        Field::new("summary_count", DataType::UInt64, false),
        // Exact role totals cover artifacts listed by the run roster. The
        // roster object itself is intentionally excluded to avoid self-reference.
        Field::new("row_bundle_bytes", DataType::UInt64, false),
        Field::new("summary_shard_bytes", DataType::UInt64, false),
        Field::new("summary_root_role_bytes", DataType::UInt64, false),
    ]))
}

fn validate_authority_artifact(artifact: &ArtifactRef, role: &str) -> Result<()> {
    artifact.validate(".parquet")?;
    if artifact.encoded_bytes > FORMAT_MAX_AUTHORITY_OBJECT_BYTES {
        return Err(BorsukError::InvalidStorage(format!(
            "{role} exceeds the v1 format hard cap"
        )));
    }
    Ok(())
}

fn validate_row_bundle_run_ref(run: &RowBundleRunRef) -> Result<()> {
    if usize::from(run.level) >= MAX_ACTIVE_ROW_BUNDLE_LEVELS
        || run.bundle_count == 0
        || run.bundle_count > MAX_RUN_BUNDLES as u64
        || run.summary_count == 0
        || run.summary_count > MAX_RUN_SUMMARIES as u64
        || run.summary_count
            > run.bundle_count.checked_mul(16).ok_or_else(|| {
                BorsukError::InvalidStorage("row-bundle run summary count overflows".into())
            })?
    {
        return invalid("row-bundle run reference has invalid level or counts");
    }
    validate_authority_artifact(&run.summary_root, "row-bundle summary root")?;
    validate_authority_artifact(&run.roster, "row-bundle roster")?;
    let expected_roles = [
        "row_bundle",
        "row_bundle_summary_root",
        "row_bundle_summary_shard",
    ];
    if run.role_bytes.len() != expected_roles.len()
        || expected_roles.iter().any(|role| {
            run.role_bytes
                .get(*role)
                .is_none_or(|encoded_bytes| *encoded_bytes == 0)
        })
        || run.role_bytes.get("row_bundle_summary_root") != Some(&run.summary_root.encoded_bytes)
    {
        return invalid("row-bundle run reference has invalid role-byte authority");
    }
    run.role_bytes
        .values()
        .try_fold(0_u64, |sum, value| sum.checked_add(*value))
        .ok_or_else(|| BorsukError::InvalidStorage("generation role bytes overflow".into()))?;
    Ok(())
}

fn encode_generation_root(
    runs: &[RowBundleRunRef],
    directory_root: &ArtifactRef,
) -> Result<Vec<u8>> {
    let batch = RecordBatch::try_new(
        generation_root_schema(),
        vec![
            Arc::new(UInt16Array::from_iter_values(
                runs.iter().map(|_| ROW_BUNDLE_FORMAT_VERSION),
            )),
            Arc::new(StringArray::from_iter_values(
                runs.iter().map(|_| directory_root.path.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                runs.iter().map(|_| directory_root.checksum.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                runs.iter().map(|_| directory_root.encoded_bytes),
            )),
            Arc::new(UInt8Array::from_iter_values(
                runs.iter().map(|run| run.level),
            )),
            Arc::new(StringArray::from_iter_values(
                runs.iter().map(|run| run.summary_root.path.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                runs.iter().map(|run| run.summary_root.checksum.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                runs.iter().map(|run| run.summary_root.encoded_bytes),
            )),
            Arc::new(StringArray::from_iter_values(
                runs.iter().map(|run| run.roster.path.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                runs.iter().map(|run| run.roster.checksum.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                runs.iter().map(|run| run.roster.encoded_bytes),
            )),
            Arc::new(UInt64Array::from_iter_values(
                runs.iter().map(|run| run.bundle_count),
            )),
            Arc::new(UInt64Array::from_iter_values(
                runs.iter().map(|run| run.summary_count),
            )),
            Arc::new(UInt64Array::from_iter_values(runs.iter().map(|run| {
                run.role_bytes
                    .get("row_bundle")
                    .copied()
                    .unwrap_or_default()
            }))),
            Arc::new(UInt64Array::from_iter_values(runs.iter().map(|run| {
                run.role_bytes
                    .get("row_bundle_summary_shard")
                    .copied()
                    .unwrap_or_default()
            }))),
            Arc::new(UInt64Array::from_iter_values(runs.iter().map(|run| {
                run.role_bytes
                    .get("row_bundle_summary_root")
                    .copied()
                    .unwrap_or_default()
            }))),
        ],
    )?;
    write_parquet_batch(batch)
}

pub(crate) fn decode_generation_root(
    artifact: &ArtifactRef,
    bytes: Bytes,
) -> Result<RowBundleGenerationRef> {
    let decoded = decode_exact_parquet_artifact(
        artifact,
        bytes,
        &generation_root_schema(),
        "row-bundle generation root",
        MAX_ACTIVE_ROW_BUNDLE_LEVELS,
    )?;
    if decoded.num_rows() == 0 || decoded.num_rows() > MAX_ACTIVE_ROW_BUNDLE_LEVELS {
        return invalid("row-bundle generation root has no runs or exceeds its active level cap");
    }
    if (0..decoded.num_columns()).any(|column| decoded.column(column).null_count() != 0) {
        return invalid("row-bundle generation root contains null authority");
    }
    let format = decoded
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .expect("exact schema");
    let levels = decoded
        .column(4)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("exact schema");
    let directory_paths = decoded
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let directory_checksums = decoded
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let directory_bytes = decoded
        .column(3)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let root_paths = decoded
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let root_checksums = decoded
        .column(6)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let root_bytes = decoded
        .column(7)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let roster_paths = decoded
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let roster_checksums = decoded
        .column(9)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let roster_bytes = decoded
        .column(10)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let bundle_counts = decoded
        .column(11)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let summary_counts = decoded
        .column(12)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let bundle_role_bytes = decoded
        .column(13)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let shard_role_bytes = decoded
        .column(14)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let root_role_bytes = decoded
        .column(15)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let mut runs = Vec::with_capacity(decoded.num_rows());
    for row in 0..decoded.num_rows() {
        if format.value(row) != ROW_BUNDLE_FORMAT_VERSION
            || bundle_counts.value(row) == 0
            || bundle_counts.value(row) > MAX_RUN_BUNDLES as u64
            || summary_counts.value(row) == 0
            || summary_counts.value(row) > MAX_RUN_SUMMARIES as u64
            || summary_counts.value(row)
                > bundle_counts.value(row).checked_mul(16).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "generation root bundle summary count overflows".into(),
                    )
                })?
        {
            return invalid("row-bundle generation root has invalid counts or format");
        }
        let role_bytes = BTreeMap::from([
            ("row_bundle".to_string(), bundle_role_bytes.value(row)),
            (
                "row_bundle_summary_shard".to_string(),
                shard_role_bytes.value(row),
            ),
            (
                "row_bundle_summary_root".to_string(),
                root_role_bytes.value(row),
            ),
        ]);
        role_bytes
            .values()
            .try_fold(0_u64, |sum, value| sum.checked_add(*value))
            .ok_or_else(|| BorsukError::InvalidStorage("generation role bytes overflow".into()))?;
        let run = RowBundleRunRef {
            level: levels.value(row),
            summary_root: ArtifactRef {
                path: root_paths.value(row).to_string(),
                checksum: root_checksums.value(row).to_string(),
                encoded_bytes: root_bytes.value(row),
            },
            roster: ArtifactRef {
                path: roster_paths.value(row).to_string(),
                checksum: roster_checksums.value(row).to_string(),
                encoded_bytes: roster_bytes.value(row),
            },
            bundle_count: bundle_counts.value(row),
            summary_count: summary_counts.value(row),
            role_bytes,
        };
        validate_row_bundle_run_ref(&run)?;
        runs.push(run);
    }
    if runs.windows(2).any(|pair| pair[0].level >= pair[1].level) {
        return invalid("row-bundle generation root levels are not strictly ordered");
    }
    let directory_root = ArtifactRef {
        path: directory_paths.value(0).to_string(),
        checksum: directory_checksums.value(0).to_string(),
        encoded_bytes: directory_bytes.value(0),
    };
    validate_authority_artifact(&directory_root, "ID-directory root")?;
    for row in 1..decoded.num_rows() {
        if directory_paths.value(row) != directory_root.path
            || directory_checksums.value(row) != directory_root.checksum
            || directory_bytes.value(row) != directory_root.encoded_bytes
        {
            return invalid("generation rows disagree on authenticated ID-directory root");
        }
    }
    Ok(RowBundleGenerationRef {
        active_runs: runs,
        directory_root,
    })
}

pub(crate) fn stage_row_bundle_generation_to_sink(
    active: &[RowBundleRunRef],
    new_run: &RowBundleRunRef,
    directory_root: &ArtifactRef,
    sink: &mut dyn RowBundleObjectSink,
) -> Result<StagedRowBundleGeneration> {
    if active.len() >= MAX_ACTIVE_ROW_BUNDLE_LEVELS {
        return invalid("row-bundle generation exceeds its active level cap");
    }
    let mut runs = Vec::with_capacity(active.len() + 1);
    runs.push(new_run.clone());
    runs.extend_from_slice(active);
    runs.sort_by_key(|run| run.level);
    if runs.windows(2).any(|pair| pair[0].level == pair[1].level) {
        return invalid("row-bundle generation repeats one active level");
    }
    for run in &runs {
        validate_row_bundle_run_ref(run)?;
    }
    validate_authority_artifact(directory_root, "ID-directory root")?;
    let root = emit_packed_artifact(
        "row-bundle-generation-roots",
        encode_generation_root(&runs, directory_root)?,
        sink,
    )?;
    Ok(StagedRowBundleGeneration {
        active_runs: runs,
        directory_root: directory_root.clone(),
        metrics: ConstructionMetrics {
            peak_staged_bytes: root.reference.encoded_bytes,
            directory_writes: 0,
        },
        root,
    })
}

#[cfg(test)]
fn stage_row_bundle_generation(
    active: &[RowBundleRunRef],
    new_run: &RowBundleRunRef,
    directory_root: &ArtifactRef,
) -> Result<StagedRowBundleGeneration> {
    let mut sink = CollectingRowBundleSink::default();
    stage_row_bundle_generation_to_sink(active, new_run, directory_root, &mut sink)
}

/// Authenticates all immutable recovery roots once. The returned handle owns
/// the verified root authorities, so point lookup never depends on a cache and
/// never refetches run rosters or summary roots.
pub(crate) fn open_row_bundle_generation<F>(
    generation_root: &ArtifactRef,
    generation_bytes: Bytes,
    mut fetch_objects: F,
) -> Result<OpenedRowBundleGeneration>
where
    F: FnMut(&[ArtifactRef]) -> Result<Vec<Bytes>>,
{
    let generation = decode_generation_root(generation_root, generation_bytes)?;
    let mut requests = Vec::with_capacity(1 + generation.active_runs.len() * 2);
    requests.push(generation.directory_root.clone());
    for run in &generation.active_runs {
        requests.push(run.summary_root.clone());
        requests.push(run.roster.clone());
    }
    let fetched = fetch_objects(&requests)?;
    if fetched.len() != requests.len() {
        return invalid("batched root fetch returned the wrong object count");
    }
    let mut fetched = fetched.into_iter();
    let directory_runs = decode_directory_root(
        &generation.directory_root,
        fetched.next().expect("request count checked"),
    )?;
    let mut active_runs = Vec::with_capacity(generation.active_runs.len());
    for run in &generation.active_runs {
        let shards = decode_summary_root(
            &run.summary_root,
            fetched.next().expect("request count checked"),
        )?;
        if shards.iter().map(|shard| shard.row_count).sum::<u64>() != run.summary_count {
            return invalid("row-bundle summary root totals disagree with generation authority");
        }
        let roster = decode_roster(&run.roster, fetched.next().expect("request count checked"))?;
        validate_roster_for_run(run, &roster)?;
        for shard in &shards {
            if !roster.iter().any(|entry| {
                entry.role == RowBundleRole::SummaryShard
                    && entry.path == shard.artifact.path
                    && entry.checksum == shard.artifact.checksum
                    && entry.encoded_bytes == shard.artifact.encoded_bytes
            }) {
                return invalid("row-bundle summary root references an unrostered shard");
            }
        }
        active_runs.push(OpenedRowBundleRun {
            run: run.clone(),
            roster,
            shards,
        });
    }
    Ok(OpenedRowBundleGeneration {
        generation,
        active_runs,
        directory_runs,
    })
}

fn directory_root_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("partition", DataType::UInt8, false),
        Field::new("level", DataType::UInt8, false),
        Field::new("run_path", DataType::Utf8, false),
        Field::new("run_checksum", DataType::Utf8, false),
        Field::new("run_bytes", DataType::UInt64, false),
        Field::new("footer_offset", DataType::UInt64, false),
        Field::new("footer_length", DataType::UInt64, false),
        Field::new("footer_checksum", DataType::Utf8, false),
        Field::new(
            "batch_min_record_ids",
            DataType::List(Arc::new(Field::new_list_field(DataType::Binary, true))),
            false,
        ),
        Field::new(
            "batch_max_record_ids",
            DataType::List(Arc::new(Field::new_list_field(DataType::Binary, true))),
            false,
        ),
        Field::new(
            "batch_row_counts",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true))),
            false,
        ),
        Field::new(
            "batch_offsets",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true))),
            false,
        ),
        Field::new(
            "batch_lengths",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true))),
            false,
        ),
        Field::new(
            "batch_checksums",
            DataType::List(Arc::new(Field::new_list_field(DataType::Binary, true))),
            false,
        ),
        Field::new(
            "batch_metadata_lengths",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            false,
        ),
        Field::new(
            "batch_body_lengths",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            false,
        ),
    ]))
}

fn validate_directory_run_refs(runs: &[DirectoryRunRef]) -> Result<()> {
    if runs.is_empty() || runs.len() > MAX_DIRECTORY_ROOT_RUNS {
        return invalid("ID-directory root has no runs or exceeds its fixed active-run bound");
    }
    let mut seen = BTreeSet::new();
    for run in runs {
        if usize::from(run.level) >= MAX_ACTIVE_DIRECTORY_LEVELS
            || !seen.insert((run.partition, run.level))
            || run.batches.is_empty()
        {
            return invalid("ID-directory root repeats or exceeds one partition level");
        }
        run.artifact.validate(".arrow")?;
        if run.artifact.encoded_bytes > FORMAT_MAX_DIRECTORY_OBJECT_BYTES {
            return invalid("ID-directory run exceeds the v1 object format hard cap");
        }
        run.footer.checked(run.artifact.encoded_bytes)?;
        let mut previous = None::<&DirectoryBatchRef>;
        for batch in &run.batches {
            batch.range.checked(run.artifact.encoded_bytes)?;
            if batch.row_count == 0
                || batch.row_count > MAX_RUN_ROWS as u64
                || batch.range.length > FORMAT_MAX_DIRECTORY_BATCH_BYTES
                || batch.metadata_length <= 0
                || batch.body_length < 0
                || batch.min_record_id.is_empty()
                || batch.min_record_id > batch.max_record_id
                || previous.is_some_and(|left| {
                    left.max_record_id >= batch.min_record_id
                        || left.range.offset >= batch.range.offset
                })
            {
                return invalid("ID-directory root has invalid or overlapping batch authority");
            }
            previous = Some(batch);
        }
    }
    Ok(())
}

pub(crate) fn pack_directory_root(runs: &[DirectoryRunRef]) -> Result<PackedDirectoryRoot> {
    validate_directory_run_refs(runs)?;
    let mut runs = runs.to_vec();
    runs.sort_by_key(|run| (run.partition, run.level));
    let mut minimum_ids = ListBuilder::new(BinaryBuilder::new());
    let mut maximum_ids = ListBuilder::new(BinaryBuilder::new());
    let mut checksums = ListBuilder::new(BinaryBuilder::new());
    for run in &runs {
        for batch in &run.batches {
            minimum_ids.values().append_value(&batch.min_record_id);
            maximum_ids.values().append_value(&batch.max_record_id);
            checksums
                .values()
                .append_value(batch.range.checksum.as_bytes());
        }
        minimum_ids.append(true);
        maximum_ids.append(true);
        checksums.append(true);
    }
    let batch = RecordBatch::try_new(
        directory_root_schema(),
        vec![
            Arc::new(UInt16Array::from_iter_values(
                runs.iter().map(|_| DIRECTORY_FORMAT_VERSION),
            )),
            Arc::new(UInt8Array::from_iter_values(
                runs.iter().map(|run| run.partition.0),
            )),
            Arc::new(UInt8Array::from_iter_values(
                runs.iter().map(|run| run.level),
            )),
            Arc::new(StringArray::from_iter_values(
                runs.iter().map(|run| run.artifact.path.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                runs.iter().map(|run| run.artifact.checksum.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                runs.iter().map(|run| run.artifact.encoded_bytes),
            )),
            Arc::new(UInt64Array::from_iter_values(
                runs.iter().map(|run| run.footer.offset),
            )),
            Arc::new(UInt64Array::from_iter_values(
                runs.iter().map(|run| run.footer.length),
            )),
            Arc::new(StringArray::from_iter_values(
                runs.iter().map(|run| run.footer.checksum.as_str()),
            )),
            Arc::new(minimum_ids.finish()),
            Arc::new(maximum_ids.finish()),
            Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>(
                runs.iter().map(|run| {
                    Some(
                        run.batches
                            .iter()
                            .map(|batch| Some(batch.row_count))
                            .collect::<Vec<_>>(),
                    )
                }),
            )),
            Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>(
                runs.iter().map(|run| {
                    Some(
                        run.batches
                            .iter()
                            .map(|batch| Some(batch.range.offset))
                            .collect::<Vec<_>>(),
                    )
                }),
            )),
            Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>(
                runs.iter().map(|run| {
                    Some(
                        run.batches
                            .iter()
                            .map(|batch| Some(batch.range.length))
                            .collect::<Vec<_>>(),
                    )
                }),
            )),
            Arc::new(checksums.finish()),
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                runs.iter().map(|run| {
                    Some(
                        run.batches
                            .iter()
                            .map(|batch| Some(batch.metadata_length))
                            .collect::<Vec<_>>(),
                    )
                }),
            )),
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                runs.iter().map(|run| {
                    Some(
                        run.batches
                            .iter()
                            .map(|batch| Some(batch.body_length))
                            .collect::<Vec<_>>(),
                    )
                }),
            )),
        ],
    )?;
    let bytes = write_parquet_batch(batch)?;
    let reference = artifact_ref_for_bytes("id-directory-roots", &bytes)?;
    Ok(PackedDirectoryRoot { bytes, reference })
}

pub(crate) fn decode_directory_root(
    artifact: &ArtifactRef,
    bytes: Bytes,
) -> Result<Vec<DirectoryRunRef>> {
    let decoded = decode_exact_parquet_artifact(
        artifact,
        bytes,
        &directory_root_schema(),
        "ID-directory root",
        MAX_DIRECTORY_ROOT_RUNS,
    )?;
    if decoded.num_rows() == 0 {
        return invalid("ID-directory root decoded no rows");
    }
    if (0..decoded.num_columns()).any(|column| decoded.column(column).null_count() != 0) {
        return invalid("ID-directory root contains null authority");
    }
    let format = decoded
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .expect("exact schema");
    let partitions = decoded
        .column(1)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("exact schema");
    let levels = decoded
        .column(2)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("exact schema");
    let paths = decoded
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let checksums = decoded
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let encoded_bytes = decoded
        .column(5)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let footer_offsets = decoded
        .column(6)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let footer_lengths = decoded
        .column(7)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let footer_checksums = decoded
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("exact schema");
    let minimum_ids = decoded
        .column(9)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exact schema");
    let maximum_ids = decoded
        .column(10)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exact schema");
    let row_counts = decoded
        .column(11)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exact schema");
    let batch_offsets = decoded
        .column(12)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exact schema");
    let batch_lengths = decoded
        .column(13)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exact schema");
    let batch_checksums = decoded
        .column(14)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exact schema");
    let metadata_lengths = decoded
        .column(15)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exact schema");
    let body_lengths = decoded
        .column(16)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exact schema");
    let mut runs = Vec::with_capacity(decoded.num_rows());
    for row in 0..decoded.num_rows() {
        if format.value(row) != DIRECTORY_FORMAT_VERSION {
            return invalid("ID-directory root format marker is unsupported");
        }
        let minimum_ids = decoded_binary_list(minimum_ids, row, "directory minimum IDs")?;
        let maximum_ids = decoded_binary_list(maximum_ids, row, "directory maximum IDs")?;
        let row_counts = decoded_u64_list(row_counts, row, "directory row counts")?;
        let batch_offsets = decoded_u64_list(batch_offsets, row, "directory batch offsets")?;
        let batch_lengths = decoded_u64_list(batch_lengths, row, "directory batch lengths")?;
        let batch_checksums =
            decoded_binary_list(batch_checksums, row, "directory batch checksums")?;
        let metadata_lengths =
            decoded_i32_list(metadata_lengths, row, "directory metadata lengths")?;
        let body_lengths = decoded_i64_list(body_lengths, row, "directory body lengths")?;
        let batch_count = minimum_ids.len();
        if batch_count == 0
            || [
                maximum_ids.len(),
                row_counts.len(),
                batch_offsets.len(),
                batch_lengths.len(),
                batch_checksums.len(),
                metadata_lengths.len(),
                body_lengths.len(),
            ]
            .into_iter()
            .any(|length| length != batch_count)
        {
            return invalid("ID-directory root batch authority lists disagree");
        }
        let mut batches = Vec::with_capacity(batch_count);
        for batch in 0..batch_count {
            batches.push(DirectoryBatchRef {
                min_record_id: minimum_ids[batch].clone(),
                max_record_id: maximum_ids[batch].clone(),
                row_count: row_counts[batch],
                range: AuthenticatedRange {
                    offset: batch_offsets[batch],
                    length: batch_lengths[batch],
                    checksum: std::str::from_utf8(&batch_checksums[batch])
                        .map_err(|_| {
                            BorsukError::InvalidStorage(
                                "directory batch checksum is not UTF-8".into(),
                            )
                        })?
                        .to_string(),
                },
                metadata_length: metadata_lengths[batch],
                body_length: body_lengths[batch],
            });
        }
        runs.push(DirectoryRunRef {
            partition: DirectoryPartition(partitions.value(row)),
            level: levels.value(row),
            artifact: ArtifactRef {
                path: paths.value(row).to_string(),
                checksum: checksums.value(row).to_string(),
                encoded_bytes: encoded_bytes.value(row),
            },
            footer: AuthenticatedRange {
                offset: footer_offsets.value(row),
                length: footer_lengths.value(row),
                checksum: footer_checksums.value(row).to_string(),
            },
            batches,
        });
    }
    if runs
        .windows(2)
        .any(|pair| (pair[0].partition, pair[0].level) >= (pair[1].partition, pair[1].level))
    {
        return invalid("ID-directory root rows are not strictly ordered by partition and level");
    }
    validate_directory_run_refs(&runs)?;
    Ok(runs)
}

fn directory_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("partition", DataType::UInt8, false),
        Field::new("record_id", DataType::Binary, false),
        Field::new("routing_epoch", DataType::UInt64, false),
        Field::new("cell_ordinal", DataType::UInt32, false),
        Field::new("mutation_hlc", DataType::UInt64, false),
        Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
        Field::new("deleted", DataType::Boolean, false),
    ]))
}

fn validate_directory_rows(partition: DirectoryPartition, rows: &[DirectoryRow]) -> Result<()> {
    if rows.is_empty() {
        return invalid("ID-directory partition run must not be empty");
    }
    for row in rows {
        if row.record_id.is_empty()
            || row.routing_epoch == 0
            || DirectoryPartition::for_record_id(&row.record_id) != partition
        {
            return invalid("ID-directory row has an invalid fixed partition or owner");
        }
    }
    if rows
        .windows(2)
        .any(|pair| pair[0].record_id >= pair[1].record_id)
    {
        return invalid("ID-directory rows must converge to one strictly sorted state per ID");
    }
    Ok(())
}

fn directory_record_batch(
    partition: DirectoryPartition,
    rows: &[DirectoryRow],
) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        directory_schema(),
        vec![
            Arc::new(UInt16Array::from_iter_values(
                rows.iter().map(|_| DIRECTORY_FORMAT_VERSION),
            )),
            Arc::new(UInt8Array::from_iter_values(
                rows.iter().map(|_| partition.0),
            )),
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| row.record_id.as_slice()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.routing_epoch),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.cell_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.state.stamp().version().hlc()),
            )),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                rows.iter().map(|row| row.state.stamp().version().writer()),
            )?),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                rows.iter().map(|row| row.state.stamp().digest()),
            )?),
            Arc::new(BooleanArray::from_iter(
                rows.iter().map(|row| Some(row.state.is_deleted())),
            )),
        ],
    )?)
}

enum DirectoryEncodingAttempt {
    Packed(PackedDirectoryPartitionRun),
    BatchTooLarge,
}

fn encode_directory_file(
    partition: DirectoryPartition,
    level: u8,
    rows: &[DirectoryRow],
    rows_per_batch: usize,
    options: DirectoryPackOptions,
) -> Result<DirectoryEncodingAttempt> {
    let write_options =
        IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD))?;
    let mut output = Vec::new();
    {
        let mut writer = FileWriter::try_new_with_options(
            &mut output,
            directory_schema().as_ref(),
            write_options,
        )?;
        for chunk in rows.chunks(rows_per_batch) {
            writer.write(&directory_record_batch(partition, chunk)?)?;
        }
        writer.finish()?;
    }
    let bytes = Bytes::from(output);
    if bytes.len() as u64 > options.hard_max_object_bytes {
        return invalid(
            "ID-directory object exceeds its hard cap; split the immutable run before packing",
        );
    }
    parse_completed_directory_file(partition, level, rows, rows_per_batch, bytes, options)
}

pub(crate) fn pack_directory_partition_run(
    partition: DirectoryPartition,
    level: u8,
    rows: &[DirectoryRow],
    options: DirectoryPackOptions,
) -> Result<PackedDirectoryPartitionRun> {
    options.validate()?;
    if usize::from(level) >= MAX_ACTIVE_DIRECTORY_LEVELS {
        return invalid("ID-directory run level is outside the active level bound");
    }
    validate_directory_rows(partition, rows)?;
    let bytes_per_row = directory_record_batch(partition, rows)?
        .get_array_memory_size()
        .checked_div(rows.len())
        .unwrap_or(1)
        .max(1);
    let mut rows_per_batch = checked_usize(options.target_batch_bytes, "directory batch target")?
        .checked_div(bytes_per_row)
        .unwrap_or(1)
        .max(1)
        .min(rows.len());
    loop {
        match encode_directory_file(partition, level, rows, rows_per_batch, options) {
            Ok(DirectoryEncodingAttempt::Packed(packed)) => return Ok(packed),
            Ok(DirectoryEncodingAttempt::BatchTooLarge) if rows_per_batch > 1 => {
                rows_per_batch = rows_per_batch.div_ceil(2);
            }
            Ok(DirectoryEncodingAttempt::BatchTooLarge) => {
                return invalid("one ID-directory row exceeds the authenticated batch hard cap");
            }
            Err(error) => return Err(error),
        }
    }
}

fn parse_completed_directory_file(
    partition: DirectoryPartition,
    level: u8,
    rows: &[DirectoryRow],
    rows_per_batch: usize,
    bytes: Bytes,
    options: DirectoryPackOptions,
) -> Result<DirectoryEncodingAttempt> {
    if bytes.len() < 10 {
        return invalid("ID-directory Arrow IPC file is shorter than its trailer");
    }
    let trailer_start = bytes.len() - 10;
    let trailer: [u8; 10] = bytes[trailer_start..]
        .try_into()
        .map_err(|_| BorsukError::InvalidStorage("Arrow trailer is truncated".into()))?;
    let footer_len = read_footer_length(trailer)?;
    let footer_start = trailer_start.checked_sub(footer_len).ok_or_else(|| {
        BorsukError::InvalidStorage("Arrow footer length exceeds directory object".into())
    })?;
    let footer = root_as_footer(&bytes[footer_start..trailer_start]).map_err(|error| {
        BorsukError::InvalidStorage(format!("ID-directory Arrow footer is invalid: {error}"))
    })?;
    if footer
        .dictionaries()
        .is_some_and(|dictionaries| !dictionaries.is_empty())
    {
        return invalid("ID-directory Arrow IPC must not use dictionaries");
    }
    let decoded_schema = fb_to_schema(footer.schema().ok_or_else(|| {
        BorsukError::InvalidStorage("ID-directory Arrow footer has no schema".into())
    })?);
    if &decoded_schema != directory_schema().as_ref() || footer.version() != MetadataVersion::V5 {
        return invalid("ID-directory Arrow schema or metadata version is unsupported");
    }
    let blocks = footer
        .recordBatches()
        .map(|blocks| blocks.iter().copied().collect::<Vec<_>>())
        .unwrap_or_default();
    if blocks.len() != rows.len().div_ceil(rows_per_batch) {
        return invalid("ID-directory Arrow batch count disagrees with its rows");
    }
    let footer_range = authenticated_range(&bytes, footer_start as u64..bytes.len() as u64)?;
    let mut batches = Vec::with_capacity(blocks.len());
    for (block, chunk) in blocks.iter().zip(rows.chunks(rows_per_batch)) {
        let range = arrow_block_range(block)?;
        if range.end > footer_start as u64 {
            return invalid("ID-directory Arrow batch overlaps its footer");
        }
        let authenticated = authenticated_range(&bytes, range)?;
        if authenticated.length > options.hard_max_batch_bytes {
            return Ok(DirectoryEncodingAttempt::BatchTooLarge);
        }
        batches.push(DirectoryBatchRef {
            min_record_id: chunk.first().expect("non-empty chunk").record_id.clone(),
            max_record_id: chunk.last().expect("non-empty chunk").record_id.clone(),
            row_count: chunk.len() as u64,
            range: authenticated,
            metadata_length: block.metaDataLength(),
            body_length: block.bodyLength(),
        });
    }
    let checksum = blake3::hash(&bytes).to_hex().to_string();
    let reference = DirectoryRunRef {
        partition,
        level,
        artifact: ArtifactRef {
            path: format!(
                "id-directory/partitions/{:03}/runs/{checksum}.arrow",
                partition.0
            ),
            checksum,
            encoded_bytes: bytes.len() as u64,
        },
        footer: footer_range,
        batches,
    };
    Ok(DirectoryEncodingAttempt::Packed(
        PackedDirectoryPartitionRun { bytes, reference },
    ))
}

fn arrow_block_range(block: &Block) -> Result<Range<u64>> {
    let start = u64::try_from(block.offset())
        .map_err(|_| BorsukError::InvalidStorage("Arrow block offset is negative".into()))?;
    let metadata = u64::try_from(block.metaDataLength()).map_err(|_| {
        BorsukError::InvalidStorage("Arrow block metadata length is negative".into())
    })?;
    let body = u64::try_from(block.bodyLength())
        .map_err(|_| BorsukError::InvalidStorage("Arrow block body length is negative".into()))?;
    let end = start
        .checked_add(metadata)
        .and_then(|end| end.checked_add(body))
        .ok_or_else(|| BorsukError::InvalidStorage("Arrow block range overflows".into()))?;
    Ok(start..end)
}

fn decode_directory_batch(
    run: &DirectoryRunRef,
    batch_ref: &DirectoryBatchRef,
    bytes: Bytes,
) -> Result<Vec<DirectoryRow>> {
    let verified = VerifiedRange::new(&batch_ref.range, batch_ref.range.offset, bytes)?;
    let stored = verified.bytes.clone();
    let block = Block::new(0, batch_ref.metadata_length, batch_ref.body_length);
    let decoder = FileDecoder::new(directory_schema(), MetadataVersion::V5);
    let decoded = catch_unwind(AssertUnwindSafe(|| {
        decoder.read_record_batch(&block, &Buffer::from(stored))
    }))
    .map_err(|_| {
        BorsukError::InvalidStorage("ID-directory Arrow batch has invalid buffer ranges".into())
    })??
    .ok_or_else(|| {
        BorsukError::InvalidStorage("ID-directory Arrow batch decoded no rows".into())
    })?;
    if decoded.schema().as_ref() != directory_schema().as_ref()
        || decoded.num_rows() as u64 != batch_ref.row_count
    {
        return invalid("decoded ID-directory rows exceed or disagree with authenticated summary");
    }
    let format = decoded
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .expect("exact schema");
    let partition = decoded
        .column(1)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("exact schema");
    let ids = decoded
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("exact schema");
    let epoch = decoded
        .column(3)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let cell = decoded
        .column(4)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("exact schema");
    let hlc = decoded
        .column(5)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("exact schema");
    let writer = decoded
        .column(6)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("exact schema");
    let digest = decoded
        .column(7)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("exact schema");
    let deleted = decoded
        .column(8)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("exact schema");
    let mut rows = Vec::with_capacity(decoded.num_rows());
    for row in 0..decoded.num_rows() {
        if (0..decoded.num_columns()).any(|column| decoded.column(column).is_null(row))
            || format.value(row) != DIRECTORY_FORMAT_VERSION
            || partition.value(row) != run.partition.0
        {
            return invalid("decoded ID-directory row has invalid format or partition");
        }
        let writer: [u8; 16] = writer
            .value(row)
            .try_into()
            .map_err(|_| BorsukError::InvalidStorage("directory writer width changed".into()))?;
        let digest: [u8; 32] = digest
            .value(row)
            .try_into()
            .map_err(|_| BorsukError::InvalidStorage("directory digest width changed".into()))?;
        rows.push(DirectoryRow {
            record_id: ids.value(row).to_vec(),
            routing_epoch: epoch.value(row),
            cell_ordinal: cell.value(row),
            state: MutationState::new(
                MutationStamp::new(MutationVersion::from_parts(hlc.value(row), writer), digest),
                if deleted.value(row) {
                    MutationOperation::Delete
                } else {
                    MutationOperation::Put
                },
            ),
        });
    }
    validate_directory_rows(run.partition, &rows)?;
    if rows.first().map(|row| row.record_id.as_slice()) != Some(batch_ref.min_record_id.as_slice())
        || rows.last().map(|row| row.record_id.as_slice())
            != Some(batch_ref.max_record_id.as_slice())
    {
        return invalid("decoded ID-directory bounds disagree with authenticated summary");
    }
    Ok(rows)
}

pub(crate) fn lookup_directory_owner<F>(
    active_levels: &[DirectoryRunRef],
    record_id: &[u8],
    mut fetch: F,
) -> Result<DirectoryLookup>
where
    F: FnMut(&str, Range<u64>) -> Result<Vec<u8>>,
{
    if record_id.is_empty() || active_levels.len() > MAX_ACTIVE_DIRECTORY_LEVELS {
        return invalid("ID-directory lookup is empty or exceeds its active level cap");
    }
    let partition = DirectoryPartition::for_record_id(record_id);
    let mut seen_levels = BTreeSet::new();
    let mut winner = None::<DirectoryOwnerState>;
    for run in active_levels {
        if run.partition != partition {
            continue;
        }
        if !seen_levels.insert(run.level) {
            return invalid("ID-directory root repeats one active level for this partition");
        }
        validate_directory_run_refs(std::slice::from_ref(run))?;
        let matching = run
            .batches
            .iter()
            .filter(|batch| {
                batch.min_record_id.as_slice() <= record_id
                    && record_id <= batch.max_record_id.as_slice()
            })
            .collect::<Vec<_>>();
        if matching.len() > 1 {
            return invalid("ID-directory batch summaries overlap one point lookup");
        }
        let Some(batch) = matching.first() else {
            continue;
        };
        let range = batch.range.checked(run.artifact.encoded_bytes)?;
        let bytes = fetch(&run.artifact.path, range)?;
        let rows = decode_directory_batch(run, batch, Bytes::from(bytes))?;
        if let Ok(row) = rows.binary_search_by(|row| row.record_id.as_slice().cmp(record_id)) {
            let row = &rows[row];
            let candidate = DirectoryOwnerState {
                routing_epoch: row.routing_epoch,
                cell_ordinal: row.cell_ordinal,
                state: row.state,
            };
            winner = Some(match winner {
                None => candidate,
                Some(current) => {
                    let state = current.state.greatest(candidate.state)?;
                    if current.state.stamp().version() == candidate.state.stamp().version()
                        && (current.routing_epoch, current.cell_ordinal)
                            != (candidate.routing_epoch, candidate.cell_ordinal)
                    {
                        return invalid("equal ID-directory versions disagree on routed owner");
                    }
                    if state == candidate.state {
                        candidate
                    } else {
                        current
                    }
                }
            });
        }
    }
    Ok(winner.map_or(DirectoryLookup::Unknown, DirectoryLookup::Found))
}

fn validate_row_bundle_reference(reference: &RowBundleRef) -> Result<()> {
    reference.artifact.validate(".parquet")?;
    if reference.artifact.encoded_bytes > FORMAT_MAX_BUNDLE_BYTES
        || reference.row_groups.is_empty()
        || reference.row_groups.len() > 16
    {
        return invalid("row-bundle reference exceeds a v1 format hard cap");
    }
    reference.footer.checked(reference.artifact.encoded_bytes)?;
    let mut total_rows = 0_u64;
    for summary in &reference.row_groups {
        total_rows = total_rows.checked_add(summary.row_count).ok_or_else(|| {
            BorsukError::InvalidStorage("row-bundle reference row count overflows".into())
        })?;
        if summary.row_count == 0
            || summary.row_count > MAX_RUN_ROWS as u64
            || summary.data.length > FORMAT_MAX_ROW_GROUP_BYTES
        {
            return invalid("row-bundle row-group authority exceeds a v1 format hard cap");
        }
        summary.data.checked(reference.artifact.encoded_bytes)?;
        if let Some(bloom) = &summary.record_id_bloom {
            bloom.checked(reference.artifact.encoded_bytes)?;
        }
        for range in &summary.page_indexes {
            range.checked(reference.artifact.encoded_bytes)?;
        }
    }
    if total_rows > MAX_RUN_ROWS as u64
        || reference
            .row_groups
            .windows(2)
            .any(|pair| pair[0].row_group >= pair[1].row_group)
    {
        return invalid("row-bundle reference exceeds its positioned row or ordinal bound");
    }
    Ok(())
}

pub(crate) fn validate_row_bundle_ranges(reference: &RowBundleRef, bytes: &[u8]) -> Result<()> {
    validate_row_bundle_reference(reference)?;
    if bytes.len() as u64 != reference.artifact.encoded_bytes {
        return invalid("row-bundle object length disagrees with its artifact reference");
    }
    verify_checksum(&reference.artifact.checksum, bytes, "row-bundle object")?;
    let mut ranges = vec![&reference.footer];
    for summary in &reference.row_groups {
        ranges.push(&summary.data);
        if let Some(bloom) = &summary.record_id_bloom {
            ranges.push(bloom);
        }
        ranges.extend(summary.page_indexes.iter());
    }
    for range in ranges {
        let checked = range.checked(reference.artifact.encoded_bytes)?;
        let start = checked_usize(checked.start, "row-bundle range start")?;
        let end = checked_usize(checked.end, "row-bundle range end")?;
        verify_checksum(&range.checksum, &bytes[start..end], "row-bundle range")?;
    }
    Ok(())
}

fn verified_bundle_footer_reader<F>(
    reference: &RowBundleRef,
    fetch: &mut F,
) -> Result<BoundedChunkReader>
where
    F: FnMut(&str, Range<u64>) -> Result<Vec<u8>>,
{
    validate_row_bundle_reference(reference)?;
    let checked = reference.footer.checked(reference.artifact.encoded_bytes)?;
    let bytes = fetch(&reference.artifact.path, checked)?;
    if bytes.len() < 4 || &bytes[bytes.len() - 4..] != b"PAR1" {
        return invalid("authenticated Parquet footer has no trailing PAR1 magic");
    }
    let footer = VerifiedRange::new(
        &reference.footer,
        reference.footer.offset,
        Bytes::from(bytes),
    )?;
    BoundedChunkReader::new(reference.artifact.encoded_bytes, vec![footer])
}

fn verified_bundle_reader<F>(
    reference: &RowBundleRef,
    summary: &RowGroupSummary,
    footer_reader: BoundedChunkReader,
    fetch: &mut F,
) -> Result<BoundedChunkReader>
where
    F: FnMut(&str, Range<u64>) -> Result<Vec<u8>>,
{
    let mut ranges = vec![summary.data.clone()];
    if let Some(bloom) = &summary.record_id_bloom {
        ranges.push(bloom.clone());
    }
    let mut verified = footer_reader.ranges;
    verified.extend(
        ranges
            .iter()
            .map(|range| {
                let checked = range.checked(reference.artifact.encoded_bytes)?;
                let bytes = fetch(&reference.artifact.path, checked)?;
                VerifiedRange::new(range, range.offset, Bytes::from(bytes))
            })
            .collect::<Result<Vec<_>>>()?,
    );
    BoundedChunkReader::new(reference.artifact.encoded_bytes, verified)
}

fn summary_matches_authority(
    summary: &RowGroupSummary,
    authority: &MaterializedLookupAuthority,
) -> bool {
    summary.modality == authority.modality()
        && summary.projection_kind == authority.projection_kind()
        && summary.assignment_kind == authority.assignment_kind()
        && summary.assignment_checksum == authority.assignment_checksum()
        && summary.routing_epoch == authority.routing_epoch()
        && match authority.cell_ordinal() {
            Some(cell) => {
                summary
                    .min_cell_ordinal
                    .is_some_and(|minimum| cell >= minimum)
                    && summary
                        .max_cell_ordinal
                        .is_some_and(|maximum| cell <= maximum)
            }
            None => summary.min_cell_ordinal.is_none() && summary.max_cell_ordinal.is_none(),
        }
}

struct MaterializedLookupAttempt {
    found: Option<RecordBatch>,
    saw_assignment: bool,
}

fn lookup_materialized_row_refs<'a, I, F>(
    references: I,
    authority: &MaterializedLookupAuthority,
    record_id: &[u8],
    fetch: &mut F,
) -> Result<MaterializedLookupAttempt>
where
    I: IntoIterator<Item = &'a RowBundleRef>,
    F: FnMut(&str, Range<u64>) -> Result<Vec<u8>>,
{
    if authority.state().is_deleted() {
        return Ok(MaterializedLookupAttempt {
            found: None,
            saw_assignment: true,
        });
    }
    let mut saw_assignment = false;
    for reference in references {
        for summary in &reference.row_groups {
            if !summary_matches_authority(summary, authority) {
                continue;
            }
            saw_assignment = true;
            if record_id < summary.min_record_id.as_slice()
                || record_id > summary.max_record_id.as_slice()
            {
                continue;
            }
            let footer_reader = verified_bundle_footer_reader(reference, fetch)?;
            let metadata = ArrowReaderMetadata::load(&footer_reader, ArrowReaderOptions::new())?;
            validate_canonical_row_schema(metadata.schema().as_ref())?;
            let file_metadata = metadata.metadata();
            if file_metadata.num_row_groups() > 16
                || file_metadata.file_metadata().num_rows() < 0
                || file_metadata.file_metadata().num_rows() as u64 > MAX_RUN_ROWS as u64
                || file_metadata.row_groups().iter().any(|group| {
                    group
                        .columns()
                        .iter()
                        .any(|column| column.file_path().is_some())
                })
            {
                return invalid("row-bundle footer exceeds a v1 schema, row, or object bound");
            }
            let selected = file_metadata
                .row_groups()
                .get(summary.row_group as usize)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "authenticated summary row-group ordinal is absent from footer".into(),
                    )
                })?;
            if selected.num_rows() < 0
                || selected.num_rows() as u64 != summary.row_count
                || selected.num_rows() as u64 > MAX_RUN_ROWS as u64
            {
                return invalid(
                    "Parquet footer row count disagrees with authenticated row-group summary",
                );
            }
            let reader = verified_bundle_reader(reference, summary, footer_reader, fetch)?;
            let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(reader, metadata);
            let record_id_column = builder
                .parquet_schema()
                .columns()
                .iter()
                .position(|column| column.name() == "record_id")
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("bundle has no record_id column".into())
                })?;
            if let Some(bloom) = builder
                .get_row_group_column_bloom_filter(summary.row_group as usize, record_id_column)?
                && !bloom.check(record_id)
            {
                continue;
            }
            let batches = builder
                .with_row_groups(vec![summary.row_group as usize])
                .build()?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let decoded_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
            if decoded_rows as u64 != summary.row_count {
                return invalid("decoded row count disagrees with authenticated summary");
            }
            for batch in batches {
                let ids = batch
                    .column_by_name("record_id")
                    .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("decoded bundle record_id is not Binary".into())
                    })?;
                let modalities = batch
                    .column_by_name("modality")
                    .and_then(|array| array.as_any().downcast_ref::<StringArray>())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("decoded bundle modality is invalid".into())
                    })?;
                let projections = batch
                    .column_by_name("projection_kind")
                    .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "decoded bundle projection kind is invalid".into(),
                        )
                    })?;
                let assignment_kinds = batch
                    .column_by_name("assignment_kind")
                    .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "decoded bundle assignment kind is invalid".into(),
                        )
                    })?;
                let assignment_checksums = batch
                    .column_by_name("assignment_checksum")
                    .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "decoded bundle assignment checksum is invalid".into(),
                        )
                    })?;
                let epochs = batch
                    .column_by_name("routing_epoch")
                    .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("decoded bundle epoch is invalid".into())
                    })?;
                let cells = batch
                    .column_by_name("cell_ordinal")
                    .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("decoded bundle cell is invalid".into())
                    })?;
                let hlc = batch
                    .column_by_name("mutation_hlc")
                    .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("decoded bundle mutation HLC is invalid".into())
                    })?;
                let writers = batch
                    .column_by_name("mutation_writer")
                    .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "decoded bundle mutation writer is invalid".into(),
                        )
                    })?;
                let digests = batch
                    .column_by_name("mutation_digest")
                    .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "decoded bundle mutation digest is invalid".into(),
                        )
                    })?;
                for row in 0..batch.num_rows() {
                    if ids.value(row) == record_id {
                        let assignment_checksum: [u8; 32] =
                            assignment_checksums.value(row).try_into().map_err(|_| {
                                BorsukError::InvalidStorage(
                                    "decoded assignment checksum width changed".into(),
                                )
                            })?;
                        let routing_epoch = (!epochs.is_null(row)).then(|| epochs.value(row));
                        let cell_ordinal = (!cells.is_null(row)).then(|| cells.value(row));
                        if modalities.value(row) != authority.modality()
                            || projections.value(row) != authority.projection_kind()
                            || assignment_kinds.value(row) != authority.assignment_kind()
                            || assignment_checksum != authority.assignment_checksum()
                            || routing_epoch != authority.routing_epoch()
                            || cell_ordinal != authority.cell_ordinal()
                        {
                            return invalid(
                                "exact bundle row disagrees with typed assignment authority",
                            );
                        }
                        let stamp = MutationStamp::new(
                            MutationVersion::from_parts(
                                hlc.value(row),
                                writers.value(row).try_into().map_err(|_| {
                                    BorsukError::InvalidStorage(
                                        "decoded bundle writer width changed".into(),
                                    )
                                })?,
                            ),
                            digests.value(row).try_into().map_err(|_| {
                                BorsukError::InvalidStorage(
                                    "decoded bundle digest width changed".into(),
                                )
                            })?,
                        );
                        if stamp == authority.state().stamp() {
                            return Ok(MaterializedLookupAttempt {
                                found: Some(batch.slice(row, 1)),
                                saw_assignment: true,
                            });
                        }
                    }
                }
            }
        }
    }
    Ok(MaterializedLookupAttempt {
        found: None,
        saw_assignment,
    })
}

pub(crate) fn lookup_materialized_row<F>(
    run: &PackedRowBundleRun,
    authority: &MaterializedLookupAuthority,
    record_id: &[u8],
    mut fetch: F,
) -> Result<Option<RecordBatch>>
where
    F: FnMut(&str, Range<u64>) -> Result<Vec<u8>>,
{
    let attempt = lookup_materialized_row_refs(
        run.bundles.iter().map(|bundle| &bundle.reference),
        authority,
        record_id,
        &mut fetch,
    )?;
    if !attempt.saw_assignment {
        return invalid("materialized lookup assignment is absent from the opened run");
    }
    Ok(attempt.found)
}

fn validate_roster_for_run(run: &RowBundleRunRef, entries: &[RosterEntry]) -> Result<()> {
    let bundle_count = entries
        .iter()
        .filter(|entry| entry.role == RowBundleRole::Bundle)
        .count() as u64;
    let roots = entries
        .iter()
        .filter(|entry| entry.role == RowBundleRole::SummaryRoot)
        .collect::<Vec<_>>();
    if bundle_count != run.bundle_count
        || roots.len() != 1
        || roots[0].path != run.summary_root.path
        || roots[0].checksum != run.summary_root.checksum
        || roots[0].encoded_bytes != run.summary_root.encoded_bytes
    {
        return invalid("row-bundle run roster disagrees with generation authority");
    }
    let mut totals = BTreeMap::<String, u64>::new();
    for entry in entries {
        let current = totals.get(entry.role.as_str()).copied().unwrap_or_default();
        totals.insert(
            entry.role.as_str().to_string(),
            current.checked_add(entry.encoded_bytes).ok_or_else(|| {
                BorsukError::InvalidStorage("row-bundle roster role bytes overflow".into())
            })?,
        );
    }
    if totals != run.role_bytes {
        return invalid("row-bundle roster role totals disagree with generation authority");
    }
    Ok(())
}

fn shard_may_contain_authority(
    shard: &SummaryShardRef,
    authority: &MaterializedLookupAuthority,
    record_id: &[u8],
) -> bool {
    let target = SummaryBoundary {
        modality: authority.modality().to_string(),
        projection_kind: authority.projection_kind(),
        assignment_kind: authority.assignment_kind(),
        assignment_checksum: authority.assignment_checksum(),
        routing_epoch: authority.routing_epoch(),
        cell_ordinal: authority.cell_ordinal(),
        record_id: record_id.to_vec(),
    };
    shard.first <= target && target <= shard.last
}

fn unique_range_requests(
    requests: impl IntoIterator<Item = ObjectRangeRequest>,
) -> Vec<ObjectRangeRequest> {
    let mut unique = BTreeMap::<(String, u64, u64), ObjectRangeRequest>::new();
    for request in requests {
        unique
            .entry((request.path.clone(), request.range.start, request.range.end))
            .or_insert(request);
    }
    unique.into_values().collect()
}

fn fetch_request_batch<F>(
    requests: &[ObjectRangeRequest],
    fetch_ranges: &mut F,
) -> Result<BTreeMap<(String, u64, u64), Bytes>>
where
    F: FnMut(&[ObjectRangeRequest]) -> Result<Vec<Bytes>>,
{
    if requests.is_empty() {
        return Ok(BTreeMap::new());
    }
    let fetched = fetch_ranges(requests)?;
    if fetched.len() != requests.len() {
        return invalid("batched range fetch returned the wrong range count");
    }
    Ok(requests
        .iter()
        .cloned()
        .zip(fetched)
        .map(|(request, bytes)| {
            (
                (request.path, request.range.start, request.range.end),
                bytes,
            )
        })
        .collect())
}

fn preflight_bundle_footer(
    reference: &RowBundleRef,
    summary: &RowGroupSummary,
    bytes: Bytes,
) -> Result<()> {
    let mut supplied = Some(bytes);
    let footer_reader = verified_bundle_footer_reader(reference, &mut |_path, _range| {
        Ok(supplied.take().expect("one footer fetch").to_vec())
    })?;
    let metadata = ArrowReaderMetadata::load(&footer_reader, ArrowReaderOptions::new())?;
    validate_canonical_row_schema(metadata.schema().as_ref())?;
    let file_metadata = metadata.metadata();
    if file_metadata.num_row_groups() > 16
        || file_metadata.file_metadata().num_rows() < 0
        || file_metadata.file_metadata().num_rows() as u64 > MAX_RUN_ROWS as u64
        || file_metadata.row_groups().iter().any(|group| {
            group
                .columns()
                .iter()
                .any(|column| column.file_path().is_some())
        })
    {
        return invalid("row-bundle footer exceeds a v1 schema, row, or object bound");
    }
    let selected = file_metadata
        .row_groups()
        .get(summary.row_group as usize)
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "authenticated summary row-group ordinal is absent from footer".into(),
            )
        })?;
    if selected.num_rows() < 0
        || selected.num_rows() as u64 != summary.row_count
        || selected.num_rows() as u64 > MAX_RUN_ROWS as u64
    {
        return invalid("Parquet footer row count disagrees with authenticated row-group summary");
    }
    Ok(())
}

pub(crate) fn lookup_materialized_row_opened<FO, FR>(
    opened: &OpenedRowBundleGeneration,
    authority: &MaterializedLookupAuthority,
    record_id: &[u8],
    mut fetch_objects: FO,
    mut fetch_ranges: FR,
) -> Result<Option<RecordBatch>>
where
    FO: FnMut(&[ArtifactRef]) -> Result<Vec<Bytes>>,
    FR: FnMut(&[ObjectRangeRequest]) -> Result<Vec<Bytes>>,
{
    if record_id.is_empty() || opened.active_runs.len() > MAX_ACTIVE_ROW_BUNDLE_LEVELS {
        return invalid("opened row-bundle lookup is empty or exceeds its active level cap");
    }
    let candidates = opened
        .active_runs
        .iter()
        .flat_map(|run| {
            run.shards
                .iter()
                .filter(|shard| shard_may_contain_authority(shard, authority, record_id))
                .map(move |shard| (run, shard))
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return invalid("materialized lookup assignment is absent from opened roots");
    }
    let artifacts = candidates
        .iter()
        .map(|(_, shard)| shard.artifact.clone())
        .collect::<Vec<_>>();
    let fetched = fetch_objects(&artifacts)?;
    if fetched.len() != candidates.len() {
        return invalid("batched summary-shard fetch returned the wrong object count");
    }
    let mut bundles = Vec::new();
    for ((run, shard), bytes) in candidates.into_iter().zip(fetched) {
        let decoded = decode_summary_shard(&shard.artifact, bytes)?;
        if decoded
            .iter()
            .map(|bundle| bundle.row_groups.len() as u64)
            .sum::<u64>()
            != shard.row_count
        {
            return invalid("row-bundle summary shard row count disagrees with its root");
        }
        for bundle in &decoded {
            if !run.roster.iter().any(|entry| {
                entry.role == RowBundleRole::Bundle
                    && entry.path == bundle.artifact.path
                    && entry.checksum == bundle.artifact.checksum
                    && entry.encoded_bytes == bundle.artifact.encoded_bytes
            }) {
                return invalid("row-bundle summary shard references an unrostered bundle");
            }
        }
        bundles.extend(decoded);
    }
    let candidate_rows = bundles
        .iter()
        .flat_map(|bundle| {
            bundle.row_groups.iter().filter_map(move |summary| {
                (summary_matches_authority(summary, authority)
                    && record_id >= summary.min_record_id.as_slice()
                    && record_id <= summary.max_record_id.as_slice())
                .then_some((bundle, summary))
            })
        })
        .collect::<Vec<_>>();
    if candidate_rows.is_empty() {
        if bundles.iter().any(|bundle| {
            bundle
                .row_groups
                .iter()
                .any(|summary| summary_matches_authority(summary, authority))
        }) {
            return Ok(None);
        }
        return invalid("materialized lookup assignment is absent from authenticated shards");
    }
    let mut footer_requests = Vec::with_capacity(candidate_rows.len());
    for (bundle, _) in &candidate_rows {
        footer_requests.push(ObjectRangeRequest {
            path: bundle.artifact.path.clone(),
            range: bundle.footer.checked(bundle.artifact.encoded_bytes)?,
        });
    }
    let footer_requests = unique_range_requests(footer_requests);
    let mut cached = fetch_request_batch(&footer_requests, &mut fetch_ranges)?;
    for (bundle, summary) in &candidate_rows {
        let footer = bundle.footer.checked(bundle.artifact.encoded_bytes)?;
        let key = (bundle.artifact.path.clone(), footer.start, footer.end);
        preflight_bundle_footer(
            bundle,
            summary,
            cached
                .get(&key)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("batched footer response is absent".into())
                })?
                .clone(),
        )?;
    }
    let mut data_requests = Vec::new();
    for (bundle, summary) in &candidate_rows {
        data_requests.push(ObjectRangeRequest {
            path: bundle.artifact.path.clone(),
            range: summary.data.checked(bundle.artifact.encoded_bytes)?,
        });
        if let Some(bloom) = &summary.record_id_bloom {
            data_requests.push(ObjectRangeRequest {
                path: bundle.artifact.path.clone(),
                range: bloom.checked(bundle.artifact.encoded_bytes)?,
            });
        }
    }
    let data_requests = unique_range_requests(data_requests);
    cached.extend(fetch_request_batch(&data_requests, &mut fetch_ranges)?);
    let attempt = lookup_materialized_row_refs(
        &bundles,
        authority,
        record_id,
        &mut |path, range: Range<u64>| {
            cached
                .get(&(path.to_string(), range.start, range.end))
                .map(|bytes| bytes.to_vec())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "Parquet requested range {path}:{}..{} absent from the authenticated batch",
                        range.start, range.end
                    ))
                })
        },
    )?;
    if !attempt.saw_assignment {
        return invalid("materialized lookup assignment is absent from authenticated shards");
    }
    Ok(attempt.found)
}

#[allow(
    dead_code,
    reason = "wired into materialization at the Task 4 atomic switch"
)]
pub(crate) fn compact_canonical_row_bundles_to_sink(
    batches: &[CanonicalRowBatch],
    route_plan: &[PositionedRoutePlanRow],
    target_level: u8,
    options: RowBundlePackOptions,
    directory_runs: &[DirectoryRunRef],
    sink: &mut dyn RowBundleObjectSink,
) -> Result<CompactedRowBundles> {
    let row_bundles =
        pack_canonical_row_bundles_to_sink(batches, route_plan, target_level, options, sink)?;
    Ok(CompactedRowBundles {
        metrics: ConstructionMetrics {
            peak_staged_bytes: row_bundles.metrics.peak_staged_bytes,
            directory_writes: 0,
        },
        row_bundles,
        directory_runs: directory_runs.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        ops::Range,
        sync::Arc,
    };

    use arrow_array::{
        ArrayRef, BinaryArray, FixedSizeBinaryArray, RecordBatch, StringArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::{
        arrow::arrow_reader::ParquetRecordBatchReaderBuilder, file::reader::ChunkReader,
    };

    use crate::{
        format::{
            PositionedRouteAssignment, PositionedRoutePlanRow, PositionedRouteProjectionKind,
        },
        mutation::{MutationOperation, MutationStamp, MutationState, MutationVersion},
    };

    use super::{
        ArtifactRef, AuthenticatedRange, BoundedChunkReader, CanonicalRowBatch,
        CollectingRowBundleSink, DirectoryLookup, DirectoryOwnerState, DirectoryPackOptions,
        DirectoryPartition, DirectoryRow, MAX_ACTIVE_ROW_BUNDLE_LEVELS,
        MaterializedLookupAuthority, RowBundlePackOptions, RowBundleRunRef, VerifiedRange,
        decode_directory_root, decode_exact_parquet_artifact, decode_generation_root,
        decode_roster, decode_summary_root, generation_root_schema, lookup_directory_owner,
        lookup_materialized_row, lookup_materialized_row_opened, open_row_bundle_generation,
        pack_canonical_row_bundles, pack_canonical_row_bundles_to_sink,
        pack_directory_partition_run, pack_directory_root, stage_row_bundle_generation,
        validate_row_bundle_ranges, validate_summary_root_non_overlap,
        validate_summary_shard_non_overlap,
    };

    fn stamp(row: usize) -> MutationStamp {
        MutationStamp::new(
            MutationVersion::from_parts(row as u64 + 1, [7; 16]),
            *blake3::hash(format!("row-{row}").as_bytes()).as_bytes(),
        )
    }

    fn payload(row: usize, bytes: usize) -> Vec<u8> {
        let mut payload = vec![0_u8; bytes];
        let mut hasher = blake3::Hasher::new();
        hasher.update(format!("payload-{row}").as_bytes());
        hasher.finalize_xof().fill(&mut payload);
        payload
    }

    fn canonical_rows(
        cells: usize,
        payload_bytes: usize,
    ) -> (CanonicalRowBatch, Vec<PositionedRoutePlanRow>) {
        canonical_rows_with_cells(
            &(0..cells)
                .map(|cell| u32::try_from(cell).unwrap())
                .collect::<Vec<_>>(),
            payload_bytes,
            [3; 32],
            [3; 32],
        )
    }

    fn canonical_rows_with_cells(
        cells_by_row: &[u32],
        payload_bytes: usize,
        batch_catalog_checksum: [u8; 32],
        route_catalog_checksum: [u8; 32],
    ) -> (CanonicalRowBatch, Vec<PositionedRoutePlanRow>) {
        let rows = cells_by_row.len();
        let assignment = PositionedRouteAssignment::catalog(route_catalog_checksum, 11).unwrap();
        let ids = (0..rows)
            .map(|row| format!("row-{row:05}").into_bytes())
            .collect::<Vec<_>>();
        let stamps = (0..rows).map(stamp).collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_bundle_format", DataType::UInt16, false),
            Field::new("modality", DataType::Utf8, false),
            Field::new("projection_kind", DataType::UInt8, false),
            Field::new("assignment_kind", DataType::UInt8, false),
            Field::new("assignment_checksum", DataType::FixedSizeBinary(32), false),
            Field::new("routing_epoch", DataType::UInt64, true),
            Field::new("cell_ordinal", DataType::UInt32, true),
            Field::new("record_id", DataType::Binary, false),
            Field::new("projected_ordinal", DataType::UInt32, false),
            Field::new("mutation_hlc", DataType::UInt64, false),
            Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
            Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
            Field::new("payload", DataType::Binary, false),
        ]));
        let columns = vec![
            Arc::new(UInt16Array::from_iter_values((0..rows).map(|_| 1))) as ArrayRef,
            Arc::new(StringArray::from_iter_values((0..rows).map(|_| "primary"))) as ArrayRef,
            Arc::new(UInt8Array::from_iter_values((0..rows).map(|_| 0))) as ArrayRef,
            Arc::new(UInt8Array::from_iter_values((0..rows).map(|_| 0))) as ArrayRef,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter((0..rows).map(|_| batch_catalog_checksum))
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(UInt64Array::from_iter((0..rows).map(|_| Some(11)))) as ArrayRef,
            Arc::new(UInt32Array::from_iter(
                cells_by_row.iter().copied().map(Some),
            )) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values(ids.iter().map(Vec::as_slice))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values((0..rows).map(|_| 0))) as ArrayRef,
            Arc::new(UInt64Array::from_iter_values(
                stamps.iter().map(|stamp| stamp.version().hlc()),
            )) as ArrayRef,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    stamps.iter().map(|stamp| stamp.version().writer()),
                )
                .unwrap(),
            ) as ArrayRef,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(stamps.iter().map(|stamp| stamp.digest()))
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values(
                (0..rows).map(|row| payload(row, payload_bytes)),
            )) as ArrayRef,
        ];
        let batch =
            CanonicalRowBatch::try_new(RecordBatch::try_new(schema, columns).unwrap()).unwrap();

        let mut route_plan = Vec::with_capacity(rows + 1);
        route_plan.push(
            PositionedRoutePlanRow::summary(
                "primary",
                PositionedRouteProjectionKind::Primary,
                assignment.clone(),
                rows as u64,
                stamps[0],
            )
            .unwrap(),
        );
        route_plan.extend(ids.into_iter().zip(stamps).enumerate().map(
            |(row, (record_id, stamp))| {
                PositionedRoutePlanRow::routed(
                    record_id,
                    "primary",
                    PositionedRouteProjectionKind::Primary,
                    0,
                    assignment.clone(),
                    cells_by_row[row],
                    stamp,
                )
                .unwrap()
            },
        ));
        (batch, route_plan)
    }

    fn test_options() -> RowBundlePackOptions {
        RowBundlePackOptions {
            target_bundle_bytes: 64 * 1024,
            hard_max_bundle_bytes: 128 * 1024,
            target_row_group_bytes: 4 * 1024,
            hard_max_row_group_bytes: 8 * 1024,
            max_row_groups_per_bundle: 8,
            summary_rows_per_shard: 64,
        }
    }

    fn primary_authority(owner: DirectoryOwnerState) -> MaterializedLookupAuthority {
        MaterializedLookupAuthority::catalog(
            "primary",
            PositionedRouteProjectionKind::Primary,
            [3; 32],
            owner,
        )
        .unwrap()
    }

    fn analyzer_batch(
        modality: &str,
        projection_kind: u8,
        checksum: [u8; 32],
        record_id: &[u8],
        stamp: MutationStamp,
    ) -> CanonicalRowBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_bundle_format", DataType::UInt16, false),
            Field::new("modality", DataType::Utf8, false),
            Field::new("projection_kind", DataType::UInt8, false),
            Field::new("assignment_kind", DataType::UInt8, false),
            Field::new("assignment_checksum", DataType::FixedSizeBinary(32), false),
            Field::new("routing_epoch", DataType::UInt64, true),
            Field::new("cell_ordinal", DataType::UInt32, true),
            Field::new("record_id", DataType::Binary, false),
            Field::new("projected_ordinal", DataType::UInt32, false),
            Field::new("mutation_hlc", DataType::UInt64, false),
            Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
            Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
            Field::new("term_payload", DataType::Binary, false),
        ]));
        CanonicalRowBatch::try_new(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(UInt16Array::from(vec![1])) as ArrayRef,
                    Arc::new(StringArray::from(vec![modality])) as ArrayRef,
                    Arc::new(UInt8Array::from(vec![projection_kind])) as ArrayRef,
                    Arc::new(UInt8Array::from(vec![1])) as ArrayRef,
                    Arc::new(FixedSizeBinaryArray::try_from_iter([checksum].into_iter()).unwrap())
                        as ArrayRef,
                    Arc::new(UInt64Array::from(vec![None])) as ArrayRef,
                    Arc::new(UInt32Array::from(vec![None])) as ArrayRef,
                    Arc::new(BinaryArray::from_iter_values([record_id])) as ArrayRef,
                    Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                    Arc::new(UInt64Array::from(vec![stamp.version().hlc()])) as ArrayRef,
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter([stamp.version().writer()].into_iter())
                            .unwrap(),
                    ) as ArrayRef,
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter([stamp.digest()].into_iter()).unwrap(),
                    ) as ArrayRef,
                    Arc::new(BinaryArray::from_iter_values([b"term".as_slice()])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn bundle_and_metadata_counts_follow_encoded_bytes_not_touched_cells() {
        let options = test_options();

        for cells in [3, 2_048, 16_384] {
            let (batch, route_plan) = canonical_rows(cells, 16);
            let packed = pack_canonical_row_bundles(&[batch], &route_plan, options).unwrap();

            assert!(!packed.bundles.is_empty());
            assert!(
                packed
                    .bundles
                    .iter()
                    .all(|bundle| bundle.bytes.len() as u64 <= options.hard_max_bundle_bytes)
            );
            assert!(packed.bundles.iter().all(|bundle| {
                bundle.reference.row_groups.len() <= options.max_row_groups_per_bundle
            }));
            assert_eq!(
                packed.summary_row_count(),
                packed
                    .bundles
                    .iter()
                    .map(|bundle| bundle.reference.row_groups.len())
                    .sum::<usize>()
            );
            assert!(
                packed.summary_row_count()
                    <= packed.bundles.len() * options.max_row_groups_per_bundle
            );
            assert!(
                packed
                    .roster
                    .artifacts
                    .iter()
                    .all(|artifact| !artifact.role.as_str().contains("centroid"))
            );
            if cells >= 2_048 {
                assert!(
                    packed.bundles.len() * 64 < cells,
                    "one object or metadata row per touched cell is forbidden: cells={cells}, bundles={}",
                    packed.bundles.len()
                );
                assert!(
                    packed.summary_row_count() * 8 < cells,
                    "row-group metadata must not scale with touched cells: cells={cells}, summaries={}",
                    packed.summary_row_count()
                );
            }
        }
    }

    #[test]
    fn every_bundle_metadata_object_is_emitted_and_peak_tracks_one_staged_object() {
        let (batch, route_plan) = canonical_rows(4_096, 256);
        let mut sink = CollectingRowBundleSink::default();
        let packed =
            pack_canonical_row_bundles_to_sink(&[batch], &route_plan, 0, test_options(), &mut sink)
                .unwrap();

        let expected = packed
            .roster
            .artifacts
            .iter()
            .map(|artifact| artifact.path.as_str())
            .chain(std::iter::once(packed.roster.reference.path.as_str()))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            sink.objects
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            expected,
            "bundles, summary shards, root, and roster must all flow through the sink"
        );
        let largest = sink
            .objects
            .values()
            .map(|bytes| bytes.len())
            .max()
            .unwrap() as u64;
        assert!(
            packed.metrics.peak_staged_bytes <= largest.saturating_mul(2),
            "peak staged bytes must describe bounded in-flight encoding, not retained run bytes"
        );
    }

    #[test]
    fn sparse_and_text_analyzer_rows_are_packed_with_null_cell_authority() {
        let sparse_stamp = stamp(0);
        let text_stamp = stamp(1);
        let sparse_assignment = PositionedRouteAssignment::analyzer([8; 32]).unwrap();
        let text_assignment = PositionedRouteAssignment::analyzer([9; 32]).unwrap();
        let route_plan = vec![
            PositionedRoutePlanRow::summary(
                "sparse",
                PositionedRouteProjectionKind::Sparse,
                sparse_assignment.clone(),
                1,
                sparse_stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::term_partitioned(
                b"sparse-id".to_vec(),
                "sparse",
                PositionedRouteProjectionKind::Sparse,
                0,
                sparse_assignment,
                sparse_stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::summary(
                "text",
                PositionedRouteProjectionKind::Text,
                text_assignment.clone(),
                1,
                sparse_stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::term_partitioned(
                b"text-id".to_vec(),
                "text",
                PositionedRouteProjectionKind::Text,
                0,
                text_assignment,
                text_stamp,
            )
            .unwrap(),
        ];
        let packed = pack_canonical_row_bundles(
            &[
                analyzer_batch("sparse", 2, [8; 32], b"sparse-id", sparse_stamp),
                analyzer_batch("text", 3, [9; 32], b"text-id", text_stamp),
            ],
            &route_plan,
            test_options(),
        )
        .unwrap();

        let summaries = packed
            .bundles
            .iter()
            .flat_map(|bundle| &bundle.reference.row_groups)
            .collect::<Vec<_>>();
        assert_eq!(summaries.len(), 2);
        assert!(summaries.iter().all(|summary| summary.assignment_kind == 1));
        assert!(
            summaries
                .iter()
                .all(|summary| summary.routing_epoch.is_none())
        );
        assert!(summaries.iter().all(
            |summary| summary.min_cell_ordinal.is_none() && summary.max_cell_ordinal.is_none()
        ));
        assert_eq!(
            summaries
                .iter()
                .map(|summary| summary.modality.as_str())
                .collect::<Vec<_>>(),
            ["sparse", "text"]
        );

        let objects = row_bundle_object_map(&packed);
        for (modality, projection_kind, checksum, id, state) in [
            (
                "sparse",
                2,
                [8; 32],
                b"sparse-id".as_slice(),
                MutationState::new(sparse_stamp, MutationOperation::Put),
            ),
            (
                "text",
                3,
                [9; 32],
                b"text-id".as_slice(),
                MutationState::new(text_stamp, MutationOperation::Put),
            ),
        ] {
            let authority = MaterializedLookupAuthority::analyzer(
                modality,
                if projection_kind == 2 {
                    PositionedRouteProjectionKind::Sparse
                } else {
                    PositionedRouteProjectionKind::Text
                },
                checksum,
                state,
            )
            .unwrap();
            assert!(
                lookup_materialized_row(&packed, &authority, id, |path, range: Range<u64>| {
                    let object = objects.get(path).unwrap();
                    Ok(object[usize::try_from(range.start).unwrap()
                        ..usize::try_from(range.end).unwrap()]
                        .to_vec())
                },)
                .unwrap()
                .is_some(),
                "{modality} analyzer row must be readable by explicit assignment authority"
            );
        }

        let wrong = MaterializedLookupAuthority::catalog(
            "primary",
            PositionedRouteProjectionKind::Primary,
            [3; 32],
            DirectoryOwnerState {
                routing_epoch: 11,
                cell_ordinal: 0,
                state: MutationState::new(sparse_stamp, MutationOperation::Put),
            },
        )
        .unwrap();
        assert!(
            lookup_materialized_row(&packed, &wrong, b"sparse-id", |_path, _range| {
                unreachable!("wrong assignment authority must fail before bundle I/O")
            })
            .is_err(),
            "a primary directory owner must not silently miss an analyzer row"
        );
    }

    #[test]
    fn packer_rejects_unordered_rows_oversized_rows_and_route_plan_mismatch() {
        let options = test_options();

        let (unordered, route_plan) = canonical_rows_with_cells(&[1, 0], 16, [3; 32], [3; 32]);
        let error = pack_canonical_row_bundles(&[unordered], &route_plan, options)
            .unwrap_err()
            .to_string();
        assert!(error.contains("canonical"), "{error}");

        let (oversized, route_plan) = canonical_rows(1, options.hard_max_bundle_bytes as usize);
        let error = pack_canonical_row_bundles(&[oversized], &route_plan, options)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("hard") || error.contains("oversized"),
            "{error}"
        );

        let (mismatched, route_plan) = canonical_rows_with_cells(&[0, 1], 16, [4; 32], [3; 32]);
        let error = pack_canonical_row_bundles(&[mismatched], &route_plan, options)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("route plan") || error.contains("catalog"),
            "{error}"
        );
    }

    fn artifact_ref(label: &str, encoded_bytes: u64) -> ArtifactRef {
        ArtifactRef {
            path: format!("row-bundle-tests/{label}.parquet"),
            checksum: blake3::hash(label.as_bytes()).to_hex().to_string(),
            encoded_bytes,
        }
    }

    fn sealed_run_ref(level: u8, summary_count: u64) -> RowBundleRunRef {
        let summary_root = artifact_ref(&format!("summary-root-{level}-{summary_count}"), 4_096);
        RowBundleRunRef {
            level,
            summary_root,
            roster: artifact_ref(&format!("roster-{level}-{summary_count}"), 4_096),
            bundle_count: summary_count,
            summary_count,
            role_bytes: BTreeMap::from([
                ("row_bundle".to_string(), summary_count * 1_024),
                (
                    "row_bundle_summary_shard".to_string(),
                    summary_count.div_ceil(1_024) * 4_096,
                ),
                ("row_bundle_summary_root".to_string(), 4_096),
            ]),
        }
    }

    fn constructed_prior_summary_run(summary_count: usize) -> super::PackedRowBundleRun {
        let bundles = (0..summary_count)
            .map(|row| {
                let record_id = format!("prior-row-{row:08}").into_bytes();
                let bundle = artifact_ref(&format!("prior-bundle-{row:08}"), 1_024);
                super::PackedRowBundle {
                    bytes: Bytes::new(),
                    reference: super::RowBundleRef {
                        artifact: bundle,
                        footer: AuthenticatedRange {
                            offset: 1_000,
                            length: 24,
                            checksum: blake3::hash(format!("footer-{row}").as_bytes())
                                .to_hex()
                                .to_string(),
                        },
                        row_groups: vec![super::RowGroupSummary {
                            row_group: 0,
                            modality: "primary".to_string(),
                            projection_kind: 0,
                            assignment_kind: 0,
                            assignment_checksum: [3; 32],
                            routing_epoch: Some(11),
                            min_cell_ordinal: Some(row as u32),
                            max_cell_ordinal: Some(row as u32),
                            min_record_id: record_id.clone(),
                            max_record_id: record_id.clone(),
                            first_record_id: record_id.clone(),
                            last_record_id: record_id,
                            row_count: 1,
                            data: AuthenticatedRange {
                                offset: 4,
                                length: 64,
                                checksum: blake3::hash(format!("data-{row}").as_bytes())
                                    .to_hex()
                                    .to_string(),
                            },
                            record_id_bloom: None,
                            page_indexes: Vec::new(),
                            min_stamp: stamp(row),
                            max_stamp: stamp(row),
                        }],
                    },
                }
            })
            .collect();
        let mut options = test_options();
        options.summary_rows_per_shard = 1_024;
        let mut sink = CollectingRowBundleSink::default();
        super::build_packed_run(bundles, 1, options, 0, &mut sink).unwrap()
    }

    #[test]
    fn staging_one_run_reuses_prior_shards_and_scales_with_active_levels() {
        let (batch, route_plan) = canonical_rows(32, 64);
        let new_run = pack_canonical_row_bundles(&[batch], &route_plan, test_options()).unwrap();
        let mut root_sizes = Vec::new();

        for prior_summaries in [1_000, 8_000, 18_000] {
            let prior_packed = constructed_prior_summary_run(prior_summaries);
            let prior_shards = decode_summary_root(
                &prior_packed.root.reference,
                Bytes::copy_from_slice(&prior_packed.root.bytes),
            )
            .unwrap();
            assert_eq!(
                prior_shards
                    .iter()
                    .map(|shard| shard.row_count)
                    .sum::<u64>(),
                prior_summaries as u64
            );
            let prior = prior_packed.run_ref.clone();
            let staged = stage_row_bundle_generation(
                std::slice::from_ref(&prior),
                &new_run.run_ref,
                &artifact_ref("directory-root", 4_096),
            )
            .unwrap();

            assert_eq!(staged.active_runs[0], new_run.run_ref);
            assert_eq!(staged.active_runs[1], prior);
            assert!(
                staged.metrics.peak_staged_bytes <= new_run.metrics.peak_staged_bytes + 128 * 1024
            );
            assert!(staged.root.bytes.len() < 64 * 1024);
            root_sizes.push(staged.root.bytes.len());
        }

        assert!(
            root_sizes.iter().max().unwrap() - root_sizes.iter().min().unwrap() < 256,
            "root bytes must encode bounded run refs, not prior summary rows: {root_sizes:?}"
        );
    }

    #[test]
    fn generation_root_rejects_active_level_cap_overflow() {
        let active = (0..MAX_ACTIVE_ROW_BUNDLE_LEVELS)
            .map(|level| sealed_run_ref(u8::try_from(level).unwrap(), 1_000))
            .collect::<Vec<_>>();
        let overflow = sealed_run_ref(u8::try_from(MAX_ACTIVE_ROW_BUNDLE_LEVELS).unwrap(), 1);

        let error =
            stage_row_bundle_generation(&active, &overflow, &artifact_ref("directory-root", 4_096))
                .unwrap_err()
                .to_string();
        assert!(
            error.contains("active") && error.contains("level"),
            "{error}"
        );
    }

    #[test]
    fn explicit_target_level_allows_consecutive_immutable_run_publication() {
        let (first_batch, first_plan) = canonical_rows(32, 64);
        let first =
            pack_canonical_row_bundles(&[first_batch], &first_plan, test_options()).unwrap();
        let (second_batch, second_plan) = canonical_rows(16, 96);
        let mut sink = CollectingRowBundleSink::default();
        let second = pack_canonical_row_bundles_to_sink(
            &[second_batch],
            &second_plan,
            1,
            test_options(),
            &mut sink,
        )
        .unwrap();
        assert_eq!(second.run_ref.level, 1);

        let staged = stage_row_bundle_generation(
            std::slice::from_ref(&first.run_ref),
            &second.run_ref,
            &artifact_ref("directory-root", 4_096),
        )
        .unwrap();
        assert_eq!(
            staged
                .active_runs
                .iter()
                .map(|run| run.level)
                .collect::<Vec<_>>(),
            [0, 1]
        );
    }

    #[test]
    fn summary_root_and_roster_are_stock_parquet_without_centroids() {
        let (batch, route_plan) = canonical_rows(2_048, 16);
        let packed = pack_canonical_row_bundles(&[batch], &route_plan, test_options()).unwrap();
        let artifacts = packed
            .summary_shards
            .iter()
            .map(|artifact| artifact.bytes.as_slice())
            .chain([packed.root.bytes.as_slice(), packed.roster.bytes.as_slice()]);

        for bytes in artifacts {
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes)).unwrap();
            assert!(
                builder
                    .schema()
                    .fields()
                    .iter()
                    .all(|field| !field.name().contains("centroid"))
            );
        }

        for (role, expected_bytes) in &packed.roster.role_bytes {
            let actual_bytes = packed
                .roster
                .artifacts
                .iter()
                .filter(|artifact| &artifact.role == role)
                .map(|artifact| artifact.encoded_bytes)
                .sum::<u64>();
            assert_eq!(actual_bytes, *expected_bytes, "role={role:?}");
        }
    }

    #[test]
    fn corrupt_or_escaping_authenticated_ranges_fail_before_parquet_interpretation() {
        let (batch, route_plan) = canonical_rows(64, 256);
        let packed = pack_canonical_row_bundles(&[batch], &route_plan, test_options()).unwrap();
        let bundle = &packed.bundles[0];
        let mut corrupted_bytes = bundle.bytes.to_vec();
        let data_range = &bundle.reference.row_groups[0].data;
        corrupted_bytes[usize::try_from(data_range.offset).unwrap()] ^= 0x80;
        let mut corrupted_ref = bundle.reference.clone();
        corrupted_ref.artifact.checksum = blake3::hash(&corrupted_bytes).to_hex().to_string();

        let error = validate_row_bundle_ranges(&corrupted_ref, &corrupted_bytes)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("range") && error.contains("checksum"),
            "{error}"
        );

        let verified_bytes = Bytes::from_static(b"abcd");
        let authenticated = AuthenticatedRange {
            offset: 16,
            length: 4,
            checksum: blake3::hash(&verified_bytes).to_hex().to_string(),
        };
        assert!(
            VerifiedRange::new(&authenticated, 16, Bytes::from_static(b"abce")).is_err(),
            "range bytes must be authenticated before becoming readable"
        );
        let overflowing = AuthenticatedRange {
            offset: u64::MAX - 1,
            length: 4,
            checksum: blake3::hash(b"abcd").to_hex().to_string(),
        };
        assert!(
            VerifiedRange::new(&overflowing, u64::MAX - 1, Bytes::from_static(b"abcd")).is_err(),
            "range end overflow must fail before constructing a chunk reader"
        );
        let verified = VerifiedRange::new(&authenticated, 16, verified_bytes).unwrap();
        let reader = BoundedChunkReader::new(64, vec![verified]).unwrap();
        assert_eq!(
            reader.get_bytes(16, 4).unwrap(),
            Bytes::from_static(b"abcd")
        );
        assert!(reader.get_bytes(15, 2).is_err());
        assert!(reader.get_bytes(18, 4).is_err());
    }

    fn directory_options() -> DirectoryPackOptions {
        DirectoryPackOptions {
            hard_max_object_bytes: 64 * 1024 * 1024,
            target_batch_bytes: 4 * 1024,
            hard_max_batch_bytes: 8 * 1024,
        }
    }

    fn directory_rows_for_partition(
        partition: DirectoryPartition,
        count: usize,
        cells: u32,
    ) -> Vec<DirectoryRow> {
        let mut rows = Vec::with_capacity(count);
        let mut candidate = 0_u64;
        while rows.len() < count {
            let id = format!("directory-row-{candidate:08}").into_bytes();
            candidate += 1;
            if DirectoryPartition::for_record_id(&id) != partition {
                continue;
            }
            let ordinal = u32::try_from(rows.len()).unwrap() % cells;
            rows.push(DirectoryRow {
                record_id: id,
                routing_epoch: 11,
                cell_ordinal: ordinal,
                state: MutationState::new(stamp(rows.len()), MutationOperation::Put),
            });
        }
        rows.sort_by(|left, right| left.record_id.cmp(&right.record_id));
        rows
    }

    fn directory_object_map(
        packed: &super::PackedDirectoryPartitionRun,
    ) -> BTreeMap<String, Bytes> {
        BTreeMap::from([(packed.reference.artifact.path.clone(), packed.bytes.clone())])
    }

    fn row_bundle_object_map(packed: &super::PackedRowBundleRun) -> BTreeMap<String, Bytes> {
        let mut objects = packed
            .bundles
            .iter()
            .map(|bundle| (bundle.reference.artifact.path.clone(), bundle.bytes.clone()))
            .collect::<BTreeMap<_, _>>();
        for artifact in &packed.summary_shards {
            objects.insert(
                artifact.reference.path.clone(),
                Bytes::copy_from_slice(&artifact.bytes),
            );
        }
        objects.insert(
            packed.root.reference.path.clone(),
            Bytes::copy_from_slice(&packed.root.bytes),
        );
        objects.insert(
            packed.roster.reference.path.clone(),
            Bytes::copy_from_slice(&packed.roster.bytes),
        );
        objects
    }

    #[test]
    fn directory_partition_is_fixed_and_independent_of_logical_cell_count() {
        let id = b"stable-record-id";
        let expected = DirectoryPartition::for_record_id(id);
        for _logical_cells in [1, 2_048, 16_384] {
            assert_eq!(DirectoryPartition::for_record_id(id), expected);
        }
    }

    #[test]
    fn directory_lookup_uses_one_authenticated_ipc_batch_per_active_level() {
        let target = b"directory-target".to_vec();
        let partition = DirectoryPartition::for_record_id(&target);
        let mut rows = directory_rows_for_partition(partition, 256, 16_384);
        rows.push(DirectoryRow {
            record_id: target.clone(),
            routing_epoch: 11,
            cell_ordinal: 12_345,
            state: MutationState::new(stamp(90_000), MutationOperation::Put),
        });
        rows.sort_by(|left, right| left.record_id.cmp(&right.record_id));
        let packed =
            pack_directory_partition_run(partition, 0, &rows, directory_options()).unwrap();
        assert!(
            packed
                .reference
                .batches
                .iter()
                .all(|batch| batch.range.offset > 0),
            "dictionary-free Arrow batch ranges still begin after the file schema"
        );
        let objects = directory_object_map(&packed);
        let mut calls = BTreeMap::<String, usize>::new();
        let lookup = lookup_directory_owner(
            std::slice::from_ref(&packed.reference),
            &target,
            |path, range: Range<u64>| {
                *calls.entry(path.to_string()).or_default() += 1;
                let object = objects.get(path).unwrap();
                assert!(range.end - range.start <= directory_options().hard_max_batch_bytes);
                assert!(range.end - range.start < object.len() as u64);
                Ok(object
                    [usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()]
                    .to_vec())
            },
        )
        .unwrap();

        let DirectoryLookup::Found(owner) = lookup else {
            panic!("expected exact directory owner")
        };
        assert_eq!((owner.routing_epoch, owner.cell_ordinal), (11, 12_345));
        assert_eq!(calls.values().copied().sum::<usize>(), 1);
        assert!(calls.values().all(|calls| *calls <= 1));
    }

    #[test]
    fn directory_root_round_trips_authenticated_runs_and_rejects_corruption_or_level_overlap() {
        let partition = DirectoryPartition::for_record_id(b"directory-root-probe");
        let rows = directory_rows_for_partition(partition, 512, 16_384);
        let level_zero =
            pack_directory_partition_run(partition, 0, &rows, directory_options()).unwrap();
        let root = pack_directory_root(std::slice::from_ref(&level_zero.reference)).unwrap();
        let reopened =
            decode_directory_root(&root.reference, Bytes::copy_from_slice(&root.bytes)).unwrap();
        assert_eq!(
            reopened.as_slice(),
            std::slice::from_ref(&level_zero.reference)
        );

        let mut corrupted = root.bytes.clone();
        let midpoint = corrupted.len() / 2;
        corrupted[midpoint] ^= 0x40;
        assert!(
            decode_directory_root(&root.reference, Bytes::from(corrupted)).is_err(),
            "directory root bytes must authenticate before Parquet interpretation"
        );

        let duplicate_level =
            pack_directory_partition_run(partition, 0, &rows[0..256], directory_options()).unwrap();
        assert!(
            pack_directory_root(&[level_zero.reference, duplicate_level.reference]).is_err(),
            "one partition root may not contain overlapping active levels"
        );
    }

    #[test]
    fn directory_whole_object_overflow_is_not_retried_as_a_batch_sizing_problem() {
        let partition = DirectoryPartition::for_record_id(b"oversized-directory-partition");
        let mut rows = Vec::new();
        let mut candidate = 0_u64;
        while rows.len() < 3 {
            let mut id = vec![0_u8; 8 * 1024];
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"oversized-directory-row");
            hasher.update(&candidate.to_be_bytes());
            hasher.finalize_xof().fill(&mut id);
            candidate += 1;
            if DirectoryPartition::for_record_id(&id) == partition {
                rows.push(DirectoryRow {
                    record_id: id,
                    routing_epoch: 11,
                    cell_ordinal: rows.len() as u32,
                    state: MutationState::new(stamp(rows.len()), MutationOperation::Put),
                });
            }
        }
        rows.sort_by(|left, right| left.record_id.cmp(&right.record_id));
        let error = pack_directory_partition_run(
            partition,
            0,
            &rows,
            DirectoryPackOptions {
                hard_max_object_bytes: 16 * 1024,
                target_batch_bytes: 8 * 1024,
                hard_max_batch_bytes: 12 * 1024,
            },
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("split") && error.contains("object"),
            "whole-object overflow needs an explicit non-retryable error: {error}"
        );
    }

    #[test]
    fn directory_unknown_is_explicit_and_never_synthesizes_a_cell_owner() {
        let target = b"missing-directory-target".to_vec();
        let partition = DirectoryPartition::for_record_id(&target);
        let rows = directory_rows_for_partition(partition, 256, 16_384);
        let packed =
            pack_directory_partition_run(partition, 0, &rows, directory_options()).unwrap();
        let objects = directory_object_map(&packed);
        let mut range_gets = 0_usize;

        let lookup = lookup_directory_owner(
            std::slice::from_ref(&packed.reference),
            &target,
            |path, range: Range<u64>| {
                range_gets += 1;
                let object = objects.get(path).unwrap();
                Ok(object
                    [usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()]
                    .to_vec())
            },
        )
        .unwrap();

        assert_eq!(lookup, DirectoryLookup::Unknown);
        assert!(
            range_gets <= 1,
            "one active level may fetch at most one range"
        );
    }

    #[test]
    fn directory_levels_choose_newest_stamp_and_reject_equal_version_conflicts() {
        let id = b"directory-mvcc-id".to_vec();
        let partition = DirectoryPartition::for_record_id(&id);
        let version = |hlc| MutationVersion::from_parts(hlc, [9; 16]);
        let row = |hlc, digest, operation| DirectoryRow {
            record_id: id.clone(),
            routing_epoch: 11,
            cell_ordinal: 77,
            state: MutationState::new(MutationStamp::new(version(hlc), digest), operation),
        };
        let older = pack_directory_partition_run(
            partition,
            2,
            &[row(1, [1; 32], MutationOperation::Put)],
            directory_options(),
        )
        .unwrap();
        let newer = pack_directory_partition_run(
            partition,
            1,
            &[row(2, [2; 32], MutationOperation::Put)],
            directory_options(),
        )
        .unwrap();
        let deleted = pack_directory_partition_run(
            partition,
            0,
            &[row(3, [3; 32], MutationOperation::Delete)],
            directory_options(),
        )
        .unwrap();
        let objects = [older.clone(), newer.clone(), deleted.clone()]
            .into_iter()
            .map(|run| (run.reference.artifact.path, run.bytes))
            .collect::<BTreeMap<_, _>>();
        let runs = vec![deleted.reference, newer.reference, older.reference];

        let DirectoryLookup::Found(owner) =
            lookup_directory_owner(&runs, &id, |path, range: Range<u64>| {
                let object = objects.get(path).unwrap();
                Ok(object
                    [usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()]
                    .to_vec())
            })
            .unwrap()
        else {
            panic!("same-ID levels lost the newest state")
        };
        assert!(owner.state.is_deleted());
        assert_eq!(owner.state.stamp().version().hlc(), 3);

        let conflict_a = pack_directory_partition_run(
            partition,
            0,
            &[row(4, [4; 32], MutationOperation::Put)],
            directory_options(),
        )
        .unwrap();
        let conflict_b = pack_directory_partition_run(
            partition,
            1,
            &[row(4, [5; 32], MutationOperation::Put)],
            directory_options(),
        )
        .unwrap();
        let objects = [conflict_a.clone(), conflict_b.clone()]
            .into_iter()
            .map(|run| (run.reference.artifact.path, run.bytes))
            .collect::<BTreeMap<_, _>>();
        let error = lookup_directory_owner(
            &[conflict_a.reference, conflict_b.reference],
            &id,
            |path, range: Range<u64>| {
                let object = objects.get(path).unwrap();
                Ok(object
                    [usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()]
                    .to_vec())
            },
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("conflicting canonical digests"), "{error}");

        let duplicate = row(5, [6; 32], MutationOperation::Put);
        assert!(
            pack_directory_partition_run(
                partition,
                0,
                &[duplicate.clone(), duplicate],
                directory_options(),
            )
            .is_err(),
            "one immutable directory run must converge each ID to one state"
        );
    }

    #[test]
    fn bundle_compaction_reuses_directory_refs_and_point_owner_without_directory_writes() {
        let (batch, route_plan) = canonical_rows(128, 512);
        let original =
            pack_canonical_row_bundles(std::slice::from_ref(&batch), &route_plan, test_options())
                .unwrap();
        let id = b"row-00073".to_vec();
        let partition = DirectoryPartition::for_record_id(&id);
        let directory_rows = vec![DirectoryRow {
            record_id: id.clone(),
            routing_epoch: 11,
            cell_ordinal: 73,
            state: MutationState::new(stamp(73), MutationOperation::Put),
        }];
        let directory =
            pack_directory_partition_run(partition, 0, &directory_rows, directory_options())
                .unwrap();
        let directory_bytes_before = directory.bytes.clone();
        let directory_ref_before = directory.reference.clone();
        let mut compact_options = test_options();
        compact_options.target_bundle_bytes /= 2;
        let mut sink = CollectingRowBundleSink::default();
        let compacted = super::compact_canonical_row_bundles_to_sink(
            &[batch],
            &route_plan,
            0,
            compact_options,
            std::slice::from_ref(&directory.reference),
            &mut sink,
        )
        .unwrap();

        let original_refs = original
            .bundles
            .iter()
            .map(|bundle| bundle.reference.artifact.path.clone())
            .collect::<BTreeSet<_>>();
        let rewritten_refs = compacted
            .row_bundles
            .bundles
            .iter()
            .map(|bundle| bundle.reference.artifact.path.clone())
            .collect::<BTreeSet<_>>();
        assert!(
            original_refs.is_disjoint(&rewritten_refs),
            "compaction evidence must rewrite physical row-bundle objects"
        );
        assert!(
            sink.objects
                .keys()
                .all(|path| !path.starts_with("id-director")),
            "the observed compaction sink must receive zero directory writes"
        );

        assert_eq!(
            compacted.directory_runs.as_slice(),
            std::slice::from_ref(&directory_ref_before)
        );
        assert_eq!(compacted.metrics.directory_writes, 0);
        assert_eq!(directory.bytes, directory_bytes_before);
        let objects = directory_object_map(&directory);
        let DirectoryLookup::Found(owner) = lookup_directory_owner(
            std::slice::from_ref(&directory_ref_before),
            &id,
            |path, range: Range<u64>| {
                let object = objects.get(path).unwrap();
                Ok(object
                    [usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()]
                    .to_vec())
            },
        )
        .unwrap() else {
            panic!("directory owner disappeared across row-bundle compaction")
        };
        let row_bundle_objects = row_bundle_object_map(&compacted.row_bundles);
        let authority = primary_authority(owner);
        assert!(
            lookup_materialized_row(
                &compacted.row_bundles,
                &authority,
                &id,
                |path, range: Range<u64>| {
                    let object = row_bundle_objects.get(path).unwrap();
                    Ok(object[usize::try_from(range.start).unwrap()
                        ..usize::try_from(range.end).unwrap()]
                        .to_vec())
                },
            )
            .unwrap()
            .is_some()
        );
    }

    #[test]
    fn cold_open_verifies_recovery_roots_once_and_batches_bounded_point_lookup() {
        let (batch, route_plan) = canonical_rows(4_096, 256);
        let packed = pack_canonical_row_bundles(&[batch], &route_plan, test_options()).unwrap();
        let id = b"row-02073";
        let directory_partition = DirectoryPartition::for_record_id(id);
        let directory_run = pack_directory_partition_run(
            directory_partition,
            0,
            &[DirectoryRow {
                record_id: id.to_vec(),
                routing_epoch: 11,
                cell_ordinal: 2_073,
                state: MutationState::new(stamp(2_073), MutationOperation::Put),
            }],
            directory_options(),
        )
        .unwrap();
        let directory_root =
            pack_directory_root(std::slice::from_ref(&directory_run.reference)).unwrap();
        let staged =
            stage_row_bundle_generation(&[], &packed.run_ref, &directory_root.reference).unwrap();
        let mut objects = row_bundle_object_map(&packed);
        objects.insert(
            directory_root.reference.path.clone(),
            Bytes::copy_from_slice(&directory_root.bytes),
        );
        objects.insert(
            staged.root.reference.path.clone(),
            Bytes::copy_from_slice(&staged.root.bytes),
        );

        let generation = decode_generation_root(
            &staged.root.reference,
            objects.get(&staged.root.reference.path).unwrap().clone(),
        )
        .unwrap();
        assert_eq!(
            generation.active_runs.as_slice(),
            std::slice::from_ref(&packed.run_ref)
        );
        assert_eq!(generation.directory_root, directory_root.reference);
        let shard_refs = decode_summary_root(
            &generation.active_runs[0].summary_root,
            objects
                .get(&generation.active_runs[0].summary_root.path)
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(
            shard_refs.iter().map(|shard| shard.row_count).sum::<u64>(),
            generation.active_runs[0].summary_count
        );
        let roster = decode_roster(
            &generation.active_runs[0].roster,
            objects
                .get(&generation.active_runs[0].roster.path)
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(roster, packed.roster.artifacts);

        let mut open_batches = 0_usize;
        let opened = open_row_bundle_generation(
            &staged.root.reference,
            objects.get(&staged.root.reference.path).unwrap().clone(),
            |references| {
                open_batches += 1;
                Ok(references
                    .iter()
                    .map(|reference| objects.get(&reference.path).unwrap().clone())
                    .collect())
            },
        )
        .unwrap();
        assert_eq!(
            open_batches, 1,
            "all immutable roots should open in one batch"
        );
        assert_eq!(
            opened.directory_runs,
            vec![directory_run.reference.clone()],
            "the generation must be a complete directory recovery point"
        );

        let authority = MaterializedLookupAuthority::catalog(
            "primary",
            PositionedRouteProjectionKind::Primary,
            [3; 32],
            DirectoryOwnerState {
                routing_epoch: 11,
                cell_ordinal: 2_073,
                state: MutationState::new(stamp(2_073), MutationOperation::Put),
            },
        )
        .unwrap();
        let mut shard_batches = 0_usize;
        let mut range_batches = 0_usize;
        let mut bundle_full_reads = 0_u64;
        let found = lookup_materialized_row_opened(
            &opened,
            &authority,
            id,
            |references| {
                shard_batches += 1;
                assert_eq!(
                    references.len(),
                    1,
                    "one canonical point may intersect at most one non-overlapping shard per run"
                );
                assert!(
                    references
                        .iter()
                        .all(|reference| reference.path.starts_with("row-bundle-summary-shards/")),
                    "opened lookup must not refetch a roster or summary root"
                );
                Ok(references
                    .iter()
                    .map(|reference| objects.get(&reference.path).unwrap().clone())
                    .collect())
            },
            |requests| {
                range_batches += 1;
                Ok(requests
                    .iter()
                    .map(|request| {
                        let object = objects.get(&request.path).unwrap();
                        if request.path.starts_with("row-bundles/")
                            && request.range == (0..object.len() as u64)
                        {
                            bundle_full_reads += 1;
                        }
                        Bytes::copy_from_slice(
                            &object[usize::try_from(request.range.start).unwrap()
                                ..usize::try_from(request.range.end).unwrap()],
                        )
                    })
                    .collect())
            },
        )
        .unwrap();
        assert!(found.is_some());
        assert_eq!(
            shard_batches, 1,
            "intersecting shards must be batch fetched"
        );
        assert!(
            range_batches <= 2,
            "footer and data ranges must be batch fetched"
        );
        assert_eq!(
            bundle_full_reads, 0,
            "point lookup must use authenticated bundle ranges"
        );

        drop(packed);
        drop(staged);
        assert!(
            lookup_materialized_row_opened(
                &opened,
                &authority,
                id,
                |references| {
                    Ok(references
                        .iter()
                        .map(|reference| objects.get(&reference.path).unwrap().clone())
                        .collect())
                },
                |requests| {
                    Ok(requests
                        .iter()
                        .map(|request| {
                            let object = objects.get(&request.path).unwrap();
                            Bytes::copy_from_slice(
                                &object[usize::try_from(request.range.start).unwrap()
                                    ..usize::try_from(request.range.end).unwrap()],
                            )
                        })
                        .collect())
                },
            )
            .unwrap()
            .is_some(),
            "lookup must not depend on construction-time Packed state"
        );
    }

    #[test]
    fn authority_roots_and_shards_reject_overlapping_canonical_bounds() {
        let (batch, route_plan) = canonical_rows(4_096, 256);
        let packed = pack_canonical_row_bundles(&[batch], &route_plan, test_options()).unwrap();
        let mut summaries = packed
            .bundles
            .iter()
            .flat_map(|bundle| bundle.reference.row_groups.iter().cloned())
            .collect::<Vec<_>>();
        assert!(summaries.len() > 1);
        summaries[1].modality = summaries[0].modality.clone();
        summaries[1].projection_kind = summaries[0].projection_kind;
        summaries[1].assignment_kind = summaries[0].assignment_kind;
        summaries[1].assignment_checksum = summaries[0].assignment_checksum;
        summaries[1].routing_epoch = summaries[0].routing_epoch;
        summaries[1].min_cell_ordinal = summaries[0].max_cell_ordinal;
        summaries[1].first_record_id = summaries[0].last_record_id.clone();
        assert!(
            validate_summary_shard_non_overlap(&summaries).is_err(),
            "a shard row beginning inside the prior row's bound must fail closed"
        );

        let mut shards = decode_summary_root(
            &packed.root.reference,
            Bytes::copy_from_slice(&packed.root.bytes),
        )
        .unwrap();
        assert!(shards.len() > 1);
        shards[1].first = shards[0].last.clone();
        assert!(
            validate_summary_root_non_overlap(&shards).is_err(),
            "a summary-root shard beginning inside the prior shard must fail closed"
        );
    }

    #[test]
    fn authority_decoder_checks_footer_row_cap_before_batch_materialization() {
        let (batch, route_plan) = canonical_rows(64, 64);
        let packed = pack_canonical_row_bundles(&[batch], &route_plan, test_options()).unwrap();
        let staged = stage_row_bundle_generation(
            &[],
            &packed.run_ref,
            &artifact_ref("directory-root", 4_096),
        )
        .unwrap();
        let error = decode_exact_parquet_artifact(
            &staged.root.reference,
            Bytes::copy_from_slice(&staged.root.bytes),
            &generation_root_schema(),
            "row-cap test",
            0,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("row") && error.contains("cap"), "{error}");
    }

    #[test]
    fn writer_options_and_cold_refs_cannot_exceed_format_hard_caps() {
        let (batch, route_plan) = canonical_rows(1, 64);
        let mut row_options = test_options();
        row_options.target_bundle_bytes = 128 * 1024 * 1024 + 1;
        row_options.hard_max_bundle_bytes = row_options.target_bundle_bytes;
        let error = pack_canonical_row_bundles(&[batch], &route_plan, row_options)
            .unwrap_err()
            .to_string();
        assert!(error.contains("format") && error.contains("cap"), "{error}");

        let id = b"directory-format-cap".to_vec();
        let partition = DirectoryPartition::for_record_id(&id);
        let row = DirectoryRow {
            record_id: id.clone(),
            routing_epoch: 11,
            cell_ordinal: 1,
            state: MutationState::new(stamp(1), MutationOperation::Put),
        };
        let mut directory_options = directory_options();
        directory_options.hard_max_object_bytes = 64 * 1024 * 1024 + 1;
        let error = pack_directory_partition_run(
            partition,
            0,
            std::slice::from_ref(&row),
            directory_options,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("format") && error.contains("cap"), "{error}");

        let mut directory = pack_directory_partition_run(
            partition,
            0,
            std::slice::from_ref(&row),
            super::DirectoryPackOptions {
                hard_max_object_bytes: 64 * 1024 * 1024,
                target_batch_bytes: 4 * 1024,
                hard_max_batch_bytes: 8 * 1024,
            },
        )
        .unwrap();
        directory.reference.artifact.encoded_bytes = 64 * 1024 * 1024 + 1;
        let error = lookup_directory_owner(
            std::slice::from_ref(&directory.reference),
            &id,
            |_path, _range| Ok(Vec::new()),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("format") && error.contains("cap"), "{error}");

        let (batch, route_plan) = canonical_rows(64, 64);
        let packed = pack_canonical_row_bundles(&[batch], &route_plan, test_options()).unwrap();
        let mut oversized_root = packed.root.reference.clone();
        oversized_root.encoded_bytes = 64 * 1024 * 1024 + 1;
        let error =
            decode_summary_root(&oversized_root, Bytes::copy_from_slice(&packed.root.bytes))
                .unwrap_err()
                .to_string();
        assert!(error.contains("format") && error.contains("cap"), "{error}");

        let mut oversized_directory_root = artifact_ref("directory-root", 64 * 1024 * 1024 + 1);
        oversized_directory_root.path = "id-directory-roots/oversized.parquet".to_string();
        let error = stage_row_bundle_generation(&[], &packed.run_ref, &oversized_directory_root)
            .unwrap_err()
            .to_string();
        assert!(error.contains("format") && error.contains("cap"), "{error}");
    }

    #[test]
    fn decoded_row_group_cannot_exceed_authenticated_summary_count() {
        let (batch, route_plan) = canonical_rows(64, 64);
        let mut packed = pack_canonical_row_bundles(&[batch], &route_plan, test_options()).unwrap();
        let summary = &mut packed.bundles[0].reference.row_groups[0];
        summary.row_count = summary.row_count.saturating_sub(1);
        let owner = super::DirectoryOwnerState {
            routing_epoch: 11,
            cell_ordinal: 0,
            state: MutationState::new(stamp(0), MutationOperation::Put),
        };
        let authority = primary_authority(owner);
        let objects = row_bundle_object_map(&packed);
        let footer = packed.bundles[0].reference.footer.clone();
        let mut fetched = Vec::new();
        let error = lookup_materialized_row(
            &packed,
            &authority,
            b"row-00000",
            |path, range: Range<u64>| {
                fetched.push((path.to_string(), range.clone()));
                let object = objects.get(path).unwrap();
                Ok(object
                    [usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()]
                    .to_vec())
            },
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("row count") || error.contains("summary"),
            "{error}"
        );
        assert_eq!(
            fetched,
            [(
                packed.bundles[0].reference.artifact.path.clone(),
                footer.offset..footer.offset + footer.length,
            )],
            "footer row-count authority must reject before any row-group data fetch"
        );
    }

    #[test]
    fn point_lookup_does_not_fetch_unused_page_index_ranges() {
        let (batch, route_plan) = canonical_rows(64, 64);
        let mut packed = pack_canonical_row_bundles(&[batch], &route_plan, test_options()).unwrap();
        let ignored_bytes = packed.bundles[0].bytes.slice(0..4);
        let ignored = AuthenticatedRange {
            offset: 0,
            length: ignored_bytes.len() as u64,
            checksum: blake3::hash(&ignored_bytes).to_hex().to_string(),
        };
        packed.bundles[0].reference.row_groups[0]
            .page_indexes
            .push(ignored.clone());
        let objects = row_bundle_object_map(&packed);
        let authority = primary_authority(DirectoryOwnerState {
            routing_epoch: 11,
            cell_ordinal: 0,
            state: MutationState::new(stamp(0), MutationOperation::Put),
        });
        let mut fetched = Vec::new();
        assert!(
            lookup_materialized_row(
                &packed,
                &authority,
                b"row-00000",
                |path, range: Range<u64>| {
                    fetched.push((path.to_string(), range.clone()));
                    let object = objects.get(path).unwrap();
                    Ok(object[usize::try_from(range.start).unwrap()
                        ..usize::try_from(range.end).unwrap()]
                        .to_vec())
                },
            )
            .unwrap()
            .is_some()
        );
        assert!(
            !fetched.iter().any(|(_, range)| range == &(0..4)),
            "page-index decoding is disabled, so its authenticated bytes must not be fetched"
        );
    }

    #[test]
    fn authenticated_footer_without_trailing_par1_fails_closed() {
        let (batch, route_plan) = canonical_rows(8, 64);
        let mut packed = pack_canonical_row_bundles(&[batch], &route_plan, test_options()).unwrap();
        let bundle = &mut packed.bundles[0];
        let mut bytes = bundle.bytes.to_vec();
        *bytes.last_mut().unwrap() ^= 0x01;
        bundle.bytes = Bytes::from(bytes);
        bundle.reference.artifact.checksum = blake3::hash(&bundle.bytes).to_hex().to_string();
        let footer_start = usize::try_from(bundle.reference.footer.offset).unwrap();
        bundle.reference.footer.checksum = blake3::hash(&bundle.bytes[footer_start..])
            .to_hex()
            .to_string();
        let objects = row_bundle_object_map(&packed);
        let authority = primary_authority(DirectoryOwnerState {
            routing_epoch: 11,
            cell_ordinal: 0,
            state: MutationState::new(stamp(0), MutationOperation::Put),
        });
        let error = lookup_materialized_row(
            &packed,
            &authority,
            b"row-00000",
            |path, range: Range<u64>| {
                let object = objects.get(path).unwrap();
                Ok(object
                    [usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()]
                    .to_vec())
            },
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("PAR1"), "{error}");
    }
}
