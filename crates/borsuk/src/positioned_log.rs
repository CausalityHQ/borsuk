use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use object_store::{ObjectStore, UpdateVersion};
use rayon::prelude::*;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    BorsukError, RequestCounts, Result,
    format::{
        DecodedPositionedEnvelope, positioned_envelope_from_parquet,
        positioned_envelope_to_parquet, positioned_payload_metadata,
    },
    storage::Storage,
};

/// Number of independent commit-source shards represented in V12 coverage.
pub const SOURCE_SHARD_COUNT: u8 = 64;
pub(crate) const INITIAL_POSITIONED_SOURCE_EPOCH: u64 = 1;
const MAX_COMMIT_SOURCE_RANGES: usize = SOURCE_SHARD_COUNT as usize * u64::BITS as usize;
const POSITIONED_LOG_LAYOUT: u16 = 16;
const MATERIALIZED_PREFIX_EMPTY_DOMAIN: &[u8] = b"borsuk.positioned.materialized-prefix.empty.v1\0";
const MATERIALIZED_PREFIX_EXTEND_DOMAIN: &[u8] =
    b"borsuk.positioned.materialized-prefix.extend.v1\0";
/// Maximum authoritative bytes in one serialized JSON shard head.
pub const MAX_SHARD_HEAD_BYTES: usize = 64 * 1024;
/// Maximum unmaterialized envelope references retained by one shard.
pub const MAX_PENDING_ENVELOPES_PER_SHARD: usize = 64;
/// Maximum materialized commit receipts retained for bounded idempotency.
pub const MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD: usize = 64;
/// Maximum encoded payload bytes awaiting materialization in one shard.
pub const MAX_UNMATERIALIZED_BYTES_PER_SHARD: u64 = 64 * 1024 * 1024;
/// Maximum logical payload rows awaiting materialization in one shard.
pub const MAX_UNMATERIALIZED_ROWS_PER_SHARD: u64 = 65_536;
/// Maximum typed payload objects referenced by one transaction.
pub const MAX_PAYLOADS_PER_TRANSACTION: usize = 64;
/// Maximum aggregate encoded payload bytes accepted by one append.
pub const MAX_APPEND_ENCODED_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum aggregate typed payload rows accepted by one append.
pub const MAX_APPEND_ROWS: u64 = 65_536;
/// Maximum conditional-head publication attempts before reporting contention.
pub const MAX_HEAD_CAS_ATTEMPTS: usize = 16;

/// Exact authenticated progress through one positioned-source shard.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PositionedMaterializationWatermark {
    sequence: u64,
    prefix_digest: String,
}

impl PositionedMaterializationWatermark {
    /// Return the fixed domain-separated identity of an empty source prefix.
    pub fn empty() -> Self {
        Self {
            sequence: 0,
            prefix_digest: blake3::hash(MATERIALIZED_PREFIX_EMPTY_DOMAIN)
                .to_hex()
                .to_string(),
        }
    }

    /// Reconstitute and validate persisted positioned progress.
    pub fn from_parts(sequence: u64, prefix_digest: String) -> Result<Self> {
        let watermark = Self {
            sequence,
            prefix_digest,
        };
        watermark.validate()?;
        Ok(watermark)
    }

    /// Highest source sequence included in this authenticated prefix.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Rolling BLAKE3 digest of every included positioned envelope identity.
    pub fn prefix_digest(&self) -> &str {
        &self.prefix_digest
    }

    /// Extend this watermark with the next exact positioned envelope checksum.
    pub fn advanced(
        &self,
        source_epoch: u64,
        shard: u8,
        sequence: u64,
        envelope_checksum: &str,
    ) -> Result<Self> {
        self.validate()?;
        validate_source_epoch_and_shard(source_epoch, shard)?;
        validate_hex("positioned envelope checksum", envelope_checksum)?;
        if self.sequence.checked_add(1) != Some(sequence) {
            return invalid("positioned prefix watermark extension is not contiguous");
        }
        let prior = blake3::Hash::from_hex(&self.prefix_digest).map_err(|_| {
            BorsukError::InvalidStorage(
                "positioned prefix watermark contains an invalid digest".to_owned(),
            )
        })?;
        let envelope = blake3::Hash::from_hex(envelope_checksum).map_err(|_| {
            BorsukError::InvalidStorage(
                "positioned envelope checksum is not a BLAKE3 digest".to_owned(),
            )
        })?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(MATERIALIZED_PREFIX_EXTEND_DOMAIN);
        hasher.update(prior.as_bytes());
        hasher.update(&source_epoch.to_le_bytes());
        hasher.update(&[shard]);
        hasher.update(&sequence.to_le_bytes());
        hasher.update(envelope.as_bytes());
        Ok(Self {
            sequence,
            prefix_digest: hasher.finalize().to_hex().to_string(),
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_hex("positioned materialized prefix digest", &self.prefix_digest)?;
        let is_empty_digest = self.prefix_digest
            == blake3::hash(MATERIALIZED_PREFIX_EMPTY_DOMAIN)
                .to_hex()
                .as_str();
        if self.sequence == 0 && !is_empty_digest {
            return invalid("positioned sequence-zero watermark has a nonempty prefix digest");
        }
        if self.sequence > 0 && is_empty_digest {
            return invalid("positioned nonzero watermark has the fixed empty prefix digest");
        }
        Ok(())
    }
}

/// A single durable commit source position.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
#[allow(
    dead_code,
    reason = "Task 5 consumes positioned source positions during the atomic V12 leaf-format cutover"
)]
pub struct CommitSourcePosition {
    /// Durable source incarnation containing this position.
    pub source_epoch: u64,
    /// Fixed authoritative shard within the source epoch.
    pub shard: u8,
    /// Positive, contiguous sequence within the shard.
    pub sequence: u64,
}

#[allow(
    dead_code,
    reason = "Task 5 consumes positioned source positions during the atomic V12 leaf-format cutover"
)]
impl CommitSourcePosition {
    /// Construct and validate one positive positioned source coordinate.
    pub fn new(source_epoch: u64, shard: u8, sequence: u64) -> Result<Self> {
        let position = Self {
            source_epoch,
            shard,
            sequence,
        };
        position.validate()?;
        Ok(position)
    }

    fn validate(&self) -> Result<()> {
        validate_source_epoch_and_shard(self.source_epoch, self.shard)?;
        if self.sequence == 0 {
            return invalid("V12 commit source sequence must be positive");
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for CommitSourcePosition {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WirePosition {
            source_epoch: u64,
            shard: u8,
            sequence: u64,
        }

        let wire = WirePosition::deserialize(deserializer)?;
        Self::new(wire.source_epoch, wire.shard, wire.sequence).map_err(serde::de::Error::custom)
    }
}

/// An inclusive sequence range within one durable source epoch and shard.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitSourceRange {
    pub(crate) source_epoch: u64,
    pub(crate) shard: u8,
    pub(crate) first_sequence: u64,
    pub(crate) last_sequence: u64,
}

impl CommitSourceRange {
    pub(crate) fn new(
        source_epoch: u64,
        shard: u8,
        first_sequence: u64,
        last_sequence: u64,
    ) -> Result<Self> {
        let range = Self {
            source_epoch,
            shard,
            first_sequence,
            last_sequence,
        };
        range.validate()?;
        Ok(range)
    }

    fn validate(&self) -> Result<()> {
        validate_source_epoch_and_shard(self.source_epoch, self.shard)?;
        if self.first_sequence == 0 || self.last_sequence == 0 {
            return invalid("V12 commit source sequences must be positive");
        }
        if self.first_sequence > self.last_sequence {
            return invalid("V12 commit source first sequence exceeds last sequence");
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for CommitSourceRange {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireRange {
            source_epoch: u64,
            shard: u8,
            first_sequence: u64,
            last_sequence: u64,
        }

        let wire = WireRange::deserialize(deserializer)?;
        Self::new(
            wire.source_epoch,
            wire.shard,
            wire.first_sequence,
            wire.last_sequence,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Canonical bounded V12 source coverage.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitSourceRangeSet {
    ranges: Vec<CommitSourceRange>,
}

impl CommitSourceRangeSet {
    pub(crate) fn new(mut ranges: Vec<CommitSourceRange>) -> Result<Self> {
        ranges.sort_unstable_by_key(|range| {
            (
                range.source_epoch,
                range.shard,
                range.first_sequence,
                range.last_sequence,
            )
        });

        let mut canonical = Vec::<CommitSourceRange>::with_capacity(ranges.len());
        for range in ranges {
            range.validate()?;
            if let Some(left) = canonical.last_mut()
                && left.source_epoch == range.source_epoch
                && left.shard == range.shard
            {
                if left.last_sequence >= range.first_sequence {
                    return invalid("V12 commit source ranges overlap within one source shard");
                }
                if left.last_sequence.checked_add(1) == Some(range.first_sequence) {
                    left.last_sequence = range.last_sequence;
                    continue;
                }
            }
            canonical.push(range);
        }
        validate_range_count(canonical.len())?;
        Ok(Self { ranges: canonical })
    }

    #[allow(
        dead_code,
        reason = "Task 5 inspects V12 positioned source coverage during leaf publication"
    )]
    pub(crate) fn ranges(&self) -> &[CommitSourceRange] {
        &self.ranges
    }

    pub(crate) fn subtract(&self, covered: &Self) -> Result<CommitSourceCoverageDifference> {
        self.validate_canonical()?;
        covered.validate_canonical()?;
        let mut any_overlap = false;
        let mut remaining = Vec::new();
        for candidate in &self.ranges {
            let mut fragments = vec![*candidate];
            for cover in covered.ranges.iter().filter(|cover| {
                cover.source_epoch == candidate.source_epoch && cover.shard == candidate.shard
            }) {
                let mut next_fragments =
                    Vec::with_capacity(fragments.len().checked_add(1).ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "V12 commit source fragment count overflow".to_owned(),
                        )
                    })?);
                for fragment in fragments {
                    if cover.last_sequence < fragment.first_sequence
                        || cover.first_sequence > fragment.last_sequence
                    {
                        next_fragments.push(fragment);
                        continue;
                    }
                    any_overlap = true;
                    if fragment.first_sequence < cover.first_sequence {
                        next_fragments.push(CommitSourceRange::new(
                            fragment.source_epoch,
                            fragment.shard,
                            fragment.first_sequence,
                            cover.first_sequence.checked_sub(1).ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "V12 commit source subtraction underflow".to_owned(),
                                )
                            })?,
                        )?);
                    }
                    if cover.last_sequence < fragment.last_sequence {
                        next_fragments.push(CommitSourceRange::new(
                            fragment.source_epoch,
                            fragment.shard,
                            cover.last_sequence.checked_add(1).ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "V12 commit source subtraction overflow".to_owned(),
                                )
                            })?,
                            fragment.last_sequence,
                        )?);
                    }
                }
                fragments = next_fragments;
            }
            remaining.extend(fragments);
        }
        if remaining.is_empty() {
            Ok(CommitSourceCoverageDifference::FullyCovered)
        } else {
            let difference = Self::new(remaining)?;
            Ok(if any_overlap {
                CommitSourceCoverageDifference::Partial(difference)
            } else {
                CommitSourceCoverageDifference::Disjoint(difference)
            })
        }
    }

    #[allow(
        dead_code,
        reason = "Task 5 unions V12 positioned source coverage during leaf publication"
    )]
    pub(crate) fn union_disjoint(&self, other: &Self) -> Result<Self> {
        let mut ranges = Vec::with_capacity(
            self.ranges
                .len()
                .checked_add(other.ranges.len())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V12 commit source range count overflow".to_owned())
                })?,
        );
        ranges.extend_from_slice(&self.ranges);
        ranges.extend_from_slice(&other.ranges);
        Self::new(ranges)
    }

    #[allow(
        dead_code,
        reason = "Task 5 validates V12 positioned source coverage during leaf publication"
    )]
    pub(crate) fn covers(&self, candidate: &Self) -> bool {
        matches!(
            candidate.subtract(self),
            Ok(CommitSourceCoverageDifference::FullyCovered)
        )
    }

    pub(crate) fn validate_canonical(&self) -> Result<()> {
        validate_range_count(self.ranges.len())?;
        for range in &self.ranges {
            range.validate()?;
        }
        for pair in self.ranges.windows(2) {
            let left = pair[0];
            let right = pair[1];
            if source_range_sort_key(left) >= source_range_sort_key(right) {
                return invalid("V12 commit source ranges must be sorted canonically");
            }
            if left.source_epoch == right.source_epoch && left.shard == right.shard {
                if left.last_sequence >= right.first_sequence {
                    return invalid("V12 commit source ranges overlap within one source shard");
                }
                if left.last_sequence.checked_add(1) == Some(right.first_sequence) {
                    return invalid("V12 commit source ranges must coalesce exact adjacency");
                }
            }
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for CommitSourceRangeSet {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireRangeSet {
            ranges: Vec<CommitSourceRange>,
        }

        let wire = WireRangeSet::deserialize(deserializer)?;
        let set = Self {
            ranges: wire.ranges,
        };
        set.validate_canonical().map_err(serde::de::Error::custom)?;
        Ok(set)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CommitSourceCoverageDifference {
    FullyCovered,
    Disjoint(CommitSourceRangeSet),
    Partial(CommitSourceRangeSet),
}

fn validate_source_epoch_and_shard(source_epoch: u64, shard: u8) -> Result<()> {
    if source_epoch == 0 {
        return invalid("V12 commit source epoch must be positive");
    }
    if shard >= SOURCE_SHARD_COUNT {
        return invalid("V12 commit source shard is outside the fixed shard count");
    }
    Ok(())
}

fn validate_range_count(range_count: usize) -> Result<()> {
    if range_count > MAX_COMMIT_SOURCE_RANGES {
        return invalid("V12 commit source coverage exceeds its fixed metadata bound");
    }
    Ok(())
}

fn source_range_sort_key(range: CommitSourceRange) -> (u64, u8, u64, u64) {
    (
        range.source_epoch,
        range.shard,
        range.first_sequence,
        range.last_sequence,
    )
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
/// Closed logical modality carried by a positioned mutation payload.
pub enum PositionedMutationModality {
    /// Primary dense-vector mutation rows.
    PrimaryDense,
    /// Named dense-vector mutation rows.
    NamedDense,
    /// Sparse-vector mutation rows.
    Sparse,
    /// Lexical text and statistics mutation rows.
    Text,
    /// Late-interaction token-vector mutation rows.
    LateInteraction,
    /// Exact-ID directory mutation rows.
    IdDirectory,
    /// Deletion/tombstone mutation rows.
    Tombstone,
    /// Authenticated per-modality route assignments for the transaction.
    RoutePlan,
}

impl PositionedMutationModality {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PrimaryDense => "primary_dense",
            Self::NamedDense => "named_dense",
            Self::Sparse => "sparse",
            Self::Text => "text",
            Self::LateInteraction => "late_interaction",
            Self::IdDirectory => "id_directory",
            Self::Tombstone => "tombstone",
            Self::RoutePlan => "route_plan",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "primary_dense" => Ok(Self::PrimaryDense),
            "named_dense" => Ok(Self::NamedDense),
            "sparse" => Ok(Self::Sparse),
            "text" => Ok(Self::Text),
            "late_interaction" => Ok(Self::LateInteraction),
            "id_directory" => Ok(Self::IdDirectory),
            "tombstone" => Ok(Self::Tombstone),
            "route_plan" => Ok(Self::RoutePlan),
            _ => invalid(&format!("unknown positioned mutation modality `{value}`")),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
/// Stock typed container accepted for positioned payloads.
pub enum PositionedPayloadFormat {
    /// Apache Arrow streaming IPC.
    ArrowIpc,
    /// Apache Parquet.
    Parquet,
}

impl PositionedPayloadFormat {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ArrowIpc => "arrow-ipc",
            Self::Parquet => "parquet",
        }
    }

    pub(crate) const fn extension(self) -> &'static str {
        match self {
            Self::ArrowIpc => "arrow",
            Self::Parquet => "parquet",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "arrow-ipc" => Ok(Self::ArrowIpc),
            "parquet" => Ok(Self::Parquet),
            _ => invalid(&format!("unknown positioned payload format `{value}`")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Caller-owned typed payload submitted as part of one atomic transaction.
pub struct PositionedMutationPayloadInput {
    /// Logical modality represented by the rows.
    pub modality: PositionedMutationModality,
    /// Nonempty bounded semantic object role.
    pub role: String,
    /// Optional authenticated ID-membership bloom stored in the Parquet envelope.
    pub id_bloom: Vec<u8>,
    /// Typed physical container format.
    pub format: PositionedPayloadFormat,
    /// Complete encoded container bytes.
    pub bytes: Vec<u8>,
    /// Declared rows, verified against the typed container.
    pub rows: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
/// Canonical content-addressed reference persisted in a transaction envelope.
pub struct PositionedMutationPayloadRef {
    /// Logical payload modality.
    pub modality: PositionedMutationModality,
    /// Bounded semantic object role.
    pub role: String,
    /// Optional authenticated ID-membership bloom stored in the Parquet envelope.
    pub id_bloom: Vec<u8>,
    /// Typed physical container format.
    pub format: PositionedPayloadFormat,
    /// Deterministic checksum-derived object path.
    pub path: String,
    /// Lowercase BLAKE3 checksum of the encoded object.
    pub checksum: String,
    /// Verified typed row count.
    pub rows: u64,
    /// Complete encoded object byte count.
    pub encoded_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// Canonical mutation ordering key plus equal-version conflict digest.
pub struct PositionedMutationStamp {
    /// Hybrid logical clock component.
    pub hlc: u64,
    /// Stable 128-bit writer identity.
    pub writer: [u8; 16],
    /// Canonical logical-mutation digest.
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Typed Parquet transaction envelope authorized by one shard-head reference.
pub struct PositionedMutationEnvelope {
    /// Caller transaction identity.
    pub transaction_id: String,
    /// Exact schema identity for all referenced payloads.
    pub schema_fingerprint: String,
    /// Durable source coordinate assigned by the winning head CAS.
    pub position: CommitSourcePosition,
    /// Least canonical mutation stamp carried by the payloads.
    pub min_stamp: PositionedMutationStamp,
    /// Greatest canonical mutation stamp carried by the payloads.
    pub max_stamp: PositionedMutationStamp,
    /// Strictly canonical content-addressed payload references.
    pub payloads: Vec<PositionedMutationPayloadRef>,
}

impl PositionedMutationEnvelope {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_bounded_utf8("transaction ID", &self.transaction_id)?;
        validate_hex("schema fingerprint", &self.schema_fingerprint)?;
        self.position.validate()?;
        if self.min_stamp > self.max_stamp {
            return invalid("positioned envelope minimum mutation stamp exceeds maximum");
        }
        if self.payloads.is_empty() || self.payloads.len() > MAX_PAYLOADS_PER_TRANSACTION {
            return invalid("positioned envelope payload count is outside its fixed bound");
        }
        for payload in &self.payloads {
            validate_payload_ref(payload)?;
        }
        if !self
            .payloads
            .windows(2)
            .all(|pair| payload_sort_key(&pair[0]) < payload_sort_key(&pair[1]))
        {
            return invalid("positioned envelope payload references are not canonical");
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
/// Durable append acknowledgement and request telemetry.
pub struct CommittedPositionedMutation {
    /// Assigned durable source coordinate.
    pub position: CommitSourcePosition,
    /// Fixed identity digest derived from the transaction ID.
    pub transaction_digest: String,
    /// Digest of every canonical request field.
    pub request_digest: String,
    /// BLAKE3 checksum of the position-bearing Parquet envelope.
    pub envelope_checksum: String,
    /// Aggregate typed payload rows.
    pub rows: u64,
    /// Aggregate encoded payload bytes.
    pub encoded_bytes: u64,
    /// Payload bytes submitted to backing PUT attempts, including the envelope,
    /// shard-head CAS, and any retry amplification.
    pub put_payload_bytes: u64,
    /// Backing object-store requests issued by this append call.
    pub requests: RequestCounts,
}

impl PartialEq for CommittedPositionedMutation {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position
            && self.transaction_digest == other.transaction_digest
            && self.request_digest == other.request_digest
            && self.envelope_checksum == other.envelope_checksum
            && self.rows == other.rows
            && self.encoded_bytes == other.encoded_bytes
    }
}

impl Eq for CommittedPositionedMutation {}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Consistent read of all 64 authoritative heads and their pending envelopes.
pub struct PositionedLogSnapshot {
    /// Pending transactions ordered by durable source position.
    pub transactions: Vec<PositionedMutationEnvelope>,
    /// Authoritative envelope checksum aligned one-for-one with `transactions`.
    /// The canonical immutable envelope path is derived from this checksum.
    pub envelope_checksums: Vec<String>,
    /// BLAKE3 checksum of each authoritative JSON head.
    pub head_checksums: [String; SOURCE_SHARD_COUNT as usize],
    /// Durable sequence observed for every shard.
    pub durable_sequences: [u64; SOURCE_SHARD_COUNT as usize],
    /// Authenticated materialized prefix observed for every shard.
    pub materialized_watermarks: [PositionedMaterializationWatermark; SOURCE_SHARD_COUNT as usize],
    /// Collection generation authorizing each materialized sequence.
    pub materialized_collection_generations: [u64; SOURCE_SHARD_COUNT as usize],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PositionedCommitReference {
    transaction_digest: String,
    request_digest: String,
    envelope_checksum: String,
    sequence: u64,
    rows: u64,
    encoded_bytes: u64,
    materialized_collection_generation: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PositionedShardHead {
    layout: u16,
    source_epoch: u64,
    shard: u8,
    schema_fingerprint: String,
    durable_sequence: u64,
    materialized_sequence: u64,
    materialized_prefix_digest: String,
    materialized_collection_generation: u64,
    evicted_recent_through_collection_generation: u64,
    pending_rows: u64,
    pending_bytes: u64,
    pending: Vec<PositionedCommitReference>,
    recent: Vec<PositionedCommitReference>,
}

impl PositionedShardHead {
    fn empty(source_epoch: u64, shard: u8, schema_fingerprint: &str) -> Result<Self> {
        validate_source_epoch_and_shard(source_epoch, shard)?;
        validate_hex("schema fingerprint", schema_fingerprint)?;
        Ok(Self {
            layout: POSITIONED_LOG_LAYOUT,
            source_epoch,
            shard,
            schema_fingerprint: schema_fingerprint.to_owned(),
            durable_sequence: 0,
            materialized_sequence: 0,
            materialized_prefix_digest: PositionedMaterializationWatermark::empty().prefix_digest,
            materialized_collection_generation: 0,
            evicted_recent_through_collection_generation: 0,
            pending_rows: 0,
            pending_bytes: 0,
            pending: Vec::new(),
            recent: Vec::new(),
        })
    }

    fn validate(&self, expected_epoch: u64, expected_shard: u8) -> Result<()> {
        if self.layout != POSITIONED_LOG_LAYOUT {
            return invalid("positioned shard head has an unsupported layout marker");
        }
        if self.source_epoch != expected_epoch || self.shard != expected_shard {
            return invalid("positioned shard head epoch or shard does not match its authority");
        }
        validate_source_epoch_and_shard(self.source_epoch, self.shard)?;
        validate_hex("schema fingerprint", &self.schema_fingerprint)?;
        if self.materialized_sequence > self.durable_sequence {
            return invalid("positioned shard materialized sequence exceeds durable sequence");
        }
        PositionedMaterializationWatermark::from_parts(
            self.materialized_sequence,
            self.materialized_prefix_digest.clone(),
        )?;
        if (self.materialized_sequence == 0) != (self.materialized_collection_generation == 0) {
            return invalid(
                "positioned materialized collection generation must be zero exactly at sequence zero",
            );
        }
        if self.pending.len() > MAX_PENDING_ENVELOPES_PER_SHARD
            || self.recent.len() > MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD
        {
            return invalid("positioned shard head exceeds its fixed receipt bounds");
        }
        let mut pending_rows = 0_u64;
        let mut pending_bytes = 0_u64;
        let mut expected_sequence = self.materialized_sequence.checked_add(1);
        for reference in &self.pending {
            validate_commit_reference(reference)?;
            if reference.materialized_collection_generation != 0 {
                return invalid("positioned pending reference has a materialized generation");
            }
            if Some(reference.sequence) != expected_sequence {
                return invalid("positioned shard pending references are not a contiguous prefix");
            }
            expected_sequence = reference.sequence.checked_add(1);
            pending_rows = pending_rows.checked_add(reference.rows).ok_or_else(|| {
                BorsukError::InvalidStorage("positioned pending row total overflow".to_owned())
            })?;
            pending_bytes = pending_bytes
                .checked_add(reference.encoded_bytes)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("positioned pending byte total overflow".to_owned())
                })?;
        }
        if self
            .pending
            .last()
            .map_or(self.materialized_sequence, |reference| reference.sequence)
            != self.durable_sequence
        {
            return invalid("positioned shard pending references do not reach durable sequence");
        }
        if pending_rows != self.pending_rows || pending_bytes != self.pending_bytes {
            return invalid("positioned shard pending totals disagree with its references");
        }
        if pending_rows > MAX_UNMATERIALIZED_ROWS_PER_SHARD
            || pending_bytes > MAX_UNMATERIALIZED_BYTES_PER_SHARD
        {
            return invalid("positioned shard pending totals exceed their hard bounds");
        }
        for reference in &self.recent {
            validate_commit_reference(reference)?;
            if reference.sequence > self.materialized_sequence
                || reference.materialized_collection_generation == 0
                || reference.materialized_collection_generation
                    > self.materialized_collection_generation
            {
                return invalid("positioned recent receipt has not been materialized");
            }
        }
        let expected_recent_len = usize::try_from(
            self.materialized_sequence
                .min(MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD as u64),
        )
        .expect("bounded recent length fits usize");
        if self.recent.len() != expected_recent_len {
            return invalid("positioned recent receipts are not the exact bounded suffix");
        }
        if let Some(first) = self.recent.first() {
            let expected_first = self
                .materialized_sequence
                .checked_sub(u64::try_from(self.recent.len()).map_err(|_| {
                    BorsukError::InvalidStorage(
                        "positioned recent receipt count exceeds u64".to_owned(),
                    )
                })?)
                .and_then(|sequence| sequence.checked_add(1))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "positioned recent suffix arithmetic overflow".to_owned(),
                    )
                })?;
            if first.sequence != expected_first
                || self.recent.last().map(|reference| reference.sequence)
                    != Some(self.materialized_sequence)
                || self
                    .recent
                    .last()
                    .map(|reference| reference.materialized_collection_generation)
                    != Some(self.materialized_collection_generation)
            {
                return invalid("positioned recent receipts are not the canonical suffix");
            }
        }
        if !self.recent.windows(2).all(|pair| {
            pair[0].sequence.checked_add(1) == Some(pair[1].sequence)
                && pair[0].materialized_collection_generation
                    <= pair[1].materialized_collection_generation
        }) {
            return invalid("positioned recent receipts contain a sequence or generation gap");
        }
        if self.materialized_sequence > MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD as u64 {
            if self.evicted_recent_through_collection_generation == 0
                || self.recent.first().is_some_and(|reference| {
                    self.evicted_recent_through_collection_generation
                        > reference.materialized_collection_generation
                })
            {
                return invalid("positioned evicted recent generation boundary is invalid");
            }
        } else if self.evicted_recent_through_collection_generation != 0 {
            return invalid("positioned head records impossible recent receipt eviction");
        }
        let mut transaction_digests = BTreeMap::new();
        for reference in self.pending.iter().chain(&self.recent) {
            if transaction_digests
                .insert(&reference.transaction_digest, &reference.request_digest)
                .is_some()
            {
                return invalid("positioned shard head contains a duplicate transaction receipt");
            }
        }
        Ok(())
    }
}

fn checkpointed_head(
    head: &PositionedShardHead,
    target: &PositionedMaterializationWatermark,
    collection_generation: u64,
) -> Result<PositionedShardHead> {
    head.validate(head.source_epoch, head.shard)?;
    target.validate()?;
    if target.sequence == 0 || collection_generation == 0 {
        return invalid(
            "positioned checkpoint sequence and collection generation must be positive",
        );
    }
    if target.sequence <= head.materialized_sequence {
        return invalid("positioned checkpoint does not advance the materialized prefix");
    }
    if target.sequence > head.durable_sequence {
        return invalid("positioned checkpoint exceeds durable sequence");
    }
    if collection_generation <= head.materialized_collection_generation {
        return invalid("positioned checkpoint collection generation must advance");
    }

    let split = head
        .pending
        .partition_point(|reference| reference.sequence <= target.sequence);
    let completed = &head.pending[..split];
    if completed.last().map(|reference| reference.sequence) != Some(target.sequence) {
        return invalid("positioned checkpoint must end on a contiguous pending sequence");
    }
    let mut observed = PositionedMaterializationWatermark::from_parts(
        head.materialized_sequence,
        head.materialized_prefix_digest.clone(),
    )?;
    for reference in completed {
        observed = observed.advanced(
            head.source_epoch,
            head.shard,
            reference.sequence,
            &reference.envelope_checksum,
        )?;
    }
    if &observed != target {
        return invalid(
            "positioned checkpoint target prefix digest does not match pending authority",
        );
    }

    let mut next = head.clone();
    let completed = next.pending.drain(..split).collect::<Vec<_>>();
    for mut reference in completed {
        next.pending_rows = next
            .pending_rows
            .checked_sub(reference.rows)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("positioned checkpoint row total underflow".to_owned())
            })?;
        next.pending_bytes = next
            .pending_bytes
            .checked_sub(reference.encoded_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("positioned checkpoint byte total underflow".to_owned())
            })?;
        reference.materialized_collection_generation = collection_generation;
        next.recent.push(reference);
    }
    if next.recent.len() > MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD {
        let evict = next.recent.len() - MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD;
        let evicted_through = next
            .recent
            .drain(..evict)
            .map(|reference| reference.materialized_collection_generation)
            .max()
            .unwrap_or(next.evicted_recent_through_collection_generation);
        next.evicted_recent_through_collection_generation = next
            .evicted_recent_through_collection_generation
            .max(evicted_through);
    }
    next.materialized_sequence = target.sequence;
    next.materialized_prefix_digest
        .clone_from(&target.prefix_digest);
    next.materialized_collection_generation = collection_generation;
    next.validate(next.source_epoch, next.shard)?;
    Ok(next)
}

#[derive(Clone)]
struct PinnedPositionedHead {
    head: PositionedShardHead,
    version: UpdateVersion,
}

#[derive(Clone)]
/// Pinned multi-shard positioned-log appender.
pub struct PositionedLogWriter {
    storage: Storage,
    source_epoch: u64,
    schema_fingerprint: String,
    heads: Arc<Vec<Mutex<PinnedPositionedHead>>>,
}

impl PositionedLogWriter {
    pub(crate) fn with_storage_scope(&self, storage: Storage) -> Self {
        Self {
            storage,
            source_epoch: self.source_epoch,
            schema_fingerprint: self.schema_fingerprint.clone(),
            heads: Arc::clone(&self.heads),
        }
    }

    /// Initialize all 64 authoritative heads for one new source epoch.
    ///
    /// The schema fingerprint is immutable for that epoch and is enforced by
    /// every subsequent head CAS. Starting a different schema requires a new
    /// source epoch rather than a mixed-schema pending log.
    pub fn create(
        uri: impl Into<String>,
        store: Arc<dyn ObjectStore>,
        source_epoch: u64,
        schema_fingerprint: &str,
    ) -> Result<Self> {
        if source_epoch == 0 {
            return invalid("positioned source epoch must be positive");
        }
        let storage = Storage::from_object_store(uri.into(), store)?;
        Self::create_from_storage(storage, source_epoch, schema_fingerprint)
    }

    pub(crate) fn create_from_storage(
        storage: Storage,
        source_epoch: u64,
        schema_fingerprint: &str,
    ) -> Result<Self> {
        if source_epoch == 0 {
            return invalid("positioned source epoch must be positive");
        }
        validate_hex("schema fingerprint", schema_fingerprint)?;
        let mut pinned = Vec::with_capacity(SOURCE_SHARD_COUNT as usize);
        for shard in 0..SOURCE_SHARD_COUNT {
            let head = PositionedShardHead::empty(source_epoch, shard, schema_fingerprint)?;
            let bytes = shard_head_bytes(&head)?;
            let path = shard_head_path(shard);
            let version = match storage.write_coordination_object(&path, &bytes, None) {
                Ok(version) => version,
                Err(
                    BorsukError::ConcurrentModification { .. }
                    | BorsukError::ObjectStoreRetryable { .. },
                ) => {
                    let stored = storage.read_coordination_object(&path)?.ok_or_else(|| {
                        BorsukError::InvalidStorage(format!(
                            "positioned shard head `{path}` disappeared during initialization"
                        ))
                    })?;
                    let existing = shard_head_from_bytes(&stored.bytes, source_epoch, shard)?;
                    if existing != head {
                        return invalid(
                            "positioned source initialization conflicts with an existing head",
                        );
                    }
                    stored.version
                }
                Err(error) => return Err(error),
            };
            pinned.push(Mutex::new(PinnedPositionedHead { head, version }));
        }
        Ok(Self {
            storage,
            source_epoch,
            schema_fingerprint: schema_fingerprint.to_owned(),
            heads: Arc::new(pinned),
        })
    }

    /// Open and pin every required authoritative head and backend version.
    pub fn open(
        uri: impl Into<String>,
        store: Arc<dyn ObjectStore>,
        source_epoch: u64,
    ) -> Result<Self> {
        Self::open_with_report(uri, store, source_epoch).map(|(writer, _)| writer)
    }

    /// Open all authoritative heads and report the exact cold-pin request cost.
    pub fn open_with_report(
        uri: impl Into<String>,
        store: Arc<dyn ObjectStore>,
        source_epoch: u64,
    ) -> Result<(Self, RequestCounts)> {
        if source_epoch == 0 {
            return invalid("positioned source epoch must be positive");
        }
        let storage = Storage::from_object_store(uri.into(), store)?;
        Self::open_from_storage_with_report(storage, source_epoch, None)
    }

    pub(crate) fn open_from_storage(
        storage: Storage,
        source_epoch: u64,
        schema_fingerprint: &str,
    ) -> Result<Self> {
        Self::open_from_storage_with_report(storage, source_epoch, Some(schema_fingerprint))
            .map(|(writer, _)| writer)
    }

    fn open_from_storage_with_report(
        storage: Storage,
        source_epoch: u64,
        expected_schema_fingerprint: Option<&str>,
    ) -> Result<(Self, RequestCounts)> {
        if source_epoch == 0 {
            return invalid("positioned source epoch must be positive");
        }
        if let Some(schema_fingerprint) = expected_schema_fingerprint {
            validate_hex("schema fingerprint", schema_fingerprint)?;
        }
        let loaded = load_all_heads(&storage, source_epoch, expected_schema_fingerprint)?;
        let schema_fingerprint = loaded
            .first()
            .expect("fixed positioned shard count is nonzero")
            .0
            .schema_fingerprint
            .clone();
        let pinned = loaded
            .into_iter()
            .map(|head| {
                Mutex::new(PinnedPositionedHead {
                    head: head.0,
                    version: head.1,
                })
            })
            .collect();
        let requests = storage.request_counts();
        Ok((
            Self {
                storage,
                source_epoch,
                schema_fingerprint,
                heads: Arc::new(pinned),
            },
            requests,
        ))
    }

    /// Return a reader over the same storage authority and source epoch.
    pub fn reader(&self) -> PositionedLogReader {
        PositionedLogReader {
            storage: self.storage.clone(),
            source_epoch: self.source_epoch,
            schema_fingerprint: self.schema_fingerprint.clone(),
        }
    }

    /// Publish typed payloads and one position-bearing envelope before one head CAS.
    ///
    /// A schema mismatch fails before any immutable object is uploaded.
    pub fn append(
        &self,
        transaction_id: &str,
        schema_fingerprint: &str,
        payloads: Vec<PositionedMutationPayloadInput>,
    ) -> Result<CommittedPositionedMutation> {
        let prepared = self.prepare_append(transaction_id, schema_fingerprint, payloads)?;
        self.append_prepared(&prepared)
    }

    pub(crate) fn prepare_append(
        &self,
        transaction_id: &str,
        schema_fingerprint: &str,
        payloads: Vec<PositionedMutationPayloadInput>,
    ) -> Result<PreparedPositionedAppend> {
        if schema_fingerprint != self.schema_fingerprint {
            return invalid(
                "positioned append schema fingerprint differs from its shard authority",
            );
        }
        prepare_positioned_append(transaction_id, schema_fingerprint, payloads)
    }

    pub(crate) fn append_prepared(
        &self,
        prepared: &PreparedPositionedAppend,
    ) -> Result<CommittedPositionedMutation> {
        if prepared.schema_fingerprint != self.schema_fingerprint {
            return invalid(
                "prepared positioned append schema fingerprint differs from its shard authority",
            );
        }
        let storage = self.storage.clone_with_independent_mutation_counters();
        let shard = prepared.transaction_digest[0] % SOURCE_SHARD_COUNT;
        // Append callers must not invoke this method from the positioned I/O
        // pool: this guard remains held while the immutable upload wave runs.
        let mut pinned = self.heads[usize::from(shard)]
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut authoritative = false;
        let mut cas_attempts = 0_usize;
        let mut payloads_written = false;
        let mut uploaded_envelope_checksum = None::<String>;
        while cas_attempts < MAX_HEAD_CAS_ATTEMPTS {
            pinned.head.validate(self.source_epoch, shard)?;
            if pinned.head.schema_fingerprint != self.schema_fingerprint {
                return invalid("positioned shard head schema fingerprint differs from its writer");
            }
            if find_transaction(&pinned.head, &prepared.transaction_digest).is_some()
                && !authoritative
            {
                refresh_pinned_head(
                    &storage,
                    &mut pinned,
                    self.source_epoch,
                    shard,
                    &self.schema_fingerprint,
                )?;
                authoritative = true;
                continue;
            }
            if let Some(reference) = find_transaction(&pinned.head, &prepared.transaction_digest) {
                if reference.request_digest != prepared.request_digest {
                    return invalid("positioned transaction ID conflicts with an earlier request");
                }
                return committed_from_reference(
                    self.source_epoch,
                    shard,
                    reference,
                    storage.request_counts(),
                    storage.put_payload_bytes(),
                );
            }
            let sequence = match pinned.head.durable_sequence.checked_add(1) {
                Some(sequence) => sequence,
                None if !authoritative => {
                    refresh_pinned_head(
                        &storage,
                        &mut pinned,
                        self.source_epoch,
                        shard,
                        &self.schema_fingerprint,
                    )?;
                    authoritative = true;
                    continue;
                }
                None => {
                    return invalid("positioned durable sequence overflow");
                }
            };
            let position = CommitSourcePosition::new(self.source_epoch, shard, sequence)?;
            let envelope = PositionedMutationEnvelope {
                transaction_id: prepared.transaction_id.clone(),
                schema_fingerprint: prepared.schema_fingerprint.clone(),
                position,
                min_stamp: prepared.min_stamp,
                max_stamp: prepared.max_stamp,
                payloads: prepared.payload_refs.clone(),
            };
            let envelope_bytes = positioned_envelope_to_parquet(
                &envelope,
                &prepared.transaction_digest_hex,
                &prepared.request_digest,
            )?;
            let envelope_checksum = checksum(&envelope_bytes);
            let reference = PositionedCommitReference {
                transaction_digest: prepared.transaction_digest_hex.clone(),
                request_digest: prepared.request_digest.clone(),
                envelope_checksum: envelope_checksum.clone(),
                sequence,
                rows: prepared.rows,
                encoded_bytes: prepared.encoded_bytes,
                materialized_collection_generation: 0,
            };
            let mut next = pinned.head.clone();
            let next_bytes = match admit_pending(&mut next, reference.clone())
                .and_then(|()| shard_head_bytes(&next))
            {
                Ok(bytes) => bytes,
                Err(BorsukError::IngestBackpressure { .. }) if !authoritative => {
                    refresh_pinned_head(
                        &storage,
                        &mut pinned,
                        self.source_epoch,
                        shard,
                        &self.schema_fingerprint,
                    )?;
                    authoritative = true;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let envelope_path = canonical_envelope_path(&envelope_checksum);
            if !payloads_written {
                let mut unique_payloads = BTreeMap::new();
                for payload in &prepared.payloads {
                    unique_payloads
                        .entry(payload.reference.path.as_str())
                        .or_insert((
                            payload.bytes.as_slice(),
                            payload.reference.checksum.as_str(),
                        ));
                }
                let mut immutable = unique_payloads
                    .into_iter()
                    .map(|(path, (bytes, object_checksum))| (path, bytes, object_checksum))
                    .collect::<Vec<_>>();
                immutable.push((&envelope_path, &envelope_bytes, &envelope_checksum));
                crate::parallel::install_io(|| {
                    immutable
                        .par_iter()
                        .try_for_each(|(path, bytes, object_checksum)| {
                            storage
                                .create_bytes_verified(path, bytes, object_checksum)
                                .map(|_| ())
                        })
                })?;
                payloads_written = true;
                uploaded_envelope_checksum = Some(envelope_checksum.clone());
            } else if uploaded_envelope_checksum.as_deref() != Some(&envelope_checksum) {
                storage.create_bytes_verified(
                    &envelope_path,
                    &envelope_bytes,
                    &envelope_checksum,
                )?;
                uploaded_envelope_checksum = Some(envelope_checksum.clone());
            }
            cas_attempts += 1;
            match storage.write_coordination_object(
                &shard_head_path(shard),
                &next_bytes,
                Some(pinned.version.clone()),
            ) {
                Ok(version) => {
                    pinned.head = next;
                    pinned.version = version;
                    return committed_from_reference(
                        self.source_epoch,
                        shard,
                        &reference,
                        storage.request_counts(),
                        storage.put_payload_bytes(),
                    );
                }
                Err(
                    error @ (BorsukError::ConcurrentModification { .. }
                    | BorsukError::ObjectStoreRetryable { .. }),
                ) => {
                    let ambiguous_write =
                        matches!(&error, BorsukError::ObjectStoreRetryable { .. });
                    let stored = storage
                        .read_coordination_object(&shard_head_path(shard))?
                        .ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "positioned authoritative shard head disappeared".to_owned(),
                            )
                        })?;
                    let observed = shard_head_from_bytes(&stored.bytes, self.source_epoch, shard)?;
                    if let Some(existing) =
                        find_transaction(&observed, &prepared.transaction_digest).cloned()
                    {
                        if existing.request_digest != prepared.request_digest
                            || (ambiguous_write && existing.envelope_checksum != envelope_checksum)
                        {
                            return invalid(
                                "positioned transaction receipt conflicts with the retried request",
                            );
                        }
                        pinned.head = observed;
                        pinned.version = stored.version;
                        return committed_from_reference(
                            self.source_epoch,
                            shard,
                            &existing,
                            storage.request_counts(),
                            storage.put_payload_bytes(),
                        );
                    }
                    reject_digest_conflict(
                        &observed,
                        &prepared.transaction_digest,
                        &prepared.request_digest,
                    )?;
                    pinned.head = observed;
                    pinned.version = stored.version;
                    authoritative = true;
                }
                Err(error) => return Err(error),
            }
        }
        Err(BorsukError::ConcurrentModification {
            path: shard_head_path(shard),
        })
    }

    /// Advance a shard's materialized prefix after its collection generation is durable.
    pub fn checkpoint_materialized_through(
        &self,
        shard: u8,
        target: &PositionedMaterializationWatermark,
        collection_generation: u64,
    ) -> Result<()> {
        self.checkpoint_materialized_through_inner(shard, target, collection_generation, false)
    }

    /// Repair a head only when its pinned materialized sequence is behind the
    /// collection authority. An already-repaired head is a zero-I/O no-op even
    /// when the caller's collection generation is newer; its recent receipts
    /// retain the generation at which they actually became invisible.
    pub(crate) fn checkpoint_materialized_through_if_behind(
        &self,
        shard: u8,
        target: &PositionedMaterializationWatermark,
        collection_generation: u64,
    ) -> Result<()> {
        validate_source_epoch_and_shard(self.source_epoch, shard)?;
        target.validate()?;
        {
            let pinned = self.heads[usize::from(shard)]
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if pinned.head.materialized_sequence > target.sequence {
                // A later source repair is monotonic and already subsumes the
                // caller's older published target.
                return Ok(());
            }
            if pinned.head.materialized_sequence == target.sequence {
                if pinned.head.materialized_prefix_digest != target.prefix_digest {
                    return invalid(
                        "positioned checkpoint target prefix digest conflicts with durable progress",
                    );
                }
                return Ok(());
            }
        }
        self.checkpoint_materialized_through_inner(shard, target, collection_generation, true)
    }

    fn checkpoint_materialized_through_inner(
        &self,
        shard: u8,
        target: &PositionedMaterializationWatermark,
        collection_generation: u64,
        tolerate_already_materialized: bool,
    ) -> Result<()> {
        validate_source_epoch_and_shard(self.source_epoch, shard)?;
        target.validate()?;
        if target.sequence == 0 || collection_generation == 0 {
            return invalid(
                "positioned checkpoint sequence and collection generation must be positive",
            );
        }
        let mut pinned = self.heads[usize::from(shard)]
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        refresh_pinned_head(
            &self.storage,
            &mut pinned,
            self.source_epoch,
            shard,
            &self.schema_fingerprint,
        )?;
        for _ in 0..MAX_HEAD_CAS_ATTEMPTS {
            pinned.head.validate(self.source_epoch, shard)?;
            if pinned.head.schema_fingerprint != self.schema_fingerprint {
                return invalid("positioned shard head schema fingerprint differs from its writer");
            }
            if pinned.head.materialized_sequence >= target.sequence {
                if pinned.head.materialized_sequence == target.sequence
                    && pinned.head.materialized_prefix_digest != target.prefix_digest
                {
                    return invalid(
                        "positioned checkpoint target prefix digest conflicts with durable progress",
                    );
                }
                if tolerate_already_materialized
                    // A prior recovery may already have advanced this source
                    // head beyond an older published generation. That is a
                    // safe no-op: source authority is monotonic and must never
                    // be rewound to the caller's older watermark.
                    || pinned.head.materialized_sequence > target.sequence
                    || pinned.head.materialized_collection_generation >= collection_generation
                {
                    return Ok(());
                }
                return invalid(
                    "positioned checkpoint collection generation conflicts with durable progress",
                );
            }
            let next = checkpointed_head(&pinned.head, target, collection_generation)?;
            let bytes = shard_head_bytes(&next)?;
            match self.storage.write_coordination_object(
                &shard_head_path(shard),
                &bytes,
                Some(pinned.version.clone()),
            ) {
                Ok(version) => {
                    pinned.head = next;
                    pinned.version = version;
                    return Ok(());
                }
                Err(
                    error @ (BorsukError::ConcurrentModification { .. }
                    | BorsukError::ObjectStoreRetryable { .. }),
                ) => {
                    let stored = self
                        .storage
                        .read_coordination_object(&shard_head_path(shard))?
                        .ok_or(error)?;
                    let observed = shard_head_from_bytes(&stored.bytes, self.source_epoch, shard)?;
                    if observed == next {
                        pinned.head = observed;
                        pinned.version = stored.version;
                        return Ok(());
                    }
                    pinned.head = observed;
                    pinned.version = stored.version;
                }
                Err(error) => return Err(error),
            }
        }
        Err(BorsukError::ConcurrentModification {
            path: shard_head_path(shard),
        })
    }
}

#[derive(Clone)]
/// Reader for bounded authoritative heads and their visible pending envelopes.
pub struct PositionedLogReader {
    storage: Storage,
    source_epoch: u64,
    schema_fingerprint: String,
}

impl PositionedLogReader {
    pub(crate) fn open_from_storage(
        storage: Storage,
        source_epoch: u64,
        schema_fingerprint: &str,
    ) -> Result<Self> {
        if source_epoch == 0 {
            return invalid("positioned source epoch must be positive");
        }
        validate_hex("schema fingerprint", schema_fingerprint)?;
        Ok(Self {
            storage,
            source_epoch,
            schema_fingerprint: schema_fingerprint.to_owned(),
        })
    }

    /// Read all heads and decode every currently pending transaction envelope.
    pub fn snapshot(&self) -> Result<PositionedLogSnapshot> {
        self.load_snapshot(None, None, false)?.ok_or_else(|| {
            BorsukError::InvalidStorage(
                "positioned snapshot unexpectedly reported unchanged".to_owned(),
            )
        })
    }

    /// Return `None` without envelope GETs when all authoritative head checksums match.
    pub fn snapshot_if_changed(
        &self,
        previous_head_checksums: &[String; SOURCE_SHARD_COUNT as usize],
    ) -> Result<Option<PositionedLogSnapshot>> {
        self.load_snapshot(Some(previous_head_checksums), None, false)
    }

    /// Read mutations not visible at the caller's pinned collection generation.
    ///
    /// The caller must pin the collection generation before any checkpoint can
    /// advance. Visibility is retained for exactly the bounded recent-receipt
    /// window; an older pin fails closed instead of silently omitting mutations.
    pub fn snapshot_for_collection_generation(
        &self,
        pinned_collection_generation: u64,
    ) -> Result<PositionedLogSnapshot> {
        self.load_snapshot(None, Some(pinned_collection_generation), false)?
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "positioned generation snapshot unexpectedly reported unchanged".to_owned(),
                )
            })
    }

    /// Read every bounded recent receipt plus every pending transaction for GC.
    /// Recent receipts are retained precisely because an older pinned reader may
    /// still reference their immutable envelope and payload objects.
    pub(crate) fn snapshot_with_recent_receipts(&self) -> Result<PositionedLogSnapshot> {
        self.load_snapshot(None, None, true)?.ok_or_else(|| {
            BorsukError::InvalidStorage(
                "positioned retained snapshot unexpectedly reported unchanged".to_owned(),
            )
        })
    }

    fn load_snapshot(
        &self,
        previous: Option<&[String; SOURCE_SHARD_COUNT as usize]>,
        pinned_collection_generation: Option<u64>,
        include_all_recent: bool,
    ) -> Result<Option<PositionedLogSnapshot>> {
        let loaded = load_all_heads(
            &self.storage,
            self.source_epoch,
            Some(&self.schema_fingerprint),
        )?;
        let head_checksums = std::array::from_fn(|index| loaded[index].2.clone());
        if previous == Some(&head_checksums) {
            return Ok(None);
        }
        let durable_sequences = std::array::from_fn(|index| loaded[index].0.durable_sequence);
        let materialized_watermarks =
            materialized_watermarks_from_heads(loaded.iter().map(|(head, _, _)| head))?;
        let materialized_collection_generations =
            std::array::from_fn(|index| loaded[index].0.materialized_collection_generation);
        if let Some(pinned_generation) = pinned_collection_generation {
            for (head, _, _) in &loaded {
                if pinned_generation < head.evicted_recent_through_collection_generation {
                    return invalid(
                        "positioned collection generation predates the bounded recent window",
                    );
                }
            }
        }
        let visible = loaded
            .iter()
            .flat_map(|(head, _, _)| {
                head.recent
                    .iter()
                    .filter(move |reference| {
                        include_all_recent
                            || pinned_collection_generation.is_some_and(|generation| {
                                reference.materialized_collection_generation > generation
                            })
                    })
                    .chain(head.pending.iter())
                    .map(move |reference| (head.shard, reference))
            })
            .collect::<Vec<_>>();
        let mut transactions = crate::parallel::install_io(|| {
            visible
                .par_iter()
                .map(|(shard, reference)| {
                    load_envelope(
                        &self.storage,
                        self.source_epoch,
                        *shard,
                        &self.schema_fingerprint,
                        reference,
                    )
                    .map(|envelope| (envelope, reference.envelope_checksum.clone()))
                })
                .collect::<Result<Vec<_>>>()
        })?;
        transactions.sort_unstable_by_key(|(envelope, _)| envelope.position);
        let (transactions, envelope_checksums) = transactions.into_iter().unzip();
        Ok(Some(PositionedLogSnapshot {
            transactions,
            envelope_checksums,
            head_checksums,
            durable_sequences,
            materialized_watermarks,
            materialized_collection_generations,
        }))
    }
}

fn materialized_watermarks_from_heads<'a>(
    heads: impl IntoIterator<Item = &'a PositionedShardHead>,
) -> Result<[PositionedMaterializationWatermark; SOURCE_SHARD_COUNT as usize]> {
    let watermarks = heads
        .into_iter()
        .map(|head| {
            PositionedMaterializationWatermark::from_parts(
                head.materialized_sequence,
                head.materialized_prefix_digest.clone(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    watermarks.try_into().map_err(|watermarks: Vec<_>| {
        BorsukError::InvalidStorage(format!(
            "positioned snapshot requires exactly {SOURCE_SHARD_COUNT} materialized watermarks, got {}",
            watermarks.len()
        ))
    })
}

struct PreparedPayload {
    reference: PositionedMutationPayloadRef,
    bytes: Vec<u8>,
}

pub(crate) struct PreparedPositionedAppend {
    transaction_id: String,
    schema_fingerprint: String,
    transaction_digest: [u8; 32],
    transaction_digest_hex: String,
    request_digest: String,
    min_stamp: PositionedMutationStamp,
    max_stamp: PositionedMutationStamp,
    payload_refs: Vec<PositionedMutationPayloadRef>,
    payloads: Vec<PreparedPayload>,
    rows: u64,
    encoded_bytes: u64,
}

fn prepare_positioned_append(
    transaction_id: &str,
    schema_fingerprint: &str,
    payloads: Vec<PositionedMutationPayloadInput>,
) -> Result<PreparedPositionedAppend> {
    validate_bounded_utf8("transaction ID", transaction_id)?;
    validate_hex("schema fingerprint", schema_fingerprint)?;
    if payloads.is_empty() || payloads.len() > MAX_PAYLOADS_PER_TRANSACTION {
        return invalid("positioned append payload count is outside its fixed bound");
    }
    let mut rows = 0_u64;
    let mut encoded_bytes = 0_u64;
    for payload in &payloads {
        validate_bounded_utf8("payload role", &payload.role)?;
        if payload.rows == 0 {
            return invalid("positioned payload declared rows must be positive");
        }
        rows = rows.checked_add(payload.rows).ok_or_else(|| {
            BorsukError::InvalidStorage("positioned append row total overflow".to_owned())
        })?;
        let payload_bytes = u64::try_from(payload.bytes.len()).map_err(|_| {
            BorsukError::InvalidStorage("positioned payload byte length exceeds u64".to_owned())
        })?;
        encoded_bytes = encoded_bytes.checked_add(payload_bytes).ok_or_else(|| {
            BorsukError::InvalidStorage("positioned append byte total overflow".to_owned())
        })?;
        validate_append_totals(rows, encoded_bytes)?;
    }
    let mut min_stamp = None::<PositionedMutationStamp>;
    let mut max_stamp = None::<PositionedMutationStamp>;
    let mut versions = BTreeMap::<(u64, [u8; 16]), [u8; 32]>::new();
    let mut prepared = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let metadata = positioned_payload_metadata(&payload.bytes, payload.format, payload.rows)?;
        if metadata.rows != payload.rows {
            return invalid("positioned payload declared rows disagree with its typed container");
        }
        let payload_bytes = u64::try_from(payload.bytes.len()).map_err(|_| {
            BorsukError::InvalidStorage("positioned payload byte length exceeds u64".to_owned())
        })?;
        min_stamp = Some(min_stamp.map_or(metadata.min_stamp, |existing| {
            existing.min(metadata.min_stamp)
        }));
        max_stamp = Some(max_stamp.map_or(metadata.max_stamp, |existing| {
            existing.max(metadata.max_stamp)
        }));
        for (version, digest) in metadata.version_digests {
            if let Some(existing) = versions.insert(version, digest)
                && existing != digest
            {
                return invalid("equal mutation version has unequal canonical digests");
            }
        }
        let payload_checksum = checksum(&payload.bytes);
        let path = canonical_payload_path(payload.format, &payload_checksum);
        let reference = PositionedMutationPayloadRef {
            modality: payload.modality,
            role: payload.role,
            id_bloom: payload.id_bloom,
            format: payload.format,
            path,
            checksum: payload_checksum,
            rows: payload.rows,
            encoded_bytes: payload_bytes,
        };
        validate_payload_ref(&reference)?;
        prepared.push(PreparedPayload {
            reference,
            bytes: payload.bytes,
        });
    }
    prepared.sort_unstable_by(|left, right| {
        payload_sort_key(&left.reference).cmp(&payload_sort_key(&right.reference))
    });
    if prepared
        .windows(2)
        .any(|pair| payload_sort_key(&pair[0].reference) == payload_sort_key(&pair[1].reference))
    {
        return invalid("positioned append contains duplicate canonical payload references");
    }
    let min_stamp = min_stamp.ok_or_else(|| {
        BorsukError::InvalidStorage("positioned append contains no mutation stamps".to_owned())
    })?;
    let max_stamp = max_stamp.expect("a minimum stamp implies a maximum stamp");
    let transaction_digest = *blake3::hash(transaction_id.as_bytes()).as_bytes();
    let transaction_digest_hex = hex_bytes(&transaction_digest);
    let payload_refs = prepared
        .iter()
        .map(|payload| payload.reference.clone())
        .collect::<Vec<_>>();
    let request_digest = request_digest(
        transaction_id,
        schema_fingerprint,
        min_stamp,
        max_stamp,
        &payload_refs,
    );
    Ok(PreparedPositionedAppend {
        transaction_id: transaction_id.to_owned(),
        schema_fingerprint: schema_fingerprint.to_owned(),
        transaction_digest,
        transaction_digest_hex,
        request_digest,
        min_stamp,
        max_stamp,
        payload_refs,
        payloads: prepared,
        rows,
        encoded_bytes,
    })
}

fn validate_append_totals(rows: u64, encoded_bytes: u64) -> Result<()> {
    if rows > MAX_APPEND_ROWS || encoded_bytes > MAX_APPEND_ENCODED_BYTES {
        return invalid("positioned append exceeds its hard row or byte bound");
    }
    Ok(())
}

fn request_digest(
    transaction_id: &str,
    schema_fingerprint: &str,
    min_stamp: PositionedMutationStamp,
    max_stamp: PositionedMutationStamp,
    payloads: &[PositionedMutationPayloadRef],
) -> String {
    let mut hasher = blake3::Hasher::new();
    hash_field(&mut hasher, transaction_id.as_bytes());
    hash_field(&mut hasher, schema_fingerprint.as_bytes());
    hash_stamp(&mut hasher, min_stamp);
    hash_stamp(&mut hasher, max_stamp);
    for payload in payloads {
        hash_field(&mut hasher, payload.modality.as_str().as_bytes());
        hash_field(&mut hasher, payload.role.as_bytes());
        hash_field(&mut hasher, &payload.id_bloom);
        hash_field(&mut hasher, payload.format.as_str().as_bytes());
        hash_field(&mut hasher, payload.checksum.as_bytes());
        hash_field(&mut hasher, &payload.rows.to_be_bytes());
        hash_field(&mut hasher, &payload.encoded_bytes.to_be_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn hash_stamp(hasher: &mut blake3::Hasher, stamp: PositionedMutationStamp) {
    hash_field(hasher, &stamp.hlc.to_be_bytes());
    hash_field(hasher, &stamp.writer);
    hash_field(hasher, &stamp.digest);
}

fn hash_field(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

fn admit_pending(
    head: &mut PositionedShardHead,
    reference: PositionedCommitReference,
) -> Result<()> {
    if head.pending.len() == MAX_PENDING_ENVELOPES_PER_SHARD {
        return ingest_backpressure(head, reference.rows, reference.encoded_bytes);
    }
    let pending_rows = head
        .pending_rows
        .checked_add(reference.rows)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("positioned pending row total overflow".to_owned())
        })?;
    let pending_bytes = head
        .pending_bytes
        .checked_add(reference.encoded_bytes)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("positioned pending byte total overflow".to_owned())
        })?;
    if pending_rows > MAX_UNMATERIALIZED_ROWS_PER_SHARD
        || pending_bytes > MAX_UNMATERIALIZED_BYTES_PER_SHARD
    {
        return ingest_backpressure(head, reference.rows, reference.encoded_bytes);
    }
    head.durable_sequence = reference.sequence;
    head.pending_rows = pending_rows;
    head.pending_bytes = pending_bytes;
    head.pending.push(reference);
    Ok(())
}

fn ingest_backpressure<T>(head: &PositionedShardHead, rows: u64, bytes: u64) -> Result<T> {
    Err(BorsukError::IngestBackpressure {
        lane: u16::from(head.shard),
        tail_bytes: head.pending_bytes.saturating_add(bytes),
        tail_records: head.pending_rows.saturating_add(rows),
        max_bytes: MAX_UNMATERIALIZED_BYTES_PER_SHARD,
        max_records: MAX_UNMATERIALIZED_ROWS_PER_SHARD,
    })
}

fn load_all_heads(
    storage: &Storage,
    source_epoch: u64,
    expected_schema_fingerprint: Option<&str>,
) -> Result<Vec<(PositionedShardHead, UpdateVersion, String)>> {
    crate::parallel::install_io(|| {
        (0..SOURCE_SHARD_COUNT)
            .into_par_iter()
            .map(|shard| {
                let path = shard_head_path(shard);
                let stored = storage.read_coordination_object(&path)?.ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "required positioned shard head `{path}` is missing"
                    ))
                })?;
                let head = shard_head_from_bytes(&stored.bytes, source_epoch, shard)?;
                if expected_schema_fingerprint
                    .is_some_and(|schema_fingerprint| head.schema_fingerprint != schema_fingerprint)
                {
                    return invalid(
                        "positioned shard head schema fingerprint differs from its collection",
                    );
                }
                Ok((head, stored.version, checksum(&stored.bytes)))
            })
            .collect::<Result<Vec<_>>>()
    })
    .and_then(|loaded| {
        let schema_fingerprint = loaded
            .first()
            .expect("fixed positioned shard count is nonzero")
            .0
            .schema_fingerprint
            .as_str();
        if loaded
            .iter()
            .any(|(head, _, _)| head.schema_fingerprint != schema_fingerprint)
        {
            return invalid("positioned shard heads disagree on their schema fingerprint");
        }
        Ok(loaded)
    })
}

fn refresh_pinned_head(
    storage: &Storage,
    pinned: &mut PinnedPositionedHead,
    source_epoch: u64,
    shard: u8,
    schema_fingerprint: &str,
) -> Result<()> {
    let path = shard_head_path(shard);
    let stored = storage.read_coordination_object(&path)?.ok_or_else(|| {
        BorsukError::InvalidStorage(format!(
            "required positioned shard head `{path}` disappeared"
        ))
    })?;
    pinned.head = shard_head_from_bytes(&stored.bytes, source_epoch, shard)?;
    if pinned.head.schema_fingerprint != schema_fingerprint {
        return invalid("positioned shard head schema fingerprint differs from its writer");
    }
    pinned.version = stored.version;
    Ok(())
}

fn load_envelope(
    storage: &Storage,
    source_epoch: u64,
    shard: u8,
    schema_fingerprint: &str,
    reference: &PositionedCommitReference,
) -> Result<PositionedMutationEnvelope> {
    let path = canonical_envelope_path(&reference.envelope_checksum);
    let bytes = storage.read_object_fresh(&path)?.ok_or_else(|| {
        BorsukError::InvalidStorage(format!("positioned envelope `{path}` is missing"))
    })?;
    if checksum(&bytes) != reference.envelope_checksum {
        return invalid("positioned envelope checksum does not match its authoritative reference");
    }
    let envelope = validate_authorized_envelope(
        positioned_envelope_from_parquet(&bytes)?,
        source_epoch,
        shard,
        reference,
    )?;
    if envelope.schema_fingerprint != schema_fingerprint {
        return invalid("positioned envelope schema fingerprint differs from its shard authority");
    }
    Ok(envelope)
}

pub(crate) fn validate_claim_authorization_envelope(
    storage: &Storage,
    source_epoch: u64,
    shard: u8,
    sequence: u64,
    transaction_id: &str,
    envelope_checksum: &str,
) -> Result<()> {
    validate_hex("positioned claim envelope checksum", envelope_checksum)?;
    let path = canonical_envelope_path(envelope_checksum);
    let bytes = storage.read_object_fresh(&path)?.ok_or_else(|| {
        BorsukError::InvalidStorage(format!(
            "positioned claim authorization envelope `{path}` is missing"
        ))
    })?;
    if checksum(&bytes) != envelope_checksum {
        return invalid("positioned claim authorization envelope checksum is inconsistent");
    }
    let decoded = positioned_envelope_from_parquet(&bytes)?;
    let rows = decoded
        .envelope
        .payloads
        .iter()
        .try_fold(0_u64, |total, payload| {
            total.checked_add(payload.rows).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "positioned claim authorization envelope row total overflow".to_string(),
                )
            })
        })?;
    let encoded_bytes = decoded
        .envelope
        .payloads
        .iter()
        .try_fold(0_u64, |total, payload| {
            total.checked_add(payload.encoded_bytes).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "positioned claim authorization envelope byte total overflow".to_string(),
                )
            })
        })?;
    let reference = PositionedCommitReference {
        transaction_digest: blake3::hash(transaction_id.as_bytes()).to_hex().to_string(),
        request_digest: decoded.request_digest.clone(),
        envelope_checksum: envelope_checksum.to_string(),
        sequence,
        rows,
        encoded_bytes,
        materialized_collection_generation: 0,
    };
    let envelope = validate_authorized_envelope(decoded, source_epoch, shard, &reference)?;
    if envelope.transaction_id != transaction_id {
        return invalid("positioned claim authorization envelope transaction id is inconsistent");
    }
    Ok(())
}

pub(crate) fn authorized_transaction_receipt(
    storage: &Storage,
    source_epoch: u64,
    transaction_id: &str,
) -> Result<Option<(CommitSourcePosition, String)>> {
    let transaction_hash = blake3::hash(transaction_id.as_bytes());
    let transaction_digest = transaction_hash.to_hex().to_string();
    let shard = transaction_hash.as_bytes()[0] % SOURCE_SHARD_COUNT;
    let path = shard_head_path(shard);
    let Some(stored) = storage.read_coordination_object(&path)? else {
        return Ok(None);
    };
    let head = shard_head_from_bytes(&stored.bytes, source_epoch, shard)?;
    let Some(reference) = head
        .pending
        .iter()
        .chain(head.recent.iter())
        .find(|reference| reference.transaction_digest == transaction_digest)
    else {
        return Ok(None);
    };
    let envelope = load_envelope(
        storage,
        source_epoch,
        shard,
        &head.schema_fingerprint,
        reference,
    )?;
    if envelope.transaction_id != transaction_id {
        return invalid("positioned transaction digest resolved to a different transaction id");
    }
    Ok(Some((
        envelope.position,
        reference.envelope_checksum.clone(),
    )))
}

fn validate_authorized_envelope(
    decoded: DecodedPositionedEnvelope,
    source_epoch: u64,
    shard: u8,
    reference: &PositionedCommitReference,
) -> Result<PositionedMutationEnvelope> {
    let DecodedPositionedEnvelope {
        envelope,
        transaction_digest,
        request_digest: decoded_request_digest,
    } = decoded;
    envelope.validate()?;
    if envelope.position.source_epoch != source_epoch
        || envelope.position.shard != shard
        || envelope.position.sequence != reference.sequence
        || transaction_digest != reference.transaction_digest
        || decoded_request_digest != reference.request_digest
    {
        return invalid(
            "positioned envelope identity disagrees with its authoritative head reference",
        );
    }
    let expected_transaction_digest = blake3::hash(envelope.transaction_id.as_bytes())
        .to_hex()
        .to_string();
    let expected_request_digest = request_digest(
        &envelope.transaction_id,
        &envelope.schema_fingerprint,
        envelope.min_stamp,
        envelope.max_stamp,
        &envelope.payloads,
    );
    let (rows, encoded_bytes) =
        envelope
            .payloads
            .iter()
            .try_fold((0_u64, 0_u64), |(rows, encoded_bytes), payload| {
                Ok::<_, BorsukError>((
                    rows.checked_add(payload.rows).ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "positioned envelope payload row total overflow".to_owned(),
                        )
                    })?,
                    encoded_bytes
                        .checked_add(payload.encoded_bytes)
                        .ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "positioned envelope payload byte total overflow".to_owned(),
                            )
                        })?,
                ))
            })?;
    if transaction_digest != expected_transaction_digest
        || decoded_request_digest != expected_request_digest
        || rows != reference.rows
        || encoded_bytes != reference.encoded_bytes
    {
        return invalid("positioned envelope canonical digests or totals are inconsistent");
    }
    Ok(envelope)
}

fn find_transaction<'a>(
    head: &'a PositionedShardHead,
    transaction_digest: &[u8; 32],
) -> Option<&'a PositionedCommitReference> {
    let digest = hex_bytes(transaction_digest);
    head.pending
        .iter()
        .chain(&head.recent)
        .find(|reference| reference.transaction_digest == digest)
}

fn reject_digest_conflict(
    head: &PositionedShardHead,
    transaction_digest: &[u8; 32],
    request_digest: &str,
) -> Result<()> {
    if let Some(reference) = find_transaction(head, transaction_digest)
        && reference.request_digest != request_digest
    {
        return invalid("positioned transaction ID conflicts with an earlier request");
    }
    Ok(())
}

fn committed_from_reference(
    source_epoch: u64,
    shard: u8,
    reference: &PositionedCommitReference,
    requests: RequestCounts,
    put_payload_bytes: u64,
) -> Result<CommittedPositionedMutation> {
    Ok(CommittedPositionedMutation {
        position: CommitSourcePosition::new(source_epoch, shard, reference.sequence)?,
        transaction_digest: reference.transaction_digest.clone(),
        request_digest: reference.request_digest.clone(),
        envelope_checksum: reference.envelope_checksum.clone(),
        rows: reference.rows,
        encoded_bytes: reference.encoded_bytes,
        put_payload_bytes,
        requests,
    })
}

fn validate_bounded_utf8(label: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 256 {
        return invalid(&format!(
            "positioned {label} must contain 1..=256 UTF-8 bytes"
        ));
    }
    Ok(())
}

fn validate_hex(label: &str, value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid(&format!(
            "positioned {label} must be exactly 64 lowercase hexadecimal characters"
        ));
    }
    Ok(())
}

fn validate_commit_reference(reference: &PositionedCommitReference) -> Result<()> {
    validate_hex("transaction digest", &reference.transaction_digest)?;
    validate_hex("request digest", &reference.request_digest)?;
    validate_hex("envelope checksum", &reference.envelope_checksum)?;
    if reference.sequence == 0 || reference.rows == 0 || reference.encoded_bytes == 0 {
        return invalid("positioned commit reference contains a zero sequence, row, or byte total");
    }
    Ok(())
}

fn validate_payload_ref(reference: &PositionedMutationPayloadRef) -> Result<()> {
    validate_bounded_utf8("payload role", &reference.role)?;
    if reference.id_bloom.len() > 64 * 1024 {
        return invalid("positioned payload ID bloom exceeds 65536 bytes");
    }
    validate_hex("payload checksum", &reference.checksum)?;
    if reference.rows == 0 || reference.encoded_bytes == 0 {
        return invalid("positioned payload reference contains a zero row or byte total");
    }
    if reference.path != canonical_payload_path(reference.format, &reference.checksum) {
        return invalid(
            "positioned payload path is not deterministically derived from its checksum",
        );
    }
    Ok(())
}

fn payload_sort_key(
    reference: &PositionedMutationPayloadRef,
) -> (
    PositionedMutationModality,
    &str,
    PositionedPayloadFormat,
    &str,
) {
    (
        reference.modality,
        &reference.role,
        reference.format,
        &reference.checksum,
    )
}

fn shard_head_bytes(head: &PositionedShardHead) -> Result<Vec<u8>> {
    head.validate(head.source_epoch, head.shard)?;
    let bytes = serde_json::to_vec(head).map_err(|error| {
        BorsukError::InvalidStorage(format!("failed to encode positioned shard head: {error}"))
    })?;
    if bytes.len() > MAX_SHARD_HEAD_BYTES {
        return ingest_backpressure(head, 0, 0);
    }
    Ok(bytes)
}

fn shard_head_from_bytes(
    bytes: &[u8],
    source_epoch: u64,
    shard: u8,
) -> Result<PositionedShardHead> {
    if bytes.len() > MAX_SHARD_HEAD_BYTES {
        return invalid("positioned shard head exceeds its serialized hard bound");
    }
    #[derive(Deserialize)]
    struct PositionedHeadLayout {
        layout: u16,
    }
    let layout = serde_json::from_slice::<PositionedHeadLayout>(bytes).map_err(|error| {
        BorsukError::InvalidStorage(format!("failed to decode positioned shard head: {error}"))
    })?;
    if layout.layout != POSITIONED_LOG_LAYOUT {
        return invalid("positioned shard head has an unsupported layout marker");
    }
    let head = serde_json::from_slice::<PositionedShardHead>(bytes).map_err(|error| {
        BorsukError::InvalidStorage(format!("failed to decode positioned shard head: {error}"))
    })?;
    head.validate(source_epoch, shard)?;
    Ok(head)
}

fn shard_head_path(shard: u8) -> String {
    format!("positioned-log/heads/{shard:02}.json")
}

pub(crate) fn canonical_payload_path(format: PositionedPayloadFormat, checksum: &str) -> String {
    format!(
        "positioned-log/payloads/{}/{}/{}.{}",
        format.as_str(),
        &checksum[..2],
        checksum,
        format.extension()
    )
}

pub(crate) fn canonical_envelope_path(checksum: &str) -> String {
    format!(
        "positioned-log/envelopes/{}/{}.parquet",
        &checksum[..2],
        checksum
    )
}

fn checksum(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(BorsukError::InvalidStorage(message.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(
        source_epoch: u64,
        shard: u8,
        first_sequence: u64,
        last_sequence: u64,
    ) -> CommitSourceRange {
        CommitSourceRange::new(source_epoch, shard, first_sequence, last_sequence).unwrap()
    }

    #[test]
    fn adjacent_positions_coalesce_but_overlap_fails() {
        let set = CommitSourceRangeSet::new(vec![range(7, 3, 5, 8), range(7, 3, 1, 4)]).unwrap();
        assert_eq!(set.ranges(), &[range(7, 3, 1, 8)]);
        assert!(CommitSourceRangeSet::new(vec![range(7, 3, 1, 4), range(7, 3, 4, 9)]).is_err());
    }

    #[test]
    fn commit_source_ranges_round_trip_losslessly_through_leaf_ranges() {
        use crate::global_leaf_run::{LaneSourceRange, SourceRangeSet};

        let positioned = CommitSourceRangeSet::new(vec![
            range(11, 3, 8, 13),
            range(11, 3, 1, 7),
            range(12, 63, 21, 34),
        ])
        .unwrap();
        let leaf = SourceRangeSet::try_from(&positioned).unwrap();
        assert_eq!(
            leaf.ranges(),
            &[
                LaneSourceRange {
                    lane: 3,
                    lease_epoch: 11,
                    first_sequence: 1,
                    last_sequence: 13,
                },
                LaneSourceRange {
                    lane: 63,
                    lease_epoch: 12,
                    first_sequence: 21,
                    last_sequence: 34,
                },
            ]
        );
        assert_eq!(CommitSourceRangeSet::try_from(&leaf).unwrap(), positioned);

        assert!(
            SourceRangeSet::new(vec![LaneSourceRange {
                lane: u16::from(SOURCE_SHARD_COUNT),
                lease_epoch: 11,
                first_sequence: 1,
                last_sequence: 1,
            }])
            .is_err()
        );
    }

    #[test]
    fn sixty_four_shards_and_levels_have_a_fixed_metadata_bound() {
        let coverage = fixture_coverage_for_all_shards_and_levels();
        assert!(coverage.ranges().len() <= 64 * 64);
        coverage.validate_canonical().unwrap();
    }

    #[test]
    fn metadata_range_count_cannot_exceed_the_fixed_bound() {
        let mut ranges = fixture_ranges_for_all_shards_and_levels();
        ranges.push(range(65, 0, 1, 1));

        assert!(CommitSourceRangeSet::new(ranges).is_err());
    }

    #[test]
    fn subtract_preserves_sequence_maximum_without_wrapping() {
        let full = CommitSourceRangeSet::new(vec![range(1, 0, 1, u64::MAX)]).unwrap();
        let covered = CommitSourceRangeSet::new(vec![range(1, 0, 2, u64::MAX - 1)]).unwrap();

        assert_eq!(
            full.subtract(&covered).unwrap(),
            CommitSourceCoverageDifference::Partial(
                CommitSourceRangeSet::new(vec![range(1, 0, 1, 1), range(1, 0, u64::MAX, u64::MAX)])
                    .unwrap()
            )
        );
    }

    #[test]
    fn deserialization_rejects_noncanonical_and_malformed_wire_ranges() {
        let noncanonical = r#"{"ranges":[
            {"source_epoch":7,"shard":3,"first_sequence":5,"last_sequence":8},
            {"source_epoch":7,"shard":3,"first_sequence":1,"last_sequence":4}
        ]}"#;
        let adjacent = r#"{"ranges":[
            {"source_epoch":7,"shard":3,"first_sequence":1,"last_sequence":4},
            {"source_epoch":7,"shard":3,"first_sequence":5,"last_sequence":8}
        ]}"#;
        let invalid_shard = r#"{"ranges":[
            {"source_epoch":7,"shard":64,"first_sequence":1,"last_sequence":4}
        ]}"#;

        assert!(serde_json::from_str::<CommitSourceRangeSet>(noncanonical).is_err());
        assert!(serde_json::from_str::<CommitSourceRangeSet>(adjacent).is_err());
        assert!(serde_json::from_str::<CommitSourceRangeSet>(invalid_shard).is_err());
    }

    #[test]
    fn exact_append_and_pending_totals_admit_then_bound_plus_one_backpressures() {
        validate_append_totals(MAX_APPEND_ROWS, MAX_APPEND_ENCODED_BYTES).unwrap();
        assert!(validate_append_totals(MAX_APPEND_ROWS + 1, MAX_APPEND_ENCODED_BYTES).is_err());
        assert!(validate_append_totals(MAX_APPEND_ROWS, MAX_APPEND_ENCODED_BYTES + 1).is_err());

        let mut head = PositionedShardHead::empty(7, 3, &"a".repeat(64)).unwrap();
        admit_pending(
            &mut head,
            PositionedCommitReference {
                transaction_digest: "a".repeat(64),
                request_digest: "b".repeat(64),
                envelope_checksum: "c".repeat(64),
                sequence: 1,
                rows: MAX_UNMATERIALIZED_ROWS_PER_SHARD,
                encoded_bytes: MAX_UNMATERIALIZED_BYTES_PER_SHARD,
                materialized_collection_generation: 0,
            },
        )
        .unwrap();
        assert!(shard_head_bytes(&head).unwrap().len() <= MAX_SHARD_HEAD_BYTES);
        assert!(
            admit_pending(
                &mut head,
                PositionedCommitReference {
                    transaction_digest: "d".repeat(64),
                    request_digest: "e".repeat(64),
                    envelope_checksum: "f".repeat(64),
                    sequence: 2,
                    rows: 1,
                    encoded_bytes: 1,
                    materialized_collection_generation: 0,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn recent_receipts_are_a_gapless_suffix_and_generation_zero_tracks_zero_progress() {
        let reference = |sequence: u64, digit: char| PositionedCommitReference {
            transaction_digest: digit.to_string().repeat(64),
            request_digest: "a".repeat(64),
            envelope_checksum: "b".repeat(64),
            sequence,
            rows: 1,
            encoded_bytes: 1,
            materialized_collection_generation: sequence,
        };
        let mut head = PositionedShardHead::empty(7, 3, &"a".repeat(64)).unwrap();
        head.durable_sequence = 3;
        head.materialized_sequence = 3;
        head.materialized_collection_generation = 3;
        head.recent = vec![reference(1, 'c'), reference(3, 'd')];
        assert!(head.validate(7, 3).is_err());

        let mut impossible_generation = PositionedShardHead::empty(7, 3, &"a".repeat(64)).unwrap();
        impossible_generation.materialized_collection_generation = 1;
        assert!(impossible_generation.validate(7, 3).is_err());

        let mut missing_generation = PositionedShardHead::empty(7, 3, &"a".repeat(64)).unwrap();
        missing_generation.durable_sequence = 1;
        missing_generation.materialized_sequence = 1;
        missing_generation.recent = vec![reference(1, 'e')];
        assert!(missing_generation.validate(7, 3).is_err());
    }

    #[test]
    fn shard_head_accepts_exact_sixty_four_kibibytes_and_rejects_plus_one() {
        let head = PositionedShardHead::empty(7, 3, &"a".repeat(64)).unwrap();
        let mut bytes = shard_head_bytes(&head).unwrap();
        let json = std::str::from_utf8(&bytes).unwrap();
        assert!(json.contains("\"layout\":16"), "{json}");
        assert!(
            json.contains(&format!("\"schema_fingerprint\":\"{}\"", "a".repeat(64))),
            "{json}"
        );
        bytes.resize(MAX_SHARD_HEAD_BYTES, b' ');
        assert_eq!(bytes.len(), MAX_SHARD_HEAD_BYTES);
        assert_eq!(shard_head_from_bytes(&bytes, 7, 3).unwrap(), head);
        bytes.push(b' ');
        assert!(shard_head_from_bytes(&bytes, 7, 3).is_err());
    }

    #[test]
    fn positioned_head_rejects_v14_layout_marker() {
        let head = PositionedShardHead::empty(7, 3, &"a".repeat(64)).unwrap();
        let bytes = shard_head_bytes(&head).unwrap();
        let old = std::str::from_utf8(&bytes)
            .unwrap()
            .replace("\"layout\":16", "\"layout\":14")
            .into_bytes();
        let error = shard_head_from_bytes(&old, 7, 3).unwrap_err().to_string();
        assert!(error.contains("unsupported layout marker"), "{error}");
    }

    #[test]
    fn positioned_head_rejects_v15_without_materialized_prefix_digest() {
        let head = PositionedShardHead::empty(7, 3, &"a".repeat(64)).unwrap();
        let mut document: serde_json::Value =
            serde_json::from_slice(&shard_head_bytes(&head).unwrap()).unwrap();
        document["layout"] = serde_json::Value::from(15);
        document
            .as_object_mut()
            .unwrap()
            .remove("materialized_prefix_digest");
        let old = serde_json::to_vec(&document).unwrap();

        let error = shard_head_from_bytes(&old, 7, 3).unwrap_err().to_string();
        assert!(error.contains("unsupported layout marker"), "{error}");
    }

    #[test]
    fn materialized_prefix_digest_binds_ordered_envelope_identity() {
        let empty = PositionedMaterializationWatermark::empty();
        let expected_empty = blake3::hash(b"borsuk.positioned.materialized-prefix.empty.v1\0")
            .to_hex()
            .to_string();
        assert_eq!(empty.sequence(), 0);
        assert_eq!(empty.prefix_digest(), expected_empty);

        let checksum = "c".repeat(64);
        let first = empty.advanced(7, 3, 1, &checksum).unwrap();
        let ordered = first.advanced(7, 3, 2, &"d".repeat(64)).unwrap();
        let reordered = empty
            .advanced(7, 3, 1, &"d".repeat(64))
            .unwrap()
            .advanced(7, 3, 2, &checksum)
            .unwrap();
        let other_shard = empty.advanced(7, 4, 1, &checksum).unwrap();
        let other_envelope = empty.advanced(7, 3, 1, &"d".repeat(64)).unwrap();
        assert_eq!(first.sequence(), 1);
        assert_ne!(ordered.prefix_digest(), reordered.prefix_digest());
        assert_ne!(first.prefix_digest(), other_shard.prefix_digest());
        assert_ne!(first.prefix_digest(), other_envelope.prefix_digest());

        let error = PositionedMaterializationWatermark::from_parts(
            1,
            PositionedMaterializationWatermark::empty()
                .prefix_digest()
                .to_owned(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("nonzero") && error.contains("empty"),
            "{error}"
        );
    }

    #[test]
    fn checkpoint_head_verifies_target_prefix_digest_before_draining() {
        let mut head = PositionedShardHead::empty(7, 3, &"a".repeat(64)).unwrap();
        for (sequence, digit) in [(1, 'c'), (2, 'd')] {
            admit_pending(
                &mut head,
                PositionedCommitReference {
                    transaction_digest: digit.to_string().repeat(64),
                    request_digest: "e".repeat(64),
                    envelope_checksum: digit.to_string().repeat(64),
                    sequence,
                    rows: 1,
                    encoded_bytes: 1,
                    materialized_collection_generation: 0,
                },
            )
            .unwrap();
        }
        let target = PositionedMaterializationWatermark::empty()
            .advanced(7, 3, 1, &"c".repeat(64))
            .unwrap()
            .advanced(7, 3, 2, &"d".repeat(64))
            .unwrap();
        let wrong = PositionedMaterializationWatermark::from_parts(2, "f".repeat(64)).unwrap();

        let error = checkpointed_head(&head, &wrong, 9).unwrap_err().to_string();
        assert!(error.contains("prefix digest"), "{error}");
        assert_eq!(head.materialized_sequence, 0);
        assert_eq!(head.pending.len(), 2);

        let checkpointed = checkpointed_head(&head, &target, 9).unwrap();
        assert_eq!(checkpointed.materialized_sequence, 2);
        assert_eq!(
            checkpointed.materialized_prefix_digest,
            target.prefix_digest()
        );
        assert!(checkpointed.pending.is_empty());
    }

    #[test]
    fn snapshot_watermark_reconstruction_returns_corruption_without_panicking() {
        let mut heads = (0..SOURCE_SHARD_COUNT)
            .map(|shard| PositionedShardHead::empty(7, shard, &"a".repeat(64)).unwrap())
            .collect::<Vec<_>>();
        heads[3].materialized_sequence = 1;
        heads[3].durable_sequence = 1;

        let error = materialized_watermarks_from_heads(heads.iter())
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("nonzero") && error.contains("empty"),
            "{error}"
        );
    }

    #[test]
    fn authorized_envelope_validation_reaches_path_digest_and_total_checks() {
        let payload_checksum = "b".repeat(64);
        let envelope = PositionedMutationEnvelope {
            transaction_id: "authorized-decoder".to_owned(),
            schema_fingerprint: "a".repeat(64),
            position: CommitSourcePosition::new(7, 3, 1).unwrap(),
            min_stamp: PositionedMutationStamp {
                hlc: 1,
                writer: [2; 16],
                digest: [3; 32],
            },
            max_stamp: PositionedMutationStamp {
                hlc: 1,
                writer: [2; 16],
                digest: [3; 32],
            },
            payloads: vec![PositionedMutationPayloadRef {
                modality: PositionedMutationModality::PrimaryDense,
                role: "primary".to_owned(),
                id_bloom: Vec::new(),
                format: PositionedPayloadFormat::ArrowIpc,
                path: canonical_payload_path(PositionedPayloadFormat::ArrowIpc, &payload_checksum),
                checksum: payload_checksum,
                rows: 1,
                encoded_bytes: 128,
            }],
        };
        let transaction_digest = blake3::hash(envelope.transaction_id.as_bytes())
            .to_hex()
            .to_string();
        let request_digest = request_digest(
            &envelope.transaction_id,
            &envelope.schema_fingerprint,
            envelope.min_stamp,
            envelope.max_stamp,
            &envelope.payloads,
        );
        let decoded = |envelope: PositionedMutationEnvelope| DecodedPositionedEnvelope {
            envelope,
            transaction_digest: transaction_digest.clone(),
            request_digest: request_digest.clone(),
        };
        let reference = PositionedCommitReference {
            transaction_digest: transaction_digest.clone(),
            request_digest: request_digest.clone(),
            envelope_checksum: "c".repeat(64),
            sequence: 1,
            rows: 1,
            encoded_bytes: 128,
            materialized_collection_generation: 0,
        };
        assert!(validate_authorized_envelope(decoded(envelope.clone()), 7, 3, &reference).is_ok());

        let mut bad_digest = reference.clone();
        bad_digest.transaction_digest = "d".repeat(64);
        assert!(
            validate_authorized_envelope(decoded(envelope.clone()), 7, 3, &bad_digest).is_err()
        );
        let mut bad_total = reference.clone();
        bad_total.encoded_bytes += 1;
        assert!(validate_authorized_envelope(decoded(envelope.clone()), 7, 3, &bad_total).is_err());
        let mut bad_path = envelope;
        bad_path.payloads[0].path = "positioned-log/payloads/wrong".to_owned();
        assert!(validate_authorized_envelope(decoded(bad_path), 7, 3, &reference).is_err());
    }

    fn fixture_coverage_for_all_shards_and_levels() -> CommitSourceRangeSet {
        CommitSourceRangeSet::new(fixture_ranges_for_all_shards_and_levels()).unwrap()
    }

    fn fixture_ranges_for_all_shards_and_levels() -> Vec<CommitSourceRange> {
        (1..=64)
            .flat_map(|source_epoch| {
                (0..SOURCE_SHARD_COUNT).map(move |shard| range(source_epoch, shard, 1, 1))
            })
            .collect()
    }
}
