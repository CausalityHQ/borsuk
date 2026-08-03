//! Stable logical-cell identities and cell-local WAL lane layout.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use object_store::UpdateVersion;
use rayon::prelude::*;

use crate::storage::Storage;
use crate::{BorsukError, Result};

/// Default number of independently published WAL lanes owned by each cell.
pub const DEFAULT_CELL_WAL_LANES: u8 = 8;
/// Maximum supported lanes per logical cell.
pub const MAX_CELL_WAL_LANES: u8 = 64;
const CELL_WAL_CODEC_VERSION: u8 = 1;
const CELL_WAL_CHECKSUM_LEN: usize = 32;
const CELL_WAL_HEAD_MAGIC: &[u8; 4] = b"BWH1";
const CELL_WAL_NODE_MAGIC: &[u8; 4] = b"BWN1";
const CELL_WAL_DESCRIPTOR_MAGIC: &[u8; 4] = b"BWD1";
const CELL_WAL_COMMIT_MAGIC: &[u8; 4] = b"BWC1";
const CELL_WAL_STATE_MAGIC: &[u8; 4] = b"BWS1";
const CELL_WAL_CLAIM_MAGIC: &[u8; 4] = b"BCL1";
/// Routing-independent explicit-ID coordination shards.
pub(crate) const CELL_WAL_CLAIM_SHARDS: u16 = 4_096;
pub(crate) const CELL_WAL_GENERATION_SHARDS: u8 = 16;
const CELL_WAL_CLAIM_PAGE_SLOTS: u16 = 192;
const CELL_WAL_TRANSACTION_TTL_MS: u64 = 5 * 60 * 1_000;

/// Stable write ownership unit for one routing epoch.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct LogicalCellId {
    /// Routing topology version that created the cell.
    pub routing_epoch: u64,
    /// Stable ordinal within the routing epoch.
    pub cell_ordinal: u32,
}

impl LogicalCellId {
    /// Construct a logical cell identifier.
    #[must_use]
    pub const fn new(routing_epoch: u64, cell_ordinal: u32) -> Self {
        Self {
            routing_epoch,
            cell_ordinal,
        }
    }
}

/// Per-cell immutable WAL-lane policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CellWalConfig {
    /// Independent conditional lane heads per logical cell.
    pub lane_count: u8,
}

impl Default for CellWalConfig {
    fn default() -> Self {
        Self {
            lane_count: DEFAULT_CELL_WAL_LANES,
        }
    }
}

impl CellWalConfig {
    /// Validate the production-supported lane-count range.
    pub fn validate(self) -> Result<()> {
        if !(1..=MAX_CELL_WAL_LANES).contains(&self.lane_count) {
            return Err(BorsukError::InvalidStorage(format!(
                "cell WAL lane count must be in 1..={MAX_CELL_WAL_LANES}, got {}",
                self.lane_count
            )));
        }
        Ok(())
    }

    /// Select a stable lane by hashing the handle's stable writer identifier.
    pub fn lane_for_writer(self, writer_id: &[u8]) -> Result<u8> {
        self.validate()?;
        if writer_id.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "cell WAL writer id must not be empty".to_string(),
            ));
        }
        let digest = blake3::hash(writer_id);
        Ok(digest.as_bytes()[0] % self.lane_count)
    }
}

/// Stable object paths for one logical cell WAL lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellWalObjectPaths {
    cell: LogicalCellId,
    lane: u8,
}

impl CellWalObjectPaths {
    /// Construct paths for a supported lane ordinal.
    pub fn new(cell: LogicalCellId, lane: u8) -> Result<Self> {
        if lane >= MAX_CELL_WAL_LANES {
            return Err(BorsukError::InvalidStorage(format!(
                "cell WAL lane ordinal must be below {MAX_CELL_WAL_LANES}, got {lane}"
            )));
        }
        Ok(Self { cell, lane })
    }

    fn prefix(&self) -> String {
        format!(
            "cells/{}/{}/wal/{}",
            self.cell.routing_epoch, self.cell.cell_ordinal, self.lane
        )
    }

    /// Conditional pointer for the lane's persistent linked frontier.
    #[must_use]
    pub fn head(&self) -> String {
        format!("{}/HEAD", self.prefix())
    }

    /// Transaction-scoped content-addressed immutable mutation run.
    ///
    /// The transaction namespace is correctness-critical: a new root
    /// reservation must not reuse an old orphan with an expired object-store
    /// timestamp while concurrent GC is deciding whether to reclaim it.
    #[must_use]
    pub fn run(
        &self,
        transaction_id: &str,
        kind: CellWalRunKind,
        checksum: &str,
        extension: &str,
    ) -> String {
        format!(
            "{}/runs/{}/transactions/{transaction_id}/{checksum}.{extension}",
            self.prefix(),
            kind.as_str()
        )
    }

    /// Transaction-scoped node linking a prepared run to the preceding head.
    #[must_use]
    pub fn frontier_node(&self, transaction_id: &str, checksum: &str) -> String {
        format!(
            "{}/frontier/transactions/{transaction_id}/{checksum}.bin",
            self.prefix()
        )
    }
}

/// One immutable mutation run prepared in a cell lane.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PreparedCellWalRun {
    /// Transaction that owns this prepared run.
    pub transaction_id: String,
    /// Stable logical cell receiving the mutation.
    pub cell: LogicalCellId,
    /// Cell-local WAL lane.
    pub lane: u8,
    /// Logical payload role. Readers dispatch from this persisted value rather
    /// than inferring semantics from a file extension.
    #[serde(default)]
    pub kind: CellWalRunKind,
    /// Role-specific immutable metadata (for example an ID bloom).
    #[serde(default)]
    pub metadata: Vec<u8>,
    /// Content-addressed payload path.
    pub path: String,
    /// BLAKE3 payload checksum.
    pub checksum: String,
    /// Logical records carried by the run.
    pub record_count: usize,
    /// Encoded payload bytes.
    pub byte_len: u64,
}

/// Content-addressed reference to one persistent frontier node.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CellWalFrontierRef {
    /// Immutable node path.
    pub path: String,
    /// BLAKE3 checksum of the serialized node.
    pub checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CellWalFrontierNode {
    run: PreparedCellWalRun,
    previous: Option<CellWalFrontierRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CellWalLaneHead {
    generation: u64,
    node: Option<CellWalFrontierRef>,
}

/// Immutable descriptor that becomes visible through one commit marker.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CellWalTransactionDescriptor {
    /// Stable transaction identity, derived from an idempotency key when given.
    pub transaction_id: String,
    /// Every prepared cell run made visible by the commit.
    pub runs: Vec<PreparedCellWalRun>,
    /// Caller-owned immutable transaction metadata. The cell-WAL protocol
    /// checksums this together with the run list and exposes both through the
    /// same commit marker.
    #[serde(default)]
    pub metadata: Vec<u8>,
}

/// Prepared transaction whose lane entries are reachable but not yet visible.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PreparedCellWalTransaction {
    /// Transaction identity.
    pub transaction_id: String,
    /// Immutable descriptor path.
    pub descriptor_path: String,
    /// Descriptor checksum to pin in the commit marker.
    pub descriptor_checksum: String,
    /// Prepared runs awaiting one atomic commit marker.
    pub runs: Vec<PreparedCellWalRun>,
    /// Caller-owned metadata pinned by the descriptor checksum.
    #[serde(default)]
    pub metadata: Vec<u8>,
}

/// Reference returned after an atomic cell-WAL commit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CommittedCellWalTransaction {
    /// Transaction identity.
    pub transaction_id: String,
    /// Immutable descriptor path.
    pub descriptor_path: String,
    /// Descriptor checksum pinned by the commit marker.
    pub descriptor_checksum: String,
    /// Runs made visible together.
    pub runs: Vec<PreparedCellWalRun>,
    /// Caller-owned metadata made visible atomically with every run.
    #[serde(default)]
    pub metadata: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CellWalCommitMarker {
    descriptor_path: String,
    descriptor_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CellWalTransactionState {
    Prepared {
        expires_at_ms: u64,
    },
    Committing {
        descriptor_path: String,
        descriptor_checksum: String,
    },
    Committed {
        descriptor_path: String,
        descriptor_checksum: String,
    },
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CellWalClaimLock {
    Available { revision: String },
    Owned { transaction_id: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CellWalClaimPage {
    slots: BTreeMap<u16, CellWalClaimLock>,
}

/// Encoded mutation payload to prepare in one logical cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellWalRunInput {
    /// Logical cell receiving the run.
    pub cell: LogicalCellId,
    /// Logical payload role.
    pub kind: CellWalRunKind,
    /// Role-specific immutable metadata checksummed through the transaction
    /// descriptor.
    pub metadata: Vec<u8>,
    /// Encoded immutable run payload.
    pub bytes: Vec<u8>,
    /// Logical mutation records in the payload.
    pub record_count: usize,
    /// Physical format extension persisted in the object path.
    pub extension: String,
}

/// Persisted logical role of a cell-WAL payload.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum CellWalRunKind {
    /// Inserted or replacement vector records.
    #[default]
    Records,
    /// `(record id, minimum visible generation)` mutation rows.
    Tombstones,
    /// Hash-partitioned record ownership updates.
    IdDirectory,
}

impl CellWalRunKind {
    /// Stable path/schema name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Records => "records",
            Self::Tombstones => "tombstones",
            Self::IdDirectory => "id-directory",
        }
    }
}

/// Cell-WAL protocol handle over a shared object store.
///
/// This low-level handle is public so concurrency and failure-injection
/// harnesses can verify the persistence protocol independently of indexing.
pub struct CellWalStore {
    storage: Storage,
    config: CellWalConfig,
    writer_id: Vec<u8>,
}

pub(crate) struct CellWalClaimGuard {
    storage: Storage,
    transaction_id: String,
    locks: Vec<CellWalHeldClaim>,
    transaction_committed: bool,
}

#[derive(Debug)]
struct CellWalHeldClaim {
    path: String,
    previous_revisions: Vec<(u16, Option<String>)>,
    owned_version: UpdateVersion,
    owned_page: CellWalClaimPage,
}

pub(crate) type CellWalClaimCheckpoint = BTreeMap<u16, String>;

impl CellWalClaimGuard {
    pub(crate) fn matches_checkpoint(&self, checkpoint: &CellWalClaimCheckpoint) -> bool {
        self.locks.iter().all(|claim| {
            claim.previous_revisions.iter().all(|(shard, revision)| {
                revision.as_ref().map_or_else(
                    || !checkpoint.contains_key(shard),
                    |revision| checkpoint.get(shard) == Some(revision),
                )
            })
        })
    }

    /// Snapshot all durable revisions while these claims are held.
    ///
    /// A caller takes this snapshot before refreshing and adopts it only after
    /// validating the refreshed collection snapshot. Holding the current IDs'
    /// claim pages fences their observations until the mutation either commits
    /// or aborts; later changes to any other shard advance its revision and
    /// invalidate the resulting checkpoint rather than becoming invisible.
    pub(crate) fn synchronized_checkpoint(&self) -> Result<CellWalClaimCheckpoint> {
        let page_count = CELL_WAL_CLAIM_SHARDS.div_ceil(CELL_WAL_CLAIM_PAGE_SLOTS);
        let pages = crate::parallel::install_io(|| {
            (0..page_count)
                .into_par_iter()
                .map(|page| {
                    let page = u8::try_from(page).expect("claim page index fits u8");
                    let path = claim_page_path(page);
                    self.storage
                        .read_coordination_object(&path)?
                        .map(|object| claim_page_from_slice(&object.bytes, &path, page))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()
        })?;
        let mut checkpoint = pages
            .into_iter()
            .flatten()
            .flat_map(|page| page.slots.into_iter())
            .filter_map(|(shard, lock)| match lock {
                CellWalClaimLock::Available { revision } => Some((shard, revision)),
                CellWalClaimLock::Owned { .. } => None,
            })
            .collect::<CellWalClaimCheckpoint>();

        // The current guard owns its slots, so the page snapshot cannot expose
        // their predecessors. Merge those fenced observations explicitly.
        for claim in &self.locks {
            for (shard, revision) in &claim.previous_revisions {
                match revision {
                    Some(revision) => {
                        checkpoint.insert(*shard, revision.clone());
                    }
                    None => {
                        checkpoint.remove(shard);
                    }
                }
            }
        }
        Ok(checkpoint)
    }

    pub(crate) fn finish(&mut self) -> CellWalClaimCheckpoint {
        self.transaction_committed = true;
        self.release()
    }

    fn release(&mut self) -> CellWalClaimCheckpoint {
        release_claims(
            &self.storage,
            &self.transaction_id,
            self.locks.drain(..).collect(),
        )
    }
}

impl Drop for CellWalClaimGuard {
    fn drop(&mut self) {
        if !self.transaction_committed {
            let _ = abort_prepared_transaction(&self.storage, &self.transaction_id);
        }
        let _ = self.release();
    }
}

impl CellWalStore {
    /// Construct a protocol handle for one stable writer identity.
    pub fn new(
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        uri: impl Into<String>,
        config: CellWalConfig,
        writer_id: impl Into<Vec<u8>>,
    ) -> Result<Self> {
        config.validate()?;
        let writer_id = writer_id.into();
        if writer_id.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "cell WAL writer id must not be empty".to_string(),
            ));
        }
        Ok(Self {
            storage: Storage::from_object_store(uri.into(), store)?,
            config,
            writer_id,
        })
    }

    pub(crate) fn from_storage(
        storage: Storage,
        config: CellWalConfig,
        writer_id: Vec<u8>,
    ) -> Result<Self> {
        config.validate()?;
        if writer_id.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "cell WAL writer id must not be empty".to_string(),
            ));
        }
        Ok(Self {
            storage,
            config,
            writer_id,
        })
    }

    pub(crate) fn claim_ids<'a, I>(&self, transaction_id: &str, ids: I) -> Result<CellWalClaimGuard>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        validate_transaction_id(transaction_id)?;
        ensure_prepared_transaction(&self.storage, transaction_id)?;
        let shards = ids.into_iter().map(id_claim_shard).collect::<BTreeSet<_>>();
        let mut guard = CellWalClaimGuard {
            storage: self.storage.clone(),
            transaction_id: transaction_id.to_string(),
            locks: Vec::with_capacity(shards.len()),
            transaction_committed: false,
        };
        guard.locks = acquire_claim_shards(&self.storage, transaction_id, &shards)?;
        Ok(guard)
    }

    /// Prepare every run and atomically expose the transaction.
    pub fn commit(
        &self,
        transaction_id: &str,
        inputs: &[CellWalRunInput],
    ) -> Result<CommittedCellWalTransaction> {
        self.commit_with_metadata(transaction_id, inputs, &[])
    }

    /// Prepare and atomically expose runs plus caller-owned metadata.
    pub fn commit_with_metadata(
        &self,
        transaction_id: &str,
        inputs: &[CellWalRunInput],
        metadata: &[u8],
    ) -> Result<CommittedCellWalTransaction> {
        let prepared = self.prepare_transaction_with_metadata(transaction_id, inputs, metadata)?;
        self.commit_prepared(&prepared)
    }

    /// Stage immutable runs and one checked descriptor for collection-root
    /// authorization. The collection frontier is the visibility authority, so
    /// this path deliberately omits lane heads and a second commit marker.
    pub(crate) fn stage_root_authorized_with_metadata(
        &self,
        transaction_id: &str,
        inputs: &[CellWalRunInput],
        metadata: &[u8],
    ) -> Result<CommittedCellWalTransaction> {
        validate_transaction_id(transaction_id)?;
        if inputs.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "cell WAL transaction must contain at least one run".to_string(),
            ));
        }
        for input in inputs {
            validate_cell_wal_run_extension(input.kind, &input.extension)?;
        }
        let lane = self.config.lane_for_writer(&self.writer_id)?;
        let mut prepared = inputs
            .iter()
            .map(|input| {
                let checksum = blake3::hash(&input.bytes).to_hex().to_string();
                let paths = CellWalObjectPaths::new(input.cell, lane)?;
                let path = paths.run(transaction_id, input.kind, &checksum, &input.extension);
                Ok((
                    PreparedCellWalRun {
                        transaction_id: transaction_id.to_string(),
                        cell: input.cell,
                        lane,
                        kind: input.kind,
                        metadata: input.metadata.clone(),
                        path,
                        checksum,
                        record_count: input.record_count,
                        byte_len: input.bytes.len() as u64,
                    },
                    input.bytes.as_slice(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        prepared.sort_by_key(|(run, _)| (run.cell, run.lane, run.checksum.clone()));
        let runs = prepared
            .iter()
            .map(|(run, _)| run.clone())
            .collect::<Vec<_>>();
        let descriptor = CellWalTransactionDescriptor {
            transaction_id: transaction_id.to_string(),
            runs: runs.clone(),
            metadata: metadata.to_vec(),
        };
        let descriptor_bytes = transaction_descriptor_bytes(&descriptor)?;
        let descriptor_checksum = blake3::hash(&descriptor_bytes).to_hex().to_string();
        let descriptor_path =
            format!("transactions/{transaction_id}/descriptors/{descriptor_checksum}.bin");
        // Every immutable path and checksum is known before upload. Publish the
        // checked descriptor alongside its payloads rather than after them; the
        // later collection-root CAS still waits for both branches and remains
        // the sole visibility boundary.
        let (payloads, descriptor_upload) = crate::parallel::install_io(|| {
            rayon::join(
                || {
                    prepared
                        .par_iter()
                        .map(|(run, bytes)| {
                            self.storage.write_bytes_content_addressed(&run.path, bytes)
                        })
                        .collect::<Result<Vec<_>>>()
                },
                || {
                    self.storage
                        .write_bytes_content_addressed(&descriptor_path, &descriptor_bytes)
                },
            )
        });
        payloads?;
        descriptor_upload?;
        Ok(CommittedCellWalTransaction {
            transaction_id: transaction_id.to_string(),
            descriptor_path,
            descriptor_checksum,
            runs,
            metadata: metadata.to_vec(),
        })
    }

    /// Best-effort claim-owner finalization after collection-root visibility.
    pub(crate) fn mark_root_authorized(
        &self,
        transaction: &CommittedCellWalTransaction,
    ) -> Result<()> {
        let state_path = transaction_state_path(&transaction.transaction_id);
        let Some(state) = self.storage.read_coordination_object(&state_path)? else {
            return Ok(());
        };
        let committed = CellWalTransactionState::Committed {
            descriptor_path: transaction.descriptor_path.clone(),
            descriptor_checksum: transaction.descriptor_checksum.clone(),
        };
        match transaction_state_from_slice(&state.bytes, &state_path)? {
            CellWalTransactionState::Prepared { .. } => {
                match self.storage.write_coordination_object(
                    &state_path,
                    &transaction_state_bytes(&committed)?,
                    Some(state.version),
                ) {
                    Ok(_) | Err(BorsukError::ConcurrentModification { .. }) => Ok(()),
                    Err(error) => Err(error),
                }
            }
            CellWalTransactionState::Committed {
                descriptor_path,
                descriptor_checksum,
            } if descriptor_path == transaction.descriptor_path
                && descriptor_checksum == transaction.descriptor_checksum =>
            {
                Ok(())
            }
            other => Err(BorsukError::InvalidStorage(format!(
                "root-authorized transaction `{}` has conflicting state {other:?}",
                transaction.transaction_id
            ))),
        }
    }

    /// Publish prepared lane entries and their immutable descriptor without
    /// making any run visible to readers.
    pub fn prepare_transaction(
        &self,
        transaction_id: &str,
        inputs: &[CellWalRunInput],
    ) -> Result<PreparedCellWalTransaction> {
        self.prepare_transaction_with_metadata(transaction_id, inputs, &[])
    }

    /// Publish prepared lane entries and a descriptor containing immutable
    /// caller metadata without making the transaction visible.
    pub fn prepare_transaction_with_metadata(
        &self,
        transaction_id: &str,
        inputs: &[CellWalRunInput],
        metadata: &[u8],
    ) -> Result<PreparedCellWalTransaction> {
        self.prepare_transaction_with_metadata_inner(transaction_id, inputs, metadata, true)
    }

    fn prepare_transaction_with_metadata_inner(
        &self,
        transaction_id: &str,
        inputs: &[CellWalRunInput],
        metadata: &[u8],
        ensure_state: bool,
    ) -> Result<PreparedCellWalTransaction> {
        validate_transaction_id(transaction_id)?;
        if inputs.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "cell WAL transaction must contain at least one run".to_string(),
            ));
        }
        for input in inputs {
            validate_cell_wal_run_extension(input.kind, &input.extension)?;
        }
        if ensure_state {
            ensure_prepared_transaction(&self.storage, transaction_id)?;
        }
        let lane = self.config.lane_for_writer(&self.writer_id)?;
        let mut prepared = inputs
            .iter()
            .map(|input| {
                let checksum = blake3::hash(&input.bytes).to_hex().to_string();
                let paths = CellWalObjectPaths::new(input.cell, lane)?;
                let path = paths.run(transaction_id, input.kind, &checksum, &input.extension);
                Ok((
                    PreparedCellWalRun {
                        transaction_id: transaction_id.to_string(),
                        cell: input.cell,
                        lane,
                        kind: input.kind,
                        metadata: input.metadata.clone(),
                        path,
                        checksum,
                        record_count: input.record_count,
                        byte_len: input.bytes.len() as u64,
                    },
                    input.bytes.as_slice(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        prepared.sort_by_key(|(run, _)| (run.cell, run.lane, run.checksum.clone()));
        let runs = prepared
            .iter()
            .map(|(run, _)| run.clone())
            .collect::<Vec<_>>();

        if let Some(committed) = self.load_committed_transaction(transaction_id)? {
            if committed.runs != runs || committed.metadata != metadata {
                return Err(BorsukError::InvalidStorage(format!(
                    "idempotency transaction `{transaction_id}` was retried with different runs or metadata"
                )));
            }
            return Ok(PreparedCellWalTransaction {
                transaction_id: committed.transaction_id,
                descriptor_path: committed.descriptor_path,
                descriptor_checksum: committed.descriptor_checksum,
                runs: committed.runs,
                metadata: committed.metadata,
            });
        }

        crate::parallel::install_io(|| {
            prepared
                .par_iter()
                .map(|(run, bytes)| self.storage.write_bytes_content_addressed(&run.path, bytes))
                .collect::<Result<Vec<_>>>()
        })?;
        let mut lane_groups = BTreeMap::<(LogicalCellId, u8), Vec<&PreparedCellWalRun>>::new();
        for (run, _) in &prepared {
            lane_groups
                .entry((run.cell, run.lane))
                .or_default()
                .push(run);
        }
        let lane_groups = lane_groups.into_values().collect::<Vec<_>>();
        let publication_results = crate::parallel::install_io(|| {
            lane_groups
                .par_iter()
                .map(|runs| self.publish_prepared_runs(runs))
                .collect::<Vec<_>>()
        });
        if publication_results.iter().any(Result::is_err) {
            let published_runs = lane_groups
                .iter()
                .zip(&publication_results)
                .filter(|(_, result)| result.is_ok())
                .flat_map(|(runs, _)| runs.iter().copied())
                .collect::<Vec<_>>();
            let mut error = publication_results
                .into_iter()
                .find_map(Result::err)
                .expect("at least one lane publication failed");
            if let Err(cleanup_error) = self.rollback_prepared_runs(transaction_id, &published_runs)
            {
                error = cleanup_error;
            }
            return Err(error);
        }

        let descriptor = CellWalTransactionDescriptor {
            transaction_id: transaction_id.to_string(),
            runs: runs.clone(),
            metadata: metadata.to_vec(),
        };
        let descriptor_bytes = transaction_descriptor_bytes(&descriptor)?;
        let descriptor_checksum = blake3::hash(&descriptor_bytes).to_hex().to_string();
        let descriptor_path =
            format!("transactions/{transaction_id}/descriptors/{descriptor_checksum}.bin");
        if let Err(error) = self
            .storage
            .write_bytes_content_addressed(&descriptor_path, &descriptor_bytes)
        {
            return match self
                .rollback_prepared_runs(transaction_id, &runs.iter().collect::<Vec<_>>())
            {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(cleanup_error),
            };
        }
        Ok(PreparedCellWalTransaction {
            transaction_id: transaction_id.to_string(),
            descriptor_path,
            descriptor_checksum,
            runs,
            metadata: metadata.to_vec(),
        })
    }

    /// Atomically expose every run listed by a prepared descriptor.
    pub fn commit_prepared(
        &self,
        prepared: &PreparedCellWalTransaction,
    ) -> Result<CommittedCellWalTransaction> {
        validate_transaction_id(&prepared.transaction_id)?;
        let descriptor = self.storage.read_bytes_with_cache_status_and_checksum(
            &prepared.descriptor_path,
            &prepared.descriptor_checksum,
        )?;
        let descriptor =
            transaction_descriptor_from_slice(&descriptor.bytes, &prepared.descriptor_path)?;
        if descriptor.transaction_id != prepared.transaction_id
            || descriptor.runs != prepared.runs
            || descriptor.metadata != prepared.metadata
        {
            return Err(BorsukError::InvalidStorage(format!(
                "prepared transaction `{}` does not match its descriptor",
                prepared.transaction_id
            )));
        }
        let committing = CellWalTransactionState::Committing {
            descriptor_path: prepared.descriptor_path.clone(),
            descriptor_checksum: prepared.descriptor_checksum.clone(),
        };
        let state_path = transaction_state_path(&prepared.transaction_id);
        let state = self
            .storage
            .read_coordination_object(&state_path)?
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "prepared transaction `{}` has no fenced state",
                    prepared.transaction_id
                ))
            })?;
        let committing_version = match transaction_state_from_slice(&state.bytes, &state_path)? {
            CellWalTransactionState::Prepared { .. } => self.storage.write_coordination_object(
                &state_path,
                &transaction_state_bytes(&committing)?,
                Some(state.version),
            )?,
            CellWalTransactionState::Committing {
                descriptor_path,
                descriptor_checksum,
            }
            | CellWalTransactionState::Committed {
                descriptor_path,
                descriptor_checksum,
            } if descriptor_path == prepared.descriptor_path
                && descriptor_checksum == prepared.descriptor_checksum =>
            {
                state.version
            }
            CellWalTransactionState::Aborted => {
                return Err(BorsukError::ConcurrentModification { path: state_path });
            }
            other => {
                return Err(BorsukError::InvalidStorage(format!(
                    "transaction `{}` has conflicting fenced state {other:?}",
                    prepared.transaction_id
                )));
            }
        };
        let marker = CellWalCommitMarker {
            descriptor_path: prepared.descriptor_path.clone(),
            descriptor_checksum: prepared.descriptor_checksum.clone(),
        };
        let marker_bytes = commit_marker_bytes(&marker)?;
        let marker_path = commit_marker_path(&prepared.transaction_id);
        match self
            .storage
            .write_coordination_object(&marker_path, &marker_bytes, None)
        {
            Ok(_) => {}
            Err(BorsukError::ConcurrentModification { .. }) => {
                let existing = self
                    .storage
                    .read_coordination_object(&marker_path)?
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(format!(
                            "commit marker `{marker_path}` disappeared after create conflict"
                        ))
                    })?;
                if existing.bytes != marker_bytes {
                    return Err(BorsukError::InvalidStorage(format!(
                        "transaction `{}` has a conflicting commit marker",
                        prepared.transaction_id
                    )));
                }
            }
            Err(error) => return Err(error),
        }
        let committed = CellWalTransactionState::Committed {
            descriptor_path: prepared.descriptor_path.clone(),
            descriptor_checksum: prepared.descriptor_checksum.clone(),
        };
        let _ = self.storage.write_coordination_object(
            &state_path,
            &transaction_state_bytes(&committed)?,
            Some(committing_version),
        );
        Ok(CommittedCellWalTransaction {
            transaction_id: prepared.transaction_id.clone(),
            descriptor_path: prepared.descriptor_path.clone(),
            descriptor_checksum: prepared.descriptor_checksum.clone(),
            runs: prepared.runs.clone(),
            metadata: prepared.metadata.clone(),
        })
    }

    fn publish_prepared_runs(&self, runs: &[&PreparedCellWalRun]) -> Result<()> {
        const MAX_CAS_ATTEMPTS: usize = 128;
        let Some(first) = runs.first() else {
            return Ok(());
        };
        if runs
            .iter()
            .any(|run| run.cell != first.cell || run.lane != first.lane)
        {
            return Err(BorsukError::InvalidStorage(
                "one lane publication group contains mixed cell or lane identities".to_string(),
            ));
        }
        let paths = CellWalObjectPaths::new(first.cell, first.lane)?;
        let head_path = paths.head();
        let target_identities = runs
            .iter()
            .map(|run| cell_wal_run_identity(run))
            .collect::<BTreeSet<_>>();
        for _ in 0..MAX_CAS_ATTEMPTS {
            let current = match self.storage.read_coordination_object(&head_path) {
                Ok(current) => current,
                Err(BorsukError::ObjectStoreRetryable { .. }) => continue,
                Err(error) => return Err(error),
            };
            let (generation, mut previous, version) = match current {
                Some(current) => {
                    let head = lane_head_from_slice(&current.bytes, &head_path)?;
                    if let Some(node) = &head.node {
                        let mut current_runs = Vec::new();
                        self.collect_frontier_runs(node, &mut current_runs)?;
                        let matching_identities = current_runs
                            .iter()
                            .filter(|run| {
                                run.transaction_id == first.transaction_id
                                    && run.cell == first.cell
                                    && run.lane == first.lane
                            })
                            .map(cell_wal_run_identity)
                            .collect::<BTreeSet<_>>();
                        if target_identities == matching_identities {
                            return Ok(());
                        }
                        if !matching_identities.is_empty() {
                            return Err(BorsukError::InvalidStorage(format!(
                                "cell WAL transaction `{}` was retried with conflicting runs in cell {:?} lane {}",
                                first.transaction_id, first.cell, first.lane
                            )));
                        }
                    }
                    (
                        head.generation.checked_add(1).ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "cell WAL lane generation exceeds u64".to_string(),
                            )
                        })?,
                        head.node,
                        Some(current.version),
                    )
                }
                None => (1, None, None),
            };
            for run in runs {
                let node = CellWalFrontierNode {
                    run: (*run).clone(),
                    previous,
                };
                let node_bytes = frontier_node_bytes(&node)?;
                let node_checksum = blake3::hash(&node_bytes).to_hex().to_string();
                let node_ref = CellWalFrontierRef {
                    path: paths.frontier_node(&run.transaction_id, &node_checksum),
                    checksum: node_checksum,
                };
                self.storage
                    .write_bytes_content_addressed(&node_ref.path, &node_bytes)?;
                previous = Some(node_ref);
            }
            let head_bytes = lane_head_bytes(&CellWalLaneHead {
                generation,
                node: previous,
            })?;
            match self
                .storage
                .write_coordination_object(&head_path, &head_bytes, version)
            {
                Ok(_) => return Ok(()),
                Err(
                    BorsukError::ConcurrentModification { .. }
                    | BorsukError::ObjectStoreRetryable { .. },
                ) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(BorsukError::ConcurrentModification { path: head_path })
    }

    fn rollback_prepared_runs(
        &self,
        transaction_id: &str,
        runs: &[&PreparedCellWalRun],
    ) -> Result<()> {
        let cells = runs
            .iter()
            .map(|run| run.cell)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let identities = runs
            .iter()
            .map(|run| cell_wal_run_identity(run))
            .collect::<BTreeSet<_>>();
        let prune_result = self.prune_consumed_runs(&cells, &identities);
        let abort_result = abort_prepared_transaction(&self.storage, transaction_id);
        prune_result?;
        abort_result?;
        Ok(())
    }

    /// Double-collect lane heads and return only atomically committed runs.
    pub fn committed_runs_snapshot(
        &self,
        cells: &[LogicalCellId],
    ) -> Result<Vec<PreparedCellWalRun>> {
        Ok(self
            .committed_transactions_snapshot(cells)?
            .into_iter()
            .flat_map(|transaction| transaction.runs)
            .collect())
    }

    /// Double-collect lane heads and return complete committed transactions.
    ///
    /// A descriptor is admitted only when every run it lists is reachable in
    /// the collected frontier. This prevents a reader from exposing a partial
    /// multi-cell transaction if the supplied catalog omits a required cell or
    /// a frontier is corrupt.
    pub fn committed_transactions_snapshot(
        &self,
        cells: &[LogicalCellId],
    ) -> Result<Vec<CommittedCellWalTransaction>> {
        self.committed_transactions_snapshot_with_retries(cells)
            .map(|(transactions, _)| transactions)
    }

    /// Double-collect committed transactions and report how many unstable
    /// observations were retried before the returned snapshot stabilized.
    pub fn committed_transactions_snapshot_with_retries(
        &self,
        cells: &[LogicalCellId],
    ) -> Result<(Vec<CommittedCellWalTransaction>, usize)> {
        const MAX_SNAPSHOT_ATTEMPTS: usize = 32;
        for retries in 0..MAX_SNAPSHOT_ATTEMPTS {
            let first = self.collect_heads(cells)?;
            let mut prepared_runs = Vec::new();
            for (_, head) in &first {
                if let Some(node) = head.as_ref().and_then(|head| head.node.as_ref()) {
                    self.collect_frontier_runs(node, &mut prepared_runs)?;
                }
            }
            let mut transactions = self.filter_committed_transactions(&prepared_runs)?;
            let second = self.collect_heads(cells)?;
            if heads_match(&first, &second) {
                transactions.sort_by(|left, right| left.transaction_id.cmp(&right.transaction_id));
                transactions.dedup_by(|left, right| left.transaction_id == right.transaction_id);
                return Ok((transactions, retries));
            }
        }
        Err(BorsukError::ConcurrentModification {
            path: "cell WAL snapshot".to_string(),
        })
    }

    /// Load an exact descriptor authorized by the collection frontier. The
    /// descriptor itself does not grant visibility; the caller must have
    /// validated the frontier entry that supplied this path and checksum.
    pub(crate) fn load_authorized_descriptor(
        &self,
        transaction_id: &str,
        descriptor_path: &str,
        descriptor_checksum: &str,
    ) -> Result<CommittedCellWalTransaction> {
        validate_transaction_id(transaction_id)?;
        let expected_prefix = format!("transactions/{transaction_id}/descriptors/");
        if !descriptor_path.starts_with(&expected_prefix) || !descriptor_path.ends_with(".bin") {
            return Err(BorsukError::InvalidStorage(format!(
                "authorized descriptor `{descriptor_path}` does not belong to transaction `{transaction_id}`"
            )));
        }
        let read = self
            .storage
            .read_bytes_with_cache_status_and_checksum(descriptor_path, descriptor_checksum)?;
        let descriptor = transaction_descriptor_from_slice(&read.bytes, descriptor_path)?;
        if descriptor.transaction_id != transaction_id {
            return Err(BorsukError::InvalidStorage(format!(
                "authorized descriptor `{descriptor_path}` belongs to `{}` instead of `{transaction_id}`",
                descriptor.transaction_id
            )));
        }
        Ok(CommittedCellWalTransaction {
            transaction_id: descriptor.transaction_id,
            descriptor_path: descriptor_path.to_string(),
            descriptor_checksum: descriptor_checksum.to_string(),
            runs: descriptor.runs,
            metadata: descriptor.metadata,
        })
    }

    fn collect_heads(
        &self,
        cells: &[LogicalCellId],
    ) -> Result<Vec<(String, Option<CellWalLaneHead>)>> {
        let mut heads = Vec::with_capacity(cells.len() * usize::from(self.config.lane_count));
        for &cell in cells {
            for lane in 0..self.config.lane_count {
                let path = CellWalObjectPaths::new(cell, lane)?.head();
                let head = self
                    .storage
                    .read_coordination_object(&path)?
                    .map(|object| lane_head_from_slice(&object.bytes, &path))
                    .transpose()?;
                heads.push((path, head));
            }
        }
        Ok(heads)
    }

    fn collect_frontier_runs(
        &self,
        start: &CellWalFrontierRef,
        runs: &mut Vec<PreparedCellWalRun>,
    ) -> Result<()> {
        let mut next = Some(start.clone());
        let mut visited = HashSet::new();
        while let Some(reference) = next {
            if !visited.insert(reference.checksum.clone()) {
                return Err(BorsukError::InvalidStorage(
                    "cell WAL frontier contains a cycle".to_string(),
                ));
            }
            let read = self
                .storage
                .read_bytes_with_cache_status_and_checksum(&reference.path, &reference.checksum)?;
            let node = frontier_node_from_slice(&read.bytes, &reference.path)?;
            runs.push(node.run);
            next = node.previous;
        }
        Ok(())
    }

    /// Remove already materialized runs from every lane frontier with
    /// compare-and-swap rebasing. Concurrent writers either precede this rewrite
    /// and are retained or lose their HEAD CAS and rebase onto the compacted
    /// chain.
    pub(crate) fn prune_consumed_runs(
        &self,
        cells: &[LogicalCellId],
        consumed: &BTreeSet<String>,
    ) -> Result<()> {
        const MAX_CAS_ATTEMPTS: usize = 128;
        if consumed.is_empty() {
            return Ok(());
        }
        for &cell in cells {
            for lane in 0..self.config.lane_count {
                let paths = CellWalObjectPaths::new(cell, lane)?;
                let head_path = paths.head();
                for attempt in 0..MAX_CAS_ATTEMPTS {
                    let Some(current) = self.storage.read_coordination_object(&head_path)? else {
                        break;
                    };
                    let head = lane_head_from_slice(&current.bytes, &head_path)?;
                    let mut nodes = Vec::new();
                    let mut next = head.node.clone();
                    let mut visited = HashSet::new();
                    while let Some(reference) = next {
                        if !visited.insert(reference.checksum.clone()) {
                            return Err(BorsukError::InvalidStorage(
                                "cell WAL frontier contains a cycle".to_string(),
                            ));
                        }
                        let read = self.storage.read_bytes_with_cache_status_and_checksum(
                            &reference.path,
                            &reference.checksum,
                        )?;
                        let node = frontier_node_from_slice(&read.bytes, &reference.path)?;
                        next = node.previous.clone();
                        if !consumed.contains(&cell_wal_run_identity(&node.run)) {
                            nodes.push(node.run);
                        }
                    }
                    let mut previous = None;
                    for run in nodes.into_iter().rev() {
                        let transaction_id = run.transaction_id.clone();
                        let node = CellWalFrontierNode {
                            run,
                            previous: previous.clone(),
                        };
                        let bytes = frontier_node_bytes(&node)?;
                        let checksum = blake3::hash(&bytes).to_hex().to_string();
                        let reference = CellWalFrontierRef {
                            path: paths.frontier_node(&transaction_id, &checksum),
                            checksum,
                        };
                        self.storage
                            .write_bytes_content_addressed(&reference.path, &bytes)?;
                        previous = Some(reference);
                    }
                    let generation = head.generation.checked_add(1).ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "cell WAL lane generation exceeds u64".to_string(),
                        )
                    })?;
                    let bytes = lane_head_bytes(&CellWalLaneHead {
                        generation,
                        node: previous,
                    })?;
                    match self.storage.write_coordination_object(
                        &head_path,
                        &bytes,
                        Some(current.version),
                    ) {
                        Ok(_) => break,
                        Err(BorsukError::ConcurrentModification { .. })
                            if attempt + 1 < MAX_CAS_ATTEMPTS =>
                        {
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        Ok(())
    }

    /// Return every immutable object reachable from a stable double-collected
    /// lane snapshot. This feeds garbage collection's keep set.
    pub(crate) fn active_object_paths(&self, cells: &[LogicalCellId]) -> Result<BTreeSet<String>> {
        const MAX_SNAPSHOT_ATTEMPTS: usize = 32;
        for _ in 0..MAX_SNAPSHOT_ATTEMPTS {
            let first = self.collect_heads(cells)?;
            let mut paths = BTreeSet::new();
            let mut transaction_ids = BTreeSet::new();
            for (_, head) in &first {
                let mut next = head.as_ref().and_then(|head| head.node.clone());
                let mut visited = HashSet::new();
                while let Some(reference) = next {
                    if !visited.insert(reference.checksum.clone()) {
                        return Err(BorsukError::InvalidStorage(
                            "cell WAL frontier contains a cycle".to_string(),
                        ));
                    }
                    paths.insert(reference.path.clone());
                    let read = self.storage.read_bytes_with_cache_status_and_checksum(
                        &reference.path,
                        &reference.checksum,
                    )?;
                    let node = frontier_node_from_slice(&read.bytes, &reference.path)?;
                    paths.insert(node.run.path.clone());
                    transaction_ids.insert(node.run.transaction_id.clone());
                    next = node.previous;
                }
            }
            for transaction_id in transaction_ids {
                if let Some(transaction) = self.load_committed_transaction(&transaction_id)? {
                    paths.insert(transaction.descriptor_path);
                    paths.insert(commit_marker_path(&transaction_id));
                    paths.insert(transaction_state_path(&transaction_id));
                }
            }
            let second = self.collect_heads(cells)?;
            if heads_match(&first, &second) {
                return Ok(paths);
            }
        }
        Err(BorsukError::ConcurrentModification {
            path: "cell WAL active-object snapshot".to_string(),
        })
    }

    pub(crate) fn run_identities_without_root_authorization(
        &self,
        cells: &[LogicalCellId],
        authorized_transaction_ids: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>> {
        const MAX_SNAPSHOT_ATTEMPTS: usize = 32;
        for _ in 0..MAX_SNAPSHOT_ATTEMPTS {
            let first = self.collect_heads(cells)?;
            let mut runs = Vec::new();
            for (_, head) in &first {
                if let Some(node) = head.as_ref().and_then(|head| head.node.as_ref()) {
                    self.collect_frontier_runs(node, &mut runs)?;
                }
            }
            let second = self.collect_heads(cells)?;
            if heads_match(&first, &second) {
                return Ok(runs
                    .iter()
                    .filter(|run| !authorized_transaction_ids.contains(&run.transaction_id))
                    .map(cell_wal_run_identity)
                    .collect());
            }
        }
        Err(BorsukError::ConcurrentModification {
            path: "cell WAL unauthorized-run snapshot".to_string(),
        })
    }

    pub(crate) fn retained_consumed_objects(
        &self,
        consumed_run_identities: &BTreeSet<String>,
    ) -> Result<(BTreeSet<String>, Vec<CommittedCellWalTransaction>)> {
        let transaction_ids = consumed_run_identities
            .iter()
            .map(|identity| {
                identity
                    .split_once(':')
                    .map(|(transaction_id, _)| transaction_id.to_string())
            })
            .collect::<Option<BTreeSet<_>>>()
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "consumed cell WAL run identity is missing its transaction prefix".to_string(),
                )
            })?;
        let mut paths = BTreeSet::new();
        let mut transactions = Vec::with_capacity(transaction_ids.len());
        for transaction_id in transaction_ids {
            let transaction = self
                .load_committed_transaction(&transaction_id)?
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "retained manifest references missing cell WAL transaction `{transaction_id}`"
                    ))
                })?;
            paths.insert(transaction.descriptor_path.clone());
            paths.insert(commit_marker_path(&transaction_id));
            paths.insert(transaction_state_path(&transaction_id));
            paths.extend(
                transaction
                    .runs
                    .iter()
                    .filter(|run| consumed_run_identities.contains(&cell_wal_run_identity(run)))
                    .map(|run| run.path.clone()),
            );
            transactions.push(transaction);
        }
        Ok((paths, transactions))
    }

    pub(crate) fn object_paths_detached_by_pruning(
        &self,
        cells: &[LogicalCellId],
        consumed_run_identities: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>> {
        const MAX_SNAPSHOT_ATTEMPTS: usize = 32;
        if consumed_run_identities.is_empty() {
            return Ok(BTreeSet::new());
        }
        for _ in 0..MAX_SNAPSHOT_ATTEMPTS {
            let first = self.collect_heads(cells)?;
            let mut paths = BTreeSet::new();
            let mut transaction_ids = BTreeSet::new();
            for (_, head) in &first {
                let mut next = head.as_ref().and_then(|head| head.node.clone());
                let mut lane_frontier_paths = BTreeSet::new();
                let mut lane_contains_consumed_run = false;
                let mut visited = HashSet::new();
                while let Some(reference) = next {
                    if !visited.insert(reference.checksum.clone()) {
                        return Err(BorsukError::InvalidStorage(
                            "cell WAL frontier contains a cycle".to_string(),
                        ));
                    }
                    lane_frontier_paths.insert(reference.path.clone());
                    let read = self.storage.read_bytes_with_cache_status_and_checksum(
                        &reference.path,
                        &reference.checksum,
                    )?;
                    let node = frontier_node_from_slice(&read.bytes, &reference.path)?;
                    if consumed_run_identities.contains(&cell_wal_run_identity(&node.run)) {
                        lane_contains_consumed_run = true;
                        paths.insert(node.run.path.clone());
                        transaction_ids.insert(node.run.transaction_id.clone());
                    }
                    next = node.previous;
                }
                if lane_contains_consumed_run {
                    // Pruning rebuilds the entire retained chain, so every old
                    // immutable frontier node in the lane becomes obsolete.
                    paths.extend(lane_frontier_paths);
                }
            }
            let second = self.collect_heads(cells)?;
            if !heads_match(&first, &second) {
                continue;
            }
            for transaction_id in transaction_ids {
                if let Some(transaction) = self.load_committed_transaction(&transaction_id)? {
                    paths.insert(transaction.descriptor_path);
                    paths.insert(commit_marker_path(&transaction_id));
                    paths.insert(transaction_state_path(&transaction_id));
                }
            }
            return Ok(paths);
        }
        Err(BorsukError::ConcurrentModification {
            path: "cell WAL prune simulation snapshot".to_string(),
        })
    }

    fn filter_committed_transactions(
        &self,
        prepared_runs: &[PreparedCellWalRun],
    ) -> Result<Vec<CommittedCellWalTransaction>> {
        let reachable = prepared_runs.iter().collect::<HashSet<_>>();
        let transaction_ids = prepared_runs
            .iter()
            .map(|run| run.transaction_id.clone())
            .collect::<BTreeSet<_>>();
        let mut committed = Vec::new();
        for transaction_id in transaction_ids {
            if let Some(transaction) = self.load_committed_transaction(&transaction_id)?
                && transaction.runs.iter().all(|run| reachable.contains(run))
            {
                committed.push(transaction);
            }
        }
        Ok(committed)
    }

    fn load_committed_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<CommittedCellWalTransaction>> {
        let marker_path = commit_marker_path(transaction_id);
        let Some(marker) = self.storage.read_coordination_object(&marker_path)? else {
            return Ok(None);
        };
        let marker = commit_marker_from_slice(&marker.bytes, &marker_path)?;
        let descriptor = self.storage.read_bytes_with_cache_status_and_checksum(
            &marker.descriptor_path,
            &marker.descriptor_checksum,
        )?;
        let descriptor =
            transaction_descriptor_from_slice(&descriptor.bytes, &marker.descriptor_path)?;
        if descriptor.transaction_id != transaction_id {
            return Err(BorsukError::InvalidStorage(format!(
                "transaction descriptor `{}` belongs to `{}` instead of `{transaction_id}`",
                marker.descriptor_path, descriptor.transaction_id
            )));
        }
        Ok(Some(CommittedCellWalTransaction {
            transaction_id: descriptor.transaction_id,
            descriptor_path: marker.descriptor_path,
            descriptor_checksum: marker.descriptor_checksum,
            runs: descriptor.runs,
            metadata: descriptor.metadata,
        }))
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn transaction_state_path(transaction_id: &str) -> String {
    format!("transactions/{transaction_id}/STATE")
}

fn claim_page_path(page: u8) -> String {
    format!("id-directory/claim-pages/{page:02}/STATE")
}

pub(crate) fn id_claim_shard(id: &[u8]) -> u16 {
    let digest = blake3::hash(id);
    u16::from_le_bytes([digest.as_bytes()[0], digest.as_bytes()[1]]) % CELL_WAL_CLAIM_SHARDS
}

pub(crate) fn id_generation_shard(id: &[u8]) -> u8 {
    blake3::hash(id).as_bytes()[0] % CELL_WAL_GENERATION_SHARDS
}

fn ensure_prepared_transaction(storage: &Storage, transaction_id: &str) -> Result<()> {
    let path = transaction_state_path(transaction_id);
    let prepared = CellWalTransactionState::Prepared {
        expires_at_ms: now_unix_ms().saturating_add(CELL_WAL_TRANSACTION_TTL_MS),
    };
    match storage.try_create_coordination_object(&path, &transaction_state_bytes(&prepared)?)? {
        Some(_) => Ok(()),
        None => {
            let current = storage
                .read_coordination_object(&path)?
                .ok_or_else(|| BorsukError::ConcurrentModification { path: path.clone() })?;
            match transaction_state_from_slice(&current.bytes, &path)? {
                CellWalTransactionState::Prepared { .. }
                | CellWalTransactionState::Committing { .. }
                | CellWalTransactionState::Committed { .. } => Ok(()),
                CellWalTransactionState::Aborted => {
                    Err(BorsukError::ConcurrentModification { path })
                }
            }
        }
    }
}

fn abort_prepared_transaction(storage: &Storage, transaction_id: &str) -> Result<bool> {
    let path = transaction_state_path(transaction_id);
    let Some(current) = storage.read_coordination_object(&path)? else {
        return Ok(false);
    };
    if !matches!(
        transaction_state_from_slice(&current.bytes, &path)?,
        CellWalTransactionState::Prepared { .. }
    ) {
        return Ok(false);
    }
    match storage.write_coordination_object(
        &path,
        &transaction_state_bytes(&CellWalTransactionState::Aborted)?,
        Some(current.version),
    ) {
        Ok(_) => Ok(true),
        Err(BorsukError::ConcurrentModification { .. }) => Ok(false),
        Err(error) => Err(error),
    }
}

fn finish_committing_transaction(
    storage: &Storage,
    transaction_id: &str,
    state_path: &str,
    state_version: UpdateVersion,
    descriptor_path: String,
    descriptor_checksum: String,
) -> Result<()> {
    let marker = CellWalCommitMarker {
        descriptor_path: descriptor_path.clone(),
        descriptor_checksum: descriptor_checksum.clone(),
    };
    let marker_path = commit_marker_path(transaction_id);
    let marker_bytes = commit_marker_bytes(&marker)?;
    match storage.write_coordination_object(&marker_path, &marker_bytes, None) {
        Ok(_) => {}
        Err(BorsukError::ConcurrentModification { .. }) => {
            let existing = storage
                .read_coordination_object(&marker_path)?
                .ok_or_else(|| BorsukError::ConcurrentModification {
                    path: marker_path.clone(),
                })?;
            if existing.bytes != marker_bytes {
                return Err(BorsukError::InvalidStorage(format!(
                    "transaction `{transaction_id}` has a conflicting commit marker"
                )));
            }
        }
        Err(error) => return Err(error),
    }
    let committed = CellWalTransactionState::Committed {
        descriptor_path,
        descriptor_checksum,
    };
    let _ = storage.write_coordination_object(
        state_path,
        &transaction_state_bytes(&committed)?,
        Some(state_version),
    );
    Ok(())
}

fn reclaim_claim_owner(storage: &Storage, transaction_id: &str) -> Result<bool> {
    let state_path = transaction_state_path(transaction_id);
    let Some(current) = storage.read_coordination_object(&state_path)? else {
        return Err(BorsukError::InvalidStorage(format!(
            "claim owner `{transaction_id}` has no transaction state"
        )));
    };
    match transaction_state_from_slice(&current.bytes, &state_path)? {
        CellWalTransactionState::Prepared { expires_at_ms } if now_unix_ms() <= expires_at_ms => {
            Ok(false)
        }
        CellWalTransactionState::Prepared { .. } => {
            match storage.write_coordination_object(
                &state_path,
                &transaction_state_bytes(&CellWalTransactionState::Aborted)?,
                Some(current.version),
            ) {
                Ok(_) => Ok(true),
                Err(BorsukError::ConcurrentModification { .. }) => Ok(false),
                Err(error) => Err(error),
            }
        }
        CellWalTransactionState::Committing {
            descriptor_path,
            descriptor_checksum,
        } => {
            finish_committing_transaction(
                storage,
                transaction_id,
                &state_path,
                current.version,
                descriptor_path,
                descriptor_checksum,
            )?;
            Ok(true)
        }
        CellWalTransactionState::Committed { .. } | CellWalTransactionState::Aborted => Ok(true),
    }
}

enum ClaimAcquireAttempt {
    Acquired(CellWalHeldClaim),
    Contended,
}

fn try_acquire_claim_page(
    storage: &Storage,
    transaction_id: &str,
    page_index: u8,
    path: &str,
    shards: &[u16],
) -> Result<ClaimAcquireAttempt> {
    let current = storage.read_coordination_object(path)?;
    let (mut page, expected) = match current {
        Some(current) => (
            claim_page_from_slice(&current.bytes, path, page_index)?,
            Some(current.version),
        ),
        None => (CellWalClaimPage::default(), None),
    };
    let mut previous_revisions = Vec::with_capacity(shards.len());
    for &shard in shards {
        let previous = match page.slots.get(&shard) {
            None => None,
            Some(CellWalClaimLock::Available { revision }) => Some(revision.clone()),
            Some(CellWalClaimLock::Owned {
                transaction_id: owner,
            }) if owner == transaction_id => Some(owner.clone()),
            Some(CellWalClaimLock::Owned {
                transaction_id: owner,
            }) => {
                if !reclaim_claim_owner(storage, owner)? {
                    return Ok(ClaimAcquireAttempt::Contended);
                }
                Some(owner.clone())
            }
        };
        previous_revisions.push((shard, previous));
        page.slots.insert(
            shard,
            CellWalClaimLock::Owned {
                transaction_id: transaction_id.to_string(),
            },
        );
    }
    let bytes = claim_page_bytes(&page)?;
    let write = match expected {
        Some(version) => storage.write_coordination_object(path, &bytes, Some(version)),
        None => storage
            .try_create_coordination_object(path, &bytes)?
            .ok_or_else(|| BorsukError::ConcurrentModification {
                path: path.to_string(),
            }),
    };
    match write {
        Ok(owned_version) => Ok(ClaimAcquireAttempt::Acquired(CellWalHeldClaim {
            path: path.to_string(),
            previous_revisions,
            owned_version,
            owned_page: page,
        })),
        Err(BorsukError::ConcurrentModification { .. }) => Ok(ClaimAcquireAttempt::Contended),
        Err(error) => Err(error),
    }
}

fn release_claim_page(
    storage: &Storage,
    revision: &str,
    claim: CellWalHeldClaim,
) -> Result<CellWalClaimCheckpoint> {
    const MAX_ATTEMPTS: usize = 128;
    let page_index = u8::try_from(claim.previous_revisions[0].0 / CELL_WAL_CLAIM_PAGE_SLOTS)
        .expect("claim page index fits u8");
    let checkpoint = || {
        claim
            .previous_revisions
            .iter()
            .map(|(shard, _)| (*shard, revision.to_string()))
            .collect()
    };
    let release_slots = |page: &mut CellWalClaimPage| -> Result<bool> {
        for &(shard, _) in &claim.previous_revisions {
            match page.slots.get(&shard) {
                Some(CellWalClaimLock::Owned { transaction_id }) if transaction_id == revision => {}
                _ => return Ok(false),
            }
            page.slots.insert(
                shard,
                CellWalClaimLock::Available {
                    revision: revision.to_string(),
                },
            );
        }
        Ok(true)
    };

    // The uncontended path needs no read: the acquired page is the exact CAS
    // predecessor. If another writer updated an unrelated slot on this packed
    // page, fall back to a read/merge CAS loop so that update is preserved.
    let mut released_page = claim.owned_page.clone();
    if !release_slots(&mut released_page)? {
        return Ok(CellWalClaimCheckpoint::new());
    }
    match storage.write_coordination_object(
        &claim.path,
        &claim_page_bytes(&released_page)?,
        Some(claim.owned_version.clone()),
    ) {
        Ok(_) => return Ok(checkpoint()),
        Err(BorsukError::ConcurrentModification { .. }) => {}
        Err(error) => return Err(error),
    }

    for _ in 0..MAX_ATTEMPTS {
        let current = storage
            .read_coordination_object(&claim.path)?
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!("claim page `{}` disappeared", claim.path))
            })?;
        let mut page = claim_page_from_slice(&current.bytes, &claim.path, page_index)?;
        if !release_slots(&mut page)? {
            return Ok(CellWalClaimCheckpoint::new());
        }
        match storage.write_coordination_object(
            &claim.path,
            &claim_page_bytes(&page)?,
            Some(current.version),
        ) {
            Ok(_) => return Ok(checkpoint()),
            Err(BorsukError::ConcurrentModification { .. }) => continue,
            Err(error) => return Err(error),
        }
    }
    Err(BorsukError::ConcurrentModification { path: claim.path })
}

fn release_claims(
    storage: &Storage,
    revision: &str,
    claims: Vec<CellWalHeldClaim>,
) -> CellWalClaimCheckpoint {
    crate::parallel::install_io(|| {
        claims
            .into_par_iter()
            .filter_map(|claim| release_claim_page(storage, revision, claim).ok())
            .flatten()
            .collect()
    })
}

fn claim_retry_delay(transaction_id: &str, attempt: usize) -> std::time::Duration {
    let digest = blake3::hash(format!("{transaction_id}:{attempt}").as_bytes());
    std::time::Duration::from_millis(1 + u64::from(digest.as_bytes()[0] % 10))
}

fn acquire_claim_shards(
    storage: &Storage,
    transaction_id: &str,
    shards: &BTreeSet<u16>,
) -> Result<Vec<CellWalHeldClaim>> {
    const MAX_ATTEMPTS: usize = 10_000;
    let pages = shards
        .iter()
        .fold(BTreeMap::<u8, Vec<u16>>::new(), |mut pages, &shard| {
            let page =
                u8::try_from(shard / CELL_WAL_CLAIM_PAGE_SLOTS).expect("claim page index fits u8");
            pages.entry(page).or_default().push(shard);
            pages
        });
    let mut last_contended_path = pages
        .keys()
        .next()
        .map(|&page| claim_page_path(page))
        .unwrap_or_else(|| "id-directory/claim-pages".to_string());
    for attempt in 0..MAX_ATTEMPTS {
        let mut acquired = Vec::with_capacity(pages.len());
        let mut contended = false;
        for (&page, shards) in &pages {
            let path = claim_page_path(page);
            match try_acquire_claim_page(storage, transaction_id, page, &path, shards) {
                Ok(ClaimAcquireAttempt::Acquired(claim)) => acquired.push(claim),
                Ok(ClaimAcquireAttempt::Contended) => {
                    last_contended_path = path;
                    contended = true;
                    break;
                }
                Err(error) => {
                    let _ = release_claims(storage, transaction_id, acquired);
                    return Err(error);
                }
            }
        }
        if !contended {
            return Ok(acquired);
        }
        let _ = release_claims(storage, transaction_id, acquired);
        std::thread::sleep(claim_retry_delay(transaction_id, attempt));
    }
    Err(BorsukError::ConcurrentModification {
        path: last_contended_path,
    })
}

fn heads_match(
    left: &[(String, Option<CellWalLaneHead>)],
    right: &[(String, Option<CellWalLaneHead>)],
) -> bool {
    left == right
}

fn commit_marker_path(transaction_id: &str) -> String {
    format!("transactions/{transaction_id}/COMMIT")
}

fn cell_wal_run_identity(run: &PreparedCellWalRun) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        run.transaction_id, run.cell.routing_epoch, run.cell.cell_ordinal, run.lane, run.checksum
    )
}

fn validate_transaction_id(transaction_id: &str) -> Result<()> {
    if transaction_id.is_empty()
        || !transaction_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(BorsukError::InvalidStorage(
            "cell WAL transaction id must contain only ASCII letters, digits, '-' or '_'"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_cell_wal_run_extension(kind: CellWalRunKind, extension: &str) -> Result<()> {
    let valid = match kind {
        CellWalRunKind::Records => matches!(extension, "parquet" | "vortex"),
        CellWalRunKind::Tombstones => extension == "parquet",
        CellWalRunKind::IdDirectory => extension == "bin",
    };
    if !valid {
        let expected = match kind {
            CellWalRunKind::Records => "parquet or vortex",
            CellWalRunKind::Tombstones => "parquet",
            CellWalRunKind::IdDirectory => "bin",
        };
        return Err(BorsukError::InvalidStorage(format!(
            "cell WAL {} run extension must be `{expected}`, got `{extension}`",
            kind.as_str()
        )));
    }
    Ok(())
}

struct PackedWalWriter {
    bytes: Vec<u8>,
}

impl PackedWalWriter {
    fn new(magic: &[u8; 4]) -> Self {
        let mut bytes = Vec::with_capacity(128);
        bytes.extend_from_slice(magic);
        bytes.push(CELL_WAL_CODEC_VERSION);
        Self { bytes }
    }

    fn write_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn write_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn write_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn write_bytes(&mut self, value: &[u8], label: &str) -> Result<()> {
        let length = u32::try_from(value.len()).map_err(|_| {
            BorsukError::InvalidStorage(format!("cell WAL {label} exceeds u32 bytes"))
        })?;
        self.write_u32(length);
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn write_string(&mut self, value: &str, label: &str) -> Result<()> {
        self.write_bytes(value.as_bytes(), label)
    }

    fn finish(mut self) -> Vec<u8> {
        let checksum = blake3::hash(&self.bytes);
        self.bytes.extend_from_slice(checksum.as_bytes());
        self.bytes
    }
}

struct PackedWalReader<'a> {
    payload: &'a [u8],
    cursor: usize,
    path: String,
}

impl<'a> PackedWalReader<'a> {
    fn new(bytes: &'a [u8], magic: &[u8; 4], path: &str) -> Result<Self> {
        if bytes.len() < magic.len() + 1 + CELL_WAL_CHECKSUM_LEN {
            return Err(BorsukError::InvalidStorage(format!(
                "packed cell WAL object `{path}` is truncated"
            )));
        }
        let payload_len = bytes.len() - CELL_WAL_CHECKSUM_LEN;
        let (payload, stored_checksum) = bytes.split_at(payload_len);
        if stored_checksum != blake3::hash(payload).as_bytes() {
            return Err(BorsukError::InvalidStorage(format!(
                "packed cell WAL object `{path}` checksum mismatch"
            )));
        }
        if payload.get(..magic.len()) != Some(magic.as_slice()) {
            return Err(BorsukError::InvalidStorage(format!(
                "packed cell WAL object `{path}` has invalid magic"
            )));
        }
        let codec_version = payload[magic.len()];
        if codec_version != CELL_WAL_CODEC_VERSION {
            return Err(BorsukError::InvalidStorage(format!(
                "packed cell WAL object `{path}` uses unsupported codec version {codec_version}"
            )));
        }
        Ok(Self {
            payload,
            cursor: magic.len() + 1,
            path: path.to_string(),
        })
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.cursor.checked_add(length).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "packed cell WAL object `{}` length overflows usize",
                self.path
            ))
        })?;
        let value = self.payload.get(self.cursor..end).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "packed cell WAL object `{}` is truncated",
                self.path
            ))
        })?;
        self.cursor = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .expect("packed WAL reader returned four bytes");
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .expect("packed WAL reader returned eight bytes");
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_bytes(&mut self, label: &str) -> Result<Vec<u8>> {
        let length = self.read_u32()? as usize;
        let value = self.take(length).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "packed cell WAL {label} in `{}` is invalid: {error}",
                self.path
            ))
        })?;
        Ok(value.to_vec())
    }

    fn read_string(&mut self, label: &str) -> Result<String> {
        let value = self.read_bytes(label)?;
        String::from_utf8(value).map_err(|_| {
            BorsukError::InvalidStorage(format!(
                "packed cell WAL {label} in `{}` is not valid UTF-8",
                self.path
            ))
        })
    }

    fn finish(self) -> Result<()> {
        if self.cursor != self.payload.len() {
            return Err(BorsukError::InvalidStorage(format!(
                "packed cell WAL object `{}` contains trailing bytes",
                self.path
            )));
        }
        Ok(())
    }
}

fn write_frontier_ref(writer: &mut PackedWalWriter, reference: &CellWalFrontierRef) -> Result<()> {
    writer.write_string(&reference.path, "frontier path")?;
    writer.write_string(&reference.checksum, "frontier checksum")
}

fn read_frontier_ref(reader: &mut PackedWalReader<'_>) -> Result<CellWalFrontierRef> {
    let reference = CellWalFrontierRef {
        path: reader.read_string("frontier path")?,
        checksum: reader.read_string("frontier checksum")?,
    };
    validate_cell_wal_checksum(&reference.checksum, "frontier", &reader.path)?;
    Ok(reference)
}

fn write_prepared_run(writer: &mut PackedWalWriter, run: &PreparedCellWalRun) -> Result<()> {
    validate_transaction_id(&run.transaction_id)?;
    writer.write_string(&run.transaction_id, "transaction id")?;
    writer.write_u64(run.cell.routing_epoch);
    writer.write_u32(run.cell.cell_ordinal);
    writer.write_u8(run.lane);
    writer.write_u8(match run.kind {
        CellWalRunKind::Records => 0,
        CellWalRunKind::Tombstones => 1,
        CellWalRunKind::IdDirectory => 2,
    });
    writer.write_bytes(&run.metadata, "run metadata")?;
    writer.write_string(&run.path, "run path")?;
    writer.write_string(&run.checksum, "run checksum")?;
    writer.write_u64(u64::try_from(run.record_count).map_err(|_| {
        BorsukError::InvalidStorage("cell WAL record count exceeds u64".to_string())
    })?);
    writer.write_u64(run.byte_len);
    Ok(())
}

fn read_prepared_run(reader: &mut PackedWalReader<'_>) -> Result<PreparedCellWalRun> {
    let transaction_id = reader.read_string("transaction id")?;
    validate_transaction_id(&transaction_id)?;
    let cell = LogicalCellId::new(reader.read_u64()?, reader.read_u32()?);
    let lane = reader.read_u8()?;
    if lane >= MAX_CELL_WAL_LANES {
        return Err(BorsukError::InvalidStorage(format!(
            "packed cell WAL run in `{}` has invalid lane {lane}",
            reader.path
        )));
    }
    let kind = match reader.read_u8()? {
        0 => CellWalRunKind::Records,
        1 => CellWalRunKind::Tombstones,
        2 => CellWalRunKind::IdDirectory,
        value => {
            return Err(BorsukError::InvalidStorage(format!(
                "packed cell WAL run in `{}` has invalid kind {value}",
                reader.path
            )));
        }
    };
    let metadata = reader.read_bytes("run metadata")?;
    let path = reader.read_string("run path")?;
    let checksum = reader.read_string("run checksum")?;
    validate_cell_wal_checksum(&checksum, "run", &reader.path)?;
    let record_count = usize::try_from(reader.read_u64()?).map_err(|_| {
        BorsukError::InvalidStorage(format!(
            "packed cell WAL run in `{}` has a record count that exceeds usize",
            reader.path
        ))
    })?;
    let byte_len = reader.read_u64()?;
    Ok(PreparedCellWalRun {
        transaction_id,
        cell,
        lane,
        kind,
        metadata,
        path,
        checksum,
        record_count,
        byte_len,
    })
}

fn lane_head_bytes(head: &CellWalLaneHead) -> Result<Vec<u8>> {
    let mut writer = PackedWalWriter::new(CELL_WAL_HEAD_MAGIC);
    writer.write_u64(head.generation);
    match &head.node {
        Some(reference) => {
            writer.write_u8(1);
            write_frontier_ref(&mut writer, reference)?;
        }
        None => writer.write_u8(0),
    }
    Ok(writer.finish())
}

fn lane_head_from_slice(bytes: &[u8], path: &str) -> Result<CellWalLaneHead> {
    let mut reader = PackedWalReader::new(bytes, CELL_WAL_HEAD_MAGIC, path)?;
    let generation = reader.read_u64()?;
    let node = match reader.read_u8()? {
        0 => None,
        1 => Some(read_frontier_ref(&mut reader)?),
        value => {
            return Err(BorsukError::InvalidStorage(format!(
                "packed cell WAL head `{path}` has invalid option tag {value}"
            )));
        }
    };
    reader.finish()?;
    Ok(CellWalLaneHead { generation, node })
}

fn frontier_node_bytes(node: &CellWalFrontierNode) -> Result<Vec<u8>> {
    let mut writer = PackedWalWriter::new(CELL_WAL_NODE_MAGIC);
    write_prepared_run(&mut writer, &node.run)?;
    match &node.previous {
        Some(reference) => {
            writer.write_u8(1);
            write_frontier_ref(&mut writer, reference)?;
        }
        None => writer.write_u8(0),
    }
    Ok(writer.finish())
}

fn frontier_node_from_slice(bytes: &[u8], path: &str) -> Result<CellWalFrontierNode> {
    let mut reader = PackedWalReader::new(bytes, CELL_WAL_NODE_MAGIC, path)?;
    let run = read_prepared_run(&mut reader)?;
    let previous = match reader.read_u8()? {
        0 => None,
        1 => Some(read_frontier_ref(&mut reader)?),
        value => {
            return Err(BorsukError::InvalidStorage(format!(
                "packed cell WAL frontier `{path}` has invalid option tag {value}"
            )));
        }
    };
    reader.finish()?;
    Ok(CellWalFrontierNode { run, previous })
}

fn transaction_descriptor_bytes(descriptor: &CellWalTransactionDescriptor) -> Result<Vec<u8>> {
    validate_transaction_id(&descriptor.transaction_id)?;
    let mut writer = PackedWalWriter::new(CELL_WAL_DESCRIPTOR_MAGIC);
    writer.write_string(&descriptor.transaction_id, "transaction id")?;
    writer.write_u32(u32::try_from(descriptor.runs.len()).map_err(|_| {
        BorsukError::InvalidStorage("cell WAL transaction contains too many runs".to_string())
    })?);
    for run in &descriptor.runs {
        write_prepared_run(&mut writer, run)?;
    }
    writer.write_bytes(&descriptor.metadata, "transaction metadata")?;
    Ok(writer.finish())
}

fn transaction_descriptor_from_slice(
    bytes: &[u8],
    path: &str,
) -> Result<CellWalTransactionDescriptor> {
    let mut reader = PackedWalReader::new(bytes, CELL_WAL_DESCRIPTOR_MAGIC, path)?;
    let transaction_id = reader.read_string("transaction id")?;
    validate_transaction_id(&transaction_id)?;
    let run_count = reader.read_u32()? as usize;
    let mut runs = Vec::with_capacity(run_count.min(1_024));
    for _ in 0..run_count {
        runs.push(read_prepared_run(&mut reader)?);
    }
    let metadata = reader.read_bytes("transaction metadata")?;
    reader.finish()?;
    Ok(CellWalTransactionDescriptor {
        transaction_id,
        runs,
        metadata,
    })
}

fn commit_marker_bytes(marker: &CellWalCommitMarker) -> Result<Vec<u8>> {
    let mut writer = PackedWalWriter::new(CELL_WAL_COMMIT_MAGIC);
    writer.write_string(&marker.descriptor_path, "descriptor path")?;
    writer.write_string(&marker.descriptor_checksum, "descriptor checksum")?;
    Ok(writer.finish())
}

fn commit_marker_from_slice(bytes: &[u8], path: &str) -> Result<CellWalCommitMarker> {
    let mut reader = PackedWalReader::new(bytes, CELL_WAL_COMMIT_MAGIC, path)?;
    let marker = CellWalCommitMarker {
        descriptor_path: reader.read_string("descriptor path")?,
        descriptor_checksum: reader.read_string("descriptor checksum")?,
    };
    validate_cell_wal_checksum(&marker.descriptor_checksum, "descriptor", path)?;
    reader.finish()?;
    Ok(marker)
}

fn transaction_state_bytes(state: &CellWalTransactionState) -> Result<Vec<u8>> {
    let mut writer = PackedWalWriter::new(CELL_WAL_STATE_MAGIC);
    match state {
        CellWalTransactionState::Prepared { expires_at_ms } => {
            writer.write_u8(0);
            writer.write_u64(*expires_at_ms);
        }
        CellWalTransactionState::Committing {
            descriptor_path,
            descriptor_checksum,
        } => {
            writer.write_u8(1);
            writer.write_string(descriptor_path, "descriptor path")?;
            writer.write_string(descriptor_checksum, "descriptor checksum")?;
        }
        CellWalTransactionState::Committed {
            descriptor_path,
            descriptor_checksum,
        } => {
            writer.write_u8(2);
            writer.write_string(descriptor_path, "descriptor path")?;
            writer.write_string(descriptor_checksum, "descriptor checksum")?;
        }
        CellWalTransactionState::Aborted => writer.write_u8(3),
    }
    Ok(writer.finish())
}

fn transaction_state_from_slice(bytes: &[u8], path: &str) -> Result<CellWalTransactionState> {
    let mut reader = PackedWalReader::new(bytes, CELL_WAL_STATE_MAGIC, path)?;
    let state = match reader.read_u8()? {
        0 => CellWalTransactionState::Prepared {
            expires_at_ms: reader.read_u64()?,
        },
        tag @ (1 | 2) => {
            let descriptor_path = reader.read_string("descriptor path")?;
            let descriptor_checksum = reader.read_string("descriptor checksum")?;
            validate_cell_wal_checksum(&descriptor_checksum, "descriptor", path)?;
            if tag == 1 {
                CellWalTransactionState::Committing {
                    descriptor_path,
                    descriptor_checksum,
                }
            } else {
                CellWalTransactionState::Committed {
                    descriptor_path,
                    descriptor_checksum,
                }
            }
        }
        3 => CellWalTransactionState::Aborted,
        value => {
            return Err(BorsukError::InvalidStorage(format!(
                "packed cell WAL transaction state `{path}` has invalid tag {value}"
            )));
        }
    };
    reader.finish()?;
    Ok(state)
}

fn write_claim_lock(writer: &mut PackedWalWriter, lock: &CellWalClaimLock) -> Result<()> {
    match lock {
        CellWalClaimLock::Available { revision } => {
            validate_transaction_id(revision)?;
            writer.write_u8(0);
            writer.write_string(revision, "claim revision")?;
        }
        CellWalClaimLock::Owned { transaction_id } => {
            validate_transaction_id(transaction_id)?;
            writer.write_u8(1);
            writer.write_string(transaction_id, "claim transaction id")?;
        }
    }
    Ok(())
}

fn read_claim_lock(reader: &mut PackedWalReader<'_>) -> Result<CellWalClaimLock> {
    let lock = match reader.read_u8()? {
        0 => {
            let revision = reader.read_string("claim revision")?;
            validate_transaction_id(&revision)?;
            CellWalClaimLock::Available { revision }
        }
        1 => {
            let transaction_id = reader.read_string("claim transaction id")?;
            validate_transaction_id(&transaction_id)?;
            CellWalClaimLock::Owned { transaction_id }
        }
        value => {
            return Err(BorsukError::InvalidStorage(format!(
                "packed cell WAL claim lock in `{}` has invalid tag {value}",
                reader.path
            )));
        }
    };
    Ok(lock)
}

fn claim_page_bytes(page: &CellWalClaimPage) -> Result<Vec<u8>> {
    let mut writer = PackedWalWriter::new(CELL_WAL_CLAIM_MAGIC);
    writer.write_u32(u32::try_from(page.slots.len()).map_err(|_| {
        BorsukError::InvalidStorage("claim page slot count exceeds u32".to_string())
    })?);
    for (&shard, lock) in &page.slots {
        if shard >= CELL_WAL_CLAIM_SHARDS {
            return Err(BorsukError::InvalidStorage(format!(
                "claim shard {shard} exceeds {}",
                CELL_WAL_CLAIM_SHARDS - 1
            )));
        }
        writer.write_u32(u32::from(shard));
        write_claim_lock(&mut writer, lock)?;
    }
    Ok(writer.finish())
}

fn claim_page_from_slice(bytes: &[u8], path: &str, page: u8) -> Result<CellWalClaimPage> {
    let mut reader = PackedWalReader::new(bytes, CELL_WAL_CLAIM_MAGIC, path)?;
    let count = reader.read_u32()? as usize;
    if count > usize::from(CELL_WAL_CLAIM_PAGE_SLOTS) {
        return Err(BorsukError::InvalidStorage(format!(
            "claim page `{path}` contains {count} slots"
        )));
    }
    let mut slots = BTreeMap::new();
    for _ in 0..count {
        let shard = u16::try_from(reader.read_u32()?).map_err(|_| {
            BorsukError::InvalidStorage(format!("claim page `{path}` has an invalid shard"))
        })?;
        if shard / CELL_WAL_CLAIM_PAGE_SLOTS != u16::from(page)
            || slots.insert(shard, read_claim_lock(&mut reader)?).is_some()
        {
            return Err(BorsukError::InvalidStorage(format!(
                "claim page `{path}` has a misplaced or duplicate shard {shard}"
            )));
        }
    }
    reader.finish()?;
    Ok(CellWalClaimPage { slots })
}

fn validate_cell_wal_checksum(checksum: &str, label: &str, path: &str) -> Result<()> {
    if checksum.len() != 64 || !checksum.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(BorsukError::InvalidStorage(format!(
            "packed cell WAL {label} checksum in `{path}` is invalid"
        )));
    }
    Ok(())
}

/// Derive a filesystem-safe stable transaction ID from an idempotency key.
pub fn cell_wal_transaction_id(idempotency_key: &[u8]) -> Result<String> {
    if idempotency_key.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "cell WAL idempotency key must not be empty".to_string(),
        ));
    }
    Ok(blake3::hash(idempotency_key).to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run() -> PreparedCellWalRun {
        PreparedCellWalRun {
            transaction_id: "transaction-1".to_string(),
            cell: LogicalCellId::new(7, 9),
            lane: 3,
            kind: CellWalRunKind::IdDirectory,
            metadata: vec![1, 2, 3],
            path: format!("cells/7/9/wal/3/runs/id-directory/{}.bin", "ab".repeat(32)),
            checksum: "ab".repeat(32),
            record_count: 5,
            byte_len: 123,
        }
    }

    #[test]
    fn packed_control_records_round_trip_with_distinct_magic() {
        let reference = CellWalFrontierRef {
            path: format!("cells/7/9/wal/3/frontier/{}.bin", "cd".repeat(32)),
            checksum: "cd".repeat(32),
        };
        let head = CellWalLaneHead {
            generation: 11,
            node: Some(reference.clone()),
        };
        let node = CellWalFrontierNode {
            run: run(),
            previous: Some(reference),
        };
        let descriptor = CellWalTransactionDescriptor {
            transaction_id: "transaction-1".to_string(),
            runs: vec![run()],
            metadata: vec![4, 5, 6],
        };
        let marker = CellWalCommitMarker {
            descriptor_path: format!(
                "transactions/transaction-1/descriptors/{}.bin",
                "ef".repeat(32)
            ),
            descriptor_checksum: "ef".repeat(32),
        };

        let head_bytes = lane_head_bytes(&head).unwrap();
        let node_bytes = frontier_node_bytes(&node).unwrap();
        let descriptor_bytes = transaction_descriptor_bytes(&descriptor).unwrap();
        let marker_bytes = commit_marker_bytes(&marker).unwrap();
        assert!(head_bytes.starts_with(CELL_WAL_HEAD_MAGIC));
        assert!(node_bytes.starts_with(CELL_WAL_NODE_MAGIC));
        assert!(descriptor_bytes.starts_with(CELL_WAL_DESCRIPTOR_MAGIC));
        assert!(marker_bytes.starts_with(CELL_WAL_COMMIT_MAGIC));
        assert_eq!(lane_head_from_slice(&head_bytes, "HEAD").unwrap(), head);
        assert_eq!(frontier_node_from_slice(&node_bytes, "node").unwrap(), node);
        assert_eq!(
            transaction_descriptor_from_slice(&descriptor_bytes, "descriptor").unwrap(),
            descriptor
        );
        assert_eq!(
            commit_marker_from_slice(&marker_bytes, "COMMIT").unwrap(),
            marker
        );
    }

    #[test]
    fn packed_control_records_reject_corruption() {
        let mut bytes = lane_head_bytes(&CellWalLaneHead {
            generation: 1,
            node: None,
        })
        .unwrap();
        bytes[5] ^= 1;

        let error = lane_head_from_slice(&bytes, "HEAD").unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"), "{error}");
    }

    #[test]
    fn fenced_transaction_states_and_claim_locks_round_trip() {
        let descriptor_path = format!(
            "transactions/transaction-1/descriptors/{}.bin",
            "ef".repeat(32)
        );
        let states = [
            CellWalTransactionState::Prepared {
                expires_at_ms: 123_456,
            },
            CellWalTransactionState::Committing {
                descriptor_path: descriptor_path.clone(),
                descriptor_checksum: "ef".repeat(32),
            },
            CellWalTransactionState::Committed {
                descriptor_path,
                descriptor_checksum: "ef".repeat(32),
            },
            CellWalTransactionState::Aborted,
        ];
        for state in states {
            let bytes = transaction_state_bytes(&state).unwrap();
            assert!(bytes.starts_with(CELL_WAL_STATE_MAGIC));
            assert_eq!(
                transaction_state_from_slice(&bytes, "STATE").unwrap(),
                state
            );
        }

        let page = CellWalClaimPage {
            slots: BTreeMap::from([
                (
                    0,
                    CellWalClaimLock::Available {
                        revision: "transaction-0".to_string(),
                    },
                ),
                (
                    1,
                    CellWalClaimLock::Owned {
                        transaction_id: "transaction-1".to_string(),
                    },
                ),
            ]),
        };
        let bytes = claim_page_bytes(&page).unwrap();
        assert!(bytes.starts_with(CELL_WAL_CLAIM_MAGIC));
        assert_eq!(claim_page_from_slice(&bytes, "STATE", 0).unwrap(), page);
    }

    #[test]
    fn available_claim_revisions_change_the_persisted_lock_version() {
        let page = |revision: &str| CellWalClaimPage {
            slots: BTreeMap::from([(
                0,
                CellWalClaimLock::Available {
                    revision: revision.to_string(),
                },
            )]),
        };
        let first = claim_page_bytes(&page("transaction-1")).unwrap();
        let second = claim_page_bytes(&page("transaction-2")).unwrap();

        assert_ne!(first, second);
    }

    #[test]
    fn claim_shards_use_all_twelve_digest_bits() {
        let shards = (0_u32..100_000)
            .map(|value| id_claim_shard(&value.to_le_bytes()))
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(shards.len(), usize::from(CELL_WAL_CLAIM_SHARDS));
        assert_eq!(shards.first(), Some(&0));
        assert_eq!(shards.last(), Some(&(CELL_WAL_CLAIM_SHARDS - 1)));
    }

    #[test]
    fn refreshed_claims_synchronize_the_writer_checkpoint() {
        let store = CellWalStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            "memory:///claim-checkpoint-synchronization",
            CellWalConfig::default(),
            b"writer".to_vec(),
        )
        .unwrap();
        let mut first = store
            .claim_ids("first-transaction", [b"stable-id".as_slice()])
            .unwrap();
        let _discarded_cold_writer_checkpoint = first.finish();
        let mut second = store
            .claim_ids("second-transaction", [b"stable-id".as_slice()])
            .unwrap();
        let mut reopened_writer_checkpoint = CellWalClaimCheckpoint::new();

        assert!(!second.matches_checkpoint(&reopened_writer_checkpoint));
        reopened_writer_checkpoint = second.synchronized_checkpoint().unwrap();
        assert!(second.matches_checkpoint(&reopened_writer_checkpoint));

        second.finish();
    }

    #[test]
    fn one_lane_group_publishes_one_head_update() {
        let store = CellWalStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            "memory:///grouped-lane-publication",
            CellWalConfig::default(),
            b"writer".to_vec(),
        )
        .unwrap();
        let cell = LogicalCellId::new(1, 0);
        let inputs = [
            b"first".as_slice(),
            b"second".as_slice(),
            b"third".as_slice(),
        ]
        .map(|bytes| CellWalRunInput {
            cell,
            kind: CellWalRunKind::Records,
            metadata: Vec::new(),
            bytes: bytes.to_vec(),
            record_count: 1,
            extension: "parquet".to_string(),
        });
        let before = store.storage.request_counts();

        store
            .prepare_transaction("grouped-publication", &inputs)
            .unwrap();

        let requests = store.storage.request_counts().delta(&before);
        assert_eq!(
            requests.puts, 9,
            "three immutable payloads and frontier nodes need one state, one HEAD, and one descriptor PUT: {requests:?}"
        );
    }

    #[test]
    fn root_authorization_filter_identifies_and_detaches_crash_orphans() {
        let store = CellWalStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            "memory:///root-authorization-prune",
            CellWalConfig::default(),
            b"writer".to_vec(),
        )
        .unwrap();
        let cell = LogicalCellId::new(3, 4);
        store
            .commit(
                "orphaned-before-root-cas",
                &[CellWalRunInput {
                    cell,
                    kind: CellWalRunKind::Records,
                    metadata: Vec::new(),
                    bytes: b"orphan".to_vec(),
                    record_count: 1,
                    extension: "parquet".to_string(),
                }],
            )
            .unwrap();

        let unrooted = store
            .run_identities_without_root_authorization(&[cell], &BTreeSet::new())
            .unwrap();
        assert_eq!(unrooted.len(), 1);
        store.prune_consumed_runs(&[cell], &unrooted).unwrap();
        assert!(
            store
                .run_identities_without_root_authorization(&[cell], &BTreeSet::new())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn fenced_control_records_reject_corruption_and_trailing_bytes() {
        let mut corrupted = transaction_state_bytes(&CellWalTransactionState::Prepared {
            expires_at_ms: 123_456,
        })
        .unwrap();
        corrupted[5] ^= 1;
        assert!(
            transaction_state_from_slice(&corrupted, "STATE")
                .unwrap_err()
                .to_string()
                .contains("checksum mismatch")
        );

        let mut trailing = claim_page_bytes(&CellWalClaimPage {
            slots: BTreeMap::from([(
                0,
                CellWalClaimLock::Available {
                    revision: "transaction-1".to_string(),
                },
            )]),
        })
        .unwrap();
        let checksum = trailing.split_off(trailing.len() - CELL_WAL_CHECKSUM_LEN);
        trailing.push(0);
        let replacement = blake3::hash(&trailing);
        trailing.extend_from_slice(replacement.as_bytes());
        assert_ne!(checksum, trailing[trailing.len() - CELL_WAL_CHECKSUM_LEN..]);
        assert!(
            claim_page_from_slice(&trailing, "STATE", 0)
                .unwrap_err()
                .to_string()
                .contains("trailing bytes")
        );
    }
}
