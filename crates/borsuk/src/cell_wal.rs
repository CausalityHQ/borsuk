//! Stable logical-cell identities and cell-local WAL lane layout.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use object_store::UpdateVersion;
use rayon::prelude::*;

use crate::positioned_log::{
    authorized_transaction_receipt, validate_claim_authorization_envelope,
};
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
pub(crate) struct CellWalObjectPaths {
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
pub(crate) struct PreparedCellWalRun {
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
pub(crate) struct CellWalFrontierRef {
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
pub(crate) struct CellWalTransactionDescriptor {
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

/// Reference returned after an atomic cell-WAL commit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CommittedCellWalTransaction {
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
pub(crate) struct CellWalRunInput {
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
pub(crate) struct CellWalStore {
    storage: Storage,
    config: CellWalConfig,
    source_epoch: u64,
}

pub(crate) struct CellWalClaimGuard {
    storage: Storage,
    transaction_id: String,
    locks: Vec<CellWalHeldClaim>,
    transaction_committed: bool,
    source_epoch: u64,
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
        let mut checkpoint = CellWalClaimCheckpoint::new();
        let mut owner_revisions = BTreeMap::<String, Option<String>>::new();
        for (shard, lock) in pages
            .into_iter()
            .flatten()
            .flat_map(|page| page.slots.into_iter())
        {
            match lock {
                CellWalClaimLock::Available { revision } => {
                    checkpoint.insert(shard, revision);
                }
                CellWalClaimLock::Owned { transaction_id } => {
                    let revision = if let Some(revision) = owner_revisions.get(&transaction_id) {
                        revision.clone()
                    } else {
                        let revision =
                            reclaim_claim_owner(&self.storage, self.source_epoch, &transaction_id)?;
                        owner_revisions.insert(transaction_id.clone(), revision.clone());
                        revision
                    };
                    if let Some(revision) = revision {
                        checkpoint.insert(shard, revision);
                    }
                }
            }
        }

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

    /// Release exact-ID claims with the checksum of the positioned root that
    /// authorizes the mutation as the durable revision.
    pub(crate) fn finish_authorized(
        &mut self,
        source_epoch: u64,
        shard: u8,
        sequence: u64,
        positioned_envelope_checksum: &str,
    ) -> Result<CellWalClaimCheckpoint> {
        // The positioned head CAS is irreversible: from this point Drop must
        // never abort STATE or restore the predecessor revisions.
        self.transaction_committed = true;
        if source_epoch != self.source_epoch {
            return Err(BorsukError::InvalidStorage(format!(
                "positioned claim source epoch {source_epoch} differs from guard epoch {}",
                self.source_epoch
            )));
        }
        let claims = self.locks.drain(..).collect::<Vec<_>>();
        let expected = claims
            .iter()
            .flat_map(|claim| claim.previous_revisions.iter().map(|(shard, _)| *shard))
            .collect::<BTreeSet<_>>();
        let released = release_claims(
            &self.storage,
            &self.transaction_id,
            positioned_envelope_checksum,
            claims,
        );
        if released.len() == expected.len() {
            return Ok(released);
        }
        write_claim_authorization_receipt(
            &self.storage,
            source_epoch,
            shard,
            sequence,
            &self.transaction_id,
            positioned_envelope_checksum,
        )?;
        Ok(expected
            .into_iter()
            .map(|shard| (shard, positioned_envelope_checksum.to_string()))
            .collect())
    }

    fn release(&mut self) -> CellWalClaimCheckpoint {
        release_claims(
            &self.storage,
            &self.transaction_id,
            &self.transaction_id,
            self.locks.drain(..).collect(),
        )
    }
}

impl Drop for CellWalClaimGuard {
    fn drop(&mut self) {
        if !self.transaction_committed {
            let _ = abort_prepared_transaction(&self.storage, &self.transaction_id);
            let _ = restore_claims(
                &self.storage,
                &self.transaction_id,
                self.locks.drain(..).collect(),
            );
            return;
        }
        let _ = self.release();
    }
}

impl CellWalStore {
    pub(crate) fn from_storage(
        storage: Storage,
        config: CellWalConfig,
        source_epoch: u64,
    ) -> Result<Self> {
        config.validate()?;
        if source_epoch == 0 {
            return Err(BorsukError::InvalidStorage(
                "cell WAL positioned source epoch must be positive".to_string(),
            ));
        }
        Ok(Self {
            storage,
            config,
            source_epoch,
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
            source_epoch: self.source_epoch,
        };
        guard.locks =
            acquire_claim_shards(&self.storage, self.source_epoch, transaction_id, &shards)?;
        Ok(guard)
    }

    pub(crate) fn live_staging_transaction_ids(&self) -> Result<BTreeSet<String>> {
        let store_now = self.storage.store_clock_now()?;
        let ttl = chrono::TimeDelta::milliseconds(
            i64::try_from(CELL_WAL_TRANSACTION_TTL_MS).unwrap_or(i64::MAX),
        );
        let mut transaction_ids = BTreeSet::new();
        for object in self.storage.list_objects("transactions")? {
            let Some(transaction_id) = object
                .path
                .strip_prefix("transactions/")
                .and_then(|path| path.strip_suffix("/STATE"))
            else {
                continue;
            };
            let Some(state) = self.storage.read_coordination_object(&object.path)? else {
                continue;
            };
            if matches!(
                transaction_state_from_slice(&state.bytes, &object.path)?,
                CellWalTransactionState::Prepared { .. }
            ) && object
                .last_modified
                .checked_add_signed(ttl)
                .is_some_and(|expires_at| expires_at >= store_now)
            {
                transaction_ids.insert(transaction_id.to_string());
            }
        }
        Ok(transaction_ids)
    }

    /// Publish prepared lane entries and their immutable descriptor without
    /// making any run visible to readers.
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
            let expected_runs = consumed_run_identities
                .iter()
                .filter(|identity| {
                    identity
                        .split_once(':')
                        .is_some_and(|(candidate, _)| candidate == transaction_id)
                })
                .cloned()
                .collect::<BTreeSet<_>>();
            let transaction = self
                .load_retained_transaction(&transaction_id, &expected_runs)?
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

    /// Resolve immutable transaction metadata authorized by a retained
    /// manifest. Collection-root publication is the visibility authority and
    /// its frontier entry may already be compacted, so GC is allowed to recover
    /// the content-addressed descriptor only when exactly one descriptor
    /// contains every run identity named by that retained manifest.
    fn load_retained_transaction(
        &self,
        transaction_id: &str,
        expected_run_identities: &BTreeSet<String>,
    ) -> Result<Option<CommittedCellWalTransaction>> {
        if let Some(transaction) = self.load_committed_transaction(transaction_id)? {
            return Ok(Some(transaction));
        }
        let prefix = format!("transactions/{transaction_id}/descriptors/");
        let mut matches = Vec::new();
        for object in self.storage.list_objects(&prefix)? {
            let Some(checksum) = object
                .path
                .strip_prefix(&prefix)
                .and_then(|name| name.strip_suffix(".bin"))
            else {
                continue;
            };
            let transaction =
                self.load_authorized_descriptor(transaction_id, &object.path, checksum)?;
            let descriptor_runs = transaction
                .runs
                .iter()
                .map(cell_wal_run_identity)
                .collect::<BTreeSet<_>>();
            if expected_run_identities.is_subset(&descriptor_runs) {
                matches.push(transaction);
            }
        }
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.pop()),
            count => Err(BorsukError::InvalidStorage(format!(
                "retained cell WAL transaction `{transaction_id}` has {count} matching descriptors"
            ))),
        }
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

    fn load_committed_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<CommittedCellWalTransaction>> {
        let marker_path = commit_marker_path(transaction_id);
        let descriptor_reference =
            if let Some(marker) = self.storage.read_coordination_object(&marker_path)? {
                let marker = commit_marker_from_slice(&marker.bytes, &marker_path)?;
                Some((marker.descriptor_path, marker.descriptor_checksum))
            } else {
                let state_path = transaction_state_path(transaction_id);
                self.storage
                    .read_coordination_object(&state_path)?
                    .map(|state| transaction_state_from_slice(&state.bytes, &state_path))
                    .transpose()?
                    .and_then(|state| match state {
                        CellWalTransactionState::Committed {
                            descriptor_path,
                            descriptor_checksum,
                        } => Some((descriptor_path, descriptor_checksum)),
                        _ => None,
                    })
            };
        let Some((descriptor_path, descriptor_checksum)) = descriptor_reference else {
            return Ok(None);
        };
        let descriptor = self
            .storage
            .read_bytes_with_cache_status_and_checksum(&descriptor_path, &descriptor_checksum)?;
        let descriptor = transaction_descriptor_from_slice(&descriptor.bytes, &descriptor_path)?;
        if descriptor.transaction_id != transaction_id {
            return Err(BorsukError::InvalidStorage(format!(
                "transaction descriptor `{}` belongs to `{}` instead of `{transaction_id}`",
                descriptor_path, descriptor.transaction_id
            )));
        }
        Ok(Some(CommittedCellWalTransaction {
            transaction_id: descriptor.transaction_id,
            descriptor_path,
            descriptor_checksum,
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

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct PositionedClaimAuthorization {
    layout: u16,
    source_epoch: u64,
    transaction_id: String,
    transaction_digest: String,
    shard: u8,
    sequence: u64,
    envelope_checksum: String,
}

fn claim_authorization_path(source_epoch: u64, transaction_id: &str) -> String {
    let digest = blake3::hash(transaction_id.as_bytes()).to_hex().to_string();
    format!(
        "positioned-log/claim-authorizations/{source_epoch}/{}/{}.json",
        &digest[..2],
        digest
    )
}

fn claim_authorization_bytes(
    source_epoch: u64,
    shard: u8,
    sequence: u64,
    transaction_id: &str,
    envelope_checksum: &str,
) -> Result<Vec<u8>> {
    validate_transaction_id(transaction_id)?;
    validate_cell_wal_checksum(
        envelope_checksum,
        "positioned envelope",
        "claim authorization",
    )?;
    if source_epoch == 0 {
        return Err(BorsukError::InvalidStorage(
            "positioned claim authorization source epoch must be positive".to_string(),
        ));
    }
    if shard >= crate::positioned_log::SOURCE_SHARD_COUNT || sequence == 0 {
        return Err(BorsukError::InvalidStorage(
            "positioned claim authorization has an invalid source position".to_string(),
        ));
    }
    serde_json::to_vec(&PositionedClaimAuthorization {
        layout: 1,
        source_epoch,
        transaction_id: transaction_id.to_string(),
        transaction_digest: blake3::hash(transaction_id.as_bytes()).to_hex().to_string(),
        shard,
        sequence,
        envelope_checksum: envelope_checksum.to_string(),
    })
    .map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "failed to encode positioned claim authorization: {error}"
        ))
    })
}

fn write_claim_authorization_receipt(
    storage: &Storage,
    source_epoch: u64,
    shard: u8,
    sequence: u64,
    transaction_id: &str,
    envelope_checksum: &str,
) -> Result<()> {
    let path = claim_authorization_path(source_epoch, transaction_id);
    let bytes = claim_authorization_bytes(
        source_epoch,
        shard,
        sequence,
        transaction_id,
        envelope_checksum,
    )?;
    match storage.write_coordination_object(&path, &bytes, None) {
        Ok(_) => Ok(()),
        Err(BorsukError::ConcurrentModification { .. }) => {
            let existing = storage
                .read_coordination_object(&path)?
                .ok_or_else(|| BorsukError::ConcurrentModification { path: path.clone() })?;
            if existing.bytes == bytes {
                Ok(())
            } else {
                Err(BorsukError::InvalidStorage(format!(
                    "positioned claim authorization `{path}` conflicts"
                )))
            }
        }
        Err(error) => Err(error),
    }
}

fn read_claim_authorization_receipt(
    storage: &Storage,
    source_epoch: u64,
    transaction_id: &str,
) -> Result<Option<String>> {
    let path = claim_authorization_path(source_epoch, transaction_id);
    let Some(stored) = storage.read_coordination_object(&path)? else {
        return Ok(None);
    };
    let receipt: PositionedClaimAuthorization =
        serde_json::from_slice(&stored.bytes).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "positioned claim authorization `{path}` is invalid: {error}"
            ))
        })?;
    if receipt.layout != 1
        || receipt.source_epoch != source_epoch
        || receipt.transaction_id != transaction_id
        || receipt.transaction_digest
            != blake3::hash(transaction_id.as_bytes()).to_hex().to_string()
        || receipt.shard
            != blake3::hash(transaction_id.as_bytes()).as_bytes()[0]
                % crate::positioned_log::SOURCE_SHARD_COUNT
        || receipt.sequence == 0
    {
        return Err(BorsukError::InvalidStorage(format!(
            "positioned claim authorization `{path}` has conflicting identity"
        )));
    }
    validate_cell_wal_checksum(&receipt.envelope_checksum, "positioned envelope", &path)?;
    validate_claim_authorization_envelope(
        storage,
        receipt.source_epoch,
        receipt.shard,
        receipt.sequence,
        &receipt.transaction_id,
        &receipt.envelope_checksum,
    )?;
    Ok(Some(receipt.envelope_checksum))
}

fn reclaim_claim_owner(
    storage: &Storage,
    source_epoch: u64,
    transaction_id: &str,
) -> Result<Option<String>> {
    if let Some(envelope_checksum) =
        read_claim_authorization_receipt(storage, source_epoch, transaction_id)?
    {
        return Ok(Some(envelope_checksum));
    }
    if let Some((position, envelope_checksum)) =
        authorized_transaction_receipt(storage, source_epoch, transaction_id)?
    {
        write_claim_authorization_receipt(
            storage,
            position.source_epoch,
            position.shard,
            position.sequence,
            transaction_id,
            &envelope_checksum,
        )?;
        return Ok(Some(envelope_checksum));
    }
    let state_path = transaction_state_path(transaction_id);
    let Some(current) = storage.read_coordination_object(&state_path)? else {
        return Err(BorsukError::InvalidStorage(format!(
            "claim owner `{transaction_id}` has no transaction state"
        )));
    };
    match transaction_state_from_slice(&current.bytes, &state_path)? {
        CellWalTransactionState::Prepared { expires_at_ms } if now_unix_ms() <= expires_at_ms => {
            Ok(None)
        }
        CellWalTransactionState::Prepared { .. } => {
            match storage.write_coordination_object(
                &state_path,
                &transaction_state_bytes(&CellWalTransactionState::Aborted)?,
                Some(current.version),
            ) {
                Ok(_) => Ok(Some(transaction_id.to_string())),
                Err(BorsukError::ConcurrentModification { .. }) => Ok(None),
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
            Ok(Some(transaction_id.to_string()))
        }
        CellWalTransactionState::Committed {
            descriptor_checksum,
            ..
        } => Ok(Some(descriptor_checksum)),
        CellWalTransactionState::Aborted => Ok(Some(transaction_id.to_string())),
    }
}

enum ClaimAcquireAttempt {
    Acquired(CellWalHeldClaim),
    Contended,
}

fn try_acquire_claim_page(
    storage: &Storage,
    source_epoch: u64,
    owner_revisions: &mut BTreeMap<String, Option<String>>,
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
                let revision = if let Some(revision) = owner_revisions.get(owner) {
                    revision.clone()
                } else {
                    let revision = reclaim_claim_owner(storage, source_epoch, owner)?;
                    owner_revisions.insert(owner.clone(), revision.clone());
                    revision
                };
                let Some(revision) = revision else {
                    return Ok(ClaimAcquireAttempt::Contended);
                };
                Some(revision)
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
    owner: &str,
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
                Some(CellWalClaimLock::Owned { transaction_id }) if transaction_id == owner => {}
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
    owner: &str,
    revision: &str,
    claims: Vec<CellWalHeldClaim>,
) -> CellWalClaimCheckpoint {
    crate::parallel::install_io(|| {
        claims
            .into_par_iter()
            .filter_map(|claim| release_claim_page(storage, owner, revision, claim).ok())
            .flatten()
            .collect()
    })
}

fn restore_claim_page(
    storage: &Storage,
    owner: &str,
    claim: CellWalHeldClaim,
) -> Result<CellWalClaimCheckpoint> {
    const MAX_ATTEMPTS: usize = 128;
    let page_index = u8::try_from(claim.previous_revisions[0].0 / CELL_WAL_CLAIM_PAGE_SLOTS)
        .expect("claim page index fits u8");
    for _ in 0..MAX_ATTEMPTS {
        let current = storage
            .read_coordination_object(&claim.path)?
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!("claim page `{}` disappeared", claim.path))
            })?;
        let mut page = claim_page_from_slice(&current.bytes, &claim.path, page_index)?;
        for (shard, previous) in &claim.previous_revisions {
            match page.slots.get(shard) {
                Some(CellWalClaimLock::Owned { transaction_id }) if transaction_id == owner => {}
                _ => return Ok(CellWalClaimCheckpoint::new()),
            }
            match previous {
                Some(revision) => {
                    page.slots.insert(
                        *shard,
                        CellWalClaimLock::Available {
                            revision: revision.clone(),
                        },
                    );
                }
                None => {
                    page.slots.remove(shard);
                }
            }
        }
        match storage.write_coordination_object(
            &claim.path,
            &claim_page_bytes(&page)?,
            Some(current.version),
        ) {
            Ok(_) => {
                return Ok(claim
                    .previous_revisions
                    .into_iter()
                    .filter_map(|(shard, revision)| revision.map(|revision| (shard, revision)))
                    .collect());
            }
            Err(BorsukError::ConcurrentModification { .. }) => continue,
            Err(error) => return Err(error),
        }
    }
    Err(BorsukError::ConcurrentModification { path: claim.path })
}

fn restore_claims(
    storage: &Storage,
    owner: &str,
    claims: Vec<CellWalHeldClaim>,
) -> CellWalClaimCheckpoint {
    crate::parallel::install_io(|| {
        claims
            .into_par_iter()
            .filter_map(|claim| restore_claim_page(storage, owner, claim).ok())
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
    source_epoch: u64,
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
        let mut owner_revisions = BTreeMap::new();
        for (&page, shards) in &pages {
            let path = claim_page_path(page);
            match try_acquire_claim_page(
                storage,
                source_epoch,
                &mut owner_revisions,
                transaction_id,
                page,
                &path,
                shards,
            ) {
                Ok(ClaimAcquireAttempt::Acquired(claim)) => acquired.push(claim),
                Ok(ClaimAcquireAttempt::Contended) => {
                    last_contended_path = path;
                    contended = true;
                    break;
                }
                Err(error) => {
                    let _ = restore_claims(storage, transaction_id, acquired);
                    return Err(error);
                }
            }
        }
        if !contended {
            return Ok(acquired);
        }
        let _ = restore_claims(storage, transaction_id, acquired);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::positioned_log::INITIAL_POSITIONED_SOURCE_EPOCH;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn run() -> PreparedCellWalRun {
        PreparedCellWalRun {
            transaction_id: "transaction-1".to_string(),
            cell: LogicalCellId::new(7, 9),
            lane: 3,
            kind: CellWalRunKind::IdDirectory,
            metadata: vec![1, 2, 3],
            path: format!(
                "cells/7/9/wal/3/runs/id-directory/{}.parquet",
                "ab".repeat(32)
            ),
            checksum: "ab".repeat(32),
            record_count: 5,
            byte_len: 123,
        }
    }

    #[test]
    fn aborted_claim_restores_exact_prior_revision_or_absence() {
        let storage = Storage::from_object_store(
            "memory:///claim-abort-restore".to_string(),
            Arc::new(InMemory::new()),
        )
        .unwrap();
        let shard = 7_u16;
        let page = u8::try_from(shard / CELL_WAL_CLAIM_PAGE_SLOTS).unwrap();
        let path = claim_page_path(page);

        ensure_prepared_transaction(&storage, "first-attempt").unwrap();
        let first = match try_acquire_claim_page(
            &storage,
            INITIAL_POSITIONED_SOURCE_EPOCH,
            &mut BTreeMap::new(),
            "first-attempt",
            page,
            &path,
            &[shard],
        )
        .unwrap()
        {
            ClaimAcquireAttempt::Acquired(claim) => claim,
            ClaimAcquireAttempt::Contended => panic!("fresh claim unexpectedly contended"),
        };
        restore_claims(&storage, "first-attempt", vec![first]);
        let restored = storage.read_coordination_object(&path).unwrap().unwrap();
        assert!(
            !claim_page_from_slice(&restored.bytes, &path, page)
                .unwrap()
                .slots
                .contains_key(&shard)
        );

        let revision = "ab".repeat(32);
        let mut available = CellWalClaimPage::default();
        available.slots.insert(
            shard,
            CellWalClaimLock::Available {
                revision: revision.clone(),
            },
        );
        storage
            .write_coordination_object(
                &path,
                &claim_page_bytes(&available).unwrap(),
                Some(restored.version),
            )
            .unwrap();
        ensure_prepared_transaction(&storage, "second-attempt").unwrap();
        let second = match try_acquire_claim_page(
            &storage,
            INITIAL_POSITIONED_SOURCE_EPOCH,
            &mut BTreeMap::new(),
            "second-attempt",
            page,
            &path,
            &[shard],
        )
        .unwrap()
        {
            ClaimAcquireAttempt::Acquired(claim) => claim,
            ClaimAcquireAttempt::Contended => panic!("available claim unexpectedly contended"),
        };
        restore_claims(&storage, "second-attempt", vec![second]);
        let restored = storage.read_coordination_object(&path).unwrap().unwrap();
        assert_eq!(
            claim_page_from_slice(&restored.bytes, &path, page)
                .unwrap()
                .slots
                .get(&shard),
            Some(&CellWalClaimLock::Available { revision })
        );
    }

    #[test]
    fn positioned_claim_authorization_is_idempotent_and_epoch_scoped() {
        let storage = Storage::from_object_store(
            "memory:///claim-authorization".to_string(),
            Arc::new(InMemory::new()),
        )
        .unwrap();
        let transaction_id = "positioned-transaction";
        let shard = blake3::hash(transaction_id.as_bytes()).as_bytes()[0]
            % crate::positioned_log::SOURCE_SHARD_COUNT;
        let checksum = "ab".repeat(32);
        write_claim_authorization_receipt(&storage, 7, shard, 3, transaction_id, &checksum)
            .unwrap();
        write_claim_authorization_receipt(&storage, 7, shard, 3, transaction_id, &checksum)
            .unwrap();
        write_claim_authorization_receipt(&storage, 8, shard, 4, transaction_id, &"cd".repeat(32))
            .unwrap();
        let epoch_seven = storage
            .read_coordination_object(&claim_authorization_path(7, transaction_id))
            .unwrap()
            .unwrap();
        let epoch_eight = storage
            .read_coordination_object(&claim_authorization_path(8, transaction_id))
            .unwrap()
            .unwrap();
        assert_ne!(epoch_seven.bytes, epoch_eight.bytes);
    }

    #[test]
    fn positioned_claim_authorization_requires_exact_envelope_identity() {
        let storage = Storage::from_object_store(
            "memory:///claim-authorization-envelope".to_string(),
            Arc::new(InMemory::new()),
        )
        .unwrap();
        let owner = "positioned-owner";
        let other = "other-positioned-owner";
        let stamp = crate::mutation::MutationStamp::new(
            crate::mutation::MutationVersion::from_parts(1, [1; 16]),
            [2; 32],
        );
        let payload = |id: &[u8]| {
            crate::format::tombstone_ids_to_parquet(&[(
                id.to_vec(),
                crate::mutation::MutationState::new(stamp, crate::mutation::MutationOperation::Put),
            )])
            .unwrap()
        };
        let positioned = crate::positioned_log::PositionedLogWriter::create_from_storage(
            storage.clone(),
            7,
            &"ab".repeat(32),
        )
        .unwrap();
        let committed = positioned
            .append(
                owner,
                &"ab".repeat(32),
                vec![crate::positioned_log::PositionedMutationPayloadInput {
                    modality: crate::positioned_log::PositionedMutationModality::Tombstone,
                    role: "owner".to_string(),
                    id_bloom: Vec::new(),
                    format: crate::positioned_log::PositionedPayloadFormat::Parquet,
                    rows: 1,
                    bytes: payload(b"owner"),
                }],
            )
            .unwrap();
        let other_committed = positioned
            .append(
                other,
                &"ab".repeat(32),
                vec![crate::positioned_log::PositionedMutationPayloadInput {
                    modality: crate::positioned_log::PositionedMutationModality::Tombstone,
                    role: "other".to_string(),
                    id_bloom: Vec::new(),
                    format: crate::positioned_log::PositionedPayloadFormat::Parquet,
                    rows: 1,
                    bytes: payload(b"other"),
                }],
            )
            .unwrap();
        let path = claim_authorization_path(7, owner);
        let transaction_digest = blake3::hash(owner.as_bytes()).to_hex().to_string();
        let exact = PositionedClaimAuthorization {
            layout: 1,
            source_epoch: 7,
            transaction_id: owner.to_string(),
            transaction_digest: transaction_digest.clone(),
            shard: committed.position.shard,
            sequence: committed.position.sequence,
            envelope_checksum: committed.envelope_checksum.clone(),
        };
        let replace = |receipt: &PositionedClaimAuthorization| {
            let bytes = serde_json::to_vec(receipt).unwrap();
            let current = storage.read_coordination_object(&path).unwrap();
            storage
                .write_coordination_object(&path, &bytes, current.map(|stored| stored.version))
                .unwrap();
        };

        replace(&exact);
        let before = storage.request_counts();
        assert_eq!(
            read_claim_authorization_receipt(&storage, 7, owner).unwrap(),
            Some(committed.envelope_checksum.clone())
        );
        let delta = storage.request_counts().delta(&before);
        assert_eq!(delta.gets, 2, "receipt plus authoritative envelope");

        let mut wrong_sequence = exact;
        wrong_sequence.sequence += 1;
        replace(&wrong_sequence);
        assert!(read_claim_authorization_receipt(&storage, 7, owner).is_err());

        let mut wrong_envelope = wrong_sequence;
        wrong_envelope.sequence = committed.position.sequence;
        wrong_envelope.envelope_checksum = other_committed.envelope_checksum;
        replace(&wrong_envelope);
        assert!(read_claim_authorization_receipt(&storage, 7, owner).is_err());

        let mut missing_envelope = wrong_envelope;
        missing_envelope.envelope_checksum = "ef".repeat(32);
        replace(&missing_envelope);
        assert!(read_claim_authorization_receipt(&storage, 7, owner).is_err());

        let corrupt = b"not a parquet envelope";
        let corrupt_checksum = blake3::hash(corrupt).to_hex().to_string();
        storage
            .write_bytes(
                &crate::positioned_log::canonical_envelope_path(&corrupt_checksum),
                corrupt,
            )
            .unwrap();
        let mut corrupt_envelope = missing_envelope;
        corrupt_envelope.envelope_checksum = corrupt_checksum;
        replace(&corrupt_envelope);
        assert!(read_claim_authorization_receipt(&storage, 7, owner).is_err());
    }

    #[test]
    fn normal_one_page_claim_has_exact_counts_and_no_authorization_object() {
        let storage = Storage::from_object_store(
            "memory:///claim-normal-counts".to_string(),
            Arc::new(InMemory::new()),
        )
        .unwrap();
        let store =
            CellWalStore::from_storage(storage.clone(), CellWalConfig::default(), 7).unwrap();
        let before = storage.request_counts();
        let mut guard = store.claim_ids("normal-claim", [b"id".as_slice()]).unwrap();
        let transaction_hash = blake3::hash(b"normal-claim");
        let shard = transaction_hash.as_bytes()[0] % crate::positioned_log::SOURCE_SHARD_COUNT;
        guard
            .finish_authorized(7, shard, 1, &"ab".repeat(32))
            .unwrap();
        let delta = storage.request_counts().delta(&before);
        assert_eq!(delta.gets, 1);
        assert_eq!(delta.puts, 3);
        assert!(
            storage
                .read_coordination_object(&claim_authorization_path(7, "normal-claim"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn one_authorized_owner_across_twenty_two_pages_resolves_once() {
        let storage = Storage::from_object_store(
            "memory:///claim-owner-resolution".to_string(),
            Arc::new(InMemory::new()),
        )
        .unwrap();
        let owner = "wide-owner";
        let stamp = crate::mutation::MutationStamp::new(
            crate::mutation::MutationVersion::from_parts(1, [1; 16]),
            [2; 32],
        );
        let bytes = crate::format::tombstone_ids_to_parquet(&[(
            b"wide-id".to_vec(),
            crate::mutation::MutationState::new(stamp, crate::mutation::MutationOperation::Put),
        )])
        .unwrap();
        let committed = crate::positioned_log::PositionedLogWriter::create_from_storage(
            storage.clone(),
            7,
            &"ab".repeat(32),
        )
        .unwrap()
        .append(
            owner,
            &"ab".repeat(32),
            vec![crate::positioned_log::PositionedMutationPayloadInput {
                modality: crate::positioned_log::PositionedMutationModality::Tombstone,
                role: "wide-owner".to_string(),
                id_bloom: Vec::new(),
                format: crate::positioned_log::PositionedPayloadFormat::Parquet,
                rows: 1,
                bytes,
            }],
        )
        .unwrap();
        write_claim_authorization_receipt(
            &storage,
            committed.position.source_epoch,
            committed.position.shard,
            committed.position.sequence,
            owner,
            &committed.envelope_checksum,
        )
        .unwrap();
        let page_count = CELL_WAL_CLAIM_SHARDS.div_ceil(CELL_WAL_CLAIM_PAGE_SLOTS);
        for page in 0..page_count {
            let page = u8::try_from(page).unwrap();
            let claim_shard = u16::from(page) * CELL_WAL_CLAIM_PAGE_SLOTS;
            let mut claim_page = CellWalClaimPage::default();
            claim_page.slots.insert(
                claim_shard,
                CellWalClaimLock::Owned {
                    transaction_id: owner.to_string(),
                },
            );
            storage
                .write_coordination_object(
                    &claim_page_path(page),
                    &claim_page_bytes(&claim_page).unwrap(),
                    None,
                )
                .unwrap();
        }
        let guard = CellWalClaimGuard {
            storage: storage.clone(),
            transaction_id: "observer".to_string(),
            locks: Vec::new(),
            transaction_committed: true,
            source_epoch: 7,
        };
        let before = storage.request_counts();
        let checkpoint = guard.synchronized_checkpoint().unwrap();
        let delta = storage.request_counts().delta(&before);
        assert_eq!(checkpoint.len(), usize::from(page_count));
        assert_eq!(delta.gets, u64::from(page_count) + 2);
        assert_eq!(delta.puts, 0);
    }

    #[test]
    fn crash_gap_recovers_from_one_head_and_backfills_authorization() {
        let storage = Storage::from_object_store(
            "memory:///claim-crash-gap".to_string(),
            Arc::new(InMemory::new()),
        )
        .unwrap();
        let store =
            CellWalStore::from_storage(storage.clone(), CellWalConfig::default(), 7).unwrap();
        let owner = "crashed-after-positioned-cas";
        let guard = store.claim_ids(owner, [b"same-id".as_slice()]).unwrap();
        let stamp = crate::mutation::MutationStamp::new(
            crate::mutation::MutationVersion::from_parts(1, [1; 16]),
            [2; 32],
        );
        let bytes = crate::format::tombstone_ids_to_parquet(&[(
            b"same-id".to_vec(),
            crate::mutation::MutationState::new(stamp, crate::mutation::MutationOperation::Put),
        )])
        .unwrap();
        let positioned = crate::positioned_log::PositionedLogWriter::create_from_storage(
            storage.clone(),
            7,
            &"ab".repeat(32),
        )
        .unwrap();
        positioned
            .append(
                owner,
                &"ab".repeat(32),
                vec![crate::positioned_log::PositionedMutationPayloadInput {
                    modality: crate::positioned_log::PositionedMutationModality::Tombstone,
                    role: "claim-crash-gap".to_string(),
                    id_bloom: Vec::new(),
                    format: crate::positioned_log::PositionedPayloadFormat::Parquet,
                    rows: 1,
                    bytes,
                }],
            )
            .unwrap();
        std::mem::forget(guard);

        let before = storage.request_counts();
        let recovered = store
            .claim_ids("recovery-attempt", [b"same-id".as_slice()])
            .unwrap();
        let delta = storage.request_counts().delta(&before);
        assert_eq!(delta.gets, 4);
        assert_eq!(delta.puts, 3);
        assert!(
            storage
                .read_coordination_object(&claim_authorization_path(7, owner))
                .unwrap()
                .is_some()
        );
        std::mem::forget(recovered);
    }

    #[test]
    fn corrupt_claim_authorization_fails_closed() {
        let storage = Storage::from_object_store(
            "memory:///claim-corrupt-authorization".to_string(),
            Arc::new(InMemory::new()),
        )
        .unwrap();
        let owner = "corrupt-owner";
        storage
            .write_coordination_object(&claim_authorization_path(7, owner), b"{}", None)
            .unwrap();
        assert!(read_claim_authorization_receipt(&storage, 7, owner).is_err());
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
        let marker = CellWalCommitMarker {
            descriptor_path: format!(
                "transactions/transaction-1/descriptors/{}.bin",
                "ef".repeat(32)
            ),
            descriptor_checksum: "ef".repeat(32),
        };

        let head_bytes = lane_head_bytes(&head).unwrap();
        let node_bytes = frontier_node_bytes(&node).unwrap();
        let marker_bytes = commit_marker_bytes(&marker).unwrap();
        assert!(head_bytes.starts_with(CELL_WAL_HEAD_MAGIC));
        assert!(node_bytes.starts_with(CELL_WAL_NODE_MAGIC));
        assert!(marker_bytes.starts_with(CELL_WAL_COMMIT_MAGIC));
        assert_eq!(lane_head_from_slice(&head_bytes, "HEAD").unwrap(), head);
        assert_eq!(frontier_node_from_slice(&node_bytes, "node").unwrap(), node);
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
        let storage = Storage::from_object_store(
            "memory:///claim-checkpoint-synchronization".to_string(),
            Arc::new(InMemory::new()),
        )
        .unwrap();
        let store = CellWalStore::from_storage(
            storage,
            CellWalConfig::default(),
            INITIAL_POSITIONED_SOURCE_EPOCH,
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
