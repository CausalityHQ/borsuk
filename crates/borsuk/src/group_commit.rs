//! Process-local group commit for high-throughput object-store ingest.

use std::{
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
    },
    time::{Duration, Instant},
};

use crate::{
    BorsukError, BorsukIndex, RequestCounts, Result, VectorRecord,
    mutation::{CanonicalMutation, MutationClock},
};
use rayon::prelude::*;
use uuid::Uuid;

const LANE_LEASE_TTL_MS: u64 = 60 * 60 * 1_000;
// Keep the active WAL bounded while allowing one materialization pass to
// coalesce enough extents into production-sized immutable segments. A smaller
// interval repeatedly rewrites the growing delta and can exceed the frozen
// physical-write-amplification gate before the caller reaches drain.
const BACKGROUND_MATERIALIZATION_BLOCK_INTERVAL: u64 = 1_024;

#[derive(Default)]
struct MaintenanceState {
    running: bool,
    requested: bool,
}

#[derive(Default)]
struct WorkerThreads {
    handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl Drop for WorkerThreads {
    fn drop(&mut self) {
        let handles = self
            .handles
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        for handle in handles.drain(..) {
            let _ = handle.join();
        }
    }
}

impl MaintenanceState {
    fn request_pass(&mut self) -> bool {
        self.requested = true;
        if self.running {
            return false;
        }
        self.running = true;
        self.requested = false;
        true
    }

    fn finish_pass(&mut self) -> bool {
        if self.requested {
            self.requested = false;
            true
        } else {
            self.running = false;
            false
        }
    }

    fn stop(&mut self) {
        self.running = false;
        self.requested = false;
    }
}

/// Bounds for process-local WAL group commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommitConfig {
    /// Maximum time the first caller waits for concurrent appends to join it.
    pub max_delay: Duration,
    /// Flush as soon as the pending group reaches this many records.
    pub max_records: usize,
    /// Independent commit lanes. Each lane owns an index handle and publishes
    /// through the shared collection WAL coordination protocol.
    pub worker_lanes: usize,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_delay: Duration::from_millis(2),
            max_records: 1_024,
            worker_lanes: 8,
        }
    }
}

impl GroupCommitConfig {
    fn validate(self) -> Result<Self> {
        if self.max_delay > Duration::from_secs(1) {
            return Err(BorsukError::InvalidStorage(
                "group commit max_delay must not exceed one second".to_string(),
            ));
        }
        if self.max_records == 0 {
            return Err(BorsukError::InvalidStorage(
                "group commit max_records must be positive".to_string(),
            ));
        }
        if self.worker_lanes == 0 || self.worker_lanes > 64 {
            return Err(BorsukError::InvalidStorage(
                "group commit worker_lanes must be between 1 and 64".to_string(),
            ));
        }
        Ok(self)
    }
}

/// Receipt returned after the caller's records are durably visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommitLaneReceipt {
    /// Records supplied by this caller.
    pub records: usize,
    /// Total records sharing the same durable WAL transaction.
    pub committed_records: usize,
    /// Worker-local sequence shared by every caller in the same commit.
    pub commit_sequence: u64,
    /// Fencing epoch that owns this durable sequence.
    pub lease_epoch: u64,
    /// Persisted writer-stripe ordinal. Together with `lease_epoch` and
    /// `commit_sequence`, this uniquely identifies the durable group and its
    /// deterministic Arrow extent path.
    pub commit_lane: usize,
    /// Bytes in the immutable extent created by this receipt.
    pub acknowledgement_bytes: u64,
    /// BLAKE3 checksum of the exact immutable Arrow extent acknowledged.
    pub extent_checksum: [u8; 32],
    /// BLAKE3 checksum of the exact writer-stripe HEAD successor published
    /// before acknowledgement.
    pub published_head_checksum: [u8; 32],
    /// Physical requests issued by the whole shared commit.
    pub requests: RequestCounts,
}

/// One writer stripe that did not acknowledge a multi-stripe append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCommitLaneFailure {
    /// Persisted writer-stripe ordinal.
    pub commit_lane: usize,
    /// Exact worker or storage failure for this lane.
    pub message: String,
}

/// Receipt returned after all of the caller's records are durably visible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCommitReceipt {
    /// Records supplied by this caller across all writer stripes.
    pub records: usize,
    /// Total records sharing the durable lane commits that contain this call.
    pub committed_records: usize,
    /// Sequence of the first lane receipt. Meaningful as a commit identity only
    /// when [`GroupCommitReceipt::lane_receipts`] contains one entry.
    pub commit_sequence: u64,
    /// Ordinal of the first lane receipt. Meaningful as a commit identity only
    /// when [`GroupCommitReceipt::lane_receipts`] contains one entry.
    pub commit_lane: usize,
    /// Aggregate immutable extent bytes across this call's stripe commits.
    pub acknowledgement_bytes: u64,
    /// Aggregate physical requests issued by this call's lane commits.
    pub requests: RequestCounts,
    /// One durable receipt for every writer stripe touched by this call.
    pub lane_receipts: Vec<GroupCommitLaneReceipt>,
}

/// One asynchronously submitted append awaiting its durable group receipt.
pub struct GroupCommitTicket {
    records: usize,
    results: Vec<(
        usize,
        Receiver<std::result::Result<GroupCommitLaneReceipt, String>>,
    )>,
    maintenance: Option<GroupCommitWriter>,
}

impl GroupCommitTicket {
    /// Wait until the shared WAL transaction containing this append is durable.
    pub fn wait(self) -> Result<GroupCommitReceipt> {
        let mut lane_receipts = Vec::with_capacity(self.results.len());
        let mut failed_lanes = Vec::new();
        for (commit_lane, result) in self.results {
            match result.recv() {
                Ok(Ok(receipt)) => lane_receipts.push(receipt),
                Ok(Err(message)) => failed_lanes.push(GroupCommitLaneFailure {
                    commit_lane,
                    message,
                }),
                Err(_) => failed_lanes.push(GroupCommitLaneFailure {
                    commit_lane,
                    message: "group commit worker stopped before acknowledging append".to_string(),
                }),
            }
        }
        lane_receipts.sort_by_key(|receipt| receipt.commit_lane);
        failed_lanes.sort_by_key(|failure| failure.commit_lane);
        if !failed_lanes.is_empty() {
            if let Some(maintenance) = self.maintenance {
                maintenance.trigger_background_materialization(&lane_receipts);
            }
            return Err(BorsukError::PartialGroupCommit {
                committed_lane_receipts: lane_receipts,
                failed_lanes,
            });
        }
        let committed_records = lane_receipts
            .iter()
            .map(|receipt| receipt.committed_records)
            .sum();
        let requests = lane_receipts
            .iter()
            .fold(RequestCounts::default(), |mut total, receipt| {
                total.gets += receipt.requests.gets;
                total.puts += receipt.requests.puts;
                total.deletes += receipt.requests.deletes;
                total.heads += receipt.requests.heads;
                total.lists += receipt.requests.lists;
                total
            });
        let acknowledgement_bytes = lane_receipts
            .iter()
            .map(|receipt| receipt.acknowledgement_bytes)
            .sum();
        let (commit_lane, commit_sequence) = lane_receipts.first().map_or((0, 0), |receipt| {
            (receipt.commit_lane, receipt.commit_sequence)
        });
        let receipt = GroupCommitReceipt {
            records: self.records,
            committed_records,
            commit_sequence,
            commit_lane,
            acknowledgement_bytes,
            requests,
            lane_receipts,
        };
        if let Some(maintenance) = self.maintenance {
            maintenance.trigger_background_materialization(&receipt.lane_receipts);
        }
        Ok(receipt)
    }
}

struct AppendRequest {
    lane: u16,
    records: Vec<VectorRecord>,
    response: Sender<std::result::Result<GroupCommitLaneReceipt, String>>,
}

enum WorkerRequest {
    Append(AppendRequest),
    Barrier(Sender<()>),
    Materialize(Sender<std::result::Result<(Vec<u64>, u64), String>>),
    CheckpointAll {
        sequences: Vec<u64>,
        manifest_version: u64,
        response: Sender<std::result::Result<(), String>>,
    },
    RetireMaterialized {
        manifest_version: u64,
        response: Sender<std::result::Result<(), String>>,
    },
}

/// Cloneable high-throughput writer that group-commits concurrent appends.
///
/// The writer owns one independent [`BorsukIndex`] handle per background
/// worker stripe. Calls remain synchronous and return only after the selected
/// stripe creates its immutable WAL extent and conditionally publishes the
/// stripe head that makes it visible. Records carry handle-local convergent
/// mutation versions; independent unobserved writers resolve deterministically
/// rather than by acknowledgement order. Strict duplicate-rejecting insertion
/// remains available through [`BorsukIndex::add`]. A short bounded delay
/// amortizes object-store requests over larger immutable transactions, while
/// stripes avoid process-local head-of-line blocking between them.
pub struct GroupCommitWriter {
    requests: Arc<[Sender<WorkerRequest>]>,
    worker_stripes: Arc<[u16]>,
    lane_count: u16,
    drain_lock: Arc<Mutex<()>>,
    maintenance_state: Arc<Mutex<MaintenanceState>>,
    maintenance_error: Arc<Mutex<Option<String>>>,
    workers: Arc<WorkerThreads>,
}

impl Clone for GroupCommitWriter {
    fn clone(&self) -> Self {
        Self {
            requests: Arc::clone(&self.requests),
            worker_stripes: Arc::clone(&self.worker_stripes),
            lane_count: self.lane_count,
            drain_lock: Arc::clone(&self.drain_lock),
            maintenance_state: Arc::clone(&self.maintenance_state),
            maintenance_error: Arc::clone(&self.maintenance_error),
            workers: Arc::clone(&self.workers),
        }
    }
}

impl GroupCommitWriter {
    /// Consume an index handle and start its group-commit worker.
    pub fn new(index: BorsukIndex, config: GroupCommitConfig) -> Result<Self> {
        Self::new_with_mutation_clock(
            index,
            config,
            Arc::new(MutationClock::new(*Uuid::new_v4().as_bytes())),
        )
    }

    fn new_with_mutation_clock(
        index: BorsukIndex,
        config: GroupCommitConfig,
        mutation_clock: Arc<MutationClock>,
    ) -> Result<Self> {
        let config = config.validate()?;
        index.ensure_lane_log_payloads_supported()?;
        let lane_count = index.lane_log_lane_count();
        if config.worker_lanes > usize::from(lane_count) {
            return Err(BorsukError::InvalidStorage(format!(
                "group commit worker_lanes {} exceeds persisted lane count {lane_count}",
                config.worker_lanes
            )));
        }
        let mut indexes = Vec::with_capacity(config.worker_lanes);
        // Every worker, including lane zero, needs its own outer request scope.
        // Independent scopes wrap the original counted store; retaining the
        // original handle as lane zero would therefore charge every child
        // lane's requests to lane zero as well as to the child itself.
        for _ in 0..config.worker_lanes {
            indexes.push(index.clone_for_independent_writer());
        }
        drop(index);
        let mut requests = Vec::with_capacity(config.worker_lanes);
        let mut worker_handles = Vec::with_capacity(config.worker_lanes);
        let mut worker_stripes = Vec::with_capacity(config.worker_lanes);
        let mut claimed_stripes = std::collections::HashSet::with_capacity(config.worker_lanes);
        let claim_start = (Uuid::new_v4().as_u128() % u128::from(lane_count)) as u16;
        for (worker, index) in indexes.into_iter().enumerate() {
            let mut claimed = None;
            let worker_offset =
                u16::try_from(worker % usize::from(lane_count)).expect("persisted lane fits u16");
            let worker_start = (claim_start + worker_offset) % lane_count;
            for lane in index.lane_log_claim_candidates(worker_start)? {
                if claimed_stripes.contains(&lane) {
                    continue;
                }
                match index.acquire_lane_log_upsert_writer(
                    lane,
                    current_time_ms()?,
                    LANE_LEASE_TTL_MS,
                ) {
                    Ok(writer) => {
                        claimed = Some(writer);
                        break;
                    }
                    Err(BorsukError::ConcurrentModification { .. }) => continue,
                    Err(error) => return Err(error),
                }
            }
            let lane_writer = claimed.ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "group commit cannot claim {0} writer stripes: only {worker} of {lane_count} persisted stripes are available",
                    config.worker_lanes
                ))
            })?;
            let stripe = lane_writer.lane();
            claimed_stripes.insert(stripe);
            worker_stripes.push(stripe);
            let dimensions = index.primary_dimensions();
            let (sender, receiver) = mpsc::channel();
            let worker_mutation_clock = Arc::clone(&mutation_clock);
            let handle = std::thread::Builder::new()
                .name(format!("borsuk-group-commit-{worker}"))
                .spawn(move || {
                    run_worker(
                        index,
                        vec![lane_writer],
                        worker_mutation_clock,
                        dimensions,
                        config,
                        receiver,
                    )
                })
                .map_err(|error| {
                    BorsukError::InvalidStorage(format!(
                        "failed to start group commit worker {worker}: {error}"
                    ))
                })?;
            worker_handles.push(handle);
            requests.push(sender);
        }
        Ok(Self {
            requests: requests.into(),
            worker_stripes: worker_stripes.into(),
            lane_count,
            drain_lock: Arc::new(Mutex::new(())),
            maintenance_state: Arc::new(Mutex::new(MaintenanceState::default())),
            maintenance_error: Arc::new(Mutex::new(None)),
            workers: Arc::new(WorkerThreads {
                handles: Mutex::new(worker_handles),
            }),
        })
    }

    /// Append records and wait for their shared durable commit.
    pub fn append(&self, records: Vec<VectorRecord>) -> Result<GroupCommitReceipt> {
        self.append_async(records)?.wait()
    }

    /// Submit records without blocking, allowing one producer to keep several
    /// durable appends in flight and therefore participate in group commit.
    pub fn append_async(&self, records: Vec<VectorRecord>) -> Result<GroupCommitTicket> {
        let retry_maintenance = self
            .maintenance_error
            .lock()
            .map_err(|_| {
                BorsukError::InvalidStorage(
                    "group commit maintenance error lock is poisoned".to_string(),
                )
            })?
            .take()
            .is_some();
        if retry_maintenance && let Err(error) = self.drain() {
            if let Ok(mut slot) = self.maintenance_error.lock() {
                *slot = Some(error.to_string());
            }
            return Err(error);
        }
        let record_count = records.len();
        if records.is_empty() {
            return Ok(GroupCommitTicket {
                records: 0,
                results: Vec::new(),
                maintenance: None,
            });
        }
        // Most scalar appends touch one local worker stripe. Keep the dispatch
        // map sparse so each acknowledgement does not allocate an empty bucket
        // for every worker owned by this process.
        let mut by_worker = std::collections::HashMap::<usize, Vec<VectorRecord>>::with_capacity(
            record_count.min(self.requests.len()),
        );
        for record in records {
            let worker = lane_for_id(record.id.as_bytes(), self.requests.len());
            by_worker.entry(worker).or_default().push(record);
        }
        let mut results = Vec::new();
        let mut workers = by_worker.into_iter().collect::<Vec<_>>();
        workers.sort_unstable_by_key(|(worker, _)| *worker);
        for (worker, records) in workers {
            let lane = self.worker_stripes[worker];
            let (response, result) = mpsc::channel();
            self.requests[worker]
                .send(WorkerRequest::Append(AppendRequest {
                    lane,
                    records,
                    response,
                }))
                .map_err(|_| {
                    BorsukError::InvalidStorage("group commit worker stopped".to_string())
                })?;
            results.push((usize::from(lane), result));
        }
        Ok(GroupCommitTicket {
            records: record_count,
            results,
            maintenance: Some(self.clone()),
        })
    }

    fn trigger_background_materialization(&self, receipts: &[GroupCommitLaneReceipt]) {
        if !receipts.iter().any(|receipt| {
            receipt.commit_sequence > 0
                && receipt.commit_sequence % BACKGROUND_MATERIALIZATION_BLOCK_INTERVAL == 0
        }) {
            return;
        }
        let should_spawn = match self.maintenance_state.lock() {
            Ok(mut state) => state.request_pass(),
            Err(_) => {
                if let Ok(mut slot) = self.maintenance_error.lock() {
                    *slot = Some("group commit maintenance state lock is poisoned".to_string());
                }
                return;
            }
        };
        if !should_spawn {
            return;
        }
        let writer = self.clone();
        let state = Arc::clone(&self.maintenance_state);
        let error_slot = Arc::clone(&self.maintenance_error);
        let spawn = std::thread::Builder::new()
            .name("borsuk-lane-materializer".to_string())
            .spawn(move || {
                loop {
                    if let Err(error) = writer.drain() {
                        if let Ok(mut slot) = error_slot.lock() {
                            *slot = Some(error.to_string());
                        }
                        if let Ok(mut state) = state.lock() {
                            state.stop();
                        }
                        break;
                    }
                    let run_again = match state.lock() {
                        Ok(mut state) => state.finish_pass(),
                        Err(_) => false,
                    };
                    if !run_again {
                        break;
                    }
                }
            });
        if let Err(error) = spawn {
            if let Ok(mut slot) = self.maintenance_error.lock() {
                *slot = Some(format!("failed to start lane materializer: {error}"));
            }
            if let Ok(mut state) = self.maintenance_state.lock() {
                state.stop();
            }
        }
    }

    /// Materialize and retire every group acknowledged before this call.
    pub fn drain(&self) -> Result<()> {
        let _drain = self.drain_lock.lock().map_err(|_| {
            BorsukError::InvalidStorage("group commit drain lock is poisoned".to_string())
        })?;
        let mut barriers = Vec::with_capacity(self.requests.len());
        for requests in self.requests.iter() {
            let (done, wait) = mpsc::channel();
            requests.send(WorkerRequest::Barrier(done)).map_err(|_| {
                BorsukError::InvalidStorage("group commit worker stopped".to_string())
            })?;
            barriers.push(wait);
        }
        for barrier in barriers {
            barrier.recv().map_err(|_| {
                BorsukError::InvalidStorage(
                    "group commit worker stopped before drain barrier".to_string(),
                )
            })?;
        }
        let (done, wait) = mpsc::channel();
        self.requests[0]
            .send(WorkerRequest::Materialize(done))
            .map_err(|_| BorsukError::InvalidStorage("group commit worker stopped".to_string()))?;
        let (committed_sequences, manifest_version) = wait
            .recv()
            .map_err(|_| {
                BorsukError::InvalidStorage(
                    "group commit worker stopped before materialization completed".to_string(),
                )
            })?
            .map_err(BorsukError::InvalidStorage)?;
        if committed_sequences.len() != usize::from(self.lane_count) {
            return Err(BorsukError::InvalidStorage(format!(
                "lane materializer returned {} frontiers for {} lanes",
                committed_sequences.len(),
                self.lane_count
            )));
        }
        let (response, wait) = mpsc::channel();
        self.requests[0]
            .send(WorkerRequest::CheckpointAll {
                sequences: committed_sequences,
                manifest_version,
                response,
            })
            .map_err(|_| BorsukError::InvalidStorage("group commit worker stopped".to_string()))?;
        wait.recv()
            .map_err(|_| {
                BorsukError::InvalidStorage(
                    "group commit worker stopped before checkpoint completed".to_string(),
                )
            })?
            .map_err(BorsukError::InvalidStorage)?;
        let mut retirements = Vec::with_capacity(self.requests.len());
        for requests in self.requests.iter() {
            let (response, wait) = mpsc::channel();
            requests
                .send(WorkerRequest::RetireMaterialized {
                    manifest_version,
                    response,
                })
                .map_err(|_| {
                    BorsukError::InvalidStorage("group commit worker stopped".to_string())
                })?;
            retirements.push(wait);
        }
        for retirement in retirements {
            retirement
                .recv()
                .map_err(|_| {
                    BorsukError::InvalidStorage(
                        "group commit worker stopped before stripe retirement completed"
                            .to_string(),
                    )
                })?
                .map_err(BorsukError::InvalidStorage)?;
        }
        Ok(())
    }
}

fn lane_for_id(id: &[u8], lane_count: usize) -> usize {
    let digest = blake3::hash(id);
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&digest.as_bytes()[..8]);
    (u64::from_le_bytes(prefix) % lane_count as u64) as usize
}

#[cfg(test)]
mod maintenance_state_tests {
    use super::MaintenanceState;

    #[test]
    fn demand_arriving_during_a_pass_is_claimed_before_the_worker_exits() {
        let mut state = MaintenanceState::default();

        assert!(state.request_pass());
        assert!(!state.request_pass());
        assert!(state.finish_pass());
        assert!(!state.finish_pass());
    }

    #[test]
    fn demand_after_worker_exit_starts_a_new_worker() {
        let mut state = MaintenanceState::default();

        assert!(state.request_pass());
        assert!(!state.finish_pass());
        assert!(state.request_pass());
    }
}

fn current_time_ms() -> Result<u64> {
    u64::try_from(chrono::Utc::now().timestamp_millis()).map_err(|_| {
        BorsukError::InvalidStorage("system clock is before the Unix epoch".to_string())
    })
}

fn run_worker(
    mut index: BorsukIndex,
    mut lane_writers: Vec<crate::lane_log::LaneEpochWriter>,
    mutation_clock: Arc<MutationClock>,
    dimensions: usize,
    config: GroupCommitConfig,
    requests: Receiver<WorkerRequest>,
) {
    let mut deferred = None;
    loop {
        let request = match deferred.take().map_or_else(|| requests.recv(), Ok) {
            Ok(request) => request,
            Err(_) => break,
        };
        let first = match request {
            WorkerRequest::Append(request) => request,
            WorkerRequest::Barrier(done) => {
                let _ = done.send(());
                continue;
            }
            WorkerRequest::Materialize(done) => {
                let result = index
                    .materialize_lane_log_tail()
                    .map_err(|error| error.to_string());
                let _ = done.send(result);
                continue;
            }
            WorkerRequest::CheckpointAll {
                sequences,
                manifest_version,
                response,
            } => {
                let result = index
                    .checkpoint_lane_log_materialized_through(&sequences, manifest_version)
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
                continue;
            }
            WorkerRequest::RetireMaterialized {
                manifest_version,
                response,
            } => {
                let result = lane_writers
                    .iter_mut()
                    .try_for_each(|writer| {
                        writer
                            .retire_directory_if_materialized(
                                crate::GROUP_COMMIT_STRIPE_COUNT,
                                manifest_version,
                            )
                            .map(|_| ())
                    })
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
                continue;
            }
        };
        let deadline = Instant::now() + config.max_delay;
        let mut group = vec![first];
        let mut records = group[0].records.len();
        while records < config.max_records {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            match requests.recv_timeout(remaining) {
                Ok(WorkerRequest::Append(request)) => {
                    records = records.saturating_add(request.records.len());
                    group.push(request);
                }
                Ok(control) => {
                    deferred = Some(control);
                    break;
                }
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
            }
        }

        let mut by_lane = std::collections::HashMap::<u16, Vec<AppendRequest>>::new();
        for request in group {
            by_lane.entry(request.lane).or_default().push(request);
        }
        let lane_work = lane_writers
            .iter()
            .map(|writer| by_lane.remove(&writer.lane()))
            .collect::<Vec<_>>();
        for orphaned in by_lane.into_values().flatten() {
            let _ = orphaned
                .response
                .send(Err(format!("worker does not own lane {}", orphaned.lane)));
        }
        lane_writers
            .par_iter_mut()
            .zip(lane_work.into_par_iter())
            .for_each(|(writer, same_lane)| {
                let Some(mut same_lane) = same_lane else {
                    return;
                };
                let grouped_records = same_lane.iter().map(|request| request.records.len()).sum();
                let mut combined = Vec::with_capacity(grouped_records);
                let caller_sizes = same_lane
                    .iter_mut()
                    .map(|request| {
                        let len = request.records.len();
                        combined.append(&mut request.records);
                        len
                    })
                    .collect::<Vec<_>>();
                let now_ms = match current_time_ms() {
                    Ok(now_ms) => now_ms,
                    Err(error) => {
                        let message = error.to_string();
                        for request in same_lane {
                            let _ = request.response.send(Err(message.clone()));
                        }
                        return;
                    }
                };
                let now_ms_i64 = match i64::try_from(now_ms) {
                    Ok(now_ms) => now_ms,
                    Err(_) => {
                        let message = "system clock milliseconds exceed i64".to_string();
                        for request in same_lane {
                            let _ = request.response.send(Err(message.clone()));
                        }
                        return;
                    }
                };
                let versions = match mutation_clock.allocate_range_at(now_ms_i64, combined.len()) {
                    Ok(versions) => versions,
                    Err(error) => {
                        let message = error.to_string();
                        for request in same_lane {
                            let _ = request.response.send(Err(message.clone()));
                        }
                        return;
                    }
                };
                let mut stamped = Vec::with_capacity(combined.len());
                for (ordinal, record) in combined.into_iter().enumerate() {
                    let mutation = match versions
                        .at(ordinal)
                        .and_then(|version| CanonicalMutation::put(version, record))
                    {
                        Ok(mutation) => mutation,
                        Err(error) => {
                            let message = error.to_string();
                            for request in same_lane {
                                let _ = request.response.send(Err(message.clone()));
                            }
                            return;
                        }
                    };
                    stamped.push(
                        mutation
                            .into_record()
                            .expect("canonical put mutations carry records"),
                    );
                }
                let mut by_id = std::collections::HashMap::<Vec<u8>, usize>::new();
                let mut deduplicated = Vec::with_capacity(stamped.len());
                for record in stamped {
                    let key = record.id.as_bytes().to_vec();
                    if let Some(index) = by_id.get(&key).copied() {
                        deduplicated[index] = record;
                    } else {
                        by_id.insert(key, deduplicated.len());
                        deduplicated.push(record);
                    }
                }
                let committed_records = deduplicated.len();
                let committed = writer
                    .append_upsert_records_with_renewal_at(
                        &deduplicated,
                        dimensions,
                        now_ms,
                        LANE_LEASE_TTL_MS,
                    )
                    .map_err(|error| error.to_string());
                for (request, caller_records) in same_lane.into_iter().zip(caller_sizes) {
                    let response = committed.as_ref().map_or_else(
                        |error| Err(error.clone()),
                        |receipt| {
                            Ok(GroupCommitLaneReceipt {
                                records: caller_records,
                                committed_records,
                                commit_sequence: receipt.sequence,
                                lease_epoch: receipt.lease_epoch,
                                commit_lane: usize::from(receipt.lane),
                                acknowledgement_bytes: receipt.acknowledgement_bytes,
                                extent_checksum: receipt.extent_checksum,
                                published_head_checksum: receipt.published_head_checksum,
                                requests: receipt.requests,
                            })
                        },
                    );
                    let _ = request.response.send(response);
                }
                if committed.is_ok() {
                    let _ = writer.publish_durable_watermark_if_due();
                }
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IndexConfig, VectorMetric, mutation::MutationVersion};
    use object_store::{ObjectStore, memory::InMemory};

    #[test]
    fn partial_lane_failure_preserves_every_committed_receipt() {
        let (first_sender, first_receiver) = mpsc::channel();
        let (second_sender, second_receiver) = mpsc::channel();
        first_sender
            .send(Ok(GroupCommitLaneReceipt {
                records: 1,
                committed_records: 2,
                commit_sequence: 7,
                lease_epoch: 3,
                commit_lane: 1,
                acknowledgement_bytes: 4096,
                extent_checksum: [1; 32],
                published_head_checksum: [2; 32],
                requests: RequestCounts {
                    puts: 1,
                    ..RequestCounts::default()
                },
            }))
            .unwrap();
        second_sender
            .send(Err("injected lane failure".into()))
            .unwrap();

        let error = GroupCommitTicket {
            records: 2,
            results: vec![(1, first_receiver), (6, second_receiver)],
            maintenance: None,
        }
        .wait()
        .unwrap_err();

        let BorsukError::PartialGroupCommit {
            committed_lane_receipts,
            failed_lanes,
        } = error
        else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(committed_lane_receipts.len(), 1);
        assert_eq!(committed_lane_receipts[0].commit_lane, 1);
        assert_eq!(failed_lanes.len(), 1);
        assert_eq!(failed_lanes[0].commit_lane, 6);
        assert_eq!(failed_lanes[0].message, "injected lane failure");
    }

    #[test]
    fn independent_unobserved_writers_converge_by_complete_version_not_ack_order() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = "memory:///group-convergent-writer-order";
        let config = IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1_000,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        };
        let writer_config = GroupCommitConfig {
            max_delay: Duration::ZERO,
            max_records: 1,
            worker_lanes: 1,
        };
        let observed_floor = MutationVersion::from_parts(4_000_000_000_000_u64 << 16, [0; 16]);
        let high_clock = Arc::new(MutationClock::new([0xff; 16]));
        high_clock.observe(observed_floor).unwrap();
        let low_clock = Arc::new(MutationClock::new([0x01; 16]));
        low_clock.observe(observed_floor).unwrap();
        let high = GroupCommitWriter::new_with_mutation_clock(
            BorsukIndex::create_with_object_store(Arc::clone(&store), config).unwrap(),
            writer_config,
            high_clock,
        )
        .unwrap();
        let low = GroupCommitWriter::new_with_mutation_clock(
            BorsukIndex::open_with_object_store(Arc::clone(&store), uri).unwrap(),
            writer_config,
            low_clock,
        )
        .unwrap();

        let high_first = high
            .append(vec![VectorRecord::new("shared", vec![9.0, 0.0])])
            .unwrap();
        let low_later = low
            .append(vec![VectorRecord::new("shared", vec![1.0, 0.0])])
            .unwrap();
        assert_ne!(high_first.commit_lane, low_later.commit_lane);

        let reopened = BorsukIndex::open_with_object_store(store, uri).unwrap();
        assert_eq!(reopened.get_vector("shared").unwrap(), Some(vec![9.0, 0.0]));
    }
}
