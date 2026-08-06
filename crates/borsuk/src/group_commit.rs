//! Process-local group commit for high-throughput object-store ingest.

use std::{
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
    },
    time::{Duration, Instant},
};

use crate::{BorsukError, BorsukIndex, RequestCounts, Result, VectorRecord};
use rayon::prelude::*;

const LANE_LEASE_TTL_MS: u64 = 60 * 60 * 1_000;
const BACKGROUND_MATERIALIZATION_BLOCK_INTERVAL: u64 = 64;

#[derive(Default)]
struct MaintenanceState {
    running: bool,
    requested: bool,
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
    /// Persisted ownership-lane ordinal; paired with `commit_sequence`, this
    /// uniquely identifies the durable group.
    pub commit_lane: usize,
    /// Bytes in the authoritative lane HEAD PUT acknowledged by this receipt.
    pub acknowledgement_bytes: u64,
    /// Physical requests issued by the whole shared commit.
    pub requests: RequestCounts,
}

/// One ownership lane that did not acknowledge a multi-lane append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCommitLaneFailure {
    /// Persisted ownership-lane ordinal.
    pub commit_lane: usize,
    /// Exact worker or storage failure for this lane.
    pub message: String,
}

/// Receipt returned after all of the caller's records are durably visible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCommitReceipt {
    /// Records supplied by this caller across all ownership lanes.
    pub records: usize,
    /// Total records sharing the durable lane commits that contain this call.
    pub committed_records: usize,
    /// Sequence of the first lane receipt. Meaningful as a commit identity only
    /// when [`GroupCommitReceipt::lane_receipts`] contains one entry.
    pub commit_sequence: u64,
    /// Ordinal of the first lane receipt. Meaningful as a commit identity only
    /// when [`GroupCommitReceipt::lane_receipts`] contains one entry.
    pub commit_lane: usize,
    /// Aggregate authoritative lane HEAD bytes across this call's lane commits.
    pub acknowledgement_bytes: u64,
    /// Aggregate physical requests issued by this call's lane commits.
    pub requests: RequestCounts,
    /// One durable receipt for every ownership lane touched by this call.
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
    Materialize(Sender<std::result::Result<Vec<u64>, String>>),
    Checkpoint {
        lane: u16,
        sequence: u64,
        response: Sender<std::result::Result<(), String>>,
    },
}

/// Cloneable high-throughput writer that group-commits concurrent appends.
///
/// The writer owns one independent [`BorsukIndex`] handle per background lane.
/// Calls remain synchronous and return only after the selected lane publishes
/// its shared WAL transaction, so grouping and parallelism change neither
/// durability nor read visibility. Groups use claim-free last-write-wins
/// generations; strict duplicate-rejecting insertion remains available through
/// [`BorsukIndex::add`]. A short bounded delay replaces cross-writer S3 CAS
/// storms with larger immutable transactions, while lanes avoid process-local
/// head-of-line blocking between those transactions.
pub struct GroupCommitWriter {
    requests: Arc<[Sender<WorkerRequest>]>,
    lane_count: u16,
    drain_lock: Arc<Mutex<()>>,
    maintenance_state: Arc<Mutex<MaintenanceState>>,
    maintenance_error: Arc<Mutex<Option<String>>>,
}

impl Clone for GroupCommitWriter {
    fn clone(&self) -> Self {
        Self {
            requests: Arc::clone(&self.requests),
            lane_count: self.lane_count,
            drain_lock: Arc::clone(&self.drain_lock),
            maintenance_state: Arc::clone(&self.maintenance_state),
            maintenance_error: Arc::clone(&self.maintenance_error),
        }
    }
}

impl GroupCommitWriter {
    /// Consume an index handle and start its group-commit worker.
    pub fn new(index: BorsukIndex, config: GroupCommitConfig) -> Result<Self> {
        let config = config.validate()?;
        index.ensure_lane_log_payloads_supported()?;
        let lane_count = index.lane_log_lane_count();
        let minimum_generation = index.lane_log_generation_floor()?;
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
        for (worker, index) in indexes.into_iter().enumerate() {
            let mut lane_writers = Vec::new();
            for lane in (worker..usize::from(lane_count)).step_by(config.worker_lanes) {
                lane_writers.push(index.acquire_lane_log_upsert_writer(
                    u16::try_from(lane).expect("persisted lane fits u16"),
                    current_time_ms()?,
                    LANE_LEASE_TTL_MS,
                    minimum_generation,
                )?);
            }
            let dimensions = index.primary_dimensions();
            let (sender, receiver) = mpsc::channel();
            std::thread::Builder::new()
                .name(format!("borsuk-group-commit-{worker}"))
                .spawn(move || run_worker(index, lane_writers, dimensions, config, receiver))
                .map_err(|error| {
                    BorsukError::InvalidStorage(format!(
                        "failed to start group commit worker {worker}: {error}"
                    ))
                })?;
            requests.push(sender);
        }
        Ok(Self {
            requests: requests.into(),
            lane_count,
            drain_lock: Arc::new(Mutex::new(())),
            maintenance_state: Arc::new(Mutex::new(MaintenanceState::default())),
            maintenance_error: Arc::new(Mutex::new(None)),
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
        let mut by_lane = (0..usize::from(self.lane_count))
            .map(|_| Vec::new())
            .collect::<Vec<Vec<VectorRecord>>>();
        for record in records {
            let lane = lane_for_id(record.id.as_bytes(), usize::from(self.lane_count));
            by_lane[lane].push(record);
        }
        let mut results = Vec::new();
        for (lane, records) in by_lane.into_iter().enumerate() {
            if records.is_empty() {
                continue;
            }
            let (response, result) = mpsc::channel();
            let worker = lane % self.requests.len();
            self.requests[worker]
                .send(WorkerRequest::Append(AppendRequest {
                    lane: u16::try_from(lane).expect("persisted lane fits u16"),
                    records,
                    response,
                }))
                .map_err(|_| {
                    BorsukError::InvalidStorage("group commit worker stopped".to_string())
                })?;
            results.push((lane, result));
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
        let committed_sequences = wait
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
        let mut checkpoints = Vec::with_capacity(usize::from(self.lane_count));
        for (lane, sequence) in committed_sequences.into_iter().enumerate() {
            let (response, wait) = mpsc::channel();
            self.requests[lane % self.requests.len()]
                .send(WorkerRequest::Checkpoint {
                    lane: u16::try_from(lane).expect("persisted lane fits u16"),
                    sequence,
                    response,
                })
                .map_err(|_| {
                    BorsukError::InvalidStorage("group commit worker stopped".to_string())
                })?;
            checkpoints.push(wait);
        }
        for checkpoint in checkpoints {
            checkpoint
                .recv()
                .map_err(|_| {
                    BorsukError::InvalidStorage(
                        "group commit worker stopped before checkpoint completed".to_string(),
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
            WorkerRequest::Checkpoint {
                lane,
                sequence,
                response,
            } => {
                let result = lane_writers
                    .iter_mut()
                    .find(|writer| writer.lane() == lane)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(format!("worker does not own lane {lane}"))
                    })
                    .and_then(|writer| writer.mark_materialized_through(sequence))
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
            let _ = orphaned.response.send(Err(format!(
                "worker does not own lane {}",
                orphaned.lane
            )));
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
                let mut by_id = std::collections::HashMap::<Vec<u8>, usize>::new();
                let mut deduplicated = Vec::with_capacity(combined.len());
                for record in combined {
                    let key = record.id.as_bytes().to_vec();
                    if let Some(index) = by_id.get(&key).copied() {
                        deduplicated[index] = record;
                    } else {
                        by_id.insert(key, deduplicated.len());
                        deduplicated.push(record);
                    }
                }
                let committed_records = deduplicated.len();
                let committed = current_time_ms()
                    .and_then(|now_ms| {
                        writer.append_upsert_records_with_renewal_at(
                            &deduplicated,
                            dimensions,
                            now_ms,
                            LANE_LEASE_TTL_MS,
                        )
                    })
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
                                requests: receipt.requests,
                            })
                        },
                    );
                    let _ = request.response.send(response);
                }
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
