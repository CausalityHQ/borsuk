//! Process-local group commit for high-throughput object-store ingest.

use std::{
    sync::{
        Arc,
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
    },
    time::{Duration, Instant},
};

use crate::{BorsukError, BorsukIndex, RequestCounts, Result, VectorRecord};

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
            worker_lanes: 1,
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
    /// Process-local lane ordinal; paired with `commit_sequence`, this uniquely
    /// identifies the durable group.
    pub commit_lane: usize,
    /// Physical requests issued by the whole shared commit.
    pub requests: RequestCounts,
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
    /// Aggregate physical requests issued by this call's lane commits.
    pub requests: RequestCounts,
    /// One durable receipt for every ownership lane touched by this call.
    pub lane_receipts: Vec<GroupCommitLaneReceipt>,
}

/// One asynchronously submitted append awaiting its durable group receipt.
pub struct GroupCommitTicket {
    records: usize,
    results: Vec<Receiver<std::result::Result<GroupCommitLaneReceipt, String>>>,
}

impl GroupCommitTicket {
    /// Wait until the shared WAL transaction containing this append is durable.
    pub fn wait(self) -> Result<GroupCommitReceipt> {
        let mut lane_receipts = Vec::with_capacity(self.results.len());
        for result in self.results {
            let receipt = result.recv().map_err(|_| {
                BorsukError::InvalidStorage(
                    "group commit worker stopped before acknowledging append".to_string(),
                )
            })?;
            lane_receipts.push(receipt.map_err(BorsukError::InvalidStorage)?);
        }
        lane_receipts.sort_by_key(|receipt| receipt.commit_lane);
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
        let (commit_lane, commit_sequence) = lane_receipts.first().map_or((0, 0), |receipt| {
            (receipt.commit_lane, receipt.commit_sequence)
        });
        Ok(GroupCommitReceipt {
            records: self.records,
            committed_records,
            commit_sequence,
            commit_lane,
            requests,
            lane_receipts,
        })
    }
}

struct AppendRequest {
    records: Vec<VectorRecord>,
    response: Sender<std::result::Result<GroupCommitLaneReceipt, String>>,
}

enum WorkerRequest {
    Append(AppendRequest),
    Barrier(Sender<()>),
    Drain(Sender<std::result::Result<(), String>>),
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
}

impl Clone for GroupCommitWriter {
    fn clone(&self) -> Self {
        Self {
            requests: Arc::clone(&self.requests),
        }
    }
}

impl GroupCommitWriter {
    /// Consume an index handle and start its group-commit worker.
    pub fn new(index: BorsukIndex, config: GroupCommitConfig) -> Result<Self> {
        let config = config.validate()?;
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
        for (lane, index) in indexes.into_iter().enumerate() {
            let (sender, receiver) = mpsc::channel();
            std::thread::Builder::new()
                .name(format!("borsuk-group-commit-{lane}"))
                .spawn(move || run_worker(index, config, lane, receiver))
                .map_err(|error| {
                    BorsukError::InvalidStorage(format!(
                        "failed to start group commit worker lane {lane}: {error}"
                    ))
                })?;
            requests.push(sender);
        }
        Ok(Self {
            requests: requests.into(),
        })
    }

    /// Append records and wait for their shared durable commit.
    pub fn append(&self, records: Vec<VectorRecord>) -> Result<GroupCommitReceipt> {
        self.append_async(records)?.wait()
    }

    /// Submit records without blocking, allowing one producer to keep several
    /// durable appends in flight and therefore participate in group commit.
    pub fn append_async(&self, records: Vec<VectorRecord>) -> Result<GroupCommitTicket> {
        let record_count = records.len();
        if records.is_empty() {
            return Ok(GroupCommitTicket {
                records: 0,
                results: Vec::new(),
            });
        }
        let mut by_lane = (0..self.requests.len())
            .map(|_| Vec::new())
            .collect::<Vec<Vec<VectorRecord>>>();
        for record in records {
            let lane = lane_for_id(record.id.as_bytes(), self.requests.len());
            by_lane[lane].push(record);
        }
        let mut results = Vec::new();
        for (lane, records) in by_lane.into_iter().enumerate() {
            if records.is_empty() {
                continue;
            }
            let (response, result) = mpsc::channel();
            self.requests[lane]
                .send(WorkerRequest::Append(AppendRequest { records, response }))
                .map_err(|_| {
                    BorsukError::InvalidStorage("group commit worker stopped".to_string())
                })?;
            results.push(result);
        }
        Ok(GroupCommitTicket {
            records: record_count,
            results,
        })
    }

    /// Materialize and retire every group acknowledged before this call.
    pub fn drain(&self) -> Result<()> {
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
            .send(WorkerRequest::Drain(done))
            .map_err(|_| BorsukError::InvalidStorage("group commit worker stopped".to_string()))?;
        wait.recv()
            .map_err(|_| {
                BorsukError::InvalidStorage(
                    "group commit worker stopped before drain completed".to_string(),
                )
            })?
            .map_err(BorsukError::InvalidStorage)
    }
}

fn lane_for_id(id: &[u8], lane_count: usize) -> usize {
    let digest = blake3::hash(id);
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&digest.as_bytes()[..8]);
    (u64::from_le_bytes(prefix) % lane_count as u64) as usize
}

fn run_worker(
    mut index: BorsukIndex,
    config: GroupCommitConfig,
    commit_lane: usize,
    requests: Receiver<WorkerRequest>,
) {
    let mut commit_sequence = 0_u64;
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
            WorkerRequest::Drain(done) => {
                let result = index
                    .refresh()
                    .and_then(|_| index.flush())
                    .and_then(|_| index.optimize_drained_reads())
                    .map_err(|error| error.to_string());
                let _ = done.send(result);
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

        let mut combined = Vec::with_capacity(records);
        let caller_sizes = group
            .iter_mut()
            .map(|request| {
                let len = request.records.len();
                combined.append(&mut request.records);
                len
            })
            .collect::<Vec<_>>();
        commit_sequence = commit_sequence.saturating_add(1);
        let committed = index
            .group_commit_add(combined)
            .map_err(|error| error.to_string());
        for (request, caller_records) in group.into_iter().zip(caller_sizes) {
            let response = committed.as_ref().map_or_else(
                |error| Err(error.clone()),
                |requests| {
                    Ok(GroupCommitLaneReceipt {
                        records: caller_records,
                        committed_records: records,
                        commit_sequence,
                        commit_lane,
                        requests: *requests,
                    })
                },
            );
            let _ = request.response.send(response);
        }
    }
}
