//! Process-local batching facade over the collection's positioned mutation log.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
    },
    time::{Duration, Instant},
};

use crate::{BorsukError, BorsukIndex, CommitSourcePosition, RequestCounts, Result, VectorRecord};

/// Bounds for process-local positioned group commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommitConfig {
    /// Maximum time the first caller waits for concurrent callers to join it.
    pub max_delay: Duration,
    /// Commit as soon as the pending group reaches this many records.
    pub max_records: usize,
    /// Independent process-local batching workers.
    pub workers: usize,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_delay: Duration::from_millis(2),
            max_records: 1_024,
            workers: 8,
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
        if self.workers == 0 || self.workers > 64 {
            return Err(BorsukError::InvalidStorage(
                "group commit workers must be between 1 and 64".to_string(),
            ));
        }
        Ok(self)
    }
}

/// Receipt for one caller whose complete batch is covered by one positioned
/// transaction. Concurrent callers may share the same position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCommitReceipt {
    /// Records supplied by this caller.
    pub records: usize,
    /// Deduplicated records sharing this durable positioned transaction.
    pub committed_records: usize,
    /// Atomic durable source coordinate for the complete caller batch.
    pub position: Option<CommitSourcePosition>,
    /// BLAKE3 checksum of the authorized position-bearing Parquet envelope.
    pub envelope_checksum: String,
    /// Aggregate encoded mutation payload bytes in the shared transaction.
    pub encoded_bytes: u64,
    /// Physical requests issued by the shared positioned append.
    pub requests: RequestCounts,
}

/// One asynchronously submitted append awaiting its positioned receipt.
pub struct GroupCommitTicket {
    records: usize,
    result: Option<Receiver<std::result::Result<GroupCommitReceipt, String>>>,
}

impl GroupCommitTicket {
    /// Wait until the positioned transaction containing this append is durable.
    pub fn wait(self) -> Result<GroupCommitReceipt> {
        let Some(result) = self.result else {
            return Ok(GroupCommitReceipt {
                records: 0,
                committed_records: 0,
                position: None,
                envelope_checksum: String::new(),
                encoded_bytes: 0,
                requests: RequestCounts::default(),
            });
        };
        result
            .recv()
            .map_err(|_| {
                BorsukError::InvalidStorage(
                    "group commit worker stopped before acknowledging append".to_string(),
                )
            })?
            .map_err(BorsukError::InvalidStorage)
            .map(|mut receipt| {
                receipt.records = self.records;
                receipt
            })
    }
}

struct AppendRequest {
    records: Vec<VectorRecord>,
    response: Sender<std::result::Result<GroupCommitReceipt, String>>,
}

enum WorkerRequest {
    Append(AppendRequest),
    Barrier(Sender<()>),
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

/// Cloneable process-local batching facade over positioned transactions.
#[derive(Clone)]
pub struct GroupCommitWriter {
    requests: Arc<[Sender<WorkerRequest>]>,
    next_worker: Arc<AtomicUsize>,
    drain_lock: Arc<Mutex<()>>,
    /// Shared ownership keeps the worker threads alive until the last writer drops.
    _workers: Arc<WorkerThreads>,
}

impl GroupCommitWriter {
    /// Consume an index handle and start positioned batching workers.
    pub fn new(index: BorsukIndex, config: GroupCommitConfig) -> Result<Self> {
        Self::new_with_spawner(index, config, |worker, index, config, requests| {
            std::thread::Builder::new()
                .name(format!("borsuk-positioned-group-{worker}"))
                .spawn(move || run_worker(index, config, requests))
        })
    }

    fn new_with_spawner<F>(
        index: BorsukIndex,
        config: GroupCommitConfig,
        mut spawn: F,
    ) -> Result<Self>
    where
        F: FnMut(
            usize,
            BorsukIndex,
            GroupCommitConfig,
            Receiver<WorkerRequest>,
        ) -> std::io::Result<std::thread::JoinHandle<()>>,
    {
        let config = config.validate()?;
        let mut requests = Vec::with_capacity(config.workers);
        let mut handles = Vec::<std::thread::JoinHandle<()>>::with_capacity(config.workers);
        for worker in 0..config.workers {
            let worker_index = index.clone_for_independent_writer();
            let (sender, receiver) = mpsc::channel();
            let handle = match spawn(worker, worker_index, config, receiver) {
                Ok(handle) => handle,
                Err(error) => {
                    // Every started worker is blocked on its receiver. Close
                    // all sender endpoints before joining any of them.
                    drop(sender);
                    drop(requests);
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(BorsukError::InvalidStorage(format!(
                        "failed to start group commit worker {worker}: {error}"
                    )));
                }
            };
            requests.push(sender);
            handles.push(handle);
        }
        drop(index);
        Ok(Self {
            requests: requests.into(),
            next_worker: Arc::new(AtomicUsize::new(0)),
            drain_lock: Arc::new(Mutex::new(())),
            _workers: Arc::new(WorkerThreads {
                handles: Mutex::new(handles),
            }),
        })
    }

    /// Append one caller batch and wait for its positioned transaction.
    pub fn append(&self, records: Vec<VectorRecord>) -> Result<GroupCommitReceipt> {
        self.append_async(records)?.wait()
    }

    /// Submit one indivisible caller batch. Different callers are distributed
    /// across workers; records from one caller are never split across positions.
    pub fn append_async(&self, records: Vec<VectorRecord>) -> Result<GroupCommitTicket> {
        let record_count = records.len();
        if records.is_empty() {
            return Ok(GroupCommitTicket {
                records: 0,
                result: None,
            });
        }
        let worker = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.requests.len();
        let (response, result) = mpsc::channel();
        self.requests[worker]
            .send(WorkerRequest::Append(AppendRequest { records, response }))
            .map_err(|_| BorsukError::InvalidStorage("group commit worker stopped".to_string()))?;
        Ok(GroupCommitTicket {
            records: record_count,
            result: Some(result),
        })
    }

    /// Wait until every append submitted before this call has completed. Task 4
    /// owns materialization/checkpointing, so this barrier changes no durable state.
    pub fn drain(&self) -> Result<()> {
        let _guard = self.drain_lock.lock().map_err(|_| {
            BorsukError::InvalidStorage("group commit drain lock is poisoned".to_string())
        })?;
        let mut waits = Vec::with_capacity(self.requests.len());
        for requests in self.requests.iter() {
            let (done, wait) = mpsc::channel();
            requests.send(WorkerRequest::Barrier(done)).map_err(|_| {
                BorsukError::InvalidStorage("group commit worker stopped".to_string())
            })?;
            waits.push(wait);
        }
        for wait in waits {
            wait.recv().map_err(|_| {
                BorsukError::InvalidStorage(
                    "group commit worker stopped before drain barrier".to_string(),
                )
            })?;
        }
        Ok(())
    }
}

fn run_worker(
    mut index: BorsukIndex,
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
        };
        let deadline = Instant::now() + config.max_delay;
        let mut group = vec![first];
        let mut record_count = group[0].records.len();
        while record_count < config.max_records {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            match requests.recv_timeout(remaining) {
                Ok(WorkerRequest::Append(request)) => {
                    record_count = record_count.saturating_add(request.records.len());
                    group.push(request);
                }
                Ok(control) => {
                    deferred = Some(control);
                    break;
                }
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
            }
        }

        let mut combined = Vec::with_capacity(record_count);
        for request in &mut group {
            combined.append(&mut request.records);
        }
        let mut by_id = std::collections::HashMap::<Vec<u8>, usize>::new();
        let mut deduplicated = Vec::with_capacity(combined.len());
        for record in combined {
            let key = record.id.as_bytes().to_vec();
            if let Some(ordinal) = by_id.get(&key).copied() {
                deduplicated[ordinal] = record;
            } else {
                by_id.insert(key, deduplicated.len());
                deduplicated.push(record);
            }
        }
        let committed_records = deduplicated.len();
        let committed = index
            .upsert_with_report(deduplicated)
            .and_then(|report| {
                let position = report.positioned_position.ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "grouped mutation completed without a positioned receipt".to_string(),
                    )
                })?;
                Ok(GroupCommitReceipt {
                    records: 0,
                    committed_records,
                    position: Some(position),
                    envelope_checksum: report.positioned_envelope_checksum,
                    encoded_bytes: report.positioned_encoded_bytes,
                    requests: report.requests,
                })
            })
            .map_err(|error| error.to_string());
        for request in group {
            let _ = request.response.send(committed.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IndexConfig, VectorMetric};
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn partial_worker_spawn_failure_drops_senders_before_joining() {
        let index = BorsukIndex::create(IndexConfig {
            uri: "memory:///group-partial-spawn".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1_000,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let exited = Arc::new(AtomicUsize::new(0));
        let exited_by_workers = Arc::clone(&exited);
        let (returned, wait) = mpsc::channel();
        std::thread::spawn(move || {
            let result = GroupCommitWriter::new_with_spawner(
                index,
                GroupCommitConfig {
                    max_delay: Duration::ZERO,
                    max_records: 1,
                    workers: 3,
                },
                move |worker, index, config, requests| {
                    if worker == 2 {
                        return Err(std::io::Error::other("injected spawn failure"));
                    }
                    let exited = Arc::clone(&exited_by_workers);
                    std::thread::Builder::new()
                        .name(format!("borsuk-injected-group-{worker}"))
                        .spawn(move || {
                            run_worker(index, config, requests);
                            exited.fetch_add(1, Ordering::SeqCst);
                        })
                },
            );
            let _ = returned.send(result.map(|_| ()));
        });

        let error = wait
            .recv_timeout(Duration::from_secs(2))
            .expect("partial construction deadlocked while joining live workers")
            .unwrap_err();
        assert!(error.to_string().contains("injected spawn failure"));
        assert_eq!(exited.load(Ordering::SeqCst), 2);
    }
}
