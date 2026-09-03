use std::{
    path::Path,
    sync::{Condvar, Mutex},
    time::{Duration, Instant},
};

use rayon::prelude::*;

use crate::{BorsukError, Result, core::rank_candidates, snapshot::Pq4Snapshot};

const CANDIDATE_DEPTH: usize = 3_072;

/// Local shard opening and bounded-concurrency policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pq4OpenOptions {
    /// Stable deployment ordinal used by deterministic cross-shard ties.
    pub shard_ordinal: u32,
    /// Total bytes available to the owned code plane and concurrent searches.
    pub memory_budget_bytes: u64,
    /// Maximum concurrent searches admitted to the dedicated Rayon pool.
    pub query_threads: usize,
    /// Maximum time a caller waits for scratch admission.
    pub admission_timeout_ms: u64,
}

/// One exact-reranked local shard match.
#[derive(Debug, Clone, PartialEq)]
pub struct Pq4Match {
    /// Opaque source ID bytes.
    pub id: Vec<u8>,
    /// Squared L2 distance between unit-normalized vectors.
    pub squared_distance: f32,
    /// Stable source-order ordinal used for deterministic ties.
    pub source_ordinal: u64,
    /// Stable shard ordinal used by deterministic global merging.
    pub shard_ordinal: u32,
}

struct Admission {
    available: Mutex<usize>,
    wake: Condvar,
    timeout: Duration,
}

struct Permit<'a> {
    admission: &'a Admission,
}

impl Admission {
    fn acquire(&self) -> Result<Permit<'_>> {
        let deadline = Instant::now() + self.timeout;
        let mut available = self
            .available
            .lock()
            .map_err(|_| invalid("PQ4 admission lock is poisoned"))?;
        while *available == 0 {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| invalid("PQ4 admission timed out"))?;
            let (next, timed) = self
                .wake
                .wait_timeout(available, remaining)
                .map_err(|_| invalid("PQ4 admission lock is poisoned"))?;
            available = next;
            if timed.timed_out() && *available == 0 {
                return Err(invalid("PQ4 admission timed out"));
            }
        }
        *available -= 1;
        Ok(Permit { admission: self })
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        if let Ok(mut available) = self.admission.available.lock() {
            *available += 1;
            self.admission.wake.notify_one();
        }
    }
}

/// Authenticated local PQ4 shard with bounded exact-row search.
pub struct Pq4Index {
    snapshot: Pq4Snapshot,
    pool: rayon::ThreadPool,
    admission: Admission,
    shard_ordinal: u32,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn normalized(query: &[f32; 96]) -> Result<[f32; 96]> {
    if query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("PQ4 query must be finite and nonzero"));
    }
    let norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
    if !norm.is_finite() || norm <= 0.0 {
        return Err(invalid("PQ4 query must be finite and nonzero"));
    }
    Ok(query.map(|value| value / norm))
}

fn query_scratch_bytes(row_count: u64) -> Result<u64> {
    let histogram_chunks = row_count.div_ceil(8_192 * 32);
    row_count
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(histogram_chunks.checked_mul(8_192 * 4)?))
        .and_then(|bytes| bytes.checked_add(CANDIDATE_DEPTH as u64 * (16 + 96 * 4)))
        .ok_or_else(|| invalid("PQ4 query scratch projection overflows"))
}

impl Pq4Index {
    /// Authenticate and open one local shard without any network or page surface.
    pub fn open(directory: &Path, options: Pq4OpenOptions) -> Result<Self> {
        if options.query_threads == 0
            || options.query_threads > 256
            || options.admission_timeout_ms == 0
        {
            return Err(invalid("PQ4 open options differ"));
        }
        let snapshot = Pq4Snapshot::open(directory)?;
        let row_count = snapshot.row_count();
        let resident = u64::try_from(snapshot.blocks().len())
            .unwrap()
            .checked_mul(512)
            .and_then(|bytes| bytes.checked_add(32 * 16 * 3 * 4))
            .ok_or_else(|| invalid("PQ4 resident projection overflows"))?;
        let scratch = query_scratch_bytes(row_count)?;
        let capacity = options
            .memory_budget_bytes
            .checked_sub(resident)
            .map(|bytes| bytes / scratch)
            .unwrap_or(0)
            .min(u64::try_from(options.query_threads).unwrap());
        if capacity == 0 {
            return Err(invalid("PQ4 memory budget cannot admit one query"));
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(options.query_threads)
            .thread_name(|index| format!("pq4-query-{index}"))
            .build()
            .map_err(|error| invalid(&format!("PQ4 query pool failed: {error}")))?;
        Ok(Self {
            snapshot,
            pool,
            admission: Admission {
                available: Mutex::new(usize::try_from(capacity).unwrap()),
                wake: Condvar::new(),
                timeout: Duration::from_millis(options.admission_timeout_ms),
            },
            shard_ordinal: options.shard_ordinal,
        })
    }

    /// Search one shard and return exact rows in deterministic distance/source order.
    pub fn search(&self, query: &[f32; 96], k: usize) -> Result<Vec<Pq4Match>> {
        self.search_with_exact_rerank_observer(query, k, &|| {})
    }

    fn search_with_exact_rerank_observer<F>(
        &self,
        query: &[f32; 96],
        k: usize,
        observe: &F,
    ) -> Result<Vec<Pq4Match>>
    where
        F: Fn() + Sync,
    {
        if k == 0 || k > CANDIDATE_DEPTH {
            return Err(invalid("PQ4 result count differs"));
        }
        let query = normalized(query)?;
        let _permit = self.admission.acquire()?;
        let candidates = self.pool.install(|| {
            rank_candidates(
                self.snapshot.codebook(),
                self.snapshot.blocks(),
                usize::try_from(self.snapshot.row_count()).unwrap(),
                &query,
                CANDIDATE_DEPTH,
            )
        })?;
        let mut exact = self.pool.install(|| {
            candidates
                .into_par_iter()
                .map(|candidate| {
                    observe();
                    let vector = self.snapshot.read_vector(candidate.source_ordinal)?;
                    let distance = vector
                        .iter()
                        .zip(query)
                        .map(|(left, right)| {
                            let delta = left - right;
                            delta * delta
                        })
                        .sum::<f32>();
                    if !distance.is_finite() {
                        return Err(invalid("PQ4 exact distance differs"));
                    }
                    Ok((distance, candidate.source_ordinal))
                })
                .collect::<Result<Vec<_>>>()
        })?;
        exact.sort_unstable_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        exact.truncate(k);
        exact
            .into_iter()
            .map(|(squared_distance, source_ordinal)| {
                Ok(Pq4Match {
                    id: self.snapshot.read_id(source_ordinal)?,
                    squared_distance,
                    source_ordinal,
                    shard_ordinal: self.shard_ordinal,
                })
            })
            .collect()
    }
}

#[cfg(test)]
pub(crate) fn search_with_exact_rerank_observer_for_test<F>(
    index: &Pq4Index,
    query: &[f32; 96],
    k: usize,
    observe: F,
) -> Result<Vec<Pq4Match>>
where
    F: Fn() + Sync,
{
    index.search_with_exact_rerank_observer(query, k, &observe)
}
