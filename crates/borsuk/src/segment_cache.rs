//! Concurrency helpers for the read path: a shared decoded-segment cache and a
//! search admission gate.
//!
//! Both target the same problem — keeping memory bounded when many readers hit
//! one index at once. The [`DecodedSegmentCache`] lets concurrent queries share
//! a single decoded `Arc<Segment>` for hot segments instead of each decoding
//! its own copy, and the [`AdmissionGate`] caps how many searches run their
//! memory-heavy phase simultaneously so peak memory scales with the permit
//! count rather than the number of caller threads.

use std::collections::HashMap;
use std::sync::{Condvar, Mutex};

use crate::segment::Segment;
use std::sync::Arc;

const CACHE_SHARDS: usize = 16;

/// A byte-bounded, sharded LRU of decoded segments keyed by content checksum.
///
/// Segment checksums are BLAKE3 over the immutable segment bytes, so a checksum
/// hit always refers to identical content — there is no stale-read risk, and
/// segments obsoleted by compaction simply age out through LRU eviction.
#[derive(Debug)]
pub(crate) struct DecodedSegmentCache {
    shards: Vec<Mutex<CacheShard>>,
    shard_budget: u64,
}

#[derive(Debug, Default)]
struct CacheShard {
    entries: HashMap<String, CacheEntry>,
    tick: u64,
    resident_bytes: u64,
}

#[derive(Debug)]
struct CacheEntry {
    segment: Arc<Segment>,
    bytes: u64,
    last_access: u64,
}

impl DecodedSegmentCache {
    /// Create a cache bounded to roughly `max_bytes` of decoded segments,
    /// spread evenly across shards to reduce lock contention under concurrency.
    pub(crate) fn new(max_bytes: u64) -> Self {
        let shards = (0..CACHE_SHARDS)
            .map(|_| Mutex::new(CacheShard::default()))
            .collect();
        Self {
            shards,
            shard_budget: (max_bytes / CACHE_SHARDS as u64).max(1),
        }
    }

    fn shard_for(&self, key: &str) -> &Mutex<CacheShard> {
        // FNV-1a over the checksum string keeps shard selection cheap and stable.
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in key.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        &self.shards[(hash as usize) % self.shards.len()]
    }

    /// Return the shared decoded segment for `checksum` if it is cached.
    pub(crate) fn get(&self, checksum: &str) -> Option<Arc<Segment>> {
        let mut shard = self
            .shard_for(checksum)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shard.tick += 1;
        let tick = shard.tick;
        shard.entries.get_mut(checksum).map(|entry| {
            entry.last_access = tick;
            Arc::clone(&entry.segment)
        })
    }

    /// Insert a decoded segment, evicting least-recently-used entries in the
    /// shard until it is back within budget.
    pub(crate) fn insert(&self, checksum: String, segment: Arc<Segment>, bytes: u64) {
        let mut shard = self
            .shard_for(&checksum)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shard.tick += 1;
        let tick = shard.tick;
        if let Some(previous) = shard.entries.insert(
            checksum,
            CacheEntry {
                segment,
                bytes,
                last_access: tick,
            },
        ) {
            shard.resident_bytes = shard.resident_bytes.saturating_sub(previous.bytes);
        }
        shard.resident_bytes = shard.resident_bytes.saturating_add(bytes);
        while shard.resident_bytes > self.shard_budget && shard.entries.len() > 1 {
            let Some(victim) = shard
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_access)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            if let Some(removed) = shard.entries.remove(&victim) {
                shard.resident_bytes = shard.resident_bytes.saturating_sub(removed.bytes);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn resident_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.lock().unwrap().resident_bytes)
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().unwrap().entries.len())
            .sum()
    }
}

/// Estimate the in-memory footprint of a decoded segment for cache budgeting.
pub(crate) fn decoded_segment_bytes(segment: &Segment) -> u64 {
    let vector_bytes: usize = segment
        .records
        .iter()
        .map(|record| {
            record.vector.len() * std::mem::size_of::<f32>()
                + record.id.as_bytes().len()
                + std::mem::size_of::<crate::record::VectorRecord>()
        })
        .sum();
    let pq_bytes: usize = segment.pq_codes.iter().map(|code| code.len()).sum();
    let routing_bytes = segment.routing_codes.len() * std::mem::size_of::<f32>();
    let centroid_bytes = segment.centroid.len() * std::mem::size_of::<f32>();
    (vector_bytes + pq_bytes + routing_bytes + centroid_bytes) as u64
}

/// A sync counting semaphore that caps how many searches run concurrently.
#[derive(Debug)]
pub(crate) struct AdmissionGate {
    available: Mutex<usize>,
    ready: Condvar,
}

impl AdmissionGate {
    pub(crate) fn new(permits: usize) -> Self {
        Self {
            available: Mutex::new(permits.max(1)),
            ready: Condvar::new(),
        }
    }

    /// Block until a permit is available and return an RAII guard that releases
    /// it on drop.
    pub(crate) fn acquire(&self) -> AdmissionPermit<'_> {
        let mut available = self.available.lock().unwrap_or_else(|e| e.into_inner());
        while *available == 0 {
            available = self
                .ready
                .wait(available)
                .unwrap_or_else(|e| e.into_inner());
        }
        *available -= 1;
        AdmissionPermit { gate: self }
    }

    fn release(&self) {
        let mut available = self.available.lock().unwrap_or_else(|e| e.into_inner());
        *available += 1;
        drop(available);
        self.ready.notify_one();
    }
}

/// Releases its admission permit back to the gate when dropped.
pub(crate) struct AdmissionPermit<'a> {
    gate: &'a AdmissionGate,
}

impl Drop for AdmissionPermit<'_> {
    fn drop(&mut self) {
        self.gate.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::VectorMetric;
    use crate::record::VectorRecord;
    use crate::segment::Segment;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    fn sample_segment(id: &str) -> Arc<Segment> {
        Arc::new(
            Segment::from_records(
                id.to_string(),
                0,
                VectorMetric::Euclidean,
                2,
                vec![
                    VectorRecord::new("a", vec![0.0, 0.0]),
                    VectorRecord::new("b", vec![1.0, 0.0]),
                ],
            )
            .unwrap(),
        )
    }

    #[test]
    fn decoded_cache_shares_and_evicts_by_budget() {
        let one = sample_segment("one");
        let bytes = decoded_segment_bytes(&one);
        // Budget for ~1.5 segments so the second insert evicts the first.
        let cache = DecodedSegmentCache::new(bytes * CACHE_SHARDS as u64 + 1);

        assert!(cache.get("c1").is_none());
        cache.insert("c1".to_string(), Arc::clone(&one), bytes);
        let hit = cache.get("c1").expect("c1 should be cached");
        // The cache returns the same allocation, not a fresh decode.
        assert!(Arc::ptr_eq(&hit, &one));
        assert!(Arc::strong_count(&one) >= 2);

        // Insert enough distinct entries into the same shard-space to exceed the
        // per-shard budget; total resident bytes must stay within the budget.
        for i in 0..64 {
            let seg = sample_segment(&format!("seg-{i}"));
            let b = decoded_segment_bytes(&seg);
            cache.insert(format!("k{i}"), seg, b);
        }
        assert!(
            cache.resident_bytes() <= (bytes * CACHE_SHARDS as u64 + 1),
            "resident {} must stay within budget",
            cache.resident_bytes()
        );
        assert!(cache.len() >= 1);
    }

    #[test]
    fn admission_gate_never_exceeds_permits() {
        let permits = 3;
        let gate = Arc::new(AdmissionGate::new(permits));
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..32)
            .map(|_| {
                let gate = Arc::clone(&gate);
                let live = Arc::clone(&live);
                let peak = Arc::clone(&peak);
                thread::spawn(move || {
                    let _permit = gate.acquire();
                    let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    thread::sleep(std::time::Duration::from_millis(2));
                    live.fetch_sub(1, Ordering::SeqCst);
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        assert!(
            peak.load(Ordering::SeqCst) <= permits,
            "peak concurrency {} exceeded permits {}",
            peak.load(Ordering::SeqCst),
            permits
        );
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }
}
