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
use std::time::Instant;

use crate::Result;
use crate::segment::{GraphEdge, Segment, SegmentGraph};
use std::sync::{Arc, Weak};

const CACHE_SHARDS: usize = 16;

/// Coalesce reads of one immutable object while any caller is still using the
/// decoded allocation, without retaining it after the last caller drops it.
///
/// This differs deliberately from [`DecodedSegmentCache`]: it prevents a burst
/// of users from fetching and decoding the same checksum N times, but does not
/// turn those decoded cells into a resident cache. A later, non-overlapping
/// request loads the object again unless the explicitly budgeted decoded cache
/// is enabled.
#[derive(Debug)]
pub(crate) struct InFlightReads<T> {
    flights: Mutex<HashMap<String, Arc<ReadFlight<T>>>>,
}

#[derive(Debug)]
struct ReadFlight<T> {
    state: Mutex<ReadFlightState<T>>,
    ready: Condvar,
}

#[derive(Debug)]
enum ReadFlightState<T> {
    Loading,
    Ready(Weak<T>),
    Failed,
}

impl<T> Default for InFlightReads<T> {
    fn default() -> Self {
        Self {
            flights: Mutex::new(HashMap::new()),
        }
    }
}

pub(crate) type InFlightSegmentReads = InFlightReads<Segment>;
pub(crate) type InFlightGraphReads = InFlightReads<SegmentGraph>;

/// Byte-bounded shared cache for immutable decoded objects whose traversal
/// state remains query-local. This is used by global cell graphs so concurrent
/// users share adjacency/code storage without allowing the corpus-sized graph
/// working set to become resident.
#[derive(Debug)]
pub(crate) struct DecodedObjectCache<T> {
    shards: Vec<Mutex<ObjectCacheShard<T>>>,
    shard_budget: u64,
    retained_pool: Option<Arc<RetainedBytePool>>,
}

#[derive(Debug)]
struct ObjectCacheShard<T> {
    entries: HashMap<String, ObjectCacheEntry<T>>,
    tick: u64,
    resident_bytes: u64,
}

impl<T> Default for ObjectCacheShard<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            tick: 0,
            resident_bytes: 0,
        }
    }
}

#[derive(Debug)]
struct ObjectCacheEntry<T> {
    value: Arc<T>,
    bytes: u64,
    last_access: u64,
    _retained: Option<RetainedBytePermit>,
}

impl<T> DecodedObjectCache<T> {
    pub(crate) fn new(max_bytes: u64) -> Self {
        Self::new_with_optional_pool(max_bytes, None)
    }

    pub(crate) fn new_with_pool(max_bytes: u64, retained_pool: Arc<RetainedBytePool>) -> Self {
        Self::new_with_optional_pool(max_bytes, Some(retained_pool))
    }

    fn new_with_optional_pool(
        max_bytes: u64,
        retained_pool: Option<Arc<RetainedBytePool>>,
    ) -> Self {
        Self {
            shards: (0..CACHE_SHARDS)
                .map(|_| Mutex::new(ObjectCacheShard::default()))
                .collect(),
            shard_budget: if max_bytes == 0 {
                0
            } else {
                (max_bytes / CACHE_SHARDS as u64).max(1)
            },
            retained_pool,
        }
    }

    fn shard_for(&self, key: &str) -> &Mutex<ObjectCacheShard<T>> {
        let hash = key.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3)
        });
        &self.shards[hash as usize % self.shards.len()]
    }

    pub(crate) fn get(&self, checksum: &str) -> Option<Arc<T>> {
        let mut shard = self
            .shard_for(checksum)
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        shard.tick = shard.tick.saturating_add(1);
        let tick = shard.tick;
        shard.entries.get_mut(checksum).map(|entry| {
            entry.last_access = tick;
            Arc::clone(&entry.value)
        })
    }

    pub(crate) fn insert(&self, checksum: String, value: Arc<T>, bytes: u64) {
        let mut shard = self
            .shard_for(&checksum)
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        shard.tick = shard.tick.saturating_add(1);
        let tick = shard.tick;
        if let Some(previous) = shard.entries.remove(&checksum) {
            shard.resident_bytes = shard.resident_bytes.saturating_sub(previous.bytes);
            drop(previous);
        }
        if bytes == 0 || bytes > self.shard_budget {
            return;
        }
        while shard.resident_bytes.saturating_add(bytes) > self.shard_budget {
            let Some(oldest) = Self::oldest_key(&shard) else {
                return;
            };
            Self::remove_entry(&mut shard, &oldest);
        }
        let retained = if let Some(pool) = &self.retained_pool {
            loop {
                if let Some(permit) = pool.try_reserve(bytes) {
                    break Some(permit);
                }
                let Some(oldest) = Self::oldest_key(&shard) else {
                    return;
                };
                Self::remove_entry(&mut shard, &oldest);
            }
        } else {
            None
        };
        shard.resident_bytes = shard.resident_bytes.saturating_add(bytes);
        shard.entries.insert(
            checksum,
            ObjectCacheEntry {
                value,
                bytes,
                last_access: tick,
                _retained: retained,
            },
        );
    }

    pub(crate) fn remove(&self, checksum: &str) {
        let mut shard = self
            .shard_for(checksum)
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        Self::remove_entry(&mut shard, checksum);
    }

    fn oldest_key(shard: &ObjectCacheShard<T>) -> Option<String> {
        shard
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(key, _)| key.clone())
    }

    fn remove_entry(shard: &mut ObjectCacheShard<T>, key: &str) {
        if let Some(removed) = shard.entries.remove(key) {
            shard.resident_bytes = shard.resident_bytes.saturating_sub(removed.bytes);
        }
    }

    #[cfg(test)]
    pub(crate) fn resident_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .resident_bytes
            })
            .sum()
    }
}

impl<T> InFlightReads<T> {
    /// Load `checksum` once for callers whose load or consumption overlaps.
    ///
    /// The leader reports the physical bytes read; followers report zero so
    /// aggregate query telemetry does not multiply one object-store request.
    pub(crate) fn load<F>(&self, checksum: &str, loader: F) -> Result<(Arc<T>, u64, bool)>
    where
        F: FnOnce() -> Result<(T, u64)>,
    {
        let mut loader = Some(loader);
        loop {
            let (flight, leader) = {
                let mut flights = self.flights.lock().unwrap_or_else(|e| e.into_inner());
                if flights.len() >= 1024 {
                    flights.retain(|_, flight| {
                        let state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
                        match &*state {
                            ReadFlightState::Loading => true,
                            ReadFlightState::Ready(value) => value.strong_count() > 0,
                            ReadFlightState::Failed => false,
                        }
                    });
                }
                if let Some(flight) = flights.get(checksum) {
                    (Arc::clone(flight), false)
                } else {
                    let flight = Arc::new(ReadFlight {
                        state: Mutex::new(ReadFlightState::Loading),
                        ready: Condvar::new(),
                    });
                    flights.insert(checksum.to_string(), Arc::clone(&flight));
                    (flight, true)
                }
            };

            if leader {
                let loader = loader
                    .take()
                    .expect("single-flight leader must own its loader");
                let result = loader();
                match result {
                    Ok((value, bytes)) => {
                        let value = Arc::new(value);
                        let mut state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
                        *state = ReadFlightState::Ready(Arc::downgrade(&value));
                        drop(state);
                        flight.ready.notify_all();
                        return Ok((value, bytes, false));
                    }
                    Err(error) => {
                        let mut state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
                        *state = ReadFlightState::Failed;
                        drop(state);
                        flight.ready.notify_all();
                        self.remove(checksum, &flight);
                        return Err(error);
                    }
                }
            }

            let mut state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
            while matches!(*state, ReadFlightState::Loading) {
                state = flight.ready.wait(state).unwrap_or_else(|e| e.into_inner());
            }
            match &*state {
                ReadFlightState::Ready(value) => {
                    if let Some(value) = value.upgrade() {
                        return Ok((value, 0, true));
                    }
                    drop(state);
                    self.remove(checksum, &flight);
                }
                ReadFlightState::Failed => {
                    // The failed leader removed its flight. Retry using this
                    // caller's untouched loader and become the next leader.
                    drop(state);
                }
                ReadFlightState::Loading => unreachable!("wait loop exits only when ready"),
            }
        }
    }

    fn remove(&self, checksum: &str, completed: &Arc<ReadFlight<T>>) {
        let mut flights = self.flights.lock().unwrap_or_else(|e| e.into_inner());
        if flights
            .get(checksum)
            .is_some_and(|current| Arc::ptr_eq(current, completed))
        {
            flights.remove(checksum);
        }
    }
}

/// A byte-bounded, sharded LRU of decoded segments keyed by content checksum.
///
/// Segment checksums are BLAKE3 over the immutable segment bytes, so a checksum
/// hit always refers to identical content — there is no stale-read risk, and
/// segments obsoleted by compaction simply age out through LRU eviction.
#[derive(Debug)]
pub(crate) struct DecodedSegmentCache {
    shards: Vec<Mutex<CacheShard>>,
    shard_budget: u64,
    retained_pool: Option<Arc<RetainedBytePool>>,
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
    graph: Option<Arc<SegmentGraph>>,
    graph_checksum: Option<String>,
    graph_bytes: u64,
    bytes: u64,
    last_access: u64,
    pinned: bool,
    retained: Option<RetainedBytePermit>,
}

impl DecodedSegmentCache {
    /// Create a cache bounded to roughly `max_bytes` of decoded segments,
    /// spread evenly across shards to reduce lock contention under concurrency.
    pub(crate) fn new(max_bytes: u64) -> Self {
        Self::new_with_optional_pool(max_bytes, None)
    }

    pub(crate) fn new_with_pool(max_bytes: u64, retained_pool: Arc<RetainedBytePool>) -> Self {
        Self::new_with_optional_pool(max_bytes, Some(retained_pool))
    }

    fn new_with_optional_pool(
        max_bytes: u64,
        retained_pool: Option<Arc<RetainedBytePool>>,
    ) -> Self {
        let shards = (0..CACHE_SHARDS)
            .map(|_| Mutex::new(CacheShard::default()))
            .collect();
        Self {
            shards,
            shard_budget: if max_bytes == 0 {
                0
            } else {
                (max_bytes / CACHE_SHARDS as u64).max(1)
            },
            retained_pool,
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

    /// Return whether a decoded segment is resident without changing its LRU
    /// position. Search planning uses this to distinguish an empty optional
    /// cache from an explicitly warmed, complete working set.
    pub(crate) fn contains(&self, checksum: &str) -> bool {
        self.shard_for(checksum)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .contains_key(checksum)
    }

    /// Return whether both a segment and its exact immutable graph version are
    /// resident. Planning uses this as a no-I/O coverage check before selecting
    /// the cache-only graph engine.
    pub(crate) fn contains_graph(&self, segment_checksum: &str, graph_checksum: &str) -> bool {
        self.shard_for(segment_checksum)
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .get(segment_checksum)
            .is_some_and(|entry| {
                entry.graph.is_some() && entry.graph_checksum.as_deref() == Some(graph_checksum)
            })
    }

    /// Return the shared decoded segment for `checksum` if it is cached.
    #[cfg(test)]
    pub(crate) fn get(&self, checksum: &str) -> Option<Arc<Segment>> {
        self.get_with_pin(checksum, false)
    }

    /// Return a cached segment and optionally pin it against budget eviction.
    pub(crate) fn get_with_pin(&self, checksum: &str, pin: bool) -> Option<Arc<Segment>> {
        let mut shard = self
            .shard_for(checksum)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shard.tick += 1;
        let tick = shard.tick;
        shard.entries.get_mut(checksum).map(|entry| {
            entry.last_access = tick;
            entry.pinned |= pin;
            Arc::clone(&entry.segment)
        })
    }

    /// Return a graph already decoded and validated for the cached segment.
    /// Graphs share the segment entry's LRU and pin state, so enabling graph
    /// reuse cannot bypass the caller's decoded-cache byte budget.
    pub(crate) fn get_graph(
        &self,
        segment_checksum: &str,
        graph_checksum: &str,
    ) -> Option<Arc<SegmentGraph>> {
        let mut shard = self
            .shard_for(segment_checksum)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shard.tick += 1;
        let tick = shard.tick;
        shard.entries.get_mut(segment_checksum).and_then(|entry| {
            entry.last_access = tick;
            (entry.graph_checksum.as_deref() == Some(graph_checksum))
                .then(|| entry.graph.as_ref().map(Arc::clone))
                .flatten()
        })
    }

    /// Insert a decoded segment, evicting least-recently-used entries in the
    /// shard until it is back within budget.
    pub(crate) fn insert(&self, checksum: String, segment: Arc<Segment>, bytes: u64) {
        self.insert_with_pin(checksum, segment, bytes, false);
    }

    /// Insert a decoded segment and optionally pin it against budget eviction.
    pub(crate) fn insert_with_pin(
        &self,
        checksum: String,
        segment: Arc<Segment>,
        bytes: u64,
        pin: bool,
    ) {
        let mut shard = self
            .shard_for(&checksum)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shard.tick += 1;
        let tick = shard.tick;
        let previous = shard.entries.remove(&checksum);
        let previous_bytes = previous.as_ref().map_or(0, |entry| entry.bytes);
        let pin = pin || previous.as_ref().is_some_and(|entry| entry.pinned);
        let graph = previous
            .as_ref()
            .and_then(|entry| entry.graph.as_ref().map(Arc::clone));
        let graph_checksum = previous
            .as_ref()
            .and_then(|entry| entry.graph_checksum.clone());
        let graph_bytes = previous.as_ref().map_or(0, |entry| entry.graph_bytes);
        let bytes = bytes.saturating_add(graph_bytes);
        shard.resident_bytes = shard.resident_bytes.saturating_sub(previous_bytes);
        if bytes == 0 || bytes > self.shard_budget {
            return;
        }
        while shard.resident_bytes.saturating_add(bytes) > self.shard_budget {
            let Some(victim) = Self::oldest_unpinned_key(&shard, None) else {
                return;
            };
            Self::remove_entry(&mut shard, &victim);
        }
        let retained = if let Some(pool) = &self.retained_pool {
            loop {
                if let Some(permit) = pool.try_reserve(bytes) {
                    break Some(permit);
                }
                let Some(victim) = Self::oldest_unpinned_key(&shard, None) else {
                    return;
                };
                Self::remove_entry(&mut shard, &victim);
            }
        } else {
            None
        };
        shard.entries.insert(
            checksum,
            CacheEntry {
                segment,
                graph,
                graph_checksum,
                graph_bytes,
                bytes,
                last_access: tick,
                pinned: pin,
                retained,
            },
        );
        shard.resident_bytes = shard.resident_bytes.saturating_add(bytes);
    }

    /// Attach a decoded graph to an existing segment cache entry. Returns false
    /// if the segment was evicted between its read and the graph decode.
    pub(crate) fn insert_graph(
        &self,
        segment_checksum: &str,
        graph_checksum: String,
        graph: Arc<SegmentGraph>,
        graph_bytes: u64,
    ) -> bool {
        let mut shard = self
            .shard_for(segment_checksum)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shard.tick += 1;
        let tick = shard.tick;
        let Some(current) = shard.entries.get(segment_checksum) else {
            return false;
        };
        let old_graph_bytes = current.graph_bytes;
        let previous_bytes = current.bytes;
        let next_bytes = previous_bytes
            .saturating_sub(old_graph_bytes)
            .saturating_add(graph_bytes);
        if next_bytes == 0 || next_bytes > self.shard_budget {
            return false;
        }
        while shard
            .resident_bytes
            .saturating_sub(previous_bytes)
            .saturating_add(next_bytes)
            > self.shard_budget
        {
            let Some(victim) = Self::oldest_unpinned_key(&shard, Some(segment_checksum)) else {
                return false;
            };
            Self::remove_entry(&mut shard, &victim);
        }
        if self.retained_pool.is_some() {
            loop {
                let resized = shard
                    .entries
                    .get_mut(segment_checksum)
                    .and_then(|entry| entry.retained.as_mut())
                    .is_some_and(|permit| permit.try_resize(next_bytes));
                if resized {
                    break;
                }
                let Some(victim) = Self::oldest_unpinned_key(&shard, Some(segment_checksum)) else {
                    return false;
                };
                Self::remove_entry(&mut shard, &victim);
            }
        }
        let entry = shard
            .entries
            .get_mut(segment_checksum)
            .expect("protected segment cache entry remains present");
        entry.graph = Some(graph);
        entry.graph_checksum = Some(graph_checksum);
        entry.graph_bytes = graph_bytes;
        entry.bytes = next_bytes;
        entry.last_access = tick;
        shard.resident_bytes = shard
            .resident_bytes
            .saturating_sub(previous_bytes)
            .saturating_add(next_bytes);
        true
    }

    fn oldest_unpinned_key(shard: &CacheShard, exclude: Option<&str>) -> Option<String> {
        shard
            .entries
            .iter()
            .filter(|(key, entry)| !entry.pinned && exclude != Some(key.as_str()))
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(key, _)| key.clone())
    }

    fn remove_entry(shard: &mut CacheShard, key: &str) {
        if let Some(removed) = shard.entries.remove(key) {
            shard.resident_bytes = shard.resident_bytes.saturating_sub(removed.bytes);
        }
    }

    pub(crate) fn resident_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .resident_bytes
            })
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .entries
                    .len()
            })
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

/// Estimate the retained heap footprint of a decoded segment graph.
pub(crate) fn decoded_graph_bytes(graph: &SegmentGraph) -> u64 {
    (std::mem::size_of::<SegmentGraph>()
        + graph.segment_id.capacity()
        + graph.edges.capacity() * std::mem::size_of::<GraphEdge>()
        + graph.adjacency_offsets.capacity() * std::mem::size_of::<usize>()) as u64
}

/// A sync counting semaphore that caps how many searches run concurrently.
#[derive(Debug)]
pub(crate) struct AdmissionGate {
    state: Mutex<AdmissionState>,
    ready: Condvar,
}

#[derive(Debug)]
struct AdmissionState {
    permits: u64,
    max_waiters: Option<u64>,
    next_ticket: u64,
    released: u64,
    admitted: u64,
    rejected: u64,
    wait_nanos: u64,
    wait_count: u64,
    peak_active: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AdmissionSnapshot {
    pub(crate) capacity: usize,
    pub(crate) active: usize,
    pub(crate) waiting: usize,
    pub(crate) admitted: u64,
    pub(crate) rejected: u64,
    pub(crate) wait_micros: u64,
    pub(crate) wait_count: u64,
    pub(crate) peak_active: usize,
}

impl AdmissionGate {
    pub(crate) fn new(permits: usize) -> Self {
        Self {
            state: Mutex::new(AdmissionState {
                permits: permits.max(1) as u64,
                max_waiters: None,
                next_ticket: 0,
                released: 0,
                admitted: 0,
                rejected: 0,
                wait_nanos: 0,
                wait_count: 0,
                peak_active: 0,
            }),
            ready: Condvar::new(),
        }
    }

    pub(crate) fn new_bounded(permits: usize, max_waiters: usize) -> Self {
        Self {
            state: Mutex::new(AdmissionState {
                permits: permits.max(1) as u64,
                max_waiters: Some(max_waiters as u64),
                next_ticket: 0,
                released: 0,
                admitted: 0,
                rejected: 0,
                wait_nanos: 0,
                wait_count: 0,
                peak_active: 0,
            }),
            ready: Condvar::new(),
        }
    }

    /// Block until a permit is available and return an RAII guard that releases
    /// it on drop. Tickets make admission FIFO: an active caller cannot release
    /// and immediately reacquire ahead of callers that are already waiting.
    pub(crate) fn acquire(&self) -> AdmissionPermit<'_> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.acquire_locked(state);
        AdmissionPermit { gate: self }
    }

    /// Join the bounded FIFO queue, or reject immediately when every active
    /// and waiting slot is already occupied.
    pub(crate) fn acquire_bounded(&self) -> Option<AdmissionPermit<'_>> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let outstanding = state.next_ticket.saturating_sub(state.released);
        if state
            .max_waiters
            .is_some_and(|max_waiters| outstanding >= state.permits.saturating_add(max_waiters))
        {
            state.rejected = state.rejected.saturating_add(1);
            return None;
        }
        self.acquire_locked(state);
        Some(AdmissionPermit { gate: self })
    }

    fn acquire_locked<'a>(&'a self, mut state: std::sync::MutexGuard<'a, AdmissionState>) {
        let started = Instant::now();
        let mut waited = false;
        let ticket = state.next_ticket;
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .expect("admission ticket counter exhausted");
        state.admitted = state.admitted.saturating_add(1);
        while ticket >= state.released.saturating_add(state.permits) {
            waited = true;
            state = self.ready.wait(state).unwrap_or_else(|e| e.into_inner());
        }
        if waited {
            let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            state.wait_nanos = state.wait_nanos.saturating_add(nanos);
            state.wait_count = state.wait_count.saturating_add(1);
        }
        let active = state
            .next_ticket
            .saturating_sub(state.released)
            .min(state.permits);
        state.peak_active = state.peak_active.max(active);
    }

    pub(crate) fn snapshot(&self) -> AdmissionSnapshot {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let outstanding = state.next_ticket.saturating_sub(state.released);
        let active = outstanding.min(state.permits);
        AdmissionSnapshot {
            capacity: state.permits as usize,
            active: active as usize,
            waiting: outstanding.saturating_sub(active) as usize,
            admitted: state.admitted,
            rejected: state.rejected,
            wait_micros: state.wait_nanos / 1_000,
            wait_count: state.wait_count,
            peak_active: state.peak_active as usize,
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.released = state
            .released
            .checked_add(1)
            .expect("admission release counter exhausted");
        drop(state);
        // A targeted notify is unsafe because the runtime may wake a later
        // ticket, which must go back to sleep while the next ticket remains
        // blocked. Broadcasting lets the uniquely eligible waiter proceed.
        self.ready.notify_all();
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

/// Nonblocking byte reservations for immutable objects retained by caches.
///
/// Cache admission is an optimization, so callers never wait for this pool:
/// they evict an eligible local entry and retry or skip retention. The owned
/// permit releases its exact reservation even when it outlives the call site.
#[derive(Debug)]
pub(crate) struct RetainedBytePool {
    capacity: u64,
    state: Mutex<RetainedByteState>,
}

#[derive(Debug, Default)]
struct RetainedByteState {
    used: u64,
    peak: u64,
}

impl RetainedBytePool {
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            capacity,
            state: Mutex::new(RetainedByteState::default()),
        }
    }

    pub(crate) fn try_reserve(self: &Arc<Self>, bytes: u64) -> Option<RetainedBytePermit> {
        if bytes == 0 || bytes > self.capacity {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let next = state.used.checked_add(bytes)?;
        if next > self.capacity {
            return None;
        }
        state.used = next;
        state.peak = state.peak.max(next);
        drop(state);
        Some(RetainedBytePermit {
            pool: Arc::clone(self),
            bytes,
        })
    }

    fn release(&self, bytes: u64) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        debug_assert!(state.used >= bytes);
        state.used = state.used.saturating_sub(bytes);
    }

    fn try_resize(&self, previous: u64, next: u64) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(without_previous) = state.used.checked_sub(previous) else {
            return false;
        };
        let Some(next_used) = without_previous.checked_add(next) else {
            return false;
        };
        if next == 0 || next_used > self.capacity {
            return false;
        }
        state.used = next_used;
        state.peak = state.peak.max(next_used);
        true
    }

    pub(crate) fn capacity_bytes(&self) -> u64 {
        self.capacity
    }

    pub(crate) fn used_bytes(&self) -> u64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).used
    }

    pub(crate) fn peak_bytes(&self) -> u64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).peak
    }
}

#[derive(Debug)]
pub(crate) struct RetainedBytePermit {
    pool: Arc<RetainedBytePool>,
    bytes: u64,
}

impl RetainedBytePermit {
    fn try_resize(&mut self, bytes: u64) -> bool {
        if self.pool.try_resize(self.bytes, bytes) {
            self.bytes = bytes;
            true
        } else {
            false
        }
    }
}

impl Drop for RetainedBytePermit {
    fn drop(&mut self) {
        self.pool.release(self.bytes);
    }
}

/// A FIFO weighted semaphore for transient decoded bytes.
///
/// Count-based search admission is not sufficient for heterogeneous data:
/// callers decoding tiny cells and callers decoding unusually large lexical
/// sidecars have different memory peaks. This gate admits the ingest-measured
/// byte estimate, shared across every query and modality on one index handle.
#[derive(Debug)]
pub(crate) struct ByteAdmissionGate {
    capacity: u64,
    state: Mutex<ByteAdmissionState>,
    ready: Condvar,
}

#[derive(Debug, Default)]
struct ByteAdmissionState {
    used: u64,
    peak: u64,
    next_ticket: u64,
    serving_ticket: u64,
    admitted: u64,
    wait_nanos: u64,
    wait_count: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ByteAdmissionSnapshot {
    pub(crate) capacity_bytes: u64,
    pub(crate) used_bytes: u64,
    pub(crate) peak_bytes: u64,
    pub(crate) waiting: usize,
    pub(crate) admitted: u64,
    pub(crate) wait_micros: u64,
    pub(crate) wait_count: u64,
}

impl ByteAdmissionGate {
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            capacity: capacity.max(1),
            state: Mutex::new(ByteAdmissionState::default()),
            ready: Condvar::new(),
        }
    }

    /// Admit `bytes` of transient work and release it when the guard drops.
    ///
    /// One irreducible object may exceed the configured capacity. Its weight is
    /// normalized to the full capacity, so it runs alone instead of deadlocking
    /// or exceeding the limit alongside other decodes.
    #[cfg(test)]
    pub(crate) fn acquire(&self, bytes: u64) -> ByteAdmissionPermit<'_> {
        let weight = self.acquire_weight(bytes);
        ByteAdmissionPermit { gate: self, weight }
    }

    /// Owned form used when the decoded allocation outlives its leaf loader.
    pub(crate) fn acquire_owned(self: &Arc<Self>, bytes: u64) -> OwnedByteAdmissionPermit {
        let weight = self.acquire_weight(bytes);
        OwnedByteAdmissionPermit {
            gate: Arc::clone(self),
            weight,
        }
    }

    fn acquire_weight(&self, bytes: u64) -> u64 {
        let weight = bytes.max(1).min(self.capacity);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let ticket = state.next_ticket;
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .expect("byte admission ticket counter exhausted");
        let started = Instant::now();
        let mut waited = false;
        while ticket != state.serving_ticket || state.used.saturating_add(weight) > self.capacity {
            waited = true;
            state = self.ready.wait(state).unwrap_or_else(|e| e.into_inner());
        }
        if waited {
            let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            state.wait_nanos = state.wait_nanos.saturating_add(nanos);
            state.wait_count = state.wait_count.saturating_add(1);
        }
        state.used = state.used.saturating_add(weight);
        state.peak = state.peak.max(state.used);
        state.admitted = state.admitted.saturating_add(1);
        state.serving_ticket = state
            .serving_ticket
            .checked_add(1)
            .expect("byte admission serving counter exhausted");
        drop(state);
        // The next FIFO waiter may fit concurrently with this permit.
        self.ready.notify_all();
        weight
    }

    fn release(&self, weight: u64) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.used = state.used.saturating_sub(weight);
        drop(state);
        self.ready.notify_all();
    }

    pub(crate) fn capacity_bytes(&self) -> u64 {
        self.capacity
    }

    pub(crate) fn used_bytes(&self) -> u64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).used
    }

    pub(crate) fn peak_bytes(&self) -> u64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).peak
    }

    pub(crate) fn snapshot(&self) -> ByteAdmissionSnapshot {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        ByteAdmissionSnapshot {
            capacity_bytes: self.capacity,
            used_bytes: state.used,
            peak_bytes: state.peak,
            waiting: usize::try_from(state.next_ticket.saturating_sub(state.serving_ticket))
                .unwrap_or(usize::MAX),
            admitted: state.admitted,
            wait_micros: state.wait_nanos / 1_000,
            wait_count: state.wait_count,
        }
    }
}

#[cfg(test)]
pub(crate) struct ByteAdmissionPermit<'a> {
    gate: &'a ByteAdmissionGate,
    weight: u64,
}

#[cfg(test)]
impl Drop for ByteAdmissionPermit<'_> {
    fn drop(&mut self) {
        self.gate.release(self.weight);
    }
}

#[derive(Debug)]
pub(crate) struct OwnedByteAdmissionPermit {
    gate: Arc<ByteAdmissionGate>,
    weight: u64,
}

impl Drop for OwnedByteAdmissionPermit {
    fn drop(&mut self) {
        self.gate.release(self.weight);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::VectorMetric;
    use crate::record::VectorRecord;
    use crate::segment::{Segment, SegmentGraph};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

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
    fn overlapping_segment_consumers_share_without_retaining_after_last_drop() {
        let flights = Arc::new(InFlightSegmentReads::default());
        let started = Arc::new(std::sync::Barrier::new(8));
        let loads = Arc::new(AtomicUsize::new(0));
        let handles = (0..8)
            .map(|_| {
                let flights = Arc::clone(&flights);
                let started = Arc::clone(&started);
                let loads = Arc::clone(&loads);
                thread::spawn(move || {
                    started.wait();
                    flights
                        .load("checksum", || {
                            loads.fetch_add(1, Ordering::SeqCst);
                            thread::sleep(Duration::from_millis(40));
                            Ok(((*sample_segment("shared")).clone(), 123))
                        })
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();

        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(results.iter().map(|(_, bytes, _)| bytes).sum::<u64>(), 123);
        assert_eq!(results.iter().filter(|(_, _, shared)| *shared).count(), 7);
        for (segment, _, _) in &results[1..] {
            assert!(Arc::ptr_eq(&results[0].0, segment));
        }

        // The completed decode remains shareable while any consumer still owns
        // it, without the flight table itself holding a strong reference.
        let (still_shared, bytes, shared) = flights
            .load("checksum", || {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok(((*sample_segment("later")).clone(), 456))
            })
            .unwrap();
        assert_eq!(bytes, 0);
        assert!(shared);
        assert!(Arc::ptr_eq(&still_shared, &results[0].0));
        assert_eq!(loads.load(Ordering::SeqCst), 1);

        drop(still_shared);
        drop(results);
        let (_, bytes, shared) = flights
            .load("checksum", || {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok(((*sample_segment("later")).clone(), 456))
            })
            .unwrap();
        assert_eq!(bytes, 456);
        assert!(!shared);
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn concurrent_graph_loads_share_one_immutable_adjacency() {
        let flights = Arc::new(InFlightGraphReads::default());
        let started = Arc::new(std::sync::Barrier::new(8));
        let loads = Arc::new(AtomicUsize::new(0));
        let handles = (0..8)
            .map(|_| {
                let flights = Arc::clone(&flights);
                let started = Arc::clone(&started);
                let loads = Arc::clone(&loads);
                thread::spawn(move || {
                    started.wait();
                    flights
                        .load("graph-checksum", || {
                            loads.fetch_add(1, Ordering::SeqCst);
                            thread::sleep(Duration::from_millis(40));
                            let segment = sample_segment("shared-graph");
                            Ok((SegmentGraph::from_segment(&segment, 1).unwrap(), 321))
                        })
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();

        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(results.iter().map(|(_, bytes, _)| bytes).sum::<u64>(), 321);
        assert_eq!(results.iter().filter(|(_, _, shared)| *shared).count(), 7);
        for (graph, _, _) in &results[1..] {
            assert!(Arc::ptr_eq(&results[0].0, graph));
        }
    }

    #[test]
    fn decoded_object_cache_shares_values_under_one_byte_budget() {
        let cache = DecodedObjectCache::new(160);
        let first = "first".to_string();
        let first_shard = cache.shard_for(&first) as *const _;
        let second = (0..10_000)
            .map(|index| format!("candidate-{index}"))
            .find(|key| std::ptr::eq(cache.shard_for(key), first_shard))
            .unwrap();

        let first_value = Arc::new(vec![1_u8]);
        cache.insert(first.clone(), Arc::clone(&first_value), 8);
        assert!(Arc::ptr_eq(&cache.get(&first).unwrap(), &first_value));

        let second_value = Arc::new(vec![2_u8]);
        cache.insert(second.clone(), Arc::clone(&second_value), 8);
        assert!(cache.get(&first).is_none());
        assert!(Arc::ptr_eq(&cache.get(&second).unwrap(), &second_value));
        assert_eq!(cache.resident_bytes(), 8);
    }

    #[test]
    fn zero_byte_decoded_object_cache_retains_nothing() {
        let cache = DecodedObjectCache::new(0);
        cache.insert("zero".to_string(), Arc::new(vec![1_u8]), 1);
        assert!(cache.get("zero").is_none());
        assert_eq!(cache.resident_bytes(), 0);
    }

    #[test]
    fn differently_typed_caches_share_one_retained_pool() {
        let pool = Arc::new(RetainedBytePool::new(100));
        let bytes = DecodedObjectCache::<Vec<u8>>::new_with_pool(1_600, Arc::clone(&pool));
        let strings = DecodedObjectCache::<String>::new_with_pool(1_600, Arc::clone(&pool));

        bytes.insert("bytes".to_string(), Arc::new(vec![1_u8]), 60);
        strings.insert("strings".to_string(), Arc::new("value".to_string()), 60);

        assert_eq!(bytes.resident_bytes(), 60);
        assert_eq!(strings.resident_bytes(), 0);
        assert_eq!(pool.used_bytes(), 60);
        assert!(pool.used_bytes() <= pool.capacity_bytes());
        drop(bytes);
        assert_eq!(pool.used_bytes(), 0);
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
    fn decoded_cache_shares_graph_with_its_segment_and_accounts_for_bytes() {
        let segment = sample_segment("graph");
        let segment_bytes = decoded_segment_bytes(&segment);
        let graph = Arc::new(SegmentGraph::from_segment(&segment, 1).unwrap());
        let graph_bytes = decoded_graph_bytes(&graph);
        let cache = DecodedSegmentCache::new(u64::MAX);

        cache.insert(
            "segment-checksum".to_string(),
            Arc::clone(&segment),
            segment_bytes,
        );
        assert!(
            cache
                .get_graph("segment-checksum", "graph-checksum")
                .is_none()
        );
        cache.insert_graph(
            "segment-checksum",
            "graph-checksum".to_string(),
            Arc::clone(&graph),
            graph_bytes,
        );
        assert!(
            cache
                .get_graph("segment-checksum", "different-graph-checksum")
                .is_none(),
            "a reused segment must not return a graph from another manifest checksum"
        );

        let cached = cache
            .get_graph("segment-checksum", "graph-checksum")
            .expect("graph should share the decoded segment cache entry");
        assert!(Arc::ptr_eq(&cached, &graph));
        assert_eq!(cache.resident_bytes(), segment_bytes + graph_bytes);
    }

    #[test]
    fn ordinary_insert_does_not_unpin_a_warmed_entry() {
        let warmed = sample_segment("warmed");
        let bytes = decoded_segment_bytes(&warmed);
        let cache = DecodedSegmentCache::new(bytes * CACHE_SHARDS as u64);
        cache.insert_with_pin("warm".to_string(), Arc::clone(&warmed), bytes, true);
        cache.insert("warm".to_string(), Arc::clone(&warmed), bytes);

        let same_shard_key = (0..usize::MAX)
            .map(|index| format!("candidate-{index}"))
            .find(|key| std::ptr::eq(cache.shard_for("warm"), cache.shard_for(key)))
            .unwrap();
        let other = sample_segment("other");
        cache.insert(same_shard_key, other, bytes);

        assert!(cache.get("warm").is_some());
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

    #[test]
    fn admission_gate_serves_waiters_in_ticket_order() {
        let gate = Arc::new(AdmissionGate::new(1));
        let held = gate.acquire();
        let admitted = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        for ordinal in 0..8 {
            let thread_gate = Arc::clone(&gate);
            let admitted = Arc::clone(&admitted);
            handles.push(thread::spawn(move || {
                let _permit = thread_gate.acquire();
                admitted
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(ordinal);
            }));

            // Queue each thread before starting the next one so its ticket
            // order is deterministic even under an adversarial scheduler.
            loop {
                let queued = gate
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .next_ticket;
                if queued == ordinal as u64 + 2 {
                    break;
                }
                thread::yield_now();
            }
        }

        drop(held);
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(
            *admitted.lock().unwrap_or_else(|e| e.into_inner()),
            (0..8).collect::<Vec<_>>()
        );
    }

    #[test]
    fn admission_gate_rejects_work_beyond_its_bounded_queue() {
        let gate = Arc::new(AdmissionGate::new_bounded(1, 1));
        let held = gate.acquire_bounded().unwrap();
        let waiter_gate = Arc::clone(&gate);
        let waiter = thread::spawn(move || {
            let _permit = waiter_gate.acquire_bounded().unwrap();
        });

        loop {
            let queued = gate
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .next_ticket;
            if queued == 2 {
                break;
            }
            thread::yield_now();
        }

        assert!(gate.acquire_bounded().is_none());
        assert_eq!(gate.snapshot().rejected, 1);
        assert_eq!(gate.snapshot().waiting, 1);
        drop(held);
        waiter.join().unwrap();
        let snapshot = gate.snapshot();
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.waiting, 0);
        assert_eq!(snapshot.admitted, 2);
    }

    #[test]
    fn decoded_segment_eviction_releases_shared_retained_bytes() {
        let first = sample_segment("first");
        let bytes = decoded_segment_bytes(&first);
        let pool = Arc::new(RetainedBytePool::new(bytes));
        let cache =
            DecodedSegmentCache::new_with_pool(bytes * CACHE_SHARDS as u64, Arc::clone(&pool));
        let first_key = "first-key".to_string();
        let shard = cache.shard_for(&first_key) as *const _;
        let second_key = (0..10_000)
            .map(|index| format!("replacement-{index}"))
            .find(|key| std::ptr::eq(cache.shard_for(key), shard))
            .unwrap();

        cache.insert(first_key.clone(), Arc::clone(&first), bytes);
        assert_eq!(pool.used_bytes(), bytes);
        cache.insert(second_key.clone(), sample_segment("second"), bytes);

        assert!(cache.get(&first_key).is_none());
        assert!(cache.get(&second_key).is_some());
        assert_eq!(pool.used_bytes(), bytes);
        drop(cache);
        assert_eq!(pool.used_bytes(), 0);
    }

    #[test]
    fn byte_admission_gate_allows_only_work_that_fits_the_shared_capacity() {
        let gate = Arc::new(ByteAdmissionGate::new(100));
        let first = gate.acquire(70);
        assert_eq!(gate.used_bytes(), 70);

        let acquired = Arc::new(AtomicUsize::new(0));
        let thread_gate = Arc::clone(&gate);
        let thread_acquired = Arc::clone(&acquired);
        let waiter = thread::spawn(move || {
            let _permit = thread_gate.acquire(40);
            thread_acquired.store(1, Ordering::SeqCst);
        });
        thread::sleep(Duration::from_millis(25));
        assert_eq!(acquired.load(Ordering::SeqCst), 0);
        let waiting = gate.snapshot();
        assert_eq!(waiting.waiting, 1);
        assert_eq!(waiting.admitted, 1);

        drop(first);
        waiter.join().unwrap();
        assert_eq!(acquired.load(Ordering::SeqCst), 1);
        assert_eq!(gate.used_bytes(), 0);
        let finished = gate.snapshot();
        assert_eq!(finished.waiting, 0);
        assert_eq!(finished.admitted, 2);
        assert_eq!(finished.wait_count, 1);
        assert!(finished.wait_micros > 0);
    }

    #[test]
    fn byte_admission_gate_runs_an_oversize_unit_alone() {
        let gate = Arc::new(ByteAdmissionGate::new(64));
        let oversize = gate.acquire(512);
        assert_eq!(gate.used_bytes(), 64);

        let acquired = Arc::new(AtomicUsize::new(0));
        let thread_gate = Arc::clone(&gate);
        let thread_acquired = Arc::clone(&acquired);
        let waiter = thread::spawn(move || {
            let _permit = thread_gate.acquire(1);
            thread_acquired.store(1, Ordering::SeqCst);
        });
        thread::sleep(Duration::from_millis(25));
        assert_eq!(acquired.load(Ordering::SeqCst), 0);

        drop(oversize);
        waiter.join().unwrap();
        assert_eq!(acquired.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn retained_byte_pool_reservations_are_owned_and_nonblocking() {
        let pool = Arc::new(RetainedBytePool::new(100));
        let first = pool.try_reserve(70).expect("first reservation fits");
        assert_eq!(pool.used_bytes(), 70);
        assert_eq!(pool.peak_bytes(), 70);
        assert_eq!(pool.capacity_bytes(), 100);
        assert!(pool.try_reserve(31).is_none());

        let second = pool.try_reserve(30).expect("remaining capacity fits");
        assert_eq!(pool.used_bytes(), 100);
        assert_eq!(pool.peak_bytes(), 100);
        drop(first);
        assert_eq!(pool.used_bytes(), 30);
        drop(second);
        assert_eq!(pool.used_bytes(), 0);
        assert_eq!(pool.peak_bytes(), 100);
    }

    #[test]
    fn retained_byte_pool_zero_capacity_rejects_every_reservation() {
        let pool = Arc::new(RetainedBytePool::new(0));
        assert!(pool.try_reserve(0).is_none());
        assert!(pool.try_reserve(1).is_none());
        assert_eq!(pool.capacity_bytes(), 0);
        assert_eq!(pool.used_bytes(), 0);
    }

    #[test]
    fn owned_byte_admission_permit_outlives_acquire_call() {
        fn reserve(gate: Arc<ByteAdmissionGate>) -> OwnedByteAdmissionPermit {
            gate.acquire_owned(48)
        }

        let gate = Arc::new(ByteAdmissionGate::new(64));
        let permit = reserve(Arc::clone(&gate));
        assert_eq!(gate.used_bytes(), 48);
        assert_eq!(gate.peak_bytes(), 48);
        assert_eq!(gate.capacity_bytes(), 64);
        drop(permit);
        assert_eq!(gate.used_bytes(), 0);
    }

    #[test]
    fn owned_byte_admission_records_bounded_mixed_peak() {
        let gate = Arc::new(ByteAdmissionGate::new(100));
        let first = gate.acquire_owned(60);
        let second = gate.acquire_owned(40);
        assert_eq!(gate.used_bytes(), 100);
        assert_eq!(gate.peak_bytes(), 100);
        drop(first);
        drop(second);
        assert_eq!(gate.used_bytes(), 0);

        let oversize = gate.acquire_owned(1_000);
        assert_eq!(gate.used_bytes(), 100);
        assert_eq!(gate.peak_bytes(), 100);
        drop(oversize);
    }
}
