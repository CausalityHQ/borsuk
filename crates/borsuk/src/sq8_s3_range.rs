//! Conditional S3 page-range fetch with generation-bound SHA-256 verification.

use crate::sq8_page_authority::{PageAuthority, PageError};
use crate::{
    exact_sq8_nominee::{ScoredNominee, Sq8Geometry, Sq8ScoreError},
    returned_sq8::{RankPhases, ReturnedRange, rank_returned_ranges_excluding_traced},
};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream};
use http_body_util::BodyExt;
use object_store::aws::{AmazonS3, AmazonS3Builder, S3CopyIfNotExists};
use object_store::client::{
    ClientConfigKey, ClientOptions, HttpClient, HttpConnector, HttpError, HttpErrorKind,
    HttpRequest, HttpResponse, HttpResponseBody, HttpService,
};
use object_store::{
    GetOptions, GetResultPayload, ObjectStore, RetryConfig, path::Path, prefix::PrefixStore,
};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;

/// SOURCE response authentication policy; SQ8 authentication stays inline.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAuthentication {
    /// Production control, with no worker allocation.
    #[default]
    Inline,
    /// Experimental two-worker compute bound, independent of download concurrency.
    Offload2,
}

impl SourceAuthentication {
    /// Extra library reservation for all active queries: worker stacks, jobs and scheduler
    /// state. Existing full payload and ordinary range-state reservations stay charged.
    pub fn additional_memory_bytes(
        self,
        downloads: usize,
        ranges: usize,
        active_queries: usize,
    ) -> Option<u64> {
        if self == Self::Inline {
            return Some(0);
        }
        u64::try_from(source_authentication_bytes(downloads, ranges)?)
            .ok()?
            .checked_mul(u64::try_from(active_queries).ok()?)?
            .checked_add(256)
    }
}

const SOURCE_AUTH_WORKERS: usize = 2;
const SOURCE_AUTH_STACK_BYTES: usize = 1024 * 1024;
const SOURCE_AUTH_FUTURE_BYTES: usize = 4096;

/// Extra per-query state; payloads move into jobs and remain in the existing full-body budget.
pub(crate) fn source_authentication_bytes(downloads: usize, ranges: usize) -> Option<usize> {
    let pending = ranges.min(downloads.checked_add(SOURCE_AUTH_WORKERS)?);
    pending
        .checked_mul(SOURCE_AUTH_FUTURE_BYTES)?
        .checked_add(SOURCE_AUTH_WORKERS * (SOURCE_AUTH_STACK_BYTES + 4096))?
        .checked_add(4096)
}

pub(crate) fn authenticate_inline<T>(
    mut probe: Option<Box<Probe>>,
    authenticate: impl FnOnce() -> Result<T, RangeFetchError>,
) -> (Result<T, RangeFetchError>, Option<Box<Probe>>) {
    let result = measured_auth(&mut probe.as_deref_mut(), authenticate);
    (result, probe)
}

#[cfg(test)]
pub(crate) static SOURCE_AUTH_TEST_SERIAL: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

type SourceAuthJob = Box<dyn FnOnce() + Send + 'static>;

// Weak ownership lets the last query close and join the pool. The worker lease also
// covers that join, so reinitialization cannot overlap old and new worker threads.
static SOURCE_AUTH_POOL: LazyLock<Mutex<std::sync::Weak<SourceAuthPool>>> =
    LazyLock::new(|| Mutex::new(std::sync::Weak::new()));
static SOURCE_AUTH_INITIALIZE: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(1)));
static SOURCE_AUTH_THREADS: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(SOURCE_AUTH_WORKERS)));

struct SourceAuthPool {
    sender: Option<std::sync::mpsc::SyncSender<SourceAuthJob>>,
    workers: Vec<std::thread::JoinHandle<()>>,
    slots: Arc<tokio::sync::Semaphore>,
    _worker_lease: tokio::sync::OwnedSemaphorePermit,
}

impl SourceAuthPool {
    async fn acquire() -> Result<Arc<Self>, RangeFetchError> {
        let current = SOURCE_AUTH_POOL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upgrade();
        if let Some(current) = current {
            return Ok(current);
        }
        let _initialize = Arc::clone(&SOURCE_AUTH_INITIALIZE)
            .acquire_owned()
            .await
            .map_err(|_| RangeFetchError::UnexpectedMetadata)?;
        let current = SOURCE_AUTH_POOL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upgrade();
        if let Some(current) = current {
            return Ok(current);
        }
        let worker_lease = Arc::clone(&SOURCE_AUTH_THREADS)
            .acquire_many_owned(SOURCE_AUTH_WORKERS as u32)
            .await
            .map_err(|_| RangeFetchError::UnexpectedMetadata)?;
        let (sender, receiver) =
            std::sync::mpsc::sync_channel::<SourceAuthJob>(SOURCE_AUTH_WORKERS);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut pool = Self {
            sender: Some(sender),
            workers: Vec::with_capacity(SOURCE_AUTH_WORKERS),
            slots: Arc::new(tokio::sync::Semaphore::new(SOURCE_AUTH_WORKERS)),
            _worker_lease: worker_lease,
        };
        for _ in 0..SOURCE_AUTH_WORKERS {
            let receiver = Arc::clone(&receiver);
            let worker = std::thread::Builder::new()
                .name("borsuk-source-auth".into())
                .stack_size(SOURCE_AUTH_STACK_BYTES)
                .spawn(move || {
                    loop {
                        let next = receiver
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .recv();
                        let Ok(job) = next else { break };
                        job();
                    }
                })
                .map_err(|_| RangeFetchError::UnexpectedMetadata)?;
            pool.workers.push(worker);
        }
        let pool = Arc::new(pool);
        *SOURCE_AUTH_POOL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::downgrade(&pool);
        Ok(pool)
    }
}

impl Drop for SourceAuthPool {
    fn drop(&mut self) {
        self.sender.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[derive(Default)]
struct SourceAuthGroup {
    pending: Mutex<usize>,
    changed: std::sync::Condvar,
}

struct SourceAuthPending {
    group: Arc<SourceAuthGroup>,
    #[cfg(test)]
    _lifetime: Arc<()>,
}

impl Drop for SourceAuthPending {
    fn drop(&mut self) {
        let mut pending = self
            .group
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *pending -= 1;
        if *pending == 0 {
            self.group.changed.notify_all();
        }
    }
}

/// Per-query download admission and drain, with two shared process-wide compute workers.
pub(crate) struct SourceAuthWorkers {
    pool: Arc<SourceAuthPool>,
    group: Arc<SourceAuthGroup>,
    slots: Arc<tokio::sync::Semaphore>,
    downloads: tokio::sync::Semaphore,
    #[cfg(test)]
    lifetime: Arc<()>,
}

impl SourceAuthWorkers {
    pub(crate) async fn new(downloads: usize) -> Result<Self, RangeFetchError> {
        if downloads == 0 || downloads > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(RangeFetchError::UnexpectedMetadata);
        }
        let pool = SourceAuthPool::acquire().await?;
        Ok(Self {
            slots: Arc::clone(&pool.slots),
            pool,
            group: Arc::new(SourceAuthGroup::default()),
            downloads: tokio::sync::Semaphore::new(downloads),
            #[cfg(test)]
            lifetime: Arc::new(()),
        })
    }

    pub(crate) async fn authenticate<T: Send + 'static>(
        &self,
        mut probe: Option<Box<Probe>>,
        authenticate: impl FnOnce() -> Result<T, RangeFetchError> + Send + 'static,
    ) -> (Result<T, RangeFetchError>, Option<Box<Probe>>) {
        let slot = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .expect("SOURCE worker slots stay open");
        *self
            .group
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        let pending = SourceAuthPending {
            group: Arc::clone(&self.group),
            #[cfg(test)]
            _lifetime: Arc::clone(&self.lifetime),
        };
        let (send, receive) = tokio::sync::oneshot::channel();
        let job = Box::new(move || {
            let _pending = pending;
            let _slot = slot;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                measured_auth(&mut probe.as_deref_mut(), authenticate)
            }))
            .unwrap_or(Err(RangeFetchError::UnexpectedMetadata));
            let _ = send.send((result, probe));
        });
        if self
            .pool
            .sender
            .as_ref()
            .expect("live SOURCE pool")
            .try_send(job)
            .is_err()
        {
            return (Err(RangeFetchError::UnexpectedMetadata), None);
        }
        receive
            .await
            .unwrap_or((Err(RangeFetchError::UnexpectedMetadata), None))
    }
}

impl Drop for SourceAuthWorkers {
    fn drop(&mut self) {
        let pending = self
            .group
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        drop(
            self.group
                .changed
                .wait_while(pending, |pending| *pending != 0)
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }
}

/// Shared FIFO admission for bytes being downloaded and authenticated.
/// Completed payloads remain subject to the caller's separate resident-byte cap.
pub struct InFlightBytes {
    capacity: usize,
    max_waiters: usize,
    available: Mutex<usize>,
    waiters: Mutex<std::collections::VecDeque<Arc<()>>>,
    changed: tokio::sync::Notify,
}

impl InFlightBytes {
    #[cfg(test)]
    fn new(capacity: usize) -> Arc<Self> {
        Self::with_waiter_limit(capacity, 1024, usize::MAX).unwrap()
    }

    /// Create a byte window with an explicit, bounded waiting-request roster.
    pub fn with_waiter_limit(
        capacity: usize,
        max_waiters: usize,
        max_state_bytes: usize,
    ) -> Result<Arc<Self>, &'static str> {
        if max_waiters == 0 {
            return Err("zero byte-window waiter limit");
        }
        if Self::modeled_state_bytes(max_waiters).is_none_or(|bytes| bytes > max_state_bytes) {
            return Err("byte-window state budget");
        }
        let mut waiters = std::collections::VecDeque::new();
        waiters
            .try_reserve_exact(max_waiters)
            .map_err(|_| "byte-window waiter allocation")?;
        if Self::modeled_state_bytes(waiters.capacity()).is_none_or(|bytes| bytes > max_state_bytes)
        {
            return Err("byte-window allocated state budget");
        }
        Ok(Arc::new(Self {
            capacity,
            max_waiters,
            available: Mutex::new(capacity),
            waiters: Mutex::new(waiters),
            changed: tokio::sync::Notify::new(),
        }))
    }

    /// Conservative shared state charge, including the queue and waiter token allocations.
    /// Pending async frames and retained payloads are charged separately by the query.
    pub fn modeled_state_bytes(max_waiters: usize) -> Option<usize> {
        let per_waiter = std::mem::size_of::<Arc<()>>()
            .checked_add(2 * std::mem::size_of::<usize>())?
            .checked_add(ALLOCATION_ALLOWANCE_BYTES)?;
        max_waiters
            .checked_mul(per_waiter)?
            .checked_add(std::mem::size_of::<Self>())?
            .checked_add(2 * std::mem::size_of::<usize>())?
            .checked_add(ALLOCATION_ALLOWANCE_BYTES)
    }

    /// Charged shared state for this window, using the allocated queue capacity.
    pub fn state_bytes(&self) -> usize {
        let capacity = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .capacity();
        Self::modeled_state_bytes(capacity).expect("admitted byte-window state")
    }

    async fn reserve(self: &Arc<Self>, bytes: usize) -> Option<InFlightPermit> {
        if bytes > self.capacity {
            return None;
        }
        let token = Arc::new(());
        {
            let mut waiters = self
                .waiters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if waiters.len() >= self.max_waiters {
                return None;
            }
            waiters.push_back(token.clone());
        }
        let mut waiting = ByteWaiter {
            budget: self.clone(),
            token,
            registered: true,
        };
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut waiters = self
                    .waiters
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let mut available = self
                    .available
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if waiters
                    .front()
                    .is_some_and(|head| Arc::ptr_eq(head, &waiting.token))
                    && bytes <= *available
                {
                    *available -= bytes;
                    waiters.pop_front();
                    waiting.registered = false;
                    self.changed.notify_waiters();
                    return Some(InFlightPermit {
                        budget: self.clone(),
                        bytes,
                    });
                }
            }
            changed.await;
        }
    }
}

struct ByteWaiter {
    budget: Arc<InFlightBytes>,
    token: Arc<()>,
    registered: bool,
}

impl Drop for ByteWaiter {
    fn drop(&mut self) {
        if self.registered {
            let mut waiters = self
                .budget
                .waiters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(position) = waiters
                .iter()
                .position(|token| Arc::ptr_eq(token, &self.token))
            {
                waiters.remove(position);
            }
            drop(waiters);
            self.budget.changed.notify_waiters();
        }
    }
}

pub(crate) struct InFlightPermit {
    budget: Arc<InFlightBytes>,
    bytes: usize,
}

impl Drop for InFlightPermit {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        let mut available = self
            .budget
            .available
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *available = available
            .checked_add(self.bytes)
            .expect("byte permit conservation");
        assert!(
            *available <= self.budget.capacity,
            "byte permit conservation"
        );
        drop(available);
        self.budget.changed.notify_waiters();
    }
}

#[cfg(test)]
mod byte_admission_tests {
    use super::*;

    #[test]
    fn byte_window_state_budget_is_checked_before_allocation() {
        let bytes = InFlightBytes::modeled_state_bytes(3).unwrap();
        assert!(InFlightBytes::with_waiter_limit(7, 3, bytes - 1).is_err());
        assert!(InFlightBytes::with_waiter_limit(7, 3, bytes).is_ok());
        assert!(InFlightBytes::with_waiter_limit(7, 0, usize::MAX).is_err());
        assert!(InFlightBytes::modeled_state_bytes(usize::MAX).is_none());
        assert!(InFlightBytes::with_waiter_limit(7, usize::MAX, usize::MAX).is_err());
    }

    #[tokio::test]
    async fn byte_window_rejects_impossible_reservation_without_waiting() {
        let budget = InFlightBytes::new(7);
        let outcome =
            tokio::time::timeout(std::time::Duration::from_millis(100), budget.reserve(8))
                .await
                .expect("an impossible weighted reservation must refuse without waiting");
        assert!(outcome.is_none(), "missing pre-I/O byte-window admission");
    }

    #[tokio::test]
    async fn byte_window_refills_and_cancelled_waiter_never_consumes_capacity() {
        let budget = InFlightBytes::new(7);
        let held = budget.reserve(6).await.unwrap();
        let mut waiting = Box::pin(budget.reserve(2));
        assert!(futures_util::poll!(waiting.as_mut()).is_pending());
        assert_eq!(*budget.available.lock().unwrap(), 1);
        drop(held);
        let second = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*budget.available.lock().unwrap(), 5);
        let first = budget.reserve(5).await.unwrap();
        let mut cancelled = Box::pin(budget.reserve(1));
        assert!(futures_util::poll!(cancelled.as_mut()).is_pending());
        drop(cancelled);
        drop(first);
        assert_eq!(*budget.available.lock().unwrap(), 5);
        drop(second);
        assert_eq!(*budget.available.lock().unwrap(), 7);
        let all = budget.reserve(7).await.unwrap();
        assert_eq!(*budget.available.lock().unwrap(), 0);
        drop(all);
        assert_eq!(*budget.available.lock().unwrap(), 7);
    }

    #[tokio::test]
    async fn byte_window_does_not_bypass_an_older_large_waiter() {
        let budget = InFlightBytes::new(7);
        let held = budget.reserve(6).await.unwrap();
        let mut older = Box::pin(budget.reserve(7));
        assert!(futures_util::poll!(older.as_mut()).is_pending());
        let mut younger = Box::pin(budget.reserve(1));
        assert!(
            futures_util::poll!(younger.as_mut()).is_pending(),
            "a younger small reservation must not starve an older large reservation"
        );
        drop(held);
        let large = tokio::time::timeout(std::time::Duration::from_secs(1), older)
            .await
            .unwrap()
            .unwrap();
        assert!(futures_util::poll!(younger.as_mut()).is_pending());
        drop(large);
        let small = tokio::time::timeout(std::time::Duration::from_secs(1), younger)
            .await
            .unwrap()
            .unwrap();
        drop(small);
        assert_eq!(*budget.available.lock().unwrap(), 7);
    }

    #[tokio::test]
    async fn byte_window_cancelled_head_unblocks_successor_and_full_queue_refuses() {
        let budget = InFlightBytes::new(7);
        let held = budget.reserve(6).await.unwrap();
        let mut head = Box::pin(budget.reserve(7));
        assert!(futures_util::poll!(head.as_mut()).is_pending());
        let mut successor = Box::pin(budget.reserve(1));
        assert!(futures_util::poll!(successor.as_mut()).is_pending());
        drop(head);
        let small = tokio::time::timeout(std::time::Duration::from_secs(1), successor)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*budget.available.lock().unwrap(), 0);
        let mut waiting = Vec::new();
        for _ in 0..1024 {
            let mut request = Box::pin(budget.reserve(1));
            assert!(futures_util::poll!(request.as_mut()).is_pending());
            waiting.push(request);
        }
        assert_eq!(budget.waiters.lock().unwrap().len(), 1024);
        assert!(budget.reserve(1).await.is_none());
        drop(waiting);
        assert!(budget.waiters.lock().unwrap().is_empty());
        drop(small);
        drop(held);
        assert_eq!(*budget.available.lock().unwrap(), 7);
    }

    #[tokio::test]
    async fn byte_window_handles_zero_and_usize_max_without_rounding_or_overflow() {
        let empty = InFlightBytes::new(0);
        assert!(empty.reserve(1).await.is_none());
        drop(empty.reserve(0).await.unwrap());
        assert_eq!(*empty.available.lock().unwrap(), 0);
        let budget = InFlightBytes::new(usize::MAX);
        let held = budget.reserve(usize::MAX).await.unwrap();
        let mut waiting = Box::pin(budget.reserve(1));
        assert!(futures_util::poll!(waiting.as_mut()).is_pending());
        drop(waiting);
        drop(held);
        assert_eq!(*budget.available.lock().unwrap(), usize::MAX);
        let almost_all = budget.reserve(usize::MAX - 1).await.unwrap();
        let last = budget.reserve(1).await.unwrap();
        assert_eq!(*budget.available.lock().unwrap(), 0);
        drop(almost_all);
        drop(last);
        assert_eq!(*budget.available.lock().unwrap(), usize::MAX);
    }
}

#[derive(Debug)]
pub enum RangeFetchError {
    Store(object_store::Error),
    Page(PageError),
    Score(Sq8ScoreError),
    UnexpectedMetadata,
}

/// Submitted object-store reads and bytes that passed authentication.
/// Failed responses may have transferred bytes that are not counted here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Sq8ReadStats {
    pub submitted_gets: usize,
    pub verified_bytes: usize,
    pub failed_gets: usize,
}

/// Optional transport caps; logical cover budgets and scoring batches stay unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Sq8RangeSplit {
    pub max_physical_range_bytes: usize,
    pub max_physical_gets: usize,
}

impl Sq8RangeSplit {
    /// Additional per-query reservation: reassembly, transport bodies, split roster,
    /// retained range descriptors and the complete physical future/outcome drain.
    pub fn scratch_bytes(self, max_bytes: usize, parallel: usize) -> Option<usize> {
        if self.max_physical_range_bytes == 0
            || self.max_physical_range_bytes > isize::MAX as usize
            || self.max_physical_gets == 0
            || parallel == 0
        {
            return None;
        }
        let descriptors = self.max_physical_gets.checked_mul(
            std::mem::size_of::<(usize, usize)>()
                .checked_add(2 * std::mem::size_of::<VerifiedRange>())?
                .checked_add(std::mem::size_of::<ReturnedRange<'_>>())?,
        )?;
        let pending =
            Sq8RangeTrace::checked_pending_futures_bytes(self.max_physical_gets, parallel)?;
        max_bytes
            .checked_add(descriptors)?
            .checked_add(
                self.max_physical_range_bytes
                    .min(max_bytes)
                    .checked_mul(self.max_physical_gets.min(parallel))?
                    .min(max_bytes)
                    .checked_mul(2)?,
            )?
            .checked_add(pending)
            .filter(|&bytes| bytes <= isize::MAX as usize)
    }

    // Allocation-free pass; repeat only after all count/byte/trace admission succeeds.
    fn visit(
        self,
        authority: &PageAuthority,
        ranges: &[(usize, usize)],
        logical_cap: usize,
        max_bytes: usize,
        mut emit: impl FnMut((usize, usize)),
    ) -> Result<usize, RangeFetchError> {
        let bad = || RangeFetchError::UnexpectedMetadata;
        if !authority.is_sq8()
            || ranges.is_empty()
            || ranges.len() > logical_cap
            || self.max_physical_gets == 0
            || self.max_physical_range_bytes == 0
        {
            return Err(bad());
        }
        let block = authority
            .dimensions()
            .checked_add(12)
            .and_then(|row| row.checked_mul(authority.page_rows()))
            .ok_or_else(bad)?;
        let per_piece = self.max_physical_range_bytes / block;
        let mut total = 0usize;
        let mut count = 0usize;
        let mut previous = None;
        for &(first, last) in ranges {
            if previous.is_some_and(|end| first <= end) {
                return Err(bad());
            }
            let interval = authority
                .byte_range(first, last)
                .map_err(RangeFetchError::Page)?;
            total = total
                .checked_add(interval.end - interval.start)
                .ok_or_else(bad)?;
            if total > max_bytes || total > isize::MAX as usize {
                return Err(bad());
            }
            let blocks = last
                .checked_sub(first)
                .and_then(|n| n.checked_add(1))
                .ok_or_else(bad)?;
            let pieces = if per_piece == 0 {
                if blocks != 1 || interval.end - interval.start > self.max_physical_range_bytes {
                    return Err(bad());
                }
                1
            } else {
                blocks.div_ceil(per_piece)
            };
            count = count
                .checked_add(pieces)
                .filter(|&n| n <= self.max_physical_gets)
                .ok_or_else(bad)?;
            let mut start = first;
            for _ in 0..pieces {
                let end = start
                    .checked_add(per_piece.max(1).min(last - start + 1) - 1)
                    .ok_or_else(bad)?;
                emit((start, end));
                if end < last {
                    start = end.checked_add(1).ok_or_else(bad)?;
                }
            }
            previous = Some(last);
        }
        Ok(count)
    }
}

/// A bounded remote SQ8 query; no source-vector plane is loaded or retained.
#[derive(Debug)]
pub struct RankedSq8 {
    pub candidates: Vec<ScoredNominee>,
    pub stats: Sq8ReadStats,
}

/// A query failure retains its physical charge for error reporting.
#[derive(Debug)]
pub struct RankedSq8Failure {
    pub error: RangeFetchError,
    pub stats: Sq8ReadStats,
}

/// A returned range whose ETag, offsets, length and page digests passed.
pub struct VerifiedRange {
    pub start: usize,
    pub bytes: Bytes,
}

/// Cumulative native transport observations across all production readers in this
/// process. Concurrent queries overlap: these are never per-query charges.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct NativeTransportStats {
    /// Submitted HttpService calls, including scheme/preconnect failures; not
    /// proof that a request reached the wire or S3.
    pub attempts: u64,
    /// GET, HEAD, PUT, DELETE, POST, PATCH, OPTIONS, CONNECT, TRACE, other.
    pub method_counts: [u64; 10],
    /// Nonzero (HTTP status code, count) pairs from a fixed 100..=999 histogram.
    pub status_counts: Vec<(u16, u64)>,
    /// Service errors, including header-policy refusals before body consumption.
    pub transport_failures: u64,
    pub stream_failures: u64,
    pub consumed_payload_bytes: u64,
    /// Non-success response bodies discarded at headers, without reading them.
    pub dropped_error_bodies: u64,
}

#[derive(Debug)]
struct TransportCounters {
    attempts: u64,
    methods: [u64; 10],
    statuses: [u64; 900],
    transport_failures: u64,
    stream_failures: u64,
    consumed_payload_bytes: u64,
    dropped_error_bodies: u64,
}

impl Default for TransportCounters {
    fn default() -> Self {
        Self {
            attempts: 0,
            methods: [0; 10],
            statuses: [0; 900],
            transport_failures: 0,
            stream_failures: 0,
            consumed_payload_bytes: 0,
            dropped_error_bodies: 0,
        }
    }
}

// ponytail: one short lock per observation makes snapshots coherent; use
// atomics if measured contention warrants giving up that snapshot guarantee.
static PROCESS_TRANSPORT: LazyLock<Arc<Mutex<TransportCounters>>> =
    LazyLock::new(|| Arc::new(Mutex::new(TransportCounters::default())));

/// S3 reader with SDK and reqwest retries disabled. Logical reads and consumed
/// payload observations remain separate; callers do not retry whole queries.
pub struct OneAttemptS3 {
    store: PrefixStore<AmazonS3>,
    counters: Arc<Mutex<TransportCounters>>,
}

/// Explicit opt-in for readonly data requests; status and body-resume retries
/// share the SDK's single retry counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum S3ReadRetryPolicy {
    #[serde(rename = "one_retry_100ms")]
    OneRetry100Ms,
}

impl S3ReadRetryPolicy {
    fn retry_config(self) -> RetryConfig {
        RetryConfig {
            max_retries: 1,
            backoff: object_store::BackoffConfig {
                init_backoff: std::time::Duration::from_millis(100),
                max_backoff: std::time::Duration::from_millis(100),
                ..Default::default()
            },
            // SDK checks before backoff: this is not a hard request deadline.
            retry_timeout: std::time::Duration::from_secs(1),
        }
    }
}

/// Readonly S3 transport with at most two submissions per logical GET/HEAD,
/// including body resume. Mutations are refused before reqwest submission.
pub struct BoundedReadS3 {
    store: PrefixStore<AmazonS3>,
    counters: Arc<Mutex<TransportCounters>>,
}

impl BoundedReadS3 {
    pub fn new_with_prefix(
        bucket: &str,
        region: &str,
        prefix: Path,
        policy: S3ReadRetryPolicy,
    ) -> Result<Self, RangeFetchError> {
        let counters = PROCESS_TRANSPORT.clone();
        let timeout = std::time::Duration::from_secs(30);
        // Keep IMDS (including its token PUT) on the existing zero-retry client.
        let credentials = one_attempt_builder(bucket, region, counters.clone(), timeout)?
            .build()
            .map_err(RangeFetchError::Store)?
            .credentials()
            .clone();
        let store = native_builder(bucket, region, counters.clone(), timeout, Some(policy))?
            .with_credentials(credentials)
            .build()
            .map_err(RangeFetchError::Store)?;
        Ok(Self {
            store: PrefixStore::new(store, prefix),
            counters,
        })
    }

    pub fn store(&self) -> &dyn ObjectStore {
        &self.store
    }

    /// Same cumulative process observations as the zero-retry reader.
    pub fn transport_stats(&self) -> NativeTransportStats {
        transport_snapshot(&self.counters)
    }
}

#[derive(Debug)]
struct NativeConnector {
    client: reqwest::Client,
    counters: Arc<Mutex<TransportCounters>>,
    read_only: bool,
}

impl HttpConnector for NativeConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(NativeHttp {
            client: self.client.clone(),
            counters: self.counters.clone(),
            read_only: self.read_only,
            allow_http: options
                .get_config_value(&ClientConfigKey::AllowHttp)
                .as_deref()
                == Some("true"),
        }))
    }
}

#[derive(Debug)]
struct NativeHttp {
    client: reqwest::Client,
    counters: Arc<Mutex<TransportCounters>>,
    allow_http: bool,
    read_only: bool,
}

#[async_trait::async_trait]
impl HttpService for NativeHttp {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        {
            let mut counters = self.counters.lock().unwrap();
            counters.attempts = counters.attempts.saturating_add(1);
            let method = match request.method().as_str() {
                "GET" => 0,
                "HEAD" => 1,
                "PUT" => 2,
                "DELETE" => 3,
                "POST" => 4,
                "PATCH" => 5,
                "OPTIONS" => 6,
                "CONNECT" => 7,
                "TRACE" => 8,
                _ => 9,
            };
            counters.methods[method] = counters.methods[method].saturating_add(1);
        }
        if self.read_only && !matches!(request.method().as_str(), "GET" | "HEAD") {
            let mut counters = self.counters.lock().unwrap();
            counters.transport_failures = counters.transport_failures.saturating_add(1);
            return Err(HttpError::new(
                HttpErrorKind::Decode,
                std::io::Error::other("bounded read transport refuses mutations"),
            ));
        }
        let range = request.headers().get("range").cloned();
        let etag = request.headers().get("if-match").cloned();
        if request.uri().scheme_str() != Some("https")
            && !(self.allow_http && request.uri().scheme_str() == Some("http"))
        {
            let mut counters = self.counters.lock().unwrap();
            counters.transport_failures = counters.transport_failures.saturating_add(1);
            return Err(HttpError::new(
                HttpErrorKind::Request,
                std::io::Error::other("native transport requires HTTPS"),
            ));
        }
        let mut response = HttpService::call(&self.client, request)
            .await
            .inspect_err(|_| {
                let mut counters = self.counters.lock().unwrap();
                counters.transport_failures = counters.transport_failures.saturating_add(1);
            })?;
        {
            let mut counters = self.counters.lock().unwrap();
            let status = usize::from(response.status().as_u16()) - 100;
            counters.statuses[status] = counters.statuses[status].saturating_add(1);
            if !response.status().is_success() {
                counters.dropped_error_bodies = counters.dropped_error_bodies.saturating_add(1);
            }
        }
        if self.read_only && response.status().is_success() {
            let exact = match range.as_ref() {
                Some(range) => range
                    .to_str()
                    .ok()
                    .is_some_and(|range| exact_read_response(Some(range), &response)),
                None => exact_read_response(None, &response),
            };
            if !exact
                || etag
                    .as_ref()
                    .is_some_and(|etag| response.headers().get("etag") != Some(etag))
            {
                let mut counters = self.counters.lock().unwrap();
                counters.transport_failures = counters.transport_failures.saturating_add(1);
                // Decode is terminal in the SDK; never consume a refused body.
                return Err(HttpError::new(
                    HttpErrorKind::Decode,
                    std::io::Error::other("bounded read response range/length/ETag mismatch"),
                ));
            }
        }
        // SDK error handling collects bodies before the range guard. Drop them
        // at headers, retaining status and headers for its normal error mapping.
        if !response.status().is_success() {
            *response.body_mut() = HttpResponseBody::from(Bytes::new());
        } else {
            let body = std::mem::replace(response.body_mut(), HttpResponseBody::from(Bytes::new()));
            let chunks = self.counters.clone();
            let failures = self.counters.clone();
            *response.body_mut() = HttpResponseBody::new(
                body.map_frame(move |frame| {
                    if let Some(data) = frame.data_ref() {
                        let mut counters = chunks.lock().unwrap();
                        counters.consumed_payload_bytes = counters
                            .consumed_payload_bytes
                            .saturating_add(data.len() as u64);
                    }
                    frame
                })
                .map_err(move |error| {
                    let mut counters = failures.lock().unwrap();
                    counters.stream_failures = counters.stream_failures.saturating_add(1);
                    error
                }),
            );
        }
        Ok(response)
    }
}

// Check every underlying 206, including SDK resume, before any body polling.
fn exact_read_response(range: Option<&str>, response: &HttpResponse) -> bool {
    let Some(range) = range else {
        return response.status().as_u16() != 206;
    };
    let parse = |range: &str| -> Option<(u64, u64)> {
        let (first, last) = range.split_once('-')?;
        Some((first.parse().ok()?, last.parse().ok()?))
    };
    let Some((first, last)) = range.strip_prefix("bytes=").and_then(parse) else {
        return false;
    };
    let Some((actual, total)) = response
        .headers()
        .get("content-range")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("bytes "))
        .and_then(|v| v.split_once('/'))
    else {
        return false;
    };
    response.status().as_u16() == 206
        && first <= last
        && parse(actual) == Some((first, last))
        && total.parse::<u64>().is_ok_and(|total| last < total)
        && response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            == last.checked_sub(first).and_then(|n| n.checked_add(1))
}

fn one_attempt_builder(
    bucket: &str,
    region: &str,
    counters: Arc<Mutex<TransportCounters>>,
    timeout: std::time::Duration,
) -> Result<AmazonS3Builder, RangeFetchError> {
    native_builder(bucket, region, counters, timeout, None)
}

fn native_builder(
    bucket: &str,
    region: &str,
    counters: Arc<Mutex<TransportCounters>>,
    timeout: std::time::Duration,
    policy: Option<S3ReadRetryPolicy>,
) -> Result<AmazonS3Builder, RangeFetchError> {
    if bucket.is_empty() || region.is_empty() || timeout.is_zero() {
        return Err(RangeFetchError::UnexpectedMetadata);
    }
    // Fixed native transport: caller-bounded request/5s connect, HTTP/1,
    // verified system TLS, and no redirects or transport-level retries.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .http1_only()
        .timeout(timeout)
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|source| {
            RangeFetchError::Store(object_store::Error::Generic {
                store: "native S3 transport",
                source: Box::new(source),
            })
        })?;
    Ok(AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        // ponytail: one copy part, at most 5 GiB per copied sidecar; split parts
        // only when an admitted workload requires larger sidecars.
        .with_copy_if_not_exists(S3CopyIfNotExists::Multipart)
        .with_http_connector(NativeConnector {
            client,
            counters,
            read_only: policy.is_some(),
        })
        .with_retry(policy.map_or_else(
            || RetryConfig {
                max_retries: 0,
                ..Default::default()
            },
            S3ReadRetryPolicy::retry_config,
        )))
}

fn transport_snapshot(counters: &Mutex<TransportCounters>) -> NativeTransportStats {
    let counters = counters.lock().unwrap();
    NativeTransportStats {
        attempts: counters.attempts,
        method_counts: counters.methods,
        status_counts: counters
            .statuses
            .iter()
            .enumerate()
            .filter(|(_, count)| **count != 0)
            .map(|(index, count)| ((index + 100) as u16, *count))
            .collect(),
        transport_failures: counters.transport_failures,
        stream_failures: counters.stream_failures,
        consumed_payload_bytes: counters.consumed_payload_bytes,
        dropped_error_bodies: counters.dropped_error_bodies,
    }
}

impl OneAttemptS3 {
    /// Process totals since the first native reader was created. Only chunks
    /// actually delivered by the body count, including later-rejected payloads.
    /// Unread/cancelled bodies, headers and kernel/TLS wire bytes are unknown.
    /// Counters saturate at u64::MAX and never reset between queries.
    pub fn transport_stats(&self) -> NativeTransportStats {
        transport_snapshot(&self.counters)
    }

    /// Reuse the same bounded transport for metadata and HEAD admission.
    pub fn store(&self) -> &dyn ObjectStore {
        &self.store
    }
    pub fn new(bucket: &str, region: &str) -> Result<Self, RangeFetchError> {
        Self::new_with_prefix(bucket, region, Path::default())
    }

    /// Resolve logical generation keys beneath a physical S3 namespace.
    /// Returned object paths remain logical; metadata and range reads share
    /// the same no-retry transport and process counters. Empty prefix is identity.
    pub fn new_with_prefix(
        bucket: &str,
        region: &str,
        prefix: Path,
    ) -> Result<Self, RangeFetchError> {
        Self::new_with_prefix_and_timeout(
            bucket,
            region,
            prefix,
            std::time::Duration::from_secs(30),
        )
    }

    /// Whole-object publication may need a longer, prospectively admitted total
    /// request deadline. Queries retain the default 30 seconds. Credentials are
    /// instance-role IMDS only; no environment/profile credential discovery.
    pub fn new_with_prefix_and_timeout(
        bucket: &str,
        region: &str,
        prefix: Path,
        timeout: std::time::Duration,
    ) -> Result<Self, RangeFetchError> {
        let counters = PROCESS_TRANSPORT.clone();
        let store = one_attempt_builder(bucket, region, counters.clone(), timeout)?
            .build()
            .map_err(RangeFetchError::Store)?;
        Ok(Self {
            store: PrefixStore::new(store, prefix),
            counters,
        })
    }

    pub async fn fetch_verified_pages(
        &self,
        location: &Path,
        authority: &PageAuthority,
        first_page: usize,
        last_page: usize,
        etag: &str,
        max_bytes: usize,
    ) -> Result<VerifiedRange, RangeFetchError> {
        fetch_verified_pages_inner(
            &self.store,
            location,
            authority,
            first_page,
            last_page,
            etag,
            max_bytes,
        )
        .await
    }

    /// Fetch sorted, disjoint authenticated source or SQ8 ranges. Byte and GET
    /// caps are admitted before I/O; caller retains at most max_bytes of payload.
    /// No retries, detached tasks, scoring or application cache are added.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_verified_ranges(
        &self,
        location: &Path,
        authority: &PageAuthority,
        ranges: &[(usize, usize)],
        etag: &str,
        max_gets: usize,
        max_bytes: usize,
        max_parallel: usize,
    ) -> Result<(Vec<VerifiedRange>, Sq8ReadStats), RankedSq8Failure> {
        fetch_verified_ranges_inner(
            &self.store,
            location,
            authority,
            ranges,
            etag,
            max_gets,
            max_bytes,
            max_parallel,
        )
        .await
    }

    /// Fetch only caller-selected pages from a separately authenticated
    /// generation. The total byte and GET caps are checked before any request.
    #[allow(clippy::too_many_arguments)]
    pub async fn rank_verified_sq8_pages(
        &self,
        location: &Path,
        authority: &PageAuthority,
        ranges: &[(usize, usize)],
        etag: &str,
        query: &[f32],
        low: &[f32],
        step: &[f32],
        top_k: usize,
        max_gets: usize,
        max_bytes: usize,
        max_parallel: usize,
    ) -> Result<RankedSq8, RankedSq8Failure> {
        let ranked = self
            .rank_verified_sq8_pages_excluding(
                location,
                authority,
                ranges,
                etag,
                query,
                low,
                step,
                top_k,
                max_gets,
                max_bytes,
                max_parallel,
                &[],
            )
            .await?;
        if ranked.candidates.len() < top_k {
            return Err(RankedSq8Failure {
                error: RangeFetchError::Score(Sq8ScoreError::InvalidRoster),
                stats: ranked.stats,
            });
        }
        Ok(ranked)
    }

    /// Apply a sorted unique logical-ID exclusion roster before top-k. The
    /// caller authenticates and admits its immutable snapshot separately. Underfill
    /// is returned explicitly as fewer candidates, without additional GETs.
    #[allow(clippy::too_many_arguments)]
    pub async fn rank_verified_sq8_pages_excluding(
        &self,
        location: &Path,
        authority: &PageAuthority,
        ranges: &[(usize, usize)],
        etag: &str,
        query: &[f32],
        low: &[f32],
        step: &[f32],
        top_k: usize,
        max_gets: usize,
        max_bytes: usize,
        max_parallel: usize,
        excluded_ids: &[i64],
    ) -> Result<RankedSq8, RankedSq8Failure> {
        rank_verified_sq8_pages_inner(
            &self.store,
            location,
            authority,
            ranges,
            etag,
            query,
            low,
            step,
            top_k,
            max_gets,
            max_bytes,
            max_parallel,
            excluded_ids,
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn rank_verified_sq8_pages_inner(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    ranges: &[(usize, usize)],
    etag: &str,
    query: &[f32],
    low: &[f32],
    step: &[f32],
    top_k: usize,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
    excluded_ids: &[i64],
) -> Result<RankedSq8, RankedSq8Failure> {
    rank_verified_sq8_pages_traced(
        store,
        location,
        authority,
        ranges,
        etag,
        query,
        low,
        step,
        top_k,
        max_gets,
        max_bytes,
        max_parallel,
        excluded_ids,
        None,
    )
    .await
}

/// The same fetch, authentication, scoring and ranking as the ordinary path.
/// `trace` only observes: ordinary callers pass `None` and read no clock.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn rank_verified_sq8_pages_traced(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    ranges: &[(usize, usize)],
    etag: &str,
    query: &[f32],
    low: &[f32],
    step: &[f32],
    top_k: usize,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
    excluded_ids: &[i64],
    trace: Option<&mut Sq8RangeTrace>,
) -> Result<RankedSq8, RankedSq8Failure> {
    rank_verified_sq8_pages_traced_with_byte_window(
        store,
        location,
        authority,
        ranges,
        etag,
        query,
        low,
        step,
        top_k,
        max_gets,
        max_bytes,
        max_parallel,
        excluded_ids,
        trace,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn rank_verified_sq8_pages_traced_with_byte_window(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    ranges: &[(usize, usize)],
    etag: &str,
    query: &[f32],
    low: &[f32],
    step: &[f32],
    top_k: usize,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
    excluded_ids: &[i64],
    trace: Option<&mut Sq8RangeTrace>,
    budget: Option<&Arc<InFlightBytes>>,
) -> Result<RankedSq8, RankedSq8Failure> {
    rank_verified_sq8_pages_traced_with_split(
        store,
        location,
        authority,
        ranges,
        etag,
        query,
        low,
        step,
        top_k,
        max_gets,
        max_bytes,
        max_parallel,
        excluded_ids,
        trace,
        budget,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn rank_verified_sq8_pages_traced_with_split(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    ranges: &[(usize, usize)],
    etag: &str,
    query: &[f32],
    low: &[f32],
    step: &[f32],
    top_k: usize,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
    excluded_ids: &[i64],
    mut trace: Option<&mut Sq8RangeTrace>,
    budget: Option<&Arc<InFlightBytes>>,
    split: Option<Sq8RangeSplit>,
) -> Result<RankedSq8, RankedSq8Failure> {
    let fail = |error| RankedSq8Failure {
        error,
        stats: Sq8ReadStats::default(),
    };
    if let Some(trace) = trace.as_deref_mut() {
        trace.outcome = "rejected_before_io";
    }
    if excluded_ids.windows(2).any(|ids| ids[0] >= ids[1]) {
        return Err(fail(RangeFetchError::Score(Sq8ScoreError::InvalidRoster)));
    }
    if !authority.is_sq8()
        || ranges.is_empty()
        || ranges.len() > max_gets
        || max_parallel == 0
        || etag.is_empty()
    {
        return Err(fail(RangeFetchError::UnexpectedMetadata));
    }
    if top_k == 0 || top_k > authority.rows() {
        return Err(fail(RangeFetchError::Score(Sq8ScoreError::InvalidGeometry)));
    }
    if query.len() != authority.dimensions()
        || low.len() != authority.dimensions()
        || step.len() != authority.dimensions()
        || query.iter().chain(low).any(|value| !value.is_finite())
        || step.iter().any(|value| !value.is_finite() || *value <= 0.0)
    {
        return Err(fail(RangeFetchError::Score(Sq8ScoreError::InvalidQuery)));
    }
    let mut physical = Vec::new();
    if let Some(split) = split {
        split
            .scratch_bytes(max_bytes, max_parallel)
            .ok_or_else(|| fail(RangeFetchError::UnexpectedMetadata))?;
        // Check the byte window in the allocation-free count pass as well.
        let mut window_refused = false;
        let count = split
            .visit(authority, ranges, max_gets, max_bytes, |(first, last)| {
                let interval = authority.byte_range(first, last).expect("admitted split");
                if budget.is_some_and(|budget| interval.end - interval.start > budget.capacity) {
                    window_refused = true;
                }
            })
            .map_err(fail)?;
        if window_refused
            || trace
                .as_deref()
                .is_some_and(|trace| count > trace.capacity())
        {
            return Err(fail(RangeFetchError::UnexpectedMetadata));
        }
        physical = Vec::with_capacity(count);
        split
            .visit(authority, ranges, max_gets, max_bytes, |range| {
                physical.push(range)
            })
            .map_err(fail)?;
    }
    let fetch_ranges = if split.is_some() {
        physical.as_slice()
    } else {
        ranges
    };
    let (mut verified, stats) = fetch_verified_ranges_traced_with_byte_window(
        store,
        location,
        authority,
        fetch_ranges,
        etag,
        split.map_or(max_gets, |split| split.max_physical_gets),
        max_bytes,
        max_parallel,
        trace.as_deref_mut(),
        budget,
    )
    .await?;
    let regroup = trace
        .as_deref()
        .filter(|_| split.is_some())
        .map(|_| SyncStart::now());
    if split.is_some() {
        // Preserve the exact original scoring groups, including kernel tail behavior.
        let mut pieces = verified.into_iter();
        verified = Vec::with_capacity(ranges.len());
        for &(first, last) in ranges {
            let interval = authority
                .byte_range(first, last)
                .map_err(|error| RankedSq8Failure {
                    error: RangeFetchError::Page(error),
                    stats,
                })?;
            let mut bytes = BytesMut::with_capacity(interval.end - interval.start);
            while bytes.len() < interval.end - interval.start {
                let piece = pieces.next().expect("authenticated split coverage");
                assert_eq!(piece.start, interval.start + bytes.len());
                bytes.extend_from_slice(&piece.bytes);
            }
            verified.push(VerifiedRange {
                start: interval.start,
                bytes: bytes.freeze(),
            });
        }
        assert!(pieces.next().is_none());
    }
    if let (Some(trace), Some(regroup)) = (trace.as_deref_mut(), regroup) {
        regroup.finish(&mut trace.regroup);
        trace.regroup_copied_bytes = stats.verified_bytes as u64;
    }
    if let Some(trace) = trace.as_deref_mut() {
        trace.rank_start_ns = Some(trace.now_ns());
    }
    let rank_cpu = trace.as_deref().map(|_| thread_cpu_ns());
    let returned = verified
        .iter()
        .map(|range| ReturnedRange {
            start: range.start,
            bytes: &range.bytes,
        })
        .collect::<Vec<_>>();
    let mut phases = trace.is_some().then(RankPhases::default);
    let ranked = rank_returned_ranges_excluding_traced(
        Sq8Geometry {
            rows: authority.rows(),
            dimensions: authority.dimensions(),
        },
        &returned,
        query,
        low,
        step,
        top_k,
        max_bytes,
        excluded_ids,
        phases.as_mut(),
    );
    let rank_cpu = rank_cpu.map(|start| thread_cpu_ns().saturating_sub(start));
    if let Some(trace) = trace.as_deref_mut() {
        trace.rank_thread_cpu_ns = rank_cpu;
        trace.rank_clock_reads = 2;
        trace.rank_end_ns = Some(trace.now_ns());
    }
    // The payload is freed here, in the order the end of the function always freed it (the
    // views, then the verified buffers): earlier than the return on both the success and the
    // error path, never later. A traced query stamps the boundary; nothing else changes.
    drop(returned);
    drop(verified);
    if let Some(trace) = trace.as_deref_mut() {
        trace.release_end_ns = Some(trace.now_ns());
        trace.rank = phases;
        trace.outcome = if ranked.is_ok() {
            "ranked"
        } else {
            "rank_failed"
        };
    }
    let candidates = ranked.map_err(|error| RankedSq8Failure {
        error: RangeFetchError::Score(error),
        stats,
    })?;
    Ok(RankedSq8 { candidates, stats })
}

/// Shared bounded range transport; all requests finish within this future.
/// Fetch authenticated ranges with a byte window shared by concurrent callers.
/// Reject any range larger than the window before issuing any request.
#[allow(clippy::too_many_arguments)]
pub async fn fetch_verified_ranges_with_byte_window(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    ranges: &[(usize, usize)],
    etag: &str,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
    budget: &Arc<InFlightBytes>,
) -> Result<(Vec<VerifiedRange>, Sq8ReadStats), RankedSq8Failure> {
    fetch_verified_ranges_traced_with_byte_window(
        store,
        location,
        authority,
        ranges,
        etag,
        max_gets,
        max_bytes,
        max_parallel,
        None,
        Some(budget),
    )
    .await
}

/// Shared bounded range transport; all requests finish within this future.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_verified_ranges_inner(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    ranges: &[(usize, usize)],
    etag: &str,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
) -> Result<(Vec<VerifiedRange>, Sq8ReadStats), RankedSq8Failure> {
    fetch_verified_ranges_traced(
        store,
        location,
        authority,
        ranges,
        etag,
        max_gets,
        max_bytes,
        max_parallel,
        None,
    )
    .await
}

/// `fetch_verified_ranges_inner` plus opt-in diagnostics. Every range future is
/// the production one, polled by the same completion-driven combinator; spans are
/// returned by value from each future, so no shared state is touched while
/// requests are in flight. All requests drain before any outcome is examined.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_verified_ranges_traced(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    ranges: &[(usize, usize)],
    etag: &str,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
    trace: Option<&mut Sq8RangeTrace>,
) -> Result<(Vec<VerifiedRange>, Sq8ReadStats), RankedSq8Failure> {
    fetch_verified_ranges_traced_with_byte_window(
        store,
        location,
        authority,
        ranges,
        etag,
        max_gets,
        max_bytes,
        max_parallel,
        trace,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_verified_ranges_traced_with_byte_window(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    ranges: &[(usize, usize)],
    etag: &str,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
    trace: Option<&mut Sq8RangeTrace>,
    budget: Option<&Arc<InFlightBytes>>,
) -> Result<(Vec<VerifiedRange>, Sq8ReadStats), RankedSq8Failure> {
    fetch_verified_ranges_with_authentication(
        store,
        location,
        authority,
        ranges,
        etag,
        max_gets,
        max_bytes,
        max_parallel,
        trace,
        budget,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_source_ranges_traced(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &Arc<PageAuthority>,
    ranges: &[(usize, usize)],
    etag: &str,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
    trace: Option<&mut Sq8RangeTrace>,
    budget: Option<&Arc<InFlightBytes>>,
    authentication: SourceAuthentication,
) -> Result<(Vec<VerifiedRange>, Sq8ReadStats), RankedSq8Failure> {
    fetch_verified_ranges_with_authentication(
        store,
        location,
        authority,
        ranges,
        etag,
        max_gets,
        max_bytes,
        max_parallel,
        trace,
        budget,
        (authentication == SourceAuthentication::Offload2).then_some(authority),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn fetch_verified_ranges_with_authentication(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    ranges: &[(usize, usize)],
    etag: &str,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
    mut trace: Option<&mut Sq8RangeTrace>,
    budget: Option<&Arc<InFlightBytes>>,
    offload_authority: Option<&Arc<PageAuthority>>,
) -> Result<(Vec<VerifiedRange>, Sq8ReadStats), RankedSq8Failure> {
    let fail = |error| RankedSq8Failure {
        error,
        stats: Sq8ReadStats::default(),
    };
    if let Some(trace) = trace.as_deref_mut() {
        trace.outcome = "rejected_before_io";
    }
    if ranges.is_empty() || ranges.len() > max_gets || max_parallel == 0 || etag.is_empty() {
        return Err(fail(RangeFetchError::UnexpectedMetadata));
    }
    // A trace holds a fixed number of spans and the memory model charges exactly that many
    // probes: a plan it cannot hold is refused before any future exists, never partly traced.
    if trace
        .as_deref()
        .is_some_and(|trace| ranges.len() > trace.capacity)
    {
        return Err(fail(RangeFetchError::UnexpectedMetadata));
    }
    let mut planned_bytes = 0usize;
    let mut previous_last = None;
    for &(first, last) in ranges {
        if first > last || previous_last.is_some_and(|previous| first <= previous) {
            return Err(fail(RangeFetchError::UnexpectedMetadata));
        }
        let interval = authority
            .byte_range(first, last)
            .map_err(|error| fail(RangeFetchError::Page(error)))?;
        if budget.is_some_and(|budget| interval.end - interval.start > budget.capacity) {
            return Err(fail(RangeFetchError::UnexpectedMetadata));
        }
        planned_bytes = planned_bytes
            .checked_add(interval.end - interval.start)
            .ok_or_else(|| fail(RangeFetchError::UnexpectedMetadata))?;
        if planned_bytes > max_bytes {
            return Err(fail(RangeFetchError::UnexpectedMetadata));
        }
        previous_last = Some(last);
    }
    let workers = if offload_authority.is_some() {
        Some(
            SourceAuthWorkers::new(max_parallel.min(ranges.len()))
                .await
                .map_err(fail)?,
        )
    } else {
        None
    };
    let scheduled = max_parallel.saturating_add(if workers.is_some() {
        SOURCE_AUTH_WORKERS
    } else {
        0
    });
    let origin = trace.as_deref_mut().map(|trace| {
        trace.outcome = "fetching";
        trace.planned_ranges = u32::try_from(ranges.len()).unwrap_or(u32::MAX);
        trace.planned_bytes = u64::try_from(planned_bytes).unwrap_or(u64::MAX);
        trace.max_parallel = u32::try_from(max_parallel.min(ranges.len())).unwrap_or(u32::MAX);
        trace.fetch_start_ns = Some(trace.now_ns());
        trace.origin
    });
    // The indexed future has a named type-producing function so the layout test measures
    // the actual scheduler element, including its plan index.
    let outcomes = if let (Some(workers), Some(authority)) = (workers.as_ref(), offload_authority) {
        drain_ordered(
            ranges
                .iter()
                .copied()
                .enumerate()
                .map(|(index, (first, last))| {
                    let future = fetch_source_pages_offloaded(
                        store, location, authority, first, last, etag, max_bytes, index, origin,
                        budget, workers,
                    );
                    debug_assert!(
                        std::mem::size_of_val(&future)
                            + BUFFERED_NODE_ALLOWANCE_BYTES
                            + ALLOCATION_ALLOWANCE_BYTES
                            <= SOURCE_AUTH_FUTURE_BYTES
                    );
                    future
                }),
            scheduled.min(ranges.len()),
            |(index, _, _)| *index,
        )
        .await
    } else {
        drain_ordered(
            ranges
                .iter()
                .copied()
                .enumerate()
                .map(|(index, (first, last))| {
                    fetch_verified_pages_admitted_indexed(
                        store, location, authority, first, last, etag, max_bytes, index, origin,
                        budget,
                    )
                }),
            max_parallel.min(ranges.len()),
            |(index, _, _)| *index,
        )
        .await
    };
    if let Some(trace) = trace.as_deref_mut() {
        trace.all_ranges_complete_ns = Some(trace.now_ns());
    }
    let mut stats = Sq8ReadStats::default();
    let mut verified = Vec::with_capacity(ranges.len());
    let mut first_error = None;
    for (_, submitted, (outcome, probe)) in outcomes {
        stats.submitted_gets += usize::from(submitted);
        if let (Some(trace), Some(probe)) = (trace.as_deref_mut(), probe) {
            trace.push_probe(*probe);
        }
        match outcome {
            Ok(value) => {
                stats.verified_bytes += value.bytes.len();
                verified.push(value);
            }
            Err(error) => {
                stats.failed_gets += usize::from(submitted);
                first_error.get_or_insert(error);
            }
        }
    }
    if let Some(trace) = trace.as_deref_mut() {
        trace.retained_payload_bytes = Some(stats.verified_bytes as u64);
        trace.outcome = if first_error.is_some() {
            "fetch_failed"
        } else {
            "fetched"
        };
    }
    if let Some(error) = first_error {
        return Err(RankedSq8Failure { error, stats });
    }
    Ok((verified, stats))
}

#[allow(clippy::too_many_arguments)]
async fn fetch_source_pages_offloaded(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &Arc<PageAuthority>,
    first: usize,
    last: usize,
    etag: &str,
    max_bytes: usize,
    index: usize,
    origin: Option<Instant>,
    budget: Option<&Arc<InFlightBytes>>,
    workers: &SourceAuthWorkers,
) -> IndexedRangeOutcome {
    let expected = match authority.byte_range(first, last) {
        Ok(range) => range,
        Err(error) => return (index, false, (Err(RangeFetchError::Page(error)), None)),
    };
    let byte_lease = if let Some(budget) = budget {
        match budget.reserve(expected.len()).await {
            Some(permit) => Some(permit),
            None => {
                return (
                    index,
                    false,
                    (Err(RangeFetchError::UnexpectedMetadata), None),
                );
            }
        }
    } else {
        None
    };
    let mut probe = origin.map(|origin| {
        Box::new(Probe {
            origin,
            cpu: RangeCpu::default(),
            span: RangeSpan::new(index, first, last, Some(expected.clone())),
        })
    });
    let download = workers
        .downloads
        .acquire()
        .await
        .expect("SOURCE downloads stay open");
    let body = fetch_exact_range_core(
        store,
        location,
        expected.clone(),
        authority.object_bytes(),
        etag,
        max_bytes,
        probe.as_deref_mut(),
    )
    .await;
    drop(download);
    let result = match body {
        Ok(bytes) => {
            let authority = Arc::clone(authority);
            let (result, returned_probe) = workers
                .authenticate(probe, move || {
                    let _byte_lease = byte_lease;
                    authority
                        .verify_payload(first, last, &bytes)
                        .map_err(RangeFetchError::Page)?;
                    Ok(VerifiedRange {
                        start: expected.start,
                        bytes,
                    })
                })
                .await;
            probe = returned_probe;
            result
        }
        Err(error) => Err(error),
    };
    if let Some(probe) = probe.as_deref_mut() {
        probe.span.complete_ns = Some(probe.now_ns());
    }
    (index, true, (result, probe))
}

/// Fetch one inclusive S3 byte range through `object_store`'s HTTP client.
/// Its HTTP get parser requires 206 and the exact Content-Range for ranged
/// requests. We additionally verify object size, ETag, collected length and
/// SHA-256 for each page against the pinned generation sidecar.
async fn fetch_verified_pages_inner(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    first_page: usize,
    last_page: usize,
    etag: &str,
    max_bytes: usize,
) -> Result<VerifiedRange, RangeFetchError> {
    fetch_verified_pages_core(
        store, location, authority, first_page, last_page, etag, max_bytes, None,
    )
    .await
}

/// One range with an optional probe. The probe is boxed, allocated only when
/// tracing, and handed back with the result: an ordinary range future holds one
/// null pointer where a traced one holds its diagnostic record. Its boxed record
/// stays alive until every range has drained and the caller has collected it.
#[allow(clippy::too_many_arguments)]
async fn fetch_verified_pages_traced(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    first_page: usize,
    last_page: usize,
    etag: &str,
    max_bytes: usize,
    probe: Option<(Instant, usize)>,
) -> RangeOutcome {
    let mut probe = probe.map(|(origin, index)| {
        Box::new(Probe {
            origin,
            cpu: RangeCpu::default(),
            span: RangeSpan::new(
                index,
                first_page,
                last_page,
                authority.byte_range(first_page, last_page).ok(),
            ),
        })
    });
    let result = fetch_verified_pages_core(
        store,
        location,
        authority,
        first_page,
        last_page,
        etag,
        max_bytes,
        probe.as_deref_mut(),
    )
    .await;
    if let Some(probe) = probe.as_deref_mut() {
        probe.span.complete_ns = Some(probe.now_ns());
    }
    (result, probe)
}

/// A finished range: its result and, only when tracing, its diagnostic record.
type RangeOutcome = (Result<VerifiedRange, RangeFetchError>, Option<Box<Probe>>);

type IndexedRangeOutcome = (usize, bool, RangeOutcome);

#[allow(clippy::too_many_arguments)]
async fn fetch_verified_pages_admitted_indexed(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    first_page: usize,
    last_page: usize,
    etag: &str,
    max_bytes: usize,
    index: usize,
    origin: Option<Instant>,
    budget: Option<&Arc<InFlightBytes>>,
) -> IndexedRangeOutcome {
    let _permit = if let Some(budget) = budget {
        let interval = match authority.byte_range(first_page, last_page) {
            Ok(interval) => interval,
            Err(error) => return (index, false, (Err(RangeFetchError::Page(error)), None)),
        };
        match budget.reserve(interval.end - interval.start).await {
            Some(permit) => Some(permit),
            None => {
                return (
                    index,
                    false,
                    (Err(RangeFetchError::UnexpectedMetadata), None),
                );
            }
        }
    } else {
        None
    };
    fetch_verified_pages_indexed(
        store, location, authority, first_page, last_page, etag, max_bytes, index, origin,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn fetch_verified_pages_indexed(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    first_page: usize,
    last_page: usize,
    etag: &str,
    max_bytes: usize,
    index: usize,
    origin: Option<Instant>,
) -> IndexedRangeOutcome {
    let outcome = fetch_verified_pages_traced(
        store,
        location,
        authority,
        first_page,
        last_page,
        etag,
        max_bytes,
        origin.map(|origin| (origin, index)),
    )
    .await;
    (index, true, outcome)
}

#[allow(clippy::too_many_arguments)]
async fn fetch_verified_pages_core(
    store: &dyn ObjectStore,
    location: &Path,
    authority: &PageAuthority,
    first_page: usize,
    last_page: usize,
    etag: &str,
    max_bytes: usize,
    mut probe: Option<&mut Probe>,
) -> Result<VerifiedRange, RangeFetchError> {
    let expected = authority
        .byte_range(first_page, last_page)
        .map_err(RangeFetchError::Page)?;
    let bytes = fetch_exact_range_core(
        store,
        location,
        expected.clone(),
        authority.object_bytes(),
        etag,
        max_bytes,
        probe.as_deref_mut(),
    )
    .await?;
    let authenticated = measured_auth(&mut probe, || {
        authority.verify_payload(first_page, last_page, &bytes)
    });
    authenticated.map_err(RangeFetchError::Page)?;
    Ok(VerifiedRange {
        start: expected.start,
        bytes,
    })
}

// Physical transport has no page geometry; the caller supplies real authentication.
#[allow(clippy::too_many_arguments)]
async fn fetch_exact_range_core(
    store: &dyn ObjectStore,
    location: &Path,
    expected: std::ops::Range<usize>,
    object_bytes: usize,
    etag: &str,
    max_bytes: usize,
    mut probe: Option<&mut Probe>,
) -> Result<Bytes, RangeFetchError> {
    stamp(&mut probe, |span, ns| span.first_poll_ns = Some(ns));
    if etag.is_empty() || expected.start >= expected.end || expected.end > object_bytes {
        return Err(RangeFetchError::UnexpectedMetadata);
    }
    let expected_len = expected.end - expected.start;
    if expected_len > max_bytes {
        return Err(RangeFetchError::UnexpectedMetadata);
    }
    let start = u64::try_from(expected.start).map_err(|_| RangeFetchError::UnexpectedMetadata)?;
    let stop = u64::try_from(expected.end).map_err(|_| RangeFetchError::UnexpectedMetadata)?;
    let options = GetOptions::new()
        .with_range(Some(start..stop))
        .with_if_match(Some(etag.to_owned()));
    stamp(&mut probe, |span, ns| {
        span.request_ns = Some(ns);
        span.outcome = "store_headers";
    });
    let result = store
        .get_opts(location, options)
        .await
        .map_err(RangeFetchError::Store)?;
    stamp(&mut probe, |span, ns| {
        span.headers_ns = Some(ns);
        span.outcome = "metadata";
    });
    if result.range != (start..stop)
        || result.meta.size
            != u64::try_from(object_bytes).map_err(|_| RangeFetchError::UnexpectedMetadata)?
        || result.meta.e_tag.as_deref() != Some(etag)
    {
        return Err(RangeFetchError::UnexpectedMetadata);
    }
    let mut stream = match result.payload {
        GetResultPayload::Stream(stream) => stream,
        GetResultPayload::File(..) => return Err(RangeFetchError::UnexpectedMetadata),
    };
    stamp(&mut probe, |span, ns| {
        span.metadata_ns = Some(ns);
        span.outcome = "store_body";
    });
    let allocation = probe.as_deref().map(|_| SyncStart::now());
    let mut collected = BytesMut::with_capacity(expected_len);
    if let (Some(probe), Some(allocation)) = (probe.as_deref_mut(), allocation) {
        allocation.finish(&mut probe.cpu.collector_allocation);
    }
    let mut mark = probe.as_deref().map(Probe::now_ns);
    loop {
        let next = stream.next().await;
        let arrived = probe.as_deref().map(Probe::now_ns);
        if let (Some(probe), Some(from), Some(to)) = (probe.as_deref_mut(), mark, arrived) {
            probe.span.note_wait(from, to);
        }
        let Some(next) = next else {
            if let (Some(probe), Some(ns)) = (probe.as_deref_mut(), arrived) {
                probe.span.eof_ns = Some(ns);
            }
            break;
        };
        let chunk = next.map_err(RangeFetchError::Store)?;
        if let (Some(probe), Some(ns)) = (probe.as_deref_mut(), arrived) {
            probe.span.note_chunk(ns, chunk.len());
        }
        let new_len = collected
            .len()
            .checked_add(chunk.len())
            .ok_or(RangeFetchError::UnexpectedMetadata)?;
        if new_len > expected_len {
            stamp(&mut probe, |span, _| span.outcome = "overlong");
            return Err(RangeFetchError::UnexpectedMetadata);
        }
        let copy = probe.as_deref().map(|_| SyncStart::now());
        collected.extend_from_slice(&chunk);
        if let (Some(probe), Some(copy)) = (probe.as_deref_mut(), copy) {
            copy.finish(&mut probe.cpu.collector_copy);
            probe.cpu.collector_copied_bytes = probe
                .cpu
                .collector_copied_bytes
                .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        }
        mark = probe.as_deref().map(Probe::now_ns);
        if let (Some(probe), Some(from), Some(to)) = (probe.as_deref_mut(), arrived, mark) {
            probe.span.note_copy(from, to);
        }
    }
    let bytes = collected.freeze();
    Ok(bytes)
}

// Include both the growing ordered-outcome vector and the retained result vector.
pub(crate) fn capsule_fetch_descriptor_bytes<T>(ranges: usize) -> Option<usize> {
    std::mem::size_of::<(
        usize,
        bool,
        (Result<T, RangeFetchError>, Option<Box<Probe>>),
    )>()
    .checked_mul(OUTCOME_VECTOR_GROWTH_FACTOR)?
    .checked_add(std::mem::size_of::<T>())?
    .checked_mul(ranges)
}

struct CapsuleRangeReader<'a, A> {
    store: &'a dyn ObjectStore,
    location: &'a Path,
    object_bytes: usize,
    etag: &'a str,
    max_bytes: usize,
    budget: Option<&'a Arc<InFlightBytes>>,
    authenticate: &'a A,
    workers: Option<&'a SourceAuthWorkers>,
    origin: Option<Instant>,
}

impl<A: Sync> CapsuleRangeReader<'_, A> {
    async fn fetch_range<T, F>(
        &self,
        index: usize,
        range: &std::ops::Range<usize>,
    ) -> (
        usize,
        bool,
        (Result<T, RangeFetchError>, Option<Box<Probe>>),
    )
    where
        A: Fn(usize, Vec<u8>, Option<Box<Probe>>, Option<InFlightPermit>) -> F,
        F: std::future::Future<Output = (Result<T, RangeFetchError>, Option<Box<Probe>>)>,
    {
        let mut byte_lease = if let Some(budget) = self.budget {
            match budget.reserve(range.len()).await {
                Some(permit) => Some(permit),
                None => {
                    return (
                        index,
                        false,
                        (Err(RangeFetchError::UnexpectedMetadata), None),
                    );
                }
            }
        } else {
            None
        };
        let mut probe = self.origin.map(|origin| {
            Box::new(Probe {
                origin,
                // Physical capsule covers do not have one logical page interval.
                span: RangeSpan::new(index, usize::MAX, usize::MAX, Some(range.clone())),
                cpu: RangeCpu::default(),
            })
        });
        let result = async {
            let download = if let Some(workers) = self.workers {
                Some(
                    workers
                        .downloads
                        .acquire()
                        .await
                        .expect("SOURCE downloads stay open"),
                )
            } else {
                None
            };
            let body = fetch_exact_range_core(
                self.store,
                self.location,
                range.clone(),
                self.object_bytes,
                self.etag,
                self.max_bytes,
                probe.as_deref_mut(),
            )
            .await?;
            drop(download);
            if body.len() != range.len() {
                return Err(RangeFetchError::UnexpectedMetadata);
            }
            let allocation = probe.as_deref().map(|_| SyncStart::now());
            let mut bytes = Vec::new();
            let allocated = bytes.try_reserve_exact(body.len());
            if let (Some(probe), Some(allocation)) = (probe.as_deref_mut(), allocation) {
                allocation.finish(&mut probe.cpu.owned_allocation);
            }
            allocated.map_err(|_| RangeFetchError::UnexpectedMetadata)?;
            // The generation admits two full framed copies; reject excess capacity.
            if bytes.capacity() > range.len() {
                return Err(RangeFetchError::UnexpectedMetadata);
            }
            let copy = probe.as_deref().map(|_| SyncStart::now());
            bytes.extend_from_slice(&body);
            if let (Some(probe), Some(copy)) = (probe.as_deref_mut(), copy) {
                copy.finish(&mut probe.cpu.owned_copy);
                probe.cpu.owned_copied_bytes = body.len() as u64;
            }
            drop(body);
            let (result, returned_probe) =
                (self.authenticate)(range.start, bytes, probe.take(), byte_lease.take()).await;
            probe = returned_probe;
            result
        }
        .await;
        if let Some(probe) = probe.as_deref_mut() {
            probe.span.complete_ns = Some(probe.now_ns());
        }
        (index, true, (result, probe))
    }
}

// Both paths refill on completion, drain all original requests and restore plan order.
async fn drain_ordered<F: std::future::Future>(
    futures: impl Iterator<Item = F>,
    parallel: usize,
    mut index: impl FnMut(&F::Output) -> usize,
) -> Vec<F::Output> {
    let mut outcomes = stream::iter(futures)
        .buffer_unordered(parallel)
        .collect::<Vec<_>>()
        .await;
    outcomes.sort_unstable_by_key(|outcome| index(outcome));
    outcomes
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_capsule_ranges<T>(
    store: &dyn ObjectStore,
    location: &Path,
    object_bytes: usize,
    ranges: &[std::ops::Range<usize>],
    etag: &str,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
    budget: Option<&Arc<InFlightBytes>>,
    authenticate: impl Fn(usize, Vec<u8>) -> Result<T, RangeFetchError> + Sync,
) -> Result<(Vec<T>, Sq8ReadStats), RankedSq8Failure> {
    fetch_capsule_ranges_traced(
        store,
        location,
        object_bytes,
        ranges,
        etag,
        max_gets,
        max_bytes,
        max_parallel,
        budget,
        None,
        authenticate,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_capsule_ranges_traced<T>(
    store: &dyn ObjectStore,
    location: &Path,
    object_bytes: usize,
    ranges: &[std::ops::Range<usize>],
    etag: &str,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
    budget: Option<&Arc<InFlightBytes>>,
    trace: Option<&mut Sq8RangeTrace>,
    authenticate: impl Fn(usize, Vec<u8>) -> Result<T, RangeFetchError> + Sync,
) -> Result<(Vec<T>, Sq8ReadStats), RankedSq8Failure> {
    fetch_capsule_ranges_with_authentication(
        store,
        location,
        object_bytes,
        ranges,
        etag,
        max_gets,
        max_bytes,
        max_parallel,
        budget,
        trace,
        None,
        |offset, bytes, probe, byte_lease| {
            std::future::ready(authenticate_inline(probe, || {
                let _byte_lease = byte_lease;
                authenticate(offset, bytes)
            }))
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_capsule_ranges_with_authentication<T, F>(
    store: &dyn ObjectStore,
    location: &Path,
    object_bytes: usize,
    ranges: &[std::ops::Range<usize>],
    etag: &str,
    max_gets: usize,
    max_bytes: usize,
    max_parallel: usize,
    budget: Option<&Arc<InFlightBytes>>,
    mut trace: Option<&mut Sq8RangeTrace>,
    workers: Option<&SourceAuthWorkers>,
    authenticate: impl Fn(usize, Vec<u8>, Option<Box<Probe>>, Option<InFlightPermit>) -> F + Sync,
) -> Result<(Vec<T>, Sq8ReadStats), RankedSq8Failure>
where
    F: std::future::Future<Output = (Result<T, RangeFetchError>, Option<Box<Probe>>)>,
{
    let fail = || RankedSq8Failure {
        error: RangeFetchError::UnexpectedMetadata,
        stats: Sq8ReadStats::default(),
    };
    if let Some(trace) = trace.as_deref_mut() {
        trace.outcome = "rejected_before_io";
    }
    if trace
        .as_deref()
        .is_some_and(|trace| ranges.len() > trace.capacity())
    {
        return Err(fail());
    }
    if ranges.is_empty()
        || ranges.len() > max_gets
        || max_parallel == 0
        || etag.is_empty()
        || etag.starts_with("W/")
        || etag.chars().any(char::is_control)
    {
        return Err(fail());
    }
    let mut bytes = 0usize;
    let mut previous = 0;
    for range in ranges {
        if range.start < previous
            || range.start >= range.end
            || range.end > object_bytes
            || budget.is_some_and(|b| range.len() > b.capacity)
        {
            return Err(fail());
        }
        bytes = bytes
            .checked_add(range.len())
            .filter(|n| *n <= max_bytes)
            .ok_or_else(fail)?;
        previous = range.end;
    }
    let origin = trace.as_deref_mut().map(|trace| {
        trace.outcome = "fetching";
        trace.planned_ranges = u32::try_from(ranges.len()).unwrap_or(u32::MAX);
        trace.planned_bytes = bytes as u64;
        trace.max_parallel = u32::try_from(max_parallel.min(ranges.len())).unwrap_or(u32::MAX);
        trace.fetch_start_ns = Some(trace.now_ns());
        trace.origin
    });
    let reader = CapsuleRangeReader {
        store,
        location,
        object_bytes,
        etag,
        max_bytes,
        budget,
        authenticate: &authenticate,
        workers,
        origin,
    };
    let outcomes = drain_ordered(
        // Keep borrowed range lifetimes out of the future factory's input.
        (0..ranges.len()).map(|index| {
            let future = reader.fetch_range(index, &ranges[index]);
            let allowance = if workers.is_some() {
                SOURCE_AUTH_FUTURE_BYTES
            } else {
                RANGE_FUTURE_ALLOWANCE_BYTES
            };
            debug_assert!(
                std::mem::size_of_val(&future)
                    + BUFFERED_NODE_ALLOWANCE_BYTES
                    + ALLOCATION_ALLOWANCE_BYTES
                    <= allowance
            );
            future
        }),
        max_parallel
            .saturating_add(if workers.is_some() {
                SOURCE_AUTH_WORKERS
            } else {
                0
            })
            .min(ranges.len()),
        |(index, _, _)| *index,
    )
    .await;
    if let Some(trace) = trace.as_deref_mut() {
        trace.all_ranges_complete_ns = Some(trace.now_ns());
    }
    let mut stats = Sq8ReadStats::default();
    let mut fetched = Vec::with_capacity(ranges.len());
    let mut first_error = None;
    for (index, submitted, (outcome, probe)) in outcomes {
        if let (Some(trace), Some(probe)) = (trace.as_deref_mut(), probe) {
            trace.push_probe(*probe);
        }
        stats.submitted_gets += usize::from(submitted);
        match outcome {
            Ok(range) => {
                stats.verified_bytes += ranges[index].len();
                fetched.push(range);
            }
            Err(error) => {
                stats.failed_gets += usize::from(submitted);
                first_error.get_or_insert(error);
            }
        }
    }
    if let Some(trace) = trace.as_deref_mut() {
        trace.retained_payload_bytes = Some(stats.verified_bytes as u64);
        trace.outcome = if first_error.is_some() {
            "fetch_failed"
        } else {
            "fetched"
        };
    }
    if let Some(error) = first_error {
        return Err(RankedSq8Failure { error, stats });
    }
    Ok((fetched, stats))
}

/// Raw CPU on the calling thread for one synchronous scope; wall includes descheduling.
/// Both include observer and first-touch costs, so copy scopes are not unbiased memcpy
/// timings and must not have a point estimate of observer cost automatically subtracted.
/// Clock reads count only the added profile clocks, not pre-existing range stamps.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct CpuWall {
    pub thread_cpu_ns: u64,
    pub wall_ns: u64,
    pub clock_reads: u64,
}

impl CpuWall {
    fn add_to(self, total: &mut Option<Self>) {
        let total = total.get_or_insert_with(Self::default);
        total.thread_cpu_ns = total.thread_cpu_ns.saturating_add(self.thread_cpu_ns);
        total.wall_ns = total.wall_ns.saturating_add(self.wall_ns);
        total.clock_reads = total.clock_reads.saturating_add(self.clock_reads);
    }
}

/// Never retain this across an await: the two thread clocks must run in one poll.
/// Reports raw scope time, including observer and first-touch costs.
pub(crate) struct SyncStart {
    wall: Instant,
    cpu: u64,
}

#[cfg(test)]
thread_local! {
    pub(crate) static PROFILE_CLOCK_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn profile_clock_read() {
    #[cfg(test)]
    PROFILE_CLOCK_READS.with(|reads| reads.set(reads.get() + 1));
}

fn cpu_ns(clock: rustix::time::ClockId) -> u64 {
    profile_clock_read();
    let time = rustix::time::clock_gettime(clock);
    u64::try_from(time.tv_sec)
        .unwrap_or(u64::MAX)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::try_from(time.tv_nsec).unwrap_or(u64::MAX))
}

fn thread_cpu_ns() -> u64 {
    cpu_ns(rustix::time::ClockId::ThreadCPUTime)
}

impl SyncStart {
    pub(crate) fn now() -> Self {
        profile_clock_read();
        Self {
            wall: Instant::now(),
            cpu: thread_cpu_ns(),
        }
    }

    pub(crate) fn finish(self, total: &mut Option<CpuWall>) {
        let cpu = thread_cpu_ns().saturating_sub(self.cpu);
        profile_clock_read();
        CpuWall {
            thread_cpu_ns: cpu,
            wall_ns: ns_since(self.wall),
            clock_reads: 4,
        }
        .add_to(total);
    }
}

/// Fixed aggregates, never events or per-vector-element clocks. Unreached scopes are null.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct RangeCpu {
    pub collector_allocation: Option<CpuWall>,
    pub collector_copy: Option<CpuWall>,
    pub owned_allocation: Option<CpuWall>,
    pub owned_copy: Option<CpuWall>,
    pub authentication: Option<CpuWall>,
    pub collector_copied_bytes: u64,
    pub owned_copied_bytes: u64,
}

impl RangeCpu {
    fn add(&mut self, other: Self) {
        for (from, to) in [
            (other.collector_allocation, &mut self.collector_allocation),
            (other.collector_copy, &mut self.collector_copy),
            (other.owned_allocation, &mut self.owned_allocation),
            (other.owned_copy, &mut self.owned_copy),
            (other.authentication, &mut self.authentication),
        ] {
            if let Some(from) = from {
                from.add_to(to);
            }
        }
        self.collector_copied_bytes = self
            .collector_copied_bytes
            .saturating_add(other.collector_copied_bytes);
        self.owned_copied_bytes = self
            .owned_copied_bytes
            .saturating_add(other.owned_copied_bytes);
    }

    fn thread_cpu_ns(&self) -> u64 {
        [
            self.collector_allocation,
            self.collector_copy,
            self.owned_allocation,
            self.owned_copy,
            self.authentication,
        ]
        .into_iter()
        .flatten()
        .fold(0u64, |sum, scope| sum.saturating_add(scope.thread_cpu_ns))
    }
}

/// One opt-in query owns both reservations; SOURCE emits only its aggregate and last span.
#[derive(Debug, serde::Serialize)]
pub struct QueryProfile {
    #[serde(skip)]
    pub source: Sq8RangeTrace,
    #[serde(skip)]
    pub sq8: Sq8RangeTrace,
    pub discovery: Option<CpuWall>,
    pub cover: Option<CpuWall>,
    pub planning: Option<CpuWall>,
    /// shortcut: consumer polls are not instrumented, add only with forwarding/layout proof.
    pub acquisition_poll_cpu: &'static str,
}

impl QueryProfile {
    pub fn new(source_ranges: usize, sq8_ranges: usize) -> Result<Self, &'static str> {
        if Self::modeled_bytes(source_ranges, sq8_ranges, source_ranges, sq8_ranges)
            > SQ8_TRACE_MAX_BYTES
        {
            return Err("query profile capacity");
        }
        Ok(Self {
            source: Sq8RangeTrace::new(source_ranges)?,
            sq8: Sq8RangeTrace::new(sq8_ranges)?,
            discovery: None,
            cover: None,
            planning: None,
            acquisition_poll_cpu: "unresolved_not_instrumented",
        })
    }

    pub fn retained_bytes(source_ranges: usize, sq8_ranges: usize) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(2 * ALLOCATION_ALLOWANCE_BYTES)
            .saturating_add(
                source_ranges
                    .saturating_add(sq8_ranges)
                    .saturating_mul(std::mem::size_of::<RangeSpan>()),
            )
    }

    /// Both buffers survive both phases. Only one phase has probes/futures at a time.
    /// Separate maxima conservatively cover different SOURCE/SQ8 parallel limits.
    pub fn additional_bytes(source_ranges: usize, sq8_ranges: usize) -> usize {
        Self::retained_bytes(source_ranges, sq8_ranges).saturating_add(
            source_ranges.max(sq8_ranges).saturating_mul(
                std::mem::size_of::<Probe>().saturating_add(ALLOCATION_ALLOWANCE_BYTES),
            ),
        )
    }

    pub fn modeled_bytes(
        source_ranges: usize,
        sq8_ranges: usize,
        source_parallel: usize,
        sq8_parallel: usize,
    ) -> usize {
        Self::additional_bytes(source_ranges, sq8_ranges).saturating_add(
            Sq8RangeTrace::pending_futures_bytes(source_ranges, source_parallel).max(
                Sq8RangeTrace::pending_futures_bytes(sq8_ranges, sq8_parallel),
            ),
        )
    }

    pub fn begin(&mut self, origin: Instant) {
        self.source.begin(origin);
        self.sq8.begin(origin);
        self.discovery = None;
        self.cover = None;
        self.planning = None;
    }

    pub fn refuse(&mut self) {
        self.source.refuse();
        self.sq8.refuse();
        self.discovery = None;
        self.cover = None;
        self.planning = None;
    }

    pub fn source_summary(&self) -> SourceRangeSummary<'_> {
        let source = &self.source;
        SourceRangeSummary {
            outcome: source.outcome,
            planned_ranges: source.planned_ranges,
            planned_bytes: source.planned_bytes,
            max_parallel: source.max_parallel,
            fetch_start_ns: source.fetch_start_ns,
            all_ranges_complete_ns: source.all_ranges_complete_ns,
            release_end_ns: source.release_end_ns,
            retained_payload_bytes: source.retained_payload_bytes,
            cpu: &source.cpu,
            phase_process_cpu_ns: source.phase_process_cpu_ns,
            unattributed_process_cpu_ns: source.unattributed_process_cpu_ns(),
            phase_clock_reads: source.phase_clock_reads,
            last_completing_range: source.ranges.iter().max_by_key(|span| span.complete_ns),
        }
    }
}

#[derive(serde::Serialize)]
pub struct SourceRangeSummary<'a> {
    outcome: &'static str,
    planned_ranges: u32,
    planned_bytes: u64,
    max_parallel: u32,
    fetch_start_ns: Option<u64>,
    all_ranges_complete_ns: Option<u64>,
    release_end_ns: Option<u64>,
    retained_payload_bytes: Option<u64>,
    cpu: &'a RangeCpu,
    phase_process_cpu_ns: Option<u64>,
    unattributed_process_cpu_ns: Option<u64>,
    phase_clock_reads: u64,
    last_completing_range: Option<&'a RangeSpan>,
}

/// A range's diagnostic state while its future runs: private to that future.
pub(crate) struct Probe {
    origin: Instant,
    span: RangeSpan,
    cpu: RangeCpu,
}

// Keep test instrumentation outside the async frame and its admission model.
#[cfg(test)]
thread_local! {
    static PROBE_CLOCK_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

impl Probe {
    fn now_ns(&self) -> u64 {
        #[cfg(test)]
        PROBE_CLOCK_READS.with(|reads| reads.set(reads.get() + 1));
        ns_since(self.origin)
    }
}

fn ns_since(origin: Instant) -> u64 {
    u64::try_from(origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Observe one boundary. With no probe this is a single `None` check: no clock.
fn stamp(probe: &mut Option<&mut Probe>, set: impl FnOnce(&mut RangeSpan, u64)) {
    if let Some(probe) = probe.as_deref_mut() {
        let ns = probe.now_ns();
        set(&mut probe.span, ns);
    }
}

// The CPU pair brackets only authentication; wall reuses the existing auth stamps.
fn measured_auth<T, E>(
    probe: &mut Option<&mut Probe>,
    authenticate: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    stamp(probe, |span, ns| {
        span.auth_start_ns = Some(ns);
        span.outcome = "page_auth";
    });
    let cpu = probe.as_deref().map(|_| thread_cpu_ns());
    let result = authenticate();
    let cpu = cpu.map(|start| thread_cpu_ns().saturating_sub(start));
    stamp(probe, |span, ns| {
        span.auth_end_ns = Some(ns);
        if result.is_ok() {
            span.outcome = "ok";
        }
    });
    if let (Some(probe), Some(cpu)) = (probe.as_deref_mut(), cpu) {
        CpuWall {
            thread_cpu_ns: cpu,
            wall_ns: probe
                .span
                .auth_end_ns
                .unwrap()
                .saturating_sub(probe.span.auth_start_ns.unwrap()),
            clock_reads: 2,
        }
        .add_to(&mut probe.cpu.authentication);
    }
    result
}

/// Upper bounds for one query's opt-in range diagnostics.
pub const SQ8_TRACE_MAX_RANGES: usize = 128;
pub const SQ8_TRACE_MAX_BYTES: usize = 1 << 20;
/// Preregistered allowance for ONE pending range, whether or not tracing is on: the
/// complete scheduled future (`fetch_verified_pages_traced` or
/// `CapsuleRangeReader::fetch_range`), plus its executor node
/// (`BUFFERED_NODE_ALLOWANCE_BYTES`) and the allocation
/// header. A test fails if the compiled future grows past `RANGE_FUTURE_ALLOWANCE_BYTES -
/// BUFFERED_NODE_ALLOWANCE_BYTES - ALLOCATION_ALLOWANCE_BYTES`.
pub const RANGE_FUTURE_ALLOWANCE_BYTES: usize = 2048;
/// `buffered` keeps each future in `OrderWrapper<Task<Fut>>` inside an `Arc`: by the
/// futures-util 0.3 layout a task adds its future-cell, five pointer-sized links, a
/// weak queue pointer and two flags (about 56 bytes), the Arc counters 16, the order
/// index 8 and the allocator header up to 16 - roughly 100 bytes. 256 leaves room for a
/// layout change; it is an estimate of library internals, not a measurement.
pub const BUFFERED_NODE_ALLOWANCE_BYTES: usize = 256;
/// Per-allocation header and rounding allowance for each boxed probe and node.
const ALLOCATION_ALLOWANCE_BYTES: usize = 32;
/// `collect` grows its vector by doubling: a growth step briefly holds the old and the new
/// buffer, at most three times the final length.
const OUTCOME_VECTOR_GROWTH_FACTOR: usize = 3;

/// One physical range, in plan order. Offsets are nanoseconds from the owning
/// trace's origin. `None` means the boundary was never reached, never zero.
/// Ranges overlap in time: never sum their intervals. The headers interval
/// (`request_ns` to `headers_ns`) is `get_opts` as a whole: client queue,
/// connect, TLS and server combined. Body gaps include `stream.next()`, executor
/// scheduling and loop-back observer bookkeeping; they do not identify TCP waits.
/// Observer bookkeeping has no asserted nanosecond error bound.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RangeSpan {
    /// Position in the sorted range plan.
    pub index: u32,
    /// Inclusive first and last page of the planned range.
    pub first_page: u64,
    pub last_page: u64,
    /// Planned physical half-open byte range; zero when the page range is invalid.
    pub start_byte: u64,
    pub end_byte: u64,
    /// First poll of this range's future: when `buffered` admitted it.
    pub first_poll_ns: Option<u64>,
    /// Immediately before `get_opts` is called.
    pub request_ns: Option<u64>,
    /// `get_opts` returned a response: status, range, ETag and size are known.
    pub headers_ns: Option<u64>,
    /// The response range, size and ETag matched the plan and the pinned object.
    pub metadata_ns: Option<u64>,
    /// First and last body frame handed to the collector.
    pub first_chunk_ns: Option<u64>,
    pub last_chunk_ns: Option<u64>,
    /// The body stream reported its end.
    pub eof_ns: Option<u64>,
    /// Page-digest verification of the collected body, inline on the executor thread.
    pub auth_start_ns: Option<u64>,
    pub auth_end_ns: Option<u64>,
    /// The range future is about to return, success or failure.
    pub complete_ns: Option<u64>,
    /// Body frames received and their total bytes, including any overlong frame.
    pub chunks: u32,
    pub body_bytes: u64,
    /// Longest interval from prior copy completion (or the initial pre-poll mark)
    /// to next body frame or EOF, including loop-back observer bookkeeping,
    /// and when that interval ended.
    pub max_body_gap_ns: u64,
    pub max_body_gap_end_ns: Option<u64>,
    /// Aggregate arrival-to-copy-completion time and copy count; time includes
    /// note_wait, note_chunk and length validation, rather than pure memcpy time.
    pub copy_ns: u64,
    pub copy_count: u32,
    /// Where the range stopped: invalid_request, store_headers, metadata, store_body,
    /// overlong, page_auth or ok. Empty only before the range was ever polled.
    pub outcome: &'static str,
}

impl RangeSpan {
    /// Serialized column order of a span (see the `Serialize` impl).
    pub const FIELDS: [&str; 22] = [
        "index",
        "first_page",
        "last_page",
        "start_byte",
        "end_byte",
        "first_poll_ns",
        "request_ns",
        "headers_ns",
        "metadata_ns",
        "first_chunk_ns",
        "last_chunk_ns",
        "eof_ns",
        "auth_start_ns",
        "auth_end_ns",
        "complete_ns",
        "chunks",
        "body_bytes",
        "max_body_gap_ns",
        "max_body_gap_end_ns",
        "copy_ns",
        "copy_count",
        "outcome",
    ];

    fn new(
        index: usize,
        first_page: usize,
        last_page: usize,
        bytes: Option<std::ops::Range<usize>>,
    ) -> Self {
        let bytes = bytes.unwrap_or(0..0);
        Self {
            index: u32::try_from(index).unwrap_or(u32::MAX),
            first_page: u64::try_from(first_page).unwrap_or(u64::MAX),
            last_page: u64::try_from(last_page).unwrap_or(u64::MAX),
            start_byte: u64::try_from(bytes.start).unwrap_or(u64::MAX),
            end_byte: u64::try_from(bytes.end).unwrap_or(u64::MAX),
            outcome: "invalid_request",
            ..Self::default()
        }
    }

    fn note_chunk(&mut self, ns: u64, len: usize) {
        self.first_chunk_ns.get_or_insert(ns);
        self.last_chunk_ns = Some(ns);
        self.chunks = self.chunks.saturating_add(1);
        self.body_bytes = self
            .body_bytes
            .saturating_add(u64::try_from(len).unwrap_or(u64::MAX));
    }

    fn note_wait(&mut self, from: u64, to: u64) {
        let gap = to.saturating_sub(from);
        if gap > self.max_body_gap_ns {
            self.max_body_gap_ns = gap;
            self.max_body_gap_end_ns = Some(to);
        }
    }

    fn note_copy(&mut self, from: u64, to: u64) {
        self.copy_ns = self.copy_ns.saturating_add(to.saturating_sub(from));
        self.copy_count = self.copy_count.saturating_add(1);
    }
}

/// Compact row form: values follow `RangeSpan::FIELDS`, `null` = never reached.
impl serde::Serialize for RangeSpan {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut row = serializer.serialize_tuple(Self::FIELDS.len())?;
        row.serialize_element(&self.index)?;
        row.serialize_element(&self.first_page)?;
        row.serialize_element(&self.last_page)?;
        row.serialize_element(&self.start_byte)?;
        row.serialize_element(&self.end_byte)?;
        row.serialize_element(&self.first_poll_ns)?;
        row.serialize_element(&self.request_ns)?;
        row.serialize_element(&self.headers_ns)?;
        row.serialize_element(&self.metadata_ns)?;
        row.serialize_element(&self.first_chunk_ns)?;
        row.serialize_element(&self.last_chunk_ns)?;
        row.serialize_element(&self.eof_ns)?;
        row.serialize_element(&self.auth_start_ns)?;
        row.serialize_element(&self.auth_end_ns)?;
        row.serialize_element(&self.complete_ns)?;
        row.serialize_element(&self.chunks)?;
        row.serialize_element(&self.body_bytes)?;
        row.serialize_element(&self.max_body_gap_ns)?;
        row.serialize_element(&self.max_body_gap_end_ns)?;
        row.serialize_element(&self.copy_ns)?;
        row.serialize_element(&self.copy_count)?;
        row.serialize_element(&self.outcome)?;
        row.end()
    }
}

/// Opt-in diagnostics for one query, owned by the caller so a failed query still
/// returns every span. Capacity is fixed and admitted before any request: a plan with
/// more ranges than the capacity is refused before any future or GET exists (the
/// generation also checks it at admission), so no range is silently left untraced and
/// `dropped_ranges` stays zero. Offsets share the origin passed to `begin`, which the
/// generation sets to its stage origin.
///
/// There is deliberately no `Clone` (a clone would copy the advertised capacity but not the
/// reservation, so the next `push` would allocate during a drain) and the span vector is
/// private and read-only through `ranges()`, so nothing outside this module can replace,
/// shrink or grow the reservation the admission model charged.
#[derive(Debug, serde::Serialize)]
pub struct Sq8RangeTrace {
    #[serde(skip)]
    origin: Instant,
    #[serde(skip)]
    capacity: usize,
    /// Column names of every row in `ranges`.
    pub range_fields: &'static [&'static str],
    /// not_started, admission_refused, started, rejected_before_io, fetching, fetch_failed,
    /// fetched, rank_failed, ranked, cache_hit, local_uninstrumented or local_failed.
    pub outcome: &'static str,
    pub planned_ranges: u32,
    pub planned_bytes: u64,
    pub max_parallel: u32,
    /// Defensive counter of `push` refusals: zero in practice, because a plan larger than the
    /// capacity is refused before any future exists.
    pub dropped_ranges: u32,
    /// Plan validated; futures are about to be created and polled.
    pub fetch_start_ns: Option<u64>,
    /// Every range future has finished, success or failure: the barrier.
    pub all_ranges_complete_ns: Option<u64>,
    pub rank_start_ns: Option<u64>,
    /// Ranking returned (its scoring scratch is already released inside the finish phase).
    pub rank_end_ns: Option<u64>,
    /// The fetched payload (`returned` views, then the verified buffers) has been freed: the
    /// last boundary of the SQ8 stage. None whenever ranking was never reached.
    pub release_end_ns: Option<u64>,
    pub rank: Option<RankPhases>,
    pub cpu: RangeCpu,
    pub regroup: Option<CpuWall>,
    pub regroup_copied_bytes: u64,
    pub rank_thread_cpu_ns: Option<u64>,
    pub rank_clock_reads: u64,
    /// Verified body bytes retained after the drain, before planning/ranking.
    /// This is not the SDK/TLS heap and can be nonzero in a failed, discarded batch.
    pub retained_payload_bytes: Option<u64>,
    /// Coarse process CPU includes sibling/connection work; residual is never TLS CPU.
    pub phase_process_cpu_ns: Option<u64>,
    pub unattributed_process_cpu_ns: Option<u64>,
    pub phase_clock_reads: u64,
    #[serde(skip)]
    phase_cpu_start: Option<u64>,
    /// Private so the reservation made by `new` cannot be swapped out; read through `ranges()`.
    ranges: Vec<RangeSpan>,
}

impl Sq8RangeTrace {
    /// Allocates the whole span buffer once; refuses capacities that would exceed
    /// `SQ8_TRACE_MAX_RANGES` or, at the worst parallelism, `SQ8_TRACE_MAX_BYTES`
    /// of modeled state, before allocating anything.
    pub fn new(capacity: usize) -> Result<Self, &'static str> {
        if capacity == 0
            || capacity > SQ8_TRACE_MAX_RANGES
            || Self::modeled_bytes(capacity, capacity) > SQ8_TRACE_MAX_BYTES
        {
            return Err("SQ8 range trace capacity");
        }
        Ok(Self {
            origin: Instant::now(),
            capacity,
            range_fields: &RangeSpan::FIELDS,
            outcome: "not_started",
            planned_ranges: 0,
            planned_bytes: 0,
            max_parallel: 0,
            dropped_ranges: 0,
            fetch_start_ns: None,
            all_ranges_complete_ns: None,
            rank_start_ns: None,
            rank_end_ns: None,
            release_end_ns: None,
            rank: None,
            cpu: RangeCpu::default(),
            regroup: None,
            regroup_copied_bytes: 0,
            rank_thread_cpu_ns: None,
            rank_clock_reads: 0,
            retained_payload_bytes: None,
            phase_process_cpu_ns: None,
            unattributed_process_cpu_ns: None,
            phase_clock_reads: 0,
            phase_cpu_start: None,
            ranges: Vec::with_capacity(capacity),
        })
    }

    /// RETAINED bytes: what one trace holds between queries - the struct and its
    /// preallocated span buffer, allocated once by `new`.
    pub fn retained_bytes(capacity: usize) -> usize {
        capacity
            .saturating_mul(std::mem::size_of::<RangeSpan>())
            .saturating_add(std::mem::size_of::<Self>())
    }

    /// CUMULATIVE PEAK bytes while one query drains with `capacity` planned ranges and at most
    /// `max_parallel` pending: the retained trace, plus everything that coexists with it until
    /// the full drain is collected. That is every completed range's boxed probe (with
    /// allocator overhead) and `pending_futures_bytes`. Not an average and not the retained
    /// size; admission charges this figure.
    pub fn modeled_bytes(capacity: usize, max_parallel: usize) -> usize {
        let probes = capacity.saturating_mul(
            std::mem::size_of::<Probe>().saturating_add(ALLOCATION_ALLOWANCE_BYTES),
        );
        Self::retained_bytes(capacity)
            .saturating_add(probes)
            .saturating_add(Self::pending_futures_bytes(capacity, max_parallel))
    }

    /// Peak range-drain state that exists with tracing ON OR OFF, as a conservative bound
    /// rather than a delta: each of at most `max_parallel` pending ranges is charged its
    /// whole `RANGE_FUTURE_ALLOWANCE_BYTES` (future, executor node, allocation), the scheduler
    /// may hold that many finished-but-unyielded outcomes, and the collected outcome
    /// vector is charged at its growth peak. Every outcome includes its plan index and probe
    /// slot. The tracing-off DELTA against the pre-change layout is only a part of this
    /// (one extra async layer, one probe pointer per future, one pointer per outcome). It is
    /// NOT measured by the layout test, which prints the current futures only: the
    /// pre-change future no longer exists in the tree. Until a remote run builds the same
    /// fixture on the base commit it is an unmeasured estimate of a few hundred bytes per
    /// pending range, inside the allowance charged above.
    pub fn pending_futures_bytes(ranges: usize, max_parallel: usize) -> usize {
        Self::checked_pending_futures_bytes(ranges, max_parallel).unwrap_or(usize::MAX)
    }

    /// Checked form of the scheduler bound for admission and accounting reports.
    pub fn checked_pending_futures_bytes(ranges: usize, max_parallel: usize) -> Option<usize> {
        let outcome = std::mem::size_of::<IndexedRangeOutcome>();
        let pending = ranges.min(max_parallel);
        pending
            .checked_mul(RANGE_FUTURE_ALLOWANCE_BYTES)?
            .checked_add(pending.checked_mul(outcome.checked_add(8)?)?)?
            .checked_add(
                ranges
                    .checked_mul(outcome)?
                    .checked_mul(OUTCOME_VECTOR_GROWTH_FACTOR)?,
            )
    }

    pub fn reserved_bytes(&self, max_parallel: usize) -> usize {
        Self::modeled_bytes(self.capacity, max_parallel)
    }

    /// The recorded spans in plan order (read-only).
    pub fn ranges(&self) -> &[RangeSpan] {
        &self.ranges
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Reuse for the next query: nothing is reallocated, every stamp is unset.
    pub fn begin(&mut self, origin: Instant) {
        self.origin = origin;
        self.clear("started");
    }

    /// Called by a traced entry point BEFORE any early return, so a reused trace can never
    /// show a previous query's spans, stamps or phases as the evidence of a refused one.
    /// Clears like `begin` but reads no clock and allocates nothing; a query that actually
    /// starts replaces this state through `begin`. The typed error stays the caller's.
    pub fn refuse(&mut self) {
        self.clear("admission_refused");
    }

    fn clear(&mut self, outcome: &'static str) {
        self.outcome = outcome;
        self.planned_ranges = 0;
        self.planned_bytes = 0;
        self.max_parallel = 0;
        self.dropped_ranges = 0;
        self.fetch_start_ns = None;
        self.all_ranges_complete_ns = None;
        self.rank_start_ns = None;
        self.rank_end_ns = None;
        self.release_end_ns = None;
        self.rank = None;
        self.cpu = RangeCpu::default();
        self.regroup = None;
        self.regroup_copied_bytes = 0;
        self.rank_thread_cpu_ns = None;
        self.rank_clock_reads = 0;
        self.retained_payload_bytes = None;
        self.phase_cpu_start = None;
        self.phase_process_cpu_ns = None;
        self.unattributed_process_cpu_ns = None;
        self.phase_clock_reads = 0;
        self.ranges.clear();
    }

    fn now_ns(&self) -> u64 {
        ns_since(self.origin)
    }

    pub(crate) fn start_phase(&mut self) {
        self.phase_cpu_start = Some(cpu_ns(rustix::time::ClockId::ProcessCPUTime));
        self.phase_clock_reads = self.phase_clock_reads.saturating_add(1);
    }

    pub(crate) fn finish_phase(&mut self) {
        if let Some(start) = self.phase_cpu_start.take() {
            self.phase_process_cpu_ns =
                Some(cpu_ns(rustix::time::ClockId::ProcessCPUTime).saturating_sub(start));
            self.phase_clock_reads = self.phase_clock_reads.saturating_add(1);
            self.unattributed_process_cpu_ns = self.unattributed_process_cpu_ns();
        }
    }

    fn unattributed_process_cpu_ns(&self) -> Option<u64> {
        let measured = self
            .cpu
            .thread_cpu_ns()
            .saturating_add(self.regroup.map_or(0, |scope| scope.thread_cpu_ns))
            .saturating_add(self.rank_thread_cpu_ns.unwrap_or(0));
        self.phase_process_cpu_ns
            .map(|cpu| cpu.saturating_sub(measured))
    }

    pub(crate) fn released(&mut self) {
        self.release_end_ns = Some(self.now_ns());
    }

    fn push_probe(&mut self, probe: Probe) {
        self.cpu.add(probe.cpu);
        self.push(probe.span);
    }

    fn push(&mut self, span: RangeSpan) {
        if self.ranges.len() < self.capacity {
            self.ranges.push(span);
        } else {
            self.dropped_ranges = self.dropped_ranges.saturating_add(1);
        }
    }
}

// Real HTTP body barriers for the generation scheduling regression. Kept here
// so the fixture uses the production OneAttemptS3 builder and private counters.
#[cfg(test)]
pub(crate) mod cold_http_fixture {
    use super::*;
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::Condvar,
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    };

    pub const ETAG: &str = "\"cold-fixture\"";
    pub type Request = (String, bool, Option<(usize, usize)>, Option<String>);

    #[derive(Clone, Default)]
    pub struct State {
        pub requests: Vec<Request>,
        pub active: [usize; 3],
        pub peak: [usize; 3],
        pub finished: [Vec<usize>; 3],
        pub errors: Vec<String>,
        released: [bool; 3],
        // Stage, then starts of delayed stream error, early digest error, held sibling.
        fault: Option<(usize, [usize; 3])>,
        retry_status: bool,
        error_released: bool,
        sibling_released: bool,
        stop: bool,
        bytes: usize,
    }

    pub struct Fixture {
        pub reader: OneAttemptS3,
        endpoint: String,
        state: Arc<(Mutex<State>, Condvar)>,
        server: Option<JoinHandle<()>>,
        deadline: Instant,
    }

    impl Fixture {
        pub fn new(objects: BTreeMap<String, Vec<u8>>, deadline: Instant) -> Self {
            // Fresh1m's encoded unit means/leaves fit here; no raw source or
            // SQ8 corpus is hydrated by the metadata-only leaf fixture.
            assert!(objects.values().map(Vec::len).sum::<usize>() <= 64 * 1024 * 1024);
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let endpoint = format!("http://{address}");
            listener.set_nonblocking(true).unwrap();
            let counters = Arc::new(Mutex::new(TransportCounters::default()));
            let store = one_attempt_builder(
                "fixture",
                "eu-central-1",
                counters.clone(),
                Duration::from_secs(10),
            )
            .unwrap()
            .with_endpoint(endpoint.clone())
            .with_allow_http(true)
            .with_access_key_id("fixture")
            .with_secret_access_key("fixture")
            .build()
            .unwrap();
            let state = Arc::new((
                Mutex::new(State {
                    released: [true; 3],
                    ..State::default()
                }),
                Condvar::new(),
            ));
            let shared = state.clone();
            let objects = Arc::new(objects);
            let server = thread::spawn(move || {
                let mut handlers: Vec<JoinHandle<()>> = Vec::new();
                let mut accepted = 0;
                while !shared.0.lock().unwrap().stop && Instant::now() < deadline {
                    while let Some(index) = handlers.iter().position(JoinHandle::is_finished) {
                        if handlers.swap_remove(index).join().is_err() {
                            shared.0.lock().unwrap().errors.push("handler panic".into());
                        }
                    }
                    match listener.accept() {
                        Ok((socket, _)) => {
                            accepted += 1;
                            // Allow 128 held bodies plus handlers finishing after refill.
                            // Each handler has a bounded 128 KiB stack.
                            if accepted > 2048 || handlers.len() >= 256 {
                                shared.0.lock().unwrap().errors.push("handler cap".into());
                                break;
                            }
                            let objects = objects.clone();
                            let state = shared.clone();
                            match thread::Builder::new()
                                .stack_size(128 * 1024)
                                .spawn(move || {
                                    if let Err(error) = serve(socket, &objects, &state, deadline) {
                                        state.0.lock().unwrap().errors.push(error.to_string());
                                    }
                                }) {
                                Ok(handler) => handlers.push(handler),
                                Err(error) => {
                                    shared.0.lock().unwrap().errors.push(error.to_string());
                                    break;
                                }
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(error) => {
                            shared.0.lock().unwrap().errors.push(error.to_string());
                            break;
                        }
                    }
                }
                shared.0.lock().unwrap().stop = true;
                shared.1.notify_all();
                for handler in handlers {
                    if handler.join().is_err() {
                        shared.0.lock().unwrap().errors.push("handler panic".into());
                    }
                }
            });
            Self {
                reader: OneAttemptS3 {
                    store: PrefixStore::new(store, Path::default()),
                    counters,
                },
                endpoint,
                state,
                server: Some(server),
                deadline,
            }
        }

        pub fn retry_reader(&self) -> BoundedReadS3 {
            let counters = self.reader.counters.clone();
            let store = native_builder(
                "fixture",
                "eu-central-1",
                counters.clone(),
                Duration::from_secs(10),
                Some(S3ReadRetryPolicy::OneRetry100Ms),
            )
            .unwrap()
            .with_endpoint(self.endpoint.clone())
            .with_allow_http(true)
            .with_access_key_id("fixture")
            .with_secret_access_key("fixture")
            .build()
            .unwrap();
            BoundedReadS3 {
                store: PrefixStore::new(store, Path::default()),
                counters,
            }
        }

        pub fn arm_retry_status(&self, starts: [usize; 3]) {
            self.arm(Some((1, starts)));
            let mut state = self.state.0.lock().unwrap();
            state.retry_status = true;
            state.released[1] = false;
        }

        pub fn snapshot(&self) -> State {
            self.state.0.lock().unwrap().clone()
        }

        pub fn arm(&self, fault: Option<(usize, [usize; 3])>) {
            let state = self.snapshot();
            assert_eq!(state.active, [0; 3]);
            assert!(state.errors.is_empty(), "{:?}", state.errors);
            assert!(!state.stop);
            let mut state = self.state.0.lock().unwrap();
            // Keep the lifetime byte allowance across all arms.
            *state = State {
                fault,
                released: [fault.is_some(), fault.is_some(), true],
                bytes: state.bytes,
                ..State::default()
            };
        }

        pub fn arm_leaves(&self, fault: Option<[usize; 3]>) {
            self.arm(fault.map(|starts| (2, starts)));
            self.state.0.lock().unwrap().released[2] = fault.is_some();
        }

        pub fn release(&self, stage: usize) {
            self.state.0.lock().unwrap().released[stage] = true;
            self.state.1.notify_all();
        }

        pub fn release_error(&self) {
            self.state.0.lock().unwrap().error_released = true;
            self.state.1.notify_all();
        }

        pub fn release_sibling(&self) {
            self.state.0.lock().unwrap().sibling_released = true;
            self.state.1.notify_all();
        }

        pub async fn wait(&self, ready: impl Fn(&State) -> bool) {
            let deadline = self.deadline.min(Instant::now() + Duration::from_secs(5));
            loop {
                let state = self.snapshot();
                assert!(state.errors.is_empty(), "{:?}", state.errors);
                assert!(
                    !state.stop && Instant::now() < deadline,
                    "body barrier deadline"
                );
                if ready(&state) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }

        pub fn finish(mut self) {
            self.shutdown();
            let state = self.snapshot();
            assert_eq!(state.active, [0; 3]);
            assert!(state.errors.is_empty(), "{:?}", state.errors);
        }

        fn shutdown(&mut self) {
            self.state.0.lock().unwrap().stop = true;
            self.state.1.notify_all();
            if let Some(server) = self.server.take() {
                // Joining all handlers is mandatory even during an assertion unwind.
                if server.join().is_err() {
                    self.state
                        .0
                        .lock()
                        .unwrap()
                        .errors
                        .push("server panic".into());
                }
            }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.shutdown();
        }
    }

    fn serve(
        mut socket: TcpStream,
        objects: &BTreeMap<String, Vec<u8>>,
        shared: &(Mutex<State>, Condvar),
        deadline: Instant,
    ) -> std::io::Result<()> {
        let bad = || std::io::Error::other("invalid fixture request");
        socket.set_read_timeout(Some(Duration::from_secs(2)))?;
        socket.set_write_timeout(Some(Duration::from_secs(2)))?;
        let mut raw = Vec::new();
        let mut block = [0; 1024];
        while !raw.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = socket.read(&mut block)?;
            if count == 0 || raw.len() + count > 8192 || Instant::now() >= deadline {
                return Err(bad());
            }
            raw.extend_from_slice(&block[..count]);
        }
        let request = std::str::from_utf8(&raw).map_err(|_| bad())?;
        let mut line = request.lines().next().ok_or_else(bad)?.split_whitespace();
        let method = line.next().ok_or_else(bad)?;
        if !matches!(method, "GET" | "HEAD") {
            return Err(bad());
        }
        let key = line
            .next()
            .and_then(|path| path.strip_prefix("/fixture/"))
            .ok_or_else(bad)?;
        let body = objects.get(key).ok_or_else(bad)?;
        let header = |name: &str| {
            request.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case(name).then(|| value.trim())
            })
        };
        if header("authorization").is_none() {
            return Err(bad());
        }
        let range = header("range")
            .map(|value| -> std::io::Result<_> {
                let (first, last) = value
                    .strip_prefix("bytes=")
                    .and_then(|v| v.split_once('-'))
                    .ok_or_else(bad)?;
                let first: usize = first.parse().map_err(|_| bad())?;
                let end = last
                    .parse::<usize>()
                    .map_err(|_| bad())?
                    .checked_add(1)
                    .ok_or_else(bad)?;
                if first >= end || end > body.len() {
                    return Err(bad());
                }
                Ok((first, end))
            })
            .transpose()?;
        let etag = header("if-match").map(str::to_owned);
        if range.is_some() && etag.as_deref() != Some(ETAG) {
            return Err(bad());
        }
        let head = method == "HEAD";
        let (first, end) = range.unwrap_or((0, body.len()));
        let stage = if key.ends_with("/plane/records.bin")
            || key.ends_with("/plane/capsules.bin")
            || key.ends_with("/plane/frames.bin")
        {
            Some(0)
        } else if key.contains("/objects/") {
            Some(1)
        } else if key.ends_with("/router/leaves.bin") {
            Some(2)
        } else {
            None
        };
        let mut state = shared.0.lock().unwrap();
        if state.stop {
            return Ok(());
        }
        state.requests.push((key.into(), head, range, etag));
        if state.requests.len() > 1024 {
            return Err(bad());
        }
        state.bytes += if head { 0 } else { end - first };
        if state.bytes > 64 * 1024 * 1024 {
            return Err(bad());
        }
        let fault = state
            .fault
            .filter(|(s, _)| Some(*s) == stage)
            .map(|(_, starts)| starts);
        if state.retry_status && fault.is_some_and(|starts| first == starts[0]) {
            let stage = stage.unwrap();
            let retried = state.finished[stage].contains(&first);
            state.active[stage] += 1;
            state.peak[stage] = state.peak[stage].max(state.active[stage]);
            // Hold the retry at headers while another logical read finishes.
            while retried && !state.error_released && !state.stop && Instant::now() < deadline {
                state = shared
                    .1
                    .wait_timeout(state, Duration::from_millis(50))
                    .unwrap()
                    .0;
            }
            let result = if state.stop || Instant::now() >= deadline {
                Ok(())
            } else {
                socket.write_all(
                    b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
            };
            state.active[stage] -= 1;
            state.finished[stage].push(first);
            return result;
        }
        drop(state);
        let status = if range.is_some() {
            "206 Partial Content"
        } else {
            "200 OK"
        };
        write!(
            socket,
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nETag: {ETAG}\r\nLast-Modified: Wed, 23 Sep 2026 00:00:00 GMT\r\nConnection: close\r\n",
            end - first
        )?;
        if range.is_some() {
            write!(
                socket,
                "Content-Range: bytes {first}-{}/{}\r\n",
                end - 1,
                body.len()
            )?;
        }
        socket.write_all(b"\r\n")?;
        if head {
            return Ok(());
        }
        let Some(stage) = stage.filter(|_| range.is_some()) else {
            socket.write_all(&body[first..end])?;
            return Ok(());
        };
        // A valid prefix followed by a held final byte proves unfinished bodies,
        // rather than just simultaneous response headers or task starts.
        socket.write_all(&body[first..end - 1])?;
        let mut state = shared.0.lock().unwrap();
        state.active[stage] += 1;
        state.peak[stage] = state.peak[stage].max(state.active[stage]);
        while !state.stop && Instant::now() < deadline {
            let released = match fault {
                Some(starts) if first == starts[0] => state.error_released,
                Some(starts) if first == starts[2] => state.sibling_released,
                _ => state.released[stage],
            };
            if released {
                break;
            }
            state = shared
                .1
                .wait_timeout(state, Duration::from_millis(50))
                .unwrap()
                .0;
        }
        let result = if state.stop || Instant::now() >= deadline {
            Ok(())
        } else if fault.is_some_and(|starts| first == starts[0]) {
            // Clean socket EOF with Content-Length still one byte short.
            socket.shutdown(std::net::Shutdown::Both)
        } else {
            let byte = body[end - 1] ^ u8::from(fault.is_some_and(|starts| first == starts[1]));
            socket.write_all(&[byte])
        };
        state.active[stage] -= 1;
        state.finished[stage].push(first);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory};
    use sha2::{Digest, Sha256};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn bounded_range_query_future_is_send() {
        fn require_send<T: Send>(_: T) {}
        let store = InMemory::new();
        let location = Path::from("sq8.bin");
        let (authority, _) = short_tail_authority();
        require_send(rank_verified_sq8_pages_inner(
            &store,
            &location,
            &authority,
            &[(0, 0)],
            "etag",
            &[1.0],
            &[0.0],
            &[1.0],
            1,
            1,
            3328,
            1,
            &[],
        ));
    }

    #[tokio::test]
    async fn source_authority_is_rejected_before_sq8_get() {
        let (authority, _) = source_tail_authority();
        let failure = rank_verified_sq8_pages_inner(
            &InMemory::new(),
            &Path::from("absent.bin"),
            &authority,
            &[(1, 1)],
            "etag",
            &[1.0; 5],
            &[0.0; 5],
            &[1.0; 5],
            1,
            1,
            10,
            1,
            &[],
        )
        .await
        .unwrap_err();
        assert_eq!(failure.stats.submitted_gets, 0);
        assert!(matches!(failure.error, RangeFetchError::UnexpectedMetadata));
    }

    fn offload_authority() -> (PageAuthority, Vec<u8>) {
        let object = vec![7u8; (8 * 32 + 1) * 10];
        let sidecar = object
            .chunks(320)
            .flat_map(|page| Sha256::digest(page).to_vec())
            .collect::<Vec<_>>();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-two-bit-plane-v3", "rows":8 * 32 + 1, "dimensions":5,
            "seed":20260923, "record_bytes":10, "page_rows":32,
            "source_sha256":"0".repeat(64), "sq8_sha256":"1".repeat(64),
            "source_order_sha256":"2".repeat(64), "mean_sha256":"3".repeat(64),
            "records_sha256":format!("{:x}", Sha256::digest(&object)),
            "page_digest_sha256":format!("{:x}", Sha256::digest(&sidecar)),
            "query_or_truth_used":false,
        }))
        .unwrap();
        let authority = PageAuthority::load_two_bit(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            9,
            &sidecar,
        )
        .unwrap();
        (authority, object)
    }

    #[derive(Debug)]
    struct OffloadState {
        requested: Mutex<Vec<usize>>,
        polled: std::sync::atomic::AtomicUsize,
        completed: std::sync::atomic::AtomicUsize,
        active: std::sync::atomic::AtomicUsize,
        bodies: tokio::sync::Semaphore,
    }
    #[derive(Debug)]
    struct OffloadStore {
        inner: InMemory,
        state: Arc<OffloadState>,
        hold_bodies: bool,
        corrupt: Option<usize>,
        transport_error: Option<usize>,
    }
    impl std::fmt::Display for OffloadStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("SOURCE offload fixture")
        }
    }
    struct BodyActive(Arc<OffloadState>);
    impl Drop for BodyActive {
        fn drop(&mut self) {
            self.0
                .active
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for OffloadStore {
        async fn get_opts(
            &self,
            path: &Path,
            options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let head = options.head;
            let mut result = self.inner.get_opts(path, options).await?;
            if head {
                return Ok(result);
            }
            let index = result.range.start as usize / 320;
            self.state.requested.lock().unwrap().push(index);
            let mut body = match result.payload {
                GetResultPayload::Stream(stream) => {
                    use futures_util::TryStreamExt;
                    let chunks: Vec<Bytes> = stream.try_collect().await?;
                    chunks.concat()
                }
                _ => panic!("in-memory fixture must stream"),
            };
            if self.corrupt == Some(index) {
                body[0] ^= 1;
            }
            let fail = self.transport_error == Some(index);
            let hold = self.hold_bodies && index >= 2;
            let state = Arc::clone(&self.state);
            result.payload = GetResultPayload::Stream(
                stream::once(async move {
                    state
                        .polled
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    state
                        .active
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _active = BodyActive(Arc::clone(&state));
                    let _held = if hold {
                        Some(state.bodies.acquire().await.unwrap())
                    } else {
                        None
                    };
                    state
                        .completed
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if fail {
                        Err(object_store::Error::Generic {
                            store: "SOURCE offload fixture",
                            source: std::io::Error::other("ordered transport fault").into(),
                        })
                    } else {
                        Ok(Bytes::from(body))
                    }
                })
                .boxed(),
            );
            Ok(result)
        }
        async fn put_opts(
            &self,
            path: &Path,
            body: PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(path, body, options).await
        }

        async fn put_multipart_opts(
            &self,
            path: &Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(path, options).await
        }

        fn delete_stream(
            &self,
            paths: stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(paths)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    async fn offload_store(
        hold_bodies: bool,
        corrupt: Option<usize>,
        transport_error: Option<usize>,
    ) -> (OffloadStore, Arc<PageAuthority>, Path, String, usize) {
        let (authority, body) = offload_authority();
        let bytes = body.len();
        let location = Path::from("source.bin");
        let inner = InMemory::new();
        inner.put(&location, PutPayload::from(body)).await.unwrap();
        let etag = inner.head(&location).await.unwrap().e_tag.unwrap();
        let store = OffloadStore {
            inner,
            hold_bodies,
            corrupt,
            transport_error,
            state: Arc::new(OffloadState {
                requested: Mutex::new(Vec::new()),
                polled: 0.into(),
                completed: 0.into(),
                active: 0.into(),
                bodies: tokio::sync::Semaphore::new(0),
            }),
        };
        (store, Arc::new(authority), location, etag, bytes)
    }

    #[derive(Default)]
    struct ComputeGate {
        active: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
        entered: std::sync::atomic::AtomicUsize,
        released: Mutex<bool>,
        changed: std::sync::Condvar,
    }
    impl ComputeGate {
        fn hold(&self) {
            use std::sync::atomic::Ordering::SeqCst;
            let active = self.active.fetch_add(1, SeqCst) + 1;
            self.peak.fetch_max(active, SeqCst);
            self.entered.fetch_add(1, SeqCst);
            let (released, deadline) = self
                .changed
                .wait_timeout_while(
                    self.released.lock().unwrap(),
                    std::time::Duration::from_secs(5),
                    |released| !*released,
                )
                .unwrap();
            self.active.fetch_sub(1, SeqCst);
            assert!(
                *released && !deadline.timed_out(),
                "held compute fixture deadline"
            );
        }
        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }

    #[tokio::test]
    async fn source_auth_shared_queries_admit_without_batch_serialization() {
        let _serial = SOURCE_AUTH_TEST_SERIAL.lock().await;
        use std::time::Duration;
        let first = SourceAuthWorkers::new(2).await.unwrap();
        let second = tokio::time::timeout(Duration::from_millis(100), SourceAuthWorkers::new(2))
            .await
            .expect("a live first query must not serialize second query admission")
            .unwrap();
        // Both owners remain alive here: admission cannot wait for the first batch to drop.
        drop(second);
        drop(first);
    }

    #[tokio::test]
    async fn source_auth_shared_queries_download_while_compute_is_held() {
        let _serial = SOURCE_AUTH_TEST_SERIAL.lock().await;
        use std::sync::atomic::Ordering::SeqCst;
        let (store, authority, location, etag, bytes) = offload_store(true, None, None).await;
        let ranges: Vec<_> = (0..9)
            .map(|page| authority.byte_range(page, page).unwrap())
            .collect();
        let first = SourceAuthWorkers::new(4).await.unwrap();
        let second = SourceAuthWorkers::new(4).await.unwrap();
        assert!(Arc::ptr_eq(&first.pool, &second.pool));
        assert_eq!(first.pool.workers.len(), SOURCE_AUTH_WORKERS);
        let gate = Arc::new(ComputeGate::default());
        let window = InFlightBytes::new(bytes);
        let first_job = |gate: Arc<ComputeGate>| {
            first.authenticate(None, move || {
                gate.hold();
                Ok(())
            })
        };
        let work = fetch_capsule_ranges_with_authentication(
            &store,
            &location,
            bytes,
            &ranges,
            &etag,
            9,
            bytes,
            4,
            Some(&window),
            None,
            Some(&second),
            |offset, bytes, probe, byte_lease| {
                let gate = Arc::clone(&gate);
                let authority = Arc::clone(&authority);
                second.authenticate(probe, move || {
                    let _byte_lease = byte_lease;
                    gate.hold();
                    authority
                        .verify_payload(offset / 320, offset / 320, &bytes)
                        .map_err(RangeFetchError::Page)?;
                    Ok(bytes)
                })
            },
        );
        let (one, two, result, ()) =
            tokio::time::timeout(std::time::Duration::from_secs(4), async {
                tokio::join!(
                    first_job(Arc::clone(&gate)),
                    first_job(Arc::clone(&gate)),
                    work,
                    async {
                        while gate.entered.load(SeqCst) < 2 || store.state.polled.load(SeqCst) < 6 {
                            tokio::task::yield_now().await;
                        }
                        assert_eq!(gate.active.load(SeqCst), 2);
                        assert_eq!(store.state.active.load(SeqCst), 4);
                        store.state.bodies.add_permits(4);
                        while store.state.completed.load(SeqCst) < 6 {
                            tokio::task::yield_now().await;
                        }
                        assert_eq!(gate.entered.load(SeqCst), 2);
                        assert_eq!(store.state.requested.lock().unwrap().len(), 6);
                        assert_eq!(second.slots.available_permits(), 0);
                        gate.release();
                    }
                )
            })
            .await
            .expect("cross-query download progress deadline");
        one.0.unwrap();
        two.0.unwrap();
        let (_, stats) = result.unwrap();
        assert_eq!(stats.submitted_gets, 9);
        assert_eq!(stats.verified_bytes, bytes);
        assert_eq!(gate.entered.load(SeqCst), 11);
        assert_eq!(gate.peak.load(SeqCst), 2);
        assert_eq!(gate.active.load(SeqCst), 0);
        assert_eq!(*window.available.lock().unwrap(), bytes);
        drop(second);
        drop(first);
        assert_eq!(SOURCE_AUTH_THREADS.available_permits(), SOURCE_AUTH_WORKERS);
    }

    #[tokio::test]
    async fn source_auth_shared_verifier_panic_preserves_both_workers() {
        let _serial = SOURCE_AUTH_TEST_SERIAL.lock().await;
        use std::sync::atomic::Ordering::SeqCst;
        let first = SourceAuthWorkers::new(2).await.unwrap();
        let second = SourceAuthWorkers::new(2).await.unwrap();
        assert!(Arc::ptr_eq(&first.pool, &second.pool));
        let failed = first
            .authenticate::<()>(None, || panic!("expected SOURCE verifier panic"))
            .await;
        assert!(matches!(failed.0, Err(RangeFetchError::UnexpectedMetadata)));
        drop(first);
        let gate = Arc::new(ComputeGate::default());
        let authenticate = |gate: Arc<ComputeGate>| {
            second.authenticate(None, move || {
                gate.hold();
                Ok(())
            })
        };
        let (one, two, ()) = tokio::time::timeout(std::time::Duration::from_secs(4), async {
            tokio::join!(
                authenticate(Arc::clone(&gate)),
                authenticate(Arc::clone(&gate)),
                async {
                    while gate.entered.load(SeqCst) < 2 {
                        tokio::task::yield_now().await;
                    }
                    gate.release();
                }
            )
        })
        .await
        .expect("both workers must survive a verifier panic");
        one.0.unwrap();
        two.0.unwrap();
        assert_eq!(gate.peak.load(SeqCst), 2);
        drop(second);
        assert_eq!(SOURCE_AUTH_THREADS.available_permits(), SOURCE_AUTH_WORKERS);
    }

    #[test]
    fn source_auth_shared_pool_reuses_across_runtimes_and_releases_last_owner() {
        let first_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _serial = first_runtime.block_on(SOURCE_AUTH_TEST_SERIAL.lock());
        let first = first_runtime.block_on(SourceAuthWorkers::new(2)).unwrap();
        let old_pool = Arc::downgrade(&first.pool);
        let second_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let second = second_runtime.block_on(SourceAuthWorkers::new(2)).unwrap();
        assert!(Arc::ptr_eq(&first.pool, &second.pool));
        drop(first);
        drop(first_runtime);
        assert_eq!(
            second_runtime
                .block_on(second.authenticate(None, || Ok(41usize)))
                .0
                .unwrap(),
            41
        );
        drop(second);
        assert!(old_pool.upgrade().is_none());
        assert_eq!(SOURCE_AUTH_THREADS.available_permits(), SOURCE_AUTH_WORKERS);
        let replacement = second_runtime.block_on(SourceAuthWorkers::new(2)).unwrap();
        assert_ne!(old_pool.as_ptr(), Arc::as_ptr(&replacement.pool));
        drop(replacement);
        assert_eq!(SOURCE_AUTH_THREADS.available_permits(), SOURCE_AUTH_WORKERS);
    }

    #[tokio::test]
    async fn source_auth_offload_shared_admission_and_cancelled_job_keep_permits() {
        let _serial = SOURCE_AUTH_TEST_SERIAL.lock().await;
        use std::time::Duration;
        let workers = SourceAuthWorkers::new(2).await.unwrap();
        let lifetime = Arc::downgrade(&workers.lifetime);
        let window = InFlightBytes::new(16);
        let byte_lease = window.reserve(16).await.unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut authentication = Box::pin(workers.authenticate(None, move || {
            let _byte_lease = byte_lease;
            entered_tx.send(()).unwrap();
            // A failing assertion must not strand this test's destructor.
            let _ = release_rx.recv_timeout(Duration::from_secs(2));
            Ok(())
        }));
        tokio::select! {
            _ = &mut authentication => panic!("held authentication finished"),
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(authentication);
        assert_eq!(workers.slots.available_permits(), 1);
        assert_eq!(*window.available.lock().unwrap(), 0);
        // Destruction joins off-thread, while a second query/runtime may seek admission.
        let dropping = std::thread::spawn(move || drop(workers));
        let next = tokio::time::timeout(Duration::from_secs(1), SourceAuthWorkers::new(2))
            .await
            .expect("another query may download while cancelled work drains")
            .unwrap();
        assert_eq!(next.slots.available_permits(), 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), window.reserve(16))
                .await
                .is_err()
        );
        assert_eq!(*window.available.lock().unwrap(), 0);
        release_tx.send(()).unwrap();
        dropping.join().unwrap();
        assert!(lifetime.upgrade().is_none());
        assert_eq!(*window.available.lock().unwrap(), 16);
        drop(next);
    }

    #[tokio::test]
    async fn source_auth_offload_parity_and_ordered_errors() {
        let _serial = SOURCE_AUTH_TEST_SERIAL.lock().await;
        for (corrupt, transport) in [(None, None), (Some(0), Some(1)), (Some(1), Some(0))] {
            let mut control = None;
            for mode in [SourceAuthentication::Inline, SourceAuthentication::Offload2] {
                let (store, authority, location, etag, bytes) =
                    offload_store(false, corrupt, transport).await;
                let ranges: Vec<_> = (0..9).map(|page| (page, page)).collect();
                let window = InFlightBytes::new(bytes);
                let mut trace = Sq8RangeTrace::new(9).unwrap();
                trace.begin(Instant::now());
                let result = fetch_source_ranges_traced(
                    &store,
                    &location,
                    &authority,
                    &ranges,
                    &etag,
                    9,
                    bytes,
                    2,
                    Some(&mut trace),
                    Some(&window),
                    mode,
                )
                .await;
                let (payloads, stats, error) = match result {
                    Ok((ranges, stats)) => (
                        ranges
                            .into_iter()
                            .map(|r| (r.start, r.bytes))
                            .collect::<Vec<_>>(),
                        stats,
                        "ok",
                    ),
                    Err(failure) => {
                        let error = match failure.error {
                            RangeFetchError::Page(PageError::HashMismatch) => "auth",
                            RangeFetchError::Store(_) => "transport",
                            other => panic!("unexpected error {other:?}"),
                        };
                        (Vec::new(), failure.stats, error)
                    }
                };
                assert_eq!(stats.submitted_gets, 9);
                assert_eq!(stats.failed_gets, if corrupt.is_some() { 2 } else { 0 });
                assert_eq!(
                    stats.verified_bytes,
                    bytes - if corrupt.is_some() { 640 } else { 0 }
                );
                assert_eq!(
                    error,
                    if corrupt == Some(0) {
                        "auth"
                    } else if transport == Some(0) {
                        "transport"
                    } else {
                        "ok"
                    }
                );
                assert_eq!(
                    store
                        .state
                        .completed
                        .load(std::sync::atomic::Ordering::SeqCst),
                    9
                );
                let mut requests = store.state.requested.lock().unwrap().clone();
                requests.sort_unstable();
                assert_eq!(requests, (0..9).collect::<Vec<_>>());
                assert_eq!(*window.available.lock().unwrap(), bytes);
                assert_eq!(Arc::strong_count(&authority), 1);
                let spans: Vec<_> = trace
                    .ranges()
                    .iter()
                    .map(|span| {
                        assert_body_boundaries(span);
                        (
                            span.index,
                            span.start_byte,
                            span.end_byte,
                            span.outcome,
                            span.body_bytes,
                        )
                    })
                    .collect();
                let observed = (
                    payloads,
                    stats,
                    error,
                    spans,
                    trace.outcome,
                    trace.retained_payload_bytes,
                );
                if let Some(control) = &control {
                    assert_eq!(&observed, control);
                } else {
                    control = Some(observed);
                }
            }
        }
    }

    #[tokio::test]
    async fn source_auth_offload_polls_downloads_and_bounds_compute() {
        let _serial = SOURCE_AUTH_TEST_SERIAL.lock().await;
        use std::sync::atomic::Ordering::SeqCst;
        let (store, authority, location, etag, bytes) = offload_store(true, None, None).await;
        let ranges: Vec<_> = (0..9)
            .map(|page| authority.byte_range(page, page).unwrap())
            .collect();
        let workers = SourceAuthWorkers::new(4).await.unwrap();
        let lifetime = Arc::clone(&workers.lifetime);
        let gate = Arc::new(ComputeGate::default());
        let window = InFlightBytes::new(bytes);
        let work = fetch_capsule_ranges_with_authentication(
            &store,
            &location,
            bytes,
            &ranges,
            &etag,
            9,
            bytes,
            4,
            Some(&window),
            None,
            Some(&workers),
            |offset, bytes, probe, byte_lease| {
                let gate = Arc::clone(&gate);
                let authority = Arc::clone(&authority);
                workers.authenticate(probe, move || {
                    let _byte_lease = byte_lease;
                    gate.hold();
                    authority
                        .verify_payload(offset / 320, offset / 320, &bytes)
                        .map_err(RangeFetchError::Page)?;
                    Ok(bytes)
                })
            },
        );
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(4), async {
            tokio::join!(work, async {
                while gate.entered.load(SeqCst) < 2 || store.state.polled.load(SeqCst) < 6 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(gate.active.load(SeqCst), 2);
                assert_eq!(store.state.active.load(SeqCst), 4);
                store.state.bodies.add_permits(4);
                while store.state.completed.load(SeqCst) < 6 {
                    tokio::task::yield_now().await;
                }
                // Bodies completed while both compute workers were held; no unbounded refill.
                assert_eq!(gate.entered.load(SeqCst), 2);
                assert_eq!(store.state.requested.lock().unwrap().len(), 6);
                gate.release();
            })
        })
        .await
        .expect("offload polling deadline");
        let (_, stats) = result.unwrap();
        assert_eq!(stats.submitted_gets, 9);
        assert_eq!(stats.verified_bytes, bytes);
        assert_eq!(gate.peak.load(SeqCst), 2);
        assert_eq!(gate.entered.load(SeqCst), 9);
        assert_eq!(*window.available.lock().unwrap(), bytes);
        drop(workers);
        assert_eq!(
            Arc::strong_count(&lifetime),
            1,
            "zero query-owned authentication jobs on return"
        );
        assert_eq!(Arc::strong_count(&authority), 1);
    }

    #[tokio::test]
    async fn source_auth_offload_cancellation_joins_workers() {
        let _serial = SOURCE_AUTH_TEST_SERIAL.lock().await;
        use std::sync::atomic::Ordering::SeqCst;
        let (store, authority, location, etag, bytes) = offload_store(true, None, None).await;
        let ranges: Vec<_> = (0..9)
            .map(|page| authority.byte_range(page, page).unwrap())
            .collect();
        let workers = SourceAuthWorkers::new(4).await.unwrap();
        let lifetime = Arc::clone(&workers.lifetime);
        let gate = Arc::new(ComputeGate::default());
        let window = InFlightBytes::new(bytes);
        let mut work = Box::pin(async {
            let workers = workers;
            fetch_capsule_ranges_with_authentication(
                &store,
                &location,
                bytes,
                &ranges,
                &etag,
                9,
                bytes,
                4,
                Some(&window),
                None,
                Some(&workers),
                |offset, bytes, probe, byte_lease| {
                    let gate = Arc::clone(&gate);
                    let authority = Arc::clone(&authority);
                    workers.authenticate(probe, move || {
                        let _byte_lease = byte_lease;
                        gate.hold();
                        authority
                            .verify_payload(offset / 320, offset / 320, &bytes)
                            .map_err(RangeFetchError::Page)?;
                        Ok(bytes)
                    })
                },
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                tokio::select! {
                    result = &mut work => panic!("held work finished: {result:?}"),
                    () = tokio::task::yield_now() => {
                        if gate.active.load(SeqCst) == 2 && store.state.active.load(SeqCst) == 4 { break; }
                    }
                }
            }
        }).await.unwrap();
        let release = Arc::clone(&gate);
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            release.release();
        });
        drop(work);
        assert!(
            *gate.released.lock().unwrap(),
            "cancellation waited for the bounded owned work"
        );
        assert_eq!(gate.active.load(SeqCst), 0);
        assert_eq!(store.state.active.load(SeqCst), 0);
        assert_eq!(*window.available.lock().unwrap(), bytes);
        assert_eq!(
            Arc::strong_count(&lifetime),
            1,
            "zero worker threads after cancellation"
        );
        assert_eq!(Arc::strong_count(&authority), 1, "generation pins returned");
        releaser.join().unwrap();
    }

    fn source_tail_authority() -> (PageAuthority, Vec<u8>) {
        let object = vec![7u8; 33 * 10];
        let sidecar = object
            .chunks(320)
            .flat_map(|page| Sha256::digest(page).to_vec())
            .collect::<Vec<_>>();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-two-bit-plane-v3", "rows":33, "dimensions":5,
            "seed":20260923, "record_bytes":10, "page_rows":32,
            "source_sha256":"0".repeat(64), "sq8_sha256":"1".repeat(64),
            "source_order_sha256":"2".repeat(64), "mean_sha256":"3".repeat(64),
            "records_sha256":format!("{:x}", Sha256::digest(&object)),
            "page_digest_sha256":format!("{:x}", Sha256::digest(&sidecar)),
            "query_or_truth_used":false,
        }))
        .unwrap();
        let authority = PageAuthority::load_two_bit(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            9,
            &sidecar,
        )
        .unwrap();
        (authority, object)
    }

    #[tokio::test]
    async fn bounded_source_ranges_authenticate_tail_and_charge_failures() {
        let store = InMemory::new();
        let location = Path::from("source.bin");
        let (authority, object) = source_tail_authority();
        store
            .put(&location, PutPayload::from(object.clone()))
            .await
            .unwrap();
        let etag = store.head(&location).await.unwrap().e_tag.unwrap();
        let ranges = [(0, 0), (1, 1)];
        let (verified, stats) =
            fetch_verified_ranges_inner(&store, &location, &authority, &ranges, &etag, 2, 330, 2)
                .await
                .unwrap();
        assert_eq!(
            stats,
            Sq8ReadStats {
                submitted_gets: 2,
                verified_bytes: 330,
                failed_gets: 0
            }
        );
        assert_eq!(verified[0].bytes.as_ref(), &object[..320]);
        assert_eq!(verified[1].start, 320);
        assert_eq!(verified[1].bytes.as_ref(), &object[320..]);
        for (planned, gets, bytes, parallel) in [
            (ranges.as_slice(), 1, 330, 2),
            (ranges.as_slice(), 2, 329, 2),
            (ranges.as_slice(), 2, 330, 0),
            (&[(0, 1), (1, 1)], 2, 340, 2),
        ] {
            let failure = fetch_verified_ranges_inner(
                &store, &location, &authority, planned, &etag, gets, bytes, parallel,
            )
            .await
            .err()
            .unwrap();
            assert_eq!(failure.stats, Sq8ReadStats::default());
        }
        let mut changed = object;
        changed[320] ^= 1;
        store
            .put(&location, PutPayload::from(changed))
            .await
            .unwrap();
        let etag = store.head(&location).await.unwrap().e_tag.unwrap();
        let failure =
            fetch_verified_ranges_inner(&store, &location, &authority, &ranges, &etag, 2, 330, 2)
                .await
                .err()
                .unwrap();
        assert_eq!(
            failure.stats,
            Sq8ReadStats {
                submitted_gets: 2,
                verified_bytes: 320,
                failed_gets: 1
            }
        );
        assert!(matches!(
            failure.error,
            RangeFetchError::Page(PageError::HashMismatch)
        ));
    }

    fn short_tail_authority() -> (PageAuthority, Vec<u8>) {
        let object = vec![7u8; 273 * 13];
        let sidecar = object
            .chunks(256 * 13)
            .flat_map(|page| Sha256::digest(page).to_vec())
            .collect::<Vec<_>>();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":1,
            "rows":273, "dimensions":1, "page_rows":256,
            "object_sha256":format!("{:x}", Sha256::digest(&object)),
            "page_digest_sha256":format!("{:x}", Sha256::digest(&sidecar)),
        }))
        .unwrap();
        let authority = PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .unwrap();
        (authority, object)
    }

    /// Script the collector's exact stream items, independent of HTTP coalescing.
    #[derive(Debug)]
    struct ChunkStore {
        inner: InMemory,
        chunks: Vec<Option<Bytes>>,
    }

    impl std::fmt::Display for ChunkStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("chunk-fixture")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ChunkStore {
        async fn get_opts(
            &self,
            path: &Path,
            options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let mut result = self.inner.get_opts(path, options).await?;
            result.payload = GetResultPayload::Stream(
                stream::iter(self.chunks.clone().into_iter().map(|chunk| {
                    chunk.ok_or_else(|| object_store::Error::Generic {
                        store: "chunk-fixture",
                        source: std::io::Error::other("injected EOF error").into(),
                    })
                }))
                .boxed(),
            );
            Ok(result)
        }

        async fn put_opts(
            &self,
            path: &Path,
            body: PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(path, body, options).await
        }

        async fn put_multipart_opts(
            &self,
            path: &Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(path, options).await
        }

        fn delete_stream(
            &self,
            paths: stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(paths)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn assert_body_boundaries(span: &RangeSpan) {
        let chain = [
            span.first_poll_ns,
            span.request_ns,
            span.headers_ns,
            span.metadata_ns,
            span.first_chunk_ns,
            span.last_chunk_ns,
            span.eof_ns,
            span.auth_start_ns,
            span.auth_end_ns,
            span.complete_ns,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        assert!(chain.windows(2).all(|w| w[0] <= w[1]), "{chain:?}");
        if let Some(eof) = span.eof_ns {
            let metadata = span.metadata_ns.unwrap();
            assert!(span.copy_ns + span.max_body_gap_ns <= eof - metadata);
            if let Some(gap_end) = span.max_body_gap_end_ns {
                assert!(metadata <= gap_end && gap_end <= eof);
            }
        }
    }

    #[tokio::test]
    async fn traced_body_clock_reuse_counts_chunks_and_failure_boundaries() {
        let (authority, object) = short_tail_authority();
        let tail = Bytes::copy_from_slice(&object[3328..]);
        let mut changed = tail.to_vec();
        changed[0] ^= 1;
        let mut overlong = tail.to_vec();
        overlong.push(0);
        let cases = [
            ("coalesced", vec![Some(tail.clone())], "ok", 1, 1, 11),
            (
                "multi-empty",
                vec![
                    Some(tail.slice(..17)),
                    Some(Bytes::new()),
                    Some(tail.slice(17..)),
                ],
                "ok",
                3,
                3,
                15,
            ),
            (
                "eof-error",
                vec![Some(tail.clone()), None],
                "store_body",
                1,
                1,
                9,
            ),
            (
                "overlong",
                vec![Some(Bytes::from(overlong))],
                "overlong",
                1,
                0,
                8,
            ),
            (
                "truncated",
                vec![Some(tail.slice(..tail.len() - 1))],
                "page_auth",
                1,
                1,
                11,
            ),
            ("empty-body", vec![], "page_auth", 0, 0, 9),
            (
                "auth-failure",
                vec![Some(Bytes::from(changed))],
                "page_auth",
                1,
                1,
                11,
            ),
        ];
        for (name, chunks, outcome, chunk_count, copy_count, clock_count) in cases {
            let store = ChunkStore {
                inner: InMemory::new(),
                chunks,
            };
            let location = Path::from("sq8.bin");
            let etag = store
                .put(&location, PutPayload::from(object.clone()))
                .await
                .unwrap()
                .e_tag
                .unwrap();
            for tracing in [false, true] {
                PROBE_CLOCK_READS.with(|reads| reads.set(0));
                PROFILE_CLOCK_READS.with(|reads| reads.set(0));
                let (result, probe) = fetch_verified_pages_traced(
                    &store,
                    &location,
                    &authority,
                    1,
                    1,
                    &etag,
                    tail.len(),
                    tracing.then(|| (Instant::now(), 0)),
                )
                .await;
                assert_eq!(result.is_ok(), outcome == "ok", "{name}");
                assert_eq!(
                    PROBE_CLOCK_READS.with(|reads| reads.get()),
                    if tracing { clock_count } else { 0 },
                    "{name}"
                );
                let measured_auth = !matches!(name, "eof-error" | "overlong");
                assert_eq!(
                    PROFILE_CLOCK_READS.with(|reads| reads.get()),
                    if tracing {
                        4 + 4 * u64::from(copy_count) + 2 * u64::from(measured_auth)
                    } else {
                        0
                    },
                    "{name}"
                );
                if let Ok(verified) = result.as_ref() {
                    assert_eq!(verified.start, 3328);
                    assert_eq!(verified.bytes, tail);
                }
                match name {
                    "eof-error" => assert!(matches!(result, Err(RangeFetchError::Store(_)))),
                    "overlong" => {
                        assert!(matches!(result, Err(RangeFetchError::UnexpectedMetadata)))
                    }
                    "auth-failure" => assert!(matches!(
                        result,
                        Err(RangeFetchError::Page(PageError::HashMismatch))
                    )),
                    _ => (),
                }
                if let Some(probe) = probe {
                    let span = &probe.span;
                    assert_eq!(span.outcome, outcome, "{name}");
                    assert_eq!(
                        (span.chunks, span.copy_count),
                        (chunk_count, copy_count),
                        "{name}"
                    );
                    let expected_bytes =
                        store.chunks.iter().flatten().map(Bytes::len).sum::<usize>();
                    assert_eq!(span.body_bytes, expected_bytes as u64, "{name}");
                    assert_eq!(
                        span.eof_ns.is_some(),
                        !matches!(name, "eof-error" | "overlong")
                    );
                    assert_eq!(span.auth_start_ns.is_some(), span.eof_ns.is_some());
                    assert_eq!(span.auth_end_ns.is_some(), span.eof_ns.is_some());
                    assert_eq!(span.first_chunk_ns.is_some(), chunk_count > 0);
                    assert_eq!(span.last_chunk_ns.is_some(), chunk_count > 0);
                    assert!(span.complete_ns.is_some());
                    assert_eq!(probe.cpu.collector_allocation.unwrap().clock_reads, 4);
                    assert_eq!(probe.cpu.collector_copy.is_some(), copy_count > 0);
                    assert_eq!(probe.cpu.authentication.is_some(), measured_auth);
                    assert_eq!(
                        probe.cpu.collector_copied_bytes,
                        if copy_count == 0 { 0 } else { span.body_bytes }
                    );
                    assert_body_boundaries(span);
                    if outcome == "ok" {
                        // Four pre-body boundaries + initial mark + EOF + auth pair + completion.
                        assert_eq!(clock_count, 2 * u64::from(span.chunks) + 9);
                    }
                } else {
                    assert!(!tracing);
                }
            }
        }
    }

    #[tokio::test]
    async fn real_http_traced_body_clock_count_matches_delivered_chunks() {
        let (authority, object) = short_tail_authority();
        let tail = &object[3328..];
        let (reader, stop, requests, server) = http_fixture(http_response(
            "206 Partial Content",
            Some("bytes 3328-3548/3549"),
            "\"frozen\"",
            tail.len(),
            tail,
        ));
        PROBE_CLOCK_READS.with(|reads| reads.set(0));
        let (result, probe) = fetch_verified_pages_traced(
            reader.store(),
            &Path::from("sq8.bin"),
            &authority,
            1,
            1,
            "\"frozen\"",
            tail.len(),
            Some((Instant::now(), 0)),
        )
        .await;
        let reads = PROBE_CLOCK_READS.with(|reads| reads.get());
        stop.store(true, Ordering::Relaxed);
        server.join().unwrap();
        assert_eq!(result.unwrap().bytes.as_ref(), tail);
        assert_eq!(requests.lock().unwrap().len(), 1);
        let span = &probe.unwrap().span;
        assert!(span.chunks >= 1);
        assert_eq!(span.chunks, span.copy_count);
        assert_eq!(span.body_bytes, tail.len() as u64);
        assert_eq!(reads, 2 * u64::from(span.chunks) + 9);
        assert_body_boundaries(span);
    }

    fn http_response(
        status: &str,
        content_range: Option<&str>,
        etag: &str,
        declared_bytes: usize,
        body: &[u8],
    ) -> Vec<u8> {
        let mut headers = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {declared_bytes}\r\nETag: {etag}\r\nLast-Modified: Wed, 23 Sep 2026 00:00:00 GMT\r\nConnection: close\r\n"
        );
        if let Some(range) = content_range {
            headers.push_str(&format!("Content-Range: {range}\r\n"));
        }
        headers.push_str("\r\n");
        let mut response = headers.into_bytes();
        response.extend_from_slice(body);
        response
    }

    fn http_fixture(
        response: Vec<u8>,
    ) -> (
        OneAttemptS3,
        Arc<AtomicBool>,
        Arc<Mutex<Vec<String>>>,
        thread::JoinHandle<()>,
    ) {
        http_fixture_responses(response, None)
    }

    fn http_fixture_responses(
        response: Vec<u8>,
        source_response: Option<Vec<u8>>,
    ) -> (
        OneAttemptS3,
        Arc<AtomicBool>,
        Arc<Mutex<Vec<String>>>,
        thread::JoinHandle<()>,
    ) {
        http_fixture_responses_with_prefix(response, source_response, Path::default())
    }

    fn http_fixture_responses_with_prefix(
        response: Vec<u8>,
        source_response: Option<Vec<u8>>,
        prefix: Path,
    ) -> (
        OneAttemptS3,
        Arc<AtomicBool>,
        Arc<Mutex<Vec<String>>>,
        thread::JoinHandle<()>,
    ) {
        http_fixture_with_copy_response(
            response,
            source_response,
            prefix,
            None,
            Duration::from_secs(30),
            None,
        )
    }

    fn http_fixture_with_copy_response(
        response: Vec<u8>,
        source_response: Option<Vec<u8>>,
        prefix: Path,
        copy_success: Option<bool>,
        request_timeout: Duration,
        response_byte_delay: Option<Duration>,
    ) -> (
        OneAttemptS3,
        Arc<AtomicBool>,
        Arc<Mutex<Vec<String>>>,
        thread::JoinHandle<()>,
    ) {
        let (store, counters, stop, requests, server) = http_transport_fixture(
            response,
            source_response,
            prefix,
            copy_success,
            request_timeout,
            response_byte_delay,
            None,
            None,
        );
        (OneAttemptS3 { store, counters }, stop, requests, server)
    }

    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn http_transport_fixture(
        response: Vec<u8>,
        source_response: Option<Vec<u8>>,
        prefix: Path,
        copy_success: Option<bool>,
        request_timeout: Duration,
        response_byte_delay: Option<Duration>,
        policy: Option<S3ReadRetryPolicy>,
        sequence: Option<Vec<Vec<u8>>>,
    ) -> (
        PrefixStore<AmazonS3>,
        Arc<Mutex<TransportCounters>>,
        Arc<AtomicBool>,
        Arc<Mutex<Vec<String>>>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_stop = Arc::clone(&stop);
        let server_requests = Arc::clone(&requests);
        let server = thread::spawn(move || {
            let mut sequence = sequence.map(std::collections::VecDeque::from);
            let deadline = Instant::now() + Duration::from_secs(8);
            while !server_stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        socket
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        let mut raw = Vec::new();
                        let mut buffer = [0u8; 4096];
                        while !raw.windows(4).any(|part| part == b"\r\n\r\n") {
                            let count = socket.read(&mut buffer).unwrap();
                            if count == 0 {
                                break;
                            }
                            raw.extend_from_slice(&buffer[..count]);
                            assert!(raw.len() < 64 * 1024);
                        }
                        if copy_success.is_some() {
                            let header_end =
                                raw.windows(4).position(|v| v == b"\r\n\r\n").unwrap() + 4;
                            let headers = std::str::from_utf8(&raw[..header_end]).unwrap();
                            let body_len = headers
                                .lines()
                                .find_map(|line| {
                                    let (key, value) = line.split_once(':')?;
                                    key.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            assert!(header_end + body_len < 64 * 1024);
                            while raw.len() < header_end + body_len {
                                let count = socket.read(&mut buffer).unwrap();
                                assert!(count > 0);
                                raw.extend_from_slice(&buffer[..count]);
                            }
                        }
                        let request = String::from_utf8(raw).unwrap();
                        let copy_response = copy_success.map(|success| {
                            let line = request.lines().next().unwrap();
                            let (status, body): (&str, &[u8]) = if line.starts_with("DELETE ") {
                                ("204 No Content", b"")
                            } else if line.contains("uploads") {
                                ("200 OK", b"<InitiateMultipartUploadResult><UploadId>fixture-upload</UploadId></InitiateMultipartUploadResult>")
                            } else if line.starts_with("PUT ") {
                                ("200 OK", b"<CopyPartResult><ETag>part-etag</ETag></CopyPartResult>")
                            } else if success {
                                ("200 OK", b"<CompleteMultipartUploadResult><ETag>new-etag</ETag></CompleteMultipartUploadResult>")
                            } else {
                                ("412 Precondition Failed", b"<Error><Code>PreconditionFailed</Code></Error>")
                            };
                            http_response(status, None, "\"fixture\"", body.len(), body)
                        });
                        let source = request
                            .lines()
                            .next()
                            .unwrap()
                            .contains("/plane/records.bin ");
                        server_requests.lock().unwrap().push(request);
                        let next_response = sequence.as_mut().and_then(|v| v.pop_front());
                        let body = if let Some(body) = &next_response {
                            body
                        } else if let Some(body) = &copy_response {
                            body
                        } else if source {
                            source_response.as_ref().unwrap_or(&response)
                        } else {
                            &response
                        };
                        if let Some(delay) = response_byte_delay {
                            let header_end =
                                body.windows(4).position(|v| v == b"\r\n\r\n").unwrap() + 4;
                            if socket.write_all(&body[..header_end]).is_ok() {
                                for byte in &body[header_end..] {
                                    thread::sleep(delay);
                                    if socket.write_all(&[*byte]).is_err() {
                                        break;
                                    }
                                }
                            }
                        } else {
                            socket.write_all(body).unwrap();
                        }
                        let unfinished = b"Content-Length: 1073741824\r\n";
                        if body.windows(unfinished.len()).any(|v| v == unfinished) {
                            while !server_stop.load(Ordering::Relaxed) && Instant::now() < deadline
                            {
                                thread::sleep(Duration::from_millis(5));
                            }
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                }
            }
        });
        // Isolate live fixtures from each other while exercising the same boundary.
        let counters = Arc::new(Mutex::new(TransportCounters::default()));
        let store = native_builder(
            "fixture",
            "eu-central-1",
            counters.clone(),
            request_timeout,
            policy,
        )
        .unwrap()
        .with_endpoint(format!("http://{address}"))
        .with_allow_http(true)
        .with_access_key_id("fixture")
        .with_secret_access_key("fixture")
        .build()
        .unwrap();
        (
            PrefixStore::new(store, prefix),
            counters,
            stop,
            requests,
            server,
        )
    }

    fn read_retry_fixture(
        responses: Vec<Vec<u8>>,
    ) -> (
        BoundedReadS3,
        Arc<AtomicBool>,
        Arc<Mutex<Vec<String>>>,
        thread::JoinHandle<()>,
    ) {
        let fallback = responses.last().unwrap().clone();
        let (store, counters, stop, requests, server) = http_transport_fixture(
            fallback,
            None,
            Path::default(),
            None,
            Duration::from_secs(30),
            None,
            Some(S3ReadRetryPolicy::OneRetry100Ms),
            Some(responses),
        );
        (BoundedReadS3 { store, counters }, stop, requests, server)
    }

    #[tokio::test]
    async fn native_s3_create_copy_uses_conditional_multipart() {
        for success in [true, false] {
            let (reader, stop, requests, server) = http_fixture_with_copy_response(
                Vec::new(),
                None,
                Path::from("tenant/import"),
                Some(success),
                Duration::from_secs(30),
                None,
            );
            let result = reader
                .store()
                .copy_opts(
                    &Path::from("source.bin"),
                    &Path::from("copy.bin"),
                    object_store::CopyOptions::new().with_mode(object_store::CopyMode::Create),
                )
                .await;
            stop.store(true, Ordering::Relaxed);
            server.join().unwrap();
            if success {
                assert!(result.is_ok(), "{result:?}");
            } else {
                assert!(
                    matches!(result, Err(object_store::Error::AlreadyExists { .. })),
                    "{result:?}"
                );
            }
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), if success { 3 } else { 4 });
            assert!(
                requests[0]
                    .lines()
                    .next()
                    .unwrap()
                    .contains("tenant/import/copy.bin?uploads")
            );
            assert!(requests[0].starts_with("POST "));
            assert!(requests[1].starts_with("PUT "));
            assert!(
                requests[1]
                    .lines()
                    .next()
                    .unwrap()
                    .contains("partNumber=1&uploadId=fixture-upload")
            );
            assert!(requests[2].starts_with("POST "));
            assert!(
                requests[2]
                    .lines()
                    .next()
                    .unwrap()
                    .contains("uploadId=fixture-upload")
            );
            assert!(
                requests[1]
                    .to_ascii_lowercase()
                    .contains("x-amz-copy-source: fixture/tenant/import/source.bin")
            );
            assert!(
                requests[2]
                    .to_ascii_lowercase()
                    .contains("if-none-match: *")
            );
            if !success {
                assert!(requests[3].starts_with("DELETE "));
            }
            assert_eq!(reader.transport_stats().attempts, requests.len() as u64);
        }
    }

    #[tokio::test]
    async fn publication_request_deadline_allows_progressing_whole_body() {
        for (deadline, success) in [
            (Duration::from_millis(20), false),
            (Duration::from_secs(1), true),
        ] {
            let (reader, stop, _, server) = http_fixture_with_copy_response(
                http_response("200 OK", None, "\"frozen\"", 3, b"abc"),
                None,
                Path::default(),
                None,
                deadline,
                Some(Duration::from_millis(40)),
            );
            let result = match reader.store().get(&Path::from("whole.bin")).await {
                Ok(body) => body.bytes().await,
                Err(error) => Err(error),
            };
            stop.store(true, Ordering::Relaxed);
            server.join().unwrap();
            if success {
                assert_eq!(result.unwrap().as_ref(), b"abc");
            } else {
                assert!(result.is_err());
            }
        }
        assert!(
            OneAttemptS3::new_with_prefix_and_timeout(
                "fixture",
                "eu-central-1",
                Path::default(),
                Duration::ZERO
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn physical_namespace_preserves_logical_paths_and_read_accounting() {
        let (authority, object) = short_tail_authority();
        let tail = &object[3328..];
        for prefix in ["", "tenant/import"] {
            let (reader, stop, requests, server) = http_fixture_responses_with_prefix(
                http_response("200 OK", None, "\"frozen\"", object.len(), &[]),
                Some(http_response(
                    "206 Partial Content",
                    Some("bytes 3328-3548/3549"),
                    "\"frozen\"",
                    tail.len(),
                    tail,
                )),
                Path::parse(prefix).unwrap(),
            );
            let head_location = Path::from("generation/head.bin");
            let location = Path::from("generation/plane/records.bin");
            let head = reader.store().head(&head_location).await.unwrap();
            assert_eq!(head.location, head_location);
            assert_eq!(head.size, object.len() as u64);
            assert_eq!(head.e_tag.as_deref(), Some("\"frozen\""));
            let result = reader
                .store()
                .get_opts(
                    &location,
                    GetOptions::new()
                        .with_range(Some(3328..3549))
                        .with_if_match(Some("\"frozen\"")),
                )
                .await
                .unwrap();
            assert_eq!(result.meta.location, location);
            assert_eq!(result.meta.size, object.len() as u64);
            assert_eq!(result.meta.e_tag.as_deref(), Some("\"frozen\""));
            assert_eq!(result.range, 3328..3549);
            assert_eq!(result.bytes().await.unwrap().as_ref(), tail);
            let (ranges, stats) = reader
                .fetch_verified_ranges(
                    &location,
                    &authority,
                    &[(1, 1)],
                    "\"frozen\"",
                    1,
                    tail.len(),
                    1,
                )
                .await
                .unwrap();
            assert_eq!(ranges[0].start, 3328);
            assert_eq!(ranges[0].bytes.as_ref(), tail);
            assert_eq!(
                stats,
                Sq8ReadStats {
                    submitted_gets: 1,
                    verified_bytes: tail.len(),
                    failed_gets: 0,
                }
            );
            // A source-plane authority cannot authorize SQ8 reads, even under a namespace.
            let (foreign, _) = source_tail_authority();
            let failure = reader
                .rank_verified_sq8_pages(
                    &location,
                    &foreign,
                    &[(1, 1)],
                    "\"frozen\"",
                    &[1.0; 5],
                    &[0.0; 5],
                    &[1.0; 5],
                    1,
                    1,
                    tail.len(),
                    1,
                )
                .await
                .unwrap_err();
            assert!(matches!(failure.error, RangeFetchError::UnexpectedMetadata));
            assert_eq!(failure.stats, Sq8ReadStats::default());
            stop.store(true, Ordering::Relaxed);
            server.join().unwrap();
            let physical = if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            };
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 3);
            assert_eq!(
                requests[0].lines().next().unwrap(),
                format!("HEAD /fixture/{physical}generation/head.bin HTTP/1.1")
            );
            for request in &requests[1..] {
                assert_eq!(
                    request.lines().next().unwrap(),
                    format!("GET /fixture/{physical}generation/plane/records.bin HTTP/1.1")
                );
                let headers = request.to_ascii_lowercase();
                assert!(headers.contains("range: bytes=3328-3548\r\n"));
                assert!(headers.contains("if-match: \"frozen\"\r\n"));
            }
            let transport = reader.transport_stats();
            assert_eq!(transport.attempts, 3);
            assert_eq!(transport.method_counts, [2, 1, 0, 0, 0, 0, 0, 0, 0, 0]);
            assert_eq!(transport.status_counts, vec![(200, 1), (206, 2)]);
            assert_eq!(transport.consumed_payload_bytes, 2 * tail.len() as u64);
            assert_eq!(
                transport.transport_failures
                    + transport.stream_failures
                    + transport.dropped_error_bodies,
                0
            );
        }
    }

    async fn retry_tail(
        responses: Vec<Vec<u8>>,
    ) -> (
        Result<(Vec<VerifiedRange>, Sq8ReadStats), RankedSq8Failure>,
        Vec<String>,
        NativeTransportStats,
    ) {
        let (authority, object) = short_tail_authority();
        let (reader, stop, requests, server) = read_retry_fixture(responses);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            fetch_verified_ranges_inner(
                reader.store(),
                &Path::from("sq8.bin"),
                &authority,
                &[(1, 1)],
                "\"frozen\"",
                1,
                object.len() - 3328,
                1,
            ),
        )
        .await;
        stop.store(true, Ordering::Relaxed);
        server.join().unwrap();
        let requests = requests.lock().unwrap().clone();
        (
            result.expect("bounded retry deadline"),
            requests,
            reader.transport_stats(),
        )
    }

    #[tokio::test]
    async fn read_retry_status_policy_is_explicit_and_finite() {
        let (authority, object) = short_tail_authority();
        let tail = &object[3328..];
        let error = http_response("500 Internal Server Error", None, "\"frozen\"", 0, b"");
        let good = http_response(
            "206 Partial Content",
            Some("bytes 3328-3548/3549"),
            "\"frozen\"",
            tail.len(),
            tail,
        );
        let (off, requests, stats) = request_fixture_once(&authority, error.clone()).await;
        assert!(off.is_err());
        assert_eq!(requests.len(), 1);
        assert_eq!(stats.attempts, 1);

        let (on, requests, stats) = retry_tail(vec![error.clone(), good]).await;
        let (ranges, logical) = on.unwrap();
        assert_eq!(ranges[0].bytes.as_ref(), tail);
        assert_eq!(
            logical,
            Sq8ReadStats {
                submitted_gets: 1,
                verified_bytes: tail.len(),
                failed_gets: 0,
            }
        );
        assert_eq!(requests.len(), 2);
        assert_eq!(stats.attempts, 2);
        assert_eq!(stats.status_counts, vec![(206, 1), (500, 1)]);
        assert_eq!(stats.consumed_payload_bytes, tail.len() as u64);
        assert_eq!(stats.dropped_error_bodies, 1);

        let (failed, requests, stats) = retry_tail(vec![error]).await;
        assert_eq!(
            failed.err().unwrap().stats,
            Sq8ReadStats {
                submitted_gets: 1,
                verified_bytes: 0,
                failed_gets: 1,
            }
        );
        assert_eq!(requests.len(), 2);
        assert_eq!(stats.attempts, 2);
        assert_eq!(stats.status_counts, vec![(500, 2)]);
        assert_eq!(stats.consumed_payload_bytes, 0);
        assert_eq!(stats.dropped_error_bodies, 2);
    }

    #[tokio::test]
    async fn read_retry_head_uses_the_same_bounded_status_policy() {
        let (reader, stop, requests, server) = read_retry_fixture(vec![
            http_response("500 Internal Server Error", None, "\"frozen\"", 0, b""),
            http_response("200 OK", None, "\"frozen\"", 3549, b""),
        ]);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            reader.store().head(&Path::from("sq8.bin")),
        )
        .await;
        stop.store(true, Ordering::Relaxed);
        server.join().unwrap();
        assert_eq!(result.unwrap().unwrap().size, 3549);
        assert_eq!(requests.lock().unwrap().len(), 2);
        let stats = reader.transport_stats();
        assert_eq!(stats.attempts, 2);
        assert_eq!(stats.method_counts, [0, 2, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(stats.status_counts, vec![(200, 1), (500, 1)]);
        assert_eq!(stats.consumed_payload_bytes, 0);
    }

    #[tokio::test]
    async fn read_retry_terminal_status_and_integrity_errors_do_not_retry() {
        let (_, object) = short_tail_authority();
        let tail = &object[3328..];
        let mut corrupt = tail.to_vec();
        corrupt[0] ^= 1;
        for (response, consumed) in [
            (http_response("401 Unauthorized", None, "\"frozen\"", 0, b""), 0),
            (http_response("403 Forbidden", None, "\"frozen\"", 0, b""), 0),
            (http_response("404 Not Found", None, "\"frozen\"", 0, b""), 0),
            (http_response("412 Precondition Failed", None, "\"frozen\"", 0, b""), 0),
            (b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /other\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(), 0),
            (http_response("206 Partial Content", Some("bytes 3328-3548/3549"),
                "\"foreign\"", tail.len(), tail), 0),
            (http_response("206 Partial Content", Some("bytes 0-3548/3549"),
                "\"frozen\"", object.len(), &object), 0),
            (http_response("206 Partial Content", Some("bytes 3328-3548/3549"),
                "\"frozen\"", tail.len(), &corrupt), tail.len() as u64),
        ] {
            let (result, requests, stats) = retry_tail(vec![response]).await;
            assert!(result.is_err());
            assert_eq!(requests.len(), 1);
            assert_eq!(stats.attempts, 1);
            assert_eq!(stats.consumed_payload_bytes, consumed);
        }
    }

    #[tokio::test]
    async fn read_retry_body_resume_is_exact_and_shares_attempt_limit() {
        let (_, object) = short_tail_authority();
        let tail = &object[3328..];
        let partial = http_response(
            "206 Partial Content",
            Some("bytes 3328-3548/3549"),
            "\"frozen\"",
            tail.len(),
            &tail[..100],
        );
        for mode in [
            "exact",
            "etag",
            "superset",
            "partial",
            "partial_then_status",
            "status_then_partial",
        ] {
            let resumed = match mode {
                "partial_then_status" => {
                    http_response("500 Internal Server Error", None, "\"frozen\"", 0, b"")
                }
                "etag" => http_response(
                    "206 Partial Content",
                    Some("bytes 3428-3548/3549"),
                    "\"changed\"",
                    121,
                    &tail[100..],
                ),
                "superset" => http_response(
                    "206 Partial Content",
                    Some("bytes 3328-3548/3549"),
                    "\"frozen\"",
                    tail.len(),
                    tail,
                ),
                "partial" => http_response(
                    "206 Partial Content",
                    Some("bytes 3428-3548/3549"),
                    "\"frozen\"",
                    121,
                    &tail[100..160],
                ),
                _ => http_response(
                    "206 Partial Content",
                    Some("bytes 3428-3548/3549"),
                    "\"frozen\"",
                    121,
                    &tail[100..],
                ),
            };
            let responses = if mode == "status_then_partial" {
                vec![
                    http_response("500 Internal Server Error", None, "\"frozen\"", 0, b""),
                    partial.clone(),
                ]
            } else {
                vec![partial.clone(), resumed]
            };
            let (result, requests, stats) = retry_tail(responses).await;
            assert_eq!(requests.len(), 2, "{mode}");
            assert_eq!(stats.attempts, 2, "{mode}");
            assert_eq!(
                stats.method_counts,
                [2, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                "{mode}"
            );
            if mode != "status_then_partial" {
                assert!(
                    requests[1]
                        .to_ascii_lowercase()
                        .contains("range: bytes=3428-3548"),
                    "{mode}"
                );
                assert!(
                    requests[1]
                        .to_ascii_lowercase()
                        .contains("if-match: \"frozen\""),
                    "{mode}"
                );
            }
            match mode {
                "exact" => {
                    assert_eq!(result.unwrap().0[0].bytes.as_ref(), tail);
                    assert_eq!(stats.consumed_payload_bytes, tail.len() as u64);
                    assert_eq!(stats.stream_failures, 1);
                }
                "etag" | "superset" => {
                    assert!(result.is_err(), "{mode}");
                    assert_eq!(stats.consumed_payload_bytes, 100, "{mode}");
                    assert_eq!(stats.transport_failures, 1, "{mode}");
                    assert_eq!(stats.stream_failures, 1, "{mode}");
                }
                "partial" => {
                    assert!(result.is_err());
                    assert_eq!(stats.consumed_payload_bytes, 160);
                    assert_eq!(stats.stream_failures, 2);
                }
                "partial_then_status" => {
                    assert_eq!(
                        result.err().unwrap().stats,
                        Sq8ReadStats {
                            submitted_gets: 1,
                            verified_bytes: 0,
                            failed_gets: 1,
                        }
                    );
                    assert_eq!(stats.consumed_payload_bytes, 100);
                    assert_eq!(stats.status_counts, vec![(206, 1), (500, 1)]);
                    assert_eq!(stats.stream_failures, 1);
                    assert_eq!(stats.dropped_error_bodies, 1);
                }
                _ => {
                    assert!(result.is_err());
                    assert_eq!(stats.consumed_payload_bytes, 100);
                    assert_eq!(stats.status_counts, vec![(206, 1), (500, 1)]);
                    assert_eq!(stats.stream_failures, 1);
                }
            }
        }
    }

    #[tokio::test]
    async fn read_retry_ordered_error_drains_queued_reads_and_accounts_physical_calls() {
        let object = unique_sq8_object(600);
        let authority = sq8_authority(&object, 600);
        let mut corrupt = object[3328..6656].to_vec();
        corrupt[0] ^= 1;
        let error = http_response("500 Internal Server Error", None, "\"frozen\"", 0, b"");
        let (reader, stop, requests, server) = read_retry_fixture(vec![
            error.clone(),
            error,
            http_response(
                "206 Partial Content",
                Some("bytes 3328-6655/7800"),
                "\"frozen\"",
                corrupt.len(),
                &corrupt,
            ),
            http_response(
                "206 Partial Content",
                Some("bytes 6656-7799/7800"),
                "\"frozen\"",
                1144,
                &object[6656..],
            ),
        ]);
        let window = InFlightBytes::new(3328);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            fetch_verified_ranges_with_byte_window(
                reader.store(),
                &Path::from("sq8.bin"),
                &authority,
                &[(0, 0), (1, 1), (2, 2)],
                "\"frozen\"",
                3,
                object.len(),
                1,
                &window,
            ),
        )
        .await;
        stop.store(true, Ordering::Relaxed);
        server.join().unwrap();
        let failure = result.unwrap().err().unwrap();
        assert!(matches!(failure.error, RangeFetchError::Store(_)));
        assert_eq!(
            failure.stats,
            Sq8ReadStats {
                submitted_gets: 3,
                verified_bytes: 1144,
                failed_gets: 2,
            }
        );
        assert_eq!(*window.available.lock().unwrap(), 3328);
        assert_eq!(requests.lock().unwrap().len(), 4);
        let stats = reader.transport_stats();
        assert_eq!(stats.attempts, 4);
        assert_eq!(stats.status_counts, vec![(206, 2), (500, 2)]);
        assert_eq!(stats.consumed_payload_bytes, 4472);
        assert_eq!(stats.dropped_error_bodies, 2);
    }

    #[tokio::test]
    async fn read_retry_concurrent_ordered_error_drains_queued_reads_and_restores_window() {
        use super::cold_http_fixture::{ETAG, Fixture};
        let object = unique_sq8_object(4 * 256);
        let authority = sq8_authority(&object, 4 * 256);
        let key = "gen/objects/sq8";
        let location = Path::from(key);
        let ranges = [(0, 0), (1, 1), (2, 2), (3, 3)];
        let window = InFlightBytes::new(2 * 3328);
        let fixture = Fixture::new(
            std::collections::BTreeMap::from([(key.to_owned(), object.clone())]),
            Instant::now() + Duration::from_secs(15),
        );
        let reader = fixture.retry_reader();
        fixture.arm_retry_status([0, 3328, 6656]);
        let mut trace = Sq8RangeTrace::new(4).unwrap();
        trace.begin(Instant::now());
        let mut work = Box::pin(fetch_verified_ranges_traced_with_byte_window(
            reader.store(),
            &location,
            &authority,
            &ranges,
            ETAG,
            4,
            object.len(),
            4,
            Some(&mut trace),
            Some(&window),
        ));

        // The first read is on its held second submission; the corrupt body is also active.
        tokio::select! {
            _ = &mut work => panic!("returned before overlapping retry and corrupt read"),
            () = fixture.wait(|state| {
                state.active[1] == 2
                    && state.requests.iter().filter(|request| {
                        request.2 == Some((0, 3328))
                    }).count() == 2
            }) => {}
        }
        let state = fixture.snapshot();
        assert_eq!(state.finished[1], vec![0]);
        assert_eq!(state.requests.len(), 3);
        assert_eq!(*window.available.lock().unwrap(), 0);
        assert_eq!(window.waiters.lock().unwrap().len(), 2);

        // Authentication of the later corrupt read frees a permit for the held sibling.
        fixture.release(1);
        tokio::select! {
            _ = &mut work => panic!("returned before delayed retry failure"),
            () = fixture.wait(|state| {
                state.finished[1].contains(&3328)
                    && state.active[1] == 2
                    && state.requests.iter().any(|request| request.2 == Some((6656, 9984)))
            }) => {}
        }
        assert_eq!(fixture.snapshot().finished[1], vec![0, 3328]);
        assert_eq!(window.waiters.lock().unwrap().len(), 1);

        // The terminal 500 frees the other permit: queued work drains before the sibling.
        fixture.release_error();
        tokio::select! {
            _ = &mut work => panic!("returned before held sibling drained"),
            () = fixture.wait(|state| state.finished[1].contains(&9984)) => {}
        }
        let state = fixture.snapshot();
        assert_eq!(state.finished[1], vec![0, 3328, 0, 9984]);
        assert_eq!(state.active[1], 1);
        assert!(futures_util::poll!(work.as_mut()).is_pending());
        fixture.release_sibling();
        let failure = tokio::time::timeout(Duration::from_secs(5), &mut work)
            .await
            .expect("bounded retry drain")
            .err()
            .unwrap();
        drop(work);

        assert!(matches!(failure.error, RangeFetchError::Store(_)));
        assert_eq!(
            failure.stats,
            Sq8ReadStats {
                submitted_gets: 4,
                verified_bytes: 2 * 3328,
                failed_gets: 2,
            }
        );
        let state = fixture.snapshot();
        assert_eq!(state.finished[1], vec![0, 3328, 0, 9984, 6656]);
        assert_eq!(state.active, [0; 3]);
        assert_eq!(state.peak[1], 2);
        assert!(state.errors.is_empty());
        let mut requested = state
            .requests
            .iter()
            .map(|request| request.2)
            .collect::<Vec<_>>();
        requested.sort_unstable();
        assert_eq!(
            requested,
            vec![
                Some((0, 3328)),
                Some((0, 3328)),
                Some((3328, 6656)),
                Some((6656, 9984)),
                Some((9984, 13312)),
            ]
        );
        assert_eq!(*window.available.lock().unwrap(), 2 * 3328);
        assert!(window.waiters.lock().unwrap().is_empty());
        assert_eq!(trace.outcome, "fetch_failed");
        assert_eq!(trace.ranges.len(), 4);
        assert_eq!(trace.ranges[0].outcome, "store_headers");
        assert_eq!(trace.ranges[1].outcome, "page_auth");
        assert_eq!(trace.ranges[2].outcome, "ok");
        assert_eq!(trace.ranges[3].outcome, "ok");
        for span in &trace.ranges {
            assert!(span.complete_ns.is_some());
            assert!(span.complete_ns.unwrap() <= trace.all_ranges_complete_ns.unwrap());
        }
        assert!(trace.ranges[1].complete_ns.unwrap() < trace.ranges[0].complete_ns.unwrap());
        assert!(trace.ranges[3].complete_ns.unwrap() < trace.ranges[2].complete_ns.unwrap());
        let stats = reader.transport_stats();
        assert_eq!(stats.attempts, 5);
        assert_eq!(stats.method_counts, [5, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(stats.status_counts, vec![(206, 3), (500, 2)]);
        assert_eq!(stats.consumed_payload_bytes, 3 * 3328);
        assert_eq!(stats.dropped_error_bodies, 2);
        assert_eq!(stats.transport_failures, 0);
        assert_eq!(stats.stream_failures, 0);
        fixture.finish();
    }

    #[tokio::test]
    async fn read_retry_refuses_publication_before_network_submission() {
        let (reader, stop, requests, server) = read_retry_fixture(vec![http_response(
            "500 Internal Server Error",
            None,
            "\"frozen\"",
            0,
            b"",
        )]);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            reader.store().put(
                &Path::from("never-published"),
                PutPayload::from_static(b"x"),
            ),
        )
        .await;
        stop.store(true, Ordering::Relaxed);
        server.join().unwrap();
        assert!(result.unwrap().is_err());
        assert!(requests.lock().unwrap().is_empty());
        assert_eq!(reader.transport_stats().attempts, 1);
        assert_eq!(reader.transport_stats().transport_failures, 1);
    }

    #[tokio::test]
    async fn physical_namespace_rejects_bad_range_etag_and_sha_without_retries() {
        let (authority, object) = short_tail_authority();
        let tail = &object[3328..];
        let mut corrupt = tail.to_vec();
        corrupt[0] ^= 1;
        for (range, etag, body) in [
            ("bytes 0-220/3549", "\"frozen\"", tail),
            ("bytes 3328-3548/3549", "\"foreign\"", tail),
            ("bytes 3328-3548/3549", "\"frozen\"", corrupt.as_slice()),
        ] {
            let (reader, stop, requests, server) = http_fixture_responses_with_prefix(
                http_response("206 Partial Content", Some(range), etag, body.len(), body),
                None,
                Path::parse("tenant/import").unwrap(),
            );
            let failure = reader
                .fetch_verified_ranges(
                    &Path::from("generation/sq8.bin"),
                    &authority,
                    &[(1, 1)],
                    "\"frozen\"",
                    1,
                    tail.len(),
                    1,
                )
                .await
                .err()
                .unwrap();
            stop.store(true, Ordering::Relaxed);
            server.join().unwrap();
            assert_eq!(
                failure.stats,
                Sq8ReadStats {
                    submitted_gets: 1,
                    verified_bytes: 0,
                    failed_gets: 1,
                }
            );
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0].lines().next().unwrap(),
                "GET /fixture/tenant/import/generation/sq8.bin HTTP/1.1"
            );
            let transport = reader.transport_stats();
            assert_eq!(transport.attempts, 1);
            assert_eq!(transport.method_counts, [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
            assert_eq!(transport.status_counts, vec![(206, 1)]);
            assert_eq!(
                transport.consumed_payload_bytes,
                if body == corrupt.as_slice() {
                    tail.len() as u64
                } else {
                    0
                }
            );
        }
    }

    async fn request_fixture_once(
        authority: &PageAuthority,
        response: Vec<u8>,
    ) -> (
        Result<VerifiedRange, RangeFetchError>,
        Vec<String>,
        NativeTransportStats,
    ) {
        let (reader, stop, requests, server) = http_fixture(response);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            reader.fetch_verified_pages(
                &Path::from("sq8.bin"),
                authority,
                1,
                1,
                "\"frozen\"",
                17 * 13,
            ),
        )
        .await
        .expect("fixture request timed out");
        stop.store(true, Ordering::Relaxed);
        server.join().unwrap();
        let captured = requests.lock().unwrap().clone();
        (result, captured, reader.transport_stats())
    }

    #[tokio::test]
    async fn redirects_and_unfinished_error_bodies_fail_after_one_request() {
        let (authority, _) = short_tail_authority();
        for response in [
            b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /redirected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
            // A huge advertised body that never arrives must not be collected.
            b"HTTP/1.1 412 Precondition Failed\r\nContent-Length: 1073741824\r\nConnection: close\r\n\r\n".to_vec(),
        ] {
            let (reader, stop, requests, server) = http_fixture(response);
            let failure = tokio::time::timeout(Duration::from_secs(5),
                fetch_verified_ranges_inner(reader.store(), &Path::from("sq8.bin"),
                    &authority, &[(1, 1)], "\"frozen\"", 1, 17 * 13, 1))
                .await.expect("error body must not be collected").err().unwrap();
            stop.store(true, Ordering::Relaxed);
            server.join().unwrap();
            assert_eq!(failure.stats, Sq8ReadStats {
                submitted_gets: 1, verified_bytes: 0, failed_gets: 1,
            });
            assert_eq!(requests.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn generation_publish_reload_and_http_search_fail_closed() {
        use crate::{
            two_bit_build::TwoBitGenerationBuilder,
            two_bit_generation::{TwoBitGeneration, TwoBitGenerationError, TwoBitGenerationLimits},
            two_bit_source::TwoBitSource,
            two_bit_store::{publish_two_bit_generation, read_two_bit_head},
        };
        let temp = tempfile::tempdir().unwrap();
        let raw = [2_f32, 0., 1.8, 0.8717798]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<_>>();
        let hash = |bytes: &[u8]| format!("{:x}", Sha256::digest(bytes));
        let raw_path = temp.path().join("raw");
        let sq8_path = temp.path().join("sq8");
        std::fs::write(&raw_path, &raw).unwrap();
        let normalized_path = temp.path().join("normalized");
        let normalized_sha = crate::sq8_source::normalize_source(
            &raw_path,
            &hash(&raw),
            2,
            2,
            &normalized_path,
            200000,
        )
        .unwrap();
        let order =
            crate::source_order::fit_source_order(&normalized_path, &normalized_sha, 2, 2, 1 << 20)
                .unwrap();
        let encoded = crate::sq8_source::build_sq8_source(
            &normalized_path,
            &normalized_sha,
            2,
            &order,
            &sq8_path,
            200000,
        )
        .unwrap();
        let sq8 = std::fs::read(&sq8_path).unwrap();
        let store = InMemory::new();
        let key = format!("fixture/objects/{}", hash(&sq8));
        store
            .put(&Path::from(key.clone()), PutPayload::from(sq8.clone()))
            .await
            .unwrap();
        let etag = store
            .head(&Path::from(key.clone()))
            .await
            .unwrap()
            .e_tag
            .unwrap();
        let root = temp.path().join("generation");
        let root_sha = TwoBitGenerationBuilder {
            base_epoch: 0,
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &hash(&raw),
                sq8: &sq8_path,
                sq8_sha256: &hash(&sq8),
                rows: 2,
                dimensions: 2,
            },
            generation: 1,
            low: &encoded.low,
            step: &encoded.step,
            sq8_object_key: &key,
            sq8_etag: &etag,
        }
        .build(&root, 2_000_000)
        .unwrap();
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 4_000_000,
            max_active_queries: 2,
            max_query_bytes: 28,
            max_query_gets: 1,
            max_parallel_gets: 1,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 16,
            max_query_scratch_bytes: 8192,
            already_pinned_bytes: 0,
        };
        let prefix = Path::from("index");
        let published = publish_two_bit_generation(&store, &prefix, &root, &root_sha, limits, None)
            .await
            .unwrap();
        let head = read_two_bit_head(&store, &prefix).await.unwrap().unwrap();
        assert_eq!(head.root_sha256(), published.root_sha256());
        let source_body = std::fs::read(root.join("plane/records.bin")).unwrap();
        let source_etag = store
            .head(&crate::object_native_generation::metadata_location(
                &head.metadata_prefix(),
                "plane/records.bin",
            ))
            .await
            .unwrap()
            .e_tag
            .unwrap();
        let source_response = http_response(
            "206 Partial Content",
            Some("bytes 0-17/18"),
            &source_etag,
            18,
            &source_body,
        );
        for query in [[2., 1.], [5e29, 2.5e29], [5e-31, 2.5e-31]] {
            let generation = TwoBitGeneration::open_remote(
                &store,
                &head.metadata_prefix(),
                head.root_sha256(),
                limits,
                temp.path(),
            )
            .await
            .unwrap();
            let response = http_response(
                "206 Partial Content",
                Some("bytes 0-27/28"),
                &etag,
                28,
                &sq8,
            );
            let (reader, stop, requests, server) =
                http_fixture_responses(response, Some(source_response.clone()));
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                generation.search(&reader, &query, 1),
            )
            .await
            .unwrap();
            stop.store(true, Ordering::Relaxed);
            server.join().unwrap();
            let result = result.unwrap();
            assert_eq!(result.ranked.candidates[0].id, 1);
            assert_eq!(result.plan.planned_bytes, 28);
            assert_eq!(
                result.ranked.stats,
                Sq8ReadStats {
                    submitted_gets: 1,
                    verified_bytes: 28,
                    failed_gets: 0
                }
            );
            {
                let captured = requests.lock().unwrap();
                assert_eq!(captured.len(), 2);
                let source_request = captured[0].to_ascii_lowercase();
                assert!(source_request.contains("/plane/records.bin "));
                assert!(source_request.contains("range: bytes=0-17\r\n"));
                assert!(source_request.contains(&format!(
                    "if-match: {}\r\n",
                    source_etag.to_ascii_lowercase()
                )));
                let request = captured[1].to_ascii_lowercase();
                assert!(request.starts_with(&format!("get /fixture/{key} ")));
                assert!(request.contains("range: bytes=0-27\r\n"));
                assert!(request.contains(&format!("if-match: {}\r\n", etag.to_ascii_lowercase())));
                assert!(request.contains("authorization: aws4-hmac-sha256 "));
            }
            let logical = crate::two_bit_index::TwoBitIndex::open_remote(
                &store,
                read_two_bit_head(&store, &prefix).await.unwrap().unwrap(),
                limits,
                temp.path(),
            )
            .await
            .unwrap();
            let (reader, stop, requests, server) = http_fixture_responses(
                http_response(
                    "206 Partial Content",
                    Some("bytes 0-27/28"),
                    &etag,
                    28,
                    &sq8,
                ),
                Some(source_response.clone()),
            );
            let logical = tokio::time::timeout(
                Duration::from_secs(5),
                logical.search(&reader, &query, usize::MAX, None),
            )
            .await
            .unwrap()
            .unwrap();
            stop.store(true, Ordering::Relaxed);
            server.join().unwrap();
            assert_eq!(logical.candidates.len(), 2);
            assert_eq!(logical.candidates[0].id, 1);
            assert_eq!(logical.stats.submitted_gets, 1);
            assert_eq!(logical.stats.verified_bytes, 28);
            assert_eq!(requests.lock().unwrap().len(), 2);
        }
        let generation = TwoBitGeneration::open_remote(
            &store,
            &head.metadata_prefix(),
            head.root_sha256(),
            limits,
            temp.path(),
        )
        .await
        .unwrap();
        use crate::two_bit_mutations::{
            TwoBitMutation, TwoBitMutationLimits, apply_two_bit_mutations, read_two_bit_mutations,
        };
        let mutation_caps = TwoBitMutationLimits {
            max_snapshot_bytes: 16384,
            max_memory_bytes: 1_000_000,
        };
        let first = apply_two_bit_mutations(
            &store,
            &head,
            2,
            None,
            &[
                TwoBitMutation {
                    id: 0,
                    vector: Some(vec![-1., 0.]),
                },
                TwoBitMutation {
                    id: 1,
                    vector: None,
                },
                TwoBitMutation {
                    id: 7,
                    vector: Some(vec![2., 1.]),
                },
            ],
            mutation_caps,
        )
        .await
        .unwrap();
        let recovered = read_two_bit_mutations(&store, &head, 2, mutation_caps)
            .await
            .unwrap()
            .unwrap();
        let charged = TwoBitGeneration::open_remote(
            &store,
            &head.metadata_prefix(),
            head.root_sha256(),
            TwoBitGenerationLimits {
                already_pinned_bytes: 4 * recovered.resident_payload_bytes() as u64,
                ..limits
            },
            temp.path(),
        )
        .await
        .unwrap();
        let other_root = temp.path().join("other-generation");
        let other_sha = TwoBitGenerationBuilder {
            base_epoch: 0,
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &hash(&raw),
                sq8: &sq8_path,
                sq8_sha256: &hash(&sq8),
                rows: 2,
                dimensions: 2,
            },
            generation: 2,
            low: &encoded.low,
            step: &encoded.step,
            sq8_object_key: &key,
            sq8_etag: &etag,
        }
        .build(&other_root, 2_000_000)
        .unwrap();
        let other_generation = TwoBitGeneration::open(
            &other_root,
            &other_sha,
            TwoBitGenerationLimits {
                already_pinned_bytes: 4 * recovered.resident_payload_bytes() as u64,
                ..limits
            },
        )
        .unwrap();
        let response = http_response(
            "206 Partial Content",
            Some("bytes 0-27/28"),
            &etag,
            28,
            &sq8,
        );
        let (reader, stop, requests, server) =
            http_fixture_responses(response, Some(source_response.clone()));
        assert!(matches!(
            generation
                .search_with_mutations(&reader, &[2., 1.], 3, &recovered)
                .await,
            Err(TwoBitGenerationError::Invalid(
                "mutation binding or admission"
            ))
        ));
        assert!(matches!(
            other_generation
                .search_with_mutations(&reader, &[2., 1.], 3, &recovered)
                .await,
            Err(TwoBitGenerationError::Invalid(
                "mutation binding or admission"
            ))
        ));
        assert!(requests.lock().unwrap().is_empty());
        let hits = charged
            .search_with_mutations(&reader, &[2., 1.], usize::MAX, &recovered)
            .await
            .unwrap();
        assert_eq!(
            hits.candidates.iter().map(|h| h.id).collect::<Vec<_>>(),
            vec![7, 0]
        );
        assert!(hits.candidates[0].score.abs() < 1e-6);
        assert!(
            hits.candidates.capacity() <= 4,
            "large k must not allocate a large heap"
        );
        assert_eq!(hits.mutation_revision, first.revision());
        assert_eq!(hits.mutation_sha256, first.sha256());
        assert_eq!(hits.mutation_rows_scanned, 3);
        assert_eq!(hits.mutation_put_rows_scored, 2);
        assert_eq!(
            hits.stats,
            Sq8ReadStats {
                submitted_gets: 1,
                verified_bytes: 28,
                failed_gets: 0
            }
        );
        let second = apply_two_bit_mutations(
            &store,
            &head,
            2,
            Some(&recovered),
            &[
                TwoBitMutation {
                    id: 0,
                    vector: Some(vec![1., 0.]),
                },
                TwoBitMutation {
                    id: 7,
                    vector: None,
                },
            ],
            mutation_caps,
        )
        .await
        .unwrap();
        let restarted = read_two_bit_mutations(&store, &head, 2, mutation_caps)
            .await
            .unwrap()
            .unwrap();
        let after = charged
            .search_with_mutations(&reader, &[1., 0.], 2, &restarted)
            .await
            .unwrap();
        assert_eq!(
            after.candidates.iter().map(|h| h.id).collect::<Vec<_>>(),
            vec![0]
        );
        assert_eq!(after.mutation_revision, second.revision());
        let pinned = charged
            .search_with_mutations(&reader, &[2., 1.], 1, &first)
            .await
            .unwrap();
        assert_eq!(pinned.candidates, vec![hits.candidates[0]]);
        stop.store(true, Ordering::Relaxed);
        server.join().unwrap();
        assert_eq!(
            requests.lock().unwrap().len(),
            6,
            "one source and one SQ8 GET per query, none for admission failure"
        );
        let logical = crate::two_bit_index::TwoBitIndex::open_remote(
            &store,
            read_two_bit_head(&store, &prefix).await.unwrap().unwrap(),
            TwoBitGenerationLimits {
                already_pinned_bytes: 4 * recovered.resident_payload_bytes() as u64,
                ..limits
            },
            temp.path(),
        )
        .await
        .unwrap();
        let (reader, stop, requests, server) = http_fixture_responses(
            http_response(
                "206 Partial Content",
                Some("bytes 0-27/28"),
                &etag,
                28,
                &sq8,
            ),
            Some(source_response.clone()),
        );
        let wrapped = logical
            .search(&reader, &[2., 1.], usize::MAX, Some(&recovered))
            .await
            .unwrap();
        stop.store(true, Ordering::Relaxed);
        server.join().unwrap();
        assert_eq!(wrapped.candidates, hits.candidates);
        assert_eq!(wrapped.stats.submitted_gets, 1);
        assert_eq!(requests.lock().unwrap().len(), 2);
        let mut corrupted = sq8.clone();
        corrupted[12] ^= 1;
        for response in [
            http_response(
                "206 Partial Content",
                Some("bytes 0-27/28"),
                &etag,
                28,
                &corrupted,
            ),
            http_response(
                "206 Partial Content",
                Some("bytes 0-27/28"),
                "changed",
                28,
                &sq8,
            ),
            http_response("412 Precondition Failed", None, "changed", 0, &[]),
        ] {
            let (reader, stop, requests, server) =
                http_fixture_responses(response, Some(source_response.clone()));
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                generation.search(&reader, &[2., 1.], 1),
            )
            .await
            .unwrap();
            stop.store(true, Ordering::Relaxed);
            server.join().unwrap();
            let failure = result.err().expect("bad data must be rejected");
            assert!(failure.stages().is_some());
            let (source, sq8) = failure.read_stats();
            assert_eq!(
                sq8,
                Sq8ReadStats {
                    submitted_gets: 1,
                    verified_bytes: 0,
                    failed_gets: 1
                }
            );
            assert_eq!(
                source,
                Sq8ReadStats {
                    submitted_gets: 1,
                    verified_bytes: 18,
                    failed_gets: 0
                }
            );
            assert_eq!(requests.lock().unwrap().len(), 2, "hidden retry");
        }
    }

    #[tokio::test]
    async fn real_http_range_faults_fail_closed_without_hidden_retries() {
        let (authority, object) = short_tail_authority();
        let tail = &object[256 * 13..];
        let correct_range = "bytes 3328-3548/3549";
        let good = http_response(
            "206 Partial Content",
            Some(correct_range),
            "\"frozen\"",
            tail.len(),
            tail,
        );
        let (fetched, requests, stats) = request_fixture_once(&authority, good).await;
        assert_eq!(stats.attempts, 1);
        assert_eq!(stats.method_counts, [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(stats.status_counts, vec![(206, 1)]);
        assert_eq!(stats.consumed_payload_bytes, tail.len() as u64);
        assert_eq!(stats.transport_failures, 0);
        assert_eq!(stats.stream_failures, 0);
        assert_eq!(stats.dropped_error_bodies, 0);
        assert_eq!(fetched.unwrap().bytes.as_ref(), tail);
        assert_eq!(requests.len(), 1);
        let request = requests[0].to_ascii_lowercase();
        assert!(request.contains("range: bytes=3328-3548\r\n"));
        assert!(request.contains("if-match: \"frozen\"\r\n"));

        let mut changed = tail.to_vec();
        changed[0] ^= 1;
        let faulty = [
            http_response(
                "200 OK",
                Some(correct_range),
                "\"frozen\"",
                tail.len(),
                tail,
            ),
            http_response(
                "206 Partial Content",
                Some("bytes 0-220/3549"),
                "\"frozen\"",
                tail.len(),
                tail,
            ),
            http_response(
                "206 Partial Content",
                Some(correct_range),
                "\"changed\"",
                tail.len(),
                tail,
            ),
            http_response(
                "206 Partial Content",
                Some(correct_range),
                "\"frozen\"",
                tail.len(),
                &changed,
            ),
            http_response(
                "206 Partial Content",
                Some(correct_range),
                "\"frozen\"",
                tail.len(),
                &tail[..tail.len() - 1],
            ),
            http_response(
                "412 Precondition Failed",
                None,
                "\"changed\"",
                7,
                b"unread!",
            ),
            http_response(
                "500 Internal Server Error",
                None,
                "\"frozen\"",
                7,
                b"unread!",
            ),
            Vec::new(), // Closed connection before headers: one transport failure.
        ];
        for (case, response) in faulty.into_iter().enumerate() {
            let (result, requests, stats) = request_fixture_once(&authority, response).await;
            assert!(result.is_err());
            assert_eq!(requests.len(), 1, "unexpected hidden HTTP retry");
            assert_eq!(stats.attempts, 1);
            assert_eq!(stats.method_counts, [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
            let status = match case {
                0 => 200,
                5 => 412,
                6 => 500,
                _ => 206,
            };
            assert_eq!(
                stats.status_counts,
                if case == 7 { vec![] } else { vec![(status, 1)] }
            );
            assert_eq!(stats.transport_failures, u64::from(case == 7));
            assert_eq!(stats.stream_failures, u64::from(case == 4));
            assert_eq!(stats.dropped_error_bodies, u64::from(matches!(case, 5 | 6)));
            match case {
                3 => assert_eq!(stats.consumed_payload_bytes, tail.len() as u64),
                // A truncated chunk can be delivered or rejected whole by HTTP framing.
                4 => assert!(stats.consumed_payload_bytes < tail.len() as u64),
                _ => assert_eq!(stats.consumed_payload_bytes, 0),
            }
        }

        let (reader, stop, requests, server) = http_fixture(http_response(
            "206 Partial Content",
            Some(correct_range),
            "\"frozen\"",
            tail.len(),
            tail,
        ));
        let location = Path::from("sq8.bin");
        // Receiving headers then cancelling consumption must not charge Content-Length.
        let unread = reader
            .store()
            .get_opts(&location, GetOptions::new().with_range(Some(3328..3549)))
            .await
            .unwrap();
        assert_eq!(reader.transport_stats().consumed_payload_bytes, 0);
        drop(unread);
        let fetch =
            || reader.fetch_verified_pages(&location, &authority, 1, 1, "\"frozen\"", tail.len());
        let (first, second, head) = tokio::join!(fetch(), fetch(), reader.store().head(&location));
        assert!(first.is_ok() && second.is_ok() && head.is_ok());
        stop.store(true, Ordering::Relaxed);
        server.join().unwrap();
        let stats = reader.transport_stats();
        assert_eq!(requests.lock().unwrap().len(), 4);
        assert_eq!(stats.attempts, 4);
        assert_eq!(stats.method_counts, [3, 1, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(stats.status_counts, vec![(206, 4)]);
        assert_eq!(stats.consumed_payload_bytes, 2 * tail.len() as u64);
        assert_eq!(
            stats.transport_failures + stats.stream_failures + stats.dropped_error_bodies,
            0
        );
    }

    #[tokio::test]
    async fn conditional_short_tail_rejects_mutated_object() {
        let store = InMemory::new();
        let location = Path::from("sq8.bin");
        let object = vec![7u8; 273 * 13];
        store
            .put(&location, PutPayload::from(object.clone()))
            .await
            .unwrap();
        let etag = store.head(&location).await.unwrap().e_tag.unwrap();
        let sidecar = object
            .chunks(256 * 13)
            .flat_map(|page| Sha256::digest(page).to_vec())
            .collect::<Vec<_>>();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":1,
            "rows":273, "dimensions":1, "page_rows":256,
            "object_sha256":format!("{:x}", Sha256::digest(&object)),
            "page_digest_sha256":format!("{:x}", Sha256::digest(&sidecar)),
        }))
        .unwrap();
        let authority = PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .unwrap();
        let fetched =
            fetch_verified_pages_inner(&store, &location, &authority, 1, 1, &etag, 17 * 13)
                .await
                .unwrap();
        assert_eq!(fetched.start, 256 * 13);
        assert_eq!(fetched.bytes.as_ref(), &object[256 * 13..]);
        assert!(matches!(
            fetch_verified_pages_inner(&store, &location, &authority, 1, 1, &etag, 17 * 13 - 1)
                .await,
            Err(RangeFetchError::UnexpectedMetadata)
        ));
        let changed = vec![8u8; object.len()];
        store
            .put(&location, PutPayload::from(changed))
            .await
            .unwrap();
        assert!(matches!(
            fetch_verified_pages_inner(&store, &location, &authority, 1, 1, &etag, 17 * 13).await,
            Err(RangeFetchError::Store(_))
        ));
    }

    fn split_object(rows: usize, dimensions: usize) -> (Vec<u8>, PageAuthority) {
        let mut object = Vec::new();
        for row in 0..rows {
            object.extend_from_slice(&(row as i64).to_le_bytes());
            object.extend_from_slice(&(0.25 + row as f32 / rows as f32).to_le_bytes());
            object.extend((0..dimensions).map(|dim| ((row * 17 + dim * 13) % 251) as u8));
        }
        let sidecar = object
            .chunks(256 * (dimensions + 12))
            .flat_map(|page| Sha256::digest(page).to_vec())
            .collect::<Vec<_>>();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":7,
            "rows":rows,"dimensions":dimensions,"page_rows":256,
            "object_sha256":format!("{:x}", Sha256::digest(&object)),
            "page_digest_sha256":format!("{:x}", Sha256::digest(&sidecar)),
        }))
        .unwrap();
        let authority = PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .unwrap();
        (object, authority)
    }

    #[test]
    fn split_plan_exact_coverage_tail_caps_and_overflow() {
        for dimensions in [1, 7, 33, 1024] {
            let (object, authority) = split_object(4 * 256 + 19, dimensions);
            let block = 256 * (dimensions + 12);
            for cap in [block, 2 * block, 3 * block + 7, object.len()] {
                let split = Sq8RangeSplit {
                    max_physical_range_bytes: cap,
                    max_physical_gets: 5,
                };
                let mut pieces = Vec::new();
                let count = split
                    .visit(&authority, &[(0, 4)], 1, object.len(), |r| pieces.push(r))
                    .unwrap();
                assert_eq!(count, pieces.len());
                let mut end = 0;
                let mut covered = Vec::new();
                for (first, last) in pieces {
                    let interval = authority.byte_range(first, last).unwrap();
                    assert_eq!(interval.start, end);
                    assert!(interval.len() <= cap);
                    end = interval.end;
                    covered.extend_from_slice(&object[interval]);
                }
                assert_eq!(end, object.len());
                assert_eq!(covered, object);
            }
            let split = Sq8RangeSplit {
                max_physical_range_bytes: block,
                max_physical_gets: 3,
            };
            assert!(
                split
                    .visit(&authority, &[(0, 4)], 1, object.len(), |_| {})
                    .is_err()
            );
            assert!(
                split
                    .visit(&authority, &[(0, 1), (1, 1)], 2, object.len(), |_| {})
                    .is_err()
            );
            assert!(
                split
                    .visit(&authority, &[(0, usize::MAX)], 1, usize::MAX, |_| {})
                    .is_err()
            );
            assert!(
                split
                    .visit(&authority, &[(0, 1)], 1, block, |_| {})
                    .is_err()
            );
            let tail = Sq8RangeSplit {
                max_physical_range_bytes: 19 * (dimensions + 12),
                ..split
            };
            assert_eq!(
                tail.visit(&authority, &[(4, 4)], 1, object.len(), |_| {})
                    .unwrap(),
                1
            );
            assert!(
                tail.visit(&authority, &[(0, 0)], 1, object.len(), |_| {})
                    .is_err()
            );
            let mut pieces = Vec::new();
            split
                .visit(
                    &authority,
                    &[(0, 0), (2, 2), (4, 4)],
                    3,
                    object.len(),
                    |r| pieces.push(r),
                )
                .unwrap();
            assert_eq!(pieces, vec![(0, 0), (2, 2), (4, 4)]);
            assert!(
                split
                    .visit(&authority, &pieces, 2, object.len(), |_| {})
                    .is_err()
            );
        }
        let object = unique_sq8_object(1);
        let sidecar = Sha256::digest(&object).to_vec();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":1,
            "rows":1,"dimensions":1,"page_rows":usize::MAX,
            "object_sha256":format!("{:x}", Sha256::digest(&object)),
            "page_digest_sha256":format!("{:x}", Sha256::digest(&sidecar)),
        }))
        .unwrap();
        let overflow = PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .unwrap();
        assert!(
            Sq8RangeSplit {
                max_physical_range_bytes: 13,
                max_physical_gets: 1
            }
            .visit(&overflow, &[(0, 0)], 1, 13, |_| {})
            .is_err()
        );
        for split in [
            Sq8RangeSplit {
                max_physical_range_bytes: 0,
                max_physical_gets: 1,
            },
            Sq8RangeSplit {
                max_physical_range_bytes: usize::MAX,
                max_physical_gets: 1,
            },
            Sq8RangeSplit {
                max_physical_range_bytes: 1,
                max_physical_gets: usize::MAX,
            },
            Sq8RangeSplit {
                max_physical_range_bytes: 1,
                max_physical_gets: 0,
            },
        ] {
            assert!(split.scratch_bytes(usize::MAX, 1).is_none());
        }
    }

    #[tokio::test]
    async fn split_unsplit_score_bits_ordinals_exclusions_and_charges() {
        for dimensions in [1, 7, 16, 33, 1024] {
            // Both full blocks and a kernel tail; logical gaps and a bridged range.
            let (object, authority) = split_object(4 * 256 + 19, dimensions);
            let store = InMemory::new();
            let location = Path::from("sq8");
            let etag = store
                .put(&location, PutPayload::from(object.clone()))
                .await
                .unwrap()
                .e_tag
                .unwrap();
            let query = vec![0.1; dimensions];
            let low = vec![-0.3; dimensions];
            let step = vec![0.02; dimensions];
            for excluded in [&[][..], &[3, 7, 257, 1035][..]] {
                for ranges in [&[(0, 4)][..], &[(0, 1), (3, 4)][..]] {
                    let plain = rank_verified_sq8_pages_traced_with_byte_window(
                        &store,
                        &location,
                        &authority,
                        ranges,
                        &etag,
                        &query,
                        &low,
                        &step,
                        10,
                        2,
                        object.len(),
                        3,
                        excluded,
                        None,
                        None,
                    )
                    .await
                    .unwrap();
                    for blocks in [1, 2, 3] {
                        let split = Sq8RangeSplit {
                            max_physical_range_bytes: blocks * 256 * (dimensions + 12),
                            max_physical_gets: 5,
                        };
                        let mut trace = Sq8RangeTrace::new(5).unwrap();
                        let ranked = rank_verified_sq8_pages_traced_with_split(
                            &store,
                            &location,
                            &authority,
                            ranges,
                            &etag,
                            &query,
                            &low,
                            &step,
                            10,
                            2,
                            object.len(),
                            3,
                            excluded,
                            Some(&mut trace),
                            None,
                            Some(split),
                        )
                        .await
                        .unwrap();
                        let bits = |ranked: &RankedSq8| {
                            ranked
                                .candidates
                                .iter()
                                .map(|hit| (hit.id, hit.ordinal, hit.score.to_bits()))
                                .collect::<Vec<_>>()
                        };
                        assert_eq!(bits(&plain), bits(&ranked));
                        assert_eq!(plain.stats.verified_bytes, ranked.stats.verified_bytes);
                        assert_eq!(ranked.stats.failed_gets, 0);
                        let physical = split
                            .visit(&authority, ranges, 2, object.len(), |_| {})
                            .unwrap();
                        assert_eq!(ranked.stats.submitted_gets, physical);
                        assert_eq!(trace.ranges().len(), physical);
                        assert_eq!(trace.rank.unwrap().ranges as usize, ranges.len());
                        let mut spans = trace.ranges().iter();
                        for &(first, last) in ranges {
                            let logical = authority.byte_range(first, last).unwrap();
                            let mut end = logical.start;
                            while end < logical.end {
                                let span = spans.next().unwrap();
                                assert_eq!(span.start_byte as usize, end);
                                assert!(span.end_byte as usize > end);
                                assert!(span.end_byte as usize <= logical.end);
                                assert!(
                                    span.end_byte as usize - end <= split.max_physical_range_bytes
                                );
                                end = span.end_byte as usize;
                            }
                            assert_eq!(end, logical.end);
                        }
                        assert!(spans.next().is_none());
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn split_http_count_etag_auth_ordered_error_and_drain() {
        use super::cold_http_fixture::{ETAG, Fixture};
        let object = unique_sq8_object(600);
        let authority = sq8_authority(&object, 600);
        let key = "gen/objects/sq8";
        let location = Path::from(key);
        let fixture = Fixture::new(
            std::collections::BTreeMap::from([(key.into(), object.clone())]),
            Instant::now() + Duration::from_secs(30),
        );
        let split = Sq8RangeSplit {
            max_physical_range_bytes: 256 * 13,
            max_physical_gets: 3,
        };
        let mut trace = Sq8RangeTrace::new(3).unwrap();
        let window = InFlightBytes::new(split.max_physical_range_bytes);
        let before = fixture.reader.transport_stats().attempts;
        let result = rank_verified_sq8_pages_traced_with_split(
            fixture.reader.store(),
            &location,
            &authority,
            &[(0, 2)],
            ETAG,
            &[1.],
            &[0.],
            &[1.],
            10,
            1,
            object.len(),
            3,
            &[],
            Some(&mut trace),
            Some(&window),
            Some(split),
        )
        .await
        .unwrap();
        assert_eq!(result.stats.submitted_gets, 3);
        assert_eq!(result.stats.verified_bytes, object.len());
        assert_eq!(fixture.reader.transport_stats().attempts - before, 3);
        assert_eq!(
            *window.available.lock().unwrap(),
            split.max_physical_range_bytes
        );
        assert_eq!(trace.ranges().len(), result.stats.submitted_gets);
        let expected = vec![
            (0, 256 * 13),
            (256 * 13, 512 * 13),
            (512 * 13, object.len()),
        ];
        assert_eq!(
            trace
                .ranges()
                .iter()
                .map(|span| (span.start_byte as usize, span.end_byte as usize))
                .collect::<Vec<_>>(),
            expected
        );
        let requests = fixture.snapshot().requests;
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|(_, head, range, etag)| !head
            && range.is_some()
            && etag.as_deref() == Some(ETAG)));
        let mut requested = requests
            .iter()
            .map(|(_, _, range, _)| range.unwrap())
            .collect::<Vec<_>>();
        requested.sort_unstable();
        assert_eq!(requested, expected);
        fixture.arm(Some((1, [0, 256 * 13, 512 * 13])));
        trace.begin(Instant::now());
        let work = rank_verified_sq8_pages_traced_with_split(
            fixture.reader.store(),
            &location,
            &authority,
            &[(0, 2)],
            ETAG,
            &[1.],
            &[0.],
            &[1.],
            10,
            1,
            object.len(),
            3,
            &[],
            Some(&mut trace),
            None,
            Some(split),
        );
        let (failure, ()) = tokio::join!(work, async {
            fixture
                .wait(|state| state.active[1] == 2 && state.finished[1].contains(&(256 * 13)))
                .await;
            fixture.release_error();
            fixture.wait(|state| state.finished[1].contains(&0)).await;
            assert_eq!(fixture.snapshot().active[1], 1);
            fixture.release_sibling();
        });
        let failure = failure.unwrap_err();
        assert!(matches!(failure.error, RangeFetchError::Store(_)));
        assert_eq!(
            failure.stats,
            Sq8ReadStats {
                submitted_gets: 3,
                failed_gets: 2,
                verified_bytes: 88 * 13
            }
        );
        assert_eq!(trace.ranges()[1].outcome, "page_auth");
        assert_eq!(fixture.snapshot().finished[1], vec![256 * 13, 0, 512 * 13]);
        assert!(trace.rank_start_ns.is_none());
        assert!(trace.all_ranges_complete_ns.is_some());
        fixture.finish();
    }

    #[tokio::test]
    async fn split_http_wrong_etag_fails_closed_and_drains_every_get() {
        let object = unique_sq8_object(600);
        let authority = sq8_authority(&object, 600);
        let response = http_response(
            "206 Partial Content",
            Some("bytes 0-3327/7800"),
            "\"changed\"",
            3328,
            &object[..3328],
        );
        let (reader, stop, requests, server) = http_fixture(response);
        let result = rank_verified_sq8_pages_traced_with_split(
            reader.store(),
            &Path::from("sq8"),
            &authority,
            &[(0, 2)],
            "\"frozen\"",
            &[1.],
            &[0.],
            &[1.],
            10,
            1,
            object.len(),
            2, // Three failing pieces exceed the scheduler slots; queued work must still drain.
            &[],
            None,
            None,
            Some(Sq8RangeSplit {
                max_physical_range_bytes: 3328,
                max_physical_gets: 3,
            }),
        )
        .await;
        stop.store(true, Ordering::Relaxed);
        server.join().unwrap();
        let failure = result.unwrap_err();
        assert!(matches!(failure.error, RangeFetchError::UnexpectedMetadata));
        assert_eq!(
            failure.stats,
            Sq8ReadStats {
                submitted_gets: 3,
                failed_gets: 3,
                verified_bytes: 0
            }
        );
        assert_eq!(reader.transport_stats().attempts, 3);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|request| {
            request
                .to_ascii_lowercase()
                .contains("if-match: \"frozen\"\r\n")
        }));
    }

    #[tokio::test]
    async fn split_refuses_caps_trace_and_window_before_http() {
        use super::cold_http_fixture::{ETAG, Fixture};
        let object = unique_sq8_object(600);
        let authority = sq8_authority(&object, 600);
        let key = "gen/objects/sq8";
        let fixture = Fixture::new(
            std::collections::BTreeMap::from([(key.into(), object.clone())]),
            Instant::now() + Duration::from_secs(10),
        );
        let location = Path::from(key);
        for case in 0..7 {
            let split = Sq8RangeSplit {
                max_physical_range_bytes: if case == 0 { 0 } else { 256 * 13 },
                max_physical_gets: if case == 1 {
                    2
                } else if case == 6 {
                    usize::MAX
                } else {
                    3
                },
            };
            let mut trace = Sq8RangeTrace::new(if case == 2 { 2 } else { 3 }).unwrap();
            let window = InFlightBytes::new(if case == 3 { 1 } else { object.len() });
            let ranges = if case == 5 {
                &[(0, 0), (2, 2)][..]
            } else {
                &[(0, 2)][..]
            };
            let result = rank_verified_sq8_pages_traced_with_split(
                fixture.reader.store(),
                &location,
                &authority,
                ranges,
                ETAG,
                &[1.],
                &[0.],
                &[1.],
                10,
                1,
                if case == 4 { 1 } else { object.len() },
                3,
                &[],
                Some(&mut trace),
                Some(&window),
                Some(split),
            )
            .await
            .unwrap_err();
            assert_eq!(result.stats, Sq8ReadStats::default());
            assert_eq!(trace.outcome, "rejected_before_io");
            assert!(fixture.snapshot().requests.is_empty());
            assert_eq!(fixture.reader.transport_stats().attempts, 0);
        }
        fixture.finish();
    }

    fn unique_sq8_object(rows: usize) -> Vec<u8> {
        let mut object = Vec::new();
        for id in 0..rows as i64 {
            object.extend_from_slice(&id.to_le_bytes());
            object.extend_from_slice(&(id as f32).to_le_bytes());
            object.push(0);
        }
        object
    }

    fn sq8_authority(object: &[u8], rows: usize) -> PageAuthority {
        let sidecar = object
            .chunks(256 * 13)
            .flat_map(|page| Sha256::digest(page).to_vec())
            .collect::<Vec<_>>();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":1,
            "rows":rows, "dimensions":1, "page_rows":256,
            "object_sha256":format!("{:x}", Sha256::digest(object)),
            "page_digest_sha256":format!("{:x}", Sha256::digest(&sidecar)),
        }))
        .unwrap();
        PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .unwrap()
    }

    #[test]
    fn traced_range_query_future_is_send() {
        fn require_send<T: Send>(_: T) {}
        let store = InMemory::new();
        let location = Path::from("sq8.bin");
        let (authority, _) = short_tail_authority();
        let mut trace = Sq8RangeTrace::new(1).unwrap();
        require_send(rank_verified_sq8_pages_traced(
            &store,
            &location,
            &authority,
            &[(0, 0)],
            "etag",
            &[1.0],
            &[0.0],
            &[1.0],
            1,
            1,
            3328,
            1,
            &[],
            Some(&mut trace),
        ));
    }

    #[test]
    fn range_future_and_outcome_layout_stay_within_the_modeled_allowance() {
        use std::mem::{size_of, size_of_val};
        let store = InMemory::new();
        let location = Path::from("sq8.bin");
        let (authority, _) = short_tail_authority();
        // The exact element type the scheduler holds in production, the body it wraps with no
        // probe, and the plain entry point used outside batches. Unpolled futures report
        // their whole state, so this is the full per-range footprint.
        let budget = InFlightBytes::new(3328);
        let production = fetch_verified_pages_admitted_indexed(
            &store,
            &location,
            &authority,
            0,
            0,
            "etag",
            3328,
            0,
            None,
            Some(&budget),
        );
        let body =
            fetch_verified_pages_core(&store, &location, &authority, 0, 0, "etag", 3328, None);
        let plain = fetch_verified_pages_inner(&store, &location, &authority, 0, 0, "etag", 3328);
        let (production_bytes, body_bytes, plain_bytes) = (
            size_of_val(&production),
            size_of_val(&body),
            size_of_val(&plain),
        );
        let outcome = size_of::<IndexedRangeOutcome>();
        let result = size_of::<Result<VerifiedRange, RangeFetchError>>();
        // Run with --nocapture to record the measured layout next to the preregistered allowances.
        println!(
            "range_layout production_future={production_bytes} body_future={body_bytes} plain_future={plain_bytes} \
             node_allowance={BUFFERED_NODE_ALLOWANCE_BYTES} alloc_allowance={ALLOCATION_ALLOWANCE_BYTES} \
             per_range_allowance={RANGE_FUTURE_ALLOWANCE_BYTES} outcome={outcome} result={result} \
             range_span={} probe={} trace_struct={} probe_slot={}",
            size_of::<RangeSpan>(),
            size_of::<Probe>(),
            size_of::<Sq8RangeTrace>(),
            size_of::<Option<Box<Probe>>>(),
        );
        // FAILS if the compiled instrumentable future, plus its buffered node and allocation
        // header, outgrows the preregistered per-range allowance that admission charges.
        assert!(
            production_bytes + BUFFERED_NODE_ALLOWANCE_BYTES + ALLOCATION_ALLOWANCE_BYTES
                <= RANGE_FUTURE_ALLOWANCE_BYTES,
            "production future {production_bytes} body {body_bytes} plain {plain_bytes}"
        );
        // Instrumentation and indexing stay within the charged future allowance.
        assert!(body_bytes <= production_bytes);
        assert!(
            production_bytes - body_bytes <= 512,
            "{production_bytes} {body_bytes}"
        );
        // With tracing off the probe slot, its borrow and the returned record are one pointer each,
        // and an outcome adds the probe pointer and plan index to the bare result.
        assert_eq!(size_of::<Option<Box<Probe>>>(), size_of::<usize>());
        assert_eq!(size_of::<Option<&mut Probe>>(), size_of::<usize>());
        assert!(outcome <= result + 3 * size_of::<usize>());
        // Retained trace bytes, cumulative drain peak and the on/off-path state are distinct figures.
        let (capacity, parallel) = (32, 16);
        let retained = Sq8RangeTrace::retained_bytes(capacity);
        let peak = Sq8RangeTrace::modeled_bytes(capacity, parallel);
        let both_paths = Sq8RangeTrace::pending_futures_bytes(capacity, parallel);
        assert_eq!(
            retained,
            capacity * size_of::<RangeSpan>() + size_of::<Sq8RangeTrace>()
        );
        assert_eq!(
            peak,
            retained + capacity * (size_of::<Probe>() + ALLOCATION_ALLOWANCE_BYTES) + both_paths
        );
        assert!(
            both_paths
                >= parallel * RANGE_FUTURE_ALLOWANCE_BYTES
                    + parallel * (outcome + 8)
                    + OUTCOME_VECTOR_GROWTH_FACTOR * capacity * outcome
        );
        assert!(retained < peak && both_paths < peak);
        // Parallelism above the range count never charges futures that cannot exist.
        assert_eq!(
            Sq8RangeTrace::pending_futures_bytes(4, 64),
            Sq8RangeTrace::pending_futures_bytes(4, 4)
        );
        assert_eq!(
            Sq8RangeTrace::checked_pending_futures_bytes(capacity, parallel),
            Some(both_paths)
        );
        assert_eq!(
            Sq8RangeTrace::checked_pending_futures_bytes(usize::MAX, 1),
            None
        );
        assert_eq!(
            Sq8RangeTrace::checked_pending_futures_bytes(1, usize::MAX),
            Some(Sq8RangeTrace::pending_futures_bytes(1, 1))
        );
        let trace = Sq8RangeTrace::new(capacity).unwrap();
        assert_eq!(trace.reserved_bytes(parallel), peak);
        assert_eq!(trace.capacity(), capacity);
        assert!(
            Sq8RangeTrace::modeled_bytes(SQ8_TRACE_MAX_RANGES, SQ8_TRACE_MAX_RANGES)
                <= SQ8_TRACE_MAX_BYTES
        );
        assert!(Sq8RangeTrace::new(SQ8_TRACE_MAX_RANGES).is_ok());
        assert!(Sq8RangeTrace::new(0).is_err());
        assert!(Sq8RangeTrace::new(SQ8_TRACE_MAX_RANGES + 1).is_err());
        assert!(Sq8RangeTrace::new(usize::MAX).is_err());
        assert_eq!(trace.ranges.capacity(), capacity);
    }

    // Independent clock reads do not use the profile helpers or their read counter.
    fn reference_cpu_ns(clock: rustix::time::ClockId) -> u64 {
        let time = rustix::time::clock_gettime(clock);
        u64::try_from(time.tv_sec).unwrap() * 1_000_000_000 + u64::try_from(time.tv_nsec).unwrap()
    }

    fn burn_reference_thread_cpu(budget_ns: u64) -> u64 {
        use std::hint::black_box;
        let start = reference_cpu_ns(rustix::time::ClockId::ThreadCPUTime);
        let step = 0x9e37_79b9_7f4a_7c15u64;
        let mut value = 17u64;
        let mut iterations = 0u64;
        let elapsed = loop {
            for _ in 0..4096 {
                value = black_box(value.wrapping_add(black_box(step)));
            }
            iterations += 4096;
            let elapsed = reference_cpu_ns(rustix::time::ClockId::ThreadCPUTime) - start;
            if elapsed >= budget_ns {
                break elapsed;
            }
        };
        assert_eq!(value, 17u64.wrapping_add(step.wrapping_mul(iterations)));
        elapsed
    }

    #[test]
    fn cpu_profile_clocks_distinguish_cpu_sleep_and_worker_phase() {
        use rustix::time::ClockId::{ProcessCPUTime, ThreadCPUTime};
        const WORK_CPU_NS: u64 = 60_000_000;
        let clocks_before = PROFILE_CLOCK_READS.with(|reads| reads.get());

        let reference_start = reference_cpu_ns(ThreadCPUTime);
        let start = SyncStart::now();
        let work_cpu = burn_reference_thread_cpu(WORK_CPU_NS);
        let mut busy = None;
        start.finish(&mut busy);
        let reference_busy = reference_cpu_ns(ThreadCPUTime) - reference_start;
        let busy = busy.unwrap();
        assert!(
            busy.thread_cpu_ns >= work_cpu / 2,
            "{busy:?}, work={work_cpu}"
        );
        assert!(
            busy.thread_cpu_ns <= reference_busy,
            "{busy:?}, reference={reference_busy}"
        );

        let reference_start = reference_cpu_ns(ThreadCPUTime);
        let start = SyncStart::now();
        thread::sleep(Duration::from_millis(80));
        let mut asleep = None;
        start.finish(&mut asleep);
        let reference_sleep = reference_cpu_ns(ThreadCPUTime) - reference_start;
        let asleep = asleep.unwrap();
        assert!(asleep.wall_ns >= 60_000_000, "{asleep:?}");
        assert!(asleep.thread_cpu_ns < asleep.wall_ns / 4, "{asleep:?}");
        assert!(
            asleep.thread_cpu_ns <= reference_sleep,
            "{asleep:?}, reference={reference_sleep}"
        );

        let mut phase = Sq8RangeTrace::new(1).unwrap();
        let (go, ready) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            ready.recv().unwrap();
            burn_reference_thread_cpu(WORK_CPU_NS)
        });
        let caller_start = reference_cpu_ns(ThreadCPUTime);
        let process_start = reference_cpu_ns(ProcessCPUTime);
        let start = SyncStart::now();
        phase.start_phase();
        go.send(()).unwrap();
        let worker_cpu = worker.join().unwrap();
        phase.finish_phase();
        let mut waited = None;
        start.finish(&mut waited);
        let reference_process = reference_cpu_ns(ProcessCPUTime) - process_start;
        let reference_caller = reference_cpu_ns(ThreadCPUTime) - caller_start;
        let waited = waited.unwrap();
        let phase_cpu = phase.phase_process_cpu_ns.unwrap();
        assert!(
            phase_cpu >= worker_cpu / 2,
            "phase={phase_cpu}, worker={worker_cpu}"
        );
        assert!(
            phase_cpu <= reference_process,
            "phase={phase_cpu}, reference={reference_process}"
        );
        assert!(
            reference_caller < phase_cpu / 4,
            "caller={reference_caller}, phase={phase_cpu}"
        );
        assert!(
            waited.thread_cpu_ns <= reference_caller,
            "{waited:?}, caller={reference_caller}"
        );
        assert!(
            waited.thread_cpu_ns < phase_cpu / 4,
            "{waited:?}, phase={phase_cpu}"
        );
        assert_eq!(phase.unattributed_process_cpu_ns, Some(phase_cpu));
        assert_eq!(phase.phase_clock_reads, 2);
        assert_eq!(
            [busy.clock_reads, asleep.clock_reads, waited.clock_reads],
            [4; 3]
        );
        assert_eq!(
            PROFILE_CLOCK_READS.with(|reads| reads.get()) - clocks_before,
            14
        );
    }

    #[test]
    #[ignore = "exploratory raw observer floor for this test ELF; root runs and interprets"]
    fn cpu_profile_sync_scope_observer_control() {
        let clocks_before = PROFILE_CLOCK_READS.with(|reads| reads.get());
        let mut empty_samples = [CpuWall::default(); 64];
        let mut empty_total = None;
        for sample in &mut empty_samples {
            let start = SyncStart::now();
            let mut measured = None;
            start.finish(&mut measured);
            *sample = measured.unwrap();
            sample.add_to(&mut empty_total);
        }
        let empty_reads = PROFILE_CLOCK_READS.with(|reads| reads.get()) - clocks_before;
        assert_eq!(empty_reads, 4 * empty_samples.len() as u64);
        assert_eq!(empty_total.unwrap().clock_reads, empty_reads);
        println!(
            "{}",
            serde_json::json!({
                "control": "empty_sync_scope", "test_elf_only": true,
                "sample_count": empty_samples.len(), "bytes": 0, "chunks": 0,
                "observed_clock_reads": empty_reads,
                "raw_total": empty_total, "raw_samples": empty_samples.as_slice(),
            })
        );

        // At most 2 MiB of byte buffers, 32 MiB copied, and 34,952 timed chunks.
        let source = vec![0x5au8; 1 << 20];
        for chunk_bytes in [256usize, 4096, 65536, 1 << 20] {
            let mut target = BytesMut::with_capacity(source.len());
            let clocks_before = PROFILE_CLOCK_READS.with(|reads| reads.get());
            let mut samples = [CpuWall::default(); 8];
            let mut total = None;
            for sample in &mut samples {
                target.clear();
                let mut measured = None;
                for chunk in source.chunks(chunk_bytes) {
                    let start = SyncStart::now();
                    target.extend_from_slice(std::hint::black_box(chunk));
                    start.finish(&mut measured);
                }
                assert_eq!(&target[..], source.as_slice());
                *sample = measured.unwrap();
                sample.add_to(&mut total);
            }
            let chunks = samples.len() * (source.len() / chunk_bytes);
            let reads = PROFILE_CLOCK_READS.with(|reads| reads.get()) - clocks_before;
            assert_eq!(reads, 4 * chunks as u64);
            assert_eq!(total.unwrap().clock_reads, reads);
            println!(
                "{}",
                serde_json::json!({
                    "control": "chunked_copy_sync_scope", "test_elf_only": true,
                    "allocation_outside_scope": true, "first_touch_controlled": false,
                    "destination_reused_within_shape": true,
                    "raw_includes_observer_and_first_touch": true,
                    "sample_count": samples.len(), "bytes_per_sample": source.len(),
                    "total_bytes": samples.len() * source.len(),
                    "chunk_bytes": chunk_bytes, "chunks": chunks,
                    "observed_clock_reads": reads,
                    "raw_total": total, "raw_samples": samples.as_slice(),
                })
            );
        }
    }

    #[test]
    fn cpu_profile_observer_reuse_layout_and_admission() {
        use std::mem::size_of;
        let mut profile = QueryProfile::new(128, 128).unwrap();
        let retained = QueryProfile::retained_bytes(128, 128);
        assert_eq!(
            retained,
            size_of::<QueryProfile>()
                + 2 * ALLOCATION_ALLOWANCE_BYTES
                + 256 * size_of::<RangeSpan>()
        );
        let extra = QueryProfile::additional_bytes(128, 128);
        assert_eq!(
            extra,
            retained + 128 * (size_of::<Probe>() + ALLOCATION_ALLOWANCE_BYTES)
        );
        assert_eq!(
            QueryProfile::modeled_bytes(128, 128, 64, 16),
            extra + Sq8RangeTrace::pending_futures_bytes(128, 64)
        );
        assert!(QueryProfile::modeled_bytes(128, 128, 128, 128) <= SQ8_TRACE_MAX_BYTES);
        assert!(QueryProfile::new(0, 1).is_err());
        assert!(QueryProfile::new(1, 129).is_err());
        assert!(QueryProfile::new(usize::MAX, 1).is_err());
        let origin = Instant::now();
        profile.begin(origin);
        for (index, complete_ns) in [(0, 9), (1, 7)] {
            let mut span = RangeSpan::new(index, index, index, Some(index..index + 1));
            span.complete_ns = Some(complete_ns);
            profile.source.push_probe(Probe {
                origin,
                span,
                cpu: RangeCpu {
                    collector_copy: Some(CpuWall {
                        thread_cpu_ns: 2,
                        wall_ns: 3,
                        clock_reads: 4,
                    }),
                    collector_copied_bytes: 1,
                    ..RangeCpu::default()
                },
            });
        }
        assert_eq!(profile.source.cpu.collector_copy.unwrap().thread_cpu_ns, 4);
        assert_eq!(profile.source.cpu.collector_copy.unwrap().clock_reads, 8);
        let summary = serde_json::to_value(profile.source_summary()).unwrap();
        assert!(summary.get("ranges").is_none());
        assert_eq!(summary["last_completing_range"][0], 0);
        assert!(summary["phase_process_cpu_ns"].is_null());
        assert_eq!(profile.acquisition_poll_cpu, "unresolved_not_instrumented");
        let pointers = (profile.source.ranges.as_ptr(), profile.sq8.ranges.as_ptr());
        profile.discovery = Some(CpuWall::default());
        profile.source.phase_cpu_start = Some(1);
        profile.source.phase_process_cpu_ns = Some(2);
        profile.sq8.rank_thread_cpu_ns = Some(3);
        PROFILE_CLOCK_READS.with(|reads| reads.set(0));
        profile.refuse();
        assert_eq!(PROFILE_CLOCK_READS.with(|reads| reads.get()), 0);
        for trace in [&profile.source, &profile.sq8] {
            assert_eq!(trace.outcome, "admission_refused");
            assert_eq!(trace.cpu, RangeCpu::default());
            assert_eq!(trace.phase_process_cpu_ns, None);
            assert_eq!(trace.unattributed_process_cpu_ns, None);
            assert_eq!(trace.rank_thread_cpu_ns, None);
            assert_eq!(trace.phase_cpu_start, None);
            assert!(trace.ranges().is_empty());
        }
        assert_eq!(profile.discovery, None);
        profile.begin(origin);
        assert_eq!(PROFILE_CLOCK_READS.with(|reads| reads.get()), 0);
        assert_eq!(
            pointers,
            (profile.source.ranges.as_ptr(), profile.sq8.ranges.as_ptr())
        );
        assert_eq!(profile.source.outcome, "started");
        assert_eq!(profile.cover, None);
        assert_eq!(profile.planning, None);
    }

    #[test]
    fn refused_trace_clears_every_stamp_without_reallocating_or_reading_a_clock() {
        let mut trace = Sq8RangeTrace::new(4).unwrap();
        trace.begin(Instant::now());
        for index in 0..4 {
            let mut span = RangeSpan::new(index, index, index, Some(0..13));
            span.first_poll_ns = Some(1);
            span.complete_ns = Some(2);
            span.outcome = "ok";
            trace.push(span);
        }
        trace.push(RangeSpan::default());
        assert_eq!((trace.ranges.len(), trace.dropped_ranges), (4, 1));
        trace.planned_ranges = 4;
        trace.planned_bytes = 52;
        trace.max_parallel = 2;
        trace.fetch_start_ns = Some(1);
        trace.all_ranges_complete_ns = Some(2);
        trace.rank_start_ns = Some(3);
        trace.rank_end_ns = Some(4);
        trace.release_end_ns = Some(5);
        trace.rank = Some(RankPhases {
            ranges: 4,
            rows: 4,
            ..RankPhases::default()
        });
        trace.outcome = "ranked";
        let (pointer, capacity) = (trace.ranges.as_ptr(), trace.ranges.capacity());
        trace.refuse();
        assert_eq!(trace.outcome, "admission_refused");
        assert!(trace.ranges.is_empty());
        assert_eq!(
            (
                trace.planned_ranges,
                trace.planned_bytes,
                trace.max_parallel,
                trace.dropped_ranges
            ),
            (0, 0, 0, 0)
        );
        assert_eq!(
            (
                trace.fetch_start_ns,
                trace.all_ranges_complete_ns,
                trace.rank_start_ns,
                trace.rank_end_ns
            ),
            (None, None, None, None)
        );
        assert_eq!((trace.rank, trace.release_end_ns), (None, None));
        assert_eq!(
            (trace.ranges.as_ptr(), trace.ranges.capacity()),
            (pointer, capacity)
        );
        // A query that does start replaces the refused state.
        trace.begin(Instant::now());
        assert_eq!(trace.outcome, "started");
    }

    #[tokio::test]
    async fn traced_ranking_matches_untraced_and_leaves_unreached_stamps_unset() {
        let store = InMemory::new();
        let location = Path::from("sq8.bin");
        let object = unique_sq8_object(273);
        let authority = sq8_authority(&object, 273);
        store
            .put(&location, PutPayload::from(object.clone()))
            .await
            .unwrap();
        let etag = store.head(&location).await.unwrap().e_tag.unwrap();
        let mut trace = Sq8RangeTrace::new(2).unwrap();
        // Success: same candidates, bits and charges; every stamp is set and ordered.
        let mut ok = Sq8RangeTrace::new(2).unwrap();
        ok.begin(Instant::now());
        let ranges = [(0, 0), (1, 1)];
        let traced = rank_verified_sq8_pages_traced(
            &store,
            &location,
            &authority,
            &ranges,
            &etag,
            &[1.0],
            &[0.0],
            &[1.0],
            1,
            2,
            object.len(),
            2,
            &[],
            Some(&mut ok),
        )
        .await;
        let plain = rank_verified_sq8_pages_inner(
            &store,
            &location,
            &authority,
            &ranges,
            &etag,
            &[1.0],
            &[0.0],
            &[1.0],
            1,
            2,
            object.len(),
            2,
            &[],
        )
        .await;
        let (traced, plain) = (traced.unwrap(), plain.unwrap());
        assert_eq!(traced.stats, plain.stats);
        assert_eq!(
            traced
                .candidates
                .iter()
                .map(|hit| (hit.ordinal, hit.id, hit.score.to_bits()))
                .collect::<Vec<_>>(),
            plain
                .candidates
                .iter()
                .map(|hit| (hit.ordinal, hit.id, hit.score.to_bits()))
                .collect::<Vec<_>>()
        );
        assert_eq!(ok.outcome, "ranked");
        let (start, end, release) = (
            ok.rank_start_ns.unwrap(),
            ok.rank_end_ns.unwrap(),
            ok.release_end_ns.unwrap(),
        );
        assert!(ok.all_ranges_complete_ns.unwrap() <= start && start <= end && end <= release);
        assert_eq!((ok.planned_ranges, ok.ranges.len()), (2, 2));
        assert_eq!(ok.planned_bytes, object.len() as u64);
        assert!(ok.ranges.iter().all(|span| span.outcome == "ok"));
        // Rejected before any I/O: no span, no stamp, no fabricated zero.
        for (name, ranges, gets, bytes, parallel, excluded) in [
            (
                "overlap",
                &[(1, 1), (1, 1)][..],
                2,
                object.len(),
                2,
                &[][..],
            ),
            ("bytes", &[(0, 1)][..], 2, 17 * 13, 2, &[][..]),
            ("gets", &[(0, 0), (1, 1)][..], 1, object.len(), 2, &[][..]),
            ("parallel", &[(0, 0)][..], 2, object.len(), 0, &[][..]),
            ("roster", &[(0, 0)][..], 2, object.len(), 2, &[5, 4][..]),
        ] {
            let mut rejected = Sq8RangeTrace::new(2).unwrap();
            rejected.begin(Instant::now());
            let failure = rank_verified_sq8_pages_traced(
                &store,
                &location,
                &authority,
                ranges,
                &etag,
                &[1.0],
                &[0.0],
                &[1.0],
                1,
                gets,
                bytes,
                parallel,
                excluded,
                Some(&mut rejected),
            )
            .await
            .unwrap_err();
            assert_eq!(failure.stats, Sq8ReadStats::default(), "{name}");
            assert_eq!(rejected.outcome, "rejected_before_io", "{name}");
            assert!(rejected.ranges.is_empty(), "{name}");
            assert_eq!(
                (rejected.fetch_start_ns, rejected.all_ranges_complete_ns),
                (None, None),
                "{name}"
            );
            assert_eq!(
                (
                    rejected.rank_start_ns,
                    rejected.rank_end_ns,
                    rejected.release_end_ns
                ),
                (None, None, None)
            );
            assert_eq!(rejected.rank, None);
            assert_eq!((rejected.planned_ranges, rejected.planned_bytes), (0, 0));
        }
        // A failed request keeps a truthful partial span: polled and requested, never answered.
        store
            .put(&location, PutPayload::from(vec![8u8; object.len()]))
            .await
            .unwrap();
        trace.begin(Instant::now());
        let failure = rank_verified_sq8_pages_traced(
            &store,
            &location,
            &authority,
            &[(1, 1)],
            &etag,
            &[1.0],
            &[0.0],
            &[1.0],
            1,
            2,
            object.len(),
            2,
            &[],
            Some(&mut trace),
        )
        .await
        .unwrap_err();
        assert_eq!(failure.stats.failed_gets, 1);
        assert_eq!(trace.outcome, "fetch_failed");
        assert_eq!(trace.ranges.len(), 1);
        let span = trace.ranges[0];
        assert_eq!(span.outcome, "store_headers");
        assert!(span.first_poll_ns.is_some() && span.request_ns.is_some());
        assert!(span.complete_ns.is_some());
        assert_eq!(
            (
                span.headers_ns,
                span.metadata_ns,
                span.first_chunk_ns,
                span.eof_ns,
                span.auth_start_ns,
                span.auth_end_ns
            ),
            (None, None, None, None, None, None)
        );
        assert_eq!((span.chunks, span.body_bytes, span.copy_count), (0, 0, 0));
        assert!(trace.all_ranges_complete_ns.is_some());
        assert_eq!(
            (trace.rank_start_ns, trace.rank_end_ns, trace.release_end_ns),
            (None, None, None)
        );
        // A trace never silently omits a range: a plan larger than its fixed capacity is refused
        // before any future or GET exists, while the same plan runs untraced.
        let mut small = Sq8RangeTrace::new(1).unwrap();
        small.begin(Instant::now());
        store
            .put(&location, PutPayload::from(object.clone()))
            .await
            .unwrap();
        let etag = store.head(&location).await.unwrap().e_tag.unwrap();
        let wide = [(0, 0), (1, 1)];
        let refusal = rank_verified_sq8_pages_traced(
            &store,
            &location,
            &authority,
            &wide,
            &etag,
            &[1.0],
            &[0.0],
            &[1.0],
            1,
            2,
            object.len(),
            2,
            &[],
            Some(&mut small),
        )
        .await
        .unwrap_err();
        assert!(matches!(refusal.error, RangeFetchError::UnexpectedMetadata));
        assert_eq!(refusal.stats, Sq8ReadStats::default());
        assert_eq!(small.outcome, "rejected_before_io");
        assert_eq!((small.ranges.len(), small.dropped_ranges), (0, 0));
        assert_eq!(small.ranges.capacity(), 1);
        assert_eq!(small.fetch_start_ns, None);
        rank_verified_sq8_pages_inner(
            &store,
            &location,
            &authority,
            &wide,
            &etag,
            &[1.0],
            &[0.0],
            &[1.0],
            1,
            2,
            object.len(),
            2,
            &[],
        )
        .await
        .unwrap();
        // Ranking that fails after a complete fetch still stamps ranking and the payload release.
        let (dup_authority, dup_object) = short_tail_authority();
        let dup_store = InMemory::new();
        dup_store
            .put(&location, PutPayload::from(dup_object))
            .await
            .unwrap();
        let dup_etag = dup_store.head(&location).await.unwrap().e_tag.unwrap();
        let mut failed = Sq8RangeTrace::new(1).unwrap();
        failed.begin(Instant::now());
        let failure = rank_verified_sq8_pages_traced(
            &dup_store,
            &location,
            &dup_authority,
            &[(0, 0)],
            &dup_etag,
            &[1.0],
            &[0.0],
            &[1.0],
            1,
            1,
            256 * 13,
            1,
            &[],
            Some(&mut failed),
        )
        .await
        .unwrap_err();
        assert!(matches!(failure.error, RangeFetchError::Score(_)));
        assert_eq!(
            failure.stats,
            Sq8ReadStats {
                submitted_gets: 1,
                verified_bytes: 256 * 13,
                failed_gets: 0
            }
        );
        assert_eq!(failed.outcome, "rank_failed");
        let (start, end, release) = (
            failed.rank_start_ns.unwrap(),
            failed.rank_end_ns.unwrap(),
            failed.release_end_ns.unwrap(),
        );
        assert!(failed.all_ranges_complete_ns.unwrap() <= start && start <= end && end <= release);
        assert!(failed.rank.is_some());
        assert_eq!(failed.ranges[0].outcome, "ok");
    }

    #[tokio::test]
    async fn traced_ranges_overlap_in_flight_and_order_boundaries_before_rank() {
        use super::cold_http_fixture::{ETAG, Fixture};
        let object = unique_sq8_object(600);
        let authority = sq8_authority(&object, 600);
        let key = "gen/objects/sq8";
        let deadline = Instant::now() + Duration::from_secs(60);
        let fixture = Fixture::new(
            std::collections::BTreeMap::from([(key.to_owned(), object.clone())]),
            deadline,
        );
        // Stage-1 bodies hold their final byte until released.
        fixture.arm(None);
        let location = Path::from(key);
        let mut trace = Sq8RangeTrace::new(2).unwrap();
        trace.begin(Instant::now());
        let work = rank_verified_sq8_pages_traced(
            fixture.reader.store(),
            &location,
            &authority,
            &[(0, 0), (2, 2)],
            ETAG,
            &[1.0],
            &[0.0],
            &[1.0],
            3,
            2,
            object.len(),
            2,
            &[],
            Some(&mut trace),
        );
        let (ranked, ()) = tokio::join!(work, async {
            fixture.wait(|state| state.active[1] >= 2).await;
            fixture.release(1);
        });
        let ranked = ranked.unwrap();
        assert_eq!(ranked.candidates.len(), 3);
        assert_eq!(ranked.stats.submitted_gets, 2);
        assert_eq!(trace.outcome, "ranked");
        assert_eq!(
            (
                trace.planned_ranges,
                trace.dropped_ranges,
                trace.ranges.len()
            ),
            (2, 0, 2)
        );
        assert!(fixture.snapshot().peak[1] >= 2);
        let (a, b) = (trace.ranges[0], trace.ranges[1]);
        assert_eq!(
            (a.start_byte, a.end_byte, b.start_byte, b.end_byte),
            (0, 3328, 6656, 7800)
        );
        // Neither range can finish before the other has started: they overlap in flight.
        assert!(a.first_poll_ns.unwrap() < b.complete_ns.unwrap());
        assert!(b.first_poll_ns.unwrap() < a.complete_ns.unwrap());
        for span in &trace.ranges {
            let chain = [
                span.first_poll_ns,
                span.request_ns,
                span.headers_ns,
                span.metadata_ns,
                span.first_chunk_ns,
                span.last_chunk_ns,
                span.eof_ns,
                span.auth_start_ns,
                span.auth_end_ns,
                span.complete_ns,
            ]
            .map(Option::unwrap);
            assert!(chain.windows(2).all(|w| w[0] <= w[1]), "{chain:?}");
            assert_eq!(span.outcome, "ok");
            assert_eq!(span.body_bytes, span.end_byte - span.start_byte);
            assert!(span.chunks >= 1 && span.copy_count == span.chunks);
            assert!(span.max_body_gap_end_ns.is_some());
        }
        let all = trace.all_ranges_complete_ns.unwrap();
        let latest = a.complete_ns.unwrap().max(b.complete_ns.unwrap());
        assert!(
            trace.fetch_start_ns.unwrap() <= a.first_poll_ns.unwrap().min(b.first_poll_ns.unwrap())
        );
        // Authentication of every range precedes the barrier; ranking starts after it.
        assert!(latest <= all && all <= trace.rank_start_ns.unwrap());
        assert!(trace.rank_start_ns.unwrap() <= trace.rank_end_ns.unwrap());
        assert!(trace.rank_end_ns.unwrap() <= trace.release_end_ns.unwrap());
        let phases = trace.rank.unwrap();
        assert_eq!((phases.ranges, phases.rows), (2, 256 + 88));
        fixture.finish();
    }

    #[tokio::test]
    async fn traced_failures_report_truthful_partial_spans_through_the_full_drain() {
        use super::cold_http_fixture::{ETAG, Fixture};
        let object = unique_sq8_object(600);
        let authority = sq8_authority(&object, 600);
        let key = "gen/objects/sq8";
        let deadline = Instant::now() + Duration::from_secs(60);
        let fixture = Fixture::new(
            std::collections::BTreeMap::from([(key.to_owned(), object.clone())]),
            deadline,
        );
        let location = Path::from(key);
        // Range 0: clean EOF one byte short after the sibling failure; range 1: flipped
        // final byte (digest failure) at once; range 2: a held, valid sibling.
        let starts = [0, 3328, 6656];
        fixture.arm(Some((1, starts)));
        let mut trace = Sq8RangeTrace::new(3).unwrap();
        trace.begin(Instant::now());
        let work = rank_verified_sq8_pages_traced(
            fixture.reader.store(),
            &location,
            &authority,
            &[(0, 0), (1, 1), (2, 2)],
            ETAG,
            &[1.0],
            &[0.0],
            &[1.0],
            3,
            3,
            object.len(),
            3,
            &[],
            Some(&mut trace),
        );
        let (result, ()) = tokio::join!(work, async {
            fixture
                .wait(|state| state.finished[1].contains(&starts[1]) && state.active[1] >= 2)
                .await;
            fixture.release_error();
            fixture
                .wait(|state| state.finished[1].contains(&starts[0]))
                .await;
            fixture.release_sibling();
        });
        let failure = result.unwrap_err();
        assert_eq!(
            failure.stats,
            Sq8ReadStats {
                submitted_gets: 3,
                verified_bytes: 1144,
                failed_gets: 2
            }
        );
        // The drain finished every GET, in the order the fixture released them.
        let state = fixture.snapshot();
        assert_eq!(state.finished[1], vec![starts[1], starts[0], starts[2]]);
        assert_eq!(trace.outcome, "fetch_failed");
        assert_eq!(trace.ranges.len(), 3);
        let [stream, digest, sibling] = [trace.ranges[0], trace.ranges[1], trace.ranges[2]];
        assert_eq!(stream.outcome, "store_body");
        assert!(stream.headers_ns.is_some() && stream.metadata_ns.is_some());
        assert!(stream.body_bytes < stream.end_byte - stream.start_byte);
        assert_eq!(
            (stream.eof_ns, stream.auth_start_ns, stream.auth_end_ns),
            (None, None, None)
        );
        assert_eq!(digest.outcome, "page_auth");
        assert!(digest.eof_ns.is_some() && digest.auth_end_ns.is_some());
        assert_eq!(
            digest.body_bytes,
            digest.end_byte - digest.start_byte,
            "the whole corrupt body was still received"
        );
        assert_eq!(sibling.outcome, "ok");
        for span in &trace.ranges {
            assert!(span.complete_ns.is_some());
        }
        let all = trace.all_ranges_complete_ns.unwrap();
        assert!(
            trace
                .ranges
                .iter()
                .all(|span| span.complete_ns.unwrap() <= all)
        );
        // Failed fetch: ranking never started and the payload release is unreached.
        assert_eq!(
            (trace.rank_start_ns, trace.rank_end_ns, trace.release_end_ns),
            (None, None, None)
        );
        fixture.finish();
    }

    #[tokio::test]
    async fn shared_byte_window_limits_actual_http_bodies() {
        use super::cold_http_fixture::{ETAG, Fixture};
        let object = unique_sq8_object(8 * 256);
        let authority = sq8_authority(&object, 8 * 256);
        let key = "gen/objects/sq8";
        let location = Path::from(key);
        let ranges = (0..8).map(|page| (page, page)).collect::<Vec<_>>();
        let budget = InFlightBytes::new(2 * 3328);
        let fixture = Fixture::new(
            std::collections::BTreeMap::from([(key.to_owned(), object.clone())]),
            Instant::now() + Duration::from_secs(10),
        );
        fixture.arm(None);
        let work = fetch_verified_ranges_with_byte_window(
            fixture.reader.store(),
            &location,
            &authority,
            &ranges,
            ETAG,
            8,
            object.len(),
            8,
            &budget,
        );
        let (result, peak) = tokio::join!(work, async {
            fixture.wait(|state| state.active[1] >= 2).await;
            tokio::time::sleep(Duration::from_millis(30)).await;
            let peak = fixture.snapshot().peak[1];
            fixture.release(1);
            peak
        });
        let (returned, stats) = result.unwrap();
        assert_eq!(stats.submitted_gets, 8);
        assert_eq!(stats.verified_bytes, object.len());
        for (page, payload) in returned.iter().enumerate() {
            assert_eq!(&payload.bytes[..], &object[page * 3328..(page + 1) * 3328]);
        }
        assert_eq!(fixture.snapshot().finished[1].len(), 8);
        assert_eq!(fixture.snapshot().active[1], 0);
        fixture.finish();
        assert!(
            peak <= 2,
            "missing shared byte-window admission in actual HTTP transport"
        );
    }

    #[tokio::test]
    async fn shared_byte_window_spans_queries_and_refuses_late_oversize_before_io() {
        use super::cold_http_fixture::{ETAG, Fixture};
        let object = unique_sq8_object(8 * 256);
        let authority = sq8_authority(&object, 8 * 256);
        let key = "gen/objects/sq8";
        let location = Path::from(key);
        let ranges = (0..8).map(|page| (page, page)).collect::<Vec<_>>();
        let budget = InFlightBytes::new(2 * 3328);
        let fixture = Fixture::new(
            std::collections::BTreeMap::from([(key.to_owned(), object.clone())]),
            Instant::now() + Duration::from_secs(10),
        );
        fixture.arm(None);
        let narrow = InFlightBytes::new(3328);
        let refusal = fetch_verified_ranges_with_byte_window(
            fixture.reader.store(),
            &location,
            &authority,
            &[(0, 0), (1, 2)],
            ETAG,
            8,
            object.len(),
            8,
            &narrow,
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(refusal.error, RangeFetchError::UnexpectedMetadata));
        assert_eq!(refusal.stats.submitted_gets, 0);
        assert!(fixture.snapshot().requests.is_empty());
        let full = InFlightBytes::with_waiter_limit(3328, 1, 4096).unwrap();
        let held = full.reserve(3328).await.unwrap();
        let mut waiter = Box::pin(full.reserve(1));
        assert!(futures_util::poll!(waiter.as_mut()).is_pending());
        let rejected = fetch_verified_ranges_with_byte_window(
            fixture.reader.store(),
            &location,
            &authority,
            &ranges,
            ETAG,
            8,
            object.len(),
            8,
            &full,
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(
            rejected.error,
            RangeFetchError::UnexpectedMetadata
        ));
        assert_eq!(rejected.stats.submitted_gets, 0);
        assert_eq!(rejected.stats.failed_gets, 0);
        assert!(fixture.snapshot().requests.is_empty());
        drop(waiter);
        drop(held);
        assert_eq!(*full.available.lock().unwrap(), 3328);
        let first = fetch_verified_ranges_with_byte_window(
            fixture.reader.store(),
            &location,
            &authority,
            &ranges,
            ETAG,
            8,
            object.len(),
            8,
            &budget,
        );
        let second = fetch_verified_ranges_with_byte_window(
            fixture.reader.store(),
            &location,
            &authority,
            &ranges,
            ETAG,
            8,
            object.len(),
            8,
            &budget,
        );
        let (first, second, peak) = tokio::join!(first, second, async {
            fixture.wait(|state| state.active[1] == 2).await;
            tokio::time::sleep(Duration::from_millis(30)).await;
            let peak = fixture.snapshot().peak[1];
            fixture.release(1);
            peak
        });
        for result in [first, second] {
            let (returned, stats) = result.unwrap();
            assert_eq!(stats.submitted_gets, 8);
            assert_eq!(stats.verified_bytes, object.len());
            for (page, payload) in returned.iter().enumerate() {
                assert_eq!(&payload.bytes[..], &object[page * 3328..(page + 1) * 3328]);
            }
        }
        assert!(peak <= 2);
        assert_eq!(fixture.snapshot().finished[1].len(), 16);
        assert_eq!(fixture.snapshot().active[1], 0);
        assert_eq!(*budget.available.lock().unwrap(), 2 * 3328);
        fixture.finish();
    }

    #[tokio::test]
    async fn shared_byte_window_preserves_ordered_error_and_queued_drain() {
        use super::cold_http_fixture::{ETAG, Fixture};
        let object = unique_sq8_object(4 * 256);
        let authority = sq8_authority(&object, 4 * 256);
        let key = "gen/objects/sq8";
        let location = Path::from(key);
        let ranges = (0..4).map(|page| (page, page)).collect::<Vec<_>>();
        let budget = InFlightBytes::new(2 * 3328);
        let fixture = Fixture::new(
            std::collections::BTreeMap::from([(key.to_owned(), object.clone())]),
            Instant::now() + Duration::from_secs(10),
        );
        fixture.arm(Some((1, [0, 3328, 6656])));
        let mut work = Box::pin(fetch_verified_ranges_with_byte_window(
            fixture.reader.store(),
            &location,
            &authority,
            &ranges,
            ETAG,
            4,
            object.len(),
            4,
            &budget,
        ));
        tokio::select! {
            _ = &mut work => panic!("returned before ordered stream error"),
            () = fixture.wait(|state| state.finished[1].contains(&3328)) => {}
        }
        assert!(!fixture.snapshot().finished[1].contains(&0));
        fixture.release_error();
        tokio::select! {
            _ = &mut work => panic!("returned before held sibling drained"),
            () = fixture.wait(|state| state.finished[1].contains(&0)) => {}
        }
        fixture.release_sibling();
        let error = work.await.err().unwrap();
        assert!(matches!(error.error, RangeFetchError::Store(_)));
        assert_eq!(error.stats.submitted_gets, 4);
        assert_eq!(error.stats.failed_gets, 2);
        assert_eq!(error.stats.verified_bytes, object.len() - 2 * 3328);
        let state = fixture.snapshot();
        assert_eq!(state.active[1], 0);
        assert_eq!(state.finished[1].len(), 4);
        assert!(state.peak[1] <= 2);
        assert!(state.errors.is_empty());
        assert_eq!(*budget.available.lock().unwrap(), 2 * 3328);
        fixture.finish();
    }

    #[tokio::test]
    async fn one_wave128_range_transport_preserves_bytes_errors_and_drain() {
        use super::cold_http_fixture::{ETAG, Fixture};
        let object = unique_sq8_object(128 * 256);
        let authority = sq8_authority(&object, 128 * 256);
        let key = "gen/objects/sq8";
        let location = Path::from(key);
        let ranges = (0..128).map(|page| (page, page)).collect::<Vec<_>>();
        let fixture = Fixture::new(
            std::collections::BTreeMap::from([(key.to_owned(), object.clone())]),
            Instant::now() + Duration::from_secs(60),
        );
        for parallelism in [64, 128] {
            fixture.arm(None);
            let work = fetch_verified_ranges_inner(
                fixture.reader.store(),
                &location,
                &authority,
                &ranges,
                ETAG,
                128,
                object.len(),
                parallelism,
            );
            let (result, ()) = tokio::join!(work, async {
                fixture.wait(|state| state.active[1] == parallelism).await;
                tokio::time::sleep(Duration::from_millis(30)).await;
                assert_eq!(fixture.snapshot().peak[1], parallelism);
                fixture.release(1);
            });
            let (verified, stats) = result.unwrap();
            assert_eq!(stats.submitted_gets, 128);
            assert_eq!(stats.verified_bytes, object.len());
            assert_eq!(stats.failed_gets, 0);
            assert_eq!(verified.len(), 128);
            for (page, returned) in verified.iter().enumerate() {
                assert_eq!(returned.start, page * 3328);
                assert_eq!(&returned.bytes[..], &object[page * 3328..(page + 1) * 3328]);
            }
            assert_eq!(fixture.snapshot().finished[1].len(), 128);
            assert_eq!(fixture.snapshot().active[1], 0);

            fixture.arm(Some((1, [0, 3328, 6656])));
            let mut work = Box::pin(fetch_verified_ranges_inner(
                fixture.reader.store(),
                &location,
                &authority,
                &ranges,
                ETAG,
                128,
                object.len(),
                parallelism,
            ));
            tokio::select! {
                _ = &mut work => panic!("returned before ordered stream error"),
                () = fixture.wait(|state| state.finished[1].contains(&3328)) => {}
            }
            assert!(!fixture.snapshot().finished[1].contains(&0));
            fixture.release_error();
            tokio::select! {
                _ = &mut work => panic!("returned before held sibling drained"),
                () = fixture.wait(|state| state.finished[1].contains(&0)) => {}
            }
            fixture.release_sibling();
            let error = work.await.err().unwrap();
            assert!(matches!(error.error, RangeFetchError::Store(_)));
            assert_eq!(error.stats.submitted_gets, 128);
            assert_eq!(error.stats.failed_gets, 2);
            assert_eq!(error.stats.verified_bytes, object.len() - 2 * 3328);
            let state = fixture.snapshot();
            assert_eq!(state.active[1], 0);
            assert_eq!(state.finished[1].len(), 128);
            assert!(state.peak[1] <= parallelism);
            assert!(state.errors.is_empty(), "{:?}", state.errors);
        }
        fixture.finish();
    }

    #[tokio::test]
    async fn completion_driven_fetch_refills_before_earlier_body_finishes() {
        use super::cold_http_fixture::{ETAG, Fixture};
        let object = unique_sq8_object(600);
        let authority = sq8_authority(&object, 600);
        let key = "gen/objects/sq8";
        let deadline = Instant::now() + Duration::from_secs(60);
        let fixture = Fixture::new(
            std::collections::BTreeMap::from([(key.to_owned(), object.clone())]),
            deadline,
        );
        let location = Path::from(key);
        // Range 0: clean EOF one byte short after the sibling failure; range 1: flipped
        // final byte (digest failure) at once; range 2: a held, valid sibling.
        let starts = [0, 3328, 6656];
        fixture.arm(Some((1, starts)));
        let mut trace = Sq8RangeTrace::new(3).unwrap();
        trace.begin(Instant::now());
        let work = rank_verified_sq8_pages_traced(
            fixture.reader.store(),
            &location,
            &authority,
            &[(0, 0), (1, 1), (2, 2)],
            ETAG,
            &[1.0],
            &[0.0],
            &[1.0],
            3,
            3,
            object.len(),
            2,
            &[],
            Some(&mut trace),
        );
        let (result, refilled) = tokio::join!(work, async {
            let refilled = tokio::time::timeout(
                Duration::from_secs(2),
                fixture
                    .wait(|state| state.finished[1].contains(&starts[1]) && state.active[1] >= 2),
            )
            .await
            .is_ok();
            fixture.release_error();
            fixture
                .wait(|state| state.finished[1].contains(&starts[0]))
                .await;
            fixture.release_sibling();
            refilled
        });
        let failure = result.unwrap_err();
        assert!(matches!(failure.error, RangeFetchError::Store(_)));
        assert_eq!(
            failure.stats,
            Sq8ReadStats {
                submitted_gets: 3,
                verified_bytes: 1144,
                failed_gets: 2
            }
        );
        // The drain finished every GET, in the order the fixture released them.
        let state = fixture.snapshot();
        assert_eq!(state.finished[1], vec![starts[1], starts[0], starts[2]]);
        assert_eq!(trace.outcome, "fetch_failed");
        assert_eq!(trace.ranges.len(), 3);
        let [stream, digest, sibling] = [trace.ranges[0], trace.ranges[1], trace.ranges[2]];
        assert_eq!(stream.outcome, "store_body");
        assert!(stream.headers_ns.is_some() && stream.metadata_ns.is_some());
        assert!(stream.body_bytes < stream.end_byte - stream.start_byte);
        assert_eq!(
            (stream.eof_ns, stream.auth_start_ns, stream.auth_end_ns),
            (None, None, None)
        );
        assert_eq!(digest.outcome, "page_auth");
        assert!(digest.eof_ns.is_some() && digest.auth_end_ns.is_some());
        assert_eq!(
            digest.body_bytes,
            digest.end_byte - digest.start_byte,
            "the whole corrupt body was still received"
        );
        assert_eq!(sibling.outcome, "ok");
        for span in &trace.ranges {
            assert!(span.complete_ns.is_some());
        }
        let all = trace.all_ranges_complete_ns.unwrap();
        assert!(
            trace
                .ranges
                .iter()
                .all(|span| span.complete_ns.unwrap() <= all)
        );
        // Failed fetch: ranking never started and the payload release is unreached.
        assert_eq!(
            (trace.rank_start_ns, trace.rank_end_ns, trace.release_end_ns),
            (None, None, None)
        );
        assert_eq!(state.active[1], 0);
        assert!(state.peak[1] <= 2);
        fixture.finish();
        assert!(
            refilled,
            "completed request must refill its slot before earlier body finishes"
        );
    }

    #[tokio::test]
    async fn bounded_query_reads_only_verified_pages_and_charges_failures() {
        let store = InMemory::new();
        let location = Path::from("sq8.bin");
        let mut object = Vec::new();
        for id in 0_i64..273 {
            object.extend_from_slice(&id.to_le_bytes());
            object.extend_from_slice(&(id as f32).to_le_bytes());
            object.push(0);
        }
        store
            .put(&location, PutPayload::from(object.clone()))
            .await
            .unwrap();
        let etag = store.head(&location).await.unwrap().e_tag.unwrap();
        let sidecar = object
            .chunks(256 * 13)
            .flat_map(|page| Sha256::digest(page).to_vec())
            .collect::<Vec<_>>();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":1,
            "rows":273, "dimensions":1, "page_rows":256,
            "object_sha256":format!("{:x}", Sha256::digest(&object)),
            "page_digest_sha256":format!("{:x}", Sha256::digest(&sidecar)),
        }))
        .unwrap();
        let authority = PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .unwrap();
        async fn run(
            store: &InMemory,
            location: &Path,
            authority: &PageAuthority,
            etag: &str,
            ranges: &[(usize, usize)],
            bytes: usize,
        ) -> Result<RankedSq8, RankedSq8Failure> {
            run_excluding(store, location, authority, etag, ranges, bytes, &[]).await
        }
        async fn run_excluding(
            store: &InMemory,
            location: &Path,
            authority: &PageAuthority,
            etag: &str,
            ranges: &[(usize, usize)],
            bytes: usize,
            excluded: &[i64],
        ) -> Result<RankedSq8, RankedSq8Failure> {
            rank_verified_sq8_pages_inner(
                store,
                location,
                authority,
                ranges,
                etag,
                &[1.0],
                &[0.0],
                &[1.0],
                1,
                2,
                bytes,
                2,
                excluded,
            )
            .await
        }
        let ranked = run(&store, &location, &authority, &etag, &[(1, 1)], 17 * 13)
            .await
            .unwrap();
        assert_eq!(ranked.candidates[0].id, 256);
        assert_eq!(ranked.stats.submitted_gets, 1);
        assert_eq!(ranked.stats.verified_bytes, 17 * 13);
        assert_eq!(ranked.stats.failed_gets, 0);
        let replaced = run_excluding(
            &store,
            &location,
            &authority,
            &etag,
            &[(1, 1)],
            17 * 13,
            &[256],
        )
        .await
        .unwrap();
        assert_eq!(replaced.candidates[0].id, 257);
        assert_eq!(replaced.stats, ranked.stats);
        let all_ids = (256..273).collect::<Vec<i64>>();
        let deleted = run_excluding(
            &store,
            &location,
            &authority,
            &etag,
            &[(1, 1)],
            17 * 13,
            &all_ids,
        )
        .await
        .unwrap();
        assert!(deleted.candidates.is_empty());
        assert_eq!(deleted.stats, ranked.stats);
        let invalid = run_excluding(
            &store,
            &location,
            &authority,
            &etag,
            &[(1, 1)],
            17 * 13,
            &[257, 256],
        )
        .await
        .unwrap_err();
        assert_eq!(invalid.stats.submitted_gets, 0);
        let over_budget = run(&store, &location, &authority, &etag, &[(0, 1)], 17 * 13)
            .await
            .unwrap_err();
        assert_eq!(over_budget.stats.submitted_gets, 0);
        let overlapping = run(
            &store,
            &location,
            &authority,
            &etag,
            &[(1, 1), (1, 1)],
            object.len(),
        )
        .await
        .unwrap_err();
        assert_eq!(overlapping.stats.submitted_gets, 0);
        store
            .put(&location, PutPayload::from(vec![8u8; object.len()]))
            .await
            .unwrap();
        let failed = run(&store, &location, &authority, &etag, &[(1, 1)], 17 * 13)
            .await
            .unwrap_err();
        assert_eq!(failed.stats.submitted_gets, 1);
        assert_eq!(failed.stats.failed_gets, 1);
        assert_eq!(failed.stats.verified_bytes, 0);
    }
    #[tokio::test]
    async fn capsule_http_exact_ranges_reject_corruption_truncation_etag_and_rebound() {
        let expected = vec![9_u8; 128];
        for fault in ["ok", "corrupt", "short", "etag", "range", "size", "status"] {
            let mut body = expected.clone();
            if fault == "corrupt" {
                body[80] ^= 1;
            }
            if fault == "short" {
                body.pop();
            }
            let range = match fault {
                "range" => "bytes 1-128/256",
                "size" => "bytes 0-127/257",
                _ => "bytes 0-127/256",
            };
            let response = http_response(
                if fault == "status" {
                    "200 OK"
                } else {
                    "206 Partial Content"
                },
                Some(range),
                if fault == "etag" {
                    "\"wrong\""
                } else {
                    "\"fixture\""
                },
                expected.len(),
                &body,
            );
            let (reader, stop, requests, server) = http_fixture(response);
            let result = fetch_capsule_ranges(
                reader.store(),
                &Path::from("gen/plane/capsules.bin"),
                256,
                std::slice::from_ref(&(0..128)),
                "\"fixture\"",
                1,
                128,
                1,
                None,
                |offset, body| {
                    if offset == 0 && body == expected {
                        Ok(body)
                    } else {
                        Err(RangeFetchError::UnexpectedMetadata)
                    }
                },
            )
            .await;
            if fault == "ok" {
                let (ranges, stats) = result.unwrap();
                assert_eq!(ranges[0], expected);
                assert_eq!(
                    stats,
                    Sq8ReadStats {
                        submitted_gets: 1,
                        verified_bytes: 128,
                        failed_gets: 0
                    }
                );
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.stats.submitted_gets, 1, "{fault}");
                assert_eq!(error.stats.failed_gets, 1, "{fault}");
            }
            stop.store(true, Ordering::Relaxed);
            server.join().unwrap();
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1, "{fault}");
            let request = requests[0].to_ascii_lowercase();
            assert!(request.contains("range: bytes=0-127"));
            assert!(request.contains("if-match: \"fixture\""));
        }
    }

    #[tokio::test]
    async fn capsule_transport_admission_rejects_duplicate_overlap_and_minus_one_without_io() {
        assert!(
            capsule_fetch_descriptor_bytes::<crate::source_capsule::VerifiedPageSpan<'_, '_>>(
                usize::MAX
            )
            .is_none()
        );
        let (reader, stop, requests, server) = http_fixture(http_response(
            "206 Partial Content",
            Some("bytes 0-15/64"),
            "\"fixture\"",
            16,
            &[7; 16],
        ));
        for (ranges, cap, parallel) in [
            (vec![0..16, 0..16], 32, 2),
            (vec![0..16, 8..24], 32, 2),
            (vec![16..32, 0..16], 32, 2),
            (std::iter::once(0..16).collect(), 15, 1),
            (std::iter::once(0..65).collect(), 65, 1),
            (std::iter::once(0..16).collect(), 16, 0),
        ] {
            let error = fetch_capsule_ranges(
                reader.store(),
                &Path::from("gen/plane/capsules.bin"),
                64,
                &ranges,
                "\"fixture\"",
                2,
                cap,
                parallel,
                None,
                |_, _| Ok(()),
            )
            .await
            .unwrap_err();
            assert_eq!(error.stats, Sq8ReadStats::default());
        }
        let window = InFlightBytes::new(15);
        assert!(
            fetch_capsule_ranges(
                reader.store(),
                &Path::from("gen/plane/capsules.bin"),
                64,
                std::slice::from_ref(&(0..16)),
                "\"fixture\"",
                1,
                16,
                1,
                Some(&window),
                |_, _| Ok(())
            )
            .await
            .is_err()
        );
        stop.store(true, Ordering::Relaxed);
        server.join().unwrap();
        assert!(requests.lock().unwrap().is_empty());
        assert_eq!(*window.available.lock().unwrap(), 15);
    }

    fn framed_source_fixture(
        count: usize,
    ) -> (
        Arc<crate::source_frame::Directory>,
        Arc<PageAuthority>,
        Vec<u8>,
        Vec<std::ops::Range<usize>>,
    ) {
        use crate::source_frame::{Directory, Limits, encode};
        let rows = count * 256;
        let original = vec![7u8; rows * 9];
        let sidecar = original
            .chunks(32 * 9)
            .flat_map(|p| Sha256::digest(p).to_vec())
            .collect::<Vec<_>>();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-two-bit-plane-v3","rows":rows,"dimensions":1,"seed":20260923,
            "record_bytes":9,"page_rows":32,"source_sha256":"0".repeat(64),"sq8_sha256":"1".repeat(64),
            "source_order_sha256":"2".repeat(64),"mean_sha256":"3".repeat(64),
            "records_sha256":format!("{:x}",Sha256::digest(&original)),
            "page_digest_sha256":format!("{:x}",Sha256::digest(&sidecar)), "query_or_truth_used":false
        })).unwrap();
        let authority = PageAuthority::load_two_bit(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            2,
            &sidecar,
        )
        .unwrap();
        let mut blob = Vec::new();
        let mut offsets = Vec::with_capacity((count + 1) * 8);
        offsets.extend_from_slice(&0u64.to_le_bytes());
        for _ in 0..count {
            blob.extend(
                encode(
                    256,
                    9,
                    &original[..256 * 9],
                    Limits {
                        max_decoded_bytes: 1 << 20,
                    },
                )
                .unwrap(),
            );
            offsets.extend_from_slice(&(blob.len() as u64).to_le_bytes());
        }
        let directory =
            Directory::open(offsets, rows, 9, blob.len(), (count + 1) * 16 + 128).unwrap();
        let ranges = (0..count)
            .map(|i| directory.physical_range(i, i).unwrap())
            .collect();
        (Arc::new(directory), Arc::new(authority), blob, ranges)
    }

    #[tokio::test]
    async fn framed_source_http_ordered_errors_queued_drain_and_permits() {
        use super::cold_http_fixture::{ETAG, Fixture};
        let (directory, authority, blob, ranges) = framed_source_fixture(5);
        let total = blob.len();
        let window = InFlightBytes::new(ranges[0].len() * 2);
        let key = "framed/plane/frames.bin";
        let fixture = Fixture::new(
            std::collections::BTreeMap::from([(key.to_owned(), blob)]),
            Instant::now() + Duration::from_secs(10),
        );
        fixture.arm(Some((
            0,
            [ranges[0].start, ranges[1].start, ranges[2].start],
        )));
        let location = Path::from(key);
        let mut work = Box::pin(fetch_capsule_ranges(
            fixture.reader.store(),
            &location,
            total,
            &ranges,
            ETAG,
            5,
            total,
            2,
            Some(&window),
            |offset, bytes| {
                directory
                    .decode_physical(offset, &bytes, &authority)
                    .map_err(|_| RangeFetchError::UnexpectedMetadata)
            },
        ));
        tokio::select! {
            _ = &mut work => panic!("returned before first body"),
            () = fixture.wait(|s|s.finished[0].contains(&ranges[1].start)) => {}
        }
        fixture.release_error();
        tokio::select! {
            _ = &mut work => panic!("returned before queued drainage"),
            () = fixture.wait(|s|s.finished[0].contains(&ranges[3].start) && s.finished[0].contains(&ranges[4].start)) => {}
        }
        assert!(!fixture.snapshot().finished[0].contains(&ranges[2].start));
        fixture.release_sibling();
        let failure = work.await.err().unwrap();
        assert!(matches!(failure.error, RangeFetchError::Store(_)));
        assert_eq!(failure.stats.submitted_gets, 5);
        assert_eq!(failure.stats.failed_gets, 2);
        assert_eq!(failure.stats.verified_bytes, ranges[0].len() * 3);
        assert_eq!(*window.available.lock().unwrap(), ranges[0].len() * 2);
        assert_eq!(fixture.snapshot().finished[0].len(), 5);
        fixture.finish();
    }

    #[tokio::test]
    async fn framed_source_offloaded_cancel_joins_and_restores_bytes() {
        let _serial = SOURCE_AUTH_TEST_SERIAL.lock().await;
        let (directory, authority, blob, ranges) = framed_source_fixture(5);
        let total = blob.len();
        let store = InMemory::new();
        let location = Path::from("frames/plane/frames.bin");
        let etag = store
            .put(&location, blob.into())
            .await
            .unwrap()
            .e_tag
            .unwrap();
        let workers = SourceAuthWorkers::new(2).await.unwrap();
        let gate = Arc::new(ComputeGate::default());
        let window = InFlightBytes::new(total);
        let mut work = Box::pin(async {
            let workers = workers;
            fetch_capsule_ranges_with_authentication(
                &store,
                &location,
                total,
                &ranges,
                &etag,
                5,
                total,
                2,
                Some(&window),
                None,
                Some(&workers),
                |offset, bytes, probe, byte_lease| {
                    let gate = Arc::clone(&gate);
                    let directory = Arc::clone(&directory);
                    let authority = Arc::clone(&authority);
                    workers.authenticate(probe, move || {
                        let _lease = byte_lease;
                        gate.hold();
                        directory
                            .decode_physical(offset, &bytes, &authority)
                            .map_err(|_| RangeFetchError::UnexpectedMetadata)
                    })
                },
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(3),async {
            loop {
                tokio::select! {
                    _ = &mut work => panic!("held work completed"),
                    () = tokio::task::yield_now() => if gate.active.load(std::sync::atomic::Ordering::SeqCst)==2 { break; }
                }
            }
        }).await.unwrap();
        let release = Arc::clone(&gate);
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            release.release();
        });
        drop(work);
        releaser.join().unwrap();
        assert_eq!(*window.available.lock().unwrap(), total);
        let workers = SourceAuthWorkers::new(2).await.unwrap();
        assert_eq!(workers.slots.available_permits(), 2);
    }

    #[tokio::test]
    async fn page_range_v2_capsule_http_queued_errors_full_drain_and_permits() {
        use super::cold_http_fixture::{ETAG, Fixture};
        let key = "gen/plane/capsules.bin";
        let body = vec![7; 80];
        let fixture = Fixture::new(
            std::collections::BTreeMap::from([(key.to_owned(), body)]),
            Instant::now() + Duration::from_secs(10),
        );
        let ranges = (0..5).map(|i| i * 16..(i + 1) * 16).collect::<Vec<_>>();
        let window = InFlightBytes::new(32);
        fixture.arm(Some((0, [0, 16, 32])));
        let location = Path::from(key);
        let mut work = Box::pin(fetch_capsule_ranges(
            fixture.reader.store(),
            &location,
            80,
            &ranges,
            ETAG,
            5,
            80,
            2,
            Some(&window),
            |_, bytes| {
                if bytes.iter().all(|b| *b == 7) {
                    Ok(())
                } else {
                    Err(RangeFetchError::UnexpectedMetadata)
                }
            },
        ));
        tokio::select! {
            _ = &mut work => panic!("returned before earliest planned body"),
            () = fixture.wait(|s| s.finished[0].contains(&16)) => {}
        }
        assert!(!fixture.snapshot().finished[0].contains(&0));
        fixture.release_error();
        tokio::select! {
            _ = &mut work => panic!("returned before held sibling drained"),
            () = fixture.wait(|s| s.finished[0].contains(&0)) => {}
        }
        tokio::select! {
            _ = &mut work => panic!("returned before queued ranges and held sibling drained"),
            () = fixture.wait(|s| s.finished[0].contains(&48) && s.finished[0].contains(&64)) => {}
        }
        assert!(!fixture.snapshot().finished[0].contains(&32));
        fixture.release_sibling();
        let error = work.await.unwrap_err();
        assert!(matches!(error.error, RangeFetchError::Store(_)));
        assert_eq!(
            error.stats,
            Sq8ReadStats {
                submitted_gets: 5,
                verified_bytes: 48,
                failed_gets: 2,
            }
        );
        let state = fixture.snapshot();
        assert_eq!(state.active[0], 0);
        assert_eq!(state.finished[0].len(), 5);
        assert_eq!(state.finished[0][0], 16);
        assert!(state.peak[0] <= 2);
        assert_eq!(*window.available.lock().unwrap(), 32);
        fixture.finish();
    }
    #[tokio::test]
    async fn page_range_v2_capsule_http_disjoint_original_units_and_drain() {
        use super::cold_http_fixture::{ETAG, Fixture};
        use crate::rotated_two_bit::RotatedTwoBitCodec;
        use crate::semantic_unit_router::SemanticProfile;
        use crate::source_capsule::{Directory, FetchedSpan, Geometry, LeafPages, pack};
        use crate::two_bit_generation::authenticate_capsule_response;
        use crate::two_bit_source::{SourceBuildError, SourcePlaneReceipt};
        use std::{collections::BTreeMap, io::Cursor};

        // Genuine codec capsules exercise the serving callback without inventing router geometry.
        let cap = 16 * 1024 * 1024;
        let rows = 513;
        let codec = RotatedTwoBitCodec::new(&[0.], 20260923).unwrap();
        let flat = (0..rows)
            .flat_map(|row| codec.encode(&[(1 + row % 11) as f32]).unwrap())
            .collect::<Vec<_>>();
        let geometry = Geometry::new(rows, 1, SemanticProfile::Scale1m).unwrap();
        let width = geometry.record_bytes();
        let sidecar = flat
            .chunks(32 * width)
            .flat_map(|bytes| Sha256::digest(bytes).to_vec())
            .collect::<Vec<_>>();
        let hash = |bytes: &[u8]| format!("{:x}", Sha256::digest(bytes));
        let receipt = SourcePlaneReceipt {
            schema: "borsuk-two-bit-plane-v3".into(),
            rows,
            dimensions: 1,
            seed: 20260923,
            record_bytes: width,
            source_sha256: "0".repeat(64),
            sq8_sha256: "1".repeat(64),
            source_order_sha256: "2".repeat(64),
            mean_sha256: "3".repeat(64),
            records_sha256: hash(&flat),
            page_rows: 32,
            page_digest_sha256: hash(&sidecar),
            query_or_truth_used: false,
        };
        let manifest = serde_json::to_vec(&receipt).unwrap();
        let authority =
            PageAuthority::load_two_bit(&manifest, &hash(&manifest), 7, &sidecar).unwrap();
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[0],
            },
            LeafPages {
                leaf_id: 2,
                pages: &[1],
            },
            LeafPages {
                leaf_id: 3,
                pages: &[1],
            },
            LeafPages {
                leaf_id: 4,
                pages: &[2],
            },
        ];
        let mut blob = Vec::new();
        let mut encoded = Vec::new();
        let built = pack(
            geometry,
            &leaves,
            &authority,
            &mut Cursor::new(&flat),
            &mut blob,
            &mut encoded,
            cap,
        )
        .unwrap();
        let selected = [leaves[0], leaves[2], leaves[4]];
        let plan = built
            .directory
            .plan_page_ranges(&[0, 2, 4], &[0, 1, 2], &authority, cap)
            .unwrap();
        let ranges = plan.ranges().to_vec();
        assert!(ranges.windows(2).all(|pair| pair[0].end < pair[1].start));
        let physical = ranges.iter().map(|range| range.len()).sum::<usize>();
        let window = InFlightBytes::new(physical);
        let key = "disjoint/plane/capsules.bin";
        let location = Path::from(key);
        let fixture = Fixture::new(
            BTreeMap::from([(key.to_owned(), blob.clone())]),
            Instant::now() + Duration::from_secs(10),
        );
        // Hold the earliest response while both later selected capsules finish.
        fixture.arm(Some((0, [usize::MAX, usize::MAX, ranges[0].start])));
        let mut work = Box::pin(fetch_capsule_ranges(
            fixture.reader.store(),
            &location,
            blob.len(),
            &ranges,
            ETAG,
            3,
            physical,
            3,
            Some(&window),
            |offset, bytes| authenticate_capsule_response(&plan, cap, offset, bytes),
        ));
        tokio::select! {
            _ = &mut work => panic!("returned before earliest selected capsule"),
            () = fixture.wait(|s| s.finished[0].contains(&ranges[1].start)
                && s.finished[0].contains(&ranges[2].start)) => {}
        }
        assert!(!fixture.snapshot().finished[0].contains(&ranges[0].start));
        fixture.release_sibling();
        let (fetched, stats) = work.await.unwrap();
        assert_eq!(
            stats,
            Sq8ReadStats {
                submitted_gets: 3,
                verified_bytes: physical,
                failed_gets: 0,
            }
        );
        assert_eq!(
            fetched
                .iter()
                .map(|range| range.start() as usize)
                .collect::<Vec<_>>(),
            ranges.iter().map(|range| range.start).collect::<Vec<_>>(),
        );
        let verified = plan.verified_rows(&fetched, cap).unwrap();
        assert_eq!(verified.record(0), Some(&flat[..width]));
        assert_eq!(verified.record(512), Some(&flat[512 * width..513 * width]));
        assert_eq!(verified.record(256), Some(&flat[256 * width..257 * width]));
        assert_eq!(verified.record(511), Some(&flat[511 * width..512 * width]));
        // All original pages are fetched exactly once; endpoint framing is omitted.
        assert_eq!(physical, 513 * width);
        assert_eq!(verified.charge().hash_bytes, physical);
        assert_eq!(verified.charge().framing_bytes, 0);
        assert_eq!(*window.available.lock().unwrap(), physical);
        assert_eq!(fixture.snapshot().finished[0].len(), 3);
        assert!(
            fixture
                .snapshot()
                .requests
                .iter()
                .all(|r| r.0 == key && !r.1)
        );
        fixture.finish();

        // Rebind the outer hash after corrupting a required original page.
        let mut corrupt = blob.clone();
        corrupt[ranges[1].start] ^= 1;
        let full_range = built.directory.byte_range(2).unwrap();
        let full_range = full_range.start as usize..full_range.end as usize;
        let mut rebound = encoded;
        let hash_offset = 32 + 2 * 64 + 32;
        rebound[hash_offset..hash_offset + 32]
            .copy_from_slice(&Sha256::digest(&corrupt[full_range.clone()]));
        let digest: [u8; 32] = Sha256::digest(&rebound).into();
        let directory =
            Directory::open(&rebound, &digest, geometry, corrupt.len(), 7, cap).unwrap();
        let bad_copy = corrupt[full_range.clone()].to_vec();
        assert!(matches!(
            directory.verify(
                &selected[1..2],
                &[FetchedSpan {
                    offset: full_range.start as u64,
                    bytes: &bad_copy
                }],
                &authority,
                cap,
            ),
            Err(SourceBuildError::Invalid("capsule original unit digest")),
        ));
        let corrupt_plan = directory
            .plan_page_ranges(&[0, 2, 4], &[0, 1, 2], &authority, cap)
            .unwrap();
        assert_eq!(corrupt_plan.ranges(), ranges);
        for stream_failure in [false, true] {
            let fixture = Fixture::new(
                BTreeMap::from([(key.to_owned(), corrupt.clone())]),
                Instant::now() + Duration::from_secs(10),
            );
            // Leave the corrupt body untouched by the fixture; its original unit SHA fails.
            fixture.arm(Some((
                0,
                [
                    if stream_failure {
                        ranges[0].start
                    } else {
                        usize::MAX
                    },
                    usize::MAX,
                    ranges[2].start,
                ],
            )));
            let authenticated = Mutex::new(Vec::new());
            let mut work = Box::pin(fetch_capsule_ranges(
                fixture.reader.store(),
                &location,
                corrupt.len(),
                &ranges,
                ETAG,
                3,
                physical,
                3,
                Some(&window),
                |offset, bytes| {
                    let result = authenticate_capsule_response(&corrupt_plan, cap, offset, bytes);
                    authenticated.lock().unwrap().push((offset, result.is_ok()));
                    result
                },
            ));
            tokio::select! {
                _ = &mut work => panic!("returned before held valid sibling"),
                () = fixture.wait(|s| s.finished[0].contains(&ranges[1].start)
                    && s.active[0] >= 1
                    && authenticated.lock().unwrap().contains(&(ranges[1].start, false))) => {}
            }
            if stream_failure {
                assert!(!fixture.snapshot().finished[0].contains(&ranges[0].start));
                fixture.release_error();
                tokio::select! {
                    _ = &mut work => panic!("returned before full capsule drain"),
                    () = fixture.wait(|s| s.finished[0].contains(&ranges[0].start)) => {}
                }
            } else {
                tokio::select! {
                    _ = &mut work => panic!("accepted corrupt required page"),
                    () = fixture.wait(|_| authenticated.lock().unwrap()
                        .contains(&(ranges[0].start, true))) => {}
                }
            }
            tokio::select! {
                _ = &mut work => panic!("cancelled held valid capsule"),
                () = tokio::time::sleep(Duration::from_millis(30)) => {}
            }
            fixture.release_sibling();
            let error = work.await.unwrap_err();
            if stream_failure {
                assert!(matches!(error.error, RangeFetchError::Store(_)));
            } else {
                assert!(matches!(error.error, RangeFetchError::UnexpectedMetadata));
            }
            assert_eq!(
                error.stats,
                Sq8ReadStats {
                    submitted_gets: 3,
                    verified_bytes: ranges[2].len()
                        + if stream_failure { 0 } else { ranges[0].len() },
                    failed_gets: if stream_failure { 2 } else { 1 },
                }
            );
            let state = fixture.snapshot();
            assert_eq!(state.active, [0; 3]);
            assert_eq!(state.finished[0].len(), 3);
            assert!(state.peak[0] <= 3);
            assert_eq!(state.requests.len(), 3);
            assert!(
                state
                    .requests
                    .iter()
                    .all(|r| r.0 == key && !r.1 && r.3.as_deref() == Some(ETAG))
            );
            assert!(
                authenticated
                    .lock()
                    .unwrap()
                    .contains(&(ranges[2].start, true))
            );
            assert_eq!(*window.available.lock().unwrap(), physical);
            fixture.finish();
        }
    }

    #[test]
    fn capsule_transport_future_is_send_and_fits_scheduler_allowance() {
        fn require_send<T: Send>(_: T) {}
        let store = InMemory::new();
        let location = Path::from("gen/plane/capsules.bin");
        let window = InFlightBytes::new(16);
        fn authenticate(
            _: usize,
            _: Vec<u8>,
        ) -> Result<crate::source_capsule::VerifiedPageSpan<'static, 'static>, RangeFetchError>
        {
            Err(RangeFetchError::UnexpectedMetadata)
        }
        let authenticate_async = |offset, bytes, probe, byte_lease| {
            std::future::ready(authenticate_inline(probe, || {
                let _byte_lease = byte_lease;
                authenticate(offset, bytes)
            }))
        };
        let reader = CapsuleRangeReader {
            store: &store,
            location: &location,
            object_bytes: 16,
            etag: "etag",
            max_bytes: 16,
            budget: Some(&window),
            authenticate: &authenticate_async,
            workers: None,
            origin: None,
        };
        let range = 0..16;
        // This unboxed future is the exact element held by buffer_unordered.
        let future = reader.fetch_range(0, &range);
        let future_bytes = std::mem::size_of_val(&future);
        println!(
            "capsule_range_future_bytes={future_bytes} scheduler_allowance_bytes={} node_allowance_bytes={} allocation_allowance_bytes={}",
            RANGE_FUTURE_ALLOWANCE_BYTES, BUFFERED_NODE_ALLOWANCE_BYTES, ALLOCATION_ALLOWANCE_BYTES,
        );
        assert!(
            future_bytes + BUFFERED_NODE_ALLOWANCE_BYTES + ALLOCATION_ALLOWANCE_BYTES
                <= RANGE_FUTURE_ALLOWANCE_BYTES,
            "{future_bytes} byte capsule range future exceeds scheduler allowance"
        );
        require_send(future);
        require_send(fetch_capsule_ranges(
            &store,
            &location,
            16,
            std::slice::from_ref(&(0..16)),
            "etag",
            1,
            16,
            1,
            None,
            authenticate,
        ));
    }
}
