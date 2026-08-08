use std::{
    collections::{BTreeMap, BTreeSet},
    env, fmt,
    fs::{self, File, OpenOptions},
    future::Future,
    io::{self, Write},
    ops::Range,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use arrow_array::RecordBatch;
use bytes::Bytes;
use futures_util::{FutureExt, StreamExt, TryStreamExt, future::try_join_all, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, UpdateVersion, parse_url_opts, path::Path as ObjectPath,
};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use rayon::prelude::*;
use tokio::{
    runtime::{Builder, Handle, Runtime, RuntimeFlavor},
    sync::Semaphore,
    task::JoinHandle,
};
use url::Url;

use crate::{
    collection_control::{
        COLLECTION_CURRENT, COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD,
        COLLECTION_WAL_FRONTIER_SHARDS, COLLECTION_WAL_FRONTIER_SOFT_TRANSACTIONS_PER_SHARD,
        COLLECTION_WAL_RESERVATION_TTL_MS, CollectionCommit, CollectionCurrent,
        CollectionManifestRef, CollectionSnapshot, CollectionWalFrontierHead,
        CollectionWalReservation, PendingCollectionCommit, collection_current_bytes,
        collection_current_from_slice, collection_modality_prefix, collection_snapshot_bytes,
        collection_snapshot_from_slice, collection_wal_frontier_head_bytes,
        collection_wal_frontier_head_from_slice, collection_wal_frontier_head_path,
        collection_wal_frontier_shard, consumed_wal_frontier_checksum,
        pending_collection_commit_bytes, pending_collection_commit_from_slice,
        pending_collection_commit_path, validate_collection_manifest_ref,
    },
    error::{BorsukError, Result},
    format::{
        manifest_from_parquet, manifest_has_next_generated_id, manifest_metadata_from_parquet,
        manifest_to_parquet, pivots_from_parquet, pivots_to_parquet,
        routing_layer_page_index_from_parquet, routing_layer_page_index_to_parquet,
        routing_layer_page_to_parquet, routing_to_parquet,
    },
    manifest::{Manifest, RoutingLayerPageRef, SegmentSummary},
    observability,
    record::RequestCounts,
    segment_cache::DecodedObjectCache,
    storage_trace::{
        StorageAccessEvent, StorageAccessTrace, configured_storage_access_trace,
        physical_format_for_path,
    },
};

const MULTIPART_WRITE_THRESHOLD_BYTES: usize = 64 * 1024 * 1024;
const MULTIPART_PART_BYTES: usize = 8 * 1024 * 1024;
const RESIDENT_ROUTING_ESTIMATE_SLACK_BYTES: u64 = 4 * 1024;
const PENDING_COLLECTION_COMMIT_HARD_BOUND: usize = 2_000;

pub(crate) fn collection_wal_now_ms() -> Result<u64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            BorsukError::InvalidStorage(format!("system clock precedes the Unix epoch: {error}"))
        })?;
    u64::try_from(elapsed.as_millis()).map_err(|_| {
        BorsukError::InvalidStorage("system clock milliseconds exceed u64".to_string())
    })
}

// Keep nearby exact-vector rows together, but never let a scattered candidate
// set turn into a whole-sidecar transfer. A four-megabyte physical cap keeps
// reranks bounded for 768d/1536d vectors while still amortizing adjacent rows;
// larger spans are split into independently parallel range GETs.
// Sidecar candidate ranges are already bounded Arrow record batches.  Merging
// across larger gaps turns sparse reranks into near-full-object reads on a
// cold handle, so only coalesce adjacent batches with a small IPC boundary.
const SIDECAR_RANGE_COALESCE_BYTES: u64 = 64 * 1024;
const SIDECAR_MAX_PHYSICAL_RANGE_BYTES: u64 = 4 * 1024 * 1024;
const SIDECAR_RANGE_MAX_PARALLEL: usize = 10;
// Global exact reranks commonly need 12-24 tiny, scattered rows from one
// immutable bundle. Issue the complete bounded shortlist in one S3 wave
// instead of serializing after ten.
const GLOBAL_RERANK_RANGE_MAX_PARALLEL: usize = 32;
// Global reranks retain the generic sidecar's small coalescing gap. A wider
// one-megabyte policy reduced request count but multiplied uncached AWS bytes
// and worsened tail latency for scattered exact rows. The separate 32-request
// wave overlaps those sparse reads without turning them into bulk transfer.
const GLOBAL_RERANK_RANGE_COALESCE_BYTES: u64 = SIDECAR_RANGE_COALESCE_BYTES;
static COORDINATION_FALLBACK_LOCK: Mutex<()> = Mutex::new(());

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

fn join_usize(values: &[usize]) -> String {
    values
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join("|")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedRangeSlice {
    physical_index: usize,
    range: Range<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BoundedRangePlan {
    physical: Vec<Range<u64>>,
    slices: Vec<PlannedRangeSlice>,
}

fn push_planned_range(
    physical: &mut Vec<Range<u64>>,
    slices: &mut [Option<PlannedRangeSlice>],
    start: u64,
    end: u64,
    members: &[(usize, Range<u64>)],
) {
    let physical_index = physical.len();
    physical.push(start..end);
    for (input_index, range) in members {
        slices[*input_index] = Some(PlannedRangeSlice {
            physical_index,
            range: (range.start - start) as usize..(range.end - start) as usize,
        });
    }
}

fn plan_bounded_ranges(
    ranges: &[Range<u64>],
    max_gap: u64,
    max_physical_range: u64,
) -> BoundedRangePlan {
    if ranges.is_empty() {
        return BoundedRangePlan {
            physical: Vec::new(),
            slices: Vec::new(),
        };
    }

    let mut sorted = ranges.iter().cloned().enumerate().collect::<Vec<_>>();
    sorted.sort_unstable_by_key(|(_, range)| (range.start, range.end));

    let mut physical = Vec::with_capacity(ranges.len());
    let mut slices = vec![None; ranges.len()];
    let mut group_start = sorted[0].1.start;
    let mut group_end = sorted[0].1.end;
    let mut members = vec![sorted[0].clone()];

    for (input_index, range) in sorted.into_iter().skip(1) {
        let candidate_end = group_end.max(range.end);
        let gap = range.start.saturating_sub(group_end);
        let candidate_span = candidate_end.saturating_sub(group_start);
        if gap <= max_gap && candidate_span <= max_physical_range {
            group_end = candidate_end;
            members.push((input_index, range));
        } else {
            push_planned_range(&mut physical, &mut slices, group_start, group_end, &members);
            group_start = range.start;
            group_end = range.end;
            members.clear();
            members.push((input_index, range));
        }
    }
    push_planned_range(&mut physical, &mut slices, group_start, group_end, &members);

    BoundedRangePlan {
        physical,
        slices: slices
            .into_iter()
            .map(|slice| slice.expect("every input range must be assigned"))
            .collect(),
    }
}

async fn coalesce_bounded_ranges<F, E, Fut>(
    ranges: &[Range<u64>],
    mut fetch: F,
    max_gap: u64,
    max_physical_range: u64,
    max_parallel: usize,
) -> std::result::Result<Vec<Bytes>, E>
where
    F: Send + FnMut(Range<u64>) -> Fut,
    E: Send,
    Fut: Future<Output = std::result::Result<Bytes, E>> + Send,
{
    let plan = plan_bounded_ranges(ranges, max_gap, max_physical_range);
    let fetched = futures_util::stream::iter(plan.physical.iter().cloned())
        .map(&mut fetch)
        .buffered(max_parallel.max(1))
        .try_collect::<Vec<_>>()
        .await?;

    Ok(plan
        .slices
        .into_iter()
        .map(|slice| {
            let bytes = &fetched[slice.physical_index];
            let start = slice.range.start.min(bytes.len());
            let end = slice.range.end.min(bytes.len());
            bytes.slice(start..end)
        })
        .collect())
}

/// Atomic per-operation object-store request tallies shared by every clone of the
/// wrapped store, so parallel prefetch tasks and the main runtime accumulate into
/// one place. Snapshot into [`RequestCounts`] to report deltas around an operation.
#[derive(Debug, Default)]
pub(crate) struct RequestCounters {
    gets: AtomicU64,
    puts: AtomicU64,
    deletes: AtomicU64,
    heads: AtomicU64,
    lists: AtomicU64,
}

/// Successful bytes served by the local read-through cache versus the backing
/// object store. These counters sit below every index search path so range
/// reads from the resident global index are measured too.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CacheReadCounts {
    pub(crate) disk_reads: u64,
    pub(crate) disk_bytes: u64,
    pub(crate) backing_reads: u64,
    pub(crate) backing_bytes: u64,
}

impl CacheReadCounts {
    pub(crate) fn delta(&self, earlier: &Self) -> Self {
        Self {
            disk_reads: self.disk_reads.saturating_sub(earlier.disk_reads),
            disk_bytes: self.disk_bytes.saturating_sub(earlier.disk_bytes),
            backing_reads: self.backing_reads.saturating_sub(earlier.backing_reads),
            backing_bytes: self.backing_bytes.saturating_sub(earlier.backing_bytes),
        }
    }
}

#[derive(Debug, Default)]
struct CacheReadCounters {
    disk_reads: AtomicU64,
    disk_bytes: AtomicU64,
    backing_reads: AtomicU64,
    backing_bytes: AtomicU64,
}

impl CacheReadCounters {
    fn snapshot(&self) -> CacheReadCounts {
        CacheReadCounts {
            disk_reads: self.disk_reads.load(Ordering::Relaxed),
            disk_bytes: self.disk_bytes.load(Ordering::Relaxed),
            backing_reads: self.backing_reads.load(Ordering::Relaxed),
            backing_bytes: self.backing_bytes.load(Ordering::Relaxed),
        }
    }

    fn record_disk(&self, bytes: usize) {
        self.disk_reads.fetch_add(1, Ordering::Relaxed);
        self.disk_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn record_backing(&self, bytes: u64) {
        self.backing_reads.fetch_add(1, Ordering::Relaxed);
        self.backing_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl RequestCounters {
    fn snapshot(&self) -> RequestCounts {
        RequestCounts {
            gets: self.gets.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            heads: self.heads.load(Ordering::Relaxed),
            lists: self.lists.load(Ordering::Relaxed),
        }
    }
}

/// Object-store decorator that tallies every request it forwards to the inner
/// store. Counting at the store boundary captures all reads, writes, and retries
/// regardless of which higher-level storage helper issued them. HEAD probes ride
/// on `get_opts` with `options.head`; deletes flow through `delete_stream`.
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<RequestCounters>,
    cache_read_counters: Arc<CacheReadCounters>,
}

impl fmt::Debug for CountingObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CountingObjectStore")
            .field("inner", &self.inner)
            .finish()
    }
}

impl fmt::Display for CountingObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CountingObjectStore({})", self.inner)
    }
}

impl ObjectStore for CountingObjectStore {
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> BoxFuture<'async_trait, object_store::Result<PutResult>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move {
            self.counters.puts.fetch_add(1, Ordering::Relaxed);
            self.inner.put_opts(location, payload, opts).await
        })
    }

    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        opts: PutMultipartOptions,
    ) -> BoxFuture<'async_trait, object_store::Result<Box<dyn MultipartUpload>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move {
            self.counters.puts.fetch_add(1, Ordering::Relaxed);
            self.inner.put_multipart_opts(location, opts).await
        })
    }

    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        options: GetOptions,
    ) -> BoxFuture<'async_trait, object_store::Result<GetResult>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move {
            if options.head {
                self.counters.heads.fetch_add(1, Ordering::Relaxed);
            } else {
                self.counters.gets.fetch_add(1, Ordering::Relaxed);
            }
            let is_head = options.head;
            let result = self.inner.get_opts(location, options).await;
            if !is_head && let Ok(read) = &result {
                self.cache_read_counters
                    .record_backing(read.range.end.saturating_sub(read.range.start));
            }
            result
        })
    }

    fn get_ranges<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        ranges: &'life2 [Range<u64>],
    ) -> BoxFuture<'async_trait, object_store::Result<Vec<Bytes>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move {
            object_store::coalesce_ranges(
                ranges,
                |range| self.get_range(location, range),
                object_store::OBJECT_STORE_COALESCE_DEFAULT,
            )
            .await
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        let counters = Arc::clone(&self.counters);
        let counted = locations
            .map(move |location| {
                if location.is_ok() {
                    counters.deletes.fetch_add(1, Ordering::Relaxed);
                }
                location
            })
            .boxed();
        self.inner.delete_stream(counted)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.counters.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 ObjectPath>,
    ) -> BoxFuture<'async_trait, object_store::Result<ListResult>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move {
            self.counters.lists.fetch_add(1, Ordering::Relaxed);
            self.inner.list_with_delimiter(prefix).await
        })
    }

    fn copy_opts<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 ObjectPath,
        to: &'life2 ObjectPath,
        options: CopyOptions,
    ) -> BoxFuture<'async_trait, object_store::Result<()>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move {
            self.counters.puts.fetch_add(1, Ordering::Relaxed);
            self.inner.copy_opts(from, to, options).await
        })
    }

    fn rename_opts<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 ObjectPath,
        to: &'life2 ObjectPath,
        options: RenameOptions,
    ) -> BoxFuture<'async_trait, object_store::Result<()>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move {
            self.counters.puts.fetch_add(1, Ordering::Relaxed);
            self.inner.rename_opts(from, to, options).await
        })
    }
}

/// Dedicated storage runtime that can be driven safely from synchronous
/// callers even when the host is already executing inside Tokio.
///
/// The public BORSUK API is synchronous. Language bindings and async services
/// commonly invoke it from a Tokio worker, where calling `Runtime::block_on`
/// directly would panic. Multi-thread runtimes provide `block_in_place` for
/// this exact bridge; current-thread hosts use a scoped helper thread.
struct BlockingRuntime {
    inner: Option<Runtime>,
}

impl BlockingRuntime {
    fn new(inner: Runtime) -> Self {
        Self { inner: Some(inner) }
    }

    fn runtime(&self) -> &Runtime {
        self.inner
            .as_ref()
            .expect("storage runtime is available until drop")
    }

    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime().spawn(future)
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        match Handle::try_current().map(|handle| handle.runtime_flavor()) {
            Err(_) => self.runtime().block_on(future),
            Ok(RuntimeFlavor::MultiThread) => {
                tokio::task::block_in_place(|| self.runtime().block_on(future))
            }
            Ok(_) => std::thread::scope(|scope| {
                scope
                    .spawn(|| self.runtime().block_on(future))
                    .join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            }),
        }
    }
}

impl Drop for BlockingRuntime {
    fn drop(&mut self) {
        let Some(runtime) = self.inner.take() else {
            return;
        };
        if Handle::try_current().is_ok() {
            runtime.shutdown_background();
        } else {
            drop(runtime);
        }
    }
}

#[derive(Clone)]
pub(crate) struct Storage {
    uri: String,
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    cache_dir: Option<PathBuf>,
    cache_max_bytes: Option<u64>,
    runtime: Arc<BlockingRuntime>,
    request_counters: Arc<RequestCounters>,
    cache_read_counters: Arc<CacheReadCounters>,
    storage_trace: StorageAccessTrace,
    immutable_object_sizes: Arc<DecodedObjectCache<u64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreateOutcome {
    Created,
    Existing,
}

impl Storage {
    pub(crate) fn clone_with_independent_request_counters(&self) -> Self {
        self.isolated_read_scope()
    }

    pub(crate) fn clone_with_request_counters_from(&self, source: &Self) -> Self {
        self.with_read_scope_of(source)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredObject {
    pub path: String,
    pub size: u64,
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadBytes {
    pub bytes: Vec<u8>,
    pub cache_hit: bool,
    pub cache_repaired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadRanges {
    pub chunks: Vec<Vec<u8>>,
    pub cache_hit: bool,
    pub bytes_fetched: u64,
}

#[derive(Debug, Clone)]
#[allow(
    dead_code,
    reason = "connected to collection create/open in the following implementation task"
)]
pub(crate) struct StagedManifest {
    pub(crate) manifest: Manifest,
    pub(crate) reference: CollectionManifestRef,
}

#[derive(Clone)]
pub(crate) struct LoadedCollectionSnapshot {
    pub(crate) snapshot: CollectionSnapshot,
    pub(crate) checksum: String,
    pub(crate) current_version: UpdateVersion,
}

#[derive(Debug, Clone)]
pub(crate) struct CollectionWalReservationReceipt {
    shard: u8,
    head: CollectionWalFrontierHead,
    version: UpdateVersion,
    #[allow(
        dead_code,
        reason = "retained only until the v2 frontier admission primitive is deleted"
    )]
    pub(crate) admission_bytes_written: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct CollectionWalCommitOutcome {
    pub(crate) root_pressure: bool,
    #[allow(
        dead_code,
        reason = "retained only until the v2 frontier primitive is deleted in the format cutover"
    )]
    pub(crate) successor: Option<CollectionWalReservationReceipt>,
}

#[derive(Debug, Clone, Copy)]
struct ManifestTableChecksums {
    manifest: blake3::Hash,
    routing: blake3::Hash,
    pivots: blake3::Hash,
}

pub(crate) struct CoordinationObject {
    pub(crate) bytes: Vec<u8>,
    pub(crate) version: UpdateVersion,
}

/// Result of a projected, range-based Parquet read: the decoded batches for the
/// requested columns plus the object-store bytes those column chunks cost.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct RangedParquetRead {
    pub batches: Vec<RecordBatch>,
    pub bytes_fetched: u64,
    pub total_rows: usize,
}

/// Which columns a ranged Parquet read should fetch. `Keep` fetches exactly the
/// named columns; `DropVector` fetches everything except the big `vector`
/// column (scoring: ids, metadata, `pq_code`, bounds).
///
/// `Keep` is retained for tests that validate arbitrary-column ranged reads
/// against the segment's Parquet columns; the production rerank now range-reads
/// the dense-vector sidecar instead, so no non-test path constructs `Keep`.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum RangedColumns<'a> {
    #[cfg_attr(not(test), allow(dead_code))]
    Keep(&'a [&'a str]),
    DropVector,
}

/// A [`parquet`] `AsyncFileReader` backed by BORSUK's own object store handle, so
/// projected reads fetch only the needed column-chunk byte ranges without
/// coupling to `parquet`'s (older) bundled `object_store` version. The metadata
/// is pre-loaded from the footer, so the reader only ever issues data range GETs.
#[allow(dead_code)]
struct BorsukAsyncReader {
    context: PrefetchReadContext,
    relative: String,
    metadata: Arc<ParquetMetaData>,
    bytes_fetched: Arc<AtomicU64>,
}

impl AsyncFileReader for BorsukAsyncReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let context = self.context.clone();
        let relative = self.relative.clone();
        let counter = Arc::clone(&self.bytes_fetched);
        async move {
            let (bytes, cache_hit) = context
                .read_range_cached(&relative, range.clone())
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))?;
            if !cache_hit {
                counter.fetch_add(range.end - range.start, Ordering::Relaxed);
            }
            Ok(Bytes::from(bytes))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let context = self.context.clone();
        let relative = self.relative.clone();
        let counter = Arc::clone(&self.bytes_fetched);
        async move {
            let reads = ranges.into_iter().map(|range| {
                let context = context.clone();
                let relative = relative.clone();
                let counter = Arc::clone(&counter);
                async move {
                    let len = range.end - range.start;
                    let (bytes, cache_hit) = context.read_range_cached(&relative, range).await?;
                    if !cache_hit {
                        counter.fetch_add(len, Ordering::Relaxed);
                    }
                    Ok::<_, BorsukError>(Bytes::from(bytes))
                }
            });
            try_join_all(reads)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let metadata = Arc::clone(&self.metadata);
        async move { Ok(metadata) }.boxed()
    }
}

#[derive(Debug)]
pub(crate) struct PrefetchedRead {
    relative: String,
    handle: Option<JoinHandle<Result<ReadBytes>>>,
}

impl PrefetchedRead {
    pub(crate) fn relative(&self) -> &str {
        &self.relative
    }

    pub(crate) fn abort(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl Drop for PrefetchedRead {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

#[derive(Clone)]
struct PrefetchReadContext {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    cache_dir: Option<PathBuf>,
    cache_max_bytes: Option<u64>,
    request_counters: Arc<RequestCounters>,
    cache_read_counters: Arc<CacheReadCounters>,
    storage_trace: StorageAccessTrace,
    immutable_object_sizes: Arc<DecodedObjectCache<u64>>,
}

impl PrefetchReadContext {
    fn from_storage(storage: &Storage) -> Self {
        Self {
            store: Arc::clone(&storage.store),
            prefix: storage.prefix.clone(),
            cache_dir: storage.cache_dir.clone(),
            cache_max_bytes: storage.cache_max_bytes,
            request_counters: Arc::clone(&storage.request_counters),
            cache_read_counters: Arc::clone(&storage.cache_read_counters),
            storage_trace: storage.storage_trace.clone(),
            immutable_object_sizes: Arc::clone(&storage.immutable_object_sizes),
        }
    }

    async fn read_bytes_with_cache_status_and_checksum(
        &self,
        relative: &str,
        expected_checksum: &str,
    ) -> Result<ReadBytes> {
        let read = self.read_bytes_with_cache_status(relative).await?;
        let actual_checksum = blake3::hash(&read.bytes).to_hex().to_string();
        if actual_checksum == expected_checksum {
            return Ok(read);
        }
        if !read.cache_hit {
            return Err(BorsukError::ChecksumMismatch {
                path: relative.to_string(),
                expected: expected_checksum.to_string(),
                actual: actual_checksum,
            });
        }

        self.delete_cache_file(relative)?;
        let requests_before = self.request_counters.snapshot();
        let size = self.object_size(relative).await?;
        let (bytes, _) = self.read_range_uncached(relative, 0..size).await?;
        let actual_checksum = blake3::hash(&bytes).to_hex().to_string();
        if actual_checksum != expected_checksum {
            return Err(BorsukError::ChecksumMismatch {
                path: relative.to_string(),
                expected: expected_checksum.to_string(),
                actual: actual_checksum,
            });
        }
        self.write_cache_file(relative, &bytes)?;
        self.storage_trace
            .record(StorageAccessEvent::observed_read(
                relative,
                physical_format_for_path(relative),
                size,
                self.request_counters
                    .snapshot()
                    .delta(&requests_before)
                    .total(),
                bytes.len() as u64,
            ))?;
        Ok(ReadBytes {
            bytes,
            cache_hit: false,
            cache_repaired: true,
        })
    }

    async fn read_bytes_with_cache_status(&self, relative: &str) -> Result<ReadBytes> {
        if let Some(bytes) = self.read_cache_file(relative)? {
            self.storage_trace.record(StorageAccessEvent::cached_read(
                relative,
                physical_format_for_path(relative),
                bytes.len() as u64,
            ))?;
            return Ok(ReadBytes {
                bytes,
                cache_hit: true,
                cache_repaired: false,
            });
        }

        let requests_before = self.request_counters.snapshot();
        let location = self.resolve(relative)?;
        let result = self
            .store
            .get_opts(&location, GetOptions::default())
            .await
            .map_err(|err| map_object_store_error(relative, err))?;
        let size = result.meta.size;
        let bytes = result
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|err| map_object_store_error(relative, err))?;
        self.write_cache_file(relative, &bytes)?;
        self.storage_trace
            .record(StorageAccessEvent::observed_read(
                relative,
                physical_format_for_path(relative),
                size,
                self.request_counters
                    .snapshot()
                    .delta(&requests_before)
                    .total(),
                bytes.len() as u64,
            ))?;
        Ok(ReadBytes {
            bytes,
            cache_hit: false,
            cache_repaired: false,
        })
    }

    async fn object_size(&self, relative: &str) -> Result<u64> {
        if !is_mutable_lane_head(relative)
            && let Some(size) = self.immutable_object_sizes.get(relative)
        {
            return Ok(*size);
        }
        let location = self.resolve(relative)?;
        let meta = self
            .store
            .head(&location)
            .await
            .map_err(|err| map_object_store_error(relative, err))?;
        if !is_mutable_lane_head(relative) {
            self.immutable_object_sizes.insert(
                relative.to_string(),
                Arc::new(meta.size),
                std::mem::size_of::<u64>() as u64,
            );
        }
        Ok(meta.size)
    }

    async fn read_range_uncached(
        &self,
        relative: &str,
        range: Range<u64>,
    ) -> Result<(Vec<u8>, u64)> {
        let location = self.resolve(relative)?;
        let result = self
            .store
            .get_opts(
                &location,
                GetOptions::new().with_range(Some(GetRange::Bounded(range))),
            )
            .await
            .map_err(|err| map_object_store_error(relative, err))?;
        let object_bytes = result.meta.size;
        let bytes = result
            .bytes()
            .await
            .map_err(|err| map_object_store_error(relative, err))?;
        Ok((bytes.to_vec(), object_bytes))
    }

    async fn read_range_cached(
        &self,
        relative: &str,
        range: Range<u64>,
    ) -> Result<(Vec<u8>, bool)> {
        let cacheable = !is_mutable_lane_head(relative);
        if cacheable && let Some(bytes) = self.read_cache_file(relative)? {
            let start = usize::try_from(range.start).map_err(|_| {
                BorsukError::InvalidStorage("cached range start exceeds usize".to_string())
            })?;
            let end = usize::try_from(range.end).map_err(|_| {
                BorsukError::InvalidStorage("cached range end exceeds usize".to_string())
            })?;
            let slice = bytes.get(start..end).ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "cached range {}..{} is outside `{relative}`",
                    range.start, range.end
                ))
            })?;
            return Ok((slice.to_vec(), true));
        }
        let cache_key = range_cache_key(relative, range.start, range.end);
        if cacheable && let Some(bytes) = self.read_cache_file(&cache_key)? {
            return Ok((bytes, true));
        }
        let requested_bytes = range.end.saturating_sub(range.start);
        let (bytes, object_bytes) = self.read_range_uncached(relative, range).await?;
        if cacheable {
            self.write_cache_file(&cache_key, &bytes)?;
        }
        self.storage_trace
            .record(StorageAccessEvent::observed_read(
                relative,
                physical_format_for_path(relative),
                object_bytes,
                1,
                requested_bytes,
            ))?;
        Ok((bytes, false))
    }

    fn resolve(&self, relative: &str) -> Result<ObjectPath> {
        let relative = relative.trim_matches('/');
        let path = if self.prefix.as_ref().is_empty() {
            relative.to_string()
        } else if relative.is_empty() {
            self.prefix.as_ref().to_string()
        } else {
            format!("{}/{relative}", self.prefix.as_ref())
        };

        ObjectPath::parse(path).map_err(|err| {
            BorsukError::InvalidStorage(format!("invalid object path `{relative}`: {err}"))
        })
    }

    fn cache_path(&self, relative: &str) -> Option<PathBuf> {
        if is_mutable_lane_head(relative) {
            return None;
        }
        let cache_dir = self.cache_dir.as_ref()?;
        let mut path = cache_dir.clone();
        for component in Path::new(relative.trim_matches('/')).components() {
            if let std::path::Component::Normal(value) = component {
                path.push(value);
            }
        }
        Some(path)
    }

    fn read_cache_file(&self, relative: &str) -> Result<Option<Vec<u8>>> {
        let Some(path) = self.cache_path(relative) else {
            return Ok(None);
        };

        match fs::read(&path) {
            Ok(bytes) => {
                self.cache_read_counters.record_disk(bytes.len());
                // Recency refresh is best-effort; valid cached bytes remain usable.
                let _refresh_result = self.touch_cache_file(&path);
                Ok(Some(bytes))
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(BorsukError::InvalidStorage(format!(
                "failed to read cache file `{}`: {err}",
                path.display()
            ))),
        }
    }

    fn write_cache_file(&self, relative: &str, bytes: &[u8]) -> Result<()> {
        let Some(path) = self.cache_path(relative) else {
            return Ok(());
        };

        atomic_write_cache_file(&path, bytes)?;
        self.enforce_cache_max_bytes()
    }

    fn delete_cache_file(&self, relative: &str) -> Result<()> {
        let Some(path) = self.cache_path(relative) else {
            return Ok(());
        };

        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(BorsukError::InvalidStorage(format!(
                "failed to delete cache file `{}`: {err}",
                path.display()
            ))),
        }
    }

    fn touch_cache_file(&self, path: &Path) -> Result<()> {
        if self.cache_max_bytes.is_none() {
            return Ok(());
        }

        refresh_cache_file_mtime(path).map_err(|err| {
            BorsukError::InvalidStorage(format!(
                "failed to refresh cache file `{}`: {err}",
                path.display()
            ))
        })
    }

    fn enforce_cache_max_bytes(&self) -> Result<()> {
        enforce_cache_max_bytes(self.cache_dir.as_deref(), self.cache_max_bytes)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RoutingLayerPageIndexRead {
    pub page_refs: Vec<RoutingLayerPageRef>,
    pub bytes_read: u64,
    pub page_indexes_read: usize,
    pub object_cache_hits: usize,
    pub object_cache_misses: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StorageWriteReport {
    pub metadata_tables_written: usize,
    pub routing_pages_written: usize,
    pub bytes_written: u64,
}

impl StorageWriteReport {
    fn record_metadata_table(&mut self, bytes_len: usize) {
        self.metadata_tables_written += 1;
        self.bytes_written += bytes_len as u64;
    }

    fn record_routing_page(&mut self, bytes_len: usize) {
        self.routing_pages_written += 1;
        self.bytes_written += bytes_len as u64;
    }

    fn record_collection_snapshot(&mut self, snapshot_bytes: usize, current_bytes: usize) {
        self.metadata_tables_written += 1;
        self.bytes_written += snapshot_bytes.saturating_add(current_bytes) as u64;
    }
}

impl fmt::Debug for Storage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Storage")
            .field("uri", &self.uri)
            .field("prefix", &self.prefix)
            .field("cache_dir", &self.cache_dir)
            .field("cache_max_bytes", &self.cache_max_bytes)
            .finish_non_exhaustive()
    }
}

impl Storage {
    pub(crate) fn from_uri(uri: &str) -> Result<Self> {
        Self::from_uri_with_cache(uri, None)
    }

    pub(crate) fn from_uri_with_cache(uri: &str, cache_dir: Option<PathBuf>) -> Result<Self> {
        Self::from_uri_with_cache_and_max(uri, cache_dir, None)
    }

    pub(crate) fn from_uri_with_cache_and_max(
        uri: &str,
        cache_dir: Option<PathBuf>,
        cache_max_bytes: Option<u64>,
    ) -> Result<Self> {
        let (store, prefix) = store_from_uri(uri)?;
        Self::from_parts(uri.to_string(), store, prefix, cache_dir, cache_max_bytes)
    }

    pub(crate) fn from_object_store(uri: String, store: Arc<dyn ObjectStore>) -> Result<Self> {
        let prefix = ObjectPath::parse("").map_err(|err| {
            BorsukError::InvalidStorage(format!("invalid injected storage root `{uri}`: {err}"))
        })?;
        Self::from_parts(uri, store, prefix, None, None)
    }

    pub(crate) fn child(&self, uri: String, name: &str) -> Result<Self> {
        let relative = format!("vectors/{name}");
        let prefix = if self.prefix.as_ref().is_empty() {
            relative
        } else {
            format!("{}/{relative}", self.prefix.as_ref())
        };
        let prefix = ObjectPath::parse(prefix).map_err(|err| {
            BorsukError::InvalidStorage(format!(
                "invalid child index object prefix for named vector `{name}`: {err}"
            ))
        })?;
        let cache_dir = self.cache_dir.as_ref().map(|root| {
            let mut path = root.clone();
            path.push("vectors");
            path.push(name);
            path
        });

        Ok(Self {
            uri,
            store: Arc::clone(&self.store),
            prefix,
            cache_dir,
            cache_max_bytes: self.cache_max_bytes,
            runtime: Arc::clone(&self.runtime),
            request_counters: Arc::clone(&self.request_counters),
            cache_read_counters: Arc::clone(&self.cache_read_counters),
            storage_trace: self.storage_trace.clone(),
            immutable_object_sizes: Arc::clone(&self.immutable_object_sizes),
        })
    }

    /// Return a shallow storage handle whose request and cache-tier counters
    /// belong only to one logical read operation. The existing counted store
    /// remains underneath this decorator, preserving lifetime totals, while the
    /// outer decorator prevents overlapping searches from attributing each
    /// other's I/O to their per-query reports.
    pub(crate) fn isolated_read_scope(&self) -> Self {
        let request_counters = Arc::new(RequestCounters::default());
        let cache_read_counters = Arc::new(CacheReadCounters::default());
        self.with_read_scope_counters(request_counters, cache_read_counters)
    }

    /// Join the same logical read scope as `scope`, retaining this handle's URI,
    /// prefix, and cache path. Named-vector child indexes use this so all work
    /// for one query lands in one report even though their storage prefixes
    /// differ.
    pub(crate) fn with_read_scope_of(&self, scope: &Self) -> Self {
        self.with_read_scope_counters(
            Arc::clone(&scope.request_counters),
            Arc::clone(&scope.cache_read_counters),
        )
    }

    fn with_read_scope_counters(
        &self,
        request_counters: Arc<RequestCounters>,
        cache_read_counters: Arc<CacheReadCounters>,
    ) -> Self {
        let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore {
            inner: Arc::clone(&self.store),
            counters: Arc::clone(&request_counters),
            cache_read_counters: Arc::clone(&cache_read_counters),
        });
        Self {
            uri: self.uri.clone(),
            store,
            prefix: self.prefix.clone(),
            cache_dir: self.cache_dir.clone(),
            cache_max_bytes: self.cache_max_bytes,
            runtime: Arc::clone(&self.runtime),
            request_counters,
            cache_read_counters,
            storage_trace: self.storage_trace.clone(),
            immutable_object_sizes: Arc::clone(&self.immutable_object_sizes),
        }
    }

    fn from_parts(
        uri: String,
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
        cache_dir: Option<PathBuf>,
        cache_max_bytes: Option<u64>,
    ) -> Result<Self> {
        let cpu_threads = crate::configured_cpu_threads();
        let runtime = Builder::new_multi_thread()
            .worker_threads(cpu_threads)
            .max_blocking_threads(cpu_threads)
            .enable_all()
            .build()
            .map_err(|err| {
                BorsukError::InvalidStorage(format!("failed to create storage runtime: {err}"))
            })?;

        let request_counters = Arc::new(RequestCounters::default());
        let cache_read_counters = Arc::new(CacheReadCounters::default());
        let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore {
            inner: store,
            counters: Arc::clone(&request_counters),
            cache_read_counters: Arc::clone(&cache_read_counters),
        });

        Ok(Self {
            uri,
            store,
            prefix,
            cache_dir,
            cache_max_bytes,
            runtime: Arc::new(BlockingRuntime::new(runtime)),
            request_counters,
            cache_read_counters,
            storage_trace: configured_storage_access_trace()?,
            immutable_object_sizes: Arc::new(DecodedObjectCache::new(1 << 20)),
        })
    }

    /// Snapshot of object-store requests issued since this handle was opened.
    /// Callers diff two snapshots to attribute requests to a single operation.
    pub(crate) fn request_counts(&self) -> RequestCounts {
        self.request_counters.snapshot()
    }

    pub(crate) fn cache_read_counts(&self) -> CacheReadCounts {
        self.cache_read_counters.snapshot()
    }

    pub(crate) fn record_access_event(&self, event: StorageAccessEvent) -> Result<()> {
        self.storage_trace.record(event)
    }

    pub(crate) fn create_layout(&self) -> Result<()> {
        Ok(())
    }

    pub(crate) fn ensure_collection_absent(&self) -> Result<()> {
        if self.read_coordination_object(COLLECTION_CURRENT)?.is_some() {
            return Err(BorsukError::InvalidStorage(format!(
                "{} already contains a collection; create at a new empty URI or open the existing collection",
                self.uri
            )));
        }
        Ok(())
    }

    pub(crate) fn create_collection_snapshot(
        &self,
        snapshot: &CollectionSnapshot,
    ) -> Result<LoadedCollectionSnapshot> {
        self.publish_collection_snapshot(snapshot, None)
    }

    pub(crate) fn compare_and_swap_collection_snapshot_with_report(
        &self,
        expected: UpdateVersion,
        snapshot: &CollectionSnapshot,
        report: &mut StorageWriteReport,
    ) -> Result<LoadedCollectionSnapshot> {
        let snapshot_bytes = collection_snapshot_bytes(snapshot)?;
        let checksum = blake3::hash(&snapshot_bytes).to_hex().to_string();
        let current_bytes = collection_current_bytes(&CollectionCurrent {
            snapshot_path: format!("collection/snapshots/{checksum}.bin"),
            snapshot_checksum: checksum,
        })?;
        let loaded = self.publish_collection_snapshot(snapshot, Some(expected))?;
        report.record_collection_snapshot(snapshot_bytes.len(), current_bytes.len());
        Ok(loaded)
    }

    fn publish_collection_snapshot(
        &self,
        snapshot: &CollectionSnapshot,
        expected: Option<UpdateVersion>,
    ) -> Result<LoadedCollectionSnapshot> {
        let snapshot_bytes = collection_snapshot_bytes(snapshot)?;
        let checksum = blake3::hash(&snapshot_bytes).to_hex().to_string();
        let snapshot_path = format!("collection/snapshots/{checksum}.bin");
        self.write_bytes_content_addressed(&snapshot_path, &snapshot_bytes)?;
        let current = CollectionCurrent {
            snapshot_path,
            snapshot_checksum: checksum.clone(),
        };
        let current_version = self.write_coordination_object(
            COLLECTION_CURRENT,
            &collection_current_bytes(&current)?,
            expected,
        )?;
        Ok(LoadedCollectionSnapshot {
            snapshot: snapshot.clone(),
            checksum,
            current_version,
        })
    }

    pub(crate) fn load_collection_snapshot(&self) -> Result<LoadedCollectionSnapshot> {
        let current = self
            .read_coordination_object(COLLECTION_CURRENT)?
            .ok_or_else(|| BorsukError::IndexNotFound(self.uri.clone()))?;
        let pointer = collection_current_from_slice(&current.bytes, COLLECTION_CURRENT)?;
        let snapshot_bytes = self
            .read_bytes_with_cache_status_and_checksum(
                &pointer.snapshot_path,
                &pointer.snapshot_checksum,
            )?
            .bytes;
        let snapshot = collection_snapshot_from_slice(&snapshot_bytes, &pointer.snapshot_path)?;
        Ok(LoadedCollectionSnapshot {
            snapshot,
            checksum: pointer.snapshot_checksum,
            current_version: current.version,
        })
    }

    pub(crate) fn collection_snapshot_generation_if_schema_compatible(
        &self,
        pinned_checksum: &str,
        pinned_generation: u64,
        schema_fingerprint: &str,
    ) -> Result<u64> {
        let current = self
            .read_coordination_object(COLLECTION_CURRENT)?
            .ok_or_else(|| BorsukError::IndexNotFound(self.uri.clone()))?;
        let pointer = collection_current_from_slice(&current.bytes, COLLECTION_CURRENT)?;
        if pointer.snapshot_checksum == pinned_checksum {
            return Ok(pinned_generation);
        }
        let snapshot_bytes = self
            .read_bytes_with_cache_status_and_checksum(
                &pointer.snapshot_path,
                &pointer.snapshot_checksum,
            )?
            .bytes;
        let snapshot = collection_snapshot_from_slice(&snapshot_bytes, &pointer.snapshot_path)?;
        if snapshot.schema_fingerprint != schema_fingerprint {
            return Err(BorsukError::ConcurrentModification {
                path: COLLECTION_CURRENT.to_string(),
            });
        }
        Ok(snapshot.generation)
    }

    pub(crate) fn reserve_collection_wal_transaction(
        &self,
        transaction_id: &str,
        schema_fingerprint: &str,
    ) -> Result<CollectionWalReservationReceipt> {
        const MAX_CAS_ATTEMPTS: usize = 128;
        let shard = collection_wal_frontier_shard(transaction_id)?;
        let head_path = collection_wal_frontier_head_path(shard)?;
        let now_ms = collection_wal_now_ms()?;
        let expires_at_ms = now_ms
            .checked_add(COLLECTION_WAL_RESERVATION_TTL_MS)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "collection WAL reservation expiry exceeds u64".to_string(),
                )
            })?;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let current = match self.read_coordination_object(&head_path) {
                Ok(current) => current,
                Err(BorsukError::ObjectStoreRetryable { .. }) => continue,
                Err(error) => return Err(error),
            };
            let (mut head, version) = match current {
                Some(current) => (
                    collection_wal_frontier_head_from_slice(&current.bytes, &head_path, shard)?,
                    Some(current.version),
                ),
                None => (
                    CollectionWalFrontierHead {
                        generation: 0,
                        reservations: Vec::new(),
                        transactions: Vec::new(),
                    },
                    None,
                ),
            };
            if let Some(existing) = head
                .transactions
                .iter()
                .find(|commit| commit.transaction_id == transaction_id)
            {
                if existing.schema_fingerprint != schema_fingerprint {
                    return Err(BorsukError::InvalidStorage(format!(
                        "collection transaction `{transaction_id}` conflicts with its published schema"
                    )));
                }
                let version = version.ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "collection transaction `{transaction_id}` exists without a root version"
                    ))
                })?;
                return Ok(CollectionWalReservationReceipt {
                    shard,
                    head,
                    version,
                    admission_bytes_written: 0,
                });
            }
            if let Some(existing) = head
                .reservations
                .iter()
                .find(|reservation| reservation.transaction_id == transaction_id)
            {
                if existing.schema_fingerprint != schema_fingerprint {
                    return Err(BorsukError::InvalidStorage(format!(
                        "collection transaction `{transaction_id}` conflicts with its root reservation"
                    )));
                }
                if existing.expires_at_ms > now_ms {
                    let version = version.ok_or_else(|| {
                        BorsukError::InvalidStorage(format!(
                            "collection transaction `{transaction_id}` is reserved without a root version"
                        ))
                    })?;
                    return Ok(CollectionWalReservationReceipt {
                        shard,
                        head,
                        version,
                        admission_bytes_written: 0,
                    });
                }
            }
            head.reservations
                .retain(|reservation| reservation.expires_at_ms > now_ms);
            if head
                .reservations
                .len()
                .saturating_add(head.transactions.len())
                >= COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize
            {
                return Err(BorsukError::ConcurrentModification {
                    path: format!("{head_path}/CAPACITY"),
                });
            }
            head.reservations.push(CollectionWalReservation {
                transaction_id: transaction_id.to_string(),
                schema_fingerprint: schema_fingerprint.to_string(),
                expires_at_ms,
            });
            head.reservations
                .sort_by(|left, right| left.transaction_id.cmp(&right.transaction_id));
            head.generation = head.generation.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "collection WAL frontier generation exceeds u64".to_string(),
                )
            })?;
            let bytes = collection_wal_frontier_head_bytes(&head, shard)?;
            match self.write_coordination_object(&head_path, &bytes, version) {
                Ok(version) => {
                    return Ok(CollectionWalReservationReceipt {
                        shard,
                        head,
                        version,
                        admission_bytes_written: bytes.len() as u64,
                    });
                }
                Err(
                    BorsukError::ConcurrentModification { .. }
                    | BorsukError::ObjectStoreRetryable { .. },
                ) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(BorsukError::ConcurrentModification { path: head_path })
    }

    pub(crate) fn cancel_collection_wal_reservation(&self, transaction_id: &str) -> Result<()> {
        const MAX_CAS_ATTEMPTS: usize = 128;
        let shard = collection_wal_frontier_shard(transaction_id)?;
        let head_path = collection_wal_frontier_head_path(shard)?;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let current = match self.read_coordination_object(&head_path) {
                Ok(current) => current,
                Err(BorsukError::ObjectStoreRetryable { .. }) => continue,
                Err(error) => return Err(error),
            };
            let Some(current) = current else {
                return Ok(());
            };
            let mut head =
                collection_wal_frontier_head_from_slice(&current.bytes, &head_path, shard)?;
            if head
                .transactions
                .iter()
                .any(|commit| commit.transaction_id == transaction_id)
            {
                return Ok(());
            }
            let previous_len = head.reservations.len();
            head.reservations
                .retain(|reservation| reservation.transaction_id != transaction_id);
            if head.reservations.len() == previous_len {
                return Ok(());
            }
            head.generation = head.generation.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "collection WAL frontier generation exceeds u64".to_string(),
                )
            })?;
            let bytes = collection_wal_frontier_head_bytes(&head, shard)?;
            match self.write_coordination_object(&head_path, &bytes, Some(current.version)) {
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

    /// Returns whether this append crossed the cooperative per-shard
    /// maintenance threshold.
    pub(crate) fn create_collection_commit_from_reservation(
        &self,
        commit: &CollectionCommit,
        receipt: &CollectionWalReservationReceipt,
        successor_transaction_id: Option<&str>,
    ) -> Result<CollectionWalCommitOutcome> {
        let shard = collection_wal_frontier_shard(&commit.transaction_id)?;
        if shard != receipt.shard {
            return Err(BorsukError::InvalidStorage(format!(
                "collection transaction `{}` does not match its reservation shard",
                commit.transaction_id
            )));
        }
        let head_path = collection_wal_frontier_head_path(shard)?;
        let mut head = receipt.head.clone();
        if let Some(existing) = head
            .transactions
            .iter()
            .find(|existing| existing.transaction_id == commit.transaction_id)
        {
            if existing != commit {
                return Err(BorsukError::InvalidStorage(format!(
                    "collection transaction `{}` conflicts with its published frontier entry",
                    commit.transaction_id
                )));
            }
            return Ok(CollectionWalCommitOutcome {
                root_pressure: head.transactions.len()
                    >= COLLECTION_WAL_FRONTIER_SOFT_TRANSACTIONS_PER_SHARD as usize,
                successor: None,
            });
        }
        let Some(reservation) = head
            .reservations
            .iter()
            .find(|reservation| reservation.transaction_id == commit.transaction_id)
        else {
            return self
                .append_collection_wal_transaction(commit)
                .map(|root_pressure| CollectionWalCommitOutcome {
                    root_pressure,
                    successor: None,
                });
        };
        if reservation.schema_fingerprint != commit.schema_fingerprint {
            return Err(BorsukError::InvalidStorage(format!(
                "collection transaction `{}` conflicts with its root reservation schema",
                commit.transaction_id
            )));
        }
        if reservation.expires_at_ms <= collection_wal_now_ms()? {
            return Err(BorsukError::ConcurrentModification {
                path: format!("{head_path}/EXPIRED"),
            });
        }
        let mut successor = successor_transaction_id
            .map(|transaction_id| -> Result<CollectionWalReservation> {
                if transaction_id == commit.transaction_id {
                    return Err(BorsukError::InvalidStorage(
                        "collection WAL successor must use a new transaction id".to_string(),
                    ));
                }
                if collection_wal_frontier_shard(transaction_id)? != shard {
                    return Err(BorsukError::InvalidStorage(format!(
                        "collection WAL successor `{transaction_id}` does not match shard {shard}"
                    )));
                }
                if head
                    .reservations
                    .iter()
                    .any(|entry| entry.transaction_id == transaction_id)
                    || head
                        .transactions
                        .iter()
                        .any(|entry| entry.transaction_id == transaction_id)
                {
                    return Err(BorsukError::InvalidStorage(format!(
                        "collection WAL successor `{transaction_id}` already exists"
                    )));
                }
                let expires_at_ms = collection_wal_now_ms()?
                    .checked_add(COLLECTION_WAL_RESERVATION_TTL_MS)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "collection WAL reservation expiry exceeds u64".to_string(),
                        )
                    })?;
                Ok(CollectionWalReservation {
                    transaction_id: transaction_id.to_string(),
                    schema_fingerprint: commit.schema_fingerprint.clone(),
                    expires_at_ms,
                })
            })
            .transpose()?;
        if head
            .reservations
            .len()
            .saturating_add(head.transactions.len())
            >= COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize
        {
            successor = None;
        }
        head.generation = head.generation.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage(
                "collection WAL frontier generation exceeds u64".to_string(),
            )
        })?;
        head.reservations
            .retain(|reservation| reservation.transaction_id != commit.transaction_id);
        if let Some(successor) = &successor {
            head.reservations.push(successor.clone());
            head.reservations
                .sort_by(|left, right| left.transaction_id.cmp(&right.transaction_id));
        }
        head.transactions.push(commit.clone());
        head.transactions
            .sort_by(|left, right| left.transaction_id.cmp(&right.transaction_id));
        let head_bytes = collection_wal_frontier_head_bytes(&head, shard)?;
        match self.write_coordination_object(&head_path, &head_bytes, Some(receipt.version.clone()))
        {
            Ok(version) => Ok(CollectionWalCommitOutcome {
                root_pressure: head.transactions.len()
                    >= COLLECTION_WAL_FRONTIER_SOFT_TRANSACTIONS_PER_SHARD as usize,
                successor: successor.map(|_| CollectionWalReservationReceipt {
                    shard,
                    head,
                    version,
                    admission_bytes_written: 0,
                }),
            }),
            Err(
                BorsukError::ConcurrentModification { .. }
                | BorsukError::ObjectStoreRetryable { .. },
            ) => self
                .append_collection_wal_transaction(commit)
                .map(|root_pressure| CollectionWalCommitOutcome {
                    root_pressure,
                    successor: None,
                }),
            Err(error) => Err(error),
        }
    }

    fn append_collection_wal_transaction(&self, commit: &CollectionCommit) -> Result<bool> {
        const MAX_CAS_ATTEMPTS: usize = 128;
        let shard = collection_wal_frontier_shard(&commit.transaction_id)?;
        let head_path = collection_wal_frontier_head_path(shard)?;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let current = match self.read_coordination_object(&head_path) {
                Ok(current) => current,
                Err(BorsukError::ObjectStoreRetryable { .. }) => continue,
                Err(error) => return Err(error),
            };
            let (mut head, version) = match current {
                Some(current) => {
                    let head =
                        collection_wal_frontier_head_from_slice(&current.bytes, &head_path, shard)?;
                    (head, Some(current.version))
                }
                None => (
                    CollectionWalFrontierHead {
                        generation: 0,
                        reservations: Vec::new(),
                        transactions: Vec::new(),
                    },
                    None,
                ),
            };
            if let Some(existing) = head
                .transactions
                .iter()
                .find(|existing| existing.transaction_id == commit.transaction_id)
            {
                if existing != commit {
                    return Err(BorsukError::InvalidStorage(format!(
                        "collection transaction `{}` conflicts with its published frontier entry",
                        commit.transaction_id
                    )));
                }
                return Ok(head.transactions.len()
                    >= COLLECTION_WAL_FRONTIER_SOFT_TRANSACTIONS_PER_SHARD as usize);
            }
            let Some(reservation) = head
                .reservations
                .iter()
                .find(|reservation| reservation.transaction_id == commit.transaction_id)
            else {
                return Err(BorsukError::ConcurrentModification {
                    path: format!("{head_path}/RESERVATION"),
                });
            };
            if reservation.schema_fingerprint != commit.schema_fingerprint {
                return Err(BorsukError::InvalidStorage(format!(
                    "collection transaction `{}` conflicts with its root reservation schema",
                    commit.transaction_id
                )));
            }
            if reservation.expires_at_ms <= collection_wal_now_ms()? {
                return Err(BorsukError::ConcurrentModification {
                    path: format!("{head_path}/EXPIRED"),
                });
            }
            head.generation = head.generation.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "collection WAL frontier generation exceeds u64".to_string(),
                )
            })?;
            head.reservations
                .retain(|reservation| reservation.transaction_id != commit.transaction_id);
            head.transactions.push(commit.clone());
            head.transactions
                .sort_by(|left, right| left.transaction_id.cmp(&right.transaction_id));
            let head_bytes = collection_wal_frontier_head_bytes(&head, shard)?;
            match self.write_coordination_object(&head_path, &head_bytes, version) {
                Ok(_) => {
                    return Ok(head.transactions.len()
                        >= COLLECTION_WAL_FRONTIER_SOFT_TRANSACTIONS_PER_SHARD as usize);
                }
                Err(
                    BorsukError::ConcurrentModification { .. }
                    | BorsukError::ObjectStoreRetryable { .. },
                ) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(BorsukError::ConcurrentModification { path: head_path })
    }

    pub(crate) fn collection_wal_transactions_snapshot_with_retries(
        &self,
    ) -> Result<(BTreeMap<String, CollectionCommit>, usize)> {
        Ok((self.pending_collection_commits_all_epochs()?, 0))
    }

    pub(crate) fn collection_wal_authorized_transaction_ids_snapshot(
        &self,
    ) -> Result<BTreeSet<String>> {
        let mut transaction_ids = BTreeSet::new();
        self.for_each_object("collection/write-epochs/", |object| {
            if let Some(transaction_id) = object
                .path
                .strip_suffix(".commit")
                .and_then(|path| path.rsplit_once("/pending/").map(|(_, id)| id))
            {
                transaction_ids.insert(transaction_id.to_string());
            }
            Ok(())
        })?;
        Ok(transaction_ids)
    }

    fn pending_collection_commits_all_epochs(&self) -> Result<BTreeMap<String, CollectionCommit>> {
        let prefix = "collection/write-epochs/";
        let mut paths = Vec::new();
        self.for_each_object(prefix, |object| {
            if object.path.contains("/pending/") && object.path.ends_with(".commit") {
                paths.push(object.path);
                if paths.len() > PENDING_COLLECTION_COMMIT_HARD_BOUND {
                    return Err(BorsukError::InvalidStorage(format!(
                        "pending collection commit backlog exceeds {PENDING_COLLECTION_COMMIT_HARD_BOUND}"
                    )));
                }
            }
            Ok(())
        })?;
        paths.sort();
        let pending_commits = crate::parallel::install_io(|| {
            paths
                .into_par_iter()
                .map(|path| {
                    let object = self.read_coordination_object(&path)?.ok_or_else(|| {
                        BorsukError::ConcurrentModification { path: path.clone() }
                    })?;
                    pending_collection_commit_from_slice(&object.bytes, &path)
                })
                .collect::<Result<Vec<_>>>()
        })?;
        let mut commits = BTreeMap::new();
        for pending in pending_commits {
            match commits.insert(
                pending.commit.transaction_id.clone(),
                pending.commit.clone(),
            ) {
                None => {}
                Some(existing) if existing == pending.commit => {}
                Some(_) => {
                    return Err(BorsukError::InvalidStorage(format!(
                        "pending collection transaction `{}` conflicts across write epochs",
                        pending.commit.transaction_id
                    )));
                }
            }
        }
        Ok(commits)
    }

    fn pending_collection_commits_for_schema(
        &self,
        schema_fingerprint: &str,
    ) -> Result<BTreeMap<String, CollectionCommit>> {
        let epoch = format!("schema-{schema_fingerprint}");
        let prefix = format!("collection/write-epochs/{epoch}/pending/");
        let mut paths = Vec::new();
        self.for_each_object(&prefix, |object| {
            if object.path.ends_with(".commit") {
                paths.push(object.path);
                if paths.len() > PENDING_COLLECTION_COMMIT_HARD_BOUND {
                    return Err(BorsukError::InvalidStorage(format!(
                        "pending collection commit backlog exceeds {PENDING_COLLECTION_COMMIT_HARD_BOUND}"
                    )));
                }
            }
            Ok(())
        })?;
        paths.sort();
        let pending_commits = crate::parallel::install_io(|| {
            paths
                .into_par_iter()
                .map(|path| {
                    let object = self.read_coordination_object(&path)?.ok_or_else(|| {
                        BorsukError::ConcurrentModification { path: path.clone() }
                    })?;
                    pending_collection_commit_from_slice(&object.bytes, &path)
                })
                .collect::<Result<Vec<_>>>()
        })?;
        let mut commits = BTreeMap::new();
        for pending in pending_commits {
            let path =
                pending_collection_commit_path(&pending.epoch, &pending.commit.transaction_id)?;
            if pending.epoch != epoch || pending.commit.schema_fingerprint != schema_fingerprint {
                return Err(BorsukError::InvalidStorage(format!(
                    "pending collection commit `{path}` does not match active schema"
                )));
            }
            if commits
                .insert(
                    pending.commit.transaction_id.clone(),
                    pending.commit.clone(),
                )
                .is_some()
            {
                return Err(BorsukError::InvalidStorage(format!(
                    "duplicate pending collection transaction `{}`",
                    pending.commit.transaction_id
                )));
            }
        }
        Ok(commits)
    }

    pub(crate) fn prune_expired_collection_wal_reservations(&self) -> Result<()> {
        const MAX_CAS_ATTEMPTS: usize = 128;
        let now_ms = collection_wal_now_ms()?;
        for shard in 0..COLLECTION_WAL_FRONTIER_SHARDS {
            let head_path = collection_wal_frontier_head_path(shard)?;
            let mut updated = false;
            for _ in 0..MAX_CAS_ATTEMPTS {
                let current = match self.read_coordination_object(&head_path) {
                    Ok(current) => current,
                    Err(BorsukError::ObjectStoreRetryable { .. }) => continue,
                    Err(error) => return Err(error),
                };
                let Some(current) = current else {
                    updated = true;
                    break;
                };
                let mut head =
                    collection_wal_frontier_head_from_slice(&current.bytes, &head_path, shard)?;
                let previous_len = head.reservations.len();
                head.reservations
                    .retain(|reservation| reservation.expires_at_ms > now_ms);
                if head.reservations.len() == previous_len {
                    updated = true;
                    break;
                }
                head.generation = head.generation.checked_add(1).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "collection WAL frontier generation exceeds u64".to_string(),
                    )
                })?;
                let bytes = collection_wal_frontier_head_bytes(&head, shard)?;
                match self.write_coordination_object(&head_path, &bytes, Some(current.version)) {
                    Ok(_) => {
                        updated = true;
                        break;
                    }
                    Err(
                        BorsukError::ConcurrentModification { .. }
                        | BorsukError::ObjectStoreRetryable { .. },
                    ) => continue,
                    Err(error) => return Err(error),
                }
            }
            if !updated {
                return Err(BorsukError::ConcurrentModification { path: head_path });
            }
        }
        Ok(())
    }

    /// Load one collection catalog/frontier view that cannot straddle a
    /// manifest publication followed by frontier pruning.
    pub(crate) fn load_collection_view(
        &self,
    ) -> Result<(
        LoadedCollectionSnapshot,
        BTreeMap<String, CollectionCommit>,
        usize,
    )> {
        const MAX_VIEW_ATTEMPTS: usize = 32;
        for view_retries in 0..MAX_VIEW_ATTEMPTS {
            let before = self.load_collection_snapshot()?;
            let mut transactions = BTreeMap::new();
            for (transaction_id, commit) in
                self.pending_collection_commits_for_schema(&before.snapshot.schema_fingerprint)?
            {
                match transactions.insert(transaction_id.clone(), commit.clone()) {
                    None => {}
                    Some(existing) if existing == commit => {}
                    Some(_) => {
                        return Err(BorsukError::InvalidStorage(format!(
                            "collection transaction `{transaction_id}` conflicts between frontier and pending log"
                        )));
                    }
                }
            }
            let after = self.load_collection_snapshot()?;
            if before.current_version == after.current_version && before.checksum == after.checksum
            {
                return Ok((before, transactions, view_retries));
            }
        }
        Err(BorsukError::ConcurrentModification {
            path: "collection catalog/frontier view".to_string(),
        })
    }

    pub(crate) fn prune_collection_wal_transactions(
        &self,
        consumed: &BTreeSet<String>,
    ) -> Result<()> {
        const MAX_CAS_ATTEMPTS: usize = 128;
        let shards = consumed
            .iter()
            .map(|transaction_id| collection_wal_frontier_shard(transaction_id))
            .collect::<Result<BTreeSet<_>>>()?;
        for shard in shards {
            let head_path = collection_wal_frontier_head_path(shard)?;
            let mut updated = false;
            for _ in 0..MAX_CAS_ATTEMPTS {
                let Some(current) = self.read_coordination_object(&head_path)? else {
                    updated = true;
                    break;
                };
                let head =
                    collection_wal_frontier_head_from_slice(&current.bytes, &head_path, shard)?;
                let mut next = head.clone();
                next.transactions
                    .retain(|commit| !consumed.contains(&commit.transaction_id));
                if next.transactions.len() == head.transactions.len() {
                    updated = true;
                    break;
                }
                next.generation = head.generation.checked_add(1).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "collection WAL frontier generation exceeds u64".to_string(),
                    )
                })?;
                let bytes = collection_wal_frontier_head_bytes(&next, shard)?;
                match self.write_coordination_object(&head_path, &bytes, Some(current.version)) {
                    Ok(_) => {
                        updated = true;
                        break;
                    }
                    Err(BorsukError::ConcurrentModification { .. }) => continue,
                    Err(error) => return Err(error),
                }
            }
            if !updated {
                return Err(BorsukError::ConcurrentModification { path: head_path });
            }
        }
        Ok(())
    }

    pub(crate) fn stage_manifest(
        &self,
        modality: &str,
        manifest: &Manifest,
        previous: Option<&Manifest>,
    ) -> Result<StagedManifest> {
        Ok(self
            .stage_manifest_with_report(modality, manifest, previous)?
            .0)
    }

    pub(crate) fn stage_manifest_with_report(
        &self,
        modality: &str,
        manifest: &Manifest,
        previous: Option<&Manifest>,
    ) -> Result<(StagedManifest, StorageWriteReport)> {
        self.stage_manifest_with_report_and_routing_summaries(modality, manifest, previous, None)
    }

    pub(crate) fn stage_manifest_with_report_and_routing_summaries(
        &self,
        modality: &str,
        manifest: &Manifest,
        previous: Option<&Manifest>,
        routing_summaries: Option<&[SegmentSummary]>,
    ) -> Result<(StagedManifest, StorageWriteReport)> {
        let span = observability::publish_span(manifest.version);
        let _entered = span.enter();
        let mut report = StorageWriteReport::default();
        let page_refs = self.routing_layer_page_refs_with_report(
            manifest,
            previous,
            0,
            &mut report,
            routing_summaries,
        )?;
        let staged = self.stage_manifest_with_routing_page_refs_with_report(
            modality,
            manifest,
            &page_refs,
            &mut report,
        )?;
        observability::record_publish_report(&span, &staged.manifest, &report);
        Ok((staged, report))
    }

    pub(crate) fn stage_manifest_with_routing_page_refs_with_report(
        &self,
        modality: &str,
        manifest: &Manifest,
        page_refs: &[RoutingLayerPageRef],
        report: &mut StorageWriteReport,
    ) -> Result<StagedManifest> {
        let mut manifest = manifest.clone();
        manifest.set_routing_max_level_for_leaf_pages(page_refs.len())?;
        self.write_routing_layer_page_indexes_with_report(&manifest, page_refs, report)?;
        let (reference, _) =
            self.write_manifest_metadata_tables_with_report(modality, &manifest, report)?;
        Ok(StagedManifest {
            manifest,
            reference,
        })
    }

    pub(crate) fn stage_manifest_with_top_routing_page_refs_with_report(
        &self,
        modality: &str,
        manifest: &Manifest,
        routing_level: u8,
        page_refs: &[RoutingLayerPageRef],
        report: &mut StorageWriteReport,
    ) -> Result<StagedManifest> {
        let mut manifest = manifest.clone();
        manifest.routing_max_level = routing_level;
        let page_index_bytes =
            routing_layer_page_index_to_parquet(&manifest, routing_level, page_refs)?;
        self.write_bytes_if_absent(
            &Manifest::routing_layer_page_index_file_name(manifest.version, routing_level),
            &page_index_bytes,
        )?;
        report.record_metadata_table(page_index_bytes.len());
        let (reference, _) =
            self.write_manifest_metadata_tables_with_report(modality, &manifest, report)?;
        Ok(StagedManifest {
            manifest,
            reference,
        })
    }

    fn write_manifest_metadata_tables_with_report(
        &self,
        modality: &str,
        manifest: &Manifest,
        report: &mut StorageWriteReport,
    ) -> Result<(CollectionManifestRef, ManifestTableChecksums)> {
        let prefix = collection_modality_prefix(modality)?;
        let manifest_bytes = manifest_to_parquet(manifest)?;
        let routing_bytes = routing_to_parquet(manifest)?;
        let pivots_bytes = pivots_to_parquet(manifest)?;
        let checksums = ManifestTableChecksums {
            manifest: blake3::hash(&manifest_bytes),
            routing: blake3::hash(&routing_bytes),
            pivots: blake3::hash(&pivots_bytes),
        };
        let paged_resident_bytes =
            manifest_metadata_from_parquet(&manifest_bytes)?.resident_bytes_estimate();

        self.write_bytes_if_absent(&manifest.file_name(), &manifest_bytes)?;
        report.record_metadata_table(manifest_bytes.len());
        self.write_bytes_if_absent(&manifest.routing_file_name(), &routing_bytes)?;
        report.record_metadata_table(routing_bytes.len());
        self.write_bytes_if_absent(&manifest.pivots_file_name(), &pivots_bytes)?;
        report.record_metadata_table(pivots_bytes.len());
        let reference = CollectionManifestRef {
            modality: modality.to_string(),
            prefix: prefix.clone(),
            version: manifest.version,
            manifest_path: format!("{prefix}{}", manifest.file_name()),
            manifest_checksum: checksums.manifest.to_hex().to_string(),
            routing_path: format!("{prefix}{}", manifest.routing_file_name()),
            routing_checksum: checksums.routing.to_hex().to_string(),
            pivots_path: format!("{prefix}{}", manifest.pivots_file_name()),
            pivots_checksum: checksums.pivots.to_hex().to_string(),
            consumed_wal_frontier_checksum: consumed_wal_frontier_checksum(
                manifest.cell_wal_consumed_runs.iter().map(String::as_str),
            ),
            resident_bytes_estimate: paged_resident_bytes,
            // Decoding the resident-routing tables can materialize a few
            // canonical default fields that are absent from the writer's
            // transient manifest. Keep a small fixed per-modality allowance
            // rather than reparsing every routing table on the write path.
            resident_routing_bytes_estimate: manifest
                .resident_bytes_estimate()
                .max(paged_resident_bytes)
                .saturating_add(RESIDENT_ROUTING_ESTIMATE_SLACK_BYTES),
        };
        validate_collection_manifest_ref(&reference)?;
        Ok((reference, checksums))
    }

    fn write_routing_layer_page_indexes_with_report(
        &self,
        manifest: &Manifest,
        leaf_page_refs: &[RoutingLayerPageRef],
        report: &mut StorageWriteReport,
    ) -> Result<()> {
        let mut routing_level = 0_u8;
        let mut page_refs = leaf_page_refs.to_vec();
        loop {
            let page_index_bytes =
                routing_layer_page_index_to_parquet(manifest, routing_level, &page_refs)?;
            self.write_bytes_if_absent(
                &Manifest::routing_layer_page_index_file_name(manifest.version, routing_level),
                &page_index_bytes,
            )?;
            report.record_metadata_table(page_index_bytes.len());

            if page_refs.len() <= 1 {
                break;
            }

            routing_level = routing_level.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage("routing layer depth exceeds u8".to_string())
            })?;
            page_refs = self.parent_routing_layer_page_refs_with_report(
                manifest,
                routing_level,
                &page_refs,
                report,
            )?;
        }

        Ok(())
    }

    pub(crate) fn write_routing_layer_page(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        page_ordinal: usize,
        segments: &[SegmentSummary],
    ) -> Result<RoutingLayerPageRef> {
        let mut report = StorageWriteReport::default();
        self.write_routing_layer_page_with_report(
            manifest,
            routing_level,
            page_ordinal,
            segments,
            &mut report,
        )
    }

    pub(crate) fn write_routing_layer_page_with_report(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        page_ordinal: usize,
        segments: &[SegmentSummary],
        report: &mut StorageWriteReport,
    ) -> Result<RoutingLayerPageRef> {
        let bytes = routing_layer_page_to_parquet(
            manifest,
            routing_level,
            page_ordinal,
            page_ordinal
                .checked_mul(manifest.routing_page_fanout)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("routing page ordinal overflow".to_string())
                })?,
            segments,
        )?;
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let path = Manifest::routing_layer_page_content_file_name(routing_level, &checksum);
        if !self.exists(&path)? {
            self.write_bytes(&path, &bytes)?;
            report.record_routing_page(bytes.len());
        }
        Ok(RoutingLayerPageRef {
            routing_level,
            page_ordinal,
            path,
            checksum,
            page_segments: segments.len(),
            leaf_segments: segments.len(),
            leaf_pages: 1,
            routing_pages: 1,
            dimensions: manifest.config.dimensions,
            centroid: routing_layer_page_centroid(manifest.config.dimensions, segments),
            radius: routing_layer_page_radius(manifest, segments)?,
            bounds_min: routing_layer_page_bounds_min(manifest.config.dimensions, segments),
            bounds_max: routing_layer_page_bounds_max(manifest.config.dimensions, segments),
            id_bloom: routing_layer_page_id_bloom(segments),
            vector_signature_bloom: routing_layer_page_vector_signature_bloom(segments),
            level_mask: routing_layer_page_level_mask(segments),
            page_records: routing_layer_page_record_count(segments),
            page_segment_bytes: routing_layer_page_segment_bytes(segments),
            page_vector_bytes: routing_layer_page_vector_bytes(segments),
            page_graph_bytes: routing_layer_page_graph_bytes(segments),
            page_sparse_encoded_vectors: routing_layer_page_sparse_encoded_vectors(segments),
            page_dense_encoded_vectors: routing_layer_page_dense_encoded_vectors(segments),
        })
    }

    fn routing_layer_page_refs_with_report(
        &self,
        manifest: &Manifest,
        previous: Option<&Manifest>,
        routing_level: u8,
        report: &mut StorageWriteReport,
        routing_summaries: Option<&[SegmentSummary]>,
    ) -> Result<Vec<RoutingLayerPageRef>> {
        let previous_refs = previous
            .map(|previous| self.read_routing_layer_page_index(previous.version, routing_level))
            .transpose()?
            .unwrap_or_default();
        let mut page_refs = Vec::new();

        let segments = routing_summaries.unwrap_or(&manifest.segments);
        for (page_ordinal, segments) in segments.chunks(manifest.routing_page_fanout).enumerate() {
            if let Some(previous_manifest) = previous
                && routing_layer_page_unchanged(
                    previous_manifest,
                    manifest.routing_page_fanout,
                    page_ordinal,
                    segments,
                )
                && let Some(page_ref) = previous_refs.get(page_ordinal)
            {
                page_refs.push(page_ref.clone());
                continue;
            }

            page_refs.push(self.write_routing_layer_page_with_report(
                manifest,
                routing_level,
                page_ordinal,
                segments,
                report,
            )?);
        }

        Ok(page_refs)
    }

    fn parent_routing_layer_page_refs_with_report(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        child_refs: &[RoutingLayerPageRef],
        report: &mut StorageWriteReport,
    ) -> Result<Vec<RoutingLayerPageRef>> {
        child_refs
            .chunks(manifest.routing_page_fanout)
            .enumerate()
            .map(|(page_ordinal, children)| {
                self.write_parent_routing_layer_page_with_report(
                    manifest,
                    routing_level,
                    page_ordinal,
                    children,
                    report,
                )
            })
            .collect()
    }

    pub(crate) fn write_parent_routing_layer_page(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        page_ordinal: usize,
        child_refs: &[RoutingLayerPageRef],
    ) -> Result<RoutingLayerPageRef> {
        let mut report = StorageWriteReport::default();
        self.write_parent_routing_layer_page_with_report(
            manifest,
            routing_level,
            page_ordinal,
            child_refs,
            &mut report,
        )
    }

    pub(crate) fn write_parent_routing_layer_page_with_report(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        page_ordinal: usize,
        child_refs: &[RoutingLayerPageRef],
        report: &mut StorageWriteReport,
    ) -> Result<RoutingLayerPageRef> {
        let child_routing_level = routing_level.checked_sub(1).ok_or_else(|| {
            BorsukError::InvalidStorage("parent routing layer must be above L0".to_string())
        })?;
        let bytes = routing_layer_page_index_to_parquet(manifest, child_routing_level, child_refs)?;
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let path = Manifest::routing_layer_page_content_file_name(routing_level, &checksum);
        if !self.exists(&path)? {
            self.write_bytes(&path, &bytes)?;
            report.record_routing_page(bytes.len());
        }

        Ok(RoutingLayerPageRef {
            routing_level,
            page_ordinal,
            path,
            checksum,
            page_segments: child_refs.len(),
            leaf_segments: routing_page_refs_leaf_segments(child_refs),
            leaf_pages: routing_page_refs_leaf_pages(child_refs),
            routing_pages: routing_page_refs_routing_pages(child_refs),
            dimensions: manifest.config.dimensions,
            centroid: routing_page_refs_centroid(manifest.config.dimensions, child_refs),
            radius: routing_page_refs_radius(manifest, child_refs)?,
            bounds_min: routing_page_refs_bounds_min(manifest.config.dimensions, child_refs),
            bounds_max: routing_page_refs_bounds_max(manifest.config.dimensions, child_refs),
            id_bloom: routing_page_refs_id_bloom(child_refs),
            vector_signature_bloom: routing_page_refs_vector_signature_bloom(child_refs),
            level_mask: routing_page_refs_level_mask(child_refs),
            page_records: routing_page_refs_record_count(child_refs),
            page_segment_bytes: routing_page_refs_segment_bytes(child_refs),
            page_vector_bytes: routing_page_refs_vector_bytes(child_refs),
            page_graph_bytes: routing_page_refs_graph_bytes(child_refs),
            page_sparse_encoded_vectors: routing_page_refs_sparse_encoded_vectors(child_refs),
            page_dense_encoded_vectors: routing_page_refs_dense_encoded_vectors(child_refs),
        })
    }

    pub(crate) fn read_routing_layer_page_index(
        &self,
        version: u64,
        routing_level: u8,
    ) -> Result<Vec<RoutingLayerPageRef>> {
        Ok(self
            .read_routing_layer_page_index_with_status(version, routing_level)?
            .page_refs)
    }

    pub(crate) fn read_routing_layer_page_index_with_status(
        &self,
        version: u64,
        routing_level: u8,
    ) -> Result<RoutingLayerPageIndexRead> {
        let path = Manifest::routing_layer_page_index_file_name(version, routing_level);
        match self.read_bytes_with_cache_status(&path) {
            Ok(read) => Ok(RoutingLayerPageIndexRead {
                page_refs: routing_layer_page_index_from_parquet(
                    &read.bytes,
                    version,
                    routing_level,
                )?,
                bytes_read: read.bytes.len() as u64,
                page_indexes_read: 1,
                object_cache_hits: usize::from(read.cache_hit),
                object_cache_misses: usize::from(!read.cache_hit),
            }),
            Err(err) if is_object_store_not_found(&err) => Ok(RoutingLayerPageIndexRead {
                page_refs: Vec::new(),
                bytes_read: 0,
                page_indexes_read: 0,
                object_cache_hits: 0,
                object_cache_misses: 0,
            }),
            Err(err) => Err(err),
        }
    }

    /// Load one exact checksum-pinned modality manifest through the collection
    /// root storage. The reference paths are root-relative, even for children.
    pub(crate) fn load_manifest_ref(
        &self,
        reference: &CollectionManifestRef,
        resident_routing: bool,
    ) -> Result<Manifest> {
        validate_collection_manifest_ref(reference)?;
        let expected_paths = [
            (
                &reference.manifest_path,
                format!(
                    "{}{}",
                    reference.prefix,
                    Manifest::file_name_for_version(reference.version)
                ),
                "manifest",
            ),
            (
                &reference.routing_path,
                format!(
                    "{}{}",
                    reference.prefix,
                    Manifest::routing_file_name_for_version(reference.version)
                ),
                "routing",
            ),
            (
                &reference.pivots_path,
                format!(
                    "{}{}",
                    reference.prefix,
                    Manifest::pivots_file_name_for_version(reference.version)
                ),
                "pivots",
            ),
        ];
        for (actual, expected, label) in expected_paths {
            if actual != &expected {
                return Err(BorsukError::InvalidStorage(format!(
                    "collection {label} reference for version {} must use `{expected}`, got `{actual}`",
                    reference.version
                )));
            }
        }

        let manifest_bytes = self
            .read_bytes_with_cache_status_and_checksum(
                &reference.manifest_path,
                &reference.manifest_checksum,
            )?
            .bytes;
        if !manifest_has_next_generated_id(&manifest_bytes)? {
            return Err(BorsukError::InvalidStorage(
                "manifest table is missing the next_generated_id column".to_string(),
            ));
        }

        let mut manifest = if resident_routing {
            let routing_bytes = self
                .read_bytes_with_cache_status_and_checksum(
                    &reference.routing_path,
                    &reference.routing_checksum,
                )?
                .bytes;
            let pivots_bytes = self
                .read_bytes_with_cache_status_and_checksum(
                    &reference.pivots_path,
                    &reference.pivots_checksum,
                )?
                .bytes;
            let mut manifest = manifest_from_parquet(&manifest_bytes, &routing_bytes)?;
            manifest.pivots =
                pivots_from_parquet(&pivots_bytes, manifest.config.dimensions, manifest.version)?;
            manifest
        } else {
            manifest_metadata_from_parquet(&manifest_bytes)?
        };
        if manifest.version != reference.version {
            return Err(BorsukError::InvalidStorage(format!(
                "collection manifest reference pins version {}, but the manifest table contains version {}",
                reference.version, manifest.version
            )));
        }
        let frontier_checksum = consumed_wal_frontier_checksum(
            manifest.cell_wal_consumed_runs.iter().map(String::as_str),
        );
        if frontier_checksum != reference.consumed_wal_frontier_checksum {
            return Err(BorsukError::InvalidStorage(format!(
                "collection manifest version {} consumed WAL frontier checksum mismatch",
                reference.version
            )));
        }
        manifest.cell_wal_visible_runs = 0;
        manifest.cell_wal_visible_tombstone_runs = 0;
        Ok(manifest)
    }

    /// Load the manifest published under an explicit version, independent of `CURRENT`.
    ///
    /// Returns `Ok(None)` when the version's manifest or routing table no longer exists,
    /// for example after a crash left a partially staged version namespace. The result is
    /// only suitable for reference walks such as garbage collection: pivot payloads and
    /// legacy generated-id recovery are intentionally skipped.
    pub(crate) fn load_manifest_for_version(&self, version: u64) -> Result<Option<Manifest>> {
        let manifest_bytes =
            match self.read_bytes_uncached(&Manifest::file_name_for_version(version)) {
                Ok(bytes) => bytes,
                Err(err) if is_object_store_not_found(&err) => return Ok(None),
                Err(err) => return Err(err),
            };
        let routing_bytes =
            match self.read_bytes_uncached(&Manifest::routing_file_name_for_version(version)) {
                Ok(bytes) => bytes,
                Err(err) if is_object_store_not_found(&err) => return Ok(None),
                Err(err) => return Err(err),
            };
        let manifest = manifest_from_parquet(&manifest_bytes, &routing_bytes)?;
        if manifest.version != version {
            return Err(BorsukError::InvalidStorage(format!(
                "manifest table for version {version} contains version {}",
                manifest.version
            )));
        }
        Ok(Some(manifest))
    }

    pub(crate) fn write_bytes(&self, relative: &str, bytes: &[u8]) -> Result<()> {
        self.invalidate_cached_object_size(relative);
        if bytes.len() > MULTIPART_WRITE_THRESHOLD_BYTES {
            self.write_bytes_multipart(relative, bytes)?;
        } else {
            self.write_bytes_with_mode(relative, bytes, PutMode::Overwrite)?;
        }
        Ok(())
    }

    fn write_bytes_if_absent(&self, relative: &str, bytes: &[u8]) -> Result<PutResult> {
        self.write_bytes_with_mode(relative, bytes, PutMode::Create)
    }

    pub(crate) fn create_pending_collection_commit(
        &self,
        pending: &PendingCollectionCommit,
    ) -> Result<()> {
        let path = pending_collection_commit_path(&pending.epoch, &pending.commit.transaction_id)?;
        let bytes = pending_collection_commit_bytes(pending)?;
        match self.write_bytes_if_absent(&path, &bytes) {
            Ok(_) => Ok(()),
            Err(
                error @ (BorsukError::ConcurrentModification { .. }
                | BorsukError::ObjectStoreRetryable { .. }),
            ) => {
                let Some(existing) = self.read_coordination_object(&path)? else {
                    return Err(error);
                };
                let existing = pending_collection_commit_from_slice(&existing.bytes, &path)?;
                if existing.epoch == pending.epoch && existing.commit == pending.commit {
                    Ok(())
                } else {
                    Err(BorsukError::InvalidStorage(format!(
                        "pending collection commit `{path}` conflicts with existing content"
                    )))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Write a content-addressed object: the caller guarantees `relative` is
    /// derived from a hash of `bytes`, so any object already living at that path
    /// is byte-identical. Two writers racing to publish the same logical write
    /// (e.g. concurrent WAL appends at the same version/seq) therefore target
    /// distinct paths, and a benign re-write of an existing content-addressed
    /// object is a no-op rather than a clobber. Uses write-if-absent so a loser
    /// never overwrites (and corrupts) a winner's committed object; an
    /// `AlreadyExists` conflict is expected and swallowed. Large objects fall
    /// back to multipart — an overwrite there is still safe because the content
    /// (hence the bytes on disk) is identical.
    pub(crate) fn write_bytes_content_addressed(&self, relative: &str, bytes: &[u8]) -> Result<()> {
        if bytes.len() > MULTIPART_WRITE_THRESHOLD_BYTES {
            self.write_bytes_multipart(relative, bytes)?;
            return Ok(());
        }
        match self.write_bytes_if_absent(relative, bytes) {
            Ok(_) | Err(BorsukError::ConcurrentModification { .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn create_bytes_verified(
        &self,
        relative: &str,
        bytes: &[u8],
        expected_checksum: &str,
    ) -> Result<CreateOutcome> {
        let actual_checksum = blake3::hash(bytes).to_hex().to_string();
        if expected_checksum.len() != 64
            || expected_checksum != actual_checksum
            || !expected_checksum
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(BorsukError::InvalidStorage(format!(
                "create-only object `{relative}` checksum does not match its bytes"
            )));
        }
        match self.write_bytes_if_absent(relative, bytes) {
            Ok(_) => Ok(CreateOutcome::Created),
            Err(
                error @ (BorsukError::ConcurrentModification { .. }
                | BorsukError::ObjectStoreRetryable { .. }),
            ) => match self.read_object_fresh(relative)? {
                Some(existing)
                    if existing.len() == bytes.len()
                        && blake3::hash(&existing).to_hex().as_str() == expected_checksum =>
                {
                    Ok(CreateOutcome::Existing)
                }
                Some(_) => Err(BorsukError::InvalidStorage(format!(
                    "create-only object `{relative}` conflicts with existing content"
                ))),
                None => Err(error),
            },
            Err(error) => Err(error),
        }
    }

    /// Create an object only if it does not already exist. Returns `true` when
    /// this call created it and `false` when another writer already holds it.
    /// Backs maintenance leases and instance membership (correctness of publishes
    /// still rests on the `CURRENT` compare-and-swap; leases only avoid duplicated
    /// maintenance work).
    pub(crate) fn try_create_object(&self, relative: &str, bytes: &[u8]) -> Result<bool> {
        match self.write_bytes_with_mode(relative, bytes, PutMode::Create) {
            Ok(_) => Ok(true),
            Err(BorsukError::ConcurrentModification { .. }) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Create a mutable coordination object and return the exact version owned
    /// by this caller. `None` means another writer already created the path.
    /// Callers use the returned version for rollback that cannot overwrite a
    /// concurrent update.
    pub(crate) fn try_create_coordination_object(
        &self,
        relative: &str,
        bytes: &[u8],
    ) -> Result<Option<UpdateVersion>> {
        match self.write_bytes_with_mode(relative, bytes, PutMode::Create) {
            Ok(result) => Ok(Some(UpdateVersion::from(result))),
            Err(BorsukError::ConcurrentModification { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Read an object fresh, bypassing the read-through cache, returning `None`
    /// when it does not exist. Used for coordination objects whose content changes
    /// under a stable path (heartbeats, leases).
    pub(crate) fn read_object_fresh(&self, relative: &str) -> Result<Option<Vec<u8>>> {
        match self.read_bytes_uncached(relative) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(BorsukError::ObjectStoreNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Read a mutable coordination object and retain the backend version token
    /// required for a compare-and-swap update. This bypasses the read-through
    /// cache because lane heads change under a stable path.
    pub(crate) fn read_coordination_object(
        &self,
        relative: &str,
    ) -> Result<Option<CoordinationObject>> {
        let location = self.resolve(relative)?;
        let result = match self
            .runtime
            .block_on(async { self.store.get_opts(&location, GetOptions::default()).await })
        {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(map_object_store_error(relative, err)),
        };
        let object_bytes = result.meta.size;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = self
            .runtime
            .block_on(result.bytes())
            .map(|bytes| bytes.to_vec())
            .map_err(|err| map_object_store_error(relative, err))?;
        self.storage_trace
            .record(StorageAccessEvent::observed_read(
                relative,
                physical_format_for_path(relative),
                object_bytes,
                1,
                bytes.len() as u64,
            ))?;
        Ok(Some(CoordinationObject { bytes, version }))
    }

    /// Create a coordination object or conditionally replace the exact version
    /// returned by [`Self::read_coordination_object`].
    pub(crate) fn write_coordination_object(
        &self,
        relative: &str,
        bytes: &[u8],
        expected: Option<UpdateVersion>,
    ) -> Result<UpdateVersion> {
        let mode = expected.clone().map_or(PutMode::Create, PutMode::Update);
        match self.write_bytes_with_mode(relative, bytes, mode) {
            Ok(result) => Ok(UpdateVersion::from(result)),
            Err(BorsukError::ObjectStore(object_store::Error::NotImplemented { .. }))
                if expected.is_some() =>
            {
                // LocalFileSystem does not implement conditional update. Keep
                // its fallback correct across processes on the same filesystem;
                // production object stores continue through native ETag CAS.
                let _guard = COORDINATION_FALLBACK_LOCK
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let _file_guard = self.lock_local_coordination_path(relative)?;
                let current = self.coordination_update_version(relative)?;
                if current != expected {
                    return Err(BorsukError::ConcurrentModification {
                        path: relative.to_string(),
                    });
                }
                self.write_bytes_with_mode(relative, bytes, PutMode::Overwrite)
                    .map(UpdateVersion::from)
            }
            Err(error) => Err(error),
        }
    }

    fn lock_local_coordination_path(&self, relative: &str) -> Result<Option<File>> {
        let local_root = if has_uri_scheme(&self.uri) {
            let url = Url::parse(&self.uri).map_err(|error| {
                BorsukError::InvalidStorage(format!(
                    "invalid storage URI `{}` while locking coordination path: {error}",
                    self.uri
                ))
            })?;
            if url.scheme() != "file" {
                return Ok(None);
            }
            url.to_file_path().map_err(|()| {
                BorsukError::InvalidStorage(format!(
                    "file storage URI `{}` is not a local path",
                    self.uri
                ))
            })?
        } else {
            PathBuf::from(&self.uri)
        };
        let canonical_root = fs::canonicalize(&local_root).map_err(|source| BorsukError::Io {
            path: local_root,
            source,
        })?;
        let identity = format!(
            "{}\0{}\0{relative}",
            canonical_root.display(),
            self.prefix.as_ref()
        );
        let lock_root = env::temp_dir().join("borsuk-coordination-locks");
        fs::create_dir_all(&lock_root).map_err(|source| BorsukError::Io {
            path: lock_root.clone(),
            source,
        })?;
        let lock_path = lock_root.join(format!("{}.lock", blake3::hash(identity.as_bytes())));
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|source| BorsukError::Io {
                path: lock_path.clone(),
                source,
            })?;
        lock_file.lock().map_err(|source| BorsukError::Io {
            path: lock_path,
            source,
        })?;
        Ok(Some(lock_file))
    }

    fn coordination_update_version(&self, relative: &str) -> Result<Option<UpdateVersion>> {
        let location = self.resolve(relative)?;
        match self
            .runtime
            .block_on(async { self.store.head(&location).await })
        {
            Ok(meta) => Ok(Some(UpdateVersion {
                e_tag: meta.e_tag,
                version: meta.version,
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(map_object_store_error(relative, error)),
        }
    }

    fn write_bytes_with_mode(
        &self,
        relative: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<PutResult> {
        self.invalidate_cached_object_size(relative);
        let location = self.resolve(relative)?;
        let payload = PutPayload::from(Bytes::copy_from_slice(bytes));
        let requests_before = self.request_counts();
        let result = self
            .runtime
            .block_on(async {
                self.store
                    .put_opts(
                        &location,
                        payload,
                        PutOptions {
                            mode,
                            ..Default::default()
                        },
                    )
                    .await
            })
            .map_err(|err| map_conditional_put_error(relative, err))?;
        self.write_cache_file(relative, bytes)?;
        self.storage_trace
            .record(StorageAccessEvent::observed_write(
                relative,
                physical_format_for_path(relative),
                bytes.len() as u64,
                self.request_counts().delta(&requests_before).total(),
            ))?;
        Ok(result)
    }

    fn write_bytes_multipart(&self, relative: &str, bytes: &[u8]) -> Result<PutResult> {
        let location = self.resolve(relative)?;
        let requests_before = self.request_counts();
        let result = self
            .runtime
            .block_on(async {
                let mut upload = self.store.put_multipart(&location).await?;
                for chunk in bytes.chunks(MULTIPART_PART_BYTES) {
                    if let Err(err) = upload
                        .put_part(PutPayload::from(Bytes::copy_from_slice(chunk)))
                        .await
                    {
                        let _ = upload.abort().await;
                        return Err(err);
                    }
                }
                upload.complete().await
            })
            .map_err(|err| map_object_store_error(relative, err))?;
        self.write_cache_file(relative, bytes)?;
        self.storage_trace
            .record(StorageAccessEvent::observed_write(
                relative,
                physical_format_for_path(relative),
                bytes.len() as u64,
                self.request_counts().delta(&requests_before).total(),
            ))?;
        Ok(result)
    }

    fn read_bytes_uncached(&self, relative: &str) -> Result<Vec<u8>> {
        let requests_before = self.request_counts();
        let location = self.resolve(relative)?;
        let result = self
            .runtime
            .block_on(async { self.store.get_opts(&location, GetOptions::default()).await })
            .map_err(|err| map_object_store_error(relative, err))?;
        let size = result.meta.size;
        let bytes = self
            .runtime
            .block_on(result.bytes())
            .map(|bytes| bytes.to_vec())
            .map_err(|err| map_object_store_error(relative, err))?;
        self.write_cache_file(relative, &bytes)?;
        self.storage_trace
            .record(StorageAccessEvent::observed_read(
                relative,
                physical_format_for_path(relative),
                size,
                self.request_counts().delta(&requests_before).total(),
                bytes.len() as u64,
            ))?;
        Ok(bytes)
    }

    pub(crate) fn read_bytes_with_cache_status(&self, relative: &str) -> Result<ReadBytes> {
        if let Some(bytes) = self.read_cache_file(relative)? {
            self.storage_trace.record(StorageAccessEvent::cached_read(
                relative,
                physical_format_for_path(relative),
                bytes.len() as u64,
            ))?;
            return Ok(ReadBytes {
                bytes,
                cache_hit: true,
                cache_repaired: false,
            });
        }

        let bytes = self.read_bytes_uncached(relative)?;
        Ok(ReadBytes {
            bytes,
            cache_hit: false,
            cache_repaired: false,
        })
    }

    pub(crate) fn read_bytes_with_cache_status_and_checksum(
        &self,
        relative: &str,
        expected_checksum: &str,
    ) -> Result<ReadBytes> {
        let read = self.read_bytes_with_cache_status(relative)?;
        let actual_checksum = blake3::hash(&read.bytes).to_hex().to_string();
        if actual_checksum == expected_checksum {
            return Ok(read);
        }
        if !read.cache_hit {
            return Err(BorsukError::ChecksumMismatch {
                path: relative.to_string(),
                expected: expected_checksum.to_string(),
                actual: actual_checksum,
            });
        }

        self.delete_cache_file(relative)?;
        let bytes = self.read_bytes_uncached(relative)?;
        let actual_checksum = blake3::hash(&bytes).to_hex().to_string();
        if actual_checksum != expected_checksum {
            return Err(BorsukError::ChecksumMismatch {
                path: relative.to_string(),
                expected: expected_checksum.to_string(),
                actual: actual_checksum,
            });
        }
        self.write_cache_file(relative, &bytes)?;
        Ok(ReadBytes {
            bytes,
            cache_hit: false,
            cache_repaired: true,
        })
    }

    /// Read a validated object only when it is already present in the local
    /// disk cache. A miss or corrupt cache entry returns `None` and never
    /// reaches the backing object store, which lets mixed execution fall back
    /// to its storage scan without turning a graph-cache miss into network I/O.
    pub(crate) fn read_cached_bytes_with_checksum(
        &self,
        relative: &str,
        expected_checksum: &str,
    ) -> Result<Option<Vec<u8>>> {
        let Some(bytes) = self.read_cache_file(relative)? else {
            return Ok(None);
        };
        if blake3::hash(&bytes).to_hex().as_str() == expected_checksum {
            self.storage_trace.record(StorageAccessEvent::cached_read(
                relative,
                physical_format_for_path(relative),
                bytes.len() as u64,
            ))?;
            return Ok(Some(bytes));
        }
        self.delete_cache_file(relative)?;
        Ok(None)
    }

    pub(crate) fn has_cached_object(&self, relative: &str) -> bool {
        self.cache_path(relative).is_some_and(|path| path.is_file())
    }

    /// Read a content-addressed object whose size is already present in routing
    /// metadata. Avoids a redundant HEAD and performs exactly one object GET on
    /// a cache miss.
    pub(crate) fn read_known_size_with_cache_status_and_checksum(
        &self,
        relative: &str,
        known_size: u64,
        expected_checksum: &str,
    ) -> Result<ReadBytes> {
        let mut repaired = false;
        if let Some(bytes) = self.read_cache_file(relative)? {
            let actual_checksum = blake3::hash(&bytes).to_hex().to_string();
            if actual_checksum == expected_checksum {
                self.storage_trace.record(StorageAccessEvent::cached_read(
                    relative,
                    physical_format_for_path(relative),
                    bytes.len() as u64,
                ))?;
                return Ok(ReadBytes {
                    bytes,
                    cache_hit: true,
                    cache_repaired: false,
                });
            }
            self.delete_cache_file(relative)?;
            repaired = true;
        }

        let location = self.resolve(relative)?;
        let bytes = self
            .runtime
            .block_on(async { self.store.get_range(&location, 0..known_size).await })
            .map_err(|err| map_object_store_error(relative, err))?
            .to_vec();
        if bytes.len() as u64 != known_size {
            return Err(BorsukError::InvalidStorage(format!(
                "object `{relative}` returned {} bytes, expected {known_size}",
                bytes.len()
            )));
        }
        let actual_checksum = blake3::hash(&bytes).to_hex().to_string();
        if actual_checksum != expected_checksum {
            return Err(BorsukError::ChecksumMismatch {
                path: relative.to_string(),
                expected: expected_checksum.to_string(),
                actual: actual_checksum,
            });
        }
        self.write_cache_file(relative, &bytes)?;
        self.storage_trace
            .record(StorageAccessEvent::observed_read(
                relative,
                physical_format_for_path(relative),
                known_size,
                1,
                bytes.len() as u64,
            ))?;
        Ok(ReadBytes {
            bytes,
            cache_hit: false,
            cache_repaired: repaired,
        })
    }

    pub(crate) fn prefetch_read_bytes_with_cache_status_and_checksum(
        &self,
        relative: String,
        expected_checksum: String,
        semaphore: Arc<Semaphore>,
    ) -> PrefetchedRead {
        let context = PrefetchReadContext::from_storage(self);
        let handle_relative = relative.clone();
        let handle = self.runtime.spawn(async move {
            let _permit = semaphore.acquire_owned().await.map_err(|err| {
                BorsukError::InvalidStorage(format!("prefetch semaphore closed: {err}"))
            })?;
            context
                .read_bytes_with_cache_status_and_checksum(&relative, &expected_checksum)
                .await
        });
        PrefetchedRead {
            relative: handle_relative,
            handle: Some(handle),
        }
    }

    pub(crate) fn consume_prefetched_read(&self, mut read: PrefetchedRead) -> Result<ReadBytes> {
        let relative = std::mem::take(&mut read.relative);
        let handle = read.handle.take().ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "prefetched read `{relative}` was already consumed"
            ))
        })?;
        self.runtime.block_on(handle).map_err(|err| {
            BorsukError::InvalidStorage(format!("prefetched read `{relative}` task failed: {err}"))
        })?
    }

    pub(crate) fn read_range(&self, relative: &str, range: Range<u64>) -> Result<Vec<u8>> {
        let cacheable = !is_mutable_lane_head(relative);
        if cacheable && let Some(bytes) = self.read_cache_file(relative)? {
            let start = usize::try_from(range.start).map_err(|_| {
                BorsukError::InvalidStorage(format!(
                    "range start {} does not fit usize",
                    range.start
                ))
            })?;
            let end = usize::try_from(range.end).map_err(|_| {
                BorsukError::InvalidStorage(format!("range end {} does not fit usize", range.end))
            })?;
            if end > bytes.len() || start > end {
                return Err(BorsukError::InvalidStorage(format!(
                    "range {}..{} is outside cached object `{relative}` of {} bytes",
                    range.start,
                    range.end,
                    bytes.len()
                )));
            }
            let selected = bytes[start..end].to_vec();
            self.storage_trace.record(StorageAccessEvent::cached_read(
                relative,
                physical_format_for_path(relative),
                bytes.len() as u64,
            ))?;
            return Ok(selected);
        }

        let range_cache_key = range_cache_key(relative, range.start, range.end);
        if cacheable && let Some(bytes) = self.read_cache_file(&range_cache_key)? {
            self.storage_trace.record(StorageAccessEvent::cached_read(
                relative,
                physical_format_for_path(relative),
                0,
            ))?;
            return Ok(bytes);
        }

        let requested_bytes = range.end.saturating_sub(range.start);
        let location = self.resolve(relative)?;
        let result = self
            .runtime
            .block_on(async {
                self.store
                    .get_opts(
                        &location,
                        GetOptions::new().with_range(Some(GetRange::Bounded(range))),
                    )
                    .await
            })
            .map_err(|err| map_object_store_error(relative, err))?;
        let object_bytes = result.meta.size;
        let bytes = self
            .runtime
            .block_on(result.bytes())
            .map(|bytes| bytes.to_vec())
            .map_err(|err| map_object_store_error(relative, err))?;
        if cacheable {
            self.write_cache_file(&range_cache_key, &bytes)?;
        }
        self.storage_trace
            .record(StorageAccessEvent::observed_read(
                relative,
                physical_format_for_path(relative),
                object_bytes,
                1,
                requested_bytes,
            ))?;
        Ok(bytes)
    }

    pub(crate) fn evict_cached_range(&self, relative: &str, range: Range<u64>) -> Result<()> {
        self.delete_cache_file(relative)?;
        self.delete_cache_file(&range_cache_key(relative, range.start, range.end))
    }

    pub(crate) fn read_suffix(&self, relative: &str, length: u64) -> Result<ReadBytes> {
        let cacheable = !is_mutable_lane_head(relative);
        if cacheable && let Some(bytes) = self.read_cache_file(relative)? {
            let length = usize::try_from(length)
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            let selected = bytes[bytes.len() - length..].to_vec();
            self.storage_trace.record(StorageAccessEvent::cached_read(
                relative,
                physical_format_for_path(relative),
                bytes.len() as u64,
            ))?;
            return Ok(ReadBytes {
                bytes: selected,
                cache_hit: true,
                cache_repaired: false,
            });
        }
        let suffix_cache_key = range_cache_key(relative, u64::MAX, length);
        if cacheable && let Some(bytes) = self.read_cache_file(&suffix_cache_key)? {
            self.storage_trace.record(StorageAccessEvent::cached_read(
                relative,
                physical_format_for_path(relative),
                0,
            ))?;
            return Ok(ReadBytes {
                bytes,
                cache_hit: true,
                cache_repaired: false,
            });
        }
        let location = self.resolve(relative)?;
        let result = self
            .runtime
            .block_on(async {
                self.store
                    .get_opts(
                        &location,
                        GetOptions::new().with_range(Some(GetRange::Suffix(length))),
                    )
                    .await
            })
            .map_err(|err| map_object_store_error(relative, err))?;
        let object_bytes = result.meta.size;
        let bytes = self
            .runtime
            .block_on(result.bytes())
            .map(|bytes| bytes.to_vec())
            .map_err(|err| map_object_store_error(relative, err))?;
        if cacheable {
            self.write_cache_file(&suffix_cache_key, &bytes)?;
        }
        self.storage_trace
            .record(StorageAccessEvent::observed_read(
                relative,
                physical_format_for_path(relative),
                object_bytes,
                1,
                bytes.len() as u64,
            ))?;
        Ok(ReadBytes {
            bytes,
            cache_hit: false,
            cache_repaired: false,
        })
    }

    /// Fetch several byte ranges of one object using the object-store crate's
    /// sidecar-specific bounded coalescing policy.
    ///
    /// Returns the bytes for each requested range, in the same order as
    /// `ranges`. When the object is present in the local cache the ranges are
    /// sliced from the cached bytes. Otherwise nearby ranges are merged into
    /// physical range GETs and fetched in parallel. `bytes_fetched` reports the
    /// merged physical spans (including bytes between requested rows), and the
    /// request counter observes each physical GET.
    pub(crate) fn read_ranges(&self, relative: &str, ranges: &[Range<u64>]) -> Result<ReadRanges> {
        self.read_ranges_with_policy(
            relative,
            ranges,
            SIDECAR_RANGE_COALESCE_BYTES,
            SIDECAR_RANGE_MAX_PARALLEL,
        )
    }

    /// Fetch global exact-rerank rows with the same byte-bounded range plan as
    /// ordinary sidecars, but admit the complete production shortlist in one
    /// remote wave. The 4 MiB span cap remains authoritative per request.
    pub(crate) fn read_global_rerank_ranges(
        &self,
        relative: &str,
        ranges: &[Range<u64>],
    ) -> Result<ReadRanges> {
        self.read_ranges_with_policy(
            relative,
            ranges,
            GLOBAL_RERANK_RANGE_COALESCE_BYTES,
            GLOBAL_RERANK_RANGE_MAX_PARALLEL,
        )
    }

    fn read_ranges_with_policy(
        &self,
        relative: &str,
        ranges: &[Range<u64>],
        max_gap: u64,
        max_parallel: usize,
    ) -> Result<ReadRanges> {
        let cacheable = !is_mutable_lane_head(relative);
        if cacheable && let Some(bytes) = self.read_cache_file(relative)? {
            let mut out = Vec::with_capacity(ranges.len());
            for range in ranges {
                let start = usize::try_from(range.start).map_err(|_| {
                    BorsukError::InvalidStorage(format!(
                        "range start {} does not fit usize",
                        range.start
                    ))
                })?;
                let end = usize::try_from(range.end).map_err(|_| {
                    BorsukError::InvalidStorage(format!(
                        "range end {} does not fit usize",
                        range.end
                    ))
                })?;
                if end > bytes.len() || start > end {
                    return Err(BorsukError::InvalidStorage(format!(
                        "range {}..{} is outside cached object `{relative}` of {} bytes",
                        range.start,
                        range.end,
                        bytes.len()
                    )));
                }
                out.push(bytes[start..end].to_vec());
            }
            self.storage_trace.record(StorageAccessEvent::cached_read(
                relative,
                physical_format_for_path(relative),
                bytes.len() as u64,
            ))?;
            return Ok(ReadRanges {
                chunks: out,
                cache_hit: true,
                bytes_fetched: 0,
            });
        }

        let bundle_key = range_bundle_cache_key(relative, ranges);
        if cacheable && let Some(bytes) = self.read_cache_file(&bundle_key)? {
            self.storage_trace.record(StorageAccessEvent::cached_read(
                relative,
                physical_format_for_path(relative),
                0,
            ))?;
            return Ok(ReadRanges {
                chunks: split_range_bundle(relative, ranges, &bytes)?,
                cache_hit: true,
                bytes_fetched: 0,
            });
        }

        let requests_before = self.request_counts();
        let location = self.resolve(relative)?;
        let physical_bytes = Arc::new(AtomicU64::new(0));
        let object_bytes = Arc::new(AtomicU64::new(0));
        let fetched = self
            .runtime
            .block_on(async {
                coalesce_bounded_ranges(
                    ranges,
                    |range| {
                        let store = Arc::clone(&self.store);
                        let location = location.clone();
                        let physical_bytes = Arc::clone(&physical_bytes);
                        let object_bytes = Arc::clone(&object_bytes);
                        async move {
                            let result = store
                                .get_opts(
                                    &location,
                                    GetOptions::new().with_range(Some(GetRange::Bounded(range))),
                                )
                                .await?;
                            object_bytes.fetch_max(result.meta.size, Ordering::Relaxed);
                            let bytes = result.bytes().await?;
                            physical_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                            Ok(bytes)
                        }
                    },
                    max_gap,
                    SIDECAR_MAX_PHYSICAL_RANGE_BYTES,
                    max_parallel,
                )
                .await
            })
            .map_err(|err| map_object_store_error(relative, err))?;
        let chunks = fetched
            .into_iter()
            .map(|bytes| bytes.to_vec())
            .collect::<Vec<_>>();
        let bytes_fetched = physical_bytes.load(Ordering::Relaxed);
        let bundle = chunks.concat();
        if cacheable {
            self.write_cache_file(&bundle_key, &bundle)?;
        }
        let request_count = self.request_counts().delta(&requests_before).gets;
        self.storage_trace
            .record(StorageAccessEvent::observed_read(
                relative,
                physical_format_for_path(relative),
                object_bytes.load(Ordering::Relaxed),
                request_count,
                bytes_fetched,
            ))?;
        Ok(ReadRanges {
            chunks,
            cache_hit: false,
            bytes_fetched,
        })
    }

    /// Read a projected subset of a Parquet object's columns (and, optionally, a
    /// subset of its rows) by fetching only the relevant column chunks over the
    /// object store — never the whole object. This is the object-store-native
    /// low-latency read: score from the compact `pq_codes` column, then rerank
    /// full vectors for a handful of rows, each a tight range read.
    ///
    /// `bytes_fetched` sums the Parquet footer plus the compressed projected
    /// column chunks in the row groups actually touched; it is the tunable,
    /// object-store-billed cost of the query.
    #[allow(dead_code)]
    pub(crate) fn read_parquet_columns_ranged(
        &self,
        relative: &str,
        size: u64,
        columns: RangedColumns<'_>,
        rows: Option<&[usize]>,
    ) -> Result<RangedParquetRead> {
        self.read_parquet_projected_ranged(relative, size, columns, rows, None)
    }

    /// Read only selected Parquet row groups and projected columns. Lexical
    /// roots address immutable posting blocks by row-group ordinal, so this
    /// path fetches their footer plus compressed column chunks—not the file.
    pub(crate) fn read_parquet_row_groups_ranged(
        &self,
        relative: &str,
        size: u64,
        columns: RangedColumns<'_>,
        row_groups: &[usize],
    ) -> Result<RangedParquetRead> {
        if row_groups.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "ranged Parquet read requires a row group".to_string(),
            ));
        }
        self.read_parquet_projected_ranged(relative, size, columns, None, Some(row_groups))
    }

    fn read_parquet_projected_ranged(
        &self,
        relative: &str,
        size: u64,
        columns: RangedColumns<'_>,
        rows: Option<&[usize]>,
        row_groups: Option<&[usize]>,
    ) -> Result<RangedParquetRead> {
        let logical_projection = match columns {
            RangedColumns::Keep(names) => names.join("|"),
            RangedColumns::DropVector => "*|-vector".to_string(),
        };
        let keep_column = |name: &str| match columns {
            RangedColumns::Keep(names) => names.contains(&name),
            RangedColumns::DropVector => name != "vector",
        };
        // Prefetch just the Parquet footer (metadata) with two small range reads
        // so the async reader never fetches the whole object to learn its layout.
        // Layout: [ FileMetaData thrift | metadata_len: u32 LE | b"PAR1" ].
        if size < 8 {
            return Err(BorsukError::InvalidStorage(format!(
                "object `{relative}` of {size} bytes is too small to be a parquet file"
            )));
        }
        let tail = self.read_range(relative, size - 8..size)?;
        let metadata_len = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]) as u64;
        if metadata_len + 8 > size {
            return Err(BorsukError::InvalidStorage(format!(
                "parquet `{relative}` footer length {metadata_len} exceeds object size {size}"
            )));
        }
        let metadata_bytes = self.read_range(relative, size - 8 - metadata_len..size - 8)?;
        let parquet_metadata = Arc::new(
            ParquetMetaDataReader::decode_metadata(&metadata_bytes).map_err(|err| {
                BorsukError::InvalidStorage(format!("decode parquet metadata `{relative}`: {err}"))
            })?,
        );
        // parquet's async range reader calls ColumnChunkMetaData::byte_range(),
        // which asserts (panics) when corrupt thrift metadata contains a
        // negative offset/length. Validate every chunk before handing metadata
        // to that reader so adversarial objects remain ordinary typed errors.
        for (row_group_index, row_group) in parquet_metadata.row_groups().iter().enumerate() {
            for (column_index, column) in row_group.columns().iter().enumerate() {
                let start = column
                    .dictionary_page_offset()
                    .unwrap_or_else(|| column.data_page_offset());
                let length = column.compressed_size();
                let valid_end = start
                    .checked_add(length)
                    .is_some_and(|end| end >= 0 && (end as u64) <= size);
                if start < 0 || length < 0 || !valid_end {
                    return Err(BorsukError::InvalidStorage(format!(
                        "parquet `{relative}` row group {row_group_index} column {column_index} \
                         has invalid byte range start={start} length={length} for object size {size}"
                    )));
                }
            }
        }
        let footer_bytes = 8 + metadata_len;

        let schema_descr = parquet_metadata.file_metadata().schema_descr();
        let roots: Vec<usize> = schema_descr
            .root_schema()
            .get_fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| keep_column(field.name()).then_some(index))
            .collect();
        let mask = ProjectionMask::roots(schema_descr, roots);
        let total_rows: usize = parquet_metadata
            .row_groups()
            .iter()
            .map(|group| group.num_rows() as usize)
            .sum();

        let arrow_metadata = catch_unwind(AssertUnwindSafe(|| {
            ArrowReaderMetadata::try_new(Arc::clone(&parquet_metadata), ArrowReaderOptions::new())
        }))
        .map_err(|_| {
            BorsukError::InvalidStorage(format!(
                "derive arrow metadata for `{relative}`: corrupt embedded Arrow metadata"
            ))
        })?
        .map_err(|err| {
            BorsukError::InvalidStorage(format!("derive arrow metadata for `{relative}`: {err}"))
        })?;

        let counter = Arc::new(AtomicU64::new(0));
        let reader = BorsukAsyncReader {
            context: PrefetchReadContext::from_storage(self),
            relative: relative.to_string(),
            metadata: Arc::clone(&parquet_metadata),
            bytes_fetched: Arc::clone(&counter),
        };

        let logical_rows_requested = rows.map_or_else(
            || {
                row_groups.map_or(total_rows, |groups| {
                    groups
                        .iter()
                        .filter_map(|group| parquet_metadata.row_groups().get(*group))
                        .map(|group| group.num_rows() as usize)
                        .sum()
                })
            },
            |rows| {
                let mut sorted = rows.to_vec();
                sorted.sort_unstable();
                sorted.dedup();
                sorted.len()
            },
        );
        let row_selection = if let Some(rows) = rows {
            format!("rows:{}", join_usize(rows))
        } else if let Some(groups) = row_groups {
            format!("row_groups:{}", join_usize(groups))
        } else {
            "all".to_string()
        };
        let selection = rows.map(|rows| {
            let mut sorted = rows.to_vec();
            sorted.sort_unstable();
            sorted.dedup();
            crate::format::row_selection_for_rows(&sorted, total_rows)
        });
        let selected_row_groups = row_groups.map(|groups| {
            let mut groups = groups.to_vec();
            groups.sort_unstable();
            groups.dedup();
            groups
        });
        let relative_owned = relative.to_string();

        let decode_started = Instant::now();
        let batches = self.runtime.block_on(async move {
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_metadata)
                    .with_projection(mask);
            if let Some(row_groups) = selected_row_groups {
                if row_groups
                    .iter()
                    .any(|row_group| *row_group >= parquet_metadata.num_row_groups())
                {
                    return Err(BorsukError::InvalidStorage(format!(
                        "ranged parquet read of `{relative_owned}` selects an out-of-range row group"
                    )));
                }
                builder = builder.with_row_groups(row_groups);
            }
            if let Some(selection) = selection {
                builder = builder.with_row_selection(selection);
            }
            let stream = builder.build().map_err(|err| {
                BorsukError::InvalidStorage(format!(
                    "build ranged parquet reader for `{relative_owned}`: {err}"
                ))
            })?;
            stream
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|err| {
                    BorsukError::InvalidStorage(format!(
                        "ranged parquet read of `{relative_owned}` failed: {err}"
                    ))
                })
        })?;

        let logical_rows_decoded = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        self.record_access_event(StorageAccessEvent::decode(
            relative,
            physical_format_for_path(relative),
            size,
            logical_projection,
            row_selection,
            logical_rows_requested as u64,
            logical_rows_decoded as u64,
            decode_started
                .elapsed()
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64,
        ))?;
        Ok(RangedParquetRead {
            batches,
            bytes_fetched: footer_bytes + counter.load(Ordering::Relaxed),
            total_rows,
        })
    }

    pub(crate) fn for_each_object(
        &self,
        relative_prefix: &str,
        mut visit: impl FnMut(StoredObject) -> Result<()> + Send,
    ) -> Result<()> {
        let prefix = self.resolve(relative_prefix)?;
        self.runtime.block_on(async {
            let mut stream = self.store.list(Some(&prefix));
            while let Some(meta) = stream
                .try_next()
                .await
                .map_err(|err| map_object_store_error(relative_prefix, err))?
            {
                visit(StoredObject {
                    path: self.relative_path(&meta.location)?,
                    size: meta.size,
                    last_modified: meta.last_modified,
                })?;
            }
            Ok(())
        })
    }

    pub(crate) fn list_objects(&self, relative_prefix: &str) -> Result<Vec<StoredObject>> {
        let mut objects = Vec::new();
        self.for_each_object(relative_prefix, |object| {
            objects.push(object);
            Ok(())
        })?;
        objects.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(objects)
    }

    pub(crate) fn delete_object(&self, relative: &str) -> Result<bool> {
        self.invalidate_cached_object_size(relative);
        let location = self.resolve(relative)?;
        match self
            .runtime
            .block_on(async { self.store.delete(&location).await })
        {
            Ok(()) => {
                self.delete_cache_file(relative)?;
                Ok(true)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(map_object_store_error(relative, err)),
        }
    }

    pub(crate) fn store_clock_now(&self) -> Result<chrono::DateTime<chrono::Utc>> {
        let relative = format!(
            "collection/clock-probes/{}.probe",
            uuid::Uuid::new_v4().simple()
        );
        self.write_bytes_if_absent(&relative, b"")?;
        let location = self.resolve(&relative)?;
        let head = self
            .runtime
            .block_on(async { self.store.head(&location).await })
            .map_err(|error| map_object_store_error(&relative, error));
        let cleanup = self.delete_object(&relative);
        match (head, cleanup) {
            (Ok(meta), Ok(_)) => Ok(meta.last_modified),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    fn invalidate_cached_object_size(&self, relative: &str) {
        self.immutable_object_sizes.remove(relative);
    }

    fn exists(&self, relative: &str) -> Result<bool> {
        let location = self.resolve(relative)?;
        match self
            .runtime
            .block_on(async { self.store.head(&location).await })
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(map_object_store_error(relative, err)),
        }
    }

    fn resolve(&self, relative: &str) -> Result<ObjectPath> {
        let relative = relative.trim_matches('/');
        let path = if self.prefix.as_ref().is_empty() {
            relative.to_string()
        } else if relative.is_empty() {
            self.prefix.as_ref().to_string()
        } else {
            format!("{}/{relative}", self.prefix.as_ref())
        };

        ObjectPath::parse(path).map_err(|err| {
            BorsukError::InvalidStorage(format!("invalid object path `{relative}`: {err}"))
        })
    }

    fn relative_path(&self, location: &ObjectPath) -> Result<String> {
        let path = location.as_ref();
        let prefix = self.prefix.as_ref();
        if prefix.is_empty() {
            return Ok(path.to_string());
        }

        path.strip_prefix(prefix)
            .and_then(|value| value.strip_prefix('/'))
            .map(ToString::to_string)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "listed object `{path}` is outside index prefix `{prefix}`"
                ))
            })
    }

    fn cache_path(&self, relative: &str) -> Option<PathBuf> {
        if is_mutable_lane_head(relative) {
            return None;
        }
        let cache_dir = self.cache_dir.as_ref()?;
        let mut path = cache_dir.clone();
        for component in Path::new(relative.trim_matches('/')).components() {
            if let std::path::Component::Normal(value) = component {
                path.push(value);
            }
        }
        Some(path)
    }

    fn read_cache_file(&self, relative: &str) -> Result<Option<Vec<u8>>> {
        let Some(path) = self.cache_path(relative) else {
            return Ok(None);
        };

        match fs::read(&path) {
            Ok(bytes) => {
                self.cache_read_counters.record_disk(bytes.len());
                // Recency refresh is best-effort; valid cached bytes remain usable.
                let _refresh_result = self.touch_cache_file(&path);
                Ok(Some(bytes))
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(BorsukError::InvalidStorage(format!(
                "failed to read cache file `{}`: {err}",
                path.display()
            ))),
        }
    }

    fn write_cache_file(&self, relative: &str, bytes: &[u8]) -> Result<()> {
        let Some(path) = self.cache_path(relative) else {
            return Ok(());
        };

        atomic_write_cache_file(&path, bytes)?;
        self.enforce_cache_max_bytes()
    }

    fn delete_cache_file(&self, relative: &str) -> Result<()> {
        let Some(path) = self.cache_path(relative) else {
            return Ok(());
        };

        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(BorsukError::InvalidStorage(format!(
                "failed to delete cache file `{}`: {err}",
                path.display()
            ))),
        }
    }

    fn touch_cache_file(&self, path: &Path) -> Result<()> {
        if self.cache_max_bytes.is_none() {
            return Ok(());
        }

        refresh_cache_file_mtime(path).map_err(|err| {
            BorsukError::InvalidStorage(format!(
                "failed to refresh cache file `{}`: {err}",
                path.display()
            ))
        })
    }

    fn enforce_cache_max_bytes(&self) -> Result<()> {
        enforce_cache_max_bytes(self.cache_dir.as_deref(), self.cache_max_bytes)
    }
}

fn is_mutable_lane_head(relative: &str) -> bool {
    let relative = relative.trim_matches('/');
    relative.starts_with("lane-log/lanes/") && relative.ends_with("/HEAD")
}

fn map_conditional_put_error(relative: &str, err: object_store::Error) -> BorsukError {
    match err {
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. } => {
            BorsukError::ConcurrentModification {
                path: relative.to_string(),
            }
        }
        err => map_object_store_error(relative, err),
    }
}

fn range_cache_key(relative: &str, start: u64, end: u64) -> String {
    let object = blake3::hash(relative.as_bytes()).to_hex();
    format!(".borsuk-ranges/{object}/{start}-{end}")
}

fn atomic_write_cache_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        BorsukError::InvalidStorage(format!(
            "cache file `{}` has no parent directory",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|err| {
        BorsukError::InvalidStorage(format!(
            "failed to create cache directory `{}`: {err}",
            parent.display()
        ))
    })?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|err| {
        BorsukError::InvalidStorage(format!(
            "failed to create temporary cache file for `{}`: {err}",
            path.display()
        ))
    })?;
    temporary.write_all(bytes).map_err(|err| {
        BorsukError::InvalidStorage(format!(
            "failed to write temporary cache file for `{}`: {err}",
            path.display()
        ))
    })?;
    temporary.flush().map_err(|err| {
        BorsukError::InvalidStorage(format!(
            "failed to flush temporary cache file for `{}`: {err}",
            path.display()
        ))
    })?;
    temporary.persist(path).map_err(|err| {
        BorsukError::InvalidStorage(format!(
            "failed to publish cache file `{}` atomically: {}",
            path.display(),
            err.error
        ))
    })?;
    Ok(())
}

fn range_bundle_cache_key(relative: &str, ranges: &[Range<u64>]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(relative.as_bytes());
    for range in ranges {
        hasher.update(&range.start.to_le_bytes());
        hasher.update(&range.end.to_le_bytes());
    }
    format!(".borsuk-range-bundles/{}", hasher.finalize().to_hex())
}

fn split_range_bundle(relative: &str, ranges: &[Range<u64>], bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let expected = ranges.iter().try_fold(0_usize, |total, range| {
        let range_len = range.end.checked_sub(range.start).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "cached range for `{relative}` has end before start"
            ))
        })?;
        let len = usize::try_from(range_len).map_err(|_| {
            BorsukError::InvalidStorage(format!(
                "cached range length for `{relative}` does not fit usize"
            ))
        })?;
        total.checked_add(len).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "cached range bundle length for `{relative}` overflowed"
            ))
        })
    })?;
    if bytes.len() != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "cached range bundle for `{relative}` has {} bytes, expected {expected}",
            bytes.len()
        )));
    }
    let mut cursor = 0_usize;
    Ok(ranges
        .iter()
        .map(|range| {
            let len = usize::try_from(range.end - range.start).expect("validated range length");
            let chunk = bytes[cursor..cursor + len].to_vec();
            cursor += len;
            chunk
        })
        .collect())
}

#[derive(Debug)]
struct CacheFile {
    path: PathBuf,
    bytes: u64,
    modified: SystemTime,
}

fn enforce_cache_max_bytes(cache_dir: Option<&Path>, cache_max_bytes: Option<u64>) -> Result<()> {
    let (Some(cache_dir), Some(cache_max_bytes)) = (cache_dir, cache_max_bytes) else {
        return Ok(());
    };
    if !cache_dir.exists() {
        return Ok(());
    }

    let mut files = Vec::new();
    collect_cache_files(cache_dir, &mut files).map_err(|err| {
        BorsukError::InvalidStorage(format!(
            "failed to scan cache directory `{}`: {err}",
            cache_dir.display()
        ))
    })?;
    let mut total_bytes = files.iter().map(|file| file.bytes).sum::<u64>();
    if total_bytes <= cache_max_bytes {
        return Ok(());
    }

    files.sort_by(|left, right| {
        left.modified
            .cmp(&right.modified)
            .then_with(|| left.path.cmp(&right.path))
    });
    for file in files {
        if total_bytes <= cache_max_bytes {
            break;
        }
        match fs::remove_file(&file.path) {
            Ok(()) => {
                total_bytes = total_bytes.saturating_sub(file.bytes);
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(BorsukError::InvalidStorage(format!(
                    "failed to evict cache file `{}`: {err}",
                    file.path.display()
                )));
            }
        }
    }

    Ok(())
}

fn refresh_cache_file_mtime(path: &Path) -> io::Result<()> {
    let file = match OpenOptions::new().append(true).open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    file.set_modified(SystemTime::now())
}

fn collect_cache_files(path: &Path, files: &mut Vec<CacheFile>) -> io::Result<()> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    for entry in entries {
        let path = entry?.path();
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        if metadata.is_dir() {
            collect_cache_files(&path, files)?;
        } else if metadata.is_file() {
            files.push(CacheFile {
                path,
                bytes: metadata.len(),
                modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            });
        }
    }
    Ok(())
}

fn map_object_store_error(relative: &str, err: object_store::Error) -> BorsukError {
    match err {
        object_store::Error::NotFound { .. } => BorsukError::ObjectStoreNotFound {
            path: relative.to_string(),
            source: err,
        },
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => BorsukError::ObjectStorePermissionDenied {
            path: relative.to_string(),
            source: err,
        },
        object_store::Error::Generic { .. } | object_store::Error::JoinError { .. } => {
            BorsukError::ObjectStoreRetryable {
                path: relative.to_string(),
                source: err,
            }
        }
        err => BorsukError::ObjectStore(err),
    }
}

fn is_object_store_not_found(err: &BorsukError) -> bool {
    matches!(
        err,
        BorsukError::ObjectStoreNotFound { .. }
            | BorsukError::ObjectStore(object_store::Error::NotFound { .. })
    )
}

fn store_from_uri(uri: &str) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    if has_uri_scheme(uri) {
        let url = Url::parse(uri).map_err(|err| {
            BorsukError::InvalidStorage(format!("invalid storage URI `{uri}`: {err}"))
        })?;
        let (store, prefix) = parse_url_opts(&url, env::vars())?;
        return Ok((store.into(), prefix));
    }

    let path = Path::new(uri);
    fs::create_dir_all(path).map_err(|source| BorsukError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok((
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(path)?),
        ObjectPath::parse("").map_err(|err| {
            BorsukError::InvalidStorage(format!("invalid local storage root `{uri}`: {err}"))
        })?,
    ))
}

fn has_uri_scheme(uri: &str) -> bool {
    if looks_like_windows_drive_path(uri) {
        return false;
    }

    uri.split_once(':').is_some_and(|(scheme, _)| {
        !scheme.is_empty()
            && scheme
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
    })
}

fn routing_layer_page_unchanged(
    previous: &Manifest,
    routing_page_fanout: usize,
    page_ordinal: usize,
    segments: &[SegmentSummary],
) -> bool {
    if previous.routing_page_fanout != routing_page_fanout {
        return false;
    }
    let Some(start) = page_ordinal.checked_mul(previous.routing_page_fanout) else {
        return false;
    };
    let end = start + segments.len();
    previous
        .segments
        .get(start..end)
        .is_some_and(|previous_segments| previous_segments == segments)
}

fn routing_layer_page_centroid(dimensions: usize, segments: &[SegmentSummary]) -> Vec<f32> {
    let total_objects = segments
        .iter()
        .map(|segment| segment.object_count)
        .sum::<usize>()
        .max(1);
    let mut centroid = vec![0.0_f32; dimensions];
    for segment in segments {
        let weight = segment.object_count as f32 / total_objects as f32;
        crate::metric::add_scaled_assign_simd(&mut centroid, &segment.centroid, weight);
    }
    centroid
}

fn routing_layer_page_radius(manifest: &Manifest, segments: &[SegmentSummary]) -> Result<f32> {
    let centroid = routing_layer_page_centroid(manifest.config.dimensions, segments);
    // Derived centroid over stored, already-validated segment centroids — skip
    // the finite/dim re-scan on this radius fold.
    segments.iter().try_fold(0.0_f32, |radius, segment| {
        let center_distance = manifest
            .config
            .metric
            .centroid_geometry_distance_unchecked(&centroid, &segment.centroid)?;
        Ok(radius.max(center_distance + segment.radius))
    })
}

fn routing_layer_page_bounds_min(dimensions: usize, segments: &[SegmentSummary]) -> Vec<f32> {
    let mut bounds = vec![f32::INFINITY; dimensions];
    for segment in segments {
        if segment.bounds_min.len() != dimensions {
            return Vec::new();
        }
        for (target, source) in bounds.iter_mut().zip(&segment.bounds_min) {
            *target = target.min(*source);
        }
    }
    bounds
}

fn routing_layer_page_bounds_max(dimensions: usize, segments: &[SegmentSummary]) -> Vec<f32> {
    let mut bounds = vec![f32::NEG_INFINITY; dimensions];
    for segment in segments {
        if segment.bounds_max.len() != dimensions {
            return Vec::new();
        }
        for (target, source) in bounds.iter_mut().zip(&segment.bounds_max) {
            *target = target.max(*source);
        }
    }
    bounds
}

fn routing_layer_page_id_bloom(segments: &[SegmentSummary]) -> Vec<u8> {
    let mut bloom = vec![0_u8; crate::manifest::SEGMENT_ID_BLOOM_BYTES];
    for segment in segments {
        if segment.id_bloom.len() != bloom.len() {
            return Vec::new();
        }
        for (target, source) in bloom.iter_mut().zip(&segment.id_bloom) {
            *target |= source;
        }
    }
    bloom
}

fn routing_layer_page_vector_signature_bloom(segments: &[SegmentSummary]) -> Vec<u8> {
    let mut bloom = vec![0_u8; crate::manifest::SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES];
    for segment in segments {
        if segment.vector_signature_bloom.len() != bloom.len() {
            return Vec::new();
        }
        for (target, source) in bloom.iter_mut().zip(&segment.vector_signature_bloom) {
            *target |= source;
        }
    }
    bloom
}

fn routing_layer_page_level_mask(segments: &[SegmentSummary]) -> u64 {
    let mut mask = 0_u64;
    for segment in segments {
        if segment.level >= u64::BITS as u8 {
            return u64::MAX;
        }
        mask |= 1_u64 << segment.level;
    }
    mask
}

fn routing_layer_page_record_count(segments: &[SegmentSummary]) -> usize {
    segments.iter().map(|segment| segment.object_count).sum()
}

fn routing_layer_page_segment_bytes(segments: &[SegmentSummary]) -> u64 {
    segments.iter().map(|segment| segment.size_bytes).sum()
}

fn routing_layer_page_vector_bytes(segments: &[SegmentSummary]) -> u64 {
    segments
        .iter()
        .map(|segment| segment.vector_size_bytes)
        .sum()
}

fn routing_layer_page_graph_bytes(segments: &[SegmentSummary]) -> u64 {
    segments
        .iter()
        .map(|segment| segment.graph_size_bytes)
        .sum()
}

fn routing_layer_page_sparse_encoded_vectors(segments: &[SegmentSummary]) -> usize {
    segments.iter().map(|segment| segment.sparse_encoded).sum()
}

fn routing_layer_page_dense_encoded_vectors(segments: &[SegmentSummary]) -> usize {
    segments.iter().map(|segment| segment.dense_encoded).sum()
}

fn routing_page_refs_centroid(dimensions: usize, page_refs: &[RoutingLayerPageRef]) -> Vec<f32> {
    let total_records = page_refs
        .iter()
        .map(|page_ref| page_ref.page_records)
        .sum::<usize>()
        .max(1);
    let mut centroid = vec![0.0_f32; dimensions];
    for page_ref in page_refs {
        let weight = page_ref.page_records as f32 / total_records as f32;
        crate::metric::add_scaled_assign_simd(&mut centroid, &page_ref.centroid, weight);
    }
    centroid
}

fn routing_page_refs_radius(manifest: &Manifest, page_refs: &[RoutingLayerPageRef]) -> Result<f32> {
    let centroid = routing_page_refs_centroid(manifest.config.dimensions, page_refs);
    // Derived centroid over stored, already-validated page-ref centroids.
    page_refs.iter().try_fold(0.0_f32, |radius, page_ref| {
        let center_distance = manifest
            .config
            .metric
            .centroid_geometry_distance_unchecked(&centroid, &page_ref.centroid)?;
        Ok(radius.max(center_distance + page_ref.radius))
    })
}

fn routing_page_refs_bounds_min(dimensions: usize, page_refs: &[RoutingLayerPageRef]) -> Vec<f32> {
    let mut bounds = vec![f32::INFINITY; dimensions];
    for page_ref in page_refs {
        if page_ref.bounds_min.len() != dimensions {
            return Vec::new();
        }
        for (target, source) in bounds.iter_mut().zip(&page_ref.bounds_min) {
            *target = target.min(*source);
        }
    }
    bounds
}

fn routing_page_refs_bounds_max(dimensions: usize, page_refs: &[RoutingLayerPageRef]) -> Vec<f32> {
    let mut bounds = vec![f32::NEG_INFINITY; dimensions];
    for page_ref in page_refs {
        if page_ref.bounds_max.len() != dimensions {
            return Vec::new();
        }
        for (target, source) in bounds.iter_mut().zip(&page_ref.bounds_max) {
            *target = target.max(*source);
        }
    }
    bounds
}

fn routing_page_refs_id_bloom(page_refs: &[RoutingLayerPageRef]) -> Vec<u8> {
    let mut bloom = vec![0_u8; crate::manifest::SEGMENT_ID_BLOOM_BYTES];
    for page_ref in page_refs {
        if page_ref.id_bloom.len() != bloom.len() {
            return Vec::new();
        }
        for (target, source) in bloom.iter_mut().zip(&page_ref.id_bloom) {
            *target |= source;
        }
    }
    bloom
}

fn routing_page_refs_vector_signature_bloom(page_refs: &[RoutingLayerPageRef]) -> Vec<u8> {
    let mut bloom = vec![0_u8; crate::manifest::SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES];
    for page_ref in page_refs {
        if page_ref.vector_signature_bloom.len() != bloom.len() {
            return Vec::new();
        }
        for (target, source) in bloom.iter_mut().zip(&page_ref.vector_signature_bloom) {
            *target |= source;
        }
    }
    bloom
}

fn routing_page_refs_level_mask(page_refs: &[RoutingLayerPageRef]) -> u64 {
    let mut mask = 0_u64;
    for page_ref in page_refs {
        if page_ref.level_mask == u64::MAX {
            return u64::MAX;
        }
        mask |= page_ref.level_mask;
    }
    mask
}

fn routing_page_refs_record_count(page_refs: &[RoutingLayerPageRef]) -> usize {
    page_refs.iter().map(|page_ref| page_ref.page_records).sum()
}

fn routing_page_refs_leaf_segments(page_refs: &[RoutingLayerPageRef]) -> usize {
    page_refs
        .iter()
        .map(|page_ref| page_ref.leaf_segments)
        .sum()
}

fn routing_page_refs_leaf_pages(page_refs: &[RoutingLayerPageRef]) -> usize {
    if page_refs.iter().any(|page_ref| page_ref.leaf_pages == 0) {
        return 0;
    }

    page_refs.iter().map(|page_ref| page_ref.leaf_pages).sum()
}

fn routing_page_refs_routing_pages(page_refs: &[RoutingLayerPageRef]) -> usize {
    if page_refs.iter().any(|page_ref| page_ref.routing_pages == 0) {
        return 0;
    }

    1 + page_refs
        .iter()
        .map(|page_ref| page_ref.routing_pages)
        .sum::<usize>()
}

fn routing_page_refs_segment_bytes(page_refs: &[RoutingLayerPageRef]) -> u64 {
    page_refs
        .iter()
        .map(|page_ref| page_ref.page_segment_bytes)
        .sum()
}

fn routing_page_refs_vector_bytes(page_refs: &[RoutingLayerPageRef]) -> u64 {
    page_refs
        .iter()
        .map(|page_ref| page_ref.page_vector_bytes)
        .sum()
}

fn routing_page_refs_graph_bytes(page_refs: &[RoutingLayerPageRef]) -> u64 {
    page_refs
        .iter()
        .map(|page_ref| page_ref.page_graph_bytes)
        .sum()
}

fn routing_page_refs_sparse_encoded_vectors(page_refs: &[RoutingLayerPageRef]) -> usize {
    page_refs
        .iter()
        .map(|page_ref| page_ref.page_sparse_encoded_vectors)
        .sum()
}

fn routing_page_refs_dense_encoded_vectors(page_refs: &[RoutingLayerPageRef]) -> usize {
    page_refs
        .iter()
        .map(|page_ref| page_ref.page_dense_encoded_vectors)
        .sum()
}

fn looks_like_windows_drive_path(uri: &str) -> bool {
    let bytes = uri.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs::{self, OpenOptions},
        path::{Path, PathBuf},
        process::Command,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc,
        },
        time::{Duration, SystemTime},
    };

    use super::{
        CacheReadCounts, CreateOutcome, GLOBAL_RERANK_RANGE_MAX_PARALLEL, PrefetchedRead,
        RangedColumns, ReadBytes, SIDECAR_MAX_PHYSICAL_RANGE_BYTES, SIDECAR_RANGE_COALESCE_BYTES,
        Storage, coalesce_bounded_ranges, plan_bounded_ranges,
    };
    use crate::{
        collection_control::{
            COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD,
            COLLECTION_WAL_FRONTIER_SOFT_TRANSACTIONS_PER_SHARD, CollectionCommit,
            CollectionDescriptorRef, CollectionWalFrontierHead, CollectionWalReservation,
            PRIMARY_MODALITY, PendingCollectionCommit, collection_wal_frontier_head_bytes,
            collection_wal_frontier_head_path, collection_wal_frontier_shard,
        },
        error::Result,
        index::IndexConfig,
        manifest::{DEFAULT_GRAPH_NEIGHBORS, DEFAULT_ROUTING_PAGE_FANOUT, Manifest},
        metric::VectorMetric,
        record::{BuildConfig, LeafCapability},
    };
    use url::Url;

    struct DropFlag(Arc<AtomicBool>);

    #[test]
    fn global_rerank_range_wave_does_not_serialize_twenty_small_gets() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let ranges = (0_u64..20)
            .map(|index| {
                let start = index * 128 * 1024;
                start..start + 4
            })
            .collect::<Vec<_>>();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let fetched = runtime
            .block_on(coalesce_bounded_ranges(
                &ranges,
                |range: std::ops::Range<u64>| {
                    let active = Arc::clone(&active);
                    let peak = Arc::clone(&peak);
                    async move {
                        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok::<_, ()>(bytes::Bytes::from(vec![
                            7_u8;
                            (range.end - range.start) as usize
                        ]))
                    }
                },
                0,
                SIDECAR_MAX_PHYSICAL_RANGE_BYTES,
                GLOBAL_RERANK_RANGE_MAX_PARALLEL,
            ))
            .unwrap();

        assert_eq!(fetched, vec![bytes::Bytes::from_static(&[7_u8; 4]); 20]);
        assert_eq!(peak.load(Ordering::SeqCst), 20);
    }

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn exact_manifest(uri: &str) -> Manifest {
        Manifest::new_with_routing_page_fanout(
            IndexConfig {
                uri: uri.to_string(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 4,
                ram_budget_bytes: None,
                text: false,
                named_vectors: BTreeMap::new(),
            },
            DEFAULT_ROUTING_PAGE_FANOUT,
            DEFAULT_GRAPH_NEIGHBORS,
            LeafCapability::PqScanOnly,
            BuildConfig::default(),
        )
    }

    #[test]
    fn create_bytes_verified_is_idempotent_and_rejects_conflicting_content() {
        let storage = Storage::from_uri("memory:///verified-create").unwrap();
        let bytes = b"immutable extent";
        let checksum = blake3::hash(bytes).to_hex().to_string();

        assert_eq!(
            storage
                .create_bytes_verified("extent.wal", bytes, &checksum)
                .unwrap(),
            CreateOutcome::Created
        );
        assert_eq!(
            storage
                .create_bytes_verified("extent.wal", bytes, &checksum)
                .unwrap(),
            CreateOutcome::Existing
        );
        assert!(
            storage
                .create_bytes_verified(
                    "extent.wal",
                    b"conflicting extent",
                    blake3::hash(b"conflicting extent").to_hex().as_ref(),
                )
                .is_err()
        );
    }

    fn root_commit(transaction_id: &str) -> CollectionCommit {
        CollectionCommit {
            transaction_id: transaction_id.to_string(),
            snapshot_generation: 1,
            schema_fingerprint: "a".repeat(64),
            descriptors: vec![CollectionDescriptorRef {
                modality: PRIMARY_MODALITY.to_string(),
                prefix: String::new(),
                descriptor_path: format!(
                    "transactions/{transaction_id}/descriptors/descriptor.bin"
                ),
                descriptor_checksum: "b".repeat(64),
            }],
        }
    }

    #[test]
    fn exact_manifest_ref_survives_newer_manifest_staging() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();
        let first = exact_manifest(&uri);
        let staged = storage
            .stage_manifest(PRIMARY_MODALITY, &first, None)
            .unwrap();
        let second = first.next_version();
        storage
            .stage_manifest(PRIMARY_MODALITY, &second, Some(&first))
            .unwrap();

        let loaded = storage.load_manifest_ref(&staged.reference, true).unwrap();
        let paged = storage.load_manifest_ref(&staged.reference, false).unwrap();

        assert_eq!(loaded.version, first.version);
        assert_eq!(loaded.config.uri, first.config.uri);
        assert_eq!(loaded.config.metric, first.config.metric);
        assert_eq!(loaded.config.dimensions, first.config.dimensions);
        assert!(
            staged.reference.resident_routing_bytes_estimate >= loaded.resident_bytes_estimate()
        );
        assert!(staged.reference.resident_bytes_estimate >= paged.resident_bytes_estimate());
    }

    #[test]
    fn pending_collection_commit_create_is_one_immutable_put() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();
        let pending = PendingCollectionCommit {
            epoch: "epoch-1".to_string(),
            created_at_ms: 123_456,
            commit: root_commit("pending-1"),
        };
        let before = storage.request_counts();

        storage.create_pending_collection_commit(&pending).unwrap();

        let first = storage.request_counts().delta(&before);
        assert_eq!(first.puts, 1);
        assert_eq!(first.gets, 0);
        assert_eq!(first.heads, 0);
        storage.create_pending_collection_commit(&pending).unwrap();

        let mut retry = pending.clone();
        retry.created_at_ms += 1;
        storage.create_pending_collection_commit(&retry).unwrap();

        let mut conflict = pending.clone();
        conflict.commit.descriptors[0].descriptor_checksum = "c".repeat(64);
        let error = storage
            .create_pending_collection_commit(&conflict)
            .unwrap_err();
        assert!(error.to_string().contains("conflicts"), "{error}");

        let path = crate::collection_control::pending_collection_commit_path(
            &pending.epoch,
            &pending.commit.transaction_id,
        )
        .unwrap();
        let stored = storage.read_coordination_object(&path).unwrap().unwrap();
        assert_eq!(
            crate::collection_control::pending_collection_commit_from_slice(&stored.bytes, &path,)
                .unwrap(),
            pending,
            "a retry or conflicting create must not replace the first durable object"
        );
    }

    #[test]
    fn mutable_lane_head_is_never_admitted_to_the_read_through_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let storage =
            Storage::from_uri_with_cache(&file_uri(dir.path()), Some(cache.path().to_path_buf()))
                .unwrap();

        assert!(storage.cache_path("lane-log/lanes/0003/HEAD").is_none());
        assert!(
            storage
                .cache_path("lane-log/lanes/0003/blocks/one.blk")
                .is_some()
        );
        let head = "lane-log/lanes/0003/HEAD";
        storage
            .write_coordination_object(head, b"abcdefgh", None)
            .unwrap();
        let before = storage.request_counts();
        assert_eq!(storage.read_range(head, 0..4).unwrap(), b"abcd");
        assert_eq!(storage.read_range(head, 0..4).unwrap(), b"abcd");
        assert_eq!(storage.read_suffix(head, 4).unwrap().bytes, b"efgh");
        assert_eq!(storage.read_suffix(head, 4).unwrap().bytes, b"efgh");
        assert_eq!(
            storage.request_counts().delta(&before).gets,
            4,
            "mutable HEAD ranges and suffixes must always be fetched fresh"
        );

        let before_whole = storage.request_counts();
        assert_eq!(
            storage.read_bytes_with_cache_status(head).unwrap().bytes,
            b"abcdefgh"
        );
        assert_eq!(
            storage.read_bytes_with_cache_status(head).unwrap().bytes,
            b"abcdefgh"
        );
        let whole_requests = storage.request_counts().delta(&before_whole);
        assert_eq!(whole_requests.heads, 0);
        assert_eq!(whole_requests.gets, 2);
    }

    #[test]
    fn local_coordination_lock_serializes_processes() {
        const ROLE: &str = "BORSUK_COORDINATION_LOCK_TEST_ROLE";
        const ROOT: &str = "BORSUK_COORDINATION_LOCK_TEST_ROOT";
        if let Ok(role) = std::env::var(ROLE) {
            let root = PathBuf::from(std::env::var_os(ROOT).unwrap());
            let storage = Storage::from_uri(&file_uri(&root)).unwrap();
            if role == "waiter" {
                fs::write(root.join("WAITING"), b"waiting\n").unwrap();
            }
            let _guard = storage
                .lock_local_coordination_path("lane-log/ACTIVE")
                .unwrap()
                .expect("local storage must use a process-shared lock");
            if role == "holder" {
                fs::write(root.join("HELD"), b"held\n").unwrap();
                while !root.join("RELEASE").is_file() {
                    std::thread::sleep(Duration::from_millis(5));
                }
            } else {
                fs::write(root.join("ACQUIRED"), b"acquired\n").unwrap();
            }
            return;
        }

        let root = tempfile::tempdir().unwrap();
        let executable = std::env::current_exe().unwrap();
        let test_name = "storage::tests::local_coordination_lock_serializes_processes";
        let mut holder = Command::new(&executable)
            .arg("--exact")
            .arg(test_name)
            .env(ROLE, "holder")
            .env(ROOT, root.path())
            .spawn()
            .unwrap();
        wait_for_test_marker(root.path(), "HELD");
        let mut waiter = Command::new(executable)
            .arg("--exact")
            .arg(test_name)
            .env(ROLE, "waiter")
            .env(ROOT, root.path())
            .spawn()
            .unwrap();
        wait_for_test_marker(root.path(), "WAITING");
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !root.path().join("ACQUIRED").is_file(),
            "a second process acquired the same local coordination lock"
        );
        fs::write(root.path().join("RELEASE"), b"release\n").unwrap();
        assert!(holder.wait().unwrap().success());
        assert!(waiter.wait().unwrap().success());
        assert!(root.path().join("ACQUIRED").is_file());
    }

    fn wait_for_test_marker(root: &Path, marker: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !root.join(marker).is_file() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for child marker {marker}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn immutable_whole_object_reads_reuse_size_without_repeating_head() {
        let storage = Storage::from_uri("memory:///known-object-size").unwrap();
        storage
            .write_bytes("segments/object.bin", b"immutable bytes")
            .unwrap();

        let before = storage.request_counts();
        let first = storage
            .read_bytes_with_cache_status("segments/object.bin")
            .unwrap();
        let first_requests = storage.request_counts().delta(&before);
        assert_eq!(first.bytes, b"immutable bytes");
        assert_eq!(first_requests.heads, 0);
        assert_eq!(first_requests.gets, 1);

        let before_second = storage.request_counts();
        let second = storage
            .read_bytes_with_cache_status("segments/object.bin")
            .unwrap();
        let second_requests = storage.request_counts().delta(&before_second);
        assert_eq!(second.bytes, b"immutable bytes");
        assert_eq!(second_requests.heads, 0);
        assert_eq!(second_requests.gets, 1);
    }

    #[test]
    fn pending_collection_commit_discovery_fails_closed_above_hard_bound() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();
        let schema_fingerprint = "a".repeat(64);
        let epoch = format!("schema-{schema_fingerprint}");
        for ordinal in 0..=2_000 {
            storage
                .create_pending_collection_commit(&PendingCollectionCommit {
                    epoch: epoch.clone(),
                    created_at_ms: 1,
                    commit: root_commit(&format!("bounded-pending-{ordinal}")),
                })
                .unwrap();
        }

        let error = storage
            .pending_collection_commits_for_schema(&schema_fingerprint)
            .unwrap_err();
        assert!(
            error.to_string().contains("backlog exceeds 2000"),
            "{error}"
        );
    }

    #[test]
    fn collection_wal_frontier_soft_pressure_and_hard_admission_are_exact() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();
        let target_shard = 0;
        let transaction_ids = (0_u64..)
            .map(|value| format!("bounded-root-{value}"))
            .filter(|transaction_id| {
                collection_wal_frontier_shard(transaction_id).unwrap() == target_shard
            })
            .take(COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize + 1)
            .collect::<Vec<_>>();

        for (ordinal, transaction_id) in transaction_ids
            .iter()
            .take(COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize)
            .enumerate()
        {
            storage
                .reserve_collection_wal_transaction(transaction_id, &"a".repeat(64))
                .unwrap();
            let pressure = storage
                .append_collection_wal_transaction(&root_commit(transaction_id))
                .unwrap();
            assert_eq!(
                pressure,
                ordinal + 1 >= COLLECTION_WAL_FRONTIER_SOFT_TRANSACTIONS_PER_SHARD as usize
            );
        }

        let error = storage
            .reserve_collection_wal_transaction(
                &transaction_ids[COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize],
                &"a".repeat(64),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            crate::BorsukError::ConcurrentModification { path }
                if path.ends_with("/CAPACITY")
        ));

        let consumed = transaction_ids
            .iter()
            .take(COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize)
            .cloned()
            .collect();
        storage
            .prune_collection_wal_transactions(&consumed)
            .unwrap();
        storage
            .reserve_collection_wal_transaction(
                &transaction_ids[COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize],
                &"a".repeat(64),
            )
            .unwrap();
        assert!(
            !storage
                .append_collection_wal_transaction(&root_commit(
                    &transaction_ids[COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize],
                ),)
                .unwrap(),
            "pruning must reset the exact shard count below soft pressure"
        );
    }

    #[test]
    fn collection_commit_requires_a_live_root_reservation() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();
        let commit = root_commit("reservation-fence");

        let error = storage
            .append_collection_wal_transaction(&commit)
            .unwrap_err();
        assert!(matches!(
            error,
            crate::BorsukError::ConcurrentModification { path }
                if path.ends_with("/RESERVATION")
        ));

        storage
            .reserve_collection_wal_transaction(&commit.transaction_id, &commit.schema_fingerprint)
            .unwrap();
        storage.append_collection_wal_transaction(&commit).unwrap();
    }

    #[test]
    fn reservation_receipt_commits_without_rereading_the_root_head() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();
        let commit = root_commit("reservation-receipt");
        let receipt = storage
            .reserve_collection_wal_transaction(&commit.transaction_id, &commit.schema_fingerprint)
            .unwrap();

        let before = storage.request_counts();
        storage
            .create_collection_commit_from_reservation(&commit, &receipt, None)
            .unwrap();
        let requests = storage.request_counts().delta(&before);

        assert_eq!(requests.gets, 0, "happy-path commit must reuse its receipt");
    }

    #[test]
    fn commit_carries_one_successor_reservation_without_a_root_reread() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();
        let first = root_commit("carried-reservation-first");
        let shard = collection_wal_frontier_shard(&first.transaction_id).unwrap();
        let successor_id = (0_u64..)
            .map(|value| format!("carried-reservation-next-{value}"))
            .find(|candidate| collection_wal_frontier_shard(candidate).unwrap() == shard)
            .unwrap();
        let successor = root_commit(&successor_id);
        let first_receipt = storage
            .reserve_collection_wal_transaction(&first.transaction_id, &first.schema_fingerprint)
            .unwrap();

        let before = storage.request_counts();
        let outcome = storage
            .create_collection_commit_from_reservation(
                &first,
                &first_receipt,
                Some(&successor.transaction_id),
            )
            .unwrap();
        let requests = storage.request_counts().delta(&before);
        assert_eq!(requests.gets, 0);
        let successor_receipt = outcome.successor.unwrap();

        let before_successor = storage.request_counts();
        storage
            .create_collection_commit_from_reservation(&successor, &successor_receipt, None)
            .unwrap();
        let successor_requests = storage.request_counts().delta(&before_successor);
        assert_eq!(successor_requests.gets, 0);
    }

    #[test]
    fn commit_at_hard_capacity_drops_successor_without_failing() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();
        let shard = 0;
        let transaction_ids = (0_u64..)
            .map(|value| format!("carried-capacity-{value}"))
            .filter(|transaction_id| {
                collection_wal_frontier_shard(transaction_id).unwrap() == shard
            })
            .take(COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize + 1)
            .collect::<Vec<_>>();
        for transaction_id in transaction_ids
            .iter()
            .take(COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize - 1)
        {
            let commit = root_commit(transaction_id);
            let receipt = storage
                .reserve_collection_wal_transaction(transaction_id, &commit.schema_fingerprint)
                .unwrap();
            storage
                .create_collection_commit_from_reservation(&commit, &receipt, None)
                .unwrap();
        }
        let current = root_commit(
            &transaction_ids[COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize - 1],
        );
        let receipt = storage
            .reserve_collection_wal_transaction(
                &current.transaction_id,
                &current.schema_fingerprint,
            )
            .unwrap();
        let outcome = storage
            .create_collection_commit_from_reservation(
                &current,
                &receipt,
                Some(
                    &transaction_ids[COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize],
                ),
            )
            .unwrap();

        assert!(outcome.successor.is_none());
    }

    #[test]
    fn stale_reservation_receipt_rebases_after_same_shard_contention() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();
        let first = root_commit("reservation-rebase-first");
        let shard = collection_wal_frontier_shard(&first.transaction_id).unwrap();
        let second_id = (0_u64..)
            .map(|value| format!("reservation-rebase-second-{value}"))
            .find(|candidate| collection_wal_frontier_shard(candidate).unwrap() == shard)
            .unwrap();
        let second = root_commit(&second_id);
        let first_receipt = storage
            .reserve_collection_wal_transaction(&first.transaction_id, &first.schema_fingerprint)
            .unwrap();
        let second_receipt = storage
            .reserve_collection_wal_transaction(&second.transaction_id, &second.schema_fingerprint)
            .unwrap();

        storage
            .create_collection_commit_from_reservation(&first, &first_receipt, None)
            .unwrap();
        storage
            .create_collection_commit_from_reservation(&second, &second_receipt, None)
            .unwrap();
    }

    #[test]
    fn expired_root_reservations_are_removed_from_authorization_truth() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();
        let transaction_id = "expired-reservation";
        let shard = collection_wal_frontier_shard(transaction_id).unwrap();
        let head_path = collection_wal_frontier_head_path(shard).unwrap();
        let head = CollectionWalFrontierHead {
            generation: 1,
            reservations: vec![CollectionWalReservation {
                transaction_id: transaction_id.to_string(),
                schema_fingerprint: "a".repeat(64),
                expires_at_ms: 1,
            }],
            transactions: Vec::new(),
        };
        storage
            .write_coordination_object(
                &head_path,
                &collection_wal_frontier_head_bytes(&head, shard).unwrap(),
                None,
            )
            .unwrap();

        storage.prune_expired_collection_wal_reservations().unwrap();

        assert!(
            !storage
                .collection_wal_authorized_transaction_ids_snapshot()
                .unwrap()
                .contains(transaction_id)
        );
    }

    #[test]
    fn exact_manifest_ref_rejects_corrupt_routing() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();
        let staged = storage
            .stage_manifest(PRIMARY_MODALITY, &exact_manifest(&uri), None)
            .unwrap();
        storage
            .write_bytes(&staged.reference.routing_path, b"corrupt routing")
            .unwrap();

        let error = storage
            .load_manifest_ref(&staged.reference, true)
            .unwrap_err();

        assert!(matches!(error, crate::BorsukError::ChecksumMismatch { .. }));
    }

    #[test]
    fn accepts_s3_compatible_uri() {
        let storage = Storage::from_uri("s3://vectors/indexes/docs-index");

        assert!(
            storage.is_ok(),
            "S3-compatible URIs must be supported by the storage layer: {storage:?}"
        );
    }

    #[test]
    fn synchronous_storage_is_safe_inside_a_multithreaded_tokio_host() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let host = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        host.block_on(async {
            let storage = Storage::from_uri(&uri).unwrap();
            storage
                .write_bytes("runtime/nested.bin", b"nested-runtime-safe")
                .unwrap();
            assert_eq!(
                storage.read_bytes_uncached("runtime/nested.bin").unwrap(),
                b"nested-runtime-safe"
            );
            drop(storage);
        });
    }

    #[test]
    fn storage_runtime_uses_the_process_cpu_budget() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri(&file_uri(dir.path())).unwrap();

        assert_eq!(
            storage.runtime.runtime().metrics().num_workers(),
            crate::configured_cpu_threads(),
            "object-store I/O workers must not silently scale to every host CPU"
        );
    }

    #[test]
    fn windows_drive_paths_are_local_paths_not_uri_schemes() {
        assert!(!super::has_uri_scheme("C:\\Users\\borsuk\\index"));
        assert!(!super::has_uri_scheme("D:/data/borsuk-index"));
    }

    #[test]
    fn reads_byte_ranges_without_fetching_whole_object() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();

        storage
            .write_bytes("segments/L0/aa/test.bin", b"0123456789")
            .unwrap();

        let range = storage.read_range("segments/L0/aa/test.bin", 2..6).unwrap();

        assert_eq!(range, b"2345");
    }

    #[test]
    fn disk_cache_reuses_range_and_suffix_reads_without_store_requests() {
        let dir = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let writer = Storage::from_uri(&file_uri(dir.path())).unwrap();
        writer
            .write_bytes("vectors/sidecar.bin", b"0123456789abcdef")
            .unwrap();
        let storage =
            Storage::from_uri_with_cache(&file_uri(dir.path()), Some(cache.path().to_path_buf()))
                .unwrap();

        let ranges = [2..6, 10..14];
        let first = storage.read_ranges("vectors/sidecar.bin", &ranges).unwrap();
        assert!(!first.cache_hit);
        assert_eq!(first.bytes_fetched, 12);
        let requests_after_first = storage.request_counts();

        let second = storage.read_ranges("vectors/sidecar.bin", &ranges).unwrap();
        assert!(second.cache_hit);
        assert_eq!(second.bytes_fetched, 0);
        assert_eq!(second.chunks, first.chunks);
        assert_eq!(
            storage.request_counts().delta(&requests_after_first).gets,
            0
        );

        let suffix = storage.read_suffix("vectors/sidecar.bin", 4).unwrap();
        assert!(!suffix.cache_hit);
        assert_eq!(suffix.bytes, b"cdef");
        let requests_after_suffix = storage.request_counts();
        let cached_suffix = storage.read_suffix("vectors/sidecar.bin", 4).unwrap();
        assert!(cached_suffix.cache_hit);
        assert_eq!(cached_suffix.bytes, b"cdef");
        assert_eq!(
            storage.request_counts().delta(&requests_after_suffix).gets,
            0
        );
    }

    #[test]
    fn concurrent_cache_publication_never_exposes_partial_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            Storage::from_uri_with_cache(&file_uri(dir.path()), Some(cache.path().to_path_buf()))
                .unwrap(),
        );
        let bytes = 8 * 1024 * 1024;
        let first = vec![0x35_u8; bytes];
        let second = vec![0xca_u8; bytes];
        storage
            .write_cache_file("shared/chunk.bin", &first)
            .unwrap();

        let reading = Arc::new(AtomicBool::new(true));
        let reader_path = cache.path().join("shared/chunk.bin");
        let reader_running = Arc::clone(&reading);
        let reader = std::thread::spawn(move || {
            while reader_running.load(Ordering::Acquire) {
                let observed = fs::read(&reader_path).unwrap();
                assert_eq!(
                    observed.len(),
                    bytes,
                    "cache reader observed a partial file"
                );
                assert!(
                    observed.iter().all(|byte| *byte == 0x35)
                        || observed.iter().all(|byte| *byte == 0xca),
                    "cache reader observed interleaved bytes"
                );
            }
        });

        let writers = (0..4)
            .map(|writer| {
                let storage = Arc::clone(&storage);
                let payload = if writer % 2 == 0 {
                    first.clone()
                } else {
                    second.clone()
                };
                std::thread::spawn(move || {
                    for _ in 0..12 {
                        storage
                            .write_cache_file("shared/chunk.bin", &payload)
                            .unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }
        reading.store(false, Ordering::Release);
        reader.join().unwrap();
    }

    #[test]
    fn atomic_cache_publication_replaces_only_with_complete_bytes() {
        let cache = tempfile::tempdir().unwrap();
        let path = cache.path().join("nested/chunk.bin");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"old complete bytes").unwrap();

        super::atomic_write_cache_file(&path, b"new complete bytes").unwrap();

        assert_eq!(fs::read(path).unwrap(), b"new complete bytes");
    }

    #[test]
    fn cache_read_counters_separate_backing_and_disk_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let writer = Storage::from_uri(&file_uri(dir.path())).unwrap();
        writer
            .write_bytes("vectors/tiered.bin", b"0123456789abcdef")
            .unwrap();
        let storage =
            Storage::from_uri_with_cache(&file_uri(dir.path()), Some(cache.path().to_path_buf()))
                .unwrap();

        let before_backing = storage.cache_read_counts();
        assert_eq!(
            storage.read_range("vectors/tiered.bin", 2..6).unwrap(),
            b"2345"
        );
        let backing = storage.cache_read_counts().delta(&before_backing);
        assert_eq!(backing.disk_reads, 0);
        assert_eq!(backing.disk_bytes, 0);
        assert_eq!(backing.backing_reads, 1);
        assert_eq!(backing.backing_bytes, 4);

        let before_disk = storage.cache_read_counts();
        assert_eq!(
            storage.read_range("vectors/tiered.bin", 2..6).unwrap(),
            b"2345"
        );
        let disk = storage.cache_read_counts().delta(&before_disk);
        assert_eq!(disk.disk_reads, 1);
        assert_eq!(disk.disk_bytes, 4);
        assert_eq!(disk.backing_reads, 0);
        assert_eq!(disk.backing_bytes, 0);
    }

    #[test]
    fn isolated_read_scopes_do_not_attribute_other_queries_io() {
        let dir = tempfile::tempdir().unwrap();
        let writer = Storage::from_uri(&file_uri(dir.path())).unwrap();
        writer
            .write_bytes("vectors/scoped.bin", b"0123456789abcdef")
            .unwrap();
        let storage = Storage::from_uri(&file_uri(dir.path())).unwrap();
        let first = storage.isolated_read_scope();
        let second = storage.isolated_read_scope();

        assert_eq!(
            first.read_range("vectors/scoped.bin", 0..4).unwrap(),
            b"0123"
        );
        assert_eq!(
            second.read_range("vectors/scoped.bin", 8..12).unwrap(),
            b"89ab"
        );

        assert_eq!(
            first.cache_read_counts(),
            CacheReadCounts {
                backing_reads: 1,
                backing_bytes: 4,
                ..CacheReadCounts::default()
            }
        );
        assert_eq!(
            second.cache_read_counts(),
            CacheReadCounts {
                backing_reads: 1,
                backing_bytes: 4,
                ..CacheReadCounts::default()
            }
        );
        assert_eq!(first.request_counts().gets, 1);
        assert_eq!(second.request_counts().gets, 1);
        assert_eq!(storage.request_counts().gets, 2);
    }

    #[test]
    fn range_reads_report_physical_coalesced_bytes_and_gets() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri(&file_uri(dir.path())).unwrap();
        let object = vec![7_u8; 2 * 1024 * 1024];
        storage
            .write_bytes("vectors/coalesced.bin", &object)
            .unwrap();

        // Sidecar reads merge ranges separated by at most 1 MiB. These two
        // requested four-byte rows therefore become one physical range GET
        // spanning the bytes between them. Publication accounting must report
        // that transferred span, not merely the eight requested payload bytes.
        let ranges = [0..4, 32 * 1024..32 * 1024 + 4];
        let before = storage.request_counts();
        let read = storage
            .read_ranges("vectors/coalesced.bin", &ranges)
            .unwrap();
        let requests = storage.request_counts().delta(&before);

        assert_eq!(read.chunks, vec![vec![7_u8; 4], vec![7_u8; 4]]);
        assert_eq!(requests.gets, 1);
        assert_eq!(read.bytes_fetched, 32 * 1024 + 4);
    }

    #[test]
    fn global_rerank_ranges_do_not_fetch_a_half_mib_unselected_gap() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri(&file_uri(dir.path())).unwrap();
        let object = vec![7_u8; 512 * 1024 + 4];
        storage
            .write_bytes("global-pq/bundles/rerank.arrow", &object)
            .unwrap();
        let ranges = [0..4, 512 * 1024..512 * 1024 + 4];
        let before = storage.request_counts();

        let read = storage
            .read_global_rerank_ranges("global-pq/bundles/rerank.arrow", &ranges)
            .unwrap();
        let requests = storage.request_counts().delta(&before);

        assert_eq!(read.chunks, vec![vec![7_u8; 4]; 2]);
        assert_eq!(requests.gets, 2);
        assert_eq!(read.bytes_fetched, 8);
    }

    #[test]
    fn range_reads_enforce_the_four_mib_physical_span_cap() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri(&file_uri(dir.path())).unwrap();
        let object = vec![7_u8; 4 * 1024 * 1024 + 4];
        storage.write_bytes("vectors/capped.bin", &object).unwrap();

        // Every adjacent requested row is exactly within the 64 KiB merge
        // gap. Without the independent physical-span cap all 65 rows would
        // become one 4 MiB+4-byte GET.
        let ranges = (0_u64..=64)
            .map(|index| {
                let start = index * 64 * 1024;
                start..start + 4
            })
            .collect::<Vec<_>>();
        let before = storage.request_counts();
        let read = storage.read_ranges("vectors/capped.bin", &ranges).unwrap();
        let requests = storage.request_counts().delta(&before);

        assert_eq!(read.chunks, vec![vec![7_u8; 4]; 65]);
        assert_eq!(requests.gets, 2);
        assert_eq!(read.bytes_fetched, 63 * 64 * 1024 + 8);
    }

    #[test]
    fn sidecar_range_plan_does_not_merge_scattered_rows_into_large_gets() {
        let ranges = [
            0..4,
            512 * 1024..512 * 1024 + 4,
            1536 * 1024..1536 * 1024 + 4,
            5 * 1024 * 1024..5 * 1024 * 1024 + 4,
        ];

        let plan = plan_bounded_ranges(
            &ranges,
            SIDECAR_RANGE_COALESCE_BYTES,
            SIDECAR_MAX_PHYSICAL_RANGE_BYTES,
        );

        assert_eq!(
            plan.physical,
            vec![
                0..4,
                512 * 1024..512 * 1024 + 4,
                1536 * 1024..1536 * 1024 + 4,
                5 * 1024 * 1024..5 * 1024 * 1024 + 4,
            ]
        );
        assert!(
            plan.physical
                .iter()
                .all(|range| range.end - range.start <= SIDECAR_MAX_PHYSICAL_RANGE_BYTES)
        );
        assert_eq!(plan.slices.len(), ranges.len());
    }

    #[test]
    fn sidecar_range_plan_preserves_input_order_under_the_cap_span() {
        let ranges = [
            4 * 1024 * 1024..4 * 1024 * 1024 + 4,
            0..4,
            3 * 1024 * 1024..3 * 1024 * 1024 + 4,
        ];

        let plan = plan_bounded_ranges(
            &ranges,
            SIDECAR_RANGE_COALESCE_BYTES,
            SIDECAR_MAX_PHYSICAL_RANGE_BYTES,
        );

        assert_eq!(
            plan.physical,
            vec![
                0..4,
                3 * 1024 * 1024..3 * 1024 * 1024 + 4,
                4 * 1024 * 1024..4 * 1024 * 1024 + 4,
            ]
        );
        assert_eq!(plan.slices[0].physical_index, 2);
        assert_eq!(plan.slices[1].physical_index, 0);
        assert_eq!(plan.slices[2].physical_index, 1);
    }

    #[test]
    fn dropping_prefetched_read_aborts_in_flight_task() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = mpsc::channel();
        let dropped_in_task = Arc::clone(&dropped);
        let handle = runtime.spawn(async move {
            let _drop_flag = DropFlag(dropped_in_task);
            started_tx.send(()).unwrap();
            futures_util::future::pending::<Result<ReadBytes>>().await
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(PrefetchedRead {
            relative: "segments/L0/test.parquet".to_string(),
            handle: Some(handle),
        });

        runtime
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while !dropped.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                })
                .await
            })
            .expect("dropping PrefetchedRead must abort and drop its task");
    }

    #[test]
    fn lists_and_deletes_objects_relative_to_index_root() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();

        storage.write_bytes("segments/L0/aa/a.bin", b"aaa").unwrap();
        storage
            .write_bytes("segments/L1/bb/b.bin", b"bbbb")
            .unwrap();

        let listed = storage.list_objects("segments").unwrap();

        assert_eq!(
            listed
                .iter()
                .map(|object| (object.path.as_str(), object.size))
                .collect::<Vec<_>>(),
            vec![("segments/L0/aa/a.bin", 3), ("segments/L1/bb/b.bin", 4)]
        );
        assert!(storage.delete_object("segments/L0/aa/a.bin").unwrap());
        assert!(!storage.delete_object("segments/L0/aa/a.bin").unwrap());
        assert_eq!(
            storage
                .list_objects("segments")
                .unwrap()
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["segments/L1/bb/b.bin"]
        );
    }

    #[cfg(unix)]
    #[test]
    fn collect_cache_files_skips_entries_removed_before_metadata() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let live_path = dir.path().join("live.bin");
        let vanished_path = dir.path().join("vanished.bin");
        let dangling_entry = dir.path().join("dangling.bin");
        fs::write(&live_path, b"live").unwrap();
        symlink(&vanished_path, &dangling_entry).unwrap();

        let mut files = Vec::new();
        super::collect_cache_files(dir.path(), &mut files).unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, live_path);
        assert_eq!(files[0].bytes, 4);
    }

    #[test]
    fn collect_cache_files_skips_directories_removed_before_read_dir() {
        let dir = tempfile::tempdir().unwrap();
        let removed_dir = dir.path().join("removed");
        fs::create_dir(&removed_dir).unwrap();
        fs::remove_dir(&removed_dir).unwrap();

        let mut files = Vec::new();
        super::collect_cache_files(&removed_dir, &mut files).unwrap();

        assert!(files.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn read_cache_file_keeps_valid_bytes_when_touch_refresh_fails() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri_with_cache_and_max(
            &file_uri(dir.path()),
            Some(cache.path().to_path_buf()),
            Some(1024),
        )
        .unwrap();
        let path = cache.path().join("segments/L0/file.bin");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"valid cache contents").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).unwrap();

        let read = storage.read_cache_file("segments/L0/file.bin");

        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(read.unwrap(), Some(b"valid cache contents".to_vec()));
    }

    #[test]
    fn touch_cache_file_refreshes_mtime_without_rewriting_contents() {
        let dir = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri_with_cache_and_max(
            &file_uri(dir.path()),
            Some(cache.path().to_path_buf()),
            Some(1024),
        )
        .unwrap();
        let path = cache.path().join("segments/L0/file.bin");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"newer cache contents").unwrap();
        let old_modified = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .set_modified(old_modified)
            .unwrap();

        storage.touch_cache_file(&path).unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"newer cache contents");
        assert!(
            fs::metadata(&path).unwrap().modified().unwrap() > old_modified,
            "touching the cache file should refresh mtime"
        );
    }

    #[test]
    fn touch_cache_file_ignores_file_evicted_before_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri_with_cache_and_max(
            &file_uri(dir.path()),
            Some(cache.path().to_path_buf()),
            Some(1024),
        )
        .unwrap();
        let path = cache.path().join("segments/L0/file.bin");
        fs::create_dir_all(path.parent().unwrap()).unwrap();

        storage.touch_cache_file(&path).unwrap();

        assert!(!path.exists());
    }

    fn file_uri(path: &Path) -> String {
        Url::from_directory_path(path).unwrap().to_string()
    }

    /// A projected, range-based Parquet read must fetch only the requested
    /// columns' bytes (score from the small column without paying for the big
    /// one), and a row-selective read must fetch far fewer bytes than a full
    /// scan of the same column — the object-store-native byte savings.
    #[test]
    fn ranged_parquet_read_fetches_only_projected_columns_and_rows() {
        use std::sync::Arc;

        use arrow_array::{BinaryArray, Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

        let rows = 1_024_usize;
        let ids: Vec<i64> = (0..rows as i64).collect();
        // A large, distinct per-row payload so the "vector" column dominates the
        // file and does not simply compress away.
        let blobs: Vec<Vec<u8>> = (0..rows)
            .map(|i| {
                (0..2_048)
                    .map(|j| ((i * 31 + j * 17) % 251) as u8)
                    .collect()
            })
            .collect();
        let payloads: Vec<&[u8]> = blobs.iter().map(Vec::as_slice).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Binary, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(BinaryArray::from(payloads)),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        {
            // Small row groups so a row-selective read touches only a couple of
            // them; uncompressed so column-chunk sizes reflect the real payload.
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(128))
                .set_compression(Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .build();
            let mut writer =
                ArrowWriter::try_new(&mut buffer, Arc::clone(&schema), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let size = buffer.len() as u64;

        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri(&file_uri(dir.path())).unwrap();
        storage
            .write_bytes("segments/test.parquet", &buffer)
            .unwrap();

        let requests_before = storage.request_counts();
        let checksum = blake3::hash(&buffer).to_hex().to_string();
        let known = storage
            .read_known_size_with_cache_status_and_checksum(
                "segments/test.parquet",
                size,
                &checksum,
            )
            .unwrap();
        let known_requests = storage.request_counts().delta(&requests_before);
        assert_eq!(known.bytes, buffer);
        assert_eq!(known_requests.gets, 1);
        assert_eq!(known_requests.heads, 0);

        // Scoring: read only the small `id` column — a fraction of the object.
        let id_read = storage
            .read_parquet_columns_ranged(
                "segments/test.parquet",
                size,
                RangedColumns::Keep(&["id"]),
                None,
            )
            .unwrap();
        assert_eq!(id_read.total_rows, rows);
        assert!(
            id_read.bytes_fetched * 5 < size,
            "id-only read fetched {} bytes, expected far below whole object {size}",
            id_read.bytes_fetched
        );

        // Rerank: a row-selective read of the big column fetches far fewer bytes
        // than a full scan of that column.
        let full_payload = storage
            .read_parquet_columns_ranged(
                "segments/test.parquet",
                size,
                RangedColumns::Keep(&["payload"]),
                None,
            )
            .unwrap();
        let selected_payload = storage
            .read_parquet_columns_ranged(
                "segments/test.parquet",
                size,
                RangedColumns::Keep(&["payload"]),
                Some(&[0, rows - 1]),
            )
            .unwrap();
        assert!(
            selected_payload.bytes_fetched * 2 < full_payload.bytes_fetched,
            "row-selective payload read fetched {} bytes, expected far below full scan {}",
            selected_payload.bytes_fetched,
            full_payload.bytes_fetched
        );
        let one_group = storage
            .read_parquet_row_groups_ranged(
                "segments/test.parquet",
                size,
                RangedColumns::Keep(&["payload"]),
                &[3],
            )
            .unwrap();
        assert_eq!(
            one_group
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            128
        );
        assert!(
            one_group.bytes_fetched * 2 < full_payload.bytes_fetched,
            "one row-group range read fetched {} bytes, expected far below full file column {}",
            one_group.bytes_fetched,
            full_payload.bytes_fetched
        );

        // The projected data must still decode correctly.
        let id_column = id_read.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 0);
    }
}
