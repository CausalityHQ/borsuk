use std::{
    env, fmt,
    fs::{self, OpenOptions},
    future::Future,
    io,
    ops::Range,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};

use arrow_array::RecordBatch;
use bytes::Bytes;
use futures_util::{FutureExt, StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    UpdateVersion, parse_url_opts, path::Path as ObjectPath,
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
use tokio::{
    runtime::{Builder, Runtime},
    sync::Semaphore,
    task::JoinHandle,
};
use url::Url;

use crate::{
    error::{BorsukError, Result},
    format::{
        CurrentPointer, current_metadata_checksum, current_table_checksum, decode_current,
        encode_current, manifest_from_parquet, manifest_has_next_generated_id,
        manifest_metadata_from_parquet, manifest_to_parquet, pivots_from_parquet,
        pivots_to_parquet, routing_layer_page_index_from_parquet,
        routing_layer_page_index_to_parquet, routing_layer_page_to_parquet, routing_to_parquet,
    },
    manifest::{Manifest, RoutingLayerPageRef, SegmentSummary},
    observability,
    record::RequestCounts,
};

const CURRENT: &str = "CURRENT";
const MULTIPART_WRITE_THRESHOLD_BYTES: usize = 64 * 1024 * 1024;
const MULTIPART_PART_BYTES: usize = 8 * 1024 * 1024;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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
            self.inner.get_opts(location, options).await
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
            self.counters.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.get_ranges(location, ranges).await
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

#[derive(Clone)]
pub(crate) struct Storage {
    uri: String,
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    cache_dir: Option<PathBuf>,
    cache_max_bytes: Option<u64>,
    runtime: Arc<Runtime>,
    request_counters: Arc<RequestCounters>,
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

/// Result of a projected, range-based Parquet read: the decoded batches for the
/// requested columns plus the object-store bytes those column chunks cost.
#[derive(Debug)]
pub(crate) struct RangedParquetRead {
    pub batches: Vec<RecordBatch>,
    pub bytes_fetched: u64,
    pub total_rows: usize,
}

/// Which columns a ranged Parquet read should fetch. `Keep` fetches exactly the
/// named columns (rerank: the `vector` leg); `DropVector` fetches everything
/// except the big `vector` column (scoring: ids, metadata, `pq_code`, bounds).
#[derive(Debug, Clone, Copy)]
pub(crate) enum RangedColumns<'a> {
    Keep(&'a [&'a str]),
    DropVector,
}

/// A [`parquet`] `AsyncFileReader` backed by BORSUK's own object store handle, so
/// projected reads fetch only the needed column-chunk byte ranges without
/// coupling to `parquet`'s (older) bundled `object_store` version. The metadata
/// is pre-loaded from the footer, so the reader only ever issues data range GETs.
struct BorsukAsyncReader {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    metadata: Arc<ParquetMetaData>,
    bytes_fetched: Arc<AtomicU64>,
}

impl AsyncFileReader for BorsukAsyncReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let counter = Arc::clone(&self.bytes_fetched);
        async move {
            let bytes = store
                .get_range(&path, range.clone())
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))?;
            counter.fetch_add(range.end - range.start, Ordering::Relaxed);
            Ok(bytes)
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
}

impl PrefetchReadContext {
    fn from_storage(storage: &Storage) -> Self {
        Self {
            store: Arc::clone(&storage.store),
            prefix: storage.prefix.clone(),
            cache_dir: storage.cache_dir.clone(),
            cache_max_bytes: storage.cache_max_bytes,
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
        let size = self.object_size(relative).await?;
        let bytes = self.read_range_uncached(relative, 0..size).await?;
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

    async fn read_bytes_with_cache_status(&self, relative: &str) -> Result<ReadBytes> {
        if let Some(bytes) = self.read_cache_file(relative)? {
            return Ok(ReadBytes {
                bytes,
                cache_hit: true,
                cache_repaired: false,
            });
        }

        let size = self.object_size(relative).await?;
        let bytes = self.read_range_uncached(relative, 0..size).await?;
        self.write_cache_file(relative, &bytes)?;
        Ok(ReadBytes {
            bytes,
            cache_hit: false,
            cache_repaired: false,
        })
    }

    async fn object_size(&self, relative: &str) -> Result<u64> {
        let location = self.resolve(relative)?;
        let meta = self
            .store
            .head(&location)
            .await
            .map_err(|err| map_object_store_error(relative, err))?;
        Ok(meta.size)
    }

    async fn read_range_uncached(&self, relative: &str, range: Range<u64>) -> Result<Vec<u8>> {
        let location = self.resolve(relative)?;
        let bytes = self
            .store
            .get_range(&location, range)
            .await
            .map_err(|err| map_object_store_error(relative, err))?;
        Ok(bytes.to_vec())
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

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                BorsukError::InvalidStorage(format!(
                    "failed to create cache directory `{}`: {err}",
                    parent.display()
                ))
            })?;
        }
        fs::write(&path, bytes).map_err(|err| {
            BorsukError::InvalidStorage(format!(
                "failed to write cache file `{}`: {err}",
                path.display()
            ))
        })?;
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

    fn record_current_pointer(&mut self, bytes_len: usize) {
        self.bytes_written += bytes_len as u64;
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
        })
    }

    fn from_parts(
        uri: String,
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
        cache_dir: Option<PathBuf>,
        cache_max_bytes: Option<u64>,
    ) -> Result<Self> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| {
                BorsukError::InvalidStorage(format!("failed to create storage runtime: {err}"))
            })?;

        let request_counters = Arc::new(RequestCounters::default());
        let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore {
            inner: store,
            counters: Arc::clone(&request_counters),
        });

        Ok(Self {
            uri,
            store,
            prefix,
            cache_dir,
            cache_max_bytes,
            runtime: Arc::new(runtime),
            request_counters,
        })
    }

    /// Snapshot of object-store requests issued since this handle was opened.
    /// Callers diff two snapshots to attribute requests to a single operation.
    pub(crate) fn request_counts(&self) -> RequestCounts {
        self.request_counters.snapshot()
    }

    pub(crate) fn create_layout(&self) -> Result<()> {
        Ok(())
    }

    pub(crate) fn publish_manifest(&self, manifest: &Manifest) -> Result<Manifest> {
        self.publish_manifest_reusing_routing_pages(manifest, None)
    }

    pub(crate) fn publish_manifest_reusing_routing_pages(
        &self,
        manifest: &Manifest,
        previous: Option<&Manifest>,
    ) -> Result<Manifest> {
        Ok(self
            .publish_manifest_reusing_routing_pages_with_report(manifest, previous)?
            .0)
    }

    pub(crate) fn publish_manifest_reusing_routing_pages_with_report(
        &self,
        manifest: &Manifest,
        previous: Option<&Manifest>,
    ) -> Result<(Manifest, StorageWriteReport)> {
        let mut report = StorageWriteReport::default();
        let page_refs =
            self.routing_layer_page_refs_with_report(manifest, previous, 0, &mut report)?;
        let manifest = self.publish_manifest_with_routing_page_refs_with_report(
            manifest,
            &page_refs,
            &mut report,
        )?;
        Ok((manifest, report))
    }

    pub(crate) fn publish_manifest_with_routing_page_refs_with_report(
        &self,
        manifest: &Manifest,
        page_refs: &[RoutingLayerPageRef],
        report: &mut StorageWriteReport,
    ) -> Result<Manifest> {
        let span = observability::publish_span(manifest.version);
        let _entered = span.enter();
        let current_update_version = self.current_update_version()?;
        let mut manifest = manifest.clone();
        manifest.set_routing_max_level_for_leaf_pages(page_refs.len())?;
        self.write_routing_layer_page_indexes_with_report(&manifest, page_refs, report)?;
        self.publish_manifest_metadata_with_report(&manifest, current_update_version, report)?;
        observability::record_publish_report(&span, &manifest, report);
        Ok(manifest)
    }

    #[cfg(test)]
    pub(crate) fn publish_manifest_with_top_routing_page_refs(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        page_refs: &[RoutingLayerPageRef],
    ) -> Result<Manifest> {
        let mut report = StorageWriteReport::default();
        self.publish_manifest_with_top_routing_page_refs_with_report(
            manifest,
            routing_level,
            page_refs,
            &mut report,
        )
    }

    pub(crate) fn publish_manifest_with_top_routing_page_refs_with_report(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        page_refs: &[RoutingLayerPageRef],
        report: &mut StorageWriteReport,
    ) -> Result<Manifest> {
        let span = observability::publish_span(manifest.version);
        let _entered = span.enter();
        let current_update_version = self.current_update_version()?;
        let mut manifest = manifest.clone();
        manifest.routing_max_level = routing_level;
        let page_index_bytes =
            routing_layer_page_index_to_parquet(&manifest, routing_level, page_refs)?;
        self.write_bytes_if_absent(
            &Manifest::routing_layer_page_index_file_name(manifest.version, routing_level),
            &page_index_bytes,
        )?;
        report.record_metadata_table(page_index_bytes.len());
        self.publish_manifest_metadata_with_report(&manifest, current_update_version, report)?;
        observability::record_publish_report(&span, &manifest, report);
        Ok(manifest)
    }

    fn publish_manifest_metadata_with_report(
        &self,
        manifest: &Manifest,
        current_update_version: Option<UpdateVersion>,
        report: &mut StorageWriteReport,
    ) -> Result<()> {
        let manifest_bytes = manifest_to_parquet(manifest)?;
        let routing_bytes = routing_to_parquet(manifest)?;
        let pivots_bytes = pivots_to_parquet(manifest)?;
        let manifest_checksum = current_table_checksum(&manifest_bytes);
        let routing_checksum = current_table_checksum(&routing_bytes);
        let pivots_checksum = current_table_checksum(&pivots_bytes);

        self.write_bytes_if_absent(&manifest.file_name(), &manifest_bytes)?;
        report.record_metadata_table(manifest_bytes.len());
        self.write_bytes_if_absent(&manifest.routing_file_name(), &routing_bytes)?;
        report.record_metadata_table(routing_bytes.len());
        self.write_bytes_if_absent(&manifest.pivots_file_name(), &pivots_bytes)?;
        report.record_metadata_table(pivots_bytes.len());
        let current_pointer = encode_current(
            manifest.version,
            manifest_checksum,
            routing_checksum,
            pivots_checksum,
        );
        self.write_current_pointer(&current_pointer, current_update_version)?;
        report.record_current_pointer(current_pointer.len());
        Ok(())
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
    ) -> Result<Vec<RoutingLayerPageRef>> {
        let previous_refs = previous
            .map(|previous| self.read_routing_layer_page_index(previous.version, routing_level))
            .transpose()?
            .unwrap_or_default();
        let mut page_refs = Vec::new();

        for (page_ordinal, segments) in manifest
            .segments
            .chunks(manifest.routing_page_fanout)
            .enumerate()
        {
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

    pub(crate) fn load_current_manifest(&self) -> Result<Manifest> {
        if !self.exists(CURRENT)? {
            return Err(BorsukError::IndexNotFound(self.uri.clone()));
        }

        let pointer = decode_current(&self.read_bytes_uncached(CURRENT)?)?;
        let manifest_bytes = self.read_current_metadata_table(
            &Manifest::file_name_for_version(pointer.version),
            pointer.version,
            "manifest",
            pointer.manifest_checksum,
        )?;
        let routing_bytes = self.read_current_metadata_table(
            &Manifest::routing_file_name_for_version(pointer.version),
            pointer.version,
            "routing",
            pointer.routing_checksum,
        )?;
        let pivots_bytes = self.read_current_metadata_table(
            &Manifest::pivots_file_name_for_version(pointer.version),
            pointer.version,
            "pivots",
            pointer.pivots_checksum,
        )?;
        validate_current_metadata(
            &pointer,
            &manifest_bytes,
            Some(&routing_bytes),
            Some(&pivots_bytes),
        )?;

        let manifest_stores_next_generated_id = manifest_has_next_generated_id(&manifest_bytes)?;
        let mut manifest = manifest_from_parquet(&manifest_bytes, &routing_bytes)?;
        if manifest.version != pointer.version {
            return Err(BorsukError::InvalidStorage(format!(
                "CURRENT points to manifest version {}, but manifest table contains version {}",
                pointer.version, manifest.version
            )));
        }
        if !manifest_stores_next_generated_id {
            return Err(BorsukError::InvalidStorage(
                "manifest table is missing the next_generated_id column".to_string(),
            ));
        }
        manifest.pivots =
            pivots_from_parquet(&pivots_bytes, manifest.config.dimensions, manifest.version)?;
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

    pub(crate) fn load_current_manifest_metadata(&self) -> Result<Manifest> {
        if !self.exists(CURRENT)? {
            return Err(BorsukError::IndexNotFound(self.uri.clone()));
        }

        let pointer = decode_current(&self.read_bytes_uncached(CURRENT)?)?;
        let manifest_bytes = self.read_current_metadata_table(
            &Manifest::file_name_for_version(pointer.version),
            pointer.version,
            "manifest",
            pointer.manifest_checksum,
        )?;
        if pointer.manifest_checksum.is_some() {
            validate_current_metadata(&pointer, &manifest_bytes, None, None)?;
        } else {
            let routing_bytes = self.read_current_metadata_table(
                &Manifest::routing_file_name_for_version(pointer.version),
                pointer.version,
                "routing",
                pointer.routing_checksum,
            )?;
            let pivots_bytes = self.read_current_metadata_table(
                &Manifest::pivots_file_name_for_version(pointer.version),
                pointer.version,
                "pivots",
                pointer.pivots_checksum,
            )?;
            validate_current_metadata(
                &pointer,
                &manifest_bytes,
                Some(&routing_bytes),
                Some(&pivots_bytes),
            )?;
        }

        if !manifest_has_next_generated_id(&manifest_bytes)? {
            let mut manifest = self.load_current_manifest()?;
            manifest.segments.clear();
            manifest.pivots.clear();
            return Ok(manifest);
        }

        let manifest = manifest_metadata_from_parquet(&manifest_bytes)?;
        if manifest.version != pointer.version {
            return Err(BorsukError::InvalidStorage(format!(
                "CURRENT points to manifest version {}, but manifest table contains version {}",
                pointer.version, manifest.version
            )));
        }
        Ok(manifest)
    }

    fn read_current_metadata_table(
        &self,
        relative: &str,
        version: u64,
        table_name: &str,
        expected_checksum: Option<[u8; 32]>,
    ) -> Result<Vec<u8>> {
        let read = self.read_bytes_with_cache_status(relative)?;
        let Some(expected_checksum) = expected_checksum else {
            return Ok(read.bytes);
        };
        if current_table_checksum(&read.bytes) == expected_checksum {
            return Ok(read.bytes);
        }
        if !read.cache_hit {
            validate_current_table_checksum(version, table_name, &read.bytes, expected_checksum)?;
            return Ok(read.bytes);
        }

        self.delete_cache_file(relative)?;
        let fresh_bytes = self.read_bytes_uncached(relative)?;
        validate_current_table_checksum(version, table_name, &fresh_bytes, expected_checksum)?;
        Ok(fresh_bytes)
    }

    pub(crate) fn write_bytes(&self, relative: &str, bytes: &[u8]) -> Result<()> {
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

    fn write_bytes_with_mode(
        &self,
        relative: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<PutResult> {
        let location = self.resolve(relative)?;
        let payload = PutPayload::from(Bytes::copy_from_slice(bytes));
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
        Ok(result)
    }

    fn write_bytes_multipart(&self, relative: &str, bytes: &[u8]) -> Result<PutResult> {
        let location = self.resolve(relative)?;
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
        Ok(result)
    }

    fn write_current_pointer(
        &self,
        bytes: &[u8],
        current_update_version: Option<UpdateVersion>,
    ) -> Result<()> {
        match current_update_version {
            Some(version) => {
                match self.write_bytes_with_mode(CURRENT, bytes, PutMode::Update(version)) {
                    Ok(_) => Ok(()),
                    Err(BorsukError::ObjectStore(object_store::Error::NotImplemented {
                        ..
                    })) => self.write_bytes(CURRENT, bytes),
                    Err(err) => Err(err),
                }
            }
            None => {
                self.write_bytes_with_mode(CURRENT, bytes, PutMode::Create)?;
                Ok(())
            }
        }
    }

    fn current_update_version(&self) -> Result<Option<UpdateVersion>> {
        let location = self.resolve(CURRENT)?;
        match self
            .runtime
            .block_on(async { self.store.head(&location).await })
        {
            Ok(meta) => Ok(Some(UpdateVersion {
                e_tag: meta.e_tag,
                version: meta.version,
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(map_object_store_error(CURRENT, err)),
        }
    }

    fn read_bytes_uncached(&self, relative: &str) -> Result<Vec<u8>> {
        let size = self.object_size(relative)?;
        let location = self.resolve(relative)?;
        let bytes = self
            .runtime
            .block_on(async { self.store.get_range(&location, 0..size).await })
            .map_err(|err| map_object_store_error(relative, err))?
            .to_vec();
        self.write_cache_file(relative, &bytes)?;
        Ok(bytes)
    }

    pub(crate) fn read_bytes_with_cache_status(&self, relative: &str) -> Result<ReadBytes> {
        if let Some(bytes) = self.read_cache_file(relative)? {
            return Ok(ReadBytes {
                bytes,
                cache_hit: true,
                cache_repaired: false,
            });
        }

        let size = self.object_size(relative)?;
        let bytes = self.read_range(relative, 0..size)?;
        self.write_cache_file(relative, &bytes)?;
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
        let size = self.object_size(relative)?;
        let bytes = self.read_range(relative, 0..size)?;
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
        if let Some(bytes) = self.read_cache_file(relative)? {
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
            return Ok(bytes[start..end].to_vec());
        }

        let location = self.resolve(relative)?;
        let bytes = self
            .runtime
            .block_on(async { self.store.get_range(&location, range).await })
            .map_err(|err| map_object_store_error(relative, err))?;
        Ok(bytes.to_vec())
    }

    /// Read a projected subset of a Parquet object's columns (and, optionally, a
    /// subset of its rows) by fetching only the relevant column chunks over the
    /// object store — never the whole object. This is the object-store-native
    /// low-latency read: score from the compact `pq_codes` column, then rerank
    /// full vectors for a handful of rows, each a tight range read.
    ///
    /// `bytes_fetched` sums the compressed size of the projected column chunks in
    /// the row groups actually touched (the Parquet footer read is small and
    /// excluded); it is the tunable, object-store-billed cost of the query.
    pub(crate) fn read_parquet_columns_ranged(
        &self,
        relative: &str,
        size: u64,
        columns: RangedColumns<'_>,
        rows: Option<&[usize]>,
    ) -> Result<RangedParquetRead> {
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

        let arrow_metadata =
            ArrowReaderMetadata::try_new(Arc::clone(&parquet_metadata), ArrowReaderOptions::new())
                .map_err(|err| {
                    BorsukError::InvalidStorage(format!(
                        "derive arrow metadata for `{relative}`: {err}"
                    ))
                })?;

        let counter = Arc::new(AtomicU64::new(0));
        let reader = BorsukAsyncReader {
            store: Arc::clone(&self.store),
            path: self.resolve(relative)?,
            metadata: Arc::clone(&parquet_metadata),
            bytes_fetched: Arc::clone(&counter),
        };

        let selection = rows.map(|rows| {
            let mut sorted = rows.to_vec();
            sorted.sort_unstable();
            sorted.dedup();
            crate::format::row_selection_for_rows(&sorted, total_rows)
        });
        let relative_owned = relative.to_string();

        let batches = self.runtime.block_on(async move {
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_metadata)
                    .with_projection(mask);
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

        Ok(RangedParquetRead {
            batches,
            bytes_fetched: footer_bytes + counter.load(Ordering::Relaxed),
            total_rows,
        })
    }

    pub(crate) fn for_each_object(
        &self,
        relative_prefix: &str,
        mut visit: impl FnMut(StoredObject) -> Result<()>,
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

    #[cfg(test)]
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

    fn object_size(&self, relative: &str) -> Result<u64> {
        let location = self.resolve(relative)?;
        let meta = self
            .runtime
            .block_on(async { self.store.head(&location).await })
            .map_err(|err| map_object_store_error(relative, err))?;
        Ok(meta.size)
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

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                BorsukError::InvalidStorage(format!(
                    "failed to create cache directory `{}`: {err}",
                    parent.display()
                ))
            })?;
        }
        fs::write(&path, bytes).map_err(|err| {
            BorsukError::InvalidStorage(format!(
                "failed to write cache file `{}`: {err}",
                path.display()
            ))
        })?;
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

fn validate_current_metadata(
    pointer: &CurrentPointer,
    manifest_bytes: &[u8],
    routing_bytes: Option<&[u8]>,
    pivots_bytes: Option<&[u8]>,
) -> Result<()> {
    if let Some(manifest_checksum) = pointer.manifest_checksum {
        validate_current_table_checksum(
            pointer.version,
            "manifest",
            manifest_bytes,
            manifest_checksum,
        )?;
        if let (Some(routing_checksum), Some(routing_bytes)) =
            (pointer.routing_checksum, routing_bytes)
        {
            validate_current_table_checksum(
                pointer.version,
                "routing",
                routing_bytes,
                routing_checksum,
            )?;
        }
        if let (Some(pivots_checksum), Some(pivots_bytes)) = (pointer.pivots_checksum, pivots_bytes)
        {
            validate_current_table_checksum(
                pointer.version,
                "pivots",
                pivots_bytes,
                pivots_checksum,
            )?;
        }
        return Ok(());
    }

    let Some(routing_bytes) = routing_bytes else {
        return Err(BorsukError::InvalidStorage(
            "CURRENT v1 metadata validation requires routing bytes".to_string(),
        ));
    };
    let Some(pivots_bytes) = pivots_bytes else {
        return Err(BorsukError::InvalidStorage(
            "CURRENT v1 metadata validation requires pivot bytes".to_string(),
        ));
    };
    let actual_checksum = current_metadata_checksum(manifest_bytes, routing_bytes, pivots_bytes);
    if actual_checksum != pointer.metadata_checksum {
        return Err(BorsukError::InvalidStorage(format!(
            "CURRENT metadata checksum mismatch for manifest version {}",
            pointer.version
        )));
    }
    Ok(())
}

fn validate_current_table_checksum(
    version: u64,
    table_name: &str,
    bytes: &[u8],
    expected_checksum: [u8; 32],
) -> Result<()> {
    let actual_checksum = current_table_checksum(bytes);
    if actual_checksum != expected_checksum {
        return Err(BorsukError::InvalidStorage(format!(
            "CURRENT metadata checksum mismatch for manifest version {version} ({table_name} table)"
        )));
    }
    Ok(())
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
        for (coordinate, value) in centroid.iter_mut().zip(&segment.centroid) {
            *coordinate += value * weight;
        }
    }
    centroid
}

fn routing_layer_page_radius(manifest: &Manifest, segments: &[SegmentSummary]) -> Result<f32> {
    let centroid = routing_layer_page_centroid(manifest.config.dimensions, segments);
    segments.iter().try_fold(0.0_f32, |radius, segment| {
        let center_distance = manifest
            .config
            .metric
            .centroid_geometry_distance(&centroid, &segment.centroid)?;
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
        for (coordinate, value) in centroid.iter_mut().zip(&page_ref.centroid) {
            *coordinate += value * weight;
        }
    }
    centroid
}

fn routing_page_refs_radius(manifest: &Manifest, page_refs: &[RoutingLayerPageRef]) -> Result<f32> {
    let centroid = routing_page_refs_centroid(manifest.config.dimensions, page_refs);
    page_refs.iter().try_fold(0.0_f32, |radius, page_ref| {
        let center_distance = manifest
            .config
            .metric
            .centroid_geometry_distance(&centroid, &page_ref.centroid)?;
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
        fs::{self, OpenOptions},
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::{Duration, SystemTime},
    };

    use super::{PrefetchedRead, RangedColumns, ReadBytes, Storage};
    use crate::error::Result;
    use url::Url;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
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

        // The projected data must still decode correctly.
        let id_column = id_read.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 0);
    }
}
