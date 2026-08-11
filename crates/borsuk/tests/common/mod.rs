#![allow(
    dead_code,
    reason = "shared integration-test support is target-specific"
)]

use std::{
    collections::HashSet,
    error::Error,
    fmt,
    future::Future,
    ops::Range,
    pin::Pin,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures_util::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    path::Path as ObjectPath,
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOperation {
    Put,
    MultipartPut,
    Get,
    Head,
    Delete,
    List,
    Copy,
    Rename,
}

#[derive(Debug, Default)]
pub struct OperationLog {
    entries: Mutex<Vec<OperationLogEntry>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoggedPutMode {
    Overwrite,
    Create,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationLogEntry {
    pub operation: StoreOperation,
    pub path: String,
    pub put_mode: Option<LoggedPutMode>,
    pub payload_bytes: Option<u64>,
}

impl OperationLog {
    pub fn clear(&self) {
        self.entries.lock().expect("operation log poisoned").clear();
    }

    pub fn count_matching(&self, predicate: impl Fn(StoreOperation, &str) -> bool) -> usize {
        self.entries
            .lock()
            .expect("operation log poisoned")
            .iter()
            .filter(|entry| predicate(entry.operation, &entry.path))
            .count()
    }

    pub fn matching_paths(&self, predicate: impl Fn(StoreOperation, &str) -> bool) -> Vec<String> {
        self.entries
            .lock()
            .expect("operation log poisoned")
            .iter()
            .filter(|entry| predicate(entry.operation, &entry.path))
            .map(|entry| entry.path.clone())
            .collect()
    }

    pub fn entries(&self) -> Vec<OperationLogEntry> {
        self.entries.lock().expect("operation log poisoned").clone()
    }

    fn record(&self, operation: StoreOperation, location: &ObjectPath) {
        self.entries
            .lock()
            .expect("operation log poisoned")
            .push(OperationLogEntry {
                operation,
                path: location.to_string(),
                put_mode: None,
                payload_bytes: None,
            });
    }

    fn record_put(&self, location: &ObjectPath, mode: &PutMode, payload_bytes: usize) {
        let put_mode = match mode {
            PutMode::Overwrite => LoggedPutMode::Overwrite,
            PutMode::Create => LoggedPutMode::Create,
            PutMode::Update(_) => LoggedPutMode::Update,
        };
        self.entries
            .lock()
            .expect("operation log poisoned")
            .push(OperationLogEntry {
                operation: StoreOperation::Put,
                path: location.to_string(),
                put_mode: Some(put_mode),
                payload_bytes: Some(payload_bytes as u64),
            });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectedErrorKind {
    Generic,
    NotFound,
    PermissionDenied,
    Unauthenticated,
}

type PathPredicate = dyn Fn(StoreOperation, &ObjectPath) -> bool + Send + Sync;

#[derive(Clone)]
pub struct FaultInjectingObjectStore {
    inner: Arc<dyn ObjectStore>,
    fault: Option<Arc<FaultRule>>,
    latency: Duration,
    get_latency: Option<(Duration, Arc<PathPredicate>)>,
    get_payload_latency: Option<(Duration, Arc<PathPredicate>)>,
    operation_log: Option<Arc<OperationLog>>,
    put_barrier: Option<Arc<PutBarrier>>,
    put_overlap_barrier: Option<Arc<PutOverlapBarrier>>,
    put_concurrency: Option<Arc<PutConcurrencyProbe>>,
    get_group_concurrency: Option<Arc<GetGroupConcurrencyProbe>>,
    fail_after_put: bool,
}

struct PutBarrier {
    barrier: Arc<Barrier>,
    predicate: Arc<PathPredicate>,
    released: AtomicBool,
}

struct PutOverlapBarrier {
    barrier: Arc<Barrier>,
    predicate: Arc<PathPredicate>,
    arrivals: AtomicUsize,
    parties: usize,
}

#[derive(Debug, Default)]
pub struct PutConcurrencyProbe {
    active: AtomicUsize,
    peak: AtomicUsize,
}

#[derive(Debug)]
pub struct GetGroupConcurrencyProbe {
    first_paths: HashSet<String>,
    second_paths: HashSet<String>,
    first_active: AtomicUsize,
    second_active: AtomicUsize,
    overlapped: AtomicBool,
}

impl GetGroupConcurrencyProbe {
    fn new(first_paths: HashSet<String>, second_paths: HashSet<String>) -> Self {
        Self {
            first_paths,
            second_paths,
            first_active: AtomicUsize::new(0),
            second_active: AtomicUsize::new(0),
            overlapped: AtomicBool::new(false),
        }
    }

    fn enter(&self, location: &ObjectPath) -> ActiveGroupedGet<'_> {
        let path = location.to_string();
        let group = if self.first_paths.contains(&path) {
            Some(GetGroup::First)
        } else if self.second_paths.contains(&path) {
            Some(GetGroup::Second)
        } else {
            None
        };
        match group {
            Some(GetGroup::First) => {
                self.first_active.fetch_add(1, Ordering::SeqCst);
                if self.second_active.load(Ordering::SeqCst) > 0 {
                    self.overlapped.store(true, Ordering::SeqCst);
                }
            }
            Some(GetGroup::Second) => {
                self.second_active.fetch_add(1, Ordering::SeqCst);
                if self.first_active.load(Ordering::SeqCst) > 0 {
                    self.overlapped.store(true, Ordering::SeqCst);
                }
            }
            None => {}
        }
        ActiveGroupedGet { probe: self, group }
    }

    pub fn overlapped(&self) -> bool {
        self.overlapped.load(Ordering::SeqCst)
    }
}

#[derive(Clone, Copy)]
enum GetGroup {
    First,
    Second,
}

struct ActiveGroupedGet<'a> {
    probe: &'a GetGroupConcurrencyProbe,
    group: Option<GetGroup>,
}

impl Drop for ActiveGroupedGet<'_> {
    fn drop(&mut self) {
        match self.group {
            Some(GetGroup::First) => {
                self.probe.first_active.fetch_sub(1, Ordering::SeqCst);
            }
            Some(GetGroup::Second) => {
                self.probe.second_active.fetch_sub(1, Ordering::SeqCst);
            }
            None => {}
        }
    }
}

impl PutConcurrencyProbe {
    fn enter(&self) -> ActivePut<'_> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        ActivePut { probe: self }
    }

    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

struct ActivePut<'a> {
    probe: &'a PutConcurrencyProbe,
}

impl Drop for ActivePut<'_> {
    fn drop(&mut self) {
        self.probe.active.fetch_sub(1, Ordering::SeqCst);
    }
}

struct FaultRule {
    fail_on_match: usize,
    recover_after_failure: bool,
    error_kind: InjectedErrorKind,
    predicate: Arc<PathPredicate>,
    state: Mutex<FaultState>,
}

#[derive(Debug, Default)]
struct FaultState {
    matches: usize,
    failed: bool,
}

impl FaultInjectingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            fault: None,
            latency: Duration::ZERO,
            get_latency: None,
            get_payload_latency: None,
            operation_log: None,
            put_barrier: None,
            put_overlap_barrier: None,
            put_concurrency: None,
            get_group_concurrency: None,
            fail_after_put: false,
        }
    }

    pub fn fail_nth_matching<F>(
        inner: Arc<dyn ObjectStore>,
        fail_on_match: usize,
        recover_after_failure: bool,
        predicate: F,
    ) -> Self
    where
        F: Fn(StoreOperation, &ObjectPath) -> bool + Send + Sync + 'static,
    {
        Self::fail_nth_matching_with_error(
            inner,
            fail_on_match,
            recover_after_failure,
            InjectedErrorKind::Generic,
            predicate,
        )
    }

    pub fn fail_nth_matching_with_error<F>(
        inner: Arc<dyn ObjectStore>,
        fail_on_match: usize,
        recover_after_failure: bool,
        error_kind: InjectedErrorKind,
        predicate: F,
    ) -> Self
    where
        F: Fn(StoreOperation, &ObjectPath) -> bool + Send + Sync + 'static,
    {
        assert!(fail_on_match > 0, "fail_on_match is one-based");
        Self {
            inner,
            fault: Some(Arc::new(FaultRule {
                fail_on_match,
                recover_after_failure,
                error_kind,
                predicate: Arc::new(predicate),
                state: Mutex::new(FaultState::default()),
            })),
            latency: Duration::ZERO,
            get_latency: None,
            get_payload_latency: None,
            operation_log: None,
            put_barrier: None,
            put_overlap_barrier: None,
            put_concurrency: None,
            get_group_concurrency: None,
            fail_after_put: false,
        }
    }

    pub fn accept_then_fail_nth_put<F>(
        inner: Arc<dyn ObjectStore>,
        fail_on_match: usize,
        predicate: F,
    ) -> Self
    where
        F: Fn(StoreOperation, &ObjectPath) -> bool + Send + Sync + 'static,
    {
        let mut store = Self::fail_nth_matching(inner, fail_on_match, true, predicate);
        store.fail_after_put = true;
        store
    }

    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = latency;
        self
    }

    pub fn with_get_latency_for<F>(mut self, latency: Duration, predicate: F) -> Self
    where
        F: Fn(StoreOperation, &ObjectPath) -> bool + Send + Sync + 'static,
    {
        self.get_latency = Some((latency, Arc::new(predicate)));
        self
    }

    pub fn with_get_payload_latency_for<F>(mut self, latency: Duration, predicate: F) -> Self
    where
        F: Fn(StoreOperation, &ObjectPath) -> bool + Send + Sync + 'static,
    {
        self.get_payload_latency = Some((latency, Arc::new(predicate)));
        self
    }

    /// Block the first matching put until every party of `barrier` reached its own first
    /// matching put, so overlapping publish races can be released into storage together.
    pub fn with_put_barrier<F>(mut self, barrier: Arc<Barrier>, predicate: F) -> Self
    where
        F: Fn(StoreOperation, &ObjectPath) -> bool + Send + Sync + 'static,
    {
        self.put_barrier = Some(Arc::new(PutBarrier {
            barrier,
            predicate: Arc::new(predicate),
            released: AtomicBool::new(false),
        }));
        self
    }

    /// Block the first `parties` matching PUTs together. When each operation can
    /// issue only one matching PUT before the barrier, this proves distinct
    /// public operations were simultaneously inside the real store boundary.
    pub fn with_first_matching_puts_barrier<F>(
        mut self,
        barrier: Arc<Barrier>,
        parties: usize,
        predicate: F,
    ) -> Self
    where
        F: Fn(StoreOperation, &ObjectPath) -> bool + Send + Sync + 'static,
    {
        assert!(parties > 1, "overlap barrier needs at least two parties");
        self.put_overlap_barrier = Some(Arc::new(PutOverlapBarrier {
            barrier,
            predicate: Arc::new(predicate),
            arrivals: AtomicUsize::new(0),
            parties,
        }));
        self
    }

    fn maybe_wait_at_put_barrier(&self, operation: StoreOperation, location: &ObjectPath) {
        let Some(put_barrier) = &self.put_barrier else {
            return;
        };
        if !(put_barrier.predicate)(operation, location) {
            return;
        }
        if put_barrier.released.swap(true, Ordering::SeqCst) {
            return;
        }
        put_barrier.barrier.wait();
    }

    fn maybe_wait_at_put_overlap_barrier(&self, operation: StoreOperation, location: &ObjectPath) {
        let Some(put_barrier) = &self.put_overlap_barrier else {
            return;
        };
        if !(put_barrier.predicate)(operation, location) {
            return;
        }
        if put_barrier.arrivals.fetch_add(1, Ordering::SeqCst) < put_barrier.parties {
            put_barrier.barrier.wait();
        }
    }

    pub fn with_operation_log(mut self) -> (Self, Arc<OperationLog>) {
        let operation_log = Arc::new(OperationLog::default());
        self.operation_log = Some(Arc::clone(&operation_log));
        (self, operation_log)
    }

    pub fn with_put_concurrency_probe(mut self) -> (Self, Arc<PutConcurrencyProbe>) {
        let probe = Arc::new(PutConcurrencyProbe::default());
        self.put_concurrency = Some(Arc::clone(&probe));
        (self, probe)
    }

    pub fn with_get_group_concurrency_probe(
        mut self,
        first_paths: HashSet<String>,
        second_paths: HashSet<String>,
    ) -> (Self, Arc<GetGroupConcurrencyProbe>) {
        let probe = Arc::new(GetGroupConcurrencyProbe::new(first_paths, second_paths));
        self.get_group_concurrency = Some(Arc::clone(&probe));
        (self, probe)
    }

    async fn maybe_sleep(&self) {
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }
    }

    async fn maybe_sleep_get(&self, operation: StoreOperation, location: &ObjectPath) {
        if let Some((latency, predicate)) = &self.get_latency
            && predicate(operation, location)
        {
            if !latency.is_zero() {
                tokio::time::sleep(*latency).await;
            }
            return;
        }
        self.maybe_sleep().await;
    }

    fn maybe_fail(
        &self,
        operation: StoreOperation,
        location: &ObjectPath,
    ) -> object_store::Result<()> {
        let Some(fault) = &self.fault else {
            return Ok(());
        };
        if !(fault.predicate)(operation, location) {
            return Ok(());
        }

        let mut state = fault.state.lock().expect("fault state poisoned");
        state.matches += 1;
        let should_fail =
            state.matches >= fault.fail_on_match && (!fault.recover_after_failure || !state.failed);
        if should_fail {
            state.failed = true;
            return Err(injected_error(fault.error_kind, operation, location));
        }
        Ok(())
    }

    fn record_operation(&self, operation: StoreOperation, location: &ObjectPath) {
        if let Some(operation_log) = &self.operation_log {
            operation_log.record(operation, location);
        }
    }
}

impl fmt::Debug for FaultInjectingObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FaultInjectingObjectStore")
            .field("inner", &self.inner)
            .field("has_fault", &self.fault.is_some())
            .field("latency", &self.latency)
            .finish()
    }
}

impl fmt::Display for FaultInjectingObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "FaultInjectingObjectStore({})", self.inner)
    }
}

impl ObjectStore for FaultInjectingObjectStore {
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
            let payload_bytes = payload.content_length();
            let _active_put = self
                .put_concurrency
                .as_deref()
                .map(PutConcurrencyProbe::enter);
            self.maybe_sleep().await;
            if let Some(operation_log) = &self.operation_log {
                operation_log.record_put(location, &opts.mode, payload_bytes);
            }
            if !self.fail_after_put {
                self.maybe_fail(StoreOperation::Put, location)?;
            }
            self.maybe_wait_at_put_barrier(StoreOperation::Put, location);
            self.maybe_wait_at_put_overlap_barrier(StoreOperation::Put, location);
            let result = self.inner.put_opts(location, payload, opts).await?;
            if self.fail_after_put {
                self.maybe_fail(StoreOperation::Put, location)?;
            }
            Ok(result)
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
            let _active_put = self
                .put_concurrency
                .as_deref()
                .map(PutConcurrencyProbe::enter);
            self.maybe_sleep().await;
            self.maybe_fail(StoreOperation::MultipartPut, location)?;
            self.record_operation(StoreOperation::MultipartPut, location);
            self.maybe_wait_at_put_barrier(StoreOperation::MultipartPut, location);
            self.maybe_wait_at_put_overlap_barrier(StoreOperation::MultipartPut, location);
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
            let _active_group_get = self
                .get_group_concurrency
                .as_deref()
                .map(|probe| probe.enter(location));
            let operation = if options.head {
                StoreOperation::Head
            } else {
                StoreOperation::Get
            };
            self.maybe_sleep_get(operation, location).await;
            self.maybe_fail(operation, location)?;
            self.record_operation(operation, location);
            let result = self.inner.get_opts(location, options).await?;
            let Some((latency, predicate)) = &self.get_payload_latency else {
                return Ok(result);
            };
            if !predicate(operation, location) || latency.is_zero() {
                return Ok(result);
            }
            let GetResult {
                payload,
                meta,
                range,
                attributes,
                extensions,
            } = result;
            let latency = *latency;
            let payload = match payload {
                GetResultPayload::Stream(stream) => GetResultPayload::Stream(
                    stream
                        .then(move |item| async move {
                            tokio::time::sleep(latency).await;
                            item
                        })
                        .boxed(),
                ),
                other => other,
            };
            Ok(GetResult {
                payload,
                meta,
                range,
                attributes,
                extensions,
            })
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
            let _active_group_get = self
                .get_group_concurrency
                .as_deref()
                .map(|probe| probe.enter(location));
            self.maybe_sleep_get(StoreOperation::Get, location).await;
            self.maybe_fail(StoreOperation::Get, location)?;
            self.record_operation(StoreOperation::Get, location);
            self.inner.get_ranges(location, ranges).await
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        let this = self.clone();
        let checked_locations = locations
            .then(move |location| {
                let this = this.clone();
                async move {
                    let location = location?;
                    this.maybe_sleep().await;
                    this.maybe_fail(StoreOperation::Delete, &location)?;
                    this.record_operation(StoreOperation::Delete, &location);
                    Ok(location)
                }
            })
            .boxed();
        self.inner.delete_stream(checked_locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let this = self.clone();
        let prefix = prefix.cloned();
        stream::once(async move {
            let location = prefix.clone().unwrap_or_else(|| ObjectPath::from(""));
            this.maybe_sleep().await;
            this.maybe_fail(StoreOperation::List, &location)?;
            this.record_operation(StoreOperation::List, &location);
            Ok::<_, object_store::Error>(this.inner.list(prefix.as_ref()))
        })
        .try_flatten()
        .boxed()
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
            let location = prefix.cloned().unwrap_or_else(|| ObjectPath::from(""));
            self.maybe_sleep().await;
            self.maybe_fail(StoreOperation::List, &location)?;
            self.record_operation(StoreOperation::List, &location);
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
            self.maybe_sleep().await;
            self.maybe_fail(StoreOperation::Copy, from)?;
            self.maybe_fail(StoreOperation::Copy, to)?;
            self.record_operation(StoreOperation::Copy, from);
            self.record_operation(StoreOperation::Copy, to);
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
            self.maybe_sleep().await;
            self.maybe_fail(StoreOperation::Rename, from)?;
            self.maybe_fail(StoreOperation::Rename, to)?;
            self.record_operation(StoreOperation::Rename, from);
            self.record_operation(StoreOperation::Rename, to);
            self.inner.rename_opts(from, to, options).await
        })
    }
}

fn injected_error(
    kind: InjectedErrorKind,
    operation: StoreOperation,
    location: &ObjectPath,
) -> object_store::Error {
    let path = location.to_string();
    let source = |path: &str| {
        Box::new(InjectedStoreError {
            operation,
            path: path.to_string(),
        }) as Box<dyn Error + Send + Sync>
    };
    match kind {
        InjectedErrorKind::Generic => object_store::Error::Generic {
            store: "fault-injecting",
            source: source(&path),
        },
        InjectedErrorKind::NotFound => object_store::Error::NotFound {
            source: source(&path),
            path,
        },
        InjectedErrorKind::PermissionDenied => object_store::Error::PermissionDenied {
            source: source(&path),
            path,
        },
        InjectedErrorKind::Unauthenticated => object_store::Error::Unauthenticated {
            source: source(&path),
            path,
        },
    }
}

#[derive(Debug)]
struct InjectedStoreError {
    operation: StoreOperation,
    path: String,
}

impl fmt::Display for InjectedStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "injected {:?} failure at {}",
            self.operation, self.path
        )
    }
}

impl Error for InjectedStoreError {}
