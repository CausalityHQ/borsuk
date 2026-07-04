use std::{
    error::Error,
    fmt,
    future::Future,
    ops::Range,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use futures_util::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
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
    entries: Mutex<Vec<(StoreOperation, String)>>,
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
            .filter(|(operation, path)| predicate(*operation, path))
            .count()
    }

    fn record(&self, operation: StoreOperation, location: &ObjectPath) {
        self.entries
            .lock()
            .expect("operation log poisoned")
            .push((operation, location.to_string()));
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
    operation_log: Option<Arc<OperationLog>>,
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
            operation_log: None,
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
            operation_log: None,
        }
    }

    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = latency;
        self
    }

    pub fn with_operation_log(mut self) -> (Self, Arc<OperationLog>) {
        let operation_log = Arc::new(OperationLog::default());
        self.operation_log = Some(Arc::clone(&operation_log));
        (self, operation_log)
    }

    async fn maybe_sleep(&self) {
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }
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
            self.maybe_sleep().await;
            self.maybe_fail(StoreOperation::Put, location)?;
            self.record_operation(StoreOperation::Put, location);
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
            self.maybe_sleep().await;
            self.maybe_fail(StoreOperation::MultipartPut, location)?;
            self.record_operation(StoreOperation::MultipartPut, location);
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
            let operation = if options.head {
                StoreOperation::Head
            } else {
                StoreOperation::Get
            };
            self.maybe_sleep().await;
            self.maybe_fail(operation, location)?;
            self.record_operation(operation, location);
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
            self.maybe_sleep().await;
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
