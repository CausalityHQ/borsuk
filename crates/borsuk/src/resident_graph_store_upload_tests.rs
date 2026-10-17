//! Authored source-only falsifiers; native execution belongs to the parent.
use super::*;
use async_trait::async_trait;
use futures_util::{stream, stream::BoxStream};
use object_store::{
    CopyOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutResult, UploadPart, memory::InMemory,
};
use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[path = "../tests/common/mod.rs"]
mod common;

#[derive(Debug, Default)]
struct Observed {
    events: Mutex<Vec<(&'static str, ObjectPath)>>,
    requests: Mutex<Vec<GetOptions>>,
    put_modes: Mutex<Vec<(ObjectPath, PutMode)>>,
    part_lengths: Mutex<Vec<usize>>,
    uploads: AtomicUsize,
    read_uploads: Mutex<Vec<usize>>,
    active_parts: AtomicUsize,
    peak_parts: AtomicUsize,
}
impl Observed {
    fn record(&self, event: &'static str, path: &ObjectPath) {
        self.events.lock().unwrap().push((event, path.clone()));
    }
    fn count(&self, event: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(e, _)| *e == event)
            .count()
    }
}

#[derive(Debug, Default)]
struct TestStore {
    inner: Arc<InMemory>,
    seen: Arc<Observed>,
    upload_fault: &'static str,
    read_fault: &'static str,
    read_fault_start: u64,
    root_lost: Option<ObjectPath>,
    abort_error: bool,
    barrier: Option<Arc<tokio::sync::Barrier>>,
    mutate_source: Option<std::path::PathBuf>,
}
impl fmt::Display for TestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("authenticated-upload-fixture")
    }
}
fn injected(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "authenticated-upload-fixture",
        source: io::Error::other(message).into(),
    }
}
fn exists(path: &ObjectPath) -> object_store::Error {
    object_store::Error::AlreadyExists {
        path: path.to_string(),
        source: io::Error::other("existing object").into(),
    }
}

fn alter_identity(fault: &str, e_tag: &mut Option<String>, version: &mut Option<String>) {
    match fault {
        "missing_identity" => {
            *e_tag = None;
            *version = None;
        }
        "empty_etag" => *e_tag = Some(String::new()),
        "weak_etag" => *e_tag = Some("W/weak".into()),
        "wildcard_etag" => *e_tag = Some("*".into()),
        "list_etag" => *e_tag = Some("a,b".into()),
        "control_etag" => *e_tag = Some("bad\n".into()),
        "padded_etag" => *e_tag = Some(" tag".into()),
        "large_etag" => *e_tag = Some("x".repeat(MAX_IDENTITY_BYTES + 1)),
        "capacity_etag" => e_tag
            .get_or_insert_with(|| "tag".into())
            .reserve(MAX_IDENTITY_BYTES + 1),
        "empty_version" => *version = Some(String::new()),
        "control_version" => *version = Some("bad\n".into()),
        "padded_version" => *version = Some("version ".into()),
        "large_version" => *version = Some("x".repeat(MAX_IDENTITY_BYTES + 1)),
        "capacity_version" => version
            .get_or_insert_with(|| "version".into())
            .reserve(MAX_IDENTITY_BYTES + 1),
        "etag_only" => *version = None,
        "version_only" => {
            *e_tag = None;
            *version = Some("version-1".into());
        }
        "max_etag" => {
            *e_tag = Some("x".repeat(MAX_IDENTITY_BYTES));
            *version = None;
        }
        "max_version" => {
            *e_tag = None;
            *version = Some("x".repeat(MAX_IDENTITY_BYTES));
        }
        _ => {}
    }
}

#[derive(Debug)]
struct TestUpload {
    inner: Box<dyn MultipartUpload>,
    store: Arc<InMemory>,
    path: ObjectPath,
    seen: Arc<Observed>,
    fault: &'static str,
    abort_error: bool,
    barrier: Option<Arc<tokio::sync::Barrier>>,
}
impl Drop for TestUpload {
    fn drop(&mut self) {
        self.seen.uploads.fetch_sub(1, Ordering::SeqCst);
        self.seen.record("drop_upload", &self.path);
    }
}
#[async_trait]
impl MultipartUpload for TestUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        self.seen
            .part_lengths
            .lock()
            .unwrap()
            .push(payload.content_length());
        self.seen.record("part", &self.path);
        if self.fault == "part" || (self.fault == "part_second" && self.seen.count("part") == 2) {
            return Box::pin(async { Err(injected("part failure")) });
        }
        if self.fault == "part_exists" {
            let path = self.path.clone();
            return Box::pin(async move { Err(exists(&path)) });
        }
        let pending = self.inner.put_part(payload);
        let seen = Arc::clone(&self.seen);
        Box::pin(async move {
            let active = seen.active_parts.fetch_add(1, Ordering::SeqCst) + 1;
            seen.peak_parts.fetch_max(active, Ordering::SeqCst);
            tokio::task::yield_now().await;
            let result = pending.await;
            seen.active_parts.fetch_sub(1, Ordering::SeqCst);
            result
        })
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.seen.record("complete", &self.path);
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        if self.fault == "foreign" {
            self.store.put(&self.path, "foreign winner".into()).await?;
            return Err(injected("completion response lost"));
        }
        let mut result = self.inner.complete().await?;
        if self.fault == "lost" {
            return Err(injected("completion response lost"));
        }
        alter_identity(self.fault, &mut result.e_tag, &mut result.version);
        Ok(result)
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.seen.record("abort", &self.path);
        self.inner.abort().await?;
        if self.abort_error {
            return Err(injected("abort failure"));
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for TestStore {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.seen.record("put", path);
        self.seen
            .put_modes
            .lock()
            .unwrap()
            .push((path.clone(), options.mode.clone()));
        let receipt = self.inner.put_opts(path, payload, options).await?;
        if self.root_lost.as_ref() == Some(path) {
            return Err(injected("root completion response lost"));
        }
        Ok(receipt)
    }
    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.seen.record("ordinary_multipart", path);
        self.inner.put_multipart_opts(path, options).await
    }
    async fn put_multipart_create(
        &self,
        path: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.seen.record("create", path);
        if let Some(source) = &self.mutate_source {
            std::fs::write(source, b"source!").unwrap();
        }
        if self.upload_fault == "init_exists" {
            return Err(exists(path));
        }
        let inner = self.inner.put_multipart_create(path, options).await?;
        self.seen.uploads.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(TestUpload {
            inner,
            store: Arc::clone(&self.inner),
            path: path.clone(),
            seen: Arc::clone(&self.seen),
            fault: self.upload_fault,
            abort_error: self.abort_error,
            barrier: self.barrier.clone(),
        }))
    }
    async fn get_opts(
        &self,
        path: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.seen
            .record(if options.head { "head" } else { "get" }, path);
        self.seen
            .read_uploads
            .lock()
            .unwrap()
            .push(self.seen.uploads.load(Ordering::SeqCst));
        self.seen.requests.lock().unwrap().push(options.clone());
        let head = options.head;
        let mut result = self.inner.get_opts(path, options).await?;
        if head {
            match self.read_fault {
                "head_path" => result.meta.location = ObjectPath::from("other/key"),
                "head_size" => result.meta.size += 1,
                _ => {}
            }
            alter_identity(
                self.read_fault,
                &mut result.meta.e_tag,
                &mut result.meta.version,
            );
        } else {
            let fault = if result.range.start >= self.read_fault_start {
                self.read_fault
            } else {
                ""
            };
            match fault {
                "get_path" => result.meta.location = ObjectPath::from("other/key"),
                "get_size" => result.meta.size += 1,
                "get_etag" => result.meta.e_tag = Some("different".into()),
                "get_version" => result.meta.version = Some("different".into()),
                "get_range" => result.range.start += 1,
                "get_capacity_etag" => result
                    .meta
                    .e_tag
                    .as_mut()
                    .unwrap()
                    .reserve(MAX_IDENTITY_BYTES + 1),
                _ => {}
            }
            let payload = std::mem::replace(
                &mut result.payload,
                GetResultPayload::Stream(stream::empty().boxed()),
            );
            let GetResultPayload::Stream(body) = payload else {
                unreachable!()
            };
            let body = body.map(move |chunk| {
                let bytes = chunk?;
                Ok(match fault {
                    "short" => bytes.slice(..bytes.len().saturating_sub(1)),
                    "corrupt" if !bytes.is_empty() => {
                        let mut bytes = bytes.to_vec();
                        bytes[0] ^= 1;
                        Bytes::from(bytes)
                    }
                    _ => bytes,
                })
            });
            let suffix = match fault {
                "extra" => vec![Ok(Bytes::from_static(b"!"))],
                "late_error" => vec![Err(injected("late body error"))],
                _ => vec![],
            };
            result.payload = GetResultPayload::Stream(body.chain(stream::iter(suffix)).boxed());
        }
        Ok(result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.seen.record("delete", &ObjectPath::default());
        self.inner.delete_stream(paths)
    }
    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

// Intentionally leave put_multipart_create at the trait's fail-closed default.
#[derive(Debug, Default)]
struct UnsupportedStore(TestStore);
impl fmt::Display for UnsupportedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
#[async_trait]
impl ObjectStore for UnsupportedStore {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.0.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(path, options).await
    }
    async fn get_opts(
        &self,
        path: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.0.get_opts(path, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.0.delete_stream(paths)
    }
    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.0.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}

fn source(bytes: &[u8]) -> (NamedTempFile, Artifact) {
    let file = NamedTempFile::new().unwrap();
    std::fs::write(file.path(), bytes).unwrap();
    let artifact = Artifact {
        bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(bytes)),
    };
    (file, artifact)
}
fn budget(artifact: &Artifact) -> usize {
    2 * multipart_part_bytes(artifact.bytes)
        .unwrap()
        .min(artifact.bytes as usize)
        .max(1)
        + LOCAL_READ_BYTES
        + AUTH_STATE_BYTES
}
async fn body(store: &TestStore, key: &ObjectPath) -> Bytes {
    store.inner.get(key).await.unwrap().bytes().await.unwrap()
}

#[tokio::test]
async fn fresh_create_and_reuse_return_exact_receipt_and_bounded_conditional_ranges() {
    assert_eq!(READBACK_RANGE_BYTES, 4 * 1024 * 1024);
    let bytes = vec![17; 2 * READBACK_RANGE_BYTES + 13];
    let (file, artifact) = source(&bytes);
    let store = TestStore::default();
    let key = ObjectPath::from("immutable/object");
    let first = upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
        .await
        .unwrap();
    assert_eq!(store.seen.count("head"), 0);
    let second = upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
        .await
        .unwrap();
    assert_eq!(first, second);
    let stored = body(&store, &key).await;
    assert_eq!(stored, bytes);
    assert_eq!(format!("{:x}", Sha256::digest(&stored)), artifact.sha256);
    assert_eq!(store.seen.count("create"), 2);
    assert_eq!(store.seen.count("ordinary_multipart"), 0);
    assert_eq!(store.seen.count("abort"), 1);
    assert_eq!(store.seen.count("head"), 1);
    assert_eq!(store.seen.count("get"), 3);
    assert_eq!(store.seen.count("delete"), 0);
    assert!(
        store
            .seen
            .read_uploads
            .lock()
            .unwrap()
            .iter()
            .all(|n| *n == 0)
    );
    let mut ranges = Vec::new();
    for request in store
        .seen
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|r| !r.head)
    {
        let Some(object_store::GetRange::Bounded(range)) = &request.range else {
            panic!("unbounded reuse read")
        };
        assert!(range.end - range.start <= READBACK_RANGE_BYTES as u64);
        assert_eq!(request.if_match, first.e_tag);
        assert_eq!(request.version, first.version);
        ranges.push(range.clone());
    }
    let window = 4 * 1024 * 1024_u64;
    assert_eq!(
        ranges,
        [0..window, window..2 * window, 2 * window..2 * window + 13]
    );
    let events = store.seen.events.lock().unwrap();
    let abort = events.iter().position(|(e, _)| *e == "abort").unwrap();
    let dropped = events
        .iter()
        .rposition(|(e, _)| *e == "drop_upload")
        .unwrap();
    let head = events.iter().position(|(e, _)| *e == "head").unwrap();
    assert!(abort < dropped && dropped < head);
}

#[tokio::test]
async fn reuse_readback_clamps_ranges_and_rejects_unsafe_budget_before_transport() {
    let window = 1024 * 1024 + 3;
    for (range_bytes, bytes, expected) in [
        (
            1,
            b"source".to_vec(),
            vec![0..1, 1..2, 2..3, 3..4, 4..5, 5..6],
        ),
        (
            window,
            vec![23; 2 * window + 17],
            vec![
                0..window as u64,
                window as u64..2 * window as u64,
                2 * window as u64..(2 * window + 17) as u64,
            ],
        ),
    ] {
        let (_file, artifact) = source(&bytes);
        let key = ObjectPath::from("immutable/budget-clamped");
        let store = TestStore::default();
        let winner = store
            .inner
            .put(&key, Bytes::copy_from_slice(&bytes).into())
            .await
            .unwrap();
        for limit in [
            0,
            AUTH_STATE_BYTES - 1,
            AUTH_STATE_BYTES,
            AUTH_STATE_BYTES + 1,
        ] {
            let error = authenticate_existing(&store, &key, &artifact, limit)
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                ResidentGraphStoreError::Invalid("upload memory budget")
            ));
            assert!(store.seen.events.lock().unwrap().is_empty());
        }
        let remaining = 2 * range_bytes + 1;
        assert!(remaining < READBACK_RANGE_BYTES);
        let receipt = authenticate_existing(&store, &key, &artifact, AUTH_STATE_BYTES + remaining)
            .await
            .unwrap();
        assert_eq!(receipt, UpdateVersion::from(winner));
        assert_eq!(store.seen.count("head"), 1);
        assert_eq!(store.seen.count("get"), expected.len());
        assert_eq!(store.seen.count("create"), 0);
        assert_eq!(store.seen.count("delete"), 0);
        let requests = store.seen.requests.lock().unwrap();
        let ranges: Vec<_> = requests
            .iter()
            .filter(|request| !request.head)
            .map(|request| {
                assert_eq!(request.if_match, receipt.e_tag);
                assert_eq!(request.version, receipt.version);
                let Some(object_store::GetRange::Bounded(range)) = &request.range else {
                    panic!("unbounded reuse read")
                };
                assert!(range.end - range.start <= range_bytes as u64);
                range.clone()
            })
            .collect();
        assert_eq!(ranges, expected);
    }
}

#[tokio::test]
async fn late_reuse_readback_faults_block_root_and_head_after_multiple_windows() {
    for (fault, expected) in [
        ("corrupt", "existing artifact SHA-256"),
        ("short", "existing artifact range length"),
        ("extra", "existing artifact range length"),
        ("late_error", "late body error"),
        ("get_etag", "existing artifact range identity"),
        ("get_version", "existing artifact range identity"),
        ("get_range", "existing artifact range identity"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let root = super::tests::fixture(directory.path(), 1);
        let mut root: serde_json::Value = serde_json::from_slice(&root).unwrap();
        let bytes = vec![29; 2 * READBACK_RANGE_BYTES + 13];
        let sha = format!("{:x}", Sha256::digest(&bytes));
        std::fs::write(directory.path().join("plane.bin"), &bytes).unwrap();
        root["plane"] = serde_json::json!({"bytes": bytes.len(), "sha256": sha});
        let root = serde_json::to_vec(&root).unwrap();
        let root_sha = format!("{:x}", Sha256::digest(&root));
        let prefix = ObjectPath::from("graph");
        let key = blob_path(&prefix, &sha);
        let store = TestStore {
            read_fault: fault,
            read_fault_start: 2 * READBACK_RANGE_BYTES as u64,
            ..Default::default()
        };
        store
            .inner
            .put(&key, Bytes::copy_from_slice(&bytes).into())
            .await
            .unwrap();
        let error = publish_graph_generation(&store, &prefix, &root, directory.path(), None)
            .await
            .unwrap_err();
        if fault == "late_error" {
            assert!(matches!(&error, ResidentGraphStoreError::Store(_)));
            assert!(error.to_string().contains(expected), "{fault}: {error}");
        } else {
            assert!(
                matches!(&error, ResidentGraphStoreError::Invalid(actual) if *actual == expected),
                "{fault}: {error}"
            );
        }
        assert_eq!(store.seen.count("head"), 1, "{fault}");
        assert_eq!(store.seen.count("get"), 3, "{fault}");
        assert_eq!(store.seen.count("create"), 1, "{fault}");
        assert_eq!(store.seen.count("abort"), 1, "{fault}");
        assert_eq!(store.seen.count("put"), 0, "{fault}");
        assert_eq!(store.seen.count("delete"), 0, "{fault}");
        assert!(
            store
                .inner
                .head(&root_path(&prefix, &root_sha))
                .await
                .is_err()
        );
        assert!(
            store
                .inner
                .head(&prefix.clone().join("head.json"))
                .await
                .is_err()
        );
        assert_eq!(body(&store, &key).await, bytes, "{fault}");
    }
}

#[tokio::test]
async fn invalid_fresh_receipts_preserve_committed_bytes_and_block_publication() {
    for fault in [
        "missing_identity",
        "empty_etag",
        "weak_etag",
        "wildcard_etag",
        "list_etag",
        "control_etag",
        "padded_etag",
        "large_etag",
        "capacity_etag",
        "empty_version",
        "control_version",
        "padded_version",
        "large_version",
        "capacity_version",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let root = super::tests::fixture(directory.path(), 1);
        let sha = format!("{:x}", Sha256::digest(&root));
        let parsed: Root = parse_authenticated_root(&root, &sha).unwrap();
        let prefix = ObjectPath::from("graph");
        let store = TestStore {
            upload_fault: fault,
            ..Default::default()
        };
        let error = publish_graph_generation(&store, &prefix, &root, directory.path(), None)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                ResidentGraphStoreError::Invalid("artifact completion identity")
            ),
            "{fault}: {error}"
        );
        assert_eq!(
            body(&store, &blob_path(&prefix, &parsed.plane.sha256)).await,
            "1:plane.bin",
            "{fault}"
        );
        assert!(store.inner.head(&root_path(&prefix, &sha)).await.is_err());
        assert!(
            store
                .inner
                .head(&prefix.clone().join("head.json"))
                .await
                .is_err()
        );
        assert_eq!(store.seen.count("create"), 1, "{fault}");
        assert_eq!(store.seen.count("complete"), 1, "{fault}");
        assert_eq!(store.seen.count("abort"), 1, "{fault}");
        assert_eq!(store.seen.count("head"), 0, "{fault}");
        assert_eq!(store.seen.count("get"), 0, "{fault}");
        assert_eq!(store.seen.count("put"), 0, "{fault}");
        assert_eq!(store.seen.count("delete"), 0, "{fault}");
    }
}

#[tokio::test]
async fn fresh_receipts_accept_etag_only_version_only_and_exact_identity_limit() {
    for fault in ["etag_only", "version_only", "max_etag", "max_version"] {
        let (file, artifact) = source(b"source");
        let key = ObjectPath::from("immutable/object");
        let store = TestStore {
            upload_fault: fault,
            ..Default::default()
        };
        let receipt =
            upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
                .await
                .unwrap();
        let expected = match fault {
            "etag_only" => UpdateVersion {
                e_tag: store.inner.head(&key).await.unwrap().e_tag,
                version: None,
            },
            "version_only" => UpdateVersion {
                e_tag: None,
                version: Some("version-1".into()),
            },
            "max_etag" => UpdateVersion {
                e_tag: Some("x".repeat(MAX_IDENTITY_BYTES)),
                version: None,
            },
            "max_version" => UpdateVersion {
                e_tag: None,
                version: Some("x".repeat(MAX_IDENTITY_BYTES)),
            },
            _ => unreachable!(),
        };
        assert_eq!(receipt, expected, "{fault}");
        for token in [receipt.e_tag.as_ref(), receipt.version.as_ref()]
            .into_iter()
            .flatten()
        {
            assert!(token.capacity() <= MAX_IDENTITY_BYTES, "{fault}");
        }
        assert_eq!(body(&store, &key).await, "source", "{fault}");
        assert_eq!(store.seen.count("complete"), 1, "{fault}");
        assert_eq!(store.seen.count("abort"), 0, "{fault}");
        assert_eq!(store.seen.count("head"), 0, "{fault}");
        assert_eq!(store.seen.count("get"), 0, "{fault}");
        assert_eq!(store.seen.count("delete"), 0, "{fault}");
    }
}

#[tokio::test]
async fn admission_and_default_not_supported_never_initiate_overwrite() {
    let (file, artifact) = source(b"source");
    let key = ObjectPath::from("immutable/object");
    let store = TestStore::default();
    let error =
        upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact) - 1)
            .await
            .unwrap_err();
    assert!(matches!(
        error,
        ResidentGraphStoreError::Invalid("upload memory budget")
    ));
    assert!(store.seen.events.lock().unwrap().is_empty());
    let unsupported = UnsupportedStore::default();
    let error = upload_authenticated_file(
        &unsupported,
        &key,
        file.path(),
        &artifact,
        budget(&artifact),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        ResidentGraphStoreError::Store(object_store::Error::NotSupported { .. })
    ));
    assert!(unsupported.0.seen.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn preexisting_wrong_length_or_bytes_are_preserved() {
    for winner in [b"bad".as_slice(), b"wrong!".as_slice()] {
        let (file, artifact) = source(b"source");
        let store = TestStore::default();
        let key = ObjectPath::from("immutable/object");
        store
            .inner
            .put(&key, Bytes::copy_from_slice(winner).into())
            .await
            .unwrap();
        assert!(
            upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
                .await
                .is_err()
        );
        assert_eq!(body(&store, &key).await, winner);
        assert_eq!(store.seen.count("abort"), 1);
        assert_eq!(store.seen.count("ordinary_multipart"), 0);
        assert_eq!(store.seen.count("delete"), 0);
    }
}

#[tokio::test]
async fn correct_destination_never_excuses_corrupt_or_growing_source() {
    for fault in ["", "init_exists", "part_exists", "grow"] {
        let (file, artifact) = source(b"source");
        let store = TestStore {
            upload_fault: if matches!(fault, "init_exists" | "part_exists") {
                fault
            } else {
                ""
            },
            mutate_source: (fault == "grow").then(|| file.path().to_owned()),
            ..Default::default()
        };
        let key = ObjectPath::from("immutable/object");
        store.inner.put(&key, "source".into()).await.unwrap();
        if fault != "grow" {
            std::fs::write(file.path(), b"broken").unwrap();
        }
        assert!(
            upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
                .await
                .is_err()
        );
        assert_eq!(store.seen.count("complete"), 0);
        assert_eq!(store.seen.count("head"), 0);
        assert_eq!(store.seen.count("get"), 0);
        assert_eq!(body(&store, &key).await, "source");
    }
}

#[tokio::test]
async fn identical_and_conflicting_completion_races_authenticate_the_winner() {
    for right in [b"left".as_slice(), b"else".as_slice()] {
        let (left_file, left_artifact) = source(b"left");
        let (right_file, right_artifact) = source(right);
        let store = TestStore {
            barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
            ..Default::default()
        };
        let key = ObjectPath::from("immutable/race");
        let (left, result) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                upload_authenticated_file(
                    &store,
                    &key,
                    left_file.path(),
                    &left_artifact,
                    budget(&left_artifact)
                ),
                upload_authenticated_file(
                    &store,
                    &key,
                    right_file.path(),
                    &right_artifact,
                    budget(&right_artifact)
                ),
            )
        })
        .await
        .unwrap();
        let winner = body(&store, &key).await;
        if right == b"left" {
            assert_eq!(left.unwrap(), result.unwrap());
            assert_eq!(winner, "left");
        } else {
            assert_ne!(left.is_ok(), result.is_ok());
            assert_eq!(
                winner,
                if left.is_ok() {
                    b"left".as_slice()
                } else {
                    right
                }
            );
        }
        assert_eq!(store.seen.count("abort"), 1);
        assert_eq!(store.seen.count("delete"), 0);
        assert_eq!(store.seen.count("ordinary_multipart"), 0);
    }
}

#[tokio::test]
async fn every_reuse_identity_range_and_body_fault_fails_closed() {
    for fault in [
        "head_path",
        "head_size",
        "missing_identity",
        "empty_etag",
        "weak_etag",
        "wildcard_etag",
        "list_etag",
        "control_etag",
        "large_etag",
        "empty_version",
        "control_version",
        "large_version",
        "padded_etag",
        "padded_version",
        "capacity_etag",
        "capacity_version",
        "get_capacity_etag",
        "get_path",
        "get_size",
        "get_etag",
        "get_version",
        "get_range",
        "short",
        "extra",
        "corrupt",
        "late_error",
    ] {
        let (file, artifact) = source(b"source");
        let store = TestStore {
            read_fault: fault,
            ..Default::default()
        };
        let key = ObjectPath::from("immutable/object");
        store.inner.put(&key, "source".into()).await.unwrap();
        assert!(
            upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
                .await
                .is_err(),
            "{fault}"
        );
        assert_eq!(body(&store, &key).await, "source", "{fault}");
        assert_eq!(store.seen.count("delete"), 0, "{fault}");
        assert!(
            store
                .seen
                .read_uploads
                .lock()
                .unwrap()
                .iter()
                .all(|n| *n == 0),
            "{fault}"
        );
    }
}

#[tokio::test]
async fn part_unknown_completion_foreign_winner_and_abort_errors_never_publish_dependencies() {
    for fault in ["part", "lost", "foreign"] {
        let directory = tempfile::tempdir().unwrap();
        let root = super::tests::fixture(directory.path(), 1);
        let store = TestStore {
            upload_fault: fault,
            abort_error: true,
            ..Default::default()
        };
        let prefix = ObjectPath::from("graph");
        let error = publish_graph_generation(&store, &prefix, &root, directory.path(), None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains(if fault == "part" {
            "part failure"
        } else {
            "completion response lost"
        }));
        assert_eq!(store.seen.count("abort"), 1);
        assert_eq!(store.seen.count("get"), 0);
        assert_eq!(store.seen.count("head"), 0);
        assert_eq!(store.seen.count("put"), 0);
        assert_eq!(store.seen.count("delete"), 0);
        assert!(
            store
                .inner
                .head(&prefix.clone().join("head.json"))
                .await
                .is_err()
        );
        let parsed: Root =
            parse_authenticated_root(&root, &format!("{:x}", Sha256::digest(&root))).unwrap();
        assert!(
            store
                .inner
                .head(&root_path(&prefix, &format!("{:x}", Sha256::digest(&root))))
                .await
                .is_err()
        );
        match fault {
            "lost" => assert_eq!(
                body(&store, &blob_path(&prefix, &parsed.plane.sha256)).await,
                "1:plane.bin"
            ),
            "foreign" => assert_eq!(
                body(&store, &blob_path(&prefix, &parsed.plane.sha256)).await,
                "foreign winner"
            ),
            _ => assert!(
                store
                    .inner
                    .head(&blob_path(&prefix, &parsed.plane.sha256))
                    .await
                    .is_err()
            ),
        }
    }
}

#[tokio::test]
async fn empty_source_has_one_empty_part_and_authenticated_eof_on_reuse() {
    let (file, artifact) = source(b"");
    let key = ObjectPath::from("immutable/empty");
    let store = TestStore::default();
    let first = upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
        .await
        .unwrap();
    let second = upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(*store.seen.part_lengths.lock().unwrap(), [0, 0]);
    {
        let requests = store.seen.requests.lock().unwrap();
        let get = requests.iter().find(|r| !r.head).unwrap();
        assert!(get.range.is_none());
        assert_eq!(get.if_match, first.e_tag);
    }
    for fault in ["extra", "late_error"] {
        let store = TestStore {
            read_fault: fault,
            ..Default::default()
        };
        store.inner.put(&key, PutPayload::default()).await.unwrap();
        assert!(
            upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
                .await
                .is_err(),
            "{fault}"
        );
    }
}

#[tokio::test]
async fn sequential_parts_keep_the_short_tail_and_validate_eof_before_complete() {
    let bytes = vec![19; MIN_PART_BYTES as usize + 1];
    let (file, artifact) = source(&bytes);
    let store = TestStore::default();
    let key = ObjectPath::from("immutable/multiple-parts");
    upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
        .await
        .unwrap();
    assert_eq!(
        *store.seen.part_lengths.lock().unwrap(),
        [MIN_PART_BYTES as usize, 1]
    );
    assert_eq!(store.seen.peak_parts.load(Ordering::SeqCst), 1);
    assert_eq!(store.seen.active_parts.load(Ordering::SeqCst), 0);
    assert_eq!(body(&store, &key).await, bytes);
}

#[test]
fn multipart_arithmetic_crosses_five_gib_without_an_object_size_cap() {
    for length in [
        0,
        MAX_PART_BYTES,
        MAX_PART_BYTES + 1,
        100_000_000 * 768 * 2,
        MAX_PART_BYTES * MAX_PARTS,
    ] {
        let part = multipart_part_bytes(length).unwrap() as u64;
        assert!((MIN_PART_BYTES..=MAX_PART_BYTES).contains(&part));
        assert!(length.div_ceil(part) <= MAX_PARTS);
    }
    assert!(multipart_part_bytes(MAX_PART_BYTES * MAX_PARTS + 1).is_err());
    assert!(multipart_part_bytes(u64::MAX).is_err());
}

#[tokio::test]
async fn stage_then_publish_reuses_all_blobs_and_the_create_only_root() {
    let directory = tempfile::tempdir().unwrap();
    let root = super::tests::fixture(directory.path(), 1);
    let prefix = ObjectPath::from("graph");
    let store = TestStore::default();
    let sha = stage_graph_generation(&store, &prefix, &root, directory.path())
        .await
        .unwrap();
    let version = store
        .inner
        .head(&root_path(&prefix, &sha))
        .await
        .unwrap()
        .e_tag;
    let head = publish_graph_generation(&store, &prefix, &root, directory.path(), None)
        .await
        .unwrap();
    assert_eq!(head.root_sha256, sha);
    assert_eq!(
        store
            .inner
            .head(&root_path(&prefix, &sha))
            .await
            .unwrap()
            .e_tag,
        version
    );
    assert_eq!(store.seen.count("create"), 10);
    assert_eq!(store.seen.count("ordinary_multipart"), 0);
    assert_eq!(store.seen.count("delete"), 0);
    assert!(
        store
            .seen
            .put_modes
            .lock()
            .unwrap()
            .iter()
            .all(|(_, mode)| matches!(mode, PutMode::Create))
    );
    let events = store.seen.events.lock().unwrap();
    assert_eq!(events.last().unwrap(), &("put", prefix.join("head.json")));
}

#[tokio::test]
async fn conflicting_blob_or_root_blocks_dependent_head_without_changing_winner() {
    for bad_root in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let root = super::tests::fixture(directory.path(), 1);
        let sha = format!("{:x}", Sha256::digest(&root));
        let parsed: Root = parse_authenticated_root(&root, &sha).unwrap();
        let prefix = ObjectPath::from("graph");
        let key = if bad_root {
            root_path(&prefix, &sha)
        } else {
            blob_path(&prefix, &parsed.plane.sha256)
        };
        let winner = if bad_root {
            let mut bytes = root.clone();
            bytes[0] ^= 1;
            bytes
        } else {
            b"bad blob".to_vec()
        };
        let store = TestStore::default();
        store.inner.put(&key, winner.clone().into()).await.unwrap();
        assert!(
            publish_graph_generation(&store, &prefix, &root, directory.path(), None)
                .await
                .is_err()
        );
        assert!(
            store
                .inner
                .head(&prefix.clone().join("head.json"))
                .await
                .is_err()
        );
        assert_eq!(body(&store, &key).await, winner);
        assert_eq!(store.seen.count("put"), usize::from(bad_root));
        assert_eq!(store.seen.count("delete"), 0);
    }
}

#[tokio::test]
async fn fault_wrapper_preserves_create_counts_and_fault_injection() {
    use common::{FaultInjectingObjectStore, StoreOperation};
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (store, log) = FaultInjectingObjectStore::new(inner).with_operation_log();
    let (file, artifact) = source(b"source");
    let key = ObjectPath::from("immutable/object");
    for _ in 0..2 {
        upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
            .await
            .unwrap();
    }
    assert_eq!(
        log.count_matching(|op, _| op == StoreOperation::MultipartPut),
        2
    );
    assert_eq!(log.count_matching(|op, _| op == StoreOperation::Head), 1);
    assert_eq!(log.count_matching(|op, _| op == StoreOperation::Get), 1);
    assert_eq!(
        log.count_matching(|op, _| matches!(op, StoreOperation::Put | StoreOperation::Delete)),
        0
    );
}

#[tokio::test]
async fn initiation_conflict_and_abort_failure_still_authenticate_exact_reuse() {
    for fault in ["init_exists", ""] {
        let (file, artifact) = source(b"source");
        let store = TestStore {
            upload_fault: fault,
            abort_error: true,
            ..Default::default()
        };
        let key = ObjectPath::from("immutable/object");
        let winner = store.inner.put(&key, "source".into()).await.unwrap();
        let receipt =
            upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
                .await
                .unwrap();
        assert_eq!(receipt, UpdateVersion::from(winner));
        assert_eq!(store.seen.count("abort"), usize::from(fault.is_empty()));
        assert_eq!(store.seen.count("head"), 1);
        assert_eq!(store.seen.count("get"), 1);
        assert_eq!(store.seen.count("delete"), 0);
    }
}

#[tokio::test]
async fn shared_fault_wrapper_does_not_bypass_multipart_injection() {
    use common::{FaultInjectingObjectStore, StoreOperation};
    let inner = Arc::new(InMemory::new());
    let store = FaultInjectingObjectStore::fail_nth_matching(
        Arc::clone(&inner) as Arc<dyn ObjectStore>,
        1,
        false,
        |operation, _| operation == StoreOperation::MultipartPut,
    );
    let (file, artifact) = source(b"source");
    let key = ObjectPath::from("immutable/object");
    assert!(
        upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
            .await
            .is_err()
    );
    assert!(inner.head(&key).await.is_err());
}

#[tokio::test]
async fn lost_root_reply_preserves_the_committed_root_and_blocks_head() {
    let directory = tempfile::tempdir().unwrap();
    let root = super::tests::fixture(directory.path(), 1);
    let sha = format!("{:x}", Sha256::digest(&root));
    let prefix = ObjectPath::from("graph");
    let store = TestStore {
        root_lost: Some(root_path(&prefix, &sha)),
        ..Default::default()
    };
    let error = publish_graph_generation(&store, &prefix, &root, directory.path(), None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("root completion response lost"));
    assert_eq!(body(&store, &root_path(&prefix, &sha)).await, root);
    assert!(
        store
            .inner
            .head(&prefix.clone().join("head.json"))
            .await
            .is_err()
    );
    assert_eq!(store.seen.count("put"), 1);
    assert_eq!(store.seen.count("head"), 0);
    assert_eq!(store.seen.count("get"), 0);
    assert_eq!(store.seen.count("delete"), 0);
}

#[tokio::test]
async fn second_part_failure_aborts_owned_upload_without_publication() {
    let (file, artifact) = source(&vec![7; MIN_PART_BYTES as usize + 1]);
    let key = ObjectPath::from("immutable/part-failure");
    let store = TestStore {
        upload_fault: "part_second",
        abort_error: true,
        ..Default::default()
    };
    let error = upload_authenticated_file(&store, &key, file.path(), &artifact, budget(&artifact))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("part failure"));
    assert_eq!(store.seen.count("part"), 2);
    assert_eq!(store.seen.count("complete"), 0);
    assert_eq!(store.seen.count("abort"), 1);
    assert_eq!(store.seen.count("head"), 0);
    assert_eq!(store.seen.count("get"), 0);
    assert_eq!(store.seen.count("delete"), 0);
    assert!(store.inner.head(&key).await.is_err());
}

// Real S3 is deliberately separate from the ordinary, in-memory falsifiers.
mod native_s3_canary {
    use super::*;
    use futures_util::FutureExt;
    use object_store::{ClientOptions, RetryConfig, aws::AmazonS3Builder};
    use std::{panic::AssertUnwindSafe, sync::atomic::AtomicBool};

    const MAX_CALLS: usize = 512;
    const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
    const MAX_UPLOADS: usize = 24;

    #[derive(Debug, Default)]
    struct Counters {
        calls: AtomicUsize,
        sent: AtomicUsize,
        received: AtomicUsize,
        initiations: AtomicUsize,
        parts: Mutex<Vec<usize>>,
    }

    fn charge(counter: &AtomicUsize, amount: usize, cap: usize) -> object_store::Result<()> {
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(amount).filter(|next| *next <= cap)
            })
            .map(|_| ())
            .map_err(|_| injected("native canary local budget exceeded"))
    }

    #[derive(Debug)]
    struct OwnedUpload {
        key: ObjectPath,
        inner: tokio::sync::Mutex<Box<dyn MultipartUpload>>,
        settled: AtomicBool,
    }

    #[derive(Debug)]
    struct CanaryUpload {
        owned: Arc<OwnedUpload>,
        counters: Arc<Counters>,
    }

    #[async_trait]
    impl MultipartUpload for CanaryUpload {
        fn put_part(&mut self, payload: PutPayload) -> UploadPart {
            let owned = Arc::clone(&self.owned);
            let counters = Arc::clone(&self.counters);
            Box::pin(async move {
                charge(&counters.calls, 1, MAX_CALLS)?;
                charge(&counters.sent, payload.content_length(), MAX_BODY_BYTES)?;
                counters
                    .parts
                    .lock()
                    .unwrap()
                    .push(payload.content_length());
                let mut upload = owned.inner.lock().await;
                upload.put_part(payload).await
            })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            charge(&self.counters.calls, 1, MAX_CALLS)?;
            let result = self.owned.inner.lock().await.complete().await;
            if result.is_ok() {
                self.owned.settled.store(true, Ordering::SeqCst);
            }
            result
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            charge(&self.counters.calls, 1, MAX_CALLS)?;
            let result = self.owned.inner.lock().await.abort().await;
            if result.is_ok() {
                self.owned.settled.store(true, Ordering::SeqCst);
            }
            result
        }
    }

    #[derive(Debug)]
    struct CanaryStore {
        inner: object_store::aws::AmazonS3,
        keys: Vec<ObjectPath>,
        touched: Mutex<Vec<ObjectPath>>,
        uploads: Mutex<Vec<Arc<OwnedUpload>>>,
        counters: Arc<Counters>,
    }

    impl fmt::Display for CanaryStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("bounded-native-s3-canary")
        }
    }

    impl CanaryStore {
        fn admit(&self, key: &ObjectPath, write: bool) -> object_store::Result<()> {
            if !self.keys.contains(key) {
                return Err(injected("native canary key outside owned roster"));
            }
            charge(&self.counters.calls, 1, MAX_CALLS)?;
            if write {
                let mut touched = self.touched.lock().unwrap();
                if !touched.contains(key) {
                    touched.push(key.clone());
                }
            }
            Ok(())
        }
    }

    #[async_trait]
    impl ObjectStore for CanaryStore {
        async fn put_opts(
            &self,
            key: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.admit(key, true)?;
            if !matches!(options.mode, PutMode::Create) {
                return Err(injected("native canary requires create-only writes"));
            }
            charge(
                &self.counters.sent,
                payload.content_length(),
                MAX_BODY_BYTES,
            )?;
            self.inner.put_opts(key, payload, options).await
        }

        async fn put_multipart_create(
            &self,
            key: &ObjectPath,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.admit(key, true)?;
            charge(&self.counters.initiations, 1, MAX_UPLOADS)?;
            let inner = self.inner.put_multipart_create(key, options).await?;
            let owned = Arc::new(OwnedUpload {
                key: key.clone(),
                inner: tokio::sync::Mutex::new(inner),
                settled: AtomicBool::new(false),
            });
            self.uploads.lock().unwrap().push(Arc::clone(&owned));
            Ok(Box::new(CanaryUpload {
                owned,
                counters: Arc::clone(&self.counters),
            }))
        }

        async fn get_opts(
            &self,
            key: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.admit(key, false)?;
            let mut result = self.inner.get_opts(key, options).await?;
            let payload = std::mem::replace(
                &mut result.payload,
                GetResultPayload::Stream(stream::empty().boxed()),
            );
            let GetResultPayload::Stream(body) = payload else {
                return Err(injected("native canary expected streaming body"));
            };
            let counters = Arc::clone(&self.counters);
            result.payload = GetResultPayload::Stream(
                body.map(move |chunk| {
                    let bytes = chunk?;
                    charge(&counters.received, bytes.len(), MAX_BODY_BYTES)?;
                    Ok(bytes)
                })
                .boxed(),
            );
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            _: &ObjectPath,
            _: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            Err(injected("native canary forbids overwrite multipart"))
        }

        fn delete_stream(
            &self,
            _: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            stream::once(async { Err(injected("native canary cleanup uses owned roster")) }).boxed()
        }

        fn list(
            &self,
            _: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            stream::once(async { Err(injected("native canary listing uses exact prefix")) }).boxed()
        }

        async fn list_with_delimiter(
            &self,
            _: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            Err(injected("native canary forbids delimiter listing"))
        }

        async fn copy_opts(
            &self,
            _: &ObjectPath,
            _: &ObjectPath,
            _: CopyOptions,
        ) -> object_store::Result<()> {
            Err(injected("native canary forbids copy"))
        }
    }

    async fn verify_bytes(
        store: &CanaryStore,
        key: &ObjectPath,
        expected: &[u8],
        identity: &UpdateVersion,
    ) -> Result<(), ResidentGraphStoreError> {
        let mut start = 0;
        loop {
            let end = (start + READBACK_RANGE_BYTES).min(expected.len());
            let result = store
                .get_opts(
                    key,
                    GetOptions {
                        if_match: identity.e_tag.clone(),
                        version: identity.version.clone(),
                        range: (!expected.is_empty()).then(|| (start as u64..end as u64).into()),
                        ..Default::default()
                    },
                )
                .await?;
            assert_eq!(result.meta.location, *key);
            assert_eq!(result.meta.size, expected.len() as u64);
            assert_eq!(result.meta.e_tag, identity.e_tag);
            assert_eq!(result.meta.version, identity.version);
            assert_eq!(result.range, start as u64..end as u64);
            let mut body = result.into_stream();
            let mut offset = start;
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                assert!(chunk.len() <= end - offset);
                assert_eq!(chunk.as_ref(), &expected[offset..offset + chunk.len()]);
                offset += chunk.len();
            }
            assert_eq!(offset, end);
            start = end;
            if start == expected.len() {
                return Ok(());
            }
        }
    }

    async fn cleanup(store: &CanaryStore, prefix: &ObjectPath) -> Vec<String> {
        let mut errors = Vec::new();
        let uploads = store.uploads.lock().unwrap().clone();
        for upload in &uploads {
            if !upload.settled.load(Ordering::SeqCst) {
                let result = tokio::time::timeout(Duration::from_secs(5), async {
                    upload.inner.lock().await.abort().await
                })
                .await;
                match result {
                    Ok(Ok(())) => upload.settled.store(true, Ordering::SeqCst),
                    other => errors.push(format!("abort {}: {other:?}", upload.key)),
                }
            }
        }
        let touched = store.touched.lock().unwrap().clone();
        for key in touched {
            match tokio::time::timeout(Duration::from_secs(5), store.inner.delete(&key)).await {
                Ok(Ok(())) => {}
                other => errors.push(format!("delete {key}: {other:?}")),
            }
        }
        // At most the ten planned keys plus one unexpected object, with no collecting listing.
        let mut remaining = store.inner.list(Some(prefix));
        for _ in 0..=store.keys.len() {
            match remaining.next().await {
                None => return errors,
                Some(Ok(meta)) => errors.push(format!("remaining object: {}", meta.location)),
                Some(Err(error)) => {
                    errors.push(format!("remaining object inventory failed: {error}"));
                    return errors;
                }
            }
        }
        errors.push("remaining object inventory truncated".into());
        errors
    }

    /// Root must independently verify unversioned storage, exclusive ownership and an empty
    /// multipart inventory, then inventory/clean the same prefix after exit (including SIGKILL).
    /// shortcut: opaque SDK upload IDs prevent server orphan inventory here; root owns AWS inventory.
    #[tokio::test]
    #[ignore = "real S3: explicit bucket/region/fresh owned prefix and unversioned declaration required"]
    async fn real_s3_authenticated_upload_canary() -> Result<(), Box<dyn std::error::Error>> {
        let bucket = std::env::var("BORSUK_NATIVE_TEST_BUCKET")?;
        let region = std::env::var("BORSUK_NATIVE_TEST_REGION")?;
        let prefix_text = std::env::var("BORSUK_NATIVE_TEST_PREFIX")?;
        if bucket.is_empty()
            || region.is_empty()
            || std::env::var("BORSUK_NATIVE_TEST_UNVERSIONED_BUCKET")? != bucket
        {
            return Err(io::Error::other("root must declare this bucket unversioned").into());
        }
        let suffix = prefix_text
            .strip_prefix("research/native-library-check/")
            .filter(|suffix| {
                !suffix.is_empty()
                    && suffix.len() <= 192
                    && suffix.split('/').all(|part| {
                        !part.is_empty()
                            && part
                                .bytes()
                                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
                    })
            })
            .ok_or_else(|| {
                io::Error::other(
                    "prefix must be a fresh owned research/native-library-check/ child",
                )
            })?;
        let prefix = ObjectPath::parse(format!("research/native-library-check/{suffix}"))?;
        if prefix.as_ref() != prefix_text {
            return Err(io::Error::other("prefix must be canonical").into());
        }
        let directory = tempfile::tempdir()?;
        let root_bytes = super::super::tests::fixture(directory.path(), 1);
        let root_sha = format!("{:x}", Sha256::digest(&root_bytes));
        let root: Root = parse_authenticated_root(&root_bytes, &root_sha)?;
        let graph_prefix = prefix.clone().join("graph");
        let mut keys = vec![
            prefix.clone().join("empty"),
            prefix.clone().join("two-parts"),
            prefix.clone().join("foreign"),
            root_path(&graph_prefix, &root_sha),
            graph_prefix.clone().join("head.json"),
        ];
        for artifact in [
            &root.plane,
            &root.graph,
            &root.map,
            &root.books,
            &root.codes,
        ] {
            keys.push(blob_path(&graph_prefix, &artifact.sha256));
        }
        // new(), not from_env(): static/web/container credentials and custom endpoints are excluded.
        let store = CanaryStore {
            inner: AmazonS3Builder::new()
                .with_bucket_name(&bucket)
                .with_region(&region)
                .with_client_options(
                    ClientOptions::new()
                        .with_timeout(Duration::from_secs(10))
                        .with_connect_timeout(Duration::from_secs(2)),
                )
                .with_retry(RetryConfig {
                    max_retries: 0,
                    retry_timeout: Duration::from_secs(10),
                    ..Default::default()
                })
                .build()?,
            keys,
            touched: Mutex::new(Vec::new()),
            uploads: Mutex::new(Vec::new()),
            counters: Arc::new(Counters::default()),
        };
        eprintln!("real S3 canary owned bucket={bucket} region={region} prefix={prefix}");
        // No writes or deletes on a failed preflight; stop at the first object or listing error.
        if let Some(item) = tokio::time::timeout(Duration::from_secs(15), async {
            store.inner.list(Some(&prefix)).next().await
        })
        .await?
        {
            return Err(io::Error::other(format!("namespace is not empty: {item:?}")).into());
        }

        let primary = tokio::time::timeout(
            Duration::from_secs(180),
            AssertUnwindSafe(async {
                for (name, length) in [("empty", 0), ("two-parts", MIN_PART_BYTES as usize + 1)] {
                    let bytes = vec![23; length];
                    let (file, artifact) = source(&bytes);
                    let key = prefix.clone().join(name);
                    let first = upload_authenticated_file(
                        &store,
                        &key,
                        file.path(),
                        &artifact,
                        budget(&artifact),
                    )
                    .await?;
                    verify_bytes(&store, &key, &bytes, &first).await?;
                    let second = upload_authenticated_file(
                        &store,
                        &key,
                        file.path(),
                        &artifact,
                        budget(&artifact),
                    )
                    .await?;
                    assert_eq!(first, second);
                    let meta = store.head(&key).await?;
                    assert_eq!(meta.size, length as u64);
                    assert_eq!(meta.e_tag, first.e_tag);
                    assert_eq!(meta.version, first.version);
                }
                assert_eq!(
                    *store.counters.parts.lock().unwrap(),
                    [0, 0, MIN_PART_BYTES as usize, 1, MIN_PART_BYTES as usize, 1]
                );

                let foreign_key = prefix.clone().join("foreign");
                let foreign = b"foreign winner";
                let receipt = store
                    .put_opts(
                        &foreign_key,
                        PutPayload::from(foreign.to_vec()),
                        PutOptions {
                            mode: PutMode::Create,
                            ..Default::default()
                        },
                    )
                    .await?;
                // Same length, different SHA: conflict authentication must read and reject the body.
                let (file, artifact) = source(b"another winner");
                assert_eq!(artifact.bytes, foreign.len() as u64);
                assert!(matches!(
                    upload_authenticated_file(
                        &store,
                        &foreign_key,
                        file.path(),
                        &artifact,
                        budget(&artifact),
                    )
                    .await,
                    Err(ResidentGraphStoreError::Invalid(
                        "existing artifact SHA-256"
                    ))
                ));
                verify_bytes(&store, &foreign_key, foreign, &UpdateVersion::from(receipt)).await?;

                let mut identities = Vec::new();
                for pass in 0..3 {
                    if pass < 2 {
                        assert_eq!(
                            stage_graph_generation(
                                &store,
                                &graph_prefix,
                                &root_bytes,
                                directory.path()
                            )
                            .await?,
                            root_sha
                        );
                        assert!(read_graph_head(&store, &graph_prefix).await?.is_none());
                    } else {
                        let head = publish_graph_generation(
                            &store,
                            &graph_prefix,
                            &root_bytes,
                            directory.path(),
                            None,
                        )
                        .await?;
                        assert_eq!(head.root_sha256, root_sha);
                        assert_eq!(head.root_bytes, root_bytes);
                    }
                    let mut current = Vec::new();
                    for key in &store.keys[3..] {
                        if *key != graph_prefix.clone().join("head.json") {
                            let meta = store.head(key).await?;
                            current.push((key.clone(), meta.size, meta.e_tag, meta.version));
                        }
                    }
                    if pass == 0 {
                        identities = current;
                    } else {
                        assert_eq!(current, identities);
                    }
                }
                assert_eq!(
                    read_graph_head(&store, &graph_prefix)
                        .await?
                        .unwrap()
                        .root_bytes,
                    root_bytes
                );
                Ok::<(), ResidentGraphStoreError>(())
            })
            .catch_unwind(),
        )
        .await;

        // Independent cleanup grace also runs after assertion panic, body error or scenario timeout.
        let cleaned = tokio::time::timeout(Duration::from_secs(90), cleanup(&store, &prefix)).await;
        let unconfirmed: Vec<_> = store
            .uploads
            .lock()
            .unwrap()
            .iter()
            .filter(|upload| !upload.settled.load(Ordering::SeqCst))
            .map(|upload| upload.key.to_string())
            .collect();
        // Method/body counters exclude preflight/cleanup, HTTP attempts, transport buffering and billing.
        eprintln!(
            "method calls={} sent_body_bytes={} received_body_bytes={} initiation_attempts={} returned_handles={} unconfirmed_handles={unconfirmed:?}; server multipart inventory UNKNOWN, root must inspect exact prefix={prefix}; cleanup={cleaned:?}; touched_keys={:?}; remaining objects UNKNOWN if cleanup timed out",
            store.counters.calls.load(Ordering::SeqCst),
            store.counters.sent.load(Ordering::SeqCst),
            store.counters.received.load(Ordering::SeqCst),
            store.counters.initiations.load(Ordering::SeqCst),
            store.uploads.lock().unwrap().len(),
            store.touched.lock().unwrap(),
        );
        match primary {
            Ok(Ok(result)) => result?,
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(error) => return Err(error.into()),
        }
        let errors = cleaned?;
        if !errors.is_empty() || !unconfirmed.is_empty() {
            return Err(io::Error::other(
                "canary cleanup incomplete; see exact-prefix diagnostics",
            )
            .into());
        }
        Ok(())
    }
}
