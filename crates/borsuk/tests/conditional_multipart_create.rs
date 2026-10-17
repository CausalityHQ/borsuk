//! Native contract checks for the pinned conditional multipart dependency patch.
//! These loopback checks exercise AmazonS3's HTTP path, not the real S3 service.

use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, Response, StatusCode},
    response::IntoResponse,
};
use futures_util::{TryStreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut},
    chunked::ChunkedStore,
    limit::LimitStore,
    local::LocalFileSystem,
    memory::InMemory,
    path::Path,
    prefix::PrefixStore,
    throttle::{ThrottleConfig, ThrottledStore},
};

async fn contract(store: &dyn ObjectStore) {
    let path = Path::from("race/object");
    let mut left = store
        .put_multipart_create(&path, Default::default())
        .await
        .unwrap();
    let mut right = store
        .put_multipart_create(&path, Default::default())
        .await
        .unwrap();
    left.put_part("left-".into()).await.unwrap();
    left.put_part("body".into()).await.unwrap();
    right.put_part("right-".into()).await.unwrap();
    right.put_part("body".into()).await.unwrap();
    assert!(matches!(
        store.get(&path).await,
        Err(Error::NotFound { .. })
    ));
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut completions = tokio::task::JoinSet::new();
    for (mut upload, body) in [(left, "left-body"), (right, "right-body")] {
        let barrier = Arc::clone(&barrier);
        completions.spawn(async move {
            barrier.wait().await;
            (upload.complete().await, body)
        });
    }
    let (first, first_body) = completions.join_next().await.unwrap().unwrap();
    let (second, second_body) = completions.join_next().await.unwrap().unwrap();
    let expected = match (first, second) {
        (Ok(_), Err(Error::AlreadyExists { .. })) => first_body,
        (Err(Error::AlreadyExists { .. }), Ok(_)) => second_body,
        other => panic!("expected one winner and AlreadyExists: {other:?}"),
    };
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        expected
    );

    let sentinel = Path::from("sentinel");
    store.put(&sentinel, "sentinel".into()).await.unwrap();
    let mut upload = store
        .put_multipart_create(&sentinel, Default::default())
        .await
        .unwrap();
    upload.put_part("replacement".into()).await.unwrap();
    assert!(matches!(
        upload.complete().await,
        Err(Error::AlreadyExists { .. })
    ));
    assert_eq!(
        store.get(&sentinel).await.unwrap().bytes().await.unwrap(),
        "sentinel"
    );

    let late = Path::from("created-after-initiation");
    let mut upload = store
        .put_multipart_create(&late, Default::default())
        .await
        .unwrap();
    upload.put_part("replacement".into()).await.unwrap();
    store.put(&late, "late sentinel".into()).await.unwrap();
    assert!(matches!(
        upload.complete().await,
        Err(Error::AlreadyExists { .. })
    ));
    assert_eq!(
        store.get(&late).await.unwrap().bytes().await.unwrap(),
        "late sentinel"
    );

    let mut overwrite = store.put_multipart(&sentinel).await.unwrap();
    overwrite.put_part("ordinary-".into()).await.unwrap();
    overwrite.put_part("overwrite".into()).await.unwrap();
    overwrite.complete().await.unwrap();
    assert_eq!(
        store.get(&sentinel).await.unwrap().bytes().await.unwrap(),
        "ordinary-overwrite"
    );

    let aborted = Path::from("aborted");
    let mut upload = store
        .put_multipart_create(&aborted, Default::default())
        .await
        .unwrap();
    upload.put_part("unpublished".into()).await.unwrap();
    upload.abort().await.unwrap();
    assert!(matches!(
        store.get(&aborted).await,
        Err(Error::NotFound { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_conditional_multipart_race_sentinel_and_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path())
        .unwrap()
        .with_fsync(true);
    tokio::time::timeout(Duration::from_secs(10), contract(&store))
        .await
        .unwrap();
    for directory in [dir.path().to_path_buf(), dir.path().join("race")] {
        for entry in std::fs::read_dir(directory).unwrap() {
            assert!(!entry.unwrap().file_name().to_string_lossy().contains('#'));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_conditional_multipart_race_sentinel_and_overwrite() {
    tokio::time::timeout(Duration::from_secs(10), contract(&InMemory::new()))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_multipart_forwards_through_provided_wrappers() {
    let inner: Box<dyn ObjectStore> = Box::new(InMemory::new());
    let store = Arc::new(inner);
    let store = PrefixStore::new(store, "prefix");
    let store = LimitStore::new(store, 2);
    let store = ThrottledStore::new(store, ThrottleConfig::default());
    let store = ChunkedStore::new(Arc::new(store), 2);
    tokio::time::timeout(Duration::from_secs(10), contract(&store))
        .await
        .unwrap();
}

#[tokio::test]
async fn memory_multipart_keeps_chunks_and_cross_chunk_ranges() {
    let store = InMemory::new();
    let path = Path::from("chunks");
    let first = Bytes::from(vec![1, 2, 3]);
    let second = Bytes::from(vec![4, 5, 6, 7]);
    let mut upload = store
        .put_multipart_create(&path, Default::default())
        .await
        .unwrap();
    upload.put_part(first.clone().into()).await.unwrap();
    upload.put_part(second.clone().into()).await.unwrap();
    upload.complete().await.unwrap();

    let chunks: Vec<_> = store
        .get(&path)
        .await
        .unwrap()
        .into_stream()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].as_ptr(), first.as_ptr());
    assert_eq!(chunks[1].as_ptr(), second.as_ptr());
    assert_eq!(store.get_range(&path, 2..5).await.unwrap(), &[3, 4, 5][..]);
    assert_eq!(
        store.get_ranges(&path, &[1..4, 4..7]).await.unwrap(),
        vec![
            Bytes::from_static(&[2, 3, 4]),
            Bytes::from_static(&[5, 6, 7])
        ]
    );
    assert_eq!(store.head(&path).await.unwrap().size, 7);
    let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].size, 7);
    assert_eq!(
        store.list_with_delimiter(None).await.unwrap().objects[0].size,
        7
    );
    let copy = Path::from("copy");
    store.copy(&path, &copy).await.unwrap();
    assert_eq!(
        store.get(&copy).await.unwrap().bytes().await.unwrap(),
        &[1, 2, 3, 4, 5, 6, 7][..]
    );
}

#[tokio::test]
async fn memory_conditional_multipart_retry_preserves_attributes() {
    use object_store::{Attribute, AttributeValue, Attributes};

    let store = InMemory::new();
    let path = Path::from("retry-attributes");
    let attributes: Attributes = [
        (Attribute::ContentType, AttributeValue::from("application/x-borsuk")),
        (Attribute::Metadata("upload".into()), AttributeValue::from("retry")),
    ]
    .into_iter()
    .collect();
    let mut upload = store
        .put_multipart_create(
            &path,
            PutMultipartOptions {
                attributes: attributes.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    upload.put_part("first-".into()).await.unwrap();
    upload.put_part("second".into()).await.unwrap();
    let competitor = store.put(&path, "competitor".into()).await.unwrap();
    for _ in 0..2 {
        assert!(matches!(
            upload.complete().await,
            Err(Error::AlreadyExists { .. })
        ));
        let result = store.get(&path).await.unwrap();
        assert_eq!(result.meta.e_tag, competitor.e_tag);
        assert_eq!(result.bytes().await.unwrap(), "competitor");
    }

    store.delete(&path).await.unwrap();
    let completed = upload.complete().await.unwrap();
    let result = store.get(&path).await.unwrap();
    assert_eq!(result.meta.e_tag, completed.e_tag);
    assert_eq!(result.meta.size, 12);
    assert_eq!(result.attributes, attributes);
    assert_eq!(result.bytes().await.unwrap(), "first-second");
}

// Aliases total 4 GiB using only a 1 MiB allocation and bounded chunk metadata.
#[cfg(target_pointer_width = "32")]
#[tokio::test]
async fn memory_rejects_aliased_payload_length_overflow() {
    use object_store::{PutMode, UpdateVersion};

    let store = InMemory::new();
    let path = Path::from("overflow");
    let sentinel = store.put(&path, "sentinel".into()).await.unwrap();
    let chunk = Bytes::from(vec![0; 1024 * 1024]);
    let payload: PutPayload = (0..4096).map(|_| chunk.clone()).collect();
    for mode in [
        PutMode::Overwrite,
        PutMode::Create,
        PutMode::Update(UpdateVersion {
            e_tag: sentinel.e_tag.clone(),
            version: None,
        }),
    ] {
        assert!(matches!(
            store
                .put_opts(&path, payload.clone(), PutOptions::from(mode))
                .await,
            Err(Error::Generic {
                store: "InMemory",
                ..
            })
        ));
        let result = store.get(&path).await.unwrap();
        assert_eq!(result.meta.e_tag, sentinel.e_tag);
        assert_eq!(result.bytes().await.unwrap(), "sentinel");
    }
    let absent = Path::from("overflow-absent");
    assert!(matches!(
        store.put(&absent, payload).await,
        Err(Error::Generic {
            store: "InMemory",
            ..
        })
    ));
    assert!(matches!(
        store.head(&absent).await,
        Err(Error::NotFound { .. })
    ));

    for create in [false, true] {
        let mut upload = if create {
            store
                .put_multipart_create(&absent, Default::default())
                .await
                .unwrap()
        } else {
            store.put_multipart(&absent).await.unwrap()
        };
        for _ in 0..4096 {
            upload.put_part(chunk.clone().into()).await.unwrap();
        }
        for _ in 0..2 {
            assert!(matches!(
                upload.complete().await,
                Err(Error::Generic {
                    store: "InMemory",
                    ..
                })
            ));
            assert!(matches!(
                store.head(&absent).await,
                Err(Error::NotFound { .. })
            ));
        }
    }
}

#[derive(Debug, Default)]
struct UnsupportedStore {
    inner: InMemory,
    initiated: AtomicUsize,
}

impl fmt::Display for UnsupportedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UnsupportedStore")
    }
}

// Deliberately inherits the new method's default to model an existing external implementation.
#[async_trait]
impl ObjectStore for UnsupportedStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(path, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.initiated.fetch_add(1, Ordering::SeqCst);
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

#[tokio::test]
async fn unsupported_conditional_multipart_refuses_before_initiation() {
    let store = Arc::new(UnsupportedStore::default());
    assert!(matches!(
        store
            .put_multipart_create(&Path::from("object"), Default::default())
            .await,
        Err(Error::NotSupported { .. })
    ));
    let wrapped = PrefixStore::new(Arc::clone(&store), "prefix");
    assert!(matches!(
        wrapped
            .put_multipart_create(&Path::from("object"), Default::default())
            .await,
        Err(Error::NotSupported { .. })
    ));
    assert_eq!(store.initiated.load(Ordering::SeqCst), 0);
}

#[derive(Default)]
struct S3State {
    next_id: usize,
    uploads: HashMap<String, Vec<u8>>,
    objects: HashMap<String, Vec<u8>>,
    completions: Vec<Option<String>>,
    requests: usize,
}

fn xml(status: StatusCode, body: impl Into<Body>) -> axum::response::Response {
    (status, [("content-type", "application/xml")], body.into()).into_response()
}

async fn s3_request(
    State(state): State<Arc<Mutex<S3State>>>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let mut state = state.lock().unwrap();
    state.requests += 1;
    let key = uri.path().to_string();
    let query: HashMap<_, _> = url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .into_owned()
        .collect();
    if method == Method::POST && query.contains_key("uploads") {
        state.next_id += 1;
        let id = state.next_id.to_string();
        state.uploads.insert(id.clone(), Vec::new());
        return xml(
            StatusCode::OK,
            format!(
                "<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"
            ),
        );
    }
    if let Some(id) = query.get("uploadId") {
        if method == Method::PUT {
            state.uploads.get_mut(id).unwrap().extend_from_slice(&body);
            return (StatusCode::OK, [("etag", "\"part\"")]).into_response();
        }
        if method == Method::DELETE {
            state.uploads.remove(id);
            return StatusCode::NO_CONTENT.into_response();
        }
        if method == Method::POST {
            let conditional = headers
                .get("if-none-match")
                .map(|v| v.to_str().unwrap().to_owned());
            state.completions.push(conditional.clone());
            if key.ends_with("/error500") {
                return xml(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "<Error><Code>InternalError</Code></Error>",
                );
            }
            if key.ends_with("/error409") {
                return xml(
                    StatusCode::CONFLICT,
                    "<Error><Code>ConditionalRequestConflict</Code></Error>",
                );
            }
            if conditional.as_deref() == Some("*") && state.objects.contains_key(&key) {
                return xml(
                    StatusCode::PRECONDITION_FAILED,
                    "<Error><Code>PreconditionFailed</Code></Error>",
                );
            }
            let data = state.uploads.remove(id).unwrap();
            state.objects.insert(key.clone(), data);
            if key.ends_with("/lost") {
                // Commit first, then fail the response body: the client must preserve ambiguity.
                let stream = futures_util::stream::once(async {
                    Err::<Bytes, _>(std::io::Error::other("lost completion response"))
                });
                return Response::builder()
                    .status(200)
                    .header("content-length", "128")
                    .body(Body::from_stream(stream))
                    .unwrap();
            }
            return xml(
                StatusCode::OK,
                "<CompleteMultipartUploadResult><ETag>\"completed\"</ETag></CompleteMultipartUploadResult>",
            );
        }
    }
    xml(
        StatusCode::BAD_REQUEST,
        "<Error><Code>UnexpectedRequest</Code></Error>",
    )
}

async fn loopback(
    conditional_put: S3ConditionalPut,
) -> (AmazonS3, Arc<Mutex<S3State>>, tokio::task::JoinHandle<()>) {
    let state = Arc::new(Mutex::new(S3State::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .fallback(s3_request)
        .with_state(Arc::clone(&state));
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let store = AmazonS3Builder::new()
        .with_bucket_name("bucket")
        .with_region("us-east-1")
        .with_access_key_id("loopback")
        .with_secret_access_key("loopback")
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_conditional_put(conditional_put)
        .with_retry(object_store::RetryConfig {
            max_retries: 0,
            retry_timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .build()
        .unwrap();
    (store, state, task)
}

async fn s3_upload(store: &AmazonS3, key: &str, body: &'static str) -> Box<dyn MultipartUpload> {
    let mut upload = store
        .put_multipart_create(&Path::from(key), Default::default())
        .await
        .unwrap();
    upload.put_part(body.into()).await.unwrap();
    upload
}

#[tokio::test]
async fn amazon_s3_loopback_conditional_header_race_and_overwrite() {
    let (store, state, task) = loopback(S3ConditionalPut::ETagMatch).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut left = s3_upload(&store, "race", "left-body").await;
        let mut right = s3_upload(&store, "race", "right-body").await;
        let (a, b) = tokio::join!(left.complete(), right.complete());
        let expected = match (a, b) {
            (Ok(_), Err(Error::AlreadyExists { .. })) => {
                right.abort().await.unwrap();
                b"left-body".as_slice()
            }
            (Err(Error::AlreadyExists { .. }), Ok(_)) => {
                left.abort().await.unwrap();
                b"right-body".as_slice()
            }
            other => panic!("expected one conditional winner: {other:?}"),
        };
        {
            let state = state.lock().unwrap();
            assert_eq!(state.objects["/bucket/race"].as_slice(), expected);
            assert_eq!(state.completions, [Some("*".into()), Some("*".into())]);
        }
        let mut sentinel = s3_upload(&store, "race", "replacement").await;
        assert!(matches!(
            sentinel.complete().await,
            Err(Error::AlreadyExists { .. })
        ));
        assert_eq!(
            state.lock().unwrap().objects["/bucket/race"].as_slice(),
            expected
        );
        sentinel.abort().await.unwrap();

        let mut overwrite = store.put_multipart(&Path::from("race")).await.unwrap();
        overwrite.put_part("ordinary".into()).await.unwrap();
        overwrite.complete().await.unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.objects["/bucket/race"].as_slice(), b"ordinary");
        assert_eq!(state.completions.last(), Some(&None));
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn amazon_s3_loopback_http_errors_and_lost_completion_response() {
    let (store, state, task) = loopback(S3ConditionalPut::ETagMatch).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        for key in ["error500", "error409"] {
            let mut upload = s3_upload(&store, key, "failed").await;
            let error = upload.complete().await.unwrap_err();
            // The pinned client's existing HTTP 409 mapping is AlreadyExists.
            if key == "error409" {
                assert!(matches!(error, Error::AlreadyExists { .. }), "{error}");
            } else {
                assert!(matches!(error, Error::Generic { .. }), "{error}");
            }
            assert!(
                !state
                    .lock()
                    .unwrap()
                    .objects
                    .contains_key(&format!("/bucket/{key}"))
            );
            upload.abort().await.unwrap();
        }
        let mut upload = s3_upload(&store, "lost", "committed").await;
        assert!(upload.complete().await.is_err());
        assert_eq!(
            state.lock().unwrap().objects["/bucket/lost"].as_slice(),
            b"committed"
        );
        let mut retry = s3_upload(&store, "lost", "replacement").await;
        assert!(matches!(
            retry.complete().await,
            Err(Error::AlreadyExists { .. })
        ));
        assert_eq!(
            state.lock().unwrap().objects["/bucket/lost"].as_slice(),
            b"committed"
        );
        retry.abort().await.unwrap();
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn amazon_s3_disabled_conditional_multipart_refuses_before_http() {
    let (disabled, state, task) = loopback(S3ConditionalPut::Disabled).await;
    assert!(matches!(
        disabled
            .put_multipart_create(&Path::from("disabled"), Default::default())
            .await,
        Err(Error::NotSupported { .. })
    ));
    assert_eq!(state.lock().unwrap().requests, 0);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}
