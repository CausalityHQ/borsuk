//! Conditional S3 page-range fetch with generation-bound SHA-256 verification.

use crate::sq8_page_authority::{PageAuthority, PageError};
use crate::{
    exact_sq8_nominee::{ScoredNominee, Sq8Geometry, Sq8ScoreError},
    returned_sq8::{ReturnedRange, rank_returned_ranges_excluding},
};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream};
use http_body_util::BodyExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::client::{
    ClientConfigKey, ClientOptions, HttpClient, HttpConnector, HttpError, HttpErrorKind,
    HttpRequest, HttpResponse, HttpResponseBody, HttpService,
};
use object_store::{
    GetOptions, GetResultPayload, ObjectStore, RetryConfig, path::Path, prefix::PrefixStore,
};
use std::sync::{Arc, LazyLock, Mutex};

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

/// S3 data reader with the `object_store` request retry loop disabled. The
/// query coordinator owns explicit retries and must charge every wire GET
/// against the physical cap. A live HTTP fixture must still verify this
/// transport's request accounting before a bounded-cost claim.
pub struct OneAttemptS3 {
    store: PrefixStore<AmazonS3>,
    counters: Arc<Mutex<TransportCounters>>,
}

#[derive(Debug)]
struct NativeConnector {
    client: reqwest::Client,
    counters: Arc<Mutex<TransportCounters>>,
}

impl HttpConnector for NativeConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(NativeHttp {
            client: self.client.clone(),
            counters: self.counters.clone(),
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

fn one_attempt_builder(
    bucket: &str,
    region: &str,
    counters: Arc<Mutex<TransportCounters>>,
) -> Result<AmazonS3Builder, RangeFetchError> {
    if bucket.is_empty() || region.is_empty() {
        return Err(RangeFetchError::UnexpectedMetadata);
    }
    // Fixed native transport: SDK-default 30s request/5s connect, HTTP/1,
    // verified system TLS, and no redirects or transport-level retries.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .http1_only()
        .timeout(std::time::Duration::from_secs(30))
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
        .with_http_connector(NativeConnector { client, counters })
        .with_retry(RetryConfig {
            max_retries: 0,
            ..Default::default()
        }))
}

impl OneAttemptS3 {
    /// Process totals since the first native reader was created. Only chunks
    /// actually delivered by the body count, including later-rejected payloads.
    /// Unread/cancelled bodies, headers and kernel/TLS wire bytes are unknown.
    /// Counters saturate at u64::MAX and never reset between queries.
    pub fn transport_stats(&self) -> NativeTransportStats {
        let counters = self.counters.lock().unwrap();
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
        let counters = PROCESS_TRANSPORT.clone();
        let store = one_attempt_builder(bucket, region, counters.clone())?
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
    let fail = |error| RankedSq8Failure {
        error,
        stats: Sq8ReadStats::default(),
    };
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
    let (verified, stats) = fetch_verified_ranges_inner(
        store,
        location,
        authority,
        ranges,
        etag,
        max_gets,
        max_bytes,
        max_parallel,
    )
    .await?;
    let returned = verified
        .iter()
        .map(|range| ReturnedRange {
            start: range.start,
            bytes: &range.bytes,
        })
        .collect::<Vec<_>>();
    let candidates = rank_returned_ranges_excluding(
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
    )
    .map_err(|error| RankedSq8Failure {
        error: RangeFetchError::Score(error),
        stats,
    })?;
    Ok(RankedSq8 { candidates, stats })
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
    let fail = |error| RankedSq8Failure {
        error,
        stats: Sq8ReadStats::default(),
    };
    if ranges.is_empty() || ranges.len() > max_gets || max_parallel == 0 || etag.is_empty() {
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
        planned_bytes = planned_bytes
            .checked_add(interval.end - interval.start)
            .ok_or_else(|| fail(RangeFetchError::UnexpectedMetadata))?;
        if planned_bytes > max_bytes {
            return Err(fail(RangeFetchError::UnexpectedMetadata));
        }
        previous_last = Some(last);
    }
    let outcomes = stream::iter(ranges.iter().copied().map(|(first, last)| async move {
        fetch_verified_pages_inner(store, location, authority, first, last, etag, max_bytes).await
    }))
    .buffered(max_parallel)
    .collect::<Vec<_>>()
    .await;
    let mut stats = Sq8ReadStats {
        submitted_gets: ranges.len(),
        ..Sq8ReadStats::default()
    };
    let mut verified = Vec::with_capacity(ranges.len());
    let mut first_error = None;
    for outcome in outcomes {
        match outcome {
            Ok(value) => {
                stats.verified_bytes += value.bytes.len();
                verified.push(value);
            }
            Err(error) => {
                stats.failed_gets += 1;
                first_error.get_or_insert(error);
            }
        }
    }
    if let Some(error) = first_error {
        return Err(RankedSq8Failure { error, stats });
    }
    Ok((verified, stats))
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
    if etag.is_empty() {
        return Err(RangeFetchError::UnexpectedMetadata);
    }
    let expected = authority
        .byte_range(first_page, last_page)
        .map_err(RangeFetchError::Page)?;
    let expected_len = expected.end - expected.start;
    if expected_len > max_bytes {
        return Err(RangeFetchError::UnexpectedMetadata);
    }
    let start = u64::try_from(expected.start).map_err(|_| RangeFetchError::UnexpectedMetadata)?;
    let stop = u64::try_from(expected.end).map_err(|_| RangeFetchError::UnexpectedMetadata)?;
    let options = GetOptions::new()
        .with_range(Some(start..stop))
        .with_if_match(Some(etag.to_owned()));
    let result = store
        .get_opts(location, options)
        .await
        .map_err(RangeFetchError::Store)?;
    if result.range != (start..stop)
        || result.meta.size
            != u64::try_from(authority.object_bytes())
                .map_err(|_| RangeFetchError::UnexpectedMetadata)?
        || result.meta.e_tag.as_deref() != Some(etag)
    {
        return Err(RangeFetchError::UnexpectedMetadata);
    }
    let mut stream = match result.payload {
        GetResultPayload::Stream(stream) => stream,
        GetResultPayload::File(..) => return Err(RangeFetchError::UnexpectedMetadata),
    };
    let mut collected = BytesMut::with_capacity(expected_len);
    while let Some(next) = stream.next().await {
        let chunk = next.map_err(RangeFetchError::Store)?;
        let new_len = collected
            .len()
            .checked_add(chunk.len())
            .ok_or(RangeFetchError::UnexpectedMetadata)?;
        if new_len > expected_len {
            return Err(RangeFetchError::UnexpectedMetadata);
        }
        collected.extend_from_slice(&chunk);
    }
    let bytes = collected.freeze();
    authority
        .verify_payload(first_page, last_page, &bytes)
        .map_err(RangeFetchError::Page)?;
    Ok(VerifiedRange {
        start: expected.start,
        bytes,
    })
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
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_stop = Arc::clone(&stop);
        let server_requests = Arc::clone(&requests);
        let server = thread::spawn(move || {
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
                        let request = String::from_utf8(raw).unwrap();
                        let source = request
                            .lines()
                            .next()
                            .unwrap()
                            .contains("/plane/records.bin ");
                        server_requests.lock().unwrap().push(request);
                        let body = if source {
                            source_response.as_ref().unwrap_or(&response)
                        } else {
                            &response
                        };
                        socket.write_all(body).unwrap();
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
        let store = one_attempt_builder("fixture", "eu-central-1", counters.clone())
            .unwrap()
            .with_endpoint(format!("http://{address}"))
            .with_allow_http(true)
            .with_access_key_id("fixture")
            .with_secret_access_key("fixture")
            .build()
            .unwrap();
        let reader = OneAttemptS3 {
            store: PrefixStore::new(store, prefix),
            counters,
        };
        (reader, stop, requests, server)
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
}
