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
    timeout: std::time::Duration,
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
    mut trace: Option<&mut Sq8RangeTrace>,
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
    let (verified, stats) = fetch_verified_ranges_traced(
        store,
        location,
        authority,
        ranges,
        etag,
        max_gets,
        max_bytes,
        max_parallel,
        trace.as_deref_mut(),
    )
    .await?;
    if let Some(trace) = trace.as_deref_mut() {
        trace.rank_start_ns = Some(trace.now_ns());
    }
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
    if let Some(trace) = trace.as_deref_mut() {
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
/// the production one, polled by the same ordered `buffered` combinator; spans are
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
    mut trace: Option<&mut Sq8RangeTrace>,
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
        planned_bytes = planned_bytes
            .checked_add(interval.end - interval.start)
            .ok_or_else(|| fail(RangeFetchError::UnexpectedMetadata))?;
        if planned_bytes > max_bytes {
            return Err(fail(RangeFetchError::UnexpectedMetadata));
        }
        previous_last = Some(last);
    }
    let origin = trace.as_deref_mut().map(|trace| {
        trace.outcome = "fetching";
        trace.planned_ranges = u32::try_from(ranges.len()).unwrap_or(u32::MAX);
        trace.planned_bytes = u64::try_from(planned_bytes).unwrap_or(u64::MAX);
        trace.max_parallel = u32::try_from(max_parallel.min(ranges.len())).unwrap_or(u32::MAX);
        trace.fetch_start_ns = Some(trace.now_ns());
        trace.origin
    });
    // The buffered element is the named `fetch_verified_pages_traced` future itself, with no
    // extra async block around it, so a test can measure exactly the type `buffered` holds.
    let outcomes = stream::iter(ranges.iter().copied().enumerate().map(
        |(index, (first, last))| {
            fetch_verified_pages_traced(
                store,
                location,
                authority,
                first,
                last,
                etag,
                max_bytes,
                origin.map(|origin| (origin, index)),
            )
        },
    ))
    .buffered(max_parallel.min(ranges.len()))
    .collect::<Vec<_>>()
    .await;
    if let Some(trace) = trace.as_deref_mut() {
        trace.all_ranges_complete_ns = Some(trace.now_ns());
    }
    let mut stats = Sq8ReadStats {
        submitted_gets: ranges.len(),
        ..Sq8ReadStats::default()
    };
    let mut verified = Vec::with_capacity(ranges.len());
    let mut first_error = None;
    for (outcome, probe) in outcomes {
        if let (Some(trace), Some(probe)) = (trace.as_deref_mut(), probe) {
            trace.push(probe.span);
        }
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
    if let Some(trace) = trace.as_deref_mut() {
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
    stamp(&mut probe, |span, ns| span.first_poll_ns = Some(ns));
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
    stamp(&mut probe, |span, ns| {
        span.metadata_ns = Some(ns);
        span.outcome = "store_body";
    });
    let mut collected = BytesMut::with_capacity(expected_len);
    loop {
        let waited_from = probe.as_deref().map(Probe::now_ns);
        let next = stream.next().await;
        if let (Some(probe), Some(from)) = (probe.as_deref_mut(), waited_from) {
            let now = probe.now_ns();
            probe.span.note_wait(from, now);
        }
        let Some(next) = next else {
            stamp(&mut probe, |span, ns| span.eof_ns = Some(ns));
            break;
        };
        let chunk = next.map_err(RangeFetchError::Store)?;
        stamp(&mut probe, |span, ns| span.note_chunk(ns, chunk.len()));
        let new_len = collected
            .len()
            .checked_add(chunk.len())
            .ok_or(RangeFetchError::UnexpectedMetadata)?;
        if new_len > expected_len {
            stamp(&mut probe, |span, _| span.outcome = "overlong");
            return Err(RangeFetchError::UnexpectedMetadata);
        }
        let copied_from = probe.as_deref().map(Probe::now_ns);
        collected.extend_from_slice(&chunk);
        if let (Some(probe), Some(from)) = (probe.as_deref_mut(), copied_from) {
            let now = probe.now_ns();
            probe.span.note_copy(from, now);
        }
    }
    let bytes = collected.freeze();
    stamp(&mut probe, |span, ns| {
        span.auth_start_ns = Some(ns);
        span.outcome = "page_auth";
    });
    let authenticated = authority.verify_payload(first_page, last_page, &bytes);
    stamp(&mut probe, |span, ns| {
        span.auth_end_ns = Some(ns);
        if authenticated.is_ok() {
            span.outcome = "ok";
        }
    });
    authenticated.map_err(RangeFetchError::Page)?;
    Ok(VerifiedRange {
        start: expected.start,
        bytes,
    })
}

/// A range's diagnostic state while its future runs: private to that future.
struct Probe {
    origin: Instant,
    span: RangeSpan,
}

impl Probe {
    fn now_ns(&self) -> u64 {
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

/// Upper bounds for one query's opt-in range diagnostics.
pub const SQ8_TRACE_MAX_RANGES: usize = 128;
pub const SQ8_TRACE_MAX_BYTES: usize = 1 << 20;
/// Preregistered allowance for ONE pending range, whether or not tracing is on: the
/// instrumentable future (`fetch_verified_pages_traced`, the exact element `buffered`
/// holds), plus its executor node (`BUFFERED_NODE_ALLOWANCE_BYTES`) and the allocation
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
/// connect, TLS and server combined. Body gaps are application-observed waits
/// inside `stream.next()` (including executor scheduling), not packet arrival.
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
    /// Longest single wait for the next body frame, and when that wait ended.
    pub max_body_gap_ns: u64,
    pub max_body_gap_end_ns: Option<u64>,
    /// Aggregate time and count of the inline body copies into the collector.
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
    /// fetched, rank_failed or ranked.
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
    /// whole `RANGE_FUTURE_ALLOWANCE_BYTES` (future, executor node, allocation), the ordered
    /// queue may hold that many finished-but-unyielded outcomes, and the collected outcome
    /// vector is charged at its growth peak. Every outcome includes the pointer-sized probe
    /// slot. The tracing-off DELTA against the pre-change layout is only a part of this
    /// (one extra async layer, one probe pointer per future, one pointer per outcome). It is
    /// NOT measured by the layout test, which prints the current futures only: the
    /// pre-change future no longer exists in the tree. Until a remote run builds the same
    /// fixture on the base commit it is an unmeasured estimate of a few hundred bytes per
    /// pending range, inside the allowance charged above.
    pub fn pending_futures_bytes(ranges: usize, max_parallel: usize) -> usize {
        let outcome = std::mem::size_of::<RangeOutcome>();
        let pending = ranges.min(max_parallel);
        pending
            .saturating_mul(RANGE_FUTURE_ALLOWANCE_BYTES)
            .saturating_add(pending.saturating_mul(outcome.saturating_add(8)))
            .saturating_add(
                ranges
                    .saturating_mul(outcome)
                    .saturating_mul(OUTCOME_VECTOR_GROWTH_FACTOR),
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
        self.ranges.clear();
    }

    fn now_ns(&self) -> u64 {
        ns_since(self.origin)
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
        error_released: bool,
        sibling_released: bool,
        stop: bool,
        bytes: usize,
    }

    pub struct Fixture {
        pub reader: OneAttemptS3,
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
            listener.set_nonblocking(true).unwrap();
            let counters = Arc::new(Mutex::new(TransportCounters::default()));
            let store = one_attempt_builder(
                "fixture",
                "eu-central-1",
                counters.clone(),
                Duration::from_secs(10),
            )
            .unwrap()
            .with_endpoint(format!("http://{address}"))
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
                            if accepted > 2048 || handlers.len() >= 64 {
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
                state,
                server: Some(server),
                deadline,
            }
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
        let stage = if key.ends_with("/plane/records.bin") {
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
                        let body = if let Some(body) = &copy_response {
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
        let store =
            one_attempt_builder("fixture", "eu-central-1", counters.clone(), request_timeout)
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
        // The exact element type `buffered` holds in production, the body it wraps with no
        // probe, and the plain entry point used outside batches. Unpolled futures report
        // their whole state, so this is the full per-range footprint.
        let production =
            fetch_verified_pages_traced(&store, &location, &authority, 0, 0, "etag", 3328, None);
        let body =
            fetch_verified_pages_core(&store, &location, &authority, 0, 0, "etag", 3328, None);
        let plain = fetch_verified_pages_inner(&store, &location, &authority, 0, 0, "etag", 3328);
        let (production_bytes, body_bytes, plain_bytes) = (
            size_of_val(&production),
            size_of_val(&body),
            size_of_val(&plain),
        );
        let outcome = size_of::<RangeOutcome>();
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
        // The instrumentable layer is the only addition over the body, and it stays small.
        assert!(body_bytes <= production_bytes);
        assert!(
            production_bytes - body_bytes <= 512,
            "{production_bytes} {body_bytes}"
        );
        // With tracing off the probe slot, its borrow and the returned record are one pointer each,
        // and an outcome carries exactly one pointer more than the bare result.
        assert_eq!(size_of::<Option<Box<Probe>>>(), size_of::<usize>());
        assert_eq!(size_of::<Option<&mut Probe>>(), size_of::<usize>());
        assert!(outcome <= result + size_of::<usize>());
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
