//! Conditional S3 page-range fetch with generation-bound SHA-256 verification.

use crate::sq8_page_authority::{PageAuthority, PageError};
use crate::{
    exact_sq8_nominee::{ScoredNominee, Sq8Geometry, Sq8ScoreError},
    returned_sq8::{ReturnedRange, rank_returned_ranges},
};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::{GetOptions, GetResultPayload, ObjectStore, RetryConfig, path::Path};

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

/// S3 data reader with the `object_store` request retry loop disabled. The
/// query coordinator owns explicit retries and must charge every wire GET
/// against the physical cap. A live HTTP fixture must still verify this
/// transport's request accounting before a bounded-cost claim.
pub struct OneAttemptS3 {
    store: AmazonS3,
}

fn one_attempt_builder(bucket: &str, region: &str) -> Result<AmazonS3Builder, RangeFetchError> {
    if bucket.is_empty() || region.is_empty() {
        return Err(RangeFetchError::UnexpectedMetadata);
    }
    Ok(AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_retry(RetryConfig {
            max_retries: 0,
            ..Default::default()
        }))
}

impl OneAttemptS3 {
    pub fn new(bucket: &str, region: &str) -> Result<Self, RangeFetchError> {
        let store = one_attempt_builder(bucket, region)?
            .build()
            .map_err(RangeFetchError::Store)?;
        Ok(Self { store })
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
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn rank_verified_sq8_pages_inner(
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
) -> Result<RankedSq8, RankedSq8Failure> {
    let fail = |error| RankedSq8Failure {
        error,
        stats: Sq8ReadStats::default(),
    };
    if ranges.is_empty()
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
    let outcomes = stream::iter(ranges.iter().map(|&(first, last)| async move {
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
    let returned = verified
        .iter()
        .map(|range| ReturnedRange {
            start: range.start,
            bytes: &range.bytes,
        })
        .collect::<Vec<_>>();
    let candidates = rank_returned_ranges(
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
    )
    .map_err(|error| RankedSq8Failure {
        error: RangeFetchError::Score(error),
        stats,
    })?;
    Ok(RankedSq8 { candidates, stats })
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

    async fn request_fixture_once(
        authority: &PageAuthority,
        response: Vec<u8>,
    ) -> (Result<VerifiedRange, RangeFetchError>, Vec<String>) {
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
                        server_requests
                            .lock()
                            .unwrap()
                            .push(String::from_utf8(raw).unwrap());
                        socket.write_all(&response).unwrap();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                }
            }
        });
        let store = one_attempt_builder("fixture", "eu-central-1")
            .unwrap()
            .with_endpoint(format!("http://{address}"))
            .with_allow_http(true)
            .with_access_key_id("fixture")
            .with_secret_access_key("fixture")
            .build()
            .unwrap();
        let reader = OneAttemptS3 { store };
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
        (result, captured)
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
        let (fetched, requests) = request_fixture_once(&authority, good).await;
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
            http_response("412 Precondition Failed", None, "\"changed\"", 0, &[]),
            http_response("500 Internal Server Error", None, "\"frozen\"", 0, &[]),
        ];
        for response in faulty {
            let (result, requests) = request_fixture_once(&authority, response).await;
            assert!(result.is_err());
            assert_eq!(requests.len(), 1, "unexpected hidden HTTP retry");
        }
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
