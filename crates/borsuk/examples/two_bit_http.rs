//! Development HTTP boundary for an authenticated, immutable native S3 generation.

#[path = "../src/native_development_memory.rs"]
mod native_development_memory;
use std::{error::Error, net::SocketAddr, sync::Arc, time::Instant};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, State, rejection::JsonRejection},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};
use borsuk::{
    rotated_two_bit::RotatedTwoBitCodec,
    sq8_s3_range::OneAttemptS3,
    two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits},
    two_bit_store::read_two_bit_head,
};
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const QUERY_SLOTS: usize = 4;
// Development HTTP admits Native100k's dimension range. Opening the generation
// still validates its actual profile, including Fresh1m's fixed D768 geometry.
const HTTP_DIMENSIONS: std::ops::RangeInclusive<usize> = 1..=1024;

#[derive(Clone, Serialize)]
struct Authority {
    root_sha256: String,
    generation: u64,
    control_epoch: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchRequest {
    query: Vec<f32>,
    k: usize,
    root_sha256: String,
    generation: u64,
    control_epoch: u64,
}

struct AppState {
    generation: TwoBitGeneration,
    reader: OneAttemptS3,
    authority: Authority,
    dimensions: usize,
    permits: Arc<Semaphore>,
}

fn transport_report(reader: &OneAttemptS3) -> Value {
    json!({
        "schema":"borsuk-native-transport-v1",
        "scope":"process_all_native_s3_readers",
        "per_query_delta":false,
        "attempt_measurement":"submitted HttpService calls, not confirmed wire or S3 requests",
        "totals":reader.transport_stats(),
        "method_order":["GET","HEAD","PUT","DELETE","POST","PATCH","OPTIONS","CONNECT","TRACE","other"],
        "status_counts_format":"[http_status,count] nonzero entries",
        "payload_measurement":"consumed response data frames, including unauthenticated payload",
        "dropped_error_body_consumed_bytes":0,
        "unknown":["unread_response_payload_bytes","response_header_bytes","request_wire_bytes","kernel_tls_wire_bytes"],
    })
}

fn validate(
    request: &SearchRequest,
    authority: &Authority,
    dimensions: usize,
) -> Result<(), StatusCode> {
    if request.root_sha256 != authority.root_sha256
        || request.generation != authority.generation
        || request.control_epoch != authority.control_epoch
    {
        return Err(StatusCode::CONFLICT);
    }
    if !HTTP_DIMENSIONS.contains(&dimensions)
        || !matches!(request.k, 10 | 100)
        || request.query.len() != dimensions
        || request.query.iter().any(|x| !x.is_finite())
        || !request.query.iter().any(|x| *x != 0.0)
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}

fn admit(permits: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit, StatusCode> {
    permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

async fn gate(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, Json<Value>)> {
    // Admit before JSON extraction, so rejected requests never allocate vector bodies.
    let _permit = admit(&state.permits).map_err(|status| {
        (
            status,
            Json(json!({
                "error":"query_capacity", "transport":transport_report(&state.reader),
            })),
        )
    })?;
    Ok(next.run(request).await)
}

async fn search(
    State(state): State<Arc<AppState>>,
    request: Result<Json<SearchRequest>, JsonRejection>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let Json(request) = request.map_err(|error| {
        (
            error.status(),
            Json(json!({
                "error":"invalid_request", "transport":transport_report(&state.reader),
            })),
        )
    })?;
    validate(&request, &state.authority, state.dimensions).map_err(|status| {
        (
            status,
            Json(json!({"error":"invalid_request", "transport":transport_report(&state.reader)})),
        )
    })?;
    let started = Instant::now();
    let result = state
        .generation
        .search(&state.reader, &request.query, request.k)
        .await
        .map_err(|error| {
            let (source, stats) = error.read_stats();
            let router = error.router_stats();
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error":"search_failed",
                "native_wall_ns":started.elapsed().as_nanos(),
                "router_submitted_gets":router.submitted_gets,
                "router_verified_bytes":router.verified_bytes,
                "router_failed_gets":router.failed_gets,
                "query_stages":error.stages(),
                "transport":transport_report(&state.reader),
                "authority":state.authority,
                "submitted_gets":stats.submitted_gets,
                "verified_bytes":stats.verified_bytes,
                "failed_gets":stats.failed_gets,
                "source_submitted_gets":source.submitted_gets,
                "source_verified_bytes":source.verified_bytes,
                "source_failed_gets":source.failed_gets})),
            )
        })?;
    let stats = result.ranked.stats;
    Ok(Json(json!({"authority":state.authority,
        "router_submitted_gets":result.router_stats.submitted_gets,
        "router_verified_bytes":result.router_stats.verified_bytes,
        "router_failed_gets":result.router_stats.failed_gets,
        "query_stages":result.stages,
        "transport":transport_report(&state.reader),
        "ids":result.ranked.candidates.iter().map(|r|r.id).collect::<Vec<_>>(),
        "ranges":result.plan.ranges.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>(),
        "planned_bytes":result.plan.planned_bytes,"native_wall_ns":started.elapsed().as_nanos(),
        "submitted_gets":stats.submitted_gets,"verified_bytes":stats.verified_bytes,
        "failed_gets":stats.failed_gets,
        "source_submitted_gets":result.source_stats.submitted_gets,
        "source_verified_bytes":result.source_stats.verified_bytes,
        "source_failed_gets":result.source_stats.failed_gets})))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 8 {
        return Err("usage: two_bit_http BUCKET REGION PREFIX TRUSTED_ROOT_SHA GENERATION CONTROL_EPOCH LOCAL_LISTEN".into());
    }
    let listen: SocketAddr = args[7].parse()?;
    if !listen.ip().is_loopback()
        && !matches!(listen.ip(), std::net::IpAddr::V4(ip) if ip.is_private())
    {
        return Err("development listener must be loopback or private IPv4".into());
    }
    let authority = Authority {
        root_sha256: args[4].clone(),
        generation: args[5].parse()?,
        control_epoch: args[6].parse()?,
    };
    let reader =
        OneAttemptS3::new(&args[1], &args[2]).map_err(|error| format!("reader: {error:?}"))?;
    let store = reader.store();
    let started = Instant::now();
    let head = read_two_bit_head(store, &ObjectPath::from(args[3].as_str()))
        .await?
        .ok_or("head missing")?;
    if head.is_empty()
        || head.root_sha256() != authority.root_sha256
        || head.generation() != authority.generation
        || head.control_epoch() != authority.control_epoch
    {
        return Err("trusted head authority mismatch".into());
    }
    if !HTTP_DIMENSIONS.contains(&head.dimensions()) {
        return Err("development HTTP dimensions must be in 1..=1024".into());
    }
    let head_read_wall_ns = started.elapsed().as_nanos();
    let scratch = tempfile::tempdir()?;
    let limits = TwoBitGenerationLimits {
        max_memory_bytes: native_development_memory::memory_limit()?,
        max_active_queries: QUERY_SLOTS,
        max_query_bytes: 16_773_120,
        max_query_gets: 32,
        max_parallel_gets: 32,
        max_source_bytes: 64 * 1024 * 1024,
        max_source_gets: 128,
        max_parallel_source_gets: 16,
        max_query_scratch_bytes: RotatedTwoBitCodec::required_query_scratch_bytes(
            head.dimensions(),
        )?,
        already_pinned_bytes: 0,
    };
    let started = Instant::now();
    let generation =
        TwoBitGeneration::open_remote_from_head(store, &head, limits, scratch.path()).await?;
    let remote_open_wall_ns = started.elapsed().as_nanos();
    let state = Arc::new(AppState {
        generation,
        reader,
        authority,
        dimensions: head.dimensions(),
        permits: Arc::new(Semaphore::new(QUERY_SLOTS)),
    });
    let router = Router::new()
        .route(
            "/search",
            post(search).layer(middleware::from_fn_with_state(state.clone(), gate)),
        )
        .route(
            "/health",
            get(|State(state): State<Arc<AppState>>| async move {
                Json(
                    json!({"ready":true,"authority":state.authority,"dimensions":state.dimensions,
                        "transport":transport_report(&state.reader)}),
                )
            }),
        )
        .layer(DefaultBodyLimit::max(65_536))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind(listen).await?;
    println!(
        "{}",
        json!({"phase":"ready","listen":listener.local_addr()?,
        "transport":transport_report(&state.reader),
        "authority":state.authority,"head_read_wall_ns":head_read_wall_ns,
        "remote_open_wall_ns":remote_open_wall_ns,
        "remote_open_stats":state.generation.remote_open_stats()})
    );
    axum::serve(listener, router).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn request_identity_geometry_and_nonqueued_admission() {
        let authority = Authority {
            root_sha256: "a".repeat(64),
            generation: 1,
            control_epoch: 1,
        };
        let mut request = SearchRequest {
            query: vec![1.0; 768],
            k: 100,
            root_sha256: authority.root_sha256.clone(),
            generation: 1,
            control_epoch: 1,
        };
        for dimensions in [1, 5, 768, 1024] {
            request.query = vec![1.0; dimensions];
            assert_eq!(validate(&request, &authority, dimensions), Ok(()));
        }
        for dimensions in [0, 1025] {
            request.query = vec![1.0; dimensions];
            assert_eq!(
                validate(&request, &authority, dimensions),
                Err(StatusCode::BAD_REQUEST)
            );
        }
        request.query = vec![1.0; 768];
        assert_eq!(validate(&request, &authority, 768), Ok(()));
        request.k = 10;
        assert_eq!(validate(&request, &authority, 768), Ok(()));
        request.k = 100;
        request.control_epoch = 2;
        assert_eq!(
            validate(&request, &authority, 768),
            Err(StatusCode::CONFLICT)
        );
        request.control_epoch = 1;
        request.query[0] = f32::NAN;
        assert_eq!(
            validate(&request, &authority, 768),
            Err(StatusCode::BAD_REQUEST)
        );
        request.query.fill(0.0);
        assert_eq!(
            validate(&request, &authority, 768),
            Err(StatusCode::BAD_REQUEST)
        );
        request.query.fill(1.0);
        for k in [0, 1, 9, 11, 99, 101] {
            request.k = k;
            assert_eq!(
                validate(&request, &authority, 768),
                Err(StatusCode::BAD_REQUEST)
            );
        }
        request.k = 100;
        request.query.pop();
        assert_eq!(
            validate(&request, &authority, 768),
            Err(StatusCode::BAD_REQUEST)
        );
        assert_eq!(QUERY_SLOTS, 4);
        let permits = Arc::new(Semaphore::new(QUERY_SLOTS));
        let mut held = (0..QUERY_SLOTS)
            .map(|_| admit(&permits).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            admit(&permits).unwrap_err(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        drop(held.pop());
        let replacement = admit(&permits).unwrap();
        assert_eq!(
            admit(&permits).unwrap_err(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        drop(held);
        drop(replacement);
        assert_eq!(permits.available_permits(), QUERY_SLOTS);
        assert!(
            serde_json::from_value::<SearchRequest>(serde_json::json!({
                "query":[1.0],"k":100,"root_sha256":authority.root_sha256,
                "generation":1,"control_epoch":1,"unexpected":true
            }))
            .is_err()
        );
    }
}
