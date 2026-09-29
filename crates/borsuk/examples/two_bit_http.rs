//! Development HTTP boundary for an authenticated, immutable native S3 generation.

#[path = "../src/native_development_memory.rs"]
mod native_development_memory;
use std::{error::Error, net::SocketAddr, sync::Arc, time::Instant};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};
use borsuk::{
    sq8_s3_range::OneAttemptS3,
    two_bit_generation::{TwoBitGeneration, TwoBitGenerationError, TwoBitGenerationLimits},
    two_bit_store::read_two_bit_head,
};
use object_store::{RetryConfig, aws::AmazonS3Builder, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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
    if request.k != 100
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
) -> Result<Response, StatusCode> {
    // Admit before JSON extraction, so rejected requests never allocate vector bodies.
    let _permit = admit(&state.permits)?;
    Ok(next.run(request).await)
}

async fn search(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SearchRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    validate(&request, &state.authority, state.dimensions)
        .map_err(|status| (status, Json(json!({"error":"invalid_request"}))))?;
    let started = Instant::now();
    let result = state
        .generation
        .search(&state.reader, &request.query, request.k)
        .await
        .map_err(|error| {
            let stats = match error {
                TwoBitGenerationError::Read(failure) => Some(failure.stats),
                _ => None,
            };
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error":"search_failed",
                "authority":state.authority,
                "submitted_gets":stats.map(|s|s.submitted_gets),
                "verified_bytes":stats.map(|s|s.verified_bytes),
                "failed_gets":stats.map(|s|s.failed_gets)})),
            )
        })?;
    let stats = result.ranked.stats;
    Ok(Json(json!({"authority":state.authority,
        "ids":result.ranked.candidates.iter().map(|r|r.id).collect::<Vec<_>>(),
        "ranges":result.plan.ranges.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>(),
        "planned_bytes":result.plan.planned_bytes,"native_wall_ns":started.elapsed().as_nanos(),
        "submitted_gets":stats.submitted_gets,"verified_bytes":stats.verified_bytes,
        "failed_gets":stats.failed_gets})))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 8 {
        return Err("usage: two_bit_http BUCKET REGION PREFIX TRUSTED_ROOT_SHA GENERATION CONTROL_EPOCH LOOPBACK_LISTEN".into());
    }
    let listen: SocketAddr = args[7].parse()?;
    if !listen.ip().is_loopback() {
        return Err("development listener must be loopback".into());
    }
    let authority = Authority {
        root_sha256: args[4].clone(),
        generation: args[5].parse()?,
        control_epoch: args[6].parse()?,
    };
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(&args[1])
        .with_region(&args[2])
        .with_retry(RetryConfig {
            max_retries: 0,
            ..Default::default()
        })
        .build()?;
    let started = Instant::now();
    let head = read_two_bit_head(&store, &ObjectPath::from(args[3].as_str()))
        .await?
        .ok_or("head missing")?;
    if head.is_empty()
        || head.root_sha256() != authority.root_sha256
        || head.generation() != authority.generation
        || head.control_epoch() != authority.control_epoch
        || head.dimensions() != 768
    {
        return Err("trusted head authority mismatch".into());
    }
    let head_read_wall_ns = started.elapsed().as_nanos();
    let scratch = tempfile::tempdir()?;
    let limits = TwoBitGenerationLimits {
        max_memory_bytes: native_development_memory::memory_limit()?,
        max_active_queries: 1,
        max_query_bytes: 16_773_120,
        max_query_gets: 32,
        max_parallel_gets: 32,
        max_query_scratch_bytes: 400_000,
        already_pinned_bytes: 0,
    };
    let started = Instant::now();
    let generation = TwoBitGeneration::open_remote(
        &store,
        &head.metadata_prefix(),
        head.root_sha256(),
        limits,
        scratch.path(),
    )
    .await?;
    let remote_open_wall_ns = started.elapsed().as_nanos();
    let state = Arc::new(AppState {
        generation,
        reader: OneAttemptS3::new(&args[1], &args[2])
            .map_err(|error| format!("reader: {error:?}"))?,
        authority,
        dimensions: head.dimensions(),
        permits: Arc::new(Semaphore::new(1)),
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
                    json!({"ready":true,"authority":state.authority,"dimensions":state.dimensions}),
                )
            }),
        )
        .layer(DefaultBodyLimit::max(65_536))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind(listen).await?;
    println!(
        "{}",
        json!({"phase":"ready","listen":listener.local_addr()?,
        "authority":state.authority,"head_read_wall_ns":head_read_wall_ns,
        "remote_open_wall_ns":remote_open_wall_ns})
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
        assert_eq!(validate(&request, &authority, 768), Ok(()));
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
        request.k = 101;
        assert_eq!(
            validate(&request, &authority, 768),
            Err(StatusCode::BAD_REQUEST)
        );
        request.k = 100;
        request.query.pop();
        assert_eq!(
            validate(&request, &authority, 768),
            Err(StatusCode::BAD_REQUEST)
        );
        let permits = Arc::new(Semaphore::new(1));
        let permit = admit(&permits).unwrap();
        assert_eq!(
            admit(&permits).unwrap_err(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        drop(permit);
        assert!(admit(&permits).is_ok());
        assert!(
            serde_json::from_value::<SearchRequest>(serde_json::json!({
                "query":[1.0],"k":100,"root_sha256":authority.root_sha256,
                "generation":1,"control_epoch":1,"unexpected":true
            }))
            .is_err()
        );
    }
}
