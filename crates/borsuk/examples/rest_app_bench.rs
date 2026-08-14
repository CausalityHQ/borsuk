//! Same-process REST workload-isolation benchmark application.
//!
//! The cheap endpoints remain on Axum's async runtime, while vector search is
//! admitted without queueing and executed on BORSUK's bounded blocking pools.

use std::{env, net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use borsuk::{BorsukError, BorsukIndex, LeafMode, OpenOptions, ProcessLimits, SearchOptions};
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
struct SearchAdmission {
    permits: Arc<Semaphore>,
}

impl SearchAdmission {
    fn new(limit: usize) -> Result<Self, String> {
        if limit == 0 {
            return Err("REST search admission must be greater than zero".to_owned());
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(limit)),
        })
    }

    fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.permits).try_acquire_owned().ok()
    }
}

fn validate_page_budget(budget: usize) -> Result<usize, String> {
    matches!(budget, 4 | 8 | 16 | 32)
        .then_some(budget)
        .ok_or_else(|| {
            format!("V13 REST leaf-block budget must be 4, 8, 16, or 32; received {budget}")
        })
}

#[derive(Default)]
struct AppMetrics {
    cheap_requests: std::sync::atomic::AtomicU64,
    search_accepted: std::sync::atomic::AtomicU64,
    search_rejected: std::sync::atomic::AtomicU64,
    search_in_flight: std::sync::atomic::AtomicU64,
}

#[derive(Clone)]
struct AppState {
    index: BorsukIndex,
    admission: SearchAdmission,
    page_budget: usize,
    metrics: Arc<AppMetrics>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchRequest {
    vector: Vec<f32>,
    #[serde(default = "default_k")]
    k: usize,
}

fn default_k() -> usize {
    10
}

#[derive(Debug, Serialize)]
struct SearchHitResponse {
    id: String,
    distance: f32,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    hits: Vec<SearchHitResponse>,
    engine: String,
    pages_read: usize,
    bytes_read: u64,
    elapsed_ms: u64,
}

#[derive(Debug, Serialize)]
struct ErrorResponse<'a> {
    error: &'a str,
}

#[derive(Debug, Serialize)]
struct MetricsResponse {
    cheap_requests: u64,
    search_accepted: u64,
    search_rejected: u64,
    search_in_flight: u64,
    borsuk_search_waiting: usize,
    borsuk_leaf_reads_in_flight: usize,
    borsuk_search_rejected: u64,
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/item/{id}", get(item))
        .route("/api/search", post(search))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    state
        .metrics
        .cheap_requests
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Json(serde_json::json!({"status": "ok"}))
}

async fn item(State(state): State<AppState>, Path(id): Path<String>) -> Json<serde_json::Value> {
    state
        .metrics
        .cheap_requests
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Json(serde_json::json!({"id": id, "kind": "benchmark-item", "active": true}))
}

async fn metrics(State(state): State<AppState>) -> Json<MetricsResponse> {
    use std::sync::atomic::Ordering::Relaxed;
    let flow = state.index.flow_control_stats();
    Json(MetricsResponse {
        cheap_requests: state.metrics.cheap_requests.load(Relaxed),
        search_accepted: state.metrics.search_accepted.load(Relaxed),
        search_rejected: state.metrics.search_rejected.load(Relaxed),
        search_in_flight: state.metrics.search_in_flight.load(Relaxed),
        borsuk_search_waiting: flow.searches.waiting,
        borsuk_leaf_reads_in_flight: flow.leaf_reads.active,
        borsuk_search_rejected: flow.searches.rejected,
    })
}

async fn search(State(state): State<AppState>, Json(request): Json<SearchRequest>) -> Response {
    use std::sync::atomic::Ordering::Relaxed;
    if request.k == 0
        || request.vector.is_empty()
        || request.vector.iter().any(|value| !value.is_finite())
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid query",
            }),
        )
            .into_response();
    }
    let Some(permit) = state.admission.try_acquire() else {
        state.metrics.search_rejected.fetch_add(1, Relaxed);
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorResponse {
                error: "search capacity exhausted",
            }),
        )
            .into_response();
    };
    state.metrics.search_accepted.fetch_add(1, Relaxed);
    state.metrics.search_in_flight.fetch_add(1, Relaxed);
    let index = state.index.clone();
    let page_budget = state.page_budget;
    let metrics = Arc::clone(&state.metrics);
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let result = index.search_with_report(
            &request.vector,
            SearchOptions::approx(request.k, LeafMode::SrhtPqScan).with_max_segments(page_budget),
        );
        metrics.search_in_flight.fetch_sub(1, Relaxed);
        result
    })
    .await;

    match result {
        Ok(Ok(report)) => Json(SearchResponse {
            hits: report
                .hits
                .into_iter()
                .map(|hit| SearchHitResponse {
                    id: String::from_utf8_lossy(hit.id.as_bytes()).into_owned(),
                    distance: hit.distance,
                })
                .collect(),
            engine: report.leaf_mode,
            pages_read: report.global_leaf_pages_read,
            bytes_read: report.bytes_read,
            elapsed_ms: report.elapsed_ms,
        })
        .into_response(),
        Ok(Err(error @ BorsukError::Overloaded { .. })) => {
            state.metrics.search_rejected.fetch_add(1, Relaxed);
            (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({"error": error.to_string()})),
            )
                .into_response()
        }
        Ok(Err(error)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
    }
}

fn env_usize(name: &str, default: usize) -> Result<usize, String> {
    env::var(name).ok().map_or(Ok(default), |value| {
        value
            .parse::<usize>()
            .map_err(|_| format!("{name} must be an integer"))
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let process_defaults = ProcessLimits::default();
    borsuk::configure_process(ProcessLimits {
        cpu_threads: env_usize("BORSUK_REST_CPU_THREADS", process_defaults.cpu_threads)?,
        io_threads: env_usize("BORSUK_REST_IO_THREADS", process_defaults.io_threads)?,
        s3_get_concurrency: env_usize(
            "BORSUK_REST_S3_GET_CONCURRENCY",
            process_defaults.s3_get_concurrency,
        )?,
    })?;
    let uri = env::var("BORSUK_REST_INDEX_URI")?;
    let cache_dir = PathBuf::from(env::var("BORSUK_REST_CACHE_DIR")?);
    let listen = env::var("BORSUK_REST_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_owned())
        .parse::<SocketAddr>()?;
    let page_budget = validate_page_budget(env_usize("BORSUK_REST_PAGE_BUDGET", 32)?)?;
    let search_limit = env_usize("BORSUK_REST_SEARCH_ADMISSION", 2)?;
    let leaf_read_width = env_usize("BORSUK_REST_LEAF_READ_WIDTH", 32)?;
    let max_inflight_leaf_reads = env_usize("BORSUK_REST_MAX_INFLIGHT_LEAF_READS", 48)?;
    let index = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            cache_dir: Some(cache_dir),
            cache_max_bytes: Some(1024 * 1024 * 1024),
            ram_budget_bytes: Some(2 * 1024 * 1024 * 1024),
            max_active_searches: search_limit,
            max_waiting_searches: 0,
            leaf_read_width,
            max_inflight_leaf_reads,
            ..OpenOptions::default()
        },
    )?;
    index.prepare_serving_metadata()?;
    let state = AppState {
        index,
        admission: SearchAdmission::new(search_limit)?,
        page_budget,
        metrics: Arc::new(AppMetrics::default()),
    };
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(listener, router(state)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use borsuk::{IndexConfig, VectorMetric};
    use tower::ServiceExt;

    use super::{AppMetrics, AppState, SearchAdmission, router, validate_page_budget};

    #[test]
    fn rest_search_accepts_only_bounded_v13_block_budgets() {
        for budget in [4, 8, 16, 32] {
            assert_eq!(validate_page_budget(budget).unwrap(), budget);
        }
        for budget in [0, 1, 3, 5, 17, 33, 128] {
            let error = validate_page_budget(budget)
                .expect_err("unsupported page budget reached the REST search path");
            assert!(error.contains("4, 8, 16, or 32"), "{error}");
        }
    }

    #[tokio::test]
    async fn saturated_search_admission_rejects_without_queueing() {
        let admission = SearchAdmission::new(1).unwrap();
        let _held = admission
            .try_acquire()
            .expect("first search should be admitted");
        assert!(
            admission.try_acquire().is_none(),
            "saturated REST admission queued or over-admitted a search"
        );
    }

    #[tokio::test]
    async fn cheap_endpoint_stays_available_while_search_is_saturated() {
        let directory = tempfile::tempdir().unwrap();
        let index = borsuk::BorsukIndex::create(IndexConfig {
            uri: directory.path().to_string_lossy().into_owned(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let admission = SearchAdmission::new(1).unwrap();
        let _held = admission.try_acquire().unwrap();
        let app = router(AppState {
            index,
            admission,
            page_budget: 4,
            metrics: std::sync::Arc::new(AppMetrics::default()),
        });

        let health = app
            .clone()
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let search = app
            .oneshot(
                Request::post("/api/search")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[0.0,0.0],"k":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(search.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
