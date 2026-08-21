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
use borsuk::{BorsukError, BorsukIndex, OpenOptions, ProcessLimits, SearchOptions};
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
    matches!(budget, 4 | 8 | 16 | 32 | 64)
        .then_some(budget)
        .ok_or_else(|| {
            format!("V13 REST leaf-block budget must be 4, 8, 16, 32, or 64; received {budget}")
        })
}

fn validate_exact_candidates(candidates: usize) -> Result<usize, String> {
    (10..=2_048)
        .contains(&candidates)
        .then_some(candidates)
        .ok_or_else(|| {
            format!("V15 REST exact candidate budget must be in 10..=2048; received {candidates}")
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
    exact_candidates: usize,
    ram_budget_bytes: u64,
    disk_cache_bytes: u64,
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
    backing_bytes_read: u64,
    backing_reads: u64,
    disk_cache_bytes_read: u64,
    disk_cache_reads: u64,
    elapsed_ms: u64,
    records_scored: usize,
    global_leaf_code_pages_read: usize,
    global_leaf_code_requests: usize,
    global_leaf_exact_requests: usize,
    global_leaf_exact_cells: usize,
    global_leaf_exact_cards: usize,
    global_leaf_deepest_winning_card_rank: usize,
    global_leaf_exact_groups: usize,
    global_leaf_exact_selected_bytes: u64,
    global_leaf_exact_speculative_bytes: u64,
    global_leaf_exact_scores: usize,
    global_leaf_waves: usize,
    global_base_approximate_us: u64,
    global_base_head_admission_us: u64,
    global_base_head_fetch_us: u64,
    global_base_head_decode_admission_us: u64,
    global_base_head_decode_us: u64,
    global_base_exact_admission_us: u64,
    global_base_exact_fetch_us: u64,
    global_base_exact_cpu_us: u64,
    global_base_exact_rerank_us: u64,
    transient_bytes: u64,
    transient_capacity_bytes: u64,
    transient_peak_bytes: u64,
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
    borsuk_search_wait_count: u64,
    borsuk_search_wait_micros: u64,
    borsuk_leaf_reads_in_flight: usize,
    borsuk_leaf_read_peak_active: usize,
    borsuk_leaf_read_wait_count: u64,
    borsuk_leaf_read_wait_micros: u64,
    borsuk_decode_rank_capacity: usize,
    borsuk_decode_rank_active: usize,
    borsuk_decode_rank_waiting: usize,
    borsuk_decode_rank_peak_active: usize,
    borsuk_decode_rank_wait_count: u64,
    borsuk_decode_rank_wait_micros: u64,
    borsuk_transient_waiting: usize,
    borsuk_transient_admitted: u64,
    borsuk_transient_wait_count: u64,
    borsuk_transient_wait_micros: u64,
    borsuk_transient_capacity_bytes: u64,
    borsuk_transient_peak_bytes: u64,
    borsuk_search_rejected: u64,
    borsuk_search_capacity: usize,
    borsuk_leaf_read_width: usize,
    borsuk_leaf_read_capacity: usize,
    borsuk_exact_read_max_physical_amplification: u64,
    borsuk_cpu_threads: usize,
    borsuk_io_threads: usize,
    borsuk_s3_get_concurrency: usize,
    borsuk_page_budget: usize,
    borsuk_exact_candidates: usize,
    borsuk_global_scan_codec: String,
    borsuk_serving_leaf_mode: String,
    borsuk_ram_budget_bytes: u64,
    borsuk_disk_cache_bytes: u64,
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
        borsuk_search_wait_count: flow.searches.wait_count,
        borsuk_search_wait_micros: flow.searches.wait_micros,
        borsuk_leaf_reads_in_flight: flow.leaf_reads.active,
        borsuk_leaf_read_peak_active: flow.leaf_reads.peak_active,
        borsuk_leaf_read_wait_count: flow.leaf_reads.wait_count,
        borsuk_leaf_read_wait_micros: flow.leaf_reads.wait_micros,
        borsuk_decode_rank_capacity: flow.decode_rank.capacity,
        borsuk_decode_rank_active: flow.decode_rank.active,
        borsuk_decode_rank_waiting: flow.decode_rank.waiting,
        borsuk_decode_rank_peak_active: flow.decode_rank.peak_active,
        borsuk_decode_rank_wait_count: flow.decode_rank.wait_count,
        borsuk_decode_rank_wait_micros: flow.decode_rank.wait_micros,
        borsuk_transient_waiting: flow.transient.waiting,
        borsuk_transient_admitted: flow.transient.admitted,
        borsuk_transient_wait_count: flow.transient.wait_count,
        borsuk_transient_wait_micros: flow.transient.wait_micros,
        borsuk_transient_capacity_bytes: flow.transient.capacity_bytes,
        borsuk_transient_peak_bytes: flow.transient.peak_bytes,
        borsuk_search_rejected: flow.searches.rejected,
        borsuk_search_capacity: flow.searches.capacity,
        borsuk_leaf_read_width: flow.leaf_read_width,
        borsuk_leaf_read_capacity: flow.leaf_reads.capacity,
        borsuk_exact_read_max_physical_amplification: flow.exact_read_max_physical_amplification,
        borsuk_cpu_threads: borsuk::configured_cpu_threads(),
        borsuk_io_threads: borsuk::configured_io_threads(),
        borsuk_s3_get_concurrency: borsuk::configured_backing_get_concurrency(),
        borsuk_page_budget: state.page_budget,
        borsuk_exact_candidates: state.exact_candidates,
        borsuk_global_scan_codec: state.index.build_config().global_scan_codec.to_string(),
        borsuk_serving_leaf_mode: state
            .index
            .build_config()
            .global_scan_codec
            .leaf_mode()
            .to_string(),
        borsuk_ram_budget_bytes: state.ram_budget_bytes,
        borsuk_disk_cache_bytes: state.disk_cache_bytes,
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
    let exact_candidates = state.exact_candidates;
    let metrics = Arc::clone(&state.metrics);
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let leaf_mode = index.build_config().global_scan_codec.leaf_mode();
        let result = index.search_with_report(
            &request.vector,
            SearchOptions::approx(request.k, leaf_mode)
                .with_max_segments(page_budget)
                .with_max_candidates_per_segment(exact_candidates),
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
            backing_bytes_read: report.backing_bytes_read,
            backing_reads: report.backing_reads,
            disk_cache_bytes_read: report.disk_cache_bytes_read,
            disk_cache_reads: report.disk_cache_reads,
            elapsed_ms: report.elapsed_ms,
            records_scored: report.records_scored,
            global_leaf_code_pages_read: report.global_leaf_code_pages_read,
            global_leaf_code_requests: report.global_leaf_code_requests,
            global_leaf_exact_requests: report.global_leaf_exact_requests,
            global_leaf_exact_cells: report.global_leaf_exact_cells,
            global_leaf_exact_cards: report.global_leaf_exact_cards,
            global_leaf_deepest_winning_card_rank: report.global_leaf_deepest_winning_card_rank,
            global_leaf_exact_groups: report.global_leaf_exact_groups,
            global_leaf_exact_selected_bytes: report.global_leaf_exact_selected_bytes,
            global_leaf_exact_speculative_bytes: report.global_leaf_exact_speculative_bytes,
            global_leaf_exact_scores: report.global_leaf_exact_scores,
            global_leaf_waves: report.global_leaf_waves,
            global_base_approximate_us: report.global_base_approximate_us,
            global_base_head_admission_us: report.global_base_head_admission_us,
            global_base_head_fetch_us: report.global_base_head_fetch_us,
            global_base_head_decode_admission_us: report.global_base_head_decode_admission_us,
            global_base_head_decode_us: report.global_base_head_decode_us,
            global_base_exact_admission_us: report.global_base_exact_admission_us,
            global_base_exact_fetch_us: report.global_base_exact_fetch_us,
            global_base_exact_cpu_us: report.global_base_exact_cpu_us,
            global_base_exact_rerank_us: report.global_base_exact_rerank_us,
            transient_bytes: report.transient_bytes,
            transient_capacity_bytes: report.transient_capacity_bytes,
            transient_peak_bytes: report.transient_peak_bytes,
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
    let ram_budget_bytes = env_usize("BORSUK_REST_RAM_BUDGET_BYTES", 2 * 1024 * 1024 * 1024)?;
    let disk_cache_bytes = env_usize("BORSUK_REST_DISK_CACHE_BYTES", 1024 * 1024 * 1024)?;
    let cache_dir = if disk_cache_bytes == 0 {
        None
    } else {
        Some(PathBuf::from(env::var("BORSUK_REST_CACHE_DIR")?))
    };
    let listen = env::var("BORSUK_REST_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_owned())
        .parse::<SocketAddr>()?;
    let page_budget = validate_page_budget(env_usize("BORSUK_REST_PAGE_BUDGET", 32)?)?;
    let exact_candidates =
        validate_exact_candidates(env_usize("BORSUK_REST_EXACT_CANDIDATES", 512)?)?;
    let search_limit = env_usize("BORSUK_REST_SEARCH_ADMISSION", 2)?;
    let leaf_read_width = env_usize("BORSUK_REST_LEAF_READ_WIDTH", 32)?;
    let max_inflight_leaf_reads = env_usize("BORSUK_REST_MAX_INFLIGHT_LEAF_READS", 48)?;
    let max_parallel_decode_rank_tasks = env_usize(
        "BORSUK_REST_MAX_PARALLEL_DECODE_RANK_TASKS",
        OpenOptions::default().max_parallel_decode_rank_tasks,
    )?;
    let exact_read_max_physical_amplification = env_usize(
        "BORSUK_REST_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION",
        borsuk::DEFAULT_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION as usize,
    )? as u64;
    let index = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            cache_dir,
            cache_max_bytes: (disk_cache_bytes > 0).then_some(disk_cache_bytes as u64),
            ram_budget_bytes: Some(ram_budget_bytes as u64),
            max_active_searches: search_limit,
            max_waiting_searches: 0,
            leaf_read_width,
            max_inflight_leaf_reads,
            max_parallel_decode_rank_tasks,
            exact_read_max_physical_amplification,
            ..OpenOptions::default()
        },
    )?;
    index.prepare_serving_metadata()?;
    let state = AppState {
        index,
        admission: SearchAdmission::new(search_limit)?,
        page_budget,
        exact_candidates,
        ram_budget_bytes: ram_budget_bytes as u64,
        disk_cache_bytes: disk_cache_bytes as u64,
        metrics: Arc::new(AppMetrics::default()),
    };
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(listener, router(state)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use borsuk::{BuildConfig, GlobalScanCodec, IndexConfig, VectorMetric, VectorRecord};
    use tower::ServiceExt;

    use super::{AppMetrics, AppState, SearchAdmission, router, validate_page_budget};

    #[test]
    fn rest_search_accepts_only_bounded_v13_block_budgets() {
        for budget in [4, 8, 16, 32, 64] {
            assert_eq!(validate_page_budget(budget).unwrap(), budget);
        }
        for budget in [0, 1, 3, 5, 17, 33, 128] {
            let error = validate_page_budget(budget)
                .expect_err("unsupported page budget reached the REST search path");
            assert!(error.contains("4, 8, 16, 32, or 64"), "{error}");
        }
    }

    #[test]
    fn rest_search_accepts_only_bounded_exact_candidate_budgets() {
        for budget in [10, 128, 184, 256, 384, 512, 1_024, 2_048] {
            assert_eq!(super::validate_exact_candidates(budget).unwrap(), budget);
        }
        for budget in [0, 1, 9, 2_049, usize::MAX] {
            let error = super::validate_exact_candidates(budget)
                .expect_err("unsupported exact candidate budget reached REST search");
            assert!(error.contains("10..=2048"), "{error}");
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
            exact_candidates: 512,
            ram_budget_bytes: 1024,
            disk_cache_bytes: 0,
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

    #[tokio::test]
    async fn search_response_exposes_transient_admission_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let index = borsuk::BorsukIndex::create(IndexConfig {
            uri: directory.path().to_string_lossy().into_owned(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: Some(8 * 1024 * 1024),
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let app = router(AppState {
            index,
            admission: SearchAdmission::new(1).unwrap(),
            page_budget: 4,
            exact_candidates: 512,
            ram_budget_bytes: 8 * 1024 * 1024,
            disk_cache_bytes: 0,
            metrics: std::sync::Arc::new(AppMetrics::default()),
        });

        let response = app
            .oneshot(
                Request::post("/api/search")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[0.0,0.0],"k":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let used = value["transient_bytes"].as_u64().unwrap();
        let capacity = value["transient_capacity_bytes"].as_u64().unwrap();
        let peak = value["transient_peak_bytes"].as_u64().unwrap();
        for field in [
            "global_leaf_exact_cells",
            "global_leaf_exact_cards",
            "global_leaf_deepest_winning_card_rank",
            "global_leaf_exact_groups",
            "global_leaf_exact_selected_bytes",
            "global_leaf_exact_speculative_bytes",
        ] {
            assert!(
                value[field].is_u64(),
                "missing search topology field {field}"
            );
        }
        assert!(capacity > 0);
        assert!(used <= capacity);
        assert!(peak <= capacity);
    }

    #[tokio::test]
    async fn search_uses_the_immutable_index_scan_codec() {
        let directory = tempfile::tempdir().unwrap();
        let mut index = borsuk::BorsukIndex::create_with_build_config(
            IndexConfig {
                uri: directory.path().to_string_lossy().into_owned(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 64,
                ram_budget_bytes: Some(8 * 1024 * 1024),
                text: false,
                named_vectors: Default::default(),
            },
            BuildConfig {
                global_scan_codec: GlobalScanCodec::FastTurboQuantProd,
                ..BuildConfig::default()
            },
        )
        .unwrap();
        index
            .add(
                (0..512)
                    .map(|ordinal| {
                        VectorRecord::new(
                            format!("row-{ordinal}"),
                            vec![ordinal as f32, (ordinal % 17) as f32],
                        )
                    })
                    .collect(),
            )
            .unwrap();
        index.finish_bulk_load().unwrap();
        index.prepare_serving_metadata().unwrap();
        let app = router(AppState {
            index,
            admission: SearchAdmission::new(1).unwrap(),
            page_budget: 4,
            exact_candidates: 512,
            ram_budget_bytes: 8 * 1024 * 1024,
            disk_cache_bytes: 0,
            metrics: std::sync::Arc::new(AppMetrics::default()),
        });

        let response = app
            .oneshot(
                Request::post("/api/search")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[0.0,0.0],"k":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["engine"], "bounded-cell-card-v20");
        assert!(value["global_leaf_code_pages_read"].as_u64().unwrap() > 0);
        assert!(value["global_leaf_exact_requests"].as_u64().unwrap() > 0);
        assert!(value["global_leaf_exact_selected_bytes"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn metrics_attest_effective_process_and_handle_limits() {
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
        let app = router(AppState {
            index,
            admission: SearchAdmission::new(1).unwrap(),
            page_budget: 4,
            exact_candidates: 512,
            ram_budget_bytes: 1024,
            disk_cache_bytes: 0,
            metrics: std::sync::Arc::new(AppMetrics::default()),
        });
        let response = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["borsuk_search_capacity"], 8);
        assert_eq!(value["borsuk_search_wait_count"], 0);
        assert_eq!(value["borsuk_search_wait_micros"], 0);
        assert_eq!(value["borsuk_leaf_read_width"], 32);
        assert_eq!(value["borsuk_leaf_read_capacity"], 48);
        assert_eq!(value["borsuk_leaf_read_peak_active"], 0);
        assert_eq!(value["borsuk_leaf_read_wait_count"], 0);
        assert_eq!(value["borsuk_leaf_read_wait_micros"], 0);
        assert_eq!(value["borsuk_decode_rank_capacity"], 1);
        assert_eq!(value["borsuk_decode_rank_active"], 0);
        assert_eq!(value["borsuk_decode_rank_waiting"], 0);
        assert_eq!(value["borsuk_decode_rank_peak_active"], 0);
        assert_eq!(value["borsuk_decode_rank_wait_count"], 0);
        assert_eq!(value["borsuk_decode_rank_wait_micros"], 0);
        assert_eq!(value["borsuk_transient_waiting"], 0);
        assert_eq!(value["borsuk_transient_admitted"], 0);
        assert_eq!(value["borsuk_transient_wait_count"], 0);
        assert_eq!(value["borsuk_transient_wait_micros"], 0);
        // This fixture deliberately opens without a RAM budget, so no weighted
        // transient gate exists. The search-response test above exercises and
        // attests the nonzero-capacity path.
        assert_eq!(value["borsuk_transient_capacity_bytes"], 0);
        assert_eq!(value["borsuk_transient_peak_bytes"], 0);
        assert_eq!(value["borsuk_exact_read_max_physical_amplification"], 1);
        assert_eq!(value["borsuk_page_budget"], 4);
        assert_eq!(value["borsuk_exact_candidates"], 512);
        assert_eq!(value["borsuk_global_scan_codec"], "srht-pq-scan");
        assert_eq!(value["borsuk_serving_leaf_mode"], "srht-pq-scan");
        assert_eq!(value["borsuk_ram_budget_bytes"], 1024);
        assert_eq!(value["borsuk_disk_cache_bytes"], 0);
        assert_eq!(
            value["borsuk_cpu_threads"],
            borsuk::configured_cpu_threads()
        );
        assert_eq!(value["borsuk_io_threads"], borsuk::configured_io_threads());
        assert_eq!(
            value["borsuk_s3_get_concurrency"],
            borsuk::configured_backing_get_concurrency()
        );
    }
}
