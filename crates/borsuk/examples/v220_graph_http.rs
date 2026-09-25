//! Network serving gate for the frozen V219 resident graph.

use std::{
    env,
    error::Error,
    fs,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        mpsc::{self, SyncSender, TrySendError},
    },
};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use borsuk::{
    resident_graph_generation::ResidentGraphGeneration,
    resident_graph_overlay::{ResidentGraphOverlay, ResidentMutation},
    resident_vector_graph::GraphSearchWorkspace,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

const WORKERS: usize = 8;

struct Work {
    query: Vec<f32>,
    reply: oneshot::Sender<Result<(Vec<u64>, usize), String>>,
}

struct AppState {
    sender: SyncSender<Work>,
    dimensions: usize,
    delta_rows: usize,
    overlay_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchRequest {
    query: Vec<f32>,
    k: usize,
}

#[derive(Serialize)]
struct SearchResponse {
    ids: Vec<u64>,
    base_visits: usize,
    vector_body_gets: u8,
}

fn workers(
    loaded: Arc<ResidentGraphGeneration>,
    overlay: Option<Arc<ResidentGraphOverlay>>,
) -> Result<Arc<AppState>, Box<dyn Error>> {
    let delta_rows = overlay.as_ref().map_or(0, |_| loaded.rows() / 100);
    let overlay_bytes = overlay.as_ref().map_or(0, |value| value.resident_bytes());
    let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();
    let (sender, receiver) = mpsc::sync_channel::<Work>(WORKERS);
    let receiver = Arc::new(Mutex::new(receiver));
    for _ in 0..WORKERS {
        let receiver = Arc::clone(&receiver);
        let loaded = Arc::clone(&loaded);
        let overlay = overlay.clone();
        let ready = ready_tx.clone();
        std::thread::spawn(move || {
            let run = || -> Result<(), String> {
                let cosine = loaded.cosine_view().map_err(|error| error.to_string())?;
                let bound = loaded.bind(&cosine).map_err(|error| error.to_string())?;
                let delta = overlay
                    .as_ref()
                    .map(|value| value.bind(&cosine))
                    .transpose()
                    .map_err(|error| error.to_string())?;
                let mut workspace =
                    GraphSearchWorkspace::new(loaded.rows()).map_err(|error| error.to_string())?;
                ready.send(Ok(())).map_err(|error| error.to_string())?;
                loop {
                    let work = match receiver.lock().map_err(|error| error.to_string())?.recv() {
                        Ok(work) => work,
                        Err(_) => break,
                    };
                    let result = if let Some(delta) = &delta {
                        delta
                            .search(&work.query, 100, 4096, 4096, &mut workspace)
                            .map(|(ids, stats)| (ids, stats.base_visits))
                            .map_err(|error| error.to_string())
                    } else {
                        bound
                            .search(&work.query, 100, 4096, 4096, &mut workspace)
                            .map_err(|error| error.to_string())
                    };
                    let _ = work.reply.send(result);
                }
                Ok(())
            };
            if let Err(error) = run() {
                let _ = ready.send(Err(error));
            }
        });
    }
    drop(ready_tx);
    for _ in 0..WORKERS {
        ready_rx.recv()??;
    }
    Ok(Arc::new(AppState {
        sender,
        dimensions: loaded.dimensions(),
        delta_rows,
        overlay_bytes,
    }))
}

fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/health",
            get(|State(state): State<Arc<AppState>>| async move {
                Json(
                    serde_json::json!({"status":"ok","delta_rows":state.delta_rows,
                    "overlay_resident_bytes":state.overlay_bytes}),
                )
            }),
        )
        .route("/search", post(search))
        .with_state(state)
}

async fn search(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, StatusCode> {
    if request.k != 100
        || request.query.len() != state.dimensions
        || request.query.iter().any(|value| !value.is_finite())
        || !request.query.iter().any(|value| *value != 0.0)
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let (reply, result) = oneshot::channel();
    let work = Work {
        query: request.query,
        reply,
    };
    match state.sender.try_send(work) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => return Err(StatusCode::SERVICE_UNAVAILABLE),
        Err(TrySendError::Disconnected(_)) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
    let (ids, base_visits) = result
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(SearchResponse {
        ids,
        base_visits,
        vector_body_gets: 0,
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    if !matches!(args.len(), 6 | 7 | 8) {
        return Err(
            "usage: v220_graph_http ROOT_JSON TRUSTED_SHA DIRECTORY MAX_RESIDENT_BYTES LISTEN [SAME_VECTOR_UPSERT_STRIDE [decoded]]"
                .into(),
        );
    }
    if args.len() == 8 && args[7] != "decoded" {
        return Err("unsupported mutation representation".into());
    }
    let listen: SocketAddr = args[5].parse()?;
    let loaded = ResidentGraphGeneration::open_local_authenticated(
        &fs::read(&args[1])?,
        &args[2],
        std::path::Path::new(&args[3]),
        args[4].parse()?,
        WORKERS,
    )?;
    let loaded = Arc::new(loaded);
    let overlay = if let Some(stride) = args.get(6) {
        let stride: usize = stride.parse()?;
        if stride != 100 || loaded.rows() != 1_000_000 || loaded.dimensions() != 768 {
            return Err("unsupported frozen mutation geometry".into());
        }
        let mutations = (0..loaded.rows())
            .step_by(stride)
            .map(|ordinal| {
                Ok(ResidentMutation {
                    id: loaded.source_id(ordinal)?,
                    vector: Some(loaded.vector_f32(ordinal)?),
                })
            })
            .collect::<Result<Vec<_>, borsuk::resident_graph_generation::ResidentGraphGenerationError>>()?;
        let mut overlay = ResidentGraphOverlay::new(
            Arc::clone(&loaded),
            mutations,
            32 * 1024 * 1024,
        )?;
        if args.len() == 8 {
            overlay = overlay.with_decoded_delta(64 * 1024 * 1024)?;
        }
        Some(Arc::new(overlay))
    } else {
        None
    };
    let state = workers(loaded, overlay)?;
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(listener, router(state)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{AppState, Work, router};
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use std::sync::{Arc, mpsc::sync_channel};
    use tower::ServiceExt;

    #[tokio::test]
    async fn http_search_returns_worker_ids_and_rejects_wrong_k() {
        let (sender, receiver) = sync_channel(1);
        std::thread::spawn(move || {
            let work: Work = receiver.recv().unwrap();
            work.reply.send(Ok((vec![42], 3))).unwrap();
        });
        let state = Arc::new(AppState {
            sender,
            dimensions: 768,
            delta_rows: 0,
            overlay_bytes: 0,
        });
        let app = router(state);
        let request = Request::builder()
            .method("POST")
            .uri("/search")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"query":vec![1.0f32;768],"k":100}).to_string(),
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["ids"],
            serde_json::json!([42])
        );
        let wrong = Request::builder()
            .method("POST")
            .uri("/search")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"query":vec![1.0f32;768],"k":99}).to_string(),
            ))
            .unwrap();
        assert_eq!(
            app.oneshot(wrong).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
    }
}
