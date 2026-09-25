//! Network serving gate for the frozen V219 resident graph.

use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{BufReader, Read},
    net::SocketAddr,
    path::Path,
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
    pq64_nominee::Pq64Router,
    resident_fp16_tier::ResidentFp16Tier,
    resident_vector_graph::{GraphSearchWorkspace, ResidentPqCosineGraph, ResidentVectorGraph},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

const ROWS: usize = 1_000_000;
const DIMS: usize = 768;
const WORKERS: usize = 8;
const SOURCE: &str = "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86";

struct Loaded {
    graph: ResidentVectorGraph,
    plane: ResidentFp16Tier,
    pq: Pq64Router,
    old_for_new: Vec<usize>,
}

struct Work {
    query: Vec<f32>,
    reply: oneshot::Sender<Result<(Vec<u64>, usize), String>>,
}

struct AppState {
    sender: SyncSender<Work>,
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

fn digest(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut input = BufReader::new(File::open(path)?);
    let mut hash = Sha256::new();
    let mut block = [0u8; 1024 * 1024];
    loop {
        let n = input.read(&mut block)?;
        if n == 0 {
            break;
        }
        hash.update(&block[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn load(args: &[String]) -> Result<Loaded, Box<dyn Error>> {
    let prep: Value = serde_json::from_slice(&fs::read(&args[1])?)?;
    let build: Value = serde_json::from_slice(&fs::read(&args[2])?)?;
    if prep["schema"] != "borsuk-v217-graph-1m-preparation-v1"
        || build["schema"] != "borsuk-v219-reachable-graph-build-1m-v1"
        || prep["source_sha256"] != SOURCE
        || build["source_sha256"] != SOURCE
        || prep["plane_sha256"] != build["plane_sha256"]
        || prep["generation"] != build["generation"]
        || prep["plane_sha256"].as_str() != Some(digest(Path::new(&args[3]))?.as_str())
        || build["graph_sha256"].as_str() != Some(digest(Path::new(&args[4]))?.as_str())
        || prep["map_sha256"].as_str() != Some(digest(Path::new(&args[5]))?.as_str())
        || prep["books_sha256"].as_str() != Some(digest(Path::new(&args[6]))?.as_str())
        || prep["codes_sha256"].as_str() != Some(digest(Path::new(&args[7]))?.as_str())
    {
        return Err("V219 serving artifact identity differs".into());
    }
    let plane = ResidentFp16Tier::open_authenticated(
        Path::new(&args[3]),
        prep["plane_sha256"].as_str().ok_or("plane SHA missing")?,
        SOURCE,
        ROWS as u64,
        DIMS,
        prep["generation"].as_u64().ok_or("generation missing")?,
        2_000_000_000,
    )?;
    let graph = ResidentVectorGraph::open_authenticated(
        Path::new(&args[4]),
        build["graph_sha256"].as_str().ok_or("graph SHA missing")?,
        &plane,
    )?;
    let structure = graph.structural_stats();
    if structure.reachable != ROWS || structure.min_indegree < 4 {
        return Err("V219 graph structure differs".into());
    }
    let mapping = fs::read(&args[5])?;
    if mapping.len() != ROWS * 4 {
        return Err("V219 row map length differs".into());
    }
    let old_for_new = mapping
        .chunks_exact(4)
        .map(|word| u32::from_le_bytes(word.try_into().unwrap()) as usize)
        .collect::<Vec<_>>();
    let raw_books = fs::read(&args[6])?;
    if raw_books.len() != 64 * 256 * 12 * 4 {
        return Err("V219 PQ books length differs".into());
    }
    let books = raw_books
        .chunks_exact(4)
        .map(|word| f32::from_le_bytes(word.try_into().unwrap()))
        .collect::<Vec<_>>();
    let pq = Pq64Router::new(
        ROWS,
        DIMS,
        256,
        1,
        vec![0.0; ROWS.div_ceil(256) * DIMS],
        books,
        fs::read(&args[7])?,
    )
    .map_err(|error| format!("V219 PQ: {error:?}"))?;
    Ok(Loaded {
        graph,
        plane,
        pq,
        old_for_new,
    })
}

fn workers(loaded: Arc<Loaded>) -> Result<Arc<AppState>, Box<dyn Error>> {
    let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();
    let (sender, receiver) = mpsc::sync_channel::<Work>(WORKERS);
    let receiver = Arc::new(Mutex::new(receiver));
    for _ in 0..WORKERS {
        let receiver = Arc::clone(&receiver);
        let loaded = Arc::clone(&loaded);
        let ready = ready_tx.clone();
        std::thread::spawn(move || {
            let run = || -> Result<(), String> {
                let cosine = loaded
                    .pq
                    .cosine_view()
                    .map_err(|error| format!("{error:?}"))?;
                let bound = ResidentPqCosineGraph::bind(
                    &loaded.graph,
                    &loaded.plane,
                    &cosine,
                    &loaded.old_for_new,
                )
                .map_err(|error| error.to_string())?;
                let mut workspace =
                    GraphSearchWorkspace::new(ROWS).map_err(|error| error.to_string())?;
                ready.send(Ok(())).map_err(|error| error.to_string())?;
                loop {
                    let work = match receiver.lock().map_err(|error| error.to_string())?.recv() {
                        Ok(work) => work,
                        Err(_) => break,
                    };
                    let result = bound
                        .search(&work.query, 100, 4096, 4096, &mut workspace)
                        .map_err(|error| error.to_string());
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
    Ok(Arc::new(AppState { sender }))
}

fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/health",
            get(|| async { Json(serde_json::json!({"status":"ok"})) }),
        )
        .route("/search", post(search))
        .with_state(state)
}

async fn search(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, StatusCode> {
    if request.k != 100
        || request.query.len() != DIMS
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
    if args.len() != 9 {
        return Err("usage: v220_graph_http PREP BUILD PLANE GRAPH MAP BOOKS CODES LISTEN".into());
    }
    let listen: SocketAddr = args[8].parse()?;
    let state = workers(Arc::new(load(&args)?))?;
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
        let state = Arc::new(AppState { sender });
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
