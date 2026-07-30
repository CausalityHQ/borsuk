#![allow(missing_docs)]

//! Shared-qrels dense/sparse/text retrieval benchmark.
//!
//! The companion `scripts/prepare_hybrid_dataset.py` writes a streaming binary
//! contract. `build` creates one index containing all three representations;
//! `query` runs any subset of the seven unimodal/hybrid query modes against the
//! same IDs and relevance judgments.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error,
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use borsuk::{
    BorsukIndex, BuildConfig, Fusion, GlobalScanCodec, HybridOptions, HybridQuery, IndexConfig,
    OpenOptions, SearchOptions, VectorElementType, VectorKind, VectorMetric, VectorRecord,
    VectorSpec, recommended_segment_max_vectors,
};
use serde::Deserialize;

type BenchResult<T> = Result<T, Box<dyn Error>>;

const K_DEFAULT: usize = 10;
const SPARSE_NAME: &str = "lexical";
const HYBRID_BUILD_HEADER: &str = "dataset,split,documents,dense_backend,dense_dimensions,dense_element_type,sparse_backend,sparse_dimensions,sparse_element_type,scan_codec,segment_max_vectors,batch_size,ingest_ms,finish_ms,total_ms,publication_vectors,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes";
const HYBRID_QUERY_HEADER: &str = "dataset,scan_codec,k,candidate_depth,max_candidates,max_segments,fusion,cache_profile,target_hot_query_fraction,client_concurrency,query_class,mode,repetition,query_position,query_id,latency_ms,ndcg_at_10,recall_at_10,precision_at_10,mrr_at_10,hits,observed_cache_tier,observed_cached_byte_fraction,decoded_cache_hits,decoded_cache_bytes_read,disk_cache_reads,disk_cache_bytes_read,backing_reads,backing_bytes_read,bytes_read,network_gets,segments_searched,query_seed,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes";
const MODES: [&str; 7] = [
    "dense",
    "sparse",
    "text",
    "dense+sparse",
    "dense+text",
    "sparse+text",
    "dense+sparse+text",
];

#[derive(Debug, Deserialize)]
struct Manifest {
    schema_version: u32,
    dataset: String,
    split: String,
    documents: usize,
    queries: usize,
    qrels: usize,
    dense: DenseManifest,
    sparse: SparseManifest,
    qrels_semantics: String,
}

#[derive(Debug, Deserialize)]
struct DenseManifest {
    dimensions: usize,
    publication_valid: bool,
    backend: String,
}

#[derive(Debug, Deserialize)]
struct SparseManifest {
    dimensions: usize,
    backend: String,
}

#[derive(Debug, Deserialize)]
struct TextRow {
    id: String,
    text: String,
}

#[derive(Debug, Clone)]
struct SparseRow {
    indices: Vec<u32>,
    values: Vec<f32>,
}

struct RowPayloadReader {
    rows: io::Lines<BufReader<File>>,
    dense: BufReader<File>,
    sparse_offsets: BufReader<File>,
    sparse_indices: BufReader<File>,
    sparse_values: BufReader<File>,
    dimensions: usize,
    previous_offset: u64,
    rows_read: usize,
}

impl RowPayloadReader {
    fn open(dataset: &Path, prefix: &str, dimensions: usize) -> BenchResult<Self> {
        let mut sparse_offsets = BufReader::new(File::open(
            dataset.join(format!("{prefix}.sparse.offsets.u64")),
        )?);
        let previous_offset = read_u64(&mut sparse_offsets)?;
        if previous_offset != 0 {
            return Err(invalid_input("the first sparse offset must be zero").into());
        }
        Ok(Self {
            rows: BufReader::new(File::open(dataset.join(format!("{prefix}.jsonl")))?).lines(),
            dense: BufReader::new(File::open(dataset.join(format!("{prefix}.dense.f32")))?),
            sparse_offsets,
            sparse_indices: BufReader::new(File::open(
                dataset.join(format!("{prefix}.sparse.indices.u32")),
            )?),
            sparse_values: BufReader::new(File::open(
                dataset.join(format!("{prefix}.sparse.values.f32")),
            )?),
            dimensions,
            previous_offset,
            rows_read: 0,
        })
    }

    fn next(&mut self) -> BenchResult<Option<(TextRow, Vec<f32>, SparseRow)>> {
        let Some(line) = self.rows.next() else {
            return Ok(None);
        };
        let row: TextRow = serde_json::from_str(&line?)?;
        let dense = read_f32_values(&mut self.dense, self.dimensions)?;
        let next_offset = read_u64(&mut self.sparse_offsets)?;
        let non_zero = next_offset
            .checked_sub(self.previous_offset)
            .ok_or_else(|| invalid_input("sparse offsets must be monotonic"))?;
        let non_zero = usize::try_from(non_zero)?;
        let indices = read_u32_values(&mut self.sparse_indices, non_zero)?;
        let values = read_f32_values(&mut self.sparse_values, non_zero)?;
        self.previous_offset = next_offset;
        self.rows_read += 1;
        Ok(Some((row, dense, SparseRow { indices, values })))
    }

    fn finish(mut self, expected_rows: usize) -> BenchResult<()> {
        if self.next()?.is_some() {
            return Err(invalid_input("dataset contains more rows than the manifest").into());
        }
        if self.rows_read != expected_rows {
            return Err(invalid_input(&format!(
                "dataset contains {} rows; manifest says {expected_rows}",
                self.rows_read
            ))
            .into());
        }
        ensure_eof(&mut self.dense, "dense payload")?;
        ensure_eof(&mut self.sparse_offsets, "sparse offsets")?;
        ensure_eof(&mut self.sparse_indices, "sparse indices")?;
        ensure_eof(&mut self.sparse_values, "sparse values")?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Mode {
    name: &'static str,
    dense: bool,
    sparse: bool,
    text: bool,
}

impl Mode {
    fn parse(name: &str) -> BenchResult<Self> {
        let canonical = name.trim().to_ascii_lowercase();
        let mode = match canonical.as_str() {
            "dense" => Self {
                name: "dense",
                dense: true,
                sparse: false,
                text: false,
            },
            "sparse" => Self {
                name: "sparse",
                dense: false,
                sparse: true,
                text: false,
            },
            "text" => Self {
                name: "text",
                dense: false,
                sparse: false,
                text: true,
            },
            "dense+sparse" => Self {
                name: "dense+sparse",
                dense: true,
                sparse: true,
                text: false,
            },
            "dense+text" => Self {
                name: "dense+text",
                dense: true,
                sparse: false,
                text: true,
            },
            "sparse+text" => Self {
                name: "sparse+text",
                dense: false,
                sparse: true,
                text: true,
            },
            "dense+sparse+text" => Self {
                name: "dense+sparse+text",
                dense: true,
                sparse: true,
                text: true,
            },
            _ => {
                return Err(invalid_input(&format!(
                    "unknown hybrid mode {name:?}; expected one of {}",
                    MODES.join(",")
                ))
                .into());
            }
        };
        Ok(mode)
    }
}

#[derive(Clone)]
struct Query {
    id: String,
    text: String,
    dense: Vec<f32>,
    sparse: SparseRow,
}

#[derive(Debug, Clone)]
struct Metrics {
    ndcg: f64,
    recall: f64,
    precision: f64,
    mrr: f64,
}

#[derive(Debug)]
struct QueryMeasurement {
    k: usize,
    candidate_depth: usize,
    max_candidates: usize,
    max_segments: usize,
    fusion: String,
    cache_profile: String,
    target_hot_query_fraction: f64,
    client_concurrency: usize,
    query_class: &'static str,
    mode: &'static str,
    repetition: usize,
    query_seed: u64,
    query_position: usize,
    query_id: String,
    latency_ms: f64,
    metrics: Metrics,
    hits: usize,
    cache_tier: String,
    observed_cached_byte_fraction: f64,
    decoded_cache_hits: usize,
    decoded_cache_bytes_read: u64,
    disk_cache_reads: u64,
    disk_cache_bytes_read: u64,
    backing_reads: u64,
    backing_bytes_read: u64,
    bytes_read: u64,
    network_gets: u64,
    segments_searched: usize,
    collection_resident_bytes: u64,
    retained_bytes: u64,
    retained_capacity_bytes: u64,
    retained_peak_bytes: u64,
    transient_bytes: u64,
    transient_capacity_bytes: u64,
    transient_peak_bytes: u64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("hybrid_retrieval_bench: {error}");
        std::process::exit(1);
    }
}

fn run() -> BenchResult<()> {
    let action = env::args()
        .nth(1)
        .ok_or_else(|| invalid_input("expected `build` or `query` argument"))?;
    let dataset = required_path("BORSUK_HYBRID_DATASET")?;
    let index_uri = required_env("BORSUK_HYBRID_INDEX_URI")?;
    let output = required_path("BORSUK_HYBRID_OUTPUT")?;
    fs::create_dir_all(&output)?;
    let manifest = load_manifest(&dataset)?;
    validate_dataset_files(&dataset, &manifest)?;
    match action.as_str() {
        "build" => build(&dataset, &index_uri, &output, &manifest),
        "query" => query(&dataset, &index_uri, &output, &manifest),
        _ => Err(invalid_input("expected `build` or `query` argument").into()),
    }
}

fn load_manifest(dataset: &Path) -> BenchResult<Manifest> {
    let manifest: Manifest =
        serde_json::from_reader(BufReader::new(File::open(dataset.join("manifest.json"))?))?;
    if manifest.schema_version != 1 {
        return Err(invalid_input(&format!(
            "unsupported hybrid schema version {}",
            manifest.schema_version
        ))
        .into());
    }
    if manifest.documents == 0
        || manifest.queries == 0
        || manifest.qrels == 0
        || manifest.dense.dimensions == 0
        || manifest.sparse.dimensions == 0
    {
        return Err(invalid_input("hybrid manifest counts and dimensions must be positive").into());
    }
    if manifest.qrels_semantics != "shared-across-all-retrieval-modes" {
        return Err(invalid_input("hybrid qrels must be shared across retrieval modes").into());
    }
    Ok(manifest)
}

fn validate_dataset_files(dataset: &Path, manifest: &Manifest) -> BenchResult<()> {
    for prefix in ["corpus", "queries"] {
        for suffix in [
            "jsonl",
            "dense.f32",
            "sparse.offsets.u64",
            "sparse.indices.u32",
            "sparse.values.f32",
        ] {
            let path = dataset.join(format!("{prefix}.{suffix}"));
            if !path.is_file() {
                return Err(invalid_input(&format!("missing {}", path.display())).into());
            }
        }
    }
    if !dataset.join("qrels.tsv").is_file() {
        return Err(invalid_input("missing qrels.tsv").into());
    }
    let corpus_dense = fs::metadata(dataset.join("corpus.dense.f32"))?.len();
    let expected = byte_size(manifest.documents, manifest.dense.dimensions, 4)?;
    if corpus_dense != expected {
        return Err(invalid_input("corpus dense file size does not match manifest").into());
    }
    let query_dense = fs::metadata(dataset.join("queries.dense.f32"))?.len();
    let expected = byte_size(manifest.queries, manifest.dense.dimensions, 4)?;
    if query_dense != expected {
        return Err(invalid_input("query dense file size does not match manifest").into());
    }
    Ok(())
}

fn build(dataset: &Path, index_uri: &str, output: &Path, manifest: &Manifest) -> BenchResult<()> {
    let codec = scan_codec()?;
    let dense_element_type = dense_element_type()?;
    let sparse_element_type = sparse_element_type()?;
    let segment_max = env_usize(
        "BORSUK_HYBRID_SEGMENT_MAX",
        recommended_segment_max_vectors(manifest.dense.dimensions),
    )?;
    let batch_size = env_usize("BORSUK_HYBRID_BATCH_SIZE", 5_000)?;
    let ram_budget_bytes = env_optional_u64("BORSUK_HYBRID_RAM_BUDGET_BYTES")?
        .or(Some(borsuk::DEFAULT_RAM_BUDGET_BYTES));
    let mut index = BorsukIndex::create_with_build_config(
        IndexConfig {
            uri: index_uri.to_string(),
            metric: VectorMetric::Cosine,
            dimensions: manifest.dense.dimensions,
            segment_max_vectors: segment_max,
            ram_budget_bytes,
            text: true,
            named_vectors: BTreeMap::from([(
                SPARSE_NAME.to_string(),
                VectorSpec {
                    dimensions: manifest.sparse.dimensions,
                    metric: VectorMetric::InnerProduct,
                    kind: VectorKind::Sparse,
                    element_type: sparse_element_type,
                },
            )]),
        },
        BuildConfig {
            vector_element_type: dense_element_type,
            global_scan_codec: codec,
            ..BuildConfig::default()
        },
    )?;

    let total_started = Instant::now();
    let ingest_started = Instant::now();
    let mut reader = RowPayloadReader::open(dataset, "corpus", manifest.dense.dimensions)?;
    let mut accepted = 0usize;
    loop {
        let mut batch = Vec::with_capacity(batch_size);
        while batch.len() < batch_size {
            let Some((row, dense, sparse)) = reader.next()? else {
                break;
            };
            let record = VectorRecord::new(row.id, dense)
                .with_named_sparse_vector(SPARSE_NAME, sparse.indices, sparse.values)?
                .with_text(row.text);
            batch.push(record);
        }
        if batch.is_empty() {
            break;
        }
        accepted += batch.len();
        index.add(batch)?;
        eprintln!(
            "hybrid build dataset={} accepted={accepted}/{}",
            manifest.dataset, manifest.documents
        );
    }
    reader.finish(manifest.documents)?;
    let ingest_ms = elapsed_ms(ingest_started);
    let finish_started = Instant::now();
    index.finish_bulk_load()?;
    let finish_ms = elapsed_ms(finish_started);
    let total_ms = elapsed_ms(total_started);
    let stats = index.stats();

    let path = output.join("hybrid_build.csv");
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(writer, "{HYBRID_BUILD_HEADER}")?;
    writeln!(
        writer,
        "{},{},{},{},{},{},{},{},{},{},{},{},{ingest_ms:.3},{finish_ms:.3},{total_ms:.3},{},{},{},{},{},{},{},{},{}",
        csv_field(&manifest.dataset),
        csv_field(&manifest.split),
        manifest.documents,
        csv_field(&manifest.dense.backend),
        manifest.dense.dimensions,
        dense_element_type,
        csv_field(&manifest.sparse.backend),
        manifest.sparse.dimensions,
        sparse_element_type,
        codec,
        segment_max,
        batch_size,
        manifest.dense.publication_valid,
        ram_budget_bytes.unwrap_or(0),
        stats.collection_resident_bytes,
        stats.retained_bytes,
        stats.retained_capacity_bytes,
        stats.retained_peak_bytes,
        stats.transient_bytes,
        stats.transient_capacity_bytes,
        stats.transient_peak_bytes,
    )?;
    writer.flush()?;
    Ok(())
}

fn query(dataset: &Path, index_uri: &str, output: &Path, manifest: &Manifest) -> BenchResult<()> {
    let codec = scan_codec()?;
    let k = env_usize("BORSUK_HYBRID_K", K_DEFAULT)?;
    let candidate_depth = env_usize("BORSUK_HYBRID_CANDIDATE_DEPTH", 100)?;
    let max_candidates = env_usize("BORSUK_HYBRID_MAX_CANDIDATES", candidate_depth)?;
    let max_segments = env_usize("BORSUK_HYBRID_MAX_SEGMENTS", 0)?;
    let repetitions = env_usize("BORSUK_HYBRID_REPETITIONS", 5)?;
    let query_limit = env_usize("BORSUK_HYBRID_QUERY_LIMIT", manifest.queries)?;
    let warmups = env_usize("BORSUK_HYBRID_WARMUPS", 0)?;
    let client_concurrency = env_usize("BORSUK_HYBRID_CLIENT_CONCURRENCY", 1)?;
    let cache_profile =
        env::var("BORSUK_HYBRID_CACHE_PROFILE").unwrap_or_else(|_| "unspecified".to_string());
    let target_hot_query_fraction = env_fraction("BORSUK_HYBRID_TARGET_HOT_FRACTION", 0.0)?;
    let query_seed = env_u64("BORSUK_HYBRID_QUERY_SEED", 0)?;
    let ram_budget_bytes = env_optional_u64("BORSUK_HYBRID_RAM_BUDGET_BYTES")?
        .or(Some(borsuk::DEFAULT_RAM_BUDGET_BYTES))
        .unwrap_or(0);
    let modes = selected_modes()?;
    let (fusion, fusion_name) = resolved_fusion()?;
    let loaded_queries = load_queries(dataset, manifest, query_limit)?;
    let queries = permuted_positions(loaded_queries.len(), query_seed)
        .into_iter()
        .map(|position| loaded_queries[position].clone())
        .collect::<Vec<_>>();
    let target_hot_queries = (target_hot_query_fraction * queries.len() as f64).ceil() as usize;
    let measured_query_ids = queries
        .iter()
        .map(|query| query.id.clone())
        .collect::<BTreeSet<_>>();
    let qrels = load_qrels(dataset, manifest, &measured_query_ids)?;
    let cache_dir = env::var_os("BORSUK_HYBRID_CACHE_DIR").map(PathBuf::from);
    let open_started = Instant::now();
    let index = BorsukIndex::open_with_options(
        index_uri,
        OpenOptions {
            cache_dir,
            cache_max_bytes: env_optional_u64("BORSUK_HYBRID_CACHE_MAX_BYTES")?,
            ram_budget_bytes: Some(ram_budget_bytes),
            resident_routing: env_bool("BORSUK_HYBRID_RESIDENT_ROUTING", false)?,
            segment_cache_max_bytes: env_optional_u64("BORSUK_HYBRID_SEGMENT_CACHE_MAX_BYTES")?,
            global_cell_graph_cache_max_bytes: env_u64("BORSUK_HYBRID_GRAPH_CACHE_MAX_BYTES", 0)?,
            tombstone_page_cache_max_bytes: env_u64(
                "BORSUK_HYBRID_TOMBSTONE_CACHE_MAX_BYTES",
                32 * 1024 * 1024,
            )?,
            bm25_stats_page_cache_max_bytes: env_u64(
                "BORSUK_HYBRID_BM25_STATS_CACHE_MAX_BYTES",
                16 * 1024 * 1024,
            )?,
            lexical_run_cache_max_bytes: env_u64(
                "BORSUK_HYBRID_LEXICAL_RUN_CACHE_MAX_BYTES",
                32 * 1024 * 1024,
            )?,
            lexical_term_page_cache_max_bytes: env_u64(
                "BORSUK_HYBRID_LEXICAL_TERM_PAGE_CACHE_MAX_BYTES",
                32 * 1024 * 1024,
            )?,
            late_interaction_batch_cache_max_bytes: env_u64(
                "BORSUK_HYBRID_LATE_INTERACTION_BATCH_CACHE_MAX_BYTES",
                borsuk::DEFAULT_LATE_INTERACTION_BATCH_CACHE_BYTES,
            )?,
            wal_tail_cache_max_bytes: env_u64(
                "BORSUK_HYBRID_WAL_TAIL_CACHE_MAX_BYTES",
                borsuk::DEFAULT_WAL_TAIL_CACHE_BYTES,
            )?,
            wal_tail_decode_max_bytes: env_u64(
                "BORSUK_HYBRID_WAL_TAIL_DECODE_MAX_BYTES",
                borsuk::DEFAULT_WAL_TAIL_DECODE_BYTES,
            )?,
            preload: false,
            max_concurrent_searches: Some(env_usize(
                "BORSUK_HYBRID_MAX_CONCURRENT_SEARCHES",
                borsuk::DEFAULT_MAX_CONCURRENT_SEARCHES,
            )?),
            max_concurrent_cell_decodes: Some(env_usize(
                "BORSUK_HYBRID_MAX_CONCURRENT_CELL_DECODES",
                borsuk::DEFAULT_MAX_CONCURRENT_CELL_DECODES,
            )?),
        },
    )?;
    let open_ms = elapsed_ms(open_started);
    let metadata_started = Instant::now();
    let metadata_segments = index.prepare_serving_metadata()?;
    let metadata_ms = elapsed_ms(metadata_started);
    write_startup(output, manifest, open_ms, metadata_ms, metadata_segments)?;

    let dense_options = dense_options(codec, candidate_depth, max_candidates, max_segments);
    if env_bool("BORSUK_HYBRID_PRIME_TARGET_HOT_SET", false)? {
        for mode in &modes {
            for query in queries.iter().take(target_hot_queries) {
                let _ = run_one_query(
                    &index,
                    query,
                    *mode,
                    k,
                    candidate_depth,
                    dense_options.clone(),
                    fusion.clone(),
                )?;
            }
        }
    }
    for mode in &modes {
        for _ in 0..warmups {
            for query in &queries {
                let _ = run_one_query(
                    &index,
                    query,
                    *mode,
                    k,
                    candidate_depth,
                    dense_options.clone(),
                    fusion.clone(),
                )?;
            }
        }
    }

    let mut measurements = Vec::with_capacity(modes.len() * repetitions * queries.len());
    for mode in &modes {
        for repetition in 0..repetitions {
            let next_query = AtomicUsize::new(0);
            let repetition_rows = Mutex::new(Vec::with_capacity(queries.len()));
            let worker_result = std::thread::scope(|scope| {
                let mut workers = Vec::new();
                for _ in 0..client_concurrency.min(queries.len().max(1)) {
                    workers.push(scope.spawn(|| {
                        loop {
                            let query_position = next_query.fetch_add(1, Ordering::Relaxed);
                            let Some(query) = queries.get(query_position) else {
                                break;
                            };
                            let started = Instant::now();
                            let report = run_one_query(
                                &index,
                                query,
                                *mode,
                                k,
                                candidate_depth,
                                dense_options.clone(),
                                fusion.clone(),
                            )
                            .map_err(|error| error.to_string())?;
                            let latency_ms = elapsed_ms(started);
                            let judged = qrels
                                .get(&query.id)
                                .ok_or_else(|| format!("query {} has no qrels", query.id))?;
                            let hit_ids = report
                                .hits
                                .iter()
                                .map(|hit| hit.id.to_string())
                                .collect::<Vec<_>>();
                            let metrics = effectiveness_at_k(&hit_ids, judged, k);
                            repetition_rows
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .push(QueryMeasurement {
                                    k,
                                    candidate_depth,
                                    max_candidates,
                                    max_segments,
                                    fusion: fusion_name.clone(),
                                    cache_profile: cache_profile.clone(),
                                    target_hot_query_fraction,
                                    client_concurrency,
                                    query_class: if query_position < target_hot_queries {
                                        "target-hot"
                                    } else {
                                        "target-outside"
                                    },
                                    mode: mode.name,
                                    repetition,
                                    query_seed,
                                    query_position,
                                    query_id: query.id.clone(),
                                    latency_ms,
                                    metrics,
                                    hits: hit_ids.len(),
                                    cache_tier: observed_cache_tier(
                                        report.decoded_cache_bytes_read,
                                        report.disk_cache_bytes_read,
                                        report.backing_bytes_read,
                                    ),
                                    observed_cached_byte_fraction: observed_cached_byte_fraction(
                                        report.decoded_cache_bytes_read,
                                        report.disk_cache_bytes_read,
                                        report.backing_bytes_read,
                                    ),
                                    decoded_cache_hits: report.decoded_cache_hits,
                                    decoded_cache_bytes_read: report.decoded_cache_bytes_read,
                                    disk_cache_reads: report.disk_cache_reads,
                                    disk_cache_bytes_read: report.disk_cache_bytes_read,
                                    backing_reads: report.backing_reads,
                                    backing_bytes_read: report.backing_bytes_read,
                                    bytes_read: report.bytes_read,
                                    network_gets: report.requests.gets,
                                    segments_searched: report.segments_searched,
                                    collection_resident_bytes: report.collection_resident_bytes,
                                    retained_bytes: report.retained_bytes,
                                    retained_capacity_bytes: report.retained_capacity_bytes,
                                    retained_peak_bytes: report.retained_peak_bytes,
                                    transient_bytes: report.transient_bytes,
                                    transient_capacity_bytes: report.transient_capacity_bytes,
                                    transient_peak_bytes: report.transient_peak_bytes,
                                });
                        }
                        Ok::<(), String>(())
                    }));
                }
                for worker in workers {
                    worker
                        .join()
                        .map_err(|_| "hybrid query worker panicked".to_string())??;
                }
                Ok::<(), String>(())
            });
            worker_result.map_err(|error| invalid_input(&error))?;
            let mut repetition_rows = repetition_rows
                .into_inner()
                .unwrap_or_else(|error| error.into_inner());
            repetition_rows.sort_by_key(|row| row.query_position);
            measurements.extend(repetition_rows);
        }
    }
    write_query_rows(output, manifest, codec, ram_budget_bytes, &measurements)?;
    write_summary(output, manifest, codec, &modes, &measurements)?;
    Ok(())
}

fn load_queries(dataset: &Path, manifest: &Manifest, limit: usize) -> BenchResult<Vec<Query>> {
    let count = limit.min(manifest.queries);
    let mut reader = RowPayloadReader::open(dataset, "queries", manifest.dense.dimensions)?;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        let (row, dense, sparse) = reader
            .next()?
            .ok_or_else(|| invalid_input("query payload ended before manifest count"))?;
        result.push(Query {
            id: row.id,
            text: row.text,
            dense,
            sparse,
        });
    }
    // A limited run intentionally leaves payload rows unread.
    if count == manifest.queries {
        reader.finish(manifest.queries)?;
    }
    Ok(result)
}

fn load_qrels(
    dataset: &Path,
    manifest: &Manifest,
    measured_query_ids: &BTreeSet<String>,
) -> BenchResult<BTreeMap<String, BTreeMap<String, i32>>> {
    let path = dataset.join("qrels.tsv");
    let mut lines = BufReader::new(File::open(path)?).lines();
    if lines.next().transpose()?.as_deref() != Some("query-id\tcorpus-id\tscore") {
        return Err(invalid_input("invalid qrels.tsv header").into());
    }
    let mut result = BTreeMap::<String, BTreeMap<String, i32>>::new();
    let mut count = 0usize;
    for line in lines {
        let line = line?;
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() != 3 {
            return Err(invalid_input("qrel row must have three fields").into());
        }
        let score = fields[2].parse::<i32>()?;
        if score > 0 {
            if measured_query_ids.contains(fields[0]) {
                result
                    .entry(fields[0].to_string())
                    .or_default()
                    .insert(fields[1].to_string(), score);
            }
            count += 1;
        }
    }
    if count != manifest.qrels {
        return Err(invalid_input("qrels count does not match manifest").into());
    }
    let missing = measured_query_ids
        .iter()
        .filter(|query_id| !result.contains_key(*query_id))
        .count();
    if missing > 0 {
        return Err(invalid_input(&format!(
            "{missing} measured queries have no positive qrels"
        ))
        .into());
    }
    Ok(result)
}

fn run_one_query(
    index: &BorsukIndex,
    query: &Query,
    mode: Mode,
    k: usize,
    candidate_depth: usize,
    dense_options: SearchOptions,
    fusion: Fusion,
) -> borsuk::Result<borsuk::SearchReport> {
    let mut hybrid = HybridQuery::new();
    if mode.dense {
        hybrid = hybrid.with_vector("", query.dense.clone());
    }
    if mode.sparse {
        hybrid = hybrid.with_named_sparse_query(
            SPARSE_NAME,
            query.sparse.indices.clone(),
            query.sparse.values.clone(),
        );
    }
    if mode.text {
        hybrid = hybrid.with_text(query.text.clone());
    }
    let mut options = HybridOptions::new(k);
    options.candidate_depth = candidate_depth.max(k);
    options.dense_options = dense_options;
    options.fusion = fusion;
    index.search_hybrid(&hybrid, options)
}

fn dense_options(
    codec: GlobalScanCodec,
    candidate_depth: usize,
    max_candidates: usize,
    max_segments: usize,
) -> SearchOptions {
    let mut options = SearchOptions::approx(candidate_depth, codec.leaf_mode())
        .with_max_candidates_per_segment(max_candidates.max(candidate_depth));
    if max_segments > 0 {
        options = options.with_max_segments(max_segments);
    }
    options
}

fn effectiveness_at_k(hits: &[String], qrels: &BTreeMap<String, i32>, k: usize) -> Metrics {
    if qrels.is_empty() {
        return Metrics {
            ndcg: 0.0,
            recall: 0.0,
            precision: 0.0,
            mrr: 0.0,
        };
    }
    let mut dcg = 0.0;
    let mut relevant = BTreeSet::new();
    let mut reciprocal_rank = 0.0;
    for (rank, id) in hits.iter().take(k).enumerate() {
        let relevance = f64::from(*qrels.get(id).unwrap_or(&0));
        if relevance > 0.0 {
            relevant.insert(id);
            if reciprocal_rank == 0.0 {
                reciprocal_rank = 1.0 / (rank + 1) as f64;
            }
            dcg += (2.0_f64.powf(relevance) - 1.0) / (rank as f64 + 2.0).log2();
        }
    }
    let mut ideal = qrels.values().copied().collect::<Vec<_>>();
    ideal.sort_unstable_by(|left, right| right.cmp(left));
    let idcg = ideal
        .iter()
        .take(k)
        .enumerate()
        .map(|(rank, relevance)| (2.0_f64.powi(*relevance) - 1.0) / (rank as f64 + 2.0).log2())
        .sum::<f64>();
    Metrics {
        ndcg: if idcg > 0.0 { dcg / idcg } else { 0.0 },
        recall: relevant.len() as f64 / qrels.len() as f64,
        precision: relevant.len() as f64 / k.max(1) as f64,
        mrr: reciprocal_rank,
    }
}

fn observed_cache_tier(decoded: u64, disk: u64, backing: u64) -> String {
    let mut tiers = Vec::new();
    if decoded > 0 {
        tiers.push("decoded");
    }
    if disk > 0 {
        tiers.push("disk");
    }
    if backing > 0 {
        tiers.push("backing");
    }
    if tiers.is_empty() {
        "none".to_string()
    } else {
        tiers.join("+")
    }
}

fn observed_cached_byte_fraction(decoded: u64, disk: u64, backing: u64) -> f64 {
    let cached = decoded.saturating_add(disk);
    let total = cached.saturating_add(backing);
    if total == 0 {
        0.0
    } else {
        cached as f64 / total as f64
    }
}

fn write_startup(
    output: &Path,
    manifest: &Manifest,
    open_ms: f64,
    metadata_ms: f64,
    metadata_segments: usize,
) -> BenchResult<()> {
    let mut writer = BufWriter::new(File::create(output.join("hybrid_startup.csv"))?);
    writeln!(
        writer,
        "dataset,open_ms,prepare_metadata_ms,metadata_segments"
    )?;
    writeln!(
        writer,
        "{},{open_ms:.3},{metadata_ms:.3},{metadata_segments}",
        csv_field(&manifest.dataset)
    )?;
    writer.flush()?;
    Ok(())
}

fn write_query_rows(
    output: &Path,
    manifest: &Manifest,
    codec: GlobalScanCodec,
    ram_budget_bytes: u64,
    rows: &[QueryMeasurement],
) -> BenchResult<()> {
    let mut writer = BufWriter::new(File::create(output.join("hybrid_queries.csv"))?);
    writeln!(writer, "{HYBRID_QUERY_HEADER}")?;
    for row in rows {
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{:.3},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{},{},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            csv_field(&manifest.dataset),
            codec,
            row.k,
            row.candidate_depth,
            row.max_candidates,
            row.max_segments,
            csv_field(&row.fusion),
            csv_field(&row.cache_profile),
            row.target_hot_query_fraction,
            row.client_concurrency,
            row.query_class,
            row.mode,
            row.repetition,
            row.query_position,
            csv_field(&row.query_id),
            row.latency_ms,
            row.metrics.ndcg,
            row.metrics.recall,
            row.metrics.precision,
            row.metrics.mrr,
            row.hits,
            row.cache_tier,
            row.observed_cached_byte_fraction,
            row.decoded_cache_hits,
            row.decoded_cache_bytes_read,
            row.disk_cache_reads,
            row.disk_cache_bytes_read,
            row.backing_reads,
            row.backing_bytes_read,
            row.bytes_read,
            row.network_gets,
            row.segments_searched,
            row.query_seed,
            ram_budget_bytes,
            row.collection_resident_bytes,
            row.retained_bytes,
            row.retained_capacity_bytes,
            row.retained_peak_bytes,
            row.transient_bytes,
            row.transient_capacity_bytes,
            row.transient_peak_bytes,
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_summary(
    output: &Path,
    manifest: &Manifest,
    codec: GlobalScanCodec,
    modes: &[Mode],
    rows: &[QueryMeasurement],
) -> BenchResult<()> {
    let mut writer = BufWriter::new(File::create(output.join("hybrid_summary.csv"))?);
    writeln!(
        writer,
        "dataset,scan_codec,k,candidate_depth,max_candidates,max_segments,fusion,cache_profile,target_hot_query_fraction,client_concurrency,mode,samples,mean_ms,stddev_ms,p50_ms,p95_ms,p99_ms,max_ms,ndcg_at_10,recall_at_10,precision_at_10,mrr_at_10,mean_observed_cached_byte_fraction,mean_bytes_read,mean_disk_cache_bytes_read,mean_backing_bytes_read,mean_network_gets"
    )?;
    for mode in modes {
        let selected = rows
            .iter()
            .filter(|row| row.mode == mode.name)
            .collect::<Vec<_>>();
        let latencies = selected
            .iter()
            .map(|row| row.latency_ms)
            .collect::<Vec<_>>();
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{:.3},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3},{:.3}",
            csv_field(&manifest.dataset),
            codec,
            selected.first().map_or(0, |row| row.k),
            selected.first().map_or(0, |row| row.candidate_depth),
            selected.first().map_or(0, |row| row.max_candidates),
            selected.first().map_or(0, |row| row.max_segments),
            csv_field(
                selected
                    .first()
                    .map_or("unspecified", |row| row.fusion.as_str())
            ),
            csv_field(
                selected
                    .first()
                    .map_or("unspecified", |row| row.cache_profile.as_str())
            ),
            selected
                .first()
                .map_or(0.0, |row| row.target_hot_query_fraction),
            selected.first().map_or(0, |row| row.client_concurrency),
            mode.name,
            selected.len(),
            mean(&latencies),
            sample_stddev(&latencies),
            percentile(&latencies, 0.50),
            percentile(&latencies, 0.95),
            percentile(&latencies, 0.99),
            maximum(&latencies),
            mean_iter(selected.iter().map(|row| row.metrics.ndcg)),
            mean_iter(selected.iter().map(|row| row.metrics.recall)),
            mean_iter(selected.iter().map(|row| row.metrics.precision)),
            mean_iter(selected.iter().map(|row| row.metrics.mrr)),
            mean_iter(selected.iter().map(|row| row.observed_cached_byte_fraction)),
            mean_iter(selected.iter().map(|row| row.bytes_read as f64)),
            mean_iter(selected.iter().map(|row| row.disk_cache_bytes_read as f64),),
            mean_iter(selected.iter().map(|row| row.backing_bytes_read as f64)),
            mean_iter(selected.iter().map(|row| row.network_gets as f64)),
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn selected_modes() -> BenchResult<Vec<Mode>> {
    match env::var("BORSUK_HYBRID_MODES") {
        Ok(value) if value.trim().eq_ignore_ascii_case("all") => {
            MODES.iter().map(|name| Mode::parse(name)).collect()
        }
        Ok(value) => value
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(Mode::parse)
            .collect(),
        Err(env::VarError::NotPresent) => MODES.iter().map(|name| Mode::parse(name)).collect(),
        Err(error) => Err(error.into()),
    }
}

fn scan_codec() -> BenchResult<GlobalScanCodec> {
    let value = env::var("BORSUK_HYBRID_SCAN_CODEC").unwrap_or_else(|_| "srht-pq-scan".to_string());
    Ok(GlobalScanCodec::from_str(&value)?)
}

fn parse_dense_element_type(value: &str) -> BenchResult<VectorElementType> {
    let element_type = VectorElementType::from_str(value)?;
    if element_type == VectorElementType::Binary {
        return Err(invalid_input(
            "BORSUK_HYBRID_DENSE_ELEMENT_TYPE cannot be binary with cosine dense retrieval",
        )
        .into());
    }
    Ok(element_type)
}

fn dense_element_type() -> BenchResult<VectorElementType> {
    match env::var("BORSUK_HYBRID_DENSE_ELEMENT_TYPE") {
        Ok(value) => parse_dense_element_type(&value),
        Err(env::VarError::NotPresent) => Ok(VectorElementType::Float32),
        Err(error) => Err(error.into()),
    }
}

fn parse_sparse_element_type(value: &str) -> BenchResult<VectorElementType> {
    let element_type = VectorElementType::from_str(value)?;
    if !matches!(
        element_type,
        VectorElementType::Float32 | VectorElementType::Float16
    ) {
        return Err(
            invalid_input("BORSUK_HYBRID_SPARSE_ELEMENT_TYPE must be float32 or float16").into(),
        );
    }
    Ok(element_type)
}

fn sparse_element_type() -> BenchResult<VectorElementType> {
    match env::var("BORSUK_HYBRID_SPARSE_ELEMENT_TYPE") {
        Ok(value) => parse_sparse_element_type(&value),
        Err(env::VarError::NotPresent) => Ok(VectorElementType::Float32),
        Err(error) => Err(error.into()),
    }
}

fn resolved_fusion() -> BenchResult<(Fusion, String)> {
    let name = env::var("BORSUK_HYBRID_FUSION").unwrap_or_else(|_| "rrf".to_string());
    match name.trim().to_ascii_lowercase().as_str() {
        "rrf" => {
            let rank_constant = env_usize("BORSUK_HYBRID_RRF_K", 1)?;
            Ok((
                Fusion::Rrf { k: rank_constant },
                format!("rrf-k{rank_constant}"),
            ))
        }
        "weighted" => {
            let dense = env_nonnegative_f64("BORSUK_HYBRID_DENSE_WEIGHT", 1.0)?;
            let sparse = env_nonnegative_f64("BORSUK_HYBRID_SPARSE_WEIGHT", 1.0)?;
            let text = env_nonnegative_f64("BORSUK_HYBRID_TEXT_WEIGHT", 1.0)?;
            if dense + sparse + text == 0.0 {
                return Err(invalid_input("at least one fusion weight must be positive").into());
            }
            Ok((
                Fusion::Weighted {
                    weights: BTreeMap::from([
                        (String::new(), dense as f32),
                        (SPARSE_NAME.to_string(), sparse as f32),
                        ("@text".to_string(), text as f32),
                    ]),
                },
                format!("weighted-d{dense:.3}-s{sparse:.3}-t{text:.3}"),
            ))
        }
        _ => Err(invalid_input("BORSUK_HYBRID_FUSION must be rrf or weighted").into()),
    }
}

fn required_env(name: &str) -> BenchResult<String> {
    env::var(name)
        .map_err(|_| invalid_input(&format!("{name} is required")))
        .map_err(Into::into)
}

fn required_path(name: &str) -> BenchResult<PathBuf> {
    Ok(PathBuf::from(required_env(name)?))
}

fn env_usize(name: &str, default: usize) -> BenchResult<usize> {
    let value = match env::var(name) {
        Ok(value) => value.parse::<usize>()?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if value == 0 && !matches!(name, "BORSUK_HYBRID_MAX_SEGMENTS" | "BORSUK_HYBRID_WARMUPS") {
        return Err(invalid_input(&format!("{name} must be positive")).into());
    }
    Ok(value)
}

fn env_u64(name: &str, default: u64) -> BenchResult<u64> {
    match env::var(name) {
        Ok(value) => Ok(value.parse::<u64>()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_optional_u64(name: &str) -> BenchResult<Option<u64>> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value.parse::<u64>()?;
            Ok((parsed > 0).then_some(parsed))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn env_bool(name: &str, default: bool) -> BenchResult<bool> {
    match env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => Err(invalid_input(&format!("{name} must be true or false")).into()),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_fraction(name: &str, default: f64) -> BenchResult<f64> {
    let value = match env::var(name) {
        Ok(value) => value.parse::<f64>()?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(invalid_input(&format!("{name} must be within [0,1]")).into());
    }
    Ok(value)
}

fn env_nonnegative_f64(name: &str, default: f64) -> BenchResult<f64> {
    let value = match env::var(name) {
        Ok(value) => value.parse::<f64>()?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if !value.is_finite() || value < 0.0 {
        return Err(invalid_input(&format!("{name} must be finite and non-negative")).into());
    }
    Ok(value)
}

fn byte_size(rows: usize, columns: usize, bytes: u64) -> BenchResult<u64> {
    u64::try_from(rows)?
        .checked_mul(u64::try_from(columns)?)
        .and_then(|value| value.checked_mul(bytes))
        .ok_or_else(|| invalid_input("dataset byte-size overflow").into())
}

fn read_u64(reader: &mut impl Read) -> io::Result<u64> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u32_values(reader: &mut impl Read, count: usize) -> io::Result<Vec<u32>> {
    let mut result = Vec::with_capacity(count);
    let mut bytes = [0_u8; 4];
    for _ in 0..count {
        reader.read_exact(&mut bytes)?;
        result.push(u32::from_le_bytes(bytes));
    }
    Ok(result)
}

fn read_f32_values(reader: &mut impl Read, count: usize) -> io::Result<Vec<f32>> {
    let mut result = Vec::with_capacity(count);
    let mut bytes = [0_u8; 4];
    for _ in 0..count {
        reader.read_exact(&mut bytes)?;
        result.push(f32::from_le_bytes(bytes));
    }
    Ok(result)
}

fn ensure_eof(reader: &mut impl Read, label: &str) -> BenchResult<()> {
    let mut trailing = [0_u8; 1];
    if reader.read(&mut trailing)? != 0 {
        return Err(invalid_input(&format!("{label} has trailing data")).into());
    }
    Ok(())
}

fn mean(values: &[f64]) -> f64 {
    mean_iter(values.iter().copied())
}

fn mean_iter(values: impl Iterator<Item = f64>) -> f64 {
    let (sum, count) = values.fold((0.0, 0usize), |(sum, count), value| {
        (sum + value, count + 1)
    });
    if count == 0 { 0.0 } else { sum / count as f64 }
}

fn sample_stddev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = mean(values);
    (values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64)
        .sqrt()
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

fn maximum(values: &[f64]) -> f64 {
    values.iter().copied().reduce(f64::max).unwrap_or(0.0)
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn permuted_positions(count: usize, seed: u64) -> Vec<usize> {
    let mut positions = (0..count).collect::<Vec<_>>();
    let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
    for upper in (1..count).rev() {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^= mixed >> 31;
        positions.swap(upper, mixed as usize % (upper + 1));
    }
    positions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_permutation_is_seeded_and_membership_preserving() {
        let first = permuted_positions(8, 17);
        assert_eq!(first, permuted_positions(8, 17));
        assert_ne!(first, permuted_positions(8, 23));
        let mut sorted = first;
        sorted.sort_unstable();
        assert_eq!(sorted, (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn physical_type_controls_cover_dense_and_sparse_simd_matrix() {
        for value in [
            "float32",
            "float16",
            "bfloat16",
            "float8-e4m3fn",
            "float8-e5m2",
            "int8",
        ] {
            assert_eq!(parse_dense_element_type(value).unwrap().as_str(), value);
        }
        assert!(parse_dense_element_type("binary").is_err());
        assert_eq!(
            parse_sparse_element_type("float32").unwrap(),
            VectorElementType::Float32
        );
        assert_eq!(
            parse_sparse_element_type("float16").unwrap(),
            VectorElementType::Float16
        );
        assert!(parse_sparse_element_type("bfloat16").is_err());
    }

    #[test]
    fn raw_hybrid_artifacts_expose_the_governed_memory_envelope() {
        assert_eq!(HYBRID_BUILD_HEADER.split(',').count(), 24);
        assert_eq!(HYBRID_QUERY_HEADER.split(',').count(), 41);
        for column in [
            "ram_budget_bytes",
            "collection_resident_bytes",
            "retained_bytes",
            "retained_capacity_bytes",
            "retained_peak_bytes",
            "transient_bytes",
            "transient_capacity_bytes",
            "transient_peak_bytes",
        ] {
            assert!(HYBRID_BUILD_HEADER.contains(column), "missing {column}");
            assert!(HYBRID_QUERY_HEADER.contains(column), "missing {column}");
        }
    }

    #[test]
    fn graded_effectiveness_uses_shared_qrels() {
        let qrels = BTreeMap::from([("best".to_string(), 2), ("also".to_string(), 1)]);
        let perfect = effectiveness_at_k(&["best".to_string(), "also".to_string()], &qrels, 10);
        assert!((perfect.ndcg - 1.0).abs() < 1e-12);
        assert!((perfect.recall - 1.0).abs() < 1e-12);
        assert!((perfect.precision - 0.2).abs() < 1e-12);
        assert!((perfect.mrr - 1.0).abs() < 1e-12);

        let partial = effectiveness_at_k(&["missing".to_string(), "also".to_string()], &qrels, 10);
        assert!(partial.ndcg > 0.0 && partial.ndcg < 1.0);
        assert_eq!(partial.recall, 0.5);
        assert_eq!(partial.precision, 0.1);
        assert_eq!(partial.mrr, 0.5);
    }

    #[test]
    fn sample_standard_deviation_is_reported() {
        assert_eq!(sample_stddev(&[]), 0.0);
        assert_eq!(sample_stddev(&[4.0]), 0.0);
        assert!((sample_stddev(&[1.0, 3.0]) - 2.0_f64.sqrt()).abs() < 1e-12);
    }

    #[test]
    fn cache_tier_preserves_mixed_reads() {
        assert_eq!(observed_cache_tier(0, 0, 0), "none");
        assert_eq!(observed_cache_tier(0, 10, 0), "disk");
        assert_eq!(observed_cache_tier(0, 10, 20), "disk+backing");
        assert_eq!(observed_cache_tier(5, 10, 20), "decoded+disk+backing");
    }

    #[test]
    fn qrel_loader_validates_all_rows_but_retains_only_measured_queries() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join("qrels.tsv"),
            "query-id\tcorpus-id\tscore\nq1\td1\t1\nq2\td2\t2\n",
        )
        .unwrap();
        let manifest = Manifest {
            schema_version: 1,
            dataset: "fixture".to_string(),
            split: "test".to_string(),
            documents: 2,
            queries: 2,
            qrels: 2,
            dense: DenseManifest {
                dimensions: 2,
                publication_valid: true,
                backend: "fixture".to_string(),
            },
            sparse: SparseManifest {
                dimensions: 2,
                backend: "fixture".to_string(),
            },
            qrels_semantics: "shared-across-all-retrieval-modes".to_string(),
        };
        let selected = BTreeSet::from(["q1".to_string()]);
        let loaded = load_qrels(directory.path(), &manifest, &selected).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded["q1"]["d1"], 1);
        assert!(!loaded.contains_key("q2"));
    }
}
