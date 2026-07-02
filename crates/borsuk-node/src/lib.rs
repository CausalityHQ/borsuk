//! Native Node/TypeScript bindings for BORSUK.
#![allow(missing_docs)]

use std::{path::PathBuf, sync::Mutex};

use borsuk::{
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, OpenOptions, SearchMode,
    SearchOptions, StringMetric, VectorMetric, VectorRecord,
};
use napi::{Error, Result, Status, bindgen_prelude::Float32Array};
use napi_derive::napi;

#[napi(object)]
pub struct CreateOptions {
    pub uri: String,
    pub metric: String,
    pub dim: Option<u32>,
    pub dimensions: Option<u32>,
    pub segment_size: Option<u32>,
    pub segment_max_vectors: Option<u32>,
    pub ram_budget: Option<String>,
    pub cache_dir: Option<String>,
}

#[napi(object)]
#[derive(Default)]
pub struct OpenOptionsJs {
    pub cache_dir: Option<String>,
    pub ram_budget: Option<String>,
}

#[napi(object)]
#[derive(Default)]
pub struct SearchOptionsJs {
    pub k: Option<u32>,
    pub mode: Option<String>,
    pub eps: Option<f64>,
    pub max_segments: Option<u32>,
    pub max_bytes: Option<f64>,
    pub max_bytes_text: Option<String>,
    pub max_latency_ms: Option<u32>,
    pub max_candidates_per_segment: Option<u32>,
}

#[napi(object)]
pub struct Hit {
    pub id: String,
    pub distance: f64,
    #[napi(js_name = "payloadRef")]
    pub payload_ref: Option<String>,
}

#[napi(object)]
pub struct IndexStatsJs {
    pub metric: String,
    pub dimensions: u32,
    pub segment_max_vectors: u32,
    pub ram_budget_bytes: Option<f64>,
    pub manifest_version: f64,
    pub segments: u32,
    pub records: u32,
    pub segment_bytes: f64,
    pub graph_bytes: f64,
    pub resident_bytes_estimate: f64,
}

#[napi(object)]
pub struct SearchReportJs {
    pub hits: Vec<Hit>,
    pub segments_total: u32,
    pub segments_searched: u32,
    pub segments_skipped: u32,
    pub bytes_read: f64,
    pub graph_bytes_read: f64,
    pub object_cache_hits: u32,
    pub object_cache_misses: u32,
    pub records_considered: u32,
    pub records_scored: u32,
    pub graph_candidates_added: u32,
    pub resident_bytes_estimate: f64,
    pub elapsed_ms: u32,
}

#[napi(object)]
#[derive(Default)]
pub struct CompactionOptionsJs {
    pub source_level: Option<u32>,
    pub target_level: Option<u32>,
    pub max_segments: Option<u32>,
    pub min_segments: Option<u32>,
    pub target_segment_max_vectors: Option<u32>,
}

#[napi(object)]
pub struct CompactionReportJs {
    pub compacted: bool,
    pub source_level: u32,
    pub target_level: u32,
    pub segments_read: u32,
    pub segments_written: u32,
    pub records_rewritten: u32,
    pub bytes_read: f64,
    pub bytes_written: f64,
    pub object_cache_hits: u32,
    pub object_cache_misses: u32,
    pub manifest_version: f64,
}

#[napi(object)]
#[derive(Default)]
pub struct GarbageCollectionOptionsJs {
    pub dry_run: Option<bool>,
}

#[napi(object)]
pub struct GarbageCollectionReportJs {
    pub dry_run: bool,
    pub objects_scanned: u32,
    pub objects_deleted: u32,
    pub bytes_reclaimable: f64,
    pub bytes_reclaimed: f64,
    pub candidates: Vec<String>,
}

#[napi(js_name = "Index")]
pub struct JsIndex {
    inner: Mutex<BorsukIndex>,
}

#[napi]
impl JsIndex {
    #[napi(constructor)]
    pub fn new(uri: String) -> Result<Self> {
        open(uri, None, None)
    }

    #[napi]
    pub fn add(
        &self,
        ids: Vec<String>,
        vectors: Vec<Vec<f64>>,
        payload_refs: Option<Vec<Option<String>>>,
    ) -> Result<()> {
        if ids.len() != vectors.len() {
            return Err(Error::new(
                Status::InvalidArg,
                "ids and vectors must have the same length",
            ));
        }

        let payload_refs = optional_payload_refs(payload_refs, ids.len())?;
        let records = ids
            .into_iter()
            .zip(vectors)
            .zip(payload_refs)
            .map(|((id, vector), payload_ref)| VectorRecord {
                id,
                vector: vector.into_iter().map(f64_to_f32).collect(),
                payload_ref,
            })
            .collect::<Vec<_>>();

        self.inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .add(records)
            .map_err(to_js_error)
    }

    #[napi(js_name = "addBuffer")]
    pub fn add_buffer(
        &self,
        ids: Vec<String>,
        vectors: Float32Array,
        payload_refs: Option<Vec<Option<String>>>,
    ) -> Result<()> {
        let mut index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let records = records_from_flat_vectors(ids, vectors.as_ref(), dimensions, payload_refs)?;

        index.add(records).map_err(to_js_error)
    }

    #[napi]
    pub fn stats(&self) -> Result<IndexStatsJs> {
        let stats = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .stats();

        index_stats_to_js(stats)
    }

    #[napi]
    pub fn search(&self, query: Vec<f64>, options: Option<SearchOptionsJs>) -> Result<Vec<Hit>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let query = query.into_iter().map(f64_to_f32).collect::<Vec<_>>();
        let hits = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search(
                &query,
                SearchOptions {
                    k: options.k.unwrap_or(10) as usize,
                    mode,
                },
            )
            .map_err(to_js_error)?;

        Ok(hits.into_iter().map(hit_to_js).collect())
    }

    #[napi(js_name = "searchWithReportBuffer")]
    pub fn search_with_report_buffer(
        &self,
        query: Float32Array,
        options: Option<SearchOptionsJs>,
    ) -> Result<SearchReportJs> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let query = query_from_flat_vector(query.as_ref(), dimensions, "query buffer")?;
        let report = index
            .search_with_report(
                &query,
                SearchOptions {
                    k: options.k.unwrap_or(10) as usize,
                    mode,
                },
            )
            .map_err(to_js_error)?;

        search_report_to_js(report)
    }

    #[napi]
    pub fn search_batch(
        &self,
        queries: Vec<Vec<f64>>,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<Vec<Hit>>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let queries = queries
            .into_iter()
            .map(|query| query.into_iter().map(f64_to_f32).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let results = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_batch(
                &queries,
                SearchOptions {
                    k: options.k.unwrap_or(10) as usize,
                    mode,
                },
            )
            .map_err(to_js_error)?;

        Ok(results
            .into_iter()
            .map(|hits| hits.into_iter().map(hit_to_js).collect())
            .collect())
    }

    #[napi(js_name = "searchBatchBuffer")]
    pub fn search_batch_buffer(
        &self,
        queries: Float32Array,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<Vec<Hit>>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let queries = vectors_from_flat_rows(queries.as_ref(), dimensions, "query buffer")?;
        let results = index
            .search_batch(
                &queries,
                SearchOptions {
                    k: options.k.unwrap_or(10) as usize,
                    mode,
                },
            )
            .map_err(to_js_error)?;

        Ok(results
            .into_iter()
            .map(|hits| hits.into_iter().map(hit_to_js).collect())
            .collect())
    }

    #[napi]
    pub fn search_with_report(
        &self,
        query: Vec<f64>,
        options: Option<SearchOptionsJs>,
    ) -> Result<SearchReportJs> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let query = query.into_iter().map(f64_to_f32).collect::<Vec<_>>();
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_with_report(
                &query,
                SearchOptions {
                    k: options.k.unwrap_or(10) as usize,
                    mode,
                },
            )
            .map_err(to_js_error)?;

        search_report_to_js(report)
    }

    #[napi]
    pub fn search_batch_with_report(
        &self,
        queries: Vec<Vec<f64>>,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<SearchReportJs>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let queries = queries
            .into_iter()
            .map(|query| query.into_iter().map(f64_to_f32).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let reports = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_batch_with_report(
                &queries,
                SearchOptions {
                    k: options.k.unwrap_or(10) as usize,
                    mode,
                },
            )
            .map_err(to_js_error)?;

        reports.into_iter().map(search_report_to_js).collect()
    }

    #[napi]
    pub fn compact(&self, options: Option<CompactionOptionsJs>) -> Result<CompactionReportJs> {
        let options = options.unwrap_or_default();
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .compact(CompactionOptions {
                source_level: option_u32_to_u8(options.source_level, 0, "sourceLevel")?,
                target_level: option_u32_to_u8(options.target_level, 1, "targetLevel")?,
                max_segments: options.max_segments.map(|value| value as usize),
                min_segments: options.min_segments.unwrap_or(2) as usize,
                target_segment_max_vectors: options
                    .target_segment_max_vectors
                    .map(|value| value as usize),
            })
            .map_err(to_js_error)?;

        Ok(CompactionReportJs {
            compacted: report.compacted,
            source_level: u32::from(report.source_level),
            target_level: u32::from(report.target_level),
            segments_read: usize_to_u32(report.segments_read)?,
            segments_written: usize_to_u32(report.segments_written)?,
            records_rewritten: usize_to_u32(report.records_rewritten)?,
            bytes_read: report.bytes_read as f64,
            bytes_written: report.bytes_written as f64,
            object_cache_hits: usize_to_u32(report.object_cache_hits)?,
            object_cache_misses: usize_to_u32(report.object_cache_misses)?,
            manifest_version: report.manifest_version as f64,
        })
    }

    #[napi]
    pub fn gc_obsolete_segments(
        &self,
        options: Option<GarbageCollectionOptionsJs>,
    ) -> Result<GarbageCollectionReportJs> {
        let options = options.unwrap_or_default();
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .gc_obsolete_segments(GarbageCollectionOptions {
                dry_run: options.dry_run.unwrap_or(true),
            })
            .map_err(to_js_error)?;

        Ok(GarbageCollectionReportJs {
            dry_run: report.dry_run,
            objects_scanned: usize_to_u32(report.objects_scanned)?,
            objects_deleted: usize_to_u32(report.objects_deleted)?,
            bytes_reclaimable: report.bytes_reclaimable as f64,
            bytes_reclaimed: report.bytes_reclaimed as f64,
            candidates: report.candidates,
        })
    }
}

#[napi(js_name = "vectorDistance")]
pub fn vector_distance(metric: String, left: Vec<f64>, right: Vec<f64>) -> Result<f64> {
    let metric = metric.parse::<VectorMetric>().map_err(to_js_error)?;
    let left = left.into_iter().map(f64_to_f32).collect::<Vec<_>>();
    let right = right.into_iter().map(f64_to_f32).collect::<Vec<_>>();
    metric
        .distance(&left, &right)
        .map(f64::from)
        .map_err(to_js_error)
}

#[napi(js_name = "stringDistance")]
pub fn string_distance(metric: String, left: String, right: String) -> Result<f64> {
    let metric = metric.parse::<StringMetric>().map_err(to_js_error)?;
    Ok(f64::from(metric.distance(&left, &right)))
}

#[napi(js_name = "recallAtK")]
pub fn recall_at_k(exact_ids: Vec<String>, actual_ids: Vec<String>, k: u32) -> Result<f64> {
    borsuk::recall_at_k(&exact_ids, &actual_ids, k as usize)
        .map(f64::from)
        .map_err(to_js_error)
}

#[napi]
pub fn create(options: CreateOptions) -> Result<JsIndex> {
    let ram_budget_bytes = options
        .ram_budget
        .as_deref()
        .map(borsuk::parse_ram_budget)
        .transpose()
        .map_err(to_js_error)?;
    let dimensions = resolve_dimensions(options.dim, options.dimensions)?;
    let metric = options
        .metric
        .parse::<VectorMetric>()
        .map_err(to_js_error)?;
    let index = BorsukIndex::create_with_cache(
        IndexConfig {
            uri: options.uri,
            metric,
            dimensions,
            segment_max_vectors: options
                .segment_max_vectors
                .or(options.segment_size)
                .unwrap_or(4096) as usize,
            ram_budget_bytes,
        },
        options.cache_dir.map(PathBuf::from),
    )
    .map_err(to_js_error)?;

    Ok(JsIndex {
        inner: Mutex::new(index),
    })
}

#[napi(js_name = "open")]
pub fn open_index(uri: String, options: Option<OpenOptionsJs>) -> Result<JsIndex> {
    let options = options.unwrap_or_default();
    open(uri, options.cache_dir, options.ram_budget)
}

fn open(uri: String, cache_dir: Option<String>, ram_budget: Option<String>) -> Result<JsIndex> {
    let ram_budget_bytes = ram_budget
        .as_deref()
        .map(borsuk::parse_ram_budget)
        .transpose()
        .map_err(to_js_error)?;
    let index = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            cache_dir: cache_dir.map(PathBuf::from),
            ram_budget_bytes,
        },
    )
    .map_err(to_js_error)?;
    Ok(JsIndex {
        inner: Mutex::new(index),
    })
}

fn resolve_dimensions(dim: Option<u32>, dimensions: Option<u32>) -> Result<usize> {
    match (dim, dimensions) {
        (Some(left), Some(right)) if left != right => Err(Error::new(
            Status::InvalidArg,
            "dim and dimensions disagree",
        )),
        (Some(value), _) | (_, Some(value)) => Ok(value as usize),
        (None, None) => Err(Error::new(
            Status::InvalidArg,
            "dim or dimensions is required",
        )),
    }
}

fn parse_mode(options: &SearchOptionsJs) -> Result<SearchMode> {
    match options.mode.as_deref().unwrap_or("exact") {
        "exact" => Ok(SearchMode::Exact),
        "approx" => Ok(SearchMode::Approx {
            eps: options.eps.map(f64_to_f32),
            max_segments: options.max_segments.map(|value| value as usize),
            max_bytes: option_byte_size_to_u64(
                options.max_bytes,
                options.max_bytes_text.as_deref(),
                "maxBytes",
            )?,
            max_latency_ms: options.max_latency_ms.map(u64::from),
            max_candidates_per_segment: options
                .max_candidates_per_segment
                .map(|value| value as usize),
        }),
        other => Err(Error::new(
            Status::InvalidArg,
            format!("unknown search mode `{other}`"),
        )),
    }
}

fn f64_to_f32(value: f64) -> f32 {
    value as f32
}

fn option_f64_to_u64(value: Option<f64>, field: &str) -> Result<Option<u64>> {
    value.map_or(Ok(None), |actual| {
        if actual.is_finite() && actual >= 0.0 && actual.fract() == 0.0 && actual <= u64::MAX as f64
        {
            Ok(Some(actual as u64))
        } else {
            Err(Error::new(
                Status::InvalidArg,
                format!("{field} must be a non-negative integer"),
            ))
        }
    })
}

fn option_byte_size_to_u64(
    numeric: Option<f64>,
    text: Option<&str>,
    field: &str,
) -> Result<Option<u64>> {
    match (numeric, text) {
        (Some(_), Some(_)) => Err(Error::new(
            Status::InvalidArg,
            format!("{field} must be provided as either a number or a byte-size string"),
        )),
        (Some(value), None) => option_f64_to_u64(Some(value), field),
        (None, Some(value)) => borsuk::parse_byte_size(value, field)
            .map(Some)
            .map_err(to_js_error),
        (None, None) => Ok(None),
    }
}

fn usize_to_u32(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::new(Status::GenericFailure, "value exceeds u32"))
}

fn index_stats_to_js(stats: borsuk::IndexStats) -> Result<IndexStatsJs> {
    Ok(IndexStatsJs {
        metric: stats.metric,
        dimensions: usize_to_u32(stats.dimensions)?,
        segment_max_vectors: usize_to_u32(stats.segment_max_vectors)?,
        ram_budget_bytes: stats.ram_budget_bytes.map(|value| value as f64),
        manifest_version: stats.manifest_version as f64,
        segments: usize_to_u32(stats.segments)?,
        records: usize_to_u32(stats.records)?,
        segment_bytes: stats.segment_bytes as f64,
        graph_bytes: stats.graph_bytes as f64,
        resident_bytes_estimate: stats.resident_bytes_estimate as f64,
    })
}

fn search_report_to_js(report: borsuk::SearchReport) -> Result<SearchReportJs> {
    Ok(SearchReportJs {
        hits: report.hits.into_iter().map(hit_to_js).collect(),
        segments_total: usize_to_u32(report.segments_total)?,
        segments_searched: usize_to_u32(report.segments_searched)?,
        segments_skipped: usize_to_u32(report.segments_skipped)?,
        bytes_read: report.bytes_read as f64,
        graph_bytes_read: report.graph_bytes_read as f64,
        object_cache_hits: usize_to_u32(report.object_cache_hits)?,
        object_cache_misses: usize_to_u32(report.object_cache_misses)?,
        records_considered: usize_to_u32(report.records_considered)?,
        records_scored: usize_to_u32(report.records_scored)?,
        graph_candidates_added: usize_to_u32(report.graph_candidates_added)?,
        resident_bytes_estimate: report.resident_bytes_estimate as f64,
        elapsed_ms: u64_to_u32(report.elapsed_ms)?,
    })
}

fn optional_payload_refs(
    payload_refs: Option<Vec<Option<String>>>,
    expected_len: usize,
) -> Result<Vec<Option<String>>> {
    match payload_refs {
        Some(payload_refs) if payload_refs.len() != expected_len => Err(Error::new(
            Status::InvalidArg,
            "payloadRefs must have the same length as ids and vectors",
        )),
        Some(payload_refs) => Ok(payload_refs),
        None => Ok(vec![None; expected_len]),
    }
}

fn records_from_flat_vectors(
    ids: Vec<String>,
    vectors: &[f32],
    dimensions: usize,
    payload_refs: Option<Vec<Option<String>>>,
) -> Result<Vec<VectorRecord>> {
    let expected_values = ids.len().checked_mul(dimensions).ok_or_else(|| {
        Error::new(
            Status::InvalidArg,
            "flat vector buffer length exceeds usize",
        )
    })?;
    if vectors.len() != expected_values {
        return Err(Error::new(
            Status::InvalidArg,
            format!(
                "flat vector buffer length must equal ids length * index dimensions (expected {expected_values} float32 values, got {})",
                vectors.len()
            ),
        ));
    }

    let payload_refs = optional_payload_refs(payload_refs, ids.len())?;
    Ok(ids
        .into_iter()
        .zip(vectors.chunks_exact(dimensions))
        .zip(payload_refs)
        .map(|((id, vector), payload_ref)| VectorRecord {
            id,
            vector: vector.to_vec(),
            payload_ref,
        })
        .collect())
}

fn vectors_from_flat_rows(
    vectors: &[f32],
    dimensions: usize,
    label: &str,
) -> Result<Vec<Vec<f32>>> {
    if dimensions == 0 {
        return Err(Error::new(
            Status::InvalidArg,
            "index dimensions must be greater than zero",
        ));
    }
    if !vectors.len().is_multiple_of(dimensions) {
        return Err(Error::new(
            Status::InvalidArg,
            format!(
                "flat {label} length must be a multiple of index dimensions ({dimensions}); got {} float32 values",
                vectors.len()
            ),
        ));
    }

    Ok(vectors
        .chunks_exact(dimensions)
        .map(<[f32]>::to_vec)
        .collect())
}

fn query_from_flat_vector(query: &[f32], dimensions: usize, label: &str) -> Result<Vec<f32>> {
    if query.len() != dimensions {
        return Err(Error::new(
            Status::InvalidArg,
            format!(
                "flat {label} length must equal index dimensions ({dimensions}); got {} float32 values",
                query.len()
            ),
        ));
    }

    Ok(query.to_vec())
}

fn hit_to_js(hit: borsuk::SearchHit) -> Hit {
    Hit {
        id: hit.id,
        distance: f64::from(hit.distance),
        payload_ref: hit.payload_ref,
    }
}

fn option_u32_to_u8(value: Option<u32>, default: u8, field: &str) -> Result<u8> {
    value.map_or(Ok(default), |actual| {
        u8::try_from(actual).map_err(|_| {
            Error::new(
                Status::InvalidArg,
                format!("{field} must be between 0 and 255"),
            )
        })
    })
}

fn u64_to_u32(value: u64) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::new(Status::GenericFailure, "value exceeds u32"))
}

fn to_js_error(error: borsuk::BorsukError) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}
