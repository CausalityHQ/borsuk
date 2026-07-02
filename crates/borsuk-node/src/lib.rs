//! Native Node/TypeScript bindings for BORSUK.
#![allow(missing_docs)]

use std::{path::PathBuf, sync::Mutex};

use borsuk::{
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, SearchMode,
    SearchOptions, StringMetric, VectorMetric, VectorRecord,
};
use napi::{Error, Result, Status};
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
}

#[napi(object)]
#[derive(Default)]
pub struct SearchOptionsJs {
    pub k: Option<u32>,
    pub mode: Option<String>,
    pub eps: Option<f64>,
    pub max_segments: Option<u32>,
    pub max_latency_ms: Option<u32>,
    pub max_candidates_per_segment: Option<u32>,
}

#[napi(object)]
pub struct Hit {
    pub id: String,
    pub distance: f64,
}

#[napi(object)]
pub struct SearchReportJs {
    pub hits: Vec<Hit>,
    pub segments_total: u32,
    pub segments_searched: u32,
    pub segments_skipped: u32,
    pub bytes_read: f64,
    pub graph_bytes_read: f64,
    pub records_considered: u32,
    pub records_scored: u32,
    pub graph_candidates_added: u32,
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
        open(uri, None)
    }

    #[napi]
    pub fn add(&self, ids: Vec<String>, vectors: Vec<Vec<f64>>) -> Result<()> {
        if ids.len() != vectors.len() {
            return Err(Error::new(
                Status::InvalidArg,
                "ids and vectors must have the same length",
            ));
        }

        let records = ids
            .into_iter()
            .zip(vectors)
            .map(|(id, vector)| VectorRecord::new(id, vector.into_iter().map(f64_to_f32).collect()))
            .collect::<Vec<_>>();

        self.inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .add(records)
            .map_err(to_js_error)
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

        Ok(hits
            .into_iter()
            .map(|hit| Hit {
                id: hit.id,
                distance: f64::from(hit.distance),
            })
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

        Ok(SearchReportJs {
            hits: report
                .hits
                .into_iter()
                .map(|hit| Hit {
                    id: hit.id,
                    distance: f64::from(hit.distance),
                })
                .collect(),
            segments_total: usize_to_u32(report.segments_total)?,
            segments_searched: usize_to_u32(report.segments_searched)?,
            segments_skipped: usize_to_u32(report.segments_skipped)?,
            bytes_read: report.bytes_read as f64,
            graph_bytes_read: report.graph_bytes_read as f64,
            records_considered: usize_to_u32(report.records_considered)?,
            records_scored: usize_to_u32(report.records_scored)?,
            graph_candidates_added: usize_to_u32(report.graph_candidates_added)?,
            elapsed_ms: u64_to_u32(report.elapsed_ms)?,
        })
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

#[napi]
pub fn create(options: CreateOptions) -> Result<JsIndex> {
    drop(options.ram_budget);
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
    open(uri, options.unwrap_or_default().cache_dir)
}

fn open(uri: String, cache_dir: Option<String>) -> Result<JsIndex> {
    let index =
        BorsukIndex::open_with_cache(&uri, cache_dir.map(PathBuf::from)).map_err(to_js_error)?;
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

fn usize_to_u32(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::new(Status::GenericFailure, "value exceeds u32"))
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
