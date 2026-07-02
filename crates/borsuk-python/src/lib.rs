//! Native Python bindings for BORSUK.

use std::{path::PathBuf, sync::Mutex};

use borsuk::{
    BorsukIndex, CompactionOptions, CompactionReport, GarbageCollectionOptions,
    GarbageCollectionReport, IndexConfig, IndexStats, OpenOptions, SearchHit, SearchMode,
    SearchOptions, SearchReport, StringMetric, VectorMetric, VectorRecord,
};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    types::PyAny,
};

pyo3::create_exception!(
    _borsuk,
    BorsukError,
    PyRuntimeError,
    "Runtime error raised by the BORSUK native core."
);

#[pyclass(name = "Hit", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyHit {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    distance: f32,
    #[pyo3(get)]
    payload_ref: Option<String>,
}

#[pymethods]
impl PyHit {
    fn __repr__(&self) -> String {
        format!(
            "Hit(id={:?}, distance={}, payload_ref={:?})",
            self.id, self.distance, self.payload_ref
        )
    }
}

#[pyclass(name = "IndexStats", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyIndexStats {
    #[pyo3(get)]
    metric: String,
    #[pyo3(get)]
    dimensions: usize,
    #[pyo3(get)]
    segment_max_vectors: usize,
    #[pyo3(get)]
    ram_budget_bytes: Option<u64>,
    #[pyo3(get)]
    manifest_version: u64,
    #[pyo3(get)]
    segments: usize,
    #[pyo3(get)]
    records: usize,
    #[pyo3(get)]
    segment_bytes: u64,
    #[pyo3(get)]
    graph_bytes: u64,
    #[pyo3(get)]
    resident_bytes_estimate: u64,
}

#[pymethods]
impl PyIndexStats {
    fn __repr__(&self) -> String {
        format!(
            "IndexStats(metric={:?}, dimensions={}, segment_max_vectors={}, ram_budget_bytes={:?}, manifest_version={}, segments={}, records={}, segment_bytes={}, graph_bytes={}, resident_bytes_estimate={})",
            self.metric,
            self.dimensions,
            self.segment_max_vectors,
            self.ram_budget_bytes,
            self.manifest_version,
            self.segments,
            self.records,
            self.segment_bytes,
            self.graph_bytes,
            self.resident_bytes_estimate
        )
    }
}

#[pyclass(name = "SearchReport", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PySearchReport {
    #[pyo3(get)]
    hits: Vec<PyHit>,
    #[pyo3(get)]
    segments_total: usize,
    #[pyo3(get)]
    segments_searched: usize,
    #[pyo3(get)]
    segments_skipped: usize,
    #[pyo3(get)]
    bytes_read: u64,
    #[pyo3(get)]
    graph_bytes_read: u64,
    #[pyo3(get)]
    object_cache_hits: usize,
    #[pyo3(get)]
    object_cache_misses: usize,
    #[pyo3(get)]
    records_considered: usize,
    #[pyo3(get)]
    records_scored: usize,
    #[pyo3(get)]
    graph_candidates_added: usize,
    #[pyo3(get)]
    resident_bytes_estimate: u64,
    #[pyo3(get)]
    elapsed_ms: u64,
}

#[pymethods]
impl PySearchReport {
    fn __repr__(&self) -> String {
        format!(
            "SearchReport(hits={}, segments_total={}, segments_searched={}, segments_skipped={}, bytes_read={}, graph_bytes_read={}, object_cache_hits={}, object_cache_misses={}, records_considered={}, records_scored={}, graph_candidates_added={}, resident_bytes_estimate={}, elapsed_ms={})",
            self.hits.len(),
            self.segments_total,
            self.segments_searched,
            self.segments_skipped,
            self.bytes_read,
            self.graph_bytes_read,
            self.object_cache_hits,
            self.object_cache_misses,
            self.records_considered,
            self.records_scored,
            self.graph_candidates_added,
            self.resident_bytes_estimate,
            self.elapsed_ms
        )
    }
}

#[pyclass(name = "CompactionReport", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyCompactionReport {
    #[pyo3(get)]
    compacted: bool,
    #[pyo3(get)]
    source_level: u8,
    #[pyo3(get)]
    target_level: u8,
    #[pyo3(get)]
    segments_read: usize,
    #[pyo3(get)]
    segments_written: usize,
    #[pyo3(get)]
    records_rewritten: usize,
    #[pyo3(get)]
    bytes_read: u64,
    #[pyo3(get)]
    bytes_written: u64,
    #[pyo3(get)]
    object_cache_hits: usize,
    #[pyo3(get)]
    object_cache_misses: usize,
    #[pyo3(get)]
    manifest_version: u64,
}

#[pymethods]
impl PyCompactionReport {
    fn __repr__(&self) -> String {
        format!(
            "CompactionReport(compacted={}, source_level={}, target_level={}, segments_read={}, segments_written={}, records_rewritten={}, bytes_read={}, bytes_written={}, object_cache_hits={}, object_cache_misses={}, manifest_version={})",
            self.compacted,
            self.source_level,
            self.target_level,
            self.segments_read,
            self.segments_written,
            self.records_rewritten,
            self.bytes_read,
            self.bytes_written,
            self.object_cache_hits,
            self.object_cache_misses,
            self.manifest_version
        )
    }
}

#[pyclass(name = "GarbageCollectionReport", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyGarbageCollectionReport {
    #[pyo3(get)]
    dry_run: bool,
    #[pyo3(get)]
    objects_scanned: usize,
    #[pyo3(get)]
    objects_deleted: usize,
    #[pyo3(get)]
    bytes_reclaimable: u64,
    #[pyo3(get)]
    bytes_reclaimed: u64,
    #[pyo3(get)]
    candidates: Vec<String>,
}

#[pymethods]
impl PyGarbageCollectionReport {
    fn __repr__(&self) -> String {
        format!(
            "GarbageCollectionReport(dry_run={}, objects_scanned={}, objects_deleted={}, bytes_reclaimable={}, bytes_reclaimed={}, candidates={})",
            self.dry_run,
            self.objects_scanned,
            self.objects_deleted,
            self.bytes_reclaimable,
            self.bytes_reclaimed,
            self.candidates.len()
        )
    }
}

#[pyclass(name = "Index")]
struct PyIndex {
    inner: Mutex<BorsukIndex>,
}

#[pymethods]
impl PyIndex {
    #[new]
    fn new(uri: String) -> PyResult<Self> {
        open(uri, None, None)
    }

    #[pyo3(signature = (ids, vectors, payload_refs = None))]
    fn add(
        &self,
        ids: Vec<String>,
        vectors: Vec<Vec<f32>>,
        payload_refs: Option<Vec<Option<String>>>,
    ) -> PyResult<()> {
        if ids.len() != vectors.len() {
            return Err(PyValueError::new_err(
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
                vector,
                payload_ref,
            })
            .collect::<Vec<_>>();

        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .add(records)
            .map_err(to_py_error)
    }

    fn stats(&self) -> PyResult<PyIndexStats> {
        let stats = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .stats();

        Ok(stats.into())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, k = 10, mode = "exact", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, max_candidates_per_segment = None))]
    fn search(
        &self,
        query: Vec<f32>,
        k: usize,
        mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        max_candidates_per_segment: Option<usize>,
    ) -> PyResult<Vec<PyHit>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            max_candidates_per_segment,
        )?;
        let hits = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search(&query, SearchOptions { k, mode })
            .map_err(to_py_error)?;

        Ok(hits.into_iter().map(PyHit::from).collect())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queries, k = 10, mode = "exact", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, max_candidates_per_segment = None))]
    fn search_batch(
        &self,
        queries: Vec<Vec<f32>>,
        k: usize,
        mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        max_candidates_per_segment: Option<usize>,
    ) -> PyResult<Vec<Vec<PyHit>>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            max_candidates_per_segment,
        )?;
        let results = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_batch(&queries, SearchOptions { k, mode })
            .map_err(to_py_error)?;

        Ok(results
            .into_iter()
            .map(|hits| hits.into_iter().map(PyHit::from).collect())
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queries, k = 10, mode = "exact", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, max_candidates_per_segment = None))]
    fn search_batch_with_report(
        &self,
        queries: Vec<Vec<f32>>,
        k: usize,
        mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        max_candidates_per_segment: Option<usize>,
    ) -> PyResult<Vec<PySearchReport>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            max_candidates_per_segment,
        )?;
        let reports = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_batch_with_report(&queries, SearchOptions { k, mode })
            .map_err(to_py_error)?;

        Ok(reports.into_iter().map(PySearchReport::from).collect())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, k = 10, mode = "exact", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, max_candidates_per_segment = None))]
    fn search_with_report(
        &self,
        query: Vec<f32>,
        k: usize,
        mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        max_candidates_per_segment: Option<usize>,
    ) -> PyResult<PySearchReport> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            max_candidates_per_segment,
        )?;
        let report = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_with_report(&query, SearchOptions { k, mode })
            .map_err(to_py_error)?;

        Ok(report.into())
    }

    #[pyo3(signature = (*, source_level = 0, target_level = 1, max_segments = None, min_segments = 2, target_segment_max_vectors = None))]
    fn compact(
        &self,
        source_level: u8,
        target_level: u8,
        max_segments: Option<usize>,
        min_segments: usize,
        target_segment_max_vectors: Option<usize>,
    ) -> PyResult<PyCompactionReport> {
        let report = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .compact(CompactionOptions {
                source_level,
                target_level,
                max_segments,
                min_segments,
                target_segment_max_vectors,
            })
            .map_err(to_py_error)?;

        Ok(report.into())
    }

    #[pyo3(signature = (*, dry_run = true))]
    fn gc_obsolete_segments(&self, dry_run: bool) -> PyResult<PyGarbageCollectionReport> {
        let report = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .gc_obsolete_segments(GarbageCollectionOptions { dry_run })
            .map_err(to_py_error)?;

        Ok(report.into())
    }
}

#[pyfunction]
fn vector_distance(metric: String, left: Vec<f32>, right: Vec<f32>) -> PyResult<f32> {
    let metric = metric.parse::<VectorMetric>().map_err(to_py_value_error)?;
    metric.distance(&left, &right).map_err(to_py_value_error)
}

#[pyfunction]
fn string_distance(metric: String, left: String, right: String) -> PyResult<f32> {
    let metric = metric.parse::<StringMetric>().map_err(to_py_value_error)?;
    Ok(metric.distance(&left, &right))
}

#[pyfunction]
fn recall_at_k(exact_ids: Vec<String>, actual_ids: Vec<String>, k: usize) -> PyResult<f32> {
    borsuk::recall_at_k(&exact_ids, &actual_ids, k).map_err(to_py_value_error)
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (*, uri, metric, dim = None, dimensions = None, segment_size = 4096, segment_max_vectors = None, ram_budget = None, cache_dir = None))]
fn create(
    uri: String,
    metric: String,
    dim: Option<usize>,
    dimensions: Option<usize>,
    segment_size: usize,
    segment_max_vectors: Option<usize>,
    ram_budget: Option<String>,
    cache_dir: Option<String>,
) -> PyResult<PyIndex> {
    let dimensions = resolve_dimensions(dim, dimensions)?;
    let metric = metric.parse::<VectorMetric>().map_err(to_py_error)?;
    let ram_budget_bytes = ram_budget
        .as_deref()
        .map(borsuk::parse_ram_budget)
        .transpose()
        .map_err(to_py_value_error)?;
    let index = BorsukIndex::create_with_cache(
        IndexConfig {
            uri,
            metric,
            dimensions,
            segment_max_vectors: segment_max_vectors.unwrap_or(segment_size),
            ram_budget_bytes,
        },
        cache_dir.map(PathBuf::from),
    )
    .map_err(to_py_error)?;

    Ok(PyIndex {
        inner: Mutex::new(index),
    })
}

#[pyfunction]
#[pyo3(signature = (uri, cache_dir = None, ram_budget = None))]
#[pyo3(name = "open")]
fn open_py(
    uri: String,
    cache_dir: Option<String>,
    ram_budget: Option<String>,
) -> PyResult<PyIndex> {
    open(uri, cache_dir, ram_budget)
}

#[pymodule]
fn _borsuk(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("BorsukError", module.py().get_type::<BorsukError>())?;
    module.add_class::<PyCompactionReport>()?;
    module.add_class::<PyGarbageCollectionReport>()?;
    module.add_class::<PyHit>()?;
    module.add_class::<PyIndexStats>()?;
    module.add_class::<PySearchReport>()?;
    module.add_class::<PyIndex>()?;
    module.add_function(wrap_pyfunction!(create, module)?)?;
    module.add_function(wrap_pyfunction!(open_py, module)?)?;
    module.add_function(wrap_pyfunction!(recall_at_k, module)?)?;
    module.add_function(wrap_pyfunction!(string_distance, module)?)?;
    module.add_function(wrap_pyfunction!(vector_distance, module)?)?;
    Ok(())
}

fn open(uri: String, cache_dir: Option<String>, ram_budget: Option<String>) -> PyResult<PyIndex> {
    let ram_budget_bytes = ram_budget
        .as_deref()
        .map(borsuk::parse_ram_budget)
        .transpose()
        .map_err(to_py_value_error)?;
    let index = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            cache_dir: cache_dir.map(PathBuf::from),
            ram_budget_bytes,
        },
    )
    .map_err(to_py_error)?;
    Ok(PyIndex {
        inner: Mutex::new(index),
    })
}

fn optional_payload_refs(
    payload_refs: Option<Vec<Option<String>>>,
    expected_len: usize,
) -> PyResult<Vec<Option<String>>> {
    match payload_refs {
        Some(payload_refs) if payload_refs.len() != expected_len => Err(PyValueError::new_err(
            "payload_refs must have the same length as ids and vectors",
        )),
        Some(payload_refs) => Ok(payload_refs),
        None => Ok(vec![None; expected_len]),
    }
}

fn resolve_dimensions(dim: Option<usize>, dimensions: Option<usize>) -> PyResult<usize> {
    match (dim, dimensions) {
        (Some(left), Some(right)) if left != right => {
            Err(PyValueError::new_err("dim and dimensions disagree"))
        }
        (Some(value), _) | (_, Some(value)) => Ok(value),
        (None, None) => Err(PyValueError::new_err("dim or dimensions is required")),
    }
}

fn parse_mode(
    mode: &str,
    eps: Option<f32>,
    max_segments: Option<usize>,
    max_bytes: Option<u64>,
    max_latency_ms: Option<u64>,
    max_candidates_per_segment: Option<usize>,
) -> PyResult<SearchMode> {
    match mode {
        "exact" => Ok(SearchMode::Exact),
        "approx" => Ok(SearchMode::Approx {
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            max_candidates_per_segment,
        }),
        other => Err(PyValueError::new_err(format!(
            "unknown search mode `{other}`"
        ))),
    }
}

fn parse_optional_byte_size(
    value: Option<&Bound<'_, PyAny>>,
    field_name: &str,
) -> PyResult<Option<u64>> {
    let Some(value) = value else {
        return Ok(None);
    };

    if let Ok(bytes) = value.extract::<u64>() {
        return Ok(Some(bytes));
    }

    if let Ok(text) = value.extract::<String>() {
        return borsuk::parse_byte_size(&text, field_name)
            .map(Some)
            .map_err(to_py_value_error);
    }

    Err(PyValueError::new_err(format!(
        "{field_name} must be an integer byte count or byte-size string"
    )))
}

fn to_py_error(error: borsuk::BorsukError) -> PyErr {
    BorsukError::new_err(error.to_string())
}

fn to_py_value_error(error: borsuk::BorsukError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

impl From<IndexStats> for PyIndexStats {
    fn from(stats: IndexStats) -> Self {
        Self {
            metric: stats.metric,
            dimensions: stats.dimensions,
            segment_max_vectors: stats.segment_max_vectors,
            ram_budget_bytes: stats.ram_budget_bytes,
            manifest_version: stats.manifest_version,
            segments: stats.segments,
            records: stats.records,
            segment_bytes: stats.segment_bytes,
            graph_bytes: stats.graph_bytes,
            resident_bytes_estimate: stats.resident_bytes_estimate,
        }
    }
}

impl From<SearchHit> for PyHit {
    fn from(hit: SearchHit) -> Self {
        Self {
            id: hit.id,
            distance: hit.distance,
            payload_ref: hit.payload_ref,
        }
    }
}

impl From<SearchReport> for PySearchReport {
    fn from(report: SearchReport) -> Self {
        Self {
            hits: report.hits.into_iter().map(PyHit::from).collect(),
            segments_total: report.segments_total,
            segments_searched: report.segments_searched,
            segments_skipped: report.segments_skipped,
            bytes_read: report.bytes_read,
            graph_bytes_read: report.graph_bytes_read,
            object_cache_hits: report.object_cache_hits,
            object_cache_misses: report.object_cache_misses,
            records_considered: report.records_considered,
            records_scored: report.records_scored,
            graph_candidates_added: report.graph_candidates_added,
            resident_bytes_estimate: report.resident_bytes_estimate,
            elapsed_ms: report.elapsed_ms,
        }
    }
}

impl From<CompactionReport> for PyCompactionReport {
    fn from(report: CompactionReport) -> Self {
        Self {
            compacted: report.compacted,
            source_level: report.source_level,
            target_level: report.target_level,
            segments_read: report.segments_read,
            segments_written: report.segments_written,
            records_rewritten: report.records_rewritten,
            bytes_read: report.bytes_read,
            bytes_written: report.bytes_written,
            object_cache_hits: report.object_cache_hits,
            object_cache_misses: report.object_cache_misses,
            manifest_version: report.manifest_version,
        }
    }
}

impl From<GarbageCollectionReport> for PyGarbageCollectionReport {
    fn from(report: GarbageCollectionReport) -> Self {
        Self {
            dry_run: report.dry_run,
            objects_scanned: report.objects_scanned,
            objects_deleted: report.objects_deleted,
            bytes_reclaimable: report.bytes_reclaimable,
            bytes_reclaimed: report.bytes_reclaimed,
            candidates: report.candidates,
        }
    }
}
