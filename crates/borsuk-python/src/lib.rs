//! Native Python bindings for BORSUK.

use std::{path::PathBuf, sync::Mutex, time::Duration};

use borsuk::{
    AddReport, BorsukIndex, CompactionOptions, CompactionReport, DEFAULT_COMPACTION_MAX_SEGMENTS,
    DeleteReport, Fusion, GarbageCollectionOptions, GarbageCollectionReport, HybridOptions,
    HybridQuery, IncrementalMaintenanceOptions, IncrementalReport, IndexConfig, IndexStats,
    LeafMode, OpenOptions, PurgeReport, RebuildOptions, RebuildReport, RequestCounts, SearchHit,
    SearchMode, SearchOptions, SearchReport, VectorMetric, VectorRecord,
};
use pyo3::{
    buffer::PyBuffer,
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

use borsuk::{Filter, MetaValue, Metadata};
use pyo3::types::{
    PyAnyMethods, PyBool, PyBoolMethods, PyDict, PyDictMethods, PyList, PyListMethods,
    PyTypeMethods,
};

/// A listed record as `(id, vector, metadata)` for the Python `list_records` API.
type PyRecordEntry = (String, Vec<f32>, Py<PyAny>);
type PySparseEntry = Option<(Vec<u32>, Vec<f32>)>;

/// Convert a Python value into a typed `MetaValue`. `bool` is checked before
/// `int` (Python bools are ints), and `datetime` objects become epoch-ms
/// timestamps.
fn py_to_metavalue(value: &Bound<'_, PyAny>) -> PyResult<MetaValue> {
    if value.is_none() {
        return Ok(MetaValue::Null);
    }
    if let Ok(b) = value.cast::<PyBool>() {
        return Ok(MetaValue::Bool(b.is_true()));
    }
    if let Some(ms) = datetime_to_epoch_ms(value)? {
        return Ok(MetaValue::Timestamp(ms));
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(MetaValue::Int(i));
    }
    if let Ok(f) = value.extract::<f64>() {
        return Ok(MetaValue::Float(f));
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(MetaValue::Str(s));
    }
    if let Ok(list) = value.cast::<PyList>() {
        return Ok(MetaValue::List(
            list.iter()
                .map(|item| py_to_metavalue(&item))
                .collect::<PyResult<_>>()?,
        ));
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        return Ok(MetaValue::Map(py_to_metadata(dict)?));
    }
    Err(PyValueError::new_err(
        "unsupported metadata value; expected None/bool/int/float/str/datetime/list/dict",
    ))
}

fn datetime_to_epoch_ms(value: &Bound<'_, PyAny>) -> PyResult<Option<i64>> {
    let type_name = value.get_type().name()?;
    if type_name == "datetime" && value.hasattr("timestamp")? {
        let seconds: f64 = value.call_method0("timestamp")?.extract()?;
        return Ok(Some((seconds * 1000.0) as i64));
    }
    Ok(None)
}

/// Convert an optional per-record list of metadata dicts, checking the count
/// matches the number of vectors.
fn convert_metadata_list(
    metadata: Option<&[Bound<'_, PyAny>]>,
    expected: usize,
) -> PyResult<Option<Vec<Metadata>>> {
    let Some(rows) = metadata else {
        return Ok(None);
    };
    if rows.len() != expected {
        return Err(PyValueError::new_err(format!(
            "metadata length {} must match vectors length {expected}",
            rows.len()
        )));
    }
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let dict = row
            .cast::<PyDict>()
            .map_err(|_| PyValueError::new_err("each metadata entry must be a dict"))?;
        out.push(py_to_metadata(dict)?);
    }
    Ok(Some(out))
}

fn convert_sparse_list(
    sparse: Option<&[PySparseEntry]>,
    expected: usize,
) -> PyResult<Option<Vec<PySparseEntry>>> {
    let Some(rows) = sparse else {
        return Ok(None);
    };
    if rows.len() != expected {
        return Err(PyValueError::new_err(format!(
            "sparse length {} must match vectors length {expected}",
            rows.len()
        )));
    }
    Ok(Some(rows.to_vec()))
}

fn convert_text_list(
    text: Option<&[Option<String>]>,
    expected: usize,
) -> PyResult<Option<Vec<Option<String>>>> {
    let Some(rows) = text else {
        return Ok(None);
    };
    if rows.len() != expected {
        return Err(PyValueError::new_err(format!(
            "text length {} must match vectors length {expected}",
            rows.len()
        )));
    }
    Ok(Some(rows.to_vec()))
}

fn py_to_metadata(dict: &Bound<'_, PyDict>) -> PyResult<Metadata> {
    let mut metadata = Metadata::new();
    for (key, value) in dict.iter() {
        let key: String = key
            .extract()
            .map_err(|_| PyValueError::new_err("metadata keys must be strings"))?;
        metadata.insert(key, py_to_metavalue(&value)?);
    }
    Ok(metadata)
}

fn metavalue_to_py(py: Python<'_>, value: &MetaValue) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObjectExt;
    Ok(match value {
        MetaValue::Null => py.None(),
        MetaValue::Bool(b) => b.into_py_any(py)?,
        MetaValue::Int(i) | MetaValue::Timestamp(i) => i.into_py_any(py)?,
        MetaValue::Float(f) => f.into_py_any(py)?,
        MetaValue::Str(s) => s.as_str().into_py_any(py)?,
        MetaValue::List(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(metavalue_to_py(py, item)?)?;
            }
            list.into_py_any(py)?
        }
        MetaValue::Map(map) => metadata_to_py(py, map)?,
    })
}

fn metadata_to_py(py: Python<'_>, metadata: &Metadata) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObjectExt;
    let dict = PyDict::new(py);
    for (key, value) in metadata {
        dict.set_item(key, metavalue_to_py(py, value)?)?;
    }
    dict.into_py_any(py)
}

/// Parse a Pinecone-style filter dict into a `Filter` via JSON. Values keep
/// their JSON kinds (integral numbers become `Int`); timestamp operands can be
/// passed as epoch numbers.
fn py_to_filter(value: &Bound<'_, PyAny>) -> PyResult<Filter> {
    Filter::from_json(&py_to_json(value)?).map_err(to_py_error)
}

fn py_to_json(value: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if value.is_none() {
        return Ok(serde_json::Value::Null);
    }
    if let Ok(b) = value.cast::<PyBool>() {
        return Ok(serde_json::Value::Bool(b.is_true()));
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(serde_json::Value::from(i));
    }
    if let Ok(f) = value.extract::<f64>() {
        return Ok(serde_json::Value::from(f));
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(serde_json::Value::String(s));
    }
    if let Ok(list) = value.cast::<PyList>() {
        return Ok(serde_json::Value::Array(
            list.iter()
                .map(|item| py_to_json(&item))
                .collect::<PyResult<_>>()?,
        ));
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (key, item) in dict.iter() {
            map.insert(key.extract()?, py_to_json(&item)?);
        }
        return Ok(serde_json::Value::Object(map));
    }
    Err(PyValueError::new_err("unsupported value in filter"))
}

#[pyclass(name = "Hit", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyHit {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    id_bytes: Vec<u8>,
    #[pyo3(get)]
    distance: f32,
    metadata: Option<Metadata>,
}

#[pymethods]
impl PyHit {
    fn __repr__(&self) -> String {
        format!("Hit(id={:?}, distance={})", self.id, self.distance)
    }

    /// The hit's metadata dict, present only when `include_metadata=True`.
    #[getter]
    fn metadata(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.metadata
            .as_ref()
            .map(|metadata| metadata_to_py(py, metadata))
            .transpose()
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
    text: bool,
    #[pyo3(get)]
    sparse_encoded_vectors: usize,
    #[pyo3(get)]
    dense_encoded_vectors: usize,
    #[pyo3(get)]
    manifest_version: u64,
    #[pyo3(get)]
    routing_max_level: u8,
    #[pyo3(get)]
    routing_page_fanout: usize,
    #[pyo3(get)]
    routing_leaf_pages: usize,
    #[pyo3(get)]
    routing_pages: usize,
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
            "IndexStats(metric={:?}, dimensions={}, segment_max_vectors={}, ram_budget_bytes={:?}, text={}, sparse_encoded_vectors={}, dense_encoded_vectors={}, manifest_version={}, routing_max_level={}, routing_page_fanout={}, routing_leaf_pages={}, routing_pages={}, segments={}, records={}, segment_bytes={}, graph_bytes={}, resident_bytes_estimate={})",
            self.metric,
            self.dimensions,
            self.segment_max_vectors,
            self.ram_budget_bytes,
            self.text,
            self.sparse_encoded_vectors,
            self.dense_encoded_vectors,
            self.manifest_version,
            self.routing_max_level,
            self.routing_page_fanout,
            self.routing_leaf_pages,
            self.routing_pages,
            self.segments,
            self.records,
            self.segment_bytes,
            self.graph_bytes,
            self.resident_bytes_estimate
        )
    }
}

#[pyclass(name = "RequestCounts", frozen, skip_from_py_object)]
#[derive(Clone, Copy)]
struct PyRequestCounts {
    #[pyo3(get)]
    gets: u64,
    #[pyo3(get)]
    puts: u64,
    #[pyo3(get)]
    deletes: u64,
    #[pyo3(get)]
    heads: u64,
    #[pyo3(get)]
    lists: u64,
    #[pyo3(get)]
    total: u64,
}

#[pymethods]
impl PyRequestCounts {
    fn __repr__(&self) -> String {
        format!(
            "RequestCounts(gets={}, puts={}, deletes={}, heads={}, lists={}, total={})",
            self.gets, self.puts, self.deletes, self.heads, self.lists, self.total
        )
    }
}

impl From<RequestCounts> for PyRequestCounts {
    fn from(counts: RequestCounts) -> Self {
        Self {
            gets: counts.gets,
            puts: counts.puts,
            deletes: counts.deletes,
            heads: counts.heads,
            lists: counts.lists,
            total: counts.total(),
        }
    }
}

#[pyclass(name = "AddReport", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyAddReport {
    #[pyo3(get)]
    segments_written: usize,
    #[pyo3(get)]
    graph_payloads_written: usize,
    #[pyo3(get)]
    manifest_tables_written: usize,
    #[pyo3(get)]
    routing_pages_written: usize,
    #[pyo3(get)]
    total_bytes_written: u64,
    #[pyo3(get)]
    bytes_per_vector: f64,
    #[pyo3(get)]
    requests: PyRequestCounts,
}

#[pymethods]
impl PyAddReport {
    fn __repr__(&self) -> String {
        format!(
            "AddReport(segments_written={}, graph_payloads_written={}, manifest_tables_written={}, routing_pages_written={}, total_bytes_written={}, bytes_per_vector={}, requests={})",
            self.segments_written,
            self.graph_payloads_written,
            self.manifest_tables_written,
            self.routing_pages_written,
            self.total_bytes_written,
            self.bytes_per_vector,
            self.requests.__repr__()
        )
    }
}

#[pyclass(name = "DeleteReport", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyDeleteReport {
    #[pyo3(get)]
    deleted: usize,
    #[pyo3(get)]
    total_tombstoned: usize,
    #[pyo3(get)]
    published: bool,
    #[pyo3(get)]
    requests: PyRequestCounts,
}

#[pymethods]
impl PyDeleteReport {
    fn __repr__(&self) -> String {
        format!(
            "DeleteReport(deleted={}, total_tombstoned={}, published={}, requests={})",
            self.deleted,
            self.total_tombstoned,
            self.published,
            self.requests.__repr__()
        )
    }
}

impl From<DeleteReport> for PyDeleteReport {
    fn from(report: DeleteReport) -> Self {
        Self {
            deleted: report.deleted,
            total_tombstoned: report.total_tombstoned,
            published: report.published,
            requests: report.requests.into(),
        }
    }
}

#[pyclass(name = "PurgeReport", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyPurgeReport {
    #[pyo3(get)]
    segments_rewritten: usize,
    #[pyo3(get)]
    records_purged: usize,
    #[pyo3(get)]
    tombstones_cleared: usize,
    #[pyo3(get)]
    published: bool,
    #[pyo3(get)]
    requests: PyRequestCounts,
}

#[pymethods]
impl PyPurgeReport {
    fn __repr__(&self) -> String {
        format!(
            "PurgeReport(segments_rewritten={}, records_purged={}, tombstones_cleared={}, published={}, requests={})",
            self.segments_rewritten,
            self.records_purged,
            self.tombstones_cleared,
            self.published,
            self.requests.__repr__()
        )
    }
}

impl From<PurgeReport> for PyPurgeReport {
    fn from(report: PurgeReport) -> Self {
        Self {
            segments_rewritten: report.segments_rewritten,
            records_purged: report.records_purged,
            tombstones_cleared: report.tombstones_cleared,
            published: report.published,
            requests: report.requests.into(),
        }
    }
}

#[pyclass(name = "IncrementalReport", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyIncrementalReport {
    #[pyo3(get)]
    splits: usize,
    #[pyo3(get)]
    merges: usize,
    #[pyo3(get)]
    segments_created: usize,
    #[pyo3(get)]
    segments_removed: usize,
    #[pyo3(get)]
    records_moved: usize,
    #[pyo3(get)]
    published: bool,
    #[pyo3(get)]
    requests: PyRequestCounts,
}

#[pymethods]
impl PyIncrementalReport {
    fn __repr__(&self) -> String {
        format!(
            "IncrementalReport(splits={}, merges={}, segments_created={}, segments_removed={}, records_moved={}, published={}, requests={})",
            self.splits,
            self.merges,
            self.segments_created,
            self.segments_removed,
            self.records_moved,
            self.published,
            self.requests.__repr__()
        )
    }
}

impl From<IncrementalReport> for PyIncrementalReport {
    fn from(report: IncrementalReport) -> Self {
        Self {
            splits: report.splits,
            merges: report.merges,
            segments_created: report.segments_created,
            segments_removed: report.segments_removed,
            records_moved: report.records_moved,
            published: report.published,
            requests: report.requests.into(),
        }
    }
}

#[pyclass(name = "SearchReport", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PySearchReport {
    #[pyo3(get)]
    hits: Vec<PyHit>,
    #[pyo3(get)]
    leaf_mode: String,
    #[pyo3(get)]
    termination_reason: String,
    #[pyo3(get)]
    recall_guarantee: String,
    #[pyo3(get)]
    segments_total: usize,
    #[pyo3(get)]
    segments_searched: usize,
    #[pyo3(get)]
    segments_skipped: usize,
    #[pyo3(get)]
    routing_page_indexes_read: usize,
    #[pyo3(get)]
    routing_pages_read: usize,
    #[pyo3(get)]
    bytes_read: u64,
    #[pyo3(get)]
    prefetched_bytes_unused: u64,
    #[pyo3(get)]
    graph_bytes_read: u64,
    #[pyo3(get)]
    object_cache_hits: usize,
    #[pyo3(get)]
    object_cache_misses: usize,
    #[pyo3(get)]
    cache_repairs: usize,
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
    #[pyo3(get)]
    requests: PyRequestCounts,
    #[pyo3(get)]
    rows_evaluated: usize,
    #[pyo3(get)]
    rows_passed_filter: usize,
    #[pyo3(get)]
    segments_pruned_by_filter: usize,
}

#[pymethods]
impl PySearchReport {
    fn __repr__(&self) -> String {
        format!(
            "SearchReport(hits={}, leaf_mode={:?}, termination_reason={:?}, recall_guarantee={:?}, segments_total={}, segments_searched={}, segments_skipped={}, routing_page_indexes_read={}, routing_pages_read={}, bytes_read={}, prefetched_bytes_unused={}, graph_bytes_read={}, object_cache_hits={}, object_cache_misses={}, cache_repairs={}, records_considered={}, records_scored={}, graph_candidates_added={}, resident_bytes_estimate={}, elapsed_ms={}, requests={})",
            self.hits.len(),
            self.leaf_mode,
            self.termination_reason,
            self.recall_guarantee,
            self.segments_total,
            self.segments_searched,
            self.segments_skipped,
            self.routing_page_indexes_read,
            self.routing_pages_read,
            self.bytes_read,
            self.prefetched_bytes_unused,
            self.graph_bytes_read,
            self.object_cache_hits,
            self.object_cache_misses,
            self.cache_repairs,
            self.records_considered,
            self.records_scored,
            self.graph_candidates_added,
            self.resident_bytes_estimate,
            self.elapsed_ms,
            self.requests.__repr__()
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
    routing_page_indexes_read: usize,
    #[pyo3(get)]
    routing_pages_read: usize,
    #[pyo3(get)]
    routing_page_indexes_written: usize,
    #[pyo3(get)]
    routing_pages_written: usize,
    #[pyo3(get)]
    graph_payloads_read: usize,
    #[pyo3(get)]
    graph_bytes_read: u64,
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
            "CompactionReport(compacted={}, source_level={}, target_level={}, segments_read={}, segments_written={}, records_rewritten={}, routing_page_indexes_read={}, routing_pages_read={}, routing_page_indexes_written={}, routing_pages_written={}, graph_payloads_read={}, graph_bytes_read={}, bytes_read={}, bytes_written={}, object_cache_hits={}, object_cache_misses={}, manifest_version={})",
            self.compacted,
            self.source_level,
            self.target_level,
            self.segments_read,
            self.segments_written,
            self.records_rewritten,
            self.routing_page_indexes_read,
            self.routing_pages_read,
            self.routing_page_indexes_written,
            self.routing_pages_written,
            self.graph_payloads_read,
            self.graph_bytes_read,
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
    routing_objects_deleted: usize,
    #[pyo3(get)]
    tables_deleted: usize,
    #[pyo3(get)]
    routing_page_indexes_read: usize,
    #[pyo3(get)]
    routing_pages_read: usize,
    #[pyo3(get)]
    bytes_read: u64,
    #[pyo3(get)]
    bytes_reclaimable: u64,
    #[pyo3(get)]
    bytes_reclaimed: u64,
    #[pyo3(get)]
    object_cache_hits: usize,
    #[pyo3(get)]
    object_cache_misses: usize,
    #[pyo3(get)]
    candidates: Vec<String>,
}

#[pymethods]
impl PyGarbageCollectionReport {
    fn __repr__(&self) -> String {
        format!(
            "GarbageCollectionReport(dry_run={}, objects_scanned={}, objects_deleted={}, routing_objects_deleted={}, tables_deleted={}, routing_page_indexes_read={}, routing_pages_read={}, bytes_read={}, bytes_reclaimable={}, bytes_reclaimed={}, object_cache_hits={}, object_cache_misses={}, candidates={})",
            self.dry_run,
            self.objects_scanned,
            self.objects_deleted,
            self.routing_objects_deleted,
            self.tables_deleted,
            self.routing_page_indexes_read,
            self.routing_pages_read,
            self.bytes_read,
            self.bytes_reclaimable,
            self.bytes_reclaimed,
            self.object_cache_hits,
            self.object_cache_misses,
            self.candidates.len()
        )
    }
}

#[pyclass(name = "RebuildReport", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyRebuildReport {
    #[pyo3(get)]
    compaction: PyCompactionReport,
    #[pyo3(get)]
    garbage_collection: PyGarbageCollectionReport,
}

#[pymethods]
impl PyRebuildReport {
    fn __repr__(&self) -> String {
        format!(
            "RebuildReport(compaction={}, garbage_collection={})",
            self.compaction.__repr__(),
            self.garbage_collection.__repr__()
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
        open(uri, None, None, true, None)
    }

    #[pyo3(signature = (vectors, ids = None, metadata = None, sparse = None, text = None))]
    fn add(
        &self,
        vectors: Vec<Vec<f32>>,
        ids: Option<Vec<String>>,
        metadata: Option<Vec<Bound<'_, PyAny>>>,
        sparse: Option<Vec<PySparseEntry>>,
        text: Option<Vec<Option<String>>>,
    ) -> PyResult<Vec<String>> {
        let metadata = convert_metadata_list(metadata.as_deref(), vectors.len())?;
        let sparse = convert_sparse_list(sparse.as_deref(), vectors.len())?;
        let text = convert_text_list(text.as_deref(), vectors.len())?;
        let mut index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        match ids {
            Some(ids) => {
                let ids = ids_for_vectors(Some(ids), vectors.len(), &index)?;
                let records = records_from_vectors(
                    &ids,
                    vectors,
                    index.manifest().config.dimensions,
                    metadata.as_deref(),
                    sparse.as_deref(),
                    text.as_deref(),
                )?;

                index.add(records).map_err(to_py_error)?;
                Ok(ids)
            }
            None => {
                if metadata.is_some() {
                    return Err(PyValueError::new_err(
                        "metadata requires explicit ids; pass ids=[...] alongside metadata=[...]",
                    ));
                }
                if sparse.is_some() || text.is_some() {
                    let ids = ids_for_vectors(None, vectors.len(), &index)?;
                    let records = records_from_vectors(
                        &ids,
                        vectors,
                        index.manifest().config.dimensions,
                        None,
                        sparse.as_deref(),
                        text.as_deref(),
                    )?;

                    index.add(records).map_err(to_py_error)?;
                    return Ok(ids);
                }
                index.add_vectors(vectors).map_err(to_py_error)
            }
        }
    }

    #[pyo3(signature = (vectors, ids = None))]
    fn add_with_report(
        &self,
        vectors: Vec<Vec<f32>>,
        ids: Option<Vec<String>>,
    ) -> PyResult<(Vec<String>, PyAddReport)> {
        let mut index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let (ids, report) = index.add_with_report(vectors, ids).map_err(to_py_error)?;
        Ok((ids, report.into()))
    }

    fn add_id_bytes(&self, vectors: Vec<Vec<f32>>, ids: Vec<Vec<u8>>) -> PyResult<Vec<Vec<u8>>> {
        let mut index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let ids = id_bytes_for_vectors(ids, vectors.len())?;
        let records = ids
            .iter()
            .cloned()
            .zip(vectors)
            .map(|(id, vector)| VectorRecord::new_bytes(id, vector))
            .collect::<Vec<_>>();

        index.add(records).map_err(to_py_error)?;
        Ok(ids)
    }

    #[pyo3(signature = (vectors, ids = None))]
    fn add_buffer(
        &self,
        py: Python<'_>,
        vectors: PyBuffer<f32>,
        ids: Option<Vec<String>>,
    ) -> PyResult<Vec<String>> {
        let flat = vectors.to_vec(py)?;
        let mut index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let row_count = flat_vector_row_count(&flat, dimensions)?;
        match ids {
            Some(ids) => {
                let ids = ids_for_vectors(Some(ids), row_count, &index)?;
                let records = records_from_flat_vectors(ids, &flat, dimensions)?;
                let ids = records
                    .iter()
                    .map(|record| record.id.to_utf8_string().map_err(to_py_error))
                    .collect::<PyResult<Vec<_>>>()?;

                index.add(records).map_err(to_py_error)?;
                Ok(ids)
            }
            None => index
                .add_vectors(vectors_from_flat_rows(&flat, dimensions, "vector buffer")?)
                .map_err(to_py_error),
        }
    }

    fn add_buffer_id_bytes(
        &self,
        py: Python<'_>,
        vectors: PyBuffer<f32>,
        ids: Vec<Vec<u8>>,
    ) -> PyResult<Vec<Vec<u8>>> {
        let flat = vectors.to_vec(py)?;
        let mut index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let row_count = flat_vector_row_count(&flat, dimensions)?;
        let ids = id_bytes_for_vectors(ids, row_count)?;
        let records = records_from_flat_vectors_with_id_bytes(ids.clone(), &flat, dimensions)?;

        index.add(records).map_err(to_py_error)?;
        Ok(ids)
    }

    fn stats(&self) -> PyResult<PyIndexStats> {
        let stats = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .try_stats()
            .map_err(to_py_error)?;

        Ok(stats.into())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None, filter = None))]
    fn search_ids(
        &self,
        query: Vec<f32>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
        filter: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Vec<String>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let filter = filter.as_ref().map(py_to_filter).transpose()?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;

        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_ids(
                &query,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_id_bytes(
        &self,
        query: Vec<f32>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<Vec<u8>>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;

        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_id_bytes(
                &query,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_vectors(
        &self,
        query: Vec<f32>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<Vec<f32>>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;

        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_vectors(
                &query,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)
    }

    fn get_vector_by_id(&self, id: Vec<u8>) -> PyResult<Option<Vec<f32>>> {
        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .get_vector_by_id(id)
            .map_err(to_py_error)
    }

    fn get_vector(&self, id: &str) -> PyResult<Option<Vec<f32>>> {
        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .get_vector(id)
            .map_err(to_py_error)
    }

    /// Load a stored vector and its metadata dict by id, or `None`.
    fn get_record(&self, py: Python<'_>, id: &str) -> PyResult<Option<(Vec<f32>, Py<PyAny>)>> {
        let record = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .get_record(id)
            .map_err(to_py_error)?;
        match record {
            Some((vector, metadata)) => Ok(Some((vector, metadata_to_py(py, &metadata)?))),
            None => Ok(None),
        }
    }

    /// List up to `limit` live records starting at `offset`, as
    /// `(id, vector, metadata)` tuples. Backs "scroll"/"get all" in the
    /// drop-in adapters; scans segment payloads, so it is an export path.
    #[pyo3(signature = (offset = 0, limit = 100))]
    fn list_records(
        &self,
        py: Python<'_>,
        offset: usize,
        limit: usize,
    ) -> PyResult<Vec<PyRecordEntry>> {
        let records = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .list_records(offset, limit)
            .map_err(to_py_error)?;
        records
            .into_iter()
            .map(|(id, vector, metadata)| {
                Ok((id.to_string(), vector, metadata_to_py(py, &metadata)?))
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_ids_buffer(
        &self,
        py: Python<'_>,
        query: PyBuffer<f32>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<String>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        let flat = query.to_vec(py)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let query = query_from_flat_vector(&flat, dimensions, "query buffer")?;
        index
            .search_ids(
                &query,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_id_bytes_buffer(
        &self,
        py: Python<'_>,
        query: PyBuffer<f32>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<Vec<u8>>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        let flat = query.to_vec(py)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let query = query_from_flat_vector(&flat, dimensions, "query buffer")?;
        index
            .search_id_bytes(
                &query,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_vectors_buffer(
        &self,
        py: Python<'_>,
        query: PyBuffer<f32>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<Vec<f32>>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        let flat = query.to_vec(py)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let query = query_from_flat_vector(&flat, dimensions, "query buffer")?;
        index
            .search_vectors(
                &query,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_with_report_buffer(
        &self,
        py: Python<'_>,
        query: PyBuffer<f32>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<PySearchReport> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        let flat = query.to_vec(py)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let query = query_from_flat_vector(&flat, dimensions, "query buffer")?;
        let report = index
            .search_with_report(
                &query,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)?;

        report.try_into()
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queries, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_ids_batch(
        &self,
        queries: Vec<Vec<f32>>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<Vec<String>>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_ids_batch(
                &queries,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queries, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_id_bytes_batch(
        &self,
        queries: Vec<Vec<f32>>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<Vec<Vec<u8>>>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_id_bytes_batch(
                &queries,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queries, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_vectors_batch(
        &self,
        queries: Vec<Vec<f32>>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<Vec<Vec<f32>>>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_vectors_batch(
                &queries,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queries, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_id_bytes_batch_buffer(
        &self,
        py: Python<'_>,
        queries: PyBuffer<f32>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<Vec<Vec<u8>>>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        let flat = queries.to_vec(py)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let queries = vectors_from_flat_rows(&flat, dimensions, "query buffer")?;
        index
            .search_id_bytes_batch(
                &queries,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queries, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_ids_batch_buffer(
        &self,
        py: Python<'_>,
        queries: PyBuffer<f32>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<Vec<String>>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        let flat = queries.to_vec(py)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let queries = vectors_from_flat_rows(&flat, dimensions, "query buffer")?;
        index
            .search_ids_batch(
                &queries,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queries, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_vectors_batch_buffer(
        &self,
        py: Python<'_>,
        queries: PyBuffer<f32>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<Vec<Vec<f32>>>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        let flat = queries.to_vec(py)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let queries = vectors_from_flat_rows(&flat, dimensions, "query buffer")?;
        index
            .search_vectors_batch(
                &queries,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queries, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_batch_with_report(
        &self,
        queries: Vec<Vec<f32>>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<PySearchReport>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        let reports = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_batch_with_report(
                &queries,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)?;

        reports
            .into_iter()
            .map(PySearchReport::try_from)
            .collect::<PyResult<Vec<_>>>()
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queries, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None))]
    fn search_batch_with_report_buffer(
        &self,
        py: Python<'_>,
        queries: PyBuffer<f32>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
    ) -> PyResult<Vec<PySearchReport>> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        let flat = queries.to_vec(py)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let queries = vectors_from_flat_rows(&flat, dimensions, "query buffer")?;
        let reports = index
            .search_batch_with_report(
                &queries,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter: None,
                    include_metadata: false,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)?;

        reports
            .into_iter()
            .map(PySearchReport::try_from)
            .collect::<PyResult<Vec<_>>>()
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, k = 10, mode = "exact", leaf_mode = "graph", eps = None, max_segments = None, max_bytes = None, max_latency_ms = None, routing_page_overfetch = None, max_candidates_per_segment = None, guaranteed_recall = false, prefetch_depth = None, filter = None, include_metadata = false))]
    fn search_with_report(
        &self,
        query: Vec<f32>,
        k: usize,
        mode: &str,
        leaf_mode: &str,
        eps: Option<f32>,
        max_segments: Option<usize>,
        max_bytes: Option<Bound<'_, PyAny>>,
        max_latency_ms: Option<u64>,
        routing_page_overfetch: Option<usize>,
        max_candidates_per_segment: Option<usize>,
        guaranteed_recall: bool,
        prefetch_depth: Option<usize>,
        filter: Option<Bound<'_, PyAny>>,
        include_metadata: bool,
    ) -> PyResult<PySearchReport> {
        let max_bytes = parse_optional_byte_size(max_bytes.as_ref(), "max_bytes")?;
        let filter = filter.as_ref().map(py_to_filter).transpose()?;
        let mode = parse_mode(
            mode,
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
        )?;
        let report = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_with_report(
                &query,
                SearchOptions {
                    k,
                    mode,
                    guaranteed_recall,
                    prefetch_depth: prefetch_depth.unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
                    filter,
                    include_metadata,
                    vector_name: String::new(),
                },
            )
            .map_err(to_py_error)?;

        report.try_into()
    }

    #[pyo3(signature = (text, k = 10))]
    fn search_text(&self, text: &str, k: usize) -> PyResult<Vec<String>> {
        let report = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_text(text, k)
            .map_err(to_py_error)?;

        search_report_ids(report)
    }

    #[pyo3(signature = (text, k = 10, include_metadata = false))]
    fn search_text_with_report(
        &self,
        text: &str,
        k: usize,
        include_metadata: bool,
    ) -> PyResult<PySearchReport> {
        let index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let report = index.search_text(text, k).map_err(to_py_error)?;

        search_report_with_optional_metadata(&index, report, include_metadata)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, dense = None, text = None, k = 10, fusion = "rrf", rrf_k = 60, weights = None))]
    fn search_hybrid(
        &self,
        dense: Option<Vec<f32>>,
        text: Option<String>,
        k: usize,
        fusion: &str,
        rrf_k: usize,
        weights: Option<(f32, f32)>,
    ) -> PyResult<Vec<String>> {
        let query = hybrid_query(dense, text);
        let options = hybrid_options(k, fusion, rrf_k, weights)?;
        let report = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .search_hybrid(&query, options)
            .map_err(to_py_error)?;

        search_report_ids(report)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, dense = None, text = None, k = 10, fusion = "rrf", rrf_k = 60, weights = None, include_metadata = false))]
    fn search_hybrid_with_report(
        &self,
        dense: Option<Vec<f32>>,
        text: Option<String>,
        k: usize,
        fusion: &str,
        rrf_k: usize,
        weights: Option<(f32, f32)>,
        include_metadata: bool,
    ) -> PyResult<PySearchReport> {
        let query = hybrid_query(dense, text);
        let options = hybrid_options(k, fusion, rrf_k, weights)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?;
        let report = index.search_hybrid(&query, options).map_err(to_py_error)?;

        search_report_with_optional_metadata(&index, report, include_metadata)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, source_level = 0, target_level = 1, max_segments = None, all_matching = false, min_segments = 2, target_segment_max_vectors = None, target_segment_max_radius = None))]
    fn compact(
        &self,
        source_level: u8,
        target_level: u8,
        max_segments: Option<usize>,
        all_matching: bool,
        min_segments: usize,
        target_segment_max_vectors: Option<usize>,
        target_segment_max_radius: Option<f32>,
    ) -> PyResult<PyCompactionReport> {
        if all_matching && max_segments.is_some() {
            return Err(PyValueError::new_err(
                "all_matching cannot be combined with max_segments",
            ));
        }
        let max_segments = if all_matching {
            None
        } else {
            Some(max_segments.unwrap_or(DEFAULT_COMPACTION_MAX_SEGMENTS))
        };
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
                target_segment_max_radius,
            })
            .map_err(to_py_error)?;

        Ok(report.into())
    }

    #[pyo3(signature = (*, source_level = 0, target_level = 1, min_segments = 1, target_segment_max_vectors = None, delete_obsolete = false))]
    fn rebuild(
        &self,
        source_level: u8,
        target_level: u8,
        min_segments: usize,
        target_segment_max_vectors: Option<usize>,
        delete_obsolete: bool,
    ) -> PyResult<PyRebuildReport> {
        let report = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .rebuild(RebuildOptions {
                source_level,
                target_level,
                min_segments,
                target_segment_max_vectors,
                delete_obsolete,
            })
            .map_err(to_py_error)?;

        Ok(report.into())
    }

    /// Logically delete records by id. Hidden from search immediately; reclaimed
    /// by compaction or purge.
    fn delete(&self, ids: Vec<String>) -> PyResult<PyDeleteReport> {
        let report = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .delete_with_report(ids)
            .map_err(to_py_error)?;
        Ok(report.into())
    }

    /// Physically remove deleted records and clear the tombstone, re-enabling
    /// those ids for add.
    fn purge(&self) -> PyResult<PyPurgeReport> {
        let report = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .purge_with_report()
            .map_err(to_py_error)?;
        Ok(report.into())
    }

    /// Run one incremental-maintenance pass: split oversized bubbles and merge
    /// sparse ones locally.
    #[pyo3(signature = (*, max_segment_vectors = None, max_segment_radius = None, min_segment_vectors = None, max_operations = None))]
    fn maintain(
        &self,
        max_segment_vectors: Option<usize>,
        max_segment_radius: Option<f32>,
        min_segment_vectors: Option<usize>,
        max_operations: Option<usize>,
    ) -> PyResult<PyIncrementalReport> {
        let defaults = IncrementalMaintenanceOptions::default();
        let options = IncrementalMaintenanceOptions {
            max_segment_vectors: max_segment_vectors.unwrap_or(defaults.max_segment_vectors),
            max_segment_radius,
            min_segment_vectors: min_segment_vectors.unwrap_or(defaults.min_segment_vectors),
            max_operations: max_operations.unwrap_or(defaults.max_operations),
        };
        let report = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .run_incremental_maintenance(options)
            .map_err(to_py_error)?;
        Ok(report.into())
    }

    #[pyo3(signature = (*, dry_run = true, min_age_seconds = 86_400.0))]
    fn gc_obsolete_segments(
        &self,
        dry_run: bool,
        min_age_seconds: f64,
    ) -> PyResult<PyGarbageCollectionReport> {
        let report = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("index lock poisoned"))?
            .gc_obsolete_segments(GarbageCollectionOptions {
                dry_run,
                min_age: duration_from_seconds(min_age_seconds, "min_age_seconds")?,
            })
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
fn vector_metric_names() -> Vec<String> {
    borsuk::vector_metric_names()
        .iter()
        .map(|name| (*name).to_string())
        .collect()
}

#[pyfunction]
fn leaf_mode_names() -> Vec<String> {
    borsuk::leaf_mode_names()
        .iter()
        .map(|name| (*name).to_string())
        .collect()
}

#[pyfunction]
fn recall_at_k(exact_ids: Vec<String>, actual_ids: Vec<String>, k: usize) -> PyResult<f32> {
    borsuk::recall_at_k(&exact_ids, &actual_ids, k).map_err(to_py_value_error)
}

#[pyfunction]
fn tie_aware_recall_at_k(
    exact_distances: Vec<f32>,
    actual_distances: Vec<f32>,
    k: usize,
) -> PyResult<f32> {
    borsuk::tie_aware_recall_at_k(&exact_distances, &actual_distances, k).map_err(to_py_value_error)
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (*, uri, metric, dim = None, dimensions = None, segment_size = None, segment_max_vectors = None, routing_page_fanout = None, graph_neighbors = None, ram_budget = None, cache_dir = None, text = false))]
fn create(
    uri: String,
    metric: String,
    dim: Option<usize>,
    dimensions: Option<usize>,
    segment_size: Option<usize>,
    segment_max_vectors: Option<usize>,
    routing_page_fanout: Option<usize>,
    graph_neighbors: Option<usize>,
    ram_budget: Option<String>,
    cache_dir: Option<String>,
    text: bool,
) -> PyResult<PyIndex> {
    let dimensions = resolve_dimensions(dim, dimensions)?;
    let segment_max_vectors = resolve_segment_max_vectors(segment_size, segment_max_vectors)?;
    let metric = metric.parse::<VectorMetric>().map_err(to_py_error)?;
    let ram_budget_bytes = ram_budget
        .as_deref()
        .map(borsuk::parse_ram_budget)
        .transpose()
        .map_err(to_py_value_error)?;
    let index = BorsukIndex::create_with_cache_routing_page_fanout_and_graph_neighbors(
        IndexConfig {
            uri,
            metric,
            dimensions,
            segment_max_vectors,
            ram_budget_bytes,
            text,
            named_vectors: Default::default(),
        },
        cache_dir.map(PathBuf::from),
        routing_page_fanout.unwrap_or(borsuk::DEFAULT_ROUTING_PAGE_FANOUT),
        graph_neighbors.unwrap_or(borsuk::DEFAULT_GRAPH_NEIGHBORS),
    )
    .map_err(to_py_error)?;

    Ok(PyIndex {
        inner: Mutex::new(index),
    })
}

#[pyfunction]
#[pyo3(signature = (uri, cache_dir = None, ram_budget = None, resident_routing = false, cache_max_bytes = None))]
#[pyo3(name = "open")]
fn open_py(
    uri: String,
    cache_dir: Option<String>,
    ram_budget: Option<String>,
    resident_routing: bool,
    cache_max_bytes: Option<String>,
) -> PyResult<PyIndex> {
    open(
        uri,
        cache_dir,
        ram_budget,
        resident_routing,
        cache_max_bytes,
    )
}

#[pymodule]
fn _borsuk(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("BorsukError", module.py().get_type::<BorsukError>())?;
    module.add_class::<PyCompactionReport>()?;
    module.add_class::<PyGarbageCollectionReport>()?;
    module.add_class::<PyRebuildReport>()?;
    module.add_class::<PyDeleteReport>()?;
    module.add_class::<PyPurgeReport>()?;
    module.add_class::<PyIncrementalReport>()?;
    module.add_class::<PyHit>()?;
    module.add_class::<PyIndexStats>()?;
    module.add_class::<PyAddReport>()?;
    module.add_class::<PySearchReport>()?;
    module.add_class::<PyRequestCounts>()?;
    module.add_class::<PyIndex>()?;
    module.add_function(wrap_pyfunction!(create, module)?)?;
    module.add_function(wrap_pyfunction!(open_py, module)?)?;
    module.add_function(wrap_pyfunction!(leaf_mode_names, module)?)?;
    module.add_function(wrap_pyfunction!(recall_at_k, module)?)?;
    module.add_function(wrap_pyfunction!(tie_aware_recall_at_k, module)?)?;
    module.add_function(wrap_pyfunction!(vector_distance, module)?)?;
    module.add_function(wrap_pyfunction!(vector_metric_names, module)?)?;
    Ok(())
}

fn open(
    uri: String,
    cache_dir: Option<String>,
    ram_budget: Option<String>,
    resident_routing: bool,
    cache_max_bytes: Option<String>,
) -> PyResult<PyIndex> {
    let ram_budget_bytes = ram_budget
        .as_deref()
        .map(borsuk::parse_ram_budget)
        .transpose()
        .map_err(to_py_value_error)?;
    let cache_max_bytes = cache_max_bytes
        .as_deref()
        .map(|value| borsuk::parse_byte_size(value, "cache_max_bytes"))
        .transpose()
        .map_err(to_py_value_error)?;
    let index = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            cache_dir: cache_dir.map(PathBuf::from),
            cache_max_bytes,
            ram_budget_bytes,
            resident_routing,
            ..OpenOptions::default()
        },
    )
    .map_err(to_py_error)?;
    Ok(PyIndex {
        inner: Mutex::new(index),
    })
}

fn ids_for_vectors(
    ids: Option<Vec<String>>,
    expected_len: usize,
    index: &BorsukIndex,
) -> PyResult<Vec<String>> {
    match ids {
        Some(ids) if ids.len() != expected_len => Err(PyValueError::new_err(
            "ids must have the same length as vectors",
        )),
        Some(ids) => Ok(ids),
        None => index.generate_ids(expected_len).map_err(to_py_error),
    }
}

fn id_bytes_for_vectors(ids: Vec<Vec<u8>>, expected_len: usize) -> PyResult<Vec<Vec<u8>>> {
    if ids.len() != expected_len {
        return Err(PyValueError::new_err(
            "ids must have the same length as vectors",
        ));
    }
    Ok(ids)
}

fn records_from_vectors(
    ids: &[String],
    vectors: Vec<Vec<f32>>,
    dimensions: usize,
    metadata: Option<&[Metadata]>,
    sparse: Option<&[PySparseEntry]>,
    text: Option<&[Option<String>]>,
) -> PyResult<Vec<VectorRecord>> {
    ids.iter()
        .cloned()
        .zip(vectors)
        .enumerate()
        .map(|(row, (id, vector))| {
            let mut record = if let Some(rows) = sparse
                && let Some((indices, values)) = &rows[row]
            {
                VectorRecord::from_sparse(id, indices.clone(), values.clone(), dimensions)
                    .map_err(to_py_error)?
            } else {
                VectorRecord::new(id, vector)
            };
            if let Some(rows) = metadata {
                record = record.with_metadata(rows[row].clone());
            }
            if let Some(rows) = text
                && let Some(text) = &rows[row]
            {
                record = record.with_text(text.clone());
            }
            Ok(record)
        })
        .collect()
}

fn records_from_flat_vectors(
    ids: Vec<String>,
    vectors: &[f32],
    dimensions: usize,
) -> PyResult<Vec<VectorRecord>> {
    if !vectors.len().is_multiple_of(dimensions) {
        return Err(PyValueError::new_err(format!(
            "flat vector buffer length must be a multiple of index dimensions (dimensions {dimensions}, got {} float32 values)",
            vectors.len()
        )));
    }

    let expected_values = ids
        .len()
        .checked_mul(dimensions)
        .ok_or_else(|| PyValueError::new_err("flat vector buffer length exceeds platform usize"))?;
    if vectors.len() != expected_values {
        return Err(PyValueError::new_err(format!(
            "flat vector buffer length must equal ids length * index dimensions (expected {expected_values} float32 values, got {})",
            vectors.len()
        )));
    }

    Ok(ids
        .into_iter()
        .zip(vectors.chunks_exact(dimensions))
        .map(|(id, vector)| VectorRecord::new(id, vector.to_vec()))
        .collect())
}

fn records_from_flat_vectors_with_id_bytes(
    ids: Vec<Vec<u8>>,
    vectors: &[f32],
    dimensions: usize,
) -> PyResult<Vec<VectorRecord>> {
    if !vectors.len().is_multiple_of(dimensions) {
        return Err(PyValueError::new_err(format!(
            "flat vector buffer length must be a multiple of index dimensions (dimensions {dimensions}, got {} float32 values)",
            vectors.len()
        )));
    }

    let expected_values = ids
        .len()
        .checked_mul(dimensions)
        .ok_or_else(|| PyValueError::new_err("flat vector buffer length exceeds usize"))?;
    if vectors.len() != expected_values {
        return Err(PyValueError::new_err(format!(
            "flat vector buffer length must equal ids length * index dimensions (expected {expected_values} float32 values, got {})",
            vectors.len()
        )));
    }

    Ok(ids
        .into_iter()
        .zip(vectors.chunks_exact(dimensions))
        .map(|(id, vector)| VectorRecord::new_bytes(id, vector.to_vec()))
        .collect())
}

fn flat_vector_row_count(vectors: &[f32], dimensions: usize) -> PyResult<usize> {
    if !vectors.len().is_multiple_of(dimensions) {
        return Err(PyValueError::new_err(format!(
            "flat vector buffer length must be a multiple of index dimensions (dimensions {dimensions}, got {} float32 values)",
            vectors.len()
        )));
    }

    Ok(vectors.len() / dimensions)
}

fn vectors_from_flat_rows(
    vectors: &[f32],
    dimensions: usize,
    label: &str,
) -> PyResult<Vec<Vec<f32>>> {
    if dimensions == 0 {
        return Err(PyValueError::new_err(
            "index dimensions must be greater than zero",
        ));
    }
    if !vectors.len().is_multiple_of(dimensions) {
        return Err(PyValueError::new_err(format!(
            "flat {label} length must be a multiple of index dimensions ({dimensions}); got {} float32 values",
            vectors.len()
        )));
    }

    Ok(vectors
        .chunks_exact(dimensions)
        .map(<[f32]>::to_vec)
        .collect())
}

fn query_from_flat_vector(query: &[f32], dimensions: usize, label: &str) -> PyResult<Vec<f32>> {
    if query.len() != dimensions {
        return Err(PyValueError::new_err(format!(
            "flat {label} length must equal index dimensions ({dimensions}); got {} float32 values",
            query.len()
        )));
    }

    Ok(query.to_vec())
}

fn search_report_ids(report: SearchReport) -> PyResult<Vec<String>> {
    report
        .hits
        .into_iter()
        .map(|hit| hit.id.to_utf8_string().map_err(to_py_error))
        .collect()
}

fn search_report_with_optional_metadata(
    index: &BorsukIndex,
    mut report: SearchReport,
    include_metadata: bool,
) -> PyResult<PySearchReport> {
    if include_metadata {
        for hit in &mut report.hits {
            let id = hit.id.to_utf8_string().map_err(to_py_error)?;
            hit.metadata = index
                .get_record(&id)
                .map_err(to_py_error)?
                .map(|(_, metadata)| metadata);
        }
    }
    report.try_into()
}

fn hybrid_query(dense: Option<Vec<f32>>, text: Option<String>) -> HybridQuery {
    let mut query = HybridQuery::new();
    if let Some(dense) = dense {
        query = query.with_dense(dense);
    }
    if let Some(text) = text {
        query = query.with_text(text);
    }
    query
}

fn hybrid_options(
    k: usize,
    fusion: &str,
    rrf_k: usize,
    weights: Option<(f32, f32)>,
) -> PyResult<HybridOptions> {
    let mut options = HybridOptions::new(k);
    options.fusion = match fusion {
        "rrf" => Fusion::Rrf { k: rrf_k },
        "weighted" => {
            let (dense, text) = weights.unwrap_or((1.0, 1.0));
            Fusion::Weighted { dense, text }
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown hybrid fusion `{other}`; expected 'rrf' or 'weighted'"
            )));
        }
    };
    Ok(options)
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

fn resolve_segment_max_vectors(
    segment_size: Option<usize>,
    segment_max_vectors: Option<usize>,
) -> PyResult<usize> {
    match (segment_size, segment_max_vectors) {
        (Some(left), Some(right)) if left != right => Err(PyValueError::new_err(
            "segment_size and segment_max_vectors disagree",
        )),
        (Some(value), _) | (_, Some(value)) => Ok(value),
        (None, None) => Ok(4096),
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_mode(
    mode: &str,
    leaf_mode: &str,
    eps: Option<f32>,
    max_segments: Option<usize>,
    max_bytes: Option<u64>,
    max_latency_ms: Option<u64>,
    routing_page_overfetch: Option<usize>,
    max_candidates_per_segment: Option<usize>,
) -> PyResult<SearchMode> {
    match mode {
        "exact" => Ok(SearchMode::Exact),
        "approx" => Ok(SearchMode::Approx {
            leaf_mode: leaf_mode.parse::<LeafMode>().map_err(to_py_value_error)?,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
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

fn duration_from_seconds(value: f64, field_name: &str) -> PyResult<Duration> {
    if value.is_finite() && value >= 0.0 {
        Ok(Duration::from_secs_f64(value))
    } else {
        Err(PyValueError::new_err(format!(
            "{field_name} must be a non-negative finite number"
        )))
    }
}

fn to_py_error(error: borsuk::BorsukError) -> PyErr {
    let code = error.code();
    let message = error.to_string();
    Python::attach(|py| {
        let err = BorsukError::new_err(message);
        if let Err(setattr_error) = err.value(py).setattr("code", code) {
            return setattr_error;
        }
        err
    })
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
            text: stats.text,
            sparse_encoded_vectors: stats.sparse_encoded_vectors,
            dense_encoded_vectors: stats.dense_encoded_vectors,
            manifest_version: stats.manifest_version,
            routing_max_level: stats.routing_max_level,
            routing_page_fanout: stats.routing_page_fanout,
            routing_leaf_pages: stats.routing_leaf_pages,
            routing_pages: stats.routing_pages,
            segments: stats.segments,
            records: stats.records,
            segment_bytes: stats.segment_bytes,
            graph_bytes: stats.graph_bytes,
            resident_bytes_estimate: stats.resident_bytes_estimate,
        }
    }
}

impl From<AddReport> for PyAddReport {
    fn from(report: AddReport) -> Self {
        Self {
            segments_written: report.segments_written,
            graph_payloads_written: report.graph_payloads_written,
            manifest_tables_written: report.manifest_tables_written,
            routing_pages_written: report.routing_pages_written,
            total_bytes_written: report.total_bytes_written,
            bytes_per_vector: report.bytes_per_vector,
            requests: report.requests.into(),
        }
    }
}

impl TryFrom<SearchHit> for PyHit {
    type Error = PyErr;

    fn try_from(hit: SearchHit) -> PyResult<Self> {
        let id = hit
            .id
            .to_utf8_string()
            .unwrap_or_else(|_| hit.id.to_string());
        let id_bytes = hit.id.as_bytes().to_vec();
        Ok(Self {
            id,
            id_bytes,
            distance: hit.distance,
            metadata: hit.metadata,
        })
    }
}

impl TryFrom<SearchReport> for PySearchReport {
    type Error = PyErr;

    fn try_from(report: SearchReport) -> PyResult<Self> {
        let hits = report
            .hits
            .into_iter()
            .map(PyHit::try_from)
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            hits,
            leaf_mode: report.leaf_mode,
            termination_reason: report.termination_reason.to_string(),
            recall_guarantee: report.recall_guarantee.to_string(),
            segments_total: report.segments_total,
            segments_searched: report.segments_searched,
            segments_skipped: report.segments_skipped,
            routing_page_indexes_read: report.routing_page_indexes_read,
            routing_pages_read: report.routing_pages_read,
            bytes_read: report.bytes_read,
            prefetched_bytes_unused: report.prefetched_bytes_unused,
            graph_bytes_read: report.graph_bytes_read,
            object_cache_hits: report.object_cache_hits,
            object_cache_misses: report.object_cache_misses,
            cache_repairs: report.cache_repairs,
            records_considered: report.records_considered,
            records_scored: report.records_scored,
            graph_candidates_added: report.graph_candidates_added,
            resident_bytes_estimate: report.resident_bytes_estimate,
            elapsed_ms: report.elapsed_ms,
            requests: report.requests.into(),
            rows_evaluated: report.rows_evaluated,
            rows_passed_filter: report.rows_passed_filter,
            segments_pruned_by_filter: report.segments_pruned_by_filter,
        })
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
            routing_page_indexes_read: report.routing_page_indexes_read,
            routing_pages_read: report.routing_pages_read,
            routing_page_indexes_written: report.routing_page_indexes_written,
            routing_pages_written: report.routing_pages_written,
            graph_payloads_read: report.graph_payloads_read,
            graph_bytes_read: report.graph_bytes_read,
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
            routing_objects_deleted: report.routing_objects_deleted,
            tables_deleted: report.tables_deleted,
            routing_page_indexes_read: report.routing_page_indexes_read,
            routing_pages_read: report.routing_pages_read,
            bytes_read: report.bytes_read,
            bytes_reclaimable: report.bytes_reclaimable,
            bytes_reclaimed: report.bytes_reclaimed,
            object_cache_hits: report.object_cache_hits,
            object_cache_misses: report.object_cache_misses,
            candidates: report.candidates,
        }
    }
}

impl From<RebuildReport> for PyRebuildReport {
    fn from(report: RebuildReport) -> Self {
        Self {
            compaction: report.compaction.into(),
            garbage_collection: report.garbage_collection.into(),
        }
    }
}
