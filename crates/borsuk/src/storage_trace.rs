//! Format-independent persisted-object access classification and tracing.

use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::{Arc, Mutex, OnceLock},
};

use crate::{BorsukError, Result};

/// Logical role of one persisted object, independent of its physical codec.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PhysicalObjectRole {
    /// Collection pointer, manifest, metadata, or pivot catalog.
    Catalog,
    /// Immutable mutation journal run.
    WalRun,
    /// Conditional pointer for one logical cell WAL lane.
    LaneHead,
    /// Atomic visibility marker for a prepared transaction.
    CommitMarker,
    /// Hierarchical vector-routing metadata page.
    RoutingPage,
    /// Per-segment graph edge table for graph-enabled indexes.
    GraphIndex,
    /// Immutable row metadata and candidate-code segment.
    NormalSegment,
    /// Global or cell-local approximate product-code payload.
    ProductCodes,
    /// Typed lossless vector rerank payload.
    ExactVectors,
    /// Per-segment metadata-filter acceleration sidecar.
    FilterIndex,
    /// Sparse or BM25 term, postings, or row-metadata block.
    LexicalBlock,
    /// Persisted multi-vector entity/token payload.
    LateInteraction,
    /// Record-generation suppression frontier.
    Tombstone,
    /// Record-ID to logical-cell ownership mapping.
    IdDirectory,
    /// Unclassified object that must not be silently attributed.
    Unknown,
}

/// Classify a relative object path without relying on its file extension.
#[must_use]
pub fn physical_object_role_for_path(path: &str) -> PhysicalObjectRole {
    let path = path.trim_matches('/');
    if path == "collection/CURRENT"
        || path.starts_with("collection/snapshots/")
        || path.starts_with("manifests/")
        || path.starts_with("metadata/")
        || path.starts_with("pivots/")
    {
        PhysicalObjectRole::Catalog
    } else if path.starts_with("cells/") && path.ends_with("/HEAD") {
        PhysicalObjectRole::LaneHead
    } else if path.starts_with("cells/")
        && path.contains("/wal/")
        && path.contains("/runs/tombstones/")
    {
        PhysicalObjectRole::Tombstone
    } else if path.starts_with("cells/")
        && path.contains("/wal/")
        && path.contains("/runs/id-directory/")
    {
        PhysicalObjectRole::IdDirectory
    } else if path.starts_with("cells/") && path.contains("/wal/") && path.contains("/runs/") {
        PhysicalObjectRole::WalRun
    } else if path.starts_with("cells/") && path.contains("/wal/") && path.contains("/frontier/") {
        PhysicalObjectRole::LaneHead
    } else if path.starts_with("transactions/") {
        PhysicalObjectRole::CommitMarker
    } else if path.starts_with("wal/") {
        PhysicalObjectRole::WalRun
    } else if path.starts_with("routing/") {
        PhysicalObjectRole::RoutingPage
    } else if path.starts_with("graphs/") {
        PhysicalObjectRole::GraphIndex
    } else if path.starts_with("segments/") {
        PhysicalObjectRole::NormalSegment
    } else if path.starts_with("global-pq/") {
        PhysicalObjectRole::ProductCodes
    } else if path.starts_with("vectors/") {
        PhysicalObjectRole::ExactVectors
    } else if path.starts_with("fidx/") {
        PhysicalObjectRole::FilterIndex
    } else if path.starts_with("lexical/") {
        PhysicalObjectRole::LexicalBlock
    } else if path.starts_with("late-interaction/") {
        PhysicalObjectRole::LateInteraction
    } else if path.starts_with("tombstones/") {
        PhysicalObjectRole::Tombstone
    } else if path.starts_with("id-directory/") {
        PhysicalObjectRole::IdDirectory
    } else {
        PhysicalObjectRole::Unknown
    }
}

impl PhysicalObjectRole {
    /// Stable persisted and trace name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::WalRun => "wal_run",
            Self::LaneHead => "lane_head",
            Self::CommitMarker => "commit_marker",
            Self::RoutingPage => "routing_page",
            Self::GraphIndex => "graph_index",
            Self::NormalSegment => "normal_segment",
            Self::ProductCodes => "product_codes",
            Self::ExactVectors => "exact_vectors",
            Self::FilterIndex => "filter_index",
            Self::LexicalBlock => "lexical_block",
            Self::LateInteraction => "late_interaction",
            Self::Tombstone => "tombstone",
            Self::IdDirectory => "id_directory",
            Self::Unknown => "unknown",
        }
    }
}

/// One raw persisted-object access sample.
#[derive(Debug, Clone)]
pub struct StorageAccessEvent {
    operation: &'static str,
    path: String,
    physical_format: String,
    object_bytes: u64,
    request_count: u64,
    bytes_fetched: u64,
    logical_projection: String,
    row_selection: String,
    logical_rows_requested: Option<u64>,
    logical_rows_decoded: Option<u64>,
    decode_cpu_ns: Option<u64>,
    cache_state: &'static str,
    status: &'static str,
}

impl StorageAccessEvent {
    /// Construct one object read sample.
    #[must_use]
    pub fn read(
        path: impl Into<String>,
        physical_format: impl Into<String>,
        object_bytes: u64,
        bytes_fetched: u64,
    ) -> Self {
        Self::observed_read(path, physical_format, object_bytes, 1, bytes_fetched)
    }

    /// Construct a read sample with observed physical request accounting.
    #[must_use]
    pub fn observed_read(
        path: impl Into<String>,
        physical_format: impl Into<String>,
        object_bytes: u64,
        request_count: u64,
        bytes_fetched: u64,
    ) -> Self {
        Self {
            operation: "read",
            path: path.into(),
            physical_format: physical_format.into(),
            object_bytes,
            request_count,
            bytes_fetched,
            logical_projection: String::new(),
            row_selection: String::new(),
            logical_rows_requested: None,
            logical_rows_decoded: None,
            decode_cpu_ns: None,
            cache_state: "backing",
            status: "ok",
        }
    }

    /// Construct a read served without backing-store requests.
    #[must_use]
    pub fn cached_read(
        path: impl Into<String>,
        physical_format: impl Into<String>,
        object_bytes: u64,
    ) -> Self {
        Self {
            operation: "read",
            path: path.into(),
            physical_format: physical_format.into(),
            object_bytes,
            request_count: 0,
            bytes_fetched: 0,
            logical_projection: String::new(),
            row_selection: String::new(),
            logical_rows_requested: None,
            logical_rows_decoded: None,
            decode_cpu_ns: None,
            cache_state: "disk",
            status: "ok",
        }
    }

    /// Construct one complete object write sample.
    #[must_use]
    pub fn write(
        path: impl Into<String>,
        physical_format: impl Into<String>,
        object_bytes: u64,
    ) -> Self {
        Self::observed_write(path, physical_format, object_bytes, 1)
    }

    /// Construct a write sample with observed physical request accounting.
    #[must_use]
    pub fn observed_write(
        path: impl Into<String>,
        physical_format: impl Into<String>,
        object_bytes: u64,
        request_count: u64,
    ) -> Self {
        Self {
            operation: "write",
            path: path.into(),
            physical_format: physical_format.into(),
            object_bytes,
            request_count,
            bytes_fetched: object_bytes,
            logical_projection: String::new(),
            row_selection: String::new(),
            logical_rows_requested: None,
            logical_rows_decoded: None,
            decode_cpu_ns: None,
            cache_state: "write",
            status: "ok",
        }
    }

    /// Construct one logical decode sample for an object already charged by
    /// the storage boundary. Projection and selection use `|` as their stable
    /// item separator so traces remain convenient to inspect as CSV.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn decode(
        path: impl Into<String>,
        physical_format: impl Into<String>,
        object_bytes: u64,
        logical_projection: impl Into<String>,
        row_selection: impl Into<String>,
        logical_rows_requested: u64,
        logical_rows_decoded: u64,
        decode_cpu_ns: u64,
    ) -> Self {
        Self {
            operation: "decode",
            path: path.into(),
            physical_format: physical_format.into(),
            object_bytes,
            request_count: 0,
            bytes_fetched: 0,
            logical_projection: logical_projection.into(),
            row_selection: row_selection.into(),
            logical_rows_requested: Some(logical_rows_requested),
            logical_rows_decoded: Some(logical_rows_decoded),
            decode_cpu_ns: Some(decode_cpu_ns),
            cache_state: "memory",
            status: "ok",
        }
    }
}

/// Cloneable process trace sink; disabled sinks perform no file I/O.
#[derive(Clone, Default)]
pub struct StorageAccessTrace {
    writer: Arc<Mutex<Option<BufWriter<File>>>>,
}

impl StorageAccessTrace {
    /// Construct a disabled trace sink.
    #[must_use]
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Create a trace CSV and write its stable header.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path.as_ref()).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "failed to create storage trace `{}`: {error}",
                path.as_ref().display()
            ))
        })?;
        let mut writer = BufWriter::new(file);
        writeln!(
            writer,
            "operation,object_role,path,physical_format,object_bytes,request_count,bytes_fetched,logical_projection,row_selection,logical_rows_requested,logical_rows_decoded,decode_cpu_ns,cache_state,status"
        )
        .map_err(trace_write_error)?;
        writer.flush().map_err(trace_write_error)?;
        Ok(Self {
            writer: Arc::new(Mutex::new(Some(writer))),
        })
    }

    /// Append one raw event, or return immediately when disabled.
    pub fn record(&self, event: StorageAccessEvent) -> Result<()> {
        let mut guard = self
            .writer
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(writer) = guard.as_mut() else {
            return Ok(());
        };
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            event.operation,
            physical_object_role_for_path(&event.path).as_str(),
            csv_field(&event.path),
            csv_field(&event.physical_format),
            event.object_bytes,
            event.request_count,
            event.bytes_fetched,
            csv_field(&event.logical_projection),
            csv_field(&event.row_selection),
            optional_u64(event.logical_rows_requested),
            optional_u64(event.logical_rows_decoded),
            optional_u64(event.decode_cpu_ns),
            event.cache_state,
            event.status,
        )
        .map_err(trace_write_error)?;
        writer.flush().map_err(trace_write_error)
    }
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn trace_write_error(error: std::io::Error) -> BorsukError {
    BorsukError::InvalidStorage(format!("failed to write storage trace: {error}"))
}

static INSTALLED_TRACE: OnceLock<StorageAccessTrace> = OnceLock::new();
static TRACE_INSTALL_LOCK: Mutex<()> = Mutex::new(());

/// Install the process-wide storage trace used by subsequently created indexes.
pub fn install_storage_access_trace(trace: StorageAccessTrace) -> Result<()> {
    let _guard = TRACE_INSTALL_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    INSTALLED_TRACE.set(trace).map_err(|_| {
        BorsukError::InvalidStorage("storage access trace is already installed".to_string())
    })
}

pub(crate) fn configured_storage_access_trace() -> Result<StorageAccessTrace> {
    if let Some(trace) = INSTALLED_TRACE.get() {
        return Ok(trace.clone());
    }
    let _guard = TRACE_INSTALL_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if let Some(trace) = INSTALLED_TRACE.get() {
        return Ok(trace.clone());
    }
    match std::env::var_os("BORSUK_STORAGE_TRACE") {
        Some(path) => {
            let trace = StorageAccessTrace::create(Path::new(&path))?;
            let _ = INSTALLED_TRACE.set(trace.clone());
            Ok(trace)
        }
        None => Ok(StorageAccessTrace::disabled()),
    }
}

pub(crate) fn physical_format_for_path(path: &str) -> &'static str {
    match Path::new(path).extension().and_then(|value| value.to_str()) {
        Some("parquet" | "parq") => "parquet",
        Some("vortex") => "vortex",
        Some("arrow" | "ipc") => "arrow",
        _ => "packed",
    }
}
