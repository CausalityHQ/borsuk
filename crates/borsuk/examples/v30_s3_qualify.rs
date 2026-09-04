//! Explicit authenticated local/S3 qualification boundary for the V30 page index.

use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc, time::Instant};

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    BorsukError, V27HierarchyArtifactIdentity, V27HierarchyArtifacts, V30LayoutArtifactIdentity,
    V30LayoutArtifacts, V30PqArtifactIdentity, V30PqArtifacts, V32Index, V32PageLocation,
    V32PageLocationsArtifact, V32PagePrefixes, V32PageStore, V32Router, V32RoutingDiagnostic,
    V32RoutingTargetStage, V32SearchArm, V32SearchPhase, V32SearchResult, V32ServingTier,
    decode_v32_page_locations,
};
use bytes::Bytes;
use futures_util::future::try_join_all;
use object_store::{ObjectStore, ObjectStoreExt, parse_url_opts, path::Path as ObjectPath};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArtifactArg {
    path: PathBuf,
    sha256: String,
    encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PageSource {
    Local(PathBuf),
    Tier(V32ServingTier),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Args {
    manifest: ArtifactArg,
    artifact_dir: PathBuf,
    query: ArtifactArg,
    query_start: usize,
    query_count: usize,
    root_beam: usize,
    leaf_beam: usize,
    candidate_depth: usize,
    page_count: usize,
    k: usize,
    page_source: Option<PageSource>,
    diagnostic: Option<DiagnosticRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiagnosticRequest {
    logicals: Vec<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskArtifact {
    encoded_bytes: u64,
    file: String,
    role: String,
    sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskPqArtifact {
    dependencies: Vec<String>,
    encoded_bytes: u64,
    file: String,
    role: String,
    row_count: u64,
    sha256: String,
    width_bytes: u8,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskHierarchy {
    leaves: DiskArtifact,
    roots: DiskArtifact,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskLayout {
    maximum_code_parent_rows: u64,
    maximum_routing_leaf_rows: u64,
    maximum_routing_leaves_per_root: u64,
    packing_algorithm: String,
    page_ranges: DiskArtifact,
    page_rows: usize,
    projected_resident_bytes: u64,
    routing_ranges: DiskArtifact,
    source_rows: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskPq {
    artifacts: Vec<DiskPqArtifact>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskRoutingArm {
    leaf_beam: usize,
    maximum_scanned_codes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskRouting {
    algorithm: String,
    arms: Vec<DiskRoutingArm>,
    candidate_depth: usize,
    page_count: usize,
    root_beam: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskServing {
    express_page_prefix: serde_json::Value,
    page_locations: DiskArtifact,
    standard_page_prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskDiagnostics {
    logical_sources: DiskArtifact,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskSource {
    commit: String,
    corpus_manifest_bytes: u64,
    corpus_manifest_sha256: String,
    corpus_manifest_uri: String,
    dataset_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskManifest {
    diagnostics: DiskDiagnostics,
    hierarchy: DiskHierarchy,
    layout: DiskLayout,
    page_key_suffix: String,
    pq: DiskPq,
    routing: DiskRouting,
    schema_version: u8,
    serving: DiskServing,
    source: DiskSource,
}

#[derive(Debug)]
struct Manifest {
    hierarchy: Vec<(String, V30LayoutArtifactIdentity)>,
    layout: Vec<(String, V30LayoutArtifactIdentity)>,
    pq: Vec<(String, V30PqArtifactIdentity)>,
    source_rows: u64,
    page_key_suffix: String,
    page_locations: (String, V30LayoutArtifactIdentity),
    page_prefixes: V32PagePrefixes,
    routing_arms: Vec<(usize, u64)>,
    routing_candidate_depth: usize,
    routing_page_count: usize,
}

fn argument_error(message: &str) -> String {
    format!("V30 qualifier arguments {message}")
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn canonical(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical(value)))
                .collect(),
        ),
        value => value,
    }
}

fn read_bytes(argument: &ArtifactArg, role: &str) -> borsuk::Result<Vec<u8>> {
    let bytes = fs::read(&argument.path).map_err(|source| BorsukError::Io {
        path: argument.path.clone(),
        source,
    })?;
    if bytes.len() as u64 != argument.encoded_bytes
        || format!("{:x}", Sha256::digest(&bytes)) != argument.sha256
    {
        return Err(invalid(&format!(
            "V30 qualifier {role} byte authority differs"
        )));
    }
    Ok(bytes)
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('/') && !name.contains('\\')
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn tier_store_options(tier: V32ServingTier) -> Vec<(String, String)> {
    match tier {
        V32ServingTier::Standard => Vec::new(),
        V32ServingTier::Express => {
            vec![("aws_s3_express".to_owned(), "true".to_owned())]
        }
    }
}

fn serving_runtime() -> borsuk::Result<Arc<tokio::runtime::Runtime>> {
    Ok(Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .map_err(|source| BorsukError::Io {
                path: PathBuf::from("tokio-runtime"),
                source,
            })?,
    ))
}

fn disk_identity(
    artifact: DiskArtifact,
    role: &str,
) -> borsuk::Result<(String, V30LayoutArtifactIdentity)> {
    if artifact.role != role
        || artifact.encoded_bytes == 0
        || !valid_name(&artifact.file)
        || !valid_digest(&artifact.sha256)
    {
        return Err(invalid("V30 qualifier manifest artifact authority differs"));
    }
    Ok((
        artifact.file,
        V30LayoutArtifactIdentity {
            role: artifact.role,
            sha256: artifact.sha256,
            encoded_bytes: artifact.encoded_bytes,
        },
    ))
}

fn read_manifest(argument: &ArtifactArg) -> borsuk::Result<Manifest> {
    let bytes = read_bytes(argument, "manifest")?;
    if bytes.last() != Some(&b'\n') || bytes[..bytes.len() - 1].contains(&b'\n') {
        return Err(invalid("V30 qualifier manifest canonical bytes differ"));
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|_| invalid("V30 qualifier manifest JSON differs"))?;
    let mut expected = serde_json::to_vec(&canonical(value.clone()))
        .map_err(|_| invalid("V30 qualifier manifest serialization failed"))?;
    expected.push(b'\n');
    if bytes != expected {
        return Err(invalid("V30 qualifier manifest canonical bytes differ"));
    }
    let disk: DiskManifest = serde_json::from_value(value)
        .map_err(|_| invalid("V30 qualifier manifest schema differs"))?;
    if disk.schema_version != 3
        || disk.page_key_suffix != ".arrow"
        || disk.layout.source_rows == 0
        || disk.layout.maximum_code_parent_rows == 0
        || disk.layout.maximum_code_parent_rows > 131_072
        || disk.layout.maximum_routing_leaf_rows == 0
        || disk.layout.maximum_routing_leaf_rows > 1_024
        || disk.layout.maximum_routing_leaves_per_root == 0
        || disk.layout.projected_resident_bytes == 0
        || disk.layout.projected_resident_bytes > 3 * 1_024 * 1_024 * 1_024
        || disk.layout.packing_algorithm != "routing-microleaf-global-v1"
        || disk.layout.page_rows == 0
        || disk.layout.page_rows > 480
        || disk.pq.artifacts.len() != 5
        || disk.source.dataset_id != "deep-image-96"
        || disk.source.commit.len() != 40
        || !disk
            .source
            .commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || disk.source.corpus_manifest_bytes == 0
        || !valid_digest(&disk.source.corpus_manifest_sha256)
        || !disk.source.corpus_manifest_uri.starts_with("s3://")
        || disk.routing.algorithm != "hierarchical-routing-microleaf-pq-v1"
        || disk.routing.root_beam != 8
        || disk.routing.candidate_depth != 12_288
        || disk
            .routing
            .arms
            .iter()
            .map(|arm| (arm.leaf_beam, arm.maximum_scanned_codes))
            .ne([(64, 65_536), (128, 131_072), (256, 262_144)])
        || disk.routing.page_count != 16
    {
        return Err(invalid("V30 qualifier manifest constants differ"));
    }
    let hierarchy = vec![
        disk_identity(disk.hierarchy.roots, "v27-roots-arrow")?,
        disk_identity(disk.hierarchy.leaves, "v27-leaves-arrow")?,
    ];
    let layout = vec![
        disk_identity(disk.layout.routing_ranges, "v32-routing-ranges-arrow")?,
        disk_identity(disk.layout.page_ranges, "v32-page-ranges-parquet")?,
    ];
    let express_page_prefix = match &disk.serving.express_page_prefix {
        serde_json::Value::Null => None,
        serde_json::Value::String(value) => Some(value.clone()),
        _ => return Err(invalid("V32 Express page prefix type differs")),
    };
    let page_prefixes = V32PagePrefixes::new(
        disk.serving.standard_page_prefix.clone(),
        express_page_prefix,
    )?;
    let page_locations = disk_identity(disk.serving.page_locations, "v32-page-locations-parquet")?;
    let logical_sources = disk_identity(
        disk.diagnostics.logical_sources,
        "v32-logical-sources-arrow",
    )?;
    if logical_sources.0 != "logical-sources.arrow" {
        return Err(invalid("V32 logical source manifest authority differs"));
    }
    let roles = [
        "pq24-codebook",
        "pq48-codebook",
        "pq-base-codes",
        "pq-fidelity",
        "pq-high-codes",
    ];
    let pq = disk
        .pq
        .artifacts
        .into_iter()
        .zip(roles)
        .map(|(artifact, role)| {
            if artifact.role != role
                || artifact.encoded_bytes == 0
                || artifact.row_count == 0
                || !valid_name(&artifact.file)
                || !valid_digest(&artifact.sha256)
                || artifact
                    .dependencies
                    .iter()
                    .any(|value| !valid_digest(value))
            {
                return Err(invalid("V30 qualifier PQ manifest authority differs"));
            }
            Ok((
                artifact.file,
                V30PqArtifactIdentity {
                    role: artifact.role,
                    sha256: artifact.sha256,
                    encoded_bytes: artifact.encoded_bytes,
                    row_count: artifact.row_count,
                    width_bytes: artifact.width_bytes,
                    dependencies: artifact.dependencies,
                },
            ))
        })
        .collect::<borsuk::Result<Vec<_>>>()?;
    Ok(Manifest {
        hierarchy,
        layout,
        pq,
        source_rows: disk.layout.source_rows,
        page_key_suffix: disk.page_key_suffix,
        page_locations,
        page_prefixes,
        routing_arms: disk
            .routing
            .arms
            .into_iter()
            .map(|arm| (arm.leaf_beam, arm.maximum_scanned_codes))
            .collect(),
        routing_candidate_depth: disk.routing.candidate_depth,
        routing_page_count: disk.routing.page_count,
    })
}

#[derive(Clone)]
struct LocalPageStore {
    directory: PathBuf,
    suffix: String,
}

impl V32PageStore for LocalPageStore {
    fn read_wave(&self, pages: &[borsuk::V27PageIdentity]) -> borsuk::Result<Vec<Bytes>> {
        pages
            .iter()
            .map(|page| {
                let path = self
                    .directory
                    .join(format!("{}{}", page.sha256, self.suffix));
                fs::read(&path)
                    .map(Bytes::from)
                    .map_err(|source| BorsukError::Io { path, source })
            })
            .collect()
    }
}

#[derive(Clone)]
struct ObjectPageStore {
    store: Arc<dyn ObjectStore>,
    locations: Arc<Vec<V32PageLocation>>,
    page_prefix: ObjectPath,
    runtime: Arc<tokio::runtime::Runtime>,
}

#[derive(Clone, Copy, Default)]
struct SearchPhaseTiming {
    routing_cpu_ns: u64,
    page_read_cpu_ns: u64,
    exact_rerank_cpu_ns: u64,
    routing_elapsed_ns: u64,
    page_read_elapsed_ns: u64,
    exact_rerank_elapsed_ns: u64,
}

impl V32PageStore for ObjectPageStore {
    fn read_wave(&self, pages: &[borsuk::V27PageIdentity]) -> borsuk::Result<Vec<Bytes>> {
        self.runtime.block_on(async {
            let reads = pages
                .iter()
                .map(|page| {
                    let location = self
                        .locations
                        .get(page.ordinal as usize)
                        .ok_or_else(|| invalid("V30 qualifier page location ordinal differs"))?;
                    if location.page_ordinal != page.ordinal
                        || location.sha256_hex() != page.sha256
                        || location.encoded_bytes != page.encoded_bytes
                    {
                        return Err(invalid("V30 qualifier selected page authority differs"));
                    }
                    let store = Arc::clone(&self.store);
                    let path = self
                        .page_prefix
                        .clone()
                        .join(format!("{}.arrow", location.sha256_hex()));
                    Ok(async move { Ok::<_, BorsukError>(store.get(&path).await?.bytes().await?) })
                })
                .collect::<borsuk::Result<Vec<_>>>()?;
            try_join_all(reads).await
        })
    }
}

fn result_bytes(
    result: &V32SearchResult,
    process_cpu_ns: u64,
    elapsed_ns: u64,
    peak_rss_bytes: u64,
    phases: SearchPhaseTiming,
) -> borsuk::Result<Vec<u8>> {
    if result
        .matches
        .iter()
        .any(|item| !item.squared_distance.is_finite())
    {
        return Err(invalid("V30 qualifier result distance differs"));
    }
    let matches = result
        .matches
        .iter()
        .map(|item| {
            serde_json::json!({
                "source_ordinal": item.source_ordinal,
                "squared_distance": item.squared_distance,
            })
        })
        .collect::<Vec<_>>();
    let phase_cpu_ns = phases
        .routing_cpu_ns
        .checked_add(phases.page_read_cpu_ns)
        .and_then(|value| value.checked_add(phases.exact_rerank_cpu_ns))
        .ok_or_else(|| invalid("V30 qualifier phase CPU overflows"))?;
    let phase_elapsed_ns = phases
        .routing_elapsed_ns
        .checked_add(phases.page_read_elapsed_ns)
        .and_then(|value| value.checked_add(phases.exact_rerank_elapsed_ns))
        .ok_or_else(|| invalid("V30 qualifier phase elapsed overflows"))?;
    if phase_cpu_ns > process_cpu_ns || phase_elapsed_ns > elapsed_ns {
        return Err(invalid("V30 qualifier phase timing differs"));
    }
    let value = serde_json::json!({
        "claim_eligible": false,
        "matches": matches,
        "schema_version": 2,
        "timing": {
            "elapsed_ns": elapsed_ns,
            "exact_rerank_cpu_ns": phases.exact_rerank_cpu_ns,
            "exact_rerank_elapsed_ns": phases.exact_rerank_elapsed_ns,
            "page_read_cpu_ns": phases.page_read_cpu_ns,
            "page_read_elapsed_ns": phases.page_read_elapsed_ns,
            "peak_rss_bytes": peak_rss_bytes,
            "process_cpu_ns": process_cpu_ns,
            "routing_cpu_ns": phases.routing_cpu_ns,
            "routing_elapsed_ns": phases.routing_elapsed_ns,
        },
        "work": {
            "decoded_rows": result.work.decoded_rows,
            "encoded_bytes": result.work.encoded_bytes,
            "get_count": result.work.get_count,
            "routing": {
                "candidates_retained": result.work.routing.candidates_retained,
                "codes_scanned": result.work.routing.codes_scanned,
                "leaves_eligible": result.work.routing.leaves_eligible,
                "leaves_scanned": result.work.routing.leaves_scanned,
                "pages_considered": result.work.routing.pages_considered,
                "peak_query_table_pairs_live": result.work.routing.peak_query_table_pairs_live,
                "query_table_pairs_built": result.work.routing.query_table_pairs_built,
                "roots_scored": result.work.routing.roots_scored,
                "selected_pages": result.work.routing.selected_pages,
            },
            "unique_rows": result.work.unique_rows,
        },
    });
    let mut bytes = serde_json::to_vec(&canonical(value))
        .map_err(|_| invalid("V30 qualifier result serialization failed"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn diagnostic_bytes(
    query_ordinal: usize,
    truth_independent_selection: bool,
    diagnostic: V32RoutingDiagnostic,
) -> borsuk::Result<Vec<u8>> {
    if !truth_independent_selection {
        return Err(invalid(
            "V32 diagnostic truth-independent selection differs",
        ));
    }
    let page_selection = |pages: &[borsuk::V27PageIdentity]| -> borsuk::Result<serde_json::Value> {
        let selected_page_bytes = pages
            .iter()
            .try_fold(0_u64, |total, page| total.checked_add(page.encoded_bytes))
            .ok_or_else(|| invalid("V32 diagnostic selected bytes overflow"))?;
        Ok(serde_json::json!({
            "pages": pages.iter().map(|page| serde_json::json!({
                "encoded_bytes": page.encoded_bytes,
                "ordinal": page.ordinal,
                "sha256": page.sha256,
            })).collect::<Vec<_>>(),
            "selected_page_bytes": selected_page_bytes,
        }))
    };
    let first_distinct = page_selection(&diagnostic.selection.pages)?;
    let reciprocal_rank = page_selection(&diagnostic.reciprocal_rank_pages)?;
    let selected_page_bytes = diagnostic
        .selection
        .pages
        .iter()
        .try_fold(0_u64, |total, page| total.checked_add(page.encoded_bytes))
        .ok_or_else(|| invalid("V32 diagnostic selected bytes overflow"))?;
    let work = diagnostic.selection.work;
    let diagnostics = diagnostic
        .targets
        .into_iter()
        .map(|report| {
            let stage = match report.stage {
                V32RoutingTargetStage::LeafFrontier => "leaf-frontier",
                V32RoutingTargetStage::CandidateRetention => "candidate-retention",
                V32RoutingTargetStage::PageReducer => "page-reducer",
                V32RoutingTargetStage::SelectedPage => "selected-page",
            };
            serde_json::json!({
                "candidate_rank": report.candidate_rank,
                "first_unique_page_rank": report.first_unique_page_rank,
                "leaf_ordinal": report.leaf_ordinal,
                "logical": report.logical,
                "page_ordinal": report.page_ordinal,
                "reciprocal_rank_selected": report.reciprocal_rank_selected,
                "routing_leaf_rank": report.routing_leaf_rank,
                "stage": stage,
            })
        })
        .collect::<Vec<_>>();
    let value = serde_json::json!({
        "claim_eligible": false,
        "diagnostics": diagnostics,
        "page_body_reads": 0,
        "page_selections": {
            "first_distinct": first_distinct,
            "reciprocal_rank": reciprocal_rank,
        },
        "query_ordinal": query_ordinal,
        "routing": {
            "candidates_retained": work.candidates_retained,
            "codes_scanned": work.codes_scanned,
            "leaves_eligible": work.leaves_eligible,
            "leaves_scanned": work.leaves_scanned,
            "pages_considered": work.pages_considered,
            "peak_query_table_pairs_live": work.peak_query_table_pairs_live,
            "query_table_pairs_built": work.query_table_pairs_built,
            "roots_scored": work.roots_scored,
            "selected_page_bytes": selected_page_bytes,
            "selected_pages": work.selected_pages,
        },
        "schema_version": 4,
        "truth_independent_selection": truth_independent_selection,
    });
    let mut bytes = serde_json::to_vec(&canonical(value))
        .map_err(|_| invalid("V30 qualifier diagnostic serialization failed"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn process_cpu_nanoseconds() -> borsuk::Result<u64> {
    let task_root = PathBuf::from("/proc/self/task");
    let tasks = fs::read_dir(&task_root).map_err(|source| BorsukError::Io {
        path: task_root.clone(),
        source,
    })?;
    let mut total = 0_u64;
    for task in tasks {
        let path = task
            .map_err(|source| BorsukError::Io {
                path: task_root.clone(),
                source,
            })?
            .path()
            .join("schedstat");
        let schedstat = match fs::read_to_string(&path) {
            Ok(value) => value,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(BorsukError::Io { path, source }),
        };
        let value = schedstat
            .split_whitespace()
            .next()
            .ok_or_else(|| invalid("V30 qualifier process CPU differs"))?
            .parse::<u64>()
            .map_err(|_| invalid("V30 qualifier process CPU differs"))?;
        total = total
            .checked_add(value)
            .ok_or_else(|| invalid("V30 qualifier process CPU overflows"))?;
    }
    Ok(total)
}

fn peak_rss_bytes() -> borsuk::Result<u64> {
    let status = fs::read_to_string("/proc/self/status")
        .map_err(|_| invalid("V30 qualifier process RSS status differs"))?;
    let line = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .ok_or_else(|| invalid("V30 qualifier process RSS field differs"))?;
    let kib = line
        .split_ascii_whitespace()
        .nth(1)
        .ok_or_else(|| invalid("V30 qualifier process RSS value differs"))?
        .parse::<u64>()
        .map_err(|_| invalid("V30 qualifier process RSS value differs"))?;
    kib.checked_mul(1024)
        .ok_or_else(|| invalid("V30 qualifier process RSS overflows"))
}

fn read_resident(
    directory: &std::path::Path,
    file: &str,
    sha256: &str,
    encoded_bytes: u64,
) -> borsuk::Result<Vec<u8>> {
    read_bytes(
        &ArtifactArg {
            path: directory.join(file),
            sha256: sha256.to_owned(),
            encoded_bytes,
        },
        "resident artifact",
    )
}

fn query_schema() -> Schema {
    let child = Arc::new(Field::new("element", DataType::Float32, false));
    Schema::new(vec![Field::new(
        "emb",
        DataType::FixedSizeList(child, 96),
        false,
    )])
}

fn read_queries(
    argument: &ArtifactArg,
    query_start: usize,
    query_count: usize,
) -> borsuk::Result<Vec<[f32; 96]>> {
    let query_end = query_start
        .checked_add(query_count)
        .ok_or_else(|| invalid("V30 qualifier query range overflows"))?;
    if query_count == 0 {
        return Err(invalid("V30 qualifier query count differs"));
    }
    let bytes = read_bytes(argument, "query")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
    if builder.schema().as_ref() != &query_schema() {
        return Err(invalid("V30 qualifier query Parquet schema differs"));
    }
    let mut offset = 0_usize;
    let mut queries = Vec::with_capacity(query_count);
    for batch in builder.build()? {
        let batch = batch?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V30 qualifier query nullability differs"));
        }
        let vectors = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V30 qualifier query vector type differs"))?;
        let values = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V30 qualifier query value type differs"))?;
        let batch_end = offset + batch.num_rows();
        let selected_start = query_start.max(offset);
        let selected_end = query_end.min(batch_end);
        for query_row in selected_start..selected_end {
            let value_start = (query_row - offset) * 96;
            let mut query: [f32; 96] = values.values()[value_start..value_start + 96]
                .try_into()
                .map_err(|_| invalid("V30 qualifier query dimension differs"))?;
            if query.iter().any(|value| !value.is_finite()) {
                return Err(invalid("V30 qualifier query value differs"));
            }
            let norm = query
                .iter()
                .map(|value| f64::from(*value) * f64::from(*value))
                .sum::<f64>()
                .sqrt();
            if !norm.is_finite() || norm <= 0.0 {
                return Err(invalid("V30 qualifier query norm differs"));
            }
            for value in &mut query {
                *value = (f64::from(*value) / norm) as f32;
            }
            queries.push(query);
        }
        offset = batch_end;
    }
    if queries.len() != query_count {
        return Err(invalid("V30 qualifier query range differs"));
    }
    Ok(queries)
}

fn read_query(argument: &ArtifactArg, query_row: usize) -> borsuk::Result<[f32; 96]> {
    read_queries(argument, query_row, 1)?
        .pop()
        .ok_or_else(|| invalid("V30 qualifier query row differs"))
}

fn run_batch<S: V32PageStore>(
    router: V32Router,
    store: S,
    arm: V32SearchArm,
    query: &ArtifactArg,
    query_start: usize,
    query_count: usize,
    k: usize,
) -> borsuk::Result<Vec<u8>> {
    let queries = read_queries(query, query_start, query_count)?;
    let mut results = Vec::with_capacity(query_count);
    let index = V32Index::new(router, store, arm)?;
    for query_vector in queries {
        let cpu_before = process_cpu_nanoseconds()?;
        let started = Instant::now();
        let mut previous_cpu_ns = cpu_before;
        let mut previous_elapsed_ns = 0_u64;
        let mut phases = SearchPhaseTiming::default();
        let result = index.search_observed(&query_vector, k, |phase| {
            let current_cpu_ns = process_cpu_nanoseconds()?;
            let current_elapsed_ns = u64::try_from(started.elapsed().as_nanos())
                .map_err(|_| invalid("V30 qualifier phase elapsed time overflows"))?;
            let cpu_ns = current_cpu_ns
                .checked_sub(previous_cpu_ns)
                .ok_or_else(|| invalid("V30 qualifier phase CPU regressed"))?;
            let elapsed_ns = current_elapsed_ns
                .checked_sub(previous_elapsed_ns)
                .ok_or_else(|| invalid("V30 qualifier phase elapsed regressed"))?;
            match phase {
                V32SearchPhase::RoutingComplete => {
                    phases.routing_cpu_ns = cpu_ns;
                    phases.routing_elapsed_ns = elapsed_ns;
                }
                V32SearchPhase::PageReadComplete => {
                    phases.page_read_cpu_ns = cpu_ns;
                    phases.page_read_elapsed_ns = elapsed_ns;
                }
                V32SearchPhase::ExactRerankComplete => {
                    phases.exact_rerank_cpu_ns = cpu_ns;
                    phases.exact_rerank_elapsed_ns = elapsed_ns;
                }
            }
            previous_cpu_ns = current_cpu_ns;
            previous_elapsed_ns = current_elapsed_ns;
            Ok(())
        })?;
        let elapsed_ns = u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| invalid("V30 qualifier elapsed time overflows"))?;
        let process_cpu_ns = process_cpu_nanoseconds()?
            .checked_sub(cpu_before)
            .ok_or_else(|| invalid("V30 qualifier process CPU regressed"))?;
        let bytes = result_bytes(
            &result,
            process_cpu_ns,
            elapsed_ns,
            peak_rss_bytes()?,
            phases,
        )?;
        results.push(
            serde_json::from_slice::<serde_json::Value>(&bytes)
                .map_err(|_| invalid("V30 qualifier batch result differs"))?,
        );
    }
    let value = serde_json::json!({
        "claim_eligible": false,
        "results": results,
        "schema_version": 2,
    });
    let mut bytes = serde_json::to_vec(&canonical(value))
        .map_err(|_| invalid("V30 qualifier batch serialization failed"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn manifest_arm(args: &Args, manifest: &Manifest) -> borsuk::Result<V32SearchArm> {
    if args.candidate_depth != manifest.routing_candidate_depth
        || args.page_count != manifest.routing_page_count
    {
        return Err(invalid("V30 qualifier routing manifest differs"));
    }
    let scan_budget = manifest
        .routing_arms
        .iter()
        .find_map(|&(leaf_beam, scan_budget)| (leaf_beam == args.leaf_beam).then_some(scan_budget))
        .ok_or_else(|| invalid("V30 qualifier routing arm differs"))?;
    Ok(V32SearchArm {
        root_beam: args.root_beam,
        leaf_beam: args.leaf_beam,
        scan_budget,
        candidate_depth: args.candidate_depth,
        page_count: args.page_count,
    })
}

fn execute(args: Args) -> borsuk::Result<Vec<u8>> {
    let manifest = read_manifest(&args.manifest)?;
    let page_location_bytes = read_resident(
        &args.artifact_dir,
        &manifest.page_locations.0,
        &manifest.page_locations.1.sha256,
        manifest.page_locations.1.encoded_bytes,
    )?;
    let page_locations = decode_v32_page_locations(&V32PageLocationsArtifact {
        role: manifest.page_locations.1.role.clone(),
        sha256: manifest.page_locations.1.sha256.clone(),
        encoded_bytes: manifest.page_locations.1.encoded_bytes,
        parquet: page_location_bytes,
    })?;
    let hierarchy_bytes = manifest
        .hierarchy
        .iter()
        .map(|(file, identity)| {
            read_resident(
                &args.artifact_dir,
                file,
                &identity.sha256,
                identity.encoded_bytes,
            )
        })
        .collect::<borsuk::Result<Vec<_>>>()?;
    let hierarchy = V27HierarchyArtifacts {
        roots: V27HierarchyArtifactIdentity {
            role: manifest.hierarchy[0].1.role.clone(),
            sha256: manifest.hierarchy[0].1.sha256.clone(),
            encoded_bytes: manifest.hierarchy[0].1.encoded_bytes,
        },
        leaves: V27HierarchyArtifactIdentity {
            role: manifest.hierarchy[1].1.role.clone(),
            sha256: manifest.hierarchy[1].1.sha256.clone(),
            encoded_bytes: manifest.hierarchy[1].1.encoded_bytes,
        },
        roots_bytes: hierarchy_bytes[0].clone(),
        leaves_bytes: hierarchy_bytes[1].clone(),
    };
    let pq_bytes = manifest
        .pq
        .iter()
        .map(|(file, identity)| {
            read_resident(
                &args.artifact_dir,
                file,
                &identity.sha256,
                identity.encoded_bytes,
            )
        })
        .collect::<borsuk::Result<Vec<_>>>()?;
    let pq = V30PqArtifacts {
        identities: manifest
            .pq
            .iter()
            .map(|(_, identity)| identity.clone())
            .collect(),
        bytes: pq_bytes,
    };
    let layout_bytes = manifest
        .layout
        .iter()
        .map(|(file, identity)| {
            read_resident(
                &args.artifact_dir,
                file,
                &identity.sha256,
                identity.encoded_bytes,
            )
        })
        .collect::<borsuk::Result<Vec<_>>>()?;
    let layout = V30LayoutArtifacts {
        source_rows: manifest.source_rows,
        leaf_ranges: manifest.layout[0].1.clone(),
        page_ranges: manifest.layout[1].1.clone(),
        leaf_ranges_arrow: layout_bytes[0].clone(),
        page_ranges_parquet: layout_bytes[1].clone(),
    };
    let router = V32Router::from_artifacts(&hierarchy, &pq, &layout)?;
    router.validate_page_locations(&page_locations)?;
    let arm = manifest_arm(&args, &manifest)?;
    if let Some(diagnostic) = args.diagnostic {
        let query = read_query(&args.query, args.query_start)?;
        let control = router.select_pages(&query, arm)?;
        let report = router.diagnose_logicals_with_selection(&query, arm, &diagnostic.logicals)?;
        let truth_independent_selection = control == report.selection;
        return diagnostic_bytes(args.query_start, truth_independent_selection, report);
    }
    match args.page_source {
        Some(PageSource::Local(directory)) => run_batch(
            router,
            LocalPageStore {
                directory,
                suffix: manifest.page_key_suffix,
            },
            arm,
            &args.query,
            args.query_start,
            args.query_count,
            args.k,
        ),
        Some(PageSource::Tier(tier)) => {
            let prefix = match tier {
                V32ServingTier::Standard => manifest.page_prefixes.standard(),
                V32ServingTier::Express => manifest
                    .page_prefixes
                    .express()
                    .ok_or_else(|| invalid("V32 Express page prefix is missing"))?,
            };
            let prefix = Url::parse(prefix).map_err(|_| invalid("V30 qualifier S3 URI differs"))?;
            let options = std::env::vars()
                .filter(|(key, _)| {
                    matches!(
                        key.as_str(),
                        "AWS_ACCESS_KEY_ID"
                            | "AWS_SECRET_ACCESS_KEY"
                            | "AWS_SESSION_TOKEN"
                            | "AWS_REGION"
                    )
                })
                .chain(tier_store_options(tier));
            let (store, page_prefix) = parse_url_opts(&prefix, options)?;
            let store: Arc<dyn ObjectStore> = store.into();
            let runtime = serving_runtime()?;
            run_batch(
                router,
                ObjectPageStore {
                    store,
                    locations: Arc::new(page_locations),
                    page_prefix,
                    runtime,
                },
                arm,
                &args.query,
                args.query_start,
                args.query_count,
                args.k,
            )
        }
        None => Err(invalid("V30 qualifier page source differs")),
    }
}

fn take(values: &mut BTreeMap<String, String>, name: &str) -> Result<String, String> {
    values
        .remove(name)
        .ok_or_else(|| argument_error(&format!("missing --{name}")))
}

fn number<T: std::str::FromStr>(
    values: &mut BTreeMap<String, String>,
    name: &str,
) -> Result<T, String> {
    take(values, name)?
        .parse()
        .map_err(|_| argument_error(&format!("--{name} type differs")))
}

fn artifact(values: &mut BTreeMap<String, String>, role: &str) -> Result<ArtifactArg, String> {
    let path_flag = if role == "query" {
        "query-parquet"
    } else {
        role
    };
    let artifact = ArtifactArg {
        path: PathBuf::from(take(values, path_flag)?),
        sha256: take(values, &format!("{role}-sha256"))?,
        encoded_bytes: number(values, &format!("{role}-bytes"))?,
    };
    if !artifact.path.is_absolute()
        || artifact.encoded_bytes == 0
        || artifact.sha256.len() != 64
        || artifact
            .sha256
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err(argument_error(&format!("--{role} authority differs")));
    }
    Ok(artifact)
}

fn parse_args(arguments: Vec<String>) -> Result<Args, String> {
    let mut arguments = arguments.into_iter();
    arguments
        .next()
        .ok_or_else(|| argument_error("program is missing"))?;
    let mut execute = false;
    let mut values = BTreeMap::new();
    while let Some(flag) = arguments.next() {
        if flag == "--execute" {
            if execute {
                return Err(argument_error("duplicate --execute"));
            }
            execute = true;
            continue;
        }
        let name = flag
            .strip_prefix("--")
            .ok_or_else(|| argument_error("flag syntax differs"))?;
        let value = arguments
            .next()
            .ok_or_else(|| argument_error(&format!("--{name} value is missing")))?;
        if values.insert(name.to_owned(), value).is_some() {
            return Err(argument_error(&format!("duplicate --{name}")));
        }
    }
    if !execute {
        return Err(argument_error("--execute is required"));
    }
    let manifest = artifact(&mut values, "manifest")?;
    let artifact_dir = PathBuf::from(take(&mut values, "artifact-dir")?);
    let query = artifact(&mut values, "query")?;
    let query_start = number(&mut values, "query-start")?;
    let query_count = number(&mut values, "query-count")?;
    let root_beam = number(&mut values, "root-beam")?;
    let leaf_beam = number(&mut values, "leaf-beam")?;
    let candidate_depth = number(&mut values, "candidate-depth")?;
    let page_count = number(&mut values, "page-count")?;
    let k = number(&mut values, "k")?;
    let diagnostic_logicals = values
        .remove("diagnose-logicals")
        .map(|value| {
            let logicals = value
                .split(',')
                .map(|logical| {
                    logical
                        .parse::<u64>()
                        .map_err(|_| argument_error("--diagnose-logicals type differs"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let unique = logicals
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>();
            if logicals.len() != 10 || unique.len() != logicals.len() {
                return Err(argument_error("--diagnose-logicals cardinality differs"));
            }
            Ok(logicals)
        })
        .transpose()?;
    let diagnostic = diagnostic_logicals
        .map(|logicals| -> Result<DiagnosticRequest, String> { Ok(DiagnosticRequest { logicals }) })
        .transpose()?;
    let local = values.remove("local-page-dir").map(PathBuf::from);
    let tier = values
        .remove("serving-tier")
        .map(|value| match value.as_str() {
            "standard" => Ok(V32ServingTier::Standard),
            "express" => Ok(V32ServingTier::Express),
            _ => Err(argument_error("--serving-tier value differs")),
        })
        .transpose()?;
    let page_source = match (diagnostic.is_some(), local, tier) {
        (true, None, None) => None,
        (false, Some(path), None) if path.is_absolute() => Some(PageSource::Local(path)),
        (false, None, Some(tier)) => Some(PageSource::Tier(tier)),
        _ => return Err(argument_error("exactly one page source is required")),
    };
    if !artifact_dir.is_absolute()
        || match diagnostic {
            Some(_) => query_count != 1,
            None => query_count != 32,
        }
        || !matches!(root_beam, 8 | 16 | 32)
        || !matches!(leaf_beam, 64 | 128 | 256)
        || candidate_depth != 12_288
        || page_count != 16
        || k == 0
        || k > 10
        || !values.is_empty()
    {
        return Err(argument_error("unknown flag or numeric bound differs"));
    }
    Ok(Args {
        manifest,
        artifact_dir,
        query,
        query_start,
        query_count,
        root_beam,
        leaf_beam,
        candidate_depth,
        page_count,
        k,
        page_source,
        diagnostic,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::Arc,
        time::{Duration, Instant},
    };

    use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch};
    use borsuk::{
        V27PageIdentity, V32Match, V32PageLocation, V32PageSelection, V32PageStore,
        V32RoutingDiagnostic, V32RoutingTargetReport, V32RoutingTargetStage, V32RoutingWork,
        V32SearchResult, V32SearchWork, V32ServingTier,
    };
    use bytes::Bytes;
    use object_store::ObjectStoreExt;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    use parquet::arrow::ArrowWriter;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::{
        Args, ArtifactArg, DiagnosticRequest, LocalPageStore, ObjectPageStore, PageSource,
        SearchPhaseTiming, diagnostic_bytes, execute, manifest_arm, parse_args, peak_rss_bytes,
        process_cpu_nanoseconds, read_manifest, read_queries, result_bytes, run_batch,
    };

    fn arguments() -> Vec<String> {
        [
            "v30_s3_qualify",
            "--execute",
            "--manifest",
            "/tmp/manifest.json",
            "--manifest-sha256",
            &"1".repeat(64),
            "--manifest-bytes",
            "1234",
            "--artifact-dir",
            "/tmp/artifacts",
            "--query-parquet",
            "/tmp/query.parquet",
            "--query-sha256",
            &"2".repeat(64),
            "--query-bytes",
            "4003585",
            "--query-start",
            "0",
            "--query-count",
            "32",
            "--root-beam",
            "8",
            "--leaf-beam",
            "64",
            "--candidate-depth",
            "12288",
            "--page-count",
            "16",
            "--k",
            "10",
            "--serving-tier",
            "standard",
        ]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect()
    }

    #[test]
    fn v32_s3_qualify_process_cpu_clock_resolves_below_the_latency_gate() {
        // Break caught: /proc/self/stat quantizes CPU to 10-ms ticks, making a
        // 15-ms p99 gate alternate between 10 and 20 ms.
        let before = process_cpu_nanoseconds().unwrap();
        let started = std::time::Instant::now();
        let mut value = 1_u64;
        let after = loop {
            value = std::hint::black_box(value.wrapping_mul(6364136223846793005));
            let observed = process_cpu_nanoseconds().unwrap();
            if observed > before || started.elapsed().as_millis() >= 100 {
                break observed;
            }
        };
        assert!(after > before);
        assert!(after - before < 15_000_000);
    }

    #[test]
    fn v32_s3_qualify_reports_process_peak_rss_for_the_release_gate() {
        let peak = peak_rss_bytes().unwrap();
        assert!(peak > 0);
        assert_eq!(peak % 1024, 0);
    }

    #[test]
    fn v32_s3_qualify_reads_one_authenticated_query_range_per_batch() {
        // Break caught: each of 32 queries rereads, rehashes, and reparses the
        // complete Parquet artifact instead of sharing one authenticated load.
        let values =
            Float32Array::from_iter_values((0..40 * 96).map(|index| 1.0_f32 + (index / 96) as f32));
        let vectors = FixedSizeListArray::try_new(
            Arc::new(arrow_schema::Field::new(
                "element",
                arrow_schema::DataType::Float32,
                false,
            )),
            96,
            Arc::new(values),
            None,
        )
        .unwrap();
        let batch =
            RecordBatch::try_new(Arc::new(super::query_schema()), vec![Arc::new(vectors)]).unwrap();
        let mut parquet = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut parquet, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let directory = tempdir().unwrap();
        let path = directory.path().join("query.parquet");
        fs::write(&path, &parquet).unwrap();
        let argument = ArtifactArg {
            path,
            sha256: format!("{:x}", Sha256::digest(&parquet)),
            encoded_bytes: parquet.len() as u64,
        };
        let queries = read_queries(&argument, 3, 32).unwrap();
        assert_eq!(queries.len(), 32);
        for query in queries {
            let norm = query.iter().map(|value| value * value).sum::<f32>();
            assert!((norm - 1.0).abs() < 1e-5);
        }
        assert!(read_queries(&argument, 9, 32).is_err());
    }

    struct NonClonePageStore;

    impl V32PageStore for NonClonePageStore {
        fn read_wave(&self, _pages: &[V27PageIdentity]) -> borsuk::Result<Vec<Bytes>> {
            unreachable!("compile-only batch ownership contract")
        }
    }

    #[test]
    fn v32_s3_qualify_batch_reuses_one_index_without_cloning_router_or_store() {
        // Break caught: every query clones the resident router/store and builds
        // a fresh index instead of reusing one immutable serving instance.
        let _runner = run_batch::<NonClonePageStore>;
    }

    #[test]
    fn v32_s3_qualify_object_store_reuses_one_runtime_across_read_waves() {
        // Break caught: every S3 query builds and tears down a Tokio runtime,
        // adding scheduler setup to the measured serving CPU path.
        let runtime = super::serving_runtime().unwrap();
        assert!(runtime.metrics().num_workers() >= 2);
        let object_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let digest = "a".repeat(64);
        runtime
            .block_on(object_store.put(
                &object_store::path::Path::from(format!("pages/{digest}.arrow")),
                bytes::Bytes::from_static(b"abc").into(),
            ))
            .unwrap();
        let location = V32PageLocation::from_hex(0, &digest, 3, 1).unwrap();
        let store = ObjectPageStore {
            store: object_store,
            locations: Arc::new(vec![location]),
            page_prefix: object_store::path::Path::from("pages"),
            runtime: Arc::clone(&runtime),
        };
        let page = V27PageIdentity {
            ordinal: 0,
            sha256: digest,
            encoded_bytes: 3,
            primary_rows: 1,
            replica_rows: 0,
        };
        assert_eq!(
            store.read_wave(std::slice::from_ref(&page)).unwrap(),
            vec![Bytes::from_static(b"abc")]
        );
        assert!(store.read_wave(&[]).unwrap().is_empty());
        assert_eq!(Arc::strong_count(&runtime), 2);

        let mut drifted = page;
        drifted.sha256.replace_range(0..1, "b");
        assert!(store.read_wave(&[drifted]).is_err());
    }

    #[test]
    fn v32_s3_qualify_enables_express_session_auth_only_for_express_tier() {
        // Break caught: an Express directory-bucket URI is sent through the
        // Standard S3 signer/endpoint path and fails before the microbenchmark.
        assert_eq!(super::tier_store_options(V32ServingTier::Standard), []);
        assert_eq!(
            super::tier_store_options(V32ServingTier::Express),
            [("aws_s3_express".to_owned(), "true".to_owned())]
        );
    }

    #[test]
    fn v32_s3_qualify_starts_the_complete_page_wave_concurrently() {
        // Break caught: sixteen object reads are awaited serially, multiplying
        // request latency instead of paying one concurrent wave maximum.
        let runtime = super::serving_runtime().unwrap();
        let memory = object_store::memory::InMemory::new();
        let locations = (0..4_u32)
            .map(|ordinal| {
                V32PageLocation::from_hex(ordinal, &format!("{ordinal:064x}"), 1, 1).unwrap()
            })
            .collect::<Vec<_>>();
        let paths = locations
            .iter()
            .map(|location| {
                object_store::path::Path::from(format!("pages/{}.arrow", location.sha256_hex()))
            })
            .collect::<Vec<_>>();
        for path in &paths {
            runtime
                .block_on(memory.put(path, Bytes::from_static(b"x").into()))
                .unwrap();
        }
        let store = ObjectPageStore {
            store: Arc::new(ThrottledStore::new(
                memory,
                ThrottleConfig {
                    wait_get_per_call: Duration::from_millis(40),
                    ..Default::default()
                },
            )),
            locations: Arc::new(locations.clone()),
            page_prefix: object_store::path::Path::from("pages"),
            runtime,
        };
        let pages = locations
            .into_iter()
            .map(|location| V27PageIdentity {
                ordinal: location.page_ordinal,
                sha256: location.sha256_hex(),
                encoded_bytes: location.encoded_bytes,
                primary_rows: 1,
                replica_rows: 0,
            })
            .collect::<Vec<_>>();
        let started = Instant::now();
        assert_eq!(store.read_wave(&pages).unwrap().len(), 4);
        assert!(
            started.elapsed() < Duration::from_millis(120),
            "four delayed GETs executed serially: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn v32_s3_qualify_parser_requires_explicit_authority_and_one_page_source() {
        // Break caught: qualification discovers latest artifacts, accepts an ETag,
        // or silently switches between local/cache/S3 page bodies.
        let parsed = parse_args(arguments()).unwrap();
        assert_eq!(parsed.manifest.path, PathBuf::from("/tmp/manifest.json"));
        assert_eq!(parsed.manifest.sha256, "1".repeat(64));
        assert_eq!(parsed.query.sha256, "2".repeat(64));
        assert_eq!(parsed.query_start, 0);
        assert_eq!(parsed.query_count, 32);
        assert_eq!(parsed.page_count, 16);
        assert_eq!(
            parsed.page_source,
            Some(PageSource::Tier(V32ServingTier::Standard))
        );

        let mut expanded = arguments();
        let page_count = expanded
            .iter()
            .position(|value| value == "--page-count")
            .unwrap();
        expanded[page_count + 1] = "16".to_owned();
        assert_eq!(parse_args(expanded.clone()).unwrap().page_count, 16);
        expanded[page_count + 1] = "17".to_owned();
        assert!(parse_args(expanded).is_err());

        for forbidden in [
            "--latest",
            "--etag",
            "--version",
            "--legacy",
            "--d3",
            "--page-bucket",
            "--s3-page-prefix",
        ] {
            let mut values = arguments();
            values.extend([forbidden.to_owned(), "value".to_owned()]);
            assert!(parse_args(values).is_err(), "accepted {forbidden}");
        }

        let mut missing_execute = arguments();
        missing_execute.remove(1);
        assert!(parse_args(missing_execute).is_err());

        let mut both_sources = arguments();
        both_sources.extend(["--local-page-dir".to_owned(), "/tmp/pages".to_owned()]);
        assert!(parse_args(both_sources).is_err());

        let mut diagnostic = arguments();
        let source = diagnostic
            .iter()
            .position(|value| value == "--serving-tier")
            .unwrap();
        diagnostic.drain(source..=source + 1);
        let count = diagnostic
            .iter()
            .position(|value| value == "--query-count")
            .unwrap();
        diagnostic[count + 1] = "1".to_owned();
        diagnostic.extend([
            "--diagnose-logicals".to_owned(),
            "0,1,2,3,4,5,6,7,8,9".to_owned(),
        ]);
        let parsed = parse_args(diagnostic).unwrap();
        assert_eq!(
            parsed.diagnostic.unwrap().logicals,
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        );
        assert_eq!(parsed.page_source, None);

        let mut singular = arguments();
        let source = singular
            .iter()
            .position(|value| value == "--serving-tier")
            .unwrap();
        singular.drain(source..=source + 1);
        let count = singular
            .iter()
            .position(|value| value == "--query-count")
            .unwrap();
        singular[count + 1] = "1".to_owned();
        singular.extend(["--diagnose-logical".to_owned(), "25".to_owned()]);
        assert!(parse_args(singular).is_err());
    }

    #[test]
    fn v32_s3_qualify_requires_the_frozen_pq_arm_for_production() {
        // Break caught: the serving qualifier silently substitutes centroid
        // routing or an implicit root/candidate frontier for the proven arm.
        let parsed = parse_args(arguments()).unwrap();
        assert_eq!(parsed.root_beam, 8);
        assert_eq!(parsed.leaf_beam, 64);
        assert_eq!(parsed.candidate_depth, 12_288);
        assert_eq!(parsed.page_count, 16);
    }

    #[test]
    fn v32_s3_qualify_root_beam_ladder_is_runtime_not_construction_authority() {
        // Break caught: a page-free containment rerun cannot widen only the
        // cheap root frontier because the build manifest freezes root beam 8.
        let directory = tempdir().unwrap();
        let path = directory.path().join("manifest.json");
        let bytes = manifest_bytes();
        fs::write(&path, &bytes).unwrap();
        let manifest = read_manifest(&ArtifactArg {
            path,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            encoded_bytes: bytes.len() as u64,
        })
        .unwrap();
        for root_beam in [8, 16, 32] {
            let mut values = arguments();
            let position = values
                .iter()
                .position(|value| value == "--root-beam")
                .unwrap();
            values[position + 1] = root_beam.to_string();
            let parsed = parse_args(values).unwrap();
            assert_eq!(
                manifest_arm(&parsed, &manifest).unwrap().root_beam,
                root_beam
            );
        }
    }

    #[test]
    fn v32_s3_qualify_manifest_ladder_pairs_are_exact_for_diagnostics_and_serving() {
        // Break caught: diagnostic containment can run an arm outside the
        // authenticated ladder, or construction preselects only one rung.
        let directory = tempdir().unwrap();
        let path = directory.path().join("manifest.json");
        let baseline = manifest_bytes();
        fs::write(&path, &baseline).unwrap();
        let manifest = read_manifest(&ArtifactArg {
            path: path.clone(),
            sha256: format!("{:x}", Sha256::digest(&baseline)),
            encoded_bytes: baseline.len() as u64,
        })
        .unwrap();
        for (leaf_beam, scan_budget) in [(64, 65_536), (128, 131_072), (256, 262_144)] {
            let mut diagnostic = parse_args(arguments()).unwrap();
            diagnostic.leaf_beam = leaf_beam;
            diagnostic.diagnostic = Some(DiagnosticRequest {
                logicals: (0..10).collect(),
            });
            assert_eq!(
                manifest_arm(&diagnostic, &manifest).unwrap().scan_budget,
                scan_budget
            );
        }
        let mut outside = parse_args(arguments()).unwrap();
        outside.leaf_beam = 192;
        assert!(manifest_arm(&outside, &manifest).is_err());

        let drifted = String::from_utf8(baseline)
            .unwrap()
            .replace("\"leaf_beam\":128", "\"leaf_beam\":127");
        fs::write(&path, drifted.as_bytes()).unwrap();
        assert!(
            read_manifest(&ArtifactArg {
                path,
                sha256: format!("{:x}", Sha256::digest(drifted.as_bytes())),
                encoded_bytes: drifted.len() as u64,
            })
            .is_err()
        );
    }

    #[test]
    fn v32_s3_qualify_diagnostic_output_is_canonical_and_names_the_loss_boundary() {
        // Break caught: the fail-fast diagnostic performs a page read or hides
        // whether routing, candidate retention, or page reduction lost truth.
        let bytes = diagnostic_bytes(
            7,
            true,
            V32RoutingDiagnostic {
                selection: V32PageSelection {
                    pages: vec![
                        V27PageIdentity {
                            ordinal: 11,
                            sha256: "1".repeat(64),
                            encoded_bytes: 100,
                            primary_rows: 1,
                            replica_rows: 0,
                        },
                        V27PageIdentity {
                            ordinal: 12,
                            sha256: "2".repeat(64),
                            encoded_bytes: 200,
                            primary_rows: 1,
                            replica_rows: 0,
                        },
                    ],
                    work: V32RoutingWork {
                        roots_scored: 16,
                        leaves_eligible: 32,
                        leaves_scanned: 32,
                        query_table_pairs_built: 4,
                        peak_query_table_pairs_live: 1,
                        codes_scanned: 40_000,
                        candidates_retained: 12_288,
                        pages_considered: 20,
                        selected_pages: 2,
                    },
                },
                reciprocal_rank_pages: vec![
                    V27PageIdentity {
                        ordinal: 12,
                        sha256: "2".repeat(64),
                        encoded_bytes: 200,
                        primary_rows: 1,
                        replica_rows: 0,
                    },
                    V27PageIdentity {
                        ordinal: 13,
                        sha256: "3".repeat(64),
                        encoded_bytes: 300,
                        primary_rows: 1,
                        replica_rows: 0,
                    },
                ],
                targets: vec![
                    V32RoutingTargetReport {
                        logical: 25,
                        leaf_ordinal: 3,
                        page_ordinal: 11,
                        routing_leaf_rank: Some(9),
                        candidate_rank: None,
                        first_unique_page_rank: Some(12),
                        stage: V32RoutingTargetStage::CandidateRetention,
                        reciprocal_rank_selected: false,
                    },
                    V32RoutingTargetReport {
                        logical: 26,
                        leaf_ordinal: 3,
                        page_ordinal: 12,
                        routing_leaf_rank: Some(9),
                        candidate_rank: Some(7),
                        first_unique_page_rank: Some(4),
                        stage: V32RoutingTargetStage::SelectedPage,
                        reciprocal_rank_selected: true,
                    },
                ],
            },
        )
        .unwrap();
        assert_eq!(
            bytes,
            b"{\"claim_eligible\":false,\"diagnostics\":[{\"candidate_rank\":null,\"first_unique_page_rank\":12,\"leaf_ordinal\":3,\"logical\":25,\"page_ordinal\":11,\"reciprocal_rank_selected\":false,\"routing_leaf_rank\":9,\"stage\":\"candidate-retention\"},{\"candidate_rank\":7,\"first_unique_page_rank\":4,\"leaf_ordinal\":3,\"logical\":26,\"page_ordinal\":12,\"reciprocal_rank_selected\":true,\"routing_leaf_rank\":9,\"stage\":\"selected-page\"}],\"page_body_reads\":0,\"page_selections\":{\"first_distinct\":{\"pages\":[{\"encoded_bytes\":100,\"ordinal\":11,\"sha256\":\"1111111111111111111111111111111111111111111111111111111111111111\"},{\"encoded_bytes\":200,\"ordinal\":12,\"sha256\":\"2222222222222222222222222222222222222222222222222222222222222222\"}],\"selected_page_bytes\":300},\"reciprocal_rank\":{\"pages\":[{\"encoded_bytes\":200,\"ordinal\":12,\"sha256\":\"2222222222222222222222222222222222222222222222222222222222222222\"},{\"encoded_bytes\":300,\"ordinal\":13,\"sha256\":\"3333333333333333333333333333333333333333333333333333333333333333\"}],\"selected_page_bytes\":500}},\"query_ordinal\":7,\"routing\":{\"candidates_retained\":12288,\"codes_scanned\":40000,\"leaves_eligible\":32,\"leaves_scanned\":32,\"pages_considered\":20,\"peak_query_table_pairs_live\":1,\"query_table_pairs_built\":4,\"roots_scored\":16,\"selected_page_bytes\":300,\"selected_pages\":2},\"schema_version\":4,\"truth_independent_selection\":true}\n"
        );
    }

    fn manifest_bytes() -> Vec<u8> {
        let artifact = |role: &str, file: &str, digit: char| {
            format!(
                r#"{{"encoded_bytes":123,"file":"{file}","role":"{role}","sha256":"{}"}}"#,
                digit.to_string().repeat(64)
            )
        };
        let pq = |role: &str,
                  file: &str,
                  digit: char,
                  row_count: u64,
                  width_bytes: u8,
                  dependencies: &str| {
            format!(
                r#"{{"dependencies":{dependencies},"encoded_bytes":123,"file":"{file}","role":"{role}","row_count":{row_count},"sha256":"{}","width_bytes":{width_bytes}}}"#,
                digit.to_string().repeat(64)
            )
        };
        let base_sha = "3".repeat(64);
        let high_sha = "4".repeat(64);
        format!(
            concat!(
                "{{\"diagnostics\":{{\"logical_sources\":{}}},",
                "\"hierarchy\":{{\"leaves\":{},\"roots\":{}}},",
                "\"layout\":{{\"maximum_code_parent_rows\":40,",
                "\"maximum_routing_leaf_rows\":24,\"maximum_routing_leaves_per_root\":2,",
                "\"packing_algorithm\":\"routing-microleaf-global-v1\",",
                "\"page_ranges\":{},\"page_rows\":128,\"projected_resident_bytes\":100000,",
                "\"routing_ranges\":{},",
                "\"source_rows\":40}},",
                "\"page_key_suffix\":\".arrow\",",
                "\"pq\":{{\"artifacts\":[{},{},{},{},{}]}},",
                "\"routing\":{{\"algorithm\":\"hierarchical-routing-microleaf-pq-v1\",",
                "\"arms\":[{{\"leaf_beam\":64,\"maximum_scanned_codes\":65536}},",
                "{{\"leaf_beam\":128,\"maximum_scanned_codes\":131072}},",
                "{{\"leaf_beam\":256,\"maximum_scanned_codes\":262144}}],",
                "\"candidate_depth\":12288,",
                "\"page_count\":16,\"root_beam\":8}},",
                "\"schema_version\":3,",
                "\"serving\":{{\"express_page_prefix\":null,\"page_locations\":{},",
                "\"standard_page_prefix\":\"s3://bucket/v30/build-a0001/pages/\"}},",
                "\"source\":{{\"commit\":\"{}\",\"corpus_manifest_bytes\":4096,",
                "\"corpus_manifest_sha256\":\"{}\",",
                "\"corpus_manifest_uri\":\"s3://bucket/deep-10m/corpus.json\",",
                "\"dataset_id\":\"deep-image-96\"}}}}\n"
            ),
            artifact("v32-logical-sources-arrow", "logical-sources.arrow", 'c',),
            artifact("v27-leaves-arrow", "leaves.arrow", '2'),
            artifact("v27-roots-arrow", "roots.arrow", '1'),
            artifact("v32-page-ranges-parquet", "page-ranges.parquet", '9'),
            artifact("v32-routing-ranges-arrow", "routing-ranges.arrow", '8'),
            pq("pq24-codebook", "pq24.arrow", '3', 1, 24, "[]"),
            pq("pq48-codebook", "pq48.arrow", '4', 1, 48, "[]"),
            pq(
                "pq-base-codes",
                "base.arrow",
                '5',
                38,
                24,
                &format!(r#"["{base_sha}"]"#),
            ),
            pq(
                "pq-fidelity",
                "fidelity.arrow",
                '6',
                40,
                0,
                &format!(r#"["{base_sha}","{high_sha}"]"#),
            ),
            pq(
                "pq-high-codes",
                "high.arrow",
                '7',
                2,
                48,
                &format!(r#"["{high_sha}"]"#),
            ),
            artifact("v32-page-locations-parquet", "page-locations.parquet", 'b',),
            "b701eada33a5d6782f9ebb0adaac5fd7573da40f",
            "a".repeat(64),
        )
        .into_bytes()
    }

    #[test]
    fn v32_s3_qualify_manifest_binds_every_resident_artifact_before_use() {
        // Break caught: serving combines hierarchy, PQ, and layout objects from
        // different constructions or discovers an artifact name from storage.
        let directory = tempdir().unwrap();
        let path = directory.path().join("manifest.json");
        let bytes = manifest_bytes();
        fs::write(&path, &bytes).unwrap();
        let argument = ArtifactArg {
            path,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            encoded_bytes: bytes.len() as u64,
        };
        let manifest = read_manifest(&argument).unwrap();
        assert_eq!(manifest.source_rows, 40);
        assert_eq!(manifest.hierarchy[0].1.role, "v27-roots-arrow");
        assert_eq!(manifest.pq.len(), 5);
        assert_eq!(manifest.layout[1].1.role, "v32-page-ranges-parquet");
        assert_eq!(manifest.page_locations.0, "page-locations.parquet");
        assert_eq!(manifest.page_locations.1.role, "v32-page-locations-parquet");
        assert_eq!(
            manifest.page_prefixes.standard(),
            "s3://bucket/v30/build-a0001/pages/"
        );
        assert_eq!(manifest.page_prefixes.express(), None);
        assert_eq!(
            manifest.routing_arms,
            [(64, 65_536), (128, 131_072), (256, 262_144)]
        );
        assert_eq!(manifest.routing_candidate_depth, 12_288);
        assert_eq!(manifest.routing_page_count, 16);

        let mut missing_prefix: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        missing_prefix["serving"]
            .as_object_mut()
            .unwrap()
            .remove("express_page_prefix");
        let mut missing_prefix = serde_json::to_vec(&super::canonical(missing_prefix)).unwrap();
        missing_prefix.push(b'\n');
        fs::write(&argument.path, &missing_prefix).unwrap();
        let missing_prefix_argument = ArtifactArg {
            path: argument.path.clone(),
            sha256: format!("{:x}", Sha256::digest(&missing_prefix)),
            encoded_bytes: missing_prefix.len() as u64,
        };
        assert!(read_manifest(&missing_prefix_argument).is_err());

        let mut corrupted = argument.clone();
        let replacement = if corrupted.sha256.starts_with('f') {
            "e"
        } else {
            "f"
        };
        corrupted.sha256.replace_range(0..1, replacement);
        assert!(read_manifest(&corrupted).is_err());

        let mut logical_source_drifted = bytes.clone();
        let logical_source_offset = logical_source_drifted
            .windows(b"v32-logical-sources-arrow".len())
            .position(|window| window == b"v32-logical-sources-arrow")
            .unwrap();
        logical_source_drifted[logical_source_offset] = b'x';
        fs::write(&argument.path, &logical_source_drifted).unwrap();
        let logical_source_argument = ArtifactArg {
            path: argument.path.clone(),
            sha256: format!("{:x}", Sha256::digest(&logical_source_drifted)),
            encoded_bytes: logical_source_drifted.len() as u64,
        };
        assert!(read_manifest(&logical_source_argument).is_err());

        let logical_source_name_drifted = manifest_bytes()
            .windows(b"logical-sources.arrow".len())
            .position(|window| window == b"logical-sources.arrow")
            .unwrap();
        let mut logical_source_name_bytes = manifest_bytes();
        logical_source_name_bytes[logical_source_name_drifted] = b'x';
        fs::write(&logical_source_argument.path, &logical_source_name_bytes).unwrap();
        let logical_source_name_argument = ArtifactArg {
            path: logical_source_argument.path.clone(),
            sha256: format!("{:x}", Sha256::digest(&logical_source_name_bytes)),
            encoded_bytes: logical_source_name_bytes.len() as u64,
        };
        assert!(read_manifest(&logical_source_name_argument).is_err());

        let drifted = bytes
            .windows(b"pq24-codebook".len())
            .position(|window| window == b"pq24-codebook")
            .unwrap();
        let mut drifted_bytes = bytes;
        drifted_bytes[drifted] = b'x';
        fs::write(&argument.path, &drifted_bytes).unwrap();
        let drifted_argument = ArtifactArg {
            sha256: format!("{:x}", Sha256::digest(&drifted_bytes)),
            encoded_bytes: drifted_bytes.len() as u64,
            ..argument
        };
        assert!(read_manifest(&drifted_argument).is_err());

        let mut source_drifted = manifest_bytes();
        let source_offset = source_drifted
            .windows(40)
            .position(|window| window == b"b701eada33a5d6782f9ebb0adaac5fd7573da40f")
            .unwrap();
        source_drifted[source_offset] = b'z';
        fs::write(&drifted_argument.path, &source_drifted).unwrap();
        let source_argument = ArtifactArg {
            sha256: format!("{:x}", Sha256::digest(&source_drifted)),
            encoded_bytes: source_drifted.len() as u64,
            ..drifted_argument
        };
        assert!(read_manifest(&source_argument).is_err());

        let packing_drifted = manifest_bytes()
            .windows(b"routing-microleaf-global-v1".len())
            .position(|window| window == b"routing-microleaf-global-v1")
            .unwrap();
        let mut packing_bytes = manifest_bytes();
        packing_bytes[packing_drifted] = b'x';
        fs::write(&source_argument.path, &packing_bytes).unwrap();
        let packing_argument = ArtifactArg {
            sha256: format!("{:x}", Sha256::digest(&packing_bytes)),
            encoded_bytes: packing_bytes.len() as u64,
            ..source_argument
        };
        assert!(read_manifest(&packing_argument).is_err());

        let mut routing_bytes = manifest_bytes();
        let routing_drifted = routing_bytes
            .windows(b"hierarchical-routing-microleaf-pq-v1".len())
            .position(|window| window == b"hierarchical-routing-microleaf-pq-v1")
            .unwrap();
        routing_bytes[routing_drifted] = b'x';
        fs::write(&packing_argument.path, &routing_bytes).unwrap();
        let routing_argument = ArtifactArg {
            sha256: format!("{:x}", Sha256::digest(&routing_bytes)),
            encoded_bytes: routing_bytes.len() as u64,
            ..packing_argument
        };
        assert!(read_manifest(&routing_argument).is_err());
    }

    #[test]
    fn v32_s3_qualify_local_store_and_stdout_are_content_addressed() {
        // Break caught: the qualifier discovers page names, performs more than
        // one wave, or emits noncanonical/claim-eligible output.
        let directory = tempdir().unwrap();
        let page = V27PageIdentity {
            ordinal: 7,
            sha256: "a".repeat(64),
            encoded_bytes: 3,
            primary_rows: 1,
            replica_rows: 0,
        };
        fs::write(
            directory.path().join(format!("{}.arrow", page.sha256)),
            b"abc",
        )
        .unwrap();
        let bodies = LocalPageStore {
            directory: directory.path().to_path_buf(),
            suffix: ".arrow".to_owned(),
        }
        .read_wave(std::slice::from_ref(&page))
        .unwrap();
        assert_eq!(bodies, vec![b"abc".to_vec()]);

        let result = V32SearchResult {
            matches: vec![V32Match {
                source_ordinal: 9,
                squared_distance: 0.25,
            }],
            work: V32SearchWork {
                routing: V32RoutingWork {
                    roots_scored: 16,
                    leaves_eligible: 64,
                    leaves_scanned: 64,
                    query_table_pairs_built: 8,
                    peak_query_table_pairs_live: 1,
                    codes_scanned: 40,
                    candidates_retained: 12,
                    pages_considered: 3,
                    selected_pages: 1,
                },
                get_count: 1,
                encoded_bytes: 3,
                decoded_rows: 1,
                unique_rows: 1,
            },
        };
        assert_eq!(
            String::from_utf8(
                result_bytes(
                    &result,
                    7_500_000,
                    42_000_000,
                    123_456_000,
                    SearchPhaseTiming {
                        routing_cpu_ns: 1_000_000,
                        page_read_cpu_ns: 2_000_000,
                        exact_rerank_cpu_ns: 3_000_000,
                        routing_elapsed_ns: 5_000_000,
                        page_read_elapsed_ns: 20_000_000,
                        exact_rerank_elapsed_ns: 10_000_000,
                    },
                )
                .unwrap(),
            )
            .unwrap(),
            concat!(
                "{\"claim_eligible\":false,\"matches\":[{\"source_ordinal\":9,",
                "\"squared_distance\":0.25}],\"schema_version\":2,",
                "\"timing\":{\"elapsed_ns\":42000000,\"exact_rerank_cpu_ns\":3000000,",
                "\"exact_rerank_elapsed_ns\":10000000,\"page_read_cpu_ns\":2000000,",
                "\"page_read_elapsed_ns\":20000000,\"peak_rss_bytes\":123456000,",
                "\"process_cpu_ns\":7500000,\"routing_cpu_ns\":1000000,",
                "\"routing_elapsed_ns\":5000000},\"work\":{",
                "\"decoded_rows\":1,\"encoded_bytes\":3,\"get_count\":1,",
                "\"routing\":{\"candidates_retained\":12,\"codes_scanned\":40,",
                "\"leaves_eligible\":64,\"leaves_scanned\":64,\"pages_considered\":3,\"peak_query_table_pairs_live\":1,\"query_table_pairs_built\":8,\"roots_scored\":16,",
                "\"selected_pages\":1},\"unique_rows\":1}}\n"
            )
        );
    }

    #[test]
    fn v32_s3_qualify_execution_reads_only_explicit_authenticated_artifacts() {
        // Break caught: the executable bypasses the serving manifest or begins
        // page access before all resident artifacts and the query authenticate.
        let directory = tempdir().unwrap();
        let manifest_path = directory.path().join("manifest.json");
        let bytes = manifest_bytes();
        fs::write(&manifest_path, &bytes).unwrap();
        let args = Args {
            manifest: ArtifactArg {
                path: manifest_path,
                sha256: format!("{:x}", Sha256::digest(&bytes)),
                encoded_bytes: bytes.len() as u64,
            },
            artifact_dir: directory.path().to_path_buf(),
            query: ArtifactArg {
                path: directory.path().join("query.parquet"),
                sha256: "f".repeat(64),
                encoded_bytes: 1,
            },
            query_start: 0,
            query_count: 32,
            root_beam: 1,
            leaf_beam: 1,
            candidate_depth: 1,
            page_count: 1,
            k: 1,
            page_source: Some(PageSource::Local(directory.path().to_path_buf())),
            diagnostic: None,
        };
        let error = execute(args).unwrap_err().to_string();
        assert!(
            error.contains("page-locations.parquet"),
            "unexpected error: {error}"
        );
    }
}

#[cfg(not(test))]
fn main() {
    match parse_args(std::env::args().collect())
        .map_err(|error| invalid(&error))
        .and_then(execute)
    {
        Ok(bytes) => {
            use std::io::Write;
            if let Err(error) = std::io::stdout().write_all(&bytes) {
                eprintln!("v30_s3_qualify: {error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("v30_s3_qualify: {error}");
            std::process::exit(1);
        }
    }
}
