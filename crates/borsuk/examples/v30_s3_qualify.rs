//! Explicit authenticated local/S3 qualification boundary for the V30 page index.

use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc, time::Instant};

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    BorsukError, V27HierarchyArtifactIdentity, V27HierarchyArtifacts, V30DiagnosticArm, V30Index,
    V30LayoutArtifactIdentity, V30LayoutArtifacts, V30PageStore, V30PqArtifactIdentity,
    V30PqArtifacts, V30Router, V30RoutingTargetReport, V30RoutingTargetStage, V30SearchArm,
    V30SearchPhase, V30SearchResult,
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
    S3(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Args {
    manifest: ArtifactArg,
    artifact_dir: PathBuf,
    query: ArtifactArg,
    query_start: usize,
    query_count: usize,
    leaf_beam: usize,
    page_count: usize,
    k: usize,
    page_source: Option<PageSource>,
    diagnostic: Option<DiagnosticRequest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DiagnosticRequest {
    logical: u64,
    arm: V30DiagnosticArm,
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
    leaf_ranges: DiskArtifact,
    maximum_leaf_rows: u64,
    packing_algorithm: String,
    page_ranges: DiskArtifact,
    page_rows: usize,
    source_rows: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskPq {
    artifacts: Vec<DiskPqArtifact>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskRouting {
    algorithm: String,
    leaf_beam: usize,
    maximum_pages_per_leaf: usize,
    page_centroid_dimensions: usize,
    page_centroid_element: String,
    page_count: usize,
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
    hierarchy: DiskHierarchy,
    layout: DiskLayout,
    page_key_suffix: String,
    pq: DiskPq,
    routing: DiskRouting,
    schema_version: u8,
    source: DiskSource,
}

#[derive(Debug)]
struct Manifest {
    hierarchy: Vec<(String, V30LayoutArtifactIdentity)>,
    layout: Vec<(String, V30LayoutArtifactIdentity)>,
    pq: Vec<(String, V30PqArtifactIdentity)>,
    source_rows: u64,
    page_key_suffix: String,
    routing_leaf_beam: usize,
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
    if disk.schema_version != 2
        || disk.page_key_suffix != ".arrow"
        || disk.layout.source_rows == 0
        || disk.layout.maximum_leaf_rows == 0
        || disk.layout.maximum_leaf_rows > disk.layout.source_rows
        || disk.layout.maximum_leaf_rows > 65_536
        || disk.layout.packing_algorithm != "balanced-geometric-v1"
        || disk.layout.page_rows == 0
        || disk.layout.page_rows > 512
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
        || disk.routing.algorithm != "flat-leaf-page-centroid-v1"
        || disk.routing.leaf_beam == 0
        || disk.routing.leaf_beam > 512
        || disk.routing.maximum_pages_per_leaf != 64
        || disk.routing.page_centroid_dimensions != 96
        || disk.routing.page_centroid_element != "float16"
        || disk.routing.page_count != 16
    {
        return Err(invalid("V30 qualifier manifest constants differ"));
    }
    let hierarchy = vec![
        disk_identity(disk.hierarchy.roots, "v27-roots-arrow")?,
        disk_identity(disk.hierarchy.leaves, "v27-leaves-arrow")?,
    ];
    let layout = vec![
        disk_identity(disk.layout.leaf_ranges, "v30-leaf-ranges-arrow")?,
        disk_identity(disk.layout.page_ranges, "v30-page-ranges-parquet")?,
    ];
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
        routing_leaf_beam: disk.routing.leaf_beam,
        routing_page_count: disk.routing.page_count,
    })
}

#[derive(Clone)]
struct LocalPageStore {
    directory: PathBuf,
    suffix: String,
}

impl V30PageStore for LocalPageStore {
    fn read_wave(&self, pages: &[borsuk::V27PageIdentity]) -> borsuk::Result<Vec<Vec<u8>>> {
        pages
            .iter()
            .map(|page| {
                let path = self
                    .directory
                    .join(format!("{}{}", page.sha256, self.suffix));
                fs::read(&path).map_err(|source| BorsukError::Io { path, source })
            })
            .collect()
    }
}

#[derive(Clone)]
struct ObjectPageStore {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    suffix: String,
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

impl V30PageStore for ObjectPageStore {
    fn read_wave(&self, pages: &[borsuk::V27PageIdentity]) -> borsuk::Result<Vec<Vec<u8>>> {
        self.runtime.block_on(async {
            let reads = pages.iter().map(|page| {
                let store = Arc::clone(&self.store);
                let path = ObjectPath::from(format!(
                    "{}/{}{}",
                    self.prefix.as_ref(),
                    page.sha256,
                    self.suffix
                ));
                async move { Ok::<_, BorsukError>(store.get(&path).await?.bytes().await?.to_vec()) }
            });
            try_join_all(reads).await
        })
    }
}

fn result_bytes(
    result: &V30SearchResult,
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
                "leaves_scored": result.work.routing.leaves_scored,
                "pages_considered": result.work.routing.pages_considered,
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
    report: V30RoutingTargetReport,
) -> borsuk::Result<Vec<u8>> {
    let stage = match report.stage {
        V30RoutingTargetStage::LeafFrontier => "leaf-frontier",
        V30RoutingTargetStage::CandidateRetention => "candidate-retention",
        V30RoutingTargetStage::PageReducer => "page-reducer",
        V30RoutingTargetStage::SelectedPage => "selected-page",
    };
    let value = serde_json::json!({
        "claim_eligible": false,
        "diagnostic": {
            "candidate_rank": report.candidate_rank,
            "first_unique_page_rank": report.first_unique_page_rank,
            "leaf_ordinal": report.leaf_ordinal,
            "logical": report.logical,
            "page_ordinal": report.page_ordinal,
            "reciprocal_rank_selected": report.reciprocal_rank_selected,
            "stage": stage,
        },
        "query_ordinal": query_ordinal,
        "schema_version": 1,
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

fn read_query(argument: &ArtifactArg, query_row: usize) -> borsuk::Result<[f32; 96]> {
    let bytes = read_bytes(argument, "query")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
    if builder.schema().as_ref() != &query_schema() {
        return Err(invalid("V30 qualifier query Parquet schema differs"));
    }
    let mut offset = 0_usize;
    for batch in builder.build()? {
        let batch = batch?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V30 qualifier query nullability differs"));
        }
        if query_row < offset + batch.num_rows() {
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
            let start = (query_row - offset) * 96;
            let mut query: [f32; 96] = values.values()[start..start + 96]
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
            return Ok(query);
        }
        offset += batch.num_rows();
    }
    Err(invalid("V30 qualifier query row differs"))
}

fn run_batch<S: V30PageStore>(
    router: V30Router,
    store: S,
    arm: V30SearchArm,
    query: &ArtifactArg,
    query_start: usize,
    query_count: usize,
    k: usize,
) -> borsuk::Result<Vec<u8>> {
    let mut results = Vec::with_capacity(query_count);
    let index = V30Index::new(router, store, arm)?;
    let query_end = query_start
        .checked_add(query_count)
        .ok_or_else(|| invalid("V30 qualifier query range overflows"))?;
    for query_row in query_start..query_end {
        let query_vector = read_query(query, query_row)?;
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
                V30SearchPhase::RoutingComplete => {
                    phases.routing_cpu_ns = cpu_ns;
                    phases.routing_elapsed_ns = elapsed_ns;
                }
                V30SearchPhase::PageReadComplete => {
                    phases.page_read_cpu_ns = cpu_ns;
                    phases.page_read_elapsed_ns = elapsed_ns;
                }
                V30SearchPhase::ExactRerankComplete => {
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

fn execute(args: Args) -> borsuk::Result<Vec<u8>> {
    let manifest = read_manifest(&args.manifest)?;
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
    let router = V30Router::from_artifacts(&hierarchy, &pq, &layout)?;
    if args.leaf_beam != manifest.routing_leaf_beam
        || args.page_count != manifest.routing_page_count
    {
        return Err(invalid("V30 qualifier routing manifest differs"));
    }
    let arm = V30SearchArm {
        leaf_beam: args.leaf_beam,
        page_count: args.page_count,
    };
    if let Some(diagnostic) = args.diagnostic {
        let query = read_query(&args.query, args.query_start)?;
        let report = router
            .diagnose_logicals(&query, diagnostic.arm, &[diagnostic.logical])?
            .into_iter()
            .next()
            .ok_or_else(|| invalid("V30 qualifier diagnostic result differs"))?;
        return diagnostic_bytes(args.query_start, report);
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
        Some(PageSource::S3(uri)) => {
            let url = Url::parse(&uri).map_err(|_| invalid("V30 qualifier S3 URI differs"))?;
            let options = std::env::vars().filter(|(key, _)| {
                matches!(
                    key.as_str(),
                    "AWS_ACCESS_KEY_ID"
                        | "AWS_SECRET_ACCESS_KEY"
                        | "AWS_SESSION_TOKEN"
                        | "AWS_REGION"
                )
            });
            let (store, prefix) = parse_url_opts(&url, options)?;
            let store: Arc<dyn ObjectStore> = store.into();
            let runtime = Arc::new(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|source| BorsukError::Io {
                        path: PathBuf::from("tokio-runtime"),
                        source,
                    })?,
            );
            run_batch(
                router,
                ObjectPageStore {
                    store,
                    prefix,
                    suffix: manifest.page_key_suffix,
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
    let leaf_beam = number(&mut values, "leaf-beam")?;
    let page_count = number(&mut values, "page-count")?;
    let k = number(&mut values, "k")?;
    let diagnostic_logical = values
        .remove("diagnose-logical")
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| argument_error("--diagnose-logical type differs"))
        })
        .transpose()?;
    let diagnostic = diagnostic_logical
        .map(|logical| -> Result<DiagnosticRequest, String> {
            Ok(DiagnosticRequest {
                logical,
                arm: V30DiagnosticArm {
                    root_beam: number(&mut values, "root-beam")?,
                    leaf_beam,
                    candidate_depth: number(&mut values, "candidate-depth")?,
                    page_count,
                },
            })
        })
        .transpose()?;
    let local = values.remove("local-page-dir").map(PathBuf::from);
    let s3 = values.remove("s3-page-prefix");
    let page_source = match (diagnostic, local, s3) {
        (Some(_), None, None) => None,
        (None, Some(path), None) if path.is_absolute() => Some(PageSource::Local(path)),
        (None, None, Some(uri)) if uri.starts_with("s3://") && !uri.ends_with('/') => {
            Some(PageSource::S3(uri))
        }
        _ => return Err(argument_error("exactly one page source is required")),
    };
    if !artifact_dir.is_absolute()
        || match diagnostic {
            Some(_) => query_count != 1,
            None => query_count != 32,
        }
        || leaf_beam == 0
        || diagnostic.is_some_and(|request| {
            request.arm.root_beam == 0
                || request.arm.candidate_depth == 0
                || request.arm.candidate_depth > 12_288
        })
        || page_count == 0
        || page_count > 16
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
        leaf_beam,
        page_count,
        k,
        page_source,
        diagnostic,
    })
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf, sync::Arc};

    use borsuk::{
        V27PageIdentity, V30Match, V30PageStore, V30RoutingTargetReport, V30RoutingTargetStage,
        V30RoutingWork, V30SearchResult, V30SearchWork,
    };
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::{
        Args, ArtifactArg, LocalPageStore, ObjectPageStore, PageSource, SearchPhaseTiming,
        diagnostic_bytes, execute, parse_args, peak_rss_bytes, process_cpu_nanoseconds,
        read_manifest, result_bytes, run_batch,
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
            "--leaf-beam",
            "64",
            "--page-count",
            "16",
            "--k",
            "10",
            "--s3-page-prefix",
            "s3://frozen/pages",
        ]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect()
    }

    #[test]
    fn v30_s3_qualify_process_cpu_clock_resolves_below_the_latency_gate() {
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
    fn v30_s3_qualify_reports_process_peak_rss_for_the_release_gate() {
        let peak = peak_rss_bytes().unwrap();
        assert!(peak > 0);
        assert_eq!(peak % 1024, 0);
    }

    struct NonClonePageStore;

    impl V30PageStore for NonClonePageStore {
        fn read_wave(&self, _pages: &[V27PageIdentity]) -> borsuk::Result<Vec<Vec<u8>>> {
            unreachable!("compile-only batch ownership contract")
        }
    }

    #[test]
    fn v30_s3_qualify_batch_reuses_one_index_without_cloning_router_or_store() {
        // Break caught: every query clones the resident router/store and builds
        // a fresh index instead of reusing one immutable serving instance.
        let _runner = run_batch::<NonClonePageStore>;
    }

    #[test]
    fn v30_s3_qualify_object_store_reuses_one_runtime_across_read_waves() {
        // Break caught: every S3 query builds and tears down a Tokio runtime,
        // adding scheduler setup to the measured serving CPU path.
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let store = ObjectPageStore {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: object_store::path::Path::from("pages"),
            suffix: ".arrow".to_owned(),
            runtime: Arc::clone(&runtime),
        };
        assert!(store.read_wave(&[]).unwrap().is_empty());
        assert!(store.read_wave(&[]).unwrap().is_empty());
        assert_eq!(Arc::strong_count(&runtime), 2);
    }

    #[test]
    fn v30_s3_qualify_parser_requires_explicit_authority_and_one_page_source() {
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
            Some(PageSource::S3("s3://frozen/pages".to_owned()))
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
            "--root-beam",
            "--candidate-depth",
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
            .position(|value| value == "--s3-page-prefix")
            .unwrap();
        diagnostic.drain(source..=source + 1);
        let count = diagnostic
            .iter()
            .position(|value| value == "--query-count")
            .unwrap();
        diagnostic[count + 1] = "1".to_owned();
        diagnostic.extend([
            "--diagnose-logical".to_owned(),
            "25".to_owned(),
            "--root-beam".to_owned(),
            "8".to_owned(),
            "--candidate-depth".to_owned(),
            "12288".to_owned(),
        ]);
        let parsed = parse_args(diagnostic).unwrap();
        assert_eq!(parsed.diagnostic.unwrap().logical, 25);
        assert_eq!(parsed.page_source, None);
    }

    #[test]
    fn v30_s3_qualify_diagnostic_output_is_canonical_and_names_the_loss_boundary() {
        // Break caught: the fail-fast diagnostic performs a page read or hides
        // whether routing, candidate retention, or page reduction lost truth.
        let bytes = diagnostic_bytes(
            7,
            V30RoutingTargetReport {
                logical: 25,
                leaf_ordinal: 3,
                page_ordinal: 11,
                candidate_rank: None,
                first_unique_page_rank: Some(12),
                stage: V30RoutingTargetStage::CandidateRetention,
                reciprocal_rank_selected: false,
            },
        )
        .unwrap();
        assert_eq!(
            bytes,
            b"{\"claim_eligible\":false,\"diagnostic\":{\"candidate_rank\":null,\"first_unique_page_rank\":12,\"leaf_ordinal\":3,\"logical\":25,\"page_ordinal\":11,\"reciprocal_rank_selected\":false,\"stage\":\"candidate-retention\"},\"query_ordinal\":7,\"schema_version\":1}\n"
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
                "{{\"hierarchy\":{{\"leaves\":{},\"roots\":{}}},",
                "\"layout\":{{\"leaf_ranges\":{},\"maximum_leaf_rows\":24,",
                "\"packing_algorithm\":\"balanced-geometric-v1\",\"page_ranges\":{},",
                "\"page_rows\":128,\"source_rows\":40}},",
                "\"page_key_suffix\":\".arrow\",",
                "\"pq\":{{\"artifacts\":[{},{},{},{},{}]}},",
                "\"routing\":{{\"algorithm\":\"flat-leaf-page-centroid-v1\",",
                "\"leaf_beam\":4,\"maximum_pages_per_leaf\":64,",
                "\"page_centroid_dimensions\":96,\"page_centroid_element\":\"float16\",",
                "\"page_count\":16}},",
                "\"schema_version\":2,",
                "\"source\":{{\"commit\":\"{}\",\"corpus_manifest_bytes\":4096,",
                "\"corpus_manifest_sha256\":\"{}\",",
                "\"corpus_manifest_uri\":\"s3://bucket/deep-10m/corpus.json\",",
                "\"dataset_id\":\"deep-image-96\"}}}}\n"
            ),
            artifact("v27-leaves-arrow", "leaves.arrow", '2'),
            artifact("v27-roots-arrow", "roots.arrow", '1'),
            artifact("v30-leaf-ranges-arrow", "leaf-ranges.arrow", '8'),
            artifact("v30-page-ranges-parquet", "page-ranges.parquet", '9'),
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
            "b701eada33a5d6782f9ebb0adaac5fd7573da40f",
            "a".repeat(64),
        )
        .into_bytes()
    }

    #[test]
    fn v30_s3_qualify_manifest_binds_every_resident_artifact_before_use() {
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
        assert_eq!(manifest.layout[1].1.role, "v30-page-ranges-parquet");
        assert_eq!(manifest.routing_leaf_beam, 4);
        assert_eq!(manifest.routing_page_count, 16);

        let mut corrupted = argument.clone();
        corrupted.sha256.replace_range(0..1, "f");
        assert!(read_manifest(&corrupted).is_err());

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
            .windows(b"balanced-geometric-v1".len())
            .position(|window| window == b"balanced-geometric-v1")
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
            .windows(b"flat-leaf-page-centroid-v1".len())
            .position(|window| window == b"flat-leaf-page-centroid-v1")
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
    fn v30_s3_qualify_local_store_and_stdout_are_content_addressed() {
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

        let result = V30SearchResult {
            matches: vec![V30Match {
                source_ordinal: 9,
                squared_distance: 0.25,
            }],
            work: V30SearchWork {
                routing: V30RoutingWork {
                    roots_scored: 16,
                    leaves_scored: 64,
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
                "\"leaves_scored\":64,\"pages_considered\":3,\"roots_scored\":16,",
                "\"selected_pages\":1},\"unique_rows\":1}}\n"
            )
        );
    }

    #[test]
    fn v30_s3_qualify_execution_reads_only_explicit_authenticated_artifacts() {
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
            leaf_beam: 1,
            page_count: 1,
            k: 1,
            page_source: Some(PageSource::Local(directory.path().to_path_buf())),
            diagnostic: None,
        };
        let error = execute(args).unwrap_err().to_string();
        assert!(error.contains("roots.arrow"), "unexpected error: {error}");
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
