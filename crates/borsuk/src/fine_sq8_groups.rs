//! UNVERIFIED source-only 100k falsifier. No 100M, maintenance, RSS, S3 or vendor
//! claim. Only this new root format serves PQ graphs without an FP16 plane.
use crate::{
    budgeted_page_rank::cover_pages,
    exact_sq8_nominee::{ScoredNominee, Sq8Geometry, score_nominees},
    hierarchical_semantic_cells::{
        Artifact, BuildConfig, Prototype, Result, read_source_probe_artifact,
    },
    pq64_nominee::{Pq64Codes, fit_source_codes},
    resident_vector_graph::{GraphSearchWorkspace, PqGraphIdentity, PqVectorGraph},
    returned_sq8::{ReturnedRange, rank_returned_ranges_excluding},
    sq8_page_authority::PageAuthority,
    sq8_source::cosine_vector,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::Write,
    ops::Range,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::Path,
    sync::Arc,
};

pub const BUILD_SCHEMA: &str = "borsuk-fine-sq8-build-v1";
pub use pack_diagnostic::sq4_diagnostic;
pub const SCHEMA: &str = "borsuk-fine-sq8-v1";
pub const GROUP_ROWS: usize = 16;
pub const MAX_GETS: usize = 256;
pub const MAX_BYTES: usize = 16 * 1024 * 1024;
const ROOT_CAP: usize = 65536;
const FIXED_BYTES: usize = 2 * 1024 * 1024;
fn require(ok: bool, msg: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(msg.into()) }
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn digest(hex: &str) -> Result<[u8; 32]> {
    require(
        hex.len() == 64
            && hex
                .bytes()
                .all(|v| v.is_ascii_hexdigit() && !v.is_ascii_uppercase()),
        "fine digest",
    )?;
    let mut out = [0; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)?;
    }
    Ok(out)
}
fn secure_file(artifact: &Artifact) -> Result<File> {
    digest(&artifact.sha256)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(&artifact.path)?;
    require(
        file.metadata()?.is_file() && file.metadata()?.len() == artifact.bytes as u64,
        "fine regular exact file",
    )?;
    Ok(file)
}
fn authenticated_file(artifact: &Artifact) -> Result<File> {
    let file = secure_file(artifact)?;
    let mut h = Sha256::new();
    let mut offset = 0;
    let mut buffer = [0; 65536];
    while offset < artifact.bytes {
        let count = buffer.len().min(artifact.bytes - offset);
        file.read_exact_at(&mut buffer[..count], offset as u64)?;
        h.update(&buffer[..count]);
        offset += count;
    }
    require(
        format!("{:x}", h.finalize()) == artifact.sha256,
        "fine streamed SHA256",
    )?;
    Ok(file)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FineBuildReceipt {
    pub modeled_build_payload_bytes: usize,
    pub modeled_output_and_staging_bytes: usize,
    pub actual_graph_capacity_bytes: usize,
    pub construction_graph_capacity_bytes: usize,
    pub actual_pq_capacity_bytes: usize,
    pub source_rows: usize,
    pub sq8_body_bytes: usize,
}
fn write_body(directory: &Path, output: &Path, name: &str, body: &[u8]) -> Result<Artifact> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join(name))?;
    file.write_all(body)?;
    file.sync_all()?;
    Ok(Artifact {
        path: output.join(name),
        bytes: body.len(),
        sha256: hash(body),
    })
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FineBuildConfig {
    pub schema: String,
    pub primary_root: Artifact,
    pub max_build_payload_bytes: usize,
    pub max_output_bytes: usize,
}
/// Caller supplies distinct preexisting pinned generations once, plus external
/// budgets. This is allocation arithmetic; measured RSS remains a separate gate.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResidentLimits {
    pub max_peak_payload_bytes: usize,
    pub pinned_generation_bytes: usize,
    pub active_queries: usize,
    pub delta_bytes: usize,
    pub maintenance_bytes: usize,
    pub runtime_bytes: usize,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceReceipt {
    pub graph_capacity_bytes: usize,
    pub pq_capacity_bytes: usize,
    pub map_capacity_bytes: usize,
    pub group_hash_bytes: usize,
    pub resident_bytes: usize,
    pub workspace_bytes_per_query: usize,
    pub query_payload_bytes: usize,
    pub startup_transient_bytes: usize,
    pub admitted_peak_bytes: usize,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: String,
    primary_root: Artifact,
    original: BuildConfig,
    identity: PqGraphIdentity,
    low: Vec<f32>,
    step: Vec<f32>,
    pq: Artifact,
    graph: Artifact,
    records: Artifact,
    groups: Artifact,
    order: Artifact,
    build: FineBuildReceipt,
}
fn layout_identity(
    root: &Artifact,
    records: &Artifact,
    order: &Artifact,
    low: &[f32],
    step: &[f32],
) -> Result<[u8; 32]> {
    Ok(Sha256::digest(serde_json::to_vec(&(
        root.sha256.as_str(),
        records.sha256.as_str(),
        order.sha256.as_str(),
        low.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
        step.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
    ))?)
    .into())
}
fn query_digest(query: &[f32]) -> String {
    let mut h = Sha256::new();
    for x in query {
        h.update(x.to_le_bytes());
    }
    format!("{:x}", h.finalize())
}

// Shared metadata-only binding. In particular, layout_identity hashes f32
// bits, preserving signed zero; the packing diagnostic never opens payloads.
fn validate_manifest(manifest: &Manifest, root: &Artifact) -> Result<()> {
    let n = manifest.identity.rows;
    let d = manifest.identity.dimensions;
    require((2..=100_000).contains(&n) && (1..=768).contains(&d), "fine root geometry")?;
    require(
        manifest.schema == SCHEMA
            && manifest.low.len() == d
            && manifest.step.len() == d
            && manifest.low.iter().all(|v| v.is_finite())
            && manifest.step.iter().all(|v| v.is_finite() && *v > 0.0)
            && manifest.records.bytes == n * (d + 12)
            && manifest.order.bytes == n * 8
            && manifest.groups.bytes == n.div_ceil(16) * 32
            && manifest.graph.bytes <= n * 512
            && manifest.pq.bytes == 24 + 64 * 256 * d.div_ceil(64) * 4 + n * 64
            && manifest.identity.source == digest(&manifest.original.canonical.sha256)?
            && manifest.identity.pq == digest(&manifest.pq.sha256)?
            && manifest.identity.layout == layout_identity(&manifest.primary_root,
                &manifest.records, &manifest.order, &manifest.low, &manifest.step)?,
        "fine root binding",
    )?;
    let parent = root.path.parent().ok_or("fine root parent")?;
    for (a, name) in [(&manifest.pq, "pq.bin"), (&manifest.graph, "graph.bin"),
        (&manifest.records, "records.bin"), (&manifest.groups, "groups.bin"), (&manifest.order, "order.bin")] {
        require(a.path == parent.join(name), "fine artifact path binding")?;
    }
    Ok(())
}
fn encode_pq(pq: &Pq64Codes) -> Vec<u8> {
    let mut body = Vec::with_capacity(24 + pq.books().len() * 4 + pq.codes().len());
    body.extend_from_slice(b"BORSPQ01");
    body.extend_from_slice(&(pq.rows() as u64).to_le_bytes());
    body.extend_from_slice(&(pq.dimensions() as u64).to_le_bytes());
    for value in pq.books() {
        body.extend_from_slice(&value.to_le_bytes());
    }
    body.extend_from_slice(pq.codes());
    body
}
fn decode_pq(body: &[u8], n: usize, d: usize) -> Result<Pq64Codes> {
    let books = 64 * 256 * d.div_ceil(64);
    require(
        body.len() == 24 + 4 * books + n * 64
            && &body[..8] == b"BORSPQ01"
            && u64::from_le_bytes(body[8..16].try_into()?) == n as u64
            && u64::from_le_bytes(body[16..24].try_into()?) == d as u64,
        "fine PQ format",
    )?;
    Pq64Codes::new(
        n,
        d,
        body[24..24 + 4 * books]
            .chunks_exact(4)
            .map(|v| f32::from_le_bytes(v.try_into().unwrap()))
            .collect(),
        body[24 + 4 * books..].to_vec(),
    )
    .map_err(|e| format!("fine PQ: {e:?}").into())
}
fn model(
    n: usize,
    d: usize,
    graph_encoded: usize,
    root_bytes: usize,
    limits: &ResidentLimits,
) -> Result<ResourceReceipt> {
    require(
        (2..=100_000).contains(&n) && (1..=768).contains(&d) && limits.active_queries > 0,
        "fine bounded geometry/concurrency",
    )?;
    let graph = n.checked_mul(512).ok_or("fine graph overflow")?;
    let pq = n
        .checked_mul(68)
        .and_then(|x| x.checked_add(64 * 256 * d.div_ceil(64) * 4))
        .ok_or("fine PQ overflow")?;
    let map = n.checked_mul(8).ok_or("fine map overflow")?;
    let hashes = n.div_ceil(16).checked_mul(32).ok_or("fine hash overflow")?;
    let resident = graph
        .checked_add(pq)
        .and_then(|x| x.checked_add(map))
        .and_then(|x| x.checked_add(hashes))
        .and_then(|x| x.checked_add(FIXED_BYTES))
        .ok_or("fine resident overflow")?;
    // Reuse the scorer's conservative allowance: 3 payload copies + 256 bytes
    // per fetched row; heaps can retain <=65536 attempted graph scores.
    let query = crate::returned_sq8::query_payload_bytes(
        Sq8Geometry {
            rows: n,
            dimensions: d,
        },
        MAX_BYTES as u64,
        (8 * 1024 * 1024) as u64,
        1,
    )
    .map_err(|e| format!("fine query model: {e:?}"))? as usize;
    let workspace = n * 4;
    let transients = graph_encoded
        .checked_add(pq)
        .and_then(|x| x.checked_add(hashes))
        .and_then(|x| x.checked_add(map))
        .and_then(|x| x.checked_add(n * 16))
        .and_then(|x| x.checked_add(root_bytes.checked_mul(8)?))
        .and_then(|x| x.checked_add(FIXED_BYTES))
        .ok_or("fine startup overflow")?;
    let concurrent = query
        .checked_add(workspace)
        .and_then(|x| x.checked_mul(limits.active_queries))
        .ok_or("fine concurrency overflow")?;
    let peak = resident
        .checked_add(limits.pinned_generation_bytes)
        .and_then(|x| x.checked_add(concurrent.max(transients)))
        .and_then(|x| x.checked_add(limits.delta_bytes))
        .and_then(|x| x.checked_add(limits.maintenance_bytes))
        .and_then(|x| x.checked_add(limits.runtime_bytes))
        .ok_or("fine aggregate overflow")?;
    require(
        peak <= limits.max_peak_payload_bytes,
        "fine aggregate admission before bodies",
    )?;
    Ok(ResourceReceipt {
        graph_capacity_bytes: graph,
        pq_capacity_bytes: pq,
        map_capacity_bytes: map,
        group_hash_bytes: hashes,
        resident_bytes: resident,
        workspace_bytes_per_query: workspace,
        query_payload_bytes: query,
        startup_transient_bytes: transients,
        admitted_peak_bytes: peak,
    })
}

pub struct FineSq8Index {
    root_sha256: String,
    manifest: Manifest,
    pq: Pq64Codes,
    graph: PqVectorGraph,
    authority: PageAuthority,
    records: Option<File>,
    ids: Vec<i64>,
    delta_budget: usize,
    pub resources: ResourceReceipt,
}
impl FineSq8Index {
    /// Metadata-only admission, for aggregate evaluator/multiple-pin reservation.
    /// Does not open graph, codes, group table or record payload.
    pub fn admission(root: &Artifact, limits: &ResidentLimits) -> Result<ResourceReceipt> {
        let root_peak = root
            .bytes
            .checked_mul(8)
            .and_then(|v| v.checked_add(limits.pinned_generation_bytes))
            .and_then(|v| v.checked_add(limits.delta_bytes))
            .and_then(|v| v.checked_add(limits.maintenance_bytes))
            .and_then(|v| v.checked_add(limits.runtime_bytes))
            .ok_or("fine root aggregate overflow")?;
        require(
            root.bytes <= ROOT_CAP && root_peak <= limits.max_peak_payload_bytes,
            "fine root aggregate admission",
        )?;
        let manifest: Manifest =
            serde_json::from_slice(&read_source_probe_artifact(root, ROOT_CAP)?)?;
        require(manifest.schema == SCHEMA, "fine root schema")?;
        model(
            manifest.identity.rows,
            manifest.identity.dimensions,
            manifest.graph.bytes,
            root.bytes,
            limits,
        )
    }
    /// Source-only bounded builder. The retained primary root is the sole input
    /// authority; original descriptors cannot be substituted by the caller.
    pub fn build(config: &FineBuildConfig, output: &Path) -> Result<Artifact> {
        require(
            config.schema == BUILD_SCHEMA
                && config.max_build_payload_bytes <= 8usize * 1024 * 1024 * 1024
                && config.max_output_bytes <= 8usize * 1024 * 1024 * 1024,
            "fine build schema/caps",
        )?;
        require(
            !output.try_exists()? && std::fs::symlink_metadata(output).is_err(),
            "fine output exists",
        )?;
        let metadata = Prototype::admit_root_metadata(&config.primary_root, ROOT_CAP * 8)?;
        let n = metadata.rows;
        let d = metadata.dimensions;
        require(
            (2..=100_000).contains(&n) && (1..=768).contains(&d),
            "fine bounded build geometry",
        )?;
        // Conservative resident source + HNSW build clones/scratch + PQ fitter,
        // full SQ8 output, authenticated directories and all serialization copies.
        let build_bytes = n
            .checked_mul(d * 24 + 8192)
            .and_then(|x| x.checked_add(metadata.directory_admission.modeled_preload_peak_bytes))
            .and_then(|x| x.checked_add(16 * 1024 * 1024))
            .ok_or("fine build overflow")?;
        let output_bound = n
            .checked_mul(d + 12 + 512 + 64 + 8 + 2)
            .and_then(|x| x.checked_add(4 * 1024 * 1024))
            .and_then(|x| x.checked_mul(2))
            .ok_or("fine output overflow")?;
        require(
            build_bytes <= config.max_build_payload_bytes
                && output_bound <= config.max_output_bytes,
            "fine build aggregate admission",
        )?;
        let primary = Prototype::open_for_source_probes(
            &config.primary_root,
            metadata.directory_admission.modeled_preload_peak_bytes,
        )?;
        let original = primary.source_identity().clone();
        require(
            original.canonical.bytes == n * (8 + 4 * d)
                && original.order.bytes == n * 8
                && original.sq8.bytes == n * (d + 12),
            "fine original lengths",
        )?;
        let canonical = authenticated_file(&original.canonical)?;
        let sq8 = authenticated_file(&original.sq8)?;
        let order_body = read_source_probe_artifact(&original.order, n * 8)?;
        let mut inverse = vec![usize::MAX; n];
        for (physical, encoded) in order_body.chunks_exact(8).enumerate() {
            let id = usize::try_from(u64::from_le_bytes(encoded.try_into()?))?;
            require(
                id < n && inverse[id] == usize::MAX,
                "fine source order bijection",
            )?;
            inverse[id] = physical;
        }
        let mut seen = vec![false; n];
        let mut ordinals = Vec::with_capacity(n);
        let mut vectors = Vec::with_capacity(n);
        let mut records = Vec::with_capacity(n * (d + 12));
        let mut canonical_row = vec![0; 8 + d * 4];
        let mut original_row = vec![0; d + 12];
        for cell in 0..metadata.cells {
            let body = primary.primary_cell_sq8(cell)?;
            for row in body.chunks_exact(d + 12) {
                let id = usize::try_from(i64::from_le_bytes(row[..8].try_into()?))?;
                require(
                    id < n && !std::mem::replace(&mut seen[id], true),
                    "fine primary ID bijection",
                )?;
                let physical = inverse[id];
                canonical.read_exact_at(&mut canonical_row, (physical * (8 + 4 * d)) as u64)?;
                sq8.read_exact_at(&mut original_row, (physical * (d + 12)) as u64)?;
                require(
                    canonical_row[..8] == row[..8] && original_row == row,
                    "fine complete original SQ8 parity",
                )?;
                let vector = canonical_row[8..]
                    .chunks_exact(4)
                    .map(|v| f32::from_le_bytes(v.try_into().unwrap()))
                    .collect::<Vec<_>>();
                cosine_vector(&vector)?; // Validate before fitting/construction.
                vectors.push(vector);
                ordinals.push(id);
                records.extend_from_slice(row);
            }
        }
        require(
            ordinals.len() == n && seen.iter().all(|v| *v),
            "fine complete primary permutation",
        )?;
        let pq = fit_source_codes(&vectors, &ordinals).map_err(|e| format!("fine fit: {e:?}"))?;
        let parent = output
            .parent()
            .filter(|v| !v.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let stage = tempfile::tempdir_in(parent)?;
        let pq_artifact = write_body(stage.path(), output, "pq.bin", &encode_pq(&pq))?;
        let records_artifact = write_body(stage.path(), output, "records.bin", &records)?;
        let groups = records
            .chunks(16 * (d + 12))
            .flat_map(|group| Sha256::digest(group).to_vec())
            .collect::<Vec<_>>();
        let group_artifact = write_body(stage.path(), output, "groups.bin", &groups)?;
        let order = ordinals
            .iter()
            .flat_map(|&id| (id as u64).to_le_bytes())
            .collect::<Vec<_>>();
        let order_artifact = write_body(stage.path(), output, "order.bin", &order)?;
        let (low, step) = primary.sq8_coefficients();
        let identity = PqGraphIdentity {
            generation: u64::from_le_bytes(digest(&config.primary_root.sha256)?[..8].try_into()?)
                | 1,
            rows: n,
            dimensions: d,
            source: digest(&original.canonical.sha256)?,
            layout: layout_identity(
                &config.primary_root,
                &records_artifact,
                &order_artifact,
                low,
                step,
            )?,
            pq: digest(&pq_artifact.sha256)?,
        };
        let graph = PqVectorGraph::build(vectors, identity.clone())?;
        let graph_path = stage.path().join("graph.bin");
        let graph_sha = graph.write_authenticated(&graph_path)?;
        let graph_artifact = Artifact {
            path: output.join("graph.bin"),
            bytes: usize::try_from(graph_path.metadata()?.len())?,
            sha256: graph_sha,
        };
        // Source inputs are immutable by contract. Final authentication rejects
        // changed/truncated originals before publishing this generation.
        for artifact in [
            &config.primary_root,
            &original.canonical,
            &original.order,
            &original.sq8,
        ] {
            artifact.authenticate_streamed(artifact.bytes)?;
        }
        let build = FineBuildReceipt {
            modeled_build_payload_bytes: build_bytes,
            modeled_output_and_staging_bytes: output_bound,
            actual_graph_capacity_bytes: graph.heap_bytes(),
            construction_graph_capacity_bytes: graph
                .construction_capacity_bytes()
                .ok_or("fine construction receipt missing")?,
            actual_pq_capacity_bytes: pq.resident_bytes(),
            source_rows: n,
            sq8_body_bytes: records.len(),
        };
        let manifest = Manifest {
            schema: SCHEMA.into(),
            primary_root: config.primary_root.clone(),
            original,
            identity,
            low: low.to_vec(),
            step: step.to_vec(),
            pq: pq_artifact,
            graph: graph_artifact,
            records: records_artifact,
            groups: group_artifact,
            order: order_artifact,
            build,
        };
        let body = serde_json::to_vec(&manifest)?;
        require(body.len() <= ROOT_CAP, "fine manifest cap")?;
        let root = write_body(stage.path(), output, "manifest.json", &body)?;
        File::open(stage.path())?.sync_all()?;
        // No-replace install: reserve destination, move only into our new empty
        // directory; a failed install remains incomplete and cannot be served.
        std::fs::create_dir(output)?;
        for name in [
            "pq.bin",
            "graph.bin",
            "records.bin",
            "groups.bin",
            "order.bin",
            "manifest.json",
        ] {
            std::fs::hard_link(stage.path().join(name), output.join(name))?;
        }
        File::open(output)?.sync_all()?;
        File::open(parent)?.sync_all()?;
        Ok(root)
    }

    /// Authenticate resident artifacts and pin the local SQ8 record file.
    pub fn open(root: &Artifact, limits: &ResidentLimits) -> Result<Self> {
        let mut index = Self::open_remote(root, limits)?;
        index.records = Some(secure_file(&index.manifest.records)?);
        Ok(index)
    }

    /// Authenticate/admit the same resident artifacts without opening the SQ8
    /// record body or any original source plane. Use a snapshot's `search_s3`
    /// with the existing conditional reader to fetch authenticated groups.
    pub fn open_remote(root: &Artifact, limits: &ResidentLimits) -> Result<Self> {
        Self::admission(root, limits)?;
        let manifest: Manifest =
            serde_json::from_slice(&read_source_probe_artifact(root, ROOT_CAP)?)?;
        let n = manifest.identity.rows;
        let d = manifest.identity.dimensions;
        let mut resources = model(n, d, manifest.graph.bytes, root.bytes, limits)?;
        validate_manifest(&manifest, root)?;
        let pq = decode_pq(
            &read_source_probe_artifact(&manifest.pq, manifest.pq.bytes)?,
            n,
            d,
        )?;
        let graph = PqVectorGraph::open_authenticated(&manifest.graph, &manifest.identity)?;
        let encoded = read_source_probe_artifact(&manifest.order, n * 8)?;
        let mut seen = vec![false; n];
        let mut ids = Vec::with_capacity(n);
        for word in encoded.chunks_exact(8) {
            let id = usize::try_from(u64::from_le_bytes(word.try_into()?))?;
            require(
                id < n && !std::mem::replace(&mut seen[id], true),
                "fine opened order bijection",
            )?;
            ids.push(id as i64);
        }
        let hashes = read_source_probe_artifact(&manifest.groups, manifest.groups.bytes)?;
        let page_manifest = serde_json::to_vec(
            &serde_json::json!({"schema":"borsuk-v115-sq8-page-authority-v2",
            "generation":manifest.identity.generation,"rows":n,"dimensions":d,"page_rows":16,
            "object_sha256":manifest.records.sha256,"page_digest_sha256":manifest.groups.sha256}),
        )?;
        let authority = PageAuthority::load(&page_manifest, &hash(&page_manifest), &hashes)
            .map_err(|e| format!("fine authority: {e:?}"))?;
        resources.graph_capacity_bytes = graph.heap_bytes();
        resources.pq_capacity_bytes = pq.resident_bytes();
        resources.map_capacity_bytes = ids.capacity() * 8;
        require(
            resources.graph_capacity_bytes <= n * 512
                && resources.pq_capacity_bytes <= n * 68 + 64 * 256 * d.div_ceil(64) * 4
                && resources.map_capacity_bytes <= n * 8,
            "fine actual capacity admission",
        )?;
        resources.resident_bytes = resources.graph_capacity_bytes
            + resources.pq_capacity_bytes
            + resources.map_capacity_bytes
            + resources.group_hash_bytes
            + FIXED_BYTES;
        Ok(Self {
            root_sha256: root.sha256.clone(),
            manifest,
            pq,
            graph,
            authority,
            records: None,
            ids,
            delta_budget: limits.delta_bytes,
            resources,
        })
    }
    pub fn rows(&self) -> usize {
        self.ids.len()
    }
    pub fn dimensions(&self) -> usize {
        self.manifest.identity.dimensions
    }
    pub fn root_sha256(&self) -> &str {
        &self.root_sha256
    }
    pub fn build_receipt(&self) -> &FineBuildReceipt {
        &self.manifest.build
    }
    pub fn new_workspace(&self) -> Result<GraphSearchWorkspace> {
        Ok(GraphSearchWorkspace::new(self.rows())?)
    }
    pub fn plan(
        &self,
        query: &[f32],
        workspace: &mut GraphSearchWorkspace,
    ) -> Result<FineFetchPlan> {
        let nomination = self
            .graph
            .nominate_pq(query, &self.pq, workspace)
            .map_err(|e| {
                format!(
                    "fine nomination INVALID: {}; attempted evaluations={}",
                    e.error, e.evaluations
                )
            })?;
        self.plan_nomination(query, nomination)
    }
    fn plan_nomination(
        &self,
        query: &[f32],
        nomination: crate::resident_vector_graph::PqNomination,
    ) -> Result<FineFetchPlan> {
        let selected = nomination
            .ordinals
            .iter()
            .map(|v| v / 16)
            .collect::<BTreeSet<_>>();
        let (ranges, bytes) = if selected.is_empty() {
            (Vec::new(), 0)
        } else {
            cover_pages(&selected, self.rows(), self.dimensions() + 12, 16, MAX_GETS)
                .map_err(|e| format!("fine cover: {e:?}"))?
        };
        let old = nomination
            .ordinals
            .iter()
            .map(|v| v / 256)
            .collect::<BTreeSet<_>>();
        let (old_ranges, old_bytes) = if old.is_empty() {
            (Vec::new(), 0)
        } else {
            cover_pages(&old, self.rows(), self.dimensions() + 12, 256, MAX_GETS)
                .map_err(|e| format!("fine paired cover: {e:?}"))?
        };
        let actual_shortlist_rows = nomination.ordinals.len();
        Ok(FineFetchPlan {
            root_sha256: self.root_sha256.clone(),
            query_sha256: query_digest(query),
            revision: 0,
            mutation_sha256: String::new(),
            nominees: nomination.ordinals,
            ranges,
            planned_bytes: bytes,
            feasible: bytes <= MAX_BYTES,
            exhausted: nomination.exhausted,
            converged: !nomination.exhausted,
            actual_shortlist_rows,
            evaluations: nomination.evaluations,
            base_visits: nomination.base_visits,
            old_page_gets: old_ranges.len(),
            old_page_bytes: old_bytes,
        })
    }
    pub fn search(
        &self,
        plan: &FineFetchPlan,
        query: &[f32],
        k: usize,
    ) -> std::result::Result<FineSearchTrace, FineSearchFailure> {
        self.search_excluding(plan, query, k, 0, "", &[])
    }
    fn validate_plan(
        &self,
        plan: &FineFetchPlan,
        query: &[f32],
        k: usize,
        revision: u64,
        snapshot: &str,
    ) -> Result<()> {
        require(
            plan.root_sha256 == self.root_sha256
                && plan.query_sha256 == query_digest(query)
                && plan.revision == revision
                && plan.mutation_sha256 == snapshot
                && k > 0
                && k <= self.rows(),
            "fine plan query/generation/revision binding",
        )?;
        require(
            plan.feasible && plan.planned_bytes <= MAX_BYTES && plan.ranges.len() <= MAX_GETS,
            "fine refused infeasible nomination",
        )
    }
    fn score_returned(
        &self,
        plan: &FineFetchPlan,
        query: &[f32],
        k: usize,
        excluded: &[i64],
        ranges: &[ReturnedRange<'_>],
        accounting: &mut FineAccounting,
    ) -> Result<FineSearchTrace> {
        require(
            ranges.len() == plan.ranges.len(),
            "fine returned range count",
        )?;
        let mut fetched_ids = Vec::new();
        for (body, expected) in ranges.iter().zip(&plan.ranges) {
            require(
                body.start == expected.start && body.bytes.len() == expected.len(),
                "fine returned range binding",
            )?;
            let first = expected.start / ((self.dimensions() + 12) * 16);
            let last = (expected.end - 1) / ((self.dimensions() + 12) * 16);
            self.authority
                .verify_payload(first, last, body.bytes)
                .map_err(|e| format!("fine group auth: {e:?}"))?;
            for (slot, row) in body.bytes.chunks_exact(self.dimensions() + 12).enumerate() {
                let id = i64::from_le_bytes(row[..8].try_into()?);
                require(
                    id == self.ids[expected.start / (self.dimensions() + 12) + slot],
                    "fine authenticated row ID mapping",
                )?;
                fetched_ids.push(id);
            }
            accounting.verified_bytes += body.bytes.len();
        }
        let ranked = if ranges.is_empty() {
            Vec::new()
        } else {
            rank_returned_ranges_excluding(
                Sq8Geometry {
                    rows: self.rows(),
                    dimensions: self.dimensions(),
                },
                ranges,
                &cosine_vector(query)?,
                &self.manifest.low,
                &self.manifest.step,
                k,
                MAX_BYTES,
                excluded,
            )
            .map_err(|e| format!("fine SQ8: {e:?}"))?
        };
        Ok(FineSearchTrace {
            ranked,
            fetched_ids,
            nominee_ids: plan.nominees.iter().map(|&r| self.ids[r]).collect(),
            accounting: accounting.clone(),
        })
    }
    fn search_excluding(
        &self,
        plan: &FineFetchPlan,
        query: &[f32],
        k: usize,
        revision: u64,
        snapshot: &str,
        excluded: &[i64],
    ) -> std::result::Result<FineSearchTrace, FineSearchFailure> {
        let mut accounting = FineAccounting {
            planned_bytes: plan.planned_bytes,
            ..Default::default()
        };
        let result = (|| -> Result<FineSearchTrace> {
            self.validate_plan(plan, query, k, revision, snapshot)?;
            let records = self
                .records
                .as_ref()
                .ok_or("fine local record provider unavailable")?;
            let mut payloads = Vec::with_capacity(plan.ranges.len());
            for range in &plan.ranges {
                accounting.attempted_gets += 1;
                accounting.attempted_bytes = accounting.attempted_bytes.map(|n| n + range.len());
                let mut body = vec![0; range.len()];
                let prior_read_bytes = accounting.read_bytes.take();
                records.read_exact_at(&mut body, range.start as u64)?;
                accounting.read_bytes = prior_read_bytes.map(|n| n + body.len());
                payloads.push(body);
            }
            let ranges = plan
                .ranges
                .iter()
                .zip(&payloads)
                .map(|(r, b)| ReturnedRange {
                    start: r.start,
                    bytes: b,
                })
                .collect::<Vec<_>>();
            self.score_returned(plan, query, k, excluded, &ranges, &mut accounting)
        })();
        result.map_err(|e| FineSearchFailure {
            error: e.to_string(),
            accounting,
        })
    }
}
/// Private fields prevent caller-edited ranges; serialization is audit-only.
#[derive(Clone, Debug, Serialize)]
pub struct FineFetchPlan {
    root_sha256: String,
    query_sha256: String,
    revision: u64,
    mutation_sha256: String,
    nominees: Vec<usize>,
    ranges: Vec<Range<usize>>,
    planned_bytes: usize,
    feasible: bool,
    exhausted: bool,
    converged: bool,
    actual_shortlist_rows: usize,
    evaluations: usize,
    base_visits: usize,
    old_page_gets: usize,
    old_page_bytes: usize,
}
impl FineFetchPlan {
    pub fn feasible(&self) -> bool {
        self.feasible
    }
    pub fn exhausted(&self) -> bool {
        self.exhausted
    }
    pub fn nominees(&self) -> &[usize] {
        &self.nominees
    }
    pub fn ranges(&self) -> &[Range<usize>] {
        &self.ranges
    }
    pub fn planned_bytes(&self) -> usize {
        self.planned_bytes
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct FineAccounting {
    pub planned_bytes: usize,
    pub attempted_gets: usize,
    /// None when the adapter cannot observe exact submitted bytes.
    pub attempted_bytes: Option<usize>,
    /// None on a partial local read or when the remote adapter cannot observe it.
    pub read_bytes: Option<usize>,
    pub verified_bytes: usize,
}
impl Default for FineAccounting {
    fn default() -> Self {
        Self {
            planned_bytes: 0,
            attempted_gets: 0,
            attempted_bytes: Some(0),
            read_bytes: Some(0),
            verified_bytes: 0,
        }
    }
}
impl FineAccounting {
    fn from_s3(stats: crate::sq8_s3_range::Sq8ReadStats, planned_bytes: usize) -> Self {
        Self {
            planned_bytes,
            attempted_gets: stats.submitted_gets,
            attempted_bytes: None,
            read_bytes: None,
            verified_bytes: stats.verified_bytes,
        }
    }
}
fn s3_failure(
    error: crate::sq8_s3_range::RankedSq8Failure,
    planned_bytes: usize,
) -> FineSearchFailure {
    FineSearchFailure {
        error: format!("fine S3: {:?}", error.error),
        accounting: FineAccounting::from_s3(error.stats, planned_bytes),
    }
}
#[derive(Debug)]
pub struct FineSearchTrace {
    pub ranked: Vec<ScoredNominee>,
    pub fetched_ids: Vec<i64>,
    pub nominee_ids: Vec<i64>,
    pub accounting: FineAccounting,
}
#[derive(Debug)]
pub struct FineSearchFailure {
    pub error: String,
    pub accounting: FineAccounting,
}

/// Immutable in-memory visibility seam only, not a persistence/maintenance claim.
/// An old snapshot owns its Arc generation and mutation revision until release.
#[derive(Clone)]
pub struct FineSq8Snapshot {
    index: Arc<FineSq8Index>,
    revision: u64,
    digest: String,
    excluded: Vec<i64>,
    replacements: Vec<u8>,
}
impl FineSq8Snapshot {
    pub fn new(index: Arc<FineSq8Index>) -> Self {
        Self {
            index,
            revision: 0,
            digest: String::new(),
            excluded: Vec::new(),
            replacements: Vec::new(),
        }
    }
    /// Bodies use unchanged coefficients and the same existing SQ8 validation.
    /// Caller authenticates the mutation snapshot and admits its coexistence.
    pub fn with_mutations(
        &self,
        revision: u64,
        deleted: &[i64],
        replacements: &[u8],
        max_bytes: usize,
    ) -> Result<Self> {
        require(
            revision > self.revision
                && replacements.len() % (self.index.dimensions() + 12) == 0
                && deleted.windows(2).all(|v| v[0] < v[1])
                && deleted
                    .iter()
                    .all(|&id| id >= 0 && (id as usize) < self.index.rows()),
            "fine mutation revision/geometry",
        )?;
        let modeled = replacements
            .len()
            .checked_mul(4)
            .and_then(|x| x.checked_add(deleted.len().checked_mul(512)?))
            .and_then(|x| x.checked_add(self.replacements.len().checked_mul(4)?))
            .and_then(|x| x.checked_add(self.excluded.len().checked_mul(512)?))
            .and_then(|x| {
                x.checked_add(
                    (replacements.len().checked_add(self.replacements.len())?
                        / (self.index.dimensions() + 12))
                        .checked_mul(512)?,
                )
            })
            .ok_or("fine mutation overflow")?;
        require(
            modeled <= max_bytes && max_bytes <= self.index.delta_budget,
            "fine mutation admission",
        )?;
        let width = self.index.dimensions() + 12;
        let mut excluded = self.excluded.iter().copied().collect::<BTreeSet<_>>();
        let mut latest = self
            .replacements
            .chunks_exact(width)
            .map(|r| (i64::from_le_bytes(r[..8].try_into().unwrap()), r))
            .collect::<BTreeMap<_, _>>();
        for &id in deleted {
            excluded.insert(id);
            latest.remove(&id);
        }
        let mut replacement_ids = BTreeSet::new();
        for row in replacements.chunks_exact(width) {
            let id = i64::from_le_bytes(row[..8].try_into()?);
            require(
                id >= 0
                    && (id as usize) < self.index.rows()
                    && replacement_ids.insert(id)
                    && deleted.binary_search(&id).is_err(),
                "fine duplicate mutation ID",
            )?;
            score_nominees(
                row,
                Sq8Geometry {
                    rows: 1,
                    dimensions: self.index.dimensions(),
                },
                &[0],
                &vec![0.0; self.index.dimensions()],
                &self.index.manifest.low,
                &self.index.manifest.step,
            )
            .map_err(|e| format!("fine mutation SQ8: {e:?}"))?;
            excluded.insert(id);
            latest.insert(id, row);
        }
        let excluded = excluded.into_iter().collect::<Vec<_>>();
        let replacements = latest
            .values()
            .flat_map(|row| row.iter().copied())
            .collect::<Vec<_>>();
        let digest = hash(&serde_json::to_vec(&(revision, &excluded, &replacements))?);
        Ok(Self {
            index: self.index.clone(),
            revision,
            digest,
            excluded,
            replacements,
        })
    }
    pub fn plan(
        &self,
        query: &[f32],
        workspace: &mut GraphSearchWorkspace,
    ) -> Result<FineFetchPlan> {
        let mut plan = self.index.plan(query, workspace)?;
        plan.revision = self.revision;
        plan.mutation_sha256 = self.digest.clone();
        Ok(plan)
    }
    pub fn search(
        &self,
        plan: &FineFetchPlan,
        query: &[f32],
        k: usize,
    ) -> std::result::Result<FineSearchTrace, FineSearchFailure> {
        let result = self.index.search_excluding(
            plan,
            query,
            k,
            self.revision,
            &self.digest,
            &self.excluded,
        )?;
        self.merge_replacements(result, query, k)
    }
    /// Existing one-attempt native conditional reader; no controller, retries
    /// or independent transport. Local counters must not be called S3 latency.
    /// The caller separately admits the existing client's runtime/TLS buffers.
    /// Works with `FineSq8Index::open_remote`; no local record file is accessed.
    pub async fn search_s3(
        &self,
        plan: &FineFetchPlan,
        query: &[f32],
        k: usize,
        reader: &crate::sq8_s3_range::OneAttemptS3,
        location: &object_store::path::Path,
        etag: &str,
    ) -> std::result::Result<FineSearchTrace, FineSearchFailure> {
        self.index
            .validate_plan(plan, query, k, self.revision, &self.digest)
            .map_err(|e| FineSearchFailure {
                error: e.to_string(),
                accounting: FineAccounting::default(),
            })?;
        let ranges = plan
            .ranges
            .iter()
            .map(|r| {
                (
                    r.start / ((self.index.dimensions() + 12) * 16),
                    (r.end - 1) / ((self.index.dimensions() + 12) * 16),
                )
            })
            .collect::<Vec<_>>();
        let mut accounting = FineAccounting::from_s3(Default::default(), plan.planned_bytes);
        let verified = if ranges.is_empty() {
            Vec::new()
        } else {
            let (verified, stats) = reader
                .fetch_verified_ranges(
                    location,
                    &self.index.authority,
                    &ranges,
                    etag,
                    MAX_GETS,
                    MAX_BYTES,
                    32,
                )
                .await
                .map_err(|e| s3_failure(e, plan.planned_bytes))?;
            accounting = FineAccounting::from_s3(stats, plan.planned_bytes);
            verified
        };
        let returned = verified
            .iter()
            .map(|r| ReturnedRange {
                start: r.start,
                bytes: &r.bytes,
            })
            .collect::<Vec<_>>();
        let mut checked = FineAccounting::default();
        let mut result = self
            .index
            .score_returned(plan, query, k, &self.excluded, &returned, &mut checked)
            .map_err(|e| FineSearchFailure {
                error: e.to_string(),
                accounting: accounting.clone(),
            })?;
        result.accounting = accounting;
        self.merge_replacements(result, query, k)
    }
    fn merge_replacements(
        &self,
        mut result: FineSearchTrace,
        query: &[f32],
        k: usize,
    ) -> std::result::Result<FineSearchTrace, FineSearchFailure> {
        if !self.replacements.is_empty() {
            let count = self.replacements.len() / (self.index.dimensions() + 12);
            let scored = score_nominees(
                &self.replacements,
                Sq8Geometry {
                    rows: count,
                    dimensions: self.index.dimensions(),
                },
                &(0..count).collect::<Vec<_>>(),
                &cosine_vector(query).map_err(|e| FineSearchFailure {
                    error: e.to_string(),
                    accounting: result.accounting.clone(),
                })?,
                &self.index.manifest.low,
                &self.index.manifest.step,
            )
            .map_err(|e| FineSearchFailure {
                error: format!("fine delta: {e:?}"),
                accounting: result.accounting.clone(),
            })?;
            result.ranked.extend(scored);
            result
                .ranked
                .sort_by(|a, b| a.score.total_cmp(&b.score).then(a.id.cmp(&b.id)));
            result.ranked.truncate(k);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fine_remote_failure_preserves_partial_known_stats_and_unknown_bytes() {
        use crate::sq8_s3_range::{RangeFetchError, RankedSq8Failure, Sq8ReadStats};
        let failure = s3_failure(
            RankedSq8Failure {
                error: RangeFetchError::Page(crate::sq8_page_authority::PageError::HashMismatch),
                stats: Sq8ReadStats {
                    submitted_gets: 2,
                    verified_bytes: 224,
                    failed_gets: 1,
                },
            },
            10_000,
        );
        assert_eq!(failure.accounting.attempted_gets, 2);
        assert_eq!(failure.accounting.planned_bytes, 10_000);
        assert_eq!(failure.accounting.verified_bytes, 224);
        assert_eq!(failure.accounting.attempted_bytes, None);
        assert_eq!(failure.accounting.read_bytes, None);
        let json = serde_json::to_value(&failure.accounting).unwrap();
        assert!(json["attempted_bytes"].is_null() && json["read_bytes"].is_null());
    }
    fn limits() -> ResidentLimits {
        ResidentLimits {
            max_peak_payload_bytes: 256 * 1024 * 1024,
            pinned_generation_bytes: 0,
            active_queries: 1,
            delta_bytes: 65536,
            maintenance_bytes: 0,
            runtime_bytes: 0,
        }
    }
    fn fixture(dir: &Path, n: usize, tag: u8) -> Artifact {
        std::fs::create_dir(dir).unwrap();
        let vectors = (0..n).map(|_| vec![1., 0.]).collect::<Vec<_>>();
        let ordinals = (0..n).rev().collect::<Vec<_>>();
        let pq = fit_source_codes(&vectors, &ordinals).unwrap();
        let pq = write_body(dir, dir, "pq.bin", &encode_pq(&pq)).unwrap();
        let mut records = Vec::new();
        for &id in &ordinals {
            records.extend_from_slice(&(id as i64).to_le_bytes());
            records.extend_from_slice(&1.0f32.to_le_bytes());
            records.extend_from_slice(&[1, 0]);
        }
        let records_a = write_body(dir, dir, "records.bin", &records).unwrap();
        let groups = records
            .chunks(16 * 14)
            .flat_map(|r| Sha256::digest(r).to_vec())
            .collect::<Vec<_>>();
        let groups = write_body(dir, dir, "groups.bin", &groups).unwrap();
        let order = write_body(
            dir,
            dir,
            "order.bin",
            &ordinals
                .iter()
                .flat_map(|&i| (i as u64).to_le_bytes())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let source = Artifact {
            path: dir.join("absent-original"),
            bytes: n * 16,
            sha256: hash(&[tag]),
        };
        let original = BuildConfig {
            schema: crate::hierarchical_semantic_cells::BUILD_SCHEMA.into(),
            generation: source.clone(),
            plane: source.clone(),
            canonical: source.clone(),
            order: source.clone(),
            records: source.clone(),
            mean: source.clone(),
            sq8: source.clone(),
            cell_rows: 16,
            sample_rows: 16,
            max_depth: 24,
            max_build_payload_bytes: 1,
            max_output_bytes: 1,
        };
        let identity = PqGraphIdentity {
            generation: 1,
            rows: n,
            dimensions: 2,
            source: digest(&source.sha256).unwrap(),
            layout: layout_identity(&source, &records_a, &order, &[0., 0.], &[1., 1.]).unwrap(),
            pq: digest(&pq.sha256).unwrap(),
        };
        let graph = PqVectorGraph::build(vectors, identity.clone()).unwrap();
        let graph_sha = graph.write_authenticated(&dir.join("graph.bin")).unwrap();
        let graph_capacity = graph.heap_bytes();
        let graph = Artifact {
            path: dir.join("graph.bin"),
            bytes: dir.join("graph.bin").metadata().unwrap().len() as usize,
            sha256: graph_sha,
        };
        let root = Manifest {
            schema: SCHEMA.into(),
            primary_root: source,
            original,
            identity,
            low: vec![0., 0.],
            step: vec![1., 1.],
            pq,
            graph,
            records: records_a,
            groups,
            order,
            build: FineBuildReceipt {
                modeled_build_payload_bytes: 128 * 1024 * 1024,
                modeled_output_and_staging_bytes: 16 * 1024 * 1024,
                actual_graph_capacity_bytes: graph_capacity,
                construction_graph_capacity_bytes: graph_capacity,
                actual_pq_capacity_bytes: n * 68 + 64 * 256 * 4,
                source_rows: n,
                sq8_body_bytes: records.len(),
            },
        };
        write_body(
            dir,
            dir,
            "manifest.json",
            &serde_json::to_vec(&root).unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn fine_cover_matches_independent_bruteforce_with_tail() {
        // Enumerate all contiguous interval collections via row-group masks;
        // independently count their runs and actual byte cost, including tails.
        for wanted in 1usize..64 {
            let selected = (0..6)
                .filter(|i| wanted & (1 << i) != 0)
                .collect::<BTreeSet<_>>();
            for cap in 1..=3 {
                let (ranges, charge) = cover_pages(&selected, 83, 14, 16, cap).unwrap();
                let optimum = (wanted..64)
                    .filter(|m| m & wanted == wanted)
                    .filter_map(|mask| {
                        let runs = (0..6)
                            .filter(|i| {
                                mask & (1 << i) != 0 && (*i == 0 || mask & (1 << (i - 1)) == 0)
                            })
                            .count();
                        (runs <= cap).then(|| {
                            (0..6)
                                .filter(|i| mask & (1 << i) != 0)
                                .map(|i| (83usize - i * 16).min(16) * 14)
                                .sum::<usize>()
                        })
                    })
                    .min()
                    .unwrap();
                assert_eq!(charge, optimum);
                assert!(ranges.len() <= cap);
                for group in &selected {
                    assert!(
                        (*group * 16..((*group + 1) * 16).min(83))
                            .all(|r| ranges.iter().any(|p| p.contains(&(r * 14))))
                    );
                }
            }
        }
        let dispersed = (0..1024).map(|i| i * 6).collect::<BTreeSet<_>>();
        let (ranges, bytes) = cover_pages(&dispersed, 100_000, 780, 16, MAX_GETS).unwrap();
        assert!(bytes > MAX_BYTES);
        assert_eq!(ranges.len(), MAX_GETS);
        assert!(
            dispersed
                .iter()
                .all(|group| ranges.iter().any(|r| r.contains(&(group * 16 * 780))))
        );
    }
    #[test]
    fn fine_tail_scalar_ties_nonunit_and_pins() {
        let tmp = tempfile::tempdir().unwrap();
        let root = fixture(&tmp.path().join("a"), 35, 1);
        let index = Arc::new(FineSq8Index::open(&root, &limits()).unwrap());
        assert_eq!(index.resources.group_hash_bytes, 3 * 32);
        let q = [3.125, 0.875];
        let mut workspace = index.new_workspace().unwrap();
        let plan = index.plan(&q, &mut workspace).unwrap();
        assert_eq!(plan.nominees.len(), 35);
        assert_eq!(plan.planned_bytes, 35 * 14);
        let trace = index.search(&plan, &q, 35).unwrap();
        let norm = (3.125f64.powi(2) + 0.875f64.powi(2)).sqrt();
        let qn = [(3.125 / norm) as f32, (0.875 / norm) as f32];
        // Independent scalar oracle, no native scorer or codec call.
        let expected = 1.0f32 - 2.0 * (qn[0] - (qn[0] * qn[0] + qn[1] * qn[1]) / 2.0);
        assert_eq!(
            trace.ranked.iter().map(|r| r.id).collect::<Vec<_>>(),
            (0..35).collect::<Vec<_>>()
        );
        assert!(
            trace
                .ranked
                .iter()
                .all(|r| r.score.to_bits() == expected.to_bits())
        );
        assert!(index.search(&plan, &[1., 0.], 10).is_err());
        let other_root = fixture(&tmp.path().join("b"), 35, 2);
        let other = FineSq8Index::open(&other_root, &limits()).unwrap();
        assert!(other.search(&plan, &q, 10).is_err());
        let old = FineSq8Snapshot::new(index.clone());
        let old_plan = old.plan(&q, &mut workspace).unwrap();
        let mut replacement = 1i64.to_le_bytes().to_vec();
        replacement.extend_from_slice(&0.0f32.to_le_bytes());
        replacement.extend_from_slice(&[1, 0]);
        let new = old.with_mutations(1, &[0], &replacement, 4096).unwrap();
        let new_plan = new.plan(&q, &mut workspace).unwrap();
        let competing = old.with_mutations(1, &[2], &[], 4096).unwrap();
        assert!(competing.search(&new_plan, &q, 10).is_err());
        assert!(new.search(&old_plan, &q, 10).is_err());
        assert!(old.search(&new_plan, &q, 10).is_err());
        let changed = new.search(&new_plan, &q, 10).unwrap();
        assert_eq!(changed.ranked[0].id, 1);
        assert!(!changed.ranked.iter().any(|s| s.id == 0));
        assert_eq!(changed.ranked.iter().filter(|s| s.id == 1).count(), 1);
        drop(new);
        drop(index);
        drop(other);
        // The old pinned generation remains readable after pathname replacement.
        let records_path = root.path.parent().unwrap().join("records.bin");
        std::fs::rename(&records_path, records_path.with_extension("old")).unwrap();
        std::fs::write(&records_path, vec![0; 35 * 14]).unwrap();
        assert_eq!(old.search(&old_plan, &q, 10).unwrap().ranked[0].id, 0);
    }
    #[test]
    fn fine_exhausted_shortlist_is_scored_empty_is_underfilled_and_refusal_reads_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let root = fixture(&tmp.path().join("a"), 35, 1);
        let index = FineSq8Index::open(&root, &limits()).unwrap();
        let q = [3.125, 0.875];
        let mut workspace = index.new_workspace().unwrap();
        for cap in [0, 1, 4] {
            let nomination = index
                .graph
                .nominate_capped(&q, &index.pq, &mut workspace, cap)
                .unwrap();
            assert!(nomination.exhausted);
            assert_eq!(nomination.evaluations, cap);
            let plan = index.plan_nomination(&q, nomination).unwrap();
            let result = index.search(&plan, &q, 35).unwrap();
            assert!(plan.exhausted && !plan.converged);
            assert_eq!(plan.actual_shortlist_rows, plan.nominees.len());
            assert_eq!(result.nominee_ids.len(), plan.nominees.len());
            if cap == 0 {
                assert!(result.ranked.is_empty());
                assert_eq!(result.accounting.attempted_gets, 0);
            } else {
                assert!(!result.ranked.is_empty());
            }
        }
        let mut plan = index.plan(&q, &mut workspace).unwrap();
        plan.feasible = false;
        let failure = index.search(&plan, &q, 10).unwrap_err();
        assert_eq!(failure.accounting.attempted_gets, 0);
        let shared = (0..512).map(|i| i * 3).collect::<BTreeSet<_>>();
        let (ranges, bytes) = cover_pages(&shared, 100_000, 780, 16, 256).unwrap();
        assert_eq!(ranges.len(), 256);
        assert_eq!(bytes, 12_779_520);
        let isolated = (0..1024).map(|i| i * 2).collect::<BTreeSet<_>>();
        let (ranges, bytes) = cover_pages(&isolated, 100_000, 780, 16, 256).unwrap();
        assert_eq!(ranges.len(), 256);
        assert_eq!(bytes, 22_364_160);
        assert!(bytes > MAX_BYTES);
    }
    #[test]
    fn fine_nonzero_coefficients_scalar_oracle_extremes_and_near_ties() {
        let tmp = tempfile::tempdir().unwrap();
        let root = fixture(&tmp.path().join("a"), 17, 1);
        let mut manifest: Manifest =
            serde_json::from_slice(&std::fs::read(&root.path).unwrap()).unwrap();
        manifest.low = vec![-0.125, 0.0625];
        manifest.step = vec![0.5, 1.25];
        let mut body = Vec::new();
        for id in (0..17i64).rev() {
            let codes = [(id % 3 + 1) as u8, (id % 2) as u8];
            let decoded = [
                manifest.low[0] + manifest.step[0] * f32::from(codes[0]),
                manifest.low[1] + manifest.step[1] * f32::from(codes[1]),
            ];
            let mut norm = decoded[0] * decoded[0] + decoded[1] * decoded[1];
            if id == 16 {
                norm = f32::from_bits(norm.to_bits() + 1);
            } // one-ULP near tie
            body.extend_from_slice(&id.to_le_bytes());
            body.extend_from_slice(&norm.to_le_bytes());
            body.extend_from_slice(&codes);
        }
        std::fs::write(&manifest.records.path, &body).unwrap();
        manifest.records.sha256 = hash(&body);
        let groups = body
            .chunks(16 * 14)
            .flat_map(|b| Sha256::digest(b).to_vec())
            .collect::<Vec<_>>();
        std::fs::write(&manifest.groups.path, &groups).unwrap();
        manifest.groups.sha256 = hash(&groups);
        manifest.identity.layout = layout_identity(
            &manifest.primary_root,
            &manifest.records,
            &manifest.order,
            &manifest.low,
            &manifest.step,
        )
        .unwrap();
        let graph =
            PqVectorGraph::build(vec![vec![1., 0.]; 17], manifest.identity.clone()).unwrap();
        std::fs::remove_file(&manifest.graph.path).unwrap();
        manifest.graph.sha256 = graph.write_authenticated(&manifest.graph.path).unwrap();
        manifest.graph.bytes = manifest.graph.path.metadata().unwrap().len() as usize;
        let encoded = serde_json::to_vec(&manifest).unwrap();
        std::fs::write(&root.path, &encoded).unwrap();
        let root = Artifact {
            bytes: encoded.len(),
            sha256: hash(&encoded),
            ..root
        };
        let index = FineSq8Index::open(&root, &limits()).unwrap();
        let mut workspace = index.new_workspace().unwrap();
        let mut reference = None;
        for scale in [1.0f32, 2.0, 2.0f32.powi(120), 2.0f32.powi(-120)] {
            let query = [3.125 * scale, 0.875 * scale];
            let norm = (f64::from(query[0]).powi(2) + f64::from(query[1]).powi(2)).sqrt();
            let q = [
                (f64::from(query[0]) / norm) as f32,
                (f64::from(query[1]) / norm) as f32,
            ];
            let mut shift = 0.0f32;
            let mut qnorm = 0.0f32;
            let mut weights = [0.0; 2];
            for axis in 0..2 {
                shift += q[axis] * manifest.low[axis];
                qnorm += q[axis] * q[axis];
                weights[axis] = q[axis] * manifest.step[axis];
            }
            shift -= qnorm / 2.0;
            let mut expected = body
                .chunks_exact(14)
                .map(|row| {
                    let id = i64::from_le_bytes(row[..8].try_into().unwrap());
                    let norm = f32::from_le_bytes(row[8..12].try_into().unwrap());
                    let mut inner = 0.0f32;
                    for axis in 0..2 {
                        inner += f32::from(row[12 + axis]) * weights[axis];
                    }
                    (id, norm - 2.0 * (inner + shift))
                })
                .collect::<Vec<_>>();
            expected.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            let expected = expected
                .iter()
                .map(|&(id, score)| (id, score.to_bits()))
                .collect::<Vec<_>>();
            let plan = index.plan(&query, &mut workspace).unwrap();
            let result = index
                .search(&plan, &query, 17)
                .unwrap()
                .ranked
                .iter()
                .map(|s| (s.id, s.score.to_bits()))
                .collect::<Vec<_>>();
            assert_eq!(result, expected);
            if let Some(previous) = &reference {
                assert_eq!(&result, previous);
            } else {
                reference = Some(result);
            }
        }
        for q in [[0., 0.], [f32::NAN, 1.], [f32::INFINITY, 0.]] {
            assert!(index.plan(&q, &mut workspace).is_err());
        }
        let snapshot = FineSq8Snapshot::new(Arc::new(index));
        assert!(snapshot.with_mutations(1, &[-1], &[], 4096).is_err());
        assert!(snapshot.with_mutations(1, &[17], &[], 4096).is_err());
        let mut invalid = body[..14].to_vec();
        invalid[..8].copy_from_slice(&17i64.to_le_bytes());
        assert!(snapshot.with_mutations(1, &[], &invalid, 4096).is_err());
    }

    #[test]
    fn fine_bridged_corruption_and_best_replacement_outside_shortlist() {
        let tmp = tempfile::tempdir().unwrap();
        let root = fixture(&tmp.path().join("a"), 49, 1);
        let index = Arc::new(FineSq8Index::open(&root, &limits()).unwrap());
        let q = [1., 0.];
        let mut plan = index
            .plan_nomination(
                &q,
                crate::resident_vector_graph::PqNomination {
                    ordinals: vec![0, 48],
                    evaluations: 2,
                    base_visits: 2,
                    exhausted: true,
                },
            )
            .unwrap();
        // Same all-nominee cover primitive with a deliberately small test cap
        // forces the middle two groups into the authenticated response.
        let (ranges, bytes) = cover_pages(&[0, 3].into_iter().collect(), 49, 14, 16, 1).unwrap();
        plan.ranges = ranges;
        plan.planned_bytes = bytes;
        assert_eq!(index.search(&plan, &q, 10).unwrap().ranked.len(), 10);
        let path = root.path.parent().unwrap().join("records.bin");
        let original = std::fs::read(&path).unwrap();
        let mut corrupt = original.clone();
        corrupt[16 * 14 + 12] ^= 1;
        std::fs::write(&path, &corrupt).unwrap();
        assert!(index.search(&plan, &q, 10).is_err());
        std::fs::write(&path, &original).unwrap();
        let old = FineSq8Snapshot::new(index.clone());
        let mut row = 20i64.to_le_bytes().to_vec();
        row.extend_from_slice(&0.0f32.to_le_bytes());
        row.extend_from_slice(&[1, 0]);
        // A real cap=1 nomination only fetches one group; ID20 is outside it.
        let nomination = index
            .graph
            .nominate_capped(&q, &index.pq, &mut index.new_workspace().unwrap(), 1)
            .unwrap();
        let mut limited = index.plan_nomination(&q, nomination).unwrap();
        let fetched = index.search(&limited, &q, 49).unwrap().fetched_ids;
        let replacement_id = (0..49i64).find(|id| !fetched.contains(id)).unwrap();
        row[..8].copy_from_slice(&replacement_id.to_le_bytes());
        let new = old.with_mutations(1, &[], &row, 4096).unwrap();
        limited.revision = new.revision;
        limited.mutation_sha256 = new.digest.clone();
        assert_eq!(
            new.search(&limited, &q, 10).unwrap().ranked[0].id,
            replacement_id
        );
        assert!(old.search(&limited, &q, 10).is_err());
        drop(new);
    }

    #[test]
    fn fine_auth_length_symlink_and_admission_fail_closed() {
        let tmp = tempfile::tempdir().unwrap();
        let root = fixture(&tmp.path().join("a"), 35, 1);
        let index = FineSq8Index::open(&root, &limits()).unwrap();
        let original_manifest = std::fs::read(&root.path).unwrap();
        let mut denied = limits();
        denied.max_peak_payload_bytes = 2 * 1024 * 1024;
        denied.pinned_generation_bytes = 512 * 1024;
        assert!(
            FineSq8Index::open(&root, &denied)
                .err()
                .unwrap()
                .to_string()
                .contains("aggregate admission")
        );
        let pq_path = root.path.parent().unwrap().join("pq.bin");
        let pq_body = std::fs::read(&pq_path).unwrap();
        let mut swapped = pq_body.clone();
        let last = swapped.len() - 1;
        swapped[last] ^= 1;
        std::fs::write(&pq_path, &swapped).unwrap();
        assert!(FineSq8Index::open(&root, &limits()).is_err());
        std::fs::write(&pq_path, &pq_body).unwrap();
        let q = [1., 0.];
        let plan = index.plan(&q, &mut index.new_workspace().unwrap()).unwrap();
        let path = root.path.parent().unwrap().join("records.bin");
        let original = std::fs::read(&path).unwrap();
        let mut corrupt = original.clone();
        corrupt[20] ^= 1;
        std::fs::write(&path, &corrupt).unwrap();
        let failure = index.search(&plan, &q, 10).unwrap_err();
        assert_eq!(failure.accounting.attempted_gets, 1);
        assert_eq!(failure.accounting.verified_bytes, 0);
        std::fs::write(&path, &original[..original.len() - 1]).unwrap();
        assert!(FineSq8Index::open(&root, &limits()).is_err());
        std::fs::write(&path, &original).unwrap();
        let group = root.path.parent().unwrap().join("groups.bin");
        let original_groups = std::fs::read(&group).unwrap();
        std::fs::write(&group, vec![0; original_groups.len()]).unwrap();
        assert!(FineSq8Index::open(&root, &limits()).is_err());
        std::fs::write(&group, &original_groups).unwrap();
        let order = root.path.parent().unwrap().join("order.bin");
        let order_body = std::fs::read(&order).unwrap();
        let mut duplicate = order_body.clone();
        duplicate[..8].copy_from_slice(&order_body[8..16]);
        std::fs::write(&order, &duplicate).unwrap();
        // Even a newly authenticated root cannot authorize a non-bijective map.
        let mut manifest: Manifest =
            serde_json::from_slice(&std::fs::read(&root.path).unwrap()).unwrap();
        manifest.order.sha256 = hash(&duplicate);
        manifest.identity.layout = layout_identity(
            &manifest.primary_root,
            &manifest.records,
            &manifest.order,
            &manifest.low,
            &manifest.step,
        )
        .unwrap();
        let body = serde_json::to_vec(&manifest).unwrap();
        // Graph layout binding independently refuses before the map is consumed.
        let replaced = Artifact {
            bytes: body.len(),
            sha256: hash(&body),
            ..root.clone()
        };
        std::fs::write(&root.path, &body).unwrap();
        assert!(FineSq8Index::open(&replaced, &limits()).is_err());
        std::fs::write(&root.path, &original_manifest).unwrap();
        std::fs::write(&order, &order_body).unwrap();
        std::fs::remove_file(&group).unwrap();
        std::os::unix::fs::symlink(&path, &group).unwrap();
        assert!(FineSq8Index::open(&root, &limits()).is_err());
        let mut denied = limits();
        denied.max_peak_payload_bytes = 1024;
        assert!(
            FineSq8Index::open(&root, &denied)
                .err()
                .unwrap()
                .to_string()
                .contains("admission")
        );
    }
}

/// Query-blind group placement falsifier. This never rewrites an SQ8 object,
/// serves a query, or establishes recall. Independent process closure is required.
pub mod pack_diagnostic {
    use super::*;
    use serde_json::{Value, json};
    use std::{
        io::{Read, Seek, SeekFrom},
        path::{Component, PathBuf},
        time::{Duration, Instant},
    };

    pub const CONFIG_SCHEMA: &str = "borsuk-fine-pack-config-v1";
    pub const REPORT_SCHEMA: &str = "borsuk-fine-pack-diagnostic-v1";
    const PREFIX_BYTES: usize = 1_665_668;
    const PREFIX_SHA: &str = "7abcf7830e10d214999b26fb499cebf925e16b8a88a931ddb9f981ad3d28d025";
    const SEAL_SHA: &str = "374cbd0e8700c85c2b4229468c3a9fa20283deb969b661386fb0078024b9de62";
    const MEMORY_CAP: usize = 256 * 1024 * 1024;
    const OUTPUT_CAP: usize = 2 * 1024 * 1024;
    const FIXED_WORKSPACE: usize = 48 * 1024 * 1024;
    const PACK_GROUPS: usize = 42;
    const OPERATIONS: u64 = 500_000_000;
    fn diagnostic_sources() -> Value {
        json!({"fine_sq8_groups.rs":hash(include_bytes!("fine_sq8_groups.rs")),
            "resident_vector_graph.rs":hash(include_bytes!("resident_vector_graph.rs")),
            "bin/hierarchical_semantic_cells.rs":hash(include_bytes!("bin/hierarchical_semantic_cells.rs"))})
    }
    #[cfg(test)]
    thread_local! {
        static OPENS: std::cell::RefCell<Vec<PathBuf>> = const { std::cell::RefCell::new(Vec::new()) };
        static READS: std::cell::RefCell<Vec<(PathBuf, usize)>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Panel {
        pub dataset: String,
        pub root: Artifact,
        pub graph: Artifact,
        pub identity: PqGraphIdentity,
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Caps {
        pub memory_bytes: usize,
        pub output_bytes: usize,
        pub deadline_seconds: u64,
        pub operations: u64,
        pub cpu_threads: usize,
        pub swap_bytes: usize,
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Config {
        pub schema: String,
        pub panels: [Panel; 2],
        pub original_seal: Artifact,
        /// bytes/SHA describe only the frozen prefix. The regular file may be longer.
        pub prefix: Artifact,
        pub caps: Caps,
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct SupervisorReceipt {
        pub run_id: String,
        pub config_sha256: String,
        pub report_sha256: String,
        pub process_exit_code: i32,
        pub resource_limits_observed: bool,
        pub drain_complete: bool,
        pub cleanup_complete: bool,
    }
    /// The caller must independently authenticate this receipt from the original
    /// supervisor. A report body (including a copied success body) has no authority.
    pub fn admit_survival(body: &[u8], run_id: &str, receipt: &SupervisorReceipt) -> Result<()> {
        require(body.len() <= OUTPUT_CAP, "pack report admission cap")?;
        let report: Value = serde_json::from_slice(body)?;
        digest(&receipt.config_sha256)?;
        digest(&receipt.report_sha256)?;
        require(
            !run_id.is_empty()
                && receipt.run_id == run_id
                && receipt.report_sha256 == hash(body)
                && report["config_sha256"] == receipt.config_sha256
                && receipt.process_exit_code == 0
                && receipt.resource_limits_observed
                && receipt.drain_complete
                && receipt.cleanup_complete
                && report["schema"] == REPORT_SCHEMA
                && report["status"] == "SURVIVED_NECESSARY_LOCALITY"
                && report["complete"] == true
                && report["queries"] == 128
                && report["standalone_authority"] == false
                && report["requires_matching_supervisor_exit_receipt"] == true
                && report["quality_or_performance_claim"] == false
                && report["diagnostic_source_sha256"] == diagnostic_sources(),
            "pack survival requires original exit-zero/resource/drain/cleanup receipt",
        )
    }

    struct Guard {
        start: Instant,
        deadline: Duration,
        operations: u64,
        limit: u64,
        next_poll: u64,
    }
    impl Guard {
        fn new(caps: &Caps) -> Self {
            Self {
                start: Instant::now(),
                deadline: Duration::from_secs(caps.deadline_seconds),
                operations: 0,
                limit: caps.operations,
                next_poll: 0,
            }
        }
        fn tick(&mut self, work: u64) -> Result<()> {
            self.operations = self
                .operations
                .checked_add(work)
                .ok_or("pack operation overflow")?;
            require(self.operations <= self.limit, "pack operation cap")?;
            if self.operations >= self.next_poll || work == 0 {
                require(self.start.elapsed() <= self.deadline, "pack deadline")?;
                self.next_poll = self.operations.saturating_add(4096);
            }
            Ok(())
        }
    }
    fn sum(values: &[usize]) -> Result<usize> {
        values.iter().try_fold(0usize, |a, b| {
            a.checked_add(*b)
                .ok_or_else(|| "pack allocation overflow".into())
        })
    }
    fn memory(caps: &Caps, values: &[usize]) -> Result<usize> {
        let required = sum(values)?;
        require(
            required <= caps.memory_bytes,
            &format!(
                "pack memory admission requires {required} bytes; cap {}",
                caps.memory_bytes
            ),
        )?;
        Ok(required)
    }
    fn reserved<T>(n: usize) -> Result<Vec<T>> {
        require(
            n.checked_mul(std::mem::size_of::<T>())
                .is_some_and(|v| v <= MEMORY_CAP),
            "pack allocation overflow/cap",
        )?;
        let mut out = Vec::new();
        out.try_reserve_exact(n)?;
        require(out.capacity() == n, "pack unexpected allocation capacity")?;
        Ok(out)
    }
    fn filled<T: Clone>(n: usize, value: T) -> Result<Vec<T>> {
        let mut out = reserved(n)?;
        out.resize(n, value);
        Ok(out)
    }

    // Resolve every component through pinned directories; reject ancestor as
    // well as final symlinks, and never block on a FIFO masquerading as a file.
    fn secure_open(path: &Path, flags: rustix::fs::OFlags) -> Result<File> {
        #[cfg(test)]
        OPENS.with(|opens| opens.borrow_mut().push(path.into()));
        use rustix::fs::{Mode, OFlags as F};
        require(
            path.is_absolute() && path.as_os_str().len() <= 4096,
            "pack absolute bounded path",
        )?;
        let mut directory = File::from(rustix::fs::open(
            "/",
            F::RDONLY | F::DIRECTORY | F::CLOEXEC,
            Mode::empty(),
        )?);
        let mut components = path.components();
        require(
            components.next() == Some(Component::RootDir),
            "pack absolute path",
        )?;
        require(
            components.all(|component| matches!(component, Component::Normal(_))),
            "pack normal path components",
        )?;
        let mut components = path.components().skip(1).peekable();
        let mut count = 0;
        while let Some(Component::Normal(name)) = components.next() {
            count += 1;
            require(count <= 128, "pack path component cap")?;
            let last = components.peek().is_none();
            let fd = rustix::fs::openat(
                &directory,
                name,
                (if last {
                    flags
                } else {
                    F::RDONLY | F::DIRECTORY
                }) | F::NOFOLLOW
                    | F::NONBLOCK
                    | F::CLOEXEC,
                Mode::empty(),
            )?;
            directory = File::from(fd);
        }
        Ok(directory)
    }
    fn same_artifact(a: &Artifact, b: &Artifact) -> bool {
        a.path == b.path && a.bytes == b.bytes && a.sha256 == b.sha256
    }
    fn read_pinned(a: &Artifact, cap: usize, prefix: bool, guard: &mut Guard) -> Result<Vec<u8>> {
        digest(&a.sha256)?;
        require(a.bytes > 0 && a.bytes <= cap, "pack input cap before body")?;
        let mut file = secure_open(&a.path, rustix::fs::OFlags::RDONLY)?;
        let metadata = file.metadata()?;
        require(
            metadata.is_file()
                && if prefix {
                    metadata.len() >= a.bytes as u64
                } else {
                    metadata.len() == a.bytes as u64
                },
            "pack regular descriptor length",
        )?;
        let mut body = filled(a.bytes, 0u8)?;
        // No BufReader and no EOF probe: even prefetch must not touch the tail.
        let mut input = (&mut file).take(a.bytes as u64);
        for part in body.chunks_mut(65536) {
            guard.tick(part.len() as u64)?;
            input.read_exact(part)?;
            #[cfg(test)]
            READS.with(|reads| reads.borrow_mut().push((a.path.clone(), part.len())));
        }
        require(hash(&body) == a.sha256, "pack authenticated input SHA256")?;
        guard.tick(0)?;
        Ok(body)
    }
    struct Outputs {
        parent: File,
        output: PathBuf,
        report: File,
        bytes: usize,
        cap: usize,
        #[cfg(test)]
        fail_sync: Option<usize>,
        #[cfg(test)]
        sync_count: usize,
    }
    impl Outputs {
        fn create(output: &Path) -> Result<Self> {
            let parent = secure_open(
                output.parent().ok_or("pack output parent")?,
                rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY,
            )?;
            let report = Self::create_at(&parent, output)?;
            Ok(Self {
                parent,
                output: output.into(),
                report,
                bytes: 0,
                cap: OUTPUT_CAP,
                #[cfg(test)]
                fail_sync: None,
                #[cfg(test)]
                sync_count: 0,
            })
        }
        fn create_at(parent: &File, path: &Path) -> Result<File> {
            use rustix::fs::{Mode, OFlags as F};
            let file = File::from(rustix::fs::openat(
                parent,
                path.file_name().ok_or("pack output basename")?,
                F::WRONLY | F::CREATE | F::EXCL | F::NOFOLLOW | F::NONBLOCK | F::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )?);
            require(file.metadata()?.is_file(), "pack regular output")?;
            Ok(file)
        }
        fn sync(&mut self, file: &File) -> Result<()> {
            #[cfg(test)]
            {
                self.sync_count += 1;
                require(
                    self.fail_sync != Some(self.sync_count),
                    "pack injected file sync failure",
                )?;
            }
            file.sync_all()?;
            #[cfg(test)]
            {
                self.sync_count += 1;
                require(
                    self.fail_sync != Some(self.sync_count),
                    "pack injected parent sync failure",
                )?;
            }
            self.parent.sync_all()?;
            Ok(())
        }
        fn publish(&mut self, extension: &str, body: &[u8]) -> Result<Artifact> {
            require(
                self.bytes
                    .checked_add(body.len())
                    .and_then(|n| n.checked_add(8192))
                    .is_some_and(|n| n <= self.cap),
                "pack output cap before write",
            )?;
            let path = self.output.with_extension(extension);
            require(path != self.output, "pack output companion collision")?;
            let mut file = Self::create_at(&self.parent, &path)?;
            file.write_all(body)?;
            self.bytes += body.len();
            self.sync(&file)?;
            Ok(Artifact {
                path,
                bytes: body.len(),
                sha256: hash(body),
            })
        }
        fn finish(&mut self, value: &Value) -> Result<()> {
            let body = serde_json::to_vec(value)?;
            require(
                self.bytes
                    .checked_add(body.len())
                    .is_some_and(|n| n <= self.cap),
                "pack terminal output cap",
            )?;
            self.report.write_all(&body)?;
            // A cloned handle references the created inode, never a reopened path.
            let file = self.report.try_clone()?;
            self.sync(&file)
        }
        fn invalidate(&mut self, config_sha: &str, error: &dyn std::fmt::Display) {
            let body = serde_json::to_vec(&terminal(
                config_sha,
                "INVALID",
                false,
                json!({"error":error.to_string().chars().take(512).collect::<String>()}),
            ))
            .unwrap_or_default();
            // Only our owned inode is rewritten after failure. The exit remains
            // nonzero even if this best-effort invalidation itself cannot sync.
            let _ = self.report.set_len(0);
            let _ = self.report.seek(SeekFrom::Start(0));
            let _ = self.report.write_all(&body);
            let _ = self.report.sync_all();
            let _ = self.parent.sync_all();
        }
    }
    fn terminal(config_sha: &str, status: &str, complete: bool, details: Value) -> Value {
        json!({"schema":REPORT_SCHEMA,"status":status,"complete":complete,"config_sha256":config_sha,
            "diagnostic_source_sha256":diagnostic_sources(),
            "standalone_authority":false,"requires_matching_supervisor_exit_receipt":true,
            "quality_or_performance_claim":false,"truth_opened":false,"requests_opened":false,
            "sq8_canonical_pq_bodies_opened":false,"scope":"necessary locality only; preserved nominees do not preserve returned recall",
            "details":details})
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct OriginalPanel {
        dataset: String,
        root: Artifact,
        requests: Artifact,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct OriginalSeal {
        schema: String,
        config_sha256: String,
        source_identity_sha256: String,
        prefix_bytes: usize,
        prefix_sha256: String,
        plans_per_panel: usize,
        panels: [OriginalPanel; 2],
        truth_opened: bool,
    }
    struct Protocol {
        rows: usize,
        dimensions: usize,
        prefix_bytes: usize,
        prefix_sha: String,
        seal_sha: String,
    }
    impl Protocol {
        fn frozen() -> Self {
            Self {
                rows: 100_000,
                dimensions: 768,
                prefix_bytes: PREFIX_BYTES,
                prefix_sha: PREFIX_SHA.into(),
                seal_sha: SEAL_SHA.into(),
            }
        }
    }
    fn validate_config(config: &Config, protocol: &Protocol) -> Result<()> {
        require(
            (2..=100_000).contains(&protocol.rows)
                && (1..=768).contains(&protocol.dimensions)
                && (1..=PREFIX_BYTES).contains(&protocol.prefix_bytes),
            "pack bounded diagnostic geometry",
        )?;
        require(
            config.schema == CONFIG_SCHEMA
                && config.panels[0].dataset == "relaion"
                && config.panels[1].dataset == "cohere"
                && config.caps.memory_bytes <= MEMORY_CAP
                && config.caps.memory_bytes >= FIXED_WORKSPACE
                && (8192..=OUTPUT_CAP).contains(&config.caps.output_bytes)
                && (1..=300).contains(&config.caps.deadline_seconds)
                && (1..=OPERATIONS).contains(&config.caps.operations)
                && config.caps.cpu_threads == 1
                && config.caps.swap_bytes == 0
                && config.prefix.bytes == protocol.prefix_bytes
                && config.prefix.sha256 == protocol.prefix_sha
                && config.original_seal.sha256 == protocol.seal_sha
                && config.original_seal.bytes <= 4096,
            "pack fixed schema/prefix/seal/resource contract",
        )?;
        for panel in &config.panels {
            require(
                panel.root.bytes <= ROOT_CAP
                    && panel.identity.rows == protocol.rows
                    && panel.identity.dimensions == protocol.dimensions
                    && panel.graph.bytes <= protocol.rows * 512,
                "pack panel geometry/input caps",
            )?;
            digest(&panel.root.sha256)?;
            digest(&panel.graph.sha256)?;
        }
        Ok(())
    }

    // Adjacency contains one target per original directed edge in EACH
    // direction. Multiplicity is the unordered-pair weight; no dense matrix.
    struct Adjacency {
        offsets: Vec<usize>,
        targets: Vec<u32>,
    }
    fn adjacency(
        graph: &PqVectorGraph,
        groups: usize,
        caps: &Caps,
        guard: &mut Guard,
        retained: usize,
    ) -> Result<(Adjacency, usize)> {
        let mut directed = 0usize;
        graph.inspect_authenticated_base_edges(|a, b| {
            guard.tick(1)?;
            if a / 16 != b / 16 {
                directed = directed.checked_add(1).ok_or("pack edge count overflow")?;
            }
            Ok(())
        })?;
        let slots = directed
            .checked_mul(2)
            .ok_or("pack adjacency count overflow")?;
        let bytes = slots.checked_mul(4).ok_or("pack adjacency byte overflow")?;
        let peak = memory(
            caps,
            &[
                FIXED_WORKSPACE,
                retained,
                graph.heap_bytes(),
                bytes,
                (groups + 1) * 16,
                groups * 32,
            ],
        )?;
        let mut offsets = filled(groups + 1, 0usize)?;
        graph.inspect_authenticated_base_edges(|a, b| {
            guard.tick(1)?;
            let (a, b) = ((a / 16) as usize, (b / 16) as usize);
            if a != b {
                offsets[a + 1] += 1;
                offsets[b + 1] += 1;
            }
            Ok(())
        })?;
        for i in 1..=groups {
            offsets[i] += offsets[i - 1];
        }
        require(offsets[groups] == slots, "pack adjacency count")?;
        let mut cursor = reserved(groups)?;
        cursor.extend_from_slice(&offsets[..groups]);
        let mut targets = filled(slots, 0u32)?;
        graph.inspect_authenticated_base_edges(|a, b| {
            guard.tick(1)?;
            let (a, b) = ((a / 16) as usize, (b / 16) as usize);
            if a != b {
                targets[cursor[a]] = b as u32;
                cursor[a] += 1;
                targets[cursor[b]] = a as u32;
                cursor[b] += 1;
            }
            Ok(())
        })?;
        Ok((Adjacency { offsets, targets }, peak))
    }
    fn pack(adjacency: &Adjacency, rows: usize, guard: &mut Guard) -> Result<Vec<u32>> {
        let groups = rows.div_ceil(16);
        let full = rows / 16;
        require(
            adjacency.offsets.len() == groups + 1,
            "pack adjacency geometry",
        )?;
        let mut used = filled(groups, false)?;
        let mut scores = filled(groups, 0u64)?;
        let mut new_to_old = reserved(groups)?;
        // ponytail: bounded O(groups^2) selection, at most 6,250 groups; use an
        // indexed heap only if a separately qualified larger diagnostic needs it.
        while new_to_old.len() < full {
            scores.fill(0);
            guard.tick(groups as u64)?;
            let seed = (0..full).find(|&g| !used[g]).ok_or("pack seed")?;
            let count = PACK_GROUPS.min(full - new_to_old.len());
            let mut next = seed;
            for position in 0..count {
                used[next] = true;
                new_to_old.push(next as u32);
                for &target in
                    &adjacency.targets[adjacency.offsets[next]..adjacency.offsets[next + 1]]
                {
                    guard.tick(1)?;
                    let target = target as usize;
                    require(target < groups, "pack adjacency ordinal")?;
                    if target < full && !used[target] {
                        scores[target] = scores[target]
                            .checked_add(1)
                            .ok_or("pack affinity overflow")?;
                    }
                }
                if position + 1 < count {
                    let mut best: Option<usize> = None;
                    for g in 0..full {
                        guard.tick(1)?;
                        if !used[g] && best.is_none_or(|b| scores[g] > scores[b]) {
                            best = Some(g);
                        }
                    }
                    next = best.ok_or("pack next group")?;
                }
            }
        }
        if rows % 16 != 0 {
            new_to_old.push(full as u32);
        }
        let mut old_to_new = filled(groups, u32::MAX)?;
        for (new, &old) in new_to_old.iter().enumerate() {
            require(
                (old as usize) < groups && old_to_new[old as usize] == u32::MAX,
                "pack bijection",
            )?;
            old_to_new[old as usize] = new as u32;
        }
        require(
            old_to_new.iter().all(|&n| n != u32::MAX)
                && (rows % 16 == 0 || old_to_new[full] == full as u32),
            "pack bijection/tail",
        )?;
        Ok(old_to_new)
    }
    fn build_permutation(
        panel: &Panel,
        caps: &Caps,
        guard: &mut Guard,
        retained: usize,
    ) -> Result<(Vec<u32>, usize)> {
        let body = read_pinned(&panel.root, ROOT_CAP, false, guard)?;
        let manifest: Manifest = serde_json::from_slice(&body)?;
        validate_manifest(&manifest, &panel.root)?;
        require(
            manifest.identity == panel.identity && same_artifact(&manifest.graph, &panel.graph),
            "pack pinned graph/root identity",
        )?;
        let n = panel.identity.rows;
        let g = n.div_ceil(16);
        let mut peak = memory(
            caps,
            &[
                FIXED_WORKSPACE,
                retained,
                panel.graph.bytes,
                n * 512,
                n * 16,
                g * 32,
            ],
        )?;
        let encoded = read_pinned(&panel.graph, n * 512, false, guard)?;
        let graph = PqVectorGraph::decode_for_inspection(&encoded, &panel.identity, &mut |n| {
            guard.tick(n)
        })?;
        drop(encoded);
        let (adjacency, adjacency_peak) = adjacency(&graph, g, caps, guard, retained)?;
        peak = peak.max(adjacency_peak);
        drop(graph);
        let permutation = pack(&adjacency, n, guard)?;
        guard.tick(0)?;
        Ok((permutation, peak))
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct FrozenPlan {
        root_sha256: String,
        query_sha256: String,
        revision: u64,
        mutation_sha256: String,
        nominees: Vec<usize>,
        ranges: Vec<Range<usize>>,
        planned_bytes: usize,
        feasible: bool,
        exhausted: bool,
        converged: bool,
        actual_shortlist_rows: usize,
        evaluations: usize,
        base_visits: usize,
        old_page_gets: usize,
        old_page_bytes: usize,
    }
    #[derive(Deserialize)]
    #[serde(tag = "phase", deny_unknown_fields)]
    enum FrozenEvent {
        #[serde(rename = "startup")]
        Startup {
            dataset: String,
            root: Artifact,
            resources: ResourceReceipt,
            build: FineBuildReceipt,
            truth_opened: bool,
            wall_ns: u64,
            process_cpu_ns: i64,
        },
        #[serde(rename = "fine_plan")]
        Plan {
            dataset: String,
            ordinal: usize,
            plan: FrozenPlan,
            truth_opened: bool,
            wall_ns: u64,
            process_cpu_ns: i64,
        },
    }
    fn mapped_cover(
        nominees: &[usize],
        map: &[u32],
        rows: usize,
        d: usize,
        gets: usize,
    ) -> Result<(Vec<usize>, Vec<Range<usize>>, usize)> {
        let mut mapped = reserved(nominees.len())?;
        for &old in nominees {
            require(old < rows && old / 16 < map.len(), "pack nominee ordinal")?;
            let new = map[old / 16] as usize * 16 + old % 16;
            require(new < rows, "pack mapped ordinal")?;
            mapped.push(new);
        }
        let selected = mapped.iter().map(|v| v / 16).collect::<BTreeSet<_>>();
        let (ranges, bytes) = cover_pages(&selected, rows, d + 12, 16, gets)
            .map_err(|e| format!("pack cover: {e:?}"))?;
        require(
            mapped.iter().all(|n| {
                ranges
                    .iter()
                    .any(|r| r.start <= n * (d + 12) && (n + 1) * (d + 12) <= r.end)
            }),
            "pack every nominee retained",
        )?;
        Ok((mapped, ranges, bytes))
    }
    fn replay(
        body: &[u8],
        config: &Config,
        maps: &[Vec<u32>; 2],
        guard: &mut Guard,
    ) -> Result<Value> {
        require(body.last() == Some(&b'\n'), "pack complete newline prefix")?;
        let mut queries = Vec::new();
        let mut passed = [0usize; 2];
        let mut maxima = [0usize; 2];
        let mut lines = std::str::from_utf8(body)?.lines();
        for panel in &config.panels {
            let line = lines.next().ok_or("pack missing startup")?;
            require(line.len() <= ROOT_CAP, "pack startup decoder cap")?;
            let event: FrozenEvent = serde_json::from_str(line)?;
            let FrozenEvent::Startup {
                dataset,
                root,
                resources,
                build,
                truth_opened,
                wall_ns,
                process_cpu_ns,
            } = event
            else {
                return Err("pack startup shape".into());
            };
            let _ = (resources, wall_ns);
            require(
                dataset == panel.dataset
                    && same_artifact(&root, &panel.root)
                    && !truth_opened
                    && build.source_rows == panel.identity.rows
                    && process_cpu_ns >= 0,
                "pack startup binding",
            )?;
        }
        for (p, panel) in config.panels.iter().enumerate() {
            let n = panel.identity.rows;
            let d = panel.identity.dimensions;
            for expected in 0..64 {
                guard.tick(0)?;
                let line = lines.next().ok_or("pack incomplete 128-query prefix")?;
                require(line.len() <= ROOT_CAP, "pack plan decoder cap")?;
                let event: FrozenEvent = serde_json::from_str(line)?;
                let FrozenEvent::Plan {
                    dataset,
                    ordinal,
                    plan,
                    truth_opened,
                    wall_ns,
                    process_cpu_ns,
                } = event
                else {
                    return Err("pack plan shape".into());
                };
                let _ = wall_ns;
                digest(&plan.query_sha256)?;
                let unique = plan.nominees.iter().copied().collect::<BTreeSet<_>>();
                require(
                    dataset == panel.dataset
                        && ordinal == expected
                        && !truth_opened
                        && process_cpu_ns >= 0
                        && plan.root_sha256 == panel.root.sha256
                        && plan.revision == 0
                        && plan.mutation_sha256.is_empty()
                        && !plan.nominees.is_empty()
                        && plan.nominees.len() <= 1024
                        && unique.len() == plan.nominees.len()
                        && unique.last().is_some_and(|&id| id < n)
                        && plan.actual_shortlist_rows == plan.nominees.len()
                        && plan.converged != plan.exhausted
                        && plan.evaluations <= 65536
                        && plan.base_visits <= n.min(plan.evaluations),
                    "pack frozen dataset/root/query/ordinal/nominee shape",
                )?;
                let selected = unique.iter().map(|n| n / 16).collect();
                let (old_ranges, old_bytes) = cover_pages(&selected, n, d + 12, 16, MAX_GETS)
                    .map_err(|e| format!("pack original cover {e:?}"))?;
                let old_pages = unique.iter().map(|n| n / 256).collect();
                let (coarse, coarse_bytes) = cover_pages(&old_pages, n, d + 12, 256, MAX_GETS)
                    .map_err(|e| format!("pack original coarse cover {e:?}"))?;
                require(
                    plan.ranges == old_ranges
                        && plan.planned_bytes == old_bytes
                        && plan.feasible == (old_bytes <= MAX_BYTES)
                        && plan.old_page_gets == coarse.len()
                        && plan.old_page_bytes == coarse_bytes,
                    "pack original exact cover parity",
                )?;
                let (mapped, ranges, bytes) = mapped_cover(&plan.nominees, &maps[p], n, d, 32)?;
                guard.tick((plan.nominees.len() * 128) as u64)?;
                passed[p] += usize::from(bytes <= MAX_BYTES);
                maxima[p] = maxima[p].max(bytes);
                queries.push(json!({"dataset":dataset,"ordinal":ordinal,"query_sha256":plan.query_sha256,
                    "nominees":plan.nominees.len(),"nominees_sha256":hash(&serde_json::to_vec(&plan.nominees)?),
                    "mapped_nominees_sha256":hash(&serde_json::to_vec(&mapped)?),"all_nominees_retained":true,
                    "ranges":ranges,"planned_bytes":bytes,"fits":bytes<=MAX_BYTES}));
            }
        }
        require(
            lines.next().is_none(),
            "pack prefix contains only two startups and 128 plans",
        )?;
        Ok(
            json!({"per_panel_passes":passed,"per_panel_max_bytes":maxima,"queries":queries,"survived":passed == [64,64]}),
        )
    }

    fn run(
        config: &Config,
        config_sha: &str,
        outputs: &mut Outputs,
        protocol: &Protocol,
        guard: &mut Guard,
        before_prefix: &mut impl FnMut(&Artifact) -> Result<()>,
    ) -> Result<Value> {
        validate_config(config, protocol)?;
        outputs.cap = config.caps.output_bytes;
        let seal_body = read_pinned(&config.original_seal, 4096, false, guard)?;
        let seal: OriginalSeal = serde_json::from_slice(&seal_body)?;
        digest(&seal.config_sha256)?;
        digest(&seal.source_identity_sha256)?;
        require(
            seal.schema == "borsuk-fine-sq8-seal-v1"
                && !seal.truth_opened
                && seal.plans_per_panel == 64
                && seal.prefix_bytes == config.prefix.bytes
                && seal.prefix_sha256 == config.prefix.sha256,
            "pack original seal binding",
        )?;
        for (original, panel) in seal.panels.iter().zip(&config.panels) {
            digest(&original.requests.sha256)?;
            require(
                original.dataset == panel.dataset && same_artifact(&original.root, &panel.root),
                "pack original sealed root",
            )?;
        }
        let mut maps = [Vec::new(), Vec::new()];
        let mut peaks = [0usize; 2];
        for i in 0..2 {
            (maps[i], peaks[i]) = build_permutation(
                &config.panels[i],
                &config.caps,
                guard,
                maps.iter().map(|m| m.capacity() * 4).sum(),
            )?;
        }
        let permutations = serde_json::to_vec(
            &json!({"schema":"borsuk-fine-pack-permutations-v1","config_sha256":config_sha,
            "diagnostic_source_sha256":diagnostic_sources(),"original_trace_source_identity_sha256":seal.source_identity_sha256,
            "original_seal":config.original_seal,"nomination_prefix":config.prefix,
            "group_rows":16,"groups_per_pack":42,"panels":config.panels,"old_to_new":maps,
            "permutation_sha256_le_u32":maps.each_ref().map(|map| {
                let mut sha=Sha256::new();for old in map {sha.update(old.to_le_bytes());}format!("{:x}",sha.finalize())
            }),
            "rule":"lowest unassigned seed; max summed directed-base-edge unordered affinity; old ID ties; short group last",
            "query_blind":true}),
        )?;
        let permutation_seal = outputs.publish("permutations.json", &permutations)?;
        // Both arrays and their joint binding are durably published before even
        // acquiring the nomination descriptor. This hook is private and test-only in use.
        before_prefix(&permutation_seal)?;
        let prefix = read_pinned(
            &config.prefix,
            PREFIX_BYTES.max(protocol.prefix_bytes),
            true,
            guard,
        )?;
        let prefix_receipt = outputs.publish("prefix.jsonl", &prefix)?;
        let replay = replay(&prefix, config, &maps, guard)?;
        guard.tick(0)?;
        let mut report = terminal(
            config_sha,
            if replay["survived"] == true {
                "SURVIVED_NECESSARY_LOCALITY"
            } else {
                "REJECT"
            },
            true,
            json!({"original_seal":config.original_seal,"original_seal_bytes_sha256":hash(&seal_body),
                "original_trace_source_identity_sha256":seal.source_identity_sha256,
                "prefix":prefix_receipt,"permutation_seal":permutation_seal,"replay":replay,
                "modeled_peak_bytes":peaks,"caps":config.caps,"operations":guard.operations,
                "operation_units":"input bytes + edges (duplicate scan bounded at 256) + group scans + cover allowance",
                "wall_ms":guard.start.elapsed().as_millis(),"runtime_reserve_bytes":32*1024*1024,
                "recall_caveat":"CoHere original nominee containment 95.609375%, p05 87; incidental rows rescued returned quality"}),
        );
        report["queries"] = json!(128);
        Ok(report)
    }

    /// Strict frozen CONFIG SHA NEW_OUTPUT interface. Inputs are local pinned
    /// roots/graphs/seal and nomination prefix only; all other bodies stay closed.
    /// The original supervisor must enforce CPU1/256MiB/no-swap/300s and supply
    /// observed exit, resources, drain and cleanup before admitting survival.
    pub fn check_fine_pack(config_path: &Path, config_sha: &str, output: &Path) -> Result<()> {
        let mut outputs = Outputs::create(output)?;
        let result = (|| {
            let caps = Caps {
                memory_bytes: MEMORY_CAP,
                output_bytes: OUTPUT_CAP,
                deadline_seconds: 300,
                operations: OPERATIONS,
                cpu_threads: 1,
                swap_bytes: 0,
            };
            let mut guard = Guard::new(&caps);
            let file = secure_open(config_path, rustix::fs::OFlags::RDONLY)?;
            let metadata = file.metadata()?;
            require(
                metadata.is_file() && metadata.len() <= ROOT_CAP as u64,
                "pack config regular/cap",
            )?;
            // Config cannot redirect an input after admission: its bytes are
            // read and hashed from the same pinned descriptor.
            let mut body = filled(metadata.len() as usize, 0u8)?;
            file.take(ROOT_CAP as u64).read_exact(&mut body)?;
            require(hash(&body) == config_sha, "pack config SHA256")?;
            let config: Config = serde_json::from_slice(&body)?;
            require(
                crate::configured_cpu_threads() == 1,
                "pack requires BORSUK_NUM_THREADS=1",
            )?;
            validate_config(&config, &Protocol::frozen())?;
            guard.deadline = Duration::from_secs(config.caps.deadline_seconds);
            guard.limit = config.caps.operations;
            let report = run(
                &config,
                config_sha,
                &mut outputs,
                &Protocol::frozen(),
                &mut guard,
                &mut |_| Ok(()),
            )?;
            outputs.finish(&report)?;
            guard.tick(0)
        })();
        if let Err(error) = &result {
            outputs.invalidate(config_sha, error);
        }
        result
    }

    /// Bounded diagnostic API for caller-authenticated synthetic/source fixtures.
    /// The CLI additionally pins the historical 100k/D768 seal and prefix. This
    /// API uses the same algorithm and 128-query shape, with no serving mutation.
    pub fn diagnose(config: &Config, config_sha: &str, output: &Path) -> Result<()> {
        let mut outputs = Outputs::create(output)?;
        let result = (|| {
            digest(config_sha)?;
            digest(&config.prefix.sha256)?;
            digest(&config.original_seal.sha256)?;
            let protocol = Protocol {
                rows: config.panels[0].identity.rows,
                dimensions: config.panels[0].identity.dimensions,
                prefix_bytes: config.prefix.bytes,
                prefix_sha: config.prefix.sha256.clone(),
                seal_sha: config.original_seal.sha256.clone(),
            };
            let mut guard = Guard::new(&config.caps);
            let report = run(
                config,
                config_sha,
                &mut outputs,
                &protocol,
                &mut guard,
                &mut |_| Ok(()),
            )?;
            outputs.finish(&report)?;
            guard.tick(0)
        })();
        if let Err(error) = &result {
            outputs.invalidate(config_sha, error);
        }
        result
    }

    /// Fixed SQ4 falsifier; deliberately separate from the serving index.
    pub mod sq4_diagnostic {
        use super::*;
        pub use super::{Caps, SupervisorReceipt};

        pub const CONFIG_SCHEMA: &str = "borsuk-fixed-sq4-config-v1";
        pub const REPORT_SCHEMA: &str = "borsuk-fixed-sq4-report-v1";
        const CODEC: &str = "borsuk-sq4-nearest17-original-coefficients-v1";
        const RESERVE: usize = 8192;
        const FIXED: usize = 64 * 1024 * 1024;
        const REQUEST_CAP: usize = 4 * 1024 * 1024;
        const RESULT_CAP: usize = 8 * 1024 * 1024;
        // paired100k/a0002/screen/fine/measurement/paired-fine-config.json,
        // independently corroborated by the archived aws-terminal.json roster.
        const TRUTH_SHA256: [&str; 2] = [
            "3ad233f399ba5172051e747f24dbde402bdd357a69460c349ec5ed5872ee5a5c",
            "f6630d0edf06539752c3fbf129ae01e58d3a3cf7b6aefa4decaa9c979e8ba355",
        ];
        fn archived_truth(truth: [&Artifact; 2]) -> bool {
            truth
                .into_iter()
                .zip(TRUTH_SHA256)
                .all(|(a, sha)| a.bytes == 25_600 && a.sha256 == sha)
        }
        #[cfg(test)]
        thread_local! {
            static APPEND_ON_OPEN: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
            static TAMPER_CLOSURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
            static APPEND_AFTER_TRANSCODE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
        }

        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct Panel {
            pub dataset: String,
            pub root: Artifact,
            pub requests: Artifact,
            pub truth: Artifact,
            pub truth_width: usize,
        }
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct Config {
            pub schema: String,
            pub source_identity_sha256: String,
            pub panels: [Panel; 2],
            pub original_seal: Artifact,
            pub prefix: Artifact,
            pub caps: Caps,
        }

        /// Source closure for this diagnostic, including the unchanged scorer.
        pub fn source_identity() -> String {
            let mut h = Sha256::new();
            for (name, body) in [
                (
                    "fine_sq8_groups.rs",
                    include_bytes!("fine_sq8_groups.rs").as_slice(),
                ),
                (
                    "bin/hierarchical_semantic_cells.rs",
                    include_bytes!("bin/hierarchical_semantic_cells.rs").as_slice(),
                ),
                (
                    "budgeted_page_rank.rs",
                    include_bytes!("budgeted_page_rank.rs").as_slice(),
                ),
                (
                    "exact_sq8_nominee.rs",
                    include_bytes!("exact_sq8_nominee.rs").as_slice(),
                ),
                (
                    "returned_sq8.rs",
                    include_bytes!("returned_sq8.rs").as_slice(),
                ),
                ("sq8_source.rs", include_bytes!("sq8_source.rs").as_slice()),
                (
                    "sq8_page_authority.rs",
                    include_bytes!("sq8_page_authority.rs").as_slice(),
                ),
                (
                    "resident_vector_graph.rs",
                    include_bytes!("resident_vector_graph.rs").as_slice(),
                ),
                (
                    "pq64_nominee.rs",
                    include_bytes!("pq64_nominee.rs").as_slice(),
                ),
                (
                    "hierarchical_semantic_cells.rs",
                    include_bytes!("hierarchical_semantic_cells.rs").as_slice(),
                ),
                ("lib.rs", include_bytes!("lib.rs").as_slice()),
                (
                    "centroid_hnsw.rs",
                    include_bytes!("centroid_hnsw.rs").as_slice(),
                ),
            ] {
                h.update((name.len() as u64).to_le_bytes());
                h.update(name.as_bytes());
                h.update((body.len() as u64).to_le_bytes());
                h.update(body);
            }
            format!("{:x}", h.finalize())
        }

        fn terminal(config_sha: &str, status: &str, complete: bool, details: Value) -> Value {
            json!({"schema":REPORT_SCHEMA,"codec":CODEC,"status":status,"complete":complete,
                "config_sha256":config_sha,"source_identity_sha256":source_identity(),"details":details,
                "queries":if complete {128} else {0},"standalone_authority":false,
                "requires_matching_supervisor_exit_receipt":true,"quality_or_performance_claim":false,
                "scope":"consumed64 FIRST100k only; no fresh quality, S3, lifecycle, or 100M RAM claim"})
        }
        #[derive(Default)]
        struct Progress {
            stage: &'static str,
            scored_queries: usize,
            truth_opened: bool,
            freeze: Option<Artifact>,
            operations: u64,
        }
        fn invalidate(
            output: &mut Outputs,
            sha: &str,
            error: &dyn std::fmt::Display,
            progress: &Progress,
        ) {
            let mut value = terminal(
                sha,
                "INVALID",
                false,
                json!({"error":error.to_string().chars().take(512).collect::<String>(),
                    "stage":progress.stage,"scored_queries":progress.scored_queries,
                    "truth_opened":progress.truth_opened,"freeze":progress.freeze,
                    "published_bytes":output.bytes,"operations":progress.operations}),
            );
            value["queries"] = json!(progress.scored_queries);
            if let Ok(body) = serde_json::to_vec(&value) {
                // Only the inode created by this run is rewritten. A failed sync
                // always leaves the process unsuccessful, even if retry succeeds.
                let _ = output.report.set_len(0);
                let _ = output.report.seek(SeekFrom::Start(0));
                let _ = output.report.write_all(&body);
                let _ = output.report.sync_all();
                let _ = output.parent.sync_all();
            }
        }

        fn config_valid(config: &Config) -> Result<()> {
            require(
                config.schema == CONFIG_SCHEMA
                    && config.source_identity_sha256 == source_identity()
                    && config.panels[0].dataset == "relaion"
                    && config.panels[1].dataset == "cohere"
                    && (FIXED..=1024 * 1024 * 1024).contains(&config.caps.memory_bytes)
                    && (RESERVE..=256 * 1024 * 1024).contains(&config.caps.output_bytes)
                    && (1..=600).contains(&config.caps.deadline_seconds)
                    && (1..=20_000_000_000).contains(&config.caps.operations)
                    && config.caps.cpu_threads == 1
                    && config.caps.swap_bytes == 0
                    && config.original_seal.bytes <= 4096
                    && config.prefix.bytes <= 2 * 1024 * 1024,
                "SQ4 fixed config/code/resource contract",
            )?;
            for panel in &config.panels {
                require(
                    panel.root.bytes <= ROOT_CAP
                        && panel.requests.bytes <= REQUEST_CAP
                        && panel.truth_width == 100
                        && panel.truth.bytes == 64 * 100 * 4,
                    "SQ4 root/request/truth geometry caps",
                )?;
                for a in [&panel.root, &panel.requests, &panel.truth] {
                    digest(&a.sha256)?;
                }
            }
            digest(&config.prefix.sha256)?;
            digest(&config.original_seal.sha256)?;
            Ok(())
        }
        fn allocation_model(config: &Config, n: usize, d: usize) -> Result<usize> {
            require(
                (100..=100_000).contains(&n) && (1..=768).contains(&d),
                "SQ4 bounded geometry",
            )?;
            // Fixed reserve covers JSON trees/serialization, manifests, two
            // metadata sets and allocator/runtime overhead. Variable terms cover
            // simultaneous prefix/decoded plans, request parser, and one range's
            // compressed bytes + expanded bytes + native scorer + full ranking.
            // Original-SQ8 and SQ4 reads run sequentially, never coexist.
            memory(
                &config.caps,
                &[
                    FIXED,
                    config
                        .prefix
                        .bytes
                        .checked_mul(16)
                        .ok_or("SQ4 prefix model overflow")?,
                    config
                        .panels
                        .iter()
                        .map(|p| p.requests.bytes)
                        .max()
                        .unwrap_or(0)
                        .checked_mul(16)
                        .ok_or("SQ4 request model overflow")?,
                    n.checked_mul(64).ok_or("SQ4 resident model overflow")?,
                    n.checked_mul(12 + d.div_ceil(2) + 12 + d + 256)
                        .ok_or("SQ4 scoring model overflow")?,
                ],
            )
        }
        fn descriptor(a: &Artifact, prefix: bool) -> Result<File> {
            digest(&a.sha256)?;
            let file = secure_open(&a.path, rustix::fs::OFlags::RDONLY)?;
            let meta = file.metadata()?;
            require(
                meta.is_file()
                    && if prefix {
                        meta.len() >= a.bytes as u64
                    } else {
                        meta.len() == a.bytes as u64
                    },
                "SQ4 regular exact descriptor",
            )?;
            #[cfg(test)]
            APPEND_ON_OPEN.with(|path| -> Result<()> {
                if path.borrow().as_ref() == Some(&a.path) {
                    path.borrow_mut().take();
                    OpenOptions::new()
                        .append(true)
                        .open(&a.path)?
                        .write_all(b"growth")?;
                }
                Ok(())
            })?;
            Ok(file)
        }
        fn exact_eof(file: &File, bytes: usize) -> Result<()> {
            require(
                file.metadata()?.len() == bytes as u64
                    && file.read_at(&mut [0u8; 1], bytes as u64)? == 0,
                "SQ4 exact descriptor EOF/growth",
            )
        }
        fn read_pinned(
            a: &Artifact,
            cap: usize,
            prefix: bool,
            guard: &mut Guard,
        ) -> Result<Vec<u8>> {
            require(
                a.bytes > 0 && a.bytes <= cap,
                "SQ4 input cap before allocation",
            )?;
            let file = descriptor(a, prefix)?;
            read_descriptor(a, &file, prefix, guard)
        }
        fn read_descriptor(
            a: &Artifact,
            file: &File,
            prefix: bool,
            guard: &mut Guard,
        ) -> Result<Vec<u8>> {
            let mut body = filled(a.bytes, 0u8)?;
            for (i, part) in body.chunks_mut(65536).enumerate() {
                guard.tick(part.len() as u64)?;
                file.read_exact_at(part, (i * 65536) as u64)?;
            }
            require(hash(&body) == a.sha256, "SQ4 pinned SHA256")?;
            // The historical trace intentionally has an unread tail. All other
            // descriptors must still end at the declared body after the read.
            if !prefix {
                exact_eof(file, a.bytes)?;
            }
            Ok(body)
        }
        fn authenticate(a: &Artifact, guard: &mut Guard) -> Result<File> {
            let file = descriptor(a, false)?;
            let mut h = Sha256::new();
            let mut buffer = [0u8; 65536];
            let mut offset = 0;
            while offset < a.bytes {
                let count = buffer.len().min(a.bytes - offset);
                guard.tick(count as u64)?;
                file.read_exact_at(&mut buffer[..count], offset as u64)?;
                h.update(&buffer[..count]);
                offset += count;
            }
            require(
                format!("{:x}", h.finalize()) == a.sha256,
                "SQ4 streamed original SHA256",
            )?;
            exact_eof(&file, a.bytes)?;
            Ok(file)
        }

        fn encode(row: &[u8], low: &[f32], step: &[f32], out: &mut [u8]) -> Result<()> {
            let d = low.len();
            require(
                d > 0
                    && step.len() == d
                    && row.len() == 12 + d
                    && out.len() == 12 + d.div_ceil(2)
                    && low.iter().all(|v| v.is_finite())
                    && step.iter().all(|v| v.is_finite() && *v > 0.),
                "SQ4 codec geometry",
            )?;
            require(
                f32::from_le_bytes(row[8..12].try_into()?).is_finite(),
                "SQ4 original norm",
            )?;
            out.fill(0);
            out[..8].copy_from_slice(&row[..8]);
            let mut norm = 0_f32;
            for axis in 0..d {
                let nibble = ((u16::from(row[12 + axis]) + 8) / 17) as u8;
                out[12 + axis / 2] |= nibble << (4 * (axis % 2));
                // Same ordered f32 reconstruction and accumulation as sq8_source.
                let value = low[axis] + f32::from(17 * nibble) * step[axis];
                norm += value * value;
            }
            require(norm.is_finite(), "SQ4 reconstructed norm")?;
            out[8..12].copy_from_slice(&norm.to_le_bytes());
            Ok(())
        }
        fn expand(body: &[u8], d: usize, out: &mut [u8]) -> Result<()> {
            let width = 12 + d.div_ceil(2);
            let old = 12 + d;
            require(
                d > 0 && body.len() % width == 0 && out.len() == body.len() / width * old,
                "SQ4 expansion geometry",
            )?;
            for (packed, row) in body.chunks_exact(width).zip(out.chunks_exact_mut(old)) {
                require(
                    d % 2 == 0 || packed[width - 1] & 0xf0 == 0,
                    "SQ4 unused high nibble",
                )?;
                row[..12].copy_from_slice(&packed[..12]);
                for axis in 0..d {
                    row[12 + axis] = 17 * ((packed[12 + axis / 2] >> (4 * (axis % 2))) & 15);
                }
            }
            Ok(())
        }
        struct Plane {
            root: Artifact,
            manifest: Manifest,
            ids: Vec<i64>,
            original: File,
            groups: Vec<u8>,
            sq4: File,
            payload: Artifact,
            sq4_hashes: Vec<[u8; 32]>,
        }
        impl Plane {
            fn transcode(
                root: &Artifact,
                output: &mut Outputs,
                extension: &str,
                n: usize,
                d: usize,
                guard: &mut Guard,
            ) -> Result<Self> {
                let width = 12 + d.div_ceil(2);
                let old = 12 + d;
                let bytes = n.checked_mul(width).ok_or("SQ4 payload overflow")?;
                require(
                    sum(&[output.bytes, bytes, RESERVE])? <= output.cap,
                    "SQ4 output cap before transcode",
                )?;
                let manifest: Manifest =
                    serde_json::from_slice(&read_pinned(root, ROOT_CAP, false, guard)?)?;
                validate_manifest(&manifest, root)?;
                require(
                    manifest.identity.rows == n && manifest.identity.dimensions == d,
                    "SQ4 exact paired geometry",
                )?;
                // Authenticate the unchanged router artifacts without decoding or rebuilding them.
                authenticate(&manifest.graph, guard)?;
                authenticate(&manifest.pq, guard)?;
                let order = read_pinned(&manifest.order, n * 8, false, guard)?;
                let mut ids = reserved::<i64>(n)?;
                let mut seen = filled(n, false)?;
                for word in order.chunks_exact(8) {
                    let id = usize::try_from(u64::from_le_bytes(word.try_into()?))?;
                    require(
                        id < n && !std::mem::replace(&mut seen[id], true),
                        "SQ4 order bijection",
                    )?;
                    ids.push(id as i64);
                }
                drop(order);
                drop(seen);
                let groups = read_pinned(&manifest.groups, n.div_ceil(16) * 32, false, guard)?;
                let original = descriptor(&manifest.records, false)?;
                let path = output.output.with_extension(extension);
                require(path != output.output, "SQ4 payload path collision")?;
                let mut file = Outputs::create_at(&output.parent, &path)?;
                let mut input = filled(16 * old, 0u8)?;
                let mut packed = filled(16 * width, 0u8)?;
                let mut sq4_hashes = reserved(n.div_ceil(16))?;
                let mut input_hash = Sha256::new();
                let mut payload_hash = Sha256::new();
                for first in (0..n).step_by(16) {
                    let count = (n - first).min(16);
                    let source = &mut input[..count * old];
                    let destination = &mut packed[..count * width];
                    guard.tick((source.len() + count * d) as u64)?;
                    original.read_exact_at(source, (first * old) as u64)?;
                    require(
                        Sha256::digest(&*source).as_slice()
                            == &groups[first / 16 * 32..(first / 16 + 1) * 32],
                        "SQ4 original group authentication",
                    )?;
                    input_hash.update(&*source);
                    for (slot, (row, target)) in source
                        .chunks_exact(old)
                        .zip(destination.chunks_exact_mut(width))
                        .enumerate()
                    {
                        require(
                            i64::from_le_bytes(row[..8].try_into()?) == ids[first + slot],
                            "SQ4 original row ID binding",
                        )?;
                        encode(row, &manifest.low, &manifest.step, target)?;
                    }
                    file.write_all(destination)?;
                    payload_hash.update(&*destination);
                    sq4_hashes.push(Sha256::digest(&*destination).into());
                }
                require(
                    format!("{:x}", input_hash.finalize()) == manifest.records.sha256,
                    "SQ4 original whole payload authentication",
                )?;
                exact_eof(&original, manifest.records.bytes)?;
                output.bytes += bytes;
                output.sync(&file)?;
                drop(file);
                let payload = Artifact {
                    path,
                    bytes,
                    sha256: format!("{:x}", payload_hash.finalize()),
                };
                let sq4 = descriptor(&payload, false)?;
                Ok(Self {
                    root: root.clone(),
                    manifest,
                    ids,
                    original,
                    groups,
                    sq4,
                    payload,
                    sq4_hashes,
                })
            }
        }
        #[derive(Serialize)]
        struct Plan {
            query_sha256: String,
            nominees: Vec<usize>,
            row_ranges: Vec<Range<usize>>,
            original_row_ranges: Vec<Range<usize>>,
            candidate_bytes: usize,
            envelope_fits: bool,
        }
        fn plan(original: FrozenPlan, n: usize, d: usize) -> Result<Plan> {
            let selected = original
                .nominees
                .iter()
                .map(|i| i / 16)
                .collect::<BTreeSet<_>>();
            let width = 12 + d.div_ceil(2);
            let (ranges, bytes) = cover_pages(&selected, n, width, 16, 32)
                .map_err(|e| format!("SQ4 cover: {e:?}"))?;
            let row_ranges = ranges
                .into_iter()
                .map(|r| r.start / width..r.end / width)
                .collect::<Vec<_>>();
            let original_row_ranges = original
                .ranges
                .iter()
                .map(|r| r.start / (d + 12)..r.end / (d + 12))
                .collect::<Vec<_>>();
            require(
                original
                    .nominees
                    .iter()
                    .all(|id| row_ranges.iter().any(|r| r.contains(id)))
                    && original_row_ranges.iter().all(|old| {
                        row_ranges
                            .iter()
                            .any(|new| new.start <= old.start && new.end >= old.end)
                    }),
                "SQ4 complete nominees and original256 cover containment",
            )?;
            Ok(Plan {
                query_sha256: original.query_sha256,
                nominees: original.nominees,
                row_ranges,
                original_row_ranges,
                candidate_bytes: bytes,
                envelope_fits: bytes <= MAX_BYTES,
            })
        }
        #[derive(Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Score {
            id: i64,
            ordinal: usize,
            score_bits: u32,
        }
        #[derive(Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Scored {
            ranked: Vec<Score>,
            fetched_ids: Vec<i64>,
            range_reads: usize,
            verified_bytes: usize,
            range_payload_capacity_peak: usize,
            expanded_capacity_peak: usize,
            coexisting_score_allocation_bound: usize,
        }
        fn score(
            plane: &Plane,
            ranges: &[Range<usize>],
            query: &[f32],
            packed: bool,
            guard: &mut Guard,
        ) -> Result<Scored> {
            let n = plane.ids.len();
            let d = plane.manifest.identity.dimensions;
            let width = if packed { 12 + d.div_ceil(2) } else { 12 + d };
            let normalized = cosine_vector(query)?;
            let total = ranges.iter().try_fold(0usize, |sum, r| {
                sum.checked_add(r.len()).ok_or("SQ4 range overflow")
            })?;
            require(total <= n, "SQ4 fetched row cap")?;
            let mut all = reserved::<ScoredNominee>(total)?;
            let mut fetched = reserved::<i64>(total)?;
            let mut prior = 0;
            let mut verified = 0;
            let mut range_peak = 0;
            let mut expansion_peak = 0;
            let mut score_peak = 0;
            for r in ranges {
                require(
                    r.start >= prior
                        && r.start < r.end
                        && r.end <= n
                        && r.start % 16 == 0
                        && (r.end == n || r.end % 16 == 0),
                    "SQ4 range geometry",
                )?;
                let mut body = filled(
                    r.len()
                        .checked_mul(width)
                        .ok_or("SQ4 range bytes overflow")?,
                    0u8,
                )?;
                // Every live allocation was admitted before the body read. Exact
                // reservation checks allocator capacity, including expanded bytes.
                let mut expansion = if packed {
                    filled(
                        r.len()
                            .checked_mul(12 + d)
                            .ok_or("SQ4 expansion overflow")?,
                        0u8,
                    )?
                } else {
                    Vec::new()
                };
                let ordinals = (0..r.len()).collect::<Vec<_>>();
                require(
                    ordinals.capacity() == r.len(),
                    "SQ4 ordinal actual capacity",
                )?;
                let actual = sum(&[
                    body.capacity(),
                    expansion.capacity(),
                    all.capacity()
                        .checked_mul(std::mem::size_of::<ScoredNominee>())
                        .ok_or("SQ4 score capacity overflow")?,
                    fetched
                        .capacity()
                        .checked_mul(8)
                        .ok_or("SQ4 fetched capacity overflow")?,
                    ordinals
                        .capacity()
                        .checked_mul(8)
                        .ok_or("SQ4 ordinal capacity overflow")?,
                    r.len()
                        .checked_mul(192)
                        .ok_or("SQ4 native scorer workspace overflow")?,
                ])?;
                require(
                    actual
                        <= n.checked_mul(12 + d.div_ceil(2) + 12 + d + 256)
                            .ok_or("SQ4 coexistence overflow")?,
                    "SQ4 actual coexistence before read/expansion",
                )?;
                range_peak = range_peak.max(body.capacity());
                expansion_peak = expansion_peak.max(expansion.capacity());
                score_peak = score_peak.max(actual);
                guard.tick((body.len() + r.len() * d) as u64)?;
                let file = if packed { &plane.sq4 } else { &plane.original };
                file.read_exact_at(&mut body, (r.start * width) as u64)?;
                for (group, chunk) in body.chunks(16 * width).enumerate() {
                    let index = r.start / 16 + group;
                    let expected = if packed {
                        &plane.sq4_hashes[index][..]
                    } else {
                        &plane.groups[index * 32..(index + 1) * 32]
                    };
                    require(
                        Sha256::digest(chunk).as_slice() == expected,
                        "SQ4 fetched group authentication",
                    )?;
                }
                verified += body.len();
                let expanded = if packed {
                    expand(&body, d, &mut expansion)?;
                    &expansion[..]
                } else {
                    &body[..]
                };
                for (slot, row) in expanded.chunks_exact(12 + d).enumerate() {
                    let id = i64::from_le_bytes(row[..8].try_into()?);
                    require(id == plane.ids[r.start + slot], "SQ4 fetched order binding")?;
                    fetched.push(id);
                }
                let local = score_nominees(
                    expanded,
                    Sq8Geometry {
                        rows: r.len(),
                        dimensions: d,
                    },
                    &ordinals,
                    &normalized,
                    &plane.manifest.low,
                    &plane.manifest.step,
                )
                .map_err(|e| format!("SQ4 native scorer: {e:?}"))?;
                for mut s in local {
                    s.ordinal += r.start;
                    all.push(s);
                }
                prior = r.end;
            }
            all.sort_unstable_by(|a, b| a.score.total_cmp(&b.score).then(a.id.cmp(&b.id)));
            let ranked = all
                .iter()
                .take(100)
                .map(|s| Score {
                    id: s.id,
                    ordinal: s.ordinal,
                    score_bits: s.score.to_bits(),
                })
                .collect();
            Ok(Scored {
                ranked,
                fetched_ids: fetched,
                range_reads: ranges.len(),
                verified_bytes: verified,
                range_payload_capacity_peak: range_peak,
                expanded_capacity_peak: expansion_peak,
                coexisting_score_allocation_bound: score_peak,
            })
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            ordinal: usize,
            query: Vec<f32>,
        }

        fn output_admission(config: &Config, plans: &[Plan], n: usize, d: usize) -> Result<usize> {
            let payloads = n
                .checked_mul(12 + d.div_ceil(2))
                .and_then(|v| v.checked_mul(2))
                .ok_or("SQ4 pair payload overflow")?;
            // Two MiB covers the payload seal, 128 bounded-path result pins in
            // the freeze, and terminal reserve. Immutable source inputs remain
            // separate; there is no second staging copy of either payload.
            let mut required = sum(&[payloads, config.prefix.bytes, 2 * 1024 * 1024, RESERVE])?;
            let digits = (n - 1).to_string().len() + 1; // decimal ID plus delimiter
            for plan in plans {
                let candidate = plan.row_ranges.iter().map(|r| r.len()).sum::<usize>();
                let original = plan
                    .original_row_ranges
                    .iter()
                    .map(|r| r.len())
                    .sum::<usize>();
                let ids = candidate
                    .checked_mul(2)
                    .and_then(|v| v.checked_add(original))
                    .and_then(|v| v.checked_add(plan.nominees.len()))
                    .and_then(|v| v.checked_mul(digits))
                    .ok_or("SQ4 output roster overflow")?;
                // 300 top-k score records, counters, schema and scalar fields
                // fit 32KiB. The exact serialized plan and all fetched IDs are
                // charged separately, before either payload output is created.
                required = sum(&[required, ids, serde_json::to_vec(plan)?.len(), 32768])?;
            }
            require(
                required <= config.caps.output_bytes,
                "SQ4 paired payload/results/closure output admission",
            )?;
            Ok(required)
        }

        fn run(
            config: &Config,
            sha: &str,
            output: &mut Outputs,
            protocol: &Protocol,
            guard: &mut Guard,
            strict: bool,
            progress: &mut Progress,
        ) -> Result<Value> {
            progress.stage = "validation";
            config_valid(config)?;
            digest(sha)?;
            if strict {
                require(
                    archived_truth(config.panels.each_ref().map(|p| &p.truth)),
                    "SQ4 archived truth descriptor binding",
                )?;
                require(crate::configured_cpu_threads() == 1, "SQ4 requires CPU1")?;
            }
            let n = protocol.rows;
            let d = protocol.dimensions;
            let modeled = allocation_model(config, n, d)?;
            require(
                config.prefix.bytes == protocol.prefix_bytes
                    && config.prefix.sha256 == protocol.prefix_sha
                    && config.original_seal.sha256 == protocol.seal_sha,
                "SQ4 original frozen authority",
            )?;
            output.cap = config.caps.output_bytes;
            let seal_body = read_pinned(&config.original_seal, 4096, false, guard)?;
            let seal: OriginalSeal = serde_json::from_slice(&seal_body)?;
            digest(&seal.config_sha256)?;
            digest(&seal.source_identity_sha256)?;
            require(
                seal.schema == "borsuk-fine-sq8-seal-v1"
                    && !seal.truth_opened
                    && seal.plans_per_panel == 64
                    && seal.prefix_bytes == config.prefix.bytes
                    && seal.prefix_sha256 == config.prefix.sha256,
                "SQ4 original seal binding",
            )?;
            for (old, panel) in seal.panels.iter().zip(&config.panels) {
                require(
                    old.dataset == panel.dataset
                        && same_artifact(&old.root, &panel.root)
                        && same_artifact(&old.requests, &panel.requests),
                    "SQ4 sealed root/request binding",
                )?;
            }
            let mut manifests = reserved(2)?;
            for panel in &config.panels {
                let manifest: Manifest =
                    serde_json::from_slice(&read_pinned(&panel.root, ROOT_CAP, false, guard)?)?;
                validate_manifest(&manifest, &panel.root)?;
                require(
                    manifest.identity.rows == n && manifest.identity.dimensions == d,
                    "SQ4 paired root geometry before bodies",
                )?;
                manifests.push(manifest);
            }
            // Original nominations are already frozen. Their covers permit an
            // exact paired scratch/output bound before either payload is written.
            // Actual query vectors stay unopened until both payloads are sealed.
            let prefix = read_pinned(&config.prefix, 2 * 1024 * 1024, true, guard)?;
            let replay_config = super::Config {
                schema: super::CONFIG_SCHEMA.into(),
                original_seal: config.original_seal.clone(),
                prefix: config.prefix.clone(),
                caps: config.caps.clone(),
                panels: std::array::from_fn(|i| super::Panel {
                    dataset: config.panels[i].dataset.clone(),
                    root: config.panels[i].root.clone(),
                    graph: manifests[i].graph.clone(),
                    identity: manifests[i].identity.clone(),
                }),
            };
            let maps = [
                (0..n.div_ceil(16) as u32).collect(),
                (0..n.div_ceil(16) as u32).collect(),
            ];
            // Reuse the exact strict historical parser and every original cover/
            // generation/query/nominee validation, with an identity permutation.
            super::replay(&prefix, &replay_config, &maps, guard)?;
            let mut plans = reserved(128)?;
            for line in std::str::from_utf8(&prefix)?.lines().skip(2) {
                let FrozenEvent::Plan { plan: original, .. } = serde_json::from_str(line)? else {
                    return Err("SQ4 plan event".into());
                };
                plans.push(plan(original, n, d)?);
            }
            require(plans.len() == 128, "SQ4 all128 plans")?;
            let modeled_output = output_admission(config, &plans, n, d)?;
            let immutable_source_bytes = manifests.iter().try_fold(0usize, |total, m| {
                sum(&[
                    total,
                    m.records.bytes,
                    m.groups.bytes,
                    m.order.bytes,
                    m.graph.bytes,
                    m.pq.bytes,
                ])
            })?;
            drop(manifests);
            progress.stage = "transcode";
            let mut planes = reserved(2)?;
            for (i, panel) in config.panels.iter().enumerate() {
                planes.push(Plane::transcode(
                    &panel.root,
                    output,
                    &format!("sq4-{i}.bin"),
                    n,
                    d,
                    guard,
                )?);
            }
            let payloads=planes.iter().map(|p|json!({"payload":p.payload,"original_root":p.root,
                "original_records":p.manifest.records,"original_router":p.manifest.identity,
                "codec":CODEC,"dimensions":d,"rows":n,"row_bytes":12+d.div_ceil(2),
                "group_rows":16,"low_bits":p.manifest.low.iter().map(|v|v.to_bits()).collect::<Vec<_>>(),
                "step_bits":p.manifest.step.iter().map(|v|v.to_bits()).collect::<Vec<_>>(),
                "group_hashes_sha256":hash(&p.sq4_hashes.iter().flatten().copied().collect::<Vec<_>>())})).collect::<Vec<_>>();
            let payload_seal = output.publish(
                "sq4-payloads.json",
                &serde_json::to_vec(&json!({
                "schema":"borsuk-fixed-sq4-payload-seal-v1","config_sha256":sha,"codec":CODEC,
                "source_identity_sha256":source_identity(),"original_seal":config.original_seal,
                "payloads":payloads,"queries_opened":false,"truth_opened":false}))?,
            )?;
            let prefix_pin = output.publish("sq4-prefix.jsonl", &prefix)?;
            progress.stage = "scoring";
            let mut results = reserved(128)?;
            let mut envelope = true;
            for (panel_index, panel) in config.panels.iter().enumerate() {
                let body = read_pinned(&panel.requests, REQUEST_CAP, false, guard)?;
                let mut count = 0;
                for (ordinal, line) in std::str::from_utf8(&body)?.lines().enumerate() {
                    require(
                        ordinal < 64 && line.len() <= ROOT_CAP,
                        "SQ4 request line cap",
                    )?;
                    let request: Request = serde_json::from_str(line)?;
                    let plan = &plans[panel_index * 64 + ordinal];
                    require(
                        request.ordinal == ordinal
                            && request.query.len() == d
                            && query_digest(&request.query) == plan.query_sha256,
                        "SQ4 original query binding",
                    )?;
                    let sq4 = score(
                        &planes[panel_index],
                        &plan.row_ranges,
                        &request.query,
                        true,
                        guard,
                    )?;
                    let reference = score(
                        &planes[panel_index],
                        &plan.row_ranges,
                        &request.query,
                        false,
                        guard,
                    )?;
                    let baseline = score(
                        &planes[panel_index],
                        &plan.original_row_ranges,
                        &request.query,
                        false,
                        guard,
                    )?;
                    require(
                        sq4.fetched_ids == reference.fetched_ids
                            && sq4.verified_bytes == plan.candidate_bytes
                            && sq4.range_reads <= 32,
                        "SQ4 same-population accounting",
                    )?;
                    envelope &= plan.envelope_fits;
                    progress.scored_queries += 1;
                    let result = json!({"schema":"borsuk-fixed-sq4-query-v1","dataset":panel.dataset,"ordinal":ordinal,
                        "plan":plan,"nominee_ids":plan.nominees.iter().map(|&i|planes[panel_index].ids[i]).collect::<Vec<_>>(),
                        "nominees_retained":true,"original_cover_contained":true,"sq4":sq4,"sq8_reference":reference,
                        "original256_baseline":baseline,"sq8_reference_serving_eligible":false,"truth_opened":false});
                    let encoded = serde_json::to_vec(&result)?;
                    require(encoded.len() <= RESULT_CAP, "SQ4 result serialization cap")?;
                    results.push(output.publish(
                        &format!("sq4-result-{}.json", panel_index * 64 + ordinal),
                        &encoded,
                    )?);
                    count += 1;
                }
                require(count == 64, "SQ4 complete consumed64 requests")?;
            }
            require(results.len() == 128, "SQ4 all128 results before truth")?;
            progress.stage = "freeze";
            let freeze=output.publish("sq4-freeze.json",&serde_json::to_vec(&json!({
                "schema":"borsuk-fixed-sq4-freeze-v1","config_sha256":sha,"source_identity_sha256":source_identity(),
                "payload_seal":payload_seal,"payloads":payloads,"original_seal":config.original_seal,
                "truth":config.panels.each_ref().map(|p|&p.truth),
                "nomination_prefix":prefix_pin,"results":results,"truth_opened":false}))?)?;
            progress.freeze = Some(freeze.clone());
            progress.stage = "pretruth authentication";
            #[cfg(test)]
            if let Some(panel) = APPEND_AFTER_TRANSCODE.with(|v| v.take()) {
                OpenOptions::new()
                    .append(true)
                    .open(&planes[panel].manifest.records.path)?
                    .write_all(b"growth")?;
            }
            #[cfg(test)]
            if TAMPER_CLOSURE.with(|v| v.replace(false)) {
                OpenOptions::new()
                    .write(true)
                    .open(&results[0].path)?
                    .write_all(b"!")?;
            }
            // Verify the entire published closure before acquiring even the
            // first truth descriptor. Later per-result reads authenticate again.
            for pin in [&freeze, &payload_seal, &prefix_pin]
                .into_iter()
                .chain(results.iter())
            {
                authenticate(pin, guard)?;
            }
            for plane in &planes {
                authenticate(&plane.payload, guard)?;
                exact_eof(&plane.original, plane.manifest.records.bytes)?;
            }
            require(
                sum(&[output.bytes, RESERVE])? <= modeled_output,
                "SQ4 actual durable output exceeds pre-admission",
            )?;
            guard.tick(0)?;
            let mut summaries = Vec::new();
            let mut quality = true;
            progress.stage = "truth";
            for (p, panel) in config.panels.iter().enumerate() {
                // Both payloads, all128 complete plans and ranked outputs are
                // immutable and durably sealed before acquiring any truth FD.
                let truth_file = descriptor(&panel.truth, false)?;
                progress.truth_opened = true;
                let truth = read_descriptor(&panel.truth, &truth_file, false, guard)?;
                let mut nominee_hits = Vec::new();
                let mut coverage_hits = Vec::new();
                let mut returned_hits = Vec::new();
                let mut reference_hits = Vec::new();
                let mut baseline_hits = Vec::new();
                for ordinal in 0..64 {
                    let gt = truth[ordinal * 400..(ordinal + 1) * 400]
                        .chunks_exact(4)
                        .map(|v| i64::from(u32::from_le_bytes(v.try_into().unwrap())))
                        .collect::<BTreeSet<_>>();
                    require(
                        gt.len() == 100 && gt.iter().all(|&id| id >= 0 && (id as usize) < n),
                        "SQ4 truth unique source IDs",
                    )?;
                    let result: Value = serde_json::from_slice(&read_pinned(
                        &results[p * 64 + ordinal],
                        RESULT_CAP,
                        false,
                        guard,
                    )?)?;
                    let hits = |key: &str| -> Result<usize> {
                        let scored: Scored = serde_json::from_value(result[key].clone())?;
                        Ok(scored.ranked.iter().filter(|s| gt.contains(&s.id)).count())
                    };
                    nominee_hits.push(
                        plans[p * 64 + ordinal]
                            .nominees
                            .iter()
                            .filter(|&&i| gt.contains(&planes[p].ids[i]))
                            .count(),
                    );
                    coverage_hits.push(
                        plans[p * 64 + ordinal]
                            .row_ranges
                            .iter()
                            .flat_map(|r| r.clone())
                            .filter(|&i| gt.contains(&planes[p].ids[i]))
                            .count(),
                    );
                    returned_hits.push(hits("sq4")?);
                    reference_hits.push(hits("sq8_reference")?);
                    baseline_hits.push(hits("original256_baseline")?);
                }
                let summary = |values: &[usize]| {
                    let mut sorted = values.to_vec();
                    sorted.sort_unstable();
                    json!({"mean_recall":sorted.iter().sum::<usize>() as f64/6400.,"p05_hits":sorted[3]})
                };
                let returned = summary(&returned_hits);
                quality &= returned_hits.iter().sum::<usize>() >= 6272
                    && returned["p05_hits"].as_u64().unwrap() >= 95;
                let loss = reference_hits
                    .iter()
                    .zip(&returned_hits)
                    .map(|(a, b)| *a as i64 - *b as i64)
                    .collect::<Vec<_>>();
                summaries.push(json!({"dataset":panel.dataset,"nominee_containment":summary(&nominee_hits),"fetched_coverage":summary(&coverage_hits),
                    "sq4_returned":returned,"same_population_sq8_returned":summary(&reference_hits),"original256_returned":summary(&baseline_hits),
                    "sq4_vs_same_population_sq8_loss_hits":loss,"mean_recall_loss":loss.iter().sum::<i64>() as f64/6400.}));
            }
            guard.tick(0)?;
            Ok(terminal(
                sha,
                if quality && envelope {
                    "SURVIVED_CONSUMED_PANELS"
                } else {
                    "REJECT"
                },
                true,
                json!({"freeze":freeze,"summaries":summaries,"all128_envelopes_fit":envelope,"modeled_peak_bytes":modeled,
                    "modeled_output_bytes":modeled_output,"durable_output_bytes_before_terminal":output.bytes,
                    "immutable_source_artifact_bytes":immutable_source_bytes,"pair_payload_bytes":2*n*(12+d.div_ceil(2)),
                    "rows":n,"dimensions":d,"truth":config.panels.each_ref().map(|p|&p.truth),
                    "frozen_original_authority":strict && n==100_000 && d==768 && config.prefix.bytes==PREFIX_BYTES
                        && config.prefix.sha256==PREFIX_SHA && config.original_seal.sha256==SEAL_SHA
                        && archived_truth(config.panels.each_ref().map(|p|&p.truth)),
                    "caps":config.caps,"operations":guard.operations,"whole_process_supervisor_required":true}),
            ))
        }

        /// Strict historical geometry, prefix and seal. Supervisor admission is
        /// external; successful exit alone never establishes a quality PASS.
        pub fn check_fine_sq4(config_path: &Path, config_sha: &str, path: &Path) -> Result<()> {
            let mut output = Outputs::create(path)?;
            let mut progress = Progress {
                stage: "config",
                ..Progress::default()
            };
            let result = (|| {
                digest(config_sha)?;
                let file = secure_open(config_path, rustix::fs::OFlags::RDONLY)?;
                let meta = file.metadata()?;
                require(
                    meta.is_file() && meta.len() <= ROOT_CAP as u64,
                    "SQ4 config regular/cap",
                )?;
                let mut body = filled(meta.len() as usize, 0u8)?;
                (&file).take(ROOT_CAP as u64).read_exact(&mut body)?;
                exact_eof(&file, body.len())?;
                require(hash(&body) == config_sha, "SQ4 config SHA256")?;
                let config: Config = serde_json::from_slice(&body)?;
                config_valid(&config)?;
                let mut guard = Guard::new(&config.caps);
                let result = run(
                    &config,
                    config_sha,
                    &mut output,
                    &Protocol::frozen(),
                    &mut guard,
                    true,
                    &mut progress,
                );
                progress.operations = guard.operations;
                let report = result?;
                progress.stage = "terminal";
                require(
                    serde_json::to_vec(&report)?.len() <= RESERVE,
                    "SQ4 fixed terminal reserve",
                )?;
                output.finish(&report)?;
                guard.tick(0)
            })();
            if let Err(error) = &result {
                invalidate(&mut output, config_sha, error, &progress);
            }
            result
        }
        /// Explicit caller-authenticated tiny/source fixture API. The command
        /// above alone enforces the frozen FIRST100k/D768 historical authority.
        pub fn diagnose(config: &Config, sha: &str, path: &Path) -> Result<()> {
            let mut output = Outputs::create(path)?;
            let mut guard = Guard::new(&config.caps);
            let mut progress = Progress {
                stage: "fixture metadata",
                ..Progress::default()
            };
            let result = (|| {
                config_valid(config)?;
                let manifest: Manifest = serde_json::from_slice(&read_pinned(
                    &config.panels[0].root,
                    ROOT_CAP,
                    false,
                    &mut guard,
                )?)?;
                let protocol = Protocol {
                    rows: manifest.identity.rows,
                    dimensions: manifest.identity.dimensions,
                    prefix_bytes: config.prefix.bytes,
                    prefix_sha: config.prefix.sha256.clone(),
                    seal_sha: config.original_seal.sha256.clone(),
                };
                let result = run(
                    config,
                    sha,
                    &mut output,
                    &protocol,
                    &mut guard,
                    false,
                    &mut progress,
                );
                progress.operations = guard.operations;
                let report = result?;
                progress.stage = "terminal";
                require(
                    serde_json::to_vec(&report)?.len() <= RESERVE,
                    "SQ4 fixed terminal reserve",
                )?;
                output.finish(&report)?;
                guard.tick(0)
            })();
            progress.operations = guard.operations;
            if let Err(error) = &result {
                invalidate(&mut output, sha, error, &progress);
            }
            result
        }
        #[cfg(test)]
        mod tests {
            use super::*;
            use std::fs;

            #[test]
            fn fine_sq4_all_codes_numeric_oracle_odd_tail_nonunit_ties() {
                let low = [-2.25_f32, 0.125, -0.75];
                let step = [0.03125_f32, 0.25, 0.0625];
                let mut packed = Vec::new();
                let mut vectors = Vec::new();
                for code in 0..=255u8 {
                    let codes = [code, 255 - code, code];
                    let mut row = (code as i64).to_le_bytes().to_vec();
                    row.extend_from_slice(&1234_f32.to_le_bytes());
                    row.extend(codes);
                    let mut encoded = [0u8; 14];
                    encode(&row, &low, &step, &mut encoded).unwrap();
                    // Independent nearest-center search over all sixteen levels.
                    let levels = codes.map(|c| {
                        (0..16u8)
                            .min_by_key(|&n| i32::from(c).abs_diff(i32::from(n) * 17))
                            .unwrap()
                    });
                    assert_eq!(encoded[12], levels[0] + 16 * levels[1]);
                    assert_eq!(encoded[13], levels[2]);
                    let decoded = std::array::from_fn::<_, 3, _>(|i| {
                        low[i] + step[i] * (levels[i] as f32 * 17.)
                    });
                    let mut norm = 0_f32;
                    for value in decoded {
                        norm += value * value;
                    }
                    assert_eq!(&encoded[8..12], &norm.to_le_bytes());
                    assert_ne!(&encoded[8..12], &1234_f32.to_le_bytes());
                    vectors.push(decoded);
                    packed.extend(encoded);
                }
                let mut expanded = vec![0; 256 * 15];
                expand(&packed, 3, &mut expanded).unwrap();
                for (row, vector) in expanded.chunks_exact(15).zip(&vectors) {
                    for axis in 0..3 {
                        assert_eq!(
                            low[axis] + step[axis] * f32::from(row[12 + axis]),
                            vector[axis]
                        );
                    }
                }
                let query = [3.125_f32, -2., 0.875];
                let actual = score_nominees(
                    &expanded,
                    Sq8Geometry {
                        rows: 256,
                        dimensions: 3,
                    },
                    &(0..256).collect::<Vec<_>>(),
                    &query,
                    &low,
                    &step,
                )
                .unwrap();
                for (score, v) in actual.iter().zip(&vectors) {
                    let oracle = (0..3)
                        .map(|i| (f64::from(v[i]) - f64::from(query[i])).powi(2))
                        .sum::<f64>();
                    assert!((f64::from(score.score) - oracle).abs() < 0.002);
                    let bytes = &packed[score.ordinal * 14..(score.ordinal + 1) * 14];
                    let mut norm = 0_f32;
                    let mut shift = 0_f32;
                    let mut qnorm = 0_f32;
                    let mut inner = 0_f32;
                    for axis in 0..3 {
                        let nibble = if axis % 2 == 0 {
                            bytes[12 + axis / 2] & 15
                        } else {
                            bytes[12 + axis / 2] >> 4
                        };
                        let code = f32::from(nibble) * 17.;
                        let value = low[axis] + code * step[axis];
                        norm += value * value;
                        shift += query[axis] * low[axis];
                        qnorm += query[axis] * query[axis];
                        let weight = query[axis] * step[axis];
                        inner += code * weight;
                    }
                    shift -= qnorm / 2.;
                    assert_eq!(
                        score.score.to_bits(),
                        (norm - 2. * (inner + shift)).to_bits()
                    );
                }
                // A 17-row range has a short final group. Stable ID tie order is
                // checked independently of the original reversed physical order.
                let mut tied = Vec::new();
                for id in (0..17_i64).rev() {
                    tied.extend(id.to_le_bytes());
                    tied.extend(0_f32.to_le_bytes());
                    tied.extend([0, 0, 0]);
                }
                let mut scores = score_nominees(
                    &tied,
                    Sq8Geometry {
                        rows: 17,
                        dimensions: 3,
                    },
                    &(0..17).collect::<Vec<_>>(),
                    &query,
                    &[0.; 3],
                    &[1.; 3],
                )
                .unwrap();
                scores.sort_by(|a, b| a.score.total_cmp(&b.score).then(a.id.cmp(&b.id)));
                assert_eq!(
                    scores.iter().map(|s| s.id).collect::<Vec<_>>(),
                    (0..17).collect::<Vec<_>>()
                );
                packed[13] |= 0xf0;
                assert!(expand(&packed, 3, &mut expanded).is_err());

                // Exercise the complete wrapper. Expected decoding, independent
                // cosine normalization and sequential f32 arithmetic use no
                // production codec, normalization helper or scoring call.
                let temp = tempfile::tempdir().unwrap();
                let root = root(&temp.path().join("source"));
                let caps = Caps {
                    memory_bytes: 256 * 1024 * 1024,
                    output_bytes: 128 * 1024 * 1024,
                    deadline_seconds: 600,
                    operations: 20_000_000_000,
                    cpu_threads: 1,
                    swap_bytes: 0,
                };
                let mut guard = Guard::new(&caps);
                let mut output = Outputs::create(&temp.path().join("oracle.json")).unwrap();
                let plane =
                    Plane::transcode(&root, &mut output, "payload", 135, 3, &mut guard).unwrap();
                let bytes = fs::read(&plane.payload.path).unwrap();
                // A non-dyadic witness rejects both wider reconstruction and
                // wider norm accumulation, even when cast back to f32.
                let witness_low = [0.1_f32, 0.2, 0.3];
                let witness_step = [0.01_f32, 0.02, 0.03];
                let witness_codes = [17_f32, 34., 51.];
                let mut sequential = 0_f32;
                let mut wider = 0_f64;
                let mut wider_sum = 0_f64;
                for axis in 0..3 {
                    let decoded = witness_low[axis] + witness_codes[axis] * witness_step[axis];
                    sequential += decoded * decoded;
                    wider_sum += f64::from(decoded) * f64::from(decoded);
                    let decoded = f64::from(witness_low[axis])
                        + f64::from(witness_codes[axis]) * f64::from(witness_step[axis]);
                    wider += decoded * decoded;
                }
                assert_eq!(sequential.to_bits(), 0x40864744);
                assert_eq!((wider as f32).to_bits(), 0x40864745);
                assert_eq!((wider_sum as f32).to_bits(), 0x40864745);
                assert_eq!(
                    u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
                    0x40864744
                );
                assert_eq!(&bytes[12..14], &[0x21, 0x03]);
                for query in [[3.125_f32, -2., 0.875], [-0., 4., 0.], [1., 0., 0.]] {
                    let mut squared = 0_f64;
                    for x in query {
                        squared += f64::from(x) * f64::from(x);
                    }
                    let q = query.map(|x| {
                        if (squared - 1.).abs() <= 1e-6 {
                            x
                        } else {
                            (f64::from(x) / squared.sqrt()) as f32
                        }
                    });
                    let mut shift = 0_f32;
                    let mut qnorm = 0_f32;
                    let mut weights = [0_f32; 3];
                    for axis in 0..3 {
                        shift += q[axis] * plane.manifest.low[axis];
                        qnorm += q[axis] * q[axis];
                        weights[axis] = q[axis] * plane.manifest.step[axis];
                    }
                    shift -= qnorm / 2.;
                    let mut expected = Vec::new();
                    for (ordinal, row) in bytes.chunks_exact(14).enumerate() {
                        let id = i64::from_le_bytes(row[..8].try_into().unwrap());
                        let mut norm = 0_f32;
                        let mut inner = 0_f32;
                        for axis in 0..3 {
                            let nibble = if axis % 2 == 0 {
                                row[12 + axis / 2] & 15
                            } else {
                                row[12 + axis / 2] >> 4
                            };
                            let code = f32::from(nibble) * 17.;
                            let decoded =
                                plane.manifest.low[axis] + code * plane.manifest.step[axis];
                            norm += decoded * decoded;
                            inner += code * weights[axis];
                        }
                        assert_eq!(
                            norm.to_bits(),
                            f32::from_le_bytes(row[8..12].try_into().unwrap()).to_bits()
                        );
                        expected.push((norm - 2. * (inner + shift), id, ordinal));
                    }
                    expected.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                    let actual = score(
                        &plane,
                        std::slice::from_ref(&(0..135)),
                        &query,
                        true,
                        &mut guard,
                    )
                    .unwrap();
                    assert!(actual.ranked.iter().any(|score| score.ordinal == 0));
                    for (actual, expected) in actual.ranked.iter().zip(expected.iter().take(100)) {
                        assert_eq!(
                            (actual.score_bits, actual.id, actual.ordinal),
                            (expected.0.to_bits(), expected.1, expected.2)
                        );
                    }
                }
                let mut original = 7_i64.to_le_bytes().to_vec();
                original.extend(3_f32.to_le_bytes());
                original.extend([1, 1, 1]);
                let mut zero = [0u8; 14];
                encode(&original, &[-0., 0., 0.], &[1.; 3], &mut zero).unwrap();
                assert_eq!(&zero[8..12], &0_f32.to_le_bytes());
                assert_eq!(&zero[12..], &[0, 0]);
                let mut expanded = [0u8; 15];
                expand(&zero, 3, &mut expanded).unwrap();
                let retained = score_nominees(
                    &expanded,
                    Sq8Geometry {
                        rows: 1,
                        dimensions: 3,
                    },
                    &[0],
                    &[-0., 1., 0.],
                    &[-0., 0., 0.],
                    &[1.; 3],
                )
                .unwrap();
                assert_eq!(retained[0].id, 7);
                assert_eq!(retained[0].score.to_bits(), 1_f32.to_bits());
                assert!(encode(&original, &[f32::NAN, 0., 0.], &[1.; 3], &mut zero).is_err());
                assert!(encode(&original, &[0.; 3], &[0., 1., 1.], &mut zero).is_err());
            }

            fn frozen(n: usize, d: usize, nominees: Vec<usize>) -> FrozenPlan {
                let pages = nominees.iter().map(|i| i / 16).collect();
                let (ranges, bytes) = cover_pages(&pages, n, d + 12, 16, 256).unwrap();
                let coarse = nominees.iter().map(|i| i / 256).collect();
                let (old, old_bytes) = cover_pages(&coarse, n, d + 12, 256, 256).unwrap();
                FrozenPlan {
                    root_sha256: hash(b"root"),
                    query_sha256: hash(b"query"),
                    revision: 0,
                    mutation_sha256: String::new(),
                    actual_shortlist_rows: nominees.len(),
                    nominees,
                    ranges,
                    planned_bytes: bytes,
                    feasible: bytes <= MAX_BYTES,
                    exhausted: false,
                    converged: true,
                    evaluations: 1024,
                    base_visits: 100,
                    old_page_gets: old.len(),
                    old_page_bytes: old_bytes,
                }
            }
            #[test]
            fn fine_sq4_exact_cover_superset_and_binding() {
                // Brute-force minimum byte covers, including a short tail.
                for wanted in 1usize..64 {
                    let selected = (0..6)
                        .filter(|i| wanted & (1 << i) != 0)
                        .collect::<BTreeSet<_>>();
                    for cap in 1..=3 {
                        let (_, bytes) = cover_pages(&selected, 83, 14, 16, cap).unwrap();
                        let expected = (0usize..64)
                            .filter(|m| m & wanted == wanted)
                            .filter(|m| {
                                (0..6)
                                    .filter(|i| {
                                        m & (1 << i) != 0 && (*i == 0 || m & (1 << (i - 1)) == 0)
                                    })
                                    .count()
                                    <= cap
                            })
                            .map(|m| {
                                (0..6)
                                    .filter(|i| m & (1 << i) != 0)
                                    .map(|i| (83usize - 16 * i).min(16) * 14)
                                    .sum::<usize>()
                            })
                            .min()
                            .unwrap();
                        assert_eq!(bytes, expected);
                    }
                }
                let p = plan(
                    frozen(100_000, 768, (0..600).map(|i| i * 160).collect()),
                    100_000,
                    768,
                )
                .unwrap();
                assert_eq!(p.row_ranges.len(), 32);
                assert!(p.original_row_ranges.iter().all(|r| {
                    p.row_ranges
                        .iter()
                        .any(|c| c.start <= r.start && c.end >= r.end)
                }));
                assert!(
                    p.nominees
                        .iter()
                        .all(|i| p.row_ranges.iter().any(|r| r.contains(i)))
                );
                assert!(!p.envelope_fits);
                let mut invalid = frozen(135, 3, vec![0, 134]);
                invalid.ranges = std::iter::once(0..135 * 15).collect();
                assert!(plan(invalid, 135, 3).is_err());
                let report = terminal(
                    &hash(b"config"),
                    "SURVIVED_CONSUMED_PANELS",
                    true,
                    json!({"rows":100_000,"dimensions":768,"frozen_original_authority":true,
                        "truth":TRUTH_SHA256.map(|sha|Artifact {path:"/relocated/truth".into(),bytes:25_600,sha256:sha.into()})}),
                );
                let body = serde_json::to_vec(&report).unwrap();
                let mut receipt = SupervisorReceipt {
                    run_id: "original".into(),
                    config_sha256: hash(b"config"),
                    report_sha256: hash(&body),
                    process_exit_code: 0,
                    resource_limits_observed: true,
                    drain_complete: true,
                    cleanup_complete: true,
                };
                admit_survival(&body, "original", &receipt).unwrap();
                for panel in 0..2 {
                    for field in ["sha256", "bytes"] {
                        let mut bad = report.clone();
                        bad["details"]["truth"][panel][field] = if field == "sha256" {
                            json!("0".repeat(64))
                        } else {
                            json!(25_599)
                        };
                        let bad = serde_json::to_vec(&bad).unwrap();
                        let mut bound = receipt.clone();
                        bound.report_sha256 = hash(&bad);
                        assert!(admit_survival(&bad, "original", &bound).is_err());
                    }
                }
                receipt.process_exit_code = 2;
                assert!(admit_survival(&body, "original", &receipt).is_err());
                receipt.process_exit_code = 0;
                receipt.drain_complete = false;
                assert!(admit_survival(&body, "original", &receipt).is_err());
                receipt.drain_complete = true;
                receipt.cleanup_complete = false;
                assert!(admit_survival(&body, "original", &receipt).is_err());
                receipt.cleanup_complete = true;
                receipt.resource_limits_observed = false;
                assert!(admit_survival(&body, "original", &receipt).is_err());
            }
            fn root(dir: &Path) -> Artifact {
                fs::create_dir(dir).unwrap();
                let n = 135usize;
                let d = 3usize;
                let mut records = Vec::new();
                for id in (0..n as i64).rev() {
                    records.extend(id.to_le_bytes());
                    records.extend(999_f32.to_le_bytes());
                    records.extend(if id == 134 {
                        [17, 34, 51]
                    } else {
                        [id as u8, 17, 255]
                    });
                }
                let rec = write_body(dir, dir, "records.bin", &records).unwrap();
                let groups = write_body(
                    dir,
                    dir,
                    "groups.bin",
                    &records
                        .chunks(16 * (d + 12))
                        .flat_map(|b| Sha256::digest(b).to_vec())
                        .collect::<Vec<_>>(),
                )
                .unwrap();
                let order = write_body(
                    dir,
                    dir,
                    "order.bin",
                    &(0..n as u64)
                        .rev()
                        .flat_map(u64::to_le_bytes)
                        .collect::<Vec<_>>(),
                )
                .unwrap();
                let pq =
                    write_body(dir, dir, "pq.bin", &vec![0; 24 + 64 * 256 * 4 + n * 64]).unwrap();
                let graph =
                    write_body(dir, dir, "graph.bin", b"source-only authentication fixture")
                        .unwrap();
                let primary = Artifact {
                    path: dir.join("absent-primary"),
                    bytes: 1,
                    sha256: hash(b"source"),
                };
                let original = BuildConfig {
                    schema: crate::hierarchical_semantic_cells::BUILD_SCHEMA.into(),
                    generation: primary.clone(),
                    plane: primary.clone(),
                    canonical: primary.clone(),
                    order: primary.clone(),
                    records: primary.clone(),
                    mean: primary.clone(),
                    sq8: primary.clone(),
                    cell_rows: 16,
                    sample_rows: 16,
                    max_depth: 24,
                    max_build_payload_bytes: 1,
                    max_output_bytes: 1,
                };
                let low = vec![0.1, 0.2, 0.3];
                let step = vec![0.01, 0.02, 0.03];
                let manifest = Manifest {
                    schema: SCHEMA.into(),
                    identity: PqGraphIdentity {
                        generation: 1,
                        rows: n,
                        dimensions: d,
                        source: digest(&primary.sha256).unwrap(),
                        layout: layout_identity(&primary, &rec, &order, &low, &step).unwrap(),
                        pq: digest(&pq.sha256).unwrap(),
                    },
                    primary_root: primary,
                    original,
                    low,
                    step,
                    pq,
                    graph,
                    records: rec,
                    groups,
                    order,
                    build: FineBuildReceipt {
                        modeled_build_payload_bytes: 1,
                        modeled_output_and_staging_bytes: 1,
                        actual_graph_capacity_bytes: 1,
                        construction_graph_capacity_bytes: 1,
                        actual_pq_capacity_bytes: 1,
                        source_rows: n,
                        sq8_body_bytes: records.len(),
                    },
                };
                write_body(
                    dir,
                    dir,
                    "manifest.json",
                    &serde_json::to_vec(&manifest).unwrap(),
                )
                .unwrap()
            }
            #[test]
            fn fine_sq4_auth_fifo_corruption_caps_and_durability() {
                let tmp = tempfile::tempdir().unwrap();
                let root = root(&tmp.path().join("source"));
                let caps = Caps {
                    memory_bytes: 256 * 1024 * 1024,
                    output_bytes: 128 * 1024 * 1024,
                    deadline_seconds: 600,
                    operations: 20_000_000_000,
                    cpu_threads: 1,
                    swap_bytes: 0,
                };
                let mut output = Outputs::create(&tmp.path().join("out.json")).unwrap();
                output.cap = caps.output_bytes;
                let mut guard = Guard::new(&caps);
                let plane =
                    Plane::transcode(&root, &mut output, "sq4.bin", 135, 3, &mut guard).unwrap();
                assert_eq!(plane.payload.bytes, 135 * 14);
                assert_eq!(plane.sq4_hashes.len(), 9);
                let scored = score(
                    &plane,
                    std::slice::from_ref(&(0..135)),
                    &[3.125, -2., 0.875],
                    true,
                    &mut guard,
                )
                .unwrap();
                assert_eq!(scored.verified_bytes, 135 * 14);
                assert_eq!(scored.fetched_ids.len(), 135);
                let payload = fs::read(&plane.payload.path).unwrap();
                let mut corrupt = payload.clone();
                corrupt[16 * 14 + 12] ^= 1;
                fs::write(&plane.payload.path, &corrupt).unwrap();
                assert!(
                    score(
                        &plane,
                        std::slice::from_ref(&(0..135)),
                        &[3.125, -2., 0.875],
                        true,
                        &mut guard
                    )
                    .is_err()
                );
                fs::write(&plane.payload.path, &payload).unwrap();
                let original = fs::read(&plane.manifest.records.path).unwrap();
                let mut bad = original.clone();
                bad[20] ^= 1;
                fs::write(&plane.manifest.records.path, &bad).unwrap();
                assert!(
                    score(
                        &plane,
                        std::slice::from_ref(&(0..135)),
                        &[3.125, -2., 0.875],
                        false,
                        &mut guard
                    )
                    .is_err()
                );
                let mut corrupt_out = Outputs::create(&tmp.path().join("bad.json")).unwrap();
                assert!(
                    Plane::transcode(&root, &mut corrupt_out, "payload", 135, 3, &mut guard)
                        .is_err()
                );
                fs::write(&plane.manifest.records.path, &original).unwrap();
                APPEND_ON_OPEN
                    .with(|p| *p.borrow_mut() = Some(plane.manifest.records.path.clone()));
                let mut growth = Outputs::create(&tmp.path().join("growth.json")).unwrap();
                let error = Plane::transcode(&root, &mut growth, "payload", 135, 3, &mut guard)
                    .err()
                    .unwrap();
                assert!(error.to_string().contains("EOF/growth"));
                fs::write(&plane.manifest.records.path, &original).unwrap();
                let graph = fs::read(&plane.manifest.graph.path).unwrap();
                APPEND_ON_OPEN.with(|p| *p.borrow_mut() = Some(plane.manifest.graph.path.clone()));
                assert!(
                    authenticate(&plane.manifest.graph, &mut guard)
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("EOF/growth")
                );
                fs::write(&plane.manifest.graph.path, &graph).unwrap();
                APPEND_ON_OPEN.with(|p| *p.borrow_mut() = Some(plane.payload.path.clone()));
                assert!(
                    read_pinned(&plane.payload, RESULT_CAP, false, &mut guard)
                        .unwrap_err()
                        .to_string()
                        .contains("EOF/growth")
                );
                let before = guard.operations;
                assert_eq!(
                    read_pinned(&plane.payload, RESULT_CAP, true, &mut guard).unwrap(),
                    payload
                );
                assert_eq!(guard.operations - before, plane.payload.bytes as u64);
                fs::write(&plane.payload.path, &payload).unwrap();
                let mut cap = Outputs::create(&tmp.path().join("cap.json")).unwrap();
                cap.cap = RESERVE;
                assert!(Plane::transcode(&root, &mut cap, "payload", 135, 3, &mut guard).is_err());
                assert!(!tmp.path().join("cap.payload").exists());
                for at in [1, 2] {
                    let mut out =
                        Outputs::create(&tmp.path().join(format!("sync-{at}.json"))).unwrap();
                    out.fail_sync = Some(at);
                    let error = Plane::transcode(&root, &mut out, "payload", 135, 3, &mut guard)
                        .err()
                        .unwrap();
                    invalidate(&mut out, &hash(b"config"), &error, &Progress::default());
                    let report: Value =
                        serde_json::from_slice(&fs::read(&out.output).unwrap()).unwrap();
                    assert_eq!(report["status"], "INVALID");
                }
                let fifo = tmp.path().join("fifo");
                assert!(
                    std::process::Command::new("mkfifo")
                        .arg(&fifo)
                        .status()
                        .unwrap()
                        .success()
                );
                let fake = Artifact {
                    path: fifo,
                    bytes: 0,
                    sha256: hash(b""),
                };
                assert!(descriptor(&fake, false).is_err());
                let linked = tmp.path().join("linked");
                std::os::unix::fs::symlink(&plane.payload.path, &linked).unwrap();
                assert!(
                    descriptor(
                        &Artifact {
                            path: linked,
                            ..plane.payload.clone()
                        },
                        false
                    )
                    .is_err()
                );
                assert!(Outputs::create(&output.output).is_err());
            }

            #[test]
            fn fine_sq4_full_pipeline_failure_order_and_sync() {
                let tmp = tempfile::tempdir().unwrap();
                let roots = [root(&tmp.path().join("a")), root(&tmp.path().join("b"))];
                let request = write_body(
                    tmp.path(),
                    tmp.path(),
                    "requests",
                    (0..64)
                        .map(|ordinal| {
                            format!("{}\n", json!({"ordinal":ordinal,"query":[3.125,-2.,0.875]}))
                        })
                        .collect::<String>()
                        .as_bytes(),
                )
                .unwrap();
                let truth = write_body(
                    tmp.path(),
                    tmp.path(),
                    "truth",
                    &(0..64)
                        .flat_map(|_| (0..100u32).flat_map(u32::to_le_bytes))
                        .collect::<Vec<_>>(),
                )
                .unwrap();
                let mut prefix = String::new();
                for (i, root) in roots.iter().enumerate() {
                    let m: Manifest =
                        serde_json::from_slice(&fs::read(&root.path).unwrap()).unwrap();
                    prefix += &format!(
                        "{}\n",
                        json!({"phase":"startup","dataset":(["relaion","cohere"][i]),"root":root,
                        "resources":ResourceReceipt::default(),"build":m.build,"truth_opened":false,"wall_ns":0,"process_cpu_ns":0})
                    );
                }
                for (i, root) in roots.iter().enumerate() {
                    for ordinal in 0..64 {
                        let p = frozen(135, 3, vec![0, 134]);
                        prefix += &format!(
                            "{}\n",
                            json!({"phase":"fine_plan","dataset":(["relaion","cohere"][i]),"ordinal":ordinal,
                            "truth_opened":false,"wall_ns":0,"process_cpu_ns":0,"plan":{
                            "root_sha256":root.sha256,"query_sha256":query_digest(&[3.125,-2.,0.875]),"revision":0,"mutation_sha256":"",
                            "nominees":p.nominees,"ranges":p.ranges,"planned_bytes":p.planned_bytes,"feasible":p.feasible,
                            "exhausted":false,"converged":true,"actual_shortlist_rows":2,"evaluations":135,"base_visits":135,
                            "old_page_gets":p.old_page_gets,"old_page_bytes":p.old_page_bytes}})
                        );
                    }
                }
                let prefix =
                    write_body(tmp.path(), tmp.path(), "prefix", prefix.as_bytes()).unwrap();
                let seal=write_body(tmp.path(),tmp.path(),"seal",&serde_json::to_vec(&json!({
                    "schema":"borsuk-fine-sq8-seal-v1","config_sha256":hash(b"original config"),"source_identity_sha256":hash(b"original source"),
                    "prefix_bytes":prefix.bytes,"prefix_sha256":prefix.sha256,"plans_per_panel":64,"truth_opened":false,
                    "panels":roots.iter().enumerate().map(|(i,r)|json!({"dataset":(["relaion","cohere"][i]),"root":r,"requests":request})).collect::<Vec<_>>()
                })).unwrap()).unwrap();
                let config = Config {
                    schema: CONFIG_SCHEMA.into(),
                    source_identity_sha256: source_identity(),
                    panels: std::array::from_fn(|i| Panel {
                        dataset: ["relaion", "cohere"][i].into(),
                        root: roots[i].clone(),
                        requests: request.clone(),
                        truth: truth.clone(),
                        truth_width: 100,
                    }),
                    original_seal: seal.clone(),
                    prefix: prefix.clone(),
                    caps: Caps {
                        memory_bytes: 256 * 1024 * 1024,
                        output_bytes: 128 * 1024 * 1024,
                        deadline_seconds: 600,
                        operations: 20_000_000_000,
                        cpu_threads: 1,
                        swap_bytes: 0,
                    },
                };
                let protocol = Protocol {
                    rows: 135,
                    dimensions: 3,
                    prefix_bytes: prefix.bytes,
                    prefix_sha: prefix.sha256.clone(),
                    seal_sha: seal.sha256,
                };
                // Each publish performs file and directory syncs. Failure at the
                // all128 freeze (call265) must leave truth unopened; terminal
                // sync failure must replace the owned report with INVALID.
                for fail_at in [
                    None,
                    Some(0),
                    Some(1),
                    Some(2),
                    Some(5),
                    Some(265),
                    Some(267),
                ] {
                    super::super::OPENS.with(|v| v.borrow_mut().clear());
                    let path = tmp.path().join(format!("sync-{fail_at:?}.json"));
                    let mut out = Outputs::create(&path).unwrap();
                    out.fail_sync = fail_at.filter(|v| *v > 2);
                    TAMPER_CLOSURE.with(|v| v.set(fail_at == Some(0)));
                    APPEND_AFTER_TRANSCODE
                        .with(|v| v.set(fail_at.filter(|v| (1..=2).contains(v)).map(|v| v - 1)));
                    let mut guard = Guard::new(&config.caps);
                    let mut progress = Progress::default();
                    let result = run(
                        &config,
                        &hash(b"config"),
                        &mut out,
                        &protocol,
                        &mut guard,
                        false,
                        &mut progress,
                    );
                    progress.operations = guard.operations;
                    let result = result.and_then(|report| {
                        progress.stage = "terminal";
                        require(
                            serde_json::to_vec(&report)?.len() <= RESERVE,
                            "SQ4 terminal reserve",
                        )?;
                        out.finish(&report)?;
                        Ok(report)
                    });
                    if let Err(error) = &result {
                        invalidate(&mut out, &hash(b"config"), error, &progress);
                    }
                    let report: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                    let opened = super::super::OPENS.with(|v| v.borrow().clone());
                    if let Some(at) = fail_at {
                        assert!(result.is_err());
                        assert_eq!(report["status"], "INVALID");
                        if at <= 265 {
                            assert!(!opened.contains(&truth.path));
                            assert_eq!(report["details"]["truth_opened"], false);
                        }
                        if at == 267 {
                            assert_eq!(report["queries"], 128);
                            assert_eq!(report["details"]["scored_queries"], 128);
                            assert_eq!(report["details"]["truth_opened"], true);
                            assert_eq!(report["details"]["stage"], "terminal");
                            assert!(report["details"]["freeze"]["sha256"].is_string());
                            assert!(report["details"]["published_bytes"].as_u64().unwrap() > 0);
                            assert!(report["details"]["operations"].as_u64().unwrap() > 0);
                        }
                        if (1..=2).contains(&at) {
                            assert!(result.unwrap_err().to_string().contains("EOF/growth"));
                            assert_eq!(report["queries"], 128);
                            assert_eq!(report["details"]["stage"], "pretruth authentication");
                            let manifest: Manifest =
                                serde_json::from_slice(&fs::read(&roots[at - 1].path).unwrap())
                                    .unwrap();
                            OpenOptions::new()
                                .write(true)
                                .open(&manifest.records.path)
                                .unwrap()
                                .set_len(manifest.records.bytes as u64)
                                .unwrap();
                        }
                    } else {
                        assert!(result.is_ok());
                        assert_eq!(report["status"], "REJECT"); // Only23 incidental rows; no early resource/error exit.
                        assert_eq!(report["queries"], 128);
                        assert_eq!(report["details"]["frozen_original_authority"], false);
                        let query_open = opened.iter().position(|p| p == &request.path).unwrap();
                        for i in 0..2 {
                            let payload = path.with_extension(format!("sq4-{i}.bin"));
                            assert!(
                                opened.iter().position(|p| p == &payload).unwrap() < query_open
                            );
                        }
                        assert_eq!(opened.iter().filter(|p| *p == &truth.path).count(), 2);
                    }
                }
                // The strict mode must refuse an alternate same-size truth
                // descriptor before acquiring any data FD, including root/seal.
                let mut archived = config.clone();
                for (panel, sha) in archived.panels.iter_mut().zip(TRUTH_SHA256) {
                    panel.truth.sha256 = sha.into();
                    panel.truth.path = tmp.path().join(format!("relocated-{}", panel.dataset));
                }
                assert!(archived_truth(archived.panels.each_ref().map(|p| &p.truth)));
                for panel in 0..2 {
                    let mut bad = archived.clone();
                    bad.panels[panel].truth.sha256 = "0".repeat(64);
                    let mut out =
                        Outputs::create(&tmp.path().join(format!("truth-binding-{panel}.json")))
                            .unwrap();
                    super::super::OPENS.with(|v| v.borrow_mut().clear());
                    let error = run(
                        &bad,
                        &hash(b"config"),
                        &mut out,
                        &Protocol::frozen(),
                        &mut Guard::new(&bad.caps),
                        true,
                        &mut Progress::default(),
                    )
                    .unwrap_err();
                    assert!(
                        error
                            .to_string()
                            .contains("archived truth descriptor binding")
                    );
                    assert!(super::super::OPENS.with(|v| v.borrow().is_empty()));
                }
            }
        }

        /// Requires an independently authenticated original supervisor receipt.
        /// The caller owns receipt provenance, process drain and cleanup checks.
        pub fn admit_survival(
            body: &[u8],
            run_id: &str,
            receipt: &SupervisorReceipt,
        ) -> Result<()> {
            require(body.len() <= RESERVE, "SQ4 terminal cap")?;
            let report: Value = serde_json::from_slice(body)?;
            let truth: [Artifact; 2] = serde_json::from_value(report["details"]["truth"].clone())?;
            digest(&receipt.config_sha256)?;
            digest(&receipt.report_sha256)?;
            require(
                !run_id.is_empty()
                    && receipt.run_id == run_id
                    && receipt.report_sha256 == hash(body)
                    && report["config_sha256"] == receipt.config_sha256
                    && receipt.process_exit_code == 0
                    && receipt.resource_limits_observed
                    && receipt.drain_complete
                    && receipt.cleanup_complete
                    && report["schema"] == REPORT_SCHEMA
                    && report["codec"] == CODEC
                    && report["source_identity_sha256"] == source_identity()
                    && report["details"]["rows"] == 100_000
                    && report["details"]["dimensions"] == 768
                    && report["details"]["frozen_original_authority"] == true
                    && archived_truth(truth.each_ref())
                    && report["queries"] == 128
                    && report["status"] == "SURVIVED_CONSUMED_PANELS"
                    && report["complete"] == true
                    && report["standalone_authority"] == false
                    && report["quality_or_performance_claim"] == false
                    && report["requires_matching_supervisor_exit_receipt"] == true,
                "SQ4 survival requires original exit-zero/resource/drain/cleanup",
            )
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::{fs, os::unix::fs::symlink};

        fn caps() -> Caps {
            Caps {
                memory_bytes: MEMORY_CAP,
                output_bytes: OUTPUT_CAP,
                deadline_seconds: 300,
                operations: OPERATIONS,
                cpu_threads: 1,
                swap_bytes: 0,
            }
        }
        fn artifact(path: &Path, body: &[u8]) -> Artifact {
            fs::write(path, body).unwrap();
            Artifact {
                path: path.into(),
                bytes: body.len(),
                sha256: hash(body),
            }
        }
        fn encode(identity: &PqGraphIdentity, towers: &[Vec<Vec<u32>>]) -> Vec<u8> {
            let mut out = b"BORSVG02".to_vec();
            for word in [
                identity.generation,
                identity.rows as u64,
                identity.dimensions as u64,
            ] {
                out.extend(word.to_le_bytes());
            }
            for hash in [identity.source, identity.layout, identity.pq] {
                out.extend(hash);
            }
            out.extend(0u32.to_le_bytes());
            for tower in towers {
                out.push(tower.len() as u8);
                for layer in tower {
                    out.extend((layer.len() as u16).to_le_bytes());
                    for node in layer {
                        out.extend(node.to_le_bytes());
                    }
                }
            }
            out
        }
        fn identity(rows: usize) -> PqGraphIdentity {
            PqGraphIdentity {
                generation: 1,
                rows,
                dimensions: 2,
                source: [1; 32],
                layout: [2; 32],
                pq: [3; 32],
            }
        }
        fn ring(rows: usize) -> Vec<Vec<Vec<u32>>> {
            (0..rows)
                .map(|i| vec![vec![((i + 1) % rows) as u32]])
                .collect()
        }
        fn decode(body: &[u8], identity: &PqGraphIdentity) -> Result<PqVectorGraph> {
            PqVectorGraph::decode_for_inspection(body, identity, &mut |_| Ok(()))
        }
        fn fixture(path: &Path) -> (Config, Protocol) {
            let n = 83usize;
            let panels = ["relaion", "cohere"].map(|dataset| {
                let dir = path.join(dataset);
                fs::create_dir(&dir).unwrap();
                let absent = |name: &str, bytes| Artifact {
                    path: dir.join(name),
                    bytes,
                    sha256: hash(name.as_bytes()),
                };
                let primary = absent("absent-primary", 1);
                let canonical = absent("absent-canonical", n * 16);
                let original = BuildConfig {
                    schema: crate::hierarchical_semantic_cells::BUILD_SCHEMA.into(),
                    generation: primary.clone(),
                    plane: primary.clone(),
                    canonical: canonical.clone(),
                    order: absent("absent-order", n * 8),
                    records: primary.clone(),
                    mean: primary.clone(),
                    sq8: absent("absent-sq8", n * 14),
                    cell_rows: 16,
                    sample_rows: 16,
                    max_depth: 24,
                    max_build_payload_bytes: 1,
                    max_output_bytes: 1,
                };
                let records = absent("records.bin", n * 14);
                let order = absent("order.bin", n * 8);
                let pq = absent("pq.bin", 24 + 64 * 256 * 4 + n * 64);
                let groups = absent("groups.bin", n.div_ceil(16) * 32);
                let id = PqGraphIdentity {
                    source: digest(&canonical.sha256).unwrap(),
                    layout: layout_identity(&primary, &records, &order, &[-0.0, 0.0], &[1., 1.])
                        .unwrap(),
                    pq: digest(&pq.sha256).unwrap(),
                    ..identity(n)
                };
                let graph = artifact(&dir.join("graph.bin"), &encode(&id, &ring(n)));
                let manifest = Manifest {
                    schema: SCHEMA.into(),
                    primary_root: primary,
                    original,
                    identity: id.clone(),
                    low: vec![-0.0, 0.0],
                    step: vec![1., 1.],
                    pq,
                    graph: graph.clone(),
                    records,
                    groups,
                    order,
                    build: FineBuildReceipt {
                        modeled_build_payload_bytes: 1,
                        modeled_output_and_staging_bytes: 1,
                        actual_graph_capacity_bytes: 1,
                        construction_graph_capacity_bytes: 1,
                        actual_pq_capacity_bytes: 1,
                        source_rows: n,
                        sq8_body_bytes: n * 14,
                    },
                };
                let root = artifact(
                    &dir.join("manifest.json"),
                    &serde_json::to_vec(&manifest).unwrap(),
                );
                Panel {
                    dataset: dataset.into(),
                    root,
                    graph,
                    identity: id,
                }
            });
            let mut prefix = Vec::new();
            for p in &panels {
                let root: Manifest =
                    serde_json::from_slice(&fs::read(&p.root.path).unwrap()).unwrap();
                prefix.extend(serde_json::to_vec(&json!({"phase":"startup","dataset":p.dataset,"root":p.root,
                    "resources":ResourceReceipt::default(),"build":root.build,"truth_opened":false,"wall_ns":1,"process_cpu_ns":1})).unwrap());
                prefix.push(b'\n');
            }
            for p in &panels {
                for ordinal in 0..64 {
                    let mut nominees = vec![ordinal % n, (ordinal * 17 + 3) % n];
                    nominees.sort_unstable();
                    nominees.dedup();
                    let (ranges, bytes) =
                        cover_pages(&nominees.iter().map(|n| n / 16).collect(), n, 14, 16, 256)
                            .unwrap();
                    let (coarse, coarse_bytes) =
                        cover_pages(&nominees.iter().map(|n| n / 256).collect(), n, 14, 256, 256)
                            .unwrap();
                    prefix.extend(serde_json::to_vec(&json!({"phase":"fine_plan","dataset":p.dataset,"ordinal":ordinal,
                    "truth_opened":false,"wall_ns":1,"process_cpu_ns":1,"plan":{
                    "root_sha256":p.root.sha256,"query_sha256":hash(&[ordinal as u8]),"revision":0,"mutation_sha256":"",
                    "nominees":nominees,"ranges":ranges,"planned_bytes":bytes,"feasible":true,"exhausted":false,"converged":true,
                    "actual_shortlist_rows":nominees.len(),"evaluations":n,"base_visits":n,"old_page_gets":coarse.len(),"old_page_bytes":coarse_bytes}})).unwrap());
                    prefix.push(b'\n');
                }
            }
            let prefix = artifact(&path.join("trace.jsonl"), &prefix);
            let seal=artifact(&path.join("original-seal.json"),&serde_json::to_vec(&json!({
                "schema":"borsuk-fine-sq8-seal-v1","config_sha256":hash(b"original config"),"source_identity_sha256":hash(b"original source"),
                "prefix_bytes":prefix.bytes,"prefix_sha256":prefix.sha256,"plans_per_panel":64,"truth_opened":false,
                "panels":panels.iter().map(|p|json!({"dataset":p.dataset,"root":p.root,
                    "requests":Artifact {path:path.join("forbidden-requests"),bytes:100,sha256:hash(b"requests")}})).collect::<Vec<_>>() })).unwrap());
            let protocol = Protocol {
                rows: n,
                dimensions: 2,
                prefix_bytes: prefix.bytes,
                prefix_sha: prefix.sha256.clone(),
                seal_sha: seal.sha256.clone(),
            };
            (
                Config {
                    schema: CONFIG_SCHEMA.into(),
                    panels,
                    original_seal: seal,
                    prefix,
                    caps: caps(),
                },
                protocol,
            )
        }
        fn run_fixture(
            config: &Config,
            protocol: &Protocol,
            path: &Path,
            fail_sync: Option<usize>,
            callback: &mut impl FnMut(&Artifact) -> Result<()>,
        ) -> Result<Value> {
            let mut outputs = Outputs::create(path)?;
            outputs.fail_sync = fail_sync;
            let mut guard = Guard::new(&config.caps);
            let result = run(
                config,
                &hash(b"config"),
                &mut outputs,
                protocol,
                &mut guard,
                callback,
            )
            .and_then(|report| {
                outputs.finish(&report)?;
                Ok(report)
            });
            if let Err(e) = &result {
                outputs.invalidate(&hash(b"config"), e);
            }
            result
        }
        fn from_arcs(groups: usize, arcs: &[(usize, usize)]) -> Adjacency {
            let mut offsets = vec![0; groups + 1];
            for &(a, b) in arcs {
                if a != b {
                    offsets[a + 1] += 1;
                    offsets[b + 1] += 1;
                }
            }
            for i in 1..=groups {
                offsets[i] += offsets[i - 1];
            }
            let mut targets = vec![0; offsets[groups]];
            let mut cursor = offsets.clone();
            for &(a, b) in arcs {
                if a != b {
                    targets[cursor[a]] = b as u32;
                    cursor[a] += 1;
                    targets[cursor[b]] = a as u32;
                    cursor[b] += 1;
                }
            }
            Adjacency { offsets, targets }
        }
        fn reference(groups: usize, arcs: &[(usize, usize)]) -> Vec<u32> {
            let mut remaining = (0..groups).collect::<BTreeSet<_>>();
            let mut order = Vec::new();
            while let Some(seed) = remaining.pop_first() {
                let mut pack = vec![seed];
                order.push(seed);
                while pack.len() < 42 && !remaining.is_empty() {
                    let mut ranked = remaining
                        .iter()
                        .map(|&g| {
                            let affinity = arcs
                                .iter()
                                .filter(|&&(a, b)| {
                                    (a == g && pack.contains(&b)) || (b == g && pack.contains(&a))
                                })
                                .count();
                            (std::cmp::Reverse(affinity), g)
                        })
                        .collect::<Vec<_>>();
                    ranked.sort_unstable();
                    let g = ranked[0].1;
                    remaining.remove(&g);
                    pack.push(g);
                    order.push(g);
                }
            }
            let mut map = vec![0; groups];
            for (new, old) in order.into_iter().enumerate() {
                map[old] = new as u32;
            }
            map
        }
        #[test]
        fn pack_affinity_matches_exhaustive_ties() {
            let possible = (0..4)
                .flat_map(|a| (0..4).filter(move |&b| a != b).map(move |b| (a, b)))
                .collect::<Vec<_>>();
            for mask in 0..1usize << possible.len() {
                let arcs = possible
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| mask & (1 << i) != 0)
                    .map(|(_, a)| *a)
                    .collect::<Vec<_>>();
                let actual = pack(&from_arcs(4, &arcs), 64, &mut Guard::new(&caps())).unwrap();
                assert_eq!(actual, reference(4, &arcs), "mask={mask}");
            }
            let arcs = (0..85)
                .flat_map(|a| [(a, (a + 7) % 85), (a, (a + 19) % 85)])
                .collect::<Vec<_>>();
            assert_eq!(
                pack(&from_arcs(85, &arcs), 85 * 16, &mut Guard::new(&caps())).unwrap(),
                reference(85, &arcs)
            );
            let summed = [((0, 1), 5), ((0, 2), 4), ((0, 3), 3), ((1, 3), 3)]
                .into_iter()
                .flat_map(|(arc, count)| std::iter::repeat_n(arc, count))
                .collect::<Vec<_>>();
            assert_eq!(
                pack(&from_arcs(4, &summed), 64, &mut Guard::new(&caps())).unwrap(),
                vec![0, 1, 3, 2]
            );
            // Upper layers deliberately prefer group2. Only last-layer ring
            // arcs count, including incoming arcs and the wraparound arc.
            let id = identity(48);
            let mut towers = ring(48);
            for (i, tower) in towers.iter_mut().enumerate() {
                tower.insert(0, vec![if i == 32 { 33 } else { 32 }]);
            }
            let graph = decode(&encode(&id, &towers), &id).unwrap();
            let (adj, _) = adjacency(&graph, 3, &caps(), &mut Guard::new(&caps()), 0).unwrap();
            assert_eq!(adj.targets.len(), 6);
            assert_eq!(
                pack(&adj, 48, &mut Guard::new(&caps())).unwrap(),
                vec![0, 1, 2]
            );
        }
        #[test]
        fn pack_tail_bijection_and_cover_match_brute() {
            for rows in [80usize, 81, 95] {
                let groups = rows.div_ceil(16);
                let arcs = [(0, 3), (3, 2), (2, 4), (1, 4), (0, 0)];
                let map = pack(&from_arcs(groups, &arcs), rows, &mut Guard::new(&caps())).unwrap();
                assert_eq!(
                    map.iter().copied().collect::<BTreeSet<_>>(),
                    (0..groups as u32).collect()
                );
                if rows % 16 != 0 {
                    assert_eq!(map[groups - 1], (groups - 1) as u32);
                }
                for wanted in 1usize..1usize << groups {
                    for gets in 1..=3 {
                        let nominees = (0..groups)
                            .filter(|g| wanted & (1 << g) != 0)
                            .map(|g| (g * 16 + 7).min(rows - 1))
                            .collect::<Vec<_>>();
                        let (mapped, ranges, bytes) =
                            mapped_cover(&nominees, &map, rows, 2, gets).unwrap();
                        let mask = mapped.iter().fold(0usize, |m, n| m | (1 << (n / 16)));
                        let brute = (0usize..1 << groups)
                            .filter(|m| m & mask == mask)
                            .filter(|m| {
                                (0..groups)
                                    .filter(|&g| {
                                        m & (1 << g) != 0 && (g == 0 || m & (1 << (g - 1)) == 0)
                                    })
                                    .count()
                                    <= gets
                            })
                            .map(|m| {
                                (0..groups)
                                    .filter(|g| m & (1 << g) != 0)
                                    .map(|g| 16.min(rows - g * 16) * 14)
                                    .sum::<usize>()
                            })
                            .min()
                            .unwrap();
                        assert_eq!(bytes, brute);
                        assert_eq!(bytes, ranges.iter().map(|r| r.len()).sum::<usize>());
                        assert_eq!(mapped.len(), nominees.len());
                    }
                }
            }
            let map = (0..6250).collect::<Vec<u32>>();
            let nominees = (0..500).map(|g| g * 3 * 16).collect::<Vec<_>>();
            assert_eq!(
                mapped_cover(&nominees, &map, 100_000, 768, 256).unwrap().2,
                12_330_240
            );
            assert_eq!(
                mapped_cover(&nominees, &map, 100_000, 768, 32).unwrap().2,
                17_921_280
            );
            assert!(17_921_280 > MAX_BYTES);
            // Exercise the actual replay call site: changing ONLY its range
            // cap from32 to256 must turn this complete rejection into survival
            // and fail these assertions. No 100k graph is built or opened.
            let temp = tempfile::tempdir().unwrap();
            let (mut config, _) = fixture(temp.path());
            let body = fs::read(&config.prefix.path).unwrap();
            let mut events = std::str::from_utf8(&body)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(events.len(), 130);
            for (p, panel) in config.panels.iter_mut().enumerate() {
                panel.identity.rows = 100_000;
                panel.identity.dimensions = 768;
                events[p]["build"]["source_rows"] = json!(100_000);
                events[p]["build"]["sq8_body_bytes"] = json!(78_000_000);
            }
            for (i, event) in events.iter_mut().skip(2).enumerate() {
                // A first-query failure must not skip the other127 queries;
                // the last query independently fails in the other panel.
                let selected = if i == 0 || i == 127 {
                    nominees.clone()
                } else {
                    vec![0, 16]
                };
                let (ranges, bytes) = cover_pages(
                    &selected.iter().map(|n| n / 16).collect(),
                    100_000,
                    780,
                    16,
                    256,
                )
                .unwrap();
                let (coarse, coarse_bytes) = cover_pages(
                    &selected.iter().map(|n| n / 256).collect(),
                    100_000,
                    780,
                    256,
                    256,
                )
                .unwrap();
                if i == 0 || i == 127 {
                    assert_eq!(bytes, 12_330_240);
                }
                let plan = &mut event["plan"];
                plan["nominees"] = json!(selected);
                plan["actual_shortlist_rows"] = json!(selected.len());
                plan["ranges"] = json!(ranges);
                plan["planned_bytes"] = json!(bytes);
                plan["old_page_gets"] = json!(coarse.len());
                plan["old_page_bytes"] = json!(coarse_bytes);
                plan["evaluations"] = json!(1024);
                plan["base_visits"] = json!(1024);
            }
            let prefix = events.iter().map(|v| format!("{v}\n")).collect::<String>();
            config.prefix = artifact(
                &temp.path().join("dispersed-prefix.jsonl"),
                prefix.as_bytes(),
            );
            let prefix =
                read_pinned(&config.prefix, PREFIX_BYTES, true, &mut Guard::new(&caps())).unwrap();
            let maps = [map.clone(), map];
            let replayed = replay(&prefix, &config, &maps, &mut Guard::new(&caps())).unwrap();
            assert_eq!(replayed["survived"], false); // completed REJECT, not INVALID
            assert_eq!(replayed["per_panel_passes"], json!([63, 63]));
            assert_eq!(
                replayed["per_panel_max_bytes"],
                json!([17_921_280, 17_921_280])
            );
            let results = replayed["queries"].as_array().unwrap();
            assert_eq!(results.len(), 128);
            assert_eq!(results.iter().filter(|q| q["fits"] == false).count(), 2);
            assert!(
                results
                    .iter()
                    .all(|q| q["ranges"].as_array().unwrap().len() <= 32)
            );
            for index in [0, 127] {
                assert_eq!(results[index]["planned_bytes"], 17_921_280);
                assert_eq!(results[index]["all_nominees_retained"], true);
                assert_eq!(results[index]["nominees"], 500);
            }
            // Keep only the first locality failure; all remaining127 must be
            // evaluated and fit, including the final CoHere query.
            events[129]["plan"] = events[128]["plan"].clone();
            let prefix = events.iter().map(|v| format!("{v}\n")).collect::<String>();
            config.prefix = artifact(
                &temp.path().join("one-fail-prefix.jsonl"),
                prefix.as_bytes(),
            );
            let prefix =
                read_pinned(&config.prefix, PREFIX_BYTES, true, &mut Guard::new(&caps())).unwrap();
            let replayed = replay(&prefix, &config, &maps, &mut Guard::new(&caps())).unwrap();
            assert_eq!(replayed["survived"], false);
            assert_eq!(replayed["per_panel_passes"], json!([63, 64]));
            let results = replayed["queries"].as_array().unwrap();
            assert_eq!(results.len(), 128);
            assert_eq!(results.iter().filter(|q| q["fits"] == true).count(), 127);
            assert_eq!(results[0]["fits"], false);
            assert_eq!(results[127]["fits"], true);
            assert!(
                results
                    .iter()
                    .all(|q| q["ranges"].as_array().unwrap().len() <= 32)
            );

            // Authentication succeeds, but a malformed last ordinal must still
            // produce INVALID (Err), even after the first cover exceeded16MiB.
            events[129]["ordinal"] = json!(62);
            let prefix = events.iter().map(|v| format!("{v}\n")).collect::<String>();
            config.prefix = artifact(
                &temp.path().join("late-invalid-prefix.jsonl"),
                prefix.as_bytes(),
            );
            let prefix =
                read_pinned(&config.prefix, PREFIX_BYTES, true, &mut Guard::new(&caps())).unwrap();
            let error = replay(&prefix, &config, &maps, &mut Guard::new(&caps())).unwrap_err();
            assert_eq!(
                error.to_string(),
                "pack frozen dataset/root/query/ordinal/nominee shape"
            );
            let map = pack(&from_arcs(43, &[(0, 42)]), 673, &mut Guard::new(&caps())).unwrap();
            assert_eq!(map, (0..43).collect::<Vec<u32>>());
            assert_eq!(map[42] as usize / 42, 1);
            let (mapped, _, _) = mapped_cover(&[0, 17, 34], &[1, 2, 0], 48, 2, 32).unwrap();
            assert_eq!(mapped, vec![16, 33, 2]);
        }
        #[test]
        fn pack_graph_binding_corruption_caps() {
            let id = identity(83);
            let good = encode(&id, &ring(83));
            let graph = decode(&good, &id).unwrap();
            assert!(decode(&vec![0; id.rows * 512 + 1], &id).is_err());
            let mut edges = 0;
            graph
                .inspect_authenticated_base_edges(|_, _| {
                    edges += 1;
                    Ok(())
                })
                .unwrap();
            assert_eq!(edges, 83);
            for offset in [0, 8, 16, 24, 32, 64, 96, 128] {
                let mut bad = good.clone();
                bad[offset] ^= 255;
                assert!(decode(&bad, &id).is_err());
            }
            for bad in [
                &good[..131],
                &good[..good.len() - 1],
                &[good.clone(), vec![0]].concat(),
            ] {
                assert!(decode(bad, &id).is_err());
            }
            let mut towers = ring(83);
            towers[0] = vec![vec![0]];
            assert!(decode(&encode(&id, &towers), &id).is_err());
            towers[0] = vec![vec![1, 1]];
            assert!(decode(&encode(&id, &towers), &id).is_err());
            towers[0] = vec![vec![84]];
            assert!(decode(&encode(&id, &towers), &id).is_err());
            towers[0] = vec![vec![1], vec![1]];
            assert!(decode(&encode(&id, &towers), &id).is_err());
            towers = ring(83);
            towers[0] = vec![Vec::new()];
            assert!(decode(&encode(&id, &towers), &id).is_err());
            towers = ring(83);
            towers[0] = vec![(1..83).collect()];
            assert!(decode(&encode(&id, &towers), &id).is_ok()); // retain repair edges above64
            assert!(
                PqVectorGraph::decode_for_inspection(&good, &id, &mut |_| Err("expired".into()))
                    .is_err()
            );
            let temp = tempfile::tempdir().unwrap();
            let (config, protocol) = fixture(temp.path());
            let mut low = config.clone();
            low.caps.memory_bytes = FIXED_WORKSPACE;
            OPENS.with(|o| o.borrow_mut().clear());
            assert!(
                run_fixture(
                    &low,
                    &protocol,
                    &temp.path().join("memory.json"),
                    None,
                    &mut |_| Ok(())
                )
                .is_err()
            );
            OPENS.with(|o| assert!(!o.borrow().contains(&config.panels[0].graph.path)));
            let mut low = config.clone();
            low.caps.operations = 1;
            assert!(
                run_fixture(
                    &low,
                    &protocol,
                    &temp.path().join("operations.json"),
                    None,
                    &mut |_| Ok(())
                )
                .is_err()
            );
            let mut guard = Guard::new(&caps());
            guard.start = Instant::now() - Duration::from_secs(301);
            assert!(guard.tick(0).is_err());
            let mut changed = config.clone();
            changed.panels[0].identity.layout[0] ^= 1;
            assert!(
                run_fixture(
                    &changed,
                    &protocol,
                    &temp.path().join("identity.json"),
                    None,
                    &mut |_| Ok(())
                )
                .is_err()
            );
            let graph_path = &config.panels[0].graph.path;
            let original = fs::read(graph_path).unwrap();
            let mut corrupt = original.clone();
            corrupt[0] ^= 1;
            fs::write(graph_path, &corrupt).unwrap();
            assert!(
                build_permutation(&config.panels[0], &caps(), &mut Guard::new(&caps()), 0).is_err()
            );
            fs::write(graph_path, &original).unwrap();
            let mut manifest: Manifest =
                serde_json::from_slice(&fs::read(&config.panels[0].root.path).unwrap()).unwrap();
            manifest.low[0] = 0.0; // same numeric zero, different raw layout identity
            let mut changed = config.clone();
            changed.panels[0].root = artifact(
                &config.panels[0].root.path,
                &serde_json::to_vec(&manifest).unwrap(),
            );
            assert!(
                build_permutation(&changed.panels[0], &caps(), &mut Guard::new(&caps()), 0)
                    .is_err()
            );
        }
        #[test]
        fn pack_secure_prefix_and_forbidden_opens() {
            let temp = tempfile::tempdir().unwrap();
            let (config, protocol) = fixture(temp.path());
            let prefix = fs::read(&config.prefix.path).unwrap();
            OpenOptions::new()
                .append(true)
                .open(&config.prefix.path)
                .unwrap()
                .write_all(b"\xffUNREAD_TRUTH_TAIL\n")
                .unwrap();
            OPENS.with(|o| o.borrow_mut().clear());
            READS.with(|o| o.borrow_mut().clear());
            run_fixture(
                &config,
                &protocol,
                &temp.path().join("good.json"),
                None,
                &mut |_| Ok(()),
            )
            .unwrap();
            READS.with(|r| {
                assert_eq!(
                    r.borrow()
                        .iter()
                        .filter(|(p, _)| p == &config.prefix.path)
                        .map(|(_, n)| n)
                        .sum::<usize>(),
                    prefix.len()
                )
            });
            let allowed = config
                .panels
                .iter()
                .flat_map(|p| [p.root.path.clone(), p.graph.path.clone()])
                .chain([
                    temp.path().to_path_buf(),
                    config.original_seal.path.clone(),
                    config.prefix.path.clone(),
                ])
                .collect::<BTreeSet<_>>();
            OPENS.with(|o| assert!(o.borrow().iter().all(|p| allowed.contains(p))));
            for kind in ["fifo", "symlink", "ancestor"] {
                let mut changed = config.clone();
                let path = temp.path().join(kind);
                if kind == "fifo" {
                    rustix::fs::mknodat(
                        rustix::fs::CWD,
                        &path,
                        rustix::fs::FileType::Fifo,
                        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
                        0,
                    )
                    .unwrap();
                } else if kind == "symlink" {
                    symlink(&config.prefix.path, &path).unwrap();
                } else {
                    symlink(temp.path(), &path).unwrap();
                }
                changed.prefix.path = if kind == "ancestor" {
                    path.join("trace.jsonl")
                } else {
                    path
                };
                assert!(
                    run_fixture(
                        &changed,
                        &protocol,
                        &temp.path().join(format!("bad-{kind}.json")),
                        None,
                        &mut |_| Ok(())
                    )
                    .is_err()
                );
            }
            let mut bad = prefix.clone();
            bad[0] ^= 1;
            fs::write(&config.prefix.path, &bad).unwrap();
            assert!(
                run_fixture(
                    &config,
                    &protocol,
                    &temp.path().join("tamper.json"),
                    None,
                    &mut |_| Ok(())
                )
                .is_err()
            );
        }
        #[test]
        fn pack_both_seals_precede_prefix() {
            let temp = tempfile::tempdir().unwrap();
            let (config, protocol) = fixture(temp.path());
            OPENS.with(|o| o.borrow_mut().clear());
            let mut observed = false;
            let result = run_fixture(
                &config,
                &protocol,
                &temp.path().join("out.json"),
                None,
                &mut |seal| {
                    OPENS.with(|o| assert!(!o.borrow().contains(&config.prefix.path)));
                    let body = fs::read(&seal.path)?;
                    assert_eq!(hash(&body), seal.sha256);
                    let v: Value = serde_json::from_slice(&body)?;
                    assert_eq!(v["diagnostic_source_sha256"], diagnostic_sources());
                    assert_eq!(
                        v["original_trace_source_identity_sha256"],
                        hash(b"original source")
                    );
                    assert_eq!(v["old_to_new"].as_array().unwrap().len(), 2);
                    for i in 0..2 {
                        assert_eq!(v["old_to_new"][i].as_array().unwrap().len(), 6);
                        assert_eq!(
                            v["panels"][i]["root"]["sha256"],
                            config.panels[i].root.sha256
                        );
                    }
                    observed = true;
                    Ok(())
                },
            )
            .unwrap();
            assert!(observed);
            assert_eq!(result["queries"], 128);
            assert_eq!(result["status"], "SURVIVED_NECESSARY_LOCALITY");
            assert_eq!(result["diagnostic_source_sha256"], diagnostic_sources());
            assert_eq!(
                result["details"]["replay"]["per_panel_passes"],
                json!([64, 64])
            );
        }
        #[test]
        fn pack_incomplete_output_sync_and_supervisor() {
            let temp = tempfile::tempdir().unwrap();
            let (config, protocol) = fixture(temp.path());
            for at in 1..=6 {
                let out = temp.path().join(format!("sync-{at}.json"));
                assert!(run_fixture(&config, &protocol, &out, Some(at), &mut |_| Ok(())).is_err());
                let v: Value = serde_json::from_slice(&fs::read(out).unwrap()).unwrap();
                assert_eq!(v["status"], "INVALID");
            }
            let out = temp.path().join("occupied.json");
            fs::write(&out, b"keep").unwrap();
            assert!(diagnose(&config, &hash(b"config"), &out).is_err());
            assert_eq!(fs::read(out).unwrap(), b"keep");
            let out = temp.path().join("occupied-companion.json");
            fs::write(out.with_extension("permutations.json"), b"keep").unwrap();
            assert!(run_fixture(&config, &protocol, &out, None, &mut |_| Ok(())).is_err());
            assert_eq!(
                fs::read(out.with_extension("permutations.json")).unwrap(),
                b"keep"
            );
            let mut low = config.clone();
            low.caps.output_bytes = 8192;
            assert!(
                run_fixture(
                    &low,
                    &protocol,
                    &temp.path().join("cap.json"),
                    None,
                    &mut |_| Ok(())
                )
                .is_err()
            );
            let out = temp.path().join("good.json");
            run_fixture(&config, &protocol, &out, None, &mut |_| Ok(())).unwrap();
            let body = fs::read(out).unwrap();
            let mut receipt = SupervisorReceipt {
                run_id: "original".into(),
                config_sha256: hash(b"config"),
                report_sha256: hash(&body),
                process_exit_code: 0,
                resource_limits_observed: true,
                drain_complete: true,
                cleanup_complete: true,
            };
            admit_survival(&body, "original", &receipt).unwrap();
            receipt.process_exit_code = 2;
            assert!(admit_survival(&body, "original", &receipt).is_err());
            receipt.process_exit_code = 0;
            receipt.cleanup_complete = false;
            assert!(admit_survival(&body, "original", &receipt).is_err());
            let prefix = fs::read(&config.prefix.path).unwrap();
            fs::write(&config.prefix.path, &prefix[..prefix.len() - 1]).unwrap();
            assert!(
                run_fixture(
                    &config,
                    &protocol,
                    &temp.path().join("short.json"),
                    None,
                    &mut |_| Ok(())
                )
                .is_err()
            );
            let maps = [(0..6).collect::<Vec<u32>>(), (0..6).collect::<Vec<u32>>()];
            assert!(
                replay(
                    &prefix[..prefix.len() - 1],
                    &config,
                    &maps,
                    &mut Guard::new(&caps())
                )
                .is_err()
            );
            let mut events = std::str::from_utf8(&prefix)
                .unwrap()
                .lines()
                .map(|s| serde_json::from_str::<Value>(s).unwrap())
                .collect::<Vec<_>>();
            for field in ["ordinal", "dataset", "query_sha256", "nominees"] {
                let old = events[2].clone();
                match field {
                    "ordinal" => events[2]["ordinal"] = json!(1),
                    "dataset" => events[2]["dataset"] = json!("cohere"),
                    "query_sha256" => events[2]["plan"]["query_sha256"] = json!("invalid"),
                    _ => events[2]["plan"]["nominees"] = json!([0, 0]),
                };
                let bad = events.iter().map(|v| format!("{v}\n")).collect::<String>();
                assert!(replay(bad.as_bytes(), &config, &maps, &mut Guard::new(&caps())).is_err());
                events[2] = old;
            }
            events.pop();
            let bad = events.iter().map(|v| format!("{v}\n")).collect::<String>();
            assert!(replay(bad.as_bytes(), &config, &maps, &mut Guard::new(&caps())).is_err());
        }
    }
}
