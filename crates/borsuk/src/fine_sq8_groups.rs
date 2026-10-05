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
    records: File,
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

    pub fn open(root: &Artifact, limits: &ResidentLimits) -> Result<Self> {
        Self::admission(root, limits)?;
        let manifest: Manifest =
            serde_json::from_slice(&read_source_probe_artifact(root, ROOT_CAP)?)?;
        let n = manifest.identity.rows;
        let d = manifest.identity.dimensions;
        let mut resources = model(n, d, manifest.graph.bytes, root.bytes, limits)?;
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
                && manifest.identity.layout
                    == layout_identity(
                        &manifest.primary_root,
                        &manifest.records,
                        &manifest.order,
                        &manifest.low,
                        &manifest.step,
                    )?,
            "fine root binding",
        )?;
        let parent = root.path.parent().ok_or("fine root parent")?;
        for (a, name) in [
            (&manifest.pq, "pq.bin"),
            (&manifest.graph, "graph.bin"),
            (&manifest.records, "records.bin"),
            (&manifest.groups, "groups.bin"),
            (&manifest.order, "order.bin"),
        ] {
            require(a.path == parent.join(name), "fine artifact path binding")?;
        }
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
        let records = secure_file(&manifest.records)?; // No payload read at startup.
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
            records,
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
            let mut payloads = Vec::with_capacity(plan.ranges.len());
            for range in &plan.ranges {
                accounting.attempted_gets += 1;
                accounting.attempted_bytes = accounting.attempted_bytes.map(|n| n + range.len());
                let mut body = vec![0; range.len()];
                let prior_read_bytes = accounting.read_bytes.take();
                self.records.read_exact_at(&mut body, range.start as u64)?;
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
