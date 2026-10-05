//! UNVERIFIED bounded whole-SQ8 overlap falsifier. No production lifecycle or
//! remote concurrency claim; the paired scientific arm always uses empty delta.

use crate::{
    exact_sq8_nominee::{Sq8Geometry, score_nominees},
    hierarchical_semantic_cells::{
        ARTIFACT_STREAM_BUFFER_BYTES, Artifact, BuildConfig, PrimaryLayoutMetadata, PrimaryReplay,
        Prototype, ReadStats, Result, SelectedCell, read_source_probe_artifact, replay_primary,
    },
    returned_sq8::{RecordSlice, rank_unique_sq8},
    sq8_source::cosine_vector,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::Path,
    sync::Arc,
};

pub const BUILD_SCHEMA: &str = "borsuk-cell-overlap-build-v1";
pub const SCHEMA: &str = "borsuk-cell-overlap-v1";
pub const HEADER_BYTES: usize = 64;
pub const MAX_GETS: usize = 32;
pub const MAX_BYTES: usize = 16 * 1024 * 1024;
const ROOT_CAP: usize = 1024 * 1024;
const MAGIC: &[u8; 8] = b"BSOV0001";

fn require(ok: bool, message: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(message.into()) }
}
fn hash(body: &[u8]) -> String {
    format!("{:x}", Sha256::digest(body))
}
fn create(path: &Path) -> Result<File> {
    Ok(OpenOptions::new().write(true).create_new(true).open(path)?)
}
fn secure_file(path: &Path, bytes: usize) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)?;
    require(
        file.metadata()?.is_file() && file.metadata()?.len() == bytes as u64,
        "overlap regular exact file",
    )?;
    Ok(file)
}

/// Explicit immutable source descriptors; no query/truth inputs are admitted.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverlapBuildConfig {
    pub schema: String,
    pub retained_root: Artifact,
    pub original: BuildConfig,
    pub overlap: bool,
    pub max_resident_payload_bytes: usize,
    pub max_build_payload_bytes: usize,
    pub max_output_bytes: usize,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverlapBuildReceipt {
    pub root_sha256: String,
    pub logical_rows: usize,
    pub physical_rows: usize,
    pub proposed: usize,
    pub admitted: usize,
    pub rejected: usize,
    pub cells: usize,
    pub extent_bytes: usize,
    pub mapping_bytes: usize,
    pub boundary_bytes: usize,
    pub output_bytes: usize,
    pub modeled_build_payload_bytes: usize,
}
/// Frame offsets and physical ordinals have different units.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Extent {
    pub cell_id: usize,
    pub primary_rows: usize,
    pub replica_rows: usize,
    pub first_physical_ordinal: usize,
    pub offset: usize,
    pub bytes: usize,
    pub sha256: String,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: String,
    primary_root: Artifact,
    logical_rows: usize,
    physical_rows: usize,
    dimensions: usize,
    overlap: bool,
    extent_sha256: String,
    mapping_sha256: String,
    boundary_bytes: usize,
    boundary_sha256: String,
    cells: Vec<Extent>,
    build: OverlapBuildReceipt,
}
#[derive(Debug, Clone)]
struct Proposal {
    margin: f64,
    source: usize,
    destination: usize,
}

fn boundary_body(replay: &PrimaryReplay) -> Result<Vec<u8>> {
    use crate::hierarchical_semantic_cells::SplitBoundary;
    let nodes = replay.nodes.iter().enumerate().map(|(id, node)| {
        let predicate = match &node.boundary {
            Some(SplitBoundary::Centers { left, right, separation, cut }) => serde_json::json!({
                "kind":"centers","left_bits":left.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                "right_bits":right.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),"separation_bits":separation.to_bits(),
                "cut_score_bits":cut.map(|v| v.0.to_bits()),"cut_tie_id":cut.map(|v| v.1)}),
            Some(SplitBoundary::Coordinate { axis, cut, range }) => serde_json::json!({
                "kind":"coordinate","axis":axis,"cut_score_bits":cut.0.to_bits(),"cut_tie_id":cut.1,"range_bits":range.to_bits()}),
            None => serde_json::Value::Null,
        };
        serde_json::json!({"identity":id,"depth":node.depth,"children":node.children,
            "cell_id":node.cell.as_ref().map(|c| c.cell_id),"predicate":predicate})
    }).collect::<Vec<_>>();
    let bytes = serde_json::to_vec(
        &serde_json::json!({"schema":"borsuk-cell-overlap-boundaries-v1",
        "logical_rows":replay.logical_rows,"dimensions":replay.dimensions,"nodes":nodes}),
    )?;
    require(
        bytes.len() <= 32 * 1024 * 1024,
        "overlap boundary artifact cap",
    )?;
    Ok(bytes)
}

fn descend(replay: &PrimaryReplay, mut node: usize, vector: &[f32], id: usize) -> Result<usize> {
    for _ in 0..replay.nodes.len() {
        let n = replay.nodes.get(node).ok_or("replay descent node")?;
        if let Some(cell) = &n.cell {
            return Ok(cell.cell_id);
        }
        let children = n.children.ok_or("replay descent children")?;
        let left = n
            .boundary
            .as_ref()
            .ok_or("replay descent predicate")?
            .goes_left(vector, id)?;
        node = children[usize::from(!left)];
    }
    Err("replay descent cycle".into())
}
fn propose(
    replay: &PrimaryReplay,
    vector: &[f32],
    id: usize,
    owner: usize,
) -> Result<Option<Proposal>> {
    let mut node = 0;
    let mut best: Option<(f64, usize, usize, usize)> = None;
    for _ in 0..replay.nodes.len() {
        let n = replay.nodes.get(node).ok_or("replay path node")?;
        if let Some(cell) = &n.cell {
            require(
                cell.cell_id == owner && cell.ids.contains(&id),
                "exact primary predicate replay",
            )?;
            return best
                .map(|(margin, _, _, sibling)| {
                    let destination = descend(replay, sibling, vector, id)?;
                    require(destination != owner, "replica cannot target owner")?;
                    Ok(Proposal {
                        margin,
                        source: id,
                        destination,
                    })
                })
                .transpose();
        }
        let boundary = n.boundary.as_ref().ok_or("replay path predicate")?;
        let children = n.children.ok_or("replay path children")?;
        let side = usize::from(!boundary.goes_left(vector, id)?);
        if let Some(margin) = boundary.margin(vector)? {
            let candidate = (margin, n.depth, node, children[1 - side]);
            if best.as_ref().is_none_or(|b| {
                candidate
                    .0
                    .total_cmp(&b.0)
                    .then(candidate.1.cmp(&b.1))
                    .then(candidate.2.cmp(&b.2))
                    .is_lt()
            }) {
                best = Some(candidate);
            }
        }
        node = children[side];
    }
    Err("replay path cycle".into())
}
fn admit_proposals(
    rows: usize,
    cells: usize,
    proposals: &mut [Proposal],
) -> Result<(Vec<Proposal>, usize)> {
    proposals.sort_by(|a, b| {
        a.margin
            .total_cmp(&b.margin)
            .then(a.source.cmp(&b.source))
            .then(a.destination.cmp(&b.destination))
    });
    let mut occupancy = vec![0_usize; cells];
    let mut seen = BTreeSet::new();
    let mut accepted = Vec::new();
    for p in proposals.iter() {
        require(
            p.margin.is_finite()
                && p.margin >= 0.
                && p.source < rows
                && p.destination < cells
                && seen.insert(p.source),
            "proposal identity/finite/one per source",
        )?;
        if accepted.len() < rows / 4 && occupancy[p.destination] < 128 {
            occupancy[p.destination] += 1;
            accepted.push(p.clone());
        }
    }
    let rejected = proposals.len() - accepted.len();
    Ok((accepted, rejected))
}
fn frame(extent: &Extent, dimensions: usize, logical: usize, records: &[u8]) -> Result<Vec<u8>> {
    require(
        records.len() == (extent.primary_rows + extent.replica_rows) * (dimensions + 12),
        "frame records",
    )?;
    let mut bytes = MAGIC.to_vec();
    for value in [
        extent.cell_id,
        dimensions,
        extent.primary_rows,
        extent.replica_rows,
        extent.first_physical_ordinal,
        logical,
        1,
    ] {
        bytes.extend_from_slice(&(value as u64).to_le_bytes());
    }
    bytes.extend_from_slice(records);
    Ok(bytes)
}
fn validate_frame<'a>(
    extent: &Extent,
    dimensions: usize,
    logical: usize,
    bytes: &'a [u8],
) -> Result<&'a [u8]> {
    require(
        bytes.len() == extent.bytes && bytes.len() >= HEADER_BYTES && &bytes[..8] == MAGIC,
        "overlap frame magic/length",
    )?;
    let expected = [
        extent.cell_id,
        dimensions,
        extent.primary_rows,
        extent.replica_rows,
        extent.first_physical_ordinal,
        logical,
        1,
    ];
    for (slot, value) in expected.into_iter().enumerate() {
        require(
            u64::from_le_bytes(bytes[8 + slot * 8..16 + slot * 8].try_into()?) == value as u64,
            "overlap frame identity",
        )?;
    }
    require(
        bytes.len() - HEADER_BYTES
            == (extent.primary_rows + extent.replica_rows) * (dimensions + 12),
        "overlap frame geometry",
    )?;
    Ok(&bytes[HEADER_BYTES..])
}
fn validate_placement(
    id: usize,
    cell: usize,
    replica: bool,
    placement: &[(usize, Option<usize>)],
) -> Result<()> {
    let &(owner, destination) = placement.get(id).ok_or("overlap source ID bounds")?;
    require(
        if replica {
            destination == Some(cell) && owner != cell
        } else {
            owner == cell
        },
        "overlap owner/replica mapping",
    )
}

/// Build one new SQ8-only arm without changing any retained primary artifact.
/// Exact replay and all input authentication precede replica admission.
pub fn build_overlap(config: &OverlapBuildConfig, output: &Path) -> Result<OverlapBuildReceipt> {
    require(
        config.schema == BUILD_SCHEMA
            && config.max_build_payload_bytes <= 8 * 1024 * 1024 * 1024
            && config.max_output_bytes <= 8 * 1024 * 1024 * 1024,
        "overlap build schema/resources",
    )?;
    require(
        std::fs::symlink_metadata(output).is_err(),
        "overlap output exists",
    )?;
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let retained = Prototype::open_for_source_probes(
        &config.retained_root,
        config.max_resident_payload_bytes,
    )?;
    let n = retained.rows();
    let d = retained.dimensions();
    let modeled = config
        .original
        .max_build_payload_bytes
        .checked_add(config.original.canonical.bytes)
        .and_then(|v| v.checked_add(config.original.sq8.bytes))
        .and_then(|v| v.checked_add(config.original.order.bytes))
        .and_then(|v| v.checked_add(n.checked_mul(512)?))
        .and_then(|v| v.checked_add(n.checked_mul(d)?.checked_mul(16)?))
        .and_then(|v| v.checked_add(ARTIFACT_STREAM_BUFFER_BYTES))
        .ok_or("overlap build payload overflow")?;
    require(
        modeled <= config.max_build_payload_bytes,
        "overlap build payload admission",
    )?;
    let replay = replay_primary(&config.original, &retained, parent)?;
    let canonical = config
        .original
        .canonical
        .read(config.original.canonical.bytes)?;
    let sq8 = config.original.sq8.read(config.original.sq8.bytes)?;
    let order = config.original.order.read(config.original.order.bytes)?;
    let mut inverse = vec![usize::MAX; n];
    for (physical, encoded) in order.chunks_exact(8).enumerate() {
        let id = usize::try_from(u64::from_le_bytes(encoded.try_into()?))?;
        require(
            id < n && inverse[id] == usize::MAX,
            "overlap original permutation",
        )?;
        inverse[id] = physical;
    }
    let mut leaves = replay
        .nodes
        .iter()
        .filter_map(|v| v.cell.clone())
        .collect::<Vec<_>>();
    leaves.sort_by_key(|v| v.cell_id);
    let mut placement = vec![(usize::MAX, None); n];
    for (cell_id, leaf) in leaves.iter().enumerate() {
        require(
            leaf.cell_id == cell_id && !leaf.ids.is_empty() && leaf.ids.len() <= 512,
            "overlap primary leaf",
        )?;
        for &id in &leaf.ids {
            require(
                id < n && placement[id].0 == usize::MAX,
                "overlap primary bijection",
            )?;
            placement[id].0 = cell_id;
        }
    }
    require(
        placement.iter().all(|p| p.0 != usize::MAX),
        "overlap complete primaries",
    )?;
    let mut proposals = Vec::new();
    for id in 0..n {
        let bytes = &canonical[inverse[id] * (8 + 4 * d)..(inverse[id] + 1) * (8 + 4 * d)];
        require(
            i64::from_le_bytes(bytes[..8].try_into()?) == id as i64,
            "overlap canonical ID",
        )?;
        let vector = bytes[8..]
            .chunks_exact(4)
            .map(|v| f32::from_le_bytes(v.try_into().unwrap()))
            .collect::<Vec<_>>();
        // Always validate predicates, including the no-overlap replay control.
        if let Some(p) = propose(&replay, &vector, id, placement[id].0)? {
            if config.overlap {
                proposals.push(p);
            }
        }
    }
    let (accepted, rejected) = admit_proposals(n, leaves.len(), &mut proposals)?;
    let mut replicas = vec![Vec::new(); leaves.len()];
    for p in &accepted {
        placement[p.source].1 = Some(p.destination);
        replicas[p.destination].push(p.source);
    }
    let staging = tempfile::tempdir_in(parent)?;
    let boundaries = boundary_body(&replay)?;
    let mut boundary_file = create(&staging.path().join("boundaries.json"))?;
    boundary_file.write_all(&boundaries)?;
    let mut cells_file = create(&staging.path().join("sq8-cells.bin"))?;
    let mut extents = Vec::new();
    let mut offset = 0;
    let mut ordinal = 0;
    let mut digest = Sha256::new();
    for leaf in &leaves {
        let mut records = Vec::new();
        for &id in leaf.ids.iter().chain(&replicas[leaf.cell_id]) {
            records.extend_from_slice(&sq8[inverse[id] * (d + 12)..(inverse[id] + 1) * (d + 12)]);
        }
        let mut extent = Extent {
            cell_id: leaf.cell_id,
            primary_rows: leaf.ids.len(),
            replica_rows: replicas[leaf.cell_id].len(),
            first_physical_ordinal: ordinal,
            offset,
            bytes: HEADER_BYTES + records.len(),
            sha256: String::new(),
        };
        let bytes = frame(&extent, d, n, &records)?;
        extent.sha256 = hash(&bytes);
        offset = offset
            .checked_add(bytes.len())
            .ok_or("overlap output overflow")?;
        require(
            offset + n * 16 + boundaries.len() + ROOT_CAP <= config.max_output_bytes,
            "overlap staging/output cap",
        )?;
        ordinal += leaf.ids.len() + replicas[leaf.cell_id].len();
        cells_file.write_all(&bytes)?;
        digest.update(&bytes);
        extents.push(extent);
    }
    let mut mapping = Vec::with_capacity(n * 16);
    for &(owner, destination) in &placement {
        mapping.extend_from_slice(&(owner as u64).to_le_bytes());
        mapping.extend_from_slice(&destination.map_or(u64::MAX, |v| v as u64).to_le_bytes());
    }
    let mut mapping_file = create(&staging.path().join("placement.bin"))?;
    mapping_file.write_all(&mapping)?;
    let mut manifest = Manifest {
        schema: SCHEMA.into(),
        primary_root: config.retained_root.clone(),
        logical_rows: n,
        physical_rows: ordinal,
        dimensions: d,
        overlap: config.overlap,
        extent_sha256: format!("{:x}", digest.finalize()),
        mapping_sha256: hash(&mapping),
        boundary_bytes: boundaries.len(),
        boundary_sha256: hash(&boundaries),
        cells: extents,
        build: OverlapBuildReceipt {
            logical_rows: n,
            physical_rows: ordinal,
            proposed: proposals.len(),
            admitted: accepted.len(),
            rejected,
            cells: leaves.len(),
            extent_bytes: offset,
            mapping_bytes: mapping.len(),
            boundary_bytes: boundaries.len(),
            modeled_build_payload_bytes: modeled,
            ..OverlapBuildReceipt::default()
        },
    };
    let mut body = Vec::new();
    for _ in 0..8 {
        body = serde_json::to_vec(&manifest)?;
        let total = offset + mapping.len() + boundaries.len() + body.len();
        if total == manifest.build.output_bytes {
            break;
        }
        manifest.build.output_bytes = total;
    }
    require(
        body.len() <= ROOT_CAP
            && manifest.build.output_bytes
                == offset + mapping.len() + boundaries.len() + body.len()
            && manifest.build.output_bytes <= config.max_output_bytes,
        "overlap manifest/output cap",
    )?;
    let mut root_file = create(&staging.path().join("manifest.json"))?;
    root_file.write_all(&body)?;
    for file in [&cells_file, &mapping_file, &boundary_file, &root_file] {
        file.sync_all()?;
    }
    File::open(staging.path())?.sync_all()?;
    // Reauthenticate originals after all random access and before publication.
    for artifact in [
        &config.original.canonical,
        &config.original.sq8,
        &config.original.order,
    ] {
        artifact.authenticate_streamed(config.max_build_payload_bytes)?;
    }
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        staging.path(),
        rustix::fs::CWD,
        output,
        rustix::fs::RenameFlags::NOREPLACE,
    )?;
    File::open(parent)?.sync_all()?;
    manifest.build.root_sha256 = hash(&body);
    Ok(manifest.build)
}

/// Full admission I/O, separate from the known-location query payload wave.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OverlapStartup {
    pub root: ReadStats,
    pub retained_metadata_root: ReadStats,
    pub mapping: ReadStats,
    pub boundaries: ReadStats,
    pub frames: ReadStats,
    pub retained_primary_gets: usize,
    pub retained_primary_bytes: usize,
    pub modeled_preload_peak_bytes: usize,
    pub modeled_resident_payload_bytes: usize,
}
/// Immutable root, placement mapping and retained unchanged resident router.
pub struct OverlapIndex {
    root_sha256: String,
    manifest: Manifest,
    router: Prototype,
    file: File,
    placement: Vec<(usize, Option<usize>)>,
    representatives: Vec<usize>,
    pub startup: OverlapStartup,
}
/// Immutable, authenticated metadata-only admission. No directory, mapping,
/// boundary or cell body is opened until this complete peak has been admitted.
#[derive(Debug)]
pub struct OverlapAdmission {
    root: Artifact,
    manifest: Manifest,
    primary: PrimaryLayoutMetadata,
    cap: usize,
    metadata_payload_bytes: usize,
    resident_payload_bytes: usize,
    preload_peak_bytes: usize,
    root_reads: ReadStats,
    primary_root_reads: ReadStats,
}
impl OverlapAdmission {
    pub fn read(root: &Artifact, max_resident_payload_bytes: usize) -> Result<Self> {
        Self::read_with_metadata_budget(
            root,
            max_resident_payload_bytes,
            max_resident_payload_bytes,
        )
    }
    fn read_with_metadata_budget(root: &Artifact, cap: usize, metadata_cap: usize) -> Result<Self> {
        let root_parse = root
            .bytes
            .checked_mul(8)
            .ok_or("overlap root metadata overflow")?;
        require(
            root.bytes > 0
                && root.bytes <= ROOT_CAP
                && cap <= 512 * 1024 * 1024
                && root_parse <= metadata_cap,
            "overlap root metadata admission",
        )?;
        let body = read_source_probe_artifact(root, ROOT_CAP)?;
        let manifest: Manifest = serde_json::from_slice(&body)?;
        let n = manifest.logical_rows;
        let d = manifest.dimensions;
        require(
            manifest.schema == SCHEMA
                && (1..=100_000).contains(&n)
                && (1..=768).contains(&d)
                && manifest.physical_rows >= n
                && manifest.physical_rows <= n + n / 4
                && !manifest.cells.is_empty()
                && manifest.cells.len() <= n
                && manifest.build.logical_rows == n
                && manifest.build.physical_rows == manifest.physical_rows
                && manifest.build.mapping_bytes == n * 16
                && manifest.boundary_bytes > 0
                && manifest.boundary_bytes <= 32 * 1024 * 1024
                && manifest.build.boundary_bytes == manifest.boundary_bytes
                && manifest.build.cells == manifest.cells.len()
                && manifest.build.admitted == manifest.physical_rows - n
                && manifest.build.admitted.checked_add(manifest.build.rejected)
                    == Some(manifest.build.proposed)
                && manifest.build.proposed <= n
                && manifest.build.extent_bytes
                    == manifest.physical_rows * (d + 12) + manifest.cells.len() * HEADER_BYTES
                && manifest
                    .build
                    .extent_bytes
                    .checked_add(manifest.build.mapping_bytes)
                    .and_then(|v| v.checked_add(manifest.boundary_bytes))
                    .and_then(|v| v.checked_add(root.bytes))
                    == Some(manifest.build.output_bytes)
                && (manifest.overlap || manifest.physical_rows == n),
            "overlap manifest geometry/receipt",
        )?;
        root.path.parent().ok_or("overlap root parent")?;
        let valid_digest = |value: &str| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|v| v.is_ascii_hexdigit() && !v.is_ascii_uppercase())
        };
        require(
            [
                &manifest.extent_sha256,
                &manifest.mapping_sha256,
                &manifest.boundary_sha256,
            ]
            .into_iter()
            .all(|v| valid_digest(v)),
            "overlap metadata artifact digests",
        )?;
        let mut offset = 0_usize;
        let mut ordinal = 0_usize;
        let mut primary_rows = 0_usize;
        for (id, extent) in manifest.cells.iter().enumerate() {
            require(
                extent.cell_id == id
                    && (1..=512).contains(&extent.primary_rows)
                    && extent.replica_rows <= 128
                    && extent.offset == offset
                    && extent.first_physical_ordinal == ordinal
                    && extent.bytes
                        == HEADER_BYTES + (extent.primary_rows + extent.replica_rows) * (d + 12)
                    && valid_digest(&extent.sha256),
                "overlap metadata extent geometry",
            )?;
            offset = offset
                .checked_add(extent.bytes)
                .ok_or("overlap metadata extent overflow")?;
            ordinal = ordinal
                .checked_add(extent.primary_rows + extent.replica_rows)
                .ok_or("overlap metadata ordinal overflow")?;
            primary_rows = primary_rows
                .checked_add(extent.primary_rows)
                .ok_or("overlap metadata primary overflow")?;
        }
        require(
            offset == manifest.build.extent_bytes
                && ordinal == manifest.physical_rows
                && primary_rows == n,
            "overlap metadata extent totals",
        )?;
        let primary = Prototype::admit_root_metadata(
            &manifest.primary_root,
            metadata_cap
                .checked_sub(root_parse)
                .ok_or("overlap metadata admission")?,
        )?;
        require(
            primary.rows == n && primary.dimensions == d && primary.cells == manifest.cells.len(),
            "overlap retained router geometry",
        )?;
        let metadata_payload_bytes = root_parse
            .checked_add(primary.modeled_metadata_payload_bytes)
            .ok_or("overlap metadata allowance overflow")?;
        let resident = primary
            .directory_admission
            .modeled_parsed_payload_bytes
            .checked_add(metadata_payload_bytes)
            .and_then(|v| v.checked_add(n.checked_mul(40)?))
            .ok_or("overlap resident overflow")?;
        let peak = primary
            .directory_admission
            .modeled_preload_peak_bytes
            // Original and reauthenticated metadata may coexist. The remaining
            // terms charge mapping/row tables, all-frame validation scratch,
            // and the separately parsed boundary artifact, before any preload.
            .checked_add(
                metadata_payload_bytes
                    .checked_mul(2)
                    .ok_or("overlap metadata coexistence overflow")?,
            )
            .and_then(|v| v.checked_add(n.checked_mul(d + 12 + 256)?))
            .and_then(|v| v.checked_add(2 * MAX_BYTES))
            .and_then(|v| v.checked_add(manifest.boundary_bytes.checked_mul(8)?))
            .ok_or("overlap preload overflow")?;
        require(peak <= cap, "overlap preload admission")?;
        let reads = |bytes| ReadStats {
            submitted_gets: 1,
            requested_bytes: bytes,
            verified_bytes: bytes,
            failed_gets: 0,
        };
        let primary_root_reads = reads(primary.root.bytes);
        Ok(Self {
            root: root.clone(),
            manifest,
            primary,
            cap,
            metadata_payload_bytes,
            resident_payload_bytes: resident,
            preload_peak_bytes: peak,
            root_reads: reads(root.bytes),
            primary_root_reads,
        })
    }
    fn reauthenticate(&mut self) -> Result<()> {
        let fresh = Self::read(&self.root, self.cap)?;
        require(
            fresh.metadata_payload_bytes == self.metadata_payload_bytes
                && fresh.resident_payload_bytes == self.resident_payload_bytes
                && fresh.preload_peak_bytes == self.preload_peak_bytes
                && serde_json::to_vec(&fresh.primary)? == serde_json::to_vec(&self.primary)?,
            "overlap admission metadata changed",
        )?;
        for (old, new) in [
            (&mut self.root_reads, fresh.root_reads),
            (&mut self.primary_root_reads, fresh.primary_root_reads),
        ] {
            old.submitted_gets += new.submitted_gets;
            old.requested_bytes += new.requested_bytes;
            old.verified_bytes += new.verified_bytes;
            old.failed_gets += new.failed_gets;
        }
        Ok(())
    }
    pub fn logical_rows(&self) -> usize {
        self.manifest.logical_rows
    }
    pub fn dimensions(&self) -> usize {
        self.manifest.dimensions
    }
    pub fn overlap_enabled(&self) -> bool {
        self.manifest.overlap
    }
    pub fn primary_root_identity(&self) -> &Artifact {
        &self.primary.root
    }
    pub fn primary_directory(&self) -> &Artifact {
        &self.primary.directory
    }
    pub fn boundary_sha256(&self) -> &str {
        &self.manifest.boundary_sha256
    }
    pub fn modeled_metadata_payload_bytes(&self) -> usize {
        self.metadata_payload_bytes
    }
    pub fn modeled_resident_payload_bytes(&self) -> usize {
        self.resident_payload_bytes
    }
    pub fn modeled_preload_peak_bytes(&self) -> usize {
        self.preload_peak_bytes
    }
}

/// Both metadata descriptors and their complete coexistence bound, admitted
/// before opening either heavy index. Root contents are reauthenticated first.
#[derive(Debug)]
pub struct OverlapPairAdmission {
    control: OverlapAdmission,
    candidate: OverlapAdmission,
    pub modeled_peak_payload_bytes: usize,
}
impl OverlapPairAdmission {
    pub fn read(
        control: &Artifact,
        candidate: &Artifact,
        per_arm_cap: usize,
        pair_cap: usize,
        evaluator_bytes: usize,
        query_scratch_bytes: usize,
    ) -> Result<Self> {
        require(
            pair_cap <= 512 * 1024 * 1024,
            "overlap pair metadata admission",
        )?;
        let control = OverlapAdmission::read_with_metadata_budget(control, per_arm_cap, pair_cap)?;
        let candidate = OverlapAdmission::read_with_metadata_budget(
            candidate,
            per_arm_cap,
            pair_cap
                .checked_sub(control.metadata_payload_bytes)
                .ok_or("overlap pair metadata admission")?,
        )?;
        let metadata = control
            .metadata_payload_bytes
            .checked_add(candidate.metadata_payload_bytes)
            .and_then(|v| {
                v.checked_add(
                    control
                        .metadata_payload_bytes
                        .max(candidate.metadata_payload_bytes),
                )
            })
            .ok_or("overlap pair metadata coexistence overflow")?;
        // Preserve both conservative startup coexistence bounds even though
        // the implementation opens control first and candidate second.
        let first = control
            .preload_peak_bytes
            .checked_add(candidate.resident_payload_bytes)
            .ok_or("overlap first preload coexistence overflow")?;
        let second = candidate
            .preload_peak_bytes
            .checked_add(control.resident_payload_bytes)
            .ok_or("overlap second preload coexistence overflow")?;
        let queries = control
            .resident_payload_bytes
            .checked_add(candidate.resident_payload_bytes)
            .and_then(|v| v.checked_add(evaluator_bytes))
            .and_then(|v| v.checked_add(query_scratch_bytes))
            .ok_or("overlap pair query coexistence overflow")?;
        let modeled_peak_payload_bytes = metadata.max(first).max(second).max(queries);
        require(
            modeled_peak_payload_bytes <= pair_cap,
            "overlap paired resident/evaluator/query coexistence",
        )?;
        Ok(Self {
            control,
            candidate,
            modeled_peak_payload_bytes,
        })
    }
    pub fn control(&self) -> &OverlapAdmission {
        &self.control
    }
    pub fn candidate(&self) -> &OverlapAdmission {
        &self.candidate
    }
    pub fn open(mut self) -> Result<(OverlapIndex, OverlapIndex)> {
        // Reauthenticate BOTH descriptors before either heavy index starts.
        self.control.reauthenticate()?;
        self.candidate.reauthenticate()?;
        let control = OverlapIndex::open_reauthenticated(self.control)?;
        let candidate = OverlapIndex::open_reauthenticated(self.candidate)?;
        Ok((control, candidate))
    }
}

impl OverlapIndex {
    /// Admit all root/parser and preload bounds using metadata only, then
    /// reauthenticate before loading the router or any overlap bodies.
    pub fn open(root: &Artifact, max_resident_payload_bytes: usize) -> Result<Self> {
        Self::open_admitted(OverlapAdmission::read(root, max_resident_payload_bytes)?)
    }
    pub fn open_admitted(mut admission: OverlapAdmission) -> Result<Self> {
        admission.reauthenticate()?;
        Self::open_reauthenticated(admission)
    }
    fn open_reauthenticated(admission: OverlapAdmission) -> Result<Self> {
        let OverlapAdmission {
            root,
            manifest,
            primary,
            cap,
            resident_payload_bytes: resident,
            preload_peak_bytes: peak,
            root_reads,
            primary_root_reads,
            ..
        } = admission;
        let n = manifest.logical_rows;
        let d = manifest.dimensions;
        let dir = root.path.parent().ok_or("overlap root parent")?;
        let router = Prototype::open_for_source_probes(&manifest.primary_root, cap)?;
        require(
            router.directory_admission.encoded_bytes == primary.directory.bytes
                && router.rows() == n
                && router.dimensions() == d,
            "admitted retained router metadata mismatch",
        )?;
        let boundaries = read_source_probe_artifact(
            &Artifact {
                path: dir.join("boundaries.json"),
                bytes: manifest.boundary_bytes,
                sha256: manifest.boundary_sha256.clone(),
            },
            32 * 1024 * 1024,
        )?;
        let boundary_root: serde_json::Value = serde_json::from_slice(&boundaries)?;
        require(
            boundary_root["schema"] == "borsuk-cell-overlap-boundaries-v1"
                && boundary_root["logical_rows"] == n
                && boundary_root["dimensions"] == d
                && boundary_root["nodes"]
                    .as_array()
                    .is_some_and(|v| !v.is_empty() && v.len() <= 2 * n),
            "overlap boundary evidence geometry",
        )?;
        drop(boundary_root);
        drop(boundaries);
        let mapping = read_source_probe_artifact(
            &Artifact {
                path: dir.join("placement.bin"),
                bytes: n * 16,
                sha256: manifest.mapping_sha256.clone(),
            },
            n * 16,
        )?;
        let mut placement = Vec::with_capacity(n);
        let mut replicas = 0;
        let mut occupancy = vec![0_usize; manifest.cells.len()];
        for item in mapping.chunks_exact(16) {
            let owner = usize::try_from(u64::from_le_bytes(item[..8].try_into()?))?;
            let dest = u64::from_le_bytes(item[8..].try_into()?);
            let dest = if dest == u64::MAX {
                None
            } else {
                Some(usize::try_from(dest)?)
            };
            require(
                owner < manifest.cells.len()
                    && dest.is_none_or(|v| v < manifest.cells.len() && v != owner),
                "overlap placement bounds",
            )?;
            if let Some(dest) = dest {
                replicas += 1;
                occupancy[dest] += 1;
            }
            placement.push((owner, dest));
        }
        require(
            replicas == manifest.physical_rows - n && occupancy.iter().all(|v| *v <= 128),
            "overlap placement quotas",
        )?;
        let file = secure_file(&dir.join("sq8-cells.bin"), manifest.build.extent_bytes)?;
        let mut original = vec![0_u8; n * (d + 12)];
        let mut primary_seen = vec![false; n];
        let mut replica_seen = vec![false; n];
        let mut representatives = vec![usize::MAX; n];
        let mut startup = OverlapStartup {
            modeled_preload_peak_bytes: peak,
            modeled_resident_payload_bytes: resident,
            root: root_reads,
            retained_metadata_root: primary_root_reads,
            mapping: ReadStats {
                submitted_gets: 1,
                requested_bytes: mapping.len(),
                verified_bytes: mapping.len(),
                failed_gets: 0,
            },
            boundaries: ReadStats {
                submitted_gets: 1,
                requested_bytes: manifest.boundary_bytes,
                verified_bytes: manifest.boundary_bytes,
                failed_gets: 0,
            },
            ..OverlapStartup::default()
        };
        let mut offset = 0;
        let mut ordinal = 0;
        let mut digest = Sha256::new();
        // First pass validates primaries against retained byte-exact bodies and
        // saves one canonical body per ID; replicas may occur before the owner.
        for (id, extent) in manifest.cells.iter().enumerate() {
            require(
                extent.cell_id == id
                    && extent.primary_rows > 0
                    && extent.primary_rows <= 512
                    && extent.replica_rows <= 128
                    && occupancy[id] == extent.replica_rows
                    && extent.offset == offset
                    && extent.first_physical_ordinal == ordinal
                    && extent.bytes
                        == HEADER_BYTES + (extent.primary_rows + extent.replica_rows) * (d + 12),
                "overlap extent geometry",
            )?;
            let mut bytes = vec![0; extent.bytes];
            startup.frames.submitted_gets += 1;
            startup.frames.requested_bytes += bytes.len();
            file.read_exact_at(&mut bytes, extent.offset as u64)?;
            require(hash(&bytes) == extent.sha256, "overlap full frame digest")?;
            let records = validate_frame(extent, d, n, &bytes)?;
            startup.frames.verified_bytes += bytes.len();
            digest.update(&bytes);
            let retained = router.primary_cell_sq8(id)?;
            startup.retained_primary_gets += 1;
            startup.retained_primary_bytes +=
                extent.primary_rows * (d + 20 + router.source_identity().records.bytes / n);
            require(
                retained == records[..extent.primary_rows * (d + 12)],
                "overlap retained primary body parity",
            )?;
            for (slot, record) in records.chunks_exact(d + 12).enumerate() {
                let source = usize::try_from(i64::from_le_bytes(record[..8].try_into()?))?;
                validate_placement(source, id, slot >= extent.primary_rows, &placement)?;
                representatives[source] =
                    representatives[source].min(extent.first_physical_ordinal + slot);
                let norm = f32::from_le_bytes(record[8..12].try_into()?);
                require(norm.is_finite() && norm > 0., "overlap record norm")?;
                if slot < extent.primary_rows {
                    require(!primary_seen[source], "overlap primary repeated")?;
                    primary_seen[source] = true;
                    original[source * (d + 12)..(source + 1) * (d + 12)].copy_from_slice(record);
                }
            }
            offset = offset
                .checked_add(extent.bytes)
                .ok_or("overlap offset overflow")?;
            ordinal += extent.primary_rows + extent.replica_rows;
        }
        require(
            offset == manifest.build.extent_bytes
                && ordinal == manifest.physical_rows
                && primary_seen.iter().all(|v| *v)
                && format!("{:x}", digest.finalize()) == manifest.extent_sha256,
            "overlap complete primary/frame identity",
        )?;
        for extent in &manifest.cells {
            if extent.replica_rows == 0 {
                continue;
            }
            let mut bytes = vec![0; extent.bytes];
            startup.frames.submitted_gets += 1;
            startup.frames.requested_bytes += bytes.len();
            file.read_exact_at(&mut bytes, extent.offset as u64)?;
            require(
                hash(&bytes) == extent.sha256,
                "overlap replica frame digest",
            )?;
            startup.frames.verified_bytes += bytes.len();
            let records = validate_frame(extent, d, n, &bytes)?;
            for record in records[extent.primary_rows * (d + 12)..].chunks_exact(d + 12) {
                let id = usize::try_from(i64::from_le_bytes(record[..8].try_into()?))?;
                validate_placement(id, extent.cell_id, true, &placement)?;
                require(
                    !replica_seen[id] && original[id * (d + 12)..(id + 1) * (d + 12)] == *record,
                    "overlap duplicate record bytes",
                )?;
                replica_seen[id] = true;
            }
        }
        require(
            replica_seen
                .iter()
                .zip(&placement)
                .all(|(seen, p)| *seen == p.1.is_some()),
            "overlap complete replica mapping",
        )?;
        Ok(Self {
            root_sha256: root.sha256.clone(),
            manifest,
            router,
            file,
            placement,
            representatives,
            startup,
        })
    }
    pub fn logical_rows(&self) -> usize {
        self.manifest.logical_rows
    }
    pub fn physical_rows(&self) -> usize {
        self.manifest.physical_rows
    }
    pub fn dimensions(&self) -> usize {
        self.manifest.dimensions
    }
    pub fn overlap_enabled(&self) -> bool {
        self.manifest.overlap
    }
    pub fn primary_root_identity(&self) -> &Artifact {
        &self.manifest.primary_root
    }
    pub fn router(&self) -> &Prototype {
        &self.router
    }
    pub fn root_sha256(&self) -> &str {
        &self.root_sha256
    }
    pub fn boundary_sha256(&self) -> &str {
        &self.manifest.boundary_sha256
    }
    pub fn replica_destinations(&self) -> &[(usize, Option<usize>)] {
        &self.placement
    }
    /// Global smallest encoded ordinal, including a copy outside this query's
    /// fetched cells; startup authenticated its logical ID and complete body.
    pub fn representative_location(&self, id: usize) -> Result<(usize, usize, usize)> {
        let ordinal = *self
            .representatives
            .get(id)
            .ok_or("representative source ID")?;
        for e in &self.manifest.cells {
            if (e.first_physical_ordinal
                ..e.first_physical_ordinal + e.primary_rows + e.replica_rows)
                .contains(&ordinal)
            {
                return Ok((
                    ordinal,
                    e.cell_id,
                    e.offset
                        + HEADER_BYTES
                        + (ordinal - e.first_physical_ordinal) * (self.dimensions() + 12),
                ));
            }
        }
        Err("representative extent missing".into())
    }
    /// All locations and complete serialized lengths are known before fetching.
    pub fn plan(&self, query: &[f32]) -> Result<FetchPlan> {
        let selected = self.router.select_cells(query)?;
        let extents = selected
            .iter()
            .map(|c| self.manifest.cells[c.cell_id].clone())
            .collect::<Vec<_>>();
        let bytes = extents.iter().try_fold(0_usize, |n, e| {
            n.checked_add(e.bytes).ok_or("overlap plan overflow")
        })?;
        require(
            extents.len() <= MAX_GETS && bytes <= MAX_BYTES,
            "overlap complete framed fetch cap",
        )?;
        Ok(FetchPlan {
            root_sha256: self.root_sha256.clone(),
            query_sha256: query_hash(query),
            selected,
            extents,
            bytes,
        })
    }
}
fn query_hash(query: &[f32]) -> String {
    hash(
        &query
            .iter()
            .flat_map(|v| v.to_bits().to_le_bytes())
            .collect::<Vec<_>>(),
    )
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FetchPlan {
    pub root_sha256: String,
    pub query_sha256: String,
    pub selected: Vec<SelectedCell>,
    pub extents: Vec<Extent>,
    pub bytes: usize,
}

/// Atomically pinned immutable base/delta tuple, with SQ8-body semantics.
#[derive(Clone)]
pub struct OverlapSnapshot {
    index: Arc<OverlapIndex>,
    revision: u64,
    delta: Arc<BTreeMap<i64, Option<Vec<u8>>>>,
    digest: String,
}
impl OverlapSnapshot {
    pub fn index(&self) -> &Arc<OverlapIndex> {
        &self.index
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn delta_sha256(&self) -> &str {
        &self.digest
    }
    pub fn delta_is_empty(&self) -> bool {
        self.delta.is_empty()
    }
}
fn delta_digest(
    index: &OverlapIndex,
    revision: u64,
    delta: &BTreeMap<i64, Option<Vec<u8>>>,
) -> String {
    let mut bytes = index.root_sha256.as_bytes().to_vec();
    bytes.extend_from_slice(&revision.to_le_bytes());
    for (id, body) in delta {
        bytes.extend_from_slice(&id.to_le_bytes());
        bytes.push(u8::from(body.is_some()));
        if let Some(body) = body {
            bytes.extend_from_slice(body);
        }
    }
    hash(&bytes)
}
/// Single publisher; sharing a pinned snapshot is independent of later writes.
/// No inserts, cell publication, compaction, recovery or generation swap API.
pub struct OverlapRevisions {
    current: OverlapSnapshot,
    max_delta_rows: usize,
    max_delta_bytes: usize,
}
impl OverlapRevisions {
    pub fn new(
        index: Arc<OverlapIndex>,
        max_delta_rows: usize,
        max_delta_bytes: usize,
    ) -> Result<Self> {
        require(
            max_delta_rows <= index.logical_rows() && max_delta_bytes <= 16 * 1024 * 1024,
            "overlap delta limits",
        )?;
        let delta = Arc::new(BTreeMap::new());
        let digest = delta_digest(&index, 0, &delta);
        Ok(Self {
            current: OverlapSnapshot {
                index,
                revision: 0,
                delta,
                digest,
            },
            max_delta_rows,
            max_delta_bytes,
        })
    }
    pub fn pin(&self) -> OverlapSnapshot {
        self.current.clone()
    }
    fn publish(&mut self, id: i64, body: Option<Vec<u8>>) -> Result<u64> {
        require(
            id >= 0 && (id as usize) < self.current.index.logical_rows(),
            "unsupported overlap insertion",
        )?;
        let mut delta = (*self.current.delta).clone();
        delta.insert(id, body);
        let bytes = delta.values().try_fold(0_usize, |n, v| {
            n.checked_add(v.as_ref().map_or(0, Vec::len) + 32)
                .ok_or("delta byte overflow")
        })?;
        require(
            delta.len() <= self.max_delta_rows && bytes <= self.max_delta_bytes,
            "overlap delta capacity refusal",
        )?;
        let revision = self
            .current
            .revision
            .checked_add(1)
            .ok_or("overlap revision overflow")?;
        let digest = delta_digest(&self.current.index, revision, &delta);
        self.current = OverlapSnapshot {
            index: Arc::clone(&self.current.index),
            revision,
            delta: Arc::new(delta),
            digest,
        };
        Ok(revision)
    }
    pub fn delete(&mut self, id: i64) -> Result<u64> {
        self.publish(id, None)
    }
    /// Replacement is one exact D+12 SQ8 body encoded under the pinned base's
    /// coefficients. No FP32 quantization, changed arithmetic or routing occurs.
    pub fn replace_sq8(&mut self, body: &[u8]) -> Result<u64> {
        let index = &self.current.index;
        require(
            body.len() == index.dimensions() + 12,
            "overlap replacement SQ8 width",
        )?;
        let id = i64::from_le_bytes(body[..8].try_into()?);
        let norm = f32::from_le_bytes(body[8..12].try_into()?);
        require(norm.is_finite() && norm > 0., "overlap replacement norm")?;
        let (low, step) = index.router.sq8_coefficients();
        score_nominees(
            body,
            Sq8Geometry {
                rows: 1,
                dimensions: index.dimensions(),
            },
            &[0],
            &vec![0.; index.dimensions()],
            low,
            step,
        )
        .map_err(|e| format!("overlap replacement SQ8: {e:?}"))?;
        self.publish(id, Some(body.to_vec()))
    }
    pub fn unsupported_maintenance(&self) -> Result<()> {
        Err(
            "unsupported overlap incremental publication/recovery/compaction/GC/generation swap"
                .into(),
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OverlapQueryAccounting {
    pub payload: ReadStats,
    pub dependency_waves: usize,
    pub max_parallel_gets: usize,
    pub physical_records: usize,
    pub unique_base_rows: usize,
    pub visible_unique_rows: usize,
    pub delta_scan_rows: usize,
    pub delta_body_bytes: usize,
    pub modeled_query_payload_bytes: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverlapScoredRow {
    pub id: i64,
    pub score_bits: u32,
    pub physical_ordinal: Option<usize>,
    pub framed_file_offset: Option<usize>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverlapSearchTrace {
    pub root_sha256: String,
    pub revision: u64,
    pub delta_sha256: String,
    pub selected: Vec<SelectedCell>,
    pub base_ids: Vec<i64>,
    pub replica_ids: Vec<i64>,
    pub ranked: Vec<OverlapScoredRow>,
    pub underfill: bool,
    pub accounting: OverlapQueryAccounting,
}
#[derive(Debug)]
pub struct OverlapSearchFailure {
    pub message: String,
    pub accounting: OverlapQueryAccounting,
}
impl std::fmt::Display for OverlapSearchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for OverlapSearchFailure {}

/// Authenticate every selected frame, check base copies before suppression,
/// score ALL visible unique SQ8 bodies, and truncate only after delta merging.
pub fn search_selected_sq8(
    snapshot: &OverlapSnapshot,
    plan: &FetchPlan,
    query: &[f32],
    top_k: usize,
) -> std::result::Result<OverlapSearchTrace, OverlapSearchFailure> {
    let mut accounting = OverlapQueryAccounting::default();
    search_inner(snapshot, plan, query, top_k, &mut accounting).map_err(|e| OverlapSearchFailure {
        message: e.to_string(),
        accounting,
    })
}
fn search_inner(
    snapshot: &OverlapSnapshot,
    plan: &FetchPlan,
    query: &[f32],
    top_k: usize,
    accounting: &mut OverlapQueryAccounting,
) -> Result<OverlapSearchTrace> {
    let index = &snapshot.index;
    require(
        plan.root_sha256 == index.root_sha256
            && plan.query_sha256 == query_hash(query)
            && !plan.extents.is_empty()
            && plan.extents.len() <= MAX_GETS
            && plan.extents.len() == plan.selected.len()
            && plan.bytes <= MAX_BYTES
            && top_k > 0
            && query.len() == index.dimensions(),
        "overlap pinned plan/limits",
    )?;
    let mut seen_cells = BTreeSet::new();
    let mut expected_bytes = 0_usize;
    for (extent, selected) in plan.extents.iter().zip(&plan.selected) {
        require(
            seen_cells.insert(extent.cell_id)
                && selected.cell_id == extent.cell_id
                && index.manifest.cells.get(extent.cell_id) == Some(extent),
            "overlap authenticated plan spans",
        )?;
        expected_bytes = expected_bytes
            .checked_add(extent.bytes)
            .ok_or("overlap query bytes overflow")?;
    }
    require(
        expected_bytes == plan.bytes,
        "overlap plan complete byte charge",
    )?;
    let d = index.dimensions();
    let count = plan
        .extents
        .iter()
        .map(|e| e.primary_rows + e.replica_rows)
        .sum::<usize>();
    let delta_bytes = snapshot
        .delta
        .values()
        .filter_map(|v| v.as_ref())
        .map(Vec::len)
        .sum::<usize>();
    accounting.modeled_query_payload_bytes = plan.bytes * 3
        + count * (d + 12 + 512)
        + snapshot.delta.len() * (d + 12 + 512)
        + 8 * (8 + 24) * (d * 4 + 4096);
    require(
        accounting.modeled_query_payload_bytes <= 512 * 1024 * 1024,
        "overlap query payload admission",
    )?;
    accounting.delta_scan_rows = snapshot.delta.len();
    accounting.delta_body_bytes = delta_bytes;
    let normalized = cosine_vector(query)?;
    accounting.dependency_waves = 1;
    accounting.max_parallel_gets = 1;
    let mut bodies = Vec::with_capacity(plan.extents.len());
    let mut base_ids = BTreeSet::new();
    let mut replica_ids = BTreeSet::new();
    for extent in &plan.extents {
        let mut bytes = vec![0; extent.bytes];
        accounting.payload.submitted_gets += 1;
        accounting.payload.requested_bytes += extent.bytes;
        if let Err(error) = index.file.read_exact_at(&mut bytes, extent.offset as u64) {
            accounting.payload.failed_gets += 1;
            return Err(error.into());
        }
        if hash(&bytes) != extent.sha256 {
            accounting.payload.failed_gets += 1;
            return Err("overlap query full frame authentication".into());
        }
        let records = match validate_frame(extent, d, index.logical_rows(), &bytes) {
            Ok(records) => records,
            Err(error) => {
                accounting.payload.failed_gets += 1;
                return Err(error);
            }
        };
        accounting.payload.verified_bytes += extent.bytes;
        for (slot, record) in records.chunks_exact(d + 12).enumerate() {
            let id = i64::from_le_bytes(record[..8].try_into()?);
            validate_placement(
                usize::try_from(id)?,
                extent.cell_id,
                slot >= extent.primary_rows,
                &index.placement,
            )?;
            base_ids.insert(id);
            if slot >= extent.primary_rows {
                replica_ids.insert(id);
            }
        }
        bodies.push(bytes);
    }
    accounting.physical_records = count;
    accounting.unique_base_rows = base_ids.len();
    let slices = bodies
        .iter()
        .zip(&plan.extents)
        .map(|(bytes, e)| RecordSlice {
            first_physical_ordinal: e.first_physical_ordinal,
            bytes: &bytes[HEADER_BYTES..],
        })
        .collect::<Vec<_>>();
    let excluded = snapshot.delta.keys().copied().collect::<Vec<_>>();
    let (low, step) = index.router.sq8_coefficients();
    let base = rank_unique_sq8(
        index.logical_rows(),
        Sq8Geometry {
            rows: index.physical_rows(),
            dimensions: d,
        },
        &slices,
        &normalized,
        low,
        step,
        index.logical_rows(),
        plan.bytes,
        &excluded,
    )
    .map_err(|e| format!("overlap unique SQ8: {e:?}"))?;
    let mut ranked = Vec::with_capacity(base.len() + snapshot.delta.len());
    for s in base {
        let (ordinal, _, offset) = index.representative_location(usize::try_from(s.id)?)?;
        ranked.push(OverlapScoredRow {
            id: s.id,
            score_bits: s.score.to_bits(),
            physical_ordinal: Some(ordinal),
            framed_file_offset: Some(offset),
        });
    }
    for body in snapshot.delta.values().filter_map(|v| v.as_ref()) {
        let s = score_nominees(
            body,
            Sq8Geometry {
                rows: 1,
                dimensions: d,
            },
            &[0],
            &normalized,
            low,
            step,
        )
        .map_err(|e| format!("overlap delta SQ8: {e:?}"))?[0];
        ranked.push(OverlapScoredRow {
            id: s.id,
            score_bits: s.score.to_bits(),
            physical_ordinal: None,
            framed_file_offset: None,
        });
    }
    ranked.sort_by(|a, b| {
        f32::from_bits(a.score_bits)
            .total_cmp(&f32::from_bits(b.score_bits))
            .then(a.id.cmp(&b.id))
    });
    accounting.visible_unique_rows = ranked.len();
    ranked.truncate(top_k);
    Ok(OverlapSearchTrace {
        root_sha256: index.root_sha256.clone(),
        revision: snapshot.revision,
        delta_sha256: snapshot.digest.clone(),
        selected: plan.selected.clone(),
        base_ids: base_ids.into_iter().collect(),
        replica_ids: replica_ids.into_iter().collect(),
        underfill: ranked.len() < top_k,
        ranked,
        accounting: accounting.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: i64, code: u8) -> Vec<u8> {
        let mut v = id.to_le_bytes().to_vec();
        v.extend_from_slice(&1_f32.to_le_bytes());
        v.push(code);
        v
    }

    #[test]
    fn overlap_degenerate_descent_without_crossing() {
        use crate::hierarchical_semantic_cells::{ReplayCell, ReplayNode, SplitBoundary};
        let nodes = vec![
            ReplayNode {
                depth: 1,
                boundary: Some(SplitBoundary::Coordinate {
                    axis: 0,
                    cut: (0., 0),
                    range: 1.,
                }),
                children: Some([1, 2]),
                cell: None,
            },
            ReplayNode {
                depth: 2,
                cell: Some(ReplayCell {
                    cell_id: 0,
                    ids: vec![0],
                }),
                ..ReplayNode::default()
            },
            ReplayNode {
                depth: 2,
                boundary: Some(SplitBoundary::Coordinate {
                    axis: 0,
                    cut: (0., 5),
                    range: 0.,
                }),
                children: Some([3, 4]),
                cell: None,
            },
            ReplayNode {
                depth: 3,
                cell: Some(ReplayCell {
                    cell_id: 1,
                    ids: vec![5],
                }),
                ..ReplayNode::default()
            },
            ReplayNode {
                depth: 3,
                cell: Some(ReplayCell {
                    cell_id: 2,
                    ids: vec![6],
                }),
                ..ReplayNode::default()
            },
        ];
        let replay = PrimaryReplay {
            logical_rows: 7,
            dimensions: 1,
            nodes,
        };
        assert_eq!(
            propose(&replay, &[0.], 0, 0).unwrap().unwrap().destination,
            1
        );
        assert_eq!(descend(&replay, 2, &[0.], 6).unwrap(), 2);
        assert_eq!(
            replay.nodes[2]
                .boundary
                .as_ref()
                .unwrap()
                .margin(&[0.])
                .unwrap(),
            None
        );
    }

    #[test]
    fn overlap_quota_priority_and_full_destination() {
        let mut proposals = (0..140)
            .rev()
            .map(|id| Proposal {
                margin: 0.,
                source: id,
                destination: 1,
            })
            .collect::<Vec<_>>();
        proposals.push(Proposal {
            margin: 1.,
            source: 140,
            destination: 2,
        });
        let (accepted, rejected) = admit_proposals(1000, 3, &mut proposals).unwrap();
        assert_eq!(accepted.len(), 129);
        assert_eq!(rejected, 12);
        assert_eq!(
            accepted[..128].iter().map(|p| p.source).collect::<Vec<_>>(),
            (0..128).collect::<Vec<_>>()
        );
        assert_eq!(accepted[128].source, 140);
        let (accepted, _) = admit_proposals(8, 3, &mut proposals[..8]).unwrap();
        assert_eq!(accepted.len(), 2);
    }

    #[test]
    fn overlap_equal_path_margin_prefers_depth_then_boundary_identity() {
        use crate::hierarchical_semantic_cells::{ReplayCell, ReplayNode, SplitBoundary};
        let boundary = SplitBoundary::Coordinate {
            axis: 0,
            cut: (0., 1),
            range: 1.,
        };
        let replay = PrimaryReplay {
            logical_rows: 3,
            dimensions: 1,
            nodes: vec![
                ReplayNode {
                    depth: 1,
                    boundary: Some(boundary.clone()),
                    children: Some([1, 4]),
                    cell: None,
                },
                ReplayNode {
                    depth: 2,
                    boundary: Some(boundary),
                    children: Some([2, 3]),
                    cell: None,
                },
                ReplayNode {
                    depth: 3,
                    cell: Some(ReplayCell {
                        cell_id: 0,
                        ids: vec![0],
                    }),
                    ..ReplayNode::default()
                },
                ReplayNode {
                    depth: 3,
                    cell: Some(ReplayCell {
                        cell_id: 1,
                        ids: vec![1],
                    }),
                    ..ReplayNode::default()
                },
                ReplayNode {
                    depth: 2,
                    cell: Some(ReplayCell {
                        cell_id: 2,
                        ids: vec![2],
                    }),
                    ..ReplayNode::default()
                },
            ],
        };
        assert_eq!(
            propose(&replay, &[0.], 0, 0).unwrap().unwrap().destination,
            2
        );
        // Give equal declared depths to make boundary identity, rather than
        // alternate destination ID, decide the equal-margin tie.
        let mut same_depth = replay;
        same_depth.nodes[1].depth = 1;
        assert_eq!(
            propose(&same_depth, &[0.], 0, 0)
                .unwrap()
                .unwrap()
                .destination,
            2
        );
    }

    #[test]
    fn overlap_corrupt_frame_mapping_and_body() {
        let extent = Extent {
            cell_id: 0,
            primary_rows: 1,
            replica_rows: 0,
            first_physical_ordinal: 0,
            offset: 0,
            bytes: HEADER_BYTES + 13,
            sha256: String::new(),
        };
        let mut body = frame(&extent, 1, 1, &record(0, 1)).unwrap();
        assert!(validate_frame(&extent, 1, 1, &body).is_ok());
        body[0] ^= 1;
        assert!(validate_frame(&extent, 1, 1, &body).is_err());
        body[0] ^= 1;
        body[48] ^= 1;
        assert!(validate_frame(&extent, 1, 1, &body).is_err());
        assert!(validate_placement(0, 1, true, &[(0, None)]).is_err());
        assert!(validate_placement(0, 0, false, &[(0, Some(1))]).is_ok());
        assert!(validate_placement(0, 1, false, &[(0, Some(1))]).is_err());
        assert!(validate_placement(0, 1, true, &[(0, Some(1))]).is_ok());
        assert!(validate_placement(0, 0, true, &[(0, Some(1))]).is_err());
    }
}
