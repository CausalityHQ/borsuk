//! Experiment-private construction authority; no query, truth or serving decoder.
use super::*;
use borsuk::semantic_unit_router::{
    Geometry, RouterArtifacts, SemanticProfile, SemanticUnitRouter, SourceIdentity, admit, build,
    preflight, validate_publication,
};

const CONFIG_SCHEMA: &str = "borsuk-partition-reconstruction-config-v1";
const APPROVAL_SCHEMA: &str = "borsuk-partition-reconstruction-approval-v1";
const RECEIPT_SCHEMA: &str = "borsuk-partition-reconstruction-receipt-v1";
const EXPERIMENT_CAP: u64 = 512 * 1024 * 1024;
const STREAM_BYTES: usize = 16 * 1024;
const METADATA_RESERVE: u64 = 1024 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReconstructionConfig {
    schema: String,
    generation_manifest: CompletedInput,
    plane_manifest: CompletedInput,
    leaves: CompletedInput,
    historical: Option<HistoricalPins>,
    caller_pinned_bytes: u64,
    max_payload_bytes: u64,
    output_directory: PathBuf,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoricalPins {
    root: CompletedInput,
    membership: CompletedInput,
}

// This separately SHA-approved document is not accepted through CONFIG authority.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OriginalApproval {
    schema: String,
    generation_sha256: String,
    generation: u64,
    base_epoch: u64,
    plane_sha256: String,
    discovery_sha256: String,
    input_schema: String,
    input_root_sha256: String,
    centroids_sha256: String,
    profile: SemanticProfile,
    rows: usize,
    dimensions: usize,
}

#[derive(Serialize)]
struct Descriptor {
    bytes: u64,
    sha256: String,
}

// Only authenticate_construction creates this token. In particular it is not Deserialize.
struct ConstructionAuthority {
    approved: OriginalApproval,
    root: Descriptor,
    membership: Descriptor,
    leaves: Descriptor,
    source: Value,
    metadata: Vec<HeaderSnapshot>,
    geometry: Geometry,
    metadata_charge: u64,
    max_payload_bytes: u64,
}

impl ConstructionAuthority {
    fn identity(&self) -> SourceIdentity<'_> {
        SourceIdentity {
            profile: self.approved.profile,
            schema: &self.approved.input_schema,
            root_sha256: &self.approved.input_root_sha256,
            centroids_sha256: &self.approved.centroids_sha256,
            rows: self.geometry.rows,
            dimensions: self.geometry.dimensions,
        }
    }

    fn recheck(&self) -> Result<()> {
        recheck_distinct(&self.metadata.iter().collect::<Vec<_>>())
    }
}

struct ReconstructedCentroids {
    blob: Vec<u8>,
    leaf: HeaderSnapshot,
    generation_sha256: String,
    reconstruction_charge: u64,
}

fn digest(body: &[u8]) -> String {
    format!("{:x}", Sha256::digest(body))
}

fn times(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b).ok_or_else(|| "product overflow".into())
}

fn admission(cap: u64, parts: &[u64]) -> Result<u64> {
    let total = parts.iter().try_fold(0, |n, &part| plus(n, part))?;
    require(total <= cap, "cumulative reconstruction admission")?;
    Ok(total)
}

fn geometry(a: &OriginalApproval) -> Result<Geometry> {
    require(
        size_of::<usize>() == 8 && a.profile.valid_geometry(a.rows, a.dimensions),
        "approved reconstruction geometry",
    )?;
    let units = plus(a.rows as u64, 31)? / 32;
    let blob_bytes = plus(32, times(times(units, a.dimensions as u64)?, 2)?)?;
    Ok(Geometry {
        rows: a.rows,
        dimensions: a.dimensions,
        units: usize::try_from(units)?,
        blob_bytes: usize::try_from(blob_bytes)?,
    })
}

fn recheck_distinct(snapshots: &[&HeaderSnapshot]) -> Result<()> {
    let mut identities = BTreeSet::new();
    for snapshot in snapshots {
        snapshot.recheck()?;
        require(
            identities.insert((snapshot.identity.dev, snapshot.identity.ino)),
            "reconstruction role alias",
        )?;
    }
    Ok(())
}

fn read_small<T: DeserializeOwned>(path: &Path, sha: &str) -> Result<(T, HeaderSnapshot)> {
    let file = header_file(path)?;
    let identity = file_identity(&file)?;
    let (value, _) = read_config_file(&file, path, sha, CONFIG_CAP)?;
    let snapshot = HeaderSnapshot {
        file,
        path: path.to_owned(),
        identity,
    };
    snapshot.recheck()?;
    Ok((value, snapshot))
}

fn descriptor(d: &Value, name: &str, expected_bytes: Option<u64>, cap: u64) -> Result<Descriptor> {
    let bytes = d[format!("{name}_bytes")]
        .as_u64()
        .ok_or("descriptor integer bytes")?;
    let sha256 = d[format!("{name}_sha256")]
        .as_str()
        .ok_or("descriptor SHA type")?;
    require(
        bytes > 0 && bytes <= cap && expected_bytes.is_none_or(|n| n == bytes) && valid_sha(sha256),
        "original Discovery descriptor geometry/SHA",
    )?;
    Ok(Descriptor {
        bytes,
        sha256: sha256.to_owned(),
    })
}

fn match_pin(pin: &CompletedInput, descriptor: &Descriptor) -> Result<()> {
    header_pin(pin, descriptor.bytes)?;
    require(
        pin.bytes == descriptor.bytes && pin.sha256 == descriptor.sha256,
        "original publication descriptor binding",
    )
}

fn authenticate_construction(
    config_path: &Path,
    config_sha: &str,
    approval_path: &Path,
    approval_sha: &str,
) -> Result<(ReconstructionConfig, ConstructionAuthority)> {
    // Fixed bootstrap bounds precede config/approval parsing; neither controls this bound.
    let bootstrap_charge = admission(
        EXPERIMENT_CAP,
        &[
            times(64, times(2, CONFIG_CAP)?)?,
            METADATA_RESERVE,
            (2 * STREAM_BYTES) as u64,
        ],
    )?;
    let (raw, cs): (Value, _) = read_small(config_path, config_sha)?;
    fields(
        &raw,
        "schema generation_manifest plane_manifest leaves historical caller_pinned_bytes max_payload_bytes output_directory",
    )?;
    let c: ReconstructionConfig = serde_json::from_value(raw)?;
    let (a, approved_snapshot): (OriginalApproval, _) = read_small(approval_path, approval_sha)?;
    require(
        c.schema == CONFIG_SCHEMA && a.schema == APPROVAL_SCHEMA,
        "reconstruction schema",
    )?;
    require(
        c.max_payload_bytes > 0
            && c.max_payload_bytes <= EXPERIMENT_CAP
            && a.generation > 0
            && (1..=256).contains(&a.input_schema.len())
            && [
                &a.generation_sha256,
                &a.plane_sha256,
                &a.discovery_sha256,
                &a.input_root_sha256,
                &a.centroids_sha256,
            ]
            .iter()
            .all(|s| valid_sha(s))
            && c.generation_manifest.sha256 == a.generation_sha256
            && c.plane_manifest.sha256 == a.plane_sha256,
        "independently approved original identities",
    )?;
    let geometry = geometry(&a)?;
    header_pin(&c.generation_manifest, HEADER_JSON_CAP)?;
    header_pin(&c.plane_manifest, HEADER_JSON_CAP)?;
    header_path(&c.output_directory)?;
    // Covers retained typed/Value copies, recursive parser objects, paths, receipt and stream buffers.
    let metadata_charge = admission(
        c.max_payload_bytes,
        &[
            c.caller_pinned_bytes,
            bootstrap_charge,
            times(
                64,
                plus(c.generation_manifest.bytes, c.plane_manifest.bytes)?,
            )?,
        ],
    )?;
    recheck_distinct(&[&cs, &approved_snapshot])?;
    let (g, gs) = header_json(&c.generation_manifest)?;
    fields(
        &g,
        "schema generation base_epoch plane_manifest_sha256 page_manifest_sha256 discovery sq8_object_sha256 sq8_object_key sq8_etag canonical low step",
    )?;
    require(
        g["schema"] == "borsuk-two-bit-generation-v8"
            && g["generation"].as_u64() == Some(a.generation)
            && g["base_epoch"].as_u64() == Some(a.base_epoch)
            && g["plane_manifest_sha256"] == a.plane_sha256
            && g["page_manifest_sha256"].as_str().is_some_and(valid_sha),
        "original generation/epoch/plane approval",
    )?;
    let d = &g["discovery"];
    fields(
        d,
        "mode profile root_sha256 root_bytes membership_sha256 membership_bytes leaves_sha256 leaves_bytes input_schema input_root_sha256 centroids_sha256 source_sha256 source_order_sha256 mean_sha256 records_sha256 sq8_sha256",
    )?;
    let profile = match a.profile {
        SemanticProfile::Native100k => "native100k",
        SemanticProfile::Fresh1m => "fresh1m",
        SemanticProfile::Scale1m => "scale1m",
    };
    require(
        d["mode"] == "semantic"
            && d["profile"] == profile
            && digest(&serde_json::to_vec(d)?) == a.discovery_sha256
            && d["input_schema"] == a.input_schema
            && d["input_root_sha256"] == a.input_root_sha256
            && d["centroids_sha256"] == a.centroids_sha256,
        "independent Discovery/construction approval",
    )?;
    let u = geometry.units as u64;
    let leaf_bytes = times(u, plus(4, times(2, geometry.dimensions as u64)?)?)?;
    let root = descriptor(d, "root", None, a.profile.root_cap() as u64)?;
    let entry_bytes = plus(64, times(4, geometry.dimensions as u64)?)?;
    let max_leaves = times(2, u.div_ceil(64))? - 1;
    require(
        root.bytes >= plus(512, entry_bytes)?
            && (root.bytes - 512) % entry_bytes == 0
            && (root.bytes - 512) / entry_bytes <= max_leaves,
        "original root directory/prototype descriptor geometry",
    )?;
    let membership = descriptor(d, "membership", Some(times(u, 4)?), times(u, 4)?)?;
    let leaves = descriptor(d, "leaves", Some(leaf_bytes), leaf_bytes)?;
    match_pin(&c.leaves, &leaves)?;
    // Historical paths remain unopened here, including when supplied for the later parity stage.
    if let Some(h) = &c.historical {
        match_pin(&h.root, &root)?;
        match_pin(&h.membership, &membership)?;
    }
    let (p, ps) = header_json(&c.plane_manifest)?;
    let plane: borsuk::two_bit_source::SourcePlaneReceipt = serde_json::from_value(p)?;
    require(
        plane.schema == "borsuk-two-bit-plane-v3"
            && plane.rows == geometry.rows
            && plane.dimensions == geometry.dimensions
            && plane.seed == 20260923
            && plane.page_rows == 32
            && !plane.query_or_truth_used
            && plane.record_bytes == source_record_bytes(geometry.dimensions)?
            && valid_sha(&plane.page_digest_sha256)
            && g["sq8_object_sha256"] == plane.sq8_sha256
            && g["sq8_object_key"]
                .as_str()
                .is_some_and(|key| neighborhood_object_key(key, &plane.sq8_sha256))
            && g["sq8_etag"].as_str().is_some_and(|s| {
                !s.is_empty() && !s.starts_with("W/") && !s.chars().any(char::is_control)
            }),
        "original source-only plane geometry/SQ8 descriptor",
    )?;
    let mut source = serde_json::Map::new();
    for (name, sha) in [
        ("source_sha256", &plane.source_sha256),
        ("source_order_sha256", &plane.source_order_sha256),
        ("mean_sha256", &plane.mean_sha256),
        ("records_sha256", &plane.records_sha256),
        ("sq8_sha256", &plane.sq8_sha256),
    ] {
        require(
            valid_sha(sha) && d[name] == *sha,
            "original Discovery/plane source binding",
        )?;
        source.insert(name.into(), json!(sha));
    }
    fields(&g["canonical"], "object_key sha256 bytes rows dimensions")?;
    let canonical = &g["canonical"];
    require(
        canonical["rows"].as_u64() == Some(geometry.rows as u64)
            && canonical["dimensions"].as_u64() == Some(geometry.dimensions as u64)
            && canonical["bytes"].as_u64()
                == Some(times(
                    geometry.rows as u64,
                    plus(8, times(4, geometry.dimensions as u64)?)?,
                )?)
            && canonical["sha256"].as_str().is_some_and(valid_sha)
            && canonical["object_key"]
                .as_str()
                .zip(canonical["sha256"].as_str())
                .is_some_and(|(key, sha)| neighborhood_object_key(key, sha)),
        "original canonical descriptor geometry/types",
    )?;
    for (name, positive) in [("low", false), ("step", true)] {
        let values: Vec<f32> = serde_json::from_value(g[name].clone())?;
        require(
            values.len() == geometry.dimensions
                && values
                    .iter()
                    .all(|n| n.is_finite() && (!positive || *n > 0.)),
            "original calibration finite geometry",
        )?;
    }
    let authority = ConstructionAuthority {
        approved: a,
        root,
        membership,
        leaves,
        source: Value::Object(source),
        metadata: vec![cs, approved_snapshot, gs, ps],
        geometry,
        metadata_charge,
        max_payload_bytes: c.max_payload_bytes,
    };
    authority.recheck()?;
    Ok((c, authority))
}

fn stream_snapshot(pin: &CompletedInput) -> Result<HeaderSnapshot> {
    let file = header_file(&pin.path)?;
    let identity = file_identity(&file)?;
    require(identity.len == pin.bytes, "stream exact descriptor length")?;
    Ok(HeaderSnapshot {
        file,
        path: pin.path.clone(),
        identity,
    })
}

fn authenticate_stream(pin: &CompletedInput) -> Result<HeaderSnapshot> {
    let mut snapshot = stream_snapshot(pin)?;
    let mut sha = Sha256::new();
    let mut block = [0; STREAM_BYTES];
    let mut remaining = pin.bytes;
    while remaining > 0 {
        let n = usize::try_from(remaining.min(STREAM_BYTES as u64))?;
        snapshot.file.read_exact(&mut block[..n])?;
        sha.update(&block[..n]);
        remaining -= n as u64;
    }
    require(
        snapshot.file.read(&mut block[..1])? == 0 && format!("{:x}", sha.finalize()) == pin.sha256,
        "stream whole SHA/exact EOF",
    )?;
    snapshot.recheck()?;
    snapshot.file.rewind()?;
    Ok(snapshot)
}

fn reconstruct_centroids(
    a: &ConstructionAuthority,
    pin: &CompletedInput,
) -> Result<ReconstructedCentroids> {
    a.recheck()?;
    match_pin(pin, &a.leaves)?;
    let g = a.geometry;
    let record_bytes = usize::try_from(plus(4, times(2, g.dimensions as u64)?)?)?;
    let seen_bytes = g.units.div_ceil(8);
    let reconstruction_charge = admission(
        a.max_payload_bytes,
        &[
            a.metadata_charge,
            g.blob_bytes as u64,
            seen_bytes as u64,
            record_bytes as u64,
        ],
    )?;
    let mut leaf = stream_snapshot(pin)?;
    let mut refs = a.metadata.iter().collect::<Vec<_>>();
    refs.push(&leaf);
    recheck_distinct(&refs)?;
    drop(refs);
    let mut blob = vec![0; g.blob_bytes];
    blob[..8].copy_from_slice(b"BORSUCP1");
    blob[8..16].copy_from_slice(&(g.rows as u64).to_le_bytes());
    for (offset, word) in [(16, g.dimensions as u32), (20, 32), (24, 256)] {
        blob[offset..offset + 4].copy_from_slice(&word.to_le_bytes());
    }
    let mut seen = vec![0_u8; seen_bytes];
    let mut record = vec![0_u8; record_bytes];
    let mut sha = Sha256::new();
    for _ in 0..g.units {
        leaf.file.read_exact(&mut record)?;
        sha.update(&record);
        let unit = u32::from_le_bytes(record[..4].try_into()?) as usize;
        require(unit < g.units, "physical centroid unit ID range")?;
        let bit = 1 << (unit % 8);
        require(
            seen[unit / 8] & bit == 0,
            "duplicate physical centroid unit ID",
        )?;
        seen[unit / 8] |= bit;
        require(
            record[4..]
                .chunks_exact(2)
                .all(|w| u16::from_le_bytes([w[0], w[1]]) & 0x7c00 != 0x7c00),
            "nonfinite FP16 centroid",
        )?;
        let start = 32 + unit * g.dimensions * 2;
        blob[start..start + g.dimensions * 2].copy_from_slice(&record[4..]);
    }
    require(
        leaf.file.read(&mut record[..1])? == 0,
        "centroid leaves exact EOF",
    )?;
    require(
        (0..g.units).all(|u| seen[u / 8] & (1 << (u % 8)) != 0),
        "complete physical centroid roster",
    )?;
    require(
        format!("{:x}", sha.finalize()) == a.leaves.sha256,
        "original complete leaf SHA",
    )?;
    require(
        digest(&blob) == a.approved.centroids_sha256,
        "original whole construction SHA",
    )?;
    preflight(&blob, g.rows, g.dimensions, a.approved.profile)?;
    a.recheck()?;
    leaf.recheck()?;
    // seen/record are dropped here; builder admission already includes the one retained blob.
    Ok(ReconstructedCentroids {
        blob,
        leaf,
        generation_sha256: a.approved.generation_sha256.clone(),
        reconstruction_charge,
    })
}

fn compare_stream(snapshot: &mut HeaderSnapshot, expected: &[u8], root: bool) -> Result<()> {
    require(
        snapshot.identity.len == expected.len() as u64,
        "historical parity exact length",
    )?;
    snapshot.recheck()?;
    snapshot.file.rewind()?;
    let mut block = [0; STREAM_BYTES];
    let mut offset = 0;
    while offset < expected.len() {
        let n = (expected.len() - offset).min(STREAM_BYTES);
        snapshot.file.read_exact(&mut block[..n])?;
        for (i, &old) in block[..n].iter().enumerate() {
            let at = offset + i;
            let new = expected[at];
            let valid = if root && at < 8 {
                old == b"BORSUSR2"[at] && new == b"BORSUSR3"[at]
            } else if root && (88..92).contains(&at) {
                old == 0 && new == [1, 0, 0, 0][at - 88]
            } else {
                old == new
            };
            require(valid, "forbidden historical radial parity byte/literal")?;
        }
        offset += n;
    }
    require(
        snapshot.file.read(&mut block[..1])? == 0,
        "historical parity EOF",
    )?;
    snapshot.recheck()
}

struct HistoricalParity {
    artifacts: RouterArtifacts,
    snapshots: [HeaderSnapshot; 3],
    old_allocation_limit: usize,
    builder_estimate: usize,
    experiment_charge: u64,
}

fn verify_historical_radial(
    a: &ConstructionAuthority,
    reconstructed: &ReconstructedCentroids,
    pins: &HistoricalPins,
    leaves: &CompletedInput,
) -> Result<HistoricalParity> {
    a.recheck()?;
    reconstructed.leaf.recheck()?;
    require(
        reconstructed.generation_sha256 == a.approved.generation_sha256
            && digest(&reconstructed.blob) == a.approved.centroids_sha256,
        "immutable original reconstruction authority",
    )?;
    match_pin(&pins.root, &a.root)?;
    match_pin(&pins.membership, &a.membership)?;
    match_pin(leaves, &a.leaves)?;
    let mut root = authenticate_stream(&pins.root)?;
    let mut membership = authenticate_stream(&pins.membership)?;
    let mut payload = authenticate_stream(leaves)?;
    let mut refs = a.metadata.iter().collect::<Vec<_>>();
    refs.extend([&root, &membership, &payload]);
    recheck_distinct(&refs)?;
    drop(refs);
    // Whole original root authentication must precede extraction of construction identity.
    let mut header = [0; 96];
    root.file.read_exact(&mut header)?;
    require(
        &header[..8] == b"BORSUSR2" && header[88..92] == [0; 4],
        "historical root literals",
    )?;
    let old_allocation_limit = usize::try_from(u64::from_le_bytes(header[80..88].try_into()?))?;
    require(
        old_allocation_limit <= a.approved.profile.allocation_cap(),
        "unsupported historical allocation limit",
    )?;
    let builder_estimate = admit(a.geometry, old_allocation_limit, a.approved.profile)?;
    let experiment_charge = admission(
        a.max_payload_bytes,
        &[a.metadata_charge, builder_estimate as u64],
    )?;
    let identity = a.identity();
    let artifacts = build(&reconstructed.blob, &identity, old_allocation_limit)?;
    // Validation/open apply only to actual current-builder artifacts, never patched historical bytes.
    validate_publication(
        &artifacts.manifest,
        &artifacts.membership,
        &artifacts.leaves,
        &identity,
        &reconstructed.blob,
    )?;
    let router = SemanticUnitRouter::open(
        &artifacts.manifest,
        &artifacts.membership,
        &digest(&artifacts.manifest),
        &identity,
        a.approved.profile.root_cap(),
    )?;
    require(
        router.manifest().modeled_allocation_limit_bytes == old_allocation_limit,
        "unchanged historical construction limit",
    )?;
    drop(router);
    compare_stream(&mut root, &artifacts.manifest, true)?;
    compare_stream(&mut membership, &artifacts.membership, false)?;
    compare_stream(&mut payload, &artifacts.leaves, false)?;
    a.recheck()?;
    reconstructed.leaf.recheck()?;
    Ok(HistoricalParity {
        artifacts,
        snapshots: [root, membership, payload],
        old_allocation_limit,
        builder_estimate,
        experiment_charge,
    })
}

fn publish_blob(
    output: &Path,
    body: &[u8],
    inputs: &[&HeaderSnapshot],
    sync: &mut impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<HeaderSnapshot> {
    recheck_distinct(inputs)?;
    let dir = parent(output)?;
    let original_dir = dir.metadata()?;
    let mut file = File::from(openat(
        &dir,
        output.file_name().ok_or("construction filename")?,
        OFlags::RDWR
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    let created = file_identity(&file)?;
    file.write_all(body)?;
    let identity = file_identity(&file)?;
    require(
        identity.dev == created.dev
            && identity.ino == created.ino
            && identity.len == body.len() as u64,
        "construction output descriptor",
    )?;
    sync(&file, false)?;
    require(
        file_identity(&file)? == identity,
        "construction output changed during file sync",
    )?;
    let mut snapshot = HeaderSnapshot {
        file,
        path: output.to_owned(),
        identity,
    };
    compare_stream(&mut snapshot, body, false)?;
    recheck_distinct(inputs)?;
    sync(&dir, true)?;
    snapshot.recheck()?;
    let current_dir = parent(output)?.metadata()?;
    require(
        original_dir.dev() == current_dir.dev() && original_dir.ino() == current_dir.ino(),
        "construction output parent replacement",
    )?;
    recheck_distinct(inputs)?;
    Ok(snapshot)
}

fn publish_with_sync(
    output: &Path,
    a: &ConstructionAuthority,
    reconstructed: &ReconstructedCentroids,
    parity: Option<&HistoricalParity>,
    binary: &HeaderSnapshot,
    mut sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<bool> {
    header_path(output)?;
    let mut inputs = a.metadata.iter().collect::<Vec<_>>();
    inputs.extend([&reconstructed.leaf, binary]);
    if let Some(p) = parity {
        // The reconstructed leaf and historical payload share the same authenticated role.
        inputs.extend([&p.snapshots[0], &p.snapshots[1]]);
        p.snapshots[2].recheck()?;
    }
    recheck_distinct(&inputs)?;
    let parent_dir = parent(output)?;
    let parent_stamp = parent_dir.metadata()?;
    let name = output.file_name().ok_or("construction directory name")?;
    rustix::fs::mkdirat(&parent_dir, name, Mode::RUSR | Mode::WUSR | Mode::XUSR)?;
    let output_dir = directory(output)?;
    let output_stamp = output_dir.metadata()?;
    sync(&output_dir, true)?;
    sync(&parent_dir, true)?;
    let recheck_directory = || -> Result<()> {
        let now = directory(output)?.metadata()?;
        let parent_now = parent(output)?.metadata()?;
        require(
            now.dev() == output_stamp.dev()
                && now.ino() == output_stamp.ino()
                && parent_now.dev() == parent_stamp.dev()
                && parent_now.ino() == parent_stamp.ino(),
            "construction directory identity",
        )
    };
    recheck_directory()?;
    let mut outputs = vec![publish_blob(
        &output.join("centroids.bin"),
        &reconstructed.blob,
        &inputs,
        &mut sync,
    )?];
    if let Some(p) = parity {
        for (name, body) in [
            ("radial-root.bin", &p.artifacts.manifest),
            ("radial-membership.bin", &p.artifacts.membership),
            ("radial-leaves.bin", &p.artifacts.leaves),
        ] {
            recheck_directory()?;
            outputs.push(publish_blob(&output.join(name), body, &inputs, &mut sync)?);
        }
    }
    inputs.extend(outputs.iter());
    let descriptor =
        |s: &HeaderSnapshot, sha: &str| json!({"path":s.path,"bytes":s.identity.len,"sha256":sha});
    let parity_receipt = parity.map(|p| json!({
        "old_root":a.root,"old_membership":a.membership,"old_leaves":a.leaves,
        "radial_root_sha256":digest(&p.artifacts.manifest),
        "radial_membership_sha256":digest(&p.artifacts.membership),
        "radial_leaves_sha256":digest(&p.artifacts.leaves),
        "old_allocation_limit_bytes":p.old_allocation_limit,"builder_peak_bytes":p.builder_estimate,
        "experiment_charge_bytes":p.experiment_charge,
        "root_exceptions":[{"offset":0,"old":"BORSUSR2","new":"BORSUSR3"},{"offset":88,"old_le_u32":0,"new_le_u32":1}],
    }));
    let receipt = json!({
        "schema":RECEIPT_SCHEMA,"status":if parity.is_some(){"HISTORICAL_RADIAL_PARITY"}else{"RECONSTRUCTED"},
        "complete":true,"requests_opened":false,"truth_opened":false,
        "historical_root_opened":parity.is_some(),"historical_membership_opened":parity.is_some(),
        "config":descriptor(&a.metadata[0], &digest_file_snapshot(&a.metadata[0])?),
        "approval":descriptor(&a.metadata[1], &digest_file_snapshot(&a.metadata[1])?),
        "generation":descriptor(&a.metadata[2], &a.approved.generation_sha256),
        "plane":descriptor(&a.metadata[3], &a.approved.plane_sha256),
        "binary":descriptor(binary, &digest_file_snapshot(binary)?),
        "implementation":implementation_hashes(),"original":a.approved,"source":a.source,
        "leaves":descriptor(&reconstructed.leaf, &a.leaves.sha256),
        "unit_rows":32,"page_rows":256,"unit_count":a.geometry.units,
        "final_unit_rows":a.geometry.rows - (a.geometry.units - 1) * 32,
        "centroids_bytes":reconstructed.blob.len(),"centroids_sha256":a.approved.centroids_sha256,
        "centroids":descriptor(&outputs[0], &a.approved.centroids_sha256),
        "radial_outputs":parity.map(|p| json!({
            "root":descriptor(&outputs[1], &digest(&p.artifacts.manifest)),
            "membership":descriptor(&outputs[2], &digest(&p.artifacts.membership)),
            "leaves":descriptor(&outputs[3], &digest(&p.artifacts.leaves)),
        })),
        "metadata_charge_bytes":a.metadata_charge,"reconstruction_charge_bytes":reconstructed.reconstruction_charge,
        "max_payload_bytes":a.max_payload_bytes,"historical_parity":parity_receipt,
        "requires_successful_file_sync":true,"requires_successful_directory_sync":true,
        "requires_original_successful_terminal_receipt":true,
    });
    recheck_directory()?;
    // The existing writer invalidates its owned receipt descriptor on ANY sync/recheck failure.
    publish_selection_seal_with_sync(
        &output.join("receipt.json"),
        &receipt,
        &inputs,
        |file, is_dir| {
            if is_dir {
                sync(file, true)?;
                sync(&parent_dir, true)?;
            } else {
                let identity = file_identity(file).map_err(io::Error::other)?;
                sync(file, false)?;
                require(
                    file_identity(file).map_err(io::Error::other)? == identity,
                    "construction receipt changed during file sync",
                )
                .map_err(io::Error::other)?;
            }
            recheck_directory().map_err(io::Error::other)?;
            if let Some(p) = parity {
                p.snapshots[2].recheck().map_err(io::Error::other)?;
            }
            Ok(())
        },
    )?;
    Ok(true)
}

fn digest_file_snapshot(snapshot: &HeaderSnapshot) -> Result<String> {
    snapshot.recheck()?;
    let mut file = snapshot.file.try_clone()?;
    file.rewind()?;
    let mut sha = Sha256::new();
    let mut block = [0; STREAM_BYTES];
    let mut remaining = snapshot.identity.len;
    while remaining > 0 {
        let n = usize::try_from(remaining.min(STREAM_BYTES as u64))?;
        file.read_exact(&mut block[..n])?;
        sha.update(&block[..n]);
        remaining -= n as u64;
    }
    require(file.read(&mut block[..1])? == 0, "receipt source exact EOF")?;
    snapshot.recheck()?;
    Ok(format!("{:x}", sha.finalize()))
}

fn implementation_hashes() -> Value {
    json!({
        "example_sha256":digest(include_bytes!("../compare_native_replay.rs")),
        "module_sha256":digest(include_bytes!("partition_reconstruction.rs")),
        "router_sha256":digest(include_bytes!("../../src/semantic_unit_router.rs")),
        "generation_sha256":digest(include_bytes!("../../src/two_bit_generation.rs")),
        "plane_sha256":digest(include_bytes!("../../src/two_bit_source.rs")),
        "centroid_sha256":digest(include_bytes!("../../src/unit_centroid_pages.rs")),
        "trainer_sha256":digest(include_bytes!("../../src/logical_cell_catalog.rs")),
    })
}

pub(super) fn execute(
    config_path: &Path,
    config_sha: &str,
    approval_path: &Path,
    approval_sha: &str,
) -> Result<bool> {
    let (c, a) = authenticate_construction(config_path, config_sha, approval_path, approval_sha)?;
    let reconstructed = reconstruct_centroids(&a, &c.leaves)?;
    let parity = c
        .historical
        .as_ref()
        .map(|h| verify_historical_radial(&a, &reconstructed, h, &c.leaves))
        .transpose()?;
    let binary_path = std::env::current_exe()?;
    let binary_file = header_file(&binary_path)?;
    let identity = file_identity(&binary_file)?;
    require(
        identity.len > 0 && identity.len <= 256 * 1024 * 1024,
        "bounded reconstruction binary",
    )?;
    let binary = HeaderSnapshot {
        file: binary_file,
        path: binary_path,
        identity,
    };
    publish_with_sync(
        &c.output_directory,
        &a,
        &reconstructed,
        parity.as_ref(),
        &binary,
        |file, _| file.sync_all(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROWS: usize = 4097;
    const D: usize = 1024;
    const U: usize = 129;
    const RECORD: usize = 4 + 2 * D;

    fn put(path: &Path, body: &[u8]) -> CompletedInput {
        std::fs::write(path, body).unwrap();
        CompletedInput {
            path: path.to_owned(),
            bytes: body.len() as u64,
            sha256: digest(body),
        }
    }

    fn original_blob() -> Vec<u8> {
        let mut blob = vec![0; 32 + U * D * 2];
        blob[..8].copy_from_slice(b"BORSUCP1");
        blob[8..16].copy_from_slice(&(ROWS as u64).to_le_bytes());
        for (offset, word) in [(16, D as u32), (20, 32), (24, 256)] {
            blob[offset..offset + 4].copy_from_slice(&word.to_le_bytes());
        }
        for unit in 0..U {
            for (coordinate, bits) in [
                (0, half::f16::from_f32(unit as f32 / 32. - 2.).to_bits()),
                (1, half::f16::from_f32((unit % 7) as f32 / 8.).to_bits()),
                (2, 0x8000),
                (3, 0x0001),
                (4, 0x8001),
                (5, 0x7bff),
                (6, 0xfbff),
            ] {
                let at = 32 + 2 * (unit * D + coordinate);
                blob[at..at + 2].copy_from_slice(&bits.to_le_bytes());
            }
        }
        blob
    }

    struct Fixture {
        dir: tempfile::TempDir,
        blob: Vec<u8>,
        config: ReconstructionConfig,
        approval: OriginalApproval,
        generation: Value,
        config_pin: CompletedInput,
        approval_pin: CompletedInput,
        binary: CompletedInput,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let blob = original_blob();
            let centroid_sha = digest(&blob);
            let input_root = "1".repeat(64);
            let old = frozen_old_builder::build(
                &blob,
                &frozen_old_builder::SourceIdentity {
                    profile: frozen_old_builder::SemanticProfile::Scale1m,
                    schema: "frozen-old-reconstruction-fixture",
                    root_sha256: &input_root,
                    centroids_sha256: &centroid_sha,
                    rows: ROWS,
                    dimensions: D,
                },
                64 * 1024 * 1024,
            )
            .unwrap();
            assert_eq!(&old.manifest[..8], b"BORSUSR2");
            assert_eq!(&old.manifest[88..92], &[0; 4]);
            let root = put(&dir.path().join("old-root.bin"), &old.manifest);
            let membership = put(&dir.path().join("old-membership.bin"), &old.membership);
            let leaves = put(&dir.path().join("old-leaves.bin"), &old.leaves);
            let plane = json!({"schema":"borsuk-two-bit-plane-v3","rows":ROWS,"dimensions":D,
                "seed":20260923,"record_bytes":source_record_bytes(D).unwrap(),
                "source_sha256":"2".repeat(64),"sq8_sha256":"3".repeat(64),
                "source_order_sha256":"4".repeat(64),"mean_sha256":"5".repeat(64),
                "records_sha256":"6".repeat(64),"page_rows":32,
                "page_digest_sha256":"7".repeat(64),"query_or_truth_used":false});
            let plane_pin = put(
                &dir.path().join("plane.json"),
                &serde_json::to_vec(&plane).unwrap(),
            );
            let generation = json!({"schema":"borsuk-two-bit-generation-v8","generation":41,"base_epoch":17,
                "plane_manifest_sha256":plane_pin.sha256,"page_manifest_sha256":"8".repeat(64),
                "discovery":{"mode":"semantic","profile":"scale1m",
                    "root_sha256":root.sha256,"root_bytes":root.bytes,
                    "membership_sha256":membership.sha256,"membership_bytes":membership.bytes,
                    "leaves_sha256":leaves.sha256,"leaves_bytes":leaves.bytes,
                    "input_schema":"frozen-old-reconstruction-fixture","input_root_sha256":input_root,
                    "centroids_sha256":centroid_sha,
                    "source_sha256":plane["source_sha256"],"source_order_sha256":plane["source_order_sha256"],
                    "mean_sha256":plane["mean_sha256"],"records_sha256":plane["records_sha256"],
                    "sq8_sha256":plane["sq8_sha256"]},
                "sq8_object_sha256":plane["sq8_sha256"],"sq8_object_key":format!("semantic/objects/{}", "3".repeat(64)),
                "sq8_etag":"frozen-fixture","canonical":{"object_key":format!("semantic/objects/{}", "9".repeat(64)),
                    "sha256":"9".repeat(64),"bytes":ROWS*(8+4*D),"rows":ROWS,"dimensions":D},
                "low":vec![-1.;D],"step":vec![0.125;D]});
            let generation_pin = put(
                &dir.path().join("generation.json"),
                &serde_json::to_vec(&generation).unwrap(),
            );
            let approval = OriginalApproval {
                schema: APPROVAL_SCHEMA.into(),
                generation_sha256: generation_pin.sha256.clone(),
                generation: 41,
                base_epoch: 17,
                plane_sha256: plane_pin.sha256.clone(),
                discovery_sha256: digest(&serde_json::to_vec(&generation["discovery"]).unwrap()),
                input_schema: "frozen-old-reconstruction-fixture".into(),
                input_root_sha256: input_root,
                centroids_sha256: centroid_sha,
                profile: SemanticProfile::Scale1m,
                rows: ROWS,
                dimensions: D,
            };
            let config = ReconstructionConfig {
                schema: CONFIG_SCHEMA.into(),
                generation_manifest: generation_pin,
                plane_manifest: plane_pin,
                leaves,
                historical: Some(HistoricalPins { root, membership }),
                caller_pinned_bytes: 4096,
                max_payload_bytes: 64 * 1024 * 1024,
                output_directory: dir.path().join("published"),
            };
            let config_pin = put(
                &dir.path().join("config.json"),
                &serde_json::to_vec(&config).unwrap(),
            );
            let approval_pin = put(
                &dir.path().join("approval.json"),
                &serde_json::to_vec(&approval).unwrap(),
            );
            let binary = put(
                &dir.path().join("binary"),
                b"independently pinned test executable stand-in",
            );
            Self {
                dir,
                blob,
                config,
                approval,
                generation,
                config_pin,
                approval_pin,
                binary,
            }
        }

        fn save(&mut self) {
            self.config.generation_manifest = put(
                &self.config.generation_manifest.path,
                &serde_json::to_vec(&self.generation).unwrap(),
            );
            self.approval.generation_sha256 = self.config.generation_manifest.sha256.clone();
            self.approval.discovery_sha256 =
                digest(&serde_json::to_vec(&self.generation["discovery"]).unwrap());
            self.config_pin = put(
                &self.config_pin.path,
                &serde_json::to_vec(&self.config).unwrap(),
            );
            self.approval_pin = put(
                &self.approval_pin.path,
                &serde_json::to_vec(&self.approval).unwrap(),
            );
        }

        fn auth(&self) -> Result<(ReconstructionConfig, ConstructionAuthority)> {
            authenticate_construction(
                &self.config_pin.path,
                &self.config_pin.sha256,
                &self.approval_pin.path,
                &self.approval_pin.sha256,
            )
        }

        fn leaf_body(&mut self, body: &[u8]) {
            self.config.leaves = put(&self.config.leaves.path, body);
            self.generation["discovery"]["leaves_bytes"] = json!(self.config.leaves.bytes);
            self.generation["discovery"]["leaves_sha256"] = json!(self.config.leaves.sha256);
            self.save();
        }

        fn plane_change(&mut self, change: impl FnOnce(&mut Value)) {
            let mut plane: Value =
                serde_json::from_slice(&std::fs::read(&self.config.plane_manifest.path).unwrap())
                    .unwrap();
            change(&mut plane);
            self.config.plane_manifest = put(
                &self.config.plane_manifest.path,
                &serde_json::to_vec(&plane).unwrap(),
            );
            self.approval.plane_sha256 = self.config.plane_manifest.sha256.clone();
            self.generation["plane_manifest_sha256"] = json!(self.config.plane_manifest.sha256);
            self.save();
        }
    }

    #[test]
    fn partition_reconstruction_frozen_old_4097_d1024_radial_parity() {
        // These dependencies are shared ONLY because exact frozen source bytes match.
        assert_eq!(
            digest(include_bytes!("../../src/unit_centroid_pages.rs")),
            "116e79911f83ec5631c1b392f5a6b54f54c1ce33255d9c0340a106c7397eb307"
        );
        assert_eq!(
            digest(include_bytes!("../../src/logical_cell_catalog.rs")),
            "7460bfe2025bd80f5df55b36988cdd4e588b5d617d93aea5a47bff24d9c59225"
        );
        assert_eq!(
            digest(include_bytes!("../../src/metric.rs")),
            "43a9c3a3fbc9a1f017a7ad7ce131aac7f253a77cb418f451587bd1043a1cdabe"
        );
        assert_eq!(
            digest(include_bytes!("../../src/simd_control.rs")),
            "82bf82bed5167662439160cb0ea8b3db2870e8322c196e50f439ddcadc514b56"
        );
        assert_eq!(
            digest(include_bytes!("../../src/lib.rs")),
            "40be49046e4f292c5c62101c8e2e08333e1a6c6d6715cf1c8d9c5da661aaefa8"
        );
        assert_eq!(
            digest(include_bytes!("../../../../Cargo.lock")),
            "81d59b7f67df2a2269974028eeaaffbec29b482abb1a0e8f9e10be63263f055e"
        );
        let f = Fixture::new();
        let (_, a) = f.auth().unwrap();
        let r = reconstruct_centroids(&a, &f.config.leaves).unwrap();
        assert_eq!(r.blob, f.blob);
        assert_eq!(a.geometry.units, U);
        assert_eq!(ROWS - 32 * (U - 1), 1);
        assert_eq!(
            &r.blob[32 + 4..32 + 14],
            &[0, 128, 1, 0, 1, 128, 255, 123, 255, 251]
        );
        let p = verify_historical_radial(
            &a,
            &r,
            f.config.historical.as_ref().unwrap(),
            &f.config.leaves,
        )
        .unwrap();
        assert_eq!(p.old_allocation_limit, 64 * 1024 * 1024);
        let mut raw_old = std::fs::read(&f.config.historical.as_ref().unwrap().root.path).unwrap();
        assert_eq!(&raw_old[..8], b"BORSUSR2");
        // Production remains BORSUSR3-only; historical evidence never enters its decoder.
        assert!(
            SemanticUnitRouter::open(
                &raw_old,
                &p.artifacts.membership,
                &digest(&raw_old),
                &a.identity(),
                a.approved.profile.root_cap()
            )
            .is_err()
        );
        raw_old.clear();
        let binary = authenticate_stream(&f.binary).unwrap();
        assert!(
            publish_with_sync(
                &f.config.output_directory,
                &a,
                &r,
                Some(&p),
                &binary,
                |file, _| file.sync_all()
            )
            .unwrap()
        );
        let receipt: Value = serde_json::from_slice(
            &std::fs::read(f.config.output_directory.join("receipt.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(receipt["status"], "HISTORICAL_RADIAL_PARITY");
        assert_eq!(receipt["requests_opened"], false);
        assert_eq!(receipt["truth_opened"], false);
        assert_eq!(
            std::fs::read(f.config.output_directory.join("centroids.bin")).unwrap(),
            f.blob
        );
    }

    #[test]
    fn partition_reconstruction_absent_centroid_root_membership_and_permuted_records() {
        let mut f = Fixture::new();
        let centroid = f.dir.path().join("deleted-original-centroids.bin");
        std::fs::write(&centroid, &f.blob).unwrap();
        std::fs::remove_file(centroid).unwrap();
        let h = f.config.historical.take().unwrap();
        let old_root = std::fs::read(&h.root.path).unwrap();
        let old_member = std::fs::read(&h.membership.path).unwrap();
        std::fs::remove_file(&h.root.path).unwrap();
        std::fs::remove_file(&h.membership.path).unwrap();
        f.save();
        let (_, a) = f.auth().unwrap();
        let r = reconstruct_centroids(&a, &f.config.leaves).unwrap();
        assert_eq!(r.blob, f.blob);
        assert!(verify_historical_radial(&a, &r, &h, &f.config.leaves).is_err());
        let binary = authenticate_stream(&f.binary).unwrap();
        publish_with_sync(
            &f.config.output_directory,
            &a,
            &r,
            None,
            &binary,
            |file, _| file.sync_all(),
        )
        .unwrap();
        let receipt: Value = serde_json::from_slice(
            &std::fs::read(f.config.output_directory.join("receipt.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(receipt["status"], "RECONSTRUCTED");
        assert_eq!(receipt["historical_root_opened"], false);
        assert_eq!(receipt["historical_membership_opened"], false);
        put(&h.root.path, &old_root);
        put(&h.membership.path, &old_member);
        let original_leaves = std::fs::read(&f.config.leaves.path).unwrap();
        let mut permuted = Vec::with_capacity(original_leaves.len());
        for record in original_leaves.chunks_exact(RECORD).rev() {
            permuted.extend_from_slice(record);
        }
        assert_ne!(permuted, original_leaves);
        f.leaf_body(&permuted);
        let (_, new_a) = f.auth().unwrap();
        let new_r = reconstruct_centroids(&new_a, &f.config.leaves).unwrap();
        assert_eq!(new_r.blob, f.blob);
        assert!(verify_historical_radial(&new_a, &new_r, &h, &f.config.leaves).is_err());
        assert!(verify_historical_radial(&a, &new_r, &h, &f.config.leaves).is_err());
    }

    #[test]
    fn partition_reconstruction_strict_approval_metadata_and_caps() {
        for case in [
            "generation",
            "epoch",
            "plane",
            "discovery",
            "construction",
            "geometry",
            "schema",
            "source",
            "calibration",
            "descriptor",
            "root_geometry",
            "generation_type",
            "canonical_type",
            "plane_queries",
            "plane_record_geometry",
            "plane_digest",
            "cap",
            "cap_above",
            "overflow",
        ] {
            let mut f = Fixture::new();
            match case {
                "generation" => f.approval.generation_sha256 = "0".repeat(64),
                "epoch" => f.approval.base_epoch += 1,
                "plane" => f.approval.plane_sha256 = "0".repeat(64),
                "discovery" => f.approval.discovery_sha256 = "0".repeat(64),
                "construction" => f.approval.centroids_sha256 = "0".repeat(64),
                "geometry" => f.approval.rows += 1,
                "schema" => f.approval.schema = "unknown".into(),
                "source" => {
                    f.generation["discovery"]["source_sha256"] = json!("0".repeat(64));
                    f.save();
                }
                "calibration" => {
                    f.generation["step"][0] = json!(0);
                    f.save();
                }
                "descriptor" => {
                    f.generation["discovery"]["membership_bytes"] = json!(513);
                    f.save();
                }
                "root_geometry" => {
                    f.generation["discovery"]["root_bytes"] = json!(513);
                    f.save();
                }
                "generation_type" => {
                    f.generation["generation"] = json!(41.0);
                    f.save();
                }
                "canonical_type" => {
                    f.generation["canonical"]["rows"] = json!(ROWS.to_string());
                    f.save();
                }
                "plane_queries" => f.plane_change(|p| p["query_or_truth_used"] = json!(true)),
                "plane_record_geometry" => f.plane_change(|p| p["record_bytes"] = json!(1)),
                "plane_digest" => f.plane_change(|p| p["page_digest_sha256"] = json!("invalid")),
                "cap" => f.config.max_payload_bytes = 1,
                "cap_above" => f.config.max_payload_bytes = EXPERIMENT_CAP + 1,
                "overflow" => f.config.caller_pinned_bytes = u64::MAX,
                _ => unreachable!(),
            }
            f.config_pin = put(&f.config_pin.path, &serde_json::to_vec(&f.config).unwrap());
            f.approval_pin = put(
                &f.approval_pin.path,
                &serde_json::to_vec(&f.approval).unwrap(),
            );
            assert!(f.auth().is_err(), "{case}");
        }
        assert!(plus(u64::MAX, 1).is_err());
        assert!(times(u64::MAX, 2).is_err());
        let mut f = Fixture::new();
        let (_, a) = f.auth().unwrap();
        f.config.max_payload_bytes = a.metadata_charge;
        f.save();
        let (_, a) = f.auth().unwrap();
        assert!(reconstruct_centroids(&a, &f.config.leaves).is_err());
    }

    #[test]
    fn partition_reconstruction_rejects_duplicate_keys_unknowns_and_wrong_types() {
        for role in ["config", "approval", "generation", "plane"] {
            for case in [
                "duplicate",
                "nested_duplicate",
                "unknown",
                "type",
                "missing",
            ] {
                let mut f = Fixture::new();
                let pin = match role {
                    "config" => &f.config_pin,
                    "approval" => &f.approval_pin,
                    "generation" => &f.config.generation_manifest,
                    "plane" => &f.config.plane_manifest,
                    _ => unreachable!(),
                };
                let mut body = String::from_utf8(std::fs::read(&pin.path).unwrap()).unwrap();
                match case {
                    "duplicate" => body = body.replacen('{', "{\"dup\":0,\"dup\":1,", 1),
                    "nested_duplicate" => {
                        body = body.replacen('{', "{\"dup\":{\"key\":0,\"key\":1},", 1)
                    }
                    "unknown" => body = body.replacen('{', "{\"unknown\":0,", 1),
                    "type" | "missing" => {
                        let mut v: Value = serde_json::from_str(&body).unwrap();
                        if case == "type" {
                            v["schema"] = json!(1);
                        } else {
                            v.as_object_mut().unwrap().remove("schema");
                        }
                        body = serde_json::to_string(&v).unwrap();
                    }
                    _ => unreachable!(),
                }
                let new_pin = put(&pin.path, body.as_bytes());
                match role {
                    "config" => f.config_pin = new_pin,
                    "approval" => f.approval_pin = new_pin,
                    "generation" => {
                        f.config.generation_manifest = new_pin;
                        f.approval.generation_sha256 = f.config.generation_manifest.sha256.clone();
                        f.config_pin =
                            put(&f.config_pin.path, &serde_json::to_vec(&f.config).unwrap());
                        f.approval_pin = put(
                            &f.approval_pin.path,
                            &serde_json::to_vec(&f.approval).unwrap(),
                        );
                    }
                    "plane" => {
                        f.config.plane_manifest = new_pin;
                        f.approval.plane_sha256 = f.config.plane_manifest.sha256.clone();
                        f.generation["plane_manifest_sha256"] =
                            json!(f.config.plane_manifest.sha256);
                        f.save();
                    }
                    _ => unreachable!(),
                }
                assert!(f.auth().is_err(), "{role}/{case}");
            }
        }
    }

    #[test]
    fn partition_reconstruction_rejects_ids_nonfinite_sha_length_header() {
        for case in [
            "duplicate",
            "range",
            "missing",
            "nan",
            "negative_nan",
            "infinity",
            "negative_infinity",
            "bad_sha",
            "truncated",
            "grown",
            "bad_header",
            "bad_reserved",
        ] {
            let mut f = Fixture::new();
            let mut leaves = std::fs::read(&f.config.leaves.path).unwrap();
            match case {
                "duplicate" => {
                    let id = leaves[RECORD..RECORD + 4].to_vec();
                    leaves[..4].copy_from_slice(&id);
                    f.leaf_body(&leaves);
                }
                "range" => {
                    leaves[..4].copy_from_slice(&(U as u32).to_le_bytes());
                    f.leaf_body(&leaves);
                }
                "missing" => {
                    leaves.truncate(leaves.len() - RECORD);
                    f.leaf_body(&leaves);
                }
                "nan" | "negative_nan" | "infinity" | "negative_infinity" => {
                    let bits: u16 = match case {
                        "nan" => 0x7e00,
                        "negative_nan" => 0xfe01,
                        "infinity" => 0x7c00,
                        _ => 0xfc00,
                    };
                    leaves[4..6].copy_from_slice(&bits.to_le_bytes());
                    f.leaf_body(&leaves);
                }
                "bad_sha" => {
                    leaves[4] ^= 1;
                    std::fs::write(&f.config.leaves.path, &leaves).unwrap();
                }
                "truncated" => {
                    leaves.pop();
                    std::fs::write(&f.config.leaves.path, &leaves).unwrap();
                }
                "grown" => {
                    leaves.push(0);
                    std::fs::write(&f.config.leaves.path, &leaves).unwrap();
                }
                "bad_header" | "bad_reserved" => {
                    f.blob[if case == "bad_header" { 0 } else { 28 }] ^= 1;
                    f.approval.centroids_sha256 = digest(&f.blob);
                    f.generation["discovery"]["centroids_sha256"] =
                        json!(f.approval.centroids_sha256);
                    f.save();
                }
                _ => unreachable!(),
            }
            let result = f
                .auth()
                .and_then(|(_, a)| reconstruct_centroids(&a, &f.config.leaves));
            assert!(result.is_err(), "{case}");
        }
    }

    #[test]
    fn partition_reconstruction_every_forbidden_root_byte_and_literal() {
        let f = Fixture::new();
        let (_, a) = f.auth().unwrap();
        let r = reconstruct_centroids(&a, &f.config.leaves).unwrap();
        let p = verify_historical_radial(
            &a,
            &r,
            f.config.historical.as_ref().unwrap(),
            &f.config.leaves,
        )
        .unwrap();
        let mut old = authenticate_stream(&f.config.historical.as_ref().unwrap().root).unwrap();
        let mut current = p.artifacts.manifest.clone();
        for at in 0..current.len() {
            current[at] ^= 1;
            assert!(
                compare_stream(&mut old, &current, true).is_err(),
                "forbidden current byte {at}"
            );
            current[at] ^= 1;
        }
        current.push(0);
        assert!(compare_stream(&mut old, &current, true).is_err());
        current.pop();
        current.pop();
        assert!(compare_stream(&mut old, &current, true).is_err());
        let original = std::fs::read(&old.path).unwrap();
        for at in (0..8).chain(88..92) {
            let mut mutated = original.clone();
            mutated[at] ^= 1;
            let pin = put(&f.dir.path().join("bad-literal-root"), &mutated);
            let mut bad = authenticate_stream(&pin).unwrap();
            assert!(
                compare_stream(&mut bad, &p.artifacts.manifest, true).is_err(),
                "old literal {at}"
            );
        }
    }

    #[test]
    fn partition_reconstruction_historical_corruption_limit_and_retained_budget() {
        for case in [
            "root_sha",
            "directory",
            "prototype",
            "membership",
            "payload",
            "limit_zero",
            "limit_high",
            "retained",
        ] {
            let mut f = Fixture::new();
            match case {
                "root_sha" | "directory" | "prototype" | "limit_zero" | "limit_high" => {
                    let h = f.config.historical.as_mut().unwrap();
                    let mut root = std::fs::read(&h.root.path).unwrap();
                    match case {
                        "root_sha" => root[92] ^= 1,
                        "directory" => root[512] ^= 1,
                        "prototype" => {
                            let last = root.len() - 1;
                            root[last] ^= 1;
                        }
                        "limit_zero" => root[80..88].fill(0),
                        "limit_high" => {
                            root[80..88].copy_from_slice(&((EXPERIMENT_CAP + 1).to_le_bytes()))
                        }
                        _ => unreachable!(),
                    }
                    if case == "root_sha" {
                        std::fs::write(&h.root.path, &root).unwrap();
                    } else {
                        h.root = put(&h.root.path, &root);
                        f.generation["discovery"]["root_sha256"] = json!(h.root.sha256);
                        f.save();
                    }
                }
                "membership" => {
                    let h = f.config.historical.as_mut().unwrap();
                    let mut body = std::fs::read(&h.membership.path).unwrap();
                    body[0] ^= 1;
                    h.membership = put(&h.membership.path, &body);
                    f.generation["discovery"]["membership_sha256"] = json!(h.membership.sha256);
                    f.save();
                }
                "payload" => {
                    let mut body = std::fs::read(&f.config.leaves.path).unwrap();
                    body.rotate_left(RECORD);
                    f.leaf_body(&body);
                }
                "retained" => {
                    let (_, a) = f.auth().unwrap();
                    let estimate = admit(a.geometry, 64 * 1024 * 1024, a.approved.profile).unwrap();
                    f.config.max_payload_bytes = a.metadata_charge + estimate as u64 - 4096;
                    f.save();
                }
                _ => unreachable!(),
            }
            let (_, a) = f.auth().unwrap();
            let r = reconstruct_centroids(&a, &f.config.leaves).unwrap();
            assert!(
                verify_historical_radial(
                    &a,
                    &r,
                    f.config.historical.as_ref().unwrap(),
                    &f.config.leaves
                )
                .is_err(),
                "{case}"
            );
        }
    }

    #[test]
    fn partition_reconstruction_refuses_alias_replacement_and_mutation() {
        use std::os::unix::fs::symlink;
        for case in [
            "symlink",
            "hardlink",
            "parent_symlink",
            "replace_generation",
            "mutate_leaves",
            "replace_leaves",
        ] {
            let f = Fixture::new();
            let (_, a) = f.auth().unwrap();
            let r = reconstruct_centroids(&a, &f.config.leaves).unwrap();
            match case {
                "symlink" => {
                    let target = f.dir.path().join("actual-leaf");
                    std::fs::rename(&f.config.leaves.path, &target).unwrap();
                    symlink(target, &f.config.leaves.path).unwrap();
                }
                "hardlink" => {
                    std::fs::hard_link(&f.config.leaves.path, f.dir.path().join("alias")).unwrap()
                }
                "parent_symlink" => {
                    let link = f.dir.path().join("linked-dir");
                    symlink(f.dir.path(), &link).unwrap();
                    assert!(header_file(&link.join("old-leaves.bin")).is_err());
                    continue;
                }
                "replace_generation" => {
                    let body = std::fs::read(&f.config.generation_manifest.path).unwrap();
                    std::fs::rename(
                        &f.config.generation_manifest.path,
                        f.dir.path().join("retained-gen"),
                    )
                    .unwrap();
                    std::fs::write(&f.config.generation_manifest.path, body).unwrap();
                }
                "mutate_leaves" => {
                    let mut file = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&f.config.leaves.path)
                        .unwrap();
                    file.write_all(&[42]).unwrap();
                }
                "replace_leaves" => {
                    let body = std::fs::read(&f.config.leaves.path).unwrap();
                    std::fs::rename(&f.config.leaves.path, f.dir.path().join("retained-leaf"))
                        .unwrap();
                    std::fs::write(&f.config.leaves.path, body).unwrap();
                }
                _ => unreachable!(),
            }
            let binary = authenticate_stream(&f.binary).unwrap();
            assert!(
                publish_with_sync(
                    &f.config.output_directory,
                    &a,
                    &r,
                    None,
                    &binary,
                    |file, _| file.sync_all()
                )
                .is_err(),
                "{case}"
            );
            assert!(!f.config.output_directory.join("receipt.json").exists());
        }
        let f = Fixture::new();
        let one = authenticate_stream(&f.binary).unwrap();
        let two = authenticate_stream(&f.binary).unwrap();
        assert!(recheck_distinct(&[&one, &two]).is_err());
    }

    #[test]
    fn partition_reconstruction_create_only_and_all_sync_failures_never_success() {
        let f = Fixture::new();
        let (_, a) = f.auth().unwrap();
        let r = reconstruct_centroids(&a, &f.config.leaves).unwrap();
        let binary = authenticate_stream(&f.binary).unwrap();
        let p = verify_historical_radial(
            &a,
            &r,
            f.config.historical.as_ref().unwrap(),
            &f.config.leaves,
        )
        .unwrap();
        for (stage, parity, sync_calls) in [(1, None, 7), (2, Some(&p), 13)] {
            let success = f.dir.path().join(format!("stage-{stage}-success"));
            let mut roster = Vec::new();
            assert!(
                publish_with_sync(&success, &a, &r, parity, &binary, |file, is_dir| {
                    let metadata = file.metadata()?;
                    assert_eq!(metadata.is_dir(), is_dir);
                    roster.push((metadata.dev(), metadata.ino(), is_dir));
                    file.sync_all()
                })
                .unwrap()
            );
            let mut expected_paths = vec![
                (success.clone(), true),
                (f.dir.path().to_owned(), true),
                (success.join("centroids.bin"), false),
                (success.clone(), true),
            ];
            if stage == 2 {
                for name in [
                    "radial-root.bin",
                    "radial-membership.bin",
                    "radial-leaves.bin",
                ] {
                    expected_paths.extend([(success.join(name), false), (success.clone(), true)]);
                }
            }
            expected_paths.extend([
                (success.join("receipt.json"), false),
                (success.clone(), true),
                (f.dir.path().to_owned(), true),
            ]);
            let expected_roster = expected_paths
                .iter()
                .map(|(path, is_dir)| {
                    let metadata = std::fs::metadata(path).unwrap();
                    (metadata.dev(), metadata.ino(), *is_dir)
                })
                .collect::<Vec<_>>();
            assert_eq!(
                roster, expected_roster,
                "stage {stage} successful sync roster"
            );
            assert_eq!(
                roster.len(),
                sync_calls,
                "stage {stage} successful sync count"
            );
            for failed_call in 1..=roster.len() {
                let output = f
                    .dir
                    .path()
                    .join(format!("stage-{stage}-sync-{failed_call}"));
                let mut calls = 0;
                let original_failure = format!("injected stage {stage} sync {failed_call} failure");
                let result = publish_with_sync(&output, &a, &r, parity, &binary, |file, _| {
                    calls += 1;
                    if calls == failed_call {
                        Err(io::Error::other(original_failure.clone()))
                    } else {
                        file.sync_all()
                    }
                });
                let error = result.expect_err("injected publication sync must fail");
                assert!(error.to_string().contains(&original_failure), "{error}");
                assert_eq!(calls, failed_call);
                if output.join("receipt.json").exists() {
                    let v: Value = serde_json::from_slice(
                        &std::fs::read(output.join("receipt.json")).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(v["complete"], false, "stage {stage} sync {failed_call}");
                }
            }
            for (index, (_, _, is_dir)) in roster.iter().enumerate() {
                if *is_dir {
                    continue;
                }
                let rewrite_call = index + 1;
                let output = f
                    .dir
                    .path()
                    .join(format!("stage-{stage}-rewrite-{rewrite_call}"));
                let target = output.join(expected_paths[index].0.file_name().unwrap());
                let mut calls = 0;
                let mut rewritten = false;
                let result = publish_with_sync(&output, &a, &r, parity, &binary, |file, is_dir| {
                    calls += 1;
                    file.sync_all()?;
                    if calls == rewrite_call {
                        assert!(!is_dir);
                        let synced = file_identity(file).map_err(io::Error::other)?;
                        let bytes = std::fs::read(&target)?;
                        let mut rewrite = file.try_clone()?;
                        rewrite.set_len(0)?;
                        rewrite.rewind()?;
                        rewrite.write_all(&bytes)?;
                        // A distinct mtime keeps coarse filesystem clocks from hiding this rewrite.
                        rewrite.set_modified(std::time::UNIX_EPOCH)?;
                        assert_eq!(std::fs::read(&target)?, bytes);
                        assert_ne!(file_identity(file).map_err(io::Error::other)?, synced);
                        rewritten = true;
                    }
                    Ok(())
                });
                assert!(rewritten, "stage {stage} file sync {rewrite_call}");
                let error = result.expect_err("identical unsynced rewrite must refuse publication");
                assert!(
                    error.to_string().contains("changed during file sync"),
                    "{error}"
                );
                assert_eq!(calls, rewrite_call);
                if output.join("receipt.json").exists() {
                    let v: Value = serde_json::from_slice(
                        &std::fs::read(output.join("receipt.json")).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(v["complete"], false, "stage {stage} rewrite {rewrite_call}");
                }
            }
        }
        std::fs::create_dir(&f.config.output_directory).unwrap();
        std::fs::write(f.config.output_directory.join("occupied"), b"retain").unwrap();
        assert!(
            publish_with_sync(
                &f.config.output_directory,
                &a,
                &r,
                None,
                &binary,
                |file, _| file.sync_all()
            )
            .is_err()
        );
        assert_eq!(
            std::fs::read(f.config.output_directory.join("occupied")).unwrap(),
            b"retain"
        );
        let occupied = f.dir.path().join("occupied-file");
        std::fs::write(&occupied, b"retain").unwrap();
        assert!(
            publish_blob(&occupied, &r.blob, &[&binary], &mut |file: &File, _| file
                .sync_all())
            .is_err()
        );
        assert_eq!(std::fs::read(occupied).unwrap(), b"retain");
    }

    #[test]
    fn partition_reconstruction_sync_time_mutation_and_directory_replacement() {
        for (stage, receipt_directory_sync) in [(1, 6), (2, 12)] {
            for case in ["input", "output", "directory"] {
                let f = Fixture::new();
                let (_, a) = f.auth().unwrap();
                let r = reconstruct_centroids(&a, &f.config.leaves).unwrap();
                let binary = authenticate_stream(&f.binary).unwrap();
                let parity = if stage == 2 {
                    Some(
                        verify_historical_radial(
                            &a,
                            &r,
                            f.config.historical.as_ref().unwrap(),
                            &f.config.leaves,
                        )
                        .unwrap(),
                    )
                } else {
                    None
                };
                let mut calls = 0;
                let result = publish_with_sync(
                    &f.config.output_directory,
                    &a,
                    &r,
                    parity.as_ref(),
                    &binary,
                    |file, _| {
                        calls += 1;
                        if calls == receipt_directory_sync {
                            match case {
                                "input" => std::fs::write(
                                    if stage == 2 {
                                        &f.config.historical.as_ref().unwrap().membership.path
                                    } else {
                                        &f.config.leaves.path
                                    },
                                    b"mutated",
                                )?,
                                "output" => std::fs::write(
                                    f.config.output_directory.join(if stage == 2 {
                                        "radial-leaves.bin"
                                    } else {
                                        "centroids.bin"
                                    }),
                                    b"mutated",
                                )?,
                                "directory" => {
                                    std::fs::rename(
                                        &f.config.output_directory,
                                        f.dir.path().join("replaced-dir"),
                                    )?;
                                    std::fs::create_dir(&f.config.output_directory)?;
                                }
                                _ => unreachable!(),
                            }
                        }
                        file.sync_all()
                    },
                );
                assert!(result.is_err(), "stage {stage} {case}");
                assert!(calls >= receipt_directory_sync, "stage {stage} {case}");
                let owned_dir = if case == "directory" {
                    f.dir.path().join("replaced-dir")
                } else {
                    f.config.output_directory.clone()
                };
                let v: Value =
                    serde_json::from_slice(&std::fs::read(owned_dir.join("receipt.json")).unwrap())
                        .unwrap();
                assert_eq!(v["complete"], false, "stage {stage} {case}");
            }
        }
    }
}

// Independently frozen BORSUSR2 construction/encoder dependency closure from
// 2e2a789ad0e11c5d79348b93e21fdc8066425eac, semantic_unit_router.rs.
// Unused serving/profile helpers and the final old validation call are omitted.
// No old decoder is included.
// Public centroid/trainer dependencies are source-pinned by the fixture test.
#[cfg(test)]
#[allow(dead_code)]
#[rustfmt::skip]
mod frozen_old_builder {
    use borsuk::{VectorMetric, train_logical_cell_centroids, unit_centroid_pages::UnitCentroidPages};
    use serde::{Deserialize, Serialize};
    use sha2::{Digest, Sha256};
    use std::error::Error;
/// Errors include malformed identities/geometry, admission and native trainer failures.
pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const ROOT_CAP: usize = 64 * 1024;
const BLOB_CAP: usize = 8 * 1024 * 1024;
const BINARY_HEADER: usize = 512;
const DIRECTORY_BYTES: usize = 64;
/// Maximum admitted construction payload, not an RSS limit.
pub const ALLOCATION_CAP: usize = 128 * 1024 * 1024;
const HEADER_BYTES: usize = 32;
const LEAF_UNITS: usize = 64;
const ITERATIONS: usize = 12;
const SCHEMA: &str = "BORSUSR2";

/// Explicit geometry and payload admission; persisted profiles have no default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticProfile {
    /// Existing 1–100,000 rows, 1–1024 coordinates.
    Native100k,
    /// Fresh research arm: exactly one million rows and 768 coordinates.
    Fresh1m,
    /// Generic explicit scale geometry: 1–1,000,000 rows, 1–1024 coordinates.
    /// Uses existing scale algorithm/caps; geometry is not runtime qualification.
    Scale1m,
}
impl SemanticProfile {
    /// Construction payload ceiling, independently of measured process RSS.
    pub const fn allocation_cap(self) -> usize {
        match self {
            Self::Native100k => ALLOCATION_CAP,
            Self::Fresh1m | Self::Scale1m => 512 * 1024 * 1024,
        }
    }
    /// Maximum encoded root length.
    pub const fn root_cap(self) -> usize {
        match self {
            Self::Native100k => 1024 * 1024,
            Self::Fresh1m | Self::Scale1m => 4 * 1024 * 1024,
        }
    }
    /// Whether the supplied source geometry belongs to this profile.
    pub fn valid_geometry(self, rows: usize, dimensions: usize) -> bool {
        match self {
            Self::Native100k => (1..=100_000).contains(&rows) && (1..=1024).contains(&dimensions),
            Self::Fresh1m => rows == 1_000_000 && dimensions == 768,
            Self::Scale1m => (1..=1_000_000).contains(&rows) && (1..=1024).contains(&dimensions),
        }
    }





    fn code(self) -> u32 {
        match self {
            Self::Native100k => 1,
            Self::Fresh1m => 2,
            Self::Scale1m => 3,
        }
    }

}
#[allow(missing_docs)]
#[derive(Clone, Copy)]
pub struct Geometry {
    pub rows: usize,
    pub dimensions: usize,
    pub units: usize,
    pub blob_bytes: usize,
}

#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub bytes: usize,
    pub sha256: String,
}

#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Algorithm {
    pub trainer: String,
    pub metric: String,
    pub iterations: usize,
    pub requested_centers: usize,
    pub training_centers: usize,
    pub max_leaf_units: usize,
    pub nearest_ties: String,
    pub group_sort: String,
    pub root_prototype: String,
    pub normalization: String,
    pub payload: String,
}

#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Leaf {
    pub leaf_id: usize,
    pub group_ordinal: usize,
    pub chunk_ordinal: usize,
    pub offset: usize,
    pub bytes: usize,
    pub unit_count: usize,
    pub source_rows: usize,
    pub sha256: String,
    pub prototype: Vec<f32>,
}

#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub profile: SemanticProfile,
    pub schema: String,
    pub input_schema: String,
    pub input_root_sha256: String,
    pub input_centroids_sha256: String,
    pub input_centroids_bytes: usize,
    pub rows: usize,
    pub dimensions: usize,
    pub unit_rows: usize,
    pub page_rows: usize,
    pub unit_count: usize,
    pub final_unit_rows: usize,
    pub algorithm: Algorithm,
    pub modeled_peak_allocation_bytes: usize,
    pub modeled_allocation_limit_bytes: usize,
    pub allocation_model: String,
    pub membership: Artifact,
    pub leaf_payload: Artifact,
    pub leaves: Vec<Leaf>,
}

/// Authenticated source binding supplied by the enclosing generation or research CLI.
/// `root_sha256` is an input identity, never the output router's own digest.
pub struct SourceIdentity<'a> {
    /// Explicit admission profile authenticated by the enclosing generation.
    pub profile: SemanticProfile,
    /// Nonempty source format (at most 256 bytes); caller validates its meaning.
    pub schema: &'a str,
    /// Authenticated input root or source descriptor digest.
    pub root_sha256: &'a str,
    /// Digest of the original native FP16 unit-centroid bytes.
    pub centroids_sha256: &'a str,
    /// Physical source row count.
    pub rows: usize,
    /// Coordinates per unit mean.
    pub dimensions: usize,
}

/// Complete binary publication with unchanged membership and FP16 leaf encoding.
pub struct RouterArtifacts {
    /// BORSUSR2 root with exact leaf ranges, identities and f32 bits.
    pub manifest: Vec<u8>,
    /// One little-endian u32 leaf ID per original unit.
    pub membership: Vec<u8>,
    /// Concatenated unit ID and unchanged FP16 records.
    pub leaves: Vec<u8>,
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn require(ok: bool, message: &str) -> Result<()> {
    if !ok {
        return Err(message.into());
    }
    Ok(())
}
fn valid_sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn product(values: &[usize]) -> Result<usize> {
    values.iter().try_fold(1_usize, |total, value| {
        total
            .checked_mul(*value)
            .ok_or_else(|| "allocation arithmetic overflow".into())
    })
}

fn sum(values: &[usize]) -> Result<usize> {
    values.iter().try_fold(0_usize, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| "allocation arithmetic overflow".into())
    })
}

pub fn preflight(
    blob: &[u8],
    rows: usize,
    dimensions: usize,
    profile: SemanticProfile,
) -> Result<Geometry> {
    if blob.len() < HEADER_BYTES || &blob[..8] != b"BORSUCP1" || blob[28..32] != [0; 4] {
        return Err("centroid header".into());
    }
    let header_rows = usize::try_from(u64::from_le_bytes(blob[8..16].try_into()?))?;
    let header_dimensions = u32::from_le_bytes(blob[16..20].try_into()?) as usize;
    let unit_rows = u32::from_le_bytes(blob[20..24].try_into()?);
    let page_rows = u32::from_le_bytes(blob[24..28].try_into()?);
    if !profile.valid_geometry(header_rows, header_dimensions)
        || unit_rows != 32
        || page_rows != 256
        || header_rows != rows
        || header_dimensions != dimensions
    {
        return Err("centroid geometry or manifest agreement".into());
    }
    let units = sum(&[rows, 31])? / 32;
    let blob_bytes = sum(&[HEADER_BYTES, product(&[units, dimensions, 2])?])?;
    if blob_bytes != blob.len() || (profile == SemanticProfile::Native100k && blob_bytes > BLOB_CAP)
    {
        return Err("centroid exact payload length or unit cap".into());
    }
    Ok(Geometry {
        rows,
        dimensions,
        units,
        blob_bytes,
    })
}

/// Conservative maximum of phases whose allocations do not coexist; not RSS.
pub fn admit(geometry: Geometry, cap: usize, profile: SemanticProfile) -> Result<usize> {
    let Geometry {
        rows,
        dimensions: d,
        units: u,
        blob_bytes: b,
    } = geometry;
    require(
        size_of::<usize>() == 8
            && profile.valid_geometry(rows, d)
            && u == rows.div_ceil(32)
            && b == sum(&[HEADER_BYTES, product(&[u, d, 2])?])?,
        "allocation geometry",
    )?;
    let c = u.div_ceil(LEAF_UNITS);
    let l = product(&[2, c])?
        .checked_sub(1)
        .ok_or("allocation geometry")?;
    let f = sum(&[product(&[u, d, 4])?, product(&[u, 4])?])?;
    let v = sum(&[
        product(&[u, d, 4])?,
        product(&[u, size_of::<Vec<f32>>() + 64])?,
    ])?;
    let i = product(&[c, sum(&[product(&[d, 4])?, 128])?])?;
    // Every nonterminal child loses >=31 quotas above fanout32; at <=32
    // all children are terminal. 489 centers therefore need <=16 frames.
    let h = (c - 1).div_ceil(31);
    let r = product(&[
        h,
        sum(&[product(&[32, d, 16])?, product(&[u, 64])?, 32 * 128])?,
    ])?;
    let p = product(&[u, sum(&[product(&[d, 2])?, 4])?])?;
    let w = sum(&[
        BINARY_HEADER,
        product(&[l, sum(&[DIRECTORY_BYTES, product(&[d, 4])?])?])?,
    ])?;
    let prototypes = product(&[3, l, sum(&[product(&[d, 4])?, 256])?])?;
    let training = sum(&[
        2 * ROOT_CAP,
        b,
        f,
        product(&[2, v])?,
        r,
        product(&[2, i])?,
        8 * 1024 * 1024,
    ])?;
    let emit = sum(&[
        2 * ROOT_CAP,
        b,
        f,
        v,
        i,
        product(&[64, u])?,
        p,
        product(&[4, u])?,
        prototypes,
        4 * 1024 * 1024,
        8 * 1024 * 1024,
    ])?;
    let validate = sum(&[
        2 * ROOT_CAP,
        b,
        f,
        product(&[2, p])?,
        product(&[16, u])?,
        product(&[3, w])?,
        prototypes,
        8 * 1024 * 1024,
    ])?;
    let estimate = training.max(emit).max(validate);
    require(
        estimate <= cap,
        &format!("modeled allocation admission: {estimate} bytes exceeds {cap}"),
    )?;
    Ok(estimate)
}

fn squared_distance(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(a, b)| (f64::from(*a) - f64::from(*b)).powi(2))
        .sum()
}

/// Assign original units to trained centers with the frozen distance/ID ordering.
pub fn assign_groups(sample: &[Vec<f32>], centers: &[Vec<f32>]) -> Result<Vec<Vec<(f64, usize)>>> {
    if sample.is_empty()
        || sample[0].is_empty()
        || sample
            .iter()
            .any(|v| v.len() != sample[0].len() || v.iter().any(|x| !x.is_finite()))
        || centers.is_empty()
        || centers
            .iter()
            .any(|center| center.len() != sample[0].len() || center.iter().any(|v| !v.is_finite()))
    {
        return Err("trainer returned invalid centers".into());
    }
    let mut assignments = Vec::with_capacity(sample.len());
    let mut counts = vec![0_usize; centers.len()];
    for (unit, vector) in sample.iter().enumerate() {
        let mut best = 0;
        let mut distance = squared_distance(vector, &centers[0]);
        for (ordinal, center) in centers.iter().enumerate().skip(1) {
            let candidate = squared_distance(vector, center);
            if candidate < distance {
                best = ordinal;
                distance = candidate;
            }
        }
        if !distance.is_finite() {
            return Err("nonfinite assignment distance".into());
        }
        counts[best] += 1;
        assignments.push((best, distance, unit));
    }
    let mut groups = counts
        .into_iter()
        .map(Vec::with_capacity)
        .collect::<Vec<_>>();
    for (best, distance, unit) in assignments {
        groups[best].push((distance, unit));
    }
    for group in &mut groups {
        group.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    }
    Ok(groups)
}

fn prototype(scorer: &UnitCentroidPages, units: &[usize]) -> Result<Vec<f32>> {
    let mut sums = vec![0.0_f64; scorer.dimensions()];
    for &unit in units {
        let vector = scorer.unit_centroid(unit).ok_or("prototype unit ID")?;
        for (sum, value) in sums.iter_mut().zip(vector) {
            *sum += f64::from(*value);
        }
    }
    let mean = sums
        .into_iter()
        .map(|sum| (sum / units.len() as f64) as f32)
        .collect::<Vec<_>>();
    if mean.iter().any(|v| !v.is_finite()) {
        return Err("nonfinite root prototype".into());
    }
    Ok(mean)
}

fn unit_row_count(geometry: Geometry, unit: usize) -> usize {
    (geometry.rows - 32 * unit).min(32)
}

/// Build using the existing deterministic trainer and an explicit allocation cap.
/// The source descriptor must already be authenticated by the caller.
pub fn build(
    blob: &[u8],
    input: &SourceIdentity<'_>,
    allocation_limit: usize,
) -> Result<RouterArtifacts> {
    require(
        allocation_limit <= input.profile.allocation_cap(),
        "construction allocation limit",
    )?;
    let geometry = preflight(blob, input.rows, input.dimensions, input.profile)?;
    let estimate = admit(geometry, allocation_limit, input.profile)?;
    require(
        (1..=256).contains(&input.schema.len())
            && valid_sha(input.root_sha256)
            && valid_sha(input.centroids_sha256)
            && hash(blob) == input.centroids_sha256,
        "source identity",
    )?;
    let scorer = UnitCentroidPages::decode(blob)?;
    let sample = (0..geometry.units)
        .map(|unit| scorer.unit_centroid(unit).unwrap().to_vec())
        .collect::<Vec<_>>();
    let requested = geometry.units.div_ceil(LEAF_UNITS);
    let identical = sample.iter().all(|vector| vector == &sample[0]);
    let training_centers = if identical { 1 } else { requested };
    // No retry or fallback geometry: duplicate-center and other trainer errors propagate.
    let centers = train_logical_cell_centroids(
        &sample,
        VectorMetric::SquaredEuclidean,
        training_centers,
        ITERATIONS,
    )?;
    let groups = assign_groups(&sample, &centers)?;
    let record_bytes = 4 + geometry.dimensions * 2;
    let mut membership = vec![0_u8; geometry.units * 4];
    let mut payload = Vec::with_capacity(geometry.units * record_bytes);
    let mut leaves = Vec::with_capacity(2 * requested - 1);
    for (group_ordinal, group) in groups.iter().enumerate() {
        for (chunk_ordinal, chunk) in group.chunks(LEAF_UNITS).enumerate() {
            let leaf_id = leaves.len();
            let offset = payload.len();
            let units = chunk.iter().map(|(_, unit)| *unit).collect::<Vec<_>>();
            let mut source_rows = 0;
            for &unit in &units {
                membership[unit * 4..unit * 4 + 4]
                    .copy_from_slice(&u32::try_from(leaf_id)?.to_le_bytes());
                payload.extend_from_slice(&u32::try_from(unit)?.to_le_bytes());
                let original = HEADER_BYTES + unit * geometry.dimensions * 2;
                payload.extend_from_slice(&blob[original..original + geometry.dimensions * 2]);
                source_rows += unit_row_count(geometry, unit);
            }
            leaves.push(Leaf {
                leaf_id,
                group_ordinal,
                chunk_ordinal,
                offset,
                bytes: payload.len() - offset,
                unit_count: units.len(),
                source_rows,
                sha256: hash(&payload[offset..]),
                prototype: prototype(&scorer, &units)?,
            });
        }
    }
    drop((scorer, sample, centers, groups));
    let manifest = Manifest {
        profile: input.profile,
        schema: SCHEMA.into(),
        input_schema: input.schema.into(),
        input_root_sha256: input.root_sha256.into(),
        input_centroids_sha256: hash(&blob),
        input_centroids_bytes: blob.len(),
        rows: geometry.rows,
        dimensions: geometry.dimensions,
        unit_rows: 32,
        page_rows: 256,
        unit_count: geometry.units,
        final_unit_rows: unit_row_count(geometry, geometry.units - 1),
        algorithm: fixed_algorithm(requested, training_centers),
        modeled_peak_allocation_bytes: estimate,
        modeled_allocation_limit_bytes: allocation_limit,
        allocation_model:
            "conservative allocation capacities; not RSS or an enforced process limit".into(),
        membership: Artifact {
            bytes: membership.len(),
            sha256: hash(&membership),
        },
        leaf_payload: Artifact {
            bytes: payload.len(),
            sha256: hash(&payload),
        },
        leaves,
    };
    let body = manifest.encode()?;

    Ok(RouterArtifacts {
        manifest: body,
        membership,
        leaves: payload,
    })
}

fn fixed_algorithm(requested: usize, training: usize) -> Algorithm {
    Algorithm {
        trainer: "train_logical_cell_centroids".into(),
        metric: "SquaredEuclidean".into(),
        iterations: ITERATIONS,
        requested_centers: requested,
        training_centers: training,
        max_leaf_units: LEAF_UNITS,
        nearest_ties: "center ordinal".into(),
        group_sort: "squared distance, original unit ID".into(),
        root_prototype: "unweighted unit mean; f64 accumulation to finite f32".into(),
        normalization: "none".into(),
        payload: "original FP16 little endian".into(),
    }
}


fn put32(body: &mut [u8], offset: usize, value: usize) -> Result<()> {
    body[offset..offset + 4].copy_from_slice(&u32::try_from(value)?.to_le_bytes());
    Ok(())
}

fn put64(body: &mut [u8], offset: usize, value: usize) -> Result<()> {
    body[offset..offset + 8].copy_from_slice(&u64::try_from(value)?.to_le_bytes());
    Ok(())
}

fn put_digest(body: &mut [u8], offset: usize, digest: &str) -> Result<()> {
    require(valid_sha(digest), "binary digest")?;
    for (slot, pair) in body[offset..offset + 32]
        .iter_mut()
        .zip(digest.as_bytes().chunks_exact(2))
    {
        *slot = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(())
}

impl Manifest {
    /// Encode the fixed BORSUSR2 header, directory and exact little-endian f32 bits.
    /// Publication/open still validate geometry, identities and the full partition.
    pub fn encode(&self) -> Result<Vec<u8>> {
        require(
            (1..=256).contains(&self.input_schema.len()),
            "source schema length",
        )?;
        let plane = product(&[self.leaves.len(), self.dimensions, 4])?;
        let directory = product(&[self.leaves.len(), DIRECTORY_BYTES])?;
        let length = sum(&[BINARY_HEADER, directory, plane])?;
        require(length <= self.profile.root_cap(), "binary root cap")?;
        let mut body = vec![0; length];
        body[..8].copy_from_slice(b"BORSUSR2");
        put32(&mut body, 8, BINARY_HEADER)?;
        put32(&mut body, 12, self.profile.code() as usize)?;
        put64(&mut body, 16, self.rows)?;
        for (offset, value) in [
            (24, self.dimensions),
            (28, self.unit_count),
            (32, self.leaves.len()),
            (36, self.algorithm.requested_centers),
            (40, self.algorithm.training_centers),
            (44, self.input_schema.len()),
        ] {
            put32(&mut body, offset, value)?;
        }
        for (offset, value) in [
            (48, self.input_centroids_bytes),
            (56, self.membership.bytes),
            (64, self.leaf_payload.bytes),
            (72, self.modeled_peak_allocation_bytes),
            (80, self.modeled_allocation_limit_bytes),
        ] {
            put64(&mut body, offset, value)?;
        }
        for (offset, digest) in [
            (128, &self.input_root_sha256),
            (160, &self.input_centroids_sha256),
            (192, &self.membership.sha256),
            (224, &self.leaf_payload.sha256),
        ] {
            put_digest(&mut body, offset, digest)?;
        }
        body[256..256 + self.input_schema.len()].copy_from_slice(self.input_schema.as_bytes());
        for (id, leaf) in self.leaves.iter().enumerate() {
            require(
                leaf.prototype.len() == self.dimensions,
                "prototype encoding geometry",
            )?;
            let start = BINARY_HEADER + id * DIRECTORY_BYTES;
            for (offset, value) in [
                (0, leaf.group_ordinal),
                (4, leaf.chunk_ordinal),
                (16, leaf.bytes),
                (20, leaf.unit_count),
                (24, leaf.source_rows),
            ] {
                put32(&mut body, start + offset, value)?;
            }
            put64(&mut body, start + 8, leaf.offset)?;
            put_digest(&mut body, start + 32, &leaf.sha256)?;
            let start = BINARY_HEADER + directory + id * self.dimensions * 4;
            for (word, value) in body[start..start + self.dimensions * 4]
                .chunks_exact_mut(4)
                .zip(&leaf.prototype)
            {
                word.copy_from_slice(&value.to_bits().to_le_bytes());
            }
        }
        Ok(body)
    }
}
}
