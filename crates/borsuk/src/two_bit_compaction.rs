//! Callable in-process compaction with a durable ready point and immutable uploads.
use crate::{
    canonical_source::{
        TwoBitCompactionLimits, TwoBitCompactionSource, prepare_two_bit_compaction_in_job,
    },
    object_native_generation::valid_object_key,
    resident_graph_generation::{Artifact, valid_sha256},
    resident_graph_store::upload_authenticated_file,
    rotated_two_bit::RotatedTwoBitCodec,
    semantic_unit_router::SemanticProfile,
    sq8_source::build_sq8_source_with_ids,
    two_bit_build::TwoBitGenerationBuilder,
    two_bit_generation::{
        DiscoveryMode, Manifest, SourceLayoutMode, TwoBitGenerationError, TwoBitGenerationLimits,
    },
    two_bit_mutations::{TwoBitMutationLimits, read_two_bit_mutations, seal_two_bit_mutations},
    two_bit_source::{SourceBuildError, TwoBitSource, read_authenticated},
    two_bit_store::{
        EmptyRoot, TwoBitHead, TwoBitStoreError, discovery_profile_and_source_layout,
        publish_empty_two_bit_generation_with_source_layout, publish_two_bit_generation,
        read_two_bit_head,
    },
};
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

type Result<T> = std::result::Result<T, TwoBitStoreError>;
fn bad(message: &'static str) -> TwoBitStoreError {
    TwoBitStoreError::Invalid(message)
}
fn plane(error: SourceBuildError) -> TwoBitStoreError {
    TwoBitGenerationError::Plane(error).into()
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Phase caps; runtime/allocator/transport are caller charges.
/// Declare all retained generations/query pins in generation.already_pinned_bytes;
/// these pins also count against source-phase memory, without expanding any cap.
#[derive(Clone, Copy)]
pub struct TwoBitCompactionOptions {
    /// Bounded recovery/sealing payload, no greater than source maintenance cap.
    pub mutations: TwoBitMutationLimits,
    /// Maintenance payload and temporary-disk cap for merge/build/upload.
    pub source: TwoBitCompactionLimits,
    /// New generation publication admission, including caller-retained pins.
    pub generation: TwoBitGenerationLimits,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Job {
    #[serde(deserialize_with = "deserialize_job_schema")]
    schema: String,
    index_prefix: String,
    base_root_sha256: String,
    base_generation: u64,
    base_epoch: u64,
    dimensions: usize,
    mutation_sha256: String,
    mutation_revision: u64,
    base_discovery: DiscoveryMode,
    #[serde(deserialize_with = "Option::deserialize")]
    base_profile: Option<SemanticProfile>,
    source_layout: SourceLayoutMode,
    discovery: DiscoveryMode,
    #[serde(deserialize_with = "Option::deserialize")]
    profile: Option<SemanticProfile>,
}
fn deserialize_job_schema<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let schema = String::deserialize(deserializer)?;
    if schema != "borsuk-two-bit-compaction-job-v6" {
        return Err(serde::de::Error::custom(
            "unsupported compaction job schema",
        ));
    }
    Ok(schema)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    #[serde(deserialize_with = "deserialize_ready_schema")]
    schema: String,
    job_sha256: String,
    input_sha256: String,
    target_root_sha256: String,
    rows: usize,
}
fn deserialize_ready_schema<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let schema = String::deserialize(deserializer)?;
    if schema != "borsuk-two-bit-compaction-ready-v3" {
        return Err(serde::de::Error::custom(
            "unsupported compaction ready schema",
        ));
    }
    Ok(schema)
}
fn read_json<T: DeserializeOwned>(path: &Path) -> Result<(T, Vec<u8>)> {
    let file = File::open(path)?;
    let size = file.metadata()?.len();
    if size == 0 || size > 4096 {
        return Err(bad("compaction journal length"));
    }
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != size {
        return Err(bad("compaction journal length changed"));
    }
    Ok((
        serde_json::from_slice(&bytes).map_err(|_| bad("compaction journal schema"))?,
        bytes,
    ))
}
fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| bad("compaction journal encoding"))?;
    if bytes.len() > 4096 {
        return Err(bad("compaction journal length"));
    }
    let mut file = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
    file.write_all(&bytes)?;
    file.as_file().sync_all()?;
    file.persist_noclobber(path)
        .map_err(|e| TwoBitStoreError::Io(e.error))?;
    File::open(path.parent().unwrap())?.sync_all()?;
    Ok(bytes)
}
fn digest_file(path: &Path, length: u64, digest: &str) -> Result<()> {
    if !valid_sha256(digest) {
        return Err(bad("compaction input digest"));
    }
    let mut file = File::open(path)?;
    if file.metadata()?.len() != length {
        return Err(bad("compaction input length"));
    }
    let mut buffer = [0u8; 65536];
    let mut sha = Sha256::new();
    let mut count = 0u64;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        count = count
            .checked_add(n as u64)
            .ok_or(bad("compaction input length"))?;
        if count > length {
            return Err(bad("compaction input length changed"));
        }
        sha.update(&buffer[..n]);
    }
    if count != length || format!("{:x}", sha.finalize()) != digest {
        return Err(bad("compaction input identity"));
    }
    Ok(())
}
fn verified_input(
    directory: &Path,
    job: &Job,
    expected: Option<&str>,
) -> Result<(TwoBitCompactionSource, String)> {
    let (input, bytes): (TwoBitCompactionSource, _) = read_json(&directory.join("manifest.json"))?;
    let digest = hash(&bytes);
    if expected.is_some_and(|s| s != digest)
        || input.schema != "borsuk-two-bit-compaction-source-v1"
        || input.base_root_sha256 != job.base_root_sha256
        || input.mutation_sha256 != job.mutation_sha256
        || input.mutation_revision != job.mutation_revision
        || input.dimensions != job.dimensions
    {
        return Err(bad("compaction prepared binding"));
    }
    let raw_bytes = input
        .rows
        .checked_mul(input.dimensions)
        .and_then(|n| n.checked_mul(4))
        .ok_or(bad("compaction source geometry"))?;
    let id_bytes = input
        .rows
        .checked_mul(8)
        .ok_or(bad("compaction ID geometry"))?;
    digest_file(
        &directory.join("source.f32"),
        raw_bytes as u64,
        &input.raw_sha256,
    )?;
    digest_file(
        &directory.join("ids.i64"),
        id_bytes as u64,
        &input.ids_sha256,
    )?;
    Ok((input, digest))
}
fn prepared_manifest(directory: &Path, ready: &Ready) -> Result<Manifest> {
    let path = directory.join("manifest.json");
    let length = fs::metadata(&path)?.len();
    if length == 0 || length > 65536 {
        return Err(bad("compaction root length"));
    }
    let root =
        read_authenticated(&path, length as usize, &ready.target_root_sha256).map_err(plane)?;
    serde_json::from_slice(&root).map_err(|_| bad("compaction root schema"))
}
fn validate_target(
    directory: &Path,
    root: &Manifest,
    job: &Job,
    input: &TwoBitCompactionSource,
    generation: u64,
) -> Result<()> {
    if root.schema != crate::two_bit_generation::SCHEMA
        || root.generation != generation
        || root.base_epoch != job.base_epoch
        || root.discovery.mode() != job.discovery
        || root.discovery.semantic_profile() != job.profile
        || root.source_layout.mode() != job.source_layout
        || root.canonical.rows != input.rows
        || root.canonical.dimensions != input.dimensions
    {
        return Err(bad("compaction target mode/input/job"));
    }
    let path = directory.join("plane/manifest.json");
    let length = fs::metadata(&path)?.len();
    if length > 65536 {
        return Err(bad("compaction plane length"));
    }
    let body =
        read_authenticated(&path, length as usize, &root.plane_manifest_sha256).map_err(plane)?;
    let receipt: crate::two_bit_source::SourcePlaneReceipt =
        serde_json::from_slice(&body).map_err(|_| bad("compaction plane schema"))?;
    if receipt.source_sha256 != input.raw_sha256
        || receipt.rows != input.rows
        || receipt.dimensions != input.dimensions
    {
        return Err(bad("compaction source binding"));
    }
    // Bind physical logical IDs and normalized vectors to the authenticated
    // prepared input, with one row of scratch. Source hash alone omits IDs.
    let input_dir = directory
        .parent()
        .ok_or(bad("compaction input directory"))?
        .join("input");
    let mut raw =
        std::io::BufReader::with_capacity(65536, File::open(input_dir.join("source.f32"))?);
    let mut ids = std::io::BufReader::with_capacity(65536, File::open(input_dir.join("ids.i64"))?);
    let mut row = vec![
        0;
        input
            .dimensions
            .checked_mul(4)
            .ok_or(bad("compaction row width"))?
    ];
    let mut values = vec![0.; input.dimensions];
    let mut canonical = Sha256::new();
    for _ in 0..input.rows {
        let mut id = [0; 8];
        ids.read_exact(&mut id)?;
        raw.read_exact(&mut row)?;
        for (value, encoded) in values.iter_mut().zip(row.chunks_exact(4)) {
            *value = f32::from_le_bytes(encoded.try_into().unwrap());
        }
        canonical.update(id);
        for value in crate::sq8_source::cosine_vector(&values)
            .map_err(plane)?
            .iter()
        {
            canonical.update(value.to_le_bytes());
        }
    }
    if raw.read(&mut [0])? != 0
        || ids.read(&mut [0])? != 0
        || !root.canonical.valid()
        || format!("{:x}", canonical.finalize()) != root.canonical.sha256
    {
        return Err(bad("compaction canonical input/ID binding"));
    }
    Ok(())
}
fn disk_bound(rows: usize, dimensions: usize, layout: SourceLayoutMode) -> Result<u64> {
    let codec = RotatedTwoBitCodec::padded_dimensions(dimensions)
        .map_err(|e| plane(SourceBuildError::Codec(e)))?
        .div_ceil(4)
        .checked_add(8)
        .ok_or(bad("compaction disk geometry"))?;
    let per_row = dimensions
        .checked_mul(9)
        .and_then(|n| n.checked_add(36))
        .and_then(|n| n.checked_add(codec))
        .ok_or(bad("compaction disk geometry"))?;
    // Same conservative graph workspace upper bound used by the native builder;
    // also dominates BOTH serialized graphs (each<=64layers/M16/M032)
    // and centroids. Include page hashes and metadata.
    let graph = dimensions
        .checked_mul(32)
        .and_then(|n| n.checked_add(16384))
        .and_then(|n| n.checked_mul(rows.div_ceil(32)))
        .ok_or(bad("compaction disk geometry"))?;
    // Each unit belongs to one leaf, so a physical page can occur in at most
    // eight capsules. Keep the temporary flat plane and both capsule files.
    let capsules = if layout == SourceLayoutMode::LeafCapsulesV2 {
        let units = rows.div_ceil(32);
        codec
            .checked_mul(rows)
            .and_then(|n| n.checked_mul(units.min(8)))
            .and_then(|n| n.checked_add(units.checked_mul(32 + 4 + 64)?))
            .and_then(|n| n.checked_add(units.checked_mul(4)?))
            .and_then(|n| n.checked_add(32))
            .ok_or(bad("compaction capsule disk geometry"))?
    } else if layout == SourceLayoutMode::FramedRecordsV1 {
        crate::source_frame::frame_bytes(rows, codec).map_err(TwoBitGenerationError::Io)?;
        codec
            .checked_mul(rows)
            .and_then(|n| n.checked_add(rows.div_ceil(256).checked_mul(25)?))
            .and_then(|n| n.checked_add(crate::source_frame::directory_bytes(rows).ok()?))
            .ok_or(bad("compaction frame disk geometry"))?
    } else {
        0
    };
    per_row
        .checked_mul(rows)
        .and_then(|n| n.checked_add(capsules))
        .and_then(|n| n.checked_add(graph))
        .and_then(|n| {
            n.checked_add(
                rows.div_ceil(crate::sq8_page_authority::SQ8_PAGE_ROWS)
                    .checked_mul(32)?,
            )
        })
        .and_then(|n| n.checked_add(262144))
        .map(|n| n as u64)
        .ok_or(bad("compaction disk geometry"))
}

// Shared lifetime gate for cooperating processes on ONE host/filesystem.
// Destructive GC must acquire this same file exclusively before remote I/O.
// Advisory locks do not coordinate other hosts or uncoordinated low-level APIs.
pub(crate) fn shared_lifecycle(prefix: &ObjectPath, directory: &Path) -> Result<File> {
    lifecycle_lock(prefix, directory, false)
}
pub(crate) fn lifecycle_lock(
    prefix: &ObjectPath,
    directory: &Path,
    exclusive: bool,
) -> Result<File> {
    fs::create_dir_all(directory)?;
    let open_lock = |name: &str| -> Result<File> {
        Ok(OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join(name))?)
    };
    let binding = open_lock("binding.lock")?;
    binding
        .try_lock()
        .map_err(|_| bad("lifecycle binding busy"))?;
    let identity = directory.join("index.json");
    if identity.exists() {
        let (saved, _): (String, _) = read_json(&identity)?;
        if saved != prefix.as_ref() {
            return Err(bad("compaction directory namespace"));
        }
    } else {
        write_json(&identity, &prefix.as_ref())?;
    }
    let lifetime = open_lock("lifecycle.lock")?;
    if exclusive {
        lifetime
            .try_lock()
            .map_err(|_| bad("readers or maintenance still active"))?;
    } else {
        lifetime
            .try_lock_shared()
            .map_err(|_| bad("reclamation in progress"))?;
    }
    drop(binding);
    Ok(lifetime)
}

/// Compact the latest authenticated base/delta and return its published head.
/// Reuse ONE exclusive maintenance directory per index across retries/calls.
/// Local journal/files are trusted writer state; checksums detect corruption,
/// not a malicious local writer. Other directories/processes need coordination.
/// An owned worker holds the file lock until completion even if the awaiting
/// future is dropped. SDK retry/deadline/accounting policy belongs to the store.
/// Queries remain readable after sealing; a crash pauses writes until resume.
/// This performs local staging cleanup, not remote reader-safe object GC.
pub async fn compact_two_bit_index(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    maintenance_directory: &Path,
    options: TwoBitCompactionOptions,
) -> Result<TwoBitHead> {
    compact_two_bit_index_with_discovery(store, prefix, maintenance_directory, options, None).await
}
/// Compact into an explicitly selected semantic profile, captured for restart.
/// Geometry 1–1M / D1–1024 still requires existing memory/disk admission.
pub async fn compact_two_bit_index_with_semantic_profile(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    maintenance_directory: &Path,
    options: TwoBitCompactionOptions,
    profile: SemanticProfile,
) -> Result<TwoBitHead> {
    compact_with_profile(
        store,
        prefix,
        maintenance_directory,
        options,
        Some(DiscoveryMode::Semantic),
        Some(profile),
    )
    .await
}
/// Compact with an explicit mode change, authenticated in the durable job.
/// None inherits the base mode (or a previously captured job on restart).
pub async fn compact_two_bit_index_with_discovery(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    maintenance_directory: &Path,
    options: TwoBitCompactionOptions,
    requested: Option<DiscoveryMode>,
) -> Result<TwoBitHead> {
    compact_with_profile(
        store,
        prefix,
        maintenance_directory,
        options,
        requested,
        None,
    )
    .await
}
async fn compact_with_profile(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    maintenance_directory: &Path,
    options: TwoBitCompactionOptions,
    requested: Option<DiscoveryMode>,
    requested_profile: Option<SemanticProfile>,
) -> Result<TwoBitHead> {
    let base = read_two_bit_head(store.as_ref(), prefix)
        .await?
        .ok_or(bad("compaction index absent"))?;
    let (base_discovery, profile, source_layout) =
        discovery_profile_and_source_layout(store.as_ref(), &base).await?;
    // Read the captured target before creating any maintenance state. This
    // preliminary read grants no authority; the owned worker rebinds under lock.
    let job_path = maintenance_directory
        .join(base.root_sha256())
        .join("job.json");
    let captured = if job_path.exists() {
        Some(read_json::<Job>(&job_path)?.0)
    } else {
        None
    };
    let (discovery, target_profile) = resolve_target(
        base_discovery,
        profile,
        captured.as_ref(),
        requested,
        requested_profile,
    );
    if captured
        .as_ref()
        .is_some_and(|job| job.source_layout != source_layout)
    {
        return Err(bad("compaction job changed"));
    }
    admit_compaction_dimensions(discovery, target_profile, source_layout, base.dimensions())?;
    drop(base);
    let prefix = prefix.clone();
    let directory = maintenance_directory.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let _lifecycle = shared_lifecycle(&prefix, &directory)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("compaction.lock"))?;
        lock.try_lock()
            .map_err(|_| bad("compaction already running"))?;
        // Independent in-process I/O driver lets this owned worker finish after
        // its caller future/runtime stops waiting. No external maintenance service.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = runtime.block_on(compact_owned(
            store.as_ref(),
            &prefix,
            &directory,
            options,
            requested,
            requested_profile,
        ));
        drop(lock);
        result
    })
    .await
    .map_err(|_| bad("compaction worker failed"))?
}

fn resolve_target(
    base_discovery: DiscoveryMode,
    base_profile: Option<SemanticProfile>,
    captured: Option<&Job>,
    requested: Option<DiscoveryMode>,
    requested_profile: Option<SemanticProfile>,
) -> (DiscoveryMode, Option<SemanticProfile>) {
    let discovery = requested
        .or(captured.map(|job| job.discovery))
        .unwrap_or(base_discovery);
    let profile = if discovery == DiscoveryMode::Semantic {
        Some(
            requested_profile
                .or(captured
                    .filter(|job| job.discovery == discovery)
                    .and_then(|job| job.profile))
                .or(base_profile)
                .unwrap_or(SemanticProfile::Native100k),
        )
    } else {
        None
    };
    (discovery, profile)
}

fn admit_compaction_dimensions(
    discovery: DiscoveryMode,
    profile: Option<SemanticProfile>,
    source_layout: SourceLayoutMode,
    dimensions: usize,
) -> Result<()> {
    if source_layout == SourceLayoutMode::LeafCapsulesV2 && discovery != DiscoveryMode::Semantic {
        return Err(bad("capsules require semantic discovery"));
    }
    let profile = profile.unwrap_or(SemanticProfile::Native100k);
    if discovery == DiscoveryMode::Semantic && profile == SemanticProfile::Fresh1m {
        return Err(bad("Fresh1m maintenance is unsupported"));
    }
    if discovery == DiscoveryMode::Semantic
        && !(profile.valid_geometry(1, dimensions) || profile.valid_geometry(1_000_000, dimensions))
    {
        return Err(bad("semantic compaction dimensions"));
    }
    Ok(())
}

async fn compact_owned(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    directory: &Path,
    options: TwoBitCompactionOptions,
    requested: Option<DiscoveryMode>,
    requested_profile: Option<SemanticProfile>,
) -> Result<TwoBitHead> {
    if options.mutations.max_memory_bytes > options.source.max_memory_bytes
        || options.source.max_memory_bytes < 262144
        || options.source.max_disk_bytes < 262144
    {
        return Err(bad("compaction phase admission"));
    }
    let namespace = format!("{}/maintenance", prefix.as_ref());
    if !valid_object_key(
        &format!("{namespace}/{}/objects/{}", "0".repeat(48), "0".repeat(64)),
        &"0".repeat(64),
    ) {
        return Err(bad("compaction object namespace"));
    }
    let base = read_two_bit_head(store, prefix)
        .await?
        .ok_or(bad("compaction index absent"))?;
    let (base_discovery, base_profile, source_layout) =
        discovery_profile_and_source_layout(store, &base).await?;
    // Caller pins and the actual retained old head coexist with every phase.
    let retained = options
        .generation
        .already_pinned_bytes
        .checked_add(base.retained_root_bytes())
        .and_then(|n| n.checked_add(262144))
        .and_then(|n| usize::try_from(n).ok())
        .ok_or(bad("compaction coexistence memory"))?;
    let phase_memory = options
        .source
        .max_memory_bytes
        .checked_sub(retained)
        .ok_or(bad("compaction coexistence memory"))?;
    let options = TwoBitCompactionOptions {
        mutations: TwoBitMutationLimits {
            max_memory_bytes: options.mutations.max_memory_bytes.min(phase_memory),
            ..options.mutations
        },
        source: TwoBitCompactionLimits {
            max_memory_bytes: phase_memory,
            ..options.source
        },
        ..options
    };

    let target_generation = base
        .generation()
        .checked_add(1)
        .ok_or(bad("compaction generation overflow"))?;
    let job_dir = directory.join(base.root_sha256());
    let job_path = job_dir.join("job.json");
    let captured = if job_path.exists() {
        let previous = read_json::<Job>(&job_path)?.0;
        if previous.schema != "borsuk-two-bit-compaction-job-v6"
            || previous.index_prefix != prefix.as_ref()
            || previous.base_root_sha256 != base.root_sha256()
            || previous.base_generation != base.generation()
            || previous.dimensions != base.dimensions()
            || previous.base_discovery != base_discovery
            || previous.base_profile != base_profile
            || previous.source_layout != source_layout
            || (previous.discovery == DiscoveryMode::Semantic) != previous.profile.is_some()
        {
            return Err(bad("compaction job changed"));
        }
        Some(previous)
    } else {
        None
    };
    let ready_path = job_dir.join("ready.json");
    // Reject unsupported ready journals before mutation I/O or scratch changes.
    let mut captured_ready = if ready_path.exists() {
        Some(read_json::<Ready>(&ready_path)?.0)
    } else {
        None
    };
    let (discovery, profile) = resolve_target(
        base_discovery,
        base_profile,
        captured.as_ref(),
        requested,
        requested_profile,
    );
    if captured.as_ref().is_some_and(|previous| {
        (previous.discovery, previous.profile) != (discovery, profile) && captured_ready.is_some()
    }) {
        return Err(bad("compaction job changed"));
    }
    admit_compaction_dimensions(discovery, profile, source_layout, base.dimensions())?;
    // Local generations are not query caches. Discard only recognized obsolete
    // jobs, under the directory lock; never touch caller/unrecognized files.
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().ok_or(bad("compaction path"))?;
        if [
            "index.json",
            "compaction.lock",
            "binding.lock",
            "lifecycle.lock",
        ]
        .contains(&name)
            || name == base.root_sha256()
        {
            continue;
        }
        if !valid_sha256(name) || !entry.file_type()?.is_dir() {
            return Err(bad("unmanaged compaction path"));
        }
        let (job, _): (Job, _) = read_json(&entry.path().join("job.json"))?;
        if job.schema != "borsuk-two-bit-compaction-job-v6"
            || job.index_prefix != prefix.as_ref()
            || job.base_root_sha256 != name
            || job.base_generation >= base.generation()
        {
            return Err(bad("obsolete compaction job binding"));
        }
        fs::remove_dir_all(entry.path())?;
    }

    let latest = read_two_bit_mutations(store, &base, base.dimensions(), options.mutations).await?;
    let Some(latest) = latest else {
        if requested.is_some_and(|mode| mode != base_discovery)
            || requested_profile.is_some_and(|p| base_profile != Some(p))
        {
            return Err(bad("discovery change requires a mutation snapshot"));
        }
        return Ok(base);
    };
    let base_rows = if base.is_empty() {
        0
    } else {
        let root: Manifest = serde_json::from_slice(
            base.authenticated_root(&base.metadata_prefix(), base.root_sha256())?,
        )
        .map_err(|_| bad("compaction base schema"))?;
        root.canonical.rows
    };
    // Conservative prospective output count also covers recovery of all old rows.
    // Updates/deletes may reduce it; exact merged geometry is checked before IDs/build.
    let upper_rows = base_rows
        .checked_add(latest.put_rows())
        .ok_or(bad("compaction row count"))?;
    if disk_bound(upper_rows, base.dimensions(), source_layout)? > options.source.max_disk_bytes {
        return Err(bad("compaction build disk cap"));
    }
    if upper_rows > 0 {
        let budget = options
            .source
            .max_memory_bytes
            .checked_sub(latest.resident_payload_bytes())
            .and_then(|n| n.checked_sub(upper_rows.checked_mul(8)?))
            .ok_or(bad("compaction build memory"))?;
        let build_rows = profile.map_or(upper_rows, |p| {
            upper_rows.min(match p {
                SemanticProfile::Native100k => 100_000,
                SemanticProfile::Fresh1m | SemanticProfile::Scale1m => 1_000_000,
            })
        });
        crate::two_bit_build::admit_build_payload(
            build_rows,
            base.dimensions(),
            true,
            discovery,
            profile.unwrap_or(SemanticProfile::Native100k),
            budget,
        )?;
    }
    let sealed = seal_two_bit_mutations(store, &base, Some(&latest), options.mutations).await?;
    drop(latest);
    fs::create_dir_all(&job_dir)?;
    let (authority, _) =
        crate::two_bit_mutations::require_sealed_two_bit_mutations(store, &base).await?;
    let job = Job {
        schema: "borsuk-two-bit-compaction-job-v6".into(),
        index_prefix: prefix.as_ref().into(),
        base_root_sha256: base.root_sha256().into(),
        base_generation: base.generation(),
        base_epoch: authority.epoch,
        dimensions: base.dimensions(),
        mutation_sha256: sealed.sha256().into(),
        mutation_revision: sealed.revision(),
        base_discovery,
        base_profile,
        source_layout,
        discovery,
        profile,
    };
    let job_bytes = serde_json::to_vec(&job).map_err(|_| bad("compaction job"))?;
    if job_path.exists() {
        let (mut previous, bytes): (Job, _) = read_json(&job_path)?;
        if bytes != job_bytes {
            let old_epoch = previous.base_epoch;
            previous.base_epoch = job.base_epoch;
            let replace_mode = (requested.is_some() || requested_profile.is_some())
                && (previous.discovery, previous.profile) != (job.discovery, job.profile)
                && !ready_path.exists();
            if replace_mode {
                previous.discovery = job.discovery;
                previous.profile = job.profile;
            }
            if old_epoch > job.base_epoch
                || (old_epoch == job.base_epoch && !replace_mode)
                || serde_json::to_vec(&previous).map_err(|_| bad("compaction job"))? != job_bytes
            {
                return Err(bad("compaction job changed"));
            }
            // Discard owned scratch after GC or an explicit unready mode change.
            // Rebuild claims a fresh namespace; fenced GC reclaims abandoned claims.
            fs::remove_dir_all(&job_dir)?;
            captured_ready = None;
            fs::create_dir_all(&job_dir)?;
            write_json(&job_path, &job)?;
        }
    } else {
        write_json(&job_path, &job)?;
    }
    let input_dir = job_dir.join("input");
    let generation_dir = job_dir.join("generation");
    let recovered_ready = if let Some(ready) = captured_ready {
        if ready.schema != "borsuk-two-bit-compaction-ready-v3"
            || ready.job_sha256 != hash(&job_bytes)
            || !valid_sha256(&ready.target_root_sha256)
        {
            return Err(bad("compaction ready binding"));
        }
        if disk_bound(ready.rows, job.dimensions, job.source_layout)?
            > options.source.max_disk_bytes
        {
            return Err(bad("compaction build disk cap"));
        }
        let (input, _) = verified_input(&input_dir, &job, Some(&ready.input_sha256))?;
        if input.rows != ready.rows {
            return Err(bad("compaction ready rows"));
        }
        if ready.rows > 0 {
            let root = prepared_manifest(&generation_dir, &ready)?;
            validate_target(&generation_dir, &root, &job, &input, target_generation)?;
            let staged = async {
                store
                    .head(&ObjectPath::from(root.sq8_object_key.clone()))
                    .await?;
                let (owner, _) = root
                    .sq8_object_key
                    .rsplit_once("/objects/")
                    .ok_or(bad("compaction namespace"))?;
                let (claim, _) = crate::two_bit_store::small_object(
                    store,
                    &ObjectPath::from(format!("{owner}/claim.json")),
                    4096,
                )
                .await?;
                if claim.as_slice() != job_bytes.as_slice() {
                    return Err(bad("compaction namespace claim"));
                }
                Ok::<(), TwoBitStoreError>(())
            }
            .await;
            match staged {
                Ok(()) => Some(ready),
                Err(TwoBitStoreError::Store(object_store::Error::NotFound { .. })) => {
                    // Missing prepared artifacts require a rebuild. GC epoch changes
                    // already discard the whole job before this existence check.
                    fs::remove_file(&ready_path)?;
                    None
                }
                Err(error) => return Err(error),
            }
        } else {
            Some(ready)
        }
    } else {
        None
    };
    let ready = if let Some(ready) = recovered_ready {
        ready
    } else {
        // job.json has been bound to the authenticated base, sealed mutation,
        // epoch and requested target above, under the compaction lock. Reclaim
        // only this recognized job-owned crash staging; never scan temp prefixes.
        let preparation = job_dir.join("preparation");
        match fs::symlink_metadata(&preparation) {
            Ok(metadata) if metadata.file_type().is_dir() => fs::remove_dir_all(&preparation)?,
            Ok(_) => return Err(bad("compaction preparation path")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        for path in [&input_dir, &generation_dir] {
            if path.exists() {
                fs::remove_dir_all(path)?;
            }
        }
        let sq8_path = job_dir.join("sq8.bin");
        if sq8_path.exists() {
            fs::remove_file(&sq8_path)?;
        }
        let input = prepare_two_bit_compaction_in_job(
            store,
            &base,
            &sealed,
            &input_dir,
            &preparation,
            options.source,
        )
        .await
        .map_err(|e| e.error)?;
        if disk_bound(input.rows, input.dimensions, job.source_layout)?
            > options.source.max_disk_bytes
        {
            return Err(bad("compaction build disk cap"));
        }
        let (_, input_sha) = verified_input(&input_dir, &job, None)?;
        let target = if input.rows == 0 {
            hash(
                &serde_json::to_vec(&EmptyRoot {
                    schema: "borsuk-two-bit-empty-generation-v6".into(),
                    generation: target_generation,
                    dimensions: base.dimensions(),
                    base_epoch: job.base_epoch,
                    discovery: job.discovery,
                    profile: job.profile,
                    source_layout: job.source_layout,
                })
                .map_err(|_| bad("empty root"))?,
            )
        } else {
            let ids_bytes = input
                .rows
                .checked_mul(8)
                .ok_or(bad("compaction ID memory"))?;
            let build_budget = options
                .source
                .max_memory_bytes
                .checked_sub(sealed.resident_payload_bytes())
                .and_then(|n| n.checked_sub(ids_bytes))
                .ok_or(bad("compaction build memory"))?;
            // Native builders charge order + validation copies; reserve the
            // caller-owned logical IDs separately throughout metadata build.
            if input
                .rows
                .checked_mul(24)
                .and_then(|n| n.checked_add(262144))
                .is_none_or(|n| n > build_budget)
            {
                return Err(bad("compaction ID memory"));
            }
            crate::two_bit_build::admit_build_payload(
                input.rows,
                input.dimensions,
                true,
                job.discovery,
                job.profile.unwrap_or(SemanticProfile::Native100k),
                build_budget,
            )?;
            let mut bytes = File::open(input_dir.join("ids.i64"))?;

            let mut ids = Vec::new();
            ids.try_reserve_exact(input.rows)
                .map_err(|_| bad("compaction ID memory"))?;
            for _ in 0..input.rows {
                let mut id = [0; 8];
                bytes.read_exact(&mut id)?;
                ids.push(i64::from_le_bytes(id));
            }
            let mut order = Vec::new();
            order
                .try_reserve_exact(input.rows)
                .map_err(|_| bad("compaction order memory"))?;
            order.extend(0..input.rows as u64);
            let build_budget = build_budget
                .checked_sub((ids.capacity() - input.rows) * std::mem::size_of::<i64>())
                .and_then(|n| {
                    n.checked_sub((order.capacity() - input.rows) * std::mem::size_of::<u64>())
                })
                .ok_or(bad("compaction build capacity"))?;
            let raw = input_dir.join("source.f32");
            let encoding = build_sq8_source_with_ids(
                &raw,
                &input.raw_sha256,
                input.dimensions,
                &order,
                &ids,
                &sq8_path,
                build_budget,
            )
            .map_err(plane)?;
            let owner = ObjectPath::from(format!(
                "{namespace}/{:016x}{}",
                job.base_epoch,
                uuid::Uuid::new_v4().simple()
            ));
            // Claim once so a namespace collision never overwrites a pinned blob.
            store
                .put_opts(
                    &owner.clone().join("claim.json"),
                    PutPayload::from(job_bytes.clone()),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await?;
            let key = owner.join("objects").join(encoding.sha256.as_str());
            let upload_budget = build_budget
                .checked_sub(
                    input
                        .rows
                        .checked_mul(8)
                        .ok_or(bad("compaction upload memory"))?,
                )
                .and_then(|n| n.checked_sub(input.dimensions.checked_mul(8)?))
                .and_then(|n| n.checked_sub(131072))
                .ok_or(bad("compaction upload memory"))?;
            let uploaded = upload_authenticated_file(
                store,
                &key,
                &sq8_path,
                &Artifact {
                    bytes: fs::metadata(&sq8_path)?.len(),
                    sha256: encoding.sha256.clone(),
                },
                upload_budget,
            )
            .await?;
            let etag = uploaded.e_tag.ok_or(bad("compaction SQ8 ETag"))?;
            let builder = TwoBitGenerationBuilder {
                base_epoch: job.base_epoch,
                source: TwoBitSource {
                    raw: &raw,
                    raw_sha256: &input.raw_sha256,
                    sq8: &sq8_path,
                    sq8_sha256: &encoding.sha256,
                    rows: input.rows,
                    dimensions: input.dimensions,
                },
                generation: target_generation,
                low: &encoding.low,
                step: &encoding.step,
                sq8_object_key: key.as_ref(),
                sq8_etag: &etag,
            };
            if let Some(profile) = job.profile {
                builder.build_with_source_layout(
                    Some(&order),
                    profile,
                    None,
                    job.source_layout,
                    &generation_dir,
                    build_budget,
                )?
            } else {
                builder.build_with_discovery_and_source_layout(
                    Some(&order),
                    job.discovery,
                    job.source_layout,
                    &generation_dir,
                    build_budget,
                )?
            }
        };
        let ready = Ready {
            schema: "borsuk-two-bit-compaction-ready-v3".into(),
            job_sha256: hash(&job_bytes),
            input_sha256: input_sha,
            target_root_sha256: target,
            rows: input.rows,
        };
        write_json(&ready_path, &ready)?;
        ready
    };
    let published = if ready.rows == 0 {
        let expected = hash(
            &serde_json::to_vec(&EmptyRoot {
                schema: "borsuk-two-bit-empty-generation-v6".into(),
                generation: target_generation,
                dimensions: base.dimensions(),
                base_epoch: job.base_epoch,
                discovery: job.discovery,
                profile: job.profile,
                source_layout: job.source_layout,
            })
            .map_err(|_| bad("empty root"))?,
        );
        if ready.target_root_sha256 != expected {
            return Err(bad("empty ready root"));
        }
        publish_empty_two_bit_generation_with_source_layout(
            store,
            prefix,
            base.dimensions(),
            target_generation,
            Some(&base),
            Some(job.discovery),
            job.profile,
            job.source_layout,
        )
        .await?
    } else {
        let root = prepared_manifest(&generation_dir, &ready)?;
        let (input, _) = verified_input(&input_dir, &job, Some(&ready.input_sha256))?;
        validate_target(&generation_dir, &root, &job, &input, target_generation)?;
        if root.generation != target_generation
            || root.canonical.rows != ready.rows
            || root.canonical.dimensions != base.dimensions()
            || root.base_epoch != job.base_epoch
            || root.discovery.mode() != job.discovery
            || root.discovery.semantic_profile() != job.profile
            || root.schema != crate::two_bit_generation::SCHEMA
        {
            return Err(bad("compaction target binding"));
        }
        // Publication admits its actual manifest-selected roster and capsule
        // directory; release decoded target/input metadata before that phase.
        drop(root);
        drop(input);
        let limits = TwoBitGenerationLimits {
            already_pinned_bytes: options
                .generation
                .already_pinned_bytes
                .checked_add(sealed.resident_payload_bytes() as u64)
                .and_then(|n| n.checked_add(262144))
                .ok_or(bad("compaction publication memory"))?,
            ..options.generation
        };
        publish_two_bit_generation(
            store,
            prefix,
            &generation_dir,
            &ready.target_root_sha256,
            limits,
            Some(&base),
        )
        .await?
    };
    // A committed operation returns its head even if local cleanup is deferred;
    // the next call removes this recognized obsolete job before doing more work.
    let _ = fs::remove_dir_all(&job_dir);
    Ok(published)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framed_source_compaction_disk_reserves_flat_and_worst_case_frames() {
        for rows in [0usize, 1, 32, 256, 257, 513] {
            for dimensions in [1usize, 257, 1024] {
                let width = RotatedTwoBitCodec::padded_dimensions(dimensions)
                    .unwrap()
                    .div_ceil(4)
                    + 8;
                assert_eq!(
                    disk_bound(rows, dimensions, SourceLayoutMode::FramedRecordsV1).unwrap()
                        - disk_bound(rows, dimensions, SourceLayoutMode::FlatRecords).unwrap(),
                    (rows * width + rows.div_ceil(256) * 25 + (rows.div_ceil(256) + 1) * 8) as u64
                );
            }
        }
        assert!(disk_bound(usize::MAX, 1024, SourceLayoutMode::FramedRecordsV1).is_err());
    }

    #[test]
    fn scale_profile_compaction_million_row_payload_and_disk_arithmetic() {
        let profile = SemanticProfile::Scale1m;
        // Proof digests grow by874976B, while the one-block buffer shrinks
        // by232064B (256->32 rows at1036B/row): net+642912B.
        let build = 516_156_296;
        assert_eq!(
            crate::two_bit_build::admit_build_payload(
                1_000_000,
                1024,
                true,
                DiscoveryMode::Semantic,
                profile,
                build
            )
            .unwrap(),
            506_729_928
        );
        assert!(
            crate::two_bit_build::admit_build_payload(
                1_000_000,
                1024,
                true,
                DiscoveryMode::Semantic,
                profile,
                build - 1
            )
            .is_err()
        );
        assert_eq!(build + 8_000_000 + 262144, 524_418_440);
        assert_eq!(profile.allocation_cap() - 524_418_440, 12_452_472);
        assert_eq!(
            disk_bound(1_000_000, 1024, SourceLayoutMode::FlatRecords).unwrap(),
            11_053_262_144
        );
    }

    #[test]
    fn page_range_v2_capsule_compaction_disk_bound_includes_both_rosters() {
        for (rows, dimensions) in [
            (0_usize, 1_usize),
            (1, 1),
            (31, 255),
            (32, 256),
            (33, 257),
            (255, 1024),
            (256, 1),
            (257, 3),
            (513, 257),
            (1_000_000, 1024),
        ] {
            let units = rows.div_ceil(32);
            let width = RotatedTwoBitCodec::padded_dimensions(dimensions)
                .unwrap()
                .div_ceil(4)
                + 8;
            assert_eq!(
                disk_bound(rows, dimensions, SourceLayoutMode::LeafCapsulesV2).unwrap()
                    - disk_bound(rows, dimensions, SourceLayoutMode::FlatRecords).unwrap(),
                (rows * width * units.min(8) + units * (32 + 4 + 64) + units * 4 + 32) as u64,
            );
        }
        assert!(disk_bound(usize::MAX, 1024, SourceLayoutMode::LeafCapsulesV2).is_err());
    }

    #[test]
    fn empty_and_job_profiles_are_required_and_old_formats_refuse() {
        let root = EmptyRoot {
            schema: "borsuk-two-bit-empty-generation-v6".into(),
            generation: 1,
            dimensions: 257,
            base_epoch: 0,
            discovery: DiscoveryMode::Semantic,
            profile: Some(SemanticProfile::Scale1m),
            source_layout: SourceLayoutMode::LeafCapsulesV2,
        };
        assert!(root.valid());
        let mut value = serde_json::to_value(&root).unwrap();
        value.as_object_mut().unwrap().remove("source_layout");
        assert!(serde_json::from_value::<EmptyRoot>(value).is_err());
        let mut value = serde_json::to_value(&root).unwrap();
        value["source_layout"] = "unknown".into();
        assert!(serde_json::from_value::<EmptyRoot>(value).is_err());
        let mut value = serde_json::to_value(&root).unwrap();
        value["schema"] = "borsuk-two-bit-empty-generation-v4".into();
        assert!(!serde_json::from_value::<EmptyRoot>(value).unwrap().valid());
        assert!(
            admit_compaction_dimensions(
                DiscoveryMode::Graph,
                None,
                SourceLayoutMode::LeafCapsulesV2,
                257
            )
            .is_err()
        );
        let mut value = serde_json::to_value(&root).unwrap();
        value.as_object_mut().unwrap().remove("profile");
        assert!(serde_json::from_value::<EmptyRoot>(value).is_err());
        let mut value = serde_json::to_value(&root).unwrap();
        value["profile"] = serde_json::Value::Null;
        assert!(!serde_json::from_value::<EmptyRoot>(value).unwrap().valid());
        let mut value = serde_json::to_value(&root).unwrap();
        value["schema"] = "borsuk-two-bit-empty-generation-v3".into();
        assert!(!serde_json::from_value::<EmptyRoot>(value).unwrap().valid());
        let job = Job {
            schema: "borsuk-two-bit-compaction-job-v6".into(),
            index_prefix: "test".into(),
            base_root_sha256: "1".repeat(64),
            base_generation: 1,
            base_epoch: 0,
            dimensions: 257,
            mutation_sha256: "2".repeat(64),
            mutation_revision: 1,
            base_discovery: DiscoveryMode::Semantic,
            base_profile: Some(SemanticProfile::Scale1m),
            source_layout: SourceLayoutMode::LeafCapsulesV2,
            discovery: DiscoveryMode::Semantic,
            profile: Some(SemanticProfile::Scale1m),
        };
        let mut value = serde_json::to_value(&job).unwrap();
        assert!(serde_json::from_value::<Job>(value.clone()).is_ok());
        value["schema"] = "borsuk-two-bit-compaction-job-v4".into();
        assert!(serde_json::from_value::<Job>(value).is_err());
        for key in ["profile", "base_profile", "source_layout"] {
            let mut value = serde_json::to_value(&job).unwrap();
            value.as_object_mut().unwrap().remove(key);
            assert!(serde_json::from_value::<Job>(value).is_err());
        }
        assert!(
            admit_compaction_dimensions(
                DiscoveryMode::Semantic,
                Some(SemanticProfile::Scale1m),
                SourceLayoutMode::FlatRecords,
                1025
            )
            .is_err()
        );
    }

    #[test]
    fn page_range_v2_old_empty_job_and_ready_schemas_refuse() {
        let empty = serde_json::json!({
            "schema":"borsuk-two-bit-empty-generation-v5", "generation":1,
            "dimensions":257, "base_epoch":0, "discovery":"semantic",
            "profile":"scale1m", "source_layout":"flat_records"
        });
        assert!(!serde_json::from_value::<EmptyRoot>(empty).unwrap().valid());
        let job = serde_json::json!({
            "schema":"borsuk-two-bit-compaction-job-v5", "index_prefix":"old",
            "base_root_sha256":"1".repeat(64), "base_generation":1,
            "base_epoch":0, "dimensions":257, "mutation_sha256":"2".repeat(64),
            "mutation_revision":1, "base_discovery":"semantic", "base_profile":"scale1m",
            "source_layout":"flat_records", "discovery":"semantic", "profile":"scale1m"
        });
        assert!(
            serde_json::from_value::<Job>(job)
                .err()
                .unwrap()
                .to_string()
                .contains("unsupported compaction job schema")
        );
        let old_ready = serde_json::json!({
            "schema":"borsuk-two-bit-compaction-ready-v2", "job_sha256":"1".repeat(64),
            "input_sha256":"2".repeat(64), "target_root_sha256":"3".repeat(64), "rows":1
        });
        assert!(
            serde_json::from_value::<Ready>(old_ready)
                .err()
                .unwrap()
                .to_string()
                .contains("unsupported compaction ready schema")
        );
        let ready = Ready {
            schema: "borsuk-two-bit-compaction-ready-v3".into(),
            job_sha256: "1".repeat(64),
            input_sha256: "2".repeat(64),
            target_root_sha256: "3".repeat(64),
            rows: 1,
        };
        assert!(serde_json::from_slice::<Ready>(&serde_json::to_vec(&ready).unwrap()).is_ok());
    }

    #[test]
    fn prepared_target_binds_logical_ids_even_with_identical_raw_source() {
        for wrong in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let input_dir = temp.path().join("input");
            fs::create_dir(&input_dir).unwrap();
            let raw = [1.0_f32, 0.0, 0.0, 1.0]
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>();
            let ids = [71_i64, 92]
                .into_iter()
                .flat_map(i64::to_le_bytes)
                .collect::<Vec<_>>();
            let raw_path = input_dir.join("source.f32");
            fs::write(&raw_path, &raw).unwrap();
            fs::write(input_dir.join("ids.i64"), &ids).unwrap();
            let sq8_path = temp.path().join("sq8");
            let encoding = build_sq8_source_with_ids(
                &raw_path,
                &hash(&raw),
                2,
                &[0, 1],
                if wrong { &[72, 93] } else { &[71, 92] },
                &sq8_path,
                1_000_000,
            )
            .unwrap();
            let input = TwoBitCompactionSource {
                schema: "borsuk-two-bit-compaction-source-v1".into(),
                rows: 2,
                dimensions: 2,
                raw_sha256: hash(&raw),
                ids_sha256: hash(&ids),
                base_root_sha256: "1".repeat(64),
                mutation_sha256: "2".repeat(64),
                mutation_revision: 1,
                recovery: Default::default(),
            };
            let job = Job {
                schema: "borsuk-two-bit-compaction-job-v6".into(),
                index_prefix: "test".into(),
                base_root_sha256: input.base_root_sha256.clone(),
                base_generation: 1,
                base_epoch: 7,
                dimensions: 2,
                mutation_sha256: input.mutation_sha256.clone(),
                mutation_revision: 1,
                base_discovery: DiscoveryMode::Graph,
                base_profile: None,
                source_layout: SourceLayoutMode::FlatRecords,
                discovery: DiscoveryMode::Graph,
                profile: None,
            };
            let generation = temp.path().join("generation");
            TwoBitGenerationBuilder {
                source: TwoBitSource {
                    raw: &raw_path,
                    raw_sha256: &input.raw_sha256,
                    sq8: &sq8_path,
                    sq8_sha256: &encoding.sha256,
                    rows: 2,
                    dimensions: 2,
                },
                base_epoch: 7,
                generation: 2,
                low: &encoding.low,
                step: &encoding.step,
                sq8_object_key: &format!("test/objects/{}", encoding.sha256),
                sq8_etag: "etag",
            }
            .build_with_order(&[0, 1], &generation, 1_000_000)
            .unwrap();
            let root: Manifest =
                serde_json::from_slice(&fs::read(generation.join("manifest.json")).unwrap())
                    .unwrap();
            let mut wrong_layout = job;
            wrong_layout.source_layout = SourceLayoutMode::LeafCapsulesV2;
            assert!(matches!(
                validate_target(&generation, &root, &wrong_layout, &input, 2),
                Err(TwoBitStoreError::Invalid(
                    "compaction target mode/input/job"
                ))
            ));
            wrong_layout.source_layout = SourceLayoutMode::FlatRecords;
            let result = validate_target(&generation, &root, &wrong_layout, &input, 2);
            if wrong {
                assert!(matches!(
                    result,
                    Err(TwoBitStoreError::Invalid(
                        "compaction canonical input/ID binding"
                    ))
                ));
            } else {
                result.unwrap();
            }
        }
    }
}
