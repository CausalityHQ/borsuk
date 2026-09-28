//! Callable in-process compaction with a durable ready point and immutable uploads.
use crate::{
    canonical_source::{
        TwoBitCompactionLimits, TwoBitCompactionSource, prepare_two_bit_compaction,
    },
    object_native_generation::valid_object_key,
    resident_graph_generation::{Artifact, valid_sha256},
    resident_graph_store::upload_authenticated_file,
    rotated_two_bit::RotatedTwoBitCodec,
    sq8_source::build_sq8_source_with_ids,
    two_bit_build::TwoBitGenerationBuilder,
    two_bit_generation::{Manifest, TwoBitGenerationError, TwoBitGenerationLimits},
    two_bit_mutations::{TwoBitMutationLimits, read_two_bit_mutations, seal_two_bit_mutations},
    two_bit_source::{SourceBuildError, TwoBitSource, read_authenticated},
    two_bit_store::{
        EmptyRoot, TwoBitHead, TwoBitStoreError, publish_empty_two_bit_generation,
        publish_two_bit_generation, read_two_bit_head,
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

/// Phase caps; runtime/allocator/transport and other query pins are caller charges.
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
    schema: String,
    index_prefix: String,
    base_root_sha256: String,
    base_generation: u64,
    base_epoch: u64,
    dimensions: usize,
    mutation_sha256: String,
    mutation_revision: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    schema: String,
    job_sha256: String,
    input_sha256: String,
    target_root_sha256: String,
    rows: usize,
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
fn disk_bound(rows: usize, dimensions: usize) -> Result<u64> {
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
    // also dominates serialized graph/centroids. Include page hashes and metadata.
    let graph = dimensions
        .checked_mul(32)
        .and_then(|n| n.checked_add(16384))
        .and_then(|n| n.checked_mul(rows.div_ceil(32)))
        .ok_or(bad("compaction disk geometry"))?;
    per_row
        .checked_mul(rows)
        .and_then(|n| n.checked_add(graph))
        .and_then(|n| n.checked_add(rows.div_ceil(256).checked_mul(32)?))
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
        let result = runtime.block_on(compact_owned(store.as_ref(), &prefix, &directory, options));
        drop(lock);
        result
    })
    .await
    .map_err(|_| bad("compaction worker failed"))?
}

async fn compact_owned(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    directory: &Path,
    options: TwoBitCompactionOptions,
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
        if job.schema != "borsuk-two-bit-compaction-job-v2"
            || job.index_prefix != prefix.as_ref()
            || job.base_root_sha256 != name
            || job.base_generation >= base.generation()
        {
            return Err(bad("obsolete compaction job binding"));
        }
        fs::remove_dir_all(entry.path())?;
    }
    let target_generation = base
        .generation()
        .checked_add(1)
        .ok_or(bad("compaction generation overflow"))?;
    let latest = read_two_bit_mutations(store, &base, base.dimensions(), options.mutations).await?;
    let Some(latest) = latest else {
        return Ok(base);
    };
    let sealed = seal_two_bit_mutations(store, &base, Some(&latest), options.mutations).await?;
    drop(latest);
    let job_dir = directory.join(base.root_sha256());
    fs::create_dir_all(&job_dir)?;
    let (authority, _) =
        crate::two_bit_mutations::require_sealed_two_bit_mutations(store, &base).await?;
    let job = Job {
        schema: "borsuk-two-bit-compaction-job-v2".into(),
        index_prefix: prefix.as_ref().into(),
        base_root_sha256: base.root_sha256().into(),
        base_generation: base.generation(),
        base_epoch: authority.epoch,
        dimensions: base.dimensions(),
        mutation_sha256: sealed.sha256().into(),
        mutation_revision: sealed.revision(),
    };
    let job_path = job_dir.join("job.json");
    let job_bytes = serde_json::to_vec(&job).map_err(|_| bad("compaction job"))?;
    if job_path.exists() {
        let (mut previous, bytes): (Job, _) = read_json(&job_path)?;
        if bytes != job_bytes {
            let old_epoch = previous.base_epoch;
            previous.base_epoch = job.base_epoch;
            if old_epoch >= job.base_epoch
                || serde_json::to_vec(&previous).map_err(|_| bad("compaction job"))? != job_bytes
            {
                return Err(bad("compaction job changed"));
            }
            // GC advanced the authority. Old prepared keys may have a pending DELETE.
            fs::remove_dir_all(&job_dir)?;
            fs::create_dir_all(&job_dir)?;
            write_json(&job_path, &job)?;
        }
    } else {
        write_json(&job_path, &job)?;
    }
    let input_dir = job_dir.join("input");
    let generation_dir = job_dir.join("generation");
    let ready_path = job_dir.join("ready.json");
    let recovered_ready = if ready_path.exists() {
        let (ready, _): (Ready, _) = read_json(&ready_path)?;
        if ready.schema != "borsuk-two-bit-compaction-ready-v1"
            || ready.job_sha256 != hash(&job_bytes)
            || !valid_sha256(&ready.target_root_sha256)
        {
            return Err(bad("compaction ready binding"));
        }
        if disk_bound(ready.rows, job.dimensions)? > options.source.max_disk_bytes {
            return Err(bad("compaction build disk cap"));
        }
        let (input, _) = verified_input(&input_dir, &job, Some(&ready.input_sha256))?;
        if input.rows != ready.rows {
            return Err(bad("compaction ready rows"));
        }
        if ready.rows > 0 {
            let root = prepared_manifest(&generation_dir, &ready)?;
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
        for path in [&input_dir, &generation_dir] {
            if path.exists() {
                fs::remove_dir_all(path)?;
            }
        }
        let sq8_path = job_dir.join("sq8.bin");
        if sq8_path.exists() {
            fs::remove_file(&sq8_path)?;
        }
        let input = prepare_two_bit_compaction(store, &base, &sealed, &input_dir, options.source)
            .await
            .map_err(|e| e.error)?;
        if disk_bound(input.rows, input.dimensions)? > options.source.max_disk_bytes {
            return Err(bad("compaction build disk cap"));
        }
        let (_, input_sha) = verified_input(&input_dir, &job, None)?;
        let target = if input.rows == 0 {
            hash(
                &serde_json::to_vec(&EmptyRoot {
                    schema: "borsuk-two-bit-empty-generation-v2".into(),
                    generation: target_generation,
                    dimensions: base.dimensions(),
                    base_epoch: job.base_epoch,
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
            upload_authenticated_file(
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
            let etag = store
                .head(&key)
                .await?
                .e_tag
                .ok_or(bad("compaction SQ8 ETag"))?;
            let root = TwoBitGenerationBuilder {
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
            }
            .build_with_order(&order, &generation_dir, build_budget)?;
            root
        };
        let ready = Ready {
            schema: "borsuk-two-bit-compaction-ready-v1".into(),
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
                schema: "borsuk-two-bit-empty-generation-v2".into(),
                generation: target_generation,
                dimensions: base.dimensions(),
                base_epoch: job.base_epoch,
            })
            .map_err(|_| bad("empty root"))?,
        );
        if ready.target_root_sha256 != expected {
            return Err(bad("empty ready root"));
        }
        publish_empty_two_bit_generation(
            store,
            prefix,
            base.dimensions(),
            target_generation,
            Some(&base),
        )
        .await?
    } else {
        let root = prepared_manifest(&generation_dir, &ready)?;
        if root.generation != target_generation
            || root.canonical.rows != ready.rows
            || root.canonical.dimensions != base.dimensions()
        {
            return Err(bad("compaction target binding"));
        }
        let limits = TwoBitGenerationLimits {
            already_pinned_bytes: options
                .generation
                .already_pinned_bytes
                .checked_add(sealed.resident_payload_bytes() as u64)
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
