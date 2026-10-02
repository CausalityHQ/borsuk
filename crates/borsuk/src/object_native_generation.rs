//! A source-only router and authenticated remote SQ8 pages for one generation.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use futures_util::{StreamExt, future::try_join_all};
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::budgeted_page_rank::{
    BudgetedPageError, BudgetedPagePlan, choose_budgeted_pages, choose_budgeted_pages_sparse,
};
use crate::pq64_nominee::Pq64Error;
use crate::pq64_router_artifact::{RouterArtifactError, SourceRouterArtifact, load_source_router};
use crate::sq8_page_authority::{PageAuthority, PageError};
use crate::sq8_s3_range::{
    OneAttemptS3, RangeFetchError, RankedSq8, RankedSq8Failure, Sq8ReadStats,
};
use crate::unit_centroid_graph::{UnitCentroidGraph, UnitCentroidGraphError};
use crate::unit_centroid_pages::{UnitCentroidError, UnitCentroidPages};

const MAX_MANIFEST: usize = 64 * 1024;
const METADATA_RANGE_BYTES: u64 = 4 * 1024 * 1024;
const METADATA_PARALLEL_GETS: u64 = 8;
const METADATA_WAVE_OBJECTS: usize = 8;
const METADATA_FILES: [&str; 11] = [
    "manifest.json",
    "page_manifest.json",
    "page_digests.bin",
    "centroids.bin",
    "graph.bin",
    "router/manifest.json",
    "router/summaries.bin",
    "router/books.bin",
    "router/codes.bin",
    "router/low.bin",
    "router/step.bin",
];

#[derive(Debug)]
pub enum ObjectNativeOpenError {
    Io(std::io::Error),
    Store(object_store::Error),
    Invalid(&'static str),
    HashMismatch(&'static str),
    Router(RouterArtifactError),
    Page(PageError),
    Centroid(UnitCentroidError),
    Graph(UnitCentroidGraphError),
}

#[derive(Debug)]
pub enum ObjectNativePlanError {
    Router(Pq64Error),
    Graph(UnitCentroidGraphError),
    Centroid(UnitCentroidError),
    Budget(BudgetedPageError),
    Arithmetic,
}

#[derive(Debug)]
pub enum ObjectNativeSearchError {
    Plan(ObjectNativePlanError),
    Read(RankedSq8Failure),
}

pub struct ObjectNativeSearchResult {
    pub plan: BudgetedPagePlan,
    pub ranked: RankedSq8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: String,
    generation: u64,
    rows: usize,
    dimensions: usize,
    page_rows: usize,
    unit_rows: usize,
    router_manifest_sha256: String,
    page_manifest_sha256: String,
    centroids_sha256: String,
    graph_sha256: String,
    graph_resident_bytes: u64,
    sq8_object_sha256: String,
    sq8_object_key: String,
    sq8_etag: String,
}

/// Admission estimate for loading overlap and concurrent query buffers.
/// Measure cgroup RSS separately; allocator and transport overhead can differ.
#[derive(Clone, Copy)]
pub struct ObjectNativeLimits {
    pub max_memory_bytes: u64,
    pub max_active_queries: u64,
    pub max_query_bytes: u64,
    pub max_query_gets: usize,
    pub max_parallel_gets: usize,
    pub max_router_regions: usize,
    pub max_router_shortlist: usize,
    pub already_pinned_bytes: u64,
}

/// Immutable router, page hashes and conditional remote object identity.
/// The SQ8 vector plane is never loaded by this generation.
pub struct ObjectNativeGeneration {
    router: SourceRouterArtifact,
    pages: PageAuthority,
    centroids: UnitCentroidPages,
    graph: UnitCentroidGraph,
    object_key: ObjectPath,
    etag: String,
    max_query_bytes: usize,
    max_query_gets: usize,
    max_parallel_gets: usize,
    max_router_regions: usize,
    max_router_shortlist: usize,
    query_slots: Semaphore,
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(crate) fn valid_object_key(key: &str, hash: &str) -> bool {
    let parts = key.split('/').collect::<Vec<_>>();
    parts.len() >= 2
        && parts[parts.len() - 2] == "objects"
        && parts[parts.len() - 1] == hash
        && parts.iter().all(|part| {
            !part.is_empty()
                && *part != "."
                && *part != ".."
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        })
}

fn read_capped(path: &Path, cap: u64) -> Result<Vec<u8>, ObjectNativeOpenError> {
    let file = File::open(path).map_err(ObjectNativeOpenError::Io)?;
    let size = file.metadata().map_err(ObjectNativeOpenError::Io)?.len();
    if size == 0 || size > cap {
        return Err(ObjectNativeOpenError::Invalid("artifact length"));
    }
    let mut bytes = Vec::new();
    file.take(cap.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(ObjectNativeOpenError::Io)?;
    if bytes.len() as u64 != size {
        return Err(ObjectNativeOpenError::Invalid("artifact length"));
    }
    Ok(bytes)
}

/// Fixed relative metadata names may contain directory separators.
pub(crate) fn metadata_location(prefix: &ObjectPath, name: &str) -> ObjectPath {
    name.split('/')
        .fold(prefix.clone(), |path, segment| path.join(segment))
}

/// Bounded per-object accounting for authenticated remote metadata staging.
#[derive(Debug, serde::Serialize)]
pub struct MetadataReadStats {
    /// Fixed metadata filename under the generation prefix.
    pub name: String,
    /// Root is wave zero; child waves retain the original roster order.
    pub metadata_wave: u64,
    /// Actual critical wall of this whole wave, repeated for every member.
    /// Count once per wave, never sum overlapping per-object waits.
    pub metadata_wave_wall_ns: u128,
    /// Remote bytes streamed to the scratch file; excludes a local root seed.
    pub bytes: u64,
    /// Authenticated root bytes copied locally, never network transfer.
    pub reused_root_bytes: u64,
    /// Immutable head allocation charged separately from payload buffers.
    pub retained_root_bytes: u64,
    /// Local root-seed authentication time; zero for remote payloads.
    pub local_auth_wall_ns: u128,
    /// Local root-seed file creation/copy/flush time; zero for remote payloads.
    pub local_copy_wall_ns: u128,
    /// Number of transport chunks received.
    pub chunks: u64,
    /// HEAD admission wait, in nanoseconds.
    pub head_wall_ns: u128,
    /// Logical HEAD calls; excludes SDK retries (zero for authenticated lengths).
    pub logical_head_requests: u64,
    /// Logical payload GET calls; excludes SDK retries.
    pub logical_get_requests: u64,
    /// Conservative staging payload buffer bound, not measured RSS.
    /// Excludes transport buffers; at most 32 MiB of range buffers.
    pub payload_buffer_bound_bytes: u64,
    /// Small-object GET response-header wait; zero for ranged objects.
    /// Ranged response-header waits are included in stream_wall_ns.
    pub get_wall_ns: u128,
    /// Whole payload critical interval and output, including flush.
    /// Includes concurrent range GETs, never a sum of overlapping waits.
    pub stream_wall_ns: u128,
    /// File writes and flush; included in stream_wall_ns.
    pub write_wall_ns: u128,
}

// Collect one exact range without trusting the response length for allocation.
async fn fetch_metadata_range(
    store: &dyn ObjectStore,
    location: &ObjectPath,
    expected: u64,
    range: std::ops::Range<u64>,
) -> Result<(Vec<u8>, u64), ObjectNativeOpenError> {
    let fetched = store
        .get_opts(location, GetOptions::new().with_range(Some(range.clone())))
        .await
        .map_err(ObjectNativeOpenError::Store)?;
    if fetched.meta.size != expected || fetched.range != range {
        return Err(ObjectNativeOpenError::Invalid("remote metadata length"));
    }
    let length = (range.end - range.start) as usize;
    let mut bytes = Vec::with_capacity(length);
    let mut chunks = 0;
    let mut stream = fetched.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(ObjectNativeOpenError::Store)?;
        if chunk.len() > length - bytes.len() {
            return Err(ObjectNativeOpenError::Invalid("remote metadata length"));
        }
        bytes.extend_from_slice(&chunk);
        chunks += 1;
    }
    if bytes.len() != length {
        return Err(ObjectNativeOpenError::Invalid("remote metadata length"));
    }
    Ok((bytes, chunks))
}

/// Stage a fixed metadata set to owned scratch; authenticate the root first.
/// Child identities and decoded memory are checked by the generation opener.
pub(crate) async fn stage_generation_metadata(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    trusted_sha256: &str,
    max_bytes: u64,
    names: &[&str],
    scratch_parent: &Path,
) -> Result<(tempfile::TempDir, Vec<MetadataReadStats>), ObjectNativeOpenError> {
    stage_metadata(
        store,
        prefix,
        trusted_sha256,
        max_bytes,
        names,
        scratch_parent,
        false,
        None,
    )
    .await
}

/// Derive the v7 startup roster only after authenticating its bounded root.
pub(crate) async fn stage_two_bit_metadata(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    trusted_sha256: &str,
    max_bytes: u64,
    scratch_parent: &Path,
    head: Option<&crate::two_bit_store::TwoBitHead>,
) -> Result<(tempfile::TempDir, Vec<MetadataReadStats>), ObjectNativeOpenError> {
    stage_metadata(
        store,
        prefix,
        trusted_sha256,
        max_bytes,
        &["manifest.json"],
        scratch_parent,
        true,
        head,
    )
    .await
}
// Called only after every object in the wave has passed length admission.
async fn stage_metadata_payload(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    scratch: &Path,
    name: &str,
    expected: u64,
    parallel_gets: u64,
    trusted_sha256: &str,
) -> Result<MetadataReadStats, ObjectNativeOpenError> {
    let location = metadata_location(prefix, name);
    let ranged = expected > METADATA_RANGE_BYTES;
    let get_started = std::time::Instant::now();
    let fetched = if ranged {
        None
    } else {
        let fetched = store
            .get(&location)
            .await
            .map_err(ObjectNativeOpenError::Store)?;
        if fetched.meta.size != expected || fetched.range != (0..expected) {
            return Err(ObjectNativeOpenError::Invalid("remote metadata length"));
        }
        Some(fetched)
    };
    let get_wall_ns = if ranged {
        0
    } else {
        get_started.elapsed().as_nanos()
    };
    let local = scratch.join(name);
    // Create directories/files synchronously: cancellation cannot leave a
    // queued directory creation that recreates scratch after TempDir drops.
    if let Some(parent) = local.parent() {
        fs::create_dir_all(parent).map_err(ObjectNativeOpenError::Io)?;
    }
    // ponytail: direct writes block this worker; use zero-copy async output
    // if runtime stalls matter. No copied payload or detached write task.
    let mut output = File::create(&local).map_err(ObjectNativeOpenError::Io)?;
    let mut count = 0_u64;
    let mut digest = Sha256::new();
    let stream_started = std::time::Instant::now();
    let mut chunks = 0_u64;
    let mut write_wall_ns = 0_u128;
    if let Some(fetched) = fetched {
        let mut stream = fetched.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(ObjectNativeOpenError::Store)?;
            count = count
                .checked_add(chunk.len() as u64)
                .ok_or(ObjectNativeOpenError::Invalid("remote metadata length"))?;
            if count > expected {
                return Err(ObjectNativeOpenError::Invalid("remote metadata length"));
            }
            if name == "manifest.json" {
                digest.update(&chunk);
            }
            chunks += 1;
            let write_started = std::time::Instant::now();
            output
                .write_all(&chunk)
                .map_err(ObjectNativeOpenError::Io)?;
            write_wall_ns += write_started.elapsed().as_nanos();
        }
    } else {
        while count < expected {
            let batch_end = count
                .saturating_add(METADATA_RANGE_BYTES * parallel_gets)
                .min(expected);
            // Finish and drain this batch before admitting another, so
            // output backpressure cannot accumulate completed range buffers.
            let batch = try_join_all(
                (count..batch_end)
                    .step_by(METADATA_RANGE_BYTES as usize)
                    .map(|start| {
                        fetch_metadata_range(
                            store,
                            &location,
                            expected,
                            start..start.saturating_add(METADATA_RANGE_BYTES).min(expected),
                        )
                    }),
            )
            .await?;
            for (bytes, range_chunks) in batch {
                count += bytes.len() as u64;
                chunks += range_chunks;
                let write_started = std::time::Instant::now();
                output
                    .write_all(&bytes)
                    .map_err(ObjectNativeOpenError::Io)?;
                write_wall_ns += write_started.elapsed().as_nanos();
            }
        }
    }
    if count != expected {
        return Err(ObjectNativeOpenError::Invalid("remote metadata length"));
    }
    let flush_started = std::time::Instant::now();
    output.flush().map_err(ObjectNativeOpenError::Io)?;
    write_wall_ns += flush_started.elapsed().as_nanos();
    let stats = MetadataReadStats {
        metadata_wave: 0,
        metadata_wave_wall_ns: 0,
        name: name.to_owned(),
        bytes: count,
        reused_root_bytes: 0,
        retained_root_bytes: 0,
        local_auth_wall_ns: 0,
        local_copy_wall_ns: 0,
        chunks,
        head_wall_ns: 0,
        logical_head_requests: 0,
        logical_get_requests: if ranged {
            expected.div_ceil(METADATA_RANGE_BYTES)
        } else {
            1
        },
        payload_buffer_bound_bytes: expected.min(METADATA_RANGE_BYTES * parallel_gets),
        get_wall_ns,
        stream_wall_ns: stream_started.elapsed().as_nanos(),
        write_wall_ns,
    };
    drop(output);
    if name == "manifest.json" && format!("{:x}", digest.finalize()) != trusted_sha256 {
        return Err(ObjectNativeOpenError::HashMismatch("generation manifest"));
    }
    Ok(stats)
}

async fn stage_metadata(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    trusted_sha256: &str,
    max_bytes: u64,
    names: &[&str],
    scratch_parent: &Path,
    two_bit: bool,
    head: Option<&crate::two_bit_store::TwoBitHead>,
) -> Result<(tempfile::TempDir, Vec<MetadataReadStats>), ObjectNativeOpenError> {
    if !is_hash(trusted_sha256) || max_bytes == 0 {
        return Err(ObjectNativeOpenError::Invalid(
            "trusted digest or memory cap",
        ));
    }
    if names.first() != Some(&"manifest.json") {
        return Err(ObjectNativeOpenError::Invalid("metadata root order"));
    }
    let auth_started = std::time::Instant::now();
    let seed = head
        .map(|head| {
            head.authenticated_root(prefix, trusted_sha256)
                .map_err(|_| ObjectNativeOpenError::Invalid("root seed admission"))
        })
        .transpose()?;
    let local_auth_wall_ns = if seed.is_some() {
        auth_started.elapsed().as_nanos()
    } else {
        0
    };
    let scratch = tempfile::tempdir_in(scratch_parent).map_err(ObjectNativeOpenError::Io)?;
    let mut total = 0_u64;
    let mut stats = Vec::with_capacity(names.len());
    let mut authenticated: Option<crate::two_bit_generation::Manifest> = None;
    let mut names = names.to_vec();
    let mut index = 0;
    let mut wave = 0;
    while index < names.len() {
        let end = if index == 0 {
            1
        } else {
            (index + METADATA_WAVE_OBJECTS).min(names.len())
        };
        let wave_started = std::time::Instant::now();
        let root = authenticated.as_ref();
        let admitted = try_join_all(names[index..end].iter().map(|&name| async move {
            let location = metadata_location(prefix, name);
            let limit = if two_bit && name == "router/root.bin" {
                match root.map(|root| &root.discovery) {
                    Some(crate::two_bit_generation::Discovery::Semantic { profile, .. }) => {
                        profile.root_cap() as u64
                    }
                    _ => return Err(ObjectNativeOpenError::Invalid("router profile descriptor")),
                }
            } else if name.ends_with(".json") {
                MAX_MANIFEST as u64
            } else {
                u64::MAX
            };
            let exact = if let Some(root) = root {
                let rows = root.canonical.rows;
                let dimensions = root.canonical.dimensions;
                let overflow = ObjectNativeOpenError::Invalid("descriptor metadata length");
                match name {
                    "plane/mean.bin" => Some(dimensions.checked_mul(4).ok_or(overflow)?),
                    "plane/page_digests.bin" => {
                        Some(rows.div_ceil(32).checked_mul(32).ok_or(overflow)?)
                    }
                    "page_digests.bin" => Some(rows.div_ceil(256).checked_mul(32).ok_or(overflow)?),
                    "router/root.bin" => match &root.discovery {
                        crate::two_bit_generation::Discovery::Semantic { root_bytes, .. } => {
                            Some(*root_bytes)
                        }
                        _ => None,
                    },
                    "router/membership.bin" => match &root.discovery {
                        crate::two_bit_generation::Discovery::Semantic {
                            membership_bytes, ..
                        } => Some(*membership_bytes),
                        _ => None,
                    },
                    _ => None,
                }
            } else {
                seed.map(<[u8]>::len)
            };
            let (expected, logical_head_requests, head_wall_ns) = match exact {
                Some(length) => (length as u64, 0, 0),
                None => {
                    let head_started = std::time::Instant::now();
                    let head = store
                        .head(&location)
                        .await
                        .map_err(ObjectNativeOpenError::Store)?;
                    (head.size, 1, head_started.elapsed().as_nanos())
                }
            };
            if expected == 0 || expected > limit {
                return Err(ObjectNativeOpenError::Invalid("remote metadata length"));
            }
            Ok((name, expected, logical_head_requests, head_wall_ns))
        }))
        .await?;
        // Admit the entire wave before creating any payload GET future.
        let mut admitted_total = total;
        for (_, expected, _, _) in &admitted {
            admitted_total = admitted_total
                .checked_add(*expected)
                .filter(|&n| n <= max_bytes)
                .ok_or(ObjectNativeOpenError::Invalid("remote metadata length"))?;
        }
        let parallel_gets = METADATA_PARALLEL_GETS / admitted.len() as u64;
        let mut batch = try_join_all(admitted.into_iter().map(
            |(name, expected, heads, head_wall)| {
                let scratch = scratch.path();
                async move {
                    if name == "manifest.json"
                        && let Some(seed) = seed
                    {
                        let copy_started = std::time::Instant::now();
                        let mut output =
                            File::create(scratch.join(name)).map_err(ObjectNativeOpenError::Io)?;
                        output.write_all(seed).map_err(ObjectNativeOpenError::Io)?;
                        output.flush().map_err(ObjectNativeOpenError::Io)?;
                        return Ok(MetadataReadStats {
                            name: name.to_owned(),
                            metadata_wave: 0,
                            metadata_wave_wall_ns: 0,
                            bytes: 0,
                            reused_root_bytes: seed.len() as u64,
                            retained_root_bytes: seed.len() as u64,
                            local_auth_wall_ns,
                            local_copy_wall_ns: copy_started.elapsed().as_nanos(),
                            chunks: 0,
                            head_wall_ns: 0,
                            logical_head_requests: 0,
                            logical_get_requests: 0,
                            payload_buffer_bound_bytes: 0,
                            get_wall_ns: 0,
                            stream_wall_ns: 0,
                            write_wall_ns: 0,
                        });
                    }
                    let mut entry = stage_metadata_payload(
                        store,
                        prefix,
                        scratch,
                        name,
                        expected,
                        parallel_gets,
                        trusted_sha256,
                    )
                    .await?;
                    entry.logical_head_requests = heads;
                    entry.head_wall_ns = head_wall;
                    Ok(entry)
                }
            },
        ))
        .await?;
        if index == 0 {
            if two_bit {
                let root: crate::two_bit_generation::Manifest = serde_json::from_slice(
                    &fs::read(scratch.path().join("manifest.json"))
                        .map_err(ObjectNativeOpenError::Io)?,
                )
                .map_err(|_| ObjectNativeOpenError::Invalid("two-bit root schema"))?;
                if root.schema != crate::two_bit_generation::SCHEMA
                    || !root.canonical.valid()
                    || !root
                        .discovery
                        .valid(root.canonical.rows, root.canonical.dimensions)
                {
                    return Err(ObjectNativeOpenError::Invalid(
                        "two-bit discovery descriptor",
                    ));
                }
                if let crate::two_bit_generation::Discovery::Semantic {
                    root_bytes,
                    membership_bytes,
                    ..
                } = &root.discovery
                {
                    let modeled = (*root_bytes as u64)
                        .checked_mul(35)
                        .and_then(|n| n.checked_add(*membership_bytes as u64 * 8))
                        .and_then(|n| n.checked_add(131072));
                    if modeled.is_none_or(|n| n > max_bytes) {
                        return Err(ObjectNativeOpenError::Invalid("router metadata admission"));
                    }
                }
                names = root.discovery.files(true);
                authenticated = Some(root);
            }
            if !two_bit {
                let root: serde_json::Value = serde_json::from_slice(
                    &fs::read(scratch.path().join("manifest.json"))
                        .map_err(ObjectNativeOpenError::Io)?,
                )
                .map_err(|_| ObjectNativeOpenError::Invalid("generation root schema"))?;
                if !root.is_object() {
                    return Err(ObjectNativeOpenError::Invalid("generation root schema"));
                }
            }
        }
        let wave_wall_ns =
            wave_started.elapsed().as_nanos() + if index == 0 { local_auth_wall_ns } else { 0 };
        for entry in &mut batch {
            entry.metadata_wave = wave;
            entry.metadata_wave_wall_ns = wave_wall_ns;
        }
        stats.extend(batch);
        total = admitted_total;
        index = end;
        wave += 1;
    }
    Ok((scratch, stats))
}

impl ObjectNativeGeneration {
    /// Download only routing metadata under an authorized, immutable prefix.
    /// The caller supplies a trusted root digest and scratch space; source
    /// vectors and the SQ8 object are never downloaded by this method.
    pub async fn open_remote(
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        trusted_sha256: &str,
        limits: ObjectNativeLimits,
        scratch_parent: &Path,
    ) -> Result<Self, ObjectNativeOpenError> {
        let (scratch, _) = stage_generation_metadata(
            store,
            prefix,
            trusted_sha256,
            limits.max_memory_bytes,
            &METADATA_FILES,
            scratch_parent,
        )
        .await?;
        Self::open(scratch.path(), trusted_sha256, limits)
    }

    fn primary_rows(
        &self,
        query: &[f32],
        regions: usize,
        shortlist: usize,
        primary_count: usize,
    ) -> Result<Vec<usize>, ObjectNativePlanError> {
        if regions > self.max_router_regions
            || shortlist > self.max_router_shortlist
            || primary_count == 0
            || primary_count > shortlist
        {
            return Err(ObjectNativePlanError::Router(Pq64Error::InvalidRequest));
        }
        let mut nominees = self
            .router
            .router
            .nominate(query, regions, shortlist)
            .map_err(ObjectNativePlanError::Router)?;
        nominees.truncate(primary_count);
        Ok(nominees)
    }

    /// Open local routing metadata only. `trusted_sha256` must come from an
    /// authorized generation pointer, outside this API's trust boundary.
    pub fn open(
        root: &Path,
        trusted_sha256: &str,
        limits: ObjectNativeLimits,
    ) -> Result<Self, ObjectNativeOpenError> {
        if !is_hash(trusted_sha256) {
            return Err(ObjectNativeOpenError::Invalid("trusted digest"));
        }
        let raw = read_capped(&root.join("manifest.json"), MAX_MANIFEST as u64)?;
        if sha256(&raw) != trusted_sha256 {
            return Err(ObjectNativeOpenError::HashMismatch("generation manifest"));
        }
        let manifest: Manifest = serde_json::from_slice(&raw)
            .map_err(|_| ObjectNativeOpenError::Invalid("generation schema"))?;
        if manifest.schema != "borsuk-object-native-generation-v2"
            || manifest.generation == 0
            || manifest.rows == 0
            || manifest.dimensions == 0
            || manifest.page_rows != 256
            || manifest.unit_rows == 0
            || manifest.page_rows % manifest.unit_rows != 0
            || !is_hash(&manifest.router_manifest_sha256)
            || !is_hash(&manifest.page_manifest_sha256)
            || !is_hash(&manifest.centroids_sha256)
            || !is_hash(&manifest.graph_sha256)
            || manifest.graph_resident_bytes == 0
            || !is_hash(&manifest.sq8_object_sha256)
            || !valid_object_key(&manifest.sq8_object_key, &manifest.sq8_object_sha256)
            || manifest.sq8_etag.is_empty()
            || manifest.sq8_etag.chars().any(char::is_control)
            || limits.max_active_queries == 0
            || limits.max_query_bytes == 0
            || limits.max_query_gets == 0
            || limits.max_parallel_gets == 0
            || limits.max_router_regions == 0
            || limits.max_router_regions > manifest.rows.div_ceil(manifest.page_rows)
            || limits.max_router_shortlist == 0
            || limits.max_router_shortlist > manifest.rows
            || limits.max_active_queries > usize::MAX as u64
            || limits.max_query_bytes > usize::MAX as u64
        {
            return Err(ObjectNativeOpenError::Invalid("generation identity"));
        }
        let router_root = root.join("router");
        let router_manifest = read_capped(&router_root.join("manifest.json"), MAX_MANIFEST as u64)?;
        if sha256(&router_manifest) != manifest.router_manifest_sha256 {
            return Err(ObjectNativeOpenError::HashMismatch("router manifest"));
        }
        let page_manifest = read_capped(&root.join("page_manifest.json"), MAX_MANIFEST as u64)?;
        if sha256(&page_manifest) != manifest.page_manifest_sha256 {
            return Err(ObjectNativeOpenError::HashMismatch("page manifest"));
        }
        let page_count = manifest.rows.div_ceil(manifest.page_rows);
        let sidecar_bytes = page_count
            .checked_mul(32)
            .ok_or(ObjectNativeOpenError::Invalid("page count"))?;
        let sidecar_size = fs::metadata(root.join("page_digests.bin"))
            .map_err(ObjectNativeOpenError::Io)?
            .len();
        if sidecar_size != sidecar_bytes as u64 {
            return Err(ObjectNativeOpenError::Invalid("page digest length"));
        }
        let centroid_size = fs::metadata(root.join("centroids.bin"))
            .map_err(ObjectNativeOpenError::Io)?
            .len();
        let expected_centroid_size = manifest
            .rows
            .div_ceil(manifest.unit_rows)
            .checked_mul(manifest.dimensions)
            .and_then(|v| v.checked_mul(2))
            .and_then(|v| v.checked_add(32))
            .ok_or(ObjectNativeOpenError::Invalid("centroid size"))?;
        if centroid_size != expected_centroid_size as u64 {
            return Err(ObjectNativeOpenError::Invalid("centroid length"));
        }
        let graph_size = fs::metadata(root.join("graph.bin"))
            .map_err(ObjectNativeOpenError::Io)?
            .len();
        // Three copies allow authenticated reads and decoding. Three query
        // buffers cover response collection/transport; ranking is charged by row count.
        // PQ nomination materializes one tuple per selected-region row.
        // Retiring generations are charged through `already_pinned_bytes`.
        let router_disk_bytes = ["summaries", "books", "codes", "low", "step"]
            .into_iter()
            .try_fold(0_u64, |total, name| {
                let size = fs::metadata(router_root.join(format!("{name}.bin")))
                    .map_err(ObjectNativeOpenError::Io)?
                    .len();
                total
                    .checked_add(size)
                    .ok_or(ObjectNativeOpenError::Invalid("router size"))
            })?;
        let planner_bytes = page_count
            .checked_mul(16)
            .and_then(|v| {
                v.checked_add(
                    limits
                        .max_router_regions
                        .checked_mul(manifest.page_rows)?
                        .checked_mul(16)?,
                )
            })
            .and_then(|v| v.checked_add(limits.max_router_shortlist.checked_mul(1200)?))
            .and_then(|v| v.checked_add(64 * 256 * 4))
            .ok_or(ObjectNativeOpenError::Invalid("planner memory"))?
            as u64;
        let query_memory = crate::returned_sq8::query_payload_bytes(
            crate::exact_sq8_nominee::Sq8Geometry {
                rows: manifest.rows,
                dimensions: manifest.dimensions,
            },
            limits.max_query_bytes,
            planner_bytes,
            limits.max_active_queries,
        )
        .map_err(|_| ObjectNativeOpenError::Invalid("query memory"))?;
        let modeled = router_disk_bytes
            .checked_add(sidecar_size)
            .and_then(|v| v.checked_mul(3))
            .and_then(|v| v.checked_add(centroid_size.checked_mul(3)?))
            .and_then(|v| v.checked_add(graph_size))
            .and_then(|v| v.checked_add(manifest.graph_resident_bytes))
            .and_then(|v| v.checked_add(query_memory))
            .and_then(|v| v.checked_add(limits.already_pinned_bytes))
            .ok_or(ObjectNativeOpenError::Invalid("memory arithmetic"))?;
        if modeled > limits.max_memory_bytes {
            return Err(ObjectNativeOpenError::Invalid("memory cap"));
        }
        let router = load_source_router(&router_root, &manifest.router_manifest_sha256)
            .map_err(ObjectNativeOpenError::Router)?;
        let sidecar = read_capped(&root.join("page_digests.bin"), sidecar_size)?;
        let pages = PageAuthority::load(&page_manifest, &manifest.page_manifest_sha256, &sidecar)
            .map_err(ObjectNativeOpenError::Page)?;
        let centroid_blob = read_capped(&root.join("centroids.bin"), centroid_size)?;
        if sha256(&centroid_blob) != manifest.centroids_sha256 {
            return Err(ObjectNativeOpenError::HashMismatch("centroids"));
        }
        let centroids =
            UnitCentroidPages::decode(&centroid_blob).map_err(ObjectNativeOpenError::Centroid)?;
        let graph_blob = read_capped(&root.join("graph.bin"), graph_size)?;
        if sha256(&graph_blob) != manifest.graph_sha256 {
            return Err(ObjectNativeOpenError::HashMismatch("graph"));
        }
        let graph = UnitCentroidGraph::decode_bounded(
            &graph_blob,
            &centroid_blob,
            &centroids,
            usize::try_from(manifest.graph_resident_bytes)
                .map_err(|_| ObjectNativeOpenError::Invalid("graph memory"))?,
        )
        .map_err(ObjectNativeOpenError::Graph)?;
        if UnitCentroidGraph::preflight_resident_bytes(&graph_blob, &centroids)
            .map_err(ObjectNativeOpenError::Graph)? as u64
            != manifest.graph_resident_bytes
        {
            return Err(ObjectNativeOpenError::Invalid("graph resident declaration"));
        }
        if router.generation != manifest.generation
            || router.router.rows() != manifest.rows
            || router.router.dimensions() != manifest.dimensions
            || router.router.page_rows() != manifest.page_rows
            || router.sq8_sha256 != manifest.sq8_object_sha256
            || pages.generation() != manifest.generation
            || pages.rows() != manifest.rows
            || pages.dimensions() != manifest.dimensions
            || pages.page_rows() != manifest.page_rows
            || pages.object_sha256() != manifest.sq8_object_sha256
            || centroids.rows() != manifest.rows
            || centroids.dimensions() != manifest.dimensions
            || centroids.page_rows() != manifest.page_rows
            || centroids.unit_rows() != manifest.unit_rows
            || graph.node_count() != centroids.unit_count()
        {
            return Err(ObjectNativeOpenError::Invalid("generation binding"));
        }
        Ok(Self {
            router,
            pages,
            centroids,
            graph,
            object_key: ObjectPath::from(manifest.sq8_object_key),
            etag: manifest.sq8_etag,
            max_query_bytes: limits.max_query_bytes as usize,
            max_query_gets: limits.max_query_gets,
            max_parallel_gets: limits.max_parallel_gets,
            max_router_regions: limits.max_router_regions,
            max_router_shortlist: limits.max_router_shortlist,
            query_slots: Semaphore::new(limits.max_active_queries as usize),
        })
    }

    pub fn router(&self) -> &SourceRouterArtifact {
        &self.router
    }
    pub fn pages(&self) -> &PageAuthority {
        &self.pages
    }
    pub fn centroids(&self) -> &UnitCentroidPages {
        &self.centroids
    }
    pub fn graph(&self) -> &UnitCentroidGraph {
        &self.graph
    }
    pub fn object_key(&self) -> &ObjectPath {
        &self.object_key
    }
    pub fn etag(&self) -> &str {
        &self.etag
    }

    /// PQ-primary nomination followed by cached sparse graph page expansion.
    /// Historical V155 used local SQ8-exact primaries; its recall does not
    /// transfer to this cold object-native proxy without a new measurement.
    /// The caller freezes `regions`, `shortlist`, `primary_count` and `beta`.
    pub fn plan_pages(
        &self,
        query: &[f32],
        regions: usize,
        shortlist: usize,
        primary_count: usize,
        beta: usize,
    ) -> Result<BudgetedPagePlan, ObjectNativePlanError> {
        let primary = self.primary_rows(query, regions, shortlist, primary_count)?;
        let primary_pages = primary
            .iter()
            .map(|row| row / self.pages.page_rows())
            .collect::<BTreeSet<_>>();
        let p = primary_pages.len();
        let found = self
            .graph
            .search_pages_seeded_with_scores(
                &self.centroids,
                query,
                &primary_pages.iter().copied().collect::<Vec<_>>(),
                p.checked_mul(4).ok_or(ObjectNativePlanError::Arithmetic)?,
                p.checked_mul(16).ok_or(ObjectNativePlanError::Arithmetic)?,
            )
            .map_err(ObjectNativePlanError::Graph)?;
        let mut pages = primary_pages;
        pages.extend(found.pages.iter().map(|&(page, _)| page));
        let (scored, _) = self
            .centroids
            .score_pages_sparse_cached(
                query,
                &pages.into_iter().collect::<Vec<_>>(),
                &found.evaluated_scores,
            )
            .map_err(ObjectNativePlanError::Centroid)?;
        choose_budgeted_pages_sparse(
            &scored,
            &primary,
            self.pages.rows(),
            self.pages.dimensions(),
            beta,
            self.max_query_gets,
            self.max_query_bytes,
        )
        .map_err(ObjectNativePlanError::Budget)
    }

    /// Flat centroid quality/I/O diagnostic with the same PQ primary seeds.
    /// This scans every page and is not the scalable serving route.
    pub fn plan_pages_flat_control(
        &self,
        query: &[f32],
        regions: usize,
        shortlist: usize,
        primary_count: usize,
        beta: usize,
    ) -> Result<BudgetedPagePlan, ObjectNativePlanError> {
        let primary = self.primary_rows(query, regions, shortlist, primary_count)?;
        let scores = self
            .centroids
            .score_pages(query)
            .map_err(ObjectNativePlanError::Centroid)?;
        choose_budgeted_pages(
            &scores,
            &primary,
            self.pages.rows(),
            self.pages.dimensions(),
            beta,
            self.max_query_gets,
            self.max_query_bytes,
        )
        .map_err(ObjectNativePlanError::Budget)
    }

    /// Plan, fetch and rank from one pinned generation without a vector cache.
    pub async fn search(
        &self,
        reader: &OneAttemptS3,
        query: &[f32],
        regions: usize,
        shortlist: usize,
        beta: usize,
        top_k: usize,
    ) -> Result<ObjectNativeSearchResult, ObjectNativeSearchError> {
        let _permit = self.query_slots.acquire().await.map_err(|_| {
            ObjectNativeSearchError::Read(RankedSq8Failure {
                error: RangeFetchError::UnexpectedMetadata,
                stats: Sq8ReadStats::default(),
            })
        })?;
        let plan = self
            .plan_pages(query, regions, shortlist, top_k, beta)
            .map_err(ObjectNativeSearchError::Plan)?;
        let page_bytes = self
            .pages
            .page_rows()
            .checked_mul(self.pages.dimensions() + 12)
            .ok_or(ObjectNativeSearchError::Plan(
                ObjectNativePlanError::Arithmetic,
            ))?;
        let ranges = plan
            .ranges
            .iter()
            .map(|range| (range.start / page_bytes, (range.end - 1) / page_bytes))
            .collect::<Vec<_>>();
        let ranked = reader
            .rank_verified_sq8_pages(
                &self.object_key,
                &self.pages,
                &ranges,
                &self.etag,
                query,
                &self.router.low,
                &self.router.step,
                top_k,
                self.max_query_gets,
                self.max_query_bytes,
                self.max_parallel_gets,
            )
            .await
            .map_err(ObjectNativeSearchError::Read)?;
        Ok(ObjectNativeSearchResult { plan, ranked })
    }

    /// `ranges` are inclusive page pairs produced by an admitted plan.
    pub async fn rank_pages(
        &self,
        reader: &OneAttemptS3,
        ranges: &[(usize, usize)],
        query: &[f32],
        top_k: usize,
    ) -> Result<RankedSq8, RankedSq8Failure> {
        let _permit = self
            .query_slots
            .acquire()
            .await
            .map_err(|_| RankedSq8Failure {
                error: RangeFetchError::UnexpectedMetadata,
                stats: Sq8ReadStats::default(),
            })?;
        reader
            .rank_verified_sq8_pages(
                &self.object_key,
                &self.pages,
                ranges,
                &self.etag,
                query,
                &self.router.low,
                &self.router.step,
                top_k,
                self.max_query_gets,
                self.max_query_bytes,
                self.max_parallel_gets,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream::{self, BoxStream};
    use object_store::{
        CopyOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
        ObjectMeta, PutMultipartOptions, PutOptions, PutResult,
    };
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Clone, Copy, Debug, Default)]
    enum RangeFault {
        #[default]
        None,
        Short,
        Long,
        WrongRange,
        WrongSize,
        Error,
        Pending,
    }

    #[derive(Debug, Default)]
    struct StagingStore {
        inner: InMemory,
        fault: RangeFault,
        requests: Mutex<Vec<(String, bool, Option<std::ops::Range<u64>>)>>,
        active: Arc<AtomicUsize>,
        peak: AtomicUsize,
        buffered: Arc<AtomicUsize>,
        peak_buffered: AtomicUsize,
    }

    struct ActiveRange {
        active: Arc<AtomicUsize>,
        buffered: Arc<AtomicUsize>,
        bytes: usize,
    }
    impl Drop for ActiveRange {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
            self.buffered.fetch_sub(self.bytes, Ordering::SeqCst);
        }
    }

    impl std::fmt::Display for StagingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "metadata staging test store")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for StagingStore {
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let range = match &options.range {
                Some(GetRange::Bounded(range)) => Some(range.clone()),
                None => None,
                _ => panic!("unexpected metadata range"),
            };
            self.requests
                .lock()
                .unwrap()
                .push((location.to_string(), options.head, range.clone()));
            let options_head = options.head;
            let mut result = self.inner.get_opts(location, options).await?;
            if !options_head {
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(active, Ordering::SeqCst);
                let bytes = (result.range.end - result.range.start) as usize;
                let buffered = self.buffered.fetch_add(bytes, Ordering::SeqCst) + bytes;
                self.peak_buffered.fetch_max(buffered, Ordering::SeqCst);
                let guard = ActiveRange {
                    active: self.active.clone(),
                    buffered: self.buffered.clone(),
                    bytes,
                };
                let fault = if !location.as_ref().ends_with("/manifest.json")
                    && range.as_ref().is_none_or(|r| r.start == 0)
                {
                    self.fault
                } else {
                    RangeFault::None
                };
                if matches!(fault, RangeFault::WrongRange) {
                    result.range.start += 1;
                }
                if matches!(fault, RangeFault::WrongSize) {
                    result.meta.size += 1;
                }
                // Account for GET-header overlap as well as streamed range bodies.
                tokio::task::yield_now().await;
                let original = result.payload;
                result.payload = GetResultPayload::Stream(
                    stream::once(async move {
                        let _guard = guard;
                        tokio::task::yield_now().await;
                        if matches!(fault, RangeFault::Pending) {
                            std::future::pending::<()>().await;
                        }
                        if matches!(fault, RangeFault::Error) {
                            return Err(object_store::Error::Generic {
                                store: "staging test",
                                source: std::io::Error::other("payload failure").into(),
                            });
                        }
                        let GetResultPayload::Stream(mut body) = original else {
                            unreachable!()
                        };
                        let bytes = body.next().await.unwrap()?;
                        Ok(match fault {
                            RangeFault::Short => bytes.slice(..bytes.len() - 1),
                            RangeFault::Long => bytes::Bytes::from(vec![0; bytes.len() + 1]),
                            _ => bytes,
                        })
                    })
                    .boxed(),
                );
            }
            Ok(result)
        }
        async fn put_opts(
            &self,
            p: &ObjectPath,
            b: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(p, b, o).await
        }
        async fn put_multipart_opts(
            &self,
            p: &ObjectPath,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(p, o).await
        }
        fn delete_stream(
            &self,
            p: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(p)
        }
        fn list(
            &self,
            p: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(
            &self,
            p: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(
            &self,
            a: &ObjectPath,
            b: &ObjectPath,
            o: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(a, b, o).await
        }
    }

    async fn staging_fixture(
        fault: RangeFault,
        size: usize,
    ) -> (StagingStore, ObjectPath, Vec<u8>) {
        let store = StagingStore {
            fault,
            ..StagingStore::default()
        };
        let prefix = ObjectPath::from("profile/metadata");
        let payload = (0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        for (name, body) in [
            ("manifest.json", b"{}".as_slice()),
            ("nested/payload.bin", payload.as_slice()),
        ] {
            store
                .inner
                .put(&metadata_location(&prefix, name), body.to_vec().into())
                .await
                .unwrap();
        }
        (store, prefix, payload)
    }

    #[tokio::test]
    async fn opens_bound_metadata_without_vector_plane_and_rejects_changes() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("borsuk-object-native-{suffix}"));
        let router_dir = root.join("router");
        fs::create_dir_all(&router_dir).unwrap();
        let sq8 = vec![0u8; 256 * (64 + 12)];
        let sq8_hash = sha256(&sq8);
        let centroids_blob = UnitCentroidPages::build_from_sq8_reader(
            &mut sq8.as_slice(),
            256,
            64,
            32,
            256,
            &[0.0; 64],
            &[1.0; 64],
        )
        .unwrap();
        let centroids = UnitCentroidPages::decode(&centroids_blob).unwrap();
        let graph_blob = UnitCentroidGraph::build(&centroids, &centroids_blob)
            .unwrap()
            .encode()
            .unwrap();
        let graph_resident =
            UnitCentroidGraph::preflight_resident_bytes(&graph_blob, &centroids).unwrap();
        fs::write(root.join("centroids.bin"), &centroids_blob).unwrap();
        fs::write(root.join("graph.bin"), &graph_blob).unwrap();
        let digest = Sha256::digest(&sq8).to_vec();
        fs::write(root.join("page_digests.bin"), &digest).unwrap();
        let page_manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":1,
            "rows":256, "dimensions":64, "page_rows":256,
            "object_sha256":sq8_hash, "page_digest_sha256":sha256(&digest),
        }))
        .unwrap();
        fs::write(root.join("page_manifest.json"), &page_manifest).unwrap();
        let mut sections = serde_json::Map::new();
        for (name, mut bytes) in [
            ("summaries", vec![0; 64 * 4]),
            ("books", vec![0; 64 * 256 * 4]),
            ("codes", vec![0; 256 * 64]),
            ("low", vec![0; 64 * 4]),
            ("step", vec![0; 64 * 4]),
        ] {
            if name == "step" {
                for chunk in bytes.chunks_exact_mut(4) {
                    chunk.copy_from_slice(&1.0_f32.to_le_bytes());
                }
            }
            fs::write(router_dir.join(format!("{name}.bin")), &bytes).unwrap();
            sections.insert(
                name.to_owned(),
                serde_json::json!({
                    "bytes":bytes.len(), "sha256":sha256(&bytes),
                }),
            );
        }
        let router_manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-source-router-v2", "generation":1,
            "source_sha256":"a".repeat(64), "layout_sha256":"b".repeat(64),
            "sq8_sha256":sq8_hash,
            "geometry":{"rows":256,"dimensions":64,"page_rows":256,
                        "blocks_per_page":1,"subspaces":64,"pq_width":1,
                        "pq_partition":"balanced_floor_v1"},
            "sections":sections,
        }))
        .unwrap();
        fs::write(router_dir.join("manifest.json"), &router_manifest).unwrap();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-object-native-generation-v2", "generation":1,
            "rows":256,"dimensions":64,"page_rows":256,"unit_rows":32,
            "router_manifest_sha256":sha256(&router_manifest),
            "page_manifest_sha256":sha256(&page_manifest),
            "centroids_sha256":sha256(&centroids_blob),
            "graph_sha256":sha256(&graph_blob),
            "graph_resident_bytes":graph_resident,
            "sq8_object_sha256":sq8_hash,
            "sq8_object_key":format!("tenant/g1/objects/{sq8_hash}"), "sq8_etag":"etag-1",
        }))
        .unwrap();
        fs::write(root.join("manifest.json"), &manifest).unwrap();
        let limits = ObjectNativeLimits {
            max_memory_bytes: 2_000_000,
            max_active_queries: 2,
            max_query_bytes: 32_768,
            max_query_gets: 2,
            max_parallel_gets: 2,
            max_router_regions: 1,
            max_router_shortlist: 128,
            already_pinned_bytes: 0,
        };
        // The former response-only cap undercharges two planners plus ranking.
        assert!(matches!(
            ObjectNativeGeneration::open(
                &root,
                &sha256(&manifest),
                ObjectNativeLimits {
                    max_memory_bytes: 1_000_000,
                    ..limits
                }
            ),
            Err(ObjectNativeOpenError::Invalid("memory cap"))
        ));
        let opened = ObjectNativeGeneration::open(&root, &sha256(&manifest), limits).unwrap();
        assert_eq!(opened.router().router.rows(), 256);
        assert_eq!(opened.pages().object_sha256(), sq8_hash);
        assert_eq!(opened.graph().node_count(), 8);
        let store = InMemory::new();
        let prefix = ObjectPath::from("tenant/g1/metadata");
        for name in METADATA_FILES {
            store
                .put(
                    &metadata_location(&prefix, name),
                    PutPayload::from(fs::read(root.join(name)).unwrap()),
                )
                .await
                .unwrap();
        }
        let remote =
            ObjectNativeGeneration::open_remote(&store, &prefix, &sha256(&manifest), limits, &root)
                .await
                .unwrap();
        assert_eq!(remote.pages().object_sha256(), sq8_hash);
        assert!(matches!(
            ObjectNativeGeneration::open_remote(&store, &prefix, &"f".repeat(64), limits, &root)
                .await,
            Err(ObjectNativeOpenError::HashMismatch("generation manifest"))
        ));
        store
            .put(
                &metadata_location(&prefix, "router/books.bin"),
                PutPayload::from(vec![0xff; 64 * 256 * 4]),
            )
            .await
            .unwrap();
        assert!(
            ObjectNativeGeneration::open_remote(&store, &prefix, &sha256(&manifest), limits, &root)
                .await
                .is_err()
        );
        let plan = opened.plan_pages(&[0.0; 64], 1, 128, 100, 4).unwrap();
        assert_eq!(plan.ranges, vec![0..sq8.len()]);
        assert_eq!(
            opened
                .plan_pages_flat_control(&[0.0; 64], 1, 128, 100, 4)
                .unwrap()
                .ranges,
            plan.ranges,
        );
        assert_eq!(
            opened.object_key().as_ref(),
            format!("tenant/g1/objects/{sq8_hash}")
        );
        assert!(matches!(
            ObjectNativeGeneration::open(&root, &"f".repeat(64), limits),
            Err(ObjectNativeOpenError::HashMismatch("generation manifest"))
        ));
        assert!(matches!(
            ObjectNativeGeneration::open(
                &root,
                &sha256(&manifest),
                ObjectNativeLimits {
                    max_memory_bytes: 1,
                    ..limits
                }
            ),
            Err(ObjectNativeOpenError::Invalid("memory cap"))
        ));
        let mut unsafe_root: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        unsafe_root["sq8_object_key"] = serde_json::json!(format!("tenant/../objects/{sq8_hash}"));
        let unsafe_bytes = serde_json::to_vec(&unsafe_root).unwrap();
        fs::write(root.join("manifest.json"), &unsafe_bytes).unwrap();
        assert!(matches!(
            ObjectNativeGeneration::open(&root, &sha256(&unsafe_bytes), limits),
            Err(ObjectNativeOpenError::Invalid("generation identity"))
        ));
        fs::write(root.join("manifest.json"), &manifest).unwrap();
        fs::write(root.join("page_digests.bin"), vec![0; 32]).unwrap();
        assert!(matches!(
            ObjectNativeGeneration::open(&root, &sha256(&manifest), limits),
            Err(ObjectNativeOpenError::Page(PageError::HashMismatch))
                | Err(ObjectNativeOpenError::Page(PageError::InvalidSidecar))
        ));
        fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn metadata_staging_overlaps_bounded_object_waves_in_roster_order() {
        let names = [
            "manifest.json",
            "one.bin",
            "two.bin",
            "three.bin",
            "four.bin",
            "five.bin",
            "six.bin",
            "seven.bin",
            "eight.bin",
        ];
        let size = (METADATA_RANGE_BYTES * 2) as usize + 17;
        for children in [2, 3, 4, 7, 8] {
            let (store, prefix, payload) = staging_fixture(RangeFault::None, size).await;
            for name in &names[1..=children] {
                store
                    .inner
                    .put(&metadata_location(&prefix, name), payload.clone().into())
                    .await
                    .unwrap();
            }
            let parent = tempfile::tempdir().unwrap();
            let started = std::time::Instant::now();
            let (scratch, stats) = stage_generation_metadata(
                &store,
                &prefix,
                &sha256(b"{}"),
                u64::MAX,
                &names[..=children],
                parent.path(),
            )
            .await
            .unwrap();
            assert_eq!(
                stats.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
                names[..=children]
            );
            assert_eq!(stats[0].metadata_wave, 0);
            let mut wall = stats[0].metadata_wave_wall_ns;
            for (index, batch) in stats[1..].chunks(8).enumerate() {
                wall += batch[0].metadata_wave_wall_ns;
                assert!(batch.iter().all(|r| r.metadata_wave == index as u64 + 1
                    && r.metadata_wave_wall_ns == batch[0].metadata_wave_wall_ns));
                let ranges = METADATA_PARALLEL_GETS / batch.len() as u64;
                assert!(batch.iter().all(|r| r.payload_buffer_bound_bytes
                    == (size as u64).min(METADATA_RANGE_BYTES * ranges)));
                assert!(
                    batch
                        .iter()
                        .map(|r| r.payload_buffer_bound_bytes)
                        .sum::<u64>()
                        <= METADATA_RANGE_BYTES * METADATA_PARALLEL_GETS
                );
            }
            assert!(wall <= started.elapsed().as_nanos());
            assert!(store.peak.load(Ordering::SeqCst) >= children);
            assert!(store.peak.load(Ordering::SeqCst) <= METADATA_PARALLEL_GETS as usize);
            assert!(
                store.peak_buffered.load(Ordering::SeqCst)
                    <= (METADATA_RANGE_BYTES * METADATA_PARALLEL_GETS) as usize
            );
            assert_eq!(store.active.load(Ordering::SeqCst), 0);
            assert_eq!(store.buffered.load(Ordering::SeqCst), 0);
            for name in &names[1..=children] {
                assert_eq!(fs::read(scratch.path().join(name)).unwrap(), payload);
            }
            let requests = store.requests.lock().unwrap();
            assert_eq!(
                requests[0].0,
                metadata_location(&prefix, names[0]).to_string()
            );
            assert_eq!(requests[1].0, requests[0].0);
            let mut cursor = 2;
            for batch in names[1..=children].chunks(8) {
                assert!(requests[cursor..cursor + batch.len()].iter().all(|r| r.1));
                cursor += batch.len()
                    + batch.len() * (size as u64).div_ceil(METADATA_RANGE_BYTES) as usize;
            }
            assert_eq!(cursor, requests.len());
            drop(scratch);
            assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
        }
    }

    #[tokio::test]
    async fn metadata_staging_small_gets_overlap_and_cancel_without_detached_work() {
        let names = [
            "manifest.json",
            "nested/payload.bin",
            "two.bin",
            "three.bin",
            "four.bin",
            "five.bin",
            "six.bin",
            "seven.bin",
            "eight.bin",
            "nine.bin",
        ];
        for fault in [RangeFault::None, RangeFault::Pending] {
            let (store, prefix, payload) = staging_fixture(fault, 1024).await;
            for name in &names[2..] {
                store
                    .inner
                    .put(&metadata_location(&prefix, name), payload.clone().into())
                    .await
                    .unwrap();
            }
            let parent = tempfile::tempdir().unwrap();
            let hash = sha256(b"{}");
            let mut staging = Box::pin(stage_generation_metadata(
                &store,
                &prefix,
                &hash,
                u64::MAX,
                &names,
                parent.path(),
            ));
            if matches!(fault, RangeFault::None) {
                let (scratch, stats) = staging.await.unwrap();
                assert_eq!(store.peak.load(Ordering::SeqCst), 8);
                assert!(stats[1..9].iter().all(|r| r.metadata_wave == 1));
                assert_eq!(stats[9].metadata_wave, 2);
                drop(scratch);
            } else {
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    loop {
                        tokio::select! {
                            result = &mut staging => panic!("pending wave completed: {result:?}"),
                            _ = tokio::task::yield_now() => {
                                if store.active.load(Ordering::SeqCst) == 8 { break; }
                            }
                        }
                    }
                })
                .await
                .unwrap();
                drop(staging);
                let requests = store.requests.lock().unwrap().len();
                tokio::task::yield_now().await;
                assert_eq!(store.requests.lock().unwrap().len(), requests);
                assert!(
                    !store
                        .requests
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|r| r.0.ends_with("/nine.bin"))
                );
            }
            assert_eq!(store.active.load(Ordering::SeqCst), 0);
            assert_eq!(store.buffered.load(Ordering::SeqCst), 0);
            assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
        }
    }

    #[tokio::test]
    async fn metadata_staging_admits_whole_wave_before_any_child_get() {
        let (store, prefix, _) = staging_fixture(RangeFault::None, 10).await;
        for name in ["other.bin", "oversized.json"] {
            store
                .inner
                .put(
                    &metadata_location(&prefix, name),
                    vec![0; MAX_MANIFEST + 1].into(),
                )
                .await
                .unwrap();
        }
        let parent = tempfile::tempdir().unwrap();
        for (names, cap) in [
            (
                vec!["manifest.json", "nested/payload.bin", "other.bin"],
                MAX_MANIFEST as u64 + 12,
            ),
            (
                vec!["manifest.json", "nested/payload.bin", "oversized.json"],
                u64::MAX,
            ),
        ] {
            store.requests.lock().unwrap().clear();
            assert!(matches!(
                stage_generation_metadata(
                    &store,
                    &prefix,
                    &sha256(b"{}"),
                    cap,
                    &names,
                    parent.path()
                )
                .await,
                Err(ObjectNativeOpenError::Invalid("remote metadata length"))
            ));
            let requests = store.requests.lock().unwrap();
            assert_eq!(requests.iter().filter(|r| !r.1).count(), 1);
            assert!(requests[2..].iter().all(|r| r.1));
            assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
        }
    }

    #[tokio::test]
    async fn metadata_staging_parses_authenticated_root_before_children() {
        let (store, prefix, _) = staging_fixture(RangeFault::None, 10).await;
        store
            .inner
            .put(
                &metadata_location(&prefix, "manifest.json"),
                b"{".to_vec().into(),
            )
            .await
            .unwrap();
        let parent = tempfile::tempdir().unwrap();
        assert!(matches!(
            stage_generation_metadata(
                &store,
                &prefix,
                &sha256(b"{"),
                u64::MAX,
                &["manifest.json", "nested/payload.bin"],
                parent.path()
            )
            .await,
            Err(ObjectNativeOpenError::Invalid("generation root schema"))
        ));
        assert_eq!(store.requests.lock().unwrap().len(), 2);
        assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn metadata_staging_reports_exact_bounded_transfer_geometry() {
        for size in [
            32768,
            METADATA_RANGE_BYTES as usize,
            METADATA_RANGE_BYTES as usize + 1,
            (METADATA_RANGE_BYTES * METADATA_PARALLEL_GETS) as usize + 17,
        ] {
            let (store, prefix, payload) = staging_fixture(RangeFault::None, size).await;
            let parent = tempfile::tempdir().unwrap();
            let started = std::time::Instant::now();
            let (scratch, stats) = stage_generation_metadata(
                &store,
                &prefix,
                &sha256(b"{}"),
                size as u64 + 2,
                &["manifest.json", "nested/payload.bin"],
                parent.path(),
            )
            .await
            .unwrap();
            let elapsed = started.elapsed().as_nanos();
            assert_eq!(stats.len(), 2);
            assert_eq!(stats[0].name, "manifest.json");
            assert_eq!(stats[0].bytes, 2);
            assert_eq!(stats[1].bytes, size as u64);
            let ranged = size as u64 > METADATA_RANGE_BYTES;
            let gets = if ranged {
                (size as u64).div_ceil(METADATA_RANGE_BYTES)
            } else {
                1
            };
            assert_eq!(stats[1].logical_get_requests, gets);
            assert_eq!(
                stats[1].payload_buffer_bound_bytes,
                (size as u64).min(METADATA_RANGE_BYTES * METADATA_PARALLEL_GETS)
            );
            if ranged {
                assert_eq!(stats[1].get_wall_ns, 0);
            }
            assert!(stats.iter().all(|entry| entry.chunks > 0
                && entry.logical_head_requests == 1
                && entry.write_wall_ns <= entry.stream_wall_ns));
            assert!(stats.iter().map(|e| e.metadata_wave_wall_ns).sum::<u128>() <= elapsed);
            assert_eq!(
                fs::read(scratch.path().join("nested/payload.bin")).unwrap(),
                payload
            );
            assert_eq!(store.active.load(Ordering::SeqCst), 0);
            assert_eq!(
                store.peak.load(Ordering::SeqCst),
                if ranged {
                    gets.min(METADATA_PARALLEL_GETS) as usize
                } else {
                    1
                }
            );
            let requests = store.requests.lock().unwrap();
            assert_eq!(requests.len(), 3 + gets as usize);
            assert_eq!(
                requests[0],
                (
                    metadata_location(&prefix, "manifest.json").to_string(),
                    true,
                    None
                )
            );
            assert_eq!(
                requests[1],
                (
                    metadata_location(&prefix, "manifest.json").to_string(),
                    false,
                    None
                )
            );
            assert_eq!(
                requests[2],
                (
                    metadata_location(&prefix, "nested/payload.bin").to_string(),
                    true,
                    None
                )
            );
            for (index, (_, head, range)) in requests[3..].iter().enumerate() {
                assert!(!head);
                assert_eq!(
                    *range,
                    if ranged {
                        let start = index as u64 * METADATA_RANGE_BYTES;
                        Some(start..(start + METADATA_RANGE_BYTES).min(size as u64))
                    } else {
                        None
                    }
                );
            }
            drop(scratch);
            assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
        }
    }

    #[tokio::test]
    async fn metadata_staging_rejects_bad_ranges_and_cleans_scratch() {
        for fault in [
            RangeFault::Short,
            RangeFault::Long,
            RangeFault::WrongRange,
            RangeFault::WrongSize,
            RangeFault::Error,
        ] {
            let (store, prefix, payload) =
                staging_fixture(fault, METADATA_RANGE_BYTES as usize + 1).await;
            let names = [
                "manifest.json",
                "nested/payload.bin",
                "other.bin",
                "third.bin",
                "fourth.bin",
                "fifth.bin",
                "sixth.bin",
                "seventh.bin",
                "eighth.bin",
            ];
            for name in &names[2..] {
                store
                    .inner
                    .put(&metadata_location(&prefix, name), payload.clone().into())
                    .await
                    .unwrap();
            }
            let parent = tempfile::tempdir().unwrap();
            let result = stage_generation_metadata(
                &store,
                &prefix,
                &sha256(b"{}"),
                u64::MAX,
                &names,
                parent.path(),
            )
            .await;
            if matches!(fault, RangeFault::Error) {
                assert!(matches!(result, Err(ObjectNativeOpenError::Store(_))));
            } else {
                assert!(matches!(
                    result,
                    Err(ObjectNativeOpenError::Invalid("remote metadata length"))
                ));
            }
            assert_eq!(store.active.load(Ordering::SeqCst), 0);
            assert_eq!(store.buffered.load(Ordering::SeqCst), 0);
            assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
            let requests = store.requests.lock().unwrap().len();
            tokio::task::yield_now().await;
            assert_eq!(store.requests.lock().unwrap().len(), requests);
            assert_eq!(store.active.load(Ordering::SeqCst), 0);
            assert_eq!(store.buffered.load(Ordering::SeqCst), 0);
            assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
        }
    }

    #[tokio::test]
    async fn metadata_staging_authenticates_root_and_admits_before_payload() {
        let (store, prefix, _) =
            staging_fixture(RangeFault::None, METADATA_RANGE_BYTES as usize + 1).await;
        let parent = tempfile::tempdir().unwrap();
        for (hash, cap, expected_requests) in [
            ("f".repeat(64), METADATA_RANGE_BYTES + 3, 2),
            (sha256(b"{}"), METADATA_RANGE_BYTES + 2, 3),
        ] {
            store.requests.lock().unwrap().clear();
            let result = stage_generation_metadata(
                &store,
                &prefix,
                &hash,
                cap,
                &["manifest.json", "nested/payload.bin"],
                parent.path(),
            )
            .await;
            if expected_requests == 2 {
                assert!(matches!(
                    result,
                    Err(ObjectNativeOpenError::HashMismatch("generation manifest"))
                ));
            } else {
                assert!(matches!(
                    result,
                    Err(ObjectNativeOpenError::Invalid("remote metadata length"))
                ));
            }
            assert_eq!(store.requests.lock().unwrap().len(), expected_requests);
            assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
        }
        store
            .inner
            .put(
                &metadata_location(&prefix, "manifest.json"),
                vec![0; MAX_MANIFEST + 1].into(),
            )
            .await
            .unwrap();
        store.requests.lock().unwrap().clear();
        assert!(matches!(
            stage_generation_metadata(
                &store,
                &prefix,
                &sha256(b"{}"),
                u64::MAX,
                &["manifest.json", "nested/payload.bin"],
                parent.path()
            )
            .await,
            Err(ObjectNativeOpenError::Invalid("remote metadata length"))
        ));
        assert_eq!(store.requests.lock().unwrap().len(), 1);
        assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
        assert!(matches!(
            stage_generation_metadata(
                &store,
                &prefix,
                &sha256(b"{}"),
                u64::MAX,
                &["nested/payload.bin", "manifest.json"],
                parent.path()
            )
            .await,
            Err(ObjectNativeOpenError::Invalid("metadata root order"))
        ));
    }

    #[tokio::test]
    async fn metadata_staging_cancellation_drops_inflight_ranges_and_scratch() {
        let (store, prefix, _) = staging_fixture(
            RangeFault::Pending,
            (METADATA_RANGE_BYTES * METADATA_PARALLEL_GETS) as usize + 17,
        )
        .await;
        let parent = tempfile::tempdir().unwrap();
        let hash = sha256(b"{}");
        let mut staging = Box::pin(stage_generation_metadata(
            &store,
            &prefix,
            &hash,
            u64::MAX,
            &["manifest.json", "nested/payload.bin"],
            parent.path(),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    result = &mut staging => panic!("pending range completed: {result:?}"),
                    _ = tokio::task::yield_now() => {
                        if store.peak.load(Ordering::SeqCst) == METADATA_PARALLEL_GETS as usize { break; }
                    }
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 1);
        // No new batch is admitted while the first batch is pending.
        assert_eq!(
            store.requests.lock().unwrap().len(),
            3 + METADATA_PARALLEL_GETS as usize
        );
        drop(staging);
        assert_eq!(store.active.load(Ordering::SeqCst), 0);
        assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
        tokio::task::yield_now().await;
        assert_eq!(
            store.requests.lock().unwrap().len(),
            3 + METADATA_PARALLEL_GETS as usize
        );
        assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
    }
}
