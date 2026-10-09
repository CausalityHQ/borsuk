//! Prepared two-bit generations: immutable metadata, conditional head last.
use crate::{
    object_native_generation::{metadata_location, valid_object_key},
    resident_graph_generation::{Artifact, valid_sha256},
    resident_graph_store::{ResidentGraphStoreError, upload_authenticated_file},
    two_bit_generation::{
        Discovery, DiscoveryMode, Manifest, TwoBitGeneration, TwoBitGenerationError,
        TwoBitGenerationLimits,
    },
    two_bit_source::{SourcePlaneReceipt, read_authenticated},
};
use futures_util::StreamExt;
use object_store::{
    CopyMode, CopyOptions, GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    PutOptions, PutPayload, UpdateVersion, path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
use thiserror::Error;
/// Publication/read failures leave the previous head unchanged unless CAS committed.
#[derive(Debug, Error)]
pub enum TwoBitStoreError {
    /// Object-store transport/conditional operation.
    #[error("two-bit store: {0}")]
    Store(#[from] object_store::Error),
    /// Local preparation I/O.
    #[error("two-bit local: {0}")]
    Io(#[from] std::io::Error),
    /// Generation authentication/admission.
    #[error("two-bit generation: {0}")]
    Generation(#[from] TwoBitGenerationError),
    /// Streamed metadata upload.
    #[error("two-bit upload: {0}")]
    Upload(#[from] ResidentGraphStoreError),
    /// Invalid authority or generation order.
    #[error("two-bit publication: {0}")]
    Invalid(&'static str),
}
type Result<T> = std::result::Result<T, TwoBitStoreError>;
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct MutationState {
    pub(crate) revision: u64,
    pub(crate) sha256: String,
    pub(crate) sealed: bool,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct HeadBody {
    pub(crate) schema: String,
    pub(crate) epoch: u64,
    pub(crate) generation: u64,
    pub(crate) root_sha256: String,
    pub(crate) mutation: Option<MutationState>,
    pub(crate) fence: Option<String>,
}
impl HeadBody {
    pub(crate) fn advance(&mut self) -> Result<()> {
        self.epoch = self
            .epoch
            .checked_add(1)
            .ok_or(TwoBitStoreError::Invalid("control epoch overflow"))?;
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EmptyRoot {
    pub(crate) schema: String,
    pub(crate) generation: u64,
    pub(crate) dimensions: usize,
    pub(crate) base_epoch: u64,
    pub(crate) discovery: DiscoveryMode,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) profile: Option<crate::semantic_unit_router::SemanticProfile>,
}
impl EmptyRoot {
    pub(crate) fn valid(&self) -> bool {
        self.schema == "borsuk-two-bit-empty-generation-v4"
            && self.generation > 0
            && self.dimensions > 0
            && u32::try_from(self.dimensions).is_ok()
            && match (self.discovery, self.profile) {
                (DiscoveryMode::Graph, None) => true,
                (DiscoveryMode::Semantic, Some(profile)) => {
                    profile.valid_geometry(1, self.dimensions)
                        || profile.valid_geometry(1_000_000, self.dimensions)
                }
                _ => false,
            }
    }
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Root {
    Empty(EmptyRoot),
    Populated(Manifest),
}
/// Opaque conditional token bound to this index prefix and authenticated root.
#[derive(Debug)]
pub struct TwoBitHead {
    epoch: u64,
    dimensions: usize,
    empty: bool,
    generation: u64,
    root_sha256: String,
    root: Box<[u8]>,
    prefix: ObjectPath,
    pub(crate) version: UpdateVersion,
}
impl TwoBitHead {
    /// Control epoch observed with this head; prepare replacements after sealing.
    /// A reclamation fence invalidates preparations from this epoch.
    pub fn control_epoch(&self) -> u64 {
        self.epoch
    }
    pub(crate) fn index_prefix(&self) -> &ObjectPath {
        &self.prefix
    }
    /// Dimensions declared by the authenticated generation root.
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }
    /// True when the authenticated base contains no rows or vector objects.
    pub fn is_empty(&self) -> bool {
        self.empty
    }
    /// Monotonically published generation ID.
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// Authenticated immutable root digest.
    pub fn root_sha256(&self) -> &str {
        &self.root_sha256
    }
    /// Exact retained immutable manifest allocation, separate from transport buffers.
    pub fn retained_root_bytes(&self) -> u64 {
        self.root.len() as u64
    }
    pub(crate) fn authenticated_root(&self, prefix: &ObjectPath, digest: &str) -> Result<&[u8]> {
        if *prefix != self.metadata_prefix() || digest != self.root_sha256 {
            return Err(TwoBitStoreError::Invalid("root seed namespace/identity"));
        }
        let (dimensions, empty) = root_properties(&self.root, digest, self.generation)?;
        if dimensions != self.dimensions || empty != self.empty {
            return Err(TwoBitStoreError::Invalid("root seed binding"));
        }
        Ok(&self.root)
    }
    /// Prefix to pass to `TwoBitGeneration::open_remote`.
    pub fn metadata_prefix(&self) -> ObjectPath {
        self.prefix
            .clone()
            .join("generations")
            .join(self.root_sha256.as_str())
    }
}
pub(crate) async fn small_object(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    cap: u64,
) -> Result<(Vec<u8>, UpdateVersion)> {
    let result = store.get(path).await?;
    let size = result.meta.size;
    if size == 0 || size > cap {
        return Err(TwoBitStoreError::Invalid("object length"));
    }
    let version = UpdateVersion {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    let mut bytes = Vec::with_capacity(size as usize);
    let mut stream = result.into_stream();
    while let Some(next) = stream.next().await {
        let chunk = next?;
        if chunk.len() > size as usize - bytes.len() {
            return Err(TwoBitStoreError::Invalid("object length"));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != size as usize {
        return Err(TwoBitStoreError::Invalid("object length"));
    }
    Ok((bytes, version))
}
/// Read the application-authorized head and authenticate its immutable root.
/// Store ACLs authorize head writers; a digest does not authorize an arbitrary writer.
pub async fn read_two_bit_head(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<Option<TwoBitHead>> {
    let Some((head, version)) = read_control(store, prefix).await? else {
        return Ok(None);
    };
    if head.fence.is_some() {
        return Err(TwoBitStoreError::Invalid("writes fenced for reclamation"));
    }
    Ok(Some(
        head_from_control(store, prefix, &head, version).await?,
    ))
}

pub(crate) async fn read_control(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<Option<(HeadBody, UpdateVersion)>> {
    let (body, version) = match small_object(store, &prefix.clone().join("head.json"), 4096).await {
        Ok(v) => v,
        Err(TwoBitStoreError::Store(object_store::Error::NotFound { .. })) => return Ok(None),
        Err(e) => return Err(e),
    };
    let head: HeadBody =
        serde_json::from_slice(&body).map_err(|_| TwoBitStoreError::Invalid("head schema"))?;
    if head.schema != "borsuk-two-bit-head-v2"
        || head.epoch == 0
        || head.generation == 0
        || !valid_sha256(&head.root_sha256)
        || head
            .mutation
            .as_ref()
            .is_some_and(|m| m.revision == 0 || !valid_sha256(&m.sha256))
        || head
            .fence
            .as_ref()
            .is_some_and(|f| f.len() != 32 || !f.bytes().all(|c| c.is_ascii_hexdigit()))
        || (version.e_tag.is_none() && version.version.is_none())
    {
        return Err(TwoBitStoreError::Invalid("head authority"));
    }
    Ok(Some((head, version)))
}
pub(crate) async fn commit_control(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    head: &HeadBody,
    expected: Option<UpdateVersion>,
) -> Result<UpdateVersion> {
    let body =
        serde_json::to_vec(head).map_err(|_| TwoBitStoreError::Invalid("head serialization"))?;
    let result = store
        .put_opts(
            &prefix.clone().join("head.json"),
            PutPayload::from(body),
            PutOptions {
                mode: expected.map_or(PutMode::Create, PutMode::Update),
                ..Default::default()
            },
        )
        .await;
    let version = match result {
        Ok(r) => UpdateVersion::from(r),
        Err(error) => {
            if let Ok(Some((current, version))) = read_control(store, prefix).await {
                if current == *head {
                    return Ok(version);
                }
            }
            return Err(error.into());
        }
    };
    if version.e_tag.is_none() && version.version.is_none() {
        return Err(TwoBitStoreError::Invalid("head conditional token"));
    }
    Ok(version)
}
async fn head_from_control(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    head: &HeadBody,
    version: UpdateVersion,
) -> Result<TwoBitHead> {
    let root_path = prefix
        .clone()
        .join("generations")
        .join(head.root_sha256.as_str())
        .join("manifest.json");
    let (root, _) = small_object(store, &root_path, 65536).await?;
    let (dimensions, empty) = root_properties(&root, &head.root_sha256, head.generation)?;
    Ok(TwoBitHead {
        epoch: head.epoch,
        dimensions,
        empty,
        generation: head.generation,
        root_sha256: head.root_sha256.clone(),
        root: root.into_boxed_slice(),
        prefix: prefix.clone(),
        version,
    })
}
fn root_properties(bytes: &[u8], digest: &str, generation: u64) -> Result<(usize, bool)> {
    if bytes.is_empty() || bytes.len() > 65536 {
        return Err(TwoBitStoreError::Invalid("root length"));
    }
    use sha2::{Digest, Sha256};
    if !valid_sha256(digest) || format!("{:x}", Sha256::digest(bytes)) != digest {
        return Err(TwoBitStoreError::Invalid("root identity"));
    }
    let root: Root =
        serde_json::from_slice(bytes).map_err(|_| TwoBitStoreError::Invalid("root schema"))?;
    match root {
        Root::Empty(root) if root.valid() && root.generation == generation => {
            Ok((root.dimensions, true))
        }
        Root::Populated(manifest)
            if manifest.schema == crate::two_bit_generation::SCHEMA
                && manifest.generation == generation
                && !manifest.low.is_empty()
                && manifest.low.len() == manifest.step.len()
                && manifest.canonical.valid()
                && manifest.canonical.dimensions == manifest.low.len()
                && manifest
                    .discovery
                    .valid(manifest.canonical.rows, manifest.canonical.dimensions) =>
        {
            Ok((manifest.low.len(), false))
        }
        _ => Err(TwoBitStoreError::Invalid("head generation")),
    }
}
pub(crate) async fn discovery_profile(
    store: &dyn ObjectStore,
    head: &TwoBitHead,
) -> Result<(
    DiscoveryMode,
    Option<crate::semantic_unit_router::SemanticProfile>,
)> {
    let (body, _) =
        small_object(store, &head.metadata_prefix().join("manifest.json"), 65536).await?;
    use sha2::{Digest, Sha256};
    if format!("{:x}", Sha256::digest(&body)) != head.root_sha256 {
        return Err(TwoBitStoreError::Invalid("discovery root identity"));
    }
    let root: Root =
        serde_json::from_slice(&body).map_err(|_| TwoBitStoreError::Invalid("discovery schema"))?;
    match root {
        Root::Empty(root) if root.valid() && root.generation == head.generation => {
            Ok((root.discovery, root.profile))
        }
        Root::Populated(root)
            if root.schema == crate::two_bit_generation::SCHEMA
                && root.generation == head.generation
                && root
                    .discovery
                    .valid(root.canonical.rows, root.canonical.dimensions) =>
        {
            let profile = root.discovery.semantic_profile();
            Ok((root.discovery.mode(), profile))
        }
        _ => Err(TwoBitStoreError::Invalid("discovery authority")),
    }
}
pub(crate) async fn discovery_mode(
    store: &dyn ObjectStore,
    head: &TwoBitHead,
) -> Result<DiscoveryMode> {
    Ok(discovery_profile(store, head).await?.0)
}
// Maintenance keys include the owning epoch in their physical namespace. A claim
// cannot relabel an old key after GC: the key itself must match the captured epoch.
async fn validate_owned_object(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &str,
    epoch: u64,
) -> Result<()> {
    let namespace = format!("{}/maintenance/", prefix.as_ref());
    let Some(relative) = key.strip_prefix(&namespace) else {
        if key.contains("/maintenance/") {
            return Err(TwoBitStoreError::Invalid("foreign maintenance owner"));
        }
        // Other immutable application-owned objects are outside this GC's ownership.
        return Ok(());
    };
    let (owner, object) = relative
        .split_once('/')
        .ok_or(TwoBitStoreError::Invalid("maintenance owner key"))?;
    if owner.len() != 48
        || !owner.bytes().all(|c| c.is_ascii_hexdigit())
        || owner[..16] != format!("{epoch:016x}")
        || !object.strip_prefix("objects/").is_some_and(valid_sha256)
    {
        return Err(TwoBitStoreError::Invalid("maintenance owner epoch"));
    }
    let (bytes, _) = small_object(
        store,
        &ObjectPath::from(format!("{namespace}{owner}/claim.json")),
        4096,
    )
    .await?;
    let claim: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|_| TwoBitStoreError::Invalid("maintenance claim schema"))?;
    if claim["schema"] != "borsuk-two-bit-compaction-job-v4"
        || claim["index_prefix"].as_str() != Some(prefix.as_ref())
        || claim["base_epoch"].as_u64() != Some(epoch)
    {
        return Err(TwoBitStoreError::Invalid("maintenance claim epoch"));
    }
    Ok(())
}

// All three JSON schemas/lengths were authenticated by the shared reader.
// Eight encoded copies cover retained bodies, String payloads/copies and
// numeric Vec growth (<= twice length apart from fixed initial capacity;
// each f32 uses >=2 JSON bytes including array separators).
// Page Value has exactly seven scalar fields; plane has thirteen fixed fields.
// 128KiB covers fixed structs/map nodes/roster/local paths. Namespace copies and
// the actual sealed HeadBody/UpdateVersion allocations are charged separately.
// These allocations coexist with semantic validation or multipart buffers.
fn publication_phase_budget(
    max_memory_bytes: u64,
    pinned_bytes: u64,
    manifest_bytes: [usize; 3],
    head_bytes: usize,
    namespace_bytes: usize,
) -> Result<usize> {
    let retained = manifest_bytes
        .into_iter()
        .chain(std::iter::once(namespace_bytes))
        .try_fold(131072_u64, |total, bytes| {
            u64::try_from(bytes)
                .ok()?
                .checked_mul(8)?
                .checked_add(total)
        })
        .and_then(|n| n.checked_add(u64::try_from(head_bytes).ok()?))
        .ok_or(TwoBitStoreError::Invalid("publication memory"))?;
    let remaining = max_memory_bytes
        .checked_sub(pinned_bytes)
        .and_then(|n| n.checked_sub(retained))
        .ok_or(TwoBitStoreError::Invalid("publication memory"))?;
    usize::try_from(remaining).map_err(|_| TwoBitStoreError::Invalid("publication memory"))
}

/// Independent construction approval and the current, explicitly approved SQ8
/// transport identity. A copied head alone does not supply this authority.
#[derive(Clone, Copy)]
pub struct RetainedTwoBitApproval<'a> {
    /// Independently approved original generation root, before transport copying.
    pub root_sha256: &'a str,
    /// Original generation ID; republishing preserves it.
    pub generation: u64,
    /// Approved control epoch of the retained source head.
    pub control_epoch: u64,
    /// Current immutable digest-suffixed SQ8 key, already present in this store.
    pub sq8_object_key: &'a str,
    /// Strong current SQ8 ETag, independently approved by the caller.
    pub sq8_etag: &'a str,
}

fn retained_etag(tag: &str) -> bool {
    !tag.is_empty()
        && tag.len() <= 4096
        && tag != "*"
        && !tag.contains(',')
        && !tag.starts_with("W/")
        && !tag.chars().any(char::is_control)
}

async fn retained_meta(
    store: &dyn ObjectStore,
    location: &ObjectPath,
    bytes: Option<u64>,
) -> Result<ObjectMeta> {
    let meta = store.head(location).await?;
    if meta.location != *location
        || meta.size == 0
        || bytes.is_some_and(|size| size != meta.size)
        || !meta
            .e_tag
            .as_ref()
            .is_some_and(|v| retained_etag(v) && v.capacity() <= 4096)
        || meta.version.as_ref().is_some_and(|v| v.capacity() > 4096)
    {
        return Err(TwoBitStoreError::Invalid("retained object identity"));
    }
    Ok(meta)
}

// One conditional full-body stream, drained through EOF even at the declared
// length. No whole payload allocation; only the three capped JSON bodies collect.
async fn authenticate_retained_body(
    store: &dyn ObjectStore,
    meta: &ObjectMeta,
    digest: &str,
    buffer_bytes: usize,
    collect: bool,
) -> Result<Vec<u8>> {
    use sha2::{Digest, Sha256};
    if !valid_sha256(digest) || buffer_bytes == 0 || (collect && meta.size > 65536) {
        return Err(TwoBitStoreError::Invalid("retained body admission"));
    }
    let result = store
        .get_opts(
            &meta.location,
            GetOptions {
                if_match: meta.e_tag.clone(),
                version: meta.version.clone(),
                ..Default::default()
            },
        )
        .await?;
    if result.meta != *meta || result.range != (0..meta.size) {
        return Err(TwoBitStoreError::Invalid("retained response identity"));
    }
    let mut body = if collect {
        Vec::with_capacity(meta.size as usize)
    } else {
        Vec::new()
    };
    let mut count = 0_u64;
    let mut sha = Sha256::new();
    let mut stream = result.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        count = count
            .checked_add(chunk.len() as u64)
            .filter(|&n| n <= meta.size)
            .ok_or(TwoBitStoreError::Invalid("retained body length"))?;
        if chunk.len() > buffer_bytes {
            return Err(TwoBitStoreError::Invalid("retained transport buffer"));
        }
        sha.update(&chunk);
        if collect {
            body.extend_from_slice(&chunk);
        }
    }
    if count != meta.size || format!("{:x}", sha.finalize()) != digest {
        return Err(TwoBitStoreError::Invalid("retained body SHA/length/EOF"));
    }
    Ok(body)
}

async fn recheck_retained_control(store: &dyn ObjectStore, retained: &TwoBitHead) -> Result<()> {
    let bad = TwoBitStoreError::Invalid;
    let location = retained.prefix.clone().join("head.json");
    let meta = retained_meta(store, &location, None).await?;
    if meta.size > 4096
        || meta.e_tag != retained.version.e_tag
        || meta.version != retained.version.version
    {
        return Err(bad("retained control changed"));
    }
    // Check bounded header capacities BEFORE copying conditional tokens. Unlike
    // small_object/read_control this cannot clone an arbitrarily long new tag.
    let response = store
        .get_opts(
            &location,
            GetOptions {
                if_match: meta.e_tag.clone(),
                version: meta.version.clone(),
                ..Default::default()
            },
        )
        .await?;
    if response.meta != meta || response.range != (0..meta.size) {
        return Err(bad("retained control response"));
    }
    let mut bytes = Vec::with_capacity(meta.size as usize);
    let mut stream = response.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if chunk.len() > meta.size as usize - bytes.len() {
            return Err(bad("retained control length"));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != meta.size as usize {
        return Err(bad("retained control length"));
    }
    let control: HeadBody =
        serde_json::from_slice(&bytes).map_err(|_| bad("retained control schema"))?;
    if control.schema != "borsuk-two-bit-head-v2"
        || control.epoch != retained.epoch
        || control.generation != retained.generation
        || control.root_sha256 != retained.root_sha256
        || control.mutation.is_some()
        || control.fence.is_some()
    {
        return Err(TwoBitStoreError::Invalid("retained control changed"));
    }
    Ok(())
}

/// Republish an independently approved initial semantic generation after copying.
/// Authenticates every retained body, including canonical/source/leaves/SQ8, then
/// changes only the SQ8 key/ETag. No centroids, rebuild, fitting or query changes.
/// The destination must be fresh and disjoint; every object and the last head
/// use create-only writes. Source control/objects remain immutable throughout.
/// Native copies do not condition on a source version: the modeled scratch
/// admission requires immutable source assets and cannot bound concurrent raw
/// file replacement. Shared payloads must remain live while the destination does.
/// LocalFileSystem Create copies hard-link files on the same writable filesystem;
/// staged assets prohibit in-place writes. EXDEV/EROFS are environment INVALID.
///
/// The provider must support `copy_opts(CopyMode::Create)` and create-only puts.
/// In object_store 0.14.1, AmazonS3 requires `copy_if_not_exists` configuration
/// (for example `S3CopyIfNotExists::Multipart`); without it create-only copy returns
/// `NotSupported`. Such a propagated store error is a provider/configuration
/// failure, not a scientific rejection. There is no overwrite-copy fallback.
///
/// Memory includes caller pins, the retained head, conservative JSON/control/
/// roster copies, bounded transport chunks and the existing serving validator.
/// `max_scratch_bytes` cumulatively admits serving scratch and all new metadata
/// (including unreachable objects) before staging. Serving scratch is RAII owned;
/// failed copies are removed best-effort only before attempting the head create.
/// Once the head create is attempted, preserve all admitted metadata even on Err:
/// a lost response may hide a committed head. There is no implicit recovery.
/// Cancellation/cleanup failures can leave admitted objects for reclamation.
/// Transport implementations must supply bounded chunks (at most 1MiB); runtime,
/// allocator and backend-internal overhead require separate resource measurement.
pub async fn republish_retained_two_bit_generation(
    store: &dyn ObjectStore,
    retained: &TwoBitHead,
    approved: RetainedTwoBitApproval<'_>,
    destination: &ObjectPath,
    limits: TwoBitGenerationLimits,
    scratch_parent: &Path,
    max_scratch_bytes: u64,
) -> Result<TwoBitHead> {
    let bad = TwoBitStoreError::Invalid;
    [
        retained.prefix.as_ref().len(),
        destination.as_ref().len(),
        approved.sq8_object_key.len(),
        approved.sq8_etag.len(),
        scratch_parent.as_os_str().len(),
    ]
    .into_iter()
    .try_fold(0_usize, usize::checked_add)
    .filter(|&n| n <= 65536)
    .ok_or(bad("retained namespace admission"))?;
    let pinned = limits
        .already_pinned_bytes
        .checked_add(retained.retained_root_bytes())
        .ok_or(bad("retained pin memory"))?;
    let root_bytes = retained.root.len();
    if root_bytes == 0 || root_bytes > 65536 {
        return Err(bad("retained root memory admission"));
    }
    // Every metadata path adds <128 bytes to its namespace; object keys decoded
    // from the approved root have at most root_bytes bytes. String/PathBuf growth
    // is charged at twice the longest final path, not eight namespace copies.
    let path_bytes = [
        retained.prefix.as_ref().len(),
        destination.as_ref().len(),
        approved.sq8_object_key.len(),
        scratch_parent.as_os_str().len(),
        root_bytes,
    ]
    .into_iter()
    .max()
    .unwrap()
    .checked_add(128)
    .ok_or(bad("retained path memory"))?;
    // Ten source ObjectMeta paths + ten destination pins + ten created clones;
    // three payload pins, four prefixes/returned-head paths, eight temporaries;
    // eight serving-wave objects each reserve eight path/parent/response copies.
    // Staged destination rosters coexist with the serving validation wave.
    let path_slots = 10_usize + 10 + 10 + 3 + 4 + 8 + 8 * 8;
    let paths = path_bytes
        .checked_mul(2)
        .and_then(|n| n.checked_mul(path_slots))
        .ok_or(bad("retained path memory"))?;
    // Source/destination/payload tokens, two control views, a conditional response
    // and its GetOptions clones, and eight serving-wave responses. Each strong
    // ETag/version String capacity is capped at 4096 before any clone we own.
    let token_slots = 10_usize + 10 + 3 + 2 + 1 + 1 + 8;
    let tokens = token_slots
        .checked_mul(2 * 4096)
        .ok_or(bad("retained token memory"))?;
    // Each coefficient serializes in <=32 bytes, including sign/exponent. Root
    // strings already occupy <=root_bytes encoded bytes; new key/tag escaping is
    // charged separately (six bytes per tag byte). Bound to_vec growth BEFORE it
    // allocates, plus the capped payload clone/boxed-root copy and head buffers.
    let serialized = retained
        .dimensions
        .checked_mul(2 * 32)
        .and_then(|n| n.checked_add(root_bytes))
        .and_then(|n| n.checked_add(approved.sq8_object_key.len()))
        .and_then(|n| n.checked_add(approved.sq8_etag.len().checked_mul(6)?))
        .and_then(|n| n.checked_add(256))
        .and_then(|n| n.checked_mul(2))
        .and_then(|n| n.checked_add(2 * 65536 + 2 * 4096))
        .ok_or(bad("retained serialization memory"))?;
    let bookkeeping = paths
        .checked_add(tokens)
        .and_then(|n| n.checked_add(serialized))
        .and_then(|n| n.checked_add(8 * 4096))
        .ok_or(bad("retained cumulative memory"))?;
    // Scalar-only schemas, control/roster structs and allocator growth are in
    // addition to these explicit paths/tokens/serializers and all caller pins.
    let available = publication_phase_budget(
        limits.max_memory_bytes,
        pinned,
        [root_bytes, 65536, 65536],
        bookkeeping,
        0,
    )?;
    let buffer_bytes = available.min(1024 * 1024);
    if buffer_bytes == 0
        || max_scratch_bytes == 0
        || retained.empty
        || retained.root_sha256 != approved.root_sha256
        || retained.generation != approved.generation
        || retained.epoch != approved.control_epoch
        || retained
            .version
            .e_tag
            .as_ref()
            .is_some_and(|v| v.capacity() > 4096)
        || retained
            .version
            .version
            .as_ref()
            .is_some_and(|v| v.capacity() > 4096)
        || !retained_etag(approved.sq8_etag)
        || destination.as_ref().is_empty()
        || retained.prefix.as_ref().is_empty()
        || destination == &retained.prefix
        || destination
            .as_ref()
            .starts_with(&format!("{}/", retained.prefix))
        || retained
            .prefix
            .as_ref()
            .starts_with(&format!("{destination}/"))
    {
        return Err(bad("retained approval/destination"));
    }
    let source_prefix = retained.metadata_prefix();
    retained.authenticated_root(&source_prefix, approved.root_sha256)?;
    recheck_retained_control(store, retained).await?;
    if store
        .list(Some(destination))
        .next()
        .await
        .transpose()?
        .is_some()
    {
        return Err(bad("retained destination occupied"));
    }
    let root_meta = retained_meta(
        store,
        &metadata_location(&source_prefix, "manifest.json"),
        Some(retained.retained_root_bytes()),
    )
    .await?;
    let root =
        authenticate_retained_body(store, &root_meta, approved.root_sha256, buffer_bytes, true)
            .await?;
    let manifest: Manifest =
        serde_json::from_slice(&root).map_err(|_| bad("retained root schema"))?;
    if manifest.schema != crate::two_bit_generation::SCHEMA
        || manifest.base_epoch != 0
        || manifest.generation != approved.generation
        || manifest.discovery.mode() != DiscoveryMode::Semantic
        || !manifest.canonical.valid()
        || manifest.canonical.dimensions != retained.dimensions
        || !manifest
            .discovery
            .valid(manifest.canonical.rows, retained.dimensions)
        || !valid_object_key(&manifest.sq8_object_key, &manifest.sq8_object_sha256)
        || !valid_object_key(approved.sq8_object_key, &manifest.sq8_object_sha256)
        || (manifest.sq8_object_key == approved.sq8_object_key
            && manifest.sq8_etag == approved.sq8_etag)
        || [
            manifest.sq8_object_key.as_str(),
            approved.sq8_object_key,
            manifest.canonical.object_key.as_str(),
        ]
        .into_iter()
        .any(|key| key.starts_with(&format!("{destination}/")))
    {
        return Err(bad("retained initial semantic generation"));
    }
    validate_owned_object(store, destination, &manifest.sq8_object_key, 0).await?;
    validate_owned_object(store, destination, approved.sq8_object_key, 0).await?;
    validate_owned_object(store, destination, &manifest.canonical.object_key, 0).await?;
    let plane_meta = retained_meta(
        store,
        &metadata_location(&source_prefix, "plane/manifest.json"),
        None,
    )
    .await?;
    let plane_body = authenticate_retained_body(
        store,
        &plane_meta,
        &manifest.plane_manifest_sha256,
        buffer_bytes,
        true,
    )
    .await?;
    let plane: SourcePlaneReceipt =
        serde_json::from_slice(&plane_body).map_err(|_| bad("retained plane schema"))?;
    let page_meta = retained_meta(
        store,
        &metadata_location(&source_prefix, "page_manifest.json"),
        None,
    )
    .await?;
    let page_body = authenticate_retained_body(
        store,
        &page_meta,
        &manifest.page_manifest_sha256,
        buffer_bytes,
        true,
    )
    .await?;
    // Scalar-only parsing rejects unknown fields before their values and rejects
    // nested values for known fields. No unbounded JSON Value tree is allocated.
    // The shared serving PageAuthority validator below still validates sidecars.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RetainedPages {
        schema: String,
        generation: u64,
        rows: usize,
        dimensions: usize,
        page_rows: usize,
        object_sha256: String,
        page_digest_sha256: String,
    }
    let pages: RetainedPages =
        serde_json::from_slice(&page_body).map_err(|_| bad("retained page schema"))?;
    if plane.rows != manifest.canonical.rows
        || plane.dimensions != retained.dimensions
        || pages.schema != "borsuk-v115-sq8-page-authority-v2"
        || pages.generation != manifest.generation
        || pages.rows != plane.rows
        || pages.dimensions != plane.dimensions
        || pages.page_rows != 256
        || pages.object_sha256 != manifest.sq8_object_sha256
        || !valid_sha256(&pages.page_digest_sha256)
    {
        return Err(bad("retained source geometry"));
    }
    let padded = crate::rotated_two_bit::RotatedTwoBitCodec::padded_dimensions(plane.dimensions)
        .map_err(|_| bad("retained codec geometry"))?;
    let records_bytes = plane
        .rows
        .checked_mul(padded.div_ceil(4) + 8)
        .ok_or(bad("retained record geometry"))?;
    let sq8_bytes = plane
        .rows
        .checked_mul(
            plane
                .dimensions
                .checked_add(12)
                .ok_or(bad("retained SQ8 geometry"))?,
        )
        .ok_or(bad("retained SQ8 geometry"))?;
    let Discovery::Semantic {
        root_sha256,
        root_bytes,
        membership_sha256,
        membership_bytes,
        leaves_sha256,
        leaves_bytes,
        ..
    } = &manifest.discovery
    else {
        return Err(bad("retained semantic descriptor"));
    };
    let descriptors = [
        (
            "page_digests.bin",
            plane.rows.div_ceil(256).checked_mul(32),
            pages.page_digest_sha256.as_str(),
        ),
        (
            "plane/mean.bin",
            plane.dimensions.checked_mul(4),
            plane.mean_sha256.as_str(),
        ),
        (
            "plane/records.bin",
            Some(records_bytes),
            plane.records_sha256.as_str(),
        ),
        (
            "plane/page_digests.bin",
            plane.rows.div_ceil(32).checked_mul(32),
            plane.page_digest_sha256.as_str(),
        ),
        ("router/root.bin", Some(*root_bytes), root_sha256.as_str()),
        (
            "router/membership.bin",
            Some(*membership_bytes),
            membership_sha256.as_str(),
        ),
        (
            "router/leaves.bin",
            Some(*leaves_bytes),
            leaves_sha256.as_str(),
        ),
    ];
    let mut roster = Vec::with_capacity(10);
    roster.extend([
        ("manifest.json", approved.root_sha256, root_meta),
        (
            "plane/manifest.json",
            manifest.plane_manifest_sha256.as_str(),
            plane_meta,
        ),
        (
            "page_manifest.json",
            manifest.page_manifest_sha256.as_str(),
            page_meta,
        ),
    ]);
    for (name, size, digest) in descriptors {
        let size = size.ok_or(bad("retained artifact geometry"))? as u64;
        let meta =
            retained_meta(store, &metadata_location(&source_prefix, name), Some(size)).await?;
        roster.push((name, digest, meta));
    }
    let sq8_meta = retained_meta(
        store,
        &ObjectPath::from(manifest.sq8_object_key.as_str()),
        Some(sq8_bytes as u64),
    )
    .await?;
    let current_sq8_meta = retained_meta(
        store,
        &ObjectPath::from(approved.sq8_object_key),
        Some(sq8_bytes as u64),
    )
    .await?;
    if current_sq8_meta.e_tag.as_deref() != Some(approved.sq8_etag) {
        return Err(bad("retained approved SQ8 ETag"));
    }
    let canonical_meta = retained_meta(
        store,
        &ObjectPath::from(manifest.canonical.object_key.as_str()),
        Some(manifest.canonical.bytes),
    )
    .await?;
    // Startup stages no records/leaves; all publication metadata is charged,
    // including the maximum new root, before any scratch or destination write.
    roster
        .iter()
        .try_fold(2 * 65536_u64 + 4096, |total, (name, _, meta)| {
            // New root can grow: separately reserve both destination and serving
            // copies at the root cap, rather than charging the old root length.
            let copies = if *name == "manifest.json" {
                0
            } else {
                1 + u64::from(!["plane/records.bin", "router/leaves.bin"].contains(name))
            };
            total.checked_add(meta.size.checked_mul(copies)?)
        })
        .filter(|&n| n <= max_scratch_bytes)
        .ok_or(bad("retained cumulative scratch"))?;
    for (name, digest, meta) in &roster {
        if !["manifest.json", "plane/manifest.json", "page_manifest.json"].contains(name) {
            authenticate_retained_body(store, meta, digest, buffer_bytes, false).await?;
        }
    }
    for (meta, digest) in [
        (&sq8_meta, manifest.sq8_object_sha256.as_str()),
        (&current_sq8_meta, manifest.sq8_object_sha256.as_str()),
        (&canonical_meta, manifest.canonical.sha256.as_str()),
    ] {
        authenticate_retained_body(store, meta, digest, buffer_bytes, false).await?;
    }
    let mut created = Vec::with_capacity(10);
    let mut head_attempted = false;
    let result = async {
        // Native create-only copies retain every payload byte; copied identity is
        // independently authenticated, since transport copy changes local ETags.
        let mut new_manifest: Manifest =
            serde_json::from_slice(&root).map_err(|_| bad("retained root schema"))?;
        new_manifest.sq8_object_key = approved.sq8_object_key.to_owned();
        new_manifest.sq8_etag = approved.sq8_etag.to_owned();
        let new_root =
            serde_json::to_vec(&new_manifest).map_err(|_| bad("retained root serialization"))?;
        if new_root.is_empty() || new_root.len() > 65536 {
            return Err(bad("retained new root length"));
        }
        // Replace the already admitted second parsed root; do not retain a third.
        drop(new_manifest);
        let roundtrip: Manifest =
            serde_json::from_slice(&new_root).map_err(|_| bad("retained new root schema"))?;
        for (original, rewritten) in [
            (&manifest.low, &roundtrip.low),
            (&manifest.step, &roundtrip.step),
        ] {
            if !original
                .iter()
                .map(|v| v.to_bits())
                .eq(rewritten.iter().map(|v| v.to_bits()))
            {
                return Err(bad("retained coefficient bits"));
            }
        }
        drop(roundtrip);
        use sha2::{Digest, Sha256};
        let new_sha = format!("{:x}", Sha256::digest(&new_root));
        let new_prefix = destination
            .clone()
            .join("generations")
            .join(new_sha.as_str());
        let mut destination_pins = Vec::with_capacity(10);
        for (name, digest, meta) in &roster {
            if *name == "manifest.json" {
                continue;
            }
            let location = metadata_location(&new_prefix, name);
            store
                .copy_opts(
                    &meta.location,
                    &location,
                    CopyOptions::new().with_mode(CopyMode::Create),
                )
                .await?;
            created.push(location.clone());
            let copied = retained_meta(store, &location, Some(meta.size)).await?;
            authenticate_retained_body(store, &copied, digest, buffer_bytes, false).await?;
            destination_pins.push(copied);
        }
        let location = metadata_location(&new_prefix, "manifest.json");
        store
            .put_opts(
                &location,
                PutPayload::from(new_root.clone()),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;
        created.push(location.clone());
        let copied = retained_meta(store, &location, Some(new_root.len() as u64)).await?;
        authenticate_retained_body(store, &copied, &new_sha, buffer_bytes, false).await?;
        destination_pins.push(copied);
        // Validate the exact staged root/metadata that the head will publish.
        // One serving pass, no centroids; caller bodies/rosters/pins are already
        // subtracted from this budget. Its temporary scratch is RAII owned.
        drop(
            TwoBitGeneration::open_remote(
                store,
                &new_prefix,
                &new_sha,
                TwoBitGenerationLimits {
                    max_memory_bytes: available as u64,
                    already_pinned_bytes: 0,
                    ..limits
                },
                scratch_parent,
            )
            .await?,
        );
        for meta in roster
            .iter()
            .map(|(_, _, meta)| meta)
            .chain([&sq8_meta, &current_sq8_meta, &canonical_meta])
            .chain(destination_pins.iter())
        {
            if store.head(&meta.location).await? != *meta {
                return Err(bad("retained object changed"));
            }
        }
        recheck_retained_control(store, retained).await?;
        let control = HeadBody {
            schema: "borsuk-two-bit-head-v2".into(),
            epoch: 1,
            generation: approved.generation,
            root_sha256: new_sha.clone(),
            mutation: None,
            fence: None,
        };
        let body = serde_json::to_vec(&control).map_err(|_| bad("retained head serialization"))?;
        // Mark BEFORE polling the create: its response may be lost after commit.
        // Propagate its original failure; keep metadata for independent recovery.
        head_attempted = true;
        let version = UpdateVersion::from(
            store
                .put_opts(
                    &destination.clone().join("head.json"),
                    PutPayload::from(body),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await?,
        );
        if (version.e_tag.is_none() && version.version.is_none())
            || version.e_tag.as_ref().is_some_and(|v| v.capacity() > 4096)
            || version
                .version
                .as_ref()
                .is_some_and(|v| v.capacity() > 4096)
        {
            return Err(bad("retained head token admission"));
        }
        Ok(TwoBitHead {
            epoch: 1,
            dimensions: retained.dimensions,
            empty: false,
            generation: approved.generation,
            root_sha256: new_sha,
            root: new_root.into_boxed_slice(),
            prefix: destination.clone(),
            version,
        })
    }
    .await;
    if result.is_err() && !head_attempted {
        for location in created.into_iter().rev() {
            let _ = store.delete(&location).await;
        }
    }
    result
}

/// Validate prepared local metadata, stream/hash it to an immutable root prefix,
/// then CAS the head. SQ8 must already exist at its immutable approved key/ETag.
/// Replacement requires the previous mutation head to be sealed first. The caller
/// must incorporate those sealed states in the prepared replacement; this low-level
/// publisher does not prove corpus equivalence. Initial publication needs no seal.
/// Inputs must remain immutable. This checks SQ8 HEAD, not a full SQ8 re-download.
/// Failed staging can leave unreachable metadata for in-process GC to reclaim.
pub async fn publish_two_bit_generation(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    local: &Path,
    trusted_root_sha256: &str,
    limits: TwoBitGenerationLimits,
    expected: Option<&TwoBitHead>,
) -> Result<TwoBitHead> {
    if expected.is_some_and(|h| h.prefix != *prefix) {
        return Err(TwoBitStoreError::Invalid("head namespace"));
    }
    // The old authenticated head remains live through new validation and upload.
    let limits = TwoBitGenerationLimits {
        already_pinned_bytes: limits
            .already_pinned_bytes
            .checked_add(expected.map_or(0, TwoBitHead::retained_root_bytes))
            .ok_or(TwoBitStoreError::Invalid("publication coexistence memory"))?,
        ..limits
    };
    let authority = if let Some(previous) = expected {
        Some(crate::two_bit_mutations::require_sealed_two_bit_mutations(store, previous).await?)
    } else {
        None
    };
    let head_bytes = authority
        .as_ref()
        .map_or(Some(0), |(head, version)| {
            [
                std::mem::size_of_val(head),
                std::mem::size_of_val(version),
                head.schema.capacity(),
                head.root_sha256.capacity(),
                head.mutation.as_ref().map_or(0, |m| m.sha256.capacity()),
                head.fence.as_ref().map_or(0, String::capacity),
                version.e_tag.as_ref().map_or(0, String::capacity),
                version.version.as_ref().map_or(0, String::capacity),
            ]
            .into_iter()
            .try_fold(0_usize, usize::checked_add)
        })
        .ok_or(TwoBitStoreError::Invalid("publication memory"))?;
    // Share serving metadata admission/identity checks and stream every local
    // record page/full SHA, then release validation buffers before uploads.
    // Retained sealed control is charged without expanding caller pin authority.
    let validation_limits = TwoBitGenerationLimits {
        max_memory_bytes: limits
            .max_memory_bytes
            .checked_sub(
                u64::try_from(head_bytes)
                    .map_err(|_| TwoBitStoreError::Invalid("publication memory"))?,
            )
            .ok_or(TwoBitStoreError::Invalid("publication memory"))?,
        ..limits
    };
    TwoBitGeneration::validate_local_publication(local, trusted_root_sha256, validation_limits)?;
    let read = |name: &str, digest: &str| -> Result<Vec<u8>> {
        let size = fs::metadata(local.join(name))?.len();
        if size == 0 || size > 65536 {
            return Err(TwoBitStoreError::Invalid("manifest size"));
        }
        read_authenticated(&local.join(name), size as usize, digest)
            .map_err(|e| TwoBitGenerationError::Plane(e).into())
    };
    let root = read("manifest.json", trusted_root_sha256)?;
    let manifest: Manifest =
        serde_json::from_slice(&root).map_err(|_| TwoBitStoreError::Invalid("root schema"))?;
    if expected
        .is_some_and(|h| manifest.generation <= h.generation || manifest.low.len() != h.dimensions)
    {
        return Err(TwoBitStoreError::Invalid("generation order"));
    }
    let base_epoch = authority.as_ref().map_or(0, |(control, _)| control.epoch);
    if manifest.base_epoch != base_epoch {
        return Err(TwoBitStoreError::Invalid(
            "prepared generation epoch changed",
        ));
    }
    let plane_body = read("plane/manifest.json", &manifest.plane_manifest_sha256)?;
    let plane: SourcePlaneReceipt = serde_json::from_slice(&plane_body)
        .map_err(|_| TwoBitStoreError::Invalid("plane schema"))?;
    let page_body = read("page_manifest.json", &manifest.page_manifest_sha256)?;
    let pages: serde_json::Value =
        serde_json::from_slice(&page_body).map_err(|_| TwoBitStoreError::Invalid("page schema"))?;
    let budget = publication_phase_budget(
        limits.max_memory_bytes,
        limits.already_pinned_bytes,
        [root.capacity(), plane_body.capacity(), page_body.capacity()],
        head_bytes,
        prefix.as_ref().len(),
    )?;
    drop(plane_body);
    drop(page_body);
    validate_owned_object(store, prefix, &manifest.sq8_object_key, base_epoch).await?;
    if manifest
        .canonical
        .object_key
        .rsplit_once("/objects/")
        .map(|(owner, _)| owner)
        != manifest
            .sq8_object_key
            .rsplit_once("/objects/")
            .map(|(owner, _)| owner)
    {
        validate_owned_object(store, prefix, &manifest.canonical.object_key, base_epoch).await?;
    }
    let source = store
        .head(&ObjectPath::from(manifest.sq8_object_key.clone()))
        .await?;
    let source_size = plane
        .rows
        .checked_mul(
            plane
                .dimensions
                .checked_add(12)
                .ok_or(TwoBitStoreError::Invalid("SQ8 geometry"))?,
        )
        .ok_or(TwoBitStoreError::Invalid("SQ8 geometry"))?;
    if source.size != source_size as u64 || source.e_tag.as_deref() != Some(&manifest.sq8_etag) {
        return Err(TwoBitStoreError::Invalid("SQ8 HEAD identity"));
    }
    drop(source);
    let mut roster = vec![
        ("manifest.json", trusted_root_sha256),
        ("page_manifest.json", manifest.page_manifest_sha256.as_str()),
        (
            "page_digests.bin",
            pages["page_digest_sha256"]
                .as_str()
                .ok_or(TwoBitStoreError::Invalid("page digest"))?,
        ),
        (
            "plane/manifest.json",
            manifest.plane_manifest_sha256.as_str(),
        ),
        ("plane/mean.bin", plane.mean_sha256.as_str()),
        ("plane/records.bin", plane.records_sha256.as_str()),
        ("plane/page_digests.bin", plane.page_digest_sha256.as_str()),
    ];
    match &manifest.discovery {
        Discovery::Graph {
            centroids_sha256,
            graph_sha256,
            diverse_graph_sha256,
            ..
        } => {
            roster.extend([
                ("centroids.bin", centroids_sha256.as_str()),
                ("graph.bin", graph_sha256.as_str()),
                ("diverse_graph.bin", diverse_graph_sha256.as_str()),
            ]);
        }
        Discovery::Semantic {
            profile,
            root_sha256,
            root_bytes,
            membership_sha256,
            membership_bytes,
            leaves_sha256,
            leaves_bytes,
            centroids_sha256,
            ..
        } => {
            let geometry = crate::semantic_unit_router::Geometry {
                rows: plane.rows,
                dimensions: plane.dimensions,
                units: plane.rows.div_ceil(32),
                blob_bytes: 32 + plane.rows.div_ceil(32) * plane.dimensions * 2,
            };
            crate::semantic_unit_router::admit(geometry, budget, *profile)
                .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
            let read = |name: &str, size, sha: &str| {
                read_authenticated(&local.join(name), size, sha)
                    .map_err(TwoBitGenerationError::Plane)
            };
            let root = read("router/root.bin", *root_bytes, root_sha256)?;
            let membership = read(
                "router/membership.bin",
                *membership_bytes,
                membership_sha256,
            )?;
            let leaves = read("router/leaves.bin", *leaves_bytes, leaves_sha256)?;
            let centroids = read("centroids.bin", geometry.blob_bytes, centroids_sha256)?;
            crate::semantic_unit_router::validate_publication(
                &root,
                &membership,
                &leaves,
                &manifest.discovery.input(&plane)?,
                &centroids,
            )
            .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
            roster.extend([
                ("router/root.bin", root_sha256.as_str()),
                ("router/membership.bin", membership_sha256.as_str()),
                ("router/leaves.bin", leaves_sha256.as_str()),
            ]);
        }
    }
    let metadata_prefix = prefix.clone().join("generations").join(trusted_root_sha256);
    upload_authenticated_file(
        store,
        &ObjectPath::from(manifest.canonical.object_key.clone()),
        &local.join("canonical.bin"),
        &Artifact {
            bytes: manifest.canonical.bytes,
            sha256: manifest.canonical.sha256.clone(),
        },
        budget,
    )
    .await?;
    for (name, digest) in roster {
        let artifact = Artifact {
            bytes: fs::metadata(local.join(name))?.len(),
            sha256: digest.to_owned(),
        };
        upload_authenticated_file(
            store,
            &metadata_location(&metadata_prefix, name),
            &local.join(name),
            &artifact,
            budget,
        )
        .await?;
    }
    let generation = manifest.generation;
    drop(manifest);
    drop(plane);
    drop(pages);
    publish_head(
        store,
        prefix,
        generation,
        trusted_root_sha256,
        root.into_boxed_slice(),
        expected,
        authority,
    )
    .await
}

async fn publish_head(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    generation: u64,
    root_sha256: &str,
    root: Box<[u8]>,
    expected: Option<&TwoBitHead>,
    authority: Option<(HeadBody, UpdateVersion)>,
) -> Result<TwoBitHead> {
    let (dimensions, empty) = root_properties(&root, root_sha256, generation)?;
    if expected.is_some_and(|h| {
        h.prefix != *prefix || h.generation >= generation || h.dimensions != dimensions
    }) {
        return Err(TwoBitStoreError::Invalid("head namespace/order/dimensions"));
    }
    let (mut control, version) = match authority {
        Some((mut control, version)) => {
            control.advance()?;
            (control, Some(version))
        }
        None => (
            HeadBody {
                schema: "borsuk-two-bit-head-v2".into(),
                epoch: 1,
                generation,
                root_sha256: root_sha256.into(),
                mutation: None,
                fence: None,
            },
            None,
        ),
    };
    control.generation = generation;
    control.root_sha256 = root_sha256.into();
    control.mutation = None;
    let version = commit_control(store, prefix, &control, version).await?;
    Ok(TwoBitHead {
        epoch: control.epoch,
        dimensions,
        empty,
        generation,
        root_sha256: root_sha256.to_owned(),
        root,
        prefix: prefix.clone(),
        version,
    })
}

/// Publish a real empty base, with no SQ8/canonical/graph objects or sentinel rows.
/// Initial create or monotonic same-dimension replacement. Replacement requires
/// the previous mutation seal; caller must only use this for an all-deleted state.
/// This low-level publisher cannot prove corpus equivalence. ACLs authorize it.
pub async fn publish_empty_two_bit_generation(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    dimensions: usize,
    generation: u64,
    expected: Option<&TwoBitHead>,
) -> Result<TwoBitHead> {
    publish_empty_with_mode(store, prefix, dimensions, generation, expected, None).await
}
/// Create or replace an empty semantic base with an explicit profile.
/// The dimension ceiling is 1024; replacements require a sealed mutation head.
pub async fn publish_empty_two_bit_generation_with_semantic_profile(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    dimensions: usize,
    generation: u64,
    expected: Option<&TwoBitHead>,
    profile: crate::semantic_unit_router::SemanticProfile,
) -> Result<TwoBitHead> {
    publish_empty_with_profile(
        store,
        prefix,
        dimensions,
        generation,
        expected,
        Some(DiscoveryMode::Semantic),
        Some(profile),
    )
    .await
}
pub(crate) async fn publish_empty_with_mode(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    dimensions: usize,
    generation: u64,
    expected: Option<&TwoBitHead>,
    mode: Option<DiscoveryMode>,
) -> Result<TwoBitHead> {
    publish_empty_with_profile(store, prefix, dimensions, generation, expected, mode, None).await
}
pub(crate) async fn publish_empty_with_profile(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    dimensions: usize,
    generation: u64,
    expected: Option<&TwoBitHead>,
    mode: Option<DiscoveryMode>,
    profile: Option<crate::semantic_unit_router::SemanticProfile>,
) -> Result<TwoBitHead> {
    let bad = TwoBitStoreError::Invalid;
    if dimensions == 0
        || generation == 0
        || u32::try_from(dimensions).is_err()
        || expected.is_some_and(|h| {
            h.prefix != *prefix || h.generation >= generation || h.dimensions != dimensions
        })
    {
        return Err(bad("empty generation namespace/order/dimensions"));
    }
    let inherited = if let Some(previous) = expected {
        discovery_profile(store, previous).await?
    } else {
        (DiscoveryMode::Graph, None)
    };
    let discovery = mode.unwrap_or(inherited.0);
    let profile = if discovery == DiscoveryMode::Semantic {
        Some(
            profile
                .or(inherited.1)
                .unwrap_or(crate::semantic_unit_router::SemanticProfile::Native100k),
        )
    } else {
        None
    };
    if profile == Some(crate::semantic_unit_router::SemanticProfile::Fresh1m) {
        return Err(bad("Fresh1m maintenance is unsupported"));
    }
    let mut root = EmptyRoot {
        schema: "borsuk-two-bit-empty-generation-v4".into(),
        generation,
        dimensions,
        base_epoch: 0,
        discovery,
        profile,
    };
    if !root.valid() {
        return Err(bad("empty generation namespace/order/dimensions"));
    }
    let authority = if let Some(previous) = expected {
        Some(crate::two_bit_mutations::require_sealed_two_bit_mutations(store, previous).await?)
    } else {
        None
    };
    root.base_epoch = authority.as_ref().map_or(0, |(control, _)| control.epoch);
    let bytes = serde_json::to_vec(&root).map_err(|_| bad("empty root schema"))?;
    use sha2::{Digest, Sha256};
    let digest = format!("{:x}", Sha256::digest(&bytes));
    let location = prefix
        .clone()
        .join("generations")
        .join(digest.as_str())
        .join("manifest.json");
    match store
        .put_opts(
            &location,
            PutPayload::from(bytes.clone()),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => {}
        Err(object_store::Error::AlreadyExists { .. }) => {
            let (old, _) = small_object(store, &location, 1024).await?;
            if old != bytes {
                return Err(bad("empty root immutable collision"));
            }
        }
        Err(error) => return Err(error.into()),
    }
    publish_head(
        store,
        prefix,
        generation,
        &digest,
        bytes.into_boxed_slice(),
        expected,
        authority,
    )
    .await
}

/// Durable write fence. It preserves data references and does not pin readers.
/// Store ACLs authorize acquiring/resuming it; no objects are deleted by this API.
pub struct TwoBitWriteFence {
    control: HeadBody,
    head: TwoBitHead,
    version: UpdateVersion,
}
impl TwoBitWriteFence {
    pub(crate) async fn validate_mutations(
        &self,
        store: &dyn ObjectStore,
        limits: crate::two_bit_mutations::TwoBitMutationLimits,
    ) -> Result<()> {
        crate::two_bit_mutations::admit(limits, 0)?;
        if let Some(state) = &self.control.mutation {
            let (bytes, _) = small_object(
                store,
                &self
                    .head
                    .metadata_prefix()
                    .join("mutations")
                    .join(state.sha256.as_str()),
                limits.max_snapshot_bytes as u64,
            )
            .await?;
            drop(crate::two_bit_mutations::decode(
                &bytes,
                &state.sha256,
                &self.head,
                self.head.dimensions(),
                state.revision,
                self.version.clone(),
                limits,
            )?);
        }
        Ok(())
    }
    /// Stable identity for this interrupted/recovered fence.
    pub fn id(&self) -> &str {
        self.control.fence.as_deref().unwrap()
    }
    /// Authenticated base retained by the fence, including empty bases.
    pub fn head(&self) -> &TwoBitHead {
        &self.head
    }
    /// Latest mutation object that reclamation must retain, if present.
    pub fn mutation_sha256(&self) -> Option<&str> {
        self.control.mutation.as_ref().map(|m| m.sha256.as_str())
    }
}

/// CAS-freeze BOTH generation and mutation commits, or recover an existing fence.
/// New latest reads reject while fenced; already-pinned queries remain readable.
/// This does NOT establish reader quiescence or enable remote GC by itself.
pub async fn begin_two_bit_write_fence(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<TwoBitWriteFence> {
    let (mut control, version) = read_control(store, prefix)
        .await?
        .ok_or(TwoBitStoreError::Invalid("fence index absent"))?;
    let mut head = head_from_control(store, prefix, &control, version.clone()).await?;
    let version = if control.fence.is_some() {
        version
    } else {
        control.advance()?;
        control.fence = Some(uuid::Uuid::new_v4().simple().to_string());
        commit_control(store, prefix, &control, Some(version)).await?
    };
    head.version = version.clone();
    head.epoch = control.epoch;
    Ok(TwoBitWriteFence {
        control,
        head,
        version,
    })
}

/// Release precisely this fence, preserving data references and advancing epoch.
/// Conditional failure requires rereading/resuming; never clear another fence.
pub async fn end_two_bit_write_fence(
    store: &dyn ObjectStore,
    fence: &TwoBitWriteFence,
) -> Result<()> {
    let mut control = fence.control.clone();
    control.advance()?;
    control.fence = None;
    commit_control(
        store,
        fence.head.index_prefix(),
        &control,
        Some(fence.version.clone()),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod root_seed_tests {
    use super::*;

    #[tokio::test]
    async fn publication_reserves_metadata_before_upload_buffers() {
        use sha2::{Digest, Sha256};
        let remaining = 16 * 1024 * 1024;
        let pinned = 262144;
        let budget =
            publication_phase_budget(remaining + pinned, pinned, [4096, 1024, 512], 512, 128)
                .unwrap();
        assert!(budget < remaining as usize);
        let file = tempfile::NamedTempFile::new().unwrap();
        let bytes = 8 * 1024 * 1024 + 1;
        file.as_file().set_len(bytes).unwrap();
        let mut digest = Sha256::new();
        for _ in 0..128 {
            digest.update([0_u8; 65536]);
        }
        digest.update([0]);
        let artifact = Artifact {
            bytes,
            sha256: format!("{:x}", digest.finalize()),
        };
        let store = object_store::memory::InMemory::new();
        let key = ObjectPath::from("publication/canonical");
        let error = upload_authenticated_file(&store, &key, file.path(), &artifact, budget)
            .await
            .err()
            .unwrap();
        assert!(
            matches!(
                error,
                ResidentGraphStoreError::Invalid("upload memory budget")
            ),
            "{error:?}"
        );
        assert!(store.head(&key).await.is_err());
        let sufficient = publication_phase_budget(
            remaining + pinned + 2 * 1024 * 1024,
            pinned,
            [4096, 1024, 512],
            512,
            128,
        )
        .unwrap();
        upload_authenticated_file(&store, &key, file.path(), &artifact, sufficient)
            .await
            .unwrap();
        assert_eq!(store.head(&key).await.unwrap().size, bytes);
        assert!(
            publication_phase_budget(remaining, u64::MAX, [4096, 1024, 512], 512, 128).is_err()
        );
        assert!(
            publication_phase_budget(remaining, 0, [4096, 1024, 512], usize::MAX, 128).is_err()
        );
    }

    #[tokio::test]
    async fn root_seed_rechecks_body_namespace_digest_and_generation_before_staging() {
        let store = object_store::memory::InMemory::new();
        let prefix = ObjectPath::from("seed/index");
        let published = publish_empty_two_bit_generation(&store, &prefix, 2, 1, None)
            .await
            .unwrap();
        let metadata = published.metadata_prefix();
        let scratch = tempfile::tempdir().unwrap();
        assert_eq!(published.retained_root_bytes(), published.root.len() as u64);
        assert_eq!(
            published
                .authenticated_root(&metadata, published.root_sha256())
                .unwrap(),
            published.root.as_ref()
        );
        // No remote root exists during these attempts. The exact seed-admission
        // error proves rejection precedes any remote HEAD/GET or child future.
        store
            .delete(&metadata.clone().join("manifest.json"))
            .await
            .unwrap();
        for fault in [
            "corrupt",
            "empty",
            "oversized",
            "digest",
            "prefix",
            "generation",
            "dimensions",
            "kind",
        ] {
            let mut head = TwoBitHead {
                epoch: published.epoch,
                dimensions: published.dimensions,
                empty: true,
                generation: published.generation,
                root_sha256: published.root_sha256.clone(),
                root: published.root.clone(),
                prefix: prefix.clone(),
                version: published.version.clone(),
            };
            match fault {
                "corrupt" => head.root[0] ^= 1,
                "empty" => head.root = Box::new([]),
                "oversized" => head.root = vec![0; 65537].into_boxed_slice(),
                "digest" => head.root_sha256 = "0".repeat(64),
                "prefix" => head.prefix = ObjectPath::from("foreign/index"),
                "generation" => head.generation += 1,
                "dimensions" => head.dimensions += 1,
                _ => head.empty = false,
            }
            let error = crate::object_native_generation::stage_two_bit_metadata(
                &store,
                &metadata,
                published.root_sha256(),
                128_000_000,
                scratch.path(),
                Some(&head),
            )
            .await
            .err()
            .unwrap();
            assert!(
                matches!(
                    error,
                    crate::object_native_generation::ObjectNativeOpenError::Invalid(
                        "root seed admission"
                    )
                ),
                "{fault}: {error:?}"
            );
            assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
        }
    }
}
