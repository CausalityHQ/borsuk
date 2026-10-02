//! Prepared two-bit generations: immutable metadata, conditional head last.
use crate::{
    object_native_generation::metadata_location,
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
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
    path::Path as ObjectPath,
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
}
impl EmptyRoot {
    pub(crate) fn valid(&self) -> bool {
        self.schema == "borsuk-two-bit-empty-generation-v3"
            && self.generation > 0
            && self.dimensions > 0
            && u32::try_from(self.dimensions).is_ok()
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
            Ok((root.discovery, None))
        }
        Root::Populated(root)
            if root.schema == crate::two_bit_generation::SCHEMA
                && root.generation == head.generation
                && root
                    .discovery
                    .valid(root.canonical.rows, root.canonical.dimensions) =>
        {
            let profile = match &root.discovery {
                Discovery::Semantic { profile, .. } => Some(*profile),
                _ => None,
            };
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
    if claim["schema"] != "borsuk-two-bit-compaction-job-v3"
        || claim["index_prefix"].as_str() != Some(prefix.as_ref())
        || claim["base_epoch"].as_u64() != Some(epoch)
    {
        return Err(TwoBitStoreError::Invalid("maintenance claim epoch"));
    }
    Ok(())
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
    let authority = if let Some(previous) = expected {
        Some(crate::two_bit_mutations::require_sealed_two_bit_mutations(store, previous).await?)
    } else {
        None
    };
    // Share serving metadata admission/identity checks and stream every local
    // record page/full SHA, then release validation buffers before uploads.
    TwoBitGeneration::validate_local_publication(local, trusted_root_sha256, limits)?;
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
    let plane: SourcePlaneReceipt = serde_json::from_slice(&read(
        "plane/manifest.json",
        &manifest.plane_manifest_sha256,
    )?)
    .map_err(|_| TwoBitStoreError::Invalid("plane schema"))?;
    let pages: serde_json::Value =
        serde_json::from_slice(&read("page_manifest.json", &manifest.page_manifest_sha256)?)
            .map_err(|_| TwoBitStoreError::Invalid("page schema"))?;
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
            let cap = usize::try_from(
                limits
                    .max_memory_bytes
                    .saturating_sub(limits.already_pinned_bytes),
            )
            .map_err(|_| TwoBitStoreError::Invalid("publication memory"))?;
            crate::semantic_unit_router::admit(geometry, cap, *profile)
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
    let budget = usize::try_from(
        limits
            .max_memory_bytes
            .saturating_sub(limits.already_pinned_bytes),
    )
    .map_err(|_| TwoBitStoreError::Invalid("upload budget"))?;
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
    publish_head(
        store,
        prefix,
        manifest.generation,
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
pub(crate) async fn publish_empty_with_mode(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    dimensions: usize,
    generation: u64,
    expected: Option<&TwoBitHead>,
    mode: Option<DiscoveryMode>,
) -> Result<TwoBitHead> {
    let bad = TwoBitStoreError::Invalid;
    let mut root = EmptyRoot {
        schema: "borsuk-two-bit-empty-generation-v3".into(),
        generation,
        dimensions,
        base_epoch: 0,
        discovery: mode.unwrap_or(DiscoveryMode::Graph),
    };
    if !root.valid()
        || expected.is_some_and(|h| {
            h.prefix != *prefix || h.generation >= generation || h.dimensions != dimensions
        })
    {
        return Err(bad("empty generation namespace/order/dimensions"));
    }
    if let Some(previous) = expected {
        if discovery_profile(store, previous).await?.1
            == Some(crate::semantic_unit_router::SemanticProfile::Fresh1m)
        {
            return Err(bad("Fresh1m maintenance is unsupported"));
        }
    }
    if mode.is_none()
        && let Some(previous) = expected
    {
        root.discovery = discovery_mode(store, previous).await?;
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
