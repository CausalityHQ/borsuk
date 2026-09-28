//! Prepared two-bit generations: immutable metadata, conditional head last.
use crate::{
    object_native_generation::metadata_location,
    resident_graph_generation::{Artifact, valid_sha256},
    resident_graph_store::{ResidentGraphStoreError, upload_authenticated_file},
    two_bit_generation::{
        METADATA_FILES, Manifest, TwoBitGeneration, TwoBitGenerationError, TwoBitGenerationLimits,
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
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadBody {
    schema: String,
    generation: u64,
    root_sha256: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EmptyRoot {
    pub(crate) schema: String,
    pub(crate) generation: u64,
    pub(crate) dimensions: usize,
}
impl EmptyRoot {
    pub(crate) fn valid(&self) -> bool {
        self.schema == "borsuk-two-bit-empty-generation-v1"
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
    dimensions: usize,
    empty: bool,
    generation: u64,
    root_sha256: String,
    prefix: ObjectPath,
    version: UpdateVersion,
}
impl TwoBitHead {
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
    let (body, version) = match small_object(store, &prefix.clone().join("head.json"), 1024).await {
        Ok(v) => v,
        Err(TwoBitStoreError::Store(object_store::Error::NotFound { .. })) => return Ok(None),
        Err(e) => return Err(e),
    };
    let head: HeadBody =
        serde_json::from_slice(&body).map_err(|_| TwoBitStoreError::Invalid("head schema"))?;
    if head.schema != "borsuk-two-bit-head-v1"
        || head.generation == 0
        || !valid_sha256(&head.root_sha256)
        || (version.e_tag.is_none() && version.version.is_none())
    {
        return Err(TwoBitStoreError::Invalid("head authority"));
    }
    let root_path = prefix
        .clone()
        .join("generations")
        .join(head.root_sha256.as_str())
        .join("manifest.json");
    let (root, _) = small_object(store, &root_path, 65536).await?;
    use sha2::{Digest, Sha256};
    if format!("{:x}", Sha256::digest(&root)) != head.root_sha256 {
        return Err(TwoBitStoreError::Invalid("root identity"));
    }
    let root: Root =
        serde_json::from_slice(&root).map_err(|_| TwoBitStoreError::Invalid("root schema"))?;
    let (dimensions, empty) = match root {
        Root::Empty(root) if root.valid() && root.generation == head.generation => {
            (root.dimensions, true)
        }
        Root::Populated(manifest)
            if manifest.schema == "borsuk-two-bit-generation-v2"
                && manifest.generation == head.generation
                && !manifest.low.is_empty()
                && manifest.low.len() == manifest.step.len()
                && manifest.canonical.valid()
                && manifest.canonical.dimensions == manifest.low.len() =>
        {
            (manifest.low.len(), false)
        }
        _ => return Err(TwoBitStoreError::Invalid("head generation")),
    };
    Ok(Some(TwoBitHead {
        dimensions,
        empty,
        generation: head.generation,
        root_sha256: head.root_sha256,
        prefix: prefix.clone(),
        version,
    }))
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
    if let Some(previous) = expected {
        crate::two_bit_mutations::require_sealed_two_bit_mutations(store, previous).await?;
    }
    // Admit and authenticate using exactly the serving reader, then release it
    // before multipart buffers are allocated. No SQ8 payload is loaded.
    drop(TwoBitGeneration::open(local, trusted_root_sha256, limits)?);
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
    let digests = [
        trusted_root_sha256,
        &manifest.page_manifest_sha256,
        pages["page_digest_sha256"]
            .as_str()
            .ok_or(TwoBitStoreError::Invalid("page digest"))?,
        &manifest.centroids_sha256,
        &manifest.graph_sha256,
        &manifest.plane_manifest_sha256,
        &plane.mean_sha256,
        &plane.records_sha256,
    ];
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
    for (name, digest) in METADATA_FILES.iter().zip(digests) {
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
        manifest.low.len(),
        false,
        expected,
    )
    .await
}

async fn publish_head(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    generation: u64,
    root_sha256: &str,
    dimensions: usize,
    empty: bool,
    expected: Option<&TwoBitHead>,
) -> Result<TwoBitHead> {
    if expected.is_some_and(|h| {
        h.prefix != *prefix || h.generation >= generation || h.dimensions != dimensions
    }) {
        return Err(TwoBitStoreError::Invalid("head namespace/order/dimensions"));
    }
    let body = serde_json::to_vec(&HeadBody {
        schema: "borsuk-two-bit-head-v1".into(),
        generation,
        root_sha256: root_sha256.to_owned(),
    })
    .map_err(|_| TwoBitStoreError::Invalid("head serialization"))?;
    let mode = expected.map_or(PutMode::Create, |h| PutMode::Update(h.version.clone()));
    let result = store
        .put_opts(
            &prefix.clone().join("head.json"),
            PutPayload::from(body),
            PutOptions {
                mode,
                ..Default::default()
            },
        )
        .await;
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            // A committed CAS can lose its acknowledgement; authenticate readback.
            if let Ok(Some(head)) = read_two_bit_head(store, prefix).await {
                if head.generation == generation && head.root_sha256 == root_sha256 {
                    return Ok(head);
                }
            }
            return Err(error.into());
        }
    };
    let version = UpdateVersion::from(result);
    if version.e_tag.is_none() && version.version.is_none() {
        return Err(TwoBitStoreError::Invalid("head conditional token"));
    }
    Ok(TwoBitHead {
        dimensions,
        empty,
        generation,
        root_sha256: root_sha256.to_owned(),
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
    let bad = TwoBitStoreError::Invalid;
    let root = EmptyRoot {
        schema: "borsuk-two-bit-empty-generation-v1".into(),
        generation,
        dimensions,
    };
    if !root.valid()
        || expected.is_some_and(|h| {
            h.prefix != *prefix || h.generation >= generation || h.dimensions != dimensions
        })
    {
        return Err(bad("empty generation namespace/order/dimensions"));
    }
    if let Some(previous) = expected {
        crate::two_bit_mutations::require_sealed_two_bit_mutations(store, previous).await?;
    }
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
        store, prefix, generation, &digest, dimensions, true, expected,
    )
    .await
}
