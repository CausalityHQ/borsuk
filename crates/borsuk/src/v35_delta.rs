use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result, V35ArtifactIdentity, V35GenerationManifest, V35SnapshotVisibility,
    V35StoredArtifactIdentity, project_v35_serving_memory, validate_v35_manifest,
};

const HEAD_FORMAT: &str = "borsuk-v35-generation-head-v2";
const DELTA_FORMAT: &str = "borsuk-v35-delta-manifest-v1";
const VERSION_ID_MAX_BYTES: usize = 1_024;
const URI_MAX_BYTES: usize = 4_096;
const HEAD_MAX_BYTES: usize = 16 * 1_024;
const DELTA_MANIFEST_MAX_BYTES: usize = 64 * 1_024;
const MAX_DELTA_RUNS: usize = 4;
const MAX_DELTA_ROWS: u64 = 1_000_000;
const MAX_DELTA_TREE_NODES: u64 = 699;
const DELTA_LEAF_BYTES: u64 = 6_890_624;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn validate_version_id(version_id: &str) -> Result<()> {
    if version_id.is_empty() || version_id.len() > VERSION_ID_MAX_BYTES {
        return Err(invalid("V35 object-store version differs"));
    }
    Ok(())
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_head_target_identity(identity: &V35ArtifactIdentity) -> Result<()> {
    if !matches!(
        identity.role.as_str(),
        "generation-manifest" | "delta-manifest"
    ) || identity.digest_algorithm != "sha256"
        || !is_digest(&identity.digest)
        || identity.length == 0
        || !identity.uri.starts_with("s3://")
        || identity.uri.contains("/corpus/")
        || identity.uri.len() > URI_MAX_BYTES
    {
        return Err(invalid("V35 generation-head target identity differs"));
    }
    Ok(())
}

fn validate_stored_role(stored: &V35StoredArtifactIdentity, role: &str) -> Result<()> {
    let identity = &stored.object;
    if identity.role != role
        || identity.digest_algorithm != "sha256"
        || !is_digest(&identity.digest)
        || identity.length == 0
        || !identity.uri.starts_with("s3://")
        || identity.uri.contains("/corpus/")
        || identity.uri.len() > URI_MAX_BYTES
    {
        return Err(invalid("V35 stored delta artifact identity differs"));
    }
    validate_version_id(&stored.version_id)
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        scalar => scalar,
    }
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)
        .map_err(|_| invalid("V35 generation head cannot be canonicalized"))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|_| invalid("V35 generation head cannot be serialized"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Terminal result of the single conditional generation-head write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum V35ConditionalHeadWrite {
    /// The head was committed at the returned opaque object-store version.
    Committed {
        /// Opaque version returned by the successful head write.
        version_id: String,
    },
    /// The registered predecessor no longer matches the visible head.
    Conflict,
    /// The store cannot prove whether the conditional write committed.
    Indeterminate,
}

/// Storage boundary for immutable-manifest-first, conditional-head-last publication.
pub trait V35GenerationPublicationSink {
    /// Store the authenticated immutable generation manifest and return its actual version.
    fn write_manifest(&mut self, identity: &V35ArtifactIdentity, bytes: &[u8]) -> Result<String>;

    /// Attempt the head write exactly once.
    ///
    /// `None` means create-if-absent; `Some` requires an exact match with the
    /// currently visible opaque version. Implementations must never translate
    /// either precondition into an unconditional write.
    ///
    /// A transport result that cannot prove whether the store committed must
    /// return [`V35ConditionalHeadWrite::Indeterminate`], never an error.
    fn compare_and_swap_head(
        &mut self,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> Result<V35ConditionalHeadWrite>;
}

/// Terminal outcome of publishing one immutable generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum V35PublicationOutcome {
    /// The manifest and generation head are both durably visible.
    Committed {
        /// Stored immutable manifest identity referenced by the head.
        manifest: V35StoredArtifactIdentity,
        /// Opaque version returned by the successful head write.
        head_version_id: String,
    },
    /// Another publisher won the conditional head update.
    Conflict,
    /// The conditional head update may or may not have committed.
    Indeterminate,
}

/// One immutable ordered delta run and its exact serving roots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V35DeltaRun {
    /// Base generation digest whose projection and source authority this run extends.
    pub base_generation_sha256: String,
    /// Authenticated code-directory root and store version.
    pub code_directory: V35StoredArtifactIdentity,
    /// Resident routing leaves represented by this run.
    pub leaf_count: u32,
    /// Dense run ordinal in oldest-to-newest order.
    pub ordinal: u8,
    /// Authenticated exact-page-directory root and store version.
    pub page_directory: V35StoredArtifactIdentity,
    /// Authenticated routing generation and store version.
    pub routing: V35StoredArtifactIdentity,
    /// Mutation rows represented by the run.
    pub rows: u32,
    /// Least global mutation sequence represented by this run.
    pub sequence_floor: u64,
    /// Greatest global mutation sequence incorporated by this run.
    pub sequence_horizon: u64,
    /// Resident routing tree nodes represented by this run.
    pub tree_node_count: u32,
}

/// Exact base-plus-ordered-delta snapshot admitted by serving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V35DeltaManifest {
    /// Immutable base generation manifest and store version.
    pub base_generation: V35StoredArtifactIdentity,
    /// Greatest mutation sequence already folded into the compacted base.
    pub base_sequence_horizon: u64,
    /// Breaking delta-manifest format marker.
    pub format: String,
    /// One or two projected patches per delta leaf, inherited from the base.
    pub patches_per_leaf: u8,
    /// Total rows across all admitted runs.
    pub rows: u64,
    /// Resident routing width inherited from the authenticated base.
    pub routing_dimension: u16,
    /// Oldest-to-newest immutable delta runs.
    pub runs: Vec<V35DeltaRun>,
    /// Complete latest-sequence visibility directory for this snapshot.
    pub visibility: V35StoredArtifactIdentity,
    /// Latest-sequence deleted IDs represented only by visibility state.
    pub tombstones: u64,
    /// IDs represented by the complete visibility directory.
    pub visibility_rows: u64,
    /// Least mutation sequence represented by the visibility directory.
    pub visibility_sequence_floor: u64,
    /// Greatest mutation sequence incorporated by the visibility directory.
    pub visibility_sequence_horizon: u64,
}

fn delta_leaf_bytes(manifest: &V35DeltaManifest, leaves: u64) -> Option<u64> {
    u64::from(manifest.routing_dimension)
        .checked_mul(8)?
        .checked_add(128)?
        .checked_mul(u64::from(manifest.patches_per_leaf))?
        .checked_mul(leaves)
}

fn validate_delta_manifest_fields(manifest: &V35DeltaManifest) -> Result<()> {
    if manifest.format != DELTA_FORMAT
        || manifest.base_sequence_horizon == 0
        || !matches!(manifest.routing_dimension, 64 | 128 | 192)
        || !matches!(manifest.patches_per_leaf, 1 | 2)
        || manifest.runs.len() > MAX_DELTA_RUNS
    {
        return Err(invalid("V35 delta manifest authority differs"));
    }
    validate_stored_role(&manifest.base_generation, "generation-manifest")?;
    validate_stored_role(&manifest.visibility, "snapshot-visibility-directory")?;

    let mut uris = std::collections::BTreeSet::from([
        manifest.base_generation.object.uri.as_str(),
        manifest.visibility.object.uri.as_str(),
    ]);
    if uris.len() != 2 {
        return Err(invalid("V35 delta artifact URIs must be unique"));
    }
    let mut rows = 0_u64;
    let mut leaves = 0_u64;
    let mut tree_nodes = 0_u64;
    let mut previous_horizon = manifest.base_sequence_horizon;
    for (position, run) in manifest.runs.iter().enumerate() {
        if usize::from(run.ordinal) != position
            || run.rows == 0
            || run.leaf_count == 0
            || run.tree_node_count == 0
            || u64::from(run.leaf_count) > u64::from(run.rows)
            || run.sequence_floor <= previous_horizon
            || run.sequence_floor > run.sequence_horizon
            || run.sequence_horizon <= previous_horizon
            || run.base_generation_sha256 != manifest.base_generation.object.digest
        {
            return Err(invalid("V35 delta run order differs"));
        }
        validate_stored_role(&run.routing, "active-routing")?;
        validate_stored_role(&run.code_directory, "code-directory")?;
        validate_stored_role(&run.page_directory, "page-directory")?;
        for stored in [&run.routing, &run.code_directory, &run.page_directory] {
            if !uris.insert(stored.object.uri.as_str()) {
                return Err(invalid("V35 delta artifact URIs must be unique"));
            }
        }
        rows = rows
            .checked_add(u64::from(run.rows))
            .ok_or_else(|| invalid("V35 delta row count overflows"))?;
        leaves = leaves
            .checked_add(u64::from(run.leaf_count))
            .ok_or_else(|| invalid("V35 delta leaf count overflows"))?;
        tree_nodes = tree_nodes
            .checked_add(u64::from(run.tree_node_count))
            .ok_or_else(|| invalid("V35 delta tree-node count overflows"))?;
        previous_horizon = run.sequence_horizon;
    }
    let live_visibility_rows = manifest
        .visibility_rows
        .checked_sub(manifest.tombstones)
        .ok_or_else(|| invalid("V35 delta tombstone count differs"))?;
    let visibility_covers_runs =
        manifest.runs.is_empty() || manifest.visibility_sequence_horizon >= previous_horizon;
    if rows != manifest.rows
        || rows > MAX_DELTA_ROWS
        || manifest.visibility_rows > MAX_DELTA_ROWS
        || delta_leaf_bytes(manifest, leaves).is_none_or(|bytes| bytes > DELTA_LEAF_BYTES)
        || tree_nodes > MAX_DELTA_TREE_NODES
        || manifest.visibility_rows == 0
        || live_visibility_rows > rows
        || manifest.visibility_sequence_floor <= manifest.base_sequence_horizon
        || manifest.visibility_sequence_floor > manifest.visibility_sequence_horizon
        || manifest.visibility_sequence_horizon <= manifest.base_sequence_horizon
        || !visibility_covers_runs
    {
        return Err(invalid("V35 delta row admission differs"));
    }
    Ok(())
}

/// Seal one canonical immutable manifest for a bounded ordered delta snapshot.
pub fn seal_v35_delta(
    base_generation: V35StoredArtifactIdentity,
    base_manifest: &V35GenerationManifest,
    visibility: V35StoredArtifactIdentity,
    snapshot: &V35SnapshotVisibility,
    runs: Vec<V35DeltaRun>,
    uri: &str,
) -> Result<(Vec<u8>, V35ArtifactIdentity)> {
    if !uri.starts_with("s3://") || uri.contains("/corpus/") || uri.len() > URI_MAX_BYTES {
        return Err(invalid("V35 delta manifest URI differs"));
    }
    let base_bytes = canonical_json_bytes(base_manifest)?;
    validate_v35_manifest(&base_bytes, &base_generation.object)?;
    project_v35_serving_memory(base_manifest)?;
    let rows = runs.iter().try_fold(0_u64, |sum, run| {
        sum.checked_add(u64::from(run.rows))
            .ok_or_else(|| invalid("V35 delta row count overflows"))
    })?;
    let manifest = V35DeltaManifest {
        base_generation,
        base_sequence_horizon: base_manifest.sequence_horizon,
        format: DELTA_FORMAT.to_owned(),
        patches_per_leaf: base_manifest.patches_per_leaf,
        rows,
        routing_dimension: base_manifest.dimensions.routing,
        runs,
        visibility,
        tombstones: u64::try_from(snapshot.tombstone_rows())
            .map_err(|_| invalid("V35 delta tombstone rows overflow"))?,
        visibility_rows: u64::try_from(snapshot.row_count())
            .map_err(|_| invalid("V35 delta visibility rows overflow"))?,
        visibility_sequence_floor: snapshot.minimum_sequence(),
        visibility_sequence_horizon: snapshot.sequence_horizon(),
    };
    validate_delta_manifest_fields(&manifest)?;
    validate_v35_delta_visibility(&manifest, snapshot)?;
    let bytes = canonical_json_bytes(&manifest)?;
    if bytes.len() > DELTA_MANIFEST_MAX_BYTES {
        return Err(invalid("V35 delta manifest exceeds admission"));
    }
    let identity = V35ArtifactIdentity {
        digest: format!("{:x}", Sha256::digest(&bytes)),
        digest_algorithm: "sha256".to_owned(),
        length: u64::try_from(bytes.len())
            .map_err(|_| invalid("V35 delta manifest length overflows"))?,
        role: "delta-manifest".to_owned(),
        uri: uri.to_owned(),
    };
    Ok((bytes, identity))
}

/// Recompute a decoded visibility directory's exact delta-manifest bindings.
pub fn validate_v35_delta_visibility(
    manifest: &V35DeltaManifest,
    snapshot: &V35SnapshotVisibility,
) -> Result<()> {
    validate_delta_manifest_fields(manifest)?;
    let bytes = snapshot.canonical_bytes()?;
    let live_sequence_is_uncovered = snapshot.live_sequences().any(|sequence| {
        !manifest
            .runs
            .iter()
            .any(|run| run.sequence_floor <= sequence && sequence <= run.sequence_horizon)
    });
    if manifest.visibility.object.length != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || manifest.visibility.object.digest != format!("{:x}", Sha256::digest(&bytes))
        || manifest.visibility_rows != u64::try_from(snapshot.row_count()).unwrap_or(u64::MAX)
        || manifest.tombstones != u64::try_from(snapshot.tombstone_rows()).unwrap_or(u64::MAX)
        || manifest.visibility_sequence_floor != snapshot.minimum_sequence()
        || manifest.visibility_sequence_horizon != snapshot.sequence_horizon()
        || live_sequence_is_uncovered
    {
        return Err(invalid("V35 delta visibility binding differs"));
    }
    Ok(())
}

/// Authenticate and cross-check the compacted base referenced by a delta.
pub fn validate_v35_delta_base(
    manifest: &V35DeltaManifest,
    base_bytes: &[u8],
) -> Result<V35GenerationManifest> {
    validate_delta_manifest_fields(manifest)?;
    let base = validate_v35_manifest(base_bytes, &manifest.base_generation.object)?;
    project_v35_serving_memory(&base)?;
    if manifest.base_sequence_horizon != base.sequence_horizon
        || manifest.routing_dimension != base.dimensions.routing
        || manifest.patches_per_leaf != base.patches_per_leaf
    {
        return Err(invalid("V35 delta base binding differs"));
    }
    Ok(base)
}

/// Authenticate and decode one canonical bounded V35 delta manifest.
pub fn validate_v35_delta_manifest(
    bytes: &[u8],
    registered: &V35ArtifactIdentity,
) -> Result<V35DeltaManifest> {
    if bytes.is_empty()
        || bytes.len() > DELTA_MANIFEST_MAX_BYTES
        || registered.role != "delta-manifest"
        || registered.digest_algorithm != "sha256"
        || !is_digest(&registered.digest)
        || registered.length != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || registered.digest != format!("{:x}", Sha256::digest(bytes))
        || !registered.uri.starts_with("s3://")
        || registered.uri.contains("/corpus/")
        || registered.uri.len() > URI_MAX_BYTES
    {
        return Err(invalid("V35 registered delta manifest differs"));
    }
    let manifest: V35DeltaManifest =
        serde_json::from_slice(bytes).map_err(|_| invalid("V35 delta manifest JSON differs"))?;
    validate_delta_manifest_fields(&manifest)?;
    if canonical_json_bytes(&manifest)? != bytes {
        return Err(invalid("V35 delta manifest is not canonical"));
    }
    Ok(manifest)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V35GenerationHead {
    format: String,
    manifest: V35StoredArtifactIdentity,
}

/// Authenticate and decode the manifest capability in one canonical V35 head.
pub fn decode_v35_head(bytes: &[u8]) -> Result<V35StoredArtifactIdentity> {
    if bytes.is_empty()
        || bytes.len() > HEAD_MAX_BYTES
        || !bytes.ends_with(b"\n")
        || bytes.ends_with(b"\n\n")
    {
        return Err(invalid("V35 generation head bytes differ"));
    }
    let head: V35GenerationHead =
        serde_json::from_slice(bytes).map_err(|_| invalid("V35 generation head JSON differs"))?;
    if canonical_json_bytes(&head)? != bytes || head.format != HEAD_FORMAT {
        return Err(invalid("V35 generation head authority differs"));
    }
    validate_head_target_identity(&head.manifest.object)?;
    validate_version_id(&head.manifest.version_id)?;
    Ok(head.manifest)
}

/// Store an authenticated manifest, then attempt its generation-head update once.
fn publish_v35_head(
    manifest_bytes: &[u8],
    manifest_identity: &V35ArtifactIdentity,
    expected_head_version: Option<&str>,
    sink: &mut impl V35GenerationPublicationSink,
) -> Result<V35PublicationOutcome> {
    validate_head_target_identity(manifest_identity)?;
    if let Some(version_id) = expected_head_version {
        validate_version_id(version_id)?;
    }

    let manifest_version_id = sink.write_manifest(manifest_identity, manifest_bytes)?;
    validate_version_id(&manifest_version_id)?;
    let manifest = V35StoredArtifactIdentity {
        object: manifest_identity.clone(),
        version_id: manifest_version_id,
    };
    let head_bytes = canonical_json_bytes(&V35GenerationHead {
        format: HEAD_FORMAT.to_owned(),
        manifest: manifest.clone(),
    })?;
    if head_bytes.len() > HEAD_MAX_BYTES {
        return Err(invalid("V35 generation head exceeds admission"));
    }

    match sink.compare_and_swap_head(expected_head_version, &head_bytes)? {
        V35ConditionalHeadWrite::Committed { version_id } => {
            if validate_version_id(&version_id).is_err() {
                return Ok(V35PublicationOutcome::Indeterminate);
            }
            Ok(V35PublicationOutcome::Committed {
                manifest,
                head_version_id: version_id,
            })
        }
        V35ConditionalHeadWrite::Conflict => Ok(V35PublicationOutcome::Conflict),
        V35ConditionalHeadWrite::Indeterminate => Ok(V35PublicationOutcome::Indeterminate),
    }
}

/// Publish one authenticated base generation through the single V35 head.
pub fn publish_v35_generation(
    manifest_bytes: &[u8],
    manifest_identity: &V35ArtifactIdentity,
    expected_head_version: Option<&str>,
    sink: &mut impl V35GenerationPublicationSink,
) -> Result<V35PublicationOutcome> {
    validate_v35_manifest(manifest_bytes, manifest_identity)?;
    publish_v35_head(
        manifest_bytes,
        manifest_identity,
        expected_head_version,
        sink,
    )
}

/// Publish one authenticated delta snapshot through the same single V35 head.
pub fn publish_v35_delta(
    manifest_bytes: &[u8],
    manifest_identity: &V35ArtifactIdentity,
    base_manifest_bytes: &[u8],
    snapshot: &V35SnapshotVisibility,
    expected_head_version: Option<&str>,
    sink: &mut impl V35GenerationPublicationSink,
) -> Result<V35PublicationOutcome> {
    let manifest = validate_v35_delta_manifest(manifest_bytes, manifest_identity)?;
    validate_v35_delta_base(&manifest, base_manifest_bytes)?;
    validate_v35_delta_visibility(&manifest, snapshot)?;
    publish_v35_head(
        manifest_bytes,
        manifest_identity,
        expected_head_version,
        sink,
    )
}
