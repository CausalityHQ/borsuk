use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    BorsukError, Result, V35ArtifactIdentity, V35StoredArtifactIdentity, validate_v35_manifest,
};

const HEAD_FORMAT: &str = "borsuk-v35-generation-head-v1";
const VERSION_ID_MAX_BYTES: usize = 1_024;

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

fn validate_manifest_identity(identity: &V35ArtifactIdentity) -> Result<()> {
    if identity.role != "generation-manifest"
        || identity.digest_algorithm != "sha256"
        || !is_digest(&identity.digest)
        || identity.length == 0
        || identity.uri.is_empty()
    {
        return Err(invalid("V35 generation-head manifest identity differs"));
    }
    Ok(())
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V35GenerationHead {
    format: String,
    manifest: V35StoredArtifactIdentity,
}

/// Authenticate and decode the manifest capability in one canonical V35 head.
pub fn decode_v35_generation_head(bytes: &[u8]) -> Result<V35StoredArtifactIdentity> {
    if !bytes.ends_with(b"\n") || bytes.ends_with(b"\n\n") {
        return Err(invalid("V35 generation head bytes differ"));
    }
    let head: V35GenerationHead =
        serde_json::from_slice(bytes).map_err(|_| invalid("V35 generation head JSON differs"))?;
    if canonical_json_bytes(&head)? != bytes || head.format != HEAD_FORMAT {
        return Err(invalid("V35 generation head authority differs"));
    }
    validate_manifest_identity(&head.manifest.object)?;
    validate_version_id(&head.manifest.version_id)?;
    Ok(head.manifest)
}

/// Store an authenticated manifest, then attempt its generation-head update once.
pub fn publish_v35_generation(
    manifest_bytes: &[u8],
    manifest_identity: &V35ArtifactIdentity,
    expected_head_version: Option<&str>,
    sink: &mut impl V35GenerationPublicationSink,
) -> Result<V35PublicationOutcome> {
    validate_v35_manifest(manifest_bytes, manifest_identity)?;
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
