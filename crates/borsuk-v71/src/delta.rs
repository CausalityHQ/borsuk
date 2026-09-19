//! Immutable-generation authority and query-visible mutation semantics.

use std::{collections::BTreeMap, error::Error, fmt};

use bytes::Bytes;
use object_store::{
    ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion, path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};

const SCHEMA: &str = "borsuk-v85-generation-v1";

/// A fail-closed V85 authority or visibility error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaError(String);

impl DeltaError {
    fn authority(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for DeltaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for DeltaError {}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ObjectIdentity {
    bytes: u64,
    sha256: String,
    uri: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PageAuthority {
    bytes: u64,
    offset: u64,
    page: u32,
    rows: u32,
}

/// One immutable base or delta run registered by a generation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunAuthority {
    generation: u64,
    kind: String,
    object: ObjectIdentity,
    pages: Vec<PageAuthority>,
    run_id: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GenerationAuthority {
    base_horizon: u64,
    dimensions: u32,
    generation: u64,
    mutation_directory: ObjectIdentity,
    neighbors: u32,
    page_rows: u32,
    previous_generation_sha256: String,
    router: ObjectIdentity,
    runs: Vec<RunAuthority>,
    schema: String,
    source_split: String,
}

/// Exact authenticated authority pinned by one V85 reader.
#[derive(Clone, Debug)]
pub struct GenerationManifest {
    authority: GenerationAuthority,
}

impl GenerationManifest {
    /// Parses only exact, compact, sorted canonical JSON with one trailing LF.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, DeltaError> {
        let payload = bytes
            .strip_suffix(b"\n")
            .ok_or_else(|| DeltaError::authority("generation must end with one LF"))?;
        if payload.ends_with(b"\n") {
            return Err(DeltaError::authority(
                "generation has multiple trailing LFs",
            ));
        }
        let authority: GenerationAuthority = serde_json::from_slice(payload)
            .map_err(|error| DeltaError::authority(format!("generation JSON differs: {error}")))?;
        let manifest = Self { authority };
        manifest.validate()?;
        if manifest.canonical_bytes()? != bytes {
            return Err(DeltaError::authority("generation bytes are not canonical"));
        }
        Ok(manifest)
    }

    /// Serializes the exact canonical generation authority with one trailing LF.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DeltaError> {
        self.validate()?;
        let mut bytes = serde_json::to_vec(&self.authority).map_err(|error| {
            DeltaError::authority(format!("generation serialization failed: {error}"))
        })?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Returns the monotonically increasing generation number.
    pub fn generation(&self) -> u64 {
        self.authority.generation
    }

    /// Returns the ordered immutable runs pinned by this generation.
    pub fn runs(&self) -> &[RunAuthority] {
        &self.authority.runs
    }

    fn validate(&self) -> Result<(), DeltaError> {
        let authority = &self.authority;
        if authority.schema != SCHEMA {
            return Err(DeltaError::authority("generation schema differs"));
        }
        if authority.generation == 0
            || authority.base_horizon == 0
            || authority.dimensions == 0
            || authority.neighbors == 0
            || authority.page_rows == 0
            || authority.source_split.is_empty()
        {
            return Err(DeltaError::authority("generation scalar authority differs"));
        }
        validate_digest(&authority.previous_generation_sha256)?;
        validate_object(&authority.mutation_directory)?;
        validate_object(&authority.router)?;
        if authority.runs.is_empty() {
            return Err(DeltaError::authority("generation has no runs"));
        }
        let mut prior_run = None;
        let mut uris = std::collections::BTreeSet::new();
        uris.insert(authority.mutation_directory.uri.as_str());
        if !uris.insert(authority.router.uri.as_str()) {
            return Err(DeltaError::authority("generation object URI is reused"));
        }
        for run in &authority.runs {
            if prior_run.is_some_and(|prior| run.run_id <= prior) {
                return Err(DeltaError::authority(
                    "generation runs are not strictly ordered",
                ));
            }
            prior_run = Some(run.run_id);
            if run.generation > authority.generation
                || !matches!(run.kind.as_str(), "base" | "delta")
            {
                return Err(DeltaError::authority("run authority differs"));
            }
            validate_object(&run.object)?;
            if !uris.insert(run.object.uri.as_str()) {
                return Err(DeltaError::authority("generation object URI is reused"));
            }
            if run.pages.is_empty() {
                return Err(DeltaError::authority("run has no pages"));
            }
            let mut prior_page = None;
            let mut prior_end = 0u64;
            for page in &run.pages {
                if page.bytes == 0 || page.rows == 0 {
                    return Err(DeltaError::authority("page length or row count is zero"));
                }
                if prior_page.is_some_and(|prior| page.page <= prior) {
                    return Err(DeltaError::authority("run pages are not strictly ordered"));
                }
                let end = page
                    .offset
                    .checked_add(page.bytes)
                    .ok_or_else(|| DeltaError::authority("page byte range overflows"))?;
                if page.offset < prior_end || end > run.object.bytes {
                    return Err(DeltaError::authority("page byte range differs"));
                }
                prior_page = Some(page.page);
                prior_end = end;
            }
        }
        Ok(())
    }
}

fn validate_digest(value: &str) -> Result<(), DeltaError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(DeltaError::authority("SHA-256 digest differs"));
    }
    Ok(())
}

fn validate_object(object: &ObjectIdentity) -> Result<(), DeltaError> {
    if object.bytes == 0 || !object.uri.starts_with("s3://") {
        return Err(DeltaError::authority("object identity differs"));
    }
    validate_digest(&object.sha256)
}

/// The newest known state for one post-base mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MutationState {
    /// A live row stored in the named immutable run and row ordinal.
    Live {
        /// Immutable run containing the newest row.
        run_id: u32,
        /// Row ordinal within that run.
        row: u32,
    },
    /// A deletion with no dummy vector payload.
    Tombstone,
}

/// One ordered mutation-directory input record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationRecord {
    /// Stable record identity.
    pub id: i64,
    /// Monotonic mutation sequence.
    pub sequence: u64,
    /// New live location or tombstone state.
    pub state: MutationState,
}

/// Resident newest-state directory for IDs changed after the base horizon.
#[derive(Clone, Debug, Default)]
pub struct MutationDirectory {
    entries: BTreeMap<i64, MutationRecord>,
}

impl MutationDirectory {
    /// Retains the greatest sequence per ID and rejects ambiguous sequence ties.
    pub fn try_from_entries(entries: Vec<MutationRecord>) -> Result<Self, DeltaError> {
        let mut newest = BTreeMap::<i64, MutationRecord>::new();
        for entry in entries {
            if entry.sequence == 0 {
                return Err(DeltaError::authority("mutation sequence is zero"));
            }
            match newest.get(&entry.id) {
                Some(prior) if prior.sequence == entry.sequence => {
                    return Err(DeltaError::authority("mutation sequence tie"));
                }
                Some(prior) if prior.sequence > entry.sequence => {}
                _ => {
                    newest.insert(entry.id, entry);
                }
            }
        }
        Ok(Self { entries: newest })
    }
}

/// The row state a reader may admit for one ID at its pinned generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Visibility {
    /// The immutable base row remains current.
    Base {
        /// Base row sequence.
        sequence: u64,
    },
    /// A newer live mutation replaces the base row.
    LiveMutation {
        /// Newest live sequence.
        sequence: u64,
        /// Immutable run containing the newest row.
        run_id: u32,
        /// Row ordinal within that run.
        row: u32,
    },
    /// A newer tombstone suppresses all physical copies.
    Suppressed {
        /// Tombstone sequence suppressing physical copies.
        sequence: u64,
    },
}

/// Resolves newest-write-wins visibility and rejects a base/mutation tie.
pub fn resolve_visibility(
    base_sequence: u64,
    id: i64,
    directory: &MutationDirectory,
) -> Result<Visibility, DeltaError> {
    let Some(mutation) = directory.entries.get(&id) else {
        return Ok(Visibility::Base {
            sequence: base_sequence,
        });
    };
    if mutation.sequence == base_sequence {
        return Err(DeltaError::authority("base and mutation sequence tie"));
    }
    if mutation.sequence < base_sequence {
        return Ok(Visibility::Base {
            sequence: base_sequence,
        });
    }
    match mutation.state {
        MutationState::Live { run_id, row } => Ok(Visibility::LiveMutation {
            sequence: mutation.sequence,
            run_id,
            row,
        }),
        MutationState::Tombstone => Ok(Visibility::Suppressed {
            sequence: mutation.sequence,
        }),
    }
}

/// Creates a generation head or conditionally advances its exact prior version.
pub async fn publish_head(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    bytes: Bytes,
    expected: Option<UpdateVersion>,
) -> Result<UpdateVersion, DeltaError> {
    let mode = expected.map_or(PutMode::Create, PutMode::Update);
    store
        .put_opts(
            path,
            PutPayload::from(bytes),
            PutOptions {
                mode,
                ..PutOptions::default()
            },
        )
        .await
        .map(UpdateVersion::from)
        .map_err(|error| DeltaError::authority(format!("generation head publish failed: {error}")))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::{ObjectStoreExt, UpdateVersion, memory::InMemory, path::Path};

    use super::{
        GenerationManifest, MutationDirectory, MutationRecord, MutationState, Visibility,
        publish_head, resolve_visibility,
    };

    const ONE_DIGEST: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const TWO_DIGEST: &str = "2222222222222222222222222222222222222222222222222222222222222222";
    const A_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn canonical_manifest() -> Vec<u8> {
        format!(
            "{{\"base_horizon\":900000,\"dimensions\":768,\"generation\":7,\"mutation_directory\":{{\"bytes\":4096,\"sha256\":\"{A_DIGEST}\",\"uri\":\"s3://bucket/index/g0007/mutations.arrow\"}},\"neighbors\":100,\"page_rows\":256,\"previous_generation_sha256\":\"{ONE_DIGEST}\",\"router\":{{\"bytes\":8192,\"sha256\":\"{TWO_DIGEST}\",\"uri\":\"s3://bucket/index/g0007/router.arrow\"}},\"runs\":[{{\"generation\":0,\"kind\":\"base\",\"object\":{{\"bytes\":10000,\"sha256\":\"{ONE_DIGEST}\",\"uri\":\"s3://bucket/index/g0000/base-000.arrow\"}},\"pages\":[{{\"bytes\":400,\"offset\":0,\"page\":0,\"rows\":256}},{{\"bytes\":300,\"offset\":400,\"page\":1,\"rows\":128}}],\"run_id\":0}},{{\"generation\":7,\"kind\":\"delta\",\"object\":{{\"bytes\":9000,\"sha256\":\"{TWO_DIGEST}\",\"uri\":\"s3://bucket/index/g0007/delta-000.arrow\"}},\"pages\":[{{\"bytes\":200,\"offset\":64,\"page\":1,\"rows\":12}},{{\"bytes\":240,\"offset\":264,\"page\":3,\"rows\":16}}],\"run_id\":1}}],\"schema\":\"borsuk-v85-generation-v1\",\"source_split\":\"relaion-1m-base900000-delta100000\"}}\n"
        )
        .into_bytes()
    }

    #[test]
    fn generation_manifest_accepts_exact_canonical_authority() {
        let bytes = canonical_manifest();
        let manifest = GenerationManifest::from_canonical_bytes(&bytes).unwrap();
        assert_eq!(manifest.generation(), 7);
        assert_eq!(manifest.runs().len(), 2);
        assert_eq!(manifest.canonical_bytes().unwrap(), bytes);
    }

    #[test]
    fn generation_manifest_rejects_schema_digest_order_and_range_drift() {
        let baseline = String::from_utf8(canonical_manifest()).unwrap();
        let cases = [
            (
                "unknown-key",
                baseline.replace(
                    "\"schema\":\"borsuk-v85-generation-v1\"",
                    "\"extra\":0,\"schema\":\"borsuk-v85-generation-v1\"",
                ),
            ),
            (
                "uppercase-digest",
                baseline.replacen(A_DIGEST, &A_DIGEST.to_uppercase(), 1),
            ),
            (
                "zero-length",
                baseline.replacen("\"bytes\":4096", "\"bytes\":0", 1),
            ),
            (
                "unsorted-pages",
                baseline.replace(
                    "\"page\":0,\"rows\":256},{\"bytes\":300,\"offset\":400,\"page\":1",
                    "\"page\":2,\"rows\":256},{\"bytes\":300,\"offset\":400,\"page\":1",
                ),
            ),
            (
                "overlap",
                baseline.replacen(
                    "\"bytes\":300,\"offset\":400",
                    "\"bytes\":300,\"offset\":399",
                    1,
                ),
            ),
            (
                "noncanonical-key-order",
                baseline.replacen(
                    "\"base_horizon\":900000,\"dimensions\":768",
                    "\"dimensions\":768,\"base_horizon\":900000",
                    1,
                ),
            ),
        ];
        for (name, candidate) in cases {
            assert!(
                GenerationManifest::from_canonical_bytes(candidate.as_bytes()).is_err(),
                "{name} unexpectedly passed"
            );
        }

        let overflow = baseline.replacen(
            "\"bytes\":240,\"offset\":264",
            "\"bytes\":18446744073709551615,\"offset\":264",
            1,
        );
        assert!(GenerationManifest::from_canonical_bytes(overflow.as_bytes()).is_err());
    }

    #[test]
    fn mutation_visibility_is_latest_wins_and_ties_fail_closed() {
        let directory = MutationDirectory::try_from_entries(vec![
            MutationRecord {
                id: 11,
                sequence: 4,
                state: MutationState::Live { run_id: 2, row: 9 },
            },
            MutationRecord {
                id: 12,
                sequence: 7,
                state: MutationState::Tombstone,
            },
        ])
        .unwrap();

        assert_eq!(
            resolve_visibility(3, 10, &directory).unwrap(),
            Visibility::Base { sequence: 3 }
        );
        assert_eq!(
            resolve_visibility(3, 11, &directory).unwrap(),
            Visibility::LiveMutation {
                sequence: 4,
                run_id: 2,
                row: 9,
            }
        );
        assert_eq!(
            resolve_visibility(3, 12, &directory).unwrap(),
            Visibility::Suppressed { sequence: 7 }
        );
        assert!(resolve_visibility(4, 11, &directory).is_err());

        assert!(
            MutationDirectory::try_from_entries(vec![
                MutationRecord {
                    id: 99,
                    sequence: 5,
                    state: MutationState::Tombstone,
                },
                MutationRecord {
                    id: 99,
                    sequence: 5,
                    state: MutationState::Live { run_id: 1, row: 0 },
                },
            ])
            .is_err()
        );
    }

    #[tokio::test]
    async fn generation_head_is_create_or_conditional_update_never_overwrite() {
        let store = InMemory::new();
        let path = Path::from("index/HEAD.json");
        let first = publish_head(&store, &path, Bytes::from_static(b"first\n"), None)
            .await
            .unwrap();
        assert!(
            publish_head(&store, &path, Bytes::from_static(b"duplicate\n"), None)
                .await
                .is_err()
        );
        let stale = UpdateVersion {
            e_tag: Some("stale".into()),
            version: None,
        };
        assert!(
            publish_head(&store, &path, Bytes::from_static(b"stale\n"), Some(stale),)
                .await
                .is_err()
        );
        publish_head(&store, &path, Bytes::from_static(b"second\n"), Some(first))
            .await
            .unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            "second\n"
        );
    }
}
