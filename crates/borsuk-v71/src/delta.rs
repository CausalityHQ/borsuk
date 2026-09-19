//! Immutable-generation authority and query-visible mutation semantics.

use std::{collections::BTreeMap, error::Error, fmt};

use bytes::Bytes;
use object_store::{
    ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion, path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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

impl RunAuthority {
    /// Returns the generation-local immutable run ID.
    pub fn run_id(&self) -> u32 {
        self.run_id
    }

    /// Returns `base` or `delta`.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Returns the exact URI, SHA-256, and byte length of the parent object.
    pub fn object_identity(&self) -> (&str, &str, u64) {
        (&self.object.uri, &self.object.sha256, self.object.bytes)
    }

    /// Returns the run-relative row offset of a registered page.
    pub fn page_row_offset(&self, target: u32) -> Option<u32> {
        let mut offset = 0u32;
        for page in &self.pages {
            if page.page == target {
                return Some(offset);
            }
            offset = offset.checked_add(page.rows)?;
        }
        None
    }
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

    /// Returns the last stable base-row ordinal excluded from the delta.
    pub fn base_horizon(&self) -> u64 {
        self.authority.base_horizon
    }

    /// Returns the exact vector dimensionality.
    pub fn dimensions(&self) -> u32 {
        self.authority.dimensions
    }

    /// Returns the requested result cardinality.
    pub fn neighbors(&self) -> u32 {
        self.authority.neighbors
    }

    /// Returns the registered maximum rows per page.
    pub fn page_rows(&self) -> u32 {
        self.authority.page_rows
    }

    /// Returns the exact URI, SHA-256, and byte length of the router.
    pub fn router_identity(&self) -> (&str, &str, u64) {
        let object = &self.authority.router;
        (&object.uri, &object.sha256, object.bytes)
    }

    /// Returns the exact URI, SHA-256, and byte length of the mutation directory.
    pub fn mutation_directory_identity(&self) -> (&str, &str, u64) {
        let object = &self.authority.mutation_directory;
        (&object.uri, &object.sha256, object.bytes)
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

/// One create-only object written before a compacted generation becomes visible.
#[derive(Clone, Debug)]
pub struct PublicationObject {
    /// Exact immutable object URI registered by the generation.
    pub uri: String,
    /// Object-store path of the immutable payload.
    pub path: ObjectPath,
    /// Complete authenticated payload bytes.
    pub bytes: Bytes,
}

/// Atomic publication plan for one already-validated compacted generation.
#[derive(Clone, Debug)]
pub struct CompactedPublication {
    /// Immutable run, directory, router, and receipt objects.
    pub immutable: Vec<PublicationObject>,
    /// Canonical generation document written after all immutable objects.
    pub generation: PublicationObject,
    /// SHA-256 of the generation that the new generation declares as prior.
    pub previous_generation_sha256: String,
    /// Conditional mutable head path.
    pub head_path: ObjectPath,
    /// Head payload binding the new generation.
    pub head_bytes: Bytes,
}

/// Testable crash boundary in the publication state machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationStop {
    /// Stop after every immutable payload is durable.
    AfterImmutable,
    /// Stop after the generation document is durable.
    AfterGeneration,
    /// Stop immediately before the conditional head update.
    BeforeHead,
    /// Stop after the conditional head update has committed.
    AfterHead,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GenerationHead {
    generation: u64,
    generation_path: String,
    generation_sha256: String,
    schema: String,
}

/// Publishes immutable compaction outputs before one conditional HEAD update.
pub async fn publish_compacted_generation(
    store: &dyn ObjectStore,
    publication: &CompactedPublication,
    expected_head: UpdateVersion,
    stop: Option<PublicationStop>,
) -> Result<UpdateVersion, DeltaError> {
    validate_publication(publication)?;
    for object in &publication.immutable {
        put_create_only(store, object).await?;
    }
    if stop == Some(PublicationStop::AfterImmutable) {
        return Err(DeltaError::authority("injected stop after immutable PUT"));
    }

    put_create_only(store, &publication.generation).await?;
    if stop == Some(PublicationStop::AfterGeneration) {
        return Err(DeltaError::authority("injected stop after generation PUT"));
    }
    if stop == Some(PublicationStop::BeforeHead) {
        return Err(DeltaError::authority("injected stop before head CAS"));
    }

    let version = publish_head(
        store,
        &publication.head_path,
        publication.head_bytes.clone(),
        Some(expected_head),
    )
    .await?;
    if stop == Some(PublicationStop::AfterHead) {
        return Err(DeltaError::authority("injected stop after head CAS"));
    }
    Ok(version)
}

fn validate_publication(publication: &CompactedPublication) -> Result<(), DeltaError> {
    let manifest = GenerationManifest::from_canonical_bytes(&publication.generation.bytes)?;
    if manifest.authority.previous_generation_sha256 != publication.previous_generation_sha256 {
        return Err(DeltaError::authority(
            "publication previous generation binding differs",
        ));
    }
    let generation_sha256 = format!("{:x}", Sha256::digest(&publication.generation.bytes));
    let head = GenerationHead {
        generation: manifest.generation(),
        generation_path: publication.generation.path.to_string(),
        generation_sha256,
        schema: "borsuk-v85-head-v1".into(),
    };
    let mut expected_head = serde_json::to_vec(&head)
        .map_err(|error| DeltaError::authority(format!("head serialization failed: {error}")))?;
    expected_head.push(b'\n');
    if publication.head_bytes.as_ref() != expected_head {
        return Err(DeltaError::authority("publication head binding differs"));
    }

    let mut required = std::collections::BTreeSet::<(&str, &str, u64)>::new();
    let mutation = &manifest.authority.mutation_directory;
    required.insert((&mutation.uri, &mutation.sha256, mutation.bytes));
    for run in &manifest.authority.runs {
        if run.generation == manifest.generation() {
            required.insert((&run.object.uri, &run.object.sha256, run.object.bytes));
        }
    }
    let mut supplied = std::collections::BTreeSet::new();
    for object in &publication.immutable {
        let sha256 = format!("{:x}", Sha256::digest(&object.bytes));
        let bytes = u64::try_from(object.bytes.len())
            .map_err(|_| DeltaError::authority("publication object length differs"))?;
        if !supplied.insert((object.uri.as_str(), sha256, bytes)) {
            return Err(DeltaError::authority("publication object is duplicated"));
        }
    }
    if supplied
        != required
            .into_iter()
            .map(|(uri, sha256, bytes)| (uri, sha256.to_string(), bytes))
            .collect()
    {
        return Err(DeltaError::authority("publication object binding differs"));
    }
    Ok(())
}

async fn put_create_only(
    store: &dyn ObjectStore,
    object: &PublicationObject,
) -> Result<(), DeltaError> {
    store
        .put_opts(
            &object.path,
            PutPayload::from(object.bytes.clone()),
            PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            },
        )
        .await
        .map(|_| ())
        .map_err(|error| DeltaError::authority(format!("immutable publish failed: {error}")))
}

/// One exact byte range required from an immutable run for a selected page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageRead {
    /// Immutable run containing the page.
    pub run_id: u32,
    /// Global routed page ordinal.
    pub page: u32,
    /// Run-relative row offset of the first row in this page.
    pub row_offset: u32,
    /// Authenticated parent-object URI.
    pub uri: String,
    /// Byte offset within the parent object.
    pub offset: u64,
    /// Exact byte length of the self-contained page stream.
    pub bytes: u64,
    /// Number of rows declared for the page stream.
    pub rows: u32,
}

/// Plans deterministic sparse range reads without falling back to whole runs.
pub fn plan_page_reads(
    selected: &[u32],
    generation: &GenerationManifest,
) -> Result<Vec<PageRead>, DeltaError> {
    if selected.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(DeltaError::authority(
            "selected pages are not strictly ordered",
        ));
    }

    let mut reads = Vec::new();
    for run in generation.runs() {
        let mut selected_index = 0usize;
        let mut page_index = 0usize;
        let mut row_offset = 0u32;
        while selected_index < selected.len() && page_index < run.pages.len() {
            let selected_page = selected[selected_index];
            let page = &run.pages[page_index];
            match page.page.cmp(&selected_page) {
                std::cmp::Ordering::Less => {
                    row_offset = row_offset
                        .checked_add(page.rows)
                        .ok_or_else(|| DeltaError::authority("run row offset overflows"))?;
                    page_index += 1;
                }
                std::cmp::Ordering::Greater => selected_index += 1,
                std::cmp::Ordering::Equal => {
                    reads.push(PageRead {
                        run_id: run.run_id,
                        page: page.page,
                        row_offset,
                        uri: run.object.uri.clone(),
                        offset: page.offset,
                        bytes: page.bytes,
                        rows: page.rows,
                    });
                    selected_index += 1;
                    row_offset = row_offset
                        .checked_add(page.rows)
                        .ok_or_else(|| DeltaError::authority("run row offset overflows"))?;
                    page_index += 1;
                }
            }
        }
    }
    Ok(reads)
}

/// One scored physical row before snapshot visibility and ID de-duplication.
#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    /// Stable record identity.
    pub id: i64,
    /// Row mutation sequence.
    pub sequence: u64,
    /// Smaller-is-better distance.
    pub distance: f32,
    /// Immutable run containing the physical row.
    pub run_id: u32,
    /// Row ordinal within the immutable run.
    pub row: u32,
}

/// Resolves snapshot visibility before deterministic `(distance, id)` top-k.
pub fn merge_candidates(
    base: impl Iterator<Item = Candidate>,
    delta: impl Iterator<Item = Candidate>,
    directory: &MutationDirectory,
    k: usize,
) -> Result<Vec<Candidate>, DeltaError> {
    let mut visible = BTreeMap::<i64, Candidate>::new();

    for candidate in base {
        validate_candidate(&candidate)?;
        if matches!(
            resolve_visibility(candidate.sequence, candidate.id, directory)?,
            Visibility::Base { .. }
        ) {
            retain_best_physical_copy(&mut visible, candidate);
        }
    }

    for candidate in delta {
        validate_candidate(&candidate)?;
        let mutation = directory.entries.get(&candidate.id).ok_or_else(|| {
            DeltaError::authority("delta candidate is absent from mutation directory")
        })?;
        if candidate.sequence < mutation.sequence {
            continue;
        }
        if candidate.sequence > mutation.sequence {
            return Err(DeltaError::authority(
                "delta candidate is newer than mutation directory",
            ));
        }
        match mutation.state {
            MutationState::Live { run_id, row }
                if run_id == candidate.run_id && row == candidate.row =>
            {
                retain_best_physical_copy(&mut visible, candidate);
            }
            MutationState::Live { .. } => {
                return Err(DeltaError::authority(
                    "delta candidate location differs from mutation directory",
                ));
            }
            MutationState::Tombstone => {
                return Err(DeltaError::authority(
                    "tombstone has a physical delta candidate",
                ));
            }
        }
    }

    let mut ordered = visible.into_values().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.distance
            .total_cmp(&right.distance)
            .then_with(|| left.id.cmp(&right.id))
    });
    ordered.truncate(k);
    Ok(ordered)
}

fn validate_candidate(candidate: &Candidate) -> Result<(), DeltaError> {
    if candidate.sequence == 0 || !candidate.distance.is_finite() {
        return Err(DeltaError::authority("candidate authority differs"));
    }
    Ok(())
}

fn retain_best_physical_copy(visible: &mut BTreeMap<i64, Candidate>, candidate: Candidate) {
    match visible.get(&candidate.id) {
        Some(prior)
            if (prior.distance, prior.run_id, prior.row)
                <= (candidate.distance, candidate.run_id, candidate.row) => {}
        _ => {
            visible.insert(candidate.id, candidate);
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::{ObjectStoreExt, UpdateVersion, memory::InMemory, path::Path};
    use sha2::Digest as _;

    use super::{
        Candidate, CompactedPublication, GenerationManifest, MutationDirectory, MutationRecord,
        MutationState, PageRead, PublicationObject, PublicationStop, SCHEMA, Visibility,
        merge_candidates, plan_page_reads, publish_compacted_generation, publish_head,
        resolve_visibility,
    };

    const ONE_DIGEST: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const TWO_DIGEST: &str = "2222222222222222222222222222222222222222222222222222222222222222";
    const A_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", sha2::Sha256::digest(bytes))
    }

    fn compacted_publication(head_path: Path) -> CompactedPublication {
        let mutation = Bytes::from_static(b"mutations");
        let delta = Bytes::from_static(b"delta");
        let generation_path = Path::from("index/g8/generation.json");
        let value = serde_json::json!({
            "base_horizon": 900_000,
            "dimensions": 2,
            "generation": 8,
            "mutation_directory": {"bytes": mutation.len(), "sha256": digest(&mutation), "uri": "s3://bucket/index/g8/mutations.arrow"},
            "neighbors": 1,
            "page_rows": 256,
            "previous_generation_sha256": ONE_DIGEST,
            "router": {"bytes": 10, "sha256": TWO_DIGEST, "uri": "s3://bucket/index/g0/router.arrow"},
            "runs": [
                {"generation": 0, "kind": "base", "object": {"bytes": 10, "sha256": ONE_DIGEST, "uri": "s3://bucket/index/g0/base.arrow"}, "pages": [{"bytes": 10, "offset": 0, "page": 0, "rows": 1}], "run_id": 0},
                {"generation": 8, "kind": "delta", "object": {"bytes": delta.len(), "sha256": digest(&delta), "uri": "s3://bucket/index/g8/delta-l1.arrow"}, "pages": [{"bytes": delta.len(), "offset": 0, "page": 0, "rows": 1}], "run_id": 1}
            ],
            "schema": SCHEMA,
            "source_split": "fixture"
        });
        let mut generation = serde_json::to_vec(&value).unwrap();
        generation.push(b'\n');
        let generation_sha256 = digest(&generation);
        let mut head = serde_json::to_vec(&serde_json::json!({
            "generation": 8,
            "generation_path": generation_path.to_string(),
            "generation_sha256": generation_sha256,
            "schema": "borsuk-v85-head-v1"
        }))
        .unwrap();
        head.push(b'\n');
        CompactedPublication {
            immutable: vec![
                PublicationObject {
                    uri: "s3://bucket/index/g8/mutations.arrow".into(),
                    path: Path::from("index/g8/mutations.arrow"),
                    bytes: mutation,
                },
                PublicationObject {
                    uri: "s3://bucket/index/g8/delta-l1.arrow".into(),
                    path: Path::from("index/g8/delta-l1.arrow"),
                    bytes: delta,
                },
            ],
            generation: PublicationObject {
                uri: "s3://bucket/index/g8/generation.json".into(),
                path: generation_path,
                bytes: Bytes::from(generation),
            },
            previous_generation_sha256: ONE_DIGEST.into(),
            head_path,
            head_bytes: Bytes::from(head),
        }
    }

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
        assert_eq!(manifest.base_horizon(), 900_000);
        assert_eq!(manifest.dimensions(), 768);
        assert_eq!(manifest.neighbors(), 100);
        assert_eq!(manifest.page_rows(), 256);
        assert_eq!(
            manifest.router_identity(),
            ("s3://bucket/index/g0007/router.arrow", TWO_DIGEST, 8192)
        );
        assert_eq!(
            manifest.mutation_directory_identity(),
            ("s3://bucket/index/g0007/mutations.arrow", A_DIGEST, 4096)
        );
        assert_eq!(manifest.runs().len(), 2);
        assert_eq!(manifest.runs()[1].run_id(), 1);
        assert_eq!(manifest.runs()[1].kind(), "delta");
        assert_eq!(
            manifest.runs()[1].object_identity(),
            ("s3://bucket/index/g0007/delta-000.arrow", TWO_DIGEST, 9000)
        );
        assert_eq!(manifest.runs()[1].page_row_offset(3), Some(12));
        assert_eq!(manifest.runs()[1].page_row_offset(2), None);
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

    #[tokio::test]
    async fn compacted_generation_crash_boundaries_reopen_one_complete_snapshot() {
        // Break caught: publication advances HEAD before every immutable object and
        // generation document are durable, exposing a mixture after a crash.
        for (stop, head_advanced) in [
            (PublicationStop::AfterImmutable, false),
            (PublicationStop::AfterGeneration, false),
            (PublicationStop::BeforeHead, false),
            (PublicationStop::AfterHead, true),
        ] {
            let store = InMemory::new();
            let head = Path::from("index/HEAD.json");
            let prior = publish_head(&store, &head, Bytes::from_static(b"old\n"), None)
                .await
                .unwrap();
            let publication = compacted_publication(head.clone());

            assert!(
                publish_compacted_generation(&store, &publication, prior, Some(stop))
                    .await
                    .is_err()
            );
            let expected_head = if head_advanced {
                publication.head_bytes.as_ref()
            } else {
                b"old\n".as_slice()
            };
            assert_eq!(
                store.get(&head).await.unwrap().bytes().await.unwrap(),
                expected_head
            );
            if head_advanced {
                assert_eq!(
                    store
                        .get(&Path::from("index/g8/delta-l1.arrow"))
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap(),
                    "delta"
                );
                assert_eq!(
                    store
                        .get(&Path::from("index/g8/generation.json"))
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap(),
                    publication.generation.bytes
                );
            }
        }
    }

    #[tokio::test]
    async fn compacted_publication_rejects_unbound_generation_and_head() {
        // Break caught: callers can CAS HEAD to arbitrary bytes unrelated to the
        // generation document or immutable objects they just uploaded.
        let store = InMemory::new();
        let head = Path::from("index/HEAD.json");
        let prior = publish_head(&store, &head, Bytes::from_static(b"old\n"), None)
            .await
            .unwrap();
        let publication = CompactedPublication {
            immutable: vec![PublicationObject {
                uri: "s3://bucket/index/g8/delta-l1.arrow".into(),
                path: Path::from("index/g8/delta-l1.arrow"),
                bytes: Bytes::from_static(b"delta"),
            }],
            generation: PublicationObject {
                uri: "s3://bucket/index/g8/generation.json".into(),
                path: Path::from("index/g8/generation.json"),
                bytes: Bytes::from_static(b"not-a-generation\n"),
            },
            previous_generation_sha256: ONE_DIGEST.into(),
            head_path: head.clone(),
            head_bytes: Bytes::from_static(b"unbound-head\n"),
        };
        assert!(
            publish_compacted_generation(&store, &publication, prior, None)
                .await
                .is_err()
        );
        assert_eq!(
            store.get(&head).await.unwrap().bytes().await.unwrap(),
            "old\n"
        );
    }

    fn sparse_generation() -> GenerationManifest {
        let object = |uri: &str, digest: &str, bytes: u64| serde_json::json!({"bytes": bytes, "sha256": digest, "uri": uri});
        let run = |generation: u64,
                   kind: &str,
                   run_id: u32,
                   uri: &str,
                   digest: &str,
                   pages: Vec<(u64, u64, u32, u32)>| {
            serde_json::json!({
                "generation": generation,
                "kind": kind,
                "object": object(uri, digest, 10_000),
                "pages": pages.into_iter().map(|(bytes, offset, page, rows)| {
                    serde_json::json!({"bytes": bytes, "offset": offset, "page": page, "rows": rows})
                }).collect::<Vec<_>>(),
                "run_id": run_id,
            })
        };
        let value = serde_json::json!({
            "base_horizon": 900_000,
            "dimensions": 768,
            "generation": 7,
            "mutation_directory": object("s3://bucket/g7/mutations.arrow", A_DIGEST, 4096),
            "neighbors": 100,
            "page_rows": 256,
            "previous_generation_sha256": ONE_DIGEST,
            "router": object("s3://bucket/g7/router.arrow", TWO_DIGEST, 8192),
            "runs": [
                run(0, "base", 0, "s3://bucket/g0/base-0.arrow", ONE_DIGEST,
                    vec![(100, 0, 0, 256), (110, 100, 1, 256)]),
                run(0, "base", 1, "s3://bucket/g0/base-1.arrow", TWO_DIGEST,
                    vec![(120, 0, 2, 256), (130, 120, 3, 256)]),
                run(7, "delta", 2, "s3://bucket/g7/delta-0.arrow", A_DIGEST,
                    vec![(40, 8, 1, 9), (50, 48, 4, 11)]),
                run(7, "delta", 3, "s3://bucket/g7/delta-1.arrow", ONE_DIGEST,
                    vec![(60, 16, 3, 13)]),
                run(7, "delta", 4, "s3://bucket/g7/delta-2.arrow", TWO_DIGEST,
                    vec![(70, 24, 1, 17), (80, 94, 3, 19)]),
            ],
            "schema": SCHEMA,
            "source_split": "relaion-1m-base900000-delta100000",
        });
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        GenerationManifest::from_canonical_bytes(&bytes).unwrap()
    }

    #[test]
    fn page_plan_reads_only_selected_pages_present_in_each_pinned_run() {
        let generation = sparse_generation();
        let reads = plan_page_reads(&[1, 3], &generation).unwrap();
        assert_eq!(
            reads,
            vec![
                PageRead {
                    run_id: 0,
                    page: 1,
                    row_offset: 256,
                    uri: "s3://bucket/g0/base-0.arrow".into(),
                    offset: 100,
                    bytes: 110,
                    rows: 256
                },
                PageRead {
                    run_id: 1,
                    page: 3,
                    row_offset: 256,
                    uri: "s3://bucket/g0/base-1.arrow".into(),
                    offset: 120,
                    bytes: 130,
                    rows: 256
                },
                PageRead {
                    run_id: 2,
                    page: 1,
                    row_offset: 0,
                    uri: "s3://bucket/g7/delta-0.arrow".into(),
                    offset: 8,
                    bytes: 40,
                    rows: 9
                },
                PageRead {
                    run_id: 3,
                    page: 3,
                    row_offset: 0,
                    uri: "s3://bucket/g7/delta-1.arrow".into(),
                    offset: 16,
                    bytes: 60,
                    rows: 13
                },
                PageRead {
                    run_id: 4,
                    page: 1,
                    row_offset: 0,
                    uri: "s3://bucket/g7/delta-2.arrow".into(),
                    offset: 24,
                    bytes: 70,
                    rows: 17
                },
                PageRead {
                    run_id: 4,
                    page: 3,
                    row_offset: 17,
                    uri: "s3://bucket/g7/delta-2.arrow".into(),
                    offset: 94,
                    bytes: 80,
                    rows: 19
                },
            ]
        );
        assert!(reads.iter().all(|read| read.bytes < 10_000));
        assert!(plan_page_reads(&[3, 1], &generation).is_err());
        assert!(plan_page_reads(&[1, 1], &generation).is_err());
    }

    fn candidate(id: i64, sequence: u64, distance: f32, run_id: u32, row: u32) -> Candidate {
        Candidate {
            id,
            sequence,
            distance,
            run_id,
            row,
        }
    }

    #[test]
    fn merge_suppresses_replaced_and_tombstoned_base_before_top_k() {
        let directory = MutationDirectory::try_from_entries(vec![
            MutationRecord {
                id: 2,
                sequence: 5,
                state: MutationState::Live { run_id: 9, row: 4 },
            },
            MutationRecord {
                id: 3,
                sequence: 6,
                state: MutationState::Tombstone,
            },
        ])
        .unwrap();
        let base = vec![
            candidate(1, 1, 0.40, 0, 0),
            candidate(2, 1, 0.01, 0, 1),
            candidate(3, 1, 0.02, 0, 2),
            candidate(4, 1, 0.50, 0, 3),
        ];
        let merged =
            merge_candidates(base.into_iter(), Vec::new().into_iter(), &directory, 2).unwrap();
        assert_eq!(merged.iter().map(|row| row.id).collect::<Vec<_>>(), [1, 4]);
    }

    #[test]
    fn merge_admits_only_directory_bound_live_delta_and_orders_ties_by_id() {
        let directory = MutationDirectory::try_from_entries(vec![
            MutationRecord {
                id: 5,
                sequence: 8,
                state: MutationState::Live { run_id: 2, row: 7 },
            },
            MutationRecord {
                id: 6,
                sequence: 9,
                state: MutationState::Live { run_id: 3, row: 1 },
            },
        ])
        .unwrap();
        let base = vec![candidate(1, 1, 0.2, 0, 0), candidate(9, 1, 0.4, 0, 1)];
        let delta = vec![
            candidate(6, 7, 0.01, 1, 1),
            candidate(5, 8, 0.1, 2, 7),
            candidate(5, 8, 0.1, 2, 7),
            candidate(6, 9, 0.1, 3, 1),
        ];
        let merged = merge_candidates(base.into_iter(), delta.into_iter(), &directory, 3).unwrap();
        assert_eq!(
            merged.iter().map(|row| row.id).collect::<Vec<_>>(),
            [5, 6, 1]
        );
    }

    #[test]
    fn merge_rejects_nonfinite_ties_and_mutation_directory_omissions() {
        let empty = MutationDirectory::try_from_entries(Vec::new()).unwrap();
        assert!(
            merge_candidates(
                vec![candidate(1, 1, f32::NAN, 0, 0)].into_iter(),
                Vec::new().into_iter(),
                &empty,
                1
            )
            .is_err()
        );

        let tied = MutationDirectory::try_from_entries(vec![MutationRecord {
            id: 2,
            sequence: 4,
            state: MutationState::Tombstone,
        }])
        .unwrap();
        assert!(
            merge_candidates(
                vec![candidate(2, 4, 0.2, 0, 0)].into_iter(),
                Vec::new().into_iter(),
                &tied,
                1
            )
            .is_err()
        );

        assert!(
            merge_candidates(
                Vec::new().into_iter(),
                vec![candidate(7, 5, 0.2, 3, 0)].into_iter(),
                &empty,
                1
            )
            .is_err()
        );
    }
}
