use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array,
    Int64Array, RecordBatch, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::{FileReader as ArrowFileReader, read_footer_length},
    root_as_footer, root_as_message,
    writer::{FileWriter as ArrowFileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, ObjectStoreScheme};
use parquet::{
    arrow::{ArrowSchemaConverter, ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
    schema::types::SchemaDescriptor,
};
use rayon::{ThreadPoolBuilder, prelude::*};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result, V36ArtifactIdentity, V36PrefixCheckpointContext,
    V36PrefixCheckpointManifest, V36PrefixCheckpointPhase, V36PrefixCheckpointPointer,
    V36PrefixCheckpointPublication, V36PrefixFreezeAuthority, V36PrefixFreezeExecutionAuthority,
    V36PrefixFreezeReceipt, V36PrefixPopulationAuthority, V36PrefixPopulationCheckpoint,
    V36PrefixRegisteredSourceObject, V36PrefixRoleAuthority, V36PrefixSourceObject,
    bind_v36_prefix_population_authority, canonical_v36_prefix_checkpoint_manifest_bytes,
    canonical_v36_prefix_checkpoint_pointer_bytes, canonical_v36_prefix_freeze_authority_bytes,
    canonical_v36_prefix_freeze_execution_authority_bytes,
    canonical_v36_prefix_freeze_receipt_bytes, canonical_v36_prefix_population_authority_bytes,
    canonical_v36_prefix_source_registry_bytes, plan_v36_prefix_checkpoint_publication,
    validate_v36_prefix_checkpoint_manifest_with_context,
    validate_v36_prefix_checkpoint_transition, validate_v36_prefix_freeze_authority,
    validate_v36_prefix_freeze_execution_authority, validate_v36_prefix_population_authority,
};

const DIMENSIONS: usize = 768;
const GT_NEIGHBORS: usize = 100;
const DISTINCT_CANDIDATES: usize = 1_100_000;
const CORPUS_ROWS: usize = 1_000_000;
const PARQUET_ROW_GROUP_ROWS: usize = 8_192;
const IDENTITY_RUN_BATCH_ROWS: usize = 65_536;
const IDENTITY_RUN_FORMAT: &str = "borsuk-v36-prefix-identity-run-v2";
const SELECTED_IDS_BATCH_ROWS: usize = 65_536;
const POPULATION_SCORE_ALGORITHM: &str =
    "sha256-seed-sha256-manifest-sha256-feature-row-id-le-u64-v2";
const POPULATION_SEED_SHA256: &str =
    "bcb490ff7944bfa3a0a6d5abe6d35ba34ecaba60b615e214edb057a1a5b63b8e";
const SELECTED_IDS_FORMAT: &str = "borsuk-v36-prefix-selected-identities-v2";

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn resource_limit(message: &str) -> BorsukError {
    BorsukError::V36PrefixResourceLimit(message.to_owned())
}

fn temporary_output(path: &Path) -> Result<tempfile::NamedTempFile> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("V36 prefix output path has no parent"))?;
    tempfile::NamedTempFile::new_in(parent).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })
}

fn publish_output(temporary: tempfile::NamedTempFile, path: &Path) -> Result<()> {
    temporary
        .as_file()
        .sync_all()
        .map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
    temporary.persist(path).map_err(|error| BorsukError::Io {
        path: path.to_owned(),
        source: error.error,
    })?;
    Ok(())
}

fn publish_output_noclobber(temporary: tempfile::NamedTempFile, path: &Path) -> Result<()> {
    temporary
        .as_file()
        .sync_all()
        .map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| BorsukError::Io {
            path: path.to_owned(),
            source: error.error,
        })?;
    sync_directory(
        path.parent()
            .ok_or_else(|| invalid("V36 prefix output path has no parent"))?,
    )
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })
}

fn install_content_addressed(path: &Path, bytes: &[u8]) -> Result<()> {
    if path.exists() {
        if read_file(path)? == bytes {
            return Ok(());
        }
        return Err(invalid("V36 checkpoint outbox object conflicts"));
    }
    let mut temporary = temporary_output(path)?;
    temporary
        .write_all(bytes)
        .map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| BorsukError::Io {
            path: path.to_owned(),
            source: error.error,
        })?;
    sync_directory(
        path.parent()
            .ok_or_else(|| invalid("V36 checkpoint outbox parent is missing"))?,
    )
}

#[derive(serde::Serialize)]
struct V36PrefixCheckpointReady<'a> {
    dependencies: &'a [V36ArtifactIdentity],
    generation: u32,
    manifest: &'a V36ArtifactIdentity,
    pointer_encoded_bytes: u64,
    pointer_sha256: String,
    pointer_uri: &'a str,
    previous_pointer_sha256: &'a Option<String>,
    schema: &'static str,
}

#[derive(Debug)]
/// Private durable filesystem bridge from Rust science to the S3 supervisor.
pub struct V36PrefixCheckpointOutbox {
    root: PathBuf,
}

impl V36PrefixCheckpointOutbox {
    /// Create one empty private outbox and its fixed subdirectories.
    pub fn create(root: &Path) -> Result<Self> {
        let metadata = fs::symlink_metadata(root).map_err(|source| BorsukError::Io {
            path: root.to_owned(),
            source,
        })?;
        if !metadata.file_type().is_dir()
            || metadata.file_type().is_symlink()
            || root
                .read_dir()
                .map_err(|source| BorsukError::Io {
                    path: root.to_owned(),
                    source,
                })?
                .next()
                .is_some()
        {
            return Err(invalid("V36 checkpoint outbox root differs"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root, fs::Permissions::from_mode(0o700)).map_err(|source| {
                BorsukError::Io {
                    path: root.to_owned(),
                    source,
                }
            })?;
        }
        for child in ["objects", "manifests", "pointers", "commits"] {
            fs::create_dir(root.join(child)).map_err(|source| BorsukError::Io {
                path: root.join(child),
                source,
            })?;
        }
        sync_directory(root)?;
        Ok(Self {
            root: root.to_owned(),
        })
    }

    /// Commit one already validated publication, exposing its descriptor last.
    pub fn commit(
        &self,
        publication: &V36PrefixCheckpointPublication,
        dependencies: &[(V36ArtifactIdentity, Vec<u8>)],
    ) -> Result<PathBuf> {
        if dependencies.len() != publication.dependencies.len()
            || dependencies
                .iter()
                .zip(&publication.dependencies)
                .any(|((identity, _), expected)| identity != expected)
        {
            return Err(invalid("V36 checkpoint outbox dependencies differ"));
        }
        for (identity, bytes) in dependencies {
            if identity.encoded_bytes != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
                || identity.sha256 != format!("{:x}", Sha256::digest(bytes))
                || identity.blake3 != blake3::hash(bytes).to_hex().as_str()
            {
                return Err(invalid(
                    "V36 checkpoint outbox dependency authority differs",
                ));
            }
        }
        let manifest: V36PrefixCheckpointManifest =
            serde_json::from_slice(&publication.manifest_bytes)
                .map_err(|_| invalid("V36 checkpoint outbox manifest JSON differs"))?;
        let pointer: V36PrefixCheckpointPointer =
            serde_json::from_slice(&publication.pointer_bytes)
                .map_err(|_| invalid("V36 checkpoint outbox pointer JSON differs"))?;
        if publication.manifest.encoded_bytes
            != u64::try_from(publication.manifest_bytes.len()).unwrap_or(u64::MAX)
            || publication.manifest.sha256
                != format!("{:x}", Sha256::digest(&publication.manifest_bytes))
            || publication.manifest.blake3
                != blake3::hash(&publication.manifest_bytes).to_hex().as_str()
            || manifest.generation != pointer.generation
            || pointer.manifest != publication.manifest
        {
            return Err(invalid(
                "V36 checkpoint outbox publication authority differs",
            ));
        }
        for (identity, bytes) in dependencies {
            install_content_addressed(
                &self
                    .root
                    .join("objects")
                    .join(format!("{}.blob", identity.sha256)),
                bytes,
            )?;
        }
        install_content_addressed(
            &self
                .root
                .join("manifests")
                .join(format!("{}.json", publication.manifest.sha256)),
            &publication.manifest_bytes,
        )?;
        let pointer_sha256 = format!("{:x}", Sha256::digest(&publication.pointer_bytes));
        install_content_addressed(
            &self
                .root
                .join("pointers")
                .join(format!("{pointer_sha256}.json")),
            &publication.pointer_bytes,
        )?;
        let ready = V36PrefixCheckpointReady {
            dependencies: &publication.dependencies,
            generation: manifest.generation,
            manifest: &publication.manifest,
            pointer_encoded_bytes: publication.pointer_bytes.len() as u64,
            pointer_sha256,
            pointer_uri: &publication.pointer_uri,
            previous_pointer_sha256: &publication.previous_pointer_sha256,
            schema: "borsuk-v36-prefix-checkpoint-outbox-v1",
        };
        let mut ready_bytes = serde_json::to_vec(&ready)
            .map_err(|_| invalid("V36 checkpoint outbox commit JSON differs"))?;
        ready_bytes.push(b'\n');
        let ready_path = self
            .root
            .join("commits")
            .join(format!("generation-{:08}.json", manifest.generation));
        install_content_addressed(&ready_path, &ready_bytes)?;
        Ok(ready_path)
    }
}

#[derive(Debug)]
/// Stateful producer for crash-atomic complete-object population checkpoints.
pub struct V36PrefixPopulationCheckpointWriter {
    context: V36PrefixCheckpointContext,
    dependencies: Vec<(V36ArtifactIdentity, Vec<u8>)>,
    execution_authority_sha256: String,
    outbox: V36PrefixCheckpointOutbox,
    previous_manifest: Option<V36PrefixCheckpointManifest>,
    previous_manifest_identity: Option<V36ArtifactIdentity>,
    previous_pointer_bytes: Option<Vec<u8>>,
    producer_attempt_id: String,
    producer_attempt_ordinal: u8,
    producer_instance_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Fully authenticated local material needed to continue one published head.
pub struct V36PrefixPopulationCheckpointHead {
    /// Cumulative identity-run artifacts and their exact local bytes.
    pub dependencies: Vec<(V36ArtifactIdentity, Vec<u8>)>,
    /// Newest immutable population manifest.
    pub manifest: V36PrefixCheckpointManifest,
    /// Exact canonical pointer bytes naming `manifest`.
    pub pointer_bytes: Vec<u8>,
}

impl V36PrefixPopulationCheckpointHead {
    fn identity_runs(&self) -> Result<Vec<V36PrefixIdentityRun>> {
        let selected_object_start = self.manifest.population.selected_object_start;
        self.dependencies
            .iter()
            .enumerate()
            .map(|(ordinal, (identity, bytes))| {
                let selected_object_ordinal = selected_object_start
                    .checked_add(
                        u16::try_from(ordinal)
                            .map_err(|_| invalid("V36 population checkpoint ordinal overflows"))?,
                    )
                    .ok_or_else(|| invalid("V36 population checkpoint ordinal overflows"))?;
                let source = self
                    .manifest
                    .population
                    .consumed_objects
                    .get(ordinal)
                    .ok_or_else(|| invalid("V36 population checkpoint resume state differs"))?;
                decode_v36_prefix_identity_run(bytes, identity, source, selected_object_ordinal)
            })
            .collect()
    }
}

/// Load one exact locally staged newest population head without history fallback.
pub fn load_v36_prefix_population_checkpoint_head(
    root: &Path,
    context: &V36PrefixCheckpointContext,
) -> Result<V36PrefixPopulationCheckpointHead> {
    let regular_bytes = |path: &Path| -> Result<Vec<u8>> {
        let metadata = fs::symlink_metadata(path).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        if !metadata.file_type().is_file() {
            return Err(invalid("V36 population checkpoint staged file differs"));
        }
        read_file(path)
    };
    if !root.is_dir() || root.is_symlink() || !root.join("objects").is_dir() {
        return Err(invalid("V36 population checkpoint staged root differs"));
    }
    let pointer_bytes = regular_bytes(&root.join("pointer.json"))?;
    let pointer: V36PrefixCheckpointPointer = serde_json::from_slice(&pointer_bytes)
        .map_err(|_| invalid("V36 population checkpoint pointer JSON differs"))?;
    if canonical_v36_prefix_checkpoint_pointer_bytes(context, &pointer)? != pointer_bytes {
        return Err(invalid("V36 population checkpoint pointer bytes differ"));
    }
    let manifest_bytes = regular_bytes(&root.join("manifest.json"))?;
    if pointer.manifest.encoded_bytes != manifest_bytes.len() as u64
        || pointer.manifest.sha256 != format!("{:x}", Sha256::digest(&manifest_bytes))
        || pointer.manifest.blake3 != blake3::hash(&manifest_bytes).to_hex().as_str()
    {
        return Err(invalid(
            "V36 population checkpoint manifest authority differs",
        ));
    }
    let manifest: V36PrefixCheckpointManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|_| invalid("V36 population checkpoint manifest JSON differs"))?;
    if canonical_v36_prefix_checkpoint_manifest_bytes(&manifest)? != manifest_bytes
        || manifest.generation != pointer.generation
        || !matches!(&manifest.phase, V36PrefixCheckpointPhase::Population)
    {
        return Err(invalid("V36 population checkpoint manifest bytes differ"));
    }
    validate_v36_prefix_checkpoint_manifest_with_context(context, &manifest)?;
    let mut dependencies = Vec::with_capacity(manifest.population.identity_runs.len());
    for identity in &manifest.population.identity_runs {
        if identity.encoded_bytes > 256 * 1024 * 1024 {
            return Err(invalid(
                "V36 population checkpoint dependency limit differs",
            ));
        }
        let path = root
            .join("objects")
            .join(format!("{}.blob", identity.sha256));
        authenticate_file(&path, identity)?;
        dependencies.push((identity.clone(), regular_bytes(&path)?));
    }
    Ok(V36PrefixPopulationCheckpointHead {
        dependencies,
        manifest,
        pointer_bytes,
    })
}

impl V36PrefixPopulationCheckpointWriter {
    /// Create an empty writer bound to one exact attempt and campaign authority.
    pub fn create(
        root: &Path,
        context: V36PrefixCheckpointContext,
        execution_authority_sha256: String,
        producer_attempt_id: String,
        producer_attempt_ordinal: u8,
        producer_instance_id: String,
    ) -> Result<Self> {
        digest_bytes(&execution_authority_sha256)?;
        if producer_attempt_id
            != format!("{}-attempt-{producer_attempt_ordinal:04}", context.run_id)
            || producer_attempt_ordinal >= 3
            || producer_instance_id.is_empty()
        {
            return Err(invalid("V36 population checkpoint producer differs"));
        }
        Ok(Self {
            context,
            dependencies: Vec::new(),
            execution_authority_sha256,
            outbox: V36PrefixCheckpointOutbox::create(root)?,
            previous_manifest: None,
            previous_manifest_identity: None,
            previous_pointer_bytes: None,
            producer_attempt_id,
            producer_attempt_ordinal,
            producer_instance_id,
        })
    }

    /// Resume from one fully authenticated published population head.
    pub fn resume(
        root: &Path,
        context: V36PrefixCheckpointContext,
        execution_authority_sha256: String,
        producer_attempt_id: String,
        producer_attempt_ordinal: u8,
        producer_instance_id: String,
        head: V36PrefixPopulationCheckpointHead,
    ) -> Result<Self> {
        let runs = head.identity_runs()?;
        let V36PrefixPopulationCheckpointHead {
            dependencies,
            manifest: previous_manifest,
            pointer_bytes: previous_pointer_bytes,
        } = head;
        digest_bytes(&execution_authority_sha256)?;
        validate_v36_prefix_checkpoint_manifest_with_context(&context, &previous_manifest)?;
        let pointer: V36PrefixCheckpointPointer =
            serde_json::from_slice(&previous_pointer_bytes)
                .map_err(|_| invalid("V36 population checkpoint pointer JSON differs"))?;
        if canonical_v36_prefix_checkpoint_pointer_bytes(&context, &pointer)?
            != previous_pointer_bytes
            || pointer.generation != previous_manifest.generation
            || pointer.run_id != previous_manifest.run_id
            || pointer.producer_attempt_id != previous_manifest.producer_attempt_id
            || pointer.producer_attempt_ordinal != previous_manifest.producer_attempt_ordinal
            || !matches!(
                &previous_manifest.phase,
                V36PrefixCheckpointPhase::Population
            )
            || dependencies.len() != previous_manifest.population.identity_runs.len()
            || dependencies
                .iter()
                .zip(&previous_manifest.population.identity_runs)
                .any(|((identity, bytes), expected)| {
                    identity != expected
                        || identity.encoded_bytes != bytes.len() as u64
                        || identity.sha256 != format!("{:x}", Sha256::digest(bytes))
                        || identity.blake3 != blake3::hash(bytes).to_hex().as_str()
                })
        {
            return Err(invalid(
                "V36 population checkpoint resume authority differs",
            ));
        }
        let restored = restore_v36_prefix_population_state(
            &runs,
            usize::try_from(context.distinct_candidates)
                .map_err(|_| invalid("V36 population checkpoint row count overflows"))?,
        )?;
        if restored.consumed_objects != previous_manifest.population.consumed_objects
            || restored.distinct_rows_observed != previous_manifest.population.distinct_rows
            || restored.duplicate_rows != previous_manifest.population.duplicate_rows
            || restored.next_object_ordinal
                != previous_manifest
                    .population
                    .selected_object_start
                    .checked_add(previous_manifest.population.completed_objects)
                    .ok_or_else(|| invalid("V36 population checkpoint ordinal overflows"))?
            || restored.physical_rows != previous_manifest.population.physical_rows
        {
            return Err(invalid("V36 population checkpoint resume state differs"));
        }
        let manifest_bytes = canonical_v36_prefix_checkpoint_manifest_bytes(&previous_manifest)?;
        if pointer.manifest.encoded_bytes != manifest_bytes.len() as u64
            || pointer.manifest.sha256 != format!("{:x}", Sha256::digest(&manifest_bytes))
            || pointer.manifest.blake3 != blake3::hash(&manifest_bytes).to_hex().as_str()
            || producer_attempt_id
                != format!("{}-attempt-{producer_attempt_ordinal:04}", context.run_id)
            || producer_attempt_ordinal >= 3
            || producer_attempt_ordinal < previous_manifest.producer_attempt_ordinal
            || producer_instance_id.is_empty()
        {
            return Err(invalid(
                "V36 population checkpoint resume authority differs",
            ));
        }
        Ok(Self {
            context,
            dependencies,
            execution_authority_sha256,
            outbox: V36PrefixCheckpointOutbox::create(root)?,
            previous_manifest: Some(previous_manifest),
            previous_manifest_identity: Some(pointer.manifest),
            previous_pointer_bytes: Some(previous_pointer_bytes),
            producer_attempt_id,
            producer_attempt_ordinal,
            producer_instance_id,
        })
    }

    /// Commit one complete authenticated source-object boundary.
    pub fn commit(&mut self, boundary: &V36PrefixPopulationCommit) -> Result<PathBuf> {
        let ordinal = self.dependencies.len();
        let previous_population = self
            .previous_manifest
            .as_ref()
            .map(|manifest| &manifest.population);
        let expected_global_ordinal = self
            .context
            .selected_object_start
            .checked_add(
                u16::try_from(ordinal)
                    .map_err(|_| invalid("V36 population checkpoint ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V36 population checkpoint ordinal overflows"))?;
        if boundary.run.selected_object_ordinal != expected_global_ordinal
            || self
                .context
                .ranked_objects
                .get(ordinal)
                .is_none_or(|registered| {
                    registered.path != boundary.run.source.path
                        || registered.uri != boundary.run.source.uri
                        || registered.sha256 != boundary.run.source.sha256
                        || registered.encoded_bytes != boundary.run.source.encoded_bytes
                })
        {
            return Err(invalid("V36 population checkpoint object differs"));
        }
        let bytes = encode_v36_prefix_identity_run(&boundary.run)?;
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        let identity = V36ArtifactIdentity {
            blake3: blake3::hash(&bytes).to_hex().to_string(),
            encoded_bytes: bytes.len().try_into().unwrap_or(u64::MAX),
            role: format!("population-identity-run-{expected_global_ordinal:04}"),
            sha256: sha256.clone(),
            uri: format!(
                "{}{sha256}-population-identity-run-{expected_global_ordinal:04}.arrow",
                self.context.object_prefix
            ),
        };
        let mut consumed_objects = self
            .previous_manifest
            .as_ref()
            .map_or_else(Vec::new, |manifest| {
                manifest.population.consumed_objects.clone()
            });
        consumed_objects.push(boundary.run.source.clone());
        let mut identity_runs = self
            .previous_manifest
            .as_ref()
            .map_or_else(Vec::new, |manifest| {
                manifest.population.identity_runs.clone()
            });
        identity_runs.push(identity.clone());
        let generation = self.previous_manifest.as_ref().map_or(Ok(0), |manifest| {
            manifest
                .generation
                .checked_add(1)
                .ok_or_else(|| invalid("V36 population checkpoint generation overflows"))
        })?;
        let previous_distinct =
            previous_population.map_or(0, |population| population.distinct_rows);
        let previous_physical =
            previous_population.map_or(0, |population| population.physical_rows);
        let distinct_rows = previous_distinct
            .checked_add(
                boundary
                    .run
                    .rows
                    .len()
                    .try_into()
                    .map_err(|_| invalid("V36 population checkpoint distinct rows overflow"))?,
            )
            .ok_or_else(|| invalid("V36 population checkpoint distinct rows overflow"))?;
        let physical_rows = previous_physical
            .checked_add(boundary.run.physical_rows)
            .ok_or_else(|| invalid("V36 population checkpoint physical rows overflow"))?;
        let duplicate_rows = physical_rows
            .checked_sub(distinct_rows)
            .ok_or_else(|| invalid("V36 population checkpoint duplicate rows underflow"))?;
        let cutoff = if previous_distinct < self.context.distinct_candidates
            && distinct_rows >= self.context.distinct_candidates
        {
            let local_index = self
                .context
                .distinct_candidates
                .checked_sub(previous_distinct)
                .and_then(|count| count.checked_sub(1))
                .and_then(|index| usize::try_from(index).ok())
                .ok_or_else(|| invalid("V36 population checkpoint cutoff overflows"))?;
            Some((
                boundary.run.selected_object_ordinal,
                boundary
                    .run
                    .rows
                    .get(local_index)
                    .ok_or_else(|| invalid("V36 population checkpoint cutoff differs"))?
                    .row_offset,
            ))
        } else {
            None
        };
        if boundary.distinct_rows != distinct_rows
            || boundary.duplicate_rows != duplicate_rows
            || boundary.physical_rows != physical_rows
            || boundary.cutoff != cutoff
        {
            return Err(invalid("V36 population checkpoint accounting differs"));
        }
        let manifest = V36PrefixCheckpointManifest {
            claim_eligible: false,
            execution_authority_sha256: self.execution_authority_sha256.clone(),
            freeze_authority_sha256: self.context.freeze_authority_sha256.clone(),
            generation,
            phase: V36PrefixCheckpointPhase::Population,
            population: V36PrefixPopulationCheckpoint {
                completed_objects: u16::try_from(identity_runs.len())
                    .map_err(|_| invalid("V36 population checkpoint ordinal overflows"))?,
                consumed_objects,
                distinct_rows,
                duplicate_rows,
                identity_runs,
                physical_rows,
                selected_object_count: self.context.selected_object_count,
                selected_object_start: self.context.selected_object_start,
            },
            previous_checkpoint: self.previous_manifest_identity.clone(),
            producer_attempt_id: self.producer_attempt_id.clone(),
            producer_attempt_ordinal: self.producer_attempt_ordinal,
            producer_instance_id: self.producer_instance_id.clone(),
            schema: "borsuk-v36-prefix-freeze-checkpoint-v2".to_owned(),
            run_id: self.context.run_id.clone(),
            source_archive_sha256: self.context.source_archive_sha256.clone(),
            source_commit: self.context.source_commit.clone(),
            source_registry_sha256: self.context.source_registry_sha256.clone(),
        };
        if let Some(previous) = &self.previous_manifest {
            validate_v36_prefix_checkpoint_transition(&self.context, previous, &manifest)?;
        }
        let publication = plan_v36_prefix_checkpoint_publication(
            &self.context,
            &manifest,
            self.previous_manifest.as_ref(),
            self.previous_pointer_bytes
                .as_deref()
                .map(|pointer| (pointer, "local-predecessor")),
        )?;
        self.dependencies.push((identity, bytes));
        let ready = match self.outbox.commit(&publication, &self.dependencies) {
            Ok(ready) => ready,
            Err(error) => {
                self.dependencies.pop();
                return Err(error);
            }
        };
        self.previous_manifest = Some(manifest);
        self.previous_manifest_identity = Some(publication.manifest.clone());
        self.previous_pointer_bytes = Some(publication.pointer_bytes);
        Ok(ready)
    }
}

fn parquet_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(PARQUET_ROW_GROUP_ROWS))
        .set_data_page_size_limit(1024 * 1024)
        .build()
}

fn validate_parquet_descriptor(actual: &SchemaDescriptor, expected: &Schema) -> Result<()> {
    let expected = ArrowSchemaConverter::new().convert(expected)?;
    // Parquet root-group names are writer metadata (`schema` in PyArrow and
    // `arrow_schema` in parquet-rs), not a column or logical-type authority.
    if actual.columns() != expected.columns()
        || actual.root_schema().get_fields().len() != expected.root_schema().get_fields().len()
    {
        return Err(invalid("V36 prefix Parquet physical schema differs"));
    }
    Ok(())
}

fn digest_bytes(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("V36 prefix digest differs"));
    }
    let mut decoded = [0_u8; 32];
    for (index, chunk) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let pair = std::str::from_utf8(chunk).map_err(|_| invalid("V36 prefix digest differs"))?;
        decoded[index] =
            u8::from_str_radix(pair, 16).map_err(|_| invalid("V36 prefix digest differs"))?;
    }
    if decoded.iter().all(|byte| *byte == 0) {
        return Err(invalid("V36 prefix digest differs"));
    }
    Ok(decoded)
}

fn digest_hex(digest: &[u8; 32]) -> String {
    digest
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            std::fmt::Write::write_fmt(&mut output, format_args!("{byte:02x}"))
                .expect("writing to a String cannot fail");
            output
        })
}

fn validate_embedding(embedding: &[f32]) -> Result<()> {
    if embedding.len() != DIMENSIONS
        || embedding.iter().any(|value| !value.is_finite())
        || embedding.iter().all(|value| *value == 0.0)
    {
        return Err(invalid("V36 prefix embedding differs"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
/// One physical input row before duplicate resolution and role selection.
pub struct V36PrefixInputRow {
    /// Signed physical feature ID; negative IDs are invalid.
    pub feature_row_id: i64,
    /// Position of the complete object in the registered sampled order.
    pub selected_object_ordinal: u16,
    /// Zero-based physical row offset inside that object.
    pub row_offset: u64,
    /// Exact source-domain f32 vector.
    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Compact identity retained after a streamed row passes vector validation.
pub struct V36PrefixRowIdentity {
    /// Unsigned logical feature ID.
    pub feature_row_id: u64,
    /// Corpus ordinal, absent until query removal and source ordering finish.
    pub source_ordinal: Option<u64>,
    /// Position of the complete object in the registered sampled order.
    pub selected_object_ordinal: u16,
    /// Zero-based physical row offset inside that object.
    pub row_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete-object first-occurrence evidence stored in one Arrow IPC identity run.
pub struct V36PrefixIdentityRun {
    /// Physical rows validated in the complete source object.
    pub physical_rows: u64,
    /// Global first occurrences contributed by this object in row-offset order.
    pub rows: Vec<V36PrefixRowIdentity>,
    /// Position of the complete object in registered sample order.
    pub selected_object_ordinal: u16,
    /// Exact complete source object authenticated before the run was committed.
    pub source: V36PrefixSourceObject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One authenticated local durable identity-run input for external selection.
pub struct V36PrefixIdentityRunFile {
    /// Registered immutable Arrow artifact identity.
    pub identity: V36ArtifactIdentity,
    /// Exact local regular-file path.
    pub path: PathBuf,
    /// Global object ordinal bound into the artifact.
    pub selected_object_ordinal: u16,
    /// Authenticated source object bound into the artifact.
    pub source: V36PrefixSourceObject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Hard resource limits for one attempt-local external population selection.
pub struct V36PrefixExternalSelectionLimits {
    /// Per-stream buffered I/O bytes.
    pub io_buffer_bytes: usize,
    /// Maximum encoded bytes accepted for any durable input artifact.
    pub max_input_bytes: u64,
    /// Maximum bytes created beneath the attempt scratch directory; excludes the final artifact.
    pub max_scratch_bytes: u64,
    /// Maximum private spills permitted in one merge generation.
    pub max_spills: usize,
    /// Maximum number of input spills opened by one merge.
    pub merge_fan_in: usize,
    /// Maximum scored records retained for one in-memory sort.
    pub sort_buffer_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Authenticated receipt for one file-backed selected-population artifact.
pub struct V36PrefixSelectedFileReceipt {
    /// Exact content-addressed selected-ID Arrow identity.
    pub identity: V36ArtifactIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One authenticated predecessor selected-ID file used for cohort exclusion.
pub struct V36PrefixSelectedIdsFile {
    /// Exact contract encoded by the predecessor artifact.
    pub contract: V36PrefixSelectedIdsContract,
    /// Registered immutable Arrow artifact identity.
    pub identity: V36ArtifactIdentity,
    /// Exact local regular-file path.
    pub path: PathBuf,
}

/// Complete file-backed request for one bounded external population selection.
pub struct V36PrefixExternalSelectionRequest<'a> {
    /// Selected-ID authority and derived-count contract.
    pub contract: &'a V36PrefixSelectedIdsContract,
    /// Optional authenticated predecessor selected-ID artifact.
    pub exclusion: Option<&'a V36PrefixSelectedIdsFile>,
    /// Hard resource limits for the attempt.
    pub limits: &'a V36PrefixExternalSelectionLimits,
    /// Final canonical selected-ID Arrow destination.
    pub output: &'a Path,
    /// S3 URI prefix used to form the content-addressed receipt.
    pub output_uri_prefix: &'a str,
    /// Complete authenticated identity-run window.
    pub runs: &'a [V36PrefixIdentityRunFile],
    /// Existing directory beneath which attempt-owned scratch is created.
    pub scratch_root: &'a Path,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Exact authority embedded in one immutable selected-population Arrow artifact.
pub struct V36PrefixSelectedIdsContract {
    /// Zero-based independently registered population cohort.
    pub cohort_ordinal: u8,
    /// Distinct rows eligible after applying the prior-cohort exclusion.
    pub eligible_rows: u64,
    /// Exact prior-cohort selected-ID artifact, absent only for cohort zero.
    pub excluded_population_identity: Option<V36ArtifactIdentity>,
    /// Distinct rows removed by the authenticated exclusion.
    pub excluded_rows: u64,
    /// SHA-256 of the complete ordered source manifest.
    pub ordered_source_manifest_sha256: String,
    /// SHA-256 population-row seed.
    pub population_seed_sha256: String,
    /// Number of complete source objects in the registered window.
    pub selected_object_count: u16,
    /// Global ordinal of the first source object in the window.
    pub selected_object_start: u16,
    /// Exact number of selected population rows.
    pub selected_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Authenticated selected population decoded from its immutable Arrow artifact.
pub struct V36PrefixSelectedIds {
    /// Feature-row ID at the inclusive population-score cutoff.
    pub cutoff_feature_row_id: u64,
    /// Population score at the inclusive cutoff.
    pub cutoff_score_sha256: String,
    /// Selected rows in strict `(score, unsigned feature ID)` order.
    pub rows: Vec<V36PrefixRowIdentity>,
}

fn selected_ids_metadata(
    contract: &V36PrefixSelectedIdsContract,
    cutoff_feature_row_id: u64,
    cutoff_score_sha256: &str,
) -> HashMap<String, String> {
    let mut metadata = HashMap::from([
        (
            "cohort_ordinal".to_owned(),
            contract.cohort_ordinal.to_string(),
        ),
        (
            "cutoff_feature_row_id".to_owned(),
            cutoff_feature_row_id.to_string(),
        ),
        (
            "cutoff_score_sha256".to_owned(),
            cutoff_score_sha256.to_owned(),
        ),
        (
            "eligible_rows".to_owned(),
            contract.eligible_rows.to_string(),
        ),
        (
            "excluded_rows".to_owned(),
            contract.excluded_rows.to_string(),
        ),
        ("format".to_owned(), SELECTED_IDS_FORMAT.to_owned()),
        (
            "ordered_source_manifest_sha256".to_owned(),
            contract.ordered_source_manifest_sha256.clone(),
        ),
        (
            "population_seed_sha256".to_owned(),
            contract.population_seed_sha256.clone(),
        ),
        (
            "score_algorithm".to_owned(),
            POPULATION_SCORE_ALGORITHM.to_owned(),
        ),
        (
            "selected_object_count".to_owned(),
            contract.selected_object_count.to_string(),
        ),
        (
            "selected_object_start".to_owned(),
            contract.selected_object_start.to_string(),
        ),
        (
            "selected_rows".to_owned(),
            contract.selected_rows.to_string(),
        ),
    ]);
    match &contract.excluded_population_identity {
        Some(identity) => {
            metadata.insert(
                "excluded_population_blake3".to_owned(),
                identity.blake3.clone(),
            );
            metadata.insert(
                "excluded_population_encoded_bytes".to_owned(),
                identity.encoded_bytes.to_string(),
            );
            metadata.insert("excluded_population_role".to_owned(), identity.role.clone());
            metadata.insert(
                "excluded_population_sha256".to_owned(),
                identity.sha256.clone(),
            );
            metadata.insert("excluded_population_uri".to_owned(), identity.uri.clone());
        }
        None => {
            for key in [
                "excluded_population_blake3",
                "excluded_population_encoded_bytes",
                "excluded_population_role",
                "excluded_population_sha256",
                "excluded_population_uri",
            ] {
                metadata.insert(key.to_owned(), "none".to_owned());
            }
        }
    }
    metadata
}

fn selected_ids_schema(
    contract: &V36PrefixSelectedIdsContract,
    cutoff_feature_row_id: u64,
    cutoff_score_sha256: &str,
) -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("feature_row_id", DataType::UInt64, false),
            Field::new("population_score", DataType::FixedSizeBinary(32), false),
            Field::new("selected_object_ordinal", DataType::UInt16, false),
            Field::new("row_offset", DataType::UInt64, false),
        ],
        selected_ids_metadata(contract, cutoff_feature_row_id, cutoff_score_sha256),
    )
}

fn validate_selected_ids_contract(
    contract: &V36PrefixSelectedIdsContract,
) -> Result<([u8; 32], [u8; 32])> {
    let seed = digest_bytes(&contract.population_seed_sha256)?;
    let manifest = digest_bytes(&contract.ordered_source_manifest_sha256)?;
    let window_end = contract
        .selected_object_start
        .checked_add(contract.selected_object_count)
        .ok_or_else(|| invalid("V36 prefix selected-ID object window overflows"))?;
    if contract.population_seed_sha256 != POPULATION_SEED_SHA256
        || contract.selected_object_count == 0
        || contract.selected_rows == 0
        || contract.selected_rows > contract.eligible_rows
        || (contract.cohort_ordinal == 0
            && (contract.excluded_rows != 0 || contract.excluded_population_identity.is_some()))
        || (contract.cohort_ordinal != 0 && contract.excluded_population_identity.is_none())
        || contract
            .excluded_population_identity
            .as_ref()
            .is_some_and(|identity| {
                identity.encoded_bytes == 0
                    || identity.role != "population-selected-identities"
                    || digest_bytes(&identity.sha256).is_err()
                    || digest_bytes(&identity.blake3).is_err()
                    || identity.uri.is_empty()
            })
        || window_end <= contract.selected_object_start
    {
        return Err(invalid("V36 prefix selected-ID contract differs"));
    }
    Ok((seed, manifest))
}

fn validate_selected_id_rows(
    contract: &V36PrefixSelectedIdsContract,
    rows: &[V36PrefixRowIdentity],
) -> Result<()> {
    let (seed, manifest) = validate_selected_ids_contract(contract)?;
    if rows.len() as u64 != contract.selected_rows {
        return Err(invalid("V36 prefix selected-ID row count differs"));
    }
    let window_end = contract.selected_object_start + contract.selected_object_count;
    let mut physical = HashSet::with_capacity(rows.len());
    let mut previous = None;
    for row in rows {
        let score = score(&seed, &manifest, row.feature_row_id);
        let rank = (score, row.feature_row_id);
        if row.source_ordinal.is_some()
            || row.selected_object_ordinal < contract.selected_object_start
            || row.selected_object_ordinal >= window_end
            || !physical.insert((row.selected_object_ordinal, row.row_offset))
            || previous.is_some_and(|prior| prior >= rank)
        {
            return Err(invalid("V36 prefix selected-ID rows differ"));
        }
        previous = Some(rank);
    }
    Ok(())
}

/// Encode one strict, deterministic selected-population Arrow IPC artifact.
pub fn encode_v36_prefix_selected_ids(
    contract: &V36PrefixSelectedIdsContract,
    rows: &[V36PrefixRowIdentity],
) -> Result<Vec<u8>> {
    validate_selected_id_rows(contract, rows)?;
    let cutoff = rows
        .last()
        .ok_or_else(|| invalid("V36 prefix selected-ID cutoff is missing"))?;
    let (seed, manifest) = validate_selected_ids_contract(contract)?;
    let cutoff_score = score(&seed, &manifest, cutoff.feature_row_id);
    let schema = Arc::new(selected_ids_schema(
        contract,
        cutoff.feature_row_id,
        &digest_hex(&cutoff_score),
    ));
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    let mut writer = ArrowFileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
    for rows in rows.chunks(SELECTED_IDS_BATCH_ROWS) {
        let scores = rows
            .iter()
            .map(|row| score(&seed, &manifest, row.feature_row_id))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(
                    rows.iter()
                        .map(|row| row.feature_row_id)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(FixedSizeBinaryArray::try_from_iter(
                    scores.iter().map(<[u8; 32]>::as_slice),
                )?),
                Arc::new(UInt16Array::from(
                    rows.iter()
                        .map(|row| row.selected_object_ordinal)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|row| row.row_offset).collect::<Vec<_>>(),
                )),
            ],
        )?;
        writer.write(&batch)?;
    }
    writer.finish()?;
    drop(writer);
    Ok(bytes)
}

/// Authenticate and decode one immutable selected-population Arrow IPC artifact.
pub fn decode_v36_prefix_selected_ids(
    bytes: &[u8],
    registered: &V36ArtifactIdentity,
    contract: &V36PrefixSelectedIdsContract,
) -> Result<V36PrefixSelectedIds> {
    validate_selected_ids_contract(contract)?;
    let sha256 = format!("{:x}", Sha256::digest(bytes));
    let blake3 = blake3::hash(bytes).to_hex().to_string();
    let content_addressed = url::Url::parse(&registered.uri)
        .ok()
        .filter(|uri| uri.scheme() == "s3" && uri.host_str().is_some())
        .and_then(|uri| uri.path().rsplit('/').next().map(str::to_owned))
        .is_some_and(|name| name.starts_with(&format!("{sha256}-")));
    if registered.role != "population-selected-identities"
        || registered.encoded_bytes != bytes.len() as u64
        || registered.sha256 != sha256
        || registered.blake3 != blake3
        || !content_addressed
    {
        return Err(invalid("V36 prefix selected-ID artifact differs"));
    }
    let mut reader = ArrowFileReader::try_new(std::io::Cursor::new(bytes), None)?;
    let selected_rows = usize::try_from(contract.selected_rows)
        .map_err(|_| invalid("V36 prefix selected-ID row count overflows"))?;
    let expected_batches = selected_rows.div_ceil(SELECTED_IDS_BATCH_ROWS);
    if reader.num_batches() != expected_batches {
        return Err(invalid("V36 prefix selected-ID batches differ"));
    }
    let schema = reader.schema();
    let cutoff_feature_row_id = schema
        .metadata()
        .get("cutoff_feature_row_id")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| invalid("V36 prefix selected-ID schema differs"))?;
    let cutoff_score_sha256 = schema
        .metadata()
        .get("cutoff_score_sha256")
        .ok_or_else(|| invalid("V36 prefix selected-ID schema differs"))?;
    digest_bytes(cutoff_score_sha256)?;
    if schema.as_ref() != &selected_ids_schema(contract, cutoff_feature_row_id, cutoff_score_sha256)
    {
        return Err(invalid("V36 prefix selected-ID schema differs"));
    }
    let (seed, manifest) = validate_selected_ids_contract(contract)?;
    let mut rows = Vec::with_capacity(selected_rows);
    for batch_ordinal in 0..expected_batches {
        let batch = reader
            .next()
            .transpose()?
            .ok_or_else(|| invalid("V36 prefix selected-ID batch is missing"))?;
        let remaining = selected_rows - rows.len();
        let expected_rows = remaining.min(SELECTED_IDS_BATCH_ROWS);
        if batch.num_rows() != expected_rows || batch.num_columns() != 4 {
            return Err(invalid("V36 prefix selected-ID batches differ"));
        }
        let feature_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V36 prefix selected-ID feature IDs differ"))?;
        let encoded_scores = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| invalid("V36 prefix selected-ID scores differ"))?;
        let object_ordinals = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| invalid("V36 prefix selected-ID object ordinals differ"))?;
        let row_offsets = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V36 prefix selected-ID row offsets differ"))?;
        for index in 0..batch.num_rows() {
            let feature_row_id = feature_ids.value(index);
            let expected_score = score(&seed, &manifest, feature_row_id);
            if encoded_scores.value(index) != expected_score {
                return Err(invalid("V36 prefix selected-ID scores differ"));
            }
            rows.push(V36PrefixRowIdentity {
                feature_row_id,
                source_ordinal: None,
                selected_object_ordinal: object_ordinals.value(index),
                row_offset: row_offsets.value(index),
            });
        }
        if batch_ordinal + 1 < expected_batches && batch.num_rows() != SELECTED_IDS_BATCH_ROWS {
            return Err(invalid("V36 prefix selected-ID batches differ"));
        }
    }
    if reader.next().is_some() || rows.len() != selected_rows {
        return Err(invalid("V36 prefix selected-ID row count differs"));
    }
    validate_selected_id_rows(contract, &rows)?;
    let cutoff = rows
        .last()
        .ok_or_else(|| invalid("V36 prefix selected-ID cutoff is missing"))?;
    let derived_cutoff_score_sha256 = digest_hex(&score(&seed, &manifest, cutoff.feature_row_id));
    if cutoff.feature_row_id != cutoff_feature_row_id
        || derived_cutoff_score_sha256 != *cutoff_score_sha256
    {
        return Err(invalid("V36 prefix selected-ID cutoff differs"));
    }
    Ok(V36PrefixSelectedIds {
        cutoff_feature_row_id: cutoff.feature_row_id,
        cutoff_score_sha256: derived_cutoff_score_sha256,
        rows,
    })
}

fn v36_prefix_identity_run_schema(run: &V36PrefixIdentityRun, row_count: usize) -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("feature_row_id", DataType::UInt64, false),
            Field::new("row_offset", DataType::UInt64, false),
        ],
        HashMap::from([
            ("format".to_owned(), IDENTITY_RUN_FORMAT.to_owned()),
            (
                "selected_object_ordinal".to_owned(),
                run.selected_object_ordinal.to_string(),
            ),
            ("physical_rows".to_owned(), run.physical_rows.to_string()),
            ("rows".to_owned(), row_count.to_string()),
            ("source_blake3".to_owned(), run.source.blake3.clone()),
            (
                "source_encoded_bytes".to_owned(),
                run.source.encoded_bytes.to_string(),
            ),
            ("source_path".to_owned(), run.source.path.clone()),
            (
                "source_sample_sha256".to_owned(),
                run.source.sample_sha256.clone(),
            ),
            ("source_sha256".to_owned(), run.source.sha256.clone()),
            ("source_uri".to_owned(), run.source.uri.clone()),
        ]),
    )
}

fn v36_prefix_object_sample_sha256(path: &str, encoded_bytes: u64) -> String {
    let mut sample = Sha256::new();
    sample.update(b"borsuk-v36-screen-object-v1");
    sample.update(path.as_bytes());
    sample.update(encoded_bytes.to_le_bytes());
    format!("{:x}", sample.finalize())
}

fn validate_v36_prefix_identity_run(run: &V36PrefixIdentityRun) -> Result<()> {
    let mut feature_ids = HashSet::with_capacity(run.rows.len());
    if run.physical_rows == 0
        || run.rows.len() as u64 > run.physical_rows
        || run.source.path.is_empty()
        || run.source.uri.is_empty()
        || run.source.encoded_bytes == 0
        || digest_bytes(&run.source.sha256).is_err()
        || digest_bytes(&run.source.blake3).is_err()
        || digest_bytes(&run.source.sample_sha256).is_err()
        || run.source.sample_sha256
            != v36_prefix_object_sample_sha256(&run.source.path, run.source.encoded_bytes)
        || run.rows.iter().any(|row| {
            row.selected_object_ordinal != run.selected_object_ordinal
                || row.source_ordinal.is_some()
                || row.row_offset >= run.physical_rows
                || !feature_ids.insert(row.feature_row_id)
        })
        || run
            .rows
            .windows(2)
            .any(|pair| pair[0].row_offset >= pair[1].row_offset)
    {
        return Err(invalid("V36 prefix identity-run rows differ"));
    }
    Ok(())
}

/// Encode the first-occurrence identities contributed by one complete source object.
pub fn encode_v36_prefix_identity_run(run: &V36PrefixIdentityRun) -> Result<Vec<u8>> {
    validate_v36_prefix_identity_run(run)?;
    let schema = Arc::new(v36_prefix_identity_run_schema(run, run.rows.len()));
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    let mut writer = ArrowFileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
    for rows in run.rows.chunks(IDENTITY_RUN_BATCH_ROWS) {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(
                    rows.iter()
                        .map(|row| row.feature_row_id)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|row| row.row_offset).collect::<Vec<_>>(),
                )),
            ],
        )?;
        writer.write(&batch)?;
    }
    writer.finish()?;
    drop(writer);
    Ok(bytes)
}

/// Authenticate and decode one complete immutable source-object identity run.
pub fn decode_v36_prefix_identity_run(
    bytes: &[u8],
    registered: &V36ArtifactIdentity,
    source: &V36PrefixSourceObject,
    selected_object_ordinal: u16,
) -> Result<V36PrefixIdentityRun> {
    let expected_role = format!("population-identity-run-{selected_object_ordinal:04}");
    let sha256 = format!("{:x}", Sha256::digest(bytes));
    let blake3 = blake3::hash(bytes).to_hex().to_string();
    let content_addressed = url::Url::parse(&registered.uri)
        .ok()
        .filter(|uri| uri.scheme() == "s3" && uri.host_str().is_some())
        .and_then(|uri| uri.path().rsplit('/').next().map(str::to_owned))
        .is_some_and(|name| name.starts_with(&format!("{sha256}-")));
    if registered.role != expected_role
        || registered.encoded_bytes != bytes.len() as u64
        || registered.sha256 != sha256
        || registered.blake3 != blake3
        || !content_addressed
    {
        return Err(invalid("V36 prefix identity-run artifact differs"));
    }

    let mut reader = ArrowFileReader::try_new(std::io::Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let row_count = schema
        .metadata()
        .get("rows")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| invalid("V36 prefix identity-run schema differs"))?;
    let physical_rows = schema
        .metadata()
        .get("physical_rows")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| invalid("V36 prefix identity-run schema differs"))?;
    if row_count > physical_rows {
        return Err(invalid("V36 prefix identity-run row count differs"));
    }
    let row_count = usize::try_from(row_count)
        .map_err(|_| invalid("V36 prefix identity-run row count differs"))?;
    let expected_run = V36PrefixIdentityRun {
        physical_rows,
        rows: Vec::new(),
        selected_object_ordinal,
        source: source.clone(),
    };
    let expected_batches = row_count.div_ceil(IDENTITY_RUN_BATCH_ROWS);
    if schema.as_ref() != &v36_prefix_identity_run_schema(&expected_run, row_count)
        || reader.num_batches() != expected_batches
    {
        return Err(invalid("V36 prefix identity-run schema differs"));
    }
    let mut rows = Vec::with_capacity(row_count);
    for batch_index in 0..expected_batches {
        let batch = reader
            .next()
            .transpose()?
            .ok_or_else(|| invalid("V36 prefix identity-run batch is missing"))?;
        let expected_rows =
            (row_count - batch_index * IDENTITY_RUN_BATCH_ROWS).min(IDENTITY_RUN_BATCH_ROWS);
        if batch.num_rows() != expected_rows
            || batch.num_columns() != 2
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V36 prefix identity-run batches differ"));
        }
        let feature_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V36 prefix identity-run feature IDs differ"))?;
        let row_offsets = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V36 prefix identity-run row offsets differ"))?;
        rows.extend(feature_ids.values().iter().zip(row_offsets.values()).map(
            |(&feature_row_id, &row_offset)| V36PrefixRowIdentity {
                feature_row_id,
                source_ordinal: None,
                selected_object_ordinal,
                row_offset,
            },
        ));
    }
    if reader.next().is_some() || rows.len() != row_count {
        return Err(invalid("V36 prefix identity-run batches differ"));
    }
    let run = V36PrefixIdentityRun {
        physical_rows,
        rows,
        selected_object_ordinal,
        source: source.clone(),
    };
    validate_v36_prefix_identity_run(&run)?;
    Ok(run)
}

/// Reconstruct the exact completed population prefix from authenticated identity runs.
pub fn restore_v36_prefix_population(
    runs: &[V36PrefixIdentityRun],
    distinct_candidates: usize,
) -> Result<V36PrefixObjectPrefixScan> {
    let restored = restore_v36_prefix_population_state(runs, distinct_candidates)?;
    let (cutoff_object_ordinal, cutoff_row_offset) = restored
        .cutoff
        .ok_or(BorsukError::V36PrefixSourceInsufficient)?;
    Ok(V36PrefixObjectPrefixScan {
        consumed_objects: restored.consumed_objects,
        cutoff_object_ordinal,
        cutoff_row_offset,
        distinct_rows_observed: restored.distinct_rows_observed,
        duplicate_rows: restored.duplicate_rows,
        physical_rows: restored.physical_rows,
        unique_rows: restored.unique_rows,
    })
}

/// Reconstruct resumable population state at a complete source-object boundary.
pub fn restore_v36_prefix_population_state(
    runs: &[V36PrefixIdentityRun],
    distinct_candidates: usize,
) -> Result<V36PrefixRestoredPopulation> {
    if runs.is_empty() || runs.len() > 16 || distinct_candidates == 0 {
        return Err(invalid("V36 prefix identity-run replay limits differ"));
    }
    let mut source_paths = BTreeSet::new();
    let mut feature_ids = HashSet::with_capacity(distinct_candidates);
    let mut consumed_objects = Vec::with_capacity(runs.len());
    let mut unique_rows = Vec::with_capacity(distinct_candidates);
    let mut physical_rows = 0_u64;
    let mut cutoff = None;
    let selected_object_start = runs[0].selected_object_ordinal;
    for (ordinal, run) in runs.iter().enumerate() {
        validate_v36_prefix_identity_run(run)?;
        let expected_ordinal = selected_object_start
            .checked_add(
                u16::try_from(ordinal)
                    .map_err(|_| invalid("V36 prefix identity-run ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V36 prefix identity-run ordinal overflows"))?;
        if run.selected_object_ordinal != expected_ordinal
            || !source_paths.insert(run.source.path.as_str())
        {
            return Err(invalid("V36 prefix identity-run sequence differs"));
        }
        physical_rows = physical_rows
            .checked_add(run.physical_rows)
            .ok_or_else(|| invalid("V36 prefix identity-run physical rows overflow"))?;
        for row in &run.rows {
            if !feature_ids.insert(row.feature_row_id) {
                return Err(invalid("V36 prefix identity-run global ID repeats"));
            }
            unique_rows.push(row.clone());
            if unique_rows.len() == distinct_candidates {
                cutoff = Some((run.selected_object_ordinal, row.row_offset));
            }
        }
        consumed_objects.push(run.source.clone());
    }
    let distinct_rows_observed = u64::try_from(feature_ids.len()).unwrap_or(u64::MAX);
    let duplicate_rows = physical_rows
        .checked_sub(distinct_rows_observed)
        .ok_or_else(|| invalid("V36 prefix identity-run duplicate rows underflow"))?;
    Ok(V36PrefixRestoredPopulation {
        consumed_objects,
        cutoff,
        distinct_rows_observed,
        duplicate_rows,
        next_object_ordinal: selected_object_start
            .checked_add(
                runs.len()
                    .try_into()
                    .map_err(|_| invalid("V36 prefix identity-run ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V36 prefix identity-run ordinal overflows"))?,
        physical_rows,
        unique_rows,
    })
}

#[derive(Debug, Clone, PartialEq)]
/// One valid unique row after materialization ordering.
pub struct V36PrefixMaterializedRow {
    /// Unsigned logical feature ID.
    pub feature_row_id: u64,
    /// Corpus ordinal, absent for a query row.
    pub source_ordinal: Option<u64>,
    /// Exact source-domain f32 vector.
    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
/// One query row with its role-local ordinal.
pub struct V36PrefixQueryRow {
    /// Zero-based ordinal within a query role.
    pub query_ordinal: u32,
    /// Unsigned logical feature ID.
    pub feature_row_id: u64,
    /// Exact source-domain f32 vector.
    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
/// Disjoint role selection plus the query-excluded corpus.
pub struct V36PrefixRoleSplit {
    /// Development queries.
    pub development: Vec<V36PrefixRowIdentity>,
    /// Validation queries.
    pub validation: Vec<V36PrefixRowIdentity>,
    /// Once-opened sealed holdout queries.
    pub sealed_holdout: Vec<V36PrefixRowIdentity>,
    /// Timing-only queries with no exact-GT obligation.
    pub performance: Vec<V36PrefixRowIdentity>,
    /// Remaining rows ordered by the registered source score.
    pub corpus: Vec<V36PrefixRowIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One registered source object with its query-independent sample score.
pub struct V36PrefixRankedSourceObject {
    /// Complete encoded length.
    pub encoded_bytes: u64,
    /// Registered path.
    pub path: String,
    /// Query-independent sample digest.
    pub sample_sha256: String,
    /// Complete-object SHA-256.
    pub sha256: String,
    /// Immutable object URI.
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Local immutable inputs for one bounded V36 prefix-freeze attempt.
pub struct V36PrefixFreezeRequest {
    /// Pre-freeze scientific authority path.
    pub authority: PathBuf,
    /// Empty private directory used for crash-atomic checkpoint handoff.
    pub checkpoint_outbox: PathBuf,
    /// Executable whose exact bytes are bound by the attempt authority.
    pub executable: PathBuf,
    /// Attempt lifecycle and provenance authority path.
    pub execution_authority: PathBuf,
    /// Empty output directory owned by this attempt.
    pub output: PathBuf,
    /// Exact EC2 instance producing this attempt's checkpoints.
    pub producer_instance_id: String,
    /// Exact locally staged newest population head, absent for a fresh attempt.
    pub resume_checkpoint: Option<PathBuf>,
    /// Empty encrypted scratch directory owned by this attempt.
    pub scratch: PathBuf,
    /// Exact source-code archive evidence path.
    pub source_archive: PathBuf,
    /// Complete registered source-object list path.
    pub source_registry: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Authenticated local state produced before any source-object network access.
pub struct V36PrefixFreezePreflight {
    /// Validated pre-freeze scientific authority.
    pub authority: V36PrefixFreezeAuthority,
    /// Trusted campaign authority used for every population checkpoint.
    pub checkpoint_context: V36PrefixCheckpointContext,
    /// Validated lifecycle and provenance authority.
    pub execution_authority: V36PrefixFreezeExecutionAuthority,
    /// Zero-based attempt ordinal parsed from the exact attempt identity.
    pub producer_attempt_ordinal: u8,
    /// Complete source registry in its canonical encoded order.
    pub registry: Vec<V36PrefixRegisteredSourceObject>,
    /// Query-independently ranked complete objects.
    pub ranked_objects: Vec<V36PrefixRankedSourceObject>,
    /// Fully authenticated prior population head, absent for a fresh attempt.
    pub resume_head: Option<V36PrefixPopulationCheckpointHead>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete bounded source-window scan evidence before role selection.
pub struct V36PrefixObjectPrefixScan {
    /// Every authenticated object in the registered window.
    pub consumed_objects: Vec<crate::V36PrefixSourceObject>,
    /// Selected-object ordinal containing the target distinct row.
    pub cutoff_object_ordinal: u16,
    /// Physical row offset of the target distinct row.
    pub cutoff_row_offset: u64,
    /// Distinct IDs observed across every complete consumed object.
    pub distinct_rows_observed: u64,
    /// Physical rows whose ID repeated an earlier occurrence.
    pub duplicate_rows: u64,
    /// Physical rows scanned across every complete consumed object.
    pub physical_rows: u64,
    /// Every first-occurrence identity in the registered window.
    pub unique_rows: Vec<V36PrefixRowIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Authenticated population progress at any complete source-object boundary.
pub struct V36PrefixRestoredPopulation {
    /// Complete source objects incorporated in ranked order.
    pub consumed_objects: Vec<crate::V36PrefixSourceObject>,
    /// Registered cutoff, absent while more complete objects are required.
    pub cutoff: Option<(u16, u64)>,
    /// Distinct IDs observed through the complete object prefix.
    pub distinct_rows_observed: u64,
    /// Duplicate physical rows observed through the complete object prefix.
    pub duplicate_rows: u64,
    /// First registered object ordinal not yet incorporated.
    pub next_object_ordinal: u16,
    /// Physical rows observed through the complete object prefix.
    pub physical_rows: u64,
    /// Every first occurrence retained through the completed object prefix.
    pub unique_rows: Vec<V36PrefixRowIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One durable population boundary emitted after a complete authenticated object.
pub struct V36PrefixPopulationCommit {
    /// Cutoff position once the requested distinct prefix has been reached.
    pub cutoff: Option<(u16, u64)>,
    /// Distinct IDs observed through this complete object.
    pub distinct_rows: u64,
    /// Duplicate physical rows observed through this complete object.
    pub duplicate_rows: u64,
    /// Physical rows observed through this complete object.
    pub physical_rows: u64,
    /// Complete-object first-occurrence evidence.
    pub run: V36PrefixIdentityRun,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Strict role-separated Parquet outputs from one prefix population.
pub struct V36PrefixRoleParquetPaths {
    /// Development queries.
    pub development: PathBuf,
    /// Performance queries.
    pub performance: PathBuf,
    /// Sealed-holdout queries.
    pub sealed_holdout: PathBuf,
    /// Canonically ordered corpus.
    pub source: PathBuf,
    /// Validation queries.
    pub validation: PathBuf,
}

#[derive(Debug, Clone, PartialEq)]
/// One exact GT@100 row.
pub struct V36PrefixGtNeighbor {
    /// Query ordinal.
    pub query_ordinal: u32,
    /// Zero-based neighbor rank.
    pub rank: u16,
    /// Unsigned logical feature ID.
    pub feature_row_id: u64,
    /// Exact binary64 squared-L2 distance.
    pub squared_distance: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// One query role authorized to receive exact GT@100.
pub enum V36PrefixQualityRole {
    /// Development queries.
    Development,
    /// Validation queries.
    Validation,
    /// Once-opened sealed holdout queries.
    SealedHoldout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One quality-query Parquet input and exact-GT output role.
pub struct V36PrefixGtParquetJob {
    /// Exact number of queries in this role.
    pub expected_queries: u32,
    /// Destination exact-GT Parquet path.
    pub output: PathBuf,
    /// Source query Parquet path.
    pub query: PathBuf,
    /// Closed quality role.
    pub role: V36PrefixQualityRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Bounded work evidence from one corpus-outer exact-GT pass.
pub struct V36PrefixGtRunStats {
    /// Quality queries updated for every source row.
    pub quality_queries: u32,
    /// Complete source scans performed.
    pub source_scans: u32,
    /// Source rows processed in canonical order.
    pub source_rows: u64,
}

/// Rank a complete authenticated registry by the frozen sample rule.
pub fn rank_v36_prefix_source_objects(
    authority: &V36PrefixFreezeAuthority,
    registry: &[V36PrefixRegisteredSourceObject],
) -> Result<Vec<V36PrefixRankedSourceObject>> {
    validate_v36_prefix_freeze_authority(authority, registry)?;
    let mut paths = BTreeSet::new();
    let mut uris = BTreeSet::new();
    let mut ranked = registry
        .iter()
        .map(|object| {
            if object.encoded_bytes == 0
                || object.path.is_empty()
                || !paths.insert(object.path.as_str())
                || !uris.insert(object.uri.as_str())
            {
                return Err(invalid("V36 prefix source registry differs"));
            }
            digest_bytes(&object.sha256)?;
            let mut hasher = Sha256::new();
            hasher.update(b"borsuk-v36-screen-object-v1");
            hasher.update(object.path.as_bytes());
            hasher.update(object.encoded_bytes.to_le_bytes());
            Ok(V36PrefixRankedSourceObject {
                encoded_bytes: object.encoded_bytes,
                path: object.path.clone(),
                sample_sha256: format!("{:x}", hasher.finalize()),
                sha256: object.sha256.clone(),
                uri: object.uri.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_by(|left, right| {
        (&left.sample_sha256, &left.path).cmp(&(&right.sample_sha256, &right.path))
    });
    Ok(ranked)
}

/// Validate one streamed physical row and retain only its compact identity.
pub fn validate_v36_prefix_input_row(row: &V36PrefixInputRow) -> Result<V36PrefixRowIdentity> {
    if row.feature_row_id < 0 {
        return Err(invalid("V36 prefix feature ID differs"));
    }
    validate_embedding(&row.embedding)?;
    Ok(V36PrefixRowIdentity {
        feature_row_id: u64::try_from(row.feature_row_id)
            .map_err(|_| invalid("V36 prefix feature ID differs"))?,
        source_ordinal: None,
        selected_object_ordinal: row.selected_object_ordinal,
        row_offset: row.row_offset,
    })
}

/// Keep the first sampled-object occurrence per ID without retaining vectors.
pub fn deduplicate_v36_prefix_row_identities(
    mut rows: Vec<V36PrefixRowIdentity>,
) -> Result<Vec<V36PrefixRowIdentity>> {
    rows.sort_by_key(|row| (row.selected_object_ordinal, row.row_offset));
    let mut physical = BTreeSet::new();
    let mut feature_ids = BTreeSet::new();
    let mut unique = Vec::new();
    for row in rows {
        if row.source_ordinal.is_some()
            || !physical.insert((row.selected_object_ordinal, row.row_offset))
        {
            return Err(invalid("V36 prefix physical row differs"));
        }
        if feature_ids.insert(row.feature_row_id) {
            unique.push(row);
        }
    }
    Ok(unique)
}

/// Select the lowest v2 population-row hashes after physical-first deduplication.
pub fn select_v36_prefix_population_rows(
    rows: Vec<V36PrefixRowIdentity>,
    ordered_source_manifest_sha256: &str,
    count: usize,
) -> Result<Vec<V36PrefixRowIdentity>> {
    if count == 0 {
        return Err(invalid("V36 prefix population row count differs"));
    }
    let source_identity = digest_bytes(ordered_source_manifest_sha256)?;
    let seed: [u8; 32] = Sha256::digest(b"borsuk-v36-prefix-screen-population-row-v2").into();
    let mut ranked = deduplicate_v36_prefix_row_identities(rows)?
        .into_iter()
        .map(|row| (score(&seed, &source_identity, row.feature_row_id), row))
        .collect::<Vec<_>>();
    if ranked.len() < count {
        return Err(invalid("V36 prefix population is insufficient"));
    }
    ranked.sort_by(|left, right| {
        (&left.0, left.1.feature_row_id).cmp(&(&right.0, right.1.feature_row_id))
    });
    ranked.truncate(count);
    Ok(ranked.into_iter().map(|(_, row)| row).collect())
}

const EXTERNAL_SPILL_MAGIC: [u8; 8] = *b"V36SCR02";
const EXTERNAL_SPILL_HEADER_BYTES: u64 = 16;
const EXTERNAL_SPILL_DIGEST_BYTES: u64 = 32;
const EXTERNAL_SPILL_RECORD_BYTES: u64 = 50;
const EXTERNAL_MAX_FAN_IN: usize = 16;
const EXTERNAL_MAX_IO_BUFFER_BYTES: usize = 65_536;
const EXTERNAL_MAX_SORT_BUFFER_RECORDS: usize = 65_536;
const EXTERNAL_MAX_SPILLS: usize = 65_536;
const EXTERNAL_MAX_ARROW_BATCHES: usize = 65_536;
const EXTERNAL_MAX_ARROW_FOOTER_BYTES: usize = 1 << 20;

fn v36_prefix_external_io<T>(path: &Path, result: std::io::Result<T>) -> Result<T> {
    result.map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct V36PrefixScoredIdentity {
    score: [u8; 32],
    feature_row_id: u64,
    selected_object_ordinal: u16,
    row_offset: u64,
}

fn write_v36_prefix_spill_header(writer: &mut impl Write, records: u64) -> Result<()> {
    v36_prefix_external_io(
        Path::new("V36 external spill"),
        writer.write_all(&EXTERNAL_SPILL_MAGIC),
    )?;
    v36_prefix_external_io(
        Path::new("V36 external spill"),
        writer.write_all(&records.to_le_bytes()),
    )?;
    Ok(())
}

fn write_v36_prefix_scored_identity(
    writer: &mut impl Write,
    hasher: &mut blake3::Hasher,
    record: &V36PrefixScoredIdentity,
) -> Result<()> {
    let path = Path::new("V36 external spill");
    let mut bytes = [0_u8; EXTERNAL_SPILL_RECORD_BYTES as usize];
    bytes[..32].copy_from_slice(&record.score);
    bytes[32..40].copy_from_slice(&record.feature_row_id.to_le_bytes());
    bytes[40..42].copy_from_slice(&record.selected_object_ordinal.to_le_bytes());
    bytes[42..50].copy_from_slice(&record.row_offset.to_le_bytes());
    v36_prefix_external_io(path, writer.write_all(&bytes))?;
    hasher.update(&bytes);
    Ok(())
}

fn finish_v36_prefix_spill(
    path: &Path,
    writer: &mut BufWriter<File>,
    hasher: &blake3::Hasher,
) -> Result<()> {
    v36_prefix_external_io(path, writer.write_all(hasher.finalize().as_bytes()))?;
    v36_prefix_external_io(path, writer.flush())?;
    v36_prefix_external_io(path, writer.get_ref().sync_all())?;
    Ok(())
}

struct V36PrefixSpillReader {
    path: PathBuf,
    reader: BufReader<File>,
    remaining: u64,
    previous: Option<V36PrefixScoredIdentity>,
    expected_blake3: [u8; 32],
    payload_hasher: blake3::Hasher,
}

impl V36PrefixSpillReader {
    fn open(path: &Path, io_buffer_bytes: usize) -> Result<Self> {
        let file = File::open(path).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        let encoded_bytes = file
            .metadata()
            .map_err(|source| BorsukError::Io {
                path: path.to_owned(),
                source,
            })?
            .len();
        let mut file = file;
        v36_prefix_external_io(path, file.seek(SeekFrom::End(-32)))?;
        let mut expected_blake3 = [0_u8; 32];
        v36_prefix_external_io(path, file.read_exact(&mut expected_blake3))?;
        v36_prefix_external_io(path, file.seek(SeekFrom::Start(0)))?;
        let mut reader = BufReader::with_capacity(io_buffer_bytes, file);
        let mut magic = [0_u8; 8];
        v36_prefix_external_io(path, reader.read_exact(&mut magic))?;
        let mut count = [0_u8; 8];
        v36_prefix_external_io(path, reader.read_exact(&mut count))?;
        let remaining = u64::from_le_bytes(count);
        let expected_bytes = EXTERNAL_SPILL_HEADER_BYTES
            .checked_add(
                remaining
                    .checked_mul(EXTERNAL_SPILL_RECORD_BYTES)
                    .ok_or_else(|| invalid("V36 prefix spill length overflows"))?,
            )
            .and_then(|bytes| bytes.checked_add(EXTERNAL_SPILL_DIGEST_BYTES))
            .ok_or_else(|| invalid("V36 prefix spill length overflows"))?;
        if magic != EXTERNAL_SPILL_MAGIC || encoded_bytes != expected_bytes {
            return Err(invalid("V36 prefix spill header differs"));
        }
        Ok(Self {
            path: path.to_owned(),
            reader,
            remaining,
            previous: None,
            expected_blake3,
            payload_hasher: blake3::Hasher::new(),
        })
    }

    fn next_record(&mut self) -> Result<Option<V36PrefixScoredIdentity>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut bytes = [0_u8; EXTERNAL_SPILL_RECORD_BYTES as usize];
        v36_prefix_external_io(&self.path, self.reader.read_exact(&mut bytes))?;
        self.payload_hasher.update(&bytes);
        let score = bytes[..32].try_into().expect("fixed V36 spill score width");
        let record = V36PrefixScoredIdentity {
            score,
            feature_row_id: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            selected_object_ordinal: u16::from_le_bytes(bytes[40..42].try_into().unwrap()),
            row_offset: u64::from_le_bytes(bytes[42..50].try_into().unwrap()),
        };
        if self.previous.is_some_and(|previous| previous > record) {
            return Err(invalid("V36 prefix spill order differs"));
        }
        self.previous = Some(record);
        self.remaining -= 1;
        if self.remaining == 0 && self.payload_hasher.finalize().as_bytes() != &self.expected_blake3
        {
            return Err(invalid("V36 prefix spill digest differs"));
        }
        Ok(Some(record))
    }
}

fn create_v36_prefix_spill(
    attempt: &Path,
    ordinal: usize,
    records: &mut [V36PrefixScoredIdentity],
    io_buffer_bytes: usize,
    max_scratch_bytes: u64,
) -> Result<PathBuf> {
    records.sort_unstable();
    let path = attempt.join(format!("spill-{ordinal:08}.bin"));
    let record_count = u64::try_from(records.len())
        .map_err(|_| invalid("V36 prefix spill row count overflows"))?;
    let output_bytes = EXTERNAL_SPILL_HEADER_BYTES
        .checked_add(
            record_count
                .checked_mul(EXTERNAL_SPILL_RECORD_BYTES)
                .ok_or_else(|| invalid("V36 prefix spill length overflows"))?,
        )
        .and_then(|bytes| bytes.checked_add(EXTERNAL_SPILL_DIGEST_BYTES))
        .ok_or_else(|| invalid("V36 prefix spill length overflows"))?;
    if v36_prefix_scratch_bytes(attempt)?
        .checked_add(output_bytes)
        .is_none_or(|peak| peak > max_scratch_bytes)
    {
        return Err(resource_limit("scratch bytes"));
    }
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(|source| BorsukError::Io {
            path: path.clone(),
            source,
        })?;
    let mut writer = BufWriter::with_capacity(io_buffer_bytes, file);
    write_v36_prefix_spill_header(&mut writer, record_count)?;
    let mut payload_hasher = blake3::Hasher::new();
    for record in records {
        write_v36_prefix_scored_identity(&mut writer, &mut payload_hasher, record)?;
    }
    finish_v36_prefix_spill(&path, &mut writer, &payload_hasher)?;
    Ok(path)
}

fn merge_v36_prefix_spills(
    inputs: &[PathBuf],
    output: &Path,
    io_buffer_bytes: usize,
    max_scratch_bytes: u64,
) -> Result<()> {
    let mut readers = inputs
        .iter()
        .map(|path| V36PrefixSpillReader::open(path, io_buffer_bytes))
        .collect::<Result<Vec<_>>>()?;
    let count = readers.iter().try_fold(0_u64, |count, reader| {
        count
            .checked_add(reader.remaining)
            .ok_or_else(|| invalid("V36 prefix spill row count overflows"))
    })?;
    let output_bytes = EXTERNAL_SPILL_HEADER_BYTES
        .checked_add(
            count
                .checked_mul(EXTERNAL_SPILL_RECORD_BYTES)
                .ok_or_else(|| invalid("V36 prefix spill length overflows"))?,
        )
        .and_then(|bytes| bytes.checked_add(EXTERNAL_SPILL_DIGEST_BYTES))
        .ok_or_else(|| invalid("V36 prefix spill length overflows"))?;
    let attempt = output
        .parent()
        .ok_or_else(|| invalid("V36 prefix spill path differs"))?;
    let current_bytes = v36_prefix_scratch_bytes(attempt)?;
    if current_bytes
        .checked_add(output_bytes)
        .is_none_or(|peak| peak > max_scratch_bytes)
    {
        return Err(resource_limit("scratch bytes"));
    }
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output)
        .map_err(|source| BorsukError::Io {
            path: output.to_owned(),
            source,
        })?;
    let mut writer = BufWriter::with_capacity(io_buffer_bytes, file);
    write_v36_prefix_spill_header(&mut writer, count)?;
    let mut payload_hasher = blake3::Hasher::new();
    let mut heap = BinaryHeap::new();
    for (reader_index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = reader.next_record()? {
            heap.push(Reverse((record, reader_index)));
        }
    }
    while let Some(Reverse((record, reader_index))) = heap.pop() {
        write_v36_prefix_scored_identity(&mut writer, &mut payload_hasher, &record)?;
        if let Some(next) = readers[reader_index].next_record()? {
            heap.push(Reverse((next, reader_index)));
        }
    }
    finish_v36_prefix_spill(output, &mut writer, &payload_hasher)?;
    drop(writer);
    for reader in readers {
        fs::remove_file(&reader.path).map_err(|source| BorsukError::Io {
            path: reader.path,
            source,
        })?;
    }
    Ok(())
}

fn v36_prefix_scratch_bytes(attempt: &Path) -> Result<u64> {
    let mut bytes = 0_u64;
    for entry in fs::read_dir(attempt).map_err(|source| BorsukError::Io {
        path: attempt.to_owned(),
        source,
    })? {
        let entry = entry.map_err(|source| BorsukError::Io {
            path: attempt.to_owned(),
            source,
        })?;
        let path = entry.path();
        let metadata = entry.metadata().map_err(|source| BorsukError::Io {
            path: path.clone(),
            source,
        })?;
        if !metadata.is_file() {
            return Err(invalid("V36 prefix scratch entry differs"));
        }
        bytes = bytes
            .checked_add(metadata.len())
            .ok_or_else(|| invalid("V36 prefix scratch bytes overflow"))?;
    }
    Ok(bytes)
}

fn validate_v36_prefix_scratch_limit(attempt: &Path, limit: u64) -> Result<()> {
    if v36_prefix_scratch_bytes(attempt)? > limit {
        return Err(resource_limit("scratch bytes"));
    }
    Ok(())
}

fn v36_prefix_file_handle_digests(
    file: &mut File,
    path: &Path,
    io_buffer_bytes: usize,
) -> Result<(u64, String, String)> {
    v36_prefix_external_io(path, file.seek(SeekFrom::Start(0)))?;
    let encoded_bytes = file
        .metadata()
        .map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?
        .len();
    let mut sha256 = Sha256::new();
    let mut blake3 = blake3::Hasher::new();
    let mut buffer = vec![0_u8; io_buffer_bytes];
    loop {
        let read = v36_prefix_external_io(path, file.read(&mut buffer))?;
        if read == 0 {
            break;
        }
        sha256.update(&buffer[..read]);
        blake3.update(&buffer[..read]);
    }
    Ok((
        encoded_bytes,
        format!("{:x}", sha256.finalize()),
        blake3.finalize().to_hex().to_string(),
    ))
}

fn validate_v36_prefix_identity_batch_body(rows: u64, body_bytes: u64) -> Result<()> {
    const ALIGNMENT: u64 = 8;
    let validity_bytes = rows
        .checked_add(7)
        .map(|bits| bits / 8)
        .and_then(|bytes| bytes.checked_add(ALIGNMENT - 1))
        .map(|bytes| bytes / ALIGNMENT * ALIGNMENT)
        .ok_or_else(|| resource_limit("Arrow batch body bytes"))?;
    let values_bytes = rows
        .checked_mul(u64::try_from(size_of::<u64>()).unwrap())
        .ok_or_else(|| resource_limit("Arrow batch body bytes"))?;
    let expected = validity_bytes
        .checked_add(values_bytes)
        .and_then(|column| column.checked_mul(2))
        .ok_or_else(|| resource_limit("Arrow batch body bytes"))?;
    if body_bytes != expected {
        return Err(invalid("V36 prefix Arrow batch body layout differs"));
    }
    Ok(())
}

fn align_v36_prefix_arrow_buffer(bytes: u64) -> Result<u64> {
    bytes
        .checked_add(7)
        .map(|value| value / 8 * 8)
        .ok_or_else(|| resource_limit("Arrow batch body bytes"))
}

fn validate_v36_prefix_selected_batch_body(rows: u64, body_bytes: u64) -> Result<()> {
    let validity = align_v36_prefix_arrow_buffer(
        rows.checked_add(7)
            .map(|bits| bits / 8)
            .ok_or_else(|| resource_limit("Arrow batch body bytes"))?,
    )?;
    let values = rows
        .checked_mul(8 + 32 + 8)
        .and_then(|bytes| {
            rows.checked_mul(2)
                .and_then(|u16_bytes| align_v36_prefix_arrow_buffer(u16_bytes).ok())
                .and_then(|u16_bytes| bytes.checked_add(u16_bytes))
        })
        .ok_or_else(|| resource_limit("Arrow batch body bytes"))?;
    let expected = validity
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(values))
        .ok_or_else(|| resource_limit("Arrow batch body bytes"))?;
    if body_bytes != expected {
        return Err(invalid("V36 prefix Arrow batch body layout differs"));
    }
    Ok(())
}

fn copy_v36_prefix_snapshot_exact(
    source: &mut impl Read,
    snapshot: &mut impl Write,
    paths: (&Path, &Path),
    expected_bytes: u64,
    io_buffer_bytes: usize,
) -> Result<(String, String)> {
    let (source_path, snapshot_path) = paths;
    let mut sha256 = Sha256::new();
    let mut blake3 = blake3::Hasher::new();
    let mut remaining = expected_bytes;
    let mut buffer = vec![0_u8; io_buffer_bytes];
    while remaining > 0 {
        let limit = usize::try_from(remaining.min(io_buffer_bytes as u64)).unwrap();
        let read = v36_prefix_external_io(source_path, source.read(&mut buffer[..limit]))?;
        if read == 0 {
            return Err(invalid("V36 prefix identity-run snapshot differs"));
        }
        sha256.update(&buffer[..read]);
        blake3.update(&buffer[..read]);
        v36_prefix_external_io(snapshot_path, snapshot.write_all(&buffer[..read]))?;
        remaining -= read as u64;
    }
    let mut excess = [0_u8; 1];
    if v36_prefix_external_io(source_path, source.read(&mut excess))? != 0 {
        return Err(resource_limit("identity-run input bytes"));
    }
    Ok((
        format!("{:x}", sha256.finalize()),
        blake3.finalize().to_hex().to_string(),
    ))
}

fn preflight_v36_prefix_arrow_file(
    file: &mut File,
    path: &Path,
    encoded_bytes: u64,
    max_batch_rows: usize,
    validate_body: fn(u64, u64) -> Result<()>,
) -> Result<()> {
    const TRAILER_BYTES: u64 = 10;
    if encoded_bytes < TRAILER_BYTES {
        return Err(invalid("V36 prefix identity-run Arrow footer differs"));
    }
    v36_prefix_external_io(path, file.seek(SeekFrom::End(-10)))?;
    let mut trailer = [0_u8; 10];
    v36_prefix_external_io(path, file.read_exact(&mut trailer))?;
    let footer_len = read_footer_length(trailer)
        .map_err(|_| invalid("V36 prefix identity-run Arrow footer differs"))?;
    if footer_len > EXTERNAL_MAX_ARROW_FOOTER_BYTES {
        return Err(resource_limit("Arrow footer bytes"));
    }
    if footer_len == 0
        || u64::try_from(footer_len)
            .ok()
            .and_then(|length| length.checked_add(TRAILER_BYTES))
            .is_none_or(|required| required > encoded_bytes)
    {
        return Err(invalid("V36 prefix identity-run Arrow footer differs"));
    }
    let footer_start = encoded_bytes - TRAILER_BYTES - footer_len as u64;
    v36_prefix_external_io(path, file.seek(SeekFrom::Start(footer_start)))?;
    let mut footer_bytes = vec![0_u8; footer_len];
    v36_prefix_external_io(path, file.read_exact(&mut footer_bytes))?;
    let footer = root_as_footer(&footer_bytes)
        .map_err(|_| invalid("V36 prefix identity-run Arrow footer differs"))?;
    if footer
        .dictionaries()
        .is_some_and(|blocks| !blocks.is_empty())
    {
        return Err(invalid("V36 prefix identity-run Arrow dictionaries differ"));
    }
    let batches = footer
        .recordBatches()
        .ok_or_else(|| invalid("V36 prefix identity-run Arrow batches differ"))?;
    if batches.len() > EXTERNAL_MAX_ARROW_BATCHES {
        return Err(resource_limit("Arrow batch count"));
    }
    for block in batches {
        let offset = u64::try_from(block.offset())
            .map_err(|_| invalid("V36 prefix identity-run Arrow block differs"))?;
        let metadata = u64::try_from(block.metaDataLength())
            .map_err(|_| invalid("V36 prefix identity-run Arrow block differs"))?;
        let body = u64::try_from(block.bodyLength())
            .map_err(|_| invalid("V36 prefix identity-run Arrow block differs"))?;
        let end = offset
            .checked_add(metadata)
            .and_then(|value| value.checked_add(body))
            .ok_or_else(|| invalid("V36 prefix identity-run Arrow block differs"))?;
        if metadata < 4 || metadata > EXTERNAL_MAX_ARROW_FOOTER_BYTES as u64 || end > footer_start {
            return Err(invalid("V36 prefix identity-run Arrow block differs"));
        }
        v36_prefix_external_io(path, file.seek(SeekFrom::Start(offset)))?;
        let mut message_bytes = vec![0_u8; metadata as usize];
        v36_prefix_external_io(path, file.read_exact(&mut message_bytes))?;
        let (declared, message_start) = if message_bytes[..4] == [0xff; 4] {
            if message_bytes.len() < 8 {
                return Err(invalid("V36 prefix identity-run Arrow message differs"));
            }
            (
                u32::from_le_bytes(message_bytes[4..8].try_into().unwrap()) as usize,
                8_usize,
            )
        } else {
            (
                u32::from_le_bytes(message_bytes[..4].try_into().unwrap()) as usize,
                4_usize,
            )
        };
        let message_end = message_start
            .checked_add(declared)
            .filter(|end| *end <= message_bytes.len())
            .ok_or_else(|| invalid("V36 prefix identity-run Arrow message differs"))?;
        let message = root_as_message(&message_bytes[message_start..message_end])
            .map_err(|_| invalid("V36 prefix identity-run Arrow message differs"))?;
        let batch = message
            .header_as_record_batch()
            .ok_or_else(|| invalid("V36 prefix identity-run Arrow message differs"))?;
        if batch.length() <= 0
            || usize::try_from(batch.length())
                .ok()
                .is_none_or(|rows| rows > max_batch_rows)
            || batch.compression().is_some()
        {
            return Err(invalid("V36 prefix identity-run Arrow message differs"));
        }
        let rows = u64::try_from(batch.length())
            .map_err(|_| invalid("V36 prefix identity-run Arrow message differs"))?;
        let message_body = u64::try_from(message.bodyLength())
            .map_err(|_| invalid("V36 prefix identity-run Arrow message differs"))?;
        if message_body != body {
            return Err(invalid("V36 prefix identity-run Arrow block differs"));
        }
        validate_body(rows, body)?;
    }
    v36_prefix_external_io(path, file.seek(SeekFrom::Start(0)))?;
    Ok(())
}

fn stream_v36_prefix_identity_run_file(
    input: &V36PrefixIdentityRunFile,
    limits: &V36PrefixExternalSelectionLimits,
    attempt: &Path,
    seed: &[u8; 32],
    manifest: &[u8; 32],
    consume: &mut impl FnMut(V36PrefixScoredIdentity) -> Result<()>,
) -> Result<()> {
    let file_type = fs::symlink_metadata(&input.path)
        .map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?
        .file_type();
    if !file_type.is_file() || file_type.is_symlink() {
        return Err(invalid("V36 prefix local artifact path differs"));
    }
    let mut source_file = File::open(&input.path).map_err(|source| BorsukError::Io {
        path: input.path.clone(),
        source,
    })?;
    let encoded_bytes = source_file
        .metadata()
        .map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?
        .len();
    if encoded_bytes == 0 {
        return Err(invalid("V36 prefix identity-run artifact differs"));
    }
    if encoded_bytes > limits.max_input_bytes {
        return Err(resource_limit("identity-run input bytes"));
    }
    if v36_prefix_scratch_bytes(attempt)?
        .checked_add(encoded_bytes)
        .is_none_or(|peak| peak > limits.max_scratch_bytes)
    {
        return Err(resource_limit("scratch bytes"));
    }
    let snapshot_path = attempt.join(format!("input-{:04}.arrow", input.selected_object_ordinal));
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&snapshot_path)
        .map_err(|source| BorsukError::Io {
            path: snapshot_path.clone(),
            source,
        })?;
    let (sha256, blake3) = copy_v36_prefix_snapshot_exact(
        &mut source_file,
        &mut file,
        (&input.path, &snapshot_path),
        encoded_bytes,
        limits.io_buffer_bytes,
    )?;
    v36_prefix_external_io(&snapshot_path, file.sync_all())?;
    if file
        .metadata()
        .map_err(|source| BorsukError::Io {
            path: snapshot_path.clone(),
            source,
        })?
        .len()
        != encoded_bytes
    {
        return Err(invalid("V36 prefix identity-run snapshot differs"));
    }
    v36_prefix_external_io(&snapshot_path, file.seek(SeekFrom::Start(0)))?;
    let expected_role = format!(
        "population-identity-run-{:04}",
        input.selected_object_ordinal
    );
    let content_addressed = url::Url::parse(&input.identity.uri)
        .ok()
        .filter(|uri| uri.scheme() == "s3" && uri.host_str().is_some())
        .and_then(|uri| uri.path().rsplit('/').next().map(str::to_owned))
        .is_some_and(|name| name.starts_with(&format!("{sha256}-")));
    if input.identity.role != expected_role
        || input.identity.encoded_bytes != encoded_bytes
        || input.identity.sha256 != sha256
        || input.identity.blake3 != blake3
        || !content_addressed
    {
        return Err(invalid("V36 prefix identity-run artifact differs"));
    }
    preflight_v36_prefix_arrow_file(
        &mut file,
        &snapshot_path,
        encoded_bytes,
        IDENTITY_RUN_BATCH_ROWS,
        validate_v36_prefix_identity_batch_body,
    )?;
    let mut reader = ArrowFileReader::try_new(file, None)?;
    let schema = reader.schema();
    let row_count = schema
        .metadata()
        .get("rows")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| invalid("V36 prefix identity-run schema differs"))?;
    let physical_rows = schema
        .metadata()
        .get("physical_rows")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| invalid("V36 prefix identity-run schema differs"))?;
    if row_count > physical_rows {
        return Err(invalid("V36 prefix identity-run row count differs"));
    }
    let row_count_usize = usize::try_from(row_count)
        .map_err(|_| invalid("V36 prefix identity-run row count differs"))?;
    let expected_run = V36PrefixIdentityRun {
        physical_rows,
        rows: Vec::new(),
        selected_object_ordinal: input.selected_object_ordinal,
        source: input.source.clone(),
    };
    validate_v36_prefix_identity_run(&expected_run)?;
    let expected_batches = row_count_usize.div_ceil(IDENTITY_RUN_BATCH_ROWS);
    if schema.as_ref() != &v36_prefix_identity_run_schema(&expected_run, row_count_usize)
        || reader.num_batches() != expected_batches
    {
        return Err(invalid("V36 prefix identity-run schema differs"));
    }
    let mut previous_offset = None;
    let mut seen = 0_u64;
    for batch_index in 0..expected_batches {
        let batch = reader
            .next()
            .transpose()?
            .ok_or_else(|| invalid("V36 prefix identity-run batch is missing"))?;
        let expected_rows =
            (row_count_usize - batch_index * IDENTITY_RUN_BATCH_ROWS).min(IDENTITY_RUN_BATCH_ROWS);
        if batch.num_rows() != expected_rows {
            return Err(invalid("V36 prefix identity-run batches differ"));
        }
        let feature_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V36 prefix identity-run feature IDs differ"))?;
        let row_offsets = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V36 prefix identity-run row offsets differ"))?;
        for (&feature_row_id, &row_offset) in feature_ids.values().iter().zip(row_offsets.values())
        {
            if row_offset >= physical_rows
                || previous_offset.is_some_and(|prior| prior >= row_offset)
            {
                return Err(invalid("V36 prefix identity-run rows differ"));
            }
            previous_offset = Some(row_offset);
            consume(V36PrefixScoredIdentity {
                score: score(seed, manifest, feature_row_id),
                feature_row_id,
                selected_object_ordinal: input.selected_object_ordinal,
                row_offset,
            })?;
            seen += 1;
        }
    }
    if reader.next().is_some() || seen != row_count {
        return Err(invalid("V36 prefix identity-run batches differ"));
    }
    drop(reader);
    fs::remove_file(&snapshot_path).map_err(|source| BorsukError::Io {
        path: snapshot_path,
        source,
    })?;
    Ok(())
}

struct V36PrefixSelectedStream {
    reader: ArrowFileReader<File>,
    batch: Option<RecordBatch>,
    batch_index: usize,
    contract: V36PrefixSelectedIdsContract,
    cutoff_feature_row_id: u64,
    cutoff_score_sha256: String,
    expected_rows: usize,
    finished: bool,
    manifest: [u8; 32],
    previous: Option<([u8; 32], u64)>,
    seed: [u8; 32],
    seen: usize,
}

#[derive(Clone, Copy)]
struct V36PrefixSelectedStreamRecord {
    feature_row_id: u64,
    row_offset: u64,
    score: [u8; 32],
    selected_object_ordinal: u16,
}

impl V36PrefixSelectedStream {
    fn next_record(&mut self) -> Result<Option<V36PrefixSelectedStreamRecord>> {
        loop {
            if let Some(batch) = &self.batch
                && self.batch_index < batch.num_rows()
            {
                let index = self.batch_index;
                self.batch_index += 1;
                let feature_ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| invalid("V36 prefix selected-ID feature IDs differ"))?;
                let encoded_scores = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| invalid("V36 prefix selected-ID scores differ"))?;
                let object_ordinals = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .ok_or_else(|| invalid("V36 prefix selected-ID object ordinals differ"))?;
                let row_offsets = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| invalid("V36 prefix selected-ID row offsets differ"))?;
                let feature_row_id = feature_ids.value(index);
                let computed = score(&self.seed, &self.manifest, feature_row_id);
                let rank = (computed, feature_row_id);
                let window_end =
                    self.contract.selected_object_start + self.contract.selected_object_count;
                if encoded_scores.value(index) != computed
                    || object_ordinals.value(index) < self.contract.selected_object_start
                    || object_ordinals.value(index) >= window_end
                    || self.previous.is_some_and(|previous| previous >= rank)
                    || row_offsets.is_null(index)
                {
                    return Err(invalid("V36 prefix selected-ID rows differ"));
                }
                self.previous = Some(rank);
                self.seen += 1;
                return Ok(Some(V36PrefixSelectedStreamRecord {
                    feature_row_id,
                    row_offset: row_offsets.value(index),
                    score: computed,
                    selected_object_ordinal: object_ordinals.value(index),
                }));
            }
            if self.finished {
                return Ok(None);
            }
            match self.reader.next().transpose()? {
                Some(batch) => {
                    let remaining = self.expected_rows.saturating_sub(self.seen);
                    let expected = remaining.min(SELECTED_IDS_BATCH_ROWS);
                    if batch.num_rows() != expected
                        || batch.num_columns() != 4
                        || batch
                            .columns()
                            .iter()
                            .any(|column| column.null_count() != 0)
                    {
                        return Err(invalid("V36 prefix selected-ID batches differ"));
                    }
                    self.batch = Some(batch);
                    self.batch_index = 0;
                }
                None => {
                    self.finished = true;
                    let final_rank = self
                        .previous
                        .ok_or_else(|| invalid("V36 prefix selected-ID cutoff is missing"))?;
                    if self.seen != self.expected_rows
                        || final_rank.1 != self.cutoff_feature_row_id
                        || digest_hex(&final_rank.0) != self.cutoff_score_sha256
                    {
                        return Err(invalid("V36 prefix selected-ID cutoff differs"));
                    }
                    return Ok(None);
                }
            }
        }
    }
}

struct V36PrefixPhysicalUniqueness<'a> {
    attempt: &'a Path,
    buffer: Vec<V36PrefixScoredIdentity>,
    limits: &'a V36PrefixExternalSelectionLimits,
    next_spill: usize,
    spills: Vec<PathBuf>,
}

impl<'a> V36PrefixPhysicalUniqueness<'a> {
    fn new(
        attempt: &'a Path,
        limits: &'a V36PrefixExternalSelectionLimits,
        next_spill: usize,
    ) -> Self {
        Self {
            attempt,
            buffer: Vec::with_capacity(limits.sort_buffer_records),
            limits,
            next_spill,
            spills: Vec::new(),
        }
    }

    fn push(&mut self, selected: V36PrefixSelectedStreamRecord) -> Result<()> {
        let mut physical_key = [0_u8; 32];
        physical_key[..2].copy_from_slice(&selected.selected_object_ordinal.to_be_bytes());
        physical_key[2..10].copy_from_slice(&selected.row_offset.to_be_bytes());
        self.buffer.push(V36PrefixScoredIdentity {
            score: physical_key,
            feature_row_id: selected.feature_row_id,
            selected_object_ordinal: selected.selected_object_ordinal,
            row_offset: selected.row_offset,
        });
        if self.buffer.len() == self.limits.sort_buffer_records {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        if self.spills.len() >= self.limits.max_spills {
            return Err(resource_limit("spill count"));
        }
        let path = create_v36_prefix_spill(
            self.attempt,
            self.next_spill,
            &mut self.buffer,
            self.limits.io_buffer_bytes,
            self.limits.max_scratch_bytes,
        )?;
        self.spills.push(path);
        self.next_spill += 1;
        self.buffer.clear();
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        self.flush()?;
        while self.spills.len() > self.limits.merge_fan_in {
            let mut outputs = Vec::new();
            for group in self.spills.chunks(self.limits.merge_fan_in) {
                let output = self
                    .attempt
                    .join(format!("spill-{:08}.bin", self.next_spill));
                merge_v36_prefix_spills(
                    group,
                    &output,
                    self.limits.io_buffer_bytes,
                    self.limits.max_scratch_bytes,
                )?;
                outputs.push(output);
                self.next_spill += 1;
            }
            self.spills = outputs;
        }
        let physical = if self.spills.len() == 1 {
            self.spills.pop().unwrap()
        } else {
            let output = self
                .attempt
                .join(format!("spill-{:08}.bin", self.next_spill));
            merge_v36_prefix_spills(
                &self.spills,
                &output,
                self.limits.io_buffer_bytes,
                self.limits.max_scratch_bytes,
            )?;
            output
        };
        let mut reader = V36PrefixSpillReader::open(&physical, self.limits.io_buffer_bytes)?;
        let mut previous_key = None;
        while let Some(record) = reader.next_record()? {
            if previous_key == Some(record.score) {
                return Err(invalid("V36 prefix selected-ID rows differ"));
            }
            previous_key = Some(record.score);
        }
        drop(reader);
        fs::remove_file(&physical).map_err(|source| BorsukError::Io {
            path: physical,
            source,
        })?;
        Ok(())
    }
}

fn next_v36_prefix_excluded_rank(
    stream: &mut V36PrefixSelectedStream,
    physical: &mut V36PrefixPhysicalUniqueness<'_>,
) -> Result<Option<([u8; 32], u64)>> {
    let Some(record) = stream.next_record()? else {
        return Ok(None);
    };
    physical.push(record)?;
    Ok(Some((record.score, record.feature_row_id)))
}

fn open_v36_prefix_selected_stream(
    selected: &V36PrefixSelectedIdsFile,
    limits: &V36PrefixExternalSelectionLimits,
    attempt: &Path,
    snapshot_filename: &'static str,
) -> Result<V36PrefixSelectedStream> {
    if !matches!(
        snapshot_filename,
        "excluded-population.arrow" | "selected-population.arrow"
    ) {
        return Err(invalid("V36 prefix selected-ID snapshot role differs"));
    }
    let file_type = fs::symlink_metadata(&selected.path)
        .map_err(|source| BorsukError::Io {
            path: selected.path.clone(),
            source,
        })?
        .file_type();
    if !file_type.is_file() || file_type.is_symlink() {
        return Err(invalid("V36 prefix local artifact path differs"));
    }
    let mut source = File::open(&selected.path).map_err(|source| BorsukError::Io {
        path: selected.path.clone(),
        source,
    })?;
    let encoded_bytes = source
        .metadata()
        .map_err(|source| BorsukError::Io {
            path: selected.path.clone(),
            source,
        })?
        .len();
    if encoded_bytes == 0 || encoded_bytes > limits.max_input_bytes {
        return Err(resource_limit("selected-ID input bytes"));
    }
    if v36_prefix_scratch_bytes(attempt)?
        .checked_add(encoded_bytes)
        .is_none_or(|peak| peak > limits.max_scratch_bytes)
    {
        return Err(resource_limit("scratch bytes"));
    }
    let snapshot_path = attempt.join(snapshot_filename);
    let mut snapshot = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&snapshot_path)
        .map_err(|source| BorsukError::Io {
            path: snapshot_path.clone(),
            source,
        })?;
    let (sha256, blake3) = copy_v36_prefix_snapshot_exact(
        &mut source,
        &mut snapshot,
        (&selected.path, &snapshot_path),
        encoded_bytes,
        limits.io_buffer_bytes,
    )?;
    v36_prefix_external_io(&snapshot_path, snapshot.sync_all())?;
    let content_addressed = url::Url::parse(&selected.identity.uri)
        .ok()
        .filter(|uri| uri.scheme() == "s3" && uri.host_str().is_some())
        .and_then(|uri| uri.path().rsplit('/').next().map(str::to_owned))
        .is_some_and(|name| name.starts_with(&format!("{sha256}-")));
    if selected.identity.role != "population-selected-identities"
        || selected.identity.encoded_bytes != encoded_bytes
        || selected.identity.sha256 != sha256
        || selected.identity.blake3 != blake3
        || !content_addressed
    {
        return Err(invalid("V36 prefix selected-ID artifact differs"));
    }
    preflight_v36_prefix_arrow_file(
        &mut snapshot,
        &snapshot_path,
        encoded_bytes,
        SELECTED_IDS_BATCH_ROWS,
        validate_v36_prefix_selected_batch_body,
    )?;
    let reader = ArrowFileReader::try_new(snapshot, None)?;
    let expected_rows = usize::try_from(selected.contract.selected_rows)
        .map_err(|_| invalid("V36 prefix selected-ID row count overflows"))?;
    let expected_batches = expected_rows.div_ceil(SELECTED_IDS_BATCH_ROWS);
    let schema = reader.schema();
    let cutoff_feature_row_id = schema
        .metadata()
        .get("cutoff_feature_row_id")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| invalid("V36 prefix selected-ID schema differs"))?;
    let cutoff_score_sha256 = schema
        .metadata()
        .get("cutoff_score_sha256")
        .ok_or_else(|| invalid("V36 prefix selected-ID schema differs"))?
        .clone();
    if schema.as_ref()
        != &selected_ids_schema(
            &selected.contract,
            cutoff_feature_row_id,
            &cutoff_score_sha256,
        )
        || reader.num_batches() != expected_batches
    {
        return Err(invalid("V36 prefix selected-ID schema differs"));
    }
    let (seed, manifest) = validate_selected_ids_contract(&selected.contract)?;
    Ok(V36PrefixSelectedStream {
        reader,
        batch: None,
        batch_index: 0,
        contract: selected.contract.clone(),
        cutoff_feature_row_id,
        cutoff_score_sha256,
        expected_rows,
        finished: false,
        manifest,
        previous: None,
        seed,
        seen: 0,
    })
}

fn write_v36_prefix_selected_file(
    contract: &V36PrefixSelectedIdsContract,
    selected_spill: &Path,
    cutoff: V36PrefixScoredIdentity,
    limits: &V36PrefixExternalSelectionLimits,
    output: &Path,
) -> Result<(u64, String, String)> {
    let cutoff_sha256 = digest_hex(&cutoff.score);
    let seed = digest_bytes(&contract.population_seed_sha256)?;
    let manifest = digest_bytes(&contract.ordered_source_manifest_sha256)?;
    let schema = Arc::new(selected_ids_schema(
        contract,
        cutoff.feature_row_id,
        &cutoff_sha256,
    ));
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut reader = V36PrefixSpillReader::open(selected_spill, limits.io_buffer_bytes)?;
    let mut temporary = temporary_output(output)?;
    let mut writer = ArrowFileWriter::try_new_with_options(
        BufWriter::with_capacity(limits.io_buffer_bytes, temporary.as_file()),
        schema.as_ref(),
        options,
    )?;
    let mut rows = Vec::with_capacity(SELECTED_IDS_BATCH_ROWS);
    while let Some(record) = reader.next_record()? {
        if score(&seed, &manifest, record.feature_row_id) != record.score {
            return Err(invalid("V36 prefix selected-ID score differs"));
        }
        rows.push(record);
        if rows.len() == SELECTED_IDS_BATCH_ROWS {
            write_v36_prefix_selected_batch(&mut writer, schema.clone(), &rows)?;
            rows.clear();
        }
    }
    if !rows.is_empty() {
        write_v36_prefix_selected_batch(&mut writer, schema, &rows)?;
    }
    writer.finish()?;
    let mut buffered = writer.into_inner()?;
    v36_prefix_external_io(output, buffered.flush())?;
    drop(buffered);
    let identity =
        v36_prefix_file_handle_digests(temporary.as_file_mut(), output, limits.io_buffer_bytes)?;
    publish_output_noclobber(temporary, output)?;
    Ok(identity)
}

fn write_v36_prefix_selected_batch(
    writer: &mut ArrowFileWriter<BufWriter<&File>>,
    schema: Arc<Schema>,
    rows: &[V36PrefixScoredIdentity],
) -> Result<()> {
    let scores = rows
        .iter()
        .map(|row| row.score.as_slice())
        .collect::<Vec<_>>();
    writer.write(&RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|row| row.feature_row_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(FixedSizeBinaryArray::try_from_iter(scores.into_iter())?),
            Arc::new(UInt16Array::from(
                rows.iter()
                    .map(|row| row.selected_object_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.row_offset).collect::<Vec<_>>(),
            )),
        ],
    )?)?;
    Ok(())
}

/// Externally select one complete authenticated population without resident bulk state.
pub fn externally_select_v36_prefix_population_rows(
    request: V36PrefixExternalSelectionRequest<'_>,
) -> Result<V36PrefixSelectedFileReceipt> {
    let V36PrefixExternalSelectionRequest {
        contract,
        exclusion,
        limits,
        output,
        output_uri_prefix,
        runs,
        scratch_root,
    } = request;
    validate_selected_ids_contract(contract)?;
    if limits.max_spills > EXTERNAL_MAX_SPILLS
        || limits.merge_fan_in > EXTERNAL_MAX_FAN_IN
        || limits.sort_buffer_records > EXTERNAL_MAX_SORT_BUFFER_RECORDS
        || limits.io_buffer_bytes > EXTERNAL_MAX_IO_BUFFER_BYTES
    {
        return Err(resource_limit("external selection configuration"));
    }
    let output_uri = url::Url::parse(output_uri_prefix)
        .map_err(|_| invalid("V36 prefix selected-ID output URI differs"))?;
    let output_parent = output
        .parent()
        .ok_or_else(|| invalid("V36 prefix selected-ID output path differs"))?;
    let scratch_type = fs::symlink_metadata(scratch_root)
        .map_err(|source| BorsukError::Io {
            path: scratch_root.to_owned(),
            source,
        })?
        .file_type();
    let output_parent_type = fs::symlink_metadata(output_parent)
        .map_err(|source| BorsukError::Io {
            path: output_parent.to_owned(),
            source,
        })?
        .file_type();
    if output_uri.scheme() != "s3"
        || output_uri.host_str().is_none()
        || output_uri.query().is_some()
        || output_uri.fragment().is_some()
        || output.exists()
        || !scratch_type.is_dir()
        || scratch_type.is_symlink()
        || !output_parent_type.is_dir()
        || output_parent_type.is_symlink()
    {
        return Err(invalid("V36 prefix external selection output differs"));
    }
    let exclusion_is_valid = match (contract.cohort_ordinal, exclusion) {
        (0, None) => contract.excluded_population_identity.is_none() && contract.excluded_rows == 0,
        (0, Some(_)) | (_, None) => false,
        (_, Some(selected)) => {
            validate_selected_ids_contract(&selected.contract).is_ok()
                && contract.excluded_population_identity.as_ref() == Some(&selected.identity)
                && selected.contract.cohort_ordinal.checked_add(1) == Some(contract.cohort_ordinal)
                && selected.contract.population_seed_sha256 == contract.population_seed_sha256
                && selected.contract.ordered_source_manifest_sha256
                    == contract.ordered_source_manifest_sha256
                && selected
                    .contract
                    .selected_object_start
                    .checked_add(selected.contract.selected_object_count)
                    == Some(contract.selected_object_start)
                && contract.excluded_rows <= selected.contract.selected_rows
        }
    };
    if !exclusion_is_valid
        || runs.len() != usize::from(contract.selected_object_count)
        || runs.is_empty()
        || limits.io_buffer_bytes == 0
        || limits.max_input_bytes == 0
        || limits.max_scratch_bytes == 0
        || limits.max_spills == 0
        || limits.merge_fan_in < 2
        || limits.sort_buffer_records == 0
        || runs.len() > 16
        || output_uri_prefix.trim_end_matches('/').is_empty()
    {
        return Err(invalid("V36 prefix external selection authority differs"));
    }
    let seed = digest_bytes(&contract.population_seed_sha256)?;
    let manifest = digest_bytes(&contract.ordered_source_manifest_sha256)?;
    let mut source_paths = BTreeSet::new();
    for (index, run) in runs.iter().enumerate() {
        let expected_ordinal = contract
            .selected_object_start
            .checked_add(
                u16::try_from(index)
                    .map_err(|_| invalid("V36 prefix identity-run ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V36 prefix identity-run ordinal overflows"))?;
        if run.selected_object_ordinal != expected_ordinal
            || !source_paths.insert(run.source.path.as_str())
        {
            return Err(invalid("V36 prefix identity-run sequence differs"));
        }
    }
    let attempt = tempfile::tempdir_in(scratch_root).map_err(|source| BorsukError::Io {
        path: scratch_root.to_owned(),
        source,
    })?;
    let mut exclusion_stream = exclusion
        .map(|selected| {
            open_v36_prefix_selected_stream(
                selected,
                limits,
                attempt.path(),
                "excluded-population.arrow",
            )
        })
        .transpose()?;
    let mut spills = Vec::new();
    let mut buffer = Vec::with_capacity(limits.sort_buffer_records);
    let mut next_spill = 0_usize;
    let mut scratch_bytes = 0_u64;
    for run in runs {
        stream_v36_prefix_identity_run_file(
            run,
            limits,
            attempt.path(),
            &seed,
            &manifest,
            &mut |record| {
                buffer.push(record);
                if buffer.len() == limits.sort_buffer_records {
                    if spills.len() >= limits.max_spills {
                        return Err(resource_limit("spill count"));
                    }
                    let path = create_v36_prefix_spill(
                        attempt.path(),
                        next_spill,
                        &mut buffer,
                        limits.io_buffer_bytes,
                        limits.max_scratch_bytes,
                    )?;
                    let spill_bytes = v36_prefix_external_io(&path, fs::metadata(&path))?.len();
                    scratch_bytes = scratch_bytes
                        .checked_add(spill_bytes)
                        .ok_or_else(|| invalid("V36 prefix scratch bytes overflow"))?;
                    if scratch_bytes > limits.max_scratch_bytes {
                        return Err(resource_limit("scratch bytes"));
                    }
                    spills.push(path);
                    next_spill += 1;
                    buffer.clear();
                }
                Ok(())
            },
        )?;
    }
    if !buffer.is_empty() {
        if spills.len() >= limits.max_spills {
            return Err(resource_limit("spill count"));
        }
        let path = create_v36_prefix_spill(
            attempt.path(),
            next_spill,
            &mut buffer,
            limits.io_buffer_bytes,
            limits.max_scratch_bytes,
        )?;
        let spill_bytes = v36_prefix_external_io(&path, fs::metadata(&path))?.len();
        scratch_bytes = scratch_bytes
            .checked_add(spill_bytes)
            .ok_or_else(|| invalid("V36 prefix scratch bytes overflow"))?;
        if scratch_bytes > limits.max_scratch_bytes {
            return Err(resource_limit("scratch bytes"));
        }
        spills.push(path);
        next_spill += 1;
    }
    if spills.is_empty() {
        if let Some(stream) = exclusion_stream.as_mut() {
            let mut physical = V36PrefixPhysicalUniqueness::new(attempt.path(), limits, next_spill);
            while next_v36_prefix_excluded_rank(stream, &mut physical)?.is_some() {}
            physical.finish()?;
        }
        return Err(BorsukError::V36PrefixSourceInsufficient);
    }
    while spills.len() > limits.merge_fan_in {
        let mut outputs = Vec::new();
        for group in spills.chunks(limits.merge_fan_in) {
            let output = attempt.path().join(format!("spill-{next_spill:08}.bin"));
            merge_v36_prefix_spills(
                group,
                &output,
                limits.io_buffer_bytes,
                limits.max_scratch_bytes,
            )?;
            validate_v36_prefix_scratch_limit(attempt.path(), limits.max_scratch_bytes)?;
            outputs.push(output);
            next_spill += 1;
        }
        spills = outputs;
    }
    let merged = attempt.path().join(format!("spill-{next_spill:08}.bin"));
    merge_v36_prefix_spills(
        &spills,
        &merged,
        limits.io_buffer_bytes,
        limits.max_scratch_bytes,
    )?;
    validate_v36_prefix_scratch_limit(attempt.path(), limits.max_scratch_bytes)?;
    next_spill += 1;
    let selected_path = attempt.path().join(format!("spill-{next_spill:08}.bin"));
    let selected_bytes = EXTERNAL_SPILL_HEADER_BYTES
        .checked_add(
            contract
                .selected_rows
                .checked_mul(EXTERNAL_SPILL_RECORD_BYTES)
                .ok_or_else(|| invalid("V36 prefix spill length overflows"))?,
        )
        .and_then(|bytes| bytes.checked_add(EXTERNAL_SPILL_DIGEST_BYTES))
        .ok_or_else(|| invalid("V36 prefix spill length overflows"))?;
    if v36_prefix_scratch_bytes(attempt.path())?
        .checked_add(selected_bytes)
        .is_none_or(|peak| peak > limits.max_scratch_bytes)
    {
        return Err(resource_limit("scratch bytes"));
    }
    let selected_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&selected_path)
        .map_err(|source| BorsukError::Io {
            path: selected_path.clone(),
            source,
        })?;
    let mut selected_writer = BufWriter::with_capacity(limits.io_buffer_bytes, selected_file);
    write_v36_prefix_spill_header(&mut selected_writer, contract.selected_rows)?;
    let mut selected_hasher = blake3::Hasher::new();
    let mut merged_reader = V36PrefixSpillReader::open(&merged, limits.io_buffer_bytes)?;
    let mut eligible_rows = 0_u64;
    let mut excluded_rows = 0_u64;
    let mut previous_id = None;
    let mut cutoff = None;
    let mut exclusion_physical =
        V36PrefixPhysicalUniqueness::new(attempt.path(), limits, next_spill + 1);
    let mut excluded_rank = exclusion_stream
        .as_mut()
        .map(|stream| next_v36_prefix_excluded_rank(stream, &mut exclusion_physical))
        .transpose()?
        .flatten();
    while let Some(record) = merged_reader.next_record()? {
        if previous_id == Some(record.feature_row_id) {
            return Err(invalid("V36 prefix identity-run global ID repeats"));
        }
        previous_id = Some(record.feature_row_id);
        let rank = (record.score, record.feature_row_id);
        while excluded_rank.is_some_and(|excluded| excluded < rank) {
            excluded_rank = next_v36_prefix_excluded_rank(
                exclusion_stream
                    .as_mut()
                    .expect("V36 exclusion stream exists with an exclusion rank"),
                &mut exclusion_physical,
            )?;
        }
        if excluded_rank == Some(rank) {
            excluded_rows += 1;
            excluded_rank = next_v36_prefix_excluded_rank(
                exclusion_stream
                    .as_mut()
                    .expect("V36 exclusion stream exists with an exclusion rank"),
                &mut exclusion_physical,
            )?;
            continue;
        }
        eligible_rows += 1;
        if eligible_rows <= contract.selected_rows {
            write_v36_prefix_scored_identity(&mut selected_writer, &mut selected_hasher, &record)?;
            cutoff = Some(record);
        }
    }
    if let Some(stream) = exclusion_stream.as_mut() {
        while next_v36_prefix_excluded_rank(stream, &mut exclusion_physical)?.is_some() {}
        exclusion_physical.finish()?;
    }
    if eligible_rows < contract.selected_rows {
        return Err(BorsukError::V36PrefixSourceInsufficient);
    }
    if eligible_rows != contract.eligible_rows || excluded_rows != contract.excluded_rows {
        return Err(invalid("V36 prefix selected-ID eligible rows differ"));
    }
    finish_v36_prefix_spill(&selected_path, &mut selected_writer, &selected_hasher)?;
    drop(selected_writer);
    validate_v36_prefix_scratch_limit(attempt.path(), limits.max_scratch_bytes)?;
    let (encoded_bytes, sha256, blake3) = write_v36_prefix_selected_file(
        contract,
        &selected_path,
        cutoff.ok_or(BorsukError::V36PrefixSourceInsufficient)?,
        limits,
        output,
    )?;
    let prefix = output_uri_prefix.trim_end_matches('/');
    Ok(V36PrefixSelectedFileReceipt {
        identity: V36ArtifactIdentity {
            blake3,
            encoded_bytes,
            role: "population-selected-identities".to_owned(),
            sha256: sha256.clone(),
            uri: format!("{prefix}/{sha256}-population-selected-identities.arrow"),
        },
    })
}

/// Validate membership against independently authenticated complete-object evidence.
pub fn validate_v36_prefix_cutoff_membership(
    rows: &[V36PrefixRowIdentity],
    consumed_objects: usize,
    distinct_candidates: usize,
) -> Result<()> {
    validate_v36_prefix_cutoff_membership_from_start(rows, 0, consumed_objects, distinct_candidates)
}

fn validate_v36_prefix_cutoff_membership_from_start(
    rows: &[V36PrefixRowIdentity],
    selected_object_start: u16,
    consumed_objects: usize,
    distinct_candidates: usize,
) -> Result<()> {
    let selected_object_end = selected_object_start
        .checked_add(
            u16::try_from(consumed_objects)
                .map_err(|_| invalid("V36 prefix consumed-object count overflows"))?,
        )
        .ok_or_else(|| invalid("V36 prefix consumed-object window overflows"))?;
    if consumed_objects == 0
        || rows.len() < distinct_candidates
        || distinct_candidates == 0
        || rows.iter().any(|row| {
            row.selected_object_ordinal < selected_object_start
                || row.selected_object_ordinal >= selected_object_end
        })
    {
        return Err(invalid("V36 prefix consumed-object membership differs"));
    }
    Ok(())
}

fn score(seed: &[u8; 32], source_identity: &[u8; 32], feature_row_id: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(seed);
    hasher.update(source_identity);
    hasher.update(feature_row_id.to_le_bytes());
    hasher.finalize().into()
}

/// Compute one frozen prefix-population query score for audit and mutation tests.
pub fn v36_prefix_query_score_sha256(
    seed_label: &str,
    source_identity_sha256: &str,
    feature_row_id: u64,
) -> Result<String> {
    let seed: [u8; 32] = Sha256::digest(seed_label.as_bytes()).into();
    let source_identity = digest_bytes(source_identity_sha256)?;
    Ok(score(&seed, &source_identity, feature_row_id)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// Compute one frozen prefix-population corpus score for audit and mutation tests.
pub fn v36_prefix_source_score_sha256(
    source_identity_sha256: &str,
    feature_row_id: u64,
) -> Result<String> {
    let seed: [u8; 32] = Sha256::digest(b"borsuk-v36-prefix-screen-corpus-v2").into();
    let source_identity = digest_bytes(source_identity_sha256)?;
    Ok(score(&seed, &source_identity, feature_row_id)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// Validate the exact population-specific query-role authority.
pub fn validate_v36_prefix_role_authority(roles: &[V36PrefixRoleAuthority]) -> Result<()> {
    let expected_names = ["development", "validation", "sealed-holdout", "performance"];
    let expected_labels = [
        "borsuk-v36-prefix-screen-development-query-v2",
        "borsuk-v36-prefix-screen-validation-query-v2",
        "borsuk-v36-prefix-screen-sealed-holdout-query-v2",
        "borsuk-v36-prefix-screen-performance-query-v2",
    ];
    let expected_rows = [1_000_u64, 1_000, 1_000, 10_000];
    if roles.len() != expected_names.len() {
        return Err(invalid("V36 prefix query role authority differs"));
    }
    let mut seeds = BTreeSet::new();
    for (((role, expected_name), expected_label), expected_rows) in roles
        .iter()
        .zip(expected_names)
        .zip(expected_labels)
        .zip(expected_rows)
    {
        if role.role != expected_name
            || role.seed_label != expected_label
            || role.rows != expected_rows
            || role.seed_sha256 != format!("{:x}", Sha256::digest(role.seed_label.as_bytes()))
            || !seeds.insert(role.seed_sha256.as_str())
        {
            return Err(invalid("V36 prefix query role authority differs"));
        }
    }
    Ok(())
}

/// Select disjoint query roles in priority order, then order the corpus.
pub fn select_v36_prefix_roles(
    rows: Vec<V36PrefixRowIdentity>,
    population: &V36PrefixPopulationAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<V36PrefixRoleSplit> {
    validate_v36_prefix_population_authority(population, source_registry)?;
    validate_v36_prefix_role_authority(&population.roles)?;
    let source_identity = digest_bytes(&population.ordered_source_manifest_sha256)?;
    let window_start = population.selected_object_start;
    let window_end = window_start
        .checked_add(population.selected_object_count)
        .ok_or_else(|| invalid("V36 prefix selected object window overflows"))?;
    if rows.iter().any(|row| {
        row.selected_object_ordinal < window_start || row.selected_object_ordinal >= window_end
    }) {
        return Err(invalid("V36 prefix population row object differs"));
    }
    let mut remaining = select_v36_prefix_population_rows(
        rows,
        &population.ordered_source_manifest_sha256,
        DISTINCT_CANDIDATES,
    )?;
    let mut selected = Vec::with_capacity(4);
    for role in &population.roles {
        let seed = digest_bytes(&role.seed_sha256)?;
        let mut ranked = remaining
            .into_iter()
            .map(|row| (score(&seed, &source_identity, row.feature_row_id), row))
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            (&left.0, left.1.feature_row_id).cmp(&(&right.0, right.1.feature_row_id))
        });
        let count =
            usize::try_from(role.rows).map_err(|_| invalid("V36 prefix query count overflows"))?;
        if ranked.len() < count {
            return Err(invalid("V36 prefix query population is insufficient"));
        }
        let rest = ranked.split_off(count);
        selected.push(ranked.into_iter().map(|(_, row)| row).collect::<Vec<_>>());
        remaining = rest.into_iter().map(|(_, row)| row).collect();
    }
    let corpus_seed = digest_bytes(&population.corpus_seed_sha256)?;
    let mut corpus = remaining
        .into_iter()
        .map(|row| {
            (
                score(&corpus_seed, &source_identity, row.feature_row_id),
                row,
            )
        })
        .collect::<Vec<_>>();
    corpus.sort_by(|left, right| {
        (&left.0, left.1.feature_row_id).cmp(&(&right.0, right.1.feature_row_id))
    });
    if corpus.len() < CORPUS_ROWS {
        return Err(invalid("V36 prefix corpus size differs"));
    }
    corpus.truncate(CORPUS_ROWS);
    let corpus = corpus
        .into_iter()
        .enumerate()
        .map(|(ordinal, (_, mut row))| {
            row.source_ordinal = Some(u64::try_from(ordinal).unwrap());
            row
        })
        .collect();
    let [development, validation, sealed_holdout, performance] = selected
        .try_into()
        .map_err(|_| invalid("V36 prefix query roles differ"))?;
    Ok(V36PrefixRoleSplit {
        development,
        validation,
        sealed_holdout,
        performance,
        corpus,
    })
}

fn vector_field() -> Field {
    Field::new(
        "embedding",
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, false)),
            DIMENSIONS as i32,
        ),
        false,
    )
}

fn registered_input_schema() -> Schema {
    Schema::new(vec![
        Field::new("url", DataType::Utf8, true),
        Field::new("natural_score", DataType::Float32, true),
        Field::new("feature_row_id", DataType::Int64, true),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIMENSIONS as i32,
            ),
            true,
        ),
    ])
}

fn sha256_file(path: &Path) -> Result<(u64, String)> {
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut bytes = 0_u64;
    let mut hasher = Sha256::new();
    loop {
        let read = reader.read(&mut buffer).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(u64::try_from(read).unwrap())
            .ok_or_else(|| invalid("V36 prefix registered object length overflows"))?;
        hasher.update(&buffer[..read]);
    }
    Ok((bytes, format!("{:x}", hasher.finalize())))
}

fn authenticate_file(path: &Path, expected: &V36ArtifactIdentity) -> Result<()> {
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut bytes = 0_u64;
    let mut sha256 = Sha256::new();
    let mut blake3 = blake3::Hasher::new();
    loop {
        let read = reader.read(&mut buffer).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(u64::try_from(read).unwrap())
            .ok_or_else(|| invalid("V36 prefix local input length overflows"))?;
        sha256.update(&buffer[..read]);
        blake3.update(&buffer[..read]);
    }
    if bytes != expected.encoded_bytes
        || format!("{:x}", sha256.finalize()) != expected.sha256
        || blake3.finalize().to_hex().as_str() != expected.blake3
    {
        return Err(invalid("V36 prefix local input authority differs"));
    }
    Ok(())
}

fn read_file(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })
}

fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut temporary = temporary_output(path)?;
    temporary
        .write_all(bytes)
        .map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
    publish_output(temporary, path)
}

fn acquire_v36_prefix_object(
    runtime: &tokio::runtime::Runtime,
    object: &V36PrefixRankedSourceObject,
    scratch: &Path,
    ordinal: u16,
) -> Result<PathBuf> {
    if !scratch.is_dir() || object.encoded_bytes == 0 {
        return Err(invalid("V36 prefix object acquisition request differs"));
    }
    digest_bytes(&object.sha256)?;
    let uri = url::Url::parse(&object.uri)
        .map_err(|_| invalid("V36 prefix registered object URI differs"))?;
    let (scheme, location) = ObjectStoreScheme::parse(&uri)
        .map_err(|_| invalid("V36 prefix registered object URI differs"))?;
    let store: Box<dyn ObjectStore> = match scheme {
        ObjectStoreScheme::Http => {
            let origin = uri.origin().ascii_serialization();
            Box::new(
                object_store::http::HttpBuilder::new()
                    .with_url(origin)
                    .with_retry(object_store::RetryConfig {
                        max_retries: 0,
                        ..Default::default()
                    })
                    .with_client_options(
                        object_store::ClientOptions::new()
                            .with_allow_http(uri.scheme() == "http")
                            .with_timeout_disabled()
                            .with_read_timeout(std::time::Duration::from_secs(120)),
                    )
                    .build()?,
            )
        }
        ObjectStoreScheme::Local => object_store::parse_url(&uri)?.0,
        _ => return Err(invalid("V36 prefix registered object scheme differs")),
    };
    let output = scratch.join(format!("source-{ordinal:04}.parquet"));
    if output.exists() {
        return Err(invalid("V36 prefix acquired object path already exists"));
    }
    let mut temporary =
        tempfile::NamedTempFile::new_in(scratch).map_err(|source| BorsukError::Io {
            path: scratch.to_owned(),
            source,
        })?;
    let (encoded_bytes, sha256) = runtime.block_on(async {
        let result = store.get(&location).await?;
        if result.meta.size != object.encoded_bytes {
            return Err(invalid("V36 prefix registered object length differs"));
        }
        let mut stream = result.into_stream();
        let mut encoded_bytes = 0_u64;
        let mut sha256 = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            encoded_bytes = encoded_bytes
                .checked_add(u64::try_from(chunk.len()).unwrap())
                .ok_or_else(|| invalid("V36 prefix acquired object length overflows"))?;
            if encoded_bytes > object.encoded_bytes {
                return Err(invalid("V36 prefix registered object length differs"));
            }
            temporary
                .write_all(&chunk)
                .map_err(|source| BorsukError::Io {
                    path: output.clone(),
                    source,
                })?;
            sha256.update(&chunk);
        }
        Ok((encoded_bytes, format!("{:x}", sha256.finalize())))
    })?;
    if encoded_bytes != object.encoded_bytes || sha256 != object.sha256 {
        return Err(invalid("V36 prefix registered object authority differs"));
    }
    temporary
        .as_file()
        .sync_all()
        .map_err(|source| BorsukError::Io {
            path: output.clone(),
            source,
        })?;
    temporary
        .persist_noclobber(&output)
        .map_err(|error| BorsukError::Io {
            path: output.clone(),
            source: error.error,
        })?;
    Ok(output)
}

struct V36PrefixAcquiredObjects {
    paths: Vec<PathBuf>,
}

impl V36PrefixAcquiredObjects {
    fn cleanup(mut self) -> Result<()> {
        while let Some(path) = self.paths.pop() {
            if let Err(source) = fs::remove_file(&path) {
                self.paths.push(path.clone());
                return Err(BorsukError::Io { path, source });
            }
        }
        Ok(())
    }
}

impl Drop for V36PrefixAcquiredObjects {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
    }
}

fn output_identity(
    role: &str,
    filename: &str,
    output_prefix: &str,
    path: &Path,
) -> Result<V36ArtifactIdentity> {
    let (encoded_bytes, sha256) = sha256_file(path)?;
    Ok(V36ArtifactIdentity {
        blake3: blake3_file(path)?,
        encoded_bytes,
        role: role.to_owned(),
        sha256,
        uri: format!("{output_prefix}{filename}"),
    })
}

/// Execute one complete bounded V36 diagnostic population freeze locally.
pub fn run_v36_prefix_freeze(request: V36PrefixFreezeRequest) -> Result<()> {
    let mut preflight = load_v36_prefix_freeze_preflight(&request)?;
    crate::validate_v36_prefix_registered_screen_authority(
        &preflight.authority,
        &preflight.registry,
    )?;
    ensure_v36_prefix_complete_window_execution_available(&preflight.authority)?;
    let execution_authority_sha256 = format!(
        "{:x}",
        Sha256::digest(canonical_v36_prefix_freeze_execution_authority_bytes(
            &preflight.execution_authority,
        )?)
    );
    let resume_head = preflight.resume_head.take();
    let prior_runs = resume_head
        .as_ref()
        .map(V36PrefixPopulationCheckpointHead::identity_runs)
        .transpose()?;
    let mut checkpoint_writer = match resume_head {
        Some(head) => V36PrefixPopulationCheckpointWriter::resume(
            &request.checkpoint_outbox,
            preflight.checkpoint_context.clone(),
            execution_authority_sha256,
            preflight.execution_authority.attempt_id.clone(),
            preflight.producer_attempt_ordinal,
            request.producer_instance_id.clone(),
            head,
        )?,
        None => V36PrefixPopulationCheckpointWriter::create(
            &request.checkpoint_outbox,
            preflight.checkpoint_context.clone(),
            execution_authority_sha256,
            preflight.execution_authority.attempt_id.clone(),
            preflight.producer_attempt_ordinal,
            request.producer_instance_id.clone(),
        )?,
    };
    let runtime =
        tokio::runtime::Runtime::new().map_err(|_| invalid("V36 prefix object runtime differs"))?;
    let mut acquired = V36PrefixAcquiredObjects { paths: Vec::new() };
    let mut acquired_by_ordinal = BTreeMap::new();
    let selected_object_start = preflight.authority.selected_object_start;
    let selected_object_count = usize::from(preflight.authority.selected_object_count);
    let selected_object_end = usize::from(selected_object_start)
        .checked_add(selected_object_count)
        .ok_or_else(|| invalid("V36 prefix selected object window overflows"))?;
    let ranked_window = preflight
        .ranked_objects
        .get(usize::from(selected_object_start)..selected_object_end)
        .ok_or_else(|| invalid("V36 prefix selected object window differs"))?;
    let distinct_candidates = usize::try_from(preflight.authority.distinct_candidates)
        .map_err(|_| invalid("V36 prefix distinct row count overflows"))?;
    let scan = {
        let mut acquire = |ordinal, object: &V36PrefixRankedSourceObject| {
            let path = acquire_v36_prefix_object(&runtime, object, &request.scratch, ordinal)?;
            if acquired_by_ordinal.insert(ordinal, path.clone()).is_some() {
                return Err(invalid("V36 prefix acquired object ordinal differs"));
            }
            acquired.paths.push(path.clone());
            Ok(path)
        };
        let mut commit = |boundary: &V36PrefixPopulationCommit| {
            checkpoint_writer.commit(boundary)?;
            Ok(())
        };
        match prior_runs.as_deref() {
            Some(runs) => scan_v36_prefix_object_prefix_resumed(
                ranked_window,
                selected_object_start,
                preflight.authority.source_byte_cap,
                distinct_candidates,
                runs,
                &mut acquire,
                &mut commit,
            )?,
            None => scan_v36_prefix_object_prefix_checkpointed(
                ranked_window,
                selected_object_start,
                preflight.authority.source_byte_cap,
                distinct_candidates,
                &mut acquire,
                &mut commit,
            )?,
        }
    };
    for (local_ordinal, object) in ranked_window
        .iter()
        .enumerate()
        .take(scan.consumed_objects.len())
    {
        let ordinal = selected_object_start
            .checked_add(
                u16::try_from(local_ordinal)
                    .map_err(|_| invalid("V36 prefix acquired object ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V36 prefix acquired object ordinal overflows"))?;
        if let std::collections::btree_map::Entry::Vacant(entry) =
            acquired_by_ordinal.entry(ordinal)
        {
            let path = acquire_v36_prefix_object(&runtime, object, &request.scratch, ordinal)?;
            acquired.paths.push(path.clone());
            entry.insert(path);
        }
    }
    let ordered_paths = acquired_by_ordinal.into_values().collect::<Vec<_>>();
    let population = bind_v36_prefix_population_authority(
        &preflight.authority,
        scan.consumed_objects.clone(),
        &preflight.registry,
    )?;
    let split = select_v36_prefix_roles(scan.unique_rows, &population, &preflight.registry)?;
    let paths = materialize_v36_prefix_role_parquets(
        &ordered_paths,
        ranked_window,
        selected_object_start,
        &split,
        &request.scratch,
        &request.output,
    )?;
    let gt_paths = [
        request.output.join("development-gt100.parquet"),
        request.output.join("validation-gt100.parquet"),
        request.output.join("sealed-holdout-gt100.parquet"),
    ];
    let gt_jobs = [
        V36PrefixGtParquetJob {
            expected_queries: u32::try_from(split.development.len())
                .map_err(|_| invalid("V36 prefix development count overflows"))?,
            output: gt_paths[0].clone(),
            query: paths.development.clone(),
            role: V36PrefixQualityRole::Development,
        },
        V36PrefixGtParquetJob {
            expected_queries: u32::try_from(split.validation.len())
                .map_err(|_| invalid("V36 prefix validation count overflows"))?,
            output: gt_paths[1].clone(),
            query: paths.validation.clone(),
            role: V36PrefixQualityRole::Validation,
        },
        V36PrefixGtParquetJob {
            expected_queries: u32::try_from(split.sealed_holdout.len())
                .map_err(|_| invalid("V36 prefix holdout count overflows"))?,
            output: gt_paths[2].clone(),
            query: paths.sealed_holdout.clone(),
            role: V36PrefixQualityRole::SealedHoldout,
        },
    ];
    let source_feature_ids = split
        .corpus
        .iter()
        .map(|row| row.feature_row_id)
        .collect::<Vec<_>>();
    let gt_stats = write_v36_prefix_gt100_roles_from_parquets(
        &paths.source,
        &source_feature_ids,
        &gt_jobs,
        usize::from(population.workspace_count),
    )?;
    if gt_stats.source_scans != 1
        || gt_stats.source_rows != population.corpus_rows
        || gt_stats.quality_queries
            != u32::try_from(
                split.development.len() + split.validation.len() + split.sealed_holdout.len(),
            )
            .map_err(|_| invalid("V36 prefix quality query count overflows"))?
    {
        return Err(invalid("V36 prefix exact truth execution differs"));
    }

    let population_path = request.output.join("population-authority.json");
    write_atomic_bytes(
        &population_path,
        &canonical_v36_prefix_population_authority_bytes(&population, &preflight.registry)?,
    )?;
    let artifact_paths = [
        (
            "population-authority",
            "population-authority.json",
            &population_path,
        ),
        ("source", "source.parquet", &paths.source),
        (
            "development-query",
            "development-query.parquet",
            &paths.development,
        ),
        (
            "development-gt100",
            "development-gt100.parquet",
            &gt_paths[0],
        ),
        (
            "validation-query",
            "validation-query.parquet",
            &paths.validation,
        ),
        ("validation-gt100", "validation-gt100.parquet", &gt_paths[1]),
        (
            "sealed-holdout-query",
            "sealed-holdout-query.parquet",
            &paths.sealed_holdout,
        ),
        (
            "sealed-holdout-gt100",
            "sealed-holdout-gt100.parquet",
            &gt_paths[2],
        ),
        (
            "performance-query",
            "performance-query.parquet",
            &paths.performance,
        ),
    ];
    let outputs = artifact_paths
        .into_iter()
        .map(|(role, filename, path)| {
            output_identity(
                role,
                filename,
                &preflight.execution_authority.output_prefix,
                path,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let receipt = V36PrefixFreezeReceipt {
        claim_eligible: false,
        cutoff_object_ordinal: scan.cutoff_object_ordinal,
        cutoff_row_offset: scan.cutoff_row_offset,
        distinct_rows_observed: scan.distinct_rows_observed,
        duplicate_rows: scan.duplicate_rows,
        execution_authority_sha256: format!(
            "{:x}",
            Sha256::digest(canonical_v36_prefix_freeze_execution_authority_bytes(
                &preflight.execution_authority,
            )?)
        ),
        freeze_authority_sha256: format!(
            "{:x}",
            Sha256::digest(canonical_v36_prefix_freeze_authority_bytes(
                &preflight.authority,
                &preflight.registry,
            )?)
        ),
        outputs,
        physical_rows: scan.physical_rows,
        population,
        schema: "borsuk-v36-prefix-freeze-receipt-v1".to_owned(),
        source_archive_sha256: preflight
            .execution_authority
            .inputs
            .iter()
            .find(|input| input.role == "source-archive")
            .ok_or_else(|| invalid("V36 prefix source archive authority differs"))?
            .sha256
            .clone(),
        source_registry_sha256: format!(
            "{:x}",
            Sha256::digest(canonical_v36_prefix_source_registry_bytes(
                &preflight.authority,
                &preflight.registry,
            )?)
        ),
    };
    write_atomic_bytes(
        &request.output.join("freeze-receipt.json"),
        &canonical_v36_prefix_freeze_receipt_bytes(
            &receipt,
            &preflight.authority,
            &preflight.execution_authority,
            &preflight.registry,
        )?,
    )?;
    acquired.cleanup()?;
    Ok(())
}

fn ensure_v36_prefix_complete_window_execution_available(
    _authority: &V36PrefixFreezeAuthority,
) -> Result<()> {
    Err(invalid(
        "V36 prefix complete-window population execution is unavailable",
    ))
}

/// Authenticate every local attempt input before any source-object network access.
pub fn load_v36_prefix_freeze_preflight(
    request: &V36PrefixFreezeRequest,
) -> Result<V36PrefixFreezePreflight> {
    if request.output == request.scratch
        || request.output == request.checkpoint_outbox
        || request.scratch == request.checkpoint_outbox
        || request.resume_checkpoint.as_ref().is_some_and(|resume| {
            resume == &request.output
                || resume == &request.scratch
                || resume == &request.checkpoint_outbox
                || !resume.is_dir()
                || resume.is_symlink()
        })
        || !request.output.is_dir()
        || !request.scratch.is_dir()
        || !request.checkpoint_outbox.is_dir()
        || !request.producer_instance_id.starts_with("i-")
        || request.producer_instance_id.len() <= 2
        || !request
            .producer_instance_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        || request
            .output
            .read_dir()
            .map_err(|source| BorsukError::Io {
                path: request.output.clone(),
                source,
            })?
            .next()
            .is_some()
        || request
            .checkpoint_outbox
            .read_dir()
            .map_err(|source| BorsukError::Io {
                path: request.checkpoint_outbox.clone(),
                source,
            })?
            .next()
            .is_some()
        || request
            .scratch
            .read_dir()
            .map_err(|source| BorsukError::Io {
                path: request.scratch.clone(),
                source,
            })?
            .next()
            .is_some()
    {
        return Err(invalid("V36 prefix attempt directories differ"));
    }
    let execution_bytes = read_file(&request.execution_authority)?;
    let execution_authority: V36PrefixFreezeExecutionAuthority =
        serde_json::from_slice(&execution_bytes)
            .map_err(|_| invalid("V36 prefix execution authority JSON differs"))?;
    validate_v36_prefix_freeze_execution_authority(&execution_authority)?;
    if canonical_v36_prefix_freeze_execution_authority_bytes(&execution_authority)?
        != execution_bytes
    {
        return Err(invalid("V36 prefix execution authority bytes differ"));
    }
    let input = |role: &str| {
        execution_authority
            .inputs
            .iter()
            .find(|input| input.role == role)
            .ok_or_else(|| invalid("V36 prefix execution input role differs"))
    };
    authenticate_file(&request.executable, input("binary")?)?;
    authenticate_file(&request.authority, input("freeze-authority")?)?;
    authenticate_file(&request.source_archive, input("source-archive")?)?;
    authenticate_file(&request.source_registry, input("source-registry")?)?;

    let authority_bytes = read_file(&request.authority)?;
    let authority: V36PrefixFreezeAuthority = serde_json::from_slice(&authority_bytes)
        .map_err(|_| invalid("V36 prefix freeze authority JSON differs"))?;
    let registry_bytes = read_file(&request.source_registry)?;
    let registry: Vec<V36PrefixRegisteredSourceObject> = serde_json::from_slice(&registry_bytes)
        .map_err(|_| invalid("V36 prefix source registry JSON differs"))?;
    validate_v36_prefix_freeze_authority(&authority, &registry)?;
    if canonical_v36_prefix_freeze_authority_bytes(&authority, &registry)? != authority_bytes
        || crate::canonical_v36_prefix_source_registry_bytes(&authority, &registry)?
            != registry_bytes
    {
        return Err(invalid("V36 prefix local authority bytes differ"));
    }
    let ranked_objects = rank_v36_prefix_source_objects(&authority, &registry)?;
    let (run_id, attempt_text) = execution_authority
        .attempt_id
        .rsplit_once("-attempt-")
        .ok_or_else(|| invalid("V36 prefix attempt identity differs"))?;
    let producer_attempt_ordinal = attempt_text
        .parse::<u8>()
        .map_err(|_| invalid("V36 prefix attempt ordinal differs"))?;
    if attempt_text.len() != 4
        || execution_authority.attempt_id
            != format!("{run_id}-attempt-{producer_attempt_ordinal:04}")
    {
        return Err(invalid("V36 prefix attempt identity differs"));
    }
    let attempt_suffix = format!("attempt-{producer_attempt_ordinal:04}/");
    let campaign_prefix = execution_authority
        .output_prefix
        .strip_suffix(&attempt_suffix)
        .ok_or_else(|| invalid("V36 prefix checkpoint campaign prefix differs"))?;
    let source_archive_sha256 = input("source-archive")?.sha256.clone();
    let checkpoint_context = V36PrefixCheckpointContext {
        cohort_ordinal: authority.cohort_ordinal,
        corpus_rows: authority.corpus_rows,
        distinct_candidates: authority.distinct_candidates,
        excluded_population_identity: authority.excluded_population_identity.clone(),
        freeze_authority_sha256: format!("{:x}", Sha256::digest(&authority_bytes)),
        gt_block_rows: PARQUET_ROW_GROUP_ROWS as u64,
        object_prefix: format!("{campaign_prefix}checkpoints/objects/"),
        pointer_uri: format!("{campaign_prefix}checkpoints/runs/{run_id}/latest.json"),
        ranked_objects: ranked_objects
            .iter()
            .skip(usize::from(authority.selected_object_start))
            .take(usize::from(authority.selected_object_count))
            .map(|object| V36PrefixRegisteredSourceObject {
                encoded_bytes: object.encoded_bytes,
                path: object.path.clone(),
                sha256: object.sha256.clone(),
                uri: object.uri.clone(),
            })
            .collect(),
        run_id: run_id.to_owned(),
        selected_object_count: authority.selected_object_count,
        selected_object_start: authority.selected_object_start,
        source_archive_sha256,
        source_byte_cap: authority.source_byte_cap,
        source_commit: execution_authority.source_commit.clone(),
        source_registry_sha256: format!("{:x}", Sha256::digest(&registry_bytes)),
    };
    let resume_head = match (&request.resume_checkpoint, &execution_authority.resume) {
        (None, None) => None,
        (Some(root), Some(binding)) => {
            let head = load_v36_prefix_population_checkpoint_head(root, &checkpoint_context)?;
            let pointer: V36PrefixCheckpointPointer =
                serde_json::from_slice(&head.pointer_bytes)
                    .map_err(|_| invalid("V36 prefix resume pointer JSON differs"))?;
            if binding.generation != pointer.generation
                || binding.manifest != pointer.manifest
                || binding.pointer_encoded_bytes != head.pointer_bytes.len() as u64
                || binding.pointer_sha256 != format!("{:x}", Sha256::digest(&head.pointer_bytes))
                || binding.pointer_uri != checkpoint_context.pointer_uri
                || head.manifest.producer_attempt_ordinal >= producer_attempt_ordinal
            {
                return Err(invalid("V36 prefix resume binding differs"));
            }
            Some(head)
        }
        _ => return Err(invalid("V36 prefix resume presence differs")),
    };
    Ok(V36PrefixFreezePreflight {
        authority,
        checkpoint_context,
        execution_authority,
        producer_attempt_ordinal,
        registry,
        ranked_objects,
        resume_head,
    })
}

/// Authenticate and stream one complete registered raw source object.
pub fn scan_v36_prefix_registered_input_parquet<F>(
    path: &Path,
    object: &V36PrefixRankedSourceObject,
    selected_object_ordinal: u16,
    mut consume: F,
) -> Result<u64>
where
    F: FnMut(V36PrefixInputRow) -> Result<()>,
{
    digest_bytes(&object.sha256)?;
    digest_bytes(&object.sample_sha256)?;
    if object.path.is_empty()
        || object.uri.is_empty()
        || object.sample_sha256
            != v36_prefix_object_sample_sha256(&object.path, object.encoded_bytes)
    {
        return Err(invalid("V36 prefix registered object identity differs"));
    }
    let (encoded_bytes, sha256) = sha256_file(path)?;
    if encoded_bytes != object.encoded_bytes || sha256 != object.sha256 {
        return Err(invalid("V36 prefix registered object authority differs"));
    }
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let expected_schema = registered_input_schema();
    validate_parquet_descriptor(builder.parquet_schema(), &expected_schema)?;
    if builder.schema().as_ref() != &expected_schema {
        return Err(invalid("V36 prefix registered object schema differs"));
    }
    let mut row_offset = 0_u64;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.schema().as_ref() != &expected_schema
            || batch.num_columns() != 4
            || batch.num_rows() == 0
        {
            return Err(invalid("V36 prefix registered object batch differs"));
        }
        let ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("V36 prefix registered object ID column differs"))?;
        let embeddings = batch
            .column(3)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V36 prefix registered object embedding column differs"))?;
        let values = embeddings
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V36 prefix registered object embedding child differs"))?;
        if ids.null_count() != 0
            || embeddings.null_count() != 0
            || values.null_count() != 0
            || values.len() != batch.num_rows() * DIMENSIONS
        {
            return Err(invalid("V36 prefix registered object gated null differs"));
        }
        for row in 0..batch.num_rows() {
            let start = row * DIMENSIONS;
            let input = V36PrefixInputRow {
                feature_row_id: ids.value(row),
                selected_object_ordinal,
                row_offset,
                embedding: values.values()[start..start + DIMENSIONS].to_vec(),
            };
            validate_v36_prefix_input_row(&input)?;
            consume(input)?;
            row_offset = row_offset
                .checked_add(1)
                .ok_or_else(|| invalid("V36 prefix registered object row count overflows"))?;
        }
    }
    if row_offset == 0 {
        return Err(invalid("V36 prefix registered object is empty"));
    }
    Ok(row_offset)
}

fn blake3_file(path: &Path) -> Result<String> {
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut hasher = blake3::Hasher::new();
    loop {
        let read = reader.read(&mut buffer).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn scan_v36_prefix_object_prefix_from_state<F, C>(
    ranked_objects: &[V36PrefixRankedSourceObject],
    selected_object_start: u16,
    byte_cap: u64,
    distinct_candidates: usize,
    restored: Option<V36PrefixRestoredPopulation>,
    mut acquire: F,
    mut commit: C,
) -> Result<V36PrefixObjectPrefixScan>
where
    F: FnMut(u16, &V36PrefixRankedSourceObject) -> Result<PathBuf>,
    C: FnMut(&V36PrefixPopulationCommit) -> Result<()>,
{
    if ranked_objects.is_empty()
        || ranked_objects.len() > 16
        || byte_cap == 0
        || distinct_candidates == 0
        || selected_object_start
            .checked_add(
                u16::try_from(ranked_objects.len())
                    .map_err(|_| invalid("V36 prefix source window count overflows"))?,
            )
            .is_none()
    {
        return Err(invalid("V36 prefix source scan limits differ"));
    }
    let restored = restored.unwrap_or(V36PrefixRestoredPopulation {
        consumed_objects: Vec::new(),
        cutoff: None,
        distinct_rows_observed: 0,
        duplicate_rows: 0,
        next_object_ordinal: selected_object_start,
        physical_rows: 0,
        unique_rows: Vec::new(),
    });
    let start = restored
        .next_object_ordinal
        .checked_sub(selected_object_start)
        .map(usize::from)
        .ok_or_else(|| invalid("V36 prefix restored source authority differs"))?;
    if start > ranked_objects.len()
        || restored.consumed_objects.len() != start
        || restored
            .consumed_objects
            .iter()
            .zip(ranked_objects)
            .any(|(source, ranked)| {
                source.encoded_bytes != ranked.encoded_bytes
                    || source.path != ranked.path
                    || source.sample_sha256 != ranked.sample_sha256
                    || source.sha256 != ranked.sha256
                    || source.uri != ranked.uri
            })
    {
        return Err(invalid("V36 prefix restored source authority differs"));
    }
    let mut consumed_objects = restored.consumed_objects;
    let mut seen = restored
        .unique_rows
        .iter()
        .map(|row| row.feature_row_id)
        .collect::<HashSet<_>>();
    if seen.len() != restored.unique_rows.len()
        || u64::try_from(seen.len()).unwrap_or(u64::MAX) != restored.distinct_rows_observed
    {
        return Err(invalid("V36 prefix restored identity authority differs"));
    }
    let mut unique_rows = restored.unique_rows;
    let mut physical_rows = restored.physical_rows;
    let mut encoded_bytes = consumed_objects.iter().try_fold(0_u64, |total, object| {
        total
            .checked_add(object.encoded_bytes)
            .ok_or_else(|| invalid("V36 prefix source scan bytes overflow"))
    })?;
    if encoded_bytes > byte_cap {
        return Err(invalid(
            "V36 prefix complete source window exceeds byte cap",
        ));
    }
    let mut cutoff = restored.cutoff;
    for (ordinal, object) in ranked_objects.iter().enumerate().skip(start) {
        encoded_bytes = encoded_bytes
            .checked_add(object.encoded_bytes)
            .ok_or_else(|| invalid("V36 prefix source scan bytes overflow"))?;
        if encoded_bytes > byte_cap {
            return Err(invalid(
                "V36 prefix complete source window exceeds byte cap",
            ));
        }
        let selected_object_ordinal = selected_object_start
            .checked_add(
                u16::try_from(ordinal)
                    .map_err(|_| invalid("V36 prefix source object ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V36 prefix source object ordinal overflows"))?;
        let path = acquire(selected_object_ordinal, object)?;
        let mut object_seen = HashSet::new();
        let mut object_identities = Vec::new();
        let object_rows = scan_v36_prefix_registered_input_parquet(
            &path,
            object,
            selected_object_ordinal,
            |row| {
                let identity = validate_v36_prefix_input_row(&row)?;
                if !seen.contains(&identity.feature_row_id)
                    && object_seen.insert(identity.feature_row_id)
                {
                    object_identities.push(identity);
                }
                Ok(())
            },
        )?;
        if object_rows == 0 {
            return Err(invalid("V36 prefix source object is empty"));
        }
        let source = crate::V36PrefixSourceObject {
            blake3: blake3_file(&path)?,
            encoded_bytes: object.encoded_bytes,
            path: object.path.clone(),
            sample_sha256: object.sample_sha256.clone(),
            sha256: object.sha256.clone(),
            uri: object.uri.clone(),
        };
        physical_rows = physical_rows
            .checked_add(object_rows)
            .ok_or_else(|| invalid("V36 prefix physical rows overflow"))?;
        let previous_distinct = seen.len();
        for identity in &object_identities {
            if !seen.insert(identity.feature_row_id) {
                return Err(invalid("V36 prefix provisional identity commit differs"));
            }
            unique_rows.push(identity.clone());
            if unique_rows.len() == distinct_candidates {
                cutoff = Some((selected_object_ordinal, identity.row_offset));
            }
        }
        consumed_objects.push(source.clone());
        let distinct_rows = u64::try_from(seen.len()).unwrap_or(u64::MAX);
        let duplicate_rows = physical_rows
            .checked_sub(distinct_rows)
            .ok_or_else(|| invalid("V36 prefix duplicate rows underflow"))?;
        let boundary_cutoff =
            if previous_distinct < distinct_candidates && seen.len() >= distinct_candidates {
                cutoff
            } else {
                None
            };
        let boundary = V36PrefixPopulationCommit {
            cutoff: boundary_cutoff,
            distinct_rows,
            duplicate_rows,
            physical_rows,
            run: V36PrefixIdentityRun {
                physical_rows: object_rows,
                rows: object_identities,
                selected_object_ordinal,
                source,
            },
        };
        validate_v36_prefix_identity_run(&boundary.run)?;
        commit(&boundary)?;
    }
    let (cutoff_object_ordinal, cutoff_row_offset) =
        cutoff.ok_or(BorsukError::V36PrefixSourceInsufficient)?;
    validate_v36_prefix_cutoff_membership_from_start(
        &unique_rows,
        selected_object_start,
        consumed_objects.len(),
        distinct_candidates,
    )?;
    let distinct_rows_observed = u64::try_from(seen.len()).unwrap_or(u64::MAX);
    Ok(V36PrefixObjectPrefixScan {
        consumed_objects,
        cutoff_object_ordinal,
        cutoff_row_offset,
        distinct_rows_observed,
        duplicate_rows: physical_rows
            .checked_sub(distinct_rows_observed)
            .ok_or_else(|| invalid("V36 prefix duplicate rows underflow"))?,
        physical_rows,
        unique_rows,
    })
}

/// Scan authenticated objects and emit a durable boundary only after each completes.
pub fn scan_v36_prefix_object_prefix_checkpointed<F, C>(
    ranked_objects: &[V36PrefixRankedSourceObject],
    selected_object_start: u16,
    byte_cap: u64,
    distinct_candidates: usize,
    acquire: F,
    commit: C,
) -> Result<V36PrefixObjectPrefixScan>
where
    F: FnMut(u16, &V36PrefixRankedSourceObject) -> Result<PathBuf>,
    C: FnMut(&V36PrefixPopulationCommit) -> Result<()>,
{
    scan_v36_prefix_object_prefix_from_state(
        ranked_objects,
        selected_object_start,
        byte_cap,
        distinct_candidates,
        None,
        acquire,
        commit,
    )
}

/// Resume a source scan strictly after an authenticated complete-object prefix.
pub fn scan_v36_prefix_object_prefix_resumed<F, C>(
    ranked_objects: &[V36PrefixRankedSourceObject],
    selected_object_start: u16,
    byte_cap: u64,
    distinct_candidates: usize,
    prior_runs: &[V36PrefixIdentityRun],
    acquire: F,
    commit: C,
) -> Result<V36PrefixObjectPrefixScan>
where
    F: FnMut(u16, &V36PrefixRankedSourceObject) -> Result<PathBuf>,
    C: FnMut(&V36PrefixPopulationCommit) -> Result<()>,
{
    let restored = restore_v36_prefix_population_state(prior_runs, distinct_candidates)?;
    scan_v36_prefix_object_prefix_from_state(
        ranked_objects,
        selected_object_start,
        byte_cap,
        distinct_candidates,
        Some(restored),
        acquire,
        commit,
    )
}

/// Scan every authenticated object in one complete registered window.
pub fn scan_v36_prefix_object_prefix<F>(
    ranked_objects: &[V36PrefixRankedSourceObject],
    selected_object_start: u16,
    byte_cap: u64,
    distinct_candidates: usize,
    acquire: F,
) -> Result<V36PrefixObjectPrefixScan>
where
    F: FnMut(u16, &V36PrefixRankedSourceObject) -> Result<PathBuf>,
{
    scan_v36_prefix_object_prefix_checkpointed(
        ranked_objects,
        selected_object_start,
        byte_cap,
        distinct_candidates,
        acquire,
        |_| Ok(()),
    )
}

/// Exact physical schema of a V36 prefix source table.
pub fn v36_prefix_source_schema() -> Schema {
    Schema::new(vec![
        Field::new("feature_row_id", DataType::UInt64, false),
        vector_field(),
    ])
}

/// Exact physical schema of a V36 prefix query table.
pub fn v36_prefix_query_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("feature_row_id", DataType::UInt64, false),
        vector_field(),
    ])
}

/// Exact physical schema of a V36 prefix exact-GT table.
pub fn v36_prefix_gt100_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("rank", DataType::UInt16, false),
        Field::new("feature_row_id", DataType::UInt64, false),
        Field::new("squared_distance", DataType::Float64, false),
    ])
}

fn validate_expected_feature_ids(expected: &[u64]) -> Result<()> {
    if expected.is_empty() {
        return Err(invalid("V36 prefix source membership is empty"));
    }
    let mut sorted = expected.to_vec();
    sorted.sort_unstable();
    if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("V36 prefix source membership overlaps"));
    }
    Ok(())
}

fn validate_source_batch(
    batch: &RecordBatch,
    expected_feature_ids: &[u64],
    next_ordinal: &mut usize,
) -> Result<()> {
    if batch.schema().as_ref() != &v36_prefix_source_schema()
        || batch.num_rows() == 0
        || batch.num_columns() != 2
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V36 prefix source Parquet batch differs"));
    }
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 prefix source Parquet ID column differs"))?;
    let embeddings = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V36 prefix source Parquet embedding column differs"))?;
    let values = embeddings
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V36 prefix source Parquet embedding child differs"))?;
    if values.null_count() != 0 || values.len() != batch.num_rows() * DIMENSIONS {
        return Err(invalid("V36 prefix source Parquet embedding shape differs"));
    }
    for row in 0..batch.num_rows() {
        if expected_feature_ids.get(*next_ordinal).copied() != Some(ids.value(row)) {
            return Err(invalid("V36 prefix source Parquet membership differs"));
        }
        let start = row * DIMENSIONS;
        validate_embedding(&values.values()[start..start + DIMENSIONS])?;
        *next_ordinal = next_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("V36 prefix source Parquet row count overflows"))?;
    }
    Ok(())
}

const MATERIALIZATION_BUCKET_ROWS: usize = 8_192;
const MATERIALIZATION_RECORD_BYTES: usize = 16 + DIMENSIONS * 4;

fn write_materialization_record<W: Write>(
    writer: &mut W,
    path: &Path,
    ordinal: usize,
    row: &V36PrefixMaterializedRow,
) -> Result<()> {
    validate_embedding(&row.embedding)?;
    let mut record = [0_u8; MATERIALIZATION_RECORD_BYTES];
    record[..8].copy_from_slice(
        &u64::try_from(ordinal)
            .map_err(|_| invalid("V36 prefix materialization ordinal overflows"))?
            .to_le_bytes(),
    );
    record[8..16].copy_from_slice(&row.feature_row_id.to_le_bytes());
    for (encoded, value) in record[16..]
        .as_chunks_mut::<4>()
        .0
        .iter_mut()
        .zip(&row.embedding)
    {
        *encoded = value.to_bits().to_le_bytes();
    }
    writer.write_all(&record).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })
}

fn materialization_rows(split: &V36PrefixRoleSplit) -> [&[V36PrefixRowIdentity]; 5] {
    [
        &split.corpus,
        &split.development,
        &split.validation,
        &split.sealed_holdout,
        &split.performance,
    ]
}

fn read_materialization_bucket(path: &Path) -> Result<Vec<(u64, V36PrefixMaterializedRow)>> {
    let bytes = read_file(path)?;
    if bytes.is_empty() || bytes.len() % MATERIALIZATION_RECORD_BYTES != 0 {
        return Err(invalid("V36 prefix materialization spool differs"));
    }
    let mut rows = Vec::with_capacity(bytes.len() / MATERIALIZATION_RECORD_BYTES);
    for record in bytes.as_chunks::<MATERIALIZATION_RECORD_BYTES>().0 {
        let ordinal = u64::from_le_bytes(record[..8].try_into().unwrap());
        let feature_row_id = u64::from_le_bytes(record[8..16].try_into().unwrap());
        let mut embedding = Vec::with_capacity(DIMENSIONS);
        for component in record[16..].as_chunks::<4>().0 {
            embedding.push(f32::from_bits(u32::from_le_bytes(*component)));
        }
        validate_embedding(&embedding)?;
        rows.push((
            ordinal,
            V36PrefixMaterializedRow {
                feature_row_id,
                source_ordinal: Some(ordinal),
                embedding,
            },
        ));
    }
    rows.sort_by_key(|(ordinal, _)| *ordinal);
    Ok(rows)
}

fn materialized_batch(
    rows: &[(u64, V36PrefixMaterializedRow)],
    query: bool,
) -> Result<RecordBatch> {
    let ordinals = rows
        .iter()
        .map(|(ordinal, _)| u32::try_from(*ordinal))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| invalid("V36 prefix materialization ordinal overflows"))?;
    let ids = rows
        .iter()
        .map(|(_, row)| row.feature_row_id)
        .collect::<Vec<_>>();
    let values = rows
        .iter()
        .flat_map(|(_, row)| row.embedding.iter().copied())
        .collect::<Vec<_>>();
    let embeddings = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        DIMENSIONS as i32,
        Arc::new(Float32Array::from(values)),
        None,
    )?;
    let mut columns = Vec::<ArrayRef>::new();
    let schema = if query {
        columns.push(Arc::new(UInt32Array::from(ordinals)));
        columns.push(Arc::new(UInt64Array::from(ids)));
        v36_prefix_query_schema()
    } else {
        columns.push(Arc::new(UInt64Array::from(ids)));
        v36_prefix_source_schema()
    };
    columns.push(Arc::new(embeddings));
    Ok(RecordBatch::try_new(Arc::new(schema), columns)?)
}

fn write_spooled_output(
    path: &Path,
    rows: &[V36PrefixRowIdentity],
    buckets: &[PathBuf],
    query: bool,
) -> Result<()> {
    let schema = if query {
        v36_prefix_query_schema()
    } else {
        v36_prefix_source_schema()
    };
    let mut temporary = temporary_output(path)?;
    let mut writer = ArrowWriter::try_new(
        temporary.as_file_mut(),
        Arc::new(schema),
        Some(parquet_writer_properties()),
    )?;
    let expected = rows
        .iter()
        .map(|row| row.feature_row_id)
        .collect::<Vec<_>>();
    let mut source_ordinal = 0_usize;
    let mut query_ordinal = 0_u64;
    let mut query_ids = BTreeSet::new();
    for bucket in buckets {
        let decoded = read_materialization_bucket(bucket)?;
        let batch = materialized_batch(&decoded, query)?;
        if query {
            validate_query_batch(&batch, &mut query_ordinal, &mut query_ids)?;
        } else {
            validate_source_batch(&batch, &expected, &mut source_ordinal)?;
        }
        writer.write(&batch)?;
    }
    if (query && query_ordinal != rows.len() as u64) || (!query && source_ordinal != rows.len()) {
        return Err(invalid("V36 prefix materialization row count differs"));
    }
    writer.close()?;
    publish_output(temporary, path)
}

/// Materialize canonical source and query Parquet using bounded ordinal buckets.
pub fn materialize_v36_prefix_role_parquets(
    source_paths: &[PathBuf],
    ranked_objects: &[V36PrefixRankedSourceObject],
    selected_object_start: u16,
    split: &V36PrefixRoleSplit,
    scratch: &Path,
    output: &Path,
) -> Result<V36PrefixRoleParquetPaths> {
    if source_paths.len() != ranked_objects.len()
        || source_paths.is_empty()
        || !scratch.is_dir()
        || !output.is_dir()
    {
        return Err(invalid("V36 prefix materialization inputs differ"));
    }
    let role_rows = materialization_rows(split);
    if role_rows.iter().any(|rows| rows.is_empty()) {
        return Err(invalid("V36 prefix materialization role is empty"));
    }
    let mut destinations = HashMap::new();
    for (role, rows) in role_rows.iter().enumerate() {
        for (ordinal, row) in rows.iter().enumerate() {
            if (role == 0 && row.source_ordinal != Some(ordinal as u64))
                || (role != 0 && row.source_ordinal.is_some())
                || destinations
                    .insert(
                        (row.selected_object_ordinal, row.row_offset),
                        (role, ordinal, row.feature_row_id),
                    )
                    .is_some()
            {
                return Err(invalid("V36 prefix materialization membership differs"));
            }
        }
    }
    let mut spool_paths = Vec::new();
    let mut spools = Vec::new();
    for (role, rows) in role_rows.iter().enumerate() {
        let mut role_paths = Vec::new();
        let mut role_files = Vec::new();
        for bucket in 0..rows.len().div_ceil(MATERIALIZATION_BUCKET_ROWS) {
            let path = scratch.join(format!("role-{role}-bucket-{bucket:06}.bin"));
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|source| BorsukError::Io {
                    path: path.clone(),
                    source,
                })?;
            role_paths.push(path);
            role_files.push(BufWriter::new(file));
        }
        spool_paths.push(role_paths);
        spools.push(role_files);
    }
    for (local_ordinal, (path, object)) in source_paths.iter().zip(ranked_objects).enumerate() {
        let object_ordinal = selected_object_start
            .checked_add(
                u16::try_from(local_ordinal)
                    .map_err(|_| invalid("V36 prefix materialization object ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V36 prefix materialization object ordinal overflows"))?;
        scan_v36_prefix_registered_input_parquet(path, object, object_ordinal, |row| {
            if let Some((role, ordinal, feature_row_id)) =
                destinations.remove(&(row.selected_object_ordinal, row.row_offset))
            {
                if feature_row_id != u64::try_from(row.feature_row_id).unwrap_or(u64::MAX) {
                    return Err(invalid("V36 prefix materialization feature ID differs"));
                }
                let file = &mut spools[role][ordinal / MATERIALIZATION_BUCKET_ROWS];
                write_materialization_record(
                    file,
                    &spool_paths[role][ordinal / MATERIALIZATION_BUCKET_ROWS],
                    ordinal,
                    &V36PrefixMaterializedRow {
                        feature_row_id,
                        source_ordinal: Some(ordinal as u64),
                        embedding: row.embedding,
                    },
                )?;
            }
            Ok(())
        })?;
    }
    if !destinations.is_empty() {
        return Err(invalid("V36 prefix materialization row is missing"));
    }
    for (role, role_spools) in spools.iter_mut().enumerate() {
        for (bucket, spool) in role_spools.iter_mut().enumerate() {
            spool.flush().map_err(|source| BorsukError::Io {
                path: spool_paths[role][bucket].clone(),
                source,
            })?;
        }
    }
    drop(spools);
    let names = [
        "source.parquet",
        "development-query.parquet",
        "validation-query.parquet",
        "sealed-holdout-query.parquet",
        "performance-query.parquet",
    ];
    let mut outputs = Vec::new();
    for (role, (rows, name)) in role_rows.iter().zip(names).enumerate() {
        let path = output.join(name);
        write_spooled_output(&path, rows, &spool_paths[role], role != 0)?;
        outputs.push(path);
    }
    for role in spool_paths {
        for path in role {
            fs::remove_file(&path).map_err(|source| BorsukError::Io { path, source })?;
        }
    }
    let [source, development, validation, sealed_holdout, performance] = outputs
        .try_into()
        .map_err(|_| invalid("V36 prefix materialization outputs differ"))?;
    Ok(V36PrefixRoleParquetPaths {
        development,
        performance,
        sealed_holdout,
        source,
        validation,
    })
}

/// Write validated source batches without retaining the complete corpus in RAM.
pub fn write_v36_prefix_source_parquet<I>(
    path: &Path,
    expected_feature_ids: &[u64],
    batches: I,
) -> Result<()>
where
    I: IntoIterator<Item = RecordBatch>,
{
    validate_expected_feature_ids(expected_feature_ids)?;
    let mut temporary = temporary_output(path)?;
    let mut writer = ArrowWriter::try_new(
        temporary.as_file_mut(),
        Arc::new(v36_prefix_source_schema()),
        Some(parquet_writer_properties()),
    )?;
    let mut next_ordinal = 0_usize;
    for batch in batches {
        validate_source_batch(&batch, expected_feature_ids, &mut next_ordinal)?;
        writer.write(&batch)?;
    }
    if next_ordinal != expected_feature_ids.len() {
        return Err(invalid("V36 prefix source Parquet row count differs"));
    }
    writer.close()?;
    publish_output(temporary, path)?;
    Ok(())
}

/// Stream and validate a complete source Parquet artifact one batch at a time.
pub fn scan_v36_prefix_source_parquet<F>(
    path: &Path,
    expected_feature_ids: &[u64],
    mut consume: F,
) -> Result<()>
where
    F: FnMut(RecordBatch) -> Result<()>,
{
    validate_expected_feature_ids(expected_feature_ids)?;
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    validate_parquet_descriptor(builder.parquet_schema(), &v36_prefix_source_schema())?;
    if builder.schema().as_ref() != &v36_prefix_source_schema() {
        return Err(invalid("V36 prefix source Parquet physical schema differs"));
    }
    let mut next_ordinal = 0_usize;
    for batch in builder.build()? {
        let batch = batch?;
        validate_source_batch(&batch, expected_feature_ids, &mut next_ordinal)?;
        consume(batch)?;
    }
    if next_ordinal != expected_feature_ids.len() {
        return Err(invalid("V36 prefix source Parquet row count differs"));
    }
    Ok(())
}

fn validate_query_batch(
    batch: &RecordBatch,
    next_ordinal: &mut u64,
    feature_ids: &mut BTreeSet<u64>,
) -> Result<()> {
    if batch.schema().as_ref() != &v36_prefix_query_schema()
        || batch.num_rows() == 0
        || batch.num_columns() != 3
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V36 prefix query Parquet batch differs"));
    }
    let ordinals = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::UInt32Array>()
        .ok_or_else(|| invalid("V36 prefix query Parquet ordinal column differs"))?;
    let ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 prefix query Parquet ID column differs"))?;
    let embeddings = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V36 prefix query Parquet embedding column differs"))?;
    let values = embeddings
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V36 prefix query Parquet embedding child differs"))?;
    if values.null_count() != 0 || values.len() != batch.num_rows() * DIMENSIONS {
        return Err(invalid("V36 prefix query Parquet embedding shape differs"));
    }
    for row in 0..batch.num_rows() {
        if u64::from(ordinals.value(row)) != *next_ordinal || !feature_ids.insert(ids.value(row)) {
            return Err(invalid("V36 prefix query Parquet ordering differs"));
        }
        let start = row * DIMENSIONS;
        validate_embedding(&values.values()[start..start + DIMENSIONS])?;
        *next_ordinal = next_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("V36 prefix query Parquet row count overflows"))?;
    }
    Ok(())
}

/// Write validated query batches without retaining all query vectors in RAM.
pub fn write_v36_prefix_query_parquet<I>(path: &Path, batches: I) -> Result<()>
where
    I: IntoIterator<Item = RecordBatch>,
{
    let mut temporary = temporary_output(path)?;
    let mut writer = ArrowWriter::try_new(
        temporary.as_file_mut(),
        Arc::new(v36_prefix_query_schema()),
        Some(parquet_writer_properties()),
    )?;
    let mut feature_ids = BTreeSet::new();
    let mut next_ordinal = 0_u64;
    for batch in batches {
        validate_query_batch(&batch, &mut next_ordinal, &mut feature_ids)?;
        writer.write(&batch)?;
    }
    if next_ordinal == 0 {
        return Err(invalid("V36 prefix query Parquet is empty"));
    }
    writer.close()?;
    publish_output(temporary, path)?;
    Ok(())
}

/// Stream and validate a complete query Parquet artifact.
pub fn scan_v36_prefix_query_parquet<F>(
    path: &Path,
    expected_rows: u64,
    mut consume: F,
) -> Result<()>
where
    F: FnMut(RecordBatch) -> Result<()>,
{
    if expected_rows == 0 {
        return Err(invalid("V36 prefix query Parquet row count differs"));
    }
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    validate_parquet_descriptor(builder.parquet_schema(), &v36_prefix_query_schema())?;
    if builder.schema().as_ref() != &v36_prefix_query_schema() {
        return Err(invalid("V36 prefix query Parquet physical schema differs"));
    }
    let mut feature_ids = BTreeSet::new();
    let mut next_ordinal = 0_u64;
    for batch in builder.build()? {
        let batch = batch?;
        validate_query_batch(&batch, &mut next_ordinal, &mut feature_ids)?;
        consume(batch)?;
    }
    if next_ordinal != expected_rows {
        return Err(invalid("V36 prefix query Parquet row count differs"));
    }
    Ok(())
}

#[derive(Default)]
struct GtValidationState {
    next_query: u32,
    next_rank: u16,
    prior: Option<(f64, u64)>,
    feature_ids: BTreeSet<u64>,
    rows: u64,
}

fn validate_gt_batch(batch: &RecordBatch, state: &mut GtValidationState) -> Result<()> {
    if batch.schema().as_ref() != &v36_prefix_gt100_schema()
        || batch.num_rows() == 0
        || batch.num_columns() != 4
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V36 prefix GT Parquet batch differs"));
    }
    let queries = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::UInt32Array>()
        .ok_or_else(|| invalid("V36 prefix GT query column differs"))?;
    let ranks = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::UInt16Array>()
        .ok_or_else(|| invalid("V36 prefix GT rank column differs"))?;
    let ids = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 prefix GT ID column differs"))?;
    let distances = batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .ok_or_else(|| invalid("V36 prefix GT distance column differs"))?;
    for row in 0..batch.num_rows() {
        let distance = distances.value(row);
        let id = ids.value(row);
        if queries.value(row) != state.next_query
            || ranks.value(row) != state.next_rank
            || !distance.is_finite()
            || distance < 0.0
            || !state.feature_ids.insert(id)
            || state
                .prior
                .is_some_and(|prior| prior.0.total_cmp(&distance).then(prior.1.cmp(&id)).is_gt())
        {
            return Err(invalid("V36 prefix GT ordering differs"));
        }
        state.rows = state
            .rows
            .checked_add(1)
            .ok_or_else(|| invalid("V36 prefix GT row count overflows"))?;
        if usize::from(state.next_rank) + 1 == GT_NEIGHBORS {
            state.next_query = state
                .next_query
                .checked_add(1)
                .ok_or_else(|| invalid("V36 prefix GT query count overflows"))?;
            state.next_rank = 0;
            state.prior = None;
            state.feature_ids.clear();
        } else {
            state.next_rank += 1;
            state.prior = Some((distance, id));
        }
    }
    Ok(())
}

/// Write validated exact-GT batches.
pub fn write_v36_prefix_gt100_parquet<I>(path: &Path, batches: I) -> Result<()>
where
    I: IntoIterator<Item = RecordBatch>,
{
    let mut temporary = temporary_output(path)?;
    let mut writer = ArrowWriter::try_new(
        temporary.as_file_mut(),
        Arc::new(v36_prefix_gt100_schema()),
        Some(parquet_writer_properties()),
    )?;
    let mut state = GtValidationState::default();
    for batch in batches {
        validate_gt_batch(&batch, &mut state)?;
        writer.write(&batch)?;
    }
    if state.rows == 0 || state.next_rank != 0 {
        return Err(invalid("V36 prefix GT row count differs"));
    }
    writer.close()?;
    publish_output(temporary, path)?;
    Ok(())
}

/// Stream and validate exact GT@100 for an exact query count.
pub fn scan_v36_prefix_gt100_parquet<F>(
    path: &Path,
    expected_queries: u32,
    mut consume: F,
) -> Result<()>
where
    F: FnMut(RecordBatch) -> Result<()>,
{
    if expected_queries == 0 {
        return Err(invalid("V36 prefix GT query count differs"));
    }
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    validate_parquet_descriptor(builder.parquet_schema(), &v36_prefix_gt100_schema())?;
    if builder.schema().as_ref() != &v36_prefix_gt100_schema() {
        return Err(invalid("V36 prefix GT physical schema differs"));
    }
    let mut state = GtValidationState::default();
    for batch in builder.build()? {
        let batch = batch?;
        validate_gt_batch(&batch, &mut state)?;
        consume(batch)?;
    }
    if state.next_query != expected_queries || state.next_rank != 0 {
        return Err(invalid("V36 prefix GT query count differs"));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct RankedNeighbor {
    distance: f64,
    feature_row_id: u64,
}

impl PartialEq for RankedNeighbor {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits()
            && self.feature_row_id == other.feature_row_id
    }
}

impl Eq for RankedNeighbor {}

impl PartialOrd for RankedNeighbor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedNeighbor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then(self.feature_row_id.cmp(&other.feature_row_id))
    }
}

fn squared_l2(left: &[f32], right: &[f32]) -> f64 {
    left.iter().zip(right).fold(0.0_f64, |sum, (left, right)| {
        let delta = f64::from(*left) - f64::from(*right);
        let squared = delta * delta;
        sum + squared
    })
}

/// Bounded exact-GT state that consumes the corpus in source-ordinal batches.
pub struct V36PrefixGtAccumulator {
    queries: Vec<V36PrefixQueryRow>,
    query_ids: BTreeSet<u64>,
    corpus_ids: BTreeSet<u64>,
    heaps: Vec<BinaryHeap<RankedNeighbor>>,
    next_source_ordinal: u64,
}

impl V36PrefixGtAccumulator {
    /// Create a GT tile accumulator for one quality-query role.
    pub fn new(_role: V36PrefixQualityRole, queries: Vec<V36PrefixQueryRow>) -> Result<Self> {
        let first_ordinal = queries.first().map(|row| row.query_ordinal);
        if queries.is_empty()
            || queries.iter().enumerate().any(|(ordinal, row)| {
                first_ordinal
                    .and_then(|first| first.checked_add(u32::try_from(ordinal).unwrap_or(u32::MAX)))
                    != Some(row.query_ordinal)
                    || validate_embedding(&row.embedding).is_err()
            })
        {
            return Err(invalid("V36 prefix exact truth query input differs"));
        }
        let query_ids = queries
            .iter()
            .map(|query| query.feature_row_id)
            .collect::<BTreeSet<_>>();
        if query_ids.len() != queries.len() {
            return Err(invalid("V36 prefix exact truth query membership differs"));
        }
        let heaps = (0..queries.len())
            .map(|_| BinaryHeap::with_capacity(GT_NEIGHBORS))
            .collect();
        Ok(Self {
            queries,
            query_ids,
            corpus_ids: BTreeSet::new(),
            heaps,
            next_source_ordinal: 0,
        })
    }

    /// Absorb one validated, source-ordered corpus batch.
    pub fn absorb(&mut self, corpus: &[V36PrefixMaterializedRow]) -> Result<()> {
        for row in corpus {
            if row.source_ordinal != Some(self.next_source_ordinal)
                || validate_embedding(&row.embedding).is_err()
                || self.query_ids.contains(&row.feature_row_id)
                || !self.corpus_ids.insert(row.feature_row_id)
            {
                return Err(invalid("V36 prefix exact truth corpus input differs"));
            }
            for (query, heap) in self.queries.iter().zip(&mut self.heaps) {
                let candidate = RankedNeighbor {
                    distance: squared_l2(&row.embedding, &query.embedding),
                    feature_row_id: row.feature_row_id,
                };
                if heap.len() < GT_NEIGHBORS {
                    heap.push(candidate);
                } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                    heap.pop();
                    heap.push(candidate);
                }
            }
            self.next_source_ordinal = self
                .next_source_ordinal
                .checked_add(1)
                .ok_or_else(|| invalid("V36 prefix exact truth corpus size overflows"))?;
        }
        Ok(())
    }

    /// Finish one complete GT tile in `(query_ordinal,rank)` order.
    pub fn finish(self) -> Result<Vec<V36PrefixGtNeighbor>> {
        if self.next_source_ordinal < GT_NEIGHBORS as u64
            || self.heaps.iter().any(|heap| heap.len() != GT_NEIGHBORS)
        {
            return Err(invalid("V36 prefix exact truth corpus is insufficient"));
        }
        let mut truth = Vec::with_capacity(self.queries.len() * GT_NEIGHBORS);
        for (query, heap) in self.queries.iter().zip(self.heaps) {
            let mut neighbors = heap.into_vec();
            neighbors.sort();
            truth.extend(neighbors.into_iter().enumerate().map(|(rank, neighbor)| {
                V36PrefixGtNeighbor {
                    query_ordinal: query.query_ordinal,
                    rank: u16::try_from(rank).unwrap(),
                    feature_row_id: neighbor.feature_row_id,
                    squared_distance: neighbor.distance,
                }
            }));
        }
        Ok(truth)
    }
}

/// Exact binary64 no-explicit-FMA GT@100 for quality queries.
pub fn exact_v36_prefix_gt100(
    role: V36PrefixQualityRole,
    corpus: &[V36PrefixMaterializedRow],
    queries: &[V36PrefixQueryRow],
) -> Result<Vec<V36PrefixGtNeighbor>> {
    let mut accumulator = V36PrefixGtAccumulator::new(role, queries.to_vec())?;
    accumulator.absorb(corpus)?;
    accumulator.finish()
}

fn v36_prefix_query_rows_from_batch(
    batch: &RecordBatch,
    offset: usize,
    rows: usize,
) -> Result<Vec<V36PrefixQueryRow>> {
    let ordinals = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V36 prefix query Parquet ordinal column differs"))?;
    let ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 prefix query Parquet ID column differs"))?;
    let embeddings = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V36 prefix query Parquet embedding column differs"))?;
    let values = embeddings
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V36 prefix query Parquet embedding child differs"))?;
    (offset..offset + rows)
        .map(|row| {
            let start = row * DIMENSIONS;
            let embedding = values.values()[start..start + DIMENSIONS].to_vec();
            validate_embedding(&embedding)?;
            Ok(V36PrefixQueryRow {
                query_ordinal: ordinals.value(row),
                feature_row_id: ids.value(row),
                embedding,
            })
        })
        .collect()
}

fn v36_prefix_source_rows_from_batch(
    batch: &RecordBatch,
    next_source_ordinal: &mut u64,
) -> Result<Vec<V36PrefixMaterializedRow>> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 prefix source Parquet ID column differs"))?;
    let embeddings = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V36 prefix source Parquet embedding column differs"))?;
    let values = embeddings
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V36 prefix source Parquet embedding child differs"))?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let start = row * DIMENSIONS;
        let embedding = values.values()[start..start + DIMENSIONS].to_vec();
        validate_embedding(&embedding)?;
        rows.push(V36PrefixMaterializedRow {
            feature_row_id: ids.value(row),
            source_ordinal: Some(*next_source_ordinal),
            embedding,
        });
        *next_source_ordinal = next_source_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("V36 prefix source Parquet row count overflows"))?;
    }
    Ok(rows)
}

fn v36_prefix_gt_batch(rows: &[V36PrefixGtNeighbor]) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(v36_prefix_gt100_schema()),
        vec![
            Arc::new(UInt32Array::from(
                rows.iter().map(|row| row.query_ordinal).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt16Array::from(
                rows.iter().map(|row| row.rank).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|row| row.feature_row_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|row| row.squared_distance)
                    .collect::<Vec<_>>(),
            )),
        ],
    )?)
}

struct V36PrefixGtQueryState {
    heap: BinaryHeap<RankedNeighbor>,
    query: V36PrefixQueryRow,
    role: usize,
}

/// Compute all quality-role GT@100 with one corpus scan and bounded state.
pub fn write_v36_prefix_gt100_roles_from_parquets(
    source_path: &Path,
    expected_source_feature_ids: &[u64],
    jobs: &[V36PrefixGtParquetJob],
    worker_threads: usize,
) -> Result<V36PrefixGtRunStats> {
    if jobs.len() != 3
        || worker_threads == 0
        || worker_threads > 16
        || jobs[0].role != V36PrefixQualityRole::Development
        || jobs[1].role != V36PrefixQualityRole::Validation
        || jobs[2].role != V36PrefixQualityRole::SealedHoldout
        || jobs.iter().any(|job| job.expected_queries == 0)
    {
        return Err(invalid("V36 prefix exact truth job set differs"));
    }
    let mut paths = BTreeSet::from([source_path]);
    if jobs
        .iter()
        .flat_map(|job| [&job.query, &job.output])
        .any(|path| !paths.insert(path))
    {
        return Err(invalid("V36 prefix exact truth paths overlap"));
    }
    let mut states = Vec::new();
    let mut query_ids = BTreeSet::new();
    for (role, job) in jobs.iter().enumerate() {
        scan_v36_prefix_query_parquet(&job.query, u64::from(job.expected_queries), |batch| {
            let rows = v36_prefix_query_rows_from_batch(&batch, 0, batch.num_rows())?;
            for query in rows {
                if !query_ids.insert(query.feature_row_id) {
                    return Err(invalid("V36 prefix exact truth query membership differs"));
                }
                states.push(V36PrefixGtQueryState {
                    heap: BinaryHeap::with_capacity(GT_NEIGHBORS),
                    query,
                    role,
                });
            }
            Ok(())
        })?;
    }
    let quality_queries = u32::try_from(states.len())
        .map_err(|_| invalid("V36 prefix exact truth query count overflows"))?;
    let expected_quality_queries = jobs.iter().try_fold(0_u32, |total, job| {
        total
            .checked_add(job.expected_queries)
            .ok_or_else(|| invalid("V36 prefix exact truth query count overflows"))
    })?;
    if quality_queries != expected_quality_queries {
        return Err(invalid("V36 prefix exact truth query count differs"));
    }
    let pool = ThreadPoolBuilder::new()
        .num_threads(worker_threads)
        .build()
        .map_err(|_| invalid("V36 prefix exact truth worker pool differs"))?;
    let mut source_rows = 0_u64;
    scan_v36_prefix_source_parquet(source_path, expected_source_feature_ids, |source_batch| {
        let rows = v36_prefix_source_rows_from_batch(&source_batch, &mut source_rows)?;
        if rows
            .iter()
            .any(|row| query_ids.contains(&row.feature_row_id))
        {
            return Err(invalid("V36 prefix exact truth corpus contains a query"));
        }
        pool.install(|| {
            states.par_iter_mut().for_each(|state| {
                for row in &rows {
                    let candidate = RankedNeighbor {
                        distance: squared_l2(&row.embedding, &state.query.embedding),
                        feature_row_id: row.feature_row_id,
                    };
                    if state.heap.len() < GT_NEIGHBORS {
                        state.heap.push(candidate);
                    } else if state.heap.peek().is_some_and(|worst| candidate < *worst) {
                        state.heap.pop();
                        state.heap.push(candidate);
                    }
                }
            });
        });
        Ok(())
    })?;
    if source_rows < GT_NEIGHBORS as u64
        || states.iter().any(|state| state.heap.len() != GT_NEIGHBORS)
    {
        return Err(invalid("V36 prefix exact truth corpus is insufficient"));
    }
    let mut truth_by_role = [Vec::new(), Vec::new(), Vec::new()];
    for state in states {
        let mut neighbors = state.heap.into_vec();
        neighbors.sort();
        truth_by_role[state.role].extend(neighbors.into_iter().enumerate().map(
            |(rank, neighbor)| V36PrefixGtNeighbor {
                query_ordinal: state.query.query_ordinal,
                rank: u16::try_from(rank).unwrap(),
                feature_row_id: neighbor.feature_row_id,
                squared_distance: neighbor.distance,
            },
        ));
    }
    for (job, truth) in jobs.iter().zip(truth_by_role) {
        write_v36_prefix_gt100_parquet(&job.output, [v36_prefix_gt_batch(&truth)?])?;
    }
    Ok(V36PrefixGtRunStats {
        quality_queries,
        source_scans: 1,
        source_rows,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        io::{BufWriter, Cursor, Write},
        rc::Rc,
        sync::{
            Arc as StdArc,
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
        },
    };

    use axum::{Router, body::Body, http::Response, routing::get};

    use super::*;

    struct CountingWriter {
        calls: Rc<Cell<usize>>,
    }

    impl Write for CountingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.calls.set(self.calls.get() + 1);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn v36_prefix_dataset_external_arrow_batch_body_is_canonically_bounded() {
        assert!(validate_v36_prefix_identity_batch_body(4, 80).is_ok());
        let error = validate_v36_prefix_identity_batch_body(4, 1 << 20).unwrap_err();
        assert_ne!(error.code(), "v36_prefix_resource_limit");
        assert!(validate_v36_prefix_selected_batch_body(4, 232).is_ok());
        let error = validate_v36_prefix_selected_batch_body(4, 1 << 20).unwrap_err();
        assert_ne!(error.code(), "v36_prefix_resource_limit");
    }

    #[test]
    fn v36_prefix_dataset_external_snapshot_rejects_source_growth_before_writing_it() {
        let mut source = Cursor::new(b"registered-plus-growth".to_vec());
        let mut snapshot = Vec::new();
        let error = copy_v36_prefix_snapshot_exact(
            &mut source,
            &mut snapshot,
            (Path::new("source.arrow"), Path::new("snapshot.arrow")),
            10,
            4,
        )
        .unwrap_err();

        assert_eq!(error.code(), "v36_prefix_resource_limit");
        assert_eq!(snapshot, b"registered");
    }

    #[test]
    fn v36_prefix_dataset_selected_stream_snapshots_have_distinct_roles() {
        let directory = tempfile::tempdir().unwrap();
        let contract = V36PrefixSelectedIdsContract {
            cohort_ordinal: 0,
            eligible_rows: 1,
            excluded_population_identity: None,
            excluded_rows: 0,
            ordered_source_manifest_sha256: "1".repeat(64),
            population_seed_sha256:
                "bcb490ff7944bfa3a0a6d5abe6d35ba34ecaba60b615e214edb057a1a5b63b8e".into(),
            selected_object_count: 1,
            selected_object_start: 0,
            selected_rows: 1,
        };
        let rows = select_v36_prefix_population_rows(
            vec![V36PrefixRowIdentity {
                feature_row_id: 7,
                row_offset: 3,
                selected_object_ordinal: 0,
                source_ordinal: None,
            }],
            &contract.ordered_source_manifest_sha256,
            1,
        )
        .unwrap();
        let bytes = encode_v36_prefix_selected_ids(&contract, &rows).unwrap();
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        let path = directory.path().join("selected.arrow");
        fs::write(&path, &bytes).unwrap();
        let selected = V36PrefixSelectedIdsFile {
            contract,
            identity: V36ArtifactIdentity {
                blake3: blake3::hash(&bytes).to_hex().to_string(),
                encoded_bytes: bytes.len().try_into().unwrap(),
                role: "population-selected-identities".into(),
                sha256: sha256.clone(),
                uri: format!("s3://fixture/v36/{sha256}-selected.arrow"),
            },
            path,
        };
        let limits = V36PrefixExternalSelectionLimits {
            io_buffer_bytes: 64,
            max_input_bytes: 1 << 20,
            max_scratch_bytes: 1 << 20,
            max_spills: 16,
            merge_fan_in: 2,
            sort_buffer_records: 2,
        };

        let mut primary = open_v36_prefix_selected_stream(
            &selected,
            &limits,
            directory.path(),
            "selected-population.arrow",
        )
        .unwrap();
        let mut exclusion = open_v36_prefix_selected_stream(
            &selected,
            &limits,
            directory.path(),
            "excluded-population.arrow",
        )
        .unwrap();
        assert_eq!(primary.next_record().unwrap().unwrap().feature_row_id, 7);
        assert_eq!(exclusion.next_record().unwrap().unwrap().feature_row_id, 7);
    }

    #[test]
    fn v36_prefix_dataset_spool_writer_coalesces_record_writes() {
        let calls = Rc::new(Cell::new(0));
        let mut writer = BufWriter::with_capacity(
            8 * 1024,
            CountingWriter {
                calls: Rc::clone(&calls),
            },
        );
        let row = V36PrefixMaterializedRow {
            feature_row_id: 7,
            source_ordinal: Some(0),
            embedding: vec![1.0; DIMENSIONS],
        };
        for ordinal in 0..32 {
            write_materialization_record(
                &mut writer,
                Path::new("counting-spool.bin"),
                ordinal,
                &row,
            )
            .unwrap();
        }
        writer.flush().unwrap();
        assert!(calls.get() <= 16, "underlying writes={}", calls.get());
    }

    #[test]
    fn v36_prefix_dataset_acquires_one_complete_authenticated_object() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("registered.bin");
        fs::write(&source, b"registered object bytes").unwrap();
        let bytes = fs::read(&source).unwrap();
        let object = V36PrefixRankedSourceObject {
            encoded_bytes: bytes.len().try_into().unwrap(),
            path: "data/registered.parquet".into(),
            sample_sha256: "1".repeat(64),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            uri: url::Url::from_file_path(&source).unwrap().to_string(),
        };
        let scratch = directory.path().join("scratch");
        fs::create_dir(&scratch).unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let acquired = acquire_v36_prefix_object(&runtime, &object, &scratch, 0).unwrap();
        assert_eq!(fs::read(acquired).unwrap(), bytes);

        let bad_scratch = directory.path().join("bad-scratch");
        fs::create_dir(&bad_scratch).unwrap();
        let mut drifted = object;
        drifted.sha256 = "f".repeat(64);
        assert!(acquire_v36_prefix_object(&runtime, &drifted, &bad_scratch, 0).is_err());
        assert!(bad_scratch.read_dir().unwrap().next().is_none());
    }

    #[test]
    fn v36_prefix_dataset_http_failure_never_resumes_a_partial_object() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let calls = StdArc::new(AtomicUsize::new(0));
        let handler_calls = StdArc::clone(&calls);
        let app = Router::new().route(
            "/object",
            get(move || {
                handler_calls.fetch_add(1, AtomicOrdering::SeqCst);
                async {
                    Response::builder()
                        .header("content-length", "64")
                        .header("etag", "\"registered-etag\"")
                        .header("last-modified", "Sun, 06 Sep 2026 00:00:00 GMT")
                        .body(Body::from("partial"))
                        .unwrap()
                }
            }),
        );
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = runtime.spawn(async move { axum::serve(listener, app).await.unwrap() });
        let directory = tempfile::tempdir().unwrap();
        let object = V36PrefixRankedSourceObject {
            encoded_bytes: 64,
            path: "data/registered.parquet".into(),
            sample_sha256: "1".repeat(64),
            sha256: "2".repeat(64),
            uri: format!("http://{address}/object"),
        };
        assert!(acquire_v36_prefix_object(&runtime, &object, directory.path(), 0).is_err());
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);
        assert!(directory.path().read_dir().unwrap().next().is_none());
        server.abort();
    }

    #[test]
    fn v36_prefix_dataset_checked_cleanup_cannot_report_false_success() {
        let directory = tempfile::tempdir().unwrap();
        let acquired = directory.path().join("source-0000.parquet");
        fs::write(&acquired, b"complete").unwrap();
        V36PrefixAcquiredObjects {
            paths: vec![acquired],
        }
        .cleanup()
        .unwrap();
        assert!(directory.path().read_dir().unwrap().next().is_none());

        let not_a_file = directory.path().join("source-0001.parquet");
        fs::create_dir(&not_a_file).unwrap();
        assert!(
            V36PrefixAcquiredObjects {
                paths: vec![not_a_file],
            }
            .cleanup()
            .is_err()
        );
    }
}
