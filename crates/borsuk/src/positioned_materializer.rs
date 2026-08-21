//! Pure immutable folds used by the Task 4 atomic positioned materializer.
//!
//! This module stages complete replacement authority, but it does not mutate
//! collection manifests, perform a collection CAS, or checkpoint source heads.

use bytes::Bytes;

use crate::{
    Result,
    row_bundle::{
        ArtifactRef, DirectoryRunRef, RowBundleObjectSink, StagedRowBundleGeneration,
        decode_directory_root, decode_generation_root, pack_directory_root,
        stage_existing_row_bundle_generation_to_sink,
    },
    storage::Storage,
};

struct StorageRowBundleSink<'a> {
    storage: &'a Storage,
}

impl RowBundleObjectSink for StorageRowBundleSink<'_> {
    fn emit(&mut self, artifact: &ArtifactRef, bytes: Bytes) -> Result<()> {
        self.storage
            .write_bytes_content_addressed(&artifact.path, &bytes)
            .map(|_| ())
    }
}

#[allow(
    dead_code,
    reason = "Task 4 Slice B wires the pure fold into publication"
)]
fn read_artifact(storage: &Storage, artifact: &ArtifactRef) -> Result<Bytes> {
    storage
        .read_bytes_with_cache_status_and_checksum(&artifact.path, &artifact.checksum)
        .map(|read| Bytes::from(read.bytes))
}

/// Fold one authenticated published row generation and its delta-local roots
/// into a fresh complete stock-Parquet generation authority.
#[allow(
    dead_code,
    reason = "Task 4 Slice B wires the pure fold into publication"
)]
pub(crate) fn fold_row_bundle_deltas_to_storage(
    storage: &Storage,
    published_root: Option<&ArtifactRef>,
    delta_roots: &[ArtifactRef],
) -> Result<StagedRowBundleGeneration> {
    let mut active_runs = Vec::new();
    let mut directory_runs = Vec::<DirectoryRunRef>::new();
    for root in published_root.into_iter().chain(delta_roots) {
        let generation = decode_generation_root(root, read_artifact(storage, root)?)?;
        directory_runs.extend(decode_directory_root(
            &generation.directory_root,
            read_artifact(storage, &generation.directory_root)?,
        )?);
        active_runs.extend(generation.active_runs);
    }
    active_runs.sort_by_key(|run| run.level);
    directory_runs.sort_by_key(|run| (run.partition, run.level));

    let directory_root = pack_directory_root(&directory_runs)?;
    storage.write_bytes_content_addressed(&directory_root.reference.path, &directory_root.bytes)?;
    let mut sink = StorageRowBundleSink { storage };
    stage_existing_row_bundle_generation_to_sink(&active_runs, &directory_root.reference, &mut sink)
}
