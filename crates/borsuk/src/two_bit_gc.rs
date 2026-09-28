//! Bounded reclamation for cooperating single-host, same-directory participants.
use crate::{
    object_native_generation::{metadata_location, valid_object_key},
    resident_graph_generation::valid_sha256,
    two_bit_compaction::lifecycle_lock,
    two_bit_generation::{METADATA_FILES, Manifest},
    two_bit_mutations::TwoBitMutationLimits,
    two_bit_store::{
        TwoBitStoreError, TwoBitWriteFence, begin_two_bit_write_fence, end_two_bit_write_fence,
        small_object,
    },
};
use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, path::Path, sync::Arc};

/// Per-call work/payload caps. SDK list pages, retries/runtime require caller charges.
#[derive(Clone, Copy)]
pub struct TwoBitGcLimits {
    /// Modeled metadata plus pending-state validation payload, not RSS.
    pub max_memory_bytes: usize,
    /// Cap for validating the current pending snapshot before deleting anything.
    pub mutations: TwoBitMutationLimits,
    /// Maximum listed objects inspected; repeated calls restart the prefix scan.
    pub max_objects_scanned: usize,
    /// Maximum acknowledged deletes per call.
    pub max_objects_deleted: usize,
    /// Maximum LIST-observed logical object bytes deleted (not retained versions).
    pub max_deleted_object_bytes: u64,
}
/// SDK operation progress, excluding hidden transport retries/versioned retention.
#[derive(Debug, Default, Clone, Copy)]
pub struct TwoBitGcReport {
    /// LIST entries inspected, including unknown/retained objects.
    pub objects_scanned: usize,
    /// Unknown keys skipped, never deleted.
    pub unknown_objects: usize,
    /// Delete calls submitted, including ambiguous failures.
    pub delete_attempts: usize,
    /// Acknowledged successful logical deletes.
    pub objects_deleted: usize,
    /// LIST-observed sizes of acknowledged deletes; not physical space reclaimed.
    pub deleted_object_bytes: u64,
    /// True only when this observed listing exhausted before a work cap.
    pub scan_complete: bool,
}
/// An interrupted call preserves its durable write fence for retry.
#[derive(Debug, thiserror::Error)]
#[error("two-bit garbage collection: {error}")]
pub struct TwoBitGcFailure {
    /// Underlying cause; after fencing, retry with the same directory/prefix.
    #[source]
    pub error: TwoBitStoreError,
    /// Acknowledged progress; failed delete side effects may be ambiguous.
    pub report: TwoBitGcReport,
}
type Result<T> = std::result::Result<T, TwoBitStoreError>;
fn bad(s: &'static str) -> TwoBitStoreError {
    TwoBitStoreError::Invalid(s)
}
fn maintenance_owner(s: &str) -> bool {
    s.len() == 48 && s.bytes().all(|c| c.is_ascii_hexdigit())
}
fn recognized(prefix: &str, key: &str) -> bool {
    let Some(relative) = key.strip_prefix(prefix).and_then(|s| s.strip_prefix('/')) else {
        return false;
    };
    if let Some((root, name)) = relative
        .strip_prefix("generations/")
        .and_then(|s| s.split_once('/'))
    {
        return valid_sha256(root)
            && (METADATA_FILES.contains(&name)
                || name.strip_prefix("mutations/").is_some_and(valid_sha256));
    }
    if let Some((owner, name)) = relative
        .strip_prefix("maintenance/")
        .and_then(|s| s.split_once('/'))
    {
        return maintenance_owner(owner)
            && (name == "claim.json" || name.strip_prefix("objects/").is_some_and(valid_sha256));
    }
    false
}
fn keep_object(keep: &mut BTreeSet<ObjectPath>, key: &str, sha: &str) -> Result<()> {
    if key.len() > 1024 || !valid_object_key(key, sha) {
        return Err(bad("GC object binding"));
    }
    keep.insert(ObjectPath::from(key));
    if let Some((owner, _)) = key.rsplit_once("/objects/") {
        // Retain the immutable namespace claim supporting each current object.
        keep.insert(ObjectPath::from(format!("{owner}/claim.json")));
    }
    Ok(())
}
async fn keep_set(
    store: &dyn ObjectStore,
    fence: &TwoBitWriteFence,
    limits: TwoBitGcLimits,
) -> Result<BTreeSet<ObjectPath>> {
    let head = fence.head();
    let mut keep = BTreeSet::new();
    keep.insert(head.index_prefix().clone().join("head.json"));
    let root = head.metadata_prefix().join("manifest.json");
    keep.insert(root.clone());
    if !head.is_empty() {
        let (bytes, _) = small_object(store, &root, 65536).await?;
        if format!("{:x}", Sha256::digest(&bytes)) != head.root_sha256() {
            return Err(bad("GC root identity"));
        }
        let manifest: Manifest =
            serde_json::from_slice(&bytes).map_err(|_| bad("GC root schema"))?;
        if manifest.schema != crate::two_bit_generation::SCHEMA
            || manifest.generation != head.generation()
            || !manifest.canonical.valid()
            || manifest.canonical.dimensions != head.dimensions()
            || manifest.low.len() != head.dimensions()
            || manifest.step.len() != head.dimensions()
        {
            return Err(bad("GC root geometry"));
        }
        for name in METADATA_FILES {
            keep.insert(metadata_location(&head.metadata_prefix(), name));
        }
        keep_object(
            &mut keep,
            &manifest.sq8_object_key,
            &manifest.sq8_object_sha256,
        )?;
        keep_object(
            &mut keep,
            &manifest.canonical.object_key,
            &manifest.canonical.sha256,
        )?;
        let expected = manifest
            .canonical
            .rows
            .checked_mul(
                head.dimensions()
                    .checked_add(12)
                    .ok_or(bad("GC SQ8 geometry"))?,
            )
            .ok_or(bad("GC SQ8 geometry"))?;
        let sq8 = store
            .head(&ObjectPath::from(manifest.sq8_object_key))
            .await?;
        if sq8.size != expected as u64 || sq8.e_tag.as_deref() != Some(&manifest.sq8_etag) {
            return Err(bad("GC SQ8 identity"));
        }
        if store
            .head(&ObjectPath::from(manifest.canonical.object_key))
            .await?
            .size
            != manifest.canonical.bytes
        {
            return Err(bad("GC canonical geometry"));
        }
    }
    fence.validate_mutations(store, limits.mutations).await?;
    if let Some(sha) = fence.mutation_sha256() {
        keep.insert(head.metadata_prefix().join("mutations").join(sha));
    }
    Ok(keep)
}

/// Collect recognized unreferenced objects under a quiescent single-host owner.
/// ALL readers and maintenance must use this same directory; direct APIs/other
/// hosts are unregistered. No independent actor may release this call's fence.
/// Lockfiles are trusted local state and must never be replaced while in use.
/// An owned worker retains exclusivity when its awaiting future is dropped.
/// Failure leaves the write fence durable; retry to finish/release. Successful
/// capped passes release it. Late orphan uploads may require another pass.
/// This retains current data, not historical rollback generations. Versioned
/// bucket retention and SDK buffers/retries are outside these logical counters.
pub async fn collect_two_bit_garbage(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    directory: &Path,
    limits: TwoBitGcLimits,
) -> std::result::Result<TwoBitGcReport, TwoBitGcFailure> {
    let prefix = prefix.clone();
    let directory = directory.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut report = TwoBitGcReport::default();
        let result = (|| {
            if prefix.as_ref().is_empty()
                || prefix.as_ref().len() > 512
                || limits.max_objects_scanned == 0
                || limits.max_objects_deleted == 0
                || limits.max_deleted_object_bytes == 0
                || limits
                    .mutations
                    .max_memory_bytes
                    .checked_add(524288)
                    .is_none_or(|n| n > limits.max_memory_bytes)
            {
                return Err(bad("GC admission"));
            }
            crate::two_bit_mutations::admit(limits.mutations, 0)?;
            let _lifetime = lifecycle_lock(&prefix, &directory, true)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async {
                let fence = begin_two_bit_write_fence(store.as_ref(), &prefix).await?;
                let keep = match keep_set(store.as_ref(), &fence, limits).await {
                    Ok(keep) => keep,
                    Err(error) => {
                        // No deletion was submitted; reopen with a distinct epoch.
                        end_two_bit_write_fence(store.as_ref(), &fence).await?;
                        return Err(error);
                    }
                };
                // ponytail: bounded scans restart at the prefix; raise the scan cap
                // if unknown/retained keys consume it, add a cursor only if needed.
                let mut objects = store.list(Some(&prefix));
                while report.objects_scanned < limits.max_objects_scanned {
                    let Some(object) = objects.next().await else {
                        report.scan_complete = true;
                        break;
                    };
                    let object = object?;
                    report.objects_scanned += 1;
                    if keep.contains(&object.location) {
                        continue;
                    }
                    if object.location.as_ref().len() > 1024
                        || !recognized(prefix.as_ref(), object.location.as_ref())
                    {
                        report.unknown_objects += 1;
                        continue;
                    }
                    let total = report
                        .deleted_object_bytes
                        .checked_add(object.size)
                        .ok_or(bad("GC byte count"))?;
                    if report.objects_deleted == limits.max_objects_deleted
                        || total > limits.max_deleted_object_bytes
                    {
                        break;
                    }
                    report.delete_attempts += 1;
                    store.delete(&object.location).await?;
                    report.objects_deleted += 1;
                    report.deleted_object_bytes = total;
                }
                drop(objects);
                end_two_bit_write_fence(store.as_ref(), &fence).await?;
                Ok(report)
            })
        })();
        result.map_err(|error| TwoBitGcFailure { error, report })
    })
    .await
    .map_err(|_| TwoBitGcFailure {
        error: bad("GC worker failed; progress unknown"),
        report: TwoBitGcReport::default(),
    })?
}
