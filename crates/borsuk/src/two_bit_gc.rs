//! Bounded reclamation for cooperating single-host, same-directory participants.
use crate::{
    object_native_generation::{metadata_location, valid_object_key},
    resident_graph_generation::valid_sha256,
    two_bit_compaction::lifecycle_lock,
    two_bit_generation::{METADATA_FILES, Manifest, ROUTER_FILES},
    two_bit_mutations::TwoBitMutationLimits,
    two_bit_store::{
        TwoBitStoreError, TwoBitWriteFence, begin_two_bit_write_fence, end_two_bit_write_fence,
        small_object,
    },
};
use futures_util::{StreamExt, stream::BoxStream};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot};

/// Per-call work/payload caps. SDK list pages, retries, runtime and thread stacks
/// require separate caller charges, including while the session is idle.
#[derive(Clone, Copy)]
pub struct TwoBitGcLimits {
    /// Modeled metadata, mutation validation and bounded continuation payload.
    /// Requires mutations.max_memory_bytes + 524288 + size_of::<Traversal>() + 1024;
    /// the latter reserves the private stream handle and one path/size, not SDK buffers.
    pub max_memory_bytes: usize,
    /// Cap for validating the current pending snapshot before deleting anything.
    pub mutations: TwoBitMutationLimits,
    /// Maximum entries inspected, including a candidate retained by an earlier call.
    pub max_objects_scanned: usize,
    /// Maximum acknowledged deletes per call.
    pub max_objects_deleted: usize,
    /// Maximum LIST-observed logical object bytes deleted (not retained versions).
    pub max_deleted_object_bytes: u64,
}
/// SDK operation progress, excluding hidden transport retries/versioned retention.
#[derive(Debug, Default, Clone, Copy)]
pub struct TwoBitGcReport {
    /// Entries inspected, including unknown/retained objects and a pending candidate.
    pub objects_scanned: usize,
    /// Unknown keys skipped, never deleted.
    pub unknown_objects: usize,
    /// Delete calls submitted, including absent objects and ambiguous failures.
    pub delete_attempts: usize,
    /// Acknowledged successful logical deletes.
    pub objects_deleted: usize,
    /// LIST-observed sizes of acknowledged deletes; not physical space reclaimed.
    pub deleted_object_bytes: u64,
    /// True only when this observed listing exhausted before a work cap.
    pub scan_complete: bool,
    /// A LIST error discarded traversal; the next admitted call must restart LIST.
    pub restart_required: bool,
    /// LIST-observed size of the one candidate retained at the end of this pass.
    /// Unavailable (None) when dispatch is rejected or worker progress is unknown.
    pub pending_object_bytes: Option<u64>,
    /// Candidate exceeds the entire byte cap; retry with at least this budget.
    /// A shortfall only in this call's remaining budget leaves this as None.
    pub required_deleted_object_bytes: Option<u64>,
    /// A non-NotFound DELETE failure may have taken effect; counters exclude it.
    /// Also true if the worker failed and its progress could not be recovered.
    pub delete_outcome_unknown: bool,
}
/// A failed call reports acknowledged progress. Ambiguous DELETE/LIST failures preserve
/// the durable fence; pre-delete validation tries to release it without advancing.
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
                || ROUTER_FILES.contains(&name)
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
            || !manifest
                .discovery
                .valid(manifest.canonical.rows, manifest.canonical.dimensions)
            || manifest.canonical.dimensions != head.dimensions()
            || manifest.low.len() != head.dimensions()
            || manifest.step.len() != head.dimensions()
        {
            return Err(bad("GC root geometry"));
        }
        for name in manifest.discovery.files(false) {
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

type GcResult = std::result::Result<TwoBitGcReport, TwoBitGcFailure>;

struct Request {
    limits: TwoBitGcLimits,
    reply: oneshot::Sender<GcResult>,
}

#[derive(Default)]
struct Traversal {
    objects: Option<BoxStream<'static, object_store::Result<ObjectMeta>>>,
    pending: Option<(ObjectPath, u64)>,
}

const CONTINUATION_PAYLOAD_BYTES: usize = std::mem::size_of::<Traversal>() + 1024;
const THREAD_STACK_BYTES: usize = 2 * 1024 * 1024;

/// In-process traversal bound immutably to one store, prefix and local directory.
/// Not Clone. Reuse this session across capped calls; recreation starts a new LIST.
/// Its owned thread/current-thread Tokio runtime outlive cancelled callers.
/// Dropping the session closes dispatch without joining: accepted work finishes,
/// then the worker drops LIST inside its runtime before shutting that runtime down.
/// Runtime/SDK buffers, bindings, allocator overhead and up to three 2 MiB thread
/// stacks are outside the modeled payload cap. Store timeouts/retries remain the
/// caller's responsibility; a hung store can keep an accepted worker alive.
pub struct TwoBitGcSession {
    dispatch: mpsc::Sender<Request>,
    busy: Arc<AtomicBool>,
}

impl TwoBitGcSession {
    /// Validate the namespace and start one owned worker. Runtime/thread startup
    /// failures are returned before a session is exposed; no remote I/O occurs.
    /// ALL cooperating readers/maintenance must use this same trusted directory.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &ObjectPath, directory: &Path) -> Result<Self> {
        if prefix.as_ref().is_empty() || prefix.as_ref().len() > 512 {
            return Err(bad("GC admission"));
        }
        let prefix = prefix.clone();
        let directory = std::path::absolute(directory)?;
        let (dispatch, mut requests) = mpsc::channel::<Request>(1);
        let busy = Arc::new(AtomicBool::new(false));
        let worker_busy = busy.clone();
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        // Create AND destroy the runtime on its own thread, never an async caller.
        std::thread::Builder::new()
            .name("two-bit-gc".into())
            .stack_size(THREAD_STACK_BYTES)
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    // LIST may use blocking I/O; keep a second slot for fresh metadata.
                    .max_blocking_threads(2)
                    .thread_stack_size(THREAD_STACK_BYTES)
                    .thread_name("two-bit-gc-io")
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                if ready.send(Ok(())).is_err() {
                    return;
                }
                runtime.block_on(async move {
                    let mut traversal = Traversal::default();
                    while let Some(request) = requests.recv().await {
                        let result = traversal
                            .collect(store.as_ref(), &prefix, &directory, request.limits)
                            .await;
                        worker_busy.store(false, Ordering::Release);
                        let _ = request.reply.send(result);
                    }
                    drop(traversal);
                });
            })?;
        started
            .recv()
            .map_err(|_| bad("GC worker startup failed"))??;
        Ok(Self { dispatch, busy })
    }
}

fn worker_failed() -> TwoBitGcFailure {
    TwoBitGcFailure {
        error: bad("GC worker failed; progress unknown"),
        report: TwoBitGcReport {
            restart_required: true,
            delete_outcome_unknown: true,
            ..Default::default()
        },
    }
}

/// Collect recognized unreferenced objects under a quiescent single-host owner.
/// ALL readers and maintenance must use the session's directory; direct APIs/other
/// hosts are unregistered. No independent actor may release the worker's fence.
/// Lockfiles are trusted local state and must never be replaced while in use.
/// Each accepted call reacquires the lifecycle lock and authenticated fence and
/// rebuilds the current keep-set, including before retrying a pending candidate.
/// Dropping this future cannot cancel accepted work or release its lock/fence.
/// A concurrent retry while that work runs fails busy, without queueing another pass.
/// Successful capped calls release the fence while retaining LIST. EOF alone ends
/// a sweep; the next call starts another. LIST errors discard traversal explicitly.
/// DELETE NotFound clears the candidate without charging acknowledged deletes.
/// Other DELETE errors retain the candidate and fence; unresolved release errors fail.
/// There is no crash-durable cursor or historical rollback retention. Late uploads
/// may require another sweep. Versioned retention is outside logical delete counts.
pub async fn collect_two_bit_garbage(
    session: &mut TwoBitGcSession,
    limits: TwoBitGcLimits,
) -> GcResult {
    if session.dispatch.is_closed() {
        return Err(worker_failed());
    }
    if session
        .busy
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(TwoBitGcFailure {
            error: bad("GC session busy"),
            report: TwoBitGcReport::default(),
        });
    }
    let (reply, result) = oneshot::channel();
    if session
        .dispatch
        .try_send(Request { limits, reply })
        .is_err()
    {
        session.busy.store(false, Ordering::Release);
        return Err(worker_failed());
    }
    result.await.map_err(|_| worker_failed())?
}

impl Traversal {
    async fn collect(
        &mut self,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        directory: &Path,
        limits: TwoBitGcLimits,
    ) -> GcResult {
        let mut report = TwoBitGcReport::default();
        let result = self
            .pass(store, prefix, directory, limits, &mut report)
            .await;
        report.pending_object_bytes = self.pending.as_ref().map(|(_, size)| *size);
        result
            .map(|()| report)
            .map_err(|error| TwoBitGcFailure { error, report })
    }

    async fn pass(
        &mut self,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        directory: &Path,
        limits: TwoBitGcLimits,
        report: &mut TwoBitGcReport,
    ) -> Result<()> {
        if limits.max_objects_scanned == 0
            || limits.max_objects_deleted == 0
            || limits.max_deleted_object_bytes == 0
            || limits
                .mutations
                .max_memory_bytes
                .checked_add(524288)
                .and_then(|n| n.checked_add(CONTINUATION_PAYLOAD_BYTES))
                .is_none_or(|n| n > limits.max_memory_bytes)
        {
            return Err(bad("GC admission"));
        }
        crate::two_bit_mutations::admit(limits.mutations, 0)?;
        let _lifetime = lifecycle_lock(prefix, directory, true)?;
        let fence = begin_two_bit_write_fence(store, prefix).await?;
        let keep = match keep_set(store, &fence, limits).await {
            Ok(keep) => keep,
            Err(error) => {
                end_two_bit_write_fence(store, &fence).await?;
                return Err(error);
            }
        };
        if self.objects.is_none() {
            self.objects = Some(store.list(Some(prefix)));
        }
        // Check both work caps BEFORE consuming even an unknown/retained entry.
        while report.objects_scanned < limits.max_objects_scanned
            && report.objects_deleted < limits.max_objects_deleted
        {
            let (location, size) = match self.pending.take() {
                Some(candidate) => candidate,
                None => match self.objects.as_mut().unwrap().next().await {
                    Some(Ok(object)) => {
                        if object.location.as_ref().len() > 1024 {
                            report.objects_scanned += 1;
                            report.unknown_objects += 1;
                            continue;
                        }
                        // Copy the bounded key; do not retain SDK metadata/excess capacity.
                        (object.location.clone(), object.size)
                    }
                    Some(Err(error)) => {
                        self.objects = None;
                        report.restart_required = true;
                        return Err(error.into());
                    }
                    None => {
                        self.objects = None;
                        report.scan_complete = true;
                        break;
                    }
                },
            };
            report.objects_scanned += 1;
            if keep.contains(&location) {
                continue;
            }
            if !recognized(prefix.as_ref(), location.as_ref()) {
                report.unknown_objects += 1;
                continue;
            }
            self.pending = Some((location, size));
            if size > limits.max_deleted_object_bytes - report.deleted_object_bytes {
                if size > limits.max_deleted_object_bytes {
                    report.required_deleted_object_bytes = Some(size);
                }
                break;
            }
            report.delete_attempts += 1;
            match store.delete(&self.pending.as_ref().unwrap().0).await {
                Ok(()) => {}
                Err(object_store::Error::NotFound { .. }) => {
                    self.pending = None;
                    continue;
                }
                Err(error) => {
                    report.delete_outcome_unknown = true;
                    return Err(error.into());
                }
            }
            self.pending = None;
            report.objects_deleted += 1;
            report.deleted_object_bytes += size;
        }
        end_two_bit_write_fence(store, &fence).await
    }
}
