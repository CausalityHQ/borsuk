mod common;

use borsuk::{
    two_bit_gc::{TwoBitGcLimits, collect_two_bit_garbage},
    two_bit_mutations::{
        TwoBitMutation, TwoBitMutationLimits, apply_two_bit_mutations, read_two_bit_mutations,
    },
    two_bit_store::{publish_empty_two_bit_generation, read_two_bit_head},
};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;

#[tokio::test]
async fn acknowledged_mutation_survives_an_earlier_ambiguous_delete() {
    let store = Arc::new(InMemory::new());
    let prefix = Path::from("late-delete");
    let base = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 1, None)
        .await
        .unwrap();
    let mutations = TwoBitMutationLimits {
        max_snapshot_bytes: 16384,
        max_memory_bytes: 1_000_000,
    };
    let batch = [TwoBitMutation {
        id: 7,
        vector: Some(vec![1., 0.]),
    }];
    let (failed_writer, writes) =
        common::FaultInjectingObjectStore::fail_nth_matching(store.clone(), 1, true, |op, path| {
            op == common::StoreOperation::Put && path.as_ref().ends_with("/head.json")
        })
        .with_operation_log();
    assert!(
        apply_two_bit_mutations(&failed_writer, &base, 2, None, &batch, mutations)
            .await
            .is_err()
    );
    let staged = writes.matching_paths(|op, path| {
        op == common::StoreOperation::Put && path.contains("/mutations/")
    });
    assert_eq!(staged.len(), 1);
    let orphan = Path::from(staged[0].clone());
    let directory = tempfile::tempdir().unwrap();
    let caps = TwoBitGcLimits {
        max_memory_bytes: 2_000_000,
        mutations,
        max_objects_scanned: 100,
        max_objects_deleted: 100,
        max_deleted_object_bytes: 1_000_000,
    };
    let target = orphan.clone();
    let failed_delete = common::FaultInjectingObjectStore::fail_nth_matching(
        store.clone(),
        1,
        true,
        move |op, path| op == common::StoreOperation::Delete && *path == target,
    );
    let failure = collect_two_bit_garbage(
        &mut borsuk::two_bit_gc::TwoBitGcSession::new(
            Arc::new(failed_delete),
            &prefix,
            directory.path(),
        )
        .unwrap(),
        caps,
    )
    .await
    .unwrap_err();
    assert_eq!(failure.report.delete_attempts, 1);
    assert!(store.head(&orphan).await.is_ok());
    collect_two_bit_garbage(
        &mut borsuk::two_bit_gc::TwoBitGcSession::new(store.clone(), &prefix, directory.path())
            .unwrap(),
        caps,
    )
    .await
    .unwrap();
    assert!(store.head(&orphan).await.is_err());
    let base = read_two_bit_head(store.as_ref(), &prefix)
        .await
        .unwrap()
        .unwrap();
    let acknowledged = apply_two_bit_mutations(store.as_ref(), &base, 2, None, &batch, mutations)
        .await
        .unwrap();
    // Model the original DELETE reaching storage after its error and the successful retry.
    // An HTTP error cannot establish that the remote operation was cancelled.
    store.delete(&orphan).await.unwrap();
    let recovered = read_two_bit_mutations(store.as_ref(), &base, 2, mutations)
        .await
        .expect("a delayed orphan DELETE must not destroy an acknowledged mutation")
        .unwrap();
    assert_eq!(recovered.rows(), acknowledged.rows());
}

#[tokio::test]
async fn acknowledged_empty_generation_survives_an_earlier_ambiguous_delete() {
    let store = Arc::new(InMemory::new());
    let prefix = Path::from("late-generation-delete");
    let base = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 1, None)
        .await
        .unwrap();
    let mutations = TwoBitMutationLimits {
        max_snapshot_bytes: 16384,
        max_memory_bytes: 1_000_000,
    };
    borsuk::two_bit_mutations::seal_two_bit_mutations(store.as_ref(), &base, None, mutations)
        .await
        .unwrap();
    let (failed_writer, writes) =
        common::FaultInjectingObjectStore::fail_nth_matching(store.clone(), 1, true, |op, path| {
            op == common::StoreOperation::Put && path.as_ref().ends_with("/head.json")
        })
        .with_operation_log();
    assert!(
        publish_empty_two_bit_generation(&failed_writer, &prefix, 2, 2, Some(&base))
            .await
            .is_err()
    );
    let staged = writes.matching_paths(|op, path| {
        op == common::StoreOperation::Put && path.ends_with("/manifest.json")
    });
    assert_eq!(staged.len(), 1);
    let orphan = Path::from(staged[0].clone());
    let directory = tempfile::tempdir().unwrap();
    let caps = TwoBitGcLimits {
        max_memory_bytes: 2_000_000,
        mutations,
        max_objects_scanned: 100,
        max_objects_deleted: 100,
        max_deleted_object_bytes: 1_000_000,
    };
    let target = orphan.clone();
    let failed_delete = common::FaultInjectingObjectStore::fail_nth_matching(
        store.clone(),
        1,
        true,
        move |op, path| op == common::StoreOperation::Delete && *path == target,
    );
    let failure = collect_two_bit_garbage(
        &mut borsuk::two_bit_gc::TwoBitGcSession::new(
            Arc::new(failed_delete),
            &prefix,
            directory.path(),
        )
        .unwrap(),
        caps,
    )
    .await
    .unwrap_err();
    assert_eq!(failure.report.delete_attempts, 1);
    assert!(store.head(&orphan).await.is_ok());
    collect_two_bit_garbage(
        &mut borsuk::two_bit_gc::TwoBitGcSession::new(store.clone(), &prefix, directory.path())
            .unwrap(),
        caps,
    )
    .await
    .unwrap();
    assert!(store.head(&orphan).await.is_err());
    let base = read_two_bit_head(store.as_ref(), &prefix)
        .await
        .unwrap()
        .unwrap();
    let acknowledged = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 2, Some(&base))
        .await
        .unwrap();
    store.delete(&orphan).await.unwrap();
    let recovered = read_two_bit_head(store.as_ref(), &prefix)
        .await
        .expect("a delayed orphan DELETE must not destroy an acknowledged generation")
        .unwrap();
    assert_eq!(recovered.root_sha256(), acknowledged.root_sha256());
}

#[tokio::test]
async fn acknowledged_compaction_survives_an_earlier_ambiguous_metadata_delete() {
    use borsuk::{
        canonical_source::TwoBitCompactionLimits,
        two_bit_compaction::{TwoBitCompactionOptions, compact_two_bit_index},
        two_bit_generation::TwoBitGenerationLimits,
    };
    let store = Arc::new(InMemory::new());
    let prefix = Path::from("late-compaction-delete");
    let base = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 1, None)
        .await
        .unwrap();
    let mutations = TwoBitMutationLimits {
        max_snapshot_bytes: 16384,
        max_memory_bytes: 1_000_000,
    };
    apply_two_bit_mutations(
        store.as_ref(),
        &base,
        2,
        None,
        &[TwoBitMutation {
            id: 7,
            vector: Some(vec![1., 0.]),
        }],
        mutations,
    )
    .await
    .unwrap();
    let options = TwoBitCompactionOptions {
        mutations,
        source: TwoBitCompactionLimits {
            max_memory_bytes: 4_000_000,
            max_disk_bytes: 4_000_000,
            max_source_chunk_bytes: 16384,
        },
        generation: TwoBitGenerationLimits {
            max_memory_bytes: 4_000_000,
            max_active_queries: 1,
            max_query_bytes: 16384,
            max_query_gets: 2,
            max_parallel_gets: 2,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 16,
            max_query_scratch_bytes: 8192,
            already_pinned_bytes: 0,
        },
    };
    let directory = tempfile::tempdir().unwrap();
    let (failed_writer, writes) =
        common::FaultInjectingObjectStore::fail_nth_matching(store.clone(), 2, true, |op, path| {
            op == common::StoreOperation::Put && path.as_ref().ends_with("/head.json")
        })
        .with_operation_log();
    let error = compact_two_bit_index(Arc::new(failed_writer), &prefix, directory.path(), options)
        .await
        .unwrap_err();
    assert!(
        matches!(error, borsuk::two_bit_store::TwoBitStoreError::Store(_)),
        "{error:?}"
    );
    let staged = writes.matching_paths(|op, path| {
        op == common::StoreOperation::MultipartPut
            && path.contains("/generations/")
            && path.ends_with("/manifest.json")
            && !path.contains("/plane/")
    });
    assert_eq!(staged.len(), 1);
    let orphan = Path::from(staged[0].clone());
    let prepared: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            directory
                .path()
                .join(base.root_sha256())
                .join("generation/manifest.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let sq8 = Path::from(prepared["sq8_object_key"].as_str().unwrap());
    let claim = Path::from(format!(
        "{}/claim.json",
        sq8.as_ref().rsplit_once("/objects/").unwrap().0
    ));
    let caps = TwoBitGcLimits {
        max_memory_bytes: 2_000_000,
        mutations,
        max_objects_scanned: 100,
        max_objects_deleted: 100,
        max_deleted_object_bytes: 1_000_000,
    };
    let target = orphan.clone();
    let failed_delete = common::FaultInjectingObjectStore::fail_nth_matching(
        store.clone(),
        1,
        true,
        move |op, path| op == common::StoreOperation::Delete && *path == target,
    );
    collect_two_bit_garbage(
        &mut borsuk::two_bit_gc::TwoBitGcSession::new(
            Arc::new(failed_delete),
            &prefix,
            directory.path(),
        )
        .unwrap(),
        caps,
    )
    .await
    .unwrap_err();
    collect_two_bit_garbage(
        &mut borsuk::two_bit_gc::TwoBitGcSession::new(store.clone(), &prefix, directory.path())
            .unwrap(),
        TwoBitGcLimits {
            max_objects_deleted: 1,
            ..caps
        },
    )
    .await
    .unwrap();
    assert!(store.head(&orphan).await.is_err());
    assert!(store.head(&sq8).await.is_ok());
    assert!(store.head(&claim).await.is_ok());
    let acknowledged = compact_two_bit_index(store.clone(), &prefix, directory.path(), options)
        .await
        .unwrap();
    store.delete(&orphan).await.unwrap();
    let recovered = read_two_bit_head(store.as_ref(), &prefix)
        .await
        .expect("a delayed metadata DELETE must not destroy acknowledged compaction")
        .unwrap();
    assert_eq!(recovered.root_sha256(), acknowledged.root_sha256());
    assert_ne!(recovered.metadata_prefix().join("manifest.json"), orphan);
}

#[tokio::test]
async fn empty_generation_rejects_invalid_geometry_before_io() {
    let store = Arc::new(InMemory::new());
    let prefix = Path::from("empty-admission");
    let base = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 1, None)
        .await
        .unwrap();
    let (guarded, operations) =
        common::FaultInjectingObjectStore::fail_nth_matching(store, usize::MAX, true, |_, _| false)
            .with_operation_log();
    for (dimensions, generation) in [(0, 2), (2, 1)] {
        assert!(matches!(
            publish_empty_two_bit_generation(
                &guarded,
                &prefix,
                dimensions,
                generation,
                Some(&base)
            )
            .await,
            Err(borsuk::two_bit_store::TwoBitStoreError::Invalid(
                "empty generation namespace/order/dimensions"
            ))
        ));
    }
    assert!(operations.matching_paths(|_, _| true).is_empty());
}

mod session_tests {
    use super::*;
    use borsuk::two_bit_gc::{TwoBitGcFailure, TwoBitGcReport, TwoBitGcSession};
    use borsuk::two_bit_store::TwoBitStoreError;
    use futures_util::{
        StreamExt,
        stream::{self, BoxStream},
    };
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::{
        fmt,
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::sync::{Notify, Semaphore};

    const DEADLINE: Duration = Duration::from_secs(5);

    fn caps() -> TwoBitGcLimits {
        TwoBitGcLimits {
            max_memory_bytes: 2_000_000,
            mutations: TwoBitMutationLimits {
                max_snapshot_bytes: 16384,
                max_memory_bytes: 1_000_000,
            },
            max_objects_scanned: 16,
            max_objects_deleted: 16,
            max_deleted_object_bytes: 1_000_000,
        }
    }

    fn injected() -> object_store::Error {
        object_store::Error::Generic {
            store: "GC session fixture",
            source: std::io::Error::other("injected failure").into(),
        }
    }

    #[derive(Debug)]
    struct Gate {
        entered: Notify,
        release: Semaphore,
    }
    impl Gate {
        fn new() -> Self {
            Self {
                entered: Notify::new(),
                release: Semaphore::new(0),
            }
        }
        async fn wait(&self) -> object_store::Result<()> {
            self.entered.notify_one();
            let permit = tokio::time::timeout(DEADLINE, self.release.acquire())
                .await
                .map_err(|_| injected())?
                .map_err(|_| injected())?;
            permit.forget();
            Ok(())
        }
    }
    // A panic must not strand the accepted worker on a fixture gate.
    struct ReleaseOnDrop(Arc<Gate>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release.add_permits(1);
        }
    }

    #[derive(Debug, Default)]
    struct Observed {
        lists: AtomicUsize,
        polls: AtomicUsize,
        list_drops: AtomicUsize,
        drop_in_runtime: AtomicBool,
        drop_on_list_thread: AtomicBool,
        deletes: AtomicUsize,
        delete_not_found: AtomicUsize,
        #[cfg(target_os = "linux")]
        threads: Mutex<Vec<OsThread>>,
    }
    struct ListLifetime(Arc<Observed>, std::thread::ThreadId);
    impl Drop for ListLifetime {
        fn drop(&mut self) {
            self.0.drop_in_runtime.store(
                tokio::runtime::Handle::try_current().is_ok(),
                Ordering::SeqCst,
            );
            self.0.list_drops.fetch_add(1, Ordering::SeqCst);
            self.0
                .drop_on_list_thread
                .store(std::thread::current().id() == self.1, Ordering::SeqCst);
        }
    }

    // A retained store Arc or a TLS destructor alone does not prove OS-thread exit.
    #[cfg(target_os = "linux")]
    #[derive(Debug)]
    struct OsThread {
        rust_id: std::thread::ThreadId,
        name: String,
        stat: std::path::PathBuf,
        start_time: String,
    }
    #[cfg(target_os = "linux")]
    impl OsThread {
        fn start_time(stat: &std::path::Path) -> std::io::Result<String> {
            let text = std::fs::read_to_string(stat)?;
            // /proc stat field 22, after the parenthesized comm (which may contain spaces).
            text.rsplit_once(')')
                .and_then(|(_, fields)| fields.split_whitespace().nth(19))
                .map(str::to_owned)
                .ok_or_else(|| std::io::Error::other("invalid thread stat"))
        }
        fn current() -> std::io::Result<Self> {
            let task = std::fs::read_link("/proc/thread-self")?;
            let stat = std::path::Path::new("/proc").join(task).join("stat");
            Ok(Self {
                rust_id: std::thread::current().id(),
                name: std::thread::current().name().unwrap_or("").to_owned(),
                start_time: Self::start_time(&stat)?,
                stat,
            })
        }
        fn exited(&self) -> std::io::Result<bool> {
            match Self::start_time(&self.stat) {
                Ok(start_time) => Ok(start_time != self.start_time),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
                Err(error) => Err(error),
            }
        }
    }

    #[cfg(target_os = "linux")]
    async fn probe_runtime_threads(observed: &Arc<Observed>) -> object_store::Result<()> {
        let worker = OsThread::current().map_err(|_| injected())?;
        observed.threads.lock().unwrap().push(worker);
        let mut releases = Vec::new();
        let mut jobs = Vec::new();
        // Hold two jobs concurrently to identify both slots of the real runtime's pool.
        for _ in 0..2 {
            let (release, wait) = std::sync::mpsc::channel::<()>();
            releases.push(release);
            let observed = observed.clone();
            jobs.push(tokio::task::spawn_blocking(move || {
                let identity = OsThread::current()?;
                observed.threads.lock().unwrap().push(identity);
                match wait.recv_timeout(DEADLINE * 2) {
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) | Ok(()) => Ok(()),
                    Err(error) => Err(std::io::Error::other(error)),
                }
            }));
        }
        let ready = tokio::time::timeout(DEADLINE, async {
            while observed.threads.lock().unwrap().len() != 3 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        // Dropping senders also releases both helpers on unwind or cancelled polling.
        drop(releases);
        let drained = tokio::time::timeout(DEADLINE, async {
            let mut succeeded = true;
            for job in jobs {
                succeeded &= matches!(job.await, Ok(Ok(())));
            }
            succeeded
        })
        .await;
        if ready.is_ok() && matches!(drained, Ok(true)) {
            Ok(())
        } else {
            Err(injected())
        }
    }

    #[derive(Debug, Clone)]
    struct SessionStore {
        data: Arc<dyn ObjectStore>,
        // LocalFileSystem 0.14.1 has no conditional UPDATE: only head CAS uses this.
        head: Option<Arc<dyn ObjectStore>>,
        script: Arc<Mutex<Option<Vec<ObjectMeta>>>>,
        observed: Arc<Observed>,
        list_error_at: Arc<AtomicUsize>,
        delete_error: Arc<AtomicUsize>, // errors: 1 before/2 after; panics: 3 before/4 after
        release_error: Arc<AtomicUsize>, // 1 before, 2 after, 3 after + failed reread
        head_read_error: Arc<AtomicBool>,
        gate: Arc<Mutex<Option<Arc<Gate>>>>,
        list_gate: Arc<Mutex<Option<Arc<Gate>>>>,
        get_gate: Arc<Mutex<Option<(Path, Arc<Gate>)>>>,
        #[cfg(target_os = "linux")]
        probe_threads: Arc<AtomicBool>,
    }
    impl SessionStore {
        fn new(data: Arc<dyn ObjectStore>) -> Self {
            Self {
                data,
                head: None,
                script: Arc::new(Mutex::new(None)),
                observed: Arc::new(Observed::default()),
                list_error_at: Arc::new(AtomicUsize::new(usize::MAX)),
                delete_error: Arc::new(AtomicUsize::new(0)),
                release_error: Arc::new(AtomicUsize::new(0)),
                head_read_error: Arc::new(AtomicBool::new(false)),
                gate: Arc::new(Mutex::new(None)),
                list_gate: Arc::new(Mutex::new(None)),
                get_gate: Arc::new(Mutex::new(None)),
                #[cfg(target_os = "linux")]
                probe_threads: Arc::new(AtomicBool::new(false)),
            }
        }
        fn at(&self, path: &Path) -> &dyn ObjectStore {
            if path.as_ref().ends_with("/head.json") {
                if let Some(head) = &self.head {
                    return head.as_ref();
                }
            }
            self.data.as_ref()
        }
        async fn script(&self, paths: &[Path]) {
            assert!(paths.len() <= 16);
            let mut objects = Vec::new();
            for path in paths {
                objects.push(self.head(path).await.unwrap());
            }
            *self.script.lock().unwrap() = Some(objects);
        }
    }
    impl fmt::Display for SessionStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "GC session fixture")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for SessionStore {
        async fn put_opts(
            &self,
            path: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            let release = path.as_ref().ends_with("/head.json") && {
                let bytes: Vec<_> = payload.iter().flatten().copied().collect();
                serde_json::from_slice::<serde_json::Value>(&bytes)
                    .is_ok_and(|head| head["fence"].is_null())
            };
            let fault = if release {
                self.release_error.swap(0, Ordering::SeqCst)
            } else {
                0
            };
            if fault == 1 {
                return Err(injected());
            }
            let result = self.at(path).put_opts(path, payload, options).await?;
            if fault == 3 {
                self.head_read_error.store(true, Ordering::SeqCst);
            }
            if fault >= 2 {
                return Err(injected());
            }
            Ok(result)
        }
        async fn put_multipart_opts(
            &self,
            path: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.at(path).put_multipart_opts(path, options).await
        }
        async fn get_opts(
            &self,
            path: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let gate = {
                let mut slot = self.get_gate.lock().unwrap();
                if slot.as_ref().is_some_and(|(target, _)| target == path) {
                    slot.take().map(|(_, gate)| gate)
                } else {
                    None
                }
            };
            if let Some(gate) = gate {
                gate.wait().await?;
            }
            if path.as_ref().ends_with("/head.json")
                && self.head_read_error.swap(false, Ordering::SeqCst)
            {
                return Err(injected());
            }
            self.at(path).get_opts(path, options).await
        }
        fn delete_stream(
            &self,
            paths: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            let this = self.clone();
            paths
                .then(move |path| {
                    let this = this.clone();
                    async move {
                        let path = path?;
                        this.observed.deletes.fetch_add(1, Ordering::SeqCst);
                        let gate = this.gate.lock().unwrap().take();
                        if let Some(gate) = gate {
                            gate.wait().await?;
                        }
                        let fault = this.delete_error.swap(0, Ordering::SeqCst);
                        assert_ne!(fault, 3, "injected worker panic before DELETE effect");
                        if fault == 1 {
                            return Err(injected());
                        }
                        let result = this.at(&path).delete(&path).await;
                        if matches!(&result, Err(object_store::Error::NotFound { .. })) {
                            this.observed
                                .delete_not_found
                                .fetch_add(1, Ordering::SeqCst);
                        }
                        result?;
                        assert_ne!(fault, 4, "injected worker panic after DELETE effect");
                        if fault == 2 {
                            return Err(injected());
                        }
                        Ok(path)
                    }
                })
                .boxed()
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.observed.lists.fetch_add(1, Ordering::SeqCst);
            let objects = match self.script.lock().unwrap().clone() {
                Some(objects) => stream::iter(objects.into_iter().map(Ok)).boxed(),
                None => self.data.list(prefix),
            };
            let guard = ListLifetime(self.observed.clone(), std::thread::current().id());
            let error_at = self.list_error_at.clone();
            let list_gate = self.list_gate.clone();
            #[cfg(target_os = "linux")]
            let probe_threads = self.probe_threads.clone();
            stream::unfold(
                (objects, guard, 0usize),
                move |(mut objects, guard, index)| {
                    let error_at = error_at.clone();
                    let list_gate = list_gate.clone();
                    #[cfg(target_os = "linux")]
                    let probe_threads = probe_threads.clone();
                    async move {
                        #[cfg(target_os = "linux")]
                        if probe_threads.swap(false, Ordering::SeqCst) {
                            if let Err(error) = probe_runtime_threads(&guard.0).await {
                                return Some((Err(error), (objects, guard, index)));
                            }
                        }
                        let gate = list_gate.lock().unwrap().take();
                        if let Some(gate) = gate {
                            if let Err(error) = gate.wait().await {
                                return Some((Err(error), (objects, guard, index)));
                            }
                        }
                        // Continuation requires a live timer driver on the owning runtime.
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        let item = if error_at
                            .compare_exchange(index, usize::MAX, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                        {
                            Some(Err(injected()))
                        } else {
                            objects.next().await
                        };
                        item.map(|item| {
                            guard.0.polls.fetch_add(1, Ordering::SeqCst);
                            (item, (objects, guard, index + 1))
                        })
                    }
                },
            )
            .boxed()
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.data.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.data.copy_opts(from, to, options).await
        }
    }

    struct Fixture {
        store: Arc<SessionStore>,
        prefix: Path,
        root: Path,
        directory: tempfile::TempDir,
    }
    impl Fixture {
        async fn new() -> Self {
            Self::with_store(SessionStore::new(Arc::new(InMemory::new()))).await
        }
        async fn with_store(store: SessionStore) -> Self {
            let store = Arc::new(store);
            let prefix = Path::from("session");
            let base = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 1, None)
                .await
                .unwrap();
            Self {
                store,
                prefix,
                root: base.metadata_prefix().join("manifest.json"),
                directory: tempfile::tempdir().unwrap(),
            }
        }
        fn session(&self) -> TwoBitGcSession {
            TwoBitGcSession::new(self.store.clone(), &self.prefix, self.directory.path()).unwrap()
        }
        async fn object(&self, name: &str, bytes: &'static [u8]) -> Path {
            let path = self.prefix.clone().join(name);
            self.store
                .put(&path, PutPayload::from_static(bytes))
                .await
                .unwrap();
            path
        }
        async fn orphan(&self, id: u8, bytes: &'static [u8]) -> Path {
            self.object(
                &format!("maintenance/{}/objects/{id:064x}", "a".repeat(48)),
                bytes,
            )
            .await
        }
        async fn finish(&self, session: TwoBitGcSession) {
            self.close(session)
                .await
                .expect("session worker must drain and tear down");
        }
        async fn close(&self, session: TwoBitGcSession) -> Result<(), tokio::time::error::Elapsed> {
            drop(session);
            tokio::time::timeout(DEADLINE, async {
                while Arc::strong_count(&self.store) != 1 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
        }
    }

    async fn run(
        session: &mut TwoBitGcSession,
        limits: TwoBitGcLimits,
    ) -> Result<TwoBitGcReport, TwoBitGcFailure> {
        tokio::time::timeout(DEADLINE, collect_two_bit_garbage(session, limits))
            .await
            .expect("bounded GC call")
    }
    async fn drain(session: &mut TwoBitGcSession) -> Result<TwoBitGcReport, TwoBitGcFailure> {
        drain_bounded(session)
            .await
            .expect("accepted request must finish")
    }
    async fn drain_bounded(
        session: &mut TwoBitGcSession,
    ) -> Result<Result<TwoBitGcReport, TwoBitGcFailure>, tokio::time::error::Elapsed> {
        tokio::time::timeout(DEADLINE, async {
            loop {
                let result = collect_two_bit_garbage(session, caps()).await;
                if matches!(&result, Err(e) if matches!(e.error, TwoBitStoreError::Invalid("GC session busy"))) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                } else {
                    break result;
                }
            }
        }).await
    }

    #[tokio::test]
    async fn four_retained_or_unknown_entries_cannot_starve_an_unordered_orphan() {
        let f = Fixture::new().await;
        let unknown_z = f.object("z-unknown", b"keep").await;
        let unknown_a = f.object("a-unknown", b"keep").await;
        let orphan = f.orphan(1, b"orphan").await;
        f.store
            .script(&[
                unknown_z.clone(),
                f.prefix.clone().join("head.json"),
                unknown_a.clone(),
                f.root.clone(),
                orphan.clone(),
            ])
            .await;
        let mut session = f.session();
        let limits = TwoBitGcLimits {
            max_objects_scanned: 4,
            ..caps()
        };
        let first = run(&mut session, limits).await.unwrap();
        let second = run(&mut session, limits).await.unwrap();
        let lists_at_eof = f.store.observed.lists.load(Ordering::SeqCst);
        let third = run(&mut session, limits).await.unwrap();
        f.finish(session).await;
        assert_eq!((first.objects_scanned, first.objects_deleted), (4, 0));
        assert!(!first.scan_complete);
        assert_eq!((second.objects_scanned, second.objects_deleted), (1, 1));
        assert!(second.scan_complete);
        assert_eq!(lists_at_eof, 1);
        assert_eq!(f.store.observed.lists.load(Ordering::SeqCst), 2);
        assert_eq!(third.objects_scanned, 4);
        assert!(f.store.head(&orphan).await.is_err());
        assert!(f.store.head(&unknown_z).await.is_ok());
        assert!(f.store.head(&unknown_a).await.is_ok());
        assert!(f.store.observed.drop_in_runtime.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn delete_cap_does_not_consume_the_next_entry_and_eof_requires_a_poll() {
        let f = Fixture::new().await;
        let a = f.orphan(2, b"a").await;
        let b = f.orphan(1, b"b").await;
        f.store.script(&[a, b]).await;
        let mut session = f.session();
        let limits = TwoBitGcLimits {
            max_objects_deleted: 1,
            ..caps()
        };
        let first = run(&mut session, limits).await.unwrap();
        let polls_after_first = f.store.observed.polls.load(Ordering::SeqCst);
        let second = run(&mut session, limits).await.unwrap();
        let eof = run(&mut session, limits).await.unwrap();
        f.finish(session).await;
        assert_eq!(
            (
                first.objects_scanned,
                first.objects_deleted,
                polls_after_first
            ),
            (1, 1, 1)
        );
        assert_eq!((second.objects_scanned, second.objects_deleted), (1, 1));
        assert!(!first.scan_complete && !second.scan_complete);
        assert!(eof.scan_complete);
        assert_eq!(eof.objects_scanned, 0);
        assert_eq!(f.store.observed.lists.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn residual_bytes_retain_one_candidate_and_pending_inspection_uses_scan_budget() {
        let f = Fixture::new().await;
        let a = f.orphan(2, b"123456").await;
        let b = f.orphan(1, b"12345").await;
        f.store.script(&[a, b.clone()]).await;
        let mut session = f.session();
        let first = run(
            &mut session,
            TwoBitGcLimits {
                max_deleted_object_bytes: 8,
                ..caps()
            },
        )
        .await
        .unwrap();
        let second = run(
            &mut session,
            TwoBitGcLimits {
                max_objects_scanned: 1,
                ..caps()
            },
        )
        .await
        .unwrap();
        let polls = f.store.observed.polls.load(Ordering::SeqCst);
        let eof = run(&mut session, caps()).await.unwrap();
        f.finish(session).await;
        assert_eq!(
            (
                first.objects_scanned,
                first.objects_deleted,
                first.deleted_object_bytes
            ),
            (2, 1, 6)
        );
        assert_eq!(first.pending_object_bytes, Some(5));
        assert_eq!(first.required_deleted_object_bytes, None);
        assert_eq!(
            (
                second.objects_scanned,
                second.objects_deleted,
                second.deleted_object_bytes
            ),
            (1, 1, 5)
        );
        assert_eq!(polls, 2);
        assert!(!second.scan_complete);
        assert!(eof.scan_complete);
        assert!(f.store.head(&b).await.is_err());
    }

    #[tokio::test]
    async fn whole_cap_refusal_allows_mutation_then_larger_retry() {
        let f = Fixture::new().await;
        let orphan = f.orphan(1, b"12345").await;
        f.store.script(&[orphan.clone()]).await;
        let mut session = f.session();
        let small = TwoBitGcLimits {
            max_deleted_object_bytes: 4,
            ..caps()
        };
        let first = run(&mut session, small).await.unwrap();
        let again = run(&mut session, small).await.unwrap();
        let polls = f.store.observed.polls.load(Ordering::SeqCst);
        let mutation_allowed = match read_two_bit_head(f.store.as_ref(), &f.prefix).await {
            Ok(Some(head)) => apply_two_bit_mutations(
                f.store.as_ref(),
                &head,
                2,
                None,
                &[TwoBitMutation {
                    id: 7,
                    vector: Some(vec![1., 0.]),
                }],
                caps().mutations,
            )
            .await
            .is_ok(),
            _ => false,
        };
        let larger = run(
            &mut session,
            TwoBitGcLimits {
                max_deleted_object_bytes: 5,
                ..caps()
            },
        )
        .await
        .unwrap();
        f.finish(session).await;
        assert_eq!(first.required_deleted_object_bytes, Some(5));
        assert_eq!(again.required_deleted_object_bytes, Some(5));
        assert!(
            mutation_allowed,
            "a nondestructive budget stop must reopen writes"
        );
        assert_eq!(
            (again.objects_scanned, again.delete_attempts, polls),
            (1, 0, 1)
        );
        assert!(larger.scan_complete);
        assert_eq!(larger.deleted_object_bytes, 5);
        assert!(f.store.head(&orphan).await.is_err());
    }

    #[tokio::test]
    async fn list_failure_discards_only_traversal_and_reports_restart() {
        let f = Fixture::new().await;
        let unknown = f.object("z-unknown", b"keep").await;
        let orphan = f.orphan(1, b"orphan").await;
        f.store.script(&[unknown, orphan.clone()]).await;
        f.store.list_error_at.store(1, Ordering::SeqCst);
        let mut session = f.session();
        let first = run(
            &mut session,
            TwoBitGcLimits {
                max_objects_scanned: 1,
                ..caps()
            },
        )
        .await
        .unwrap();
        let failed = run(&mut session, caps()).await.unwrap_err();
        let fenced = read_two_bit_head(f.store.as_ref(), &f.prefix)
            .await
            .is_err();
        let retried = run(&mut session, caps()).await.unwrap();
        f.finish(session).await;
        assert_eq!(first.objects_scanned, 1);
        assert!(failed.report.restart_required);
        assert!(!failed.report.scan_complete);
        assert!(fenced);
        assert!(retried.scan_complete);
        assert_eq!((retried.objects_scanned, retried.objects_deleted), (2, 1));
        assert_eq!(f.store.observed.lists.load(Ordering::SeqCst), 2);
        assert!(f.store.head(&orphan).await.is_err());
    }

    #[tokio::test]
    async fn delete_errors_before_or_after_effect_retain_candidate_and_durable_fence() {
        for effect in [1, 2] {
            let f = Fixture::new().await;
            let orphan = f.orphan(1, b"orphan").await;
            f.store.script(&[orphan.clone()]).await;
            f.store.delete_error.store(effect, Ordering::SeqCst);
            let mut session = f.session();
            let failed = run(&mut session, caps()).await.unwrap_err();
            let exists_after_error = f.store.head(&orphan).await.is_ok();
            let fenced = read_two_bit_head(f.store.as_ref(), &f.prefix)
                .await
                .is_err();
            let retried = run(&mut session, caps()).await.unwrap();
            f.finish(session).await;
            assert_eq!(exists_after_error, effect == 1);
            assert!(fenced && failed.report.delete_outcome_unknown);
            assert_eq!(
                (failed.report.delete_attempts, failed.report.objects_deleted),
                (1, 0)
            );
            assert_eq!(failed.report.pending_object_bytes, Some(6));
            assert_eq!((retried.objects_scanned, retried.objects_deleted), (1, 1));
            assert!(retried.scan_complete);
            assert_eq!(f.store.observed.lists.load(Ordering::SeqCst), 1);
            assert_eq!(f.store.observed.deletes.load(Ordering::SeqCst), 2);
            assert!(f.store.head(&orphan).await.is_err());
        }
    }

    #[tokio::test]
    async fn real_filesystem_absent_pending_candidate_reopens_reads_and_mutations() {
        for after_effect in [true, false] {
            let data_dir = tempfile::tempdir().unwrap();
            let mut store = SessionStore::new(Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(data_dir.path()).unwrap(),
            ));
            store.head = Some(Arc::new(InMemory::new()));
            let f = Fixture::with_store(store).await;
            let orphan = f.orphan(1, b"orphan").await;
            let mut session = f.session();
            if after_effect {
                f.store.delete_error.store(2, Ordering::SeqCst);
            }
            let first = run(
                &mut session,
                TwoBitGcLimits {
                    max_deleted_object_bytes: if after_effect { 6 } else { 5 },
                    ..caps()
                },
            )
            .await;
            let fenced =
                tokio::time::timeout(DEADLINE, read_two_bit_head(f.store.as_ref(), &f.prefix))
                    .await
                    .expect("bounded latest read")
                    .is_err();
            let competing = if after_effect {
                None
            } else {
                // Another cooperating session removes the budget-retained candidate.
                let mut other = f.session();
                let result = run(&mut other, caps()).await;
                drop(other);
                Some(result)
            };
            let lists_before_retry = f.store.observed.lists.load(Ordering::SeqCst);
            let retried = run(&mut session, caps()).await;
            let reopened = tokio::time::timeout(DEADLINE, async {
                let head = read_two_bit_head(f.store.as_ref(), &f.prefix)
                    .await?
                    .ok_or(TwoBitStoreError::Invalid("fixture head absent"))?;
                apply_two_bit_mutations(
                    f.store.as_ref(),
                    &head,
                    2,
                    None,
                    &[TwoBitMutation {
                        id: 7,
                        vector: Some(vec![1., 0.]),
                    }],
                    caps().mutations,
                )
                .await
            })
            .await;
            f.finish(session).await;
            let first_report = if after_effect {
                let failed = first.unwrap_err();
                assert!(failed.report.delete_outcome_unknown);
                failed.report
            } else {
                let capped = first.unwrap();
                assert_eq!(capped.required_deleted_object_bytes, Some(6));
                let competing = competing.unwrap().unwrap();
                assert!(competing.scan_complete);
                assert_eq!(competing.objects_deleted, 1);
                capped
            };
            assert_eq!(fenced, after_effect);
            assert_eq!(first_report.pending_object_bytes, Some(6));
            assert_eq!(first_report.delete_attempts, usize::from(after_effect));
            assert_eq!(first_report.objects_deleted, 0);
            assert_eq!(first_report.deleted_object_bytes, 0);
            let retried = retried.unwrap();
            assert!(retried.scan_complete);
            assert!(!retried.delete_outcome_unknown && !retried.restart_required);
            assert_eq!(retried.delete_attempts, 1);
            assert_eq!(retried.objects_deleted, 0);
            assert_eq!(retried.deleted_object_bytes, 0);
            assert_eq!(retried.pending_object_bytes, None);
            assert_eq!(retried.required_deleted_object_bytes, None);
            assert_eq!(f.store.observed.delete_not_found.load(Ordering::SeqCst), 1);
            assert_eq!(lists_before_retry, if after_effect { 1 } else { 2 });
            assert_eq!(
                f.store.observed.lists.load(Ordering::SeqCst),
                lists_before_retry
            );
            assert!(
                matches!(reopened, Ok(Ok(_))),
                "latest reads and mutation must reopen"
            );
            assert!(f.store.head(&orphan).await.is_err());
        }
    }

    #[tokio::test]
    async fn fence_release_reconciliation_and_unresolved_errors_preserve_continuation() {
        for effect in [1, 2, 3] {
            let f = Fixture::new().await;
            let a = f.orphan(2, b"a").await;
            let b = f.orphan(1, b"b").await;
            f.store.script(&[a, b.clone()]).await;
            f.store.release_error.store(effect, Ordering::SeqCst);
            let mut session = f.session();
            let first = run(
                &mut session,
                TwoBitGcLimits {
                    max_objects_deleted: 1,
                    ..caps()
                },
            )
            .await;
            let unread_reconciliation_fault = f.store.head_read_error.load(Ordering::SeqCst);
            let fenced = read_two_bit_head(f.store.as_ref(), &f.prefix)
                .await
                .is_err();
            let retried = run(&mut session, caps()).await.unwrap();
            f.finish(session).await;
            let report = if effect == 2 {
                first.expect("successful reread reconciles the release")
            } else {
                first.expect_err("unresolved release must fail").report
            };
            assert!(!unread_reconciliation_fault);
            assert_eq!(report.objects_deleted, 1);
            assert!(!report.scan_complete);
            assert_eq!(fenced, effect == 1);
            assert_eq!(retried.objects_deleted, 1);
            assert!(retried.scan_complete);
            assert_eq!(f.store.observed.lists.load(Ordering::SeqCst), 1);
            assert!(f.store.head(&b).await.is_err());
        }
    }

    #[tokio::test]
    async fn admission_reader_lock_and_keep_validation_failures_do_not_advance() {
        let f = Fixture::new().await;
        let base = read_two_bit_head(f.store.as_ref(), &f.prefix)
            .await
            .unwrap()
            .unwrap();
        apply_two_bit_mutations(
            f.store.as_ref(),
            &base,
            2,
            None,
            &[TwoBitMutation {
                id: 1,
                vector: Some(vec![1., 0.]),
            }],
            caps().mutations,
        )
        .await
        .unwrap();
        let unknown = f.object("unknown", b"keep").await;
        let orphan = f.orphan(1, b"orphan").await;
        f.store.script(&[unknown, orphan.clone()]).await;
        let mut session = f.session();
        run(
            &mut session,
            TwoBitGcLimits {
                max_objects_scanned: 1,
                ..caps()
            },
        )
        .await
        .unwrap();
        let admission = run(
            &mut session,
            TwoBitGcLimits {
                max_objects_scanned: 0,
                ..caps()
            },
        )
        .await;
        let reader = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(f.directory.path().join("lifecycle.lock"))
            .unwrap();
        reader.try_lock_shared().unwrap();
        let locked = run(&mut session, caps()).await;
        drop(reader);
        let validation = run(
            &mut session,
            TwoBitGcLimits {
                mutations: TwoBitMutationLimits {
                    max_snapshot_bytes: 100,
                    ..caps().mutations
                },
                ..caps()
            },
        )
        .await;
        let polls_before_retry = f.store.observed.polls.load(Ordering::SeqCst);
        let readable = read_two_bit_head(f.store.as_ref(), &f.prefix).await.is_ok();
        let retried = run(&mut session, caps()).await.unwrap();
        f.finish(session).await;
        assert!(admission.is_err() && locked.is_err() && validation.is_err());
        assert_eq!(polls_before_retry, 1);
        assert!(readable && retried.scan_complete);
        assert_eq!(retried.objects_deleted, 1);
        assert!(f.store.head(&orphan).await.is_err());
    }

    #[tokio::test]
    async fn a_new_authenticated_head_protects_the_pending_candidate() {
        let f = Fixture::new().await;
        let head_key = f.prefix.clone().join("head.json");
        let old_bytes = f.store.get(&head_key).await.unwrap().bytes().await.unwrap();
        let base = read_two_bit_head(f.store.as_ref(), &f.prefix)
            .await
            .unwrap()
            .unwrap();
        borsuk::two_bit_mutations::seal_two_bit_mutations(
            f.store.as_ref(),
            &base,
            None,
            caps().mutations,
        )
        .await
        .unwrap();
        let next = publish_empty_two_bit_generation(f.store.as_ref(), &f.prefix, 2, 2, Some(&base))
            .await
            .unwrap();
        let next_bytes = f.store.get(&head_key).await.unwrap().bytes().await.unwrap();
        let candidate = next.metadata_prefix().join("manifest.json");
        // An authorized fixture writer changes authority only between released fences.
        f.store.put(&head_key, old_bytes.into()).await.unwrap();
        f.store.script(&[candidate.clone()]).await;
        let mut session = f.session();
        let small = TwoBitGcLimits {
            max_deleted_object_bytes: 1,
            ..caps()
        };
        let pending = run(&mut session, small).await.unwrap();
        f.store.put(&head_key, next_bytes.into()).await.unwrap();
        let protected = run(
            &mut session,
            TwoBitGcLimits {
                max_objects_scanned: 1,
                ..small
            },
        )
        .await
        .unwrap();
        let eof = run(&mut session, small).await.unwrap();
        f.finish(session).await;
        assert!(pending.required_deleted_object_bytes.is_some());
        assert_eq!(
            (protected.objects_scanned, protected.delete_attempts),
            (1, 0)
        );
        assert_eq!(protected.pending_object_bytes, None);
        assert!(!protected.scan_complete && eof.scan_complete);
        assert!(f.store.head(&candidate).await.is_ok());
        assert_eq!(f.store.observed.lists.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dropped_waiter_keeps_delete_lock_and_rejects_busy_session() {
        let f = Fixture::new().await;
        let orphan = f.orphan(1, b"orphan").await;
        f.store.script(&[orphan.clone()]).await;
        let gate = Arc::new(Gate::new());
        let release = ReleaseOnDrop(gate.clone());
        *f.store.gate.lock().unwrap() = Some(gate.clone());
        let mut session = f.session();
        let mut request = Box::pin(collect_two_bit_garbage(&mut session, caps()));
        let dispatched = futures_util::poll!(&mut request).is_pending();
        let entered = tokio::time::timeout(DEADLINE, gate.entered.notified()).await;
        drop(request);
        let busy =
            tokio::time::timeout(DEADLINE, collect_two_bit_garbage(&mut session, caps())).await;
        let reader = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(f.directory.path().join("lifecycle.lock"));
        let excluded = reader
            .as_ref()
            .is_ok_and(|lock| lock.try_lock_shared().is_err());
        drop(reader);
        let fenced = read_two_bit_head(f.store.as_ref(), &f.prefix)
            .await
            .is_err();
        drop(release);
        let drained = drain(&mut session).await;
        f.finish(session).await;
        assert!(dispatched && entered.is_ok());
        assert!(
            matches!(busy, Ok(Err(e)) if matches!(e.error, TwoBitStoreError::Invalid("GC session busy")))
        );
        assert!(excluded && fenced);
        assert!(drained.is_ok());
        assert!(f.store.head(&orphan).await.is_err());
    }

    #[test]
    fn caller_runtime_and_session_drop_leave_accepted_work_owned_until_teardown() {
        for drop_session in [false, true] {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let (f, orphan, mut session, gate, release, entered) = runtime.block_on(async {
                let f = Fixture::new().await;
                let orphan = f.orphan(1, b"orphan").await;
                f.store.script(&[orphan.clone()]).await;
                let gate = Arc::new(Gate::new());
                let release = ReleaseOnDrop(gate.clone());
                *f.store.gate.lock().unwrap() = Some(gate.clone());
                let mut session = f.session();
                let mut request = Box::pin(collect_two_bit_garbage(&mut session, caps()));
                let _ = futures_util::poll!(&mut request);
                let entered = tokio::time::timeout(DEADLINE, gate.entered.notified()).await;
                drop(request);
                (f, orphan, session, gate, release, entered)
            });
            let runtime_drop_started = std::time::Instant::now();
            drop(runtime);
            let runtime_drop_elapsed = runtime_drop_started.elapsed();
            let second_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            if drop_session {
                second_runtime.block_on(async {
                    let started = std::time::Instant::now();
                    drop(session);
                    let drop_elapsed = started.elapsed();
                    drop(release);
                    let drained = tokio::time::timeout(DEADLINE, async {
                        while Arc::strong_count(&f.store) != 1 {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    })
                    .await;
                    assert!(entered.is_ok() && drained.is_ok());
                    assert!(runtime_drop_elapsed < Duration::from_secs(1));
                    assert!(drop_elapsed < Duration::from_secs(1));
                    assert!(f.store.head(&orphan).await.is_err());
                    assert!(read_two_bit_head(f.store.as_ref(), &f.prefix).await.is_ok());
                    assert!(f.store.observed.list_drops.load(Ordering::SeqCst) > 0);
                    assert!(f.store.observed.drop_in_runtime.load(Ordering::SeqCst));
                });
            } else {
                drop(release);
                second_runtime.block_on(async {
                    let result = drain(&mut session).await;
                    f.finish(session).await;
                    assert!(entered.is_ok() && result.is_ok());
                    assert!(runtime_drop_elapsed < Duration::from_secs(1));
                    assert!(f.store.head(&orphan).await.is_err());
                    assert!(read_two_bit_head(f.store.as_ref(), &f.prefix).await.is_ok());
                });
            }
            drop(gate);
        }
    }

    async fn cancelled_before_delete(validation: bool) {
        let (f, orphan, snapshot) = tokio::time::timeout(DEADLINE, async {
            let f = Fixture::new().await;
            let base = read_two_bit_head(f.store.as_ref(), &f.prefix)
                .await
                .unwrap()
                .unwrap();
            let snapshot = apply_two_bit_mutations(
                f.store.as_ref(),
                &base,
                2,
                None,
                &[TwoBitMutation {
                    id: 7,
                    vector: Some(vec![1., 0.]),
                }],
                caps().mutations,
            )
            .await
            .unwrap();
            let snapshot = base
                .metadata_prefix()
                .join("mutations")
                .join(snapshot.sha256());
            let unknown = f.object("unknown", b"k").await;
            let orphan = f.orphan(1, b"x").await;
            f.store
                .script(&[unknown, orphan.clone(), f.root.clone(), snapshot.clone()])
                .await;
            (f, orphan, snapshot)
        })
        .await
        .expect("bounded cancellation fixture");
        let mut session = f.session();
        let first = tokio::time::timeout(
            DEADLINE,
            collect_two_bit_garbage(
                &mut session,
                TwoBitGcLimits {
                    max_objects_scanned: 1,
                    ..caps()
                },
            ),
        )
        .await;
        let gate = Arc::new(Gate::new());
        let release = ReleaseOnDrop(gate.clone());
        if validation {
            *f.store.get_gate.lock().unwrap() = Some((snapshot.clone(), gate.clone()));
        } else {
            *f.store.list_gate.lock().unwrap() = Some(gate.clone());
        }
        let blocked = tokio::time::timeout(DEADLINE, async {
            let mut request = Box::pin(collect_two_bit_garbage(
                &mut session,
                TwoBitGcLimits {
                    max_objects_scanned: 1,
                    ..caps()
                },
            ));
            let dispatched = futures_util::poll!(&mut request).is_pending();
            gate.entered.notified().await;
            drop(request);
            let busy = collect_two_bit_garbage(&mut session, caps()).await;
            let reader = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(f.directory.path().join("lifecycle.lock"));
            let excluded = reader
                .as_ref()
                .is_ok_and(|lock| lock.try_lock_shared().is_err());
            drop(reader);
            let fenced = matches!(
                read_two_bit_head(f.store.as_ref(), &f.prefix).await,
                Err(TwoBitStoreError::Invalid("writes fenced for reclamation"))
            );
            let progress = || {
                (
                    f.store.observed.lists.load(Ordering::SeqCst),
                    f.store.observed.polls.load(Ordering::SeqCst),
                    f.store.observed.deletes.load(Ordering::SeqCst),
                )
            };
            let before = progress();
            tokio::time::sleep(Duration::from_millis(20)).await;
            let after = progress();
            let present = f.store.head(&orphan).await.is_ok();
            (dispatched, busy, excluded, fenced, before, after, present)
        })
        .await;
        // Release and drain on timeout/error too; no assertion may strand accepted work.
        drop(release);
        let drained = drain_bounded(&mut session).await;
        let closed = f.close(session).await;
        let readable = tokio::time::timeout(DEADLINE, async {
            let head = read_two_bit_head(f.store.as_ref(), &f.prefix)
                .await?
                .unwrap();
            let snapshot = read_two_bit_mutations(f.store.as_ref(), &head, 2, caps().mutations)
                .await?
                .unwrap();
            Ok::<_, TwoBitStoreError>((
                snapshot.rows().to_vec(),
                f.store.head(&orphan).await.is_err(),
            ))
        })
        .await;
        assert!(
            matches!(first, Ok(Ok(report)) if report.objects_scanned == 1 && !report.scan_complete)
        );
        let (dispatched, busy, excluded, fenced, before, after, present) = blocked.unwrap();
        assert!(dispatched && excluded && fenced && present);
        assert!(
            matches!(busy, Err(e) if matches!(e.error, TwoBitStoreError::Invalid("GC session busy")))
        );
        assert_eq!(before, (1, 1, 0));
        assert_eq!(after, before);
        assert!(matches!(drained, Ok(Ok(_))) && closed.is_ok());
        let (rows, absent) = readable.unwrap().unwrap();
        assert_eq!(
            rows,
            vec![TwoBitMutation {
                id: 7,
                vector: Some(vec![1., 0.])
            }]
        );
        assert!(absent);
        assert_eq!(f.store.observed.deletes.load(Ordering::SeqCst), 1);
        assert_eq!(f.store.observed.lists.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dropped_waiter_during_list_poll_keeps_lock_and_progress() {
        cancelled_before_delete(false).await;
    }

    #[tokio::test]
    async fn dropped_waiter_during_keep_validation_keeps_lock_and_progress() {
        cancelled_before_delete(true).await;
    }

    #[cfg(panic = "unwind")]
    #[tokio::test]
    async fn worker_panic_reports_unknown_progress_and_recreation_recovers() {
        for fault in [3, 4] {
            let (f, orphan) = tokio::time::timeout(DEADLINE, async {
                let f = Fixture::new().await;
                let orphan = f.orphan(1, b"x").await;
                f.store.script(&[orphan.clone(), f.root.clone()]).await;
                (f, orphan)
            })
            .await
            .expect("bounded panic fixture");
            f.store.delete_error.store(fault, Ordering::SeqCst);
            let mut session = f.session();
            let failed =
                tokio::time::timeout(DEADLINE, collect_two_bit_garbage(&mut session, caps())).await;
            // Receiver teardown may trail reply destruction during unwinding, but cannot stay busy.
            let failed_again = drain_bounded(&mut session).await;
            let closed = f.close(session).await;
            let before_recovery = tokio::time::timeout(DEADLINE, async {
                let fenced = matches!(
                    read_two_bit_head(f.store.as_ref(), &f.prefix).await,
                    Err(TwoBitStoreError::Invalid("writes fenced for reclamation"))
                );
                let present = f.store.head(&orphan).await.is_ok();
                let reader = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(f.directory.path().join("lifecycle.lock"));
                let unlocked = reader
                    .as_ref()
                    .is_ok_and(|lock| lock.try_lock_shared().is_ok());
                (
                    fenced,
                    present,
                    unlocked,
                    f.store.observed.deletes.load(Ordering::SeqCst),
                )
            })
            .await;
            assert!(closed.is_ok());
            let mut recreated = f.session();
            let recovered =
                tokio::time::timeout(DEADLINE, collect_two_bit_garbage(&mut recreated, caps()))
                    .await;
            let recovered_closed = f.close(recreated).await;
            let after_recovery = tokio::time::timeout(DEADLINE, async {
                (
                    read_two_bit_head(f.store.as_ref(), &f.prefix).await.is_ok(),
                    f.store.head(&f.root).await.is_ok(),
                    f.store.head(&orphan).await.is_err(),
                )
            })
            .await;
            assert!(recovered_closed.is_ok());
            for failure in [
                failed.unwrap().unwrap_err(),
                failed_again.unwrap().unwrap_err(),
            ] {
                assert!(matches!(
                    failure.error,
                    TwoBitStoreError::Invalid("GC worker failed; progress unknown")
                ));
                let report = failure.report;
                assert!(report.restart_required && report.delete_outcome_unknown);
                assert!(!report.scan_complete);
                assert_eq!(
                    (
                        report.objects_scanned,
                        report.unknown_objects,
                        report.delete_attempts,
                        report.objects_deleted,
                        report.deleted_object_bytes
                    ),
                    (0, 0, 0, 0, 0)
                );
                assert_eq!(
                    (
                        report.pending_object_bytes,
                        report.required_deleted_object_bytes
                    ),
                    (None, None)
                );
            }
            assert_eq!(before_recovery.unwrap(), (true, fault == 3, true, 1));
            let recovered = recovered.unwrap().unwrap();
            // InMemory acknowledges an idempotent delete even after the first DELETE took effect.
            assert_eq!(
                (
                    recovered.objects_scanned,
                    recovered.delete_attempts,
                    recovered.objects_deleted,
                    recovered.deleted_object_bytes
                ),
                (2, 1, 1, 1)
            );
            assert!(
                recovered.scan_complete
                    && !recovered.restart_required
                    && !recovered.delete_outcome_unknown
            );
            assert_eq!(after_recovery.unwrap(), (true, true, true));
            assert_eq!(f.store.observed.deletes.load(Ordering::SeqCst), 2);
            assert_eq!(f.store.observed.lists.load(Ordering::SeqCst), 2);
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn session_drop_terminates_recorded_worker_and_runtime_helpers() {
        let f = tokio::time::timeout(DEADLINE, Fixture::new())
            .await
            .unwrap();
        let unknown = f.object("unknown", b"k").await;
        f.store.script(&[unknown, f.root.clone()]).await;
        f.store.probe_threads.store(true, Ordering::SeqCst);
        let mut session = f.session();
        let capped = tokio::time::timeout(
            DEADLINE,
            collect_two_bit_garbage(
                &mut session,
                TwoBitGcLimits {
                    max_objects_scanned: 1,
                    ..caps()
                },
            ),
        )
        .await;
        let retained = f.store.observed.list_drops.load(Ordering::SeqCst);
        let alive = f
            .store
            .observed
            .threads
            .lock()
            .unwrap()
            .iter()
            .all(|thread| matches!(thread.exited(), Ok(false)));
        let gate = Arc::new(Gate::new());
        let release = ReleaseOnDrop(gate.clone());
        *f.store.list_gate.lock().unwrap() = Some(gate.clone());
        let blocked = tokio::time::timeout(DEADLINE, async {
            let mut request = Box::pin(collect_two_bit_garbage(&mut session, caps()));
            let dispatched = futures_util::poll!(&mut request).is_pending();
            gate.entered.notified().await;
            drop(request);
            dispatched
        })
        .await;
        let started = std::time::Instant::now();
        drop(session);
        let drop_elapsed = started.elapsed();
        drop(release);
        // Observe complete worker + runtime shutdown after accepted work drains, without joining.
        let exited = tokio::time::timeout(DEADLINE, async {
            loop {
                let gone = f
                    .store
                    .observed
                    .threads
                    .lock()
                    .unwrap()
                    .iter()
                    .map(OsThread::exited)
                    .collect::<std::io::Result<Vec<_>>>()?;
                if gone.iter().all(|gone| *gone) {
                    break Ok::<_, std::io::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        assert!(
            matches!(capped, Ok(Ok(report)) if report.objects_scanned == 1 && !report.scan_complete)
        );
        assert_eq!(retained, 0);
        assert!(alive && matches!(blocked, Ok(true)) && matches!(exited, Ok(Ok(()))));
        assert!(drop_elapsed < Duration::from_secs(1));
        assert_eq!(Arc::strong_count(&f.store), 1);
        let threads = f.store.observed.threads.lock().unwrap();
        eprintln!("GC worker/runtime thread identities: {threads:?}");
        assert_eq!(threads.len(), 3);
        assert_eq!(threads[0].name, "two-bit-gc");
        assert_eq!(threads[1].name, "two-bit-gc-io");
        assert_eq!(threads[2].name, "two-bit-gc-io");
        for (index, thread) in threads.iter().enumerate() {
            assert_ne!(thread.rust_id, std::thread::current().id());
            for other in &threads[..index] {
                assert_ne!(thread.rust_id, other.rust_id);
                assert_ne!(thread.stat, other.stat);
            }
        }
        assert_eq!(f.store.observed.list_drops.load(Ordering::SeqCst), 1);
        assert!(f.store.observed.drop_in_runtime.load(Ordering::SeqCst));
        assert!(f.store.observed.drop_on_list_thread.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn recreation_restarts_safely_and_invalid_prefix_never_starts_io() {
        let f = Fixture::new().await;
        let unknown = f.object("unknown", b"keep").await;
        let orphan = f.orphan(1, b"orphan").await;
        f.store.script(&[unknown, orphan.clone()]).await;
        let invalid = TwoBitGcSession::new(f.store.clone(), &Path::from(""), f.directory.path());
        let mut first = f.session();
        let capped = run(
            &mut first,
            TwoBitGcLimits {
                max_objects_scanned: 1,
                ..caps()
            },
        )
        .await
        .unwrap();
        f.finish(first).await;
        let mut second = f.session();
        let restarted = run(&mut second, caps()).await.unwrap();
        f.finish(second).await;
        assert!(invalid.is_err());
        assert_eq!(capped.objects_scanned, 1);
        assert_eq!(
            (restarted.objects_scanned, restarted.objects_deleted),
            (2, 1)
        );
        assert!(restarted.scan_complete);
        assert_eq!(f.store.observed.lists.load(Ordering::SeqCst), 2);
        assert!(f.store.head(&orphan).await.is_err());
    }

    #[tokio::test]
    async fn real_filesystem_list_survives_caps_and_fresh_metadata_io() {
        let data_dir = tempfile::tempdir().unwrap();
        let mut store = SessionStore::new(Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(data_dir.path()).unwrap(),
        ));
        store.head = Some(Arc::new(InMemory::new()));
        let f = Fixture::with_store(store).await;
        for id in 0..10 {
            f.object(&format!("unknown-{id}"), b"keep").await;
        }
        let orphan = f.orphan(1, b"orphan").await;
        let mut session = f.session();
        let outcome = tokio::time::timeout(DEADLINE, async {
            let mut scanned = 0;
            let mut deleted = 0;
            for _ in 0..16 {
                let report = collect_two_bit_garbage(
                    &mut session,
                    TwoBitGcLimits {
                        max_objects_scanned: 1,
                        ..caps()
                    },
                )
                .await?;
                scanned += report.objects_scanned;
                deleted += report.objects_deleted;
                if report.scan_complete {
                    return Ok::<_, TwoBitGcFailure>(Some((scanned, deleted)));
                }
            }
            Ok(None)
        })
        .await;
        f.finish(session).await;
        assert_eq!(outcome.unwrap().unwrap(), Some((12, 1)));
        assert_eq!(f.store.observed.lists.load(Ordering::SeqCst), 1);
        assert!(f.store.head(&orphan).await.is_err());
        assert!(f.store.head(&f.root).await.is_ok());
    }

    #[test]
    fn real_filesystem_list_crosses_sdk_batch_with_small_caps_and_fresh_metadata() {
        // The sole >16-object fixture: 1033 one-byte orphans cross SDK 0.14.1's 1024 batch.
        const ORPHANS: usize = 1033;
        struct CallerRuntime(Option<tokio::runtime::Runtime>);
        impl Drop for CallerRuntime {
            fn drop(&mut self) {
                if let Some(runtime) = self.0.take() {
                    runtime.shutdown_timeout(DEADLINE);
                }
            }
        }
        let runtime = CallerRuntime(Some(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .max_blocking_threads(2)
                .build()
                .unwrap(),
        ));
        runtime.0.as_ref().unwrap().block_on(async {
            let data_dir = tempfile::tempdir().unwrap();
            let (f, unknown, snapshot, root_sha, snapshot_sha, payload_bytes) =
                tokio::time::timeout(Duration::from_secs(10), async {
                    let mut store = SessionStore::new(Arc::new(
                        object_store::local::LocalFileSystem::new_with_prefix(data_dir.path())
                            .unwrap(),
                    ));
                    store.head = Some(Arc::new(InMemory::new()));
                    let f = Fixture::with_store(store).await;
                    let base = read_two_bit_head(f.store.as_ref(), &f.prefix)
                        .await
                        .unwrap()
                        .unwrap();
                    let snapshot = apply_two_bit_mutations(
                        f.store.as_ref(),
                        &base,
                        2,
                        None,
                        &[TwoBitMutation {
                            id: 7,
                            vector: Some(vec![1., 0.]),
                        }],
                        caps().mutations,
                    )
                    .await
                    .unwrap();
                    let snapshot_sha = snapshot.sha256().to_owned();
                    let snapshot = base
                        .metadata_prefix()
                        .join("mutations")
                        .join(snapshot.sha256());
                    let unknown = f.object("unknown", b"k").await;
                    for id in 0..ORPHANS {
                        f.object(
                            &format!("maintenance/{}/objects/{id:064x}", "a".repeat(48)),
                            b"x",
                        )
                        .await;
                    }
                    let payload_bytes = ORPHANS as u64
                        + 1
                        + f.store.head(&f.root).await.unwrap().size
                        + f.store.head(&snapshot).await.unwrap().size
                        + f.store
                            .head(&f.prefix.clone().join("head.json"))
                            .await
                            .unwrap()
                            .size;
                    (
                        f,
                        unknown,
                        snapshot,
                        base.root_sha256().to_owned(),
                        snapshot_sha,
                        payload_bytes,
                    )
                })
                .await
                .expect("bounded 1033-object fixture setup");
            assert!(ORPHANS + 4 <= 1100 && payload_bytes < 64 * 1024);
            let mut session = f.session();
            let sweep = tokio::time::timeout(Duration::from_secs(30), async {
                let (mut deleted, mut bytes, mut reads) = (0, 0, 0);
                let (mut scan_cap, mut delete_cap, mut byte_cap) = (false, false, false);
                let mut within_caps = true;
                for pass in 0..=ORPHANS + 4 {
                    let limits = TwoBitGcLimits {
                        max_objects_scanned: if pass % 3 == 0 { 1 } else { 7 },
                        max_objects_deleted: if pass % 3 == 1 { 1 } else { 3 },
                        max_deleted_object_bytes: 2,
                        ..caps()
                    };
                    let report = tokio::time::timeout(
                        DEADLINE,
                        collect_two_bit_garbage(&mut session, limits),
                    )
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| e.to_string())?;
                    within_caps &= report.objects_scanned <= limits.max_objects_scanned
                        && report.objects_deleted <= limits.max_objects_deleted
                        && report.deleted_object_bytes <= limits.max_deleted_object_bytes
                        && report.delete_attempts == report.objects_deleted
                        && !report.restart_required
                        && !report.delete_outcome_unknown;
                    scan_cap |= !report.scan_complete
                        && report.objects_scanned == limits.max_objects_scanned;
                    delete_cap |= report.objects_deleted == limits.max_objects_deleted;
                    byte_cap |=
                        report.pending_object_bytes == Some(1) && report.deleted_object_bytes == 2;
                    deleted += report.objects_deleted;
                    bytes += report.deleted_object_bytes;
                    // Both caller reads and the next pass's keep-set GETs use real filesystem data.
                    let fresh = tokio::time::timeout(DEADLINE, async {
                        let head = read_two_bit_head(f.store.as_ref(), &f.prefix)
                            .await?
                            .unwrap();
                        let current =
                            read_two_bit_mutations(f.store.as_ref(), &head, 2, caps().mutations)
                                .await?
                                .unwrap();
                        Ok::<_, TwoBitStoreError>(
                            head.root_sha256() == root_sha && current.sha256() == snapshot_sha,
                        )
                    })
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| e.to_string())?;
                    within_caps &= fresh;
                    if !report.scan_complete {
                        reads += 1;
                        within_caps &= f.store.observed.list_drops.load(Ordering::SeqCst) == 0;
                    } else {
                        within_caps &= report.pending_object_bytes.is_none();
                        return Ok::<_, String>((
                            deleted,
                            bytes,
                            reads,
                            scan_cap,
                            delete_cap,
                            byte_cap,
                            within_caps,
                        ));
                    }
                }
                Err("bounded sweep did not reach EOF".to_owned())
            })
            .await;
            let closed = f.close(session).await;
            let contents = tokio::time::timeout(DEADLINE, async {
                let mut absent = 0;
                for id in 0..ORPHANS {
                    let path = f
                        .prefix
                        .clone()
                        .join(format!("maintenance/{}/objects/{id:064x}", "a".repeat(48)));
                    absent += usize::from(matches!(
                        f.store.head(&path).await,
                        Err(object_store::Error::NotFound { .. })
                    ));
                }
                (
                    absent,
                    f.store.head(&f.root).await.is_ok(),
                    f.store.head(&snapshot).await.is_ok(),
                    f.store.head(&unknown).await.is_ok(),
                )
            })
            .await;
            assert!(closed.is_ok());
            let (deleted, bytes, reads, scan_cap, delete_cap, byte_cap, within_caps) =
                sweep.unwrap().unwrap();
            assert_eq!((deleted, bytes), (ORPHANS, ORPHANS as u64));
            assert!(reads > 1 && scan_cap && delete_cap && byte_cap && within_caps);
            assert_eq!(contents.unwrap(), (ORPHANS, true, true, true));
            assert_eq!(f.store.observed.lists.load(Ordering::SeqCst), 1);
            assert_eq!(f.store.observed.polls.load(Ordering::SeqCst), ORPHANS + 3);
            assert_eq!(f.store.observed.deletes.load(Ordering::SeqCst), ORPHANS);
            assert_eq!(f.store.observed.list_drops.load(Ordering::SeqCst), 1);
            assert!(f.store.observed.drop_in_runtime.load(Ordering::SeqCst));
            assert!(f.store.observed.drop_on_list_thread.load(Ordering::SeqCst));
        });
    }

    #[tokio::test]
    async fn admission_reserves_continuation_and_rejects_zero_or_overflowing_caps_before_io() {
        let (store, operations) =
            common::FaultInjectingObjectStore::new(Arc::new(InMemory::new())).with_operation_log();
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(store);
        let mut session =
            TwoBitGcSession::new(store.clone(), &Path::from("admission"), directory.path())
                .unwrap();
        let limits = caps();
        let mut failures = Vec::new();
        for limits in [
            TwoBitGcLimits {
                max_memory_bytes: 1_524_288,
                ..limits
            },
            TwoBitGcLimits {
                max_memory_bytes: 1_525_312,
                ..limits
            },
            TwoBitGcLimits {
                max_objects_scanned: 0,
                ..limits
            },
            TwoBitGcLimits {
                max_objects_deleted: 0,
                ..limits
            },
            TwoBitGcLimits {
                max_deleted_object_bytes: 0,
                ..limits
            },
            TwoBitGcLimits {
                max_memory_bytes: usize::MAX,
                mutations: TwoBitMutationLimits {
                    max_memory_bytes: usize::MAX,
                    ..limits.mutations
                },
                ..limits
            },
        ] {
            failures.push(run(&mut session, limits).await);
        }
        drop(session);
        let drained = tokio::time::timeout(DEADLINE, async {
            while Arc::strong_count(&store) != 1 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        assert!(drained.is_ok());
        assert!(failures.iter().all(|result| result.is_err()));
        assert!(operations.entries().is_empty());
        assert!(!directory.path().join("lifecycle.lock").exists());
    }
}
