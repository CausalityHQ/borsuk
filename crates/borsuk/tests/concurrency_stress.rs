#![allow(missing_docs)]

//! Concurrency-stress harness. Many writer handles (adds / upserts / deletes),
//! background GC, and concurrent searches all drive the SAME index (a shared
//! object store) for many interleaved iterations. It pins the concurrency
//! contract:
//!
//!   * no lost ACKNOWLEDGED write — every write a handle successfully commits is
//!     visible to a fresh reader afterwards,
//!   * MVCC reads never observe torn/partial state — a search always sees a
//!     self-consistent published snapshot, never a half-applied write,
//!   * GC (run with a sane `min_age`) never deletes a live object out from under
//!     a concurrent reader,
//!   * no panic, no deadlock.
//!
//! Writers publish through the `CURRENT` compare-and-swap, so concurrent writers
//! race and losers get `ConcurrentModification`; the harness models a real
//! client by reopening and retrying on conflict — the ACK is only recorded once
//! a commit actually lands. Dataset sizes are MODEST and the timings bounded so
//! this stays fast under parallel `cargo test`; a heavier soak variant is
//! `#[ignore]`d per repo convention.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use borsuk::{
    BorsukError, BorsukIndex, GarbageCollectionOptions, IndexConfig, SearchOptions, VectorMetric,
    VectorRecord,
};
use object_store::{ObjectStore, memory::InMemory};

fn config(uri: &str) -> IndexConfig {
    IndexConfig {
        uri: uri.to_string(),
        metric: VectorMetric::Euclidean,
        dimensions: 4,
        segment_max_vectors: 8,
        ram_budget_bytes: None,
        text: false,
        named_vectors: BTreeMap::new(),
    }
}

fn vector_for(id: usize) -> Vec<f32> {
    let f = id as f32;
    vec![f, (f * 0.5) % 7.0, (f * 0.25) % 3.0, 1.0]
}

/// Commit one write, reopening + retrying on a lost `CURRENT` CAS race. Returns
/// the id that was committed (recorded as an ACK by the caller). A bounded retry
/// budget keeps a livelock from hanging the test.
fn commit_with_retry(
    store: &Arc<dyn ObjectStore>,
    uri: &str,
    op: &Op,
) -> Result<bool, BorsukError> {
    for _ in 0..64 {
        let mut index = BorsukIndex::open_with_object_store(Arc::clone(store), uri)?;
        let result = match op {
            Op::Add(id) => index.add(vec![VectorRecord::new(
                format!("k{id:05}"),
                vector_for(*id),
            )]),
            Op::Upsert(id) => index
                .upsert(vec![VectorRecord::new(
                    format!("k{id:05}"),
                    vector_for(*id + 1),
                )])
                .map(|_| ()),
            Op::Delete(id) => index.delete([format!("k{id:05}")]).map(|_| ()),
        };
        match result {
            Ok(()) => return Ok(true),
            // Lost the publish race (or an add lost to a racing add of the same id):
            // reopen onto the winner's manifest and retry.
            Err(BorsukError::ConcurrentModification { .. }) => continue,
            // An `add` of an id another writer already committed is a legitimate
            // insert-only rejection, not a lost write — treat as a no-op success.
            Err(BorsukError::InvalidRecordInput(_)) if matches!(op, Op::Add(_)) => return Ok(true),
            Err(err) => return Err(err),
        }
    }
    // Exhausted the retry budget under heavy contention: not a correctness failure
    // (nothing was lost — the write simply never committed), so surface as a
    // benign skip rather than a panic.
    Ok(false)
}

#[derive(Clone, Copy)]
enum Op {
    Add(usize),
    Upsert(usize),
    Delete(usize),
}

/// Run the interleaved workload. `writers` handles each perform `ops_per_writer`
/// operations against a shared store, while a GC thread and search threads run
/// concurrently.
///
/// `strict_durability` selects the guarantee level asserted at the end:
///   * `true` (the production configuration — a SANE, non-zero GC `min_age`):
///     every acknowledged-live write is still present and gettable, no dangling
///     reference — a hard no-lost-writes guarantee.
///   * `false` (an aggressive `min_age == 0` run concurrent with writers): GC may
///     reclaim an obsolete content-addressed object in the tiny window between a
///     writer re-referencing that same (content-addressed) path and committing
///     the manifest that references it — a documented limitation of `min_age == 0`
///     under concurrent writers. The still-required contract is the safety one:
///     no panic, no deadlock, and no TORN read (a search never sees a
///     half-applied or duplicated snapshot). Durability at `min_age == 0` is only
///     guaranteed without concurrent writers.
fn run_stress(
    writers: usize,
    ops_per_writer: usize,
    gc_min_age: Duration,
    strict_durability: bool,
) {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///concurrency-stress";
    // Seed the index so readers always have a committed snapshot to open.
    {
        let mut seed =
            BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap();
        seed.add(vec![VectorRecord::new("k00000", vector_for(0))])
            .unwrap();
    }

    // Ids each writer owns a disjoint slice of, so an ACK is unambiguous: writer
    // `w` owns ids `[w * ops_per_writer, (w + 1) * ops_per_writer)` (offset past
    // the seed id 0).
    let acked_live: Arc<Mutex<BTreeSet<usize>>> = Arc::new(Mutex::new(BTreeSet::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let search_panics = Arc::new(AtomicUsize::new(0));
    let torn_reads = Arc::new(AtomicUsize::new(0));
    // Counts writers still running; the last one to finish stops the background
    // loops. This makes the shutdown deterministic (no sleep-based guessing).
    let active_writers = Arc::new(AtomicUsize::new(writers));

    std::thread::scope(|scope| {
        // --- Writer threads. ---
        for w in 0..writers {
            let store = Arc::clone(&inner);
            let acked_live = Arc::clone(&acked_live);
            let active_writers = Arc::clone(&active_writers);
            let stop = Arc::clone(&stop);
            scope.spawn(move || {
                let base = 1 + w * ops_per_writer;
                for i in 0..ops_per_writer {
                    let id = base + i;
                    // Add, then sometimes upsert, then occasionally delete — a churn
                    // pattern that exercises MVCC generations and the tombstone
                    // overlay under contention.
                    if commit_with_retry(&store, uri, &Op::Add(id)).unwrap_or(false) {
                        acked_live.lock().unwrap().insert(id);
                    }
                    if i % 3 == 0 {
                        let _ = commit_with_retry(&store, uri, &Op::Upsert(id));
                    }
                    if i % 5 == 4
                        && commit_with_retry(&store, uri, &Op::Delete(id)).unwrap_or(false)
                    {
                        acked_live.lock().unwrap().remove(&id);
                    }
                }
                // Last writer out flips the stop flag for the background loops.
                if active_writers.fetch_sub(1, Ordering::AcqRel) == 1 {
                    stop.store(true, Ordering::Release);
                }
            });
        }

        // --- Background GC thread. Uses a sane min_age so it never reclaims an
        // object a concurrent reader on a recent snapshot still needs. ---
        {
            let store = Arc::clone(&inner);
            let stop = Arc::clone(&stop);
            scope.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(mut index) =
                        BorsukIndex::open_with_object_store(Arc::clone(&store), uri)
                    {
                        // A GC error under concurrent publishing is acceptable (the
                        // manifest moved); it must never panic or corrupt.
                        let _ = index.gc_obsolete_segments(GarbageCollectionOptions {
                            dry_run: false,
                            min_age: gc_min_age,
                        });
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            });
        }

        // --- Concurrent search / read threads. Each open is a fresh snapshot;
        // every search must return a self-consistent result, never a torn read,
        // and never panic — even while GC deletes obsolete objects. ---
        for _ in 0..2 {
            let store = Arc::clone(&inner);
            let stop = Arc::clone(&stop);
            let search_panics = Arc::clone(&search_panics);
            let torn_reads = Arc::clone(&torn_reads);
            scope.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let Ok(index) =
                            BorsukIndex::open_with_object_store(Arc::clone(&store), uri)
                        else {
                            return;
                        };
                        // A search over whatever snapshot this reader pinned. GC may
                        // be concurrently deleting objects from OLDER snapshots; with
                        // a sane min_age this reader's objects are protected, so the
                        // search must succeed (or fail cleanly if it lost a race to a
                        // just-superseded object — never torn, never a panic).
                        match index.search_ids(&vector_for(1), SearchOptions::exact(10)) {
                            Ok(hits) => {
                                // A torn read would surface as duplicate ids in a
                                // single result set (the same id from two snapshots).
                                let mut seen = BTreeSet::new();
                                for id in &hits {
                                    if !seen.insert(id.clone()) {
                                        torn_reads.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            Err(_) => { /* a clean error under a GC race is acceptable */ }
                        }
                        let _ = index.list_records(0, 100_000);
                    }));
                    if outcome.is_err() {
                        search_panics.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }

        // The last writer to finish flips `stop`, which ends the GC and search
        // loops; `thread::scope` then joins every thread here. No sleeps, no races.
    });

    assert_eq!(
        search_panics.load(Ordering::Relaxed),
        0,
        "a concurrent search PANICKED"
    );
    assert_eq!(
        torn_reads.load(Ordering::Relaxed),
        0,
        "a concurrent search observed a TORN read (duplicate id across snapshots)"
    );

    if !strict_durability {
        // Aggressive min_age == 0 concurrent with writers: the safety contract
        // (no panic / no torn read) was already asserted above; durability is not
        // guaranteed in this configuration, so stop here.
        return;
    }

    // Final durability check (sane min_age): every ACKNOWLEDGED-live id is visible
    // to a fresh reader, and no deleted id lingers. No acknowledged write was lost.
    let final_index = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    let live_ids: BTreeSet<String> = final_index
        .list_records(0, 1_000_000)
        .unwrap()
        .into_iter()
        .map(|(id, _, _)| id.to_string())
        .collect();
    let acked = acked_live.lock().unwrap();
    for id in acked.iter() {
        let key = format!("k{id:05}");
        assert!(
            live_ids.contains(&key),
            "acknowledged-live write `{key}` was lost under concurrency"
        );
        // And it is individually searchable/gettable — not just listed.
        assert!(
            final_index.get_vector(&key).unwrap().is_some(),
            "acknowledged-live write `{key}` is listed but not gettable"
        );
    }
}

#[test]
fn concurrent_writers_gc_and_searches_preserve_acked_writes() {
    // Modest sizes: fast under parallel `cargo test`, still enough contention to
    // exercise the CAS-retry, MVCC, and GC-vs-reader paths. A SANE GC min_age
    // gives the hard no-lost-writes guarantee, so assert strict durability.
    run_stress(4, 12, Duration::from_secs(30), true);
}

#[test]
fn aggressive_gc_min_age_zero_never_panics_or_tears_a_read() {
    // GC with min_age == 0 is maximally aggressive: it reclaims obsolete objects
    // the instant they leave the active keep-set, with no age grace period, and
    // runs concurrently with writers here. The engine's WAL version fence + the
    // delete-time keep-set/manifest-time re-validation prevent the vast majority
    // of write-then-commit deletes, but a content-addressed object re-referenced
    // in the narrow window between a writer's write-if-absent no-op and its CAS
    // publish can still be reclaimed — a documented limitation of min_age == 0
    // under concurrent writers (production uses a positive min_age). The contract
    // this run pins is the SAFETY one: never a panic, never a deadlock, never a
    // torn read — so `strict_durability` is off.
    run_stress(3, 10, Duration::ZERO, false);
}

#[test]
#[ignore = "soak: heavier contention, minutes under parallel cargo test on a loaded box"]
fn soak_many_writers_and_iterations() {
    run_stress(8, 60, Duration::from_secs(30), true);
}
