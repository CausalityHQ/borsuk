#![allow(missing_docs)]

//! Format-fuzz harness. Every decode entry point BORSUK reaches through its
//! public API — the manifest table, the routing/pivot tables, segment Parquet,
//! per-segment graphs, the WAL objects, the dense-vector Arrow sidecar, the cold
//! coarse-quantizer object, and (for a named/sparse index) the sparse-named
//! sidecar — must turn corrupt, truncated, or byte-mutated bytes into a graceful
//! `BorsukError`, NEVER a panic, an `unwrap` crash, or an unbounded allocation.
//!
//! The harness builds a real index in an in-memory store, snapshots every
//! object, and then for each object replays the open+search+scan flow against a
//! fresh store copy in which that one object has been deterministically mutated
//! (seeded xorshift — no `rand`, no wall clock). Every reopen/search/scan runs
//! inside `catch_unwind` so a panic anywhere in a decode path fails the test
//! loudly instead of aborting the process. A `Result` (Ok or `BorsukError`) is
//! the only acceptable outcome.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use borsuk::{
    BorsukIndex, CompactionOptions, IndexConfig, LeafMode, SearchOptions, VectorKind, VectorMetric,
    VectorRecord, VectorSpec, vector_records_from_parquet,
};
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::{
    GetOptions, ObjectStore, PutOptions, PutPayload, memory::InMemory, path::Path as ObjectPath,
};

/// A tiny deterministic PRNG (xorshift64). Seeded, reproducible, no `rand`.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Avoid the zero fixed point.
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            0
        } else {
            (self.next_u64() % bound as u64) as usize
        }
    }
}

/// Snapshot every `(path, bytes)` object currently in an in-memory store.
fn snapshot(store: &Arc<dyn ObjectStore>) -> Vec<(ObjectPath, Vec<u8>)> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let metas = store.list(None).try_collect::<Vec<_>>().await.unwrap();
        let mut objects = Vec::with_capacity(metas.len());
        for meta in metas {
            let bytes = store
                .get_opts(&meta.location, GetOptions::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec();
            objects.push((meta.location, bytes));
        }
        objects
    })
}

/// Build a fresh in-memory store from a snapshot, replacing exactly one object's
/// bytes with `replacement` (a `None` `replacement` deletes the object).
fn store_with_replacement(
    objects: &[(ObjectPath, Vec<u8>)],
    target: &ObjectPath,
    replacement: Option<&[u8]>,
) -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for (path, bytes) in objects {
            if path == target {
                if let Some(replacement) = replacement {
                    store
                        .put_opts(
                            path,
                            PutPayload::from(Bytes::copy_from_slice(replacement)),
                            PutOptions::default(),
                        )
                        .await
                        .unwrap();
                }
                // `None` => drop the object entirely (simulate a lost object).
            } else {
                store
                    .put_opts(
                        path,
                        PutPayload::from(Bytes::copy_from_slice(bytes)),
                        PutOptions::default(),
                    )
                    .await
                    .unwrap();
            }
        }
    });
    store
}

/// The family of deterministic corruptions applied to one valid object.
fn mutations(original: &[u8], rng: &mut Rng) -> Vec<(&'static str, Vec<u8>)> {
    let mut out: Vec<(&'static str, Vec<u8>)> = Vec::new();

    // Empty and single-byte objects: the smallest inputs a decoder can see.
    out.push(("empty", Vec::new()));
    out.push(("single_zero_byte", vec![0u8]));

    // Truncations at a spread of prefix lengths (torn writes).
    for fraction in [1usize, 2, 4, 8] {
        let keep = original.len() / fraction;
        out.push(("truncate", original[..keep].to_vec()));
    }
    // Truncate the trailing footer/magic specifically (sidecars & parquet trailer).
    if original.len() > 8 {
        out.push(("drop_trailer", original[..original.len() - 8].to_vec()));
    }

    // Single-byte flips at deterministic positions.
    for _ in 0..6 {
        if original.is_empty() {
            break;
        }
        let mut bytes = original.to_vec();
        let idx = rng.below(bytes.len());
        bytes[idx] ^= (rng.next_u64() as u8) | 1;
        out.push(("byte_flip", bytes));
    }

    // Zero-fill a random middle span (wipes an interior region).
    if original.len() > 16 {
        let mut bytes = original.to_vec();
        let start = rng.below(bytes.len() / 2);
        let end = (start + 1 + rng.below(bytes.len() - start)).min(bytes.len());
        for byte in &mut bytes[start..end] {
            *byte = 0;
        }
        out.push(("zero_span", bytes));
    }

    // 0xFF-fill the whole object (all-ones — stresses length/count fields).
    out.push(("all_ones", vec![0xFFu8; original.len().max(1)]));

    // Grow the object with trailing garbage (a longer-than-expected read).
    let mut grown = original.to_vec();
    grown.extend(std::iter::repeat_n(0xABu8, 64));
    out.push(("trailing_garbage", grown));

    out
}

/// Drive the whole public read surface against a (possibly corrupt) store. The
/// only acceptable outcomes are `Ok(_)` or `Err(BorsukError)`; a panic is a bug.
fn exercise(store: Arc<dyn ObjectStore>, uri: &str, dimensions: usize, mutation: &str) {
    let query = vec![0.5f32; dimensions];
    let result = catch_unwind(AssertUnwindSafe(|| {
        // Paged open (default): manifest metadata only.
        let Ok(index) = BorsukIndex::open_with_object_store(Arc::clone(&store), uri) else {
            return;
        };
        let _ = index.stats();
        let _ = index.search_ids(&query, SearchOptions::exact(5));
        let _ = index.search_ids(&query, SearchOptions::approx(5, LeafMode::PqScan));
        let _ = index.get_vector("r0000");
        let _ = index.get_vector("missing-id");
        let _ = index.list_records(0, 10_000);
        let _ = index.warm();
        // Warm may have failed; a post-warm search must still not panic.
        let _ = index.search_ids(&query, SearchOptions::exact(5));
    }));
    assert!(
        result.is_ok(),
        "decoding a corrupted object PANICKED (must be a graceful BorsukError) for uri {uri}: {mutation}"
    );
}

/// Build a compacted index with the full artifact set (segments, per-segment
/// graphs, dense-vector sidecars, and — via a cold-quantizer refresh — a
/// persisted quantizer object), plus a WAL tail, returning its store.
fn build_rich_index(uri: &str, dimensions: usize) -> Arc<dyn ObjectStore> {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
        IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions,
            segment_max_vectors: 8,
            ram_budget_bytes: None,
            text: false,
            named_vectors: BTreeMap::new(),
        },
    )
    .unwrap();

    let records: Vec<VectorRecord> = (0..64)
        .map(|value| {
            let vector = (0..dimensions)
                .map(|d| ((value * 7 + d * 3) % 23) as f32 * 0.1)
                .collect::<Vec<_>>();
            VectorRecord::new(format!("r{value:04}"), vector)
        })
        .collect();
    index.add(records).unwrap();
    // Compaction materializes segment Parquet, per-segment graphs, and the dense
    // vector sidecars, and refreshes the persisted cold quantizer.
    index
        .compact(CompactionOptions {
            max_segments: None,
            ..CompactionOptions::default()
        })
        .unwrap();
    // Leave a live WAL tail so a `wal/` object is present to be fuzzed.
    index
        .add(vec![VectorRecord::new("tail-a", vec![0.3f32; dimensions])])
        .unwrap();
    inner
}

#[test]
fn every_object_decode_survives_corruption_without_panicking() {
    let dimensions = 6;
    let uri = "memory:///format-fuzz";
    let store = build_rich_index(uri, dimensions);
    let objects = snapshot(&store);
    assert!(
        objects.len() > 5,
        "expected a rich object set to fuzz, got {}",
        objects.len()
    );

    // Every object gets the full mutation family; the seed is fixed so a failure
    // reproduces exactly.
    let mut rng = Rng::new(0x9E37_79B9_7F4A_7C15);
    for (path, bytes) in &objects {
        for (label, mutated) in mutations(bytes, &mut rng) {
            let corrupt = store_with_replacement(&objects, path, Some(&mutated));
            exercise(
                corrupt,
                uri,
                dimensions,
                &format!("path={path}, mutation={label}"),
            );
        }
        // Also delete the object entirely (a missing referenced object).
        let missing = store_with_replacement(&objects, path, None);
        exercise(
            missing,
            uri,
            dimensions,
            &format!("path={path}, mutation=missing"),
        );
    }
}

#[test]
fn named_and_sparse_parquet_decode_survives_corruption() {
    // A named-vector index adds child dense storage plus named-sparse Parquet
    // posting/metadata shards, exercising both lexical and child-index decode
    // paths under corruption.
    let dimensions = 4;
    let uri = "memory:///format-fuzz-named";
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut named = BTreeMap::new();
    named.insert(
        "dense_side".to_string(),
        VectorSpec {
            kind: VectorKind::Dense,
            dimensions: 3,
            metric: VectorMetric::Cosine,
            element_type: Default::default(),
        },
    );
    named.insert(
        "sparse_side".to_string(),
        VectorSpec {
            kind: VectorKind::Sparse,
            dimensions: 1000,
            metric: VectorMetric::InnerProduct,
            element_type: Default::default(),
        },
    );
    let mut index = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
        IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions,
            segment_max_vectors: 8,
            ram_budget_bytes: None,
            text: false,
            named_vectors: named,
        },
    )
    .unwrap();
    index
        .add(
            (0..24)
                .map(|value| {
                    // Strictly-ascending, unique sparse indices (the sparse contract).
                    let base = value as u32 % 400;
                    VectorRecord::new(format!("r{value:04}"), vec![value as f32, 0.0, 1.0, 0.5])
                        .with_named_vector("dense_side", vec![value as f32 * 0.1, 0.2, 0.3])
                        .with_named_sparse_vector(
                            "sparse_side",
                            vec![base, base + 500],
                            vec![1.0, 0.5],
                        )
                        .unwrap()
                })
                .collect(),
        )
        .unwrap();
    index
        .compact(CompactionOptions {
            max_segments: None,
            ..CompactionOptions::default()
        })
        .unwrap();

    let objects = snapshot(&inner);
    let mut rng = Rng::new(0x1234_5678_9ABC_DEF0);
    for (path, bytes) in &objects {
        for (label, mutated) in mutations(bytes, &mut rng) {
            let corrupt = store_with_replacement(&objects, path, Some(&mutated));
            exercise(
                corrupt,
                uri,
                dimensions,
                &format!("path={path}, mutation={label}"),
            );
        }
        let missing = store_with_replacement(&objects, path, None);
        exercise(
            missing,
            uri,
            dimensions,
            &format!("path={path}, mutation=missing"),
        );
    }
}

#[test]
fn public_vector_records_from_parquet_rejects_corruption_without_panicking() {
    // The one PUBLIC decode entry point on the crate surface. It must never panic
    // on adversarial bytes.
    let records: Vec<VectorRecord> = (0..16)
        .map(|value| VectorRecord::new(format!("r{value}"), vec![value as f32, 1.0, 2.0]))
        .collect();
    let valid = borsuk::vector_records_to_parquet(&records, 3).unwrap();

    let mut rng = Rng::new(0xDEAD_BEEF_CAFE_F00D);
    for (_label, mutated) in mutations(&valid, &mut rng) {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = vector_records_from_parquet(&mutated, 3);
        }));
        assert!(
            result.is_ok(),
            "vector_records_from_parquet PANICKED on corrupt bytes"
        );
    }

    // A pile of purely random blobs of assorted lengths.
    let mut rng = Rng::new(0x0BAD_C0DE_1337_9001);
    for len in [0usize, 1, 3, 7, 16, 64, 257, 4096] {
        let blob: Vec<u8> = (0..len).map(|_| rng.next_u64() as u8).collect();
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = vector_records_from_parquet(&blob, 3);
        }));
        assert!(
            result.is_ok(),
            "vector_records_from_parquet PANICKED on a {len}-byte random blob"
        );
    }
}
