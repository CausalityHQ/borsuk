#![allow(missing_docs)]

use std::{env, ops::Range};

use borsuk::{
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, LeafCapability,
    LeafMode, PhysicalObjectRole, SearchMode, SearchOptions, VectorMetric, VectorRecord,
    physical_object_role_for_path,
};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, parse_url_opts, path::Path as ObjectPath};
use tokio::runtime::Builder;
use url::Url;
use uuid::Uuid;

const LARGE_OBJECT_BYTES: usize = 64 * 1024 * 1024 + 1;

#[test]
fn s3_compatible_index_round_trip_when_configured() {
    let Ok(base_uri) = env::var("BORSUK_S3_TEST_URI") else {
        return;
    };
    let uri = format!("{}/{}", base_uri.trim_end_matches('/'), Uuid::new_v4());

    let mut index = BorsukIndex::create_with_leaf_capability(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 3,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
        LeafCapability::GraphEnabled,
    )
    .unwrap();

    // Two segments of three records each: the candidate budget of 2 below stays
    // under the segment length, so graph-backed search genuinely traverses the
    // graph (a budget covering the whole segment would flat-scan and skip it).
    index
        .add(vec![
            VectorRecord::new("near", vec![0.0, 0.0]),
            VectorRecord::new("neighbor", vec![0.0, 0.1]),
            VectorRecord::new("midA", vec![3.0, 0.0]),
            VectorRecord::new("mid", vec![5.0, 0.0]),
            VectorRecord::new("far", vec![10.0, 0.0]),
            VectorRecord::new("farther", vec![12.0, 0.0]),
        ])
        .unwrap();
    index.flush().unwrap();

    assert_s3_compatible_binary_layout(&uri);

    let cache = tempfile::tempdir().unwrap();
    let mut reopened =
        BorsukIndex::open_with_cache(&uri, Some(cache.path().to_path_buf())).unwrap();
    let ids = reopened
        .search_ids(&[0.1, 0.0], SearchOptions::exact(1))
        .unwrap();

    assert_eq!(ids[0], "near");

    let report = reopened
        .search_with_report(
            &[0.04, 0.07],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                    adaptive_stop: None,
                    projected_reads: None,
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
                vector_name: String::new(),
                disable_coarse_quantizer: false,
                cache_execution: borsuk::CacheExecutionPolicy::Scan,
                global_exact_rerank: true,
                global_exact_bound_shadow: false,
            },
        )
        .unwrap();
    assert_eq!(report.hits[0].id, "neighbor");
    assert!(report.graph_bytes_read > 0);
    assert!(cache.path().join("segments").exists());
    assert!(cache.path().join("graphs").exists());

    let compaction = reopened
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(2),
            min_segments: 2,
            target_segment_max_vectors: Some(6),
            target_segment_max_radius: None,
        })
        .unwrap();
    assert!(compaction.compacted);
    assert_eq!(compaction.segments_written, 1);

    let gc = reopened
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: true,
            min_age: std::time::Duration::ZERO,
        })
        .unwrap();
    assert_eq!(gc.objects_deleted, 0);
    assert!(!gc.candidates.is_empty());
}

#[test]
fn s3_compatible_large_object_round_trip_when_configured() {
    let Ok(base_uri) = env::var("BORSUK_S3_TEST_URI") else {
        return;
    };
    let uri = format!("{}/{}", base_uri.trim_end_matches('/'), Uuid::new_v4());
    let large_id = deterministic_bytes(LARGE_OBJECT_BYTES);

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 1,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    })
    .unwrap();
    index
        .add(vec![VectorRecord::new_bytes(large_id.clone(), vec![42.0])])
        .unwrap();

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened.get_vector_by_id(&large_id).unwrap(),
        Some(vec![42.0])
    );
}

fn assert_s3_compatible_binary_layout(uri: &str) {
    let url = Url::parse(uri).unwrap();
    let (store, prefix) = parse_url_opts(&url, env::vars()).unwrap();
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let objects = runtime
        .block_on(async { store.list(Some(&prefix)).try_collect::<Vec<_>>().await })
        .unwrap()
        .into_iter()
        .map(|meta| (relative_path(&prefix, &meta.location), meta.size))
        .collect::<Vec<_>>();

    for (path, size) in &objects {
        let role = physical_object_role_for_path(path);
        assert_ne!(
            role,
            PhysicalObjectRole::Unknown,
            "S3-compatible storage contains an unknown durable object: {path}"
        );
        if role == PhysicalObjectRole::FilterIndex {
            assert_filter_index_envelope(store.as_ref(), &prefix, path, *size, &runtime);
            continue;
        }
        if role == PhysicalObjectRole::ExactVectors {
            let magic = read_object_range(store.as_ref(), &prefix, path, 0..6, &runtime);
            assert_eq!(magic, b"ARROW1", "{path} must be a standard Arrow IPC file");
            continue;
        }
        let magic = read_object_range(store.as_ref(), &prefix, path, 0..4, &runtime);
        let expected_magic = expected_magic(path, role);
        assert_eq!(
            magic,
            expected_magic,
            "{path} must use the checked physical format for role {}",
            role.as_str()
        );
    }
    assert!(
        objects
            .iter()
            .any(|(path, _)| path.starts_with("manifests/") && path.ends_with(".parquet")),
        "manifest tables must be Parquet objects: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .any(|(path, _)| path.starts_with("routing/segments-") && path.ends_with(".parquet")),
        "segment-summary routing tables must be Parquet objects: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .any(|(path, _)| path.starts_with("routing/pivots-") && path.ends_with(".parquet")),
        "pivot routing tables must be Parquet objects: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .any(|(path, _)| path.starts_with("segments/") && path.ends_with(".parquet")),
        "segment payloads must be Parquet objects: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .any(|(path, _)| path.starts_with("graphs/") && path.ends_with(".parquet")),
        "segment-local graphs must be Parquet objects: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .any(|(path, _)| path.contains("/wal/") && path.contains("/runs/records/")),
        "default writes must leave an immutable WAL record run: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .any(|(path, _)| path.contains("/wal/") && path.contains("/frontier/")),
        "default writes must leave an immutable WAL frontier: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .any(|(path, _)| { path.contains("/wal/") && path.contains("/runs/id-directory/") }),
        "default writes must leave a checked ID-directory delta run: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .any(|(path, _)| path.starts_with("transactions/") && path.ends_with("/COMMIT")),
        "cross-cell WAL visibility must use a commit marker: {objects:?}"
    );
    assert!(
        objects.iter().any(|(path, _)| {
            path.starts_with("transactions/") && path.contains("/descriptors/")
        }),
        "cross-cell WAL visibility must pin an immutable descriptor: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .any(|(path, _)| path.starts_with("id-directory/claim-shards/")),
        "insert-only coordination must use sharded claim locks: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .all(|(path, _)| { !path.ends_with(".json") && !path.ends_with(".borsuk") }),
        "JSON or ad-hoc manifest files must not be durable S3-compatible storage: {objects:?}"
    );

    let current_size = objects
        .iter()
        .find_map(|(path, size)| (path == "collection/CURRENT").then_some(*size))
        .expect("collection/CURRENT must exist");
    let current = read_object_range(
        store.as_ref(),
        &prefix,
        "collection/CURRENT",
        0..current_size,
        &runtime,
    );
    assert_eq!(current.len() as u64, current_size);
    assert_eq!(&current[0..4], b"BCCP");
    assert!(
        !String::from_utf8_lossy(&current).contains("manifest-"),
        "collection/CURRENT must be a checked binary pointer, not a text manifest path"
    );
}

fn assert_filter_index_envelope(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    path: &str,
    size: u64,
    runtime: &tokio::runtime::Runtime,
) {
    const SEGMENT_CHECKSUM_BYTES: usize = 64;
    const CONTENT_CHECKSUM_BYTES: usize = 32;
    const HEADER_BYTES: usize = SEGMENT_CHECKSUM_BYTES + CONTENT_CHECKSUM_BYTES;

    let bytes = read_object_range(store, prefix, path, 0..size, runtime);
    assert!(
        bytes.len() >= HEADER_BYTES,
        "{path} filter-index envelope is truncated"
    );
    let filename_checksum = path
        .rsplit('/')
        .next()
        .and_then(|name| name.strip_suffix(".fidx"))
        .expect("filter-index path must end in a checksum.fidx filename");
    assert_eq!(
        &bytes[..SEGMENT_CHECKSUM_BYTES],
        filename_checksum.as_bytes(),
        "{path} must pin the segment checksum named by its path"
    );
    assert_eq!(
        &bytes[SEGMENT_CHECKSUM_BYTES..HEADER_BYTES],
        blake3::hash(&bytes[HEADER_BYTES..]).as_bytes(),
        "{path} must checksum its metadata-index payload"
    );
}

fn expected_magic(path: &str, role: PhysicalObjectRole) -> &'static [u8] {
    if path == "collection/CURRENT" {
        b"BCCP"
    } else if path.starts_with("collection/snapshots/") {
        b"BCSN"
    } else if path.starts_with("collection/wal-frontier/") && path.ends_with("/HEAD") {
        b"BCWH"
    } else if path.ends_with(".parquet") {
        b"PAR1"
    } else if path.ends_with("/HEAD") {
        b"BWH1"
    } else if path.contains("/frontier/") {
        b"BWN1"
    } else if path.contains("/runs/id-directory/") {
        b"BID1"
    } else if path.contains("/descriptors/") {
        b"BWD1"
    } else if path.ends_with("/COMMIT") {
        b"BWC1"
    } else if path.ends_with("/STATE") {
        b"BWS1"
    } else if path.starts_with("id-directory/claim-shards/")
        && (path.ends_with("/LOCK") || path.ends_with("/GATE"))
    {
        b"BCL1"
    } else {
        panic!(
            "role {} has no S3-compatible format assertion for {path}",
            role.as_str()
        );
    }
}

#[test]
fn collection_wal_frontier_head_uses_collection_magic() {
    assert_eq!(
        expected_magic(
            "collection/wal-frontier/07/HEAD",
            PhysicalObjectRole::CommitMarker,
        ),
        b"BCWH"
    );
}

fn deterministic_bytes(len: usize) -> Vec<u8> {
    let mut state = 0x4d59_5df4_d0f3_3173_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn read_object_range(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    relative: &str,
    range: Range<u64>,
    runtime: &tokio::runtime::Runtime,
) -> Vec<u8> {
    let location = resolve(prefix, relative);
    runtime
        .block_on(async { store.get_range(&location, range).await })
        .unwrap()
        .to_vec()
}

fn resolve(prefix: &ObjectPath, relative: &str) -> ObjectPath {
    let relative = relative.trim_matches('/');
    let path = if prefix.as_ref().is_empty() {
        relative.to_string()
    } else if relative.is_empty() {
        prefix.as_ref().to_string()
    } else {
        format!("{}/{relative}", prefix.as_ref())
    };
    ObjectPath::parse(path).unwrap()
}

fn relative_path(prefix: &ObjectPath, location: &ObjectPath) -> String {
    let path = location.as_ref();
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        return path.to_string();
    }

    path.strip_prefix(prefix)
        .and_then(|value| value.strip_prefix('/'))
        .unwrap()
        .to_string()
}
