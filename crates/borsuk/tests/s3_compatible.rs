#![allow(missing_docs)]

use std::{env, ops::Range};

use borsuk::{
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, LeafCapability,
    LeafMode, PhysicalObjectRole, SearchMode, SearchOptions, VectorMetric, VectorRecord, WalConfig,
    physical_object_role_for_path,
};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, parse_url_opts, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};
use tokio::runtime::Builder;
use url::Url;
use uuid::Uuid;

const LARGE_ID_BYTES: usize = 17 * 1024 * 1024;

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

    assert_s3_compatible_standard_layout(&uri);

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
    let mut index = BorsukIndex::create_with_wal(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 1,
            segment_max_vectors: 4,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
        WalConfig {
            enabled: true,
            flush_threshold_runs: usize::MAX,
            flush_threshold_records: usize::MAX,
            flush_threshold_bytes: u64::MAX,
            collection_flush_threshold_bytes: u64::MAX,
        },
    )
    .unwrap();

    // Each ID is authenticated in the primary, ID-directory, and route-plan
    // payloads. Keep every atomic append below the 64 MiB transaction bound,
    // then flush four incompressible IDs into one >68 MiB segment so the S3
    // multipart path is exercised without weakening atomicity.
    let large_records = [
        0x4d59_5df4_d0f3_3173_u64,
        0x9e37_79b9_7f4a_7c15_u64,
        0xd1b5_4a32_d192_ed03_u64,
        0x94d0_49bb_1331_11eb_u64,
    ]
    .into_iter()
    .enumerate()
    .map(|(ordinal, seed)| {
        (
            deterministic_bytes(LARGE_ID_BYTES, seed),
            vec![ordinal as f32],
        )
    })
    .collect::<Vec<_>>();
    for (id, vector) in &large_records {
        for attempt in 0..128 {
            match index.add(vec![VectorRecord::new_bytes(id.clone(), vector.clone())]) {
                Ok(()) => break,
                Err(error) if error.code() == "ingest_backpressure" && attempt < 127 => {
                    // Retrying an uncommitted request selects another source
                    // shard if this large append collided with a near-full one.
                }
                Err(error) => panic!("bounded positioned append failed: {error}"),
            }
        }
    }
    index.flush().unwrap();
    assert_large_segment_object(&uri);

    let reopened = BorsukIndex::open(&uri).unwrap();
    for (id, vector) in large_records {
        assert_eq!(reopened.get_vector_by_id(&id).unwrap(), Some(vector));
    }
}

fn assert_large_segment_object(uri: &str) {
    const MULTIPART_THRESHOLD_BYTES: u64 = 64 * 1024 * 1024;

    let url = Url::parse(uri).unwrap();
    let (store, prefix) = parse_url_opts(&url, env::vars()).unwrap();
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let objects = runtime
        .block_on(async { store.list(Some(&prefix)).try_collect::<Vec<_>>().await })
        .unwrap();
    assert!(
        objects.iter().any(|object| {
            relative_path(&prefix, &object.location).starts_with("segments/")
                && object.size > MULTIPART_THRESHOLD_BYTES
        }),
        "the fixture must materialize a segment larger than the multipart threshold"
    );
}

fn assert_s3_compatible_standard_layout(uri: &str) {
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
        if is_checked_json_coordination_path(path) {
            assert_checked_json_coordination(store.as_ref(), &prefix, path, *size, &runtime);
            continue;
        }
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
            .any(|(path, _)| path.starts_with("logical-cell-catalogs/")
                && path.ends_with(".parquet")),
        "generation one must pin a standard logical-cell catalog: {objects:?}"
    );
    assert!(
        objects.iter().any(
            |(path, _)| path.starts_with("positioned-log/payloads/parquet/")
                && path.ends_with(".parquet")
        ),
        "default writes must leave authenticated positioned payload tables: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .any(|(path, _)| path.starts_with("positioned-log/envelopes/")
                && path.ends_with(".parquet")),
        "default writes must leave an authenticated positioned envelope: {objects:?}"
    );
    assert_eq!(
        objects
            .iter()
            .filter(|(path, _)| {
                path.starts_with("positioned-log/heads/") && path.ends_with(".json")
            })
            .count(),
        64,
        "positioned visibility must have one checked head per source shard: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .any(|(path, _)| path.starts_with("id-directory/claim-pages/")),
        "insert-only coordination must use packed claim pages: {objects:?}"
    );
    assert!(
        objects.iter().all(|(path, _)| {
            !path.ends_with(".borsuk")
                && (!path.ends_with(".json") || is_checked_json_coordination_path(path))
        }),
        "only checked coordination JSON may accompany standard binary tables: {objects:?}"
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
    let document =
        serde_json::from_slice::<LaneCoordinationDocument<CollectionCurrentDocument>>(&current)
            .expect("collection/CURRENT must be a checked JSON pointer");
    assert_eq!(document.schema_version, 1);
    assert_eq!(document.object_role, "collection_current");
    assert_lane_coordination_checksum("collection/CURRENT", &document);
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LaneCoordinationDocument<T> {
    schema_version: u8,
    object_role: String,
    payload_checksum_blake3: String,
    payload: T,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CollectionCurrentDocument {
    snapshot_path: String,
    snapshot_checksum: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CollectionMaterializationWatermarkDocument {
    sequence: u64,
    prefix_digest: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CollectionManifestReferenceDocument {
    modality: String,
    prefix: String,
    version: u64,
    manifest_path: String,
    manifest_checksum: String,
    routing_path: String,
    routing_checksum: String,
    pivots_path: String,
    pivots_checksum: String,
    consumed_wal_frontier_checksum: String,
    resident_bytes_estimate: u64,
    resident_routing_bytes_estimate: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CollectionSnapshotDocument {
    generation: u64,
    schema_fingerprint: String,
    previous_snapshot_checksum: Option<String>,
    positioned_source_epoch: u64,
    positioned_materialized_watermarks: Vec<CollectionMaterializationWatermarkDocument>,
    modalities: Vec<CollectionManifestReferenceDocument>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ActiveStripeDirectoryDocument {
    generation: u64,
    active_bits: u64,
    activation_epochs: Vec<u64>,
    retirement_manifest_versions: Vec<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LaneEpochSealDocument {
    lease_epoch: u64,
    durable_sequence: u64,
    materialized_sequence: u64,
    materialized_manifest_version: u64,
    max_mutation_hlc: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LaneEpochHeadDocument {
    lane: u16,
    lease_epoch: u64,
    lease_owner: [u8; 16],
    lease_expires_at_ms: u64,
    durable_sequence: u64,
    materialized_sequence: u64,
    materialized_manifest_version: u64,
    max_mutation_hlc: u64,
    sealed_epoch: Option<LaneEpochSealDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PositionedCommitReferenceDocument {
    transaction_digest: String,
    request_digest: String,
    envelope_checksum: String,
    sequence: u64,
    rows: u64,
    encoded_bytes: u64,
    materialized_collection_generation: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PositionedShardHeadDocument {
    layout: u16,
    source_epoch: u64,
    shard: u8,
    schema_fingerprint: String,
    durable_sequence: u64,
    materialized_sequence: u64,
    materialized_prefix_digest: String,
    materialized_collection_generation: u64,
    evicted_recent_through_collection_generation: u64,
    pending_rows: u64,
    pending_bytes: u64,
    pending: Vec<PositionedCommitReferenceDocument>,
    recent: Vec<PositionedCommitReferenceDocument>,
}

fn is_checked_json_coordination_path(path: &str) -> bool {
    path == "collection/CURRENT"
        || (path.starts_with("collection/snapshots/") && path.ends_with(".json"))
        || path == "lane-log/ACTIVE"
        || (path.starts_with("lane-log/lanes/") && path.ends_with("/HEAD"))
        || (path.starts_with("positioned-log/heads/") && path.ends_with(".json"))
}

fn assert_checked_json_coordination(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    path: &str,
    size: u64,
    runtime: &tokio::runtime::Runtime,
) {
    let max_bytes = if path.starts_with("collection/snapshots/") {
        256 * 1024
    } else {
        64 * 1024
    };
    assert!(size <= max_bytes, "{path} exceeds its coordination bound");
    let bytes = read_object_range(store, prefix, path, 0..size, runtime);
    if path == "collection/CURRENT" {
        let document =
            serde_json::from_slice::<LaneCoordinationDocument<CollectionCurrentDocument>>(&bytes)
                .unwrap_or_else(|error| panic!("{path} is not checked collection JSON: {error}"));
        assert_eq!(document.schema_version, 1, "{path} schema marker");
        assert_eq!(document.object_role, "collection_current", "{path} role");
        assert_hex_checksum(
            path,
            "snapshot checksum",
            &document.payload.snapshot_checksum,
        );
        assert_eq!(
            document.payload.snapshot_path,
            format!(
                "collection/snapshots/{}.json",
                document.payload.snapshot_checksum
            ),
            "{path} content-addressed snapshot authority"
        );
        assert_lane_coordination_checksum(path, &document);
    } else if path.starts_with("collection/snapshots/") {
        let path_checksum = path
            .strip_prefix("collection/snapshots/")
            .and_then(|suffix| suffix.strip_suffix(".json"))
            .unwrap_or_else(|| panic!("invalid collection snapshot path: {path}"));
        assert_hex_checksum(path, "path checksum", path_checksum);
        assert_eq!(
            path_checksum,
            blake3::hash(&bytes).to_hex().as_str(),
            "{path} content-addressed object checksum"
        );
        let document =
            serde_json::from_slice::<LaneCoordinationDocument<CollectionSnapshotDocument>>(&bytes)
                .unwrap_or_else(|error| panic!("{path} is not checked collection JSON: {error}"));
        assert_eq!(document.schema_version, 1, "{path} schema marker");
        assert_eq!(document.object_role, "collection_snapshot", "{path} role");
        assert_eq!(
            document.payload.positioned_materialized_watermarks.len(),
            64,
            "{path} positioned watermark count"
        );
        for watermark in &document.payload.positioned_materialized_watermarks {
            assert_hex_checksum(path, "positioned prefix digest", &watermark.prefix_digest);
            assert!(
                watermark.sequence > 0
                    || watermark.prefix_digest
                        == blake3::hash(b"borsuk.positioned.materialized-prefix.empty.v1\0")
                            .to_hex()
                            .to_string(),
                "{path} empty watermark digest"
            );
        }
        assert_lane_coordination_checksum(path, &document);
    } else if path == "lane-log/ACTIVE" {
        let document = serde_json::from_slice::<
            LaneCoordinationDocument<ActiveStripeDirectoryDocument>,
        >(&bytes)
        .unwrap_or_else(|error| panic!("{path} is not checked coordination JSON: {error}"));
        assert_eq!(document.schema_version, 31, "{path} schema marker");
        assert_eq!(
            document.object_role, "active_stripe_directory",
            "{path} role"
        );
        assert_eq!(document.payload.activation_epochs.len(), 64, "{path}");
        assert_eq!(
            document.payload.retirement_manifest_versions.len(),
            64,
            "{path}"
        );
        assert_lane_coordination_checksum(path, &document);
    } else if path.starts_with("lane-log/lanes/") {
        let document =
            serde_json::from_slice::<LaneCoordinationDocument<LaneEpochHeadDocument>>(&bytes)
                .unwrap_or_else(|error| panic!("{path} is not checked coordination JSON: {error}"));
        assert_eq!(document.schema_version, 31, "{path} schema marker");
        assert_eq!(document.object_role, "lane_epoch_head", "{path} role");
        assert_lane_coordination_checksum(path, &document);
    } else {
        let document = serde_json::from_slice::<PositionedShardHeadDocument>(&bytes)
            .unwrap_or_else(|error| panic!("{path} is not checked coordination JSON: {error}"));
        let shard = path
            .strip_prefix("positioned-log/heads/")
            .and_then(|suffix| suffix.strip_suffix(".json"))
            .and_then(|value| value.parse::<u8>().ok())
            .unwrap_or_else(|| panic!("invalid positioned shard-head path: {path}"));
        assert_eq!(document.layout, 16, "{path} layout marker");
        assert!(document.source_epoch > 0, "{path} source epoch");
        assert_eq!(document.shard, shard, "{path} shard authority");
        assert_hex_checksum(path, "schema fingerprint", &document.schema_fingerprint);
        assert_hex_checksum(
            path,
            "materialized prefix digest",
            &document.materialized_prefix_digest,
        );
        assert!(
            document.materialized_sequence <= document.durable_sequence,
            "{path} materialized frontier"
        );
        assert_eq!(
            document.pending_rows,
            document.pending.iter().map(|entry| entry.rows).sum::<u64>(),
            "{path} pending row total"
        );
        assert_eq!(
            document.pending_bytes,
            document
                .pending
                .iter()
                .map(|entry| entry.encoded_bytes)
                .sum::<u64>(),
            "{path} pending byte total"
        );
        assert!(
            document.evicted_recent_through_collection_generation
                <= document.materialized_collection_generation,
            "{path} recent-receipt frontier"
        );
        for entry in document.pending.iter().chain(&document.recent) {
            assert!(entry.sequence > 0, "{path} commit sequence");
            assert_hex_checksum(path, "transaction digest", &entry.transaction_digest);
            assert_hex_checksum(path, "request digest", &entry.request_digest);
            assert_hex_checksum(path, "envelope checksum", &entry.envelope_checksum);
            assert!(entry.rows > 0, "{path} commit rows");
            assert!(entry.encoded_bytes > 0, "{path} commit bytes");
            if entry.materialized_collection_generation > 0 {
                assert!(
                    entry.materialized_collection_generation
                        <= document.materialized_collection_generation,
                    "{path} receipt generation"
                );
            }
        }
    }
}

fn assert_lane_coordination_checksum<T: Serialize>(
    path: &str,
    document: &LaneCoordinationDocument<T>,
) {
    let payload = serde_json::to_vec(&document.payload).unwrap();
    assert_eq!(
        document.payload_checksum_blake3,
        blake3::hash(&payload).to_hex().to_string(),
        "{path} payload checksum"
    );
}

fn assert_hex_checksum(path: &str, label: &str, value: &str) {
    assert_eq!(value.len(), 64, "{path} {label} length");
    assert!(
        value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{path} {label} encoding"
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
    if path.starts_with("collection/wal-frontier/") && path.ends_with("/HEAD") {
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
    } else if path.starts_with("id-directory/claim-pages/") && path.ends_with("/STATE") {
        b"BCL1"
    } else if path.ends_with("/STATE") {
        b"BWS1"
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

fn deterministic_bytes(len: usize, mut state: u64) -> Vec<u8> {
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
