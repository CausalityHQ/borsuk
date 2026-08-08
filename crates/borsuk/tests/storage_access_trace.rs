//! Storage access tracing and persisted-object role contracts.

use borsuk::{
    BorsukIndex, Filter, IndexConfig, LeafMode, MetaValue, Metadata, Op, PhysicalObjectRole,
    SearchOptions, StorageAccessEvent, StorageAccessTrace, VectorElementType, VectorKind,
    VectorMetric, VectorRecord, VectorSpec, install_storage_access_trace,
    physical_object_role_for_path,
};
use std::collections::BTreeMap;

#[test]
fn persisted_paths_have_stable_physical_object_roles() {
    let cases = [
        ("collection/CURRENT", PhysicalObjectRole::Catalog),
        ("collection/snapshots/abc.bin", PhysicalObjectRole::Catalog),
        ("manifests/0001.parquet", PhysicalObjectRole::Catalog),
        ("wal/0001.parquet", PhysicalObjectRole::WalRun),
        ("cells/1/42/wal/3/HEAD", PhysicalObjectRole::LaneHead),
        ("lane-log/lanes/0003/HEAD", PhysicalObjectRole::LaneHead),
        ("lane-log/ACTIVE", PhysicalObjectRole::WriterDirectory),
        (
            "lane-log/lanes/0003/blocks/0001-deadbeef.blk",
            PhysicalObjectRole::WalRun,
        ),
        (
            "lane-log/lanes/0003/epochs/0000000000000001/extents/0000000000000001.arrow",
            PhysicalObjectRole::WalRun,
        ),
        (
            "cells/1/42/wal/3/frontier/abc.bin",
            PhysicalObjectRole::LaneHead,
        ),
        (
            "cells/1/42/wal/3/runs/abc.parquet",
            PhysicalObjectRole::WalRun,
        ),
        ("transactions/abc/STATE", PhysicalObjectRole::CommitMarker),
        ("transactions/abc/COMMIT", PhysicalObjectRole::CommitMarker),
        (
            "transactions/abc/descriptors/def.bin",
            PhysicalObjectRole::CommitMarker,
        ),
        (
            "collection/wal-frontier/07/HEAD",
            PhysicalObjectRole::CommitMarker,
        ),
        (
            "collection/write-epochs/schema-abc/pending/txn.commit",
            PhysicalObjectRole::CommitMarker,
        ),
        (
            "routing/pages/L0/page.parquet",
            PhysicalObjectRole::RoutingPage,
        ),
        ("graphs/L0/aa/graph.parquet", PhysicalObjectRole::GraphIndex),
        (
            "segments/L0/aa/segment.vortex",
            PhysicalObjectRole::NormalSegment,
        ),
        (
            "global-pq/bundles/aa/bundle.arrow",
            PhysicalObjectRole::ProductCodes,
        ),
        ("vectors/aa/vector.arrow", PhysicalObjectRole::ExactVectors),
        ("fidx/aa/filter.fidx", PhysicalObjectRole::FilterIndex),
        (
            "lexical/text/postings/page.parquet",
            PhysicalObjectRole::LexicalBlock,
        ),
        (
            "late-interaction/title/aa.arrow",
            PhysicalObjectRole::LateInteraction,
        ),
        ("tombstones/aa/page.parquet", PhysicalObjectRole::Tombstone),
        (
            "id-directory/ff/page.parquet",
            PhysicalObjectRole::IdDirectory,
        ),
        (
            "id-directory/claim-shards/03/LOCK",
            PhysicalObjectRole::IdDirectory,
        ),
    ];
    for (path, expected) in cases {
        assert_eq!(physical_object_role_for_path(path), expected, "{path}");
    }
}

#[test]
fn unknown_paths_are_explicit_instead_of_misattributed() {
    assert_eq!(
        physical_object_role_for_path("scratch/temporary.bin"),
        PhysicalObjectRole::Unknown
    );
}

#[test]
fn trace_sink_is_opt_in_and_writes_a_stable_schema() {
    let directory = tempfile::tempdir().unwrap();
    let disabled_path = directory.path().join("disabled.csv");
    StorageAccessTrace::disabled()
        .record(StorageAccessEvent::read(
            "wal/run.parquet",
            "parquet",
            128,
            64,
        ))
        .unwrap();
    assert!(!disabled_path.exists());

    let output = directory.path().join("trace.csv");
    let trace = StorageAccessTrace::create(&output).unwrap();
    trace
        .record(StorageAccessEvent::read(
            "segments/L0/a.vortex",
            "vortex",
            1024,
            256,
        ))
        .unwrap();
    trace
        .record(StorageAccessEvent::decode(
            "segments/L0/a.vortex",
            "vortex",
            1024,
            "record_id|pq_codes",
            "rows:3|9",
            2,
            128,
            42_000,
        ))
        .unwrap();
    let csv = std::fs::read_to_string(output).unwrap();
    assert!(csv.starts_with(
        "operation,object_role,path,physical_format,object_bytes,request_count,bytes_fetched,"
    ));
    assert!(csv.contains("read,normal_segment,segments/L0/a.vortex,vortex,1024,1,256,"));
    assert!(csv.contains(
        "decode,normal_segment,segments/L0/a.vortex,vortex,1024,0,0,\
record_id|pq_codes,rows:3|9,2,128,42000,memory,ok"
    ));
    assert!(csv.lines().all(|line| line.split(',').count() == 14));
}

#[test]
fn real_index_writes_are_traced_at_the_common_storage_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("storage-access.csv");
    install_storage_access_trace(StorageAccessTrace::create(&output).unwrap()).unwrap();
    let index_root = directory.path().join("index");
    let mut index = BorsukIndex::create_with_leaf_capability(
        IndexConfig {
            uri: index_root.to_string_lossy().into_owned(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 4,
            ram_budget_bytes: None,
            text: true,
            named_vectors: BTreeMap::from([
                (
                    "sparse".to_string(),
                    VectorSpec {
                        dimensions: 8,
                        metric: VectorMetric::InnerProduct,
                        kind: VectorKind::Sparse,
                        element_type: VectorElementType::Float32,
                    },
                ),
                (
                    "tokens".to_string(),
                    VectorSpec {
                        dimensions: 2,
                        metric: VectorMetric::InnerProduct,
                        kind: VectorKind::LateInteraction,
                        element_type: VectorElementType::Float32,
                    },
                ),
            ]),
        },
        borsuk::LeafCapability::GraphEnabled,
    )
    .unwrap();
    index
        .add(vec![
            traced_record("one", [1.0, 2.0], "needle one", "keep", 3, 2.0),
            traced_record("two", [2.0, 3.0], "needle two", "drop", 4, 1.0),
            traced_record("three", [9.0, 9.0], "unrelated", "keep", 3, 0.5),
        ])
        .unwrap();
    index.flush().unwrap();
    drop(index);
    let reopened = BorsukIndex::open(index_root.to_str().unwrap()).unwrap();
    assert_eq!(reopened.list_records(0, 10).unwrap().len(), 3);
    assert_eq!(
        reopened
            .search_ids(&[1.0, 2.0], SearchOptions::exact(1))
            .unwrap()
            .len(),
        1
    );
    assert!(
        !reopened
            .search_ids(&[1.0, 2.0], SearchOptions::approx(2, LeafMode::FlatScan),)
            .unwrap()
            .is_empty()
    );
    assert!(
        !reopened
            .search_ids(&[1.0, 2.0], SearchOptions::approx(2, LeafMode::Graph))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reopened
            .search_ids(
                &[1.0, 2.0],
                SearchOptions::exact(2).with_filter(Filter::Cmp {
                    path: "group".to_string(),
                    op: Op::Eq,
                    value: MetaValue::Str("keep".to_string()),
                }),
            )
            .unwrap()
            .len(),
        2
    );
    assert!(!reopened.search_text("needle", 2).unwrap().hits.is_empty());
    assert!(
        !reopened
            .search_sparse_named("sparse", vec![3], vec![1.0], 2)
            .unwrap()
            .is_empty()
    );
    assert!(
        !reopened
            .search_late_interaction("tokens", vec![vec![1.0, 0.0]], 2)
            .unwrap()
            .is_empty()
    );
    drop(reopened);

    let csv = std::fs::read_to_string(output).unwrap();
    assert!(csv.contains(",catalog,collection/CURRENT,"));
    assert_eq!(
        csv.lines()
            .filter(|line| {
                let fields = line.split(',').collect::<Vec<_>>();
                fields.first() == Some(&"write")
                    && fields.get(1) == Some(&"commit_marker")
                    && fields.get(2).is_some_and(|path| {
                        path.starts_with("collection/write-epochs/")
                            && path.contains("/pending/")
                            && path.ends_with(".commit")
                    })
            })
            .count(),
        1,
        "one logical mutation publishes exactly one immutable collection commit"
    );
    assert_eq!(
        csv.lines()
            .filter(|line| {
                let fields = line.split(',').collect::<Vec<_>>();
                fields.first() == Some(&"write")
                    && fields
                        .get(2)
                        .is_some_and(|path| path.starts_with("collection/wal-frontier/"))
            })
            .count(),
        0,
        "ordinary publication must not coordinate through a mutable WAL frontier"
    );
    assert!(csv.lines().any(|line| {
        let fields = line.split(',').collect::<Vec<_>>();
        fields.get(1) == Some(&"wal_run")
            && fields
                .get(2)
                .is_some_and(|path| path.starts_with("cells/") && path.contains("/wal/"))
    }));
    assert!(csv.contains(",normal_segment,segments/"));
    assert!(csv.contains(",graph_index,graphs/"));
    assert!(csv.contains(",exact_vectors,vectors/"));
    assert!(csv.lines().any(|line| line.starts_with("write,")));
    assert!(csv.lines().any(|line| line.starts_with("read,")));
    assert!(
        csv.lines()
            .any(|line| line.starts_with("read,normal_segment,"))
    );
    assert!(
        csv.lines()
            .any(|line| line.starts_with("read,exact_vectors,"))
    );
    assert!(
        csv.lines().any(|line| {
            let fields = line.split(',').collect::<Vec<_>>();
            fields.first() == Some(&"read")
                && fields.get(1) == Some(&"catalog")
                && fields.get(2) == Some(&"collection/CURRENT")
                && fields.get(5) == Some(&"1")
        }),
        "a coordination-object read must charge its conditional GET: {csv}"
    );
    assert!(
        !csv.lines()
            .skip(1)
            .any(|line| line.split(',').nth(1) == Some("unknown")),
        "{csv}"
    );
    for line in csv.lines().filter(|line| line.starts_with("decode,")) {
        let fields = line.split(',').collect::<Vec<_>>();
        assert!(!fields[3].is_empty(), "{line}");
        let requested = fields[9].parse::<u64>().unwrap();
        let decoded = fields[10].parse::<u64>().unwrap();
        assert!(requested <= decoded, "{line}");
        assert!(!fields[11].is_empty(), "{line}");
    }
    assert!(
        csv.lines()
            .any(|line| line.starts_with("decode,normal_segment,"))
    );
    assert!(
        csv.lines()
            .any(|line| line.starts_with("decode,exact_vectors,"))
    );
    assert!(csv.contains(",filter_index,"));
    assert!(csv.contains(",lexical_block,"));
    assert!(csv.contains(",late_interaction,"));
}

fn traced_record(
    id: &str,
    vector: [f32; 2],
    text: &str,
    group: &str,
    sparse_index: u32,
    sparse_value: f32,
) -> VectorRecord {
    VectorRecord::new(id, vector.to_vec())
        .with_text(text)
        .with_metadata(Metadata::from([(
            "group".to_string(),
            MetaValue::Str(group.to_string()),
        )]))
        .with_named_sparse_vector("sparse", vec![sparse_index], vec![sparse_value])
        .unwrap()
        .with_late_interaction("tokens", vec![vec![vector[0], vector[1]]])
        .unwrap()
}
