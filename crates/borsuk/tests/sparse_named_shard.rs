#![allow(missing_docs)]

use std::{collections::BTreeMap, fs, path::Path, time::Duration};

use arrow_array::{Array, BinaryArray, RecordBatch, UInt64Array};
use borsuk::{
    BorsukIndex, CompactionOptions, Fusion, GarbageCollectionOptions, HybridOptions, HybridQuery,
    IndexConfig, SearchOptions, SparseVector, VectorKind, VectorMetric, VectorRecord, VectorSpec,
    sparse_dot,
};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const SIDECAR_HEADER_LEN: usize = 64 + 32;

fn config(uri: String, segment_max_vectors: usize) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors,
        ram_budget_bytes: None,
        text: false,
        named_vectors: BTreeMap::from([(
            "lexical".to_string(),
            VectorSpec {
                dimensions: 128,
                metric: VectorMetric::InnerProduct,
                kind: VectorKind::Sparse,
            },
        )]),
    }
}

fn record(id: impl Into<String>, dense: [f32; 2], term: u32, weight: f32) -> VectorRecord {
    VectorRecord::new(id.into(), dense.to_vec())
        .with_named_sparse_vector("lexical", vec![term], vec![weight])
        .unwrap()
}

fn hit_ids(hits: &[borsuk::SearchHit]) -> Vec<String> {
    hits.iter().map(|hit| hit.id.to_string()).collect()
}

fn sidecar_path(root: &Path, checksum: &str) -> std::path::PathBuf {
    root.join("svidx")
        .join("lexical")
        .join(&checksum[..2])
        .join(format!("{checksum}.svidx"))
}

fn sidecar_batches(path: &Path) -> Vec<RecordBatch> {
    let bytes = fs::read(path).unwrap();
    ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes[SIDECAR_HEADER_LEN..]))
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn sidecar_rows(path: &Path) -> Vec<(Vec<u8>, u64)> {
    let mut rows = Vec::new();
    for batch in sidecar_batches(path) {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let generations = batch
            .column_by_name("generation")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!ids.is_null(row));
            assert!(!generations.is_null(row));
            rows.push((ids.value(row).to_vec(), generations.value(row)));
        }
    }
    rows
}

fn collect_sidecars(root: &Path, paths: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_sidecars(&path, paths);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "svidx")
        {
            paths.push(path);
        }
    }
}

#[test]
fn sparse_named_vectors_are_sharded_across_segments_and_match_brute_force() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(config(uri, 2)).unwrap();
    let rows = (0..8_u32)
        .map(|row| {
            let vector = SparseVector::new(vec![row % 3, 10], vec![row as f32 + 1.0, 0.5]).unwrap();
            (format!("doc-{row}"), vector)
        })
        .collect::<Vec<_>>();
    index
        .add(
            rows.iter()
                .enumerate()
                .map(|(row, (id, vector))| {
                    VectorRecord::new(id.clone(), vec![row as f32, 0.0])
                        .with_named_sparse_vector(
                            "lexical",
                            vector.indices().to_vec(),
                            vector.values().to_vec(),
                        )
                        .unwrap()
                })
                .collect(),
        )
        .unwrap();

    assert!(index.manifest().segments.len() > 1);
    for summary in &index.manifest().segments {
        assert!(sidecar_path(dir.path(), &summary.checksum).is_file());
    }

    let query = SparseVector::new(vec![1, 10], vec![2.0, 1.0]).unwrap();
    let mut expected = rows
        .iter()
        .filter_map(|(id, vector)| {
            let score = sparse_dot(&query, vector);
            (score > 0.0).then_some((id.clone(), score))
        })
        .collect::<Vec<_>>();
    expected.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    expected.truncate(5);

    let hits = index
        .search_sparse_named(
            "lexical",
            query.indices().to_vec(),
            query.values().to_vec(),
            5,
        )
        .unwrap();
    assert_eq!(
        hit_ids(&hits),
        expected.into_iter().map(|(id, _)| id).collect::<Vec<_>>()
    );
}

#[test]
fn sparse_named_upsert_is_visible_immediately_and_old_generation_is_hidden() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(config(uri, 2)).unwrap();
    index.add(vec![record("x", [0.0, 0.0], 1, 4.0)]).unwrap();
    index.upsert(vec![record("x", [1.0, 0.0], 2, 7.0)]).unwrap();

    let new_hits = index
        .search_sparse_named("lexical", vec![2], vec![1.0], 10)
        .unwrap();
    assert_eq!(hit_ids(&new_hits), ["x"]);
    assert_eq!(new_hits.iter().filter(|hit| hit.id == "x").count(), 1);
    assert!(
        index
            .search_sparse_named("lexical", vec![1], vec![1.0], 10)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn sparse_named_delete_is_respected_without_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(config(uri, 2)).unwrap();
    index
        .add(vec![
            record("keep", [0.0, 0.0], 3, 1.0),
            record("delete", [1.0, 0.0], 3, 9.0),
        ])
        .unwrap();
    index.delete(["delete"]).unwrap();

    assert_eq!(
        hit_ids(
            &index
                .search_sparse_named("lexical", vec![3], vec![1.0], 10)
                .unwrap()
        ),
        ["keep"]
    );
}

#[test]
fn compaction_preserves_live_sparse_rows_and_prunes_superseded_and_deleted_generations() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(config(uri, 2)).unwrap();
    index
        .add(vec![
            record("stable-a", [0.0, 0.0], 4, 2.0),
            record("replace", [1.0, 0.0], 4, 8.0),
            record("delete", [2.0, 0.0], 4, 7.0),
            record("stable-b", [3.0, 0.0], 4, 3.0),
        ])
        .unwrap();
    index
        .upsert(vec![record("replace", [1.5, 0.0], 5, 9.0)])
        .unwrap();
    index.delete(["delete"]).unwrap();

    let before_old = hit_ids(
        &index
            .search_sparse_named("lexical", vec![4], vec![1.0], 10)
            .unwrap(),
    );
    let before_new = hit_ids(
        &index
            .search_sparse_named("lexical", vec![5], vec![1.0], 10)
            .unwrap(),
    );
    let report = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: None,
            min_segments: 1,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();
    assert!(report.compacted);

    assert_eq!(
        hit_ids(
            &index
                .search_sparse_named("lexical", vec![4], vec![1.0], 10)
                .unwrap()
        ),
        before_old
    );
    assert_eq!(
        hit_ids(
            &index
                .search_sparse_named("lexical", vec![5], vec![1.0], 10)
                .unwrap()
        ),
        before_new
    );

    index
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();
    let mut active_rows = Vec::new();
    let mut active_sidecars = Vec::new();
    collect_sidecars(
        &dir.path().join("svidx").join("lexical"),
        &mut active_sidecars,
    );
    for path in active_sidecars {
        active_rows.extend(sidecar_rows(&path));
    }
    active_rows.sort();
    assert_eq!(
        active_rows,
        vec![
            (b"replace".to_vec(), 1),
            (b"stable-a".to_vec(), 0),
            (b"stable-b".to_vec(), 0),
        ]
    );
}

#[test]
fn hybrid_sparse_leg_uses_the_upserted_vector_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(config(uri, 2)).unwrap();
    index
        .add(vec![
            record("x", [0.0, 0.0], 1, 10.0),
            record("y", [2.0, 0.0], 1, 5.0),
        ])
        .unwrap();
    index
        .upsert(vec![record("x", [0.0, 0.0], 2, 10.0)])
        .unwrap();

    let options = HybridOptions {
        k: 2,
        fusion: Fusion::Weighted {
            weights: BTreeMap::from([("".to_string(), 0.1), ("lexical".to_string(), 1.0)]),
        },
        candidate_depth: 2,
        dense_options: SearchOptions::exact(2),
    };
    let new_report = index
        .search_hybrid(
            &HybridQuery::new()
                .with_vector("", vec![0.0, 0.0])
                .with_named_sparse_query("lexical", vec![2], vec![1.0]),
            options.clone(),
        )
        .unwrap();
    assert_eq!(new_report.hits[0].id, "x");

    let old_report = index
        .search_hybrid(
            &HybridQuery::new()
                .with_vector("", vec![2.0, 0.0])
                .with_named_sparse_query("lexical", vec![1], vec![1.0]),
            options,
        )
        .unwrap();
    assert_eq!(old_report.hits[0].id, "y");
}

#[test]
fn sparse_named_sidecars_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    {
        let mut index = BorsukIndex::create(config(uri.clone(), 2)).unwrap();
        index
            .add(vec![
                record("a", [0.0, 0.0], 6, 2.0),
                record("b", [1.0, 0.0], 6, 5.0),
                record("c", [2.0, 0.0], 7, 9.0),
            ])
            .unwrap();
    }

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        hit_ids(
            &reopened
                .search_sparse_named("lexical", vec![6], vec![1.0], 10)
                .unwrap()
        ),
        ["b", "a"]
    );
}
