#![allow(missing_docs)]

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use borsuk::{
    BorsukIndex, BuildConfig, CompactionOptions, Fusion, GarbageCollectionOptions, HybridOptions,
    HybridQuery, IndexConfig, LeafMode, MetaValue, Metadata, SearchOptions, VectorElementType,
    VectorKind, VectorMetric, VectorRecord, VectorSpec,
};

#[derive(Clone)]
struct DenseCase {
    name: &'static str,
    element_type: VectorElementType,
    metric: VectorMetric,
}

const DENSE_CASES: &[DenseCase] = &[
    DenseCase {
        name: "float32",
        element_type: VectorElementType::Float32,
        metric: VectorMetric::SquaredEuclidean,
    },
    DenseCase {
        name: "float16",
        element_type: VectorElementType::Float16,
        metric: VectorMetric::SquaredEuclidean,
    },
    DenseCase {
        name: "bfloat16",
        element_type: VectorElementType::BFloat16,
        metric: VectorMetric::SquaredEuclidean,
    },
    DenseCase {
        name: "float8-e4m3fn",
        element_type: VectorElementType::Float8E4M3Fn,
        metric: VectorMetric::SquaredEuclidean,
    },
    DenseCase {
        name: "float8-e5m2",
        element_type: VectorElementType::Float8E5M2,
        metric: VectorMetric::SquaredEuclidean,
    },
    DenseCase {
        name: "int8",
        element_type: VectorElementType::Int8,
        metric: VectorMetric::SquaredEuclidean,
    },
    DenseCase {
        name: "binary-hamming",
        element_type: VectorElementType::Binary,
        metric: VectorMetric::Hamming,
    },
    DenseCase {
        name: "binary-jaccard",
        element_type: VectorElementType::Binary,
        metric: VectorMetric::Jaccard,
    },
];

const SPARSE_TYPES: &[VectorElementType] =
    &[VectorElementType::Float32, VectorElementType::Float16];
const LATE_INTERACTION_TYPES: &[VectorElementType] =
    &[VectorElementType::Float32, VectorElementType::Float16];

fn base_config(uri: String, metric: VectorMetric, dimensions: usize) -> IndexConfig {
    IndexConfig {
        uri,
        metric,
        dimensions,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
        text: false,
        named_vectors: BTreeMap::new(),
    }
}

fn metadata(case: &str) -> Metadata {
    Metadata::from([("case".to_string(), MetaValue::Str(case.to_string()))])
}

fn dense_vectors(case: &DenseCase) -> [Vec<f32>; 5] {
    if case.element_type == VectorElementType::Binary {
        [
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 0.0, 0.0, 0.0],
            vec![0.0, 1.0, 0.0, 1.0],
            vec![0.0, 0.0, 0.0, 0.0],
            vec![0.0, 1.0, 1.0, 0.0],
        ]
    } else if case.element_type == VectorElementType::Int8 {
        [
            vec![1.0, -2.0, 3.0, 4.0],
            vec![1.0, -2.0, 2.0, 4.0],
            vec![-4.0, 4.0, -4.0, -4.0],
            vec![0.0, 0.0, 0.0, 0.0],
            vec![2.0, 1.0, -1.0, 3.0],
        ]
    } else {
        [
            vec![1.0, -2.0, 0.3, 4.0],
            vec![1.0, -2.0, 0.6, 4.0],
            vec![-4.0, 4.0, -4.0, -4.0],
            vec![0.0, 0.0, 0.0, 0.0],
            vec![2.0, 1.3, -0.7, 3.0],
        ]
    }
}

fn compact_all(index: &mut BorsukIndex, label: &str) {
    let report = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: None,
            min_segments: 1,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap_or_else(|error| panic!("{label}: compaction failed: {error}"));
    assert!(report.compacted, "{label}: compaction did not publish");
}

fn assert_sorted_distances(
    index: &BorsukIndex,
    query: &[f32],
    options: SearchOptions,
    label: &str,
) {
    let report = index
        .search_with_report(query, options)
        .unwrap_or_else(|error| panic!("{label}: search failed: {error}"));
    assert!(!report.hits.is_empty(), "{label}: search returned no hits");
    assert!(
        report
            .hits
            .windows(2)
            .all(|pair| pair[0].distance <= pair[1].distance),
        "{label}: distances are not ordered"
    );
}

#[test]
fn declared_dense_case_table_covers_every_stable_scalar_and_binary_metric() {
    let names = DENSE_CASES
        .iter()
        .map(|case| case.element_type.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        names,
        BTreeSet::from([
            "float32",
            "float16",
            "bfloat16",
            "float8-e4m3fn",
            "float8-e5m2",
            "int8",
            "binary",
        ])
    );
    assert!(
        DENSE_CASES
            .iter()
            .any(|case| case.metric == VectorMetric::Hamming)
    );
    assert!(
        DENSE_CASES
            .iter()
            .any(|case| case.metric == VectorMetric::Jaccard)
    );
}

#[test]
fn every_primary_dense_type_passes_the_mutable_persistent_lifecycle() {
    for case in DENSE_CASES {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let vectors = dense_vectors(case);
        let canonical = case.element_type.canonicalize(&vectors[0]).unwrap();
        let replacement = case.element_type.canonicalize(&vectors[4]).unwrap();
        let mut index = BorsukIndex::create_with_build_config(
            base_config(uri.clone(), case.metric.clone(), 4),
            BuildConfig {
                vector_element_type: case.element_type,
                ..BuildConfig::default()
            },
        )
        .unwrap_or_else(|error| panic!("{}: create failed: {error}", case.name));
        index
            .add(vec![
                VectorRecord::new("typed", vectors[0].clone()).with_metadata(metadata(case.name)),
                VectorRecord::new("near", vectors[1].clone()),
                VectorRecord::new("far", vectors[2].clone()),
                VectorRecord::new("delete", vectors[3].clone()),
            ])
            .unwrap_or_else(|error| panic!("{}: add failed: {error}", case.name));

        assert_eq!(
            index.get_vector("typed").unwrap(),
            Some(canonical.clone()),
            "{}: WAL value was not canonical",
            case.name
        );
        let record = index.get_record("typed").unwrap().unwrap();
        assert_eq!(record.1, metadata(case.name));
        assert_eq!(
            index
                .search_ids(&vectors[0], SearchOptions::exact(1))
                .unwrap(),
            ["typed"],
            "{}: source query was not canonicalized",
            case.name
        );

        index.flush().unwrap();
        drop(index);
        let mut reopened = BorsukIndex::open(&uri).unwrap();
        assert_eq!(
            reopened.build_config().vector_element_type,
            case.element_type,
            "{}: manifest type changed",
            case.name
        );
        assert_eq!(reopened.get_vector("typed").unwrap(), Some(canonical));
        assert_sorted_distances(
            &reopened,
            &vectors[0],
            SearchOptions::approx(3, LeafMode::FlatScan),
            case.name,
        );

        reopened
            .upsert(vec![
                VectorRecord::new("typed", vectors[4].clone()).with_metadata(metadata(case.name)),
            ])
            .unwrap();
        reopened.delete(&["delete".to_string()]).unwrap();
        reopened.flush().unwrap();
        compact_all(&mut reopened, case.name);
        reopened
            .gc_obsolete_segments(GarbageCollectionOptions {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .unwrap();
        drop(reopened);

        let final_reopen = BorsukIndex::open(&uri).unwrap();
        assert_eq!(final_reopen.get_vector("typed").unwrap(), Some(replacement));
        assert!(final_reopen.get_vector("delete").unwrap().is_none());
        assert_eq!(
            final_reopen
                .search_ids(&vectors[4], SearchOptions::exact(1))
                .unwrap(),
            ["typed"],
            "{}: final exact search failed",
            case.name
        );
    }
}

#[test]
fn every_named_dense_type_passes_wal_flush_compaction_and_reopen() {
    for case in DENSE_CASES {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let vectors = dense_vectors(case);
        let mut config = base_config(uri.clone(), VectorMetric::SquaredEuclidean, 2);
        config.named_vectors.insert(
            "typed".to_string(),
            VectorSpec {
                dimensions: 4,
                metric: case.metric.clone(),
                kind: VectorKind::Dense,
                element_type: case.element_type,
            },
        );
        let mut index = BorsukIndex::create(config)
            .unwrap_or_else(|error| panic!("named {}: create failed: {error}", case.name));
        index
            .add(vec![
                VectorRecord::new("best", vec![0.0, 0.0])
                    .with_named_vector("typed", vectors[0].clone()),
                VectorRecord::new("near", vec![1.0, 0.0])
                    .with_named_vector("typed", vectors[1].clone()),
                VectorRecord::new("far", vec![2.0, 0.0])
                    .with_named_vector("typed", vectors[2].clone()),
            ])
            .unwrap();
        let named_options = SearchOptions::exact(1).with_vector_name("typed");
        assert_eq!(
            index
                .search_ids(&vectors[0], named_options.clone())
                .unwrap(),
            ["best"],
            "named {}: WAL search failed",
            case.name
        );
        index.flush().unwrap();
        drop(index);

        let mut reopened = BorsukIndex::open(&uri).unwrap();
        assert_eq!(
            reopened
                .search_ids(&vectors[0], named_options.clone())
                .unwrap(),
            ["best"],
            "named {}: reopened search failed",
            case.name
        );
        reopened
            .upsert(vec![
                VectorRecord::new("best", vec![0.0, 1.0])
                    .with_named_vector("typed", vectors[4].clone()),
            ])
            .unwrap();
        reopened.delete(&["near".to_string()]).unwrap();
        reopened.flush().unwrap();
        compact_all(&mut reopened, case.name);
        reopened
            .gc_obsolete_segments(GarbageCollectionOptions {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .unwrap();
        drop(reopened);

        let final_reopen = BorsukIndex::open(&uri).unwrap();
        assert_eq!(
            final_reopen.search_ids(&vectors[4], named_options).unwrap(),
            ["best"],
            "named {}: final mutation search failed",
            case.name
        );
        let surviving = final_reopen
            .search_ids(
                &vectors[1],
                SearchOptions::exact(3).with_vector_name("typed"),
            )
            .unwrap();
        assert!(
            !surviving.iter().any(|id| id == "near"),
            "named {}: deleted row remained visible",
            case.name
        );
    }
}

#[test]
fn primary_sparse_input_is_canonical_for_float32_and_float16() {
    for element_type in SPARSE_TYPES {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create_with_build_config(
            base_config(uri.clone(), VectorMetric::InnerProduct, 8),
            BuildConfig {
                vector_element_type: *element_type,
                ..BuildConfig::default()
            },
        )
        .unwrap();
        let source = VectorRecord::from_sparse("sparse", vec![1, 6], vec![2.0, 3.0], 8).unwrap();
        let expected = element_type.canonicalize(&source.vector).unwrap();
        index.add(vec![source]).unwrap();
        assert_eq!(index.get_vector("sparse").unwrap(), Some(expected.clone()));
        index.flush().unwrap();
        drop(index);
        let reopened = BorsukIndex::open(&uri).unwrap();
        assert_eq!(reopened.get_vector("sparse").unwrap(), Some(expected));
    }
}

#[test]
fn named_sparse_float32_and_float16_pass_the_mutable_persistent_lifecycle() {
    for element_type in SPARSE_TYPES {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut config = base_config(uri.clone(), VectorMetric::SquaredEuclidean, 2);
        config.named_vectors.insert(
            "sparse".to_string(),
            VectorSpec {
                dimensions: 32,
                metric: VectorMetric::InnerProduct,
                kind: VectorKind::Sparse,
                element_type: *element_type,
            },
        );
        let mut index = BorsukIndex::create(config).unwrap();
        index
            .add(vec![
                VectorRecord::new("best", vec![0.0, 0.0])
                    .with_named_sparse_vector("sparse", vec![3, 17], vec![3.0, 2.0])
                    .unwrap(),
                VectorRecord::new("near", vec![1.0, 0.0])
                    .with_named_sparse_vector("sparse", vec![3], vec![2.0])
                    .unwrap(),
                VectorRecord::new("delete", vec![2.0, 0.0])
                    .with_named_sparse_vector("sparse", vec![17], vec![1.0])
                    .unwrap(),
            ])
            .unwrap();
        assert_eq!(
            index
                .search_sparse_named("sparse", vec![3], vec![1.0], 1)
                .unwrap()[0]
                .id
                .as_str(),
            "best"
        );
        index.flush().unwrap();
        drop(index);
        let mut reopened = BorsukIndex::open(&uri).unwrap();
        reopened
            .upsert(vec![
                VectorRecord::new("best", vec![0.0, 1.0])
                    .with_named_sparse_vector("sparse", vec![3], vec![4.0])
                    .unwrap(),
            ])
            .unwrap();
        reopened.delete(&["delete".to_string()]).unwrap();
        reopened.flush().unwrap();
        compact_all(&mut reopened, element_type.as_str());
        drop(reopened);
        let final_reopen = BorsukIndex::open(&uri).unwrap();
        let hits = final_reopen
            .search_sparse_named("sparse", vec![3], vec![1.0], 3)
            .unwrap();
        assert_eq!(hits[0].id.as_str(), "best");
        assert!(!hits.iter().any(|hit| hit.id.as_str() == "delete"));
    }
}

#[test]
fn sparse_and_late_interaction_reject_unsupported_scalar_types_at_create() {
    for element_type in [
        VectorElementType::BFloat16,
        VectorElementType::Float8E4M3Fn,
        VectorElementType::Float8E5M2,
        VectorElementType::Int8,
        VectorElementType::Binary,
    ] {
        for kind in [VectorKind::Sparse, VectorKind::LateInteraction] {
            let dir = tempfile::tempdir().unwrap();
            let mut config = base_config(
                dir.path().to_string_lossy().into_owned(),
                VectorMetric::SquaredEuclidean,
                2,
            );
            config.named_vectors.insert(
                "unsupported".to_string(),
                VectorSpec {
                    dimensions: 8,
                    metric: VectorMetric::InnerProduct,
                    kind,
                    element_type,
                },
            );
            let error = BorsukIndex::create(config).unwrap_err();
            assert!(
                error.to_string().contains("supports float32 or float16"),
                "{kind:?}/{element_type}: {error}"
            );
        }
    }
}

#[derive(Clone, Copy)]
struct HybridCase {
    name: &'static str,
    dense: bool,
    sparse: bool,
    text: bool,
}

const HYBRID_CASES: &[HybridCase] = &[
    HybridCase {
        name: "dense",
        dense: true,
        sparse: false,
        text: false,
    },
    HybridCase {
        name: "sparse",
        dense: false,
        sparse: true,
        text: false,
    },
    HybridCase {
        name: "bm25",
        dense: false,
        sparse: false,
        text: true,
    },
    HybridCase {
        name: "dense+sparse",
        dense: true,
        sparse: true,
        text: false,
    },
    HybridCase {
        name: "dense+bm25",
        dense: true,
        sparse: false,
        text: true,
    },
    HybridCase {
        name: "sparse+bm25",
        dense: false,
        sparse: true,
        text: true,
    },
    HybridCase {
        name: "dense+sparse+bm25",
        dense: true,
        sparse: true,
        text: true,
    },
];

fn hybrid_query(case: HybridCase) -> HybridQuery {
    let mut query = HybridQuery::new();
    if case.dense {
        query = query.with_vector("", vec![0.0, 0.0]);
    }
    if case.sparse {
        query = query.with_named_sparse_query("sparse", vec![7], vec![1.0]);
    }
    if case.text {
        query = query.with_text("needle");
    }
    query
}

#[test]
fn every_dense_sparse_bm25_combination_survives_flush_and_reopen() {
    for case in HYBRID_CASES {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut config = base_config(uri.clone(), VectorMetric::SquaredEuclidean, 2);
        config.text = case.text;
        if case.sparse {
            config.named_vectors.insert(
                "sparse".to_string(),
                VectorSpec {
                    dimensions: 32,
                    metric: VectorMetric::InnerProduct,
                    kind: VectorKind::Sparse,
                    element_type: VectorElementType::Float16,
                },
            );
        }
        let mut index = BorsukIndex::create(config).unwrap();
        let mut records = Vec::new();
        for (id, dense_x, sparse_score, term_count) in
            [("a", 0.0, 1.0, 1), ("b", 1.0, 4.0, 4), ("c", 2.0, 2.0, 2)]
        {
            let mut record = VectorRecord::new(id, vec![dense_x, 0.0]);
            if case.sparse {
                record = record
                    .with_named_sparse_vector("sparse", vec![7], vec![sparse_score])
                    .unwrap();
            }
            if case.text {
                record = record.with_text(
                    std::iter::repeat_n("needle", term_count)
                        .collect::<Vec<_>>()
                        .join(" "),
                );
            }
            records.push(record);
        }
        index.add(records).unwrap();
        let options = HybridOptions {
            k: 3,
            fusion: Fusion::Rrf { k: 60 },
            candidate_depth: 3,
            dense_options: SearchOptions::exact(3),
        };
        let before = index
            .search_hybrid(&hybrid_query(*case), options.clone())
            .unwrap_or_else(|error| panic!("{} before flush: {error}", case.name));
        assert!(
            !before.hits.is_empty(),
            "{} returned no WAL hits",
            case.name
        );
        index.flush().unwrap();
        drop(index);
        let mut reopened = BorsukIndex::open(&uri).unwrap();
        let after = reopened
            .search_hybrid(&hybrid_query(*case), options.clone())
            .unwrap_or_else(|error| panic!("{} after reopen: {error}", case.name));
        assert_eq!(
            after
                .hits
                .iter()
                .map(|hit| hit.id.as_bytes())
                .collect::<Vec<_>>(),
            before
                .hits
                .iter()
                .map(|hit| hit.id.as_bytes())
                .collect::<Vec<_>>(),
            "{} ranking changed after reopen",
            case.name
        );

        let mut replacement = VectorRecord::new("b", vec![0.1, 0.0]);
        if case.sparse {
            replacement = replacement
                .with_named_sparse_vector("sparse", vec![7], vec![5.0])
                .unwrap();
        }
        if case.text {
            replacement = replacement.with_text("needle needle needle needle needle");
        }
        reopened.upsert(vec![replacement]).unwrap();
        reopened.delete(&["c".to_string()]).unwrap();
        reopened.flush().unwrap();
        compact_all(&mut reopened, case.name);
        reopened
            .gc_obsolete_segments(GarbageCollectionOptions {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .unwrap();
        let after_mutation = reopened
            .search_hybrid(&hybrid_query(*case), options.clone())
            .unwrap();
        assert!(
            !after_mutation.hits.iter().any(|hit| hit.id.as_str() == "c"),
            "{} returned a deleted record",
            case.name
        );
        drop(reopened);

        let final_reopen = BorsukIndex::open(&uri).unwrap();
        let final_hits = final_reopen
            .search_hybrid(&hybrid_query(*case), options)
            .unwrap();
        assert_eq!(
            final_hits
                .hits
                .iter()
                .map(|hit| hit.id.as_bytes())
                .collect::<Vec<_>>(),
            after_mutation
                .hits
                .iter()
                .map(|hit| hit.id.as_bytes())
                .collect::<Vec<_>>(),
            "{} mutation ranking changed after final reopen",
            case.name
        );
    }
}

fn late_record(id: &str, primary_x: f32, tokens: &[[f32; 2]]) -> VectorRecord {
    VectorRecord::new(id, vec![primary_x, 0.0])
        .with_late_interaction(
            "tokens",
            tokens.iter().map(|token| token.to_vec()).collect(),
        )
        .unwrap()
}

#[test]
fn late_interaction_float32_and_float16_pass_the_mutable_persistent_lifecycle() {
    for element_type in LATE_INTERACTION_TYPES {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut config = base_config(uri.clone(), VectorMetric::SquaredEuclidean, 2);
        config.named_vectors.insert(
            "tokens".to_string(),
            VectorSpec {
                dimensions: 2,
                metric: VectorMetric::InnerProduct,
                kind: VectorKind::LateInteraction,
                element_type: *element_type,
            },
        );
        let mut index = BorsukIndex::create(config).unwrap();
        index
            .add(vec![
                late_record("best", 0.0, &[[1.0, 0.0], [0.0, 1.0]]),
                late_record("near", 1.0, &[[0.7, 0.0], [0.0, 0.7]]),
                late_record("delete", 2.0, &[[-1.0, 0.0], [0.0, -1.0]]),
            ])
            .unwrap();
        let query = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        assert_eq!(
            index
                .search_late_interaction("tokens", query.clone(), 1)
                .unwrap()[0]
                .id
                .as_str(),
            "best"
        );
        index.flush().unwrap();
        drop(index);
        let mut reopened = BorsukIndex::open(&uri).unwrap();
        reopened
            .upsert(vec![late_record("best", 3.0, &[[-0.5, 0.0], [0.0, -0.5]])])
            .unwrap();
        reopened.delete(&["delete".to_string()]).unwrap();
        reopened.flush().unwrap();
        compact_all(&mut reopened, element_type.as_str());
        drop(reopened);
        let final_reopen = BorsukIndex::open(&uri).unwrap();
        assert_eq!(
            final_reopen
                .search_late_interaction("tokens", query, 1)
                .unwrap()[0]
                .id
                .as_str(),
            "near"
        );
    }
}

#[test]
fn opaque_non_utf8_ids_survive_typed_storage_and_search() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let id = vec![0, 159, 255, 7];
    let mut index = BorsukIndex::create_with_build_config(
        base_config(uri.clone(), VectorMetric::SquaredEuclidean, 2),
        BuildConfig {
            vector_element_type: VectorElementType::Float8E4M3Fn,
            ..BuildConfig::default()
        },
    )
    .unwrap();
    index
        .add(vec![VectorRecord::new_bytes(id.clone(), vec![1.0, 0.3])])
        .unwrap();
    index.flush().unwrap();
    drop(index);
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.get_vector_by_id(&id).unwrap().is_some());
    assert_eq!(
        reopened
            .search_id_bytes(&[1.0, 0.3], SearchOptions::exact(1))
            .unwrap(),
        [id]
    );
}
