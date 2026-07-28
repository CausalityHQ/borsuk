#![allow(missing_docs)]

use borsuk::{
    BorsukIndex, BuildConfig, IndexConfig, LeafCapability, MetaValue, Metadata, PhysicalFormat,
    PhysicalLayoutContext, PhysicalLayoutPolicy, PhysicalLayoutRef, PhysicalObjectRole,
    SearchOptions, VectorElementType, VectorKind, VectorMetric, VectorRecord, VectorSpec,
    WalConfig, production_object_roles,
};

#[test]
fn production_policy_resolves_every_persisted_role() {
    let policy = PhysicalLayoutPolicy::production_default();
    policy.validate().unwrap();
    assert_eq!(policy, BuildConfig::default().physical_layout);
    for &role in production_object_roles() {
        assert_ne!(
            policy
                .resolve(role, PhysicalLayoutContext::default())
                .unwrap()
                .as_str(),
            ""
        );
    }
}

#[test]
fn production_default_does_not_require_a_collection_row_count_hint() {
    let policy = PhysicalLayoutPolicy::production_default();
    for context in [
        PhysicalLayoutContext {
            rows: 1,
            dimensions: 96,
            vector_element_type: Some(VectorElementType::Float32),
        },
        PhysicalLayoutContext {
            rows: 50_000,
            dimensions: 1_536,
            vector_element_type: Some(VectorElementType::Float32),
        },
    ] {
        assert_eq!(
            policy.resolve(PhysicalObjectRole::WalRun, context).unwrap(),
            PhysicalFormat::Parquet,
            "Parquet remains the automatic release default until the AWS candidate gate passes"
        );
    }
}

#[test]
fn wal_rules_can_require_rows_dimensions_and_primary_element_type() {
    let policy = PhysicalLayoutPolicy::production_baseline().with_vector_characteristics_rule(
        PhysicalObjectRole::WalRun,
        500,
        64,
        [
            VectorElementType::Float32,
            VectorElementType::Float16,
            VectorElementType::BFloat16,
            VectorElementType::Float8E4M3Fn,
            VectorElementType::Float8E5M2,
            VectorElementType::Binary,
        ],
        PhysicalFormat::Vortex,
    );
    let resolve = |rows, dimensions, vector_element_type| {
        policy
            .resolve(
                PhysicalObjectRole::WalRun,
                PhysicalLayoutContext {
                    rows,
                    dimensions,
                    vector_element_type: Some(vector_element_type),
                },
            )
            .unwrap()
    };

    assert_eq!(
        resolve(500, 96, VectorElementType::Float32),
        PhysicalFormat::Vortex
    );
    assert_eq!(
        resolve(499, 96, VectorElementType::Float32),
        PhysicalFormat::Parquet
    );
    assert_eq!(
        resolve(500, 32, VectorElementType::Float32),
        PhysicalFormat::Parquet
    );
    assert_eq!(
        resolve(500, 96, VectorElementType::Int8),
        PhysicalFormat::Parquet
    );
}

#[test]
fn wal_vortex_candidate_only_selects_types_with_a_local_latency_win() {
    let policy = PhysicalLayoutPolicy::production_baseline().with_wal_vortex_candidate();
    let resolve = |vector_element_type| {
        policy
            .resolve(
                PhysicalObjectRole::WalRun,
                PhysicalLayoutContext {
                    rows: 500,
                    dimensions: 96,
                    vector_element_type: Some(vector_element_type),
                },
            )
            .unwrap()
    };

    assert_eq!(resolve(VectorElementType::Float32), PhysicalFormat::Vortex);
    for element_type in [
        VectorElementType::Float16,
        VectorElementType::BFloat16,
        VectorElementType::Float8E4M3Fn,
        VectorElementType::Float8E5M2,
        VectorElementType::Int8,
        VectorElementType::Binary,
    ] {
        assert_eq!(resolve(element_type), PhysicalFormat::Parquet);
    }
}

#[test]
fn vector_characteristic_rules_reject_duplicate_or_ambiguous_selectors() {
    let duplicate = PhysicalLayoutPolicy::production_baseline().with_vector_characteristics_rule(
        PhysicalObjectRole::WalRun,
        500,
        64,
        [VectorElementType::Float32, VectorElementType::Float32],
        PhysicalFormat::Vortex,
    );
    assert!(
        duplicate
            .validate()
            .unwrap_err()
            .to_string()
            .contains("repeats a vector element type")
    );

    let ambiguous = PhysicalLayoutPolicy::production_baseline()
        .with_vector_characteristics_rule(
            PhysicalObjectRole::WalRun,
            500,
            64,
            [VectorElementType::Float32, VectorElementType::Float16],
            PhysicalFormat::Vortex,
        )
        .with_vector_characteristics_rule(
            PhysicalObjectRole::WalRun,
            500,
            64,
            [VectorElementType::Float16],
            PhysicalFormat::Parquet,
        );
    assert!(
        ambiguous
            .validate()
            .unwrap_err()
            .to_string()
            .contains("are ambiguous")
    );
}

#[test]
fn persisted_reference_rejects_the_wrong_reader_role_or_codec_family() {
    let reference = PhysicalLayoutRef {
        object_role: PhysicalObjectRole::ExactVectors,
        physical_format: PhysicalFormat::ArrowIpc,
        layout_policy_version: 7,
        integrity_chunk_bytes: 1,
        integrity_checksums: vec!["unused".to_string()],
    };
    assert!(
        reference
            .validate_for(PhysicalObjectRole::NormalSegment)
            .unwrap_err()
            .to_string()
            .contains("reader requires")
    );
    assert!(
        borsuk::DurableTableFormat::try_from(reference.physical_format)
            .unwrap_err()
            .to_string()
            .contains("cannot use")
    );
}

#[test]
fn policy_rejects_formats_without_a_real_role_writer_and_reader() {
    let unsupported = PhysicalLayoutPolicy::production_baseline()
        .with_role_format(PhysicalObjectRole::RoutingPage, PhysicalFormat::Vortex);
    assert!(
        unsupported
            .validate()
            .unwrap_err()
            .to_string()
            .contains("not implemented for object role `routing_page`")
    );
    let unsupported_rule = PhysicalLayoutPolicy::production_baseline().with_minimum_rows_rule(
        PhysicalObjectRole::ExactVectors,
        1,
        PhysicalFormat::Parquet,
    );
    assert!(
        unsupported_rule
            .validate()
            .unwrap_err()
            .to_string()
            .contains("not implemented for object role `exact_vectors`")
    );
}

#[test]
fn cell_wal_records_can_use_policy_selected_vortex_and_survive_reopen_and_flush() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let policy = PhysicalLayoutPolicy::production_baseline()
        .with_role_format(PhysicalObjectRole::WalRun, PhysicalFormat::Vortex);
    let wal = WalConfig {
        enabled: true,
        flush_threshold_runs: usize::MAX,
        flush_threshold_records: usize::MAX,
        flush_threshold_bytes: u64::MAX,
    };
    let mut index = BorsukIndex::create_with_wal_capability_and_build_config(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 4,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
        wal,
        LeafCapability::PqScanOnly,
        BuildConfig {
            physical_layout: policy,
            ..BuildConfig::default()
        },
    )
    .unwrap();
    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
        ])
        .unwrap();

    let vortex_runs = walk_files(dir.path())
        .into_iter()
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "vortex")
                && path.components().any(|part| part.as_os_str() == "records")
        })
        .collect::<Vec<_>>();
    assert_eq!(vortex_runs.len(), 1, "{vortex_runs:?}");
    assert_eq!(
        index
            .search_ids(&[0.0, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["a", "b"]
    );
    let wal_report = index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(2))
        .unwrap();
    assert_eq!(wal_report.wal_cells_examined, 1);
    assert_eq!(wal_report.wal_lanes_examined, 1);
    assert_eq!(wal_report.wal_runs_examined, 1);
    assert_eq!(wal_report.wal_records_examined, 2);
    assert_eq!(wal_report.wal_snapshot_retries, 0);
    drop(index);

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["a", "b"]
    );
    reopened.flush().unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["a", "b"]
    );
}

#[test]
fn adaptive_cell_wal_reopens_and_flushes_mixed_parquet_and_vortex_runs() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let policy = PhysicalLayoutPolicy::production_baseline().with_vector_characteristics_rule(
        PhysicalObjectRole::WalRun,
        3,
        64,
        [VectorElementType::Float32],
        PhysicalFormat::Vortex,
    );
    let wal = WalConfig {
        enabled: true,
        flush_threshold_runs: usize::MAX,
        flush_threshold_records: usize::MAX,
        flush_threshold_bytes: u64::MAX,
    };
    let mut index = BorsukIndex::create_with_wal_capability_and_build_config(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 64,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
        wal,
        LeafCapability::PqScanOnly,
        BuildConfig {
            physical_layout: policy,
            ..BuildConfig::default()
        },
    )
    .unwrap();
    index
        .add(vec![
            VectorRecord::new("p0", vector64(0.0)),
            VectorRecord::new("p1", vector64(1.0)),
        ])
        .unwrap();
    index
        .add(
            (0..4)
                .map(|row| VectorRecord::new(format!("v{row}"), vector64(10.0 + row as f32)))
                .collect(),
        )
        .unwrap();

    let record_run_extensions = walk_files(dir.path())
        .into_iter()
        .filter(|path| path.components().any(|part| part.as_os_str() == "records"))
        .filter_map(|path| {
            path.extension()
                .map(|extension| extension.to_string_lossy().into_owned())
        })
        .collect::<Vec<_>>();
    assert!(
        record_run_extensions.iter().any(|value| value == "parquet"),
        "{record_run_extensions:?}"
    );
    assert!(
        record_run_extensions.iter().any(|value| value == "vortex"),
        "{record_run_extensions:?}"
    );
    let stats = index.stats();
    assert_eq!(stats.wal_record_runs, 2);
    assert_eq!(stats.wal_parquet_record_runs, 1);
    assert_eq!(stats.wal_vortex_record_runs, 1);
    assert_eq!(
        stats.wal_record_bytes,
        stats
            .wal_parquet_record_bytes
            .saturating_add(stats.wal_vortex_record_bytes)
    );
    assert!(stats.wal_record_bytes > 0);

    drop(index);
    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened
            .search_ids(&vector64(0.0), SearchOptions::exact(1))
            .unwrap(),
        ["p0"]
    );
    assert_eq!(
        reopened
            .search_ids(&vector64(13.0), SearchOptions::exact(1))
            .unwrap(),
        ["v3"]
    );
    reopened.flush().unwrap();
    drop(reopened);

    let final_index = BorsukIndex::open(&uri).unwrap();
    assert_eq!(final_index.stats().records, 6);
    assert_eq!(
        final_index
            .search_ids(&vector64(12.0), SearchOptions::exact(1))
            .unwrap(),
        ["v2"]
    );
}

fn vector64(first: f32) -> Vec<f32> {
    let mut vector = vec![0.0; 64];
    vector[0] = first;
    vector
}

#[test]
fn vortex_cell_wal_is_end_to_end_for_every_primary_type_and_named_payload_kind() {
    for element_type in [
        VectorElementType::Float32,
        VectorElementType::Float16,
        VectorElementType::BFloat16,
        VectorElementType::Float8E4M3Fn,
        VectorElementType::Float8E5M2,
        VectorElementType::Int8,
        VectorElementType::Binary,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let policy = PhysicalLayoutPolicy::production_baseline()
            .with_role_format(PhysicalObjectRole::WalRun, PhysicalFormat::Vortex);
        let metric = if element_type == VectorElementType::Binary {
            VectorMetric::Hamming
        } else {
            VectorMetric::SquaredEuclidean
        };
        let mut named_vectors = std::collections::BTreeMap::new();
        named_vectors.insert(
            "dense".to_string(),
            VectorSpec {
                dimensions: 2,
                metric: VectorMetric::SquaredEuclidean,
                kind: VectorKind::Dense,
                element_type: VectorElementType::Float16,
            },
        );
        named_vectors.insert(
            "sparse".to_string(),
            VectorSpec {
                dimensions: 32,
                metric: VectorMetric::InnerProduct,
                kind: VectorKind::Sparse,
                element_type: VectorElementType::Float16,
            },
        );
        named_vectors.insert(
            "tokens".to_string(),
            VectorSpec {
                dimensions: 2,
                metric: VectorMetric::InnerProduct,
                kind: VectorKind::LateInteraction,
                element_type: VectorElementType::Float16,
            },
        );
        let wal = WalConfig {
            enabled: true,
            flush_threshold_runs: usize::MAX,
            flush_threshold_records: usize::MAX,
            flush_threshold_bytes: u64::MAX,
        };
        let mut index = BorsukIndex::create_with_wal_capability_and_build_config(
            IndexConfig {
                uri: uri.clone(),
                metric,
                dimensions: 4,
                segment_max_vectors: 16,
                ram_budget_bytes: None,
                text: true,
                named_vectors,
            },
            wal,
            LeafCapability::PqScanOnly,
            BuildConfig {
                vector_element_type: element_type,
                physical_layout: policy,
                ..BuildConfig::default()
            },
        )
        .unwrap_or_else(|error| panic!("{element_type}: create failed: {error}"));
        let best_primary = if element_type == VectorElementType::Binary {
            vec![1.0, 0.0, 1.0, 0.0]
        } else if element_type == VectorElementType::Int8 {
            vec![1.0, -2.0, 1.0, 4.0]
        } else {
            vec![1.0, -2.0, 0.5, 4.0]
        };
        let other_primary = if element_type == VectorElementType::Binary {
            vec![0.0, 1.0, 0.0, 1.0]
        } else {
            vec![-3.0, 2.0, -1.0, 0.0]
        };
        let best = VectorRecord::new("best", best_primary.clone())
            .with_named_vector("dense", vec![1.0, 0.0])
            .with_named_sparse_vector("sparse", vec![3, 17], vec![3.0, 2.0])
            .unwrap()
            .with_late_interaction("tokens", vec![vec![1.0, 0.0], vec![0.0, 1.0]])
            .unwrap()
            .with_text("needle needle")
            .with_metadata(Metadata::from([(
                "kind".to_string(),
                MetaValue::Str(element_type.to_string()),
            )]));
        let other = VectorRecord::new("other", other_primary)
            .with_named_vector("dense", vec![0.0, 1.0])
            .with_named_sparse_vector("sparse", vec![4], vec![1.0])
            .unwrap()
            .with_late_interaction("tokens", vec![vec![-1.0, 0.0], vec![0.0, -1.0]])
            .unwrap()
            .with_text("haystack");
        index
            .add(vec![best, other])
            .unwrap_or_else(|error| panic!("{element_type}: add failed: {error}"));
        assert_vortex_payloads(&index, element_type, &best_primary);
        drop(index);

        let mut reopened = BorsukIndex::open(&uri)
            .unwrap_or_else(|error| panic!("{element_type}: WAL reopen failed: {error}"));
        assert_vortex_payloads(&reopened, element_type, &best_primary);
        reopened
            .flush()
            .unwrap_or_else(|error| panic!("{element_type}: flush failed: {error}"));
        drop(reopened);

        let final_index = BorsukIndex::open(&uri)
            .unwrap_or_else(|error| panic!("{element_type}: segment reopen failed: {error}"));
        assert_vortex_payloads(&final_index, element_type, &best_primary);
    }
}

fn assert_vortex_payloads(
    index: &BorsukIndex,
    element_type: VectorElementType,
    primary_query: &[f32],
) {
    assert_eq!(
        index
            .search_ids(primary_query, SearchOptions::exact(1))
            .unwrap(),
        ["best"],
        "{element_type}: primary"
    );
    assert_eq!(
        index
            .search_ids(
                &[1.0, 0.0],
                SearchOptions::exact(1).with_vector_name("dense")
            )
            .unwrap(),
        ["best"],
        "{element_type}: named dense"
    );
    assert_eq!(
        index
            .search_sparse_named("sparse", vec![3], vec![1.0], 1)
            .unwrap()[0]
            .id
            .as_str(),
        "best",
        "{element_type}: named sparse"
    );
    assert_eq!(
        index
            .search_late_interaction("tokens", vec![vec![1.0, 0.0], vec![0.0, 1.0]], 1)
            .unwrap()[0]
            .id
            .as_str(),
        "best",
        "{element_type}: late interaction"
    );
    assert_eq!(
        index.search_text("needle", 1).unwrap().hits[0].id.as_str(),
        "best",
        "{element_type}: text"
    );
    let (_, metadata) = index.get_record("best").unwrap().unwrap();
    assert_eq!(
        metadata.get("kind"),
        Some(&MetaValue::Str(element_type.to_string())),
        "{element_type}: metadata"
    );
}

#[test]
fn one_index_reopens_mixed_parquet_vortex_and_arrow_objects() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let policy = PhysicalLayoutPolicy::production_baseline().with_minimum_rows_rule(
        PhysicalObjectRole::NormalSegment,
        4,
        PhysicalFormat::Vortex,
    );
    let mut index = BorsukIndex::create_with_wal_capability_and_build_config(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 4,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
        WalConfig::disabled(),
        LeafCapability::PqScanOnly,
        BuildConfig {
            physical_layout: policy,
            ..BuildConfig::default()
        },
    )
    .unwrap();
    index
        .add(
            (0..5)
                .map(|row| VectorRecord::new(format!("r{row}"), vec![row as f32, 0.0]))
                .collect(),
        )
        .unwrap();

    let formats = index
        .manifest()
        .segments
        .iter()
        .map(|segment| segment.layout.physical_format)
        .collect::<Vec<_>>();
    assert!(formats.contains(&PhysicalFormat::Parquet), "{formats:?}");
    assert!(formats.contains(&PhysicalFormat::Vortex), "{formats:?}");
    assert_eq!(
        index
            .build_config()
            .physical_layout
            .resolve(
                PhysicalObjectRole::ExactVectors,
                PhysicalLayoutContext {
                    rows: 4,
                    ..PhysicalLayoutContext::default()
                }
            )
            .unwrap(),
        PhysicalFormat::ArrowIpc
    );
    for segment in &index.manifest().segments {
        segment
            .layout
            .validate_for(PhysicalObjectRole::NormalSegment)
            .unwrap();
        assert_eq!(
            segment.layout.layout_policy_version,
            index.build_config().physical_layout.version
        );
        assert!(
            segment
                .path
                .ends_with(segment.layout.physical_format.extension())
        );
    }

    drop(index);
    let reopened = BorsukIndex::open(&uri).unwrap();
    for row in 0..5 {
        assert_eq!(
            reopened.get_vector(&format!("r{row}")).unwrap(),
            Some(vec![row as f32, 0.0])
        );
    }
    assert_eq!(
        reopened
            .search_ids(&[4.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["r4"]
    );
}

fn walk_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| entry.ok())
        .flat_map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                walk_files(&path)
            } else {
                vec![path]
            }
        })
        .collect()
}
