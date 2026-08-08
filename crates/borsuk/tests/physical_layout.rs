#![allow(missing_docs)]

use std::str::FromStr;

use borsuk::{
    BorsukIndex, BuildConfig, IndexConfig, LeafCapability, PhysicalFormat, PhysicalLayoutPolicy,
    PhysicalLayoutRef, PhysicalObjectRole, SearchOptions, VectorMetric, VectorRecord, WalConfig,
    production_object_roles,
};

#[test]
fn production_policy_resolves_every_persisted_role_to_a_supported_standard_format() {
    let policy = PhysicalLayoutPolicy::production_default();
    policy.validate().unwrap();
    assert_eq!(policy, BuildConfig::default().physical_layout);

    for &role in production_object_roles() {
        let format = policy.resolve(role).unwrap();
        match role {
            PhysicalObjectRole::Catalog
            | PhysicalObjectRole::WalRun
            | PhysicalObjectRole::RoutingPage
            | PhysicalObjectRole::GraphIndex
            | PhysicalObjectRole::NormalSegment
            | PhysicalObjectRole::LexicalBlock
            | PhysicalObjectRole::Tombstone => assert_eq!(format, PhysicalFormat::Parquet),
            PhysicalObjectRole::ProductCodes
            | PhysicalObjectRole::ExactVectors
            | PhysicalObjectRole::LateInteraction => assert_eq!(format, PhysicalFormat::ArrowIpc),
            PhysicalObjectRole::LaneHead
            | PhysicalObjectRole::CommitMarker
            | PhysicalObjectRole::FilterIndex
            | PhysicalObjectRole::IdDirectory => assert_eq!(format, PhysicalFormat::Packed),
            PhysicalObjectRole::WriterDirectory | PhysicalObjectRole::Unknown => {
                panic!("unexpected production role: {role:?}")
            }
            _ => panic!("unexpected future production role: {role:?}"),
        }
    }
}

#[test]
fn removed_vortex_format_is_rejected() {
    let error = PhysicalFormat::from_str("vortex").unwrap_err();
    assert!(error.to_string().contains("unknown physical format"));
}

#[test]
fn stale_layout_policy_version_is_rejected() {
    let mut policy = PhysicalLayoutPolicy::production_default();
    policy.version -= 1;
    let error = policy.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported physical layout policy version")
    );
}

#[test]
fn persisted_reference_rejects_the_wrong_reader_role() {
    let reference = PhysicalLayoutRef {
        object_role: PhysicalObjectRole::ExactVectors,
        physical_format: PhysicalFormat::ArrowIpc,
        layout_policy_version: borsuk::CURRENT_LAYOUT_POLICY_VERSION,
    };
    assert!(
        reference
            .validate_for(PhysicalObjectRole::NormalSegment)
            .unwrap_err()
            .to_string()
            .contains("reader requires")
    );
}

#[test]
fn persisted_reference_rejects_a_format_not_supported_by_its_role() {
    let reference = PhysicalLayoutRef {
        object_role: PhysicalObjectRole::NormalSegment,
        physical_format: PhysicalFormat::ArrowIpc,
        layout_policy_version: borsuk::CURRENT_LAYOUT_POLICY_VERSION,
    };
    let error = reference
        .validate_for(PhysicalObjectRole::NormalSegment)
        .unwrap_err();
    assert!(error.to_string().contains("not implemented"));
}

#[test]
fn index_writes_parquet_segments_and_reopens_with_arrow_exact_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create_with_wal_capability_and_build_config(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 2,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
        WalConfig::disabled(),
        LeafCapability::PqScanOnly,
        BuildConfig::default(),
    )
    .unwrap();
    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
        ])
        .unwrap();

    for segment in &index.manifest().segments {
        assert_eq!(segment.layout.physical_format, PhysicalFormat::Parquet);
        assert!(segment.path.ends_with(".parquet"));
        assert!(segment.vector_size_bytes > 0);
    }

    drop(index);
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["a"]
    );
}
