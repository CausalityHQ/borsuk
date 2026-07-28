#![allow(missing_docs)]

use borsuk::{
    BorsukIndex, BuildConfig, IndexConfig, LeafCapability, LeafMode, PhysicalFormat,
    PhysicalLayoutPolicy, PhysicalObjectRole, SearchOptions, VectorMetric, VectorRecord, WalConfig,
};

fn tail_dimension_vector(row: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dimension| {
            let distance = row.abs_diff(137) as f32;
            if dimension == dimensions - 1 {
                distance
            } else {
                ((row * 17 + dimension * 13) % 31) as f32 * 0.001
            }
        })
        .collect()
}

#[test]
fn range_aware_vortex_round_trips_tail_dimensions_and_rejects_corruption() {
    let directory = tempfile::tempdir().unwrap();
    let dimensions = 65;
    let uri = directory.path().to_string_lossy().into_owned();
    let policy = PhysicalLayoutPolicy::production_baseline()
        .with_role_format(PhysicalObjectRole::NormalSegment, PhysicalFormat::Vortex);
    let mut index = BorsukIndex::create_with_wal_capability_and_build_config(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions,
            segment_max_vectors: 257,
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
            (0..1_028)
                .map(|row| {
                    VectorRecord::new(format!("row-{row}"), tail_dimension_vector(row, dimensions))
                })
                .collect(),
        )
        .unwrap();
    let summary = index.manifest().segments[0].clone();
    assert_eq!(summary.layout.physical_format, PhysicalFormat::Vortex);
    assert!(!summary.layout.integrity_checksums.is_empty());
    drop(index);

    let reopened = BorsukIndex::open(&uri).unwrap();
    let report = reopened
        .search_with_report(
            &tail_dimension_vector(137, dimensions),
            SearchOptions::approx(8, LeafMode::PqScan),
        )
        .unwrap();
    assert_eq!(report.hits[0].id, "row-137");
    assert!(report.backing_reads > 1, "{report:?}");
    assert_eq!(
        report.bytes_read,
        report
            .disk_cache_bytes_read
            .saturating_add(report.backing_bytes_read),
        "logical byte accounting must not multiply overlapping segment scopes"
    );
    drop(reopened);

    let path = directory.path().join(&summary.path);
    let mut bytes = std::fs::read(&path).unwrap();
    let corrupt_at = bytes.len().min(4_096) - 1;
    bytes[corrupt_at] ^= 0x80;
    std::fs::write(&path, bytes).unwrap();

    let reopened = BorsukIndex::open(&uri).unwrap();
    let error = reopened
        .search_with_report(
            &tail_dimension_vector(137, dimensions),
            SearchOptions::approx(8, LeafMode::PqScan),
        )
        .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("checksum mismatch") || message.contains("range-chunk"),
        "{message}"
    );
}
