//! V36 geometry construction work and admission contracts.

use borsuk::project_v36_exact_assignment_preflight;

#[test]
fn v36_geometry_preflight_projects_exact_assignment_work_before_corpus_execution() {
    // Break caught: a long geometry cell starts without first projecting the
    // exact global assignment work from a bounded measured kernel sample.
    let one_million = project_v36_exact_assignment_preflight(
        1_000_000, 192, 4_096, 1_024, 245, 1_000_000, 43_200,
    )
    .unwrap();
    assert_eq!(one_million.posting_count, 245);
    assert_eq!(one_million.distance_evaluations, 245_000_000);
    assert_eq!(one_million.component_terms, 47_040_000_000);
    assert_eq!(one_million.projected_active_ns, 976_562_500);
    assert!(one_million.within_active_wall_cap);

    let ten_million = project_v36_exact_assignment_preflight(
        10_000_000, 192, 4_096, 1_024, 245, 1_000_000, 43_200,
    )
    .unwrap();
    assert_eq!(ten_million.posting_count, 2_442);
    assert_eq!(ten_million.component_terms, 4_688_640_000_000);

    let hundred_million = project_v36_exact_assignment_preflight(
        100_000_000,
        192,
        4_096,
        1_024,
        245,
        1_000_000,
        43_200,
    )
    .unwrap();
    assert_eq!(hundred_million.posting_count, 24_415);
    assert_eq!(hundred_million.component_terms, 468_768_000_000_000);
    assert_eq!(hundred_million.projected_active_ns, 9_731_744_260_205);
    assert!(hundred_million.within_active_wall_cap);

    let b8192 =
        project_v36_exact_assignment_preflight(100_000_000, 192, 8_192, 1_024, 123, 1_000_000, 1)
            .unwrap();
    assert_eq!(b8192.posting_count, 12_208);
    assert_eq!(b8192.component_terms, 234_393_600_000_000);
    assert!(!b8192.within_active_wall_cap);
}

#[test]
fn v36_geometry_preflight_rejects_unbounded_or_ambiguous_measurements() {
    for request in [
        (0, 192, 4_096, 1_024, 245, 1_000_000, 43_200),
        (1_000_000, 0, 4_096, 1_024, 245, 1_000_000, 43_200),
        (1_000_000, 192, 0, 1_024, 245, 1_000_000, 43_200),
        (1_000_000, 192, 4_096, 0, 245, 1_000_000, 43_200),
        (1_000_000, 192, 4_096, 1_024, 0, 1_000_000, 43_200),
        (1_000_000, 192, 4_096, 1_024, 245, 0, 43_200),
        (1_000_000, 192, 4_096, 1_024, 245, 1_000_000, 0),
    ] {
        assert!(
            project_v36_exact_assignment_preflight(
                request.0, request.1, request.2, request.3, request.4, request.5, request.6,
            )
            .is_err()
        );
    }
    assert!(
        project_v36_exact_assignment_preflight(
            u64::MAX,
            u32::MAX,
            1,
            u64::MAX,
            u32::MAX,
            u64::MAX,
            u64::MAX,
        )
        .is_err()
    );
}
