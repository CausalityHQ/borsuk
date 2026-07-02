#![allow(missing_docs)]

use borsuk::{StringMetric, VectorMetric, recall_at_k};
use std::str::FromStr;

#[test]
fn vector_metrics_cover_common_dense_and_set_like_distances() {
    let a = [1.0_f32, 2.0, 0.0, 4.0];
    let b = [1.0_f32, 4.0, 3.0, 0.0];

    assert_eq!(
        VectorMetric::SquaredEuclidean.distance(&a, &b).unwrap(),
        29.0
    );
    assert!((VectorMetric::Euclidean.distance(&a, &b).unwrap() - 29.0_f32.sqrt()).abs() < 1e-6);
    assert_eq!(VectorMetric::Manhattan.distance(&a, &b).unwrap(), 9.0);
    assert_eq!(VectorMetric::Chebyshev.distance(&a, &b).unwrap(), 4.0);
    assert_eq!(VectorMetric::Hamming.distance(&a, &b).unwrap(), 3.0);
    assert!((VectorMetric::Jaccard.distance(&a, &b).unwrap() - 0.5).abs() < 1e-6);
    assert!((VectorMetric::Cosine.distance(&a, &a).unwrap()).abs() < 1e-6);
}

#[test]
fn vector_metrics_cover_inner_product_angular_and_distribution_distances() {
    let a = [1.0_f32, 0.0, 3.0];
    let b = [0.0_f32, 2.0, 3.0];

    assert_eq!(VectorMetric::InnerProduct.distance(&a, &b).unwrap(), -9.0);
    assert!((VectorMetric::Angular.distance(&a, &a).unwrap()).abs() < 1e-6);
    assert!((VectorMetric::Lorentzian.distance(&a, &b).unwrap() - 1.7917595).abs() < 1e-6);
    assert!(
        (VectorMetric::Clark.distance(&a, &b).unwrap() - std::f32::consts::SQRT_2).abs() < 1e-6
    );
    assert!((VectorMetric::ChiSquare.distance(&a, &b).unwrap() - 3.0).abs() < 1e-6);
    assert!((VectorMetric::Hellinger.distance(&a, &b).unwrap() - 0.57374144).abs() < 1e-6);
}

#[test]
fn string_metrics_cover_edit_and_similarity_distances() {
    assert_eq!(StringMetric::Levenshtein.distance("borsuk", "borsuc"), 1.0);
    assert_eq!(
        StringMetric::DamerauLevenshtein.distance("abcd", "acbd"),
        1.0
    );
    assert_eq!(StringMetric::Hamming.distance("rust", "dust"), 1.0);

    let jaro_winkler = StringMetric::JaroWinkler.distance("segment", "segments");
    assert!(jaro_winkler > 0.0);
    assert!(jaro_winkler < 0.2);
}

#[test]
fn recall_at_k_measures_overlap_with_exact_top_k() {
    let exact = vec![
        "doc-a".to_string(),
        "doc-b".to_string(),
        "doc-c".to_string(),
        "doc-d".to_string(),
    ];
    let approximate = vec![
        "doc-c".to_string(),
        "doc-x".to_string(),
        "doc-a".to_string(),
        "doc-a".to_string(),
    ];

    assert_eq!(recall_at_k(&exact, &approximate, 3).unwrap(), 2.0 / 3.0);
    assert_eq!(recall_at_k(&exact, &approximate, 2).unwrap(), 0.0);
    assert!(
        recall_at_k(&exact, &approximate, 0)
            .unwrap_err()
            .to_string()
            .contains("k must be greater than zero")
    );
}

#[test]
fn metrics_reject_dimension_mismatch() {
    let err = VectorMetric::Euclidean
        .distance(&[1.0, 2.0], &[1.0])
        .unwrap_err();
    assert!(err.to_string().contains("dimension mismatch"));
}

#[test]
fn vector_metrics_parse_stable_api_names() {
    assert_eq!(
        VectorMetric::from_str("l2").unwrap(),
        VectorMetric::Euclidean
    );
    assert_eq!(
        VectorMetric::from_str("squared-euclidean").unwrap(),
        VectorMetric::SquaredEuclidean
    );
    assert_eq!(
        VectorMetric::from_str("cosine").unwrap(),
        VectorMetric::Cosine
    );
    assert_eq!(
        VectorMetric::from_str("l1").unwrap(),
        VectorMetric::Manhattan
    );
    assert_eq!(
        VectorMetric::from_str("minkowski:3").unwrap(),
        VectorMetric::Minkowski { p: 3.0 }
    );
    assert_eq!(
        VectorMetric::from_str("inner-product").unwrap(),
        VectorMetric::InnerProduct
    );
    assert_eq!(
        VectorMetric::from_str("angular").unwrap(),
        VectorMetric::Angular
    );
    assert_eq!(
        VectorMetric::from_str("hellinger").unwrap(),
        VectorMetric::Hellinger
    );
    assert_eq!(
        VectorMetric::from_str("chi-square").unwrap(),
        VectorMetric::ChiSquare
    );
    assert_eq!(
        VectorMetric::from_str("lorentzian").unwrap(),
        VectorMetric::Lorentzian
    );
    assert_eq!(
        VectorMetric::from_str("clark").unwrap(),
        VectorMetric::Clark
    );
    assert!(VectorMetric::from_str("not-a-metric").is_err());
}
