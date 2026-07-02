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
    assert_eq!(VectorMetric::Gower.distance(&a, &b).unwrap(), 2.25);
    assert_eq!(VectorMetric::Chebyshev.distance(&a, &b).unwrap(), 4.0);
    assert_eq!(VectorMetric::Hamming.distance(&a, &b).unwrap(), 3.0);
    assert!((VectorMetric::Jaccard.distance(&a, &b).unwrap() - 0.5).abs() < 1e-6);
    assert!((VectorMetric::Cosine.distance(&a, &a).unwrap()).abs() < 1e-6);
}

#[test]
fn vector_metrics_cover_binary_set_coefficients() {
    let a = [1.0_f32, 0.0, 1.0, 0.0];
    let b = [1.0_f32, 1.0, 0.0, 0.0];

    assert_eq!(VectorMetric::SimpleMatching.distance(&a, &b).unwrap(), 0.5);
    assert_eq!(VectorMetric::RussellRao.distance(&a, &b).unwrap(), 0.75);
    assert!((VectorMetric::RogersTanimoto.distance(&a, &b).unwrap() - 2.0 / 3.0).abs() < 1e-6);
    assert!((VectorMetric::SokalSneath.distance(&a, &b).unwrap() - 0.8).abs() < 1e-6);
    assert_eq!(VectorMetric::Yule.distance(&a, &b).unwrap(), 1.0);
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

    let p = [0.5_f32, 0.5];
    let q = [0.25_f32, 0.75];
    assert!((VectorMetric::KullbackLeibler.distance(&p, &q).unwrap() - 0.14384104).abs() < 1e-6);
    assert!((VectorMetric::Jeffreys.distance(&p, &q).unwrap() - 0.27465308).abs() < 1e-6);
    assert!((VectorMetric::JensenShannon.distance(&p, &q).unwrap() - 0.18390779).abs() < 1e-6);
    assert!((VectorMetric::Bhattacharyya.distance(&p, &q).unwrap() - 0.03466823).abs() < 1e-6);

    let left_histogram = [1.0_f32, 0.0, 0.0];
    let right_histogram = [0.0_f32, 0.0, 1.0];
    assert!(
        (VectorMetric::Wasserstein
            .distance(&left_histogram, &right_histogram)
            .unwrap()
            - 2.0)
            .abs()
            < 1e-6
    );
}

#[test]
fn vector_metrics_cover_time_series_distances() {
    let a = [0.0_f32, 0.0, 1.0, 1.0];
    let b = [0.0_f32, 1.0, 1.0, 1.0];

    assert!((VectorMetric::DynamicTimeWarping.distance(&a, &b).unwrap()).abs() < 1e-6);
}

#[test]
fn vector_metrics_cover_additional_histogram_distances() {
    let a = [1.0_f32, 2.0, 0.0];
    let b = [2.0_f32, 1.0, 3.0];

    assert!(
        (VectorMetric::from_str("ruzicka")
            .unwrap()
            .distance(&a, &b)
            .unwrap()
            - 5.0 / 7.0)
            .abs()
            < 1e-6
    );
    assert!(
        (VectorMetric::from_str("weighted-jaccard")
            .unwrap()
            .distance(&a, &b)
            .unwrap()
            - 5.0 / 7.0)
            .abs()
            < 1e-6
    );
    assert!(
        (VectorMetric::from_str("squared-chord")
            .unwrap()
            .distance(&[1.0, 4.0], &[4.0, 1.0])
            .unwrap()
            - 2.0)
            .abs()
            < 1e-6
    );
    assert!(
        (VectorMetric::from_str("wave-hedges")
            .unwrap()
            .distance(&a, &b)
            .unwrap()
            - 2.0)
            .abs()
            < 1e-6
    );

    assert!(
        VectorMetric::from_str("ruzicka")
            .unwrap()
            .distance(&[-1.0, 2.0], &[1.0, 2.0])
            .unwrap_err()
            .to_string()
            .contains("requires non-negative vectors")
    );
}

#[test]
fn string_metrics_cover_edit_and_similarity_distances() {
    assert_eq!(StringMetric::Levenshtein.distance("borsuk", "borsuc"), 1.0);
    assert_eq!(
        StringMetric::DamerauLevenshtein.distance("abcd", "acbd"),
        1.0
    );
    assert_eq!(
        StringMetric::OptimalStringAlignment.distance("abcd", "acbd"),
        1.0
    );
    assert_eq!(StringMetric::Hamming.distance("rust", "dust"), 1.0);
    assert!(
        (StringMetric::NormalizedLevenshtein.distance("kitten", "sitting") - 0.42857143).abs()
            < 1e-6
    );
    assert!(
        (StringMetric::NormalizedDamerauLevenshtein.distance("abcd", "acbd") - 0.25).abs() < 1e-6
    );
    assert!((StringMetric::SorensenDice.distance("night", "nacht") - 0.75).abs() < 1e-6);

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
fn metric_catalogs_expose_canonical_names() {
    let vector_names = VectorMetric::names();
    assert!(vector_names.contains(&"euclidean"));
    assert!(vector_names.contains(&"cosine"));
    assert!(vector_names.contains(&"gower"));
    assert!(vector_names.contains(&"jensen-shannon"));
    assert!(vector_names.contains(&"dynamic-time-warping"));
    assert!(vector_names.contains(&"clark"));
    assert!(!vector_names.contains(&"l2"));
    assert!(
        vector_names
            .iter()
            .all(|name| VectorMetric::from_str(name).is_ok())
    );

    let string_names = StringMetric::names();
    assert!(string_names.contains(&"levenshtein"));
    assert!(string_names.contains(&"normalized-levenshtein"));
    assert!(string_names.contains(&"jaro-winkler"));
    assert!(string_names.contains(&"sorensen-dice"));
    assert!(!string_names.contains(&"edit"));
    assert!(
        string_names
            .iter()
            .all(|name| StringMetric::from_str(name).is_ok())
    );
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
        VectorMetric::from_str("gower").unwrap(),
        VectorMetric::Gower
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
    assert_eq!(
        VectorMetric::from_str("kullback-leibler").unwrap(),
        VectorMetric::KullbackLeibler
    );
    assert_eq!(
        VectorMetric::from_str("jeffreys").unwrap(),
        VectorMetric::Jeffreys
    );
    assert_eq!(
        VectorMetric::from_str("jensen-shannon").unwrap(),
        VectorMetric::JensenShannon
    );
    assert_eq!(
        VectorMetric::from_str("bhattacharyya").unwrap(),
        VectorMetric::Bhattacharyya
    );
    assert_eq!(
        VectorMetric::from_str("earth-mover").unwrap(),
        VectorMetric::Wasserstein
    );
    assert_eq!(
        VectorMetric::from_str("dtw").unwrap(),
        VectorMetric::DynamicTimeWarping
    );
    assert_eq!(
        VectorMetric::from_str("simple-matching").unwrap(),
        VectorMetric::SimpleMatching
    );
    assert_eq!(
        VectorMetric::from_str("russell-rao").unwrap(),
        VectorMetric::RussellRao
    );
    assert_eq!(
        VectorMetric::from_str("rogers-tanimoto").unwrap(),
        VectorMetric::RogersTanimoto
    );
    assert_eq!(
        VectorMetric::from_str("sokal-sneath").unwrap(),
        VectorMetric::SokalSneath
    );
    assert_eq!(VectorMetric::from_str("yule").unwrap(), VectorMetric::Yule);
    assert!(VectorMetric::from_str("not-a-metric").is_err());
}
