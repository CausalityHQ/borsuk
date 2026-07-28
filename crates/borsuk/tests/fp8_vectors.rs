#![allow(missing_docs)]

use borsuk::{
    BorsukIndex, BuildConfig, CompactionOptions, IndexConfig, LeafMode, SearchOptions,
    VectorElementType, VectorMetric, VectorRecord,
};

fn config(uri: String, dimensions: usize) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::SquaredEuclidean,
        dimensions,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    }
}

fn exercise_fp8_lifecycle(element_type: VectorElementType, overflow: f32, saturated: f32) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let source = vec![1.0, -2.0, 0.3, overflow];
    let canonical = element_type.canonicalize(&source).unwrap();
    assert_eq!(canonical[3], saturated);

    let mut index = BorsukIndex::create_with_build_config(
        config(uri.clone(), source.len()),
        BuildConfig {
            vector_element_type: element_type,
            ..BuildConfig::default()
        },
    )
    .unwrap();
    index
        .add(vec![
            VectorRecord::new("origin", vec![0.0; source.len()]),
            VectorRecord::new("typed", source.clone()),
            VectorRecord::new("far", vec![-4.0, 4.0, -4.0, 0.0]),
        ])
        .unwrap();

    assert_eq!(index.get_vector("typed").unwrap(), Some(canonical.clone()));
    let source_query = index
        .search_with_report(&source, SearchOptions::exact(1))
        .unwrap();
    assert_eq!(source_query.hits[0].id.to_string(), "typed");
    assert_eq!(
        source_query.hits[0].distance, 0.0,
        "queries must use the same declared-type canonicalization as stored vectors"
    );
    assert_eq!(
        index
            .search_ids(&canonical, SearchOptions::exact(1))
            .unwrap(),
        vec!["typed"]
    );

    index.flush().unwrap();
    assert_eq!(index.get_vector("typed").unwrap(), Some(canonical.clone()));
    drop(index);

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.build_config().vector_element_type, element_type);
    assert_eq!(
        reopened.get_vector("typed").unwrap(),
        Some(canonical.clone())
    );
    assert_eq!(
        reopened
            .search_ids(&canonical, SearchOptions::exact(1))
            .unwrap(),
        vec!["typed"]
    );

    let approx = reopened
        .search_with_report(&canonical, SearchOptions::approx(3, LeafMode::FlatScan))
        .unwrap();
    assert_eq!(approx.hits.len(), 3);
    assert!(
        approx
            .hits
            .windows(2)
            .all(|pair| pair[0].distance <= pair[1].distance)
    );

    let replacement_source = vec![2.0, 1.3, -0.7, overflow / 2.0];
    let replacement = element_type.canonicalize(&replacement_source).unwrap();
    reopened
        .upsert(vec![VectorRecord::new("typed", replacement_source.clone())])
        .unwrap();
    reopened.delete(&["origin".to_string()]).unwrap();
    assert_eq!(
        reopened.get_vector("typed").unwrap(),
        Some(replacement.clone())
    );
    assert!(reopened.get_vector("origin").unwrap().is_none());
    reopened.flush().unwrap();
    let compacted = reopened
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: None,
            min_segments: 1,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();
    assert!(compacted.compacted);
    drop(reopened);

    let final_reopen = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        final_reopen.get_vector("typed").unwrap(),
        Some(replacement.clone())
    );
    assert!(final_reopen.get_vector("origin").unwrap().is_none());
    assert_eq!(
        final_reopen
            .search_ids(&replacement_source, SearchOptions::exact(1))
            .unwrap(),
        vec!["typed"]
    );
}

#[test]
fn float8_e4m3fn_survives_wal_flush_search_and_reopen() {
    exercise_fp8_lifecycle(VectorElementType::Float8E4M3Fn, 1_000.0, 448.0);
}

#[test]
fn float8_e5m2_survives_wal_flush_search_and_reopen() {
    exercise_fp8_lifecycle(VectorElementType::Float8E5M2, 1.0e9, 57_344.0);
}

#[test]
fn fp8_rejects_non_finite_vectors_before_the_wal() {
    for element_type in [
        VectorElementType::Float8E4M3Fn,
        VectorElementType::Float8E5M2,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create_with_build_config(
            config(uri, 2),
            BuildConfig {
                vector_element_type: element_type,
                ..BuildConfig::default()
            },
        )
        .unwrap();

        let error = index
            .add(vec![VectorRecord::new("bad", vec![1.0, f32::NAN])])
            .unwrap_err();
        assert!(
            error.to_string().contains("finite"),
            "unexpected rejection for {element_type}: {error}"
        );
        assert!(index.get_vector("bad").unwrap().is_none());
    }
}
