#![allow(missing_docs)]

use borsuk::{
    BorsukError, BorsukIndex, CompactionOptions, IndexConfig, RecordId, UnicodeWordLowercase,
    VectorMetric, VectorRecord, term_frequencies,
};

fn index_config(uri: String, text: bool, segment_max_vectors: usize) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors,
        ram_budget_bytes: None,
        text,
        named_vectors: Default::default(),
    }
}

fn expected_terms(text: &str) -> Vec<(u32, u32)> {
    term_frequencies(&UnicodeWordLowercase, text)
        .into_iter()
        .collect()
}

#[test]
fn text_terms_round_trip_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri.clone(), true, 2)).unwrap();
    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]).with_text("Hello, WORLD! hello"),
            VectorRecord::new("b", vec![1.0, 0.0]).with_text("goodbye world goodbye"),
        ])
        .unwrap();
    drop(index);

    let reopened = BorsukIndex::open(&uri).unwrap();

    assert!(reopened.stats().text);
    assert_eq!(
        reopened.get_text_terms(&RecordId::from("a")).unwrap(),
        Some(expected_terms("Hello, WORLD! hello"))
    );
    assert_eq!(
        reopened.get_text_terms(&RecordId::from("b")).unwrap(),
        Some(expected_terms("goodbye world goodbye"))
    );
}

#[test]
fn text_records_are_rejected_when_index_is_not_text_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri, false, 2)).unwrap();
    let err = index
        .add(vec![
            VectorRecord::new("blocked", vec![0.0, 0.0]).with_text("not enabled"),
        ])
        .unwrap_err();

    assert!(
        matches!(err, BorsukError::InvalidMetricInput(ref message) if message.contains("text")),
        "{err:?}"
    );
}

#[test]
fn compaction_preserves_text_terms() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri, true, 1)).unwrap();
    index
        .add(
            (0..12)
                .map(|id| {
                    VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0])
                        .with_text(format!("topic{id} shared topic{id}"))
                })
                .collect(),
        )
        .unwrap();

    let compaction = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(12),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();
    assert!(compaction.compacted);

    for id in [0, 3, 7, 11] {
        let text = format!("topic{id} shared topic{id}");
        assert_eq!(
            index
                .get_text_terms(&RecordId::from(format!("v{id}")))
                .unwrap(),
            Some(expected_terms(&text))
        );
    }
}

#[test]
fn text_enabled_index_accepts_records_without_text() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri, true, 2)).unwrap();
    index
        .add(vec![
            VectorRecord::new("with-text", vec![0.0, 0.0]).with_text("alpha beta alpha"),
            VectorRecord::new("vector-only", vec![1.0, 0.0]),
        ])
        .unwrap();

    assert_eq!(
        index.get_text_terms(&RecordId::from("with-text")).unwrap(),
        Some(expected_terms("alpha beta alpha"))
    );
    assert_eq!(
        index
            .get_text_terms(&RecordId::from("vector-only"))
            .unwrap(),
        None
    );
}
