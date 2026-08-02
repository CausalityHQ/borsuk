//! Process-local WAL group-commit integration coverage.

use std::sync::{Arc, Barrier};

use borsuk::{
    BorsukIndex, GroupCommitConfig, GroupCommitWriter, IndexConfig, VectorMetric, VectorRecord,
};

fn config(uri: &str) -> IndexConfig {
    IndexConfig {
        uri: uri.to_string(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1_000,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    }
}

#[test]
fn concurrent_appends_share_one_durable_wal_transaction() {
    const WRITERS: usize = 8;
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::from_millis(100),
            max_records: WRITERS,
        },
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(WRITERS));
    let handles = (0..WRITERS)
        .map(|ordinal| {
            let writer = writer.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                writer
                    .append(vec![VectorRecord::new(
                        format!("grouped-{ordinal}"),
                        vec![ordinal as f32, 0.0],
                    )])
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let mut commit_sequences = Vec::new();
    let mut request_totals = Vec::new();
    for handle in handles {
        let receipt = handle.join().unwrap();
        assert_eq!(receipt.records, 1);
        assert_eq!(receipt.committed_records, WRITERS);
        commit_sequences.push(receipt.commit_sequence);
        request_totals.push(receipt.requests.total());
    }
    assert!(commit_sequences.iter().all(|sequence| *sequence == 1));
    assert!(
        request_totals
            .iter()
            .all(|requests| *requests == request_totals[0])
    );
    assert!(request_totals[0] > 0);

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.list_records(0, WRITERS).unwrap().len(), WRITERS);
    assert_eq!(reopened.stats().wal_record_runs, 1);
}

#[test]
fn group_commit_configuration_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let error = match GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 0,
        },
    ) {
        Ok(_) => panic!("zero-sized groups must be rejected"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("max_records must be positive"));
}

#[test]
fn one_invalid_group_fails_every_joined_caller_without_partial_visibility() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::from_millis(100),
            max_records: 2,
        },
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let handles = (0..2)
        .map(|ordinal| {
            let writer = writer.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                writer.append(vec![VectorRecord::new(
                    "duplicate",
                    vec![ordinal as f32, 0.0],
                )])
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        assert!(handle.join().unwrap().is_err());
    }

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.list_records(0, 8).unwrap().is_empty());
}
