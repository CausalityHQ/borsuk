#![allow(missing_docs)]

//! Regression repro for a PRE-EXISTING correctness bug discovered while building
//! the production workload benchmark (work.md #3).
//!
//! Symptom: after a delete + compaction, a *subsequent* round's compaction makes
//! a previously-committed, never-deleted record unreachable (data loss).
//!
//! Isolation: reproduces at commit 2e088b1 (before the versioned-upsert work),
//! using plain `add` + `delete` + `compact`, so it is independent of upserts and
//! the MVCC generation/tombstone changes. It requires deletes: `add` + `compact`
//! rounds without deletes are fine.
//!
//! Likely locus: `BorsukIndex::compact_from_routing_tree` (the paged compaction
//! path taken once a routing tree exists) drops the segments referenced by
//! routing-leaf pages that this compaction did not touch ("non-dirty" pages),
//! instead of preserving them. Round 0's compaction is the flat path (correct)
//! and builds the routing tree; round 1's compaction then takes the paged path
//! and loses the round-0 survivors.
//!
//! This test is `#[ignore]`d so the suite stays green; run it with `--ignored`
//! to reproduce. Un-ignore it when the bug is fixed.

use std::collections::BTreeMap;

use borsuk::{BorsukIndex, CompactionOptions, IndexConfig, VectorMetric, VectorRecord};

fn config(uri: String) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 8,
        ram_budget_bytes: None,
        text: false,
        named_vectors: BTreeMap::new(),
    }
}

#[test]
#[ignore = "known pre-existing bug: paged compaction after delete loses non-dirty-page segments"]
fn delete_then_compaction_must_not_lose_untouched_records() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();
    let mut live: BTreeMap<String, f32> = BTreeMap::new();

    for round in 0..6u64 {
        for j in 0..20u64 {
            let id = format!("id-{round}-{j}");
            let v = (round * 100 + j) as f32;
            index
                .add(vec![VectorRecord::new(&id, vec![v, 0.0])])
                .unwrap();
            live.insert(id, v);
        }
        let keys: Vec<String> = live.keys().cloned().collect();
        for d in 0..5usize {
            if keys.len() > d {
                let id = keys[(round as usize * 3 + d) % keys.len()].clone();
                if live.remove(&id).is_some() {
                    index.delete([id]).unwrap();
                }
            }
        }
        index.compact(CompactionOptions::default()).unwrap();

        for (id, v) in &live {
            let got = index.get_record(id).unwrap();
            assert!(
                got.is_some(),
                "round {round}: committed live id {id} was lost"
            );
            assert_eq!(got.unwrap().0, vec![*v, 0.0]);
        }
        let listed = index.list_records(0, 100000).unwrap();
        assert_eq!(
            listed.len(),
            live.len(),
            "round {round}: live record count drifted"
        );
    }
}
