#![allow(missing_docs)]

//! Regression guard for a paged-index data-loss bug: deleting records once the
//! index has paged (segments live in routing pages, `manifest.segments` is
//! empty) must not wipe the index.
//!
//! Root cause (fixed): a tombstone-only publish rebuilt the routing pages from
//! `manifest.segments`, which is empty for a paged index, so `delete` published
//! an empty routing tree and lost every record. `publish_tombstone` now
//! re-publishes referencing the existing routing pages when the index has paged.
//!
//! The trigger needs deletes across a compaction: round 0's compaction pages the
//! index (moving segments into routing pages), then round 1's delete used the
//! segment-rebuild publish path and wiped it. `add`/`compact` without deletes
//! were unaffected.
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
