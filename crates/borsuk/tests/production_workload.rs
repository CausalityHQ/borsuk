#![allow(missing_docs)]

//! Production workload benchmark (work.md #3): not an ANN micro-benchmark but the
//! mix a real deployment runs — versioned upserts (inserts + overwrites),
//! deletes, metadata filtering, compaction, and process restarts — with the
//! index checked for correctness throughout and measured for latency,
//! throughput, storage, and object-store request counts.
//!
//! `production_workload_is_sound` is a fast correctness gate. The ignored
//! `production_workload_gate` runs a larger stream, prints a report, and writes a
//! CSV when `BORSUK_WORKLOAD_OUTPUT` is set.

use std::collections::HashMap;
use std::time::Instant;

use borsuk::{
    BorsukIndex, CompactionOptions, Filter, IndexConfig, MetaValue, Metadata, Op, SearchOptions,
    VectorMetric, VectorRecord,
};

const DIMENSIONS: usize = 8;
const BUCKETS: i64 = 8;
const CLUSTERS: u64 = 64;

fn config(uri: String) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: DIMENSIONS,
        segment_max_vectors: 64,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn vector_for(seed: u64, salt: u64) -> Vec<f32> {
    let cluster = splitmix64(seed) % CLUSTERS;
    let mut state = splitmix64(cluster ^ (salt << 32));
    (0..DIMENSIONS)
        .map(|_| {
            state = splitmix64(state);
            (cluster % 16) as f32 + (state >> 40) as f32 / f32::from(1u16 << 14)
        })
        .collect()
}

fn record(id: &str, seed: u64, salt: u64, bucket: i64) -> VectorRecord {
    let mut metadata = Metadata::new();
    metadata.insert("bucket".to_string(), MetaValue::Int(bucket));
    VectorRecord::new(id, vector_for(seed, salt)).with_metadata(metadata)
}

#[derive(Clone)]
struct Live {
    vector: Vec<f32>,
    bucket: i64,
}

fn run_round(
    index: &mut BorsukIndex,
    model: &mut HashMap<String, Live>,
    round: u64,
    batch: usize,
) -> (usize, usize) {
    // Upsert a batch of unique ids: half fresh, half overwrites of ids that
    // existed before this round.
    let existing: Vec<String> = model.keys().cloned().collect();
    let mut used = std::collections::HashSet::new();
    let mut records = Vec::with_capacity(batch);
    for j in 0..batch {
        let id = if j % 2 == 1 && !existing.is_empty() {
            let mut pick = None;
            for probe in 0..existing.len() {
                let idx =
                    (splitmix64(round * 131 + j as u64 + probe as u64) as usize) % existing.len();
                if !used.contains(&existing[idx]) {
                    pick = Some(existing[idx].clone());
                    break;
                }
            }
            pick.unwrap_or_else(|| format!("id-{round}-{j}"))
        } else {
            format!("id-{round}-{j}")
        };
        if !used.insert(id.clone()) {
            continue;
        }
        let seed = splitmix64(id.bytes().fold(1469u64, |a, b| {
            a.wrapping_mul(31).wrapping_add(u64::from(b))
        }));
        let salt = round * 1000 + j as u64;
        let bucket = (splitmix64(salt) % BUCKETS as u64) as i64;
        records.push(record(&id, seed, salt, bucket));
        model.insert(
            id,
            Live {
                vector: vector_for(seed, salt),
                bucket,
            },
        );
    }
    let upserts = records.len();
    index.upsert(records).unwrap();

    // Delete a few existing ids.
    let mut deletes = 0;
    if model.len() > batch {
        let victims: Vec<String> = (0..batch / 4)
            .filter_map(|d| {
                let idx = (splitmix64(round * 977 + d as u64) as usize) % model.len();
                model.keys().nth(idx).cloned()
            })
            .collect();
        for id in victims {
            if model.remove(&id).is_some() {
                index.delete([id]).unwrap();
                deletes += 1;
            }
        }
    }
    (upserts, deletes)
}

fn assert_matches_model(index: &BorsukIndex, model: &HashMap<String, Live>) {
    for (id, live) in model {
        let got = index
            .get_record(id)
            .unwrap()
            .unwrap_or_else(|| panic!("live id {id} missing"));
        assert_eq!(&got.0, &live.vector, "id {id} has a stale vector");
    }
    let listed = index.list_records(0, 1_000_000).unwrap();
    assert_eq!(listed.len(), model.len(), "live record count drifted");
    let mut seen = std::collections::HashSet::new();
    for (id, _, _) in &listed {
        assert!(seen.insert(id.to_string()), "duplicate live id {id}");
        assert!(
            model.contains_key(&id.to_string()),
            "unexpected live id {id}"
        );
    }
}

fn assert_filter_is_exact(index: &BorsukIndex, model: &HashMap<String, Live>, bucket: i64) {
    let query = vector_for(splitmix64(bucket as u64), 0);
    let hits = index
        .search_ids(
            &query,
            SearchOptions::exact(model.len().max(1)).with_filter(Filter::Cmp {
                path: "bucket".to_string(),
                op: Op::Eq,
                value: MetaValue::Int(bucket),
            }),
        )
        .unwrap();
    for id in &hits {
        assert_eq!(model.get(id).map(|live| live.bucket), Some(bucket));
    }
    let expected = model.values().filter(|live| live.bucket == bucket).count();
    assert_eq!(
        hits.len(),
        expected,
        "bucket {bucket} filter missed records"
    );
}

#[test]
fn production_workload_is_sound() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri.clone())).unwrap();
    let mut model: HashMap<String, Live> = HashMap::new();

    for round in 0..12u64 {
        run_round(&mut index, &mut model, round, 24);
        if round % 4 == 3 {
            index.compact(CompactionOptions::default()).unwrap();
        }
        if round % 6 == 5 {
            drop(index);
            index = BorsukIndex::open(&uri).unwrap();
        }
    }

    assert_matches_model(&index, &model);
    for bucket in 0..BUCKETS {
        assert_filter_is_exact(&index, &model, bucket);
    }

    drop(index);
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_matches_model(&reopened, &model);
}

#[test]
#[ignore = "benchmark; run with --ignored and optionally BORSUK_WORKLOAD_OUTPUT"]
fn production_workload_gate() {
    const ROUNDS: u64 = 40;
    const BATCH: usize = 200;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri.clone())).unwrap();
    let mut model: HashMap<String, Live> = HashMap::new();

    let mut upserts = 0usize;
    let mut deletes = 0usize;
    let write_start = Instant::now();
    for round in 0..ROUNDS {
        let (u, d) = run_round(&mut index, &mut model, round, BATCH);
        upserts += u;
        deletes += d;
        if round % 5 == 4 {
            index.compact(CompactionOptions::default()).unwrap();
        }
    }
    let write_secs = write_start.elapsed().as_secs_f64();

    drop(index);
    let index = BorsukIndex::open(&uri).unwrap();

    let mut latencies = Vec::new();
    let mut total_bytes = 0u64;
    let mut total_gets = 0u64;
    for q in 0..200u64 {
        let bucket = (q % BUCKETS as u64) as i64;
        let query = vector_for(splitmix64(q ^ 0xBEEF), 0);
        let options = SearchOptions::exact(10).with_filter(Filter::Cmp {
            path: "bucket".to_string(),
            op: Op::Eq,
            value: MetaValue::Int(bucket),
        });
        let start = Instant::now();
        let report = index.search_with_report(&query, options).unwrap();
        latencies.push(start.elapsed().as_secs_f64() * 1e3);
        total_bytes += report.bytes_read;
        total_gets += report.requests.gets;
    }
    latencies.sort_by(|a, b| a.total_cmp(b));
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[latencies.len() * 95 / 100];
    let stats = index.stats();

    println!("\nProduction workload ({ROUNDS} rounds x {BATCH} batch):");
    println!("  live records:        {}", model.len());
    println!("  upserts applied:     {upserts}");
    println!("  deletes applied:     {deletes}");
    println!(
        "  write throughput:    {:.0} ops/s",
        (upserts + deletes) as f64 / write_secs
    );
    println!("  filtered search p50: {p50:.3} ms");
    println!("  filtered search p95: {p95:.3} ms");
    println!(
        "  avg bytes/query:     {:.0}",
        total_bytes as f64 / latencies.len() as f64
    );
    println!(
        "  avg GET/query:       {:.2}",
        total_gets as f64 / latencies.len() as f64
    );
    println!("  segment storage:     {} bytes", stats.segment_bytes);

    if let Ok(path) = std::env::var("BORSUK_WORKLOAD_OUTPUT") {
        let csv = format!(
            "rounds,batch,live_records,upserts,deletes,write_ops_per_s,search_p50_ms,search_p95_ms,avg_bytes_per_query,avg_gets_per_query,segment_bytes\n\
             {ROUNDS},{BATCH},{},{upserts},{deletes},{:.1},{p50:.3},{p95:.3},{:.0},{:.2},{}\n",
            model.len(),
            (upserts + deletes) as f64 / write_secs,
            total_bytes as f64 / latencies.len() as f64,
            total_gets as f64 / latencies.len() as f64,
            stats.segment_bytes,
        );
        std::fs::write(&path, csv).expect("write workload csv");
        println!("\nwrote {path}");
    }
}
