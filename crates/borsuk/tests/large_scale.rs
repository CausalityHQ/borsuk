#![allow(missing_docs)]

use std::{
    env, fs,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    },
    thread,
    time::{Duration, Instant},
};

use borsuk::{
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, LeafMode, OpenOptions,
    SearchHit, SearchOptions, SearchReport, VectorMetric, recall_at_k, tie_aware_recall_at_k,
};
use memory_stats::memory_stats;

const DEFAULT_RECORDS: usize = 1_000_000;
const DEFAULT_DIMENSIONS: usize = 16;
const DEFAULT_SEGMENT_MAX_VECTORS: usize = 128;
const DEFAULT_BATCH_RECORDS: usize = 8_192;
const DEFAULT_MAX_SEGMENTS: usize = 512;
const DEFAULT_ROUTING_PAGE_OVERFETCH: usize = 8;
const DEFAULT_MAX_CANDIDATES_PER_SEGMENT: usize = 128;
const DEFAULT_MIN_TIE_AWARE_RECALL: f32 = 0.95;
const DEFAULT_MAX_RESIDENT_BYTES: u64 = 128 * 1024 * 1024;

#[test]
fn tie_aware_recall_counts_equal_distance_large_scale_hits() {
    let exact = (0..10)
        .map(|idx| SearchHit {
            id: format!("exact-{idx}").into(),
            distance: 0.0,
        })
        .collect::<Vec<_>>();
    let actual = (0..10)
        .map(|idx| SearchHit {
            id: format!("equivalent-{idx}").into(),
            distance: 0.0,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        tie_aware_recall_at_k(&hit_distances(&exact), &hit_distances(&actual), 10).unwrap(),
        1.0
    );
}

#[test]
fn large_scale_csv_includes_release_gate_metrics() {
    let run = LargeScaleRunSummary {
        records: 1_000_000,
        dimensions: 16,
        segment_max_vectors: 128,
        max_segments: 512,
        routing_page_overfetch: 8,
        max_candidates_per_segment: 128,
        pre_segments: 7_813,
        post_segments: 7_813,
        ingest_ms: 142_000,
        compaction_ms: 93_200,
        exact_ms: 6_890,
        compaction_bytes_read: 14_460_000,
        compaction_bytes_written: 18_880_000,
        gc_ms: 1_500,
        gc_objects_scanned: 15_800,
        gc_objects_deleted: 7_900,
        gc_bytes_reclaimed: 120_000_000,
    };
    let queries = vec![LargeScaleQuerySummary {
        mode: "pq-scan".to_string(),
        tie_aware_recall_at_10: 1.0,
        id_recall_at_10: 1.0,
        termination_reason: "max-segments".to_string(),
        query_ms: 22,
        segments_searched: 512,
        bytes_read: 14_460_000,
        graph_bytes_read: 0,
        routing_page_indexes_read: 1,
        routing_pages_read: 8,
        resident_bytes: 61_000,
        rss_before: Some(1_000_000),
        rss_peak: Some(1_250_000),
        rss_after: Some(1_100_000),
        records_considered: 65_536,
        records_scored: 65_536,
        graph_candidates_added: 0,
    }];

    let csv = large_scale_csv(&run, &queries);

    assert!(csv.starts_with("records,dimensions,segment_max_vectors,max_segments,routing_page_overfetch,max_candidates_per_segment,pre_segments,post_segments,ingest_ms,compaction_ms,exact_ms,compaction_bytes_read,compaction_bytes_written,gc_ms,gc_objects_scanned,gc_objects_deleted,gc_bytes_reclaimed,mode,tie_aware_recall_at_10,id_recall_at_10,termination_reason,query_ms,segments_searched,bytes_read,graph_bytes_read,routing_page_indexes_read,routing_pages_read,resident_bytes,rss_before,rss_peak,rss_after,rss_peak_delta,records_considered,records_scored,graph_candidates_added\n"));
    assert!(csv.contains("\n1000000,16,128,512,8,128,7813,7813,142000,93200,6890,14460000,18880000,1500,15800,7900,120000000,pq-scan,1.000000,1.000000,max-segments,22,512,14460000,0,1,8,61000,1000000,1250000,1100000,250000,65536,65536,0\n"));
}

#[test]
#[ignore = "heavy release gate; run explicitly for million-vector scale coverage"]
fn million_vector_local_search_scale_gate() {
    let record_count = env_usize("BORSUK_LARGE_SCALE_RECORDS", DEFAULT_RECORDS);
    assert!(
        record_count >= DEFAULT_RECORDS,
        "large-scale gate must run at least {DEFAULT_RECORDS} vectors; got {record_count}"
    );
    let dimensions = env_usize("BORSUK_LARGE_SCALE_DIMENSIONS", DEFAULT_DIMENSIONS);
    let segment_max_vectors = env_usize(
        "BORSUK_LARGE_SCALE_SEGMENT_MAX_VECTORS",
        DEFAULT_SEGMENT_MAX_VECTORS,
    );
    let batch_records = env_usize("BORSUK_LARGE_SCALE_BATCH_RECORDS", DEFAULT_BATCH_RECORDS);
    let max_segments = env_usize("BORSUK_LARGE_SCALE_MAX_SEGMENTS", DEFAULT_MAX_SEGMENTS);
    let routing_page_overfetch = env_usize(
        "BORSUK_LARGE_SCALE_ROUTING_PAGE_OVERFETCH",
        DEFAULT_ROUTING_PAGE_OVERFETCH,
    );
    let max_candidates_per_segment = env_usize(
        "BORSUK_LARGE_SCALE_MAX_CANDIDATES_PER_SEGMENT",
        DEFAULT_MAX_CANDIDATES_PER_SEGMENT,
    );
    let min_tie_aware_recall = env_f32(
        "BORSUK_LARGE_SCALE_MIN_TIE_AWARE_RECALL",
        DEFAULT_MIN_TIE_AWARE_RECALL,
    );
    let max_resident_bytes = env_u64(
        "BORSUK_LARGE_SCALE_MAX_RESIDENT_BYTES",
        DEFAULT_MAX_RESIDENT_BYTES,
    );

    // Default to a local tempdir, but allow pointing the gate at an
    // object-store URI (e.g. s3://bucket/prefix on SeaweedFS/MinIO/AWS) via
    // BORSUK_LARGE_SCALE_URI so the same gate measures the network read/write
    // path. The tempdir guard stays alive either way; it is simply unused when
    // an override URI is provided.
    let dir = tempfile::tempdir().unwrap();
    let uri = match env::var("BORSUK_LARGE_SCALE_URI") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => dir.path().to_string_lossy().into_owned(),
    };
    let over_object_store = uri.starts_with("s3://")
        || uri.starts_with("gs://")
        || uri.starts_with("az://")
        || uri.starts_with("azure://");
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions,
        segment_max_vectors,
        ram_budget_bytes: None,
    })
    .unwrap();

    let ingest_started = Instant::now();
    let mut inserted = 0_usize;
    while inserted < record_count {
        let end = inserted.saturating_add(batch_records).min(record_count);
        let vectors = (inserted..end)
            .map(|seed| deterministic_vector(seed, dimensions))
            .collect::<Vec<_>>();
        let ids = index.add_vectors(vectors).unwrap();
        assert_eq!(ids.len(), end - inserted);
        inserted = end;
    }
    let ingest_ms = ingest_started.elapsed().as_millis();

    let stats = index.stats();
    assert_eq!(stats.records, record_count);
    assert_eq!(stats.dimensions, dimensions);
    assert!(stats.segments > 1);

    let compaction_started = Instant::now();
    let compaction = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: None,
            min_segments: 1,
            target_segment_max_vectors: Some(segment_max_vectors),
        })
        .unwrap();
    assert!(compaction.compacted);
    assert_eq!(compaction.segments_read, stats.segments);
    assert_eq!(compaction.records_rewritten, record_count);
    assert_eq!(compaction.graph_payloads_read, 0);
    assert_eq!(compaction.graph_bytes_read, 0);
    let compaction_ms = compaction_started.elapsed().as_millis();

    let compacted_stats = index.stats();
    assert_eq!(compacted_stats.records, record_count);
    assert!(compacted_stats.resident_bytes_estimate <= max_resident_bytes);

    // The gate is single-process and quiescent, so an immediate delete-mode GC is
    // safe and reclaims the L0 segments obsoleted by the compaction above.
    let gc_started = Instant::now();
    let gc = index
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();
    let gc_ms = gc_started.elapsed().as_millis();
    assert!(!gc.dry_run);
    assert!(gc.objects_scanned > 0);
    assert!(
        gc.objects_deleted > 0,
        "compaction must leave obsolete L0 segments for GC to reclaim"
    );
    assert!(gc.bytes_reclaimed > 0);

    let query = deterministic_vector(42, dimensions);
    let exact_started = Instant::now();
    let exact = index
        .search_with_report(&query, SearchOptions::exact(10))
        .unwrap();
    assert_eq!(exact.hits.first().map(|hit| hit.id.as_str()), Some("42"));
    assert_eq!(exact.graph_bytes_read, 0);
    assert!(exact.resident_bytes_estimate <= max_resident_bytes);
    let exact_ms = exact_started.elapsed().as_millis();

    let graph_can_reduce_candidates = max_candidates_per_segment < segment_max_vectors;
    let modes = [
        (LeafMode::PqScan, false),
        (LeafMode::VamanaPq, graph_can_reduce_candidates),
        (LeafMode::Hybrid, graph_can_reduce_candidates),
    ];
    let mut query_summaries = Vec::new();
    for (leaf_mode, expect_graph_reads) in modes {
        let rss_before = current_rss_bytes();
        let peak_rss = Arc::new(AtomicU64::new(rss_before.unwrap_or(0)));
        let running = Arc::new(AtomicBool::new(true));
        let sampler_running = Arc::clone(&running);
        let sampler_peak = Arc::clone(&peak_rss);
        let sampler = thread::spawn(move || {
            while sampler_running.load(AtomicOrdering::Relaxed) {
                if let Some(rss) = current_rss_bytes() {
                    update_peak(&sampler_peak, rss);
                }
                thread::sleep(Duration::from_millis(2));
            }
            if let Some(rss) = current_rss_bytes() {
                update_peak(&sampler_peak, rss);
            }
        });

        let approx_started = Instant::now();
        let approx = index
            .search_with_report(
                &query,
                SearchOptions::approx(10, leaf_mode)
                    .with_max_segments(max_segments)
                    .with_routing_page_overfetch(routing_page_overfetch)
                    .with_max_candidates_per_segment(max_candidates_per_segment),
            )
            .unwrap();
        let query_ms = approx_started.elapsed().as_millis();
        running.store(false, AtomicOrdering::Relaxed);
        sampler
            .join()
            .expect("large-scale memory sampler should not panic");
        let rss_after = current_rss_bytes();
        if let Some(rss) = rss_after {
            update_peak(&peak_rss, rss);
        }
        let rss_peak = match peak_rss.load(AtomicOrdering::Relaxed) {
            0 => None,
            value => Some(value),
        };
        assert_high_recall_report(
            &exact.hits,
            &approx,
            min_tie_aware_recall,
            max_segments,
            max_resident_bytes,
            expect_graph_reads,
        );
        let tie_aware_recall_at_10 = tie_aware_recall_at_k(
            &hit_distances(&exact.hits),
            &hit_distances(&approx.hits),
            10,
        )
        .unwrap();
        let id_recall_at_10 =
            recall_at_k(&hit_ids(&exact.hits), &hit_ids(&approx.hits), 10).expect("id recall");

        eprintln!(
            "large_scale_query mode={} tie_recall={:.3} id_recall={:.3} query_ms={} segments={} bytes={} graph_bytes={} routing_indexes={} routing_pages={} resident_bytes={}",
            approx.leaf_mode,
            tie_aware_recall_at_10,
            id_recall_at_10,
            query_ms,
            approx.segments_searched,
            approx.bytes_read,
            approx.graph_bytes_read,
            approx.routing_page_indexes_read,
            approx.routing_pages_read,
            approx.resident_bytes_estimate,
        );
        query_summaries.push(LargeScaleQuerySummary {
            mode: approx.leaf_mode.clone(),
            tie_aware_recall_at_10,
            id_recall_at_10,
            termination_reason: approx.termination_reason.to_string(),
            query_ms,
            segments_searched: approx.segments_searched,
            bytes_read: approx.bytes_read,
            graph_bytes_read: approx.graph_bytes_read,
            routing_page_indexes_read: approx.routing_page_indexes_read,
            routing_pages_read: approx.routing_pages_read,
            resident_bytes: approx.resident_bytes_estimate,
            rss_before,
            rss_peak,
            rss_after,
            records_considered: approx.records_considered,
            records_scored: approx.records_scored,
            graph_candidates_added: approx.graph_candidates_added,
        });
    }

    let run_summary = LargeScaleRunSummary {
        records: stats.records,
        dimensions: stats.dimensions,
        segment_max_vectors,
        max_segments,
        routing_page_overfetch,
        max_candidates_per_segment,
        pre_segments: stats.segments,
        post_segments: compacted_stats.segments,
        ingest_ms,
        compaction_ms,
        exact_ms,
        compaction_bytes_read: compaction.bytes_read,
        compaction_bytes_written: compaction.bytes_written,
        gc_ms,
        gc_objects_scanned: gc.objects_scanned,
        gc_objects_deleted: gc.objects_deleted,
        gc_bytes_reclaimed: gc.bytes_reclaimed,
    };

    eprintln!(
        "large_scale_gc gc_ms={} objects_scanned={} objects_deleted={} routing_objects_deleted={} tables_deleted={} bytes_reclaimed={}",
        run_summary.gc_ms,
        gc.objects_scanned,
        gc.objects_deleted,
        gc.routing_objects_deleted,
        gc.tables_deleted,
        gc.bytes_reclaimed,
    );
    eprintln!(
        "large_scale records={} dimensions={} pre_segments={} post_segments={} ingest_ms={} compaction_ms={} exact_ms={} compaction_bytes_read={} compaction_bytes_written={} resident_bytes={}",
        run_summary.records,
        run_summary.dimensions,
        run_summary.pre_segments,
        run_summary.post_segments,
        run_summary.ingest_ms,
        run_summary.compaction_ms,
        run_summary.exact_ms,
        run_summary.compaction_bytes_read,
        run_summary.compaction_bytes_written,
        exact.resident_bytes_estimate,
    );
    eprintln!(
        "large_scale backend={} gc_ms={} ingest_ms={} compaction_ms={} exact_ms={}",
        if over_object_store {
            "object-store"
        } else {
            "local-fs"
        },
        run_summary.gc_ms,
        run_summary.ingest_ms,
        run_summary.compaction_ms,
        run_summary.exact_ms,
    );

    if let Ok(output_path) = env::var("BORSUK_LARGE_SCALE_OUTPUT") {
        write_large_scale_csv(Path::new(&output_path), &run_summary, &query_summaries).unwrap();
    }
}

#[test]
#[ignore = "heavy release gate; run explicitly for parallel query headroom coverage"]
fn parallel_search_headroom_reports_rss_peak_against_budget() {
    let record_count = env_usize("BORSUK_LARGE_SCALE_RECORDS", DEFAULT_RECORDS);
    let dimensions = env_usize("BORSUK_LARGE_SCALE_DIMENSIONS", DEFAULT_DIMENSIONS);
    let segment_max_vectors = env_usize(
        "BORSUK_LARGE_SCALE_SEGMENT_MAX_VECTORS",
        DEFAULT_SEGMENT_MAX_VECTORS,
    );
    let batch_records = env_usize("BORSUK_LARGE_SCALE_BATCH_RECORDS", DEFAULT_BATCH_RECORDS);
    let headroom_margin = env_u64(
        "BORSUK_LARGE_SCALE_HEADROOM_MARGIN_BYTES",
        DEFAULT_MAX_RESIDENT_BYTES,
    );

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions,
        segment_max_vectors,
        ram_budget_bytes: None,
    })
    .unwrap();

    let mut inserted = 0_usize;
    while inserted < record_count {
        let end = inserted.saturating_add(batch_records).min(record_count);
        let vectors = (inserted..end)
            .map(|seed| deterministic_vector(seed, dimensions))
            .collect::<Vec<_>>();
        let ids = index.add_vectors(vectors).unwrap();
        assert_eq!(ids.len(), end - inserted);
        inserted = end;
    }

    let pre_compaction_segments = index.stats().segments;
    let compaction = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: None,
            min_segments: 1,
            target_segment_max_vectors: Some(segment_max_vectors),
        })
        .unwrap();
    assert!(compaction.compacted);
    assert_eq!(compaction.records_rewritten, record_count);
    drop(index);

    let resident_estimate = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            resident_routing: false,
            ..OpenOptions::default()
        },
    )
    .unwrap()
    .stats()
    .resident_bytes_estimate;
    let resident_budget = resident_estimate.saturating_add(headroom_margin);
    // Optional shared decoded-segment cache and search admission cap, so the
    // sweep can measure how they bound peak memory under high concurrency.
    let segment_cache_max_bytes = match env::var("BORSUK_LARGE_SCALE_SEGMENT_CACHE_BYTES") {
        Ok(value) if !value.trim().is_empty() => value.trim().parse::<u64>().ok(),
        _ => None,
    };
    let max_concurrent_searches = match env::var("BORSUK_LARGE_SCALE_MAX_CONCURRENT_SEARCHES") {
        Ok(value) if !value.trim().is_empty() => value.trim().parse::<usize>().ok(),
        _ => None,
    };
    let index = Arc::new(
        BorsukIndex::open_with_options(
            &uri,
            OpenOptions {
                resident_routing: false,
                ram_budget_bytes: Some(resident_budget),
                segment_cache_max_bytes,
                max_concurrent_searches,
                ..OpenOptions::default()
            },
        )
        .unwrap(),
    );

    let parallelism_levels = env::var("BORSUK_LARGE_SCALE_PARALLELISM")
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse::<usize>().ok())
                .filter(|workers| *workers > 0)
                .collect::<Vec<_>>()
        })
        .filter(|levels| !levels.is_empty())
        .unwrap_or_else(|| vec![8]);
    // Per-query budgets default to unbounded (worst case); override them to a
    // release-gate shape to model realistic concurrent users.
    let par_leaf_mode = match env::var("BORSUK_LARGE_SCALE_PARALLEL_LEAF_MODE") {
        Ok(value) if value.eq_ignore_ascii_case("pq-scan") => LeafMode::PqScan,
        Ok(value) if value.eq_ignore_ascii_case("sq-scan") => LeafMode::SqScan,
        Ok(value) if value.eq_ignore_ascii_case("vamana-pq") => LeafMode::VamanaPq,
        _ => LeafMode::Hybrid,
    };
    let par_max_segments = env_usize("BORSUK_LARGE_SCALE_PARALLEL_MAX_SEGMENTS", usize::MAX);
    let par_overfetch = env_usize("BORSUK_LARGE_SCALE_PARALLEL_OVERFETCH", usize::MAX);
    let par_candidates = env_usize("BORSUK_LARGE_SCALE_PARALLEL_MAX_CANDIDATES", usize::MAX);

    for &workers in &parallelism_levels {
        let rss_before = current_rss_bytes();
        let peak_rss = Arc::new(AtomicU64::new(rss_before.unwrap_or(0)));
        let running = Arc::new(AtomicBool::new(true));
        let sampler_running = Arc::clone(&running);
        let sampler_peak = Arc::clone(&peak_rss);
        let sampler = thread::spawn(move || {
            while sampler_running.load(AtomicOrdering::Relaxed) {
                if let Some(rss) = current_rss_bytes() {
                    update_peak(&sampler_peak, rss);
                }
                thread::sleep(Duration::from_millis(2));
            }
            if let Some(rss) = current_rss_bytes() {
                update_peak(&sampler_peak, rss);
            }
        });

        let started = Instant::now();
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let index = Arc::clone(&index);
            handles.push(thread::spawn(move || {
                let seed = worker.saturating_mul(record_count / workers.max(1));
                let query = deterministic_vector(seed, dimensions);
                index
                    .search_with_report(
                        &query,
                        SearchOptions::approx(10, par_leaf_mode)
                            .with_max_segments(par_max_segments)
                            .with_routing_page_overfetch(par_overfetch)
                            .with_max_candidates_per_segment(par_candidates),
                    )
                    .unwrap()
            }));
        }
        let reports = handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .expect("parallel search worker should not panic")
            })
            .collect::<Vec<_>>();
        let elapsed_ms = started.elapsed().as_millis();
        running.store(false, AtomicOrdering::Relaxed);
        sampler
            .join()
            .expect("large-scale headroom memory sampler should not panic");
        let rss_after = current_rss_bytes();
        if let Some(rss) = rss_after {
            update_peak(&peak_rss, rss);
        }
        let rss_peak = match peak_rss.load(AtomicOrdering::Relaxed) {
            0 => None,
            value => Some(value),
        };
        let max_report_resident = reports
            .iter()
            .map(|report| report.resident_bytes_estimate)
            .max()
            .unwrap_or(0);

        assert!(reports.iter().all(|report| !report.hits.is_empty()));
        assert!(max_report_resident <= resident_budget);
        eprintln!(
            "large_scale_headroom workers={} records={} dimensions={} pre_segments={} resident_estimate={} resident_budget={} headroom_margin={} elapsed_ms={} rss_before={} rss_peak={} rss_after={} rss_peak_delta={} max_report_resident={}",
            workers,
            record_count,
            dimensions,
            pre_compaction_segments,
            resident_estimate,
            resident_budget,
            headroom_margin,
            elapsed_ms,
            format_optional_u64(rss_before),
            format_optional_u64(rss_peak),
            format_optional_u64(rss_after),
            format_optional_i128(rss_delta(rss_before, rss_peak)),
            max_report_resident,
        );
    }
}

struct LargeScaleRunSummary {
    records: usize,
    dimensions: usize,
    segment_max_vectors: usize,
    max_segments: usize,
    routing_page_overfetch: usize,
    max_candidates_per_segment: usize,
    pre_segments: usize,
    post_segments: usize,
    ingest_ms: u128,
    compaction_ms: u128,
    exact_ms: u128,
    compaction_bytes_read: u64,
    compaction_bytes_written: u64,
    gc_ms: u128,
    gc_objects_scanned: usize,
    gc_objects_deleted: usize,
    gc_bytes_reclaimed: u64,
}

struct LargeScaleQuerySummary {
    mode: String,
    tie_aware_recall_at_10: f32,
    id_recall_at_10: f32,
    termination_reason: String,
    query_ms: u128,
    segments_searched: usize,
    bytes_read: u64,
    graph_bytes_read: u64,
    routing_page_indexes_read: usize,
    routing_pages_read: usize,
    resident_bytes: u64,
    rss_before: Option<u64>,
    rss_peak: Option<u64>,
    rss_after: Option<u64>,
    records_considered: usize,
    records_scored: usize,
    graph_candidates_added: usize,
}

fn write_large_scale_csv(
    path: &Path,
    run: &LargeScaleRunSummary,
    queries: &[LargeScaleQuerySummary],
) -> std::io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, large_scale_csv(run, queries))
}

fn large_scale_csv(run: &LargeScaleRunSummary, queries: &[LargeScaleQuerySummary]) -> String {
    let mut csv = String::from(
        "records,dimensions,segment_max_vectors,max_segments,routing_page_overfetch,max_candidates_per_segment,pre_segments,post_segments,ingest_ms,compaction_ms,exact_ms,compaction_bytes_read,compaction_bytes_written,gc_ms,gc_objects_scanned,gc_objects_deleted,gc_bytes_reclaimed,mode,tie_aware_recall_at_10,id_recall_at_10,termination_reason,query_ms,segments_searched,bytes_read,graph_bytes_read,routing_page_indexes_read,routing_pages_read,resident_bytes,rss_before,rss_peak,rss_after,rss_peak_delta,records_considered,records_scored,graph_candidates_added\n",
    );
    for query in queries {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            run.records,
            run.dimensions,
            run.segment_max_vectors,
            run.max_segments,
            run.routing_page_overfetch,
            run.max_candidates_per_segment,
            run.pre_segments,
            run.post_segments,
            run.ingest_ms,
            run.compaction_ms,
            run.exact_ms,
            run.compaction_bytes_read,
            run.compaction_bytes_written,
            run.gc_ms,
            run.gc_objects_scanned,
            run.gc_objects_deleted,
            run.gc_bytes_reclaimed,
            query.mode,
            query.tie_aware_recall_at_10,
            query.id_recall_at_10,
            query.termination_reason,
            query.query_ms,
            query.segments_searched,
            query.bytes_read,
            query.graph_bytes_read,
            query.routing_page_indexes_read,
            query.routing_pages_read,
            query.resident_bytes,
            format_optional_u64(query.rss_before),
            format_optional_u64(query.rss_peak),
            format_optional_u64(query.rss_after),
            format_optional_i128(rss_delta(query.rss_before, query.rss_peak)),
            query.records_considered,
            query.records_scored,
            query.graph_candidates_added,
        ));
    }
    csv
}

fn current_rss_bytes() -> Option<u64> {
    memory_stats().map(|stats| stats.physical_mem as u64)
}

fn update_peak(peak: &AtomicU64, candidate: u64) {
    let mut current = peak.load(AtomicOrdering::Relaxed);
    while candidate > current {
        match peak.compare_exchange_weak(
            current,
            candidate,
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
        ) {
            Ok(_) => break,
            Err(value) => current = value,
        }
    }
}

fn rss_delta(before: Option<u64>, peak: Option<u64>) -> Option<i128> {
    Some(i128::from(peak?) - i128::from(before?))
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn format_optional_i128(value: Option<i128>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn assert_high_recall_report(
    exact_hits: &[SearchHit],
    report: &SearchReport,
    min_tie_aware_recall: f32,
    max_segments: usize,
    max_resident_bytes: u64,
    expect_graph_reads: bool,
) {
    let recall =
        tie_aware_recall_at_k(&hit_distances(exact_hits), &hit_distances(&report.hits), 10)
            .unwrap();
    assert!(
        recall >= min_tie_aware_recall,
        "{} tie-aware recall@10 was {recall}, below {min_tie_aware_recall}; hits={:?}",
        report.leaf_mode,
        report.hits
    );
    assert!(report.segments_searched <= max_segments);
    assert!(report.resident_bytes_estimate <= max_resident_bytes);
    if expect_graph_reads {
        assert!(report.graph_bytes_read > 0);
        assert!(report.graph_candidates_added > 0);
    } else {
        assert_eq!(report.graph_bytes_read, 0);
        assert_eq!(report.graph_candidates_added, 0);
    }
}

fn hit_distances(hits: &[SearchHit]) -> Vec<f32> {
    hits.iter().map(|hit| hit.distance).collect()
}

fn hit_ids(hits: &[SearchHit]) -> Vec<String> {
    hits.iter().map(|hit| hit.id.to_string()).collect()
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|error| panic!("{name} must be a usize: {error}"))
        })
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .unwrap_or_else(|error| panic!("{name} must be a u64: {error}"))
        })
        .unwrap_or(default)
}

fn env_f32(name: &str, default: f32) -> f32 {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<f32>()
                .unwrap_or_else(|error| panic!("{name} must be an f32: {error}"))
        })
        .unwrap_or(default)
}

fn deterministic_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dimension| centered_unit(seed, dimension))
        .collect()
}

fn centered_unit(seed: usize, dimension: usize) -> f32 {
    let mixed = splitmix64(
        (seed as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (dimension as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9),
    );
    let unit = (mixed >> 40) as f32 / (1_u64 << 24) as f32;
    unit - 0.5
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
