#![allow(missing_docs)]

use borsuk::{
    BorsukIndex, IndexConfig, LeafMode, SearchOptions, VectorMetric, VectorRecord, recall_at_k,
    vector_metric_names,
};

fn main() -> borsuk::Result<()> {
    let dir = std::env::temp_dir().join("borsuk-example-index");
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|source| borsuk::BorsukError::Io {
            path: dir.clone(),
            source,
        })?;
    }

    let mut index = BorsukIndex::create(IndexConfig {
        uri: dir.to_string_lossy().into_owned(),
        metric: VectorMetric::Euclidean,
        dimensions: 3,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })?;

    index.add(vec![
        VectorRecord::new("alpha", vec![0.0, 0.0, 0.0]),
        VectorRecord::new("beta", vec![1.0, 0.0, 0.0]),
        VectorRecord::new("gamma", vec![0.0, 5.0, 0.0]),
    ])?;

    let stats = index.stats();
    assert_eq!(stats.metric, "euclidean");
    assert_eq!(stats.dimensions, 3);
    assert_eq!(stats.segments, 2);
    assert_eq!(stats.records, 3);
    assert!(stats.segment_bytes > 0);
    assert!(stats.graph_bytes > 0);
    assert!(stats.resident_bytes_estimate > 0);
    println!(
        "records={}\tsegments={}\tsegment_bytes={}\tresident_bytes_estimate={}",
        stats.records, stats.segments, stats.segment_bytes, stats.resident_bytes_estimate
    );

    let exact_ids = index.search_ids(&[0.2, 0.0, 0.0], SearchOptions::exact(2))?;
    assert_eq!(exact_ids, ["alpha", "beta"]);

    let report = index.search_with_report(
        &[0.2, 0.0, 0.0],
        SearchOptions::approx(2, LeafMode::Graph).with_max_candidates_per_segment(2),
    )?;
    let approx_ids = report
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(approx_ids, exact_ids);
    assert_eq!(report.leaf_mode, "graph");
    assert_eq!(
        index.search_vectors(&[0.2, 0.0, 0.0], SearchOptions::exact(2))?,
        [vec![0.0, 0.0, 0.0], vec![1.0, 0.0, 0.0]]
    );
    assert_eq!(index.get_vector("beta")?, Some(vec![1.0, 0.0, 0.0]));
    assert!(report.bytes_read > 0);
    assert!(report.graph_bytes_read > 0);
    assert!(report.records_scored <= report.records_considered);
    assert!(report.resident_bytes_estimate > 0);

    let vamana_pq_report = index.search_with_report(
        &[0.2, 0.0, 0.0],
        SearchOptions::approx(2, LeafMode::VamanaPq).with_max_candidates_per_segment(2),
    )?;
    let vamana_pq_ids = vamana_pq_report
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(vamana_pq_ids, exact_ids);
    assert_eq!(vamana_pq_report.leaf_mode, "vamana-pq");
    assert!(vamana_pq_report.graph_bytes_read > 0);

    let hybrid_report = index.search_with_report(
        &[0.2, 0.0, 0.0],
        SearchOptions::approx(2, LeafMode::Hybrid).with_max_candidates_per_segment(2),
    )?;
    let hybrid_ids = hybrid_report
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(hybrid_ids, exact_ids);
    assert_eq!(hybrid_report.leaf_mode, "hybrid");
    assert!(hybrid_report.graph_bytes_read > 0);

    let pq_report = index.search_with_report(
        &[0.2, 0.0, 0.0],
        SearchOptions::approx(2, LeafMode::PqScan).with_max_candidates_per_segment(2),
    )?;
    let pq_ids = pq_report
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(pq_ids, exact_ids);
    assert_eq!(pq_report.leaf_mode, "pq-scan");
    assert_eq!(pq_report.graph_bytes_read, 0);
    assert_eq!(pq_report.graph_candidates_added, 0);

    let recall = recall_at_k(&exact_ids, &approx_ids, 2)?;
    assert_eq!(recall, 1.0);
    assert!(vector_metric_names().contains(&"euclidean"));

    let sq_report = index.search_with_report(
        &[0.2, 0.0, 0.0],
        SearchOptions::approx(2, LeafMode::SqScan).with_max_candidates_per_segment(2),
    )?;
    let sq_ids = sq_report
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(sq_ids, exact_ids);
    assert_eq!(sq_report.leaf_mode, "sq-scan");
    assert_eq!(sq_report.graph_bytes_read, 0);
    assert_eq!(sq_report.graph_candidates_added, 0);

    let reports = index.search_batch_with_report(
        &[vec![0.2, 0.0, 0.0], vec![0.0, 4.9, 0.0]],
        SearchOptions::exact(1),
    )?;
    for report in reports {
        println!(
            "batch_hit={}\tbytes_read={}",
            report.hits[0].id, report.bytes_read
        );
    }

    println!(
        "hits={}\tpq_hits={}\tsq_hits={}\thybrid_hits={}\tbytes_read={}\tgraph_bytes_read={}\trecall_at_2={}\tobject_cache_misses={}\trecords_scored={}",
        approx_ids.join(","),
        pq_ids.join(","),
        sq_ids.join(","),
        hybrid_ids.join(","),
        report.bytes_read,
        report.graph_bytes_read,
        recall,
        report.object_cache_misses,
        report.records_scored
    );

    Ok(())
}
