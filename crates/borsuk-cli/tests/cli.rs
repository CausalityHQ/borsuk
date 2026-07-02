#![allow(missing_docs)]

use std::{fs, process::Command};

use assert_cmd::prelude::*;
use borsuk::{VectorRecord, vector_records_to_parquet};

#[test]
fn cli_creates_adds_and_searches_local_index() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"a","vector":[0.0,0.0]},{"id":"b","vector":[1.0,0.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "1",
        ])
        .assert()
        .success();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["search", "--uri", &uri, "--query", "[0.2,0.0]", "--k", "2"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let hits: Vec<serde_json::Value> = serde_json::from_slice(&output).unwrap();
    assert_eq!(hits[0]["id"], "a");
    assert_eq!(hits[1]["id"], "b");
}

#[test]
fn cli_add_accepts_parquet_vector_records() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.parquet");
    fs::write(
        &records,
        vector_records_to_parquet(
            &[
                VectorRecord::new("a", vec![0.0, 0.0]),
                VectorRecord::new("b", vec![1.0, 0.0]),
            ],
            2,
        )
        .unwrap(),
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "1",
        ])
        .assert()
        .success();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["search", "--uri", &uri, "--query", "[0.2,0.0]", "--k", "2"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let hits: Vec<serde_json::Value> = serde_json::from_slice(&output).unwrap();
    assert_eq!(hits[0]["id"], "a");
    assert_eq!(hits[1]["id"], "b");
}

#[test]
fn cli_search_obeys_approx_byte_budget() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"near","vector":[0.0,0.0]},{"id":"mid","vector":[10.0,0.0]},{"id":"far","vector":[20.0,0.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "1",
        ])
        .assert()
        .success();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search",
            "--uri",
            &uri,
            "--query",
            "[0.0,0.0]",
            "--k",
            "3",
            "--mode",
            "approx",
            "--max-bytes",
            "1",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let hits: Vec<serde_json::Value> = serde_json::from_slice(&output).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["id"], "near");
}

#[test]
fn cli_search_accepts_byte_budget_string() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"near","vector":[0.0,0.0]},{"id":"mid","vector":[10.0,0.0]},{"id":"far","vector":[20.0,0.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "1",
        ])
        .assert()
        .success();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search",
            "--uri",
            &uri,
            "--query",
            "[0.0,0.0]",
            "--k",
            "3",
            "--mode",
            "approx",
            "--max-bytes",
            "1B",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let hits: Vec<serde_json::Value> = serde_json::from_slice(&output).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["id"], "near");
}

#[test]
fn cli_search_can_report_query_counters() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"near","vector":[0.0,0.0]},{"id":"mid","vector":[10.0,0.0]},{"id":"far","vector":[20.0,0.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "1",
        ])
        .assert()
        .success();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search",
            "--uri",
            &uri,
            "--query",
            "[0.0,0.0]",
            "--k",
            "1",
            "--mode",
            "approx",
            "--max-segments",
            "1",
            "--report",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(report["hits"][0]["id"], "near");
    assert_eq!(report["segments_total"], 3);
    assert_eq!(report["segments_searched"], 1);
    assert_eq!(report["segments_skipped"], 2);
    assert!(report["bytes_read"].as_u64().unwrap() > 0);
    assert!(report["records_scored"].as_u64().unwrap() > 0);
    assert!(report["resident_bytes_estimate"].as_u64().unwrap() > 0);
}

#[test]
fn cli_search_accepts_flat_scan_leaf_mode() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"near","vector":[0.0]},{"id":"next","vector":[0.2]},{"id":"far-a","vector":[10.0]},{"id":"far-b","vector":[20.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "1",
            "--segment-max-vectors",
            "4",
        ])
        .assert()
        .success();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search",
            "--uri",
            &uri,
            "--query",
            "[0.05]",
            "--k",
            "1",
            "--mode",
            "approx",
            "--leaf-mode",
            "flat-scan",
            "--max-candidates-per-segment",
            "2",
            "--report",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(report["leaf_mode"], "flat-scan");
    assert_eq!(report["hits"][0]["id"], "near");
    assert_eq!(report["graph_bytes_read"], 0);
    assert_eq!(report["graph_candidates_added"], 0);
}

#[test]
fn cli_search_accepts_pq_scan_leaf_mode() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"entry","vector":[0.0,0.0]},{"id":"routing-neighbor","vector":[0.2,0.0]},{"id":"graph-neighbor","vector":[0.0,0.1]},{"id":"far","vector":[100.0,100.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "4",
        ])
        .assert()
        .success();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search",
            "--uri",
            &uri,
            "--query",
            "[0.19,0.0]",
            "--k",
            "1",
            "--mode",
            "approx",
            "--leaf-mode",
            "pq-scan",
            "--max-candidates-per-segment",
            "2",
            "--report",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(report["leaf_mode"], "pq-scan");
    assert_eq!(report["hits"][0]["id"], "routing-neighbor");
    assert_eq!(report["graph_bytes_read"], 0);
    assert_eq!(report["graph_candidates_added"], 0);
}

#[test]
fn cli_search_accepts_vamana_pq_leaf_mode() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"entry","vector":[0.0,0.0]},{"id":"true-neighbor","vector":[0.0,0.1]},{"id":"routing-decoy","vector":[0.1,-0.1]},{"id":"far","vector":[100.0,100.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "4",
        ])
        .assert()
        .success();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search",
            "--uri",
            &uri,
            "--query",
            "[0.04,0.07]",
            "--k",
            "1",
            "--mode",
            "approx",
            "--leaf-mode",
            "vamana-pq",
            "--max-candidates-per-segment",
            "2",
            "--report",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(report["leaf_mode"], "vamana-pq");
    assert_eq!(report["hits"][0]["id"], "true-neighbor");
    assert!(report["graph_bytes_read"].as_u64().unwrap() > 0);
    assert_eq!(report["graph_candidates_added"], 1);
}

#[test]
fn cli_search_accepts_hybrid_leaf_mode() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"entry","vector":[0.0,0.0]},{"id":"true-neighbor","vector":[0.0,0.1]},{"id":"routing-decoy","vector":[0.1,-0.1]},{"id":"far","vector":[100.0,100.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "4",
        ])
        .assert()
        .success();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search",
            "--uri",
            &uri,
            "--query",
            "[0.04,0.07]",
            "--k",
            "1",
            "--mode",
            "approx",
            "--leaf-mode",
            "hybrid",
            "--max-candidates-per-segment",
            "2",
            "--report",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(report["leaf_mode"], "hybrid");
    assert_eq!(report["hits"][0]["id"], "true-neighbor");
    assert!(report["graph_bytes_read"].as_u64().unwrap() > 0);
    assert_eq!(report["graph_candidates_added"], 1);
}

#[test]
fn cli_search_uses_local_read_through_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"near","vector":[0.0,0.0]},{"id":"far","vector":[10.0,0.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "1",
        ])
        .assert()
        .success();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search",
            "--uri",
            &uri,
            "--query",
            "[0.0,0.0]",
            "--k",
            "1",
            "--report",
            "--cache-dir",
            cache.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    let second_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search",
            "--uri",
            &uri,
            "--query",
            "[0.0,0.0]",
            "--k",
            "1",
            "--report",
            "--cache-dir",
            cache.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let second_report: serde_json::Value = serde_json::from_slice(&second_output).unwrap();
    assert_eq!(second_report["hits"][0]["id"], "near");
    assert!(second_report["object_cache_hits"].as_u64().unwrap() > 0);
    assert_eq!(second_report["object_cache_misses"], 0);
}

#[test]
fn cli_reports_manifest_stats() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"a","vector":[0.0,0.0]},{"id":"b","vector":[1.0,0.0]},{"id":"c","vector":[10.0,0.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "2",
        ])
        .assert()
        .success();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["stats", "--uri", &uri])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let stats: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(stats["metric"], "euclidean");
    assert_eq!(stats["dimensions"], 2);
    assert_eq!(stats["segment_max_vectors"], 2);
    assert_eq!(stats["manifest_version"], 2);
    assert_eq!(stats["segments"], 2);
    assert_eq!(stats["records"], 3);
    assert!(stats["segment_bytes"].as_u64().unwrap() > 0);
    assert!(stats["graph_bytes"].as_u64().unwrap() > 0);
    assert!(stats["resident_bytes_estimate"].as_u64().unwrap() > 0);
}

#[test]
fn cli_create_persists_ram_budget() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "2",
            "--ram-budget",
            "1MB",
        ])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["stats", "--uri", &uri])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let stats: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(stats["ram_budget_bytes"], 1_000_000);
}

#[test]
fn cli_compacts_local_index() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"a","vector":[0.0,0.0]},{"id":"b","vector":[1.0,0.0]},{"id":"c","vector":[8.0,0.0]},{"id":"d","vector":[9.0,0.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "1",
        ])
        .assert()
        .success();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let compact_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "compact",
            "--uri",
            &uri,
            "--source-level",
            "0",
            "--target-level",
            "1",
            "--max-segments",
            "4",
            "--target-segment-max-vectors",
            "2",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let report: serde_json::Value = serde_json::from_slice(&compact_output).unwrap();
    assert_eq!(report["compacted"], true);
    assert_eq!(report["segments_read"], 4);
    assert_eq!(report["segments_written"], 2);
    assert_eq!(report["records_rewritten"], 4);
    assert_eq!(report["object_cache_hits"], 0);
    assert_eq!(report["object_cache_misses"], 4);

    let search_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["search", "--uri", &uri, "--query", "[8.5,0.0]", "--k", "2"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let hits: Vec<serde_json::Value> = serde_json::from_slice(&search_output).unwrap();
    assert_eq!(hits[0]["id"], "c");
    assert_eq!(hits[1]["id"], "d");
}

#[test]
fn cli_compact_uses_local_read_through_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"a","vector":[0.0,0.0]},{"id":"b","vector":[1.0,0.0]},{"id":"c","vector":[8.0,0.0]},{"id":"d","vector":[9.0,0.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "1",
        ])
        .assert()
        .success();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "compact",
            "--uri",
            &uri,
            "--target-segment-max-vectors",
            "2",
            "--cache-dir",
            cache.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    assert!(has_parquet_files(cache.path().join("segments")));
    assert!(has_parquet_files(cache.path().join("graphs")));
}

#[test]
fn cli_gc_dry_runs_and_deletes_obsolete_segments() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"a","vector":[0.0,0.0]},{"id":"b","vector":[1.0,0.0]},{"id":"c","vector":[8.0,0.0]},{"id":"d","vector":[9.0,0.0]}]"#,
    )
    .unwrap();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "create",
            "--uri",
            &uri,
            "--metric",
            "euclidean",
            "--dimensions",
            "2",
            "--segment-max-vectors",
            "1",
        ])
        .assert()
        .success();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "compact",
            "--uri",
            &uri,
            "--target-segment-max-vectors",
            "2",
        ])
        .assert()
        .success();

    let dry_run_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["gc", "--uri", &uri])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let dry_run: serde_json::Value = serde_json::from_slice(&dry_run_output).unwrap();
    assert_eq!(dry_run["dry_run"], true);
    assert_eq!(dry_run["objects_scanned"], 12);
    assert_eq!(dry_run["objects_deleted"], 0);
    assert_eq!(dry_run["candidates"].as_array().unwrap().len(), 8);

    let delete_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["gc", "--uri", &uri, "--delete"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let deleted: serde_json::Value = serde_json::from_slice(&delete_output).unwrap();
    assert_eq!(deleted["dry_run"], false);
    assert_eq!(deleted["objects_deleted"], 8);

    let search_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["search", "--uri", &uri, "--query", "[8.5,0.0]", "--k", "2"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let hits: Vec<serde_json::Value> = serde_json::from_slice(&search_output).unwrap();
    assert_eq!(hits[0]["id"], "c");
    assert_eq!(hits[1]["id"], "d");
}

fn has_parquet_files(root: impl AsRef<std::path::Path>) -> bool {
    let root = root.as_ref();
    if !root.exists() {
        return false;
    }

    fs::read_dir(root).unwrap().any(|entry| {
        let path = entry.unwrap().path();
        if path.is_dir() {
            has_parquet_files(&path)
        } else {
            path.extension().is_some_and(|actual| actual == "parquet")
        }
    })
}
