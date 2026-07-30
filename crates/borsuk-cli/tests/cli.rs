#![allow(missing_docs)]

use std::{fs, process::Command};

use assert_cmd::prelude::*;
use borsuk::{
    BorsukIndex, DurableTableFormat, GlobalScanCodec, VectorElementType, VectorRecord,
    vector_records_to_parquet,
};

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
fn cli_dense_scalar_matrix_survives_reopen() {
    for (spelling, expected, metric, vector) in [
        (
            "float32",
            VectorElementType::Float32,
            "squared-euclidean",
            "[1.0,0.3]",
        ),
        (
            "float16",
            VectorElementType::Float16,
            "squared-euclidean",
            "[1.0,0.3]",
        ),
        (
            "bfloat16",
            VectorElementType::BFloat16,
            "squared-euclidean",
            "[1.0,0.3]",
        ),
        (
            "float8-e4m3fn",
            VectorElementType::Float8E4M3Fn,
            "squared-euclidean",
            "[1.0,0.3]",
        ),
        (
            "float8-e5m2",
            VectorElementType::Float8E5M2,
            "squared-euclidean",
            "[1.0,0.3]",
        ),
        (
            "fp8",
            VectorElementType::Float8E4M3Fn,
            "squared-euclidean",
            "[1.0,0.3]",
        ),
        (
            "int8",
            VectorElementType::Int8,
            "squared-euclidean",
            "[1.0,0.0]",
        ),
        ("binary", VectorElementType::Binary, "hamming", "[1.0,0.0]"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let records = dir.path().join("records.json");
        fs::write(&records, format!(r#"[{{"id":"typed","vector":{vector}}}]"#)).unwrap();

        Command::cargo_bin("borsuk")
            .unwrap()
            .args([
                "create",
                "--uri",
                &uri,
                "--metric",
                metric,
                "--dimensions",
                "2",
                "--segment-max-vectors",
                "1",
                "--vector-element-type",
                spelling,
            ])
            .assert()
            .success();
        Command::cargo_bin("borsuk")
            .unwrap()
            .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
            .assert()
            .success();

        let index = BorsukIndex::open(&uri).unwrap();
        assert_eq!(index.build_config().vector_element_type, expected);
        let canonical = index.get_vector("typed").unwrap().unwrap();
        assert_eq!(
            index
                .search_ids(&canonical, borsuk::SearchOptions::exact(1))
                .unwrap(),
            vec!["typed"]
        );
    }
}

#[test]
fn cli_create_persists_the_selected_global_scan_codec() {
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
            "16",
            "--global-scan-codec",
            "fast-turboquant-scan",
            "--turboquant-bits",
            "3",
            "--turboquant-qjl-bits",
            "0",
            "--turboquant-shards",
            "1",
        ])
        .assert()
        .success();

    let index = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        index.build_config().global_scan_codec,
        GlobalScanCodec::FastTurboQuantProd
    );
    assert_eq!(index.build_config().global_turboquant_bits, 3);
    assert_eq!(index.build_config().global_turboquant_qjl_bits, 0);
    assert_eq!(index.build_config().global_turboquant_shards, 1);
}

#[test]
fn cli_create_persists_the_selected_segment_table_format() {
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
            "16",
            "--segment-table-format",
            "vortex",
        ])
        .assert()
        .success();

    let index = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        index.build_config().segment_table_format,
        DurableTableFormat::Vortex
    );
}

#[test]
fn cli_upsert_overwrites_an_existing_id() {
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
        ])
        .assert()
        .success();

    let initial = dir.path().join("initial.json");
    fs::write(&initial, r#"[{"id":"a","vector":[1.0,0.0]}]"#).unwrap();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", initial.to_str().unwrap()])
        .assert()
        .success();

    // Upsert the same id to a new location.
    let replacement = dir.path().join("replacement.json");
    fs::write(&replacement, r#"[{"id":"a","vector":[0.0,1.0]}]"#).unwrap();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "upsert",
            "--uri",
            &uri,
            "--input",
            replacement.to_str().unwrap(),
        ])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["search", "--uri", &uri, "--query", "[0.0,1.0]", "--k", "5"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let hits: Vec<serde_json::Value> = serde_json::from_slice(&output).unwrap();
    // Exactly one "a", now the nearest to the new location.
    assert_eq!(hits[0]["id"], "a");
    assert_eq!(hits.iter().filter(|hit| hit["id"] == "a").count(), 1);
}

#[test]
fn cli_stores_metadata_and_filters_search() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[
            {"id":"a","vector":[0.0,0.0],"metadata":{"genre":"rock","year":1975}},
            {"id":"b","vector":[1.0,0.0],"metadata":{"genre":"rock","year":2001}},
            {"id":"c","vector":[2.0,0.0],"metadata":{"genre":"jazz","year":1999}}
        ]"#,
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
            "--filter",
            r#"{"genre":"rock","year":{"$gte":2000}}"#,
            "--include-metadata",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let hits: Vec<serde_json::Value> = serde_json::from_slice(&output).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["id"], "b");
    assert_eq!(hits[0]["metadata"]["genre"], "rock");
    assert_eq!(hits[0]["metadata"]["year"], 2001);

    // The report exposes the filter counters.
    let report_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search",
            "--uri",
            &uri,
            "--query",
            "[0.0,0.0]",
            "--k",
            "3",
            "--filter",
            r#"{"genre":"jazz"}"#,
            "--report",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let report: serde_json::Value = serde_json::from_slice(&report_output).unwrap();
    assert_eq!(report["hits"].as_array().unwrap().len(), 1);
    assert_eq!(report["hits"][0]["id"], "c");
    assert!(report["rows_passed_filter"].as_u64().unwrap() >= 1);
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
fn cli_add_accepts_sparse_input_and_searches_text_hybrid_records() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[
            {"id":"alpha","vector":[0.0,0.0],"text":"apple banana apple"},
            {"id":"beta","sparse_indices":[0],"sparse_values":[10.0],"text":"orange citrus"},
            {"id":"gamma","vector":[0.2,0.0],"text":"hybrid needle"}
        ]"#,
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
            "--text",
        ])
        .assert()
        .success();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let vector_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["search", "--uri", &uri, "--query", "[10.0,0.0]", "--k", "1"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let vector_hits: Vec<serde_json::Value> = serde_json::from_slice(&vector_output).unwrap();
    assert_eq!(vector_hits[0]["id"], "beta");

    let text_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["search-text", "--uri", &uri, "--text", "needle", "--k", "1"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let text_hits: Vec<serde_json::Value> = serde_json::from_slice(&text_output).unwrap();
    assert_eq!(text_hits[0]["id"], "gamma");

    let hybrid_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search-hybrid",
            "--uri",
            &uri,
            "--vector",
            ":0.0,0.0",
            "--text",
            "needle",
            "--k",
            "1",
            "--fusion",
            "weighted",
            "--weights",
            "=0.0,@text=1.0",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let hybrid_hits: Vec<serde_json::Value> = serde_json::from_slice(&hybrid_output).unwrap();
    assert_eq!(hybrid_hits[0]["id"], "gamma");
}

#[test]
fn cli_supports_named_vectors_and_multi_vector_hybrid_search() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[
            {"id":"a","vector":[0.0,0.0],"named_vectors":{"lexical":{"indices":[0],"values":[3.0]}}},
            {"id":"b","vector":[1.0,0.0],"named_vectors":{"lexical":[0.0,0.0]}},
            {"id":"c","vector":[2.0,0.0],"named_vectors":{"lexical":[1.0,0.0]}},
            {"id":"d","vector":[3.0,0.0],"named_vectors":{"lexical":[2.0,0.0]}}
        ]"#,
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
            "--named-vector",
            "lexical:2:euclidean",
        ])
        .assert()
        .success();

    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let named_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search",
            "--uri",
            &uri,
            "--query",
            "[0.0,0.0]",
            "--vector",
            "lexical",
            "--k",
            "3",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let named_hits: Vec<serde_json::Value> = serde_json::from_slice(&named_output).unwrap();
    assert_eq!(named_hits[0]["id"], "b");
    assert_eq!(named_hits[1]["id"], "c");
    assert_eq!(named_hits[2]["id"], "d");

    let hybrid_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search-hybrid",
            "--uri",
            &uri,
            "--vector",
            ":0.0,0.0",
            "--sparse-vector",
            "lexical:0:0.0",
            "--k",
            "3",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let hybrid_hits: Vec<serde_json::Value> = serde_json::from_slice(&hybrid_output).unwrap();
    assert_eq!(hybrid_hits[0]["id"], "b");
    assert_eq!(hybrid_hits[1]["id"], "a");
    assert_eq!(hybrid_hits[2]["id"], "c");

    let stats_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["stats", "--uri", &uri])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let stats: serde_json::Value = serde_json::from_slice(&stats_output).unwrap();
    assert_eq!(stats["named_vectors"], serde_json::json!(["lexical"]));
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
            "1",
            "--mode",
            "approx",
            "--max-bytes",
            "1",
            "--report",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(report["termination_reason"], "max-bytes");
    assert!(report["hits"].as_array().unwrap().is_empty());
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
            "1",
            "--mode",
            "approx",
            "--max-bytes",
            "1MiB",
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
    assert_eq!(
        report["collection_resident_bytes"],
        report["resident_bytes_estimate"]
    );
    assert!(
        report["transient_peak_bytes"].as_u64().unwrap()
            <= report["transient_capacity_bytes"].as_u64().unwrap()
    );
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
fn cli_approx_search_defaults_to_graph_free_srht_pq_scan() {
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
        .args([
            "search",
            "--uri",
            &uri,
            "--query",
            "[0.1,0.0]",
            "--k",
            "1",
            "--mode",
            "approx",
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
    assert_eq!(report["leaf_mode"], "srht-pq-scan");
    assert_eq!(report["graph_bytes_read"], 0);
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
            "--leaf-capability",
            "graph-enabled",
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
            "--leaf-capability",
            "graph-enabled",
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
    assert_eq!(stats["graph_bytes"], 0);
    assert!(stats["resident_bytes_estimate"].as_u64().unwrap() > 0);
    assert_eq!(
        stats["collection_resident_bytes"],
        stats["resident_bytes_estimate"]
    );
    assert!(
        stats["retained_peak_bytes"].as_u64().unwrap()
            <= stats["retained_capacity_bytes"].as_u64().unwrap()
    );
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
fn cli_create_supports_routing_page_fanout() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    let records_json = (0..17)
        .map(|id| format!(r#"{{"id":"v{id}","vector":[{id}.0,0.0]}}"#))
        .collect::<Vec<_>>()
        .join(",");
    fs::write(&records, format!("[{records_json}]")).unwrap();

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
            "--routing-page-fanout",
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
        .args(["stats", "--uri", &uri])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let stats: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(stats["routing_page_fanout"], 4);
    assert_eq!(stats["routing_max_level"], 2);
    assert_eq!(stats["routing_leaf_pages"], 5);
    assert_eq!(stats["routing_pages"], 8);
}

#[test]
fn cli_stats_can_use_paged_routing_without_resident_segment_summaries() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let records = dir.path().join("records.json");
    let records_json = (0..130)
        .map(|id| format!(r#"{{"id":"v{id}","vector":[{id}.0,0.0]}}"#))
        .collect::<Vec<_>>()
        .join(",");
    fs::write(&records, format!("[{records_json}]")).unwrap();

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

    let resident_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["stats", "--uri", &uri, "--resident-routing"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let paged_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["stats", "--uri", &uri])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let resident_stats: serde_json::Value = serde_json::from_slice(&resident_output).unwrap();
    let paged_stats: serde_json::Value = serde_json::from_slice(&paged_output).unwrap();
    assert_eq!(paged_stats["segments"], 130);
    assert_eq!(paged_stats["records"], 130);
    assert!(
        paged_stats["resident_bytes_estimate"].as_u64().unwrap()
            < resident_stats["resident_bytes_estimate"].as_u64().unwrap()
    );
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
    assert_eq!(report["object_cache_misses"], 6);

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
fn cli_rebuild_compacts_and_deletes_obsolete_objects_when_requested() {
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

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "rebuild",
            "--uri",
            &uri,
            "--source-level",
            "0",
            "--target-level",
            "1",
            "--target-segment-max-vectors",
            "2",
            "--delete-obsolete",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(report["compaction"]["compacted"], true);
    assert_eq!(report["compaction"]["segments_read"], 4);
    assert_eq!(report["compaction"]["segments_written"], 2);
    assert_eq!(report["garbage_collection"]["dry_run"], false);
    let candidates = report["garbage_collection"]["candidates"]
        .as_array()
        .unwrap();
    assert_eq!(
        report["garbage_collection"]["objects_deleted"],
        candidates.len()
    );
    let routing_candidates = candidates
        .iter()
        .filter(|candidate| {
            candidate.as_str().is_some_and(|path| {
                path.starts_with("routing/pages/") || path.starts_with("routing/layers/")
            })
        })
        .count();
    let table_candidates = candidates
        .iter()
        .filter(|candidate| {
            candidate.as_str().is_some_and(|path| {
                path.starts_with("manifests/")
                    || path.starts_with("routing/")
                        && !path.starts_with("routing/pages/")
                        && !path.starts_with("routing/layers/")
                    || path.starts_with("tombstones/")
            })
        })
        .count();
    assert_eq!(
        report["garbage_collection"]["routing_objects_deleted"],
        routing_candidates
    );
    assert_eq!(
        report["garbage_collection"]["tables_deleted"],
        table_candidates
    );
    assert!(!candidates.is_empty());

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
    assert!(!has_parquet_files(cache.path().join("graphs")));
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
        .args(["gc", "--uri", &uri, "--min-age-seconds", "0"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let dry_run: serde_json::Value = serde_json::from_slice(&dry_run_output).unwrap();
    assert_eq!(dry_run["dry_run"], true);
    assert_eq!(dry_run["objects_deleted"], 0);
    assert_eq!(dry_run["routing_objects_deleted"], 0);
    assert_eq!(dry_run["tables_deleted"], 0);
    let objects_scanned = dry_run["objects_scanned"].as_u64().unwrap();
    let candidates = dry_run["candidates"].as_array().unwrap().len() as u64;
    assert!(objects_scanned > candidates);
    assert!(candidates > 0);

    let delete_output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["gc", "--uri", &uri, "--delete", "--min-age-seconds", "0"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let deleted: serde_json::Value = serde_json::from_slice(&delete_output).unwrap();
    assert_eq!(deleted["dry_run"], false);
    assert_eq!(deleted["objects_scanned"], objects_scanned);
    assert_eq!(deleted["objects_deleted"], candidates);
    assert_eq!(deleted["routing_objects_deleted"], 3);
    assert_eq!(deleted["tables_deleted"], 6);

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

#[test]
fn cli_explain_reports_query_cost() {
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
            "4",
        ])
        .assert()
        .success();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"a","vector":[0.0,0.0]},{"id":"b","vector":[1.0,0.0]}]"#,
    )
    .unwrap();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args(["explain", "--uri", &uri, "--query", "[0.0,0.0]", "--k", "2"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let plan: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(plan["hits"].as_array().unwrap().len(), 2);
    assert!(plan["get_requests"].as_u64().unwrap() >= 1);
    assert!(plan["estimated_cost_usd"].as_f64().unwrap() >= 0.0);
}

#[test]
fn cli_search_sparse_named_vector() {
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
            "--named-vector",
            "lexical:1000:inner-product:sparse",
        ])
        .assert()
        .success();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"a","vector":[1.0,0.0],"named_vectors":{"lexical":{"indices":[5,7],"values":[1.0,2.0]}}},
            {"id":"b","vector":[2.0,0.0],"named_vectors":{"lexical":{"indices":[5,9],"values":[3.0,1.0]}}}]"#,
    )
    .unwrap();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search-sparse-named",
            "--uri",
            &uri,
            "--name",
            "lexical",
            "--indices",
            "[7]",
            "--values",
            "[1.0]",
            "--k",
            "5",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let ids: Vec<String> = serde_json::from_slice(&output).unwrap();
    assert_eq!(ids, ["a"]); // term 7 only in "a"
}

#[test]
fn cli_searches_late_interaction_float16_end_to_end() {
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
            "--named-vector",
            "tokens:2:inner-product:late-interaction:float16",
        ])
        .assert()
        .success();
    let records = dir.path().join("records.json");
    fs::write(
        &records,
        r#"[{"id":"best","vector":[0.0,0.0],"named_vectors":{"tokens":[[1.0,0.0],[0.0,1.0]]}},
            {"id":"near","vector":[1.0,0.0],"named_vectors":{"tokens":[[0.7,0.0],[0.0,0.7]]}}]"#,
    )
    .unwrap();
    Command::cargo_bin("borsuk")
        .unwrap()
        .args(["add", "--uri", &uri, "--input", records.to_str().unwrap()])
        .assert()
        .success();

    let output = Command::cargo_bin("borsuk")
        .unwrap()
        .args([
            "search-late-interaction",
            "--uri",
            &uri,
            "--name",
            "tokens",
            "--query",
            "[[1.0,0.0],[0.0,1.0]]",
            "--k",
            "2",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let hits: Vec<serde_json::Value> = serde_json::from_slice(&output).unwrap();
    assert_eq!(hits[0]["id"], "best");
    assert_eq!(hits[1]["id"], "near");
}
