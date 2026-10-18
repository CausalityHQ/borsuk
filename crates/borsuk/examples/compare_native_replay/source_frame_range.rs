//! Source-only byte-cost replay. All trace observations are provisional until sealed EOF.
use super::{CompletedConfig, CompletedInput, Result, plus, require};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, path::Path};

#[allow(dead_code)]
#[path = "../check_source_frame.rs"]
mod frames;

pub(super) const REPORT_SCHEMA: &str = "borsuk-source-frame-range-replay-v1";
const CONFIG_SCHEMA: &str = "borsuk-source-frame-range-replay-config-v1";
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    authority: CompletedInput,
    completed_config: CompletedInput,
    source_config: CompletedInput,
    max_replay_metadata_bytes: u64,
}
fn n(v: &Value) -> Result<u64> {
    v.as_u64().ok_or_else(|| "range replay integer".into())
}

fn pinned_config<T: serde::de::DeserializeOwned>(a: &CompletedInput) -> Result<T> {
    require(
        a.path.is_absolute() && a.bytes > 0 && a.bytes <= 65536,
        "range replay config artifact bounds",
    )?;
    let (value, bytes) = super::read_config(&a.path, &a.sha256)?;
    require(bytes == a.bytes, "range replay config artifact length")?;
    Ok(value)
}
fn bind(c: &CompletedConfig, authority: &Value, source: &Value, config: &Config) -> Result<()> {
    require(
        authority["schema"] == "borsuk-source-frame-range-root-authority-v1"
            && authority["requests_truth_f32_opened"] == false,
        "range replay root authority schema",
    )?;
    for (key, artifact) in [
        ("completed_config", &config.completed_config),
        ("source_config", &config.source_config),
    ] {
        require(
            authority[key]["path"] == json!(artifact.path)
                && authority[key]["sha256"] == artifact.sha256,
            "range replay root config artifact pins",
        )?;
    }
    require(
        authority["input"] == serde_json::to_value(&c.input)?
            && authority["expected_identity"] == c.expected_identity
            && authority["expected_bound_inputs"] == c.expected_bound_inputs
            && authority["source_inputs"] == *source,
        "range replay exact root input bindings",
    )?;
    let i = &c.expected_bound_inputs;
    require(
        c.schema == super::SCALE_CONFIG_SCHEMA
            && c.expected_identity["schema"] == super::current_v10::CONCURRENT_RESULT_SCHEMA
            && i["execution"]["mode"] == "full"
            && i["count"] == 1000
            && i["source_layout"] == json!({"layout":"flat_records"})
            && source["schema"] == "source-frame-config-v1"
            && source["generation"]["sha256"] == i["generation_root_sha256"]
            && source["rows"] == i["rows"]
            && source["dimensions"] == i["dimensions"],
        "range replay trace/SOURCE generation and geometry",
    )?;
    super::current_v10::validate_completed(c)?;
    Ok(())
}
fn metadata_bound(rows: u64) -> Result<u64> {
    // Both size vectors, per-query scratch sets, fixed report cap and config buffers.
    // Strict completed-reader allocation is additional and retains its existing admission model.
    let frames = rows.checked_add(255).ok_or("frame count overflow")? / 256;
    require(
        frames > 0 && frames <= 1_000_000,
        "range replay frame count",
    )?;
    plus(
        2 * super::REPORT_CAP + 3 * 65536,
        frames.checked_mul(64).ok_or("replay metadata overflow")?,
    )
}
fn check_metadata_cap(rows: u64, cap: u64) -> Result<()> {
    require(
        cap <= 64 * 1024 * 1024 && metadata_bound(rows)? <= cap,
        "range replay metadata cap before SOURCE IO",
    )
}
#[derive(Debug, PartialEq)]
struct Cost {
    original_bytes: u64,
    original_logical_gets: u64,
    framed_bytes: u64,
    deduplicated_frame_bytes_lowerbound: u64,
    touched_frame_occurrences: u64,
    unique_frames: u64,
}
fn cost(ranges: &[(u64, u64)], sizes: &[usize], rows: u64, width: u64) -> Result<Cost> {
    require(
        rows > 0 && width > 0 && sizes.len() as u64 == rows.div_ceil(256),
        "range replay ordered table geometry",
    )?;
    let object_bytes = rows.checked_mul(width).ok_or("source geometry overflow")?;
    let frame_bytes = width.checked_mul(256).ok_or("frame geometry overflow")?;
    let mut out = Cost {
        original_bytes: 0,
        original_logical_gets: ranges.len() as u64,
        framed_bytes: 0,
        deduplicated_frame_bytes_lowerbound: 0,
        touched_frame_occurrences: 0,
        unique_frames: 0,
    };
    let mut unique = BTreeSet::new();
    for &(start, end) in ranges {
        require(
            start < end && end <= object_bytes,
            "range replay object byte interval",
        )?;
        out.original_bytes = plus(out.original_bytes, end - start)?;
        for index in start / frame_bytes..=(end - 1) / frame_bytes {
            let size = *sizes
                .get(index as usize)
                .ok_or("range replay frame index")? as u64;
            let actual_rows = 256.min(rows - index * 256);
            require(
                size >= borsuk::source_frame::HEADER_BYTES as u64
                    && size
                        <= plus(
                            actual_rows.checked_mul(width).ok_or("tail overflow")?,
                            borsuk::source_frame::HEADER_BYTES as u64,
                        )?,
                "range replay encoded frame bound",
            )?;
            out.framed_bytes = plus(out.framed_bytes, size)?;
            out.touched_frame_occurrences = plus(out.touched_frame_occurrences, 1)?;
            unique.insert(index);
        }
    }
    for index in &unique {
        out.deduplicated_frame_bytes_lowerbound = plus(
            out.deduplicated_frame_bytes_lowerbound,
            sizes[*index as usize] as u64,
        )?;
    }
    out.unique_frames = unique.len() as u64;
    Ok(out)
}
fn distribution(values: &[u64]) -> Result<Value> {
    require(!values.is_empty(), "replay empty distribution")?;
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let total = values.iter().try_fold(0u64, |sum, v| plus(sum, *v))?;
    Ok(
        json!({"total":total, "p50":sorted[(sorted.len()*50).div_ceil(100)-1],
        "p95":sorted[(sorted.len()*95).div_ceil(100)-1], "max":sorted[sorted.len()-1]}),
    )
}
pub(super) fn reduce(path: &Path, sha: &str, admit_only: bool) -> Result<Value> {
    let (config, config_bytes): (Config, _) = super::read_config(path, sha)?;
    require(config.schema == CONFIG_SCHEMA, "range replay config schema")?;
    let authority: Value = pinned_config(&config.authority)?;
    let completed: CompletedConfig = pinned_config(&config.completed_config)?;
    let source: Value = pinned_config(&config.source_config)?;
    bind(&completed, &authority, &source, &config)?;
    let rows = n(&source["rows"])?;
    let dimensions = n(&source["dimensions"])?;
    let width = super::source_record_bytes(usize::try_from(dimensions)?)? as u64;
    let bound = metadata_bound(rows)?;
    check_metadata_cap(rows, config.max_replay_metadata_bytes)?;
    // First full strict read: no codec SOURCE open until authenticated seal and EOF.
    let admitted = super::read_run_observed(
        &completed.input.path,
        &completed.input.sha256,
        Some(&completed),
        |line, _, _| {
            reconstructed_ranges(line, &completed)?;
            Ok(())
        },
    )?;
    let (mut measurement, sizes) = frames::replay_table(
        &config.source_config.path,
        config.source_config.bytes,
        &config.source_config.sha256,
        &source,
        completed.expected_bound_inputs["generation_root_sha256"]
            .as_str()
            .ok_or("generation pin")?,
        rows,
        dimensions,
        !admit_only,
    )
    .map_err(|e| e.to_string())?;

    require(
        measurement["record_bytes"] == width,
        "range replay codec/trace record width",
    )?;
    if admit_only {
        recheck(path, sha, &config, &completed, &admitted.file_identity)?;
        return Ok(
            json!({"schema":REPORT_SCHEMA,"status":"SOURCE_ONLY_ADMITTED",
            "complete":true,"config_path":path,"config_sha256":sha,"config_bytes":config_bytes,
            "authority":config.authority,"completed_config":config.completed_config,
            "source_config":config.source_config,"count":1000,"record_bytes":width,
            "checked_replay_metadata_bound_bytes":bound,"source_authentication":measurement,
            "encoding_executed":false,"cost_replay_executed":false,
            "requests_truth_f32_opened":false,"native_ann_called":false,"qualified":false}),
        );
    }
    let mut queries = Vec::with_capacity(1000);
    let mut original_bytes = Vec::with_capacity(1000);
    let mut gets = Vec::with_capacity(1000);
    let mut framed_bytes = Vec::with_capacity(1000);
    let mut dedup = Vec::with_capacity(1000);
    let mut occurrences = Vec::with_capacity(1000);
    let mut unique = Vec::with_capacity(1000);
    let mut range_hash = Sha256::new();
    let second = super::read_run_observed_with_identity(
        &completed.input.path,
        &completed.input.sha256,
        Some(&completed),
        Some((admitted.file_identity.dev, admitted.file_identity.ino)),
        |line, q, _| {
            let physical = reconstructed_ranges(line, &completed)?;
            let costs = cost(&physical, &sizes, rows, width)?;
            range_hash.update((q.ordinal as u64).to_le_bytes());
            range_hash.update((physical.len() as u64).to_le_bytes());
            for &(start, end) in &physical {
                range_hash.update(start.to_le_bytes());
                range_hash.update(end.to_le_bytes());
            }
            queries.push(json!({"ordinal":q.ordinal,"original_object_bytes":costs.original_bytes,
                "original_logical_gets":costs.original_logical_gets,
                "primary_framed_bytes":costs.framed_bytes,"primary_logical_gets":costs.original_logical_gets,
                "touched_frame_occurrences":costs.touched_frame_occurrences,"unique_frames":costs.unique_frames,
                "deduplicated_frame_bytes_lowerbound":costs.deduplicated_frame_bytes_lowerbound}));
            original_bytes.push(costs.original_bytes);
            gets.push(costs.original_logical_gets);
            framed_bytes.push(costs.framed_bytes);
            dedup.push(costs.deduplicated_frame_bytes_lowerbound);
            occurrences.push(costs.touched_frame_occurrences);
            unique.push(costs.unique_frames);
            Ok(())
        },
    )?;
    require(
        second.file_identity == admitted.file_identity && queries.len() == 1000,
        "range replay same completed file and population",
    )?;
    let mut frame_table_hash = Sha256::new();
    for size in &sizes {
        frame_table_hash.update((*size as u64).to_le_bytes());
    }
    // Both ordered witness and actual codec per-call timings stay attached to the measured bytes.
    measurement["mode"] = json!("range-replay-frame-scan");
    let mut report = json!({"schema":REPORT_SCHEMA,"status":"SOURCE_ONLY_COST_REPLAY","complete":true,
        "config_path":path,"config_sha256":sha,"config_bytes":config_bytes,
        "authority":config.authority,"completed_config":config.completed_config,"source_config":config.source_config,
        "identity":completed.expected_identity,"bound_inputs":completed.expected_bound_inputs,
        "count":queries.len(),"record_bytes":width,"frame_rows":256,"frame_sizes_in_original_order":sizes,
        "ordered_frame_sizes_le_u64_sha256":format!("{:x}",frame_table_hash.finalize()),
        "reconstructed_ranges_ordinal_count_start_end_le_u64_sha256":format!("{:x}",range_hash.finalize()),
        "frame_scan":measurement,"aggregate":{"original_object_bytes":distribution(&original_bytes)?,
        "original_logical_gets":distribution(&gets)?,"primary_framed_bytes":distribution(&framed_bytes)?,
        "primary_logical_gets":distribution(&gets)?,
        "deduplicated_frame_bytes_lowerbound":distribution(&dedup)?,
        "touched_frame_occurrences":distribution(&occurrences)?,"unique_frames":distribution(&unique)?},
        "queries":queries,"metadata":{"checked_replay_bound_bytes":bound,
        "max_replay_metadata_bytes":config.max_replay_metadata_bytes,
        "retained_ordered_size_table_bytes":sizes.len()*std::mem::size_of::<usize>(),
        "strict_reader_model_is_additional":true,"report_cap_bytes":super::REPORT_CAP},
        "percentile_method":"nearest_rank","range_origin":"reconstructed_from_pinned_shared_cover", "primary_policy":"Each reconstructed original planned logical range read transfers every complete touched frame, including repeated frames across original ranges. Exact original logical GET and verified byte counts are conserved; bridge pages and clipped final frames included. Full trace has no recorded diagnostic physical spans.",
        "directory_policy":"Actual 25-byte frame headers included. Hypothetical 64+24*N directory storage reported separately in frame_scan; directory fetch is not measured or added to primary.",
        "deduplication_policy":"Per-query unique-frame byte lowerbound only; no GET, cache, merge, serving or latency claim.",
        "timing_policy":"Observed encode/decode whole-call process CPU and wall times describe one local whole-plane scan. They are not query latency or network transfer measurements.",
        "source_source_sha256":format!("{:x}",Sha256::digest(include_bytes!("../check_source_frame.rs"))),
        "adapter_source_sha256":format!("{:x}",Sha256::digest(include_bytes!("source_frame_range.rs"))),
        "cover_source_sha256":format!("{:x}",Sha256::digest(include_bytes!("../../src/budgeted_page_rank.rs"))),
        "codec_source_sha256":format!("{:x}",Sha256::digest(include_bytes!("../../src/source_frame.rs"))),
        "native_ann_called":false,"requests_truth_f32_opened":false,"production_change":false,
        "claims_query_latency":false,"claims_qps":false,"claims_recall":false,
        "wire_bytes":null,"billed_bytes":null,"billed_requests":null,"vendor_win":false,
        "cache_state":"uncontrolled local input; cgroup memory includes attributed file cache",
        "qualified":false,"root_actual_input_admission_canary_and_scientific_decision_required":true});
    report["final_resources"] = frames::resource_snapshot();
    super::source_report_cap(&report)?;
    recheck(path, sha, &config, &completed, &second.file_identity)?;
    Ok(report)
}

fn reconstructed_ranges(line: &[u8], completed: &CompletedConfig) -> Result<Vec<(u64, u64)>> {
    let raw: Value = serde_json::from_slice(line)?;
    let i = &completed.expected_bound_inputs;
    require(
        i["source_layout"] == json!({"layout":"flat_records"}),
        "range replay flat SOURCE",
    )?;
    let ranked = raw["trace"]["ranked_candidate_pages"]
        .as_array()
        .ok_or("range replay discovery pages")?;
    let pages = ranked
        .iter()
        .map(|v| Ok(usize::try_from(n(v)?)?))
        .collect::<Result<BTreeSet<_>>>()?;
    let rows = usize::try_from(n(&i["rows"])?)?;
    require(
        !pages.is_empty()
            && pages.len() == ranked.len()
            && pages.iter().all(|page| *page < rows.div_ceil(256)),
        "range replay unique nonempty in-bounds discovery pages before SOURCE IO",
    )?;
    let width = super::source_record_bytes(usize::try_from(n(&i["dimensions"])?)?)?;
    let gets = usize::try_from(n(&i["source_get_budget"])?)?;
    let (cover, bytes) = super::source_cover::cover_pages(&pages, rows, width, 256, gets)
        .map_err(|e| format!("range replay shared cover {e:?}"))?;
    require(
        raw["source_nomination_skipped"] == false
            && !cover.is_empty()
            && raw["charges"]["source"]
                == json!({"submitted_gets":cover.len(),
            "verified_bytes":bytes,"failed_gets":0}),
        "range replay original charge parity before SOURCE IO",
    )?;
    Ok(cover
        .into_iter()
        .map(|r| (r.start as u64, r.end as u64))
        .collect())
}
fn recheck(
    path: &Path,
    sha: &str,
    config: &Config,
    completed: &CompletedConfig,
    identity: &super::FileIdentity,
) -> Result<()> {
    require(
        super::file_identity(&super::open_input(&completed.input.path)?)? == *identity,
        "range replay input changed before publication",
    )?;
    let _: Config = super::read_config(path, sha)?.0;
    let _: Value = pinned_config(&config.authority)?;
    let _: CompletedConfig = pinned_config(&config.completed_config)?;
    let _: Value = pinned_config(&config.source_config)?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_frame_range_handcount_alignment_overlap_and_tail() {
        let sizes = [100, 200, 34]; // Last frame has one row of nine bytes +25-byte header.
        let cases = [
            (vec![(0, 2304)], 2304, 100, 100, 1),
            (vec![(2303, 2305)], 2, 300, 300, 2),
            (vec![(0, 9), (9, 18)], 18, 200, 100, 2),
            (vec![(4608, 4617)], 9, 34, 34, 1),
            (vec![(2304, 4617)], 2313, 234, 234, 2),
            (vec![(0, 4617)], 4617, 334, 334, 3),
        ];
        for (ranges, bytes, framed, dedup, occurrences) in cases {
            let c = cost(&ranges, &sizes, 513, 9).unwrap();
            assert_eq!(
                (
                    c.original_bytes,
                    c.framed_bytes,
                    c.deduplicated_frame_bytes_lowerbound,
                    c.touched_frame_occurrences
                ),
                (bytes, framed, dedup, occurrences)
            );
        }
        assert!(cost(&[(4616, 4618)], &sizes, 513, 9).is_err());
        assert!(cost(&[(0, 1)], &[24, 200, 34], 513, 9).is_err());
        assert!(cost(&[(0, 1)], &sizes[..2], 513, 9).is_err());
        assert_ne!(
            cost(&[(0, 1)], &sizes, 513, 9).unwrap().framed_bytes,
            cost(&[(0, 1)], &[200, 100, 34], 513, 9)
                .unwrap()
                .framed_bytes
        );
    }
    #[test]
    fn source_frame_range_raw_fallback_header_expansion() {
        let size = 256 * 9 + 25;
        let c = cost(&[(0, 256 * 9)], &[size], 256, 9).unwrap();
        assert_eq!(c.framed_bytes, c.original_bytes + 25);
        assert_eq!(
            distribution(&[10, 20, 30, 40]).unwrap(),
            json!({"total":100,"p50":20,"p95":40,"max":40})
        );
        assert_eq!(
            metadata_bound(1_000_000).unwrap(),
            2 * super::super::REPORT_CAP + 3 * 65536 + 3907 * 64
        );
    }
    #[test]
    fn source_frame_range_metadata_cap_minus_one() {
        let bound = metadata_bound(1_000_000).unwrap();
        assert!(check_metadata_cap(1_000_000, bound).is_ok());
        assert!(check_metadata_cap(1_000_000, bound - 1).is_err());
        assert!(check_metadata_cap(0, bound).is_err());
        assert!(check_metadata_cap(u64::MAX, bound).is_err());
    }

    #[test]
    fn source_frame_range_fixed_qps_json_roundtrip_exact() {
        let expected = 2_f64 * 1e9 / 15_309_257_f64;
        assert_eq!(expected.to_bits(), 0x4060547a2a799f46);
        let parsed: Value = serde_json::from_str("130.63991283182457").unwrap();
        assert_eq!(parsed.as_f64().unwrap().to_bits(), expected.to_bits());
        let cohort = json!({"phase":"query_cohort","attempted_count":2,"successful_count":2,
            "max_active_queries":1,"elapsed_ns":15309257,"sum_query_wall_ns":14112923,
            "process_cpu_start_ns":10,"process_cpu_end_ns":20,"process_cpu_ns":10,
            "qps":expected,"transport":{"stage":"query_cohort","ordinal":null,"before":null,"after":null},
            "truth_opened":false});
        let parsed: Value = serde_json::from_slice(&serde_json::to_vec(&cohort).unwrap()).unwrap();
        super::super::current_v10::validate_cohort(&parsed).unwrap();
        let mut bad = parsed.clone();
        bad["qps"] = json!(f64::from_bits(expected.to_bits() - 1));
        assert!(super::super::current_v10::validate_cohort(&bad).is_err());
    }
    fn descriptor(path: &Path) -> Value {
        let bytes = std::fs::read(path).unwrap();
        json!({"path":path,"bytes":bytes.len(),"sha256":format!("{:x}",Sha256::digest(&bytes))})
    }
    fn write_json(dir: &Path, name: &str, value: &Value) -> Value {
        let path = dir.join(name);
        std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        descriptor(&path)
    }
    fn fixture_config(
        dir: &Path,
        trace: &Path,
        source: Value,
        cap: u64,
        name: &str,
    ) -> (std::path::PathBuf, String) {
        let body = std::fs::read_to_string(trace).unwrap();
        let rows = body
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let completed = json!({"schema":super::super::SCALE_CONFIG_SCHEMA,"input":descriptor(trace),
            "expected_identity":rows[0],"expected_bound_inputs":rows[1]});
        let source_descriptor = write_json(dir, &format!("{name}-source.json"), &source);
        let completed_descriptor = write_json(dir, &format!("{name}-completed.json"), &completed);
        let authority = json!({"schema":"borsuk-source-frame-range-root-authority-v1",
            "requests_truth_f32_opened":false,"input":completed["input"],
            "expected_identity":completed["expected_identity"],"expected_bound_inputs":completed["expected_bound_inputs"],
            "source_inputs":source,"completed_config":completed_descriptor,"source_config":source_descriptor});
        let authority_descriptor = write_json(dir, &format!("{name}-authority.json"), &authority);
        let config = json!({"schema":CONFIG_SCHEMA,"authority":authority_descriptor,
            "completed_config":completed_descriptor,"source_config":source_descriptor,"max_replay_metadata_bytes":cap});
        let a = write_json(dir, &format!("{name}-replay.json"), &config);
        (
            a["path"].as_str().unwrap().into(),
            a["sha256"].as_str().unwrap().into(),
        )
    }
    #[test]
    fn source_frame_range_full1000_producer_no_diagnostic_and_pre_source_refusals() {
        let (dir, trace) = super::super::scale_native_runner::source_frame_range_reducer_fixture();
        let generation = dir.path().join("generation");
        let native = std::fs::read_to_string(&trace).unwrap();
        let mut rows = native
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.iter().filter(|r| r["phase"] == "query").count(), 1000);
        assert!(
            rows.iter()
                .filter(|r| r["phase"] == "query")
                .all(|r| r.get("diagnostic").is_none())
        );
        let input = &rows[1];
        let source = json!({"schema":"source-frame-config-v1",
            "generation":descriptor(&generation.join("manifest.json")),
            "plane":descriptor(&generation.join("plane/manifest.json")),
            "digests":descriptor(&generation.join("plane/page_digests.bin")),
            "records":descriptor(&generation.join("plane/records.bin")),
            "rows":input["rows"],"dimensions":input["dimensions"],
            "max_source_bytes":16777216,"max_metadata_bytes":16777216,"max_frame_bytes":1048576});
        let (config, pin) = fixture_config(dir.path(), &trace, source.clone(), 16777216, "good");
        let admission = reduce(&config, &pin, true).unwrap();
        assert_eq!(admission["status"], "SOURCE_ONLY_ADMITTED");
        assert_eq!(admission["encoding_executed"], false);
        let measured = reduce(&config, &pin, false).unwrap();
        assert_eq!(measured["count"], 1000);
        assert_eq!(
            measured["range_origin"],
            "reconstructed_from_pinned_shared_cover"
        );
        assert_eq!(
            measured["aggregate"]["original_logical_gets"]["total"],
            1000
        );
        assert_eq!(
            measured["aggregate"]["original_object_bytes"]["total"],
            8_448_000
        );
        assert_eq!(
            measured["aggregate"]["primary_framed_bytes"]["total"]
                .as_u64()
                .unwrap(),
            1000 * measured["frame_sizes_in_original_order"][0]
                .as_u64()
                .unwrap()
        );
        assert_eq!(
            measured["frame_sizes_in_original_order"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let output = dir.path().join("create-only-report.json");
        assert!(
            super::super::execute_report(&output, REPORT_SCHEMA, || reduce(&config, &pin, true))
                .unwrap()
        );
        assert!(
            super::super::execute_report(&output, REPORT_SCHEMA, || panic!(
                "occupied output ran adapter"
            ))
            .is_err()
        );
        let mut missing = source.clone();
        missing["records"]["path"] = json!(dir.path().join("never-open-source"));
        let bound = metadata_bound(n(&source["rows"]).unwrap()).unwrap();
        let (config, pin) = fixture_config(dir.path(), &trace, missing.clone(), bound - 1, "cap");
        assert!(
            reduce(&config, &pin, false)
                .unwrap_err()
                .to_string()
                .contains("metadata cap before SOURCE IO")
        );
        let mut mismatch = missing.clone();
        mismatch["generation"]["sha256"] = json!("f".repeat(64));
        let (config, pin) = fixture_config(dir.path(), &trace, mismatch, 16777216, "misbound");
        assert!(
            reduce(&config, &pin, false)
                .unwrap_err()
                .to_string()
                .contains("generation and geometry")
        );
        let mut mismatch = missing.clone();
        mismatch["rows"] = json!(33);
        let (config, pin) = fixture_config(dir.path(), &trace, mismatch, 16777216, "rows");
        assert!(
            reduce(&config, &pin, false)
                .unwrap_err()
                .to_string()
                .contains("generation and geometry")
        );
        let mut mismatch = missing.clone();
        mismatch["dimensions"] = json!(1);
        let (config, pin) = fixture_config(dir.path(), &trace, mismatch, 16777216, "dimension");
        assert!(
            reduce(&config, &pin, false)
                .unwrap_err()
                .to_string()
                .contains("generation and geometry")
        );
        let clean_rows = rows.clone();
        for (index, field) in ["submitted_gets", "verified_bytes", "failed_gets"]
            .iter()
            .enumerate()
        {
            rows = clean_rows.clone();
            let query = rows.iter_mut().find(|r| r["phase"] == "query").unwrap();
            query["charges"]["source"][*field] =
                json!(query["charges"]["source"][*field].as_u64().unwrap() + 1);
            super::super::tests::authenticate(&mut rows);
            let changed = dir.path().join(format!("mutated-{index}.jsonl"));
            std::fs::write(&changed, super::super::tests::encode(&rows)).unwrap();
            let (config, pin) = fixture_config(
                dir.path(),
                &changed,
                missing.clone(),
                16777216,
                &format!("counter-{index}"),
            );
            let error = reduce(&config, &pin, false).unwrap_err().to_string();
            assert!(
                !error.contains("No such file"),
                "SOURCE opened before charge refusal: {error}"
            );
        }

        for (name, pages) in [
            ("out-of-range", json!([3907])),
            ("duplicate", json!([0, 0])),
            ("empty", json!([])),
        ] {
            rows = clean_rows.clone();
            rows.iter_mut().find(|r| r["phase"] == "query").unwrap()["trace"]["ranked_candidate_pages"] =
                pages;
            super::super::tests::authenticate(&mut rows);
            let changed = dir.path().join(format!("{name}.jsonl"));
            std::fs::write(&changed, super::super::tests::encode(&rows)).unwrap();
            let (config, pin) =
                fixture_config(dir.path(), &changed, missing.clone(), 16777216, name);
            for admit in [true, false] {
                let error = reduce(&config, &pin, admit).unwrap_err().to_string();
                assert!(
                    !error.contains("No such file"),
                    "SOURCE opened before page refusal: {error}"
                );
            }
        }
    }

    #[test]
    fn source_frame_range_shared_cover_bridge_tail_and_page_bounds() {
        let c: CompletedConfig=serde_json::from_value(json!({
            "schema":super::super::SCALE_CONFIG_SCHEMA,"input":{"path":"/unused","bytes":1,"sha256":"0".repeat(64)},
            "expected_identity":{},"expected_bound_inputs":{"rows":513,"dimensions":3,
            "source_layout":{"layout":"flat_records"},"source_get_budget":1}})).unwrap();
        let raw = json!({"source_nomination_skipped":false,"trace":{"ranked_candidate_pages":[0,2]},
            "charges":{"source":{"submitted_gets":1,"verified_bytes":4617,"failed_gets":0}}});
        let ranges = reconstructed_ranges(&serde_json::to_vec(&raw).unwrap(), &c).unwrap();
        assert_eq!(ranges, [(0, 4617)]);
        assert_eq!(
            cost(&ranges, &[100, 200, 34], 513, 9).unwrap().framed_bytes,
            334
        );
        let mut million = c;
        million.expected_bound_inputs["rows"] = json!(1_000_000);
        let mut invalid = raw;
        invalid["trace"]["ranked_candidate_pages"] = json!([3907]);
        assert!(reconstructed_ranges(&serde_json::to_vec(&invalid).unwrap(), &million).is_err());
    }
}
