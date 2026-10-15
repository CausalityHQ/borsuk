//! Current-format panel validation. Historical result readers remain unchanged.
use super::{Result, fields, require};
use serde_json::{Value, json};
use std::io::Cursor;

pub(super) const REPORT_SCHEMA: &str = "borsuk-current-v9-panel-reader-v1";
pub(super) const PARITY_SCHEMA: &str = "borsuk-v9-historical-count-parity-v1";
pub(super) const ORACLE_SCHEMA: &str = "borsuk-v9-source-cover-oracle-v1";

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OracleConfig {
    schema: String,
    panel: PanelConfig,
    generation_manifest: super::CompletedInput,
    plane_manifest: super::CompletedInput,
    records: super::CompletedInput,
    mean: super::CompletedInput,
    source_digests: super::CompletedInput,
    requests: super::CompletedInput,
}

pub(super) fn reduce_oracle(path: &std::path::Path, sha: &str) -> Result<Value> {
    use borsuk::{rotated_two_bit::RotatedTwoBitCodec, sq8_page_authority::PageAuthority};
    let (c, config_bytes): (OracleConfig, _) = super::read_config(path, sha)?;
    require(
        c.schema == "borsuk-v9-source-cover-oracle-config-v1"
            && c.panel.schema == "borsuk-current-v9-panel-reader-config-v1"
            && c.panel.completed.schema == super::SCALE_CONFIG_SCHEMA
            && c.panel.max_payload_bytes >= 384 * 1024 * 1024,
        "SOURCE oracle config/payload admission",
    )?;
    let inputs = &c.panel.completed.expected_bound_inputs;
    require(
        c.generation_manifest.sha256 == inputs["generation_root_sha256"]
            && c.generation_manifest.bytes <= 65536
            && c.plane_manifest.bytes <= 65536
            && c.records.bytes == 264000000
            && c.mean.bytes == 4096
            && c.source_digests.bytes == 1000000
            && c.requests.bytes == 4096000
            && c.requests.sha256 == inputs["requests_sha256"],
        "SOURCE oracle descriptor binding",
    )?;
    for (pin, cap) in [
        (&c.generation_manifest, 65536),
        (&c.plane_manifest, 65536),
        (&c.records, 264000000),
        (&c.mean, 4096),
        (&c.source_digests, 1000000),
        (&c.requests, 4096000),
    ] {
        super::header_pin(pin, cap)?;
    }
    let panel = read_candidate(
        &c.panel.completed.input,
        &c.panel.completed.expected_identity,
        inputs,
        &[0, 1, 127, 255, 511, 767, 998, 999],
        c.panel.max_payload_bytes,
    )?;
    let (root, root_snapshot) = super::header_json(&c.generation_manifest)?;
    let (plane_bytes, plane_snapshot) =
        super::loss_binary(&c.plane_manifest, c.plane_manifest.bytes)?;
    let plane: borsuk::two_bit_source::SourcePlaneReceipt = serde_json::from_slice(&plane_bytes)?;
    require(
        root["plane_manifest_sha256"] == c.plane_manifest.sha256
            && root["sq8_object_sha256"] == inputs["native_sq8_sha256"]
            && plane.rows == 1000000
            && plane.dimensions == 1024
            && plane.record_bytes == 264
            && plane.records_sha256 == c.records.sha256
            && plane.mean_sha256 == c.mean.sha256
            && plane.page_digest_sha256 == c.source_digests.sha256
            && plane.source_sha256 == inputs["native_source_sha256"]
            && plane.source_order_sha256 == inputs["native_order_sha256"]
            && plane.sq8_sha256 == inputs["native_sq8_sha256"]
            && !plane.query_or_truth_used,
        "SOURCE oracle authenticated plane/root",
    )?;
    let (digests, digest_snapshot) = super::loss_binary(&c.source_digests, 1000000)?;
    let authority = PageAuthority::load_two_bit(
        &plane_bytes,
        &c.plane_manifest.sha256,
        root["generation"]
            .as_u64()
            .ok_or("SOURCE oracle generation")?,
        &digests,
    )
    .map_err(|e| format!("SOURCE oracle authority {e:?}"))?;
    let (records, records_snapshot) = super::loss_binary(&c.records, 264000000)?;
    let (mean_bytes, mean_snapshot) = super::loss_binary(&c.mean, 4096)?;
    authority
        .verify_payload(0, 31249, &records)
        .map_err(|e| format!("SOURCE oracle payload {e:?}"))?;
    let mean = mean_bytes
        .chunks_exact(4)
        .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
        .collect::<Vec<_>>();
    require(
        mean.iter().all(|v| v.is_finite()),
        "SOURCE oracle finite mean",
    )?;
    let codec = RotatedTwoBitCodec::new(&mean, plane.seed)?;
    let scratch = RotatedTwoBitCodec::required_query_scratch_bytes(1024)?;
    let (requests, request_snapshot) = super::loss_binary(&c.requests, 4096000)?;
    let mut verified = 0usize;
    let run = super::read_run_observed(
        &c.panel.completed.input.path,
        &c.panel.completed.input.sha256,
        Some(&c.panel.completed),
        |line, q, _| {
            let super::UniqueJson(raw) = serde_json::from_slice(line)?;
            let start = q
                .ordinal
                .checked_mul(4096)
                .ok_or("SOURCE oracle request offset")?;
            let bytes = requests
                .get(start..start + 4096)
                .ok_or("SOURCE oracle request bounds")?;
            let query = bytes
                .chunks_exact(4)
                .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
                .collect::<Vec<_>>();
            require(
                query.iter().all(|v| v.is_finite()),
                "SOURCE oracle finite request",
            )?;
            let prepared = codec.prepare_query(&query, scratch)?;
            validate_source_cover(&raw, 1000000, 1024, 40 * 1024 * 1024, |row| {
                let start = row.checked_mul(264).ok_or("SOURCE oracle row offset")?;
                let record = records
                    .get(start..start + 264)
                    .ok_or("SOURCE oracle record bounds")?;
                Ok(prepared.score(record)?)
            })?;
            verified += 1;
            Ok(())
        },
    )?;
    require(
        verified == 8 && run.samples.len() == 8,
        "SOURCE oracle complete selected panel",
    )?;
    for snapshot in [
        &root_snapshot,
        &plane_snapshot,
        &records_snapshot,
        &mean_snapshot,
        &digest_snapshot,
        &request_snapshot,
    ] {
        snapshot.recheck()?;
    }
    Ok(
        json!({"schema":ORACLE_SCHEMA,"status":"EXACT_RECORDED_SOURCE_COVER","complete":true,
        "reduction_config_sha256":sha,"reduction_config_bytes":config_bytes,
        "panel_input_sha256":c.panel.completed.input.sha256,"panel_input_bytes":c.panel.completed.input.bytes,
        "selected_count":verified,"recall_numerator":panel["recall_numerator"],"recall_denominator":80,
        "recorded_source_frontier_only":true,"independent_router_frontier_qualified":false,
        "historical_rank_parity_qualified":false,"producer_provenance_qualified":false,
        "cold_performance_claim":false,"vendor_win_claim":false}),
    )
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ParityConfig {
    schema: String,
    panel: PanelConfig,
    historical_count_report: super::CompletedInput,
}

pub(super) fn reduce_parity(config: &std::path::Path, sha: &str) -> Result<Value> {
    let (c, config_bytes): (ParityConfig, _) = super::read_config(config, sha)?;
    require(
        c.schema == "borsuk-v9-historical-count-parity-config-v1"
            && c.panel.schema == "borsuk-current-v9-panel-reader-config-v1"
            && c.panel.completed.schema == super::SCALE_CONFIG_SCHEMA,
        "v9 historical parity config schema",
    )?;
    // Covers the capped historical JSON tree plus the panel reader workspace.
    require(
        c.panel.max_payload_bytes >= 160 * 1024 * 1024
            && c.historical_count_report.bytes > 0
            && c.historical_count_report.bytes <= 16 * 1024 * 1024
            && c.historical_count_report.sha256
                == "31dfa30cbf176254e7bec7cc8eb5288071f85cfbb9bb98455b61b3c87f5ec786",
        "v9 historical report pin/payload",
    )?;
    let panel = read_candidate(
        &c.panel.completed.input,
        &c.panel.completed.expected_identity,
        &c.panel.completed.expected_bound_inputs,
        &[0, 1, 127, 255, 511, 767, 998, 999],
        c.panel.max_payload_bytes,
    )?;
    let (bytes, snapshot) =
        super::loss_binary(&c.historical_count_report, c.historical_count_report.bytes)?;
    let super::UniqueJson(historical) = serde_json::from_slice(&bytes)?;
    super::sq8_subpage_shortlist::validate_count_curve_report(&historical)?;
    compare_historical(&panel, &historical)?;
    snapshot.recheck()?;
    Ok(
        json!({"schema":PARITY_SCHEMA,"status":"EXACT_SELECTED_RANK_PARITY","complete":true,
        "reduction_config_sha256":sha,"reduction_config_bytes":config_bytes,
        "panel_input_sha256":c.panel.completed.input.sha256,"panel_input_bytes":c.panel.completed.input.bytes,
        "selected_count":8,"recall_denominator":80,"recall_numerator":panel["recall_numerator"],
        "historical_count_report_sha256":c.historical_count_report.sha256,
        "exact_range_oracle_qualified":false,"producer_provenance_qualified":false,
        "cold_performance_claim":false,"vendor_win_claim":false}),
    )
}

fn compare_historical(panel: &Value, historical: &Value) -> Result<()> {
    require(
        historical["shortlist_sizes"] == json!([256, 512, 768, 1024]),
        "historical 256 arm",
    )?;
    let queries = panel["queries"].as_array().ok_or("panel queries")?;
    require(queries.len() == 8, "historical panel length")?;
    for (query, ordinal) in queries.iter().zip([0, 1, 127, 255, 511, 767, 998, 999]) {
        require(query["ordinal"] == ordinal, "historical panel ordinal")?;
        let old = &historical["queries"][ordinal];
        require(
            old["ordinal"] == ordinal
                && query["hits10"] == old["arms"][0]["quality"]["hits10"]
                && query["charges"]["sq8"]["submitted_gets"] == old["arms"][0]["gets"]
                && query["charges"]["sq8"]["verified_bytes"] == old["arms"][0]["physical_bytes"]
                && query["plan"]["selected_pages"].as_array().map(Vec::len)
                    == old["arms"][0]["selected_blocks"]
                        .as_u64()
                        .map(|n| n as usize),
            "historical selected quality/geometry charges",
        )?;
        let now = query["returned"].as_array().ok_or("panel returned ranks")?;
        let before = old["returned_evidence"][0]
            .as_array()
            .ok_or("historical returned ranks")?;
        require(
            now.len() == 10 && before.len() == 10,
            "historical rank length",
        )?;
        for (now, before) in now.iter().zip(before) {
            require(
                now["id"] == before["id"] && now["score_bits"] == before["score_bits"],
                "historical ordered ID/f32 score-bit parity",
            )?;
        }
    }
    Ok(())
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PanelConfig {
    schema: String,
    completed: super::CompletedConfig,
    max_payload_bytes: usize,
}

pub(super) fn reduce(config: &std::path::Path, sha: &str) -> Result<Value> {
    let (c, config_bytes): (PanelConfig, _) = super::read_config(config, sha)?;
    require(
        c.schema == "borsuk-current-v9-panel-reader-config-v1"
            && c.completed.schema == super::SCALE_CONFIG_SCHEMA,
        "v9 panel CLI config schema",
    )?;
    let mut report = read_candidate(
        &c.completed.input,
        &c.completed.expected_identity,
        &c.completed.expected_bound_inputs,
        &[0, 1, 127, 255, 511, 767, 998, 999],
        c.max_payload_bytes,
    )?;
    report["reduction_config_sha256"] = json!(sha);
    report["reduction_config_bytes"] = json!(config_bytes);
    Ok(report)
}

fn capped_header(value: &Value) -> Result<Vec<u8>> {
    // A fixed slice, not a growing Vec writer: rejection precedes any output
    // allocation beyond CONFIG_CAP, including a caller-supplied huge string.
    let mut bytes = vec![0; super::CONFIG_CAP as usize];
    let length = {
        let mut writer = Cursor::new(bytes.as_mut_slice());
        serde_json::to_writer(&mut writer, value)
            .map_err(|e| format!("v9 panel admission: header cap/serialization: {e}"))?;
        usize::try_from(writer.position())?
    };
    bytes.truncate(length);
    Ok(bytes)
}

#[cfg(test)]
mod admission_tests {
    use super::*;

    #[test]
    fn v9_source_oracle_full_sort_ties_partial_tail_and_pre_score_refusal() {
        let query = json!({"trace":{"nomination_evaluated_units":[8,7,6,5,4,3,2,1,0]},
            "plan":{"page_rows":32,"selected_pages":[0,1,2,3,4,5,6,7],
                "ranges":[[0,3584]],"planned_bytes":3584,"target_pages":8,
                "target_shortfall":0,"primary_pages_retained":0,"covered_pages":8,"bridge_pages":0},
            "charges":{"sq8":{"submitted_gets":1,"verified_bytes":3584,"failed_gets":0}}});
        let mut calls = 0;
        validate_source_cover(&query, 257, 2, 1 << 20, |_| {
            calls += 1;
            Ok(1.)
        })
        .unwrap();
        assert_eq!(
            calls, 257,
            "all SOURCE rows are scored, including the unselected one-row tail"
        );
        let mut bad = query.clone();
        bad["plan"]["target_pages"] = json!(9);
        bad["plan"]["target_shortfall"] = json!(1);
        assert!(validate_source_cover(&bad, 257, 2, 1 << 20, |_| Ok(1.)).is_err());
        bad = query.clone();
        bad["plan"]["selected_pages"] = json!([0, 1, 2, 3, 4, 5, 6, 8]);
        assert!(validate_source_cover(&bad, 257, 2, 1 << 20, |_| Ok(1.)).is_err());
        assert!(
            validate_source_cover(&query, 257, 2, 1, |_| panic!("admission precedes scoring"))
                .is_err()
        );
        bad = query.clone();
        bad["trace"]["nomination_evaluated_units"] = json!([0, 0]);
        assert!(
            validate_source_cover(&bad, 257, 2, 1 << 20, |_| panic!(
                "duplicate units precede scoring"
            ))
            .is_err()
        );
        assert!(validate_source_cover(&query, 257, 2, 1 << 20, |_| Ok(f64::NAN)).is_err());
    }

    #[test]
    fn v9_historical_parity_checks_every_selected_rank_and_charge() {
        let mut historical =
            json!({"shortlist_sizes":[256,512,768,1024],"queries":vec![Value::Null;1000]});
        let mut queries = Vec::new();
        for ordinal in [0, 1, 127, 255, 511, 767, 998, 999] {
            let returned = (0..10)
                .map(|rank| json!({"id":ordinal*10+rank,"score_bits":1065353216-rank}))
                .collect::<Vec<_>>();
            queries.push(json!({"ordinal":ordinal,"hits10":9,"returned":returned,
                "plan":{"selected_pages":[0,2]},"charges":{"sq8":{"submitted_gets":2,"verified_bytes":66304}}}));
            historical["queries"][ordinal] = json!({"ordinal":ordinal,
                "arms":[{"quality":{"hits10":9},"gets":2,"physical_bytes":66304,"selected_blocks":2}],
                "returned_evidence":[returned]});
        }
        let panel = json!({"queries":queries});
        compare_historical(&panel, &historical).unwrap();
        for (pointer, value) in [
            ("/queries/7/returned/9/id", json!(0)),
            ("/queries/7/returned/9/score_bits", json!(0)),
            ("/queries/7/hits10", json!(8)),
            ("/queries/7/charges/sq8/submitted_gets", json!(3)),
            ("/queries/7/charges/sq8/verified_bytes", json!(66303)),
            ("/queries/7/plan/selected_pages", json!([0])),
            ("/queries/7/ordinal", json!(998)),
        ] {
            let mut bad = panel.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(compare_historical(&bad, &historical).is_err(), "{pointer}");
        }
        let mut reordered = panel.clone();
        reordered["queries"][0]["returned"]
            .as_array_mut()
            .unwrap()
            .swap(0, 1);
        assert!(compare_historical(&reordered, &historical).is_err());
        let mut wrong_arm = historical;
        wrong_arm["shortlist_sizes"] = json!([128, 512, 768, 1024]);
        assert!(compare_historical(&panel, &wrong_arm).is_err());
    }

    #[test]
    fn v9_header_serialization_is_bounded_before_validation() {
        let small = json!({"schema":"small"});
        assert_eq!(
            capped_header(&small).unwrap(),
            serde_json::to_vec(&small).unwrap()
        );
        let oversized = json!({"padding":"x".repeat(super::super::CONFIG_CAP as usize)});
        assert!(
            capped_header(&oversized)
                .unwrap_err()
                .to_string()
                .contains("header cap/serialization")
        );
    }

    #[test]
    fn v9_trace_admission_rejects_uncharged_or_missing_reservations() {
        let good = json!({"trace":true,
            "trace_peak_charged_by_library_at_traced_admission":true,
            "trace_retained_bytes":16000,"trace_peak_bytes":240000});
        validate_trace_admission(&good).unwrap();
        for (key, value) in [
            (
                "trace_peak_charged_by_library_at_traced_admission",
                json!(false),
            ),
            ("trace_peak_bytes", Value::Null),
            ("trace_peak_bytes", json!(15999)),
            ("trace_retained_bytes", Value::Null),
        ] {
            let mut bad = good.clone();
            bad[key] = value;
            assert!(validate_trace_admission(&bad).is_err(), "{key}");
        }
        let untraced = json!({"trace":false,
            "trace_peak_charged_by_library_at_traced_admission":false,
            "trace_retained_bytes":null,"trace_peak_bytes":null});
        validate_trace_admission(&untraced).unwrap();
    }
}

fn validate_trace_admission(raw: &Value) -> Result<()> {
    let trace = raw["trace"]
        .as_bool()
        .ok_or("current admission trace boolean")?;
    require(
        raw["trace_peak_charged_by_library_at_traced_admission"] == trace,
        "current trace charge agreement",
    )?;
    if trace {
        let retained = raw["trace_retained_bytes"]
            .as_u64()
            .ok_or("current trace retained bytes")?;
        let peak = raw["trace_peak_bytes"]
            .as_u64()
            .ok_or("current trace peak bytes")?;
        require(
            retained > 0 && peak >= retained,
            "current trace reservation",
        )?;
    } else {
        require(
            raw["trace_retained_bytes"].is_null() && raw["trace_peak_bytes"].is_null(),
            "current untraced reservation",
        )?;
    }
    Ok(())
}

pub(super) fn validate_admission(raw: &Value, inputs: &Value) -> Result<()> {
    use borsuk::sq8_s3_range::Sq8RangeTrace;
    validate_trace_admission(raw)?;
    let width = usize::try_from(
        inputs["fetch_parallelism"]
            .as_u64()
            .ok_or("current fetch width")?,
    )?;
    let ranges = usize::try_from(
        inputs["sq8_get_budget"]
            .as_u64()
            .ok_or("current trace ranges")?,
    )?;
    require(
        matches!(width, 16 | 32 | 64 | 128) && (1..=32).contains(&ranges),
        "current trace geometry",
    )?;
    let trace = raw["trace"] == true;
    let peak = if trace {
        Sq8RangeTrace::modeled_bytes(ranges, width)
    } else {
        0
    };
    let both = Sq8RangeTrace::pending_futures_bytes(128, width);
    let pinned = both
        .checked_sub(if trace {
            Sq8RangeTrace::pending_futures_bytes(ranges, width)
        } else {
            0
        })
        .ok_or("current range state subtraction")?;
    let diagnostic = raw["diagnostic_bytes_per_active_query"]
        .as_u64()
        .ok_or("current diagnostic bytes")?;
    let diagnostic_pinned = diagnostic
        .checked_sub(peak as u64)
        .ok_or("current diagnostic peak subtraction")?;
    require(
        raw["host_read_cap_bytes"] == 131072
            && raw["trace_ranges"] == ranges
            && raw["diagnostic_cap_bytes"] == 1048576
            && diagnostic > 0
            && diagnostic <= 1048576
            && raw["diagnostic_pinned_bytes"] == diagnostic_pinned
            && raw["range_state_both_paths_bytes"] == both
            && raw["range_state_pinned_by_runner_bytes"] == pinned,
        "current diagnostic accounting",
    )?;
    if trace {
        require(
            raw["trace_peak_bytes"] == peak
                && raw["trace_retained_bytes"] == Sq8RangeTrace::retained_bytes(ranges),
            "current trace modeled reservation",
        )?;
    }
    let required = diagnostic_pinned
        .checked_add(pinned as u64)
        .ok_or("current pinned sum")?;
    require(
        raw["caller_pinned_bytes"]
            .as_u64()
            .is_some_and(|n| n >= required),
        "current caller pinned reservation",
    )
}

pub(super) fn ordinals(raw: &Value, count: usize) -> Result<Vec<usize>> {
    require((1..=1000).contains(&count), "current query population")?;
    let execution = &raw["execution"];
    if execution["mode"] == "full" {
        fields(execution, "mode")?;
        return Ok((0..count).collect());
    }
    fields(execution, "mode ordinals trace")?;
    require(
        execution["mode"] == "diagnostic_panel" && execution["trace"].is_boolean(),
        "current execution mode/trace",
    )?;
    let values = execution["ordinals"].as_array().ok_or("current ordinals")?;
    require(
        (1..=128).contains(&values.len()),
        "current ordinal count before allocation",
    )?;
    let ordinals = values
        .iter()
        .map(|v| usize::try_from(v.as_u64().ok_or("current ordinal integer")?).map_err(Into::into))
        .collect::<Result<Vec<_>>>()?;
    require(
        !ordinals.is_empty()
            && ordinals.len() <= 128
            && ordinals.windows(2).all(|p| p[0] < p[1])
            && ordinals.last().is_some_and(|n| *n < count),
        "current ordinal sequence",
    )?;
    Ok(ordinals)
}

pub(super) fn budgets(identity: &Value, inputs: &Value) -> Result<()> {
    for key in ["source_get_budget", "sq8_get_budget", "sq8_byte_budget"] {
        require(
            identity.get(key) == inputs.get(key),
            "current identity/input budget agreement",
        )?;
    }
    require(
        matches!(inputs["sq8_get_budget"].as_u64(), Some(1..=32))
            && matches!(inputs["sq8_byte_budget"].as_u64(), Some(1..=33_554_432)),
        "current SQ8 budgets",
    )
}

// Reuse version-neutral field/transport/stage checks without changing the
// original bytes or old protocols. The v9-only fields are validated explicitly
// before constructing a temporary validation view; file seals cover raw bytes.
pub(super) fn validate_row(line: &[u8], phase: &str) -> Result<()> {
    let super::UniqueJson(mut raw) = serde_json::from_slice(line)?;
    match phase {
        "identity" | "bound_inputs" => {
            require(
                matches!(raw["sq8_get_budget"].as_u64(), Some(1..=32))
                    && matches!(raw["sq8_byte_budget"].as_u64(), Some(1..=33_554_432)),
                "current row SQ8 budgets",
            )?;
            let object = raw.as_object_mut().ok_or("current header object")?;
            object.remove("sq8_get_budget");
            object.remove("sq8_byte_budget");
            if phase == "identity" {
                require(
                    raw["schema"] == "borsuk-cohere-native-baseline-result-v9",
                    "current result v9 required",
                )?;
                raw["schema"] = json!("borsuk-cohere-native-baseline-result-v8");
            }
        }
        "diagnostic_admission" => {
            require(
                raw["trace"].is_boolean() && raw["panel_line_cap_bytes"] == 65536,
                "current admission trace/line cap",
            )?;
            validate_trace_admission(&raw)?;
            raw["trace"] = json!(false);
        }
        "query" => {
            require(
                raw["serving"] == json!({"mode":"baseline"})
                    && raw["source_nomination_skipped"] == false
                    && raw["planning_scope"] == "source_nomination_and_cover",
                "current baseline source planning",
            )?;
            let plan = &raw["plan"];
            fields(
                plan,
                "page_rows selected_pages ranges planned_bytes target_pages target_shortfall primary_pages_retained covered_pages bridge_pages",
            )?;
            require(plan["page_rows"] == 32, "current proof rows")?;
            if let Some(diag) = raw.get("diagnostic") {
                fields(
                    diag,
                    "schema query_monotonic_start_ns query_monotonic_end_ns process_cpu_start_ns process_cpu_end_ns sq8_range_trace host",
                )?;
                require(
                    diag["schema"] == "borsuk-sq8-range-attribution-diagnostic-v2"
                        && diag["sq8_range_trace"].is_object()
                        && diag["host"].is_object(),
                    "current diagnostic envelope",
                )?;
            }
            let object = raw.as_object_mut().ok_or("current query object")?;
            for key in [
                "serving",
                "source_nomination_skipped",
                "planning_scope",
                "plan",
                "diagnostic",
            ] {
                object.remove(key);
            }
            if let Some(slot) = object.remove("selected_slot") {
                require(slot.as_u64().is_some(), "current selected slot")?;
            }
            return super::validate_v2_row(&serde_json::to_vec(&raw)?, phase);
        }
        _ => (),
    }
    super::validate_scale_row(&serde_json::to_vec(&raw)?, phase)
}

pub(super) fn read_candidate(
    pin: &super::CompletedInput,
    identity: &Value,
    inputs: &Value,
    selected: &[usize],
    max_payload_bytes: usize,
) -> Result<Value> {
    // Six MiB covers bounded row/JSON validation views and the neutral reader's
    // stage vectors/samples; borrowed config trees are charged by this caller.
    require(
        max_payload_bytes >= 6 * 1024 * 1024 + 2 * super::CONFIG_CAP as usize
            && pin.bytes > 0
            && pin.bytes <= super::FILE_CAP
            && super::valid_sha(&pin.sha256),
        "v9 panel admission: descriptor/payload",
    )?;
    require(
        identity["schema"] == "borsuk-cohere-native-baseline-result-v9"
            && inputs["dataset"] == "CohereLabs/wikipedia-2023-11-embed-multilingual-v3"
            && inputs["revision"] == "ade45fb52bd549f5e8c065636fe4160a43c2af36"
            && inputs["rows"] == 1_000_000
            && inputs["dimensions"] == 1024
            && inputs["count"] == 1000
            && inputs["k"] == 10
            && inputs["metric"] == "cosine"
            && inputs["source_cache"] == "off"
            && inputs["serving"] == json!({"mode":"baseline"})
            && inputs["source_get_budget"] == 128
            && inputs["sq8_get_budget"] == 30
            && inputs["sq8_byte_budget"] == 33554432
            && inputs["execution"]["trace"] == true
            && inputs["requests_bytes"] == 4096000
            && inputs["truth_bytes"] == 80000
            && selected == [0, 1, 127, 255, 511, 767, 998, 999],
        "v9 panel admission: production geometry/policy",
    )?;
    for (key, pin) in [
        (
            "requests_sha256",
            "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e",
        ),
        (
            "truth_sha256",
            "0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f",
        ),
        (
            "native_source_sha256",
            "31f28fa211f9237f0f7e41f569773cd207cefcdce22d4932ec96aee01ff2c40d",
        ),
        (
            "native_sq8_sha256",
            "cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1",
        ),
        (
            "native_order_sha256",
            "1060c919e34e93cc9a3d88b93eda226a8635bba41bfc60e5ba47caafb711d413",
        ),
    ] {
        require(
            inputs[key] == pin,
            "v9 panel admission: production input pin",
        )?;
    }
    require(
        identity["execution"] == inputs["execution"] && ordinals(inputs, 1000)? == selected,
        "v9 panel admission: selected ordinals",
    )?;
    budgets(identity, inputs)?;
    for (value, phase) in [(identity, "identity"), (inputs, "bound_inputs")] {
        let bytes = capped_header(value)?;
        validate_row(&bytes, phase)?;
    }
    let mut neutral_identity = identity.clone();
    neutral_identity["schema"] = json!("borsuk-cohere-native-baseline-result-v2");
    let typed_identity: super::Identity = serde_json::from_value(neutral_identity.clone())?;
    super::validate_v2_identity(&typed_identity, &neutral_identity)
        .map_err(|e| format!("v9 panel admission: {e}"))?;
    let typed_inputs: super::Inputs = serde_json::from_value(inputs.clone())?;
    super::validate_scale_inputs_for_version(&typed_inputs, inputs, true)
        .map_err(|e| format!("v9 panel admission: {e}"))?;
    let config = super::CompletedConfig {
        schema: super::SCALE_CONFIG_SCHEMA.into(),
        input: super::CompletedInput {
            path: pin.path.clone(),
            bytes: pin.bytes,
            sha256: pin.sha256.clone(),
        },
        expected_identity: identity.clone(),
        expected_bound_inputs: inputs.clone(),
    };
    let mut plans = Vec::with_capacity(selected.len());
    let run = super::read_run_observed(&pin.path, &pin.sha256, Some(&config), |line, _, _| {
        let super::UniqueJson(row) = serde_json::from_slice(line)?;
        plans.push(row["plan"].clone());
        Ok(())
    })?;
    require(plans.len() == selected.len(), "current observed plan count")?;
    require(
        run.terminal.underfilled_queries == 0,
        "current panel full-k",
    )?;
    Ok(
        json!({"schema":REPORT_SCHEMA,"status":"VALIDATED_PANEL","complete":true,
        "panel_input_sha256":pin.sha256,"panel_input_bytes":pin.bytes,
        "producer_provenance_qualified":false,"selected_count":run.samples.len(),
        "recall_denominator":run.terminal.recall_denominator,"recall_numerator":run.terminal.recall_numerator,
        "population_count":run.inputs.count,"exact_range_oracle_qualified":false,
        "historical_rank_parity_qualified":false,"cold_performance_claim":false,"vendor_win_claim":false,
        "queries":selected.iter().zip(&run.samples).zip(&plans).map(|((ordinal,s),plan)| json!({"ordinal":ordinal,"plan":plan,
            "returned":s.returned.iter().map(|h|json!({"id":h.id,"score_bits":h.score_bits})).collect::<Vec<_>>(),
            "hits10":s.hits10,"charges":s.charges})).collect::<Vec<_>>() }),
    )
}

pub(super) fn validate_geometry(
    query: &Value,
    rows: usize,
    dimensions: usize,
    max_gets: usize,
    max_bytes: usize,
) -> Result<()> {
    require(
        rows > 0
            && rows <= 1_000_000
            && dimensions > 0
            && dimensions <= 1024
            && max_gets > 0
            && max_gets <= 32
            && max_bytes > 0,
        "current geometry limits",
    )?;
    let plan = &query["plan"];
    fields(
        plan,
        "page_rows selected_pages ranges planned_bytes target_pages target_shortfall primary_pages_retained covered_pages bridge_pages",
    )?;
    require(plan["page_rows"] == 32, "current geometry proof rows")?;
    let n = |value: &Value| -> Result<usize> {
        Ok(usize::try_from(
            value.as_u64().ok_or("current geometry integer")?,
        )?)
    };
    let selected = plan["selected_pages"]
        .as_array()
        .ok_or("current selected blocks")?
        .iter()
        .map(&n)
        .collect::<Result<Vec<_>>>()?;
    let pages = rows.div_ceil(32);
    let width = dimensions + 12;
    let block = 32 * width;
    require(
        !selected.is_empty()
            && selected.len() <= 256
            && selected.windows(2).all(|p| p[0] < p[1])
            && selected.last().is_some_and(|p| *p < pages),
        "current ordered selected blocks",
    )?;
    // Independent interval oracle, not the production coalescer or cover_charge.
    let mut ranges = Vec::<[usize; 2]>::new();
    for &page in &selected {
        let start = page * block;
        let end = ((page + 1) * 32).min(rows) * width;
        if let Some(last) = ranges.last_mut().filter(|r| r[1] == start) {
            last[1] = end;
        } else {
            ranges.push([start, end]);
        }
    }
    while ranges.len() > max_gets {
        let gap = (0..ranges.len() - 1)
            .min_by_key(|&i| (ranges[i + 1][0] - ranges[i][1], i))
            .ok_or("current canonical gap")?;
        let end = ranges.remove(gap + 1)[1];
        ranges[gap][1] = end;
    }
    let actual = plan["ranges"]
        .as_array()
        .ok_or("current ranges")?
        .iter()
        .map(|r| {
            let r = r.as_array().ok_or("current range pair")?;
            require(r.len() == 2, "current range pair width")?;
            Ok([n(&r[0])?, n(&r[1])?])
        })
        .collect::<Result<Vec<_>>>()?;
    let bytes = ranges.iter().map(|r| r[1] - r[0]).sum::<usize>();
    let covered = ranges
        .iter()
        .map(|r| (r[1] / width).div_ceil(32) - r[0] / block)
        .sum::<usize>();
    let target = n(&plan["target_pages"])?;
    require(
        actual == ranges
            && bytes <= max_bytes
            && n(&plan["planned_bytes"])? == bytes
            && n(&plan["covered_pages"])? == covered
            && n(&plan["bridge_pages"])? == covered - selected.len()
            && target >= selected.len()
            && target <= 256.min(pages)
            && n(&plan["target_shortfall"])? == target - selected.len()
            && plan["primary_pages_retained"] == 0,
        "current canonical cover/counts",
    )?;
    let charge = &query["charges"]["sq8"];
    fields(charge, "submitted_gets verified_bytes failed_gets")?;
    require(
        n(&charge["submitted_gets"])? == ranges.len()
            && n(&charge["verified_bytes"])? == bytes
            && charge["failed_gets"] == 0,
        "current exact SQ8 charges",
    )
}

// Offline oracle: retain every scored SOURCE row and full-sort it. This does
// not call the serving heap, shortlist primitive, or production cover planner.
pub(super) fn validate_source_cover(
    query: &Value,
    rows: usize,
    dimensions: usize,
    workspace_bytes: usize,
    mut score: impl FnMut(usize) -> Result<f64>,
) -> Result<()> {
    require(
        rows > 0 && rows <= 1_000_000 && dimensions > 0 && dimensions <= 1024,
        "SOURCE oracle geometry",
    )?;
    let units = query["trace"]["nomination_evaluated_units"]
        .as_array()
        .ok_or("SOURCE oracle units")?;
    require(
        !units.is_empty() && units.len() <= rows.div_ceil(32),
        "SOURCE oracle unit count",
    )?;
    require(
        units
            .len()
            .checked_mul(64)
            .and_then(|n| n.checked_add(4096))
            .is_some_and(|n| n <= workspace_bytes),
        "SOURCE oracle unit workspace admission",
    )?;
    let mut seen = std::collections::BTreeSet::new();
    let mut count = 0usize;
    for unit in units {
        let unit = usize::try_from(unit.as_u64().ok_or("SOURCE oracle unit integer")?)?;
        require(
            unit < rows.div_ceil(32) && seen.insert(unit),
            "SOURCE oracle unique bounded units",
        )?;
        count = count
            .checked_add(((unit + 1) * 32).min(rows) - unit * 32)
            .ok_or("SOURCE oracle count")?;
    }
    let modeled = count
        .checked_mul(32)
        .and_then(|n| n.checked_add(units.len() * 64))
        .and_then(|n| n.checked_add(32768))
        .ok_or("SOURCE oracle workspace overflow")?;
    require(
        modeled <= workspace_bytes,
        "SOURCE oracle workspace admission",
    )?;
    let mut scored = Vec::new();
    scored.try_reserve_exact(count)?;
    for unit in seen {
        for row in unit * 32..((unit + 1) * 32).min(rows) {
            let value = score(row)?;
            require(value.is_finite(), "SOURCE oracle finite score")?;
            scored.push((row, value));
        }
    }
    scored.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    let selected = scored
        .iter()
        .take(256)
        .map(|p| p.0 / 32)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    require(
        query["plan"]["selected_pages"] == json!(selected),
        "SOURCE full-sort block parity",
    )?;
    require(
        query["plan"]["target_pages"] == selected.len() && query["plan"]["target_shortfall"] == 0,
        "SOURCE oracle exact target accounting",
    )?;
    validate_geometry(query, rows, dimensions, 30, 33554432)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn query() -> Value {
        // Required blocks 0,2,4: equal-size gaps must merge on the left.
        // Four physical blocks are covered, including the bridge at block 1.
        json!({"plan":{"page_rows":32,"selected_pages":[0,2,4],
            "ranges":[[0,99456],[132608,165760]],"planned_bytes":132608,
            "target_pages":3,"target_shortfall":0,"primary_pages_retained":0,
            "covered_pages":4,"bridge_pages":1},
            "charges":{"sq8":{"submitted_gets":2,"verified_bytes":132608,"failed_gets":0}}})
    }

    #[test]
    fn v9_plan_geometry_requires_canonical_ranges_and_exact_charge() {
        let valid = query();
        validate_geometry(&valid, 1_000_000, 1024, 2, 132608)
            .expect("accept canonical native v9 cover and exact charges");
        for (pointer, value) in [
            ("/plan/page_rows", json!(256)),
            ("/plan/selected_pages", json!([0, 4, 2])),
            ("/plan/selected_pages", json!([0, 2, 2])),
            ("/plan/selected_pages", json!([0, 2, 31250])),
            ("/plan/selected_pages", json!([])),
            // Same total charge, but it chose the right equal-size gap.
            ("/plan/ranges", json!([[0, 33152], [66304, 165760]])),
            ("/plan/ranges", json!([[0, 99457], [132608, 165760]])),
            ("/plan/ranges", json!([[0, 132608], [99456, 165760]])),
            ("/plan/planned_bytes", json!(132607)),
            ("/plan/covered_pages", json!(3)),
            ("/plan/bridge_pages", json!(0)),
            ("/plan/target_shortfall", json!(1)),
            ("/plan/target_pages", json!(2)),
            ("/plan/primary_pages_retained", json!(1)),
            ("/charges/sq8/submitted_gets", json!(1)),
            ("/charges/sq8/verified_bytes", json!(132607)),
            ("/charges/sq8/failed_gets", json!(1)),
        ] {
            let mut changed = valid.clone();
            *changed.pointer_mut(pointer).unwrap() = value;
            assert!(
                validate_geometry(&changed, 1_000_000, 1024, 2, 132608).is_err(),
                "{pointer}"
            );
        }
        assert!(validate_geometry(&valid, 1_000_000, 1024, 2, 132607).is_err());
        assert!(validate_geometry(&valid, 1_000_000, 1024, 0, 132608).is_err());
    }

    #[test]
    fn v9_plan_geometry_preserves_partial_tail_and_budget_shortfall() {
        let tail = json!({"plan":{"page_rows":32,"selected_pages":[0,2],
            "ranges":[[0,1105]],"planned_bytes":1105,"target_pages":3,
            "target_shortfall":1,"primary_pages_retained":0,
            "covered_pages":3,"bridge_pages":1},
            "charges":{"sq8":{"submitted_gets":1,"verified_bytes":1105,"failed_gets":0}}});
        validate_geometry(&tail, 65, 5, 1, 1105)
            .expect("one-row final block is counted once without padding its bytes");
        let mut padded = tail.clone();
        padded["plan"]["ranges"] = json!([[0, 1632]]);
        padded["plan"]["planned_bytes"] = json!(1632);
        padded["charges"]["sq8"]["verified_bytes"] = json!(1632);
        assert!(validate_geometry(&padded, 65, 5, 1, 1632).is_err());
        let mut unknown = tail;
        unknown["plan"]["ignored_geometry"] = json!(true);
        assert!(validate_geometry(&unknown, 65, 5, 1, 1105).is_err());
    }
}
