//! Current-format panel validation. Historical result readers remain unchanged.
use super::{Result, fields, require};
use serde_json::{Value, json};
use std::io::Cursor;

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
    let run = super::read_run_with(&pin.path, &pin.sha256, Some(&config))?;
    require(
        run.terminal.underfilled_queries == 0,
        "current panel full-k",
    )?;
    Ok(
        json!({"schema":"borsuk-current-v9-panel-reader-v1","selected_count":run.samples.len(),
        "recall_denominator":run.terminal.recall_denominator,"recall_numerator":run.terminal.recall_numerator,
        "population_count":run.inputs.count,"exact_range_oracle_qualified":false,
        "historical_rank_parity_qualified":false,"cold_performance_claim":false,"vendor_win_claim":false,
        "queries":selected.iter().zip(&run.samples).map(|(ordinal,s)| json!({"ordinal":ordinal,
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
