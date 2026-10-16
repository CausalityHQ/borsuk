//! Strict current baseline evidence. Formal experiments add their own frozen pins.
use super::{CompletedConfig, Result, fields, plus, require};
use serde_json::{Value, json};

pub(super) const RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v10";
pub(super) const READ_RETRY_RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v11";
pub(super) const LIBRARY_SCHEDULER_RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v12";
const READ_RETRY_CONFIG_SCHEMA: &str = "borsuk-cohere-native-baseline-config-v11";
const SCOPE_NOTE: &str = "diagnostic selected original ordinals or prefix; not population p99, population recall or full-cohort qualification";
const MEMORY_CAP: u64 = 512 * 1024 * 1024;
const PLAN_FIELDS: &str = "page_rows selected_pages ranges logical_range_count planned_bytes target_pages target_shortfall primary_pages_retained covered_pages bridge_pages";

pub(super) fn is_result_schema(schema: &str) -> bool {
    matches!(
        schema,
        RESULT_SCHEMA | READ_RETRY_RESULT_SCHEMA | LIBRARY_SCHEDULER_RESULT_SCHEMA
    )
}

fn number(v: &Value) -> Result<u64> {
    v.as_u64().ok_or_else(|| "v10 unsigned integer".into())
}

fn split_option(header: &Value) -> Result<()> {
    let split = header
        .get("sq8_range_split")
        .ok_or("v10 split field required")?;
    if !split.is_null() {
        fields(split, "max_physical_range_bytes max_physical_gets")?;
        require(
            matches!(
                split["max_physical_range_bytes"].as_u64(),
                Some(1..=MEMORY_CAP)
            ) && matches!(split["max_physical_gets"].as_u64(), Some(1..=128)),
            "v10 split caps",
        )?;
    }
    Ok(())
}

pub(super) fn physical_cap(inputs: &Value) -> Result<u64> {
    split_option(inputs)?;
    number(if inputs["sq8_range_split"].is_null() {
        &inputs["sq8_get_budget"]
    } else {
        &inputs["sq8_range_split"]["max_physical_gets"]
    })
}

pub(super) fn validate_row(line: &[u8], phase: &str, library_scheduler: bool) -> Result<()> {
    let super::UniqueJson(raw) = serde_json::from_slice(line)?;
    require(raw["phase"] == phase, "v10 row phase")?;
    let extras = match phase {
        "identity" => {
            "fetch_parallelism source_get_budget sq8_get_budget sq8_byte_budget sq8_range_split serving execution sq8_range_source_sha256 returned_source_sha256"
        }
        "bound_inputs" => {
            "fetch_parallelism source_cache source_get_budget sq8_get_budget sq8_byte_budget sq8_range_split serving execution selected_count corpus_intervals reserved_query_interval reserved_queries_sha256 cohort_receipt_sha256 derivation_receipt_sha256 producer_authority max_memory_bytes"
        }
        "startup" => "serving",
        "query" => {
            "serving source_nomination_skipped planning_scope plan returned_physical_ordinals"
        }
        "all_queries_sealed" => "selected_count population_count reserved_query_count",
        "diagnostic_admission" => {
            let scheduler_fields = if library_scheduler {
                "range_scheduler_owner range_state_charged_by_library_bytes trace_additional_bytes_charged_by_library"
            } else {
                "range_state_pinned_by_runner_bytes trace_peak_charged_by_library_at_traced_admission"
            };
            fields(
                &raw,
                &format!(
                    "phase execution selected_count population_count trace diagnostic_bytes_per_active_query diagnostic_cap_bytes max_active_queries charged_in_caller_pinned_bytes caller_pinned_bytes panel_line_cap_bytes host_read_cap_bytes trace_ranges population_percentiles_valid full_cohort_qualification semantics truth_opened trace_retained_bytes trace_peak_bytes diagnostic_pinned_bytes range_state_both_paths_bytes {scheduler_fields}"
                ),
            )?;
            require(
                !library_scheduler || raw["range_scheduler_owner"] == "library",
                "v12 library scheduler owner",
            )?;
            return require(
                raw["trace"].is_boolean()
                    && raw["truth_opened"] == false
                    && raw["max_active_queries"] == 1
                    && raw["charged_in_caller_pinned_bytes"] == true
                    && raw["population_percentiles_valid"] == false
                    && raw["full_cohort_qualification"] == false,
                "v10 diagnostic admission scope",
            );
        }
        _ => "",
    };
    let mut roster = format!("{} {extras}", super::v2_row_fields(phase)?);
    if library_scheduler && matches!(phase, "identity" | "bound_inputs") {
        roster.push_str(" range_scheduler_owner");
        require(
            raw["range_scheduler_owner"] == "library",
            "v12 library scheduler owner",
        )?;
    }
    if library_scheduler && phase == "startup" {
        roster.push_str(" range_state_charged_by_library_bytes");
    }
    let retry = match phase {
        "identity" => {
            raw["schema"] == READ_RETRY_RESULT_SCHEMA
                || (library_scheduler && raw.get("s3_read_retry").is_some())
        }
        "bound_inputs" => raw.get("config_schema").is_some(),
        _ => false,
    };
    if retry {
        roster.push_str(" config_schema s3_read_retry");
    }
    for key in match phase {
        "identity" | "bound_inputs" => &["shared_byte_window"][..],
        "startup" => &["shared_byte_window_state_bytes"][..],
        "query" => &["selected_slot", "diagnostic"][..],
        _ => &[][..],
    } {
        if raw.get(*key).is_some() {
            roster.push(' ');
            roster.push_str(key);
        }
    }
    fields(&raw, &roster)?;
    if retry {
        require(
            raw["config_schema"] == READ_RETRY_CONFIG_SCHEMA
                && raw["s3_read_retry"]
                    == json!({
                        "policy":"one_retry_100ms",
                        "sdk_max_retries":1,
                        "physical_submission_upper_bound_per_logical_read":2,
                        "physical_submission_bound_scope":"S3_data_only_excludes_zero_retry_IMDS",
                        "backoff_ms":100,"backoff_cap_ms":100,"sdk_retry_window_ms":1000,
                        "sdk_retry_window_is_hard_deadline":false,
                        "request_timeout_ms":30_000,"connect_timeout_ms":5000,
                        "data_methods":["GET","HEAD"],"mutations":"refused_before_reqwest",
                        "credentials":"existing_zero_retry_imds",
                        "every_206":"exact_attempted_range_and_length_before_body",
                        "query_get_and_verified_byte_budgets":"unchanged_logical_caps",
                        "transport_counters":"cumulative_process_submissions_and_consumed_payload_not_wire_or_billed"
                    }),
            "v11 exact config schema and S3 read retry policy",
        )?;
        require(
            phase != "bound_inputs" || raw["backend"]["kind"] == "s3",
            "v11 read retries require S3 backend",
        )?;
    }
    if matches!(phase, "identity" | "bound_inputs" | "startup" | "query") {
        require(
            raw["serving"] == json!({"mode":"baseline"}),
            "v10 baseline serving",
        )?;
    }
    match phase {
        "identity" | "bound_inputs" => {
            split_option(&raw)?;
            require(
                matches!(raw["fetch_parallelism"].as_u64(), Some(16 | 32 | 64 | 128))
                    && matches!(raw["source_get_budget"].as_u64(), Some(96 | 128))
                    && matches!(raw["sq8_get_budget"].as_u64(), Some(1..=256))
                    && matches!(raw["sq8_byte_budget"].as_u64(), Some(1..=MEMORY_CAP)),
                "v10 explicit budgets/width",
            )?;
            if let Some(window) = raw.get("shared_byte_window") {
                super::validate_byte_window(window, number(&raw["fetch_parallelism"])?)?;
            }
            if phase == "identity" {
                require(
                    if library_scheduler {
                        raw["schema"] == LIBRARY_SCHEDULER_RESULT_SCHEMA
                    } else {
                        matches!(
                            raw["schema"].as_str(),
                            Some(RESULT_SCHEMA | READ_RETRY_RESULT_SCHEMA)
                        )
                    },
                    "explicit v10/v11/v12 result schema",
                )?;
                for key in ["sq8_range_source_sha256", "returned_source_sha256"] {
                    require(
                        raw[key].as_str().is_some_and(super::valid_sha),
                        "v10 component SHA256",
                    )?;
                }
            } else {
                require(raw["source_cache"] == "off", "source cache must be off")?;
            }
        }
        "startup" => {
            if let Some(state) = raw.get("shared_byte_window_state_bytes") {
                require(
                    matches!(state.as_u64(), Some(1..=MEMORY_CAP)),
                    "byte-window allocated state",
                )?;
            }
        }
        "query" => {
            require(
                raw["source_nomination_skipped"] == false
                    && raw["planning_scope"] == "source_nomination_and_cover",
                "v10 baseline query semantics",
            )?;
            fields(&raw["plan"], PLAN_FIELDS)?;
            logical_count(&raw)?;
            require(
                raw["returned_physical_ordinals"].is_array(),
                "v10 returned physical ordinals required",
            )?;
            if let Some(slot) = raw.get("selected_slot") {
                require(slot.as_u64().is_some(), "v10 selected slot")?;
            }
            if let Some(diag) = raw.get("diagnostic") {
                diagnostic_shape(diag)?;
            }
        }
        "terminal" => {
            let t = &raw["summary"];
            let scoped = t["diagnostic_panel"] == true || number(&t["population_count"])? < 1000;
            fields(
                t,
                &format!(
                    "{} serving direct_memory execution selected_count executed_count population_count diagnostic_panel reserved_query_count diagnostic_prefix population_percentiles_valid full_cohort_qualification{}",
                    super::terminal_fields(),
                    if scoped { " scope_note" } else { "" }
                ),
            )?;
            if scoped {
                require(t["scope_note"] == SCOPE_NOTE, "v10 terminal scope note")?;
            }
        }
        _ => (),
    }
    super::validate_native_row_values(&raw, phase)
}

pub(super) fn validate_completed(c: &CompletedConfig) -> Result<()> {
    require(
        c.schema == super::SCALE_CONFIG_SCHEMA
            && c.input.bytes > 0
            && c.input.bytes <= super::FILE_CAP
            && super::valid_sha(&c.input.sha256),
        "v10 completed descriptor",
    )?;
    for (v, phase) in [
        (&c.expected_identity, "identity"),
        (&c.expected_bound_inputs, "bound_inputs"),
    ] {
        let bytes = serde_json::to_vec(v)?;
        require(bytes.len() < 65536, "v10 header cap")?;
        validate_row(
            &bytes,
            phase,
            c.expected_identity["schema"] == LIBRARY_SCHEDULER_RESULT_SCHEMA,
        )?;
    }
    let i = &c.expected_identity;
    let b = &c.expected_bound_inputs;
    super::validate_native_identity(
        &serde_json::from_value(i.clone())?,
        i,
        i["schema"].as_str().ok_or("current result schema")?,
    )?;
    super::validate_current_scale_inputs(&serde_json::from_value(b.clone())?, b)?;
    for key in [
        "fetch_parallelism",
        "source_get_budget",
        "sq8_get_budget",
        "sq8_byte_budget",
        "sq8_range_split",
        "shared_byte_window",
        "execution",
        "serving",
        "config_schema",
        "s3_read_retry",
        "range_scheduler_owner",
    ] {
        require(i.get(key) == b.get(key), "v10 identity/input agreement")?;
    }
    Ok(())
}

fn logical_count(raw: &Value) -> Result<()> {
    let ranges = raw["plan"]["ranges"]
        .as_array()
        .ok_or("v10 logical ranges")?;
    require(
        raw["plan"]["logical_range_count"].as_u64() == Some(ranges.len() as u64),
        "v10 logical range count",
    )
}

pub(super) fn returned_ordinals(raw: &Value, rows: u64, width: u64) -> Result<Vec<u64>> {
    let ordinals = raw["returned_physical_ordinals"]
        .as_array()
        .ok_or("v10 returned physical ordinals required")?;
    let returned = raw["returned"].as_array().ok_or("v10 returned hits")?;
    require(
        ordinals.len() == returned.len()
            && raw["returned_count"] == returned.len()
            && ordinals.len() <= super::K,
        "v10 returned ordinal count",
    )?;
    let mut seen = std::collections::BTreeSet::new();
    for ordinal in ordinals {
        let ordinal = number(ordinal)?;
        require(
            ordinal < rows && seen.insert(ordinal),
            "v10 unique bounded physical ordinal",
        )?;
        let start = ordinal * width;
        require(
            raw["plan"]["ranges"]
                .as_array()
                .ok_or("v10 logical ranges")?
                .iter()
                .any(|r| {
                    r[0].as_u64().is_some_and(|first| first <= start)
                        && r[1].as_u64().is_some_and(|last| last >= start + width)
                }),
            "v10 returned ordinal outside logical coverage",
        )?;
    }
    ordinals.iter().map(number).collect()
}

// Independent canonical cover oracle; this never invokes the serving planner.
pub(super) fn validate_geometry(raw: &Value, inputs: &Value) -> Result<()> {
    let rows = number(&inputs["rows"])?;
    let dimensions = number(&inputs["dimensions"])?;
    let max_gets = number(&inputs["sq8_get_budget"])?;
    let max_bytes = number(&inputs["sq8_byte_budget"])?;
    require(
        (1..=1_000_000).contains(&rows)
            && (1..=1024).contains(&dimensions)
            && (1..=256).contains(&max_gets)
            && (1..=MEMORY_CAP).contains(&max_bytes),
        "v10 geometry limits",
    )?;
    let plan = &raw["plan"];
    fields(plan, PLAN_FIELDS)?;
    logical_count(raw)?;
    require(plan["page_rows"] == 32, "v10 proof rows")?;
    let selected = plan["selected_pages"]
        .as_array()
        .ok_or("v10 selected blocks")?
        .iter()
        .map(number)
        .collect::<Result<Vec<_>>>()?;
    let pages = rows.div_ceil(32);
    let width = dimensions + 12;
    let block = 32 * width;
    require(
        !selected.is_empty()
            && selected.len() <= 256
            && selected.windows(2).all(|p| p[0] < p[1])
            && selected.last().is_some_and(|p| *p < pages),
        "v10 ordered selected blocks",
    )?;
    let mut canonical = Vec::<[u64; 2]>::new();
    for &page in &selected {
        let start = page * block;
        let end = ((page + 1) * 32).min(rows) * width;
        if let Some(last) = canonical.last_mut().filter(|r| r[1] == start) {
            last[1] = end;
        } else {
            canonical.push([start, end]);
        }
    }
    while canonical.len() as u64 > max_gets {
        let gap = (0..canonical.len() - 1)
            .min_by_key(|&i| (canonical[i + 1][0] - canonical[i][1], i))
            .ok_or("v10 canonical gap")?;
        let end = canonical.remove(gap + 1)[1];
        canonical[gap][1] = end;
    }
    let actual = plan["ranges"]
        .as_array()
        .ok_or("v10 ranges")?
        .iter()
        .map(|r| {
            let r = r.as_array().ok_or("v10 range pair")?;
            require(r.len() == 2, "v10 range pair width")?;
            Ok([number(&r[0])?, number(&r[1])?])
        })
        .collect::<Result<Vec<_>>>()?;
    let bytes = canonical.iter().map(|r| r[1] - r[0]).sum::<u64>();
    let covered = canonical
        .iter()
        .map(|r| (r[1] / width).div_ceil(32) - r[0] / block)
        .sum::<u64>();
    let target = number(&plan["target_pages"])?;
    require(
        actual == canonical
            && bytes <= max_bytes
            && plan["planned_bytes"] == bytes
            && plan["covered_pages"] == covered
            && plan["bridge_pages"] == covered - selected.len() as u64
            && target >= selected.len() as u64
            && target <= 256.min(pages)
            && plan["target_shortfall"] == target - selected.len() as u64
            && plan["primary_pages_retained"] == 0,
        "v10 canonical cover/counts",
    )?;
    returned_ordinals(raw, rows, width)?;
    let physical = physical_ranges(raw, inputs)?;
    require(
        raw["charges"]["sq8"]
            == json!({"submitted_gets":physical.len(), "verified_bytes":bytes,"failed_gets":0}),
        "split exact physical charges",
    )?;
    if let Some(diag) = raw.get("diagnostic") {
        diagnostic_shape(diag)?;
        let trace = &diag["sq8_range_trace"];
        let ranges = trace["ranges"].as_array().ok_or("v10 trace ranges")?;
        require(
            ranges.len() == physical.len()
                && trace["planned_ranges"] == physical.len()
                && trace["planned_bytes"] == bytes
                && trace["dropped_ranges"] == 0
                && trace["max_parallel"]
                    == (physical.len() as u64).min(number(&inputs["fetch_parallelism"])?)
                && trace["outcome"] == "ranked",
            "v10 trace physical totals",
        )?;
        for (range, expected) in ranges.iter().zip(physical) {
            require(
                range[3] == expected[0] && range[4] == expected[1],
                "split exact physical coverage",
            )?;
        }
    }
    Ok(())
}

pub(super) fn physical_ranges(raw: &Value, inputs: &Value) -> Result<Vec<Value>> {
    let cap = physical_cap(inputs)?;
    let split = &inputs["sq8_range_split"];
    let block = 32 * plus(number(&inputs["dimensions"])?, 12)?;
    let piece = if split.is_null() {
        None
    } else {
        Some(number(&split["max_physical_range_bytes"])? / block * block)
    };
    let mut physical = Vec::new();
    for range in raw["plan"]["ranges"]
        .as_array()
        .ok_or("v10 logical ranges")?
    {
        let (mut start, end) = (number(&range[0])?, number(&range[1])?);
        while start < end {
            let next = match piece {
                None => end,
                Some(0) => {
                    require(
                        end - start <= number(&split["max_physical_range_bytes"])?,
                        "v10 indivisible proof block",
                    )?;
                    end
                }
                Some(piece) => end.min(plus(start, piece)?),
            };
            require((physical.len() as u64) < cap, "physical GET cap")?;
            physical.push(json!([start, next]));
            start = next;
        }
    }
    Ok(physical)
}

pub(super) fn diagnostic_shape(diag: &Value) -> Result<()> {
    fields(
        diag,
        "schema query_monotonic_start_ns query_monotonic_end_ns process_cpu_start_ns process_cpu_end_ns sq8_range_trace host",
    )?;
    require(
        diag["schema"] == "borsuk-sq8-range-attribution-diagnostic-v3"
            && diag["sq8_range_trace"].is_object()
            && diag["host"].is_object(),
        "panel diagnostic schema",
    )
}

fn library_scheduler_bytes(inputs: &Value) -> Result<usize> {
    use borsuk::sq8_s3_range::Sq8RangeTrace;
    let width = usize::try_from(number(&inputs["fetch_parallelism"])?)?;
    let source = usize::try_from(number(&inputs["source_get_budget"])?)?;
    let sq8 = usize::try_from(physical_cap(inputs)?)?;
    Ok(Sq8RangeTrace::checked_pending_futures_bytes(source, width)
        .ok_or("v12 source scheduler overflow")?
        .max(
            Sq8RangeTrace::checked_pending_futures_bytes(sq8, width)
                .ok_or("v12 SQ8 scheduler overflow")?,
        ))
}

pub(super) fn validate_admission(raw: &Value, inputs: &Value) -> Result<()> {
    use borsuk::sq8_s3_range::Sq8RangeTrace;
    let traced = inputs["execution"]["trace"] == true;
    let ranges = usize::try_from(physical_cap(inputs)?)?;
    let width = usize::try_from(number(&inputs["fetch_parallelism"])?)?;
    let peak = if traced {
        Sq8RangeTrace::modeled_bytes(ranges, width)
    } else {
        0
    };
    let library_scheduler = inputs["range_scheduler_owner"] == "library";
    let both = if library_scheduler {
        library_scheduler_bytes(inputs)?
    } else {
        Sq8RangeTrace::pending_futures_bytes(128.max(ranges), width)
    };
    let pinned = if library_scheduler {
        0
    } else {
        both.checked_sub(if traced {
            Sq8RangeTrace::pending_futures_bytes(ranges, width)
        } else {
            0
        })
        .ok_or("v10 pinned subtraction")?
    };
    require(
        cfg!(target_pointer_width = "64")
            && std::mem::size_of::<Option<u64>>() == 16
            && std::mem::size_of::<std::path::PathBuf>() == 24
            && std::mem::size_of::<Vec<u8>>() == 24,
        "v10 producer sampler ABI",
    )?;
    let diagnostic = if traced {
        2 * 65536 + 2 * 49 * 16 + 24 + 24 + 131072 + 1 + 8192 + 262144
    } else {
        2 * 65536
    };
    require(
        raw["trace"] == traced
            && raw["trace_ranges"] == ranges
            && raw["panel_line_cap_bytes"] == 65536
            && raw["diagnostic_pinned_bytes"] == diagnostic
            && raw["diagnostic_bytes_per_active_query"] == diagnostic + peak
            && diagnostic + peak <= 1048576
            && raw["diagnostic_cap_bytes"] == 1048576
            && raw["host_read_cap_bytes"] == 131072
            && raw["range_state_both_paths_bytes"] == both
            && (if library_scheduler {
                raw["range_scheduler_owner"] == "library"
                    && raw["range_state_charged_by_library_bytes"] == both
                    && raw.get("range_state_pinned_by_runner_bytes").is_none()
            } else {
                raw["range_state_pinned_by_runner_bytes"] == pinned
                    && raw.get("range_scheduler_owner").is_none()
                    && raw.get("range_state_charged_by_library_bytes").is_none()
            })
            && (if library_scheduler {
                raw["trace_additional_bytes_charged_by_library"]
                    == (if traced {
                        peak - Sq8RangeTrace::checked_pending_futures_bytes(ranges, width)
                            .ok_or("v12 trace scheduler overflow")?
                    } else {
                        0
                    })
                    && raw
                        .get("trace_peak_charged_by_library_at_traced_admission")
                        .is_none()
            } else {
                raw["trace_peak_charged_by_library_at_traced_admission"] == traced
                    && raw
                        .get("trace_additional_bytes_charged_by_library")
                        .is_none()
            })
            && number(&raw["caller_pinned_bytes"])? >= (diagnostic + pinned) as u64
            && number(&raw["caller_pinned_bytes"])? <= number(&inputs["max_memory_bytes"])?,
        "split trace admission conservation",
    )?;
    require(
        if traced {
            raw["trace_retained_bytes"] == Sq8RangeTrace::retained_bytes(ranges)
                && raw["trace_peak_bytes"] == peak
        } else {
            raw["trace_retained_bytes"].is_null() && raw["trace_peak_bytes"].is_null()
        },
        "split trace reservation",
    )?;
    require(
        raw["semantics"]
            == json!({"headers_interval":"get_opts_client_queue_connect_tls_server_combined",
        "body_gaps":"application_observed_poll_gaps_not_packet_arrival","ranges":"overlap_in_time_never_sum_intervals",
        "host_counters":"host_wide_not_per_flow_null_means_unavailable","unset":"a boundary never reached is null, never a fabricated timestamp"}),
        "split exact diagnostic semantics",
    )
}

pub(super) fn validate_startup(raw: &Value, inputs: &Value) -> Result<()> {
    require(
        if inputs["range_scheduler_owner"] == "library" {
            raw["range_state_charged_by_library_bytes"] == library_scheduler_bytes(inputs)?
        } else {
            raw.get("range_state_charged_by_library_bytes").is_none()
        },
        "v12 startup scheduler conservation",
    )?;
    if inputs["range_scheduler_owner"] != "library" && inputs["execution"]["mode"] == "full" {
        // Historical baseline SOURCE reserved 128 ranges even with a 96-GET budget.
        let ranges = if inputs.get("sq8_range_split").is_some() {
            physical_cap(inputs)?
        } else {
            number(&inputs["sq8_get_budget"])?
        };
        let scheduler = borsuk::sq8_s3_range::Sq8RangeTrace::checked_pending_futures_bytes(
            128.max(usize::try_from(ranges)?),
            usize::try_from(number(&inputs["fetch_parallelism"])?)?,
        )
        .ok_or("historical scheduler overflow")?;
        // shortcut: headers omit caller allocation capacities; exact totals need separately pinned evidence.
        require(
            number(&raw["caller_pinned_bytes"])? >= scheduler as u64,
            "historical full-run scheduler reservation",
        )?;
    }
    let cap = number(&inputs["max_memory_bytes"])?;
    let dimensions = usize::try_from(number(&inputs["dimensions"])?)?;
    let codec =
        borsuk::rotated_two_bit::RotatedTwoBitCodec::required_query_scratch_bytes(dimensions)?
            as u64;
    let trace = borsuk::two_bit_generation::TwoBitPlanTrace::scratch_bytes(usize::try_from(
        number(&inputs["rows"])?,
    )?) as u64;
    let query = plus(codec, trace)?;
    require(
        cap == MEMORY_CAP
            && raw["library_cap_bytes"] == cap
            && raw["codec_scratch_bytes"] == codec
            && raw["trace_scratch_bytes"] == trace
            && raw["query_scratch_bytes"] == query
            && plus(number(&raw["caller_pinned_bytes"])?, query)? <= cap,
        "split startup memory/config conservation",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn geometry() -> (Value, Value) {
        let inputs = json!({"rows":65,"dimensions":2,"sq8_get_budget":1,
            "sq8_byte_budget":910,"fetch_parallelism":16,"sq8_range_split":null});
        let query = json!({"returned":[{"id":0,"score_bits":0},{"id":64,"score_bits":0}],
            "returned_count":2,"returned_physical_ordinals":[0,64],
            "charges":{"sq8":{"submitted_gets":1,"verified_bytes":910,"failed_gets":0}},
            "plan":{"page_rows":32,"selected_pages":[0,2],"ranges":[[0,910]],
                "logical_range_count":1,"planned_bytes":910,"target_pages":2,
                "target_shortfall":0,"primary_pages_retained":0,"covered_pages":3,"bridge_pages":1}});
        (query, inputs)
    }

    #[test]
    fn historical_full_startup_uses_fixed_source_and_physical_sq8_reservation() {
        use borsuk::{sq8_s3_range::Sq8RangeTrace, two_bit_generation::TwoBitPlanTrace};
        for width in [16, 32, 64, 128] {
            for source in [96, 128] {
                for (sq8, split, physical) in [
                    (32, None, 32),
                    (32, Some(Value::Null), 32),
                    (256, Some(Value::Null), 256),
                    (
                        32,
                        Some(json!({"max_physical_range_bytes":4096,"max_physical_gets":46})),
                        46,
                    ),
                ] {
                    let mut inputs = json!({"rows":32,"dimensions":1024,"max_memory_bytes":MEMORY_CAP,
                        "source_get_budget":source,"sq8_get_budget":sq8,"fetch_parallelism":width,
                        "execution":{"mode":"full"}});
                    if let Some(split) = split {
                        inputs["sq8_range_split"] = split;
                    }
                    let scheduler =
                        Sq8RangeTrace::pending_futures_bytes(128.max(physical), width) as u64;
                    let trace = TwoBitPlanTrace::scratch_bytes(32) as u64;
                    let query = 532480 + trace;
                    let mut raw = json!({"caller_pinned_bytes":2097152 + scheduler,
                        "library_cap_bytes":MEMORY_CAP,"codec_scratch_bytes":532480,
                        "trace_scratch_bytes":trace,"query_scratch_bytes":query});
                    validate_startup(&raw, &inputs).unwrap();
                    for caller in [0, scheduler - 1, MEMORY_CAP - query + 1, u64::MAX] {
                        raw["caller_pinned_bytes"] = json!(caller);
                        assert!(
                            validate_startup(&raw, &inputs).is_err(),
                            "source={source} width={width} physical={physical} caller={caller}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn v12_scheduler_ownership_and_trace_bytes_are_strict_and_legacy_stays_strict() {
        use borsuk::sq8_s3_range::Sq8RangeTrace;
        for traced in [false, true] {
            for (source, ranges, width, split) in [(32, 32, 16, false), (128, 46, 128, true)] {
                let peak = if traced {
                    Sq8RangeTrace::modeled_bytes(ranges, width)
                } else {
                    0
                };
                let pending = Sq8RangeTrace::pending_futures_bytes(ranges, width);
                let both = Sq8RangeTrace::pending_futures_bytes(128, width);
                let diagnostic = if traced { 534097 } else { 131072 };
                let legacy_pinned = both - if traced { pending } else { 0 };
                let mut inputs = json!({"source_get_budget":source,"sq8_get_budget":32,
                    "fetch_parallelism":width,"sq8_range_split":null,
                    "execution":{"trace":traced},"max_memory_bytes":MEMORY_CAP});
                if split {
                    inputs["sq8_range_split"] =
                        json!({"max_physical_range_bytes":4096,"max_physical_gets":ranges});
                }
                let mut raw = json!({"phase":"diagnostic_admission","execution":{"trace":traced},
                    "selected_count":1,"population_count":2,"trace":traced,"truth_opened":false,
                    "max_active_queries":1,"charged_in_caller_pinned_bytes":true,
                    "population_percentiles_valid":false,"full_cohort_qualification":false,
                    "diagnostic_bytes_per_active_query":diagnostic+peak,"diagnostic_cap_bytes":1048576,
                    "caller_pinned_bytes":diagnostic+legacy_pinned,"panel_line_cap_bytes":65536,
                    "host_read_cap_bytes":131072,"trace_ranges":ranges,"diagnostic_pinned_bytes":diagnostic,
                    "trace_retained_bytes":if traced { json!(Sq8RangeTrace::retained_bytes(ranges)) } else { Value::Null },
                    "trace_peak_bytes":if traced { json!(peak) } else { Value::Null },
                    "trace_peak_charged_by_library_at_traced_admission":traced,
                    "range_state_pinned_by_runner_bytes":legacy_pinned,"range_state_both_paths_bytes":both,
                    "semantics":{"headers_interval":"get_opts_client_queue_connect_tls_server_combined",
                        "body_gaps":"application_observed_poll_gaps_not_packet_arrival",
                        "ranges":"overlap_in_time_never_sum_intervals","host_counters":"host_wide_not_per_flow_null_means_unavailable",
                        "unset":"a boundary never reached is null, never a fabricated timestamp"}});
                validate_row(
                    &serde_json::to_vec(&raw).unwrap(),
                    "diagnostic_admission",
                    false,
                )
                .unwrap();
                validate_admission(&raw, &inputs).unwrap();
                let mut invalid_legacy = raw.clone();
                invalid_legacy["range_state_pinned_by_runner_bytes"] = json!(0);
                assert!(validate_admission(&invalid_legacy, &inputs).is_err());
                assert!(
                    validate_row(
                        &serde_json::to_vec(&raw).unwrap(),
                        "diagnostic_admission",
                        true
                    )
                    .is_err()
                );
                inputs["range_scheduler_owner"] = json!("library");
                raw.as_object_mut()
                    .unwrap()
                    .remove("range_state_pinned_by_runner_bytes");
                raw.as_object_mut()
                    .unwrap()
                    .remove("trace_peak_charged_by_library_at_traced_admission");
                raw["range_scheduler_owner"] = json!("library");
                let library = Sq8RangeTrace::checked_pending_futures_bytes(source, width)
                    .unwrap()
                    .max(pending);
                raw["range_state_both_paths_bytes"] = json!(library);
                raw["range_state_charged_by_library_bytes"] = json!(library);
                raw["trace_additional_bytes_charged_by_library"] =
                    json!(if traced { peak - pending } else { 0 });
                raw["caller_pinned_bytes"] = json!(diagnostic);
                validate_row(
                    &serde_json::to_vec(&raw).unwrap(),
                    "diagnostic_admission",
                    true,
                )
                .unwrap();
                validate_admission(&raw, &inputs).unwrap();
                assert!(
                    validate_row(
                        &serde_json::to_vec(&raw).unwrap(),
                        "diagnostic_admission",
                        false
                    )
                    .is_err()
                );
                for (key, wrong) in [
                    ("range_scheduler_owner", json!("runner")),
                    ("range_state_charged_by_library_bytes", json!(library + 1)),
                    ("range_state_both_paths_bytes", json!(library - 1)),
                    ("trace_additional_bytes_charged_by_library", json!(peak + 1)),
                    ("range_state_pinned_by_runner_bytes", json!(0)),
                    ("caller_pinned_bytes", json!(diagnostic - 1)),
                ] {
                    let mut bad = raw.clone();
                    bad[key] = wrong;
                    assert!(validate_admission(&bad, &inputs).is_err(), "{key}");
                }
            }
        }
    }

    #[test]
    fn ordinary_v10_geometry_uses_input_dimensions_budgets_and_partial_tail() {
        let (mut query, mut inputs) = geometry();
        validate_geometry(&query, &inputs).unwrap();
        inputs["sq8_range_split"] = json!({"max_physical_range_bytes":448,"max_physical_gets":3});
        query["charges"]["sq8"]["submitted_gets"] = json!(3);
        validate_geometry(&query, &inputs).unwrap();
        assert_eq!(
            physical_ranges(&query, &inputs).unwrap(),
            vec![json!([0, 448]), json!([448, 896]), json!([896, 910])]
        );
        for (pointer, value) in [
            ("/plan/logical_range_count", json!(2)),
            ("/plan/ranges/0/1", json!(896)),
            ("/plan/covered_pages", json!(2)),
            ("/plan/bridge_pages", json!(0)),
            ("/returned_physical_ordinals/1", json!(65)),
            ("/returned_physical_ordinals/1", json!(0)),
            ("/returned_count", json!(1)),
            ("/charges/sq8/submitted_gets", json!(1)),
            ("/charges/sq8/verified_bytes", json!(909)),
        ] {
            let mut bad = query.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(validate_geometry(&bad, &inputs).is_err(), "{pointer}");
        }
        inputs["sq8_range_split"]["max_physical_gets"] = json!(2);
        assert!(validate_geometry(&query, &inputs).is_err());
        inputs["sq8_range_split"] = json!({"max_physical_range_bytes":14,"max_physical_gets":1});
        query["plan"] = json!({"page_rows":32,"selected_pages":[2],"ranges":[[896,910]],
            "logical_range_count":1,"planned_bytes":14,"target_pages":1,"target_shortfall":0,
            "primary_pages_retained":0,"covered_pages":1,"bridge_pages":0});
        query["returned"] = json!([{"id":64,"score_bits":0}]);
        query["returned_count"] = json!(1);
        query["returned_physical_ordinals"] = json!([64]);
        query["charges"]["sq8"] = json!({"submitted_gets":1,"verified_bytes":14,"failed_gets":0});
        validate_geometry(&query, &inputs).unwrap();
        inputs["sq8_range_split"]["max_physical_range_bytes"] = json!(13);
        assert!(validate_geometry(&query, &inputs).is_err());
    }

    #[test]
    fn ordinary_v10_refuses_uncovered_ordinals_and_duplicate_unknown_missing_fields() {
        let (mut query, mut inputs) = geometry();
        inputs["sq8_get_budget"] = json!(2);
        query["plan"]["ranges"] = json!([[0, 448], [896, 910]]);
        query["plan"]["logical_range_count"] = json!(2);
        query["plan"]["planned_bytes"] = json!(462);
        query["plan"]["covered_pages"] = json!(2);
        query["plan"]["bridge_pages"] = json!(0);
        query["charges"]["sq8"] = json!({"submitted_gets":2,"verified_bytes":462,"failed_gets":0});
        validate_geometry(&query, &inputs).unwrap();
        query["returned_physical_ordinals"][1] = json!(32);
        assert!(
            validate_geometry(&query, &inputs)
                .unwrap_err()
                .to_string()
                .contains("outside logical coverage")
        );
        let duplicate = br#"{"phase":"identity","schema":"borsuk-cohere-native-baseline-result-v10","schema":"borsuk-cohere-native-baseline-result-v10"}"#;
        assert!(
            validate_row(duplicate, "identity", false)
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
        for split in [
            json!({}),
            json!({"max_physical_range_bytes":0,"max_physical_gets":1}),
            json!({"max_physical_range_bytes":448,"max_physical_gets":129}),
            json!({"max_physical_range_bytes":448,"max_physical_gets":1,"extra":true}),
        ] {
            inputs["sq8_range_split"] = split;
            assert!(physical_cap(&inputs).is_err());
        }
    }
}
