//! Strict current baseline evidence. Formal experiments add their own frozen pins.
use super::{CompletedConfig, Result, fields, plus, require};
use serde_json::{Value, json};

pub(super) const RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v10";
pub(super) const READ_RETRY_RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v11";
pub(super) const LIBRARY_SCHEDULER_RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v12";
pub(super) const SOURCE_LAYOUT_RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v13";
pub(super) const SOURCE_LAYOUT_CONFIG_SCHEMA: &str = "borsuk-cohere-native-baseline-config-v12";
pub(super) const ONEPASS_RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v14";
pub(super) const ONEPASS_CONFIG_SCHEMA: &str = "borsuk-cohere-native-baseline-config-v13";
pub(super) const SOURCE_CAPSULE_VERIFICATION_POLICY: &str = "capsule_sha256_original_units_once_v1";
pub(super) const PAGE_RANGE_RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v15";
pub(super) const PAGE_RANGE_CONFIG_SCHEMA: &str = "borsuk-cohere-native-baseline-config-v14";
pub(super) const CPU_PROFILE_RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v16";
pub(super) const CPU_PROFILE_CONFIG_SCHEMA: &str = "borsuk-cohere-native-baseline-config-v15";

fn cpu_profile_config(inputs: &Value) -> bool {
    inputs["config_schema"] == CPU_PROFILE_CONFIG_SCHEMA
}

fn page_range_config(schema: &Value) -> bool {
    matches!(
        schema.as_str(),
        Some(PAGE_RANGE_CONFIG_SCHEMA | CPU_PROFILE_CONFIG_SCHEMA)
    )
}

fn page_range_result(schema: &Value) -> bool {
    matches!(
        schema.as_str(),
        Some(PAGE_RANGE_RESULT_SCHEMA | CPU_PROFILE_RESULT_SCHEMA)
    )
}

pub(super) const PAGE_RANGE_VERIFICATION_POLICY: &str = "directory_page_ranges_original_units_v2";

fn layout_result_schema(config: &Value) -> Result<&'static str> {
    match config.as_str() {
        Some(SOURCE_LAYOUT_CONFIG_SCHEMA) => Ok(SOURCE_LAYOUT_RESULT_SCHEMA),
        Some(ONEPASS_CONFIG_SCHEMA) => Ok(ONEPASS_RESULT_SCHEMA),
        Some(PAGE_RANGE_CONFIG_SCHEMA) => Ok(PAGE_RANGE_RESULT_SCHEMA),
        Some(CPU_PROFILE_CONFIG_SCHEMA) => Ok(CPU_PROFILE_RESULT_SCHEMA),
        _ => Err("exact SOURCE layout config schema".into()),
    }
}

const READ_RETRY_CONFIG_SCHEMA: &str = "borsuk-cohere-native-baseline-config-v11";
const SCOPE_NOTE: &str = "diagnostic selected original ordinals or prefix; not population p99, population recall or full-cohort qualification";
const MEMORY_CAP: u64 = 512 * 1024 * 1024;
// Two independently bounded manifests, worst-case JSON escaping, plus the old panel envelope.
pub(super) const SOURCE_BINDING_LINE_CAP: usize = 2 * 6 * 65_536 + 65_536;
const PLAN_FIELDS: &str = "page_rows selected_pages ranges logical_range_count planned_bytes target_pages target_shortfall primary_pages_retained covered_pages bridge_pages";

pub(super) fn is_result_schema(schema: &str) -> bool {
    matches!(
        schema,
        RESULT_SCHEMA
            | READ_RETRY_RESULT_SCHEMA
            | LIBRARY_SCHEDULER_RESULT_SCHEMA
            | SOURCE_LAYOUT_RESULT_SCHEMA
            | ONEPASS_RESULT_SCHEMA
            | PAGE_RANGE_RESULT_SCHEMA
            | CPU_PROFILE_RESULT_SCHEMA
    )
}

pub(super) fn is_source_layout_schema(schema: &Value) -> bool {
    matches!(
        schema.as_str(),
        Some(
            SOURCE_LAYOUT_RESULT_SCHEMA
                | ONEPASS_RESULT_SCHEMA
                | PAGE_RANGE_RESULT_SCHEMA
                | CPU_PROFILE_RESULT_SCHEMA
        )
    )
}

pub(super) fn is_source_layout_config(header: &Value) -> bool {
    matches!(
        header["config_schema"].as_str(),
        Some(
            SOURCE_LAYOUT_CONFIG_SCHEMA
                | ONEPASS_CONFIG_SCHEMA
                | PAGE_RANGE_CONFIG_SCHEMA
                | CPU_PROFILE_CONFIG_SCHEMA
        )
    )
}

fn number(v: &Value) -> Result<u64> {
    v.as_u64().ok_or_else(|| "v10 unsigned integer".into())
}

fn memory_cap(header: &Value) -> Result<u64> {
    if is_source_layout_config(header) {
        let cap = number(&header["max_memory_bytes"])?;
        require(
            cap > 0 && usize::try_from(cap).is_ok(),
            "v13 positive memory cap with checked width",
        )?;
        Ok(cap)
    } else {
        Ok(MEMORY_CAP)
    }
}

fn split_option(header: &Value) -> Result<()> {
    let split = header
        .get("sq8_range_split")
        .ok_or("v10 split field required")?;
    if !split.is_null() {
        fields(split, "max_physical_range_bytes max_physical_gets")?;
        let cap = memory_cap(header)?;
        require(
            number(&split["max_physical_range_bytes"]).is_ok_and(|n| n > 0 && n <= cap)
                && matches!(split["max_physical_gets"].as_u64(), Some(1..=128)),
            "v10 split caps",
        )?;
    }
    Ok(())
}

fn source_authentication_charge(inputs: &Value) -> Result<Option<u64>> {
    use borsuk::sq8_s3_range::SourceAuthentication;
    let Some(value) = inputs.get("source_authentication") else {
        return Ok(None);
    };
    let policy = match value.as_str() {
        Some("inline") => SourceAuthentication::Inline,
        Some("offload2") => SourceAuthentication::Offload2,
        _ => return Err("SOURCE authentication selector".into()),
    };
    require(
        cpu_profile_config(inputs) && inputs["range_scheduler_owner"] == "library",
        "SOURCE authentication versioned library scheduler",
    )?;
    Ok(Some(
        policy
            .additional_memory_bytes(
                usize::try_from(number(&inputs["fetch_parallelism"])?)?,
                usize::try_from(number(&inputs["source_get_budget"])?)?,
                1,
            )
            .ok_or("SOURCE authentication memory overflow")?,
    ))
}

fn validate_source_authentication_charge(raw: &Value, inputs: &Value) -> Result<()> {
    match source_authentication_charge(inputs)? {
        Some(bytes) => require(
            raw.get("source_authentication_extra_library_bytes") == Some(&json!(bytes)),
            "SOURCE authentication exact library reservation",
        ),
        None => require(
            raw.get("source_authentication_extra_library_bytes")
                .is_none(),
            "historical SOURCE authentication reservation absent",
        ),
    }
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
    validate_row_envelope(line, phase, library_scheduler, false, false)
}

pub(super) fn validate_row_envelope(
    line: &[u8],
    phase: &str,
    library_scheduler: bool,
    source_layout: bool,
    cpu_profile: bool,
) -> Result<()> {
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
            let mut profile_fields = if cpu_profile {
                "source_trace_ranges".to_owned()
            } else {
                String::new()
            };
            if raw
                .get("source_authentication_extra_library_bytes")
                .is_some()
            {
                require(
                    cpu_profile && library_scheduler,
                    "SOURCE authentication admission version",
                )?;
                number(&raw["source_authentication_extra_library_bytes"])?;
                profile_fields.push_str(" source_authentication_extra_library_bytes");
            }
            fields(
                &raw,
                &format!(
                    "phase execution selected_count population_count trace diagnostic_bytes_per_active_query diagnostic_cap_bytes max_active_queries charged_in_caller_pinned_bytes caller_pinned_bytes panel_line_cap_bytes host_read_cap_bytes trace_ranges population_percentiles_valid full_cohort_qualification semantics truth_opened trace_retained_bytes trace_peak_bytes diagnostic_pinned_bytes range_state_both_paths_bytes {scheduler_fields} {profile_fields}"
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
    if source_layout {
        match phase {
            "identity" => roster.push_str(" source_capsule_source_sha256 source_layout source_byte_budget max_memory_bytes config_schema"),
            "bound_inputs" => roster.push_str(" source_layout source_byte_budget config_schema"),
            "startup" => roster.push_str(" source_layout"),
            "source_binding" => roster.push_str(" generation_authority"),
            _ => (),
        }
    }
    if source_layout
        && matches!(
            raw["config_schema"].as_str(),
            Some(ONEPASS_CONFIG_SCHEMA | PAGE_RANGE_CONFIG_SCHEMA | CPU_PROFILE_CONFIG_SCHEMA)
        )
        && matches!(phase, "identity" | "bound_inputs")
    {
        roster.push_str(" source_capsule_verification_policy");
    }
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
        "bound_inputs" => {
            raw.get("s3_read_retry").is_some()
                || (!source_layout && raw.get("config_schema").is_some())
        }
        _ => false,
    };
    if retry {
        roster.push_str(if source_layout {
            " s3_read_retry"
        } else {
            " config_schema s3_read_retry"
        });
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
    if matches!(phase, "identity" | "bound_inputs") && raw.get("source_authentication").is_some() {
        source_authentication_charge(&raw)?;
        roster.push_str(" source_authentication");
    }
    if phase == "startup"
        && raw
            .get("source_authentication_extra_library_bytes")
            .is_some()
    {
        require(
            cpu_profile && library_scheduler,
            "SOURCE authentication startup version",
        )?;
        number(&raw["source_authentication_extra_library_bytes"])?;
        roster.push_str(" source_authentication_extra_library_bytes");
    }
    fields(&raw, &roster)?;
    if retry {
        require(
            raw["config_schema"]
                == if source_layout {
                    match raw["config_schema"].as_str() {
                        Some(PAGE_RANGE_CONFIG_SCHEMA) => PAGE_RANGE_CONFIG_SCHEMA,
                        Some(CPU_PROFILE_CONFIG_SCHEMA) => CPU_PROFILE_CONFIG_SCHEMA,
                        Some(ONEPASS_CONFIG_SCHEMA) => ONEPASS_CONFIG_SCHEMA,
                        _ => SOURCE_LAYOUT_CONFIG_SCHEMA,
                    }
                } else {
                    READ_RETRY_CONFIG_SCHEMA
                }
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
    if source_layout && matches!(phase, "identity" | "bound_inputs") {
        validate_layout_header(&raw)?;
        if phase == "identity" {
            require(
                raw["source_capsule_source_sha256"]
                    .as_str()
                    .is_some_and(super::valid_sha),
                "v13 capsule component SHA256",
            )?;
        }
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
            let cap = memory_cap(&raw)?;
            require(
                matches!(raw["fetch_parallelism"].as_u64(), Some(16 | 32 | 64 | 128))
                    && matches!(raw["source_get_budget"].as_u64(), Some(96 | 128))
                    && matches!(raw["sq8_get_budget"].as_u64(), Some(1..=256))
                    && number(&raw["sq8_byte_budget"]).is_ok_and(|n| n > 0 && n <= cap),
                "v10 explicit budgets/width",
            )?;
            if let Some(window) = raw.get("shared_byte_window") {
                if source_layout {
                    super::validate_byte_window_cap(
                        window,
                        number(&raw["fetch_parallelism"])?,
                        memory_cap(&raw)?,
                    )?;
                } else {
                    super::validate_byte_window(window, number(&raw["fetch_parallelism"])?)?;
                }
            }
            if phase == "identity" {
                require(
                    if library_scheduler {
                        raw["schema"]
                            == if source_layout {
                                layout_result_schema(&raw["config_schema"])?
                            } else {
                                LIBRARY_SCHEDULER_RESULT_SCHEMA
                            }
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
                    number(state).is_ok_and(|n| {
                        n > 0
                            && n <= if source_layout {
                                raw["library_cap_bytes"].as_u64().unwrap_or(0)
                            } else {
                                MEMORY_CAP
                            }
                    }),
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
                diagnostic_shape(diag, cpu_profile)?;
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
    super::validate_native_row_values_for_layout(&raw, phase, source_layout)
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
        let layout = is_source_layout_schema(&c.expected_identity["schema"]);
        validate_row_envelope(
            &bytes,
            phase,
            layout || c.expected_identity["schema"] == LIBRARY_SCHEDULER_RESULT_SCHEMA,
            layout,
            c.expected_identity["schema"] == CPU_PROFILE_RESULT_SCHEMA,
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
        "source_layout",
        "source_byte_budget",
        "source_capsule_verification_policy",
        "source_authentication",
    ] {
        require(i.get(key) == b.get(key), "v10 identity/input agreement")?;
    }
    if is_source_layout_schema(&i["schema"]) {
        require(
            i["max_memory_bytes"] == b["max_memory_bytes"],
            "v13 memory header agreement",
        )?;
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
            && (1..=memory_cap(inputs)?).contains(&max_bytes),
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
        diagnostic_shape(diag, cpu_profile_config(inputs))?;
        if diag["schema"] == "borsuk-sq8-range-attribution-diagnostic-v5" {
            validate_source_trace(raw, inputs)?;
        }
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

pub(super) const LEGACY_TRACE_FIELDS: &str = "range_fields outcome planned_ranges planned_bytes max_parallel dropped_ranges fetch_start_ns all_ranges_complete_ns rank_start_ns rank_end_ns release_end_ns rank ranges";
const PROFILE_TRACE_FIELDS: &str = "cpu regroup regroup_copied_bytes rank_thread_cpu_ns rank_clock_reads retained_payload_bytes phase_process_cpu_ns unattributed_process_cpu_ns phase_clock_reads";

// Frozen 6cf56cd x86_64 ABI: span=264, trace=240, probe=280, allocator=32.
// Current CPU aggregates change trace/probe sizes; never reinterpret old evidence with them.
pub(super) fn historical_trace_retained(ranges: usize) -> usize {
    240usize.saturating_add(264usize.saturating_mul(ranges))
}

pub(super) fn historical_trace_peak(ranges: usize, width: usize) -> usize {
    historical_trace_retained(ranges)
        .saturating_add(ranges.saturating_mul(280 + 32))
        .saturating_add(borsuk::sq8_s3_range::Sq8RangeTrace::pending_futures_bytes(
            ranges, width,
        ))
}

pub(super) fn historical_semantics() -> Value {
    json!({"headers_interval":"get_opts_client_queue_connect_tls_server_combined",
        "body_gaps":"application_observed_poll_gaps_not_packet_arrival",
        "ranges":"overlap_in_time_never_sum_intervals",
        "host_counters":"host_wide_not_per_flow_null_means_unavailable",
        "unset":"a boundary never reached is null, never a fabricated timestamp"})
}

fn cpu_wall(value: &Value, quantum: u64, aggregate: bool) -> Result<()> {
    if value.is_null() {
        return Ok(());
    }
    fields(value, "thread_cpu_ns wall_ns clock_reads")?;
    number(&value["thread_cpu_ns"])?;
    number(&value["wall_ns"])?;
    let reads = number(&value["clock_reads"])?;
    require(
        reads > 0
            && if aggregate {
                reads.is_multiple_of(quantum)
            } else {
                reads == quantum
            },
        "synchronous CPU scope clock accounting",
    )
}

fn range_cpu(value: &Value) -> Result<()> {
    fields(
        value,
        "collector_allocation collector_copy owned_allocation owned_copy authentication collector_copied_bytes owned_copied_bytes",
    )?;
    for key in [
        "collector_allocation",
        "collector_copy",
        "owned_allocation",
        "owned_copy",
        "authentication",
    ] {
        // Authentication reuses wall stamps; the other scopes add both clock pairs.
        cpu_wall(
            &value[key],
            if key == "authentication" { 2 } else { 4 },
            true,
        )?;
    }
    number(&value["collector_copied_bytes"])?;
    number(&value["owned_copied_bytes"])?;
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) enum DiagnosticModel {
    Sq8V3,
    SourceSummaryV4,
    SourceRangesV5,
}

impl DiagnosticModel {
    pub(super) fn line_cap(self) -> usize {
        match self {
            Self::Sq8V3 | Self::SourceSummaryV4 => 65_536,
            Self::SourceRangesV5 => 131_072,
        }
    }

    pub(super) fn bind(self, diag: &Value) -> Result<()> {
        require(
            diag["schema"]
                == match self {
                    Self::Sq8V3 => "borsuk-sq8-range-attribution-diagnostic-v3",
                    Self::SourceSummaryV4 => "borsuk-sq8-range-attribution-diagnostic-v4",
                    Self::SourceRangesV5 => "borsuk-sq8-range-attribution-diagnostic-v5",
                },
            "diagnostic schema/admission model agreement",
        )
    }
}

fn profile_shape(diag: &Value, full_source: bool) -> Result<()> {
    let profile = &diag["cpu_profile"];
    fields(profile, "discovery cover planning acquisition_poll_cpu")?;
    for key in ["discovery", "cover", "planning"] {
        cpu_wall(&profile[key], 4, false)?;
    }
    require(
        profile["acquisition_poll_cpu"] == "unresolved_not_instrumented",
        "unresolved acquisition poll CPU",
    )?;
    let source = &diag["source_range_trace"];
    fields(
        source,
        &if full_source {
            format!("{LEGACY_TRACE_FIELDS} {PROFILE_TRACE_FIELDS}")
        } else {
            "outcome planned_ranges planned_bytes max_parallel fetch_start_ns all_ranges_complete_ns release_end_ns retained_payload_bytes cpu phase_process_cpu_ns unattributed_process_cpu_ns phase_clock_reads last_completing_range".into()
        },
    )?;
    let sq8 = &diag["sq8_range_trace"];
    fields(
        sq8,
        &format!("{LEGACY_TRACE_FIELDS} {PROFILE_TRACE_FIELDS}"),
    )?;
    for trace in [source, sq8] {
        require(trace["outcome"].is_string(), "profile range outcome")?;
        for key in [
            "planned_ranges",
            "planned_bytes",
            "max_parallel",
            "phase_clock_reads",
        ] {
            number(&trace[key])?;
        }
        for key in [
            "fetch_start_ns",
            "all_ranges_complete_ns",
            "release_end_ns",
            "retained_payload_bytes",
            "phase_process_cpu_ns",
            "unattributed_process_cpu_ns",
        ] {
            require(
                trace[key].is_null() || trace[key].as_u64().is_some(),
                "nullable profile counter",
            )?;
        }
        range_cpu(&trace["cpu"])?;
    }
    if !full_source {
        let last = &source["last_completing_range"];
        require(
            last.is_null() || last.as_array().is_some_and(|r| r.len() == 22),
            "SOURCE last range width",
        )?;
    }
    cpu_wall(&sq8["regroup"], 4, false)?;
    number(&sq8["regroup_copied_bytes"])?;
    number(&sq8["rank_clock_reads"])?;
    require(
        sq8["rank_thread_cpu_ns"].is_null() || sq8["rank_thread_cpu_ns"].as_u64().is_some(),
        "nullable SQ8 rank CPU",
    )
}

pub(super) fn diagnostic_shape(diag: &Value, cpu_profile: bool) -> Result<()> {
    fields(
        diag,
        &format!(
            "schema query_monotonic_start_ns query_monotonic_end_ns process_cpu_start_ns process_cpu_end_ns sq8_range_trace host {}",
            if cpu_profile {
                "source_range_trace cpu_profile"
            } else {
                ""
            }
        ),
    )?;
    let full_source = diag["schema"] == "borsuk-sq8-range-attribution-diagnostic-v5";
    require(
        (if cpu_profile {
            full_source || diag["schema"] == "borsuk-sq8-range-attribution-diagnostic-v4"
        } else {
            diag["schema"] == "borsuk-sq8-range-attribution-diagnostic-v3"
        }) && diag["sq8_range_trace"].is_object()
            && diag["host"].is_object(),
        "panel diagnostic schema",
    )?;
    if cpu_profile {
        profile_shape(diag, full_source)?;
    } else {
        // Historical SQ8-only records never carried CPU-profile fields.
        fields(&diag["sq8_range_trace"], LEGACY_TRACE_FIELDS)?;
    }
    Ok(())
}

fn validate_source_trace(raw: &Value, inputs: &Value) -> Result<()> {
    let trace = &raw["diagnostic"]["source_range_trace"];
    fields(
        trace,
        &format!("{LEGACY_TRACE_FIELDS} {PROFILE_TRACE_FIELDS}"),
    )?;
    range_cpu(&trace["cpu"])?;
    let ranges = trace["ranges"].as_array().ok_or("SOURCE range rows")?;
    require(
        trace["range_fields"] == json!(borsuk::sq8_s3_range::RangeSpan::FIELDS)
            && trace["dropped_ranges"] == 0
            && trace["rank_start_ns"].is_null()
            && trace["rank_end_ns"].is_null()
            && trace["rank"].is_null()
            && trace["regroup"].is_null()
            && trace["regroup_copied_bytes"] == 0
            && trace["rank_thread_cpu_ns"].is_null()
            && trace["rank_clock_reads"] == 0,
        "SOURCE exact range roster and non-ranking phase",
    )?;
    let charge = &raw["charges"]["source"];
    if inputs["serving"]["mode"] == "direct_closure" {
        require(
            raw["source_nomination_skipped"] == true
                && charge == &json!({"submitted_gets":0,"verified_bytes":0,"failed_gets":0})
                && trace["outcome"] == "started"
                && ranges.is_empty()
                && [
                    "planned_ranges",
                    "planned_bytes",
                    "max_parallel",
                    "phase_clock_reads",
                ]
                .iter()
                .all(|key| trace[*key] == 0)
                && [
                    "fetch_start_ns",
                    "all_ranges_complete_ns",
                    "release_end_ns",
                    "retained_payload_bytes",
                    "phase_process_cpu_ns",
                    "unattributed_process_cpu_ns",
                ]
                .iter()
                .all(|key| trace[*key].is_null())
                && trace["cpu"]
                    == json!({"collector_allocation":null,"collector_copy":null,
                    "owned_allocation":null,"owned_copy":null,"authentication":null,
                    "collector_copied_bytes":0,"owned_copied_bytes":0}),
            "direct query must leave SOURCE trace and charges empty",
        )?;
        return Ok(());
    }
    require(
        raw["source_nomination_skipped"] == false
            && trace["outcome"] == "fetched"
            && !ranges.is_empty()
            && ranges.len() as u64 <= number(&inputs["source_get_budget"])?
            && trace["planned_ranges"] == ranges.len()
            && trace["max_parallel"]
                == (ranges.len() as u64).min(number(&inputs["fetch_parallelism"])?)
            && trace["phase_clock_reads"] == 2,
        "SOURCE complete bounded range plan",
    )?;
    let flat = inputs["source_layout"]["layout"] == "flat_records";
    require(
        flat || inputs["source_layout"]["layout"] == "leaf_capsules_v2",
        "SOURCE diagnostic layout",
    )?;
    let width = super::source_record_bytes(usize::try_from(number(&inputs["dimensions"])?)?)?;
    let plan = if flat {
        let pages = raw["trace"]["ranked_candidate_pages"]
            .as_array()
            .ok_or("SOURCE discovery closure")?
            .iter()
            .map(|page| Ok(usize::try_from(number(page)?)?))
            .collect::<Result<std::collections::BTreeSet<_>>>()?;
        Some(
            super::source_cover::cover_pages(
                &pages,
                usize::try_from(number(&inputs["rows"])?)?,
                width,
                256,
                usize::try_from(number(&inputs["source_get_budget"])?)?,
            )
            .map_err(|e| format!("SOURCE diagnostic cover: {e:?}"))?
            .0,
        )
    } else {
        None
    };
    if let Some(plan) = &plan {
        require(
            plan.len() == ranges.len(),
            "SOURCE canonical cover range count",
        )?;
    }
    let fetch = number(&trace["fetch_start_ns"])?;
    let barrier = number(&trace["all_ranges_complete_ns"])?;
    let release = number(&trace["release_end_ns"])?;
    let stages = &raw["stages"];
    require(
        number(&stages["source"]["start_ns"])? <= fetch
            && fetch <= barrier
            && barrier <= number(&stages["source"]["end_ns"])?
            && number(&stages["source"]["end_ns"])? <= number(&stages["planning"]["start_ns"])?
            && number(&stages["planning"]["start_ns"])? <= number(&stages["planning"]["end_ns"])?
            && number(&stages["planning"]["end_ns"])? <= release
            && release <= number(&stages["sq8"]["start_ns"])?,
        "SOURCE fetch/planning/release stage order",
    )?;
    let mut bytes = 0;
    let mut previous_end = 0;
    for (index, row) in ranges.iter().enumerate() {
        let row = row.as_array().ok_or("SOURCE compact range row")?;
        require(row.len() == 22, "SOURCE range column count")?;
        let n = |column: usize| number(&row[column]);
        let (start, end) = (n(3)?, n(4)?);
        require(
            n(0)? == index as u64
                && row[21] == "ok"
                && start >= previous_end
                && start < end
                && n(16)? == end - start
                && (1..=u64::from(u32::MAX)).contains(&n(15)?)
                && n(20)? == n(15)?,
            "SOURCE ordered physical range identity/bytes/chunks",
        )?;
        if let Some(plan) = &plan {
            let unit = 32 * width as u64;
            require(
                start == plan[index].start as u64
                    && end == plan[index].end as u64
                    && n(1)? == start / unit
                    && n(2)? == (end - 1) / unit,
                "SOURCE canonical physical cover/page bounds",
            )?;
        } else {
            // The wire plan carries physical offsets, not the pinned binary directory's owners.
            require(
                n(1)? == u64::MAX
                    && n(2)? == u64::MAX
                    && end <= number(&inputs["source_layout"]["blob_bytes"])?,
                "SOURCE capsule physical bounds and page sentinels",
            )?;
        }
        let mut previous = fetch;
        for column in 5..=14 {
            let stamp = n(column)?;
            require(
                previous <= stamp && stamp <= barrier,
                "SOURCE range timestamp order",
            )?;
            previous = stamp;
        }
        let chunks = n(15)?;
        let gap = n(17)?;
        let gap_end = if row[18].is_null() {
            None
        } else {
            Some(n(18)?)
        };
        let copy = n(19)?;
        let body = n(11)? - n(8)?;
        require(
            (chunks != 1 || n(9)? == n(10)?)
                && plus(gap, copy)? <= body
                && n(11)? - n(9)?
                    <= chunks
                        .checked_mul(gap)
                        .and_then(|wait| copy.checked_add(wait))
                        .ok_or("SOURCE body gap overflow")?,
            "SOURCE single chunk/copy/body gap coverage",
        )?;
        if gap == 0 {
            require(gap_end.is_none(), "SOURCE zero gap boundary")?;
        } else {
            let end = gap_end.ok_or("SOURCE missing gap boundary")?;
            let start = end.checked_sub(gap).ok_or("SOURCE gap underflow")?;
            require(
                (chunks > 2 || end == n(9)? || end == n(11)? || (chunks == 2 && end == n(10)?))
                    && start >= n(8)?
                    && end >= n(9)?
                    && end <= n(11)?
                    && (end <= n(10)? || end == n(11)?)
                    && !(start < n(9)? && end > n(9)?)
                    && !(start < n(10)? && end > n(10)?),
                "SOURCE body gap bounds",
            )?;
        }
        bytes = plus(bytes, end - start)?;
        previous_end = end;
    }
    require(
        trace["planned_bytes"] == bytes
            && trace["retained_payload_bytes"] == bytes
            && bytes <= number(&inputs["source_byte_budget"])?
            && charge
                == &json!({"submitted_gets":ranges.len(),"verified_bytes":bytes,"failed_gets":0}),
        "SOURCE immutable charge conservation",
    )?;
    let cpu = &trace["cpu"];
    let measured = [
        "collector_allocation",
        "collector_copy",
        "owned_allocation",
        "owned_copy",
        "authentication",
    ]
    .iter()
    .try_fold(0u64, |sum, key| -> Result<u64> {
        Ok(sum.saturating_add(if cpu[*key].is_null() {
            0
        } else {
            number(&cpu[*key]["thread_cpu_ns"])?
        }))
    })?;
    require(
        trace["unattributed_process_cpu_ns"]
            == number(&trace["phase_process_cpu_ns"])?.saturating_sub(measured),
        "SOURCE process CPU residual conservation",
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

pub(super) fn validate_admission(raw: &Value, inputs: &Value) -> Result<DiagnosticModel> {
    validate_source_authentication_charge(raw, inputs)?;
    use borsuk::sq8_s3_range::Sq8RangeTrace;
    let traced = inputs["execution"]["trace"] == true;
    let ranges = usize::try_from(physical_cap(inputs)?)?;
    let width = usize::try_from(number(&inputs["fetch_parallelism"])?)?;
    let profile = cpu_profile_config(inputs);
    let source = usize::try_from(number(&inputs["source_get_budget"])?)?;
    let peak = if !traced {
        0
    } else if profile {
        borsuk::sq8_s3_range::QueryProfile::modeled_bytes(source, ranges, width, width)
    } else {
        historical_trace_peak(ranges, width)
    };
    require(
        if profile {
            raw["source_trace_ranges"] == source
        } else {
            raw.get("source_trace_ranges").is_none()
        },
        "versioned SOURCE trace capacity",
    )?;
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
    // Trace-OFF has no query schema: admit only the two frozen panel models.
    let model = match (profile, number(&raw["panel_line_cap_bytes"])?) {
        (false, 65_536) => DiagnosticModel::Sq8V3,
        (true, 65_536) => DiagnosticModel::SourceSummaryV4,
        (true, 131_072) => DiagnosticModel::SourceRangesV5,
        _ => return Err("versioned diagnostic panel memory model".into()),
    };
    let diagnostic = 2 * model.line_cap()
        + if traced {
            2 * 49 * 16 + 24 + 24 + 131072 + 1 + 8192 + 262144
        } else {
            0
        };
    require(
        raw["trace"] == traced
            && raw["trace_ranges"] == ranges
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
                        if profile {
                            borsuk::sq8_s3_range::QueryProfile::additional_bytes(source, ranges)
                        } else {
                            peak - Sq8RangeTrace::checked_pending_futures_bytes(ranges, width)
                                .ok_or("v12 trace scheduler overflow")?
                        }
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
            raw["trace_retained_bytes"]
                == if profile {
                    borsuk::sq8_s3_range::QueryProfile::retained_bytes(source, ranges)
                } else {
                    historical_trace_retained(ranges)
                }
                && raw["trace_peak_bytes"] == peak
        } else {
            raw["trace_retained_bytes"].is_null() && raw["trace_peak_bytes"].is_null()
        },
        "split trace reservation",
    )?;
    let mut semantics = historical_semantics();
    if profile {
        semantics["cpu_residual"] = json!("unattributed_process_cpu_not_tls_or_network_cpu");
        semantics["poll_cpu"] = json!("unresolved_not_instrumented");
        semantics["capsule_page_bounds"] = json!("u64_max_means_no_single_logical_page_interval");
    }
    require(
        raw["semantics"] == semantics,
        "split exact diagnostic semantics",
    )?;
    Ok(model)
}

pub(super) fn validate_startup(raw: &Value, inputs: &Value) -> Result<()> {
    validate_source_authentication_charge(raw, inputs)?;
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
        cap == memory_cap(inputs)?
            && raw["library_cap_bytes"] == cap
            && raw["codec_scratch_bytes"] == codec
            && raw["trace_scratch_bytes"] == trace
            && raw["query_scratch_bytes"] == query
            && plus(number(&raw["caller_pinned_bytes"])?, query)? <= cap,
        "split startup memory/config conservation",
    )
}

fn validate_layout_header(raw: &Value) -> Result<()> {
    require(
        is_source_layout_config(raw)
            && memory_cap(raw)? > 0
            && raw["source_byte_budget"]
                .as_u64()
                .is_some_and(|n| n > 0 && usize::try_from(n).is_ok()),
        "v13 explicit config/memory/SOURCE budget",
    )?;
    require(
        match raw["config_schema"].as_str() {
            Some(PAGE_RANGE_CONFIG_SCHEMA | CPU_PROFILE_CONFIG_SCHEMA) => {
                raw["source_capsule_verification_policy"] == PAGE_RANGE_VERIFICATION_POLICY
            }
            Some(ONEPASS_CONFIG_SCHEMA) => {
                raw["source_capsule_verification_policy"] == SOURCE_CAPSULE_VERIFICATION_POLICY
            }
            _ => raw.get("source_capsule_verification_policy").is_none(),
        },
        "exact SOURCE capsule verification policy for config schema",
    )?;
    // Historical wire layouts are evidence, independent of the production enum.
    let layout = &raw["source_layout"];
    let flat = layout["layout"] == "flat_records";
    fields(
        layout,
        if flat {
            "layout"
        } else {
            "layout blob_bytes blob_sha256 directory_bytes directory_sha256"
        },
    )?;
    if !flat {
        let v2 = page_range_config(&raw["config_schema"]);
        let directory = number(&layout["directory_bytes"])?;
        require(
            layout["layout"]
                == if v2 {
                    "leaf_capsules_v2"
                } else {
                    "leaf_capsules"
                }
                && number(&layout["blob_bytes"])? > 0
                && number(&layout["blob_bytes"])? <= usize::MAX as u64
                && directory <= usize::MAX as u64
                && if v2 {
                    directory >= 100 && (directory - 32) % 4 == 0
                } else {
                    directory >= 96 && (directory - 32) % 64 == 0
                },
            "v13 SOURCE capsule descriptor",
        )?;
        for key in ["blob_sha256", "directory_sha256"] {
            require(
                layout[key].as_str().is_some_and(super::valid_sha),
                "v13 SOURCE capsule SHA256",
            )?;
        }
    }
    Ok(())
}

fn generation_manifests(raw: &Value, inputs: &Value) -> Result<(Value, Value)> {
    use sha2::{Digest, Sha256};
    fields(raw, "root_manifest page_manifest")?;
    let root_body = raw["root_manifest"]
        .as_str()
        .ok_or("v13 root manifest bytes")?;
    let page_body = raw["page_manifest"]
        .as_str()
        .ok_or("v13 page manifest bytes")?;
    require(
        !root_body.is_empty()
            && root_body.len() <= 65536
            && !page_body.is_empty()
            && page_body.len() <= 65536,
        "v13 manifest byte bounds",
    )?;
    let super::UniqueJson(root) = serde_json::from_str(root_body)?;
    let super::UniqueJson(page) = serde_json::from_str(page_body)?;
    fields(
        &root,
        "schema generation base_epoch plane_manifest_sha256 page_manifest_sha256 discovery source_layout sq8_object_sha256 sq8_object_key sq8_etag canonical low step",
    )?;
    fields(
        &page,
        "schema generation rows dimensions page_rows object_sha256 page_digest_sha256",
    )?;
    require(
        format!("{:x}", Sha256::digest(root_body.as_bytes()))
            == inputs["generation_root_sha256"]
                .as_str()
                .ok_or("v13 root SHA")?
            && format!("{:x}", Sha256::digest(page_body.as_bytes()))
                == root["page_manifest_sha256"]
                    .as_str()
                    .ok_or("v13 page manifest SHA")?
            && root["schema"]
                == if page_range_config(&inputs["config_schema"]) {
                    "borsuk-two-bit-generation-v11"
                } else {
                    "borsuk-two-bit-generation-v10"
                }
            && root["source_layout"] == inputs["source_layout"]
            && number(&root["generation"])? > 0
            && root["base_epoch"].as_u64().is_some()
            && page["generation"] == root["generation"]
            && page["schema"] == "borsuk-v115-sq8-page-authority-v2"
            && page["rows"] == inputs["rows"]
            && page["dimensions"] == inputs["dimensions"]
            && page["page_rows"] == 32
            && page["object_sha256"] == inputs["native_sq8_sha256"]
            && root["sq8_object_sha256"] == inputs["native_sq8_sha256"]
            && root["plane_manifest_sha256"]
                .as_str()
                .is_some_and(super::valid_sha)
            && page["page_digest_sha256"]
                .as_str()
                .is_some_and(super::valid_sha),
        "v13 exact authenticated root/page manifest authority",
    )?;
    let router = &root["discovery"];
    fields(
        router,
        "mode profile root_sha256 root_bytes membership_sha256 membership_bytes leaves_sha256 leaves_bytes input_schema input_root_sha256 centroids_sha256 source_sha256 source_order_sha256 mean_sha256 records_sha256 sq8_sha256",
    )?;
    let rows = number(&inputs["rows"])?;
    let dimensions = number(&inputs["dimensions"])?;
    let units = rows.div_ceil(32);
    require(
        router["mode"] == "semantic"
            && router["profile"] == inputs["profile"]
            && router["membership_bytes"] == units * 4
            && router["leaves_bytes"] == units * (4 + dimensions * 2)
            && number(&router["root_bytes"])? >= 512
            && router["input_schema"]
                .as_str()
                .is_some_and(|s| !s.is_empty() && s.len() <= 256)
            && router["source_sha256"] == inputs["native_source_sha256"]
            && router["source_order_sha256"] == inputs["native_order_sha256"]
            && router["sq8_sha256"] == inputs["native_sq8_sha256"],
        "v13 authenticated router/source authority",
    )?;
    for key in "root_sha256 membership_sha256 leaves_sha256 input_root_sha256 centroids_sha256 source_sha256 source_order_sha256 mean_sha256 records_sha256 sq8_sha256".split_whitespace() {
        require(router[key].as_str().is_some_and(super::valid_sha), "v13 router SHA256")?;
    }
    for key in ["low", "step"] {
        let values = root[key].as_array().ok_or("v13 calibration")?;
        require(
            values.len() as u64 == dimensions
                && values.iter().all(|v| {
                    v.as_f64()
                        .map(|v| v as f32)
                        .is_some_and(|v| v.is_finite() && (key != "step" || v > 0.))
                }),
            "v13 finite calibration geometry",
        )?;
    }
    if inputs["backend"]["kind"] == "s3" {
        require(
            root["sq8_object_key"] == inputs["backend"]["sq8_object_key"]
                && root["sq8_etag"] == inputs["backend"]["sq8_etag"],
            "v13 SQ8 backend authority",
        )?;
    }
    Ok((root, page))
}

pub(super) fn validate_generation_authority(raw: &Value, inputs: &Value) -> Result<()> {
    generation_manifests(raw, inputs).map(|_| ())
}

pub(super) fn validate_layout_startup(
    raw: &Value,
    inputs: &Value,
    authority: &Value,
) -> Result<()> {
    require(
        raw["source_layout"] == inputs["source_layout"],
        "v13 startup layout binding",
    )?;
    let (root, _) = generation_manifests(authority, inputs)?;
    // ObjectNative stages the root alone, then eight objects per wave, sharing
    // eight 4-MiB range buffers. This runner opens without a retained root seed.
    let mut names = vec![
        "manifest.json",
        "page_manifest.json",
        "page_digests.bin",
        "plane/manifest.json",
        "plane/mean.bin",
        "plane/page_digests.bin",
        "router/root.bin",
        "router/membership.bin",
    ];
    if inputs["source_layout"]["layout"] != "flat_records" {
        names.push("plane/capsule_directory.bin");
    }
    let entries = raw["metadata"]["metadata"]
        .as_array()
        .ok_or("v13 startup metadata")?;
    require(
        entries.len() == names.len(),
        "v13 exact startup roster length",
    )?;
    let rows = number(&inputs["rows"])?;
    let dimensions = number(&inputs["dimensions"])?;
    for (index, (entry, name)) in entries.iter().zip(names).enumerate() {
        let bytes = number(&entry["bytes"])?;
        require(
            entry["name"] == name && bytes > 0,
            "v13 ordered startup roster",
        )?;
        let expected = match name {
            "manifest.json" => Some(
                authority["root_manifest"]
                    .as_str()
                    .ok_or("root bytes")?
                    .len() as u64,
            ),
            "page_manifest.json" => Some(
                authority["page_manifest"]
                    .as_str()
                    .ok_or("page bytes")?
                    .len() as u64,
            ),
            "page_digests.bin" | "plane/page_digests.bin" => {
                Some(rows.div_ceil(32).checked_mul(32).ok_or("digest bytes")?)
            }
            "plane/mean.bin" => Some(dimensions.checked_mul(4).ok_or("mean bytes")?),
            "router/root.bin" => Some(number(&root["discovery"]["root_bytes"])?),
            "router/membership.bin" => Some(number(&root["discovery"]["membership_bytes"])?),
            "plane/capsule_directory.bin" => {
                Some(number(&inputs["source_layout"]["directory_bytes"])?)
            }
            _ => None,
        };
        require(
            expected.is_none_or(|expected| bytes == expected)
                && (!name.ends_with(".json") || bytes <= 65_536),
            "v13 authenticated startup bytes",
        )?;
        let wave = usize::from(index > 0);
        let wave_len = if index == 0 { 1 } else { entries.len() - 1 };
        let range_bytes = 4 * 1024 * 1024;
        let gets = bytes.div_ceil(range_bytes);
        let heads = u64::from(matches!(
            name,
            "manifest.json" | "page_manifest.json" | "plane/manifest.json"
        ));
        require(
            entry["metadata_wave"] == wave
                && entry["metadata_wave_wall_ns"] == entries[wave]["metadata_wave_wall_ns"]
                && entry["logical_head_requests"] == heads
                && (heads != 0 || entry["head_wall_ns"] == 0)
                && entry["logical_get_requests"] == gets
                && entry["payload_buffer_bound_bytes"]
                    == bytes.min(range_bytes * (8 / wave_len as u64))
                && number(&entry["chunks"])? >= gets
                && (bytes <= range_bytes || entry["get_wall_ns"] == 0)
                && entry["reused_root_bytes"] == 0
                && entry["retained_root_bytes"] == 0
                && entry["local_auth_wall_ns"] == 0
                && entry["local_copy_wall_ns"] == 0,
            "v13 deterministic startup GET/HEAD/wave/buffer reservations",
        )?;
    }
    require(
        number(&raw["caller_pinned_bytes"])? >= (2 * SOURCE_BINDING_LINE_CAP) as u64
            && raw["metadata"]["source_head_requests"] == 1
            && raw["metadata"]["router_head_requests"] == 0
            && raw["metadata"]["router_head_wall_ns"] == 0,
        "v13 SOURCE/router HEAD roster",
    )
}

// Frozen evidence-only layouts/formula from source_capsule.rs at feb2ab357b7f.
// These types never parse or serve a production capsule. Keep the producer's
// pointer-width and sha2 scratch assumptions; do not substitute v2 runtime types.
#[allow(dead_code)]
mod frozen_capsule_v1 {
    use super::{Result, plus, require};
    use std::mem::size_of;
    enum Profile {
        Native100k,
        Fresh1m,
        Scale1m,
    }
    struct Geometry {
        rows: usize,
        dimensions: usize,
        width: usize,
        profile: Profile,
    }
    struct Entry {
        leaf_id: u32,
        page_count: u32,
        offset: u64,
        total_bytes: u64,
        payload_bytes: u64,
        sha256: [u8; 32],
    }
    struct Directory {
        geometry: Geometry,
        entries: Vec<Entry>,
        blob_bytes: usize,
    }
    pub(super) struct LeafPages<'a> {
        leaf_id: u32,
        pages: &'a [u32],
    }
    struct PageRef<'a> {
        page: u32,
        leaf: u32,
        bytes: &'a [u8],
    }
    struct Charge {
        source_bytes: usize,
        physical_bytes: usize,
        logical_bytes: usize,
        duplicate_bytes: usize,
        framing_bytes: usize,
        hash_bytes: usize,
        encoded_directory_bytes: usize,
        decoded_directory_bytes: usize,
        roster_bytes: usize,
        buffer_bytes: usize,
        lookup_bytes: usize,
        scratch_bytes: usize,
        memory_bytes: usize,
    }
    pub(super) struct VerifiedRows<'a> {
        geometry: Geometry,
        pages: Vec<PageRef<'a>>,
        charge: Charge,
    }
    pub(super) const LEAF_PAGES_BYTES: u64 = 24;
    pub(super) const VERIFIED_ROWS_BYTES: u64 = 32 + 24 + 104;

    // Public sizes/scratch measured by retained-old-rlib witness:
    // evidence/capsule-legacy-directory-native-size-a0001/result.txt.
    #[test]
    fn page_range_v2_legacy_model_matches_native_witness() {
        assert_eq!(size_of::<usize>(), 8, "frozen 64-bit producer");
        assert_eq!(size_of::<Directory>(), 64);
        assert_eq!(size_of::<Entry>(), 64);
        assert_eq!(size_of::<Geometry>(), 32);
        assert_eq!(size_of::<Charge>(), 104);
        assert_eq!(3 * size_of::<sha2::Sha256>() + 128, 464);
        // Private sizes follow the exact-base field shapes, not v2 types.
        assert_eq!(size_of::<LeafPages<'_>>() as u64, LEAF_PAGES_BYTES);
        assert_eq!(size_of::<VerifiedRows<'_>>() as u64, VERIFIED_ROWS_BYTES);
        for (rows, leaves, memory) in [(513, 1, 688), (513, 8, 1584), (1_000_000, 731, 94128)] {
            let (decoded, scratch) = open_charge(rows, leaves).unwrap();
            assert_eq!(decoded, 64 + 64 * leaves);
            assert_eq!(scratch, 464);
            assert_eq!(32 + 64 * leaves + decoded + scratch, memory);
        }
    }

    pub(super) fn open_charge(rows: usize, leaves: usize) -> Result<(usize, usize)> {
        require(
            leaves > 0 && leaves <= rows.div_ceil(32),
            "capsule leaf count",
        )?;
        let decoded = plus(
            64,
            (leaves as u64)
                .checked_mul(64)
                .ok_or("capsule arithmetic")?,
        )?;
        let scratch = 464;
        // The old open model also checked encoded-directory + decoded + scratch.
        let encoded = plus(
            32,
            (leaves as u64)
                .checked_mul(64)
                .ok_or("capsule arithmetic")?,
        )?;
        usize::try_from(plus(plus(encoded, decoded)?, scratch as u64)?)?;
        Ok((usize::try_from(decoded)?, scratch))
    }
}

fn validate_page_range_capsules(raw: &Value, inputs: &Value, stats: &Value) -> Result<()> {
    use borsuk::source_capsule::Geometry;
    let physical = number(&stats["physical_bytes"])?;
    let logical = number(&stats["logical_bytes"])?;
    let duplicate = number(&stats["duplicate_bytes"])?;
    let framing = number(&stats["framing_bytes"])?;
    let payload = plus(logical, duplicate)?;
    require(
        physical == plus(payload, framing)?
            && number(&stats["hash_bytes"])? == payload
            && raw["charges"]["source"]["verified_bytes"] == physical
            && raw["charges"]["source"]["failed_gets"] == 0
            && physical <= number(&inputs["source_byte_budget"])?
            && physical <= number(&inputs["source_layout"]["blob_bytes"])?,
        "v15 physical/logical/hash SOURCE conservation",
    )?;
    let rows = number(&inputs["rows"])?;
    let directory = number(&inputs["source_layout"]["directory_bytes"])?;
    let units = rows.div_ceil(32);
    require(
        directory <= plus(32, units.checked_mul(68).ok_or("v15 directory overflow")?)?,
        "v15 directory roster bound",
    )?;
    // Without the binary directory, only a leaf-count upper bound is observable.
    let leaf_bound = ((directory - 32) / 68).min(units);
    let leaves = raw["trace"]["semantic_leaves"]
        .as_array()
        .ok_or("v15 selected leaves")?;
    let mut selected = std::collections::BTreeSet::new();
    for leaf in leaves {
        let id = number(leaf)?;
        require(
            id < leaf_bound && selected.insert(id),
            "v15 unique selected leaf IDs",
        )?;
    }
    let n = selected.len() as u64;
    let mut previous = None;
    let mut groups = 0;
    for &id in &selected {
        if previous.is_none() || previous != id.checked_sub(1) {
            groups += 1;
        }
        previous = Some(id);
    }
    let gets = number(&raw["charges"]["source"]["submitted_gets"])?;
    require(
        n > 0 && gets > 0 && gets <= groups && gets <= number(&inputs["source_get_budget"])?,
        "v15 trimmed GETs bounded by original adjacent groups",
    )?;
    let geometry = Geometry::new(
        usize::try_from(rows)?,
        usize::try_from(number(&inputs["dimensions"])?)?,
        serde_json::from_value(inputs["profile"].clone())?,
    )?;
    let width = geometry.record_bytes() as u64;
    let membership = raw["trace"]["semantic_units"]
        .as_array()
        .ok_or("v15 semantic units")?;
    let mut pages = std::collections::BTreeSet::new();
    let mut previous = None;
    for unit in membership {
        let unit = number(unit)?;
        require(
            unit < units && previous.is_none_or(|p| p < unit),
            "v15 sorted complete semantic membership",
        )?;
        pages.insert(unit / 8);
        previous = Some(unit);
    }
    let mut useful = 0;
    for page in &pages {
        useful = plus(
            useful,
            (rows - page * 256)
                .min(256)
                .checked_mul(width)
                .ok_or("v15 useful bytes overflow")?,
        )?;
    }
    let source = rows.checked_mul(width).ok_or("v15 source bytes overflow")?;
    let page_bytes = 256 * width;
    let tail = (rows % 256) * width;
    let has_tail = tail > 0 && logical % page_bytes == tail;
    let full_unique = logical
        .checked_sub(if has_tail { tail } else { 0 })
        .ok_or("v15 unique tail bytes")?
        / page_bytes;
    let duplicate_pages = (0..if has_tail { n } else { 1 }).any(|tails| {
        let tail_bytes = tails * tail;
        duplicate.checked_sub(tail_bytes).is_some_and(|full| {
            full.is_multiple_of(page_bytes)
                && full_unique
                    .checked_mul(n - 1)
                    .is_some_and(|bound| full / page_bytes <= bound)
        })
    });
    require(
        !pages.is_empty()
            && logical >= useful
            && logical <= source
            && (tail == 0 || !pages.contains(&(rows / 256)) || has_tail)
            && (logical.is_multiple_of(page_bytes)
                || (tail > 0 && logical >= tail && (logical - tail).is_multiple_of(page_bytes)))
            && duplicate_pages
            && payload <= logical.checked_mul(n).ok_or("v15 payload overflow")?
            && (framing == 0 || (framing >= 36 && framing.is_multiple_of(4))),
        "v15 whole-page payload and included framing bounds",
    )?;
    // Unique fetched pages can include unscored bridges. Six counters cannot
    // prove owners, endpoints, exact rosters or allocation capacities.
    require(
        number(&stats["memory_bytes"])? >= physical
            && number(&stats["memory_bytes"])? <= number(&inputs["max_memory_bytes"])?,
        "v15 retained response memory bounds",
    )
}

// Only completed queries reach this validator. Failed reads retain pre-I/O estimates;
// those are not interchangeable with authenticated response bytes.
pub(super) fn validate_source_capsules(raw: &Value, inputs: &Value) -> Result<()> {
    use borsuk::source_capsule::Geometry;
    validate_layout_header(inputs)?;
    require(
        number(&raw["charges"]["source"]["submitted_gets"])? > 0
            && number(&raw["charges"]["source"]["submitted_gets"])?
                <= number(&inputs["source_get_budget"])?,
        "v13 successful SOURCE requires a payload GET within budget",
    )?;
    let stats = raw["stages"]
        .get("source_capsules")
        .ok_or("v13 capsule stats field required")?;
    if inputs["source_layout"]["layout"] == "flat_records" {
        return require(stats.is_null(), "v13 flat capsule stats must be null");
    }
    fields(
        stats,
        "physical_bytes logical_bytes duplicate_bytes framing_bytes hash_bytes memory_bytes",
    )?;
    for v in stats
        .as_object()
        .ok_or("v13 capsule stats object")?
        .values()
    {
        number(v)?;
    }
    if page_range_config(&inputs["config_schema"]) {
        return validate_page_range_capsules(raw, inputs, stats);
    }
    let physical = number(&stats["physical_bytes"])?;
    let logical = number(&stats["logical_bytes"])?;
    let duplicate = number(&stats["duplicate_bytes"])?;
    let framing = number(&stats["framing_bytes"])?;
    let payload = plus(logical, duplicate)?;
    require(
        physical == plus(payload, framing)?
            && raw["charges"]["source"]["verified_bytes"] == physical
            && raw["charges"]["source"]["failed_gets"] == 0
            && physical <= number(&inputs["source_byte_budget"])?
            && physical <= number(&inputs["source_layout"]["blob_bytes"])?
            && number(&stats["hash_bytes"])?
                == if inputs["config_schema"] == ONEPASS_CONFIG_SCHEMA {
                    physical
                        .checked_mul(2)
                        .and_then(|bytes| bytes.checked_sub(framing))
                        .ok_or("onepass capsule hash overflow")?
                } else {
                    plus(physical, payload)?
                        .checked_mul(2)
                        .ok_or("capsule hash overflow")?
                },
        "v13 physical/logical/hash SOURCE conservation",
    )?;
    let leaves = raw["trace"]["semantic_leaves"]
        .as_array()
        .ok_or("v13 selected SOURCE leaves")?;
    let n = u64::try_from(leaves.len())?;
    let headers = n.checked_mul(32).ok_or("capsule framing overflow")?;
    require(
        n > 0 && framing >= headers && (framing - headers) % 4 == 0,
        "v13 capsule framing/roster",
    )?;
    let occurrences = (framing - headers) / 4;
    let geometry = Geometry::new(
        usize::try_from(number(&inputs["rows"])?)?,
        usize::try_from(number(&inputs["dimensions"])?)?,
        serde_json::from_value(inputs["profile"].clone())?,
    )?;
    let directory = usize::try_from(number(&inputs["source_layout"]["directory_bytes"])?)?;
    let (decoded_directory_bytes, scratch_bytes) =
        frozen_capsule_v1::open_charge(geometry.rows(), (directory - 32) / 64)?;
    let leaf_count = ((directory - 32) / 64) as u64;
    let mut selected = std::collections::BTreeSet::new();
    for leaf in leaves {
        let id = number(leaf)?;
        require(
            id < leaf_count && selected.insert(id),
            "v13 unique selected leaf IDs",
        )?;
    }
    // Directory::open authenticates contiguous entries in leaf-ID order;
    // CapsuleSource::request merges exactly the adjacent selected entries.
    let mut previous = None;
    let mut gets = 0;
    for &id in &selected {
        if previous != id.checked_sub(1) || previous.is_none() {
            gets += 1;
        }
        previous = Some(id);
    }
    require(
        raw["charges"]["source"]["submitted_gets"] == gets
            && gets <= number(&inputs["source_get_budget"])?,
        "v13 exact merged capsule GET count",
    )?;
    let units = raw["trace"]["semantic_units"]
        .as_array()
        .ok_or("v13 semantic units")?;
    let mut pages = std::collections::BTreeMap::<u64, u64>::new();
    let mut previous = None;
    for unit in units {
        let unit = number(unit)?;
        require(
            unit < (geometry.rows() as u64).div_ceil(32) && previous.is_none_or(|last| last < unit),
            "v13 sorted complete semantic membership",
        )?;
        *pages.entry(unit / 8).or_default() += 1;
        previous = Some(unit);
    }
    // nominate_selected includes every unit of every selected leaf. Seed additions
    // stay inside its lowest page, so they cannot change this original-page union.
    let width = geometry.record_bytes() as u64;
    let page_bytes = width.checked_mul(256).ok_or("capsule page overflow")?;
    let mut useful = 0;
    let mut max_occurrences = 0;
    for (&page, &units) in &pages {
        useful = plus(
            useful,
            (geometry.rows() as u64 - page * 256)
                .min(256)
                .checked_mul(width)
                .ok_or("capsule useful bytes overflow")?,
        )?;
        max_occurrences = plus(max_occurrences, units.min(n))?;
    }
    let unique = pages.len() as u64;
    require(
        n > 0
            && n <= leaf_count
            && unique > 0
            && logical == useful
            && occurrences >= unique.max(n)
            && occurrences <= max_occurrences,
        "v13 exact useful page union and capsule occurrence bounds",
    )?;
    let full_payload = occurrences
        .checked_mul(page_bytes)
        .ok_or("capsule payload overflow")?;
    let tail_rows = geometry.rows() as u64 % 256;
    if tail_rows == 0 {
        require(
            payload == full_payload,
            "v13 full-page payload conservation",
        )?;
    } else {
        let tail_page = geometry.rows() as u64 / 256;
        let tail_units = pages.get(&tail_page).copied().unwrap_or(0);
        let missing = (256 - tail_rows) * width;
        let deficit = full_payload
            .checked_sub(payload)
            .ok_or("v13 capsule payload excess")?;
        require(
            deficit.is_multiple_of(missing),
            "v13 whole tail-page occurrences",
        )?;
        let tails = deficit / missing;
        let tail_min = u64::from(tail_units > 0);
        let tail_max = tail_units.min(n);
        let full_occurrences = occurrences
            .checked_sub(tails)
            .ok_or("v13 tail occurrences")?;
        require(
            (tail_min..=tail_max).contains(&tails)
                && full_occurrences >= unique - tail_min
                && full_occurrences <= max_occurrences - tail_max,
            "v13 tail/full-page duplicate conservation",
        )?;
    }
    // Aggregate membership proves the union and occurrence bounds, not each leaf's
    // page roster or physical offsets; those still require pinned runtime/source evidence.
    // The codec charges actual Vec capacities, which the six counters cannot reconstruct.
    // Its public model proves this lower bound; the explicit admission cap proves the upper.
    let roster = plus(
        n.checked_mul(frozen_capsule_v1::LEAF_PAGES_BYTES)
            .ok_or("capsule roster overflow")?,
        occurrences
            .checked_mul(4)
            .ok_or("capsule roster overflow")?,
    )?;
    let minimum = [
        physical,
        decoded_directory_bytes as u64,
        scratch_bytes as u64,
        roster,
        frozen_capsule_v1::VERIFIED_ROWS_BYTES,
    ]
    .into_iter()
    .try_fold(0, plus)?;
    require(
        number(&stats["memory_bytes"])? >= minimum
            && number(&stats["memory_bytes"])? <= number(&inputs["max_memory_bytes"])?,
        "v13 codec memory bounds",
    )
}

pub(super) fn paired_source_layout(config: &std::path::Path, sha: &str) -> Result<Value> {
    let (c, _): (super::PairedConfig, _) = super::read_config(config, sha)?;
    require(
        c.schema == "borsuk-paired-source-layout-config-v1",
        "paired SOURCE layout config schema",
    )?;
    let [a, b] = &c.arms;
    for arm in &c.arms {
        require(
            is_source_layout_schema(&arm.expected_identity["schema"]),
            "paired SOURCE requires source-layout result schema",
        )?;
        validate_completed(arm)?;
    }
    require(
        a.expected_identity["schema"] == b.expected_identity["schema"]
            && a.expected_bound_inputs["source_layout"] == json!({"layout":"flat_records"})
            && b.expected_bound_inputs["source_layout"]["layout"]
                == if page_range_result(&b.expected_identity["schema"]) {
                    "leaf_capsules_v2"
                } else {
                    "leaf_capsules"
                },
        "paired SOURCE ordered flat/capsule arms",
    )?;
    // An allowlist of derived differences; every other field remains exact.
    let mut identity = a.expected_identity.clone();
    for key in ["config_sha256", "source_layout"] {
        identity[key] = b.expected_identity[key].clone();
    }
    let mut bound = a.expected_bound_inputs.clone();
    for key in [
        "generation_prefix",
        "generation_root_sha256",
        "source_layout",
    ] {
        bound[key] = b.expected_bound_inputs[key].clone();
    }
    require(
        identity == b.expected_identity && bound == b.expected_bound_inputs,
        "paired SOURCE dataset/request/truth/order/source/SQ8/router/operating-point authority drift",
    )?;
    let mut returned_ordinals = [Vec::new(), Vec::new()];
    let mut runs = Vec::with_capacity(2);
    for (arm, ordinals) in c.arms.iter().zip(&mut returned_ordinals) {
        runs.push(super::read_run_observed(
            &arm.input.path,
            &arm.input.sha256,
            Some(arm),
            |line, _, _| {
                let raw: Value = serde_json::from_slice(line)?;
                ordinals.push(raw["returned_physical_ordinals"].clone());
                Ok(())
            },
        )?);
    }
    let [a, b] = &runs[..] else {
        return Err("paired SOURCE run count".into());
    };

    let (mut ra, mut pa) = generation_manifests(
        &a.completed
            .as_ref()
            .ok_or("flat completed")?
            .generation_authority,
        &c.arms[0].expected_bound_inputs,
    )?;
    let (mut rb, pb) = generation_manifests(
        &b.completed
            .as_ref()
            .ok_or("capsule completed")?
            .generation_authority,
        &c.arms[1].expected_bound_inputs,
    )?;
    require(
        number(&rb["generation"])? > number(&ra["generation"])?,
        "paired SOURCE repack requires newer generation",
    )?;
    // Compare in the library's f32 domain only after authenticating the raw manifests.
    for root in [&mut ra, &mut rb] {
        for key in ["low", "step"] {
            for value in root[key].as_array_mut().ok_or("v13 calibration")? {
                *value = json!((value.as_f64().ok_or("v13 calibration")? as f32).to_bits());
            }
        }
    }
    // Repack changes only these root fields and the page manifest's generation.
    // Its new manifest digest has already been recomputed against that arm's root.
    for key in ["generation", "source_layout", "page_manifest_sha256"] {
        ra[key] = rb[key].clone();
    }
    pa["generation"] = pb["generation"].clone();
    require(
        ra == rb && pa == pb,
        "paired SOURCE root/page derivation, SOURCE authority, SQ8 digests, router or calibration drift",
    )?;
    let sa = &a.completed.as_ref().ok_or("flat startup")?.startup;
    let sb = &b.completed.as_ref().ok_or("capsule startup")?.startup;
    for (key, value) in sa.as_object().ok_or("paired startup object")? {
        if !["metadata", "source_layout", "caller_pinned_bytes"].contains(&key.as_str()) {
            require(
                sb.get(key) == Some(value),
                "paired SOURCE invariant startup drift",
            )?;
        }
    }
    for entry in sa["metadata"]["metadata"]
        .as_array()
        .ok_or("flat startup roster")?
    {
        let name = entry["name"].as_str().ok_or("paired metadata name")?;
        let other = sb["metadata"]["metadata"]
            .as_array()
            .ok_or("capsule startup roster")?
            .iter()
            .find(|e| e["name"] == name)
            .ok_or("paired metadata missing")?;
        for (key, value) in entry.as_object().ok_or("paired metadata entry")? {
            // Timings and transport chunking are observations; all remaining counters
            // are deterministic, except the two rewritten manifests' sizes/buffers.
            let observed = key.ends_with("_wall_ns") || key == "chunks";
            let resized = matches!(name, "manifest.json" | "page_manifest.json")
                && matches!(key.as_str(), "bytes" | "payload_buffer_bound_bytes");
            if !observed && !resized {
                require(
                    other.get(key) == Some(value),
                    "paired SOURCE invariant metadata drift",
                )?;
            }
        }
    }
    require(
        a.samples.len() == b.samples.len() && returned_ordinals[0] == returned_ordinals[1],
        "paired SOURCE query/returned physical order drift",
    )?;
    for (a, b) in a.samples.iter().zip(&b.samples) {
        require(
            a.trace_sha256 == b.trace_sha256
                && a.physical_plan_sha256 == b.physical_plan_sha256
                && a.returned == b.returned
                && a.hits10 == b.hits10
                && a.charges.router == b.charges.router
                && a.charges.sq8 == b.charges.sq8,
            "paired SOURCE ordered nomination/shortlist/SQ8 plan/IDs/scorebits/per-query hits drift",
        )?;
    }
    Ok(
        json!({"schema":"borsuk-paired-source-layout-v1","status":"PARITY_VERIFIED",
        "complete":true,"config_sha256":sha,"queries":a.samples.len(),"ordered_nomination_shortlist_sq8_plan_parity":true,
        "returned_ids_scorebits_physical_ordinals_parity":true,"per_query_hits_parity":true,
        "inputs":c.arms.iter().map(|a| &a.input).collect::<Vec<_>>(),
        "source_bridges":"authenticated_unscored","sq8_bridges":"scored",
        "source_physical_ranges":if page_range_result(&c.arms[1].expected_identity["schema"]) {
            "GETs_bounded_by_old_adjacent_groups; exact_trim_and_owners_require_pinned_native_directory"
        } else { "count_from_adjacent_authenticated_leaf_ids; offsets_and_per_leaf_pages_not_in_trace" },
        "source_payload_validation":if page_range_result(&c.arms[1].expected_identity["schema"]) {
            "fetched_payload_conservation_and_required_union_lower_bound; exact_roster_requires_native_proof"
        } else { "exact_useful_page_union; tail_aware_occurrence_bounds_not_full_physical_roster" },"plan_ranges":"SQ8_only",
        "source_archive_and_binary_authority":"root_owned_external_receipts",
        "wire_bytes":null,"billed_bytes":null,"billed_requests":null,
        "qualified":false,"performance_claim":false,"quality_or_scientific_win_claim":false}),
    )
}

pub(super) const HANDOFF_REPORT_SCHEMA: &str = "borsuk-capsule-handoff-abba-v1";

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CapsuleHandoffAbbaConfig {
    schema: String,
    declared_run_order: [String; 4],
    // Independently frozen headers include config, executable and every component SHA.
    runs: [CompletedConfig; 4],
}

fn handoff_ordinals() -> Vec<usize> {
    (0..32).map(|i| i * 999 / 31).collect()
}

fn handoff_headers(c: &CapsuleHandoffAbbaConfig) -> Result<()> {
    require(
        c.schema == "borsuk-capsule-handoff-abba-config-v1"
            && c.declared_run_order
                .iter()
                .map(String::as_str)
                .eq(super::LABELS),
        "capsule handoff explicit A1/B1/B2/A2 order",
    )?;
    for (index, arm) in c.runs.iter().enumerate() {
        validate_completed(arm)?;
        let candidate = matches!(index, 1 | 2);
        let identity = &arm.expected_identity;
        let bound = &arm.expected_bound_inputs;
        require(
            identity["schema"]
                == if candidate {
                    ONEPASS_RESULT_SCHEMA
                } else {
                    SOURCE_LAYOUT_RESULT_SCHEMA
                }
                && bound["config_schema"]
                    == if candidate {
                        ONEPASS_CONFIG_SCHEMA
                    } else {
                        SOURCE_LAYOUT_CONFIG_SCHEMA
                    }
                && (if candidate {
                    bound["source_capsule_verification_policy"]
                        == SOURCE_CAPSULE_VERIFICATION_POLICY
                } else {
                    bound.get("source_capsule_verification_policy").is_none()
                })
                && bound["source_layout"]["layout"] == "leaf_capsules"
                && bound["execution"]
                    == json!({"mode":"diagnostic_panel","ordinals":handoff_ordinals(),"trace":false})
                && bound["dimensions"] == 1024
                && bound["count"] == 1000
                && bound["selected_count"] == 32
                && bound["k"] == 10
                && bound["source_cache"] == "off"
                && bound["serving"] == json!({"mode":"baseline"}),
            "capsule handoff schema/policy/fixed original panel/operating point",
        )?;
        let reference = &c.runs[usize::from(candidate)];
        require(
            identity == &reference.expected_identity && bound == &reference.expected_bound_inputs,
            "capsule handoff within-arm authority drift",
        )?;
    }
    let [a, b, _, _] = &c.runs;
    let mut identity = a.expected_identity.clone();
    // These four source files implement the handoff and its telemetry; no other
    // source, dataset, backend, generation or operating-point change is admitted.
    for key in [
        "schema",
        "config_schema",
        "config_sha256",
        "binary_sha256",
        "runner_source_sha256",
        "generation_source_sha256",
        "source_capsule_source_sha256",
        "sq8_range_source_sha256",
        "source_capsule_verification_policy",
    ] {
        identity[key] = b.expected_identity[key].clone();
    }
    let mut bound = a.expected_bound_inputs.clone();
    for key in ["config_schema", "source_capsule_verification_policy"] {
        bound[key] = b.expected_bound_inputs[key].clone();
    }
    require(
        identity == b.expected_identity
            && bound == b.expected_bound_inputs
            && a.expected_identity["config_sha256"] != b.expected_identity["config_sha256"]
            && a.expected_identity["binary_sha256"] != b.expected_identity["binary_sha256"],
        "capsule handoff preregistered authority differences only",
    )
}

fn handoff_descriptor_increment(gets: u64) -> Result<u64> {
    // source_capsule::VerifiedSpan adds &Directory, &PageAuthority, &[LeafPages]
    // and u64 to the retained Vec. verified_rows charges this once per span.
    require(
        std::mem::size_of::<usize>() == 8,
        "capsule handoff 64-bit producer model",
    )?;
    let bytes = 4 * std::mem::size_of::<usize>() + std::mem::size_of::<u64>();
    gets.checked_mul(bytes as u64)
        .ok_or_else(|| "capsule descriptor overflow".into())
}

fn handoff_remove(raw: &mut Value, keys: &[&str]) -> Result<()> {
    let object = raw.as_object_mut().ok_or("capsule handoff object")?;
    for key in keys {
        object.remove(*key);
    }
    Ok(())
}

// Each row has already passed its own strict schema, seals and arithmetic.
// Normalize only the enumerated observations and independently derived changes.
fn handoff_invariant(raw: &mut Value, candidate: bool) -> Result<()> {
    match raw["phase"].as_str().ok_or("capsule handoff phase")? {
        "identity" => handoff_remove(
            raw,
            &[
                "schema",
                "config_schema",
                "config_sha256",
                "binary_sha256",
                "runner_source_sha256",
                "generation_source_sha256",
                "source_capsule_source_sha256",
                "sq8_range_source_sha256",
                "source_capsule_verification_policy",
            ],
        )?,
        "bound_inputs" => handoff_remove(
            raw,
            &["config_schema", "source_capsule_verification_policy"],
        )?,
        "startup" => {
            handoff_remove(
                &mut raw["metadata"],
                &[
                    "decode_wall_ns",
                    "router_head_wall_ns",
                    "source_head_wall_ns",
                    "staging_wall_ns",
                ],
            )?;
            for entry in raw["metadata"]["metadata"]
                .as_array_mut()
                .ok_or("startup metadata roster")?
            {
                // Body frame segmentation varies independently of authenticated bytes/GETs.
                handoff_remove(
                    entry,
                    &[
                        "chunks",
                        "get_wall_ns",
                        "head_wall_ns",
                        "local_auth_wall_ns",
                        "local_copy_wall_ns",
                        "metadata_wave_wall_ns",
                        "stream_wall_ns",
                        "write_wall_ns",
                    ],
                )?;
            }
        }
        "query" => {
            require(
                raw["returned_count"] == 10 && raw["underfill"] == false,
                "capsule handoff ten returned ranks per query",
            )?;
            let gets = number(&raw["charges"]["source"]["submitted_gets"])?;
            let stats = &mut raw["stages"]["source_capsules"];
            let memory = number(&stats["memory_bytes"])?;
            stats["memory_bytes"] = json!(if candidate {
                memory
                    .checked_sub(handoff_descriptor_increment(gets)?)
                    .ok_or("capsule descriptor memory underflow")?
            } else {
                memory
            });
            // The strict reader independently proves old 4P-2F or new 2P-F.
            handoff_remove(stats, &["hash_bytes"])?;
            handoff_remove(raw, &["query_wall_ns", "query_process_cpu_ns"])?;
            for stage in ["discovery", "source", "planning", "sq8"] {
                handoff_remove(&mut raw["stages"][stage], &["start_ns", "end_ns"])?;
            }
        }
        "all_queries_sealed" => handoff_remove(raw, &["prefix_bytes", "prefix_sha256"])?,
        "terminal" => handoff_remove(
            &mut raw["summary"],
            &[
                "prefix_bytes",
                "prefix_sha256",
                "sealed_bytes",
                "sealed_sha256",
                "query_wall_ns",
                "query_process_cpu_ns",
                "process_wall_ns",
                "process_cpu_ns",
                "observed_process_peak_bytes",
            ],
        )?,
        _ => (),
    }
    Ok(())
}

fn handoff_fingerprints(
    pin: &CompletedConfig,
    run: &super::Run,
    candidate: bool,
) -> Result<Vec<[u8; 32]>> {
    use sha2::{Digest, Sha256};
    // Reopen only the same bounded result, anchored to the original strict read.
    let mut rows = super::Rows::open(&pin.input.path)?;
    require(
        rows.original == run.file_identity,
        "capsule handoff result changed",
    )?;
    rows.v10 = true;
    rows.library_scheduler = true;
    rows.source_layout = true;
    rows.line_cap = 65_536;
    let mut fingerprints = Vec::new();
    let phases = [
        "identity",
        "bound_inputs",
        "diagnostic_admission",
        "source_binding",
        "generation_open",
        "startup",
    ]
    .into_iter()
    .chain(std::iter::repeat_n("query", run.samples.len()))
    .chain(["all_queries_sealed"])
    .chain(std::iter::repeat_n("recall", run.samples.len()))
    .chain(["terminal"]);
    for phase in phases {
        let mut raw: Value = rows.row(phase)?;
        handoff_invariant(&mut raw, candidate)?;
        fingerprints.push(Sha256::digest(serde_json::to_vec(&raw)?).into());
    }
    rows.finish(&pin.input.path, &pin.input.sha256)?;
    Ok(fingerprints)
}

fn handoff_timing(a: &super::Run, b: &super::Run, label: &str) -> Result<Value> {
    let ae = a.completed.as_ref().ok_or("handoff completed A")?;
    let be = b.completed.as_ref().ok_or("handoff completed B")?;
    let aw: Vec<_> = a.samples.iter().map(|q| q.wall_ns).collect();
    let bw: Vec<_> = b.samples.iter().map(|q| q.wall_ns).collect();
    let mut distributions = serde_json::Map::new();
    for (name, av, bv) in std::iter::once(("query", &aw, &bw)).chain(
        ["discovery", "source", "planning", "sq8"]
            .into_iter()
            .zip(ae.stage_samples.iter().zip(&be.stage_samples))
            .map(|(name, (av, bv))| (name, av, bv)),
    ) {
        let (at, aq) = super::quantiles(av)?;
        let (bt, bq) = super::quantiles(bv)?;
        let deltas = av
            .iter()
            .zip(bv)
            .zip(handoff_ordinals())
            .map(|((&a, &b), ordinal)| -> Result<Value> {
                let delta = i64::try_from(i128::from(b) - i128::from(a))?;
                Ok(json!({"ordinal":ordinal,"b_minus_a_ns":delta}))
            })
            .collect::<Result<Vec<_>>>()?;
        distributions.insert(name.into(), json!({
            "a_total_ns":at,"b_total_ns":bt,"quantile_order":["p50","p90","p95","p99"],
            "a_sample_quantiles_ns":aq,"b_sample_quantiles_ns":bq,
            "b_no_slower_total":bt<=at,"b_no_slower_quantiles":aq.iter().zip(bq).all(|(&a,b)| b<=a),
            "paired_deltas":deltas,
        }));
    }
    let (at, aq) = super::quantiles(&aw)?;
    let (bt, bq) = super::quantiles(&bw)?;
    Ok(
        json!({"comparison":label,"direction":"candidate_B_minus_control_A",
        "sample_statistics":distributions,
        "serial_qps_ratio_b_over_a":at as f64/bt as f64,
        "quality_equal":a.terminal.total_hits10==b.terminal.total_hits10,
        "quality_timing_pareto":a.terminal.total_hits10==b.terminal.total_hits10
            && bt<=at && aq.iter().zip(bq).all(|(&a,b)| b<=a)
            && (bt<at || aq.iter().zip(bq).any(|(&a,b)| b<a)),
        "pareto_dimensions":"equal per-query quality; total wall and sample p50/p90/p95/p99; at least one strict improvement",
        "performance_claim":false}),
    )
}

pub(super) fn capsule_handoff_abba(config: &std::path::Path, sha: &str) -> Result<Value> {
    let file = super::open_input(config)?;
    let original = super::file_identity(&file)?;
    // Four independently frozen complete header pairs can exceed the historical 32-KiB cap.
    let (c, bytes): (CapsuleHandoffAbbaConfig, _) =
        super::read_config_file(&file, config, sha, 256 * 1024)?;
    handoff_headers(&c)?;
    let mut paths = std::collections::BTreeSet::new();
    let mut hashes = std::collections::BTreeSet::new();
    let mut inodes = std::collections::BTreeSet::new();
    let mut admitted = Vec::with_capacity(4);
    for arm in &c.runs {
        let identity = super::file_identity(&super::open_input(&arm.input.path)?)?;
        require(
            paths.insert(&arm.input.path)
                && hashes.insert(&arm.input.sha256)
                && inodes.insert((identity.dev, identity.ino))
                && (identity.dev, identity.ino) != (original.dev, original.ino),
            "capsule handoff distinct result paths/inodes/full hashes",
        )?;
        admitted.push(identity);
    }
    let mut runs = Vec::with_capacity(4);
    let mut reference: Option<Vec<[u8; 32]>> = None;
    for (index, arm) in c.runs.iter().enumerate() {
        let run = super::read_run_observed_with_identity(
            &arm.input.path,
            &arm.input.sha256,
            Some(arm),
            Some((admitted[index].dev, admitted[index].ino)),
            |_, _, _| Ok(()),
        )?;
        require(
            run.file_identity == admitted[index] && run.samples.len() == 32,
            "capsule handoff stable fixed32 inputs",
        )?;
        let invariants = handoff_fingerprints(arm, &run, matches!(index, 1 | 2))?;
        if let Some(ref expected) = reference {
            require(
                invariants.len() == expected.len(),
                "capsule handoff row count",
            )?;
            if let Some(row) = invariants.iter().zip(expected).position(|(a, b)| a != b) {
                return Err(format!(
                    "capsule handoff {} unapproved difference at row {row}",
                    super::LABELS[index]
                )
                .into());
            }
        } else {
            reference = Some(invariants);
        }
        runs.push(run);
    }
    for (arm, run) in c.runs.iter().zip(&runs) {
        super::recheck_parity_path(&arm.input, &run.file_identity)?;
    }
    super::recheck_header_file(&file, config, &original)?;
    let reports = c.runs.iter().zip(&runs).enumerate().map(|(index, (pin, run))| {
        let e = run.completed.as_ref().ok_or("handoff completed run")?;
        Ok(json!({"run":super::LABELS[index],"input":pin.input,
            "expected_identity":pin.expected_identity,"expected_bound_inputs":pin.expected_bound_inputs,
            "sample_statistics":super::statistics(&run.samples.iter().map(|q|q.wall_ns).collect::<Vec<_>>())?,
            "stage_wall_sums":run.stages,"native_terminal":e.terminal,"startup":e.startup}))
    }).collect::<Result<Vec<_>>>()?;
    Ok(
        json!({"schema":HANDOFF_REPORT_SCHEMA,"status":"MEASURED","complete":true,
        "config_sha256":sha,"config_bytes":bytes,"declared_run_order":super::LABELS,
        "original_query_ordinals":handoff_ordinals(),"population_count":1000,"selected_count":32,
        "returned_rank_parity_count":320,"returned_ids_scorebits_target_rows_parity":true,
        "same_generation_root_and_manifest_parity":true,"plan_and_seven_trace_fields_parity":true,
        "source_sq8_get_byte_duplicate_framing_parity":true,
        "hash_charge_control":"4*physical_bytes-2*framing_bytes",
        "hash_charge_candidate":"2*physical_bytes-framing_bytes",
        "memory_charge_candidate_increment_per_source_get":handoff_descriptor_increment(1)?,
        "runs":reports,"comparisons":[handoff_timing(&runs[0],&runs[1],"A1/B1")?,
            handoff_timing(&runs[3],&runs[2],"A2/B2")?],
        "quality_hits10":runs[0].terminal.total_hits10,"quality_denominator":320,
        "quality_scope":"returned parity is not perfect truth recall; ordered native recall rows must agree",
        "diagnostic_only":true,"population_percentiles_valid":false,"percentile_method":"nearest_rank",
        "execution_order_verified":false,"producer_provenance_verified":false,
        "physical_qualification_verified":false,"qualified":false,
        "external_authority":"root original producer/config/source/ELF, chronological ABBA, cache, resource and physical transport receipts",
        "duplicate_full_hash_policy":"reject byte-identical evidence; distinct hashes and inodes do not prove separate executions",
        "reduction_reads_local_files_only":true,"performance_claim":false,"vendor_or_scientific_win_claim":false}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handoff_fixture_pin(
        dir: &std::path::Path,
        name: &str,
        mut rows: Vec<Value>,
    ) -> CompletedConfig {
        super::super::tests::authenticate(&mut rows);
        let body = super::super::tests::encode(&rows);
        let path = dir.join(name);
        std::fs::write(&path, &body).unwrap();
        CompletedConfig {
            schema: super::super::SCALE_CONFIG_SCHEMA.into(),
            input: super::super::CompletedInput {
                path,
                bytes: body.len() as u64,
                sha256: format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(&body)),
            },
            expected_identity: rows[0].clone(),
            expected_bound_inputs: rows[1].clone(),
        }
    }

    fn handoff_read(pin: &CompletedConfig, candidate: bool) -> Result<Vec<[u8; 32]>> {
        let run = super::super::read_run_with(&pin.input.path, &pin.input.sha256, Some(pin))?;
        handoff_fingerprints(pin, &run, candidate)
    }

    #[test]
    fn capsule_handoff_authored_legacy_rows_and_resealed_mutations() {
        let (dir, paths) =
            super::super::scale_native_runner::source_layout_reducer_native_fixture_with(
                2049, 2, true, false,
            )
            .unwrap();
        let current: Vec<Value> = std::fs::read_to_string(&paths[1])
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let current = super::super::tests::historical_source_layout_rows(&current, true);
        let current_pin = handoff_fixture_pin(dir.path(), "native-current", current.clone());
        let expected = handoff_read(&current_pin, true).unwrap();
        let qi = current.iter().position(|r| r["phase"] == "query").unwrap();
        let si = current
            .iter()
            .position(|r| r["phase"] == "startup")
            .unwrap();

        let mut chunked = current.clone();
        for entry in chunked[si]["metadata"]["metadata"].as_array_mut().unwrap() {
            entry["chunks"] = json!(number(&entry["chunks"]).unwrap() + 1);
        }
        let pin = handoff_fixture_pin(dir.path(), "startup-chunks", chunked);
        assert_eq!(handoff_read(&pin, true).unwrap(), expected);

        // Independently authored historical full-capsule policy/model.
        let old = super::super::tests::historical_source_layout_rows(&current, false);
        let old_pin = handoff_fixture_pin(dir.path(), "synthetic-old", old);
        assert_eq!(handoff_read(&old_pin, false).unwrap(), expected);
        assert_eq!(handoff_descriptor_increment(31).unwrap(), 1240);
        assert!(handoff_descriptor_increment(u64::MAX).is_err());

        let mut observed = current.clone();
        observed.last_mut().unwrap()["summary"]["process_cpu_ns"] =
            json!(number(&observed.last().unwrap()["summary"]["process_cpu_ns"]).unwrap() + 1);
        observed.last_mut().unwrap()["summary"]["observed_process_peak_bytes"] = json!(123456789);
        let pin = handoff_fixture_pin(dir.path(), "timing-rss", observed);
        assert_eq!(handoff_read(&pin, true).unwrap(), expected);

        for mutation in [
            "rank",
            "target_row",
            "score",
            "plan",
            "trace",
            "bytes",
            "generation",
            "policy",
            "schema",
            "hash",
            "memory",
            "order",
            "startup_chunks",
            "startup_chunk_type",
            "startup_bytes",
            "startup_gets",
            "startup_roster",
            "startup_payload_bound",
            "recall",
        ] {
            let mut bad = current.clone();
            match mutation {
                "rank" => bad[qi]["returned"].as_array_mut().unwrap().swap(0, 1),
                "target_row" => bad[qi]["returned_physical_ordinals"]
                    .as_array_mut()
                    .unwrap()
                    .swap(0, 1),
                "score" => {
                    bad[qi]["returned"][0]["score_bits"] =
                        json!(number(&bad[qi]["returned"][0]["score_bits"]).unwrap() + 1)
                }
                "plan" => bad[qi]["plan"]["target_shortfall"] = json!(1),
                "trace" => bad[qi]["trace"]["ranked_candidate_pages"]
                    .as_array_mut()
                    .unwrap()
                    .reverse(),
                "bytes" => {
                    bad[qi]["stages"]["source_capsules"]["physical_bytes"] = json!(
                        number(&bad[qi]["stages"]["source_capsules"]["physical_bytes"]).unwrap()
                            + 1
                    )
                }
                "generation" => bad[1]["generation_root_sha256"] = json!("f".repeat(64)),
                "policy" => {
                    for header in &mut bad[..2] {
                        header["source_capsule_verification_policy"] = json!("skip");
                    }
                }
                "schema" => bad[0]["schema"] = json!(SOURCE_LAYOUT_RESULT_SCHEMA),
                "hash" => {
                    bad[qi]["stages"]["source_capsules"]["hash_bytes"] = json!(
                        number(&bad[qi]["stages"]["source_capsules"]["hash_bytes"]).unwrap() + 1
                    )
                }
                "memory" => {
                    bad[qi]["stages"]["source_capsules"]["memory_bytes"] = json!(
                        number(&bad[qi]["stages"]["source_capsules"]["memory_bytes"]).unwrap() + 1
                    )
                }
                "order" => bad[qi]["ordinal"] = json!(0),
                "startup_chunks"
                | "startup_chunk_type"
                | "startup_bytes"
                | "startup_gets"
                | "startup_roster"
                | "startup_payload_bound" => {
                    let entries = bad[si]["metadata"]["metadata"].as_array_mut().unwrap();
                    match mutation {
                        "startup_chunks" => entries[0]["chunks"] = json!(0),
                        "startup_chunk_type" => entries[0]["chunks"] = json!("1"),
                        "startup_bytes" => {
                            entries[0]["bytes"] = json!(number(&entries[0]["bytes"]).unwrap() + 1);
                        }
                        "startup_gets" => entries[0]["logical_get_requests"] = json!(0),
                        "startup_roster" => entries.swap(0, 1),
                        "startup_payload_bound" => {
                            entries[0]["payload_buffer_bound_bytes"] = json!(0);
                        }
                        _ => unreachable!(),
                    }
                }
                "recall" => {
                    let recall = bad.iter_mut().find(|r| r["phase"] == "recall").unwrap();
                    let hits = (number(&recall["hits10"]).unwrap() + 1) % 11;
                    recall["hits10"] = json!(hits);
                    recall["recall10"] = json!(hits as f64 / 10.0);
                    let summary = &mut bad.last_mut().unwrap()["summary"];
                    summary["total_hits10"] = json!(hits);
                    summary["recall_numerator"] = json!(hits);
                    summary["mean_recall10"] = json!(hits as f64 / 10.0);
                }
                _ => unreachable!(),
            }
            let pin = handoff_fixture_pin(dir.path(), "resealed-mutation", bad);
            match handoff_read(&pin, true) {
                Ok(actual) => {
                    assert!(
                        !mutation.starts_with("startup_"),
                        "ordinary reader accepted {mutation}"
                    );
                    assert_ne!(actual, expected, "{mutation}");
                }
                Err(error) => assert!(
                    !error.to_string().contains("seal"),
                    "mutation must survive resealing: {mutation}: {error}"
                ),
            }
        }
    }

    #[test]
    fn capsule_handoff_fixed32_and_authority_are_not_inferred() {
        assert_eq!(
            handoff_ordinals(),
            vec![
                0, 32, 64, 96, 128, 161, 193, 225, 257, 290, 322, 354, 386, 418, 451, 483, 515,
                547, 580, 612, 644, 676, 708, 741, 773, 805, 837, 870, 902, 934, 966, 999
            ]
        );
        let (dir, paths) =
            super::super::scale_native_runner::source_layout_reducer_native_fixture_with(
                32, 33, true, false,
            )
            .unwrap();
        let rows: Vec<Value> = std::fs::read_to_string(&paths[1])
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let rows = super::super::tests::historical_source_layout_rows(&rows, true);

        let runs = std::array::from_fn(|i| {
            let mut pin = handoff_fixture_pin(dir.path(), &format!("arm-{i}"), rows.clone());
            if matches!(i, 0 | 3) {
                pin.expected_identity["schema"] = json!(SOURCE_LAYOUT_RESULT_SCHEMA);
                pin.expected_identity["config_sha256"] = json!("a".repeat(64));
                pin.expected_identity["binary_sha256"] = json!("b".repeat(64));
                for header in [&mut pin.expected_identity, &mut pin.expected_bound_inputs] {
                    header["config_schema"] = json!(SOURCE_LAYOUT_CONFIG_SCHEMA);
                    header
                        .as_object_mut()
                        .unwrap()
                        .remove("source_capsule_verification_policy");
                }
            }
            pin
        });
        let mut c = CapsuleHandoffAbbaConfig {
            schema: "borsuk-capsule-handoff-abba-config-v1".into(),
            declared_run_order: super::super::LABELS.map(String::from),
            runs,
        };
        // These synthetic historical headers have 32 selected queries, but the wrong
        // original ordinals/population. Even the correct schema pair must fail.
        assert_eq!(
            handoff_headers(&c).unwrap_err().to_string(),
            "capsule handoff schema/policy/fixed original panel/operating point"
        );

        // Header-only synthetic protocol checks; these do not describe executions.
        for pin in &mut c.runs {
            for header in [&mut pin.expected_identity, &mut pin.expected_bound_inputs] {
                header["execution"] =
                    json!({"mode":"diagnostic_panel","ordinals":handoff_ordinals(),"trace":false});
            }
            let bound = &mut pin.expected_bound_inputs;
            bound["count"] = json!(1000);
            bound["requests_bytes"] = json!(1000 * number(&bound["dimensions"]).unwrap() * 4);
            bound["truth_bytes"] = json!(80_000);
            bound["reserved_query_interval"]["end"] =
                json!(number(&bound["reserved_query_interval"]["start"]).unwrap() + 1000);
        }
        handoff_headers(&c).unwrap();
        let mut wrong_dimensions: CapsuleHandoffAbbaConfig =
            serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
        for pin in &mut wrong_dimensions.runs {
            pin.expected_bound_inputs["dimensions"] = json!(768);
            pin.expected_bound_inputs["requests_bytes"] = json!(3_072_000);
            validate_completed(pin).unwrap();
        }
        assert_eq!(
            handoff_headers(&wrong_dimensions).unwrap_err().to_string(),
            "capsule handoff schema/policy/fixed original panel/operating point"
        );
        for mutation in [
            "run_order",
            "query_order",
            "unchanged_source",
            "within_arm",
            "schema",
            "policy",
        ] {
            let mut bad: CapsuleHandoffAbbaConfig =
                serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
            match mutation {
                "run_order" => bad.declared_run_order.swap(0, 1),
                "query_order" => {
                    for pin in &mut bad.runs {
                        for header in [&mut pin.expected_identity, &mut pin.expected_bound_inputs] {
                            header["execution"]["ordinals"]
                                .as_array_mut()
                                .unwrap()
                                .swap(0, 1);
                        }
                    }
                }
                "unchanged_source" => {
                    for i in [1, 2] {
                        bad.runs[i].expected_identity["codec_source_sha256"] =
                            json!("f".repeat(64));
                    }
                }
                "within_arm" => {
                    bad.runs[2].expected_identity["binary_sha256"] = json!("e".repeat(64))
                }
                "schema" => {
                    bad.runs[1].expected_identity["schema"] = json!(SOURCE_LAYOUT_RESULT_SCHEMA)
                }
                "policy" => {
                    for pin in &mut bad.runs[1..3] {
                        for header in [&mut pin.expected_identity, &mut pin.expected_bound_inputs] {
                            header["source_capsule_verification_policy"] = json!("skip");
                        }
                    }
                }
                _ => unreachable!(),
            }
            assert!(handoff_headers(&bad).is_err(), "{mutation}");
        }
        let body = serde_json::to_vec(&c).unwrap();
        let config = dir.path().join("duplicate-authority.json");
        std::fs::write(&config, &body).unwrap();
        let sha = format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(&body));
        assert_eq!(
            capsule_handoff_abba(&config, &sha).unwrap_err().to_string(),
            "capsule handoff distinct result paths/inodes/full hashes"
        );
        let mut raw = serde_json::to_value(&c).unwrap();
        raw["allow_memory_drift"] = json!(true);
        assert!(serde_json::from_value::<CapsuleHandoffAbbaConfig>(raw).is_err());
    }

    #[test]
    #[ignore = "root supplies independently frozen old/new producer ABBA files; never synthesize execution evidence"]
    fn capsule_handoff_abba_actual_producer_files() {
        let path = std::env::var_os("BORSUK_CAPSULE_HANDOFF_AUTHORITY")
            .expect("absolute path to independently frozen four-run authority");
        let sha = std::env::var("BORSUK_CAPSULE_HANDOFF_AUTHORITY_SHA256")
            .expect("independent complete authority SHA256");
        let report = capsule_handoff_abba(std::path::Path::new(&path), &sha).unwrap();
        assert_eq!(report["status"], "MEASURED");
        assert_eq!(report["returned_rank_parity_count"], 320);
        assert_eq!(report["quality_denominator"], 320);
        assert_eq!(report["population_percentiles_valid"], false);
        assert_eq!(report["qualified"], false);
        assert_eq!(report["comparisons"].as_array().unwrap().len(), 2);
    }
    #[test]
    fn cpu_profile_scope_clock_quanta_and_strict_counts() {
        let scope = |reads: Value| json!({"thread_cpu_ns":1,"wall_ns":2,"clock_reads":reads});
        let empty = serde_json::to_value(borsuk::sq8_s3_range::RangeCpu::default()).unwrap();
        range_cpu(&empty).unwrap();
        for (field, quantum) in [
            ("authentication", 2),
            ("collector_allocation", 4),
            ("collector_copy", 4),
            ("owned_allocation", 4),
            ("owned_copy", 4),
        ] {
            for calls in [1, 3, 5] {
                let mut cpu = empty.clone();
                cpu[field] = scope(json!(quantum * calls));
                range_cpu(&cpu).unwrap();
            }
            for reads in [
                json!(0),
                json!(1),
                json!(3),
                json!(5),
                json!(-1),
                json!(1.5),
                json!("2"),
                Value::Null,
            ] {
                let mut cpu = empty.clone();
                cpu[field] = scope(reads.clone());
                assert!(range_cpu(&cpu).is_err(), "{field}: {reads}");
            }
        }
        for field in [
            "collector_allocation",
            "collector_copy",
            "owned_allocation",
            "owned_copy",
        ] {
            let mut cpu = empty.clone();
            cpu[field] = scope(json!(2));
            assert!(
                range_cpu(&cpu).is_err(),
                "{field} accepted authentication quantum"
            );
        }
        cpu_wall(&Value::Null, 4, false).unwrap();
        cpu_wall(&scope(json!(4)), 4, false).unwrap();
        for reads in [0, 2, 8, 12] {
            assert!(
                cpu_wall(&scope(json!(reads)), 4, false).is_err(),
                "single scope: {reads}"
            );
        }
        let mut missing = scope(json!(4));
        missing.as_object_mut().unwrap().remove("wall_ns");
        assert!(cpu_wall(&missing, 4, false).is_err());
        let mut unknown = scope(json!(4));
        unknown["extra"] = json!(0);
        assert!(cpu_wall(&unknown, 4, false).is_err());
    }

    #[test]
    fn historical_trace_abi_is_independent_of_cpu_profile() {
        assert_eq!(historical_trace_retained(30), 8160);
        assert!(
            borsuk::sq8_s3_range::Sq8RangeTrace::retained_bytes(30) > historical_trace_retained(30)
        );
    }

    #[test]
    fn source_layout_explicit_caps_exceed_historical_limits_without_rewriting_them() {
        let mut header = json!({"config_schema":SOURCE_LAYOUT_CONFIG_SCHEMA,
            "max_memory_bytes":MEMORY_CAP+1,"source_byte_budget":67_108_865,
            "source_layout":{"layout":"flat_records"},
            "sq8_range_split":{"max_physical_range_bytes":MEMORY_CAP+1,"max_physical_gets":1}});
        validate_layout_header(&header).unwrap();
        split_option(&header).unwrap();
        super::super::validate_byte_window_cap(
            &json!({"max_bytes":MEMORY_CAP+1,"max_waiters":16,"max_state_bytes":MEMORY_CAP+1}),
            16,
            memory_cap(&header).unwrap(),
        )
        .unwrap();
        header.as_object_mut().unwrap().remove("config_schema");
        assert_eq!(memory_cap(&header).unwrap(), MEMORY_CAP);
        assert!(split_option(&header).is_err());
        header["config_schema"] = json!(SOURCE_LAYOUT_CONFIG_SCHEMA);
        for (key, value) in [
            ("source_byte_budget", json!(0)),
            ("source_byte_budget", json!(-1)),
            ("source_byte_budget", json!(1.5)),
            ("max_memory_bytes", json!(0)),
            ("max_memory_bytes", json!(-1)),
            ("max_memory_bytes", json!(1.5)),
        ] {
            let mut bad = header.clone();
            bad[key] = value;
            assert!(validate_layout_header(&bad).is_err(), "{key}");
        }
    }

    #[test]
    fn page_range_v2_dropped_groups_zero_framing_and_counter_refusals() {
        for (rows, leaves, units, directory, logical, duplicate, framing) in [
            (
                513_u64,
                vec![0, 2],
                vec![0, 1, 8, 9, 16],
                252,
                513 * 264,
                0,
                0,
            ),
            (
                832,
                vec![0, 1, 2],
                (0..26).collect(),
                256,
                832 * 264,
                256 * 264,
                44,
            ),
        ] {
            let physical = logical + duplicate + framing;
            let inputs = json!({"config_schema":PAGE_RANGE_CONFIG_SCHEMA,
                "source_capsule_verification_policy":PAGE_RANGE_VERIFICATION_POLICY,
                "rows":rows,"dimensions":1024,"profile":"scale1m",
                "source_get_budget":128,"source_byte_budget":1_000_000,
                "max_memory_bytes":2_000_000,
                "source_layout":{"layout":"leaf_capsules_v2","blob_bytes":1_000_000,
                    "blob_sha256":"a".repeat(64),"directory_bytes":directory,"directory_sha256":"b".repeat(64)}});
            let query = json!({"trace":{"semantic_leaves":leaves,"semantic_units":units},
                "charges":{"source":{"submitted_gets":1,"verified_bytes":physical,"failed_gets":0}},
                "stages":{"source_capsules":{"physical_bytes":physical,"logical_bytes":logical,
                    "duplicate_bytes":duplicate,"framing_bytes":framing,
                    "hash_bytes":physical-framing,"memory_bytes":1_000_000}}});
            validate_source_capsules(&query, &inputs).unwrap();
            if rows == 513 {
                // Whole full-page totals cannot cover a required one-row tail.
                let mut bad = query.clone();
                bad["trace"]["semantic_units"] = json!([16]);
                bad["stages"]["source_capsules"]["physical_bytes"] = json!(256 * 264);
                bad["stages"]["source_capsules"]["logical_bytes"] = json!(256 * 264);
                bad["stages"]["source_capsules"]["hash_bytes"] = json!(256 * 264);
                bad["charges"]["source"]["verified_bytes"] = json!(256 * 264);
                assert!(validate_source_capsules(&bad, &inputs).is_err());
            }
            for (pointer, value) in [
                (
                    "/stages/source_capsules/hash_bytes",
                    json!(2 * physical - framing),
                ),
                (
                    "/stages/source_capsules/physical_bytes",
                    json!(physical + 1),
                ),
                ("/stages/source_capsules/logical_bytes", json!(0)),
                ("/stages/source_capsules/memory_bytes", json!(physical - 1)),
                ("/stages/source_capsules/memory_bytes", json!(2_000_001)),
                ("/stages/source_capsules/framing_bytes", json!(-1)),
                ("/charges/source/verified_bytes", json!(physical - 1)),
                ("/charges/source/failed_gets", json!(1)),
                ("/charges/source/submitted_gets", json!(0)),
                ("/charges/source/submitted_gets", json!(3)),
            ] {
                let mut bad = query.clone();
                *bad.pointer_mut(pointer).unwrap() = value;
                assert!(
                    validate_source_capsules(&bad, &inputs).is_err(),
                    "{pointer}"
                );
            }
            for key in
                "physical_bytes logical_bytes duplicate_bytes framing_bytes hash_bytes memory_bytes"
                    .split_whitespace()
            {
                let mut bad = query.clone();
                bad["stages"]["source_capsules"]
                    .as_object_mut()
                    .unwrap()
                    .remove(key);
                assert!(validate_source_capsules(&bad, &inputs).is_err(), "{key}");
            }
            let mut bad = query.clone();
            bad["stages"]["source_capsules"]["skip_verification"] = json!(true);
            assert!(validate_source_capsules(&bad, &inputs).is_err());
            let mut old = inputs.clone();
            old["config_schema"] = json!(ONEPASS_CONFIG_SCHEMA);
            old["source_capsule_verification_policy"] = json!(SOURCE_CAPSULE_VERIFICATION_POLICY);
            assert!(validate_source_capsules(&query, &old).is_err());
        }
    }

    #[test]
    fn page_range_v2_frozen_historical_memory_boundary() {
        use std::mem::size_of;
        // Exact old source field shapes, independent of v2 Directory/VerifiedRows.
        let (decoded, scratch) = frozen_capsule_v1::open_charge(32, 1).unwrap();
        let physical = 32 * 264 + 36;
        let minimum = physical
            + decoded
            + scratch
            + size_of::<frozen_capsule_v1::LeafPages<'_>>()
            + 4
            + size_of::<frozen_capsule_v1::VerifiedRows<'_>>();
        for onepass in [false, true] {
            let mut inputs = json!({"config_schema":SOURCE_LAYOUT_CONFIG_SCHEMA,
                "rows":32,"dimensions":1024,"profile":"native100k",
                "source_get_budget":128,"source_byte_budget":1_000_000,"max_memory_bytes":1_000_000,
                "source_layout":{"layout":"leaf_capsules","blob_bytes":physical,
                    "blob_sha256":"a".repeat(64),"directory_bytes":96,"directory_sha256":"b".repeat(64)}});
            if onepass {
                inputs["config_schema"] = json!(ONEPASS_CONFIG_SCHEMA);
                inputs["source_capsule_verification_policy"] =
                    json!(SOURCE_CAPSULE_VERIFICATION_POLICY);
            }
            let mut query = json!({"trace":{"semantic_leaves":[0],"semantic_units":[0]},
                "charges":{"source":{"submitted_gets":1,"verified_bytes":physical,"failed_gets":0}},
                "stages":{"source_capsules":{"physical_bytes":physical,"logical_bytes":32*264,
                    "duplicate_bytes":0,"framing_bytes":36,
                    "hash_bytes":if onepass {2*physical-36} else {2*(2*physical-36)},
                    "memory_bytes":minimum}}});
            validate_source_capsules(&query, &inputs).unwrap();
            query["stages"]["source_capsules"]["memory_bytes"] = json!(minimum - 1);
            assert!(validate_source_capsules(&query, &inputs).is_err());
            assert!(frozen_capsule_v1::open_charge(32, 0).is_err());
            assert!(frozen_capsule_v1::open_charge(32, 2).is_err());
        }
    }

    #[test]
    fn capsule_page_union_tail_duplicates_and_adjacent_gets() {
        // Literal whole-page oracles: D1024 records are 264 bytes.
        for (rows, ids, units, logical, duplicate, framing, gets, directory_leaves) in [
            (32, vec![0], vec![0], 8448, 0, 36, 1, 1),
            (256, vec![1, 0], (0..8).collect(), 67584, 67584, 72, 1, 2),
            (257, vec![0, 1], (0..9).collect(), 67848, 67584, 76, 1, 2),
            (289, vec![1, 0], (0..10).collect(), 76296, 76296, 80, 1, 2),
            (257, vec![2, 0], vec![0, 1, 8], 67848, 67584, 76, 2, 3),
            (289, vec![0, 2], vec![0, 1], 67584, 67584, 72, 2, 3),
        ] {
            let physical = logical + duplicate + framing;
            let inputs = json!({"config_schema":SOURCE_LAYOUT_CONFIG_SCHEMA,
                "rows":rows,"dimensions":1024,"profile":"scale1m",
                "source_get_budget":128,"source_byte_budget":67_108_865,
                "max_memory_bytes":MEMORY_CAP+1,
                "source_layout":{"layout":"leaf_capsules","blob_bytes":1_000_000,
                    "blob_sha256":"a".repeat(64),"directory_bytes":32+64*directory_leaves,
                    "directory_sha256":"b".repeat(64)}});
            let query = json!({"trace":{"semantic_leaves":ids,"semantic_units":units},
                "charges":{"source":{"submitted_gets":gets,"verified_bytes":physical,"failed_gets":0}},
                "stages":{"source_capsules":{"physical_bytes":physical,"logical_bytes":logical,
                    "duplicate_bytes":duplicate,"framing_bytes":framing,
                    "hash_bytes":2*(physical+logical+duplicate),"memory_bytes":1_000_000}}});
            validate_source_capsules(&query, &inputs).unwrap();
            for bad_gets in [0, gets + 1] {
                let mut bad = query.clone();
                bad["charges"]["source"]["submitted_gets"] = json!(bad_gets);
                assert!(
                    validate_source_capsules(&bad, &inputs).is_err(),
                    "{rows}: GET {bad_gets}"
                );
            }
            for shift in [264, logical - 264] {
                let mut bad = query.clone();
                bad["stages"]["source_capsules"]["logical_bytes"] = json!(logical - shift);
                bad["stages"]["source_capsules"]["duplicate_bytes"] = json!(duplicate + shift);
                assert!(
                    validate_source_capsules(&bad, &inputs).is_err(),
                    "{rows}: split {shift}"
                );
            }
            for key in ["semantic_leaves", "semantic_units"] {
                let mut bad = query.clone();
                let array = bad["trace"][key].as_array_mut().unwrap();
                array.push(array[0].clone());
                assert!(validate_source_capsules(&bad, &inputs).is_err(), "{key}");
            }
            let mut bad = query.clone();
            bad["stages"]["source_capsules"]["framing_bytes"] = json!(framing + 4);
            bad["stages"]["source_capsules"]["physical_bytes"] = json!(physical + 4);
            bad["stages"]["source_capsules"]["hash_bytes"] =
                json!(2 * (physical + logical + duplicate + 4));
            bad["charges"]["source"]["verified_bytes"] = json!(physical + 4);
            assert!(
                validate_source_capsules(&bad, &inputs).is_err(),
                "{rows}: phantom page"
            );
            // Preserve sums/hash/record divisibility while removing one row from
            // duplicated payload: whole-page/tail multiplicity still must reject.
            if duplicate > 0 {
                let mut bad = query.clone();
                bad["stages"]["source_capsules"]["physical_bytes"] = json!(physical - 264);
                bad["stages"]["source_capsules"]["duplicate_bytes"] = json!(duplicate - 264);
                bad["stages"]["source_capsules"]["hash_bytes"] =
                    json!(2 * (physical + logical + duplicate - 528));
                bad["charges"]["source"]["verified_bytes"] = json!(physical - 264);
                assert!(
                    validate_source_capsules(&bad, &inputs).is_err(),
                    "{rows}: partial duplicate"
                );
            }
        }
    }

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

    fn source_span_fixture() -> (Value, Value) {
        let inputs = json!({"rows":769,"dimensions":32,"source_get_budget":96,
            "source_byte_budget":67108864,"fetch_parallelism":16,
            "source_layout":{"layout":"flat_records"},"serving":{"mode":"baseline"}});
        let ranges = json!([
            [
                0, 0, 7, 0, 4096, 13, 14, 15, 16, 17, 17, 18, 19, 20, 21, 1, 4096, 1, 17, 1, 1,
                "ok"
            ],
            [
                1, 16, 24, 8192, 12304, 14, 15, 16, 17, 18, 18, 19, 20, 21, 22, 1, 4112, 1, 18, 1,
                1, "ok"
            ]
        ]);
        let mut source =
            serde_json::to_value(borsuk::sq8_s3_range::Sq8RangeTrace::new(96).unwrap()).unwrap();
        for (key, value) in [
            ("outcome", json!("fetched")),
            ("planned_ranges", json!(2)),
            ("planned_bytes", json!(8208)),
            ("max_parallel", json!(2)),
            ("fetch_start_ns", json!(12)),
            ("all_ranges_complete_ns", json!(35)),
            ("release_end_ns", json!(51)),
            ("retained_payload_bytes", json!(8208)),
            ("phase_process_cpu_ns", json!(100)),
            ("unattributed_process_cpu_ns", json!(100)),
            ("phase_clock_reads", json!(2)),
            ("ranges", ranges),
        ] {
            source[key] = value;
        }
        let raw = json!({"source_nomination_skipped":false,
            "trace":{"ranked_candidate_pages":[0,2,3]},
            "stages":{"source":{"start_ns":10,"end_ns":40},
                "planning":{"start_ns":41,"end_ns":50},"sq8":{"start_ns":60}},
            "charges":{"source":{"submitted_gets":2,"verified_bytes":8208,"failed_gets":0}},
            "diagnostic":{"source_range_trace":source}});
        (raw, inputs)
    }

    #[test]
    fn source_v5_complete_roster_plan_timestamps_and_charges() {
        let (raw, inputs) = source_span_fixture();
        validate_source_trace(&raw, &inputs).unwrap();
        for (pointer, value) in [
            ("/diagnostic/source_range_trace/dropped_ranges", json!(1)),
            (
                "/diagnostic/source_range_trace/range_fields/0",
                json!("offset"),
            ),
            ("/diagnostic/source_range_trace/planned_ranges", json!(1)),
            ("/diagnostic/source_range_trace/planned_bytes", json!(8207)),
            ("/diagnostic/source_range_trace/max_parallel", json!(1)),
            (
                "/diagnostic/source_range_trace/retained_payload_bytes",
                json!(0),
            ),
            ("/diagnostic/source_range_trace/ranges/1/0", json!(0)),
            ("/diagnostic/source_range_trace/ranges/1/1", json!(15)),
            ("/diagnostic/source_range_trace/ranges/1/3", json!(8193)),
            ("/diagnostic/source_range_trace/ranges/1/5", json!(11)),
            ("/diagnostic/source_range_trace/ranges/1/12", Value::Null),
            ("/diagnostic/source_range_trace/ranges/1/14", json!(36)),
            ("/diagnostic/source_range_trace/ranges/1/15", json!(0)),
            ("/diagnostic/source_range_trace/ranges/1/16", json!(4111)),
            ("/diagnostic/source_range_trace/ranges/1/17", json!(9)),
            (
                "/diagnostic/source_range_trace/ranges/1/18",
                json!(u64::MAX),
            ),
            ("/diagnostic/source_range_trace/ranges/1/19", json!(9)),
            ("/diagnostic/source_range_trace/ranges/1/20", json!(2)),
            (
                "/diagnostic/source_range_trace/ranges/1/21",
                json!("page_auth"),
            ),
            (
                "/diagnostic/source_range_trace/all_ranges_complete_ns",
                json!(41),
            ),
            ("/diagnostic/source_range_trace/release_end_ns", json!(49)),
            ("/diagnostic/source_range_trace/rank_start_ns", json!(45)),
            ("/charges/source/submitted_gets", json!(1)),
            ("/charges/source/verified_bytes", json!(8207)),
            ("/charges/source/failed_gets", json!(1)),
        ] {
            let mut bad = raw.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(validate_source_trace(&bad, &inputs).is_err(), "{pointer}");
        }
        for mutation in [
            "drop",
            "duplicate",
            "reverse",
            "width",
            "unknown",
            "missing",
        ] {
            let mut bad = raw.clone();
            let trace = &mut bad["diagnostic"]["source_range_trace"];
            match mutation {
                "drop" => {
                    trace["ranges"].as_array_mut().unwrap().pop();
                }
                "duplicate" => {
                    trace["ranges"][1] = trace["ranges"][0].clone();
                }
                "reverse" => {
                    trace["ranges"].as_array_mut().unwrap().reverse();
                }
                "width" => {
                    trace["ranges"][1].as_array_mut().unwrap().pop();
                }
                "unknown" => {
                    trace["extra"] = json!(0);
                }
                "missing" => {
                    trace.as_object_mut().unwrap().remove("ranges");
                }
                _ => unreachable!(),
            }
            assert!(validate_source_trace(&bad, &inputs).is_err(), "{mutation}");
        }
        // Capsule byte offsets are physical; no single logical page interval exists.
        let mut capsule_inputs = inputs.clone();
        capsule_inputs["source_layout"] = json!({"layout":"leaf_capsules_v2","blob_bytes":12304});
        let mut capsule = raw.clone();
        for row in capsule["diagnostic"]["source_range_trace"]["ranges"]
            .as_array_mut()
            .unwrap()
        {
            row[1] = json!(u64::MAX);
            row[2] = json!(u64::MAX);
        }
        validate_source_trace(&capsule, &capsule_inputs).unwrap();
        for (pointer, value) in [
            ("/diagnostic/source_range_trace/ranges/0/1", json!(0)),
            ("/diagnostic/source_range_trace/ranges/1/3", json!(4095)),
            ("/diagnostic/source_range_trace/ranges/1/4", json!(12305)),
        ] {
            let mut bad = capsule.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(
                validate_source_trace(&bad, &capsule_inputs).is_err(),
                "{pointer}"
            );
        }
    }

    #[test]
    fn source_v5_direct_requires_an_empty_unstarted_source_phase() {
        let (mut raw, mut inputs) = source_span_fixture();
        inputs["serving"]["mode"] = json!("direct_closure");
        raw["source_nomination_skipped"] = json!(true);
        raw["charges"]["source"] = json!({"submitted_gets":0,"verified_bytes":0,"failed_gets":0});
        let mut trace = borsuk::sq8_s3_range::Sq8RangeTrace::new(96).unwrap();
        trace.begin(std::time::Instant::now());
        raw["diagnostic"]["source_range_trace"] = serde_json::to_value(trace).unwrap();
        validate_source_trace(&raw, &inputs).unwrap();
        for (pointer, value) in [
            ("/charges/source/submitted_gets", json!(1)),
            ("/charges/source/verified_bytes", json!(1)),
            ("/source_nomination_skipped", json!(false)),
            ("/diagnostic/source_range_trace/outcome", json!("fetched")),
            ("/diagnostic/source_range_trace/fetch_start_ns", json!(0)),
            (
                "/diagnostic/source_range_trace/retained_payload_bytes",
                json!(0),
            ),
            ("/diagnostic/source_range_trace/phase_clock_reads", json!(2)),
            (
                "/diagnostic/source_range_trace/cpu/collector_copied_bytes",
                json!(1),
            ),
            (
                "/diagnostic/source_range_trace/ranges",
                source_span_fixture().0["diagnostic"]["source_range_trace"]["ranges"].clone(),
            ),
        ] {
            let mut bad = raw.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(validate_source_trace(&bad, &inputs).is_err(), "{pointer}");
        }
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
                    historical_trace_peak(ranges, width)
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
                    "trace_retained_bytes":if traced { json!(historical_trace_retained(ranges)) } else { Value::Null },
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

#[cfg(test)]
mod source_authentication_tests {
    use super::*;
    #[test]
    fn source_authentication_selector_and_exact_reservation_are_bound() {
        let mut inputs = json!({"config_schema":CPU_PROFILE_CONFIG_SCHEMA,
            "range_scheduler_owner":"library","fetch_parallelism":64,"source_get_budget":128,
            "source_authentication":"offload2"});
        assert_eq!(
            source_authentication_charge(&inputs).unwrap(),
            Some(2_380_032)
        );
        let valid = json!({"source_authentication_extra_library_bytes":2_380_032});
        validate_source_authentication_charge(&valid, &inputs).unwrap();
        for bad in [
            json!({}),
            json!({"source_authentication_extra_library_bytes":2_380_031}),
            json!({"source_authentication_extra_library_bytes":2_380_033}),
            json!({"source_authentication_extra_library_bytes":"2380032"}),
        ] {
            assert!(validate_source_authentication_charge(&bad, &inputs).is_err());
        }
        for value in [
            Value::Null,
            json!(2),
            json!("offload"),
            json!({}),
            json!(true),
        ] {
            inputs["source_authentication"] = value;
            assert!(source_authentication_charge(&inputs).is_err());
        }
        inputs["source_authentication"] = json!("inline");
        assert_eq!(source_authentication_charge(&inputs).unwrap(), Some(0));
        validate_source_authentication_charge(
            &json!({"source_authentication_extra_library_bytes":0}),
            &inputs,
        )
        .unwrap();
        assert!(validate_source_authentication_charge(&valid, &inputs).is_err());
        inputs
            .as_object_mut()
            .unwrap()
            .remove("source_authentication");
        assert_eq!(source_authentication_charge(&inputs).unwrap(), None);
        validate_source_authentication_charge(&json!({}), &inputs).unwrap();
        assert!(validate_source_authentication_charge(&valid, &inputs).is_err());
    }
}
