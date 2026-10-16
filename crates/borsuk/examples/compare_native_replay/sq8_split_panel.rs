//! Explicit v10 transport-split screen. File authentication is not native qualification.
use super::{CompletedConfig, CompletedInput, Result, fields, plus, require};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::Path;

pub(super) const RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v10";
pub(super) const REPORT_SCHEMA: &str = "borsuk-sq8-split-panel-v1";
const CONFIG_SCHEMA: &str = "borsuk-sq8-split-panel-config-v1";
const LABELS: [&str; 6] = ["B0", "C0", "B1", "C1", "C2", "B2"];
const CONFIG_CAP: u64 = 262144;
const RETAINED_CAP: u64 = 12 * 1024 * 1024;
const SPLIT_BYTES: u64 = 2121728;
const BLOCK_BYTES: u64 = 32 * 1036;
const SCOPE_NOTE: &str = "diagnostic selected original ordinals or prefix; not population p99, population recall or full-cohort qualification";

const ORDINALS: [usize; 32] = [
    0, 32, 64, 96, 128, 161, 193, 225, 257, 290, 322, 354, 386, 418, 451, 483, 515, 547, 580, 612,
    644, 676, 708, 741, 773, 805, 837, 870, 902, 934, 966, 999,
];
const COMPONENTS: [&str; 7] = [
    "runner_source_sha256",
    "generation_source_sha256",
    "router_source_sha256",
    "codec_source_sha256",
    "source_plane_source_sha256",
    "sq8_range_source_sha256",
    "returned_source_sha256",
];
const SOURCE_PINS: [&str; 7] = [
    "e2c4c1cdf1f15d1908ad2b7f6ce9ee549f38f0853710c09c5c679c7e226ccfbf",
    "ad3002c5dec2adb360a02ff75c50688d708cd5f1dfbe1526bfba4f6d280768e7",
    "a8b6dbc74ea15b08d15499bcf83cab680c17fcc040840e422818b505a8758cd8",
    "eddf88c6c8ee23a732751f49291293f1cc081545ebb9a21c149eb757aa5c38ec",
    "dbcc4cdbc4bb5c244354b375afd657f8b5cb1892df3f0ff0b8f41ef580de42e0",
    "22e71cca27c0c8fdd1869f6c47afbf1c6d1bfc2427ebdfc8a3e7bcf920af0bea",
    "a4dcc7f9bc06cdea835f72860f3d2e796fcb6f29276858c349d624884cc9f74b",
];
const RANGE_FIELDS: [&str; 22] = [
    "index",
    "first_page",
    "last_page",
    "start_byte",
    "end_byte",
    "first_poll_ns",
    "request_ns",
    "headers_ns",
    "metadata_ns",
    "first_chunk_ns",
    "last_chunk_ns",
    "eof_ns",
    "auth_start_ns",
    "auth_end_ns",
    "complete_ns",
    "chunks",
    "body_bytes",
    "max_body_gap_ns",
    "max_body_gap_end_ns",
    "copy_ns",
    "copy_count",
    "outcome",
];
const HOST_FIELDS: [&str; 48] = [
    "TcpExt.TCPTimeouts",
    "TcpExt.TCPLossProbes",
    "TcpExt.TCPFastRetrans",
    "TcpExt.TCPSlowStartRetrans",
    "TcpExt.TCPLostRetransmit",
    "TcpExt.TCPSpuriousRTOs",
    "TcpExt.TCPSynRetrans",
    "TcpExt.TCPRetransFail",
    "TcpExt.TCPOFOQueue",
    "TcpExt.TCPOFODrop",
    "TcpExt.TCPOFOMerge",
    "TcpExt.TCPDSACKOldSent",
    "TcpExt.TCPDSACKOfoSent",
    "TcpExt.TCPDSACKRecv",
    "TcpExt.TCPToZeroWindowAdv",
    "TcpExt.TCPWantZeroWindowAdv",
    "TcpExt.TCPFromZeroWindowAdv",
    "TcpExt.TCPZeroWindowDrop",
    "TcpExt.TCPRcvQDrop",
    "TcpExt.TCPBacklogDrop",
    "TcpExt.TCPRcvCollapsed",
    "TcpExt.RcvPruned",
    "TcpExt.OfoPruned",
    "TcpExt.PruneCalled",
    "Tcp.ActiveOpens",
    "Tcp.PassiveOpens",
    "Tcp.AttemptFails",
    "Tcp.EstabResets",
    "Tcp.InSegs",
    "Tcp.OutSegs",
    "Tcp.RetransSegs",
    "Tcp.InErrs",
    "Tcp.OutRsts",
    "cpu0.user_ticks",
    "cpu0.system_ticks",
    "cpu0.irq_ticks",
    "cpu0.softirq_ticks",
    "cpu0.steal_ticks",
    "cpu.irq_ticks",
    "cpu.softirq_ticks",
    "cpu.steal_ticks",
    "thread.sched_run_ns",
    "thread.sched_wait_ns",
    "thread.sched_timeslices",
    "thread.voluntary_ctxt_switches",
    "thread.nonvoluntary_ctxt_switches",
    "process.minor_faults",
    "process.major_faults",
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    runs: [Descriptor; 6],
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    label: String,
    completed: CompletedConfig,
    runtime_config: CompletedInput,
}

fn number(v: &Value) -> Result<u64> {
    v.as_u64()
        .ok_or_else(|| "split panel unsigned integer required".into())
}
fn duration(start: u64, end: u64) -> Result<u64> {
    end.checked_sub(start)
        .ok_or_else(|| "split panel reversed timestamps".into())
}
fn candidate(slot: usize) -> bool {
    matches!(slot, 1 | 3 | 4)
}
fn split_value(enabled: bool) -> Value {
    if enabled {
        json!({"max_physical_range_bytes":SPLIT_BYTES,"max_physical_gets":46})
    } else {
        json!({"max_physical_range_bytes":33554432,"max_physical_gets":46})
    }
}
pub(super) fn physical_cap(_inputs: &Value) -> u64 {
    46
}
fn small_pieces(inputs: &Value) -> bool {
    inputs["sq8_range_split"]["max_physical_range_bytes"] == SPLIT_BYTES
}
fn split_option(header: &Value) -> Result<()> {
    let split = header
        .get("sq8_range_split")
        .ok_or("explicit v10 split selector required")?;
    require(
        *split == split_value(false) || *split == split_value(true),
        "fixed split selector/caps",
    )
}

// Header schema stays v10. Only explicitly checked extension fields are removed
// before the version-neutral exact field roster; legacy header validators stay strict.
pub(super) fn validate_row(line: &[u8], phase: &str) -> Result<()> {
    if !matches!(phase, "identity" | "bound_inputs") {
        if phase == "terminal" {
            let super::UniqueJson(raw) = serde_json::from_slice(line)?;
            require(
                raw["summary"]["scope_note"] == SCOPE_NOTE,
                "v10 terminal scope note",
            )?;
        }
        return super::v9_panel::validate_row(line, phase);
    }
    let super::UniqueJson(mut raw) = serde_json::from_slice(line)?;
    split_option(&raw)?;
    require(
        raw["source_get_budget"] == 128
            && raw["sq8_get_budget"] == 30
            && raw["sq8_byte_budget"] == 33554432
            && raw["fetch_parallelism"] == 64
            && raw["serving"] == json!({"mode":"baseline"}),
        "v10 operating point",
    )?;
    require(
        super::v9_panel::ordinals(&raw, 1000)? == ORDINALS,
        "v10 fixed32 roster",
    )?;
    if let Some(window) = raw.get("shared_byte_window") {
        super::validate_byte_window(window, 64)?;
    }
    let extras = if phase == "identity" {
        require(raw["schema"] == RESULT_SCHEMA, "explicit v10 result schema")?;
        for (key, pin) in COMPONENTS.iter().zip(SOURCE_PINS) {
            require(raw[*key] == pin, "v10 source-bound component pin")?;
        }
        "source_get_budget sq8_get_budget sq8_byte_budget sq8_range_split serving execution sq8_range_source_sha256 returned_source_sha256"
    } else {
        "source_get_budget sq8_get_budget sq8_byte_budget sq8_range_split serving execution selected_count corpus_intervals reserved_query_interval reserved_queries_sha256 cohort_receipt_sha256 derivation_receipt_sha256 producer_authority max_memory_bytes"
    };
    let obj = raw.as_object_mut().ok_or("v10 header object")?;
    for key in extras.split_whitespace() {
        require(obj.remove(key).is_some(), "v10 required extension field")?;
    }
    obj.remove("shared_byte_window");
    super::validate_v2_row(&serde_json::to_vec(&raw)?, phase)
}

pub(super) fn validate_completed(c: &CompletedConfig) -> Result<()> {
    let i = &c.expected_identity;
    let b = &c.expected_bound_inputs;
    require(
        c.schema == super::SCALE_CONFIG_SCHEMA
            && c.input.bytes > 0
            && c.input.bytes <= super::FILE_CAP
            && super::valid_sha(&c.input.sha256),
        "split completed descriptor",
    )?;
    for (v, phase) in [(i, "identity"), (b, "bound_inputs")] {
        let bytes = serde_json::to_vec(v)?;
        require(bytes.len() < 65536, "split header cap")?;
        validate_row(&bytes, phase)?;
    }
    super::validate_native_identity(&serde_json::from_value(i.clone())?, i, RESULT_SCHEMA)?;
    super::validate_scale_inputs_for_version(&serde_json::from_value(b.clone())?, b, true)?;
    require(
        b["rows"] == 1000000
            && b["count"] == 1000
            && b["dimensions"] == 1024
            && b["k"] == 10
            && b["selected_count"] == 32
            && b["backend"]["kind"] == "s3"
            && b["profile"] == "scale1m",
        "split fixed population",
    )?;
    for key in [
        "fetch_parallelism",
        "source_get_budget",
        "sq8_get_budget",
        "sq8_byte_budget",
        "sq8_range_split",
        "shared_byte_window",
        "execution",
        "serving",
    ] {
        require(i.get(key) == b.get(key), "split identity/input agreement")?;
    }
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
        require(b[key] == pin, "split fixed input authority")?;
    }
    Ok(())
}

fn neutral_header(mut v: Value, identity: bool) -> Value {
    let obj = v.as_object_mut().expect("validated header");
    obj.remove("sq8_range_split");
    if identity {
        obj.remove("config_sha256");
    }
    v["execution"]["trace"] = json!(false);
    v
}

fn runtime_config(d: &Descriptor) -> Result<(Value, super::HeaderSnapshot)> {
    super::header_pin(&d.runtime_config, 32768)?;
    let (bytes, snapshot) = super::loss_binary(&d.runtime_config, d.runtime_config.bytes)?;
    let super::UniqueJson(raw) = serde_json::from_slice(&bytes)?;
    let b = &d.completed.expected_bound_inputs;
    require(
        raw["schema"] == "borsuk-cohere-native-baseline-config-v10"
            && d.completed.expected_identity["config_sha256"] == d.runtime_config.sha256,
        "split runtime schema/config identity",
    )?;
    let optional = if raw.get("shared_byte_window").is_some() {
        " shared_byte_window"
    } else {
        ""
    };
    fields(
        &raw,
        &format!(
            "schema dataset revision metric tie_rule corpus_intervals reserved_query_interval cohort_receipt derivation_receipt producer_authority corpus_source_first query_source_first rows dimensions count k profile backend generation_prefix generation_root_sha256 scratch_parent requests truth native_source max_memory_bytes sq8_get_budget sq8_byte_budget fetch_parallelism source_get_budget sq8_range_split serving execution{optional}"
        ),
    )?;
    for key in [
        "dataset",
        "revision",
        "metric",
        "tie_rule",
        "corpus_intervals",
        "reserved_query_interval",
        "producer_authority",
        "corpus_source_first",
        "query_source_first",
        "rows",
        "dimensions",
        "count",
        "k",
        "profile",
        "backend",
        "generation_prefix",
        "generation_root_sha256",
        "max_memory_bytes",
        "sq8_get_budget",
        "sq8_byte_budget",
        "fetch_parallelism",
        "source_get_budget",
        "sq8_range_split",
        "serving",
        "execution",
    ] {
        require(raw[key] == b[key], "runtime config/header binding")?;
    }
    require(
        raw["shared_byte_window"] == b["shared_byte_window"],
        "runtime window binding",
    )?;
    for key in ["requests", "truth", "cohort_receipt", "derivation_receipt"] {
        fields(&raw[key], "path bytes sha256")?;
        require(
            raw[key]["path"]
                .as_str()
                .is_some_and(|p| !p.is_empty() && p.len() <= 4096)
                && number(&raw[key]["bytes"])? > 0
                && raw[key]["sha256"].as_str().is_some_and(super::valid_sha),
            "runtime artifact descriptor",
        )?;
        require(
            raw[key]["sha256"] == b[format!("{key}_sha256")],
            "runtime artifact hash binding",
        )?;
    }
    require(
        raw["requests"]["bytes"] == b["requests_bytes"]
            && raw["truth"]["bytes"] == b["truth_bytes"],
        "runtime query/truth lengths",
    )?;
    fields(
        &raw["native_source"],
        "source_sha256 sq8_sha256 source_order_sha256",
    )?;
    for (key, bound) in [
        ("source_sha256", "native_source_sha256"),
        ("sq8_sha256", "native_sq8_sha256"),
        ("source_order_sha256", "native_order_sha256"),
    ] {
        require(
            raw["native_source"][key] == b[bound],
            "runtime native source binding",
        )?;
    }
    require(
        raw["scratch_parent"]
            .as_str()
            .is_some_and(|p| Path::new(p).is_absolute() && p.len() <= 4096),
        "runtime scratch path",
    )?;
    Ok((raw, snapshot))
}

pub(super) fn validate_admission(raw: &Value, inputs: &Value) -> Result<()> {
    use borsuk::sq8_s3_range::Sq8RangeTrace;
    let traced = inputs["execution"]["trace"] == true;
    let ranges = physical_cap(inputs) as usize;
    let peak = if traced {
        Sq8RangeTrace::modeled_bytes(ranges, 64)
    } else {
        0
    };
    let both = Sq8RangeTrace::pending_futures_bytes(128, 64);
    let pinned = both
        .checked_sub(if traced {
            Sq8RangeTrace::pending_futures_bytes(ranges, 64)
        } else {
            0
        })
        .ok_or("split pinned subtraction")?;
    require(
        cfg!(target_pointer_width = "64")
            && std::mem::size_of::<Option<u64>>() == 16
            && std::mem::size_of::<std::path::PathBuf>() == 24
            && std::mem::size_of::<Vec<u8>>() == 24,
        "split producer sampler ABI",
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
            && raw["range_state_pinned_by_runner_bytes"] == pinned
            && raw["trace_peak_charged_by_library_at_traced_admission"] == traced
            && number(&raw["caller_pinned_bytes"])? >= (diagnostic + pinned) as u64,
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
            == json!({
                "headers_interval":"get_opts_client_queue_connect_tls_server_combined",
                "body_gaps":"application_observed_poll_gaps_not_packet_arrival",
                "ranges":"overlap_in_time_never_sum_intervals",
                "host_counters":"host_wide_not_per_flow_null_means_unavailable",
                "unset":"a boundary never reached is null, never a fabricated timestamp"
            }),
        "split diagnostic semantics",
    )
}

fn physical_ranges(raw: &Value, split: bool) -> Result<Vec<Value>> {
    let plan = raw["plan"]["ranges"].as_array().ok_or("logical ranges")?;
    let mut physical = Vec::new();
    for r in plan {
        let (mut start, end) = (number(&r[0])?, number(&r[1])?);
        while start < end {
            let next = if split {
                end.min(plus(start, (SPLIT_BYTES / BLOCK_BYTES) * BLOCK_BYTES)?)
            } else {
                end
            };
            require(physical.len() < 46, "physical GET cap")?;
            physical.push(json!([start, next]));
            start = next;
        }
    }
    Ok(physical)
}

pub(super) fn validate_geometry(raw: &Value, inputs: &Value) -> Result<()> {
    // The old canonical logical oracle checks its own exact logical charge.
    // Raw v10 physical charges are checked separately, never passed as logical charges.
    let mut logical = raw.clone();
    let count = raw["plan"]["ranges"]
        .as_array()
        .ok_or("logical range roster")?
        .len();
    logical["charges"]["sq8"]["submitted_gets"] = json!(count);
    super::v9_panel::validate_geometry(&logical, 1000000, 1024, 30, 33554432)?;
    let physical = physical_ranges(raw, small_pieces(inputs))?;
    require(
        raw["charges"]["sq8"]
            == json!({"submitted_gets":physical.len(),
        "verified_bytes":raw["plan"]["planned_bytes"],"failed_gets":0}),
        "split exact physical charges",
    )?;
    if let Some(diag) = raw.get("diagnostic") {
        let ranges = diag["sq8_range_trace"]["ranges"]
            .as_array()
            .ok_or("split trace ranges")?;
        require(
            ranges.len() == physical.len(),
            "split trace full drain roster",
        )?;
        for (r, expected) in ranges.iter().zip(&physical) {
            require(
                r[3] == expected[0] && r[4] == expected[1],
                "split exact physical coverage",
            )?;
        }
    }
    Ok(())
}

fn diagnostic_shape(diag: &Value) -> Result<()> {
    fields(
        diag,
        "schema query_monotonic_start_ns query_monotonic_end_ns process_cpu_start_ns process_cpu_end_ns sq8_range_trace host",
    )?;
    require(
        diag["schema"] == "borsuk-sq8-range-attribution-diagnostic-v2",
        "panel diagnostic schema",
    )
}

fn nullable(v: &Value) -> Result<Option<u64>> {
    if v.is_null() {
        Ok(None)
    } else {
        Ok(Some(number(v)?))
    }
}
fn host(diag: &Value, query_cpu_ns: u64) -> Result<Value> {
    let h = &diag["host"];
    fields(
        h,
        "scope window before_monotonic_ns after_monotonic_ns unavailable deltas",
    )?;
    require(
        h["scope"] == "host_wide_counter_deltas_not_per_flow_or_per_s3_connection"
            && h["window"]
                == "between_untimed_snapshots_that_bracket_the_query_and_include_runner_bookkeeping",
        "panel host evidence scope",
    )?;
    fields(&h["deltas"], &HOST_FIELDS.join(" "))?;
    let mut missing = 0;
    let mut deltas = Vec::with_capacity(HOST_FIELDS.len());
    for key in HOST_FIELDS {
        let v = nullable(&h["deltas"][key])?;
        missing += usize::from(v.is_none());
        deltas.push(v);
    }
    require(h["unavailable"] == missing, "panel host unavailable count")?;
    let anchors = [
        nullable(&h["before_monotonic_ns"])?,
        nullable(&diag["query_monotonic_start_ns"])?,
        nullable(&diag["query_monotonic_end_ns"])?,
        nullable(&h["after_monotonic_ns"])?,
    ];
    let mut previous = None;
    for value in anchors.into_iter().flatten() {
        if let Some(p) = previous {
            duration(p, value)?;
        }
        previous = Some(value);
    }
    let cpu = [
        number(&diag["process_cpu_start_ns"])?,
        number(&diag["process_cpu_end_ns"])?,
    ];
    require(
        duration(cpu[0], cpu[1])? == query_cpu_ns,
        "panel process CPU reconciliation",
    )?;
    Ok(
        json!({"deltas":deltas,"unavailable":missing,"monotonic_brackets_ns":anchors,"process_cpu_brackets_ns":cpu}),
    )
}

#[derive(Serialize)]
struct Decomposition {
    boundaries_ns: [u64; 8],
    intervals_ns: [u64; 7],
    sq8_ns: u64,
    post_barrier_ns: u64,
    latest_range: Vec<Value>,
    latest_range_intervals_ns: Value,
    largest_body_gap: Value,
    rank: Value,
    rank_unattributed_ns: u64,
    host: Value,
    fetch_to_rank_ns: u64,
    physical_to_logical_ranges: Vec<usize>,
    latest_logical_range_index: usize,
    latest_logical_range_was_split: bool,
    largest_logical_range_bytes: u64,
    largest_physical_range_bytes: u64,
}

fn decompose(raw: &Value, inputs: &Value) -> Result<Decomposition> {
    validate_geometry(raw, inputs)?;
    let physical = physical_ranges(raw, small_pieces(inputs))?;
    let diag = &raw["diagnostic"];
    diagnostic_shape(diag)?;
    if let (Some(start), Some(end)) = (
        nullable(&diag["query_monotonic_start_ns"])?,
        nullable(&diag["query_monotonic_end_ns"])?,
    ) {
        require(
            duration(start, end)? >= number(&raw["query_wall_ns"])?,
            "split query wall anchors",
        )?;
    }
    let host = host(diag, number(&raw["query_process_cpu_ns"])?)?;
    let trace = &diag["sq8_range_trace"];
    fields(
        trace,
        "range_fields outcome planned_ranges planned_bytes max_parallel dropped_ranges fetch_start_ns all_ranges_complete_ns rank_start_ns rank_end_ns release_end_ns rank ranges",
    )?;
    require(
        trace["range_fields"] == json!(RANGE_FIELDS)
            && trace["outcome"] == "ranked"
            && trace["dropped_ranges"] == 0,
        "panel exact trace roster/success",
    )?;
    let ranges = trace["ranges"].as_array().ok_or("panel range rows")?;
    let plan = &physical;
    require(
        !ranges.is_empty()
            && ranges.len() <= 128
            && ranges.len() == plan.len()
            && trace["planned_ranges"] == ranges.len()
            && trace["max_parallel"] == ranges.len().min(64)
            && trace["planned_bytes"] == raw["plan"]["planned_bytes"],
        "panel trace plan totals",
    )?;
    let logical = raw["plan"]["ranges"].as_array().ok_or("logical ranges")?;
    let mut physical_to_logical = Vec::with_capacity(plan.len());
    for piece in plan {
        let matches = logical
            .iter()
            .enumerate()
            .filter(|(_, range)| {
                range[0].as_u64() <= piece[0].as_u64() && range[1].as_u64() >= piece[1].as_u64()
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        require(
            matches.len() == 1,
            "physical span unique logical containment",
        )?;
        physical_to_logical.push(matches[0]);
    }
    let fetch = number(&trace["fetch_start_ns"])?;
    let barrier = number(&trace["all_ranges_complete_ns"])?;
    let page_rows = 32;
    let mut latest = (0, 0);
    let mut largest = (0, 0, 0); // duration, end, index; ties retain the first plan index.
    let mut auth = Vec::with_capacity(ranges.len());
    let mut bytes = 0;
    for (index, row) in ranges.iter().enumerate() {
        let row = row.as_array().ok_or("panel compact range row")?;
        require(row.len() == RANGE_FIELDS.len(), "panel range column count")?;
        let n = |i: usize| number(&row[i]);
        let (start, end) = (n(3)?, n(4)?);
        require(
            n(0)? == index as u64
                && row[21] == "ok"
                && plan[index] == json!([start, end])
                && n(1)? == start / (page_rows * 1036)
                && n(2)? == end.checked_sub(1).ok_or("empty range")? / (page_rows * 1036)
                && n(16)? == duration(start, end)?
                && (1..=u64::from(u32::MAX)).contains(&n(15)?)
                && n(20)? == n(15)?,
            "panel range identity/bytes/chunks",
        )?;
        let mut previous = fetch;
        for column in 5..=14 {
            let stamp = n(column)?;
            duration(previous, stamp)?;
            require(stamp <= barrier, "panel range after barrier")?;
            previous = stamp;
        }
        if index == 0 || n(14)? > latest.0 {
            latest = (n(14)?, index);
        }
        let chunks = n(15)?;
        require(
            chunks != 1 || n(9)? == n(10)?,
            "panel single chunk boundaries",
        )?;
        let gap = n(17)?;
        let gap_end = nullable(&row[18])?;
        let copy = n(19)?;
        let body = duration(n(8)?, n(11)?)?;
        require(
            copy <= body && plus(gap, copy)? <= body,
            "panel copy/body duration",
        )?;
        // After the first arrival, each chunk contributes one copy and one wait, including EOF.
        let covered = chunks
            .checked_mul(gap)
            .and_then(|wait| copy.checked_add(wait))
            .ok_or("panel body gap bound overflow")?;
        require(
            duration(n(9)?, n(11)?)? <= covered,
            "panel body gap coverage",
        )?;
        if gap == 0 {
            require(gap_end.is_none(), "zero body gap boundary")?;
        } else {
            let end = gap_end.ok_or("missing body gap boundary")?;
            require(
                chunks > 2 || end == n(9)? || end == n(11)? || (chunks == 2 && end == n(10)?),
                "panel body gap endpoint",
            )?;
            let start = end.checked_sub(gap).ok_or("body gap underflow")?;
            require(
                start >= n(8)?
                    && end >= n(9)?
                    && end <= n(11)?
                    && (end <= n(10)? || end == n(11)?)
                    && !(start < n(9)? && end > n(9)?)
                    && !(start < n(10)? && end > n(10)?),
                "panel body gap bounds",
            )?;
            if gap > largest.0 {
                largest = (gap, end, index);
            }
        }
        auth.push((n(12)?, n(13)?, index));
        bytes = plus(bytes, n(16)?)?;
    }
    require(
        trace["planned_bytes"] == bytes
            && raw["charges"]["sq8"]
                == json!({"submitted_gets":ranges.len(),"verified_bytes":bytes,"failed_gets":0}),
        "panel trace exact charges",
    )?;
    let boundaries = [
        number(&raw["stages"]["sq8"]["start_ns"])?,
        fetch,
        latest.0,
        barrier,
        number(&trace["rank_start_ns"])?,
        number(&trace["rank_end_ns"])?,
        number(&trace["release_end_ns"])?,
        number(&raw["stages"]["sq8"]["end_ns"])?,
    ];
    let mut intervals = [0; 7];
    for i in 0..7 {
        intervals[i] = duration(boundaries[i], boundaries[i + 1])?;
    }
    let sq8 = duration(boundaries[0], boundaries[7])?;
    require(
        intervals.iter().try_fold(0, |sum, &n| plus(sum, n))? == sq8,
        "exact SQ8 reconciliation",
    )?;
    let rank = &trace["rank"];
    fields(rank, "ranges rows score_ns roster_ns sort_ns finish_ns")?;
    require(
        rank["ranges"]
            == raw["plan"]["ranges"]
                .as_array()
                .ok_or("logical rank ranges")?
                .len()
            && rank["rows"] == bytes / 1036
            && bytes % 1036 == 0,
        "panel rank rows/ranges",
    )?;
    let phases = ["score_ns", "roster_ns", "sort_ns", "finish_ns"]
        .iter()
        .try_fold(0, |sum, key| plus(sum, number(&rank[*key])?))?;
    let rank_unattributed = intervals[4]
        .checked_sub(phases)
        .ok_or("rank subphases exceed rank interval")?;
    let gap_start = largest.1 - largest.0;
    let mut intersections = Vec::new();
    let mut by_range = Vec::new();
    for (start, end, index) in auth {
        let (start, end) = (start.max(gap_start), end.min(largest.1));
        if start < end {
            intersections.push((start, end));
            by_range.push([index as u64, end - start]);
        }
    }
    intersections.sort_unstable();
    let (mut union, mut previous_end) = (0, 0);
    for (start, end) in intersections {
        union = plus(union, end.saturating_sub(start.max(previous_end)))?;
        previous_end = previous_end.max(end);
    }
    require(union <= largest.0, "authentication overlap union")?;
    Ok(Decomposition {
        boundaries_ns: boundaries,
        intervals_ns: intervals,
        sq8_ns: sq8,
        post_barrier_ns: duration(barrier, boundaries[7])?,
        latest_range: ranges[latest.1].as_array().ok_or("latest range")?.clone(),
        latest_range_intervals_ns: {
            let r = &ranges[latest.1];
            json!({"headers":duration(number(&r[6])?,number(&r[7])?)?,
                "body":duration(number(&r[8])?,number(&r[11])?)?,
                "authentication":duration(number(&r[12])?,number(&r[13])?)?,
                "copy":number(&r[19])?})
        },
        largest_body_gap: json!({"range_index":(largest.0>0).then_some(largest.2),"start_ns":(largest.0>0).then_some(gap_start),"end_ns":(largest.0>0).then_some(largest.1),
            "duration_ns":largest.0,"authentication_overlap_union_ns":union,"authentication_overlap_by_range_ns":by_range}),
        rank: rank.clone(),
        rank_unattributed_ns: rank_unattributed,
        host,
        fetch_to_rank_ns: duration(fetch, boundaries[4])?,
        latest_logical_range_index: physical_to_logical[latest.1],
        latest_logical_range_was_split: physical_to_logical
            .iter()
            .filter(|&&index| index == physical_to_logical[latest.1])
            .count()
            > 1,
        physical_to_logical_ranges: physical_to_logical,
        largest_logical_range_bytes: logical
            .iter()
            .map(|r| duration(number(&r[0])?, number(&r[1])?))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max()
            .ok_or("logical maximum")?,
        largest_physical_range_bytes: plan
            .iter()
            .map(|r| duration(number(&r[0])?, number(&r[1])?))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max()
            .ok_or("physical maximum")?,
    })
}

// Retain original bytes rather than 192 expanded JSON trees. Timers remain typed.
#[derive(Serialize)]
struct Observation {
    ordinal: u64,
    query_wall_ns: u64,
    query_process_cpu_ns: u64,
    physical_gets: u64,
    verified_bytes: u64,
    original_jsonl: String,
}

fn paired(
    control: &Decomposition,
    candidate: &Decomposition,
    b: &Observation,
    c: &Observation,
) -> Result<Value> {
    let delta = |a: u64, b: u64| i128::from(a) - i128::from(b);
    let intervals = control
        .intervals_ns
        .iter()
        .zip(candidate.intervals_ns)
        .map(|(&b, c)| delta(b, c))
        .collect::<Vec<_>>();
    Ok(
        json!({"fetch_improvement_ns":intervals[1],"interval_improvements_ns":intervals,
        "net_fetch_to_rank_improvement_ns":delta(control.fetch_to_rank_ns,candidate.fetch_to_rank_ns),
        "sq8_improvement_ns":delta(control.sq8_ns,candidate.sq8_ns),
        "post_barrier_improvement_ns":delta(control.post_barrier_ns,candidate.post_barrier_ns),
        "query_wall_improvement_ns":delta(b.query_wall_ns,c.query_wall_ns),
        "query_cpu_improvement_ns":delta(b.query_process_cpu_ns,c.query_process_cpu_ns),
        "physical_get_increase":i128::from(c.physical_gets)-i128::from(b.physical_gets) }),
    )
}

fn screen(deltas: [[i128; 2]; 2]) -> &'static str {
    if deltas
        .iter()
        .all(|pair| pair[0] >= 12_100_000 && pair[1] >= 15_500_000)
    {
        "STRONG_TARGETED_SCREEN"
    } else if deltas
        .iter()
        .all(|pair| pair[0] < 6_000_000 && pair[1] < 7_700_000)
    {
        "SMALL_EFFECT_SCREEN"
    } else {
        "INCONCLUSIVE"
    }
}

pub(super) fn reduce(path: &Path, sha: &str) -> Result<Value> {
    let file = super::open_input(path)?;
    let original = super::file_identity(&file)?;
    let (c, config_bytes): (Config, _) = super::read_config_file(&file, path, sha, CONFIG_CAP)?;
    let config_snapshot = super::HeaderSnapshot {
        file,
        path: path.to_owned(),
        identity: original,
    };
    require(c.schema == CONFIG_SCHEMA, "split panel config schema")?;
    let mut config_snapshots = Vec::with_capacity(6);
    let mut runtime_base = None;
    // All independent descriptors and source/option pins are admitted before result content.
    for (slot, d) in c.runs.iter().enumerate() {
        require(d.label == LABELS[slot], "split panel six-run order")?;
        validate_completed(&d.completed)?;
        let i = &d.completed.expected_identity;
        let b = &d.completed.expected_bound_inputs;
        require(
            b["sq8_range_split"] == split_value(candidate(slot))
                && b["execution"]["trace"] == (slot >= 2),
            "split arm/trace order",
        )?;
        require(
            neutral_header(i.clone(), true)
                == neutral_header(c.runs[0].completed.expected_identity.clone(), true)
                && neutral_header(b.clone(), false)
                    == neutral_header(c.runs[0].completed.expected_bound_inputs.clone(), false),
            "same binary/headers/authority except split and trace",
        )?;
        for previous in &c.runs[..slot] {
            require(
                d.completed.input.path != previous.completed.input.path
                    && d.completed.input.sha256 != previous.completed.input.sha256,
                "distinct original result files",
            )?;
        }
        let (mut runtime, snapshot) = runtime_config(d)?;
        runtime
            .as_object_mut()
            .ok_or("runtime object")?
            .remove("sq8_range_split");
        runtime["execution"]["trace"] = json!(false);
        if let Some(base) = &runtime_base {
            require(base == &runtime, "same runtime config except split/trace")?;
        } else {
            runtime_base = Some(runtime);
        }
        config_snapshots.push(snapshot);
    }
    let mut runs: Vec<super::Run> = Vec::with_capacity(6);
    let mut observations = Vec::with_capacity(6);
    let mut decompositions = Vec::with_capacity(4);
    let mut retained = 0;
    for (slot, d) in c.runs.iter().enumerate() {
        let mut queries = Vec::with_capacity(32);
        let mut traces = Vec::with_capacity(if slot >= 2 { 32 } else { 0 });
        let run = super::read_run_observed_policy(
            &d.completed.input.path,
            &d.completed.input.sha256,
            Some(&d.completed),
            None,
            super::ReadPolicy::Sq8SplitPanel,
            |line, _, _| {
                retained = plus(retained, line.len() as u64)?;
                require(
                    retained <= RETAINED_CAP && queries.len() < 32,
                    "split retained observations cap",
                )?;
                let super::UniqueJson(raw) = serde_json::from_slice(line)?;
                require(
                    raw["sum"]["failed_gets"] == 0
                        && number(&raw["query_process_cpu_ns"])? <= i64::MAX as u64,
                    "split successful full-drain query/bounded CPU delta",
                )?;
                if slot >= 2 {
                    traces.push(decompose(&raw, &d.completed.expected_bound_inputs)?);
                }
                queries.push(Observation {
                    ordinal: number(&raw["ordinal"])?,
                    query_wall_ns: number(&raw["query_wall_ns"])?,
                    query_process_cpu_ns: number(&raw["query_process_cpu_ns"])?,
                    physical_gets: number(&raw["charges"]["sq8"]["submitted_gets"])?,
                    verified_bytes: number(&raw["charges"]["sq8"]["verified_bytes"])?,
                    original_jsonl: std::str::from_utf8(line)?.to_owned(),
                });
                Ok(())
            },
        )?;
        require(
            run.samples.len() == 32 && run.terminal.underfilled_queries == 0,
            "split completed full-k",
        )?;
        for previous in &runs {
            require(
                (run.file_identity.dev, run.file_identity.ino)
                    != (previous.file_identity.dev, previous.file_identity.ino),
                "split duplicate result inode",
            )?;
        }
        if let Some(base) = runs.first() {
            for (b, q) in base.samples.iter().zip(&run.samples) {
                require(
                    b.returned == q.returned
                        && b.hits10 == q.hits10
                        && b.trace_sha256 == q.trace_sha256
                        && b.physical_plan_sha256 == q.physical_plan_sha256
                        && b.charges.router == q.charges.router
                        && b.charges.source == q.charges.source
                        && b.charges.sq8.verified_bytes == q.charges.sq8.verified_bytes,
                    "split cross-arm ordered IDs/scorebits/plan/trace/bytes/recall parity",
                )?;
            }
        }
        if slot >= 2 {
            let base = usize::from(candidate(slot));
            for (off, on) in runs[base].samples.iter().zip(&run.samples) {
                require(
                    off.charges == on.charges,
                    "split within-arm trace-on/off physical charge parity",
                )?;
            }
            decompositions.push(traces);
        }
        observations.push(queries);
        runs.push(run);
    }
    let mut primary = [[0; 2]; 2];
    let mut primary_net = [[0; 2]; 2];
    let mut queries = Vec::with_capacity(32);
    for (index, ordinal) in ORDINALS.into_iter().enumerate() {
        let pair1 = paired(
            &decompositions[0][index],
            &decompositions[1][index],
            &observations[2][index],
            &observations[3][index],
        )?;
        let pair2 = paired(
            &decompositions[3][index],
            &decompositions[2][index],
            &observations[5][index],
            &observations[4][index],
        )?;
        if let Some(primary_slot) = [483, 773].iter().position(|&o| o == ordinal) {
            primary[0][primary_slot] = pair1["fetch_improvement_ns"]
                .as_i64()
                .ok_or("split signed delta")? as i128;
            primary[1][primary_slot] = pair2["fetch_improvement_ns"]
                .as_i64()
                .ok_or("split signed delta")? as i128;
            primary_net[0][primary_slot] = pair1["net_fetch_to_rank_improvement_ns"]
                .as_i64()
                .ok_or("split signed net delta")?
                as i128;
            primary_net[1][primary_slot] = pair2["net_fetch_to_rank_improvement_ns"]
                .as_i64()
                .ok_or("split signed net delta")?
                as i128;
        }
        let perturbation = [(0, 2), (0, 5), (1, 3), (1, 4)].map(|(off, on)| json!({
            "off":LABELS[off],"on":LABELS[on],
            "query_wall_delta_ns":i128::from(runs[on].samples[index].wall_ns)-i128::from(runs[off].samples[index].wall_ns),
            "sq8_delta_ns":i128::from(runs[on].completed.as_ref().expect("completed").stage_samples[3][index])
                -i128::from(runs[off].completed.as_ref().expect("completed").stage_samples[3][index]) }));
        queries.push(
            json!({"ordinal":ordinal,"B1_minus_C1":pair1,"B2_minus_C2":pair2,
            "trace_on_off_perturbation_descriptive":perturbation}),
        );
    }
    // Callbacks are provisional. Every original file must still be the sealed inode at EOF.
    for (d, r) in c.runs.iter().zip(&runs) {
        require(
            super::file_identity(&super::open_input(&d.completed.input.path)?)? == r.file_identity,
            "split original changed after authentication",
        )?;
    }
    config_snapshot.recheck()?;
    for snapshot in config_snapshots {
        snapshot.recheck()?;
    }
    Ok(
        json!({"schema":REPORT_SCHEMA,"status":"VALIDATED_FIXED_PANEL","complete":true,
        "screen":screen(primary),"screen_scope":"FETCH_ONLY_TARGETED_SCREEN_NOT_NET_PERFORMANCE",
        "primary_fetch_improvements_ns":primary,"primary_net_fetch_to_rank_improvements_ns":primary_net,
        "net_performance_screen":"EXTERNAL_ROOT_GATE_REQUIRED",
        "reduction_config_sha256":sha,"reduction_config_bytes":config_bytes,
        "diagnostic_only":true,"selected_count":32,"population_percentiles_valid":false,
        "population_p95_claim":false,"qps_claim":false,"causal_claim":false,"vendor_or_scientific_win_claim":false,
        "producer_provenance_qualified":false,"historical_disabled_B_parity_qualified":false,
        "scored_row_roster_parity_qualified":false,"physical_ordinal_parity_qualified":false,
        "recorded_logical_coverage_parity":true,"recorded_ordered_id_scorebit_parity":true,
        "valid_tradeoffs_retained_independent_of_screen":true,
        "range_fields":RANGE_FIELDS,"host_fields":HOST_FIELDS.as_slice(),
        "boundary_fields":["stage_start","fetch_start","latest_range_complete","all_ranges_barrier","rank_start","rank_end","release_end","stage_end"],
        "interval_fields":["pre_fetch","fetch_to_latest_complete","latest_complete_to_barrier","barrier_to_rank_including_reassembly","rank","release","stage_tail"],
        "proof_gaps":["No stored scored-row roster or returned physical ordinals; logical byte coverage and rank row totals do not prove scored-row identity. Root native parity falsifier required.",
            "No reassembly-only timer: barrier_to_rank includes reassembly and bookkeeping. Per-range copy_ns measures incoming body copies only. Fetch-only screen is not a net-performance screen; root must freeze net acceptance gates.",
            "Root must verify executable/source provenance, original native exit/resource/cache/cost/order receipts and disabled-path historical B compatibility."],
        "semantics":"Targeted previously accessed selected panel, noncausal. S3 server cache uncontrolled. Timings overlap; only boundary chain reconciles stage time. Headers combine client queue/connect/TLS/server. No population p95/QPS or vendor claim; retain every valid gain and its GET/CPU/RSS costs even if the screen fails.",
        "runs":c.runs.iter().zip(&runs).enumerate().map(|(slot,(d,r))| json!({
            "label":d.label,"input":d.completed.input,"runtime_config":d.runtime_config,
            "identity":d.completed.expected_identity,"bound_inputs":d.completed.expected_bound_inputs,
            "native_terminal":r.completed.as_ref().expect("completed").terminal,
            "original_queries":observations[slot],
            "decomposition":if slot>=2 {json!(decompositions[slot-2])} else {Value::Null}
        })).collect::<Vec<_>>(),"queries":queries}),
    )
}

#[cfg(test)]
mod tests {
    use super::super::tests::{authenticate, encode, paired_scale_fixture};
    use super::*;
    use sha2::{Digest, Sha256};
    fn hash(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
    fn query(split: bool) -> Value {
        let page = 32;
        let bytes = page * 1036;
        let mut deltas = serde_json::Map::new();
        for key in HOST_FIELDS {
            deltas.insert(key.into(), Value::Null);
        }
        deltas.insert("Tcp.RetransSegs".into(), json!(0));
        let mut plan = json!({"selected_pages":[0,2],"ranges":[[0,bytes],[2*bytes,3*bytes]],
            "planned_bytes":2*bytes,"target_pages":130,
            "target_shortfall":0,"primary_pages_retained":0,
            "covered_pages":2,"bridge_pages":0});
        {
            plan["page_rows"] = json!(32);
        }
        let mut raw = json!({"query_process_cpu_ns":1,"plan":plan,"charges":{"sq8":{"submitted_gets":2,"verified_bytes":2*bytes,"failed_gets":0}},
        "trace":{"primary_page":0},"stages":{"sq8":{"start_ns":100,"end_ns":550}},
        "diagnostic":{"schema":"borsuk-sq8-range-attribution-diagnostic-v2",
            "query_monotonic_start_ns":1000,"query_monotonic_end_ns":2000,
            "process_cpu_start_ns":100,"process_cpu_end_ns":101,
            "host":{"scope":"host_wide_counter_deltas_not_per_flow_or_per_s3_connection",
                "window":"between_untimed_snapshots_that_bracket_the_query_and_include_runner_bookkeeping",
                "before_monotonic_ns":900,"after_monotonic_ns":2100,"unavailable":47,"deltas":deltas},
            "sq8_range_trace":{"range_fields":RANGE_FIELDS,"outcome":"ranked","planned_ranges":2,
                "planned_bytes":2*bytes,"max_parallel":2,"dropped_ranges":0,"fetch_start_ns":110,
                "all_ranges_complete_ns":400,"rank_start_ns":420,"rank_end_ns":520,"release_end_ns":540,
                "rank":{"ranges":2,"rows":2*page,"score_ns":10,"roster_ns":20,"sort_ns":30,"finish_ns":10},
                "ranges":[
                    [0,0,0,0,bytes,120,130,140,150,160,300,310,320,350,360,2,bytes,140,300,10,2,"ok"],
                    [1,2,2,2*bytes,3*bytes,121,131,141,151,161,180,190,200,240,250,2,bytes,19,180,10,2,"ok"]
                ]}}});
        raw["query_wall_ns"] = json!(600);
        raw["plan"] = json!({"page_rows":32,"selected_pages":(0..130).collect::<Vec<_>>(),
            "ranges":[[0,130*bytes]],"planned_bytes":130*bytes,"target_pages":130,"target_shortfall":0,
            "primary_pages_retained":0,"covered_pages":130,"bridge_pages":0});
        let physical = physical_ranges(&raw, split).unwrap();
        raw["charges"]["sq8"] =
            json!({"submitted_gets":physical.len(),"verified_bytes":130*bytes,"failed_gets":0});
        let template = raw["diagnostic"]["sq8_range_trace"]["ranges"][0].clone();
        let rows = physical
            .iter()
            .enumerate()
            .map(|(index, pair)| {
                let mut row = template.clone();
                let start = pair[0].as_u64().unwrap();
                let end = pair[1].as_u64().unwrap();
                for (column, value) in [
                    (0, index as u64),
                    (1, start / BLOCK_BYTES),
                    (2, (end - 1) / BLOCK_BYTES),
                    (3, start),
                    (4, end),
                    (16, end - start),
                ] {
                    row[column] = json!(value);
                }
                row
            })
            .collect::<Vec<_>>();
        let trace = &mut raw["diagnostic"]["sq8_range_trace"];
        trace["ranges"] = json!(rows);
        trace["planned_ranges"] = json!(physical.len());
        trace["max_parallel"] = json!(physical.len());
        trace["planned_bytes"] = json!(130 * bytes);
        trace["rank"]["ranges"] = json!(1);
        trace["rank"]["rows"] = json!(130 * 32);
        raw
    }

    fn fixture(slot: usize) -> Vec<Value> {
        let split = candidate(slot);
        let traced = slot >= 2;

        let full = paired_scale_fixture(64);
        let execution = json!({"mode":"diagnostic_panel","ordinals":ORDINALS,"trace":traced});
        let mut rows = full[..2].to_vec();
        rows[0]["schema"] = json!(RESULT_SCHEMA);
        rows[0]["binary_sha256"] = json!("b".repeat(64));
        rows[0]["config_sha256"] = json!(if traced { "4" } else { "5" }.repeat(64));
        for (key, pin) in COMPONENTS.iter().zip(SOURCE_PINS) {
            rows[0][*key] = json!(pin);
        }
        for header in &mut rows[..2] {
            header["execution"] = execution.clone();
            header["sq8_range_split"] = split_value(split);
            header["source_get_budget"] = json!(128);
            header["sq8_get_budget"] = json!(30);
            header["sq8_byte_budget"] = json!(33554432);
        }
        rows[1]["selected_count"] = json!(32);
        use borsuk::sq8_s3_range::Sq8RangeTrace;
        let ranges = 46;
        let peak = if traced {
            Sq8RangeTrace::modeled_bytes(ranges, 64)
        } else {
            0
        };
        let both = Sq8RangeTrace::pending_futures_bytes(128, 64);
        let pinned = both
            - if traced {
                Sq8RangeTrace::pending_futures_bytes(ranges, 64)
            } else {
                0
            };
        let diagnostic_pinned = if traced { 534097 } else { 131072 };
        rows.push(json!({"phase":"diagnostic_admission","execution":execution,
            "selected_count":32,"population_count":1000,"trace":traced,"truth_opened":false,
            "population_percentiles_valid":false,"full_cohort_qualification":false,
            "diagnostic_bytes_per_active_query":diagnostic_pinned+peak,"diagnostic_cap_bytes":1048576,
            "max_active_queries":1,"charged_in_caller_pinned_bytes":true,"caller_pinned_bytes":2097152,
            "panel_line_cap_bytes":65536,"host_read_cap_bytes":131072,"trace_ranges":ranges,
            "trace_retained_bytes":if traced {json!(Sq8RangeTrace::retained_bytes(ranges))} else {Value::Null},
            "trace_peak_bytes":if traced {json!(peak)} else {Value::Null},
            "diagnostic_pinned_bytes":diagnostic_pinned,"trace_peak_charged_by_library_at_traced_admission":traced,
            "range_state_pinned_by_runner_bytes":pinned,"range_state_both_paths_bytes":both,
            "semantics":{"headers_interval":"get_opts_client_queue_connect_tls_server_combined",
                "body_gaps":"application_observed_poll_gaps_not_packet_arrival",
                "ranges":"overlap_in_time_never_sum_intervals","host_counters":"host_wide_not_per_flow_null_means_unavailable",
                "unset":"a boundary never reached is null, never a fabricated timestamp"}}));
        rows.extend_from_slice(&full[2..5]);
        rows[5]["caller_pinned_bytes"] = rows[2]["caller_pinned_bytes"].clone();
        let example = query(split);
        let gets = example["charges"]["sq8"]["submitted_gets"]
            .as_u64()
            .unwrap();
        let sq8_bytes = example["plan"]["planned_bytes"].as_u64().unwrap();
        let total = sq8_bytes + 8448 + 100;
        for (selected_slot, ordinal) in ORDINALS.into_iter().enumerate() {
            let mut q = full[5 + ordinal].clone();
            q["selected_slot"] = json!(selected_slot);
            q["plan"] = example["plan"].clone();
            q["charges"]["sq8"] = example["charges"]["sq8"].clone();
            q["charges"]["source"]["verified_bytes"] = json!(8448);
            q["sum"] = json!({"submitted_gets":gets+2,"verified_bytes":total,"failed_gets":0});
            q["stages"] = json!({"discovery":{"start_ns":1,"end_ns":2},"source":{"start_ns":2,"end_ns":3},
                "planning":{"start_ns":3,"end_ns":100},"sq8":{"start_ns":100,"end_ns":550},"leaf_peak_inflight":2});
            q["query_wall_ns"] = json!(600);
            q["query_process_cpu_ns"] = json!(1);
            q["trace"] = json!({"ranked_candidate_pages":[0,2],"primary_page":0,"nomination_evaluated_units":[0,16],
                "discoveries":[],"semantic_leaves":[0],"semantic_units":[0,16],"semantic_seed_additions":[]});
            if traced {
                q["diagnostic"] = example["diagnostic"].clone();
            }
            for (side, n) in [
                ("before", selected_slot as u64),
                ("after", selected_slot as u64 + 1),
            ] {
                let attempts = 5 + (gets + 2) * n;
                let snapshot = &mut q["transport"][side];
                snapshot["attempts"] = json!(attempts);
                snapshot["method_counts"] = json!([attempts - 1, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
                snapshot["status_counts"] = json!([[200, attempts]]);
                snapshot["consumed_payload_bytes"] = json!(500 + n * total);
            }
            rows.push(q);
        }
        let mut seal = full
            .iter()
            .find(|r| r["phase"] == "all_queries_sealed")
            .unwrap()
            .clone();
        seal["count"] = json!(32);
        seal["selected_count"] = json!(32);
        rows.push(seal);
        for ordinal in ORDINALS {
            rows.push(
                full.iter()
                    .find(|r| r["phase"] == "recall" && r["ordinal"] == ordinal)
                    .unwrap()
                    .clone(),
            );
        }
        let span: super::super::TransportSpan =
            serde_json::from_value(rows[37]["transport"].clone()).unwrap();
        let mut terminal = full.last().unwrap().clone();
        let s = &mut terminal["summary"];
        for key in ["queries", "selected_count", "executed_count"] {
            s[key] = json!(32);
        }
        for key in ["total_hits10", "recall_numerator"] {
            s[key] = json!(288);
        }
        s["recall_denominator"] = json!(320);
        s["execution"] = execution;
        s["diagnostic_panel"] = json!(true);
        s["scope_note"] = json!(SCOPE_NOTE);
        s["population_percentiles_valid"] = json!(false);
        s["query_wall_ns"] = json!(32 * 600);
        s["process_wall_ns"] = json!(32 * 600 + 100);
        s["query_process_cpu_ns"] = json!(32);
        s["process_cpu_ns"] = json!(132 + slot);
        s["charges"] = json!({"router":{"submitted_gets":32,"verified_bytes":3200,"failed_gets":0},
            "source":{"submitted_gets":32,"verified_bytes":32*8448,"failed_gets":0},
            "sq8":{"submitted_gets":32*gets,"verified_bytes":32*sq8_bytes,"failed_gets":0}});
        s["sum"] = json!({"submitted_gets":32*(gets+2),"verified_bytes":32*total,"failed_gets":0});
        s["transport_last_boundary"] = span.compact();
        rows.push(terminal);
        authenticate(&mut rows);
        rows
    }

    fn runtime(b: &Value) -> Value {
        let mut raw = b.clone();
        for key in [
            "phase",
            "source_cache",
            "selected_count",
            "reserved_queries_sha256",
            "truth_opened",
            "credential_source",
            "requests_bytes",
            "requests_sha256",
            "truth_bytes",
            "truth_sha256",
            "cohort_receipt_sha256",
            "derivation_receipt_sha256",
            "native_source_sha256",
            "native_sq8_sha256",
            "native_order_sha256",
        ] {
            raw.as_object_mut().unwrap().remove(key);
        }
        raw["schema"] = json!("borsuk-cohere-native-baseline-config-v10");
        raw["scratch_parent"] = json!("/tmp/split-panel-fixture");
        for key in ["requests", "truth", "cohort_receipt", "derivation_receipt"] {
            raw[key] = json!({"path":format!("/not-read/{key}"),
                "bytes":b.get(format!("{key}_bytes")).cloned().unwrap_or(json!(1)),
                "sha256":b[format!("{key}_sha256")]});
        }
        raw["native_source"] = json!({"source_sha256":b["native_source_sha256"],"sq8_sha256":b["native_sq8_sha256"],
            "source_order_sha256":b["native_order_sha256"]});
        raw
    }

    fn save(dir: &Path, name: &str, bytes: &[u8]) -> CompletedInput {
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        CompletedInput {
            path,
            bytes: bytes.len() as u64,
            sha256: hash(bytes),
        }
    }
    fn descriptor(dir: &Path, slot: usize, mut rows: Vec<Value>) -> Descriptor {
        let runtime_config = save(
            dir,
            &format!("runtime-{slot}"),
            &serde_json::to_vec(&runtime(&rows[1])).unwrap(),
        );
        rows[0]["config_sha256"] = json!(runtime_config.sha256);
        authenticate(&mut rows);
        Descriptor {
            label: LABELS[slot].into(),
            runtime_config,
            completed: CompletedConfig {
                schema: super::super::SCALE_CONFIG_SCHEMA.into(),
                input: save(dir, LABELS[slot], &encode(&rows)),
                expected_identity: rows[0].clone(),
                expected_bound_inputs: rows[1].clone(),
            },
        }
    }
    fn config(
        dir: &Path,
        changed: impl Fn(usize, &mut Vec<Value>),
    ) -> (std::path::PathBuf, String) {
        let runs = (0..6)
            .map(|slot| {
                let mut rows = fixture(slot);
                changed(slot, &mut rows);
                let d = descriptor(dir, slot, rows);
                json!({"label":d.label,"completed":d.completed,"runtime_config":d.runtime_config})
            })
            .collect::<Vec<_>>();
        let pin = save(
            dir,
            "config",
            &serde_json::to_vec(&json!({"schema":CONFIG_SCHEMA,"runs":runs})).unwrap(),
        );
        (pin.path, pin.sha256)
    }
    fn read(
        c: &CompletedConfig,
        callback: impl FnMut(&[u8], &super::super::Query, &super::super::Inputs) -> Result<()>,
    ) -> Result<super::super::Run> {
        super::super::read_run_observed_policy(
            &c.input.path,
            &c.input.sha256,
            Some(c),
            None,
            super::super::ReadPolicy::Sq8SplitPanel,
            callback,
        )
    }

    #[test]
    fn sq8_split_panel_sealed_six_runs_and_screen() {
        let dir = tempfile::tempdir().unwrap();
        let (path, sha) = config(dir.path(), |_, _| {});
        let report = reduce(&path, &sha).unwrap();
        assert_eq!(report["status"], "VALIDATED_FIXED_PANEL");
        assert_eq!(report["screen"], "SMALL_EFFECT_SCREEN");
        assert_eq!(report["queries"].as_array().unwrap().len(), 32);
        assert_eq!(report["runs"][3]["original_queries"][0]["physical_gets"], 3);
        assert_eq!(report["runs"][3]["decomposition"][0]["rank"]["ranges"], 1);
        assert_eq!(
            report["runs"][3]["decomposition"][0]["intervals_ns"],
            json!([10, 250, 40, 20, 100, 20, 10])
        );
        assert_eq!(report["scored_row_roster_parity_qualified"], false);
        assert_eq!(
            report["runs"][3]["decomposition"][0]["physical_to_logical_ranges"],
            json!([0, 0, 0])
        );
        assert_eq!(
            report["runs"][3]["decomposition"][0]["latest_logical_range_was_split"],
            true
        );
        assert_eq!(
            report["runs"][2]["decomposition"][0]["latest_logical_range_was_split"],
            false
        );
        assert_eq!(
            report["runs"][3]["decomposition"][0]["largest_physical_range_bytes"],
            SPLIT_BYTES
        );
        assert_eq!(
            report["primary_net_fetch_to_rank_improvements_ns"],
            json!([[0, 0], [0, 0]])
        );
        let original: Value = serde_json::from_str(
            report["runs"][3]["original_queries"][0]["original_jsonl"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            original["diagnostic"]["sq8_range_trace"]["ranges"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        assert!(
            original["diagnostic"]["sq8_range_trace"]["ranges"][0][19]
                .as_u64()
                .is_some()
        );
        let inputs = json!({"sq8_range_split":split_value(true)});
        let mut b = decompose(
            &query(false),
            &json!({"sq8_range_split":split_value(false)}),
        )
        .unwrap();
        let c = decompose(&query(true), &inputs).unwrap();
        b.intervals_ns[1] += 12_100_000;
        b.fetch_to_rank_ns += 12_100_000;
        let observation = Observation {
            ordinal: 483,
            query_wall_ns: 600,
            query_process_cpu_ns: 1,
            physical_gets: 3,
            verified_bytes: 130 * BLOCK_BYTES,
            original_jsonl: String::new(),
        };
        assert_eq!(
            paired(&b, &c, &observation, &observation).unwrap()["fetch_improvement_ns"],
            12_100_000
        );
        assert_eq!(
            screen([[12_100_000, 15_500_000]; 2]),
            "STRONG_TARGETED_SCREEN"
        );
        assert_eq!(screen([[5_999_999, 7_699_999]; 2]), "SMALL_EFFECT_SCREEN");
        for values in [
            [[12_099_999, 15_500_000], [12_100_000, 15_500_000]],
            [[6_000_000, 0], [0, 0]],
            [[0, 7_700_000], [0, 0]],
        ] {
            assert_eq!(screen(values), "INCONCLUSIVE");
        }
        // Historical readers cannot silently accept this opt-in protocol.
        let d = descriptor(dir.path(), 3, fixture(3));
        assert!(
            super::super::read_run_with(
                &d.completed.input.path,
                &d.completed.input.sha256,
                Some(&d.completed)
            )
            .is_err()
        );
        assert!(super::super::sq8_panel_attribution::validate_completed(&d.completed).is_err());
    }

    #[test]
    fn sq8_split_panel_coherently_resealed_negatives() {
        for (pointer, value) in [
            ("/diagnostic/sq8_range_trace/ranges/0/21", json!("error")),
            ("/diagnostic/sq8_range_trace/ranges/1/0", json!(0)),
            ("/diagnostic/sq8_range_trace/ranges/1/3", json!(0)),
            ("/diagnostic/sq8_range_trace/ranges/1/16", json!(1)),
            ("/diagnostic/sq8_range_trace/dropped_ranges", json!(1)),
            ("/diagnostic/sq8_range_trace/planned_ranges", json!(2)),
            ("/diagnostic/sq8_range_trace/rank/rows", json!(1)),
            ("/sum/submitted_gets", json!(99)),
            ("/returned/9/score_bits", json!(0x40400001_u32)),
            ("/returned/9/id", json!(11)),
            ("/trace/ranked_candidate_pages/0", json!(1)),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (path, sha) = config(dir.path(), |slot, rows| {
                if slot == 3 {
                    *rows[6].pointer_mut(pointer).unwrap() = value.clone();
                }
            });
            assert!(reduce(&path, &sha).is_err(), "accepted {pointer}");
        }
        for failure in 0..6 {
            let dir = tempfile::tempdir().unwrap();
            let (path, sha) = config(dir.path(), |slot, rows| {
                if slot == 3 {
                    match failure {
                        0 => {
                            rows.remove(6);
                        }
                        1 => {
                            rows[7] = rows[6].clone();
                        }
                        2 => {
                            rows[6]["diagnostic"]["sq8_range_trace"]["ranges"]
                                .as_array_mut()
                                .unwrap()
                                .pop();
                        }
                        3 => {
                            rows.last_mut().unwrap()["summary"]["charges"]["sq8"]["verified_bytes"] =
                                json!(0)
                        }
                        4 => {
                            rows.last_mut().unwrap()["summary"]["population_percentiles_valid"] =
                                json!(true)
                        }
                        _ => rows[6]["unexpected"] = json!(true),
                    }
                }
            });
            assert!(
                reduce(&path, &sha).is_err(),
                "accepted resealed failure {failure}"
            );
        }
    }

    #[test]
    fn sq8_split_panel_late_auth_tail_growth_and_source_pins() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let d = descriptor(dir.path(), 3, fixture(3));
        let mut wrong: CompletedConfig =
            serde_json::from_value(serde_json::to_value(&d.completed).unwrap()).unwrap();
        wrong.input.sha256 = "0".repeat(64);
        let mut observed = 0;
        assert!(
            read(&wrong, |_, _, _| {
                observed += 1;
                Ok(())
            })
            .is_err()
        );
        assert_eq!(observed, 32); // Valid observations never establish late acceptance.
        let mut grown = false;
        assert!(
            read(&d.completed, |_, _, _| {
                if !grown {
                    std::fs::OpenOptions::new()
                        .append(true)
                        .open(&d.completed.input.path)?
                        .write_all(b"\n")?;
                    grown = true;
                }
                Ok(())
            })
            .is_err()
        );
        for key in COMPONENTS {
            let mut rows = fixture(3);
            rows[0][key] = json!("0".repeat(64));
            let d = descriptor(dir.path(), 3, rows);
            assert!(
                read(&d.completed, |_, _, _| Ok(())).is_err(),
                "source {key}"
            );
        }
        let (path, sha) = config(dir.path(), |_, _| {});
        let body = std::fs::read(&path).unwrap();
        let mut cfg: Value = serde_json::from_slice(&body).unwrap();
        let runtime_path = cfg["runs"][3]["runtime_config"]["path"].as_str().unwrap();
        std::fs::write(runtime_path, b"{}").unwrap();
        assert!(reduce(&path, &sha).is_err());
        cfg["unknown"] = json!(true);
        let pin = save(dir.path(), "bad-config", &serde_json::to_vec(&cfg).unwrap());
        assert!(reduce(&pin.path, &pin.sha256).is_err());
    }

    #[test]
    fn sq8_split_panel_physical_caps_options_and_trace_parity() {
        let mut q = query(true);
        let inputs = json!({"sq8_range_split":split_value(true)});
        validate_geometry(&q, &inputs).unwrap();
        // A logical gap cover can exceed physical46 without exceeding logical30/32MiB.
        q["plan"]["ranges"] = json!(
            (0..30)
                .map(|i| [i * 70 * BLOCK_BYTES, (i * 70 + 65) * BLOCK_BYTES])
                .collect::<Vec<_>>()
        );
        assert!(physical_ranges(&q, true).is_err());
        for value in [
            Value::Null,
            json!({}),
            json!({"max_physical_range_bytes":0,"max_physical_gets":46}),
            json!({"max_physical_range_bytes":SPLIT_BYTES,"max_physical_gets":47}),
            json!({"max_physical_range_bytes":SPLIT_BYTES,"max_physical_gets":46,"extra":1}),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (path, sha) = config(dir.path(), |slot, rows| {
                if slot == 3 {
                    for header in &mut rows[..2] {
                        header["sq8_range_split"] = value.clone();
                    }
                }
            });
            assert!(reduce(&path, &sha).is_err());
        }
        for failure in 0..5 {
            let dir = tempfile::tempdir().unwrap();
            let (path, sha) = config(dir.path(), |slot, rows| {
                if slot == 3 {
                    match failure {
                        0 => {
                            rows[0].as_object_mut().unwrap().remove("sq8_range_split");
                        }
                        1 => {
                            rows[6].as_object_mut().unwrap().remove("diagnostic");
                        }
                        2 => rows[1]["execution"]["trace"] = json!(false),
                        3 => rows[6]["plan"]["selected_pages"][0] = json!(1),
                        _ => rows[6]["charges"]["source"]["verified_bytes"] = json!(0),
                    }
                }
            });
            assert!(reduce(&path, &sha).is_err());
        }
    }

    #[test]
    fn sq8_split_panel_stage_clock_and_conservation() {
        let inputs = json!({"sq8_range_split":split_value(true)});
        for (pointer, value) in [
            ("/diagnostic/sq8_range_trace/ranges/0/7", json!(100)),
            (
                "/diagnostic/sq8_range_trace/all_ranges_complete_ns",
                json!(359),
            ),
            ("/diagnostic/sq8_range_trace/rank_start_ns", json!(399)),
            ("/diagnostic/sq8_range_trace/rank_end_ns", json!(600)),
            ("/diagnostic/sq8_range_trace/rank/score_ns", json!(101)),
            ("/diagnostic/sq8_range_trace/ranges/0/19", json!(1000)),
            ("/diagnostic/sq8_range_trace/ranges/0/20", json!(3)),
            ("/diagnostic/process_cpu_end_ns", json!(102)),
            ("/diagnostic/query_monotonic_end_ns", json!(1500)),
            ("/diagnostic/host/after_monotonic_ns", json!(1999)),
        ] {
            let mut q = query(true);
            *q.pointer_mut(pointer).unwrap() = value;
            assert!(decompose(&q, &inputs).is_err(), "accepted {pointer}");
        }
        let dir = tempfile::tempdir().unwrap();
        for pointer in [
            "/trace_ranges",
            "/trace_peak_bytes",
            "/diagnostic_pinned_bytes",
            "/caller_pinned_bytes",
            "/range_state_pinned_by_runner_bytes",
            "/range_state_both_paths_bytes",
        ] {
            let (path, sha) = config(dir.path(), |slot, rows| {
                if slot == 3 {
                    *rows[2].pointer_mut(pointer).unwrap() = json!(0);
                }
            });
            assert!(reduce(&path, &sha).is_err(), "admission {pointer}");
        }
    }

    #[test]
    fn sq8_split_panel_output_collision_and_sync_errors() {
        let dir = tempfile::tempdir().unwrap();
        let (path, sha) = config(dir.path(), |_, _| {});
        let output = dir.path().join("report");
        assert!(
            super::super::execute_report(&output, REPORT_SCHEMA, || reduce(&path, &sha)).unwrap()
        );
        let original = std::fs::read(&output).unwrap();
        assert!(
            super::super::execute_report(&output, REPORT_SCHEMA, || panic!(
                "collision ran reducer"
            ))
            .is_err()
        );
        assert_eq!(std::fs::read(&output).unwrap(), original);
        for fail_dir in [false, true] {
            let output = dir.path().join(format!("sync-{fail_dir}"));
            let mut calls = Vec::new();
            assert!(
                super::super::execute_report_with_sync(
                    &output,
                    REPORT_SCHEMA,
                    || reduce(&path, &sha),
                    |_, directory| {
                        calls.push(directory);
                        if directory == fail_dir {
                            Err(std::io::Error::other("injected sync failure"))
                        } else {
                            Ok(())
                        }
                    }
                )
                .is_err()
            );
            assert_eq!(
                calls,
                if fail_dir {
                    vec![false, true]
                } else {
                    vec![false]
                }
            );
        }
        let mut bad: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        bad["runs"][5]["completed"]["input"]["sha256"] = json!("0".repeat(64));
        let bad = save(
            dir.path(),
            "late-auth-config",
            &serde_json::to_vec(&bad).unwrap(),
        );
        let output = dir.path().join("invalid");
        assert!(
            !super::super::execute_report(&output, REPORT_SCHEMA, || reduce(
                &bad.path,
                &bad.sha256
            ))
            .unwrap()
        );
        let invalid: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(invalid["complete"], false);
    }
}
