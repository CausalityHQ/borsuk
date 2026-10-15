//! Fixed-panel SQ8 diagnostics; every observation remains provisional until sealed EOF.
use super::{CompletedConfig, Result, fields, plus, require};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::Path;

pub(super) const REPORT_SCHEMA: &str = "borsuk-sq8-panel-attribution-v1";
const CONFIG_SCHEMA: &str = "borsuk-sq8-panel-attribution-config-v1";
const LABELS: [&str; 6] = ["A0", "B0", "A1", "B1", "B2", "A2"];
const ACTORS: [&str; 2] = [
    "8910ef50c1cce155eb396d71a2e05dd3b9e906a0",
    "ccbb561bec72e97bc18df363ea307616811af264",
];
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
const SOURCE_PINS: [[&str; 7]; 2] = [
    [
        "27e893f926f1ce6c56738505795f5afff1384ed595300a3f5a2c08105407ecd3",
        "0eec4171d42d42e601ffc473027870f226342e1aae70e28efc9ced4c79f35f9d",
        "a8b6dbc74ea15b08d15499bcf83cab680c17fcc040840e422818b505a8758cd8",
        "eddf88c6c8ee23a732751f49291293f1cc081545ebb9a21c149eb757aa5c38ec",
        "dbcc4cdbc4bb5c244354b375afd657f8b5cb1892df3f0ff0b8f41ef580de42e0",
        "bf31d4e0b6e53c9e7da88ab8a5d6d27a748d2492d7a915e7df7eb131c41c45e2",
        "a4dcc7f9bc06cdea835f72860f3d2e796fcb6f29276858c349d624884cc9f74b",
    ],
    [
        "3d1e3f63d88d943615c7155460aeb3c9cc05c19e27d48d9c39d4fb60d6065304",
        "84518f01fa2b9625081c7a4c2dbd3eab1987f0cc0893ba77af9c32a997422f7f",
        "a8b6dbc74ea15b08d15499bcf83cab680c17fcc040840e422818b505a8758cd8",
        "eddf88c6c8ee23a732751f49291293f1cc081545ebb9a21c149eb757aa5c38ec",
        "dbcc4cdbc4bb5c244354b375afd657f8b5cb1892df3f0ff0b8f41ef580de42e0",
        "bf31d4e0b6e53c9e7da88ab8a5d6d27a748d2492d7a915e7df7eb131c41c45e2",
        "a4dcc7f9bc06cdea835f72860f3d2e796fcb6f29276858c349d624884cc9f74b",
    ],
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
    actor_commit: String,
    completed: CompletedConfig,
}

fn number(v: &Value) -> Result<u64> {
    v.as_u64()
        .ok_or_else(|| "panel unsigned integer required".into())
}
fn duration(start: u64, end: u64) -> Result<u64> {
    end.checked_sub(start)
        .ok_or_else(|| "panel reversed timestamps".into())
}
fn candidate(c: &CompletedConfig) -> bool {
    c.expected_identity["schema"] == "borsuk-cohere-native-baseline-result-v9"
}

// This opt-in validation view preserves original v8 bytes, proof geometry and seals.
pub(super) fn validate_v8_row(line: &[u8], phase: &str) -> Result<()> {
    if phase == "diagnostic_admission" {
        return super::v9_panel::validate_row(line, phase);
    }
    if phase == "query" {
        let super::UniqueJson(mut raw) = serde_json::from_slice(line)?;
        if let Some(diag) = raw
            .as_object_mut()
            .ok_or("query object")?
            .remove("diagnostic")
        {
            diagnostic_shape(&diag)?;
        }
        return super::validate_scale_row(&serde_json::to_vec(&raw)?, phase);
    }
    super::validate_scale_row(line, phase)
}

pub(super) fn validate_completed(c: &CompletedConfig) -> Result<()> {
    let i = &c.expected_identity;
    let b = &c.expected_bound_inputs;
    require(
        c.schema == super::SCALE_CONFIG_SCHEMA,
        "panel completed schema",
    )?;
    require(
        b["execution"]["mode"] == "diagnostic_panel"
            && super::v9_panel::ordinals(b, 1000)? == ORDINALS,
        "fixed diagnostic panel required before result open",
    )?;
    require(
        matches!(
            i["schema"].as_str(),
            Some(
                "borsuk-cohere-native-baseline-result-v8"
                    | "borsuk-cohere-native-baseline-result-v9"
            )
        ) && b["rows"] == 1_000_000
            && b["dimensions"] == 1024
            && b["count"] == 1000
            && b["k"] == 10
            && b["metric"] == "cosine"
            && b["selected_count"] == 32
            && b["source_cache"] == "off"
            && b["fetch_parallelism"] == 64
            && b["source_get_budget"] == 128
            && b["serving"] == json!({"mode":"baseline"})
            && b["backend"]["kind"] == "s3"
            && c.input.bytes > 0
            && c.input.bytes <= super::FILE_CAP
            && super::valid_sha(&c.input.sha256),
        "panel descriptor geometry/envelope",
    )?;
    for key in [
        "execution",
        "serving",
        "fetch_parallelism",
        "source_get_budget",
        "shared_byte_window",
    ] {
        require(i.get(key) == b.get(key), "panel identity/input agreement")?;
    }
    let actor = usize::from(candidate(c));
    for (key, pin) in COMPONENTS.iter().zip(SOURCE_PINS[actor]) {
        require(i[*key] == pin, "panel actor source identity")?;
    }
    if actor == 1 {
        require(
            b["sq8_get_budget"] == 30 && b["sq8_byte_budget"] == 33_554_432,
            "B SQ8 envelope",
        )?;
        super::v9_panel::budgets(i, b)?;
    } else {
        for header in [i, b] {
            require(
                header.get("sq8_get_budget").is_none() && header.get("sq8_byte_budget").is_none(),
                "A implicit SQ8 envelope",
            )?;
        }
    }
    for (row, phase) in [(i, "identity"), (b, "bound_inputs")] {
        let bytes = serde_json::to_vec(row)?;
        if actor == 1 {
            super::v9_panel::validate_row(&bytes, phase)?;
        } else {
            validate_v8_row(&bytes, phase)?;
        }
    }
    let mut neutral = i.clone();
    neutral["schema"] = json!("borsuk-cohere-native-baseline-result-v2");
    let identity = serde_json::from_value(neutral.clone())?;
    super::validate_v2_identity(&identity, &neutral)?;
    super::validate_scale_inputs_for_version(&serde_json::from_value(b.clone())?, b, true)
}

fn validate_config(c: &Config) -> Result<()> {
    require(c.schema == CONFIG_SCHEMA, "panel config schema")?;
    for (slot, d) in c.runs.iter().enumerate() {
        let actor = usize::from(matches!(slot, 1 | 3 | 4));
        require(
            d.label == LABELS[slot] && d.actor_commit == ACTORS[actor],
            "panel label/actor",
        )?;
        validate_completed(&d.completed)?;
        require(
            candidate(&d.completed) == (actor == 1)
                && d.completed.expected_bound_inputs["execution"]["trace"] == (slot >= 2),
            "panel actor/trace mode",
        )?;
        for previous in &c.runs[..slot] {
            require(
                d.completed.input.path != previous.completed.input.path
                    && d.completed.input.sha256 != previous.completed.input.sha256,
                "duplicate panel descriptor",
            )?;
        }
        let base = &c.runs[actor].completed;
        let mut identity = d.completed.expected_identity.clone();
        let mut base_identity = base.expected_identity.clone();
        for value in [&mut identity, &mut base_identity] {
            for key in ["execution", "config_sha256"] {
                value.as_object_mut().ok_or("identity object")?.remove(key);
            }
        }
        let mut inputs = d.completed.expected_bound_inputs.clone();
        let mut base_inputs = base.expected_bound_inputs.clone();
        for value in [&mut inputs, &mut base_inputs] {
            value
                .as_object_mut()
                .ok_or("inputs object")?
                .remove("execution");
        }
        require(
            identity == base_identity && inputs == base_inputs,
            "within-actor header drift",
        )?;
        for key in [
            "dataset",
            "revision",
            "rows",
            "dimensions",
            "metric",
            "tie_rule",
            "count",
            "k",
            "corpus_source_first",
            "query_source_first",
            "corpus_intervals",
            "reserved_query_interval",
            "reserved_queries_sha256",
            "requests_bytes",
            "requests_sha256",
            "truth_bytes",
            "truth_sha256",
            "native_source_sha256",
            "native_sq8_sha256",
            "native_order_sha256",
            "cohort_receipt_sha256",
            "derivation_receipt_sha256",
            "producer_authority",
        ] {
            require(
                d.completed.expected_bound_inputs[key]
                    == c.runs[0].completed.expected_bound_inputs[key],
                "panel benchmark input mismatch",
            )?;
        }
    }
    Ok(())
}

pub(super) fn validate_admission(raw: &Value, inputs: &Value, current: bool) -> Result<()> {
    let mut view = inputs.clone();
    if !current {
        view["sq8_get_budget"] = json!(32);
    }
    super::v9_panel::validate_admission(raw, &view)?;
    let diagnostic_pinned = if raw["trace"] == true {
        use std::mem::{align_of, size_of};
        require(
            cfg!(target_pointer_width = "64")
                && (size_of::<Option<u64>>(), align_of::<Option<u64>>()) == (16, 8)
                && (
                    size_of::<std::path::PathBuf>(),
                    align_of::<std::path::PathBuf>(),
                ) == (24, 8)
                && (size_of::<Vec<u8>>(), align_of::<Vec<u8>>()) == (24, 8),
            "panel pinned producer host sampler ABI",
        )?;
        // Source-derived for both pinned actors: two 49-option snapshots plus PathBuf and Vec.
        let host_sampler_bytes = 2 * (1 + HOST_FIELDS.len()) * 16 + 24 + 24;
        2 * 65536 + host_sampler_bytes + 131072 + 1 + 8192 + 262144
    } else {
        2 * 65536
    };
    // Shared admission already reconciles the total with the modeled trace peak.
    require(
        raw["diagnostic_pinned_bytes"] == diagnostic_pinned,
        "panel pinned diagnostic reservation",
    )?;
    require(
        raw["panel_line_cap_bytes"] == 65536
            && raw["max_active_queries"] == 1
            && raw["charged_in_caller_pinned_bytes"] == true,
        "panel admission limits",
    )?;
    fields(
        &raw["semantics"],
        "headers_interval body_gaps ranges host_counters unset",
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
        "panel diagnostic semantics",
    )
}

fn validate_plan(raw: &Value, current: bool) -> Result<()> {
    if current {
        return super::v9_panel::validate_geometry(raw, 1_000_000, 1024, 30, 33_554_432);
    }
    let plan = &raw["plan"];
    let selected = plan["selected_pages"]
        .as_array()
        .ok_or("A selected pages")?;
    let ranges = plan["ranges"].as_array().ok_or("A ranges")?;
    require(
        !selected.is_empty() && selected.len() <= 64 && !ranges.is_empty() && ranges.len() <= 32,
        "A plan bounds",
    )?;
    let page_bytes = 256 * 1036;
    let mut covered = 0;
    let mut bytes = 0;
    let mut last = 0;
    for range in ranges {
        let pair = range.as_array().ok_or("A range pair")?;
        require(pair.len() == 2, "A range pair length")?;
        let (start, end) = (number(&pair[0])?, number(&pair[1])?);
        require(
            start >= last
                && start < end
                && start % page_bytes == 0
                && end <= 1_000_000 * 1036
                && (end % page_bytes == 0 || end == 1_000_000 * 1036),
            "A proof256 physical range",
        )?;
        bytes = plus(bytes, end - start)?;
        covered = plus(covered, (end - start).div_ceil(page_bytes))?;
        last = end;
    }
    let mut seen = std::collections::BTreeSet::new();
    for p in selected {
        let p = number(p)?;
        require(
            p < 1_000_000_u64.div_ceil(256)
                && seen.insert(p)
                && ranges.iter().any(|r| {
                    r[0].as_u64().is_some_and(|s| s <= p * page_bytes)
                        && r[1]
                            .as_u64()
                            .is_some_and(|e| e >= ((p + 1) * 256).min(1_000_000) * 1036)
                }),
            "A selected page coverage",
        )?;
    }
    require(
        selected.windows(2).all(|p| p[0].as_u64() < p[1].as_u64()),
        "A sorted selected pages",
    )?;
    let selected_pages = seen.iter().map(|&p| p as usize).collect();
    let (canonical, _) =
        super::source_cover::cover_pages(&selected_pages, 1_000_000, 1036, 256, 32)
            .map_err(|e| format!("A canonical cover: {e:?}"))?;
    require(
        plan["ranges"]
            == json!(
                canonical
                    .iter()
                    .map(|r| [r.start, r.end])
                    .collect::<Vec<_>>()
            ),
        "A canonical proof256 cover",
    )?;
    let target = number(&plan["target_pages"])?;
    require(
        bytes <= 16_773_120
            && plan["planned_bytes"] == bytes
            && plan["covered_pages"] == covered
            && plan["bridge_pages"] == covered.saturating_sub(selected.len() as u64)
            && target == 1_000_000_u64.div_ceil(256)
            && target >= selected.len() as u64
            && plan["target_shortfall"] == target - selected.len() as u64
            && plan["primary_pages_retained"] == 1
            && seen.contains(&number(&raw["trace"]["primary_page"])?)
            && raw["charges"]["sq8"]
                == json!({"submitted_gets":ranges.len(),"verified_bytes":bytes,"failed_gets":0}),
        "A plan counts/charges",
    )
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
}

fn decompose(raw: &Value, current: bool) -> Result<Decomposition> {
    validate_plan(raw, current)?;
    let diag = &raw["diagnostic"];
    diagnostic_shape(diag)?;
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
    let plan = raw["plan"]["ranges"]
        .as_array()
        .ok_or("panel physical plan")?;
    require(
        !ranges.is_empty()
            && ranges.len() <= 128
            && ranges.len() == plan.len()
            && trace["planned_ranges"] == ranges.len()
            && trace["max_parallel"] == ranges.len().min(64)
            && trace["planned_bytes"] == raw["plan"]["planned_bytes"],
        "panel trace plan totals",
    )?;
    let fetch = number(&trace["fetch_start_ns"])?;
    let barrier = number(&trace["all_ranges_complete_ns"])?;
    let page_rows = if current { 32 } else { 256 };
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
        rank["ranges"] == ranges.len() && rank["rows"] == bytes / 1036 && bytes % 1036 == 0,
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
    })
}

fn difference(a: &Decomposition, b: &Decomposition) -> Result<Value> {
    let sq8 = i128::from(b.sq8_ns) - i128::from(a.sq8_ns);
    let post = i128::from(b.post_barrier_ns) - i128::from(a.post_barrier_ns);
    let positive = sq8.max(0);
    let post_positive = post.max(0);
    let predominantly_post = positive > 0
        && post_positive
            .checked_mul(100)
            .ok_or("panel cross-product overflow")?
            >= positive
                .checked_mul(80)
                .ok_or("panel cross-product overflow")?;
    Ok(json!({"sq8_excess_ns":sq8,"post_barrier_excess_ns":post,
        "interval_excess_ns":a.intervals_ns.iter().zip(b.intervals_ns).map(|(&a,b)|i128::from(b)-i128::from(a)).collect::<Vec<_>>(),
        "at_least_20ms":sq8>=20_000_000,"post_barrier_at_least_80_percent":predominantly_post}))
}

pub(super) fn reduce(path: &Path, sha: &str) -> Result<Value> {
    let (c, config_bytes): (Config, _) = super::read_config(path, sha)?;
    validate_config(&c)?;
    let mut runs: Vec<super::Run> = Vec::with_capacity(6);
    let mut traces: Vec<Vec<Decomposition>> = Vec::with_capacity(6);
    let mut provisional_bytes = 0_u64;
    for (slot, d) in c.runs.iter().enumerate() {
        let completed = &d.completed;
        let mut observed = Vec::with_capacity(if slot >= 2 { 32 } else { 0 });
        let run = super::read_run_observed_policy(
            &completed.input.path,
            &completed.input.sha256,
            Some(completed),
            None,
            super::ReadPolicy::Sq8PanelAttribution,
            |line, _, _| {
                let super::UniqueJson(raw) = serde_json::from_slice(line)?;
                validate_plan(&raw, candidate(completed))?;
                require(
                    raw["sum"]["failed_gets"] == 0,
                    "panel successful query charges",
                )?;
                require(
                    raw.get("diagnostic").is_some() == (slot >= 2),
                    "panel trace presence",
                )?;
                if slot >= 2 {
                    require(observed.len() < 32, "panel provisional count")?;
                    let summary = decompose(&raw, candidate(completed))?;
                    provisional_bytes = plus(
                        provisional_bytes,
                        serde_json::to_vec(&summary)?.len() as u64,
                    )?;
                    require(
                        provisional_bytes <= 512 * 1024,
                        "panel provisional summary cap",
                    )?;
                    observed.push(summary);
                }
                Ok(())
            },
        )
        .map_err(|e| format!("{}: {e}", d.label))?;
        require(
            run.samples.len() == 32 && run.terminal.underfilled_queries == 0,
            "panel complete full-k",
        )?;
        for previous in &runs {
            require(
                (run.file_identity.dev, run.file_identity.ino)
                    != (previous.file_identity.dev, previous.file_identity.ino),
                "panel duplicate inode",
            )?;
        }
        if slot >= 2 {
            let base = if matches!(slot, 3 | 4) { 1 } else { 0 };
            for (a, b) in runs[base].samples.iter().zip(&run.samples) {
                require(
                    a.returned == b.returned
                        && a.hits10 == b.hits10
                        && a.charges == b.charges
                        && a.trace_sha256 == b.trace_sha256
                        && a.physical_plan_sha256 == b.physical_plan_sha256,
                    "panel within-actor scorebit/trace/plan/charge/recall parity",
                )?;
            }
        }
        runs.push(run);
        traces.push(observed);
    }
    // No retained callback observation can establish acceptance on its own.
    for (d, run) in c.runs.iter().zip(&runs) {
        require(
            super::file_identity(&super::open_input(&d.completed.input.path)?)?
                == run.file_identity,
            "panel input changed after authentication",
        )?;
    }
    let mut reproduced = 0;
    let mut queries = Vec::with_capacity(32);
    for (slot, ordinal) in ORDINALS.into_iter().enumerate() {
        let pair1 = difference(&traces[2][slot], &traces[3][slot])?;
        let pair2 = difference(&traces[5][slot], &traces[4][slot])?;
        let excess = pair1["at_least_20ms"] == true && pair2["at_least_20ms"] == true;
        let falsified = excess
            && pair1["post_barrier_at_least_80_percent"] == true
            && pair2["post_barrier_at_least_80_percent"] == true;
        reproduced += usize::from(excess);
        let perturbation=[(0,2),(0,5),(1,3),(1,4)].map(|(off,on)| {
            let a=&runs[off].completed.as_ref().expect("completed reader").stage_samples[3];
            let b=&runs[on].completed.as_ref().expect("completed reader").stage_samples[3];
            json!({"off":LABELS[off],"on":LABELS[on],"sq8_delta_ns":i128::from(b[slot])-i128::from(a[slot]),
                "query_wall_delta_ns":i128::from(runs[on].samples[slot].wall_ns)-i128::from(runs[off].samples[slot].wall_ns)})
        });
        queries.push(json!({"ordinal":ordinal,
            "traced":{"A1":traces[2][slot],"B1":traces[3][slot],"B2":traces[4][slot],"A2":traces[5][slot]},
            "B1_minus_A1":pair1,"B2_minus_A2":pair2,"reproduced_excess":excess,
            "predominantly_pre_barrier_falsified":falsified,
            "evidence":if falsified {"PREDOMINANTLY_PRE_BARRIER_FALSIFIED"} else {"UNRESOLVED"},
            "trace_on_off_perturbation_descriptive":perturbation}));
    }
    Ok(
        json!({"schema":REPORT_SCHEMA,"status":if reproduced==0 {"INCONCLUSIVE_FIXED_PANEL"} else {"MEASURED"},
        "complete":true,"diagnostic_only":true,"population_percentiles_valid":false,
        "vendor_or_scientific_win_claim":false,"default_adoption":false,
        "reduction_config_sha256":sha,"reduction_config_bytes":config_bytes,
        "selected_count":32,"reproduced_ordinals":reproduced,"within_actor_trace_off_parity":true,
        "range_fields":RANGE_FIELDS,"host_fields":HOST_FIELDS.as_slice(),
        "boundary_fields":["stage_start","fetch_start","latest_range_complete","all_ranges_barrier","rank_start","rank_end","release_end","stage_end"],
        "interval_fields":["pre_fetch","fetch_to_latest_complete","latest_complete_to_barrier","barrier_to_rank","rank","release","stage_tail"],
        "semantics":"Ranges overlap; only the boundary chain reconciles stage time. Headers include client queue/connect/TLS/server. Body gaps include polling/scheduling. Authentication overlap is a union, not a causal attribution. score_ns combines ordinal preparation and SQ8 scoring. Host evidence brackets whole queries, not individual flows. Trace timing perturbation is descriptive.",
        "runs":c.runs.iter().zip(&runs).map(|(d,r)|json!({"label":d.label,"actor_commit":d.actor_commit,
            "input":d.completed.input,"identity":d.completed.expected_identity,
            "recall_numerator":r.terminal.recall_numerator,"recall_denominator":r.terminal.recall_denominator})).collect::<Vec<_>>(),
        "queries":queries}),
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

    fn query(current: bool) -> Value {
        let page = if current { 32 } else { 256 };
        let bytes = page * 1036;
        let mut deltas = serde_json::Map::new();
        for key in HOST_FIELDS {
            deltas.insert(key.into(), Value::Null);
        }
        deltas.insert("Tcp.RetransSegs".into(), json!(0));
        let mut plan = json!({"selected_pages":[0,2],"ranges":[[0,bytes],[2*bytes,3*bytes]],
            "planned_bytes":2*bytes,"target_pages":if current {2} else {3907},
            "target_shortfall":if current {0} else {3905},"primary_pages_retained":if current {0} else {1},
            "covered_pages":2,"bridge_pages":0});
        if current {
            plan["page_rows"] = json!(32);
        }
        json!({"query_process_cpu_ns":1,"plan":plan,"charges":{"sq8":{"submitted_gets":2,"verified_bytes":2*bytes,"failed_gets":0}},
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
                ]}}})
    }

    fn move_delay(raw: &mut Value, body: u64, rank: u64) {
        let trace = &mut raw["diagnostic"]["sq8_range_trace"];
        for col in [10, 11, 12, 13, 14, 18] {
            trace["ranges"][0][col] = json!(trace["ranges"][0][col].as_u64().unwrap() + body);
        }
        trace["ranges"][0][17] = json!(140 + body);
        for key in [
            "all_ranges_complete_ns",
            "rank_start_ns",
            "rank_end_ns",
            "release_end_ns",
        ] {
            trace[key] = json!(trace[key].as_u64().unwrap() + body);
        }
        for key in ["rank_end_ns", "release_end_ns"] {
            trace[key] = json!(trace[key].as_u64().unwrap() + rank);
        }
        trace["rank"]["score_ns"] = json!(10 + rank);
        raw["stages"]["sq8"]["end_ns"] = json!(550 + body + rank);
    }

    fn fixture(slot: usize) -> Vec<Value> {
        let current = matches!(slot, 1 | 3 | 4);
        let traced = slot >= 2;
        let actor = usize::from(current);
        let full = paired_scale_fixture(64);
        let execution = json!({"mode":"diagnostic_panel","ordinals":ORDINALS,"trace":traced});
        let mut rows = full[..2].to_vec();
        rows[0]["schema"] = json!(if current {
            "borsuk-cohere-native-baseline-result-v9"
        } else {
            "borsuk-cohere-native-baseline-result-v8"
        });
        rows[0]["binary_sha256"] = json!(if current { "b" } else { "a" }.repeat(64));
        rows[0]["config_sha256"] = json!(if traced { "4" } else { "5" }.repeat(64));
        for (key, pin) in COMPONENTS.iter().zip(SOURCE_PINS[actor]) {
            rows[0][*key] = json!(pin);
        }
        for header in &mut rows[..2] {
            header["execution"] = execution.clone();
            header["source_get_budget"] = json!(128);
            if current {
                header["sq8_get_budget"] = json!(30);
                header["sq8_byte_budget"] = json!(33554432);
            }
        }
        rows[1]["selected_count"] = json!(32);
        use borsuk::sq8_s3_range::Sq8RangeTrace;
        let ranges = if current { 30 } else { 32 };
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
        let example = query(current);
        let sq8_bytes = example["plan"]["planned_bytes"].as_u64().unwrap();
        let total = sq8_bytes + 8448 + 100;
        for (selected_slot, ordinal) in ORDINALS.into_iter().enumerate() {
            let mut q = full[5 + ordinal].clone();
            q["selected_slot"] = json!(selected_slot);
            q["plan"] = example["plan"].clone();
            q["charges"]["sq8"] = example["charges"]["sq8"].clone();
            q["charges"]["source"]["verified_bytes"] = json!(8448);
            q["sum"] = json!({"submitted_gets":4,"verified_bytes":total,"failed_gets":0});
            q["stages"] = json!({"discovery":{"start_ns":1,"end_ns":2},"source":{"start_ns":2,"end_ns":3},
                "planning":{"start_ns":3,"end_ns":100},"sq8":{"start_ns":100,"end_ns":550},"leaf_peak_inflight":2});
            q["query_wall_ns"] = json!(600);
            q["query_process_cpu_ns"] = json!(1);
            q["trace"] = json!({"ranked_candidate_pages":[0,2],"primary_page":0,"nomination_evaluated_units":[0,16],
                "discoveries":[],"semantic_leaves":[0],"semantic_units":[0,16],"semantic_seed_additions":[]});
            if traced {
                q["diagnostic"] = example["diagnostic"].clone();
            }
            // A/B ranking equality is intentionally absent; within-actor bits stay identical.
            if current {
                q["returned"][9]["id"] = json!(11);
            }
            for (side, n) in [
                ("before", selected_slot as u64),
                ("after", selected_slot as u64 + 1),
            ] {
                let attempts = 5 + 4 * n;
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
        s["population_percentiles_valid"] = json!(false);
        s["query_wall_ns"] = json!(32 * 600);
        s["process_wall_ns"] = json!(32 * 600 + 100);
        s["query_process_cpu_ns"] = json!(32);
        s["process_cpu_ns"] = json!(132 + slot);
        s["charges"] = json!({"router":{"submitted_gets":32,"verified_bytes":3200,"failed_gets":0},
            "source":{"submitted_gets":32,"verified_bytes":32*8448,"failed_gets":0},
            "sq8":{"submitted_gets":64,"verified_bytes":32*sq8_bytes,"failed_gets":0}});
        s["sum"] = json!({"submitted_gets":128,"verified_bytes":32*total,"failed_gets":0});
        s["transport_last_boundary"] = span.compact();
        rows.push(terminal);
        authenticate(&mut rows);
        rows
    }

    fn completed(dir: &Path, label: &str, rows: &[Value]) -> CompletedConfig {
        let bytes = encode(rows);
        let path = dir.join(label);
        std::fs::write(&path, &bytes).unwrap();
        CompletedConfig {
            schema: super::super::SCALE_CONFIG_SCHEMA.into(),
            input: super::super::CompletedInput {
                path,
                bytes: bytes.len() as u64,
                sha256: hash(&bytes),
            },
            expected_identity: rows[0].clone(),
            expected_bound_inputs: rows[1].clone(),
        }
    }

    fn read(
        c: &CompletedConfig,
        mut callback: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<super::super::Run> {
        super::super::read_run_observed_policy(
            &c.input.path,
            &c.input.sha256,
            Some(c),
            None,
            super::super::ReadPolicy::Sq8PanelAttribution,
            |line, _, _| callback(line),
        )
    }

    fn config(dir: &Path) -> Value {
        let runs=(0..6).map(|slot| {
            let c=completed(dir,LABELS[slot],&fixture(slot));
            json!({"label":LABELS[slot],"actor_commit":ACTORS[usize::from(matches!(slot,1|3|4))],"completed":c})
        }).collect::<Vec<_>>();
        json!({"schema":CONFIG_SCHEMA,"runs":runs})
    }

    fn save_config(dir: &Path, c: &Value) -> (std::path::PathBuf, String) {
        let bytes = serde_json::to_vec(c).unwrap();
        let path = dir.join("config");
        std::fs::write(&path, &bytes).unwrap();
        (path, hash(&bytes))
    }

    #[test]
    fn sq8_panel_overlap_tie_and_exact_reconciliation() {
        for current in [false, true] {
            let mut raw = query(current);
            let d = decompose(&raw, current).unwrap();
            assert_eq!(d.boundaries_ns, [100, 110, 360, 400, 420, 520, 540, 550]);
            assert_eq!(d.intervals_ns, [10, 250, 40, 20, 100, 20, 10]);
            assert_eq!((d.sq8_ns, d.post_barrier_ns), (450, 150));
            assert_eq!(d.latest_range[0], 0);
            assert_eq!(d.largest_body_gap["authentication_overlap_union_ns"], 40);
            assert_eq!(d.rank_unattributed_ns, 30);
            raw["diagnostic"]["sq8_range_trace"]["ranges"][1][14] = json!(360);
            assert_eq!(decompose(&raw, current).unwrap().latest_range[0], 0);
            raw["diagnostic"]["sq8_range_trace"]["ranges"][1][14] = json!(361);
            assert_eq!(decompose(&raw, current).unwrap().latest_range[0], 1);
        }
    }

    #[test]
    fn sq8_panel_authentication_overlap_is_a_union() {
        let mut q = query(true);
        let bytes = 33152;
        q["plan"]["selected_pages"] = json!([0, 2, 4]);
        q["plan"]["ranges"] = json!([[0, bytes], [2 * bytes, 3 * bytes], [4 * bytes, 5 * bytes]]);
        for key in ["target_pages", "covered_pages"] {
            q["plan"][key] = json!(3);
        }
        q["plan"]["planned_bytes"] = json!(3 * bytes);
        q["charges"]["sq8"] = json!({"submitted_gets":3,"verified_bytes":3*bytes,"failed_gets":0});
        let t = &mut q["diagnostic"]["sq8_range_trace"];
        t["ranges"].as_array_mut().unwrap().push(json!([
            2,
            4,
            4,
            4 * bytes,
            5 * bytes,
            122,
            132,
            142,
            152,
            162,
            182,
            192,
            220,
            260,
            270,
            2,
            bytes,
            20,
            182,
            10,
            2,
            "ok"
        ]));
        t["planned_ranges"] = json!(3);
        t["planned_bytes"] = json!(3 * bytes);
        t["max_parallel"] = json!(3);
        t["rank"]["ranges"] = json!(3);
        t["rank"]["rows"] = json!(96);
        let d = decompose(&q, true).unwrap();
        assert_eq!(
            d.largest_body_gap["authentication_overlap_by_range_ns"],
            json!([[1, 40], [2, 40]])
        );
        assert_eq!(d.largest_body_gap["authentication_overlap_union_ns"], 60);
    }

    #[test]
    fn sq8_panel_moved_wait_and_integer_boundaries() {
        let a = decompose(&query(true), true).unwrap();
        for (body, rank, reproduced, post) in [
            (20_000_000, 0, true, false),
            (0, 20_000_000, true, true),
            (4_000_000, 16_000_000, true, true),
            (4_000_001, 15_999_999, true, false),
            (0, 19_999_999, false, true),
        ] {
            let mut raw = query(true);
            move_delay(&mut raw, body, rank);
            let b = decompose(&raw, true).unwrap();
            let diff = difference(&a, &b).unwrap();
            assert_eq!(diff["at_least_20ms"], reproduced);
            assert_eq!(diff["post_barrier_at_least_80_percent"], post);
            assert_eq!(b.sq8_ns - a.sq8_ns, body + rank);
            assert_eq!(difference(&b, &a).unwrap()["at_least_20ms"], false);
        }
        for (chunks, first, last, eof, gap, gap_end, copy, valid) in [
            (2, 160, 230, 310, 70, Some(230), 10, true),
            (2, 160, 230, 310, 69, Some(230), 10, false),
            (2, 160, 230, 310, 80, Some(310), 10, true),
            (2, 160, 300, 310, 10, Some(160), 140, true),
            (2, 160, 300, 310, 0, None, 10, false),
            (2, 160, 300, 310, 0, None, 150, true),
            (2, 160, 300, 310, 100, Some(260), 10, false),
            (3, 160, 300, 310, 100, Some(260), 10, true),
            (1, 160, 300, 310, 140, Some(300), 10, false),
            (1, 160, 160, 310, 140, Some(310), 10, true),
            (1, 160, 160, 310, 10, Some(160), 140, true),
            (1, 160, 160, 310, 0, None, 150, true),
            (1, 160, 160, 310, 100, Some(260), 50, false),
        ] {
            let mut raw = query(true);
            let row = &mut raw["diagnostic"]["sq8_range_trace"]["ranges"][0];
            for (column, value) in [
                (9, first),
                (10, last),
                (11, eof),
                (15, chunks),
                (17, gap),
                (19, copy),
                (20, chunks),
            ] {
                row[column] = json!(value);
            }
            row[18] = json!(gap_end);
            assert_eq!(
                decompose(&raw, true).is_ok(),
                valid,
                "{chunks} chunks, gap {gap}"
            );
        }
        for (gap, copy, valid) in [
            (u64::MAX / 2, 1, true),
            (u64::MAX / 2, 2, false),
            (u64::MAX / 2 + 1, 1, false),
        ] {
            let mut raw = query(true);
            raw["stages"]["sq8"]["end_ns"] = json!(u64::MAX);
            let trace = &mut raw["diagnostic"]["sq8_range_trace"];
            for key in [
                "all_ranges_complete_ns",
                "rank_start_ns",
                "rank_end_ns",
                "release_end_ns",
            ] {
                trace[key] = json!(u64::MAX);
            }
            for key in ["score_ns", "roster_ns", "sort_ns", "finish_ns"] {
                trace["rank"][key] = json!(0);
            }
            let row = &mut trace["ranges"][0];
            row[10] = json!(160 + gap);
            for column in 11..=14 {
                row[column] = json!(u64::MAX);
            }
            row[17] = json!(gap);
            row[18] = row[10].clone();
            row[19] = json!(copy);
            let result = decompose(&raw, true);
            if valid {
                assert!(result.is_ok());
            } else {
                assert_eq!(
                    result.err().unwrap().to_string(),
                    "panel body gap bound overflow"
                );
            }
        }
        for (start, end, cpu, valid) in [
            (0, 0, 0, true),
            (0, u64::MAX, u64::MAX, true),
            (u64::MAX - 1, u64::MAX, 1, true),
            (u64::MAX, 0, 1, false),
            (100, 200, 1, false),
        ] {
            let mut raw = query(true);
            raw["diagnostic"]["process_cpu_start_ns"] = json!(start);
            raw["diagnostic"]["process_cpu_end_ns"] = json!(end);
            raw["query_process_cpu_ns"] = json!(cpu);
            assert_eq!(decompose(&raw, true).is_ok(), valid);
        }
    }

    #[test]
    fn sq8_panel_host_null_and_structure_refusals() {
        let good = query(true);
        let d = decompose(&good, true).unwrap();
        assert_eq!(d.host["deltas"][30], 0);
        assert!(d.host["deltas"][0].is_null());
        let mut absent = good.clone();
        for key in ["before_monotonic_ns", "after_monotonic_ns"] {
            absent["diagnostic"]["host"][key] = Value::Null;
        }
        for key in ["query_monotonic_start_ns", "query_monotonic_end_ns"] {
            absent["diagnostic"][key] = Value::Null;
        }
        assert!(decompose(&absent, true).is_ok());
        for (pointer, value) in [
            ("/diagnostic/sq8_range_trace/fetch_start_ns", Value::Null),
            ("/diagnostic/sq8_range_trace/fetch_start_ns", json!(401)),
            ("/diagnostic/sq8_range_trace/ranges/0/0", json!(1)),
            ("/diagnostic/sq8_range_trace/ranges/0/3", json!(1)),
            ("/diagnostic/sq8_range_trace/ranges/0/6", json!(119)),
            ("/diagnostic/sq8_range_trace/ranges/0/9", Value::Null),
            ("/diagnostic/sq8_range_trace/ranges/0/16", json!(7)),
            ("/diagnostic/sq8_range_trace/ranges/0/17", json!(301)),
            ("/diagnostic/sq8_range_trace/ranges/0/18", Value::Null),
            (
                "/diagnostic/sq8_range_trace/ranges/0/21",
                json!("page_auth"),
            ),
            ("/diagnostic/sq8_range_trace/dropped_ranges", json!(1)),
            ("/diagnostic/sq8_range_trace/rank/rows", json!(63)),
            ("/diagnostic/sq8_range_trace/rank/score_ns", json!(101)),
            (
                "/diagnostic/sq8_range_trace/range_fields/21",
                json!("wrong"),
            ),
            ("/diagnostic/host/unavailable", json!(46)),
            ("/diagnostic/host/deltas/Tcp.RetransSegs", json!(-1)),
            ("/diagnostic/host/before_monotonic_ns", json!(2001)),
            ("/diagnostic/process_cpu_end_ns", json!(99)),
        ] {
            let mut bad = good.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(decompose(&bad, true).is_err(), "{pointer}");
        }
        let mut bad = good.clone();
        bad["diagnostic"]["sq8_range_trace"]["ranges"][0]
            .as_array_mut()
            .unwrap()
            .pop();
        assert!(decompose(&bad, true).is_err());
        bad = good.clone();
        bad["diagnostic"]["sq8_range_trace"]["ranges"] = json!(vec![
                good["diagnostic"]["sq8_range_trace"]["ranges"][0]
                    .clone();
                129
            ]);
        assert!(decompose(&bad, true).is_err());
    }

    #[test]
    fn sq8_panel_sealed_six_runs_and_late_authentication() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        let (path, sha) = save_config(dir.path(), &config);
        let report = reduce(&path, &sha).unwrap();
        assert_eq!(report["status"], "INCONCLUSIVE_FIXED_PANEL");
        assert_eq!(report["queries"].as_array().unwrap().len(), 32);
        assert_eq!(report["within_actor_trace_off_parity"], true);
        assert_eq!(report["queries"][31]["ordinal"], 999);
        assert!(serde_json::to_vec(&report).unwrap().len() < 512 * 1024);
        for case in ["seal", "hash", "eof", "recall", "terminal", "identity"] {
            let mut rows = fixture(2);
            if case == "seal" {
                rows[38]["prefix_sha256"] = json!("0".repeat(64));
            }
            if case == "recall" {
                rows[70]["ordinal"] = json!(998);
            }
            if case == "terminal" {
                rows[71]["summary"]["query_wall_ns"] = json!(1);
            }
            let mut c = completed(dir.path(), case, &rows);
            if case == "hash" {
                c.input.sha256 = "0".repeat(64);
            }
            if case == "eof" {
                let mut bytes = encode(&rows);
                bytes.extend_from_slice(b"{}\n");
                std::fs::write(&c.input.path, &bytes).unwrap();
                c.input.bytes = bytes.len() as u64;
                c.input.sha256 = hash(&bytes);
            }
            let mut observed = 0;
            let result = read(&c, |line| {
                let super::super::UniqueJson(raw) = serde_json::from_slice(line)?;
                decompose(&raw, false)?;
                observed += 1;
                if case == "identity" && observed == 32 {
                    let replacement = dir.path().join("replacement");
                    std::fs::write(&replacement, encode(&rows))?;
                    std::fs::rename(replacement, &c.input.path)?;
                }
                Ok(())
            });
            assert_eq!(observed, 32, "{case}");
            let error = result
                .err()
                .unwrap_or_else(|| panic!("{case}: late failure accepted"));
            let output = dir.path().join(format!("{case}-report"));
            assert!(
                !super::super::execute_report(&output, REPORT_SCHEMA, || { Err(error) }).unwrap()
            );
            let invalid: Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
            assert_eq!(invalid["complete"], false);
            assert_eq!(invalid["status"], "INVALID");
        }
    }

    #[test]
    fn sq8_panel_old_v8_policy_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let c = completed(dir.path(), "traced-v8", &fixture(2));
        assert!(read(&c, |_| Ok(())).is_ok());
        assert!(super::super::read_run_with(&c.input.path, &c.input.sha256, Some(&c)).is_err());
        let off = completed(dir.path(), "off-v8", &fixture(0));
        assert!(
            super::super::read_run_with(&off.input.path, &off.input.sha256, Some(&off)).is_ok()
        );
    }

    #[test]
    fn sq8_panel_config_caps_ordinals_and_occupied_output() {
        let dir = tempfile::tempdir().unwrap();
        let good = config(dir.path());
        for (pointer, value) in [
            ("/schema", json!("unknown")),
            ("/runs/0/actor_commit", json!(ACTORS[1])),
            ("/runs/0/label", json!("A1")),
            (
                "/runs/0/completed/expected_identity/binary_sha256",
                json!("bad"),
            ),
            (
                "/runs/0/completed/expected_identity/runner_source_sha256",
                json!("0".repeat(64)),
            ),
            (
                "/runs/0/completed/input/bytes",
                json!(super::super::FILE_CAP + 1),
            ),
            (
                "/runs/0/completed/expected_bound_inputs/execution",
                json!({"mode":"full"}),
            ),
            (
                "/runs/0/completed/expected_bound_inputs/execution/ordinals/1",
                json!(0),
            ),
            (
                "/runs/0/completed/expected_bound_inputs/execution/ordinals/31",
                json!(998),
            ),
            (
                "/runs/0/completed/expected_bound_inputs/fetch_parallelism",
                json!(32),
            ),
            (
                "/runs/0/completed/expected_bound_inputs/source_get_budget",
                json!(96),
            ),
            (
                "/runs/1/completed/expected_bound_inputs/sq8_get_budget",
                json!(32),
            ),
        ] {
            let mut bad = good.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            bad["runs"][0]["completed"]["input"]["path"] = json!(dir.path().join("never-exists"));
            let c: Config = serde_json::from_value(bad).unwrap();
            assert!(validate_config(&c).is_err(), "{pointer}");
        }
        let mut bad = good.clone();
        bad["unknown"] = json!(0);
        assert!(serde_json::from_value::<Config>(bad).is_err());
        let mut bad = good.clone();
        for d in bad["runs"].as_array_mut().unwrap() {
            d["completed"]["expected_identity"]["execution"] = json!({"mode":"full"});
            d["completed"]["expected_bound_inputs"]["execution"] = json!({"mode":"full"});
            d["completed"]["input"]["path"] = json!(dir.path().join("must-not-open"));
        }
        let (p, s) = save_config(dir.path(), &bad);
        assert_eq!(
            reduce(&p, &s).unwrap_err().to_string(),
            "fixed diagnostic panel required before result open"
        );
        let raw = serde_json::to_string(&good).unwrap();
        let duplicate = raw.replacen('{', "{\"schema\":\"duplicate\",", 1);
        std::fs::write(&p, &duplicate).unwrap();
        assert!(reduce(&p, &hash(duplicate.as_bytes())).is_err());
        let oversized = vec![b' '; 32769];
        std::fs::write(&p, &oversized).unwrap();
        assert!(reduce(&p, &hash(&oversized)).is_err());
        let output = dir.path().join("occupied");
        std::fs::write(&output, b"keep").unwrap();
        let mut called = false;
        assert!(
            super::super::execute_report(&output, REPORT_SCHEMA, || {
                called = true;
                Ok(Value::Null)
            })
            .is_err()
        );
        assert!(!called);
        assert_eq!(std::fs::read(&output).unwrap(), b"keep");
        let over = dir.path().join("oversize-report");
        assert!(
            super::super::execute_report(&over, REPORT_SCHEMA, || Ok(
                json!({"payload":"x".repeat(512*1024)})
            ))
            .is_err()
        );
        let mut rows = fixture(2);
        rows[6]["trace"]["nomination_evaluated_units"] = json!(vec![0_u64; 40000]);
        authenticate(&mut rows);
        let c = completed(dir.path(), "oversize-line", &rows);
        assert!(read(&c, |_| Ok(())).is_err());
    }

    #[test]
    fn sq8_panel_resealed_semantic_and_admission_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let good = config(dir.path());
        for (pointer, value) in [
            ("/6/returned/9/score_bits", json!(0x4040_0000_u32)),
            ("/6/trace/primary_page", json!(2)),
            ("/6/plan/ranges/0/1", json!(1)),
            ("/6/ordinal", json!(32)),
            ("/6/selected_slot", json!(1)),
            ("/2/max_active_queries", json!(2)),
            ("/2/trace_peak_bytes", json!(1)),
            ("/2/trace_ranges", json!(128)),
            ("/2/panel_line_cap_bytes", json!(262144)),
        ] {
            let mut rows = serde_json::to_value(fixture(2)).unwrap();
            *rows.pointer_mut(pointer).unwrap() = value;
            let mut rows: Vec<Value> = serde_json::from_value(rows).unwrap();
            authenticate(&mut rows);
            let c = completed(dir.path(), "mutation", &rows);
            let mut config = good.clone();
            // The original independently frozen expected headers remain untouched.
            config["runs"][2]["completed"]["input"] = serde_json::to_value(&c.input).unwrap();
            let (p, s) = save_config(dir.path(), &config);
            assert!(reduce(&p, &s).is_err(), "{pointer}");
        }
        for slot in 0..6 {
            let original = fixture(slot);
            let peak = original[2]["trace_peak_bytes"].as_u64().unwrap_or(0);
            let pinned = if slot >= 2 { 534097_u64 } else { 131072 };
            let mut cases = Vec::new();
            for wrong in [1, pinned - 1, pinned + 1] {
                cases.push((
                    vec![
                        ("/2/diagnostic_pinned_bytes", json!(wrong)),
                        ("/2/diagnostic_bytes_per_active_query", json!(wrong + peak)),
                    ],
                    "panel pinned diagnostic reservation",
                ));
            }
            if slot >= 2 {
                cases.extend([
                    (
                        vec![
                            ("/6/diagnostic/sq8_range_trace/ranges/0/17", json!(0)),
                            ("/6/diagnostic/sq8_range_trace/ranges/0/18", Value::Null),
                        ],
                        "panel body gap coverage",
                    ),
                    (
                        vec![("/6/diagnostic/sq8_range_trace/ranges/0/17", json!(1))],
                        "panel body gap coverage",
                    ),
                    (
                        vec![
                            ("/6/diagnostic/sq8_range_trace/ranges/0/15", json!(1)),
                            ("/6/diagnostic/sq8_range_trace/ranges/0/20", json!(1)),
                        ],
                        "panel single chunk boundaries",
                    ),
                    (
                        vec![
                            ("/6/diagnostic/sq8_range_trace/ranges/0/17", json!(100)),
                            ("/6/diagnostic/sq8_range_trace/ranges/0/18", json!(260)),
                        ],
                        "panel body gap endpoint",
                    ),
                    (
                        vec![
                            ("/6/diagnostic/sq8_range_trace/ranges/0/10", json!(160)),
                            ("/6/diagnostic/sq8_range_trace/ranges/0/15", json!(1)),
                            ("/6/diagnostic/sq8_range_trace/ranges/0/17", json!(100)),
                            ("/6/diagnostic/sq8_range_trace/ranges/0/18", json!(260)),
                            ("/6/diagnostic/sq8_range_trace/ranges/0/19", json!(50)),
                            ("/6/diagnostic/sq8_range_trace/ranges/0/20", json!(1)),
                        ],
                        "panel body gap endpoint",
                    ),
                    (
                        vec![("/6/diagnostic/process_cpu_end_ns", json!(200))],
                        "panel process CPU reconciliation",
                    ),
                ]);
            }
            for (changes, expected_error) in cases {
                let mut rows = serde_json::to_value(&original).unwrap();
                for (pointer, value) in changes {
                    *rows.pointer_mut(pointer).unwrap() = value;
                }
                let mut rows: Vec<Value> = serde_json::from_value(rows).unwrap();
                authenticate(&mut rows);
                let c = completed(dir.path(), "semantic-mutation", &rows);
                let mut config = good.clone();
                config["runs"][slot]["completed"]["input"] =
                    serde_json::to_value(&c.input).unwrap();
                let (p, s) = save_config(dir.path(), &config);
                let error = reduce(&p, &s)
                    .err()
                    .expect("resealed contradiction accepted");
                assert!(
                    error.to_string().contains(expected_error),
                    "{slot}: {error}"
                );
            }
        }
    }

    #[test]
    fn sq8_panel_requires_both_pairs_to_reproduce_and_falsify() {
        let dir = tempfile::tempdir().unwrap();
        let baseline = config(dir.path());
        for (second_delay, second_post, status, falsified) in [
            (false, false, "INCONCLUSIVE_FIXED_PANEL", false),
            (true, false, "MEASURED", false),
            (true, true, "MEASURED", true),
        ] {
            let mut c = baseline.clone();
            for slot in [3, 4] {
                let mut rows = fixture(slot);
                if slot == 3 || second_delay {
                    let post = slot == 3 || second_post;
                    move_delay(
                        &mut rows[6],
                        if post { 0 } else { 20_000_000 },
                        if post { 20_000_000 } else { 0 },
                    );
                    rows[6]["query_wall_ns"] = json!(20_000_600);
                    rows[6]["diagnostic"]["query_monotonic_end_ns"] = json!(20_002_000);
                    rows[6]["diagnostic"]["host"]["after_monotonic_ns"] = json!(20_002_100);
                    rows[71]["summary"]["query_wall_ns"] = json!(20_000_000 + 32 * 600);
                    rows[71]["summary"]["process_wall_ns"] = json!(20_000_100 + 32 * 600);
                }
                authenticate(&mut rows);
                let run = completed(
                    dir.path(),
                    &format!("pair-{slot}-{second_delay}-{second_post}"),
                    &rows,
                );
                c["runs"][slot]["completed"]["input"] = serde_json::to_value(&run.input).unwrap();
            }
            let (p, s) = save_config(dir.path(), &c);
            let report = reduce(&p, &s).unwrap();
            assert_eq!(report["status"], status);
            assert_eq!(
                report["queries"][0]["predominantly_pre_barrier_falsified"],
                falsified
            );
            assert_eq!(report["queries"][1]["reproduced_excess"], false);
        }
    }
}
