//! Capsule handoff: --capsule-handoff-abba CONFIG CONFIG_SHA NEW_REPORT.
//! Fixed32 old/new capsule ABBA on the same generation; external qualification required.
//! Offline reduction of four historical A1/B1/B2/A2 files or sealed v2 outputs.
//! This does not qualify resources, cost, cache state, or a vendor comparison.
//! Single-file CLI: --completed-v2 CONFIG CONFIG_SHA256 NEW_OUTPUT_JSON.
//! CONFIG pins the exact identity/bound_inputs rows and input path/bytes/SHA256.
//! Paired CLI: --paired-v2 CONFIG CONFIG_SHA256 NEW_OUTPUT_JSON. CONFIG has
//! schema borsuk-paired-native-reduction-config-v1 and two CompletedConfig arms,
//! ordered fetch_parallelism 16 then 32, with source_cache off in both.
//! Membership CLI: --membership-abba-v2 CONFIG CONFIG_SHA256 NEW_OUTPUT_JSON.
//! Four runs A1/B1/B2/A2 use width32 and one attempt-specific runtime config SHA.
//! Both arms share the same input authority. Only
//! the independently frozen binary, generation and router component pins differ.
//! Evidence CLI: --source-utilization CONFIG CONFIG_SHA256 NEW_REPORT_JSON.
//! Scored completion is query-dependent: this is a hindsight byte bound only.
//! Direct closure SQ8 costs are counterfactual; no scores or recall are evaluated.
//! Header binding: --bind-completed-scale AUTHORITY AUTHORITY_SHA NEW_CONFIG.
//! Independently pins v7 headers for --completed-scale; does not establish closure.
//! Source-only sampled boundary: --leaf-boundary-coverage CONFIG CONFIG_SHA
//! NEW_ADJACENCY NEW_SELECTION NEW_REPORT. Uses the strict CoverageConfig shape,
//! schema borsuk-leaf-boundary-coverage-config-v1, max_payload_bytes <=512 MiB.
//! Adjacency is durable before requests; both nominations are durable before truth.
//! Source geometry: --source-neighborhood CONFIG CONFIG_SHA256. Strict source-only
//! v1 config owns new anchor_seal/report paths. An original successful native
//! terminal receipt is required; surviving report bytes never establish acceptance.
//! Query-free construction: --partition-reconstruction CONFIG CONFIG_SHA APPROVAL
//! APPROVAL_SHA. Independent approval pins original metadata; optional historical
//! radial parity requires actual regeneration and two literal root exceptions.
//! Transport split screen: --sq8-split-panel CONFIG CONFIG_SHA NEW_REPORT.
//! Strict v10 six-run fixed32 inputs; native provenance remains an external gate.
//! SOURCE-preserving v9 fixture: --prepare-v9-fixture CONFIG CONFIG_SHA NEW_DIR
//! NEW_REPORT. Rebinds proof metadata; no canonical publication or serving claim.
#![recursion_limit = "256"]

#[path = "compare_native_replay/f32_top16.rs"]
mod f32_top16;
#[path = "compare_native_replay/fetched_f32.rs"]
mod fetched_f32;
#[path = "compare_native_replay/sq8_cosine.rs"]
mod sq8_cosine;

#[path = "compare_native_replay/current_v10.rs"]
mod current_v10;
#[path = "compare_native_replay/source_frame_range.rs"]
mod source_frame_range;

#[path = "compare_native_replay/sq8_split_panel.rs"]
mod sq8_split_panel;

#[path = "compare_native_replay/sq8_panel_attribution.rs"]
mod sq8_panel_attribution;

#[path = "compare_native_replay/v9_fixture.rs"]
mod v9_fixture;

#[path = "compare_native_replay/v9_panel.rs"]
mod v9_panel;

#[path = "compare_native_replay/partition_reconstruction.rs"]
mod partition_reconstruction;

#[path = "compare_native_replay/paired_partition_coverage.rs"]
mod paired_partition_coverage;

#[path = "compare_native_replay/paired_returned_panel.rs"]
mod paired_returned_panel;

#[path = "compare_native_replay/unit_layout.rs"]
mod unit_layout;
#[path = "compare_native_replay/virtual_sq8.rs"]
mod virtual_sq8;

#[path = "compare_native_replay/sq8_subpage_shortlist.rs"]
mod sq8_subpage_shortlist;

// Compile the existing checked arithmetic here because cover_pages is crate-private.
#[allow(dead_code)]
#[path = "../src/budgeted_page_rank.rs"]
mod source_cover;

use rustix::fs::{Mode, OFlags, openat};
use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, IgnoredAny},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    error::Error,
    fs::File,
    io::{self, BufRead, BufReader, Read, Seek, Write},
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const COUNT: usize = 1000;
const K: usize = 10;
const FILE_CAP: u64 = 64 * 1024 * 1024;
const LINE_CAP: usize = 256 * 1024;
const REPORT_CAP: u64 = 2 * 1024 * 1024;
const LABELS: [&str; 4] = ["A1", "B1", "B2", "A2"];
// Fixed experiment pins from cohere1024/exact-sq8-runtime-preparation/
// paired-retained-replay-inputs.json, not a configurable reducer protocol.
const BINARY_SHA256: [&str; 2] = [
    "ce43842caeea9dbb722f3497b237265e71c7d829cbaf0243a81a2a352fcf1221",
    "3911839ba9ef68604e2c487d802a3b3e125bdc1121aee9af268db3ba8a9ca8cf",
];
// A identity: cohere1024/native-preflight-spot/a0002/preflight/baseline-result.jsonl.
// These five emitted source hashes also match B's qualified source9d540e52.
// Order: runner, generation, router, codec, source plane.
const COMPONENT_SOURCE_SHA256: [&str; 5] = [
    "15f06f8b28ec23c9cd29c850a50acfea0ebb889fa053f2da09ab8f69a8cf76b5",
    "70a1e6956e4d18eacccc1205c644cbaa5dbb1eb763a2958da666be60d17bc4e3",
    "b9abd271db66d45665304e72dc1a61b176fdbc47cd6f1a5618868a3c7443d0c2",
    "0f51015f31c08022b988ac00c40be534df8115e0aa54c482852bc51f0bb9ae60",
    "dbcc4cdbc4bb5c244354b375afd657f8b5cb1892df3f0ff0b8f41ef580de42e0",
];
// Same historical bound_inputs row: requests, truth, native source, SQ8, order.
// Native source identity is NOT the canonical payload object's SHA256.
const INPUT_SHA256: [&str; 5] = [
    "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e",
    "479064239b698a2b8094c7838b1bb01af6eff5ea6eee4736692971849fdcfb2c",
    "20936913f31e48ea67d462dfffc7a831569ff7467baebc63f2d622c8ff417dce",
    "07a14360b06add9a35f82b97f4e031690ff878ff497ee047902818823992337b",
    "6b6a67098330c76bfb89340065d9326fb3f23a1f6016fbae5b9fa7f0e29759c2",
];

fn require(ok: bool, message: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(message.into()) }
}

fn plus(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or_else(|| "sum overflow".into())
}

fn valid_sha(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

const COMPLETED_SCHEMA: &str = "borsuk-completed-native-reduction-v1";
const PAIRED_SCHEMA: &str = "borsuk-paired-native-reduction-v1";
const SCALE_PAIRED_SCHEMA: &str = "borsuk-scale-paired-native-reduction-v1";
const SCALE_SCHEDULER_SCHEMA: &str = "borsuk-scale-scheduler-native-reduction-v1";
const SCALE_WIDTH64_SCHEMA: &str = "borsuk-scale-width64-native-reduction-v1";
const SCALE_WIDTH128_SCHEMA: &str = "borsuk-scale-width128-native-reduction-v1";
const MEMBERSHIP_ABBA_SCHEMA: &str = "borsuk-membership-abba-native-reduction-v1";
const BYTE_WINDOW_ABBA_SCHEMA: &str = "borsuk-byte-window-abba-native-reduction-v1";
const SOURCE_BUDGET_ABBA_SCHEMA: &str = "borsuk-source-budget-abba-native-reduction-v1";
const CONFIG_CAP: u64 = 32 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CompletedInput {
    path: PathBuf,
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CompletedConfig {
    schema: String,
    input: CompletedInput,
    // Exact native rows, including phase, backend, and all source/input pins.
    expected_identity: Value,
    expected_bound_inputs: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PairedConfig {
    schema: String,
    arms: [CompletedConfig; 2],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ByteWindowAbbaConfig {
    schema: String,
    runs: [CompletedConfig; 4],
    runtime_configs: [CompletedInput; 2],
}

fn validate_byte_window(value: &Value, width: u64) -> Result<()> {
    validate_byte_window_cap(value, width, 512 * 1024 * 1024)
}

fn validate_byte_window_cap(value: &Value, width: u64, cap: u64) -> Result<()> {
    fields(value, "max_bytes max_waiters max_state_bytes")?;
    let bytes = value["max_bytes"]
        .as_u64()
        .ok_or("byte-window capacity integer")?;
    let waiters = value["max_waiters"]
        .as_u64()
        .ok_or("byte-window waiters integer")?;
    let state = value["max_state_bytes"]
        .as_u64()
        .ok_or("byte-window state integer")?;
    require(
        (1..=cap).contains(&bytes)
            && waiters >= width
            && (1..=cap).contains(&state)
            && usize::try_from(waiters)
                .ok()
                .and_then(borsuk::sq8_s3_range::InFlightBytes::modeled_state_bytes)
                .is_some_and(|bytes| bytes as u64 <= state),
        "byte-window explicit bounded capacity/waiters/state",
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MembershipAbbaConfig {
    schema: String,
    // Declared A1/B1/B2/A2 order; actual execution order is externally attested.
    runs: [CompletedConfig; 4],
    // Root freezes an attempt-specific config (including unique scratch_parent).
    runtime_config_sha256: String,
    // Control then candidate; provenance binding remains an external root gate.
    producer_source_commits: [String; 2],
    producer_source_archive_sha256: [String; 2],
}

// Value normally accepts duplicate keys. Reject them recursively before using
// exact expected rows or checking the v2 field roster (including opaque traces).
struct UniqueJson(Value);
impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueJson;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("JSON with unique object keys")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut map = serde_json::Map::new();
                while let Some((key, UniqueJson(value))) = a.next_entry::<String, UniqueJson>()? {
                    if map.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate JSON key"));
                    }
                }
                Ok(UniqueJson(Value::Object(map)))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(UniqueJson(v)) = a.next_element()? {
                    values.push(v);
                }
                Ok(UniqueJson(Value::Array(values)))
            }
            fn visit_bool<E: serde::de::Error>(
                self,
                v: bool,
            ) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(v.into()))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(v.into()))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(v.into()))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> std::result::Result<Self::Value, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| UniqueJson(Value::Number(n)))
                    .ok_or_else(|| E::custom("finite number"))
            }
            fn visit_str<E: serde::de::Error>(
                self,
                v: &str,
            ) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(v.into()))
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(Value::Null))
            }
        }
        d.deserialize_any(Visitor)
    }
}

fn fields(v: &Value, names: &str) -> Result<()> {
    let object = v.as_object().ok_or("expected JSON object")?;
    if object.len() != names.split_whitespace().count()
        || names
            .split_whitespace()
            .any(|name| !object.contains_key(name))
    {
        let missing = names
            .split_whitespace()
            .filter(|name| !object.contains_key(*name))
            .collect::<Vec<_>>();
        let extra = object
            .keys()
            .filter(|name| {
                !names
                    .split_whitespace()
                    .any(|expected| expected == name.as_str())
            })
            .collect::<Vec<_>>();
        return Err(
            format!("unknown/missing v2 field: missing={missing:?} extra={extra:?}").into(),
        );
    }
    Ok(())
}

fn v2_row_fields(phase: &str) -> Result<&'static str> {
    Ok(match phase {
        "identity" => {
            "phase schema config_sha256 binary_sha256 runner_source_sha256 generation_source_sha256 router_source_sha256 codec_source_sha256 source_plane_source_sha256 scope physical_s3_measured io_measurement s3_credential_source native_transport_includes wire_bytes unread_bytes billed_bytes billed_requests external_gate_required truth_opened"
        }
        "bound_inputs" => {
            "phase dataset revision metric tie_rule rows dimensions count k corpus_source_first query_source_first profile backend credential_source generation_prefix generation_root_sha256 requests_bytes requests_sha256 truth_bytes truth_sha256 native_source_sha256 native_sq8_sha256 native_order_sha256 truth_opened"
        }
        "source_binding" => "phase transport charges success truth_opened",
        "generation_open" => "phase transport success truth_opened",
        "startup" => {
            "phase metadata library_cap_bytes caller_pinned_bytes codec_scratch_bytes trace_scratch_bytes query_scratch_bytes truth_opened"
        }
        "query" => {
            "phase ordinal truth_opened returned returned_count underfill charges sum stages query_wall_ns query_process_cpu_ns trace transport"
        }
        "all_queries_sealed" => {
            "phase count truth_opened prefix_bytes prefix_sha256 requests_sha256 generation_root_sha256 requires_successful_sync requires_successful_directory_sync"
        }
        "recall" => "phase ordinal hits10 recall10 returned_count underfill",
        "terminal" => "phase summary",
        _ => return Err("unknown v2 phase".into()),
    })
}

fn terminal_fields() -> &'static str {
    "status complete queries k total_hits10 recall_numerator recall_denominator mean_recall10 underfilled_queries all_queries_sealed prefix_bytes prefix_sha256 sealed_bytes sealed_sha256 requests_sha256 truth_sha256 generation_root_sha256 charges sum query_wall_ns query_process_cpu_ns physical_s3_measured external_gate_required process_wall_ns process_cpu_ns observed_process_peak_bytes binding_charge transport_last_boundary"
}

fn validate_v2_row(line: &[u8], phase: &str) -> Result<()> {
    let UniqueJson(v) = serde_json::from_slice(line)?;
    let names = v2_row_fields(phase)?;
    if matches!(phase, "identity" | "bound_inputs") && v.get("fetch_parallelism").is_some() {
        require(
            matches!(v["fetch_parallelism"].as_u64(), Some(16 | 32 | 64 | 128)),
            "fetch_parallelism must be integer 16, 32, 64 or 128",
        )?;
        if phase == "bound_inputs" {
            fields(&v, &format!("{names} fetch_parallelism source_cache"))?;
            require(v["source_cache"] == "off", "source cache must be off")?;
        } else {
            fields(&v, &format!("{names} fetch_parallelism"))?;
        }
    } else {
        fields(&v, names)?;
    }
    if phase == "terminal" {
        fields(&v["summary"], terminal_fields())?;
    }
    validate_native_row_values(&v, phase)
}

// Version-neutral nested transport, startup, stage and trace checks. Each caller
// checks its complete row roster first; no current envelope is rewritten.
fn validate_native_row_values(v: &Value, phase: &str) -> Result<()> {
    validate_native_row_values_for_layout(v, phase, false)
}

fn validate_native_row_values_for_layout(
    v: &Value,
    phase: &str,
    source_layout: bool,
) -> Result<()> {
    if matches!(phase, "source_binding" | "generation_open" | "query") {
        fields(&v["transport"], "stage ordinal before after")?;
    }
    match phase {
        "source_binding" | "generation_open" => require(
            v["success"] == true && v["truth_opened"] == false,
            "successful pre-truth admission",
        )?,
        "startup" => {
            for name in [
                "library_cap_bytes",
                "caller_pinned_bytes",
                "codec_scratch_bytes",
                "trace_scratch_bytes",
                "query_scratch_bytes",
            ] {
                require(v[name].as_u64().is_some(), "startup byte count")?;
            }
            let m = &v["metadata"];
            fields(
                m,
                "metadata staging_wall_ns decode_wall_ns source_head_requests source_head_wall_ns router_head_requests router_head_wall_ns",
            )?;
            for (key, value) in m.as_object().ok_or("startup metadata")? {
                if key != "metadata" {
                    require(value.as_u64().is_some(), "startup timing/count")?;
                }
            }
            for entry in m["metadata"].as_array().ok_or("metadata roster")? {
                fields(
                    entry,
                    "name metadata_wave metadata_wave_wall_ns bytes reused_root_bytes retained_root_bytes local_auth_wall_ns local_copy_wall_ns chunks head_wall_ns logical_head_requests logical_get_requests payload_buffer_bound_bytes get_wall_ns stream_wall_ns write_wall_ns",
                )?;
                for (key, value) in entry.as_object().ok_or("metadata entry")? {
                    require(
                        if key == "name" {
                            value.as_str().is_some_and(|s| !s.is_empty())
                        } else {
                            value.as_u64().is_some()
                        },
                        "metadata name/counter",
                    )?;
                }
            }
        }
        "query" => {
            fields(
                &v["stages"],
                if source_layout && v["stages"].get("source_frames").is_some() {
                    "discovery source planning sq8 leaf_peak_inflight source_capsules source_frames"
                } else if source_layout {
                    "discovery source planning sq8 leaf_peak_inflight source_capsules"
                } else {
                    "discovery source planning sq8 leaf_peak_inflight"
                },
            )?;
            for stage in ["discovery", "source", "planning", "sq8"] {
                fields(&v["stages"][stage], "start_ns end_ns")?;
            }
            let t = &v["trace"];
            fields(
                t,
                "ranked_candidate_pages nomination_evaluated_units primary_page discoveries semantic_leaves semantic_units semantic_seed_additions",
            )?;
            require(t["primary_page"].as_u64().is_some(), "trace primary page")?;
            for key in [
                "ranked_candidate_pages",
                "nomination_evaluated_units",
                "semantic_leaves",
                "semantic_units",
                "semantic_seed_additions",
            ] {
                require(
                    t[key]
                        .as_array()
                        .is_some_and(|a| a.iter().all(|n| n.as_u64().is_some())),
                    "trace ordinals",
                )?;
            }
            for d in t["discoveries"].as_array().ok_or("trace discoveries")? {
                fields(
                    d,
                    "seed_page seed_evaluated_units walk_evaluated_units seed_work_exhausted walk_work_exhausted",
                )?;
                require(
                    d["seed_page"].as_u64().is_some()
                        && d["seed_work_exhausted"].is_boolean()
                        && d["walk_work_exhausted"].is_boolean(),
                    "trace discovery",
                )?;
                for key in ["seed_evaluated_units", "walk_evaluated_units"] {
                    require(
                        d[key]
                            .as_array()
                            .is_some_and(|a| a.iter().all(|n| n.as_u64().is_some())),
                        "discovery ordinals",
                    )?;
                }
            }
        }
        _ => (),
    }
    Ok(())
}

fn validate_v2_identity(i: &Identity, raw: &Value) -> Result<()> {
    validate_native_identity(i, raw, "borsuk-cohere-native-baseline-result-v2")
}

fn validate_native_identity(i: &Identity, raw: &Value, schema: &str) -> Result<()> {
    require(
        i.schema == schema
            && i.scope == "AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS"
            && i.io_measurement
                == "logical_GET_charges_separate_from_cumulative_process_native_transport"
            && !i.physical_s3_measured
            && i.external_gate_required
            && !i.truth_opened
            && raw["s3_credential_source"] == "imds_instance_role_only"
            && raw["native_transport_includes"] == "S3_and_IMDS_credential_requests_including_PUT",
        "v2 native identity/scope",
    )?;
    for pin in [
        &i.config_sha256,
        &i.binary_sha256,
        &i.runner_source_sha256,
        &i.generation_source_sha256,
        &i.router_source_sha256,
        &i.codec_source_sha256,
        &i.source_plane_source_sha256,
    ] {
        require(valid_sha(pin), "expected identity SHA256")?;
    }
    for key in [
        "wire_bytes",
        "unread_bytes",
        "billed_bytes",
        "billed_requests",
    ] {
        require(raw[key].is_null(), "unknown wire/billed accounting")?;
    }
    Ok(())
}

fn validate_v2_inputs(i: &Inputs, raw: &Value) -> Result<()> {
    require(
        !i.dataset.is_empty()
            && !i.revision.is_empty()
            && !i.profile.is_empty()
            && i.metric == "cosine"
            && i.tie_rule == "corpus_ordinal_ascending"
            && i.rows >= K
            && (1..=1024).contains(&i.dimensions)
            && i.count == COUNT
            && i.k == K
            && !i.truth_opened
            && i.requests_bytes == (COUNT * i.dimensions * 4) as u64
            && i.truth_bytes == (COUNT * K * 8) as u64
            && !i.generation_prefix.is_empty()
            && raw["credential_source"] == "imds_instance_role_only",
        "v2 expected population/geometry",
    )?;
    for pin in [
        &i.generation_root_sha256,
        &i.requests_sha256,
        &i.truth_sha256,
        &i.native_source_sha256,
        &i.native_sq8_sha256,
        &i.native_order_sha256,
    ] {
        require(valid_sha(pin), "expected input/root SHA256")?;
    }
    let backend = &raw["backend"];
    fields(
        backend,
        "kind bucket region physical_prefix sq8_object_key sq8_etag",
    )?;
    require(backend["kind"] == "s3", "completed v2 S3 backend")?;
    for key in [
        "bucket",
        "region",
        "physical_prefix",
        "sq8_object_key",
        "sq8_etag",
    ] {
        require(
            backend[key]
                .as_str()
                .is_some_and(|s| !s.is_empty() && s.len() <= 512),
            "bound S3 descriptor",
        )?;
    }
    require(
        backend["sq8_object_key"]
            .as_str()
            .is_some_and(|s| s.ends_with(&format!("/objects/{}", i.native_sq8_sha256))),
        "bound SQ8 key/hash",
    )
}

const SCALE_CONFIG_SCHEMA: &str = "borsuk-completed-native-reduction-config-v4";
const SCALE_REPORT_SCHEMA: &str = "borsuk-completed-native-reduction-v4";

fn scale_ordinals(raw: &Value, count: usize) -> Result<Vec<usize>> {
    require(
        (1..=1000).contains(&count),
        "bounded query population before allocation",
    )?;
    let execution = &raw["execution"];
    match execution["mode"].as_str() {
        Some("full") => {
            fields(execution, "mode")?;
            Ok((0..count).collect())
        }
        Some("diagnostic_panel") => {
            fields(execution, "mode ordinals trace")?;
            require(
                execution["trace"] == false,
                "scale reducer requires untraced baseline execution",
            )?;
            let ordinals = execution["ordinals"]
                .as_array()
                .ok_or("explicit diagnostic ordinals")?
                .iter()
                .map(|n| usize::try_from(n.as_u64().ok_or("integer ordinal")?).map_err(Into::into))
                .collect::<Result<Vec<_>>>()?;
            require(
                (1..=128).contains(&ordinals.len())
                    && ordinals.windows(2).all(|a| a[0] < a[1])
                    && ordinals.last().is_some_and(|n| *n < count),
                "bounded unique diagnostic ordinals",
            )?;
            Ok(ordinals)
        }
        _ => Err("explicit full/diagnostic execution required".into()),
    }
}

fn validate_scale_inputs(i: &Inputs, raw: &Value) -> Result<()> {
    validate_scale_inputs_for_version(i, raw, false)
}

fn validate_scale_inputs_for_version(i: &Inputs, raw: &Value, current: bool) -> Result<()> {
    validate_scale_input_values(i, raw, current, false)
}

fn validate_current_scale_inputs(i: &Inputs, raw: &Value) -> Result<()> {
    validate_scale_input_values(i, raw, true, true)
}

fn validate_scale_input_values(i: &Inputs, raw: &Value, current: bool, v10: bool) -> Result<()> {
    let exact_fp32 = fp32_serving::enabled(raw);
    if exact_fp32 {
        require(
            current && v10 && raw["config_schema"] == current_v10::FRAMED_CONFIG_SCHEMA,
            "FP32 current config binding",
        )?;
        fp32_serving::limits(raw)?;
    }

    require(
        i.dataset == "CohereLabs/wikipedia-2023-11-embed-multilingual-v3"
            && i.revision == "ade45fb52bd549f5e8c065636fe4160a43c2af36"
            && i.metric == "cosine"
            && i.tie_rule == "corpus_ordinal_ascending"
            && !i.truth_opened
            && (K..=1_000_000).contains(&i.rows)
            && (if v10 {
                (1..=1024).contains(&i.dimensions)
            } else {
                i.dimensions == 1024
            })
            && i.k == K
            && (1..=1000).contains(&i.count)
            && matches!(i.profile.as_str(), "native100k" | "scale1m")
            && (i.profile != "native100k" || i.rows <= 100_000)
            && i.requests_bytes == i.count as u64 * i.dimensions as u64 * 4
            && i.truth_bytes == i.count as u64 * K as u64 * 8
            && (raw["serving"] == json!({"mode":"baseline"}) || exact_fp32)
            && raw["source_cache"] == "off"
            && (if current_v10::is_source_layout_config(raw) {
                raw["max_memory_bytes"]
                    .as_u64()
                    .is_some_and(|n| n > 0 && usize::try_from(n).is_ok())
            } else {
                raw["max_memory_bytes"] == 512 * 1024 * 1024_u64
            }),
        "explicit scale input geometry/profile/cap",
    )?;
    fields(&raw["reserved_query_interval"], "start end")?;
    let reserved_start = raw["reserved_query_interval"]["start"]
        .as_u64()
        .ok_or("reserved start")?;
    let reserved_end = raw["reserved_query_interval"]["end"]
        .as_u64()
        .ok_or("reserved end")?;
    require(
        reserved_start < reserved_end
            && reserved_end <= 1_001_000
            && reserved_end - reserved_start <= 1000
            && i.query_source_first as u64 == reserved_start
            && (i.count as u64)
                .checked_add(reserved_start)
                .is_some_and(|end| end <= reserved_end),
        "selected prefix within full reserved interval",
    )?;
    let reserved_sha = raw["reserved_queries_sha256"]
        .as_str()
        .ok_or("reserved query SHA")?;
    require(
        valid_sha(reserved_sha)
            && (cfg!(test)
                || v10
                || reserved_sha
                    == "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e"),
        "full reserved query digest",
    )?;
    require(
        i.count as u64 != reserved_end - reserved_start || i.requests_sha256 == reserved_sha,
        "full selected request/reserved query digest binding",
    )?;
    let intervals = raw["corpus_intervals"]
        .as_array()
        .ok_or("explicit corpus intervals")?;
    require(
        !intervals.is_empty() && intervals.len() <= 2,
        "bounded corpus intervals",
    )?;
    let mut previous = 0;
    let mut count = 0_u64;
    for interval in intervals {
        fields(interval, "start end")?;
        let start = interval["start"].as_u64().ok_or("interval start")?;
        let end = interval["end"].as_u64().ok_or("interval end")?;
        require(
            start < end
                && start >= previous
                && end <= 1_001_000
                && (end <= reserved_start || start >= reserved_end),
            "ordered excluded-query intervals",
        )?;
        count = plus(count, end - start)?;
        previous = end;
    }
    require(
        count == i.rows as u64 && i.corpus_source_first == 0,
        "exact corpus count",
    )?;
    if !cfg!(test) && !v10 {
        let expected = if i.rows == 100_000 {
            json!([{"start":0,"end":100_000}])
        } else {
            json!([{"start":0,"end":100_000},{"start":101_000,"end":i.rows+1000}])
        };
        require(
            i.rows >= 100_000
                && reserved_start == 100_000
                && reserved_end == 101_000
                && raw["corpus_intervals"] == expected,
            "historical query exclusion",
        )?;
    }
    let ordinals = if current {
        v9_panel::ordinals(raw, i.count)?
    } else {
        scale_ordinals(raw, i.count)?
    };
    require(
        raw["selected_count"] == ordinals.len(),
        "explicit selected query count",
    )?;
    for pin in [
        &i.generation_root_sha256,
        &i.requests_sha256,
        &i.truth_sha256,
        &i.native_source_sha256,
        &i.native_sq8_sha256,
        &i.native_order_sha256,
    ] {
        require(valid_sha(pin), "scale input/root SHA256")?;
    }
    for key in ["cohort_receipt_sha256", "derivation_receipt_sha256"] {
        require(
            raw[key].as_str().is_some_and(valid_sha),
            "original/derived receipt SHA256",
        )?;
    }
    let authority = &raw["producer_authority"];
    fields(
        authority,
        "source_commit executable_sha256 producer_source_sha256 sq8_source_sha256 source_order_source_sha256",
    )?;
    require(
        authority["source_commit"].as_str().is_some_and(|s| {
            s.len() == 40
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }),
        "producer source commit",
    )?;
    for key in [
        "executable_sha256",
        "producer_source_sha256",
        "sq8_source_sha256",
        "source_order_source_sha256",
    ] {
        require(
            authority[key].as_str().is_some_and(valid_sha),
            "root-frozen producer SHA256",
        )?;
    }
    require(
        !i.generation_prefix.is_empty() && i.generation_prefix.len() <= 512,
        "generation prefix",
    )?;
    match raw["backend"]["kind"].as_str() {
        Some("local") => {
            fields(&raw["backend"], "kind store_root")?;
            require(
                raw["credential_source"].is_null()
                    && raw["backend"]["store_root"]
                        .as_str()
                        .is_some_and(|p| Path::new(p).is_absolute() && p.len() <= 4096),
                "local backend descriptor",
            )?;
        }
        Some("s3") => {
            fields(
                &raw["backend"],
                "kind bucket region physical_prefix sq8_object_key sq8_etag",
            )?;
            require(
                raw["credential_source"] == "imds_instance_role_only",
                "S3 credential source",
            )?;
            for key in [
                "bucket",
                "region",
                "physical_prefix",
                "sq8_object_key",
                "sq8_etag",
            ] {
                require(
                    raw["backend"][key]
                        .as_str()
                        .is_some_and(|s| !s.is_empty() && s.len() <= 512),
                    "S3 descriptor",
                )?;
            }
            require(
                raw["backend"]["sq8_object_key"]
                    .as_str()
                    .is_some_and(|s| s.ends_with(&format!("/objects/{}", i.native_sq8_sha256))),
                "SQ8 key/SHA",
            )?;
        }
        _ => return Err("scale backend required".into()),
    }
    Ok(())
}

fn validate_scale_row(line: &[u8], phase: &str) -> Result<()> {
    let UniqueJson(mut raw) = serde_json::from_slice(line)?;
    if matches!(phase, "identity" | "bound_inputs") {
        let budget = raw
            .as_object_mut()
            .ok_or("scale row object")?
            .remove("source_get_budget");
        if phase == "identity" {
            require(
                (raw["schema"] == "borsuk-cohere-native-baseline-result-v8") == budget.is_some(),
                "v8 identity requires explicit SOURCE budget; historical identity forbids it",
            )?;
        }
        if let Some(budget) = budget {
            require(
                matches!(budget.as_u64(), Some(96 | 128)),
                "explicit SOURCE budget must be integer96 or128",
            )?;
        }
    }
    if matches!(phase, "identity" | "bound_inputs") {
        if let Some(window) = raw
            .as_object_mut()
            .ok_or("scale row object")?
            .remove("shared_byte_window")
        {
            validate_byte_window(
                &window,
                raw["fetch_parallelism"]
                    .as_u64()
                    .ok_or("byte-window width")?,
            )?;
        }
    }
    if phase == "startup" {
        if let Some(state) = raw
            .as_object_mut()
            .ok_or("startup object")?
            .remove("shared_byte_window_state_bytes")
        {
            require(
                state
                    .as_u64()
                    .is_some_and(|n| (1..=512 * 1024 * 1024).contains(&n)),
                "byte-window allocated state",
            )?;
        }
    }
    if phase == "diagnostic_admission" {
        fields(
            &raw,
            "phase execution selected_count population_count trace diagnostic_bytes_per_active_query diagnostic_cap_bytes max_active_queries charged_in_caller_pinned_bytes caller_pinned_bytes panel_line_cap_bytes host_read_cap_bytes trace_ranges population_percentiles_valid full_cohort_qualification semantics truth_opened trace_retained_bytes trace_peak_bytes diagnostic_pinned_bytes trace_peak_charged_by_library_at_traced_admission range_state_pinned_by_runner_bytes range_state_both_paths_bytes",
        )?;
        require(
            raw["trace"] == false
                && raw["truth_opened"] == false
                && raw["max_active_queries"] == 1
                && raw["charged_in_caller_pinned_bytes"] == true
                && raw["population_percentiles_valid"] == false
                && raw["full_cohort_qualification"] == false,
            "untraced diagnostic admission scope",
        )?;
        return Ok(());
    }
    let extras: &[&str] = match phase {
        "identity" => &[
            "serving",
            "execution",
            "sq8_range_source_sha256",
            "returned_source_sha256",
        ],
        "bound_inputs" => &[
            "serving",
            "execution",
            "selected_count",
            "corpus_intervals",
            "reserved_query_interval",
            "reserved_queries_sha256",
            "cohort_receipt_sha256",
            "derivation_receipt_sha256",
            "producer_authority",
            "max_memory_bytes",
        ],
        "startup" => &["serving"],
        "query" => &[
            "serving",
            "source_nomination_skipped",
            "planning_scope",
            "plan",
        ],
        "all_queries_sealed" => &["selected_count", "population_count", "reserved_query_count"],
        "terminal" => &[],
        _ => &[],
    };
    if matches!(phase, "identity" | "bound_inputs" | "startup" | "query") {
        require(
            raw["serving"] == json!({"mode":"baseline"}),
            "scale baseline serving required",
        )?;
    }
    if phase == "identity" {
        require(
            matches!(
                raw["schema"].as_str(),
                Some(
                    "borsuk-cohere-native-baseline-result-v7"
                        | "borsuk-cohere-native-baseline-result-v8"
                )
            ),
            "scale result v7/v8 required",
        )?;
        for key in ["sq8_range_source_sha256", "returned_source_sha256"] {
            require(
                raw[key].as_str().is_some_and(valid_sha),
                "runner component SHA256",
            )?;
        }
    }
    if phase == "query" {
        require(
            raw["source_nomination_skipped"] == false
                && raw["planning_scope"] == "source_nomination_and_cover",
            "baseline native planning",
        )?;
        fields(
            &raw["plan"],
            "selected_pages ranges planned_bytes target_pages target_shortfall primary_pages_retained covered_pages bridge_pages",
        )?;
        for key in [
            "planned_bytes",
            "target_pages",
            "target_shortfall",
            "primary_pages_retained",
            "covered_pages",
            "bridge_pages",
        ] {
            require(raw["plan"][key].as_u64().is_some(), "native plan integer")?;
        }
        let pages = raw["plan"]["selected_pages"]
            .as_array()
            .ok_or("native plan pages")?;
        require(
            pages.iter().all(|p| p.as_u64().is_some()),
            "native page ordinal",
        )?;
        let mut end = 0;
        let mut bytes = 0;
        for range in raw["plan"]["ranges"].as_array().ok_or("native ranges")? {
            let pair = range.as_array().ok_or("native range pair")?;
            require(pair.len() == 2, "native range pair length")?;
            let start = pair[0].as_u64().ok_or("native range start")?;
            let next = pair[1].as_u64().ok_or("native range end")?;
            require(
                start >= end && next > start,
                "ordered nonoverlapping native ranges",
            )?;
            bytes = plus(bytes, next - start)?;
            end = next;
        }
        require(
            raw["plan"]["planned_bytes"] == bytes && bytes <= 16_773_120,
            "native plan byte total/cap",
        )?;
        if let Some(slot) = raw
            .as_object_mut()
            .ok_or("query object")?
            .remove("selected_slot")
        {
            require(slot.as_u64().is_some(), "selected slot integer")?;
        }
    }
    if phase == "terminal" {
        let t = raw["summary"].as_object_mut().ok_or("terminal object")?;
        for name in [
            "serving",
            "direct_memory",
            "execution",
            "selected_count",
            "executed_count",
            "population_count",
            "diagnostic_panel",
            "reserved_query_count",
            "diagnostic_prefix",
        ] {
            require(t.remove(name).is_some(), "scale terminal field required")?;
        }
        for name in [
            "population_percentiles_valid",
            "full_cohort_qualification",
            "scope_note",
        ] {
            t.remove(name);
        }
    }
    for name in extras {
        require(
            raw.as_object_mut()
                .ok_or("scale row object")?
                .remove(*name)
                .is_some(),
            "scale row field required",
        )?;
    }
    validate_v2_row(&serde_json::to_vec(&raw)?, phase)
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct TransportStats {
    attempts: u64,
    method_counts: [u64; 10],
    status_counts: Vec<(u16, u64)>,
    transport_failures: u64,
    stream_failures: u64,
    consumed_payload_bytes: u64,
    dropped_error_bodies: u64,
}

impl TransportStats {
    fn validate(&self) -> Result<()> {
        require(
            self.method_counts.iter().try_fold(0, |n, &v| plus(n, v))? == self.attempts
                && self.status_counts.len() <= 900
                && self
                    .status_counts
                    .iter()
                    .all(|&(code, n)| (100..=999).contains(&code) && n > 0)
                && self.status_counts.windows(2).all(|a| a[0].0 < a[1].0)
                && plus(
                    self.status_counts
                        .iter()
                        .try_fold(0, |n, &(_, v)| plus(n, v))?,
                    self.transport_failures,
                )? == self.attempts
                && self.stream_failures <= self.attempts
                && self
                    .status_counts
                    .iter()
                    .filter(|(code, _)| !(200..300).contains(code))
                    .try_fold(0, |n, &(_, v)| plus(n, v))?
                    == self.dropped_error_bodies,
            "cumulative SDK counter consistency",
        )
    }
    fn follows(&self, before: &Self) -> Result<()> {
        require(
            self.attempts >= before.attempts
                && self.consumed_payload_bytes >= before.consumed_payload_bytes
                && self.transport_failures >= before.transport_failures
                && self.stream_failures >= before.stream_failures
                && self.dropped_error_bodies >= before.dropped_error_bodies
                && self
                    .method_counts
                    .iter()
                    .zip(before.method_counts)
                    .all(|(&a, b)| a >= b)
                && before
                    .status_counts
                    .iter()
                    .all(|&(code, n)| self.status_counts.iter().any(|&(c, v)| c == code && v >= n)),
            "monotonic cumulative SDK counters",
        )
    }
    fn compact(&self) -> Value {
        let mut v = serde_json::to_value(self).expect("integer transport serialization");
        v["status_counts"] = Value::Null;
        v["status_counts_entries"] = json!(self.status_counts.len());
        v
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransportSpan {
    stage: String,
    ordinal: Option<usize>,
    before: TransportStats,
    after: TransportStats,
}
impl TransportSpan {
    fn compact(&self) -> Value {
        json!({"stage":self.stage,"ordinal":self.ordinal,"before":self.before.compact(),
            "after":self.after.compact(),"scope":"cumulative_process_native_transport",
            "status_counts_omitted_from_terminal":true,"wire_bytes":null,"unread_bytes":null,
            "billed_bytes":null,"billed_requests":null})
    }
}

struct CompletedEvidence {
    binding_charge: Charge,
    generation_authority: Value,
    last: Option<TransportSpan>,
    fp32_head_attempt_cap: Option<u64>,
    stage_samples: [Vec<u64>; 4],
    startup: Value,
    terminal: Value,
    local_boundary: Value,
}
impl CompletedEvidence {
    fn new(binding_charge: Charge) -> Self {
        Self {
            binding_charge,
            generation_authority: Value::Null,
            last: None,
            fp32_head_attempt_cap: None,
            stage_samples: std::array::from_fn(|_| Vec::with_capacity(COUNT)),
            startup: Value::Null,
            terminal: Value::Null,
            local_boundary: Value::Null,
        }
    }
    fn transport_scale(
        &mut self,
        line: &[u8],
        stage: &str,
        ordinal: Option<usize>,
        local: bool,
    ) -> Result<()> {
        if !local {
            return self.transport(line, stage, ordinal);
        }
        let raw: Value = serde_json::from_slice(line)?;
        let span = &raw["transport"];
        fields(span, "stage ordinal before after")?;
        require(
            span["stage"] == stage
                && span["ordinal"] == json!(ordinal)
                && span["before"].is_null()
                && span["after"].is_null(),
            "local transport boundary",
        )?;
        let mut compact = span.clone();
        compact["scope"] = json!("cumulative_process_native_transport");
        compact["status_counts_omitted_from_terminal"] = json!(true);
        for key in [
            "wire_bytes",
            "unread_bytes",
            "billed_bytes",
            "billed_requests",
        ] {
            compact[key] = Value::Null;
        }
        self.local_boundary = compact;
        Ok(())
    }
    fn transport(&mut self, line: &[u8], stage: &str, ordinal: Option<usize>) -> Result<()> {
        #[derive(Deserialize)]
        struct Record {
            transport: TransportSpan,
        }
        let span = serde_json::from_slice::<Record>(line)?.transport;
        require(
            span.stage == stage && span.ordinal == ordinal,
            "transport stage/ordinal",
        )?;
        span.before.validate()?;
        span.after.validate()?;
        span.after.follows(&span.before)?;
        if let Some(last) = &self.last {
            span.before.follows(&last.after)?;
            if self.fp32_head_attempt_cap.is_some() {
                require(span.before == last.after, "FP32 exact serial transport continuity")?;
            }
        }
        if stage == "canonical_open" {
            let cap = self.fp32_head_attempt_cap.ok_or("FP32 HEAD policy")?;
            let heads = span.after.method_counts[1] - span.before.method_counts[1];
            let successes = |stats: &TransportStats| -> Result<u64> {
                stats.status_counts.iter().filter(|(code, _)| (200..300).contains(code))
                    .try_fold(0_u64, |n, (_, count)| plus(n, *count))
            };
            let successful_heads = successes(&span.after)? - successes(&span.before)?;
            // The preceding open resolves credentials; refresh traffic cannot certify an isolated HEAD.
            require((1..=cap).contains(&heads)
                && successful_heads == 1
                && span.after.attempts - span.before.attempts == heads
                && span.after.method_counts.iter().zip(span.before.method_counts).enumerate()
                    .all(|(i, (&a,b))| i == 1 || a == b)
                && span.after.consumed_payload_bytes == span.before.consumed_payload_bytes
                && span.after.stream_failures == span.before.stream_failures,
                "FP32 isolated canonical HEAD transport")?;
        }
        self.last = Some(span);
        Ok(())
    }
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
struct Identity {
    schema: String,
    config_sha256: String,
    binary_sha256: String,
    runner_source_sha256: String,
    generation_source_sha256: String,
    router_source_sha256: String,
    codec_source_sha256: String,
    source_plane_source_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    semantic_leaf_limit: Option<usize>,
    scope: String,
    physical_s3_measured: bool,
    io_measurement: String,
    external_gate_required: bool,
    truth_opened: bool,
}

impl Identity {
    fn validate(&self) -> Result<()> {
        require(
            self.schema == "borsuk-cohere-native-baseline-result-v1"
                && self.scope == "LOCAL_AUTHENTICATED_FILE_QUALITY_CORRECTNESS"
                && self.io_measurement
                    == "logical_object_store_GETs_and_authenticated_payload_bytes"
                && !self.physical_s3_measured
                && self.external_gate_required
                && !self.truth_opened,
            "native identity/schema/scope",
        )?;
        require(
            valid_sha(&self.config_sha256)
                && valid_sha(&self.binary_sha256)
                && [
                    self.runner_source_sha256.as_str(),
                    self.generation_source_sha256.as_str(),
                    self.router_source_sha256.as_str(),
                    self.codec_source_sha256.as_str(),
                    self.source_plane_source_sha256.as_str(),
                ] == COMPONENT_SOURCE_SHA256,
            "identity SHA256/frozen component sources",
        )
    }
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
struct Inputs {
    dataset: String,
    revision: String,
    metric: String,
    tie_rule: String,
    rows: usize,
    dimensions: usize,
    count: usize,
    k: usize,
    corpus_source_first: usize,
    query_source_first: usize,
    profile: String,
    #[serde(default)]
    store_root: String,
    generation_prefix: String,
    generation_root_sha256: String,
    requests_bytes: u64,
    requests_sha256: String,
    truth_bytes: u64,
    truth_sha256: String,
    native_source_sha256: String,
    native_sq8_sha256: String,
    native_order_sha256: String,
    truth_opened: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_get_budget: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    semantic_leaf_limit: Option<usize>,
}

impl Inputs {
    fn validate(&self) -> Result<()> {
        require(
            self.dataset == "CohereLabs/wikipedia-2023-11-embed-multilingual-v3"
                && self.revision == "ade45fb52bd549f5e8c065636fe4160a43c2af36"
                && self.metric == "cosine"
                && self.tie_rule == "corpus_ordinal_ascending"
                && self.rows == 100000
                && self.dimensions == 1024
                && self.count == COUNT
                && self.k == K
                && self.corpus_source_first == 0
                && self.query_source_first == 100000
                && self.profile == "native100k"
                && !self.truth_opened
                && self.requests_bytes == 4096000
                && self.truth_bytes == 80000,
            "fixed retained replay population/geometry/metric",
        )?;
        require(
            Path::new(&self.store_root).is_absolute()
                && self.store_root.len() <= 4096
                && !self.generation_prefix.is_empty()
                && self.generation_prefix.len() <= 512
                && self.generation_prefix.split('/').all(|s| {
                    !s.is_empty()
                        && s != "."
                        && s != ".."
                        && s.bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
                }),
            "bound store/root path",
        )?;
        require(
            valid_sha(&self.generation_root_sha256)
                && [
                    self.requests_sha256.as_str(),
                    self.truth_sha256.as_str(),
                    self.native_source_sha256.as_str(),
                    self.native_sq8_sha256.as_str(),
                    self.native_order_sha256.as_str(),
                ] == INPUT_SHA256,
            "bound root SHA256/frozen retained input pins",
        )
    }
}

#[derive(Default, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Charge {
    submitted_gets: u64,
    verified_bytes: u64,
    failed_gets: u64,
}

impl Charge {
    fn validate(self) -> Result<()> {
        require(
            self.failed_gets <= self.submitted_gets,
            "logical failed GETs exceed submitted GETs",
        )
    }

    fn add(&mut self, other: Self) -> Result<()> {
        self.submitted_gets = plus(self.submitted_gets, other.submitted_gets)?;
        self.verified_bytes = plus(self.verified_bytes, other.verified_bytes)?;
        self.failed_gets = plus(self.failed_gets, other.failed_gets)?;
        Ok(())
    }
}

#[derive(Default, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Charges {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    canonical: Option<Charge>,
    router: Charge,
    source: Charge,
    sq8: Charge,
}

impl Charges {
    fn add(&mut self, other: Self) -> Result<()> {
        self.router.add(other.router)?;
        self.source.add(other.source)?;
        self.sq8.add(other.sq8)?;
        if let Some(canonical) = other.canonical {
            self.canonical
                .get_or_insert_with(Charge::default)
                .add(canonical)?;
        }
        Ok(())
    }

    fn sum(self) -> Result<Charge> {
        let mut sum = self.router;
        sum.add(self.source)?;
        sum.add(self.sq8)?;
        if let Some(canonical) = self.canonical {
            sum.add(canonical)?;
        }
        Ok(sum)
    }
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Hit {
    id: u64,
    score_bits: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Interval {
    start_ns: u64,
    end_ns: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Stages {
    discovery: Interval,
    source: Interval,
    planning: Interval,
    sq8: Interval,
    leaf_peak_inflight: u64,
    #[serde(default, rename = "source_capsules")]
    _source_capsules: Option<IgnoredAny>,
    #[serde(default, rename = "source_frames")]
    _source_frames: Option<IgnoredAny>,
}

#[derive(Default, Serialize)]
struct StageTotals {
    discovery_ns: u64,
    source_ns: u64,
    planning_ns: u64,
    sq8_ns: u64,
    max_leaf_peak_inflight: u64,
}

impl StageTotals {
    fn add(&mut self, s: &Stages, wall_ns: u64, source_skipped: bool) -> Result<()> {
        require(
            (if source_skipped {
                s.source.start_ns == 0 && s.source.end_ns == 0
                    && s.discovery.end_ns <= s.planning.start_ns
            } else {
                s.discovery.end_ns <= s.source.start_ns && s.source.end_ns <= s.planning.start_ns
            })
                && s.planning.end_ns <= s.sq8.start_ns
                && s.sq8.end_ns <= wall_ns,
            "stage order/query wall",
        )?;
        for (index, (sum, interval)) in [
            (&mut self.discovery_ns, &s.discovery),
            (&mut self.source_ns, &s.source),
            (&mut self.planning_ns, &s.planning),
            (&mut self.sq8_ns, &s.sq8),
        ].into_iter().enumerate() {
            if source_skipped && index == 1 { continue; }
            require(
                interval.start_ns > 0 && interval.start_ns <= interval.end_ns,
                "nonzero admitted stage interval",
            )?;
            *sum = plus(*sum, interval.end_ns - interval.start_ns)?;
        }
        self.max_leaf_peak_inflight = self.max_leaf_peak_inflight.max(s.leaf_peak_inflight);
        Ok(())
    }
}

// Trace and unneeded native fields are consumed without constructing Value trees.
#[derive(Deserialize)]
struct Query {
    ordinal: usize,
    truth_opened: bool,
    returned: Vec<Hit>,
    returned_count: usize,
    underfill: bool,
    charges: Charges,
    sum: Charge,
    stages: Stages,
    query_wall_ns: u64,
    query_process_cpu_ns: Option<u64>,
    #[serde(rename = "trace")]
    _trace: IgnoredAny,
}

#[derive(Deserialize)]
struct Seal {
    count: usize,
    truth_opened: bool,
    prefix_bytes: u64,
    prefix_sha256: String,
    requests_sha256: String,
    generation_root_sha256: String,
    requires_successful_sync: bool,
    requires_successful_directory_sync: bool,
}

#[derive(Deserialize)]
struct Recall {
    ordinal: usize,
    hits10: u64,
    recall10: f64,
    returned_count: usize,
    underfill: bool,
}

#[derive(Deserialize, Serialize)]
struct Terminal {
    status: String,
    complete: bool,
    queries: usize,
    k: usize,
    total_hits10: u64,
    recall_numerator: u64,
    recall_denominator: u64,
    mean_recall10: f64,
    underfilled_queries: usize,
    all_queries_sealed: bool,
    prefix_bytes: u64,
    prefix_sha256: String,
    sealed_bytes: u64,
    sealed_sha256: String,
    requests_sha256: String,
    truth_sha256: String,
    generation_root_sha256: String,
    charges: Charges,
    sum: Charge,
    query_wall_ns: u64,
    query_process_cpu_ns: u64,
    physical_s3_measured: bool,
    external_gate_required: bool,
    process_wall_ns: u64,
    process_cpu_ns: u64,
    observed_process_peak_bytes: Option<u64>,
}

struct Sample {
    returned: Vec<Hit>,
    charges: Charges,
    hits10: u64,
    wall_ns: u64,
    trace_sha256: [u8; 32],
    physical_plan_sha256: [u8; 32],
}

struct Run {
    identity: Identity,
    inputs: Inputs,
    samples: Vec<Sample>,
    stages: StageTotals,
    terminal: Terminal,
    file_identity: FileIdentity,
    completed: Option<CompletedEvidence>,
}

#[derive(Debug, PartialEq, Eq)]
struct FileIdentity {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}

fn file_identity(file: &File) -> Result<FileIdentity> {
    let m = file.metadata()?;
    require(m.is_file(), "regular result file required")?;
    Ok(FileIdentity {
        dev: m.dev(),
        ino: m.ino(),
        len: m.len(),
        mtime: (m.mtime(), m.mtime_nsec()),
        ctime: (m.ctime(), m.ctime_nsec()),
    })
}

// Same descriptor-anchored no-symlink traversal as prepare_cohere_native_cohort.
fn directory(path: &Path) -> Result<File> {
    require(path.as_os_str().len() <= 4096, "bounded path")?;
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let flags =
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
    let mut dir = File::from(rustix::fs::open("/", flags, Mode::empty())?);
    for component in absolute.components() {
        match component {
            Component::RootDir | Component::CurDir => (),
            Component::Normal(name) => dir = File::from(openat(&dir, name, flags, Mode::empty())?),
            _ => return Err("directory traversal component".into()),
        }
    }
    Ok(dir)
}

fn parent(path: &Path) -> Result<File> {
    require(
        path.as_os_str().len() <= 4096 && path.file_name().is_some(),
        "bounded file path",
    )?;
    directory(path.parent().unwrap_or(Path::new(".")))
}

fn open_input(path: &Path) -> Result<File> {
    let dir = parent(path)?;
    Ok(File::from(openat(
        &dir,
        path.file_name().ok_or("filename")?,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadPolicy {
    Existing,
    Sq8PanelAttribution,
    Sq8SplitPanel,
}

struct Rows {
    reader: BufReader<File>,
    line: Vec<u8>,
    bytes: u64,
    digest: Sha256,
    original: FileIdentity,
    v2: bool,
    scale: bool,
    current: bool,
    v10: bool,
    library_scheduler: bool,
    source_layout: bool,
    cpu_profile: bool,
    concurrent: bool,
    policy: ReadPolicy,
    line_cap: usize,
}

impl Rows {
    fn open(path: &Path) -> Result<Self> {
        let file = open_input(path)?;
        let original = file_identity(&file)?;
        require(
            original.len > 0 && original.len <= FILE_CAP,
            "result file byte cap",
        )?;
        Ok(Self {
            reader: BufReader::with_capacity(8192, file),
            line: Vec::with_capacity(LINE_CAP + 1),
            bytes: 0,
            digest: Sha256::new(),
            original,
            v2: false,
            scale: false,
            current: false,
            v10: false,
            library_scheduler: false,
            source_layout: false,
            cpu_profile: false,
            concurrent: false,
            policy: ReadPolicy::Existing,
            line_cap: LINE_CAP,
        })
    }

    fn sha(&self) -> String {
        format!("{:x}", self.digest.clone().finalize())
    }

    fn row<T: DeserializeOwned>(&mut self, phase: &str) -> Result<T> {
        self.line.clear();
        let cap = if self.source_layout && phase == "source_binding" {
            current_v10::SOURCE_BINDING_LINE_CAP
        } else {
            self.line_cap
        };
        let n = Read::take(&mut self.reader, (cap + 1) as u64).read_until(b'\n', &mut self.line)?;
        self.bytes = plus(self.bytes, n as u64)?;
        require(
            n > 0
                && n <= cap
                && self.line.last() == Some(&b'\n')
                && self.bytes <= self.original.len,
            "row cap/truncation/file length",
        )?;
        #[derive(Deserialize)]
        struct Phase {
            phase: String,
        }
        let header: Phase = serde_json::from_slice(&self.line)?;
        require(
            header.phase == phase,
            &format!("expected {phase}, got {}", header.phase),
        )?;
        if self.concurrent {
            current_v10::validate_concurrent_row(&self.line, phase)?;
        } else if self.policy == ReadPolicy::Sq8SplitPanel {
            sq8_split_panel::validate_row(&self.line, phase)?;
        } else if self.v10 {
            current_v10::validate_row_envelope(
                &self.line,
                phase,
                self.library_scheduler,
                self.source_layout,
                self.cpu_profile,
            )?;
        } else if self.policy == ReadPolicy::Sq8PanelAttribution && !self.current {
            sq8_panel_attribution::validate_v8_row(&self.line, phase)?;
        } else if self.current {
            v9_panel::validate_row(&self.line, phase)?;
        } else if self.scale {
            validate_scale_row(&self.line, phase)?;
        } else if self.v2 {
            validate_v2_row(&self.line, phase)?;
        }
        // Two direct struct passes avoid internally-tagged enum Content buffering of traces.
        let row = serde_json::from_slice(&self.line)?;
        self.digest.update(&self.line);
        Ok(row)
    }

    fn finish(self, path: &Path, sha: &str) -> Result<FileIdentity> {
        let expected = self.sha();
        let mut reader = self.reader;
        require(
            reader.read(&mut [0])? == 0
                && self.bytes == self.original.len
                && expected == sha
                && file_identity(reader.get_ref())? == self.original
                && file_identity(&open_input(path)?)? == self.original,
            "full result SHA/EOF/identity",
        )?;
        Ok(self.original)
    }
}

fn trace_fingerprint(line: &[u8]) -> Result<[u8; 32]> {
    const DELIMITER: &[u8] = b",\"trace\":";
    require(line.ends_with(b"}\n"), "native query row ending")?;
    let mut boundaries = line
        .windows(DELIMITER.len())
        .enumerate()
        .filter_map(|(i, bytes)| (bytes == DELIMITER).then_some(i));
    let start = boundaries.next().ok_or("missing native trace delimiter")?;
    require(
        boundaries.next().is_none(),
        "ambiguous native trace delimiter",
    )?;
    let trace = line
        .get(start + DELIMITER.len()..line.len() - 2)
        .ok_or("trace suffix bounds")?;
    require(trace.first() == Some(&b'{'), "native trace object required")?;
    // Parsing just this suffix with IgnoredAny proves trace is the FINAL value.
    // A nested boundary or any field after trace leaves trailing JSON and fails.
    serde_json::from_slice::<IgnoredAny>(trace)?;
    Ok(Sha256::digest(&line[start..]).into())
}

fn read_run(path: &Path, sha: &str) -> Result<Run> {
    read_run_with(path, sha, None)
}

fn read_run_with(path: &Path, sha: &str, expected: Option<&CompletedConfig>) -> Result<Run> {
    read_run_observed(path, sha, expected, |_, _, _| Ok(()))
}

fn read_run_observed(
    path: &Path,
    sha: &str,
    expected: Option<&CompletedConfig>,
    query: impl FnMut(&[u8], &Query, &Inputs) -> Result<()>,
) -> Result<Run> {
    read_run_observed_with_identity(path, sha, expected, None, query)
}

fn read_run_observed_with_identity(
    path: &Path,
    sha: &str,
    expected: Option<&CompletedConfig>,
    admitted_inode: Option<(u64, u64)>,
    query: impl FnMut(&[u8], &Query, &Inputs) -> Result<()>,
) -> Result<Run> {
    read_run_observed_policy(
        path,
        sha,
        expected,
        admitted_inode,
        ReadPolicy::Existing,
        query,
    )
}

fn read_run_observed_policy(
    path: &Path,
    sha: &str,
    expected: Option<&CompletedConfig>,
    admitted_inode: Option<(u64, u64)>,
    policy: ReadPolicy,
    mut query: impl FnMut(&[u8], &Query, &Inputs) -> Result<()>,
) -> Result<Run> {
    require(valid_sha(sha), "result lowercase SHA256")?;
    let split_panel = policy == ReadPolicy::Sq8SplitPanel;
    let diagnostic = policy == ReadPolicy::Sq8PanelAttribution;
    let v10 = expected.is_some_and(|c| {
        c.schema == SCALE_CONFIG_SCHEMA
            && c.expected_identity["schema"]
                .as_str()
                .is_some_and(current_v10::is_result_schema)
    });
    if v10 {
        current_v10::validate_completed(expected.ok_or("v10 expected headers")?)?;
    }
    if split_panel {
        sq8_split_panel::validate_completed(expected.ok_or("split expected headers")?)?;
    }
    if diagnostic {
        sq8_panel_attribution::validate_completed(expected.ok_or("diagnostic expected headers")?)?;
    }
    let mut rows = Rows::open(path)?;
    if let Some(inode) = admitted_inode {
        require(
            (rows.original.dev, rows.original.ino) == inode,
            "parity run changed before first content read",
        )?;
    }
    let concurrent =
        expected.is_some_and(|c| current_v10::concurrent_result(&c.expected_identity["schema"]));
    require(
        !concurrent || policy == ReadPolicy::Existing,
        "concurrent completed reader policy",
    )?;
    rows.concurrent = concurrent;
    rows.policy = policy;
    rows.v10 = v10;
    rows.source_layout = expected
        .is_some_and(|c| current_v10::is_source_layout_schema(&c.expected_identity["schema"]));
    rows.library_scheduler = rows.source_layout
        || expected.is_some_and(|c| {
            c.expected_identity["schema"] == current_v10::LIBRARY_SCHEDULER_RESULT_SCHEMA
        });
    rows.cpu_profile =
        expected.is_some_and(|c| current_v10::profile_result(&c.expected_identity["schema"]));
    rows.v2 = expected.is_some();
    let scale = expected.is_some_and(|c| c.schema == SCALE_CONFIG_SCHEMA);
    rows.scale = scale;
    let current = v10
        || split_panel
        || (scale
            && expected.is_some_and(|c| {
                c.expected_identity["schema"] == "borsuk-cohere-native-baseline-result-v9"
            }));
    rows.current = current;
    if (current || diagnostic)
        && expected
            .is_some_and(|c| c.expected_bound_inputs["execution"]["mode"] == "diagnostic_panel")
    {
        rows.line_cap = 65_536;
    }
    if let Some(c) = expected {
        require(rows.original.len == c.input.bytes, "expected result bytes")?;
    }
    let identity: Identity = rows.row("identity")?;
    if let Some(c) = expected {
        require(
            serde_json::from_slice::<Value>(&rows.line)? == c.expected_identity,
            "expected v2 identity pins",
        )?;
        if scale {
            require(
                matches!(
                    identity.schema.as_str(),
                    "borsuk-cohere-native-baseline-result-v7"
                        | "borsuk-cohere-native-baseline-result-v8"
                ) || current,
                "scale result schema",
            )?;
            if v10 {
                validate_native_identity(&identity, &c.expected_identity, &identity.schema)?;
            } else {
                let mut raw = c.expected_identity.clone();
                raw["schema"] = json!("borsuk-cohere-native-baseline-result-v2");
                let identity: Identity = serde_json::from_value(raw.clone())?;
                validate_v2_identity(&identity, &raw)?;
            }
        } else {
            validate_v2_identity(&identity, &c.expected_identity)?;
        }
    } else {
        identity.validate()?;
    }
    let inputs: Inputs = rows.row("bound_inputs")?;
    if scale {
        let c = expected.ok_or("scale config")?;
        require(
            c.expected_identity.get("source_get_budget")
                == c.expected_bound_inputs.get("source_get_budget"),
            "identity/input SOURCE budget mismatch",
        )?;
    }
    let local =
        scale && expected.is_some_and(|c| c.expected_bound_inputs["backend"]["kind"] == "local");
    let ordinals = if scale {
        let raw = &expected.ok_or("scale config")?.expected_bound_inputs;
        if current || diagnostic {
            v9_panel::ordinals(raw, inputs.count)?
        } else {
            scale_ordinals(raw, inputs.count)?
        }
    } else {
        (0..COUNT).collect()
    };
    let count = ordinals.len();
    let panel = scale
        && expected
            .is_some_and(|c| c.expected_bound_inputs["execution"]["mode"] == "diagnostic_panel");
    let mut split_caller_pinned = None;
    let mut diagnostic_model = None;
    let mut completed = if let Some(c) = expected {
        require(
            serde_json::from_slice::<Value>(&rows.line)? == c.expected_bound_inputs,
            "expected v2 input/root/backend pins",
        )?;
        if scale {
            if v10 {
                validate_current_scale_inputs(&inputs, &c.expected_bound_inputs)?;
            } else {
                validate_scale_inputs_for_version(
                    &inputs,
                    &c.expected_bound_inputs,
                    current || diagnostic,
                )?;
            }
            if current && !v10 {
                v9_panel::budgets(&c.expected_identity, &c.expected_bound_inputs)?;
            }
            require(
                c.expected_identity["execution"] == c.expected_bound_inputs["execution"]
                    && c.expected_identity["serving"] == c.expected_bound_inputs["serving"],
                "identity execution agreement",
            )?;
        } else {
            validate_v2_inputs(&inputs, &c.expected_bound_inputs)?;
        }
        require(
            c.expected_identity.get("fetch_parallelism")
                == c.expected_bound_inputs.get("fetch_parallelism"),
            "identity/input fetch_parallelism agreement",
        )?;
        if panel {
            // The native untraced panel emits admission before any source or generation open.
            let admission: Value = rows.row("diagnostic_admission")?;
            if v10 {
                let model = current_v10::validate_admission(&admission, &c.expected_bound_inputs)?;
                rows.line_cap = model.line_cap();
                diagnostic_model = Some(model);
                split_caller_pinned = Some(admission["caller_pinned_bytes"].clone());
            } else if diagnostic {
                sq8_panel_attribution::validate_admission(
                    &admission,
                    &c.expected_bound_inputs,
                    current,
                )?;
            } else if current {
                v9_panel::validate_admission(&admission, &c.expected_bound_inputs)?;
            }
            require(
                admission["truth_opened"] == false
                    && admission["selected_count"] == count
                    && admission["population_count"] == inputs.count
                    && admission["execution"] == c.expected_bound_inputs["execution"]
                    && admission["trace"]
                        == if current || diagnostic {
                            c.expected_bound_inputs["execution"]["trace"].clone()
                        } else {
                            json!(false)
                        }
                    && admission["population_percentiles_valid"] == false
                    && admission["full_cohort_qualification"] == false,
                "panel admission geometry/scope",
            )?;
        }
        let binding: Value = rows.row("source_binding")?;
        let binding_charge: Charge = serde_json::from_value(binding["charges"].clone())?;
        binding_charge.validate()?;
        require(
            binding_charge.submitted_gets
                == if local {
                    0
                } else if rows.source_layout {
                    3
                } else {
                    2
                }
                && (if local {
                    binding_charge.verified_bytes == 0
                } else {
                    binding_charge.verified_bytes > 0
                })
                && binding_charge.failed_gets == 0,
            "successful source binding charges",
        )?;
        let mut evidence = CompletedEvidence::new(binding_charge);
        if fp32_serving::enabled(&expected.ok_or("FP32 expected config")?.expected_bound_inputs) {
            let policy = &expected.unwrap().expected_bound_inputs["s3_read_retry"];
            evidence.fp32_head_attempt_cap = Some(if policy["policy"] == "one_retry_100ms" { 2 } else { 1 });
        }
        if rows.source_layout {
            current_v10::validate_generation_authority(
                &binding["generation_authority"],
                &c.expected_bound_inputs,
            )?;
            evidence.generation_authority = binding["generation_authority"].clone();
        }
        evidence.transport_scale(&rows.line, "native_source", None, local)?;
        rows.row::<IgnoredAny>("generation_open")?;
        evidence.transport_scale(&rows.line, "generation_open", None, local)?;
        Some(evidence)
    } else {
        inputs.validate()?;
        None
    };
    #[derive(Deserialize)]
    struct Startup {
        truth_opened: bool,
    }
    let startup: Startup = rows.row("startup")?;
    require(!startup.truth_opened, "startup truth boundary")?;
    if scale {
        let c = expected.ok_or("byte-window expected config")?;
        require(
            c.expected_identity.get("shared_byte_window")
                == c.expected_bound_inputs.get("shared_byte_window"),
            "byte-window header agreement",
        )?;
        let raw: Value = serde_json::from_slice(&rows.line)?;
        if fp32_serving::enabled(&c.expected_bound_inputs) {
            completed
                .as_mut()
                .ok_or("FP32 startup evidence")?
                .transport_scale(
                    &serde_json::to_vec(&raw["fp32_rerank"])?,
                    "canonical_open",
                    None,
                    local,
                )?;
        }
        match c.expected_bound_inputs.get("shared_byte_window") {
            Some(window) => require(
                raw["shared_byte_window_state_bytes"]
                    .as_u64()
                    .is_some_and(|n| {
                        let minimum = window["max_waiters"]
                            .as_u64()
                            .and_then(|n| usize::try_from(n).ok())
                            .and_then(borsuk::sq8_s3_range::InFlightBytes::modeled_state_bytes);
                        minimum.is_some_and(|minimum| n >= minimum as u64)
                            && n <= window["max_state_bytes"].as_u64().unwrap_or(0)
                    }),
                "byte-window startup state/config binding",
            )?,
            None => require(
                raw.get("shared_byte_window_state_bytes").is_none(),
                "unconfigured byte-window startup state",
            )?,
        }
    }
    if let Some(e) = &mut completed {
        e.startup = serde_json::from_slice(&rows.line)?;
        if let Some(pinned) = split_caller_pinned {
            require(
                e.startup["caller_pinned_bytes"] == pinned,
                "split admission/startup caller charge",
            )?;
        }
        if rows.source_layout {
            current_v10::validate_layout_startup(
                &e.startup,
                &expected
                    .ok_or("layout startup config")?
                    .expected_bound_inputs,
                &e.generation_authority,
            )?;
        }
        if v10 || (current && !panel) {
            current_v10::validate_startup(
                &e.startup,
                &expected
                    .ok_or("split startup config")?
                    .expected_bound_inputs,
            )?;
        }
    }
    let mut samples = Vec::with_capacity(count);
    let mut charges = Charges::default();
    let mut stages = StageTotals::default();
    let (mut wall, mut cpu) = (0, 0);
    let mut underfilled = 0;
    for (slot, &ordinal) in ordinals.iter().enumerate() {
        let q: Query = rows.row("query")?;
        if scale {
            let raw: Value = serde_json::from_slice(&rows.line)?;
            let plan = &raw["plan"];
            let page_count = inputs.rows.div_ceil(if current { 32 } else { 256 }) as u64;
            let pages = plan["selected_pages"].as_array().ok_or("native pages")?;
            let mut unique = BTreeSet::new();
            require(
                pages.iter().all(|p| {
                    p.as_u64()
                        .is_some_and(|p| p < page_count && unique.insert(p))
                }),
                "native unique bounded pages",
            )?;
            require(
                plan["ranges"]
                    .as_array()
                    .ok_or("native ranges")?
                    .iter()
                    .all(|r| {
                        r[1].as_u64().is_some_and(|end| {
                            end <= inputs.rows as u64 * (inputs.dimensions as u64 + 12)
                        })
                    }),
                "native range corpus bound",
            )?;
            require(
                q.charges.source.submitted_gets <= inputs.source_get_budget.unwrap_or(128) as u64
                    && q.charges.source.verified_bytes
                        <= if rows.source_layout {
                            expected
                                .ok_or("SOURCE budget config")?
                                .expected_bound_inputs["source_byte_budget"]
                                .as_u64()
                                .ok_or("SOURCE byte budget")?
                        } else {
                            64 * 1024 * 1024
                        }
                    && q.charges.sq8.submitted_gets
                        <= if v10 {
                            current_v10::physical_cap(
                                &expected.ok_or("split config")?.expected_bound_inputs,
                            )?
                        } else if current {
                            expected.ok_or("current config")?.expected_bound_inputs["sq8_get_budget"].as_u64().ok_or("current GET budget")?
                        } else {
                            32
                        }
                    && q.charges.sq8.verified_bytes
                        <= if current {
                            fp32_serving::sq8_bytes(&expected.ok_or("current config")?.expected_bound_inputs)?
                        } else {
                            16_773_120
                        },
                "baseline SOURCE/SQ8 query caps",
            )?;
            require(
                if panel {
                    raw["selected_slot"] == slot
                } else {
                    raw.get("selected_slot").is_none()
                },
                "selected slot sequence",
            )?;
            if current {
                let c = expected.ok_or("current config")?;
                require(
                    q.returned.len() == K && q.charges.sum()?.failed_gets == 0,
                    "current complete query and zero failures",
                )?;
                if v10 {
                    current_v10::validate_geometry(&raw, &c.expected_bound_inputs)?;
                    current_v10::validate_coverage_router(&raw, &c.expected_bound_inputs,
                        &completed.as_ref().ok_or("coverage authority")?.generation_authority)?;
                    if rows.source_layout {
                        current_v10::validate_source_capsules(&raw, &c.expected_bound_inputs)?;
                    }
                    if let Some(diag) = raw.get("diagnostic") {
                        diagnostic_model
                            .ok_or("diagnostic admission model required")?
                            .bind(diag)?;
                        sq8_split_panel::validate_diagnostic(&raw, &c.expected_bound_inputs)?;
                    }
                } else {
                    v9_panel::validate_geometry(
                        &raw,
                        inputs.rows,
                        inputs.dimensions,
                        usize::try_from(
                            c.expected_bound_inputs["sq8_get_budget"]
                                .as_u64()
                                .ok_or("current GET budget")?,
                        )?,
                        usize::try_from(
                            c.expected_bound_inputs["sq8_byte_budget"]
                                .as_u64()
                                .ok_or("current byte budget")?,
                        )?,
                    )?;
                }
                require(
                    raw.get("diagnostic").is_some()
                        == (panel && c.expected_bound_inputs["execution"]["trace"] == true),
                    "current diagnostic trace presence",
                )?;
            }
        }
        let trace_sha256 = if completed.is_some() {
            // Native v2 Record emits transport AFTER trace. Hash the complete
            // validated trace object, preserving every array's order. Runtime
            // timing, inflight and transport fields are outside TwoBitPlanTrace.
            let raw: Value = serde_json::from_slice(&rows.line)?;
            Sha256::digest(serde_json::to_vec(&raw["trace"])?).into()
        } else {
            trace_fingerprint(&rows.line)?
        };
        let physical_plan_sha256 = if completed.is_some() {
            let raw: Value = serde_json::from_slice(&rows.line)?;
            Sha256::digest(serde_json::to_vec(&raw["plan"])?).into()
        } else {
            [0; 32]
        };
        require(
            q.ordinal == ordinal
                && !q.truth_opened
                && q.returned.len() <= K
                && q.returned_count == q.returned.len()
                && q.underfill == (q.returned.len() < K)
                // Historical four-arm v1 comparison still requires full-k results.
                && (completed.is_some() || !q.underfill),
            "query order/truth/count/underfill",
        )?;
        underfilled += usize::from(q.underfill);
        let exact_fp32 = expected.is_some_and(|c| fp32_serving::enabled(&c.expected_bound_inputs));
        let score = |h: &Hit| {
            if exact_fp32 {
                f64::from_bits(h.score_bits)
            } else {
                u32::try_from(h.score_bits)
                    .map(|b| f64::from(f32::from_bits(b)))
                    .unwrap_or(f64::NAN)
            }
        };
        require(
            q.charges.canonical.is_some() == exact_fp32,
            "FP32 charge selector",
        )?;
        if exact_fp32 {
            let raw: Value = serde_json::from_slice(&rows.line)?;
            fp32_serving::validate_query(&raw, &expected.unwrap().expected_bound_inputs)?;
        }
        for (i, hit) in q.returned.iter().enumerate() {
            require(
                hit.id < inputs.rows as u64
                    && score(hit).is_finite()
                    && !q.returned[..i].iter().any(|h| h.id == hit.id),
                "returned IDs/scores",
            )?;
        }
        require(
            q.returned.windows(2).all(|h| {
                score(&h[0])
                    .total_cmp(&score(&h[1]))
                    .then(h[0].id.cmp(&h[1].id))
                    .is_le()
            }),
            "returned ranking order",
        )?;
        if completed.is_some() {
            for charge in [q.charges.router, q.charges.source, q.charges.sq8, q.sum] {
                charge.validate()?;
            }
        }
        require(q.charges.sum()? == q.sum, "query charge total")?;
        require(
            q.query_wall_ns > 0 && q.query_wall_ns <= i64::MAX as u64,
            "positive bounded query wall",
        )?;
        charges.add(q.charges)?;
        stages.add(&q.stages, q.query_wall_ns,
            expected.is_some_and(|c| fp32_serving::direct(&c.expected_bound_inputs)))?;
        if let Some(e) = &mut completed {
            if !concurrent {
                e.transport_scale(&rows.line, "query", Some(ordinal), local)?;
            }
            for (samples, interval) in e.stage_samples.iter_mut().zip([
                &q.stages.discovery,
                &q.stages.source,
                &q.stages.planning,
                &q.stages.sq8,
            ]) {
                samples.push(interval.end_ns - interval.start_ns);
            }
        }
        wall = plus(wall, q.query_wall_ns)?;
        if !concurrent {
            cpu = plus(
                cpu,
                q.query_process_cpu_ns.ok_or("serial query CPU required")?,
            )?;
        }
        query(&rows.line, &q, &inputs)?;
        samples.push(Sample {
            returned: q.returned,
            charges: q.charges,
            hits10: 0,
            wall_ns: q.query_wall_ns,
            trace_sha256,
            physical_plan_sha256,
        });
    }
    let cohort = if concurrent {
        let cohort: Value = rows.row("query_cohort")?;
        let inputs = &expected.ok_or("concurrent headers")?.expected_bound_inputs;
        require(
            cohort["attempted_count"] == count
                && cohort["successful_count"] == count
                && cohort["max_active_queries"] == current_v10::concurrent_queries(inputs)?
                && cohort["sum_query_wall_ns"] == wall,
            "cohort conservation",
        )?;
        let minimum_elapsed = samples
            .chunks(current_v10::concurrent_queries(inputs)?)
            .try_fold(0, |total, batch| {
                plus(
                    total,
                    batch
                        .iter()
                        .map(|sample| sample.wall_ns)
                        .max()
                        .ok_or("empty concurrent batch")?,
                )
            })?;
        require(
            cohort["elapsed_ns"].as_u64().ok_or("cohort elapsed")? >= minimum_elapsed,
            "cohort ordered-batch elapsed floor",
        )?;
        cpu = cohort["process_cpu_ns"].as_u64().ok_or("cohort CPU")?;
        if let Some(e) = &mut completed {
            e.transport_scale(&rows.line, "query_cohort", None, local)?;
        }
        Some(cohort)
    } else {
        None
    };
    let (prefix_bytes, prefix_sha) = (rows.bytes, rows.sha());
    let seal: Seal = rows.row("all_queries_sealed")?;
    require(
        seal.count == count
            && !seal.truth_opened
            && seal.prefix_bytes == prefix_bytes
            && seal.prefix_sha256 == prefix_sha
            && seal.requests_sha256 == inputs.requests_sha256
            && seal.generation_root_sha256 == inputs.generation_root_sha256
            && seal.requires_successful_sync
            && seal.requires_successful_directory_sync,
        "authenticated query seal",
    )?;
    if scale {
        let raw: Value = serde_json::from_slice(&rows.line)?;
        let reserved =
            &expected.ok_or("scale config")?.expected_bound_inputs["reserved_query_interval"];
        let reserved_count = reserved["end"].as_u64().ok_or("reserved end")?
            - reserved["start"].as_u64().ok_or("reserved start")?;
        require(
            raw["selected_count"] == count
                && raw["population_count"] == inputs.count
                && raw["reserved_query_count"] == reserved_count,
            "scale seal counts",
        )?;
    }
    let (sealed_bytes, sealed_sha) = (rows.bytes, rows.sha());
    let mut hits = 0;
    for (ordinal, sample) in samples.iter_mut().enumerate() {
        let recall: Recall = rows.row("recall")?;
        require(
            recall.ordinal == ordinals[ordinal]
                && recall.hits10 <= sample.returned.len() as u64
                && recall.recall10 == recall.hits10 as f64 / K as f64
                && recall.returned_count == sample.returned.len()
                && recall.underfill == (sample.returned.len() < K),
            "recall order/hits/count",
        )?;
        sample.hits10 = recall.hits10;
        hits = plus(hits, recall.hits10)?;
    }
    #[derive(Deserialize)]
    struct Final {
        summary: Terminal,
    }
    let t = rows.row::<Final>("terminal")?.summary;
    if completed.is_some() {
        for charge in [t.charges.router, t.charges.source, t.charges.sq8, t.sum] {
            charge.validate()?;
        }
    }
    require(
        t.status == "MEASURED"
            && t.complete
            && t.queries == count
            && t.k == K
            && t.total_hits10 == hits
            && t.recall_numerator == hits
            && t.recall_denominator == (count * K) as u64
            && t.mean_recall10 == hits as f64 / (count * K) as f64
            && t.underfilled_queries == underfilled
            && t.all_queries_sealed
            && !t.physical_s3_measured
            && t.external_gate_required,
        "complete measured terminal/recall totals/scope",
    )?;
    require(
        t.prefix_bytes == prefix_bytes
            && t.prefix_sha256 == prefix_sha
            && t.sealed_bytes == sealed_bytes
            && t.sealed_sha256 == sealed_sha
            && t.requests_sha256 == inputs.requests_sha256
            && t.truth_sha256 == inputs.truth_sha256
            && t.generation_root_sha256 == inputs.generation_root_sha256,
        "terminal seal/input authentication",
    )?;
    require(
        t.charges == charges
            && t.sum == charges.sum()?
            && t.query_wall_ns == wall
            && t.query_process_cpu_ns == cpu
            && t.process_wall_ns
                >= cohort
                    .as_ref()
                    .map_or(wall, |c| c["elapsed_ns"].as_u64().unwrap())
            && t.process_cpu_ns >= cpu,
        "terminal charge/time totals",
    )?;
    if let Some(e) = &mut completed {
        let raw: Value = serde_json::from_slice(&rows.line)?;
        e.terminal = raw["summary"].clone();
        require(
            e.terminal.get("query_cohort") == cohort.as_ref(),
            "sealed cohort equals terminal cohort",
        )?;
        if scale {
            let t = &e.terminal;
            let c = expected.ok_or("scale config")?;
            let reserved_count = c.expected_bound_inputs["reserved_query_interval"]["end"]
                .as_u64()
                .ok_or("reserved end")?
                - c.expected_bound_inputs["reserved_query_interval"]["start"]
                    .as_u64()
                    .ok_or("reserved start")?;
            require(
                t["reserved_query_count"] == reserved_count
                    && t["diagnostic_prefix"] == ((inputs.count as u64) < reserved_count)
                    && t["population_percentiles_valid"] == (!panel && inputs.count == 1000)
                    && t["full_cohort_qualification"] == false,
                "reserved population and diagnostic prefix scope",
            )?;
            require(
                t["execution"] == c.expected_bound_inputs["execution"]
                    && t["serving"] == c.expected_bound_inputs["serving"]
                    && t["direct_memory"].is_null()
                    && t["selected_count"] == count
                    && t["executed_count"] == count
                    && t["population_count"] == inputs.count
                    && t["diagnostic_panel"] == panel,
                "scale terminal geometry/execution",
            )?;
            if panel || inputs.count < 1000 {
                require(
                    t["population_percentiles_valid"] == false
                        && t["full_cohort_qualification"] == false,
                    "diagnostic terminal scope",
                )?;
            }
        }
        let binding_charge: Charge = serde_json::from_value(e.terminal["binding_charge"].clone())?;
        binding_charge.validate()?;
        require(
            binding_charge == e.binding_charge,
            "terminal source binding charge",
        )?;
        require(
            e.terminal["transport_last_boundary"]
                == if local {
                    e.local_boundary.clone()
                } else {
                    e.last.as_ref().ok_or("missing transport")?.compact()
                },
            "terminal cumulative transport boundary",
        )?;
    }
    let file_identity = rows.finish(path, sha)?;
    Ok(Run {
        identity,
        inputs,
        samples,
        stages,
        terminal: t,
        file_identity,
        completed,
    })
}

#[derive(Serialize)]
struct Statistics {
    count: usize,
    query_wall_ns: u64,
    sequential_qps: f64,
    p50_ms: f64,
    p90_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    // Integer quantiles also make exact threshold decisions inspectable.
    p90_ns: u64,
    p95_ns: u64,
}

fn statistics(samples: &[u64]) -> Result<Statistics> {
    require(
        !samples.is_empty() && samples.len() <= 2 * COUNT && samples.iter().all(|&n| n > 0),
        "nonempty bounded positive samples",
    )?;
    let (total, [p50, p90, p95, p99]) = quantiles(samples)?;
    Ok(Statistics {
        count: samples.len(),
        query_wall_ns: total,
        sequential_qps: samples.len() as f64 * 1e9 / total as f64,
        p50_ms: p50 as f64 / 1e6,
        p90_ms: p90 as f64 / 1e6,
        p95_ms: p95 as f64 / 1e6,
        p99_ms: p99 as f64 / 1e6,
        p90_ns: p90,
        p95_ns: p95,
    })
}

fn quantiles(samples: &[u64]) -> Result<(u64, [u64; 4])> {
    require(
        !samples.is_empty() && samples.len() <= 2 * COUNT,
        "bounded quantile population",
    )?;
    let total = samples.iter().try_fold(0, |sum, &n| plus(sum, n))?;
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    // Same integer nearest-rank rule as native_ann_100k_qualify.rs. Stage
    // intervals may legitimately have zero duration; query walls may not.
    Ok((
        total,
        [50, 90, 95, 99].map(|p| sorted[(sorted.len() * p).div_ceil(100) - 1]),
    ))
}

fn read_config<T: DeserializeOwned>(config_path: &Path, config_sha: &str) -> Result<(T, u64)> {
    require(valid_sha(config_sha), "reduction config SHA256")?;
    require(config_path.to_str().is_some(), "config path must be UTF-8")?;
    read_config_file(
        &open_input(config_path)?,
        config_path,
        config_sha,
        CONFIG_CAP,
    )
}

fn read_config_file<T: DeserializeOwned>(
    mut file: &File,
    config_path: &Path,
    config_sha: &str,
    cap: u64,
) -> Result<(T, u64)> {
    require(valid_sha(config_sha), "reduction config SHA256")?;
    require(config_path.to_str().is_some(), "config path must be UTF-8")?;
    let original = file_identity(file)?;
    require(
        original.len > 0 && original.len <= cap,
        "small reduction config",
    )?;
    let mut body = Vec::with_capacity(original.len as usize);
    Read::take(&mut file, cap + 1).read_to_end(&mut body)?;
    require(
        body.len() as u64 == original.len
            && file_identity(file)? == original
            && file_identity(&open_input(config_path)?)? == original
            && format!("{:x}", Sha256::digest(&body)) == config_sha,
        "config SHA/length/identity",
    )?;
    let UniqueJson(value) = serde_json::from_slice(&body)?;
    Ok((serde_json::from_value(value)?, original.len))
}

const HEADER_AUTHORITY_SCHEMA: &str = "borsuk-completed-header-authority-v1";
const HEADER_JSON_CAP: u64 = 65_536;
const HEADER_RESULT_CAP: u64 = FILE_CAP;
const BASELINE_BINARY_SHA: &str =
    "59fe47aa1001b3ca24d1f9ff31444f97fcda72e3e297c8d7d846f5c3d811bfc3";
const RESERVED_QUERIES_SHA: &str =
    "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e";
// Exact include_bytes identities at c3e52c8; executable provenance is an external gate.
const BASELINE_HEADER_SOURCES: [(&str, &str); 7] = [
    (
        "runner_source_sha256",
        "d5e593e5f8e2aa02697b366cc3bcd8e6fa4160071ea0e2ac337063f9ff0023c4",
    ),
    (
        "generation_source_sha256",
        "6acb7cdbb23d790aee8bfbcba8924ae09f0c7c7738e87ca2b3e40006fd0e3f38",
    ),
    (
        "router_source_sha256",
        "2961d9295d49e217e4b8c24ef734b09f079fa112306e65655962cebba17c7ae5",
    ),
    (
        "codec_source_sha256",
        "eddf88c6c8ee23a732751f49291293f1cc081545ebb9a21c149eb757aa5c38ec",
    ),
    (
        "source_plane_source_sha256",
        "dbcc4cdbc4bb5c244354b375afd657f8b5cb1892df3f0ff0b8f41ef580de42e0",
    ),
    (
        "sq8_range_source_sha256",
        "3f6407664d5f2c1be817e4b32e6a9dce8d4bf8e66ca455e237d0e7b0716ad711",
    ),
    (
        "returned_source_sha256",
        "a4dcc7f9bc06cdea835f72860f3d2e796fcb6f29276858c349d624884cc9f74b",
    ),
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeaderAuthority {
    schema: String,
    config: CompletedInput,
    result: CompletedInput,
    cohort: CompletedInput,
    binary_sha256: String,
    sources: Value,
}

fn header_path(path: &Path) -> Result<()> {
    require(
        path.is_absolute() && path.to_str().is_some() && path.as_os_str().len() <= 4096,
        "absolute bounded UTF-8 header path",
    )
}

fn header_file(path: &Path) -> Result<File> {
    header_path(path)?;
    let file = open_input(path)?;
    require(file.metadata()?.nlink() == 1, "single-link header artifact")?;
    file_identity(&file)?;
    Ok(file)
}

fn header_pin(pin: &CompletedInput, cap: u64) -> Result<()> {
    header_path(&pin.path)?;
    require(
        pin.bytes > 0 && pin.bytes <= cap && valid_sha(&pin.sha256),
        "bounded header artifact pin",
    )
}

#[derive(Debug)]
struct HeaderSnapshot {
    file: File,
    path: PathBuf,
    identity: FileIdentity,
}

impl HeaderSnapshot {
    fn recheck(&self) -> Result<()> {
        recheck_header_file(&self.file, &self.path, &self.identity)
    }
}

#[derive(Debug)]
struct BoundHeaders {
    value: Value,
    inputs: [HeaderSnapshot; 4],
}

impl BoundHeaders {
    fn recheck(&self) -> Result<()> {
        for input in &self.inputs {
            input.recheck()?;
        }
        Ok(())
    }
}

fn header_json(pin: &CompletedInput) -> Result<(Value, HeaderSnapshot)> {
    header_pin(pin, HEADER_JSON_CAP)?;
    let file = header_file(&pin.path)?;
    let original = file_identity(&file)?;
    require(original.len == pin.bytes, "header JSON length")?;
    let (value, _) = read_config_file(&file, &pin.path, &pin.sha256, HEADER_JSON_CAP)?;
    recheck_header_file(&file, &pin.path, &original)?;
    Ok((
        value,
        HeaderSnapshot {
            file,
            path: pin.path.clone(),
            identity: original,
        },
    ))
}

fn recheck_header_file(file: &File, path: &Path, original: &FileIdentity) -> Result<()> {
    require(
        file.metadata()?.nlink() == 1
            && file_identity(file)? == *original
            && file_identity(&header_file(path)?)? == *original,
        "header artifact mutation/path identity",
    )
}

fn authenticated_headers(pin: &CompletedInput) -> Result<([Value; 2], HeaderSnapshot)> {
    header_pin(pin, HEADER_RESULT_CAP)?;
    let mut file = header_file(&pin.path)?;
    let original = file_identity(&file)?;
    require(original.len == pin.bytes, "header result length")?;
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut headers = [Vec::new(), Vec::new()];
    let mut row = 0;
    let mut block = [0_u8; 8192];
    loop {
        let n = file.read(&mut block)?;
        if n == 0 {
            break;
        }
        total = plus(total, n as u64)?;
        require(total <= pin.bytes, "header result growth")?;
        digest.update(&block[..n]);
        for &byte in &block[..n] {
            if row == 2 {
                break;
            }
            if byte == b'\n' {
                require(!headers[row].is_empty(), "nonempty native header")?;
                row += 1;
            } else {
                require(
                    headers[row].len() < HEADER_JSON_CAP as usize,
                    "native header cap",
                )?;
                headers[row].push(byte);
            }
        }
    }
    require(
        total == pin.bytes && format!("{:x}", digest.finalize()) == pin.sha256,
        "header result whole-body SHA/EOF",
    )?;
    recheck_header_file(&file, &pin.path, &original)?;
    require(row == 2, "two newline-terminated native headers")?;
    // No header is parsed or compared until the entire closed artifact authenticates.
    let UniqueJson(identity) = serde_json::from_slice(&headers[0])?;
    let UniqueJson(inputs) = serde_json::from_slice(&headers[1])?;
    Ok((
        [identity, inputs],
        HeaderSnapshot {
            file,
            path: pin.path.clone(),
            identity: original,
        },
    ))
}

// Match the original native Config admission, without changing historical reducers.
fn validate_header_descriptors(c: &Value) -> Result<()> {
    let key = |s: &str| {
        !s.is_empty()
            && s.len() <= 512
            && s.split('/').all(|p| {
                !p.is_empty()
                    && p != "."
                    && p != ".."
                    && p.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
            })
    };
    require(
        c["generation_prefix"].as_str().is_some_and(key),
        "native generation prefix",
    )?;
    match c["backend"]["kind"].as_str() {
        Some("local") => header_path(Path::new(
            c["backend"]["store_root"]
                .as_str()
                .ok_or("local store root")?,
        )),
        Some("s3") => {
            let backend = &c["backend"];
            let bucket = backend["bucket"].as_str().ok_or("S3 bucket")?;
            let region = backend["region"].as_str().ok_or("S3 region")?;
            let etag = backend["sq8_etag"].as_str().ok_or("S3 ETag")?;
            require(
                (3..=63).contains(&bucket.len())
                    && bucket.parse::<std::net::Ipv4Addr>().is_err()
                    && bucket.split('.').all(|p| {
                        !p.is_empty()
                            && !p.starts_with('-')
                            && !p.ends_with('-')
                            && p.bytes()
                                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                    }),
                "native S3 bucket descriptor",
            )?;
            require(
                (3..=63).contains(&region.len())
                    && !region.starts_with('-')
                    && !region.ends_with('-')
                    && region
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
                "native S3 region descriptor",
            )?;
            require(
                backend["physical_prefix"].as_str().is_some_and(key)
                    && backend["sq8_object_key"].as_str().is_some_and(key),
                "native S3 keys",
            )?;
            require(
                (3..=256).contains(&etag.len())
                    && etag.starts_with('"')
                    && etag.ends_with('"')
                    && etag.as_bytes()[1..etag.len() - 1]
                        .iter()
                        .all(|&b| (0x21..=0x7e).contains(&b) && b != b'"'),
                "native S3 quoted ETag",
            )
        }
        _ => Err("native backend descriptor".into()),
    }
}

fn expected_scale_headers(
    a: &HeaderAuthority,
    mut c: Value,
    receipt: &Value,
) -> Result<[Value; 2]> {
    require(
        a.schema == HEADER_AUTHORITY_SCHEMA && a.binary_sha256 == BASELINE_BINARY_SHA,
        "original baseline header authority/binary",
    )?;
    require(
        a.sources
            .as_object()
            .is_some_and(|s| s.len() == BASELINE_HEADER_SOURCES.len())
            && BASELINE_HEADER_SOURCES
                .iter()
                .all(|(name, sha)| a.sources[*name] == *sha),
        "exact baseline component source authority",
    )?;
    let config = c.as_object_mut().ok_or("original native config object")?;
    // The original native Config defaults only this field.
    config.entry("fetch_parallelism").or_insert(json!(16));
    validate_header_descriptors(&c)?;
    fields(
        &c,
        "schema dataset revision metric tie_rule corpus_intervals reserved_query_interval cohort_receipt derivation_receipt producer_authority corpus_source_first query_source_first rows dimensions count k profile backend generation_prefix generation_root_sha256 scratch_parent requests truth native_source max_memory_bytes fetch_parallelism serving execution",
    )?;
    require(
        c["schema"] == "borsuk-cohere-native-baseline-config-v7"
            && c["rows"] == 1_000_000
            && c["dimensions"] == 1024
            && c["k"] == 10
            && c["profile"] == "scale1m"
            && matches!(c["count"].as_u64(), Some(32 | 1000))
            && c["corpus_intervals"]
                == json!([{"start":0,"end":100_000},{"start":101_000,"end":1_001_000}])
            && c["reserved_query_interval"] == json!({"start":100_000,"end":101_000})
            && receipt["reserved_queries_sha256"] == RESERVED_QUERIES_SHA,
        "frozen scale geometry/reserved query seal",
    )?;
    header_path(Path::new(
        c["scratch_parent"].as_str().ok_or("scratch parent")?,
    ))?;
    for role in ["cohort_receipt", "derivation_receipt", "requests", "truth"] {
        let pin: CompletedInput = serde_json::from_value(c[role].clone())?;
        header_pin(
            &pin,
            if matches!(role, "requests" | "truth") {
                4_096_000
            } else {
                HEADER_JSON_CAP
            },
        )?;
    }
    require(
        c["cohort_receipt"]["bytes"] == a.cohort.bytes
            && c["cohort_receipt"]["sha256"] == a.cohort.sha256
            && c["requests"]["path"] != c["truth"]["path"],
        "original cohort binding/distinct request and truth paths",
    )?;
    fields(
        &c["native_source"],
        "source_sha256 sq8_sha256 source_order_sha256",
    )?;
    let count = c["count"].as_u64().ok_or("native count")? as usize;
    require(
        receipt["schema"] == "borsuk-cohere-native-cohort-receipt-v3"
            && receipt["status"] == "COMPLETE"
            && receipt["dataset"] == c["dataset"]
            && receipt["revision"] == c["revision"]
            && receipt["geometry"]
                == json!({"corpus_rows":1_000_000,"query_rows":count,
                "dimensions":1024,"k":10,"corpus_intervals":c["corpus_intervals"],
                "reserved_query_interval":c["reserved_query_interval"],
                "query_source_ordinals":[100_000,100_000+count]}),
        "authenticated original cohort geometry",
    )?;
    let outputs = receipt["outputs"].as_array().ok_or("cohort output seals")?;
    for (name, bytes, sha) in [
        ("corpus.f32", 4_096_000_000_u64, None),
        (
            "queries.f32",
            count as u64 * 4096,
            c["requests"]["sha256"].as_str(),
        ),
        (
            "truth.u64",
            count as u64 * 80,
            c["truth"]["sha256"].as_str(),
        ),
    ] {
        let mut found = outputs.iter().filter(|v| v["name"] == name);
        let seal = found.next().ok_or("missing original cohort output seal")?;
        require(
            found.next().is_none()
                && seal["bytes"] == bytes
                && seal["sha256"].as_str().is_some_and(valid_sha)
                && sha.is_none_or(|sha| seal["sha256"] == sha),
            "unique cohort output geometry/SHA",
        )?;
    }
    // Literal fields mirror check_cohere_native_baseline.rs at c3e52c8, not the result.
    let mut identity = json!({"schema":"borsuk-cohere-native-baseline-result-v7","phase":"identity",
        "config_sha256":a.config.sha256,"binary_sha256":a.binary_sha256,
        "fetch_parallelism":c["fetch_parallelism"],"serving":c["serving"],"execution":c["execution"],
        "scope":"AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS","physical_s3_measured":false,
        "io_measurement":"logical_GET_charges_separate_from_cumulative_process_native_transport",
        "s3_credential_source":"imds_instance_role_only",
        "native_transport_includes":"S3_and_IMDS_credential_requests_including_PUT",
        "wire_bytes":null,"unread_bytes":null,"billed_bytes":null,"billed_requests":null,
        "external_gate_required":true,"truth_opened":false});
    for (name, sha) in BASELINE_HEADER_SOURCES {
        identity[name] = json!(sha);
    }
    let mut bound = json!({"phase":"bound_inputs","source_cache":"off","truth_opened":false,
        "credential_source":if c["backend"]["kind"] == "s3" { json!("imds_instance_role_only") } else { Value::Null },
        "reserved_queries_sha256":receipt["reserved_queries_sha256"],
        "cohort_receipt_sha256":c["cohort_receipt"]["sha256"],
        "derivation_receipt_sha256":c["derivation_receipt"]["sha256"],
        "selected_count":scale_ordinals(&c, count)?.len()});
    for name in "dataset revision fetch_parallelism serving metric tie_rule rows dimensions count k corpus_source_first query_source_first profile backend generation_prefix generation_root_sha256 corpus_intervals reserved_query_interval producer_authority max_memory_bytes execution".split_whitespace() {
        bound[name] = c[name].clone();
    }
    for role in ["requests", "truth"] {
        for name in ["bytes", "sha256"] {
            bound[format!("{role}_{name}")] = c[role][name].clone();
        }
    }
    for (dest, source) in [
        ("native_source_sha256", "source_sha256"),
        ("native_sq8_sha256", "sq8_sha256"),
        ("native_order_sha256", "source_order_sha256"),
    ] {
        bound[dest] = c["native_source"][source].clone();
    }
    validate_scale_row(&serde_json::to_vec(&identity)?, "identity")?;
    validate_scale_row(&serde_json::to_vec(&bound)?, "bound_inputs")?;
    validate_scale_inputs(&serde_json::from_value(bound.clone())?, &bound)?;
    Ok([identity, bound])
}

fn bind_completed_scale(authority_path: &Path, authority_sha: &str) -> Result<BoundHeaders> {
    let file = header_file(authority_path)?;
    let original = file_identity(&file)?;
    let (a, _): (HeaderAuthority, _) =
        read_config_file(&file, authority_path, authority_sha, HEADER_JSON_CAP)?;
    recheck_header_file(&file, authority_path, &original)?;
    let (c, config_pin) = header_json(&a.config)?;
    let (receipt, cohort_pin) = header_json(&a.cohort)?;
    let [identity, bound] = expected_scale_headers(&a, c, &receipt)?;
    let (actual, result_pin) = authenticated_headers(&a.result)?;
    require(
        actual == [identity.clone(), bound.clone()],
        "native headers differ from independent authority",
    )?;
    let value = json!({"schema":SCALE_CONFIG_SCHEMA,
        "input":{"path":a.result.path,"bytes":a.result.bytes,"sha256":a.result.sha256},
        "expected_identity":identity,"expected_bound_inputs":bound});
    require(
        (serde_json::to_vec(&value)?.len() as u64) < CONFIG_CAP,
        "completed config cap",
    )?;
    let binding = BoundHeaders {
        value,
        inputs: [
            HeaderSnapshot {
                file,
                path: authority_path.to_owned(),
                identity: original,
            },
            config_pin,
            cohort_pin,
            result_pin,
        ],
    };
    binding.recheck()?;
    Ok(binding)
}

fn publish_bound_headers(output: &Path, binding: BoundHeaders) -> Result<bool> {
    publish_bound_headers_with_sync(output, binding, |file, _| file.sync_all())
}

// This narrow sync adapter lets tests exercise the actual publication boundaries.
// Other report modes retain their original writer and dispatch.
fn publish_bound_headers_with_sync(
    output: &Path,
    binding: BoundHeaders,
    mut sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<bool> {
    header_path(output)?;
    binding.recheck()?;
    let mut expected = serde_json::to_vec(&binding.value)?;
    expected.push(b'\n');
    require(
        expected.len() as u64 <= CONFIG_CAP,
        "completed config publication cap",
    )?;
    let dir = parent(output)?;
    let parent_identity = dir.metadata()?;
    let mut file = File::from(openat(
        &dir,
        output.file_name().ok_or("output filename")?,
        OFlags::WRONLY
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    let created = file_identity(&file)?;
    file.write_all(&expected)?;
    sync(&file, false)?;
    let mut readback = header_file(output)?;
    let published = file_identity(&readback)?;
    require(
        published.dev == created.dev
            && published.ino == created.ino
            && published.len == expected.len() as u64
            && file_identity(&file)? == published,
        "completed config descriptor identity",
    )?;
    let mut actual = Vec::with_capacity(expected.len());
    Read::take(&mut readback, CONFIG_CAP + 1).read_to_end(&mut actual)?;
    require(actual == expected, "completed config readback content")?;
    recheck_header_file(&readback, output, &published)?;
    binding.recheck()?;
    sync(&dir, true)?;
    // Both input and output identity checks remain live through directory sync.
    recheck_header_file(&readback, output, &published)?;
    require(
        file.metadata()?.nlink() == 1 && file_identity(&file)? == published,
        "completed config final descriptor",
    )?;
    let current_parent = parent(output)?.metadata()?;
    require(
        parent_identity.dev() == current_parent.dev()
            && parent_identity.ino() == current_parent.ino(),
        "completed config parent identity",
    )?;
    binding.recheck()?;
    Ok(true)
}

#[cfg(test)]
mod completed_header_binding_tests {
    use super::*;
    #[test]
    fn bind_completed_scale_authenticates_population_sized_result_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("population-result.jsonl");
        let mut file = File::create(&path).unwrap();
        let prefix = b"{\"first\":1}\n{\"second\":2}\n";
        file.write_all(prefix).unwrap();
        let mut digest = Sha256::new();
        digest.update(prefix);
        // The binder authenticates opaque tail bytes; the completed reducer validates rows.
        let block = [b'x'; 8192];
        let mut bytes = prefix.len() as u64;
        while bytes < FILE_CAP {
            let n = usize::try_from((FILE_CAP - bytes).min(block.len() as u64)).unwrap();
            file.write_all(&block[..n]).unwrap();
            digest.update(&block[..n]);
            bytes += n as u64;
        }
        file.sync_all().unwrap();
        drop(file);
        let mut pin = CompletedInput {
            path: path.clone(),
            bytes,
            sha256: format!("{:x}", digest.finalize()),
        };
        let (headers, snapshot) = authenticated_headers(&pin).unwrap();
        assert_eq!(headers, [json!({"first":1}), json!({"second":2})]);
        recheck_header_file(&snapshot.file, &snapshot.path, &snapshot.identity).unwrap();
        pin.sha256 = "0".repeat(64);
        assert!(
            authenticated_headers(&pin)
                .unwrap_err()
                .to_string()
                .contains("whole-body SHA/EOF")
        );
        pin.bytes = FILE_CAP + 1;
        assert!(header_pin(&pin, HEADER_RESULT_CAP).is_err());
    }

    // Frozen independently from the reviewed c3e52c8 emission and source-pin roster.
    // Changing a production pin must not silently change the golden oracle.
    const GOLDEN_BINARY: &str = "59fe47aa1001b3ca24d1f9ff31444f97fcda72e3e297c8d7d846f5c3d811bfc3";
    const GOLDEN_RESERVED: &str =
        "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e";

    fn sha(body: &[u8]) -> String {
        format!("{:x}", Sha256::digest(body))
    }

    fn put(path: &Path, body: &[u8]) -> Value {
        std::fs::write(path, body).unwrap();
        json!({"path":path,"bytes":body.len(),"sha256":sha(body)})
    }

    fn put_json(path: &Path, value: &Value) -> Value {
        put(path, &serde_json::to_vec(value).unwrap())
    }

    fn bind(dir: &Path, authority: &Value) -> Result<Value> {
        let path = dir.join("authority.json");
        let pin = put_json(&path, authority);
        bind_completed_scale(&path, pin["sha256"].as_str().unwrap()).map(|binding| binding.value)
    }

    fn result_body(rows: &[Value; 2]) -> Vec<u8> {
        let mut body = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut body, row).unwrap();
            body.push(b'\n');
        }
        // Binder authenticates these bytes; only the separate reducer interprets them.
        body.extend_from_slice(b"query/seal/terminal validation belongs to --completed-scale\n");
        body
    }

    fn fixture(dir: &Path, count: usize, s3: bool) -> (Value, Value, Value, [Value; 2]) {
        let requests = if count == 1000 {
            GOLDEN_RESERVED.to_owned()
        } else {
            "664f5b269756a1de5a77c4ec359e56ccbe85c87603fa01fc5d87cc3f02e52667".into()
        };
        let truth = "b".repeat(64);
        let intervals = json!([{"start":0,"end":100_000},{"start":101_000,"end":1_001_000}]);
        let reserved = json!({"start":100_000,"end":101_000});
        let execution = if count == 32 {
            json!({"mode":"full"})
        } else {
            json!({"mode":"diagnostic_panel","ordinals":(0..32).collect::<Vec<_>>(),"trace":false})
        };
        let backend = if s3 {
            json!({"kind":"s3","bucket":"borsuk-test","region":"eu-central-1",
                "physical_prefix":"retained","sq8_object_key":format!("generation/objects/{}", "e".repeat(64)),"sq8_etag":"\"pinned-etag\""})
        } else {
            json!({"kind":"local","store_root":"/original/store"})
        };
        let producer = json!({"source_commit":"1".repeat(40),"executable_sha256":"2".repeat(64),
            "producer_source_sha256":"3".repeat(64),"sq8_source_sha256":"4".repeat(64),"source_order_source_sha256":"5".repeat(64)});
        let receipt = json!({"schema":"borsuk-cohere-native-cohort-receipt-v3","status":"COMPLETE",
            "dataset":"CohereLabs/wikipedia-2023-11-embed-multilingual-v3",
            "revision":"ade45fb52bd549f5e8c065636fe4160a43c2af36","reserved_queries_sha256":GOLDEN_RESERVED,
            "geometry":{"corpus_rows":1_000_000,"query_rows":count,"dimensions":1024,"k":10,
                "corpus_intervals":intervals,"reserved_query_interval":reserved,"query_source_ordinals":[100_000,100_000+count]},
            "outputs":[{"name":"corpus.f32","bytes":4_096_000_000_u64,"sha256":"c".repeat(64)},
                {"name":"queries.f32","bytes":count*4096,"sha256":requests},
                {"name":"truth.u64","bytes":count*80,"sha256":truth}]});
        let cohort = put_json(&dir.join("cohort.json"), &receipt);
        let mut original_cohort = cohort.clone();
        original_cohort["path"] = json!("/original/cohort.json");
        let config = json!({"schema":"borsuk-cohere-native-baseline-config-v7",
            "dataset":"CohereLabs/wikipedia-2023-11-embed-multilingual-v3",
            "revision":"ade45fb52bd549f5e8c065636fe4160a43c2af36","metric":"cosine","tie_rule":"corpus_ordinal_ascending",
            "rows":1_000_000,"dimensions":1024,"count":count,"k":10,"corpus_source_first":0,"query_source_first":100_000,
            "profile":"scale1m","backend":backend,"generation_prefix":"generation","generation_root_sha256":"a".repeat(64),
            "scratch_parent":"/original/scratch","requests":{"path":"/original/queries.f32","bytes":count*4096,"sha256":requests},
            "truth":{"path":"/original/truth.u64","bytes":count*80,"sha256":truth},
            "native_source":{"source_sha256":"d".repeat(64),"sq8_sha256":"e".repeat(64),"source_order_sha256":"f".repeat(64)},
            "corpus_intervals":intervals,"reserved_query_interval":reserved,"cohort_receipt":original_cohort,
            "derivation_receipt":{"path":"/original/derivation.json","bytes":4096,"sha256":"6".repeat(64)},
            "producer_authority":producer,"max_memory_bytes":536_870_912,"fetch_parallelism":16,
            "serving":{"mode":"baseline"},"execution":execution});
        let config_pin = put_json(&dir.join("config.json"), &config);
        let sources = json!({
            "runner_source_sha256":"d5e593e5f8e2aa02697b366cc3bcd8e6fa4160071ea0e2ac337063f9ff0023c4",
            "generation_source_sha256":"6acb7cdbb23d790aee8bfbcba8924ae09f0c7c7738e87ca2b3e40006fd0e3f38",
            "router_source_sha256":"2961d9295d49e217e4b8c24ef734b09f079fa112306e65655962cebba17c7ae5",
            "codec_source_sha256":"eddf88c6c8ee23a732751f49291293f1cc081545ebb9a21c149eb757aa5c38ec",
            "source_plane_source_sha256":"dbcc4cdbc4bb5c244354b375afd657f8b5cb1892df3f0ff0b8f41ef580de42e0",
            "sq8_range_source_sha256":"3f6407664d5f2c1be817e4b32e6a9dce8d4bf8e66ca455e237d0e7b0716ad711",
            "returned_source_sha256":"a4dcc7f9bc06cdea835f72860f3d2e796fcb6f29276858c349d624884cc9f74b"
        }).as_object().unwrap().clone();
        // Independent full native-emission golden: never call expected_scale_headers here.
        let mut identity = json!({"schema":"borsuk-cohere-native-baseline-result-v7","phase":"identity",
            "config_sha256":config_pin["sha256"],"binary_sha256":GOLDEN_BINARY,
            "fetch_parallelism":16,"serving":{"mode":"baseline"},"execution":execution,
            "scope":"AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS","physical_s3_measured":false,
            "io_measurement":"logical_GET_charges_separate_from_cumulative_process_native_transport",
            "s3_credential_source":"imds_instance_role_only",
            "native_transport_includes":"S3_and_IMDS_credential_requests_including_PUT",
            "wire_bytes":null,"unread_bytes":null,"billed_bytes":null,"billed_requests":null,
            "external_gate_required":true,"truth_opened":false});
        identity.as_object_mut().unwrap().extend(sources.clone());
        let bound = json!({"phase":"bound_inputs","dataset":"CohereLabs/wikipedia-2023-11-embed-multilingual-v3",
            "revision":"ade45fb52bd549f5e8c065636fe4160a43c2af36","metric":"cosine","tie_rule":"corpus_ordinal_ascending",
            "rows":1_000_000,"dimensions":1024,"count":count,"k":10,"corpus_source_first":0,"query_source_first":100_000,
            "profile":"scale1m","backend":backend,"generation_prefix":"generation","generation_root_sha256":"a".repeat(64),
            "credential_source":if s3 { json!("imds_instance_role_only") } else { Value::Null },
            "requests_bytes":count*4096,"requests_sha256":requests,"truth_bytes":count*80,"truth_sha256":truth,
            "native_source_sha256":"d".repeat(64),"native_sq8_sha256":"e".repeat(64),"native_order_sha256":"f".repeat(64),
            "corpus_intervals":intervals,"reserved_query_interval":reserved,"reserved_queries_sha256":GOLDEN_RESERVED,
            "cohort_receipt_sha256":cohort["sha256"],"derivation_receipt_sha256":"6".repeat(64),"producer_authority":producer,
            "max_memory_bytes":536_870_912,"fetch_parallelism":16,"serving":{"mode":"baseline"},"source_cache":"off",
            "execution":execution,"selected_count":32,"truth_opened":false});
        let headers = [identity, bound];
        let result = put(&dir.join("result.jsonl"), &result_body(&headers));
        (
            json!({"schema":HEADER_AUTHORITY_SCHEMA,"config":config_pin,"result":result,
            "cohort":cohort,"binary_sha256":GOLDEN_BINARY,"sources":sources}),
            config,
            receipt,
            headers,
        )
    }

    #[test]
    fn bind_completed_scale_independent_headers() {
        for (count, s3) in [(32, false), (1000, false), (1000, true)] {
            let dir = tempfile::tempdir().unwrap();
            let (a, _, _, expected) = fixture(dir.path(), count, s3);
            let value = bind(dir.path(), &a).unwrap();
            let config: CompletedConfig = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(config.schema, SCALE_CONFIG_SCHEMA);
            assert_eq!(config.expected_identity, expected[0]);
            assert_eq!(config.expected_bound_inputs, expected[1]);
            assert_eq!(value.as_object().unwrap().len(), 4);
            assert!(value.get("status").is_none());
            let output = dir.path().join("completed.json");
            assert!(execute_report(&output, SCALE_CONFIG_SCHEMA, || Ok(value.clone())).unwrap());
            let body = std::fs::read(&output).unwrap();
            assert!(body.len() <= CONFIG_CAP as usize);
            let (roundtrip, _): (CompletedConfig, _) = read_config(&output, &sha(&body)).unwrap();
            assert_eq!(roundtrip.expected_bound_inputs, expected[1]);
            assert!(
                execute_report(&output, SCALE_CONFIG_SCHEMA, || panic!("occupied output")).is_err()
            );
            assert_eq!(std::fs::read(output).unwrap(), body);
            let authority = dir.path().join("authority.json");
            let pin = put_json(&authority, &a);
            let binding =
                bind_completed_scale(&authority, pin["sha256"].as_str().unwrap()).unwrap();
            let native_output = dir.path().join("native-completed.json");
            assert!(publish_bound_headers(&native_output, binding).unwrap());
            let published = std::fs::read(&native_output).unwrap();
            assert_eq!(serde_json::from_slice::<Value>(&published).unwrap(), value);
            let binding =
                bind_completed_scale(&authority, pin["sha256"].as_str().unwrap()).unwrap();
            assert!(publish_bound_headers(&native_output, binding).is_err());
            assert_eq!(std::fs::read(&native_output).unwrap(), published);
        }
    }

    #[test]
    fn bind_completed_scale_native_default_and_full_execution() {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut c, _, mut rows) = fixture(dir.path(), 1000, false);
        c.as_object_mut().unwrap().remove("fetch_parallelism");
        c["execution"] = json!({"mode":"full"});
        a["config"] = put_json(&dir.path().join("config.json"), &c);
        rows[0]["config_sha256"] = a["config"]["sha256"].clone();
        for row in &mut rows {
            row["execution"] = json!({"mode":"full"});
        }
        rows[1]["selected_count"] = json!(1000);
        a["result"] = put(&dir.path().join("result.jsonl"), &result_body(&rows));
        let value = bind(dir.path(), &a).unwrap();
        assert_eq!(value["expected_identity"], rows[0]);
        assert_eq!(value["expected_bound_inputs"], rows[1]);
        c["fetch_parallelism"] = json!(32);
        a["config"] = put_json(&dir.path().join("config.json"), &c);
        rows[0]["config_sha256"] = a["config"]["sha256"].clone();
        for row in &mut rows {
            row["fetch_parallelism"] = json!(32);
        }
        a["result"] = put(&dir.path().join("result.jsonl"), &result_body(&rows));
        assert_eq!(
            bind(dir.path(), &a).unwrap()["expected_bound_inputs"],
            rows[1]
        );
    }

    #[test]
    fn bind_completed_scale_rejects_pins_geometry_and_cohort() {
        let dir = tempfile::tempdir().unwrap();
        let (a, c, receipt, _) = fixture(dir.path(), 1000, true);
        let authority_path = dir.path().join("authority.json");
        put_json(&authority_path, &a);
        assert!(bind_completed_scale(&authority_path, &"0".repeat(64)).is_err());
        for (name, _) in BASELINE_HEADER_SOURCES {
            let mut bad = a.clone();
            bad["sources"][name] = json!("0".repeat(64));
            assert!(
                bind(dir.path(), &bad)
                    .unwrap_err()
                    .to_string()
                    .contains("component source")
            );
        }
        for pointer in [
            "/schema",
            "/binary_sha256",
            "/config/sha256",
            "/cohort/sha256",
            "/result/sha256",
        ] {
            let mut bad = a.clone();
            *bad.pointer_mut(pointer).unwrap() = json!("0".repeat(64));
            assert!(bind(dir.path(), &bad).is_err(), "{pointer}");
        }
        for (pointer, value) in [
            ("/rows", json!(100_000)),
            ("/dimensions", json!(true)),
            ("/count", json!(1000.0)),
            ("/k", json!(false)),
            ("/profile", json!("native100k")),
            ("/fetch_parallelism", json!(true)),
            ("/max_memory_bytes", json!(1)),
            ("/serving/mode", json!("direct_closure")),
            ("/execution/trace", json!(0)),
            ("/execution/ordinals/0", json!(false)),
            ("/corpus_intervals/1/start", json!(100_000)),
            ("/backend/kind", json!("unknown")),
            ("/producer_authority/executable_sha256", json!(false)),
            ("/native_source/source_sha256", json!("bad")),
            ("/cohort_receipt/sha256", json!("0".repeat(64))),
            ("/cohort_receipt/bytes", json!(true)),
            ("/requests/bytes", json!(true)),
            ("/requests/sha256", json!("0".repeat(64))),
        ] {
            let mut bad_c = c.clone();
            *bad_c.pointer_mut(pointer).unwrap() = value;
            let mut bad = a.clone();
            bad["config"] = put_json(&dir.path().join("bad-config.json"), &bad_c);
            let error = bind(dir.path(), &bad).unwrap_err().to_string();
            assert!(
                !error.contains("differ from independent authority"),
                "{pointer}: {error}"
            );
        }
        // Repin both inputs and matching emitted headers: header mismatch must not
        // hide admission of a descriptor the original native executable rejects.
        for (pointer, value) in [
            ("/generation_prefix", json!("../x")),
            ("/backend/bucket", json!("Bad_Bucket")),
            ("/backend/bucket", json!("127.0.0.1")),
            ("/backend/region", json!("-eu-central-1")),
            ("/backend/physical_prefix", json!("retained//x")),
            (
                "/backend/sq8_object_key",
                json!(format!("../objects/{}", "e".repeat(64))),
            ),
            ("/backend/sq8_etag", json!("unquoted")),
            ("/backend/sq8_etag", json!("\"bad\"quote\"")),
        ] {
            let (mut bad, mut config, _, mut rows) = fixture(dir.path(), 1000, true);
            *config.pointer_mut(pointer).unwrap() = value.clone();
            *rows[1].pointer_mut(pointer).unwrap() = value;
            bad["config"] = put_json(&dir.path().join("bad-config.json"), &config);
            rows[0]["config_sha256"] = bad["config"]["sha256"].clone();
            bad["result"] = put(&dir.path().join("bad-result.jsonl"), &result_body(&rows));
            assert!(
                bind(dir.path(), &bad).is_err(),
                "native descriptor: {pointer}"
            );
        }
        for (pointer, value) in [
            ("/schema", json!("wrong")),
            ("/status", json!("INCOMPLETE")),
            ("/reserved_queries_sha256", json!("0".repeat(64))),
            ("/geometry/query_rows", json!(32)),
            ("/geometry/k", json!(true)),
            ("/outputs/1/sha256", json!("0".repeat(64))),
            ("/outputs/2/bytes", json!(true)),
            ("/outputs/0/bytes", json!(1)),
        ] {
            let mut bad_receipt = receipt.clone();
            *bad_receipt.pointer_mut(pointer).unwrap() = value;
            let mut bad = a.clone();
            bad["cohort"] = put_json(&dir.path().join("bad-cohort.json"), &bad_receipt);
            let mut bad_c = c.clone();
            bad_c["cohort_receipt"] = bad["cohort"].clone();
            bad["config"] = put_json(&dir.path().join("bad-config.json"), &bad_c);
            let error = bind(dir.path(), &bad).unwrap_err().to_string();
            assert!(
                !error.contains("differ from independent authority"),
                "{pointer}: {error}"
            );
        }
    }

    #[test]
    fn bind_completed_scale_rejects_header_value_type_unknown_and_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let (a, c, receipt, rows) = fixture(dir.path(), 32, false);
        for (index, pointer, value) in [
            (0, "/phase", json!("bound_inputs")),
            (0, "/truth_opened", json!(0)),
            (0, "/fetch_parallelism", json!(16.0)),
            (0, "/external_gate_required", json!(1)),
            (1, "/phase", json!("identity")),
            (1, "/rows", json!(true)),
            (1, "/backend/store_root", json!("/changed/store")),
            (1, "/selected_count", json!(31)),
        ] {
            let mut bad_rows = rows.clone();
            *bad_rows[index].pointer_mut(pointer).unwrap() = value;
            let mut bad = a.clone();
            bad["result"] = put(&dir.path().join("bad-result"), &result_body(&bad_rows));
            assert!(
                bind(dir.path(), &bad)
                    .unwrap_err()
                    .to_string()
                    .contains("differ from independent authority"),
                "{pointer}"
            );
        }
        for pointer in ["", "/backend", "/execution", "/producer_authority"] {
            let mut bad_rows = rows.clone();
            bad_rows[1].pointer_mut(pointer).unwrap()["unexpected"] = json!(true);
            let mut bad = a.clone();
            bad["result"] = put(&dir.path().join("bad-result"), &result_body(&bad_rows));
            assert!(bind(dir.path(), &bad).is_err());
        }
        for role in ["authority", "config", "cohort", "result"] {
            let original = match role {
                "authority" => serde_json::to_vec(&a).unwrap(),
                "config" => serde_json::to_vec(&c).unwrap(),
                "cohort" => serde_json::to_vec(&receipt).unwrap(),
                _ => result_body(&rows),
            };
            let original = String::from_utf8(original).unwrap();
            let duplicated = original.replacen('{', "{\"duplicate\":0,\"duplicate\":1,", 1);
            let mut bad = a.clone();
            let path = dir.path().join("duplicate.json");
            let pin = put(&path, duplicated.as_bytes());
            let error = if role == "authority" {
                bind_completed_scale(&path, pin["sha256"].as_str().unwrap()).unwrap_err()
            } else {
                bad[role] = pin;
                bind(dir.path(), &bad).unwrap_err()
            };
            assert!(error.to_string().contains("duplicate JSON key"));
        }
        for pointer in ["", "/sources", "/config", "/cohort", "/result"] {
            let mut bad = a.clone();
            bad.pointer_mut(pointer).unwrap()["unexpected"] = json!(true);
            assert!(bind(dir.path(), &bad).is_err(), "{pointer}");
        }
        for role in ["config", "result"] {
            for policy in ["execution", "serving", "backend", "producer_authority"] {
                let original = if role == "config" {
                    serde_json::to_vec(&c).unwrap()
                } else {
                    result_body(&rows)
                };
                let original = String::from_utf8(original).unwrap();
                let needle = format!("\"{policy}\":{{");
                assert!(original.contains(&needle));
                let duplicated = original.replacen(
                    &needle,
                    &format!("{needle}\"duplicate\":0,\"duplicate\":1,"),
                    1,
                );
                let mut bad = a.clone();
                bad[role] = put(
                    &dir.path().join("nested-duplicate.json"),
                    duplicated.as_bytes(),
                );
                assert!(
                    bind(dir.path(), &bad)
                        .unwrap_err()
                        .to_string()
                        .contains("duplicate JSON key"),
                    "nested duplicate {role}/{policy}"
                );
            }
        }
        for pointer in [
            "",
            "/backend",
            "/execution",
            "/serving",
            "/requests",
            "/native_source",
        ] {
            let mut bad_c = c.clone();
            bad_c.pointer_mut(pointer).unwrap()["unexpected"] = json!(true);
            let mut bad = a.clone();
            bad["config"] = put_json(&dir.path().join("unknown-config.json"), &bad_c);
            let error = bind(dir.path(), &bad).unwrap_err().to_string();
            assert!(
                !error.contains("differ from independent authority"),
                "{pointer}: {error}"
            );
        }
    }

    #[test]
    fn bind_completed_scale_authenticates_whole_result() {
        let dir = tempfile::tempdir().unwrap();
        let (a, _, _, rows) = fixture(dir.path(), 32, false);
        let original = result_body(&rows);
        let path = dir.path().join("result.jsonl");
        for change in 0..3 {
            let mut body = original.clone();
            match change {
                0 => {
                    body.pop();
                }
                1 => body.push(b' '),
                _ => {
                    let last = body.len() - 2;
                    body[last] ^= 1;
                }
            }
            std::fs::write(&path, body).unwrap();
            assert!(bind(dir.path(), &a).is_err());
        }
        // A malformed first row must not be parsed before a wrong tail SHA is rejected.
        let body = b"not JSON\n{}\ntail\n";
        let mut bad = a.clone();
        bad["result"] = put(&path, body);
        bad["result"]["sha256"] = json!("0".repeat(64));
        assert!(
            bind(dir.path(), &bad)
                .unwrap_err()
                .to_string()
                .contains("whole-body SHA/EOF")
        );
        for body in [b"{}\n{}".to_vec(), vec![b' '; HEADER_JSON_CAP as usize + 1]] {
            bad["result"] = put(&path, &body);
            assert!(bind(dir.path(), &bad).is_err());
        }
        for (role, cap) in [
            ("config", HEADER_JSON_CAP),
            ("cohort", HEADER_JSON_CAP),
            ("result", HEADER_RESULT_CAP),
        ] {
            let mut bad = a.clone();
            bad[role]["bytes"] = json!(cap + 1);
            assert!(bind(dir.path(), &bad).is_err());
        }
    }

    #[test]
    fn bind_completed_scale_rejects_links_mutation_and_occupied_output() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let (a, _, _, _) = fixture(dir.path(), 32, false);
        let path = dir.path().join("result.jsonl");
        let link = dir.path().join("result-link");
        symlink(&path, &link).unwrap();
        let mut bad = a.clone();
        bad["result"]["path"] = json!(link);
        assert!(bind(dir.path(), &bad).is_err());
        let parent_link = dir.path().join("parent-link");
        symlink(dir.path(), &parent_link).unwrap();
        bad["result"]["path"] = json!(parent_link.join("result.jsonl"));
        assert!(bind(dir.path(), &bad).is_err());
        let hard = dir.path().join("hard-link");
        std::fs::hard_link(&path, &hard).unwrap();
        assert!(bind(dir.path(), &a).is_err());
        std::fs::remove_file(hard).unwrap();
        let file = header_file(&path).unwrap();
        let stamp = file_identity(&file).unwrap();
        let body = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, &body).unwrap();
        assert!(recheck_header_file(&file, &path, &stamp).is_err());
        let file = header_file(&path).unwrap();
        let stamp = file_identity(&file).unwrap();
        let mut changed = body;
        changed[0] ^= 1;
        std::fs::write(&path, &changed).unwrap();
        assert!(recheck_header_file(&file, &path, &stamp).is_err());
        assert!(execute_report(&link, SCALE_CONFIG_SCHEMA, || panic!("occupied symlink")).is_err());
        let fifo = dir.path().join("fifo");
        rustix::fs::mkfifoat(rustix::fs::CWD, &fifo, Mode::RUSR | Mode::WUSR).unwrap();
        assert!(header_file(&fifo).is_err());
    }

    #[test]
    fn bind_completed_scale_publication_rejects_mutation() {
        for role in 0..4 {
            let dir = tempfile::tempdir().unwrap();
            let (a, _, _, _) = fixture(dir.path(), 32, false);
            let authority = dir.path().join("authority.json");
            let pin = put_json(&authority, &a);
            let binding =
                bind_completed_scale(&authority, pin["sha256"].as_str().unwrap()).unwrap();
            let input = binding.inputs[role].path.clone();
            let output = dir.path().join("completed.json");
            assert!(
                publish_bound_headers_with_sync(&output, binding, |file, directory| {
                    file.sync_all()?;
                    if directory {
                        let mut body = std::fs::read(&input)?;
                        body[0] ^= 1;
                        std::fs::write(&input, body)?;
                    }
                    Ok(())
                })
                .is_err(),
                "input role {role} changed at directory sync"
            );
        }
        for role in 0..4 {
            for mutation in 0..3 {
                let dir = tempfile::tempdir().unwrap();
                let (a, _, _, _) = fixture(dir.path(), 32, false);
                let authority = dir.path().join("authority.json");
                let pin = put_json(&authority, &a);
                let binding =
                    bind_completed_scale(&authority, pin["sha256"].as_str().unwrap()).unwrap();
                let path = binding.inputs[role].path.clone();
                let body = std::fs::read(&path).unwrap();
                match mutation {
                    0 => {
                        let mut changed = body.clone();
                        changed[0] ^= 1;
                        std::fs::write(&path, changed).unwrap();
                    }
                    1 => {
                        std::fs::remove_file(&path).unwrap();
                        std::fs::write(&path, body).unwrap();
                    }
                    _ => std::fs::hard_link(&path, dir.path().join("input-alias")).unwrap(),
                }
                let output = dir.path().join("completed.json");
                assert!(
                    publish_bound_headers(&output, binding).is_err(),
                    "input role {role}, mutation {mutation}"
                );
                assert!(!output.exists());
            }
        }
        for directory_boundary in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (a, _, _, _) = fixture(dir.path(), 32, false);
            let authority = dir.path().join("authority.json");
            let pin = put_json(&authority, &a);
            let binding =
                bind_completed_scale(&authority, pin["sha256"].as_str().unwrap()).unwrap();
            let output = dir.path().join("completed.json");
            let outcome = publish_bound_headers_with_sync(&output, binding, |file, directory| {
                file.sync_all()?;
                if directory == directory_boundary {
                    let mut body = std::fs::read(&output)?;
                    body[0] ^= 1;
                    std::fs::write(&output, body)?;
                }
                Ok(())
            });
            assert!(
                outcome.is_err(),
                "same-length output mutation, directory={directory_boundary}"
            );
        }
        for fail_directory in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (a, _, _, _) = fixture(dir.path(), 32, false);
            let authority = dir.path().join("authority.json");
            let pin = put_json(&authority, &a);
            let binding =
                bind_completed_scale(&authority, pin["sha256"].as_str().unwrap()).unwrap();
            assert!(
                publish_bound_headers_with_sync(
                    &dir.path().join("completed.json"),
                    binding,
                    |file, directory| {
                        if directory == fail_directory {
                            return Err(io::Error::other("injected sync failure"));
                        }
                        file.sync_all()
                    }
                )
                .is_err()
            );
        }
        let dir = tempfile::tempdir().unwrap();
        let (a, _, _, _) = fixture(dir.path(), 32, false);
        let value = bind(dir.path(), &a).unwrap();
        for hard_link in [false, true] {
            let output = dir.path().join(if hard_link {
                "hard-output"
            } else {
                "replaced-output"
            });
            let result = execute_report(&output, SCALE_CONFIG_SCHEMA, || {
                if hard_link {
                    std::fs::hard_link(&output, dir.path().join("output-alias"))?;
                } else {
                    std::fs::remove_file(&output)?;
                    std::fs::write(&output, b"replacement")?;
                }
                Ok(value.clone())
            });
            assert!(result.is_err());
        }
    }
}

fn reduce_completed(config_path: &Path, config_sha: &str) -> Result<Value> {
    let (c, config_bytes): (CompletedConfig, _) = read_config(config_path, config_sha)?;
    require(
        c.schema == "borsuk-completed-native-reduction-config-v1",
        "reduction config schema",
    )?;
    let run = read_run_with(&c.input.path, &c.input.sha256, Some(&c))?;
    let mut report = completed_report(&c, &run)?;
    report["config_path"] = json!(config_path);
    report["config_sha256"] = json!(config_sha);
    report["config_bytes"] = json!(config_bytes);
    Ok(report)
}

const SCALE_PARITY_CONFIG_SCHEMA: &str = "borsuk-scale-prefix-parity-config-v1";
const SCALE_PARITY_REPORT_SCHEMA: &str = "borsuk-scale-prefix-parity-v1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScalePrefixParityConfig {
    schema: String,
    // Historical local Q32, full-input local panel, full-input S3 panel.
    runs: [CompletedConfig; 3],
    requests: [CompletedInput; 2],
    truth: [CompletedInput; 2],
}

fn parity_invariant_rows(c: &CompletedConfig) -> Result<(Value, Value)> {
    let mut identity = c.expected_identity.clone();
    let identity = identity.as_object_mut().ok_or("parity identity object")?;
    for key in ["config_sha256", "execution"] {
        require(
            identity.remove(key).is_some(),
            "missing parity identity field",
        )?;
    }
    let identity = Value::Object(identity.clone());
    let mut inputs = c.expected_bound_inputs.clone();
    let inputs = inputs.as_object_mut().ok_or("parity input object")?;
    // Each exception is independently validated and pinned in the scale reader.
    // Every other field, including unknown additions, must remain exactly equal.
    for key in [
        "count",
        "requests_bytes",
        "requests_sha256",
        "truth_bytes",
        "truth_sha256",
        "backend",
        "credential_source",
        "generation_prefix",
        "generation_root_sha256",
        "cohort_receipt_sha256",
        "derivation_receipt_sha256",
        "execution",
    ] {
        require(inputs.remove(key).is_some(), "missing parity input field")?;
    }
    Ok((identity, Value::Object(inputs.clone())))
}

fn parity_samples(a: &[Sample], b: &[Sample]) -> Result<()> {
    require(
        a.len() == 32 && b.len() == 32,
        "parity requires exactly32 queries",
    )?;
    for (ordinal, (a, b)) in a.iter().zip(b).enumerate() {
        require(
            a.returned.len() == K
                && b.returned.len() == K
                && a.returned == b.returned
                && a.hits10 == b.hits10
                && a.charges == b.charges
                && a.charges.sum()?.failed_gets == 0
                && b.charges.sum()?.failed_gets == 0,
            &format!("scale parity query{ordinal}: ordered IDs/score bits/recall/charges mismatch"),
        )?;
    }
    Ok(())
}

fn authenticated_parity_file(pin: &CompletedInput) -> Result<(File, FileIdentity)> {
    require(
        valid_sha(&pin.sha256) && pin.bytes > 0 && pin.bytes <= 4_096_000,
        "bounded parity file pin",
    )?;
    let mut file = open_input(&pin.path)?;
    let original = file_identity(&file)?;
    require(original.len == pin.bytes, "parity exact file length")?;
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 8192];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        total = plus(total, n as u64)?;
        require(total <= pin.bytes, "parity file growth")?;
        digest.update(&buffer[..n]);
    }
    require(
        total == pin.bytes
            && format!("{:x}", digest.finalize()) == pin.sha256
            && file_identity(&file)? == original,
        "parity full-body authentication/mutation",
    )?;
    file.rewind()?;
    Ok((file, original))
}

fn authenticated_prefix_pair(pins: &[CompletedInput; 2]) -> Result<()> {
    require(
        pins[0].bytes < pins[1].bytes,
        "strict parity prefix geometry",
    )?;
    let (mut prefix, prefix_identity) = authenticated_parity_file(&pins[0])?;
    let (mut full, full_identity) = authenticated_parity_file(&pins[1])?;
    require(
        (prefix_identity.dev, prefix_identity.ino) != (full_identity.dev, full_identity.ino),
        "distinct prefix/full descriptors",
    )?;
    let mut remaining = pins[0].bytes;
    let mut a = [0_u8; 8192];
    let mut b = [0_u8; 8192];
    while remaining > 0 {
        let n = usize::try_from(remaining.min(a.len() as u64))?;
        prefix.read_exact(&mut a[..n])?;
        full.read_exact(&mut b[..n])?;
        require(
            a[..n] == b[..n],
            "authenticated request/truth prefix mismatch",
        )?;
        remaining -= n as u64;
    }
    require(
        file_identity(&prefix)? == prefix_identity && file_identity(&full)? == full_identity,
        "prefix comparison descriptor mutation",
    )?;
    recheck_parity_path(&pins[0], &prefix_identity)?;
    recheck_parity_path(&pins[1], &full_identity)
}

fn recheck_parity_path(pin: &CompletedInput, original: &FileIdentity) -> Result<()> {
    require(
        file_identity(&open_input(&pin.path)?)? == *original,
        "authenticated parity pathname replacement",
    )
}

fn parity_role_identities(c: &ScalePrefixParityConfig) -> Result<[(u64, u64); 3]> {
    let pins = [
        &c.runs[0].input,
        &c.runs[1].input,
        &c.runs[2].input,
        &c.requests[0],
        &c.requests[1],
        &c.truth[0],
        &c.truth[1],
    ];
    let mut identities = Vec::with_capacity(pins.len());
    for pin in pins {
        // Metadata only: never open request/truth content before run seals.
        let dir = parent(&pin.path)?;
        let entry = rustix::fs::statat(
            &dir,
            pin.path.file_name().ok_or("parity filename")?,
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?;
        let identity = (entry.st_dev, entry.st_ino);
        require(!identities.contains(&identity), "parity file role alias")?;
        identities.push(identity);
    }
    Ok([identities[0], identities[1], identities[2]])
}

fn reduce_scale_prefix_parity(config_path: &Path, config_sha: &str) -> Result<Value> {
    let (c, config_bytes): (ScalePrefixParityConfig, _) = read_config(config_path, config_sha)?;
    require(
        c.schema == SCALE_PARITY_CONFIG_SCHEMA,
        "scale prefix parity config schema",
    )?;
    let panel =
        json!({"mode":"diagnostic_panel", "ordinals":(0..32).collect::<Vec<_>>(), "trace":false});
    for (index, arm) in c.runs.iter().enumerate() {
        let inputs = &arm.expected_bound_inputs;
        require(
            arm.schema == SCALE_CONFIG_SCHEMA
                && inputs["rows"] == 1_000_000
                && inputs["profile"] == "scale1m"
                && inputs["count"] == if index == 0 { 32 } else { 1000 }
                && inputs["selected_count"] == 32
                && inputs["execution"]
                    == if index == 0 {
                        json!({"mode":"full"})
                    } else {
                        panel.clone()
                    }
                && inputs["backend"]["kind"] == if index == 2 { "s3" } else { "local" },
            "parity historical/local/S3 geometry and execution",
        )?;
        require(
            parity_invariant_rows(arm)? == parity_invariant_rows(&c.runs[0])?,
            "parity source/corpus/query policy invariant mismatch",
        )?;
        let pin_index = usize::from(index != 0);
        for (pins, bytes_key, sha_key) in [
            (&c.requests, "requests_bytes", "requests_sha256"),
            (&c.truth, "truth_bytes", "truth_sha256"),
        ] {
            require(
                inputs[bytes_key] == pins[pin_index].bytes
                    && inputs[sha_key] == pins[pin_index].sha256,
                "parity file pins must bind exact run inputs",
            )?;
        }
    }
    require(
        c.requests[0].bytes == 131_072
            && c.requests[1].bytes == 4_096_000
            && c.truth[0].bytes == 2560
            && c.truth[1].bytes == 80_000,
        "parity fixed request/truth byte geometry",
    )?;
    for key in ["cohort_receipt_sha256", "derivation_receipt_sha256"] {
        require(
            c.runs[1].expected_bound_inputs[key] == c.runs[2].expected_bound_inputs[key],
            "local/S3 panel must use same full input provenance",
        )?;
    }
    // Complete every seal/terminal validation before any request or truth file opens.
    let admitted = parity_role_identities(&c)?;
    let runs = c
        .runs
        .iter()
        .zip(admitted)
        .map(|(arm, inode)| {
            read_run_observed_with_identity(
                &arm.input.path,
                &arm.input.sha256,
                Some(arm),
                Some(inode),
                |_, _, _| Ok(()),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    for i in 0..runs.len() {
        for j in 0..i {
            require(
                c.runs[i].input.sha256 != c.runs[j].input.sha256
                    && c.runs[i].expected_identity["config_sha256"]
                        != c.runs[j].expected_identity["config_sha256"]
                    && (runs[i].file_identity.dev, runs[i].file_identity.ino)
                        != (runs[j].file_identity.dev, runs[j].file_identity.ino),
                "distinct parity run artifacts",
            )?;
        }
    }
    parity_samples(&runs[0].samples, &runs[1].samples)?;
    parity_samples(&runs[1].samples, &runs[2].samples)?;
    authenticated_prefix_pair(&c.requests)?;
    authenticated_prefix_pair(&c.truth)?;
    for (arm, run) in c.runs.iter().zip(&runs) {
        recheck_parity_path(&arm.input, &run.file_identity)?;
    }
    Ok(
        json!({"schema":SCALE_PARITY_REPORT_SCHEMA,"status":"EXACT_PREFIX_PARITY",
        "config_sha256":config_sha,"config_bytes":config_bytes,"complete":true,"matched_queries":32,
        "ordered_ids_and_score_bits_equal":true,"per_query_logical_charges_equal":true,
        "request_and_truth_prefix_authenticated":true,
        "run_sha256":c.runs.iter().map(|r| &r.input.sha256).collect::<Vec<_>>(),
        "external_generation_provenance_gate_required":true,
        "cold_s3_claim":false,"performance_pass_claim":false,"vendor_or_scientific_win_claim":false}),
    )
}

#[cfg(test)]
mod scale_prefix_parity_tests {
    use super::*;

    fn pin(path: PathBuf, body: &[u8]) -> CompletedInput {
        std::fs::write(&path, body).unwrap();
        CompletedInput {
            path,
            bytes: body.len() as u64,
            sha256: format!("{:x}", Sha256::digest(body)),
        }
    }

    #[test]
    fn scale_prefix_authenticates_both_bodies_and_refuses_suffix_and_prefix_changes() {
        let dir = tempfile::tempdir().unwrap();
        let pins = [
            pin(dir.path().join("prefix"), b"abc"),
            pin(dir.path().join("full"), b"abcdef"),
        ];
        authenticated_prefix_pair(&pins).unwrap();
        std::fs::write(&pins[1].path, b"abcdeg").unwrap();
        assert!(authenticated_prefix_pair(&pins).is_err());
        let changed = [
            pin(dir.path().join("other"), b"abd"),
            pin(dir.path().join("full"), b"abcdef"),
        ];
        assert!(authenticated_prefix_pair(&changed).is_err());
        std::fs::write(&changed[1].path, b"abcde").unwrap();
        assert!(authenticated_prefix_pair(&changed).is_err());
        std::fs::write(&changed[1].path, b"abcdefg").unwrap();
        assert!(authenticated_prefix_pair(&changed).is_err());
    }

    #[test]
    fn scale_parity_refuses_path_replacement_even_with_identical_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let original = pin(dir.path().join("original"), b"authenticated");
        let (_file, stamp) = authenticated_parity_file(&original).unwrap();
        recheck_parity_path(&original, &stamp).unwrap();
        let replacement = pin(dir.path().join("replacement"), b"authenticated");
        std::fs::rename(&replacement.path, &original.path).unwrap();
        assert!(recheck_parity_path(&original, &stamp).is_err());
    }

    fn samples() -> Vec<Sample> {
        (0..32)
            .map(|ordinal| Sample {
                returned: (0..K)
                    .map(|id| Hit {
                        id: (ordinal * K + id) as u64,
                        score_bits: u64::from((id as f32).to_bits()),
                    })
                    .collect(),
                charges: Charges::default(),
                hits10: K as u64,
                wall_ns: 1,
                trace_sha256: [0; 32],
                physical_plan_sha256: [0; 32],
            })
            .collect()
    }

    #[test]
    fn scale_parity_refuses_order_score_recall_underfill_and_per_query_charge_changes() {
        let a = samples();
        parity_samples(&a, &samples()).unwrap();
        for change in 0..7 {
            let mut b = samples();
            match change {
                0 => b[31].returned.swap(0, 1),
                1 => b[31].returned[0].score_bits ^= 1,
                2 => b[31].hits10 -= 1,
                3 => {
                    b[31].returned.pop();
                }
                4 => b[31].charges.source.submitted_gets += 1,
                5 => b[31].charges.sq8.verified_bytes += 1,
                _ => b[31].charges.router.failed_gets += 1,
            }
            assert!(parity_samples(&a, &b).is_err());
        }
        assert!(parity_samples(&a[..31], &a).is_err());
    }

    #[test]
    fn scale_parity_keeps_source_policy_and_unknown_fields_invariant() {
        let mut identity = json!({"binary_sha256":"a", "config_sha256":"b", "execution":{}});
        let mut inputs = json!({"native_sq8_sha256":"x", "fetch_parallelism":16});
        for key in [
            "count",
            "requests_bytes",
            "requests_sha256",
            "truth_bytes",
            "truth_sha256",
            "backend",
            "credential_source",
            "generation_prefix",
            "generation_root_sha256",
            "cohort_receipt_sha256",
            "derivation_receipt_sha256",
            "execution",
        ] {
            inputs[key] = json!("bound");
        }
        let make = |identity, inputs| CompletedConfig {
            schema: SCALE_CONFIG_SCHEMA.into(),
            input: CompletedInput {
                path: PathBuf::from("unused"),
                bytes: 1,
                sha256: "a".repeat(64),
            },
            expected_identity: identity,
            expected_bound_inputs: inputs,
        };
        let first = make(identity.clone(), inputs.clone());
        identity["config_sha256"] = json!("different-bound-config");
        inputs["backend"] = json!("different-bound-backend");
        let changed = make(identity.clone(), inputs.clone());
        assert_eq!(
            parity_invariant_rows(&first).unwrap(),
            parity_invariant_rows(&changed).unwrap()
        );
        for (key, value) in [
            ("native_sq8_sha256", json!("changed")),
            ("fetch_parallelism", json!(32)),
            ("unexpected_policy", json!(true)),
        ] {
            let mut changed_inputs = inputs.clone();
            changed_inputs[key] = value;
            let changed = make(identity.clone(), changed_inputs);
            assert_ne!(
                parity_invariant_rows(&first).unwrap(),
                parity_invariant_rows(&changed).unwrap()
            );
        }
        inputs.as_object_mut().unwrap().remove("backend");
        assert!(parity_invariant_rows(&make(identity, inputs)).is_err());
    }
}

const SOURCE_COVER_CURVE_SCHEMA: &str = "borsuk-source-cover-cost-curve-v1";
const SOURCE_COVER_CURVE_CAPS: [usize; 5] = [128, 96, 64, 48, 32];
const SOURCE_INTERVAL_SCHEMA: &str = "borsuk-native-fetch-intervals-v1";

fn reduce_source_intervals(config_path: &Path, config_sha: &str) -> Result<Value> {
    let (c, config_bytes): (CompletedConfig, _) = read_config(config_path, config_sha)?;
    require(
        c.schema == SCALE_CONFIG_SCHEMA
            && c.expected_bound_inputs["rows"] == 1_000_000
            && c.expected_bound_inputs["dimensions"] == 1024
            && c.expected_bound_inputs["execution"]["mode"] == "full"
            && c.expected_bound_inputs["source_cache"] == "off"
            && c.expected_bound_inputs["fetch_parallelism"] == 64
            && c.expected_bound_inputs
                .get("source_get_budget")
                .is_none_or(|v| v.as_u64() == Some(128)),
        "interval diagnostic requires full width64 cache-off scale input",
    )?;
    let mut intervals = Vec::with_capacity(COUNT);
    let run = read_run_observed(
        &c.input.path,
        &c.input.sha256,
        Some(&c),
        |line, q, inputs| {
            let costs = source_cover_costs(line, q, inputs, &[128])?;
            let &(gets, source_bytes, _, max_source_range) =
                costs.first().ok_or("interval source plan")?;
            let raw: Value = serde_json::from_slice(line)?;
            let ranges: Vec<[u64; 2]> = serde_json::from_value(raw["plan"]["ranges"].clone())?;
            let max_sq8_range = ranges
                .iter()
                .map(|range| range[1] - range[0])
                .max()
                .ok_or("interval SQ8 plan")?;
            require(
                max_source_range > 0 && max_sq8_range > 0,
                "interval nonempty ranges",
            )?;
            intervals.push(json!({"ordinal":q.ordinal,"source_gets":gets,
            "source_phase_bytes":source_bytes,"max_source_range_bytes":max_source_range,
            "sq8_gets":q.charges.sq8.submitted_gets,"sq8_phase_bytes":q.charges.sq8.verified_bytes,
            "max_sq8_range_bytes":max_sq8_range}));
            Ok(())
        },
    )?;
    require(
        run.inputs.count == COUNT && intervals.len() == COUNT,
        "interval complete population",
    )?;
    let maximum = |keys: &[&str]| {
        intervals
            .iter()
            .flat_map(|row| keys.iter().map(move |key| row[*key].as_u64().unwrap_or(0)))
            .max()
            .unwrap_or(0)
    };
    let minimum = maximum(&["max_source_range_bytes", "max_sq8_range_bytes"]);
    let phase_max = maximum(&["source_phase_bytes", "sq8_phase_bytes"]);
    require(
        minimum > 0 && phase_max >= minimum,
        "interval geometry bounds",
    )?;
    require(
        file_identity(&open_input(&c.input.path)?)? == run.file_identity,
        "interval input changed before publication",
    )?;
    read_config::<CompletedConfig>(config_path, config_sha)?;
    Ok(
        json!({"schema":SOURCE_INTERVAL_SCHEMA,"status":"INTERVAL_GEOMETRY_ONLY","complete":true,
        "config_path":config_path,"config_bytes":config_bytes,"config_sha256":config_sha,
        "input":{"path":c.input.path,"bytes":c.input.bytes,"sha256":c.input.sha256},
        "identity":c.expected_identity,"inputs":c.expected_bound_inputs,"count":COUNT,
        "source_get_budget":128,"max_source_range_bytes":maximum(&["max_source_range_bytes"]),
        "max_sq8_range_bytes":maximum(&["max_sq8_range_bytes"]),
        "minimum_feasible_capacity_bytes":minimum,"maximum_phase_bytes":phase_max,
        "distinct_binding_arm":phase_max>minimum,
        "capacity_upper_exclusive_bytes":phase_max,
        "selection":if phase_max>minimum {"FEASIBLE_POTENTIALLY_BINDING_WINDOW"} else {"NO_DISTINCT_BINDING_ARM"},
        "queries":intervals,"waiting_observed":false,"native_ann_called":false,
        "corpus_queries_truth_bodies_opened":false,"claims_latency":false,"claims_quality":false,
        "claims_bill":false,"qualified":false,"root_producer_provenance_and_resource_admission_required":true,
        "reducer_source_sha256":format!("{:x}",Sha256::digest(include_bytes!("compare_native_replay.rs")))}),
    )
}

fn source_cover_costs(
    line: &[u8],
    q: &Query,
    inputs: &Inputs,
    caps: &[usize],
) -> Result<Vec<(u64, u64, bool, u64)>> {
    // Validate recorded nomination, completion, original SOURCE charges and SQ8
    // plan before deriving any counterfactual. Never substitute selected SQ8
    // pages for the full discovered SOURCE closure.
    let coverage = loss_coverage(line, q, inputs)?;
    let pages = coverage
        .scored_units
        .iter()
        .map(|unit| unit / 8)
        .collect::<BTreeSet<_>>();
    let width = source_record_bytes(inputs.dimensions)?;
    // Numeric geometry only: no allocation or GET. Keep costs outside the
    // historical envelope so a larger prospective budget can be assessed.
    let geometry_bytes = inputs
        .rows
        .checked_mul(width)
        .ok_or("source geometry overflow")?;
    caps.iter()
        .map(|&cap| {
            let (ranges, bytes) = borsuk::two_bit_generation::plan_two_bit_source_cover(
                &pages,
                inputs.rows,
                width,
                cap,
                geometry_bytes,
            )?;
            let max_range_bytes = ranges
                .iter()
                .map(|range| range.end - range.start)
                .max()
                .unwrap_or(0);
            Ok((
                ranges.len() as u64,
                bytes as u64,
                bytes <= 64 * 1024 * 1024,
                max_range_bytes as u64,
            ))
        })
        .collect()
}

fn reduce_source_cover_curve(config_path: &Path, config_sha: &str) -> Result<Value> {
    let (c, config_bytes): (CompletedConfig, _) = read_config(config_path, config_sha)?;
    require(
        c.schema == SCALE_CONFIG_SCHEMA
            && c.expected_bound_inputs["rows"] == 1_000_000
            && c.expected_bound_inputs["execution"]["mode"] == "full"
            && c.expected_bound_inputs["source_cache"] == "off"
            && c.expected_bound_inputs
                .get("source_get_budget")
                .is_none_or(|v| v.as_u64() == Some(128)),
        "source cover curve requires full cache-off scale input",
    )?;
    let mut costs: [Vec<(u64, u64, bool, u64)>; 5] =
        std::array::from_fn(|_| Vec::with_capacity(COUNT));
    let run = read_run_observed(
        &c.input.path,
        &c.input.sha256,
        Some(&c),
        |line, q, inputs| {
            for (values, cost) in costs.iter_mut().zip(source_cover_costs(
                line,
                q,
                inputs,
                &SOURCE_COVER_CURVE_CAPS,
            )?) {
                values.push(cost);
            }
            Ok(())
        },
    )?;
    require(
        run.inputs.count == COUNT && run.samples.len() == COUNT && costs[0].len() == COUNT,
        "source cover curve complete population",
    )?;
    let curves = SOURCE_COVER_CURVE_CAPS
        .iter()
        .zip(&costs)
        .map(|(&cap, values)| {
            let gets = values.iter().map(|&(gets, _, _, _)| gets).sum::<u64>();
            let bytes = values.iter().map(|&(_, bytes, _, _)| bytes).sum::<u64>();
            let inside = values.iter().filter(|&&(_, _, inside, _)| inside).count();
            json!({"get_cap":cap,"predicted_queries":values.len(),
                "inside_historical_byte_cap_queries":inside,
                "outside_historical_byte_cap_queries":COUNT-inside,
                "all_queries_inside_historical_byte_cap":inside==COUNT,
                "totals_population":"all authenticated queries, including outside historical envelope",
                "predicted_total_gets":gets,"predicted_total_bytes":bytes,
                "predicted_max_query_bytes":values.iter().map(|&(_,bytes,_,_)|bytes).max()})
        })
        .collect::<Vec<_>>();
    require(
        file_identity(&open_input(&c.input.path)?)? == run.file_identity,
        "source cover input changed before publication",
    )?;
    read_config::<CompletedConfig>(config_path, config_sha)?;
    Ok(
        json!({"schema":SOURCE_COVER_CURVE_SCHEMA,"status":"COUNTERFACTUAL_COST_ONLY","complete":true,
        "config_path":config_path,"config_bytes":config_bytes,"config_sha256":config_sha,
        "input":{"path":c.input.path,"bytes":c.input.bytes,"sha256":c.input.sha256},
        "identity":c.expected_identity,"inputs":c.expected_bound_inputs,"count":COUNT,
        "source_byte_cap":64*1024*1024,"curves":curves,
        "shared_planner":"borsuk::two_bit_generation::plan_two_bit_source_cover",
        "reducer_source_sha256":format!("{:x}",Sha256::digest(include_bytes!("compare_native_replay.rs"))),
        "generation_source_sha256":format!("{:x}",Sha256::digest(include_bytes!("../src/two_bit_generation.rs"))),
        "planner_source_sha256":format!("{:x}",Sha256::digest(include_bytes!("../src/budgeted_page_rank.rs"))),
        "counterfactual_cost_only":true,"native_ann_called":false,
        "corpus_queries_truth_bodies_opened":false,"claims_latency":false,
        "claims_quality":false,"claims_bill":false,"production_change":false,
        "qualified":false,"root_producer_provenance_and_resource_admission_required":true}),
    )
}

fn reduce_scale(config_path: &Path, config_sha: &str) -> Result<Value> {
    let (c, config_bytes): (CompletedConfig, _) = read_config(config_path, config_sha)?;
    require(
        c.schema == SCALE_CONFIG_SCHEMA,
        "scale reduction config v4 required",
    )?;
    let run = read_run_with(&c.input.path, &c.input.sha256, Some(&c))?;
    let mut report = completed_report(&c, &run)?;
    report["schema"] = json!(SCALE_REPORT_SCHEMA);
    report["config_path"] = json!(config_path);
    report["config_sha256"] = json!(config_sha);
    report["config_bytes"] = json!(config_bytes);
    let population = c.expected_bound_inputs["execution"]["mode"] == "full"
        && run.inputs.count == 1000
        && run.samples.len() == 1000;
    report["population_percentiles_valid"] = json!(population);
    report["diagnostic_only"] = json!(!population);
    report["cold_s3_claim"] = json!(false);
    if !population {
        report["statistics"] = Value::Null;
        report["stage_statistics"] = Value::Null;
        report["diagnostic_query_wall_ns"] = json!(run.terminal.query_wall_ns);
        report["diagnostic_query_count"] = json!(run.samples.len());
    }
    Ok(report)
}

fn completed_report(c: &CompletedConfig, run: &Run) -> Result<Value> {
    let e = run.completed.as_ref().ok_or("missing completed evidence")?;
    let stats = statistics(&run.samples.iter().map(|s| s.wall_ns).collect::<Vec<_>>())?;
    let mut distributions = serde_json::Map::new();
    let mut accounted = 0;
    for (name, samples) in ["discovery", "source", "planning", "sq8"]
        .into_iter()
        .zip(&e.stage_samples)
    {
        let (total, [p50, p90, p95, p99]) = quantiles(samples)?;
        accounted = plus(accounted, total)?;
        distributions.insert(name.into(), json!({"count":samples.len(),"total_ns":total,
            "p50_ns":p50,"p90_ns":p90,"p95_ns":p95,"p99_ns":p99,
            "p50_ms":p50 as f64/1e6,"p90_ms":p90 as f64/1e6,"p95_ms":p95 as f64/1e6,"p99_ms":p99 as f64/1e6}));
    }
    let mut report = json!({"schema":COMPLETED_SCHEMA,"status":"MEASURED","complete":true,
        "result_path":c.input.path,"result_bytes":run.file_identity.len,"result_sha256":c.input.sha256,
        "identity":c.expected_identity,"inputs":c.expected_bound_inputs,"statistics":stats,
        "percentile_method":"nearest_rank","sequential_qps_definition":"query_count * 1e9 / sum(query_wall_ns); not concurrent service QPS",
        "stage_wall_sums":run.stages,"stage_statistics":distributions,
        "unattributed_query_wall_ns":stats.query_wall_ns.checked_sub(accounted).ok_or("stage sum exceeds query wall")?,
        "stage_scope":"recorded query intervals; discovery includes preparation and metadata nomination, plus router payload reads only when issued; source is authenticated source fetch, planning is source scoring/SQ8 planning, sq8 is fetch/rank; no inferred network-only costs",
        "startup":e.startup,"native_terminal":e.terminal,"recall_source":"authenticated native terminal checked against ordered sealed recall rows; truth bodies not reopened",
        "transport_scope":"cumulative process SDK observations including S3 and IMDS credential requests including PUT; not per-query logical charges or wire/billed accounting",
        "wire_bytes":null,"unread_bytes":null,"billed_bytes":null,"billed_requests":null,
        "local_file_only":true,"external_resources_and_cost_gate_required":true,"qualified":false,
        "vendor_or_scientific_win_claim":false,"performance_pass_claim":false});
    if current_v10::concurrent_result(&c.expected_identity["schema"]) {
        report["schema"] = json!("borsuk-concurrent-query-completed-v1");
        report["statistics"]
            .as_object_mut()
            .unwrap()
            .remove("sequential_qps");
        report
            .as_object_mut()
            .unwrap()
            .remove("sequential_qps_definition");
        report["query_cohort"] = e.terminal["query_cohort"].clone();
        report["qps_definition"] = json!(
            "successful_count * 1e9 / cohort elapsed_ns; closed-loop ordered batches, not rolling or service-peak QPS"
        );
        report["query_cpu_definition"] =
            json!("one cohort process CPU delta, never sum overlapping per-query snapshots");
    }
    if c.schema == SCALE_CONFIG_SCHEMA {
        report["reduction_reads_local_files_only"] = json!(true);
        report["producer_backend_kind"] = c.expected_bound_inputs["backend"]["kind"].clone();
    }
    Ok(report)
}

const SOURCE_UTILIZATION_SCHEMA: &str = "borsuk-source-utilization-evidence-v1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceUtilizationConfig {
    schema: String,
    input: CompletedInput,
    expected_identity: Value,
    expected_bound_inputs: Value,
    // Root binds these historical pins and limits to the archived producer.
    producer_source_commit: String,
    producer_source_archive_sha256: String,
    record_bytes: usize,
    source_get_cap: usize,
    source_byte_cap: usize,
    source_unit_cap: usize,
    direct_sq8_get_cap: usize,
    historical_sq8_query_byte_cap: usize,
}

fn source_record_bytes(dimensions: usize) -> Result<usize> {
    require((1..=1024).contains(&dimensions), "source dimensions")?;
    let tail = dimensions % 256;
    let padded = (dimensions / 256 * 256)
        .checked_add(if tail == 0 {
            0
        } else {
            tail.checked_next_power_of_two()
                .ok_or("source padding overflow")?
        })
        .ok_or("source padding overflow")?;
    padded
        .div_ceil(4)
        .checked_add(8)
        .ok_or_else(|| "source record overflow".into())
}

impl SourceUtilizationConfig {
    fn validate(&self) -> Result<()> {
        require(
            self.schema == "borsuk-source-utilization-config-v1"
                && self.input.bytes > 0
                && self.input.bytes <= FILE_CAP
                && valid_sha(&self.input.sha256)
                && self.producer_source_commit.len() == 40
                && self
                    .producer_source_commit
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                && valid_sha(&self.producer_source_archive_sha256)
                && self.source_get_cap == 128
                && self.source_byte_cap == 64 * 1024 * 1024
                && self.source_unit_cap == 2544
                && self.direct_sq8_get_cap == 32
                && self.historical_sq8_query_byte_cap > 0,
            "source utilization schema/pins/explicit historical envelopes",
        )?;
        validate_v2_row(&serde_json::to_vec(&self.expected_identity)?, "identity")?;
        validate_v2_row(
            &serde_json::to_vec(&self.expected_bound_inputs)?,
            "bound_inputs",
        )?;
        let identity: Identity = serde_json::from_value(self.expected_identity.clone())?;
        let inputs: Inputs = serde_json::from_value(self.expected_bound_inputs.clone())?;
        validate_v2_identity(&identity, &self.expected_identity)?;
        validate_v2_inputs(&inputs, &self.expected_bound_inputs)?;
        require(
            inputs.profile == "native100k"
                && inputs.rows <= 100_000
                && self.record_bytes == source_record_bytes(inputs.dimensions)?
                && self.expected_identity["phase"] == "identity"
                && self.expected_bound_inputs["phase"] == "bound_inputs"
                && self.expected_identity["fetch_parallelism"] == 32
                && self.expected_bound_inputs["fetch_parallelism"] == 32
                && self.expected_bound_inputs["source_cache"] == "off",
            "source workload/padded record geometry/closed membership mode",
        )?;
        inputs
            .rows
            .checked_mul(self.record_bytes)
            .ok_or("source geometry overflow")?;
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceTrace {
    ranked_candidate_pages: Vec<usize>,
    nomination_evaluated_units: Vec<usize>,
    primary_page: usize,
    discoveries: Vec<Value>,
    semantic_leaves: Vec<usize>,
    semantic_units: Vec<usize>,
    semantic_seed_additions: Vec<usize>,
}

#[derive(Serialize)]
struct SourceUtilizationQuery {
    ordinal: usize,
    semantic_units: usize,
    seed_added_units: usize,
    walked_units: usize,
    completion_units: usize,
    scored_units: usize,
    closure_pages: usize,
    baseline_bytes: u64,
    baseline_gets: u64,
    walked_payload_bytes: u64,
    scored_payload_bytes: u64,
    completion_payload_bytes: u64,
    closure_payload_bytes: u64,
    unscored_closure_bytes: u64,
    baseline_bridge_bytes: u64,
    ideal_bytes: u64,
    ideal_gets: u64,
    ideal_bridge_bytes: u64,
    potential_saving_bytes: u64,
    direct_closure_sq8_bytes: u64,
    direct_closure_sq8_gets: u64,
    mandatory_closure_sq8_bytes: u64,
    direct_closure_sq8_bridge_bytes: u64,
    direct_closure_sq8_exceeds_historical_byte_cap: bool,
    original_source_sq8_bytes: u64,
    original_source_sq8_gets: u64,
    direct_minus_original_bytes: i64,
    direct_minus_original_gets: i64,
    completion_limit_reached: bool,
    completion_limited: bool,
}

fn source_ordinals(values: &[usize], bound: usize, cap: usize) -> Result<BTreeSet<usize>> {
    require(values.len() <= cap, "source trace ordinal cap")?;
    let mut set = BTreeSet::new();
    for &value in values {
        require(
            value < bound && set.insert(value),
            "duplicate/out-of-range source ordinal",
        )?;
    }
    Ok(set)
}

fn source_payload_bytes(
    units: &BTreeSet<usize>,
    rows: usize,
    width: usize,
    unit_rows: usize,
) -> Result<u64> {
    units.iter().try_fold(0, |sum, &unit| {
        let first = unit.checked_mul(unit_rows).ok_or("source row overflow")?;
        require(first < rows, "source payload unit bounds")?;
        let bytes = (rows - first)
            .min(unit_rows)
            .checked_mul(width)
            .ok_or("source payload overflow")?;
        plus(sum, u64::try_from(bytes)?)
    })
}

fn source_utilization_query(
    line: &[u8],
    q: &Query,
    inputs: &Inputs,
    c: &SourceUtilizationConfig,
) -> Result<SourceUtilizationQuery> {
    // The shared completed reader already refuses duplicate keys and unknown fields.
    let raw: Value = serde_json::from_slice(line)?;
    let t: SourceTrace = serde_json::from_value(raw["trace"].clone())?;
    require(
        t.discoveries.is_empty(),
        "semantic source trace cannot contain graph discoveries",
    )?;
    let unit_count = inputs.rows.div_ceil(32);
    let semantic = source_ordinals(&t.semantic_units, unit_count, 1024)?;
    require(
        !semantic.is_empty() && t.semantic_units.windows(2).all(|p| p[0] < p[1]),
        "sorted nonempty semantic units",
    )?;
    let leaves = source_ordinals(&t.semantic_leaves, unit_count, 16)?;
    require(!leaves.is_empty(), "semantic leaf nomination required")?;
    let additions = source_ordinals(&t.semantic_seed_additions, unit_count, 7)?;
    let seed = semantic.first().ok_or("semantic seed")? / 8;
    let expected_additions = (seed * 8..((seed + 1) * 8).min(unit_count))
        .filter(|unit| !semantic.contains(unit))
        .collect::<Vec<_>>();
    require(
        t.semantic_seed_additions == expected_additions,
        "exact lowest-page seed completion",
    )?;
    let walked = semantic.union(&additions).copied().collect::<BTreeSet<_>>();
    require(
        walked.len() <= 1031 && walked.len() <= c.source_unit_cap,
        "source walk cap",
    )?;
    let closure = walked.iter().map(|unit| unit / 8).collect::<BTreeSet<_>>();
    let ranked = source_ordinals(&t.ranked_candidate_pages, inputs.rows.div_ceil(256), 1024)?;
    require(
        ranked == closure && t.ranked_candidate_pages.first() == Some(&t.primary_page),
        "ranked source closure/primary",
    )?;
    let scored = source_ordinals(&t.nomination_evaluated_units, unit_count, c.source_unit_cap)?;
    require(
        t.nomination_evaluated_units.len() >= walked.len()
            && t.nomination_evaluated_units[..walked.len()]
                .iter()
                .copied()
                .eq(walked.iter().copied())
            && scored.iter().all(|unit| closure.contains(&(unit / 8))),
        "ordered walked prefix/scored source closure",
    )?;
    // Completion visits each page once, in score-dependent order, and scans its
    // remaining units ascending. Only the final page can stop at the unit cap.
    let completion = &t.nomination_evaluated_units[walked.len()..];
    let mut completed_pages = BTreeSet::new();
    let mut offset = 0;
    while offset < completion.len() {
        let page = completion[offset] / 8;
        require(completed_pages.insert(page), "repeated completion page")?;
        for unit in page * 8..((page + 1) * 8).min(unit_count) {
            if walked.contains(&unit) {
                continue;
            }
            if walked.len() + offset == c.source_unit_cap {
                break;
            }
            require(
                completion.get(offset) == Some(&unit),
                "impossible ordered source completion",
            )?;
            offset += 1;
        }
    }
    let closure_units = closure
        .iter()
        .flat_map(|&page| page * 8..((page + 1) * 8).min(unit_count))
        .collect::<BTreeSet<_>>();
    require(
        scored.len() == closure_units.len().min(c.source_unit_cap),
        "incomplete source completion accounting",
    )?;
    let (baseline, baseline_bytes) =
        source_cover::cover_pages(&closure, inputs.rows, c.record_bytes, 256, c.source_get_cap)
            .map_err(|e| format!("baseline source cover: {e:?}"))?;
    require(
        baseline_bytes <= c.source_byte_cap
            && q.charges.source.failed_gets == 0
            && q.charges.source.submitted_gets == baseline.len() as u64
            && q.charges.source.verified_bytes == baseline_bytes as u64,
        "recorded source GET/byte parity/envelope",
    )?;
    let (ideal, ideal_bytes) =
        source_cover::cover_pages(&scored, inputs.rows, c.record_bytes, 32, c.source_get_cap)
            .map_err(|e| format!("hindsight source cover: {e:?}"))?;
    require(
        ideal_bytes <= baseline_bytes,
        "hindsight source bytes exceed baseline",
    )?;
    let completion_limited = scored.len() < closure_units.len();
    require(
        completion_limited || ideal_bytes == baseline_bytes,
        "full source completion must preserve baseline bytes",
    )?;
    let walked_bytes = source_payload_bytes(&walked, inputs.rows, c.record_bytes, 32)?;
    let scored_bytes = source_payload_bytes(&scored, inputs.rows, c.record_bytes, 32)?;
    let closure_bytes = source_payload_bytes(&closure, inputs.rows, c.record_bytes, 256)?;
    let sq8_width = inputs
        .dimensions
        .checked_add(12)
        .ok_or("SQ8 record width overflow")?;
    let sq8_population_bytes = inputs
        .rows
        .checked_mul(sq8_width)
        .ok_or("SQ8 population overflow")?;
    require(
        q.charges.sq8.failed_gets == 0
            && q.charges.sq8.submitted_gets > 0
            && q.charges.sq8.submitted_gets <= c.direct_sq8_get_cap as u64
            && q.charges.sq8.verified_bytes > 0
            && q.charges.sq8.verified_bytes <= sq8_population_bytes as u64
            && q.charges.sq8.verified_bytes % sq8_width as u64 == 0,
        "recorded original SQ8 charge geometry/GET envelope",
    )?;
    let fetched_rows = q.charges.sq8.verified_bytes / sq8_width as u64;
    require(
        q.charges.sq8.verified_bytes <= c.historical_sq8_query_byte_cap as u64,
        "recorded original SQ8 bytes exceed historical query cap",
    )?;
    let fetched_tail_rows = fetched_rows % 256;
    let fetched_pages = fetched_rows / 256 + u64::from(fetched_tail_rows > 0);
    require(
        (fetched_tail_rows == 0 || fetched_tail_rows == (inputs.rows % 256) as u64)
            && q.charges.sq8.submitted_gets <= fetched_pages,
        "recorded SQ8 whole-page/tail/GET compatibility",
    )?;
    // Cost the whole discovered closure, independently of scored completion.
    // This calculation admits no future byte envelope or serving schedule.
    let (direct_sq8, direct_sq8_bytes) =
        source_cover::cover_pages(&closure, inputs.rows, sq8_width, 256, c.direct_sq8_get_cap)
            .map_err(|e| format!("counterfactual closure SQ8 cover: {e:?}"))?;
    let mandatory_sq8_bytes = source_payload_bytes(&closure, inputs.rows, sq8_width, 256)?;
    let original_source_sq8_bytes = plus(
        q.charges.source.verified_bytes,
        q.charges.sq8.verified_bytes,
    )?;
    let original_source_sq8_gets = plus(
        q.charges.source.submitted_gets,
        q.charges.sq8.submitted_gets,
    )?;
    Ok(SourceUtilizationQuery {
        ordinal: q.ordinal,
        semantic_units: semantic.len(),
        seed_added_units: additions.len(),
        walked_units: walked.len(),
        completion_units: scored.len() - walked.len(),
        scored_units: scored.len(),
        closure_pages: closure.len(),
        baseline_bytes: baseline_bytes as u64,
        baseline_gets: baseline.len() as u64,
        walked_payload_bytes: walked_bytes,
        scored_payload_bytes: scored_bytes,
        completion_payload_bytes: scored_bytes
            .checked_sub(walked_bytes)
            .ok_or("completion byte accounting")?,
        closure_payload_bytes: closure_bytes,
        unscored_closure_bytes: closure_bytes
            .checked_sub(scored_bytes)
            .ok_or("closure byte accounting")?,
        baseline_bridge_bytes: (baseline_bytes as u64)
            .checked_sub(closure_bytes)
            .ok_or("baseline bridge accounting")?,
        ideal_bytes: ideal_bytes as u64,
        ideal_gets: ideal.len() as u64,
        ideal_bridge_bytes: (ideal_bytes as u64)
            .checked_sub(scored_bytes)
            .ok_or("ideal bridge accounting")?,
        potential_saving_bytes: (baseline_bytes - ideal_bytes) as u64,
        direct_closure_sq8_bytes: direct_sq8_bytes as u64,
        direct_closure_sq8_gets: direct_sq8.len() as u64,
        mandatory_closure_sq8_bytes: mandatory_sq8_bytes,
        direct_closure_sq8_bridge_bytes: (direct_sq8_bytes as u64)
            .checked_sub(mandatory_sq8_bytes)
            .ok_or("direct SQ8 bridge accounting")?,
        direct_closure_sq8_exceeds_historical_byte_cap: direct_sq8_bytes
            > c.historical_sq8_query_byte_cap,
        original_source_sq8_bytes,
        original_source_sq8_gets,
        direct_minus_original_bytes: i64::try_from(direct_sq8_bytes)?
            - i64::try_from(original_source_sq8_bytes)?,
        direct_minus_original_gets: i64::try_from(direct_sq8.len())?
            - i64::try_from(original_source_sq8_gets)?,
        completion_limit_reached: scored.len() == c.source_unit_cap,
        completion_limited,
    })
}

fn source_utilization_report(
    c: &SourceUtilizationConfig,
    queries: &[SourceUtilizationQuery],
) -> Result<Value> {
    require(
        queries.len() == COUNT,
        "exact source utilization query count",
    )?;
    let per_query = serde_json::to_value(queries)?;
    let mut aggregate = serde_json::Map::new();
    for name in [
        "semantic_units",
        "seed_added_units",
        "walked_units",
        "completion_units",
        "scored_units",
        "closure_pages",
        "baseline_bytes",
        "baseline_gets",
        "walked_payload_bytes",
        "scored_payload_bytes",
        "completion_payload_bytes",
        "closure_payload_bytes",
        "unscored_closure_bytes",
        "baseline_bridge_bytes",
        "ideal_bytes",
        "ideal_gets",
        "ideal_bridge_bytes",
        "potential_saving_bytes",
        "direct_closure_sq8_bytes",
        "direct_closure_sq8_gets",
        "mandatory_closure_sq8_bytes",
        "direct_closure_sq8_bridge_bytes",
        "original_source_sq8_bytes",
        "original_source_sq8_gets",
    ] {
        let samples = per_query
            .as_array()
            .ok_or("source query report array")?
            .iter()
            .map(|q| q[name].as_u64().ok_or("source query report counter"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let (total, [median, _, p95, _]) = quantiles(&samples)?;
        aggregate.insert(name.into(), json!({"total":total,"median":median,"p95":p95,"max":samples.iter().max().ok_or("empty source samples")?}));
    }
    for name in ["direct_minus_original_bytes", "direct_minus_original_gets"] {
        let mut samples = per_query
            .as_array()
            .ok_or("source query report array")?
            .iter()
            .map(|q| q[name].as_i64().ok_or("signed counterfactual cost delta"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let total = samples.iter().try_fold(0_i64, |sum, &n| {
            sum.checked_add(n)
                .ok_or("counterfactual delta sum overflow")
        })?;
        samples.sort_unstable();
        aggregate.insert(
            name.into(),
            json!({"total":total,"median":samples[(COUNT*50).div_ceil(100)-1],
            "p95":samples[(COUNT*95).div_ceil(100)-1],"max":samples[COUNT-1]}),
        );
    }
    let limited = queries.iter().filter(|q| q.completion_limited).count();
    let at_limit = queries
        .iter()
        .filter(|q| q.completion_limit_reached)
        .count();
    Ok(
        json!({"schema":SOURCE_UTILIZATION_SCHEMA,"status":"MEASURED","complete":true,"count":COUNT,
        "input":{"path":c.input.path,"bytes":c.input.bytes,"sha256":c.input.sha256},
        "identity":c.expected_identity,"bound_inputs":c.expected_bound_inputs,
        "producer_source_commit":c.producer_source_commit,"producer_source_archive_sha256":c.producer_source_archive_sha256,
        "reducer_source_sha256":format!("{:x}",Sha256::digest(include_bytes!("compare_native_replay.rs"))),
        "shared_cover_source_sha256":format!("{:x}",Sha256::digest(include_bytes!("../src/budgeted_page_rank.rs"))),
        "record_bytes":c.record_bytes,"source_get_cap":c.source_get_cap,"source_byte_cap":c.source_byte_cap,"source_unit_cap":c.source_unit_cap,
        "direct_sq8_get_cap":c.direct_sq8_get_cap,"historical_sq8_query_byte_cap":c.historical_sq8_query_byte_cap,
        "direct_closure_sq8_exceeds_historical_byte_cap_queries":queries.iter().filter(|q|q.direct_closure_sq8_exceeds_historical_byte_cap).count(),
        "baseline_page_rows":256,"ideal_unit_rows":32,"percentile_method":"nearest_rank",
        "aggregate":aggregate,"queries":per_query,
        "completion_limit_reached_queries":at_limit,"completion_limit_reached_frequency":at_limit as f64/COUNT as f64,
        "completion_limited_queries":limited,"completion_limited_frequency":limited as f64/COUNT as f64,
        "optimistic_hindsight":true,"production_change":false,"claims_quality":false,"claims_latency":false,
        "counterfactual_cost_only":true,"score_or_recall_evaluated":false,
        "counterfactual_scope":"direct SQ8 fetch of the same complete 256-row discovered source closure under 32 GETs; width dimensions+12; cost only, no future byte envelope admitted; broader scored population and different bridges can change recall",
        "cost_delta_direction":"direct_closure_sq8 minus recorded original_source_plus_sq8; positive bytes means more payload; negative GETs means fewer logical requests; discovery charges excluded from both",
        "scope":"minimum cover of actual scored units under the SAME source GET allowance; completion order depends on query scores; not a usable one-wave serving algorithm",
        "local_file_only":true,"producer_provenance_verified":false,"root_historical_producer_and_limits_admission_required":true,
        "corpus_queries_truth_bodies_opened":false,"native_ann_called":false,"qualified":false}),
    )
}

fn reduce_source_utilization(config_path: &Path, config_sha: &str) -> Result<Value> {
    let (c, config_bytes): (SourceUtilizationConfig, _) = read_config(config_path, config_sha)?;
    if c.expected_identity["schema"]
        .as_str()
        .is_some_and(current_v10::is_result_schema)
    {
        return Err(
            "historical 256-row analysis only; v10 requires current-format analysis".into(),
        );
    }
    require(
        c.expected_identity["schema"] != "borsuk-cohere-native-baseline-result-v9",
        "historical 256-row analysis only; v9 requires current-format analysis",
    )?;
    c.validate()?;
    let completed = CompletedConfig {
        schema: "borsuk-completed-native-reduction-config-v1".into(),
        input: CompletedInput {
            path: c.input.path.clone(),
            bytes: c.input.bytes,
            sha256: c.input.sha256.clone(),
        },
        expected_identity: c.expected_identity.clone(),
        expected_bound_inputs: c.expected_bound_inputs.clone(),
    };
    let mut queries = Vec::with_capacity(COUNT);
    let run = read_run_observed(
        &c.input.path,
        &c.input.sha256,
        Some(&completed),
        |line, q, inputs| {
            queries.push(source_utilization_query(line, q, inputs, &c)?);
            Ok(())
        },
    )?;
    let mut report = source_utilization_report(&c, &queries)?;
    report["config_path"] = json!(config_path);
    report["config_sha256"] = json!(config_sha);
    report["config_bytes"] = json!(config_bytes);
    source_report_cap(&report)?;
    require(
        file_identity(&open_input(&c.input.path)?)? == run.file_identity,
        "source input changed before publication",
    )?;
    read_config::<SourceUtilizationConfig>(config_path, config_sha)?;
    Ok(report)
}

fn source_report_cap(report: &Value) -> Result<()> {
    require(
        (serde_json::to_vec(report)?.len() as u64) < REPORT_CAP,
        "source report cap before publication",
    )
}

fn reduce_paired(config_path: &Path, config_sha: &str) -> Result<Value> {
    reduce_paired_scope(config_path, config_sha, false)
}

fn reduce_paired_scope(config_path: &Path, config_sha: &str, scale: bool) -> Result<Value> {
    let config_identity = if scale {
        Some(file_identity(&open_input(config_path)?)?)
    } else {
        None
    };
    let (c, config_bytes): (PairedConfig, _) = read_config(config_path, config_sha)?;
    let scheduler = scale && c.schema == "borsuk-scale-scheduler-native-reduction-config-v1";
    let width64 = scale && c.schema == "borsuk-scale-width64-native-reduction-config-v1";
    let width128 = scale && c.schema == "borsuk-scale-width128-native-reduction-config-v1";
    let parallelisms: [u64; 2] = if width128 {
        [64, 128]
    } else if width64 {
        [32, 64]
    } else if scheduler {
        [32, 32]
    } else {
        [16, 32]
    };
    require(
        c.schema
            == if width128 {
                "borsuk-scale-width128-native-reduction-config-v1"
            } else if width64 {
                "borsuk-scale-width64-native-reduction-config-v1"
            } else if scheduler {
                "borsuk-scale-scheduler-native-reduction-config-v1"
            } else if scale {
                "borsuk-scale-paired-native-reduction-config-v1"
            } else {
                "borsuk-paired-native-reduction-config-v1"
            },
        "paired config schema",
    )?;
    for (arm, parallelism) in c.arms.iter().zip(parallelisms) {
        require(
            arm.expected_identity.get("shared_byte_window").is_none()
                && arm
                    .expected_bound_inputs
                    .get("shared_byte_window")
                    .is_none(),
            "historical paired modes refuse byte-window intervention",
        )?;
        require(
            arm.schema
                == if scale {
                    SCALE_CONFIG_SCHEMA
                } else {
                    "borsuk-completed-native-reduction-config-v1"
                }
                && arm.expected_identity["fetch_parallelism"] == parallelism
                && arm.expected_bound_inputs["fetch_parallelism"] == parallelism,
            if width128 {
                "width128 arms must be ordered integer fetch_parallelism 64 then 128"
            } else if width64 {
                "width64 arms must be ordered integer fetch_parallelism 32 then 64"
            } else if scheduler {
                "scheduler arms must both use integer fetch_parallelism 32"
            } else {
                "paired arms must be ordered integer fetch_parallelism 16 then 32"
            },
        )?;
        // This experiment compares the retained production Cohere panel, not
        // arbitrary matching outputs. Admit its pins before either input opens;
        // the generic completed reducer and frozen historical mode stay separate.
        let i: Inputs = serde_json::from_value(arm.expected_bound_inputs.clone())?;
        if scale {
            require(
                i.rows == 1_000_000
                    && i.dimensions == 1024
                    && i.count == COUNT
                    && i.k == K
                    && i.profile == "scale1m"
                    && arm.expected_bound_inputs["source_cache"] == "off"
                    && i.corpus_source_first == 0
                    && i.query_source_first == 100_000
                    && arm.expected_bound_inputs["corpus_intervals"]
                        == json!([{"start":0,"end":100000},{"start":101000,"end":1001000}])
                    && arm.expected_bound_inputs["reserved_query_interval"]
                        == json!({"start":100000,"end":101000})
                    && arm.expected_bound_inputs["reserved_queries_sha256"]
                        == "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e"
                    && arm.expected_identity["execution"] == json!({"mode":"full"})
                    && arm.expected_bound_inputs["execution"] == json!({"mode":"full"})
                    && [
                        i.requests_sha256.as_str(),
                        i.truth_sha256.as_str(),
                        i.native_source_sha256.as_str(),
                        i.native_sq8_sha256.as_str(),
                        i.native_order_sha256.as_str(),
                    ] == [
                        "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e",
                        "0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f",
                        "31f28fa211f9237f0f7e41f569773cd207cefcdce22d4932ec96aee01ff2c40d",
                        "cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1",
                        "1060c919e34e93cc9a3d88b93eda226a8635bba41bfc60e5ba47caafb711d413",
                    ],
                "paired scale full1M production pins",
            )?;
            validate_scale_inputs(&i, &arm.expected_bound_inputs)?;
        } else {
            require(
                i.dataset == "CohereLabs/wikipedia-2023-11-embed-multilingual-v3"
                    && i.revision == "ade45fb52bd549f5e8c065636fe4160a43c2af36"
                    && i.metric == "cosine"
                    && i.tie_rule == "corpus_ordinal_ascending"
                    && i.rows == 100000
                    && i.dimensions == 1024
                    && i.count == COUNT
                    && i.k == K
                    && i.corpus_source_first == 0
                    && i.query_source_first == 100000
                    && i.profile == "native100k"
                    && !i.truth_opened
                    && i.requests_bytes == 4096000
                    && i.truth_bytes == 80000
                    && [
                        i.requests_sha256.as_str(),
                        i.truth_sha256.as_str(),
                        i.native_source_sha256.as_str(),
                        i.native_sq8_sha256.as_str(),
                        i.native_order_sha256.as_str(),
                    ] == INPUT_SHA256,
                "paired production benchmark pins",
            )?;
        }
    }
    let [a, b] = &c.arms;
    let mut identity = a.expected_identity.clone();
    require(
        identity["config_sha256"] != b.expected_identity["config_sha256"],
        "paired native configs must have distinct SHA256",
    )?;
    identity["config_sha256"] = b.expected_identity["config_sha256"].clone();
    identity["fetch_parallelism"] = json!(parallelisms[1]);
    if scheduler {
        for field in ["binary_sha256", "sq8_range_source_sha256"] {
            require(
                valid_sha(identity[field].as_str().unwrap_or(""))
                    && valid_sha(b.expected_identity[field].as_str().unwrap_or(""))
                    && identity[field] != b.expected_identity[field],
                "scheduler requires distinct authenticated binary and fetcher pins",
            )?;
            identity[field] = b.expected_identity[field].clone();
        }
    }
    let mut inputs = a.expected_bound_inputs.clone();
    inputs["fetch_parallelism"] = json!(parallelisms[1]);
    require(
        identity == b.expected_identity && inputs == b.expected_bound_inputs,
        "paired binary/source/benchmark/generation/backend/input/scoring pins differ",
    )?;
    if scale {
        require(
            a.input.path != b.input.path,
            "paired scale artifact role alias",
        )?;
    }
    let runs = [
        read_run_with(&a.input.path, &a.input.sha256, Some(a))?,
        read_run_with(&b.input.path, &b.input.sha256, Some(b))?,
    ];
    if let Some(original) = &config_identity {
        recheck_scale_pair(
            config_path,
            config_sha,
            original,
            &c,
            [&runs[0].file_identity, &runs[1].file_identity],
        )?;
    }
    require(
        a.input.sha256 != b.input.sha256
            && (runs[0].file_identity.dev, runs[0].file_identity.ino)
                != (runs[1].file_identity.dev, runs[1].file_identity.ino),
        "duplicate paired run evidence",
    )?;
    for (ordinal, (a, b)) in runs[0].samples.iter().zip(&runs[1].samples).enumerate() {
        // Count and underfill have already been checked against returned.len()
        // in BOTH query and recall rows for all 1000 sealed ordinals.
        require(
            a.returned == b.returned
                && a.hits10 == b.hits10
                && a.charges == b.charges
                && a.trace_sha256 == b.trace_sha256
                && (!scale || a.physical_plan_sha256 == b.physical_plan_sha256),
            &format!("paired query {ordinal}: ordered hits/recall/plan/logical GET/bytes mismatch"),
        )?;
    }
    let mut reports = Vec::with_capacity(2);
    for ((arm, run), parallelism) in c.arms.iter().zip(&runs).zip(parallelisms) {
        // The maximum includes every query. Native100k selects at most16
        // leaves, so neither fetch width can admit a larger per-query peak.
        require(
            run.stages.max_leaf_peak_inflight <= if scale { parallelism } else { 16 },
            if scale {
                "paired Scale1m query leaf_peak_inflight exceeds selector"
            } else {
                "paired Native100k query leaf_peak_inflight exceeds 16"
            },
        )?;
        let e = run.completed.as_ref().ok_or("missing paired evidence")?;
        let transport = &e.last.as_ref().ok_or("missing transport")?.after;
        require(
            transport.transport_failures == 0
                && transport.stream_failures == 0
                && transport.dropped_error_bodies == 0
                && run.terminal.sum.failed_gets == 0,
            "paired transport/logical GET failure",
        )?;
        require(
            e.binding_charge
                == runs[0]
                    .completed
                    .as_ref()
                    .ok_or("missing binding")?
                    .binding_charge,
            "paired source binding charge mismatch",
        )?;
        let mut report = completed_report(arm, run)?;
        report["fetch_parallelism"] = arm.expected_identity["fetch_parallelism"].clone();
        report["first_query"] = json!({"ordinal":0,"query_wall_ns":run.samples[0].wall_ns,
            "charges":run.samples[0].charges,"hits10":run.samples[0].hits10,
            "returned_count":run.samples[0].returned.len(),
            "underfill":run.samples[0].returned.len() < K,
            "stage_wall_ns":{"discovery":e.stage_samples[0][0],"source":e.stage_samples[1][0],
                "planning":e.stage_samples[2][0],"sq8":e.stage_samples[3][0]}});
        reports.push(report);
    }
    let pooled = statistics(
        &runs
            .iter()
            .flat_map(|r| r.samples.iter().map(|s| s.wall_ns))
            .collect::<Vec<_>>(),
    )?;
    let [a, b] = runs.each_ref().map(|r| &r.stages);
    let stage_totals = StageTotals {
        discovery_ns: plus(a.discovery_ns, b.discovery_ns)?,
        source_ns: plus(a.source_ns, b.source_ns)?,
        planning_ns: plus(a.planning_ns, b.planning_ns)?,
        sq8_ns: plus(a.sq8_ns, b.sq8_ns)?,
        max_leaf_peak_inflight: a.max_leaf_peak_inflight.max(b.max_leaf_peak_inflight),
    };
    if let Some(original) = &config_identity {
        recheck_scale_pair(
            config_path,
            config_sha,
            original,
            &c,
            [&runs[0].file_identity, &runs[1].file_identity],
        )?;
    }
    Ok(
        json!({"schema":if width128 { SCALE_WIDTH128_SCHEMA } else if width64 { SCALE_WIDTH64_SCHEMA } else if scheduler { SCALE_SCHEDULER_SCHEMA } else if scale { SCALE_PAIRED_SCHEMA } else { PAIRED_SCHEMA },"status":"MEASURED","complete":true,
        "config_path":config_path,"config_sha256":config_sha,"config_bytes":config_bytes,
        "semantic_parity":true,"trace_plan_parity":true,"logical_charge_parity":true,
        "arms":reports,"pooled_both_arms":{"statistics":pooled,"stage_wall_sums":stage_totals},
        "statistics_population":"all 1000 ordered queries per arm including first; pooled_both_arms includes all 2000 observations without arm selection",
        "percentile_method":"nearest_rank",
        "sequential_qps_definition":"query_count * 1e9 / sum(query_wall_ns); not concurrent service QPS",
        "plan_scope":if scale { "complete ordered TwoBitPlanTrace and physical plan; timing, inflight and transport fields excluded" } else { "complete ordered TwoBitPlanTrace; timing, inflight and transport fields are outside the plan" },
        "source_cache":"off","cache_state":"external cold/cache qualification required",
        "local_file_only":true,"physical_s3":false,"external_resources_and_cost_gate_required":true,
        "frozen_runtime_root_config_admission_required":true,"qualified":false,
        "supervisor_resources_cost_and_cache_qualified":false,
        "vendor_or_scientific_win_claim":false,"performance_pass_claim":false,
        "wire_bytes":null,"unread_bytes":null,"billed_bytes":null,"billed_requests":null}),
    )
}

fn recheck_scale_pair(
    config_path: &Path,
    config_sha: &str,
    original: &FileIdentity,
    c: &PairedConfig,
    inputs: [&FileIdentity; 2],
) -> Result<()> {
    for (arm, identity) in c.arms.iter().zip(inputs) {
        recheck_parity_path(&arm.input, identity)?;
    }
    require(
        file_identity(&open_input(config_path)?)? == *original,
        "paired scale config pathname mutation",
    )?;
    read_config::<PairedConfig>(config_path, config_sha)?;
    Ok(())
}

fn reduce_source_budget_abba(config_path: &Path, config_sha: &str) -> Result<Value> {
    let config_file = open_input(config_path)?;
    let config_identity = file_identity(&config_file)?;
    let (c, config_bytes): (ByteWindowAbbaConfig, _) =
        read_config_file(&config_file, config_path, config_sha, CONFIG_CAP)?;
    require(
        c.schema == "borsuk-source-budget-abba-native-reduction-config-v1",
        "SOURCE ABBA schema",
    )?;
    require(
        c.runtime_configs[0].sha256 != c.runtime_configs[1].sha256
            && c.runtime_configs[0].path != c.runtime_configs[1].path,
        "distinct SOURCE runtime configs",
    )?;
    let runtime = [
        header_json(&c.runtime_configs[0])?,
        header_json(&c.runtime_configs[1])?,
    ];
    let names = "schema dataset revision metric tie_rule corpus_intervals reserved_query_interval cohort_receipt derivation_receipt producer_authority corpus_source_first query_source_first rows dimensions count k profile backend generation_prefix generation_root_sha256 scratch_parent requests truth native_source max_memory_bytes fetch_parallelism serving execution source_get_budget";
    for (arm, (native, _)) in runtime.iter().enumerate() {
        fields(native, names)?;
        validate_header_descriptors(native)?;
        require(
            native["schema"] == "borsuk-cohere-native-baseline-config-v8"
                && native["source_get_budget"].as_u64() == Some(if arm == 0 { 128 } else { 96 })
                && native["fetch_parallelism"] == 64
                && native["backend"]["kind"] == "s3",
            "SOURCE runtime version/budget/width/backend",
        )?;
        header_path(Path::new(
            native["scratch_parent"]
                .as_str()
                .ok_or("SOURCE scratch parent")?,
        ))?;
        for role in ["cohort_receipt", "derivation_receipt", "requests", "truth"] {
            let pin: CompletedInput = serde_json::from_value(native[role].clone())?;
            header_pin(
                &pin,
                if matches!(role, "requests" | "truth") {
                    4_096_000
                } else {
                    HEADER_JSON_CAP
                },
            )?;
        }
        fields(
            &native["native_source"],
            "source_sha256 sq8_sha256 source_order_sha256",
        )?;
    }
    let normalize_budget = |value: &Value| -> Result<Value> {
        let mut value = value.clone();
        value
            .as_object_mut()
            .ok_or("SOURCE object")?
            .remove("source_get_budget");
        Ok(value)
    };
    require(
        normalize_budget(&runtime[0].0)? == normalize_budget(&runtime[1].0)?,
        "SOURCE runtime unapproved intervention",
    )?;
    let normalized_headers = |run: &CompletedConfig| -> Result<[Value; 2]> {
        let mut identity = normalize_budget(&run.expected_identity)?;
        identity
            .as_object_mut()
            .ok_or("SOURCE identity object")?
            .remove("config_sha256");
        Ok([identity, normalize_budget(&run.expected_bound_inputs)?])
    };
    let invariant = normalized_headers(&c.runs[0])?;
    for (index, pin) in c.runs.iter().enumerate() {
        let arm = usize::from(index == 1 || index == 2);
        let native = &runtime[arm].0;
        let identity = &pin.expected_identity;
        let inputs = &pin.expected_bound_inputs;
        require(
            pin.schema == SCALE_CONFIG_SCHEMA
                && identity["schema"] == "borsuk-cohere-native-baseline-result-v8"
                && identity["config_sha256"] == c.runtime_configs[arm].sha256
                && identity["source_get_budget"] == native["source_get_budget"]
                && inputs["source_get_budget"] == native["source_get_budget"]
                && identity.get("shared_byte_window") == native.get("shared_byte_window")
                && inputs.get("shared_byte_window") == native.get("shared_byte_window"),
            "SOURCE runtime/header binding",
        )?;
        validate_scale_row(&serde_json::to_vec(identity)?, "identity")?;
        validate_scale_row(&serde_json::to_vec(inputs)?, "bound_inputs")?;
        let parsed: Inputs = serde_json::from_value(inputs.clone())?;
        validate_scale_inputs(&parsed, inputs)?;
        require(
            parsed.rows == 1_000_000
                && parsed.dimensions == 1024
                && parsed.count == COUNT
                && parsed.k == K
                && parsed.profile == "scale1m"
                && inputs["source_cache"] == "off"
                && inputs["execution"] == json!({"mode":"full"})
                && inputs["serving"] == json!({"mode":"baseline"})
                && inputs["corpus_intervals"]
                    == json!([{"start":0,"end":100000},{"start":101000,"end":1001000}])
                && inputs["reserved_query_interval"] == json!({"start":100000,"end":101000})
                && inputs["requests_sha256"]
                    == "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e"
                && inputs["reserved_queries_sha256"] == inputs["requests_sha256"]
                && inputs["truth_sha256"]
                    == "0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f",
            "SOURCE frozen 1M workload",
        )?;
        for key in "dataset revision metric tie_rule corpus_intervals reserved_query_interval producer_authority corpus_source_first query_source_first rows dimensions count k profile backend generation_prefix generation_root_sha256 max_memory_bytes fetch_parallelism serving execution source_get_budget".split_whitespace() {
            require(native[key]==inputs[key],"SOURCE runtime/input field mismatch")?;
        }
        require(
            identity["fetch_parallelism"] == 64
                && identity["serving"] == native["serving"]
                && identity["execution"] == native["execution"],
            "SOURCE runtime/identity field mismatch",
        )?;
        for role in ["requests", "truth"] {
            require(
                native[role]["bytes"] == inputs[format!("{role}_bytes")]
                    && native[role]["sha256"] == inputs[format!("{role}_sha256")],
                "SOURCE runtime input seal",
            )?;
        }
        for (role, bound) in [
            ("source_sha256", "native_source_sha256"),
            ("sq8_sha256", "native_sq8_sha256"),
            ("source_order_sha256", "native_order_sha256"),
        ] {
            require(
                native["native_source"][role] == inputs[bound],
                "SOURCE native source seal",
            )?;
        }
        require(
            native["cohort_receipt"]["sha256"] == inputs["cohort_receipt_sha256"]
                && native["derivation_receipt"]["sha256"] == inputs["derivation_receipt_sha256"],
            "SOURCE receipt seal",
        )?;
        require(
            normalized_headers(pin)? == invariant,
            "SOURCE ABBA source/binary/input drift",
        )?;
    }
    let mut runs: Vec<Run> = Vec::with_capacity(4);
    let mut distinct_queries = 0_usize;
    for (index, pin) in c.runs.iter().enumerate() {
        let run = read_run_observed(
            &pin.input.path,
            &pin.input.sha256,
            Some(pin),
            |line, q, i| {
                loss_coverage(line, q, i)?;
                Ok(())
            },
        )
        .map_err(|e| format!("{}: {e}", LABELS[index]))?;
        require(
            run.stages.max_leaf_peak_inflight <= 64,
            "SOURCE leaf peak exceeds width64",
        )?;
        require(
            run.terminal.total_hits10 == 9780 && run.terminal.underfilled_queries == 0,
            "SOURCE ABBA exact recall9780 and full-k required",
        )?;
        let evidence = run
            .completed
            .as_ref()
            .ok_or("missing SOURCE completed evidence")?;
        let transport = &evidence
            .last
            .as_ref()
            .ok_or("missing SOURCE transport")?
            .after;
        require(
            transport.transport_failures == 0
                && transport.stream_failures == 0
                && transport.dropped_error_bodies == 0
                && run.terminal.sum.failed_gets == 0,
            "SOURCE transport/logical GET failure",
        )?;
        for (old, old_pin) in runs.iter().zip(&c.runs) {
            require(
                pin.input.sha256 != old_pin.input.sha256
                    && (run.file_identity.dev, run.file_identity.ino)
                        != (old.file_identity.dev, old.file_identity.ino),
                "duplicate SOURCE run evidence",
            )?;
        }
        if let Some(first) = runs.first() {
            require(
                evidence.binding_charge
                    == first
                        .completed
                        .as_ref()
                        .ok_or("missing SOURCE binding")?
                        .binding_charge,
                "SOURCE binding charge mismatch",
            )?;
            for (ordinal, (a, b)) in first.samples.iter().zip(&run.samples).enumerate() {
                require(
                    a.returned == b.returned
                        && a.hits10 == b.hits10
                        && a.trace_sha256 == b.trace_sha256
                        && a.physical_plan_sha256 == b.physical_plan_sha256
                        && a.charges.router == b.charges.router
                        && a.charges.sq8 == b.charges.sq8,
                    &format!(
                        "{} query {ordinal}: SOURCE quality/trace/SQ8 drift",
                        LABELS[index]
                    ),
                )?;
                if index == 1 {
                    distinct_queries += usize::from(a.charges.source != b.charges.source);
                }
                if index == 3 {
                    require(
                        a.charges.source == b.charges.source,
                        "SOURCE control repetition charge drift",
                    )?;
                }
            }
        }
        if index == 2 {
            require(
                runs[1]
                    .samples
                    .iter()
                    .zip(&run.samples)
                    .all(|(a, b)| a.charges.source == b.charges.source),
                "SOURCE candidate repetition charge drift",
            )?;
        }
        runs.push(run);
    }
    require(
        distinct_queries > 0,
        "SOURCE budget intervention must change an actual cover",
    )?;
    for (pin, run) in c.runs.iter().zip(&runs) {
        recheck_header_file(
            &open_input(&pin.input.path)?,
            &pin.input.path,
            &run.file_identity,
        )?;
    }
    for (_, snapshot) in &runtime {
        snapshot.recheck()?;
    }
    recheck_header_file(&config_file, config_path, &config_identity)?;
    let comparisons = [
        byte_window_comparison(&runs[0], &runs[1], "B1-A1")?,
        byte_window_comparison(&runs[3], &runs[2], "B2-A2")?,
    ];
    let noise = [
        byte_window_comparison(&runs[0], &runs[3], "A2-A1 noise reference")?,
        byte_window_comparison(&runs[1], &runs[2], "B2-B1 noise reference")?,
    ];
    let reports=c.runs.iter().zip(&runs).enumerate().map(|(index,(pin,run))| {
        let mut report=completed_report(pin,run)?;
        report["run"]=json!(LABELS[index]);
        report["first_query"]=json!({"ordinal":0,"query_wall_ns":run.samples[0].wall_ns,"charges":run.samples[0].charges,"hits10":run.samples[0].hits10});
        Ok(report)
    }).collect::<Result<Vec<_>>>()?;
    let passed = comparisons
        .iter()
        .all(|pair| pair["timing_gate_passed"] == true);
    Ok(
        json!({"schema":"borsuk-source-budget-abba-native-reduction-v1","status":"MEASURED","complete":true,
        "config_path":config_path,"config_sha256":config_sha,"config_bytes":config_bytes,"runtime_configs":c.runtime_configs,
        "declared_run_order":LABELS,"execution_order_verified":false,"source_binary_qualification_verified":false,
        "external_authority":"root original run-order, frozen binary/source, cache/resource/transport/cost receipts required",
        "quality_trace_sq8_parity":true,"source_cover_replay_verified":true,"distinct_source_cover_verified":true,
        "source_cover_changed_queries":distinct_queries,"source_cover_scope":"native planner replay of authenticated nomination closure and logical counts/bytes; not an independently captured wire request roster",
        "recall_anchor_hits10":9780,"recall_denominator":10000,"recall_anchor_verified":true,
        "runs":reports,"comparisons":comparisons,"within_arm_noise_references":noise,
        "performance_screen_passed":passed,"source_cache":"off","cache_resources_cost_and_transport_qualified":false,"vendor_win_claim":false}),
    )
}

fn reduce_byte_window_abba(config_path: &Path, config_sha: &str) -> Result<Value> {
    let config_file = open_input(config_path)?;
    let config_identity = file_identity(&config_file)?;
    let (c, config_bytes): (ByteWindowAbbaConfig, _) =
        read_config_file(&config_file, config_path, config_sha, CONFIG_CAP)?;
    require(
        c.schema == "borsuk-byte-window-abba-native-reduction-config-v1",
        "byte-window ABBA schema",
    )?;
    require(
        c.runtime_configs[0].sha256 != c.runtime_configs[1].sha256
            && c.runtime_configs[0].path != c.runtime_configs[1].path,
        "distinct byte-window runtime configs",
    )?;
    let runtime = [
        header_json(&c.runtime_configs[0])?,
        header_json(&c.runtime_configs[1])?,
    ];
    let names = "schema dataset revision metric tie_rule corpus_intervals reserved_query_interval cohort_receipt derivation_receipt producer_authority corpus_source_first query_source_first rows dimensions count k profile backend generation_prefix generation_root_sha256 scratch_parent requests truth native_source max_memory_bytes fetch_parallelism serving execution";
    fields(&runtime[0].0, names)?;
    fields(&runtime[1].0, &format!("{names} shared_byte_window"))?;
    for (native, _) in &runtime {
        validate_header_descriptors(native)?;
        header_path(Path::new(
            native["scratch_parent"]
                .as_str()
                .ok_or("byte-window scratch parent")?,
        ))?;
        for role in ["cohort_receipt", "derivation_receipt", "requests", "truth"] {
            let pin: CompletedInput = serde_json::from_value(native[role].clone())?;
            header_pin(
                &pin,
                if matches!(role, "requests" | "truth") {
                    4_096_000
                } else {
                    HEADER_JSON_CAP
                },
            )?;
        }
        fields(
            &native["native_source"],
            "source_sha256 sq8_sha256 source_order_sha256",
        )?;
    }
    let window = &runtime[1].0["shared_byte_window"];
    validate_byte_window(window, 64)?;
    let mut candidate = runtime[1].0.clone();
    candidate
        .as_object_mut()
        .ok_or("runtime config object")?
        .remove("shared_byte_window");
    require(
        candidate == runtime[0].0,
        "byte-window runtime unapproved intervention",
    )?;
    for (index, run) in c.runs.iter().enumerate() {
        let arm = usize::from(index == 1 || index == 2);
        let native = &runtime[arm].0;
        let identity = &run.expected_identity;
        let inputs = &run.expected_bound_inputs;
        require(
            identity["schema"] == "borsuk-cohere-native-baseline-result-v7"
                && identity.get("source_get_budget").is_none()
                && inputs.get("source_get_budget").is_none(),
            "byte-window historical v7 result required",
        )?;
        require(
            run.schema == SCALE_CONFIG_SCHEMA
                && native["schema"] == "borsuk-cohere-native-baseline-config-v7"
                && native["fetch_parallelism"] == 64
                && identity["config_sha256"] == c.runtime_configs[arm].sha256
                && identity["fetch_parallelism"] == 64
                && inputs["fetch_parallelism"] == 64
                && identity.get("shared_byte_window") == native.get("shared_byte_window")
                && inputs.get("shared_byte_window") == native.get("shared_byte_window"),
            "byte-window ABBA runtime/header binding",
        )?;
        validate_scale_row(&serde_json::to_vec(identity)?, "identity")?;
        validate_scale_row(&serde_json::to_vec(inputs)?, "bound_inputs")?;
        let parsed: Inputs = serde_json::from_value(inputs.clone())?;
        validate_scale_inputs(&parsed, inputs)?;
        require(
            parsed.rows == 1_000_000
                && parsed.count == COUNT
                && parsed.profile == "scale1m"
                && inputs["corpus_intervals"]
                    == json!([{"start":0,"end":100000},{"start":101000,"end":1001000}])
                && inputs["reserved_query_interval"] == json!({"start":100000,"end":101000})
                && inputs["reserved_queries_sha256"]
                    == "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e"
                && inputs["requests_sha256"]
                    == "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e"
                && inputs["truth_sha256"]
                    == "0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f"
                && inputs["execution"] == json!({"mode":"full"}),
            "byte-window frozen 1M workload",
        )?;
        for key in "dataset revision metric tie_rule corpus_intervals reserved_query_interval producer_authority corpus_source_first query_source_first rows dimensions count k profile backend generation_prefix generation_root_sha256 max_memory_bytes fetch_parallelism serving execution".split_whitespace() {
            require(native[key] == inputs[key], "byte-window runtime/input field mismatch")?;
        }
        require(
            identity["serving"] == native["serving"]
                && identity["execution"] == native["execution"],
            "byte-window runtime/identity field mismatch",
        )?;
        for role in ["requests", "truth"] {
            require(
                native[role]["bytes"] == inputs[format!("{role}_bytes")]
                    && native[role]["sha256"] == inputs[format!("{role}_sha256")],
                "byte-window runtime input seal",
            )?;
        }
        for (source, bound) in [
            ("source_sha256", "native_source_sha256"),
            ("sq8_sha256", "native_sq8_sha256"),
            ("source_order_sha256", "native_order_sha256"),
        ] {
            require(
                native["native_source"][source] == inputs[bound],
                "byte-window native source seal",
            )?;
        }
        require(
            native["cohort_receipt"]["sha256"] == inputs["cohort_receipt_sha256"]
                && native["derivation_receipt"]["sha256"] == inputs["derivation_receipt_sha256"],
            "byte-window receipt seal",
        )?;
    }
    let normalized = |run: &CompletedConfig| -> Result<[Value; 2]> {
        let mut identity = run.expected_identity.clone();
        let mut inputs = run.expected_bound_inputs.clone();
        identity
            .as_object_mut()
            .ok_or("identity object")?
            .remove("config_sha256");
        identity
            .as_object_mut()
            .ok_or("identity object")?
            .remove("shared_byte_window");
        inputs
            .as_object_mut()
            .ok_or("input object")?
            .remove("shared_byte_window");
        Ok([identity, inputs])
    };
    let invariant = normalized(&c.runs[0])?;
    for run in &c.runs[1..] {
        require(
            normalized(run)? == invariant,
            "byte-window ABBA source/binary/input drift",
        )?;
    }
    let mut runs: Vec<Run> = Vec::with_capacity(4);
    for (index, pinned) in c.runs.iter().enumerate() {
        let run = read_run_with(&pinned.input.path, &pinned.input.sha256, Some(pinned))
            .map_err(|e| format!("{}: {e}", LABELS[index]))?;
        require(
            run.stages.max_leaf_peak_inflight <= 64,
            "byte-window query leaf peak exceeds width64",
        )?;
        let evidence = run
            .completed
            .as_ref()
            .ok_or("missing byte-window completed evidence")?;
        let transport = &evidence
            .last
            .as_ref()
            .ok_or("missing byte-window transport")?
            .after;
        require(
            transport.transport_failures == 0
                && transport.stream_failures == 0
                && transport.dropped_error_bodies == 0
                && run.terminal.sum.failed_gets == 0,
            "byte-window transport/logical GET failure",
        )?;
        for (old, pin) in runs.iter().zip(&c.runs) {
            require(
                pinned.input.sha256 != pin.input.sha256
                    && (run.file_identity.dev, run.file_identity.ino)
                        != (old.file_identity.dev, old.file_identity.ino),
                "duplicate byte-window run evidence",
            )?;
        }
        if let Some(first) = runs.first() {
            require(
                evidence.binding_charge
                    == first
                        .completed
                        .as_ref()
                        .ok_or("missing byte-window binding")?
                        .binding_charge,
                "byte-window source binding charge mismatch",
            )?;
            for (ordinal, (a, b)) in first.samples.iter().zip(&run.samples).enumerate() {
                require(
                    a.returned == b.returned
                        && a.hits10 == b.hits10
                        && a.charges == b.charges
                        && a.trace_sha256 == b.trace_sha256
                        && a.physical_plan_sha256 == b.physical_plan_sha256,
                    &format!(
                        "{} query {ordinal}: byte-window quality/plan/charge drift",
                        LABELS[index]
                    ),
                )?;
            }
        }
        runs.push(run);
    }
    for (pin, run) in c.runs.iter().zip(&runs) {
        recheck_header_file(
            &open_input(&pin.input.path)?,
            &pin.input.path,
            &run.file_identity,
        )?;
    }
    for (_, snapshot) in &runtime {
        snapshot.recheck()?;
    }
    recheck_header_file(&config_file, config_path, &config_identity)?;
    let comparisons = [
        byte_window_comparison(&runs[0], &runs[1], "B1-A1")?,
        byte_window_comparison(&runs[3], &runs[2], "B2-A2")?,
    ];
    let reports = c
        .runs
        .iter()
        .zip(&runs)
        .enumerate()
        .map(|(index, (pin, run))| {
            let mut report = completed_report(pin, run)?;
            report["run"] = json!(LABELS[index]);
            let evidence = run.completed.as_ref().ok_or("missing byte-window first query evidence")?;
            report["first_query"] = json!({"ordinal":0,"query_wall_ns":run.samples[0].wall_ns,
                "charges":run.samples[0].charges,"hits10":run.samples[0].hits10,
                "returned_count":run.samples[0].returned.len(),
                "stage_wall_ns":{"discovery":evidence.stage_samples[0][0],"source":evidence.stage_samples[1][0],
                    "planning":evidence.stage_samples[2][0],"sq8":evidence.stage_samples[3][0]}});
            Ok(report)
        })
        .collect::<Result<Vec<_>>>()?;
    let quality_anchor = runs.iter().all(|run| run.terminal.total_hits10 >= 9780);
    let capacity = window["max_bytes"].as_u64().ok_or("byte-window capacity")?;
    let opportunities = [
        byte_window_opportunities(&runs[0], capacity),
        byte_window_opportunities(&runs[3], capacity),
    ];
    let potentially_binding = opportunities
        .iter()
        .all(|value| value["potentially_binding_queries"].as_u64().unwrap_or(0) > 0);
    let noise = [
        byte_window_comparison(&runs[0], &runs[3], "A2-A1 noise reference")?,
        byte_window_comparison(&runs[1], &runs[2], "B2-B1 noise reference")?,
    ];
    let screen_passed = quality_anchor
        && potentially_binding
        && comparisons
            .iter()
            .all(|pair| pair["timing_gate_passed"] == true);
    Ok(
        json!({"schema":BYTE_WINDOW_ABBA_SCHEMA,"status":"MEASURED","complete":true,
        "config_path":config_path,"config_sha256":config_sha,"config_bytes":config_bytes,
        "runtime_configs":c.runtime_configs,"shared_byte_window":window,
        "declared_run_order":LABELS,"execution_order_verified":false,
        "execution_order_authority":"external root original campaign receipts",
        "source_binary_qualification_verified":false,
        "source_binary_authority":"external root frozen source/archive/binary qualification receipts",
        "semantic_trace_physical_plan_logical_charge_parity":true,
        "recall_anchor_hits10":9780,"recall_denominator":10000,"recall_anchor_verified":quality_anchor,
        "window_opportunities":opportunities,"window_waiting_observed":false,
        "within_arm_noise_references":noise,
        "runs":reports,"comparisons":comparisons,"performance_screen_passed":screen_passed,"source_cache":"off",
        "cache_resources_cost_and_transport_qualified":false,
        "vendor_win_claim":false}),
    )
}

fn byte_window_opportunities(run: &Run, capacity: u64) -> Value {
    let could_bind = |sample: &Sample| {
        sample.charges.source.verified_bytes > capacity
            || sample.charges.sq8.verified_bytes > capacity
    };
    let mut tail: Vec<_> = run.samples.iter().enumerate().collect();
    tail.sort_by_key(|(ordinal, sample)| (std::cmp::Reverse(sample.wall_ns), *ordinal));
    let tail_count = run.samples.len().div_ceil(20);
    json!({"potentially_binding_queries":run.samples.iter().filter(|q| could_bind(q)).count(),
        "control_top_5_percent_queries":tail_count,
        "potentially_binding_control_top_5_percent_queries":tail.iter().take(tail_count).filter(|(_,q)| could_bind(q)).count(),
        "scope":"phase total bytes above capacity is an upper bound on opportunities, not evidence of waiting; per-range SOURCE feasibility requires separate native admission"})
}

fn byte_window_comparison(a: &Run, b: &Run, label: &str) -> Result<Value> {
    let mut report = paired(a, b, label)?;
    let a = statistics(&a.samples.iter().map(|q| q.wall_ns).collect::<Vec<_>>())?;
    let b = statistics(&b.samples.iter().map(|q| q.wall_ns).collect::<Vec<_>>())?;
    let p95 = b.p95_ns < a.p95_ns;
    let p90 = u128::from(b.p90_ns) * 100 <= u128::from(a.p90_ns) * 105;
    let qps = u128::from(a.query_wall_ns) * 100 >= u128::from(b.query_wall_ns) * 95;
    report["p95_improved"] = json!(p95);
    report["p90_regression_at_most_5_percent"] = json!(p90);
    report["qps_regression_at_most_5_percent"] = json!(qps);
    report["timing_gate_passed"] = json!(p95 && p90 && qps);
    report["screen"] = json!(
        "positive p95 improvement; at most 5 percent p90/QPS regression; both pairs required externally; p99/first-query retained"
    );
    Ok(report)
}

fn reduce_membership_abba(config_path: &Path, config_sha: &str) -> Result<Value> {
    let (c, config_bytes): (MembershipAbbaConfig, _) = read_config(config_path, config_sha)?;
    require(
        c.schema == "borsuk-membership-abba-native-reduction-config-v1",
        "membership ABBA config schema",
    )?;
    require(
        valid_sha(&c.runtime_config_sha256),
        "attempt runtime config SHA256",
    )?;
    for commit in &c.producer_source_commits {
        require(
            commit.len() == 40
                && commit
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "producer source commit",
        )?;
    }
    require(
        c.producer_source_commits[0] != c.producer_source_commits[1]
            && c.producer_source_archive_sha256
                .iter()
                .all(|s| valid_sha(s))
            && c.producer_source_archive_sha256[0] != c.producer_source_archive_sha256[1],
        "independently frozen producer sources",
    )?;
    for run in &c.runs {
        require(
            run.schema == "borsuk-completed-native-reduction-config-v1"
                && run.expected_identity["fetch_parallelism"] == 32
                && run.expected_bound_inputs["fetch_parallelism"] == 32
                && run.expected_bound_inputs["source_cache"] == "off",
            "membership ABBA width32/cache pins",
        )?;
        require(
            run.expected_identity["config_sha256"] == c.runtime_config_sha256
                && run.expected_bound_inputs == c.runs[0].expected_bound_inputs,
            "membership ABBA attempt/config/input binding",
        )?;
        let i: Inputs = serde_json::from_value(run.expected_bound_inputs.clone())?;
        require(
            i.dataset == "CohereLabs/wikipedia-2023-11-embed-multilingual-v3"
                && i.revision == "ade45fb52bd549f5e8c065636fe4160a43c2af36"
                && i.metric == "cosine"
                && i.tie_rule == "corpus_ordinal_ascending"
                && i.rows == 100000
                && i.dimensions == 1024
                && i.count == COUNT
                && i.k == K
                && i.corpus_source_first == 0
                && i.query_source_first == 100000
                && i.profile == "native100k"
                && !i.truth_opened
                && i.requests_bytes == 4096000
                && i.truth_bytes == 80000
                && [
                    i.requests_sha256.as_str(),
                    i.truth_sha256.as_str(),
                    i.native_source_sha256.as_str(),
                    i.native_sq8_sha256.as_str(),
                    i.native_order_sha256.as_str(),
                ] == INPUT_SHA256,
            "membership ABBA production benchmark pins",
        )?;
    }
    require(
        c.runs[0].expected_identity == c.runs[3].expected_identity
            && c.runs[1].expected_identity == c.runs[2].expected_identity,
        "membership ABBA same-arm identity drift",
    )?;
    let mut identity = c.runs[0].expected_identity.clone();
    for field in [
        "binary_sha256",
        "generation_source_sha256",
        "router_source_sha256",
    ] {
        require(
            identity[field] != c.runs[1].expected_identity[field],
            "membership ABBA unchanged producer pin",
        )?;
        identity[field] = c.runs[1].expected_identity[field].clone();
    }
    require(
        identity == c.runs[1].expected_identity,
        "membership ABBA unapproved component delta",
    )?;
    let mut runs: Vec<Run> = Vec::with_capacity(4);
    for (index, pinned) in c.runs.iter().enumerate() {
        let run = read_run_with(&pinned.input.path, &pinned.input.sha256, Some(pinned))
            .map_err(|e| format!("{}: {e}", LABELS[index]))?;
        for (old, old_pin) in runs.iter().zip(&c.runs) {
            require(
                pinned.input.sha256 != old_pin.input.sha256
                    && (run.file_identity.dev, run.file_identity.ino)
                        != (old.file_identity.dev, old.file_identity.ino),
                "duplicate membership ABBA run evidence",
            )?;
        }
        let control = index == 0 || index == 3;
        for (ordinal, sample) in run.samples.iter().enumerate() {
            let router_valid = if control {
                (1..=16).contains(&sample.charges.router.submitted_gets)
                    && sample.charges.router.verified_bytes > 0
            } else {
                sample.charges.router == Charge::default()
            };
            require(
                router_valid,
                &format!(
                    "{} membership ABBA query {ordinal}: router charge",
                    LABELS[index]
                ),
            )?;
            if let Some(first) = runs.first() {
                let a = &first.samples[ordinal];
                require(
                    a.returned == sample.returned
                        && a.hits10 == sample.hits10
                        && a.trace_sha256 == sample.trace_sha256
                        && a.charges.source == sample.charges.source
                        && a.charges.sq8 == sample.charges.sq8
                        && (!control || a.charges.router == sample.charges.router),
                    &format!(
                        "{} membership ABBA query {ordinal}: ordered hits/scorebits/recall/nomination trace/source/SQ8 mismatch",
                        LABELS[index]
                    ),
                )?;
            }
        }
        let e = run
            .completed
            .as_ref()
            .ok_or("missing membership evidence")?;
        let transport = &e.last.as_ref().ok_or("missing transport")?.after;
        require(
            transport.transport_failures == 0
                && transport.stream_failures == 0
                && transport.dropped_error_bodies == 0
                && run.terminal.sum.failed_gets == 0
                && run.terminal.underfilled_queries == 0,
            "membership ABBA failed/underfilled run",
        )?;
        if let Some(first) = runs.first() {
            require(
                e.binding_charge
                    == first
                        .completed
                        .as_ref()
                        .ok_or("missing binding")?
                        .binding_charge,
                "membership ABBA source binding charge mismatch",
            )?;
        }
        require(
            if control {
                (1..=16).contains(&run.stages.max_leaf_peak_inflight)
                    && e.startup["metadata"]["router_head_requests"] == 1
            } else {
                run.stages.max_leaf_peak_inflight == 0
                    && e.startup["metadata"]["router_head_requests"] == 0
                    && e.startup["metadata"]["router_head_wall_ns"] == 0
            },
            "membership ABBA leaf I/O boundary",
        )?;
        runs.push(run);
    }
    let comparisons = [
        membership_comparison(&runs[0], &runs[1], "B1-A1")?,
        membership_comparison(&runs[3], &runs[2], "B2-A2")?,
    ];
    let screen_passed = comparisons
        .iter()
        .all(|pair| pair["timing_gate_passed"] == true);
    let reports = c
        .runs
        .iter()
        .zip(&runs)
        .enumerate()
        .map(|(index, (pinned, run))| {
            let mut report = completed_report(pinned, run)?;
            report["run"] = json!(LABELS[index]);
            Ok(report)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(
        json!({"schema":MEMBERSHIP_ABBA_SCHEMA,"status":"MEASURED","complete":true,
        "config_path":config_path,"config_sha256":config_sha,"config_bytes":config_bytes,
        "runtime_config_sha256":c.runtime_config_sha256,
        "producer_source_commits":c.producer_source_commits,
        "producer_source_archive_sha256":c.producer_source_archive_sha256,
        "producer_provenance_verified":false,
        "producer_provenance_authority":"external root frozen source/archive/binary qualification receipts; archive bodies not reopened",
        "declared_run_order":LABELS,"execution_order_verified":false,
        "execution_order_authority":"external root attempt lifecycle receipts; JSONL files do not attest chronological execution",
        "attempt_config_authority":"external root freezes unique scratch_parent in the runtime config; all four emitted config SHA256s must match this attempt pin",
        "semantic_parity":true,"nomination_trace_parity":true,"source_sq8_charge_parity":true,
        "candidate_router_charge_zero":true,"candidate_leaf_peak_zero":true,"candidate_router_head_zero":true,
        "physical_range_parity_verified_by_reducer":false,
        "physical_range_evidence":"external native exact-request parity fixture and unchanged source/SQ8 planner; emitted nomination traces contain no physical ranges",
        "removed_router_query_charges":[runs[0].terminal.charges.router,runs[3].terminal.charges.router],
        "runs":reports,"comparisons":comparisons,"performance_screen_passed":screen_passed,
        "screen":"p90 and p95 at least 5 percent lower AND reciprocal serial QPS no regression in BOTH A1/B1 and A2/B2 pairs",
        "disposition":if screen_passed {"SCREEN_PASSED_EXTERNAL_GATES_REQUIRED"} else {"SCREEN_FAILED"},
        "statistics_population":"all 1000 ordered queries per run including first; all four runs retained",
        "source_cache":"off","payload_cache":"off; frozen runtime gate required",
        "local_file_only":true,"external_resources_and_cost_gate_required":true,
        "frozen_runtime_root_config_admission_required":true,"qualified":false,
        "supervisor_resources_cost_and_cache_qualified":false,"production_win_claim":false,
        "vendor_or_scientific_win_claim":false}),
    )
}

// Reuse the historical ratios, exact integer quantiles and per-query deltas;
// only this prospective screen uses no QPS regression instead of +5 percent.
fn membership_comparison(a: &Run, b: &Run, label: &str) -> Result<Value> {
    let mut comparison = paired(a, b, label)?;
    let qps_pass = b.terminal.query_wall_ns <= a.terminal.query_wall_ns;
    comparison
        .as_object_mut()
        .ok_or("comparison object")?
        .remove("sequential_qps_at_least_5_percent_higher");
    comparison["sequential_qps_no_regression"] = json!(qps_pass);
    comparison["timing_gate_passed"] = json!(
        comparison["p90_at_least_5_percent_lower"] == true
            && comparison["p95_at_least_5_percent_lower"] == true
            && qps_pass
    );
    Ok(comparison)
}

fn paired(a: &Run, b: &Run, label: &str) -> Result<Value> {
    let a_stats = statistics(&a.samples.iter().map(|s| s.wall_ns).collect::<Vec<_>>())?;
    let b_stats = statistics(&b.samples.iter().map(|s| s.wall_ns).collect::<Vec<_>>())?;
    let p90_pass = u128::from(b_stats.p90_ns) * 100 <= u128::from(a_stats.p90_ns) * 95;
    let p95_pass = u128::from(b_stats.p95_ns) * 100 <= u128::from(a_stats.p95_ns) * 95;
    // Both populations have 1000 samples: QPS_B/QPS_A = wall_A/wall_B.
    let qps_pass =
        u128::from(b_stats.query_wall_ns) * 105 <= u128::from(a_stats.query_wall_ns) * 100;
    let deltas: Vec<_> = a
        .samples
        .iter()
        .zip(&b.samples)
        .enumerate()
        .map(|(ordinal, (a, b))| {
            let delta = b.wall_ns as i64 - a.wall_ns as i64;
            json!({"ordinal":ordinal,"query_wall_ns":delta,"query_wall_ms":delta as f64/1e6})
        })
        .collect();
    Ok(
        json!({"comparison":label,"delta_direction":"B_minus_A","deltas":deltas,
        "p90_ratio":b_stats.p90_ns as f64/a_stats.p90_ns as f64,
        "p95_ratio":b_stats.p95_ns as f64/a_stats.p95_ns as f64,
        "sequential_qps_ratio":a_stats.query_wall_ns as f64/b_stats.query_wall_ns as f64,
        "p90_at_least_5_percent_lower":p90_pass,"p95_at_least_5_percent_lower":p95_pass,
        "sequential_qps_at_least_5_percent_higher":qps_pass,
        "timing_gate_passed":p90_pass && p95_pass && qps_pass}),
    )
}

fn compare(paths: &[(PathBuf, String); 4]) -> Result<Value> {
    let mut runs: Vec<Run> = Vec::with_capacity(4);
    for (i, (path, sha)) in paths.iter().enumerate() {
        let run = read_run(path, sha).map_err(|e| format!("{}: {e}", LABELS[i]))?;
        let arm = usize::from(i == 1 || i == 2);
        require(
            run.identity.binary_sha256 == BINARY_SHA256[arm],
            &format!("{}: frozen arm binary mismatch", LABELS[i]),
        )?;
        for (old, (_, old_sha)) in runs.iter().zip(paths) {
            require(
                sha != old_sha
                    && (run.file_identity.dev, run.file_identity.ino)
                        != (old.file_identity.dev, old.file_identity.ino),
                "duplicate run evidence",
            )?;
        }
        if let Some(a) = runs.first() {
            require(
                run.identity.config_sha256 == a.identity.config_sha256 && run.inputs == a.inputs,
                "cross-run config/input/root/population mismatch",
            )?;
            for (ordinal, (a, b)) in a.samples.iter().zip(&run.samples).enumerate() {
                require(
                    a.returned == b.returned
                        && a.hits10 == b.hits10
                        && a.charges == b.charges
                        && a.trace_sha256 == b.trace_sha256,
                    &format!(
                        "{} query {ordinal}: semantic/recall/logical-charge/trace mismatch",
                        LABELS[i]
                    ),
                )?;
            }
        }
        runs.push(run);
    }
    require(
        runs[0].identity == runs[3].identity && runs[1].identity == runs[2].identity,
        "binary/source identity changed within arm",
    )?;
    let comparisons = [
        paired(&runs[0], &runs[1], "B1-A1")?,
        paired(&runs[3], &runs[2], "B2-A2")?,
    ];
    let timing_pass = comparisons.iter().all(|c| c["timing_gate_passed"] == true);
    let pooled = |indices: [usize; 2]| -> Result<Value> {
        let mut summary = serde_json::to_value(statistics(
            &indices
                .into_iter()
                .flat_map(|i| runs[i].samples.iter().map(|s| s.wall_ns))
                .collect::<Vec<_>>(),
        )?)?;
        let [a, b] = indices.map(|i| &runs[i].stages);
        let stages = StageTotals {
            discovery_ns: plus(a.discovery_ns, b.discovery_ns)?,
            source_ns: plus(a.source_ns, b.source_ns)?,
            planning_ns: plus(a.planning_ns, b.planning_ns)?,
            sq8_ns: plus(a.sq8_ns, b.sq8_ns)?,
            max_leaf_peak_inflight: a.max_leaf_peak_inflight.max(b.max_leaf_peak_inflight),
        };
        summary["stage_wall_sums"] = serde_json::to_value(stages)?;
        Ok(summary)
    };
    let mut reports = Vec::with_capacity(4);
    for (i, run) in runs.iter().enumerate() {
        let (path, sha) = &paths[i];
        let label = LABELS[i];
        let stats = statistics(&run.samples.iter().map(|s| s.wall_ns).collect::<Vec<_>>())?;
        reports.push(json!({"run":label,"result_path":path,"result_sha256":sha,
            "result_bytes":run.file_identity.len,"identity":run.identity,"statistics":stats,
            "stage_wall_sums":run.stages,"native_terminal":run.terminal}));
    }
    let inputs = &runs[0].inputs;
    Ok(
        json!({"schema":"borsuk-compare-native-replay-v1","status":"MEASURED","complete":true,
        "semantic_parity":true,"trace_plan_parity":true,"timing_gate_passed":timing_pass,
        "disposition":if timing_pass {"TIMING_GATE_PASSED_EXTERNAL_GATES_REQUIRED"} else {"COMPLETED_NO_WIN"},
        "local_file_only":true,"physical_s3":false,"external_resources_and_cost_gate_required":true,
        "frozen_runtime_root_config_admission_required":true,"qualified":false,
        "supervisor_resources_cost_and_cache_qualified":false,"cache_state":"shared_OS_page_cache_uncontrolled",
        "vendor_or_scientific_win_claim":false,"inputs":inputs,
        "runs":reports,"pooled":{"A":pooled([0,3])?,"B":pooled([1,2])?},"comparisons":comparisons}),
    )
}

fn report_cap(schema: &str) -> u64 {
    if schema == sq8_subpage_shortlist::GET_CURVE_SCHEMA {
        32 * 1024 * 1024
    } else if schema == sq8_split_panel::REPORT_SCHEMA {
        16 * 1024 * 1024
    } else if schema == sq8_panel_attribution::REPORT_SCHEMA {
        512 * 1024
    } else if schema == sq8_subpage_shortlist::VIRTUAL_LAYOUT_SCHEMA
        || schema == sq8_subpage_shortlist::COUNT_CURVE_SCHEMA
        || schema == sq8_cosine::REPORT_SCHEMA
        || schema == f32_top16::REPORT_SCHEMA
    {
        16 * 1024 * 1024
    } else {
        REPORT_CAP
    }
}

#[cfg(test)]
#[test]
fn sq8_cosine_report_writer_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    for (case, schema, status, bytes, accepted) in [
        (
            "cosine-over2",
            sq8_cosine::REPORT_SCHEMA,
            "NATIVE_REPLAY_VERIFIED_SQ8_COSINE",
            REPORT_CAP + 1,
            true,
        ),
        (
            "cosine-exact16",
            sq8_cosine::REPORT_SCHEMA,
            "NATIVE_REPLAY_VERIFIED_SQ8_COSINE",
            16 * 1024 * 1024,
            true,
        ),
        (
            "cosine-over16",
            sq8_cosine::REPORT_SCHEMA,
            "NATIVE_REPLAY_VERIFIED_SQ8_COSINE",
            16 * 1024 * 1024 + 1,
            false,
        ),
        (
            "f32-exact2",
            fetched_f32::REPORT_SCHEMA,
            "NATIVE_REPLAY_VERIFIED_F32_DIAGNOSTIC",
            REPORT_CAP,
            true,
        ),
        (
            "f32-over2",
            fetched_f32::REPORT_SCHEMA,
            "NATIVE_REPLAY_VERIFIED_F32_DIAGNOSTIC",
            REPORT_CAP + 1,
            false,
        ),
    ] {
        let mut report =
            json!({"schema":schema,"status":status,"complete":true,"qualified":false,"padding":""});
        let overhead = serde_json::to_vec(&report).unwrap().len() as u64 + 1;
        report["padding"] = json!("x".repeat(usize::try_from(bytes - overhead).unwrap()));
        let path = dir.path().join(case);
        let result = execute_report(&path, schema, || Ok(report));
        if accepted {
            assert!(result.unwrap(), "{case}");
            assert_eq!(std::fs::metadata(&path).unwrap().len(), bytes, "{case}");
        } else {
            assert!(
                result.unwrap_err().to_string().contains("report byte cap"),
                "{case}"
            );
            assert!(
                std::fs::metadata(&path).unwrap().len() <= report_cap(schema),
                "{case}"
            );
        }
    }
    assert_eq!(report_cap("unrelated-schema"), REPORT_CAP);
    assert_eq!(
        report_cap(sq8_subpage_shortlist::COUNT_CURVE_SCHEMA),
        16 * 1024 * 1024
    );
}

struct Output {
    file: File,
    bytes: u64,
    cap: u64,
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() as u64 > self.cap.saturating_sub(self.bytes) {
            return Err(io::Error::other("report byte cap"));
        }
        let n = self.file.write(bytes)?;
        self.bytes += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn execute(paths: &[(PathBuf, String); 4], output: &Path) -> Result<bool> {
    execute_report(output, "borsuk-compare-native-replay-v1", || compare(paths))
}

fn execute_report(
    output: &Path,
    schema: &str,
    reduce: impl FnOnce() -> Result<Value>,
) -> Result<bool> {
    execute_report_with_sync(output, schema, reduce, |file, _| file.sync_all())
}

fn execute_report_with_sync(
    output: &Path,
    schema: &str,
    reduce: impl FnOnce() -> Result<Value>,
    mut sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<bool> {
    let dir = parent(output)?;
    let file = File::from(openat(
        &dir,
        output.file_name().ok_or("output filename")?,
        OFlags::WRONLY
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    let original = file_identity(&file)?;
    let report = match reduce() {
        Ok(report) => report,
        Err(e) => {
            json!({"schema":schema,"status":"INVALID","complete":false,
            "error":e.to_string().chars().take(512).collect::<String>(),"local_file_only":true,
            "physical_s3":false,"external_resources_and_cost_gate_required":true,
            "frozen_runtime_root_config_admission_required":true,"qualified":false,
            "supervisor_resources_cost_and_cache_qualified":false,"vendor_or_scientific_win_claim":false})
        }
    };
    if schema == f32_top16::REPORT_SCHEMA {
        return f32_top16::publish(output, &report, dir, file, sync);
    }
    let mut out = Output {
        file,
        bytes: 0,
        cap: report_cap(schema),
    };
    serde_json::to_writer(&mut out, &report)?;
    out.write_all(b"\n")?;
    sync(&out.file, false)?;
    if schema == SCALE_CONFIG_SCHEMA {
        require(
            out.file.metadata()?.nlink() == 1,
            "single-link completed config",
        )?;
    }
    let current = file_identity(&open_input(output)?)?;
    let bound_dir = dir.metadata()?;
    let current_dir = parent(output)?.metadata()?;
    require(
        (current.dev, current.ino, current.len) == (original.dev, original.ino, out.bytes)
            && (bound_dir.dev(), bound_dir.ino()) == (current_dir.dev(), current_dir.ino()),
        "output/parent identity changed",
    )?;
    sync(&dir, true)?;
    if schema == fp32_serving::REPORT_SCHEMA {
        return Ok(report["schema"] == schema
            && report["valid"] == true
            && report.get("status").is_none());
    }
    if schema == "borsuk-paired-source-layout-v1" {
        return Ok(report["schema"] == schema
            && report["status"] == "PARITY_VERIFIED"
            && report["complete"] == true
            && report["qualified"] == false);
    }
    if schema == sq8_cosine::REPORT_SCHEMA {
        return Ok(report["schema"] == schema
            && report["status"] == "NATIVE_REPLAY_VERIFIED_SQ8_COSINE"
            && report["complete"] == true
            && report["qualified"] == false);
    }
    if schema == fetched_f32::REPORT_SCHEMA {
        return Ok(report["schema"] == schema
            && report["status"] == "NATIVE_REPLAY_VERIFIED_F32_DIAGNOSTIC"
            && report["complete"] == true
            && report["qualified"] == false);
    }
    if schema == source_frame_range::REPORT_SCHEMA {
        return Ok(report["schema"] == schema
            && report["complete"] == true
            && matches!(
                report["status"].as_str(),
                Some("SOURCE_ONLY_ADMITTED" | "SOURCE_ONLY_COST_REPLAY")
            )
            && report["qualified"] == false);
    }
    if schema == sq8_split_panel::REPORT_SCHEMA {
        return Ok(report["schema"] == schema
            && report["status"] == "VALIDATED_FIXED_PANEL"
            && report["complete"] == true
            && report["population_percentiles_valid"] == false);
    }
    if schema == sq8_panel_attribution::REPORT_SCHEMA {
        return Ok(report["schema"] == schema
            && report["complete"] == true
            && matches!(
                report["status"].as_str(),
                Some("MEASURED" | "INCONCLUSIVE_FIXED_PANEL")
            )
            && report["diagnostic_only"] == true
            && report["population_percentiles_valid"] == false);
    }
    if schema == v9_panel::ORACLE_SCHEMA {
        return Ok(report["schema"] == schema
            && report["status"] == "EXACT_RECORDED_SOURCE_COVER"
            && report["complete"] == true
            && report["selected_count"] == 8
            && report["recorded_source_frontier_only"] == true
            && report["independent_router_frontier_qualified"] == false
            && report["producer_provenance_qualified"] == false
            && report["cold_performance_claim"] == false
            && report["vendor_win_claim"] == false);
    }
    if schema == v9_panel::PARITY_SCHEMA {
        return Ok(report["schema"] == schema
            && report["status"] == "EXACT_SELECTED_RANK_PARITY"
            && report["complete"] == true
            && report["selected_count"] == 8
            && report["exact_range_oracle_qualified"] == false
            && report["producer_provenance_qualified"] == false
            && report["cold_performance_claim"] == false
            && report["vendor_win_claim"] == false);
    }
    if schema == v9_panel::REPORT_SCHEMA {
        return Ok(report["schema"] == schema
            && report["status"] == "VALIDATED_PANEL"
            && report["complete"] == true
            && report["producer_provenance_qualified"] == false
            && report["cold_performance_claim"] == false
            && report["vendor_win_claim"] == false);
    }
    if schema == v9_fixture::REPORT_SCHEMA {
        return Ok(report["schema"] == schema
            && report["status"] == "IDENTITY_REBOUND_PROOFS_ONLY"
            && report["complete"] == true
            && report["serving_qualified"] == false
            && report["canonical_body_authenticated"] == false
            && report["query_or_truth_opened"] == false);
    }
    if schema == SOURCE_INTERVAL_SCHEMA {
        return Ok(report["schema"] == SOURCE_INTERVAL_SCHEMA
            && report["status"] == "INTERVAL_GEOMETRY_ONLY"
            && report["complete"] == true);
    }
    if schema == SOURCE_COVER_CURVE_SCHEMA {
        return Ok(report["schema"] == SOURCE_COVER_CURVE_SCHEMA
            && report["status"] == "COUNTERFACTUAL_COST_ONLY"
            && report["complete"] == true);
    }
    if schema == sq8_subpage_shortlist::GEOMETRY_SCHEMA {
        return Ok(report["schema"] == schema
            && report["status"] == "CLOSED_GEOMETRY_ONLY"
            && report["complete"] == true);
    }
    if schema == sq8_subpage_shortlist::COUNT_CURVE_SMOKE_SCHEMA {
        return Ok(report["schema"] == schema
            && report["status"] == "STAGING_SMOKE_CLOSED"
            && report["complete"] == true
            && report["candidate_queries_executed"] == 1
            && report["arms_executed"] == 4
            && report["quality_evaluated"] == false
            && report["truth_file_opened"] == false);
    }
    if schema == sq8_subpage_shortlist::VIRTUAL_LAYOUT_SMOKE_SCHEMA {
        return Ok(report["schema"] == schema
            && report["status"] == "STAGING_SMOKE_CLOSED"
            && report["complete"] == true
            && report["candidate_queries_executed"] == 1
            && report["quality_evaluated"] == false
            && report["truth_file_opened"] == false);
    }
    if schema == sq8_subpage_shortlist::VIRTUAL_LAYOUT_SCHEMA {
        return Ok(sq8_subpage_shortlist::validate_virtual_report(&report).is_ok());
    }
    if schema == sq8_subpage_shortlist::COUNT_CURVE_SCHEMA {
        return Ok(sq8_subpage_shortlist::validate_count_curve_report(&report).is_ok());
    }
    if schema == sq8_subpage_shortlist::GET_CURVE_SCHEMA {
        return Ok(sq8_subpage_shortlist::validate_get_curve_report(&report).is_ok());
    }
    if schema == sq8_subpage_shortlist::GET_CURVE_ADMISSION_SCHEMA
        || schema == sq8_subpage_shortlist::GET_CURVE_SMOKE_SCHEMA
    {
        return Ok(sq8_subpage_shortlist::validate_get_curve_gate(&report, schema).is_ok());
    }
    if schema == sq8_subpage_shortlist::ADMISSION_SCHEMA
        || schema == sq8_subpage_shortlist::COUNT_CURVE_ADMISSION_SCHEMA
        || schema == sq8_subpage_shortlist::VIRTUAL_LAYOUT_ADMISSION_SCHEMA
    {
        return Ok(report["schema"] == schema
            && report["status"] == "READY_ACTUAL_INPUT"
            && report["complete"] == true
            && report["candidate_queries_executed"] == 0
            && report["scientific_qualified"] == false);
    }
    Ok(report["status"] == "MEASURED"
        || (schema == SCALE_PARITY_REPORT_SCHEMA
            && report["schema"] == SCALE_PARITY_REPORT_SCHEMA
            && report["status"] == "EXACT_PREFIX_PARITY"
            && report["complete"] == true)
        || (schema == SCALE_CONFIG_SCHEMA
            && report["schema"] == SCALE_CONFIG_SCHEMA
            && report.get("expected_identity").is_some()
            && report.get("expected_bound_inputs").is_some()))
}

const LOSS_SCHEMA: &str = "borsuk-sealed-scale-loss-v2";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LossConfig {
    schema: String,
    completed: CompletedConfig,
    sq8: CompletedInput,
    truth: CompletedInput,
    max_payload_bytes: u64,
}

struct LossCoverage {
    ordinal: usize,
    scored_units: Vec<usize>,
    sq8_rows: Vec<[usize; 2]>,
}

fn loss_coverage(line: &[u8], q: &Query, i: &Inputs) -> Result<LossCoverage> {
    let raw: Value = serde_json::from_slice(line)?;
    require(
        raw["serving"]["mode"] == "baseline" && raw["source_nomination_skipped"] == false,
        "loss analysis requires actual baseline nomination",
    )?;
    let t: SourceTrace = serde_json::from_value(raw["trace"].clone())?;
    require(t.discoveries.is_empty(), "semantic loss trace required")?;
    let coverage = borsuk::semantic_unit_router::SemanticCoverage::new(
        borsuk::semantic_unit_router::SemanticProfile::Scale1m, i.semantic_leaf_limit)?;
    let closure_cap = coverage.closure_page_limit();
    let unit_count = i.rows.div_ceil(32);
    let semantic = source_ordinals(&t.semantic_units, unit_count, closure_cap)?;
    require(
        !semantic.is_empty() && t.semantic_units.windows(2).all(|p| p[0] < p[1]),
        "sorted semantic loss units",
    )?;
    let seed = semantic.first().ok_or("loss seed")? / 8;
    let additions = (seed * 8..((seed + 1) * 8).min(unit_count))
        .filter(|u| !semantic.contains(u))
        .collect::<Vec<_>>();
    require(
        t.semantic_seed_additions == additions,
        "loss seed completion",
    )?;
    let walked = semantic
        .union(&additions.into_iter().collect())
        .copied()
        .collect::<BTreeSet<_>>();
    let pages = walked.iter().map(|u| u / 8).collect::<BTreeSet<_>>();
    require(
        !pages.is_empty()
            && pages.len() <= closure_cap
            && source_ordinals(&t.ranked_candidate_pages, i.rows.div_ceil(256), closure_cap)? == pages
            && t.ranked_candidate_pages.first() == Some(&t.primary_page),
        "loss ranked SOURCE closure",
    )?;
    let scored = source_ordinals(&t.nomination_evaluated_units, unit_count, coverage.source_unit_limit())?;
    let expected = pages
        .iter()
        .flat_map(|p| p * 8..((p + 1) * 8).min(unit_count))
        .collect::<BTreeSet<_>>();
    require(
        scored == expected,
        "loss trace must score full SOURCE closure",
    )?;
    let record_bytes = source_record_bytes(i.dimensions)?;
    // Missing budget belongs only to the frozen historical 128-GET protocol.
    // Current headers admit and bind the explicit integer budget separately.
    let source_get_budget = i.source_get_budget.unwrap_or(128);
    require(
        matches!(source_get_budget, 96 | 128),
        "SOURCE cover budget must be 96 or 128",
    )?;
    let (source_ranges, source_bytes) = borsuk::two_bit_generation::plan_two_bit_source_cover(
        &pages,
        i.rows,
        record_bytes,
        source_get_budget,
        64 * 1024 * 1024,
    )?;
    require(
        q.charges.source.submitted_gets == source_ranges.len() as u64
            && q.charges.source.verified_bytes == source_bytes as u64
            && q.charges.source.failed_gets == 0,
        "loss SOURCE cover charges",
    )?;
    // SOURCE closure stays 256 rows; current SQ8 selections name 32-row blocks.
    let page_rows = match raw["plan"].get("page_rows") {
        None => 256,
        Some(value) => usize::try_from(value.as_u64().ok_or("loss SQ8 page rows")?)?,
    };
    require(matches!(page_rows, 32 | 256), "loss SQ8 page geometry")?;
    let selected: Vec<usize> = serde_json::from_value(raw["plan"]["selected_pages"].clone())?;
    require(
        !selected.is_empty()
            && selected.iter().copied().collect::<BTreeSet<_>>().len() == selected.len()
            && if page_rows == 32 {
                selected.len() <= 256
                    && selected.windows(2).all(|p| p[0] < p[1])
                    && selected.iter().all(|p| scored.contains(p))
            } else {
                selected.len() <= pages.len()
                    && selected.iter().all(|p| pages.contains(p))
                    && selected.contains(&t.primary_page)
            },
        "loss selected SQ8 pages within scored SOURCE closure",
    )?;
    let ranges: Vec<[usize; 2]> = serde_json::from_value(raw["plan"]["ranges"].clone())?;
    let width = i.dimensions.checked_add(12).ok_or("loss SQ8 width")?;
    let page = width.checked_mul(page_rows).ok_or("loss SQ8 page")?;
    let end = width.checked_mul(i.rows).ok_or("loss SQ8 body")?;
    require(
        !ranges.is_empty() && ranges.len() <= 32,
        "loss SQ8 range count",
    )?;
    let mut bytes = 0_u64;
    let mut previous = 0;
    for &[start, stop] in &ranges {
        require(
            start >= previous
                && start < stop
                && stop <= end
                && start % page == 0
                && (stop % page == 0 || stop == end),
            "loss SQ8 aligned ordered cover",
        )?;
        bytes = plus(bytes, (stop - start) as u64)?;
        previous = stop;
    }
    require(
        selected.iter().all(|p| {
            ranges
                .iter()
                .any(|r| r[0] <= p * page && r[1] >= ((p + 1) * page).min(end))
        }) && raw["plan"]["planned_bytes"].as_u64() == Some(bytes)
            && q.charges.sq8.submitted_gets == ranges.len() as u64
            && q.charges.sq8.verified_bytes == bytes
            && q.charges.sq8.failed_gets == 0,
        "loss actual SQ8 cover/charges",
    )?;
    if page_rows == 32 {
        let covered = ranges
            .iter()
            .map(|r| (r[1] - r[0]).div_ceil(page))
            .sum::<usize>();
        let target = raw["plan"]["target_pages"]
            .as_u64()
            .ok_or("loss SQ8 target")?;
        require(
            bytes <= 32 * 1024 * 1024
                && target >= selected.len() as u64
                && target <= 256.min(unit_count) as u64
                && raw["plan"]["target_shortfall"] == target - selected.len() as u64
                && raw["plan"]["primary_pages_retained"] == 0
                && raw["plan"]["covered_pages"] == covered
                && raw["plan"]["bridge_pages"] == covered - selected.len()
                && raw["plan"]
                    .get("logical_range_count")
                    .is_none_or(|n| n.as_u64() == Some(ranges.len() as u64)),
            "loss current SQ8 counts/charges",
        )?;
    }
    Ok(LossCoverage {
        ordinal: q.ordinal,
        scored_units: scored.into_iter().collect(),
        sq8_rows: ranges
            .into_iter()
            .map(|r| [r[0] / width, r[1] / width])
            .collect(),
    })
}

fn loss_binary(pin: &CompletedInput, bytes: u64) -> Result<(Vec<u8>, HeaderSnapshot)> {
    header_pin(pin, bytes)?;
    require(pin.bytes == bytes, "loss binary exact geometry")?;
    let file = header_file(&pin.path)?;
    let identity = file_identity(&file)?;
    require(identity.len == bytes, "loss binary exact length")?;
    let mut body = Vec::with_capacity(usize::try_from(bytes)?);
    Read::take(&mut &file, bytes + 1).read_to_end(&mut body)?;
    require(
        body.len() as u64 == bytes && format!("{:x}", Sha256::digest(&body)) == pin.sha256,
        "loss binary full SHA/EOF",
    )?;
    let snapshot = HeaderSnapshot {
        file,
        path: pin.path.clone(),
        identity,
    };
    snapshot.recheck()?;
    Ok((body, snapshot))
}

// Authenticate the actual ranked payload and its physical ID order in one bounded pass.
// This diagnostic accepts logical ordinals only, not arbitrary application IDs.
fn loss_positions(
    pin: &CompletedInput,
    rows: usize,
    dimensions: usize,
    order_sha256: &str,
    wanted: &BTreeSet<u64>,
) -> Result<(std::collections::BTreeMap<u64, usize>, HeaderSnapshot)> {
    scan_loss_sq8(pin, rows, dimensions, order_sha256, wanted, |_, _| Ok(()))
}

fn scan_loss_sq8(
    pin: &CompletedInput,
    rows: usize,
    dimensions: usize,
    order_sha256: &str,
    wanted: &BTreeSet<u64>,
    mut observe: impl FnMut(usize, &[u8]) -> Result<()>,
) -> Result<(std::collections::BTreeMap<u64, usize>, HeaderSnapshot)> {
    require(
        rows > 0 && dimensions > 0 && dimensions <= 1024,
        "loss SQ8 shape",
    )?;
    let width = dimensions.checked_add(12).ok_or("loss SQ8 width")?;
    let bytes = (rows as u64)
        .checked_mul(width as u64)
        .ok_or("loss SQ8 bytes")?;
    header_pin(pin, bytes)?;
    require(pin.bytes == bytes, "loss SQ8 exact geometry")?;
    let file = header_file(&pin.path)?;
    let identity = file_identity(&file)?;
    require(identity.len == bytes, "loss SQ8 exact length")?;
    let mut input = BufReader::with_capacity(8192, file);
    let mut seen = vec![0_u8; rows.div_ceil(8)];
    let mut digest = Sha256::new();
    let mut order_digest = Sha256::new();
    let mut record = vec![0_u8; width];
    let mut positions = std::collections::BTreeMap::new();
    for physical in 0..rows {
        input.read_exact(&mut record)?;
        digest.update(&record);
        order_digest.update(&record[..8]);
        let id = i64::from_le_bytes(record[..8].try_into().unwrap());
        let norm = f32::from_le_bytes(record[8..12].try_into().unwrap());
        require(norm.is_finite() && norm > 0., "loss SQ8 norm")?;
        require(id >= 0 && (id as u64) < rows as u64, "loss SQ8 ordinal")?;
        let id = id as u64;
        let index = id as usize;
        let mask = 1 << (index % 8);
        require(seen[index / 8] & mask == 0, "loss SQ8 duplicate")?;
        seen[index / 8] |= mask;
        observe(physical, &record[12..])?;
        if wanted.contains(&id) {
            positions.insert(id, physical);
        }
    }
    require(
        input.read(&mut [0])? == 0
            && format!("{:x}", digest.finalize()) == pin.sha256
            && format!("{:x}", order_digest.finalize()) == order_sha256
            && positions.len() == wanted.len(),
        "loss SQ8 complete SHA/EOF/order/mapping",
    )?;
    let snapshot = HeaderSnapshot {
        file: input.into_inner(),
        path: pin.path.clone(),
        identity,
    };
    snapshot.recheck()?;
    Ok((positions, snapshot))
}

fn loss_bucket(c: &LossCoverage, physical: usize, returned: bool) -> &'static str {
    let source = c.scored_units.binary_search(&(physical / 32)).is_ok();
    let sq8 = c
        .sq8_rows
        .iter()
        .any(|r| r[0] <= physical && physical < r[1]);
    match (returned, sq8, source) {
        (true, true, false) => "bridge_rescued_hit",
        (true, true, true) => "source_covered_hit",
        (true, false, _) => "INVALID_RETURNED_OUTSIDE_SQ8",
        (false, true, _) => "covered_ranking_loss",
        (false, false, true) => "source_to_sq8_nomination_loss",
        (false, false, false) => "outside_source_and_sq8",
    }
}

fn reduce_loss(config_path: &Path, config_sha: &str) -> Result<Value> {
    let (c, config_bytes): (LossConfig, _) = read_config(config_path, config_sha)?;
    require(
        c.schema == "borsuk-sealed-scale-loss-config-v2"
            && c.completed.schema == SCALE_CONFIG_SCHEMA,
        "sealed loss config/schema",
    )?;
    if c.completed.expected_identity["schema"]
        .as_str()
        .is_some_and(current_v10::is_result_schema)
    {
        return Err(
            "historical 256-row analysis only; v10 requires current-format analysis".into(),
        );
    }
    require(
        c.completed.expected_identity["schema"] != "borsuk-cohere-native-baseline-result-v9",
        "historical 256-row analysis only; v9 requires current-format analysis",
    )?;
    let i: Inputs = serde_json::from_value(c.completed.expected_bound_inputs.clone())?;
    validate_scale_inputs(&i, &c.completed.expected_bound_inputs)?;
    let ordinals = scale_ordinals(&c.completed.expected_bound_inputs, i.count)?;
    require(
        c.completed.expected_bound_inputs["execution"]["mode"] == "diagnostic_panel"
            && c.completed.expected_bound_inputs["serving"]["mode"] == "baseline"
            && !ordinals.is_empty()
            && ordinals.len() <= 128
            && i.profile == "scale1m"
            && c.sq8.sha256 == i.native_sq8_sha256
            && c.sq8.bytes == (i.rows as u64) * (i.dimensions as u64 + 12)
            && c.truth.sha256 == i.truth_sha256
            && c.truth.bytes == i.truth_bytes,
        "loss baseline panel/SQ8/truth binding",
    )?;
    // Retained unit lists plus parser/set/report working payload, excluding allocator overhead.
    let modeled = plus(
        (ordinals.len() as u64) * (24576 * 8 + 32 * 16 + 4096),
        i.rows.div_ceil(8) as u64 + i.truth_bytes + 8 * 1024 * 1024,
    )?;
    require(
        modeled <= c.max_payload_bytes && c.max_payload_bytes <= 64 * 1024 * 1024,
        "loss modeled payload admission",
    )?;
    let mut coverage = Vec::with_capacity(ordinals.len());
    let run = read_run_observed(
        &c.completed.input.path,
        &c.completed.input.sha256,
        Some(&c.completed),
        |line, q, inputs| {
            coverage.push(loss_coverage(line, q, inputs)?);
            Ok(())
        },
    )?;
    require(
        coverage.len() == ordinals.len() && run.samples.len() == coverage.len(),
        "loss complete authenticated coverage",
    )?;
    // No truth descriptor is opened until the complete producer body/seal/terminal validates.
    let (truth, truth_snapshot) = loss_binary(&c.truth, (i.count * K * 8) as u64)?;
    let mut truth_rows = Vec::with_capacity(i.count);
    for row in truth.chunks_exact(K * 8) {
        let ids = row
            .chunks_exact(8)
            .map(|w| u64::from_le_bytes(w.try_into().unwrap()))
            .collect::<Vec<_>>();
        require(
            ids.iter().all(|id| *id < i.rows as u64)
                && ids.iter().copied().collect::<BTreeSet<_>>().len() == K,
            "loss truth ordinal uniqueness",
        )?;
        truth_rows.push(ids);
    }
    let mut wanted = BTreeSet::new();
    for (slot, &ordinal) in ordinals.iter().enumerate() {
        wanted.extend(truth_rows[ordinal].iter().copied());
        wanted.extend(run.samples[slot].returned.iter().map(|h| h.id));
    }
    let (positions, sq8_snapshot) = loss_positions(
        &c.sq8,
        i.rows,
        i.dimensions,
        &i.native_order_sha256,
        &wanted,
    )?;
    let mut totals = std::collections::BTreeMap::<&str, u64>::new();
    let mut queries = Vec::with_capacity(coverage.len());
    let mut hits = 0_u64;
    for (slot, cov) in coverage.iter().enumerate() {
        require(cov.ordinal == ordinals[slot], "loss query order")?;
        let sample = &run.samples[slot];
        for h in &sample.returned {
            require(
                loss_bucket(cov, positions[&h.id], true) != "INVALID_RETURNED_OUTSIDE_SQ8",
                "loss returned ID outside actual ranked cover",
            )?;
        }
        let mut entries = Vec::with_capacity(K);
        let mut query_hits = 0;
        for &id in &truth_rows[cov.ordinal] {
            let returned = sample.returned.iter().any(|h| h.id == id);
            query_hits += u64::from(returned);
            let physical = positions[&id];
            let bucket = loss_bucket(cov, physical, returned);
            *totals.entry(bucket).or_default() += 1;
            entries.push(json!({"id":id,"physical_row":physical,"bucket":bucket,
                "source_scored":cov.scored_units.binary_search(&(physical/32)).is_ok(),
                "sq8_ranked":cov.sq8_rows.iter().any(|r|r[0]<=physical&&physical<r[1])}));
        }
        require(query_hits == sample.hits10, "loss original recall parity")?;
        hits = plus(hits, query_hits)?;
        let returned = sample
            .returned
            .iter()
            .map(|hit| json!({"id":hit.id,"score_bits":hit.score_bits}))
            .collect::<Vec<_>>();
        queries.push(json!({"ordinal":cov.ordinal,"hits10":query_hits,"truth_entries":entries,
            "returned":returned,"original_trace_sha256":sample.trace_sha256.iter().map(|b|format!("{b:02x}")).collect::<Vec<_>>().concat(),
            "source_scored_units":cov.scored_units.len(),"sq8_ranked_rows":cov.sq8_rows}));
    }
    require(
        hits == run.terminal.total_hits10
            && totals.values().sum::<u64>() == (coverage.len() * K) as u64,
        "loss disjoint totals/original hits",
    )?;
    sq8_snapshot.recheck()?;
    truth_snapshot.recheck()?;
    require(
        file_identity(&open_input(&c.completed.input.path)?)? == run.file_identity,
        "loss producer path changed before report",
    )?;
    read_config::<LossConfig>(config_path, config_sha)?;
    Ok(
        json!({"schema":LOSS_SCHEMA,"status":"MEASURED","complete":true,
        "config_path":config_path,"config_sha256":config_sha,"config_bytes":config_bytes,
        "producer_result_sha256":c.completed.input.sha256,"sq8_sha256":c.sq8.sha256,
        "source_order_sha256":i.native_order_sha256,"mapping_authenticated_from_ranked_sq8":true,
        "truth_sha256":c.truth.sha256,"query_count":coverage.len(),"hits10":hits,
        "misses10":coverage.len()*K-hits as usize,"totals":totals,"queries":queries,
        "modeled_payload_bytes":modeled,"truth_opened_after_complete_producer_validation":true,
        "native_ann_called":false,"diagnostic_only":true,"cold_s3_claim":false,
        "quantization_attribution":false,"producer_mapping_provenance_external_gate_required":false,
        "vendor_or_scientific_win_claim":false,"qualified":false}),
    )
}

const COVERAGE_SCHEMA: &str = "borsuk-leaf-metric-coverage-v1";
const BOUNDARY_SCHEMA: &str = "borsuk-leaf-boundary-coverage-v1";
const BOUNDARY_CONFIG_SCHEMA: &str = "borsuk-leaf-boundary-coverage-config-v1";
const BOUNDARY_CANDIDATE: &str = "sampled_boundary_v1";
const SELECTION_CAP: u64 = 4 * 1024 * 1024;
const COVERAGE_LEAVES: usize = 48;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CoverageConfig {
    schema: String,
    completed: CompletedConfig,
    generation_manifest: CompletedInput,
    plane_manifest: CompletedInput,
    router_root: CompletedInput,
    membership: CompletedInput,
    requests: CompletedInput,
    sq8: CompletedInput,
    truth: CompletedInput,
    max_payload_bytes: u64,
}

// This is metadata-bound diagnostic admission, not full serving-generation open.
// The root pins an already qualified immutable generation; no payload is loaded here.
fn coverage_router(
    c: &CoverageConfig,
    i: &Inputs,
) -> Result<(
    borsuk::semantic_unit_router::SemanticUnitRouter,
    Vec<HeaderSnapshot>,
)> {
    use borsuk::semantic_unit_router::{SemanticProfile, SemanticUnitRouter, SourceIdentity};
    require(
        c.generation_manifest.sha256 == i.generation_root_sha256,
        "coverage generation binding",
    )?;
    let (g, gs) = header_json(&c.generation_manifest)?;
    fields(
        &g,
        "schema generation base_epoch plane_manifest_sha256 page_manifest_sha256 discovery sq8_object_sha256 sq8_object_key sq8_etag canonical low step",
    )?;
    require(
        g["schema"] == "borsuk-two-bit-generation-v8"
            && g["generation"].as_u64().is_some_and(|n| n > 0)
            && g["base_epoch"].as_u64().is_some()
            && g["plane_manifest_sha256"] == c.plane_manifest.sha256
            && g["sq8_object_sha256"] == i.native_sq8_sha256,
        "coverage generation descriptor",
    )?;
    if c.completed.expected_bound_inputs["backend"]["kind"] == "s3" {
        let backend = &c.completed.expected_bound_inputs["backend"];
        require(
            g["sq8_object_key"] == backend["sq8_object_key"]
                && g["sq8_etag"] == backend["sq8_etag"],
            "coverage serving object binding",
        )?;
    }
    let d = &g["discovery"];
    fields(
        d,
        "mode profile root_sha256 root_bytes membership_sha256 membership_bytes leaves_sha256 leaves_bytes input_schema input_root_sha256 centroids_sha256 source_sha256 source_order_sha256 mean_sha256 records_sha256 sq8_sha256",
    )?;
    require(
        d["mode"] == "semantic"
            && d["profile"] == "scale1m"
            && d["root_sha256"] == c.router_root.sha256
            && d["root_bytes"] == c.router_root.bytes
            && d["membership_sha256"] == c.membership.sha256
            && d["membership_bytes"] == c.membership.bytes,
        "coverage router descriptors",
    )?;
    let (p, ps) = header_json(&c.plane_manifest)?;
    let plane: borsuk::two_bit_source::SourcePlaneReceipt = serde_json::from_value(p)?;
    require(
        plane.schema == "borsuk-two-bit-plane-v3"
            && plane.rows == i.rows
            && plane.dimensions == i.dimensions
            && plane.seed == 20260923
            && plane.page_rows == 32
            && !plane.query_or_truth_used
            && plane.record_bytes == source_record_bytes(i.dimensions)?
            && plane.source_sha256 == i.native_source_sha256
            && plane.sq8_sha256 == i.native_sq8_sha256
            && plane.source_order_sha256 == i.native_order_sha256,
        "coverage plane inputs/geometry",
    )?;
    for (name, sha) in [
        ("source_sha256", &plane.source_sha256),
        ("source_order_sha256", &plane.source_order_sha256),
        ("mean_sha256", &plane.mean_sha256),
        ("records_sha256", &plane.records_sha256),
        ("sq8_sha256", &plane.sq8_sha256),
    ] {
        require(
            valid_sha(sha) && d[name] == *sha,
            "coverage discovery/plane binding",
        )?;
    }
    require(
        valid_sha(&plane.page_digest_sha256),
        "coverage plane digest",
    )?;
    fields(&g["canonical"], "object_key sha256 bytes rows dimensions")?;
    // canonical bytes identify original f32+ID storage, not the normalized-source digest.
    require(
        g["canonical"]["rows"] == i.rows
            && g["canonical"]["dimensions"] == i.dimensions
            && g["canonical"]["bytes"] == (i.rows as u64) * (8 + 4 * i.dimensions as u64)
            && g["canonical"]["sha256"].as_str().is_some_and(valid_sha),
        "coverage canonical geometry",
    )?;
    let low: Vec<f32> = serde_json::from_value(g["low"].clone())?;
    let step: Vec<f32> = serde_json::from_value(g["step"].clone())?;
    require(
        low.len() == i.dimensions
            && step.len() == i.dimensions
            && low.iter().all(|n| n.is_finite())
            && step.iter().all(|n| n.is_finite() && *n > 0.),
        "coverage calibration",
    )?;
    let identity = SourceIdentity {
        profile: SemanticProfile::Scale1m,
        schema: d["input_schema"].as_str().ok_or("coverage input schema")?,
        root_sha256: d["input_root_sha256"]
            .as_str()
            .ok_or("coverage input root")?,
        centroids_sha256: d["centroids_sha256"].as_str().ok_or("coverage centroids")?,
        rows: i.rows,
        dimensions: i.dimensions,
    };
    let (root, rs) = loss_binary(&c.router_root, c.router_root.bytes)?;
    let (membership, ms) = loss_binary(&c.membership, (i.rows.div_ceil(32) * 4) as u64)?;
    let router = SemanticUnitRouter::open(
        &root,
        &membership,
        &c.router_root.sha256,
        &identity,
        SemanticProfile::Scale1m.root_cap(),
    )?;
    require(
        d["leaves_bytes"] == i.rows.div_ceil(32) * (4 + 2 * i.dimensions)
            && router.manifest().leaf_payload.sha256 == d["leaves_sha256"]
            && router.manifest().leaf_payload.bytes as u64
                == d["leaves_bytes"].as_u64().ok_or("coverage leaf bytes")?,
        "coverage leaf descriptor",
    )?;
    Ok((router, vec![gs, ps, rs, ms]))
}

struct CoverageArm {
    nomination: borsuk::semantic_unit_router::Nomination,
    ranges: Vec<std::ops::Range<usize>>,
    bytes: usize,
}

fn coverage_arm(
    router: &borsuk::semantic_unit_router::SemanticUnitRouter,
    query: &[f32],
    metric: &borsuk::VectorMetric,
    rows: usize,
    dimensions: usize,
) -> Result<CoverageArm> {
    let ranked = router.rank_leaves_by_metric(query, metric)?;
    require(
        ranked.len() >= COVERAGE_LEAVES,
        "coverage needs exactly 48 leaves",
    )?;
    coverage_selected_arm(router, &ranked[..COVERAGE_LEAVES], rows, dimensions)
}

fn coverage_selected_arm(
    router: &borsuk::semantic_unit_router::SemanticUnitRouter,
    leaves: &[usize],
    rows: usize,
    dimensions: usize,
) -> Result<CoverageArm> {
    require(
        leaves.len() == COVERAGE_LEAVES,
        "coverage needs exactly 48 leaves",
    )?;
    let nomination = router.nominate_selected(leaves)?;
    let (ranges, bytes) = borsuk::two_bit_generation::plan_two_bit_source_cover(
        &nomination.page_closure,
        rows,
        source_record_bytes(dimensions)?,
        128,
        64 * 1024 * 1024,
    )?;
    Ok(CoverageArm {
        nomination,
        ranges,
        bytes,
    })
}

struct BoundaryEvidence {
    adjacency: borsuk::semantic_unit_router::SampledBoundaryAdjacency,
    snapshots: Vec<HeaderSnapshot>,
    sha256: String,
}

fn boundary_samples(
    c: &CoverageConfig,
    i: &Inputs,
    low: &[f32],
    step: &[f32],
) -> Result<(Vec<Vec<f32>>, HeaderSnapshot)> {
    use borsuk::semantic_unit_router::{decode_sampled_boundary_row, sampled_boundary_position};
    let mut samples = Vec::with_capacity(i.rows.div_ceil(32));
    let (_, snapshot) = scan_loss_sq8(
        &c.sq8,
        i.rows,
        i.dimensions,
        &i.native_order_sha256,
        &BTreeSet::new(),
        |physical, codes| {
            if physical == sampled_boundary_position(i.rows, physical / 32)? {
                require(
                    samples.len() == physical / 32,
                    "boundary physical sample order",
                )?;
                samples.push(decode_sampled_boundary_row(codes, low, step)?);
            }
            Ok(())
        },
    )?;
    require(
        samples.len() == i.rows.div_ceil(32),
        "boundary complete samples",
    )?;
    Ok((samples, snapshot))
}

// The caller has admitted both live phases before opening metadata. No requests
// or truth are opened until this source-only artifact has synced and read back.
fn build_boundary_evidence(
    c: &CoverageConfig,
    i: &Inputs,
    router: &borsuk::semantic_unit_router::SemanticUnitRouter,
    output: &Path,
    inputs: &[&HeaderSnapshot],
    model: &borsuk::semantic_unit_router::SampledBoundaryAllocation,
    sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<BoundaryEvidence> {
    use borsuk::semantic_unit_router::SampledBoundaryAdjacency;
    let (generation, generation_snapshot) = header_json(&c.generation_manifest)?;
    let low: Vec<f32> = serde_json::from_value(generation["low"].clone())?;
    let step: Vec<f32> = serde_json::from_value(generation["step"].clone())?;
    let calibration = json!({"low":low,"step":step});
    let calibration_sha = format!("{:x}", Sha256::digest(serde_json::to_vec(&calibration)?));
    drop(calibration);
    drop(generation);
    let (samples, sq8_snapshot) = boundary_samples(c, i, &low, &step)?;
    let adjacency = SampledBoundaryAdjacency::build(
        &samples,
        router.unit_membership(),
        router.manifest().leaves.len(),
        usize::try_from(c.max_payload_bytes)?,
    )?;
    drop(samples); // The graph was dropped by build; neither overlaps paired replay.
    let mut references = inputs.to_vec();
    references.extend([&generation_snapshot, &sq8_snapshot]);
    let sidecar = json!({"schema":"borsuk-sampled-boundary-adjacency-v1","status":"SEALED",
        "candidate":BOUNDARY_CANDIDATE,"generation_sha256":c.generation_manifest.sha256,
        "calibration_sha256":calibration_sha,"plane_sha256":c.plane_manifest.sha256,
        "router_sha256":c.router_root.sha256,"membership_sha256":c.membership.sha256,
        "sq8_sha256":c.sq8.sha256,"sq8_bytes":c.sq8.bytes,"order_sha256":i.native_order_sha256,
        "source_sha256":i.native_source_sha256,"rows":i.rows,"dimensions":i.dimensions,
        "leaf_count":router.manifest().leaves.len(),
        "unit_rows":32,"sample_rule":"min(32*u+16,rows-1)","sample_labels":"physical_unit_membership",
        "normalization":"normalize_two_bit_diagnostic_query",
        "algorithm":"sequential-nondiverse-retained-directed-base-v1",
        "m":16,"m0":32,"ef_construction":64,"ef_search":64,"destinations_per_leaf":16,
        "edge_layer":"tower.last()","edge_order":"count_desc_leaf_id_asc",
        "seeds":40,"replacements":8,"vote_weights":"40-rank; rank=0..39",
        "vote_order":"votes_desc_l2_rank_asc_leaf_id_asc","fallback":"remaining_l2",
        "biases":["fixed_midpoint","leaf_size","destination_popularity","approximate_discovery"],
        "implementation_sha256":format!("{:x}",Sha256::digest(include_bytes!("compare_native_replay.rs"))),
        "router_implementation_sha256":format!("{:x}",Sha256::digest(include_bytes!("../src/semantic_unit_router.rs"))),
        "graph_implementation_sha256":format!("{:x}",Sha256::digest(include_bytes!("../src/centroid_hnsw.rs"))),
        "allocation_model":model,"requests_opened":false,"truth_opened":false,
        "serving_publication_authority":false,"exact_knn":false,"adjacency":adjacency});
    let (snapshot, sha256) = publish_selection_seal_with_sync(output, &sidecar, &references, sync)?;
    Ok(BoundaryEvidence {
        adjacency,
        snapshots: vec![generation_snapshot, sq8_snapshot, snapshot],
        sha256,
    })
}

fn coverage_arm_value(a: &CoverageArm) -> Value {
    let n = &a.nomination;
    json!({"leaves":n.leaf_ids,"units":n.units,"seed_page":n.seed_page,
        "walk_units":n.walk_units,"seed_additions":n.seed_additions,"original_pages":n.page_closure,
        "cover_ranges":a.ranges.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>(),
        "planned_gets":a.ranges.len(),"planned_bytes":a.bytes})
}

fn coverage_control(line: &[u8], q: &Query, i: &Inputs, arm: &CoverageArm) -> Result<LossCoverage> {
    let cov = loss_coverage(line, q, i)?;
    let raw: Value = serde_json::from_slice(line)?;
    let t: SourceTrace = serde_json::from_value(raw["trace"].clone())?;
    let n = &arm.nomination;
    require(
        t.semantic_leaves == n.leaf_ids
            && t.semantic_units == n.units.iter().copied().collect::<Vec<_>>()
            && t.semantic_seed_additions == n.seed_additions,
        "coverage exact historical L2 control parity",
    )?;
    // primary_page is chosen after SOURCE scoring, not the lowest semantic seed page.
    Ok(cov)
}

fn publish_selection_seal(
    output: &Path,
    value: &Value,
    inputs: &[&HeaderSnapshot],
) -> Result<(HeaderSnapshot, String)> {
    publish_selection_seal_with_sync(output, value, inputs, |file, _| file.sync_all())
}

fn publish_selection_seal_with_sync(
    output: &Path,
    value: &Value,
    inputs: &[&HeaderSnapshot],
    mut sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<(HeaderSnapshot, String)> {
    header_path(output)?;
    for input in inputs {
        input.recheck()?;
    }
    let dir = parent(output)?;
    let file = File::from(openat(
        &dir,
        output.file_name().ok_or("selection filename")?,
        OFlags::WRONLY
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    finish_selection_publication(output, value, inputs, dir, file, &mut sync)
}

fn finish_selection_publication(
    output: &Path,
    value: &Value,
    inputs: &[&HeaderSnapshot],
    dir: File,
    file: File,
    sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<(HeaderSnapshot, String)> {
    let cleanup_dir = dir.try_clone()?;
    let mut owned = file.try_clone()?;
    let result = finish_selection_publication_inner(output, value, inputs, dir, file, sync);
    match result {
        Ok(published) => Ok(published),
        Err(error) => {
            // Invalidate through the original descriptor: a concurrent path replacement
            // cannot redirect this write, and there is no check-then-unlink race.
            let cleanup = (|| -> Result<()> {
                owned.set_len(0)?;
                owned.seek(io::SeekFrom::Start(0))?;
                owned.write_all(
                    b"{\"status\":\"INVALID\",\"complete\":false,\"publication_failed\":true}\n",
                )?;
                owned.sync_all()?;
                cleanup_dir.sync_all()?;
                Ok(())
            })();
            match cleanup {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(format!(
                    "publication failed: {error}; owned output invalidation failed: {cleanup_error}"
                )
                .into()),
            }
        }
    }
}

fn finish_selection_publication_inner(
    output: &Path,
    value: &Value,
    inputs: &[&HeaderSnapshot],
    dir: File,
    mut file: File,
    mut sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<(HeaderSnapshot, String)> {
    let mut expected = serde_json::to_vec(value)?;
    expected.push(b'\n');
    require(
        expected.len() as u64 <= SELECTION_CAP,
        "paired selection seal cap",
    )?;
    let parent_identity = dir.metadata()?;
    let created = file_identity(&file)?;
    file.write_all(&expected)?;
    sync(&file, false)?;
    let mut readback = header_file(output)?;
    let published = file_identity(&readback)?;
    require(
        published.dev == created.dev
            && published.ino == created.ino
            && published.len == expected.len() as u64
            && file_identity(&file)? == published,
        "selection descriptor identity",
    )?;
    let mut actual = Vec::with_capacity(expected.len());
    Read::take(&mut readback, SELECTION_CAP + 1).read_to_end(&mut actual)?;
    require(actual == expected, "selection exact readback")?;
    for input in inputs {
        input.recheck()?;
    }
    sync(&dir, true)?;
    let snapshot = HeaderSnapshot {
        file: readback,
        path: output.to_owned(),
        identity: published,
    };
    snapshot.recheck()?;
    require(
        file.metadata()?.nlink() == 1 && file_identity(&file)? == snapshot.identity,
        "selection final write descriptor",
    )?;
    let current_parent = parent(output)?.metadata()?;
    require(
        parent_identity.dev() == current_parent.dev()
            && parent_identity.ino() == current_parent.ino(),
        "selection parent identity",
    )?;
    for input in inputs {
        input.recheck()?;
    }
    Ok((snapshot, format!("{:x}", Sha256::digest(&expected))))
}

fn coverage_slot(a: &CoverageArm, physical: usize, width: usize) -> (bool, bool) {
    (
        a.nomination.page_closure.contains(&(physical / 256)),
        a.ranges.iter().any(|r| r.contains(&(physical * width))),
    )
}

fn coverage_survives(control: usize, candidate: usize, gained: usize, lost: usize) -> bool {
    control == 314 && candidate >= 315 && candidate + lost == control + gained && gained > lost
}

fn reduce_leaf_coverage(
    config_path: &Path,
    config_sha: &str,
    selection_path: &Path,
) -> Result<(Value, Vec<HeaderSnapshot>)> {
    reduce_leaf_coverage_with(
        config_path,
        config_sha,
        selection_path,
        &CoverageScope::consumed32(),
        |file, _| file.sync_all(),
        loss_binary,
    )
}

struct CoverageScope {
    rows: usize,
    count: usize,
    ordinals: Vec<usize>,
    original_hits: u64,
    original_coverage: usize,
}

impl CoverageScope {
    fn consumed32() -> Self {
        Self {
            rows: 1_000_000,
            count: 1000,
            ordinals: vec![
                69, 83, 114, 149, 151, 159, 188, 189, 211, 237, 257, 296, 321, 351, 388, 408, 413,
                513, 537, 587, 626, 667, 669, 731, 742, 821, 823, 849, 851, 890, 956, 991,
            ],
            original_hits: 313,
            original_coverage: 314,
        }
    }
}

// Only the fixed consumed32 scope is reachable from the CLI. Small native fixtures
// exercise this same orchestration without a population-sized synthetic payload.
fn reduce_leaf_coverage_with(
    config_path: &Path,
    config_sha: &str,
    selection_path: &Path,
    scope: &CoverageScope,
    seal_sync: impl FnMut(&File, bool) -> io::Result<()>,
    open_truth: impl FnMut(&CompletedInput, u64) -> Result<(Vec<u8>, HeaderSnapshot)>,
) -> Result<(Value, Vec<HeaderSnapshot>)> {
    reduce_leaf_coverage_candidate_with(
        config_path,
        config_sha,
        selection_path,
        None,
        scope,
        seal_sync,
        open_truth,
    )
}

fn reduce_leaf_coverage_candidate_with(
    config_path: &Path,
    config_sha: &str,
    selection_path: &Path,
    boundary_path: Option<&Path>,
    scope: &CoverageScope,
    mut seal_sync: impl FnMut(&File, bool) -> io::Result<()>,
    mut open_truth: impl FnMut(&CompletedInput, u64) -> Result<(Vec<u8>, HeaderSnapshot)>,
) -> Result<(Value, Vec<HeaderSnapshot>)> {
    let boundary_mode = boundary_path.is_some();
    let candidate_label = if boundary_mode {
        BOUNDARY_CANDIDATE
    } else {
        "cosine"
    };
    let (c, config_bytes): (CoverageConfig, _) = read_config(config_path, config_sha)?;
    require(
        c.schema
            == if boundary_mode {
                BOUNDARY_CONFIG_SCHEMA
            } else {
                "borsuk-leaf-metric-coverage-config-v1"
            }
            && c.completed.schema == SCALE_CONFIG_SCHEMA,
        "coverage config/schema",
    )?;
    let config_file = header_file(config_path)?;
    let config_snapshot = HeaderSnapshot {
        identity: file_identity(&config_file)?,
        file: config_file,
        path: config_path.to_owned(),
    };
    read_config::<CoverageConfig>(config_path, config_sha)?;
    let i: Inputs = serde_json::from_value(c.completed.expected_bound_inputs.clone())?;
    validate_scale_inputs(&i, &c.completed.expected_bound_inputs)?;
    require(
        c.completed
            .expected_bound_inputs
            .get("source_get_budget")
            .is_none_or(|v| v.as_u64() == Some(128)),
        "coverage requires frozen SOURCE128 control",
    )?;
    let ordinals = scale_ordinals(&c.completed.expected_bound_inputs, i.count)?;
    require(
        i.rows == scope.rows
            && i.count == scope.count
            && i.profile == "scale1m"
            && c.completed.expected_bound_inputs["execution"]["mode"] == "diagnostic_panel"
            && ordinals == scope.ordinals
            && c.requests.sha256 == i.requests_sha256
            && c.requests.bytes == i.requests_bytes
            && c.truth.sha256 == i.truth_sha256
            && c.truth.bytes == i.truth_bytes
            && c.sq8.sha256 == i.native_sq8_sha256
            && c.sq8.bytes == i.rows as u64 * (i.dimensions as u64 + 12),
        "coverage fixed consumed panel/input binding",
    )?;
    // Covers router construction copies, both retained nominations, producer parsing,
    // requests/truth, SQ8 streaming scratch and bounded seal serialization/readback.
    // Allocator/runtime overhead is measured separately by the remote resource gate.
    let modeled = plus(
        c.router_root
            .bytes
            .checked_mul(32)
            .ok_or("coverage model overflow")?,
        64 * 1024 * 1024,
    )?;
    require(
        c.router_root.bytes <= 4 * 1024 * 1024
            && c.membership.bytes == i.rows.div_ceil(32) as u64 * 4
            && modeled <= c.max_payload_bytes
            && c.max_payload_bytes
                <= if boundary_mode {
                    512 * 1024 * 1024
                } else {
                    256 * 1024 * 1024
                },
        "coverage modeled payload admission",
    )?;
    let boundary_model = if boundary_mode {
        // The binary root's exact width bounds leaf count before decoding it.
        let leaves = usize::try_from(c.router_root.bytes)?
            .checked_sub(512)
            .ok_or("boundary root header")?
            / (64 + 4 * i.dimensions);
        Some(borsuk::semantic_unit_router::sampled_boundary_allocation(
            i.rows,
            i.dimensions,
            leaves,
            usize::try_from(plus(
                c.router_root
                    .bytes
                    .checked_mul(32)
                    .ok_or("boundary metadata model")?,
                16 * 1024 * 1024,
            )?)?,
            usize::try_from(modeled)?,
            usize::try_from(c.max_payload_bytes)?,
        )?)
    } else {
        None
    };
    let (router, metadata_snapshots) = coverage_router(&c, &i)?;
    let boundary = if let Some(path) = boundary_path {
        let mut inputs = metadata_snapshots.iter().collect::<Vec<_>>();
        inputs.push(&config_snapshot);
        Some(build_boundary_evidence(
            &c,
            &i,
            &router,
            path,
            &inputs,
            boundary_model.as_ref().ok_or("boundary allocation model")?,
            &mut seal_sync,
        )?)
    } else {
        None
    };
    let (requests, requests_snapshot) = loss_binary(&c.requests, i.requests_bytes)?;
    let mut arms = Vec::with_capacity(ordinals.len());
    for &ordinal in &ordinals {
        let width = i.dimensions * 4;
        let raw = requests[ordinal * width..(ordinal + 1) * width]
            .chunks_exact(4)
            .map(|w| f32::from_le_bytes(w.try_into().unwrap()))
            .collect::<Vec<_>>();
        let normalized = borsuk::two_bit_generation::normalize_two_bit_diagnostic_query(&raw)?;
        arms.push([
            coverage_arm(
                &router,
                &normalized,
                &borsuk::VectorMetric::SquaredEuclidean,
                i.rows,
                i.dimensions,
            )?,
            if let Some(evidence) = &boundary {
                let ranked = router
                    .rank_leaves_by_metric(&normalized, &borsuk::VectorMetric::SquaredEuclidean)?;
                coverage_selected_arm(
                    &router,
                    &evidence.adjacency.select(&ranked)?,
                    i.rows,
                    i.dimensions,
                )?
            } else {
                coverage_arm(
                    &router,
                    &normalized,
                    &borsuk::VectorMetric::Cosine,
                    i.rows,
                    i.dimensions,
                )?
            },
        ]);
    }
    let producer_file = header_file(&c.completed.input.path)?;
    let producer_snapshot = HeaderSnapshot {
        identity: file_identity(&producer_file)?,
        file: producer_file,
        path: c.completed.input.path.clone(),
    };
    let mut slot = 0;
    let mut control_coverage = Vec::with_capacity(ordinals.len());
    let run = read_run_observed(
        &c.completed.input.path,
        &c.completed.input.sha256,
        Some(&c.completed),
        |line, q, inputs| {
            require(
                slot < ordinals.len() && q.ordinal == ordinals[slot],
                "coverage producer panel order",
            )?;
            control_coverage.push(coverage_control(line, q, inputs, &arms[slot][0])?);
            slot += 1;
            Ok(())
        },
    )?;
    require(
        slot == ordinals.len()
            && run.samples.len() == slot
            && run.file_identity == producer_snapshot.identity,
        "coverage complete producer parity",
    )?;
    let mut snapshots = metadata_snapshots.iter().collect::<Vec<_>>();
    snapshots.extend([&config_snapshot, &requests_snapshot, &producer_snapshot]);
    if let Some(evidence) = &boundary {
        snapshots.extend(evidence.snapshots.iter());
    }
    let selections = arms
        .iter()
        .enumerate()
        .map(|(s, a)| {
            json!({"ordinal":ordinals[s],
        "l2":coverage_arm_value(&a[0]),(candidate_label):coverage_arm_value(&a[1])})
        })
        .collect::<Vec<_>>();
    let mut seal = json!({"schema":if boundary_mode { "borsuk-leaf-boundary-selection-seal-v1" } else { "borsuk-leaf-metric-selection-seal-v1" },"status":"SEALED",
        "config_sha256":config_sha,"implementation_sha256":format!("{:x}",Sha256::digest(include_bytes!("compare_native_replay.rs"))),
        "router_implementation_sha256":format!("{:x}",Sha256::digest(include_bytes!("../src/semantic_unit_router.rs"))),
        "generation_implementation_sha256":format!("{:x}",Sha256::digest(include_bytes!("../src/two_bit_generation.rs"))),
        "producer_sha256":c.completed.input.sha256,"generation_sha256":c.generation_manifest.sha256,
        "plane_sha256":c.plane_manifest.sha256,"router_sha256":c.router_root.sha256,
        "membership_sha256":c.membership.sha256,"requests_sha256":c.requests.sha256,
        "truth_sha256":c.truth.sha256,"sq8_sha256":c.sq8.sha256,"order_sha256":i.native_order_sha256,
        "source_sha256":i.native_source_sha256,"normalization":"normalize_two_bit_diagnostic_query",
        "leaf_count":48,"source_get_cap":128,"source_byte_cap":64*1024*1024,
        "truth_opened":false,"control_trace_parity":true,"selections":selections});
    if let Some(evidence) = &boundary {
        seal["candidate"] = json!(BOUNDARY_CANDIDATE);
        seal["adjacency_sha256"] = json!(evidence.sha256);
        seal["adjacency_path"] = json!(boundary_path);
        seal["adjacency_sealed_before_requests"] = json!(true);
    }
    let (selection_snapshot, selection_sha) =
        publish_selection_seal_with_sync(selection_path, &seal, &snapshots, &mut seal_sync)?;
    // FIRST truth open: both complete arm selections have passed file and directory sync.
    let (truth, truth_snapshot) = open_truth(&c.truth, i.truth_bytes)?;
    let mut truth_rows = Vec::with_capacity(ordinals.len());
    let mut wanted = BTreeSet::new();
    wanted.extend(
        run.samples
            .iter()
            .flat_map(|s| s.returned.iter().map(|h| h.id)),
    );
    for &ordinal in &ordinals {
        let ids = truth[ordinal * K * 8..(ordinal + 1) * K * 8]
            .chunks_exact(8)
            .map(|w| u64::from_le_bytes(w.try_into().unwrap()))
            .collect::<Vec<_>>();
        require(
            ids.iter().all(|id| *id < i.rows as u64)
                && ids.iter().copied().collect::<BTreeSet<_>>().len() == K,
            "coverage truth IDs",
        )?;
        wanted.extend(ids.iter().copied());
        truth_rows.push(ids);
    }
    let (positions, sq8_snapshot) = loss_positions(
        &c.sq8,
        i.rows,
        i.dimensions,
        &i.native_order_sha256,
        &wanted,
    )?;
    let mut totals = [0usize; 2];
    let mut original_hits = 0_u64;
    let record_width = source_record_bytes(i.dimensions)?;
    let (mut gained, mut lost) = (0usize, 0usize);
    let mut queries = Vec::with_capacity(slot);
    for (s, ids) in truth_rows.iter().enumerate() {
        let sample = &run.samples[s];
        for hit in &sample.returned {
            require(
                loss_bucket(&control_coverage[s], positions[&hit.id], true)
                    != "INVALID_RETURNED_OUTSIDE_SQ8",
                "coverage returned ID outside fetched SQ8 cover",
            )?;
        }
        let hits = ids
            .iter()
            .filter(|id| sample.returned.iter().any(|h| h.id == **id))
            .count();
        require(
            hits as u64 == sample.hits10,
            "coverage original truth/recall parity",
        )?;
        original_hits = plus(original_hits, hits as u64)?;
        let mut entries = Vec::with_capacity(K);
        for id in ids {
            let physical = positions[id];
            let state = arms[s]
                .each_ref()
                .map(|a| coverage_slot(a, physical, record_width));
            for a in 0..2 {
                totals[a] += usize::from(state[a].0);
            }
            gained += usize::from(!state[0].0 && state[1].0);
            lost += usize::from(state[0].0 && !state[1].0);
            entries.push(json!({"id":id,"physical_row":physical,
                "l2_scored":state[0].0,(format!("{candidate_label}_scored")):state[1].0,
                "l2_fetched":state[0].1,(format!("{candidate_label}_fetched")):state[1].1,
                "l2_bridge_only":state[0].1&&!state[0].0,(format!("{candidate_label}_bridge_only")):state[1].1&&!state[1].0}));
        }
        queries.push(json!({"ordinal":ordinals[s],"truth_slots":entries}));
    }
    require(
        original_hits == scope.original_hits
            && original_hits == run.terminal.total_hits10
            && totals[0] == scope.original_coverage
            && totals[1] + lost == totals[0] + gained,
        "coverage historical total/paired decomposition",
    )?;
    for snapshot in snapshots {
        snapshot.recheck()?;
    }
    selection_snapshot.recheck()?;
    sq8_snapshot.recheck()?;
    truth_snapshot.recheck()?;
    read_config::<CoverageConfig>(config_path, config_sha)?;
    let mut report = json!({"schema":if boundary_mode { BOUNDARY_SCHEMA } else { COVERAGE_SCHEMA },"status":"MEASURED","complete":true,
        "config_sha256":config_sha,"config_bytes":config_bytes,"selection_seal_sha256":selection_sha,
        "selection_seal_bytes":selection_snapshot.identity.len,"selection_seal_path":selection_path,
        "l2_coverage":totals[0],(format!("{candidate_label}_coverage")):totals[1],"truth_slots":slot*K,
        "gained_slots":gained,"lost_slots":lost,"queries":queries,
        "coverage_gate_passed":coverage_survives(totals[0],totals[1],gained,lost),
        "original_returned_hits10":run.terminal.total_hits10,"original_returned_gate":"FAIL",
        "modeled_payload_bytes":modeled,"truth_opened_after_durable_paired_selection":true,
        "consumed_panel":true,"planner_only":true,"source_payload_read":false,"native_ann_called":false,
        "cold_s3_claim":false,"vendor_or_scientific_win_claim":false,"qualified":false});
    let mut retained = metadata_snapshots;
    if let Some(evidence) = boundary {
        report["candidate"] = json!(BOUNDARY_CANDIDATE);
        report["adjacency_sha256"] = json!(evidence.sha256);
        report["adjacency_path"] = json!(boundary_path);
        report["adjacency_sealed_before_requests"] = json!(true);
        report["allocation_model"] = json!(boundary_model);
        report["modeled_payload_bytes"] = json!(
            boundary_model
                .as_ref()
                .ok_or("boundary allocation model")?
                .peak
        );
        retained.extend(evidence.snapshots);
    }
    retained.extend([
        config_snapshot,
        requests_snapshot,
        producer_snapshot,
        selection_snapshot,
        sq8_snapshot,
        truth_snapshot,
    ]);
    Ok((report, retained))
}

fn execute_leaf_coverage(config: &Path, sha: &str, seal: &Path, output: &Path) -> Result<bool> {
    execute_leaf_coverage_with(
        output,
        || reduce_leaf_coverage(config, sha, seal),
        |file, _| file.sync_all(),
    )
}

fn execute_leaf_coverage_with(
    output: &Path,
    reduce: impl FnOnce() -> Result<(Value, Vec<HeaderSnapshot>)>,
    sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<bool> {
    execute_leaf_coverage_as(output, COVERAGE_SCHEMA, reduce, sync)
}

fn execute_leaf_coverage_as(
    output: &Path,
    schema: &str,
    reduce: impl FnOnce() -> Result<(Value, Vec<HeaderSnapshot>)>,
    sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<bool> {
    // Reserve the report before opening any input. Keep its containing directory live.
    header_path(output)?;
    let dir = parent(output)?;
    let file = File::from(openat(
        &dir,
        output.file_name().ok_or("coverage report filename")?,
        OFlags::WRONLY
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    let (report, retained) = match reduce() {
        Ok(reduced) => reduced,
        Err(e) => (
            json!({"schema":schema,"status":"INVALID","complete":false,
            "error":e.to_string().chars().take(512).collect::<String>(),"qualified":false}),
            vec![],
        ),
    };
    require(
        serde_json::to_vec(&report)?.len() as u64 + 1 <= REPORT_CAP,
        "coverage report cap",
    )?;
    let references = retained.iter().collect::<Vec<_>>();
    finish_selection_publication(output, &report, &references, dir, file, sync)?;
    Ok(report["status"] == "MEASURED")
}

const NEIGHBORHOOD_SCHEMA: &str = "borsuk-source-neighborhood-v1";
const NEIGHBORHOOD_CONFIG_SCHEMA: &str = "borsuk-source-neighborhood-config-v1";
const NEIGHBORHOOD_UNIT_DOMAIN: &[u8] = b"borsuk-source-neighborhood-unit-v1\0";
const NEIGHBORHOOD_ROW_DOMAIN: &[u8] = b"borsuk-source-neighborhood-row-v1\0";
const NEIGHBORHOOD_ARITHMETIC: &str = "ascending-f64-actual-norm-cosine-clamp-id-ties-v1";
const NEIGHBORHOOD_IO: usize = 65_536;
const NEIGHBORHOOD_SUMMARY_CAP: usize = 16 * 1024;

#[derive(Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum NeighborhoodScope {
    Scientific,
    SmallFixture,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NeighborhoodConfig {
    schema: String,
    scope: NeighborhoodScope,
    rows: usize,
    dimensions: usize,
    anchor_count: usize,
    neighbor_count: usize,
    generation_manifest: CompletedInput,
    plane_manifest: CompletedInput,
    router_root: CompletedInput,
    membership: CompletedInput,
    leaves: CompletedInput,
    canonical: CompletedInput,
    canonical_object_key: String,
    max_payload_bytes: u64,
    anchor_seal: PathBuf,
    report: PathBuf,
}

fn neighborhood_product(values: &[u64]) -> Result<u64> {
    values.iter().try_fold(1_u64, |n, &v| {
        n.checked_mul(v)
            .ok_or_else(|| "neighborhood allocation overflow".into())
    })
}

fn neighborhood_sum(values: &[u64]) -> Result<u64> {
    values.iter().try_fold(0, |n, &v| plus(n, v))
}

#[derive(Serialize)]
struct NeighborhoodAllocation {
    components: std::collections::BTreeMap<&'static str, u64>,
    phases: std::collections::BTreeMap<&'static str, u64>,
    peak_payload_bytes: u64,
    runtime_allowance_bytes: u64,
    charged_file_cache_allowance_bytes: u64,
    proposed_charged_bytes: u64,
    staged_input_bytes: u64,
    maximum_evidence_bytes: u64,
    staged_peak_bytes: u64,
}

// This model is specific to the lifetimes below. Cache/runtime allowances still
// require the root's charged-memory smoke; arithmetic admission is not RSS proof.
fn neighborhood_allocation(
    c: &NeighborhoodConfig,
    config_bytes: u64,
) -> Result<NeighborhoodAllocation> {
    use std::collections::BTreeMap;
    let n = u64::try_from(c.rows)?;
    let d = u64::try_from(c.dimensions)?;
    let a = u64::try_from(c.anchor_count)?;
    let k = u64::try_from(c.neighbor_count)?;
    let canonical_width = plus(8, neighborhood_product(&[4, d])?)?;
    let leaf_width = plus(4, neighborhood_product(&[2, d])?)?;
    let canonical_bytes = neighborhood_product(&[n, canonical_width])?;
    let units = n.div_ceil(32);
    let leaf_slot = plus(64, neighborhood_product(&[4, d])?)?;
    let leaf_bytes = c
        .router_root
        .bytes
        .checked_sub(512)
        .ok_or("neighborhood root header")?;
    let leaves = leaf_bytes / leaf_slot;
    require(
        c.schema == NEIGHBORHOOD_CONFIG_SCHEMA
            && (1..=1_000_000).contains(&c.rows)
            && (1..=1024).contains(&c.dimensions)
            && (1..=32).contains(&c.anchor_count)
            && (1..=32).contains(&c.neighbor_count)
            && c.rows > c.neighbor_count
            && a <= units
            && config_bytes > 0
            && config_bytes <= CONFIG_CAP
            && c.router_root.bytes <= 4 * 1024 * 1024
            && leaf_bytes % leaf_slot == 0
            && (1..=2 * units.div_ceil(64) - 1).contains(&leaves)
            && c.membership.bytes == neighborhood_product(&[units, 4])?
            && c.leaves.bytes == neighborhood_product(&[units, leaf_width])?
            && c.canonical.bytes == canonical_bytes
            && (c.scope == NeighborhoodScope::SmallFixture || (a == 32 && k == 32 && leaves >= 48)),
        "neighborhood schema/counts/geometry",
    )?;
    for pin in [&c.generation_manifest, &c.plane_manifest] {
        header_pin(pin, HEADER_JSON_CAP)?;
    }
    for pin in [&c.router_root, &c.membership, &c.leaves, &c.canonical] {
        header_pin(pin, pin.bytes)?;
    }
    let paths = [
        &c.generation_manifest.path,
        &c.plane_manifest.path,
        &c.router_root.path,
        &c.membership.path,
        &c.leaves.path,
        &c.canonical.path,
        &c.anchor_seal,
        &c.report,
    ];
    let mut distinct = BTreeSet::new();
    for path in paths {
        header_path(path)?;
        require(
            distinct.insert(path),
            "neighborhood distinct input/output paths",
        )?;
    }
    // Capacities, not just logical elements: 64x JSON covers UniqueJson trees,
    // conversion/clones and bounded strings; 4x root covers encoded/decoded copies.
    let metadata = neighborhood_product(&[
        64,
        neighborhood_sum(&[
            CONFIG_CAP,
            c.generation_manifest.bytes,
            c.plane_manifest.bytes,
        ])?,
    ])?;
    let router = neighborhood_sum(&[
        neighborhood_product(&[4, c.router_root.bytes])?,
        neighborhood_product(&[32, units])?,
        neighborhood_product(&[128, leaves])?,
    ])?;
    let snapshots = 16 * (4096 + 512);
    let selected = neighborhood_product(&[
        a,
        plus(neighborhood_product(&[d, 32 * 4 + 4 + 4 + 8])?, 512)?,
    ])?;
    let heaps = neighborhood_product(&[
        a,
        plus(
            neighborhood_product(&[k, std::mem::size_of::<NeighborhoodNeighbor>() as u64])?,
            64,
        )?,
    ])?;
    let scan = neighborhood_sum(&[
        NEIGHBORHOOD_IO as u64,
        canonical_width,
        neighborhood_product(&[d, 4])?,
        n.div_ceil(8),
    ])?;
    let leaf_scan = neighborhood_sum(&[
        NEIGHBORHOOD_IO as u64,
        neighborhood_product(&[64, leaf_width])?,
        neighborhood_product(&[d, 12])?,
        units.div_ceil(8),
        16 * 1024,
    ])?;
    // One ranking/nomination at a time: BTree capacities, walk units, ranks,
    // destination maps, leaf IDs and selected page sets, including return copies.
    let ranking = neighborhood_sum(&[
        neighborhood_product(&[units, 128])?,
        neighborhood_product(&[leaves, 128])?,
        64 * 1024,
    ])?;
    // Value trees + growing encoded Vec + readback coexist in the reused writer.
    let seal_publication = neighborhood_product(&[68, SELECTION_CAP])?;
    let report_publication = neighborhood_product(&[68, REPORT_CAP])?;
    let human_summary = neighborhood_product(&[4, NEIGHBORHOOD_SUMMARY_CAP as u64])?;
    let allocator = 32 * 1024 * 1024;
    let common = neighborhood_sum(&[metadata, router, snapshots, allocator])?;
    let retained = neighborhood_sum(&[common, selected, heaps])?;
    let phases = BTreeMap::from([
        ("metadata_authentication", common),
        (
            "leaf_authentication",
            neighborhood_sum(&[retained, leaf_scan])?,
        ),
        ("canonical_authentication", plus(retained, scan)?),
        ("anchor_seal_publication", plus(retained, seal_publication)?),
        ("scalar_neighborhood_scan", plus(retained, scan)?),
        (
            "residuals_and_report",
            neighborhood_sum(&[retained, ranking, report_publication, human_summary])?,
        ),
        (
            "report_publication",
            neighborhood_sum(&[retained, ranking, report_publication, human_summary])?,
        ),
    ]);
    let peak_payload_bytes = *phases.values().max().ok_or("neighborhood phase table")?;
    require(
        peak_payload_bytes <= c.max_payload_bytes,
        "neighborhood phase payload cap before body open",
    )?;
    let runtime_allowance_bytes = 64 * 1024 * 1024;
    let charged_file_cache_allowance_bytes = 256 * 1024 * 1024;
    let staged_input_bytes = neighborhood_sum(&[
        config_bytes,
        c.generation_manifest.bytes,
        c.plane_manifest.bytes,
        c.router_root.bytes,
        c.membership.bytes,
        c.leaves.bytes,
        c.canonical.bytes,
    ])?;
    // Root owns the bounded terminal/stdout/stderr allowance and transport rename.
    let maximum_evidence_bytes = neighborhood_sum(&[SELECTION_CAP, REPORT_CAP, 2 * 1024 * 1024])?;
    Ok(NeighborhoodAllocation {
        components: BTreeMap::from([
            ("metadata_json_and_config", metadata),
            ("router_copies_and_directories", router),
            ("snapshots_and_paths", snapshots),
            ("selected_unit_rows_and_centers", selected),
            ("bounded_neighbor_heaps", heaps),
            ("canonical_row_io_and_id_bitset", scan),
            ("leaf_io_decode_and_roster", leaf_scan),
            ("one_ranking_and_nomination", ranking),
            ("seal_value_serialization_readback", seal_publication),
            ("report_value_serialization_readback", report_publication),
            ("human_summary_capacity_and_formatting", human_summary),
            ("allocator_reserve", allocator),
        ]),
        phases,
        peak_payload_bytes,
        runtime_allowance_bytes,
        charged_file_cache_allowance_bytes,
        proposed_charged_bytes: neighborhood_sum(&[
            peak_payload_bytes,
            runtime_allowance_bytes,
            charged_file_cache_allowance_bytes,
        ])?,
        staged_input_bytes,
        maximum_evidence_bytes,
        staged_peak_bytes: plus(staged_input_bytes, maximum_evidence_bytes)?,
    })
}

fn neighborhood_object_key(key: &str, sha: &str) -> bool {
    let mut parts = key.rsplit('/');
    valid_sha(sha)
        && key.len() <= 512
        && parts.next() == Some(sha)
        && parts.next() == Some("objects")
        && key.split('/').all(|p| {
            !p.is_empty()
                && p != "."
                && p != ".."
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        })
}

fn neighborhood_router(
    c: &NeighborhoodConfig,
) -> Result<(
    borsuk::semantic_unit_router::SemanticUnitRouter,
    borsuk::two_bit_source::SourcePlaneReceipt,
    Vec<HeaderSnapshot>,
)> {
    use borsuk::semantic_unit_router::{SemanticProfile, SemanticUnitRouter, SourceIdentity};
    let (g, gs) = header_json(&c.generation_manifest)?;
    fields(
        &g,
        "schema generation base_epoch plane_manifest_sha256 page_manifest_sha256 discovery sq8_object_sha256 sq8_object_key sq8_etag canonical low step",
    )?;
    fields(&g["canonical"], "object_key sha256 bytes rows dimensions")?;
    let canonical = &g["canonical"];
    require(
        g["schema"] == "borsuk-two-bit-generation-v8"
            && g["generation"].as_u64().is_some_and(|v| v > 0)
            && g["base_epoch"].as_u64().is_some()
            && g["page_manifest_sha256"].as_str().is_some_and(valid_sha)
            && g["plane_manifest_sha256"] == c.plane_manifest.sha256
            && canonical["rows"] == c.rows
            && canonical["dimensions"] == c.dimensions
            && canonical["bytes"] == c.canonical.bytes
            && canonical["sha256"] == c.canonical.sha256
            && canonical["object_key"] == c.canonical_object_key
            && neighborhood_object_key(&c.canonical_object_key, &c.canonical.sha256),
        "neighborhood generation/canonical descriptor authority before body open",
    )?;
    let d = &g["discovery"];
    fields(
        d,
        "mode profile root_sha256 root_bytes membership_sha256 membership_bytes leaves_sha256 leaves_bytes input_schema input_root_sha256 centroids_sha256 source_sha256 source_order_sha256 mean_sha256 records_sha256 sq8_sha256",
    )?;
    require(
        d["mode"] == "semantic"
            && d["profile"] == "scale1m"
            && d["root_sha256"] == c.router_root.sha256
            && d["root_bytes"] == c.router_root.bytes
            && d["membership_sha256"] == c.membership.sha256
            && d["membership_bytes"] == c.membership.bytes
            && d["leaves_sha256"] == c.leaves.sha256
            && d["leaves_bytes"] == c.leaves.bytes,
        "neighborhood discovery descriptors",
    )?;
    let (p, ps) = header_json(&c.plane_manifest)?;
    let plane: borsuk::two_bit_source::SourcePlaneReceipt = serde_json::from_value(p)?;
    require(
        plane.schema == "borsuk-two-bit-plane-v3"
            && plane.rows == c.rows
            && plane.dimensions == c.dimensions
            && plane.seed == 20260923
            && plane.page_rows == 32
            && !plane.query_or_truth_used
            && plane.record_bytes == source_record_bytes(c.dimensions)?
            && valid_sha(&plane.page_digest_sha256)
            && g["sq8_object_sha256"] == plane.sq8_sha256
            && g["sq8_object_key"]
                .as_str()
                .is_some_and(|key| neighborhood_object_key(key, &plane.sq8_sha256))
            && g["sq8_etag"].as_str().is_some_and(|v| {
                !v.is_empty() && !v.starts_with("W/") && !v.chars().any(char::is_control)
            }),
        "neighborhood plane geometry/SQ8 descriptor",
    )?;
    for (name, sha) in [
        ("source_sha256", &plane.source_sha256),
        ("source_order_sha256", &plane.source_order_sha256),
        ("mean_sha256", &plane.mean_sha256),
        ("records_sha256", &plane.records_sha256),
        ("sq8_sha256", &plane.sq8_sha256),
    ] {
        require(
            valid_sha(sha) && d[name] == *sha,
            "neighborhood discovery/plane identity",
        )?;
    }
    let low: Vec<f32> = serde_json::from_value(g["low"].clone())?;
    let step: Vec<f32> = serde_json::from_value(g["step"].clone())?;
    require(
        low.len() == c.dimensions
            && step.len() == c.dimensions
            && low.iter().all(|v| v.is_finite())
            && step.iter().all(|v| v.is_finite() && *v > 0.),
        "neighborhood calibration geometry/finite",
    )?;
    let identity = SourceIdentity {
        profile: SemanticProfile::Scale1m,
        schema: d["input_schema"]
            .as_str()
            .ok_or("neighborhood construction schema")?,
        root_sha256: d["input_root_sha256"]
            .as_str()
            .ok_or("neighborhood construction root")?,
        centroids_sha256: d["centroids_sha256"]
            .as_str()
            .ok_or("neighborhood centroid identity")?,
        rows: c.rows,
        dimensions: c.dimensions,
    };
    let (root, rs) = loss_binary(&c.router_root, c.router_root.bytes)?;
    let (membership, ms) = loss_binary(&c.membership, c.membership.bytes)?;
    let router = SemanticUnitRouter::open(
        &root,
        &membership,
        &c.router_root.sha256,
        &identity,
        SemanticProfile::Scale1m.root_cap(),
    )?;
    require(
        router.manifest().leaf_payload.bytes as u64 == c.leaves.bytes
            && router.manifest().leaf_payload.sha256 == c.leaves.sha256,
        "neighborhood root leaf payload identity",
    )?;
    Ok((router, plane, vec![gs, ps, rs, ms]))
}

struct NeighborhoodAnchor {
    unit: usize,
    physical: usize,
    unit_digest: [u8; 32],
    row_digest: [u8; 32],
    id: Option<i64>,
    coordinates: Vec<f32>,
    norm2: f64,
    canonical_unit_rows: Vec<f32>,
    stored_sq8_fp16_unit_mean: Vec<f32>,
}

fn neighborhood_digest(domain: &[u8], generation: &[u8; 32], unit: usize) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(generation);
    hash.update((unit as u64).to_le_bytes());
    hash.finalize().into()
}

fn neighborhood_anchors(
    generation: &str,
    rows: usize,
    count: usize,
    dimensions: usize,
) -> Result<Vec<NeighborhoodAnchor>> {
    require(
        valid_sha(generation)
            && rows > 0
            && (1..=32).contains(&count)
            && count <= rows.div_ceil(32),
        "neighborhood anchor geometry",
    )?;
    let mut generation_bytes = [0; 32];
    for (slot, byte) in generation_bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&generation[2 * slot..2 * slot + 2], 16)?;
    }
    let mut selected = std::collections::BinaryHeap::with_capacity(count);
    for unit in 0..rows.div_ceil(32) {
        let candidate = (
            neighborhood_digest(NEIGHBORHOOD_UNIT_DOMAIN, &generation_bytes, unit),
            unit,
        );
        if selected.len() < count {
            selected.push(candidate);
        } else if selected.peek().is_some_and(|worst| candidate < *worst) {
            *selected.peek_mut().ok_or("neighborhood unit heap")? = candidate;
        }
    }
    selected
        .into_sorted_vec()
        .into_iter()
        .map(|(unit_digest, unit)| {
            let row_digest = neighborhood_digest(NEIGHBORHOOD_ROW_DOMAIN, &generation_bytes, unit);
            let actual_rows = (rows - unit * 32).min(32);
            let offset =
                (u64::from_le_bytes(row_digest[..8].try_into()?) % actual_rows as u64) as usize;
            Ok(NeighborhoodAnchor {
                unit,
                physical: unit * 32 + offset,
                unit_digest,
                row_digest,
                id: None,
                coordinates: Vec::with_capacity(dimensions),
                norm2: 0.,
                canonical_unit_rows: Vec::with_capacity(actual_rows * dimensions),
                stored_sq8_fp16_unit_mean: Vec::with_capacity(dimensions),
            })
        })
        .collect()
}

fn neighborhood_body(pin: &CompletedInput) -> Result<HeaderSnapshot> {
    let file = header_file(&pin.path)?;
    let identity = file_identity(&file)?;
    require(identity.len == pin.bytes, "neighborhood body exact length")?;
    Ok(HeaderSnapshot {
        file,
        path: pin.path.clone(),
        identity,
    })
}

fn neighborhood_mark(seen: &mut [u8], position: usize) -> Result<()> {
    let word = seen
        .get_mut(position / 8)
        .ok_or("neighborhood roster range")?;
    let bit = 1 << (position % 8);
    require(*word & bit == 0, "neighborhood duplicate roster ID")?;
    *word |= bit;
    Ok(())
}

fn neighborhood_leaves(
    c: &NeighborhoodConfig,
    router: &borsuk::semantic_unit_router::SemanticUnitRouter,
    anchors: &mut [NeighborhoodAnchor],
) -> Result<HeaderSnapshot> {
    let snapshot = neighborhood_body(&c.leaves)?;
    let mut input = BufReader::with_capacity(NEIGHBORHOOD_IO, &snapshot.file);
    let width = 4 + 2 * c.dimensions;
    let mut buffer = vec![0; 64 * width];
    let mut seen = vec![0; c.rows.div_ceil(32).div_ceil(8)];
    let mut digest = Sha256::new();
    let mut count = 0;
    for leaf in &router.manifest().leaves {
        require(
            leaf.bytes <= buffer.len() && leaf.bytes % width == 0,
            "neighborhood leaf alignment",
        )?;
        let body = &mut buffer[..leaf.bytes];
        input.read_exact(body)?;
        digest.update(&*body);
        let units = router.validate_leaf(leaf.leaf_id, body)?;
        let mut sums = vec![0.0_f64; c.dimensions];
        for (unit, record) in units.into_iter().zip(body.chunks_exact(width)) {
            neighborhood_mark(&mut seen, unit)?;
            count += 1;
            let selected = anchors.iter_mut().find(|a| a.unit == unit);
            let mut selected = selected.map(|a| &mut a.stored_sq8_fp16_unit_mean);
            for (word, sum) in record[4..].chunks_exact(2).zip(&mut sums) {
                let value = half::f16::from_bits(u16::from_le_bytes(word.try_into()?)).to_f32();
                require(value.is_finite(), "neighborhood finite FP16 unit center")?;
                *sum += f64::from(value);
                if let Some(center) = &mut selected {
                    center.push(value);
                }
            }
        }
        require(
            sums.iter().zip(&leaf.prototype).all(|(sum, expected)| {
                ((*sum / leaf.unit_count as f64) as f32).to_bits() == expected.to_bits()
            }),
            "neighborhood unweighted persisted-order prototype parity",
        )?;
    }
    require(
        input.read(&mut [0])? == 0
            && count == c.rows.div_ceil(32)
            && format!("{:x}", digest.finalize()) == c.leaves.sha256,
        "neighborhood complete leaf SHA/EOF/roster",
    )?;
    require(
        anchors
            .iter()
            .all(|a| a.stored_sq8_fp16_unit_mean.len() == c.dimensions),
        "neighborhood selected center roster",
    )?;
    drop(input);
    snapshot.recheck()?;
    Ok(snapshot)
}

fn neighborhood_norm(row: &[f32]) -> Result<f64> {
    let mut norm = 0.0_f64;
    for &value in row {
        require(
            value.is_finite(),
            "neighborhood finite canonical coordinate",
        )?;
        norm += f64::from(value) * f64::from(value);
    }
    require(
        norm.is_finite() && (norm - 1.).abs() <= 1e-6,
        "neighborhood canonical squared norm tolerance",
    )?;
    Ok(norm)
}

// Fixture-specific ordinal IDs: hashing the original signed-ID bytes is also
// the authenticated u64 ordinal order after range admission. No SQ8 body needed.
fn neighborhood_scan(
    c: &NeighborhoodConfig,
    snapshot: &mut HeaderSnapshot,
    order_sha: &str,
    mut observe: impl FnMut(usize, i64, &[f32], f64) -> Result<()>,
) -> Result<()> {
    snapshot.recheck()?;
    snapshot.file.seek(io::SeekFrom::Start(0))?;
    let mut input = BufReader::with_capacity(NEIGHBORHOOD_IO, &snapshot.file);
    let mut record = vec![0; 8 + 4 * c.dimensions];
    let mut row = vec![0.; c.dimensions];
    let mut seen = vec![0; c.rows.div_ceil(8)];
    let mut body_sha = Sha256::new();
    let mut order = Sha256::new();
    for position in 0..c.rows {
        input.read_exact(&mut record)?;
        body_sha.update(&record);
        order.update(&record[..8]);
        let id = i64::from_le_bytes(record[..8].try_into()?);
        require(
            id >= 0 && (id as u64) < c.rows as u64,
            "neighborhood fixture ordinal ID range",
        )?;
        neighborhood_mark(&mut seen, id as usize)?;
        for (value, bytes) in row.iter_mut().zip(record[8..].chunks_exact(4)) {
            *value = f32::from_le_bytes(bytes.try_into()?);
        }
        let norm = neighborhood_norm(&row)?;
        observe(position, id, &row, norm)?;
    }
    require(
        input.read(&mut [0])? == 0 && format!("{:x}", body_sha.finalize()) == c.canonical.sha256,
        "neighborhood canonical body SHA/EOF",
    )?;
    require(
        format!("{:x}", order.finalize()) == order_sha,
        "neighborhood canonical ID order",
    )?;
    drop(input);
    snapshot.recheck()
}

#[derive(Debug, Clone, Copy, Serialize)]
struct NeighborhoodNeighbor {
    distance: f64,
    id: i64,
    physical: usize,
}

impl PartialEq for NeighborhoodNeighbor {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for NeighborhoodNeighbor {}
impl PartialOrd for NeighborhoodNeighbor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for NeighborhoodNeighbor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then(self.id.cmp(&other.id))
    }
}

fn neighborhood_cosine(anchor: &[f32], anchor_norm: f64, row: &[f32], row_norm: f64) -> f64 {
    let mut dot = 0.0_f64;
    for (&a, &b) in anchor.iter().zip(row) {
        dot += f64::from(a) * f64::from(b);
    }
    let distance = 1. - (dot / (anchor_norm.sqrt() * row_norm.sqrt())).clamp(-1., 1.);
    if distance == 0. { 0. } else { distance }
}

#[allow(clippy::too_many_arguments)]
fn neighborhood_offer(
    heap: &mut std::collections::BinaryHeap<NeighborhoodNeighbor>,
    count: usize,
    anchor_id: i64,
    anchor: &[f32],
    anchor_norm: f64,
    physical: usize,
    id: i64,
    row: &[f32],
    row_norm: f64,
) {
    if id == anchor_id {
        return;
    }
    let neighbor = NeighborhoodNeighbor {
        id,
        physical,
        distance: neighborhood_cosine(anchor, anchor_norm, row, row_norm),
    };
    if heap.len() < count {
        heap.push(neighbor);
    } else if heap.peek().is_some_and(|worst| neighbor < *worst) {
        if let Some(mut worst) = heap.peek_mut() {
            *worst = neighbor;
        }
    }
}

fn neighborhood_canonical_statistics(rows: &[f32], dimensions: usize) -> (Vec<f64>, f64) {
    let count = rows.len() / dimensions;
    let mut mean = vec![0.0; dimensions];
    for row in rows.chunks_exact(dimensions) {
        for (sum, &value) in mean.iter_mut().zip(row) {
            *sum += f64::from(value);
        }
    }
    for sum in &mut mean {
        *sum /= count as f64;
    }
    let mut dispersion = 0.0;
    for row in rows.chunks_exact(dimensions) {
        for (&value, &center) in row.iter().zip(&mean) {
            let delta = f64::from(value) - center;
            dispersion += delta * delta;
        }
    }
    (mean, dispersion / count as f64)
}

fn neighborhood_residual(row: &[f32], center: &[f32]) -> f64 {
    let mut result = 0.0;
    for (&value, &mean) in row.iter().zip(center) {
        let delta = f64::from(value) - f64::from(mean);
        result += delta * delta;
    }
    result
}

fn neighborhood_anchor_report(
    c: &NeighborhoodConfig,
    router: &borsuk::semantic_unit_router::SemanticUnitRouter,
    anchor: &NeighborhoodAnchor,
    neighbors: &[NeighborhoodNeighbor],
) -> Result<Value> {
    require(
        neighbors.len() == c.neighbor_count,
        "neighborhood exact neighbor count",
    )?;
    let query =
        borsuk::two_bit_generation::normalize_two_bit_diagnostic_query(&anchor.coordinates)?;
    let ranked = router.rank_leaves_by_metric(&query, &borsuk::VectorMetric::SquaredEuclidean)?;
    let selected_count = ranked.len().min(48);
    require(
        c.scope == NeighborhoodScope::SmallFixture || selected_count == 48,
        "neighborhood frozen top48",
    )?;
    let nomination = router.nominate_selected(&ranked[..selected_count])?;
    let mut ranks = vec![0; ranked.len()];
    for (rank, &leaf) in ranked.iter().enumerate() {
        ranks[leaf] = rank + 1;
    }
    let own_leaf = router.unit_membership()[anchor.unit];
    let mut frequencies = std::collections::BTreeMap::<usize, usize>::new();
    let mut leaves = BTreeSet::new();
    let mut joint = [0_usize; 32];
    let mut evidence = Vec::with_capacity(neighbors.len());
    let mut outside_leaves = 0;
    let mut outside_pages = 0;
    let mut interaction = 0;
    for neighbor in neighbors {
        let unit = neighbor.physical / 32;
        let page = neighbor.physical / 256;
        let leaf = router.unit_membership()[unit];
        let own_unit = unit == anchor.unit;
        let own_page = page == anchor.physical / 256;
        let same_leaf = leaf == own_leaf;
        let selected_leaf = ranks[leaf] <= selected_count;
        let page_covered = nomination.page_closure.contains(&page);
        let bucket = usize::from(own_unit)
            | (usize::from(own_page) << 1)
            | (usize::from(same_leaf) << 2)
            | (usize::from(selected_leaf) << 3)
            | (usize::from(page_covered) << 4);
        joint[bucket] += 1;
        outside_leaves += usize::from(!selected_leaf);
        outside_pages += usize::from(!page_covered);
        interaction += usize::from(!same_leaf && !selected_leaf && !page_covered);
        *frequencies.entry(unit).or_default() += 1;
        leaves.insert(leaf);
        evidence.push(json!({"id":neighbor.id,"physical_row":neighbor.physical,
            "cosine_distance":neighbor.distance,"destination_unit":unit,"destination_page":page,
            "destination_leaf":leaf,"destination_leaf_l2_rank":ranks[leaf],
            "own_unit":own_unit,"own_page":own_page,"own_leaf":same_leaf,
            "inside_selected_leaf_union":selected_leaf,"inside_physical_page_closure":page_covered}));
    }
    let joint_counts = joint
        .iter()
        .enumerate()
        .filter(|(_, n)| **n > 0)
        .map(|(bits, count)| {
            json!({"own_unit":bits&1!=0,"own_page":bits&2!=0,"own_leaf":bits&4!=0,
            "inside_selected_leaf_union":bits&8!=0,"inside_physical_page_closure":bits&16!=0,
            "count":count})
        })
        .collect::<Vec<_>>();
    let (mean, dispersion) =
        neighborhood_canonical_statistics(&anchor.canonical_unit_rows, c.dimensions);
    let squared_frequency_sum: usize = frequencies.values().map(|n| n * n).sum();
    Ok(
        json!({"unit":anchor.unit,"physical_row":anchor.physical,"id":anchor.id,
        "own_leaf":own_leaf,"own_leaf_l2_rank":ranks[own_leaf],"neighbors":evidence,
        "joint_counts":joint_counts,"outside_selected_leaf_union":outside_leaves,
        "outside_physical_page_closure":outside_pages,
        "cross_leaf_and_outside_top48_and_outside_page_closure":interaction,
        "distinct_destination_units":frequencies.len(),"distinct_destination_leaves":leaves.len(),
        "destination_unit_frequencies":frequencies.iter().map(|(unit,count)|json!({"unit":unit,"count":count})).collect::<Vec<_>>(),
        "maximum_destination_unit_count":frequencies.values().max(),
        "destination_unit_concentration":squared_frequency_sum as f64 / (neighbors.len()*neighbors.len()) as f64,
        "selected_leaf_ids":nomination.leaf_ids,"physical_page_closure":nomination.page_closure,
        "canonical_selected_unit_mean":mean,"canonical_selected_unit_dispersion":dispersion,
        "canonical_selected_unit_rows":anchor.canonical_unit_rows.len()/c.dimensions,
        "canonical_anchor_to_stored_sq8_fp16_unit_mean_squared_l2":neighborhood_residual(&anchor.coordinates, &anchor.stored_sq8_fp16_unit_mean),
        "stored_sq8_fp16_unit_mean_to_persisted_unweighted_leaf_prototype_squared_l2":neighborhood_residual(&anchor.stored_sq8_fp16_unit_mean, &router.manifest().leaves[own_leaf].prototype),
        "unit_indivisibility":"UNRESOLVED","interpretation":"INCONCLUSIVE"}),
    )
}

fn reduce_source_neighborhood(
    c: &NeighborhoodConfig,
    config_sha: &str,
    config_snapshot: HeaderSnapshot,
    allocation: &NeighborhoodAllocation,
    seal_sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<(Value, Vec<HeaderSnapshot>)> {
    let (router, plane, mut snapshots) = neighborhood_router(c)?;
    snapshots.push(config_snapshot);
    let mut anchors = neighborhood_anchors(
        &c.generation_manifest.sha256,
        c.rows,
        c.anchor_count,
        c.dimensions,
    )?;
    snapshots.push(neighborhood_leaves(c, &router, &mut anchors)?);
    let mut canonical = neighborhood_body(&c.canonical)?;
    neighborhood_scan(
        c,
        &mut canonical,
        &plane.source_order_sha256,
        |position, id, row, norm| {
            for anchor in &mut anchors {
                if position / 32 == anchor.unit {
                    anchor.canonical_unit_rows.extend_from_slice(row);
                    if position == anchor.physical {
                        anchor.id = Some(id);
                        anchor.coordinates.extend_from_slice(row);
                        anchor.norm2 = norm;
                    }
                }
            }
            Ok(())
        },
    )?;
    require(
        anchors.iter().all(|a| {
            a.id.is_some()
                && a.coordinates.len() == c.dimensions
                && a.canonical_unit_rows.len() == (c.rows - a.unit * 32).min(32) * c.dimensions
        }),
        "neighborhood authenticated complete anchors",
    )?;
    let source_sha = format!(
        "{:x}",
        Sha256::digest(include_bytes!("compare_native_replay.rs"))
    );
    let seal = json!({"schema":"borsuk-source-neighborhood-anchor-seal-v1","status":"SEALED",
        "config_sha256":config_sha,"implementation_sha256":source_sha,"config":c,
        "raw_source_sha256":plane.source_sha256,"canonical_sha256":c.canonical.sha256,
        "source_order_sha256":plane.source_order_sha256,"sq8_identity_only_sha256":plane.sq8_sha256,
        "router_implementation_sha256":format!("{:x}",Sha256::digest(include_bytes!("../src/semantic_unit_router.rs"))),
        "normalization_implementation_sha256":format!("{:x}",Sha256::digest(include_bytes!("../src/sq8_source.rs"))),
        "generation_implementation_sha256":format!("{:x}",Sha256::digest(include_bytes!("../src/two_bit_generation.rs"))),
        "unit_domain":"borsuk-source-neighborhood-unit-v1","row_domain":"borsuk-source-neighborhood-row-v1",
        "domain_encoding":"ASCII then one NUL then 32 binary generation-SHA bytes then u64LE unit",
        "unit_selection":"lowest digest bytes lexicographically; tie physical unit ID",
        "row_selection":"first eight digest bytes as u64LE modulo actual unit row count",
        "method_version":1,"arithmetic_version":NEIGHBORHOOD_ARITHMETIC,
        "id_contract":"fixture-specific original ordinals; not a production-ID restriction",
        "anchors":anchors.iter().map(|a|json!({"unit":a.unit,"physical_row":a.physical,"id":a.id,
            "unit_sha256":a.unit_digest.iter().map(|b|format!("{b:02x}")).collect::<String>(),
            "row_sha256":a.row_digest.iter().map(|b|format!("{b:02x}")).collect::<String>(),
            "row_offset":a.physical%32,"actual_unit_rows":(c.rows-a.unit*32).min(32),
            "canonical_norm2":a.norm2})).collect::<Vec<_>>()});
    let mut references = snapshots.iter().collect::<Vec<_>>();
    references.push(&canonical);
    let (sealed, seal_sha) =
        publish_selection_seal_with_sync(&c.anchor_seal, &seal, &references, seal_sync)?;
    drop(references);
    drop(seal);
    snapshots.push(sealed);
    // The first neighbor operation is after successful file AND directory sync.
    let mut heaps = (0..c.anchor_count)
        .map(|_| std::collections::BinaryHeap::with_capacity(c.neighbor_count))
        .collect::<Vec<_>>();
    neighborhood_scan(
        c,
        &mut canonical,
        &plane.source_order_sha256,
        |position, id, row, norm| {
            for (anchor, heap) in anchors.iter().zip(&mut heaps) {
                neighborhood_offer(
                    heap,
                    c.neighbor_count,
                    anchor.id.ok_or("neighborhood sealed ID")?,
                    &anchor.coordinates,
                    anchor.norm2,
                    position,
                    id,
                    row,
                    norm,
                );
            }
            Ok(())
        },
    )?;
    snapshots.push(canonical);
    let results = anchors
        .iter()
        .zip(heaps)
        .map(|(anchor, heap)| {
            neighborhood_anchor_report(c, &router, anchor, &heap.into_sorted_vec())
        })
        .collect::<Result<Vec<_>>>()?;
    for snapshot in &snapshots {
        snapshot.recheck()?;
    }
    let report = json!({"schema":NEIGHBORHOOD_SCHEMA,"status":"MEASURED","complete":true,
        "interpretation":"EXPLORATORY_INCONCLUSIVE","qualified":false,
        "implementation_sha256":source_sha,"config_sha256":config_sha,
        "generation_sha256":c.generation_manifest.sha256,"canonical_sha256":c.canonical.sha256,
        "calibration_authority":"generation_sha256 binds validated low/step and plane/router provenance",
        "raw_source_sha256":plane.source_sha256,"source_order_sha256":plane.source_order_sha256,
        "anchor_seal_sha256":seal_sha,"anchor_seal_path":c.anchor_seal,
        "arithmetic_version":NEIGHBORHOOD_ARITHMETIC,"rows":c.rows,"dimensions":c.dimensions,
        "anchor_count":c.anchor_count,"neighbor_count":c.neighbor_count,"scope":c.scope,
        "sampling_units":"anchors; neighbor slots are correlated","in_sample_partition_anchors":true,
        "anchor_seal_durable_before_neighbor_scan":true,"canonical_full_authenticated_passes":2,
        "anchors":results,"allocation":allocation,
        "staging":"one canonical; config/generation/plane/root/membership/leaves; transport temp renamed, never copied; seal<=4MiB, report<=2MiB, root terminal/logs<=2MiB",
        "resource_envelope_qualified":false,"runtime_and_cache_allowances_require_native_admission":true,
        "report_alone_accepted":false,"external_gate_required":true,
        "acceptance_requires":"original exit0 and successful drained terminal receipt with exact instance, source/config/ELF/seal/report hashes and resource/OOM status; abrupt death INVALID",
        "unit_indivisibility":"UNRESOLVED","intra_unit_conflicting_neighborhoods":"UNRESOLVED",
        "destination_unit_limit_is_tautological":true,"graph_cut_go":false,
        "gap_bridge_fetches":"UNMEASURED; excluded from physical-page closure",
        "quantization_cause":"NOT_INFERRED","rare_query_generalization":false,
        "held_out_routing_claim":false,"recall_qps_cold_vendor_or_100m_claim":false,
        "source_only":true,"ann_arm_selected":false});
    require(
        serde_json::to_vec(&report)?.len() as u64 + 1 <= REPORT_CAP,
        "neighborhood report cap",
    )?;
    Ok((report, snapshots))
}

fn neighborhood_summary(report: &Value) -> Result<String> {
    use std::fmt::Write as _;
    let anchors = report["anchors"]
        .as_array()
        .ok_or("neighborhood summary anchors")?;
    require(
        (1..=32).contains(&anchors.len()),
        "neighborhood summary anchor bound",
    )?;
    let mut summary = String::with_capacity(NEIGHBORHOOD_SUMMARY_CAP);
    writeln!(summary, "Source neighborhood: EXPLORATORY / INCONCLUSIVE")?;
    writeln!(
        summary,
        "Rows {}; dimensions {}; anchors {}; neighbors per anchor {}.",
        report["rows"], report["dimensions"], report["anchor_count"], report["neighbor_count"]
    )?;
    writeln!(
        summary,
        "Per anchor: unit; own-leaf rank; outside top48/page closure/joint cross-leaf; distinct destination units/leaves; max unit count; concentration; canonical dispersion; anchor-to-SQ8-FP16 / SQ8-FP16-to-leaf squared L2. Small fixtures select all leaves if fewer than 48."
    )?;
    for anchor in anchors {
        writeln!(
            summary,
            "{}; {}; {}/{}/{}; {}/{}; {}; {}; {}; {}/{}",
            anchor["unit"],
            anchor["own_leaf_l2_rank"],
            anchor["outside_selected_leaf_union"],
            anchor["outside_physical_page_closure"],
            anchor["cross_leaf_and_outside_top48_and_outside_page_closure"],
            anchor["distinct_destination_units"],
            anchor["distinct_destination_leaves"],
            anchor["maximum_destination_unit_count"],
            anchor["destination_unit_concentration"],
            anchor["canonical_selected_unit_dispersion"],
            anchor["canonical_anchor_to_stored_sq8_fp16_unit_mean_squared_l2"],
            anchor["stored_sq8_fp16_unit_mean_to_persisted_unweighted_leaf_prototype_squared_l2"]
        )?;
    }
    writeln!(
        summary,
        "Published report remains authoritative; original exit0, drained terminal receipt, exact hashes and resource/OOM status still required. Report alone never accepted."
    )?;
    writeln!(
        summary,
        "In-sample anchors; neighbor slots correlated. Unit indivisibility and conflicting affinities unresolved; unselected leaves may share selected pages; gap bridges unmeasured. No quantization-cause, graph-cut, rare-query, held-out, recall/QPS/cold/vendor/100M claim. Both historical 312/320 KILLs preserved; no ANN arm selected."
    )?;
    require(
        summary.len() <= NEIGHBORHOOD_SUMMARY_CAP,
        "neighborhood summary cap",
    )?;
    Ok(summary)
}

fn execute_source_neighborhood(config_path: &Path, config_sha: &str) -> Result<bool> {
    execute_source_neighborhood_with(
        config_path,
        config_sha,
        |file, _| file.sync_all(),
        |file, _| file.sync_all(),
        &mut io::stdout().lock(),
    )
}

fn execute_source_neighborhood_with(
    config_path: &Path,
    config_sha: &str,
    seal_sync: impl FnMut(&File, bool) -> io::Result<()>,
    report_sync: impl FnMut(&File, bool) -> io::Result<()>,
    output: &mut impl Write,
) -> Result<bool> {
    let file = header_file(config_path)?;
    let snapshot = HeaderSnapshot {
        identity: file_identity(&file)?,
        file,
        path: config_path.to_owned(),
    };
    let (c, bytes): (NeighborhoodConfig, _) =
        read_config_file(&snapshot.file, config_path, config_sha, CONFIG_CAP)?;
    snapshot.recheck()?;
    // The entire coexistence model is admitted before any pinned input is opened.
    let allocation = neighborhood_allocation(&c, bytes)?;
    require(
        config_path != c.anchor_seal && config_path != c.report,
        "neighborhood config/output paths",
    )?;
    let mut summary = String::new();
    let measured = execute_leaf_coverage_as(
        &c.report,
        NEIGHBORHOOD_SCHEMA,
        || {
            let (report, retained) =
                reduce_source_neighborhood(&c, config_sha, snapshot, &allocation, seal_sync)?;
            summary = neighborhood_summary(&report)?;
            Ok((report, retained))
        },
        report_sync,
    )?;
    if measured {
        output.write_all(summary.as_bytes())?;
        output.flush()?;
    }
    Ok(measured)
}

#[cfg(test)]
mod source_neighborhood_tests {
    use super::*;

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn put(path: &Path, body: &[u8]) -> CompletedInput {
        std::fs::write(path, body).unwrap();
        CompletedInput {
            path: path.to_owned(),
            bytes: body.len() as u64,
            sha256: digest(body),
        }
    }

    fn repin_json(pin: &mut CompletedInput, change: impl FnOnce(&mut Value)) {
        let mut value: Value = serde_json::from_slice(&std::fs::read(&pin.path).unwrap()).unwrap();
        change(&mut value);
        *pin = put(&pin.path, &serde_json::to_vec(&value).unwrap());
    }

    fn rebind_descriptors(c: &mut NeighborhoodConfig) {
        let plane: Value =
            serde_json::from_slice(&std::fs::read(&c.plane_manifest.path).unwrap()).unwrap();
        repin_json(&mut c.generation_manifest, |g| {
            g["plane_manifest_sha256"] = json!(c.plane_manifest.sha256);
            g["discovery"]["source_order_sha256"] = plane["source_order_sha256"].clone();
            for (name, pin) in [
                ("root", &c.router_root),
                ("membership", &c.membership),
                ("leaves", &c.leaves),
            ] {
                g["discovery"][format!("{name}_sha256")] = json!(pin.sha256);
                g["discovery"][format!("{name}_bytes")] = json!(pin.bytes);
            }
            g["canonical"]["sha256"] = json!(c.canonical.sha256);
            g["canonical"]["bytes"] = json!(c.canonical.bytes);
            g["canonical"]["object_key"] = json!(c.canonical_object_key);
        });
    }

    fn fixture(dir: &Path, rows: usize, centers: &[[f32; 2]]) -> NeighborhoodConfig {
        use borsuk::semantic_unit_router::{SemanticProfile, SourceIdentity, build};
        let mut blob = b"BORSUCP1".to_vec();
        blob.extend_from_slice(&(rows as u64).to_le_bytes());
        for word in [2_u32, 32, 256, 0] {
            blob.extend_from_slice(&word.to_le_bytes());
        }
        for unit in 0..rows.div_ceil(32) {
            for value in centers[unit % centers.len()] {
                blob.extend_from_slice(&half::f16::from_f32(value).to_bits().to_le_bytes());
            }
        }
        let source = SourceIdentity {
            profile: SemanticProfile::Scale1m,
            schema: "source-neighborhood-fixture",
            root_sha256: &"1".repeat(64),
            centroids_sha256: &digest(&blob),
            rows,
            dimensions: 2,
        };
        let artifacts = build(&blob, &source, 64 * 1024 * 1024).unwrap();
        let root = put(&dir.join("root"), &artifacts.manifest);
        let membership = put(&dir.join("membership"), &artifacts.membership);
        let leaves = put(&dir.join("leaves"), &artifacts.leaves);
        let mut canonical = Vec::new();
        let mut order = Vec::new();
        for position in 0..rows {
            let id = ((position + 1) % rows) as i64;
            canonical.extend_from_slice(&id.to_le_bytes());
            order.extend_from_slice(&id.to_le_bytes());
            for value in [1.0_f32, 0.0] {
                canonical.extend_from_slice(&value.to_le_bytes());
            }
        }
        let canonical = put(&dir.join("canonical"), &canonical);
        let key = format!("semantic/objects/{}", canonical.sha256);
        let plane = json!({"schema":"borsuk-two-bit-plane-v3","rows":rows,"dimensions":2,
            "seed":20260923,"record_bytes":9,"source_sha256":"2".repeat(64),
            "sq8_sha256":"3".repeat(64),"source_order_sha256":digest(&order),
            "mean_sha256":"4".repeat(64),"records_sha256":"5".repeat(64),
            "page_rows":32,"page_digest_sha256":"6".repeat(64),"query_or_truth_used":false});
        let plane_pin = put(&dir.join("plane"), &serde_json::to_vec(&plane).unwrap());
        let generation = json!({"schema":"borsuk-two-bit-generation-v8","generation":1,"base_epoch":0,
            "plane_manifest_sha256":plane_pin.sha256,"page_manifest_sha256":"7".repeat(64),
            "discovery":{"mode":"semantic","profile":"scale1m","root_sha256":root.sha256,"root_bytes":root.bytes,
                "membership_sha256":membership.sha256,"membership_bytes":membership.bytes,
                "leaves_sha256":leaves.sha256,"leaves_bytes":leaves.bytes,"input_schema":source.schema,
                "input_root_sha256":source.root_sha256,"centroids_sha256":source.centroids_sha256,
                "source_sha256":plane["source_sha256"],"source_order_sha256":plane["source_order_sha256"],
                "mean_sha256":plane["mean_sha256"],"records_sha256":plane["records_sha256"],"sq8_sha256":plane["sq8_sha256"]},
            "sq8_object_sha256":plane["sq8_sha256"],"sq8_object_key":format!("semantic/objects/{}","3".repeat(64)),
            "sq8_etag":"fixture","canonical":{"object_key":key,"sha256":canonical.sha256,"bytes":canonical.bytes,"rows":rows,"dimensions":2},
            "low":[-1.,-1.],"step":[0.125,0.125]});
        NeighborhoodConfig {
            schema: NEIGHBORHOOD_CONFIG_SCHEMA.into(),
            scope: NeighborhoodScope::SmallFixture,
            rows,
            dimensions: 2,
            anchor_count: rows.div_ceil(32).min(3),
            neighbor_count: (rows - 1).min(32),
            generation_manifest: put(
                &dir.join("generation"),
                &serde_json::to_vec(&generation).unwrap(),
            ),
            plane_manifest: plane_pin,
            router_root: root,
            membership,
            leaves,
            canonical,
            canonical_object_key: key,
            max_payload_bytes: 1 << 30,
            anchor_seal: dir.join("anchor-seal.json"),
            report: dir.join("report.json"),
        }
    }

    fn config_pin(c: &NeighborhoodConfig) -> CompletedInput {
        put(
            &c.report.parent().unwrap().join("config.json"),
            &serde_json::to_vec(c).unwrap(),
        )
    }

    fn run_with(
        c: &NeighborhoodConfig,
        seal_sync: impl FnMut(&File, bool) -> io::Result<()>,
        report_sync: impl FnMut(&File, bool) -> io::Result<()>,
    ) -> Result<bool> {
        let pin = config_pin(c);
        let mut summary = Vec::new();
        let result = execute_source_neighborhood_with(
            &pin.path,
            &pin.sha256,
            seal_sync,
            report_sync,
            &mut summary,
        );
        if !matches!(&result, Ok(true)) {
            assert!(
                summary.is_empty(),
                "no summary before successful publication"
            );
        }
        result
    }

    fn json_file(path: &Path) -> Value {
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }

    #[test]
    fn source_neighborhood_strict_source_only_config_and_json() {
        let dir = tempfile::tempdir().unwrap();
        let c = fixture(dir.path(), 65, &[[0., 0.]]);
        let value = serde_json::to_value(&c).unwrap();
        for field in [
            "requests",
            "truth",
            "completed",
            "sq8",
            "raw",
            "centroids",
            "adjacency",
        ] {
            let mut bad = value.clone();
            bad[field] = json!({});
            assert!(serde_json::from_value::<NeighborhoodConfig>(bad).is_err());
        }
        let mut bad = value.clone();
        bad["calibration_sha256"] = json!("0".repeat(64));
        assert!(serde_json::from_value::<NeighborhoodConfig>(bad).is_err());
        let mut bad = value.clone();
        bad["canonical"]["requests"] = json!("forbidden");
        assert!(serde_json::from_value::<NeighborhoodConfig>(bad).is_err());
        let mut bad = value;
        bad["scope"] = json!("scientific");
        let bad: NeighborhoodConfig = serde_json::from_value(bad).unwrap();
        assert!(neighborhood_allocation(&bad, 4096).is_err());
        for body in [b"{\"x\":{\"sha256\":1,\"sha256\":2}}".as_slice(), b"{}\n{}"] {
            let pin = put(&dir.path().join("bad.json"), body);
            assert!(header_json(&pin).is_err());
        }
    }

    #[test]
    fn source_neighborhood_authority_refuses_before_canonical_open() {
        for fault in [
            "wrong_generation_canonical",
            "canonical_key",
            "descriptor",
            "calibration",
            "calibration_zero_step",
            "calibration_low_shape",
            "calibration_step_shape",
            "calibration_nonfinite",
            "order_binding",
            "raw_binding",
            "plane_shape",
            "extra_field",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut c = fixture(dir.path(), 65, &[[0., 0.]]);
            match fault {
                "wrong_generation_canonical" => {
                    // Independently valid normalized body with the same IDs/length.
                    let mut body = std::fs::read(&c.canonical.path).unwrap();
                    for row in body.chunks_exact_mut(16) {
                        row[8..12].copy_from_slice(&0_f32.to_le_bytes());
                        row[12..16].copy_from_slice(&1_f32.to_le_bytes());
                    }
                    c.canonical = put(&c.canonical.path, &body);
                    c.canonical_object_key = format!("semantic/objects/{}", c.canonical.sha256);
                }
                "canonical_key" => {
                    c.canonical_object_key = "other/objects/invalid".into();
                }
                "descriptor" => repin_json(&mut c.generation_manifest, |g| {
                    g["discovery"]["leaves_sha256"] = json!("0".repeat(64))
                }),
                "calibration" => {
                    let mut g = json_file(&c.generation_manifest.path);
                    g["low"][0] = json!(-2.0);
                    // Keep the authenticated generation pin unchanged.
                    std::fs::write(&c.generation_manifest.path, serde_json::to_vec(&g).unwrap())
                        .unwrap();
                }
                "calibration_zero_step" => {
                    repin_json(&mut c.generation_manifest, |g| g["step"][0] = json!(0.))
                }
                "calibration_low_shape" => {
                    repin_json(&mut c.generation_manifest, |g| g["low"] = json!([-1.]))
                }
                "calibration_step_shape" => {
                    repin_json(&mut c.generation_manifest, |g| g["step"] = json!([0.125]))
                }
                "calibration_nonfinite" => {
                    repin_json(&mut c.generation_manifest, |g| g["low"][0] = json!(1e300))
                }
                "order_binding" => repin_json(&mut c.generation_manifest, |g| {
                    g["discovery"]["source_order_sha256"] = json!("0".repeat(64))
                }),
                "raw_binding" => repin_json(&mut c.generation_manifest, |g| {
                    g["discovery"]["source_sha256"] = json!("0".repeat(64))
                }),
                "plane_shape" => {
                    repin_json(&mut c.plane_manifest, |p| p["rows"] = json!(66));
                    rebind_descriptors(&mut c);
                }
                "extra_field" => repin_json(&mut c.generation_manifest, |g| {
                    g["canonical"]["truth"] = json!(1)
                }),
                _ => unreachable!(),
            }
            std::fs::remove_file(&c.canonical.path).unwrap();
            // The error is metadata authority, not the now-missing large body.
            let error = neighborhood_router(&c).err().unwrap().to_string();
            assert!(!error.contains("No such file"), "{fault}: {error}");
            if fault == "calibration" {
                assert_eq!(error, "config SHA/length/identity");
            }
            assert!(!c.anchor_seal.exists());
        }
        let dir = tempfile::tempdir().unwrap();
        let mut c = fixture(dir.path(), 65, &[[0., 0.]]);
        let previous = c.generation_manifest.sha256.clone();
        repin_json(&mut c.generation_manifest, |g| g["low"][0] = json!(-2.0));
        assert_ne!(c.generation_manifest.sha256, previous);
        // A newly authenticated generation owns valid calibration without another pin.
        assert!(neighborhood_router(&c).is_ok());
    }

    #[test]
    fn source_neighborhood_full_canonical_body_order_bijection_and_norm_refusals() {
        for fault in [
            "body_sha",
            "wrong_order",
            "duplicate",
            "out_of_range",
            "negative",
            "nan",
            "infinity",
            "zero",
            "rescaled",
            "truncated",
            "trailing",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut c = fixture(dir.path(), 65, &[[0., 0.]]);
            let order = json_file(&c.plane_manifest.path)["source_order_sha256"]
                .as_str()
                .unwrap()
                .to_owned();
            let mut body = std::fs::read(&c.canonical.path).unwrap();
            match fault {
                "body_sha" => body[8..12].copy_from_slice(&(-1_f32).to_le_bytes()),
                "wrong_order" => {
                    for byte in 0..8 {
                        body.swap(byte, 16 + byte);
                    }
                }
                "duplicate" => body[..8].copy_from_slice(&2_i64.to_le_bytes()),
                "out_of_range" => body[..8].copy_from_slice(&65_i64.to_le_bytes()),
                "negative" => body[..8].copy_from_slice(&(-1_i64).to_le_bytes()),
                "nan" => body[8..12].copy_from_slice(&f32::NAN.to_le_bytes()),
                "infinity" => body[8..12].copy_from_slice(&f32::INFINITY.to_le_bytes()),
                "zero" => body[8..12].copy_from_slice(&0_f32.to_le_bytes()),
                "rescaled" => body[8..12].copy_from_slice(&2_f32.to_le_bytes()),
                "truncated" => {
                    body.pop();
                }
                "trailing" => body.push(0),
                _ => unreachable!(),
            }
            std::fs::write(&c.canonical.path, &body).unwrap();
            if fault != "body_sha" {
                c.canonical.sha256 = digest(&body);
            }
            if matches!(fault, "trailing" | "truncated") {
                c.canonical.bytes = body.len() as u64;
            }
            let mut snapshot = neighborhood_body(&c.canonical).unwrap();
            let error = neighborhood_scan(&c, &mut snapshot, &order, |_, _, _, _| Ok(()))
                .unwrap_err()
                .to_string();
            if fault == "wrong_order" {
                assert_eq!(error, "neighborhood canonical ID order");
            }
        }
    }

    #[test]
    fn source_neighborhood_canonical_authentication_precedes_anchor_seal() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = fixture(dir.path(), 65, &[[0., 0.]]);
        let mut body = std::fs::read(&c.canonical.path).unwrap();
        for byte in 0..8 {
            body.swap(byte, 16 + byte);
        }
        c.canonical = put(&c.canonical.path, &body);
        c.canonical_object_key = format!("semantic/objects/{}", c.canonical.sha256);
        rebind_descriptors(&mut c);
        assert!(!run_with(&c, |file, _| file.sync_all(), |file, _| file.sync_all()).unwrap());
        assert!(!c.anchor_seal.exists());
        let report = json_file(&c.report);
        assert_eq!(report["status"], "INVALID");
        assert!(
            report["error"]
                .as_str()
                .unwrap()
                .contains("canonical ID order")
        );
    }

    #[test]
    fn source_neighborhood_lifetime_stamp_growth_and_path_refusals() {
        for fault in ["same_bytes", "growth", "replace", "symlink"] {
            let dir = tempfile::tempdir().unwrap();
            let c = fixture(dir.path(), 65, &[[0., 0.]]);
            let order = json_file(&c.plane_manifest.path)["source_order_sha256"]
                .as_str()
                .unwrap()
                .to_owned();
            let mut snapshot = neighborhood_body(&c.canonical).unwrap();
            let result = neighborhood_scan(&c, &mut snapshot, &order, |position, _, _, _| {
                if position == 0 {
                    let body = std::fs::read(&c.canonical.path)?;
                    match fault {
                        "same_bytes" => std::fs::write(&c.canonical.path, body)?,
                        "growth" => {
                            std::fs::OpenOptions::new()
                                .append(true)
                                .open(&c.canonical.path)?
                                .write_all(&[0])?;
                        }
                        "replace" | "symlink" => {
                            let original = dir.path().join("original");
                            std::fs::rename(&c.canonical.path, &original)?;
                            if fault == "symlink" {
                                std::os::unix::fs::symlink(original, &c.canonical.path)?;
                            } else {
                                std::fs::write(&c.canonical.path, body)?;
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(())
            });
            assert!(result.is_err(), "{fault}");
        }
    }

    #[test]
    fn source_neighborhood_checked_phase_and_staging_cap_before_body_open() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = fixture(dir.path(), 65, &[[0., 0.]]);
        let allocation = neighborhood_allocation(&c, 4096).unwrap();
        assert_eq!(
            allocation.peak_payload_bytes,
            *allocation.phases.values().max().unwrap()
        );
        assert_eq!(
            allocation.staged_peak_bytes,
            allocation.staged_input_bytes + 8 * 1024 * 1024
        );
        assert_eq!(
            allocation.proposed_charged_bytes,
            allocation.peak_payload_bytes + 320 * 1024 * 1024
        );
        assert_eq!(
            allocation.components["human_summary_capacity_and_formatting"],
            65536
        );
        c.max_payload_bytes = allocation.peak_payload_bytes;
        assert!(neighborhood_allocation(&c, 4096).is_ok());
        c.max_payload_bytes -= 1;
        for pin in [
            &c.generation_manifest,
            &c.plane_manifest,
            &c.router_root,
            &c.membership,
            &c.leaves,
            &c.canonical,
        ] {
            std::fs::remove_file(&pin.path).unwrap();
        }
        let pin = config_pin(&c);
        let error = execute_source_neighborhood(&pin.path, &pin.sha256)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("phase payload cap before body open"),
            "{error}"
        );
        assert!(!c.report.exists() && !c.anchor_seal.exists());
        assert!(neighborhood_product(&[u64::MAX, 2]).is_err());
        assert!(neighborhood_sum(&[u64::MAX, 1]).is_err());
        c.dimensions = usize::MAX;
        assert!(
            neighborhood_allocation(&c, 4096)
                .err()
                .unwrap()
                .to_string()
                .contains("overflow")
        );
    }

    fn edit_leaf_payload(
        c: &mut NeighborhoodConfig,
        edit: impl FnOnce(&mut borsuk::semantic_unit_router::Manifest, &mut Vec<u8>, &mut Vec<u8>),
    ) {
        let (router, _, _) = neighborhood_router(c).unwrap();
        let mut manifest = router.manifest().clone();
        let mut body = std::fs::read(&c.leaves.path).unwrap();
        let mut membership = std::fs::read(&c.membership.path).unwrap();
        edit(&mut manifest, &mut body, &mut membership);
        for leaf in &mut manifest.leaves {
            leaf.sha256 = digest(&body[leaf.offset..leaf.offset + leaf.bytes]);
        }
        c.leaves = put(&c.leaves.path, &body);
        c.membership = put(&c.membership.path, &membership);
        manifest.leaf_payload.sha256 = c.leaves.sha256.clone();
        manifest.membership.sha256 = c.membership.sha256.clone();
        c.router_root = put(&c.router_root.path, &manifest.encode().unwrap());
        rebind_descriptors(c);
    }

    #[test]
    fn source_neighborhood_leaf_roster_hash_finite_and_prototype_refusals() {
        for fault in [
            "duplicate",
            "out_of_range",
            "membership",
            "nan",
            "infinity",
            "prototype",
            "full_sha",
            "leaf_sha",
            "truncated",
            "trailing",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut c = fixture(dir.path(), 2049, &[[0., 0.]]);
            if matches!(fault, "full_sha" | "leaf_sha") {
                let (router, _, _) = neighborhood_router(&c).unwrap();
                let mut manifest = router.manifest().clone();
                if fault == "full_sha" {
                    c.leaves.sha256 = "0".repeat(64);
                    manifest.leaf_payload.sha256 = c.leaves.sha256.clone();
                } else {
                    manifest.leaves[0].sha256 = "0".repeat(64);
                }
                c.router_root = put(&c.router_root.path, &manifest.encode().unwrap());
                rebind_descriptors(&mut c);
            } else if matches!(fault, "truncated" | "trailing") {
                let mut body = std::fs::read(&c.leaves.path).unwrap();
                if fault == "truncated" {
                    body.pop();
                } else {
                    body.push(0);
                }
                std::fs::write(&c.leaves.path, body).unwrap();
            } else {
                edit_leaf_payload(&mut c, |manifest, body, _| match fault {
                    "duplicate" => {
                        let id = body[8..12].to_vec();
                        body[..4].copy_from_slice(&id);
                    }
                    "out_of_range" => body[..4].copy_from_slice(&999_u32.to_le_bytes()),
                    "membership" => body[..4].copy_from_slice(&64_u32.to_le_bytes()),
                    "nan" => body[4..6].copy_from_slice(&0x7e00_u16.to_le_bytes()),
                    "infinity" => body[4..6].copy_from_slice(&0x7c00_u16.to_le_bytes()),
                    "prototype" => manifest.leaves[0].prototype[0] = 0.25,
                    _ => unreachable!(),
                });
            }
            let (router, _, _) = neighborhood_router(&c).unwrap();
            let mut anchors = neighborhood_anchors(
                &c.generation_manifest.sha256,
                c.rows,
                c.anchor_count,
                c.dimensions,
            )
            .unwrap();
            assert!(
                neighborhood_leaves(&c, &router, &mut anchors).is_err(),
                "{fault}"
            );
        }
    }

    #[test]
    fn source_neighborhood_zero_centers_and_unweighted_partial_units() {
        for centers in [&[[0.0_f32, 0.], [0., 0.]][..], &[[1., 0.], [0., 0.]][..]] {
            let dir = tempfile::tempdir().unwrap();
            let mut c = fixture(dir.path(), 33, centers);
            let (router, _, _) = neighborhood_router(&c).unwrap();
            let mut anchors =
                neighborhood_anchors(&c.generation_manifest.sha256, 33, 2, 2).unwrap();
            neighborhood_leaves(&c, &router, &mut anchors).unwrap();
            assert_eq!(
                router.manifest().leaves[0].prototype,
                [centers[0][0] / 2., 0.]
            );
            let tail = anchors.iter().find(|a| a.unit == 1).unwrap();
            assert_eq!(tail.physical, 32);
            assert_eq!(tail.stored_sq8_fp16_unit_mean, [0., 0.]);
            if centers[0][0] != 0. {
                edit_leaf_payload(&mut c, |manifest, _, _| {
                    manifest.leaves[0].prototype[0] = 32. / 33.
                });
                let (router, _, _) = neighborhood_router(&c).unwrap();
                let mut anchors =
                    neighborhood_anchors(&c.generation_manifest.sha256, 33, 2, 2).unwrap();
                assert!(neighborhood_leaves(&c, &router, &mut anchors).is_err());
            }
        }
    }

    #[test]
    fn source_neighborhood_full_pipeline_coherent31_is_inconclusive() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = fixture(dir.path(), 65, &[[0., 0.]]);
        let mut body = std::fs::read(&c.canonical.path).unwrap();
        for row in body.chunks_exact_mut(16).skip(32) {
            row[8..12].copy_from_slice(&(-1_f32).to_le_bytes());
        }
        c.canonical = put(&c.canonical.path, &body);
        c.canonical_object_key = format!("semantic/objects/{}", c.canonical.sha256);
        rebind_descriptors(&mut c);
        let pin = config_pin(&c);
        let mut summary = Vec::new();
        assert!(
            execute_source_neighborhood_with(
                &pin.path,
                &pin.sha256,
                |file, _| file.sync_all(),
                |file, _| file.sync_all(),
                &mut summary,
            )
            .unwrap()
        );
        let seal = json_file(&c.anchor_seal);
        let report = json_file(&c.report);
        assert_eq!(summary, neighborhood_summary(&report).unwrap().as_bytes());
        assert!(summary.len() <= NEIGHBORHOOD_SUMMARY_CAP);
        let summary = String::from_utf8(summary).unwrap();
        assert!(summary.contains("EXPLORATORY / INCONCLUSIVE"));
        assert!(summary.contains("anchors 3; neighbors per anchor 32"));
        assert!(summary.contains("Both historical 312/320 KILLs preserved"));
        for absent in ["canonical_selected_unit_mean", "coordinates", "[", "]"] {
            assert!(!summary.contains(absent), "{absent}");
        }
        assert_eq!(
            report["anchor_seal_sha256"],
            digest(&std::fs::read(&c.anchor_seal).unwrap())
        );
        assert_eq!(seal["config_sha256"], pin.sha256);
        assert_eq!(seal["anchors"].as_array().unwrap().len(), 3);
        assert_ne!(report["canonical_sha256"], report["raw_source_sha256"]);
        assert_eq!(report["canonical_full_authenticated_passes"], 2);
        assert_eq!(report["report_alone_accepted"], false);
        assert_eq!(report["qualified"], false);
        assert_eq!(report["interpretation"], "EXPLORATORY_INCONCLUSIVE");
        let anchor = report["anchors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["unit"] == 0)
            .unwrap();
        let neighbors = anchor["neighbors"].as_array().unwrap();
        assert_eq!(
            neighbors.iter().filter(|n| n["own_unit"] == true).count(),
            31
        );
        assert_eq!(neighbors.len(), 32);
        assert_eq!(anchor["canonical_selected_unit_dispersion"], 0.);
        assert_eq!(anchor["unit_indivisibility"], "UNRESOLVED");
        assert_eq!(anchor["interpretation"], "INCONCLUSIVE");
        let self_id = anchor["id"].as_i64().unwrap();
        let ids = neighbors
            .iter()
            .map(|n| n["id"].as_i64().unwrap())
            .collect::<Vec<_>>();
        // Literal geometry: physical rows 0..31 have IDs 1..32; all others oppose.
        let expected = (1_i64..=32)
            .filter(|id| *id != self_id)
            .chain([0])
            .collect::<Vec<_>>();
        assert_eq!(ids, expected);
        assert!(neighbors[..31].iter().all(|n| n["cosine_distance"] == 0.));
        assert_eq!(neighbors[31]["cosine_distance"], 2.);
    }

    #[test]
    fn source_neighborhood_joint_top48_shared_page_and_independent_l2_rank() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = fixture(dir.path(), 100352, &[[0., 0.]]);
        // Leaves contain 64 units. Swap two units so leaf48 has one unit on
        // leaf0's page without changing unweighted zero prototypes or counts.
        edit_leaf_payload(&mut c, |_, body, membership| {
            body[..4].copy_from_slice(&3072_u32.to_le_bytes());
            body[3072 * 8..3072 * 8 + 4].copy_from_slice(&0_u32.to_le_bytes());
            membership[..4].copy_from_slice(&48_u32.to_le_bytes());
            membership[3072 * 4..3072 * 4 + 4].copy_from_slice(&0_u32.to_le_bytes());
        });
        let (router, _, _) = neighborhood_router(&c).unwrap();
        let mut selected =
            neighborhood_anchors(&c.generation_manifest.sha256, c.rows, 3, 2).unwrap();
        neighborhood_leaves(&c, &router, &mut selected).unwrap();
        let mut independent = router
            .manifest()
            .leaves
            .iter()
            .map(|leaf| {
                let x = 1.0_f64 - f64::from(leaf.prototype[0]);
                let y = f64::from(leaf.prototype[1]);
                (x * x + y * y, leaf.leaf_id)
            })
            .collect::<Vec<_>>();
        independent.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let expected = independent.iter().map(|p| p.1).collect::<Vec<_>>();
        assert_eq!(expected, (0..49).collect::<Vec<_>>());
        let normalized =
            borsuk::two_bit_generation::normalize_two_bit_diagnostic_query(&[1., 0.]).unwrap();
        assert_eq!(
            router
                .rank_leaves_by_metric(&normalized, &borsuk::VectorMetric::SquaredEuclidean)
                .unwrap(),
            expected
        );
        let mut anchor = NeighborhoodAnchor {
            unit: 1,
            physical: 32,
            unit_digest: [0; 32],
            row_digest: [0; 32],
            id: Some(33),
            coordinates: vec![1., 0.],
            norm2: 1.,
            canonical_unit_rows: vec![1., 0.],
            stored_sq8_fp16_unit_mean: vec![0., 0.],
        };
        c.neighbor_count = 3;
        let neighbors = [0, 3080 * 32, 2 * 32].map(|physical| NeighborhoodNeighbor {
            id: physical as i64 + 1,
            physical,
            distance: 0.,
        });
        let report = neighborhood_anchor_report(&c, &router, &anchor, &neighbors).unwrap();
        assert_eq!(report["outside_selected_leaf_union"], 2);
        assert_eq!(report["outside_physical_page_closure"], 1);
        assert_eq!(
            report["cross_leaf_and_outside_top48_and_outside_page_closure"],
            1
        );
        assert_eq!(report["neighbors"][0]["destination_leaf_l2_rank"], 49);
        assert_eq!(report["neighbors"][0]["inside_selected_leaf_union"], false);
        assert_eq!(report["neighbors"][0]["inside_physical_page_closure"], true);
        // A same-leaf outside48 miss must not combine with an unrelated inside48
        // cross-leaf neighbor to manufacture the three-way interaction.
        anchor.unit = 3074;
        anchor.physical = 3074 * 32;
        let report = neighborhood_anchor_report(&c, &router, &anchor, &neighbors).unwrap();
        assert_eq!(report["outside_physical_page_closure"], 1);
        assert_eq!(
            report["cross_leaf_and_outside_top48_and_outside_page_closure"],
            0
        );
        // Nonzero prototypes independently distinguish squared L2 from cosine.
        edit_leaf_payload(&mut c, |manifest, body, _| {
            for leaf in &mut manifest.leaves {
                let center = if leaf.leaf_id == 0 {
                    [0.25_f32, 0.25]
                } else {
                    [2., 0.]
                };
                leaf.prototype = center.to_vec();
                for record in body[leaf.offset..leaf.offset + leaf.bytes].chunks_exact_mut(8) {
                    for (word, value) in record[4..].chunks_exact_mut(2).zip(center) {
                        word.copy_from_slice(&half::f16::from_f32(value).to_bits().to_le_bytes());
                    }
                }
            }
        });
        let (router, _, _) = neighborhood_router(&c).unwrap();
        let mut selected =
            neighborhood_anchors(&c.generation_manifest.sha256, c.rows, 3, 2).unwrap();
        neighborhood_leaves(&c, &router, &mut selected).unwrap();
        let ranked = router
            .rank_leaves_by_metric(&normalized, &borsuk::VectorMetric::SquaredEuclidean)
            .unwrap();
        // d(leaf0)=0.75^2+0.25^2=0.625; all others d=1. Cosine reverses these.
        assert_eq!(ranked, (0..49).collect::<Vec<_>>());
    }

    #[test]
    fn source_neighborhood_seal_failure_stops_before_neighbors() {
        for directory in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let c = fixture(dir.path(), 65, &[[0., 0.]]);
            assert!(
                !run_with(
                    &c,
                    |file, is_directory| {
                        if is_directory == directory {
                            Err(io::Error::other("anchor seal injected sync failure"))
                        } else {
                            file.sync_all()
                        }
                    },
                    |file, _| file.sync_all()
                )
                .unwrap()
            );
            let report = json_file(&c.report);
            assert_eq!(report["status"], "INVALID");
            assert!(
                report["error"]
                    .as_str()
                    .unwrap()
                    .contains("anchor seal injected sync failure")
            );
            assert!(report.get("anchors").is_none());
            assert_eq!(json_file(&c.anchor_seal)["status"], "INVALID");
        }
        let dir = tempfile::tempdir().unwrap();
        let c = fixture(dir.path(), 65, &[[0., 0.]]);
        std::fs::write(&c.anchor_seal, b"existing").unwrap();
        assert!(!run_with(&c, |file, _| file.sync_all(), |file, _| file.sync_all()).unwrap());
        assert_eq!(std::fs::read(&c.anchor_seal).unwrap(), b"existing");
        assert_eq!(json_file(&c.report)["status"], "INVALID");
    }

    #[test]
    fn source_neighborhood_report_sync_and_final_recheck_failures() {
        for fault in [
            "file_sync",
            "directory_sync",
            "canonical_recheck",
            "generation_recheck",
            "seal_recheck",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let c = fixture(dir.path(), 65, &[[0., 0.]]);
            let result = run_with(
                &c,
                |file, _| file.sync_all(),
                |file, directory| {
                    if (fault == "file_sync" && !directory)
                        || (fault == "directory_sync" && directory)
                    {
                        return Err(io::Error::other("report injected sync failure"));
                    }
                    if !directory && fault.ends_with("_recheck") {
                        let path = match fault {
                            "canonical_recheck" => &c.canonical.path,
                            "generation_recheck" => &c.generation_manifest.path,
                            _ => &c.anchor_seal,
                        };
                        std::fs::write(path, std::fs::read(path)?)?;
                    }
                    file.sync_all()
                },
            );
            assert!(result.is_err(), "{fault}");
            assert_eq!(json_file(&c.report)["status"], "INVALID");
        }
    }

    #[test]
    fn source_neighborhood_publication_write_refusal() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("read-only-descriptor");
        std::fs::write(&output, b"").unwrap();
        let file = File::open(&output).unwrap();
        assert!(
            finish_selection_publication(
                &output,
                &json!({"status":"MEASURED"}),
                &[],
                parent(&output).unwrap(),
                file,
                |file, _| file.sync_all()
            )
            .is_err()
        );
        assert_ne!(
            std::fs::read(&output).unwrap(),
            b"{\"status\":\"MEASURED\"}\n"
        );
        struct BrokenSummary;
        impl Write for BrokenSummary {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("summary output refused"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let c = fixture(dir.path(), 65, &[[0., 0.]]);
        let pin = config_pin(&c);
        let error = execute_source_neighborhood_with(
            &pin.path,
            &pin.sha256,
            |file, _| file.sync_all(),
            |file, _| file.sync_all(),
            &mut BrokenSummary,
        )
        .unwrap_err();
        assert!(error.to_string().contains("summary output refused"));
        let published = json_file(&c.report);
        assert_eq!(published["status"], "MEASURED");
        assert_eq!(published["report_alone_accepted"], false);
    }

    #[test]
    fn source_neighborhood_native_crash_window_root_only() {
        use std::os::unix::process::ExitStatusExt;
        use std::process::{Child, Command, ExitStatus, Stdio};
        use std::time::{Duration, Instant};
        const ENV: &str = "BORSUK_SOURCE_NEIGHBORHOOD_CRASH_DIR";
        if let Some(dir) = std::env::var_os(ENV) {
            let dir = PathBuf::from(dir);
            let c = fixture(&dir, 65, &[[0., 0.]]);
            let _ = run_with(
                &c,
                |file, _| file.sync_all(),
                |_, directory| {
                    assert!(!directory);
                    let surviving = json_file(&c.report);
                    assert_eq!(surviving["status"], "MEASURED");
                    assert_eq!(surviving["complete"], true);
                    // Report bytes exist, but no report sync/rechecks have run.
                    std::fs::write(dir.join("at-report-sync"), b"ready")?;
                    let deadline = Instant::now() + Duration::from_secs(30);
                    while Instant::now() < deadline {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(io::Error::other("parent did not kill crash-window child"))
                },
            );
            panic!("crash-window child survived; not a successful crash falsifier");
        }
        struct OwnedChild(Child);
        impl OwnedChild {
            fn drain(&mut self) -> io::Result<ExitStatus> {
                let deadline = Instant::now() + Duration::from_secs(5);
                while self.0.try_wait()?.is_none() {
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "child drain timeout",
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                // try_wait observed termination; wait collects that original status.
                self.0.wait()
            }
        }
        impl Drop for OwnedChild {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.drain();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut child = OwnedChild(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "source_neighborhood_tests::source_neighborhood_native_crash_window_root_only",
                    "--test-threads=1",
                ])
                .env(ENV, dir.path())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(20);
        while !dir.path().join("at-report-sync").exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before crash window"
            );
            assert!(Instant::now() < deadline, "crash-window startup timeout");
            std::thread::sleep(Duration::from_millis(10));
        }
        let report_path = dir.path().join("report.json");
        let before = std::fs::read(&report_path).unwrap();
        child.0.kill().unwrap();
        let status = child.drain().unwrap();
        assert_eq!(status.signal(), Some(9));
        assert!(
            !status.success(),
            "external exit receipt must reject this run"
        );
        let surviving = std::fs::read(&report_path).unwrap();
        assert_eq!(surviving, before);
        let report: Value = serde_json::from_slice(&surviving).unwrap();
        assert_eq!(report["status"], "MEASURED");
        assert_eq!(report["complete"], true);
        assert_eq!(report["report_alone_accepted"], false);
        assert_eq!(report["external_gate_required"], true);
        assert!(
            report["acceptance_requires"]
                .as_str()
                .unwrap()
                .contains("original exit0")
        );
        assert_eq!(
            report["anchor_seal_sha256"],
            digest(&std::fs::read(dir.path().join("anchor-seal.json")).unwrap())
        );
    }

    #[test]
    fn source_neighborhood_literal_hash_domains_and_partial_unit() {
        // Independent literal messages: no production domain or packing helper.
        let unit_message = [
            b"borsuk-source-neighborhood-unit-v1\0".as_slice(),
            &[0; 32],
            &[1, 0, 0, 0, 0, 0, 0, 0],
        ]
        .concat();
        let row_message = [
            b"borsuk-source-neighborhood-row-v1\0".as_slice(),
            &[0; 32],
            &[1, 0, 0, 0, 0, 0, 0, 0],
        ]
        .concat();
        // Independently authored fixed-message SHA256 goldens (Python hashlib),
        // not values emitted by the implementation under test.
        assert_eq!(
            digest(&unit_message),
            "2eab700a2d49ae2703249f0ee4d4a4522ed9b125453c17d7889467180e75fe9b"
        );
        assert_eq!(
            digest(&row_message),
            "b925589eedc215826bcaeb73603e70873f69d4f6ac8ac0f6ec5b978464a9adf6"
        );
        let unit_golden: [u8; 32] = Sha256::digest(unit_message).into();
        let row_golden: [u8; 32] = Sha256::digest(row_message).into();
        assert_eq!(
            neighborhood_digest(NEIGHBORHOOD_UNIT_DOMAIN, &[0; 32], 1),
            unit_golden
        );
        assert_eq!(
            neighborhood_digest(NEIGHBORHOOD_ROW_DOMAIN, &[0; 32], 1),
            row_golden
        );
        let anchors = neighborhood_anchors(&"0".repeat(64), 39, 2, 2).unwrap();
        let tail = anchors.iter().find(|a| a.unit == 1).unwrap();
        // LE 0x8215c2ed9e5825b9 mod 7 = 3 (big endian would give 2).
        assert_eq!(tail.physical, 35);
        assert!(tail.physical < 39);
        let mut expected = (0..9)
            .map(|unit| {
                let literal = [
                    b"borsuk-source-neighborhood-unit-v1\0".as_slice(),
                    &[0; 32],
                    &(unit as u64).to_le_bytes(),
                ]
                .concat();
                (<[u8; 32]>::from(Sha256::digest(literal)), unit)
            })
            .collect::<Vec<_>>();
        expected.sort();
        let selected = neighborhood_anchors(&"0".repeat(64), 257, 3, 2).unwrap();
        assert_eq!(
            selected.iter().map(|a| a.unit).collect::<Vec<_>>(),
            expected[..3].iter().map(|p| p.1).collect::<Vec<_>>()
        );
    }

    #[test]
    fn source_neighborhood_scalar_actual_norm_ties_and_self() {
        let anchor = [1.0_f32, 0.0];
        // Physical order deliberately differs from logical IDs; duplicate coordinates stay.
        let rows = [
            (4, [1., 0.]),
            (1, [0.6, 0.8]),
            (3, [1., 0.]),
            (0, [0., 1.]),
            (2, [-1., 0.]),
        ];
        let mut heap = std::collections::BinaryHeap::with_capacity(4);
        for (position, (id, row)) in rows.iter().enumerate() {
            neighborhood_offer(
                &mut heap,
                4,
                4,
                &anchor,
                1.0,
                position,
                *id,
                row,
                neighborhood_norm(row).unwrap(),
            );
        }
        let neighbors = heap.into_sorted_vec();
        assert_eq!(
            neighbors.iter().map(|n| n.id).collect::<Vec<_>>(),
            [3, 1, 0, 2]
        );
        assert_eq!(neighbors[0].distance.to_bits(), 0.0_f64.to_bits());
        let x = f64::from(0.6_f32);
        let y = f64::from(0.8_f32);
        assert_eq!(neighbors[1].distance, 1.0 - x / (x * x + y * y).sqrt());
        assert_eq!(neighbors[2].distance, 1.0);
        assert_eq!(neighbors[3].distance, 2.0);
        let near = [f32::from_bits(1.0_f32.to_bits() + 1), 0.0];
        let norm = neighborhood_norm(&near).unwrap();
        assert_eq!(neighborhood_cosine(&anchor, 1.0, &near, norm), 0.0);
        let over_tolerance = [f32::from_bits(1.0_f32.to_bits() + 5), 0.0];
        assert!(neighborhood_norm(&over_tolerance).is_err());
        // Four 2^-54 terms round away individually after 1; summing them first
        // changes f64 bits. This independently freezes ascending accumulation.
        let tiny = f32::from_bits(100 << 23); // 2^-27
        let rounding = [1., tiny, tiny, tiny, tiny];
        let opposing = [-1., tiny, tiny, tiny, tiny];
        assert_eq!(neighborhood_norm(&rounding).unwrap(), 1.);
        assert_eq!(neighborhood_cosine(&rounding, 1., &opposing, 1.), 2.);
        for row in [[0., 0.], [2., 0.], [f32::NAN, 0.], [f32::INFINITY, 0.]] {
            assert!(neighborhood_norm(&row).is_err());
        }
        let mut tied = std::collections::BinaryHeap::with_capacity(2);
        for id in [9, 7, 3, 5] {
            neighborhood_offer(&mut tied, 2, 99, &anchor, 1., id, id as i64, &anchor, 1.);
        }
        assert_eq!(
            tied.into_sorted_vec()
                .iter()
                .map(|n| n.id)
                .collect::<Vec<_>>(),
            [3, 5]
        );
    }

    #[test]
    fn source_neighborhood_canonical_dispersion_zero_mean() {
        let (mean, dispersion) = neighborhood_canonical_statistics(&[1., 0., 1., 0.], 2);
        assert_eq!(mean, [1., 0.]);
        assert_eq!(dispersion, 0.);
        let (mean, dispersion) = neighborhood_canonical_statistics(&[1., 0., -1., 0.], 2);
        assert_eq!(mean, [0., 0.]);
        assert_eq!(dispersion, 1.);
    }
}

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    if args.get(1).is_some_and(|a| a == "--fp32-serving") {
        let result = if args.len() == 5 {
            args[3]
                .to_str()
                .ok_or_else(|| "FP32 config SHA UTF-8".into())
                .and_then(|sha| {
                    execute_report(Path::new(&args[4]), fp32_serving::REPORT_SCHEMA, || {
                        fp32_serving::reduce(Path::new(&args[2]), sha)
                    })
                })
        } else {
            Err("usage: compare_native_replay --fp32-serving COMPLETED_CONFIG CONFIG_SHA NEW_REPORT".into())
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(error) => {
                eprintln!("INVALID: {error}");
                std::process::exit(2);
            }
        }
        return;
    }
    if args.get(1).is_some_and(|a| a == "--f32-top16") {
        let result = if args.len() == 5 {
            args[3]
                .to_str()
                .ok_or_else(|| "f32-top16 SHA UTF-8".into())
                .and_then(|sha| {
                    execute_report(Path::new(&args[4]), f32_top16::REPORT_SCHEMA, || {
                        f32_top16::reduce(Path::new(&args[2]), sha)
                    })
                })
        } else {
            Err("usage: compare_native_replay --f32-top16 CONFIG CONFIG_SHA NEW_REPORT".into())
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(e) => {
                eprintln!("INVALID: {e}");
                std::process::exit(2);
            }
        }
        return;
    }
    if args.get(1).is_some_and(|a| a == "--sq8-cosine") {
        let result = if args.len() == 5 {
            args[3]
                .to_str()
                .ok_or_else(|| "sq8-cosine SHA UTF-8".into())
                .and_then(|sha| {
                    execute_report(Path::new(&args[4]), sq8_cosine::REPORT_SCHEMA, || {
                        sq8_cosine::reduce(Path::new(&args[2]), sha)
                    })
                })
        } else {
            Err("usage: compare_native_replay --sq8-cosine CONFIG CONFIG_SHA NEW_REPORT".into())
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2);
            }
        }
        return;
    }
    if args.get(1).is_some_and(|a| a == "--fetched-f32") {
        let result = if args.len() == 5 {
            args[3]
                .to_str()
                .ok_or_else(|| "fetched-f32 SHA UTF-8".into())
                .and_then(|sha| {
                    execute_report(Path::new(&args[4]), fetched_f32::REPORT_SCHEMA, || {
                        fetched_f32::reduce(Path::new(&args[2]), sha)
                    })
                })
        } else {
            Err("usage: compare_native_replay --fetched-f32 CONFIG CONFIG_SHA NEW_REPORT".into())
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2);
            }
        }
        return;
    }
    if args
        .get(1)
        .is_some_and(|a| a == "--source-frame-range" || a == "--source-frame-range-admit")
    {
        let result = if args.len() == 5 {
            args[3]
                .to_str()
                .ok_or_else(|| "range config SHA UTF-8".into())
                .and_then(|sha| {
                    execute_report(
                        Path::new(&args[4]),
                        source_frame_range::REPORT_SCHEMA,
                        || {
                            source_frame_range::reduce(
                                Path::new(&args[2]),
                                sha,
                                args[1] == "--source-frame-range-admit",
                            )
                        },
                    )
                })
        } else {
            Err(
                "usage: compare_native_replay (--source-frame-range|--source-frame-range-admit) CONFIG CONFIG_SHA NEW_REPORT"
                    .into(),
            )
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2);
            }
        }
        return;
    }

    if args.get(1).is_some_and(|a| a == "--sq8-split-panel") {
        let result = if args.len() == 5 {
            args[3]
                .to_str()
                .ok_or_else(|| "panel config SHA UTF-8".into())
                .and_then(|sha| {
                    execute_report(Path::new(&args[4]), sq8_split_panel::REPORT_SCHEMA, || {
                        sq8_split_panel::reduce(Path::new(&args[2]), sha)
                    })
                })
        } else {
            Err(
                "usage: compare_native_replay --sq8-split-panel CONFIG CONFIG_SHA NEW_REPORT"
                    .into(),
            )
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2);
            }
        }
        return;
    }
    if args.get(1).is_some_and(|a| a == "--sq8-panel-attribution") {
        let result = if args.len() == 5 {
            args[3]
                .to_str()
                .ok_or_else(|| "panel config SHA UTF-8".into())
                .and_then(|sha| {
                    execute_report(
                        Path::new(&args[4]),
                        sq8_panel_attribution::REPORT_SCHEMA,
                        || sq8_panel_attribution::reduce(Path::new(&args[2]), sha),
                    )
                })
        } else {
            Err(
                "usage: compare_native_replay --sq8-panel-attribution CONFIG CONFIG_SHA NEW_REPORT"
                    .into(),
            )
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2);
            }
        }
        return;
    }
    if args.get(1).is_some_and(|a| a == "--prepare-v9-fixture") {
        let result = if args.len() == 6 {
            args[3]
                .to_str()
                .ok_or_else(|| "v9 fixture config SHA UTF-8".into())
                .and_then(|sha| {
                    v9_fixture::execute(
                        Path::new(&args[2]),
                        sha,
                        Path::new(&args[4]),
                        Path::new(&args[5]),
                    )
                })
        } else {
            Err("usage: compare_native_replay --prepare-v9-fixture CONFIG CONFIG_SHA NEW_DIRECTORY NEW_REPORT".into())
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2);
            }
        }
        return;
    }
    if args.get(1).is_some_and(|a| {
        a == "--sq8-shortlist-get-curve"
            || a == "--sq8-shortlist-get-curve-admit"
            || a == "--sq8-shortlist-get-curve-smoke"
    }) {
        let admit = args[1] == "--sq8-shortlist-get-curve-admit";
        let smoke = args[1] == "--sq8-shortlist-get-curve-smoke";
        let result = if args.len() == 5 {
            args[3]
                .to_str()
                .ok_or_else(|| "GET curve config SHA UTF-8".into())
                .and_then(|sha| {
                    sq8_subpage_shortlist::execute_get_curve(
                        std::path::Path::new(&args[2]),
                        sha,
                        std::path::Path::new(&args[4]),
                        admit,
                        smoke,
                    )
                })
        } else {
            Err("usage: compare_native_replay --sq8-shortlist-get-curve[-admit|-smoke] CONFIG CONFIG_SHA NEW_REPORT".into())
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2)
            }
        }
        return;
    }
    if args.get(1).is_some_and(|a| {
        a == "--sq8-shortlist-count-curve"
            || a == "--sq8-shortlist-count-curve-admit"
            || a == "--sq8-shortlist-count-curve-smoke"
    }) {
        let admit = args[1] == "--sq8-shortlist-count-curve-admit";
        let smoke = args[1] == "--sq8-shortlist-count-curve-smoke";
        let result = if args.len() == 5 {
            args[3]
                .to_str()
                .ok_or_else(|| "count curve config SHA UTF-8".into())
                .and_then(|sha| {
                    sq8_subpage_shortlist::execute_count_curve(
                        std::path::Path::new(&args[2]),
                        sha,
                        std::path::Path::new(&args[4]),
                        admit,
                        smoke,
                    )
                })
        } else {
            Err("usage: compare_native_replay --sq8-shortlist-count-curve[-admit|-smoke] CONFIG CONFIG_SHA NEW_REPORT".into())
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2)
            }
        }
        return;
    }
    if args.get(1).is_some_and(|a| {
        a == "--sq8-virtual-layout"
            || a == "--sq8-virtual-layout-admit"
            || a == "--sq8-virtual-layout-smoke"
    }) {
        let admit = args[1] == "--sq8-virtual-layout-admit";
        let smoke = args[1] == "--sq8-virtual-layout-smoke";
        let result = if args.len() == 5 {
            args[3]
                .to_str()
                .ok_or_else(|| "virtual layout config SHA UTF-8".into())
                .and_then(|sha| {
                    sq8_subpage_shortlist::execute_virtual_layout(
                        std::path::Path::new(&args[2]),
                        sha,
                        std::path::Path::new(&args[4]),
                        admit,
                        smoke,
                    )
                })
        } else {
            Err("usage: compare_native_replay --sq8-virtual-layout[-admit] CONFIG CONFIG_SHA NEW_REPORT".into())
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2)
            }
        }
        return;
    }

    if args
        .get(1)
        .is_some_and(|arg| arg == "--sq8-block-admission" || arg == "--sq8-block-admission-admit")
    {
        let admission_only = args[1] == "--sq8-block-admission-admit";
        let result = if args.len() == 5 {
            args[3]
                .to_str()
                .ok_or_else(|| "block admission config SHA UTF-8".into())
                .and_then(|sha| {
                    sq8_subpage_shortlist::execute_block_admission(
                        std::path::Path::new(&args[2]),
                        sha,
                        std::path::Path::new(&args[4]),
                        admission_only,
                    )
                })
        } else {
            Err("usage: compare_native_replay --sq8-block-admission[-admit] CONFIG CONFIG_SHA NEW_REPORT".into())
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2)
            }
        }
        return;
    }

    if args
        .get(1)
        .is_some_and(|arg| arg == "--sq8-shortlist-geometry")
    {
        let result = if args.len() == 5 {
            args[3]
                .to_str()
                .ok_or_else(|| "geometry config SHA UTF-8".into())
                .and_then(|sha| {
                    sq8_subpage_shortlist::execute_geometry(
                        std::path::Path::new(&args[2]),
                        sha,
                        std::path::Path::new(&args[4]),
                    )
                })
        } else {
            Err("usage: compare_native_replay --sq8-shortlist-geometry CONFIG CONFIG_SHA NEW_REPORT".into())
        };
        match result {
            Ok(true) => (),
            Ok(false) => std::process::exit(2),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2)
            }
        }
        return;
    }

    let binding = args
        .get(1)
        .is_some_and(|arg| arg == "--bind-completed-scale");
    let result = (|| -> Result<bool> {
        if args
            .get(1)
            .is_some_and(|arg| arg == "--sq8-subpage-shortlist-admit")
        {
            require(
                args.len() == 5,
                "usage: compare_native_replay --sq8-subpage-shortlist-admit CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            return sq8_subpage_shortlist::execute_admission(
                Path::new(&args[2]),
                args[3]
                    .to_str()
                    .ok_or("shortlist admission config SHA encoding")?,
                Path::new(&args[4]),
            );
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--sq8-subpage-shortlist")
        {
            require(
                args.len() == 5,
                "usage: compare_native_replay --sq8-subpage-shortlist CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            return sq8_subpage_shortlist::execute(
                Path::new(&args[2]),
                args[3].to_str().ok_or("shortlist config SHA encoding")?,
                Path::new(&args[4]),
            );
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--capsule-handoff-abba")
        {
            require(
                args.len() == 5,
                "usage: compare_native_replay --capsule-handoff-abba CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            return execute_report(
                Path::new(&args[4]),
                current_v10::HANDOFF_REPORT_SCHEMA,
                || {
                    current_v10::capsule_handoff_abba(
                        Path::new(&args[2]),
                        args[3]
                            .to_str()
                            .ok_or("capsule handoff config SHA encoding")?,
                    )
                },
            );
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--paired-source-layout")
        {
            require(
                args.len() == 5,
                "usage: compare_native_replay --paired-source-layout CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            return execute_report(
                Path::new(&args[4]),
                "borsuk-paired-source-layout-v1",
                || {
                    current_v10::paired_source_layout(
                        Path::new(&args[2]),
                        args[3]
                            .to_str()
                            .ok_or("paired SOURCE config SHA encoding")?,
                    )
                },
            );
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--paired-returned-panel")
        {
            require(
                args.len() == 5,
                "usage: compare_native_replay --paired-returned-panel CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            return paired_returned_panel::execute(
                Path::new(&args[2]),
                args[3]
                    .to_str()
                    .ok_or("paired returned config SHA encoding")?,
                Path::new(&args[4]),
            );
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--prepare-spatial-publication")
        {
            require(
                args.len() == 6,
                "usage: compare_native_replay --prepare-spatial-publication CONFIG CONFIG_SHA AUTHORITY AUTHORITY_SHA",
            )?;
            return paired_partition_coverage::prepare(
                Path::new(&args[2]),
                args[3].to_str().ok_or("spatial config SHA encoding")?,
                Path::new(&args[4]),
                args[5].to_str().ok_or("spatial authority SHA encoding")?,
            );
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--paired-partition-source-coverage")
        {
            require(
                args.len() == 8,
                "usage: compare_native_replay --paired-partition-source-coverage CONFIG CONFIG_SHA AUTHORITY AUTHORITY_SHA NEW_SELECTION NEW_REPORT",
            )?;
            return paired_partition_coverage::execute(
                Path::new(&args[2]),
                args[3].to_str().ok_or("paired config SHA encoding")?,
                Path::new(&args[4]),
                args[5].to_str().ok_or("paired authority SHA encoding")?,
                Path::new(&args[6]),
                Path::new(&args[7]),
            );
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--partition-reconstruction")
        {
            require(
                args.len() == 6,
                "usage: compare_native_replay --partition-reconstruction CONFIG CONFIG_SHA APPROVAL APPROVAL_SHA",
            )?;
            return partition_reconstruction::execute(
                Path::new(&args[2]),
                args[3]
                    .to_str()
                    .ok_or("reconstruction config SHA encoding")?,
                Path::new(&args[4]),
                args[5]
                    .to_str()
                    .ok_or("reconstruction approval SHA encoding")?,
            );
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--source-neighborhood")
        {
            require(
                args.len() == 4,
                "usage: compare_native_replay --source-neighborhood CONFIG CONFIG_SHA256",
            )?;
            return execute_source_neighborhood(
                Path::new(&args[2]),
                args[3].to_str().ok_or("neighborhood config SHA encoding")?,
            );
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--leaf-boundary-coverage")
        {
            require(
                args.len() == 7,
                "usage: compare_native_replay --leaf-boundary-coverage CONFIG CONFIG_SHA NEW_ADJACENCY NEW_SELECTION NEW_REPORT",
            )?;
            return execute_leaf_coverage_as(
                Path::new(&args[6]),
                BOUNDARY_SCHEMA,
                || {
                    reduce_leaf_coverage_candidate_with(
                        Path::new(&args[2]),
                        args[3].to_str().ok_or("coverage config SHA encoding")?,
                        Path::new(&args[5]),
                        Some(Path::new(&args[4])),
                        &CoverageScope::consumed32(),
                        |file, _| file.sync_all(),
                        loss_binary,
                    )
                },
                |file, _| file.sync_all(),
            );
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--leaf-metric-coverage")
        {
            require(
                args.len() == 6,
                "usage: compare_native_replay --leaf-metric-coverage CONFIG CONFIG_SHA NEW_SELECTION_SEAL NEW_REPORT",
            )?;
            return execute_leaf_coverage(
                Path::new(&args[2]),
                args[3].to_str().ok_or("coverage config SHA encoding")?,
                Path::new(&args[4]),
                Path::new(&args[5]),
            );
        }
        if args.get(1).is_some_and(|arg| arg == "--sealed-scale-loss") {
            require(
                args.len() == 5,
                "usage: compare_native_replay --sealed-scale-loss CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            return execute_report(Path::new(&args[4]), LOSS_SCHEMA, || {
                reduce_loss(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("loss config SHA encoding")?,
                )
            });
        }
        if binding {
            require(
                args.len() == 5,
                "usage: compare_native_replay --bind-completed-scale AUTHORITY AUTHORITY_SHA NEW_CONFIG",
            )?;
            let output = Path::new(&args[4]);
            header_path(output)?;
            let config = bind_completed_scale(
                Path::new(&args[2]),
                args[3].to_str().ok_or("authority SHA256 encoding")?,
            )?;
            return publish_bound_headers(output, config);
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--source-intervals-scale")
        {
            require(
                args.len() == 5,
                "usage: compare_native_replay --source-intervals-scale CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            return execute_report(Path::new(&args[4]), SOURCE_INTERVAL_SCHEMA, || {
                reduce_source_intervals(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("interval config SHA encoding")?,
                )
            });
        }
        if args.get(1).is_some_and(|arg| arg == "--source-cover-scale") {
            require(
                args.len() == 5,
                "usage: compare_native_replay --source-cover-scale CONFIG CONFIG_SHA256 NEW_REPORT_JSON",
            )?;
            return execute_report(Path::new(&args[4]), SOURCE_COVER_CURVE_SCHEMA, || {
                reduce_source_cover_curve(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("config SHA256 encoding")?,
                )
            });
        }
        if args.get(1).is_some_and(|arg| arg == "--source-utilization") {
            require(
                args.len() == 5,
                "usage: compare_native_replay --source-utilization CONFIG CONFIG_SHA256 NEW_REPORT_JSON",
            )?;
            return execute_report(Path::new(&args[4]), SOURCE_UTILIZATION_SCHEMA, || {
                reduce_source_utilization(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("config SHA256 encoding")?,
                )
            });
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--source-budget-abba-scale")
        {
            require(
                args.len() == 5,
                "usage: compare_native_replay --source-budget-abba-scale CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            return execute_report(Path::new(&args[4]), SOURCE_BUDGET_ABBA_SCHEMA, || {
                reduce_source_budget_abba(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("SOURCE config SHA encoding")?,
                )
            });
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--byte-window-abba-scale")
        {
            require(
                args.len() == 5,
                "usage: compare_native_replay --byte-window-abba-scale CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            return execute_report(Path::new(&args[4]), BYTE_WINDOW_ABBA_SCHEMA, || {
                reduce_byte_window_abba(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("byte-window config SHA encoding")?,
                )
            });
        }
        if args.get(1).is_some_and(|arg| arg == "--membership-abba-v2") {
            require(
                args.len() == 5,
                "usage: compare_native_replay --membership-abba-v2 CONFIG CONFIG_SHA256 NEW_OUTPUT_JSON",
            )?;
            return execute_report(Path::new(&args[4]), MEMBERSHIP_ABBA_SCHEMA, || {
                reduce_membership_abba(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("config SHA256 encoding")?,
                )
            });
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--paired-scheduler-scale")
        {
            require(
                args.len() == 5,
                "usage: compare_native_replay --paired-scheduler-scale CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            let config_path = Path::new(&args[2]);
            let config_sha = args[3].to_str().ok_or("config SHA encoding")?;
            let (config, _): (PairedConfig, _) = read_config(config_path, config_sha)?;
            require(
                config.schema == "borsuk-scale-scheduler-native-reduction-config-v1",
                "scheduler config schema",
            )?;
            return execute_report(Path::new(&args[4]), SCALE_SCHEDULER_SCHEMA, || {
                reduce_paired_scope(config_path, config_sha, true)
            });
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--paired-width64-scale" || arg == "--paired-width128-scale")
        {
            let width128 = args[1] == "--paired-width128-scale";
            require(
                args.len() == 5,
                if width128 {
                    "usage: compare_native_replay --paired-width128-scale CONFIG CONFIG_SHA NEW_REPORT"
                } else {
                    "usage: compare_native_replay --paired-width64-scale CONFIG CONFIG_SHA NEW_REPORT"
                },
            )?;
            let config_path = Path::new(&args[2]);
            let config_sha = args[3].to_str().ok_or("config SHA encoding")?;
            let (config, _): (PairedConfig, _) = read_config(config_path, config_sha)?;
            require(
                config.schema
                    == if width128 {
                        "borsuk-scale-width128-native-reduction-config-v1"
                    } else {
                        "borsuk-scale-width64-native-reduction-config-v1"
                    },
                if width128 {
                    "width128 config schema"
                } else {
                    "width64 config schema"
                },
            )?;
            return execute_report(
                Path::new(&args[4]),
                if width128 {
                    SCALE_WIDTH128_SCHEMA
                } else {
                    SCALE_WIDTH64_SCHEMA
                },
                || reduce_paired_scope(config_path, config_sha, true),
            );
        }
        if args.get(1).is_some_and(|arg| arg == "--paired-scale") {
            require(
                args.len() == 5,
                "usage: compare_native_replay --paired-scale CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            let (config, _): (PairedConfig, _) = read_config(
                Path::new(&args[2]),
                args[3].to_str().ok_or("config SHA encoding")?,
            )?;
            require(
                config.schema == "borsuk-scale-paired-native-reduction-config-v1",
                "paired config schema",
            )?;
            return execute_report(Path::new(&args[4]), SCALE_PAIRED_SCHEMA, || {
                reduce_paired_scope(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("config SHA encoding")?,
                    true,
                )
            });
        }
        if args.get(1).is_some_and(|arg| arg == "--paired-v2") {
            require(
                args.len() == 5,
                "usage: compare_native_replay --paired-v2 CONFIG CONFIG_SHA256 NEW_OUTPUT_JSON",
            )?;
            return execute_report(Path::new(&args[4]), PAIRED_SCHEMA, || {
                reduce_paired(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("config SHA256 encoding")?,
                )
            });
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--scale-prefix-parity")
        {
            require(
                args.len() == 5,
                "usage: compare_native_replay --scale-prefix-parity CONFIG CONFIG_SHA256 NEW_REPORT_JSON",
            )?;
            return execute_report(Path::new(&args[4]), SCALE_PARITY_REPORT_SCHEMA, || {
                reduce_scale_prefix_parity(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("config SHA256 encoding")?,
                )
            });
        }
        if args.get(1).is_some_and(|arg| arg == "--completed-scale") {
            require(
                args.len() == 5,
                "usage: compare_native_replay --completed-scale CONFIG CONFIG_SHA256 NEW_OUTPUT_JSON",
            )?;
            return execute_report(Path::new(&args[4]), SCALE_REPORT_SCHEMA, || {
                reduce_scale(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("config SHA256 encoding")?,
                )
            });
        }
        if args.get(1).is_some_and(|arg| arg == "--completed-v9-panel") {
            require(
                args.len() == 5,
                "usage: compare_native_replay --completed-v9-panel CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            return execute_report(Path::new(&args[4]), v9_panel::REPORT_SCHEMA, || {
                v9_panel::reduce(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("config SHA encoding")?,
                )
            });
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--v9-historical-count-parity")
        {
            require(
                args.len() == 5,
                "usage: compare_native_replay --v9-historical-count-parity CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            return execute_report(Path::new(&args[4]), v9_panel::PARITY_SCHEMA, || {
                v9_panel::reduce_parity(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("config SHA encoding")?,
                )
            });
        }
        if args
            .get(1)
            .is_some_and(|arg| arg == "--v9-source-cover-oracle")
        {
            require(
                args.len() == 5,
                "usage: compare_native_replay --v9-source-cover-oracle CONFIG CONFIG_SHA NEW_REPORT",
            )?;
            return execute_report(Path::new(&args[4]), v9_panel::ORACLE_SCHEMA, || {
                v9_panel::reduce_oracle(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("config SHA encoding")?,
                )
            });
        }
        if args.get(1).is_some_and(|arg| arg == "--completed-v2") {
            require(
                args.len() == 5,
                "usage: compare_native_replay --completed-v2 CONFIG CONFIG_SHA256 NEW_OUTPUT_JSON",
            )?;
            return execute_report(Path::new(&args[4]), COMPLETED_SCHEMA, || {
                reduce_completed(
                    Path::new(&args[2]),
                    args[3].to_str().ok_or("config SHA256 encoding")?,
                )
            });
        }
        require(
            args.len() == 10,
            "usage: compare_native_replay A1 SHA256 B1 SHA256 B2 SHA256 A2 SHA256 NEW_OUTPUT_JSON",
        )?;
        let mut inputs = Vec::with_capacity(4);
        for pair in args[1..9].chunks_exact(2) {
            inputs.push((
                PathBuf::from(&pair[0]),
                pair[1].to_str().ok_or("SHA256 encoding")?.to_owned(),
            ));
        }
        let inputs = inputs
            .try_into()
            .map_err(|_| "four result pairs required")?;
        execute(&inputs, Path::new(&args[9]))
    })();
    match result {
        Ok(true) => (),
        Ok(false) => std::process::exit(if binding { 98 } else { 2 }),
        Err(e) => {
            eprintln!("INVALID: {e}; external gates required");
            std::process::exit(if binding { 98 } else { 2 });
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
#[path = "../src/bin/check_cohere_native_baseline.rs"]
mod scale_native_runner;

#[cfg(test)]
mod tests {
    // Current producer scores cosine; legacy diagnostic controls explicitly require L2 bits.
    pub(super) fn legacy_l2_fixture(dir: &Path, result: &Path) {
        use borsuk::{
            exact_sq8_nominee::Sq8Geometry,
            returned_sq8::{ReturnedRange, rank_returned_ranges},
            two_bit_generation::normalize_two_bit_diagnostic_query,
        };
        let original = std::fs::read_to_string(result).unwrap();
        let mut rows = original
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let dimensions = rows[1]["dimensions"].as_u64().unwrap() as usize;
        let population = rows[1]["rows"].as_u64().unwrap() as usize;
        let k = rows[1]["k"].as_u64().unwrap() as usize;
        let body = std::fs::read(dir.join("sq8")).unwrap();
        let requests = std::fs::read(dir.join("requests.f32")).unwrap();
        let generation: Value =
            serde_json::from_slice(&std::fs::read(dir.join("generation/manifest.json")).unwrap())
                .unwrap();
        let low: Vec<f32> = serde_json::from_value(generation["low"].clone()).unwrap();
        let step: Vec<f32> = serde_json::from_value(generation["step"].clone()).unwrap();
        for q in rows.iter_mut().filter(|q| q["phase"] == "query") {
            let ordinal = q["ordinal"].as_u64().unwrap() as usize;
            let mut query = vec![0f32; dimensions];
            super::fetched_f32::floats(
                &requests[ordinal * dimensions * 4..(ordinal + 1) * dimensions * 4],
                &mut query,
            )
            .unwrap();
            let query = normalize_two_bit_diagnostic_query(&query).unwrap();
            let ranges = q["plan"]["ranges"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    let start = r[0].as_u64().unwrap() as usize;
                    let end = r[1].as_u64().unwrap() as usize;
                    ReturnedRange {
                        start,
                        bytes: &body[start..end],
                    }
                })
                .collect::<Vec<_>>();
            let scores = rank_returned_ranges(
                Sq8Geometry {
                    rows: population,
                    dimensions,
                },
                &ranges,
                &query,
                &low,
                &step,
                k,
                body.len(),
            )
            .unwrap();
            for (j, n) in scores.iter().enumerate() {
                assert_eq!(q["returned"][j]["id"], n.id);
                assert_eq!(q["returned_physical_ordinals"][j], n.ordinal);
                q["returned"][j]["score_bits"] = json!(n.score.to_bits());
            }
        }
        authenticate(&mut rows);
        std::fs::write(result, encode(&rows)).unwrap();
    }

    use super::*;
    use serde_json::json;
    use std::os::unix::{ffi::OsStringExt, fs::symlink};

    fn sha(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn source_fixture() -> Vec<Value> {
        let mut rows = v2_fixture();
        rows[0]["fetch_parallelism"] = json!(32);
        rows[1]["fetch_parallelism"] = json!(32);
        rows[1]["source_cache"] = json!("off");
        rows[1]["rows"] = json!(512);
        rows[1]["query_source_first"] = json!(512);
        for row in &mut rows[5..1005] {
            row["trace"] = json!({"ranked_candidate_pages":[1,0],"primary_page":1,
                "discoveries":[],"semantic_leaves":[0],"semantic_units":[0,8],
                "semantic_seed_additions":[1,2,3,4,5,6,7],
                "nomination_evaluated_units":(0..16).collect::<Vec<_>>()});
            row["charges"]["source"] =
                json!({"submitted_gets":1,"verified_bytes":6144,"failed_gets":0});
            row["charges"]["sq8"] =
                json!({"submitted_gets":1,"verified_bytes":7168,"failed_gets":0});
            row["sum"] = json!({"submitted_gets":3,"verified_bytes":13412,"failed_gets":0});
        }
        let terminal = &mut rows.last_mut().unwrap()["summary"];
        terminal["charges"]["source"] =
            json!({"submitted_gets":1000,"verified_bytes":6144000,"failed_gets":0});
        terminal["charges"]["sq8"] =
            json!({"submitted_gets":1000,"verified_bytes":7168000,"failed_gets":0});
        terminal["sum"] = json!({"submitted_gets":3000,"verified_bytes":13412000,"failed_gets":0});
        authenticate(&mut rows);
        rows
    }

    fn sealed_loss_fixture() -> (Inputs, Value) {
        let mut i: Inputs = serde_json::from_value(v2_fixture()[1].clone()).unwrap();
        i.rows = 1280;
        i.dimensions = 1024;
        let mut q = v2_fixture()[5].clone();
        q["serving"] = json!({"mode":"baseline"});
        q["source_nomination_skipped"] = json!(false);
        q["trace"] = json!({"ranked_candidate_pages":[0,2,3],"primary_page":0,
            "discoveries":[],"semantic_leaves":[0],"semantic_units":[0,16,24],
            "semantic_seed_additions":[1,2,3,4,5,6,7],
            "nomination_evaluated_units":(0..8).chain(16..32).collect::<Vec<_>>()});
        let (ranges, bytes) = borsuk::two_bit_generation::plan_two_bit_source_cover(
            &BTreeSet::from([0, 2, 3]),
            i.rows,
            source_record_bytes(i.dimensions).unwrap(),
            128,
            64 * 1024 * 1024,
        )
        .unwrap();
        q["charges"]["source"] = json!({"submitted_gets":ranges.len(),
            "verified_bytes":bytes,"failed_gets":0});
        let bytes = 768 * (i.dimensions + 12);
        q["charges"]["sq8"] = json!({"submitted_gets":1,"verified_bytes":bytes,"failed_gets":0});
        q["plan"] = json!({"selected_pages":[0,2],"ranges":[[0,bytes]],"planned_bytes":bytes});
        (i, q)
    }

    #[tokio::test]
    async fn current_replay_public_builder_runtime_plan_matches_v9_v10_v11_and_loss_coverage() {
        use borsuk::{
            two_bit_build::TwoBitGenerationBuilder,
            two_bit_generation::{DiscoveryMode, TwoBitGeneration, TwoBitGenerationLimits},
            two_bit_source::TwoBitSource,
        };
        use object_store::{ObjectStoreExt, path::Path as ObjectPath};
        let dir = tempfile::tempdir().unwrap();
        let rows = 257;
        let mut source = Vec::new();
        let mut sq8 = Vec::new();
        for row in 0..rows {
            source.extend_from_slice(&1_f32.to_le_bytes());
            source.extend_from_slice(&0_f32.to_le_bytes());
            sq8.extend_from_slice(&(row as i64).to_le_bytes());
            sq8.extend_from_slice(&1_f32.to_le_bytes());
            sq8.extend_from_slice(&[1, 0]);
        }
        let raw_path = dir.path().join("raw");
        let sq8_path = dir.path().join("sq8");
        std::fs::write(&raw_path, &source).unwrap();
        std::fs::write(&sq8_path, &sq8).unwrap();
        let store = object_store::memory::InMemory::new();
        let sq8_sha = sha(&sq8);
        let key = ObjectPath::from(format!("semantic/objects/{sq8_sha}"));
        store.put(&key, sq8.into()).await.unwrap();
        let etag = store.head(&key).await.unwrap().e_tag.unwrap();
        let root = dir.path().join("generation");
        let root_sha = TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &sha(&source),
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions: 2,
            },
            base_epoch: 0,
            generation: 1,
            low: &[0., 0.],
            step: &[1., 1.],
            sq8_object_key: key.as_ref(),
            sq8_etag: &etag,
        }
        .build_with_discovery(None, DiscoveryMode::Semantic, &root, 128_000_000)
        .unwrap();
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 128_000_000,
            max_active_queries: 1,
            max_query_bytes: 32 * 1024 * 1024,
            max_query_gets: 32,
            max_parallel_gets: 4,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 4,
            max_query_scratch_bytes: 400_000
                + borsuk::two_bit_generation::TwoBitPlanTrace::scratch_bytes(rows),
            already_pinned_bytes: 0,
        };
        let head = borsuk::two_bit_store::publish_two_bit_generation(
            &store,
            &ObjectPath::from("semantic/index"),
            &root,
            &root_sha,
            limits,
            None,
        )
        .await
        .unwrap();
        let generation = TwoBitGeneration::open_remote(
            &store,
            &head.metadata_prefix(),
            &root_sha,
            limits,
            dir.path(),
        )
        .await
        .unwrap();
        let (result, trace) = generation
            .diagnostic_search_with_store(&store, &[1., 0.], 10)
            .await
            .unwrap();
        let (mut inputs, mut raw) = sealed_loss_fixture();
        inputs.rows = rows;
        inputs.dimensions = 2;
        let plan = &result.plan;
        // Literal runner-shaped oracle: 256 tied SOURCE nominees occupy eight blocks.
        let v9 = json!({"page_rows":32,"selected_pages":[0,1,2,3,4,5,6,7],
            "ranges":[[0,3584]],"planned_bytes":3584,"target_pages":8,
            "target_shortfall":0,"primary_pages_retained":0,"covered_pages":8,"bridge_pages":0});
        for schema in [
            "borsuk-cohere-native-baseline-result-v9",
            current_v10::RESULT_SCHEMA,
            current_v10::READ_RETRY_RESULT_SCHEMA,
            current_v10::LIBRARY_SCHEDULER_RESULT_SCHEMA,
        ] {
            let mut expected = v9.clone();
            if current_v10::is_result_schema(schema) {
                expected["logical_range_count"] = json!(1);
            }
            let reconstructed = sq8_subpage_shortlist::plan_value(plan, 2, schema);
            assert_eq!(reconstructed, expected);
            assert_eq!(
                sha(&serde_json::to_vec(&reconstructed).unwrap()),
                sha(&serde_json::to_vec(&expected).unwrap())
            );
            raw["plan"] = reconstructed;
            raw["trace"] = serde_json::to_value(&trace).unwrap();
            raw["charges"]["source"] = json!({"submitted_gets":result.source_stats.submitted_gets,
                "verified_bytes":result.source_stats.verified_bytes,"failed_gets":0});
            raw["charges"]["sq8"] = json!({"submitted_gets":result.ranked.stats.submitted_gets,
                "verified_bytes":result.ranked.stats.verified_bytes,"failed_gets":0});
            let query: Query = serde_json::from_value(raw.clone()).unwrap();
            let coverage =
                loss_coverage(&serde_json::to_vec(&raw).unwrap(), &query, &inputs).unwrap();
            assert_eq!(coverage.sq8_rows, vec![[0, 256]]);
            assert!(plan.selected_pages.iter().any(|p| *p > 1));
            if current_v10::is_result_schema(schema) {
                let mut bad = raw.clone();
                bad["plan"]["logical_range_count"] = json!(2);
                assert!(
                    loss_coverage(&serde_json::to_vec(&bad).unwrap(), &query, &inputs).is_err()
                );
            }
        }
        assert_eq!(
            result
                .ranked
                .candidates
                .iter()
                .map(|p| (p.id, p.score.to_bits()))
                .collect::<Vec<_>>(),
            vec![
                (0, 0),
                (1, 0),
                (2, 0),
                (3, 0),
                (4, 0),
                (5, 0),
                (6, 0),
                (7, 0),
                (8, 0),
                (9, 0)
            ]
        );
    }

    #[test]
    fn current_loss_coverage_uses_blocks_without_forced_primary_and_clips_tail() {
        let (mut inputs, mut raw) = sealed_loss_fixture();
        inputs.rows = 1000;
        let (source, bytes) = borsuk::two_bit_generation::plan_two_bit_source_cover(
            &BTreeSet::from([0, 2, 3]),
            inputs.rows,
            source_record_bytes(inputs.dimensions).unwrap(),
            128,
            64 * 1024 * 1024,
        )
        .unwrap();
        raw["charges"]["source"] = json!({"submitted_gets":source.len(),
            "verified_bytes":bytes,"failed_gets":0});
        let width = inputs.dimensions + 12;
        raw["charges"]["sq8"] =
            json!({"submitted_gets":2,"verified_bytes":40*width,"failed_gets":0});
        raw["plan"] = json!({"page_rows":32,"selected_pages":[16,31],
            "ranges":[[512*width,544*width],[992*width,1000*width]],
            "logical_range_count":2,"planned_bytes":40*width,"target_pages":2,
            "target_shortfall":0,"primary_pages_retained":0,"covered_pages":2,"bridge_pages":0});
        let check = |raw: &Value| {
            loss_coverage(
                &serde_json::to_vec(raw).unwrap(),
                &serde_json::from_value(raw.clone()).unwrap(),
                &inputs,
            )
        };
        let coverage = check(&raw).unwrap();
        assert_eq!(coverage.sq8_rows, vec![[512, 544], [992, 1000]]);
        assert_eq!(
            coverage.scored_units,
            (0..8).chain(16..32).collect::<Vec<_>>()
        );
        let mut v9 = raw.clone();
        v9["plan"]
            .as_object_mut()
            .unwrap()
            .remove("logical_range_count");
        assert!(check(&v9).is_ok());
        for (pointer, value) in [
            ("/plan/page_rows", json!(64)),
            ("/plan/page_rows", Value::Null),
            ("/plan/selected_pages", json!([8, 31])),
            ("/plan/selected_pages", json!([16, 32])),
            ("/plan/selected_pages", json!([16, 16])),
            ("/plan/ranges/1/1", json!(1001 * width)),
            ("/plan/ranges/0/0", json!(512 * width + 1)),
            ("/plan/primary_pages_retained", json!(1)),
            ("/plan/covered_pages", json!(3)),
            ("/plan/bridge_pages", json!(1)),
            ("/plan/target_shortfall", json!(1)),
            ("/plan/logical_range_count", json!(1)),
            ("/charges/sq8/verified_bytes", json!(40 * width - 1)),
            ("/charges/sq8/submitted_gets", json!(1)),
            ("/charges/source/verified_bytes", json!(1)),
            ("/trace/nomination_evaluated_units/23", json!(30)),
        ] {
            let mut bad = raw.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(check(&bad).is_err(), "accepted {pointer}");
        }
    }

    #[test]
    fn source_budget96_cover_uses_declared_budget_and_refuses_other_cover() {
        let (original, mut raw) = sealed_loss_fixture();
        let mut metadata = serde_json::to_value(original).unwrap();
        metadata["rows"] = json!(1_000_000);
        metadata["source_get_budget"] = json!(96);
        let inputs: Inputs = serde_json::from_value(metadata.clone()).unwrap();
        let pages = (0_usize..130).map(|n| 2 * n).collect::<Vec<_>>();
        raw["trace"] = json!({"ranked_candidate_pages":pages,"primary_page":0,
            "discoveries":[],"semantic_leaves":[0],
            "semantic_units":pages.iter().map(|p| p*8).collect::<Vec<_>>(),
            "semantic_seed_additions":[1,2,3,4,5,6,7],
            "nomination_evaluated_units":pages.iter().flat_map(|p| p*8..(p+1)*8).collect::<Vec<_>>()});
        // 130 isolated pages require 34 one-page bridges to fit 96 GETs.
        // Each D1024 SOURCE page is 256 * 264 = 67584 bytes.
        raw["charges"]["source"] = json!({"submitted_gets":96,
            "verified_bytes":11_083_776,"failed_gets":0});
        raw["charges"]["sq8"] = json!({"submitted_gets":1,
            "verified_bytes":265_216,"failed_gets":0});
        raw["plan"] = json!({"selected_pages":[0],"ranges":[[0,265_216]],
            "planned_bytes":265_216});
        let query: Query = serde_json::from_value(raw.clone()).unwrap();
        let coverage = loss_coverage(&serde_json::to_vec(&raw).unwrap(), &query, &inputs)
            .expect("declared96 cover must be checked as96, not128");
        assert_eq!(coverage.scored_units.len(), 1040);
        assert_eq!(coverage.sq8_rows, vec![[0, 256]]);
        let mut wrong = raw.clone();
        // The 128-GET policy joins just two gaps: 132 * 67584 bytes.
        wrong["charges"]["source"] = json!({"submitted_gets":128,
            "verified_bytes":8_921_088,"failed_gets":0});
        let query: Query = serde_json::from_value(wrong.clone()).unwrap();
        assert!(loss_coverage(&serde_json::to_vec(&wrong).unwrap(), &query, &inputs).is_err());
        metadata["source_get_budget"] = json!(128);
        let inputs: Inputs = serde_json::from_value(metadata).unwrap();
        let query: Query = serde_json::from_value(raw.clone()).unwrap();
        assert!(loss_coverage(&serde_json::to_vec(&raw).unwrap(), &query, &inputs).is_err());
    }

    #[test]
    fn scale_replay_enforces_declared_source_budget_on_last_query() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = paired_scale_fixture(64);
        rows[0]["schema"] = json!("borsuk-cohere-native-baseline-result-v8");
        rows[0]["source_get_budget"] = json!(96);
        rows[1]["source_get_budget"] = json!(96);
        let (path, _) = write_fixture(dir.path(), "budget-valid", rows.clone());
        let (config, pin) = scale_config(dir.path(), &path, &rows);
        reduce_scale(&config, &pin).expect("valid declared96 replay remains admitted");

        for submitted in [97_u64, 128] {
            let mut bad = rows.clone();
            let last = bad
                .iter_mut()
                .find(|row| row["phase"] == "query" && row["ordinal"] == 999)
                .unwrap();
            last["charges"]["source"]["submitted_gets"] = json!(submitted);
            let charges: Charges = serde_json::from_value(last["charges"].clone()).unwrap();
            last["sum"] = serde_json::to_value(charges.sum().unwrap()).unwrap();
            let mut total = Charges::default();
            for query in bad.iter().filter(|row| row["phase"] == "query") {
                total
                    .add(serde_json::from_value(query["charges"].clone()).unwrap())
                    .unwrap();
            }
            let terminal = &mut bad.last_mut().unwrap()["summary"];
            terminal["charges"] = serde_json::to_value(total).unwrap();
            terminal["sum"] = serde_json::to_value(total.sum().unwrap()).unwrap();
            let mut allowed128 = bad.clone();
            allowed128[0]["source_get_budget"] = json!(128);
            allowed128[1]["source_get_budget"] = json!(128);
            let (path, _) = write_fixture(dir.path(), "budget128-valid", allowed128.clone());
            let (config, pin) = scale_config(dir.path(), &path, &allowed128);
            reduce_scale(&config, &pin).expect("same count remains admissible under128");
            let (path, _) = write_fixture(dir.path(), &format!("budget-{submitted}"), bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            assert_eq!(
                reduce_scale(&config, &pin).unwrap_err().to_string(),
                "baseline SOURCE/SQ8 query caps",
                "last query with SOURCE{submitted} must refuse declared96"
            );
        }
    }

    #[test]
    fn source_budget_headers_and_fixed_diagnostics_reject_contradictions() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = paired_scale_fixture(64);
        rows[0]["schema"] = json!("borsuk-cohere-native-baseline-result-v8");
        for budget in [96, 128] {
            rows[0]["source_get_budget"] = json!(budget);
            rows[1]["source_get_budget"] = json!(budget);
            for (index, phase) in [(0, "identity"), (1, "bound_inputs")] {
                validate_scale_row(&serde_json::to_vec(&rows[index]).unwrap(), phase).unwrap();
            }
        }
        for budget in [
            json!(97),
            json!(96.0),
            json!("96"),
            Value::Null,
            json!(true),
        ] {
            for (index, phase) in [(0, "identity"), (1, "bound_inputs")] {
                let mut header = rows[index].clone();
                header["source_get_budget"] = budget.clone();
                assert_eq!(
                    validate_scale_row(&serde_json::to_vec(&header).unwrap(), phase)
                        .unwrap_err()
                        .to_string(),
                    "explicit SOURCE budget must be integer96 or128"
                );
            }
        }
        let mut identity = rows[0].clone();
        identity
            .as_object_mut()
            .unwrap()
            .remove("source_get_budget");
        assert_eq!(
            validate_scale_row(&serde_json::to_vec(&identity).unwrap(), "identity")
                .unwrap_err()
                .to_string(),
            "v8 identity requires explicit SOURCE budget; historical identity forbids it"
        );
        identity["schema"] = json!("borsuk-cohere-native-baseline-result-v7");
        identity["source_get_budget"] = json!(128);
        assert_eq!(
            validate_scale_row(&serde_json::to_vec(&identity).unwrap(), "identity")
                .unwrap_err()
                .to_string(),
            "v8 identity requires explicit SOURCE budget; historical identity forbids it"
        );
        rows[0]["source_get_budget"] = json!(96);
        rows[1]["source_get_budget"] = json!(128);
        let (path, _) = write_fixture(dir.path(), "budget-mismatch", rows.clone());
        let (config, pin) = scale_config(dir.path(), &path, &rows);
        assert_eq!(
            reduce_scale(&config, &pin).unwrap_err().to_string(),
            "identity/input SOURCE budget mismatch"
        );
        rows[1]["source_get_budget"] = json!(96);
        // Keep an absent producer: both historical diagnostics must reject the
        // declared budget before opening result contents or applying 128 covers.
        let (config, pin) = scale_config(dir.path(), &path, &rows);
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            reduce_source_intervals(&config, &pin)
                .unwrap_err()
                .to_string(),
            "interval diagnostic requires full width64 cache-off scale input"
        );
        assert_eq!(
            reduce_source_cover_curve(&config, &pin)
                .unwrap_err()
                .to_string(),
            "source cover curve requires full cache-off scale input"
        );
    }

    #[test]
    fn source_intervals_report_authenticates_full_population_and_noop_geometry() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = paired_scale_fixture(64);
        let (_, fixture) = sealed_loss_fixture();
        let sq8_page = 256 * (1024 + 12);
        let mut totals = Charges::default();
        for row in &mut rows {
            if row["phase"] == "query" {
                row["trace"] = fixture["trace"].clone();
                row["charges"]["source"] = fixture["charges"]["source"].clone();
                row["charges"]["sq8"] = json!({"submitted_gets":2,
                    "verified_bytes":2*sq8_page,"failed_gets":0});
                row["plan"] = json!({"selected_pages":[0,2],
                    "ranges":[[0,sq8_page],[2*sq8_page,3*sq8_page]],
                    "planned_bytes":2*sq8_page,"target_pages":2,"target_shortfall":0,
                    "primary_pages_retained":1,"covered_pages":2,"bridge_pages":0});
                let charges: Charges = serde_json::from_value(row["charges"].clone()).unwrap();
                row["sum"] = serde_json::to_value(charges.sum().unwrap()).unwrap();
                totals.add(charges).unwrap();
            }
        }
        rows.last_mut().unwrap()["summary"]["charges"] = serde_json::to_value(totals).unwrap();
        rows.last_mut().unwrap()["summary"]["sum"] =
            serde_json::to_value(totals.sum().unwrap()).unwrap();
        authenticate(&mut rows);
        let (path, _) = write_fixture(dir.path(), "interval-full", rows.clone());
        let (config, hash) = scale_config(dir.path(), &path, &rows);
        let report = reduce_source_intervals(&config, &hash).unwrap();
        assert_eq!(report["count"], 1000);
        assert_eq!(report["queries"].as_array().unwrap().len(), 1000);
        assert_eq!(report["max_source_range_bytes"], 135168);
        assert_eq!(report["max_sq8_range_bytes"], sq8_page);
        assert_eq!(report["minimum_feasible_capacity_bytes"], sq8_page);
        assert_eq!(report["maximum_phase_bytes"], 2 * sq8_page);
        assert_eq!(report["distinct_binding_arm"], true);
        assert_eq!(report["waiting_observed"], false);
        // One joined SQ8 range includes the bridge and raises minimum to phase total.
        for row in &mut rows {
            if row["phase"] == "query" {
                row["plan"] = json!({"selected_pages":[0,2],
                    "ranges":[[0,3*sq8_page]],"planned_bytes":3*sq8_page,
                    "target_pages":2,"target_shortfall":0,"primary_pages_retained":1,
                    "covered_pages":3,"bridge_pages":1});
                row["charges"]["sq8"] = fixture["charges"]["sq8"].clone();
                let charges: Charges = serde_json::from_value(row["charges"].clone()).unwrap();
                row["sum"] = serde_json::to_value(charges.sum().unwrap()).unwrap();
            }
        }
        let mut totals = Charges::default();
        for row in &rows {
            if row["phase"] == "query" {
                totals
                    .add(serde_json::from_value(row["charges"].clone()).unwrap())
                    .unwrap();
            }
        }
        rows.last_mut().unwrap()["summary"]["charges"] = serde_json::to_value(totals).unwrap();
        rows.last_mut().unwrap()["summary"]["sum"] =
            serde_json::to_value(totals.sum().unwrap()).unwrap();
        authenticate(&mut rows);
        let (path, _) = write_fixture(dir.path(), "interval-noop", rows.clone());
        let (config, hash) = scale_config(dir.path(), &path, &rows);
        let report = reduce_source_intervals(&config, &hash).unwrap();
        assert_eq!(report["distinct_binding_arm"], false);
        assert_eq!(report["selection"], "NO_DISTINCT_BINDING_ARM");
    }

    #[test]
    fn source_intervals_refuse_wrong_authority_before_result_open() {
        let dir = tempfile::tempdir().unwrap();
        let rows = v2_fixture();
        let input = dir.path().join("absent-native-result.jsonl");
        for (index, (key, value)) in [
            ("rows", json!(100_000)),
            ("dimensions", json!(768)),
            ("fetch_parallelism", json!(32)),
            ("source_cache", json!("on")),
            ("execution", json!({"mode":"prefix"})),
        ]
        .into_iter()
        .enumerate()
        {
            let mut inputs = rows[1].clone();
            inputs["rows"] = json!(1_000_000);
            inputs["dimensions"] = json!(1024);
            inputs["fetch_parallelism"] = json!(64);
            inputs["source_cache"] = json!("off");
            inputs["execution"] = json!({"mode":"full"});
            inputs[key] = value;
            let config = json!({"schema":SCALE_CONFIG_SCHEMA,
                "input":{"path":input,"bytes":1,"sha256":"a".repeat(64)},
                "expected_identity":rows[0],"expected_bound_inputs":inputs});
            let bytes = serde_json::to_vec(&config).unwrap();
            let path = dir.path().join(format!("interval-{index}.json"));
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(
                reduce_source_intervals(&path, &sha(&bytes))
                    .unwrap_err()
                    .to_string(),
                "interval diagnostic requires full width64 cache-off scale input"
            );
        }
    }

    #[test]
    fn source_intervals_publication_is_geometry_only() {
        let dir = tempfile::tempdir().unwrap();
        for (index, (status, complete, accepted)) in [
            ("INTERVAL_GEOMETRY_ONLY", true, true),
            ("MEASURED", true, false),
            ("INTERVAL_GEOMETRY_ONLY", false, false),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                execute_report(
                    &dir.path().join(format!("interval-{index}.json")),
                    SOURCE_INTERVAL_SCHEMA,
                    || Ok(json!({"schema":SOURCE_INTERVAL_SCHEMA,
                    "status":status,"complete":complete}))
                )
                .unwrap(),
                accepted
            );
        }
    }

    #[test]
    fn source_cover_interval_lengths_preserve_bridge_and_separate_ranges() {
        let (inputs, raw) = sealed_loss_fixture();
        let query: Query = serde_json::from_value(raw.clone()).unwrap();
        let costs = source_cover_costs(
            &serde_json::to_vec(&raw).unwrap(),
            &query,
            &inputs,
            &[128, 1],
        )
        .unwrap();
        let page_bytes = 256 * source_record_bytes(inputs.dimensions).unwrap() as u64;
        let recorded = serde_json::to_value(costs).unwrap();
        // Full closure is three pages; its largest separate request is two.
        // A single GET includes the bridge and must expose four pages.
        assert_eq!(recorded[0][3], json!(2 * page_bytes));
        assert_eq!(recorded[1][3], json!(4 * page_bytes));
    }

    #[test]
    fn source_cover_curve_preserves_full_closure_and_charges_bridge_bytes() {
        let (inputs, raw) = sealed_loss_fixture();
        let query: Query = serde_json::from_value(raw.clone()).unwrap();
        let costs = source_cover_costs(
            &serde_json::to_vec(&raw).unwrap(),
            &query,
            &inputs,
            &[128, 1],
        )
        .unwrap();
        let page_bytes = 256 * source_record_bytes(inputs.dimensions).unwrap() as u64;
        // Three SOURCE pages, despite only two selected SQ8 pages. Joining the
        // two runs adds the unselected gap page; it changes no scored units.
        assert_eq!(
            costs,
            [
                (2, 3 * page_bytes, true, 2 * page_bytes),
                (1, 4 * page_bytes, true, 4 * page_bytes)
            ]
        );
        let mut corrupt = raw;
        corrupt["charges"]["source"]["verified_bytes"] = json!(3 * page_bytes - 1);
        let query: Query = serde_json::from_value(corrupt.clone()).unwrap();
        assert_eq!(
            source_cover_costs(
                &serde_json::to_vec(&corrupt).unwrap(),
                &query,
                &inputs,
                &[128, 1]
            )
            .unwrap_err()
            .to_string(),
            "loss SOURCE cover charges"
        );
    }

    #[test]
    fn source_cover_curve_retains_costs_outside_historical_byte_cap() {
        let (mut inputs, mut raw) = sealed_loss_fixture();
        inputs.rows = 1_000_000;
        raw["trace"]["ranked_candidate_pages"] = json!([0, 2, 3900]);
        raw["trace"]["semantic_units"] = json!([0, 16, 31200]);
        raw["trace"]["nomination_evaluated_units"] =
            json!((0..8).chain(16..24).chain(31200..31208).collect::<Vec<_>>());
        raw["charges"]["source"]["submitted_gets"] = json!(3);
        let query: Query = serde_json::from_value(raw.clone()).unwrap();
        let costs = source_cover_costs(
            &serde_json::to_vec(&raw).unwrap(),
            &query,
            &inputs,
            &[128, 1],
        )
        .unwrap();
        assert_eq!(costs[0].0, 3);
        assert!(costs[0].2);
        assert_eq!(costs[1].0, 1);
        assert!(costs[1].1 > 64 * 1024 * 1024);
        assert!(!costs[1].2);
        assert!(
            source_cover_costs(&serde_json::to_vec(&raw).unwrap(), &query, &inputs, &[0]).is_err()
        );
    }

    #[test]
    fn source_cover_curve_publication_rejects_measurement_status_and_incomplete_report() {
        let dir = tempfile::tempdir().unwrap();
        for (index, (report, accepted)) in [
            (json!({"schema":SOURCE_COVER_CURVE_SCHEMA,"status":"COUNTERFACTUAL_COST_ONLY","complete":true}), true),
            (json!({"schema":SOURCE_COVER_CURVE_SCHEMA,"status":"MEASURED","complete":true}), false),
            (json!({"schema":SOURCE_COVER_CURVE_SCHEMA,"status":"COUNTERFACTUAL_COST_ONLY","complete":false}), false),
            (json!({"schema":"unrelated","status":"COUNTERFACTUAL_COST_ONLY","complete":true}), false),
        ].into_iter().enumerate() {
            assert_eq!(execute_report(&dir.path().join(format!("curve-{index}.json")),
                SOURCE_COVER_CURVE_SCHEMA, || Ok(report)).unwrap(), accepted);
        }
    }

    #[test]
    fn leaf_coverage_real_source_plan_excludes_fetched_bridge_and_keeps_tail() {
        use borsuk::{
            rotated_two_bit::RotatedTwoBitCodec,
            semantic_unit_router::{Nomination, SemanticProfile},
            two_bit_generation::{TwoBitPlanTrace, plan_two_bit_source_walks},
        };
        let rows = 513;
        let codec = RotatedTwoBitCodec::new(&[0., 0.], 20260923).unwrap();
        let records = (0..rows)
            .map(|_| codec.encode(&[1., 0.]).unwrap())
            .collect::<Vec<_>>();
        let prepared = codec
            .prepare_query(
                &[1., 0.],
                RotatedTwoBitCodec::required_query_scratch_bytes(2).unwrap(),
            )
            .unwrap();
        let closure = BTreeSet::from([0, 2]);
        let width = codec.record_bytes();
        let (ranges, bytes) = borsuk::two_bit_generation::plan_two_bit_source_cover(
            &closure,
            rows,
            width,
            1,
            rows * width,
        )
        .unwrap();
        assert_eq!(ranges, [0..rows * width]);
        let mut trace = TwoBitPlanTrace {
            ranked_candidate_pages: vec![],
            nomination_evaluated_units: vec![],
            primary_page: 0,
            discoveries: vec![],
            semantic_leaves: vec![],
            semantic_units: vec![],
            semantic_seed_additions: vec![],
        };
        plan_two_bit_source_walks(
            &crate::sq8_subpage_shortlist::planning_authority(rows, 2),
            2,
            Some(SemanticProfile::Scale1m),
            &[(0, vec![0, 1, 2, 3, 4, 5, 6, 7, 16])],
            &prepared,
            |row| records.get(row).map(Vec::as_slice),
            borsuk::sq8_page_authority::SelectedSq8Limits {
                max_gets: 32,
                max_bytes: 1 << 20,
                max_planner_bytes: 256 * 1024,
            },
            Some(&mut trace),
        )
        .unwrap();
        assert_eq!(
            trace
                .nomination_evaluated_units
                .iter()
                .copied()
                .collect::<BTreeSet<_>>(),
            (0..8).chain([16]).collect()
        );
        assert_eq!(
            trace
                .ranked_candidate_pages
                .iter()
                .copied()
                .collect::<BTreeSet<_>>(),
            closure
        );
        let arm = CoverageArm {
            nomination: Nomination {
                leaf_ids: vec![0],
                units: BTreeSet::from([0, 16]),
                seed_page: 0,
                walk_units: vec![0, 1, 2, 3, 4, 5, 6, 7, 16],
                seed_additions: vec![1, 2, 3, 4, 5, 6, 7],
                page_closure: closure,
            },
            ranges,
            bytes,
        };
        assert_eq!(coverage_slot(&arm, 256, width), (false, true));
        assert_eq!(coverage_slot(&arm, 512, width), (true, true));
        assert!(
            borsuk::two_bit_generation::plan_two_bit_source_cover(
                &arm.nomination.page_closure,
                rows,
                width,
                1,
                bytes - 1
            )
            .is_err()
        );
    }

    #[test]
    fn sampled_boundary_stream_authenticates_nonidentity_order_and_partial_unit() {
        let dir = tempfile::tempdir().unwrap();
        let (mut c, mut i) = leaf_coverage_metadata_fixture(dir.path());
        i.rows = 65;
        i.dimensions = 2;
        let mut body = Vec::new();
        let mut order = Vec::new();
        for physical in 0..i.rows {
            let id = ((physical + 17) % i.rows) as i64;
            body.extend_from_slice(&id.to_le_bytes());
            order.extend_from_slice(&id.to_le_bytes());
            body.extend_from_slice(&1_f32.to_le_bytes());
            body.extend_from_slice(&[physical as u8 + 1, 1]);
        }
        c.sq8 = CompletedInput {
            path: dir.path().join("sampled-sq8"),
            bytes: body.len() as u64,
            sha256: sha(&body),
        };
        i.native_order_sha256 = sha(&order);
        std::fs::write(&c.sq8.path, &body).unwrap();
        let (samples, stamp) = boundary_samples(&c, &i, &[0., 0.], &[1., 1.]).unwrap();
        assert_eq!(samples.len(), 3);
        for (sample, physical) in samples.iter().zip([16, 48, 64]) {
            let raw = [physical as f32 + 1., 1.];
            let expected =
                borsuk::two_bit_generation::normalize_two_bit_diagnostic_query(&raw).unwrap();
            assert_eq!(sample.as_slice(), expected.as_ref());
        }
        // Authenticated record corruption, including unsampled records, must fail.
        for case in [
            "duplicate",
            "negative",
            "range",
            "norm",
            "truncated",
            "grown",
            "digest",
            "order",
            "zero",
            "nonfinite",
        ] {
            let mut corrupt = body.clone();
            let mut pin: CoverageConfig =
                serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
            let mut input: Inputs =
                serde_json::from_value(serde_json::to_value(&i).unwrap()).unwrap();
            match case {
                "duplicate" => {
                    let id = corrupt[..8].to_vec();
                    corrupt[14..22].copy_from_slice(&id);
                }
                "negative" => corrupt[..8].copy_from_slice(&(-1_i64).to_le_bytes()),
                "range" => corrupt[..8].copy_from_slice(&65_i64.to_le_bytes()),
                "norm" => corrupt[8..12].copy_from_slice(&f32::NAN.to_le_bytes()),
                "truncated" => {
                    corrupt.pop();
                }
                "grown" => corrupt.push(0),
                "order" => input.native_order_sha256 = "0".repeat(64),
                "zero" => corrupt[16 * 14 + 12..16 * 14 + 14].fill(0),
                "nonfinite" => corrupt[16 * 14 + 12] = 255,
                _ => {}
            }
            pin.sq8.sha256 = if case == "digest" {
                "0".repeat(64)
            } else {
                sha(&corrupt)
            };
            if matches!(case, "duplicate" | "negative" | "range") {
                let bytes = corrupt
                    .chunks_exact(14)
                    .flat_map(|r| r[..8].iter().copied())
                    .collect::<Vec<_>>();
                input.native_order_sha256 = sha(&bytes);
            }
            std::fs::write(&pin.sq8.path, &corrupt).unwrap();
            let step = if case == "nonfinite" {
                [f32::MAX, 1.]
            } else {
                [1., 1.]
            };
            assert!(
                boundary_samples(&pin, &input, &[0., 0.], &step).is_err(),
                "{case}"
            );
        }
        assert!(stamp.recheck().is_err());
    }

    #[test]
    fn sampled_boundary_metadata_substitutions_refuse_before_source_construction() {
        let dir = tempfile::tempdir().unwrap();
        let (mut c, mut i) = leaf_coverage_metadata_fixture(dir.path());
        let original = std::fs::read(&c.generation_manifest.path).unwrap();
        let generation_sha = i.generation_root_sha256.clone();
        for case in [
            "generation",
            "calibration_shape",
            "calibration_step",
            "membership",
            "router",
        ] {
            let mut generation: Value = serde_json::from_slice(&original).unwrap();
            match case {
                "generation" => generation["generation"] = json!(2),
                "calibration_shape" => generation["low"] = json!([0.]),
                "calibration_step" => generation["step"] = json!([0., 1.]),
                "membership" => {
                    generation["discovery"]["membership_sha256"] = json!("f".repeat(64))
                }
                "router" => generation["discovery"]["root_sha256"] = json!("f".repeat(64)),
                _ => unreachable!(),
            }
            let encoded = serde_json::to_vec(&generation).unwrap();
            std::fs::write(&c.generation_manifest.path, &encoded).unwrap();
            c.generation_manifest.bytes = encoded.len() as u64;
            c.generation_manifest.sha256 = sha(&encoded);
            i.generation_root_sha256 = if case == "generation" {
                generation_sha.clone()
            } else {
                c.generation_manifest.sha256.clone()
            };
            assert!(coverage_router(&c, &i).is_err(), "{case}");
        }
        assert!(!c.sq8.path.exists());
        assert!(!c.requests.path.exists());
        assert!(!c.truth.path.exists());
    }

    #[test]
    fn sampled_boundary_actual_seals_precede_requests_and_truth_with_distinct_labels() {
        use std::cell::Cell;
        let dir = tempfile::tempdir().unwrap();
        let (mut c, scope) = leaf_coverage_orchestration_fixture(dir.path(), true);
        c.schema = BOUNDARY_CONFIG_SCHEMA.into();
        c.max_payload_bytes = 512 * 1024 * 1024;
        let config = dir.path().join("boundary-config");
        let encoded = serde_json::to_vec(&c).unwrap();
        std::fs::write(&config, &encoded).unwrap();
        let adjacency = dir.path().join("adjacency");
        let selection = dir.path().join("selection");
        let syncs = Cell::new(0);
        let truth_opens = Cell::new(0);
        let (report, retained) = reduce_leaf_coverage_candidate_with(
            &config,
            &sha(&encoded),
            &selection,
            Some(&adjacency),
            &scope,
            |file, _| {
                file.sync_all()?;
                syncs.set(syncs.get() + 1);
                Ok(())
            },
            |pin, bytes| {
                assert_eq!(syncs.get(), 4); // adjacency file+dir, then paired file+dir.
                let a = std::fs::read(&adjacency).unwrap();
                let sidecar: Value = serde_json::from_slice(&a).unwrap();
                let seal: Value =
                    serde_json::from_slice(&std::fs::read(&selection).unwrap()).unwrap();
                assert_eq!(sidecar["requests_opened"], false);
                assert_eq!(sidecar["truth_opened"], false);
                assert_eq!(sidecar["candidate"], BOUNDARY_CANDIDATE);
                assert_eq!(seal["adjacency_sha256"], sha(&a));
                assert_eq!(seal["schema"], "borsuk-leaf-boundary-selection-seal-v1");
                assert!(seal["selections"][0].get("cosine").is_none());
                assert!(seal["selections"][0].get(BOUNDARY_CANDIDATE).is_some());
                truth_opens.set(truth_opens.get() + 1);
                loss_binary(pin, bytes)
            },
        )
        .unwrap();
        assert_eq!(truth_opens.get(), 1);
        assert_eq!(report["status"], "MEASURED");
        assert_eq!(report["schema"], BOUNDARY_SCHEMA);
        assert_eq!(report["coverage_gate_passed"], false); // weak completion stays MEASURED.
        assert_eq!(report["l2_coverage"], 10);
        assert!(report.get("cosine_coverage").is_none());
        for snapshot in &retained {
            snapshot.recheck().unwrap();
        }

        // A rehashed old-cosine config cannot be admitted through the boundary entrypoint.
        c.schema = "borsuk-leaf-metric-coverage-config-v1".into();
        let encoded = serde_json::to_vec(&c).unwrap();
        std::fs::write(&config, &encoded).unwrap();
        let error = reduce_leaf_coverage_candidate_with(
            &config,
            &sha(&encoded),
            &dir.path().join("unused-selection"),
            Some(&dir.path().join("unused-adjacency")),
            &scope,
            |_, _| panic!("wrong schema reached publication"),
            |_, _| panic!("wrong schema reached truth"),
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("config/schema"));
    }

    #[test]
    fn sampled_boundary_adjacency_sync_failure_stops_before_requests_and_truth() {
        let dir = tempfile::tempdir().unwrap();
        let (mut c, scope) = leaf_coverage_orchestration_fixture(dir.path(), true);
        c.schema = BOUNDARY_CONFIG_SCHEMA.into();
        c.max_payload_bytes = 512 * 1024 * 1024;
        // Deliberately remove requests: an adjacency failure must be reached first.
        std::fs::remove_file(&c.requests.path).unwrap();
        let config = dir.path().join("boundary-config");
        let encoded = serde_json::to_vec(&c).unwrap();
        std::fs::write(&config, &encoded).unwrap();
        let adjacency = dir.path().join("adjacency");
        let selection = dir.path().join("selection");
        let report = dir.path().join("report");
        assert!(
            !execute_leaf_coverage_as(
                &report,
                BOUNDARY_SCHEMA,
                || {
                    reduce_leaf_coverage_candidate_with(
                        &config,
                        &sha(&encoded),
                        &selection,
                        Some(&adjacency),
                        &scope,
                        |_, _| Err(io::Error::other("boundary injected sync failure")),
                        |_, _| panic!("truth opened before adjacency publication"),
                    )
                },
                |file, _| file.sync_all()
            )
            .unwrap()
        );
        let failed: Value = serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
        assert_eq!(failed["schema"], BOUNDARY_SCHEMA);
        assert_eq!(failed["status"], "INVALID");
        assert!(
            failed["error"]
                .as_str()
                .unwrap()
                .contains("boundary injected sync failure")
        );
        let failed: Value = serde_json::from_slice(&std::fs::read(&adjacency).unwrap()).unwrap();
        assert_eq!(failed["status"], "INVALID");
        assert!(!selection.exists());
    }

    fn leaf_coverage_metadata_fixture(dir: &Path) -> (CoverageConfig, Inputs) {
        use borsuk::semantic_unit_router::{
            Manifest, SemanticProfile, SemanticUnitRouter, SourceIdentity, build,
        };
        let rows: usize = 100352; // 49 full leaves, with one leaf dropped at the fixed48 boundary.
        let mut blob = b"BORSUCP1".to_vec();
        blob.extend_from_slice(&(rows as u64).to_le_bytes());
        for word in [2_u32, 32, 256, 0] {
            blob.extend_from_slice(&word.to_le_bytes());
        }
        blob.resize(32 + rows.div_ceil(32) * 4, 0);
        let centroids = sha(&blob);
        let root_identity = "1".repeat(64);
        let input = SourceIdentity {
            profile: SemanticProfile::Scale1m,
            schema: "coverage-fixture",
            root_sha256: &root_identity,
            centroids_sha256: &centroids,
            rows,
            dimensions: 2,
        };
        let artifacts = build(&blob, &input, 64 * 1024 * 1024).unwrap();
        let router = SemanticUnitRouter::open(
            &artifacts.manifest,
            &artifacts.membership,
            &sha(&artifacts.manifest),
            &input,
            4 * 1024 * 1024,
        )
        .unwrap();
        let mut manifest: Manifest = router.manifest().clone();
        assert_eq!(manifest.leaves.len(), 49);
        // Independently declared authenticated prototypes: L2 includes0; cosine excludes0.
        // This tests metadata routing, not full publication of matching leaf means.
        for leaf in &mut manifest.leaves {
            leaf.prototype = vec![2., 0.];
        }
        manifest.leaves[0].prototype = vec![0.25, 0.25];
        let root = manifest.encode().unwrap();
        let (mut i, _) = sealed_loss_fixture();
        i.rows = rows;
        i.dimensions = 2;
        i.profile = "scale1m".into();
        let plane = json!({"schema":"borsuk-two-bit-plane-v3","rows":rows,"dimensions":2,
            "seed":20260923,"record_bytes":9,"source_sha256":i.native_source_sha256,
            "sq8_sha256":i.native_sq8_sha256,"source_order_sha256":i.native_order_sha256,
            "mean_sha256":"2".repeat(64),"records_sha256":"3".repeat(64),
            "page_rows":32,"page_digest_sha256":"4".repeat(64),"query_or_truth_used":false});
        let put = |name: &str, body: &[u8]| {
            let path = dir.join(name);
            std::fs::write(&path, body).unwrap();
            CompletedInput {
                path,
                bytes: body.len() as u64,
                sha256: sha(body),
            }
        };
        let plane_pin = put("plane", &serde_json::to_vec(&plane).unwrap());
        let root_pin = put("root", &root);
        let membership_pin = put("membership", &artifacts.membership);
        let generation = json!({"schema":"borsuk-two-bit-generation-v8","generation":1,"base_epoch":0,
            "plane_manifest_sha256":plane_pin.sha256,"page_manifest_sha256":"5".repeat(64),
            "discovery":{"mode":"semantic","profile":"scale1m","root_sha256":root_pin.sha256,
                "root_bytes":root_pin.bytes,"membership_sha256":membership_pin.sha256,
                "membership_bytes":membership_pin.bytes,"leaves_sha256":manifest.leaf_payload.sha256,
                "leaves_bytes":manifest.leaf_payload.bytes,"input_schema":input.schema,
                "input_root_sha256":input.root_sha256,"centroids_sha256":input.centroids_sha256,
                "source_sha256":plane["source_sha256"],"source_order_sha256":plane["source_order_sha256"],
                "mean_sha256":plane["mean_sha256"],"records_sha256":plane["records_sha256"],
                "sq8_sha256":plane["sq8_sha256"]},
            "sq8_object_sha256":i.native_sq8_sha256,"sq8_object_key":format!("semantic/objects/{}",i.native_sq8_sha256),
            "sq8_etag":"fixture","canonical":{"rows":rows,"dimensions":2,"bytes":rows*16,
                "sha256":"6".repeat(64),"object_key":format!("semantic/objects/{}","6".repeat(64))},
            "low":[-1.,-1.],"step":[0.125,0.125]});
        let generation_pin = put("generation", &serde_json::to_vec(&generation).unwrap());
        i.generation_root_sha256 = generation_pin.sha256.clone();
        let dummy = || CompletedInput {
            path: dir.join("unopened-truth"),
            bytes: 1,
            sha256: "7".repeat(64),
        };
        (
            CoverageConfig {
                schema: "borsuk-leaf-metric-coverage-config-v1".into(),
                completed: CompletedConfig {
                    schema: SCALE_CONFIG_SCHEMA.into(),
                    input: dummy(),
                    expected_identity: json!({}),
                    expected_bound_inputs: json!({}),
                },
                generation_manifest: generation_pin,
                plane_manifest: plane_pin,
                router_root: root_pin,
                membership: membership_pin,
                requests: dummy(),
                sq8: dummy(),
                truth: dummy(),
                max_payload_bytes: 256 * 1024 * 1024,
            },
            i,
        )
    }

    #[test]
    fn leaf_coverage_authenticated_metadata_and_fixed48_metric_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let (mut c, mut i) = leaf_coverage_metadata_fixture(dir.path());
        let (router, _) = coverage_router(&c, &i).unwrap();
        let query =
            borsuk::two_bit_generation::normalize_two_bit_diagnostic_query(&[3., 0.]).unwrap();
        let l2 = coverage_arm(
            &router,
            &query,
            &borsuk::VectorMetric::SquaredEuclidean,
            i.rows,
            i.dimensions,
        )
        .unwrap();
        let cosine = coverage_arm(
            &router,
            &query,
            &borsuk::VectorMetric::Cosine,
            i.rows,
            i.dimensions,
        )
        .unwrap();
        assert_eq!(l2.nomination.leaf_ids, (0..48).collect::<Vec<_>>());
        assert_eq!(cosine.nomination.leaf_ids, (1..49).collect::<Vec<_>>());
        assert_ne!(l2.nomination.page_closure, cosine.nomination.page_closure);
        assert!(
            coverage_arm(&router, &[0., 0.], &borsuk::VectorMetric::Cosine, i.rows, 2).is_err()
        );
        assert!(
            coverage_arm(
                &router,
                &[f32::NAN, 0.],
                &borsuk::VectorMetric::Cosine,
                i.rows,
                2
            )
            .is_err()
        );
        let original = std::fs::read(&c.generation_manifest.path).unwrap();
        for field in [
            "source_sha256",
            "source_order_sha256",
            "sq8_sha256",
            "mean_sha256",
            "records_sha256",
        ] {
            let mut g: Value = serde_json::from_slice(&original).unwrap();
            g["discovery"][field] = json!("8".repeat(64));
            let body = serde_json::to_vec(&g).unwrap();
            std::fs::write(&c.generation_manifest.path, &body).unwrap();
            c.generation_manifest.bytes = body.len() as u64;
            c.generation_manifest.sha256 = sha(&body);
            i.generation_root_sha256 = c.generation_manifest.sha256.clone();
            assert!(coverage_router(&c, &i).is_err(), "rehash {field}");
        }
        assert!(!c.truth.path.exists());
    }

    fn leaf_coverage_orchestration_fixture(
        dir: &Path,
        with_sq8: bool,
    ) -> (CoverageConfig, CoverageScope) {
        use borsuk::semantic_unit_router::{
            Geometry, SemanticProfile, SemanticUnitRouter, SourceIdentity, admit,
        };
        let (mut c, mut i) = leaf_coverage_metadata_fixture(dir);
        let (old_router, _) = coverage_router(&c, &i).unwrap();
        let mut manifest = old_router.manifest().clone();
        i.dimensions = 1024;
        manifest.dimensions = i.dimensions;
        manifest.input_centroids_bytes = 32 + i.rows.div_ceil(32) * i.dimensions * 2;
        manifest.modeled_allocation_limit_bytes = SemanticProfile::Scale1m.allocation_cap();
        manifest.modeled_peak_allocation_bytes = admit(
            Geometry {
                rows: i.rows,
                dimensions: i.dimensions,
                units: i.rows.div_ceil(32),
                blob_bytes: manifest.input_centroids_bytes,
            },
            manifest.modeled_allocation_limit_bytes,
            SemanticProfile::Scale1m,
        )
        .unwrap();
        let width = 4 + i.dimensions * 2;
        for leaf in &mut manifest.leaves {
            leaf.prototype.resize(i.dimensions, 0.);
            leaf.offset = leaf.leaf_id * 64 * width;
            leaf.bytes = 64 * width;
        }
        manifest.leaf_payload.bytes = i.rows.div_ceil(32) * width;
        let root = manifest.encode().unwrap();
        std::fs::write(&c.router_root.path, &root).unwrap();
        c.router_root.bytes = root.len() as u64;
        c.router_root.sha256 = sha(&root);
        let input = SourceIdentity {
            profile: SemanticProfile::Scale1m,
            schema: &manifest.input_schema,
            root_sha256: &manifest.input_root_sha256,
            centroids_sha256: &manifest.input_centroids_sha256,
            rows: i.rows,
            dimensions: i.dimensions,
        };
        let membership = std::fs::read(&c.membership.path).unwrap();
        let router = SemanticUnitRouter::open(
            &root,
            &membership,
            &c.router_root.sha256,
            &input,
            4 * 1024 * 1024,
        )
        .unwrap();
        // Start with a real tiny native producer envelope, then explicitly synthesize
        // the larger metadata/trace geometry. This is a boundary fixture, not ANN evidence.
        let (native_dir, result) =
            scale_native_runner::scale_reducer_native_fixture(true, true).unwrap();
        let current = assert_current_native(native_dir.path(), &result);
        let mut records =
            synthetic_historical_rows(&current, "borsuk-cohere-native-baseline-result-v8");
        let requests = std::fs::read(native_dir.path().join("requests.f32")).unwrap();
        let truth = std::fs::read(native_dir.path().join("truth.u64")).unwrap();
        let put = |name: &str, body: &[u8]| {
            let path = dir.join(name);
            std::fs::write(&path, body).unwrap();
            CompletedInput {
                path,
                bytes: body.len() as u64,
                sha256: sha(body),
            }
        };
        c.requests = put("requests.f32", &requests);
        c.truth = put("truth.u64", &truth);
        let mut native_bound = records[1].clone();
        if with_sq8 {
            let mut payload = std::io::BufWriter::new(File::create(dir.join("sq8")).unwrap());
            let mut body_sha = Sha256::new();
            let mut order_sha = Sha256::new();
            let mut record = vec![0_u8; 1036];
            record[8..12].copy_from_slice(&1_f32.to_le_bytes());
            for id in 0..i.rows as u64 {
                record[..8].copy_from_slice(&id.to_le_bytes());
                payload.write_all(&record).unwrap();
                body_sha.update(&record);
                order_sha.update(&record[..8]);
            }
            payload.flush().unwrap();
            payload.get_ref().sync_all().unwrap();
            native_bound["native_sq8_sha256"] = json!(format!("{:x}", body_sha.finalize()));
            native_bound["native_order_sha256"] = json!(format!("{:x}", order_sha.finalize()));
            records[1]["native_sq8_sha256"] = native_bound["native_sq8_sha256"].clone();
            records[1]["native_order_sha256"] = native_bound["native_order_sha256"].clone();
        }
        let mut plane: Value =
            serde_json::from_slice(&std::fs::read(&c.plane_manifest.path).unwrap()).unwrap();
        plane["dimensions"] = json!(1024);
        plane["record_bytes"] = json!(264);
        for key in ["source_sha256", "sq8_sha256", "source_order_sha256"] {
            let bound_key = match key {
                "source_sha256" => "native_source_sha256",
                "sq8_sha256" => "native_sq8_sha256",
                _ => "native_order_sha256",
            };
            plane[key] = native_bound[bound_key].clone();
        }
        c.plane_manifest = put("plane", &serde_json::to_vec(&plane).unwrap());
        let mut g: Value =
            serde_json::from_slice(&std::fs::read(&c.generation_manifest.path).unwrap()).unwrap();
        g["plane_manifest_sha256"] = json!(c.plane_manifest.sha256);
        g["discovery"]["root_sha256"] = json!(c.router_root.sha256);
        g["discovery"]["root_bytes"] = json!(c.router_root.bytes);
        g["discovery"]["leaves_bytes"] = json!(manifest.leaf_payload.bytes);
        for key in ["source_sha256", "sq8_sha256", "source_order_sha256"] {
            g["discovery"][key] = plane[key].clone();
        }
        g["sq8_object_sha256"] = plane["sq8_sha256"].clone();
        g["sq8_object_key"] = json!(format!(
            "semantic/objects/{}",
            plane["sq8_sha256"].as_str().unwrap()
        ));
        g["canonical"]["dimensions"] = json!(1024);
        g["canonical"]["bytes"] = json!(i.rows * 4104);
        g["low"] = json!(vec![-1_f32; 1024]);
        g["step"] = json!(vec![0.125_f32; 1024]);
        c.generation_manifest = put("generation", &serde_json::to_vec(&g).unwrap());
        let bound = &mut records[1];
        bound["rows"] = json!(i.rows);
        bound["dimensions"] = json!(1024);
        bound["corpus_intervals"] = json!([{"start":0,"end":i.rows}]);
        bound["query_source_first"] = json!(i.rows);
        bound["reserved_query_interval"] = json!({"start":i.rows,"end":i.rows+1});
        // This synthesized fixture reserves exactly the one selected request;
        // the original tiny producer reserved two and therefore had a different digest.
        bound["reserved_queries_sha256"] = bound["requests_sha256"].clone();
        bound["generation_root_sha256"] = json!(c.generation_manifest.sha256);
        c.sq8 = CompletedInput {
            path: dir.join(if with_sq8 { "sq8" } else { "unopened-sq8" }),
            bytes: (i.rows * 1036) as u64,
            sha256: bound["native_sq8_sha256"].as_str().unwrap().into(),
        };
        let raw = requests
            .chunks_exact(4)
            .map(|w| f32::from_le_bytes(w.try_into().unwrap()))
            .collect::<Vec<_>>();
        let normalized =
            borsuk::two_bit_generation::normalize_two_bit_diagnostic_query(&raw).unwrap();
        let arm = coverage_arm(
            &router,
            &normalized,
            &borsuk::VectorMetric::SquaredEuclidean,
            i.rows,
            1024,
        )
        .unwrap();
        let q = records
            .iter_mut()
            .find(|row| row["phase"] == "query")
            .unwrap();
        if with_sq8 {
            q["returned"][9]["id"] = json!(200);
        }
        q["trace"] = json!({"ranked_candidate_pages":arm.nomination.page_closure,"primary_page":0,
            "discoveries":[],"semantic_leaves":arm.nomination.leaf_ids,"semantic_units":arm.nomination.units,
            "semantic_seed_additions":arm.nomination.seed_additions,
            "nomination_evaluated_units":arm.nomination.page_closure.iter()
                .flat_map(|p|p*8..((p+1)*8).min(i.rows.div_ceil(32))).collect::<Vec<_>>()});
        q["charges"]["source"] =
            json!({"submitted_gets":arm.ranges.len(),"verified_bytes":arm.bytes,"failed_gets":0});
        let sq8_bytes = 256 * 1036;
        q["charges"]["sq8"] =
            json!({"submitted_gets":1,"verified_bytes":sq8_bytes,"failed_gets":0});
        q["plan"] = json!({"selected_pages":[0],"ranges":[[0,sq8_bytes]],
            "planned_bytes":sq8_bytes,"target_pages":1,"target_shortfall":0,
            "primary_pages_retained":1,"covered_pages":1,"bridge_pages":0});
        for key in ["submitted_gets", "verified_bytes", "failed_gets"] {
            q["sum"][key] = json!(
                ["router", "source", "sq8"]
                    .iter()
                    .map(|stage| q["charges"][stage][key].as_u64().unwrap())
                    .sum::<u64>()
            );
        }
        let charges = q["charges"].clone();
        let sum = q["sum"].clone();
        let seal = records
            .iter_mut()
            .find(|row| row["phase"] == "all_queries_sealed")
            .unwrap();
        seal["generation_root_sha256"] = json!(c.generation_manifest.sha256);
        seal["reserved_query_count"] = json!(1);
        let summary = &mut records.last_mut().unwrap()["summary"];
        summary["generation_root_sha256"] = json!(c.generation_manifest.sha256);
        summary["reserved_query_count"] = json!(1);
        summary["diagnostic_prefix"] = json!(false);
        summary["charges"] = charges;
        summary["sum"] = sum;
        if with_sq8 {
            summary["total_hits10"] = json!(9);
            summary["recall_numerator"] = json!(9);
            summary["mean_recall10"] = json!(0.9);
            let recall = records
                .iter_mut()
                .find(|row| row["phase"] == "recall")
                .unwrap();
            recall["hits10"] = json!(9);
            recall["recall10"] = json!(0.9);
        }
        authenticate(&mut records);
        c.completed = CompletedConfig {
            schema: SCALE_CONFIG_SCHEMA.into(),
            input: put("producer.jsonl", &encode(&records)),
            expected_identity: records[0].clone(),
            expected_bound_inputs: records[1].clone(),
        };
        (
            c,
            CoverageScope {
                rows: i.rows,
                count: 1,
                ordinals: vec![0],
                original_hits: if with_sq8 { 9 } else { 10 },
                original_coverage: 10,
            },
        )
    }

    #[test]
    fn fixed128_coverage_refuses_declared96_before_producer_or_truth_open() {
        for boundary in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (mut c, scope) = leaf_coverage_orchestration_fixture(dir.path(), false);
            if boundary {
                c.schema = BOUNDARY_CONFIG_SCHEMA.to_owned();
            }
            c.completed.expected_identity["schema"] =
                json!("borsuk-cohere-native-baseline-result-v8");
            c.completed.expected_identity["source_get_budget"] = json!(96);
            c.completed.expected_bound_inputs["source_get_budget"] = json!(96);
            // An absent producer makes the ordering observable: the fixed128
            // diagnostic must reject its incompatible control before opening it.
            c.completed.input.path = dir.path().join("producer-must-not-open");
            let config = dir.path().join("fixed128-config.json");
            let bytes = serde_json::to_vec(&c).unwrap();
            std::fs::write(&config, &bytes).unwrap();
            let seal = dir.path().join("selection-must-not-exist");
            let boundary_seal = dir.path().join("boundary-must-not-exist");
            let mut truth_opens = 0;
            let error = reduce_leaf_coverage_candidate_with(
                &config,
                &sha(&bytes),
                &seal,
                boundary.then_some(boundary_seal.as_path()),
                &scope,
                |file, _| file.sync_all(),
                |pin, bytes| {
                    truth_opens += 1;
                    loss_binary(pin, bytes)
                },
            )
            .err()
            .expect("fixed128 diagnostic must refuse a declared96 control");
            assert_eq!(
                error.to_string(),
                "coverage requires frozen SOURCE128 control"
            );
            assert_eq!(truth_opens, 0);
            assert!(!seal.exists());
            assert!(!boundary_seal.exists());
        }
    }

    #[test]
    fn leaf_coverage_actual_orchestration_refuses_terminal_and_sync_before_truth() {
        let dir = tempfile::tempdir().unwrap();
        let (mut c, scope) = leaf_coverage_orchestration_fixture(dir.path(), false);
        let config = dir.path().join("coverage-config");
        let save = |c: &CoverageConfig| {
            let body = serde_json::to_vec(c).unwrap();
            std::fs::write(&config, &body).unwrap();
            sha(&body)
        };
        let original = std::fs::read(&c.completed.input.path).unwrap();
        // A rehashed terminal corruption must be caught by the actual complete reader.
        let mut records = std::str::from_utf8(&original)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        records.last_mut().unwrap()["summary"]["complete"] = json!(false);
        let bad = encode(&records);
        std::fs::write(&c.completed.input.path, &bad).unwrap();
        c.completed.input.bytes = bad.len() as u64;
        c.completed.input.sha256 = sha(&bad);
        let bad_seal = dir.path().join("bad-terminal-seal");
        let mut truth_opens = 0;
        let error = reduce_leaf_coverage_with(
            &config,
            &save(&c),
            &bad_seal,
            &scope,
            |file, _| file.sync_all(),
            |pin, bytes| {
                truth_opens += 1;
                loss_binary(pin, bytes)
            },
        )
        .unwrap_err();
        assert_eq!(truth_opens, 0);
        assert!(!bad_seal.exists());
        assert!(error.to_string().contains("terminal"), "{error}");
        std::fs::write(&c.completed.input.path, &original).unwrap();
        c.completed.input.bytes = original.len() as u64;
        c.completed.input.sha256 = sha(&original);
        let config_sha = save(&c);
        for directory in [false, true] {
            let seal = dir.path().join(format!("failed-seal-{directory}"));
            let error = reduce_leaf_coverage_with(
                &config,
                &config_sha,
                &seal,
                &scope,
                |file, is_directory| {
                    if is_directory == directory {
                        Err(io::Error::other("actual seal sync injection"))
                    } else {
                        file.sync_all()
                    }
                },
                |pin, bytes| {
                    truth_opens += 1;
                    loss_binary(pin, bytes)
                },
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("actual seal sync injection"),
                "{error}"
            );
            assert_eq!(truth_opens, 0);
            assert_eq!(
                serde_json::from_slice::<Value>(&std::fs::read(&seal).unwrap()).unwrap()["status"],
                "INVALID"
            );
        }
        let seal = dir.path().join("good-seal");
        let error = reduce_leaf_coverage_with(
            &config,
            &config_sha,
            &seal,
            &scope,
            |file, _| file.sync_all(),
            |pin, bytes| {
                truth_opens += 1;
                let sealed: Value = serde_json::from_slice(&std::fs::read(&seal)?).unwrap();
                assert_eq!(sealed["truth_opened"], false);
                assert_eq!(sealed["selections"].as_array().unwrap().len(), 1);
                assert_eq!(
                    sealed["selections"][0]["l2"]["leaves"]
                        .as_array()
                        .unwrap()
                        .len(),
                    48
                );
                assert_eq!(
                    sealed["selections"][0]["cosine"]["leaves"]
                        .as_array()
                        .unwrap()
                        .len(),
                    48
                );
                loss_binary(pin, bytes)?;
                Err("verified actual truth boundary reached".into())
            },
        )
        .unwrap_err();
        assert_eq!(truth_opens, 1);
        assert_eq!(error.to_string(), "verified actual truth boundary reached");
    }

    #[test]
    fn leaf_coverage_actual_reducer_rejects_rehashed_nontruth_return_outside_sq8() {
        let dir = tempfile::tempdir().unwrap();
        let (mut c, scope) = leaf_coverage_orchestration_fixture(dir.path(), true);
        let config = dir.path().join("coverage-config");
        let save = |c: &CoverageConfig| {
            let body = serde_json::to_vec(c).unwrap();
            std::fs::write(&config, &body).unwrap();
            sha(&body)
        };
        let config_sha = save(&c);
        let (report, _) = reduce_leaf_coverage_with(
            &config,
            &config_sha,
            &dir.path().join("good-seal"),
            &scope,
            |file, _| file.sync_all(),
            loss_binary,
        )
        .unwrap();
        assert_eq!(report["original_returned_hits10"], 9);
        assert_eq!(report["l2_coverage"], 10);
        let mut records = std::fs::read_to_string(&c.completed.input.path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let q = records
            .iter_mut()
            .find(|row| row["phase"] == "query")
            .unwrap();
        assert_eq!(q["returned"][9]["id"], 200);
        q["returned"][9]["id"] = json!(300); // both nontruth; recall and score bits are unchanged.
        authenticate(&mut records);
        let body = encode(&records);
        std::fs::write(&c.completed.input.path, &body).unwrap();
        c.completed.input.bytes = body.len() as u64;
        c.completed.input.sha256 = sha(&body);
        let report_path = dir.path().join("bad-return-report");
        assert!(
            !execute_leaf_coverage_with(
                &report_path,
                || reduce_leaf_coverage_with(
                    &config,
                    &save(&c),
                    &dir.path().join("bad-return-seal"),
                    &scope,
                    |file, _| file.sync_all(),
                    loss_binary
                ),
                |file, _| file.sync_all()
            )
            .unwrap()
        );
        let invalid: Value = serde_json::from_slice(&std::fs::read(&report_path).unwrap()).unwrap();
        assert_eq!(invalid["status"], "INVALID");
        assert_eq!(invalid["complete"], false);
        assert_eq!(
            invalid["error"],
            "coverage returned ID outside fetched SQ8 cover"
        );
    }

    #[test]
    fn leaf_coverage_seal_sync_mutation_and_occupied_path_refuse_before_truth() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input");
        std::fs::write(&input, b"input").unwrap();
        let file = header_file(&input).unwrap();
        let snapshot = HeaderSnapshot {
            identity: file_identity(&file).unwrap(),
            file,
            path: input.clone(),
        };
        for directory_failure in [false, true] {
            let output = dir.path().join(format!("sync-{directory_failure}"));
            let mut truth_opened = false;
            let result = publish_selection_seal_with_sync(
                &output,
                &json!({"both_arms":["l2","cosine"]}),
                &[&snapshot],
                |file, is_directory| {
                    if is_directory == directory_failure {
                        Err(io::Error::other("injected sync failure"))
                    } else {
                        file.sync_all()
                    }
                },
            )
            .map(|_| truth_opened = true);
            assert!(result.is_err());
            assert!(!truth_opened);
            assert_eq!(
                serde_json::from_slice::<Value>(&std::fs::read(&output).unwrap()).unwrap()["status"],
                "INVALID"
            );
        }
        let output = dir.path().join("good");
        let (sealed, sha) =
            publish_selection_seal(&output, &json!({"both_arms":[1,2]}), &[&snapshot]).unwrap();
        assert_eq!(
            sha,
            format!("{:x}", Sha256::digest(std::fs::read(&output).unwrap()))
        );
        assert!(publish_selection_seal(&output, &json!({}), &[&snapshot]).is_err());
        std::fs::write(&output, b"replacement").unwrap();
        assert!(sealed.recheck().is_err());
        let changed = dir.path().join("changed");
        let result = publish_selection_seal_with_sync(
            &changed,
            &json!({}),
            &[&snapshot],
            |file, dir_sync| {
                file.sync_all()?;
                if dir_sync {
                    std::fs::write(&input, b"changed")?;
                }
                Ok(())
            },
        );
        assert!(result.is_err());
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(&changed).unwrap()).unwrap()["status"],
            "INVALID"
        );
    }

    #[test]
    fn leaf_coverage_actual_report_publication_invalidates_success_on_sync_or_sq8_mutation() {
        let dir = tempfile::tempdir().unwrap();
        for case in 0..4 {
            let input = dir.path().join(format!("sq8-{case}"));
            std::fs::write(&input, b"ranked payload").unwrap();
            let file = header_file(&input).unwrap();
            let snapshot = HeaderSnapshot {
                identity: file_identity(&file).unwrap(),
                file,
                path: input.clone(),
            };
            let output = dir.path().join(format!("report-{case}"));
            let result = execute_leaf_coverage_with(
                &output,
                || {
                    Ok((
                        json!({"schema":COVERAGE_SCHEMA,"status":"MEASURED","complete":true}),
                        vec![snapshot],
                    ))
                },
                |file, is_directory| {
                    if case == 0 && !is_directory || case == 1 && is_directory {
                        return Err(io::Error::other("injected report sync failure"));
                    }
                    file.sync_all()?;
                    if case == 2 && is_directory {
                        std::fs::write(&input, b"mutated SQ8")?;
                    }
                    if case == 3 && is_directory {
                        std::fs::rename(&output, dir.path().join("original-report-moved"))?;
                        std::fs::write(&output, b"foreign replacement")?;
                    }
                    Ok(())
                },
            );
            assert!(result.is_err());
            if case == 3 {
                assert_eq!(std::fs::read(&output).unwrap(), b"foreign replacement");
                let moved: Value = serde_json::from_slice(
                    &std::fs::read(dir.path().join("original-report-moved")).unwrap(),
                )
                .unwrap();
                assert_eq!(moved["status"], "INVALID");
                continue;
            }
            let artifact: Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
            assert_eq!(artifact["status"], "INVALID");
            assert_eq!(artifact["complete"], false);
        }
        let occupied = dir.path().join("occupied");
        std::fs::write(&occupied, b"existing receipt").unwrap();
        let mut reducer_called = false;
        assert!(
            execute_leaf_coverage_with(
                &occupied,
                || {
                    reducer_called = true;
                    Err("must stay unopened".into())
                },
                |file, _| file.sync_all()
            )
            .is_err()
        );
        assert!(!reducer_called);
        assert_eq!(std::fs::read(&occupied).unwrap(), b"existing receipt");
    }

    #[test]
    fn leaf_coverage_requests_and_whole_sq8_mapping_refuse_rehashed_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let requests = dir.path().join("requests");
        let bytes = 3_f32.to_le_bytes();
        std::fs::write(&requests, bytes).unwrap();
        let request_pin = CompletedInput {
            path: requests.clone(),
            bytes: 4,
            sha256: sha(&bytes),
        };
        assert!(loss_binary(&request_pin, 4).is_ok());
        std::fs::write(&requests, 5_f32.to_le_bytes()).unwrap();
        assert!(loss_binary(&request_pin, 4).is_err());
        let path = dir.path().join("sq8");
        let mut original = Vec::new();
        for id in [2_i64, 0, 1] {
            original.extend_from_slice(&id.to_le_bytes());
            original.extend_from_slice(&1_f32.to_le_bytes());
            original.extend_from_slice(&[0, 1]);
        }
        let order = [2_i64, 0, 1]
            .into_iter()
            .flat_map(i64::to_le_bytes)
            .collect::<Vec<_>>();
        let wanted = BTreeSet::from([0, 1, 2]);
        for case in 0..4 {
            let mut body = original.clone();
            if case == 1 {
                body[14..22].copy_from_slice(&2_i64.to_le_bytes());
            }
            if case == 2 {
                body[8..12].copy_from_slice(&0_f32.to_le_bytes());
            }
            if case == 3 {
                body[..8].copy_from_slice(&0_i64.to_le_bytes());
                body[14..22].copy_from_slice(&2_i64.to_le_bytes());
            }
            std::fs::write(&path, &body).unwrap();
            let pin = CompletedInput {
                path: path.clone(),
                bytes: body.len() as u64,
                sha256: sha(&body),
            };
            let result = loss_positions(&pin, 3, 2, &sha(&order), &wanted);
            if case == 0 {
                assert_eq!(
                    result.unwrap().0,
                    std::collections::BTreeMap::from([(0, 1), (1, 2), (2, 0)])
                );
            } else {
                assert!(result.is_err(), "rehashed mapping case {case}");
            }
        }
    }

    #[test]
    fn leaf_coverage_exact_control_and_gain_loss_gate_refuse_mutations() {
        use borsuk::semantic_unit_router::Nomination;
        let (i, q) = sealed_loss_fixture();
        let units = BTreeSet::from([0, 16, 24]);
        let closure = BTreeSet::from([0, 2, 3]);
        let (ranges, bytes) = borsuk::two_bit_generation::plan_two_bit_source_cover(
            &closure,
            i.rows,
            source_record_bytes(i.dimensions).unwrap(),
            128,
            64 * 1024 * 1024,
        )
        .unwrap();
        let arm = CoverageArm {
            nomination: Nomination {
                leaf_ids: vec![0],
                units,
                seed_page: 0,
                walk_units: vec![0, 1, 2, 3, 4, 5, 6, 7, 16, 24],
                seed_additions: vec![1, 2, 3, 4, 5, 6, 7],
                page_closure: closure,
            },
            ranges,
            bytes,
        };
        assert!(
            coverage_control(
                &serde_json::to_vec(&q).unwrap(),
                &serde_json::from_value(q.clone()).unwrap(),
                &i,
                &arm
            )
            .is_ok()
        );
        for (field, replacement) in [
            ("semantic_leaves", json!([1])),
            ("semantic_units", json!([0, 16, 25])),
            ("semantic_seed_additions", json!([1, 2, 3, 4, 5, 6])),
            (
                "nomination_evaluated_units",
                json!([0, 1, 2, 3, 4, 5, 6, 7, 16]),
            ),
        ] {
            let mut bad = q.clone();
            bad["trace"][field] = replacement;
            assert!(
                coverage_control(
                    &serde_json::to_vec(&bad).unwrap(),
                    &serde_json::from_value(bad).unwrap(),
                    &i,
                    &arm
                )
                .is_err(),
                "{field}"
            );
        }
        assert!(!coverage_survives(314, 314, 1, 1));
        assert!(coverage_survives(314, 315, 2, 1));
        assert!(!coverage_survives(314, 315, 1, 1));
        assert!(!coverage_survives(313, 315, 2, 0));
    }

    #[test]
    fn sealed_loss_bridge_source_completion_and_disjoint_buckets() {
        let (i, q) = sealed_loss_fixture();
        let coverage = loss_coverage(
            &serde_json::to_vec(&q).unwrap(),
            &serde_json::from_value(q.clone()).unwrap(),
            &i,
        )
        .unwrap();
        assert_eq!(loss_bucket(&coverage, 0, true), "source_covered_hit");
        assert_eq!(loss_bucket(&coverage, 256, true), "bridge_rescued_hit");
        assert_eq!(loss_bucket(&coverage, 256, false), "covered_ranking_loss");
        assert_eq!(loss_bucket(&coverage, 512, false), "covered_ranking_loss");
        assert_eq!(
            loss_bucket(&coverage, 800, false),
            "source_to_sq8_nomination_loss"
        );
        assert_eq!(
            loss_bucket(&coverage, 1100, false),
            "outside_source_and_sq8"
        );
        assert_eq!(
            loss_bucket(&coverage, 1100, true),
            "INVALID_RETURNED_OUTSIDE_SQ8"
        );
        // The final physical page may be partial; completion clips units to actual rows.
        let mut tail = i;
        tail.rows = 1000;
        assert!(
            loss_coverage(
                &serde_json::to_vec(&q).unwrap(),
                &serde_json::from_value(q.clone()).unwrap(),
                &tail
            )
            .is_err()
        ); // old SOURCE charge refuses
        let mut q = q;
        let (ranges, bytes) = borsuk::two_bit_generation::plan_two_bit_source_cover(
            &BTreeSet::from([0, 2, 3]),
            tail.rows,
            source_record_bytes(tail.dimensions).unwrap(),
            128,
            64 * 1024 * 1024,
        )
        .unwrap();
        q["charges"]["source"] =
            json!({"submitted_gets":ranges.len(),"verified_bytes":bytes,"failed_gets":0});
        let cov = loss_coverage(
            &serde_json::to_vec(&q).unwrap(),
            &serde_json::from_value(q).unwrap(),
            &tail,
        )
        .unwrap();
        assert_eq!(
            loss_bucket(&cov, 999, false),
            "source_to_sq8_nomination_loss"
        );
    }

    #[test]
    fn sealed_loss_refuses_incomplete_duplicate_out_of_cover_and_charge_evidence() {
        let (i, q) = sealed_loss_fixture();
        for case in 0..8 {
            let mut bad = q.clone();
            match case {
                0 => {
                    bad["trace"]["nomination_evaluated_units"]
                        .as_array_mut()
                        .unwrap()
                        .pop();
                }
                1 => bad["trace"]["nomination_evaluated_units"][1] = json!(0),
                2 => bad["trace"]["semantic_seed_additions"][0] = json!(9),
                3 => bad["charges"]["source"]["verified_bytes"] = json!(1),
                4 => bad["plan"]["selected_pages"] = json!([0, 4]),
                5 => bad["plan"]["ranges"][0][1] = json!(100),
                6 => bad["charges"]["sq8"]["verified_bytes"] = json!(1),
                _ => bad["source_nomination_skipped"] = json!(true),
            }
            assert!(
                loss_coverage(
                    &serde_json::to_vec(&bad).unwrap(),
                    &serde_json::from_value(bad).unwrap(),
                    &i
                )
                .is_err(),
                "{case}"
            );
        }
    }

    #[test]
    fn sealed_loss_authenticates_nonidentity_order_bijection_and_file_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sq8");
        let pack = |ids: &[u64]| {
            ids.iter()
                .flat_map(|id| {
                    let mut record = id.to_le_bytes().to_vec();
                    record.extend_from_slice(&1_f32.to_le_bytes());
                    record.extend_from_slice(&[0, 1, 2, 3]);
                    record
                })
                .collect::<Vec<_>>()
        };
        let ids = (0..64_u64).rev().collect::<Vec<_>>();
        let order_sha = sha(&ids
            .iter()
            .flat_map(|id| id.to_le_bytes())
            .collect::<Vec<_>>());
        let body = pack(&ids);
        std::fs::write(&path, &body).unwrap();
        let mut pin = CompletedInput {
            path: path.clone(),
            bytes: body.len() as u64,
            sha256: sha(&body),
        };
        let wanted = BTreeSet::from([3, 44]);
        let (positions, snapshot) = loss_positions(&pin, 64, 4, &order_sha, &wanted).unwrap();
        assert_eq!(positions[&3], 60);
        assert_eq!(positions[&44], 19);
        std::fs::rename(&path, dir.path().join("old")).unwrap();
        std::fs::write(&path, &body).unwrap();
        assert!(snapshot.recheck().is_err());
        for values in [vec![0_u64; 64], (0..63_u64).chain([64]).collect()] {
            let bad = pack(&values);
            std::fs::write(&path, &bad).unwrap();
            pin.sha256 = sha(&bad);
            assert!(loss_positions(&pin, 64, 4, &order_sha, &wanted).is_err());
        }
        for norm in [0_f32, -1., f32::NAN, f32::INFINITY] {
            let mut bad = body.clone();
            bad[8..12].copy_from_slice(&norm.to_le_bytes());
            std::fs::write(&path, &bad).unwrap();
            pin.sha256 = sha(&bad);
            assert!(loss_positions(&pin, 64, 4, &order_sha, &wanted).is_err());
        }
        std::fs::write(&path, &body).unwrap();
        pin.sha256 = sha(&body);
        assert!(loss_positions(&pin, 64, 4, &"0".repeat(64), &wanted).is_err());
        let mut changed_codes = body.clone();
        changed_codes[12] ^= 1;
        std::fs::write(&path, &changed_codes).unwrap();
        assert!(loss_positions(&pin, 64, 4, &order_sha, &wanted).is_err());
        pin.sha256 = sha(&body);
        std::fs::write(&path, &body[..body.len() - 1]).unwrap();
        assert!(loss_positions(&pin, 64, 4, &order_sha, &wanted).is_err());
        let mut grown = body.clone();
        grown.push(0);
        std::fs::write(&path, &grown).unwrap();
        assert!(loss_positions(&pin, 64, 4, &order_sha, &wanted).is_err());
        std::fs::write(&path, &body).unwrap();
        let link = dir.path().join("link");
        symlink(&path, &link).unwrap();
        pin.path = link;
        assert!(loss_positions(&pin, 64, 4, &order_sha, &wanted).is_err());
    }

    #[test]
    fn sealed_loss_actual_native_panel_reconciles_recall_and_refuses_before_truth() {
        let (dir, result) = scale_native_runner::scale_reducer_native_fixture(false, true).unwrap();
        let current_rows = assert_current_native(dir.path(), &result);
        let mut rows = current_rows.clone();
        let (current_path, current_sha) = scale_config(dir.path(), &result, &rows);
        let (current, _): (CompletedConfig, _) = read_config(&current_path, &current_sha).unwrap();
        let refused = json!({"schema":"borsuk-sealed-scale-loss-config-v2",
            "completed":current,"sq8":{"path":dir.path().join("unopened-sq8"),"bytes":1,"sha256":"0".repeat(64)},
            "truth":{"path":dir.path().join("unopened-truth"),"bytes":1,"sha256":"0".repeat(64)},
            "max_payload_bytes":67108864});
        let refused_bytes = serde_json::to_vec(&refused).unwrap();
        let refused_path = dir.path().join("current-loss-refusal.json");
        std::fs::write(&refused_path, &refused_bytes).unwrap();
        assert_eq!(
            reduce_loss(&refused_path, &sha(&refused_bytes))
                .unwrap_err()
                .to_string(),
            "historical 256-row analysis only; v10 requires current-format analysis"
        );
        // SYNTHETIC legacy boundary: the tiny partial tail has the same payload
        // span at 32/256 rows. This cannot establish historical producer parity.
        rows = synthetic_historical_rows(&current_rows, "borsuk-cohere-native-baseline-result-v8");
        authenticate(&mut rows);
        let body = String::from_utf8(encode(&rows)).unwrap();
        std::fs::write(&result, &body).unwrap();
        let (completed_path, completed_sha) = scale_config(dir.path(), &result, &rows);
        let (completed, _): (CompletedConfig, _) =
            read_config(&completed_path, &completed_sha).unwrap();
        let sq8_path = dir.path().join("sq8");
        let sq8 = std::fs::read(&sq8_path).unwrap();
        let truth = std::fs::read(dir.path().join("truth.u64")).unwrap();
        let mut config = json!({"schema":"borsuk-sealed-scale-loss-config-v2",
            "completed":{"schema":completed.schema,"input":{"path":completed.input.path,
                "bytes":completed.input.bytes,"sha256":completed.input.sha256},
                "expected_identity":completed.expected_identity,
                "expected_bound_inputs":completed.expected_bound_inputs},
            "sq8":{"path":sq8_path,"bytes":sq8.len(),"sha256":sha(&sq8)},
            "truth":{"path":dir.path().join("truth.u64"),"bytes":truth.len(),"sha256":sha(&truth)},
            "max_payload_bytes":64*1024*1024});
        let path = dir.path().join("loss.json");
        let bytes = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        let report = reduce_loss(&path, &sha(&bytes)).unwrap();
        assert_eq!(report["query_count"], 1);
        assert_eq!(report["hits10"], 10);
        assert_eq!(report["misses10"], 0);
        assert_eq!(report["totals"], json!({"source_covered_hit": 10}));
        let entries = report["queries"][0]["truth_entries"].as_array().unwrap();
        let entry = |id: u64| entries.iter().find(|entry| entry["id"] == id).unwrap();
        assert_eq!(entry(21)["physical_row"], 10);
        assert_eq!(entry(8)["physical_row"], 31);
        assert!(
            entries
                .iter()
                .all(|entry| entry["source_scored"] == true && entry["sq8_ranked"] == true)
        );
        assert_eq!(
            report["truth_opened_after_complete_producer_validation"],
            true
        );
        assert_eq!(report["native_ann_called"], false);
        let original_query = rows.iter().find(|r| r["phase"] == "query").unwrap();
        assert_eq!(report["queries"][0]["returned"], original_query["returned"]);
        config["schema"] = json!("borsuk-sealed-scale-loss-config-v1");
        let bytes = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            reduce_loss(&path, &sha(&bytes)).unwrap_err().to_string(),
            "sealed loss config/schema"
        );
        config["schema"] = json!("borsuk-sealed-scale-loss-config-v2");
        config["max_payload_bytes"] = json!(1);
        config["truth"]["path"] = json!(dir.path().join("truth-must-not-open"));
        let bytes = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            reduce_loss(&path, &sha(&bytes)).unwrap_err().to_string(),
            "loss modeled payload admission"
        );
        config["max_payload_bytes"] = json!(64 * 1024 * 1024);
        // A complete body pin alone cannot make a truncated producer/seal acceptable.
        let bad_path = dir.path().join("truncated.jsonl");
        let bad = &body.as_bytes()[..body.len() - 10];
        std::fs::write(&bad_path, bad).unwrap();
        config["completed"]["input"] = json!({"path":bad_path,"bytes":bad.len(),"sha256":sha(bad)});
        let bytes = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        let error = reduce_loss(&path, &sha(&bytes)).unwrap_err().to_string();
        assert!(
            !error.contains("No such file"),
            "truncated producer must refuse before inaccessible truth: {error}"
        );
    }

    fn source_config_value(dir: &Path, rows: &[Value], bytes: &[u8]) -> Value {
        let input = dir.join("source-evidence.jsonl");
        std::fs::write(&input, bytes).unwrap();
        json!({"schema":"borsuk-source-utilization-config-v1",
            "input":{"path":input,"bytes":bytes.len(),"sha256":sha(bytes)},
            "expected_identity":rows[0],"expected_bound_inputs":rows[1],
            "producer_source_commit":"1".repeat(40),"producer_source_archive_sha256":"2".repeat(64),
            "record_bytes":12,"source_get_cap":128,"source_byte_cap":67108864,"source_unit_cap":2544,"direct_sq8_get_cap":32,"historical_sq8_query_byte_cap":16773120})
    }

    fn source_write_config(dir: &Path, config: &Value) -> (PathBuf, String) {
        let bytes = serde_json::to_vec(config).unwrap();
        let path = dir.join("source-config.json");
        std::fs::write(&path, &bytes).unwrap();
        (path, sha(&bytes))
    }

    fn source_query_fixture(
        row: &Value,
        input: &Value,
        c: &SourceUtilizationConfig,
    ) -> Result<SourceUtilizationQuery> {
        source_utilization_query(
            &serde_json::to_vec(row)?,
            &serde_json::from_value(row.clone())?,
            &serde_json::from_value(input.clone())?,
            c,
        )
    }

    #[test]
    fn source_utilization_unwalked_winner_full_completion_and_partial_tail() {
        let dir = tempfile::tempdir().unwrap();
        let rows = source_fixture();
        let value = source_config_value(dir.path(), &rows, &encode(&rows));
        let mut c: SourceUtilizationConfig = serde_json::from_value(value.clone()).unwrap();
        c.validate().unwrap();
        let full = source_query_fixture(&rows[5], &rows[1], &c).unwrap();
        assert_eq!(
            (
                full.baseline_bytes,
                full.ideal_bytes,
                full.potential_saving_bytes
            ),
            (6144, 6144, 0)
        );
        assert_eq!((full.walked_units, full.completion_units), (9, 7));
        assert!(!full.completion_limited);

        // A toy score makes unwalked unit 9 change the winning page. The trace
        // records that completed population; walked-only bytes omit the winner.
        let scores = [(0, 1), (8, 0), (9, 2)];
        assert_eq!(
            scores.iter().max_by_key(|(_, score)| score).unwrap().0 / 8,
            1
        );
        assert_eq!(
            scores[..2].iter().max_by_key(|(_, score)| score).unwrap().0 / 8,
            0
        );
        let mut bounded = rows[5].clone();
        bounded["trace"]["nomination_evaluated_units"] = json!((0..12).collect::<Vec<_>>());
        c.source_unit_cap = 12;
        let b = source_query_fixture(&bounded, &rows[1], &c).unwrap();
        assert_eq!(
            (
                b.ideal_bytes,
                b.potential_saving_bytes,
                b.completion_payload_bytes
            ),
            (4608, 1536, 1152)
        );
        assert!(b.completion_limit_reached && b.completion_limited);
        let walked = (0..9).collect::<BTreeSet<_>>();
        let (_, walked_only) = source_cover::cover_pages(&walked, 512, 12, 32, 128).unwrap();
        assert_eq!(walked_only, 3456);
        assert!(walked_only < b.ideal_bytes as usize);

        // The final authenticated unit contains only one row, not 32.
        let mut tail = rows[5].clone();
        let mut input = rows[1].clone();
        input["rows"] = json!(513);
        tail["trace"]["semantic_units"] = json!([0, 8, 16]);
        tail["trace"]["ranked_candidate_pages"] = json!([1, 0, 2]);
        tail["trace"]["nomination_evaluated_units"] = json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 16, 9, 10]);
        tail["charges"]["source"]["verified_bytes"] = json!(6156);
        let t = source_query_fixture(&tail, &input, &c).unwrap();
        assert_eq!(
            (
                t.walked_payload_bytes,
                t.scored_payload_bytes,
                t.completion_payload_bytes
            ),
            (3468, 4236, 768)
        );
        assert_eq!(
            (t.baseline_bytes, t.ideal_bytes, t.potential_saving_bytes),
            (6156, 4236, 1920)
        );
        assert!(t.completion_limited);
        c.source_unit_cap = 13;
        assert!(source_query_fixture(&tail, &input, &c).is_err());
    }

    #[test]
    fn source_utilization_shared_cover_tied_gaps_same_128_gets() {
        // 129 disjoint pages force exactly one bridge under the frozen 128 GETs.
        // All gaps tie, so the shared production helper bridges the first gap.
        let pages = (0..129).map(|p| p * 2).collect::<BTreeSet<_>>();
        let (ranges, bytes) = source_cover::cover_pages(&pages, 65537, 12, 256, 128).unwrap();
        assert_eq!(ranges.len(), 128);
        assert_eq!(ranges[0], 0..9216);
        assert_eq!(ranges.last().unwrap(), &(786432..786444));
        assert_eq!(bytes, 396300);
        let units = pages
            .iter()
            .flat_map(|&p| p * 8..(p + 1) * 8)
            .filter(|&u| u < 65537_usize.div_ceil(32))
            .collect::<BTreeSet<_>>();
        let (ideal, ideal_bytes) = source_cover::cover_pages(&units, 65537, 12, 32, 128).unwrap();
        assert_eq!((ideal, ideal_bytes), (ranges, bytes));
        assert!(source_cover::cover_pages(&pages, 65537, 12, 256, 0).is_err());
        assert!(source_cover::cover_pages(&pages, 65537, usize::MAX, 256, 128).is_err());
    }

    #[test]
    fn source_utilization_trace_and_charge_refusals() {
        let dir = tempfile::tempdir().unwrap();
        let rows = source_fixture();
        let value = source_config_value(dir.path(), &rows, &encode(&rows));
        let c: SourceUtilizationConfig = serde_json::from_value(value).unwrap();
        for case in 0..22 {
            let mut q = rows[5].clone();
            match case {
                0 => q["trace"]["semantic_units"] = json!([0, 0, 8]),
                1 => q["trace"]["semantic_units"] = json!([0, 16]),
                2 => q["trace"]["semantic_units"] = json!([8, 0]),
                3 => q["trace"]["semantic_seed_additions"] = json!([1, 2, 3, 4, 5, 6, 6]),
                4 => q["trace"]["semantic_seed_additions"] = json!([1, 2, 3, 4, 5, 6]),
                5 => {
                    q["trace"]["nomination_evaluated_units"] =
                        json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 11, 12, 13, 14, 15])
                }
                6 => q["trace"]["nomination_evaluated_units"][15] = json!(16),
                7 => q["trace"]["nomination_evaluated_units"]
                    .as_array_mut()
                    .unwrap()
                    .swap(0, 1),
                8 => q["trace"]["nomination_evaluated_units"]
                    .as_array_mut()
                    .unwrap()
                    .swap(9, 10),
                9 => {
                    q["trace"]["nomination_evaluated_units"]
                        .as_array_mut()
                        .unwrap()
                        .pop();
                }
                10 => q["trace"]["ranked_candidate_pages"] = json!([1, 1]),
                11 => q["trace"]["ranked_candidate_pages"] = json!([1, 2]),
                12 => q["trace"]["primary_page"] = json!(0),
                13 => q["trace"]["semantic_leaves"] = json!([0, 0]),
                14 => q["charges"]["source"]["submitted_gets"] = json!(2),
                15 => q["charges"]["source"]["verified_bytes"] = json!(6143),
                16 => q["charges"]["source"]["failed_gets"] = json!(1),
                17 => q["trace"]["discoveries"] = json!([{"seed_page":0}]),
                18 => q["trace"]["ignored"] = json!(true),
                19 => {
                    q["trace"]["semantic_units"] = json!([0]);
                    q["trace"]["ranked_candidate_pages"] = json!([0]);
                    q["trace"]["primary_page"] = json!(0);
                }
                20 => q["trace"]["semantic_seed_additions"] = json!([0, 1, 2, 3, 4, 5, 6]),
                _ => q["trace"]["semantic_leaves"] = json!([16]),
            }
            assert!(
                source_query_fixture(&q, &rows[1], &c).is_err(),
                "case {case}"
            );
        }
        let mut c = c;
        c.source_byte_cap = 6143;
        assert!(source_query_fixture(&rows[5], &rows[1], &c).is_err());
    }

    #[test]
    fn source_utilization_authenticated_report_and_legacy_preservation() {
        let dir = tempfile::tempdir().unwrap();
        let rows = source_fixture();
        let bytes = encode(&rows);
        let c = source_config_value(dir.path(), &rows, &bytes);
        let (config, pin) = source_write_config(dir.path(), &c);
        let report = reduce_source_utilization(&config, &pin).unwrap();
        assert_eq!(report["count"], 1000);
        assert_eq!(report["queries"].as_array().unwrap().len(), 1000);
        assert_eq!(report["queries"][999]["ordinal"], 999);
        assert_eq!(
            report["aggregate"]["baseline_bytes"],
            json!({"total":6144000,"median":6144,"p95":6144,"max":6144})
        );
        assert_eq!(
            report["aggregate"]["completion_payload_bytes"]["total"],
            2688000
        );
        assert_eq!(report["aggregate"]["potential_saving_bytes"]["total"], 0);
        assert_eq!(report["completion_limited_frequency"], 0.0);
        assert_eq!(report["optimistic_hindsight"], true);
        assert_eq!(report["counterfactual_cost_only"], true);
        assert_eq!(report["score_or_recall_evaluated"], false);
        assert_eq!(report["direct_sq8_get_cap"], 32);
        assert_eq!(report["historical_sq8_query_byte_cap"], 16773120);
        assert_eq!(
            report["direct_closure_sq8_exceeds_historical_byte_cap_queries"],
            0
        );
        assert_eq!(
            report["aggregate"]["direct_closure_sq8_bytes"],
            json!({"total":14336000,"median":14336,"p95":14336,"max":14336})
        );
        assert_eq!(
            report["aggregate"]["mandatory_closure_sq8_bytes"]["total"],
            14336000
        );
        assert_eq!(
            report["aggregate"]["direct_closure_sq8_bridge_bytes"]["total"],
            0
        );
        assert_eq!(
            report["aggregate"]["original_source_sq8_bytes"]["total"],
            13312000
        );
        assert_eq!(
            report["aggregate"]["direct_minus_original_bytes"],
            json!({"total":1024000,"median":1024,"p95":1024,"max":1024})
        );
        assert_eq!(
            report["aggregate"]["direct_minus_original_gets"],
            json!({"total":-1000,"median":-1,"p95":-1,"max":-1})
        );
        for key in [
            "production_change",
            "claims_quality",
            "claims_latency",
            "qualified",
        ] {
            assert_eq!(report[key], false);
        }
        assert_eq!(
            report["reducer_source_sha256"],
            sha(include_bytes!("compare_native_replay.rs"))
        );
        assert!((serde_json::to_vec(&report).unwrap().len() as u64) < REPORT_CAP);
        let output = dir.path().join("source-report.json");
        assert!(
            execute_report(&output, SOURCE_UTILIZATION_SCHEMA, || {
                reduce_source_utilization(&config, &pin)
            })
            .unwrap()
        );
        let original = std::fs::read(&output).unwrap();
        assert!(
            execute_report(&output, SOURCE_UTILIZATION_SCHEMA, || panic!(
                "occupied output invoked reduction"
            ))
            .is_err()
        );
        assert_eq!(std::fs::read(&output).unwrap(), original);
        let (legacy, legacy_pin) = v2_config(dir.path(), &bytes, &rows);
        assert_eq!(
            reduce_completed(&legacy, &legacy_pin).unwrap()["native_terminal"]["charges"]["source"]
                ["verified_bytes"],
            6144000
        );
        assert_eq!(
            compare(&four(dir.path())).unwrap()["schema"],
            "borsuk-compare-native-replay-v1"
        );
    }

    #[test]
    fn source_utilization_report_cap_refuses_before_publication() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("oversize-report.json");
        let oversized = json!({"status":"MEASURED","evidence":"x".repeat(REPORT_CAP as usize)});
        assert!(source_report_cap(&oversized).is_err());
        assert!(
            !execute_report(&output, SOURCE_UTILIZATION_SCHEMA, || {
                source_report_cap(&oversized)?;
                Ok(oversized)
            })
            .unwrap()
        );
        let bytes = std::fs::read(&output).unwrap();
        assert!((bytes.len() as u64) < REPORT_CAP);
        let report: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(report["status"], "INVALID");
        assert_eq!(report["error"], "source report cap before publication");
        assert!(report.get("evidence").is_none());
    }

    #[test]
    fn source_utilization_positive_bound_at_historical_completion_cap() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = source_fixture();
        rows[1]["rows"] = json!(100000);
        rows[1]["query_source_first"] = json!(100000);
        let semantic = (0..319).map(|page| page * 8).collect::<Vec<_>>();
        let mut scored = (0..8)
            .chain(semantic.iter().copied().skip(1))
            .collect::<Vec<_>>();
        for page in 1..319 {
            for unit in page * 8 + 1..(page + 1) * 8 {
                if scored.len() < 2544 {
                    scored.push(unit);
                }
            }
        }
        assert_eq!(scored.len(), 2544);
        let q = &mut rows[1004];
        q["trace"]["semantic_units"] = json!(semantic);
        q["trace"]["nomination_evaluated_units"] = json!(scored);
        q["trace"]["ranked_candidate_pages"] = json!((0..319).collect::<Vec<_>>());
        q["trace"]["primary_page"] = json!(0);
        q["charges"]["source"]["verified_bytes"] = json!(979968);
        q["sum"]["verified_bytes"] = json!(987236);
        let t = &mut rows.last_mut().unwrap()["summary"];
        t["charges"]["source"]["verified_bytes"] = json!(7117824);
        t["sum"]["verified_bytes"] = json!(14385824);
        authenticate(&mut rows);
        let value = source_config_value(dir.path(), &rows, &encode(&rows));
        let (path, pin) = source_write_config(dir.path(), &value);
        let report = reduce_source_utilization(&path, &pin).unwrap();
        assert_eq!(
            report["aggregate"]["potential_saving_bytes"],
            json!({"total":3072,"median":0,"p95":0,"max":3072})
        );
        assert_eq!(report["queries"][999]["scored_units"], 2544);
        assert_eq!(report["queries"][999]["completion_units"], 2218);
        assert_eq!(report["queries"][999]["ideal_gets"], 2);
        assert_eq!(report["completion_limited_queries"], 1);
        assert_eq!(report["completion_limited_frequency"], 0.001);
    }

    #[test]
    fn source_utilization_direct_closure_sq8_hand_counted_costs() {
        let dir = tempfile::tempdir().unwrap();
        let rows = source_fixture();
        let mut config = source_config_value(dir.path(), &rows, &encode(&rows));
        config["historical_sq8_query_byte_cap"] = json!(14336);
        let c: SourceUtilizationConfig = serde_json::from_value(config).unwrap();
        c.validate().unwrap();
        let contiguous = source_query_fixture(&rows[5], &rows[1], &c).unwrap();
        assert!(!contiguous.direct_closure_sq8_exceeds_historical_byte_cap);
        assert!(!contiguous.completion_limited);
        assert_eq!(contiguous.ideal_bytes, contiguous.baseline_bytes);
        assert_eq!(
            (
                contiguous.direct_closure_sq8_bytes,
                contiguous.mandatory_closure_sq8_bytes,
                contiguous.direct_closure_sq8_bridge_bytes
            ),
            (14336, 14336, 0)
        );
        assert_eq!(
            (
                contiguous.original_source_sq8_bytes,
                contiguous.direct_minus_original_bytes,
                contiguous.direct_minus_original_gets
            ),
            (13312, 1024, -1)
        );
        // Two discovered units expand to both complete 256-row pages, even
        // when fewer units were scored by bounded source completion.
        let mut bounded = rows[5].clone();
        bounded["trace"]["nomination_evaluated_units"] = json!((0..12).collect::<Vec<_>>());
        let mut bounded_config = c;
        bounded_config.source_unit_cap = 12;
        let cost = source_query_fixture(&bounded, &rows[1], &bounded_config).unwrap();
        assert_eq!(cost.scored_units, 12);
        assert_eq!(cost.direct_closure_sq8_bytes, 14336);
        bounded_config.source_unit_cap = 2544;

        // Already fetching both pages makes the static byte difference negative.
        let mut complete = rows[5].clone();
        complete["charges"]["sq8"]["verified_bytes"] = json!(14336);
        complete["sum"]["verified_bytes"] = json!(20580);
        let complete = source_query_fixture(&complete, &rows[1], &bounded_config).unwrap();
        assert!(!complete.direct_closure_sq8_exceeds_historical_byte_cap);
        assert_eq!(
            (
                complete.original_source_sq8_bytes,
                complete.direct_minus_original_bytes
            ),
            (20480, -6144)
        );

        let mut tail = rows[5].clone();
        let mut input = rows[1].clone();
        input["rows"] = json!(513);
        tail["trace"]["semantic_units"] = json!([0, 8, 16]);
        tail["trace"]["ranked_candidate_pages"] = json!([1, 0, 2]);
        tail["trace"]["nomination_evaluated_units"] =
            json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 16, 9, 10, 11, 12, 13, 14, 15]);
        tail["charges"]["source"]["verified_bytes"] = json!(6156);
        tail["sum"]["verified_bytes"] = json!(13424);
        let tail = source_query_fixture(&tail, &input, &bounded_config).unwrap();
        assert!(tail.direct_closure_sq8_exceeds_historical_byte_cap);
        assert!(!tail.completion_limited);
        assert_eq!(tail.ideal_bytes, tail.baseline_bytes);
        assert_eq!(
            (
                tail.direct_closure_sq8_bytes,
                tail.mandatory_closure_sq8_bytes,
                tail.direct_closure_sq8_bridge_bytes
            ),
            (14364, 14364, 0)
        );
        assert_eq!(
            (
                tail.original_source_sq8_bytes,
                tail.direct_minus_original_bytes
            ),
            (13324, 1040)
        );

        // 33 fragmented closures exceed 32 GETs by one; tied gaps force one
        // 256-row bridge. The final mandatory page has just one physical row.
        let pages = (0..33).map(|p| p * 2).collect::<BTreeSet<_>>();
        let semantic = pages.iter().map(|&p| p * 8).collect::<Vec<_>>();
        let mut scored = (0..8)
            .chain(semantic.iter().copied().skip(1))
            .collect::<Vec<_>>();
        for &page in pages.iter().skip(1) {
            scored.extend(
                (page * 8 + 1..(page + 1) * 8).filter(|&unit| unit < 16385_usize.div_ceil(32)),
            );
        }
        let mut fragmented = rows[5].clone();
        input["rows"] = json!(16385);
        fragmented["trace"]["semantic_units"] = json!(semantic);
        fragmented["trace"]["ranked_candidate_pages"] = json!(pages);
        fragmented["trace"]["primary_page"] = json!(0);
        fragmented["trace"]["nomination_evaluated_units"] = json!(scored);
        fragmented["charges"]["source"] =
            json!({"submitted_gets":33,"verified_bytes":98316,"failed_gets":0});
        fragmented["sum"] = json!({"submitted_gets":35,"verified_bytes":105584,"failed_gets":0});
        let f = source_query_fixture(&fragmented, &input, &bounded_config).unwrap();
        assert!(f.direct_closure_sq8_exceeds_historical_byte_cap);
        assert!(!f.completion_limited);
        assert_eq!(f.ideal_bytes, f.baseline_bytes);
        assert_eq!((f.baseline_gets, f.direct_closure_sq8_gets), (33, 32));
        assert_eq!(
            (
                f.direct_closure_sq8_bytes,
                f.mandatory_closure_sq8_bytes,
                f.direct_closure_sq8_bridge_bytes
            ),
            (236572, 229404, 7168)
        );
        assert_eq!(
            (
                f.original_source_sq8_bytes,
                f.original_source_sq8_gets,
                f.direct_minus_original_bytes,
                f.direct_minus_original_gets
            ),
            (105484, 34, 131088, -2)
        );
        let (ranges, bytes) = source_cover::cover_pages(&pages, 16385, 28, 256, 32).unwrap();
        assert_eq!(ranges[0], 0..21504);
        assert_eq!(ranges.last().unwrap(), &(458752..458780));
        assert_eq!(bytes, 236572);
    }

    #[test]
    fn source_utilization_direct_closure_sq8_charge_refusals() {
        let dir = tempfile::tempdir().unwrap();
        let rows = source_fixture();
        let config = source_config_value(dir.path(), &rows, &encode(&rows));
        let c: SourceUtilizationConfig = serde_json::from_value(config).unwrap();
        for (field, value) in [
            ("failed_gets", 1),
            ("submitted_gets", 0),
            ("submitted_gets", 33),
            ("submitted_gets", 2),
            ("verified_bytes", 0),
            ("verified_bytes", 28),
            ("verified_bytes", 7169),
            ("verified_bytes", 14364),
        ] {
            let mut q = rows[5].clone();
            q["charges"]["sq8"][field] = json!(value);
            assert!(
                source_query_fixture(&q, &rows[1], &c).is_err(),
                "{field}={value}"
            );
        }
        // Authenticate the unchanged complete fixture/config at the historical
        // cap boundary; counterfactual exceedances are evidence, not refusals.
        for cap in [7167, 7168] {
            let mut config = source_config_value(dir.path(), &rows, &encode(&rows));
            config["historical_sq8_query_byte_cap"] = json!(cap);
            let (path, pin) = source_write_config(dir.path(), &config);
            let result = reduce_source_utilization(&path, &pin);
            if cap == 7167 {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "recorded original SQ8 bytes exceed historical query cap"
                );
            } else {
                let report = result.unwrap();
                assert_eq!(report["historical_sq8_query_byte_cap"], 7168);
                assert_eq!(
                    report["direct_closure_sq8_exceeds_historical_byte_cap_queries"],
                    1000
                );
                assert!(
                    report["queries"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|q| q["direct_closure_sq8_exceeds_historical_byte_cap"] == true)
                );
            }
        }
        // Re-seal every mutation and consistently recompute query/terminal
        // charges: rejection must come from page geometry, not stale hashes.
        for (population, sq8_bytes, sq8_gets, admitted) in [
            (512_usize, 28_usize, 1_u64, false),
            (512, 7168, 1, true),
            (512, 14336, 1, true),
            (512, 7168, 2, false),
            (513, 28, 1, true),
            (513, 7168, 1, true),
            (513, 7196, 1, true),
            (513, 7196, 2, true),
            (513, 56, 1, false),
            (513, 7224, 1, false),
            (513, 14364, 1, true),
            (513, 7196, 3, false),
            (600, 2464, 1, true),
            (600, 9632, 2, true),
            (600, 28, 1, false),
        ] {
            let mut rows = source_fixture();
            rows[1]["rows"] = json!(population);
            rows[1]["query_source_first"] = json!(population);
            let source_bytes = if population > 512 {
                population * 12
            } else {
                6144
            };
            let q = &mut rows[5];
            if population > 512 {
                let mut scored = (0..9).chain([16]).chain(9..16).collect::<Vec<_>>();
                scored.extend(17..population.div_ceil(32));
                q["trace"]["semantic_units"] = json!([0, 8, 16]);
                q["trace"]["nomination_evaluated_units"] = json!(scored);
                if (sq8_bytes / 28) % 256 > 0 {
                    q["trace"]["ranked_candidate_pages"] = json!([2, 0, 1]);
                    q["trace"]["primary_page"] = json!(2);
                } else {
                    q["trace"]["ranked_candidate_pages"] = json!([1, 0, 2]);
                }
            }
            q["charges"]["source"]["verified_bytes"] = json!(source_bytes);
            q["charges"]["sq8"] =
                json!({"submitted_gets":sq8_gets,"verified_bytes":sq8_bytes,"failed_gets":0});
            q["sum"] = json!({"submitted_gets":sq8_gets+2,"verified_bytes":source_bytes+sq8_bytes+100,"failed_gets":0});
            let source_total = 999 * 6144 + source_bytes;
            let sq8_total = 999 * 7168 + sq8_bytes;
            let t = &mut rows.last_mut().unwrap()["summary"];
            t["charges"]["source"]["verified_bytes"] = json!(source_total);
            t["charges"]["sq8"] =
                json!({"submitted_gets":999+sq8_gets,"verified_bytes":sq8_total,"failed_gets":0});
            t["sum"] = json!({"submitted_gets":2999+sq8_gets,"verified_bytes":source_total+sq8_total+100000,"failed_gets":0});
            authenticate(&mut rows);
            let config = source_config_value(dir.path(), &rows, &encode(&rows));
            let (path, pin) = source_write_config(dir.path(), &config);
            let result = reduce_source_utilization(&path, &pin);
            if admitted {
                assert!(
                    result.is_ok(),
                    "legal {population} rows/{sq8_bytes} bytes/{sq8_gets} GETs: {result:?}"
                );
            } else {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "recorded SQ8 whole-page/tail/GET compatibility",
                    "illegal {population} rows/{sq8_bytes} bytes/{sq8_gets} GETs"
                );
            }
        }
    }

    #[test]
    fn source_utilization_config_pins_duplicates_and_unknowns() {
        let dir = tempfile::tempdir().unwrap();
        let rows = source_fixture();
        let config = source_config_value(dir.path(), &rows, &encode(&rows));
        for (name, value) in [
            ("schema", json!("other")),
            ("source_get_cap", json!(127)),
            ("source_byte_cap", json!(67108863)),
            ("source_unit_cap", json!(2543)),
            ("direct_sq8_get_cap", json!(31)),
            ("historical_sq8_query_byte_cap", json!(0)),
            ("historical_sq8_query_byte_cap", json!(-1)),
            ("record_bytes", json!(264)),
            ("producer_source_commit", json!("A".repeat(40))),
            ("producer_source_archive_sha256", json!("bad")),
            ("ignored", json!(true)),
        ] {
            let mut c = config.clone();
            c[name] = value;
            let (path, pin) = source_write_config(dir.path(), &c);
            assert!(reduce_source_utilization(&path, &pin).is_err(), "{name}");
        }
        for field in [
            "record_bytes",
            "source_get_cap",
            "source_byte_cap",
            "source_unit_cap",
            "direct_sq8_get_cap",
            "historical_sq8_query_byte_cap",
        ] {
            let mut c = config.clone();
            c.as_object_mut().unwrap().remove(field);
            let (path, pin) = source_write_config(dir.path(), &c);
            assert!(
                reduce_source_utilization(&path, &pin).is_err(),
                "missing {field}"
            );
        }
        let (path, pin) = source_write_config(dir.path(), &config);
        assert!(reduce_source_utilization(&path, &"0".repeat(64)).is_err());
        assert!(reduce_source_utilization(&path, &pin.to_uppercase()).is_err());
        for field in [
            "source_get_cap",
            "record_bytes",
            "direct_sq8_get_cap",
            "historical_sq8_query_byte_cap",
        ] {
            let raw = serde_json::to_string(&config).unwrap();
            let duplicate = raw.replacen(
                &format!("\"{field}\":"),
                &format!("\"{field}\":0,\"{field}\":"),
                1,
            );
            std::fs::write(&path, &duplicate).unwrap();
            let error = reduce_source_utilization(&path, &sha(duplicate.as_bytes()))
                .unwrap_err()
                .to_string();
            assert!(error.contains("duplicate JSON key"));
        }
        let mut generic_cap = config.clone();
        generic_cap["historical_sq8_query_byte_cap"] = json!(1);
        serde_json::from_value::<SourceUtilizationConfig>(generic_cap)
            .unwrap()
            .validate()
            .unwrap();
        for field in ["runner_source_sha256", "config_sha256"] {
            let mut c = config.clone();
            c["expected_identity"][field] = json!("8".repeat(64));
            let (path, pin) = source_write_config(dir.path(), &c);
            assert!(reduce_source_utilization(&path, &pin).is_err());
        }
        for (dimension, expected) in [
            (1, 9),
            (16, 12),
            (257, 73),
            (300, 88),
            (768, 200),
            (1024, 264),
        ] {
            assert_eq!(source_record_bytes(dimension).unwrap(), expected);
        }
        assert!(source_record_bytes(0).is_err());
        assert!(source_record_bytes(1025).is_err());
    }

    #[test]
    fn source_utilization_payload_sha_eof_seals_and_descriptor_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let valid = source_fixture();
        for case in 0..9 {
            let mut rows = valid.clone();
            match case {
                0 => rows[1005]["prefix_sha256"] = json!("0".repeat(64)),
                1 => rows[1005]["requires_successful_sync"] = json!(false),
                2 => rows.last_mut().unwrap()["summary"]["sealed_sha256"] = json!("0".repeat(64)),
                3 => {
                    rows.pop();
                }
                4 => rows[1004]["ordinal"] = json!(998),
                _ => (),
            }
            let mut bytes = encode(&rows);
            match case {
                5 => {
                    bytes.pop();
                }
                6 => bytes.extend_from_slice(b"{}\n"),
                7 => {
                    bytes = String::from_utf8(bytes)
                        .unwrap()
                        .replacen(
                            "\"semantic_units\":[0,8]",
                            "\"semantic_units\":[0,8],\"semantic_units\":[0,8]",
                            1,
                        )
                        .into_bytes()
                }
                _ => (),
            }
            let mut c = source_config_value(dir.path(), &rows, &bytes);
            if case == 8 {
                c["input"]["sha256"] = json!("0".repeat(64));
            }
            let (path, pin) = source_write_config(dir.path(), &c);
            assert!(
                reduce_source_utilization(&path, &pin).is_err(),
                "case {case}"
            );
        }
        // Mutation occurs after all queries are read but before seal/EOF: the
        // same shared reader's original descriptor/stamps must refuse it.
        for case in 0..3 {
            let bytes = encode(&valid);
            let value = source_config_value(dir.path(), &valid, &bytes);
            let c: SourceUtilizationConfig = serde_json::from_value(value).unwrap();
            let expected = CompletedConfig {
                schema: "borsuk-completed-native-reduction-config-v1".into(),
                input: CompletedInput {
                    path: c.input.path.clone(),
                    bytes: c.input.bytes,
                    sha256: c.input.sha256.clone(),
                },
                expected_identity: c.expected_identity.clone(),
                expected_bound_inputs: c.expected_bound_inputs.clone(),
            };
            let result = read_run_observed(
                &c.input.path,
                &c.input.sha256,
                Some(&expected),
                |_, q, _| {
                    if q.ordinal == COUNT - 1 {
                        match case {
                            0 => {
                                std::fs::OpenOptions::new()
                                    .append(true)
                                    .open(&c.input.path)?
                                    .write_all(b"{}\n")?;
                            }
                            1 => {
                                std::fs::OpenOptions::new()
                                    .write(true)
                                    .open(&c.input.path)?
                                    .set_len(1)?;
                            }
                            _ => {
                                let replacement = dir.path().join("replacement");
                                std::fs::write(&replacement, &bytes)?;
                                std::fs::rename(&replacement, &c.input.path)?;
                            }
                        }
                    }
                    Ok(())
                },
            );
            assert!(result.is_err(), "descriptor mutation {case}");
        }
    }

    fn charge(n: u64) -> Value {
        json!({"submitted_gets":n,"verified_bytes":n*100,"failed_gets":0})
    }

    fn charges(n: u64) -> Value {
        json!({"router":charge(n),"source":charge(n),"sq8":charge(n)})
    }

    // Native schema and literal arithmetic, independent of reducer serialization.
    fn fixture(candidate: bool, unit_ns: u64) -> Vec<Value> {
        let pin = "a".repeat(64);
        let binary = if candidate {
            "3911839ba9ef68604e2c487d802a3b3e125bdc1121aee9af268db3ba8a9ca8cf"
        } else {
            "ce43842caeea9dbb722f3497b237265e71c7d829cbaf0243a81a2a352fcf1221"
        };
        let requests = "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e";
        let truth = "479064239b698a2b8094c7838b1bb01af6eff5ea6eee4736692971849fdcfb2c";
        let mut rows = vec![
            json!({"phase":"identity","schema":"borsuk-cohere-native-baseline-result-v1",
                "config_sha256":pin,"binary_sha256":binary,
                "runner_source_sha256":"15f06f8b28ec23c9cd29c850a50acfea0ebb889fa053f2da09ab8f69a8cf76b5",
                "generation_source_sha256":"70a1e6956e4d18eacccc1205c644cbaa5dbb1eb763a2958da666be60d17bc4e3",
                "router_source_sha256":"b9abd271db66d45665304e72dc1a61b176fdbc47cd6f1a5618868a3c7443d0c2",
                "codec_source_sha256":"0f51015f31c08022b988ac00c40be534df8115e0aa54c482852bc51f0bb9ae60",
                "source_plane_source_sha256":"dbcc4cdbc4bb5c244354b375afd657f8b5cb1892df3f0ff0b8f41ef580de42e0",
                "scope":"LOCAL_AUTHENTICATED_FILE_QUALITY_CORRECTNESS","physical_s3_measured":false,
                "io_measurement":"logical_object_store_GETs_and_authenticated_payload_bytes",
                "external_gate_required":true,"truth_opened":false}),
            json!({"phase":"bound_inputs","dataset":"CohereLabs/wikipedia-2023-11-embed-multilingual-v3",
                "revision":"ade45fb52bd549f5e8c065636fe4160a43c2af36","metric":"cosine",
                "tie_rule":"corpus_ordinal_ascending","rows":100000,"dimensions":1024,
                "count":1000,"k":10,"corpus_source_first":0,"query_source_first":100000,
                "profile":"native100k","store_root":"/synthetic/store","generation_prefix":"retained",
                "generation_root_sha256":pin,"requests_bytes":4096000,"requests_sha256":requests,
                "truth_bytes":80000,"truth_sha256":truth,
                "native_source_sha256":"20936913f31e48ea67d462dfffc7a831569ff7467baebc63f2d622c8ff417dce",
                "native_sq8_sha256":"07a14360b06add9a35f82b97f4e031690ff878ff497ee047902818823992337b",
                "native_order_sha256":"6b6a67098330c76bfb89340065d9326fb3f23a1f6016fbae5b9fa7f0e29759c2",
                "truth_opened":false}),
            json!({"phase":"startup","metadata":{"metadata":[],"staging_wall_ns":1,
                "decode_wall_ns":1,"source_head_requests":1,"source_head_wall_ns":1,
                "router_head_requests":1,"router_head_wall_ns":1},
                "library_cap_bytes":536870912,"caller_pinned_bytes":1,"codec_scratch_bytes":532480,
                "trace_scratch_bytes":1,"query_scratch_bytes":1,"truth_opened":false}),
        ];
        // -2,-1,-0,+0,+0,1,1,1,1,2. Unsigned-bit sorting reverses negatives;
        // treating signed zeros as equal would incorrectly put ID1 before ID7.
        let returned: Vec<_> = [
            (8, 0xc000_0000_u32),
            (9, 0xbf80_0000),
            (7, 0x8000_0000),
            (1, 0),
            (2, 0),
            (3, 0x3f80_0000),
            (4, 0x3f80_0000),
            (5, 0x3f80_0000),
            (6, 0x3f80_0000),
            (10, 0x4000_0000),
        ]
        .into_iter()
        .map(|(id, score_bits)| json!({"id":id,"score_bits":score_bits}))
        .collect();
        for ordinal in 0..1000 {
            rows.push(
                json!({"phase":"query","ordinal":ordinal,"truth_opened":false,
                "returned":returned,"returned_count":10,"underfill":false,
                "charges":charges(1),"sum":charge(3),"query_wall_ns":(ordinal+1)*unit_ns,
                "query_process_cpu_ns":1,"stages":{
                    "discovery":{"start_ns":1,"end_ns":2},"source":{"start_ns":2,"end_ns":3},
                    "planning":{"start_ns":3,"end_ns":4},"sq8":{"start_ns":4,"end_ns":5},
                    "leaf_peak_inflight":1},"trace":{"ranked_candidate_pages":[0,1],
                    "nomination_evaluated_units":[0,1,2],"primary_page":0,"discoveries":[],
                    "semantic_leaves":[0],"semantic_units":[0,1],"semantic_seed_additions":[2]}}),
            );
        }
        rows.push(
            json!({"phase":"all_queries_sealed","count":1000,"truth_opened":false,
            "prefix_bytes":0,"prefix_sha256":pin,"requests_sha256":requests,"generation_root_sha256":pin,
            "requires_successful_sync":true,"requires_successful_directory_sync":true}),
        );
        for ordinal in 0..1000 {
            rows.push(
                json!({"phase":"recall","ordinal":ordinal,"hits10":9,"recall10":0.9,
                "returned_count":10,"underfill":false}),
            );
        }
        rows.push(json!({"phase":"terminal","summary":{"status":"MEASURED","complete":true,
            "queries":1000,"k":10,"total_hits10":9000,"recall_numerator":9000,
            "recall_denominator":10000,"mean_recall10":0.9,"underfilled_queries":0,
            "all_queries_sealed":true,"prefix_bytes":0,"prefix_sha256":pin,
            "sealed_bytes":0,"sealed_sha256":pin,"requests_sha256":requests,"truth_sha256":truth,
            "generation_root_sha256":pin,"charges":charges(1000),"sum":charge(3000),
            "query_wall_ns":500500*unit_ns,"query_process_cpu_ns":1000,
            "physical_s3_measured":false,"external_gate_required":true,
            "process_wall_ns":500500*unit_ns+100,"process_cpu_ns":1100,"observed_process_peak_bytes":12345}}));
        rows
    }

    pub(super) fn encode(rows: &[Value]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for row in rows {
            if row["phase"] == "query" && row.get("trace").is_some() {
                // Match the native Record writer's final trace field, not Value's key sorting.
                let mut prefix = row.clone();
                let trace = prefix.as_object_mut().unwrap().remove("trace").unwrap();
                let transport = prefix.as_object_mut().unwrap().remove("transport");
                serde_json::to_writer(&mut bytes, &prefix).unwrap();
                assert_eq!(bytes.pop(), Some(b'}'));
                bytes.extend_from_slice(b",\"trace\":");
                serde_json::to_writer(&mut bytes, &trace).unwrap();
                if let Some(transport) = transport {
                    bytes.extend_from_slice(b",\"transport\":");
                    serde_json::to_writer(&mut bytes, &transport).unwrap();
                }
                bytes.push(b'}');
            } else {
                serde_json::to_writer(&mut bytes, row).unwrap();
            }
            bytes.push(b'\n');
        }
        bytes
    }

    pub(super) fn authenticate(rows: &mut [Value]) {
        let seal = rows
            .iter()
            .position(|v| v["phase"] == "all_queries_sealed")
            .unwrap();
        let prefix = encode(&rows[..seal]);
        rows[seal]["prefix_bytes"] = json!(prefix.len());
        rows[seal]["prefix_sha256"] = json!(sha(&prefix));
        let sealed = encode(&rows[..seal + 1]);
        let summary = &mut rows.last_mut().unwrap()["summary"];
        summary["prefix_bytes"] = json!(prefix.len());
        summary["prefix_sha256"] = json!(sha(&prefix));
        summary["sealed_bytes"] = json!(sealed.len());
        summary["sealed_sha256"] = json!(sha(&sealed));
    }

    fn write_fixture(dir: &Path, name: &str, mut rows: Vec<Value>) -> (PathBuf, String) {
        authenticate(&mut rows);
        let bytes = encode(&rows);
        let path = dir.join(name);
        std::fs::write(&path, &bytes).unwrap();
        (path, sha(&bytes))
    }

    fn four(dir: &Path) -> [(PathBuf, String); 4] {
        // Distinct process observations prevent byte-identical repeated evidence.
        std::array::from_fn(|i| {
            let candidate = i == 1 || i == 2;
            let mut rows = fixture(candidate, if candidate { 900000 } else { 1000000 });
            rows.last_mut().unwrap()["summary"]["process_cpu_ns"] = json!(1100 + i);
            write_fixture(dir, &format!("run-{i}"), rows)
        })
    }

    fn scale_config(dir: &Path, path: &Path, rows: &[Value]) -> (PathBuf, String) {
        let bytes = std::fs::read(path).unwrap();
        let config = json!({"schema":SCALE_CONFIG_SCHEMA,"input":{"path":path,"bytes":bytes.len(),"sha256":sha(&bytes)},
            "expected_identity":rows[0],"expected_bound_inputs":rows[1]});
        let body = serde_json::to_vec(&config).unwrap();
        let path = dir.join("scale-reduction.json");
        std::fs::write(&path, &body).unwrap();
        (path, sha(&body))
    }

    pub(super) fn assert_current_native(dir: &Path, result: &Path) -> Vec<Value> {
        assert_current_native_config(&dir.join("config.json"), result)
    }

    fn assert_current_native_config(config_path: &Path, result: &Path) -> Vec<Value> {
        static BINARY_SHA: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        let binary_sha = BINARY_SHA.get_or_init(|| {
            let mut file = File::open(std::env::current_exe().unwrap()).unwrap();
            let mut digest = Sha256::new();
            let mut block = [0; 8192];
            loop {
                let n = file.read(&mut block).unwrap();
                if n == 0 {
                    break;
                }
                digest.update(&block[..n]);
            }
            format!("{:x}", digest.finalize())
        });
        let config_bytes = std::fs::read(config_path).unwrap();
        let config: Value = serde_json::from_slice(&config_bytes).unwrap();
        let result_bytes = std::fs::read(result).unwrap();
        let rows = std::str::from_utf8(&result_bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let mut identity = json!({"phase":"identity","schema":current_v10::FRAMED_RESULT_SCHEMA,
            "config_sha256":sha(&config_bytes),"binary_sha256":binary_sha,
            "scope":"AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS","physical_s3_measured":false,
            "io_measurement":"logical_GET_charges_separate_from_cumulative_process_native_transport",
            "s3_credential_source":"imds_instance_role_only","native_transport_includes":"S3_and_IMDS_credential_requests_including_PUT",
            "wire_bytes":null,"unread_bytes":null,"billed_bytes":null,"billed_requests":null,
            "external_gate_required":true,"truth_opened":false});
        for (key, bytes) in [
            (
                "runner_source_sha256",
                include_bytes!("../src/bin/check_cohere_native_baseline.rs").as_slice(),
            ),
            (
                "generation_source_sha256",
                include_bytes!("../src/two_bit_generation.rs").as_slice(),
            ),
            (
                "router_source_sha256",
                include_bytes!("../src/semantic_unit_router.rs").as_slice(),
            ),
            (
                "codec_source_sha256",
                include_bytes!("../src/rotated_two_bit.rs").as_slice(),
            ),
            (
                "source_plane_source_sha256",
                include_bytes!("../src/two_bit_source.rs").as_slice(),
            ),
            (
                "sq8_range_source_sha256",
                include_bytes!("../src/sq8_s3_range.rs").as_slice(),
            ),
            (
                "returned_source_sha256",
                include_bytes!("../src/returned_sq8.rs").as_slice(),
            ),
            (
                "source_capsule_source_sha256",
                include_bytes!("../src/source_capsule.rs").as_slice(),
            ),
        ] {
            identity[key] = json!(sha(bytes));
        }
        identity["source_frame_source_sha256"] =
            json!(sha(include_bytes!("../src/source_frame.rs")));
        identity["source_authentication"] = config
            .get("source_authentication")
            .cloned()
            .unwrap_or(json!("inline"));
        identity["config_schema"] = json!(current_v10::FRAMED_CONFIG_SCHEMA);
        identity["range_scheduler_owner"] = json!("library");
        identity["source_capsule_verification_policy"] =
            json!(current_v10::PAGE_RANGE_VERIFICATION_POLICY);
        let mut bound = json!({"phase":"bound_inputs","truth_opened":false,"source_cache":"off","credential_source":null,
            "range_scheduler_owner":"library"});
        for key in "dataset revision metric tie_rule rows dimensions count k corpus_source_first query_source_first profile backend generation_prefix generation_root_sha256 corpus_intervals reserved_query_interval producer_authority max_memory_bytes".split_whitespace() {
            bound[key] = config[key].clone();
        }
        for key in [
            "sq8_get_budget",
            "sq8_byte_budget",
            "sq8_range_split",
            "source_layout",
            "source_byte_budget",
            "max_memory_bytes",
            "serving",
            "execution",
        ] {
            identity[key] = config[key].clone();
            bound[key] = config[key].clone();
        }
        for (key, default) in [("fetch_parallelism", 16), ("source_get_budget", 128)] {
            let value = config.get(key).cloned().unwrap_or(json!(default));
            identity[key] = value.clone();
            bound[key] = value;
        }
        for key in ["requests", "truth"] {
            for field in ["bytes", "sha256"] {
                bound[format!("{key}_{field}")] = config[key][field].clone();
            }
        }
        for (key, field) in [
            ("native_source_sha256", "source_sha256"),
            ("native_sq8_sha256", "sq8_sha256"),
            ("native_order_sha256", "source_order_sha256"),
        ] {
            bound[key] = config["native_source"][field].clone();
        }
        for key in ["cohort_receipt", "derivation_receipt"] {
            bound[format!("{key}_sha256")] = config[key]["sha256"].clone();
        }
        let cohort: Value = serde_json::from_slice(
            &std::fs::read(config["cohort_receipt"]["path"].as_str().unwrap()).unwrap(),
        )
        .unwrap();
        bound["source_authentication"] = config
            .get("source_authentication")
            .cloned()
            .unwrap_or(json!("inline"));
        bound["config_schema"] = json!(current_v10::FRAMED_CONFIG_SCHEMA);
        bound["source_capsule_verification_policy"] =
            json!(current_v10::PAGE_RANGE_VERIFICATION_POLICY);
        bound["reserved_queries_sha256"] = cohort["reserved_queries_sha256"].clone();
        bound["selected_count"] = json!(
            v9_panel::ordinals(&bound, config["count"].as_u64().unwrap() as usize)
                .unwrap()
                .len()
        );
        if config.get("max_active_queries").is_some() {
            identity["schema"] = json!(current_v10::FRAMED_CONCURRENT_RESULT_SCHEMA);
            for header in [&mut identity, &mut bound] {
                header["config_schema"] = json!(current_v10::FRAMED_CONCURRENT_CONFIG_SCHEMA);
                header["max_active_queries"] = config["max_active_queries"].clone();
                header["query_runtime"] = config
                    .get("query_runtime")
                    .cloned()
                    .unwrap_or(json!("current_thread"));
                header["query_dispatch"] = json!("spawned_ordered_batches");
                header["query_wall_semantics"] =
                    json!("dispatch_to_completion_including_initial_runtime_queue");
            }
        }
        let completed = CompletedConfig {
            schema: SCALE_CONFIG_SCHEMA.into(),
            input: CompletedInput {
                path: result.into(),
                bytes: result_bytes.len() as u64,
                sha256: sha(&result_bytes),
            },
            expected_identity: identity,
            expected_bound_inputs: bound,
        };
        assert_eq!(rows[0], completed.expected_identity);
        assert_eq!(rows[1], completed.expected_bound_inputs);
        read_run_with(result, &completed.input.sha256, Some(&completed)).unwrap();
        rows
    }

    fn author_profile_v4_diagnostic(diag: &mut Value) {
        let source = &diag["source_range_trace"];
        let mut summary = Value::Object(
            "outcome planned_ranges planned_bytes max_parallel fetch_start_ns all_ranges_complete_ns release_end_ns retained_payload_bytes cpu phase_process_cpu_ns unattributed_process_cpu_ns phase_clock_reads"
                .split_whitespace().map(|key| (key.to_owned(), source[key].clone())).collect(),
        );
        summary["last_completing_range"] = source["ranges"]
            .as_array()
            .unwrap()
            .iter()
            .max_by_key(|row| row[14].as_u64())
            .cloned()
            .unwrap_or(Value::Null);
        diag["schema"] = json!("borsuk-sq8-range-attribution-diagnostic-v4");
        diag["source_range_trace"] = summary;
    }

    fn author_historical_panel_memory(rows: &mut [Value]) {
        let Some(admission) = rows
            .iter()
            .find(|row| row["phase"] == "diagnostic_admission")
        else {
            return;
        };
        let old_pinned = if admission["trace"] == true {
            534097
        } else {
            131072
        };
        let delta = admission["diagnostic_pinned_bytes"].as_u64().unwrap() - old_pinned;
        for row in rows {
            if row["phase"] == "diagnostic_admission" {
                row["panel_line_cap_bytes"] = json!(65536);
                row["diagnostic_pinned_bytes"] = json!(old_pinned);
                let total = row["diagnostic_bytes_per_active_query"].as_u64().unwrap();
                row["diagnostic_bytes_per_active_query"] = json!(total - delta);
            }
            if matches!(
                row["phase"].as_str(),
                Some("diagnostic_admission" | "startup")
            ) {
                let caller = row["caller_pinned_bytes"].as_u64().unwrap();
                row["caller_pinned_bytes"] = json!(caller - delta);
            }
        }
    }

    // Synthetic historical records have their own explicit roster and accounting.
    // Never use this conversion on the current producer interoperability path.
    fn author_historical_diagnostics(rows: &mut [Value]) {
        rows[0]
            .as_object_mut()
            .unwrap()
            .remove("source_frame_source_sha256");
        // These are explicit old-wire fixtures, never a projection of an offload run.
        for row in &mut *rows {
            if let Some(policy) = row.as_object_mut().unwrap().remove("source_authentication") {
                assert_eq!(policy, json!("inline"));
            }
            if let Some(bytes) = row
                .as_object_mut()
                .unwrap()
                .remove("source_authentication_extra_library_bytes")
            {
                assert_eq!(bytes, json!(0));
            }
        }
        author_historical_panel_memory(rows);
        use borsuk::sq8_s3_range::Sq8RangeTrace;
        let width = rows[1]["fetch_parallelism"].as_u64().unwrap() as usize;
        for row in rows {
            if row["phase"] == "diagnostic_admission" {
                let traced = row["trace"] == true;
                let ranges = row["trace_ranges"].as_u64().unwrap() as usize;
                let retained = current_v10::historical_trace_retained(ranges);
                let peak = if traced {
                    current_v10::historical_trace_peak(ranges, width)
                } else {
                    0
                };
                let library = row["range_scheduler_owner"] == "library";
                let scheduler = if library {
                    "range_scheduler_owner range_state_charged_by_library_bytes trace_additional_bytes_charged_by_library"
                } else {
                    "range_state_pinned_by_runner_bytes trace_peak_charged_by_library_at_traced_admission"
                };
                let names = format!(
                    "phase execution selected_count population_count trace diagnostic_bytes_per_active_query diagnostic_cap_bytes max_active_queries charged_in_caller_pinned_bytes caller_pinned_bytes panel_line_cap_bytes host_read_cap_bytes trace_ranges population_percentiles_valid full_cohort_qualification semantics truth_opened trace_retained_bytes trace_peak_bytes diagnostic_pinned_bytes range_state_both_paths_bytes {scheduler}"
                );
                let mut old = Value::Object(
                    names
                        .split_whitespace()
                        .map(|key| {
                            (
                                key.to_owned(),
                                row.get(key).expect("historical observation").clone(),
                            )
                        })
                        .collect(),
                );
                old["semantics"] = current_v10::historical_semantics();
                old["trace_retained_bytes"] = if traced { json!(retained) } else { Value::Null };
                old["trace_peak_bytes"] = if traced { json!(peak) } else { Value::Null };
                old["diagnostic_bytes_per_active_query"] =
                    json!(old["diagnostic_pinned_bytes"].as_u64().unwrap() + peak as u64);
                if library {
                    old["trace_additional_bytes_charged_by_library"] = json!(if traced {
                        peak - Sq8RangeTrace::pending_futures_bytes(ranges, width)
                    } else {
                        0
                    });
                }
                *row = old;
            } else if row["phase"] == "query" && row.get("diagnostic").is_some() {
                let diag = &row["diagnostic"];
                let mut old = Value::Object("query_monotonic_start_ns query_monotonic_end_ns process_cpu_start_ns process_cpu_end_ns host"
                    .split_whitespace().map(|key| (key.to_owned(), diag[key].clone())).collect());
                old["schema"] = json!("borsuk-sq8-range-attribution-diagnostic-v3");
                old["sq8_range_trace"] = Value::Object(
                    current_v10::LEGACY_TRACE_FIELDS
                        .split_whitespace()
                        .map(|key| (key.to_owned(), diag["sq8_range_trace"][key].clone()))
                        .collect(),
                );
                row["diagnostic"] = old;
            }
        }
    }

    #[test]
    fn runtime_task_reader_mutations() {
        for multi in [false, true] {
            let (dir, rows) = scale_native_runner::runtime_task_reducer_fixture(2, multi);
            // Independent headers from config, source bytes and executable, before mutations.
            assert_current_native_config(
                &dir.path().join("config.json"),
                &dir.path().join("concurrent.jsonl"),
            );
            let startup = rows.iter().position(|r| r["phase"] == "startup").unwrap();
            for mutation in 0..9 {
                let mut bad = rows.clone();
                match mutation {
                    0 => bad[0]["query_runtime"] = json!("unknown"),
                    1 => {
                        bad[1]["query_runtime"] = json!(if multi {
                            "current_thread"
                        } else {
                            "multi_thread2"
                        })
                    }
                    2 => bad[startup]["concurrent_resources"]["runtime_worker_threads"] = json!(3),
                    3 => {
                        bad[startup]["concurrent_resources"]["runtime_stack_bytes_per_thread"] =
                            json!(1)
                    }
                    4 => bad[startup]["concurrent_resources"]["runtime_reserve_bytes"] = json!(1),
                    5 => {
                        bad[startup]["concurrent_resources"]["task_state_bytes_per_query"] =
                            json!(1)
                    }
                    6 => {
                        for row in &mut bad[0..2] {
                            row["query_dispatch"] = json!("join_all");
                        }
                    }
                    7 => {
                        for row in &mut bad[0..2] {
                            row["query_runtime"] = json!(if multi {
                                "current_thread"
                            } else {
                                "multi_thread2"
                            });
                        }
                    }
                    _ => bad[startup]["caller_pinned_bytes"] = json!(1024 * 1024),
                }
                let (path, _) = write_fixture(dir.path(), "runtime-mutation.jsonl", bad.clone());
                let (config, pin) = scale_config(dir.path(), &path, &bad);
                assert!(
                    reduce_scale(&config, &pin).is_err(),
                    "mode {multi} mutation {mutation}"
                );
            }
            // Coherent alternate runtime is valid internally but is not the frozen experiment.
            let mut changed = rows.clone();
            let mode = if multi {
                "current_thread"
            } else {
                "multi_thread2"
            };
            for header in &mut changed[..2] {
                header["query_runtime"] = json!(mode);
            }
            let resources = &mut changed[startup]["concurrent_resources"];
            resources["query_runtime"] = json!(mode);
            resources["runtime_worker_threads"] = json!(if multi { 0 } else { 2 });
            resources["runtime_num_workers"] = json!(if multi { 1 } else { 2 });
            let (path, _) = write_fixture(dir.path(), "coherent-runtime.jsonl", changed.clone());
            let (internal, pin) = scale_config(dir.path(), &path, &changed);
            assert!(
                reduce_scale(&internal, &pin).is_ok(),
                "mutation must be internally coherent"
            );
            let (frozen, pin) = scale_config(dir.path(), &path, &rows);
            assert!(
                reduce_scale(&frozen, &pin).is_err(),
                "frozen original expectation refuses relabeling"
            );
        }
    }

    #[test]
    fn concurrent_probe_reader_aggregate_not_sum_and_resealed_mutations() {
        for active in [1, 2] {
            let (dir, rows) = scale_native_runner::concurrent_reducer_fixture(active);
            let check = |records: &[Value]| {
                let (path, _) =
                    write_fixture(dir.path(), "concurrent-reader.jsonl", records.to_vec());
                let (config, pin) = scale_config(dir.path(), &path, records);
                reduce_scale(&config, &pin)
            };
            let good = check(&rows).unwrap();
            for (config_schema, result_schema) in [
                (
                    current_v10::FRAMED_CONCURRENT_CONFIG_SCHEMA,
                    current_v10::CONCURRENT_RESULT_SCHEMA,
                ),
                (
                    current_v10::CONCURRENT_CONFIG_SCHEMA,
                    current_v10::FRAMED_CONCURRENT_RESULT_SCHEMA,
                ),
            ] {
                let mut bad = rows.clone();
                bad[0]["schema"] = json!(result_schema);
                bad[0]["config_schema"] = json!(config_schema);
                bad[1]["config_schema"] = json!(config_schema);
                let error = check(&bad).unwrap_err().to_string();
                assert!(
                    error.contains("concurrent result/config schema pair"),
                    "{error}"
                );
            }
            assert!(
                current_v10::validate_concurrent_geometry(&rows[1]).is_err(),
                "production geometry rejects tiny fixtures"
            );
            assert!(good["statistics"].get("sequential_qps").is_none());
            let cohort = rows
                .iter()
                .position(|r| r["phase"] == "query_cohort")
                .unwrap();
            let query = rows.iter().position(|r| r["phase"] == "query").unwrap();
            let startup = rows.iter().position(|r| r["phase"] == "startup").unwrap();
            for mutation in 0..14 {
                let mut bad = rows.clone();
                match mutation {
                    0 => bad[query]["query_process_cpu_ns"] = json!(0),
                    1 => {
                        bad[query]["transport"] =
                            json!({"stage":"query","ordinal":0,"before":null,"after":null})
                    }
                    2 => bad[cohort]["attempted_count"] = json!(1),
                    3 => bad[cohort]["sum_query_wall_ns"] = json!(1),
                    4 => bad[cohort]["qps"] = json!(1.0),
                    5 => bad[cohort]["max_active_queries"] = json!(3),
                    6 => bad.last_mut().unwrap()["summary"]["query_process_cpu_ns"] = json!(1),
                    7 => {
                        bad.last_mut().unwrap()["summary"]["query_cohort"]["elapsed_ns"] = json!(1)
                    }
                    8 => bad[startup]["concurrent_resources"]["compute_slots_global"] = json!(4),
                    9 => bad[1]["max_active_queries"] = json!(3),
                    10 => {
                        let other = 3 - active;
                        bad[startup]["concurrent_resources"]["query_slots"] = json!(other);
                        bad[startup]["concurrent_resources"]["downloads_global_bound"] =
                            json!(other * 64);
                        let old = &rows[startup]["concurrent_resources"];
                        for key in ["query_bytes_global_bound", "scheduler_bytes_global"] {
                            bad[startup]["concurrent_resources"][key] =
                                json!(old[key].as_u64().unwrap() / active as u64 * other as u64);
                        }
                    }
                    11 => bad[startup]["concurrent_resources"]["source_bytes_per_query"] = json!(1),
                    12 => bad[startup]["concurrent_resources"]["scheduler_bytes_global"] = json!(1),
                    _ => bad[cohort]["process_cpu_start_ns"] = json!(0),
                }
                assert!(check(&bad).is_err(), "mutation {mutation}");
            }
            if active == 2 {
                let mut bad = rows.clone();
                for (row, wall) in bad
                    .iter_mut()
                    .filter(|r| r["phase"] == "query")
                    .zip([90_000_000_u64, 10_000_000])
                {
                    row["query_wall_ns"] = json!(wall);
                    for (index, stage) in ["discovery", "source", "planning", "sq8"]
                        .into_iter()
                        .enumerate()
                    {
                        row["stages"][stage]["start_ns"] = json!(1 + index * 2);
                        row["stages"][stage]["end_ns"] = json!(2 + index * 2);
                    }
                }
                bad[cohort]["elapsed_ns"] = json!(50_000_000);
                bad[cohort]["sum_query_wall_ns"] = json!(100_000_000);
                bad[cohort]["qps"] = json!(40.0);
                let coherent = bad[cohort].clone();
                let summary = &mut bad.last_mut().unwrap()["summary"];
                summary["query_cohort"] = coherent;
                summary["query_wall_ns"] = json!(100_000_000);
                summary["process_wall_ns"] = json!(100_000_001);
                let error = check(&bad).unwrap_err().to_string();
                assert!(
                    error.contains("cohort ordered-batch elapsed floor"),
                    "{error}"
                );
            }
        }
    }

    #[test]
    fn cpu_profile_current_producer_reader_and_resealed_refusals() {
        for traced in [false, true] {
            let (dir, paths) = scale_native_runner::source_layout_reducer_profile_fixture(
                32, 2, true, false, traced,
            )
            .unwrap();
            for (path, config_name) in paths.iter().zip(["config.json", "capsule-config.json"]) {
                // Read the untouched producer file with independently authored header pins.
                let rows = assert_current_native_config(&dir.path().join(config_name), path);
                assert_eq!(rows[0]["schema"], current_v10::FRAMED_RESULT_SCHEMA);
                let admission = rows
                    .iter()
                    .position(|r| r["phase"] == "diagnostic_admission")
                    .unwrap();
                let query = rows.iter().position(|r| r["phase"] == "query").unwrap();
                assert_eq!(rows[admission]["panel_line_cap_bytes"], 131072);
                let check = |name: &str, records: &[Value]| {
                    let (path, _) = write_fixture(
                        dir.path(),
                        &format!("reader-{name}.jsonl"),
                        records.to_vec(),
                    );
                    let (config, pin) = scale_config(dir.path(), &path, records);
                    reduce_scale(&config, &pin)
                };
                for mutation in [
                    "missing",
                    "unknown",
                    "capacity",
                    "charge",
                    "retained",
                    "peak",
                    "semantics",
                    "line_cap",
                    "scratch",
                    "cap",
                ] {
                    let mut bad = rows.clone();
                    match mutation {
                        "missing" => {
                            bad[admission]
                                .as_object_mut()
                                .unwrap()
                                .remove("source_trace_ranges");
                        }
                        "unknown" => bad[admission]["unknown_profile_field"] = json!(0),
                        "capacity" => bad[admission]["source_trace_ranges"] = json!(129),
                        "charge" => {
                            let charge =
                                bad[admission]["trace_additional_bytes_charged_by_library"]
                                    .as_u64()
                                    .unwrap();
                            bad[admission]["trace_additional_bytes_charged_by_library"] =
                                json!(charge + 1);
                        }
                        "retained" => bad[admission]["trace_retained_bytes"] = json!(0),
                        "peak" => bad[admission]["trace_peak_bytes"] = json!(0),
                        "line_cap" => {
                            bad[admission]["panel_line_cap_bytes"] = json!(98304);
                            for key in [
                                "diagnostic_pinned_bytes",
                                "diagnostic_bytes_per_active_query",
                            ] {
                                let bytes = bad[admission][key].as_u64().unwrap();
                                bad[admission][key] = json!(bytes - 65536);
                            }
                            for row in &mut bad {
                                if matches!(
                                    row["phase"].as_str(),
                                    Some("diagnostic_admission" | "startup")
                                ) {
                                    let bytes = row["caller_pinned_bytes"].as_u64().unwrap();
                                    row["caller_pinned_bytes"] = json!(bytes - 65536);
                                }
                            }
                        }
                        "scratch" => {
                            let pinned =
                                bad[admission]["diagnostic_pinned_bytes"].as_u64().unwrap();
                            bad[admission]["diagnostic_pinned_bytes"] = json!(pinned - 1);
                        }
                        "cap" => bad[admission]["diagnostic_cap_bytes"] = json!(2097152),
                        "semantics" => {
                            bad[admission]["semantics"] = current_v10::historical_semantics()
                        }
                        _ => unreachable!(),
                    }
                    assert!(
                        check(mutation, &bad).is_err(),
                        "{mutation}, traced={traced}"
                    );
                }
                if traced {
                    assert_eq!(
                        rows[query]["diagnostic"]["schema"],
                        "borsuk-sq8-range-attribution-diagnostic-v5"
                    );
                    for mutation in [
                        "schema",
                        "source_range_trace",
                        "cpu_profile",
                        "cpu_unknown",
                        "sq8_unknown",
                        "sq8_missing",
                        "poll_cpu",
                        "auth_reads",
                        "single_reads",
                        "source_unknown",
                        "source_missing",
                        "source_dropped",
                        "source_charge",
                        "source_order",
                        "source_stamp",
                        "model_schema",
                    ] {
                        let mut bad = rows.clone();
                        let diag = &mut bad[query]["diagnostic"];
                        match mutation {
                            "schema" => {
                                diag["schema"] = json!("borsuk-sq8-range-attribution-diagnostic-v3")
                            }
                            "source_range_trace" | "cpu_profile" => {
                                diag.as_object_mut().unwrap().remove(mutation);
                            }
                            "cpu_unknown" => diag["cpu_profile"]["unknown"] = json!(0),
                            "sq8_unknown" => diag["sq8_range_trace"]["unknown"] = json!(0),
                            "sq8_missing" => {
                                diag["sq8_range_trace"]
                                    .as_object_mut()
                                    .unwrap()
                                    .remove("cpu");
                            }
                            "poll_cpu" => {
                                diag["cpu_profile"]["acquisition_poll_cpu"] = json!("measured")
                            }
                            "auth_reads" => {
                                diag["source_range_trace"]["cpu"]["authentication"]["clock_reads"] =
                                    json!(3)
                            }
                            "source_unknown" => diag["source_range_trace"]["unknown"] = json!(0),
                            "source_missing" => {
                                diag["source_range_trace"]
                                    .as_object_mut()
                                    .unwrap()
                                    .remove("ranges");
                            }
                            "source_dropped" => {
                                diag["source_range_trace"]["dropped_ranges"] = json!(1)
                            }
                            "source_charge" => {
                                diag["source_range_trace"]["ranges"][0][16] = json!(0)
                            }
                            "source_order" => diag["source_range_trace"]["ranges"][0][0] = json!(1),
                            "source_stamp" => {
                                diag["source_range_trace"]["ranges"][0][14] = json!(u64::MAX)
                            }
                            "model_schema" => {
                                author_profile_v4_diagnostic(diag);
                            }
                            "single_reads" => {
                                diag["cpu_profile"]["discovery"]["clock_reads"] = json!(8)
                            }
                            _ => unreachable!(),
                        }
                        assert!(check(mutation, &bad).is_err(), "{mutation}");
                    }
                }
                let mut v4 = rows.clone();
                author_historical_panel_memory(&mut v4);
                for row in &mut v4 {
                    if let Some(diag) = row.get_mut("diagnostic") {
                        author_profile_v4_diagnostic(diag);
                    }
                }
                check("historical-profile-v4", &v4).unwrap();
                if traced {
                    let mut bad = v4.clone();
                    bad[query]["diagnostic"] = rows[query]["diagnostic"].clone();
                    assert!(check("v5-with-v4-memory", &bad).is_err());
                    let mut bad = rows.clone();
                    bad[query]["diagnostic"] = v4[query]["diagnostic"].clone();
                    assert!(check("v4-with-v5-memory", &bad).is_err());
                }

                // Independently constructed old envelope/model remains accepted.
                let mut historical = rows.clone();
                historical[0]["schema"] = json!(current_v10::PAGE_RANGE_RESULT_SCHEMA);
                for header in &mut historical[..2] {
                    header["config_schema"] = json!(current_v10::PAGE_RANGE_CONFIG_SCHEMA);
                }
                author_historical_diagnostics(&mut historical);
                let binding = historical
                    .iter_mut()
                    .find(|row| row["phase"] == "source_binding")
                    .unwrap();
                let mut root: Value = serde_json::from_str(
                    binding["generation_authority"]["root_manifest"]
                        .as_str()
                        .unwrap(),
                )
                .unwrap();
                root["schema"] = json!("borsuk-two-bit-generation-v11");
                let body = serde_json::to_string(&root).unwrap();
                let digest = sha(body.as_bytes());
                binding["generation_authority"]["root_manifest"] = json!(body);
                for row in &mut historical {
                    if row.get("generation_root_sha256").is_some() {
                        row["generation_root_sha256"] = json!(digest);
                    }
                    if row["phase"] == "terminal" {
                        row["summary"]["generation_root_sha256"] = json!(digest);
                    }
                }
                check("historical-v15", &historical).unwrap();
                let mut bad = historical.clone();
                bad[admission]["source_trace_ranges"] =
                    rows[admission]["source_trace_ranges"].clone();
                assert!(check("old-current-capacity", &bad).is_err());
                if traced {
                    for field in ["source_range_trace", "cpu_profile"] {
                        let mut bad = historical.clone();
                        bad[query]["diagnostic"][field] = rows[query]["diagnostic"][field].clone();
                        assert!(check(field, &bad).is_err(), "old envelope accepted {field}");
                    }
                    let mut bad = historical.clone();
                    bad[query]["diagnostic"]["sq8_range_trace"]["cpu"] =
                        rows[query]["diagnostic"]["sq8_range_trace"]["cpu"].clone();
                    assert!(check("old-current-sq8-cpu", &bad).is_err());
                }

                // Reseal exact bytes so parser limits, not stale hashes, reject mutations.
                for (name, mut records, padded_len, expected_error) in [
                    ("duplicate-profile", rows.clone(), None, Some("duplicate")),
                    ("v5-line-at-cap", rows.clone(), Some(131072), None),
                    (
                        "v5-line-over-cap",
                        rows.clone(),
                        Some(131073),
                        Some("row cap"),
                    ),
                    ("v4-line-at-cap", v4.clone(), Some(65536), None),
                    ("v4-line-over-cap", v4.clone(), Some(65537), Some("row cap")),
                ] {
                    let seal = records
                        .iter()
                        .position(|r| r["phase"] == "all_queries_sealed")
                        .unwrap();
                    let mut lines = records
                        .iter()
                        .map(|r| encode(std::slice::from_ref(r)))
                        .collect::<Vec<_>>();
                    if let Some(len) = padded_len {
                        let line = &mut lines[query];
                        assert!(line.len() < len);
                        assert_eq!(line.pop(), Some(b'\n'));
                        line.resize(len - 1, b' ');
                        line.push(b'\n');
                    } else {
                        let line = &mut lines[admission];
                        line.truncate(line.len() - 2);
                        line.extend_from_slice(
                            format!(
                                ",\"source_trace_ranges\":{}}}\n",
                                rows[admission]["source_trace_ranges"]
                            )
                            .as_bytes(),
                        );
                    }
                    let prefix = lines[..seal].concat();
                    records[seal]["prefix_bytes"] = json!(prefix.len());
                    records[seal]["prefix_sha256"] = json!(sha(&prefix));
                    lines[seal] = encode(&records[seal..seal + 1]);
                    let sealed = lines[..seal + 1].concat();
                    let terminal = records.len() - 1;
                    let summary = &mut records[terminal]["summary"];
                    summary["prefix_bytes"] = json!(prefix.len());
                    summary["prefix_sha256"] = json!(sha(&prefix));
                    summary["sealed_bytes"] = json!(sealed.len());
                    summary["sealed_sha256"] = json!(sha(&sealed));
                    lines[terminal] = encode(&records[terminal..]);
                    let body = lines.concat();
                    let path = dir.path().join(format!("{name}.jsonl"));
                    std::fs::write(&path, &body).unwrap();
                    let (config, pin) = scale_config(dir.path(), &path, &records);
                    let result = reduce_scale(&config, &pin);
                    if let Some(expected) = expected_error {
                        let error = result.unwrap_err().to_string();
                        assert!(error.contains(expected), "{name}: {error}");
                    } else {
                        result.unwrap();
                    }
                }
            }
        }
    }

    // Synthetic historical evidence: author the old wire/model from invariant
    // observations. No v2 root, directory, payload or ELF is claimed as old output.
    pub(super) fn historical_source_layout_rows(
        observations: &[Value],
        onepass: bool,
    ) -> Vec<Value> {
        let mut rows = observations.to_vec();
        author_historical_diagnostics(&mut rows);
        let inputs = rows[1].clone();
        let capsule = inputs["source_layout"]["layout"] != "flat_records";
        let n = inputs["rows"].as_u64().unwrap();
        let dimensions = inputs["dimensions"].as_u64().unwrap();
        let width = dimensions.div_ceil(32) * 8 + 8;
        let leaves = n.div_ceil(32);
        let directory = 32 + leaves * 64;
        let layout = if capsule {
            json!({"layout":"leaf_capsules","blob_bytes":leaves*(n*width+32+4*n.div_ceil(256)),
                "blob_sha256":"d".repeat(64),"directory_bytes":directory,"directory_sha256":"e".repeat(64)})
        } else {
            json!({"layout":"flat_records"})
        };
        for header in &mut rows[..2] {
            header["config_schema"] = json!(if onepass {
                current_v10::ONEPASS_CONFIG_SCHEMA
            } else {
                current_v10::SOURCE_LAYOUT_CONFIG_SCHEMA
            });
            header["source_layout"] = layout.clone();
            header
                .as_object_mut()
                .unwrap()
                .remove("source_capsule_verification_policy");
            if onepass {
                header["source_capsule_verification_policy"] =
                    json!(current_v10::SOURCE_CAPSULE_VERIFICATION_POLICY);
            }
        }
        rows[0]["schema"] = json!(if onepass {
            current_v10::ONEPASS_RESULT_SCHEMA
        } else {
            current_v10::SOURCE_LAYOUT_RESULT_SCHEMA
        });
        rows[0]["binary_sha256"] = json!("1".repeat(64));
        rows[0]["config_sha256"] = json!("2".repeat(64));
        // Preserve the old handoff's allowed source identity differences.
        for key in [
            "runner_source_sha256",
            "generation_source_sha256",
            "source_capsule_source_sha256",
            "sq8_range_source_sha256",
        ] {
            rows[0][key] = json!(if onepass {
                "3".repeat(64)
            } else {
                "4".repeat(64)
            });
        }
        let startup = rows.iter().find(|r| r["phase"] == "startup").unwrap();
        let router_bytes = startup["metadata"]["metadata"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["name"] == "router/root.bin")
            .unwrap()["bytes"]
            .clone();
        let generation = if capsule { 2 } else { 1 };
        let page = json!({"schema":"borsuk-v115-sq8-page-authority-v2","generation":generation,
            "rows":n,"dimensions":dimensions,"page_rows":32,
            "object_sha256":inputs["native_sq8_sha256"],"page_digest_sha256":"5".repeat(64)});
        let page_body = serde_json::to_string(&page).unwrap();
        let sq8 = inputs["native_sq8_sha256"].as_str().unwrap();
        let key = inputs["backend"]
            .get("sq8_object_key")
            .cloned()
            .unwrap_or_else(|| json!(format!("objects/{sq8}")));
        let etag = inputs["backend"]
            .get("sq8_etag")
            .cloned()
            .unwrap_or(json!("synthetic-old-etag"));
        let root = json!({"schema":"borsuk-two-bit-generation-v10","generation":generation,"base_epoch":0,
            "plane_manifest_sha256":"6".repeat(64),"page_manifest_sha256":sha(page_body.as_bytes()),
            "discovery":{"mode":"semantic","profile":inputs["profile"],
                "root_sha256":"7".repeat(64),"root_bytes":router_bytes,
                "membership_sha256":"8".repeat(64),"membership_bytes":leaves*4,
                "leaves_sha256":"9".repeat(64),"leaves_bytes":leaves*(4+dimensions*2),
                "input_schema":"synthetic-historical-wire","input_root_sha256":"a".repeat(64),
                "centroids_sha256":"b".repeat(64),"source_sha256":inputs["native_source_sha256"],
                "source_order_sha256":inputs["native_order_sha256"],"mean_sha256":"c".repeat(64),
                "records_sha256":"d".repeat(64),"sq8_sha256":sq8},
            "source_layout":layout,"sq8_object_sha256":sq8,"sq8_object_key":key,"sq8_etag":etag,
            "canonical":null,"low":vec![0.0; dimensions as usize],"step":vec![1.0; dimensions as usize]});
        let root_body = serde_json::to_string(&root).unwrap();
        let root_sha = sha(root_body.as_bytes());
        let mut source_gets = 0;
        let mut source_bytes = 0;
        for row in &mut rows {
            match row["phase"].as_str().unwrap() {
                "bound_inputs" | "all_queries_sealed" => {
                    row["generation_root_sha256"] = json!(root_sha)
                }
                "source_binding" => {
                    row["generation_authority"] =
                        json!({"root_manifest":root_body,"page_manifest":page_body})
                }
                "startup" => {
                    row["source_layout"] = layout.clone();
                    for entry in row["metadata"]["metadata"].as_array_mut().unwrap() {
                        let bytes = match entry["name"].as_str().unwrap() {
                            "manifest.json" => root_body.len() as u64,
                            "page_manifest.json" => page_body.len() as u64,
                            "plane/capsule_directory.bin" => directory,
                            _ => continue,
                        };
                        entry["bytes"] = json!(bytes);
                        entry["payload_buffer_bound_bytes"] = json!(bytes);
                        entry["logical_get_requests"] = json!(bytes.div_ceil(4 * 1024 * 1024));
                    }
                }
                "query" => {
                    if capsule {
                        let selected = row["trace"]["semantic_leaves"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|id| id.as_u64().unwrap())
                            .collect::<std::collections::BTreeSet<_>>();
                        let mut previous = None;
                        let mut gets = 0;
                        for &id in &selected {
                            if previous.is_none() || previous != id.checked_sub(1) {
                                gets += 1;
                            }
                            previous = Some(id);
                        }
                        let mut pages = std::collections::BTreeMap::<u64, u64>::new();
                        for unit in row["trace"]["semantic_units"].as_array().unwrap() {
                            *pages.entry(unit.as_u64().unwrap() / 8).or_default() += 1;
                        }
                        let mut logical = 0;
                        let mut payload = 0;
                        let mut occurrences = 0;
                        for (page, units) in pages {
                            let bytes = (n - page * 256).min(256) * width;
                            let copies = units.min(selected.len() as u64);
                            logical += bytes;
                            payload += bytes * copies;
                            occurrences += copies;
                        }
                        let framing = 32 * selected.len() as u64 + 4 * occurrences;
                        let physical = payload + framing;
                        row["stages"]["source_capsules"] = json!({
                            "physical_bytes":physical,"logical_bytes":logical,"duplicate_bytes":payload-logical,
                            "framing_bytes":framing,"hash_bytes":if onepass {physical+payload} else {2*(physical+payload)},
                            "memory_bytes":16*1024*1024_u64 - if onepass {0} else {40*gets}});
                        row["charges"]["source"] = json!({"submitted_gets":gets,"verified_bytes":physical,"failed_gets":0});
                        row["sum"] = json!({
                            "submitted_gets":gets + row["charges"]["router"]["submitted_gets"].as_u64().unwrap() + row["charges"]["sq8"]["submitted_gets"].as_u64().unwrap(),
                            "verified_bytes":physical + row["charges"]["router"]["verified_bytes"].as_u64().unwrap() + row["charges"]["sq8"]["verified_bytes"].as_u64().unwrap(),
                            "failed_gets":0});
                    }
                    source_gets += row["charges"]["source"]["submitted_gets"].as_u64().unwrap();
                    source_bytes += row["charges"]["source"]["verified_bytes"].as_u64().unwrap();
                }
                _ => (),
            }
        }
        let summary = &mut rows.last_mut().unwrap()["summary"];
        summary["generation_root_sha256"] = json!(root_sha);
        summary["charges"]["source"] =
            json!({"submitted_gets":source_gets,"verified_bytes":source_bytes,"failed_gets":0});
        summary["sum"] = json!({
            "submitted_gets":source_gets + summary["charges"]["router"]["submitted_gets"].as_u64().unwrap() + summary["charges"]["sq8"]["submitted_gets"].as_u64().unwrap(),
            "verified_bytes":source_bytes + summary["charges"]["router"]["verified_bytes"].as_u64().unwrap() + summary["charges"]["sq8"]["verified_bytes"].as_u64().unwrap(),
            "failed_gets":0});
        rows
    }

    #[test]
    fn paired_source_layout_native_oracles_and_strict_falsifiers() {
        let (dir, paths) = scale_native_runner::source_layout_reducer_native_fixture().unwrap();
        let flat = assert_current_native(dir.path(), &paths[0]);
        let capsules =
            assert_current_native_config(&dir.path().join("capsule-config.json"), &paths[1]);
        let completed = |path: &Path, rows: &[Value]| {
            let body = std::fs::read(path).unwrap();
            json!({"schema":SCALE_CONFIG_SCHEMA,
                "input":{"path":path,"bytes":body.len(),"sha256":sha(&body)},
                "expected_identity":rows[0],"expected_bound_inputs":rows[1]})
        };
        let config = json!({"schema":"borsuk-paired-source-layout-config-v1",
            "arms":[completed(&paths[0], &flat),completed(&paths[1], &capsules)]});
        let check = |config: &Value| {
            let body = serde_json::to_vec(config).unwrap();
            let path = dir.path().join("paired-layout.json");
            std::fs::write(&path, &body).unwrap();
            current_v10::paired_source_layout(&path, &sha(&body))
        };
        let report = check(&config).unwrap();
        assert_eq!(report["status"], "PARITY_VERIFIED");
        assert_eq!(report["queries"], 2);
        assert_eq!(report["qualified"], false);
        let qi = capsules.iter().position(|r| r["phase"] == "query").unwrap();
        let bi = capsules
            .iter()
            .position(|r| r["phase"] == "source_binding")
            .unwrap();
        let si = capsules
            .iter()
            .position(|r| r["phase"] == "startup")
            .unwrap();
        let seal = capsules
            .iter()
            .position(|r| r["phase"] == "all_queries_sealed")
            .unwrap();
        let recall = capsules
            .iter()
            .position(|r| r["phase"] == "recall")
            .unwrap();
        // Synthetic calibration metadata over native rows; these queries were not
        // rerun with REAL1M calibration. Decimal tokens came from the closed roots.
        let mut observed_roots: [Value; 2] = [&flat, &capsules].map(|rows| {
            serde_json::from_str(
                rows[bi]["generation_authority"]["root_manifest"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap()
        });
        let observed_tokens = [
            ("-0.10864144563674927", "0.0008487452287226915"),
            ("-0.108641446", "0.0008487452"),
        ];
        for (root, (low, step)) in observed_roots.iter_mut().zip(observed_tokens) {
            for (key, token, bits) in [("low", low, 0xbdde7f68), ("step", step, 0x3a5e7e54)] {
                root[key][0] = serde_json::from_str(token).unwrap();
                assert_eq!(serde_json::from_str::<f32>(token).unwrap().to_bits(), bits);
            }
        }
        let synthetic_metadata = |bodies: [String; 2]| {
            let mut synthetic = config.clone();
            for (i, (original, root_body)) in [&flat, &capsules].into_iter().zip(bodies).enumerate()
            {
                let mut rows = original.clone();
                let root_sha = sha(root_body.as_bytes());
                rows[bi]["generation_authority"]["root_manifest"] = json!(root_body);
                rows[1]["generation_root_sha256"] = json!(root_sha);
                rows[seal]["generation_root_sha256"] = json!(root_sha);
                rows.last_mut().unwrap()["summary"]["generation_root_sha256"] = json!(root_sha);
                for entry in rows[si]["metadata"]["metadata"].as_array_mut().unwrap() {
                    if entry["name"] == "manifest.json" {
                        entry["bytes"] = json!(root_body.len());
                        entry["payload_buffer_bound_bytes"] = json!(root_body.len());
                    }
                }
                let (path, _) = write_fixture(
                    dir.path(),
                    &format!("synthetic-calibration-{i}.jsonl"),
                    rows.clone(),
                );
                synthetic["arms"][i] = completed(&path, &rows);
            }
            synthetic
        };
        let check_roots = |roots: &[Value; 2]| {
            check(&synthetic_metadata(
                roots
                    .clone()
                    .map(|root| serde_json::to_string(&root).unwrap()),
            ))
        };
        // Preserve the original decimal spellings in the authenticated raw bodies.
        let observed_bodies = std::array::from_fn(|i| {
            let mut root = observed_roots[i].clone();
            root["low"][0] = json!("OBSERVED_LOW");
            root["step"][0] = json!("OBSERVED_STEP");
            serde_json::to_string(&root)
                .unwrap()
                .replace("\"OBSERVED_LOW\"", observed_tokens[i].0)
                .replace("\"OBSERVED_STEP\"", observed_tokens[i].1)
        });
        assert_eq!(
            check(&synthetic_metadata(observed_bodies)).unwrap()["status"],
            "PARITY_VERIFIED",
        );
        assert_ne!(observed_roots[0]["low"][0], observed_roots[1]["low"][0]);
        assert_ne!(observed_roots[0]["step"][0], observed_roots[1]["step"][0]);
        for (key, bits) in [("low", 0xbdde7f68), ("step", 0x3a5e7e54)] {
            for changed_bits in [bits - 1, bits + 1] {
                let mut bad = observed_roots.clone();
                bad[1][key][0] = json!(f32::from_bits(changed_bits));
                assert_eq!(
                    check_roots(&bad).unwrap_err().to_string(),
                    "paired SOURCE root/page derivation, SOURCE authority, SQ8 digests, router or calibration drift",
                );
            }
            for value in [json!(null), json!(true), json!("NaN"), json!(1e100)] {
                let mut bad = observed_roots.clone();
                bad[1][key][0] = value;
                assert!(check_roots(&bad).is_err(), "{key} element type/overflow");
            }
            for value in [json!(null), json!(0), json!("array")] {
                let mut bad = observed_roots.clone();
                bad[1][key] = value;
                assert!(check_roots(&bad).is_err(), "{key} array type");
            }
            for longer in [false, true] {
                let mut bad = observed_roots.clone();
                let values = bad[1][key].as_array_mut().unwrap();
                if longer {
                    values.push(json!(1.0));
                } else {
                    values.pop();
                }
                assert!(check_roots(&bad).is_err(), "{key} width {longer}");
            }
            for token in ["NaN", "Infinity", "-Infinity", "1e400"] {
                let mut bad = observed_roots.clone();
                bad[1][key][0] = json!("NONFINITE");
                let mut bodies = bad.map(|root| serde_json::to_string(&root).unwrap());
                bodies[1] = bodies[1].replace("\"NONFINITE\"", token);
                assert!(check(&synthetic_metadata(bodies)).is_err(), "{key} {token}");
            }
        }
        for value in [0.0, -0.0, -1.0, 1e-100] {
            let mut bad = observed_roots.clone();
            bad[1]["step"][0] = json!(value);
            assert!(check_roots(&bad).is_err(), "nonpositive f32 step {value}");
        }
        for zero in [0.0, -0.0] {
            let mut roots = observed_roots.clone();
            roots[0]["low"][0] = json!(zero);
            roots[1]["low"][0] = json!(zero);
            assert!(check_roots(&roots).is_ok());
            roots[1]["low"][0] = json!(-zero);
            assert_eq!(
                check_roots(&roots).unwrap_err().to_string(),
                "paired SOURCE root/page derivation, SOURCE authority, SQ8 digests, router or calibration drift",
            );
        }

        // Existing literal fixture oracles remain unchanged across the physical repack.
        for (a, b) in flat
            .iter()
            .filter(|r| r["phase"] == "query")
            .zip(capsules.iter().filter(|r| r["phase"] == "query"))
        {
            assert_eq!(a["returned"], b["returned"]);
            assert_eq!(a["trace"], b["trace"]);
            assert_eq!(a["plan"], b["plan"]);
        }
        for (index, pointer, value) in [
            (qi, "/truth_opened", json!(true)),
            (qi, "/trace/semantic_units", json!([0, 0])),
            (qi, "/returned/0/score_bits", json!(1)),
            (qi, "/returned/0/id", json!(31)),
            (qi, "/returned_physical_ordinals/0", json!(31)),
            (qi, "/plan/logical_range_count", json!(999)),
            (qi, "/stages/source_capsules/physical_bytes", json!(0)),
            (qi, "/stages/source_capsules/hash_bytes", json!(0)),
            (qi, "/stages/source_capsules/memory_bytes", json!(0)),
            (
                bi,
                "/generation_authority/root_manifest",
                json!("0".repeat(64)),
            ),
            (si, "/metadata/metadata/0/name", json!("plane/records.bin")),
            (seal, "/requires_successful_sync", json!(false)),
            (recall, "/hits10", json!(0)),
        ] {
            let mut bad = capsules.clone();
            *bad[index].pointer_mut(pointer).unwrap() = value;
            let (path, _) = write_fixture(dir.path(), "layout-falsifier.jsonl", bad.clone());
            let mut invalid = config.clone();
            invalid["arms"][1] = completed(&path, &bad);
            assert!(check(&invalid).is_err(), "{index} {pointer}");
        }

        for (logical, duplicate) in [(8184, 264), (264, 8184)] {
            let mut bad = capsules.clone();
            bad[qi]["stages"]["source_capsules"]["logical_bytes"] = json!(logical);
            bad[qi]["stages"]["source_capsules"]["duplicate_bytes"] = json!(duplicate);
            let (path, _) = write_fixture(
                dir.path(),
                "layout-impossible-page-split.jsonl",
                bad.clone(),
            );
            let mut invalid = config.clone();
            invalid["arms"][1] = completed(&path, &bad);
            assert!(
                check(&invalid).is_err(),
                "whole-page split {logical}+{duplicate}"
            );
        }
        let mut bad = capsules.clone();
        assert_eq!(bad[qi]["charges"]["source"]["submitted_gets"], 1);
        bad[qi]["charges"]["source"]["submitted_gets"] = json!(0);
        bad[qi]["sum"]["submitted_gets"] =
            json!(bad[qi]["sum"]["submitted_gets"].as_u64().unwrap() - 1);
        let summary = &mut bad.last_mut().unwrap()["summary"];
        for pointer in ["/charges/source/submitted_gets", "/sum/submitted_gets"] {
            let counter = summary.pointer_mut(pointer).unwrap();
            *counter = json!(counter.as_u64().unwrap() - 1);
        }
        let (path, _) = write_fixture(dir.path(), "layout-impossible-zero-get.jsonl", bad.clone());
        let mut invalid = config.clone();
        invalid["arms"][1] = completed(&path, &bad);
        assert!(check(&invalid).is_err());

        for name in [
            "manifest.json",
            "plane/capsule_directory.bin",
            "router/membership.bin",
        ] {
            for (key, value) in [
                ("logical_get_requests", json!(0)),
                ("logical_head_requests", json!(9)),
                ("payload_buffer_bound_bytes", json!(u64::MAX)),
                ("metadata_wave", json!(9)),
                ("reused_root_bytes", json!(1)),
            ] {
                let mut bad = capsules.clone();
                let entry = bad[si]["metadata"]["metadata"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|entry| entry["name"] == name)
                    .unwrap();
                entry[key] = value;
                let (path, _) =
                    write_fixture(dir.path(), "layout-forged-startup.jsonl", bad.clone());
                let mut invalid = config.clone();
                invalid["arms"][1] = completed(&path, &bad);
                assert!(check(&invalid).is_err(), "{name}: {key}");
            }
        }

        // Rehash every derived manifest and seal: unchanged shared authority is
        // still required, rather than accepting any two individually pinned roots.
        for change in ["router", "source_plane", "sq8_page_digests", "calibration"] {
            let mut bad = capsules.clone();
            let mut root: Value = serde_json::from_str(
                bad[bi]["generation_authority"]["root_manifest"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap();
            let mut page: Value = serde_json::from_str(
                bad[bi]["generation_authority"]["page_manifest"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap();
            match change {
                "router" => root["discovery"]["root_sha256"] = json!("0".repeat(64)),
                "source_plane" => root["plane_manifest_sha256"] = json!("0".repeat(64)),
                "sq8_page_digests" => page["page_digest_sha256"] = json!("0".repeat(64)),
                "calibration" => root["low"][0] = json!(1.0),
                _ => unreachable!(),
            }
            let page_body = serde_json::to_string(&page).unwrap();
            root["page_manifest_sha256"] = json!(sha(page_body.as_bytes()));
            let root_body = serde_json::to_string(&root).unwrap();
            let root_sha = sha(root_body.as_bytes());
            bad[bi]["generation_authority"]["root_manifest"] = json!(root_body);
            bad[bi]["generation_authority"]["page_manifest"] = json!(page_body);
            bad[1]["generation_root_sha256"] = json!(root_sha);
            bad[seal]["generation_root_sha256"] = json!(root_sha);
            bad.last_mut().unwrap()["summary"]["generation_root_sha256"] = json!(root_sha);
            for entry in bad[si]["metadata"]["metadata"].as_array_mut().unwrap() {
                if entry["name"] == "manifest.json" {
                    entry["bytes"] = json!(root_body.len());
                    entry["payload_buffer_bound_bytes"] = json!(root_body.len());
                }
                if entry["name"] == "page_manifest.json" {
                    entry["bytes"] = json!(page_body.len());
                    entry["payload_buffer_bound_bytes"] = json!(page_body.len());
                }
            }
            let (path, _) = write_fixture(
                dir.path(),
                "layout-rehashed-authority-drift.jsonl",
                bad.clone(),
            );
            let mut invalid = config.clone();
            invalid["arms"][1] = completed(&path, &bad);
            assert!(check(&invalid).is_err(), "{change}");
        }

        // A self-consistent recall change reaches the paired per-query gate.
        let mut bad = capsules.clone();
        let hits = bad[recall]["hits10"].as_u64().unwrap();
        assert!(hits > 0);
        bad[recall]["hits10"] = json!(hits - 1);
        bad[recall]["recall10"] = json!((hits - 1) as f64 / K as f64);
        let t = &mut bad.last_mut().unwrap()["summary"];
        let total = t["total_hits10"].as_u64().unwrap() - 1;
        t["total_hits10"] = json!(total);
        t["recall_numerator"] = json!(total);
        t["mean_recall10"] = json!(total as f64 / t["recall_denominator"].as_u64().unwrap() as f64);
        let (path, _) = write_fixture(dir.path(), "layout-recall-drift.jsonl", bad.clone());
        let mut invalid = config.clone();
        invalid["arms"][1] = completed(&path, &bad);
        assert!(check(&invalid).is_err());

        for (header, key, value) in [
            (
                "expected_identity",
                "source_capsule_source_sha256",
                json!("0".repeat(64)),
            ),
            (
                "expected_identity",
                "router_source_sha256",
                json!("0".repeat(64)),
            ),
            (
                "expected_bound_inputs",
                "native_order_sha256",
                json!("0".repeat(64)),
            ),
            (
                "expected_bound_inputs",
                "native_sq8_sha256",
                json!("0".repeat(64)),
            ),
            (
                "expected_bound_inputs",
                "truth_sha256",
                json!("0".repeat(64)),
            ),
            (
                "expected_bound_inputs",
                "requests_sha256",
                json!("0".repeat(64)),
            ),
            ("expected_bound_inputs", "source_byte_budget", json!(1)),
            ("expected_bound_inputs", "max_memory_bytes", json!(1)),
        ] {
            let mut invalid = config.clone();
            invalid["arms"][1][header][key] = value;
            assert!(check(&invalid).is_err(), "{header} {key}");
        }
        let mut swapped = config.clone();
        swapped["arms"].as_array_mut().unwrap().swap(0, 1);
        assert!(check(&swapped).is_err());
        // Full result SHA and the internal query seal are independent gates.
        let mut unsealed = capsules.clone();
        unsealed[qi]["query_wall_ns"] = json!(unsealed[qi]["query_wall_ns"].as_u64().unwrap() + 1);
        let body = encode(&unsealed);
        let path = dir.path().join("unsealed-layout.jsonl");
        std::fs::write(&path, &body).unwrap();
        let mut invalid = config.clone();
        invalid["arms"][1] = completed(&path, &unsealed);
        assert!(check(&invalid).is_err());
        invalid = config.clone();
        invalid["arms"][1]["input"]["sha256"] = json!("0".repeat(64));
        assert!(check(&invalid).is_err());

        // The new CLI uses the existing create-only publisher.
        let body = serde_json::to_vec(&config).unwrap();
        let path = dir.path().join("paired-layout.json");
        std::fs::write(&path, &body).unwrap();
        let output = dir.path().join("layout-report.json");
        assert!(
            execute_report(&output, "borsuk-paired-source-layout-v1", || {
                current_v10::paired_source_layout(&path, &sha(&body))
            })
            .unwrap()
        );
        assert!(
            execute_report(&output, "borsuk-paired-source-layout-v1", || {
                current_v10::paired_source_layout(&path, &sha(&body))
            })
            .is_err()
        );
    }

    #[test]
    fn source_layout_native_multileaf_tail_and_near_cap_panel_authority() {
        for (rows, panel, padded) in [(2049, false, false), (32, true, true)] {
            let (dir, paths) = scale_native_runner::source_layout_reducer_native_fixture_with(
                rows, 2, panel, padded,
            )
            .unwrap();
            let flat = assert_current_native(dir.path(), &paths[0]);
            let capsules =
                assert_current_native_config(&dir.path().join("capsule-config.json"), &paths[1]);
            let arms = paths.iter().zip([&flat, &capsules]).map(|(path, records)| {
                let body = std::fs::read(path).unwrap();
                json!({"schema":SCALE_CONFIG_SCHEMA,"input":{"path":path,"bytes":body.len(),"sha256":sha(&body)},
                    "expected_identity":records[0],"expected_bound_inputs":records[1]})
            }).collect::<Vec<_>>();
            let config = json!({"schema":"borsuk-paired-source-layout-config-v1","arms":arms});
            let body = serde_json::to_vec(&config).unwrap();
            let path = dir.path().join("paired-layout-expanded.json");
            std::fs::write(&path, &body).unwrap();
            let report = current_v10::paired_source_layout(&path, &sha(&body)).unwrap();
            assert_eq!(report["status"], "PARITY_VERIFIED");
            for (a, b) in flat
                .iter()
                .filter(|r| r["phase"] == "query")
                .zip(capsules.iter().filter(|r| r["phase"] == "query"))
            {
                assert_eq!(a["trace"], b["trace"]);
                assert_eq!(a["returned"], b["returned"]);
                assert_eq!(
                    a["returned_physical_ordinals"],
                    b["returned_physical_ordinals"]
                );
                assert_eq!(a["plan"], b["plan"]);
                if rows > 32 {
                    assert!(b["trace"]["semantic_leaves"].as_array().unwrap().len() > 1);
                    assert_eq!(b["stages"]["source_capsules"]["logical_bytes"], rows * 264);
                    assert_eq!(b["charges"]["source"]["submitted_gets"], 1);
                }
            }
            if padded {
                let binding = flat
                    .iter()
                    .find(|r| r["phase"] == "source_binding")
                    .unwrap();
                assert_eq!(
                    binding["generation_authority"]["root_manifest"]
                        .as_str()
                        .unwrap()
                        .len(),
                    65_536
                );
                assert_eq!(
                    binding["generation_authority"]["page_manifest"]
                        .as_str()
                        .unwrap()
                        .len(),
                    65_536
                );
                assert!(serde_json::to_vec(binding).unwrap().len() > 65_536);
                assert!(flat[1]["max_memory_bytes"].as_u64().unwrap() > 512 * 1024 * 1024);
                assert!(flat[1]["source_byte_budget"].as_u64().unwrap() > 64 * 1024 * 1024);
                // The same oversized record is still refused by every historical reader.
                let mut reader = Rows::open(&paths[0]).unwrap();
                reader.line_cap = 65_536;
                for phase in ["identity", "bound_inputs", "diagnostic_admission"] {
                    reader.row::<Value>(phase).unwrap();
                }
                assert!(reader.row::<Value>("source_binding").is_err());
            }
        }
    }

    #[test]
    fn page_range_v2_diagnostic32_uses_ordinary_completed_scale_publication() {
        let (dir, paths) =
            scale_native_runner::source_layout_reducer_native_fixture_with(32, 33, true, false)
                .unwrap();
        let rows = assert_current_native_config(&dir.path().join("capsule-config.json"), &paths[1]);
        assert_eq!(rows.iter().filter(|r| r["phase"] == "query").count(), 32);
        let (config, pin) = scale_config(dir.path(), &paths[1], &rows);
        let output = dir
            .path()
            .join("ordinary-completed-scale-diagnostic32.json");
        assert!(
            execute_report(&output, SCALE_REPORT_SCHEMA, || reduce_scale(&config, &pin)).unwrap()
        );
        let report: Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
        assert_eq!(report["status"], "MEASURED");
        assert_eq!(report["diagnostic_query_count"], 32);
        assert_eq!(report["population_percentiles_valid"], false);
        assert!(report["statistics"].is_null());
        assert!(report["stage_statistics"].is_null());
    }

    #[test]
    fn framed_source_current_reader_and_corrupt_accounting() {
        let (dir, paths) = scale_native_runner::source_layout_reducer_layout_fixture(
            513,
            2,
            true,
            false,
            true,
            borsuk::two_bit_generation::SourceLayoutMode::FramedRecordsV1,
        )
        .unwrap();
        for (path, config) in paths.iter().zip(["config.json", "capsule-config.json"]) {
            assert_current_native_config(&dir.path().join(config), path);
        }
        let rows = assert_current_native_config(&dir.path().join("capsule-config.json"), &paths[1]);
        let qi = rows.iter().position(|r| r["phase"] == "query").unwrap();
        for key in ["physical_bytes", "decoded_bytes", "hash_bytes"] {
            let mut bad = rows.clone();
            bad[qi]["stages"]["source_frames"][key] = json!(1);
            let (path, _) =
                write_fixture(dir.path(), &format!("bad-frame-{key}.jsonl"), bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            assert!(reduce_scale(&config, &pin).is_err());
        }
    }

    #[test]
    fn framed_source_trace_off_coordinated_physical_refusals() {
        let (dir, paths) = scale_native_runner::source_layout_reducer_layout_fixture(
            32,
            2,
            true,
            false,
            false,
            borsuk::two_bit_generation::SourceLayoutMode::FramedRecordsV1,
        )
        .unwrap();
        let rows = assert_current_native_config(&dir.path().join("capsule-config.json"), &paths[1]);
        let blob = rows[1]["source_layout"]["blob_bytes"].as_u64().unwrap();
        let width =
            source_record_bytes(rows[1]["dimensions"].as_u64().unwrap() as usize).unwrap() as u64;
        assert!(blob > 26 && blob < 32 * width + 25);
        for physical in [1, 32 * width + 26, blob - 1] {
            let mut bad = rows.clone();
            let mut old_total = 0;
            let mut new_total = 0;
            for row in bad.iter_mut().filter(|r| r["phase"] == "query") {
                let old = row["charges"]["source"]["verified_bytes"].as_u64().unwrap();
                old_total += old;
                new_total += physical;
                row["stages"]["source_frames"]["physical_bytes"] = json!(physical);
                row["charges"]["source"]["verified_bytes"] = json!(physical);
                row["sum"]["verified_bytes"] =
                    json!(row["sum"]["verified_bytes"].as_u64().unwrap() - old + physical);
            }
            let summary = &mut bad.last_mut().unwrap()["summary"];
            summary["charges"]["source"]["verified_bytes"] = json!(new_total);
            summary["sum"]["verified_bytes"] =
                json!(summary["sum"]["verified_bytes"].as_u64().unwrap() - old_total + new_total);
            let (path, _) = write_fixture(dir.path(), "frame-coordinated.jsonl", bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            let error = reduce_scale(&config, &pin).unwrap_err().to_string();
            assert!(
                error.contains("framed SOURCE physical/decoded/hash conservation"),
                "{error}"
            );
        }
    }

    #[test]
    fn page_range_v2_completed_policy_schema_and_root_refusals() {
        let (dir, paths) = scale_native_runner::source_layout_reducer_native_fixture().unwrap();
        let current =
            assert_current_native_config(&dir.path().join("capsule-config.json"), &paths[1]);
        assert_eq!(current[0]["schema"], current_v10::FRAMED_RESULT_SCHEMA);
        assert_eq!(
            current[1]["config_schema"],
            current_v10::FRAMED_CONFIG_SCHEMA
        );
        assert_eq!(current[1]["source_layout"]["layout"], "leaf_capsules_v2");
        for mutation in [
            "missing_policy",
            "old_policy",
            "extra_policy",
            "skip_verification",
            "null_policy",
            "result_schema",
            "config_schema",
            "old_layout",
            "old_root",
        ] {
            let mut bad = current.clone();
            match mutation {
                "missing_policy" => {
                    for header in &mut bad[..2] {
                        header
                            .as_object_mut()
                            .unwrap()
                            .remove("source_capsule_verification_policy");
                    }
                }
                "old_policy" | "null_policy" => {
                    for header in &mut bad[..2] {
                        header["source_capsule_verification_policy"] = if mutation == "old_policy" {
                            json!(current_v10::SOURCE_CAPSULE_VERIFICATION_POLICY)
                        } else {
                            Value::Null
                        };
                    }
                }
                "extra_policy" | "skip_verification" => {
                    for header in &mut bad[..2] {
                        header[mutation] = json!(true);
                    }
                }
                "result_schema" => bad[0]["schema"] = json!(current_v10::ONEPASS_RESULT_SCHEMA),
                "config_schema" => {
                    for header in &mut bad[..2] {
                        header["config_schema"] = json!(current_v10::ONEPASS_CONFIG_SCHEMA);
                        header["source_capsule_verification_policy"] =
                            json!(current_v10::SOURCE_CAPSULE_VERIFICATION_POLICY);
                    }
                }
                "old_layout" => {
                    for header in &mut bad[..2] {
                        header["source_layout"]["layout"] = json!("leaf_capsules");
                    }
                }
                "old_root" => {
                    let binding = bad
                        .iter()
                        .position(|r| r["phase"] == "source_binding")
                        .unwrap();
                    let mut root: Value = serde_json::from_str(
                        bad[binding]["generation_authority"]["root_manifest"]
                            .as_str()
                            .unwrap(),
                    )
                    .unwrap();
                    // Invalid negative case only: a rehashed v10 marker cannot admit v15.
                    root["schema"] = json!("borsuk-two-bit-generation-v10");
                    let body = serde_json::to_string(&root).unwrap();
                    let digest = sha(body.as_bytes());
                    bad[binding]["generation_authority"]["root_manifest"] = json!(body);
                    bad[1]["generation_root_sha256"] = json!(digest);
                    for row in &mut bad {
                        if row["phase"] == "all_queries_sealed" {
                            row["generation_root_sha256"] = json!(digest);
                        }
                        if row["phase"] == "terminal" {
                            row["summary"]["generation_root_sha256"] = json!(digest);
                        }
                        if row["phase"] == "startup" {
                            for entry in row["metadata"]["metadata"].as_array_mut().unwrap() {
                                if entry["name"] == "manifest.json" {
                                    entry["bytes"] = json!(body.len());
                                    entry["payload_buffer_bound_bytes"] = json!(body.len());
                                }
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
            let (path, _) =
                write_fixture(dir.path(), "page-range-resealed-refusal.jsonl", bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            let error = reduce_scale(&config, &pin).unwrap_err().to_string();
            assert!(!error.contains("seal"), "{mutation}: {error}");
        }
    }

    #[test]
    fn page_range_v2_authored_historical_completion_and_policy_hash_boundaries() {
        for panel in [false, true] {
            let (dir, paths) = scale_native_runner::source_layout_reducer_native_fixture_with(
                2049, 2, panel, false,
            )
            .unwrap();
            let native =
                assert_current_native_config(&dir.path().join("capsule-config.json"), &paths[1]);
            let current = historical_source_layout_rows(&native, true);
            let (current_path, _) =
                write_fixture(dir.path(), "authored-old-onepass.jsonl", current.clone());
            assert_eq!(current[0]["schema"], current_v10::ONEPASS_RESULT_SCHEMA);
            assert_eq!(
                current[1]["config_schema"],
                current_v10::ONEPASS_CONFIG_SCHEMA
            );
            for row in current.iter().filter(|r| r["phase"] == "query") {
                let stats = &row["stages"]["source_capsules"];
                let physical = stats["physical_bytes"].as_u64().unwrap();
                let framing = stats["framing_bytes"].as_u64().unwrap();
                assert_eq!(stats["hash_bytes"], 2 * physical - framing);
            }
            let (config, pin) = scale_config(dir.path(), &current_path, &current);
            let report = reduce_scale(&config, &pin).unwrap();
            assert_eq!(report["status"], "MEASURED");
            assert_eq!(report["diagnostic_only"], true);
            assert_eq!(report["population_percentiles_valid"], false);

            let historical = historical_source_layout_rows(&native, false);
            let (path, _) = write_fixture(
                dir.path(),
                "synthetic-config-v12-result-v13.jsonl",
                historical.clone(),
            );
            let (config, pin) = scale_config(dir.path(), &path, &historical);
            assert_eq!(reduce_scale(&config, &pin).unwrap()["status"], "MEASURED");

            for (onepass, original) in [(true, &current), (false, &historical)] {
                for mutation in [
                    "opposite_hash",
                    "missing_policy",
                    "wrong_policy",
                    "null_policy",
                    "numeric_policy",
                    "identity_policy",
                    "config_pair",
                    "result_pair",
                ] {
                    if !onepass && mutation == "missing_policy" {
                        continue;
                    }
                    let mut bad = original.clone();
                    match mutation {
                        "opposite_hash" => {
                            for row in bad.iter_mut().filter(|r| r["phase"] == "query") {
                                let stats = &mut row["stages"]["source_capsules"];
                                let physical = stats["physical_bytes"].as_u64().unwrap();
                                let framing = stats["framing_bytes"].as_u64().unwrap();
                                stats["hash_bytes"] = json!(if onepass {
                                    2 * (physical + physical - framing)
                                } else {
                                    2 * physical - framing
                                });
                            }
                        }
                        "missing_policy" => {
                            for header in &mut bad[..2] {
                                header
                                    .as_object_mut()
                                    .unwrap()
                                    .remove("source_capsule_verification_policy");
                            }
                        }
                        "identity_policy" => {
                            bad[0]["source_capsule_verification_policy"] = json!("skip")
                        }
                        "wrong_policy" | "null_policy" | "numeric_policy" => {
                            let value = match mutation {
                                "wrong_policy" => json!("skip"),
                                "null_policy" => Value::Null,
                                _ => json!(1),
                            };
                            for header in &mut bad[..2] {
                                header["source_capsule_verification_policy"] = value.clone();
                            }
                        }
                        "config_pair" => {
                            for header in &mut bad[..2] {
                                header["config_schema"] = json!(if onepass {
                                    current_v10::SOURCE_LAYOUT_CONFIG_SCHEMA
                                } else {
                                    current_v10::ONEPASS_CONFIG_SCHEMA
                                });
                                if onepass {
                                    header
                                        .as_object_mut()
                                        .unwrap()
                                        .remove("source_capsule_verification_policy");
                                } else {
                                    header["source_capsule_verification_policy"] =
                                        json!(current_v10::SOURCE_CAPSULE_VERIFICATION_POLICY);
                                }
                            }
                        }
                        "result_pair" => {
                            bad[0]["schema"] = json!(if onepass {
                                current_v10::SOURCE_LAYOUT_RESULT_SCHEMA
                            } else {
                                current_v10::ONEPASS_RESULT_SCHEMA
                            })
                        }
                        _ => unreachable!(),
                    }
                    // Recompute the internal query seal and pin the changed full file.
                    let (path, _) =
                        write_fixture(dir.path(), "onepass-resealed-mutation.jsonl", bad.clone());
                    let (config, pin) = scale_config(dir.path(), &path, &bad);
                    let error = reduce_scale(&config, &pin).unwrap_err().to_string();
                    if mutation == "opposite_hash" {
                        assert_eq!(error, "v13 physical/logical/hash SOURCE conservation");
                    }
                    assert!(
                        !error.contains("seal"),
                        "semantic rejection must survive resealing: {onepass} {mutation}: {error}"
                    );
                }
            }
        }
    }

    #[test]
    fn source_layout_stats_unsigned_roster_and_physical_accounting() {
        let (dir, paths) = scale_native_runner::source_layout_reducer_native_fixture().unwrap();
        let flat = assert_current_native(dir.path(), &paths[0]);
        let capsules =
            assert_current_native_config(&dir.path().join("capsule-config.json"), &paths[1]);
        let query = capsules.iter().find(|r| r["phase"] == "query").unwrap();
        current_v10::validate_source_capsules(query, &capsules[1]).unwrap();
        let flat_query = flat.iter().find(|r| r["phase"] == "query").unwrap();
        current_v10::validate_source_capsules(flat_query, &flat[1]).unwrap();
        let mut zero_get = flat_query.clone();
        zero_get["charges"]["source"]["submitted_gets"] = json!(0);
        assert!(current_v10::validate_source_capsules(&zero_get, &flat[1]).is_err());
        for key in
            "physical_bytes logical_bytes duplicate_bytes framing_bytes hash_bytes memory_bytes"
                .split_whitespace()
        {
            for value in [
                Value::Null,
                json!(-1),
                json!(1.5),
                json!(true),
                json!(u64::MAX),
            ] {
                let mut bad = query.clone();
                bad["stages"]["source_capsules"][key] = value;
                assert!(
                    current_v10::validate_source_capsules(&bad, &capsules[1]).is_err(),
                    "{key}"
                );
            }
            let mut missing = query.clone();
            missing["stages"]["source_capsules"]
                .as_object_mut()
                .unwrap()
                .remove(key);
            assert!(current_v10::validate_source_capsules(&missing, &capsules[1]).is_err());
        }
        let mut bad = query.clone();
        bad["stages"]["source_capsules"]["extra"] = json!(0);
        assert!(current_v10::validate_source_capsules(&bad, &capsules[1]).is_err());
        let mut bad = query.clone();
        bad["charges"]["source"]["verified_bytes"] = json!(
            bad["stages"]["source_capsules"]["physical_bytes"]
                .as_u64()
                .unwrap()
                - 1
        );
        assert!(current_v10::validate_source_capsules(&bad, &capsules[1]).is_err());
        let mut bad = flat_query.clone();
        bad["stages"]["source_capsules"] = query["stages"]["source_capsules"].clone();
        assert!(current_v10::validate_source_capsules(&bad, &flat[1]).is_err());
        bad["stages"]
            .as_object_mut()
            .unwrap()
            .remove("source_capsules");
        assert!(current_v10::validate_source_capsules(&bad, &flat[1]).is_err());
    }

    fn synthetic_v12_rows(current: &[Value]) -> Vec<Value> {
        let mut rows = current.to_vec();
        author_historical_diagnostics(&mut rows);
        rows[0]["schema"] = json!(current_v10::LIBRARY_SCHEDULER_RESULT_SCHEMA);
        for (index, keys) in [
            (
                0,
                "source_capsule_source_sha256 source_layout source_byte_budget max_memory_bytes config_schema source_capsule_verification_policy",
            ),
            (
                1,
                "source_layout source_byte_budget config_schema source_capsule_verification_policy",
            ),
        ] {
            for key in keys.split_whitespace() {
                rows[index].as_object_mut().unwrap().remove(key);
            }
            if rows[index].get("s3_read_retry").is_some() {
                rows[index]["config_schema"] = json!("borsuk-cohere-native-baseline-config-v11");
            }
        }
        for row in &mut rows {
            match row["phase"].as_str().unwrap() {
                "source_binding" => {
                    row.as_object_mut().unwrap().remove("generation_authority");
                }
                "startup" => {
                    row.as_object_mut().unwrap().remove("source_layout");
                }
                "query" => {
                    row["stages"]
                        .as_object_mut()
                        .unwrap()
                        .remove("source_capsules");
                }
                _ => (),
            }
        }
        rows
    }

    // SYNTHETIC historical boundary evidence, never producer interoperability.
    // Construct literal legacy rosters and retain actual native ID/score/payload
    // bytes. Current acceptance is checked separately on the untouched file.
    pub(super) fn synthetic_historical_rows(current: &[Value], schema: &str) -> Vec<Value> {
        assert!(matches!(
            schema,
            "borsuk-cohere-native-baseline-result-v7" | "borsuk-cohere-native-baseline-result-v8"
        ));
        let mut observations = current.to_vec();
        author_historical_diagnostics(&mut observations);
        observations.iter().map(|row| {
            let phase = row["phase"].as_str().unwrap();
            let extras = match phase {
                "identity" => "fetch_parallelism serving execution sq8_range_source_sha256 returned_source_sha256",
                "bound_inputs" => "fetch_parallelism source_cache serving execution selected_count corpus_intervals reserved_query_interval reserved_queries_sha256 cohort_receipt_sha256 derivation_receipt_sha256 producer_authority max_memory_bytes",
                "startup" => "serving",
                "query" => "serving source_nomination_skipped planning_scope plan selected_slot",
                "all_queries_sealed" => "selected_count population_count reserved_query_count",
                "diagnostic_admission" => {
                    use borsuk::sq8_s3_range::Sq8RangeTrace;
                    let mut legacy = row.clone();
                    let traced = legacy["trace"] == true;
                    let ranges = legacy["trace_ranges"].as_u64().unwrap() as usize;
                    let parallel = current[0]["fetch_parallelism"].as_u64().unwrap() as usize;
                    let both = Sq8RangeTrace::pending_futures_bytes(128.max(ranges), parallel);
                    let pinned = both - if traced {
                        Sq8RangeTrace::pending_futures_bytes(ranges, parallel)
                    } else { 0 };
                    for key in ["range_scheduler_owner", "range_state_charged_by_library_bytes",
                        "trace_additional_bytes_charged_by_library"] {
                        legacy.as_object_mut().unwrap().remove(key);
                    }
                    legacy["trace_peak_charged_by_library_at_traced_admission"] = json!(traced);
                    legacy["range_state_pinned_by_runner_bytes"] = json!(pinned);
                    legacy["range_state_both_paths_bytes"] = json!(both);
                    legacy["caller_pinned_bytes"] = json!(legacy["caller_pinned_bytes"].as_u64().unwrap() + pinned as u64);
                    return legacy;
                },
                _ => "",
            };
            let mut names = format!("{} {extras}", v2_row_fields(phase).unwrap());
            if schema.ends_with("v8") && matches!(phase, "identity" | "bound_inputs") {
                names.push_str(" source_get_budget");
            }
            let mut legacy = Value::Object(names.split_whitespace().map(|key| {
                (key.to_owned(), row.get(key).expect("native fixture field").clone())
            }).collect());
            if phase == "identity" {
                legacy["schema"] = json!(schema);
            }
            if phase == "query" {
                legacy["stages"].as_object_mut().unwrap().remove("source_capsules");
                legacy["plan"] = Value::Object("selected_pages ranges planned_bytes target_pages target_shortfall primary_pages_retained covered_pages bridge_pages".split_whitespace().map(|key| {
                    (key.to_owned(), row["plan"][key].clone())
                }).collect());
            }
            legacy
        }).collect()
    }

    #[test]
    fn obsolete_scale_reduction_config_reports_exact_v4_requirement() {
        let dir = tempfile::tempdir().unwrap();
        let rows = v2_fixture();
        let run = dir.path().join("unused-run");
        std::fs::write(&run, encode(&rows)).unwrap();
        let (path, _) = scale_config(dir.path(), &run, &rows);
        let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["schema"] = json!("borsuk-completed-native-reduction-config-v3");
        let bytes = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            reduce_scale(&path, &sha(&bytes)).unwrap_err().to_string(),
            "scale reduction config v4 required"
        );
    }
    #[test]
    fn scale_report_distinguishes_local_reduction_from_authenticated_s3_producer() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = v2_fixture();
        authenticate(&mut rows);
        let (path, pin) = v2_config(dir.path(), &encode(&rows), &rows);
        let (mut config, _): (CompletedConfig, _) = read_config(&path, &pin).unwrap();
        let run = read_run_with(&config.input.path, &config.input.sha256, Some(&config)).unwrap();
        let historical = completed_report(&config, &run).unwrap();
        assert_eq!(historical["local_file_only"], true);
        assert!(historical.get("producer_backend_kind").is_none());
        // Reuse authenticated S3 evidence to check only the new report fields.
        config.schema = SCALE_CONFIG_SCHEMA.into();
        let report = completed_report(&config, &run).unwrap();
        assert_eq!(report["local_file_only"], true);
        assert_eq!(report["reduction_reads_local_files_only"], true);
        assert_eq!(report["producer_backend_kind"], "s3");
        assert_eq!(report["external_resources_and_cost_gate_required"], true);
        assert_eq!(report["qualified"], false);
    }

    #[test]
    fn source_bound_full_population_scale_report_and_frozen_config_mismatches() {
        let dir = tempfile::tempdir().unwrap();
        // Frozen authority is independent of the producer rows constructed below.
        let expected_identity = json!({"phase":"identity","schema":"borsuk-cohere-native-baseline-result-v7",
            "config_sha256":"9".repeat(64),"binary_sha256":"9".repeat(64),
            "runner_source_sha256":"9".repeat(64),"generation_source_sha256":"9".repeat(64),
            "router_source_sha256":"9".repeat(64),"codec_source_sha256":"9".repeat(64),
            "source_plane_source_sha256":"9".repeat(64),"sq8_range_source_sha256":"9".repeat(64),
            "returned_source_sha256":"9".repeat(64),"scope":"AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS",
            "physical_s3_measured":false,"io_measurement":"logical_GET_charges_separate_from_cumulative_process_native_transport",
            "s3_credential_source":"imds_instance_role_only","native_transport_includes":"S3_and_IMDS_credential_requests_including_PUT",
            "wire_bytes":null,"unread_bytes":null,"billed_bytes":null,"billed_requests":null,
            "external_gate_required":true,"truth_opened":false,"serving":{"mode":"baseline"},
            "execution":{"mode":"full"},"fetch_parallelism":16});
        let expected_bound_inputs = json!({"phase":"bound_inputs",
            "dataset":"CohereLabs/wikipedia-2023-11-embed-multilingual-v3",
            "revision":"ade45fb52bd549f5e8c065636fe4160a43c2af36","metric":"cosine","tie_rule":"corpus_ordinal_ascending",
            "rows":100_000,"dimensions":1024,"count":1000,"k":10,"corpus_source_first":0,"query_source_first":100_000,
            "profile":"native100k","generation_prefix":"retained","generation_root_sha256":"a".repeat(64),
            "requests_bytes":4_096_000,"requests_sha256":"c".repeat(64),"truth_bytes":80_000,"truth_sha256":"d".repeat(64),
            "native_source_sha256":"e".repeat(64),"native_sq8_sha256":"f".repeat(64),"native_order_sha256":"b".repeat(64),
            "credential_source":"imds_instance_role_only","backend":{"kind":"s3","bucket":"fixture-bucket","region":"test-region-1",
                "physical_prefix":"fixture/run","sq8_object_key":format!("fixture/objects/{}", "f".repeat(64)),"sq8_etag":"\"fixture-etag\""},
            "corpus_intervals":[{"start":0,"end":100_000}],"reserved_query_interval":{"start":100_000,"end":101_000},
            "reserved_queries_sha256":"c".repeat(64),"serving":{"mode":"baseline"},"source_cache":"off","fetch_parallelism":16,
            "execution":{"mode":"full"},"selected_count":1000,"max_memory_bytes":536_870_912,
            "cohort_receipt_sha256":"1".repeat(64),"derivation_receipt_sha256":"2".repeat(64),
            "producer_authority":{"source_commit":"4".repeat(40),"executable_sha256":"5".repeat(64),
                "producer_source_sha256":"6".repeat(64),"sq8_source_sha256":"7".repeat(64),"source_order_source_sha256":"8".repeat(64)},"truth_opened":false});
        let mut rows = v2_fixture();
        rows[0]["schema"] = json!("borsuk-cohere-native-baseline-result-v7");
        for key in ["sq8_range_source_sha256", "returned_source_sha256"] {
            rows[0][key] = json!("9".repeat(64));
        }
        rows[0]["serving"] = json!({"mode":"baseline"});
        rows[0]["execution"] = json!({"mode":"full"});
        rows[0]["fetch_parallelism"] = json!(16);
        let inputs = &mut rows[1];
        inputs["dataset"] = json!("CohereLabs/wikipedia-2023-11-embed-multilingual-v3");
        inputs["revision"] = json!("ade45fb52bd549f5e8c065636fe4160a43c2af36");
        inputs["rows"] = json!(100_000);
        inputs["dimensions"] = json!(1024);
        inputs["query_source_first"] = json!(100_000);
        inputs["requests_bytes"] = json!(4_096_000);
        inputs["corpus_intervals"] = json!([{"start":0,"end":100_000}]);
        inputs["reserved_query_interval"] = json!({"start":100_000,"end":101_000});
        inputs["reserved_queries_sha256"] = inputs["requests_sha256"].clone();
        inputs["serving"] = json!({"mode":"baseline"});
        inputs["source_cache"] = json!("off");
        inputs["fetch_parallelism"] = json!(16);
        inputs["execution"] = json!({"mode":"full"});
        inputs["selected_count"] = json!(1000);
        inputs["max_memory_bytes"] = json!(512 * 1024 * 1024_u64);
        inputs["cohort_receipt_sha256"] = json!("1".repeat(64));
        inputs["derivation_receipt_sha256"] = json!("2".repeat(64));
        inputs["producer_authority"] = json!({"source_commit":"4".repeat(40),"executable_sha256":"5".repeat(64),
            "producer_source_sha256":"6".repeat(64),"sq8_source_sha256":"7".repeat(64),"source_order_source_sha256":"8".repeat(64)});
        for row in &mut rows {
            match row["phase"].as_str().unwrap() {
                "startup" => row["serving"] = json!({"mode":"baseline"}),
                "query" => {
                    row["serving"] = json!({"mode":"baseline"});
                    row["source_nomination_skipped"] = json!(false);
                    row["planning_scope"] = json!("source_nomination_and_cover");
                    row["plan"] = json!({"selected_pages":[0],"ranges":[[0,100]],
                        "planned_bytes":100,"target_pages":1,"target_shortfall":0,
                        "primary_pages_retained":1,"covered_pages":1,"bridge_pages":0});
                }
                "all_queries_sealed" => {
                    row["selected_count"] = json!(1000);
                    row["population_count"] = json!(1000);
                    row["reserved_query_count"] = json!(1000);
                }
                "terminal" => {
                    let summary = &mut row["summary"];
                    summary["serving"] = json!({"mode":"baseline"});
                    summary["direct_memory"] = Value::Null;
                    summary["execution"] = json!({"mode":"full"});
                    for key in [
                        "selected_count",
                        "executed_count",
                        "population_count",
                        "reserved_query_count",
                    ] {
                        summary[key] = json!(1000);
                    }
                    summary["diagnostic_panel"] = json!(false);
                    summary["diagnostic_prefix"] = json!(false);
                    summary["population_percentiles_valid"] = json!(true);
                    summary["full_cohort_qualification"] = json!(false);
                }
                _ => (),
            }
        }
        let (result, _) = write_fixture(dir.path(), "scale-full-population.jsonl", rows.clone());
        let body = std::fs::read(&result).unwrap();
        let config = dir.path().join("scale-reduction.json");
        let frozen_config = json!({"schema":SCALE_CONFIG_SCHEMA,
            "input":{"path":result,"bytes":body.len(),"sha256":sha(&body)},
            "expected_identity":expected_identity,"expected_bound_inputs":expected_bound_inputs});
        let config_bytes = serde_json::to_vec(&frozen_config).unwrap();
        std::fs::write(&config, &config_bytes).unwrap();
        let pin = sha(&config_bytes);
        let report = reduce_scale(&config, &pin).unwrap();
        assert_eq!(report["population_percentiles_valid"], true);
        assert_eq!(report["diagnostic_only"], false);
        assert!(!report["statistics"].is_null() && !report["stage_statistics"].is_null());
        assert_eq!(report["statistics"]["p99_ms"], 990.0);
        assert_eq!(report["native_terminal"]["queries"], 1000);
        assert_eq!(report["producer_backend_kind"], "s3");
        assert_eq!(report["cold_s3_claim"], false);
        assert_eq!(report["qualified"], false);
        let frozen: Value = serde_json::from_slice(&std::fs::read(&config).unwrap()).unwrap();
        for key in ["derivation_receipt_sha256", "backend", "producer_authority"] {
            let mut mismatched = frozen.clone();
            match key {
                "backend" => {
                    mismatched["expected_bound_inputs"][key] =
                        json!({"kind":"local","store_root":"/synthetic/store"})
                }
                "producer_authority" => {
                    mismatched["expected_bound_inputs"][key]["executable_sha256"] =
                        json!("0".repeat(64))
                }
                _ => mismatched["expected_bound_inputs"][key] = json!("8".repeat(64)),
            }
            let bytes = serde_json::to_vec(&mismatched).unwrap();
            std::fs::write(&config, &bytes).unwrap();
            let error = reduce_scale(&config, &sha(&bytes)).unwrap_err().to_string();
            assert_eq!(error, "expected v2 input/root/backend pins", "{key}");
            assert_eq!(
                std::fs::read(&result).unwrap(),
                body,
                "run untouched: {key}"
            );
        }
    }

    #[test]
    fn explicit_million_population_geometry_and_allocation_bounds() {
        let rows = v2_fixture();
        let mut raw = rows[1].clone();
        raw["dataset"] = json!("CohereLabs/wikipedia-2023-11-embed-multilingual-v3");
        raw["revision"] = json!("ade45fb52bd549f5e8c065636fe4160a43c2af36");
        raw["rows"] = json!(1_000_000);
        raw["dimensions"] = json!(1024);
        raw["profile"] = json!("scale1m");
        raw["query_source_first"] = json!(100_000);
        raw["requests_bytes"] = json!(4_096_000);
        raw["truth_bytes"] = json!(80_000);
        raw["corpus_intervals"] =
            json!([{"start":0,"end":100_000},{"start":101_000,"end":1_001_000}]);
        raw["reserved_query_interval"] = json!({"start":100_000,"end":101_000});
        raw["reserved_queries_sha256"] = raw["requests_sha256"].clone();
        raw["serving"] = json!({"mode":"baseline"});
        raw["source_cache"] = json!("off");
        raw["execution"] = json!({"mode":"full"});
        raw["selected_count"] = json!(1000);
        raw["max_memory_bytes"] = json!(512 * 1024 * 1024);
        raw["cohort_receipt_sha256"] = json!("1".repeat(64));
        raw["derivation_receipt_sha256"] = json!("2".repeat(64));
        raw["producer_authority"] = json!({"source_commit":"4".repeat(40),"executable_sha256":"5".repeat(64),
            "producer_source_sha256":"6".repeat(64),"sq8_source_sha256":"7".repeat(64),"source_order_source_sha256":"8".repeat(64)});
        let inputs: Inputs = serde_json::from_value(raw.clone()).unwrap();
        validate_scale_inputs(&inputs, &raw).unwrap();
        for count in [1, 32, 1000] {
            let mut prefix = raw.clone();
            prefix["count"] = json!(count);
            prefix["selected_count"] = json!(count);
            prefix["requests_bytes"] = json!(count * 1024 * 4);
            prefix["truth_bytes"] = json!(count * 10 * 8);
            validate_scale_inputs(
                &serde_json::from_value::<Inputs>(prefix.clone()).unwrap(),
                &prefix,
            )
            .unwrap();
        }
        let mut wrong_digest = raw.clone();
        wrong_digest["reserved_queries_sha256"] = json!("8".repeat(64));
        assert_eq!(
            validate_scale_inputs(&inputs, &wrong_digest)
                .unwrap_err()
                .to_string(),
            "full selected request/reserved query digest binding"
        );
        assert!(scale_ordinals(&raw, usize::MAX).is_err());
        for (key, value) in [
            ("rows", json!(1_000_001)),
            ("profile", json!("native100k")),
            ("query_source_first", json!(usize::MAX)),
            ("count", json!(usize::MAX)),
            ("selected_count", json!(32)),
            (
                "reserved_query_interval",
                json!({"start":100_000,"end":100_032}),
            ),
            ("corpus_intervals", json!([{"start":0,"end":1_000_000}])),
        ] {
            let mut bad = raw.clone();
            bad[key] = value;
            assert!(
                validate_scale_inputs(
                    &serde_json::from_value::<Inputs>(bad.clone()).unwrap(),
                    &bad
                )
                .is_err(),
                "{key}"
            );
        }
    }

    #[test]
    fn page_range_v2_headers_bind_retry_policy_and_preserve_v10_v11_v12_completion() {
        let (dir, result) =
            scale_native_runner::scale_reducer_native_fixture(false, false).unwrap();
        let original = assert_current_native(dir.path(), &result);
        let retry_result =
            scale_native_runner::scale_reducer_retry_header_fixture(dir.path()).unwrap();
        let headers: Vec<Value> = std::fs::read_to_string(&retry_result)
            .unwrap()
            .lines()
            .take(2)
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(headers[0]["schema"], current_v10::FRAMED_RESULT_SCHEMA);
        let config_bytes = std::fs::read(dir.path().join("config.json")).unwrap();
        assert_eq!(headers[0]["config_sha256"], sha(&config_bytes));
        for key in [
            "binary_sha256",
            "runner_source_sha256",
            "generation_source_sha256",
            "router_source_sha256",
            "codec_source_sha256",
            "source_plane_source_sha256",
            "sq8_range_source_sha256",
            "returned_source_sha256",
        ] {
            assert_eq!(headers[0][key], original[0][key], "{key}");
        }

        let header_pin = CompletedConfig {
            schema: SCALE_CONFIG_SCHEMA.into(),
            input: CompletedInput {
                path: retry_result.clone(),
                bytes: std::fs::metadata(&retry_result).unwrap().len(),
                sha256: sha(&std::fs::read(&retry_result).unwrap()),
            },
            expected_identity: headers[0].clone(),
            expected_bound_inputs: headers[1].clone(),
        };
        current_v10::validate_completed(&header_pin).unwrap();
        // The historical S3 body below is synthetic; v13 raw-manifest proofs use native fixtures.
        let mut rows = original.clone();
        rows[..2].clone_from_slice(&headers[..2]);
        let transport_rows = v2_fixture();
        for row in &mut rows {
            if matches!(
                row["phase"].as_str(),
                Some("source_binding" | "generation_open" | "query")
            ) {
                let transport = transport_rows
                    .iter()
                    .find(|r| r["phase"] == row["phase"] && r["ordinal"] == row["ordinal"])
                    .unwrap();
                row["transport"] = transport["transport"].clone();
                if row["phase"] == "source_binding" {
                    row["charges"] = transport["charges"].clone();
                }
            }
        }
        let binding = rows
            .iter()
            .find(|r| r["phase"] == "source_binding")
            .unwrap()["charges"]
            .clone();
        let last: TransportSpan = serde_json::from_value(
            rows.iter().rev().find(|r| r["phase"] == "query").unwrap()["transport"].clone(),
        )
        .unwrap();
        let terminal = &mut rows.last_mut().unwrap()["summary"];
        terminal["binding_charge"] = binding;
        terminal["transport_last_boundary"] = last.compact();
        let rows = synthetic_v12_rows(&rows);
        let (result, _) = write_fixture(dir.path(), "synthetic-v11-completion.jsonl", rows.clone());
        let (config, pin) = scale_config(dir.path(), &result, &rows);
        let (pinned, _): (CompletedConfig, _) = read_config(&config, &pin).unwrap();
        current_v10::validate_completed(&pinned).unwrap();
        let run = read_run_with(&result, &pinned.input.sha256, Some(&pinned)).unwrap();
        assert_eq!(
            run.identity.schema,
            current_v10::LIBRARY_SCHEDULER_RESULT_SCHEMA
        );
        assert_eq!(run.samples.len(), 2);

        // One failed S3 response followed by a successful 206 adds one physical
        // submission, not a failed logical GET; process totals also include IMDS.
        let mut recovered = rows.clone();
        let mut started = false;
        for row in &mut recovered {
            if row["phase"] != "query" {
                continue;
            }
            for boundary in ["before", "after"] {
                if started || boundary == "after" {
                    let stats = &mut row["transport"][boundary];
                    let attempts = stats["attempts"].as_u64().unwrap();
                    stats["attempts"] = json!(attempts + 1);
                    stats["method_counts"][0] =
                        json!(stats["method_counts"][0].as_u64().unwrap() + 1);
                    stats["status_counts"] = json!([[200, attempts - 1], [206, 1], [500, 1]]);
                    stats["dropped_error_bodies"] = json!(1);
                }
            }
            started = true;
        }
        let last: TransportSpan = serde_json::from_value(
            recovered
                .iter()
                .rev()
                .find(|r| r["phase"] == "query")
                .unwrap()["transport"]
                .clone(),
        )
        .unwrap();
        recovered.last_mut().unwrap()["summary"]["transport_last_boundary"] = last.compact();
        let (path, _) = write_fixture(dir.path(), "v11-recovered-500.jsonl", recovered.clone());
        let (config, pin) = scale_config(dir.path(), &path, &recovered);
        let report = reduce_scale(&config, &pin).unwrap();
        assert_eq!(report["native_terminal"]["status"], "MEASURED");
        assert_eq!(report["native_terminal"]["sum"]["failed_gets"], 0);
        assert_eq!(report["identity"], pinned.expected_identity);
        assert_eq!(report["inputs"], pinned.expected_bound_inputs);
        let query = recovered
            .iter()
            .position(|r| r["phase"] == "query")
            .unwrap();
        assert_eq!(
            recovered[query]["transport"]["after"]["dropped_error_bodies"],
            1
        );
        assert_eq!(
            recovered[query]["transport"]["after"]["method_counts"][2],
            1
        );
        assert_eq!(recovered[query]["charges"], rows[query]["charges"]);
        for (pointer, value) in [
            ("/transport/after/attempts", json!(999)),
            ("/transport/after/dropped_error_bodies", json!(0)),
            ("/transport/after/status_counts/2/1", json!(2)),
            ("/transport/after/stream_failures", json!(999)),
        ] {
            let mut bad = recovered.clone();
            *bad[query].pointer_mut(pointer).unwrap() = value;
            let (path, _) =
                write_fixture(dir.path(), "v11-impossible-transport.jsonl", bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            let error = reduce_scale(&config, &pin).unwrap_err().to_string();
            assert_eq!(error, "cumulative SDK counter consistency", "{pointer}");
        }

        // Every policy key/value is strict, even if both frozen headers agree on a bad value.
        let policy = headers[0]["s3_read_retry"].as_object().unwrap();
        assert_eq!(policy, headers[1]["s3_read_retry"].as_object().unwrap());
        for key in policy.keys() {
            for mutation in ["missing", "wrong"] {
                for changed_headers in [&[0][..], &[1][..], &[0, 1][..]] {
                    let mut bad = rows.clone();
                    for &index in changed_headers {
                        if mutation == "missing" {
                            bad[index]["s3_read_retry"]
                                .as_object_mut()
                                .unwrap()
                                .remove(key);
                        } else {
                            bad[index]["s3_read_retry"][key] = match &policy[key] {
                                Value::Bool(value) => json!(!*value),
                                Value::Number(value) => json!(value.as_u64().unwrap() + 1),
                                Value::String(_) => json!("changed"),
                                Value::Array(_) => json!(["GET"]),
                                _ => unreachable!(),
                            };
                        }
                    }
                    let mut invalid = serde_json::to_value(&pinned).unwrap();
                    invalid["expected_identity"] = bad[0].clone();
                    invalid["expected_bound_inputs"] = bad[1].clone();
                    let invalid: CompletedConfig = serde_json::from_value(invalid).unwrap();
                    assert!(
                        current_v10::validate_completed(&invalid).is_err(),
                        "{key} {mutation} {changed_headers:?}"
                    );
                }
            }
        }
        for index in 0..2 {
            for key in ["config_schema", "s3_read_retry"] {
                let mut invalid = serde_json::to_value(&pinned).unwrap();
                let header = if index == 0 {
                    "expected_identity"
                } else {
                    "expected_bound_inputs"
                };
                invalid[header].as_object_mut().unwrap().remove(key);
                assert!(
                    current_v10::validate_completed(&serde_json::from_value(invalid).unwrap())
                        .is_err()
                );
            }
            let mut invalid = serde_json::to_value(&pinned).unwrap();
            let header = if index == 0 {
                "expected_identity"
            } else {
                "expected_bound_inputs"
            };
            invalid[header]["s3_read_retry"]["extra"] = json!(true);
            assert!(
                current_v10::validate_completed(&serde_json::from_value(invalid).unwrap()).is_err()
            );
        }
        for (pointer, value) in [
            (
                "/expected_identity/config_schema",
                json!("borsuk-cohere-native-baseline-config-v10"),
            ),
            (
                "/expected_bound_inputs/config_schema",
                json!("borsuk-cohere-native-baseline-config-v10"),
            ),
            (
                "/expected_bound_inputs/backend",
                original[1]["backend"].clone(),
            ),
            (
                "/expected_identity/schema",
                json!(current_v10::RESULT_SCHEMA),
            ),
        ] {
            let mut invalid = serde_json::to_value(&pinned).unwrap();
            *invalid.pointer_mut(pointer).unwrap() = value;
            assert!(
                current_v10::validate_completed(&serde_json::from_value(invalid).unwrap()).is_err(),
                "{pointer}"
            );
        }
        // Valid v10 rows still refuse either unexpected policy field, including null.
        for index in 0..2 {
            for key in ["config_schema", "s3_read_retry"] {
                let mut bad = original.clone();
                for value in [rows[index][key].clone(), Value::Null] {
                    bad[index][key] = value;
                    let mut invalid = serde_json::to_value(&pinned).unwrap();
                    invalid["expected_identity"] = bad[0].clone();
                    invalid["expected_bound_inputs"] = bad[1].clone();
                    assert!(
                        current_v10::validate_completed(&serde_json::from_value(invalid).unwrap())
                            .is_err()
                    );
                }
            }
        }

        // Re-sealing a changed result never changes independently pinned header authority.
        for (index, key) in [
            (0, "config_sha256"),
            (0, "binary_sha256"),
            (0, "runner_source_sha256"),
            (0, "generation_source_sha256"),
            (0, "router_source_sha256"),
            (0, "codec_source_sha256"),
            (0, "source_plane_source_sha256"),
            (0, "sq8_range_source_sha256"),
            (0, "returned_source_sha256"),
            (1, "native_source_sha256"),
            (1, "native_sq8_sha256"),
            (1, "native_order_sha256"),
        ] {
            let mut bad = rows.clone();
            bad[index][key] = json!("0".repeat(64));
            let (path, _) = write_fixture(dir.path(), "v11-changed-identity.jsonl", bad);
            let body = std::fs::read(&path).unwrap();
            let mut independent = serde_json::to_value(&pinned).unwrap();
            independent["input"] = json!({"path":path,"bytes":body.len(),"sha256":sha(&body)});
            let independent: CompletedConfig = serde_json::from_value(independent).unwrap();
            let error = read_run_with(&path, &independent.input.sha256, Some(&independent))
                .err()
                .unwrap()
                .to_string();
            assert_eq!(
                error,
                if index == 0 {
                    "expected v2 identity pins"
                } else {
                    "expected v2 input/root/backend pins"
                },
                "{key}"
            );
        }
        let query = rows.iter().position(|r| r["phase"] == "query").unwrap();
        let startup = rows.iter().position(|r| r["phase"] == "startup").unwrap();
        let historical = synthetic_v12_rows(&rows);
        let (path, _) = write_fixture(
            dir.path(),
            "synthetic-v12-completion.jsonl",
            historical.clone(),
        );
        let (config, pin) = scale_config(dir.path(), &path, &historical);
        reduce_scale(&config, &pin).unwrap();
        // c3b0e81 full runs pin caller buffers plus pending(max(128, SQ8 cap), width).
        let scheduler = borsuk::sq8_s3_range::Sq8RangeTrace::pending_futures_bytes(
            128.max(rows[1]["sq8_get_budget"].as_u64().unwrap() as usize),
            rows[1]["fetch_parallelism"].as_u64().unwrap() as usize,
        ) as u64;
        for schema in [
            "borsuk-cohere-native-baseline-result-v9",
            current_v10::RESULT_SCHEMA,
            current_v10::READ_RETRY_RESULT_SCHEMA,
        ] {
            let v9 = schema == "borsuk-cohere-native-baseline-result-v9";
            let mut historical = synthetic_v12_rows(&rows);
            historical[0]["schema"] = json!(schema);
            for header in &mut historical[..2] {
                let header = header.as_object_mut().unwrap();
                header.remove("range_scheduler_owner");
                if schema != current_v10::READ_RETRY_RESULT_SCHEMA {
                    header.remove("config_schema");
                    header.remove("s3_read_retry");
                }
                if v9 {
                    header.remove("sq8_range_split");
                }
            }
            historical[startup]
                .as_object_mut()
                .unwrap()
                .remove("range_state_charged_by_library_bytes");
            historical[startup]["caller_pinned_bytes"] =
                json!(rows[startup]["caller_pinned_bytes"].as_u64().unwrap() + scheduler);
            if v9 {
                for row in historical.iter_mut().filter(|r| r["phase"] == "query") {
                    row.as_object_mut()
                        .unwrap()
                        .remove("returned_physical_ordinals");
                    row["plan"]
                        .as_object_mut()
                        .unwrap()
                        .remove("logical_range_count");
                }
            }
            let (path, _) =
                write_fixture(dir.path(), "v10-v11-plan-parity.jsonl", historical.clone());
            let (config, pin) = scale_config(dir.path(), &path, &historical);
            let (pinned, _): (CompletedConfig, _) = read_config(&config, &pin).unwrap();
            let valid = read_run_with(&path, &pinned.input.sha256, Some(&pinned)).unwrap();
            if !v9 {
                assert_eq!(
                    valid.samples[0].physical_plan_sha256,
                    run.samples[0].physical_plan_sha256
                );
            }
            assert!(valid.samples[0].charges == run.samples[0].charges);
            assert!(valid.samples[0].returned == run.samples[0].returned);

            let cap = historical[startup]["library_cap_bytes"].as_u64().unwrap();
            let scratch = historical[startup]["query_scratch_bytes"].as_u64().unwrap();
            for (key, value, error) in [
                (
                    "caller_pinned_bytes",
                    0,
                    "historical full-run scheduler reservation",
                ),
                (
                    "caller_pinned_bytes",
                    scheduler - 1,
                    "historical full-run scheduler reservation",
                ),
                (
                    "caller_pinned_bytes",
                    cap - scratch + 1,
                    "split startup memory/config conservation",
                ),
                (
                    "codec_scratch_bytes",
                    0,
                    "split startup memory/config conservation",
                ),
                (
                    "trace_scratch_bytes",
                    0,
                    "split startup memory/config conservation",
                ),
                (
                    "query_scratch_bytes",
                    scratch + 1,
                    "split startup memory/config conservation",
                ),
                (
                    "library_cap_bytes",
                    cap - 1,
                    "split startup memory/config conservation",
                ),
            ] {
                let mut bad = historical.clone();
                bad[startup][key] = json!(value);
                let (path, _) = write_fixture(dir.path(), "historical-startup-charge.jsonl", bad);
                let body = std::fs::read(&path).unwrap();
                // Re-seal the changed body and repin only its descriptor, preserving header authority.
                let mut independent = serde_json::to_value(&pinned).unwrap();
                independent["input"] = json!({"path":path,"bytes":body.len(),"sha256":sha(&body)});
                let independent: CompletedConfig = serde_json::from_value(independent).unwrap();
                assert_eq!(
                    read_run_with(&path, &independent.input.sha256, Some(&independent))
                        .err()
                        .unwrap()
                        .to_string(),
                    error,
                    "{schema} {key}={value}"
                );
            }
            let mut bad = historical.clone();
            bad[startup]["range_state_charged_by_library_bytes"] = json!(scheduler);
            let (path, _) =
                write_fixture(dir.path(), "historical-library-owner.jsonl", bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            assert!(reduce_scale(&config, &pin).is_err(), "{schema}");

            if !v9 {
                historical[query]["plan"]["logical_range_count"] = json!(2);
                let (path, _) = write_fixture(
                    dir.path(),
                    "v10-v11-wrong-logical-count.jsonl",
                    historical.clone(),
                );
                let (config, pin) = scale_config(dir.path(), &path, &historical);
                let error = reduce_scale(&config, &pin).unwrap_err().to_string();
                assert_eq!(error, "v10 logical range count", "{schema}");
            }
        }
    }

    #[test]
    fn fp32_direct_native_producer_reader_and_resealed_refusals() {
        let (dir, result) = scale_native_runner::fp32_reducer_native_fixture_selection(false, Some(64), true).unwrap();
        let rows = std::fs::read_to_string(&result).unwrap().lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
        let (config, pin) = scale_config(dir.path(), &result, &rows);
        fp32_serving::reduce(&config, &pin).unwrap();
        let q = rows.iter().position(|r| r["phase"] == "query").unwrap();
        assert_eq!(rows[q]["source_nomination_skipped"], true);
        assert_eq!(rows[q]["charges"]["source"]["submitted_gets"], 0);
        assert_eq!(rows[q]["canonical_stage"]["shortlist"].as_array().unwrap().len(), 16);
        let startup = rows.iter().position(|r| r["phase"] == "startup").unwrap();
        for (case, index, pointer, value) in [
            ("source", q, "/charges/source/submitted_gets", json!(1)),
            ("roster", q, "/plan/selected_pages/0", json!(999)),
            ("cover", q, "/plan/ranges/0/1", json!(1)),
            ("skip", q, "/source_nomination_skipped", json!(false)),
            ("model", startup, "/fp32_rerank/modeled_memory_bytes", json!(1)),
            ("source-stage", q, "/stages/source/start_ns", json!(1)),
            ("source-duration", q, "/stages/source/end_ns", json!(1)),
            ("selector", q, "/serving/direct/max_sq8_gets", json!(1)),
            ("nominee", q, "/canonical_stage/shortlist/0/ordinal", json!(999)),
        ] {
            let mut bad = rows.clone();
            *bad[index].pointer_mut(pointer).unwrap() = value;
            let (path, _) = write_fixture(dir.path(), &format!("direct-fp32-{case}.jsonl"), bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            let output = dir.path().join(format!("direct-fp32-{case}.json"));
            assert!(!execute_report(&output, fp32_serving::REPORT_SCHEMA,
                || fp32_serving::reduce(&config, &pin)).unwrap(), "{case}");
            let written: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
            assert_eq!(written["status"], "INVALID");
        }
    }

    #[test]
    fn fp32_direct_leaf_caps_and_complete_coalesced_shortlist() {
        for (leaf, caps) in [(None, None), (Some(40), Some((32,96))), (Some(64), Some((256,128)))] {
            let (dir, result) = scale_native_runner::fp32_reducer_native_fixture_caps(true, leaf, true, caps).unwrap();
            let rows = std::fs::read_to_string(&result).unwrap().lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
            let (config, pin) = scale_config(dir.path(), &result, &rows);
            fp32_serving::reduce(&config, &pin).unwrap();
            let q = rows.iter().position(|r| r["phase"] == "query").unwrap();
            for case in ["leaf-bound", "leaf-duplicate", "unit-capacity", "seed", "shortlist"] {
                let mut bad = rows.clone();
                match case {
                    "leaf-bound" => bad[q]["trace"]["semantic_leaves"][0] = json!(u64::MAX),
                    "leaf-duplicate" => {
                        let id = bad[q]["trace"]["semantic_leaves"][0].clone();
                        bad[q]["trace"]["semantic_leaves"].as_array_mut().unwrap().push(id);
                    }
                    "unit-capacity" => bad[q]["trace"]["semantic_units"] = json!(vec![0;65]),
                    "seed" => bad[q]["trace"]["semantic_seed_additions"] = json!([0]),
                    "shortlist" => {
                        let list = bad[q]["canonical_stage"]["shortlist"].as_array().unwrap();
                        let first = list.iter().map(|r| r["ordinal"].as_u64().unwrap()).min().unwrap();
                        let last = list.iter().map(|r| r["ordinal"].as_u64().unwrap()).max().unwrap();
                        let remove = list.iter().position(|r| r["ordinal"] != first && r["ordinal"] != last
                            && !bad[q]["returned"].as_array().unwrap().iter().any(|h| h["id"] == r["id"])).unwrap();
                        bad[q]["canonical_stage"]["shortlist"].as_array_mut().unwrap().remove(remove);
                        let bridge = bad[q]["canonical_stage"]["bridge_records"].as_u64().unwrap();
                        bad[q]["canonical_stage"]["bridge_records"] = json!(bridge+1);
                        assert_eq!(bad[q]["canonical_stage"]["ranges"], rows[q]["canonical_stage"]["ranges"]);
                        assert_eq!(bad[q]["charges"], rows[q]["charges"]);
                    }
                    _ => unreachable!(),
                }
                let (path, _) = write_fixture(dir.path(), &format!("direct-leaf-{case}.jsonl"), bad.clone());
                let (config, pin) = scale_config(dir.path(), &path, &bad);
                let output = dir.path().join(format!("direct-leaf-{case}.json"));
                assert!(!execute_report(&output, fp32_serving::REPORT_SCHEMA,
                    || fp32_serving::reduce(&config, &pin)).unwrap(), "{leaf:?} {case}");
                let written: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
                assert_eq!(written["status"], "INVALID");
            }
        }
    }

    #[test]
    fn fp32_coalescing_native_producer_strict_reader_and_actual_writer_refusals() {
        let (dir, result) = scale_native_runner::fp32_reducer_native_fixture_with(true, None).unwrap();
        let rows = std::fs::read_to_string(&result)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let (config, pin) = scale_config(dir.path(), &result, &rows);
        let report = fp32_serving::reduce(&config, &pin).unwrap();
        let output = dir.path().join("coalesced-writer-success.json");
        assert!(
            execute_report(&output, fp32_serving::REPORT_SCHEMA, || {
                fp32_serving::reduce(&config, &pin)
            })
            .unwrap()
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(output).unwrap()).unwrap(),
            report
        );
        let q = rows.iter().position(|r| r["phase"] == "query").unwrap();
        assert!(
            rows[q]["charges"]["canonical"]["submitted_gets"]
                .as_u64()
                .unwrap()
                < 16
        );
        assert!(
            rows[q]["canonical_stage"]["bridge_records"]
                .as_u64()
                .unwrap()
                > 0
        );
        let startup = rows.iter().position(|r| r["phase"] == "startup").unwrap();
        assert_eq!(
            rows[0]["schema"],
            "borsuk-cohere-native-baseline-result-v21"
        );
        for case in [
            "old-version",
            "missing-ranges",
            "missing-covered",
            "missing-bridges",
        ] {
            let mut bad = rows.clone();
            if case == "old-version" {
                bad[0]["schema"] = json!("borsuk-cohere-native-baseline-result-v20");
            } else {
                let field = match case {
                    "missing-ranges" => "ranges",
                    "missing-covered" => "covered_records",
                    _ => "bridge_records",
                };
                bad[q]["canonical_stage"]
                    .as_object_mut()
                    .unwrap()
                    .remove(field);
            }
            let (path, _) = write_fixture(
                dir.path(),
                &format!("coalesced-schema-{case}.jsonl"),
                bad.clone(),
            );
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            let output = dir
                .path()
                .join(format!("coalesced-schema-refusal-{case}.json"));
            assert!(
                !execute_report(&output, fp32_serving::REPORT_SCHEMA, || {
                    fp32_serving::reduce(&config, &pin)
                })
                .unwrap()
            );
            let written: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
            assert_eq!(written["status"], "INVALID");
            assert_eq!(written["complete"], false);
        }
        for (case, index, pointer) in [
            ("range", q, "/canonical_stage/ranges/0/1"),
            ("bridge", q, "/canonical_stage/bridge_records"),
            ("covered", q, "/canonical_stage/covered_records"),
            ("bytes", q, "/charges/canonical/verified_bytes"),
            ("gets", q, "/charges/canonical/submitted_gets"),
            ("shortlist", q, "/canonical_stage/shortlist/0/ordinal"),
            ("model", startup, "/fp32_rerank/modeled_memory_bytes"),
        ] {
            let mut bad = rows.clone();
            *bad[index].pointer_mut(pointer).unwrap() = json!(u64::MAX);
            let (path, _) =
                write_fixture(dir.path(), &format!("coalesced-{case}.jsonl"), bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            let output = dir.path().join(format!("coalesced-refused-{case}.json"));
            assert!(
                !execute_report(&output, fp32_serving::REPORT_SCHEMA, || {
                    fp32_serving::reduce(&config, &pin)
                })
                .unwrap(),
                "{case}"
            );
            let written: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
            assert_eq!(written["status"], "INVALID", "{case}");
            assert_eq!(written["complete"], false, "{case}");
        }
    }

    #[test]
    fn coverage64_actual_runner_resealed_accounting_refusals() {
        let (dir, result) = scale_native_runner::fp32_reducer_native_fixture_with(false, Some(64)).unwrap();
        let rows = std::fs::read_to_string(&result).unwrap().lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
        let (config, pin) = scale_config(dir.path(), &result, &rows);
        fp32_serving::reduce(&config, &pin).unwrap();
        let startup = rows.iter().position(|r| r["phase"] == "startup").unwrap();
        let query = rows.iter().position(|r| r["phase"] == "query").unwrap();
        let admission = rows.iter().position(|r| r["phase"] == "diagnostic_admission").unwrap();
        assert_eq!(rows[admission]["panel_line_cap_bytes"], 262144);
        assert_eq!(rows[admission]["diagnostic_pinned_bytes"], 524288);
        for (case, index, pointer) in [
            ("model", startup, "/fp32_rerank/modeled_memory_bytes"),
            ("panel-cap", admission, "/panel_line_cap_bytes"),
            ("panel-pins", admission, "/diagnostic_pinned_bytes"),
            ("trace-reserve", startup, "/trace_scratch_bytes"),
            ("source-bytes", query, "/charges/source/verified_bytes"),
            ("canonical-bytes", query, "/charges/canonical/verified_bytes"),
        ] {
            let mut bad = rows.clone();
            let old = bad[index].pointer(pointer).unwrap().as_u64().unwrap();
            *bad[index].pointer_mut(pointer).unwrap() = json!(old - 1);
            let (path, _) = write_fixture(dir.path(), &format!("coverage64-{case}.jsonl"), bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            assert!(fp32_serving::reduce(&config, &pin).is_err(), "{case}");
        }
        let binding = rows.iter().find(|r| r["phase"] == "source_binding").unwrap();
        let root: Value = serde_json::from_str(binding["generation_authority"]["root_manifest"].as_str().unwrap()).unwrap();
        let leaf_count = (root["discovery"]["root_bytes"].as_u64().unwrap() - 512) / (64 + 4 * 1024);
        assert_eq!(rows[query]["trace"]["semantic_leaves"].as_array().unwrap().len() as u64, 64.min(leaf_count));
        for case in ["impossible-leaf", "missing-leaf"] {
            let mut bad = rows.clone();
            if case == "impossible-leaf" { bad[query]["trace"]["semantic_leaves"][0] = json!(leaf_count); }
            else { bad[query]["trace"]["semantic_leaves"].as_array_mut().unwrap().pop(); }
            let (path, _) = write_fixture(dir.path(), &format!("coverage64-{case}.jsonl"), bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            assert!(fp32_serving::reduce(&config, &pin).is_err(), "{case}");
        }
        let mut bad = rows.clone();
        bad[query]["trace"]["nomination_evaluated_units"] = json!([0]);
        let (path, _) = write_fixture(dir.path(), "coverage64-incomplete.jsonl", bad.clone());
        let (config, pin) = scale_config(dir.path(), &path, &bad);
        assert!(fp32_serving::reduce(&config, &pin).is_err());
    }

    #[test]
    fn fp32_actual_native_runner_strict_replay_writer_and_falsifiers() {
        let (dir, result) = scale_native_runner::fp32_reducer_native_fixture().unwrap();
        let body = std::fs::read_to_string(&result).unwrap();
        let rows = body
            .lines()
            .map(|l| serde_json::from_str::<Value>(l).unwrap())
            .collect::<Vec<_>>();
        let (config, pin) = scale_config(dir.path(), &result, &rows);
        let report = fp32_serving::reduce(&config, &pin).unwrap();
        assert_eq!(report["queries"], 2);
        assert_eq!(report["score_precision"], "f64");
        let output = dir.path().join("fp32-writer-success.json");
        assert!(execute_report(&output, fp32_serving::REPORT_SCHEMA, || {
            fp32_serving::reduce(&config, &pin)
        }).unwrap());
        let written: Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
        assert_eq!(written, report);
        for (case, field, value) in [
            ("valid-false", "valid", json!(false)),
            ("wrong-schema", "schema", json!("unexpected-report-v1")),
            ("invalid-status", "status", json!("INVALID")),
            ("valid-wrong-type", "valid", json!("true")),
        ] {
            let mut invalid = report.clone();
            invalid[field] = value;
            let output = dir.path().join(format!("fp32-writer-{case}.json"));
            assert!(!execute_report(&output, fp32_serving::REPORT_SCHEMA, || {
                Ok(invalid.clone())
            }).unwrap(), "{case}");
            let written: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
            assert_eq!(written, invalid, "{case}");
        }
        let output = dir.path().join("fp32-writer-reducer-error.json");
        assert!(!execute_report(&output, fp32_serving::REPORT_SCHEMA, || {
            Err("injected FP32 reducer refusal".into())
        }).unwrap());
        let invalid: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(invalid["schema"], fp32_serving::REPORT_SCHEMA);
        assert_eq!(invalid["status"], "INVALID");
        assert_eq!(invalid["complete"], false);
        assert_eq!(invalid["error"], "injected FP32 reducer refusal");
        for selector in [
            json!({"mode":"unknown"}),
            json!({"mode":"fp32_rerank","shortlist":9,
            "max_fp32_bytes":65664,"max_fp32_gets":16,"max_parallel_fp32_gets":3}),
        ] {
            let mut bad = rows.clone();
            bad[0]["serving"] = selector.clone();
            bad[1]["serving"] = selector;
            let (path, _) = write_fixture(dir.path(), "fp32-selector-falsifier.jsonl", bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            assert!(fp32_serving::reduce(&config, &pin).is_err());
        }
        let query = rows.iter().position(|r| r["phase"] == "query").unwrap();
        for pointer in [
            "/charges/canonical/verified_bytes",
            "/canonical_stage/record_bytes",
            "/canonical_stage/shortlist/0/id",
            "/returned/0/score_bits",
        ] {
            let mut bad = rows.clone();
            *bad[query].pointer_mut(pointer).unwrap() = json!(u64::MAX);
            let (path, _) = write_fixture(dir.path(), "fp32-falsifier.jsonl", bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            assert!(fp32_serving::reduce(&config, &pin).is_err(), "{pointer}");
        }
        let mut bad = rows.clone();
        let startup = bad.iter().position(|r| r["phase"] == "startup").unwrap();
        bad[startup]["fp32_rerank"]["open"]["row_digest_bytes"] = json!(0);
        let (path, _) = write_fixture(dir.path(), "fp32-startup-falsifier.jsonl", bad.clone());
        let (config, pin) = scale_config(dir.path(), &path, &bad);
        assert!(fp32_serving::reduce(&config, &pin).is_err());
        let mut bad = rows.clone();
        let model = bad[startup]["fp32_rerank"]["modeled_memory_bytes"].as_u64().unwrap();
        bad[startup]["fp32_rerank"]["modeled_memory_bytes"] = json!(model - 1);
        let (path, _) = write_fixture(dir.path(), "fp32-model-falsifier.jsonl", bad.clone());
        let (config, pin) = scale_config(dir.path(), &path, &bad);
        assert!(fp32_serving::reduce(&config, &pin).is_err());
        let headers = scale_native_runner::scale_reducer_retry_header_fixture(dir.path()).unwrap();
        let mut s3 = rows.clone();
        let header_rows = std::fs::read_to_string(headers).unwrap().lines().take(2)
            .map(|line| serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
        s3[..2].clone_from_slice(&header_rows);
        let binding_index = s3.iter().position(|r| r["phase"] == "source_binding").unwrap();
        let root: Value = serde_json::from_str(s3[binding_index]["generation_authority"]["root_manifest"].as_str().unwrap()).unwrap();
        s3[1]["backend"]["sq8_object_key"] = root["sq8_object_key"].clone();
        s3[1]["backend"]["sq8_etag"] = root["sq8_etag"].clone();
        let binding = json!({"submitted_gets":3,"verified_bytes":1,"failed_gets":0});
        s3[binding_index]["charges"] = binding.clone();
        let mut stats = json!({"attempts":0,"method_counts":[0,0,0,0,0,0,0,0,0,0],"status_counts":[],
            "transport_failures":0,"stream_failures":0,"consumed_payload_bytes":0,"dropped_error_bodies":0});
        for row in &mut s3 {
            if row["phase"] == "startup" {
                let before = stats.clone();
                stats["attempts"] = json!(stats["attempts"].as_u64().unwrap()+1);
                stats["method_counts"][1] = json!(1);
                stats["status_counts"] = json!([[200,stats["attempts"]]]);
                row["fp32_rerank"]["transport"] = json!({"stage":"canonical_open","ordinal":null,"before":before,"after":stats});
            } else if row.get("transport").is_some() {
                row["transport"]["before"] = stats.clone();
                row["transport"]["after"] = stats.clone();
            }
        }
        let last: TransportSpan = serde_json::from_value(
            s3.iter().rfind(|r| r["phase"] == "query").unwrap()["transport"].clone()).unwrap();
        s3.last_mut().unwrap()["summary"]["binding_charge"] = binding;
        s3.last_mut().unwrap()["summary"]["transport_last_boundary"] = last.compact();
        let (path, _) = write_fixture(dir.path(), "fp32-s3-counter-fixture.jsonl", s3.clone());
        let (config, pin) = scale_config(dir.path(), &path, &s3);
        fp32_serving::reduce(&config, &pin).unwrap();
        for case in ["zero-head", "counter-gap", "two-success-heads"] {
            let mut bad = s3.clone();
            let span = &mut bad[startup]["fp32_rerank"]["transport"];
            if case == "zero-head" {
                span["after"] = span["before"].clone();
            } else {
                let boundary = if case == "counter-gap" { "before" } else { "after" };
                let n = span[boundary]["attempts"].as_u64().unwrap() + 1;
                span[boundary]["attempts"] = json!(n);
                span[boundary]["method_counts"][1] = json!(n);
                span[boundary]["status_counts"] = json!([[200,n]]);
                if case == "counter-gap" {
                    span["after"]["attempts"] = json!(n+1);
                    span["after"]["method_counts"][1] = json!(n+1);
                    span["after"]["status_counts"] = json!([[200,n+1]]);
                }
            }
            let (path, _) = write_fixture(dir.path(), "fp32-resealed-head-falsifier.jsonl", bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            assert!(fp32_serving::reduce(&config, &pin).is_err(), "{case}");
        }
    }
    #[test]
    fn fp32_canonical_head_and_serial_continuity_are_exact() {
        fn stats(heads: u64, gets: u64, payload: u64) -> Value {
            json!({"attempts":heads+gets,"method_counts":[gets,heads,0,0,0,0,0,0,0,0],
                "status_counts":if heads+gets==0 {json!([])} else {json!([[200,heads+gets]])},
                "transport_failures":0,"stream_failures":0,
                "consumed_payload_bytes":payload,"dropped_error_bodies":0})
        }
        let span = |stage: &str, before: Value, after: Value| {
            serde_json::to_vec(&json!({"transport":{"stage":stage,"ordinal":null,"before":before,"after":after}})).unwrap()
        };
        for (before, after, valid) in [
            (stats(1,0,0),stats(2,0,0),true),
            (stats(1,0,0),stats(1,0,0),false),
            (stats(2,0,0),stats(3,0,0),false),
            (stats(1,0,0),stats(2,1,1),false),
            (stats(1,0,0),stats(3,0,0),false),
        ] {
            let mut evidence = CompletedEvidence::new(Charge::default());
            evidence.fp32_head_attempt_cap = Some(1);
            evidence.transport(&span("generation_open",stats(0,0,0),stats(1,0,0)), "generation_open",None).unwrap();
            assert_eq!(evidence.transport(&span("canonical_open",before,after),"canonical_open",None).is_ok(),valid);
        }
        let mut evidence = CompletedEvidence::new(Charge::default());
        evidence.fp32_head_attempt_cap = Some(2);
        assert!(evidence.transport(&span("canonical_open",stats(0,0,0),stats(2,0,0)),"canonical_open",None).is_err());
        let mut retried = stats(2,0,0);
        retried["status_counts"] = json!([[200,1],[503,1]]);
        retried["dropped_error_bodies"] = json!(1);
        evidence.transport(&span("canonical_open",stats(0,0,0),retried),"canonical_open",None).unwrap();
    }
    #[test]
    fn v12_library_scheduler_startup_and_protocol_binding_are_strict() {
        let (dir, result) = scale_native_runner::scale_reducer_native_fixture(false, true).unwrap();
        let rows = assert_current_native(dir.path(), &result);
        let startup = rows.iter().position(|r| r["phase"] == "startup").unwrap();
        let admission = rows
            .iter()
            .position(|r| r["phase"] == "diagnostic_admission")
            .unwrap();
        for (index, key, value) in [
            (0, "schema", json!(current_v10::RESULT_SCHEMA)),
            (1, "range_scheduler_owner", json!("runner")),
            (
                startup,
                "range_state_charged_by_library_bytes",
                json!(
                    rows[startup]["range_state_charged_by_library_bytes"]
                        .as_u64()
                        .unwrap()
                        + 1
                ),
            ),
            (admission, "range_state_charged_by_library_bytes", json!(0)),
            (admission, "range_state_pinned_by_runner_bytes", json!(0)),
            (
                admission,
                "trace_peak_charged_by_library_at_traced_admission",
                json!(false),
            ),
        ] {
            let mut bad = rows.clone();
            bad[index][key] = value;
            let (path, _) =
                write_fixture(dir.path(), "v12-accounting-falsifier.jsonl", bad.clone());
            let (config, pin) = scale_config(dir.path(), &path, &bad);
            assert!(reduce_scale(&config, &pin).is_err(), "{index} {key}");
        }
    }

    #[test]
    fn actual_native_scale_runner_reducer_seal_count_mismatches() {
        for (prefix, panel) in [(false, false), (false, true), (true, false), (true, true)] {
            let (dir, result) =
                scale_native_runner::scale_reducer_native_fixture(prefix, panel).unwrap();
            let rows = assert_current_native(dir.path(), &result);
            // Pin expected headers before any falsifier changes producer output.
            assert_eq!(rows[0]["schema"], current_v10::FRAMED_RESULT_SCHEMA);
            assert_eq!(rows[0]["sq8_range_split"], Value::Null);
            let (config, pin) = scale_config(dir.path(), &result, &rows);
            let (pinned, _): (CompletedConfig, _) = read_config(&config, &pin).unwrap();
            let report = reduce_scale(&config, &pin).unwrap();
            assert_eq!(report["diagnostic_only"], true);
            assert_eq!(report["population_percentiles_valid"], false);
            assert_eq!(report["local_file_only"], true);
            assert_eq!(report["reduction_reads_local_files_only"], true);
            assert_eq!(report["producer_backend_kind"], "local");
            assert_eq!(report["cold_s3_claim"], false);
            assert_eq!(report["external_resources_and_cost_gate_required"], true);
            assert!(report["statistics"].is_null() && report["stage_statistics"].is_null());
            assert_eq!(
                report["native_terminal"]["queries"],
                if prefix || panel { 1 } else { 2 }
            );
            let seal = rows
                .iter()
                .position(|r| r["phase"] == "all_queries_sealed")
                .unwrap();
            let query = rows.iter().position(|r| r["phase"] == "query").unwrap();
            for case in [
                "seal",
                "count",
                "population",
                "geometry",
                "terminal",
                "query",
                "profile",
                "receipt",
                "producer",
                "old",
                "reserved",
                "reserved-count",
                "diagnostic-prefix",
                "physical-ordinal",
                "logical-count",
                "split",
                "unknown",
                "missing",
            ] {
                let mut bad = rows.clone();
                match case {
                    "seal" => bad[seal]["prefix_sha256"] = json!("0".repeat(64)),
                    "count" => bad[seal]["count"] = json!(3),
                    "population" => bad[seal]["population_count"] = json!(3),
                    "geometry" => bad[1]["rows"] = json!(33),
                    "terminal" => bad.last_mut().unwrap()["summary"]["selected_count"] = json!(3),
                    "query" => bad[query]["ordinal"] = json!(3),
                    "profile" => bad[1]["profile"] = json!("fresh1m"),
                    "receipt" => bad[1]["cohort_receipt_sha256"] = json!("invalid"),
                    "producer" => {
                        bad[1]["producer_authority"]["executable_sha256"] = json!("invalid")
                    }
                    "reserved" => bad[1]["reserved_query_interval"]["start"] = json!(33),
                    "reserved-count" => bad[seal]["reserved_query_count"] = json!(3),
                    "diagnostic-prefix" => {
                        let terminal = bad.iter_mut().find(|v| v["phase"] == "terminal").unwrap();
                        terminal["summary"]["diagnostic_prefix"] = json!(!prefix);
                    }
                    "physical-ordinal" => bad[query]["returned_physical_ordinals"][0] = json!(32),
                    "logical-count" => bad[query]["plan"]["logical_range_count"] = json!(2),
                    "split" => {
                        bad[1]["sq8_range_split"] =
                            json!({"max_physical_range_bytes":0,"max_physical_gets":1})
                    }
                    "unknown" => bad[query]["unknown_current_field"] = json!(0),
                    "missing" => {
                        bad[query]
                            .as_object_mut()
                            .unwrap()
                            .remove("returned_physical_ordinals");
                    }
                    "old" => {
                        bad[0]["schema"] = json!("borsuk-cohere-native-baseline-result-v5");
                        // The historical writer had no explicit budget. Keep
                        // this falsifier about the old marker, not a v8 field.
                        bad[0].as_object_mut().unwrap().remove("source_get_budget");
                        bad[1].as_object_mut().unwrap().remove("source_get_budget");
                    }
                    _ => unreachable!(),
                }
                // Restore surrounding seals for semantic contradictions; leave hash corruption explicit.
                if case != "seal" {
                    authenticate(&mut bad);
                }
                let expected_error = match case {
                    "seal" | "count" => "authenticated query seal",
                    "population" | "reserved-count" => "scale seal counts",
                    "geometry" => "exact corpus count",
                    "terminal" => "scale terminal geometry/execution",
                    "query" => "query order/truth/count/underfill",
                    "profile" => "explicit scale input geometry/profile/cap",
                    "receipt" => "original/derived receipt SHA256",
                    "producer" => "root-frozen producer SHA256",
                    "reserved" => "selected prefix within full reserved interval",
                    "diagnostic-prefix" => "reserved population and diagnostic prefix scope",
                    "old" => "scale result v7/v8 required",
                    "physical-ordinal" => "v10 unique bounded physical ordinal",
                    "logical-count" => "v10 logical range count",
                    "split" => "v10 split caps",
                    "unknown" => r#"unknown/missing v2 field: missing=[] extra=["unknown_current_field"]"#,
                    "missing" => r#"unknown/missing v2 field: missing=["returned_physical_ordinals"] extra=[]"#,
                    _ => unreachable!(),
                };
                let path = dir.path().join(case);
                std::fs::write(&path, encode(&bad)).unwrap();
                let (config, pin) = scale_config(dir.path(), &path, &bad);
                let error = reduce_scale(&config, &pin).unwrap_err().to_string();
                assert_eq!(
                    error, expected_error,
                    "{case} prefix={prefix} panel={panel}"
                );
            }
            // A valid resealed envelope cannot replace the independently pinned headers.
            for (header, key, error) in [
                (0, "binary_sha256", "expected v2 identity pins"),
                (
                    1,
                    "native_sq8_sha256",
                    "expected v2 input/root/backend pins",
                ),
            ] {
                let mut bad = rows.clone();
                bad[header][key] = json!("0".repeat(64));
                authenticate(&mut bad);
                let bytes = encode(&bad);
                let path = dir.path().join(format!("independent-header-{header}"));
                std::fs::write(&path, &bytes).unwrap();
                let independent = CompletedConfig {
                    schema: pinned.schema.clone(),
                    input: CompletedInput {
                        path,
                        bytes: bytes.len() as u64,
                        sha256: sha(&bytes),
                    },
                    expected_identity: pinned.expected_identity.clone(),
                    expected_bound_inputs: pinned.expected_bound_inputs.clone(),
                };
                assert_eq!(
                    read_run_with(
                        &independent.input.path,
                        &independent.input.sha256,
                        Some(&independent)
                    )
                    .err()
                    .unwrap()
                    .to_string(),
                    error
                );
            }
        }
    }

    // Synthetic sealed native-schema evidence. This exercises the full reducer;
    // it is not an index-quality or S3-runtime measurement.
    fn scale_parity_dispatch_fixture(dir: &Path) -> Value {
        let requests = vec![0_u8; 4_096_000];
        let truth = vec![0_u8; 80_000];
        let file_pin = |name: &str, body: &[u8]| {
            let path = dir.join(name);
            std::fs::write(&path, body).unwrap();
            json!({"path":path,"bytes":body.len(),"sha256":sha(body)})
        };
        let request_pins = [
            file_pin("request32", &requests[..131_072]),
            file_pin("request1000", &requests),
        ];
        let truth_pins = [
            file_pin("truth32", &truth[..2560]),
            file_pin("truth1000", &truth),
        ];
        let mut arms = Vec::new();
        for arm in 0..3 {
            let mut rows = v2_fixture();
            rows.retain(|row| {
                !matches!(row["phase"].as_str(), Some("query" | "recall"))
                    || row["ordinal"].as_u64().unwrap() < 32
            });
            let execution = if arm == 0 {
                json!({"mode":"full"})
            } else {
                json!({"mode":"diagnostic_panel","ordinals":(0..32).collect::<Vec<_>>(),"trace":false})
            };
            rows[0]["schema"] = json!("borsuk-cohere-native-baseline-result-v7");
            rows[0]["config_sha256"] = json!((arm + 6).to_string().repeat(64));
            rows[0]["execution"] = execution.clone();
            rows[0]["serving"] = json!({"mode":"baseline"});
            rows[0]["fetch_parallelism"] = json!(16);
            for key in ["sq8_range_source_sha256", "returned_source_sha256"] {
                rows[0][key] = json!("9".repeat(64));
            }
            let selected = usize::from(arm != 0);
            let root = ["a", "b", "c"][arm].repeat(64);
            let inputs = &mut rows[1];
            inputs["dataset"] = json!("CohereLabs/wikipedia-2023-11-embed-multilingual-v3");
            inputs["revision"] = json!("ade45fb52bd549f5e8c065636fe4160a43c2af36");
            inputs["rows"] = json!(1_000_000);
            inputs["dimensions"] = json!(1024);
            inputs["profile"] = json!("scale1m");
            inputs["query_source_first"] = json!(100_000);
            inputs["count"] = json!(if arm == 0 { 32 } else { 1000 });
            inputs["selected_count"] = json!(32);
            inputs["execution"] = execution.clone();
            inputs["generation_root_sha256"] = json!(root);
            for (pins, size, hash) in [
                (&request_pins, "requests_bytes", "requests_sha256"),
                (&truth_pins, "truth_bytes", "truth_sha256"),
            ] {
                inputs[size] = pins[selected]["bytes"].clone();
                inputs[hash] = pins[selected]["sha256"].clone();
            }
            inputs["reserved_queries_sha256"] = request_pins[1]["sha256"].clone();
            inputs["corpus_intervals"] =
                json!([{"start":0,"end":100_000},{"start":101_000,"end":1_001_000}]);
            inputs["reserved_query_interval"] = json!({"start":100_000,"end":101_000});
            inputs["serving"] = json!({"mode":"baseline"});
            inputs["source_cache"] = json!("off");
            inputs["fetch_parallelism"] = json!(16);
            inputs["max_memory_bytes"] = json!(536_870_912);
            inputs["cohort_receipt_sha256"] = json!(if arm == 0 { "1" } else { "2" }.repeat(64));
            inputs["derivation_receipt_sha256"] =
                json!(if arm == 0 { "3" } else { "4" }.repeat(64));
            inputs["producer_authority"] = json!({"source_commit":"4".repeat(40),"executable_sha256":"5".repeat(64),
                "producer_source_sha256":"6".repeat(64),"sq8_source_sha256":"7".repeat(64),"source_order_source_sha256":"8".repeat(64)});
            if arm < 2 {
                inputs["backend"] = json!({"kind":"local","store_root":"/synthetic/store"});
                inputs["credential_source"] = Value::Null;
            }
            for row in &mut rows {
                match row["phase"].as_str().unwrap() {
                    "source_binding" if arm < 2 => row["charges"] = charge(0),
                    "startup" => row["serving"] = json!({"mode":"baseline"}),
                    "query" => {
                        if arm != 0 {
                            row["selected_slot"] = row["ordinal"].clone();
                        }
                        row["serving"] = json!({"mode":"baseline"});
                        row["source_nomination_skipped"] = json!(false);
                        row["planning_scope"] = json!("source_nomination_and_cover");
                        row["plan"] = json!({"selected_pages":[0],"ranges":[[0,100]],"planned_bytes":100,
                            "target_pages":1,"target_shortfall":0,"primary_pages_retained":1,"covered_pages":1,"bridge_pages":0});
                    }
                    "all_queries_sealed" => {
                        row["count"] = json!(32);
                        row["selected_count"] = json!(32);
                        row["population_count"] = json!(if arm == 0 { 32 } else { 1000 });
                        row["reserved_query_count"] = json!(1000);
                        row["requests_sha256"] = request_pins[selected]["sha256"].clone();
                        row["generation_root_sha256"] = json!(root);
                    }
                    _ => (),
                }
                if arm < 2 && row.get("transport").is_some() {
                    row["transport"]["before"] = Value::Null;
                    row["transport"]["after"] = Value::Null;
                }
            }
            let last_query = rows.iter().rfind(|row| row["phase"] == "query").unwrap();
            let mut boundary = last_query["transport"].clone();
            if arm == 2 {
                for key in ["before", "after"] {
                    boundary[key]["status_counts"] = Value::Null;
                    boundary[key]["status_counts_entries"] = json!(1);
                }
            }
            boundary["scope"] = json!("cumulative_process_native_transport");
            boundary["status_counts_omitted_from_terminal"] = json!(true);
            for key in [
                "wire_bytes",
                "unread_bytes",
                "billed_bytes",
                "billed_requests",
            ] {
                boundary[key] = Value::Null;
            }
            let summary = &mut rows.last_mut().unwrap()["summary"];
            if arm < 2 {
                summary["binding_charge"] = charge(0);
            }
            summary["queries"] = json!(32);
            summary["total_hits10"] = json!(288);
            summary["recall_numerator"] = json!(288);
            summary["recall_denominator"] = json!(320);
            summary["mean_recall10"] = json!(0.9);
            summary["charges"] = charges(32);
            summary["sum"] = charge(96);
            summary["query_wall_ns"] = json!(528_000_000_u64);
            summary["process_wall_ns"] = json!(528_000_100_u64);
            summary["query_process_cpu_ns"] = json!(32);
            summary["process_cpu_ns"] = json!(132);
            summary["requests_sha256"] = request_pins[selected]["sha256"].clone();
            summary["truth_sha256"] = truth_pins[selected]["sha256"].clone();
            summary["generation_root_sha256"] = json!(root);
            summary["transport_last_boundary"] = boundary;
            summary["serving"] = json!({"mode":"baseline"});
            summary["direct_memory"] = Value::Null;
            summary["execution"] = execution;
            summary["selected_count"] = json!(32);
            summary["executed_count"] = json!(32);
            summary["population_count"] = json!(if arm == 0 { 32 } else { 1000 });
            summary["reserved_query_count"] = json!(1000);
            summary["diagnostic_panel"] = json!(arm != 0);
            summary["diagnostic_prefix"] = json!(arm == 0);
            summary["population_percentiles_valid"] = json!(false);
            summary["full_cohort_qualification"] = json!(false);
            if arm != 0 {
                // A panel has an admission row before binding/open, unlike the full-run fixture.
                rows.insert(2, json!({"phase":"diagnostic_admission", "execution":rows[0]["execution"],
                    "selected_count":32,"population_count":1000,"trace":false,
                    "diagnostic_bytes_per_active_query":0,"diagnostic_cap_bytes":536_870_912,
                    "max_active_queries":1,"charged_in_caller_pinned_bytes":true,"caller_pinned_bytes":0,
                    "panel_line_cap_bytes":32768,"host_read_cap_bytes":32768,"trace_ranges":0,
                    "population_percentiles_valid":false,"full_cohort_qualification":false,
                    "semantics":"synthetic untraced panel schema fixture","truth_opened":false,
                    "trace_retained_bytes":0,"trace_peak_bytes":0,"diagnostic_pinned_bytes":0,
                    "trace_peak_charged_by_library_at_traced_admission":0,
                    "range_state_pinned_by_runner_bytes":0,"range_state_both_paths_bytes":0}));
            }
            authenticate(&mut rows);
            let body = encode(&rows);
            let path = dir.join(format!("parity-run{arm}.jsonl"));
            std::fs::write(&path, &body).unwrap();
            arms.push(json!({"schema":SCALE_CONFIG_SCHEMA,"input":{"path":path,"bytes":body.len(),"sha256":sha(&body)},
                "expected_identity":rows[0],"expected_bound_inputs":rows[1]}));
        }
        json!({"schema":SCALE_PARITY_CONFIG_SCHEMA,"runs":arms,"requests":request_pins,"truth":truth_pins})
    }

    #[test]
    fn scale_prefix_parity_full_dispatch_seals_and_input_binding() {
        let dir = tempfile::tempdir().unwrap();
        let config = scale_parity_dispatch_fixture(dir.path());
        let path = dir.path().join("parity-config.json");
        let body = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &body).unwrap();
        let report = reduce_scale_prefix_parity(&path, &sha(&body)).unwrap();
        assert_eq!(report["status"], "EXACT_PREFIX_PARITY");
        assert_eq!(report["matched_queries"], 32);
        assert_eq!(report["cold_s3_claim"], false);
        assert_eq!(report["external_generation_provenance_gate_required"], true);
        let output = dir.path().join("parity-report.json");
        assert!(
            execute_report(&output, SCALE_PARITY_REPORT_SCHEMA, || {
                reduce_scale_prefix_parity(&path, &sha(&body))
            })
            .unwrap()
        );
        let persisted: Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
        assert_eq!(persisted["status"], "EXACT_PREFIX_PARITY");
        assert_eq!(persisted["complete"], true);
        assert!(
            execute_report(&output, SCALE_PARITY_REPORT_SCHEMA, || {
                reduce_scale_prefix_parity(&path, &sha(&body))
            })
            .is_err()
        );
        for key in [
            "count",
            "fetch_parallelism",
            "native_sq8_sha256",
            "cohort_receipt_sha256",
        ] {
            let mut bad = config.clone();
            bad["runs"][2]["expected_bound_inputs"][key] = json!("changed");
            let bytes = serde_json::to_vec(&bad).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            assert!(
                reduce_scale_prefix_parity(&path, &sha(&bytes)).is_err(),
                "{key}"
            );
        }
        let mut bad = config.clone();
        bad["runs"][2] = bad["runs"][1].clone();
        let bytes = serde_json::to_vec(&bad).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert!(reduce_scale_prefix_parity(&path, &sha(&bytes)).is_err());
        std::fs::write(&path, &body).unwrap();
        std::fs::write(config["truth"][1]["path"].as_str().unwrap(), b"short").unwrap();
        assert!(reduce_scale_prefix_parity(&path, &sha(&body)).is_err());

        // A broken seal must fail before attempting to open a nofollow truth path.
        let mut bad = config.clone();
        let run_path = PathBuf::from(bad["runs"][2]["input"]["path"].as_str().unwrap());
        let mut rows: Vec<Value> = std::fs::read_to_string(&run_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        rows.iter_mut()
            .find(|row| row["phase"] == "all_queries_sealed")
            .unwrap()["prefix_sha256"] = json!("0".repeat(64));
        let bytes = encode(&rows);
        std::fs::write(&run_path, &bytes).unwrap();
        bad["runs"][2]["input"]["bytes"] = json!(bytes.len());
        bad["runs"][2]["input"]["sha256"] = json!(sha(&bytes));
        std::fs::remove_file(config["truth"][1]["path"].as_str().unwrap()).unwrap();
        std::os::unix::fs::symlink(
            "/must-not-open-truth",
            config["truth"][1]["path"].as_str().unwrap(),
        )
        .unwrap();
        let body = serde_json::to_vec(&bad).unwrap();
        std::fs::write(&path, &body).unwrap();
        let invalid = dir.path().join("invalid-parity-report.json");
        assert!(
            !execute_report(&invalid, SCALE_PARITY_REPORT_SCHEMA, || {
                reduce_scale_prefix_parity(&path, &sha(&body))
            })
            .unwrap()
        );
        let invalid: Value = serde_json::from_slice(&std::fs::read(&invalid).unwrap()).unwrap();
        assert_eq!(invalid["status"], "INVALID");
        assert_eq!(invalid["error"], "authenticated query seal");
    }

    #[test]
    fn scale_parity_role_aliases_and_native_config_reuse_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let config = scale_parity_dispatch_fixture(dir.path());
        let mut bad = config.clone();
        bad["runs"][2]["input"]["path"] = bad["truth"][1]["path"].clone();
        let typed: ScalePrefixParityConfig = serde_json::from_value(bad).unwrap();
        assert_eq!(
            parity_role_identities(&typed).unwrap_err().to_string(),
            "parity file role alias"
        );
        let alias = dir.path().join("truth-hard-link");
        std::fs::hard_link(config["truth"][1]["path"].as_str().unwrap(), &alias).unwrap();
        let mut bad = config.clone();
        bad["runs"][2]["input"]["path"] = json!(alias);
        let typed: ScalePrefixParityConfig = serde_json::from_value(bad).unwrap();
        assert_eq!(
            parity_role_identities(&typed).unwrap_err().to_string(),
            "parity file role alias"
        );

        let mut bad = config.clone();
        let run_path = PathBuf::from(bad["runs"][2]["input"]["path"].as_str().unwrap());
        let mut rows: Vec<Value> = std::fs::read_to_string(&run_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let duplicate = bad["runs"][1]["expected_identity"]["config_sha256"].clone();
        rows[0]["config_sha256"] = duplicate.clone();
        bad["runs"][2]["expected_identity"]["config_sha256"] = duplicate;
        authenticate(&mut rows);
        let bytes = encode(&rows);
        std::fs::write(&run_path, &bytes).unwrap();
        bad["runs"][2]["input"]["bytes"] = json!(bytes.len());
        bad["runs"][2]["input"]["sha256"] = json!(sha(&bytes));
        let body = serde_json::to_vec(&bad).unwrap();
        let path = dir.path().join("duplicate-config.json");
        std::fs::write(&path, &body).unwrap();
        assert_eq!(
            reduce_scale_prefix_parity(&path, &sha(&body))
                .unwrap_err()
                .to_string(),
            "distinct parity run artifacts"
        );
    }

    fn v2_fixture() -> Vec<Value> {
        fn snapshot(n: u64) -> Value {
            json!({"attempts":n,"method_counts":[n.saturating_sub(1),0,u64::from(n > 0),0,0,0,0,0,0,0],
                "status_counts":if n == 0 {json!([])} else {json!([[200,n]])},
                "transport_failures":0,"stream_failures":0,"consumed_payload_bytes":n*100,"dropped_error_bodies":0})
        }
        fn transport(stage: &str, ordinal: Option<usize>, before: u64, after: u64) -> Value {
            json!({"stage":stage,"ordinal":ordinal,"before":snapshot(before),"after":snapshot(after)})
        }
        let mut rows = fixture(false, 1_000_000);
        let identity = &mut rows[0];
        identity["schema"] = json!("borsuk-cohere-native-baseline-result-v2");
        identity["scope"] = json!("AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS");
        identity["io_measurement"] =
            json!("logical_GET_charges_separate_from_cumulative_process_native_transport");
        identity["s3_credential_source"] = json!("imds_instance_role_only");
        identity["native_transport_includes"] =
            json!("S3_and_IMDS_credential_requests_including_PUT");
        for key in [
            "wire_bytes",
            "unread_bytes",
            "billed_bytes",
            "billed_requests",
        ] {
            identity[key] = Value::Null;
        }
        for key in [
            "config_sha256",
            "binary_sha256",
            "runner_source_sha256",
            "generation_source_sha256",
            "router_source_sha256",
            "codec_source_sha256",
            "source_plane_source_sha256",
        ] {
            identity[key] = json!("9".repeat(64));
        }
        let inputs = &mut rows[1];
        inputs.as_object_mut().unwrap().remove("store_root");
        inputs["dataset"] = json!("synthetic/native-fixture");
        inputs["revision"] = json!("fixture-revision");
        inputs["rows"] = json!(32);
        inputs["dimensions"] = json!(16);
        inputs["query_source_first"] = json!(32);
        inputs["requests_bytes"] = json!(64_000);
        inputs["credential_source"] = json!("imds_instance_role_only");
        for (key, c) in [
            ("requests_sha256", "c"),
            ("truth_sha256", "d"),
            ("native_source_sha256", "e"),
            ("native_sq8_sha256", "f"),
            ("native_order_sha256", "b"),
        ] {
            inputs[key] = json!(c.repeat(64));
        }
        inputs["backend"] = json!({"kind":"s3","bucket":"fixture-bucket","region":"test-region-1",
            "physical_prefix":"fixture/run","sq8_object_key":format!("fixture/objects/{}", "f".repeat(64)),"sq8_etag":"\"fixture-etag\""});
        for (ordinal, row) in rows[3..1003].iter_mut().enumerate() {
            let n = ordinal as u64 + 1;
            row["stages"] = json!({"discovery":{"start_ns":1,"end_ns":n+1},
                "source":{"start_ns":n+1,"end_ns":2*n+1},"planning":{"start_ns":2*n+1,"end_ns":2*n+1},
                "sq8":{"start_ns":2*n+1,"end_ns":3*n+1},"leaf_peak_inflight":1});
            row["transport"] = transport(
                "query",
                Some(ordinal),
                5 + ordinal as u64 * 3,
                8 + ordinal as u64 * 3,
            );
        }
        rows[1003]["requests_sha256"] = json!("c".repeat(64));
        let mut last = rows[1002]["transport"].clone();
        for key in ["before", "after"] {
            last[key]["status_counts"] = Value::Null;
            last[key]["status_counts_entries"] = json!(1);
        }
        last["scope"] = json!("cumulative_process_native_transport");
        last["status_counts_omitted_from_terminal"] = json!(true);
        for key in [
            "wire_bytes",
            "unread_bytes",
            "billed_bytes",
            "billed_requests",
        ] {
            last[key] = Value::Null;
        }
        let terminal = &mut rows.last_mut().unwrap()["summary"];
        terminal["requests_sha256"] = json!("c".repeat(64));
        terminal["truth_sha256"] = json!("d".repeat(64));
        terminal["binding_charge"] = charge(2);
        terminal["transport_last_boundary"] = last;
        rows.insert(
            2,
            json!({"phase":"source_binding","transport":transport("native_source",None,0,3),
            "charges":charge(2),"success":true,"truth_opened":false}),
        );
        rows.insert(
            3,
            json!({"phase":"generation_open","transport":transport("generation_open",None,3,5),
            "success":true,"truth_opened":false}),
        );
        rows[4]["metadata"]["staging_wall_ns"] = json!(10);
        rows[4]["metadata"]["metadata"] = json!([{"name":"manifest.json",
            "metadata_wave":0,"metadata_wave_wall_ns":6,"bytes":100,"reused_root_bytes":0,
            "retained_root_bytes":0,"local_auth_wall_ns":0,"local_copy_wall_ns":0,"chunks":1,
            "head_wall_ns":1,"logical_head_requests":1,"logical_get_requests":1,
            "payload_buffer_bound_bytes":100,"get_wall_ns":2,"stream_wall_ns":3,"write_wall_ns":1}]);
        rows
    }

    fn v2_config(dir: &Path, bytes: &[u8], expected: &[Value]) -> (PathBuf, String) {
        let input = dir.join("completed-v2.jsonl");
        std::fs::write(&input, bytes).unwrap();
        let config = json!({"schema":"borsuk-completed-native-reduction-config-v1",
            "input":{"path":input,"bytes":bytes.len(),"sha256":sha(bytes)},
            "expected_identity":expected[0],"expected_bound_inputs":expected[1]});
        let bytes = serde_json::to_vec(&config).unwrap();
        let path = dir.join("completed-config.json");
        std::fs::write(&path, &bytes).unwrap();
        (path, sha(&bytes))
    }

    fn paired_v2_fixture(parallelism: u64) -> Vec<Value> {
        let mut rows = v2_fixture();
        rows[0]["fetch_parallelism"] = json!(parallelism);
        rows[0]["config_sha256"] = json!(if parallelism == 16 { "6" } else { "3" }.repeat(64));
        rows[1]["fetch_parallelism"] = json!(parallelism);
        rows[1]["source_cache"] = json!("off");
        // Only output metadata is constructed; no corpus/query/truth body is
        // opened. Literal production pins are independent of the admission code.
        rows[1]["dataset"] = json!("CohereLabs/wikipedia-2023-11-embed-multilingual-v3");
        rows[1]["revision"] = json!("ade45fb52bd549f5e8c065636fe4160a43c2af36");
        rows[1]["rows"] = json!(100000);
        rows[1]["dimensions"] = json!(1024);
        rows[1]["query_source_first"] = json!(100000);
        rows[1]["requests_bytes"] = json!(4096000);
        for (key, pin) in [
            (
                "requests_sha256",
                "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e",
            ),
            (
                "truth_sha256",
                "479064239b698a2b8094c7838b1bb01af6eff5ea6eee4736692971849fdcfb2c",
            ),
            (
                "native_source_sha256",
                "20936913f31e48ea67d462dfffc7a831569ff7467baebc63f2d622c8ff417dce",
            ),
            (
                "native_sq8_sha256",
                "07a14360b06add9a35f82b97f4e031690ff878ff497ee047902818823992337b",
            ),
            (
                "native_order_sha256",
                "6b6a67098330c76bfb89340065d9326fb3f23a1f6016fbae5b9fa7f0e29759c2",
            ),
        ] {
            rows[1][key] = json!(pin);
        }
        rows[1]["backend"]["sq8_object_key"] = json!(format!(
            "fixture/objects/{}",
            rows[1]["native_sq8_sha256"].as_str().unwrap()
        ));
        rows[1005]["requests_sha256"] = rows[1]["requests_sha256"].clone();
        for key in ["requests_sha256", "truth_sha256"] {
            rows.last_mut().unwrap()["summary"][key] = rows[1][key].clone();
        }
        for row in &mut rows[5..1005] {
            row["trace"]["discoveries"] = json!([{"seed_page":0,"seed_evaluated_units":[2,1],
                "walk_evaluated_units":[3,4],"seed_work_exhausted":false,"walk_work_exhausted":true}]);
            if parallelism == 32 {
                row["query_wall_ns"] = json!(row["query_wall_ns"].as_u64().unwrap() * 2);
                row["query_process_cpu_ns"] = json!(2);
                row["stages"]["sq8"]["end_ns"] =
                    json!(row["stages"]["sq8"]["end_ns"].as_u64().unwrap() + 1);
            }
        }
        if parallelism == 32 {
            // Legitimate cumulative transport variation is NOT logical charge
            // or plan variation. Independently patch the native compact footer.
            for row in &mut rows[2..1005] {
                if row.get("transport").is_some() {
                    for key in ["before", "after"] {
                        row["transport"][key]["consumed_payload_bytes"] = json!(
                            row["transport"][key]["consumed_payload_bytes"]
                                .as_u64()
                                .unwrap()
                                + 7
                        );
                    }
                }
            }
            let t = &mut rows.last_mut().unwrap()["summary"];
            t["query_wall_ns"] = json!(1_001_000_000_000_u64);
            t["process_wall_ns"] = json!(1_001_000_000_100_u64);
            t["query_process_cpu_ns"] = json!(2000);
            t["process_cpu_ns"] = json!(2100);
            t["transport_last_boundary"]["before"]["consumed_payload_bytes"] = json!(300207);
            t["transport_last_boundary"]["after"]["consumed_payload_bytes"] = json!(300507);
        }
        rows
    }

    pub(super) fn paired_scale_fixture(parallelism: u64) -> Vec<Value> {
        let mut rows = paired_v2_fixture(parallelism);
        rows[0]["schema"] = json!("borsuk-cohere-native-baseline-result-v7");
        rows[0]["serving"] = json!({"mode":"baseline"});
        rows[0]["execution"] = json!({"mode":"full"});
        for key in ["sq8_range_source_sha256", "returned_source_sha256"] {
            rows[0][key] = json!("9".repeat(64));
        }
        let inputs = &mut rows[1];
        inputs["rows"] = json!(1_000_000);
        inputs["profile"] = json!("scale1m");
        inputs["corpus_intervals"] =
            json!([{"start":0,"end":100000},{"start":101000,"end":1001000}]);
        inputs["reserved_query_interval"] = json!({"start":100000,"end":101000});
        inputs["reserved_queries_sha256"] =
            json!("8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e");
        inputs["serving"] = json!({"mode":"baseline"});
        inputs["execution"] = json!({"mode":"full"});
        inputs["selected_count"] = json!(1000);
        inputs["max_memory_bytes"] = json!(536870912);
        inputs["cohort_receipt_sha256"] = json!("1".repeat(64));
        inputs["derivation_receipt_sha256"] = json!("2".repeat(64));
        inputs["producer_authority"] = json!({"source_commit":"4".repeat(40),"executable_sha256":"5".repeat(64),
            "producer_source_sha256":"6".repeat(64),"sq8_source_sha256":"7".repeat(64),"source_order_source_sha256":"8".repeat(64)});
        for (key, pin) in [
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
            inputs[key] = json!(pin);
        }
        inputs["backend"]["sq8_object_key"] = json!(format!(
            "fixture/objects/{}",
            inputs["native_sq8_sha256"].as_str().unwrap()
        ));
        let truth = inputs["truth_sha256"].clone();
        for row in &mut rows {
            match row["phase"].as_str().unwrap() {
                "startup" => row["serving"] = json!({"mode":"baseline"}),
                "query" => {
                    row["serving"] = json!({"mode":"baseline"});
                    row["source_nomination_skipped"] = json!(false);
                    row["planning_scope"] = json!("source_nomination_and_cover");
                    row["plan"] = json!({"selected_pages":[0],"ranges":[[0,100]],"planned_bytes":100,"target_pages":1,
                        "target_shortfall":0,"primary_pages_retained":1,"covered_pages":1,"bridge_pages":0});
                }
                "all_queries_sealed" => {
                    row["selected_count"] = json!(1000);
                    row["population_count"] = json!(1000);
                    row["reserved_query_count"] = json!(1000);
                }
                "terminal" => {
                    let summary = &mut row["summary"];
                    summary["truth_sha256"] = truth.clone();
                    summary["serving"] = json!({"mode":"baseline"});
                    summary["direct_memory"] = Value::Null;
                    summary["execution"] = json!({"mode":"full"});
                    for key in [
                        "selected_count",
                        "executed_count",
                        "population_count",
                        "reserved_query_count",
                    ] {
                        summary[key] = json!(1000);
                    }
                    summary["diagnostic_panel"] = json!(false);
                    summary["diagnostic_prefix"] = json!(false);
                    summary["population_percentiles_valid"] = json!(true);
                    summary["full_cohort_qualification"] = json!(false);
                }
                _ => (),
            }
        }
        for row in rows.iter_mut().filter(|r| r["phase"] == "query") {
            row["stages"]["leaf_peak_inflight"] = json!(parallelism);
        }
        rows
    }

    pub(super) fn paired_scale_config(dir: &Path, mut rows: [Vec<Value>; 2]) -> (PathBuf, String) {
        for r in &mut rows {
            authenticate(r);
        }
        let bytes = rows.each_ref().map(|r| encode(r));
        let (path, _) = paired_v2_config(dir, [&bytes[0], &bytes[1]], [&rows[0], &rows[1]]);
        let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["schema"] = json!("borsuk-scale-paired-native-reduction-config-v1");
        for arm in config["arms"].as_array_mut().unwrap() {
            arm["schema"] = json!(SCALE_CONFIG_SCHEMA);
        }
        let body = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &body).unwrap();
        (path, sha(&body))
    }

    fn byte_window_abba_fixture(dir: &Path) -> (Value, [Vec<Value>; 4]) {
        let base = paired_scale_fixture(64);
        let bound = &base[1];
        let mut native = json!({"schema":"borsuk-cohere-native-baseline-config-v7",
            "scratch_parent":dir.join("scratch")});
        for key in "dataset revision metric tie_rule corpus_intervals reserved_query_interval producer_authority corpus_source_first query_source_first rows dimensions count k profile backend generation_prefix generation_root_sha256 max_memory_bytes fetch_parallelism serving execution".split_whitespace() {
            native[key] = bound[key].clone();
        }
        for role in ["cohort_receipt", "derivation_receipt", "requests", "truth"] {
            native[role] = json!({"path":dir.join(role),"bytes":if matches!(role,"requests"|"truth") {bound[format!("{role}_bytes")].as_u64().unwrap()} else {1},
                "sha256":bound[format!("{role}_sha256")]});
        }
        native["native_source"] = json!({"source_sha256":bound["native_source_sha256"],
            "sq8_sha256":bound["native_sq8_sha256"],"source_order_sha256":bound["native_order_sha256"]});
        let mut candidate = native.clone();
        candidate["shared_byte_window"] =
            json!({"max_bytes":16*1024*1024,"max_waiters":64,"max_state_bytes":16384});
        let pins = [native, candidate]
            .iter()
            .enumerate()
            .map(|(i, value)| {
                let bytes = serde_json::to_vec(value).unwrap();
                let path = dir.join(format!("runtime-{i}.json"));
                std::fs::write(&path, &bytes).unwrap();
                json!({"path":path,"bytes":bytes.len(),"sha256":sha(&bytes)})
            })
            .collect::<Vec<_>>();
        let rows = std::array::from_fn(|index| {
            let arm = usize::from(index == 1 || index == 2);
            let mut rows = base.clone();
            rows[0]["config_sha256"] = pins[arm]["sha256"].clone();
            if arm == 1 {
                let window =
                    json!({"max_bytes":16*1024*1024,"max_waiters":64,"max_state_bytes":16384});
                rows[0]["shared_byte_window"] = window.clone();
                rows[1]["shared_byte_window"] = window;
                let startup = rows
                    .iter_mut()
                    .find(|row| row["phase"] == "startup")
                    .unwrap();
                startup["shared_byte_window_state_bytes"] = json!(4096);
            }
            let time = if arm == 0 { 1_000_000 } else { 990_000 };
            for row in rows.iter_mut().filter(|row| row["phase"] == "query") {
                row["query_wall_ns"] = json!(time);
                row["charges"]["source"]["verified_bytes"] = json!(20_000_000_u64);
                row["charges"]["source"]["submitted_gets"] = json!(2);
                row["sum"]["submitted_gets"] = json!(
                    ["router", "source", "sq8"]
                        .iter()
                        .map(|role| row["charges"][*role]["submitted_gets"].as_u64().unwrap())
                        .sum::<u64>()
                );
                row["sum"]["verified_bytes"] = json!(
                    ["router", "source", "sq8"]
                        .iter()
                        .map(|role| row["charges"][*role]["verified_bytes"].as_u64().unwrap())
                        .sum::<u64>()
                );
                let ordinal = row["ordinal"].as_u64().unwrap();
                for (key, completed) in [("before", ordinal), ("after", ordinal + 1)] {
                    let old = row["transport"][key]["consumed_payload_bytes"]
                        .as_u64()
                        .unwrap();
                    row["transport"][key]["consumed_payload_bytes"] =
                        json!(old + completed * 19_999_999);
                }
            }
            for row in rows.iter_mut().filter(|row| row["phase"] == "recall") {
                let hits = if row["ordinal"].as_u64().unwrap() < 780 {
                    10
                } else {
                    9
                };
                row["hits10"] = json!(hits);
                row["recall10"] = json!(hits as f64 / 10.0);
            }
            let terminal = &mut rows.last_mut().unwrap()["summary"];
            terminal["total_hits10"] = json!(9780);
            terminal["recall_numerator"] = json!(9780);
            terminal["mean_recall10"] = json!(0.978);
            terminal["charges"]["source"]["verified_bytes"] = json!(20_000_000_u64 * COUNT as u64);
            terminal["charges"]["source"]["submitted_gets"] = json!(2 * COUNT as u64);
            terminal["sum"]["submitted_gets"] = json!(
                ["router", "source", "sq8"]
                    .iter()
                    .map(|role| terminal["charges"][*role]["submitted_gets"]
                        .as_u64()
                        .unwrap())
                    .sum::<u64>()
            );
            terminal["sum"]["verified_bytes"] = json!(
                ["router", "source", "sq8"]
                    .iter()
                    .map(|role| terminal["charges"][*role]["verified_bytes"]
                        .as_u64()
                        .unwrap())
                    .sum::<u64>()
            );
            for (key, completed) in [("before", 999_u64), ("after", 1000)] {
                let old = terminal["transport_last_boundary"][key]["consumed_payload_bytes"]
                    .as_u64()
                    .unwrap();
                terminal["transport_last_boundary"][key]["consumed_payload_bytes"] =
                    json!(old + completed * 19_999_999);
            }
            let first = rows
                .iter_mut()
                .find(|row| row["phase"] == "query" && row["ordinal"] == 0)
                .unwrap();
            first["query_wall_ns"] = json!(time + 12345 + index as u64);
            rows.last_mut().unwrap()["summary"]["query_wall_ns"] =
                json!(time * COUNT as u64 + 12345 + index as u64);
            rows.last_mut().unwrap()["summary"]["process_wall_ns"] =
                json!(time * COUNT as u64 + 12345 + index as u64 + 100);
            rows.last_mut().unwrap()["summary"]["observed_process_peak_bytes"] =
                json!(12345 + index);
            rows
        });
        (
            json!({"schema":"borsuk-byte-window-abba-native-reduction-config-v1","runtime_configs":pins}),
            rows,
        )
    }

    fn write_byte_window_abba(
        dir: &Path,
        mut config: Value,
        rows: &[Vec<Value>; 4],
    ) -> (PathBuf, String) {
        config["runs"] = json!(rows.iter().enumerate().map(|(index, rows)| {
            let mut rows = rows.clone();
            authenticate(&mut rows);
            let bytes = encode(&rows);
            let path = dir.join(format!("byte-run-{index}.jsonl"));
            std::fs::write(&path, &bytes).unwrap();
            json!({"schema":SCALE_CONFIG_SCHEMA,"input":{"path":path,"bytes":bytes.len(),"sha256":sha(&bytes)},
                "expected_identity":rows[0],"expected_bound_inputs":rows[1]})
        }).collect::<Vec<_>>());
        let bytes = serde_json::to_vec(&config).unwrap();
        let path = dir.join("byte-abba.json");
        std::fs::write(&path, &bytes).unwrap();
        (path, sha(&bytes))
    }

    fn source_budget_abba_fixture(dir: &Path) -> (Value, [Vec<Value>; 4]) {
        let (mut config, mut runs) = byte_window_abba_fixture(dir);
        config["schema"] = json!("borsuk-source-budget-abba-native-reduction-config-v1");
        let pages = (0_usize..130).map(|n| 2 * n).collect::<Vec<_>>();
        let trace = json!({"ranked_candidate_pages":pages,"primary_page":0,
            "discoveries":[],"semantic_leaves":[0],
            "semantic_units":pages.iter().map(|p|p*8).collect::<Vec<_>>(),
            "semantic_seed_additions":[1,2,3,4,5,6,7],
            "nomination_evaluated_units":pages.iter().flat_map(|p|p*8..(p+1)*8).collect::<Vec<_>>()});
        for arm in 0..2 {
            let old_pin = &config["runtime_configs"][arm];
            let path = PathBuf::from(old_pin["path"].as_str().unwrap());
            let mut native: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            native.as_object_mut().unwrap().remove("shared_byte_window");
            native["schema"] = json!("borsuk-cohere-native-baseline-config-v8");
            native["source_get_budget"] = json!(if arm == 0 { 128 } else { 96 });
            let bytes = serde_json::to_vec(&native).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            config["runtime_configs"][arm] =
                json!({"path":path,"bytes":bytes.len(),"sha256":sha(&bytes)});
        }
        for (index, rows) in runs.iter_mut().enumerate() {
            let arm = usize::from(index == 1 || index == 2);
            let budget = if arm == 0 { 128 } else { 96 };
            let source_bytes = if arm == 0 { 8_921_088_u64 } else { 11_083_776 };
            rows[0]["schema"] = json!("borsuk-cohere-native-baseline-result-v8");
            rows[0]["config_sha256"] = config["runtime_configs"][arm]["sha256"].clone();
            for header in &mut rows[..2] {
                header.as_object_mut().unwrap().remove("shared_byte_window");
                header["source_get_budget"] = json!(budget);
            }
            let mut totals = Charges::default();
            let mut payload_delta = 0_i128;
            for row in rows.iter_mut().filter(|row| row["phase"] == "query") {
                let old = row["sum"]["verified_bytes"].as_u64().unwrap();
                row["trace"] = trace.clone();
                row["plan"] = json!({"selected_pages":[0],"ranges":[[0,265216]],
                    "planned_bytes":265216,"target_pages":1,"target_shortfall":0,
                    "primary_pages_retained":1,"covered_pages":1,"bridge_pages":0});
                row["charges"]["source"] =
                    json!({"submitted_gets":budget,"verified_bytes":source_bytes,"failed_gets":0});
                row["charges"]["sq8"] =
                    json!({"submitted_gets":1,"verified_bytes":265216,"failed_gets":0});
                let charge: Charges = serde_json::from_value(row["charges"].clone()).unwrap();
                let sum = charge.sum().unwrap();
                payload_delta = i128::from(sum.verified_bytes) - i128::from(old);
                row["sum"] = serde_json::to_value(sum).unwrap();
                totals.add(charge).unwrap();
                let ordinal = row["ordinal"].as_u64().unwrap();
                for (boundary, completed) in [("before", ordinal), ("after", ordinal + 1)] {
                    let old = row["transport"][boundary]["consumed_payload_bytes"]
                        .as_u64()
                        .unwrap();
                    let adjusted = i128::from(old) + i128::from(completed) * payload_delta;
                    row["transport"][boundary]["consumed_payload_bytes"] =
                        json!(u64::try_from(adjusted).unwrap());
                }
            }
            let terminal = &mut rows.last_mut().unwrap()["summary"];
            terminal["charges"] = serde_json::to_value(totals).unwrap();
            terminal["sum"] = serde_json::to_value(totals.sum().unwrap()).unwrap();
            for (boundary, completed) in [("before", 999_u64), ("after", 1000)] {
                let old = terminal["transport_last_boundary"][boundary]["consumed_payload_bytes"]
                    .as_u64()
                    .unwrap();
                terminal["transport_last_boundary"][boundary]["consumed_payload_bytes"] = json!(
                    u64::try_from(i128::from(old) + i128::from(completed) * payload_delta).unwrap()
                );
            }
            let startup = rows
                .iter_mut()
                .find(|row| row["phase"] == "startup")
                .unwrap();
            startup
                .as_object_mut()
                .unwrap()
                .remove("shared_byte_window_state_bytes");
        }
        (config, runs)
    }

    #[test]
    fn source_budget_abba_replays_distinct_cover_with_identical_quality_and_sq8() {
        let dir = tempfile::tempdir().unwrap();
        let (config, runs) = source_budget_abba_fixture(dir.path());
        let (path, pin) = write_byte_window_abba(dir.path(), config, &runs);
        let report = reduce_source_budget_abba(&path, &pin)
            .expect("four sealed SOURCE128/96 runs need authenticated cover authority");
        assert_eq!(report["runs"].as_array().unwrap().len(), 4);
        assert_eq!(report["source_cover_replay_verified"], true);
        assert_eq!(report["quality_trace_sq8_parity"], true);
        assert_eq!(report["distinct_source_cover_verified"], true);
        assert_eq!(report["recall_anchor_verified"], true);
        assert_eq!(report["vendor_win_claim"], false);
    }

    #[test]
    fn source_budget_abba_refuses_config_drift_before_results_and_wrong_late_cover() {
        for case in [
            "config-binding",
            "runtime-width",
            "header-window",
            "source-cover",
            "score-bits",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (mut config, mut rows) = source_budget_abba_fixture(dir.path());
            let expected = match case {
                "config-binding" => {
                    rows[1][0]["config_sha256"] = json!("0".repeat(64));
                    "SOURCE runtime/header binding"
                }
                "runtime-width" => {
                    let path =
                        PathBuf::from(config["runtime_configs"][1]["path"].as_str().unwrap());
                    let mut runtime: Value =
                        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                    runtime["fetch_parallelism"] = json!(128);
                    let bytes = serde_json::to_vec(&runtime).unwrap();
                    std::fs::write(&path, &bytes).unwrap();
                    config["runtime_configs"][1] =
                        json!({"path":path,"bytes":bytes.len(),"sha256":sha(&bytes)});
                    "SOURCE runtime version/budget/width/backend"
                }
                "header-window" => {
                    for run in &mut rows {
                        for header in &mut run[..2] {
                            header["shared_byte_window"] = json!({
                                "max_bytes": 9 * 1024 * 1024,
                                "max_waiters": 64,
                                "max_state_bytes": 16384
                            });
                        }
                    }
                    "SOURCE runtime/header binding"
                }
                "source-cover" => {
                    let query = rows[1]
                        .iter_mut()
                        .find(|q| q["phase"] == "query" && q["ordinal"] == 999)
                        .unwrap();
                    let bytes = query["charges"]["source"]["verified_bytes"]
                        .as_u64()
                        .unwrap();
                    query["charges"]["source"]["verified_bytes"] = json!(bytes + 1);
                    let charge: Charges = serde_json::from_value(query["charges"].clone()).unwrap();
                    query["sum"] = serde_json::to_value(charge.sum().unwrap()).unwrap();
                    "B1: loss SOURCE cover charges"
                }
                "score-bits" => {
                    let query = rows[1]
                        .iter_mut()
                        .find(|q| q["phase"] == "query" && q["ordinal"] == 999)
                        .unwrap();
                    let hit = query["returned"]
                        .as_array_mut()
                        .unwrap()
                        .last_mut()
                        .unwrap();
                    let bits = hit["score_bits"].as_u64().unwrap();
                    hit["score_bits"] = json!(bits + 1);
                    "B1 query 999: SOURCE quality/trace/SQ8 drift"
                }
                _ => unreachable!(),
            };
            let (path, pin) = write_byte_window_abba(dir.path(), config, &rows);
            if matches!(case, "config-binding" | "runtime-width" | "header-window") {
                for index in 0..4 {
                    std::fs::remove_file(dir.path().join(format!("byte-run-{index}.jsonl")))
                        .unwrap();
                }
            }
            assert_eq!(
                reduce_source_budget_abba(&path, &pin)
                    .unwrap_err()
                    .to_string(),
                expected,
                "{case}"
            );
        }
    }

    #[test]
    fn byte_window_abba_binds_four_runs_and_accepts_modest_improvement() {
        let dir = tempfile::tempdir().unwrap();
        let (config, rows) = byte_window_abba_fixture(dir.path());
        let (path, pin) = write_byte_window_abba(dir.path(), config, &rows);
        let report = reduce_byte_window_abba(&path, &pin).unwrap();
        assert_eq!(report["runs"].as_array().unwrap().len(), 4);
        assert_eq!(
            report["semantic_trace_physical_plan_logical_charge_parity"],
            true
        );
        assert_eq!(report["performance_screen_passed"], true);
        assert_eq!(report["execution_order_verified"], false);
        assert_eq!(report["source_binary_qualification_verified"], false);
        assert_eq!(report["vendor_win_claim"], false);
        assert_eq!(report["recall_anchor_verified"], true);
        assert_eq!(
            report["window_opportunities"][0]["potentially_binding_queries"],
            1000
        );
        assert_eq!(
            report["window_opportunities"][0]["potentially_binding_control_top_5_percent_queries"],
            50
        );
        assert_eq!(report["runs"][0]["first_query"]["query_wall_ns"], 1_012_345);
        for pair in report["comparisons"].as_array().unwrap() {
            assert_eq!(pair["p95_improved"], true);
            assert_eq!(pair["p95_at_least_5_percent_lower"], false);
        }
    }

    #[test]
    fn historical_byte_window_refuses_current_budget_headers_with_v7_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let (config, mut rows) = byte_window_abba_fixture(dir.path());
        for run in &mut rows {
            run[0]["schema"] = json!("borsuk-cohere-native-baseline-result-v8");
            run[0]["source_get_budget"] = json!(96);
            run[1]["source_get_budget"] = json!(96);
        }
        let (path, pin) = write_byte_window_abba(dir.path(), config, &rows);
        assert_eq!(
            reduce_byte_window_abba(&path, &pin)
                .unwrap_err()
                .to_string(),
            "byte-window historical v7 result required"
        );
    }

    #[test]
    fn byte_window_abba_review_anchor_and_noop_windows_do_not_pass() {
        for noop in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (mut config, mut rows) = byte_window_abba_fixture(dir.path());
            if noop {
                let path = PathBuf::from(config["runtime_configs"][1]["path"].as_str().unwrap());
                let mut native: Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                native["shared_byte_window"]["max_bytes"] = json!(64 * 1024 * 1024);
                let bytes = serde_json::to_vec(&native).unwrap();
                std::fs::write(&path, &bytes).unwrap();
                config["runtime_configs"][1]["bytes"] = json!(bytes.len());
                config["runtime_configs"][1]["sha256"] = json!(sha(&bytes));
                for index in [1, 2] {
                    rows[index][0]["config_sha256"] = json!(sha(&bytes));
                    for row in &mut rows[index][..2] {
                        row["shared_byte_window"] = native["shared_byte_window"].clone();
                    }
                }
            } else {
                for rows in &mut rows {
                    let recall = rows
                        .iter_mut()
                        .find(|row| row["phase"] == "recall" && row["ordinal"] == 779)
                        .unwrap();
                    recall["hits10"] = json!(9);
                    recall["recall10"] = json!(0.9);
                    let terminal = &mut rows.last_mut().unwrap()["summary"];
                    terminal["total_hits10"] = json!(9779);
                    terminal["recall_numerator"] = json!(9779);
                    terminal["mean_recall10"] = json!(0.9779);
                }
            }
            let (path, pin) = write_byte_window_abba(dir.path(), config, &rows);
            let report = reduce_byte_window_abba(&path, &pin).unwrap();
            assert_eq!(report["status"], "MEASURED");
            assert_eq!(report["performance_screen_passed"], false);
            assert_eq!(report["recall_anchor_verified"], noop);
            assert_eq!(
                report["window_opportunities"][0]["potentially_binding_queries"],
                if noop { 0 } else { 1000 }
            );
        }
    }

    #[test]
    fn byte_window_abba_review_failure_and_state_falsifiers() {
        for mutation in 0..4 {
            let dir = tempfile::tempdir().unwrap();
            let (config, mut rows) = byte_window_abba_fixture(dir.path());
            let expected = match mutation {
                0 => {
                    rows[3][1004]["transport"]["after"]["stream_failures"] = json!(1);
                    rows[3].last_mut().unwrap()["summary"]["transport_last_boundary"]["after"]["stream_failures"] =
                        json!(1);
                    "byte-window transport/logical GET failure"
                }
                1 => {
                    rows[2][1004]["stages"]["leaf_peak_inflight"] = json!(65);
                    "byte-window query leaf peak exceeds width64"
                }
                _ => {
                    let minimum =
                        borsuk::sq8_s3_range::InFlightBytes::modeled_state_bytes(64).unwrap();
                    rows[2][4]["shared_byte_window_state_bytes"] =
                        json!(if mutation == 2 { 1 } else { minimum - 1 });
                    "B2: byte-window startup state/config binding"
                }
            };
            let (path, pin) = write_byte_window_abba(dir.path(), config, &rows);
            assert_eq!(
                reduce_byte_window_abba(&path, &pin)
                    .unwrap_err()
                    .to_string(),
                expected,
                "mutation {mutation}"
            );
        }
    }

    #[test]
    fn byte_window_abba_legacy_width_modes_refuse_windowed_headers() {
        let dir = tempfile::tempdir().unwrap();
        let window = json!({"max_bytes":16*1024*1024,"max_waiters":64,"max_state_bytes":16384});
        let mut rows = [paired_scale_fixture(32), paired_scale_fixture(64)];
        for arm in &mut rows {
            for row in &mut arm[..2] {
                row["shared_byte_window"] = window.clone();
            }
        }
        let (path, _) = paired_scale_config(dir.path(), rows);
        let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["schema"] = json!("borsuk-scale-width64-native-reduction-config-v1");
        let bytes = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            reduce_paired_scope(&path, &sha(&bytes), true)
                .unwrap_err()
                .to_string(),
            "historical paired modes refuse byte-window intervention"
        );
    }

    #[test]
    fn byte_window_abba_refuses_resealed_header_and_query_mutations() {
        for mutation in 0..11 {
            let dir = tempfile::tempdir().unwrap();
            let (config, mut rows) = byte_window_abba_fixture(dir.path());
            match mutation {
                0 => rows[2][0]["binary_sha256"] = json!("a".repeat(64)),
                1 => rows[2][1]["source_cache"] = json!("on"),
                2 => rows[2][0]["fetch_parallelism"] = json!(32),
                3 => rows[2][1]["shared_byte_window"]["max_bytes"] = json!(1),
                4 => {
                    rows[2][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("shared_byte_window");
                }
                5 => rows[2][1]["shared_byte_window"]["max_waiters"] = json!(63),
                6 => rows[2][1]["shared_byte_window"]["extra"] = json!(true),
                7 => {
                    rows[2][1004]["returned"][0]["score_bits"] =
                        json!(rows[2][1004]["returned"][0]["score_bits"].as_u64().unwrap() ^ 1)
                }
                8 => rows[2][1004]["trace"]["primary_page"] = json!(1),
                9 => rows[2][1004]["plan"]["ranges"] = json!([[1, 101]]),
                _ => rows[2][4]["shared_byte_window_state_bytes"] = json!(16385),
            }
            let (path, pin) = write_byte_window_abba(dir.path(), config, &rows);
            assert!(
                reduce_byte_window_abba(&path, &pin).is_err(),
                "mutation {mutation}"
            );
        }
    }

    #[test]
    fn byte_window_abba_rejects_bad_runtime_window_before_result_open() {
        for (field, value) in [
            ("max_bytes", json!(0)),
            ("max_bytes", json!(536_870_913_u64)),
            ("max_bytes", json!(true)),
            ("max_waiters", json!(0)),
            ("max_waiters", json!(63)),
            ("max_waiters", json!(u64::MAX)),
            ("max_state_bytes", json!(0)),
            ("max_state_bytes", json!(1)),
            ("max_state_bytes", json!(536_870_913_u64)),
            ("extra", json!(true)),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (config, rows) = byte_window_abba_fixture(dir.path());
            let (path, _) = write_byte_window_abba(dir.path(), config, &rows);
            let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            for run in config["runs"].as_array().unwrap() {
                std::fs::remove_file(run["input"]["path"].as_str().unwrap()).unwrap();
            }
            let native_path = PathBuf::from(config["runtime_configs"][1]["path"].as_str().unwrap());
            let mut native: Value =
                serde_json::from_slice(&std::fs::read(&native_path).unwrap()).unwrap();
            native["shared_byte_window"][field] = value;
            let bytes = serde_json::to_vec(&native).unwrap();
            std::fs::write(&native_path, &bytes).unwrap();
            config["runtime_configs"][1]["sha256"] = json!(sha(&bytes));
            config["runtime_configs"][1]["bytes"] = json!(bytes.len());
            for index in [1, 2] {
                config["runs"][index]["expected_identity"]["config_sha256"] = json!(sha(&bytes));
                config["runs"][index]["expected_identity"]["shared_byte_window"] =
                    native["shared_byte_window"].clone();
                config["runs"][index]["expected_bound_inputs"]["shared_byte_window"] =
                    native["shared_byte_window"].clone();
            }
            let bytes = serde_json::to_vec(&config).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            let error = reduce_byte_window_abba(&path, &sha(&bytes))
                .unwrap_err()
                .to_string();
            let expected = if field == "extra" {
                "unknown/missing v2 field"
            } else if field == "max_bytes" && native["shared_byte_window"][field].is_boolean() {
                "byte-window capacity integer"
            } else {
                "byte-window explicit bounded capacity/waiters/state"
            };
            assert_eq!(error, expected, "{field}");
        }
    }

    #[test]
    fn byte_window_abba_requires_second_pair_and_distinct_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let (config, mut rows) = byte_window_abba_fixture(dir.path());
        for row in rows[2].iter_mut().filter(|row| row["phase"] == "query") {
            row["query_wall_ns"] = json!(1_100_000);
        }
        rows[2].last_mut().unwrap()["summary"]["query_wall_ns"] = json!(1_100_000 * COUNT as u64);
        rows[2].last_mut().unwrap()["summary"]["process_wall_ns"] =
            json!(1_100_000 * COUNT as u64 + 100);
        let (path, pin) = write_byte_window_abba(dir.path(), config, &rows);
        let report = reduce_byte_window_abba(&path, &pin).unwrap();
        assert_eq!(report["comparisons"][0]["timing_gate_passed"], true);
        assert_eq!(report["comparisons"][1]["timing_gate_passed"], false);
        assert_eq!(report["performance_screen_passed"], false);
        let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["runs"][3] = config["runs"][0].clone();
        let bytes = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            reduce_byte_window_abba(&path, &sha(&bytes))
                .unwrap_err()
                .to_string(),
            "duplicate byte-window run evidence"
        );
    }

    #[test]
    fn paired_scheduler_scale_accepts_only_explicit_same_width_source_delta() {
        let dir = tempfile::tempdir().unwrap();
        let a = paired_scale_fixture(32);
        let mut b = paired_scale_fixture(32);
        b[0]["config_sha256"] = json!("e".repeat(64));
        b[0]["binary_sha256"] = json!("a".repeat(64));
        b[0]["sq8_range_source_sha256"] = json!("b".repeat(64));
        let (path, _) = paired_scale_config(dir.path(), [a, b]);
        let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["schema"] = json!("borsuk-scale-scheduler-native-reduction-config-v1");
        let body = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &body).unwrap();
        let report = reduce_paired_scope(&path, &sha(&body), true).unwrap();
        assert_eq!(report["schema"], SCALE_SCHEDULER_SCHEMA);
        assert_eq!(report["semantic_parity"], true);
        assert_eq!(report["trace_plan_parity"], true);
        assert_eq!(report["logical_charge_parity"], true);
        assert_eq!(report["pooled_both_arms"]["statistics"]["count"], 2000);
        assert_eq!(report["performance_pass_claim"], false);
        assert!(reduce_paired(&path, &sha(&body)).is_err());
        for mutation in 0..9 {
            let mut bad = config.clone();
            let error = match mutation {
                0 | 1 => {
                    let field = if mutation == 0 {
                        "binary_sha256"
                    } else {
                        "sq8_range_source_sha256"
                    };
                    bad["arms"][1]["expected_identity"][field] =
                        bad["arms"][0]["expected_identity"][field].clone();
                    "scheduler requires distinct authenticated binary and fetcher pins"
                }
                2..=5 => {
                    let field = [
                        "runner_source_sha256",
                        "router_source_sha256",
                        "codec_source_sha256",
                        "returned_source_sha256",
                    ][mutation - 2];
                    let current = bad["arms"][1]["expected_identity"][field].clone();
                    let replacement = if current == json!("a".repeat(64)) {
                        "b"
                    } else {
                        "a"
                    };
                    bad["arms"][1]["expected_identity"][field] = json!(replacement.repeat(64));
                    "paired binary/source/benchmark/generation/backend/input/scoring pins differ"
                }
                6 => {
                    for arm in bad["arms"].as_array_mut().unwrap() {
                        arm["expected_identity"]["fetch_parallelism"] = json!(16);
                        arm["expected_bound_inputs"]["fetch_parallelism"] = json!(16);
                    }
                    "scheduler arms must both use integer fetch_parallelism 32"
                }
                7 => {
                    bad["arms"][1]["expected_bound_inputs"]["rows"] = json!(100000);
                    "paired scale full1M production pins"
                }
                _ => {
                    bad["arms"][1]["expected_bound_inputs"]["source_cache"] = json!("on");
                    "paired scale full1M production pins"
                }
            };
            for arm in bad["arms"].as_array_mut().unwrap() {
                arm["input"]["path"] = json!(dir.path().join("must-not-open"));
            }
            let bytes = serde_json::to_vec(&bad).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(
                reduce_paired_scope(&path, &sha(&bytes), true)
                    .unwrap_err()
                    .to_string(),
                error
            );
        }
    }

    #[test]
    fn paired_width128_scale_requires_same_source_and_ordered_widths() {
        let dir = tempfile::tempdir().unwrap();
        let a = paired_scale_fixture(64);
        let mut b = paired_scale_fixture(128);
        b[0]["config_sha256"] = json!("e".repeat(64));
        let (path, _) = paired_scale_config(dir.path(), [a, b]);
        let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["schema"] = json!("borsuk-scale-width128-native-reduction-config-v1");
        let bytes = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        let report = reduce_paired_scope(&path, &sha(&bytes), true).unwrap();
        assert_eq!(
            report["schema"],
            "borsuk-scale-width128-native-reduction-v1"
        );
        assert_eq!(report["semantic_parity"], true);
        assert_eq!(report["trace_plan_parity"], true);
        assert_eq!(report["logical_charge_parity"], true);
        assert_eq!(report["pooled_both_arms"]["statistics"]["count"], 2000);
        assert_eq!(report["performance_pass_claim"], false);
        assert!(reduce_paired(&path, &sha(&bytes)).is_err());
        let mut bad = config.clone();
        bad["arms"][1]["expected_identity"]["config_sha256"] =
            bad["arms"][0]["expected_identity"]["config_sha256"].clone();
        for arm in bad["arms"].as_array_mut().unwrap() {
            arm["input"]["path"] = json!(dir.path().join("must-not-open"));
        }
        let bytes = serde_json::to_vec(&bad).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            reduce_paired_scope(&path, &sha(&bytes), true)
                .unwrap_err()
                .to_string(),
            "paired native configs must have distinct SHA256"
        );
        for field in [
            "binary_sha256",
            "runner_source_sha256",
            "sq8_range_source_sha256",
            "router_source_sha256",
            "codec_source_sha256",
            "returned_source_sha256",
        ] {
            let mut bad = config.clone();
            bad["arms"][1]["expected_identity"][field] = json!("a".repeat(64));
            for arm in bad["arms"].as_array_mut().unwrap() {
                arm["input"]["path"] = json!(dir.path().join("must-not-open"));
            }
            let bytes = serde_json::to_vec(&bad).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(
                reduce_paired_scope(&path, &sha(&bytes), true)
                    .unwrap_err()
                    .to_string(),
                "paired binary/source/benchmark/generation/backend/input/scoring pins differ"
            );
        }
        for widths in [
            [32, 64],
            [64, 64],
            [128, 64],
            [128, 128],
            [64, 127],
            [64, 129],
        ] {
            let mut bad = config.clone();
            for (arm, width) in bad["arms"].as_array_mut().unwrap().iter_mut().zip(widths) {
                arm["expected_identity"]["fetch_parallelism"] = json!(width);
                arm["expected_bound_inputs"]["fetch_parallelism"] = json!(width);
                arm["input"]["path"] = json!(dir.path().join("must-not-open"));
            }
            let bytes = serde_json::to_vec(&bad).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(
                reduce_paired_scope(&path, &sha(&bytes), true)
                    .unwrap_err()
                    .to_string(),
                "width128 arms must be ordered integer fetch_parallelism 64 then 128"
            );
        }
    }

    #[test]
    fn paired_width64_scale_requires_same_source_and_ordered_widths() {
        let dir = tempfile::tempdir().unwrap();
        let a = paired_scale_fixture(32);
        let mut b = paired_scale_fixture(64);
        b[0]["config_sha256"] = json!("e".repeat(64));
        let (path, _) = paired_scale_config(dir.path(), [a, b]);
        let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["schema"] = json!("borsuk-scale-width64-native-reduction-config-v1");
        let bytes = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        let report = reduce_paired_scope(&path, &sha(&bytes), true).unwrap();
        assert_eq!(report["schema"], "borsuk-scale-width64-native-reduction-v1");
        assert_eq!(report["semantic_parity"], true);
        assert_eq!(report["trace_plan_parity"], true);
        assert_eq!(report["logical_charge_parity"], true);
        assert_eq!(report["pooled_both_arms"]["statistics"]["count"], 2000);
        assert_eq!(report["performance_pass_claim"], false);
        assert!(reduce_paired(&path, &sha(&bytes)).is_err());
        let mut bad = config.clone();
        bad["arms"][1]["expected_identity"]["config_sha256"] =
            bad["arms"][0]["expected_identity"]["config_sha256"].clone();
        for arm in bad["arms"].as_array_mut().unwrap() {
            arm["input"]["path"] = json!(dir.path().join("must-not-open"));
        }
        let bytes = serde_json::to_vec(&bad).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            reduce_paired_scope(&path, &sha(&bytes), true)
                .unwrap_err()
                .to_string(),
            "paired native configs must have distinct SHA256"
        );
        for field in [
            "binary_sha256",
            "runner_source_sha256",
            "sq8_range_source_sha256",
            "router_source_sha256",
            "codec_source_sha256",
            "returned_source_sha256",
        ] {
            let mut bad = config.clone();
            bad["arms"][1]["expected_identity"][field] = json!("a".repeat(64));
            for arm in bad["arms"].as_array_mut().unwrap() {
                arm["input"]["path"] = json!(dir.path().join("must-not-open"));
            }
            let bytes = serde_json::to_vec(&bad).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(
                reduce_paired_scope(&path, &sha(&bytes), true)
                    .unwrap_err()
                    .to_string(),
                "paired binary/source/benchmark/generation/backend/input/scoring pins differ"
            );
        }
        for widths in [[16, 32], [32, 32], [64, 32], [64, 64], [32, 63], [32, 65]] {
            let mut bad = config.clone();
            for (arm, width) in bad["arms"].as_array_mut().unwrap().iter_mut().zip(widths) {
                arm["expected_identity"]["fetch_parallelism"] = json!(width);
                arm["expected_bound_inputs"]["fetch_parallelism"] = json!(width);
                arm["input"]["path"] = json!(dir.path().join("must-not-open"));
            }
            let bytes = serde_json::to_vec(&bad).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(
                reduce_paired_scope(&path, &sha(&bytes), true)
                    .unwrap_err()
                    .to_string(),
                "width64 arms must be ordered integer fetch_parallelism 32 then 64"
            );
        }
    }

    #[test]
    fn paired_scale_accepts_full_sealed_population_and_preserves_historical_refusal() {
        let dir = tempfile::tempdir().unwrap();
        let (path, pin) = paired_scale_config(
            dir.path(),
            [paired_scale_fixture(16), paired_scale_fixture(32)],
        );
        let report = reduce_paired_scope(&path, &pin, true).unwrap();
        assert_eq!(report["schema"], SCALE_PAIRED_SCHEMA);
        assert_eq!(report["semantic_parity"], true);
        assert_eq!(report["trace_plan_parity"], true);
        assert_eq!(report["logical_charge_parity"], true);
        assert_eq!(report["pooled_both_arms"]["statistics"]["count"], 2000);
        assert_eq!(report["performance_pass_claim"], false);
        assert!(reduce_paired(&path, &pin).is_err());
    }

    #[test]
    fn paired_scale_refuses_resealed_last_query_score_and_trace_mutations() {
        for mutation in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let a = paired_scale_fixture(16);
            let mut b = paired_scale_fixture(32);
            if mutation == 2 {
                b[1004]["plan"]["ranges"] = json!([[100, 200]]);
            } else if mutation == 1 {
                b[1004]["trace"]["ranked_candidate_pages"] = json!([1, 0]);
            } else {
                let bits = b[1004]["returned"][0]["score_bits"].as_u64().unwrap();
                b[1004]["returned"][0]["score_bits"] = json!(bits ^ 1);
            }
            let (path, pin) = paired_scale_config(dir.path(), [a, b]);
            assert!(reduce_paired_scope(&path, &pin, true).is_err());
        }
    }

    #[test]
    fn paired_scale_wrong_population_cache_and_pins_refuse_before_input_open() {
        let dir = tempfile::tempdir().unwrap();
        let (path, _) = paired_scale_config(
            dir.path(),
            [paired_scale_fixture(16), paired_scale_fixture(32)],
        );
        let original: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        for (key, value) in [
            ("rows", json!(100000)),
            ("source_cache", json!("on")),
            ("truth_sha256", json!("0".repeat(64))),
            ("reserved_queries_sha256", json!("0".repeat(64))),
            (
                "execution",
                json!({"mode":"diagnostic_panel","ordinals":[0],"trace":false}),
            ),
        ] {
            let mut c = original.clone();
            for arm in c["arms"].as_array_mut().unwrap() {
                arm["expected_bound_inputs"][key] = value.clone();
                arm["input"]["path"] = json!(dir.path().join("must-not-open"));
            }
            let body = serde_json::to_vec(&c).unwrap();
            std::fs::write(&path, &body).unwrap();
            let error = reduce_paired_scope(&path, &sha(&body), true).unwrap_err();
            assert_eq!(
                error.to_string(),
                "paired scale full1M production pins",
                "{key}: {error}"
            );
        }
        let mut control = original;
        for (ordinal, arm) in control["arms"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
        {
            arm["input"]["path"] = json!(dir.path().join(format!("must-not-open-{ordinal}")));
        }
        let body = serde_json::to_vec(&control).unwrap();
        std::fs::write(&path, &body).unwrap();
        assert!(
            reduce_paired_scope(&path, &sha(&body), true)
                .unwrap_err()
                .downcast_ref::<rustix::io::Errno>()
                .is_some()
        );
    }

    #[test]
    fn paired_scale_refuses_selector_overrun_and_unsealed_tail_mutation() {
        for (arm, peak) in [(0, 17), (1, 33)] {
            let dir = tempfile::tempdir().unwrap();
            let mut fixtures = [paired_scale_fixture(16), paired_scale_fixture(32)];
            fixtures[arm][1004]["stages"]["leaf_peak_inflight"] = json!(peak);
            let (path, pin) = paired_scale_config(dir.path(), fixtures);
            assert_eq!(
                reduce_paired_scope(&path, &pin, true)
                    .unwrap_err()
                    .to_string(),
                "paired Scale1m query leaf_peak_inflight exceeds selector"
            );
        }
        let dir = tempfile::tempdir().unwrap();
        let a = paired_scale_fixture(16);
        let mut b = paired_scale_fixture(32);
        b[1004]["stages"]["leaf_peak_inflight"] = json!(33);
        let (path, pin) = paired_scale_config(dir.path(), [a, b]);
        assert_eq!(
            reduce_paired_scope(&path, &pin, true)
                .unwrap_err()
                .to_string(),
            "paired Scale1m query leaf_peak_inflight exceeds selector"
        );
        let (path, pin) = paired_scale_config(
            dir.path(),
            [paired_scale_fixture(16), paired_scale_fixture(32)],
        );
        let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let input = PathBuf::from(config["arms"][1]["input"]["path"].as_str().unwrap());
        let mut body = std::fs::read(&input).unwrap();
        let position = body.len() - 1;
        assert_eq!(body[position], b'\n');
        body.insert(position, b' ');
        config["arms"][1]["input"]["sha256"] = json!(sha(&body));
        config["arms"][1]["input"]["bytes"] = json!(body.len());
        let config_body = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &config_body).unwrap();
        std::fs::write(&input, &body).unwrap();
        assert!(reduce_paired_scope(&path, &sha(&config_body), true).is_ok());
        body[position] = b'\t';
        std::fs::write(&input, body).unwrap();
        assert_eq!(
            reduce_paired_scope(&path, &sha(&config_body), true)
                .unwrap_err()
                .to_string(),
            "full result SHA/EOF/identity"
        );
    }

    #[test]
    fn paired_scale_rechecks_both_artifacts_and_config_after_reading() {
        for replaced in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let (path, pin) = paired_scale_config(
                dir.path(),
                [paired_scale_fixture(16), paired_scale_fixture(32)],
            );
            let (c, _): (PairedConfig, _) = read_config(&path, &pin).unwrap();
            let original = file_identity(&open_input(&path).unwrap()).unwrap();
            let runs = c
                .arms
                .each_ref()
                .map(|a| read_run_with(&a.input.path, &a.input.sha256, Some(a)).unwrap());
            let identities = [&runs[0].file_identity, &runs[1].file_identity];
            recheck_scale_pair(&path, &pin, &original, &c, identities).unwrap();
            let target = if replaced == 2 {
                &path
            } else {
                &c.arms[replaced].input.path
            };
            let replacement = dir.path().join("replacement");
            std::fs::copy(target, &replacement).unwrap();
            std::fs::rename(replacement, target).unwrap();
            assert_eq!(
                recheck_scale_pair(&path, &pin, &original, &c, identities)
                    .unwrap_err()
                    .to_string(),
                if replaced == 2 {
                    "paired scale config pathname mutation"
                } else {
                    "authenticated parity pathname replacement"
                }
            );
        }
    }

    fn paired_v2_config(
        dir: &Path,
        bytes: [&[u8]; 2],
        expected: [&[Value]; 2],
    ) -> (PathBuf, String) {
        let arms: Vec<_> = bytes
            .into_iter()
            .zip(expected)
            .enumerate()
            .map(|(i, (bytes, rows))| {
                let path = dir.join(format!("paired-{i}.jsonl"));
                std::fs::write(&path, bytes).unwrap();
                json!({"schema":"borsuk-completed-native-reduction-config-v1",
                "input":{"path":path,"bytes":bytes.len(),"sha256":sha(bytes)},
                "expected_identity":rows[0],"expected_bound_inputs":rows[1]})
            })
            .collect();
        let body = serde_json::to_vec(
            &json!({"schema":"borsuk-paired-native-reduction-config-v1","arms":arms}),
        )
        .unwrap();
        let path = dir.join("paired-config.json");
        std::fs::write(&path, &body).unwrap();
        (path, sha(&body))
    }

    #[test]
    fn membership_abba_v2_parity_screens_and_refusals() {
        fn timings(rows: &mut [Value], times: &[u64]) {
            for (row, &time) in rows[5..1005].iter_mut().zip(times) {
                row["query_wall_ns"] = json!(time);
            }
            let total = times.iter().sum::<u64>();
            let terminal = &mut rows.last_mut().unwrap()["summary"];
            terminal["query_wall_ns"] = json!(total);
            terminal["process_wall_ns"] = json!(total + 100);
        }
        fn fixture() -> [Vec<Value>; 4] {
            let mut a = paired_v2_fixture(32);
            let mut b = a.clone();
            for field in [
                "binary_sha256",
                "generation_source_sha256",
                "router_source_sha256",
            ] {
                b[0][field] = json!("8".repeat(64));
            }
            b[4]["metadata"]["router_head_requests"] = json!(0);
            b[4]["metadata"]["router_head_wall_ns"] = json!(0);
            for row in &mut b[5..1005] {
                row["charges"]["router"] = charge(0);
                row["sum"] = charge(2);
                row["stages"]["leaf_peak_inflight"] = json!(0);
            }
            let t = &mut b.last_mut().unwrap()["summary"];
            t["charges"]["router"] = charge(0);
            t["sum"] = charge(2000);
            timings(&mut a, &[1_000_000; COUNT]);
            timings(&mut b, &[900_000; COUNT]);
            let mut runs = [a.clone(), b.clone(), b, a];
            for (index, rows) in runs.iter_mut().enumerate() {
                // Independent process observations make all four file identities
                // distinct without altering the common semantic/query authority.
                rows.last_mut().unwrap()["summary"]["observed_process_peak_bytes"] =
                    json!(12345 + index);
            }
            runs
        }
        fn seal(runs: &mut [Vec<Value>; 4]) {
            for rows in runs {
                authenticate(rows);
            }
        }
        fn config(dir: &Path, rows: &[Vec<Value>; 4]) -> (PathBuf, String) {
            let runs = rows
                .iter()
                .zip(LABELS)
                .map(|(rows, label)| {
                    let path = dir.join(format!("{label}.jsonl"));
                    let bytes = encode(rows);
                    std::fs::write(&path, &bytes).unwrap();
                    json!({"schema":"borsuk-completed-native-reduction-config-v1",
                    "input":{"path":path,"bytes":bytes.len(),"sha256":sha(&bytes)},
                    "expected_identity":rows[0],"expected_bound_inputs":rows[1]})
                })
                .collect::<Vec<_>>();
            let value = json!({"schema":"borsuk-membership-abba-native-reduction-config-v1",
                "runs":runs,"runtime_config_sha256":"3".repeat(64),
                "producer_source_commits":["1".repeat(40),"2".repeat(40)],
                "producer_source_archive_sha256":["3".repeat(64),"4".repeat(64)]});
            let bytes = serde_json::to_vec(&value).unwrap();
            let path = dir.join("membership-abba-config.json");
            std::fs::write(&path, &bytes).unwrap();
            (path, sha(&bytes))
        }
        let dir = tempfile::tempdir().unwrap();
        let mut runs = fixture();
        seal(&mut runs);
        let (path, pin) = config(dir.path(), &runs);
        let report = reduce_membership_abba(&path, &pin).unwrap();
        assert_eq!(report["schema"], MEMBERSHIP_ABBA_SCHEMA);
        assert_eq!(report["semantic_parity"], true);
        assert_eq!(report["nomination_trace_parity"], true);
        assert_eq!(report["source_sq8_charge_parity"], true);
        assert_eq!(report["candidate_router_charge_zero"], true);
        assert_eq!(
            report["removed_router_query_charges"],
            json!([charge(1000), charge(1000)])
        );
        assert_eq!(
            report["declared_run_order"],
            json!(["A1", "B1", "B2", "A2"])
        );
        assert_eq!(report["runs"].as_array().unwrap().len(), 4);
        for run in report["runs"].as_array().unwrap() {
            assert_eq!(run["statistics"]["count"], COUNT);
        }
        assert_eq!(report["performance_screen_passed"], true);
        assert_eq!(report["comparisons"][0]["comparison"], "B1-A1");
        assert_eq!(report["comparisons"][1]["comparison"], "B2-A2");
        for field in [
            "execution_order_verified",
            "producer_provenance_verified",
            "physical_range_parity_verified_by_reducer",
            "qualified",
            "supervisor_resources_cost_and_cache_qualified",
            "production_win_claim",
            "vendor_or_scientific_win_claim",
        ] {
            assert_eq!(report[field], false, "{field}");
        }
        let output = dir.path().join("membership-report");
        assert!(
            execute_report(&output, MEMBERSHIP_ABBA_SCHEMA, || reduce_membership_abba(
                &path, &pin
            ))
            .unwrap()
        );
        assert!(
            execute_report(&output, MEMBERSHIP_ABBA_SCHEMA, || panic!(
                "occupied output"
            ))
            .is_err()
        );

        // Both pairs must pass. Exercise each integer threshold independently
        // in the second pair, including exact QPS equality and a 1ns regression.
        for case in 0..6 {
            let mut runs = fixture();
            let mut candidate = vec![950_000; COUNT];
            match case {
                0 => (),
                1 => candidate.fill(950_001),
                2 | 3 => {
                    candidate[950..].fill(1_950_000);
                    if case == 3 {
                        candidate[999] += 1;
                    }
                }
                4 => {
                    let mut control = vec![1_000_000; COUNT];
                    control[900..].fill(2_000_000);
                    timings(&mut runs[3], &control);
                    candidate[..900].fill(950_001);
                    candidate[900..].fill(1_800_000);
                }
                _ => {
                    candidate[..900].fill(900_000);
                    candidate[900..].fill(950_001);
                }
            }
            timings(&mut runs[2], &candidate);
            seal(&mut runs);
            let (path, pin) = config(dir.path(), &runs);
            let report = reduce_membership_abba(&path, &pin).unwrap();
            let pair = &report["comparisons"][1];
            assert_eq!(report["comparisons"][0]["timing_gate_passed"], true);
            assert_eq!(
                report["performance_screen_passed"],
                matches!(case, 0 | 2),
                "case {case}"
            );
            assert_eq!(
                pair["p90_at_least_5_percent_lower"],
                !matches!(case, 1 | 4),
                "case {case}"
            );
            assert_eq!(
                pair["p95_at_least_5_percent_lower"],
                !matches!(case, 1 | 5),
                "case {case}"
            );
            assert_eq!(
                pair["sequential_qps_no_regression"],
                case != 3,
                "case {case}"
            );
            assert!(
                pair.get("sequential_qps_at_least_5_percent_higher")
                    .is_none()
            );
            if case == 2 {
                assert_eq!(pair["sequential_qps_ratio"], 1.0);
            }
            if case == 1 {
                runs.swap(0, 3);
                runs.swap(1, 2);
                let (path, pin) = config(dir.path(), &runs);
                let report = reduce_membership_abba(&path, &pin).unwrap();
                assert_eq!(report["comparisons"][0]["timing_gate_passed"], false);
                assert_eq!(report["comparisons"][1]["timing_gate_passed"], true);
                assert_eq!(report["performance_screen_passed"], false);
            }
        }
        for case in 0..34 {
            let mut runs = fixture();
            let b = &mut runs[2];
            match case {
                // Last ordinal in B2 must be checked, even with a valid first pair.
                0 => b[1004]["returned"][9]["id"] = json!(11),
                1 => b[1004]["returned"][9]["score_bits"] = json!(0x4000_0001_u32),
                2 => b[1004]["trace"]["ranked_candidate_pages"] = json!([1, 0]),
                3 => {
                    b[2005]["hits10"] = json!(8);
                    b[2005]["recall10"] = json!(0.8);
                    let t = &mut b.last_mut().unwrap()["summary"];
                    t["total_hits10"] = json!(8999);
                    t["recall_numerator"] = json!(8999);
                    t["mean_recall10"] = json!(0.8999);
                }
                4..=6 => {
                    let stage = ["source", "sq8", "router"][case - 4];
                    for key in ["submitted_gets", "verified_bytes"] {
                        b[1004]["charges"][stage][key] =
                            json!(b[1004]["charges"][stage][key].as_u64().unwrap() + 1);
                        b[1004]["sum"][key] = json!(b[1004]["sum"][key].as_u64().unwrap() + 1);
                        let t = &mut b.last_mut().unwrap()["summary"];
                        t["charges"][stage][key] =
                            json!(t["charges"][stage][key].as_u64().unwrap() + 1);
                        t["sum"][key] = json!(t["sum"][key].as_u64().unwrap() + 1);
                    }
                }
                7 => b[1004]["stages"]["leaf_peak_inflight"] = json!(1),
                8..=11 => {
                    b[0][[
                        "runner_source_sha256",
                        "codec_source_sha256",
                        "source_plane_source_sha256",
                        "config_sha256",
                    ][case - 8]] = json!("7".repeat(64))
                }
                12 => b[1]["generation_root_sha256"] = json!("7".repeat(64)),
                13 => b[1]["backend"]["sq8_etag"] = json!("other"),
                14 => {
                    b[0]["fetch_parallelism"] = json!(16);
                    b[1]["fetch_parallelism"] = json!(16);
                }
                15 => b[1]["source_cache"] = json!("on"),
                16 => b.last_mut().unwrap()["summary"]["complete"] = json!(false),
                17 => {
                    b.remove(1004);
                }
                18 => b[4]["metadata"]["router_head_requests"] = json!(1),
                19 => b[4]["metadata"]["router_head_wall_ns"] = json!(1),
                20 => b[1004]["charges"]["source"]["failed_gets"] = json!(1),
                27 => runs[3][0]["binary_sha256"] = json!("7".repeat(64)),
                28 => {
                    for index in [1, 2] {
                        runs[index][0]["runner_source_sha256"] = json!("7".repeat(64));
                    }
                }
                30 => {
                    for rows in &mut runs {
                        rows[0]["config_sha256"] = json!("6".repeat(64));
                    }
                }
                32 => {
                    b[2]["charges"]["verified_bytes"] = json!(201);
                    b.last_mut().unwrap()["summary"]["binding_charge"]["verified_bytes"] =
                        json!(201);
                }
                33 => runs[3][1004]["trace"]["semantic_units"] = json!([0, 7]),
                _ => (),
            }
            seal(&mut runs);
            if case == 21 {
                runs[2][1005]["prefix_sha256"] = json!("0".repeat(64));
            }
            if case == 31 {
                runs[2].pop();
            }
            let (path, mut pin) = config(dir.path(), &runs);
            if matches!(case, 22..=26 | 29) {
                let mut value: Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                match case {
                    22 => {
                        value["producer_source_commits"][1] =
                            value["producer_source_commits"][0].clone()
                    }
                    23 => {
                        // Both expected candidate pins agree but don't match either file.
                        for index in [1, 2] {
                            value["runs"][index]["expected_identity"]["binary_sha256"] =
                                json!("5".repeat(64));
                        }
                    }
                    24 => value["selector"] = json!("allow_other_delta"),
                    25 | 26 => {
                        let first = value["runs"][0]["input"].clone();
                        if case == 25 {
                            value["runs"][3]["input"] = first;
                        } else {
                            // A copied file has a distinct inode but reused bytes.
                            std::fs::copy(dir.path().join("A1.jsonl"), dir.path().join("A2.jsonl"))
                                .unwrap();
                            value["runs"][3]["input"]["sha256"] = first["sha256"].clone();
                            value["runs"][3]["input"]["bytes"] = first["bytes"].clone();
                        }
                    }
                    _ => value["runtime_config_sha256"] = json!("6".repeat(64)),
                }
                let bytes = serde_json::to_vec(&value).unwrap();
                pin = sha(&bytes);
                std::fs::write(&path, bytes).unwrap();
            }
            let error = reduce_membership_abba(&path, &pin).unwrap_err().to_string();
            if case <= 6 {
                assert!(
                    error.contains("B2 membership ABBA query 999:"),
                    "case {case}: {error}"
                );
            }
            if case == 33 {
                assert!(error.contains("A2 membership ABBA query 999:"), "{error}");
            }
            if matches!(case, 25 | 26) {
                assert!(
                    error.contains("duplicate membership ABBA run evidence"),
                    "{error}"
                );
            }
        }
    }
    #[test]
    fn paired_v2_sealed_native_rows_statistics_and_create_only() {
        let dir = tempfile::tempdir().unwrap();
        let mut a = paired_v2_fixture(16);
        let mut b = paired_v2_fixture(32);
        authenticate(&mut a);
        authenticate(&mut b);
        let (config, pin) = paired_v2_config(dir.path(), [&encode(&a), &encode(&b)], [&a, &b]);
        let report = reduce_paired(&config, &pin).unwrap();
        for (index, first, p95, total, sq8) in [
            (0, 1_000_000, 950.0, 500_500_000_000_u64, 500500),
            (1, 2_000_000, 1900.0, 1_001_000_000_000_u64, 501500),
        ] {
            let arm = &report["arms"][index];
            assert_eq!(arm["first_query"]["query_wall_ns"], first);
            assert_eq!(arm["statistics"]["count"], 1000);
            assert_eq!(arm["statistics"]["p95_ms"], p95);
            assert_eq!(arm["statistics"]["query_wall_ns"], total);
            assert_eq!(arm["stage_wall_sums"]["sq8_ns"], sq8);
        }
        let pooled = &report["pooled_both_arms"];
        assert_eq!(pooled["statistics"]["count"], 2000);
        assert_eq!(pooled["statistics"]["p50_ms"], 667.0);
        assert_eq!(pooled["statistics"]["p90_ms"], 1600.0);
        assert_eq!(pooled["statistics"]["p95_ms"], 1800.0);
        assert_eq!(pooled["statistics"]["p99_ms"], 1960.0);
        assert_eq!(pooled["statistics"]["query_wall_ns"], 1_501_500_000_000_u64);
        assert!(
            (pooled["statistics"]["sequential_qps"].as_f64().unwrap() - 1.332001332001332).abs()
                < 1e-12
        );
        assert_eq!(pooled["stage_wall_sums"]["sq8_ns"], 1_002_000);
        assert_eq!(report["trace_plan_parity"], true);
        assert_eq!(report["logical_charge_parity"], true);
        assert_eq!(report["performance_pass_claim"], false);
        assert_eq!(report["qualified"], false);
        let output = dir.path().join("paired-report");
        assert!(execute_report(&output, PAIRED_SCHEMA, || reduce_paired(&config, &pin)).unwrap());
        let original = std::fs::read(&output).unwrap();
        assert!(
            execute_report(&output, PAIRED_SCHEMA, || panic!(
                "occupied output invoked reducer"
            ))
            .is_err()
        );
        assert_eq!(std::fs::read(&output).unwrap(), original);
        // The single completed mode also admits the exact new native fields.
        let (single, pin) = v2_config(dir.path(), &encode(&b), &b);
        assert_eq!(
            reduce_completed(&single, &pin).unwrap()["statistics"]["p95_ms"],
            1900.0
        );
    }

    #[test]
    fn paired_v2_rejects_authenticated_semantic_plan_and_charge_changes() {
        let dir = tempfile::tempdir().unwrap();
        let mut a = paired_v2_fixture(16);
        authenticate(&mut a);
        let a_bytes = encode(&a);
        // Each mutation remains a valid, re-sealed single run: paired parity,
        // rather than stale checksums or inconsistent terminal sums, must fail.
        for case in 0..14 {
            let mut b = paired_v2_fixture(32);
            match case {
                0 => b[5]["returned"][9]["id"] = json!(11),
                1 => b[5]["returned"][9]["score_bits"] = json!(0x4040_0000_u32),
                2 => {
                    b[1006]["hits10"] = json!(8);
                    b[1006]["recall10"] = json!(0.8);
                    let t = &mut b.last_mut().unwrap()["summary"];
                    t["total_hits10"] = json!(8999);
                    t["recall_numerator"] = json!(8999);
                    t["mean_recall10"] = json!(0.8999);
                }
                3..=9 => {
                    let key = [
                        "ranked_candidate_pages",
                        "nomination_evaluated_units",
                        "primary_page",
                        "discoveries",
                        "semantic_leaves",
                        "semantic_units",
                        "semantic_seed_additions",
                    ][case - 3];
                    match key {
                        "primary_page" => b[5]["trace"][key] = json!(1),
                        "discoveries" => {
                            b[5]["trace"][key][0]["seed_evaluated_units"] = json!([1, 2])
                        }
                        _ => {
                            let ordered = b[5]["trace"][key].as_array_mut().unwrap();
                            if ordered.len() > 1 {
                                ordered.reverse();
                            } else {
                                ordered.push(json!(7));
                            }
                        }
                    }
                }
                10 | 11 => {
                    let key = if case == 10 {
                        "submitted_gets"
                    } else {
                        "verified_bytes"
                    };
                    b[5]["charges"]["source"][key] =
                        json!(b[5]["charges"]["source"][key].as_u64().unwrap() + 1);
                    b[5]["sum"][key] = json!(b[5]["sum"][key].as_u64().unwrap() + 1);
                    let t = &mut b.last_mut().unwrap()["summary"];
                    t["charges"]["source"][key] =
                        json!(t["charges"]["source"][key].as_u64().unwrap() + 1);
                    t["sum"][key] = json!(t["sum"][key].as_u64().unwrap() + 1);
                }
                13 => b[1004]["returned"][9]["id"] = json!(11),
                _ => {
                    b[5]["returned"].as_array_mut().unwrap().pop();
                    b[5]["returned_count"] = json!(9);
                    b[5]["underfill"] = json!(true);
                    b[1006]["returned_count"] = json!(9);
                    b[1006]["underfill"] = json!(true);
                    b.last_mut().unwrap()["summary"]["underfilled_queries"] = json!(1);
                }
            }
            authenticate(&mut b);
            let b_bytes = encode(&b);
            let (single, single_pin) = v2_config(dir.path(), &b_bytes, &b);
            assert!(
                reduce_completed(&single, &single_pin).is_ok(),
                "valid single case {case}"
            );
            let (config, pin) = paired_v2_config(dir.path(), [&a_bytes, &b_bytes], [&a, &b]);
            let error = reduce_paired(&config, &pin).unwrap_err().to_string();
            let ordinal = if case == 13 { 999 } else { 0 };
            assert!(
                error.contains(&format!("paired query {ordinal}:")),
                "case {case}: {error}"
            );
        }
    }

    #[test]
    fn paired_v2_rejects_pins_selectors_unknowns_and_failures() {
        let dir = tempfile::tempdir().unwrap();
        let mut a = paired_v2_fixture(16);
        authenticate(&mut a);
        let a_bytes = encode(&a);
        for case in 0..25 {
            let mut b = paired_v2_fixture(32);
            match case {
                0..=5 => {
                    let key = [
                        "binary_sha256",
                        "runner_source_sha256",
                        "generation_source_sha256",
                        "router_source_sha256",
                        "codec_source_sha256",
                        "source_plane_source_sha256",
                    ][case];
                    b[0][key] = json!("2".repeat(64));
                }
                6 => b[1]["backend"]["sq8_etag"] = json!("different-etag"),
                7 => b[1]["generation_root_sha256"] = json!("2".repeat(64)),
                8 => b[0]["config_sha256"] = a[0]["config_sha256"].clone(),
                9 => b[1]["fetch_parallelism"] = json!(16),
                10..=14 => {
                    let v = [
                        json!(17),
                        json!(32.0),
                        json!("32"),
                        json!(true),
                        Value::Null,
                    ][case - 10]
                        .clone();
                    b[0]["fetch_parallelism"] = v.clone();
                    b[1]["fetch_parallelism"] = v;
                }
                15 => b[1]["source_cache"] = json!("on"),
                16 => b[5]["ignored"] = json!(1),
                17 => b[5]["stages"]["discovery"]["ignored"] = json!(1),
                18 => b[5]["trace"]["discoveries"][0]["ignored"] = json!(1),
                19 => b[0]["ignored"] = json!(1),
                20 => b[2]["success"] = json!(false),
                21 => b.last_mut().unwrap()["summary"]["status"] = json!("INVALID"),
                22 => {
                    // Internally consistent native counters still disclose a
                    // failed body; no timing claim may use this paired run.
                    b[1004]["transport"]["after"]["stream_failures"] = json!(1);
                    b.last_mut().unwrap()["summary"]["transport_last_boundary"]["after"]["stream_failures"] =
                        json!(1);
                }
                23 => {
                    b[0].as_object_mut().unwrap().remove("fetch_parallelism");
                    b[1].as_object_mut().unwrap().remove("fetch_parallelism");
                    b[1].as_object_mut().unwrap().remove("source_cache");
                }
                _ => b[1004]["stages"]["leaf_peak_inflight"] = json!(17),
            }
            authenticate(&mut b);
            if case == 24 {
                let (config, pin) = v2_config(dir.path(), &encode(&b), &b);
                assert!(reduce_completed(&config, &pin).is_ok());
            }
            let (config, pin) = paired_v2_config(dir.path(), [&a_bytes, &encode(&b)], [&a, &b]);
            let output = dir.path().join(format!("invalid-pair-{case}"));
            assert!(
                !execute_report(&output, PAIRED_SCHEMA, || reduce_paired(&config, &pin)).unwrap(),
                "case {case}"
            );
            let report: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
            assert_eq!(report["status"], "INVALID");
            assert_eq!(report["complete"], false);
            if case == 24 {
                assert_eq!(
                    report["error"],
                    "paired Native100k query leaf_peak_inflight exceeds 16"
                );
            }
        }
        // Individually valid new rows must agree on the selector even outside
        // paired mode; old v2 rows remain covered by completed-mode fixtures.
        let mut b = paired_v2_fixture(32);
        b[1]["fetch_parallelism"] = json!(16);
        authenticate(&mut b);
        let (config, pin) = v2_config(dir.path(), &encode(&b), &b);
        assert_eq!(
            reduce_completed(&config, &pin).unwrap_err().to_string(),
            "identity/input fetch_parallelism agreement"
        );

        // Both files and their expected rows agree on each wrong benchmark pin.
        // Re-seal and rehash them so the failure cannot be blamed on stale pins.
        for (key, wrong) in [
            ("rows", json!(99999)),
            ("dimensions", json!(16)),
            ("count", json!(999)),
            ("k", json!(9)),
            ("dataset", json!("synthetic/native-fixture")),
            ("revision", json!("fixture-revision")),
            ("metric", json!("dot")),
            ("tie_rule", json!("corpus_ordinal_descending")),
            ("profile", json!("fresh1m")),
            ("corpus_source_first", json!(1)),
            ("query_source_first", json!(100001)),
            ("requests_bytes", json!(4096004)),
            ("truth_bytes", json!(80008)),
            ("requests_sha256", json!("0".repeat(64))),
            ("truth_sha256", json!("0".repeat(64))),
            ("native_source_sha256", json!("0".repeat(64))),
            ("native_sq8_sha256", json!("0".repeat(64))),
            ("native_order_sha256", json!("0".repeat(64))),
        ] {
            let [mut a, mut b] = [paired_v2_fixture(16), paired_v2_fixture(32)];
            for rows in [&mut a, &mut b] {
                rows[1][key] = wrong.clone();
                if matches!(key, "dimensions" | "count" | "k") {
                    let i = &mut rows[1];
                    i["requests_bytes"] =
                        json!(i["count"].as_u64().unwrap() * i["dimensions"].as_u64().unwrap() * 4);
                    i["truth_bytes"] =
                        json!(i["count"].as_u64().unwrap() * i["k"].as_u64().unwrap() * 8);
                }
                rows[1]["backend"]["sq8_object_key"] = json!(format!(
                    "fixture/objects/{}",
                    rows[1]["native_sq8_sha256"].as_str().unwrap()
                ));
                rows[1005]["requests_sha256"] = rows[1]["requests_sha256"].clone();
                for pin in ["requests_sha256", "truth_sha256"] {
                    rows.last_mut().unwrap()["summary"][pin] = rows[1][pin].clone();
                }
                authenticate(rows);
            }
            let (config, pin) = paired_v2_config(dir.path(), [&encode(&a), &encode(&b)], [&a, &b]);
            assert_eq!(
                reduce_paired(&config, &pin).unwrap_err().to_string(),
                "paired production benchmark pins",
                "wrong {key}"
            );
            // The same error with no result files proves admission precedes open.
            for name in ["paired-0.jsonl", "paired-1.jsonl"] {
                std::fs::remove_file(dir.path().join(name)).unwrap();
            }
            assert_eq!(
                reduce_paired(&config, &pin).unwrap_err().to_string(),
                "paired production benchmark pins",
                "pre-open {key}"
            );
        }
    }

    #[test]
    fn paired_v2_rejects_duplicate_truncated_unsealed_and_unpinned_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut a = paired_v2_fixture(16);
        let mut b = paired_v2_fixture(32);
        authenticate(&mut a);
        authenticate(&mut b);
        let a_bytes = encode(&a);
        let b_bytes = encode(&b);
        for case in 0..10 {
            let mut rows = b.clone();
            match case {
                0 => rows[1005]["prefix_sha256"] = json!("0".repeat(64)),
                1 => rows.last_mut().unwrap()["summary"]["sealed_bytes"] = json!(1),
                2 => rows[1005]["requires_successful_sync"] = json!(false),
                3 => rows[1005]["requires_successful_directory_sync"] = json!(false),
                _ => (),
            }
            let mut bytes = encode(&rows);
            match case {
                4 => {
                    bytes.pop();
                }
                5 => bytes.truncate(bytes.len() / 2),
                6 => bytes.extend_from_slice(b"{}\n"),
                7 => {
                    bytes = String::from_utf8(bytes)
                        .unwrap()
                        .replacen(
                            "\"fetch_parallelism\":32",
                            "\"fetch_parallelism\":32,\"fetch_parallelism\":32",
                            1,
                        )
                        .into_bytes();
                }
                8 => {
                    bytes = String::from_utf8(bytes)
                        .unwrap()
                        .replacen("\"seed_page\":0", "\"seed_page\":0,\"seed_page\":0", 1)
                        .into_bytes();
                }
                9 => {
                    bytes = encode(&rows[..1005]);
                }
                _ => (),
            }
            let (config, pin) = paired_v2_config(dir.path(), [&a_bytes, &bytes], [&a, &b]);
            let error = reduce_paired(&config, &pin).unwrap_err().to_string();
            if matches!(case, 7 | 8) {
                assert!(
                    error.contains("duplicate JSON key"),
                    "file case {case}: {error}"
                );
            }
        }
        for case in 0..6 {
            let (config, pin) = paired_v2_config(dir.path(), [&a_bytes, &b_bytes], [&a, &b]);
            let original = std::fs::read(&config).unwrap();
            let mut value: Value = serde_json::from_slice(&original).unwrap();
            match case {
                0 => value["arms"][1]["input"]["bytes"] = json!(1),
                1 => value["arms"][1]["input"]["sha256"] = json!("0".repeat(64)),
                2 => value["ignored"] = json!(true),
                3 => value["arms"][1]["expected_identity"]["config_sha256"] = json!("7".repeat(64)),
                _ => (),
            }
            let mut bytes = serde_json::to_vec(&value).unwrap();
            if case == 4 {
                bytes = String::from_utf8(bytes)
                    .unwrap()
                    .replacen("\"arms\":", "\"arms\":[],\"arms\":", 1)
                    .into_bytes();
            }
            std::fs::write(&config, &bytes).unwrap();
            let config_sha = if case == 5 {
                "0".repeat(64)
            } else {
                sha(&bytes)
            };
            let error = reduce_paired(&config, &config_sha).unwrap_err().to_string();
            if case == 4 {
                assert!(
                    error.contains("duplicate JSON key"),
                    "config duplicate: {error}"
                );
            }
            if case == 5 {
                assert!(reduce_paired(&config, &pin).is_ok());
                let mut tampered = b_bytes.clone();
                tampered.extend_from_slice(b" ");
                std::fs::write(dir.path().join("paired-1.jsonl"), &tampered).unwrap();
                assert!(reduce_paired(&config, &pin).is_err());
            }
        }
    }

    #[test]
    fn completed_v2_quantiles_stages_and_create_only_report() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = v2_fixture();
        authenticate(&mut rows);
        let (config, config_sha) = v2_config(dir.path(), &encode(&rows), &rows);
        let report = reduce_completed(&config, &config_sha).unwrap();
        assert_eq!(report["statistics"]["p50_ms"], 500.0);
        assert_eq!(report["statistics"]["p90_ms"], 900.0);
        assert_eq!(report["statistics"]["p95_ms"], 950.0);
        assert_eq!(report["statistics"]["p99_ms"], 990.0);
        assert!(
            (report["statistics"]["sequential_qps"].as_f64().unwrap() - 1.998001998001998).abs()
                < 1e-12
        );
        for stage in ["discovery", "source", "sq8"] {
            assert_eq!(report["stage_statistics"][stage]["total_ns"], 500500);
            for (p, n) in [
                ("p50_ns", 500),
                ("p90_ns", 900),
                ("p95_ns", 950),
                ("p99_ns", 990),
            ] {
                assert_eq!(report["stage_statistics"][stage][p], n);
            }
            assert!(
                report["stage_statistics"][stage]
                    .get("sequential_qps")
                    .is_none()
            );
        }
        assert_eq!(report["stage_statistics"]["planning"]["total_ns"], 0);
        assert_eq!(report["stage_statistics"]["planning"]["p99_ns"], 0);
        assert_eq!(report["unattributed_query_wall_ns"], 500498498500_u64);
        assert_eq!(report["native_terminal"]["recall_numerator"], 9000);
        assert_eq!(report["native_terminal"]["sum"]["submitted_gets"], 3000);
        assert_eq!(
            report["native_terminal"]["transport_last_boundary"]["after"]["attempts"],
            3005
        );
        assert!(report["billed_requests"].is_null());
        assert_eq!(report["performance_pass_claim"], false);
        assert_eq!(report["qualified"], false);
        assert_eq!(
            report["startup"]["metadata"]["metadata"],
            rows[4]["metadata"]["metadata"]
        );
        let output = dir.path().join("completed-report");
        assert!(
            execute_report(&output, COMPLETED_SCHEMA, || reduce_completed(
                &config,
                &config_sha
            ))
            .unwrap()
        );
        let original = std::fs::read(&output).unwrap();
        assert!(
            execute_report(&output, COMPLETED_SCHEMA, || reduce_completed(
                &config,
                &config_sha
            ))
            .is_err()
        );
        assert_eq!(std::fs::read(output).unwrap(), original);
        assert!(read_run(&dir.path().join("completed-v2.jsonl"), &sha(&encode(&rows))).is_err());

        // A complete weak-quality result remains MEASURED, with the fixed k10
        // denominator. Its three returned IDs contain two authenticated hits.
        rows[5]["returned"].as_array_mut().unwrap().truncate(3);
        rows[5]["returned_count"] = json!(3);
        rows[5]["underfill"] = json!(true);
        rows[1006]["returned_count"] = json!(3);
        rows[1006]["underfill"] = json!(true);
        rows[1006]["hits10"] = json!(2);
        rows[1006]["recall10"] = json!(0.2);
        let terminal = &mut rows.last_mut().unwrap()["summary"];
        terminal["total_hits10"] = json!(8993);
        terminal["recall_numerator"] = json!(8993);
        terminal["mean_recall10"] = json!(0.8993);
        terminal["underfilled_queries"] = json!(1);
        authenticate(&mut rows);
        let (config, pin) = v2_config(dir.path(), &encode(&rows), &rows);
        let underfilled = reduce_completed(&config, &pin).unwrap();
        assert_eq!(underfilled["status"], "MEASURED");
        assert_eq!(underfilled["complete"], true);
        assert_eq!(underfilled["native_terminal"]["underfilled_queries"], 1);
        assert_eq!(underfilled["native_terminal"]["recall_numerator"], 8993);
        assert_eq!(underfilled["native_terminal"]["recall_denominator"], 10000);
        assert_eq!(underfilled["native_terminal"]["mean_recall10"], 0.8993);
        assert_eq!(underfilled["statistics"], report["statistics"]);
        let mut historical = fixture(false, 1_000_000);
        historical[3]["returned"]
            .as_array_mut()
            .unwrap()
            .truncate(3);
        historical[3]["returned_count"] = json!(3);
        historical[3]["underfill"] = json!(true);
        historical[1004] = rows[1006].clone();
        for key in [
            "total_hits10",
            "recall_numerator",
            "mean_recall10",
            "underfilled_queries",
        ] {
            historical.last_mut().unwrap()["summary"][key] =
                rows.last().unwrap()["summary"][key].clone();
        }
        let (path, pin) = write_fixture(dir.path(), "v1-underfill", historical);
        assert!(read_run(&path, &pin).is_err());
    }

    #[test]
    fn completed_v2_rejects_prefix_seal_and_full_file_tamper() {
        let dir = tempfile::tempdir().unwrap();
        let expected = v2_fixture();
        for case in 0..5 {
            let mut rows = v2_fixture();
            authenticate(&mut rows);
            match case {
                0 => rows[1005]["prefix_sha256"] = json!("0".repeat(64)),
                1 => rows[1005]["prefix_bytes"] = json!(1),
                2 => rows.last_mut().unwrap()["summary"]["sealed_sha256"] = json!("0".repeat(64)),
                3 => rows.last_mut().unwrap()["summary"]["sealed_bytes"] = json!(1),
                _ => rows.last_mut().unwrap()["summary"]["prefix_sha256"] = json!("0".repeat(64)),
            }
            let (config, pin) = v2_config(dir.path(), &encode(&rows), &expected);
            assert!(
                reduce_completed(&config, &pin).is_err(),
                "seal tamper {case}"
            );
        }
        let mut rows = v2_fixture();
        authenticate(&mut rows);
        let bytes = encode(&rows);
        let (config, pin) = v2_config(dir.path(), &bytes, &expected);
        assert!(reduce_completed(&config, &"0".repeat(64)).is_err());
        let altered = String::from_utf8(bytes).unwrap().replacen(
            "\"observed_process_peak_bytes\":12345",
            "\"observed_process_peak_bytes\":12346",
            1,
        );
        std::fs::write(dir.path().join("completed-v2.jsonl"), altered).unwrap();
        assert!(reduce_completed(&config, &pin).is_err());
    }

    #[test]
    fn completed_v2_rejects_incomplete_reordered_and_unknown_rows() {
        let dir = tempfile::tempdir().unwrap();
        let expected = v2_fixture();
        for case in 0..10 {
            let mut rows = v2_fixture();
            match case {
                0 => {
                    rows.remove(5);
                }
                1 => rows.swap(5, 6),
                2 => rows.swap(5, 1006),
                3 => rows[5]["truth_opened"] = json!(true),
                4 => rows[2]["success"] = json!(false),
                5 => rows[5]["unrecognized"] = json!(true),
                6 => rows[0]["wire_bytes"] = json!(123),
                7 => rows[5]["trace"]["truth_opened"] = json!(true),
                8 => rows[3]["phase"] = json!("unknown"),
                _ => rows[1005]["requires_successful_directory_sync"] = json!(false),
            }
            authenticate(&mut rows);
            let (config, pin) = v2_config(dir.path(), &encode(&rows), &expected);
            assert!(
                reduce_completed(&config, &pin).is_err(),
                "order/schema {case}"
            );
        }
        let mut rows = v2_fixture();
        rows[4]["metadata"]["metadata"][0]["unknown"] = json!(1);
        authenticate(&mut rows);
        let (config, pin) = v2_config(dir.path(), &encode(&rows), &expected);
        assert_eq!(
            reduce_completed(&config, &pin).unwrap_err().to_string(),
            "unknown/missing v2 field"
        );
        let mut rows = v2_fixture();
        authenticate(&mut rows);
        let bytes = encode(&rows);
        for bytes in [
            bytes[..bytes.len() - 1].to_vec(),
            encode(&rows[..rows.len() - 1]),
            [bytes.clone(), b"{}\n".to_vec()].concat(),
            String::from_utf8(bytes)
                .unwrap()
                .replacen("\"ordinal\":0", "\"ordinal\":0,\"ordinal\":0", 1)
                .into_bytes(),
        ] {
            let (config, pin) = v2_config(dir.path(), &bytes, &expected);
            assert!(reduce_completed(&config, &pin).is_err());
        }
    }

    #[test]
    fn completed_v2_rejects_pins_sums_and_transport_contradictions() {
        let dir = tempfile::tempdir().unwrap();
        let expected = v2_fixture();
        for stage in ["router", "source", "sq8"] {
            let mut rows = v2_fixture();
            // Keep every charge sum and authentication pin self-consistent;
            // only the per-stage two failures for one submission is impossible.
            rows[5]["charges"][stage]["failed_gets"] = json!(2);
            rows[5]["sum"]["failed_gets"] = json!(2);
            let terminal = &mut rows.last_mut().unwrap()["summary"];
            terminal["charges"][stage]["failed_gets"] = json!(2);
            terminal["sum"]["failed_gets"] = json!(2);
            authenticate(&mut rows);
            let (config, pin) = v2_config(dir.path(), &encode(&rows), &expected);
            assert_eq!(
                reduce_completed(&config, &pin).unwrap_err().to_string(),
                "logical failed GETs exceed submitted GETs",
                "{stage}"
            );
        }
        let mut rows = v2_fixture();
        rows[2]["charges"]["failed_gets"] = json!(3);
        rows.last_mut().unwrap()["summary"]["binding_charge"]["failed_gets"] = json!(3);
        authenticate(&mut rows);
        let (config, pin) = v2_config(dir.path(), &encode(&rows), &expected);
        assert_eq!(
            reduce_completed(&config, &pin).unwrap_err().to_string(),
            "logical failed GETs exceed submitted GETs"
        );
        for (index, keys) in [
            (
                0,
                "config_sha256 binary_sha256 runner_source_sha256 generation_source_sha256 router_source_sha256 codec_source_sha256 source_plane_source_sha256",
            ),
            (
                1,
                "generation_root_sha256 requests_sha256 truth_sha256 native_source_sha256 native_sq8_sha256 native_order_sha256",
            ),
        ] {
            for key in keys.split_whitespace() {
                let mut rows = v2_fixture();
                rows[index][key] = json!("0".repeat(64));
                authenticate(&mut rows);
                let (config, pin) = v2_config(dir.path(), &encode(&rows), &expected);
                assert!(reduce_completed(&config, &pin).is_err(), "pin {key}");
            }
        }
        for case in 0..18 {
            let mut rows = v2_fixture();
            match case {
                0 => rows[5]["sum"]["verified_bytes"] = json!(1),
                1 => rows.last_mut().unwrap()["summary"]["query_wall_ns"] = json!(1),
                2 => rows.last_mut().unwrap()["summary"]["recall_numerator"] = json!(1),
                3 => rows.last_mut().unwrap()["summary"]["binding_charge"] = charge(3),
                4 => {
                    rows.last_mut().unwrap()["summary"]["transport_last_boundary"]["wire_bytes"] =
                        json!(1)
                }
                5 => rows[5]["transport"]["after"]["attempts"] = json!(1),
                6 => rows[5]["transport"]["ordinal"] = json!(1),
                7 => rows[5]["transport"]["after"]["consumed_payload_bytes"] = json!(1),
                8 => rows[5]["stages"]["source"]["start_ns"] = json!(1),
                9 => rows[1006]["hits10"] = json!(11),
                10 => rows[1]["backend"]["physical_prefix"] = json!("other/run"),
                11 => rows.last_mut().unwrap()["summary"]["complete"] = json!(false),
                12 => rows[5]["returned"].as_array_mut().unwrap().truncate(3),
                13 => rows[5]["underfill"] = json!(true),
                14 => rows[1006]["returned_count"] = json!(9),
                15 => rows[1006]["underfill"] = json!(true),
                16 => rows.last_mut().unwrap()["summary"]["underfilled_queries"] = json!(1),
                _ => {
                    // Consistent sums/flags cannot legitimize more hits than IDs.
                    rows[5]["returned"].as_array_mut().unwrap().truncate(3);
                    rows[5]["returned_count"] = json!(3);
                    rows[5]["underfill"] = json!(true);
                    rows[1006]["returned_count"] = json!(3);
                    rows[1006]["underfill"] = json!(true);
                    rows[1006]["hits10"] = json!(4);
                    rows[1006]["recall10"] = json!(0.4);
                    let terminal = &mut rows.last_mut().unwrap()["summary"];
                    terminal["total_hits10"] = json!(8995);
                    terminal["recall_numerator"] = json!(8995);
                    terminal["mean_recall10"] = json!(0.8995);
                    terminal["underfilled_queries"] = json!(1);
                }
            }
            authenticate(&mut rows);
            let (config, pin) = v2_config(dir.path(), &encode(&rows), &expected);
            assert!(
                reduce_completed(&config, &pin).is_err(),
                "totals/transport {case}"
            );
        }
        let mut rows = v2_fixture();
        authenticate(&mut rows);
        let (config, _) = v2_config(dir.path(), &encode(&rows), &expected);
        let mut c: Value = serde_json::from_slice(&std::fs::read(&config).unwrap()).unwrap();
        c["input"]["bytes"] = json!(1);
        let bytes = serde_json::to_vec(&c).unwrap();
        std::fs::write(&config, &bytes).unwrap();
        let output = dir.path().join("invalid-v2-report");
        assert!(
            !execute_report(&output, COMPLETED_SCHEMA, || reduce_completed(
                &config,
                &sha(&bytes)
            ))
            .unwrap()
        );
        let report: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(report["status"], "INVALID");
        assert_eq!(report["complete"], false);

        let mut rows = v2_fixture();
        authenticate(&mut rows);
        let (config, pin) = v2_config(dir.path(), &encode(&rows), &expected);
        let non_utf8 = dir
            .path()
            .join(std::ffi::OsString::from_vec(b"config-\xff.json".to_vec()));
        std::fs::copy(config, &non_utf8).unwrap();
        let output = dir.path().join("non-utf8-config-report");
        let result = std::panic::catch_unwind(|| {
            execute_report(&output, COMPLETED_SCHEMA, || {
                reduce_completed(&non_utf8, &pin)
            })
        });
        assert!(!result.expect("non-UTF8 config must not panic").unwrap());
        let report: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(report["status"], "INVALID");
        assert_eq!(report["complete"], false);
        assert_eq!(report["error"], "config path must be UTF-8");
    }

    #[test]
    fn nearest_rank_and_sequential_qps_literal_golden() {
        // Round-based indexing returns 11ms at p50 here; nearest rank is 10ms.
        let s = statistics(&(1..=20).rev().map(|n| n * 1_000_000).collect::<Vec<_>>()).unwrap();
        assert_eq!(s.count, 20);
        assert_eq!(s.query_wall_ns, 210_000_000);
        assert_eq!(s.p90_ns, 18_000_000);
        assert_eq!(s.p95_ns, 19_000_000);
        assert_eq!(
            [s.p50_ms, s.p90_ms, s.p95_ms, s.p99_ms],
            [10., 18., 19., 20.]
        );
        assert!((s.sequential_qps - 95.23809523809524).abs() < 1e-12);
        assert!(statistics(&[]).is_err());
        assert!(statistics(&[u64::MAX, 1]).is_err());
        assert!(statistics(&[0]).is_err());
    }

    #[test]
    fn four_authenticated_native_runs_and_completed_no_win() {
        let dir = tempfile::tempdir().unwrap();
        let inputs = four(dir.path());
        let report = compare(&inputs).unwrap();
        assert_eq!(report["status"], "MEASURED");
        assert_eq!(report["timing_gate_passed"], true);
        assert_eq!(report["pooled"]["A"]["count"], 2000);
        assert_eq!(report["pooled"]["B"]["query_wall_ns"], 900900000000_u64);
        assert_eq!(report["pooled"]["A"]["p50_ms"], 500.);
        for arm in ["A", "B"] {
            for stage in ["discovery_ns", "source_ns", "planning_ns", "sq8_ns"] {
                assert_eq!(report["pooled"][arm]["stage_wall_sums"][stage], 2000);
            }
            assert_eq!(
                report["pooled"][arm]["stage_wall_sums"]["max_leaf_peak_inflight"],
                1
            );
        }
        assert_eq!(report["trace_plan_parity"], true);
        assert_eq!(
            report["comparisons"][0]["deltas"][999]["query_wall_ns"],
            -100000000
        );
        assert_eq!(report["local_file_only"], true);
        assert_eq!(report["physical_s3"], false);
        assert_eq!(report["external_resources_and_cost_gate_required"], true);
        assert_eq!(
            report["frozen_runtime_root_config_admission_required"],
            true
        );
        assert_eq!(report["qualified"], false);
        let output = dir.path().join("report");
        assert!(execute(&inputs, &output).unwrap());
        let original = std::fs::read(&output).unwrap();
        assert!(execute(&inputs, &output).is_err());
        assert_eq!(std::fs::read(&output).unwrap(), original);

        let mut no_win = inputs;
        no_win[2] = write_fixture(dir.path(), "slow-b2", fixture(true, 1000000));
        let report = compare(&no_win).unwrap();
        assert_eq!(report["status"], "MEASURED");
        assert_eq!(report["complete"], true);
        assert_eq!(report["timing_gate_passed"], false);
        assert_eq!(report["comparisons"][0]["timing_gate_passed"], true);
        assert_eq!(report["comparisons"][1]["timing_gate_passed"], false);

        // Exact 5% latency boundary passes; one extra ns per ordinal does not.
        for unit in [950000, 950001] {
            no_win[1] = write_fixture(dir.path(), "boundary-b1", fixture(true, unit));
            let mut rows = fixture(true, unit);
            rows.last_mut().unwrap()["summary"]["process_cpu_ns"] = json!(1200);
            no_win[2] = write_fixture(dir.path(), "boundary-b2", rows);
            assert_eq!(
                compare(&no_win).unwrap()["timing_gate_passed"],
                unit == 950000
            );
        }
        no_win[1] = write_fixture(dir.path(), "passing-b1", fixture(true, 900000));
        // Tail outlier leaves p90/p95 passing but makes sequential QPS fail.
        let mut rows = fixture(true, 900000);
        rows[1002]["query_wall_ns"] = json!(70900000000_u64);
        rows.last_mut().unwrap()["summary"]["query_wall_ns"] = json!(520450000000_u64);
        rows.last_mut().unwrap()["summary"]["process_wall_ns"] = json!(520450000100_u64);
        no_win[2] = write_fixture(dir.path(), "qps-miss", rows);
        let report = compare(&no_win).unwrap();
        assert_eq!(
            report["comparisons"][1]["p90_at_least_5_percent_lower"],
            true
        );
        assert_eq!(
            report["comparisons"][1]["p95_at_least_5_percent_lower"],
            true
        );
        assert_eq!(
            report["comparisons"][1]["sequential_qps_at_least_5_percent_higher"],
            false
        );

        // First900 candidate queries keep p90 at810ms; last100 make p95=920ms.
        let mut rows = fixture(true, 900000);
        for row in &mut rows[903..1003] {
            row["query_wall_ns"] = json!(920000000);
        }
        rows.last_mut().unwrap()["summary"]["query_wall_ns"] = json!(456905000000_u64);
        rows.last_mut().unwrap()["summary"]["process_wall_ns"] = json!(456905000100_u64);
        no_win[2] = write_fixture(dir.path(), "p95-only-miss", rows);
        let report = compare(&no_win).unwrap();
        assert_eq!(report["runs"][2]["statistics"]["p90_ns"], 810000000);
        assert_eq!(report["runs"][2]["statistics"]["p95_ns"], 920000000);
        assert_eq!(report["comparisons"][0]["timing_gate_passed"], true);
        assert_eq!(
            report["comparisons"][1]["p90_at_least_5_percent_lower"],
            true
        );
        assert_eq!(
            report["comparisons"][1]["p95_at_least_5_percent_lower"],
            false
        );
        assert_eq!(
            report["comparisons"][1]["sequential_qps_at_least_5_percent_higher"],
            true
        );

        // Literal equality:476670480000*105 ==500504004000*100. One ns more fails.
        let mut exact = four(dir.path());
        for i in [0, 3] {
            let mut rows = fixture(false, 1000008);
            rows.last_mut().unwrap()["summary"]["process_cpu_ns"] = json!(1200 + i);
            exact[i] = write_fixture(dir.path(), &format!("exact-a-{i}"), rows);
        }
        for extra in [0_u64, 1] {
            for i in [1, 2] {
                let mut rows = fixture(true, 900000);
                rows[1002]["query_wall_ns"] = json!(27120480000_u64 + extra);
                let t = &mut rows.last_mut().unwrap()["summary"];
                t["query_wall_ns"] = json!(476670480000_u64 + extra);
                t["process_wall_ns"] = json!(476670480100_u64 + extra);
                t["process_cpu_ns"] = json!(1200 + i);
                exact[i] = write_fixture(dir.path(), &format!("exact-b-{i}"), rows);
            }
            let report = compare(&exact).unwrap();
            assert_eq!(
                report["runs"][0]["statistics"]["query_wall_ns"],
                500504004000_u64
            );
            assert_eq!(
                report["runs"][1]["statistics"]["query_wall_ns"],
                476670480000_u64 + extra
            );
            assert_eq!(report["timing_gate_passed"], extra == 0);
            for i in 0..2 {
                assert_eq!(
                    report["comparisons"][i]["p90_at_least_5_percent_lower"],
                    true
                );
                assert_eq!(
                    report["comparisons"][i]["p95_at_least_5_percent_lower"],
                    true
                );
                assert_eq!(
                    report["comparisons"][i]["sequential_qps_at_least_5_percent_higher"],
                    extra == 0
                );
            }
        }
    }

    #[test]
    fn invalid_tampered_out_of_order_and_semantic_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let valid = write_fixture(dir.path(), "valid", fixture(false, 1000000));
        assert!(read_run(&valid.0, &"f".repeat(64)).is_err());
        let original = std::fs::read(&valid.0).unwrap();
        for bytes in [
            original[..original.len() - 1].to_vec(),
            [original.clone(), b"{}\n".to_vec()].concat(),
        ] {
            std::fs::write(&valid.0, &bytes).unwrap();
            assert!(read_run(&valid.0, &sha(&bytes)).is_err());
        }
        let mut rows = fixture(false, 1000000);
        authenticate(&mut rows);
        for index in [0, 1, 3, 1003, 1004, 2004] {
            for duplicate in [false, true] {
                let mut changed = rows.clone();
                if duplicate {
                    changed.insert(index, changed[index].clone());
                } else {
                    changed.remove(index);
                }
                let bytes = encode(&changed);
                std::fs::write(&valid.0, &bytes).unwrap();
                assert!(read_run(&valid.0, &sha(&bytes)).is_err());
            }
        }
        // A duplicate recognized JSON field must not silently use its last value.
        let bytes = encode(&rows);
        let text = String::from_utf8(bytes).unwrap().replacen(
            "\"ordinal\":0",
            "\"ordinal\":0,\"ordinal\":0",
            1,
        );
        std::fs::write(&valid.0, &text).unwrap();
        assert!(read_run(&valid.0, &sha(text.as_bytes())).is_err());
        // Each corruption gets fresh outer/prefix/seal hashes: semantic checks must reject it.
        for case in 0..19 {
            let mut rows = fixture(false, 1000000);
            match case {
                0 => rows[3]["ordinal"] = json!(1),
                1 => rows[3]["truth_opened"] = json!(true),
                2 => rows[3]["returned_count"] = json!(9),
                3 => rows[3]["returned"][1]["id"] = json!(8),
                4 => rows[3]["sum"]["verified_bytes"] = json!(299),
                5 => rows[1004]["ordinal"] = json!(1),
                6 => rows[1004]["recall10"] = json!(1.0),
                7 => rows.last_mut().unwrap()["summary"]["query_wall_ns"] = json!(1),
                8 => rows.last_mut().unwrap()["summary"]["status"] = json!("INVALID"),
                9 => rows.last_mut().unwrap()["summary"]["recall_numerator"] = json!(9999),
                10 => rows[3]["charges"]["sq8"]["submitted_gets"] = json!(u64::MAX),
                11 => rows.swap(3, 1004),
                12 => rows[1003]["requires_successful_directory_sync"] = json!(false),
                13 => rows[3]["stages"]["source"]["start_ns"] = json!(1),
                14 => {
                    for stage in ["discovery", "source", "planning", "sq8"] {
                        rows[3]["stages"][stage] = json!({"start_ns":0,"end_ns":0});
                    }
                }
                15 => rows[3]["returned"].as_array_mut().unwrap().swap(0, 1),
                16 => rows[3]["returned"].as_array_mut().unwrap().swap(2, 3),
                17 => {
                    rows[3].as_object_mut().unwrap().remove("trace");
                }
                _ => rows[3]["trace"] = json!({"nested":{"a":0,"trace":{}}}),
            }
            let input = write_fixture(dir.path(), "bad", rows);
            assert!(read_run(&input.0, &input.1).is_err(), "case {case}");
        }
        for case in 0..3 {
            let mut rows = fixture(false, 1000000);
            authenticate(&mut rows);
            match case {
                0 => rows[1003]["prefix_sha256"] = json!("f".repeat(64)),
                1 => rows.last_mut().unwrap()["summary"]["sealed_sha256"] = json!("f".repeat(64)),
                _ => rows.last_mut().unwrap()["summary"]["sealed_bytes"] = json!(1),
            }
            let bytes = encode(&rows);
            std::fs::write(&valid.0, &bytes).unwrap();
            assert!(read_run(&valid.0, &sha(&bytes)).is_err());
        }

        let mut inputs = four(dir.path());
        for case in 0..7 {
            let mut rows = fixture(true, 900000);
            match case {
                0 => rows[3]["returned"][9]["id"] = json!(42),
                1 => rows[3]["returned"][9]["score_bits"] = json!(1077936128),
                2 => {
                    rows[1004]["hits10"] = json!(8);
                    rows[1004]["recall10"] = json!(0.8);
                    let t = &mut rows.last_mut().unwrap()["summary"];
                    t["total_hits10"] = json!(8999);
                    t["recall_numerator"] = json!(8999);
                    t["mean_recall10"] = json!(0.8999);
                }
                3 => rows[0]["config_sha256"] = json!("d".repeat(64)),
                4 => {
                    let root = json!("d".repeat(64));
                    rows[1]["generation_root_sha256"] = root.clone();
                    rows[1003]["generation_root_sha256"] = root.clone();
                    rows.last_mut().unwrap()["summary"]["generation_root_sha256"] = root;
                }
                5 => {
                    rows[3]["charges"]["sq8"] = charge(2);
                    rows[3]["sum"] = charge(4);
                    let t = &mut rows.last_mut().unwrap()["summary"];
                    t["charges"]["sq8"] = charge(1001);
                    t["sum"] = charge(3001);
                }
                _ => rows[3]["trace"]["ranked_candidate_pages"] = json!([1, 0]),
            }
            inputs[1] = write_fixture(dir.path(), "mismatch", rows);
            assert!(read_run(&inputs[1].0, &inputs[1].1).is_ok());
            assert!(compare(&inputs).is_err(), "parity {case}");
        }
        let output = dir.path().join("invalid-report");
        assert!(!execute(&inputs, &output).unwrap());
        let report: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(report["status"], "INVALID");
        assert_eq!(report["complete"], false);
        assert_eq!(
            report["frozen_runtime_root_config_admission_required"],
            true
        );
        assert_eq!(report["qualified"], false);

        // Rehash otherwise-valid evidence with one wrong immutable pin at a time.
        for (index, fields) in [
            (
                0,
                [
                    "runner_source_sha256",
                    "generation_source_sha256",
                    "router_source_sha256",
                    "codec_source_sha256",
                    "source_plane_source_sha256",
                ],
            ),
            (
                1,
                [
                    "requests_sha256",
                    "truth_sha256",
                    "native_source_sha256",
                    "native_sq8_sha256",
                    "native_order_sha256",
                ],
            ),
        ] {
            for field in fields {
                let mut rows = fixture(false, 1000000);
                let wrong = json!("d".repeat(64));
                rows[index][field] = wrong.clone();
                if field == "requests_sha256" {
                    rows[1003][field] = wrong.clone();
                }
                if field == "requests_sha256" || field == "truth_sha256" {
                    rows.last_mut().unwrap()["summary"][field] = wrong;
                }
                let input = write_fixture(dir.path(), "wrong-pin", rows);
                assert!(read_run(&input.0, &input.1).is_err(), "pin {field}");
            }
        }
        let mut same_binary = four(dir.path());
        for (i, input) in same_binary.iter_mut().enumerate().take(3).skip(1) {
            let mut rows = fixture(false, 900000);
            rows.last_mut().unwrap()["summary"]["process_cpu_ns"] = json!(1200 + i);
            *input = write_fixture(dir.path(), &format!("same-binary-{i}"), rows);
            assert!(read_run(&input.0, &input.1).is_ok());
        }
        assert!(compare(&same_binary).is_err());
        let mut swapped = four(dir.path());
        swapped.swap(0, 1);
        swapped.swap(2, 3);
        assert!(compare(&swapped).is_err());
        let mut wrong_a2 = four(dir.path());
        wrong_a2[3] = write_fixture(dir.path(), "wrong-a2-identity", fixture(true, 1000000));
        assert!(read_run(&wrong_a2[3].0, &wrong_a2[3].1).is_ok());
        assert!(compare(&wrong_a2).is_err());

        // Ten distinct increasing scores1..10, then one reversed adjacent pair.
        let mut ascending = fixture(false, 1000000);
        ascending[3]["returned"] = Value::Array(
            [
                0x3f80_0000_u32,
                0x4000_0000,
                0x4040_0000,
                0x4080_0000,
                0x40a0_0000,
                0x40c0_0000,
                0x40e0_0000,
                0x4100_0000,
                0x4110_0000,
                0x4120_0000,
            ]
            .into_iter()
            .enumerate()
            .map(|(id, bits)| json!({"id":id,"score_bits":bits}))
            .collect(),
        );
        let input = write_fixture(dir.path(), "ascending", ascending.clone());
        assert!(read_run(&input.0, &input.1).is_ok());
        ascending[3]["returned"].as_array_mut().unwrap().swap(4, 5);
        let input = write_fixture(dir.path(), "reversed", ascending);
        assert!(read_run(&input.0, &input.1).is_err());

        let suffix = b",\"trace\":{\"value\":1}}\n";
        assert_eq!(
            trace_fingerprint(b"{\"phase\":\"query\",\"trace\":{\"value\":1}}\n").unwrap(),
            <[u8; 32]>::from(Sha256::digest(suffix))
        );
        for raw in [
            b"{\"phase\":\"query\"}\n".as_slice(),
            b"{\"phase\":\"query\",\"trace\":{},\"after\":0}\n",
            b"{\"phase\":\"query\",\"trace\":{},\"trace\":{}}\n",
            b"{\"phase\":\"query\",\"trace\" :{}}\n",
            b"{\"outer\":{\"a\":0,\"trace\":{}}}\n",
        ] {
            assert!(trace_fingerprint(raw).is_err());
        }
    }

    // Metadata-only current-native schema fixture; no corpus or truth is read.
    fn v9_panel_fixture() -> Vec<Value> {
        let full = paired_scale_fixture(64);
        let ordinals = [0, 1, 127, 255, 511, 767, 998, 999];
        let execution = json!({"mode":"diagnostic_panel","ordinals":ordinals,"trace":true});
        let mut rows = full[..2].to_vec();
        rows[0]["schema"] = json!("borsuk-cohere-native-baseline-result-v9");
        for header in &mut rows[..2] {
            header["execution"] = execution.clone();
            header["source_get_budget"] = json!(128);
            header["sq8_get_budget"] = json!(30);
            header["sq8_byte_budget"] = json!(33554432);
        }
        rows[1]["selected_count"] = json!(8);
        use borsuk::sq8_s3_range::Sq8RangeTrace;
        let width = rows[1]["fetch_parallelism"].as_u64().unwrap() as usize;
        let peak = Sq8RangeTrace::modeled_bytes(30, width);
        let both = Sq8RangeTrace::pending_futures_bytes(128, width);
        let pinned = both - Sq8RangeTrace::pending_futures_bytes(30, width);
        rows.push(json!({"phase":"diagnostic_admission","execution":execution,
            "selected_count":8,"population_count":1000,"trace":true,"truth_opened":false,
            "population_percentiles_valid":false,"full_cohort_qualification":false,
            "diagnostic_bytes_per_active_query":540672 + peak,"diagnostic_cap_bytes":1048576,
            "max_active_queries":1,"charged_in_caller_pinned_bytes":true,
            "caller_pinned_bytes":2097152,"panel_line_cap_bytes":65536,
            "host_read_cap_bytes":131072,"trace_ranges":30,
            "trace_retained_bytes":Sq8RangeTrace::retained_bytes(30),"trace_peak_bytes":peak,
            "diagnostic_pinned_bytes":540672,"trace_peak_charged_by_library_at_traced_admission":true,
            "range_state_pinned_by_runner_bytes":pinned,"range_state_both_paths_bytes":both,
            "semantics":{"headers_interval":"get_opts_client_queue_connect_tls_server_combined",
                "body_gaps":"application_observed_poll_gaps_not_packet_arrival",
                "ranges":"overlap_in_time_never_sum_intervals",
                "host_counters":"host_wide_not_per_flow_null_means_unavailable",
                "unset":"a boundary never reached is null, never a fabricated timestamp"}}));
        rows.extend_from_slice(&full[2..5]);
        let mut wall = 0_u64;
        for (slot, ordinal) in ordinals.into_iter().enumerate() {
            let mut query = full[5 + ordinal].clone();
            query["selected_slot"] = json!(slot);
            query["plan"] = json!({"page_rows":32,"selected_pages":[0],
                "ranges":[[0,33152]],"planned_bytes":33152,"target_pages":1,
                "target_shortfall":0,"primary_pages_retained":0,"covered_pages":1,"bridge_pages":0});
            query["charges"]["source"]["verified_bytes"] = json!(8448);
            query["charges"]["sq8"]["verified_bytes"] = json!(33152);
            query["sum"]["verified_bytes"] = json!(41700);
            query["transport"] = full[5 + slot]["transport"].clone();
            query["transport"]["ordinal"] = json!(ordinal);
            query["transport"]["before"]["consumed_payload_bytes"] = json!(500 + slot * 41700);
            query["transport"]["after"]["consumed_payload_bytes"] = json!(500 + (slot + 1) * 41700);
            query["trace"] = json!({"primary_page":0,"nomination_evaluated_units":[0],
                "ranked_candidate_pages":[0],"discoveries":[],"semantic_leaves":[0],
                "semantic_units":[0],"semantic_seed_additions":[]});
            query["diagnostic"] = json!({"schema":"borsuk-sq8-range-attribution-diagnostic-v2",
                "query_monotonic_start_ns":null,"query_monotonic_end_ns":null,
                "process_cpu_start_ns":1,"process_cpu_end_ns":2,
                "sq8_range_trace":{},"host":{}});
            wall += query["query_wall_ns"].as_u64().unwrap();
            rows.push(query);
        }
        let mut seal = full
            .iter()
            .find(|r| r["phase"] == "all_queries_sealed")
            .unwrap()
            .clone();
        seal["count"] = json!(8);
        seal["selected_count"] = json!(8);
        rows.push(seal);
        for ordinal in ordinals {
            rows.push(
                full.iter()
                    .find(|r| r["phase"] == "recall" && r["ordinal"] == ordinal)
                    .unwrap()
                    .clone(),
            );
        }
        let span: TransportSpan = serde_json::from_value(rows[13]["transport"].clone()).unwrap();
        let mut terminal = full.last().unwrap().clone();
        let summary = &mut terminal["summary"];
        for key in ["queries", "selected_count", "executed_count"] {
            summary[key] = json!(8);
        }
        for key in ["total_hits10", "recall_numerator"] {
            summary[key] = json!(72);
        }
        summary["recall_denominator"] = json!(80);
        summary["execution"] = execution;
        summary["diagnostic_panel"] = json!(true);
        summary["population_percentiles_valid"] = json!(false);
        summary["query_wall_ns"] = json!(wall);
        summary["process_wall_ns"] = json!(wall + 100);
        summary["query_process_cpu_ns"] = json!(8);
        summary["process_cpu_ns"] = json!(108);
        summary["charges"] = json!({"router":{"submitted_gets":8,"verified_bytes":800,"failed_gets":0},
            "source":{"submitted_gets":8,"verified_bytes":67584,"failed_gets":0},
            "sq8":{"submitted_gets":8,"verified_bytes":265216,"failed_gets":0}});
        summary["sum"] = json!({"submitted_gets":24,"verified_bytes":333600,"failed_gets":0});
        summary["transport_last_boundary"] = span.compact();
        rows.push(terminal);
        authenticate(&mut rows);
        rows
    }

    #[test]
    fn v9_panel_reader_authenticates_sealed_native_sequence_and_rejects_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let rows = v9_panel_fixture();
        let check = |mut values: Vec<Value>| {
            authenticate(&mut values);
            let bytes = encode(&values);
            let path = dir.path().join("panel.jsonl");
            std::fs::write(&path, &bytes).unwrap();
            v9_panel::read_candidate(
                &CompletedInput {
                    path,
                    bytes: bytes.len() as u64,
                    sha256: sha(&bytes),
                },
                &values[0],
                &values[1],
                &[0, 1, 127, 255, 511, 767, 998, 999],
                8 * 1024 * 1024,
            )
        };
        let valid = check(rows.clone()).expect("accept sealed current v9 native panel");
        assert_eq!(valid["selected_count"], 8);
        assert_eq!(valid["recall_denominator"], 80);
        assert_eq!(valid["exact_range_oracle_qualified"], false);
        let panel_bytes = encode(&rows);
        let config = json!({"schema":"borsuk-current-v9-panel-reader-config-v1",
            "completed":{"schema":SCALE_CONFIG_SCHEMA,
                "input":{"path":dir.path().join("panel.jsonl"),"bytes":panel_bytes.len(),"sha256":sha(&panel_bytes)},
                "expected_identity":rows[0],"expected_bound_inputs":rows[1]},
            "max_payload_bytes":8*1024*1024});
        let config_path = dir.path().join("panel-config.json");
        let config_bytes = serde_json::to_vec(&config).unwrap();
        std::fs::write(&config_path, &config_bytes).unwrap();
        let output = dir.path().join("panel-report.json");
        assert!(
            execute_report(&output, v9_panel::REPORT_SCHEMA, || v9_panel::reduce(
                &config_path,
                &sha(&config_bytes)
            ))
            .unwrap()
        );
        let saved = std::fs::read(&output).unwrap();
        let report: Value = serde_json::from_slice(&saved).unwrap();
        assert_eq!(report["status"], "VALIDATED_PANEL");
        assert_eq!(report["producer_provenance_qualified"], false);
        assert_eq!(report["reduction_config_sha256"], sha(&config_bytes));
        assert_eq!(report["reduction_config_bytes"], config_bytes.len());
        assert_eq!(report["panel_input_sha256"], sha(&panel_bytes));
        assert_eq!(report["panel_input_bytes"], panel_bytes.len());
        assert!(
            execute_report(&output, v9_panel::REPORT_SCHEMA, || panic!(
                "existing output must refuse before reduction"
            ))
            .is_err()
        );
        assert_eq!(std::fs::read(&output).unwrap(), saved);
        assert!(v9_panel::reduce(&config_path, &"0".repeat(64)).is_err());
        let mut unknown = config.clone();
        unknown["unexpected"] = json!(true);
        let unknown_bytes = serde_json::to_vec(&unknown).unwrap();
        std::fs::write(&config_path, &unknown_bytes).unwrap();
        assert!(v9_panel::reduce(&config_path, &sha(&unknown_bytes)).is_err());
        let invalid_output = dir.path().join("invalid-panel-report.json");
        assert!(
            !execute_report(&invalid_output, v9_panel::REPORT_SCHEMA, || {
                v9_panel::reduce(&config_path, &sha(&unknown_bytes))
            })
            .unwrap()
        );
        let invalid: Value =
            serde_json::from_slice(&std::fs::read(invalid_output).unwrap()).unwrap();
        assert_eq!(invalid["status"], "INVALID");
        let mut changed_header = rows.clone();
        changed_header[0]["config_sha256"] = json!("7".repeat(64));
        authenticate(&mut changed_header);
        let changed_bytes = encode(&changed_header);
        let changed_path = dir.path().join("independent-header.jsonl");
        std::fs::write(&changed_path, &changed_bytes).unwrap();
        let alternate = v9_panel::read_candidate(
            &CompletedInput {
                path: changed_path.clone(),
                bytes: changed_bytes.len() as u64,
                sha256: sha(&changed_bytes),
            },
            &changed_header[0],
            &changed_header[1],
            &[0, 1, 127, 255, 511, 767, 998, 999],
            8 * 1024 * 1024,
        )
        .unwrap();
        assert_ne!(
            alternate["panel_input_sha256"], valid["panel_input_sha256"],
            "valid panels with different runtime identities retain different evidence identities"
        );
        assert!(
            v9_panel::read_candidate(
                &CompletedInput {
                    path: changed_path,
                    bytes: changed_bytes.len() as u64,
                    sha256: sha(&changed_bytes)
                },
                &rows[0],
                &rows[1],
                &[0, 1, 127, 255, 511, 767, 998, 999],
                8 * 1024 * 1024,
            )
            .is_err(),
            "resealing must not replace independently supplied expected headers"
        );
        let mut oversized = rows.clone();
        oversized[6]["trace"]["nomination_evaluated_units"] = json!(vec![0_u64; 40000]);
        let error = check(oversized).unwrap_err().to_string();
        assert!(error.contains("row cap"), "oversized native panel: {error}");
        let check_raw = |bytes: &[u8], expected_sha: String| {
            let path = dir.path().join("raw-panel.jsonl");
            std::fs::write(&path, bytes).unwrap();
            v9_panel::read_candidate(
                &CompletedInput {
                    path,
                    bytes: bytes.len() as u64,
                    sha256: expected_sha,
                },
                &rows[0],
                &rows[1],
                &[0, 1, 127, 255, 511, 767, 998, 999],
                8 * 1024 * 1024,
            )
        };
        for (row, pointer, value) in [
            (14, "/prefix_bytes", json!(0)),
            (14, "/prefix_sha256", json!("0".repeat(64))),
            (23, "/summary/sealed_bytes", json!(0)),
            (23, "/summary/sealed_sha256", json!("0".repeat(64))),
        ] {
            let mut changed = rows.clone();
            *changed[row].pointer_mut(pointer).unwrap() = value;
            let bytes = encode(&changed);
            // Authenticate the changed whole file, deliberately not its old seal.
            assert!(check_raw(&bytes, sha(&bytes)).is_err(), "{pointer}");
        }
        let bytes = encode(&rows);
        assert!(check_raw(&bytes, "0".repeat(64)).is_err());
        let truncated = &bytes[..bytes.len() - 1];
        assert!(check_raw(truncated, sha(truncated)).is_err());
        let duplicate = String::from_utf8(bytes)
            .unwrap()
            .replacen(
                "\"phase\":\"identity\"",
                "\"phase\":\"identity\",\"phase\":\"identity\"",
                1,
            )
            .into_bytes();
        assert!(check_raw(&duplicate, sha(&duplicate)).is_err());
        for (row, pointer, value) in [
            (
                2,
                "/trace_peak_charged_by_library_at_traced_admission",
                json!(false),
            ),
            (2, "/trace_peak_bytes", Value::Null),
            (2, "/trace_peak_bytes", json!(1)),
            (2, "/trace_retained_bytes", json!(1)),
            (2, "/trace_ranges", json!(128)),
            (2, "/host_read_cap_bytes", json!(16384)),
            (2, "/range_state_both_paths_bytes", json!(0)),
            (2, "/diagnostic_bytes_per_active_query", json!(1048577)),
            (
                0,
                "/schema",
                json!("borsuk-cohere-native-baseline-result-v8"),
            ),
            (1, "/rows", json!(100000)),
            (1, "/sq8_byte_budget", json!(33554433)),
            (1, "/truth_sha256", json!("0".repeat(64))),
            (6, "/selected_slot", json!(1)),
            (6, "/ordinal", json!(1)),
            (6, "/truth_opened", json!(true)),
            (6, "/underfill", json!(true)),
            (6, "/plan/page_rows", json!(256)),
            (6, "/charges/sq8/verified_bytes", json!(33151)),
            (14, "/truth_opened", json!(true)),
            (15, "/ordinal", json!(1)),
            (23, "/summary/status", json!("INVALID")),
            (23, "/summary/complete", json!(false)),
            (23, "/summary/recall_denominator", json!(10000)),
        ] {
            let mut changed = rows.clone();
            *changed[row].pointer_mut(pointer).unwrap() = value;
            assert!(check(changed).is_err(), "row{row}{pointer}");
        }
        let mut duplicate = rows.clone();
        duplicate.insert(7, duplicate[6].clone());
        assert!(check(duplicate).is_err());
        let mut missing = rows.clone();
        missing.remove(6);
        assert!(check(missing).is_err());
        let mut trailing = rows;
        trailing.push(json!({"phase":"terminal","summary":{}}));
        // Trailing rows must not be repaired into a new valid terminal by the helper.
        let bytes = encode(&trailing);
        let path = dir.path().join("trailing.jsonl");
        std::fs::write(&path, &bytes).unwrap();
        assert!(
            v9_panel::read_candidate(
                &CompletedInput {
                    path,
                    bytes: bytes.len() as u64,
                    sha256: sha(&bytes)
                },
                &trailing[0],
                &trailing[1],
                &[0, 1, 127, 255, 511, 767, 998, 999],
                8 * 1024 * 1024
            )
            .is_err()
        );
    }

    #[test]
    fn v9_oracle_rejects_bad_plane_before_opening_source_body() {
        let dir = tempfile::tempdir().unwrap();
        let digests = vec![0u8; 1000000];
        let digest_path = dir.path().join("digests");
        std::fs::write(&digest_path, &digests).unwrap();
        let records_path = dir.path().join("MUST_NOT_OPEN_RECORDS");
        for (key, value) in [
            ("schema", json!("wrong")),
            ("seed", json!(0)),
            ("page_rows", json!(64)),
            ("generation", json!(0)),
        ] {
            let mut rows = v9_panel_fixture();
            let mut plane = json!({"schema":"borsuk-two-bit-plane-v3","rows":1000000,"dimensions":1024,
                "seed":20260923,"record_bytes":264,"source_sha256":rows[1]["native_source_sha256"],
                "sq8_sha256":rows[1]["native_sq8_sha256"],"source_order_sha256":rows[1]["native_order_sha256"],
                "mean_sha256":"0".repeat(64),"records_sha256":"0".repeat(64),"page_rows":32,
                "page_digest_sha256":sha(&digests),"query_or_truth_used":false});
            if key != "generation" {
                plane[key] = value.clone();
            }
            let plane_bytes = serde_json::to_vec(&plane).unwrap();
            let plane_path = dir.path().join("plane.json");
            std::fs::write(&plane_path, &plane_bytes).unwrap();
            let root = json!({"generation":if key=="generation" {value.clone()} else {json!(1)},
                "plane_manifest_sha256":sha(&plane_bytes),"sq8_object_sha256":rows[1]["native_sq8_sha256"]});
            let root_bytes = serde_json::to_vec(&root).unwrap();
            let root_sha = sha(&root_bytes);
            let root_path = dir.path().join("root.json");
            std::fs::write(&root_path, &root_bytes).unwrap();
            for row in &mut rows {
                if row.get("generation_root_sha256").is_some() {
                    row["generation_root_sha256"] = json!(root_sha);
                }
                if row["phase"] == "terminal" {
                    row["summary"]["generation_root_sha256"] = json!(root_sha);
                }
            }
            authenticate(&mut rows);
            let body = encode(&rows);
            let panel_path = dir.path().join("panel.jsonl");
            std::fs::write(&panel_path, &body).unwrap();
            let config = json!({"schema":"borsuk-v9-source-cover-oracle-config-v1",
                "panel":{"schema":"borsuk-current-v9-panel-reader-config-v1",
                    "completed":{"schema":SCALE_CONFIG_SCHEMA,"input":{"path":panel_path,"bytes":body.len(),"sha256":sha(&body)},
                        "expected_identity":rows[0],"expected_bound_inputs":rows[1]},"max_payload_bytes":384*1024*1024},
                "generation_manifest":{"path":root_path,"bytes":root_bytes.len(),"sha256":root_sha},
                "plane_manifest":{"path":plane_path,"bytes":plane_bytes.len(),"sha256":sha(&plane_bytes)},
                "records":{"path":records_path,"bytes":264000000,"sha256":"0".repeat(64)},
                "mean":{"path":dir.path().join("MUST_NOT_OPEN_MEAN"),"bytes":4096,"sha256":"0".repeat(64)},
                "source_digests":{"path":digest_path,"bytes":digests.len(),"sha256":sha(&digests)},
                "requests":{"path":dir.path().join("MUST_NOT_OPEN_REQUESTS"),"bytes":4096000,"sha256":rows[1]["requests_sha256"]}});
            let bytes = serde_json::to_vec(&config).unwrap();
            let path = dir.path().join("oracle.json");
            std::fs::write(&path, &bytes).unwrap();
            let error = v9_panel::reduce_oracle(&path, &sha(&bytes))
                .unwrap_err()
                .to_string();
            assert!(
                error.starts_with("SOURCE oracle authority"),
                "{key}: {error}"
            );
            assert!(!records_path.exists());
        }
    }

    #[test]
    fn v9_panel_reader_refuses_production_policy_before_opening_results() {
        let rows = v9_panel_fixture();
        let dir = tempfile::tempdir().unwrap();
        let pin = CompletedInput {
            path: dir.path().join("MUST_REMAIN_UNOPENED"),
            bytes: 4096,
            sha256: "9".repeat(64),
        };
        let descriptor = |name: &str, bytes: u64, sha256: Value| {
            json!({
            "path":dir.path().join(name),"bytes":bytes,"sha256":sha256})
        };
        let oracle = json!({"schema":"borsuk-v9-source-cover-oracle-config-v1",
            "panel":{"schema":"borsuk-current-v9-panel-reader-config-v1",
                "completed":{"schema":SCALE_CONFIG_SCHEMA,"input":pin,
                    "expected_identity":rows[0],"expected_bound_inputs":rows[1]},
                "max_payload_bytes":384*1024*1024},
            "generation_manifest":descriptor("unopened-root",27000,rows[1]["generation_root_sha256"].clone()),
            "plane_manifest":descriptor("unopened-plane",1000,json!("1".repeat(64))),
            "records":descriptor("unopened-records",264000000,json!("2".repeat(64))),
            "mean":descriptor("unopened-mean",4096,json!("3".repeat(64))),
            "source_digests":descriptor("unopened-digests",1000000,json!("4".repeat(64))),
            "requests":descriptor("unopened-requests",4096000,rows[1]["requests_sha256"].clone())});
        for (pointer, value, expected) in [
            (
                "/schema",
                json!("wrong"),
                "SOURCE oracle config/payload admission",
            ),
            (
                "/panel/max_payload_bytes",
                json!(1),
                "SOURCE oracle config/payload admission",
            ),
            (
                "/records/bytes",
                json!(263999999),
                "SOURCE oracle descriptor binding",
            ),
            (
                "/plane_manifest/bytes",
                json!(65537),
                "SOURCE oracle descriptor binding",
            ),
            (
                "/generation_manifest/sha256",
                json!("0".repeat(64)),
                "SOURCE oracle descriptor binding",
            ),
            (
                "/requests/sha256",
                json!("0".repeat(64)),
                "SOURCE oracle descriptor binding",
            ),
        ] {
            let mut bad = oracle.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            let bytes = serde_json::to_vec(&bad).unwrap();
            let path = dir.path().join("oracle-admission.json");
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(
                v9_panel::reduce_oracle(&path, &sha(&bytes))
                    .unwrap_err()
                    .to_string(),
                expected,
                "{pointer}"
            );
        }
        assert!(!pin.path.exists());
        let mut oversized_identity = rows[0].clone();
        oversized_identity["oversized"] = json!("x".repeat(CONFIG_CAP as usize));
        let error = v9_panel::read_candidate(
            &pin,
            &oversized_identity,
            &rows[1],
            &[0, 1, 127, 255, 511, 767, 998, 999],
            8 * 1024 * 1024,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("header cap/serialization"), "{error}");
        for (pointer, value) in [
            ("/rows", json!(100000)),
            ("/dimensions", json!(768)),
            ("/count", json!(8)),
            ("/k", json!(100)),
            ("/source_cache", json!("on")),
            ("/source_get_budget", json!(129)),
            ("/sq8_get_budget", json!(31)),
            ("/sq8_byte_budget", json!(33554433)),
            ("/truth_sha256", json!("0".repeat(64))),
            ("/requests_sha256", json!("0".repeat(64))),
            ("/native_source_sha256", json!("0".repeat(64))),
            ("/native_sq8_sha256", json!("0".repeat(64))),
            ("/native_order_sha256", json!("0".repeat(64))),
        ] {
            let mut inputs = rows[1].clone();
            *inputs.pointer_mut(pointer).unwrap() = value;
            let error = v9_panel::read_candidate(
                &pin,
                &rows[0],
                &inputs,
                &[0, 1, 127, 255, 511, 767, 998, 999],
                8 * 1024 * 1024,
            )
            .unwrap_err()
            .to_string();
            assert!(
                error.starts_with("v9 panel admission:"),
                "{pointer}: {error}"
            );
        }
    }

    #[test]
    fn bounded_files_reject_links_fifo_oversize_and_duplicate_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let inputs = four(dir.path());
        let link = dir.path().join("link");
        symlink(&inputs[0].0, &link).unwrap();
        assert!(read_run(&link, &inputs[0].1).is_err());
        let parent_link = dir.path().join("parent-link");
        symlink(dir.path(), &parent_link).unwrap();
        assert!(read_run(&parent_link.join("run-0"), &inputs[0].1).is_err());
        let fifo = dir.path().join("fifo");
        rustix::fs::mkfifoat(rustix::fs::CWD, &fifo, Mode::RUSR | Mode::WUSR).unwrap();
        assert!(read_run(&fifo, &inputs[0].1).is_err());
        let large = dir.path().join("large");
        File::create(&large).unwrap().set_len(FILE_CAP + 1).unwrap();
        assert!(read_run(&large, &inputs[0].1).is_err());
        std::fs::write(&large, vec![b' '; LINE_CAP + 1]).unwrap();
        assert!(read_run(&large, &inputs[0].1).is_err());
        let mut duplicate = inputs.clone();
        duplicate[3] = inputs[0].clone();
        assert!(compare(&duplicate).is_err());
        assert!(execute(&inputs, &link).is_err());
        assert!(execute(&inputs, &parent_link.join("report")).is_err());
    }
}

#[path = "compare_native_replay/fp32_serving.rs"]
mod fp32_serving;
