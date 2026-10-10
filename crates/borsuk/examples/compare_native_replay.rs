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
#![recursion_limit = "256"]

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
const MEMBERSHIP_ABBA_SCHEMA: &str = "borsuk-membership-abba-native-reduction-v1";
const CONFIG_CAP: u64 = 32 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletedInput {
    path: PathBuf,
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize)]
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
    require(
        object.len() == names.split_whitespace().count()
            && names
                .split_whitespace()
                .all(|name| object.contains_key(name)),
        "unknown/missing v2 field",
    )
}

fn validate_v2_row(line: &[u8], phase: &str) -> Result<()> {
    let UniqueJson(v) = serde_json::from_slice(line)?;
    let names = match phase {
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
    };
    if matches!(phase, "identity" | "bound_inputs") && v.get("fetch_parallelism").is_some() {
        require(
            matches!(v["fetch_parallelism"].as_u64(), Some(16 | 32)),
            "fetch_parallelism must be integer 16 or 32",
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
                "discovery source planning sq8 leaf_peak_inflight",
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
        "terminal" => fields(
            &v["summary"],
            "status complete queries k total_hits10 recall_numerator recall_denominator mean_recall10 underfilled_queries all_queries_sealed prefix_bytes prefix_sha256 sealed_bytes sealed_sha256 requests_sha256 truth_sha256 generation_root_sha256 charges sum query_wall_ns query_process_cpu_ns physical_s3_measured external_gate_required process_wall_ns process_cpu_ns observed_process_peak_bytes binding_charge transport_last_boundary",
        )?,
        _ => (),
    }
    Ok(())
}

fn validate_v2_identity(i: &Identity, raw: &Value) -> Result<()> {
    require(
        i.schema == "borsuk-cohere-native-baseline-result-v2"
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
    require(
        i.dataset == "CohereLabs/wikipedia-2023-11-embed-multilingual-v3"
            && i.revision == "ade45fb52bd549f5e8c065636fe4160a43c2af36"
            && i.metric == "cosine"
            && i.tie_rule == "corpus_ordinal_ascending"
            && !i.truth_opened
            && (K..=1_000_000).contains(&i.rows)
            && i.dimensions == 1024
            && i.k == K
            && (1..=1000).contains(&i.count)
            && matches!(i.profile.as_str(), "native100k" | "scale1m")
            && (i.profile != "native100k" || i.rows <= 100_000)
            && i.requests_bytes == i.count as u64 * 1024 * 4
            && i.truth_bytes == i.count as u64 * K as u64 * 8
            && raw["serving"] == json!({"mode":"baseline"})
            && raw["source_cache"] == "off"
            && raw["max_memory_bytes"] == 512 * 1024 * 1024_u64,
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
    if !cfg!(test) {
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
    let ordinals = scale_ordinals(raw, i.count)?;
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
            raw["schema"] == "borsuk-cohere-native-baseline-result-v7",
            "scale result v7 required",
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
    last: Option<TransportSpan>,
    stage_samples: [Vec<u64>; 4],
    startup: Value,
    terminal: Value,
    local_boundary: Value,
}
impl CompletedEvidence {
    fn new(binding_charge: Charge) -> Self {
        Self {
            binding_charge,
            last: None,
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
    router: Charge,
    source: Charge,
    sq8: Charge,
}

impl Charges {
    fn add(&mut self, other: Self) -> Result<()> {
        self.router.add(other.router)?;
        self.source.add(other.source)?;
        self.sq8.add(other.sq8)
    }

    fn sum(self) -> Result<Charge> {
        let mut sum = self.router;
        sum.add(self.source)?;
        sum.add(self.sq8)?;
        Ok(sum)
    }
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Hit {
    id: u64,
    score_bits: u32,
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
    fn add(&mut self, s: &Stages, wall_ns: u64) -> Result<()> {
        require(
            s.discovery.end_ns <= s.source.start_ns
                && s.source.end_ns <= s.planning.start_ns
                && s.planning.end_ns <= s.sq8.start_ns
                && s.sq8.end_ns <= wall_ns,
            "stage order/query wall",
        )?;
        for (sum, interval) in [
            (&mut self.discovery_ns, &s.discovery),
            (&mut self.source_ns, &s.source),
            (&mut self.planning_ns, &s.planning),
            (&mut self.sq8_ns, &s.sq8),
        ] {
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
    query_process_cpu_ns: u64,
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

#[derive(PartialEq, Eq)]
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

struct Rows {
    reader: BufReader<File>,
    line: Vec<u8>,
    bytes: u64,
    digest: Sha256,
    original: FileIdentity,
    v2: bool,
    scale: bool,
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
        })
    }

    fn sha(&self) -> String {
        format!("{:x}", self.digest.clone().finalize())
    }

    fn row<T: DeserializeOwned>(&mut self, phase: &str) -> Result<T> {
        self.line.clear();
        let n = Read::take(&mut self.reader, (LINE_CAP + 1) as u64)
            .read_until(b'\n', &mut self.line)?;
        self.bytes = plus(self.bytes, n as u64)?;
        require(
            n > 0
                && n <= LINE_CAP
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
        if self.scale {
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
    mut query: impl FnMut(&[u8], &Query, &Inputs) -> Result<()>,
) -> Result<Run> {
    require(valid_sha(sha), "result lowercase SHA256")?;
    let mut rows = Rows::open(path)?;
    if let Some(inode) = admitted_inode {
        require(
            (rows.original.dev, rows.original.ino) == inode,
            "parity run changed before first content read",
        )?;
    }
    rows.v2 = expected.is_some();
    let scale = expected.is_some_and(|c| c.schema == SCALE_CONFIG_SCHEMA);
    rows.scale = scale;
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
                identity.schema == "borsuk-cohere-native-baseline-result-v7",
                "scale result schema",
            )?;
            let mut raw = c.expected_identity.clone();
            raw["schema"] = json!("borsuk-cohere-native-baseline-result-v2");
            let identity: Identity = serde_json::from_value(raw.clone())?;
            validate_v2_identity(&identity, &raw)?;
        } else {
            validate_v2_identity(&identity, &c.expected_identity)?;
        }
    } else {
        identity.validate()?;
    }
    let inputs: Inputs = rows.row("bound_inputs")?;
    let local =
        scale && expected.is_some_and(|c| c.expected_bound_inputs["backend"]["kind"] == "local");
    let ordinals = if scale {
        scale_ordinals(
            &expected.ok_or("scale config")?.expected_bound_inputs,
            inputs.count,
        )?
    } else {
        (0..COUNT).collect()
    };
    let count = ordinals.len();
    let panel = scale
        && expected
            .is_some_and(|c| c.expected_bound_inputs["execution"]["mode"] == "diagnostic_panel");
    let mut completed = if let Some(c) = expected {
        require(
            serde_json::from_slice::<Value>(&rows.line)? == c.expected_bound_inputs,
            "expected v2 input/root/backend pins",
        )?;
        if scale {
            validate_scale_inputs(&inputs, &c.expected_bound_inputs)?;
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
            require(
                admission["truth_opened"] == false
                    && admission["selected_count"] == count
                    && admission["population_count"] == inputs.count
                    && admission["execution"] == c.expected_bound_inputs["execution"]
                    && admission["trace"] == false
                    && admission["population_percentiles_valid"] == false
                    && admission["full_cohort_qualification"] == false,
                "panel admission geometry/scope",
            )?;
        }
        let binding: Value = rows.row("source_binding")?;
        let binding_charge: Charge = serde_json::from_value(binding["charges"].clone())?;
        binding_charge.validate()?;
        require(
            binding_charge.submitted_gets == if local { 0 } else { 2 }
                && (if local {
                    binding_charge.verified_bytes == 0
                } else {
                    binding_charge.verified_bytes > 0
                })
                && binding_charge.failed_gets == 0,
            "successful source binding charges",
        )?;
        let mut evidence = CompletedEvidence::new(binding_charge);
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
    if let Some(e) = &mut completed {
        e.startup = serde_json::from_slice(&rows.line)?;
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
            let page_count = inputs.rows.div_ceil(256) as u64;
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
                q.charges.source.submitted_gets <= 128
                    && q.charges.source.verified_bytes <= 64 * 1024 * 1024
                    && q.charges.sq8.submitted_gets <= 32
                    && q.charges.sq8.verified_bytes <= 16_773_120,
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
        for (i, hit) in q.returned.iter().enumerate() {
            require(
                hit.id < inputs.rows as u64
                    && f32::from_bits(hit.score_bits).is_finite()
                    && !q.returned[..i].iter().any(|h| h.id == hit.id),
                "returned IDs/scores",
            )?;
        }
        require(
            q.returned.windows(2).all(|h| {
                f32::from_bits(h[0].score_bits)
                    .total_cmp(&f32::from_bits(h[1].score_bits))
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
        stages.add(&q.stages, q.query_wall_ns)?;
        if let Some(e) = &mut completed {
            e.transport_scale(&rows.line, "query", Some(ordinal), local)?;
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
        cpu = plus(cpu, q.query_process_cpu_ns)?;
        query(&rows.line, &q, &inputs)?;
        samples.push(Sample {
            returned: q.returned,
            charges: q.charges,
            hits10: 0,
            wall_ns: q.query_wall_ns,
            trace_sha256,
        });
    }
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
            && t.process_wall_ns >= wall
            && t.process_cpu_ns >= cpu,
        "terminal charge/time totals",
    )?;
    if let Some(e) = &mut completed {
        let raw: Value = serde_json::from_slice(&rows.line)?;
        e.terminal = raw["summary"].clone();
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
const HEADER_RESULT_CAP: u64 = 32 * 1024 * 1024;
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

fn header_json(pin: &CompletedInput) -> Result<Value> {
    header_pin(pin, HEADER_JSON_CAP)?;
    let file = header_file(&pin.path)?;
    let original = file_identity(&file)?;
    require(original.len == pin.bytes, "header JSON length")?;
    let (value, _) = read_config_file(&file, &pin.path, &pin.sha256, HEADER_JSON_CAP)?;
    recheck_header_file(&file, &pin.path, &original)?;
    Ok(value)
}

fn recheck_header_file(file: &File, path: &Path, original: &FileIdentity) -> Result<()> {
    require(
        file.metadata()?.nlink() == 1
            && file_identity(file)? == *original
            && file_identity(&header_file(path)?)? == *original,
        "header artifact mutation/path identity",
    )
}

fn authenticated_headers(pin: &CompletedInput) -> Result<[Value; 2]> {
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
    Ok([identity, inputs])
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

fn bind_completed_scale(authority_path: &Path, authority_sha: &str) -> Result<Value> {
    let file = header_file(authority_path)?;
    let original = file_identity(&file)?;
    let (a, _): (HeaderAuthority, _) =
        read_config_file(&file, authority_path, authority_sha, HEADER_JSON_CAP)?;
    recheck_header_file(&file, authority_path, &original)?;
    let c = header_json(&a.config)?;
    let receipt = header_json(&a.cohort)?;
    let [identity, bound] = expected_scale_headers(&a, c, &receipt)?;
    require(
        authenticated_headers(&a.result)? == [identity.clone(), bound.clone()],
        "native headers differ from independent authority",
    )?;
    let value = json!({"schema":SCALE_CONFIG_SCHEMA,
        "input":{"path":a.result.path,"bytes":a.result.bytes,"sha256":a.result.sha256},
        "expected_identity":identity,"expected_bound_inputs":bound});
    require(
        (serde_json::to_vec(&value)?.len() as u64) < CONFIG_CAP,
        "completed config cap",
    )?;
    Ok(value)
}

#[cfg(test)]
mod completed_header_binding_tests {
    use super::*;

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
        bind_completed_scale(&path, pin["sha256"].as_str().unwrap())
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
            RESERVED_QUERIES_SHA.to_owned()
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
            "revision":"ade45fb52bd549f5e8c065636fe4160a43c2af36","reserved_queries_sha256":RESERVED_QUERIES_SHA,
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
        let sources: serde_json::Map<String, Value> = BASELINE_HEADER_SOURCES
            .iter()
            .map(|(key, value)| (key.to_string(), json!(value)))
            .collect();
        // Independent full native-emission golden: never call expected_scale_headers here.
        let mut identity = json!({"schema":"borsuk-cohere-native-baseline-result-v7","phase":"identity",
            "config_sha256":config_pin["sha256"],"binary_sha256":BASELINE_BINARY_SHA,
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
            "corpus_intervals":intervals,"reserved_query_interval":reserved,"reserved_queries_sha256":RESERVED_QUERIES_SHA,
            "cohort_receipt_sha256":cohort["sha256"],"derivation_receipt_sha256":"6".repeat(64),"producer_authority":producer,
            "max_memory_bytes":536_870_912,"fetch_parallelism":16,"serving":{"mode":"baseline"},"source_cache":"off",
            "execution":execution,"selected_count":32,"truth_opened":false});
        let headers = [identity, bound];
        let result = put(&dir.join("result.jsonl"), &result_body(&headers));
        (
            json!({"schema":HEADER_AUTHORITY_SCHEMA,"config":config_pin,"result":result,
            "cohort":cohort,"binary_sha256":BASELINE_BINARY_SHA,"sources":sources}),
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
                        score_bits: (id as f32).to_bits(),
                    })
                    .collect(),
                charges: Charges::default(),
                hits10: K as u64,
                wall_ns: 1,
                trace_sha256: [0; 32],
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
    let (c, config_bytes): (PairedConfig, _) = read_config(config_path, config_sha)?;
    require(
        c.schema == "borsuk-paired-native-reduction-config-v1",
        "paired config schema",
    )?;
    for (arm, parallelism) in c.arms.iter().zip([16, 32]) {
        require(
            arm.schema == "borsuk-completed-native-reduction-config-v1"
                && arm.expected_identity["fetch_parallelism"] == parallelism
                && arm.expected_bound_inputs["fetch_parallelism"] == parallelism,
            "paired arms must be ordered integer fetch_parallelism 16 then 32",
        )?;
        // This experiment compares the retained production Cohere panel, not
        // arbitrary matching outputs. Admit its pins before either input opens;
        // the generic completed reducer and frozen historical mode stay separate.
        let i: Inputs = serde_json::from_value(arm.expected_bound_inputs.clone())?;
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
    let [a, b] = &c.arms;
    let mut identity = a.expected_identity.clone();
    require(
        identity["config_sha256"] != b.expected_identity["config_sha256"],
        "paired native configs must have distinct SHA256",
    )?;
    identity["config_sha256"] = b.expected_identity["config_sha256"].clone();
    identity["fetch_parallelism"] = json!(32);
    let mut inputs = a.expected_bound_inputs.clone();
    inputs["fetch_parallelism"] = json!(32);
    require(
        identity == b.expected_identity && inputs == b.expected_bound_inputs,
        "paired binary/source/benchmark/generation/backend/input/scoring pins differ",
    )?;
    let runs = [
        read_run_with(&a.input.path, &a.input.sha256, Some(a))?,
        read_run_with(&b.input.path, &b.input.sha256, Some(b))?,
    ];
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
                && a.trace_sha256 == b.trace_sha256,
            &format!("paired query {ordinal}: ordered hits/recall/plan/logical GET/bytes mismatch"),
        )?;
    }
    let mut reports = Vec::with_capacity(2);
    for (arm, run) in c.arms.iter().zip(&runs) {
        // The maximum includes every query. Native100k selects at most16
        // leaves, so neither fetch width can admit a larger per-query peak.
        require(
            run.stages.max_leaf_peak_inflight <= 16,
            "paired Native100k query leaf_peak_inflight exceeds 16",
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
    Ok(
        json!({"schema":PAIRED_SCHEMA,"status":"MEASURED","complete":true,
        "config_path":config_path,"config_sha256":config_sha,"config_bytes":config_bytes,
        "semantic_parity":true,"trace_plan_parity":true,"logical_charge_parity":true,
        "arms":reports,"pooled_both_arms":{"statistics":pooled,"stage_wall_sums":stage_totals},
        "statistics_population":"all 1000 ordered queries per arm including first; pooled_both_arms includes all 2000 observations without arm selection",
        "percentile_method":"nearest_rank",
        "sequential_qps_definition":"query_count * 1e9 / sum(query_wall_ns); not concurrent service QPS",
        "plan_scope":"complete ordered TwoBitPlanTrace; timing, inflight and transport fields are outside the plan",
        "source_cache":"off","cache_state":"external cold/cache qualification required",
        "local_file_only":true,"physical_s3":false,"external_resources_and_cost_gate_required":true,
        "frozen_runtime_root_config_admission_required":true,"qualified":false,
        "supervisor_resources_cost_and_cache_qualified":false,
        "vendor_or_scientific_win_claim":false,"performance_pass_claim":false,
        "wire_bytes":null,"unread_bytes":null,"billed_bytes":null,"billed_requests":null}),
    )
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

struct Output {
    file: File,
    bytes: u64,
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() as u64 > REPORT_CAP.saturating_sub(self.bytes) {
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
    let mut out = Output { file, bytes: 0 };
    serde_json::to_writer(&mut out, &report)?;
    out.write_all(b"\n")?;
    out.file.sync_all()?;
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
    dir.sync_all()?;
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

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    let binding = args
        .get(1)
        .is_some_and(|arg| arg == "--bind-completed-scale");
    let result = (|| -> Result<bool> {
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
            return execute_report(output, SCALE_CONFIG_SCHEMA, || Ok(config));
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

    fn encode(rows: &[Value]) -> Vec<u8> {
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

    fn authenticate(rows: &mut [Value]) {
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
    fn actual_native_scale_runner_reducer_seal_count_mismatches() {
        for (prefix, panel) in [(false, false), (false, true), (true, false)] {
            let (dir, result) =
                scale_native_runner::scale_reducer_native_fixture(prefix, panel).unwrap();
            let body = std::fs::read_to_string(&result).unwrap();
            let rows = body
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>();
            let (config, pin) = scale_config(dir.path(), &result, &rows);
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
                    "old" => bad[0]["schema"] = json!("borsuk-cohere-native-baseline-result-v5"),
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
                    "old" => "scale result v7 required",
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
