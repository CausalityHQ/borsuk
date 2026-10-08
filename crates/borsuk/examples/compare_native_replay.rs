//! Offline reduction of four historical A1/B1/B2/A2 files or sealed v2 outputs.
//! This does not qualify resources, cost, cache state, or a vendor comparison.
//! Single-file CLI: --completed-v2 CONFIG CONFIG_SHA256 NEW_OUTPUT_JSON.
//! CONFIG pins the exact identity/bound_inputs rows and input path/bytes/SHA256.
//! Paired CLI: --paired-v2 CONFIG CONFIG_SHA256 NEW_OUTPUT_JSON. CONFIG has
//! schema borsuk-paired-native-reduction-config-v1 and two CompletedConfig arms,
//! ordered fetch_parallelism 16 then 32, with source_cache off in both.

use rustix::fs::{Mode, OFlags, openat};
use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, IgnoredAny},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::File,
    io::{self, BufRead, BufReader, Read, Write},
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
}
impl CompletedEvidence {
    fn new(binding_charge: Charge) -> Self {
        Self {
            binding_charge,
            last: None,
            stage_samples: std::array::from_fn(|_| Vec::with_capacity(COUNT)),
            startup: Value::Null,
            terminal: Value::Null,
        }
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
        if self.v2 {
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
    require(valid_sha(sha), "result lowercase SHA256")?;
    let mut rows = Rows::open(path)?;
    rows.v2 = expected.is_some();
    if let Some(c) = expected {
        require(rows.original.len == c.input.bytes, "expected result bytes")?;
    }
    let identity: Identity = rows.row("identity")?;
    if let Some(c) = expected {
        require(
            serde_json::from_slice::<Value>(&rows.line)? == c.expected_identity,
            "expected v2 identity pins",
        )?;
        validate_v2_identity(&identity, &c.expected_identity)?;
    } else {
        identity.validate()?;
    }
    let inputs: Inputs = rows.row("bound_inputs")?;
    let mut completed = if let Some(c) = expected {
        require(
            serde_json::from_slice::<Value>(&rows.line)? == c.expected_bound_inputs,
            "expected v2 input/root/backend pins",
        )?;
        validate_v2_inputs(&inputs, &c.expected_bound_inputs)?;
        require(
            c.expected_identity.get("fetch_parallelism")
                == c.expected_bound_inputs.get("fetch_parallelism"),
            "identity/input fetch_parallelism agreement",
        )?;
        let binding: Value = rows.row("source_binding")?;
        let binding_charge: Charge = serde_json::from_value(binding["charges"].clone())?;
        binding_charge.validate()?;
        require(
            binding_charge.submitted_gets == 2
                && binding_charge.verified_bytes > 0
                && binding_charge.failed_gets == 0,
            "successful source binding charges",
        )?;
        let mut evidence = CompletedEvidence::new(binding_charge);
        evidence.transport(&rows.line, "native_source", None)?;
        rows.row::<IgnoredAny>("generation_open")?;
        evidence.transport(&rows.line, "generation_open", None)?;
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
    let mut samples = Vec::with_capacity(COUNT);
    let mut charges = Charges::default();
    let mut stages = StageTotals::default();
    let (mut wall, mut cpu) = (0, 0);
    let mut underfilled = 0;
    for ordinal in 0..COUNT {
        let q: Query = rows.row("query")?;
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
            e.transport(&rows.line, "query", Some(ordinal))?;
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
        seal.count == COUNT
            && !seal.truth_opened
            && seal.prefix_bytes == prefix_bytes
            && seal.prefix_sha256 == prefix_sha
            && seal.requests_sha256 == inputs.requests_sha256
            && seal.generation_root_sha256 == inputs.generation_root_sha256
            && seal.requires_successful_sync
            && seal.requires_successful_directory_sync,
        "authenticated query seal",
    )?;
    let (sealed_bytes, sealed_sha) = (rows.bytes, rows.sha());
    let mut hits = 0;
    for (ordinal, sample) in samples.iter_mut().enumerate() {
        let recall: Recall = rows.row("recall")?;
        require(
            recall.ordinal == ordinal
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
            && t.queries == COUNT
            && t.k == K
            && t.total_hits10 == hits
            && t.recall_numerator == hits
            && t.recall_denominator == (COUNT * K) as u64
            && t.mean_recall10 == hits as f64 / (COUNT * K) as f64
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
        let binding_charge: Charge = serde_json::from_value(e.terminal["binding_charge"].clone())?;
        binding_charge.validate()?;
        require(
            binding_charge == e.binding_charge,
            "terminal source binding charge",
        )?;
        require(
            e.terminal["transport_last_boundary"]
                == e.last.as_ref().ok_or("missing transport")?.compact(),
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
    let mut file = open_input(config_path)?;
    let original = file_identity(&file)?;
    require(
        original.len > 0 && original.len <= CONFIG_CAP,
        "small reduction config",
    )?;
    let mut body = Vec::with_capacity(original.len as usize);
    Read::take(&mut file, CONFIG_CAP + 1).read_to_end(&mut body)?;
    require(
        body.len() as u64 == original.len
            && file_identity(&file)? == original
            && file_identity(&open_input(config_path)?)? == original
            && format!("{:x}", Sha256::digest(&body)) == config_sha,
        "config SHA/length/identity",
    )?;
    let UniqueJson(value) = serde_json::from_slice(&body)?;
    Ok((serde_json::from_value(value)?, original.len))
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
    Ok(
        json!({"schema":COMPLETED_SCHEMA,"status":"MEASURED","complete":true,
        "result_path":c.input.path,"result_bytes":run.file_identity.len,"result_sha256":c.input.sha256,
        "identity":c.expected_identity,"inputs":c.expected_bound_inputs,"statistics":stats,
        "percentile_method":"nearest_rank","sequential_qps_definition":"query_count * 1e9 / sum(query_wall_ns); not concurrent service QPS",
        "stage_wall_sums":run.stages,"stage_statistics":distributions,
        "unattributed_query_wall_ns":stats.query_wall_ns.checked_sub(accounted).ok_or("stage sum exceeds query wall")?,
        "stage_scope":"recorded query intervals; discovery includes preparation/router reads, source is authenticated source fetch, planning is source scoring/SQ8 planning, sq8 is fetch/rank; no inferred network-only costs",
        "startup":e.startup,"native_terminal":e.terminal,"recall_source":"authenticated native terminal checked against ordered sealed recall rows; truth bodies not reopened",
        "transport_scope":"cumulative process SDK observations including S3 and IMDS credential requests including PUT; not per-query logical charges or wire/billed accounting",
        "wire_bytes":null,"unread_bytes":null,"billed_bytes":null,"billed_requests":null,
        "local_file_only":true,"external_resources_and_cost_gate_required":true,"qualified":false,
        "vendor_or_scientific_win_claim":false,"performance_pass_claim":false}),
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
    let current = file_identity(&open_input(output)?)?;
    let bound_dir = dir.metadata()?;
    let current_dir = parent(output)?.metadata()?;
    require(
        (current.dev, current.ino, current.len) == (original.dev, original.ino, out.bytes)
            && (bound_dir.dev(), bound_dir.ino()) == (current_dir.dev(), current_dir.ino()),
        "output/parent identity changed",
    )?;
    dir.sync_all()?;
    Ok(report["status"] == "MEASURED")
}

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    let result = (|| -> Result<bool> {
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
        Ok(false) => std::process::exit(2),
        Err(e) => {
            eprintln!("INVALID: {e}; external gates required");
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::os::unix::{ffi::OsStringExt, fs::symlink};

    fn sha(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
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
