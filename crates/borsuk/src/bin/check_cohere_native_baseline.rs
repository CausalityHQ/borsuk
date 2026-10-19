//! Authenticated Local/S3 Cohere quality baseline; external scientific gates required.
use borsuk::{
    exact_sq8_nominee::ScoredNominee,
    semantic_unit_router::SemanticProfile,
    sq8_s3_range::{
        BoundedReadS3, InFlightBytes, NativeTransportStats, OneAttemptS3, QueryProfile,
        S3ReadRetryPolicy, Sq8RangeSplit, Sq8RangeTrace, Sq8ReadStats,
    },
    two_bit_generation::{
        DirectClosureLimits, DirectClosureMemory, DirectClosureRejection, DiscoveryMode,
        SourceLayout, TwoBitGeneration, TwoBitGenerationError, TwoBitGenerationLimits,
        TwoBitPlanTrace,
    },
    two_bit_source::SourcePlaneReceipt,
};
use futures_util::StreamExt;
use object_store::{
    ObjectStore, ObjectStoreExt, chunked::ChunkedStore, local::LocalFileSystem,
    path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    error::Error,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    mem::size_of,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const D: usize = 1024;
const K: usize = 10;
const CONFIG_CAP: usize = 65_536;
const LINE_CAP: usize = 256 * 1024;
const OUTPUT_CAP: u64 = 64 * 1024 * 1024;
const TERMINAL_RESERVE: u64 = 8192;
#[cfg(test)]
const MEMORY_CAP: u64 = 512 * 1024 * 1024;
const BLOCK: usize = 65_536;
const DATASET: &str = "CohereLabs/wikipedia-2023-11-embed-multilingual-v3";
const REVISION: &str = "ade45fb52bd549f5e8c065636fe4160a43c2af36";
// CPU-profile admission and diagnostics have their own config/result protocol.
const CONFIG_SCHEMA: &str = "borsuk-cohere-native-baseline-config-v19";
const RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v21";
const CONCURRENT_CONFIG_SCHEMA: &str = "borsuk-cohere-native-baseline-config-v21";
const CONCURRENT_RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v22";
// Checked against each unpolled future, plus bounded join/result bookkeeping.
const QUERY_FUTURE_BYTES: usize = 512 * 1024;
// Cohort snapshot JSON and its terminal copy can coexist after query futures drain.
const COHORT_RECORD_BYTES: usize = 1024 * 1024;
const SOURCE_CAPSULE_VERIFICATION_POLICY: &str = "directory_page_ranges_original_units_v2";
const DIAGNOSTIC_SCHEMA: &str = "borsuk-sq8-range-attribution-diagnostic-v6";
// Diagnostic panel bounds. A panel line is serialized and admitted whole (newline included)
// below this cap BEFORE any byte is published; full execution keeps LINE_CAP unchanged.
const PANEL_MAX_ORDINALS: usize = 128;
const PANEL_LINE_CAP: usize = 128 * 1024;
// Two 64-KiB raw authorities may each expand sixfold in JSON; the envelope
// retains its original 64-KiB bound. No query or historical record uses this cap.
const SOURCE_BINDING_LINE_CAP: usize = 2 * 6 * CONFIG_CAP + 64 * 1024;
const PANEL_ERROR_CHARS: usize = 2048;
// Per active query: range trace, host snapshot buffers and serialization together.
const DIAGNOSTIC_CAP: usize = 2 << 20;
const HOST_READ_CAP: usize = 128 * 1024;
const HOST_JSON_BYTES: usize = 8 * 1024;
// A failed diagnostic query also materializes one JSON tree of its diagnostic object.
const FAILURE_VALUE_BYTES: usize = 256 * 1024;
#[cfg(test)]
const BASELINE_QUERY_GETS: usize = 32;
const BASELINE_SOURCE_GETS: usize = 128;

/// Which serving path answers each query. Required in every config: there is no default.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum Serving {
    /// Historical SOURCE nomination, then SQ8 under the unchanged baseline caps.
    Baseline {},
    /// Exact canonical refinement of the unchanged baseline fetch.
    Fp32Rerank {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        direct: Option<DirectClosureLimits>,
        #[serde(default)]
        coalesce: bool,
        shortlist: usize,
        max_fp32_bytes: usize,
        max_fp32_gets: usize,
        max_parallel_fp32_gets: usize,
    },
    /// Opt-in direct closure: no SOURCE I/O. These caps are explicit and independent of
    /// the baseline `max_query_*` limits.
    DirectClosure {
        max_sq8_bytes: usize,
        max_sq8_gets: usize,
    },
}
impl Serving {
    fn fp32(self) -> Option<borsuk::fp32_rerank::Fp32RerankLimits> {
        match self {
            Self::Fp32Rerank {
                direct,
                coalesce,
                shortlist,
                max_fp32_bytes,
                max_fp32_gets,
                max_parallel_fp32_gets,
            } => Some(borsuk::fp32_rerank::Fp32RerankLimits {
                direct,
                coalesce,
                shortlist,
                max_bytes: max_fp32_bytes,
                max_gets: max_fp32_gets,
                max_parallel_gets: max_parallel_fp32_gets,
            }),
            _ => None,
        }
    }
    fn direct(self) -> Option<DirectClosureLimits> {
        match self {
            Self::Baseline {} | Self::Fp32Rerank { .. } => None,
            Self::DirectClosure {
                max_sq8_bytes,
                max_sq8_gets,
            } => Some(DirectClosureLimits {
                max_sq8_bytes,
                max_sq8_gets,
            }),
        }
    }
}

/// Which queries run. Required in every config: there is no default.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum Execution {
    /// All `count` original ordinals: the population run.
    Full {},
    /// Diagnostic only: exactly these original ordinals, once each, sorted and unique.
    /// Never population percentiles and never full-cohort qualification. `trace` turns on
    /// the opt-in SQ8 range diagnostics and the outside-the-window host counter snapshots.
    DiagnosticPanel { ordinals: Vec<usize>, trace: bool },
}
impl Execution {
    fn panel(&self) -> bool {
        matches!(self, Self::DiagnosticPanel { .. })
    }
    fn trace(&self) -> bool {
        matches!(self, Self::DiagnosticPanel { trace: true, .. })
    }
}

/// A valid-input direct-closure refusal by an enforced library cap. This is a resource
/// outcome of the candidate, not a malformed-config, authentication or transport defect.
/// Host OOM, deadline and kill outcomes are never inferred here.
#[derive(Debug)]
struct ResourceReject {
    reason: DirectClosureRejection,
    message: String,
}
impl std::fmt::Display for ResourceReject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "resource rejection {:?}: {}", self.reason, self.message)
    }
}
impl Error for ResourceReject {}

/// Terminal classification. Exit codes: MEASURED 0, INVALID 2, RESOURCE_REJECT 3.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    Measured,
    Invalid,
    ResourceReject,
}
impl Status {
    fn exit_code(self) -> i32 {
        match self {
            Self::Measured => 0,
            Self::Invalid => 2,
            Self::ResourceReject => 3,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum Backend {
    Local {
        store_root: PathBuf,
    },
    S3 {
        bucket: String,
        region: String,
        physical_prefix: String,
        sq8_object_key: String,
        sq8_etag: String,
    },
}

enum Reader {
    Local(ChunkedStore),
    S3(OneAttemptS3),
    S3ReadRetry(BoundedReadS3),
    #[cfg(test)]
    Stub {
        store: Arc<dyn ObjectStore>,
        stats: Arc<std::sync::Mutex<NativeTransportStats>>,
    },
}
impl Reader {
    fn new(backend: &Backend, read_retry: Option<S3ReadRetryPolicy>) -> Result<Self> {
        Ok(match backend {
            Backend::Local { store_root } => Self::Local(ChunkedStore::new(
                Arc::new(LocalFileSystem::new_with_prefix(store_root)?),
                8192,
            )),
            Backend::S3 {
                bucket,
                region,
                physical_prefix,
                ..
            } => match read_retry {
                Some(policy) => Self::S3ReadRetry(
                    BoundedReadS3::new_with_prefix(
                        bucket,
                        region,
                        ObjectPath::from(physical_prefix.as_str()),
                        policy,
                    )
                    .map_err(|e| format!("S3 read-retry reader: {e:?}"))?,
                ),
                None => Self::S3(
                    OneAttemptS3::new_with_prefix(
                        bucket,
                        region,
                        ObjectPath::from(physical_prefix.as_str()),
                    )
                    .map_err(|e| format!("S3 reader: {e:?}"))?,
                ),
            },
        })
    }
    fn store(&self) -> &dyn ObjectStore {
        match self {
            Self::Local(store) => store,
            Self::S3(reader) => reader.store(),
            Self::S3ReadRetry(reader) => reader.store(),
            #[cfg(test)]
            Self::Stub { store, .. } => store.as_ref(),
        }
    }
    fn snapshot(&self) -> Option<NativeTransportStats> {
        match self {
            Self::Local(_) => None,
            Self::S3(reader) => Some(reader.transport_stats()),
            Self::S3ReadRetry(reader) => Some(reader.transport_stats()),
            #[cfg(test)]
            Self::Stub { stats, .. } => Some(stats.lock().unwrap().clone()),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    path: PathBuf,
    bytes: usize,
    sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeSource {
    source_sha256: String,
    sq8_sha256: String,
    source_order_sha256: String,
}
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ProducerAuthority {
    source_commit: String,
    executable_sha256: String,
    producer_source_sha256: String,
    sq8_source_sha256: String,
    source_order_source_sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DerivationSeal {
    bytes: usize,
    sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DerivationOutputs {
    normalized: DerivationSeal,
    source_order: DerivationSeal,
    sq8: DerivationSeal,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DerivationAdmission {
    wrapper_payload_bytes: usize,
    normalization_api_payload_bytes: usize,
    flat_fit_api_payload_upper_bound_bytes: usize,
    sq8_api_payload_bytes: usize,
    peak_payload_upper_bound_bytes: usize,
    aggregate_scratch_upper_bound_bytes: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DerivationReceipt {
    schema: String,
    producer_config_sha256: String,
    status: String,
    recipe: String,
    query_or_truth_used: bool,
    original_corpus: DerivationSeal,
    rows: usize,
    dimensions: usize,
    corpus_intervals: Vec<SourceInterval>,
    outputs: DerivationOutputs,
    low_f32_bits: Vec<u32>,
    step_f32_bits: Vec<u32>,
    producer_authority: ProducerAuthority,
    source_identity_qualification: String,
    admission: DerivationAdmission,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    #[serde(default)]
    max_active_queries: Option<usize>,
    #[serde(default)]
    query_runtime: QueryRuntime,
    dataset: String,
    revision: String,
    metric: String,
    tie_rule: String,
    corpus_intervals: Vec<SourceInterval>,
    reserved_query_interval: SourceInterval,
    cohort_receipt: Artifact,
    derivation_receipt: Artifact,
    producer_authority: ProducerAuthority,
    corpus_source_first: usize,
    query_source_first: usize,
    rows: usize,
    dimensions: usize,
    count: usize,
    k: usize,
    profile: SemanticProfile,
    #[serde(default, deserialize_with = "explicit_semantic_leaf_limit")]
    semantic_leaf_limit: Option<usize>,
    backend: Backend,
    #[serde(default)]
    s3_read_retry: Option<S3ReadRetryPolicy>,
    generation_prefix: String,
    generation_root_sha256: String,
    scratch_parent: PathBuf,
    requests: Artifact,
    truth: Artifact,
    native_source: NativeSource,
    max_memory_bytes: u64,
    source_byte_budget: usize,
    #[serde(default)]
    source_cache_bytes: usize,
    source_layout: SourceLayout,
    // Explicit operating-point limits, independent of transport concurrency.
    sq8_get_budget: usize,
    sq8_byte_budget: usize,
    #[serde(default = "default_fetch_parallelism")]
    fetch_parallelism: usize,
    #[serde(default = "default_source_get_budget")]
    source_get_budget: usize,
    #[serde(default)]
    source_authentication: borsuk::sq8_s3_range::SourceAuthentication,
    #[serde(default)]
    shared_byte_window: Option<SharedByteWindow>,
    #[serde(deserialize_with = "explicit_sq8_range_split")]
    sq8_range_split: Option<Sq8RangeSplit>,
    serving: Serving,
    execution: Execution,
}
fn explicit_semantic_leaf_limit<'de, T: serde::Deserializer<'de>>(
    deserializer: T,
) -> std::result::Result<Option<usize>, T::Error> {
    usize::deserialize(deserializer).map(Some)
}
fn explicit_sq8_range_split<'de, T: serde::Deserializer<'de>>(
    deserializer: T,
) -> std::result::Result<Option<Sq8RangeSplit>, T::Error> {
    Option::<Sq8RangeSplit>::deserialize(deserializer)
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SharedByteWindow {
    max_bytes: usize,
    max_waiters: usize,
    max_state_bytes: usize,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SourceInterval {
    start: usize,
    end: usize,
}

fn population(c: &Config, shape: Shape) -> Shape {
    if cfg!(test) {
        shape
    } else {
        Shape {
            rows: c.rows,
            count: c.count,
            #[cfg(test)]
            returned_limit: K,
        }
    }
}

fn validate_population(c: &Config) -> Result<()> {
    require(
        c.rows >= K
            && c.rows <= 1_000_000
            && c.count > 0
            && c.count <= 1000
            && matches!(
                c.profile,
                SemanticProfile::Native100k | SemanticProfile::Scale1m
            )
            && c.profile.valid_geometry(c.rows, D),
        "explicit bounded population/profile",
    )?;
    let query_end = c
        .query_source_first
        .checked_add(c.count)
        .ok_or("query interval overflow")?;
    let reserved = c.reserved_query_interval;
    require(
        reserved.start < reserved.end
            && reserved.end - reserved.start <= 1000
            && c.query_source_first == reserved.start
            && query_end <= reserved.end,
        "selected query prefix inside explicit reserved interval",
    )?;
    require(
        !c.corpus_intervals.is_empty() && c.corpus_intervals.len() <= 2,
        "explicit corpus intervals",
    )?;
    let mut count = 0_usize;
    let mut previous = 0;
    for interval in &c.corpus_intervals {
        require(
            interval.start < interval.end
                && interval.start >= previous
                && (interval.end <= reserved.start || interval.start >= reserved.end),
            "nonoverlapping corpus/query intervals",
        )?;
        count = count
            .checked_add(interval.end - interval.start)
            .ok_or("corpus interval overflow")?;
        previous = interval.end;
    }
    require(count == c.rows, "exact corpus interval count")?;
    if !cfg!(test) {
        let expected = if c.rows == 100_000 {
            vec![SourceInterval {
                start: 0,
                end: 100_000,
            }]
        } else {
            vec![
                SourceInterval {
                    start: 0,
                    end: 100_000,
                },
                SourceInterval {
                    start: 101_000,
                    end: c.rows.checked_add(1000).ok_or("corpus source overflow")?,
                },
            ]
        };
        require(
            c.rows >= 100_000
                && reserved
                    == (SourceInterval {
                        start: 100_000,
                        end: 101_000,
                    })
                && c.corpus_intervals == expected,
            "permanent historical query exclusion",
        )?;
    }
    Ok(())
}

#[derive(Debug)]
struct ReceiptBinding {
    reserved_queries_sha256: String,
    low_f32_bits: Vec<u32>,
    step_f32_bits: Vec<u32>,
}
fn validate_receipts(c: &Config) -> Result<ReceiptBinding> {
    let body = checked(&c.cohort_receipt, CONFIG_CAP)?.0;
    let receipt: Value = serde_json::from_slice(&body)?;
    require(
        receipt["schema"] == "borsuk-cohere-native-cohort-receipt-v3"
            && receipt["status"] == "COMPLETE"
            && receipt["dataset"] == c.dataset
            && receipt["revision"] == c.revision
            && receipt["geometry"]["corpus_rows"] == c.rows
            && receipt["geometry"]["query_rows"] == c.count
            && receipt["geometry"]["dimensions"] == D
            && receipt["geometry"]["k"] == K
            && receipt["geometry"]["corpus_intervals"] == json!(c.corpus_intervals)
            && receipt["geometry"]["reserved_query_interval"] == json!(c.reserved_query_interval)
            && receipt["geometry"]["query_source_ordinals"]
                == json!([c.query_source_first, c.query_source_first + c.count]),
        "authenticated original cohort geometry",
    )?;
    let reserved_sha = receipt["reserved_queries_sha256"]
        .as_str()
        .ok_or("full reserved query digest")?;
    require(
        valid_sha(reserved_sha)
            && (cfg!(test)
                || reserved_sha
                    == "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e"),
        "authenticated full historical reserved query body",
    )?;
    require(
        c.count != c.reserved_query_interval.end - c.reserved_query_interval.start
            || c.requests.sha256 == reserved_sha,
        "full selected request/reserved query digest binding",
    )?;
    let outputs = receipt["outputs"].as_array().ok_or("cohort output seals")?;
    let output = |name: &str| -> Result<&Value> {
        let mut found = outputs.iter().filter(|v| v["name"] == name);
        let seal = found.next().ok_or("missing original cohort output seal")?;
        require(
            found.next().is_none() && seal["sha256"].as_str().is_some_and(valid_sha),
            "unique output seal/SHA",
        )?;
        Ok(seal)
    };
    for (name, artifact) in [("queries.f32", &c.requests), ("truth.u64", &c.truth)] {
        let seal = output(name)?;
        require(
            seal["bytes"] == artifact.bytes && seal["sha256"] == artifact.sha256,
            "cohort request/truth identity binding",
        )?;
    }
    let corpus = output("corpus.f32")?;
    require(
        corpus["bytes"] == c.rows.checked_mul(D * 4).ok_or("corpus bytes overflow")?,
        "original corpus geometry",
    )?;
    let body = checked(&c.derivation_receipt, CONFIG_CAP)?.0;
    // Refuse the obsolete Q-specific receipt explicitly; the producer now binds
    // source bytes independently of the selected query prefix.
    let schema: Value = serde_json::from_slice(&body)?;
    require(
        schema["schema"] == "borsuk-native-scale-derivation-receipt-v2",
        "derivation receipt v2 required; v1 is incompatible",
    )?;
    drop(schema);
    let derivation: DerivationReceipt = serde_json::from_slice(&body)?;
    require(
        derivation.schema == "borsuk-native-scale-derivation-receipt-v2"
            && valid_sha(&derivation.producer_config_sha256)
            && derivation.status == "COMPLETE"
            && derivation.recipe == "normalize_then_flat_fit_then_sq8"
            && !derivation.query_or_truth_used
            && derivation.rows == c.rows
            && derivation.dimensions == D
            && derivation.corpus_intervals == c.corpus_intervals
            && json!(derivation.original_corpus.bytes) == corpus["bytes"]
            && derivation.original_corpus.sha256
                == corpus["sha256"].as_str().ok_or("original corpus SHA")?
            && derivation.outputs.normalized.bytes == derivation.original_corpus.bytes
            && derivation.outputs.normalized.sha256 == c.native_source.source_sha256
            && derivation.outputs.source_order.bytes
                == c.rows.checked_mul(8).ok_or("order bytes overflow")?
            && derivation.outputs.source_order.sha256 == c.native_source.source_order_sha256
            && derivation.outputs.sq8.bytes
                == c.rows.checked_mul(D + 12).ok_or("SQ8 bytes overflow")?
            && derivation.outputs.sq8.sha256 == c.native_source.sq8_sha256,
        "authenticated source-only original/normalized/fit/SQ8 derivation",
    )?;
    require(
        derivation.producer_authority == c.producer_authority
            && derivation.source_identity_qualification
                == "external_frozen_prerequisite_not_self_certified",
        "root-frozen derivation producer authority",
    )?;
    require(
        derivation.low_f32_bits.len() == D
            && derivation.step_f32_bits.len() == D
            && derivation
                .low_f32_bits
                .iter()
                .all(|&b| f32::from_bits(b).is_finite())
            && derivation
                .step_f32_bits
                .iter()
                .all(|&b| f32::from_bits(b).is_finite() && f32::from_bits(b) > 0.0),
        "derivation exact f32 calibration bits",
    )?;
    let admission = derivation.admission;
    require(
        admission.wrapper_payload_bytes > 0
            && admission.normalization_api_payload_bytes > 0
            && admission.flat_fit_api_payload_upper_bound_bytes > 0
            && admission.sq8_api_payload_bytes > 0
            && admission.peak_payload_upper_bound_bytes <= 8 * 1024 * 1024 * 1024
            && admission.peak_payload_upper_bound_bytes >= admission.wrapper_payload_bytes
            && admission.aggregate_scratch_upper_bound_bytes >= derivation.original_corpus.bytes,
        "producer declared payload/scratch admission; external gate still required",
    )?;
    Ok(ReceiptBinding {
        reserved_queries_sha256: reserved_sha.to_owned(),
        low_f32_bits: derivation.low_f32_bits,
        step_f32_bits: derivation.step_f32_bits,
    })
}

fn selected_len(c: &Config) -> usize {
    match &c.execution {
        Execution::Full {} => c.count,
        Execution::DiagnosticPanel { ordinals, .. } => ordinals.len(),
    }
}
// The ORIGINAL ordinal of the `slot`-th executed query; request and truth rows are indexed by it.
fn ordinal_at(c: &Config, slot: usize) -> usize {
    match &c.execution {
        Execution::Full {} => slot,
        Execution::DiagnosticPanel { ordinals, .. } => ordinals[slot],
    }
}

fn default_fetch_parallelism() -> usize {
    16
}
fn default_source_get_budget() -> usize {
    BASELINE_SOURCE_GETS
}

const RUNTIME_STACK_BYTES: usize = 2 * 1024 * 1024;
const RUNTIME_THREAD_STATE_BYTES: usize = 64 * 1024;
const TASK_STATE_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum QueryRuntime {
    #[default]
    CurrentThread,
    MultiThread2,
}
impl QueryRuntime {
    fn workers(self) -> usize {
        match self {
            Self::CurrentThread => 0,
            Self::MultiThread2 => 2,
        }
    }
}
fn runtime_reserve(c: &Config) -> Result<usize> {
    if !concurrent_config(c) {
        return Ok(0);
    }
    c.fetch_parallelism
        .checked_add(2) // Common reservation keeps staging capacity identical in both arms.
        .and_then(|n| n.checked_mul(RUNTIME_STACK_BYTES + RUNTIME_THREAD_STATE_BYTES))
        .and_then(|n| n.checked_add(QUERY_FUTURE_BYTES))
        .ok_or_else(|| "runtime memory overflow".into())
}
fn build_query_runtime(
    fetch_parallelism: usize,
    mode: QueryRuntime,
) -> Result<tokio::runtime::Runtime> {
    require(
        matches!(fetch_parallelism, 16 | 32 | 64 | 128),
        "fetch_parallelism must be 16, 32, 64 or 128",
    )?;
    let mut builder = match mode {
        QueryRuntime::CurrentThread => tokio::runtime::Builder::new_current_thread(),
        QueryRuntime::MultiThread2 => {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.worker_threads(2);
            builder
        }
    };
    Ok(builder
        .enable_all()
        .max_blocking_threads(fetch_parallelism)
        .thread_stack_size(RUNTIME_STACK_BYTES)
        .build()?)
}
#[cfg(test)]
fn query_runtime(fetch_parallelism: usize) -> Result<tokio::runtime::Runtime> {
    build_query_runtime(fetch_parallelism, QueryRuntime::CurrentThread)
}
fn configured_query_runtime(c: &Config) -> Result<tokio::runtime::Runtime> {
    if concurrent_config(c) {
        require(
            pinned_bytes(c)? <= c.max_memory_bytes,
            "caller/runtime memory before runtime allocation",
        )?;
    }
    build_query_runtime(c.fetch_parallelism, c.query_runtime)
}

fn observed_query_runtime(c: &Config) -> Result<(QueryRuntime, usize)> {
    let handle = tokio::runtime::Handle::current();
    let observed = match handle.runtime_flavor() {
        tokio::runtime::RuntimeFlavor::CurrentThread => QueryRuntime::CurrentThread,
        tokio::runtime::RuntimeFlavor::MultiThread => QueryRuntime::MultiThread2,
        _ => return Err("unsupported runtime flavor".into()),
    };
    let workers = handle.metrics().num_workers();
    require(
        observed == c.query_runtime
            && workers
                == match observed {
                    QueryRuntime::CurrentThread => 1,
                    QueryRuntime::MultiThread2 => 2,
                },
        "actual runtime flavor/num_workers differs from configuration",
    )?;
    Ok((observed, workers))
}

#[cfg(test)]
// Test-only issue ordinal and completion flags let the transport fixture reverse
// complete query outcomes deterministically, independently of physical GET order.
tokio::task_local! {
    static QUERY_TEST_ORDINAL: (usize, Arc<[std::sync::atomic::AtomicBool; 2]>);
}

// Drop requests cancellation. Completion is established by drain() or runtime teardown.
struct QueryTasks<T> {
    handles: Vec<tokio::task::JoinHandle<T>>,
    limit: usize,
    #[cfg(test)]
    completed: Arc<[std::sync::atomic::AtomicBool; 2]>,
}
impl<T: Send + 'static> QueryTasks<T> {
    fn new(limit: usize) -> Result<Self> {
        require(matches!(limit, 1 | 2), "query task limit")?;
        Ok(Self {
            handles: Vec::with_capacity(limit),
            limit,
            #[cfg(test)]
            completed: Default::default(),
        })
    }
    fn spawn(
        &mut self,
        future: impl std::future::Future<Output = T> + Send + 'static,
    ) -> Result<()> {
        require(
            self.handles.len() < self.limit,
            "query task slots exhausted",
        )?;
        let bytes = std::mem::size_of_val(&future)
            .max(size_of::<T>())
            .checked_add(size_of::<tokio::task::JoinHandle<T>>())
            .and_then(|n| n.checked_add(TASK_STATE_BYTES))
            .ok_or("query task memory overflow")?;
        require(
            bytes <= QUERY_FUTURE_BYTES,
            "query task exceeds precharged reservation",
        )?;
        #[cfg(test)]
        let future = {
            let ordinal = self.handles.len();
            let completed = self.completed.clone();
            QUERY_TEST_ORDINAL.scope((ordinal, completed.clone()), async move {
                let result = future.await;
                completed[ordinal].store(true, std::sync::atomic::Ordering::SeqCst);
                result
            })
        };
        self.handles.push(tokio::spawn(future));
        Ok(())
    }
    async fn drain(mut self) -> Vec<std::result::Result<T, tokio::task::JoinError>> {
        let mut output = Vec::with_capacity(self.handles.len());
        for handle in &mut self.handles {
            output.push(handle.await);
        }
        output
    }
    #[cfg(test)]
    fn abort(&self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}
impl<T> Drop for QueryTasks<T> {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}
fn owned_query(
    generation: Arc<TwoBitGeneration>,
    reader: Arc<Reader>,
    query: [f32; D],
) -> impl std::future::Future<
    Output = (
        std::result::Result<
            (
                borsuk::two_bit_generation::TwoBitSearchResult,
                TwoBitPlanTrace,
            ),
            TwoBitGenerationError,
        >,
        u128,
    ),
> + Send
+ 'static {
    // This executes synchronously at dispatch, before spawn and the first poll.
    let wall = Instant::now();
    async move {
        let result = generation
            .diagnostic_search_with_store(reader.store(), &query, K)
            .await;
        (result, wall.elapsed().as_nanos())
    }
}

// Only tests can construct a smaller shape or exercise reporting underfill.
#[derive(Clone, Copy)]
struct Shape {
    rows: usize,
    count: usize,
    #[cfg(test)]
    returned_limit: usize,
}
impl Shape {
    const PRODUCTION: Self = Self {
        rows: 100_000,
        count: 1000,
        #[cfg(test)]
        returned_limit: K,
    };
    #[cfg(test)]
    fn tiny(rows: usize) -> Self {
        assert!((K..=257).contains(&rows));
        Self {
            rows,
            count: 2,
            returned_limit: K,
        }
    }
}
fn require(ok: bool, message: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(message.into()) }
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn valid_sha(sha: &str) -> bool {
    sha.len() == 64
        && sha
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn flags() -> i32 {
    (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileIdentity {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}
fn file_identity(file: &File) -> Result<FileIdentity> {
    let m = file.metadata()?;
    require(m.is_file(), "nonregular file")?;
    Ok(FileIdentity {
        dev: m.dev(),
        ino: m.ino(),
        len: m.len(),
        mtime: (m.mtime(), m.mtime_nsec()),
        ctime: (m.ctime(), m.ctime_nsec()),
    })
}
fn regular(path: &Path, length: Option<usize>, cap: usize) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(flags())
        .open(path)?;
    let identity = file_identity(&file)?;
    require(
        identity.len > 0 && identity.len <= cap as u64,
        "file byte cap",
    )?;
    require(
        length.is_none_or(|n| identity.len == n as u64),
        "file exact length",
    )?;
    Ok(file)
}
fn checked(a: &Artifact, cap: usize) -> Result<(Vec<u8>, FileIdentity)> {
    require(
        a.bytes > 0 && a.bytes <= cap && valid_sha(&a.sha256),
        "artifact descriptor/cap",
    )?;
    checked_file(
        regular(&a.path, Some(a.bytes), cap)?,
        Some(a.bytes),
        cap,
        &a.sha256,
    )
}
fn checked_file(
    mut file: File,
    length: Option<usize>,
    cap: usize,
    sha: &str,
) -> Result<(Vec<u8>, FileIdentity)> {
    let before = file_identity(&file)?;
    require(
        before.len > 0 && before.len <= cap as u64 && length.is_none_or(|n| before.len == n as u64),
        "artifact descriptor length/cap",
    )?;
    let mut body = vec![0; usize::try_from(before.len)?];
    file.read_exact(&mut body)?;
    require(file.read(&mut [0])? == 0, "artifact EOF")?;
    require(
        file_identity(&file)? == before && hash(&body) == sha,
        "artifact identity/SHA",
    )?;
    Ok((body, before))
}
fn reauthenticate(a: &Artifact, bound: &FileIdentity) -> Result<()> {
    let mut file = regular(&a.path, Some(a.bytes), a.bytes)?;
    require(
        file_identity(&file)? == *bound,
        "request file identity changed",
    )?;
    let mut digest = Sha256::new();
    let mut block = [0; BLOCK];
    let mut remaining = a.bytes;
    while remaining > 0 {
        let n = remaining.min(BLOCK);
        file.read_exact(&mut block[..n])?;
        digest.update(&block[..n]);
        remaining -= n;
    }
    require(
        file.read(&mut [0])? == 0
            && file_identity(&file)? == *bound
            && format!("{:x}", digest.finalize()) == a.sha256,
        "request reauthentication EOF/SHA",
    )
}
fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 512
        && key.split('/').all(|p| {
            !p.is_empty()
                && p != "."
                && p != ".."
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        })
}
fn bounded_path(path: &Path) -> Result<()> {
    require(
        path.is_absolute() && path.as_os_str().len() <= 4096,
        "absolute bounded path",
    )
}
fn concurrent_config(c: &Config) -> bool {
    c.max_active_queries.is_some()
}
fn active_queries(c: &Config) -> usize {
    c.max_active_queries.unwrap_or(1)
}
fn config_schema(c: &Config) -> bool {
    match c.max_active_queries {
        None => c.schema == CONFIG_SCHEMA,
        Some(1 | 2) => c.schema == CONCURRENT_CONFIG_SCHEMA,
        _ => false,
    }
}
fn validate_config(c: &Config, shape: Shape) -> Result<()> {
    if c.source_cache_bytes > 0 {
        require(
            matches!(c.source_layout, SourceLayout::FlatRecords)
                && c.serving.fp32().is_some_and(|limits| limits.direct.is_none())
                && !concurrent_config(c) && !c.execution.trace(),
            "SOURCE cache requires serial untraced flat SOURCE FP32 serving",
        )?;
    }
    if let Some(limits) = c.serving.fp32() {
        if let Some(direct) = limits.direct {
            require(direct.max_sq8_bytes > 0 && direct.max_sq8_bytes as u64 <= c.max_memory_bytes
                && (1..=128).contains(&direct.max_sq8_gets), "FP32 direct closure explicit caps")?;
        }
        require(
            !concurrent_config(c)
                && (K..=256).contains(&limits.shortlist)
                && limits.max_gets > 0
                && (limits.coalesce || limits.max_gets >= limits.shortlist)
                && limits.max_parallel_gets > 0
                && limits
                    .shortlist
                    .checked_mul(D * 4 + 8)
                    .is_some_and(|n| n <= limits.max_bytes),
            "FP32 runner requires serial execution and bounded explicit limits",
        )?;
    }
    require(
        concurrent_config(c) || c.query_runtime == QueryRuntime::CurrentThread,
        "multi_thread2 requires the concurrent task probe",
    )?;
    if c.max_active_queries.is_some() {
        require(
            config_schema(c)
                && !c.execution.panel()
                && c.serving == Serving::Baseline {}
                && c.fetch_parallelism == 64
                && c.source_authentication == borsuk::sq8_s3_range::SourceAuthentication::Offload2
                && c.shared_byte_window.is_none()
                && c.sq8_range_split.is_none(),
            "concurrent probe requires explicit Q1/Q2, full baseline, fetch64, Offload2",
        )?;
        if !cfg!(test) || c.rows == 1_000_000 {
            require(
                c.rows == 1_000_000
                    && c.profile == SemanticProfile::Scale1m
                    && c.semantic_leaf_limit == Some(40)
                    && c.max_memory_bytes == 2 * 1024 * 1024 * 1024,
                "concurrent production probe requires fixed40 and 2 GiB",
            )?;
        }
    }
    if let Some(limit) = c.semantic_leaf_limit {
        require(
            borsuk::semantic_unit_router::SemanticCoverage::new(c.profile, Some(limit)).is_ok(),
            "semantic leaf limit requires Scale1m and integer 1..=64",
        )?;
    }
    validate_population(c)?;
    require(
        c.max_memory_bytes > 0
            && usize::try_from(c.max_memory_bytes).is_ok()
            && c.source_byte_budget > 0,
        "explicit positive memory/SOURCE byte budgets with checked width",
    )?;
    if let SourceLayout::FramedRecordsV1 {
        blob_bytes,
        blob_sha256,
        directory_bytes,
        directory_sha256,
    } = &c.source_layout
    {
        let tail = c.dimensions % 256;
        let padded = (c.dimensions / 256)
            .checked_mul(256)
            .and_then(|n| {
                n.checked_add(if tail == 0 {
                    0
                } else {
                    tail.next_power_of_two()
                })
            })
            .ok_or("frame dimensions overflow")?;
        let width = padded
            .div_ceil(4)
            .checked_add(8)
            .ok_or("frame width overflow")?;
        require(
            c.rows
                .min(256)
                .checked_mul(width)
                .is_some_and(|n| n <= 1 << 20)
                && c.rows
                    .div_ceil(256)
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(8))
                    == Some(*directory_bytes)
                && *blob_bytes > 0
                && c.rows
                    .checked_mul(width)
                    .and_then(|n| n.checked_add(c.rows.div_ceil(256).checked_mul(25)?))
                    .is_some_and(|n| *blob_bytes <= n as u64)
                && valid_sha(blob_sha256)
                && valid_sha(directory_sha256),
            "SOURCE frame descriptor",
        )?;
    }
    if let SourceLayout::LeafCapsulesV2 {
        blob_bytes,
        blob_sha256,
        directory_bytes,
        directory_sha256,
    } = &c.source_layout
    {
        require(
            *blob_bytes > 0
                && *blob_bytes <= usize::MAX as u64
                && *directory_bytes >= 100
                && (*directory_bytes - 32) % 4 == 0
                && c.rows
                    .div_ceil(32)
                    .checked_mul(68)
                    .and_then(|n| n.checked_add(32))
                    .is_some_and(|bound| *directory_bytes <= bound)
                && valid_sha(blob_sha256)
                && valid_sha(directory_sha256),
            "SOURCE capsule descriptor",
        )?;
    }
    let shape = population(c, shape);
    require(
        (1..=256).contains(&c.sq8_get_budget)
            && (1..=usize::try_from(c.max_memory_bytes)?).contains(&c.sq8_byte_budget),
        "explicit SQ8 GET/byte budgets",
    )?;
    require(
        matches!(c.source_get_budget, 96 | 128),
        "source_get_budget must be integer 96 or 128",
    )?;
    require(
        matches!(c.fetch_parallelism, 16 | 32 | 64 | 128),
        "fetch_parallelism must be 16, 32, 64 or 128",
    )?;
    if let Some(split) = c.sq8_range_split {
        require(
            (1..=usize::try_from(c.max_memory_bytes)?).contains(&split.max_physical_range_bytes)
                && (1..=128).contains(&split.max_physical_gets)
                && split
                    .scratch_bytes(c.sq8_byte_budget, c.fetch_parallelism)
                    .is_some(),
            "SQ8 split explicit physical byte/GET caps and checked scratch",
        )?;
    }
    if let Some(window) = c.shared_byte_window {
        require(
            (1..=usize::try_from(c.max_memory_bytes)?).contains(&window.max_bytes)
                && c.fetch_parallelism
                    .checked_mul(active_queries(c))
                    .is_some_and(|requests| window.max_waiters >= requests)
                && (1..=usize::try_from(c.max_memory_bytes)?).contains(&window.max_state_bytes)
                && InFlightBytes::modeled_state_bytes(window.max_waiters)
                    .is_some_and(|bytes| bytes <= window.max_state_bytes),
            "shared byte-window explicit capacity/waiters/state budget",
        )?;
    }
    require(
        config_schema(c),
        "config format marker must match serial v15 or explicit concurrent v16; older formats refuse",
    )?;
    require(
        c.s3_read_retry.is_none() || matches!(&c.backend, Backend::S3 { .. }),
        "S3 read retries require explicit v13 policy and S3 backend",
    )?;
    if let Execution::DiagnosticPanel { ordinals, .. } = &c.execution {
        require(
            (1..=PANEL_MAX_ORDINALS).contains(&ordinals.len())
                && ordinals.windows(2).all(|pair| pair[0] < pair[1])
                && ordinals.last().is_some_and(|last| *last < c.count),
            "diagnostic panel needs 1..=128 sorted unique original ordinals below count",
        )?;
    }
    if let Some(direct) = c.serving.direct() {
        // Sanity bounds only; the library admits the real envelope (modeled memory and
        // the planned cover) before any query allocation or I/O.
        require(
            (1..=usize::try_from(c.max_memory_bytes)?).contains(&direct.max_sq8_bytes)
                && (1..=128).contains(&direct.max_sq8_gets),
            "direct closure caps must be explicit, positive and bounded",
        )?;
    }
    require(
        config_schema(c)
            && c.dataset == DATASET
            && c.revision == REVISION
            && c.metric == "cosine"
            && c.tie_rule == "corpus_ordinal_ascending"
            && c.corpus_source_first == 0
            && c.rows == shape.rows
            && c.dimensions == D
            && c.count == shape.count
            && c.k == K,
        "fixed Cohere corpus/query/D1024/k10/profile/cap",
    )?;
    for path in [
        &c.scratch_parent,
        &c.requests.path,
        &c.truth.path,
        &c.cohort_receipt.path,
        &c.derivation_receipt.path,
    ] {
        bounded_path(path)?;
    }
    match &c.backend {
        Backend::Local { store_root } => bounded_path(store_root)?,
        Backend::S3 {
            bucket,
            region,
            physical_prefix,
            sq8_object_key,
            sq8_etag,
        } => {
            require(
                (3..=63).contains(&bucket.len())
                    && bucket.parse::<std::net::Ipv4Addr>().is_err()
                    && bucket.split('.').all(|label| {
                        !label.is_empty()
                            && !label.starts_with('-')
                            && !label.ends_with('-')
                            && label
                                .bytes()
                                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                    }),
                "S3 bucket descriptor",
            )?;
            require(
                (3..=63).contains(&region.len())
                    && !region.starts_with('-')
                    && !region.ends_with('-')
                    && region
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
                "S3 region descriptor",
            )?;
            require(
                valid_key(physical_prefix) && valid_key(sq8_object_key),
                "S3 namespace/logical key",
            )?;
            let mut parts = sq8_object_key.rsplit('/');
            require(
                parts.next() == Some(c.native_source.sq8_sha256.as_str())
                    && parts.next() == Some("objects"),
                "S3 SQ8 key/SHA binding",
            )?;
            require(
                (3..=256).contains(&sq8_etag.len())
                    && sq8_etag.starts_with('"')
                    && sq8_etag.ends_with('"')
                    && sq8_etag.as_bytes()[1..sq8_etag.len() - 1]
                        .iter()
                        .all(|&b| (0x21..=0x7e).contains(&b) && b != b'"'),
                "S3 actual quoted ETag required",
            )?;
        }
    }
    require(
        c.requests.path != c.truth.path
            && c.requests.bytes == shape.count * D * 4
            && c.truth.bytes == shape.count * K * 8,
        "request/truth exact geometry/distinct paths",
    )?;
    for receipt in [&c.cohort_receipt, &c.derivation_receipt] {
        require(
            receipt.bytes > 0 && receipt.bytes <= CONFIG_CAP && valid_sha(&receipt.sha256),
            "explicit receipt descriptor",
        )?;
    }
    require(valid_key(&c.generation_prefix), "generation prefix")?;
    for sha in [
        &c.generation_root_sha256,
        &c.requests.sha256,
        &c.truth.sha256,
        &c.native_source.source_sha256,
        &c.native_source.sq8_sha256,
        &c.native_source.source_order_sha256,
    ] {
        require(valid_sha(sha), "lowercase SHA256 required")?;
    }
    require(
        c.producer_authority.source_commit.len() == 40
            && c.producer_authority
                .source_commit
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "producer source commit",
    )?;
    for pin in [
        &c.producer_authority.executable_sha256,
        &c.producer_authority.producer_source_sha256,
        &c.producer_authority.sq8_source_sha256,
        &c.producer_authority.source_order_source_sha256,
    ] {
        require(valid_sha(pin), "root-frozen producer SHA256")?;
    }
    require(
        pinned_bytes(c)? <= c.max_memory_bytes,
        "caller buffers exceed explicit memory cap",
    )?;
    Ok(())
}
// Serde's internally tagged unit variants ignore extra keys; preserve exact descriptors.
fn exact_source_layout(body: &[u8], expected: &SourceLayout) -> Result<()> {
    #[derive(Deserialize)]
    struct LayoutField {
        source_layout: Value,
    }
    let raw: LayoutField = serde_json::from_slice(body)?;
    require(
        raw.source_layout == serde_json::to_value(expected)?,
        "authenticated root/config SOURCE layout agreement",
    )
}
fn config(path: &Path, sha: &str, shape: Shape) -> Result<Config> {
    // Metadata comes from a nonblocking, no-follow regular descriptor, even for CONFIG.
    let file = regular(path, None, CONFIG_CAP)?;
    let (body, _) = checked(
        &Artifact {
            path: path.into(),
            bytes: usize::try_from(file.metadata()?.len())?,
            sha256: sha.into(),
        },
        CONFIG_CAP,
    )?;
    let c: Config = serde_json::from_slice(&body)?;
    exact_source_layout(&body, &c.source_layout)?;
    validate_config(&c, shape)?;
    Ok(c)
}
fn request_row(body: &[u8], ordinal: usize) -> Result<[f32; D]> {
    let row = body
        .get(ordinal * D * 4..(ordinal + 1) * D * 4)
        .ok_or("request row length")?;
    let mut values = [0.; D];
    let mut norm2 = 0_f64;
    for (value, bytes) in values.iter_mut().zip(row.chunks_exact(4)) {
        *value = f32::from_le_bytes(bytes.try_into()?);
        require(value.is_finite(), "nonfinite request")?;
        norm2 += f64::from(*value).powi(2);
    }
    require(
        norm2.is_finite() && norm2 > 0.,
        "zero/nonfinite request norm",
    )?;
    Ok(values)
}
#[cfg(test)]
thread_local! {
    // Runs between the guarded metadata open and its read, without a race or sleep.
    static LOCAL_METADATA_OPEN_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce(&Path)>>> =
        std::cell::RefCell::new(None);
}
async fn metadata(
    c: &Config,
    store: &dyn ObjectStore,
    name: &str,
    sha: &str,
    charge: &mut Charge,
) -> Result<Vec<u8>> {
    require(valid_sha(sha), "metadata SHA descriptor")?;
    let decode = |body: Vec<u8>| -> Result<Vec<u8>> {
        if name == "manifest.json" {
            exact_source_layout(&body, &c.source_layout)?;
        }
        Ok(body)
    };
    let location = ObjectPath::from(format!("{}/{name}", c.generation_prefix));
    if let Backend::Local { store_root } = &c.backend {
        let path = store_root.join(location.as_ref());
        let file = regular(&path, None, CONFIG_CAP)?;
        #[cfg(test)]
        LOCAL_METADATA_OPEN_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook(&path);
            }
        });
        // Read the descriptor we guarded. LocalFileSystem::get would independently
        // reopen the path without NOFOLLOW/NONBLOCK, allowing a FIFO swap to hang.
        return decode(checked_file(file, None, CONFIG_CAP, sha)?.0);
    }
    charge.add(Sq8ReadStats {
        submitted_gets: 1,
        ..Default::default()
    })?;
    let result: Result<Vec<u8>> = async {
        let response = store.get(&location).await?;
        let size = usize::try_from(response.meta.size)?;
        require(
            response.meta.location == location
                && size > 0
                && size <= CONFIG_CAP
                && response.range == (0..response.meta.size),
            "metadata location/length/full range",
        )?;
        let mut body = Vec::with_capacity(size);
        let mut stream = response.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let end = body
                .len()
                .checked_add(chunk.len())
                .ok_or("metadata length overflow")?;
            require(end <= size, "metadata long EOF")?;
            body.extend_from_slice(&chunk);
        }
        require(body.len() == size && hash(&body) == sha, "metadata EOF/SHA")?;
        charge.add(Sq8ReadStats {
            verified_bytes: size,
            ..Default::default()
        })?;
        Ok(body)
    }
    .await;
    if result.is_err() {
        charge.add(Sq8ReadStats {
            failed_gets: 1,
            ..Default::default()
        })?;
    }
    // Schema refusal does not turn a successfully authenticated GET into a failed GET.
    // Authenticated bodies remain caller-owned until the binding record is emitted.
    decode(result?)
}
// Decode exactly D coefficients with a fallible, fixed reservation. A malformed
// manifest cannot grow the calibration vectors beyond the caller's admission.
fn manifest_coefficients<'de, De: serde::Deserializer<'de>>(
    deserializer: De,
) -> std::result::Result<Vec<f32>, De::Error> {
    struct Coefficients;
    impl<'de> serde::de::Visitor<'de> for Coefficients {
        type Value = Vec<f32>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "exactly {D} finite calibration coefficients")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            values
                .try_reserve_exact(D)
                .map_err(serde::de::Error::custom)?;
            for _ in 0..D {
                let v: f32 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("manifest calibration length"))?;
                if !v.is_finite() {
                    return Err(serde::de::Error::custom("manifest calibration finite"));
                }
                values.push(v);
            }
            if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("manifest calibration length"));
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Coefficients)
}
async fn bind_source(
    c: &Config,
    receipt: &ReceiptBinding,
    store: &dyn ObjectStore,
    charge: &mut Charge,
) -> Result<Value> {
    #[derive(Deserialize)]
    struct Root {
        schema: String,
        source_layout: SourceLayout,
        page_manifest_sha256: String,
        plane_manifest_sha256: String,
        sq8_object_sha256: String,
        sq8_object_key: String,
        sq8_etag: String,
        #[serde(deserialize_with = "manifest_coefficients")]
        low: Vec<f32>,
        #[serde(deserialize_with = "manifest_coefficients")]
        step: Vec<f32>,
    }
    let root_body = metadata(c, store, "manifest.json", &c.generation_root_sha256, charge).await?;
    let root: Root = serde_json::from_slice(&root_body)?;
    require(
        root.schema == "borsuk-two-bit-generation-v13" && root.source_layout == c.source_layout,
        "authenticated root/config SOURCE layout agreement",
    )?;
    require(
        root.sq8_object_sha256 == c.native_source.sq8_sha256,
        "root SQ8 source binding",
    )?;
    require(
        root.low
            .iter()
            .zip(&receipt.low_f32_bits)
            .all(|(v, &bits)| v.to_bits() == bits)
            && root
                .step
                .iter()
                .zip(&receipt.step_f32_bits)
                .all(|(v, &bits)| v.to_bits() == bits),
        "generation/producer exact calibration binding",
    )?;
    if let Backend::S3 {
        sq8_object_key,
        sq8_etag,
        ..
    } = &c.backend
    {
        require(
            root.sq8_object_key == *sq8_object_key && root.sq8_etag == *sq8_etag,
            "root S3 SQ8 key/ETag binding",
        )?;
    }
    let plane_sha = root.plane_manifest_sha256.clone();
    let page_sha = root.page_manifest_sha256.clone();
    drop(root);
    let plane: SourcePlaneReceipt = serde_json::from_slice(
        &metadata(c, store, "plane/manifest.json", &plane_sha, charge).await?,
    )?;
    require(
        plane.rows == c.rows
            && plane.dimensions == D
            && !plane.query_or_truth_used
            && plane.source_sha256 == c.native_source.source_sha256
            && plane.sq8_sha256 == c.native_source.sq8_sha256
            && plane.source_order_sha256 == c.native_source.source_order_sha256,
        "native source receipt binding",
    )?;
    let page_body = metadata(c, store, "page_manifest.json", &page_sha, charge).await?;
    Ok(json!({"root_manifest":std::str::from_utf8(&root_body)?,
        "page_manifest":std::str::from_utf8(&page_body)?}))
}
fn scratch_bytes(rows: usize) -> Result<usize> {
    // RotatedTwoBitCodec::prepare_query: (packed_bytes * 256 + padded_D) * sizeof(f64).
    let codec = (D / 4)
        .checked_mul(256)
        .and_then(|n| n.checked_add(D))
        .and_then(|n| n.checked_mul(size_of::<f64>()))
        .ok_or("codec scratch overflow")?;
    require(codec == 532_480, "D1024 codec formula")?;
    codec
        .checked_add(TwoBitPlanTrace::scratch_bytes(rows))
        .ok_or_else(|| "trace scratch overflow".into())
}
fn trace_scratch_bytes(c: &Config) -> usize {
    c.semantic_leaf_limit.map_or_else(|| TwoBitPlanTrace::scratch_bytes(c.rows),
        |limit| TwoBitPlanTrace::scratch_bytes_with_leaf_limit(c.rows, limit).expect("validated coverage"))
}
fn caller_bytes(c: &Config) -> usize {
    let backend = match &c.backend {
        Backend::Local { store_root } => store_root.capacity(),
        Backend::S3 {
            bucket,
            region,
            physical_prefix,
            sq8_object_key,
            sq8_etag,
        } => [bucket, region, physical_prefix, sq8_object_key, sq8_etag]
            .iter()
            .map(|s| s.capacity())
            .sum(),
    };
    let layout_bytes = match &c.source_layout {
        SourceLayout::FlatRecords => 0,
        SourceLayout::LeafCapsulesV2 {
            blob_sha256,
            directory_sha256,
            ..
        }
        | SourceLayout::FramedRecordsV1 {
            blob_sha256,
            directory_sha256,
            ..
        } => blob_sha256.capacity() + directory_sha256.capacity(),
    };
    let descriptors = size_of::<Config>()
        + layout_bytes
        + [
            &c.schema,
            &c.dataset,
            &c.revision,
            &c.metric,
            &c.tie_rule,
            &c.generation_prefix,
            &c.generation_root_sha256,
            &c.requests.sha256,
            &c.truth.sha256,
            &c.native_source.source_sha256,
            &c.native_source.sq8_sha256,
            &c.native_source.source_order_sha256,
            &c.cohort_receipt.sha256,
            &c.derivation_receipt.sha256,
            &c.producer_authority.source_commit,
            &c.producer_authority.executable_sha256,
            &c.producer_authority.producer_source_sha256,
            &c.producer_authority.sq8_source_sha256,
            &c.producer_authority.source_order_source_sha256,
        ]
        .iter()
        .map(|s| s.capacity())
        .sum::<usize>()
        + [&c.scratch_parent, &c.requests.path, &c.truth.path]
            .iter()
            .map(|p| p.capacity())
            .sum::<usize>()
        + 2 * backend
        + match &c.execution {
            Execution::Full {} => 0,
            Execution::DiagnosticPanel { ordinals, .. } => ordinals.capacity() * size_of::<usize>(),
        }
        + c.corpus_intervals.capacity() * size_of::<SourceInterval>()
        + c.cohort_receipt.path.capacity()
        + c.derivation_receipt.path.capacity()
        + size_of::<Reader>()
        + 2 * size_of::<Option<SourceCacheRecord>>();
    // Returned trace/result remain caller-owned while streaming, then drop before the next query.
    // Vec growth is bounded by twice the entire page roster; ranking retains only k entries.
    let returned = trace_scratch_bytes(c)
        + 2 * c.rows.div_ceil(256) * (size_of::<usize>() + size_of::<std::ops::Range<usize>>())
        + K * (size_of::<ScoredNominee>() + size_of::<Hit>())
        + size_of::<[usize; K]>();
    let query = c.requests.bytes
        + active_queries(c) * (size_of::<[f32; D]>() + returned)
        + if c.max_active_queries.is_some() {
            active_queries(c) * QUERY_FUTURE_BYTES + COHORT_RECORD_BYTES
        } else {
            0
        };
    // Reduction drops generation/requests first: one capped JSONL line, one truth body,
    // 8KiB reader, k hits. Authentication uses one 64KiB stack block.
    let reduction = SOURCE_BINDING_LINE_CAP + 1 + c.truth.bytes + 8192 + 2 * K * size_of::<Hit>();
    // Capped receipt/config bodies cover sequential authentication/decoding; identity,
    // descriptors duplicated by store/path/runtime setup fit another CONFIG_CAP.
    // Native histogram has 900 possible codes; allow doubled Vec capacity and a
    // transient snapshot alongside the retained before/after pair. The fixed process
    // histogram is also charged. SDK/TLS RSS still requires an external cgroup gate.
    let transport = if matches!(&c.backend, Backend::S3 { .. }) {
        3 * (size_of::<NativeTransportStats>() + 2 * 900 * size_of::<(u16, u64)>())
            + 900 * size_of::<u64>()
    } else {
        0
    };
    // Producer bit vectors and bounded manifest f32 vectors coexist during source binding.
    descriptors
        + transport
        + size_of::<Progress>()
        + 8 * CONFIG_CAP
        // Binding serialization and its coexisting record tree are startup-only,
        // outside the unchanged per-query diagnostic reservation.
        + 2 * SOURCE_BINDING_LINE_CAP
        + BLOCK
        + 4 * D * size_of::<u32>()
        + query.max(reduction)
}
// SQ8 ranges one traced query can plan: the baseline SQ8 cap or the explicit direct cap.
fn trace_ranges(c: &Config) -> usize {
    if let Some(split) = c.sq8_range_split {
        return split.max_physical_gets;
    }
    c.serving.direct().or_else(|| c.serving.fp32().and_then(|limits| limits.direct))
        .map_or(c.sq8_get_budget, |direct| direct.max_sq8_gets)
}
fn source_trace_ranges(c: &Config) -> usize {
    if c.serving
        .direct()
        .or_else(|| c.serving.fp32().and_then(|l| l.direct))
        .is_some()
    {
        1
    } else {
        c.source_get_budget
    }
}
// The library retains the ordinary bound even for direct queries; direct adds only excess.
fn range_scheduler_bytes(c: &Config) -> Result<usize> {
    let pending = |ranges| {
        Sq8RangeTrace::checked_pending_futures_bytes(ranges, c.fetch_parallelism)
            .ok_or("range scheduler memory")
    };
    let sq8 = c
        .sq8_range_split
        .map_or(c.sq8_get_budget.max(trace_ranges(c)), |split| {
            split.max_physical_gets
        });
    Ok(pending(c.source_get_budget)?.max(pending(sq8)?))
}
fn source_authentication_bytes(c: &Config) -> Result<u64> {
    let workers = c.source_authentication
        .additional_memory_bytes(c.fetch_parallelism, c.source_get_budget, active_queries(c))
        .ok_or("SOURCE authentication memory")?;
    let scores = if c.source_authentication == borsuk::sq8_s3_range::SourceAuthentication::Offload2
        && matches!(c.source_layout, SourceLayout::FlatRecords) {
        borsuk::two_bit_generation::source_score_overlap_bytes(c.rows, c.dimensions, c.source_byte_budget)
            .and_then(|n| n.checked_mul(active_queries(c) as u64))
            .ok_or("SOURCE scoring memory")?
    } else { 0 };
    workers.checked_add(scores).ok_or_else(|| "SOURCE authentication memory".into())
}
// Bytes one active diagnostic query holds on top of the production model, charged before
// any query payload opens: bounded line scratch and its coexisting record tree for every
// panel; for trace additionally the range trace with its pending-future, outcome and
// completed-probe state, both host snapshots with the bounded /proc read buffer, the host
// delta map, and the one JSON tree a failed query builds of its diagnostic object.
fn panel_line_cap(c: &Config) -> usize {
    if c.semantic_leaf_limit.is_some_and(|limit| limit > 48) { LINE_CAP } else { PANEL_LINE_CAP }
}
// The plan range copy and FP32 proof construction coexist with the returned plan.
// Value arrays reserve their known length. Charge two trees during to_value, each
// shortlist map's full BTree node, key allocations, range arrays and an 8-KiB envelope.
fn diagnostic_transient_bytes(c: &Config) -> Result<usize> {
    let ranges = trace_ranges(c)
        .checked_mul(size_of::<[usize; 2]>())
        .and_then(|n| n.checked_add(32))
        .ok_or("diagnostic range memory overflow")?;
    let Some(limits) = c.serving.fp32() else {
        return Ok(ranges);
    };
    let node = 64 + 11 * (size_of::<String>() + size_of::<Value>()) + 12 * size_of::<usize>();
    let nominee = node + 3 * 64 + size_of::<Value>();
    let range = 3 * size_of::<Value>() + 32 + size_of::<[usize; 2]>();
    limits
        .shortlist
        .checked_mul(nominee + range)
        .and_then(|n| n.checked_add(8192))
        .and_then(|n| n.checked_mul(2))
        .and_then(|n| n.checked_add(ranges))
        .ok_or_else(|| "diagnostic proof memory overflow".into())
}
fn diagnostic_bytes(c: &Config) -> Result<usize> {
    let trace = match &c.execution {
        Execution::Full {} => return Ok(0),
        Execution::DiagnosticPanel { trace, .. } => *trace,
    };
    let components = [
        panel_line_cap(c),
        panel_line_cap(c),
        diagnostic_transient_bytes(c)?,
        if trace { trace_peak_bytes(c) } else { 0 },
        if trace {
            size_of::<HostSampler>() + HOST_READ_CAP + 1 + HOST_JSON_BYTES + FAILURE_VALUE_BYTES
        } else {
            0
        },
    ];
    let bytes = components
        .into_iter()
        .try_fold(0usize, |sum, n| sum.checked_add(n))
        .ok_or("diagnostic memory overflow")?;
    require(
        bytes <= DIAGNOSTIC_CAP,
        "diagnostic budget exceeds 2 MiB per active query",
    )?;
    Ok(bytes)
}
// The diagnostic cap includes the full trace peak. Its scheduler share is already
// in the library model; only retained spans and probes are added at traced admission.
fn trace_peak_bytes(c: &Config) -> usize {
    if c.execution.trace() {
        QueryProfile::modeled_bytes(
            source_trace_ranges(c),
            trace_ranges(c),
            c.fetch_parallelism,
            c.fetch_parallelism,
        )
    } else {
        0
    }
}
fn library_trace_bytes(c: &Config) -> usize {
    if c.execution.trace() {
        QueryProfile::additional_bytes(source_trace_ranges(c), trace_ranges(c))
    } else {
        0
    }
}
// Every diagnostic byte the runner itself holds: `diagnostic_bytes` minus the trace peak.
fn diagnostic_pinned_bytes(c: &Config) -> Result<usize> {
    Ok(diagnostic_bytes(c)? - trace_peak_bytes(c))
}
// Scheduler state belongs to the library; the runner pins only its own buffers.
fn uninstrumented_pinned_bytes(c: &Config) -> u64 {
    caller_bytes(c) as u64
}
fn pinned_bytes(c: &Config) -> Result<u64> {
    let diagnostic = diagnostic_pinned_bytes(c)? as u64;
    (caller_bytes(c) as u64)
        .checked_add(runtime_reserve(c)? as u64)
        .and_then(|n| n.checked_add(diagnostic.checked_mul(active_queries(c) as u64)?))
        .ok_or_else(|| "caller memory overflow".into())
}
fn limits(c: &Config) -> Result<TwoBitGenerationLimits> {
    Ok(TwoBitGenerationLimits {
        max_memory_bytes: c.max_memory_bytes,
        max_active_queries: active_queries(c),
        max_query_bytes: c.sq8_byte_budget,
        max_query_gets: c.sq8_get_budget,
        max_parallel_gets: c.fetch_parallelism,
        max_source_bytes: c.source_byte_budget,
        max_source_gets: c.source_get_budget,
        max_parallel_source_gets: c.fetch_parallelism,
        max_query_scratch_bytes: scratch_bytes(c.rows)?.checked_sub(TwoBitPlanTrace::scratch_bytes(c.rows))
            .and_then(|n| n.checked_add(trace_scratch_bytes(c))).ok_or("coverage scratch")?,
        already_pinned_bytes: pinned_bytes(c)?,
    })
}
fn cpu_ns() -> i128 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    i128::from(t.tv_sec) * 1_000_000_000 + i128::from(t.tv_nsec)
}
fn executable_sha() -> Result<String> {
    let mut file = File::open(std::env::current_exe()?)?;
    let mut digest = Sha256::new();
    let mut block = [0; BLOCK];
    loop {
        let n = file.read(&mut block)?;
        if n == 0 {
            break;
        }
        digest.update(&block[..n]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
fn peak_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()?
                .checked_mul(1024)
        })
}

// ===== Opt-in host counters: bounded, named, read OUTSIDE the timed query window =====
// Host-wide kernel counters, not per-S3-flow or per-connection evidence. A counter that is
// missing, unreadable or that moved backwards is reported as null, never as zero.
#[derive(Clone, Copy, PartialEq, Eq)]
enum HostSource {
    NetStat,
    Snmp,
    ProcStat,
    ThreadSchedstat,
    ThreadStatus,
    SelfStat,
}
const HOST_SOURCES: [HostSource; 6] = [
    HostSource::NetStat,
    HostSource::Snmp,
    HostSource::ProcStat,
    HostSource::ThreadSchedstat,
    HostSource::ThreadStatus,
    HostSource::SelfStat,
];
impl HostSource {
    fn file(self) -> &'static str {
        match self {
            Self::NetStat => "net/netstat",
            Self::Snmp => "net/snmp",
            Self::ProcStat => "stat",
            Self::ThreadSchedstat => "thread-self/schedstat",
            Self::ThreadStatus => "thread-self/status",
            Self::SelfStat => "self/stat",
        }
    }
    fn value(self, text: &str, key: &str) -> Option<u64> {
        match self {
            Self::NetStat => paired_counter(text, "TcpExt", key),
            Self::Snmp => paired_counter(text, "Tcp", key),
            Self::ProcStat => {
                let (label, field) = key.split_once('.')?;
                stat_counter(text, label, field)
            }
            Self::ThreadSchedstat => schedstat_counter(text, key),
            Self::ThreadStatus => status_counter(text, key),
            Self::SelfStat => self_stat_counter(text, key),
        }
    }
}
// (reported name, source file, key within it). Gauges such as CurrEstab are excluded.
macro_rules! counter {
    (netstat $key:literal) => {
        (concat!("TcpExt.", $key), HostSource::NetStat, $key)
    };
    (snmp $key:literal) => {
        (concat!("Tcp.", $key), HostSource::Snmp, $key)
    };
}
const HOST_COUNTERS: &[(&str, HostSource, &str)] = &[
    counter!(netstat "TCPTimeouts"),
    counter!(netstat "TCPLossProbes"),
    counter!(netstat "TCPFastRetrans"),
    counter!(netstat "TCPSlowStartRetrans"),
    counter!(netstat "TCPLostRetransmit"),
    counter!(netstat "TCPSpuriousRTOs"),
    counter!(netstat "TCPSynRetrans"),
    counter!(netstat "TCPRetransFail"),
    counter!(netstat "TCPOFOQueue"),
    counter!(netstat "TCPOFODrop"),
    counter!(netstat "TCPOFOMerge"),
    counter!(netstat "TCPDSACKOldSent"),
    counter!(netstat "TCPDSACKOfoSent"),
    counter!(netstat "TCPDSACKRecv"),
    counter!(netstat "TCPToZeroWindowAdv"),
    counter!(netstat "TCPWantZeroWindowAdv"),
    counter!(netstat "TCPFromZeroWindowAdv"),
    counter!(netstat "TCPZeroWindowDrop"),
    counter!(netstat "TCPRcvQDrop"),
    counter!(netstat "TCPBacklogDrop"),
    counter!(netstat "TCPRcvCollapsed"),
    counter!(netstat "RcvPruned"),
    counter!(netstat "OfoPruned"),
    counter!(netstat "PruneCalled"),
    counter!(snmp "ActiveOpens"),
    counter!(snmp "PassiveOpens"),
    counter!(snmp "AttemptFails"),
    counter!(snmp "EstabResets"),
    counter!(snmp "InSegs"),
    counter!(snmp "OutSegs"),
    counter!(snmp "RetransSegs"),
    counter!(snmp "InErrs"),
    counter!(snmp "OutRsts"),
    ("cpu0.user_ticks", HostSource::ProcStat, "cpu0.user"),
    ("cpu0.system_ticks", HostSource::ProcStat, "cpu0.system"),
    ("cpu0.irq_ticks", HostSource::ProcStat, "cpu0.irq"),
    ("cpu0.softirq_ticks", HostSource::ProcStat, "cpu0.softirq"),
    ("cpu0.steal_ticks", HostSource::ProcStat, "cpu0.steal"),
    ("cpu.irq_ticks", HostSource::ProcStat, "cpu.irq"),
    ("cpu.softirq_ticks", HostSource::ProcStat, "cpu.softirq"),
    ("cpu.steal_ticks", HostSource::ProcStat, "cpu.steal"),
    ("thread.sched_run_ns", HostSource::ThreadSchedstat, "run_ns"),
    (
        "thread.sched_wait_ns",
        HostSource::ThreadSchedstat,
        "wait_ns",
    ),
    (
        "thread.sched_timeslices",
        HostSource::ThreadSchedstat,
        "timeslices",
    ),
    (
        "thread.voluntary_ctxt_switches",
        HostSource::ThreadStatus,
        "voluntary_ctxt_switches",
    ),
    (
        "thread.nonvoluntary_ctxt_switches",
        HostSource::ThreadStatus,
        "nonvoluntary_ctxt_switches",
    ),
    ("process.minor_faults", HostSource::SelfStat, "minflt"),
    ("process.major_faults", HostSource::SelfStat, "majflt"),
];
// Linux "Prefix: names..." header line followed by its "Prefix: values..." line.
fn paired_counter(text: &str, prefix: &str, key: &str) -> Option<u64> {
    let mut lines = text.lines();
    while let Some(header) = lines.next() {
        let values = lines.next()?;
        let (name, columns) = header.split_once(':')?;
        let (value_name, numbers) = values.split_once(':')?;
        if name == prefix && value_name == prefix {
            let at = columns
                .split_whitespace()
                .position(|column| column == key)?;
            return numbers.split_whitespace().nth(at)?.parse().ok();
        }
    }
    None
}
fn stat_counter(text: &str, label: &str, field: &str) -> Option<u64> {
    const FIELDS: [&str; 8] = [
        "user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal",
    ];
    let at = FIELDS.iter().position(|name| *name == field)?;
    text.lines().find_map(|line| {
        let mut words = line.split_whitespace();
        if words.next()? != label {
            return None;
        }
        words.nth(at)?.parse().ok()
    })
}
fn schedstat_counter(text: &str, key: &str) -> Option<u64> {
    let at = ["run_ns", "wait_ns", "timeslices"]
        .iter()
        .position(|name| *name == key)?;
    text.split_whitespace().nth(at)?.parse().ok()
}
fn status_counter(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        line.strip_prefix(key)?
            .strip_prefix(':')?
            .trim()
            .parse()
            .ok()
    })
}
// /proc/PID/stat: the fields after the parenthesised command start at field 3 (state).
fn self_stat_counter(text: &str, key: &str) -> Option<u64> {
    let at = match key {
        "minflt" => 7,
        "majflt" => 9,
        _ => return None,
    };
    text.rsplit_once(')')?
        .1
        .split_whitespace()
        .nth(at)?
        .parse()
        .ok()
}
fn monotonic_ns() -> Option<u64> {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    u64::try_from(t.tv_sec)
        .ok()?
        .checked_mul(1_000_000_000)?
        .checked_add(u64::try_from(t.tv_nsec).ok()?)
}
// One bounded read into the reusable buffer; a file over the cap is unavailable, not truncated.
fn read_bounded<'a>(path: &Path, buffer: &'a mut Vec<u8>) -> Option<&'a str> {
    buffer.clear();
    let file = File::open(path).ok()?;
    Read::take(file, HOST_READ_CAP as u64 + 1)
        .read_to_end(buffer)
        .ok()?;
    if buffer.len() > HOST_READ_CAP {
        return None;
    }
    std::str::from_utf8(buffer).ok()
}
#[derive(Clone, Copy)]
struct HostSnapshot {
    monotonic_ns: Option<u64>,
    values: [Option<u64>; HOST_COUNTERS.len()],
}
impl HostSnapshot {
    const EMPTY: Self = Self {
        monotonic_ns: None,
        values: [None; HOST_COUNTERS.len()],
    };
}
fn snapshot(root: &Path, buffer: &mut Vec<u8>) -> HostSnapshot {
    let mut snapshot = HostSnapshot::EMPTY;
    snapshot.monotonic_ns = monotonic_ns();
    for source in HOST_SOURCES {
        let Some(text) = read_bounded(&root.join(source.file()), buffer) else {
            continue;
        };
        for (slot, counter) in HOST_COUNTERS.iter().enumerate() {
            if counter.1 == source {
                snapshot.values[slot] = source.value(text, counter.2);
            }
        }
    }
    snapshot
}
#[cfg(test)]
thread_local! {
    // Points the sampler at a synthetic /proc tree, so tests are deterministic and portable.
    static PROC_ROOT: std::cell::RefCell<Option<PathBuf>> = std::cell::RefCell::new(None);
}
fn proc_root() -> PathBuf {
    #[cfg(test)]
    if let Some(root) = PROC_ROOT.with(|root| root.borrow().clone()) {
        return root;
    }
    PathBuf::from("/proc")
}
struct HostSampler {
    root: PathBuf,
    buffer: Vec<u8>,
    before: HostSnapshot,
    after: HostSnapshot,
}
impl HostSampler {
    fn new() -> Self {
        Self {
            root: proc_root(),
            buffer: Vec::with_capacity(HOST_READ_CAP + 1),
            before: HostSnapshot::EMPTY,
            after: HostSnapshot::EMPTY,
        }
    }
    fn take_before(&mut self) {
        self.before = snapshot(&self.root, &mut self.buffer);
    }
    fn take_after(&mut self) {
        self.after = snapshot(&self.root, &mut self.buffer);
    }
}
#[derive(Serialize)]
struct HostWindow {
    scope: &'static str,
    window: &'static str,
    before_monotonic_ns: Option<u64>,
    after_monotonic_ns: Option<u64>,
    unavailable: usize,
    deltas: BTreeMap<&'static str, Option<u64>>,
}
fn host_window(before: &HostSnapshot, after: &HostSnapshot) -> HostWindow {
    let mut deltas = BTreeMap::new();
    for (slot, counter) in HOST_COUNTERS.iter().enumerate() {
        let delta = match (before.values[slot], after.values[slot]) {
            (Some(b), Some(a)) if a >= b => Some(a - b),
            _ => None,
        };
        deltas.insert(counter.0, delta);
    }
    HostWindow {
        scope: "host_wide_counter_deltas_not_per_flow_or_per_s3_connection",
        window: "between_untimed_snapshots_that_bracket_the_query_and_include_runner_bookkeeping",
        before_monotonic_ns: before.monotonic_ns,
        after_monotonic_ns: after.monotonic_ns,
        unavailable: deltas.values().filter(|delta| delta.is_none()).count(),
        deltas,
    }
}

/// Opt-in per-query diagnostics, allocated once after admission and before any payload opens.
struct Diag {
    trace: QueryProfile,
    sampler: HostSampler,
}
#[derive(Clone, Copy)]
struct Anchors {
    monotonic_start_ns: Option<u64>,
    monotonic_end_ns: Option<u64>,
    process_cpu_start_ns: i128,
    process_cpu_end_ns: i128,
}
/// Physical GET diagnostics; successful records retain logical bounds in `plan.ranges`.
#[derive(Serialize)]
struct QueryDiagnostic<'a> {
    schema: &'static str,
    query_monotonic_start_ns: Option<u64>,
    query_monotonic_end_ns: Option<u64>,
    process_cpu_start_ns: i128,
    process_cpu_end_ns: i128,
    sq8_range_trace: &'a Sq8RangeTrace,
    source_range_trace: &'a Sq8RangeTrace,
    cpu_profile: &'a QueryProfile,
    host: HostWindow,
}
impl Diag {
    fn new(c: &Config) -> Result<Self> {
        Ok(Self {
            trace: QueryProfile::new(source_trace_ranges(c), trace_ranges(c))?,
            sampler: HostSampler::new(),
        })
    }
    fn query(&self, anchors: &Anchors) -> QueryDiagnostic<'_> {
        QueryDiagnostic {
            schema: DIAGNOSTIC_SCHEMA,
            query_monotonic_start_ns: anchors.monotonic_start_ns,
            query_monotonic_end_ns: anchors.monotonic_end_ns,
            process_cpu_start_ns: anchors.process_cpu_start_ns,
            process_cpu_end_ns: anchors.process_cpu_end_ns,
            sq8_range_trace: &self.trace.sq8,
            source_range_trace: &self.trace.source,
            cpu_profile: &self.trace,
            host: host_window(&self.sampler.before, &self.sampler.after),
        }
    }
}

struct Output {
    file: File,
    directory: File,
    path: PathBuf,
    digest: Sha256,
    bytes: u64,
    line_bytes: usize,
    line_cap: usize,
    cap: u64,
    // Diagnostic panels only: every line is serialized whole into this preallocated buffer and
    // admitted below `panel_cap` before any byte is published.
    panel: Option<Vec<u8>>,
    panel_cap: usize,
    #[cfg(test)]
    panel_query_cap: Option<usize>,
    #[cfg(test)]
    directory_synced: bool,
    #[cfg(test)]
    fail_query_output: bool,
}
fn output_directory(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(flags() | rustix::fs::OFlags::DIRECTORY.bits() as i32)
        .open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?)
}
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() as u64 > self.cap.saturating_sub(self.bytes)
            || bytes.len() > self.line_cap.saturating_sub(self.line_bytes)
        {
            return Err(io::Error::other("output/line byte cap"));
        }
        let n = self.file.write(bytes)?;
        self.digest.update(&bytes[..n]);
        self.bytes += n as u64;
        self.line_bytes += n;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
impl Output {
    fn create(path: &Path) -> Result<Self> {
        let directory = output_directory(path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(flags())
            .open(path)?;
        file_identity(&file)?;
        Ok(Self {
            file,
            directory,
            path: path.into(),
            digest: Sha256::new(),
            bytes: 0,
            line_bytes: 0,
            line_cap: LINE_CAP,
            cap: OUTPUT_CAP - TERMINAL_RESERVE,
            panel: None,
            panel_cap: PANEL_LINE_CAP,
            #[cfg(test)]
            panel_query_cap: None,
            #[cfg(test)]
            directory_synced: false,
            #[cfg(test)]
            fail_query_output: false,
        })
    }
    fn emit(&mut self, value: &impl Serialize) -> Result<()> {
        self.line_bytes = 0;
        if let Some(mut line) = self.panel.take() {
            let admitted = bounded_line(&mut line, value, self.panel_cap);
            let written = admitted.and_then(|()| self.write_all(&line).map_err(Into::into));
            self.panel = Some(line);
            return written;
        }
        serde_json::to_writer(&mut *self, value)?;
        self.write_all(b"\n")?;
        Ok(())
    }
    fn emit_source_binding(&mut self, value: &impl Serialize) -> Result<()> {
        // Keep the larger allocation scoped to this authority record.
        let mut line = Vec::with_capacity(SOURCE_BINDING_LINE_CAP);
        bounded_line(&mut line, value, SOURCE_BINDING_LINE_CAP)?;
        self.line_bytes = 0;
        self.line_cap = SOURCE_BINDING_LINE_CAP;
        let result = self.write_all(&line).map_err(Into::into);
        self.line_cap = LINE_CAP;
        result
    }
    // Keeps a smaller cap already installed (tests); otherwise allocates the one buffer.
    fn admit_panel_lines(&mut self, cap: usize) {
        if self.panel.is_none() {
            self.panel = Some(Vec::with_capacity(cap));
            self.panel_cap = cap;
        }
    }
    fn sha(&self) -> String {
        format!("{:x}", self.digest.clone().finalize())
    }
}
/// A diagnostic-panel line whose serialization, newline included, would exceed the cap.
#[derive(Debug)]
struct LineCap(usize);
impl std::fmt::Display for LineCap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "diagnostic panel serialized line exceeds {} bytes",
            self.0
        )
    }
}
impl Error for LineCap {}
// Writer that never grows its Vec past the cap and records that it was asked to.
struct Capped<'a> {
    line: &'a mut Vec<u8>,
    cap: usize,
    exceeded: &'a mut bool,
}
impl Write for Capped<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.cap.saturating_sub(self.line.len()) {
            *self.exceeded = true;
            return Err(io::Error::other("diagnostic panel serialized line cap"));
        }
        self.line.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
// Serializes `value` plus its newline into `line` iff the whole line is at most `cap` bytes.
// The buffer is cleared first and is never grown; nothing is published here.
fn bounded_line(line: &mut Vec<u8>, value: &impl Serialize, cap: usize) -> Result<()> {
    line.clear();
    let mut exceeded = false;
    let written = serde_json::to_writer(
        Capped {
            line: &mut *line,
            cap,
            exceeded: &mut exceeded,
        },
        value,
    );
    if exceeded || (written.is_ok() && line.len() >= cap) {
        return Err(LineCap(cap).into());
    }
    written?;
    line.push(b'\n');
    Ok(())
}
fn bounded_text(text: &str) -> String {
    text.chars().take(PANEL_ERROR_CHARS).collect()
}
// A failed diagnostic query keeps its original error: if its diagnostic object overflows the
// panel line cap, the same failure is published with the object replaced by an explicit marker.
fn emit_failure(out: &mut Output, mut record: Value, diagnostic: Option<Value>) -> Result<()> {
    let Some(diagnostic) = diagnostic else {
        return out.emit(&record);
    };
    record["diagnostic"] = diagnostic;
    match out.emit(&record) {
        Err(e) if e.downcast_ref::<LineCap>().is_some() => {
            record["diagnostic"] =
                json!({"omitted":"serialized_line_cap","cap_bytes":out.panel_cap});
            out.emit(&record)
        }
        other => other,
    }
}
struct Seal {
    prefix_bytes: u64,
    prefix_sha256: String,
    sealed_bytes: u64,
    sealed_sha256: String,
    file: FileIdentity,
    requests: FileIdentity,
}
#[derive(Default, Clone, Copy, Serialize)]
struct Charge {
    submitted_gets: u64,
    verified_bytes: u64,
    failed_gets: u64,
}
impl Charge {
    fn add(&mut self, s: Sq8ReadStats) -> Result<()> {
        self.submitted_gets = self
            .submitted_gets
            .checked_add(s.submitted_gets as u64)
            .ok_or("GET sum overflow")?;
        self.verified_bytes = self
            .verified_bytes
            .checked_add(s.verified_bytes as u64)
            .ok_or("byte sum overflow")?;
        self.failed_gets = self
            .failed_gets
            .checked_add(s.failed_gets as u64)
            .ok_or("failure sum overflow")?;
        Ok(())
    }
}
#[derive(Default, Serialize)]
struct Charges {
    #[serde(skip_serializing_if = "Option::is_none")]
    canonical: Option<Charge>,
    router: Charge,
    source: Charge,
    sq8: Charge,
}
impl Charges {
    fn add_canonical(&mut self, stats: Sq8ReadStats) -> Result<()> {
        self.canonical
            .get_or_insert_with(Charge::default)
            .add(stats)
    }
    fn add(&mut self, router: Sq8ReadStats, source: Sq8ReadStats, sq8: Sq8ReadStats) -> Result<()> {
        self.router.add(router)?;
        self.source.add(source)?;
        self.sq8.add(sq8)
    }
    fn sum(&self) -> Charge {
        Charge {
            submitted_gets: self.router.submitted_gets
                + self.source.submitted_gets
                + self.sq8.submitted_gets
                + self.canonical.map_or(0, |c| c.submitted_gets),
            verified_bytes: self.router.verified_bytes
                + self.source.verified_bytes
                + self.sq8.verified_bytes
                + self.canonical.map_or(0, |c| c.verified_bytes),
            failed_gets: self.router.failed_gets
                + self.source.failed_gets
                + self.sq8.failed_gets
                + self.canonical.map_or(0, |c| c.failed_gets),
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Hit {
    id: u64,
    score_bits: u64,
}
fn validate_hits(hits: &[Hit], rows: usize, fp32: bool) -> Result<()> {
    let score = |h: &Hit| {
        if fp32 {
            f64::from_bits(h.score_bits)
        } else {
            u32::try_from(h.score_bits)
                .map(|n| f64::from(f32::from_bits(n)))
                .unwrap_or(f64::NAN)
        }
    };
    require(hits.len() <= K, "returned count exceeds k")?;
    for (i, h) in hits.iter().enumerate() {
        require(
            h.id < rows as u64
                && score(h).is_finite()
                && !hits[..i].iter().any(|old| old.id == h.id),
            "returned IDs/scores",
        )?;
    }
    require(
        hits.windows(2).all(|h| {
            score(&h[0])
                .total_cmp(&score(&h[1]))
                .then(h[0].id.cmp(&h[1].id))
                .is_le()
        }),
        "returned ranking order",
    )
}
struct Progress {
    stage: &'static str,
    completed: usize,
    sealed: bool,
    truth_opened: bool,
    underfilled: usize,
    charges: Charges,
    binding_charge: Charge,
    query_wall_ns: u128,
    query_cpu_ns: i128,
    transport: TransportSpan,
    direct_memory: Option<DirectClosureMemory>,
    cohort: Option<Value>,
}

#[derive(Default, Serialize)]
struct TransportSpan {
    stage: &'static str,
    ordinal: Option<usize>,
    before: Option<NativeTransportStats>,
    after: Option<NativeTransportStats>,
}
impl TransportSpan {
    fn begin(&mut self, reader: &Reader, stage: &'static str, ordinal: Option<usize>) {
        // Drop the old pair before allocating its successor; never retain a panel.
        *self = Self::default();
        self.stage = stage;
        self.ordinal = ordinal;
        self.before = reader.snapshot();
    }
    fn finish(&mut self, reader: &Reader) {
        self.after = reader.snapshot();
    }
    fn terminal(&self) -> Value {
        fn compact(s: &Option<NativeTransportStats>) -> Value {
            match s {
                None => Value::Null,
                Some(s) => json!({"attempts":s.attempts,"method_counts":s.method_counts,
                    "transport_failures":s.transport_failures,"stream_failures":s.stream_failures,
                    "consumed_payload_bytes":s.consumed_payload_bytes,"dropped_error_bodies":s.dropped_error_bodies,
                    "status_counts":null,"status_counts_entries":s.status_counts.len()}),
            }
        }
        // Full histograms belong in phase records (up to 900 codes). Retain every
        // scalar/method counter on output failure without exceeding the 8KiB reserve.
        json!({"stage":self.stage,"ordinal":self.ordinal,"before":compact(&self.before),
            "after":compact(&self.after),"scope":"cumulative_process_native_transport",
            "status_counts_omitted_from_terminal":true,
            "wire_bytes":null,"unread_bytes":null,"billed_bytes":null,"billed_requests":null})
    }
}
impl Default for Progress {
    fn default() -> Self {
        Self {
            stage: "config",
            completed: 0,
            sealed: false,
            truth_opened: false,
            underfilled: 0,
            charges: Charges::default(),
            binding_charge: Charge::default(),
            query_wall_ns: 0,
            query_cpu_ns: 0,
            transport: TransportSpan::default(),
            direct_memory: None,
            cohort: None,
        }
    }
}
fn seal(c: &Config, out: &mut Output, p: &mut Progress, requests: FileIdentity) -> Result<Seal> {
    require(
        p.completed == selected_len(c),
        "cannot seal incomplete query panel",
    )?;
    p.stage = "seal";
    out.file.sync_all()?;
    let prefix_bytes = out.bytes;
    let prefix_sha256 = out.sha();
    out.emit(&json!({"phase":"all_queries_sealed","count":p.completed,
        "selected_count":selected_len(c),"population_count":c.count,"reserved_query_count":c.reserved_query_interval.end-c.reserved_query_interval.start,"truth_opened":false,
        "prefix_bytes":prefix_bytes,"prefix_sha256":prefix_sha256,
        "requests_sha256":c.requests.sha256,"generation_root_sha256":c.generation_root_sha256,
        "requires_successful_sync":true,"requires_successful_directory_sync":true}))?;
    out.file.sync_all()?;
    p.stage = "seal_directory_sync";
    require(
        !p.sealed && !p.truth_opened,
        "directory sync must precede seal/truth",
    )?;
    let bound_directory = out.directory.metadata()?;
    let current_directory = output_directory(&out.path)?.metadata()?;
    require(
        current_directory.dev() == bound_directory.dev()
            && current_directory.ino() == bound_directory.ino(),
        "output directory identity changed",
    )?;
    out.directory.sync_all()?;
    #[cfg(test)]
    {
        out.directory_synced = true;
    }
    let seal = Seal {
        prefix_bytes,
        prefix_sha256,
        sealed_bytes: out.bytes,
        sealed_sha256: out.sha(),
        file: file_identity(&out.file)?,
        requests,
    };
    p.sealed = true;
    Ok(seal)
}
#[derive(Serialize)]
struct SourceCacheRecord {
    charged_bytes: usize,
    capacity_bytes: usize,
    occupied_bytes: usize,
    hits: u64,
    misses: u64,
    evictions: u64,
    insertions: u64,
}
impl SourceCacheRecord {
    fn snapshot(c: &Config, generation: &TwoBitGeneration) -> Option<Self> {
        if c.source_cache_bytes == 0 { return None; }
        let stats = generation.source_cache_stats();
        // Every replacement evicts one block; the remaining blocks include at most one tail.
        let insertions = stats.evictions.saturating_add(
            stats.occupied_bytes.div_ceil(256 * (D / 4 + 8)) as u64);
        Some(Self {
            charged_bytes: stats.charged_bytes, capacity_bytes: stats.capacity_bytes,
            occupied_bytes: stats.occupied_bytes, hits: stats.hits, misses: stats.misses,
            evictions: stats.evictions, insertions,
        })
    }
}
async fn query_and_seal(
    c: &Config,
    shape: Shape,
    out: &mut Output,
    p: &mut Progress,
) -> Result<Seal> {
    query_and_seal_with(c, shape, out, p, || {
        Reader::new(&c.backend, c.s3_read_retry)
    })
    .await
}
async fn query_and_seal_with(
    c: &Config,
    shape: Shape,
    out: &mut Output,
    p: &mut Progress,
    reader: impl FnOnce() -> Result<Reader>,
) -> Result<Seal> {
    validate_config(c, shape)?;
    let admission = limits(c)?;
    let observed_runtime = concurrent_config(c)
        .then(|| observed_query_runtime(c))
        .transpose()?;
    let receipt = validate_receipts(c)?;
    if c.execution.panel() {
        out.admit_panel_lines(panel_line_cap(c));
    }
    p.stage = "requests";
    let (requests, request_identity) = checked(&c.requests, c.count * D * 4)?;
    // Authenticate and validate ALL original rows, selected or not, before any heavy open.
    for ordinal in 0..c.count {
        request_row(&requests, ordinal).map_err(|e| format!("request {ordinal}: {e}"))?;
    }
    let mut diag = if c.execution.trace() {
        Some(Diag::new(c)?)
    } else {
        None
    };
    if c.execution.panel() {
        let bytes = diagnostic_bytes(c)?;
        let mut admission_record = json!({"phase":"diagnostic_admission","execution":c.execution,
            "selected_count":selected_len(c),"population_count":c.count,
            "trace":c.execution.trace(),"diagnostic_bytes_per_active_query":bytes,
            "diagnostic_cap_bytes":DIAGNOSTIC_CAP,"diagnostic_schema":DIAGNOSTIC_SCHEMA,
            "diagnostic_transient_bytes":diagnostic_transient_bytes(c)?,"max_active_queries":admission.max_active_queries,
            "charged_in_caller_pinned_bytes":true,"caller_pinned_bytes":admission.already_pinned_bytes,
            "panel_line_cap_bytes":out.panel_cap,"host_read_cap_bytes":HOST_READ_CAP,
            "trace_ranges":trace_ranges(c),"source_trace_ranges":source_trace_ranges(c),
            "population_percentiles_valid":false,"full_cohort_qualification":false,
            "semantics":{
                "cpu_residual":"unattributed_process_cpu_not_tls_or_network_cpu",
                "poll_cpu":"unresolved_not_instrumented",
                "capsule_page_bounds":"u64_max_means_no_single_logical_page_interval",
                "headers_interval":"get_opts_client_queue_connect_tls_server_combined",
                "body_gaps":"application_observed_poll_gaps_not_packet_arrival",
                "ranges":"overlap_in_time_never_sum_intervals",
                "host_counters":"host_wide_not_per_flow_null_means_unavailable",
                "unset":"a boundary never reached is null, never a fabricated timestamp"},
            "truth_opened":false});
        // Three different figures, never interchangeable: what the trace RETAINS between queries,
        // the cumulative PEAK while one drains, and the range state both modes carry.
        let traced = c.execution.trace().then(|| trace_ranges(c));
        admission_record["trace_retained_bytes"] =
            json!(traced.map(|n| QueryProfile::retained_bytes(source_trace_ranges(c), n)));
        admission_record["trace_peak_bytes"] = json!(traced.map(|_| trace_peak_bytes(c)));
        admission_record["diagnostic_pinned_bytes"] = json!(diagnostic_pinned_bytes(c)?);
        admission_record["trace_additional_bytes_charged_by_library"] =
            json!(library_trace_bytes(c));
        admission_record["range_scheduler_owner"] = json!("library");
        admission_record["range_state_charged_by_library_bytes"] = json!(range_scheduler_bytes(c)?);
        admission_record["range_state_both_paths_bytes"] = json!(range_scheduler_bytes(c)?);
        admission_record["source_authentication_extra_library_bytes"] =
            json!(source_authentication_bytes(c)?);
        out.emit(&admission_record)?;
    }
    p.stage = "backend";
    let reader = Arc::new(reader()?);
    let store = reader.store();
    p.stage = "native_source";
    p.transport.begin(&reader, p.stage, None);
    let binding = bind_source(c, &receipt, store, &mut p.binding_charge).await;
    drop(receipt);
    p.transport.finish(&reader);
    out.emit_source_binding(
        &json!({"phase":"source_binding","transport":p.transport,"charges":p.binding_charge,"success":binding.is_ok(),
        "generation_authority":binding.as_ref().ok(),"truth_opened":false}),
    )?;
    binding?;
    p.stage = "generation_open";
    p.transport.begin(&reader, p.stage, None);
    let generation = TwoBitGeneration::open_remote(
        store,
        &ObjectPath::from(c.generation_prefix.clone()),
        &c.generation_root_sha256,
        admission,
        &c.scratch_parent,
    )
    .await;
    p.transport.finish(&reader);
    out.emit(&json!({"phase":"generation_open","transport":p.transport,
        "success":generation.is_ok(),"truth_opened":false}))?;
    let generation = generation?;
    let generation = match c.semantic_leaf_limit {
        Some(limit) => generation.with_semantic_leaf_limit(limit)?,
        None => generation,
    };
    let generation = generation.with_source_authentication(c.source_authentication)?;
    let generation = match c.shared_byte_window {
        Some(window) => generation.with_byte_window_limits(
            window.max_bytes,
            window.max_waiters,
            window.max_state_bytes,
        )?,
        None => generation,
    };
    let generation = match c.sq8_range_split {
        Some(split) => generation.with_sq8_range_split(split)?,
        None => generation,
    };
    require(
        generation.rows() == c.rows
            && generation.discovery_mode() == DiscoveryMode::Semantic
            && generation.semantic_profile() == Some(c.profile),
        "generation geometry/profile",
    )?;
    if let Some(limits) = c.serving.fp32() {
        let combined = generation.fp32_rerank_memory(limits)?
            .checked_add(generation.source_cache_charge(c.source_cache_bytes)? as u64)
            .ok_or("FP32 SOURCE cache memory overflow")?;
        require(combined <= c.max_memory_bytes, "FP32 total modeled memory cap")?;
    }
    let mut generation = generation.with_source_cache(c.source_cache_bytes)?;
    let fp32_startup = if let Some(limits) = c.serving.fp32() {
        let memory = generation.fp32_rerank_memory(limits)?;
        require(
            memory <= c.max_memory_bytes,
            "FP32 total modeled memory cap",
        )?;
        require(
            memory
                .checked_add(
                    (library_trace_bytes(c) as u64)
                        .checked_mul(active_queries(c) as u64)
                        .ok_or("FP32 diagnostic memory overflow")?,
                )
                .is_some_and(|n| n <= c.max_memory_bytes),
            "FP32 diagnostic memory cap",
        )?;
        p.transport.begin(&reader, "canonical_open", None);
        let opened = generation.enable_fp32_rerank(store).await;
        p.transport.finish(&reader);
        Some(
            json!({"limits":limits,"modeled_memory_bytes":memory,"open":opened?,
            "transport":p.transport,"score_precision":"f64"}),
        )
    } else {
        None
    };
    let generation = Arc::new(generation);
    let mut startup = json!({"phase":"startup","metadata":generation.remote_open_stats(),
        "serving":c.serving,
        "library_cap_bytes":c.max_memory_bytes,"caller_pinned_bytes":admission.already_pinned_bytes,
        "codec_scratch_bytes":532480,"trace_scratch_bytes":trace_scratch_bytes(c),
        "query_scratch_bytes":admission.max_query_scratch_bytes,"truth_opened":false});
    startup["source_layout"] = json!(c.source_layout);
    if let Some(cache) = SourceCacheRecord::snapshot(c, &generation) {
        startup["source_cache"] = json!(cache);
    }
    if let Some(fp32) = fp32_startup {
        startup["fp32_rerank"] = fp32;
    }
    startup["range_state_charged_by_library_bytes"] = json!(range_scheduler_bytes(c)?);
    startup["source_authentication_extra_library_bytes"] = json!(source_authentication_bytes(c)?);
    if let Some(bytes) = generation.byte_window_state_bytes() {
        startup["shared_byte_window_state_bytes"] = json!(bytes);
    }
    if concurrent_config(c) {
        startup["concurrent_resources"] = json!({
            "query_slots":active_queries(c),"downloads_per_query":c.fetch_parallelism,
            "downloads_global_bound":active_queries(c) * c.fetch_parallelism,
            "source_bytes_per_query":c.source_byte_budget,"sq8_bytes_per_query":c.sq8_byte_budget,
            "query_bytes_global_bound":active_queries(c) * (c.source_byte_budget + c.sq8_byte_budget),
            "scheduler_bytes_global":active_queries(c) * range_scheduler_bytes(c)?,
            "cohort_record_bytes":COHORT_RECORD_BYTES,
            "compute_workers_global":2,"compute_slots_global":2,
            "query_runtime":observed_runtime.unwrap().0,"query_dispatch":"spawned_ordered_batches",
            "query_wall_semantics":"dispatch_to_completion_including_initial_runtime_queue",
            "runtime_worker_threads":observed_runtime.unwrap().0.workers(),
            "runtime_num_workers":observed_runtime.unwrap().1,
            "runtime_charged_worker_threads":2,
            "runtime_max_blocking_threads":c.fetch_parallelism,
            "runtime_stack_bytes_per_thread":RUNTIME_STACK_BYTES,
            "runtime_thread_state_bytes":RUNTIME_THREAD_STATE_BYTES,
            "runtime_reserve_bytes":runtime_reserve(c)?,
            "task_state_bytes_per_query":TASK_STATE_BYTES,
            "query_future_bytes_per_query":QUERY_FUTURE_BYTES,
            "modeled_memory_bound_bytes":c.max_memory_bytes,
            "external_memory_limit_bytes":8_u64 * 1024 * 1024 * 1024});
    }
    out.emit(&startup)?;
    if let Some(direct) = c.serving.direct() {
        // Exact modeled memory must fit before any direct query: the existing model is kept
        // whole (no SOURCE credit) and a full direct query budget is added.
        p.stage = "direct_admission";
        let memory = generation.direct_closure_memory(direct)?;
        // Two totals. The ALGORITHM total is what this configuration would model with no
        // instrumentation at all (what a full run pins); only it can be a RESOURCE_REJECT.
        // The COMPLETE total is what the first query will be admitted against: the model as
        // pinned plus the library-owned trace reservation it adds at traced admission. A cap
        // exceeded only by the complete total is a diagnostic error (INVALID) found here,
        // before any payload opens, never a rejection of the candidate.
        let diagnostic_pinned = diagnostic_pinned_bytes(c)? as u64 * active_queries(c) as u64;
        let library_trace = library_trace_bytes(c) as u64 * active_queries(c) as u64;
        let algorithm_total = memory
            .total_bytes
            .saturating_sub(admission.already_pinned_bytes)
            .saturating_add(uninstrumented_pinned_bytes(c));
        let complete_total = memory.total_bytes.saturating_add(library_trace);
        let admitted = complete_total <= memory.cap_bytes;
        out.emit(
            &json!({"phase":"direct_admission","limits":c.serving,"memory":memory,
            "admitted":admitted,"algorithm_total_bytes":algorithm_total,
            "complete_total_bytes":complete_total,"library_trace_reservation_bytes":library_trace,
            "diagnostic_pinned_bytes":diagnostic_pinned,"truth_opened":false}),
        )?;
        p.direct_memory = Some(memory);
        if algorithm_total > memory.cap_bytes {
            return Err(ResourceReject {
                reason: DirectClosureRejection::ModeledMemory,
                message: format!(
                    "modeled {} bytes exceed the {} byte cap before any query",
                    algorithm_total, memory.cap_bytes
                ),
            }
            .into());
        }
        require(
            admitted,
            "diagnostic memory: the algorithm model fits the cap but the instrumented total does not",
        )?;
    }
    let concurrent = c.max_active_queries.is_some();
    let mut pending = std::collections::VecDeque::new();
    let mut first_error: Option<Box<dyn Error + Send + Sync>> = None;
    let mut attempted = 0;
    let cohort_wall = Instant::now();
    let cohort_cpu = concurrent.then(cpu_ns);
    if concurrent {
        p.transport.begin(&reader, "query_cohort", None);
    }
    for slot in 0..selected_len(c) {
        // The ORIGINAL ordinal indexes the request row here and the truth row later.
        if concurrent && pending.is_empty() {
            if first_error.is_some() {
                break;
            }
            let end = (slot + active_queries(c)).min(selected_len(c));
            let mut tasks = QueryTasks::new(active_queries(c))?;
            // Validate every row before spawning any query in this batch.
            let queries = (slot..end)
                .map(|next| request_row(&requests, ordinal_at(c, next)))
                .collect::<Result<Vec<_>>>()?;
            for query in queries {
                tasks.spawn(owned_query(
                    Arc::clone(&generation),
                    Arc::clone(&reader),
                    query,
                ))?;
            }
            attempted += end - slot;
            // Ordered joins retain every sibling even after an error or task panic.
            pending = tasks
                .drain()
                .await
                .into_iter()
                .map(|joined| {
                    joined.unwrap_or_else(|error| {
                        (
                            Err(TwoBitGenerationError::Invalid(if error.is_panic() {
                                "query task panicked"
                            } else {
                                "query task cancelled"
                            })),
                            0,
                        )
                    })
                })
                .collect();
        }
        let ordinal = ordinal_at(c, slot);
        p.stage = "query";
        let query = request_row(&requests, ordinal)?;
        // Host counters are read outside the timed window, before and after it. The trace is
        // cleared here too, so a query refused before it starts can never show a stale one.
        if let Some(diag) = diag.as_mut() {
            diag.sampler.take_before();
            diag.trace.begin(Instant::now());
        }
        if !concurrent {
            p.transport.begin(&reader, p.stage, Some(ordinal));
        }
        let monotonic_start = diag.as_ref().and_then(|_| monotonic_ns());
        let wall = Instant::now();
        let cpu = if concurrent { 0 } else { cpu_ns() };
        let prefetched = if concurrent {
            pending.pop_front()
        } else {
            None
        };
        let prefetched_wall = prefetched.as_ref().map(|(_, wall)| *wall);
        let mut fp32_result = None;
        let result = if let Some(limits) = c.serving.fp32() {
            let searched = if let Some(diag) = diag.as_mut() {
                generation
                    .diagnostic_search_fp32_with_store_traced(
                        store,
                        &query,
                        K,
                        limits,
                        &mut diag.trace,
                    )
                    .await
            } else {
                generation
                    .diagnostic_search_fp32_with_store(store, &query, K, limits)
                    .await
            };
            searched.map(|(result, trace)| {
                    fp32_result = Some((
                        result.candidates,
                        result.canonical_stats,
                        result.rerank_wall_ns,
                        result.canonical_ranges,
                    ));
                    (result.base, trace)
                })
        } else if let Some((result, _)) = prefetched {
            result
        } else {
            match (c.serving.direct(), diag.as_mut()) {
                (None, None) => {
                    generation
                        .diagnostic_search_with_store(store, &query, K)
                        .await
                }
                (None, Some(diag)) => {
                    generation
                        .diagnostic_search_with_store_traced(store, &query, K, &mut diag.trace)
                        .await
                }
                (Some(direct), None) => {
                    generation
                        .diagnostic_direct_closure_search_with_store(store, &query, K, direct)
                        .await
                }
                (Some(direct), Some(diag)) => {
                    generation
                        .diagnostic_direct_closure_search_with_store_traced(
                            store,
                            &query,
                            K,
                            direct,
                            &mut diag.trace,
                        )
                        .await
                }
            }
        };
        let wall_ns = prefetched_wall.unwrap_or_else(|| wall.elapsed().as_nanos());
        let cpu_ns = if concurrent { 0 } else { cpu_ns() - cpu };
        let monotonic_end = diag.as_ref().and_then(|_| monotonic_ns());
        if !concurrent {
            p.transport.finish(&reader);
        }
        if let Some(diag) = diag.as_mut() {
            diag.sampler.take_after();
        }
        let anchors = Anchors {
            monotonic_start_ns: monotonic_start,
            monotonic_end_ns: monotonic_end,
            process_cpu_start_ns: cpu,
            process_cpu_end_ns: cpu + cpu_ns,
        };
        p.query_wall_ns += wall_ns;
        p.query_cpu_ns += cpu_ns;
        let (result, trace) = match result {
            Ok(result) => result,
            Err(e) => {
                let (source, sq8) = e.read_stats();
                p.charges.add(e.router_stats(), source, sq8)?;
                if c.serving.fp32().is_some() {
                    p.charges.add_canonical(e.canonical_stats())?;
                }
                let rejection = e.direct_resource_rejection();
                let error_text = query_error_text(&e);
                let error_text = if c.execution.panel() {
                    bounded_text(&error_text)
                } else {
                    error_text
                };
                let mut failure = json!({"phase":"query_failure","ordinal":ordinal,"error":error_text,
                    "resource_rejection":rejection,
                    "charges_so_far":p.charges,"sum_so_far":p.charges.sum(),"stages":e.stages(),
                    "query_wall_ns":wall_ns,"query_process_cpu_ns":cpu_ns,"truth_opened":false,
                    "transport":p.transport});
                if let Some(cache) = SourceCacheRecord::snapshot(c, &generation) {
                    failure["source_cache"] = json!(cache);
                }
                if concurrent {
                    failure["query_process_cpu_ns"] = Value::Null;
                    failure["transport"] = Value::Null;
                }
                let diagnostic = diag
                    .as_ref()
                    .map(|diag| serde_json::to_value(diag.query(&anchors)))
                    .transpose()?;
                emit_failure(out, failure, diagnostic)?;
                let message = format!("native query {ordinal}: {error_text}");
                let error = match rejection {
                    Some(reason) => ResourceReject { reason, message }.into(),
                    None => message.into(),
                };
                if !concurrent {
                    return Err(error);
                }
                if first_error.is_none() {
                    first_error = Some(error);
                }
                continue;
            }
        };
        if c.serving.direct().or_else(|| c.serving.fp32().and_then(|limits| limits.direct)).is_some() {
            require(
                result.source_stats == Sq8ReadStats::default()
                    && result.router_stats == Sq8ReadStats::default()
                    && result.source_nomination_skipped,
                "direct closure SOURCE/router charges must be zero and nomination skipped",
            )?;
        }
        p.charges.add(
            result.router_stats,
            result.source_stats,
            result.ranked.stats,
        )?;
        require(
            fp32_result
                .as_ref()
                .map_or(result.ranked.candidates.len(), |r| r.0.len())
                <= K,
            "native returned count exceeds k",
        )?;
        let mut returned = Vec::with_capacity(K);
        let mut returned_physical_ordinals = [0; K];
        if let Some((hits, _, _, _)) = &fp32_result {
            for (rank, hit) in hits.iter().enumerate() {
                let ordinal = hit.ordinal.ok_or("FP32 runner base ordinal")?;
                require(
                    hit.id >= 0 && ordinal < c.rows,
                    "native exact returned identity",
                )?;
                returned.push(Hit {
                    id: hit.id as u64,
                    score_bits: hit.score.to_bits(),
                });
                returned_physical_ordinals[rank] = ordinal;
            }
        } else {
            for (rank, hit) in result.ranked.candidates.iter().enumerate() {
                require(
                    hit.id >= 0 && hit.ordinal < c.rows,
                    "native returned identity",
                )?;
                returned.push(Hit {
                    id: hit.id as u64,
                    score_bits: u64::from(hit.score.to_bits()),
                });
                returned_physical_ordinals[rank] = hit.ordinal;
            }
        }
        #[cfg(test)]
        returned.truncate(shape.returned_limit);
        let returned_physical_ordinals = &returned_physical_ordinals[..returned.len()];
        validate_hits(&returned, c.rows, c.serving.fp32().is_some())?;
        let underfill = returned.len() < K;
        let mut charges = Charges::default();
        charges.add(
            result.router_stats,
            result.source_stats,
            result.ranked.stats,
        )?;
        if let Some((_, stats, _, _)) = &fp32_result {
            charges.add_canonical(*stats)?;
            p.charges.add_canonical(*stats)?;
        }
        let canonical_stage = fp32_result.as_ref().map(|(_,_,wall,ranges)| json!({
            "score_precision":"f64","wall_ns":wall,"record_bytes":D*4+8,
            "ranges":ranges.iter().map(|r| [r.start,r.end]).collect::<Vec<_>>(),
            "covered_records":ranges.iter().map(|r|r.len()/(D*4+8)).sum::<usize>(),
            "bridge_records":ranges.iter().map(|r|r.len()/(D*4+8)).sum::<usize>()-result.ranked.candidates.len(),
            "shortlist":result.ranked.candidates.iter().map(|h| json!({"id":h.id,"ordinal":h.ordinal,"sq8_score_bits":h.score.to_bits()})).collect::<Vec<_>>()
        }));
        #[cfg(test)]
        if c.execution.panel() {
            if let Some(stage) = &canonical_stage {
                assert!(
                    2 * (size_of::<Value>() + tests::diagnostic_value_heap_bound(stage))
                        <= diagnostic_transient_bytes(c)?
                );
            }
        }
        // Full ordered range identity, not just totals: `selected_pages` is the required
        // closure (direct) or the admitted pages (baseline); bridges live only in `ranges`.
        #[derive(Serialize)]
        struct PlanRecord<'a> {
            page_rows: usize,
            selected_pages: &'a [usize],
            ranges: Vec<[usize; 2]>,
            logical_range_count: usize,
            planned_bytes: usize,
            target_pages: usize,
            target_shortfall: usize,
            primary_pages_retained: usize,
            covered_pages: usize,
            bridge_pages: usize,
        }
        let covered_pages = result
            .plan
            .ranges
            .iter()
            .map(|r| r.len().div_ceil(result.plan.page_rows * (D + 12)))
            .sum::<usize>();
        let plan = PlanRecord {
            page_rows: result.plan.page_rows,
            selected_pages: &result.plan.selected_pages,
            ranges: result
                .plan
                .ranges
                .iter()
                .map(|r| [r.start, r.end])
                .collect(),
            logical_range_count: result.plan.ranges.len(),
            planned_bytes: result.plan.planned_bytes,
            target_pages: result.plan.target_pages,
            target_shortfall: result.plan.target_shortfall,
            primary_pages_retained: result.plan.primary_pages_retained,
            covered_pages,
            bridge_pages: covered_pages.saturating_sub(result.plan.selected_pages.len()),
        };
        #[derive(Serialize)]
        struct Record<'a> {
            phase: &'static str,
            ordinal: usize,
            truth_opened: bool,
            serving: Serving,
            #[serde(skip_serializing_if = "Option::is_none")]
            canonical_stage: Option<&'a Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            source_cache: Option<SourceCacheRecord>,
            source_nomination_skipped: bool,
            // What `stages.planning` measures: baseline = SOURCE nomination plus cover;
            // direct = the cover computation alone (SOURCE nomination is skipped).
            planning_scope: &'static str,
            returned: &'a [Hit],
            returned_physical_ordinals: &'a [usize],
            returned_count: usize,
            underfill: bool,
            charges: &'a Charges,
            sum: Charge,
            plan: PlanRecord<'a>,
            stages: &'a borsuk::two_bit_generation::QueryStages,
            query_wall_ns: u128,
            query_process_cpu_ns: Option<i128>,
            trace: &'a TwoBitPlanTrace,
            transport: Option<&'a TransportSpan>,
            // Panels only: where this ordinal sits in the executed sequence.
            #[serde(skip_serializing_if = "Option::is_none")]
            selected_slot: Option<usize>,
            // Opt-in range/host diagnostics live here and nowhere in the fields above.
            #[serde(skip_serializing_if = "Option::is_none")]
            diagnostic: Option<QueryDiagnostic<'a>>,
        }
        #[cfg(test)]
        if out.fail_query_output {
            out.cap = out.bytes + 17; // Leave an actual partial query record.
        }
        #[cfg(test)]
        if let Some(cap) = out.panel_query_cap {
            out.panel_cap = cap; // Only query records and later lines meet the smaller cap.
        }
        out.emit(&Record {
            phase: "query",
            ordinal,
            truth_opened: false,
            serving: c.serving,
            canonical_stage: canonical_stage.as_ref(),
            source_cache: SourceCacheRecord::snapshot(c, &generation),
            source_nomination_skipped: result.source_nomination_skipped,
            planning_scope: if result.source_nomination_skipped {
                "direct_cover_only"
            } else {
                "source_nomination_and_cover"
            },
            returned_count: returned.len(),
            underfill,
            returned: &returned,
            returned_physical_ordinals,
            charges: &charges,
            sum: charges.sum(),
            plan,
            stages: &result.stages,
            query_wall_ns: wall_ns,
            query_process_cpu_ns: (!concurrent).then_some(cpu_ns),
            trace: &trace,
            transport: (!concurrent).then_some(&p.transport),
            selected_slot: c.execution.panel().then_some(slot),
            diagnostic: diag.as_ref().map(|diag| diag.query(&anchors)),
        })?;
        p.completed += 1;
        p.underfilled += usize::from(underfill);
        // result, trace and decoded row drop here; no panel-sized retained traces.
    }
    if let Some(cpu_start) = cohort_cpu {
        let elapsed = cohort_wall.elapsed().as_nanos();
        p.query_cpu_ns = cpu_ns() - cpu_start;
        p.transport.finish(&reader);
        let cohort = json!({"phase":"query_cohort","attempted_count":attempted,
            "successful_count":p.completed,"max_active_queries":active_queries(c),
            "elapsed_ns":elapsed,"process_cpu_ns":p.query_cpu_ns,
            "process_cpu_start_ns":cpu_start,"process_cpu_end_ns":cpu_start+p.query_cpu_ns,
            "sum_query_wall_ns":p.query_wall_ns,
            "qps":p.completed as f64 * 1e9 / elapsed as f64,
            "transport":p.transport,"truth_opened":false});
        out.emit(&cohort)?;
        p.cohort = Some(cohort);
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    drop(generation);
    drop(requests);
    seal(c, out, p, request_identity)
}
fn authenticated_prefix(out: &Output, seal: &Seal) -> Result<File> {
    let mut file = regular(
        &out.path,
        Some(usize::try_from(seal.sealed_bytes)?),
        OUTPUT_CAP as usize,
    )?;
    require(
        file_identity(&file)? == seal.file,
        "sealed output identity changed",
    )?;
    let mut prefix = Sha256::new();
    let mut whole = Sha256::new();
    let mut block = [0; BLOCK];
    let mut offset = 0_u64;
    while offset < seal.sealed_bytes {
        let n = usize::try_from((seal.sealed_bytes - offset).min(BLOCK as u64))?;
        file.read_exact(&mut block[..n])?;
        whole.update(&block[..n]);
        let prefix_n = usize::try_from(seal.prefix_bytes.saturating_sub(offset).min(n as u64))?;
        prefix.update(&block[..prefix_n]);
        offset += n as u64;
    }
    require(
        file.read(&mut [0])? == 0
            && file_identity(&file)? == seal.file
            && format!("{:x}", prefix.finalize()) == seal.prefix_sha256
            && format!("{:x}", whole.finalize()) == seal.sealed_sha256,
        "sealed output EOF/prefix/marker SHA",
    )?;
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}
fn truth_row(body: &[u8], ordinal: usize, rows: usize) -> Result<[u64; K]> {
    let row = body
        .get(ordinal * K * 8..(ordinal + 1) * K * 8)
        .ok_or("truth row length")?;
    let mut ids = [0; K];
    for (i, word) in row.chunks_exact(8).enumerate() {
        ids[i] = u64::from_le_bytes(word.try_into()?);
        require(
            ids[i] < rows as u64 && !ids[..i].contains(&ids[i]),
            "truth corpus ordinals/uniqueness",
        )?;
    }
    Ok(ids)
}
fn reduce(c: &Config, out: &mut Output, seal: &Seal, p: &mut Progress) -> Result<Value> {
    let selected = selected_len(c);
    require(
        p.sealed && p.completed == selected,
        "reduction requires complete durable seal",
    )?;
    p.stage = "reauthentication";
    reauthenticate(&c.requests, &seal.requests)?;
    let prefix = authenticated_prefix(out, seal)?;
    p.stage = "truth";
    p.truth_opened = true; // First truth open attempt in the entire pipeline.
    let (truth, _) = checked(&c.truth, c.count * K * 8)?;
    for ordinal in 0..c.count {
        truth_row(&truth, ordinal, c.rows).map_err(|e| format!("truth {ordinal}: {e}"))?;
    }
    p.stage = "reduction";
    let mut reader = BufReader::with_capacity(8192, prefix.take(seal.prefix_bytes));
    let mut line = Vec::with_capacity(SOURCE_BINDING_LINE_CAP + 1);
    let mut digest = Sha256::new();
    let mut consumed = 0_u64;
    let mut queries = 0_usize;
    let mut total_hits = 0_usize;
    #[derive(Deserialize)]
    struct Row {
        phase: String,
        ordinal: Option<usize>,
        returned: Option<Vec<Hit>>,
        returned_count: Option<usize>,
        underfill: Option<bool>,
        truth_opened: Option<bool>,
    }
    loop {
        line.clear();
        let n = Read::take(&mut reader, (SOURCE_BINDING_LINE_CAP + 1) as u64)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        require(
            n <= SOURCE_BINDING_LINE_CAP && line.last() == Some(&b'\n'),
            "sealed row cap/EOF",
        )?;
        consumed += n as u64;
        digest.update(&line);
        // Ignored trace fields deserialize without constructing a JSON tree.
        let row: Row = serde_json::from_slice(&line)?;
        require(
            n <= if row.phase == "source_binding" {
                SOURCE_BINDING_LINE_CAP
            } else {
                LINE_CAP
            },
            "sealed phase-specific row cap",
        )?;
        if row.phase != "query" {
            continue;
        }
        let ordinal = (queries < selected).then(|| ordinal_at(c, queries));
        require(
            ordinal.is_some() && row.ordinal == ordinal && row.truth_opened == Some(false),
            "sealed query sequence/truth boundary",
        )?;
        let returned = row.returned.ok_or("sealed returned IDs missing")?;
        validate_hits(&returned, c.rows, c.serving.fp32().is_some())?;
        require(
            row.returned_count == Some(returned.len()) && row.underfill == Some(returned.len() < K),
            "sealed returned count/underfill",
        )?;
        let truth_ids = truth_row(&truth, ordinal_at(c, queries), c.rows)?;
        let hits = returned
            .iter()
            .filter(|h| truth_ids.contains(&h.id))
            .count();
        total_hits += hits;
        out.emit(
            &json!({"phase":"recall","ordinal":ordinal_at(c, queries),"hits10":hits,
            "recall10":hits as f64 / K as f64,"returned_count":returned.len(),
            "underfill":returned.len() < K}),
        )?;
        queries += 1;
    }
    require(
        consumed == seal.prefix_bytes
            && format!("{:x}", digest.finalize()) == seal.prefix_sha256
            && queries == selected,
        "reduction complete bound prefix",
    )?;
    // A replaced path must not detach the published result from the descriptor we wrote.
    let current = regular(
        &out.path,
        Some(usize::try_from(out.bytes)?),
        OUTPUT_CAP as usize,
    )?;
    let current_id = file_identity(&current)?;
    require(
        current_id.dev == seal.file.dev && current_id.ino == seal.file.ino,
        "output path replaced during reduction",
    )?;
    let mut summary = json!({"status":"MEASURED","complete":true,"queries":queries,"k":K,
        "total_hits10":total_hits,"recall_numerator":total_hits,"recall_denominator":selected * K,
        "mean_recall10":total_hits as f64 / (selected * K) as f64,"underfilled_queries":p.underfilled,
        "all_queries_sealed":true,"prefix_bytes":seal.prefix_bytes,"prefix_sha256":seal.prefix_sha256,
        "sealed_bytes":seal.sealed_bytes,"sealed_sha256":seal.sealed_sha256,
        "requests_sha256":c.requests.sha256,"truth_sha256":c.truth.sha256,
        "generation_root_sha256":c.generation_root_sha256,"charges":p.charges,"sum":p.charges.sum(),
        "binding_charge":p.binding_charge,
        "serving":c.serving,"direct_memory":p.direct_memory,
        "query_wall_ns":p.query_wall_ns,"query_process_cpu_ns":p.query_cpu_ns,
        "physical_s3_measured":false,"external_gate_required":true});
    // Added after the literal: json! recursion depth grows with the entry count.
    if let Some(cohort) = &p.cohort {
        summary["query_cohort"] = cohort.clone();
    }
    summary["execution"] = json!(c.execution);
    summary["selected_count"] = json!(selected);
    summary["executed_count"] = json!(queries);
    summary["population_count"] = json!(c.count);
    summary["reserved_query_count"] =
        json!(c.reserved_query_interval.end - c.reserved_query_interval.start);
    summary["diagnostic_prefix"] =
        json!(c.count < c.reserved_query_interval.end - c.reserved_query_interval.start);
    summary["population_percentiles_valid"] = json!(!c.execution.panel() && c.count == 1000);
    summary["full_cohort_qualification"] = json!(false);
    summary["diagnostic_panel"] = json!(c.execution.panel());
    if c.execution.panel() || c.count < 1000 {
        // A panel or prefix measures the selected ordinals only: its denominators and any percentile
        // over it describe that enriched selection, never the population.
        summary["population_percentiles_valid"] = json!(false);
        summary["full_cohort_qualification"] = json!(false);
        summary["scope_note"] = json!(
            "diagnostic selected original ordinals or prefix; not population p99, population recall or full-cohort qualification"
        );
    }
    Ok(summary)
}
// Test-only conveniences over `execute_paths_status`, which `main` calls directly.
#[cfg(test)]
fn execute_paths(
    config_path: &Path,
    config_sha: &str,
    output_path: &Path,
    shape: Shape,
) -> Result<bool> {
    execute_paths_with(
        config_path,
        config_sha,
        output_path,
        shape,
        |c, shape, out, p| configured_query_runtime(&c)?.block_on(query_and_seal(c, shape, out, p)),
    )
}
#[cfg(test)]
fn execute_paths_with(
    config_path: &Path,
    config_sha: &str,
    output_path: &Path,
    shape: Shape,
    query: impl FnOnce(&Config, Shape, &mut Output, &mut Progress) -> Result<Seal>,
) -> Result<bool> {
    execute_paths_status(config_path, config_sha, output_path, shape, query)
        .map(|status| status == Status::Measured)
}
fn read_retry_identity(policy: S3ReadRetryPolicy) -> Value {
    json!({
        "policy":policy,
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
    })
}

fn execute_paths_status(
    config_path: &Path,
    config_sha: &str,
    output_path: &Path,
    shape: Shape,
    query: impl FnOnce(&Config, Shape, &mut Output, &mut Progress) -> Result<Seal>,
) -> Result<Status> {
    require(
        config_path.as_os_str().len() <= 4096
            && output_path.as_os_str().len() <= 4096
            && config_sha.len() == 64,
        "bounded CLI paths/config SHA",
    )?;
    let mut out = Output::create(output_path)?;
    let mut p = Progress::default();
    let started = Instant::now();
    let cpu = cpu_ns();
    let result = (|| -> Result<Value> {
        let configured = config(config_path, config_sha, shape);
        let mut identity = json!({"schema":RESULT_SCHEMA,"phase":"identity","config_sha256":config_sha,
            "fetch_parallelism":configured.as_ref().ok().map(|c| c.fetch_parallelism),
            "source_get_budget":configured.as_ref().ok().map(|c| c.source_get_budget),
            "sq8_get_budget":configured.as_ref().ok().map(|c| c.sq8_get_budget),
            "sq8_byte_budget":configured.as_ref().ok().map(|c| c.sq8_byte_budget),
            "serving":configured.as_ref().ok().map(|c| c.serving),
            "binary_sha256":executable_sha()?,"runner_source_sha256":hash(include_bytes!("check_cohere_native_baseline.rs")),
            "generation_source_sha256":hash(include_bytes!("../two_bit_generation.rs")),
            "router_source_sha256":hash(include_bytes!("../semantic_unit_router.rs")),
            "codec_source_sha256":hash(include_bytes!("../rotated_two_bit.rs")),
            "source_plane_source_sha256":hash(include_bytes!("../two_bit_source.rs")),
            "scope":"AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS","physical_s3_measured":false,
            "io_measurement":"logical_GET_charges_separate_from_cumulative_process_native_transport",
            "s3_credential_source":"imds_instance_role_only",
            "native_transport_includes":"S3_and_IMDS_credential_requests_including_PUT",
            "wire_bytes":null,"unread_bytes":null,"billed_bytes":null,"billed_requests":null,
            "external_gate_required":true,"truth_opened":false});
        // Added after the literal: json! recursion depth grows with the entry count.
        if configured
            .as_ref()
            .ok()
            .is_some_and(|c| c.serving.fp32().is_some())
        {
            identity["fp32_rerank_source_sha256"] =
                json!(hash(include_bytes!("../fp32_rerank.rs")));
            identity["canonical_source_sha256"] =
                json!(hash(include_bytes!("../canonical_source.rs")));
        }
        identity["source_capsule_source_sha256"] =
            json!(hash(include_bytes!("../source_capsule.rs")));
        identity["config_schema"] = json!(
            configured
                .as_ref()
                .map(|c| c.schema.as_str())
                .unwrap_or(CONFIG_SCHEMA)
        );
        if let Some(active) = configured.as_ref().ok().and_then(|c| c.max_active_queries) {
            identity["schema"] = json!(CONCURRENT_RESULT_SCHEMA);
            identity["max_active_queries"] = json!(active);
            identity["query_runtime"] = json!(configured.as_ref().unwrap().query_runtime);
            identity["query_dispatch"] = json!("spawned_ordered_batches");
            identity["query_wall_semantics"] =
                json!("dispatch_to_completion_including_initial_runtime_queue");
        }
        identity["source_capsule_verification_policy"] = json!(SOURCE_CAPSULE_VERIFICATION_POLICY);
        identity["source_frame_source_sha256"] = json!(hash(include_bytes!("../source_frame.rs")));
        identity["source_authentication"] =
            json!(configured.as_ref().ok().map(|c| c.source_authentication));
        identity["source_layout"] = json!(configured.as_ref().ok().map(|c| &c.source_layout));
        identity["source_byte_budget"] =
            json!(configured.as_ref().ok().map(|c| c.source_byte_budget));
        identity["max_memory_bytes"] = json!(configured.as_ref().ok().map(|c| c.max_memory_bytes));
        identity["range_scheduler_owner"] = json!("library");
        identity["sq8_range_split"] =
            json!(configured.as_ref().ok().and_then(|c| c.sq8_range_split));
        identity["execution"] = json!(configured.as_ref().ok().map(|c| &c.execution));
        identity["sq8_range_source_sha256"] = json!(hash(include_bytes!("../sq8_s3_range.rs")));
        identity["returned_source_sha256"] = json!(hash(include_bytes!("../returned_sq8.rs")));
        if let Some(window) = configured.as_ref().ok().and_then(|c| c.shared_byte_window) {
            identity["shared_byte_window"] = json!(window);
        }
        if let Some(policy) = configured.as_ref().ok().and_then(|c| c.s3_read_retry) {
            identity["s3_read_retry"] = read_retry_identity(policy);
        }
        if let Some(limit) = configured.as_ref().ok().and_then(|c| c.semantic_leaf_limit) {
            identity["semantic_leaf_limit"] = json!(limit);
        }
        if let Some(c) = configured.as_ref().ok().filter(|c| c.source_cache_bytes > 0) {
            identity["source_cache_bytes"] = json!(c.source_cache_bytes);
        }
        out.emit(&identity)?;
        let c = configured?;
        let shape = population(&c, shape);
        let ReceiptBinding {
            reserved_queries_sha256,
            ..
        } = validate_receipts(&c)?;
        let mut bound = json!({"phase":"bound_inputs","dataset":c.dataset,"revision":c.revision,
            "fetch_parallelism":c.fetch_parallelism,"source_get_budget":c.source_get_budget,"source_cache":"off","serving":c.serving,
            "sq8_get_budget":c.sq8_get_budget,"sq8_byte_budget":c.sq8_byte_budget,
            "metric":c.metric,"tie_rule":c.tie_rule,"rows":c.rows,"dimensions":D,"count":c.count,"k":K,
            "corpus_source_first":c.corpus_source_first,"query_source_first":c.query_source_first,
            "profile":c.profile,"backend":c.backend,"generation_prefix":c.generation_prefix,
            "credential_source":matches!(&c.backend, Backend::S3 { .. }).then_some("imds_instance_role_only"),
            "generation_root_sha256":c.generation_root_sha256,
            "requests_bytes":c.requests.bytes,"requests_sha256":c.requests.sha256,
            "truth_bytes":c.truth.bytes,"truth_sha256":c.truth.sha256,
            "native_source_sha256":c.native_source.source_sha256,"native_sq8_sha256":c.native_source.sq8_sha256,
            "native_order_sha256":c.native_source.source_order_sha256,"truth_opened":false});
        if c.source_cache_bytes > 0 {
            bound["source_cache_bytes"] = json!(c.source_cache_bytes);
            bound["source_cache"] = json!("authenticated_direct_mapped256_v1");
        }
        bound["corpus_intervals"] = json!(c.corpus_intervals);
        bound["reserved_query_interval"] = json!(c.reserved_query_interval);
        bound["reserved_queries_sha256"] = json!(reserved_queries_sha256);
        bound["cohort_receipt_sha256"] = json!(c.cohort_receipt.sha256);
        bound["derivation_receipt_sha256"] = json!(c.derivation_receipt.sha256);
        bound["producer_authority"] = json!(c.producer_authority);
        bound["max_memory_bytes"] = json!(c.max_memory_bytes);
        bound["source_byte_budget"] = json!(c.source_byte_budget);
        bound["source_authentication"] = json!(c.source_authentication);
        if let Some(limit) = c.semantic_leaf_limit {
            bound["semantic_leaf_limit"] = json!(limit);
        }
        bound["source_layout"] = json!(c.source_layout);
        bound["config_schema"] = json!(c.schema);
        if let Some(active) = c.max_active_queries {
            bound["max_active_queries"] = json!(active);
            bound["query_runtime"] = json!(c.query_runtime);
            bound["query_dispatch"] = json!("spawned_ordered_batches");
            bound["query_wall_semantics"] =
                json!("dispatch_to_completion_including_initial_runtime_queue");
        }
        bound["source_capsule_verification_policy"] = json!(SOURCE_CAPSULE_VERIFICATION_POLICY);
        bound["range_scheduler_owner"] = json!("library");
        bound["sq8_range_split"] = json!(c.sq8_range_split);
        bound["execution"] = json!(c.execution);
        bound["selected_count"] = json!(selected_len(&c));
        if let Some(window) = c.shared_byte_window {
            bound["shared_byte_window"] = json!(window);
        }
        if let Some(policy) = c.s3_read_retry {
            bound["s3_read_retry"] = read_retry_identity(policy);
        }
        out.emit(&bound)?;
        let seal = query(&c, shape, &mut out, &mut p)?;
        let summary = reduce(&c, &mut out, &seal, &mut p)?;
        p.stage = "reduction_sync";
        out.file.sync_all()?;
        Ok(summary)
    })();
    let mut summary = match result {
        Ok(summary) => summary,
        Err(e) => match e.downcast_ref::<ResourceReject>() {
            Some(reject) => resource_reject_summary(&p, reject),
            None => invalid_summary(&p, &e.to_string()),
        },
    };
    summary["transport_last_boundary"] = p.transport.terminal();
    summary["process_wall_ns"] = json!(started.elapsed().as_nanos());
    summary["process_cpu_ns"] = json!(cpu_ns() - cpu);
    summary["observed_process_peak_bytes"] = json!(peak_bytes());
    out.cap = OUTPUT_CAP;
    // A cap failure may have left a partial record; start any non-MEASURED terminal on a new line.
    if summary["status"] != "MEASURED" {
        out.line_bytes = 0;
        out.write_all(b"\n")?;
    }
    out.emit(&json!({"phase":"terminal","summary":summary}))?;
    if let Err(e) = out.file.sync_all() {
        // Never print a successful measurement when its final durable write failed.
        p.stage = "terminal_sync";
        out.emit(&json!({"phase":"terminal","summary":invalid_summary(&p, &e.to_string())}))?;
        let _ = out.file.sync_all();
        return Err(format!("terminal sync: {e}").into());
    }
    println!("{summary}");
    Ok(match summary["status"].as_str() {
        Some("MEASURED") => Status::Measured,
        Some("RESOURCE_REJECT") => Status::ResourceReject,
        _ => Status::Invalid,
    })
}
// Terminal for a valid-input direct-closure refusal by an enforced library cap: incomplete,
// never sealed or reduced, and distinct from INVALID (malformed config, authentication,
// transport or accounting defects). Host OOM, deadline and kill are NOT classified here:
// the runner cannot observe them, and an exit status never implies them.
fn resource_reject_summary(p: &Progress, reject: &ResourceReject) -> Value {
    json!({"status":"RESOURCE_REJECT","complete":false,"resource_rejection":reject.reason,
        "stage":p.stage,"error":reject.message.chars().take(512).collect::<String>(),
        "completed_queries":p.completed,"all_queries_sealed":p.sealed,"truth_opened":p.truth_opened,
        "charges":p.charges,"sum":p.charges.sum(),"binding_charge":p.binding_charge,
        "direct_memory":p.direct_memory,"transport_last_boundary":p.transport.terminal(),
        "scope":"library_enforced_modeled_memory_or_planned_byte_cap_only",
        "host_oom_or_deadline_inferred":false,
        "physical_s3_measured":false,"external_gate_required":true})
}
// Put the terminal cause before URI-heavy wrappers; native S3 drops HTTP error bodies.
fn query_error_text(mut error: &TwoBitGenerationError) -> String {
    use borsuk::sq8_s3_range::RangeFetchError;
    let mut phase = "query";
    for _ in 0..32 {
        let range = match error {
            TwoBitGenerationError::Query { error: inner, .. }
            | TwoBitGenerationError::RouterCharged { error: inner, .. } => {
                error = inner;
                continue;
            }
            TwoBitGenerationError::SourcePlanning { error: inner, .. } => {
                phase = "source_planning";
                error = inner;
                continue;
            }
            TwoBitGenerationError::SourceRead(failure) => ("source", &failure.error),
            TwoBitGenerationError::Read(failure)
            | TwoBitGenerationError::PagedRead { sq8: failure, .. } => ("sq8", &failure.error),
            other => {
                return format!("phase={phase} cause={other}")
                    .chars()
                    .take(512)
                    .collect();
            }
        };
        let (phase, range) = range;
        let RangeFetchError::Store(store) = range else {
            return format!("phase={phase} cause={range:?}")
                .chars()
                .take(512)
                .collect();
        };
        let category = match store {
            object_store::Error::Generic { .. } => "Generic",
            object_store::Error::NotFound { .. } => "NotFound",
            object_store::Error::InvalidPath { .. } => "InvalidPath",
            object_store::Error::JoinError { .. } => "JoinError",
            object_store::Error::NotSupported { .. } => "NotSupported",
            object_store::Error::AlreadyExists { .. } => "AlreadyExists",
            object_store::Error::Precondition { .. } => "Precondition",
            object_store::Error::NotModified { .. } => "NotModified",
            object_store::Error::NotImplemented { .. } => "NotImplemented",
            object_store::Error::PermissionDenied { .. } => "PermissionDenied",
            object_store::Error::Unauthenticated { .. } => "Unauthenticated",
            object_store::Error::UnknownConfigurationKey { .. } => "UnknownConfigurationKey",
            _ => "Other",
        };
        let prefix = format!("phase={phase} store={category} cause=");
        let mut leaf: &dyn Error = store;
        for _ in 0..32 {
            match leaf.source() {
                Some(source) => leaf = source,
                None => return format!("{prefix}{leaf}").chars().take(512).collect(),
            }
        }
        return format!("{prefix}[source chain limit]");
    }
    "phase=query cause=[wrapper limit]".into()
}

fn invalid_summary(p: &Progress, error: &str) -> Value {
    json!({"status":"INVALID","complete":false,"stage":p.stage,
        "error":error.chars().take(512).collect::<String>(),"completed_queries":p.completed,
        "all_queries_sealed":p.sealed,"truth_opened":p.truth_opened,"charges":p.charges,"sum":p.charges.sum(),
        "binding_charge":p.binding_charge,"transport_last_boundary":p.transport.terminal(),
        "physical_s3_measured":false,"external_gate_required":true})
}
fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let result = if args.len() == 4 {
        execute_paths_status(
            Path::new(&args[1]),
            &args[2],
            Path::new(&args[3]),
            Shape::PRODUCTION,
            |c, shape, out, p| {
                configured_query_runtime(&c)?.block_on(query_and_seal(c, shape, out, p))
            },
        )
    } else {
        Err("usage: check_cohere_native_baseline CONFIG CONFIG_SHA NEW_OUTPUT".into())
    };
    match result {
        Ok(Status::Measured) => (),
        Ok(status) => std::process::exit(status.exit_code()),
        Err(e) => {
            eprintln!("INVALID: {e}; external gate required");
            std::process::exit(2);
        }
    }
}

// The bounded producer/consumer test uses the actual existing example module,
// retaining its executable/embedded identity and unmodified v2 receipt bytes.
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../examples/build_sq8_source.rs"]
mod scale_derivation_producer;

// Shared by the reducer's native integration test; all execution stays in the root test gate.
#[cfg(test)]
pub(crate) fn scale_reducer_native_fixture(
    prefix: bool,
    panel: bool,
) -> Result<(tempfile::TempDir, PathBuf)> {
    scale_reducer_native_fixture_count(
        if prefix { 1 } else { 2 },
        panel.then_some(if prefix { 0 } else { 1 }),
    )
}

#[cfg(test)]
pub(crate) fn scale_reducer_native_fixture_count(
    count: usize,
    panel_ordinal: Option<usize>,
) -> Result<(tempfile::TempDir, PathBuf)> {
    let shape = Shape {
        count,
        ..Shape::tiny(32)
    };
    let (dir, mut value) = query_runtime(default_fetch_parallelism())?
        .block_on(tests::fixture_with_profile(shape, SemanticProfile::Scale1m));
    value["reserved_query_interval"] = json!({"start":32,"end":32+count.max(2)});
    if let Some(ordinal) = panel_ordinal {
        value["execution"] = json!({"mode":"diagnostic_panel","ordinals":[ordinal],"trace":false});
    }
    let (config, sha) = tests::write_config(dir.path(), &value);
    let result = dir.path().join("scale-native-result.jsonl");
    let status = execute_paths_status(&config, &sha, &result, shape, |c, shape, out, progress| {
        configured_query_runtime(&c)?.block_on(query_and_seal(c, shape, out, progress))
    })?;
    require(
        matches!(status, Status::Measured),
        "real native scale fixture must finish and seal",
    )?;
    Ok((dir, result))
}

#[cfg(test)]
pub(crate) fn fp32_reducer_native_fixture() -> Result<(tempfile::TempDir, PathBuf)> {
    fp32_reducer_native_fixture_with(false, None)
}
#[cfg(test)]
pub(crate) fn fp32_reducer_native_fixture_with(coalesce: bool, leaf_limit: Option<usize>) -> Result<(tempfile::TempDir, PathBuf)> {
    fp32_reducer_native_fixture_selection(coalesce, leaf_limit, false)
}
#[cfg(test)]
pub(crate) fn fp32_reducer_native_fixture_selection(coalesce: bool, leaf_limit: Option<usize>, direct: bool) -> Result<(tempfile::TempDir, PathBuf)> {
    fp32_reducer_native_fixture_caps(coalesce, leaf_limit, direct, None)
}
#[cfg(test)]
pub(crate) fn fp32_reducer_native_fixture_caps(coalesce: bool, leaf_limit: Option<usize>, direct: bool, caps: Option<(usize, usize)>) -> Result<(tempfile::TempDir, PathBuf)> {
    fp32_reducer_native_fixture_cached(coalesce, leaf_limit, direct, caps, 0)
}
#[cfg(test)]
pub(crate) fn fp32_reducer_native_fixture_cached(coalesce: bool, leaf_limit: Option<usize>, direct: bool, caps: Option<(usize, usize)>, cache_bytes: usize) -> Result<(tempfile::TempDir, PathBuf)> {
    fp32_reducer_native_fixture_options(coalesce, leaf_limit, direct, caps, cache_bytes, None)
}
#[cfg(test)]
pub(crate) fn cold_source_profile_native_fixture(
    trace: bool,
) -> Result<(tempfile::TempDir, PathBuf)> {
    cold_source_profile_native_fixture_leaf(trace, 64)
}
#[cfg(test)]
pub(crate) fn cold_source_profile_native_fixture_leaf(
    trace: bool,
    leaf: usize,
) -> Result<(tempfile::TempDir, PathBuf)> {
    fp32_reducer_native_fixture_options(false, Some(leaf), false, None, 0, Some(trace))
}
#[cfg(test)]
fn fp32_reducer_native_fixture_options(
    coalesce: bool,
    leaf_limit: Option<usize>,
    direct: bool,
    caps: Option<(usize, usize)>,
    cache_bytes: usize,
    profile: Option<bool>,
) -> Result<(tempfile::TempDir, PathBuf)> {
    let shape = Shape::tiny(33);
    let (dir, mut value) = query_runtime(default_fetch_parallelism())?
        .block_on(tests::fixture_with_profile(shape, SemanticProfile::Scale1m));
    value["reserved_query_interval"] = json!({"start":33,"end":35});
    if cache_bytes > 0 {
        value["source_cache_bytes"] = json!(cache_bytes);
    }
    if let Some(limit) = leaf_limit {
        value["semantic_leaf_limit"] = json!(limit);
        value["execution"] = json!({"mode":"diagnostic_panel","ordinals":[0,1],"trace":false});
    }
    value["serving"] = json!({"mode":"fp32_rerank","coalesce":coalesce,"shortlist":16,"max_fp32_bytes":(if coalesce {33} else {16})*(D*4+8),
        "max_fp32_gets":16,"max_parallel_fp32_gets":3});
    if direct {
        value["serving"]["direct"] = json!({"max_sq8_bytes":1_000_000,"max_sq8_gets":128});
    }
    if let Some((ordinary, source)) = caps {
        value["sq8_get_budget"] = json!(ordinary);
        value["source_get_budget"] = json!(source);
    }
    if let Some(trace) = profile {
        value["execution"]["trace"] = json!(trace);
        value["fetch_parallelism"] = json!(128);
        value["source_authentication"] = json!("offload2");
    }
    let (config, sha) = tests::write_config(dir.path(), &value);
    let result = dir.path().join("fp32-native-result.jsonl");
    let status = execute_paths_status(&config, &sha, &result, shape, |c, shape, out, progress| {
        configured_query_runtime(c)?.block_on(query_and_seal(c, shape, out, progress))
    })?;
    require(
        matches!(status, Status::Measured),
        "native FP32 fixture must seal",
    )?;
    Ok((dir, result))
}
#[cfg(test)]
pub(crate) fn source_layout_reducer_native_fixture() -> Result<(tempfile::TempDir, [PathBuf; 2])> {
    source_layout_reducer_native_fixture_with(32, 2, false, false)
}

#[cfg(test)]
pub(crate) fn source_layout_reducer_native_fixture_with(
    rows: usize,
    count: usize,
    panel: bool,
    padded_authority: bool,
) -> Result<(tempfile::TempDir, [PathBuf; 2])> {
    source_layout_reducer_profile_fixture(rows, count, panel, padded_authority, false)
}

#[cfg(test)]
pub(crate) fn source_layout_reducer_profile_fixture(
    rows: usize,
    count: usize,
    panel: bool,
    padded_authority: bool,
    traced: bool,
) -> Result<(tempfile::TempDir, [PathBuf; 2])> {
    source_layout_reducer_layout_fixture(
        rows,
        count,
        panel,
        padded_authority,
        traced,
        borsuk::two_bit_generation::SourceLayoutMode::LeafCapsulesV2,
    )
}

#[cfg(test)]
pub(crate) fn source_layout_reducer_layout_fixture(
    rows: usize,
    count: usize,
    panel: bool,
    padded_authority: bool,
    traced: bool,
    layout: borsuk::two_bit_generation::SourceLayoutMode,
) -> Result<(tempfile::TempDir, [PathBuf; 2])> {
    use borsuk::two_bit_build::repackage_source_layout;
    let shape = Shape {
        rows,
        count,
        returned_limit: K,
    };
    let (dir, mut value) = query_runtime(default_fetch_parallelism())?
        .block_on(tests::fixture_with_profile(shape, SemanticProfile::Scale1m));
    value["reserved_query_interval"] = json!({"start":rows,"end":rows + shape.count});
    if panel {
        value["execution"] = json!({"mode":"diagnostic_panel","ordinals":(1..count).collect::<Vec<_>>(),"trace":traced});
    }
    if padded_authority {
        value["source_byte_budget"] = json!(67_108_865);
        value["max_memory_bytes"] = json!(MEMORY_CAP + 1);
        let root = dir.path().join("generation");
        let mut page = std::fs::read(root.join("page_manifest.json"))?;
        require(page.len() <= CONFIG_CAP, "fixture page cap")?;
        page.resize(CONFIG_CAP, b'\t');
        std::fs::write(root.join("page_manifest.json"), &page)?;
        let mut manifest: Value =
            serde_json::from_slice(&std::fs::read(root.join("manifest.json"))?)?;
        manifest["page_manifest_sha256"] = json!(hash(&page));
        let mut body = serde_json::to_vec(&manifest)?;
        require(body.len() <= CONFIG_CAP, "fixture root cap")?;
        body.resize(CONFIG_CAP, b'\t');
        std::fs::write(root.join("manifest.json"), &body)?;
        value["generation_root_sha256"] = json!(hash(&body));
        let c: Config = serde_json::from_value(value.clone())?;
        let store = Reader::new(&c.backend, None)?;
        let head = configured_query_runtime(&c)?.block_on(
            borsuk::two_bit_store::publish_two_bit_generation(
                store.store(),
                &ObjectPath::from("padded/index"),
                &root,
                &c.generation_root_sha256,
                limits(&c)?,
                None,
            ),
        )?;
        value["generation_prefix"] = json!(head.metadata_prefix().as_ref());
    }
    let (config, sha) = tests::write_config(dir.path(), &value);
    let flat = dir.path().join("scale-native-result.jsonl");
    require(
        matches!(
            execute_paths_status(&config, &sha, &flat, shape, |c, shape, out, p| {
                configured_query_runtime(&c)?.block_on(query_and_seal(c, shape, out, p))
            })?,
            Status::Measured
        ),
        "flat layout fixture must seal",
    )?;
    let c: Config = serde_json::from_value(value.clone())?;
    let root = dir.path().join("capsule-generation");
    let sha = repackage_source_layout(
        &dir.path().join("generation"),
        &c.generation_root_sha256,
        &root,
        2,
        layout,
        limits(&c)?,
    )?;
    let manifest: Value = serde_json::from_slice(&std::fs::read(root.join("manifest.json"))?)?;
    value["source_layout"] = manifest["source_layout"].clone();
    value["generation_root_sha256"] = json!(sha);
    let store = Reader::new(&c.backend, None)?;
    let head = configured_query_runtime(&c)?.block_on(
        borsuk::two_bit_store::publish_two_bit_generation(
            store.store(),
            &ObjectPath::from("capsule/index"),
            &root,
            &sha,
            limits(&c)?,
            None,
        ),
    )?;
    value["generation_prefix"] = json!(head.metadata_prefix().as_ref());
    let config = dir.path().join("capsule-config.json");
    let body = serde_json::to_vec(&value)?;
    std::fs::write(&config, &body)?;
    let capsules = dir.path().join("capsule-result.jsonl");
    require(
        matches!(
            execute_paths_status(
                &config,
                &hash(&body),
                &capsules,
                shape,
                |c, shape, out, p| configured_query_runtime(&c)?
                    .block_on(query_and_seal(c, shape, out, p))
            )?,
            Status::Measured
        ),
        "capsule fixture must seal before truth",
    )?;
    Ok((dir, [flat, capsules]))
}

// Header-only producer interoperability fixture: the injected query never opens S3.
#[cfg(test)]
pub(crate) fn scale_reducer_retry_header_fixture(dir: &Path) -> Result<PathBuf> {
    let mut value: Value = serde_json::from_slice(&std::fs::read(dir.join("config.json"))?)?;
    let shape = Shape {
        rows: value["rows"].as_u64().ok_or("fixture rows")? as usize,
        count: value["count"].as_u64().ok_or("fixture count")? as usize,
        returned_limit: K,
    };
    value["schema"] = json!(CONFIG_SCHEMA);
    value["s3_read_retry"] = json!("one_retry_100ms");
    value["backend"] = json!({
        "kind":"s3","bucket":"fixture-bucket","region":"eu-central-1",
        "physical_prefix":"physical/ns",
        "sq8_object_key":format!("semantic/index/objects/{}", value["native_source"]["sq8_sha256"].as_str().ok_or("fixture SQ8 SHA")?),
        "sq8_etag":"\"frozen\""
    });
    let (config, sha) = tests::write_config(dir, &value);
    let result = dir.join("scale-native-retry-headers.jsonl");
    let status = execute_paths_status(&config, &sha, &result, shape, |_, _, _, _| {
        Err("header-only retry fixture".into())
    })?;
    require(
        matches!(status, Status::Invalid),
        "header-only fixture must stop before queries",
    )?;
    Ok(result)
}

#[cfg(test)]
pub(crate) fn runtime_task_reducer_fixture(
    active: usize,
    multi: bool,
) -> (tempfile::TempDir, Vec<Value>) {
    tests::runtime_task_fixture(
        active,
        if multi {
            QueryRuntime::MultiThread2
        } else {
            QueryRuntime::CurrentThread
        },
    )
}

/// Small actual producer trace for the offline frame adapter's full-population fixture.
#[cfg(test)]
pub(crate) fn source_frame_range_reducer_fixture() -> (tempfile::TempDir, PathBuf) {
    let shape = Shape {
        count: 1000,
        ..Shape::tiny(32)
    };
    let (dir, mut value) = query_runtime(64).unwrap().block_on(tests::fixture(shape));
    value["schema"] = json!(CONCURRENT_CONFIG_SCHEMA);
    value["max_active_queries"] = json!(2);
    value["query_runtime"] = json!("multi_thread2");
    value["fetch_parallelism"] = json!(64);
    value["source_authentication"] = json!("offload2");
    let (status, _) = tests::run_status(dir.path(), "frame-full.jsonl", &value, shape);
    assert_eq!(status, Status::Measured);
    let trace = dir.path().join("frame-full.jsonl");
    (dir, trace)
}

#[cfg(test)]
pub(crate) fn concurrent_reducer_fixture(active: usize) -> (tempfile::TempDir, Vec<Value>) {
    tests::concurrent_fixture(active)
}
#[cfg(test)]
mod tests {
    use super::*;
    use borsuk::{
        two_bit_build::TwoBitGenerationBuilder, two_bit_source::TwoBitSource,
        two_bit_store::publish_two_bit_generation,
    };

    fn artifact(path: &Path, bytes: &[u8]) -> Value {
        std::fs::write(path, bytes).unwrap();
        json!({"path":path,"bytes":bytes.len(),"sha256":hash(bytes)})
    }
    fn config_value(dir: &Path, shape: Shape) -> Value {
        let mut value = json!({"source_layout":{"layout":"flat_records"},"source_byte_budget":64*1024*1024,"sq8_range_split":null,"schema":CONFIG_SCHEMA,"dataset":DATASET,"revision":REVISION,
            "metric":"cosine","tie_rule":"corpus_ordinal_ascending",
            "reserved_query_interval":{"start":shape.rows,"end":shape.rows+shape.count},
            "corpus_intervals":[{"start":0,"end":shape.rows}],"corpus_source_first":0,"query_source_first":shape.rows,"rows":shape.rows,
            "dimensions":D,"count":shape.count,"k":K,"profile":"native100k",
            "backend":{"kind":"local","store_root":dir.join("store")},"generation_prefix":"semantic/index",
            "generation_root_sha256":"a".repeat(64),"scratch_parent":dir.join("scratch"),
            "requests":{"path":dir.join("requests.f32"),"bytes":shape.count*D*4,"sha256":"b".repeat(64)},
            "truth":{"path":dir.join("truth.u64"),"bytes":shape.count*K*8,"sha256":"c".repeat(64)},
            "native_source":{"source_sha256":"d".repeat(64),"sq8_sha256":"e".repeat(64),
                "source_order_sha256":"f".repeat(64)},"max_memory_bytes":MEMORY_CAP,
            "sq8_get_budget":32,"sq8_byte_budget":16_773_120,
            "serving":{"mode":"baseline"},"execution":{"mode":"full"}});
        fixture_receipts(dir, &mut value);
        value
    }
    fn fixture_receipts(dir: &Path, value: &mut Value) {
        if value.get("producer_authority").is_none() {
            value["producer_authority"] = json!({"source_commit":"4".repeat(40),
                "executable_sha256":"5".repeat(64),"producer_source_sha256":"6".repeat(64),
                "sq8_source_sha256":"7".repeat(64),"source_order_source_sha256":"8".repeat(64)});
        }
        let reserved_count = value["reserved_query_interval"]["end"].as_u64().unwrap()
            - value["reserved_query_interval"]["start"].as_u64().unwrap();
        let reserved_sha = if value["count"] == reserved_count {
            value["requests"]["sha256"].clone()
        } else {
            json!("3".repeat(64))
        };
        let receipt = json!({"schema":"borsuk-cohere-native-cohort-receipt-v3","status":"COMPLETE",
            "dataset":DATASET,"revision":REVISION,"reserved_queries_sha256":reserved_sha,"geometry":{"reserved_query_interval":value["reserved_query_interval"],"corpus_rows":value["rows"],"query_rows":value["count"],
                "dimensions":D,"k":K,"corpus_intervals":value["corpus_intervals"],
                "query_source_ordinals":[value["query_source_first"].as_u64().unwrap(),value["query_source_first"].as_u64().unwrap()+value["count"].as_u64().unwrap()]},
            "outputs":[{"name":"corpus.f32","bytes":value["rows"].as_u64().unwrap()*D as u64*4,"sha256":"1".repeat(64)},
                {"name":"queries.f32","bytes":value["requests"]["bytes"],"sha256":value["requests"]["sha256"]},
                {"name":"truth.u64","bytes":value["truth"]["bytes"],"sha256":value["truth"]["sha256"]}]});
        value["cohort_receipt"] = artifact(
            &dir.join("cohort.json"),
            &serde_json::to_vec(&receipt).unwrap(),
        );
        let derivation = json!({"schema":"borsuk-native-scale-derivation-receipt-v2","producer_config_sha256":"9".repeat(64),"status":"COMPLETE",
            "recipe":"normalize_then_flat_fit_then_sq8","query_or_truth_used":false,
            "rows":value["rows"],"dimensions":D,"corpus_intervals":value["corpus_intervals"],
            "original_corpus":{"bytes":value["rows"].as_u64().unwrap()*D as u64*4,"sha256":"1".repeat(64)},
            "outputs":{"normalized":{"bytes":value["rows"].as_u64().unwrap()*D as u64*4,"sha256":value["native_source"]["source_sha256"]},
                "source_order":{"bytes":value["rows"].as_u64().unwrap()*8,"sha256":value["native_source"]["source_order_sha256"]},
                "sq8":{"bytes":value["rows"].as_u64().unwrap()*(D as u64+12),"sha256":value["native_source"]["sq8_sha256"]}},
            "low_f32_bits":vec![(-1.0_f32).to_bits();D],"step_f32_bits":vec![(1.0_f32/16.0).to_bits();D],
            "producer_authority":value["producer_authority"],
            "source_identity_qualification":"external_frozen_prerequisite_not_self_certified",
            "admission":{"wrapper_payload_bytes":1,"normalization_api_payload_bytes":1,
                "flat_fit_api_payload_upper_bound_bytes":1,"sq8_api_payload_bytes":1,
                "peak_payload_upper_bound_bytes":8*1024*1024,"aggregate_scratch_upper_bound_bytes":8_000_000_000_u64}});
        value["derivation_receipt"] = artifact(
            &dir.join("derivation.json"),
            &serde_json::to_vec(&derivation).unwrap(),
        );
    }
    pub(super) fn write_config(dir: &Path, value: &Value) -> (PathBuf, String) {
        let mut value = value.clone();
        fixture_receipts(dir, &mut value);
        let path = dir.join("config.json");
        let body = serde_json::to_vec(&value).unwrap();
        std::fs::write(&path, &body).unwrap();
        (path, hash(&body))
    }
    #[test]
    fn framed_source_runner_paired_parity() {
        let (_dir, paths) = source_layout_reducer_layout_fixture(
            513,
            2,
            true,
            false,
            true,
            borsuk::two_bit_generation::SourceLayoutMode::FramedRecordsV1,
        )
        .unwrap();
        let rows = paths.map(|p| {
            std::fs::read_to_string(p)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>()
        });
        let a = rows[0].iter().find(|r| r["phase"] == "query").unwrap();
        let b = rows[1].iter().find(|r| r["phase"] == "query").unwrap();
        assert_eq!(a["plan"], b["plan"]);
        assert_eq!(
            a["returned_physical_ordinals"],
            b["returned_physical_ordinals"]
        );
        let stats = &b["stages"]["source_frames"];
        assert_eq!(
            stats["physical_bytes"],
            b["charges"]["source"]["verified_bytes"]
        );
        assert_eq!(stats["decoded_bytes"], stats["hash_bytes"]);
        assert!(
            stats["decoded_bytes"].as_u64().unwrap() > stats["physical_bytes"].as_u64().unwrap()
        );
    }
    #[test]
    fn full_selected_requests_require_equal_reserved_digest() {
        let dir = tempfile::tempdir().unwrap();
        let mut value = config_value(dir.path(), Shape::tiny(32));
        let good: Config = serde_json::from_value(value.clone()).unwrap();
        validate_receipts(&good).unwrap();
        let mut cohort: Value =
            serde_json::from_slice(&std::fs::read(&good.cohort_receipt.path).unwrap()).unwrap();
        cohort["reserved_queries_sha256"] = json!("8".repeat(64));
        value["cohort_receipt"] = artifact(
            &good.cohort_receipt.path,
            &serde_json::to_vec(&cohort).unwrap(),
        );
        let bad: Config = serde_json::from_value(value).unwrap();
        assert_eq!(
            validate_receipts(&bad).unwrap_err().to_string(),
            "full selected request/reserved query digest binding"
        );
    }

    #[test]
    fn source_only_derivation_reuses_one_seal_across_query_prefixes_and_refuses_v1_or_wrong_authority()
     {
        let dir = tempfile::tempdir().unwrap();
        let mut full = config_value(
            dir.path(),
            Shape {
                rows: 100_000,
                count: 1000,
                returned_limit: K,
            },
        );
        let full_c: Config = serde_json::from_value(full.clone()).unwrap();
        validate_receipts(&full_c).unwrap();
        let producer = std::fs::read(&full_c.derivation_receipt.path).unwrap();
        let producer_pin = full["derivation_receipt"].clone();
        let full_cohort_pin = full["cohort_receipt"].clone();
        full["count"] = json!(32);
        full["requests"]["bytes"] = json!(32 * D * 4);
        full["truth"]["bytes"] = json!(32 * K * 8);
        fixture_receipts(dir.path(), &mut full);
        assert_ne!(full["cohort_receipt"], full_cohort_pin);
        assert_eq!(
            std::fs::read(&full_c.derivation_receipt.path).unwrap(),
            producer
        );
        assert_eq!(full["derivation_receipt"], producer_pin);
        let prefix: Config = serde_json::from_value(full.clone()).unwrap();
        validate_receipts(&prefix).unwrap();
        for fault in ["v1", "authority", "corpus", "calibration"] {
            let mut value = full.clone();
            let mut receipt: Value = serde_json::from_slice(&producer).unwrap();
            let expected = match fault {
                "v1" => {
                    receipt["schema"] = json!("borsuk-native-scale-derivation-receipt-v1");
                    "derivation receipt v2 required; v1 is incompatible"
                }
                "authority" => {
                    receipt["producer_authority"]["executable_sha256"] = json!("0".repeat(64));
                    "root-frozen derivation producer authority"
                }
                "corpus" => {
                    receipt["original_corpus"]["sha256"] = json!("0".repeat(64));
                    "authenticated source-only original/normalized/fit/SQ8 derivation"
                }
                "calibration" => {
                    receipt["step_f32_bits"][0] = json!(0);
                    "derivation exact f32 calibration bits"
                }
                _ => unreachable!(),
            };
            value["derivation_receipt"] = artifact(
                &prefix.derivation_receipt.path,
                &serde_json::to_vec(&receipt).unwrap(),
            );
            let bad: Config = serde_json::from_value(value).unwrap();
            assert_eq!(
                validate_receipts(&bad).unwrap_err().to_string(),
                expected,
                "{fault}"
            );
        }
    }

    #[test]
    fn explicit_scale_population_receipts_and_negative_geometry() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape {
            rows: 1_000_000,
            count: 1000,
            returned_limit: K,
        };
        let mut value = config_value(dir.path(), shape);
        value["query_source_first"] = json!(100_000);
        value["reserved_query_interval"] = json!({"start":100_000,"end":101_000});
        value["corpus_intervals"] =
            json!([{"start":0,"end":100_000},{"start":101_000,"end":1_001_000}]);
        value["profile"] = json!("scale1m");
        fixture_receipts(dir.path(), &mut value);
        let c: Config = serde_json::from_value(value.clone()).unwrap();
        validate_config(&c, shape).unwrap();
        validate_receipts(&c).unwrap();
        for count in [1, 32, 1000] {
            let shape = Shape { count, ..shape };
            let mut prefix = value.clone();
            prefix["count"] = json!(count);
            prefix["requests"]["bytes"] = json!(count * D * 4);
            prefix["truth"]["bytes"] = json!(count * K * 8);
            fixture_receipts(dir.path(), &mut prefix);
            let c: Config = serde_json::from_value(prefix).unwrap();
            validate_config(&c, shape).unwrap();
            validate_receipts(&c).unwrap();
        }
        fixture_receipts(dir.path(), &mut value);
        for (key, changed) in [
            ("profile", json!("native100k")),
            ("count", json!(1001)),
            (
                "reserved_query_interval",
                json!({"start":100_000,"end":100_032}),
            ),
            ("query_source_first", json!(usize::MAX)),
            ("rows", json!(usize::MAX)),
            ("corpus_intervals", json!([{"start":0,"end":1_000_000}])),
        ] {
            let mut bad = value.clone();
            bad[key] = changed;
            assert!(
                validate_config(&serde_json::from_value::<Config>(bad).unwrap(), shape).is_err(),
                "{key}"
            );
        }
        let mut bad: Config = serde_json::from_value(value).unwrap();
        bad.requests.sha256 = "0".repeat(64);
        assert!(validate_receipts(&bad).is_err());
    }

    #[test]
    fn sq8_budgets_are_required_and_bind_the_operating_point() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut value = config_value(dir.path(), shape);
        value["sq8_get_budget"] = json!(30);
        value["sq8_byte_budget"] = json!(33_554_432);
        let configured: Config = serde_json::from_value(value.clone()).unwrap();
        validate_config(&configured, shape).unwrap();
        let admitted = limits(&configured).unwrap();
        assert_eq!(admitted.max_query_gets, 30);
        assert_eq!(admitted.max_query_bytes, 33_554_432);
        assert_eq!(trace_ranges(&configured), 30);
        for field in ["sq8_get_budget", "sq8_byte_budget"] {
            let mut missing = value.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<Config>(missing).is_err());
            let mut zero = value.clone();
            zero[field] = json!(0);
            assert!(validate_config(&serde_json::from_value(zero).unwrap(), shape).is_err());
        }
        value["schema"] = json!("borsuk-cohere-native-baseline-config-v8");
        assert!(validate_config(&serde_json::from_value(value).unwrap(), shape).is_err());
    }

    #[test]
    fn source_get_budget96_is_explicit_and_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut value = config_value(dir.path(), shape);
        let original: Config = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(limits(&original).unwrap().max_source_gets, 128);
        for budget in [96, 128] {
            value["source_get_budget"] = json!(budget);
            let config: Config = serde_json::from_value(value.clone())
                .expect("explicit SOURCE get budget must be admitted");
            validate_config(&config, shape).unwrap();
            let admission = limits(&config).unwrap();
            assert_eq!(admission.max_source_gets, budget);
            assert_eq!(admission.max_parallel_source_gets, 16);
            assert_eq!(admission.max_parallel_gets, 16);
            assert_eq!(admission.max_query_gets, 32);
            assert_eq!(admission.max_source_bytes, 64 * 1024 * 1024);
            assert_eq!(admission.max_query_bytes, 16_773_120);
            assert_eq!(admission.max_memory_bytes, MEMORY_CAP);
            assert_eq!(admission.max_active_queries, 1);
            let (path, pin) = write_config(dir.path(), &value);
            let output = dir.path().join(format!("source-budget-{budget}.jsonl"));
            // Exercise the actual production header writer. This injected query
            // refusal is an INVALID fixture, never an ANN or performance result.
            assert!(
                !execute_paths_with(&path, &pin, &output, shape, |_, _, _, _| {
                    Err("header-only SOURCE budget fixture".into())
                })
                .unwrap()
            );
            let emitted = records(&output);
            assert_eq!(emitted[0]["schema"], RESULT_SCHEMA);
            assert_eq!(emitted[0]["config_sha256"], pin);
            assert_eq!(emitted[0]["source_get_budget"], budget);
            assert_eq!(emitted[1]["source_get_budget"], budget);
            assert_eq!(emitted[1]["source_cache"], "off");
        }
        for invalid in [
            json!(0),
            json!(95),
            json!(97),
            json!(129),
            json!(true),
            json!(null),
            json!("96"),
            json!(96.5),
        ] {
            value["source_get_budget"] = invalid;
            if let Ok(config) = serde_json::from_value::<Config>(value.clone()) {
                assert!(
                    validate_config(&config, shape).is_err(),
                    "unapproved SOURCE budget"
                );
            }
        }
    }

    #[test]
    fn fp32_source_cache_combined_memory_boundary_before_queries() {
        let (dir, result) = super::fp32_reducer_native_fixture_cached(false, Some(64), false, None, 384*1024*1024).unwrap();
        let rows = std::fs::read_to_string(result).unwrap().lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
        let startup = rows.iter().find(|r| r["phase"] == "startup").unwrap();
        let model = startup["fp32_rerank"]["modeled_memory_bytes"].as_u64().unwrap();
        let mut value: Value = serde_json::from_slice(&std::fs::read(dir.path().join("config.json")).unwrap()).unwrap();
        for cap in [model - 1, model] {
            value["max_memory_bytes"] = json!(cap);
            let (status, rows) = run_status(dir.path(), &format!("cache-cap-{cap}"), &value, Shape::tiny(33));
            if cap == model { assert_eq!(status, Status::Measured, "{rows:?}"); }
            else {
                assert_eq!(status, Status::Invalid, "{rows:?}");
                assert!(rows.iter().all(|r| r["phase"] != "query" && r["phase"] != "startup"));
                assert!(rows.last().unwrap()["summary"]["error"].as_str().unwrap().contains("FP32 total modeled memory cap"), "{rows:?}");
                assert_eq!(rows.last().unwrap()["summary"]["charges"]["source"]["submitted_gets"], 0);
                assert_eq!(rows.last().unwrap()["summary"]["truth_opened"], false);
            }
        }
    }

    #[test]
    fn coverage64_panel_model_minus_one_refuses_before_query_payload() {
        let shape = Shape::tiny(33);
        let (dir, mut value) = query_runtime(default_fetch_parallelism()).unwrap()
            .block_on(fixture_with_profile(shape, SemanticProfile::Scale1m));
        value["semantic_leaf_limit"] = json!(64);
        value["execution"] = json!({"mode":"diagnostic_panel","ordinals":[0],"trace":false});
        let (path, sha) = write_config(dir.path(), &value);
        let c = config(&path, &sha, shape).unwrap();
        let model = query_runtime(default_fetch_parallelism()).unwrap().block_on(async {
            let reader = Reader::new(&c.backend, None).unwrap();
            TwoBitGeneration::open_remote(reader.store(), &ObjectPath::from(c.generation_prefix.clone()),
                &c.generation_root_sha256, limits(&c).unwrap(), &c.scratch_parent).await.unwrap()
                .with_semantic_leaf_limit(64).unwrap().direct_closure_memory(DirectClosureLimits {
                    max_sq8_bytes: c.sq8_byte_budget, max_sq8_gets: c.sq8_get_budget,
                }).unwrap().resident_bytes
        });
        for cap in [model - 1, model] {
            value["max_memory_bytes"] = json!(cap);
            let (status, rows) = run_status(dir.path(), &format!("coverage-model-{cap}"), &value, shape);
            if cap == model { assert_eq!(status, Status::Measured, "{rows:?}"); }
            else {
                assert_eq!(status, Status::Invalid);
                assert!(rows.iter().all(|r| r["phase"] != "query"));
                let terminal = &rows.last().unwrap()["summary"];
                assert_eq!(terminal["completed_queries"], 0);
                assert_eq!(terminal["truth_opened"], false);
                assert_eq!(terminal["charges"]["source"]["submitted_gets"], 0);
                assert_eq!(terminal["charges"]["sq8"]["submitted_gets"], 0);
            }
        }
    }

    #[test]
    fn coverage64_production_writer_panel_full_failure_and_global_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let mut value = config_value(dir.path(), Shape::tiny(32));
        value["profile"] = json!("scale1m");
        value["execution"] = json!({"mode":"diagnostic_panel","ordinals":[0],"trace":false});
        let mut c: Config = serde_json::from_value(value).unwrap();
        let old_pins = diagnostic_pinned_bytes(&c).unwrap();
        assert_eq!(TwoBitPlanTrace::scratch_bytes(1_000_000), 246360);
        assert_eq!(TwoBitPlanTrace::scratch_bytes_with_leaf_limit(1_000_000, 64), Some(314752));
        let mut forecast = config_value(dir.path(), Shape::PRODUCTION);
        forecast["profile"] = json!("scale1m");
        forecast["rows"] = json!(1_000_000);
        forecast["semantic_leaf_limit"] = json!(48);
        let parsed = |v: &Value| serde_json::from_slice::<Config>(&serde_json::to_vec(v).unwrap()).unwrap();
        let control = parsed(&forecast);
        forecast["semantic_leaf_limit"] = json!(64);
        let full64 = parsed(&forecast);
        assert_eq!(caller_bytes(&full64) - caller_bytes(&control), 68392);
        for count in [1, 110] {
            forecast["execution"] = json!({"mode":"diagnostic_panel","ordinals":(0..count).collect::<Vec<_>>(),"trace":false});
            let panel = parsed(&forecast);
            let Execution::DiagnosticPanel { ordinals, .. } = &panel.execution else { unreachable!() };
            assert_eq!(ordinals.capacity(), count);
            assert_eq!(
                pinned_bytes(&panel).unwrap() - pinned_bytes(&full64).unwrap(),
                524288 + 8 * count as u64 + diagnostic_transient_bytes(&panel).unwrap() as u64
            );
        }
        let trace = TwoBitPlanTrace {
            ranked_candidate_pages: (0..3907).collect(),
            nomination_evaluated_units: (0..31250).collect(),
            primary_page: 3906,
            semantic_leaves: (31186..31250).collect(),
            semantic_units: (27154..31250).collect(),
            semantic_seed_additions: (0..7).collect(),
            ..Default::default()
        };
        // Every ID list is at its maximal admitted cardinality; 32KiB covers
        // the remaining production query fields without growing their budgets.
        let record = json!({"phase":"query","trace":trace,"envelope": "x".repeat(32768)});
        let bytes = serde_json::to_vec(&record).unwrap().len() + 1;
        assert!(bytes > PANEL_LINE_CAP && bytes < LINE_CAP, "{bytes}");
        for limit in [None, Some(40), Some(48), Some(64)] {
            c.semantic_leaf_limit = limit;
            let cap = panel_line_cap(&c);
            let mut out = Output::create(&dir.path().join(format!("panel-{limit:?}.jsonl"))).unwrap();
            out.admit_panel_lines(cap);
            assert_eq!(out.emit(&record).is_ok(), limit == Some(64));
            assert_eq!(out.bytes as usize, if limit == Some(64) { bytes } else { 0 });
            assert_eq!(diagnostic_pinned_bytes(&c).unwrap(),
                if limit == Some(64) { old_pins + 2 * (LINE_CAP - PANEL_LINE_CAP) } else { old_pins });
        }
        let mut out = Output::create(&dir.path().join("new-boundary.jsonl")).unwrap();
        out.admit_panel_lines(LINE_CAP);
        out.emit(&"x".repeat(LINE_CAP - 3)).unwrap(); // quotes + newline: exact cap
        let before = out.bytes;
        assert!(out.emit(&"x".repeat(LINE_CAP - 2)).is_err());
        assert_eq!(out.bytes, before, "+1 refusal publishes no partial line");
        let mut full = Output::create(&dir.path().join("full.jsonl")).unwrap();
        full.emit(&record).unwrap();
        assert_eq!(full.line_cap, LINE_CAP);
        assert_eq!(full.cap, OUTPUT_CAP - TERMINAL_RESERVE);
        // Global byte admission remains hard and independent of per-line admission.
        full.bytes = full.cap;
        assert!(full.emit(&json!({"phase":"query"})).is_err());
        let mut failed = Output::create(&dir.path().join("failed.jsonl")).unwrap();
        failed.admit_panel_lines(LINE_CAP);
        emit_failure(&mut failed, json!({"phase":"query_failure","error":"original"}),
            Some(json!({"oversized":"x".repeat(LINE_CAP)}))).unwrap();
        let failure: Value = serde_json::from_slice(&std::fs::read(dir.path().join("failed.jsonl")).unwrap()).unwrap();
        assert_eq!(failure["error"], "original");
        assert_eq!(failure["diagnostic"]["omitted"], "serialized_line_cap");
        assert_eq!(failure["diagnostic"]["cap_bytes"], LINE_CAP);
        // Trace-OFF 110-query screen fits the unchanged global output envelope.
        assert!(110 * LINE_CAP + 16 * LINE_CAP + SOURCE_BINDING_LINE_CAP < OUTPUT_CAP as usize - TERMINAL_RESERVE as usize);
        // Full1000 is not guaranteed by the total cap; overruns must remain invalid.
        assert!(1000 * LINE_CAP > OUTPUT_CAP as usize);
    }

    #[test]
    fn semantic_leaf_limit_config_strict_and_bound_before_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut value = config_value(dir.path(), shape);
        value["profile"] = json!("scale1m");
        let default: Config = serde_json::from_value(value.clone()).unwrap();
        validate_config(&default, shape).unwrap();
        assert_eq!(default.semantic_leaf_limit, None);
        let pinned = pinned_bytes(&default).unwrap();
        for limit in [None, Some(1), Some(40), Some(48), Some(64)] {
            let mut selected = value.clone();
            if let Some(limit) = limit {
                selected["semantic_leaf_limit"] = json!(limit);
            }
            let config: Config = serde_json::from_value(selected.clone()).unwrap();
            validate_config(&config, shape).unwrap();
            assert_eq!(config.semantic_leaf_limit, limit);
            if limit.is_none_or(|n| n <= 48) { assert_eq!(pinned_bytes(&config).unwrap(), pinned); }
            else { assert!(pinned_bytes(&config).unwrap() >= pinned); }
            let (path, sha) = write_config(dir.path(), &selected);
            let output = dir.path().join(format!("leaf-limit-{limit:?}.jsonl"));
            assert!(
                !execute_paths_with(&path, &sha, &output, shape, |_, _, _, _| {
                    Err("header-only semantic leaf limit fixture".into())
                })
                .unwrap()
            );
            let rows = records(&output);
            for row in &rows[..2] {
                assert_eq!(
                    row.get("semantic_leaf_limit"),
                    limit.map(|n| json!(n)).as_ref()
                );
            }
        }
        for invalid in [
            Value::Null,
            json!(true),
            json!("40"),
            json!(40.0),
            json!(-1),
            json!({}),
            json!([]),
            json!(u64::MAX.to_string()),
        ] {
            let mut bad = value.clone();
            bad["semantic_leaf_limit"] = invalid;
            assert!(serde_json::from_value::<Config>(bad).is_err());
        }
        let runtime = query_runtime(16).unwrap();
        for (profile, limit) in [
            ("scale1m", 0),
            ("scale1m", 65),
            ("scale1m", usize::MAX),
            ("native100k", 1),
            ("native100k", 40),
            ("fresh1m", 48),
        ] {
            let mut bad = value.clone();
            bad["profile"] = json!(profile);
            bad["semantic_leaf_limit"] = json!(limit);
            let mut config: Config = serde_json::from_value(bad).unwrap();
            // Missing receipts/requests/truth would fail if admission reached input I/O.
            config.cohort_receipt.path = dir.path().join("unopened-cohort");
            config.derivation_receipt.path = dir.path().join("unopened-derivation");
            config.requests.path = dir.path().join("unopened-requests");
            config.truth.path = dir.path().join("unopened-truth");
            let output = dir.path().join(format!("invalid-{profile}-{limit}.jsonl"));
            let mut out = Output::create(&output).unwrap();
            let mut progress = Progress::default();
            let error = runtime
                .block_on(query_and_seal_with(
                    &config,
                    shape,
                    &mut out,
                    &mut progress,
                    || panic!("invalid semantic leaf policy opened the object store"),
                ))
                .err()
                .unwrap();
            assert_eq!(
                error.to_string(),
                "semantic leaf limit requires Scale1m and integer 1..=64"
            );
            assert!(!progress.truth_opened);
            assert_eq!(progress.completed, 0);
            assert!(records(&output).is_empty());
        }
    }

    #[test]
    fn source_authentication_selector_is_strict() {
        use borsuk::sq8_s3_range::SourceAuthentication;
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut value = config_value(dir.path(), shape);
        let default: Config = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(default.source_authentication, SourceAuthentication::Inline);
        assert_eq!(source_authentication_bytes(&default).unwrap(), 0);
        assert!(
            SourceAuthentication::Offload2
                .additional_memory_bytes(usize::MAX, 128, 1)
                .is_none()
        );
        for (selector, expected) in [
            ("inline", SourceAuthentication::Inline),
            ("offload2", SourceAuthentication::Offload2),
        ] {
            value["source_authentication"] = json!(selector);
            value["fetch_parallelism"] = json!(64);
            let config: Config = serde_json::from_value(value.clone()).unwrap();
            validate_config(&config, shape).unwrap();
            assert_eq!(config.source_authentication, expected);
            assert_eq!(
                source_authentication_bytes(&config).unwrap() > 0,
                expected == SourceAuthentication::Offload2
            );
            assert_eq!(limits(&config).unwrap().max_parallel_source_gets, 64);
            assert_eq!(serde_json::to_value(expected).unwrap(), json!(selector));
        }
        for invalid in [
            json!(null),
            json!(true),
            json!(2),
            json!("offload"),
            json!("offload_2"),
            json!("INLINE"),
            json!({"workers":2}),
        ] {
            value["source_authentication"] = invalid;
            assert!(serde_json::from_value::<Config>(value.clone()).is_err());
        }
    }

    #[test]
    fn cold_fetch_parallelism64_preserves_caps_and_bounded_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut value = config_value(dir.path(), shape);
        value["fetch_parallelism"] = json!(64);
        let config: Config = serde_json::from_value(value).unwrap();
        assert!(validate_config(&config, shape).is_ok());
        assert!(query_runtime(config.fetch_parallelism).is_ok());
        let admission = limits(&config).unwrap();
        assert_eq!(admission.max_parallel_source_gets, 64);
        assert_eq!(admission.max_parallel_gets, 64);
        assert_eq!(admission.max_source_gets, 128);
        assert_eq!(admission.max_query_gets, 32);
        assert_eq!(admission.max_source_bytes, 64 * 1024 * 1024);
        assert_eq!(admission.max_query_bytes, 16_773_120);
        assert_eq!(admission.max_memory_bytes, MEMORY_CAP);
        assert_eq!(admission.max_active_queries, 1);
    }

    #[test]
    fn cold_fetch_parallelism128_preserves_caps_and_one_source_wave() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut value = config_value(dir.path(), shape);
        value["fetch_parallelism"] = json!(128);
        let config: Config = serde_json::from_value(value).unwrap();
        validate_config(&config, shape).unwrap();
        query_runtime(config.fetch_parallelism).unwrap();
        let admission = limits(&config).unwrap();
        assert_eq!(
            admission.max_parallel_source_gets,
            admission.max_source_gets
        );
        assert_eq!(admission.max_parallel_source_gets, 128);
        assert_eq!(admission.max_query_gets, 32);
        assert_eq!(admission.max_source_bytes, 64 * 1024 * 1024);
        assert_eq!(admission.max_query_bytes, 16_773_120);
        assert_eq!(admission.max_memory_bytes, MEMORY_CAP);
        assert_eq!(admission.max_active_queries, 1);
        value = config_value(dir.path(), shape);
        value["fetch_parallelism"] = json!(64);
        let original: Config = serde_json::from_value(value).unwrap();
        assert_eq!(
            pinned_bytes(&config).unwrap(),
            pinned_bytes(&original).unwrap()
        );
        assert_eq!(
            range_scheduler_bytes(&config).unwrap() - range_scheduler_bytes(&original).unwrap(),
            Sq8RangeTrace::pending_futures_bytes(128, 128)
                - Sq8RangeTrace::pending_futures_bytes(128, 64)
        );
    }

    #[test]
    fn cold_fetch_parallelism_selector_is_explicit_and_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut value = config_value(dir.path(), shape);
        let default: Config = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(default.fetch_parallelism, 16);
        for parallelism in [0, 1, 16, 17, 32, 33, 63, 64, 65, 127, 128, 129, usize::MAX] {
            value["fetch_parallelism"] = json!(parallelism);
            let config: Config = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(
                validate_config(&config, shape).is_ok(),
                matches!(parallelism, 16 | 32 | 64 | 128)
            );
            assert_eq!(
                query_runtime(config.fetch_parallelism).is_ok(),
                matches!(parallelism, 16 | 32 | 64 | 128)
            );
            if matches!(parallelism, 16 | 32 | 64 | 128) {
                let admission = limits(&config).unwrap();
                assert_eq!(admission.max_parallel_gets, parallelism);
                assert_eq!(admission.max_parallel_source_gets, parallelism);
                assert_eq!(admission.max_query_gets, 32);
                assert_eq!(admission.max_source_gets, 128);
                assert_eq!(admission.max_query_bytes, 16_773_120);
                assert_eq!(admission.max_source_bytes, 64 * 1024 * 1024);
                assert_eq!(admission.max_active_queries, 1);
                assert_eq!(admission.max_memory_bytes, MEMORY_CAP);
                let (path, sha) = write_config(dir.path(), &value);
                let output = dir.path().join(format!("fetch-{parallelism}.jsonl"));
                assert!(
                    !execute_paths_with(&path, &sha, &output, shape, |_, _, _, _| {
                        Err("selector-only fixture".into())
                    })
                    .unwrap()
                );
                let output = records(&output);
                assert_eq!(output[0]["phase"], "identity");
                assert_eq!(output[0]["fetch_parallelism"], parallelism);
                assert_eq!(output[1]["phase"], "bound_inputs");
                assert_eq!(output[1]["fetch_parallelism"], parallelism);
                assert_eq!(output[1]["source_cache"], "off");
            }
        }
    }
    #[test]
    fn s3_read_retry_requires_v13_and_binds_identity_without_widening_logical_caps() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut value = config_value(dir.path(), shape);
        let original: Config = serde_json::from_value(value.clone()).unwrap();
        validate_config(&original, shape).unwrap();
        assert_eq!(original.s3_read_retry, None);
        value["schema"] = json!(CONFIG_SCHEMA);
        value["s3_read_retry"] = json!("one_retry_100ms");
        let local: Config = serde_json::from_value(value.clone()).unwrap();
        assert!(validate_config(&local, shape).is_err());
        value["backend"] = json!({
            "kind":"s3","bucket":"fixture-bucket","region":"eu-central-1",
            "physical_prefix":"physical/ns",
            "sq8_object_key":format!("semantic/index/objects/{}", "e".repeat(64)),
            "sq8_etag":"\"frozen\""
        });
        let c: Config = serde_json::from_value(value.clone()).unwrap();
        validate_config(&c, shape).unwrap();
        assert_eq!(c.s3_read_retry, Some(S3ReadRetryPolicy::OneRetry100Ms));
        let selected = Reader::new(&c.backend, c.s3_read_retry).unwrap();
        assert!(matches!(selected, Reader::S3ReadRetry(_)));
        assert_eq!(
            limits(&c).unwrap().max_query_gets,
            limits(&original).unwrap().max_query_gets
        );
        assert_eq!(
            limits(&c).unwrap().max_query_bytes,
            limits(&original).unwrap().max_query_bytes
        );
        assert_eq!(
            limits(&c).unwrap().max_source_gets,
            limits(&original).unwrap().max_source_gets
        );
        let (path, sha) = write_config(dir.path(), &value);
        let output = dir.path().join("read-retry-identity.jsonl");
        assert!(
            !execute_paths_with(&path, &sha, &output, shape, |_, _, _, _| {
                Err("identity-only retry fixture".into())
            })
            .unwrap()
        );
        let output = records(&output);
        let policy = read_retry_identity(S3ReadRetryPolicy::OneRetry100Ms);
        assert_eq!(output[0]["schema"], RESULT_SCHEMA);
        assert_eq!(output[0]["config_schema"], CONFIG_SCHEMA);
        assert_eq!(output[1]["config_schema"], CONFIG_SCHEMA);
        for header in &output[..2] {
            assert_eq!(
                header["source_capsule_verification_policy"],
                SOURCE_CAPSULE_VERIFICATION_POLICY
            );
        }
        for key in [
            "source_capsule_verification_policy",
            "skip_source_capsule_verification",
        ] {
            let mut override_policy = value.clone();
            override_policy[key] = json!(false);
            assert!(
                serde_json::from_value::<Config>(override_policy).is_err(),
                "{key}"
            );
        }
        assert_eq!(output[0]["s3_read_retry"], policy);
        assert_eq!(output[1]["s3_read_retry"], policy);
        assert_eq!(output[0]["config_sha256"], sha);
        assert_eq!(
            policy["physical_submission_upper_bound_per_logical_read"],
            2
        );
        assert_eq!(policy["sdk_retry_window_is_hard_deadline"], false);
        for schema in [
            "borsuk-cohere-native-baseline-config-v11",
            "borsuk-cohere-native-baseline-config-v12",
        ] {
            let mut old = value.clone();
            old["schema"] = json!(schema);
            let c: Config = serde_json::from_value(old).unwrap();
            assert!(validate_config(&c, shape).is_err(), "{schema}");
        }
        let mut absent = value.clone();
        absent.as_object_mut().unwrap().remove("s3_read_retry");
        let c: Config = serde_json::from_value(absent).unwrap();
        validate_config(&c, shape).unwrap();
        value["s3_read_retry"] = json!("unbounded");
        assert!(serde_json::from_value::<Config>(value).is_err());
    }

    #[test]
    fn cold_sq8_split_config_version_caps_trace_and_identity() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut value = config_value(dir.path(), shape);
        let original: Config = serde_json::from_value(value.clone()).unwrap();
        let split = json!({"max_physical_range_bytes":64 * 32 * (D + 12), "max_physical_gets":46});
        value["sq8_range_split"] = split.clone();
        let c: Config = serde_json::from_value(value.clone()).unwrap();
        validate_config(&c, shape).unwrap();
        assert_eq!(trace_ranges(&c), 46);
        assert_eq!(
            range_scheduler_bytes(&c).unwrap(),
            Sq8RangeTrace::pending_futures_bytes(BASELINE_SOURCE_GETS, c.fetch_parallelism)
        );
        assert_eq!(
            limits(&c).unwrap().max_query_gets,
            limits(&original).unwrap().max_query_gets
        );
        assert_eq!(
            limits(&c).unwrap().max_query_bytes,
            limits(&original).unwrap().max_query_bytes
        );
        let (path, sha) = write_config(dir.path(), &value);
        let output = dir.path().join("sq8-split-identity.jsonl");
        assert!(
            !execute_paths_with(&path, &sha, &output, shape, |_, _, _, _| {
                Err("identity-only split fixture".into())
            })
            .unwrap()
        );
        let output = records(&output);
        assert_eq!(output[0]["sq8_range_split"], split);
        assert_eq!(output[1]["sq8_range_split"], split);
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove("sq8_range_split");
        assert!(serde_json::from_value::<Config>(missing).is_err());
        for changed in [
            json!({"max_physical_range_bytes":0,"max_physical_gets":46}),
            json!({"max_physical_range_bytes":1,"max_physical_gets":0}),
            json!({"max_physical_range_bytes":1,"max_physical_gets":129}),
            json!({"max_physical_range_bytes":MEMORY_CAP+1,"max_physical_gets":46}),
            json!({"max_physical_range_bytes":usize::MAX,"max_physical_gets":46}),
        ] {
            let mut bad = value.clone();
            bad["sq8_range_split"] = changed;
            let bad: Config = serde_json::from_value(bad).unwrap();
            assert!(validate_config(&bad, shape).is_err());
        }
        let mut bad = value.clone();
        bad["sq8_range_split"]["unrecorded"] = json!(true);
        assert!(serde_json::from_value::<Config>(bad).is_err());
        value["schema"] = json!("borsuk-cohere-native-baseline-config-v9");
        assert!(validate_config(&serde_json::from_value(value).unwrap(), shape).is_err());
    }

    #[test]
    fn cold_shared_byte_window_config_is_explicit_and_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut value = config_value(dir.path(), shape);
        value["fetch_parallelism"] = json!(64);
        let historical: Config = serde_json::from_value(value.clone()).unwrap();
        validate_config(&historical, shape).unwrap();
        let window = json!({"max_bytes":16 * 1024 * 1024,
            "max_waiters":64,"max_state_bytes":16384});
        value["shared_byte_window"] = window.clone();
        let candidate: Config = serde_json::from_value(value.clone())
            .expect("missing explicit shared byte-window benchmark configuration");
        validate_config(&candidate, shape).unwrap();
        let original = limits(&historical).unwrap();
        let admitted = limits(&candidate).unwrap();
        assert_eq!(admitted.max_source_bytes, original.max_source_bytes);
        assert_eq!(admitted.max_source_gets, original.max_source_gets);
        assert_eq!(admitted.max_query_bytes, original.max_query_bytes);
        assert_eq!(admitted.max_query_gets, original.max_query_gets);
        assert_eq!(admitted.max_active_queries, original.max_active_queries);
        let (path, sha) = write_config(dir.path(), &value);
        let output = dir.path().join("byte-window-identity.jsonl");
        assert!(
            !execute_paths_with(&path, &sha, &output, shape, |_, _, _, _| {
                Err("identity-only byte-window fixture".into())
            })
            .unwrap()
        );
        let output = records(&output);
        assert_eq!(output[0]["shared_byte_window"], window);
        assert_eq!(output[1]["shared_byte_window"], window);
        for (field, changed) in [
            ("max_bytes", json!(0)),
            ("max_bytes", json!(MEMORY_CAP + 1)),
            ("max_waiters", json!(0)),
            ("max_waiters", json!(63)),
            ("max_waiters", json!(usize::MAX)),
            ("max_state_bytes", json!(0)),
            ("max_state_bytes", json!(1)),
            ("max_state_bytes", json!(MEMORY_CAP + 1)),
        ] {
            let mut bad = value.clone();
            bad["shared_byte_window"][field] = changed;
            let bad: Config = serde_json::from_value(bad).unwrap();
            assert!(validate_config(&bad, shape).is_err(), "{field}");
        }
        let mut bad = value;
        bad["shared_byte_window"]["unrecorded"] = json!(true);
        assert!(serde_json::from_value::<Config>(bad).is_err());
    }

    #[test]
    fn query_runtime_bounds_blocking_threads_and_drains() {
        use std::{
            sync::atomic::{AtomicBool, AtomicUsize, Ordering},
            time::Duration,
        };

        #[derive(Default)]
        struct Jobs {
            active: AtomicUsize,
            peak: AtomicUsize,
            completed: AtomicUsize,
            release: AtomicBool,
        }

        struct RuntimeCleanup {
            runtime: Option<tokio::runtime::Runtime>,
            jobs: Arc<Jobs>,
        }
        impl Drop for RuntimeCleanup {
            fn drop(&mut self) {
                self.jobs.release.store(true, Ordering::SeqCst);
                if let Some(runtime) = self.runtime.take() {
                    runtime.shutdown_timeout(Duration::from_secs(1));
                }
            }
        }

        for parallelism in [16, 32, 64] {
            let jobs = Arc::new(Jobs::default());
            let cleanup = RuntimeCleanup {
                runtime: Some(query_runtime(parallelism).unwrap()),
                jobs: jobs.clone(),
            };
            let runtime = cleanup.runtime.as_ref().unwrap();
            let mut handles = Vec::new();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            for _ in 0..128 {
                let jobs = jobs.clone();
                handles.push(runtime.spawn_blocking(move || {
                    let active = jobs.active.fetch_add(1, Ordering::SeqCst) + 1;
                    jobs.peak.fetch_max(active, Ordering::SeqCst);
                    while !jobs.release.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    jobs.active.fetch_sub(1, Ordering::SeqCst);
                    jobs.completed.fetch_add(1, Ordering::SeqCst);
                }));
            }
            let saturated = runtime.block_on(async {
                tokio::time::timeout_at(deadline, async {
                    while jobs.active.load(Ordering::SeqCst) < parallelism {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    // Give excess workers time to expose an uncapped pool before release.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                })
                .await
                .is_ok()
            });
            // Cleanup gets a separate finite grace after releasing every blocked job.
            jobs.release.store(true, Ordering::SeqCst);
            let (drained_in_time, joined) = runtime.block_on(async {
                let drain = async {
                    let mut joined = true;
                    for handle in handles {
                        joined &= handle.await.is_ok();
                    }
                    joined
                };
                match tokio::time::timeout(Duration::from_secs(5), drain).await {
                    Ok(joined) => (true, joined),
                    Err(_) => (false, false),
                }
            });
            // Also bounds shutdown on a failed drain or an earlier panic.
            drop(cleanup);
            assert!(
                saturated && drained_in_time,
                "deadline at cap {parallelism}"
            );
            assert!(joined, "blocking job failed at cap {parallelism}");
            assert_eq!(jobs.completed.load(Ordering::SeqCst), 128);
            assert_eq!(jobs.active.load(Ordering::SeqCst), 0);
            let peak = jobs.peak.load(Ordering::SeqCst);
            assert!(peak <= parallelism, "peak {peak} exceeds cap {parallelism}");
            assert_eq!(peak, parallelism, "pool did not saturate");
        }
    }
    fn records(path: &Path) -> Vec<Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter(|s| !s.is_empty())
            .map(|s| serde_json::from_str(s).unwrap())
            .collect()
    }
    const TRUTH_IDS: [[u64; K]; 2] = [
        [20, 0, 1, 2, 3, 4, 5, 6, 7, 8],
        [21, 16, 17, 18, 19, 22, 23, 24, 25, 8],
    ];
    fn fixture_sq8_cosine_bits(record: &[u8], axis: usize) -> u32 {
        // Independently decode the dyadic fixture and normalize its full stored vector.
        let mut norm2 = 0.0_f64;
        for &code in &record[12..] {
            let value = -1.0 + f64::from(code) / 16.0;
            norm2 += value * value;
        }
        assert_eq!(
            norm2 as f32,
            f32::from_le_bytes(record[8..12].try_into().unwrap())
        );
        let coordinate = -1.0 + f64::from(record[12 + axis]) / 16.0;
        ((1.0 - coordinate / norm2.sqrt()) as f32).to_bits()
    }
    fn fixture_direction(id: usize) -> [f32; D] {
        // Independently specified dyadic unit directions: 9+7=16 or 4*4=16.
        // Different residual coordinates make all 32 (or 257) directions distinct.
        let mut v = [0.; D];
        match id {
            0..=7 => {
                v[0] = 0.75;
                v[2 + 7 * id..9 + 7 * id].fill(0.25);
            }
            8..=15 => {
                v[0] = 0.5;
                v[1] = 0.5;
                v[128 + 2 * (id - 8)..130 + 2 * (id - 8)].fill(0.5);
            }
            16..=19 | 22..=25 => {
                v[1] = 0.75;
                let slot = if id <= 19 { id - 16 } else { id - 18 };
                v[64 + 7 * slot..71 + 7 * slot].fill(0.25);
            }
            20 => v[0] = 1.,
            21 => v[1] = 1.,
            26 => v[D - 1] = 1.,
            // Larger layout fixtures can reuse the query-orthogonal tail directions.
            _ => v[200 + id % (D - 200)] = 1.,
        }
        v
    }
    pub(super) async fn fixture(shape: Shape) -> (tempfile::TempDir, Value) {
        fixture_with_profile(shape, SemanticProfile::Native100k).await
    }
    pub(super) async fn fixture_with_profile(
        shape: Shape,
        profile: SemanticProfile,
    ) -> (tempfile::TempDir, Value) {
        assert!(shape.rows >= 32);
        let dir = tempfile::tempdir().unwrap();
        let mut c = config_value(dir.path(), shape);
        c["profile"] = json!(profile);
        let store_root = dir.path().join("store");
        std::fs::create_dir(&store_root).unwrap();
        std::fs::create_dir(dir.path().join("scratch")).unwrap();
        let direct = LocalFileSystem::new_with_prefix(&store_root).unwrap();
        // Raw rows are in logical order, with distinct exact norms id+2.
        let mut raw = Vec::new();
        let mut sq8 = Vec::new();
        for id in 0..shape.rows {
            for value in fixture_direction(id) {
                raw.extend_from_slice(&(value * (id + 2) as f32).to_le_bytes());
            }
        }
        let mut order = (0..shape.rows as u64).rev().collect::<Vec<_>>();
        let eight = order.iter().position(|&id| id == 8).unwrap();
        order.swap(eight, shape.rows - 1); // k10 boundary winner is the partial-page tail.
        for &id in &order {
            let id = id as usize;
            // Exact SQ8 norms vary, including both leading hits: using a constant norm
            // or skipping the nonzero offsets changes the independent score bits.
            let scale = if id == 20 || id == 21 {
                1.25_f32
            } else if id >= 26 {
                0.5
            } else {
                1.
            };
            sq8.extend_from_slice(&(id as i64).to_le_bytes());
            sq8.extend_from_slice(&(scale * scale).to_le_bytes());
            for value in fixture_direction(id) {
                sq8.push(((value * scale + 1.) * 16.) as u8);
            }
        }
        let raw_path = dir.path().join("raw");
        let sq8_path = dir.path().join("sq8");
        std::fs::write(&raw_path, &raw).unwrap();
        std::fs::write(&sq8_path, &sq8).unwrap();
        let raw_sha = hash(&raw);
        let sq8_sha = hash(&sq8);
        let key = ObjectPath::from(format!("semantic/objects/{sq8_sha}"));
        direct.put(&key, sq8.into()).await.unwrap();
        let etag = direct.head(&key).await.unwrap().e_tag.unwrap();
        let root = dir.path().join("generation");
        let root_sha = TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &raw_sha,
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows: shape.rows,
                dimensions: D,
            },
            base_epoch: 0,
            generation: 1,
            low: &[-1.; D],
            step: &[1. / 16.; D],
            sq8_object_key: key.as_ref(),
            sq8_etag: &etag,
        }
        .build_with_semantic_profile(Some(&order), profile, &root, 128_000_000)
        .unwrap();
        let native: SourcePlaneReceipt =
            serde_json::from_slice(&std::fs::read(root.join("plane/manifest.json")).unwrap())
                .unwrap();
        c["generation_root_sha256"] = json!(root_sha);
        c["native_source"] = json!({"source_sha256":raw_sha,"sq8_sha256":sq8_sha,
            "source_order_sha256":native.source_order_sha256});
        // Row i is a positive multiple of axis i % 2 (3 and 5 for the first two rows), so cosine
        // truth alternates between the two independent TRUTH_IDS rows however many rows exist.
        let mut requests = Vec::new();
        for i in 0..shape.count {
            for d in 0..D {
                let scale = (3 + 2 * i) as f32;
                requests.extend_from_slice(&(if d == i % 2 { scale } else { 0. }).to_le_bytes());
            }
        }
        c["requests"] = artifact(&dir.path().join("requests.f32"), &requests);
        // Cosine ignores raw norms: axis hit, eight .75 hits, then ID8 wins the
        // .5 tie over ID9 at the k10 boundary, independently for each query.
        let truth = (0..shape.count)
            .flat_map(|i| TRUTH_IDS[i % 2])
            .flat_map(u64::to_le_bytes)
            .collect::<Vec<_>>();
        c["truth"] = artifact(&dir.path().join("truth.u64"), &truth);
        let parsed: Config = serde_json::from_value(c.clone()).unwrap();
        let head = publish_two_bit_generation(
            &direct,
            &ObjectPath::from("semantic/index"),
            &root,
            &root_sha,
            limits(&parsed).unwrap(),
            None,
        )
        .await
        .unwrap();
        c["generation_prefix"] = json!(head.metadata_prefix().to_string());
        fixture_receipts(dir.path(), &mut c);
        (dir, c)
    }
    #[tokio::test]
    async fn actual_d1024_derivation_receipt_outputs_bind_generation_and_refuse_one_bit_calibration_mismatch()
     {
        let shape = Shape::tiny(32);
        let dir = tempfile::tempdir().unwrap();
        let original = dir.path().join("actual-original.f32");
        let mut corpus = Vec::new();
        for row in 0..shape.rows {
            for coordinate in 0..D {
                let v = if coordinate == row % 4 {
                    (row + 2) as f32
                } else {
                    0.0
                };
                corpus.extend_from_slice(&v.to_le_bytes());
            }
        }
        std::fs::write(&original, &corpus).unwrap();
        // Freeze actual executable/embedded partial-source identity before run.
        // The producer's fixture source_commit remains explicitly declared;
        // complete binary/build provenance is a separate root gate.
        let producer_config =
            scale_derivation_producer::derive::fixture_config(&original, shape.rows, D).unwrap();
        let config_bytes = serde_json::to_vec(&producer_config).unwrap();
        let config_path = dir.path().join("actual-producer-config.json");
        std::fs::write(&config_path, &config_bytes).unwrap();
        let derived = dir.path().join("actual-derived");
        scale_derivation_producer::derive::run(&config_path, &hash(&config_bytes), &derived)
            .unwrap();
        let receipt_path = derived.join("derivation.json");
        let receipt_bytes = std::fs::read(&receipt_path).unwrap();
        let producer: Value = serde_json::from_slice(&receipt_bytes).unwrap();
        let normalized = derived.join("normalized.f32");
        let sq8 = derived.join("sq8.bin");
        let order_bytes = std::fs::read(derived.join("order.u64")).unwrap();
        let order = order_bytes
            .chunks_exact(8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        let coefficients = |key: &str| {
            producer[key]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| f32::from_bits(u32::try_from(v.as_u64().unwrap()).unwrap()))
                .collect::<Vec<_>>()
        };
        let low = coefficients("low_f32_bits");
        let step = coefficients("step_f32_bits");
        assert_eq!((low.len(), step.len()), (D, D));
        let raw_sha = producer["outputs"]["normalized"]["sha256"]
            .as_str()
            .unwrap();
        let sq8_sha = producer["outputs"]["sq8"]["sha256"].as_str().unwrap();
        let store_root = dir.path().join("store");
        std::fs::create_dir(&store_root).unwrap();
        std::fs::create_dir(dir.path().join("scratch")).unwrap();
        let store = LocalFileSystem::new_with_prefix(&store_root).unwrap();
        let sq8_key = ObjectPath::from(format!("semantic/objects/{sq8_sha}"));
        store
            .put(&sq8_key, std::fs::read(&sq8).unwrap().into())
            .await
            .unwrap();
        let etag = store.head(&sq8_key).await.unwrap().e_tag.unwrap();
        let mut value = config_value(dir.path(), shape);
        value["native_source"] = json!({"source_sha256":raw_sha,"sq8_sha256":sq8_sha,
            "source_order_sha256":producer["outputs"]["source_order"]["sha256"]});
        value["producer_authority"] = producer["producer_authority"].clone();
        // Do not rewrite or fabricate any field in the produced receipt.
        value["derivation_receipt"] =
            json!({"path":receipt_path,"bytes":receipt_bytes.len(),"sha256":hash(&receipt_bytes)});
        let requests = (0..shape.count)
            .flat_map(|q| (0..D).map(move |d| if d == q { (3 + q * 2) as f32 } else { 0.0 }))
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<_>>();
        value["requests"] = artifact(&dir.path().join("actual-requests.f32"), &requests);
        let exact_ids = [
            [0_u64, 4, 8, 12, 16, 20, 24, 28, 1, 2],
            [1, 5, 9, 13, 17, 21, 25, 29, 0, 2],
        ];
        let truth = exact_ids
            .into_iter()
            .flatten()
            .flat_map(u64::to_le_bytes)
            .collect::<Vec<_>>();
        value["truth"] = artifact(&dir.path().join("actual-truth.u64"), &truth);
        let cohort = json!({"schema":"borsuk-cohere-native-cohort-receipt-v3","status":"COMPLETE",
            "dataset":DATASET,"revision":REVISION,"reserved_queries_sha256":value["requests"]["sha256"],
            "geometry":{"reserved_query_interval":value["reserved_query_interval"],"corpus_rows":shape.rows,"query_rows":shape.count,
                "dimensions":D,"k":K,"corpus_intervals":value["corpus_intervals"],"query_source_ordinals":[shape.rows,shape.rows+shape.count]},
            "outputs":[{"name":"corpus.f32","bytes":corpus.len(),"sha256":hash(&corpus)},
                {"name":"queries.f32","bytes":requests.len(),"sha256":hash(&requests)},
                {"name":"truth.u64","bytes":truth.len(),"sha256":hash(&truth)}]});
        value["cohort_receipt"] = artifact(
            &dir.path().join("actual-cohort.json"),
            &serde_json::to_vec(&cohort).unwrap(),
        );
        for changed in [false, true] {
            let mut generation_step = step.clone();
            if changed {
                generation_step[0] = f32::from_bits(step[0].to_bits() ^ 1);
                assert!(generation_step[0].is_finite() && generation_step[0] > 0.0);
                assert_eq!(generation_step[0].to_bits() ^ step[0].to_bits(), 1);
            }
            let root = dir.path().join(if changed {
                "one-bit-generation"
            } else {
                "actual-generation"
            });
            let root_sha = TwoBitGenerationBuilder {
                source: TwoBitSource {
                    raw: &normalized,
                    raw_sha256: raw_sha,
                    sq8: &sq8,
                    sq8_sha256: sq8_sha,
                    rows: shape.rows,
                    dimensions: D,
                },
                base_epoch: 0,
                generation: 1,
                low: &low,
                step: &generation_step,
                sq8_object_key: sq8_key.as_ref(),
                sq8_etag: &etag,
            }
            .build_with_semantic_profile(
                Some(&order),
                SemanticProfile::Native100k,
                &root,
                128_000_000,
            )
            .unwrap();
            value["generation_root_sha256"] = json!(root_sha);
            let before_publish: Config = serde_json::from_value(value.clone()).unwrap();
            let head = publish_two_bit_generation(
                &store,
                &ObjectPath::from(if changed {
                    "semantic/wrong"
                } else {
                    "semantic/right"
                }),
                &root,
                &root_sha,
                limits(&before_publish).unwrap(),
                None,
            )
            .await
            .unwrap();
            value["generation_prefix"] = json!(head.metadata_prefix().to_string());
            let c: Config = serde_json::from_value(value.clone()).unwrap();
            validate_config(&c, shape).unwrap();
            let receipt = validate_receipts(&c).unwrap();
            if changed {
                let output = dir.path().join("actual-one-bit-refusal.jsonl");
                let mut out = Output::create(&output).unwrap();
                let mut p = Progress::default();
                let error = query_and_seal(&c, shape, &mut out, &mut p)
                    .await
                    .err()
                    .unwrap()
                    .to_string();
                assert_eq!(error, "generation/producer exact calibration binding");
                assert_eq!(p.completed, 0);
                assert!(!p.sealed && !p.truth_opened);
                assert!(!records(&output).iter().any(|r| matches!(
                    r["phase"].as_str(),
                    Some("query" | "all_queries_sealed" | "recall")
                )));
            } else {
                let mut charge = Charge::default();
                bind_source(&c, &receipt, &store, &mut charge)
                    .await
                    .unwrap();
                TwoBitGeneration::open_remote(
                    &store,
                    &ObjectPath::from(c.generation_prefix.clone()),
                    &c.generation_root_sha256,
                    limits(&c).unwrap(),
                    &c.scratch_parent,
                )
                .await
                .unwrap();
            }
            assert_eq!(std::fs::read(&receipt_path).unwrap(), receipt_bytes);
            for (name, seal) in [
                ("normalized.f32", "normalized"),
                ("order.u64", "source_order"),
                ("sq8.bin", "sq8"),
            ] {
                let bytes = std::fs::read(derived.join(name)).unwrap();
                assert_eq!(
                    hash(&bytes),
                    producer["outputs"][seal]["sha256"].as_str().unwrap()
                );
            }
        }
    }
    #[test]
    fn generation_calibration_mismatch_refuses_before_query_seal_or_truth() {
        let shape = Shape::tiny(32);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (dir, mut value) = runtime.block_on(fixture(shape));
        let good: Config = serde_json::from_value(value.clone()).unwrap();
        let mut receipt: Value =
            serde_json::from_slice(&std::fs::read(&good.derivation_receipt.path).unwrap()).unwrap();
        // Both values are finite and positive. The producer receipt and frozen
        // config pin stay coherent; only the generation disagrees with them.
        receipt["step_f32_bits"][0] = json!((1.0_f32 / 8.0).to_bits());
        value["derivation_receipt"] = artifact(
            &good.derivation_receipt.path,
            &serde_json::to_vec(&receipt).unwrap(),
        );
        let c: Config = serde_json::from_value(value).unwrap();
        validate_config(&c, shape).unwrap();
        validate_receipts(&c).unwrap();
        let output = dir.path().join("wrong-calibration.jsonl");
        let mut out = Output::create(&output).unwrap();
        let mut progress = Progress::default();
        let error = configured_query_runtime(&c)
            .unwrap()
            .block_on(query_and_seal(&c, shape, &mut out, &mut progress))
            .err()
            .unwrap()
            .to_string();
        assert_eq!(error, "generation/producer exact calibration binding");
        assert_eq!(progress.completed, 0);
        assert!(!progress.sealed && !progress.truth_opened);
        let body = std::fs::read_to_string(output).unwrap();
        assert!(!body.contains("all_queries_sealed") && !body.contains("\"phase\":\"query\""));
        assert!(body.contains("\"success\":false"));
    }

    #[test]
    fn page_range_v2_config_schema_layout_and_compiled_policy() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut good = config_value(dir.path(), shape);
        good["source_layout"] = json!({"layout":"leaf_capsules_v2","blob_bytes":8484,
            "blob_sha256":"d".repeat(64),"directory_bytes":100,"directory_sha256":"e".repeat(64)});
        let c: Config = serde_json::from_value(good.clone()).unwrap();
        validate_config(&c, shape).unwrap();
        assert_eq!(CONFIG_SCHEMA, "borsuk-cohere-native-baseline-config-v19");
        assert_eq!(RESULT_SCHEMA, "borsuk-cohere-native-baseline-result-v21");
        assert_eq!(
            SOURCE_CAPSULE_VERIFICATION_POLICY,
            "directory_page_ranges_original_units_v2"
        );
        for directory in [0, 96, 99, 101, 104, u64::MAX] {
            let mut bad = good.clone();
            bad["source_layout"]["directory_bytes"] = json!(directory);
            let (path, sha) = write_config(dir.path(), &bad);
            assert!(config(&path, &sha, shape).is_err(), "{directory}");
        }
        for (pointer, value) in [
            ("/schema", json!("borsuk-cohere-native-baseline-config-v13")),
            ("/source_layout/layout", json!("leaf_capsules")),
        ] {
            let mut bad = good.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            let (path, sha) = write_config(dir.path(), &bad);
            assert!(config(&path, &sha, shape).is_err(), "{pointer}");
        }
        for key in ["source_capsule_verification_policy", "skip_verification"] {
            let mut bad = good.clone();
            bad[key] = json!(SOURCE_CAPSULE_VERIFICATION_POLICY);
            assert!(serde_json::from_value::<Config>(bad).is_err(), "{key}");
        }
    }

    #[test]
    fn source_layout_and_explicit_byte_memory_contract() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut good = config_value(dir.path(), shape);
        good["max_memory_bytes"] = json!(MEMORY_CAP + 1);
        good["source_byte_budget"] = json!(67_108_865);
        let c: Config = serde_json::from_value(good.clone()).unwrap();
        validate_config(&c, shape).unwrap();
        let admitted = limits(&c).unwrap();
        assert_eq!(admitted.max_memory_bytes, MEMORY_CAP + 1);
        assert_eq!(admitted.max_source_bytes, 67_108_865);
        for key in ["source_layout", "source_byte_budget"] {
            let mut bad = good.clone();
            bad.as_object_mut().unwrap().remove(key);
            assert!(serde_json::from_value::<Config>(bad).is_err(), "{key}");
        }
        for (key, value) in [
            ("source_byte_budget", json!(0)),
            ("source_byte_budget", json!(-1)),
            ("max_memory_bytes", json!(0)),
            ("max_memory_bytes", json!(1)),
            ("max_memory_bytes", json!(-1)),
            ("source_layout", json!({"layout":"unknown"})),
            ("source_layout", json!({"layout":"flat_records","extra":1})),
            (
                "source_layout",
                json!({"layout":"leaf_capsules_v2","blob_bytes":100,"blob_sha256":"bad",
                "directory_bytes":100,"directory_sha256":"e".repeat(64)}),
            ),
        ] {
            let mut bad = good.clone();
            bad[key] = value;
            let (path, sha) = write_config(dir.path(), &bad);
            assert!(config(&path, &sha, shape).is_err(), "{key}");
        }
    }

    #[test]
    fn flat_source_layout_extra_fields_are_refused_at_both_decode_boundaries() {
        let good = br#"{"source_layout":{"layout":"flat_records"}}"#;
        exact_source_layout(good, &SourceLayout::FlatRecords).unwrap();
        for bad in [
            br#"{"source_layout":{"layout":"flat_records","blob_bytes":1}}"#.as_slice(),
            br#"{"source_layout":{"layout":"flat_records"},"source_layout":{"layout":"flat_records"}}"#.as_slice(),
        ] {
            assert!(exact_source_layout(bad, &SourceLayout::FlatRecords).is_err());
        }
    }

    #[tokio::test]
    async fn configured_layout_must_match_authenticated_root_before_query_or_truth() {
        let shape = Shape::tiny(32);
        let (dir, mut value) = fixture(shape).await;
        value["source_layout"] = json!({"layout":"leaf_capsules_v2","blob_bytes":100,
            "blob_sha256":"d".repeat(64),"directory_bytes":100,"directory_sha256":"e".repeat(64)});
        let c: Config = serde_json::from_value(value).unwrap();
        validate_config(&c, shape).unwrap();
        let mut out = Output::create(&dir.path().join("bad-layout.jsonl")).unwrap();
        let mut p = Progress::default();
        let error = query_and_seal(&c, shape, &mut out, &mut p)
            .await
            .err()
            .unwrap();
        assert_eq!(
            error.to_string(),
            "authenticated root/config SOURCE layout agreement"
        );
        assert!(!p.sealed && !p.truth_opened && p.completed == 0);
    }

    #[test]
    fn production_contract_rejects_unknown_hash_geometry_and_caps() {
        let dir = tempfile::tempdir().unwrap();
        let good = config_value(dir.path(), Shape::PRODUCTION);
        let (path, sha) = write_config(dir.path(), &good);
        config(&path, &sha, Shape::PRODUCTION).unwrap();
        assert!(config(&path, &"0".repeat(64), Shape::PRODUCTION).is_err());
        for (field, value) in [
            ("unknown", json!(1)),
            ("dimensions", json!(768)),
            ("rows", json!(99999)),
            ("count", json!(999)),
            ("k", json!(100)),
            ("profile", json!("fresh1m")),
            ("max_memory_bytes", json!(0)),
            ("query_source_first", json!(0)),
            ("generation_prefix", json!("../index")),
            ("metric", json!("l2")),
        ] {
            let mut invalid = good.clone();
            invalid[field] = value;
            let (path, sha) = write_config(dir.path(), &invalid);
            assert!(config(&path, &sha, Shape::PRODUCTION).is_err(), "{field}");
        }
        for field in ["requests", "truth", "native_source"] {
            let mut invalid = good.clone();
            invalid[field]["unknown"] = json!(0);
            assert!(serde_json::from_value::<Config>(invalid).is_err());
        }
        for (field, value) in [("bucket", json!("unexpected")), ("unknown", json!(0))] {
            let mut invalid = good.clone();
            invalid["backend"][field] = value;
            assert!(serde_json::from_value::<Config>(invalid).is_err());
        }
        let mut old = good.clone();
        old.as_object_mut().unwrap().remove("backend");
        old["store_root"] = json!(dir.path().join("store"));
        old["schema"] = json!("borsuk-cohere-native-baseline-config-v1");
        assert!(serde_json::from_value::<Config>(old).is_err());
        let mut invalid = good;
        invalid["requests"]["bytes"] = json!(4_095_999);
        let (path, sha) = write_config(dir.path(), &invalid);
        assert!(config(&path, &sha, Shape::PRODUCTION).is_err());
    }
    #[test]
    fn secure_artifacts_reject_hash_length_eof_symlink_fifo_and_bad_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input");
        let a: Artifact = serde_json::from_value(artifact(&path, b"abc")).unwrap();
        checked(&a, 3).unwrap();
        assert!(checked(&a, 2).is_err());
        for body in [&b"abd"[..], &b"ab"[..], &b"abcd"[..]] {
            std::fs::write(&path, body).unwrap();
            assert!(checked(&a, 3).is_err());
        }
        std::fs::write(&path, b"abc").unwrap();
        let (_, bound) = checked(&a, 3).unwrap();
        std::fs::write(&path, b"abd").unwrap();
        assert!(reauthenticate(&a, &bound).is_err());
        std::fs::remove_file(&path).unwrap();
        let target = dir.path().join("target");
        std::fs::write(&target, b"abc").unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        assert!(checked(&a, 3).is_err());
        assert!(config(&path, &a.sha256, Shape::PRODUCTION).is_err());
        std::fs::remove_file(&path).unwrap();
        rustix::fs::mknodat(
            rustix::fs::CWD,
            &path,
            rustix::fs::FileType::Fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
            0,
        )
        .unwrap();
        assert!(checked(&a, 3).is_err());
        assert!(config(&path, &a.sha256, Shape::PRODUCTION).is_err());
        for value in [0_f32, f32::INFINITY, f32::NAN] {
            let body = std::iter::repeat_n(value, D)
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>();
            assert!(request_row(&body, 0).is_err());
        }
        assert!(request_row(&[0; D * 4 - 1], 0).is_err());
        // Replace the metadata name deterministically after its guarded open.
        // A store.get here would reopen the FIFO and block with no writer.
        let c: Config = serde_json::from_value(config_value(dir.path(), Shape::tiny(32))).unwrap();
        let root = dir.path().join("store").join(&c.generation_prefix);
        std::fs::create_dir_all(&root).unwrap();
        let metadata_path = root.join("manifest.json");
        let body = br#"{"guarded":true,"source_layout":{"layout":"flat_records"}}"#;
        std::fs::write(&metadata_path, body).unwrap();
        LOCAL_METADATA_OPEN_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(|path| {
                std::fs::rename(path, path.with_extension("retained")).unwrap();
                rustix::fs::mknodat(
                    rustix::fs::CWD,
                    path,
                    rustix::fs::FileType::Fifo,
                    rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
                    0,
                )
                .unwrap();
            }));
        });
        let store = ChunkedStore::new(
            Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("store")).unwrap()),
            8192,
        );
        let mut charge = Charge::default();
        let value: Vec<u8> = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(metadata(
                &c,
                &store,
                "manifest.json",
                &hash(body),
                &mut charge,
            ))
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&value).unwrap(),
            json!({"guarded":true,"source_layout":{"layout":"flat_records"}})
        );
        assert_eq!(charge.submitted_gets, 0); // Direct Local FD reads are not store GETs.
        LOCAL_METADATA_OPEN_HOOK.with(|hook| assert!(hook.borrow().is_none()));
        assert!(regular(&metadata_path, None, CONFIG_CAP).is_err());
    }
    #[test]
    fn scratch_formula_keeps_codec_and_trace_separate() {
        use borsuk::rotated_two_bit::{RotatedTwoBitCodec, TwoBitError};
        let codec = RotatedTwoBitCodec::new(&[0.; D], 0).unwrap();
        for rows in [257, 100_000] {
            let allowance = scratch_bytes(rows).unwrap() - TwoBitPlanTrace::scratch_bytes(rows);
            assert_eq!(allowance, 532_480);
            assert!(matches!(
                codec.prepare_query(&[1.; D], allowance - 1),
                Err(TwoBitError::MemoryBudget)
            ));
            codec.prepare_query(&[1.; D], allowance).unwrap();
        }
    }
    #[tokio::test]
    async fn actual_generation_profile_mismatch_refuses_before_seal_or_truth() {
        let shape = Shape::tiny(32);
        for (actual, requested) in [
            (SemanticProfile::Native100k, SemanticProfile::Scale1m),
            (SemanticProfile::Scale1m, SemanticProfile::Native100k),
        ] {
            let (dir, value) = fixture_with_profile(shape, actual).await;
            let mut c: Config = serde_json::from_value(value).unwrap();
            c.profile = requested;
            let mut out = Output::create(&dir.path().join("profile-mismatch")).unwrap();
            let mut progress = Progress::default();
            let error = query_and_seal(&c, shape, &mut out, &mut progress)
                .await
                .err()
                .expect("profile mismatch must refuse");
            assert!(error.to_string().contains("generation geometry/profile"));
            assert!(!progress.sealed && !progress.truth_opened && progress.completed == 0);
        }
    }

    #[test]
    fn actual_native_queries_seal_and_reduce_against_literal_oracles() {
        for parallelism in [16, 32] {
            let shape = Shape::tiny(257);
            let (dir, mut c) = query_runtime(parallelism).unwrap().block_on(fixture(shape));
            c["fetch_parallelism"] = json!(parallelism);
            let (path, sha) = write_config(dir.path(), &c);
            let output = dir.path().join("results");
            let c = config(&path, &sha, shape).unwrap();
            let runtime = configured_query_runtime(&c).unwrap();
            {
                let raw = std::fs::read(dir.path().join("raw")).unwrap();
                let mut directions = std::collections::BTreeSet::new();
                for (id, row) in raw.chunks_exact(D * 4).enumerate() {
                    let values = row
                        .chunks_exact(4)
                        .map(|word| f32::from_le_bytes(word.try_into().unwrap()))
                        .collect::<Vec<_>>();
                    let norm2 = values.iter().map(|&v| f64::from(v).powi(2)).sum::<f64>();
                    assert_eq!(norm2, ((id + 2) as f64).powi(2));
                    directions.insert(
                        values
                            .iter()
                            .map(|&v| (v / (id + 2) as f32).to_bits())
                            .collect::<Vec<_>>(),
                    );
                }
                assert_eq!(directions.len(), shape.rows);
                assert!(directions.len() >= 20);
                let requests = std::fs::read(&c.requests.path).unwrap();
                assert_eq!(request_row(&requests, 0).unwrap()[0], 3.);
                assert_eq!(request_row(&requests, 1).unwrap()[1], 5.);
            }
            let mut out = Output::create(&output).unwrap();
            assert!(!out.directory_synced);
            let mut p = Progress::default();
            let seal = runtime
                .block_on(query_and_seal(&c, shape, &mut out, &mut p))
                .unwrap();
            assert!(!p.truth_opened && p.sealed && out.directory_synced);
            let rows = records(&output);
            let queries = rows
                .iter()
                .filter(|r| r["phase"] == "query")
                .collect::<Vec<_>>();
            assert_eq!(queries.len(), 2);
            let sq8 = std::fs::read(dir.path().join("sq8")).unwrap();
            let literal_bits = [
                0_u32,
                0x3e80_0000,
                0x3e80_0000,
                0x3e80_0000,
                0x3e80_0000,
                0x3e80_0000,
                0x3e80_0000,
                0x3e80_0000,
                0x3e80_0000,
                0x3f00_0000,
            ];
            for (i, query) in queries.iter().enumerate() {
                // Independently normalize the specified 3e0/5e1 queries to e0/e1.
                // Decode stored SQ8 codes with low=-1, step=1/16 for scalar cosine.
                // No production normalization, scoring, or returned IDs feed this oracle.
                let mut oracle = sq8
                    .chunks_exact(D + 12)
                    .enumerate()
                    .map(|(physical, record)| {
                        let id = u64::from_le_bytes(record[..8].try_into().unwrap());
                        (physical, id, fixture_sq8_cosine_bits(record, i))
                    })
                    .collect::<Vec<_>>();
                oracle.sort_by(|a, b| {
                    f32::from_bits(a.2)
                        .total_cmp(&f32::from_bits(b.2))
                        .then(a.1.cmp(&b.1))
                });
                assert_eq!(
                    oracle[..K].iter().map(|r| r.1).collect::<Vec<_>>(),
                    TRUTH_IDS[i]
                );
                assert_eq!(
                    oracle[..K].iter().map(|r| r.2).collect::<Vec<_>>(),
                    literal_bits
                );
                assert_eq!((oracle[K - 1].1, oracle[K].1), (8, 9));
                assert_eq!(oracle[K - 1].2, oracle[K].2); // tie crosses the k10 boundary
                assert_eq!(oracle[K - 1].0, 256); // ID8 is the one-row SQ8 tail
                assert!(oracle[..K].iter().all(|r| r.0 as u64 != r.1));
                assert_eq!(query["returned_count"], 10);
                assert_eq!(
                    query["returned_physical_ordinals"],
                    json!(oracle[..K].iter().map(|r| r.0).collect::<Vec<_>>())
                );
                for (rank, hit) in query["returned"].as_array().unwrap().iter().enumerate() {
                    assert_eq!(hit["id"], TRUTH_IDS[i][rank]);
                    assert_eq!(hit["score_bits"], oracle[rank].2);
                }
                assert_eq!(
                    query["sum"]["submitted_gets"].as_u64().unwrap(),
                    ["router", "source", "sq8"]
                        .iter()
                        .map(|key| query["charges"][*key]["submitted_gets"].as_u64().unwrap())
                        .sum::<u64>()
                );
                assert!(
                    query["charges"]["source"]["verified_bytes"]
                        .as_u64()
                        .unwrap()
                        > 0
                );
            }
            assert_eq!(rows.last().unwrap()["phase"], "all_queries_sealed");
            let summary = reduce(&c, &mut out, &seal, &mut p).unwrap();
            assert_eq!(summary["status"], "MEASURED");
            assert_eq!(summary["total_hits10"], 20);
            assert_eq!(summary["mean_recall10"], 1.0);
            assert_eq!(summary["physical_s3_measured"], false);
            assert_eq!(
                records(&output)
                    .iter()
                    .filter(|r| r["phase"] == "recall")
                    .count(),
                2
            );
            assert!(Output::create(&output).is_err());
            let failed_path = dir.path().join("directory-sync-failure");
            let mut failed = Output::create(&failed_path).unwrap();
            // Same directory identity, but O_PATH cannot fsync: exercise the real failure
            // after the ACTUAL two native queries and sealed-file writes have completed.
            failed.directory = OpenOptions::new()
                .read(true)
                .custom_flags(rustix::fs::OFlags::PATH.bits() as i32)
                .open(dir.path())
                .unwrap();
            let mut p = Progress::default();
            assert!(
                runtime
                    .block_on(query_and_seal(&c, shape, &mut failed, &mut p))
                    .is_err()
            );
            assert_eq!(p.completed, 2);
            assert_eq!(p.stage, "seal_directory_sync");
            assert!(!p.sealed && !p.truth_opened && !failed.directory_synced);
            assert_eq!(
                records(&failed_path)
                    .iter()
                    .filter(|r| r["phase"] == "query")
                    .count(),
                2
            );
        }
    }
    #[tokio::test]
    async fn actual_shared_byte_window_config_reaches_serving_and_preserves_parity() {
        let shape = Shape::tiny(257);
        let (dir, value) = fixture(shape).await;
        let mut original = None;
        for (name, capacity) in [
            ("unset", None),
            ("bounded", Some(16 * 1024 * 1024)),
            ("tiny", Some(1)),
        ] {
            let mut value = value.clone();
            if let Some(bytes) = capacity {
                value["shared_byte_window"] = json!({"max_bytes":bytes,
                    "max_waiters":64,"max_state_bytes":16384});
            }
            let config: Config = serde_json::from_value(value).unwrap();
            let path = dir.path().join(format!("window-{name}.jsonl"));
            let mut out = Output::create(&path).unwrap();
            let mut progress = Progress::default();
            let result = query_and_seal(&config, shape, &mut out, &mut progress).await;
            let rows = records(&path);
            let startup = rows.iter().find(|row| row["phase"] == "startup").unwrap();
            if capacity.is_some() {
                let bytes = startup["shared_byte_window_state_bytes"].as_u64().unwrap();
                assert!(bytes > 0 && bytes <= 16384);
            } else {
                assert!(startup.get("shared_byte_window_state_bytes").is_none());
            }
            if capacity == Some(1) {
                assert!(result.is_err());
                assert!(!progress.sealed && !progress.truth_opened);
                assert_eq!(progress.completed, 0);
                assert!(!rows.iter().any(|row| row["phase"] == "query"));
                continue;
            }
            let seal = result.unwrap();
            let parity = rows
                .iter()
                .filter(|row| row["phase"] == "query")
                .map(|row| {
                    assert_eq!(row["returned"].as_array().unwrap().len(), K);
                    assert!(row["trace"].is_object());
                    assert!(row["charges"].is_object());
                    assert!(row["plan"].is_object());
                    json!({"returned":row["returned"],"trace":row["trace"],
                        "charges":row["charges"],"plan":row["plan"]})
                })
                .collect::<Vec<_>>();
            assert_eq!(parity.len(), 2);
            if let Some(original) = &original {
                assert_eq!(&parity, original);
            } else {
                original = Some(parity);
            }
            let summary = reduce(&config, &mut out, &seal, &mut progress).unwrap();
            assert_eq!(summary["status"], "MEASURED");
            assert_eq!(summary["total_hits10"], 20);
        }
    }

    #[tokio::test]
    async fn actual_sq8_split_config_reaches_serving_and_preserves_parity() {
        let shape = Shape::tiny(257);
        let (dir, base) = fixture(shape).await;
        let object_bytes = 257 * (D + 12);
        let piece_bytes = 64 * (D + 12); // Two authenticated 32-row blocks per full piece.
        for (mode, base) in [
            ("baseline", base.clone()),
            ("direct", with_direct(base, object_bytes, 2)),
        ] {
            let mut original = None;
            for (name, cap, window) in [
                ("null", None, false),
                ("control", Some(33_554_432), false),
                ("split", Some(piece_bytes), false),
                ("split-window", Some(piece_bytes), true),
            ] {
                let mut value = with_panel(base.clone(), &[0, 1], true);
                if let Some(cap) = cap {
                    value["sq8_range_split"] = json!({"max_physical_range_bytes":cap,
                        "max_physical_gets":46});
                }
                if window {
                    value["shared_byte_window"] = json!({"max_bytes":object_bytes,
                        "max_waiters":64,"max_state_bytes":16384});
                }
                let config: Config = serde_json::from_value(value).unwrap();
                validate_config(&config, shape).unwrap();
                let path = dir.path().join(format!("split-{mode}-{name}.jsonl"));
                let mut out = Output::create(&path).unwrap();
                let mut progress = Progress::default();
                // File-backed serving evidence. Actual HTTP attempts/ETags/drain are covered
                // by sq8_s3_range::tests::split_http_count_etag_auth_ordered_error_and_drain.
                let seal = query_and_seal(&config, shape, &mut out, &mut progress)
                    .await
                    .unwrap();
                let rows = records(&path);
                let expected = if cap == Some(piece_bytes) {
                    vec![
                        [0, piece_bytes],
                        [piece_bytes, 2 * piece_bytes],
                        [2 * piece_bytes, 3 * piece_bytes],
                        [3 * piece_bytes, 4 * piece_bytes],
                        [4 * piece_bytes, object_bytes],
                    ]
                } else {
                    vec![[0, object_bytes]]
                };
                let parity = rows
                    .iter()
                    .filter(|row| row["phase"] == "query")
                    .map(|row| {
                        assert_eq!(row["plan"]["ranges"], json!([[0, object_bytes]]));
                        assert_eq!(row["plan"]["logical_range_count"], 1);
                        assert_eq!(row["plan"]["planned_bytes"], object_bytes);
                        let diag = &row["diagnostic"];
                        assert_eq!(diag["schema"], "borsuk-sq8-range-attribution-diagnostic-v6");
                        let trace = &diag["sq8_range_trace"];
                        let fields = trace["range_fields"].as_array().unwrap();
                        let at = |name: &str| fields.iter().position(|f| f == name).unwrap();
                        let spans = trace["ranges"].as_array().unwrap();
                        assert_eq!(spans.len(), expected.len());
                        assert_eq!(trace["planned_ranges"], expected.len());
                        assert_eq!(trace["dropped_ranges"], 0);
                        assert_eq!(trace["outcome"], "ranked");
                        assert_eq!(trace["rank"]["ranges"], row["plan"]["logical_range_count"]);
                        let mut end = 0;
                        for (index, (span, bounds)) in spans.iter().zip(&expected).enumerate() {
                            assert_eq!(span[at("index")], index);
                            assert_eq!(span[at("start_byte")], bounds[0]);
                            assert_eq!(span[at("end_byte")], bounds[1]);
                            assert_eq!(span[at("outcome")], "ok");
                            assert_eq!(span[at("body_bytes")], bounds[1] - bounds[0]);
                            assert_eq!(bounds[0], end); // Ordered, gap-free exact logical union.
                            assert!(bounds[1] <= object_bytes);
                            assert!(bounds[1] - bounds[0] <= cap.unwrap_or(object_bytes));
                            end = bounds[1];
                        }
                        assert_eq!(end, object_bytes);
                        if cap == Some(piece_bytes) {
                            assert_eq!(expected.last().unwrap()[1] - expected.last().unwrap()[0], D + 12);
                            assert!(spans.len() > 1); // Removing with_sq8_range_split must fail.
                            assert!(spans.len() <= 46);
                        }
                        assert_eq!(row["charges"]["sq8"], json!({
                            "submitted_gets":spans.len(), "verified_bytes":object_bytes, "failed_gets":0
                        }));
                        assert_eq!(row["returned_physical_ordinals"].as_array().unwrap().len(), K);
                        json!({"returned":row["returned"],
                            "returned_physical_ordinals":row["returned_physical_ordinals"],
                            "trace":row["trace"],"plan":row["plan"],
                            "source":row["charges"]["source"],"router":row["charges"]["router"],
                            "sq8_bytes":row["charges"]["sq8"]["verified_bytes"]})
                    })
                    .collect::<Vec<_>>();
                assert_eq!(parity.len(), 2);
                if let Some(original) = &original {
                    assert_eq!(&parity, original);
                } else {
                    original = Some(parity);
                }
                let summary = reduce(&config, &mut out, &seal, &mut progress).unwrap();
                assert_eq!(summary["total_hits10"], 20);
                assert_eq!(summary["physical_s3_measured"], false);
            }
        }
    }

    #[tokio::test]
    async fn sq8_split_multi_query_reservation_and_direct_memory_boundaries() {
        let shape = Shape::tiny(257);
        let (_dir, value) = fixture(shape).await;
        let c: Config = serde_json::from_value(value).unwrap();
        let reader = Reader::new(&c.backend, c.s3_read_retry).unwrap();
        let prefix = ObjectPath::from(c.generation_prefix.clone());
        let object_bytes = shape.rows * (D + 12);
        let direct = DirectClosureLimits {
            max_sq8_bytes: object_bytes,
            max_sq8_gets: 2,
        };
        let split = Sq8RangeSplit {
            max_physical_range_bytes: 64 * (D + 12),
            max_physical_gets: 46,
        };
        let mut admission = limits(&c).unwrap();
        admission.max_active_queries = 2;
        admission.max_query_bytes = object_bytes;
        admission.max_source_bytes = object_bytes;
        let baseline = TwoBitGeneration::open_remote(
            reader.store(),
            &prefix,
            &c.generation_root_sha256,
            admission,
            &c.scratch_parent,
        )
        .await
        .unwrap();
        let before = baseline.direct_closure_memory(direct).unwrap();
        drop(baseline);
        let scratch = (split
            .scratch_bytes(object_bytes, admission.max_parallel_gets)
            .unwrap()
            - Sq8RangeTrace::pending_futures_bytes(
                split.max_physical_gets,
                admission.max_parallel_gets,
            )) as u64;
        let resident = before.resident_bytes + 2 * scratch;
        let total = before.total_bytes + 4 * scratch; // Both baseline and direct reserve A * E.
        assert!(total <= MEMORY_CAP);
        let query = request_row(&std::fs::read(&c.requests.path).unwrap(), 0).unwrap();
        for cap in [resident - 1, resident, total - 1, total] {
            admission.max_memory_bytes = cap;
            let generation = TwoBitGeneration::open_remote(
                reader.store(),
                &prefix,
                &c.generation_root_sha256,
                admission,
                &c.scratch_parent,
            )
            .await
            .unwrap();
            let split_generation = generation.with_sq8_range_split(split);
            if cap == resident - 1 {
                assert!(matches!(
                    split_generation,
                    Err(borsuk::two_bit_generation::TwoBitGenerationError::Invalid(
                        "SQ8 split memory"
                    ))
                ));
                continue;
            }
            let generation = split_generation.unwrap();
            let memory = generation.direct_closure_memory(direct).unwrap();
            assert_eq!(memory.resident_bytes, resident);
            assert_eq!(
                memory.direct_query_bytes,
                before.direct_query_bytes + 2 * scratch
            );
            assert_eq!(memory.total_bytes, total);
            let result = generation
                .diagnostic_direct_closure_search_with_store(reader.store(), &query, K, direct)
                .await;
            if cap < total {
                let error = result.err().unwrap();
                assert_eq!(
                    error.direct_resource_rejection(),
                    Some(DirectClosureRejection::ModeledMemory)
                );
                assert_eq!(
                    error.read_stats(),
                    (Sq8ReadStats::default(), Sq8ReadStats::default())
                );
                assert_eq!(error.router_stats(), Sq8ReadStats::default());
            } else {
                let (result, _) = result.unwrap();
                assert_eq!(
                    result.ranked.stats,
                    Sq8ReadStats {
                        submitted_gets: 5,
                        failed_gets: 0,
                        verified_bytes: object_bytes,
                    }
                );
                assert_eq!(result.ranked.candidates.len(), K);
            }
        }
    }

    #[test]
    fn missing_or_tampered_truth_is_invalid_only_after_all_queries_are_sealed() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let shape = Shape::tiny(32);
        let (dir, c) = runtime.block_on(fixture(shape));
        let (path, sha) = write_config(dir.path(), &c);
        let truth = dir.path().join("truth.u64");
        let original = std::fs::read(&truth).unwrap();
        for (index, missing) in [true, false].into_iter().enumerate() {
            if missing {
                std::fs::remove_file(&truth).unwrap();
            } else {
                let mut bad = original.clone();
                bad[0] ^= 1;
                std::fs::write(&truth, bad).unwrap();
            }
            let output = dir.path().join(format!("invalid-{index}"));
            assert!(!execute_paths(&path, &sha, &output, shape).unwrap());
            let records = records(&output);
            assert_eq!(records.iter().filter(|r| r["phase"] == "query").count(), 2);
            assert!(records.iter().any(|r| r["phase"] == "all_queries_sealed"));
            assert!(!records.iter().any(|r| r["phase"] == "recall"));
            let terminal = &records.last().unwrap()["summary"];
            assert_eq!(terminal["status"], "INVALID");
            assert_eq!(terminal["all_queries_sealed"], true);
            assert_eq!(terminal["truth_opened"], true);
        }
    }
    #[tokio::test]
    async fn prefix_or_requests_tampering_blocks_first_truth_open() {
        let shape = Shape::tiny(32);
        let (dir, value) = fixture(shape).await;
        let c: Config = serde_json::from_value(value).unwrap();
        for alter_requests in [false, true] {
            let path = dir.path().join(if alter_requests {
                "request-tamper"
            } else {
                "prefix-tamper"
            });
            let mut out = Output::create(&path).unwrap();
            let mut p = Progress::default();
            let seal = query_and_seal(&c, shape, &mut out, &mut p).await.unwrap();
            let changed = if alter_requests {
                &c.requests.path
            } else {
                &path
            };
            let mut file = OpenOptions::new().write(true).open(changed).unwrap();
            file.write_all(b"!").unwrap();
            file.sync_all().unwrap();
            assert!(reduce(&c, &mut out, &seal, &mut p).is_err());
            assert!(!p.truth_opened);
        }
    }
    #[tokio::test]
    async fn stale_cohort_and_derivation_pins_refuse_before_requests_or_reader_open() {
        let shape = Shape::tiny(32);
        let (dir, base) = fixture(shape).await;
        for (name, expected) in [
            (
                "requests",
                "full selected request/reserved query digest binding",
            ),
            ("truth", "cohort request/truth identity binding"),
            (
                "source",
                "authenticated source-only original/normalized/fit/SQ8 derivation",
            ),
            ("derivation", "artifact identity/SHA"),
        ] {
            let mut value = base.clone();
            match name {
                "requests" => value["requests"]["sha256"] = json!("0".repeat(64)),
                "truth" => value["truth"]["sha256"] = json!("0".repeat(64)),
                "source" => value["native_source"]["source_sha256"] = json!("0".repeat(64)),
                "derivation" => value["derivation_receipt"]["sha256"] = json!("0".repeat(64)),
                _ => unreachable!(),
            }
            // Keep the original receipts: these cases specifically test their refusal layer.
            let c: Config = serde_json::from_value(value).unwrap();
            let mut out = Output::create(&dir.path().join(format!("stale-{name}"))).unwrap();
            let mut p = Progress::default();
            let opened = std::cell::Cell::new(false);
            let error = query_and_seal_with(&c, shape, &mut out, &mut p, || {
                opened.set(true);
                Reader::new(&c.backend, c.s3_read_retry)
            })
            .await
            .err()
            .unwrap()
            .to_string();
            assert!(error.contains(expected), "{name}: {error}");
            assert!(!opened.get(), "{name}");
            assert_eq!((p.completed, p.sealed, p.truth_opened), (0, false, false));
            assert_eq!(out.bytes, 0);
        }
    }

    #[tokio::test]
    async fn source_binding_and_all_request_rows_fail_before_queries() {
        let shape = Shape::tiny(32);
        let (dir, value) = fixture(shape).await;
        for (index, wrong_source) in [true, false].into_iter().enumerate() {
            let mut invalid = value.clone();
            if wrong_source {
                invalid["native_source"]["source_sha256"] = json!("0".repeat(64));
            } else {
                let request_path = dir.path().join("zero-second-row");
                let mut bytes = vec![0; 2 * D * 4];
                for word in bytes[..D * 4].chunks_exact_mut(4) {
                    word.copy_from_slice(&(1_f32 / 32.).to_le_bytes());
                }
                invalid["requests"] = artifact(&request_path, &bytes);
            }
            fixture_receipts(dir.path(), &mut invalid);
            let c: Config = serde_json::from_value(invalid).unwrap();
            validate_receipts(&c).unwrap();
            let mut out =
                Output::create(&dir.path().join(format!("early-invalid-{index}"))).unwrap();
            let mut p = Progress::default();
            let error = query_and_seal(&c, shape, &mut out, &mut p)
                .await
                .err()
                .unwrap()
                .to_string();
            assert!(
                error.contains(if wrong_source {
                    "native source receipt binding"
                } else {
                    "request 1"
                }),
                "{error}"
            );
            assert_eq!(p.completed, 0);
            assert!(!p.sealed && !p.truth_opened);
        }
    }
    #[tokio::test]
    async fn reporting_underfill_keeps_fixed_ten_denominator_and_bounded_output() {
        let mut shape = Shape::tiny(32);
        shape.returned_limit = 3; // Test-only reporting seam after ACTUAL native k10 search.
        let (dir, value) = fixture(shape).await;
        let c: Config = serde_json::from_value(value).unwrap();
        let mut out = Output::create(&dir.path().join("underfill")).unwrap();
        let mut p = Progress::default();
        let seal = query_and_seal(&c, shape, &mut out, &mut p).await.unwrap();
        let summary = reduce(&c, &mut out, &seal, &mut p).unwrap();
        assert_eq!(summary["underfilled_queries"], 2);
        assert_eq!(summary["total_hits10"], 6);
        assert_eq!(summary["recall_denominator"], 20);
        assert_eq!(summary["mean_recall10"], 0.3);
        let sq8 = std::fs::read(dir.path().join("sq8")).unwrap();
        for row in records(&dir.path().join("underfill"))
            .iter()
            .filter(|row| row["phase"] == "query")
        {
            let ordinals = row["returned_physical_ordinals"].as_array().unwrap();
            let hits = row["returned"].as_array().unwrap();
            assert_eq!(row["returned_count"], 3);
            assert_eq!((ordinals.len(), hits.len()), (3, 3));
            for (ordinal, hit) in ordinals.iter().zip(hits) {
                let ordinal = ordinal.as_u64().unwrap() as usize;
                assert!(ordinal < shape.rows);
                let record = &sq8[ordinal * (D + 12)..(ordinal + 1) * (D + 12)];
                assert_eq!(
                    hit["id"],
                    u64::from_le_bytes(record[..8].try_into().unwrap())
                );
                assert_eq!(
                    hit["score_bits"],
                    fixture_sq8_cosine_bits(record, row["ordinal"].as_u64().unwrap() as usize)
                );
            }
        }
        let mut capped = Output::create(&dir.path().join("capped")).unwrap();
        capped.cap = 100;
        let mut p = Progress::default();
        assert!(
            query_and_seal(&c, shape, &mut capped, &mut p)
                .await
                .is_err()
        );
        assert!(!p.sealed && !p.truth_opened);
        assert!(capped.bytes <= 100);
        let mut unsynced = Output::create(&dir.path().join("unsynced")).unwrap();
        unsynced.file = OpenOptions::new().write(true).open("/dev/null").unwrap();
        let mut p = Progress {
            completed: c.count,
            ..Progress::default()
        };
        assert!(super::seal(&c, &mut unsynced, &mut p, seal.requests).is_err());
        assert!(!p.sealed && !p.truth_opened);
    }

    // Integration stub only. sq8_s3_range's real HTTP tests own transport evidence.
    const STUB_ETAG: &str = "\"fixture-current-etag\"";
    #[derive(Debug)]
    struct RecordingStore {
        inner: object_store::memory::InMemory,
        reads: std::sync::Mutex<Vec<(String, bool)>>,
        stats: Arc<std::sync::Mutex<NativeTransportStats>>,
        fault: std::sync::Mutex<&'static str>,
        root: String,
        sq8: String,
    }
    impl std::fmt::Display for RecordingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("recording-fixture-only")
        }
    }
    fn stub_error() -> object_store::Error {
        object_store::Error::Generic {
            store: "recording-fixture-only",
            source: io::Error::other("injected fault").into(),
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for RecordingStore {
        async fn get_opts(
            &self,
            path: &ObjectPath,
            mut options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let head = options.head;
            let fault = *self.fault.lock().unwrap();
            self.reads.lock().unwrap().push((path.to_string(), head));
            {
                let mut stats = self.stats.lock().unwrap();
                stats.attempts += 1;
                stats.method_counts[usize::from(head)] += 1;
            }
            if fault == "source_http500" && !head && path.as_ref().ends_with("/plane/records.bin") {
                return Err(object_store::Error::Generic {
                    store: "fixture",
                    source: Box::new(object_store::Error::Precondition {
                        path: "https://user:credential@example.invalid/long-object?secret=query"
                            .repeat(40),
                        source: io::Error::other(
                            "Server returned non-2xx status code: 500 Internal Server Error",
                        )
                        .into(),
                    }),
                });
            }
            if options.if_match.take().is_some_and(|tag| tag != STUB_ETAG)
                || (fault == "startup" && path.as_ref().ends_with("/plane/mean.bin"))
                || (fault == "query" && !head && path.as_ref() == self.sq8)
            {
                self.stats.lock().unwrap().transport_failures += 1;
                return Err(stub_error());
            }
            let mut result = self.inner.get_opts(path, options).await?;
            result.meta.e_tag = Some(STUB_ETAG.into());
            {
                let mut stats = self.stats.lock().unwrap();
                stats.status_counts[0].1 += 1;
            }
            let root = path.as_ref() == self.root && !head;
            if root {
                match fault {
                    "parse" => {
                        result.meta.size = 1;
                        result.range = 0..1;
                        result.payload = object_store::GetResultPayload::Stream(
                            futures_util::stream::once(async {
                                Ok(bytes::Bytes::from_static(b"{"))
                            })
                            .boxed(),
                        );
                    }
                    "zero" => result.meta.size = 0,
                    "size" => result.meta.size = CONFIG_CAP as u64 + 1,
                    "location" => result.meta.location = ObjectPath::from("physical/ns/wrong"),
                    "range" => result.range.start += 1,
                    "end" => result.range.end -= 1,
                    _ => (),
                }
            }
            if fault == "actual_etag" && !head && path.as_ref() == self.sq8 {
                result.meta.e_tag = Some("\"replaced\"".into());
            }
            let object_store::GetResultPayload::Stream(body) = result.payload else {
                unreachable!()
            };
            let stats = self.stats.clone();
            result.payload = object_store::GetResultPayload::Stream(
                body.map(move |chunk| {
                    let mut bytes = chunk?.to_vec();
                    if fault == "plane_sha" && !root && bytes.first() == Some(&b'{') {
                        bytes[0] ^= 1;
                    }
                    if root {
                        match fault {
                            "short" => {
                                bytes.pop();
                            }
                            "long" => bytes.push(0),
                            "sha" => bytes[0] ^= 1,
                            "stream" => {
                                stats.lock().unwrap().stream_failures += 1;
                                return Err(stub_error());
                            }
                            _ => (),
                        }
                    }
                    stats.lock().unwrap().consumed_payload_bytes += bytes.len() as u64;
                    Ok(bytes::Bytes::from(bytes))
                })
                .boxed(),
            );
            Ok(result)
        }
        async fn put_opts(
            &self,
            path: &ObjectPath,
            body: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(path, body, options).await
        }
        async fn put_multipart_opts(
            &self,
            path: &ObjectPath,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(path, options).await
        }
        async fn put_multipart_create(
            &self,
            path: &ObjectPath,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_create(path, options).await
        }
        fn delete_stream(
            &self,
            paths: futures_util::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(paths)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn namespaced_pipeline_faults_preserve_binding_accounting_and_seal() {
        let shape = Shape::tiny(257);
        let (dir, local) = fixture(shape).await;
        let c: Config = serde_json::from_value(local.clone()).unwrap();
        let mut out = Output::create(&dir.path().join("local-parity")).unwrap();
        let mut p = Progress::default();
        let sealed = query_and_seal(&c, shape, &mut out, &mut p).await.unwrap();
        assert!(p.transport.before.is_none() && p.transport.after.is_none());
        assert!(p.sealed && out.directory_synced && !p.truth_opened);
        assert_eq!(
            reduce(&c, &mut out, &sealed, &mut p).unwrap()["total_hits10"],
            20
        );
        let local_rows = records(&out.path);
        let local_hits = local_rows
            .iter()
            .filter(|r| r["phase"] == "query")
            .map(|r| r["returned"].clone())
            .collect::<Vec<_>>();
        for (i, hits) in local_hits.iter().enumerate() {
            for (rank, hit) in hits.as_array().unwrap().iter().enumerate() {
                assert_eq!(hit["id"], TRUTH_IDS[i][rank]);
                assert_eq!(
                    hit["score_bits"],
                    if rank == 0 {
                        0_u32
                    } else if rank < 9 {
                        0x3e80_0000
                    } else {
                        0x3f00_0000
                    }
                );
            }
        }
        // ID8 occupies physical row256 and wins the ID9 score tie at rank10.
        let sq8 = std::fs::read(dir.path().join("sq8")).unwrap();
        assert_eq!(
            u64::from_le_bytes(sq8[256 * (D + 12)..256 * (D + 12) + 8].try_into().unwrap()),
            8
        );
        let direct = LocalFileSystem::new_with_prefix(dir.path().join("store")).unwrap();
        let root_key = format!("{}/manifest.json", c.generation_prefix);
        let mut root: Value = serde_json::from_slice(
            &direct
                .get(&ObjectPath::from(root_key.clone()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        root["sq8_etag"] = json!(STUB_ETAG);
        let root_body = serde_json::to_vec(&root).unwrap();
        let initial = NativeTransportStats {
            attempts: 37,
            method_counts: [31, 6, 0, 0, 0, 0, 0, 0, 0, 0],
            status_counts: vec![(200, 34)],
            transport_failures: 3,
            stream_failures: 2,
            consumed_payload_bytes: 12345,
            dropped_error_bodies: 1,
        };
        let recorded = Arc::new(RecordingStore {
            inner: object_store::memory::InMemory::new(),
            reads: Default::default(),
            stats: Arc::new(std::sync::Mutex::new(initial.clone())),
            fault: std::sync::Mutex::new("ok"),
            root: format!("physical/ns/{root_key}"),
            sq8: format!("physical/ns/{}", root["sq8_object_key"].as_str().unwrap()),
        });
        let mut objects = direct.list(None);
        while let Some(meta) = objects.next().await {
            let meta = meta.unwrap();
            let body = direct
                .get(&meta.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            recorded
                .inner
                .put(
                    &ObjectPath::from(format!("physical/ns/{}", meta.location)),
                    body.into(),
                )
                .await
                .unwrap();
        }
        recorded
            .inner
            .put(
                &ObjectPath::from(recorded.root.clone()),
                root_body.clone().into(),
            )
            .await
            .unwrap();
        let mut remote = local.clone();
        remote["generation_root_sha256"] = json!(hash(&root_body));
        remote["backend"] = json!({"kind":"s3","bucket":"fixture-bucket","region":"eu-central-1",
            "physical_prefix":"physical/ns","sq8_object_key":root["sq8_object_key"],"sq8_etag":STUB_ETAG});

        for fault in [
            "ok",
            "underfill",
            "invalid_request",
            "invalid_etag",
            "invalid_key",
            "invalid_namespace",
            "invalid_bucket",
            "invalid_region",
            "mixed",
            "unknown",
            "old",
            "root_sha",
            "root_etag",
            "root_key",
            "root_source",
            "source",
            "order",
            "zero",
            "size",
            "location",
            "range",
            "end",
            "short",
            "long",
            "sha",
            "plane_sha",
            "parse",
            "stream",
            "startup",
            "query",
            "source_http500",
            "actual_etag",
            "output",
            "query_output",
            "seal",
            "tamper",
            "truth",
        ] {
            let mut value = remote.clone();
            match fault {
                "invalid_request" => {
                    let mut body = std::fs::read(&c.requests.path).unwrap();
                    body[D * 4..].fill(0);
                    value["requests"] = artifact(&dir.path().join("bad-request"), &body);
                }
                "invalid_etag" => value["backend"]["sq8_etag"] = json!("unquoted"),
                "invalid_key" => value["backend"]["sq8_object_key"] = json!("../bad"),
                "invalid_namespace" => value["backend"]["physical_prefix"] = json!("physical//ns"),
                "invalid_bucket" => value["backend"]["bucket"] = json!("BAD"),
                "invalid_region" => value["backend"]["region"] = json!("https://override"),
                "mixed" => value["backend"]["store_root"] = json!("/unexpected"),
                "unknown" => value["backend"]["endpoint"] = json!("https://override"),
                "old" => value["schema"] = json!("borsuk-cohere-native-baseline-config-v1"),
                "root_sha" => value["generation_root_sha256"] = json!("0".repeat(64)),
                "parse" => value["generation_root_sha256"] = json!(hash(b"{")),
                "root_etag" => value["backend"]["sq8_etag"] = json!("\"wrong\""),
                "root_key" => {
                    value["backend"]["sq8_object_key"] =
                        json!(format!("other/objects/{}", c.native_source.sq8_sha256))
                }
                "root_source" => {
                    value["native_source"]["sq8_sha256"] = json!("0".repeat(64));
                    value["backend"]["sq8_object_key"] =
                        json!(format!("semantic/objects/{}", "0".repeat(64)));
                }
                "source" => value["native_source"]["source_sha256"] = json!("0".repeat(64)),
                "order" => value["native_source"]["source_order_sha256"] = json!("0".repeat(64)),
                _ => (),
            }
            fixture_receipts(dir.path(), &mut value);
            recorded.reads.lock().unwrap().clear();
            *recorded.stats.lock().unwrap() = initial.clone();
            *recorded.fault.lock().unwrap() = fault;
            let parsed = serde_json::from_value::<Config>(value);
            if matches!(fault, "mixed" | "unknown") {
                assert!(parsed.is_err(), "{fault}");
                assert!(recorded.reads.lock().unwrap().is_empty());
                continue;
            }
            let config = parsed.unwrap();
            validate_receipts(&config).unwrap();
            let mut out = Output::create(&dir.path().join(format!("stub-{fault}"))).unwrap();
            if fault == "output" {
                out.cap = 1;
            }
            if fault == "query_output" {
                out.fail_query_output = true;
            }
            if fault == "seal" {
                out.directory = OpenOptions::new()
                    .read(true)
                    .custom_flags(rustix::fs::OFlags::PATH.bits() as i32)
                    .open(dir.path())
                    .unwrap();
            }
            let mut p = Progress::default();
            let test_shape = Shape {
                returned_limit: if fault == "underfill" { 3 } else { K },
                ..shape
            };
            let reader_created = std::cell::Cell::new(false);
            let result = query_and_seal_with(&config, test_shape, &mut out, &mut p, || {
                reader_created.set(true);
                Ok(Reader::Stub {
                    store: Arc::new(ChunkedStore::new(
                        Arc::new(object_store::prefix::PrefixStore::new(
                            recorded.clone(),
                            ObjectPath::from("physical/ns"),
                        )),
                        8192,
                    )),
                    stats: recorded.stats.clone(),
                })
            })
            .await;
            assert!(!p.truth_opened, "{fault}");
            let reads = recorded.reads.lock().unwrap().clone();
            if fault.starts_with("invalid_") || fault == "old" {
                assert!(result.is_err(), "{fault}");
                assert!(reads.is_empty(), "{fault}: {reads:?}");
                assert!(!reader_created.get(), "{fault}");
                continue;
            }
            assert_eq!(reads[0], (recorded.root.clone(), false), "{fault}");
            assert!(
                reads
                    .iter()
                    .all(|(path, _)| path.starts_with("physical/ns/")
                        && !path.starts_with("physical/ns/physical/ns/")),
                "{fault}"
            );
            let after = p.transport.after.as_ref().unwrap();
            assert_eq!(after, &*recorded.stats.lock().unwrap(), "{fault}");
            assert!(after.attempts > initial.attempts, "{fault}");
            assert_eq!(
                after.attempts,
                initial.attempts + reads.len() as u64,
                "{fault}"
            );
            assert!(p.transport.before.as_ref().unwrap().attempts >= initial.attempts);
            if matches!(
                fault,
                "root_sha"
                    | "root_etag"
                    | "root_key"
                    | "root_source"
                    | "zero"
                    | "size"
                    | "location"
                    | "range"
                    | "end"
                    | "short"
                    | "long"
                    | "sha"
                    | "parse"
                    | "stream"
                    | "output"
            ) {
                assert_eq!(
                    reads.len(),
                    if fault == "output" { 3 } else { 1 },
                    "{fault}"
                );
            } else {
                assert_eq!(
                    reads[1],
                    (
                        format!(
                            "physical/ns/{}/plane/manifest.json",
                            config.generation_prefix
                        ),
                        false
                    ),
                    "{fault}"
                );
            }
            if matches!(fault, "ok" | "underfill" | "tamper" | "truth") {
                let seal = result.unwrap();
                assert!(
                    p.sealed && out.directory_synced && p.completed == 2,
                    "{fault}"
                );
                let rows = records(&out.path);
                assert_eq!(rows.last().unwrap()["phase"], "all_queries_sealed");
                assert_eq!(rows[0]["transport"]["before"]["attempts"], 37);
                assert_eq!(rows[0]["charges"]["submitted_gets"], 3);
                let mut previous = serde_json::to_value(&initial).unwrap();
                for row in rows.iter().filter(|r| r["transport"].is_object()) {
                    assert_eq!(row["transport"]["before"], previous, "{fault}");
                    previous = row["transport"]["after"].clone();
                }
                assert_eq!(previous, serde_json::to_value(after).unwrap());
                let queries = rows
                    .iter()
                    .filter(|r| r["phase"] == "query")
                    .collect::<Vec<_>>();
                for (i, row) in queries.iter().enumerate() {
                    let local_query = local_rows
                        .iter()
                        .filter(|r| r["phase"] == "query")
                        .nth(i)
                        .unwrap();
                    assert_eq!(row["charges"], local_query["charges"], "{fault}");
                    assert_eq!(row["sum"], local_query["sum"], "{fault}");
                    assert_eq!(
                        row["returned"],
                        if fault == "underfill" {
                            json!(&local_hits[i].as_array().unwrap()[..3])
                        } else {
                            local_hits[i].clone()
                        }
                    );
                }
                if fault == "tamper" {
                    OpenOptions::new()
                        .write(true)
                        .open(&out.path)
                        .unwrap()
                        .write_all(b"!")
                        .unwrap();
                    assert!(reduce(&config, &mut out, &seal, &mut p).is_err());
                    assert!(!p.truth_opened);
                } else if fault == "truth" {
                    std::fs::remove_file(&config.truth.path).unwrap();
                    assert!(reduce(&config, &mut out, &seal, &mut p).is_err());
                    assert!(p.truth_opened && p.sealed);
                } else {
                    let summary = reduce(&config, &mut out, &seal, &mut p).unwrap();
                    assert_eq!(summary["recall_denominator"], 20);
                    assert_eq!(
                        summary["total_hits10"],
                        if fault == "underfill" { 6 } else { 20 }
                    );
                    assert_eq!(
                        summary["mean_recall10"],
                        if fault == "underfill" { 0.3 } else { 1.0 }
                    );
                }
            } else {
                assert!(result.is_err(), "{fault}");
                if fault == "parse" {
                    assert_eq!(p.binding_charge.submitted_gets, 1);
                    assert_eq!(p.binding_charge.verified_bytes, 1);
                    assert_eq!(p.binding_charge.failed_gets, 0);
                    assert!(
                        result
                            .as_ref()
                            .err()
                            .unwrap()
                            .to_string()
                            .contains("EOF while parsing")
                    );
                }
                if matches!(fault, "root_sha" | "sha" | "plane_sha" | "short") {
                    assert!(
                        result
                            .as_ref()
                            .err()
                            .unwrap()
                            .to_string()
                            .contains("metadata EOF/SHA"),
                        "{fault}"
                    );
                    assert_eq!(p.binding_charge.failed_gets, 1);
                }
                if fault == "long" {
                    assert!(
                        result
                            .as_ref()
                            .err()
                            .unwrap()
                            .to_string()
                            .contains("metadata long EOF")
                    );
                }
                assert!(!p.sealed && !p.truth_opened, "{fault}");
                if matches!(fault, "query" | "actual_etag") {
                    assert!(p.charges.sum().submitted_gets > 0 && p.charges.sum().failed_gets > 0);
                    assert_eq!(p.transport.stage, "query");
                }
                if fault == "query" || fault == "startup" {
                    assert!(after.transport_failures > initial.transport_failures);
                }
                if fault == "stream" {
                    assert!(after.stream_failures > initial.stream_failures);
                }
                if fault == "query_output" {
                    assert_eq!(p.transport.stage, "query");
                    assert!(p.charges.sum().verified_bytes > 0);
                }
                if fault == "seal" {
                    assert_eq!(p.completed, 2);
                    assert!(!out.directory_synced);
                }
                if fault == "source_http500" {
                    let rows = records(&out.path);
                    let failure = rows.iter().find(|r| r["phase"] == "query_failure").unwrap();
                    let text = failure["error"].as_str().unwrap();
                    assert!(text.contains("phase=source store=Generic"), "{text}");
                    assert!(text.contains("500 Internal Server Error"), "{text}");
                    assert!(!text.contains("credential") && !text.contains("secret"));
                    assert!(text.chars().count() <= 512);
                    assert_eq!(failure["ordinal"], 0);
                    assert_eq!(
                        failure["charges_so_far"],
                        serde_json::to_value(&p.charges).unwrap()
                    );
                    assert_eq!(
                        failure["sum_so_far"],
                        serde_json::to_value(p.charges.sum()).unwrap()
                    );
                    assert_eq!(
                        failure["transport"],
                        serde_json::to_value(&p.transport).unwrap()
                    );
                    assert_eq!(failure["truth_opened"], false);
                    assert_eq!(failure["resource_rejection"], Value::Null);
                    assert!(failure["stages"].is_object());
                    assert_eq!(failure["query_wall_ns"], json!(p.query_wall_ns));
                    assert_eq!(failure["query_process_cpu_ns"], json!(p.query_cpu_ns));
                    assert_eq!((p.completed, p.sealed, p.truth_opened), (0, false, false));
                    let returned = result.as_ref().err().unwrap().to_string();
                    let terminal = invalid_summary(&p, &returned);
                    assert!(
                        terminal["error"]
                            .as_str()
                            .unwrap()
                            .contains("500 Internal Server Error")
                    );
                    assert_eq!(terminal["charges"], failure["charges_so_far"]);
                    assert_eq!(terminal["sum"], failure["sum_so_far"]);
                    assert_eq!(
                        terminal["binding_charge"],
                        serde_json::to_value(p.binding_charge).unwrap()
                    );
                    assert_eq!(terminal["transport_last_boundary"], p.transport.terminal());
                }
                let terminal = invalid_summary(&p, "injected failure");
                assert_eq!(
                    terminal["transport_last_boundary"]["after"]["attempts"],
                    after.attempts
                );
                for field in [
                    "wire_bytes",
                    "unread_bytes",
                    "billed_bytes",
                    "billed_requests",
                ] {
                    assert!(terminal["transport_last_boundary"][field].is_null());
                }
                assert!(serde_json::to_vec(&terminal).unwrap().len() < TERMINAL_RESERVE as usize);
            }
        }
        // Even a full native histogram and worst-width scalars cannot exhaust the
        // terminal reserve: full histograms remain in phase records, never here.
        let maximal = NativeTransportStats {
            attempts: u64::MAX,
            method_counts: [u64::MAX; 10],
            status_counts: (100..=999).map(|code| (code, u64::MAX)).collect(),
            transport_failures: u64::MAX,
            stream_failures: u64::MAX,
            consumed_payload_bytes: u64::MAX,
            dropped_error_bodies: u64::MAX,
        };
        let p = Progress {
            transport: TransportSpan {
                stage: "generation_open",
                ordinal: Some(999),
                before: Some(maximal.clone()),
                after: Some(maximal),
            },
            ..Progress::default()
        };
        let terminal = json!({"phase":"terminal","summary":invalid_summary(&p, &"\0".repeat(512))});
        // Include room for execute_paths' process timings/peak and terminating newline.
        assert!(serde_json::to_vec(&terminal).unwrap().len() + 512 < TERMINAL_RESERVE as usize);

        // Exercise execute_paths' actual terminal append and successful fsync after
        // a partial query record, with the real tiny257 pipeline and injected store.
        *recorded.fault.lock().unwrap() = "ok";
        *recorded.stats.lock().unwrap() = initial.clone();
        recorded.reads.lock().unwrap().clear();
        let (config_path, config_sha) = write_config(dir.path(), &remote);
        let output_path = dir.path().join("partial-query-terminal");
        let output_for_thread = output_path.clone();
        let counters = recorded.stats.clone();
        // execute_paths creates a runtime; use another thread, not a nested runtime.
        let status = std::thread::spawn(move || {
            execute_paths_with(
                &config_path,
                &config_sha,
                &output_for_thread,
                shape,
                |c, shape, out, p| {
                    out.fail_query_output = true;
                    configured_query_runtime(&c)?.block_on(query_and_seal_with(
                        c,
                        shape,
                        out,
                        p,
                        || {
                            Ok(Reader::Stub {
                                store: Arc::new(ChunkedStore::new(
                                    Arc::new(object_store::prefix::PrefixStore::new(
                                        recorded.clone(),
                                        ObjectPath::from("physical/ns"),
                                    )),
                                    8192,
                                )),
                                stats: recorded.stats.clone(),
                            })
                        },
                    ))
                },
            )
        })
        .join()
        .unwrap()
        .unwrap();
        assert!(!status); // Ok(false) is returned only after the terminal fsync succeeds.
        let output = std::fs::read_to_string(&output_path).unwrap();
        assert!(output.ends_with('\n'));
        let identities = output
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect::<Vec<_>>();
        assert_eq!(
            identities[0]["s3_credential_source"],
            "imds_instance_role_only"
        );
        assert_eq!(
            identities[0]["native_transport_includes"],
            "S3_and_IMDS_credential_requests_including_PUT"
        );
        assert_eq!(
            identities[1]["credential_source"],
            "imds_instance_role_only"
        );
        assert!(
            output
                .lines()
                .any(|line| line.starts_with("{\"phase\":\"query\"")
                    && serde_json::from_str::<Value>(line).is_err())
        );
        let terminal: Value = serde_json::from_str(output.lines().last().unwrap()).unwrap();
        assert_eq!(terminal["phase"], "terminal");
        let summary = &terminal["summary"];
        assert_eq!(summary["status"], "INVALID");
        assert_eq!(summary["stage"], "query");
        assert_eq!(summary["truth_opened"], false);
        assert_eq!(summary["all_queries_sealed"], false);
        assert_eq!(summary["completed_queries"], 0);
        assert!(summary["sum"]["verified_bytes"].as_u64().unwrap() > 0);
        let final_stats = counters.lock().unwrap();
        let transport = &summary["transport_last_boundary"];
        assert_eq!(transport["after"]["attempts"], final_stats.attempts);
        assert_eq!(
            transport["after"]["consumed_payload_bytes"],
            final_stats.consumed_payload_bytes
        );
        assert!(transport["before"]["attempts"].as_u64().unwrap() >= initial.attempts);
        assert!(
            transport["after"]["attempts"].as_u64().unwrap()
                > transport["before"]["attempts"].as_u64().unwrap()
        );
        for field in [
            "wire_bytes",
            "unread_bytes",
            "billed_bytes",
            "billed_requests",
        ] {
            assert!(transport[field].is_null());
        }
    }

    pub(super) fn concurrent_fixture(active: usize) -> (tempfile::TempDir, Vec<Value>) {
        runtime_task_fixture(active, QueryRuntime::CurrentThread)
    }
    pub(super) fn runtime_task_fixture(
        active: usize,
        mode: QueryRuntime,
    ) -> (tempfile::TempDir, Vec<Value>) {
        let shape = Shape::tiny(32);
        let (dir, mut value) = query_runtime(64).unwrap().block_on(fixture(shape));
        value["schema"] = json!(CONCURRENT_CONFIG_SCHEMA);
        value["max_active_queries"] = json!(active);
        value["query_runtime"] = json!(mode);
        value["fetch_parallelism"] = json!(64);
        value["source_authentication"] = json!("offload2");
        let (status, rows) = run_status(dir.path(), "concurrent.jsonl", &value, shape);
        assert_eq!(status, Status::Measured);
        (dir, rows)
    }

    #[test]
    fn runtime_task_workers_and_dispatch() {
        use std::sync::{Condvar, Mutex};
        use std::time::Duration;
        for mode in [QueryRuntime::CurrentThread, QueryRuntime::MultiThread2] {
            let runtime = build_query_runtime(64, mode).unwrap();
            let main = std::thread::current().id();
            let barrier = Arc::new((Mutex::new(0), Condvar::new()));
            let result =
                runtime.block_on(async {
                    let mut tasks = QueryTasks::new(2).unwrap();
                    for ordinal in 0..2 {
                        let barrier = Arc::clone(&barrier);
                        tasks.spawn(async move {
                        if mode == QueryRuntime::MultiThread2 {
                            let (lock, changed) = &*barrier;
                            let mut count = lock.lock().unwrap();
                            *count += 1;
                            changed.notify_all();
                            let (count, timed) = changed.wait_timeout_while(
                                count, Duration::from_secs(5), |count| *count < 2,
                            ).unwrap();
                            assert!(!timed.timed_out() && *count == 2,
                                "both synchronous query-task polls must run on distinct workers");
                        }
                        (ordinal, std::thread::current().id())
                    }).unwrap();
                    }
                    assert!(
                        tasks
                            .spawn(async { (2, std::thread::current().id()) })
                            .is_err()
                    );
                    tasks
                        .drain()
                        .await
                        .into_iter()
                        .map(|r| r.unwrap())
                        .collect::<Vec<_>>()
                });
            assert_eq!([result[0].0, result[1].0], [0, 1]);
            if mode == QueryRuntime::MultiThread2 {
                assert_ne!(result[0].1, result[1].1);
                assert!(result.iter().all(|r| r.1 != main));
            } else {
                assert!(result.iter().all(|r| r.1 == main));
            }
        }
    }

    #[test]
    fn runtime_task_exact_parity_and_selector() {
        let shape = Shape::tiny(32);
        let (dir, base) = query_runtime(64).unwrap().block_on(fixture(shape));
        assert!(base.get("max_active_queries").is_none());
        let (status, original) = run_status(dir.path(), "inline-q1.jsonl", &base, shape);
        assert_eq!(status, Status::Measured);
        assert!(original[0].get("query_dispatch").is_none());
        for mode in [QueryRuntime::CurrentThread, QueryRuntime::MultiThread2] {
            for active in [1, 2] {
                let (_dir, rows) = runtime_task_fixture(active, mode);
                for (a, b) in original
                    .iter()
                    .filter(|r| r["phase"] == "query")
                    .zip(rows.iter().filter(|r| r["phase"] == "query"))
                {
                    for key in [
                        "ordinal",
                        "returned",
                        "returned_physical_ordinals",
                        "plan",
                        "trace",
                        "charges",
                        "sum",
                    ] {
                        assert_eq!(a[key], b[key], "{mode:?} Q{active} {key}");
                    }
                }
                assert_eq!(rows[0]["query_runtime"], json!(mode));
                let resources =
                    &rows.iter().find(|r| r["phase"] == "startup").unwrap()["concurrent_resources"];
                assert_eq!(resources["runtime_worker_threads"], mode.workers());
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut config = config_value(dir.path(), shape);
        let serial: Config = serde_json::from_value(config.clone()).unwrap();
        assert_eq!(serial.query_runtime, QueryRuntime::CurrentThread);
        config["query_runtime"] = json!("multi_thread2");
        assert!(validate_config(&serde_json::from_value(config.clone()).unwrap(), shape).is_err());
        for bad in [
            Value::Null,
            json!(2),
            json!("multi_thread"),
            json!("current"),
        ] {
            config["query_runtime"] = bad;
            assert!(serde_json::from_value::<Config>(config.clone()).is_err());
        }
    }

    #[test]
    fn runtime_task_caller_admission_boundary_and_runtime_mislabel() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let shape = Shape::tiny(32);
        let (dir, base) = query_runtime(64).unwrap().block_on(fixture(shape));
        let backend_calls = AtomicUsize::new(0);
        let mut common_pin = None;
        for mode in [QueryRuntime::CurrentThread, QueryRuntime::MultiThread2] {
            let mut value = base.clone();
            value["schema"] = json!(CONCURRENT_CONFIG_SCHEMA);
            value["max_active_queries"] = json!(2);
            value["fetch_parallelism"] = json!(64);
            value["source_authentication"] = json!("offload2");
            value["query_runtime"] = json!(mode);
            let mut c: Config = serde_json::from_value(value).unwrap();
            let pinned = pinned_bytes(&c).unwrap();
            assert_eq!(limits(&c).unwrap().already_pinned_bytes, pinned);
            assert_eq!(*common_pin.get_or_insert(pinned), pinned);
            c.max_memory_bytes = pinned - 1;
            let error = configured_query_runtime(&c).err().unwrap().to_string();
            assert!(error.contains("caller/runtime memory"), "{error}");
            assert_eq!(backend_calls.load(SeqCst), 0);
            c.max_memory_bytes = pinned;
            let exact = configured_query_runtime(&c).unwrap();
            exact.block_on(async {
                observed_query_runtime(&c).unwrap();
            });
            drop(exact);
            assert_eq!(backend_calls.load(SeqCst), 0);

            // Restore the valid full cap, then deliberately run under the wrong flavor.
            c.max_memory_bytes = serde_json::from_value::<Config>(base.clone())
                .unwrap()
                .max_memory_bytes;
            let wrong = build_query_runtime(
                64,
                if mode == QueryRuntime::CurrentThread {
                    QueryRuntime::MultiThread2
                } else {
                    QueryRuntime::CurrentThread
                },
            )
            .unwrap();
            let mut out =
                Output::create(&dir.path().join(format!("mislabel-{mode:?}.jsonl"))).unwrap();
            let mut progress = Progress::default();
            let error = wrong
                .block_on(query_and_seal_with(
                    &c,
                    shape,
                    &mut out,
                    &mut progress,
                    || {
                        backend_calls.fetch_add(1, SeqCst);
                        Err("backend must not open".into())
                    },
                ))
                .err()
                .unwrap()
                .to_string();
            assert!(
                error.contains("actual runtime flavor/num_workers"),
                "{error}"
            );
            assert_eq!(backend_calls.load(SeqCst), 0);
            assert!(!progress.sealed && !progress.truth_opened);
        }
    }

    async fn runtime_task_held_fixture(
        mode: QueryRuntime,
        source: bool,
    ) -> (
        tempfile::TempDir,
        Arc<HeldQueryStore>,
        Arc<Reader>,
        Arc<TwoBitGeneration>,
        [f32; D],
    ) {
        let shape = Shape::tiny(32);
        let (dir, mut value) = fixture(shape).await;
        value["schema"] = json!(CONCURRENT_CONFIG_SCHEMA);
        value["max_active_queries"] = json!(2);
        value["fetch_parallelism"] = json!(64);
        value["source_authentication"] = json!("offload2");
        value["query_runtime"] = json!(mode);
        let c: Config = serde_json::from_value(value).unwrap();
        observed_query_runtime(&c).unwrap();
        let root: Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("generation/manifest.json")).unwrap(),
        )
        .unwrap();
        let store = Arc::new(HeldQueryStore {
            inner: ChunkedStore::new(
                Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("store")).unwrap()),
                8192,
            ),
            key: if source {
                format!("{}/plane/records.bin", c.generation_prefix)
            } else {
                root["sq8_object_key"].as_str().unwrap().into()
            },
            entered: Default::default(),
            payload_reads: Default::default(),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
            fail_first: false,
            panic_first: false,
            hold_body: source,
            active_bodies: Default::default(),
            ordered_faults: false,
            second_fault: Default::default(),
        });
        let reader = Arc::new(Reader::Stub {
            store: store.clone(),
            stats: Arc::new(std::sync::Mutex::new(NativeTransportStats {
                attempts: 0,
                method_counts: [0; 10],
                status_counts: vec![],
                transport_failures: 0,
                stream_failures: 0,
                consumed_payload_bytes: 0,
                dropped_error_bodies: 0,
            })),
        });
        let generation = Arc::new(
            TwoBitGeneration::open_remote(
                store.as_ref(),
                &ObjectPath::from(c.generation_prefix.clone()),
                &c.generation_root_sha256,
                limits(&c).unwrap(),
                &c.scratch_parent,
            )
            .await
            .unwrap()
            .with_source_authentication(borsuk::sq8_s3_range::SourceAuthentication::Offload2)
            .unwrap(),
        );
        let query = request_row(&std::fs::read(&c.requests.path).unwrap(), 0).unwrap();
        (dir, store, reader, generation, query)
    }

    #[test]
    fn runtime_task_source_active_cancellation_drains_and_reuses_slots() {
        use std::sync::atomic::Ordering::SeqCst;
        use std::time::Duration;
        let workers = || {
            std::fs::read_dir("/proc/self/task")
                .unwrap()
                .filter_map(|p| std::fs::read_to_string(p.ok()?.path().join("comm")).ok())
                .filter(|name| name.trim() == "borsuk-source-a")
                .count()
        };
        for mode in [QueryRuntime::CurrentThread, QueryRuntime::MultiThread2] {
            assert_eq!(workers(), 0);
            let runtime = build_query_runtime(64, mode).unwrap();
            let (dir, store, reader, generation, query) =
                runtime.block_on(runtime_task_held_fixture(mode, true));
            let weak_reader = Arc::downgrade(&reader);
            let weak_generation = Arc::downgrade(&generation);
            let weak_store = Arc::downgrade(&store);
            runtime.block_on(async {
                for cancel in [true, false] {
                    let entered = store.entered.load(SeqCst);
                    let mut tasks = QueryTasks::new(2).unwrap();
                    for _ in 0..2 {
                        tasks
                            .spawn(owned_query(generation.clone(), reader.clone(), query))
                            .unwrap();
                    }
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while store.entered.load(SeqCst) < entered + 2 {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    assert_eq!(store.active_bodies.load(SeqCst), 2);
                    assert_eq!(
                        workers(),
                        2,
                        "SOURCE response phase owns the shared authentication workers"
                    );
                    if cancel {
                        tasks.abort();
                    } else {
                        store.release.add_permits(2);
                    }
                    let results = tokio::time::timeout(Duration::from_secs(5), tasks.drain())
                        .await
                        .unwrap();
                    for result in results {
                        if cancel {
                            assert!(result.err().unwrap().is_cancelled());
                        } else {
                            assert!(result.unwrap().0.is_ok());
                        }
                    }
                    assert_eq!(store.active_bodies.load(SeqCst), 0);
                    assert_eq!(
                        workers(),
                        0,
                        "explicit drain joins SOURCE workers and releases leases"
                    );
                    assert_eq!(Arc::strong_count(&generation), 1);
                    assert_eq!(Arc::strong_count(&reader), 1);
                }
            });
            drop(generation);
            drop(reader);
            drop(store);
            let start = Instant::now();
            drop(runtime);
            assert!(start.elapsed() < Duration::from_secs(5));
            assert_eq!(workers(), 0);
            assert!(weak_generation.upgrade().is_none());
            assert!(weak_reader.upgrade().is_none());
            assert!(
                weak_store.upgrade().is_none(),
                "no retained client/transport"
            );
            drop(dir);
        }
    }

    #[test]
    fn runtime_task_dispatch_wall_includes_delayed_first_poll() {
        use std::time::Duration;
        for mode in [QueryRuntime::CurrentThread, QueryRuntime::MultiThread2] {
            let runtime = build_query_runtime(64, mode).unwrap();
            let (_dir, store, reader, generation, query) =
                runtime.block_on(runtime_task_held_fixture(mode, false));
            store.release.add_permits(64);
            let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
            let (ready, entered) = std::sync::mpsc::channel();
            let mut blockers = Vec::new();
            for _ in 0..mode.workers() {
                let ready = ready.clone();
                let release = release.clone();
                blockers.push(runtime.spawn(async move {
                    let (lock, changed) = &*release;
                    let held = lock.lock().unwrap();
                    ready.send(()).unwrap();
                    let (held, _) = changed
                        .wait_timeout_while(held, Duration::from_secs(5), |held| !*held)
                        .unwrap();
                    assert!(*held, "bounded worker hold");
                }));
            }
            for _ in 0..mode.workers() {
                entered.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            runtime.block_on(async {
                let mut tasks = QueryTasks::new(1).unwrap();
                tasks.spawn(owned_query(generation, reader, query)).unwrap();
                let held = Instant::now();
                // Current-thread cannot poll while this coordinator blocks; both M2 workers
                // are independently held above. No query can receive its first poll yet.
                std::thread::sleep(Duration::from_millis(50));
                let queued = held.elapsed().as_nanos();
                let (lock, changed) = &*release;
                *lock.lock().unwrap() = true;
                changed.notify_all();
                let rows = tokio::time::timeout(Duration::from_secs(5), tasks.drain())
                    .await
                    .unwrap();
                assert!(rows[0].as_ref().unwrap().0.is_ok());
                assert!(
                    rows[0].as_ref().unwrap().1 >= queued,
                    "dispatch latency must include first-poll queue time"
                );
                for blocker in blockers {
                    blocker.await.unwrap();
                }
            });
        }
    }

    #[test]
    fn runtime_task_owned_query_fault_panic_cancel_drain_and_model() {
        use std::sync::atomic::Ordering::SeqCst;
        use std::time::Duration;
        let shape = Shape::tiny(32);
        let (dir, base) = query_runtime(64).unwrap().block_on(fixture(shape));
        let root: Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("generation/manifest.json")).unwrap(),
        )
        .unwrap();
        for mode in [QueryRuntime::CurrentThread, QueryRuntime::MultiThread2] {
            for fault in 0..4 {
                let mut value = base.clone();
                value["schema"] = json!(CONCURRENT_CONFIG_SCHEMA);
                value["max_active_queries"] = json!(2);
                value["fetch_parallelism"] = json!(64);
                value["source_authentication"] = json!("offload2");
                value["query_runtime"] = json!(mode);
                let c: Config = serde_json::from_value(value).unwrap();
                let runtime = configured_query_runtime(&c).unwrap();
                runtime.block_on(async {
                    let store = Arc::new(HeldQueryStore {
                        inner: ChunkedStore::new(
                            Arc::new(
                                LocalFileSystem::new_with_prefix(dir.path().join("store")).unwrap(),
                            ),
                            8192,
                        ),
                        key: root["sq8_object_key"].as_str().unwrap().into(),
                        entered: Default::default(),
                        payload_reads: Default::default(),
                        release: Arc::new(tokio::sync::Semaphore::new(0)),
                        hold_body: false,
                        active_bodies: Default::default(),
                        ordered_faults: false,
                        second_fault: Default::default(),
                        fail_first: fault == 1,
                        panic_first: fault == 2,
                    });
                    let reader = Arc::new(Reader::Stub {
                        store: store.clone(),
                        stats: Arc::new(std::sync::Mutex::new(NativeTransportStats {
                            attempts: 0,
                            method_counts: [0; 10],
                            status_counts: vec![],
                            transport_failures: 0,
                            stream_failures: 0,
                            consumed_payload_bytes: 0,
                            dropped_error_bodies: 0,
                        })),
                    });
                    let query = request_row(&std::fs::read(&c.requests.path).unwrap(), 0).unwrap();
                    let mut admission = limits(&c).unwrap();
                    let generation = Arc::new(
                        TwoBitGeneration::open_remote(
                            store.as_ref(),
                            &ObjectPath::from(c.generation_prefix.clone()),
                            &c.generation_root_sha256,
                            admission,
                            &c.scratch_parent,
                        )
                        .await
                        .unwrap()
                        .with_source_authentication(
                            borsuk::sq8_s3_range::SourceAuthentication::Offload2,
                        )
                        .unwrap(),
                    );
                    let mut tasks = QueryTasks::new(2).unwrap();
                    // Bind physical first fault to ordinal zero before starting ordinal one.
                    for expected in 1..=2 {
                        tasks
                            .spawn(owned_query(generation.clone(), reader.clone(), query))
                            .unwrap();
                        tokio::time::timeout(Duration::from_secs(5), async {
                            while store.entered.load(SeqCst) < expected {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .unwrap();
                    }
                    if fault == 3 {
                        tasks.abort();
                    } else {
                        store.release.add_permits(2);
                    }
                    let results = tokio::time::timeout(Duration::from_secs(5), tasks.drain())
                        .await
                        .unwrap();
                    assert_eq!(Arc::strong_count(&generation), 1);
                    assert_eq!(Arc::strong_count(&reader), 1);
                    if fault == 3 {
                        assert!(
                            results
                                .iter()
                                .all(|r| r.as_ref().err().unwrap().is_cancelled())
                        );
                    } else {
                        assert!(
                            results[1].as_ref().unwrap().0.is_ok(),
                            "every sibling drains after fault or panic"
                        );
                        match fault {
                            0 => assert!(results[0].as_ref().unwrap().0.is_ok()),
                            1 => assert!(results[0].as_ref().unwrap().0.is_err()),
                            2 => assert!(results[0].as_ref().err().unwrap().is_panic()),
                            _ => unreachable!(),
                        }
                    }
                    // Both slots are reusable after normal, fault, panic and explicit cancellation.
                    let entered = store.entered.load(SeqCst);
                    let mut replacements = QueryTasks::new(2).unwrap();
                    for _ in 0..2 {
                        replacements
                            .spawn(owned_query(generation.clone(), reader.clone(), query))
                            .unwrap();
                    }
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while store.entered.load(SeqCst) < entered + 2 {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    store.release.add_permits(2);
                    assert!(
                        tokio::time::timeout(Duration::from_secs(5), replacements.drain())
                            .await
                            .unwrap()
                            .iter()
                            .all(|r| r.as_ref().unwrap().0.is_ok())
                    );
                    let model = generation
                        .direct_closure_memory(DirectClosureLimits {
                            max_sq8_bytes: c.sq8_byte_budget,
                            max_sq8_gets: c.sq8_get_budget,
                        })
                        .unwrap()
                        .resident_bytes;
                    drop(generation);
                    let before = store.payload_reads.load(SeqCst);
                    for cap in [model - 1, model] {
                        admission.max_memory_bytes = cap;
                        let opened = TwoBitGeneration::open_remote(
                            store.as_ref(),
                            &ObjectPath::from(c.generation_prefix.clone()),
                            &c.generation_root_sha256,
                            admission,
                            &c.scratch_parent,
                        )
                        .await
                        .unwrap();
                        assert_eq!(
                            opened
                                .with_source_authentication(
                                    borsuk::sq8_s3_range::SourceAuthentication::Offload2
                                )
                                .is_ok(),
                            cap == model
                        );
                        assert_eq!(store.payload_reads.load(SeqCst), before);
                    }
                    let mut out =
                        Output::create(&dir.path().join(format!("size-{mode:?}-{fault}.jsonl")))
                            .unwrap();
                    let mut progress = Progress::default();
                    let future = query_and_seal(&c, shape, &mut out, &mut progress);
                    assert!(
                        std::mem::size_of_val(&future) + TASK_STATE_BYTES <= QUERY_FUTURE_BYTES
                    );
                    assert_eq!(store.payload_reads.load(SeqCst), before);
                });
            }
        }
    }

    #[test]
    fn runtime_task_ordered_faults_full_drain() {
        use futures_util::FutureExt;
        use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
        for mode in [QueryRuntime::CurrentThread, QueryRuntime::MultiThread2] {
            let runtime = build_query_runtime(64, mode).unwrap();
            runtime.block_on(async {
                let finished = Arc::new(AtomicBool::new(false));
                let (release, held) = tokio::sync::oneshot::channel::<()>();
                let mut tasks = QueryTasks::new(2).unwrap();
                tasks
                    .spawn(async move {
                        held.await.unwrap();
                        Err::<(), _>("first ordinal")
                    })
                    .unwrap();
                let second = finished.clone();
                tasks
                    .spawn(async move {
                        second.store(true, SeqCst);
                        Err::<(), _>("second ordinal")
                    })
                    .unwrap();
                let mut drain = Box::pin(tasks.drain());
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while !finished.load(SeqCst) {
                        assert!(drain.as_mut().now_or_never().is_none());
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert!(drain.as_mut().now_or_never().is_none());
                release.send(()).unwrap();
                let ordered = drain.await;
                assert_eq!(ordered[0].as_ref().unwrap(), &Err("first ordinal"));
                assert_eq!(ordered[1].as_ref().unwrap(), &Err("second ordinal"));
            });
        }
    }

    #[test]
    fn runtime_task_runner_panic_or_fault_stops_later_batches_without_seal() {
        use futures_util::FutureExt;
        use std::sync::atomic::Ordering::SeqCst;
        let shape = Shape {
            count: 4,
            ..Shape::tiny(32)
        };
        let (dir, base) = query_runtime(64).unwrap().block_on(fixture(shape));
        let root: Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("generation/manifest.json")).unwrap(),
        )
        .unwrap();
        for mode in [QueryRuntime::CurrentThread, QueryRuntime::MultiThread2] {
            for fault in 0..3 {
                let panic = fault == 1;
                let mut value = base.clone();
                value["schema"] = json!(CONCURRENT_CONFIG_SCHEMA);
                value["max_active_queries"] = json!(2);
                value["fetch_parallelism"] = json!(64);
                value["source_authentication"] = json!("offload2");
                value["query_runtime"] = json!(mode);
                let c: Config = serde_json::from_value(value).unwrap();
                let runtime = configured_query_runtime(&c).unwrap();
                let store = Arc::new(HeldQueryStore {
                    inner: ChunkedStore::new(
                        Arc::new(
                            LocalFileSystem::new_with_prefix(dir.path().join("store")).unwrap(),
                        ),
                        8192,
                    ),
                    key: root["sq8_object_key"].as_str().unwrap().into(),
                    entered: Default::default(),
                    payload_reads: Default::default(),
                    release: Arc::new(tokio::sync::Semaphore::new(0)),
                    hold_body: false,
                    active_bodies: Default::default(),
                    ordered_faults: fault == 2,
                    second_fault: Default::default(),
                    fail_first: fault == 0,
                    panic_first: panic,
                });
                let mut out =
                    Output::create(&dir.path().join(format!("runner-{mode:?}-{fault}.jsonl")))
                        .unwrap();
                let mut progress = Progress::default();
                runtime.block_on(async {
                    let mut running = Box::pin(query_and_seal_with(
                        &c,
                        shape,
                        &mut out,
                        &mut progress,
                        || {
                            Ok(Reader::Stub {
                                store: store.clone(),
                                stats: Arc::new(std::sync::Mutex::new(NativeTransportStats {
                                    attempts: 0,
                                    method_counts: [0; 10],
                                    status_counts: vec![],
                                    transport_failures: 0,
                                    stream_failures: 0,
                                    consumed_payload_bytes: 0,
                                    dropped_error_bodies: 0,
                                })),
                            })
                        },
                    ));
                    tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        while store.entered.load(SeqCst) < 2 {
                            assert!(running.as_mut().now_or_never().is_none());
                            tokio::task::yield_now().await;
                        }
                        store.release.add_permits(2);
                        let error = running.await.err().unwrap().to_string();
                        if fault == 2 {
                            assert!(store.second_fault.load(SeqCst));
                            assert!(error.contains("ordered fault ordinal 0"), "{error}");
                        }
                    })
                    .await
                    .unwrap();
                });
                assert_eq!(store.entered.load(SeqCst), 2, "later batches must not run");
                assert_eq!(
                    Arc::strong_count(&store),
                    1,
                    "no detached query owns transport"
                );
                assert!(!progress.sealed && !progress.truth_opened);
                let rows = records(&out.path);
                let outcomes = rows
                    .iter()
                    .filter(|r| matches!(r["phase"].as_str(), Some("query" | "query_failure")))
                    .collect::<Vec<_>>();
                assert_eq!(outcomes.len(), 2);
                assert_eq!(outcomes[0]["ordinal"], 0);
                assert_eq!(outcomes[1]["ordinal"], 1);
                assert_eq!(
                    outcomes.iter().filter(|r| r["phase"] == "query").count(),
                    if fault == 2 { 0 } else { 1 }
                );
                assert!(!rows.iter().any(|r| r["phase"] == "all_queries_sealed"));
            }
        }
    }

    #[test]
    fn runtime_task_drop_drains_at_runtime_boundary() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        struct Live(Arc<AtomicUsize>);
        impl Drop for Live {
            fn drop(&mut self) {
                self.0.fetch_sub(1, SeqCst);
            }
        }
        for mode in [QueryRuntime::CurrentThread, QueryRuntime::MultiThread2] {
            let runtime = build_query_runtime(64, mode).unwrap();
            let live = Arc::new(AtomicUsize::new(0));
            let pins = Arc::new(());
            let weak = Arc::downgrade(&pins);
            runtime.block_on(async {
                let mut tasks = QueryTasks::new(2).unwrap();
                for _ in 0..2 {
                    let live = live.clone();
                    let pin = pins.clone();
                    tasks
                        .spawn(async move {
                            let _pin = pin;
                            live.fetch_add(1, SeqCst);
                            let _live = Live(live);
                            std::future::pending::<()>().await;
                        })
                        .unwrap();
                }
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while live.load(SeqCst) != 2 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                drop(tasks);
            });
            drop(pins);
            drop(runtime);
            assert_eq!(live.load(SeqCst), 0);
            assert!(weak.upgrade().is_none());
        }
    }

    #[test]
    fn concurrent_probe_configuration_refuses_widening_and_preserves_serial_default() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let original = config_value(dir.path(), shape);
        let serial: Config = serde_json::from_value(original.clone()).unwrap();
        assert_eq!(active_queries(&serial), 1);
        assert!(config_schema(&serial));
        let mut good = original;
        good["schema"] = json!(CONCURRENT_CONFIG_SCHEMA);
        good["max_active_queries"] = json!(2);
        good["fetch_parallelism"] = json!(64);
        good["source_authentication"] = json!("offload2");
        validate_config(&serde_json::from_value(good.clone()).unwrap(), shape).unwrap();
        for (key, bad) in [
            ("max_active_queries", json!(0)),
            ("max_active_queries", json!(3)),
            ("max_active_queries", Value::Null),
            ("schema", json!(CONFIG_SCHEMA)),
            ("fetch_parallelism", json!(128)),
            ("source_authentication", json!("inline")),
            (
                "execution",
                json!({"mode":"diagnostic_panel","ordinals":[0],"trace":true}),
            ),
        ] {
            let mut value = good.clone();
            value[key] = bad;
            let parsed: Config = serde_json::from_value(value).unwrap();
            assert!(validate_config(&parsed, shape).is_err(), "{key}");
        }
        let two: Config = serde_json::from_value(good.clone()).unwrap();
        good["max_active_queries"] = json!(1);
        let one: Config = serde_json::from_value(good).unwrap();
        assert!(
            pinned_bytes(&two).unwrap() >= pinned_bytes(&one).unwrap() + QUERY_FUTURE_BYTES as u64
        );
    }

    #[test]
    fn concurrent_probe_exact_results_plans_charges_and_cohort() {
        let (_a, one) = concurrent_fixture(1);
        let (_b, two) = concurrent_fixture(2);
        let queries = |rows: &[Value]| {
            rows.iter()
                .filter(|r| r["phase"] == "query")
                .cloned()
                .collect::<Vec<_>>()
        };
        for (a, b) in queries(&one).iter().zip(queries(&two)) {
            for key in [
                "ordinal",
                "returned",
                "returned_physical_ordinals",
                "plan",
                "trace",
                "charges",
                "sum",
            ] {
                assert_eq!(a[key], b[key], "{key}");
            }
            assert!(a["query_process_cpu_ns"].is_null() && b["transport"].is_null());
        }
        for rows in [&one, &two] {
            let cohort = rows.iter().find(|r| r["phase"] == "query_cohort").unwrap();
            assert_eq!(cohort["attempted_count"], 2);
            assert_eq!(cohort["successful_count"], 2);
            assert_eq!(rows.last().unwrap()["summary"]["query_cohort"], *cohort);
            assert_eq!(
                rows.last().unwrap()["summary"]["query_process_cpu_ns"],
                cohort["process_cpu_ns"]
            );
        }
    }

    struct HeldBody(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for HeldBody {
        fn drop(&mut self) {
            self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    #[derive(Debug)]
    struct HeldQueryStore {
        inner: ChunkedStore,
        key: String,
        entered: std::sync::atomic::AtomicUsize,
        payload_reads: std::sync::atomic::AtomicUsize,
        release: Arc<tokio::sync::Semaphore>,
        hold_body: bool,
        active_bodies: Arc<std::sync::atomic::AtomicUsize>,
        ordered_faults: bool,
        second_fault: std::sync::atomic::AtomicBool,
        fail_first: bool,
        panic_first: bool,
    }
    impl std::fmt::Display for HeldQueryStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("held-query-fixture")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for HeldQueryStore {
        async fn get_opts(
            &self,
            path: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if !options.head
                && (path.as_ref() == self.key
                    || path.as_ref().ends_with("/plane/records.bin")
                    || path.as_ref().ends_with("/router/leaves.bin"))
            {
                self.payload_reads
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            if !options.head && path.as_ref() == self.key && self.hold_body {
                use futures_util::StreamExt;
                let mut result = self.inner.get_opts(path, options).await?;
                let extensions = std::mem::take(&mut result.extensions);
                let meta = result.meta.clone();
                let range = result.range.clone();
                let attributes = result.attributes.clone();
                let release = self.release.clone();
                let guard = HeldBody(self.active_bodies.clone());
                guard.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.entered
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Ok(object_store::GetResult {
                    meta,
                    range,
                    attributes,
                    extensions,
                    payload: object_store::GetResultPayload::Stream(
                        futures_util::stream::once(async move {
                            let _guard = guard;
                            release.acquire_owned().await.unwrap().forget();
                            result.bytes().await
                        })
                        .boxed(),
                    ),
                });
            }
            if !options.head && path.as_ref() == self.key {
                let index = self
                    .entered
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.release.acquire().await.unwrap().forget();
                if self.ordered_faults {
                    use std::sync::atomic::Ordering::SeqCst;
                    let (ordinal, completed) = QUERY_TEST_ORDINAL.with(|state| state.clone());
                    if ordinal == 0 {
                        while !completed[1].load(SeqCst) {
                            tokio::task::yield_now().await;
                        }
                    } else {
                        self.second_fault.store(true, SeqCst);
                    }
                    return Err(object_store::Error::Generic {
                        store: "issue-order-fixture",
                        source: format!("ordered fault ordinal {ordinal}").into(),
                    });
                }
                assert!(!(self.panic_first && index == 0), "injected query panic");
                if self.fail_first && index == 0 {
                    return Err(stub_error());
                }
            }
            self.inner.get_opts(path, options).await
        }
        async fn put_opts(
            &self,
            p: &ObjectPath,
            b: object_store::PutPayload,
            o: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(p, b, o).await
        }
        async fn put_multipart_opts(
            &self,
            p: &ObjectPath,
            o: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(p, o).await
        }
        async fn put_multipart_create(
            &self,
            p: &ObjectPath,
            o: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_create(p, o).await
        }
        fn delete_stream(
            &self,
            p: futures_util::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(p)
        }
        fn list(
            &self,
            p: Option<&ObjectPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(p)
        }
        async fn list_with_delimiter(
            &self,
            p: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(
            &self,
            a: &ObjectPath,
            b: &ObjectPath,
            o: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(a, b, o).await
        }
    }

    #[tokio::test]
    async fn concurrent_probe_real_api_overlap_serialization_fault_cancel_and_model_boundary() {
        use futures_util::FutureExt;
        use std::sync::atomic::Ordering::SeqCst;
        let shape = Shape::tiny(32);
        let (dir, value) = fixture(shape).await;
        let c: Config = serde_json::from_value(value.clone()).unwrap();
        let root: Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("generation/manifest.json")).unwrap(),
        )
        .unwrap();
        let query = request_row(&std::fs::read(&c.requests.path).unwrap(), 0).unwrap();
        for active in [1, 2] {
            let mut concurrent = value.clone();
            concurrent["schema"] = json!(CONCURRENT_CONFIG_SCHEMA);
            concurrent["max_active_queries"] = json!(active);
            concurrent["fetch_parallelism"] = json!(64);
            concurrent["source_authentication"] = json!("offload2");
            let c: Config = serde_json::from_value(concurrent).unwrap();
            validate_config(&c, shape).unwrap();
            for fault in [false, true] {
                let store = HeldQueryStore {
                    inner: ChunkedStore::new(
                        Arc::new(
                            LocalFileSystem::new_with_prefix(dir.path().join("store")).unwrap(),
                        ),
                        8192,
                    ),
                    key: root["sq8_object_key"].as_str().unwrap().into(),
                    entered: Default::default(),
                    payload_reads: Default::default(),
                    release: Arc::new(tokio::sync::Semaphore::new(0)),
                    hold_body: false,
                    active_bodies: Default::default(),
                    ordered_faults: false,
                    second_fault: Default::default(),
                    fail_first: fault,
                    panic_first: false,
                };
                let mut admission = limits(&c).unwrap();
                assert_eq!(admission.max_active_queries, active);
                assert!(
                    admission.already_pinned_bytes
                        >= (active * QUERY_FUTURE_BYTES + COHORT_RECORD_BYTES) as u64
                );
                let generation = TwoBitGeneration::open_remote(
                    &store,
                    &ObjectPath::from(c.generation_prefix.clone()),
                    &c.generation_root_sha256,
                    admission,
                    &c.scratch_parent,
                )
                .await
                .unwrap()
                .with_source_authentication(borsuk::sq8_s3_range::SourceAuthentication::Offload2)
                .unwrap();
                // The fault belongs to the first physical GET. Hold ordinal zero
                // there before starting its sibling so the failure oracle is deterministic.
                let mut first =
                    Box::pin(generation.diagnostic_search_with_store(&store, &query, K));
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while store.entered.load(SeqCst) < 1 {
                        assert!(first.as_mut().now_or_never().is_none());
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                let mut joined = Box::pin(futures_util::future::join(
                    first,
                    generation.diagnostic_search_with_store(&store, &query, K),
                ));
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    loop {
                        assert!(joined.as_mut().now_or_never().is_none());
                        if store.entered.load(SeqCst) == active {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    for _ in 0..20 {
                        assert!(joined.as_mut().now_or_never().is_none());
                        tokio::task::yield_now().await;
                    }
                    assert_eq!(store.entered.load(SeqCst), active);
                    store.release.add_permits(2);
                    let (first, second) = joined.await;
                    assert_eq!(first.is_err(), fault);
                    assert!(
                        second.is_ok(),
                        "failed sibling must not cancel successful query"
                    );
                    if let Ok((first, _)) = first {
                        assert_eq!(first.plan.ranges, second.as_ref().unwrap().0.plan.ranges);
                        assert_eq!(first.source_stats, second.as_ref().unwrap().0.source_stats);
                    }
                })
                .await
                .unwrap();
                // Cancel a held API future, then demonstrate its slot and pins are reusable.
                if !fault {
                    let mut cancelled =
                        Box::pin(generation.diagnostic_search_with_store(&store, &query, K));
                    tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        while store.entered.load(SeqCst) < 3 {
                            assert!(cancelled.as_mut().now_or_never().is_none());
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    drop(cancelled);
                    let entered = store.entered.load(SeqCst);
                    let mut replacements =
                        Box::pin(futures_util::future::join_all((0..active).map(|_| {
                            generation.diagnostic_search_with_store(&store, &query, K)
                        })));
                    tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        while store.entered.load(SeqCst) < entered + active {
                            assert!(replacements.as_mut().now_or_never().is_none());
                            tokio::task::yield_now().await;
                        }
                        assert_eq!(store.entered.load(SeqCst), entered + active);
                        store.release.add_permits(active);
                        assert!(replacements.await.iter().all(|result| result.is_ok()));
                    })
                    .await
                    .unwrap();
                }
                // Public memory witness: resident is the baseline model, without the direct extra.
                let model = generation
                    .direct_closure_memory(DirectClosureLimits {
                        max_sq8_bytes: c.sq8_byte_budget,
                        max_sq8_gets: c.sq8_get_budget,
                    })
                    .unwrap()
                    .resident_bytes;
                drop(generation);
                let before = store.payload_reads.load(SeqCst);
                for cap in [model - 1, model] {
                    admission.max_memory_bytes = cap;
                    let opened = TwoBitGeneration::open_remote(
                        &store,
                        &ObjectPath::from(c.generation_prefix.clone()),
                        &c.generation_root_sha256,
                        admission,
                        &c.scratch_parent,
                    )
                    .await
                    .unwrap();
                    let admitted = opened.with_source_authentication(
                        borsuk::sq8_s3_range::SourceAuthentication::Offload2,
                    );
                    assert_eq!(admitted.is_ok(), cap == model);
                    assert_eq!(
                        store.payload_reads.load(SeqCst),
                        before,
                        "concurrent caller model boundary performs zero SOURCE/SQ8 payload IO"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn concurrent_probe_runner_failure_drains_and_records_successful_sibling() {
        let shape = Shape::tiny(32);
        let (dir, mut value) = fixture(shape).await;
        value["schema"] = json!(CONCURRENT_CONFIG_SCHEMA);
        value["max_active_queries"] = json!(2);
        value["fetch_parallelism"] = json!(64);
        value["source_authentication"] = json!("offload2");
        let c: Config = serde_json::from_value(value).unwrap();
        let root: Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("generation/manifest.json")).unwrap(),
        )
        .unwrap();
        let store = Arc::new(HeldQueryStore {
            inner: ChunkedStore::new(
                Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("store")).unwrap()),
                8192,
            ),
            key: root["sq8_object_key"].as_str().unwrap().into(),
            entered: Default::default(),
            payload_reads: Default::default(),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
            hold_body: false,
            active_bodies: Default::default(),
            ordered_faults: false,
            second_fault: Default::default(),
            fail_first: true,
            panic_first: false,
        });
        let mut out = Output::create(&dir.path().join("failure.jsonl")).unwrap();
        let mut progress = Progress::default();
        use futures_util::FutureExt;
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut running = Box::pin(query_and_seal_with(
                &c,
                shape,
                &mut out,
                &mut progress,
                || {
                    Ok(Reader::Stub {
                        store: store.clone(),
                        stats: Arc::new(std::sync::Mutex::new(NativeTransportStats {
                            attempts: 0,
                            method_counts: [0; 10],
                            status_counts: vec![],
                            transport_failures: 0,
                            stream_failures: 0,
                            consumed_payload_bytes: 0,
                            dropped_error_bodies: 0,
                        })),
                    })
                },
            ));
            while store.entered.load(std::sync::atomic::Ordering::SeqCst) < 2 {
                assert!(running.as_mut().now_or_never().is_none());
                tokio::task::yield_now().await;
            }
            assert_eq!(store.entered.load(std::sync::atomic::Ordering::SeqCst), 2);
            store.release.add_permits(2);
            running.await
        })
        .await
        .unwrap();
        assert!(result.is_err());
        assert_eq!(store.entered.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(!progress.sealed && !progress.truth_opened);
        let rows = records(&out.path);
        let outcomes = rows
            .iter()
            .filter(|r| matches!(r["phase"].as_str(), Some("query" | "query_failure")))
            .collect::<Vec<_>>();
        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes[0]["ordinal"], 0);
        assert_eq!(outcomes[1]["ordinal"], 1);
        assert_eq!(outcomes.iter().filter(|r| r["phase"] == "query").count(), 1);
        assert_eq!(progress.cohort.as_ref().unwrap()["successful_count"], 1);
    }

    // ===== Direct-closure serving mode: explicit format, controls, resource outcomes =====
    fn with_direct(mut value: Value, bytes: usize, gets: usize) -> Value {
        value["serving"] =
            json!({"mode":"direct_closure","max_sq8_bytes":bytes,"max_sq8_gets":gets});
        value
    }
    pub(super) fn run_status(
        dir: &Path,
        name: &str,
        value: &Value,
        shape: Shape,
    ) -> (Status, Vec<Value>) {
        let (path, sha) = write_config(dir, value);
        let output = dir.join(name);
        let status = execute_paths_status(&path, &sha, &output, shape, |c, shape, out, p| {
            configured_query_runtime(&c)?.block_on(query_and_seal(c, shape, out, p))
        })
        .unwrap();
        (status, records(&output))
    }
    #[test]
    fn serving_mode_and_format_marker_are_required_explicit_and_strict() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let baseline: Config = serde_json::from_value(config_value(dir.path(), shape)).unwrap();
        assert_eq!(baseline.serving, Serving::Baseline {});
        assert_eq!(baseline.serving.direct(), None);
        validate_config(&baseline, shape).unwrap();
        let good = with_direct(config_value(dir.path(), shape), 1_000_000, 32);
        let direct: Config = serde_json::from_value(good.clone()).unwrap();
        validate_config(&direct, shape).unwrap();
        assert_eq!(
            direct.serving.direct(),
            Some(DirectClosureLimits {
                max_sq8_bytes: 1_000_000,
                max_sq8_gets: 32
            })
        );
        // Opening limits are the benchmark's, in both modes: direct caps live elsewhere.
        for c in [&baseline, &direct] {
            let admission = limits(c).unwrap();
            assert_eq!(admission.max_query_bytes, 16_773_120);
            assert_eq!(admission.max_query_gets, 32);
            assert_eq!(admission.max_source_bytes, 64 * 1024 * 1024);
            assert_eq!(admission.max_source_gets, 128);
            assert_eq!(admission.max_active_queries, 1);
            assert_eq!(admission.max_memory_bytes, MEMORY_CAP);
        }
        // The serving mode has no default: its absence refuses, as does every malformed form.
        let mut missing = good.clone();
        missing.as_object_mut().unwrap().remove("serving");
        assert!(serde_json::from_value::<Config>(missing).is_err());
        for bad in [
            json!({"mode":"baseline","max_sq8_bytes":1}),
            json!({"mode":"baseline","x":0}),
            json!({"mode":"direct_closure"}),
            json!({"mode":"direct_closure","max_sq8_bytes":1}),
            json!({"mode":"direct_closure","max_sq8_gets":1}),
            json!({"mode":"direct_closure","max_sq8_bytes":1,"max_sq8_gets":1,"x":0}),
            json!({"mode":"direct_closure","max_sq8_bytes":-1,"max_sq8_gets":1}),
            json!({"mode":"direct","max_sq8_bytes":1,"max_sq8_gets":1}),
            json!({"max_sq8_bytes":1,"max_sq8_gets":1}),
            json!("direct_closure"),
            json!(null),
        ] {
            let mut value = good.clone();
            value["serving"] = bad.clone();
            assert!(serde_json::from_value::<Config>(value).is_err(), "{bad}");
        }
        // Parsed but out of the sanity bounds: explicit, positive and bounded caps only.
        for (bytes, gets, ok) in [
            (1, 1, true),
            (MEMORY_CAP as usize, 128, true),
            (0, 32, false),
            (1_000_000, 0, false),
            (MEMORY_CAP as usize + 1, 32, false),
            (1_000_000, 129, false),
        ] {
            let c: Config =
                serde_json::from_value(with_direct(config_value(dir.path(), shape), bytes, gets))
                    .unwrap();
            assert_eq!(validate_config(&c, shape).is_ok(), ok, "{bytes}/{gets}");
        }
        // Older format markers refuse by name, even with a valid serving mode present.
        for old in [
            "borsuk-cohere-native-baseline-config-v2",
            "borsuk-cohere-native-baseline-config-v1",
        ] {
            let mut value = good.clone();
            value["schema"] = json!(old);
            let c: Config = serde_json::from_value(value).unwrap();
            let error = validate_config(&c, shape).unwrap_err().to_string();
            assert!(error.contains("older formats refuse"), "{error}");
        }
        // A v2-shaped config (no serving field) fails to parse at all.
        let mut v2 = config_value(dir.path(), shape);
        v2["schema"] = json!("borsuk-cohere-native-baseline-config-v2");
        v2.as_object_mut().unwrap().remove("serving");
        assert!(serde_json::from_value::<Config>(v2).is_err());
    }
    #[tokio::test]
    async fn actual_direct_closure_queries_match_the_baseline_control_with_zero_source() {
        let shape = Shape::tiny(257);
        let (dir, base) = fixture(shape).await;
        let object_bytes = 257 * (D + 12);
        // Baseline control on the same store, unchanged by the serving field.
        let baseline_config: Config = serde_json::from_value(base.clone()).unwrap();
        let mut baseline_out = Output::create(&dir.path().join("baseline")).unwrap();
        let mut baseline_progress = Progress::default();
        query_and_seal(
            &baseline_config,
            shape,
            &mut baseline_out,
            &mut baseline_progress,
        )
        .await
        .unwrap();
        let baseline_rows = records(&dir.path().join("baseline"));
        let baseline_queries = baseline_rows
            .iter()
            .filter(|r| r["phase"] == "query")
            .collect::<Vec<_>>();
        assert_eq!(baseline_queries.len(), 2);
        assert!(baseline_queries.iter().all(|q| {
            q["serving"] == json!({"mode":"baseline"})
                && q["charges"]["source"]["verified_bytes"].as_u64().unwrap() > 0
                && q["source_nomination_skipped"] == false
                && q["planning_scope"] == "source_nomination_and_cover"
        }));

        let direct_value = with_direct(base, object_bytes, 2);
        let (path, sha) = write_config(dir.path(), &direct_value);
        let c = config(&path, &sha, shape).unwrap();
        let output = dir.path().join("direct");
        let mut out = Output::create(&output).unwrap();
        let mut p = Progress::default();
        let seal = query_and_seal(&c, shape, &mut out, &mut p).await.unwrap();
        let rows = records(&output);
        let admission = rows
            .iter()
            .position(|r| r["phase"] == "direct_admission")
            .unwrap();
        assert!(rows[..admission].iter().all(|r| r["phase"] != "query"));
        assert!(rows[admission..].iter().any(|r| r["phase"] == "query"));
        let record = &rows[admission];
        assert_eq!(record["admitted"], true);
        assert_eq!(
            record["limits"],
            json!({"mode":"direct_closure","max_sq8_bytes":object_bytes,"max_sq8_gets":2})
        );
        // Exact modeled memory: whole existing model plus one full direct query budget.
        let memory = &record["memory"];
        let planner = 1_048_576
            + 262_144
            + TwoBitPlanTrace::scratch_bytes(257) as u64
            + 4 * D as u64
            + 512 * 2;
        let ranking = 256 * 257 + 4 * D as u64;
        let direct = 3 * object_bytes as u64 + planner + ranking;
        assert_eq!(memory["direct_planner_bytes"], planner);
        assert_eq!(memory["direct_query_bytes"], direct);
        assert_eq!(
            memory["total_bytes"].as_u64().unwrap(),
            memory["resident_bytes"].as_u64().unwrap() + direct
        );
        assert_eq!(memory["cap_bytes"], MEMORY_CAP);
        assert!(memory["total_bytes"].as_u64().unwrap() <= MEMORY_CAP);
        let queries = rows
            .iter()
            .filter(|r| r["phase"] == "query")
            .collect::<Vec<_>>();
        assert_eq!(queries.len(), 2);
        let sq8 = std::fs::read(dir.path().join("sq8")).unwrap();
        let zero = json!({"submitted_gets":0,"verified_bytes":0,"failed_gets":0});
        for (i, query) in queries.iter().enumerate() {
            assert_eq!(query["serving"]["mode"], "direct_closure");
            assert_eq!(query["charges"]["source"], zero);
            assert_eq!(query["charges"]["router"], zero);
            assert_eq!(
                query["charges"]["sq8"],
                json!({"submitted_gets":1,"verified_bytes":object_bytes,"failed_gets":0})
            );
            assert_eq!(query["sum"]["submitted_gets"], 1);
            // Required closure vs fetched ranges: both pages, one contiguous range, no bridge.
            assert_eq!(query["plan"]["selected_pages"], json!([0, 1]));
            assert_eq!(query["plan"]["ranges"], json!([[0, object_bytes]]));
            assert_eq!(query["plan"]["planned_bytes"], object_bytes);
            assert_eq!(query["plan"]["covered_pages"], 2);
            assert_eq!(query["plan"]["bridge_pages"], 0);
            assert_eq!(query["source_nomination_skipped"], true);
            assert_eq!(query["planning_scope"], "direct_cover_only");
            // SOURCE stays skipped; the real cover time is reported under its own scope.
            assert!(
                query["stages"]["planning"]["end_ns"].as_u64().unwrap()
                    >= query["stages"]["planning"]["start_ns"].as_u64().unwrap()
            );
            assert_eq!(query["trace"]["ranked_candidate_pages"], json!([]));
            assert_eq!(query["trace"]["nomination_evaluated_units"], json!([]));
            assert_eq!(query["stages"]["source"]["start_ns"], 0);
            assert_eq!(query["stages"]["source"]["end_ns"], 0);
            // Independent scalar reference over every row of the one fetched range.
            let mut oracle = sq8
                .chunks_exact(D + 12)
                .enumerate()
                .map(|(physical, record)| {
                    let id = u64::from_le_bytes(record[..8].try_into().unwrap());
                    (physical, id, fixture_sq8_cosine_bits(record, i))
                })
                .collect::<Vec<_>>();
            oracle.sort_by(|a, b| {
                f32::from_bits(a.2)
                    .total_cmp(&f32::from_bits(b.2))
                    .then(a.1.cmp(&b.1))
            });
            assert_eq!(query["returned_count"], 10);
            assert_eq!(
                query["returned_physical_ordinals"],
                json!(oracle[..K].iter().map(|r| r.0).collect::<Vec<_>>())
            );
            assert_eq!(
                query["returned_physical_ordinals"],
                baseline_queries[i]["returned_physical_ordinals"]
            );
            for (rank, hit) in query["returned"].as_array().unwrap().iter().enumerate() {
                assert_eq!(hit["id"], oracle[rank].1);
                assert_eq!(hit["score_bits"], oracle[rank].2);
                // Same ranking and bits as the SOURCE-nominating control.
                assert_eq!(hit, &baseline_queries[i]["returned"][rank]);
            }
        }
        let summary = reduce(&c, &mut out, &seal, &mut p).unwrap();
        assert_eq!(summary["status"], "MEASURED");
        assert_eq!(summary["total_hits10"], 20);
        assert_eq!(summary["serving"]["mode"], "direct_closure");
        assert_eq!(
            summary["direct_memory"]["total_bytes"],
            memory["total_bytes"]
        );
        assert_eq!(summary["charges"]["source"], zero);
    }
    #[test]
    fn direct_resource_rejections_are_terminal_machine_readable_and_distinct_from_invalid() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let shape = Shape::tiny(257);
        let (dir, base) = runtime.block_on(fixture(shape));
        let object_bytes = 257 * (D + 12);
        assert_eq!(
            [Status::Measured, Status::Invalid, Status::ResourceReject].map(Status::exit_code),
            [0, 2, 3]
        );

        // Cap-minus-one on the planned cover: refused after local discovery, before any GET.
        let (status, rows) = run_status(
            dir.path(),
            "planned-bytes",
            &with_direct(base.clone(), object_bytes - 1, 2),
            shape,
        );
        assert_eq!(status, Status::ResourceReject);
        let terminal = &rows.last().unwrap()["summary"];
        assert_eq!(terminal["status"], "RESOURCE_REJECT");
        assert_eq!(terminal["resource_rejection"], "planned_bytes");
        assert_eq!(terminal["complete"], false);
        assert_eq!(terminal["completed_queries"], 0);
        assert_eq!(terminal["all_queries_sealed"], false);
        assert_eq!(terminal["truth_opened"], false);
        assert_eq!(terminal["host_oom_or_deadline_inferred"], false);
        assert_eq!(terminal["sum"]["submitted_gets"], 0);
        assert_eq!(terminal["sum"]["verified_bytes"], 0);
        let failure = rows.iter().find(|r| r["phase"] == "query_failure").unwrap();
        assert_eq!(failure["resource_rejection"], "planned_bytes");
        assert!(rows.iter().all(|r| {
            r["phase"] != "all_queries_sealed" && r["phase"] != "recall" && r["phase"] != "query"
        }));

        // Modeled memory over the cap: refused at startup, before any query.
        let (status, rows) = run_status(
            dir.path(),
            "modeled-memory",
            &with_direct(base.clone(), 200 * 1024 * 1024, 2),
            shape,
        );
        assert_eq!(status, Status::ResourceReject);
        let terminal = &rows.last().unwrap()["summary"];
        assert_eq!(terminal["status"], "RESOURCE_REJECT");
        assert_eq!(terminal["resource_rejection"], "modeled_memory");
        assert_eq!(terminal["stage"], "direct_admission");
        assert!(
            terminal["direct_memory"]["total_bytes"].as_u64().unwrap()
                > terminal["direct_memory"]["cap_bytes"].as_u64().unwrap()
        );
        let admission = rows
            .iter()
            .find(|r| r["phase"] == "direct_admission")
            .unwrap();
        assert_eq!(admission["admitted"], false);
        assert!(rows.iter().all(|r| {
            r["phase"] != "query"
                && r["phase"] != "query_failure"
                && r["phase"] != "all_queries_sealed"
        }));

        // Exact caps measure normally.
        let (status, rows) = run_status(
            dir.path(),
            "exact",
            &with_direct(base.clone(), object_bytes, 2),
            shape,
        );
        assert_eq!(status, Status::Measured);
        let terminal = &rows.last().unwrap()["summary"];
        assert_eq!(terminal["status"], "MEASURED");
        assert_eq!(terminal["serving"]["mode"], "direct_closure");

        // Malformed format, authentication and arithmetic defects stay INVALID, never a
        // resource outcome.
        let mut old = with_direct(base.clone(), object_bytes, 2);
        old["schema"] = json!("borsuk-cohere-native-baseline-config-v2");
        let mut wrong_root = with_direct(base.clone(), object_bytes, 2);
        wrong_root["generation_root_sha256"] = json!("0".repeat(64));
        let mut zero_cap = with_direct(base, object_bytes, 2);
        zero_cap["serving"]["max_sq8_gets"] = json!(0);
        for (name, value) in [("old", old), ("root", wrong_root), ("zero", zero_cap)] {
            let (status, rows) = run_status(dir.path(), name, &value, shape);
            assert_eq!(status, Status::Invalid, "{name}");
            let terminal = &rows.last().unwrap()["summary"];
            assert_eq!(terminal["status"], "INVALID", "{name}");
            assert!(terminal["resource_rejection"].is_null(), "{name}");
            assert!(rows.iter().all(|r| r["phase"] != "query"), "{name}");
        }
    }
    // ===== Diagnostic panel: selection, bounded lines, host counters, trace parity =====
    pub(super) fn diagnostic_value_heap_bound(value: &Value) -> usize {
        match value {
            Value::String(text) => text.capacity() + 32,
            Value::Array(rows) => {
                rows.capacity() * size_of::<Value>()
                    + 32
                    + rows.iter().map(diagnostic_value_heap_bound).sum::<usize>()
            }
            Value::Object(fields) => {
                // BTreeMap: <= one root plus one node per five non-root keys.
                let node =
                    64 + 11 * (size_of::<String>() + size_of::<Value>()) + 12 * size_of::<usize>();
                node * (1 + fields.len().saturating_sub(1) / 5)
                    + fields
                        .iter()
                        .map(|(key, value)| {
                            key.capacity() + 32 + diagnostic_value_heap_bound(value)
                        })
                        .sum::<usize>()
            }
            _ => 0,
        }
    }

    fn with_panel(mut value: Value, ordinals: &[usize], trace: bool) -> Value {
        value["execution"] = json!({"mode":"diagnostic_panel","ordinals":ordinals,"trace":trace});
        value
    }
    fn write_proc(root: &Path) {
        for (name, body) in [
            (
                "net/netstat",
                "TcpExt: SyncookiesSent TCPTimeouts TCPOFOQueue TCPFromZeroWindowAdv\nTcpExt: 0 7 11 13\nIpExt: InNoRoutes InOctets\nIpExt: 1 99\n",
            ),
            (
                "net/snmp",
                "Ip: Forwarding DefaultTTL\nIp: 1 64\nTcp: RtoAlgorithm ActiveOpens RetransSegs CurrEstab\nTcp: 1 5 17 3\n",
            ),
            (
                "stat",
                "cpu  100 0 200 3000 40 5 6 70 0 0\ncpu0 10 0 20 300 4 1 2 9 0 0\ncpu1 11 0 21 301 5 2 3 10 0 0\nintr 1 2 3\n",
            ),
            ("thread-self/schedstat", "123456 789 42\n"),
            (
                "thread-self/status",
                "Name:\tt\nvoluntary_ctxt_switches:\t21\nnonvoluntary_ctxt_switches:\t4\n",
            ),
            (
                "self/stat",
                "4242 (we ird)) S 1 2 3 4 5 6 777 8 99 10 0 0\n",
            ),
        ] {
            let path = root.join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, body).unwrap();
        }
    }
    fn use_proc(root: Option<&Path>) {
        PROC_ROOT.with(|cell| *cell.borrow_mut() = root.map(Path::to_path_buf));
    }
    fn counter(snapshot: &HostSnapshot, name: &str) -> Option<u64> {
        let slot = HOST_COUNTERS
            .iter()
            .position(|counter| counter.0 == name)
            .unwrap();
        snapshot.values[slot]
    }
    async fn run_panel(dir: &Path, name: &str, value: &Value, shape: Shape) -> (Vec<Value>, Value) {
        let c: Config = serde_json::from_value(value.clone()).unwrap();
        let output = dir.join(name);
        let mut out = Output::create(&output).unwrap();
        let mut p = Progress::default();
        let seal = query_and_seal(&c, shape, &mut out, &mut p).await.unwrap();
        assert!(p.sealed && !p.truth_opened);
        assert!(records(&output).iter().all(|r| r["truth_opened"] != true));
        let summary = reduce(&c, &mut out, &seal, &mut p).unwrap();
        (records(&output), summary)
    }

    #[test]
    fn fp32_score_precision_rejects_baseline_high_bits() {
        let baseline = Hit {
            id: 0,
            score_bits: u64::from(0.5_f32.to_bits()),
        };
        assert!(validate_hits(&[baseline], 33, false).is_ok());
        let high = Hit {
            id: 0,
            score_bits: (1_u64 << 32) | u64::from(0.5_f32.to_bits()),
        };
        assert!(validate_hits(&[high], 33, false).is_err());
        let exact = Hit {
            id: 0,
            score_bits: (0.5_f64 + f64::EPSILON).to_bits(),
        };
        assert!(validate_hits(&[exact], 33, true).is_ok());
    }
    #[test]
    fn execution_selector_is_explicit_strict_sorted_unique_and_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let good = config_value(dir.path(), Shape::PRODUCTION);
        let parsed: Config = serde_json::from_value(good.clone()).unwrap();
        assert_eq!(parsed.execution, Execution::Full {});
        validate_config(&parsed, Shape::PRODUCTION).unwrap();
        let mut missing = good.clone();
        missing.as_object_mut().unwrap().remove("execution");
        assert!(serde_json::from_value::<Config>(missing).is_err());
        for bad in [
            json!({"mode":"full","ordinals":[0]}),
            json!({"mode":"full","trace":true}),
            json!({"mode":"diagnostic_panel","ordinals":[0]}),
            json!({"mode":"diagnostic_panel","trace":false}),
            json!({"mode":"diagnostic_panel","ordinals":[0],"trace":false,"x":0}),
            json!({"mode":"diagnostic_panel","ordinals":[-1],"trace":false}),
            json!({"mode":"diagnostic_panel","ordinals":[1.5],"trace":false}),
            json!({"mode":"diagnostic_panel","ordinals":"0","trace":false}),
            json!({"mode":"diagnostic_panel","ordinals":[0],"trace":"yes"}),
            json!({"mode":"panel","ordinals":[0],"trace":false}),
            json!({"ordinals":[0],"trace":false}),
            json!("full"),
            json!(null),
        ] {
            let mut value = good.clone();
            value["execution"] = bad.clone();
            assert!(serde_json::from_value::<Config>(value).is_err(), "{bad}");
        }
        let exact: Vec<usize> = (872..1000).collect();
        let over: Vec<usize> = (0..129).collect();
        for (ordinals, ok) in [
            (vec![0], true),
            (vec![999], true),
            (vec![0, 1, 999], true),
            (exact, true),
            (vec![], false),
            (vec![1000], false),
            (vec![5, 5], false),
            (vec![5, 4], false),
            (vec![0, 1000], false),
            (over, false),
            (vec![usize::MAX], false),
        ] {
            let c: Config =
                serde_json::from_value(with_panel(good.clone(), &ordinals, true)).unwrap();
            assert_eq!(
                validate_config(&c, Shape::PRODUCTION).is_ok(),
                ok,
                "{ordinals:?}"
            );
        }
        for old in [
            "borsuk-cohere-native-baseline-config-v14",
            "borsuk-cohere-native-baseline-config-v3",
            "borsuk-cohere-native-baseline-config-v2",
        ] {
            let mut value = with_panel(good.clone(), &[0], false);
            value["schema"] = json!(old);
            let c: Config = serde_json::from_value(value).unwrap();
            let error = validate_config(&c, Shape::PRODUCTION)
                .unwrap_err()
                .to_string();
            assert!(error.contains("older formats refuse"), "{error}");
        }
    }

    #[test]
    fn cold_source_profile_fp32_trace_admission() {
        let dir = tempfile::tempdir().unwrap();
        let mut value = with_panel(config_value(dir.path(), Shape::PRODUCTION), &[0], true);
        value["profile"] = json!("scale1m");
        value["semantic_leaf_limit"] = json!(64);
        value["fetch_parallelism"] = json!(128);
        value["source_authentication"] = json!("offload2");
        value["serving"] = json!({"mode":"fp32_rerank","coalesce":false,"shortlist":16,
            "max_fp32_bytes":16*(D*4+8),"max_fp32_gets":16,"max_parallel_fp32_gets":16});
        let c: Config = serde_json::from_value(value).unwrap();
        validate_config(&c, Shape::PRODUCTION).unwrap();
        let bytes = diagnostic_bytes(&c).unwrap();
        eprintln!(
            "cold-source-profile pinned={} trace_peak={} total={} cap={}",
            diagnostic_pinned_bytes(&c).unwrap(),
            trace_peak_bytes(&c),
            bytes,
            DIAGNOSTIC_CAP
        );
        assert!(bytes <= DIAGNOSTIC_CAP);
    }

    #[tokio::test]
    async fn cold_source_profile_cap_refuses_before_receipts_requests_or_backend() {
        let dir = tempfile::tempdir().unwrap();
        let mut value = with_panel(config_value(dir.path(), Shape::PRODUCTION), &[0], true);
        value["profile"] = json!("scale1m");
        value["semantic_leaf_limit"] = json!(64);
        value["fetch_parallelism"] = json!(128);
        value["sq8_get_budget"] = json!(128);
        value["serving"] = json!({"mode":"fp32_rerank","coalesce":false,"shortlist":256,
            "max_fp32_bytes":256*(D*4+8),"max_fp32_gets":256,"max_parallel_fp32_gets":128});
        let c: Config = serde_json::from_value(value).unwrap();
        validate_config(&c, Shape::PRODUCTION).unwrap();
        let mut out = Output::create(&dir.path().join("cap-refusal")).unwrap();
        let mut progress = Progress::default();
        let error = query_and_seal_with(&c, Shape::PRODUCTION, &mut out, &mut progress, || {
            panic!("backend must remain unopened")
        })
        .await
        .err()
        .unwrap()
        .to_string();
        assert!(error.contains("diagnostic budget exceeds 2 MiB"), "{error}");
        assert_eq!(out.bytes, 0);
        assert!(!progress.truth_opened);
    }

    #[test]
    fn cold_source_profile_proof_value_capacity_bound() {
        let dir = tempfile::tempdir().unwrap();
        for shortlist in [16, 256] {
            let mut value = with_panel(config_value(dir.path(), Shape::PRODUCTION), &[0], true);
            value["serving"] = json!({"mode":"fp32_rerank","coalesce":false,"shortlist":shortlist,
                "max_fp32_bytes":shortlist*(D*4+8),"max_fp32_gets":shortlist,"max_parallel_fp32_gets":16});
            let c: Config = serde_json::from_value(value).unwrap();
            let proof = json!({"score_precision":"f64","wall_ns":u64::MAX,"record_bytes":D*4+8,
                "ranges":vec![[usize::MAX;2];shortlist],"covered_records":u64::MAX,"bridge_records":u64::MAX,
                "shortlist":(0..shortlist).map(|_| json!({"id":i64::MAX,"ordinal":usize::MAX,
                    "sq8_score_bits":u32::MAX})).collect::<Vec<_>>()});
            let peak = 2 * (size_of::<Value>() + diagnostic_value_heap_bound(&proof))
                + shortlist * size_of::<[usize; 2]>()
                + 32;
            assert!(
                peak <= diagnostic_transient_bytes(&c).unwrap(),
                "{shortlist}: {peak}"
            );
            eprintln!(
                "proof shortlist={shortlist} actual_capacity_bound={peak} reserved={}",
                diagnostic_transient_bytes(&c).unwrap()
            );
        }
    }

    #[test]
    fn diagnostic_budget_is_charged_before_open_per_active_query_and_bounded_by_two_mebibytes() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::PRODUCTION;
        let full: Config = serde_json::from_value(config_value(dir.path(), shape)).unwrap();
        assert_eq!(diagnostic_bytes(&full).unwrap(), 0);
        assert_eq!(
            limits(&full).unwrap().already_pinned_bytes,
            caller_bytes(&full) as u64
        );
        for (direct, trace, parallelism) in [
            (false, false, 16),
            (false, true, 16),
            (false, true, 64),
            (true, false, 32),
            (true, true, 32),
        ] {
            let mut value = with_panel(config_value(dir.path(), shape), &[1, 5, 999], trace);
            value["fetch_parallelism"] = json!(parallelism);
            if direct {
                value = with_direct(value, 1_000_000, 128);
            }
            let c: Config = serde_json::from_value(value).unwrap();
            validate_config(&c, shape).unwrap();
            let bytes = diagnostic_bytes(&c).unwrap();
            assert!(bytes <= DIAGNOSTIC_CAP, "{bytes}");
            let lines = 2 * PANEL_LINE_CAP + diagnostic_transient_bytes(&c).unwrap();
            if trace {
                let ranges = if direct { 128 } else { BASELINE_QUERY_GETS };
                assert_eq!(trace_ranges(&c), ranges);
                assert_eq!(
                    bytes,
                    lines
                        + QueryProfile::modeled_bytes(
                            source_trace_ranges(&c),
                            ranges,
                            parallelism,
                            parallelism
                        )
                        + size_of::<HostSampler>()
                        + HOST_READ_CAP
                        + 1
                        + HOST_JSON_BYTES
                        + FAILURE_VALUE_BYTES
                );
            } else {
                assert_eq!(bytes, lines);
            }
            // The runner pins its own buffers; the library holds scheduler state at open
            // and adds retained spans/probes only when a traced query is admitted.
            let peak = if trace {
                QueryProfile::modeled_bytes(
                    source_trace_ranges(&c),
                    trace_ranges(&c),
                    parallelism,
                    parallelism,
                )
            } else {
                0
            };
            assert_eq!(trace_peak_bytes(&c), peak);
            assert_eq!(diagnostic_pinned_bytes(&c).unwrap(), bytes - peak);
            let admission = limits(&c).unwrap();
            assert_eq!(admission.max_active_queries, 1);
            assert_eq!(
                admission.already_pinned_bytes,
                caller_bytes(&c) as u64
                    + (bytes - peak) as u64 * admission.max_active_queries as u64
            );
            assert_eq!(
                range_scheduler_bytes(&c).unwrap(),
                Sq8RangeTrace::pending_futures_bytes(c.source_get_budget, parallelism).max(
                    Sq8RangeTrace::pending_futures_bytes(trace_ranges(&c), parallelism)
                )
            );
        }
    }

    #[test]
    fn source_diagnostic_refuses_overflow_before_open() {
        let dir = tempfile::tempdir().unwrap();
        let value = with_panel(config_value(dir.path(), Shape::PRODUCTION), &[0], true);
        let mut c: Config = serde_json::from_value(value).unwrap();
        c.source_get_budget = usize::MAX;
        let error = diagnostic_bytes(&c).unwrap_err().to_string();
        assert!(error.contains("overflow"), "{error}");
        assert_eq!(pinned_bytes(&c).unwrap_err().to_string(), error);
    }

    #[test]
    fn worst_case_diagnostic_object_fits_the_panel_line_with_room_for_the_record() {
        use borsuk::sq8_s3_range::RangeSpan;
        let dir = tempfile::tempdir().unwrap();
        let value = with_direct(
            with_panel(config_value(dir.path(), Shape::PRODUCTION), &[0], true),
            1_000_000,
            128,
        );
        let c: Config = serde_json::from_value(value).unwrap();
        let mut diag = Diag::new(&c).unwrap();
        let max = Some(u64::MAX);
        let span = RangeSpan {
            index: u32::MAX,
            first_page: u64::MAX,
            last_page: u64::MAX,
            start_byte: u64::MAX,
            end_byte: u64::MAX,
            first_poll_ns: max,
            request_ns: max,
            headers_ns: max,
            metadata_ns: max,
            first_chunk_ns: max,
            last_chunk_ns: max,
            eof_ns: max,
            auth_start_ns: max,
            auth_end_ns: max,
            complete_ns: max,
            chunks: u32::MAX,
            body_bytes: u64::MAX,
            max_body_gap_ns: u64::MAX,
            max_body_gap_end_ns: max,
            copy_ns: u64::MAX,
            copy_count: u32::MAX,
            outcome: "store_headers",
        };
        diag.sampler.before = HostSnapshot {
            monotonic_ns: max,
            values: [Some(0); HOST_COUNTERS.len()],
        };
        diag.sampler.after = HostSnapshot {
            monotonic_ns: max,
            values: [max; HOST_COUNTERS.len()],
        };
        let anchors = Anchors {
            monotonic_start_ns: max,
            monotonic_end_ns: max,
            process_cpu_start_ns: i128::from(u64::MAX),
            process_cpu_end_ns: i128::from(u64::MAX),
        };
        // The trace's span buffer is private and read-only, so the object is measured as
        // its span-free envelope plus n worst-case rows (and the n - 1 commas between them).
        assert!(diag.trace.sq8.ranges().is_empty());
        let scope = Some(borsuk::sq8_s3_range::CpuWall {
            thread_cpu_ns: u64::MAX,
            wall_ns: u64::MAX,
            clock_reads: u64::MAX,
        });
        diag.trace.discovery = scope;
        diag.trace.cover = scope;
        diag.trace.planning = scope;
        for trace in [&mut diag.trace.source, &mut diag.trace.sq8] {
            trace.outcome = "rejected_before_io";
            trace.planned_ranges = u32::MAX;
            trace.planned_bytes = u64::MAX;
            trace.max_parallel = u32::MAX;
            trace.rank_start_ns = max;
            trace.rank_end_ns = max;
            trace.rank = Some(borsuk::returned_sq8::RankPhases {
                ranges: u32::MAX,
                rows: u64::MAX,
                score_ns: u64::MAX,
                roster_ns: u64::MAX,
                sort_ns: u64::MAX,
                finish_ns: u64::MAX,
            });
            trace.cpu = borsuk::sq8_s3_range::RangeCpu {
                source_scoring: scope,
                collector_allocation: scope,
                collector_copy: scope,
                owned_allocation: scope,
                owned_copy: scope,
                authentication: scope,
                collector_copied_bytes: u64::MAX,
                owned_copied_bytes: u64::MAX,
            };
            trace.regroup = scope;
            trace.regroup_copied_bytes = u64::MAX;
            trace.rank_thread_cpu_ns = max;
            trace.rank_clock_reads = u64::MAX;
            trace.retained_payload_bytes = max;
            trace.phase_process_cpu_ns = max;
            trace.unattributed_process_cpu_ns = max;
            trace.phase_clock_reads = u64::MAX;
            trace.fetch_start_ns = max;
            trace.all_ranges_complete_ns = max;
            trace.release_end_ns = max;
        }
        let mut diagnostic = serde_json::to_value(diag.query(&anchors)).unwrap();
        // Both reservations remain private: fill their serialized arrays with exact span rows.
        for name in ["source_range_trace", "sq8_range_trace"] {
            assert!(diagnostic[name]["ranges"].as_array().unwrap().is_empty());
            diagnostic[name]["ranges"] = serde_json::to_value(vec![span; 128]).unwrap();
        }
        let worst = serde_json::to_vec(&diagnostic).unwrap().len();
        assert!(
            worst > 64 * 1024,
            "the former cap cannot carry both traces: {worst}"
        );
        assert!(worst + 8192 < PANEL_LINE_CAP, "{worst}");
        // Bound the failed query's materialized Value tree, including allocation allowances.
        let failure_tree = size_of::<Value>() + diagnostic_value_heap_bound(&diagnostic);
        assert!(failure_tree <= FAILURE_VALUE_BYTES, "{failure_tree}");
        eprintln!(
            "diagnostic bounds: maximal_serialized_bytes={worst} failure_value_heap_bound={failure_tree} panel_line_cap={PANEL_LINE_CAP} failure_value_allowance={FAILURE_VALUE_BYTES}"
        );
        let mut out = Output::create(&dir.path().join("maximal-diagnostic")).unwrap();
        out.admit_panel_lines(PANEL_LINE_CAP);
        let pointer = out.panel.as_ref().unwrap().as_ptr();
        emit_failure(
            &mut out,
            json!({"phase":"query_failure","ordinal":7,
                "error":"\u{0000}".repeat(PANEL_ERROR_CHARS),"truth_opened":false}),
            Some(diagnostic.clone()),
        )
        .unwrap();
        let rows = records(&out.path);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]["diagnostic"], diagnostic,
            "all SOURCE/SQ8 spans survive failure output"
        );
        assert_eq!(
            rows[0]["error"].as_str().unwrap().chars().count(),
            PANEL_ERROR_CHARS
        );
        let published = out.bytes;
        out.panel_cap = worst / 2;
        emit_failure(
            &mut out,
            json!({"phase":"query_failure","ordinal":8,"error":"original failure"}),
            Some(diagnostic),
        )
        .unwrap();
        assert!(out.bytes > published);
        let rows = records(&out.path);
        assert_eq!(rows[1]["error"], "original failure");
        assert_eq!(rows[1]["diagnostic"]["omitted"], "serialized_line_cap");
        assert_eq!(rows[1]["diagnostic"]["cap_bytes"], worst / 2);
        assert_eq!(out.panel.as_ref().unwrap().as_ptr(), pointer);
    }

    #[test]
    fn host_counters_parse_named_values_and_report_missing_wrapped_or_oversize_as_null() {
        let dir = tempfile::tempdir().unwrap();
        write_proc(dir.path());
        let mut buffer = Vec::with_capacity(HOST_READ_CAP + 1);
        let snap = snapshot(dir.path(), &mut buffer);
        assert!(snap.monotonic_ns.is_some());
        for (name, expected) in [
            ("TcpExt.TCPTimeouts", Some(7)),
            ("TcpExt.TCPOFOQueue", Some(11)),
            ("TcpExt.TCPFromZeroWindowAdv", Some(13)),
            ("TcpExt.TCPFastRetrans", None),
            ("Tcp.ActiveOpens", Some(5)),
            ("Tcp.RetransSegs", Some(17)),
            ("Tcp.InSegs", None),
            ("cpu0.user_ticks", Some(10)),
            ("cpu0.system_ticks", Some(20)),
            ("cpu0.irq_ticks", Some(1)),
            ("cpu0.softirq_ticks", Some(2)),
            ("cpu0.steal_ticks", Some(9)),
            ("cpu.irq_ticks", Some(5)),
            ("cpu.softirq_ticks", Some(6)),
            ("cpu.steal_ticks", Some(70)),
            ("thread.sched_run_ns", Some(123_456)),
            ("thread.sched_wait_ns", Some(789)),
            ("thread.sched_timeslices", Some(42)),
            ("thread.voluntary_ctxt_switches", Some(21)),
            ("thread.nonvoluntary_ctxt_switches", Some(4)),
            ("process.minor_faults", Some(777)),
            ("process.major_faults", Some(99)),
        ] {
            assert_eq!(counter(&snap, name), expected, "{name}");
        }
        let mut names = HOST_COUNTERS.iter().map(|c| c.0).collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), HOST_COUNTERS.len());
        assert!(buffer.capacity() <= 2 * (HOST_READ_CAP + 1));
        // Nothing readable: every counter is explicitly unavailable, never zero.
        let empty = tempfile::tempdir().unwrap();
        let none = snapshot(empty.path(), &mut buffer);
        assert!(none.monotonic_ns.is_some());
        assert!(none.values.iter().all(Option::is_none));
        // Over the read cap is unavailable, not truncated; other sources are unaffected.
        std::fs::write(
            dir.path().join("net/netstat"),
            vec![b' '; HOST_READ_CAP + 1],
        )
        .unwrap();
        let over = snapshot(dir.path(), &mut buffer);
        assert_eq!(counter(&over, "TcpExt.TCPTimeouts"), None);
        assert_eq!(counter(&over, "Tcp.ActiveOpens"), Some(5));
        let mut at_cap = b"TcpExt: TCPTimeouts\nTcpExt: 3\n".to_vec();
        at_cap.resize(HOST_READ_CAP, b' ');
        std::fs::write(dir.path().join("net/netstat"), at_cap).unwrap();
        assert_eq!(
            counter(&snapshot(dir.path(), &mut buffer), "TcpExt.TCPTimeouts"),
            Some(3)
        );
        // Deltas: only a monotone pair with both ends present is reported.
        let cases = [
            (Some(5), Some(9), Some(4)),
            (Some(9), Some(5), None),
            (None, Some(5), None),
            (Some(5), None, None),
            (Some(7), Some(7), Some(0)),
            (Some(0), Some(u64::MAX), Some(u64::MAX)),
        ];
        let (mut before, mut after) = (HostSnapshot::EMPTY, HostSnapshot::EMPTY);
        before.monotonic_ns = Some(10);
        after.monotonic_ns = Some(30);
        for (slot, case) in cases.iter().enumerate() {
            before.values[slot] = case.0;
            after.values[slot] = case.1;
        }
        let window = host_window(&before, &after);
        for (slot, case) in cases.iter().enumerate() {
            assert_eq!(window.deltas[HOST_COUNTERS[slot].0], case.2, "{slot}");
        }
        let reported = cases.iter().filter(|case| case.2.is_some()).count();
        assert_eq!(window.unavailable, HOST_COUNTERS.len() - reported);
        assert_eq!(window.deltas.len(), HOST_COUNTERS.len());
        assert_eq!(
            (window.before_monotonic_ns, window.after_monotonic_ns),
            (Some(10), Some(30))
        );
    }

    #[tokio::test]
    async fn panel_executes_original_ordinals_and_indexes_truth_by_original_ordinal() {
        let shape = Shape {
            count: 5,
            ..Shape::tiny(257)
        };
        let (dir, base) = fixture(shape).await;
        let proc = dir.path().join("proc");
        write_proc(&proc);
        use_proc(Some(proc.as_path()));
        for (index, trace) in [false, true].into_iter().enumerate() {
            let value = with_panel(base.clone(), &[1, 3, 4], trace);
            let (rows, summary) =
                run_panel(dir.path(), &format!("panel-{index}"), &value, shape).await;
            let phases = rows
                .iter()
                .map(|r| r["phase"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            let first = |phase: &str| phases.iter().position(|p| p == phase).unwrap();
            let last = |phase: &str| phases.iter().rposition(|p| p == phase).unwrap();
            assert!(first("diagnostic_admission") < first("source_binding"));
            assert!(last("query") < first("all_queries_sealed"));
            assert!(first("all_queries_sealed") < first("recall"));
            let queries = rows
                .iter()
                .filter(|r| r["phase"] == "query")
                .collect::<Vec<_>>();
            assert_eq!(
                queries
                    .iter()
                    .map(|q| q["ordinal"].as_u64().unwrap())
                    .collect::<Vec<_>>(),
                [1, 3, 4]
            );
            assert_eq!(
                queries
                    .iter()
                    .map(|q| q["selected_slot"].as_u64().unwrap())
                    .collect::<Vec<_>>(),
                [0, 1, 2]
            );
            for query in &queries {
                let ordinal = query["ordinal"].as_u64().unwrap() as usize;
                let ids = query["returned"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|hit| hit["id"].as_u64().unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(ids, TRUTH_IDS[ordinal % 2]);
            }
            // Ordinals 1 and 3 are different request rows with identical results: two records.
            assert_eq!(queries[0]["returned"], queries[1]["returned"]);
            assert_ne!(queries[0]["returned"], queries[2]["returned"]);
            let seal = rows
                .iter()
                .find(|r| r["phase"] == "all_queries_sealed")
                .unwrap();
            assert_eq!(seal["count"], 3);
            assert_eq!(seal["selected_count"], 3);
            assert_eq!(seal["population_count"], 5);
            assert_eq!(seal["truth_opened"], false);
            let admission = rows
                .iter()
                .find(|r| r["phase"] == "diagnostic_admission")
                .unwrap();
            assert_eq!(admission["selected_count"], 3);
            assert_eq!(admission["population_count"], 5);
            assert_eq!(admission["trace"], trace);
            assert_eq!(admission["population_percentiles_valid"], false);
            assert_eq!(admission["full_cohort_qualification"], false);
            assert!(
                admission["diagnostic_bytes_per_active_query"]
                    .as_u64()
                    .unwrap()
                    <= DIAGNOSTIC_CAP as u64
            );
            // Retained trace bytes, cumulative drain peak and both-paths range state are reported
            // as three separate figures; the trace ones exist only when tracing is on.
            assert_eq!(
                admission["range_state_both_paths_bytes"],
                Sq8RangeTrace::pending_futures_bytes(128, 16)
            );
            assert_eq!(admission["range_scheduler_owner"], "library");
            assert_eq!(
                admission["range_state_charged_by_library_bytes"],
                Sq8RangeTrace::pending_futures_bytes(128, 16)
            );
            assert!(
                admission
                    .get("range_state_pinned_by_runner_bytes")
                    .is_none()
            );
            if trace {
                assert_eq!(
                    admission["trace_retained_bytes"],
                    QueryProfile::retained_bytes(BASELINE_SOURCE_GETS, BASELINE_QUERY_GETS)
                );
                assert_eq!(
                    admission["trace_peak_bytes"],
                    QueryProfile::modeled_bytes(BASELINE_SOURCE_GETS, BASELINE_QUERY_GETS, 16, 16)
                );
            } else {
                assert!(admission["trace_retained_bytes"].is_null());
                assert!(admission["trace_peak_bytes"].is_null());
            }
            // Recall rows and the denominator follow the selected set; a selection-position
            // truth lookup would score ordinal 1 against truth row 0 and miss nine hits.
            let recalls = rows
                .iter()
                .filter(|r| r["phase"] == "recall")
                .map(|r| r["ordinal"].as_u64().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(recalls, [1, 3, 4]);
            assert_eq!(summary["queries"], 3);
            assert_eq!(summary["selected_count"], 3);
            assert_eq!(summary["executed_count"], 3);
            assert_eq!(summary["population_count"], 5);
            assert_eq!(summary["recall_denominator"], 30);
            assert_eq!(summary["total_hits10"], 30);
            assert_eq!(summary["mean_recall10"], 1.0);
            assert_eq!(summary["diagnostic_panel"], true);
            assert_eq!(summary["population_percentiles_valid"], false);
            assert_eq!(summary["full_cohort_qualification"], false);
        }
        use_proc(None);
    }

    #[tokio::test]
    async fn panel_authenticates_every_original_request_row_before_any_open() {
        let shape = Shape {
            count: 5,
            ..Shape::tiny(32)
        };
        let (dir, base) = fixture(shape).await;
        let good = std::fs::read(dir.path().join("requests.f32")).unwrap();
        let mut zero_row = good.clone();
        zero_row[2 * D * 4..3 * D * 4].fill(0);
        let mut nan_tail = good.clone();
        nan_tail[4 * D * 4..4 * D * 4 + 4].copy_from_slice(&f32::NAN.to_le_bytes());
        for (name, bytes, row) in [("zero", zero_row, 2), ("nan-tail", nan_tail, 4)] {
            let mut value = with_panel(base.clone(), &[0, 1], true);
            value["requests"] = artifact(&dir.path().join(format!("bad-{name}")), &bytes);
            fixture_receipts(dir.path(), &mut value);
            let c: Config = serde_json::from_value(value).unwrap();
            validate_receipts(&c).unwrap();
            let mut out = Output::create(&dir.path().join(format!("early-{name}"))).unwrap();
            let mut p = Progress::default();
            let opened = std::cell::Cell::new(false);
            let error = query_and_seal_with(&c, shape, &mut out, &mut p, || {
                opened.set(true);
                Reader::new(&c.backend, c.s3_read_retry)
            })
            .await
            .err()
            .unwrap()
            .to_string();
            assert!(error.contains(&format!("request {row}")), "{name}: {error}");
            assert!(!opened.get(), "{name}");
            assert_eq!((p.completed, p.sealed, p.truth_opened), (0, false, false));
            assert_eq!(
                out.bytes, 0,
                "{name}: nothing is published before this check"
            );
        }
    }

    #[tokio::test]
    async fn panel_selector_refusals_precede_every_open_and_publication() {
        let shape = Shape {
            count: 5,
            ..Shape::tiny(32)
        };
        let (dir, base) = fixture(shape).await;
        for (index, ordinals) in [vec![], vec![5], vec![2, 2], vec![3, 2], vec![0, 7]]
            .into_iter()
            .enumerate()
        {
            let value = with_panel(base.clone(), &ordinals, index % 2 == 0);
            let c: Config = serde_json::from_value(value).unwrap();
            let mut out = Output::create(&dir.path().join(format!("refused-{index}"))).unwrap();
            let mut p = Progress::default();
            let opened = std::cell::Cell::new(false);
            let error = query_and_seal_with(&c, shape, &mut out, &mut p, || {
                opened.set(true);
                Reader::new(&c.backend, c.s3_read_retry)
            })
            .await
            .err()
            .unwrap()
            .to_string();
            assert!(error.contains("sorted unique original ordinals"), "{error}");
            assert!(!opened.get(), "{ordinals:?}");
            assert_eq!((p.completed, p.sealed, p.truth_opened), (0, false, false));
            assert_eq!(out.bytes, 0);
        }
    }

    #[tokio::test]
    async fn source_diagnostic_borrows_all_ranges_in_exact_plan_and_timestamp_order() {
        let shape = Shape::tiny(257);
        let (_dir, value) = fixture(shape).await;
        let c: Config = serde_json::from_value(with_panel(value, &[0], true)).unwrap();
        let reader = Reader::new(&c.backend, c.s3_read_retry).unwrap();
        let store = reader.store();
        let generation = TwoBitGeneration::open_remote(
            store,
            &ObjectPath::from(c.generation_prefix.clone()),
            &c.generation_root_sha256,
            limits(&c).unwrap(),
            &c.scratch_parent,
        )
        .await
        .unwrap();
        let mut diag = Diag::new(&c).unwrap();
        let (result, _) = generation
            .diagnostic_search_with_store_traced(store, &fixture_direction(0), K, &mut diag.trace)
            .await
            .unwrap();
        let anchors = Anchors {
            monotonic_start_ns: None,
            monotonic_end_ns: None,
            process_cpu_start_ns: 0,
            process_cpu_end_ns: 0,
        };
        let view = diag.query(&anchors);
        assert!(std::ptr::eq(view.source_range_trace, &diag.trace.source));
        let source = serde_json::to_value(&view).unwrap()["source_range_trace"].clone();
        let spans = diag.trace.source.ranges();
        assert!(!spans.is_empty());
        assert_eq!(spans.len(), result.source_stats.submitted_gets);
        assert_eq!(source["planned_ranges"], spans.len());
        assert_eq!(source["dropped_ranges"], 0);
        assert_eq!(
            source["range_fields"],
            serde_json::to_value(borsuk::sq8_s3_range::RangeSpan::FIELDS).unwrap()
        );
        assert_eq!(source["ranges"], serde_json::to_value(spans).unwrap());
        for (index, span) in spans.iter().enumerate() {
            assert_eq!(span.index as usize, index);
            assert!(span.auth_start_ns.is_some() && span.auth_end_ns.is_some());
            assert_eq!(source["ranges"][index], serde_json::to_value(span).unwrap());
        }
        assert!(view.cpu_profile.source.ranges().as_ptr() == spans.as_ptr());
    }

    #[tokio::test]
    async fn panel_trace_toggle_changes_only_the_diagnostic_field_for_baseline_and_direct() {
        let shape = Shape::tiny(257);
        let (dir, base) = fixture(shape).await;
        let object_bytes = 257 * (D + 12);
        let proc = dir.path().join("proc");
        write_proc(&proc);
        use_proc(Some(proc.as_path()));
        let strip = |query: &Value| {
            let mut query = query.clone();
            let fields = query.as_object_mut().unwrap();
            for timing in [
                "diagnostic",
                "stages",
                "query_wall_ns",
                "query_process_cpu_ns",
            ] {
                fields.remove(timing);
            }
            query
        };
        for (mode, value) in [
            ("baseline", base.clone()),
            ("direct", with_direct(base.clone(), object_bytes, 2)),
        ] {
            let mut runs = Vec::new();
            for trace in [false, true] {
                let panel = with_panel(value.clone(), &[0, 1], trace);
                let name = format!("toggle-{mode}-{trace}");
                runs.push(run_panel(dir.path(), &name, &panel, shape).await);
            }
            let queries = |run: usize| {
                runs[run]
                    .0
                    .iter()
                    .filter(|r| r["phase"] == "query")
                    .collect::<Vec<_>>()
            };
            let (off, on) = (queries(0), queries(1));
            assert_eq!((off.len(), on.len()), (2, 2));
            for (off, on) in off.iter().zip(&on) {
                // Returned IDs and score bits, charges, plans, GETs, bytes and the plan
                // trace are identical; only timing and the new diagnostic field differ.
                assert_eq!(strip(*off), strip(*on), "{mode}");
                assert!(off.get("diagnostic").is_none());
                assert!(on["returned_count"].as_u64().unwrap() >= 1);
            }
            for key in [
                "charges",
                "sum",
                "total_hits10",
                "serving",
                "recall_denominator",
            ] {
                assert_eq!(runs[0].1[key], runs[1].1[key], "{mode} {key}");
            }
            for query in &on {
                let diag = &query["diagnostic"];
                assert_eq!(diag["schema"], DIAGNOSTIC_SCHEMA);
                assert_eq!(diag["schema"], "borsuk-sq8-range-attribution-diagnostic-v6");
                assert_eq!(
                    diag["cpu_profile"]["acquisition_poll_cpu"],
                    "unresolved_not_instrumented"
                );
                let source = &diag["source_range_trace"];
                let source_ranges = source["ranges"].as_array().unwrap();
                let source_fields = source["range_fields"].as_array().unwrap();
                assert_eq!(source_fields.len(), 22);
                assert_eq!(source["dropped_ranges"], 0);
                assert_eq!(source["planned_ranges"], source_ranges.len());
                assert_eq!(
                    source_ranges.len() as u64,
                    query["charges"]["source"]["submitted_gets"]
                        .as_u64()
                        .unwrap()
                );
                assert!(source.get("last_completing_range").is_none());
                assert!(diag["cpu_profile"]["discovery"].is_object());
                assert!(diag["cpu_profile"]["cover"].is_object());
                if mode == "direct" {
                    assert!(source_ranges.is_empty());
                    assert_eq!(source["outcome"], "started");
                    assert!(diag["source_range_trace"]["phase_process_cpu_ns"].is_null());
                    assert!(diag["cpu_profile"]["planning"].is_null());
                } else {
                    assert!(!source_ranges.is_empty());
                    let at = |name: &str| source_fields.iter().position(|f| f == name).unwrap();
                    let mut previous_end = 0;
                    for (index, row) in source_ranges.iter().enumerate() {
                        assert_eq!(row[at("index")], index);
                        let start = row[at("start_byte")].as_u64().unwrap();
                        let end = row[at("end_byte")].as_u64().unwrap();
                        assert!(previous_end <= start && start < end);
                        previous_end = end;
                        assert_eq!(row[at("outcome")], "ok");
                        assert_eq!(row[at("body_bytes")], end - start);
                        let chain = [
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
                        ]
                        .map(|name| row[at(name)].as_u64().expect(name));
                        assert!(chain.windows(2).all(|w| w[0] <= w[1]), "{chain:?}");
                        assert!(source["fetch_start_ns"].as_u64().unwrap() <= chain[0]);
                        assert!(chain[9] <= source["all_ranges_complete_ns"].as_u64().unwrap());
                    }
                    assert!(diag["source_range_trace"]["cpu"]["authentication"].is_object());
                    assert!(diag["cpu_profile"]["planning"].is_object());
                }
                let trace = &diag["sq8_range_trace"];
                let fields = trace["range_fields"].as_array().unwrap();
                assert_eq!(fields.len(), 22);
                let at = |name: &str| fields.iter().position(|f| f == name).unwrap();
                let ranges = trace["ranges"].as_array().unwrap();
                let planned = query["plan"]["ranges"].as_array().unwrap();
                assert_eq!(ranges.len(), planned.len());
                assert_eq!(query["plan"]["logical_range_count"], planned.len());
                assert_eq!(
                    ranges.len() as u64,
                    query["charges"]["sq8"]["submitted_gets"].as_u64().unwrap()
                );
                assert_eq!(trace["planned_ranges"], planned.len());
                assert_eq!(trace["dropped_ranges"], 0);
                assert_eq!(trace["outcome"], "ranked");
                let mut latest = 0;
                for (i, row) in ranges.iter().enumerate() {
                    let row = row.as_array().unwrap();
                    assert_eq!(row.len(), fields.len());
                    assert_eq!(row[at("index")], i);
                    assert_eq!(row[at("start_byte")], planned[i][0]);
                    assert_eq!(row[at("end_byte")], planned[i][1]);
                    assert_eq!(row[at("outcome")], "ok");
                    let chain = [
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
                    ]
                    .map(|name| row[at(name)].as_u64().expect(name));
                    assert!(chain.windows(2).all(|w| w[0] <= w[1]), "{chain:?}");
                    latest = latest.max(chain[9]);
                    assert!(row[at("chunks")].as_u64().unwrap() >= 1);
                    assert_eq!(row[at("copy_count")], row[at("chunks")]);
                    assert_eq!(
                        row[at("body_bytes")].as_u64().unwrap(),
                        row[at("end_byte")].as_u64().unwrap()
                            - row[at("start_byte")].as_u64().unwrap()
                    );
                }
                let all = trace["all_ranges_complete_ns"].as_u64().unwrap();
                let rank_start = trace["rank_start_ns"].as_u64().unwrap();
                let rank_end = trace["rank_end_ns"].as_u64().unwrap();
                let release_end = trace["release_end_ns"].as_u64().unwrap();
                assert!(latest <= all && all <= rank_start && rank_start <= rank_end);
                // Payload release is the last boundary of the SQ8 stage.
                assert!(rank_end <= release_end);
                assert!(release_end <= query["stages"]["sq8"]["end_ns"].as_u64().unwrap());
                assert!(
                    query["stages"]["sq8"]["start_ns"].as_u64().unwrap()
                        <= trace["fetch_start_ns"].as_u64().unwrap()
                );
                assert_eq!(trace["rank"]["ranges"], planned.len());
                let host = &diag["host"];
                let deltas = host["deltas"].as_object().unwrap();
                assert_eq!(deltas.len(), HOST_COUNTERS.len());
                assert_eq!(host["deltas"]["Tcp.RetransSegs"], 0);
                assert!(host["deltas"]["TcpExt.TCPFastRetrans"].is_null());
                let missing = deltas.values().filter(|v| v.is_null()).count();
                assert_eq!(host["unavailable"], missing);
                let start = diag["query_monotonic_start_ns"].as_u64().unwrap();
                let end = diag["query_monotonic_end_ns"].as_u64().unwrap();
                assert!(host["before_monotonic_ns"].as_u64().unwrap() <= start);
                assert!(start <= end && end <= host["after_monotonic_ns"].as_u64().unwrap());
                assert!(
                    diag["process_cpu_start_ns"].as_i64().unwrap()
                        <= diag["process_cpu_end_ns"].as_i64().unwrap()
                );
            }
        }
        use_proc(None);
    }

    #[tokio::test]
    async fn panel_counters_unavailable_and_overcap_lines_are_bounded_invalid_before_seal() {
        let shape = Shape::tiny(257);
        let (dir, base) = fixture(shape).await;
        let empty = dir.path().join("empty-proc");
        std::fs::create_dir(&empty).unwrap();
        use_proc(Some(empty.as_path()));
        let value = with_panel(base, &[0, 1], true);
        let (rows, summary) = run_panel(dir.path(), "unavailable", &value, shape).await;
        assert_eq!(summary["status"], "MEASURED");
        for query in rows.iter().filter(|r| r["phase"] == "query") {
            let host = &query["diagnostic"]["host"];
            assert_eq!(host["unavailable"], HOST_COUNTERS.len());
            assert!(
                host["deltas"]
                    .as_object()
                    .unwrap()
                    .values()
                    .all(Value::is_null)
            );
            assert!(host["before_monotonic_ns"].is_u64() && host["after_monotonic_ns"].is_u64());
            assert_eq!(query["diagnostic"]["sq8_range_trace"]["outcome"], "ranked");
        }
        // Hold only query records to half of the shortest one: the earlier setup lines are
        // published whole, then the first query line is refused whole.
        let text = std::fs::read_to_string(dir.path().join("unavailable")).unwrap();
        let shortest_query = text
            .lines()
            .filter(|line| line.starts_with("{\"phase\":\"query\""))
            .map(|line| line.len() + 1)
            .min()
            .unwrap();
        let cap = shortest_query / 2;
        let c: Config = serde_json::from_value(value).unwrap();
        let output = dir.path().join("overcap");
        let mut out = Output::create(&output).unwrap();
        out.panel_query_cap = Some(cap);
        let mut p = Progress::default();
        let error = query_and_seal(&c, shape, &mut out, &mut p)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("serialized line exceeds"), "{error}");
        assert!(error.contains(&cap.to_string()), "{error}");
        assert_eq!((p.completed, p.sealed, p.truth_opened), (0, false, false));
        assert_eq!(p.stage, "query");
        let published = std::fs::read_to_string(&output).unwrap();
        assert!(published.ends_with('\n'));
        assert!(published.lines().all(|line| {
            serde_json::from_str::<Value>(line).is_ok() && !line.starts_with("{\"phase\":\"query")
        }));
        assert_eq!(out.bytes, published.len() as u64);
        use_proc(None);
    }

    #[test]
    fn panel_diagnostic_memory_is_charged_once_and_never_becomes_an_algorithm_rejection() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let shape = Shape::tiny(257);
        let (dir, base) = runtime.block_on(fixture(shape));
        let object_bytes = 257 * (D + 12);
        let proc = dir.path().join("proc");
        write_proc(&proc);
        use_proc(Some(proc.as_path()));
        // The direct memory model is linear in max_sq8_bytes. Two totals are compared with the
        // cap: the ALGORITHM total (what the same configuration models uninstrumented) and the
        // COMPLETE total (as pinned, plus the library-owned trace reservation the first query
        // is admitted against). Probe one admitted run through the real pipeline, then land
        // exactly on each side of each cap with further real runs.
        for trace in [false, true] {
            let panel =
                |bytes: usize| with_direct(with_panel(base.clone(), &[0, 1], trace), bytes, 2);
            let name = |what: &str| format!("{what}-{trace}");
            let (status, rows) =
                run_status(dir.path(), &name("probe"), &panel(object_bytes), shape);
            assert_eq!(status, Status::Measured);
            let admission = rows
                .iter()
                .find(|r| r["phase"] == "direct_admission")
                .unwrap()
                .clone();
            let figure = |value: &Value, key: &str| value[key].as_u64().unwrap();
            let memory = &admission["memory"];
            let (resident, planner, cap, total) = (
                figure(memory, "resident_bytes"),
                figure(memory, "direct_planner_bytes"),
                figure(memory, "cap_bytes"),
                figure(memory, "total_bytes"),
            );
            let config = serde_json::from_value::<Config>(panel(object_bytes)).unwrap();
            let pinned = limits(&config).unwrap().already_pinned_bytes;
            let reservation = figure(&admission, "library_trace_reservation_bytes");
            assert_eq!(reservation, library_trace_bytes(&config) as u64);
            assert_eq!(reservation > 0, trace);
            let diagnostic_pinned = figure(&admission, "diagnostic_pinned_bytes");
            assert!(diagnostic_pinned >= 2 * PANEL_LINE_CAP as u64);
            assert_eq!(
                figure(&admission, "complete_total_bytes"),
                total + reservation
            );
            assert_eq!(
                figure(&admission, "algorithm_total_bytes"),
                total - pinned + uninstrumented_pinned_bytes(&config)
            );
            let ranking = 256 * 257 + 4 * D as u64;
            let fixed = resident + planner + ranking;
            // complete(b) = fixed + 3b + reservation; algorithm(b) = fixed + 3b + shift.
            let shift = uninstrumented_pinned_bytes(&config) as i128 - pinned as i128;
            let largest = |limit: i128| ((cap as i128 - limit - fixed as i128) / 3) as usize;

            // A. The complete total fits the cap exactly, trace reservation counted once: measured
            //    through startup AND the library's own admission of the traced query.
            let at_complete = largest(reservation as i128);
            let (status, rows) = run_status(dir.path(), &name("fits"), &panel(at_complete), shape);
            assert_eq!(status, Status::Measured, "{rows:?}");
            // B. One more byte of the query model: the algorithm still fits but the instrumented
            //    total does not. INVALID at startup, before any query payload opens.
            let (status, rows) = run_status(
                dir.path(),
                &name("complete"),
                &panel(at_complete + 1),
                shape,
            );
            assert_eq!(status, Status::Invalid);
            let terminal = &rows.last().unwrap()["summary"];
            assert_eq!(terminal["status"], "INVALID");
            assert!(terminal["resource_rejection"].is_null());
            assert_eq!(terminal["stage"], "direct_admission");
            assert!(
                terminal["error"]
                    .as_str()
                    .unwrap()
                    .contains("diagnostic memory")
            );
            assert_eq!(terminal["completed_queries"], 0);
            assert_eq!(terminal["all_queries_sealed"], false);
            assert_eq!(terminal["truth_opened"], false);
            let refused = rows
                .iter()
                .find(|r| r["phase"] == "direct_admission")
                .unwrap();
            assert_eq!(refused["admitted"], false);
            assert!(figure(refused, "algorithm_total_bytes") <= cap);
            assert!(figure(refused, "complete_total_bytes") > cap);
            assert!(rows.iter().all(|r| {
                r["phase"] != "query"
                    && r["phase"] != "query_failure"
                    && r["phase"] != "all_queries_sealed"
            }));
            // C. The largest query model whose ALGORITHM total fits: its complete total does not
            //    (instrumentation alone), so still INVALID, never a candidate rejection.
            let at_algorithm = largest(shift);
            assert!(at_algorithm > at_complete);
            let (status, rows) = run_status(
                dir.path(),
                &name("algorithm-fits"),
                &panel(at_algorithm),
                shape,
            );
            assert_eq!(status, Status::Invalid);
            let terminal = &rows.last().unwrap()["summary"];
            assert!(terminal["resource_rejection"].is_null());
            assert_eq!(terminal["stage"], "direct_admission");
            let refused = rows
                .iter()
                .find(|r| r["phase"] == "direct_admission")
                .unwrap();
            assert!(figure(refused, "algorithm_total_bytes") <= cap);
            assert!(figure(refused, "complete_total_bytes") > cap);
            // D. One more byte of the ALGORITHM budget is a genuine, typed resource rejection.
            let (status, rows) = run_status(
                dir.path(),
                &name("algorithm"),
                &panel(at_algorithm + 1),
                shape,
            );
            assert_eq!(status, Status::ResourceReject);
            let terminal = &rows.last().unwrap()["summary"];
            assert_eq!(terminal["status"], "RESOURCE_REJECT");
            assert_eq!(terminal["resource_rejection"], "modeled_memory");
            assert_eq!(terminal["stage"], "direct_admission");
            let refused = rows
                .iter()
                .find(|r| r["phase"] == "direct_admission")
                .unwrap();
            assert!(figure(refused, "algorithm_total_bytes") > cap);
        }
        use_proc(None);
    }

    fn source_query_error(store: object_store::Error) -> TwoBitGenerationError {
        use borsuk::{
            sq8_s3_range::{RangeFetchError, RankedSq8Failure},
            two_bit_generation::{QueryStages, TwoBitGenerationError},
        };
        TwoBitGenerationError::Query {
            stages: QueryStages::default(),
            error: Box::new(TwoBitGenerationError::SourceRead(RankedSq8Failure {
                error: RangeFetchError::Store(store),
                stats: Sq8ReadStats {
                    submitted_gets: 1,
                    verified_bytes: 0,
                    failed_gets: 1,
                },
            })),
        }
    }

    #[tokio::test]
    async fn query_error_cause_keeps_real_http500_ahead_of_long_uri() {
        use object_store::{RetryConfig, aws::AmazonS3Builder};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut byte = [0; 1];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
                assert!(request.len() < 16_384);
            }
            socket.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
        });
        let store = AmazonS3Builder::new()
            .with_bucket_name("fixture")
            .with_region("eu-central-1")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_access_key_id("fixture")
            .with_secret_access_key("fixture")
            .with_retry(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build()
            .unwrap();
        let key = ObjectPath::from(format!("{}/records.bin", "long-object-name".repeat(80)));
        let store_error = tokio::time::timeout(std::time::Duration::from_secs(5), store.get(&key))
            .await
            .unwrap()
            .err()
            .unwrap();
        server.join().unwrap();
        let error = source_query_error(store_error);
        assert!(
            !error
                .to_string()
                .chars()
                .take(512)
                .collect::<String>()
                .contains("status: 500")
        );
        let text = query_error_text(&error);
        assert!(text.contains("phase=source store=Generic"), "{text}");
        assert!(text.contains("500 Internal Server Error"), "{text}");
        assert!(!text.contains("long-object-name"));
        assert!(text.chars().count() <= 512);
        let p = Progress {
            stage: "query",
            completed: 7,
            query_wall_ns: 123,
            query_cpu_ns: 45,
            cohort: None,
            transport: TransportSpan {
                stage: "query",
                ordinal: Some(7),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut expected = invalid_summary(&p, "old error");
        let terminal = invalid_summary(&p, &format!("native query 7: {text}"));
        assert!(
            terminal["error"]
                .as_str()
                .unwrap()
                .contains("500 Internal Server Error")
        );
        expected["error"] = terminal["error"].clone();
        assert_eq!(terminal, expected);
        assert_eq!(terminal["transport_last_boundary"]["ordinal"], 7);
        assert_eq!(terminal["completed_queries"], 7);
        assert_eq!(terminal["truth_opened"], false);
        assert_eq!(terminal["all_queries_sealed"], false);
        assert!(serde_json::to_vec(&terminal).unwrap().len() + 512 < TERMINAL_RESERVE as usize);
    }

    #[test]
    fn query_error_cause_keeps_transport_and_non_store_failures_identifiable() {
        let error = source_query_error(object_store::Error::Precondition {
            path: "https://user:credential@example.invalid/object?secret=query".repeat(40),
            source: io::Error::new(io::ErrorKind::ConnectionReset, "connection reset by peer")
                .into(),
        });
        let text = query_error_text(&error);
        assert!(text.contains("phase=source store=Precondition"), "{text}");
        assert!(text.contains("connection reset by peer"), "{text}");
        assert!(!text.contains("credential") && !text.contains("secret"));
        let invalid =
            borsuk::two_bit_generation::TwoBitGenerationError::Invalid("query validation");
        assert!(query_error_text(&invalid).contains("query validation"));
        let long = borsuk::two_bit_generation::TwoBitGenerationError::Router("é".repeat(5000));
        assert_eq!(query_error_text(&long).chars().count(), 512);
    }

    #[test]
    fn query_error_cause_bounds_cyclic_sources_and_wrapper_depth() {
        #[derive(Debug)]
        struct Cycle;
        impl std::fmt::Display for Cycle {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("must not display a nonterminal source")
            }
        }
        impl Error for Cycle {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                Some(self)
            }
        }
        let mut error = source_query_error(object_store::Error::Generic {
            store: "fixture",
            source: Box::new(Cycle),
        });
        let text = query_error_text(&error);
        assert!(text.contains("source chain limit"), "{text}");
        assert!(!text.contains("must not display"));
        for _ in 0..40 {
            error = borsuk::two_bit_generation::TwoBitGenerationError::RouterCharged {
                stats: Sq8ReadStats::default(),
                error: Box::new(error),
            };
        }
        let text = query_error_text(&error);
        assert!(text.contains("wrapper limit"), "{text}");
        assert!(text.chars().count() <= 512);
    }

    #[test]
    fn panel_failure_record_keeps_its_error_when_the_diagnostic_overflows_the_line() {
        let dir = tempfile::tempdir().unwrap();
        let mut out = Output::create(&dir.path().join("failure")).unwrap();
        out.admit_panel_lines(400);
        let record =
            json!({"phase":"query_failure","ordinal":7,"error":"boom","truth_opened":false});
        emit_failure(&mut out, record.clone(), Some(json!({"x":1}))).unwrap();
        emit_failure(&mut out, record, Some(json!({"x":"y".repeat(1000)}))).unwrap();
        let published = out.bytes;
        let huge = json!({"phase":"query_failure","error":"e".repeat(1000)});
        let error = emit_failure(&mut out, huge, Some(json!({"x":1}))).unwrap_err();
        assert!(error.downcast_ref::<LineCap>().is_some());
        assert_eq!(out.bytes, published, "a refused line publishes no byte");
        let rows = records(&out.path);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["diagnostic"], json!({"x":1}));
        assert_eq!(
            rows[1]["diagnostic"],
            json!({"omitted":"serialized_line_cap","cap_bytes":400})
        );
        assert_eq!(
            (&rows[1]["error"], &rows[1]["ordinal"]),
            (&json!("boom"), &json!(7))
        );
    }

    #[test]
    fn panel_line_admission_has_an_exact_128kib_boundary_and_full_keeps_the_256kib_cap() {
        assert_eq!(PANEL_LINE_CAP, 128 * 1024);
        assert_eq!(LINE_CAP, 256 * 1024);
        assert_eq!(SOURCE_BINDING_LINE_CAP, 2 * 6 * CONFIG_CAP + 64 * 1024);
        let mut line = Vec::with_capacity(PANEL_LINE_CAP);
        let (pointer, capacity) = (line.as_ptr(), line.capacity());
        // A JSON string of n bytes serializes to n + 2 bytes; its newline makes n + 3.
        for (n, ok) in [
            (PANEL_LINE_CAP - 4, true),
            (PANEL_LINE_CAP - 3, true),
            (PANEL_LINE_CAP - 2, false),
            (PANEL_LINE_CAP, false),
            (3 * PANEL_LINE_CAP, false),
        ] {
            let result = bounded_line(&mut line, &"x".repeat(n), PANEL_LINE_CAP);
            assert_eq!(result.is_ok(), ok, "{n}");
            if ok {
                assert_eq!(line.len(), n + 3);
                assert!(line.len() <= PANEL_LINE_CAP);
                assert_eq!(line.last(), Some(&b'\n'));
            } else {
                assert!(result.unwrap_err().downcast_ref::<LineCap>().is_some());
            }
            assert_eq!((line.as_ptr(), line.capacity()), (pointer, capacity));
        }
        let dir = tempfile::tempdir().unwrap();
        let mut out = Output::create(&dir.path().join("panel")).unwrap();
        out.admit_panel_lines(PANEL_LINE_CAP);
        out.emit(&"x".repeat(PANEL_LINE_CAP - 3)).unwrap();
        assert_eq!(out.bytes, PANEL_LINE_CAP as u64);
        let error = out.emit(&"x".repeat(PANEL_LINE_CAP - 2)).unwrap_err();
        assert!(error.downcast_ref::<LineCap>().is_some());
        assert_eq!(out.bytes, PANEL_LINE_CAP as u64);
        assert_eq!(
            std::fs::metadata(&out.path).unwrap().len(),
            PANEL_LINE_CAP as u64
        );
        assert_eq!(
            bounded_text(&"é".repeat(5000)).chars().count(),
            PANEL_ERROR_CHARS
        );
        assert_eq!(bounded_text("short"), "short");
        out.emit(&json!({"after":"overcap"})).unwrap();
        // Full execution keeps the unchanged 256 KiB per-line cap.
        let mut full = Output::create(&dir.path().join("full")).unwrap();
        assert!(full.panel.is_none());
        full.emit(&"x".repeat(100_000)).unwrap();
        assert!(full.emit(&"x".repeat(LINE_CAP)).is_err());
    }

    #[test]
    fn source_binding_has_a_separate_bounded_serialization_reservation() {
        let dir = tempfile::tempdir().unwrap();
        let mut out = Output::create(&dir.path().join("binding")).unwrap();
        out.admit_panel_lines(PANEL_LINE_CAP);
        // Exercise the six-byte JSON escape bound independently of manifest parsing.
        let raw = "\u{0001}".repeat(CONFIG_CAP);
        let binding = json!({"phase":"source_binding",
            "generation_authority":{"root_manifest":raw,"page_manifest":raw}});
        let bytes = serde_json::to_vec(&binding).unwrap().len() + 1;
        assert!(bytes > LINE_CAP && bytes <= SOURCE_BINDING_LINE_CAP);
        out.emit_source_binding(&binding).unwrap();
        assert_eq!(out.bytes, bytes as u64);
        assert_eq!(out.line_cap, LINE_CAP);
        assert_eq!(out.panel_cap, PANEL_LINE_CAP);
        assert!(out.emit(&"x".repeat(PANEL_LINE_CAP)).is_err());
        assert_eq!(out.bytes, bytes as u64);
        assert!(
            out.emit_source_binding(&"x".repeat(SOURCE_BINDING_LINE_CAP))
                .is_err()
        );
        assert_eq!(out.bytes, bytes as u64);
        let c: Config = serde_json::from_value(config_value(dir.path(), Shape::tiny(32))).unwrap();
        assert!(caller_bytes(&c) >= 2 * SOURCE_BINDING_LINE_CAP);
        assert_eq!(diagnostic_bytes(&c).unwrap(), 0);
    }

    #[test]
    fn panel_missing_or_tampered_truth_is_invalid_only_after_the_selected_results_are_sealed() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let shape = Shape {
            count: 5,
            ..Shape::tiny(32)
        };
        let (dir, base) = runtime.block_on(fixture(shape));
        let panel = with_panel(base, &[0, 2, 4], false);
        let truth = dir.path().join("truth.u64");
        let original = std::fs::read(&truth).unwrap();
        // Unselected truth row 1 repeats an ID: a valid SHA still cannot hide it.
        let mut repeated = original.clone();
        let second = repeated[K * 8 + 8..K * 8 + 16].to_vec();
        repeated[K * 8..K * 8 + 8].copy_from_slice(&second);
        for (index, case) in ["missing", "tampered", "unselected-row"]
            .into_iter()
            .enumerate()
        {
            let mut value = panel.clone();
            match case {
                "missing" => std::fs::remove_file(&truth).unwrap(),
                "tampered" => {
                    let mut bad = original.clone();
                    bad[0] ^= 1;
                    std::fs::write(&truth, bad).unwrap();
                }
                _ => value["truth"] = artifact(&truth, &repeated),
            }
            let (path, sha) = write_config(dir.path(), &value);
            let output = dir.path().join(format!("panel-invalid-{index}"));
            assert!(!execute_paths(&path, &sha, &output, shape).unwrap());
            let records = records(&output);
            assert_eq!(
                records.iter().filter(|r| r["phase"] == "query").count(),
                3,
                "{case}"
            );
            assert!(records.iter().any(|r| r["phase"] == "all_queries_sealed"));
            assert!(!records.iter().any(|r| r["phase"] == "recall"));
            let terminal = &records.last().unwrap()["summary"];
            assert_eq!(terminal["status"], "INVALID", "{case}");
            assert_eq!(terminal["all_queries_sealed"], true);
            assert_eq!(terminal["truth_opened"], true);
            assert_eq!(terminal["completed_queries"], 3);
        }
        // The output cap also refuses a panel line whole, leaving no partial record.
        let (dir, base) = runtime.block_on(fixture(shape));
        let c: Config = serde_json::from_value(with_panel(base, &[0, 1], false)).unwrap();
        let mut capped = Output::create(&dir.path().join("capped")).unwrap();
        capped.cap = 100;
        let mut p = Progress::default();
        assert!(
            configured_query_runtime(&c)
                .unwrap()
                .block_on(query_and_seal(&c, shape, &mut capped, &mut p))
                .is_err()
        );
        assert!(!p.sealed && !p.truth_opened && capped.bytes <= 100);
        let text = std::fs::read_to_string(dir.path().join("capped")).unwrap();
        assert!(text.is_empty() || text.ends_with('\n'));
    }
}
