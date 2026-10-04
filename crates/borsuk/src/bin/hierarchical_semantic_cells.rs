//! UNVERIFIED research interface, no production/remote serving path.
//!
//! `hierarchical_semantic_cells build CONFIG CONFIG_SHA256 NEW_DIRECTORY`
//! CONFIG is the module's strict BuildConfig, binding current-generation inputs.
//! `hierarchical_semantic_cells diagnose CONFIG CONFIG_SHA256 NEW_JSONL`
//! Diagnostic requests are JSONL {"ordinal":...,"query":[...]}; config supplies
//! first/count/top_k and explicit SearchOptions. Optional truth is row-major LE
//! u32 source ordinals at truth_width. It is opened only after all query traces
//! and a prefix-digest freeze marker are synced. No thresholds, panel choices,
//! corpus-specific tuning, paid runner, AWS, or network are supplied here.
//!
//! The full authenticated directory is resident after explicitly capped startup;
//! successful queries fetch complete cells in one payload wave. TwoStage is
//! available only in the module's synthetic parity fixtures, not this runner.
//! `build-probes` is source-only; strict schema borsuk-source-witness-build-v1.
//! `nominate-probes` freezes all64 resident top24 rosters before optional GT,
//! without cell payload reads; schema borsuk-source-witness-nomination-v1.
//! `diagnose-probes` requires both coverage panels plus independently pinned
//! original supervisor exit-zero/resource/drain/cleanup receipts, and admits
//! all16 blocks; schema borsuk-source-witness-diagnostic-v1. Its complete query
//! timer surrounds routing/read/auth/nomination/ranking, without stage sums.
//! Parent owns runtime admission, original process closure and promotion.
//! Actual GET/byte counters describe this serial local range-read adapter only.
//! Stage RSS is an end snapshot and lifetime HWM, not independent stage peaks;
//! external monitoring is required for CPU/RSS/cgroup/build-scratch qualification.

use borsuk::hierarchical_semantic_cells::{
    Artifact, BuildConfig, FetchPolicy, NominationLimits, NominationPolicy, Prototype, Result,
    SearchOptions, SearchTrace, build,
    decompose_loss, process_memory_snapshot,
    SourceProbeLimits, SourceProbeRouter, SourceProbeNomination, SourceProbeSearchTrace,
    build_source_probes, read_source_probe_artifact, read_source_probe_artifact_tracked, SourceProbeLayoutStartup, ReadStats,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
    time::Instant,
};

const CONFIG_CAP: usize = 65536;
const REQUEST_CAP: usize = 32 * 1024 * 1024;
const EVENT_CAP: usize = 8 * 1024 * 1024;
const TERMINAL_CAP: usize = 4096;
const DIAGNOSTIC_SCHEMA: &str = "borsuk-hierarchical-cells-diagnostic-v3";

const NOMINATION_SCHEMA: &str = "borsuk-hierarchical-cells-nomination-v1";
const FREEZE_CAP: usize = 4096;
const NOMINATION_COUNT: usize = 64;
const NOMINATION_POLICIES: [NominationPolicy; 2] = [
    NominationPolicy::Hierarchical8And24,
    NominationPolicy::GlobalTop24,
];

/// `nominate CONFIG CONFIG_SHA256 NEW_JSONL`: a strict truth-free interface.
/// No truth descriptor, top-k, beams, scoring options or hidden fixture fallback.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NominationConfig {
    schema: String,
    candidate_root: Artifact,
    requests: Artifact,
    first: usize,
    count: usize,
    limits: NominationLimits,
    max_resident_directory_payload_bytes: usize,
    max_evaluator_payload_bytes: usize,
    max_result_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiagnosticConfig {
    schema: String,
    candidate_root: Artifact,
    requests: Artifact,
    first: usize,
    count: usize,
    top_k: usize,
    options: SearchOptions,
    max_resident_directory_payload_bytes: usize,
    truth: Option<Artifact>,
    truth_width: usize,
    max_evaluator_payload_bytes: usize,
    max_result_bytes: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    ordinal: usize,
    query: Vec<f32>,
}
fn require(ok: bool, message: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(message.into()) }
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn cpu_ns() -> i128 {
    let time = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    i128::from(time.tv_sec) * 1_000_000_000 + i128::from(time.tv_nsec)
}
struct Events {
    file: File,
    bytes: usize,
    cap: usize,
    digest: Sha256,
    error_context: Value,
}
impl Events {
    fn new(output: &Path, cap: usize) -> Result<Self> {
        require(cap >= TERMINAL_CAP, "diagnostic terminal reserve")?;
        Ok(Self {
            file: OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(output)?,
            bytes: 0,
            cap,
            digest: Sha256::new(),
            error_context: json!({}),
        })
    }
    fn emit(&mut self, event: &Value) -> Result<()> {
        let mut body = serde_json::to_vec(event)?;
        body.push(b'\n');
        require(
            body.len() <= EVENT_CAP
                && self
                    .bytes
                    .checked_add(body.len())
                    .is_some_and(|n| n <= self.cap - TERMINAL_CAP),
            "diagnostic output byte cap",
        )?;
        self.file.write_all(&body)?;
        self.digest.update(&body);
        self.bytes += body.len();
        Ok(())
    }
    fn sync(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
    fn run(&mut self, work: impl FnOnce(&mut Self) -> Result<Value>) -> Result<()> {
        let (terminal, outcome) = match work(self) {
            Ok(terminal) => (terminal, Ok(())),
            Err(error) => {
                let mut terminal = self.error_context.clone();
                terminal["phase"] = json!("terminal");
                terminal["status"] = json!("INVALID");
                terminal["complete"] = json!(false);
                terminal["error"] = json!(error.to_string().chars().take(256).collect::<String>());
                terminal["scientific_qualification"] = json!(false);
                terminal["quality_or_performance_claim"] = json!(false);
                (terminal, Err(error))
            }
        };
        let mut body = serde_json::to_vec(&terminal)?;
        body.push(b'\n');
        require(
            body.len() <= TERMINAL_CAP
                && self
                    .bytes
                    .checked_add(body.len())
                    .is_some_and(|n| n <= self.cap),
            "diagnostic terminal byte cap",
        )?;
        self.file.write_all(&body)?;
        self.digest.update(&body);
        self.bytes += body.len();
        self.sync()?;
        outcome
    }
}

fn admit_fetch_policy(options: SearchOptions) -> Result<()> {
    require(
        options.fetch_policy == FetchPolicy::WholeCell
            && (1..=32).contains(&options.max_cells)
            && (1..=32).contains(&options.max_cell_gets)
            && options.max_cell_bytes > 0
            && options.max_cell_bytes <= 16 * 1024 * 1024,
        "diagnostic requires WholeCell within 32 cells/32 total cell GETs/16MiB",
    )
}

fn diagnose(config: DiagnosticConfig, config_sha: &str, output: &Path) -> Result<()> {
    admit_fetch_policy(config.options)?;
    require(
        config.schema == DIAGNOSTIC_SCHEMA
            && (1..=1000).contains(&config.count)
            && config.first.checked_add(config.count).is_some()
            && config.max_result_bytes >= TERMINAL_CAP
            && config.max_result_bytes <= 512 * 1024 * 1024,
        "diagnostic schema/panel/output limits",
    )?;
    let evaluator = config
        .requests
        .bytes
        .checked_mul(4)
        .and_then(|n| n.checked_add(32 * 1024 * 1024))
        .ok_or("evaluator payload overflow")?;
    require(
        evaluator <= config.max_evaluator_payload_bytes
            && config.max_evaluator_payload_bytes <= 256 * 1024 * 1024,
        "evaluator payload admission",
    )?;
    require(
        (config.truth.is_none() && config.truth_width == 0)
            || (config.truth.is_some()
                && config.truth_width >= config.top_k
                && config.truth_width <= 100),
        "truth width",
    )?;
    let candidate_dir = config
        .candidate_root
        .path
        .parent()
        .ok_or("candidate directory")?;
    require(
        config
            .candidate_root
            .path
            .file_name()
            .is_some_and(|v| v == "manifest.json")
            && fs::metadata(&config.candidate_root.path)?.len()
                == config.candidate_root.bytes as u64,
        "candidate root descriptor",
    )?;
    let prototype = Prototype::open(
        candidate_dir,
        &config.candidate_root.sha256,
        config.max_resident_directory_payload_bytes,
    )?;
    require(
        config.top_k > 0 && config.top_k <= prototype.rows(),
        "diagnostic top-k",
    )?;
    let body = config.requests.read(REQUEST_CAP)?;
    let mut requests = Vec::with_capacity(config.count);
    for (index, line) in std::str::from_utf8(&body)?.lines().enumerate() {
        require(index < config.count, "request panel overflow")?;
        let request: Request = serde_json::from_str(line)?;
        require(
            request.ordinal == config.first + index
                && request.query.len() == prototype.dimensions()
                && request.query.iter().all(|v| v.is_finite()),
            "request ordinal/vector geometry",
        )?;
        requests.push(request);
    }
    require(requests.len() == config.count, "complete request panel")?;
    drop(body);
    let mut events = Events::new(output, config.max_result_bytes)?;
    events.run(|events| {
    events.emit(&json!({"phase":"identity","schema":DIAGNOSTIC_SCHEMA,"config_sha256":config_sha,
        "candidate_root_sha256":config.candidate_root.sha256,"requests_sha256":config.requests.sha256,
        "module_source_sha256":hash(include_bytes!("../hierarchical_semantic_cells.rs")),
        "binary_source_sha256":hash(include_bytes!("hierarchical_semantic_cells.rs")),
        "top_k":config.top_k,"first":config.first,"count":config.count,"options":config.options,
        "physical_s3_measured":false,"scientific_qualification":false,"rss_peak_measurement":"external_required",
        "modeled_evaluator_payload_bytes":evaluator,"startup":prototype.startup,
        "startup_directory":prototype.startup_directory,"directory_admission":prototype.directory_admission,
        "max_resident_directory_payload_bytes":config.max_resident_directory_payload_bytes}))?;
    // Search never receives truth or a truth-derived value. All requests use
    // the same supplied options; errors are INVALID and halt this panel.
    for request in &requests {
        match prototype.search(&request.query, config.top_k, config.options) {
            Ok(trace) => {
                events.error_context = json!({"ordinal":request.ordinal,"accounting":&trace.accounting,"truth_opened":false});
                events.emit(&json!({"phase":"query_frozen","ordinal":request.ordinal,"truth_opened":false,
                    "underfilled":trace.returned.len()<config.top_k,"trace":trace}))?;
                events.error_context = json!({});
            }
            Err(error) => {
                events.error_context = json!({"ordinal":request.ordinal,"accounting":&error.accounting,"truth_opened":false});
                return Err(error.into());
            }
        }
    }
    drop(requests);
    let prefix_bytes = events.bytes;
    let prefix_sha = format!("{:x}", events.digest.clone().finalize());
    events.emit(
        &json!({"phase":"all_queries_frozen","count":config.count,"first":config.first,
        "trace_prefix_bytes":prefix_bytes,"trace_prefix_sha256":prefix_sha,"truth_opened":false}),
    )?;
    events.sync()?;
    if let Some(truth) = &config.truth {
        let mut frozen = File::open(output)?;
        let mut digest = Sha256::new();
        let mut remaining = prefix_bytes;
        let mut buffer = [0_u8; 65536];
        while remaining > 0 {
            let amount = remaining.min(buffer.len());
            frozen.read_exact(&mut buffer[..amount])?;
            digest.update(&buffer[..amount]);
            remaining -= amount;
        }
        require(
            format!("{:x}", digest.finalize()) == prefix_sha,
            "synced frozen trace prefix digest",
        )?;
        frozen.seek(SeekFrom::Start(0))?;
        let truth_bytes = config.count * config.truth_width * 4;
        require(truth.bytes == truth_bytes, "exact truth panel geometry")?;
        events.error_context = json!({"truth_read_attempted":true});
        let body = truth.read(truth_bytes)?;
        // Bound the reader to the already synced truth-free prefix; newly
        // appended loss events cannot feed back into this pass.
        let mut reader = BufReader::new(std::io::Read::take(frozen, prefix_bytes as u64));
        let mut line = String::new();
        let mut seen = 0;
        loop {
            line.clear();
            if reader
                .by_ref()
                .take((EVENT_CAP + 1) as u64)
                .read_line(&mut line)?
                == 0
            {
                break;
            }
            require(
                line.len() <= EVENT_CAP && line.ends_with('\n'),
                "frozen trace event cap/termination",
            )?;
            let event: Value = serde_json::from_str(&line)?;
            if event["phase"] != "query_frozen" {
                continue;
            }
            require(
                seen < config.count
                    && event["ordinal"].as_u64() == Some((config.first + seen) as u64),
                "frozen trace ordinal",
            )?;
            let trace: SearchTrace = serde_json::from_value(event["trace"].clone())?;
            let start = seen * config.truth_width * 4;
            let ids = body[start..start + config.top_k * 4]
                .chunks_exact(4)
                .map(|v| i64::from(u32::from_le_bytes(v.try_into().unwrap())))
                .collect::<Vec<_>>();
            require(
                ids.iter().all(|v| (*v as usize) < prototype.rows()),
                "truth source ordinal domain",
            )?;
            events.emit(&json!({"phase":"loss_attribution","ordinal":config.first+seen,"truth_sha256":truth.sha256,
                "loss":decompose_loss(&trace,&ids)?}))?;
            seen += 1;
        }
        require(seen == config.count, "complete frozen loss panel")?;
    }
    Ok(json!({"phase":"terminal","status":"DIAGNOSTIC","complete":true,"queries":config.count,
        "truth_opened":config.truth.is_some(),"scientific_qualification":false,"quality_or_performance_claim":false}))
    })?;
    println!(
        "{}",
        json!({"status":"DIAGNOSTIC","queries":config.count,"result_bytes":events.bytes})
    );
    Ok(())
}
// Re-read the synced file prefix before emitting any completion marker. A
// changed/truncated prefix is INVALID, including tampering after serialization.
fn verify_nomination_prefix(output: &Path, bytes: usize, expected: &str) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::NOFOLLOW).bits() as i32)
        .open(output)?;
    let metadata = file.metadata()?;
    require(metadata.is_file() && metadata.len() == bytes as u64,
        "nomination prefix regular file/exact length")?;
    let mut digest = Sha256::new();
    let mut remaining = bytes;
    let mut buffer = [0_u8; 65536];
    while remaining > 0 {
        let amount = remaining.min(buffer.len());
        file.read_exact(&mut buffer[..amount])?;
        digest.update(&buffer[..amount]);
        remaining -= amount;
    }
    require(format!("{:x}", digest.finalize()) == expected, "nomination prefix SHA256")
}

fn freeze_nominations(events: &mut Events, output: &Path, identity: &Value) -> Result<()> {
    events.sync()?;
    let prefix_bytes = events.bytes;
    let prefix_sha = format!("{:x}", events.digest.clone().finalize());
    verify_nomination_prefix(output, prefix_bytes, &prefix_sha)?;
    let marker = json!({"phase":"all_selections_frozen","schema":NOMINATION_SCHEMA,
        "prefix_bytes":prefix_bytes,"prefix_sha256":prefix_sha,
        "config_sha256":identity["config_sha256"],
        "candidate_root_sha256":identity["candidate_root_sha256"],
        "requests_sha256":identity["requests_sha256"],
        "module_source_sha256":identity["module_source_sha256"],
        "binary_source_sha256":identity["binary_source_sha256"],
        "source_identity_sha256":identity["source_identity_sha256"],
        "first":identity["first"],"count":NOMINATION_COUNT,"selection_receipts":128,
        "policies":NOMINATION_POLICIES,"truth_opened":false,
        "scope":"one_dataset_parent_must_seal_both_before_offline_truth"});
    require(serde_json::to_vec(&marker)?.len() < FREEZE_CAP, "nomination freeze reserve")?;
    // Normal events could not spend this reserve. Terminal's independent
    // reserve remains available even if freeze emission or syncing fails.
    events.cap += FREEZE_CAP;
    events.emit(&marker)?;
    events.sync()
}

fn nominate(config: NominationConfig, config_sha: &str, output: &Path) -> Result<()> {
    require(config.schema == NOMINATION_SCHEMA && config.count == NOMINATION_COUNT
        && config.first.checked_add(config.count).is_some()
        && (TERMINAL_CAP + FREEZE_CAP..=512 * 1024 * 1024).contains(&config.max_result_bytes),
        "nomination schema/full64/output limits")?;
    let evaluator = config.requests.bytes.checked_mul(4)
        .and_then(|n| n.checked_add(32 * 1024 * 1024))
        .ok_or("nomination evaluator payload overflow")?;
    require(evaluator <= config.max_evaluator_payload_bytes
        && config.max_evaluator_payload_bytes <= 256 * 1024 * 1024,
        "nomination evaluator payload admission")?;
    require(config.candidate_root.path.file_name().is_some_and(|v| v == "manifest.json"),
        "nomination candidate root filename")?;
    // Authenticate the exact descriptor, including length, before opening the
    // resident candidate. No original build inputs are read or reconstructed.
    drop(config.candidate_root.read(CONFIG_CAP)?);
    let prototype = Prototype::open(
        config.candidate_root.path.parent().ok_or("nomination candidate directory")?,
        &config.candidate_root.sha256, config.max_resident_directory_payload_bytes)?;
    let body = config.requests.read(REQUEST_CAP)?;
    let mut requests = Vec::with_capacity(NOMINATION_COUNT);
    for (index, line) in std::str::from_utf8(&body)?.lines().enumerate() {
        require(index < NOMINATION_COUNT, "nomination request panel overflow")?;
        let request: Request = serde_json::from_str(line)?;
        require(request.ordinal == config.first + index
            && request.query.len() == prototype.dimensions()
            && request.query.iter().all(|v| v.is_finite()),
            "nomination request ordinal/vector geometry")?;
        requests.push(request);
    }
    require(requests.len() == NOMINATION_COUNT, "nomination complete request panel")?;
    drop(body);
    let source = prototype.source_identity();
    let identity = json!({"phase":"identity","schema":NOMINATION_SCHEMA,
        "config_sha256":config_sha,"candidate_root_sha256":config.candidate_root.sha256,
        "requests_sha256":config.requests.sha256,
        "module_source_sha256":hash(include_bytes!("../hierarchical_semantic_cells.rs")),
        "binary_source_sha256":hash(include_bytes!("hierarchical_semantic_cells.rs")),
        "source_identity":source,"source_identity_sha256":hash(&serde_json::to_vec(source)?),
        "first":config.first,"count":config.count,"policies":NOMINATION_POLICIES,
        "limits":config.limits,"truth_opened":false,"record_scoring_performed":false,
        "scientific_qualification":false,"quality_or_performance_claim":false,
        "physical_s3_measured":false,"modeled_evaluator_payload_bytes":evaluator,
        "startup":prototype.startup,"startup_directory":prototype.startup_directory,
        "candidate_descriptor_authentication_bytes":config.candidate_root.bytes,
        "directory_admission":prototype.directory_admission,
        "max_resident_directory_payload_bytes":config.max_resident_directory_payload_bytes});
    let mut events = Events::new(output, config.max_result_bytes - FREEZE_CAP)?;
    events.run(|events| {
        events.error_context = json!({"truth_opened":false});
        events.emit(&identity)?;
        for request in &requests {
            for policy in NOMINATION_POLICIES {
                match prototype.nominate(&request.query, policy, config.limits) {
                    Ok(receipt) => {
                        events.error_context = json!({"ordinal":request.ordinal,"policy":policy,
                            "truth_opened":false,"accounting":receipt.accounting});
                        events.emit(&json!({"phase":"selection_frozen","ordinal":request.ordinal,
                            "query_sha256":hash(&request.query.iter().flat_map(|v| v.to_le_bytes()).collect::<Vec<_>>()),
                            "truth_opened":false,"receipt":receipt}))?;
                    }
                    Err(error) => {
                        events.error_context = json!({"ordinal":request.ordinal,"policy":policy,
                            "truth_opened":false,"accounting":error.accounting});
                        return Err(error.into());
                    }
                }
            }
        }
        events.error_context = json!({"truth_opened":false});
        freeze_nominations(events, output, &identity)?;
        Ok(json!({"phase":"terminal","status":"NOMINATIONS_FROZEN","complete":true,
            "queries":NOMINATION_COUNT,"selection_receipts":128,"truth_opened":false,
            "scientific_qualification":false,"quality_or_performance_claim":false}))
    })?;
    println!("{}", json!({"status":"NOMINATIONS_FROZEN","queries":NOMINATION_COUNT,
        "result_bytes":events.bytes,"truth_opened":false}));
    Ok(())
}

fn probe_close_invalid(
    output: &Path,
    sha: &str,
    error: Box<dyn std::error::Error + Send + Sync>,
) -> Result<()> {
    let mut events = Events::new(output, TERMINAL_CAP)?;
    File::open(
        output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    events.error_context = json!({"config_sha256":sha,"truth_opened":false});
    events.run(|_| Err(error))
}
fn probe_config<T: serde::de::DeserializeOwned>(
    body: &[u8],
    sha: &str,
    output: &Path,
) -> Result<T> {
    match serde_json::from_slice(body) {
        Ok(config) => Ok(config),
        Err(error) => {
            probe_close_invalid(output, sha, error.into())?;
            unreachable!()
        }
    }
}
const PROBE_BUILD_SCHEMA: &str = "borsuk-source-witness-build-v1";
const PROBE_NOMINATION_SCHEMA: &str = "borsuk-source-witness-nomination-v1";
const PROBE_DIAGNOSTIC_SCHEMA: &str = "borsuk-source-witness-diagnostic-v1";
const PROBE_GATE_SCHEMA: &str = "borsuk-source-witness-paired-coverage-admission-v1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeBuildConfig {
    schema: String,
    candidate_root: Artifact,
    probe_output: std::path::PathBuf,
    max_resident_directory_payload_bytes: usize,
    max_result_bytes: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeNominationConfig {
    schema: String,
    dataset: String,
    candidate_root: Artifact,
    probes: Artifact,
    requests: Artifact,
    first: usize,
    count: usize,
    limits: SourceProbeLimits,
    max_resident_directory_payload_bytes: usize,
    max_resident_probe_payload_bytes: usize,
    max_evaluator_payload_bytes: usize,
    max_result_bytes: usize,
    truth: Option<Artifact>,
    truth_width: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeDiagnosticConfig {
    schema: String,
    dataset: String,
    candidate_root: Artifact,
    probes: Artifact,
    requests: Artifact,
    /// Authenticated parent's separately admitted paired coverage result files.
    coverage_gate: Artifact,
    first: usize,
    count: usize,
    top_k: usize,
    options: SearchOptions,
    max_resident_directory_payload_bytes: usize,
    max_resident_probe_payload_bytes: usize,
    max_evaluator_payload_bytes: usize,
    max_result_bytes: usize,
    truth: Artifact,
    truth_width: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeCoverageGate {
    schema: String,
    relaion: ProbeCoverageEntry,
    cohere: ProbeCoverageEntry,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeCoverageEntry {
    run_id: String,
    result: Artifact,
    supervisor: Artifact,
}
// Separately authenticated by the parent, never emitted by the diagnostic.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeCoverageSupervisor {
    schema: String,
    closure: borsuk::hierarchical_semantic_cells::split_balance_diagnostic::SupervisorReceipt,
    resources_passed: bool,
    drain_closed: bool,
    cleanup_complete: bool,
}
fn probe_aggregate(
    directory: usize,
    probes: usize,
    evaluator: usize,
    query: usize,
) -> Result<Value> {
    let startup = directory
        .checked_add(probes)
        .and_then(|n| n.checked_add(evaluator));
    let running = startup.and_then(|n| n.checked_add(query));
    require(
        directory <= 512 * 1024 * 1024
            && probes <= 512 * 1024 * 1024
            && evaluator <= 256 * 1024 * 1024
            && query <= 512 * 1024 * 1024
            && running.is_some_and(|n| n <= 512 * 1024 * 1024),
        "probe configured aggregate startup/running payload admission before input bodies",
    )?;
    Ok(
        json!({"directory_allowance_bytes":directory,"probe_allowance_bytes":probes,
        "evaluator_allowance_bytes":evaluator,"query_allowance_bytes":query,
        "startup_bound_bytes":startup,"running_bound_bytes":running,"limit_bytes":536870912,
        "process_overhead_and_actual_peak":"external_supervisor_required"}),
    )
}
fn probe_actual_aggregate(
    layout: &Prototype,
    router: &SourceProbeRouter,
    evaluator: usize,
    query: usize,
) -> Result<Value> {
    let startup = layout
        .directory_admission
        .modeled_preload_peak_bytes
        .checked_add(router.admission.modeled_preload_peak_bytes)
        .and_then(|n| n.checked_add(evaluator))
        .ok_or("probe actual startup aggregate overflow")?;
    let running = layout
        .directory_admission
        .modeled_parsed_payload_bytes
        .checked_add(router.admission.modeled_resident_bytes)
        .and_then(|n| n.checked_add(evaluator))
        .and_then(|n| n.checked_add(query))
        .ok_or("probe actual running aggregate overflow")?;
    require(
        startup <= 512 * 1024 * 1024 && running <= 512 * 1024 * 1024,
        "probe actual aggregate startup/running payload admission",
    )?;
    Ok(
        json!({"startup_modeled_peak_bytes":startup,"running_modeled_bytes":running,
        "query_allowance_bytes":query,"evaluator_allowance_bytes":evaluator,"limit_bytes":536870912}),
    )
}
fn probe_events(output: &Path, cap: usize) -> Result<Events> {
    if !(TERMINAL_CAP + FREEZE_CAP..=128 * 1024 * 1024).contains(&cap) {
        probe_close_invalid(output, "", "probe output/terminal reserve".into())?;
        unreachable!();
    }
    let events = Events::new(output, cap)?;
    File::open(
        output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    Ok(events)
}
fn probe_panel(first: usize, count: usize, dataset: &str) -> Result<()> {
    require(
        first == 0 && count == 64 && matches!(dataset, "relaion" | "cohere"),
        "probe frozen consumed64/dataset",
    )
}
fn probe_open_layout(events: &mut Events, root: &Artifact, cap: usize) -> Result<Prototype> {
    let mut startup = SourceProbeLayoutStartup::default();
    let result = Prototype::open_for_source_probes_traced(root, cap, &mut startup);
    events.error_context["startup_root"] = json!(startup.root);
    events.error_context["startup_directory"] = json!(startup.directory);
    events.error_context["startup_layout"] = json!(startup);
    result
}
fn probe_requests(
    artifact: &Artifact,
    first: usize,
    dimensions: usize,
    cap: usize,
    context: &mut Value,
) -> Result<Vec<Request>> {
    let mut stats = ReadStats::default();
    context["startup_requests"] = json!(stats);
    let modeled = artifact
        .bytes
        .checked_mul(4)
        .and_then(|n| n.checked_add(32 * 1024 * 1024))
        .ok_or("probe evaluator payload overflow")?;
    require(
        modeled <= cap && cap <= 256 * 1024 * 1024,
        "probe evaluator payload admission",
    )?;
    let started = (Instant::now(), cpu_ns());
    let result = read_source_probe_artifact_tracked(artifact, REQUEST_CAP, &mut stats);
    context["startup_requests"] = json!(stats);
    context["startup_requests_auth_wall_ns"] = json!(started.0.elapsed().as_nanos());
    context["startup_requests_auth_process_cpu_ns"] = json!(cpu_ns() - started.1);
    let body = result?;
    let mut requests = Vec::with_capacity(64);
    for (index, line) in std::str::from_utf8(&body)?.lines().enumerate() {
        require(index < 64, "probe request panel overflow")?;
        let request: Request = serde_json::from_str(line)?;
        require(
            request.ordinal == first + index
                && request.query.len() == dimensions
                && request.query.iter().all(|v| v.is_finite()),
            "probe request ordinal/vector geometry",
        )?;
        requests.push(request);
    }
    require(requests.len() == 64, "probe complete64 requests")?;
    Ok(requests)
}
#[allow(clippy::too_many_arguments)]
fn probe_identity(
    schema: &str,
    config_sha: &str,
    dataset: &str,
    root: &Artifact,
    probes: &Artifact,
    requests: &Artifact,
    layout: &Prototype,
    router: &SourceProbeRouter,
) -> Value {
    json!({"phase":"identity","schema":schema,"config_sha256":config_sha,"dataset":dataset,
        "candidate_root_sha256":root.sha256,"probes_sha256":probes.sha256,"requests_sha256":requests.sha256,
        "module_source_sha256":hash(include_bytes!("../hierarchical_semantic_cells.rs")),
        "binary_source_sha256":hash(include_bytes!("hierarchical_semantic_cells.rs")),
        "first":0,"count":64,"witness_policy":16,"selected_cells":24,
        "hierarchy_prefilter":false,"fallback_widening":false,"truth_opened":false,
        "builder_authenticated_membership":true,"serving_membership_proof":"trusted_sidecar_SHA_and_root_binding",
        "physical_s3_measured":false,"scientific_qualification":false,
        "startup_root":layout.startup,"startup_directory":layout.startup_directory,
        "directory_admission":layout.directory_admission,"startup_probes":router.startup,
        "startup_probe_time":router.startup_time,"probe_admission":router.admission,
        "startup_requests":{"submitted_gets":1,"requested_bytes":requests.bytes,"verified_bytes":requests.bytes,"failed_gets":0}})
}
fn freeze_probes(events: &mut Events, output: &Path, identity: &Value) -> Result<(usize, String)> {
    events.sync()?;
    let bytes = events.bytes;
    let sha = format!("{:x}", events.digest.clone().finalize());
    verify_nomination_prefix(output, bytes, &sha)?;
    events.emit(&json!({"phase":"all_selections_frozen","schema":identity["schema"],
        "candidate_root_sha256":identity["candidate_root_sha256"],"probes_sha256":identity["probes_sha256"],
        "requests_sha256":identity["requests_sha256"],"config_sha256":identity["config_sha256"],
        "truth":identity["truth"],"truth_width":identity["truth_width"],
        "module_source_sha256":identity["module_source_sha256"],"binary_source_sha256":identity["binary_source_sha256"],
        "dataset":identity["dataset"],"first":0,"count":64,"selection_receipts":64,
        "prefix_bytes":bytes,"prefix_sha256":sha,"truth_opened":false}))?;
    events.sync()?;
    Ok((bytes, sha))
}
fn probe_truth(
    artifact: &Artifact,
    width: usize,
    rows: usize,
    context: &mut Value,
) -> Result<Vec<Vec<i64>>> {
    require(
        width == 100 && artifact.bytes == 64 * 100 * 4,
        "probe exact64/k100 truth geometry",
    )?;
    context["truth_read_attempted"] = json!(true);
    context["truth_opened"] = json!(null);
    let body = read_source_probe_artifact(artifact, 64 * 100 * 4)?;
    context["truth_opened"] = json!(true);
    context["truth_read_complete"] = json!(true);

    body.chunks_exact(100 * 4)
        .map(|row| {
            let ids = row
                .chunks_exact(4)
                .map(|v| i64::from(u32::from_le_bytes(v.try_into().unwrap())))
                .collect::<Vec<_>>();
            require(
                ids.iter().all(|&id| (id as usize) < rows)
                    && ids
                        .iter()
                        .copied()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        == 100,
                "probe unique in-range truth IDs",
            )?;
            Ok(ids)
        })
        .collect()
}
fn probe_quality(hits: &mut [usize]) -> Result<(f64, usize, bool)> {
    require(hits.len() == 64, "probe complete64 attribution")?;
    let mean = hits.iter().sum::<usize>() as f64 / 6400.;
    hits.sort_unstable();
    // Existing campaign convention: floor((n-1)*.05), zero-based.
    let p05 = hits[(hits.len() - 1) * 5 / 100];
    Ok((mean, p05, mean >= 0.98 && p05 >= 95))
}
fn build_probes(config: ProbeBuildConfig, config_sha: &str, output: &Path) -> Result<()> {
    let mut events = probe_events(output, config.max_result_bytes)?;
    events.error_context = json!({"schema":PROBE_BUILD_SCHEMA,"config_sha256":config_sha,"requests_opened":false,"truth_opened":false});
    events.run(|events| {
        require(config.schema == PROBE_BUILD_SCHEMA && config.probe_output != output, "probe build schema/distinct outputs")?;
        let configured_aggregate = probe_aggregate(config.max_resident_directory_payload_bytes, 0, 0, 256*1024*1024)?;
        let started = (Instant::now(), cpu_ns());
        let layout = probe_open_layout(events,&config.candidate_root,config.max_resident_directory_payload_bytes)?;
        events.emit(&json!({"phase":"identity","schema":PROBE_BUILD_SCHEMA,"config_sha256":config_sha,
            "candidate_root_sha256":config.candidate_root.sha256,"requests_opened":false,"truth_opened":false,
            "module_source_sha256":hash(include_bytes!("../hierarchical_semantic_cells.rs")),
            "binary_source_sha256":hash(include_bytes!("hierarchical_semantic_cells.rs")),
            "startup_root":layout.startup,"startup_directory":layout.startup_directory,
            "startup_wall_ns":started.0.elapsed().as_nanos(),"startup_process_cpu_ns":cpu_ns()-started.1,
            "directory_admission":layout.directory_admission,"configured_aggregate":configured_aggregate}))?;
        let receipt = match build_source_probes(&layout, &config.probe_output) {
            Ok(receipt) => receipt,
            Err(error) => { events.error_context["failure"] = serde_json::to_value(&error)?; return Err(error.into()); }
        };
        events.emit(&json!({"phase":"source_probes_sealed","receipt":receipt,"requests_opened":false,"truth_opened":false}))?;
        events.sync()?;
        Ok(json!({"phase":"terminal","schema":PROBE_BUILD_SCHEMA,"status":"SOURCE_PROBES_SEALED","complete":true,
            "artifact":receipt.artifact,"requests_opened":false,"truth_opened":false,"scientific_qualification":false}))
    })
}
fn nominate_probes(config: ProbeNominationConfig, config_sha: &str, output: &Path) -> Result<()> {
    let mut events = probe_events(output, config.max_result_bytes)?;
    events.error_context =
        json!({"schema":PROBE_NOMINATION_SCHEMA,"config_sha256":config_sha,"truth_opened":false});
    events.run(|events| {
        require(config.schema == PROBE_NOMINATION_SCHEMA, "probe nomination schema")?;
        probe_panel(config.first, config.count, &config.dataset)?;
        require((config.truth.is_none() && config.truth_width == 0) || (config.truth.is_some() && config.truth_width == 100),
            "probe nomination optional k100 truth")?;
        let configured_aggregate = probe_aggregate(config.max_resident_directory_payload_bytes,
            config.max_resident_probe_payload_bytes, config.max_evaluator_payload_bytes, config.limits.max_query_payload_bytes)?;
        let started = (Instant::now(), cpu_ns());
        let layout = probe_open_layout(events,&config.candidate_root,config.max_resident_directory_payload_bytes)?;
        let router = match SourceProbeRouter::open(&layout, &config.probes, config.max_resident_probe_payload_bytes) {
            Ok(router) => router,
            Err(error) => {
                events.error_context["startup_probes"] = json!(error.startup);
                events.error_context["failure"] = serde_json::to_value(&error)?; return Err(error.into());
            }
        };
        events.error_context["startup_probes"] = json!(router.startup);
        events.error_context["startup_probe_time"] = json!(router.startup_time);
        let actual_aggregate = probe_actual_aggregate(&layout, &router, config.max_evaluator_payload_bytes, config.limits.max_query_payload_bytes)?;
        let requests = probe_requests(&config.requests, config.first, layout.dimensions(), config.max_evaluator_payload_bytes, &mut events.error_context)?;
        let mut identity = probe_identity(PROBE_NOMINATION_SCHEMA, config_sha, &config.dataset,
            &config.candidate_root, &config.probes, &config.requests, &layout, &router);
        identity["limits"] = json!(config.limits);
        identity["truth"] = json!(config.truth);
        identity["truth_width"] = json!(config.truth_width);
        identity["configured_aggregate"] = configured_aggregate;
        identity["actual_aggregate"] = actual_aggregate;
        identity["startup_complete_wall_ns"] = json!(started.0.elapsed().as_nanos());
        identity["startup_complete_process_cpu_ns"] = json!(cpu_ns()-started.1);
        events.emit(&identity)?;
        // Query selection cannot read cells even accidentally: drop the sole
        // layout file handle before the first coverage request.
        let rows = layout.rows();
        drop(layout);
        for request in &requests {
            events.error_context["ordinal"] = json!(request.ordinal);
            let receipt = match router.nominate(&request.query, config.limits) {
                Ok(receipt) => receipt,
                Err(error) => { events.error_context["failure"] = serde_json::to_value(&error)?; return Err(error.into()); }
            };
            events.emit(&json!({"phase":"selection_frozen","ordinal":request.ordinal,"truth_opened":false,"receipt":receipt}))?;
        }
        drop(requests);
        let (prefix_bytes, prefix_sha) = freeze_probes(events, output, &identity)?;
        let mut quality = json!(null);
        if let Some(truth) = &config.truth {
            // All64 rosters and the freeze marker are durable before opening GT.
            let truths = probe_truth(truth, config.truth_width, rows, &mut events.error_context)?;
            let mut file = OpenOptions::new().read(true)
                .custom_flags((rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::NOFOLLOW).bits() as i32).open(output)?;
            require(file.metadata()?.is_file(), "probe frozen result regular file")?;
            // Re-authenticate exactly the pre-truth prefix; the marker is outside
            // that prefix, so use a capped stream instead of its full file size.
            let mut digest = Sha256::new();
            let mut remaining = prefix_bytes;
            let mut buffer = [0_u8; 65536];
            while remaining > 0 {
                let n = remaining.min(buffer.len()); file.read_exact(&mut buffer[..n])?; digest.update(&buffer[..n]); remaining -= n;
            }
            require(format!("{:x}", digest.finalize()) == prefix_sha, "probe pre-truth frozen prefix SHA")?;
            file.seek(SeekFrom::Start(0))?;
            let mut reader = BufReader::new(file.take(prefix_bytes as u64));
            let mut line = String::new();
            let mut hits = Vec::with_capacity(64);
            loop {
                line.clear();
                if reader.by_ref().take((EVENT_CAP + 1) as u64).read_line(&mut line)? == 0 { break; }
                require(line.len() <= EVENT_CAP && line.ends_with('\n'), "probe frozen event cap")?;
                let event: Value = serde_json::from_str(&line)?;
                if event["phase"] != "selection_frozen" { continue; }
                let index = hits.len();
                require(index < 64 && event["ordinal"] == json!(index), "probe frozen ordinal")?;
                let receipt: SourceProbeNomination = serde_json::from_value(event["receipt"].clone())?;
                let hit = truths[index].iter().filter(|id| receipt.covered_ids.binary_search(id).is_ok()).count();
                hits.push(hit);
                events.error_context["ordinal"] = json!(index);
                events.emit(&json!({"phase":"coverage_attribution","ordinal":index,"truth_sha256":truth.sha256,
                    "selected_hits":hit,"selected_misses":100-hit,"truth_width":100}))?;
            }
            let (mean, p05, passed) = probe_quality(&mut hits)?;
            quality = json!({"mean_selected_cell_coverage":mean,"p05_hits":p05,"passed":passed});
        }
        Ok(json!({"phase":"terminal","schema":PROBE_NOMINATION_SCHEMA,"status":if quality.is_null() { "SELECTIONS_FROZEN" }
            else if quality["passed"] == true { "COVERAGE_PASS" } else { "FAIL" },
            "complete":true,"queries":64,"dataset":config.dataset,"quality":quality,
            "config_sha256":config_sha,"requires_matching_supervisor_exit_receipt":true,
            "candidate_root_sha256":config.candidate_root.sha256,"probes_sha256":config.probes.sha256,
            "requests_sha256":config.requests.sha256,"prefix_bytes":prefix_bytes,"prefix_sha256":prefix_sha,
            "truth_opened":config.truth.is_some(),"truth":config.truth,"truth_width":config.truth_width,
            "cell_payload_query_gets":0,"cell_payload_query_bytes":0,
            "scientific_qualification":false,"quality_or_performance_claim":false}))
    })
}
fn admit_probe_coverage(config: &ProbeDiagnosticConfig) -> Result<()> {
    let body = read_source_probe_artifact(&config.coverage_gate, CONFIG_CAP)?;
    let gate: ProbeCoverageGate = serde_json::from_slice(&body)?;
    require(
        gate.schema == PROBE_GATE_SCHEMA,
        "probe paired coverage admission schema",
    )?;
    let mut matched = false;
    for (dataset, entry) in [("relaion", gate.relaion), ("cohere", gate.cohere)] {
        let supervisor_body = read_source_probe_artifact(&entry.supervisor, CONFIG_CAP)?;
        let supervisor: ProbeCoverageSupervisor = serde_json::from_slice(&supervisor_body)?;
        require(
            supervisor.schema == "borsuk-source-witness-coverage-supervisor-v1"
                && !entry.run_id.is_empty()
                && entry.run_id.len() <= 256
                && supervisor.closure.run_id == entry.run_id
                && supervisor.closure.report_sha256 == entry.result.sha256
                && supervisor.closure.process_exit_code == 0
                && supervisor.closure.resources_closed
                && supervisor.resources_passed
                && supervisor.drain_closed
                && supervisor.cleanup_complete,
            "probe coverage requires independently authenticated original exit-zero/resource/drain/cleanup closure",
        )?;
        let artifact = entry.result;
        require(
            artifact
                .bytes
                .checked_mul(2)
                .and_then(|n| n.checked_add(32 * 1024 * 1024))
                .is_some_and(|n| n <= config.max_evaluator_payload_bytes)
                && config.max_evaluator_payload_bytes <= 256 * 1024 * 1024,
            "probe coverage gate replay payload admission",
        )?;
        let body = read_source_probe_artifact(&artifact, 128 * 1024 * 1024)?;
        let text = std::str::from_utf8(&body)?;
        let terminal: Value = serde_json::from_str(
            text.lines()
                .last()
                .ok_or("probe missing coverage terminal")?,
        )?;
        require(
            terminal["schema"] == PROBE_NOMINATION_SCHEMA
                && terminal["config_sha256"] == supervisor.closure.config_sha256
                && terminal["requires_matching_supervisor_exit_receipt"] == true
                && terminal["status"] == "COVERAGE_PASS"
                && terminal["complete"] == true
                && terminal["dataset"] == dataset
                && terminal["queries"] == 64
                && terminal["truth_opened"] == true
                && terminal["truth_width"] == 100
                && terminal["truth"].is_object()
                && terminal["cell_payload_query_gets"] == 0
                && terminal["quality"]["mean_selected_cell_coverage"]
                    .as_f64()
                    .is_some_and(|v| v >= 0.98 && v <= 1.)
                && terminal["quality"]["p05_hits"]
                    .as_u64()
                    .is_some_and(|v| (95..=100).contains(&v)),
            "probe both dataset coverage gates must PASS",
        )?;
        let identity_line = text
            .lines()
            .next()
            .ok_or("probe coverage identity missing")?;
        require(
            identity_line.len() <= EVENT_CAP,
            "probe coverage identity event cap",
        )?;
        let identity: Value = serde_json::from_str(identity_line)?;
        require(
            identity["phase"] == "identity"
                && identity["schema"] == PROBE_NOMINATION_SCHEMA
                && identity["dataset"] == dataset
                && identity["config_sha256"] == terminal["config_sha256"]
                && identity["candidate_root_sha256"] == terminal["candidate_root_sha256"]
                && identity["probes_sha256"] == terminal["probes_sha256"]
                && identity["requests_sha256"] == terminal["requests_sha256"]
                && identity["truth"] == terminal["truth"]
                && identity["truth_width"] == terminal["truth_width"]
                && identity["module_source_sha256"]
                    == hash(include_bytes!("../hierarchical_semantic_cells.rs"))
                && identity["binary_source_sha256"]
                    == hash(include_bytes!("hierarchical_semantic_cells.rs")),
            "probe admitted coverage exact native source and identity binding",
        )?;
        let prefix = usize::try_from(
            terminal["prefix_bytes"]
                .as_u64()
                .ok_or("probe coverage prefix bytes")?,
        )?;
        require(
            prefix <= body.len()
                && terminal["prefix_sha256"].as_str() == Some(hash(&body[..prefix]).as_str()),
            "probe admitted coverage prefix authentication",
        )?;
        require(
            prefix > 0 && body[prefix - 1] == b'\n',
            "probe coverage frozen prefix boundary",
        )?;
        let marker_line = std::str::from_utf8(&body[prefix..])?
            .lines()
            .next()
            .ok_or("probe coverage freeze marker missing")?;
        require(
            marker_line.len() <= FREEZE_CAP,
            "probe coverage freeze marker cap",
        )?;
        let marker: Value = serde_json::from_str(marker_line)?;
        require(
            marker["phase"] == "all_selections_frozen"
                && marker["schema"] == PROBE_NOMINATION_SCHEMA
                && marker["count"] == 64
                && marker["selection_receipts"] == 64
                && marker["truth_opened"] == false
                && marker["prefix_bytes"] == terminal["prefix_bytes"]
                && marker["prefix_sha256"] == terminal["prefix_sha256"]
                && marker["config_sha256"] == terminal["config_sha256"]
                && marker["candidate_root_sha256"] == terminal["candidate_root_sha256"]
                && marker["probes_sha256"] == terminal["probes_sha256"]
                && marker["requests_sha256"] == terminal["requests_sha256"]
                && marker["truth"] == terminal["truth"]
                && marker["truth_width"] == terminal["truth_width"],
            "probe admitted coverage durable all64 freeze binding",
        )?;
        if dataset == config.dataset {
            require(
                terminal["candidate_root_sha256"] == config.candidate_root.sha256
                    && terminal["probes_sha256"] == config.probes.sha256
                    && terminal["requests_sha256"] == config.requests.sha256
                    && terminal["truth"] == json!(config.truth)
                    && terminal["truth_width"] == config.truth_width,
                "probe coverage admission exact arm/request/truth descriptor identity",
            )?;
            matched = true;
        }
    }
    require(matched, "probe diagnostic dataset coverage admission")
}
fn diagnose_probes(config: ProbeDiagnosticConfig, config_sha: &str, output: &Path) -> Result<()> {
    let mut events = probe_events(output, config.max_result_bytes)?;
    events.error_context =
        json!({"schema":PROBE_DIAGNOSTIC_SCHEMA,"config_sha256":config_sha,"truth_opened":false});
    events.run(|events| {
        require(config.schema == PROBE_DIAGNOSTIC_SCHEMA && config.top_k == 100 && config.truth_width == 100,
            "probe diagnostic frozen k100 schema")?;
        probe_panel(config.first, config.count, &config.dataset)?;
        // Reject before layout/requests/GT reads if either consumed-panel gate
        // failed or is absent. The parent supplies trusted coverage descriptors.
        let configured_aggregate = probe_aggregate(config.max_resident_directory_payload_bytes,
            config.max_resident_probe_payload_bytes, config.max_evaluator_payload_bytes, config.options.max_query_payload_bytes)?;
        admit_probe_coverage(&config)?;
        require(config.options.fetch_policy == FetchPolicy::WholeCell && config.options.blocks_per_cell == 16
            && config.options.primary_beam == 8 && config.options.boundary_beam == 24
            && config.options.max_cells == 24 && config.options.max_cell_gets == 24
            && config.options.max_cell_bytes <= 16 * 1024 * 1024, "probe diagnostic fixed24/all16 options")?;
        let started = (Instant::now(), cpu_ns());
        let layout = probe_open_layout(events,&config.candidate_root,config.max_resident_directory_payload_bytes)?;
        let router = match SourceProbeRouter::open(&layout, &config.probes, config.max_resident_probe_payload_bytes) {
            Ok(router) => router,
            Err(error) => {
                events.error_context["startup_probes"] = json!(error.startup);
                events.error_context["failure"] = serde_json::to_value(&error)?; return Err(error.into());
            }
        };
        events.error_context["startup_probes"] = json!(router.startup);
        events.error_context["startup_probe_time"] = json!(router.startup_time);
        let actual_aggregate = probe_actual_aggregate(&layout, &router, config.max_evaluator_payload_bytes, config.options.max_query_payload_bytes)?;
        let requests = probe_requests(&config.requests, config.first, layout.dimensions(), config.max_evaluator_payload_bytes, &mut events.error_context)?;
        let mut identity = probe_identity(PROBE_DIAGNOSTIC_SCHEMA, config_sha, &config.dataset,
            &config.candidate_root, &config.probes, &config.requests, &layout, &router);
        identity["coverage_gate_sha256"] = json!(config.coverage_gate.sha256);
        identity["options"] = json!(config.options);
        identity["truth"] = json!(config.truth);
        identity["truth_width"] = json!(config.truth_width);
        identity["configured_aggregate"] = configured_aggregate;
        identity["actual_aggregate"] = actual_aggregate;
        identity["startup_complete_wall_ns"] = json!(started.0.elapsed().as_nanos());
        identity["startup_complete_process_cpu_ns"] = json!(cpu_ns()-started.1);
        events.emit(&identity)?;
        for request in &requests {
            events.error_context["ordinal"] = json!(request.ordinal);
            let trace = match layout.search_probed(&router, &request.query, config.top_k, config.options) {
                Ok(trace) => trace,
                Err(error) => { events.error_context["failure"] = serde_json::to_value(&error)?; return Err(error.into()); }
            };
            events.emit(&json!({"phase":"selection_frozen","ordinal":request.ordinal,"truth_opened":false,"trace":trace}))?;
        }
        drop(requests);
        let (prefix, prefix_sha) = freeze_probes(events, output, &identity)?;
        let truths = probe_truth(&config.truth, config.truth_width, layout.rows(), &mut events.error_context)?;
        let file = OpenOptions::new().read(true)
            .custom_flags((rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::NOFOLLOW).bits() as i32).open(output)?;
        require(file.metadata()?.is_file(), "probe diagnostic result regular file")?;
        let mut reader = BufReader::new(file.take(prefix as u64));
        let mut line = String::new();
        let mut hits = Vec::with_capacity(64);
        let mut digest = Sha256::new();
        loop {
            line.clear();
            if reader.by_ref().take((EVENT_CAP + 1) as u64).read_line(&mut line)? == 0 { break; }
            require(line.len() <= EVENT_CAP && line.ends_with('\n'), "probe diagnostic frozen event cap")?;
            digest.update(line.as_bytes());
            let event: Value = serde_json::from_str(&line)?;
            if event["phase"] != "selection_frozen" { continue; }
            let index = hits.len();
            require(index < 64 && event["ordinal"] == json!(index), "probe diagnostic frozen ordinal")?;
            let trace: SourceProbeSearchTrace = serde_json::from_value(event["trace"].clone())?;
            let loss = decompose_loss(&trace.trace, &truths[index])?;
            require(loss.local_nomination_misses == 0, "probe all16 nomination must cover selected rows")?;
            hits.push(loss.returned_hits);
            events.error_context["ordinal"] = json!(index);
            events.emit(&json!({"phase":"loss_attribution","ordinal":index,"loss":loss,
                "truth_sha256":config.truth.sha256,"final_ranking_loss_remeasured":true}))?;
        }
        require(format!("{:x}", digest.finalize()) == prefix_sha, "probe diagnostic frozen prefix authentication")?;
        let (mean, p05, passed) = probe_quality(&mut hits)?;
        Ok(json!({"phase":"terminal","schema":PROBE_DIAGNOSTIC_SCHEMA,"status":if passed { "RETURNED_DIAGNOSTIC_PASS" } else { "FAIL" },
            "complete":true,"queries":64,"truth_opened":true,"mean_recall_at_100":mean,"p05_hits":p05,
            "requires_matching_supervisor_exit_receipt":true,"config_sha256":config_sha,
            "all16_blocks_admitted":true,"final_ranking_loss_remeasured":true,
            "scientific_qualification":false,"quality_or_performance_claim":false,"physical_s3_measured":false}))
    })
}

fn execute() -> Result<()> {
    execute_args(&std::env::args().collect::<Vec<_>>())
}
fn execute_args(args: &[String]) -> Result<()> {
    require(
        args.len() == 5,
        "usage: hierarchical_semantic_cells build|diagnose|nominate|build-probes|nominate-probes|diagnose-probes CONFIG CONFIG_SHA256 NEW_OUTPUT",
    )?;
    let probe_mode = matches!(
        args[1].as_str(),
        "build-probes" | "nominate-probes" | "diagnose-probes"
    );
    let path = Path::new(&args[2]);
    let metadata = if probe_mode {
        fs::symlink_metadata(path)
    } else {
        fs::metadata(path)
    };
    let bytes = match metadata.and_then(|m| usize::try_from(m.len()).map_err(std::io::Error::other))
    {
        Ok(bytes) => bytes,
        Err(error) if probe_mode => {
            return probe_close_invalid(Path::new(&args[4]), &args[3], error.into());
        }
        Err(error) => return Err(error.into()),
    };
    let descriptor = Artifact {
        path: path.into(),
        bytes,
        sha256: args[3].clone(),
    };
    let body = if probe_mode {
        match read_source_probe_artifact(&descriptor, CONFIG_CAP) {
            Ok(body) => body,
            Err(error) => return probe_close_invalid(Path::new(&args[4]), &args[3], error),
        }
    } else {
        descriptor.read(CONFIG_CAP)?
    };
    match args[1].as_str() {
        "build" => {
            let config: BuildConfig = serde_json::from_slice(&body)?;
            let wall = Instant::now();
            let cpu = cpu_ns();
            let receipt = build(&config, Path::new(&args[4]))?;
            println!(
                "{}",
                json!({"status":"BUILT_UNQUALIFIED","config_sha256":args[3],"receipt":receipt,
                "module_source_sha256":hash(include_bytes!("../hierarchical_semantic_cells.rs")),
                "build_wall_ns":wall.elapsed().as_nanos(),"build_process_cpu_ns":cpu_ns()-cpu,
                "process_rss_and_lifetime_hwm_bytes":process_memory_snapshot(),"host_peak_and_scratch_measurement":"external_required"})
            );
            Ok(())
        }
        "diagnose" => diagnose(
            serde_json::from_slice(&body)?,
            &args[3],
            Path::new(&args[4]),
        ),
        "nominate" => nominate(
            serde_json::from_slice(&body)?,
            &args[3],
            Path::new(&args[4]),
        ),
        "build-probes" => build_probes(
            probe_config(&body, &args[3], Path::new(&args[4]))?,
            &args[3],
            Path::new(&args[4]),
        ),
        "nominate-probes" => nominate_probes(
            probe_config(&body, &args[3], Path::new(&args[4]))?,
            &args[3],
            Path::new(&args[4]),
        ),
        "diagnose-probes" => diagnose_probes(
            probe_config(&body, &args[3], Path::new(&args[4]))?,
            &args[3],
            Path::new(&args[4]),
        ),
        _ => Err("unknown command".into()),
    }
}

fn main() {
    if let Err(error) = execute() {
        eprintln!("INVALID: {error}");
        std::process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn probe_artifact(path: &Path, body: &[u8]) -> Artifact {
        fs::write(path, body).unwrap();
        Artifact {
            path: path.into(),
            bytes: body.len(),
            sha256: hash(body),
        }
    }
    fn tiny_probe_inputs(dir: &Path) -> (Artifact, Artifact, Artifact) {
        fn append(bytes: &mut Vec<u8>, body: &[u8]) -> Value {
            let span = json!({"offset":bytes.len(),"bytes":body.len(),"sha256":hash(body)});
            bytes.extend_from_slice(body);
            span
        }
        let codec =
            borsuk::rotated_two_bit::RotatedTwoBitCodec::new(&[0.2, 0.1], 20260923).unwrap();
        let code = codec.encode(&[1., 0.]).unwrap();
        let mut cells = Vec::new();
        let mut nodes = Vec::new();
        for cell in 0..32 {
            let mut source = Vec::new();
            let mut refinement = Vec::new();
            for local in 0..5 {
                let id = (cell * 5 + local) as i64;
                source.extend_from_slice(&id.to_le_bytes());
                source.extend_from_slice(&code);
                refinement.extend_from_slice(&id.to_le_bytes());
                refinement.extend_from_slice(&1_f32.to_le_bytes());
                refinement.extend_from_slice(&[1, 0]);
            }
            let mut whole = source.clone();
            whole.extend_from_slice(&refinement);
            let whole_span =
                json!({"offset":cells.len(),"bytes":whole.len(),"sha256":hash(&whole)});
            let source_span = append(&mut cells, &source);
            let refinement_span = append(&mut cells, &refinement);
            nodes.push(json!({"rows":5,"prototype":[1.,0.],"target":{"kind":"cell","cell":{
                "id":cell,"first_row":cell*5,"whole":whole_span,"source":source_span,"refinement":[refinement_span]}}}));
        }
        let mut directories = Vec::new();
        let mut pages = 0;
        while nodes.len() > 1 {
            nodes = nodes.chunks(2).map(|children| {
                pages += 1;
                let rows = children.iter().map(|v| v["rows"].as_u64().unwrap()).sum::<u64>();
                let span = append(&mut directories,&serde_json::to_vec(&json!({"children":children})).unwrap());
                json!({"rows":rows,"prototype":[1.,0.],"target":{"kind":"directory","span":span}})
            }).collect();
        }
        let unused =
            json!({"path":"unopened-source-only-fixture","bytes":1,"sha256":"a".repeat(64)});
        let root = json!({"schema":"borsuk-hierarchical-cells-resident-v4","input":{
            "schema":"borsuk-hierarchical-cells-build-v2","generation":unused,"plane":unused,"canonical":unused,
            "order":unused,"records":unused,"mean":unused,"sq8":unused,"cell_rows":5,"sample_rows":32,
            "max_depth":24,"max_build_payload_bytes":67108864,"max_output_bytes":16777216},
            "rows":160,"dimensions":2,"seed":20260923,"mean":[0.2_f32,0.1_f32],"low":[-0.125,0.0625],"step":[1.,1.],
            "root_directory":nodes[0]["target"]["span"],"directory_bytes":directories.len(),
            "directory_sha256":hash(&directories),"cell_bytes":cells.len(),
            "build":borsuk::hierarchical_semantic_cells::BuildReceipt {
                cells:32,directories:pages,max_cell_rows:5,max_depth:5,..Default::default() }});
        fs::write(dir.join("cells.bin"), cells).unwrap();
        fs::write(dir.join("directories.bin"), directories).unwrap();
        let root = probe_artifact(
            &dir.join("manifest.json"),
            &serde_json::to_vec(&root).unwrap(),
        );
        let requests = (0..64)
            .map(|ordinal| format!("{}\n", json!({"ordinal":ordinal,"query":[3.125,0.875]})))
            .collect::<String>();
        let requests = probe_artifact(&dir.join("requests.jsonl"), requests.as_bytes());
        let truth = (0..64)
            .flat_map(|_| (0_u32..100).flat_map(u32::to_le_bytes))
            .collect::<Vec<_>>();
        let truth = probe_artifact(&dir.join("truth.bin"), &truth);
        (root, requests, truth)
    }
    fn probe_cli(
        dir: &Path,
        mode: &str,
        config: &Value,
        name: &str,
    ) -> (std::path::PathBuf, Result<()>) {
        let descriptor = probe_artifact(
            &dir.join(format!("{name}-config.json")),
            &serde_json::to_vec(config).unwrap(),
        );
        let output = dir.join(format!("{name}-result.jsonl"));
        let result = execute_args(&[
            "hierarchical_semantic_cells".into(),
            mode.into(),
            descriptor.path.display().to_string(),
            descriptor.sha256,
            output.display().to_string(),
        ]);
        (output, result)
    }
    fn probe_terminal(path: &Path) -> Value {
        serde_json::from_str(fs::read_to_string(path).unwrap().lines().last().unwrap()).unwrap()
    }
    fn probe_after_truth_cap(path: &Path) -> usize {
        let text = fs::read_to_string(path).unwrap();
        let mut bytes = 0;
        for line in text.lines() {
            bytes += line.len() + 1;
            let event: Value = serde_json::from_str(line).unwrap();
            if event["phase"] == "all_selections_frozen" {
                // Enough slack for timer decimal widths, insufficient for all
                // 64 attribution events. Failure must occur after the GT read.
                return bytes + TERMINAL_CAP + 8192;
            }
        }
        panic!("missing freeze marker")
    }
    fn probe_supervisor(dir: &Path, result: &Artifact, run_id: &str, exit_code: i32) -> Artifact {
        let terminal = probe_terminal(&result.path);
        probe_artifact(&dir.join(format!("{run_id}-supervisor.json")), &serde_json::to_vec(&json!({
            "schema":"borsuk-source-witness-coverage-supervisor-v1",
            "closure":{"run_id":run_id,"config_sha256":terminal["config_sha256"],"report_sha256":result.sha256,
                "process_exit_code":exit_code,"resources_closed":true},
            "resources_passed":true,"drain_closed":true,"cleanup_complete":true})).unwrap())
    }
    #[test]
    fn source_probe_cli_actual_full_pipeline_freezes_before_truth_and_closes_invalid() {
        let temp = tempfile::tempdir().unwrap();
        let (root, requests, truth) = tiny_probe_inputs(temp.path());
        let build_config = json!({"schema":PROBE_BUILD_SCHEMA,"candidate_root":root,
            "probe_output":temp.path().join("probes.bin"),"max_resident_directory_payload_bytes":67108864,"max_result_bytes":16777216});
        let (build_path, result) = probe_cli(temp.path(), "build-probes", &build_config, "build");
        result.unwrap();
        let probes: Artifact =
            serde_json::from_value(probe_terminal(&build_path)["artifact"].clone()).unwrap();
        let mut nom_config = json!({"schema":PROBE_NOMINATION_SCHEMA,"dataset":"relaion","candidate_root":root,"probes":probes,
            "requests":requests,"first":0,"count":64,
            "limits":{"max_cells":24,"max_cell_gets":24,"max_cell_bytes":16777216,"max_query_payload_bytes":134217728},
            "max_resident_directory_payload_bytes":67108864,"max_resident_probe_payload_bytes":67108864,
            "max_evaluator_payload_bytes":67108864,"max_result_bytes":16777216,"truth":truth,"truth_width":100});
        let mut receipts = Vec::new();
        for dataset in ["relaion", "cohere"] {
            nom_config["dataset"] = json!(dataset);
            let (path, result) = probe_cli(temp.path(), "nominate-probes", &nom_config, dataset);
            result.unwrap();
            let events = fs::read_to_string(&path)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>();
            let freeze = events
                .iter()
                .position(|e| e["phase"] == "all_selections_frozen")
                .unwrap();
            assert_eq!(
                events[..freeze]
                    .iter()
                    .filter(|e| e["phase"] == "selection_frozen")
                    .count(),
                64
            );
            assert!(events[..=freeze].iter().all(|e| e["truth_opened"] == false));
            assert!(
                events[freeze + 1..]
                    .iter()
                    .any(|e| e["phase"] == "coverage_attribution")
            );
            for e in events.iter().filter(|e| e["phase"] == "selection_frozen") {
                assert_eq!(
                    e["receipt"]["selected"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|s| s["cell_id"].as_u64().unwrap())
                        .collect::<Vec<_>>(),
                    (0..24).collect::<Vec<_>>()
                );
                assert_eq!(
                    e["receipt"]["accounting"]["whole_cell"]["submitted_gets"],
                    0
                );
            }
            // Independent scalar f32 SQ8 formula after the unchanged native
            // cosine preparation of a nonunit query, with nonzero low/mean.
            let norm = (3.125_f64.powi(2) + 0.875_f64.powi(2)).sqrt();
            let q = [(3.125_f64 / norm) as f32, (0.875_f64 / norm) as f32];
            let mut shift = 0_f32;
            let mut qnorm = 0_f32;
            let mut weights = [0_f32; 2];
            for d in 0..2 {
                shift += q[d] * [-0.125_f32, 0.0625][d];
                qnorm += q[d] * q[d];
                weights[d] = q[d] * 1_f32;
            }
            shift -= qnorm / 2_f32;
            let mut inner = 0_f32;
            for d in 0..2 {
                inner += f32::from([1_u8, 0][d]) * weights[d];
            }
            let expected = 1_f32 - 2_f32 * (inner + shift);
            for e in events.iter().filter(|e| e["phase"] == "selection_frozen") {
                for choice in e["receipt"]["selected"].as_array().unwrap() {
                    assert_eq!(
                        choice["score_bits"].as_u64(),
                        Some(u64::from(expected.to_bits()))
                    );
                }
            }
            assert_eq!(probe_terminal(&path)["status"], "COVERAGE_PASS");
            let bytes = fs::read(&path).unwrap();
            receipts.push(Artifact {
                path,
                bytes: bytes.len(),
                sha256: hash(&bytes),
            });
        }
        let relaion_supervisor = probe_supervisor(temp.path(), &receipts[0], "relaion-original", 0);
        let cohere_supervisor = probe_supervisor(temp.path(), &receipts[1], "cohere-original", 0);
        let gate_body = json!({"schema":PROBE_GATE_SCHEMA,
            "relaion":{"run_id":"relaion-original","result":receipts[0],"supervisor":relaion_supervisor},
            "cohere":{"run_id":"cohere-original","result":receipts[1],"supervisor":cohere_supervisor}});
        let gate = probe_artifact(
            &temp.path().join("gate.json"),
            &serde_json::to_vec(&gate_body).unwrap(),
        );
        let diagnostic = json!({"schema":PROBE_DIAGNOSTIC_SCHEMA,"dataset":"relaion","candidate_root":root,"probes":probes,
            "requests":requests,"coverage_gate":gate,"first":0,"count":64,"top_k":100,"truth":truth,"truth_width":100,
            "options":{"fetch_policy":"whole_cell","primary_beam":8,"boundary_beam":24,"blocks_per_cell":16,
                "max_cells":24,"max_cell_gets":24,"max_cell_bytes":16777216,"max_source_gets":24,"max_source_bytes":16777216,
                "max_refinement_gets":384,"max_refinement_bytes":16777216,"max_query_payload_bytes":134217728},
            "max_resident_directory_payload_bytes":67108864,"max_resident_probe_payload_bytes":67108864,
            "max_evaluator_payload_bytes":67108864,"max_result_bytes":16777216});
        let (path, result) = probe_cli(temp.path(), "diagnose-probes", &diagnostic, "diagnostic");
        result.unwrap();
        assert_eq!(probe_terminal(&path)["status"], "RETURNED_DIAGNOSTIC_PASS");
        let text = fs::read_to_string(&path).unwrap();
        let events = text
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let freeze = events
            .iter()
            .position(|e| e["phase"] == "all_selections_frozen")
            .unwrap();
        assert_eq!(
            events[..freeze]
                .iter()
                .filter(|e| e["phase"] == "selection_frozen")
                .count(),
            64
        );
        for e in events.iter().filter(|e| e["phase"] == "selection_frozen") {
            assert_eq!(
                e["trace"]["trace"]["accounting"]["whole_cell"]["submitted_gets"],
                24
            );
            assert_eq!(
                e["trace"]["trace"]["covered_ids"],
                e["trace"]["trace"]["nominated_ids"]
            );
            assert!(
                e["trace"]["complete"]["wall_ns"].as_u64().unwrap()
                    >= e["trace"]["nomination"]["complete"]["wall_ns"]
                        .as_u64()
                        .unwrap()
            );
        }
        for (name, mutation) in [
            ("exit2", json!({"closure":{"process_exit_code":2}})),
            ("cohere-exit2", json!({"closure":{"process_exit_code":2}})),
            (
                "resource-closure",
                json!({"closure":{"resources_closed":false}}),
            ),
            ("resource", json!({"resources_passed":false})),
            ("drain", json!({"drain_closed":false})),
            ("cleanup", json!({"cleanup_complete":false})),
            (
                "wrong-config",
                json!({"closure":{"config_sha256":"0".repeat(64)}}),
            ),
            (
                "wrong-report",
                json!({"closure":{"report_sha256":"0".repeat(64)}}),
            ),
            ("wrong-run", json!({"closure":{"run_id":"different-run"}})),
        ] {
            let dataset = if name.starts_with("cohere") {
                "cohere"
            } else {
                "relaion"
            };
            let original = read_source_probe_artifact(
                if dataset == "cohere" {
                    &cohere_supervisor
                } else {
                    &relaion_supervisor
                },
                CONFIG_CAP,
            )
            .unwrap();
            let mut bad: Value = serde_json::from_slice(&original).unwrap();
            for (key, value) in mutation.as_object().unwrap() {
                if key == "closure" {
                    for (field, value) in value.as_object().unwrap() {
                        bad["closure"][field] = value.clone();
                    }
                } else {
                    bad[key] = value.clone();
                }
            }
            let supervisor = probe_artifact(
                &temp.path().join(format!("{name}.json")),
                &serde_json::to_vec(&bad).unwrap(),
            );
            let mut rejected_gate = gate_body.clone();
            rejected_gate[dataset]["supervisor"] = json!(supervisor);
            let descriptor = probe_artifact(
                &temp.path().join(format!("{name}-gate.json")),
                &serde_json::to_vec(&rejected_gate).unwrap(),
            );
            let mut rejected = diagnostic.clone();
            rejected["coverage_gate"] = json!(descriptor);
            let (path, result) = probe_cli(temp.path(), "diagnose-probes", &rejected, name);
            assert!(result.is_err());
            assert_eq!(probe_terminal(&path)["status"], "INVALID");
            assert!(
                !fs::read_to_string(path)
                    .unwrap()
                    .contains("\"phase\":\"selection_frozen\"")
            );
        }
        let body_only = probe_artifact(
            &temp.path().join("body-only-gate.json"),
            &serde_json::to_vec(&json!({
            "schema":PROBE_GATE_SCHEMA,"relaion":receipts[0],"cohere":receipts[1]}))
            .unwrap(),
        );
        let mut rejected = diagnostic.clone();
        rejected["coverage_gate"] = json!(body_only);
        let (path, result) = probe_cli(temp.path(), "diagnose-probes", &rejected, "body-only");
        assert!(result.is_err());
        assert_eq!(probe_terminal(&path)["status"], "INVALID");
        let mut rejected = diagnostic.clone();
        rejected["options"]["max_query_payload_bytes"] = json!(536870912);
        rejected["coverage_gate"]["path"] = json!(temp.path().join("must-not-open-gate"));
        let (path, result) = probe_cli(
            temp.path(),
            "diagnose-probes",
            &rejected,
            "aggregate-before-gate",
        );
        assert!(result.is_err());
        assert!(
            probe_terminal(&path)["error"]
                .as_str()
                .unwrap()
                .contains("configured aggregate")
        );
        for (name, field, value) in [
            ("different-truth-sha", "sha256", json!("a".repeat(64))),
            ("different-truth-length", "bytes", json!(truth.bytes + 4)),
            (
                "different-truth-path",
                "path",
                json!(temp.path().join("different-truth.bin")),
            ),
        ] {
            let mut different = diagnostic.clone();
            different["truth"][field] = value;
            let (path, result) = probe_cli(temp.path(), "diagnose-probes", &different, name);
            assert!(result.is_err());
            let terminal = probe_terminal(&path);
            assert_eq!(terminal["status"], "INVALID");
            assert_eq!(terminal["truth_opened"], false);
            assert!(
                terminal["error"]
                    .as_str()
                    .unwrap()
                    .contains("truth descriptor identity")
            );
            assert!(
                !fs::read_to_string(path)
                    .unwrap()
                    .contains("\"phase\":\"selection_frozen\"")
            );
        }
        let mut wrong_width = diagnostic.clone();
        wrong_width["truth_width"] = json!(99);
        let (path, result) = probe_cli(
            temp.path(),
            "diagnose-probes",
            &wrong_width,
            "different-truth-width",
        );
        assert!(result.is_err());
        assert_eq!(probe_terminal(&path)["truth_opened"], false);
        assert!(
            !fs::read_to_string(path)
                .unwrap()
                .contains("\"phase\":\"selection_frozen\"")
        );
        // The descriptor still matches the coverage pin, but its original file
        // is now missing. Admission succeeds; all64 selections freeze first.
        let truth_body = fs::read(&truth.path).unwrap();
        fs::remove_file(&truth.path).unwrap();
        let (path, result) =
            probe_cli(temp.path(), "diagnose-probes", &diagnostic, "missing-truth");
        assert!(result.is_err());
        let terminal = probe_terminal(&path);
        assert_eq!(terminal["status"], "INVALID");
        assert_eq!(terminal["truth_read_attempted"], true);
        assert!(terminal["truth_opened"].is_null());
        let text = fs::read_to_string(path).unwrap();
        assert_eq!(
            text.lines()
                .filter(|line| line.contains("\"phase\":\"selection_frozen\""))
                .count(),
            64
        );
        assert!(text.contains("\"phase\":\"all_selections_frozen\""));
        fs::write(&truth.path, truth_body).unwrap();
        // Both replay/attribution paths must preserve true openness when the
        // bounded result fills only after the authenticated GT read.
        let mut coverage_capped = nom_config.clone();
        coverage_capped["dataset"] = json!("relaion");
        coverage_capped["max_result_bytes"] = json!(probe_after_truth_cap(&receipts[0].path));
        let (path, result) = probe_cli(
            temp.path(),
            "nominate-probes",
            &coverage_capped,
            "coverage-post-truth-cap",
        );
        assert!(result.is_err());
        assert_eq!(probe_terminal(&path)["status"], "INVALID");
        assert_eq!(probe_terminal(&path)["truth_opened"], true);
        let mut diagnostic_capped = diagnostic.clone();
        let successful_path = temp.path().join("diagnostic-result.jsonl");
        diagnostic_capped["max_result_bytes"] = json!(probe_after_truth_cap(&successful_path));
        let (path, result) = probe_cli(
            temp.path(),
            "diagnose-probes",
            &diagnostic_capped,
            "diagnostic-post-truth-cap",
        );
        assert!(result.is_err());
        assert_eq!(probe_terminal(&path)["status"], "INVALID");
        assert_eq!(probe_terminal(&path)["truth_opened"], true);
        let mut failed_gate = diagnostic;
        failed_gate["coverage_gate"]["sha256"] = json!("0".repeat(64));
        let (path, result) = probe_cli(temp.path(), "diagnose-probes", &failed_gate, "failed-gate");
        assert!(result.is_err());
        assert_eq!(probe_terminal(&path)["status"], "INVALID");
        assert!(
            !fs::read_to_string(path)
                .unwrap()
                .contains("\"phase\":\"selection_frozen\"")
        );
        // Source-only strict config rejects any attempt to supply requests/GT.
        let mut leaked = build_config;
        leaked["truth"] = json!(truth);
        let (path, result) = probe_cli(temp.path(), "build-probes", &leaked, "leaked");
        assert!(result.is_err());
        assert_eq!(probe_terminal(&path)["status"], "INVALID");
    }
    #[test]
    fn source_probe_cli_coverage_fifo_truth_and_output_cap_close_invalid() {
        let temp = tempfile::tempdir().unwrap();
        let (root, requests, truth) = tiny_probe_inputs(temp.path());
        let layout = Prototype::open_for_source_probes(&root, 67108864).unwrap();
        let probes = build_source_probes(&layout, &temp.path().join("probes.bin"))
            .unwrap()
            .artifact;
        let fifo = temp.path().join("truth-fifo");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        let config = json!({"schema":PROBE_NOMINATION_SCHEMA,"dataset":"relaion","candidate_root":root,"probes":probes,
            "requests":requests,"first":0,"count":64,
            "limits":{"max_cells":24,"max_cell_gets":24,"max_cell_bytes":16777216,"max_query_payload_bytes":134217728},
            "max_resident_directory_payload_bytes":67108864,"max_resident_probe_payload_bytes":67108864,
            "max_evaluator_payload_bytes":67108864,"max_result_bytes":16777216,
            "truth":{"path":fifo,"bytes":truth.bytes,"sha256":truth.sha256},"truth_width":100});
        let (path, result) = probe_cli(temp.path(), "nominate-probes", &config, "fifo-truth");
        assert!(result.is_err());
        assert_eq!(probe_terminal(&path)["status"], "INVALID");
        assert!(
            fs::read_to_string(path)
                .unwrap()
                .contains("\"phase\":\"all_selections_frozen\"")
        );
        let terminal = probe_terminal(&temp.path().join("fifo-truth-result.jsonl"));
        assert_eq!(terminal["truth_read_attempted"], true);
        assert!(terminal["truth_opened"].is_null());
        let bad_body = vec![0_u8; 64 * 100 * 4];
        let bad_truth = probe_artifact(&temp.path().join("invalid-truth-ids.bin"), &bad_body);
        let mut invalid = config.clone();
        invalid["truth"] = json!(bad_truth);
        let (path, result) = probe_cli(
            temp.path(),
            "nominate-probes",
            &invalid,
            "invalid-truth-ids",
        );
        assert!(result.is_err());
        assert_eq!(probe_terminal(&path)["status"], "INVALID");
        assert_eq!(probe_terminal(&path)["truth_opened"], true);
        assert_eq!(probe_terminal(&path)["truth_read_complete"], true);
        let mut over = config.clone();
        over["max_resident_directory_payload_bytes"] = json!(536870912);
        over["candidate_root"]["path"] = json!(temp.path().join("must-not-open-root"));
        let (path, result) = probe_cli(
            temp.path(),
            "nominate-probes",
            &over,
            "aggregate-before-root",
        );
        assert!(result.is_err());
        assert!(
            probe_terminal(&path)["error"]
                .as_str()
                .unwrap()
                .contains("configured aggregate")
        );
        assert!(
            !fs::read_to_string(path)
                .unwrap()
                .contains("\"phase\":\"identity\"")
        );
        assert!(probe_aggregate(usize::MAX, 1, 1, 1).is_err());
        assert!(
            probe_aggregate(
                64 * 1024 * 1024,
                64 * 1024 * 1024,
                64 * 1024 * 1024,
                128 * 1024 * 1024
            )
            .is_ok()
        );
        let mut bad_root = config.clone();
        bad_root["candidate_root"]["sha256"] = json!("0".repeat(64));
        let (path, result) = probe_cli(
            temp.path(),
            "nominate-probes",
            &bad_root,
            "startup-bad-root",
        );
        assert!(result.is_err());
        let terminal = probe_terminal(&path);
        assert_eq!(terminal["startup_root"]["submitted_gets"], 1);
        assert_eq!(terminal["startup_root"]["failed_gets"], 1);
        assert_eq!(terminal["startup_root"]["verified_bytes"], 0);
        assert_eq!(terminal["startup_directory"]["submitted_gets"], 0);
        let directory_path = temp.path().join("directories.bin");
        let original = fs::read(&directory_path).unwrap();
        let mut tampered = original.clone();
        tampered[0] ^= 1;
        fs::write(&directory_path, tampered).unwrap();
        let (path, result) = probe_cli(
            temp.path(),
            "nominate-probes",
            &config,
            "startup-bad-directory",
        );
        assert!(result.is_err());
        let terminal = probe_terminal(&path);
        assert_eq!(terminal["startup_root"]["verified_bytes"], root.bytes);
        assert_eq!(terminal["startup_directory"]["submitted_gets"], 1);
        assert_eq!(terminal["startup_directory"]["failed_gets"], 1);
        assert_eq!(terminal["startup_directory"]["verified_bytes"], 0);
        fs::write(&directory_path, original).unwrap();
        let mut bad_probe = config.clone();
        bad_probe["probes"]["sha256"] = json!("0".repeat(64));
        let (path, result) = probe_cli(
            temp.path(),
            "nominate-probes",
            &bad_probe,
            "startup-bad-probe",
        );
        assert!(result.is_err());
        let terminal = probe_terminal(&path);
        assert_eq!(terminal["startup_root"]["verified_bytes"], root.bytes);
        assert!(
            terminal["startup_directory"]["verified_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(terminal["startup_probes"]["submitted_gets"], 1);
        assert_eq!(terminal["startup_probes"]["failed_gets"], 1);
        let mut bad_request = config.clone();
        bad_request["requests"]["sha256"] = json!("0".repeat(64));
        let (path, result) = probe_cli(
            temp.path(),
            "nominate-probes",
            &bad_request,
            "startup-bad-request",
        );
        assert!(result.is_err());
        let terminal = probe_terminal(&path);
        assert_eq!(terminal["startup_root"]["verified_bytes"], root.bytes);
        assert!(
            terminal["startup_directory"]["verified_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(terminal["startup_probes"]["submitted_gets"], 1);
        assert_eq!(terminal["startup_probes"]["verified_bytes"], probes.bytes);
        assert_eq!(terminal["startup_requests"]["submitted_gets"], 1);
        assert_eq!(terminal["startup_requests"]["failed_gets"], 1);
        assert_eq!(terminal["startup_requests"]["verified_bytes"], 0);
        assert!(
            !fs::read_to_string(path)
                .unwrap()
                .contains("\"phase\":\"selection_frozen\"")
        );
        let mut capped = config;
        capped["max_result_bytes"] = json!(8192);
        let (path, result) = probe_cli(temp.path(), "nominate-probes", &capped, "capped");
        assert!(result.is_err());
        assert_eq!(probe_terminal(&path)["status"], "INVALID");
        assert!(fs::metadata(path).unwrap().len() <= 8192);
    }

    fn nomination_fixture(dir: &Path) -> NominationConfig {
        // Hand-authored, authenticated synthetic layout only. No corpus, truth,
        // builder execution, training or production fallback enters this helper.
        fn append(bytes: &mut Vec<u8>, body: &[u8]) -> Value {
            let span = json!({"offset":bytes.len(),"bytes":body.len(),"sha256":hash(body)});
            bytes.extend_from_slice(body);
            span
        }
        let record_bytes = borsuk::rotated_two_bit::RotatedTwoBitCodec::new(&[0., 0.], 20260923)
            .unwrap().record_bytes();
        let mut cells = Vec::new();
        let mut nodes = Vec::new();
        for cell in 0..32 {
            let id = 31_i64 - cell as i64;
            let mut source = id.to_le_bytes().to_vec();
            source.resize(8 + record_bytes, 0);
            let mut refinement = id.to_le_bytes().to_vec();
            refinement.extend_from_slice(&1_f32.to_le_bytes());
            refinement.extend_from_slice(&[1, 0]);
            let mut whole = source.clone();
            whole.extend_from_slice(&refinement);
            let whole_span = json!({"offset":cells.len(),"bytes":whole.len(),"sha256":hash(&whole)});
            let source_span = append(&mut cells, &source);
            let refinement_span = append(&mut cells, &refinement);
            nodes.push(json!({"rows":1,"prototype":[1.,0.],"target":{"kind":"cell",
                "cell":{"id":cell,"first_row":cell,"whole":whole_span,
                    "source":source_span,"refinement":[refinement_span]}}}));
        }
        let mut directories = Vec::new();
        let mut pages = 0;
        while nodes.len() > 1 {
            nodes = nodes.chunks(2).map(|children| {
                pages += 1;
                let rows = children.iter().map(|v| v["rows"].as_u64().unwrap()).sum::<u64>();
                let span = append(&mut directories, &serde_json::to_vec(&json!({"children":children})).unwrap());
                json!({"rows":rows,"prototype":[1.,0.],"target":{"kind":"directory","span":span}})
            }).collect();
        }
        let descriptor = json!({"path":"unused-synthetic-input","bytes":1,"sha256":"a".repeat(64)});
        let root = json!({"schema":"borsuk-hierarchical-cells-resident-v4",
            "input":{"schema":"borsuk-hierarchical-cells-build-v2",
                "generation":descriptor,"plane":descriptor,"canonical":descriptor,
                "order":descriptor,"records":descriptor,"mean":descriptor,"sq8":descriptor,
                "cell_rows":1,"sample_rows":32,"max_depth":24,
                "max_build_payload_bytes":67108864,"max_output_bytes":16777216},
            "rows":32,"dimensions":2,"seed":20260923,"mean":[0.,0.],"low":[0.,0.],"step":[1.,1.],
            "root_directory":nodes[0]["target"]["span"],
            "directory_bytes":directories.len(),"directory_sha256":hash(&directories),"cell_bytes":cells.len(),
            "build":borsuk::hierarchical_semantic_cells::BuildReceipt {
                cells:32, directories:pages, max_cell_rows:1, max_depth:5, ..Default::default()
            }});
        fs::write(dir.join("cells.bin"), cells).unwrap();
        fs::write(dir.join("directories.bin"), directories).unwrap();
        let root_bytes = serde_json::to_vec(&root).unwrap();
        fs::write(dir.join("manifest.json"), &root_bytes).unwrap();
        let requests = (10..74).map(|ordinal| format!("{}\n", json!({"ordinal":ordinal,"query":[3.,0.]}))).collect::<String>();
        fs::write(dir.join("requests.jsonl"), &requests).unwrap();
        NominationConfig {
            schema:NOMINATION_SCHEMA.into(),
            candidate_root:Artifact { path:dir.join("manifest.json"),bytes:root_bytes.len(),sha256:hash(&root_bytes) },
            requests:Artifact { path:dir.join("requests.jsonl"),bytes:requests.len(),sha256:hash(requests.as_bytes()) },
            first:10,count:64,
            limits:NominationLimits {max_source_gets:24,max_source_bytes:1048576,max_query_payload_bytes:67108864},
            max_resident_directory_payload_bytes:67108864,
            max_evaluator_payload_bytes:67108864,max_result_bytes:16777216,
        }
    }

    #[test]
    fn nomination_cli_full64_freezes_prefix_and_both_policies_without_truth() {
        let temp = tempfile::tempdir().unwrap();
        let config = nomination_fixture(temp.path());
        let path = temp.path().join("nomination.jsonl");
        nominate(config, &"b".repeat(64), &path).unwrap();
        let bytes = fs::read(&path).unwrap();
        let events = std::str::from_utf8(&bytes).unwrap().lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
        assert_eq!(events.len(), 131);
        for (index, event) in events[1..129].iter().enumerate() {
            assert_eq!(event["phase"], "selection_frozen");
            assert_eq!(event["ordinal"], 10 + index / 2);
            assert_eq!(event["receipt"]["policy"], json!(NOMINATION_POLICIES[index % 2]));
            assert_eq!(event["receipt"]["selected"].as_array().unwrap().len(), 24);
            // The concatenation offset is deliberately different from source ID.
            assert_eq!(event["receipt"]["selected"][0]["first_row"], 0);
            assert_eq!(event["receipt"]["selected"][0]["source_ids"], json!([31]));
            assert_eq!(event["receipt"]["accounting"]["refinement"]["submitted_gets"], 0);
        }
        let marker = &events[129];
        assert_eq!(marker["phase"], "all_selections_frozen");
        let prefix_bytes = marker["prefix_bytes"].as_u64().unwrap() as usize;
        assert_eq!(marker["prefix_sha256"], hash(&bytes[..prefix_bytes]));
        for key in ["config_sha256","candidate_root_sha256","requests_sha256",
            "module_source_sha256","binary_source_sha256","source_identity_sha256"] {
            assert_eq!(marker[key], events[0][key]);
        }
        assert_eq!(events[130]["status"], "NOMINATIONS_FROZEN");
        assert!(events.iter().all(|event| event["truth_opened"] == false));
        assert!(nominate(nomination_fixture(temp.path()), &"b".repeat(64), &path).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn nomination_cli_rejects_truth_fields_incomplete_panels_and_tampering() {
        let temp = tempfile::tempdir().unwrap();
        let config = nomination_fixture(temp.path());
        let value = json!({"schema":NOMINATION_SCHEMA,"candidate_root":config.candidate_root,
            "requests":config.requests,"first":10,"count":64,"limits":config.limits,
            "max_resident_directory_payload_bytes":67108864,"max_evaluator_payload_bytes":67108864,
            "max_result_bytes":16777216});
        for field in ["truth", "truth_width", "options", "unknown"] {
            let mut bad = value.clone();
            bad[field] = Value::Null;
            assert!(serde_json::from_value::<NominationConfig>(bad).is_err());
        }
        let mut bad = value;
        bad["limits"]["primary_beam"] = json!(8);
        assert!(serde_json::from_value::<NominationConfig>(bad).is_err());
        for case in ["count", "ordinal", "short", "truth_request", "request_hash", "root_hash", "source_hash", "cap"] {
            let mut config = nomination_fixture(temp.path());
            match case {
                "count" => config.count = 63,
                "root_hash" => config.candidate_root.sha256 = "0".repeat(64),
                "request_hash" => config.requests.sha256 = "0".repeat(64),
                "source_hash" => {
                    let mut cells = fs::read(temp.path().join("cells.bin")).unwrap();
                    cells[0] ^= 1;
                    fs::write(temp.path().join("cells.bin"), cells).unwrap();
                }
                "cap" => config.max_result_bytes = TERMINAL_CAP + FREEZE_CAP,
                _ => {
                    let mut requests = fs::read_to_string(&config.requests.path).unwrap();
                    match case {
                        "ordinal" => requests = requests.replacen("10", "11", 1),
                        "short" => requests = requests.lines().take(63).map(|line| format!("{line}\n")).collect(),
                        _ => requests = requests.replacen("{", "{\"truth\":[],", 1),
                    }
                    fs::write(&config.requests.path, &requests).unwrap();
                    config.requests.bytes = requests.len();
                    config.requests.sha256 = hash(requests.as_bytes());
                }
            }
            let path = temp.path().join(format!("{case}.jsonl"));
            assert!(nominate(config, &"b".repeat(64), &path).is_err());
            if path.exists() {
                let body = fs::read_to_string(&path).unwrap();
                assert!(!body.contains("all_selections_frozen"));
                let terminal: Value = serde_json::from_str(body.lines().last().unwrap()).unwrap();
                assert_eq!(terminal["status"], "INVALID");
            }
        }
    }

    #[test]
    fn nomination_prefix_rejects_fifo_and_symlink_without_blocking() {
        let temp = tempfile::tempdir().unwrap();
        let regular = temp.path().join("regular");
        fs::write(&regular, b"prefix\n").unwrap();
        verify_nomination_prefix(&regular, 7, &hash(b"prefix\n")).unwrap();
        let symlink = temp.path().join("symlink");
        std::os::unix::fs::symlink(&regular, &symlink).unwrap();
        assert!(verify_nomination_prefix(&symlink, 7, &hash(b"prefix\n")).is_err());
        let fifo = temp.path().join("fifo");
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
        // No writer exists: a blocking open would hang before type admission.
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            send.send(verify_nomination_prefix(&fifo, 0, &hash(b"")).is_err())
        });
        assert!(receive.recv_timeout(std::time::Duration::from_secs(1)).unwrap());
        worker.join().unwrap().unwrap();
    }

    #[test]
    fn nomination_freeze_reserve_and_synced_prefix_tamper_close_invalid() {
        let temp = tempfile::tempdir().unwrap();
        for tamper in [false, true] {
            let path = temp.path().join(if tamper { "tamper" } else { "reserved" });
            let identity = json!({"first":10});
            let event = json!({"phase":"test_prefix"});
            let prefix_size = serde_json::to_vec(&event).unwrap().len() + 1;
            let mut events = Events::new(&path, TERMINAL_CAP + prefix_size).unwrap();
            let result = events.run(|events| {
                events.emit(&event)?;
                if tamper {
                    events.sync()?;
                    let mut altered = fs::read(&path)?;
                    altered[0] ^= 1;
                    fs::write(&path, altered)?;
                }
                freeze_nominations(events, &path, &identity)?;
                Ok(json!({"phase":"terminal","status":"NOMINATIONS_FROZEN"}))
            });
            assert_eq!(result.is_err(), tamper);
            let bytes = fs::read(&path).unwrap();
            assert!(bytes.len() <= TERMINAL_CAP + prefix_size + FREEZE_CAP);
            let text = String::from_utf8(bytes).unwrap();
            assert_eq!(text.contains("all_selections_frozen"), !tamper);
            let terminal: Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
            assert_eq!(terminal["status"], if tamper { "INVALID" } else { "NOMINATIONS_FROZEN" });
        }
    }

    #[test]
    fn created_outputs_close_invalid_on_bad_truth_or_output_cap_without_overwrite() {
        let temp = tempfile::tempdir().unwrap();
        let bad_truth = Artifact {
            path: temp.path().join("mock-truth"),
            bytes: 4,
            sha256: "0".repeat(64),
        };
        fs::write(&bad_truth.path, [1_u8, 2, 3, 4]).unwrap();
        let frozen = json!({"phase":"query_frozen","ordinal":0,"truth_opened":false,"trace":{"covered_ids":[0]}});
        let marker = json!({"phase":"all_queries_frozen","truth_opened":false});
        let mut prefix = serde_json::to_vec(&frozen).unwrap();
        prefix.push(b'\n');
        prefix.extend(serde_json::to_vec(&marker).unwrap());
        prefix.push(b'\n');
        for truth_error in [true, false] {
            let path = temp.path().join(if truth_error {
                "bad-truth.jsonl"
            } else {
                "cap.jsonl"
            });
            let mut events = Events::new(&path, TERMINAL_CAP + 512).unwrap();
            let outcome = events.run(|events| {
                events.emit(&frozen)?;
                events.emit(&marker)?;
                events.sync()?;
                if truth_error {
                    events.error_context = json!({"truth_read_attempted":true});
                    drop(bad_truth.read(4)?);
                } else {
                    events.emit(&json!({"phase":"loss_attribution","padding":"x".repeat(1024)}))?;
                }
                Ok(json!({"phase":"terminal","status":"DIAGNOSTIC","complete":true}))
            });
            assert!(outcome.is_err());
            let body = fs::read(&path).unwrap();
            assert!(body.starts_with(&prefix));
            assert_eq!(body.len(), events.bytes);
            assert!(body.len() <= events.cap);
            let lines = std::str::from_utf8(&body)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(lines.len(), 3);
            let terminal = lines.last().unwrap();
            assert_eq!(terminal["phase"], "terminal");
            assert_eq!(terminal["status"], "INVALID");
            assert_eq!(terminal["complete"], false);
            assert_eq!(terminal["scientific_qualification"], false);
            assert!(Events::new(&path, events.cap).is_err());
            assert_eq!(fs::read(&path).unwrap(), body);
        }
        let success = temp.path().join("success.jsonl");
        let mut events = Events::new(&success, TERMINAL_CAP).unwrap();
        events
            .run(|_| Ok(json!({"phase":"terminal","status":"DIAGNOSTIC","complete":true})))
            .unwrap();
        let terminal: Value = serde_json::from_slice(&fs::read(success).unwrap()).unwrap();
        assert_eq!(terminal["status"], "DIAGNOSTIC");
        let too_small = temp.path().join("too-small.jsonl");
        assert!(Events::new(&too_small, TERMINAL_CAP - 1).is_err());
        assert!(!too_small.exists());
    }

    #[test]
    fn configurations_reject_unknown_fields_and_truth_in_requests() {
        assert!(
            serde_json::from_str::<Request>(r#"{"ordinal":0,"query":[1.0],"truth":[0]}"#).is_err()
        );
        let value = json!({"schema":DIAGNOSTIC_SCHEMA,"candidate_root":{"path":"manifest.json","bytes":1,"sha256":"a".repeat(64)},
            "requests":{"path":"requests","bytes":1,"sha256":"b".repeat(64)},"first":0,"count":1,"top_k":1,
            "options":{"fetch_policy":"whole_cell","primary_beam":1,"boundary_beam":2,"blocks_per_cell":1,
                "max_cells":32,"max_cell_gets":32,"max_cell_bytes":16777216,
                "max_source_gets":4,"max_source_bytes":65536,
                "max_refinement_gets":4,"max_refinement_bytes":65536,"max_query_payload_bytes":8388608},
            "truth":null,"truth_width":0,"max_resident_directory_payload_bytes":67108864,
            "max_evaluator_payload_bytes":67108864,"max_result_bytes":1048576});
        let config = serde_json::from_value::<DiagnosticConfig>(value.clone()).unwrap();
        admit_fetch_policy(config.options).unwrap();
        let mut synthetic_only = config.options;
        synthetic_only.fetch_policy = FetchPolicy::TwoStage;
        assert!(admit_fetch_policy(synthetic_only).is_err());
        let mut oversized = config.options;
        oversized.max_cell_gets = 33;
        assert!(admit_fetch_policy(oversized).is_err());
        let mut wrong = value;
        wrong["nominees"] = json!([0]);
        assert!(serde_json::from_value::<DiagnosticConfig>(wrong).is_err());
    }
}
