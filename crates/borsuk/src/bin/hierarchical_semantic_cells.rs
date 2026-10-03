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
//! Actual GET/byte counters describe this serial local range-read adapter only.
//! Stage RSS is an end snapshot and lifetime HWM, not independent stage peaks;
//! external monitoring is required for CPU/RSS/cgroup/build-scratch qualification.

use borsuk::hierarchical_semantic_cells::{
    Artifact, BuildConfig, FetchPolicy, NominationLimits, NominationPolicy, Prototype, Result,
    SearchOptions, SearchTrace, build,
    decompose_loss, process_memory_snapshot,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
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
    let mut file = File::open(output)?;
    require(file.metadata()?.len() == bytes as u64, "nomination prefix exact length")?;
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

fn execute() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    require(
        args.len() == 5,
        "usage: hierarchical_semantic_cells build|diagnose|nominate CONFIG CONFIG_SHA256 NEW_OUTPUT",
    )?;
    let path = Path::new(&args[2]);
    let bytes = usize::try_from(fs::metadata(path)?.len())?;
    let body = Artifact {
        path: path.into(),
        bytes,
        sha256: args[3].clone(),
    }
    .read(CONFIG_CAP)?;
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
        let root = json!({"schema":"borsuk-hierarchical-cells-resident-v3",
            "input":{"schema":"borsuk-hierarchical-cells-build-v1",
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
