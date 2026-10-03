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
    Artifact, BuildConfig, FetchPolicy, Prototype, Result, SearchOptions, SearchTrace, build,
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
fn execute() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    require(
        args.len() == 5,
        "usage: hierarchical_semantic_cells build|diagnose CONFIG CONFIG_SHA256 NEW_OUTPUT",
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
