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
use borsuk::semantic_cell_overlap::{self as overlap, OverlapBuildConfig,
    OverlapPairAdmission, OverlapRevisions, OverlapSearchTrace, search_selected_sq8};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::os::unix::fs::FileExt;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
    time::Instant,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FinePanel { dataset:String, root:Artifact, requests:Artifact, truth:Artifact, truth_width:usize }
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FinePairedConfig {
    schema:String, panels:[FinePanel;2], source_identity_sha256:String,
    limits:borsuk::fine_sq8_groups::ResidentLimits,
    max_evaluator_payload_bytes:usize, max_result_bytes:usize,
    #[cfg(test)] #[serde(skip)] test_geometry:Option<(usize,usize)>,
    #[cfg(test)] #[serde(skip)] fail_sync_at:Option<usize>,
}
fn fine_source_identity() -> String {
    let mut digest = Sha256::new();
    for (name, bytes) in [
        ("fine_sq8_groups.rs", include_bytes!("../fine_sq8_groups.rs").as_slice()),
        ("pq64_nominee.rs", include_bytes!("../pq64_nominee.rs").as_slice()),
        ("resident_vector_graph.rs", include_bytes!("../resident_vector_graph.rs").as_slice()),
        ("hierarchical_semantic_cells.rs", include_bytes!("../hierarchical_semantic_cells.rs").as_slice()),
        ("budgeted_page_rank.rs", include_bytes!("../budgeted_page_rank.rs").as_slice()),
        ("sq8_page_authority.rs", include_bytes!("../sq8_page_authority.rs").as_slice()),
        ("returned_sq8.rs", include_bytes!("../returned_sq8.rs").as_slice()),
        ("exact_sq8_nominee.rs", include_bytes!("../exact_sq8_nominee.rs").as_slice()),
        ("centroid_hnsw.rs", include_bytes!("../centroid_hnsw.rs").as_slice()),
        ("sq8_source.rs", include_bytes!("../sq8_source.rs").as_slice()),
        ("bin/hierarchical_semantic_cells.rs", include_bytes!("hierarchical_semantic_cells.rs").as_slice()),
        ("lib.rs", include_bytes!("../lib.rs").as_slice()),
    ] {
        digest.update((name.len() as u64).to_le_bytes()); digest.update(name.as_bytes());
        digest.update((bytes.len() as u64).to_le_bytes()); digest.update(bytes);
    }
    format!("{:x}",digest.finalize())
}
fn paired_fine(config:FinePairedConfig, config_sha:&str, output:&Path) -> Result<()> {
    use borsuk::fine_sq8_groups::{FineSq8Index, MAX_BYTES};
    let mut events = Events::new(output, config.max_result_bytes.max(TERMINAL_CAP))?;
    events.error_context = json!({"schema":"borsuk-fine-sq8-paired-v1","config_sha256":config_sha});
    #[cfg(test)] { events.fail_sync_at = config.fail_sync_at; }
    let outcome = events.run(|events| {
        let (rows, dimensions) = (100_000,768);
        #[cfg(test)] let (rows,dimensions) = config.test_geometry.unwrap_or((rows,dimensions));
        let worker_ok = borsuk::configured_cpu_threads() == 1;
        #[cfg(test)] let worker_ok = worker_ok || config.test_geometry.is_some();
        require(config.schema == "borsuk-fine-sq8-paired-v1" && config.source_identity_sha256 == fine_source_identity()
            && worker_ok
            && config.panels[0].dataset == "relaion" && config.panels[1].dataset == "cohere"
            && config.limits.active_queries == 1 && config.limits.delta_bytes == 0
            && config.limits.max_peak_payload_bytes <= 512 * 1024 * 1024
            && config.max_evaluator_payload_bytes <= 512 * 1024 * 1024
            && (TERMINAL_CAP..=128 * 1024 * 1024).contains(&config.max_result_bytes), "fine paired frozen schema/geometry/resources")?;
        let mut evaluator = 128usize.checked_mul(rows.min(MAX_BYTES/(dimensions+12)) * 16 + 1024 * 256 + dimensions * 4)
            .ok_or("fine evaluator roster overflow")?;
        for panel in &config.panels {
            require(panel.truth_width == 100 && panel.truth.bytes == 64 * 100 * 4
                && panel.requests.bytes <= REQUEST_CAP, "fine fixed consumed64 panel")?;
            evaluator = evaluator.checked_add(panel.requests.bytes.checked_mul(8).ok_or("fine request overflow")?)
                .and_then(|v| v.checked_add(panel.truth.bytes)).ok_or("fine evaluator overflow")?;
        }
        require(evaluator <= config.max_evaluator_payload_bytes, "fine evaluator admission")?;
        let seal_path = output.with_extension("fine-seal.json");
        require(fs::symlink_metadata(&seal_path).is_err(), "fine seal exists")?;
        let mut limits = config.limits.clone();
        limits.runtime_bytes = limits.runtime_bytes.checked_add(evaluator).ok_or("fine evaluator aggregate overflow")?;
        // Both metadata admissions precede either graph/PQ/hash/body allocation.
        let admissions = config.panels.iter().map(|p| FineSq8Index::admission(&p.root,&limits)).collect::<Result<Vec<_>>>()?;
        let total_pins = admissions.iter().try_fold(limits.pinned_generation_bytes,|sum,a| sum.checked_add(a.resident_bytes).ok_or("fine pin overflow"))?;
        for (panel,admission) in config.panels.iter().zip(&admissions) {
            let mut bound = limits.clone(); bound.pinned_generation_bytes = total_pins - admission.resident_bytes;
            FineSq8Index::admission(&panel.root,&bound)?;
        }
        let mut indexes = Vec::new();
        for (panel,admission) in config.panels.iter().zip(&admissions) {
            let mut bound = limits.clone(); bound.pinned_generation_bytes = total_pins - admission.resident_bytes;
            let timer = (Instant::now(),cpu_ns());
            let index = FineSq8Index::open(&panel.root,&bound)?;
            require(index.rows() == rows && index.dimensions() == dimensions, "fine paired exact runtime geometry")?;
            events.emit(&json!({"phase":"startup","dataset":panel.dataset,"root":panel.root,
                "resources":index.resources,"build":index.build_receipt(),"truth_opened":false,
                "wall_ns":timer.0.elapsed().as_nanos(),"process_cpu_ns":cpu_ns()-timer.1}))?;
            indexes.push(index);
        }
        let mut all_queries = Vec::new(); let mut all_plans = Vec::new();
        for (panel,index) in config.panels.iter().zip(&indexes) {
            let body = read_source_probe_artifact(&panel.requests,REQUEST_CAP)?;
            let mut queries = Vec::new(); let mut plans = Vec::new(); let mut workspace = index.new_workspace()?;
            for (ordinal,line) in std::str::from_utf8(&body)?.lines().enumerate() {
                require(ordinal < 64, "fine exactly64 request cap")?;
                let request:Request = serde_json::from_str(line)?;
                require(request.ordinal == ordinal && request.query.len() == dimensions, "fine request ordinal/dimensions")?;
                let timer = (Instant::now(),cpu_ns());
                let plan = index.plan(&request.query,&mut workspace)?;
                events.emit(&json!({"phase":"fine_plan","dataset":panel.dataset,"ordinal":ordinal,
                    "plan":plan,"truth_opened":false,"wall_ns":timer.0.elapsed().as_nanos(),"process_cpu_ns":cpu_ns()-timer.1}))?;
                queries.push(request.query); plans.push(plan);
            }
            require(plans.len() == 64, "fine exactly64 complete plans")?;
            all_queries.push(queries); all_plans.push(plans);
        }
        events.sync()?;
        let identity = json!({"schema":"borsuk-fine-sq8-seal-v1","config_sha256":config_sha,
            "source_identity_sha256":fine_source_identity(),"prefix_bytes":events.bytes,
            "prefix_sha256":format!("{:x}",events.digest.clone().finalize()),"plans_per_panel":64,
            "panels":config.panels.iter().map(|p| json!({"dataset":p.dataset,"root":p.root,"requests":p.requests})).collect::<Vec<_>>(),
            "truth_opened":false});
        verify_nomination_prefix(output,events.bytes,identity["prefix_sha256"].as_str().unwrap())?;
        let sealed = serde_json::to_vec(&identity)?;
        require(sealed.len() <= FREEZE_CAP, "fine seal cap")?;
        let mut seal = OpenOptions::new().write(true).create_new(true).open(&seal_path)?;
        seal.write_all(&sealed)?; seal.sync_all()?;
        File::open(output.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new(".")))?.sync_all()?;
        events.emit(&json!({"phase":"fine_seal","identity":identity,"sha256":hash(&sealed),"truth_opened":false}))?;
        events.sync()?;
        if all_plans.iter().flatten().any(|p| !p.feasible()) {
            return Ok(json!({"phase":"terminal","status":"FAIL","complete":true,"reason":"infeasible unchanged-shortlist cover",
                "truth_opened":false,"scientific_qualification":false,"quality_or_performance_claim":false}));
        }
        // Score before truth too. Keep only bounded audit rosters and top100.
        let mut traces = Vec::new();
        for panel in 0..2 {
            let mut scored = Vec::new();
            for ordinal in 0..64 {
                let timer = (Instant::now(),cpu_ns());
                let trace = indexes[panel].search(&all_plans[panel][ordinal],&all_queries[panel][ordinal],100)
                    .map_err(|e| { events.error_context["accounting"] = json!(e.accounting); e.error })?;
                events.emit(&json!({"phase":"fine_scored","dataset":config.panels[panel].dataset,"ordinal":ordinal,
                    "nominee_ids":trace.nominee_ids,"fetched_ids":trace.fetched_ids,
                    "ranked":trace.ranked.iter().map(|s| json!({"id":s.id,"ordinal":s.ordinal,"score_bits":s.score.to_bits()})).collect::<Vec<_>>(),
                    "accounting":trace.accounting,"underfill":trace.ranked.len()<100,"truth_opened":false,
                    "wall_ns":timer.0.elapsed().as_nanos(),"process_cpu_ns":cpu_ns()-timer.1}))?;
                scored.push(trace);
            }
            traces.push(scored);
        }
        events.sync()?;
        let mut summaries = Vec::new(); let mut passed = true;
        for (panel,scored) in config.panels.iter().zip(&traces) {
            let truth = read_source_probe_artifact(&panel.truth,64*100*4)?;
            let mut containment = Vec::new(); let mut coverage = Vec::new(); let mut recall = Vec::new();
            for (ordinal,trace) in scored.iter().enumerate() {
                let gt = truth[ordinal*400..(ordinal+1)*400].chunks_exact(4)
                    .map(|v| i64::from(u32::from_le_bytes(v.try_into().unwrap()))).collect::<std::collections::BTreeSet<_>>();
                require(gt.len() == 100 && gt.iter().all(|&id| id >= 0 && (id as usize) < rows), "fine truth exact unique IDs")?;
                containment.push(trace.nominee_ids.iter().filter(|id| gt.contains(id)).count());
                coverage.push(trace.fetched_ids.iter().filter(|id| gt.contains(id)).count());
                recall.push(trace.ranked.iter().filter(|s| gt.contains(&s.id)).count());
                events.emit(&json!({"phase":"fine_metrics","dataset":panel.dataset,"ordinal":ordinal,
                    "containment_hits":containment[ordinal],"coverage_hits":coverage[ordinal],"returned_hits":recall[ordinal]}))?;
            }
            let summarize = |hits:&mut Vec<usize>| { hits.sort_unstable(); json!({"mean_hits":hits.iter().sum::<usize>() as f64/64.,"p05_hits":hits[3]}) };
            let c = summarize(&mut containment); let f = summarize(&mut coverage); let r = summarize(&mut recall);
            passed &= coverage.iter().sum::<usize>() >= 6272 && recall.iter().sum::<usize>() >= 6272 && coverage[3] >= 95 && recall[3] >= 95;
            summaries.push(json!({"dataset":panel.dataset,"containment":c,"coverage":f,"returned":r}));
        }
        Ok(json!({"phase":"terminal","status":if passed {"SURVIVED_CONSUMED_PANELS"} else {"FAIL"},
            "complete":true,"summaries":summaries,"scientific_qualification":false,"quality_or_performance_claim":false,
            "scope":"consumed64 FIRST100k falsifier only; no fresh generalization,100M,maintenance,S3 or vendor qualification"}))
    });
    File::open(output.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new(".")))?.sync_all()?;
    outcome
}

const CONFIG_CAP: usize = 65536;
const REQUEST_CAP: usize = 32 * 1024 * 1024;
const EVENT_CAP: usize = 8 * 1024 * 1024;
const TERMINAL_CAP: usize = 4096;
const DIAGNOSTIC_SCHEMA: &str = "borsuk-hierarchical-cells-diagnostic-v3";

const NOMINATION_SCHEMA: &str = "borsuk-hierarchical-cells-nomination-v1";
const FREEZE_CAP: usize = 4096;
const NOMINATION_COUNT: usize = 64;

/// `build-overlap CONFIG SHA NEW_DIRECTORY` builds source-only SQ8 extents.
/// `paired-overlap CONFIG SHA NEW_JSONL` freezes both full-scanner arms before
/// truth. A PANEL_PASS is not both-panel qualification or fresh quality.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OverlapPairedConfig {
    schema: String,
    dataset: String,
    control_root: Artifact,
    candidate_root: Artifact,
    requests: Artifact,
    first: usize,
    count: usize,
    truth: Artifact,
    truth_width: usize,
    source_identity_sha256: String,
    max_resident_payload_bytes: usize,
    max_evaluator_payload_bytes: usize,
    max_result_bytes: usize,
    resources: OverlapResources,
    #[cfg(test)]
    #[serde(skip)]
    test_seam: Option<OverlapPairTestSeam>,
}
// Private native-fixture seam. It is absent from runtime builds and cannot be
// supplied through strict JSON configuration, environment or command flags.
#[cfg(test)]
#[derive(Clone, Copy)]
struct OverlapPairTestSeam {
    rows: usize,
    dimensions: usize,
    evaluator_roster_bytes: usize,
    query_scratch_bytes: usize,
    selection_mismatch_at: Option<usize>,
    fail_sync_at: Option<usize>,
    corrupt_frame_arm: Option<&'static str>,
}
#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct OverlapResources {
    build_workers: usize,
    build_memory_bytes: u64,
    build_swap_bytes: u64,
    build_scratch_bytes: u64,
    build_timeout_seconds: u64,
    query_workers: usize,
    query_memory_bytes: u64,
    query_swap_bytes: u64,
    query_timeout_seconds: u64,
}
fn overlap_resources(r: &OverlapResources) -> Result<()> {
    require((1..=4).contains(&r.build_workers) && r.build_memory_bytes == 8 * 1024 * 1024 * 1024
        && r.build_swap_bytes == 0 && r.build_scratch_bytes == 8 * 1024 * 1024 * 1024
        && r.build_timeout_seconds == 1200 && r.query_workers == 1
        && r.query_memory_bytes == 512 * 1024 * 1024 && r.query_swap_bytes == 0
        && r.query_timeout_seconds == 300, "overlap frozen resources")
}
fn overlap_source_identity() -> String {
    let mut digest = Sha256::new();
    // Order and names bind the exact five-file authored contract.
    for (name, bytes) in [
        ("hierarchical_semantic_cells.rs", include_bytes!("../hierarchical_semantic_cells.rs").as_slice()),
        ("semantic_cell_overlap.rs", include_bytes!("../semantic_cell_overlap.rs").as_slice()),
        ("returned_sq8.rs", include_bytes!("../returned_sq8.rs").as_slice()),
        ("bin/hierarchical_semantic_cells.rs", include_bytes!("hierarchical_semantic_cells.rs").as_slice()),
        ("lib.rs", include_bytes!("../lib.rs").as_slice()),
    ] {
        digest.update((name.len() as u64).to_le_bytes()); digest.update(name.as_bytes());
        digest.update((bytes.len() as u64).to_le_bytes()); digest.update(bytes);
    }
    format!("{:x}", digest.finalize())
}
fn overlap_seal(events: &mut Events, seal_path: &Path, identity: &Value) -> Result<Artifact> {
    require(identity["selections_per_arm"] == 64 && identity["complete_unique_scored_rosters_per_arm"] == 64,
        "overlap paired seal requires both64 selections and complete rosters")?;
    events.sync()?;
    let body = serde_json::to_vec(identity)?;
    require(body.len() <= FREEZE_CAP, "overlap seal cap")?;
    let mut seal = OpenOptions::new().write(true).create_new(true).open(seal_path)?;
    seal.write_all(&body)?;
    seal.sync_all()?;
    File::open(seal_path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new(".")))?.sync_all()?;
    Ok(Artifact { path: seal_path.into(), bytes: body.len(), sha256: hash(&body) })
}
fn authenticate_overlap_prefix(events: &Events, output: &Path, identity: &Value) -> Result<()> {
    let prefix = identity["prefix_bytes"].as_u64().ok_or("overlap seal prefix bytes")?;
    require(prefix <= events.bytes as u64, "overlap seal prefix length")?;
    let mut file = OpenOptions::new().read(true).custom_flags(
        (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32).open(output)?;
    require(file.metadata()?.is_file() && file.metadata()?.len() == events.bytes as u64, "overlap durable roster exact length")?;
    let mut digest = Sha256::new(); let mut remaining = prefix; let mut buffer = [0_u8; 65536];
    while remaining > 0 {
        let amount = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..amount])?; digest.update(&buffer[..amount]); remaining -= amount as u64;
    }
    require(identity["prefix_sha256"] == format!("{:x}", digest.finalize()), "overlap durable roster prefix authentication")
}
fn overlap_hits(trace: &OverlapSearchTrace, truth: &[i64]) -> (usize, usize) {
    let coverage = truth.iter().filter(|id| trace.base_ids.binary_search(id).is_ok()).count();
    let returned = trace.ranked.iter().take(100).map(|s| s.id).collect::<std::collections::BTreeSet<_>>();
    (coverage, truth.iter().filter(|id| returned.contains(id)).count())
}
fn overlap_summary(hits: &[(usize, usize)]) -> Value {
    let mut coverage = hits.iter().map(|v| v.0).collect::<Vec<_>>();
    let mut recall = hits.iter().map(|v| v.1).collect::<Vec<_>>();
    coverage.sort_unstable(); recall.sort_unstable();
    json!({"queries":64,"denominator_per_query":100,"p05_sorted_index":3,
        "coverage_mean":coverage.iter().sum::<usize>() as f64 / 6400.,"coverage_p05_hits":coverage[3],
        "recall_mean":recall.iter().sum::<usize>() as f64 / 6400.,"recall_p05_hits":recall[3],
        "pass":coverage.iter().sum::<usize>() >= 6272 && coverage[3] >= 95
            && recall.iter().sum::<usize>() >= 6272 && recall[3] >= 95})
}
fn paired_overlap(config: OverlapPairedConfig, config_sha: &str, output: &Path) -> Result<()> {
    let mut events = Events::new(output, config.max_result_bytes.min(512 * 1024 * 1024))?;
    events.error_context = json!({"schema":"borsuk-cell-overlap-paired-v1","config_sha256":config_sha});
    #[cfg(test)]
    { events.fail_sync_at = config.test_seam.and_then(|seam| seam.fail_sync_at); }
    events.run(|events| {
        let started = (Instant::now(), cpu_ns());
        let (logical_rows, dimensions, roster_allowance, query_scratch) = (100_000, 768,
            128 * 32 * 640 * 64,
            3 * overlap::MAX_BYTES + 32 * 640 * (768 + 12 + 512) + 8 * 32 * (768 * 4 + 4096));
        #[cfg(test)]
        let (logical_rows, dimensions, roster_allowance, query_scratch) = config.test_seam
            .map(|seam| (seam.rows, seam.dimensions, seam.evaluator_roster_bytes, seam.query_scratch_bytes))
            .unwrap_or((logical_rows, dimensions, roster_allowance, query_scratch));
        require(config.schema == "borsuk-cell-overlap-paired-v1" && ["relaion", "cohere"].contains(&config.dataset.as_str())
            && config.count == 64 && config.truth_width == 100 && config.first == 0
            && config.source_identity_sha256 == overlap_source_identity()
            && config.max_resident_payload_bytes <= 512 * 1024 * 1024
            && config.max_evaluator_payload_bytes <= 512 * 1024 * 1024,
            "overlap paired schema/source/panel/caps")?;
        overlap_resources(&config.resources)?;
        let modeled_evaluator = config.requests.bytes.checked_mul(4)
            .and_then(|v| v.checked_add(config.truth.bytes))
            .and_then(|v| v.checked_add(roster_allowance))
            .ok_or("overlap evaluator payload overflow")?;
        require(modeled_evaluator <= config.max_evaluator_payload_bytes && config.requests.bytes <= REQUEST_CAP,
            "overlap evaluator admission")?;
        let seal_path = output.with_extension("paired-seal.json");
        require(fs::symlink_metadata(&seal_path).is_err(), "overlap paired seal exists")?;
        // Authenticate ONLY small roots, then reject the complete pair before
        // either directory/router/mapping/boundary/cell-body preload begins.
        let admission = OverlapPairAdmission::read(&config.control_root, &config.candidate_root,
            config.max_resident_payload_bytes, config.resources.query_memory_bytes as usize,
            modeled_evaluator, query_scratch)?;
        let ca = admission.control(); let ta = admission.candidate();
        require(!ca.overlap_enabled() && ta.overlap_enabled() && ca.logical_rows() == logical_rows
            && ta.logical_rows() == logical_rows && ca.dimensions() == dimensions && ta.dimensions() == dimensions
            && ca.primary_root_identity().sha256 == ta.primary_root_identity().sha256
            && ca.boundary_sha256() == ta.boundary_sha256(),
            "overlap matched retained capacity-v4 layouts")?;
        events.emit(&json!({"phase":"paired_metadata_admission","heavy_indexes_opened":0,
            "control_primary_root":ca.primary_root_identity(),"candidate_primary_root":ta.primary_root_identity(),
            "control_directory":ca.primary_directory(),"candidate_directory":ta.primary_directory(),
            "control_metadata_payload_bytes":ca.modeled_metadata_payload_bytes(),
            "candidate_metadata_payload_bytes":ta.modeled_metadata_payload_bytes(),
            "control_preload_peak_bytes":ca.modeled_preload_peak_bytes(),"candidate_preload_peak_bytes":ta.modeled_preload_peak_bytes(),
            "control_resident_payload_bytes":ca.modeled_resident_payload_bytes(),
            "candidate_resident_payload_bytes":ta.modeled_resident_payload_bytes(),
            "pair_peak_payload_bytes":admission.modeled_peak_payload_bytes,
            "evaluator_payload_bytes":modeled_evaluator,"query_scratch_bytes":query_scratch}))?;
        events.sync()?;
        // Both admitted roots are reauthenticated before the first heavy open.
        let (control, candidate) = admission.open()?;
        let control = std::sync::Arc::new(control); let candidate = std::sync::Arc::new(candidate);
        let request_body = read_source_probe_artifact(&config.requests, REQUEST_CAP)?;
        let mut requests = Vec::new();
        let mut previous = None;
        for line in request_body.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
            require(line.len() <= 65536, "overlap request line cap")?;
            let request: Request = serde_json::from_slice(line)?;
            require(previous.is_none_or(|p| request.ordinal > p) && request.query.len() == dimensions
                && request.query.iter().all(|v| v.is_finite()), "overlap request order/geometry")?;
            previous = Some(request.ordinal);
            if (config.first..config.first + 64).contains(&request.ordinal) { requests.push(request); }
        }
        require(requests.len() == 64 && requests.iter().enumerate().all(|(i, r)| r.ordinal == config.first + i), "overlap exact64 requests")?;
        drop(request_body);
        let c = OverlapRevisions::new(control.clone(), 0, 0)?.pin();
        let t = OverlapRevisions::new(candidate.clone(), 0, 0)?.pin();
        require(c.delta_is_empty() && t.delta_is_empty(), "science requires empty delta")?;
        let mut plans = Vec::with_capacity(64);
        let mut routes_cpu = 0;
        let mut routes_wall = 0;
        for request in &requests {
            let timer = (Instant::now(), cpu_ns());
            let a = control.plan(&request.query)?;
            let b = candidate.plan(&request.query)?;
            #[cfg(test)]
            let b = {
                let mut plan = b;
                if config.test_seam.is_some_and(|seam| seam.selection_mismatch_at == Some(request.ordinal)) {
                    plan.selected[0].distance_bits ^= 1;
                }
                plan
            };
            // Identity precedes EVERY payload read in either arm.
            require(a.selected == b.selected, "paired cell IDs/primary flags/distance bits mismatch")?;
            routes_cpu += cpu_ns() - timer.1; routes_wall += timer.0.elapsed().as_nanos();
            events.emit(&json!({"phase":"paired_selection","ordinal":request.ordinal,"control":a,"candidate":b}))?;
            plans.push((a, b));
        }
        events.sync()?;
        #[cfg(test)]
        if let Some(arm) = config.test_seam.and_then(|seam| seam.corrupt_frame_arm) {
            let (root, plan) = if arm == "candidate" { (&config.candidate_root, &plans[0].1) }
                else { (&config.control_root, &plans[0].0) };
            let file = OpenOptions::new().write(true).open(root.path.parent().unwrap().join("sq8-cells.bin"))?;
            file.write_all_at(&[0xff], (plan.extents[0].offset + 64 + 8) as u64)?;
        }
        let mut rosters = Vec::with_capacity(64);
        let mut scoring_cpu = 0;
        let mut scoring_wall = 0;
        for (request, (a, b)) in requests.iter().zip(&plans) {
            // A new attempted query must not inherit the preceding receipt.
            events.error_context.as_object_mut().unwrap().remove("failed_query_receipt");
            let timer = (Instant::now(), cpu_ns());
            let mut control_trace = match search_selected_sq8(&c, a, &request.query, control.logical_rows()) {
                Ok(trace) => trace, Err(error) => {
                    let receipt = json!({"ordinal":request.ordinal,"failed_arm":"control",
                        "control":{"status":"failed","accounting":error.accounting},
                        "candidate":{"status":"not_attempted"}});
                    events.error_context["failed_query_receipt"] = receipt.clone();
                    events.emit(&json!({"phase":"paired_arm_failure","receipt":receipt}))?;
                    events.sync()?;
                    return Err(error.into());
                }
            };
            let mut candidate_trace = match search_selected_sq8(&t, b, &request.query, candidate.logical_rows()) {
                Ok(trace) => trace, Err(error) => {
                    let receipt = json!({"ordinal":request.ordinal,"failed_arm":"candidate",
                        "control":{"status":"completed","accounting":control_trace.accounting},
                        "candidate":{"status":"failed","accounting":error.accounting}});
                    events.error_context["failed_query_receipt"] = receipt.clone();
                    events.emit(&json!({"phase":"paired_arm_failure","receipt":receipt}))?;
                    events.sync()?;
                    return Err(error.into());
                }
            };
            // Preserve both completed payload accounts before validation,
            // roster emission, or the subsequent durable-prefix sync can fail.
            events.error_context["failed_query_receipt"] = json!({"ordinal":request.ordinal,
                "control":{"status":"completed","accounting":control_trace.accounting},
                "candidate":{"status":"completed","accounting":candidate_trace.accounting}});
            let added = candidate_trace.replica_ids.iter().copied().filter(|id| control_trace.base_ids.binary_search(id).is_err()).collect::<std::collections::BTreeSet<_>>();
            let expected = control_trace.base_ids.iter().copied().chain(added).collect::<std::collections::BTreeSet<_>>();
            require(expected.iter().copied().eq(candidate_trace.base_ids.iter().copied()), "paired unique membership monotonicity/admitted-replica invariant")?;
            require(control_trace.ranked.len() == control_trace.base_ids.len()
                && candidate_trace.ranked.len() == candidate_trace.base_ids.len(), "paired complete unique scored rosters")?;
            // The sealed roster is complete; the scientific request remains
            // k100, so underfill uses100 rather than the full-roster buffer cap.
            control_trace.underfill = control_trace.ranked.len() < 100;
            candidate_trace.underfill = candidate_trace.ranked.len() < 100;
            scoring_cpu += cpu_ns() - timer.1; scoring_wall += timer.0.elapsed().as_nanos();
            events.emit(&json!({"phase":"paired_scored_roster","ordinal":request.ordinal,
                "control":control_trace,"candidate":candidate_trace,
                "process_rss_and_lifetime_hwm_bytes":process_memory_snapshot()}))?;
            rosters.push((control_trace, candidate_trace));
        }
        let identity = json!({"schema":"borsuk-cell-overlap-paired-seal-v1","config_sha256":config_sha,
            "source_identity_sha256":config.source_identity_sha256,"control_root":config.control_root,
            "candidate_root":config.candidate_root,"requests":config.requests,"truth_descriptor":config.truth,
            "selections_per_arm":64,"complete_unique_scored_rosters_per_arm":64,"empty_delta":true,
            "prefix_bytes":events.bytes,"prefix_sha256":format!("{:x}",events.digest.clone().finalize())});
        let seal = overlap_seal(events, &seal_path, &identity)?;
        events.emit(&json!({"phase":"paired_seal","seal":seal,"truth_opened":false}))?;
        events.sync()?;
        let durable = read_source_probe_artifact(&seal, FREEZE_CAP)?;
        require(serde_json::from_slice::<Value>(&durable)? == identity, "overlap durable paired seal authentication")?;
        authenticate_overlap_prefix(events, output, &identity)?;
        // No truth file open (even metadata) occurs before both arms are sealed.
        let required_truth_bytes = config.first.checked_add(64).and_then(|v| v.checked_mul(400)).ok_or("overlap truth geometry overflow")?;
        require(config.truth.bytes > 0 && config.truth.bytes % 400 == 0
            && config.truth.bytes >= required_truth_bytes && config.truth.bytes <= config.max_evaluator_payload_bytes,
            "overlap actual truth descriptor admission")?;
        // The evaluator budget includes rosters and may exceed the independent
        // artifact reader's 128 MiB ceiling. Admit/read the actual truth bytes.
        let truth = read_source_probe_artifact(&config.truth, config.truth.bytes)?;
        let mut control_hits = Vec::new();
        let mut candidate_hits = Vec::new();
        for (request, (a, b)) in requests.iter().zip(&rosters) {
            let start = request.ordinal.checked_mul(400).ok_or("overlap truth start overflow")?;
            let end = request.ordinal.checked_add(1).and_then(|v| v.checked_mul(400)).ok_or("overlap truth end overflow")?;
            let ids = truth.get(start..end).ok_or("overlap truth row bounds")?.chunks_exact(4)
                .map(|v| i64::from(u32::from_le_bytes(v.try_into().unwrap()))).collect::<Vec<_>>();
            require(ids.iter().all(|id| *id >= 0 && (*id as usize) < logical_rows)
                && ids.iter().collect::<std::collections::BTreeSet<_>>().len() == 100, "overlap truth100 source IDs")?;
            let ch = overlap_hits(a, &ids); let th = overlap_hits(b, &ids);
            require(th.0 >= ch.0, "candidate unique truth coverage regression")?;
            control_hits.push(ch); candidate_hits.push(th);
            events.emit(&json!({"phase":"paired_metrics","ordinal":request.ordinal,"control_hits":ch,"candidate_hits":th,
                "fixed_denominator":100,"control_underfill":a.ranked.len()<100,"candidate_underfill":b.ranked.len()<100}))?;
        }
        let cs = overlap_summary(&control_hits); let ts = overlap_summary(&candidate_hits);
        Ok(json!({"phase":"terminal","schema":config.schema,"status":if ts["pass"] == true { "PANEL_PASS" } else { "FAIL" },
            "complete":true,"dataset":config.dataset,"control":cs,"candidate":ts,"paired_seal":seal,
            "source_identity_sha256":config.source_identity_sha256,"config_sha256":config_sha,
            "resources":config.resources,"modeled_evaluator_payload_bytes":modeled_evaluator,
            "control_startup":control.startup,"candidate_startup":candidate.startup,
            "control_router_root_reads":control.router().startup,"candidate_router_root_reads":candidate.router().startup,
            "control_router_directory_reads":control.router().startup_directory,"candidate_router_directory_reads":candidate.router().startup_directory,
            "route_wall_ns":routes_wall,"route_process_cpu_ns":routes_cpu,
            "full_sq8_scoring_wall_ns":scoring_wall,"full_sq8_scoring_process_cpu_ns":scoring_cpu,
            "whole_wall_ns":started.0.elapsed().as_nanos(),"whole_process_cpu_ns":cpu_ns()-started.1,
            "process_rss_and_lifetime_hwm_bytes":process_memory_snapshot(),"payload_dependency_waves":1,"actual_max_parallel_gets":1,
            "whole_unit_caps_and_scratch":"external_supervisor_required","scientific_qualification":false,"quality_or_performance_claim":false}))
    })
}
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
    #[cfg(test)]
    fail_sync_at: Option<usize>,
    #[cfg(test)]
    sync_calls: std::cell::Cell<usize>,
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
            #[cfg(test)]
            fail_sync_at: None,
            #[cfg(test)]
            sync_calls: std::cell::Cell::new(0),
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
        #[cfg(test)]
        {
            let call = self.sync_calls.get() + 1;
            self.sync_calls.set(call);
            if self.fail_sync_at == Some(call) {
                return Err(std::io::Error::other("paired fixture sync failure").into());
            }
        }
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
    if args.get(1).is_some_and(|action| action == "check-pq-residual-source") {
        require(args.len() == 5, "usage: hierarchical_semantic_cells check-pq-residual-source CONFIG CONFIG_SHA256 NEW_OUTPUT")?;
        return borsuk::fine_sq8_groups::pq_residual_source::check_pq_residual_source(
            Path::new(&args[2]), &args[3], Path::new(&args[4]),
        );
    }
    if args
        .get(1)
        .is_some_and(|action| action == "check-fine-corrected-four-bit")
    {
        require(args.len() == 5, "usage: hierarchical_semantic_cells check-fine-corrected-four-bit CONFIG CONFIG_SHA256 NEW_REPORT_JSON")?;
        return borsuk::fine_sq8_groups::sq4_diagnostic::corrected::check_fine_corrected_four_bit(
            Path::new(&args[2]),
            &args[3],
            Path::new(&args[4]),
        );
    }
    if args.get(1).is_some_and(|action| action == "check-fine-histogram-sq4") {
        require(args.len() == 5, "usage: hierarchical_semantic_cells check-fine-histogram-sq4 CONFIG CONFIG_SHA256 NEW_REPORT_JSON")?;
        return borsuk::fine_sq8_groups::histogram_sq4_diagnostic::check_fine_histogram_sq4(
            Path::new(&args[2]), &args[3], Path::new(&args[4]),
        );
    }
    if args.get(1).is_some_and(|action| action == "check-fine-sq4") {
        require(
            args.len() == 5,
            "usage: hierarchical_semantic_cells check-fine-sq4 CONFIG CONFIG_SHA256 NEW_REPORT_JSON",
        )?;
        return borsuk::fine_sq8_groups::sq4_diagnostic::check_fine_sq4(
            Path::new(&args[2]),
            &args[3],
            Path::new(&args[4]),
        );
    }
    execute_args_with_pack(args, borsuk::fine_sq8_groups::pack_diagnostic::check_fine_pack)
}
fn execute_args_with_pack(args: &[String], check_pack: fn(&Path, &str, &Path) -> Result<()>) -> Result<()> {
    require(
        args.len() == 5,
        "usage: hierarchical_semantic_cells build|diagnose|nominate|build-probes|nominate-probes|diagnose-probes|build-overlap|paired-overlap CONFIG CONFIG_SHA256 NEW_OUTPUT",
    )?;
    if args[1] == "check-fine-pack" {
        return check_pack(
            Path::new(&args[2]), &args[3], Path::new(&args[4]));
    }
    let probe_mode = matches!(
        args[1].as_str(),
        "build-probes" | "nominate-probes" | "diagnose-probes" | "build-overlap" | "paired-overlap" | "build-fine" | "paired-fine"
    );
    let invalid_output = if matches!(args[1].as_str(), "build-overlap" | "build-fine") { Path::new(&args[4]).with_extension("build.jsonl") }
        else { Path::new(&args[4]).to_path_buf() };
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
            return probe_close_invalid(&invalid_output, &args[3], error.into());
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
            Err(error) => return probe_close_invalid(&invalid_output, &args[3], error),
        }
    } else {
        descriptor.read(CONFIG_CAP)?
    };
    match args[1].as_str() {
        "build-fine" => {
            let config: borsuk::fine_sq8_groups::FineBuildConfig = probe_config(&body, &args[3], &invalid_output)?;
            let mut events = Events::new(&invalid_output,65536)?;
            File::open(invalid_output.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new(".")))?.sync_all()?;
            events.error_context = json!({"schema":borsuk::fine_sq8_groups::BUILD_SCHEMA,"config_sha256":args[3]});
            events.run(|_| {
                require(borsuk::configured_cpu_threads() <= 2, "fine BORSUK build worker bound")?;
                let timer = (Instant::now(),cpu_ns());
                let root = borsuk::fine_sq8_groups::FineSq8Index::build(&config,Path::new(&args[4]))?;
                Ok(json!({"phase":"terminal","status":"BUILT_UNVERIFIED","complete":true,"root":root,
                    "source_identity_sha256":fine_source_identity(),"scientific_qualification":false,"quality_or_performance_claim":false,
                    "wall_ns":timer.0.elapsed().as_nanos(),"process_cpu_ns":cpu_ns()-timer.1}))
            })
        }
        "paired-fine" => paired_fine(probe_config(&body,&args[3],&invalid_output)?,&args[3],Path::new(&args[4])),
        "build-overlap" => {
            let config: OverlapBuildConfig = probe_config(&body, &args[3], &invalid_output)?;
            let mut events = Events::new(&invalid_output, 65536)?;
            events.error_context = json!({"schema":overlap::BUILD_SCHEMA,"config_sha256":args[3]});
            events.run(|_| {
                let timer = (Instant::now(), cpu_ns());
                let receipt = overlap::build_overlap(&config, Path::new(&args[4]))?;
                Ok(json!({"phase":"terminal","status":"BUILT_UNVERIFIED","complete":true,
                    "receipt":receipt,"config_sha256":args[3],"source_identity_sha256":overlap_source_identity(),
                    "build_wall_ns":timer.0.elapsed().as_nanos(),"build_process_cpu_ns":cpu_ns()-timer.1,
                    "process_rss_and_lifetime_hwm_bytes":process_memory_snapshot(),
                    "required_build_cpu_max":4,"required_memory_bytes":8589934592_u64,"required_swap_bytes":0,
                    "required_scratch_bytes":8589934592_u64,"required_timeout_seconds":1200,
                    "actual_whole_unit_resources_and_scratch":"external_supervisor_required",
                    "scientific_qualification":false,"quality_or_performance_claim":false}))
            })
        }
        "paired-overlap" => paired_overlap(probe_config(&body, &args[3], &invalid_output)?, &args[3], Path::new(&args[4])),
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

    #[test]
    fn pq_residual_source_strict_cli_dispatch_no_geometry_or_truth_override() {
        use borsuk::fine_sq8_groups::pq_residual_source as residual;
        let tmp = tempfile::tempdir().unwrap();
        let pin = probe_artifact(&tmp.path().join("config.json"),
            br#"{"schema":"borsuk-pq-residual-source-config-v1","geometry":3,"truth":"forbidden"}"#);
        let output = tmp.path().join("report.json");
        let mut args = vec!["hierarchical_semantic_cells".into(), "check-pq-residual-source".into(),
            pin.path.display().to_string(), pin.sha256, output.display().to_string()];
        assert!(execute_args(&args[..4]).is_err());
        assert!(!output.exists());
        args.push("--fixture".into());
        assert!(execute_args(&args).is_err());
        assert!(!output.exists());
        args.pop();
        assert!(execute_args(&args).is_err());
        let body = fs::read(&output).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema"], residual::REPORT_SCHEMA);
        assert_eq!(value["codec"], residual::CODEC);
        assert_eq!(value["status"], "INVALID");
        assert_eq!(value["truth_opened"], false);
        assert!(execute_args(&args).is_err());
        assert_eq!(fs::read(&output).unwrap(), body);
    }

    #[test]
    fn fine_corrected_four_bit_strict_cli_dispatch() {
        use borsuk::fine_sq8_groups::sq4_diagnostic::corrected;
        let tmp = tempfile::tempdir().unwrap();
        let pin = probe_artifact(
            &tmp.path().join("config.json"),
            br#"{"schema":"borsuk-corrected-four-bit-config-v1","unknown":true}"#,
        );
        let path = tmp.path().join("report.json");
        let args = vec![
            "hierarchical_semantic_cells".into(),
            "check-fine-corrected-four-bit".into(),
            pin.path.display().to_string(),
            pin.sha256,
            path.display().to_string(),
        ];
        assert!(execute_args(&args[..4]).is_err());
        assert!(!path.exists());
        assert!(execute_args(&args).is_err());
        let v: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(v["schema"], corrected::REPORT_SCHEMA);
        assert_eq!(v["codec"], corrected::CODEC);
        assert_eq!(v["status"], "INVALID");
        assert_eq!(v["source_identity_sha256"], corrected::source_identity());
        assert!(execute_args(&args).is_err());
    }

    #[test]
    fn fine_corrected_four_bit_real_native_two_panel_d3_all128_incidental_and_late_truth() {
        use borsuk::fine_sq8_groups::{
            FineBuildConfig, FineSq8Index, histogram_sq4_diagnostic as histogram,
            sq4_diagnostic::corrected,
        };
        let tmp = tempfile::tempdir().unwrap();
        let dispersed = tmp.path().join("dispersed");
        fs::create_dir(&dispersed).unwrap();
        let n = 135usize; // Eight complete groups and a seven-row tail.
        let codec = borsuk::rotated_two_bit::RotatedTwoBitCodec::new(&[0.; 3], 20260923).unwrap();
        let low = [-1_f32; 3];
        let step = [0.01_f32; 3];
        let mut canonical = Vec::new();
        let mut records = Vec::new();
        let mut sq8_body = Vec::new();
        let mut order = Vec::new();
        for id in (0..n).rev() {
            let angle = id as f32 * 0.31;
            let height = ((id * 37) % n) as f32 / n as f32 * 1.8 - 0.9;
            let radius = (1. - height * height).sqrt();
            let vector = [radius * angle.cos(), radius * angle.sin(), height];
            canonical.extend((id as i64).to_le_bytes());
            canonical.extend(vector.into_iter().flat_map(f32::to_le_bytes));
            records.extend(codec.encode(&vector).unwrap());
            let codes = vector.map(|v| ((v + 1.) / 0.01).round() as u8);
            let mut norm = 0_f32;
            for axis in 0..3 {
                let value = low[axis] + f32::from(codes[axis]) * step[axis];
                norm += value * value;
            }
            sq8_body.extend((id as i64).to_le_bytes());
            sq8_body.extend(norm.to_le_bytes());
            sq8_body.extend(codes);
            order.extend((id as u64).to_le_bytes());
        }
        let canonical = probe_artifact(&dispersed.join("canonical"), &canonical);
        let records = probe_artifact(&dispersed.join("codes"), &records);
        let sq8_source = probe_artifact(&dispersed.join("sq8"), &sq8_body);
        let order = probe_artifact(&dispersed.join("order"), &order);
        let mean = probe_artifact(&dispersed.join("mean"), &[0; 12]);
        let plane = probe_artifact(
            &dispersed.join("plane"),
            &serde_json::to_vec(&borsuk::two_bit_source::SourcePlaneReceipt {
                schema: "borsuk-two-bit-plane-v3".into(),
                rows: n,
                dimensions: 3,
                seed: 20260923,
                record_bytes: codec.record_bytes(),
                source_sha256: "0".repeat(64),
                sq8_sha256: sq8_source.sha256.clone(),
                source_order_sha256: order.sha256.clone(),
                mean_sha256: mean.sha256.clone(),
                records_sha256: records.sha256.clone(),
                page_rows: 32,
                page_digest_sha256: "0".repeat(64),
                query_or_truth_used: false,
            })
            .unwrap(),
        );
        let generation = probe_artifact(&dispersed.join("generation"), &serde_json::to_vec(&json!({
            "schema":"borsuk-two-bit-generation-v8","generation":1,"base_epoch":0,
            "plane_manifest_sha256":plane.sha256,"page_manifest_sha256":"0".repeat(64),
            "discovery":{"mode":"graph","centroids_sha256":"0".repeat(64),"graph_sha256":"0".repeat(64),
                "graph_resident_bytes":1,"diverse_graph_sha256":"0".repeat(64),"diverse_graph_resident_bytes":1},
            "sq8_object_sha256":sq8_source.sha256,"sq8_object_key":format!("objects/{}",sq8_source.sha256),"sq8_etag":"fixture",
            "canonical":{"rows":n,"dimensions":3,"bytes":canonical.bytes,"sha256":canonical.sha256,
                "object_key":format!("objects/{}",canonical.sha256)},"low":low,"step":step
        })).unwrap());
        let primary_path = dispersed.join("primary");
        let receipt = borsuk::hierarchical_semantic_cells::build(
            &BuildConfig {
                schema: borsuk::hierarchical_semantic_cells::BUILD_SCHEMA.into(),
                generation,
                plane,
                canonical,
                order,
                records,
                mean,
                sq8: sq8_source,
                cell_rows: 32,
                sample_rows: 32,
                max_depth: 24,
                max_build_payload_bytes: 64 * 1024 * 1024,
                max_output_bytes: 16 * 1024 * 1024,
            },
            &primary_path,
        )
        .unwrap();
        let primary_root = Artifact {
            path: primary_path.join("manifest.json"),
            bytes: fs::metadata(primary_path.join("manifest.json"))
                .unwrap()
                .len() as usize,
            sha256: receipt.root_sha256,
        };
        let roots = ["relaion", "cohere"].map(|name| {
            FineSq8Index::build(
                &FineBuildConfig {
                    schema: borsuk::fine_sq8_groups::BUILD_SCHEMA.into(),
                    primary_root: primary_root.clone(),
                    max_build_payload_bytes: 128 * 1024 * 1024,
                    max_output_bytes: 16 * 1024 * 1024,
                },
                &dispersed.join(name),
            )
            .unwrap()
        });

        let manifests = roots
            .each_ref()
            .map(|r| serde_json::from_slice::<Value>(&fs::read(&r.path).unwrap()).unwrap());
        let sources = manifests
            .each_ref()
            .map(|m| fs::read(m["records"]["path"].as_str().unwrap()).unwrap());
        // The query follows a fetched row which is not a nominee. Source rows,
        // graph/PQ and both roots above came from the real native builders.
        let query =
            std::array::from_fn::<_, 3, _>(|j| low[j] + f32::from(sources[0][15 + 12 + j]) * step[j]);
        let request = probe_artifact(
            &dispersed.join("requests"),
            (0..64)
                .map(|ordinal| format!("{}\n", json!({"ordinal":ordinal,"query":query})))
                .collect::<String>()
                .as_bytes(),
        );
        let truth = probe_artifact(
            &dispersed.join("truth"),
            &(0..64)
                .flat_map(|_| (0..100u32).flat_map(u32::to_le_bytes))
                .collect::<Vec<_>>(),
        );
        let nominees = (0..n).step_by(16).collect::<Vec<_>>();
        let mut prefix = String::new();
        for i in 0..2 {
            prefix += &format!(
                "{}\n",
                json!({"phase":"startup","dataset":(["relaion","cohere"][i]),"root":roots[i],
            "resources":borsuk::fine_sq8_groups::ResourceReceipt::default(),"build":manifests[i]["build"],"truth_opened":false,"wall_ns":0,"process_cpu_ns":0})
            );
        }
        let query_sha = hash(
            &query
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>(),
        );
        for i in 0..2 {
            for ordinal in 0..64 {
                prefix += &format!(
                    "{}\n",
                    json!({"phase":"fine_plan","dataset":(["relaion","cohere"][i]),"ordinal":ordinal,
            "truth_opened":false,"wall_ns":0,"process_cpu_ns":0,"plan":{"root_sha256":roots[i].sha256,"query_sha256":query_sha,"revision":0,"mutation_sha256":"",
            "nominees":nominees,"ranges":[0..n*15],"planned_bytes":n*15,"feasible":true,"exhausted":false,"converged":true,"actual_shortlist_rows":nominees.len(),
            "evaluations":n,"base_visits":n,"old_page_gets":1,"old_page_bytes":n*15}})
                );
            }
        }
        let prefix = probe_artifact(&dispersed.join("prefix"), prefix.as_bytes());
        let seal=probe_artifact(&dispersed.join("seal"),&serde_json::to_vec(&json!({"schema":"borsuk-fine-sq8-seal-v1","config_sha256":hash(b"controlled native fixture"),
            "source_identity_sha256":fine_source_identity(),"prefix_bytes":prefix.bytes,"prefix_sha256":prefix.sha256,"plans_per_panel":64,"truth_opened":false,
            "panels":(0..2).map(|i|json!({"dataset":(["relaion","cohere"][i]),"root":roots[i],"requests":request})).collect::<Vec<_>>()})).unwrap());
        let old = histogram::Config {
            schema: histogram::CONFIG_SCHEMA.into(),
            source_identity_sha256: histogram::source_identity(),
            panels: std::array::from_fn(|i| histogram::Panel {
                dataset: ["relaion", "cohere"][i].into(),
                root: roots[i].clone(),
                requests: request.clone(),
                truth: truth.clone(),
                truth_width: 100,
            }),
            original_seal: seal,
            prefix,
            caps: histogram::Caps {
                memory_bytes: 256 * 1024 * 1024,
                output_bytes: 128 * 1024 * 1024,
                deadline_seconds: 600,
                operations: 20_000_000_000,
                cpu_threads: 1,
                swap_bytes: 0,
            },
        };
        let historical = dispersed.join("historical.json");
        histogram::diagnose(&old, &hash(b"historical"), &historical).unwrap();
        let freeze: Value = serde_json::from_slice(
            &fs::read(historical.with_extension("histogram-sq4-freeze.json")).unwrap(),
        )
        .unwrap();
        let panels=(0..2).map(|p|json!({"dataset":old.panels[p].dataset,"root_sha256":roots[p].sha256,"queries":(0..64).map(|ordinal|{
            let result:Value=serde_json::from_slice(&fs::read(freeze["results"][p*64+ordinal]["path"].as_str().unwrap()).unwrap()).unwrap();
            let ids=result["sq4"]["fetched_ids"].as_array().unwrap();
            json!({"ordinal":ordinal,"query_sha256":query_sha,"row_ranges":result["plan"]["row_ranges"],"fetched_rows":ids.len(),
                "fetched_ids_sha256":hash(&ids.iter().flat_map(|v|v.as_i64().unwrap().to_le_bytes()).collect::<Vec<_>>()),
                "nominee_ordinals_sha256":hash(&nominees.iter().flat_map(|&i|(i as u64).to_le_bytes()).collect::<Vec<_>>())})
        }).collect::<Vec<_>>()})).collect::<Vec<_>>();
        let closed=probe_artifact(&dispersed.join("closed.json"),&serde_json::to_vec(&json!({"schema":"borsuk-corrected-four-bit-closed-populations-v1",
            "rows":n,"dimensions":3,"count_per_dataset":64,"source_row_bytes":15,"packed_row_bytes":14,"fetched_id_hash_encoding":"ordered-le-i64",
            "nominee_ordinal_hash_encoding":"ordered-le-u64","historical_prefix":old.prefix,"panels":panels})).unwrap());
        let cfg = corrected::Config {
            schema: corrected::CONFIG_SCHEMA.into(),
            source_identity_sha256: corrected::source_identity(),
            panels: old.panels,
            original_seal: old.original_seal,
            prefix: old.prefix,
            caps: old.caps,
            rotation_seed: [23; 32],
            construction_operations: 20_000_000_000,
            query_auth_operations: 20_000_000_000,
            closed_populations: closed,
        };
        for case in ["valid", "late-truth"] {
            let mut c = cfg.clone();
            if case == "late-truth" {
                let mut body = fs::read(&truth.path).unwrap();
                body[63 * 400..63 * 400 + 4].copy_from_slice(&999u32.to_le_bytes());
                c.panels[1].truth = probe_artifact(&dispersed.join("invalid-truth"), &body);
            }
            let path = dispersed.join(format!("corrected-{case}.json"));
            let result = corrected::diagnose(&c, &hash(b"corrected"), &path);
            let report: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            assert_eq!(report["queries"], 128);
            let freeze: Value = serde_json::from_slice(
                &fs::read(path.with_extension("corrected-four-bit-freeze.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(freeze["results"].as_array().unwrap().len(), 128);
            assert_eq!(freeze["truth_opened"], false);
            if case == "late-truth" {
                assert!(result.is_err());
                assert_eq!(report["status"], "INVALID");
                assert_eq!(report["details"]["truth_opened"], true);
                continue;
            }
            result.unwrap();
            assert_eq!(report["complete"], true);
            let rotation_pin: &Value = &freeze["rotation"];
            let rotation_body = fs::read(rotation_pin["path"].as_str().unwrap()).unwrap();
            assert_eq!(json!(hash(&rotation_body)), rotation_pin["sha256"]);
            assert_eq!(rotation_body.len(), 64 + 9 * 8);
            let matrix = std::array::from_fn::<_, 9, _>(|i| {
                f64::from_le_bytes(rotation_body[64 + i * 8..72 + i * 8].try_into().unwrap())
            });
            let normalized =
                borsuk::two_bit_generation::normalize_two_bit_diagnostic_query(&query).unwrap();
            // Independent scalar direction and packed-score oracle: no codec
            // prepare/score or SQ8 scorer calls may supply expected values.
            fn scalar_unit(v: &mut [f64]) {
                let scale = v.iter().fold(0f64, |a, b| a.max(b.abs()));
                let mut squares = 0.;
                for x in v.iter_mut() {
                    *x /= scale;
                    squares += *x * *x;
                }
                let norm = squares.sqrt();
                for x in v {
                    *x /= norm;
                }
            }
            let mut unit_query = std::array::from_fn::<_, 3, _>(|j| f64::from(normalized[j]));
            scalar_unit(&mut unit_query);
            let mut rotated_query = [0.; 3];
            for i in 0..3 {
                for j in 0..3 {
                    rotated_query[i] += matrix[i * 3 + j] * unit_query[j];
                }
            }
            scalar_unit(&mut rotated_query);
            for p in 0..2 {
                let packed =
                    fs::read(path.with_extension(format!("corrected-four-bit-{p}.bin"))).unwrap();
                assert_eq!(packed.len(), n * 14);
                let ids = sources[p]
                    .chunks_exact(15)
                    .map(|row| i64::from_le_bytes(row[..8].try_into().unwrap()))
                    .collect::<Vec<_>>();
                let qnorm = normalized
                    .iter()
                    .map(|&v| f64::from(v).powi(2))
                    .sum::<f64>()
                    .sqrt();
                let mut cosine = sources[p]
                    .chunks_exact(15)
                    .enumerate()
                    .map(|(ordinal, row)| {
                        let x = std::array::from_fn::<_, 3, _>(|j| {
                            f64::from(low[j] + f32::from(row[12 + j]) * step[j])
                        });
                        let norm = x.iter().map(|v| v * v).sum::<f64>().sqrt();
                        let dot = (0..3)
                            .map(|j| x[j] / norm * (f64::from(normalized[j]) / qnorm))
                            .sum::<f64>();
                        ((2. - 2. * dot) as f32, ids[ordinal], ordinal)
                    })
                    .collect::<Vec<_>>();
                cosine.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                // The independently decoded source/query oracle must identify
                // the deliberately selected incidental row as the rank-one
                // winner. A query following a nominee fails this assertion.
                let incidental_winner = cosine[0];
                assert_eq!(incidental_winner.2, 1);
                assert!(!nominees.contains(&incidental_winner.2));
                let mut shift = 0f32;
                let mut query_norm = 0f32;
                let mut weights = [0f32; 3];
                for j in 0..3 {
                    shift += normalized[j] * low[j];
                    query_norm += normalized[j] * normalized[j];
                    weights[j] = normalized[j] * step[j];
                }
                shift -= query_norm / 2.;
                let mut sq8 = sources[p]
                    .chunks_exact(15)
                    .enumerate()
                    .map(|(ordinal, row)| {
                        let norm = f32::from_le_bytes(row[8..12].try_into().unwrap());
                        let mut inner = 0f32;
                        for j in 0..3 {
                            inner += f32::from(row[12 + j]) * weights[j];
                        }
                        (norm - 2. * (inner + shift), ids[ordinal], ordinal)
                    })
                    .collect::<Vec<_>>();
                sq8.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                let mut expected = packed
                    .chunks_exact(14)
                    .enumerate()
                    .map(|(i, row)| {
                        let correction = f64::from(f32::from_le_bytes(row[8..12].try_into().unwrap()));
                        let mut dot = 0.;
                        for j in 0..3 {
                            let code = (row[12 + j / 2] >> (4 * (j % 2))) & 15;
                            dot += rotated_query[j] * (f64::from(code) - 7.5);
                        }
                        ((2. - 2. * correction * dot) as f32, ids[i], i)
                    })
                    .collect::<Vec<_>>();
                expected.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                assert!(!nominees.contains(&expected[0].2));
                for ordinal in 0..64 {
                    let result: Value = serde_json::from_slice(
                        &fs::read(
                            freeze["results"][p * 64 + ordinal]["path"]
                                .as_str()
                                .unwrap(),
                        )
                        .unwrap(),
                    )
                    .unwrap();
                    let winner = &result["decoded_cosine"]["ranked"][0];
                    assert_eq!(winner["ordinal"], incidental_winner.2);
                    assert_eq!(winner["id"], incidental_winner.1);
                    assert_eq!(winner["score_bits"], incidental_winner.0.to_bits());
                    for key in ["corrected", "decoded_cosine", "sq8_reference"] {
                        assert_eq!(result[key]["fetched_ids"], json!(ids));
                        assert_eq!(result[key]["ranked"].as_array().unwrap().len(), 100);
                    }
                    for (key, oracle) in [
                        ("corrected", &expected),
                        ("decoded_cosine", &cosine),
                        ("sq8_reference", &sq8),
                    ] {
                        for (rank, s) in oracle.iter().take(100).enumerate() {
                            assert_eq!(result[key]["ranked"][rank]["id"], s.1);
                            assert_eq!(result[key]["ranked"][rank]["score_bits"], s.0.to_bits());
                            assert_eq!(result[key]["ranked"][rank]["ordinal"], s.2);
                        }
                        assert_eq!(result[key]["rank100_score_bits"], oracle[99].0.to_bits());
                        assert_eq!(result[key]["rank101_score_bits"], oracle[100].0.to_bits());
                        assert_eq!(
                            result[key]["rank_boundary_gap"].as_f64(),
                            Some(f64::from(oracle[100].0) - f64::from(oracle[99].0))
                        );
                    }
                }
            }
        }
        let pin = probe_artifact(
            &dispersed.join("strict-config"),
            &serde_json::to_vec(&cfg).unwrap(),
        );
        assert!(
            corrected::check_fine_corrected_four_bit(
                &pin.path,
                &pin.sha256,
                &dispersed.join("strict.json")
            )
            .is_err()
        );
    }

    #[test]
    fn fine_histogram_sq4_strict_cli_dispatch_and_schema_identity() {
        use borsuk::fine_sq8_groups::histogram_sq4_diagnostic as histogram;
        let tmp = tempfile::tempdir().unwrap();
        let pin = probe_artifact(
            &tmp.path().join("config.json"),
            br#"{"schema":"borsuk-histogram-sq4-config-v1","unknown":true}"#,
        );
        let output = tmp.path().join("report.json");
        let args = vec![
            "hierarchical_semantic_cells".into(),
            "check-fine-histogram-sq4".into(),
            pin.path.display().to_string(),
            pin.sha256,
            output.display().to_string(),
        ];
        assert!(execute_args(&args[..4]).is_err());
        assert!(!output.exists());
        assert!(execute_args(&args).is_err());
        let body = fs::read(&output).unwrap();
        let report: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(report["schema"], histogram::REPORT_SCHEMA);
        assert_eq!(report["codec"], histogram::CODEC);
        assert_eq!(report["status"], "INVALID");
        assert_eq!(
            report["source_identity_sha256"],
            histogram::source_identity()
        );
        assert_ne!(
            histogram::source_identity(),
            borsuk::fine_sq8_groups::sq4_diagnostic::source_identity()
        );
        assert!(execute_args(&args).is_err());
        assert_eq!(fs::read(&output).unwrap(), body);
    }
    #[test]
    fn fine_histogram_sq4_real_native_builder_paired_pipeline_all128_full_rosters_late_invalid() {
        use borsuk::fine_sq8_groups::{
            FineBuildConfig, FineSq8Index, ResidentLimits, histogram_sq4_diagnostic as histogram,
        };
        let tmp = tempfile::tempdir().unwrap();
        let primary = tiny_fine_primary(tmp.path());
        let build = FineBuildConfig {
            schema: borsuk::fine_sq8_groups::BUILD_SCHEMA.into(),
            primary_root: primary,
            max_build_payload_bytes: 128 * 1024 * 1024,
            max_output_bytes: 16 * 1024 * 1024,
        };
        let roots = ["relaion", "cohere"]
            .map(|name| FineSq8Index::build(&build, &tmp.path().join(name)).unwrap());
        let requests = (0..64)
            .map(|ordinal| format!("{}\n", json!({"ordinal":ordinal,"query":[3.125,0.875]})))
            .collect::<String>();
        let request = probe_artifact(&tmp.path().join("requests"), requests.as_bytes());
        let truth_bytes = (0..64)
            .flat_map(|_| (0..100u32).flat_map(u32::to_le_bytes))
            .collect::<Vec<_>>();
        let truth = probe_artifact(&tmp.path().join("truth"), &truth_bytes);
        let original = tmp.path().join("original.jsonl");
        paired_fine(
            FinePairedConfig {
                schema: "borsuk-fine-sq8-paired-v1".into(),
                panels: std::array::from_fn(|i| FinePanel {
                    dataset: ["relaion", "cohere"][i].into(),
                    root: roots[i].clone(),
                    requests: request.clone(),
                    truth: truth.clone(),
                    truth_width: 100,
                }),
                source_identity_sha256: fine_source_identity(),
                limits: ResidentLimits {
                    max_peak_payload_bytes: 256 * 1024 * 1024,
                    pinned_generation_bytes: 0,
                    active_queries: 1,
                    delta_bytes: 0,
                    maintenance_bytes: 0,
                    runtime_bytes: 0,
                },
                max_evaluator_payload_bytes: 128 * 1024 * 1024,
                max_result_bytes: 16 * 1024 * 1024,
                test_geometry: Some((135, 2)),
                fail_sync_at: None,
            },
            &"a".repeat(64),
            &original,
        )
        .unwrap();
        let seal_path = original.with_extension("fine-seal.json");
        let seal_body = fs::read(&seal_path).unwrap();
        let seal: Value = serde_json::from_slice(&seal_body).unwrap();
        let cfg = histogram::Config {
            schema: histogram::CONFIG_SCHEMA.into(),
            source_identity_sha256: histogram::source_identity(),
            panels: std::array::from_fn(|i| histogram::Panel {
                dataset: ["relaion", "cohere"][i].into(),
                root: roots[i].clone(),
                requests: request.clone(),
                truth: truth.clone(),
                truth_width: 100,
            }),
            original_seal: Artifact {
                path: seal_path,
                bytes: seal_body.len(),
                sha256: hash(&seal_body),
            },
            prefix: Artifact {
                path: original,
                bytes: seal["prefix_bytes"].as_u64().unwrap() as usize,
                sha256: seal["prefix_sha256"].as_str().unwrap().into(),
            },
            caps: histogram::Caps {
                memory_bytes: 256 * 1024 * 1024,
                output_bytes: 128 * 1024 * 1024,
                deadline_seconds: 600,
                operations: 20_000_000_000,
                cpu_threads: 1,
                swap_bytes: 0,
            },
        };
        let manifests = roots
            .each_ref()
            .map(|r| serde_json::from_slice::<Value>(&fs::read(&r.path).unwrap()).unwrap());
        let sources = manifests
            .each_ref()
            .map(|m| fs::read(m["records"]["path"].as_str().unwrap()).unwrap());
        for case in [
            "valid",
            "missing",
            "tampered",
            "late-invalid",
            "query",
            "binding",
            "output-cap",
            "memory",
            "source",
            "old-schema",
        ] {
            let mut config = cfg.clone();
            match case {
                "missing" => config.panels[1].truth.path = tmp.path().join("missing"),
                "tampered" => fs::write(&truth.path, vec![0; truth.bytes]).unwrap(),
                "late-invalid" => {
                    let mut invalid = truth_bytes.clone();
                    invalid[63 * 400..63 * 400 + 4].copy_from_slice(&999u32.to_le_bytes());
                    let pin = probe_artifact(&tmp.path().join("late-truth"), &invalid);
                    config.panels[1].truth = pin;
                }
                "query" => fs::write(&request.path, b"tampered").unwrap(),
                "binding" => config.panels[0].requests.sha256 = "0".repeat(64),
                "output-cap" => config.caps.output_bytes = 8192,
                "memory" => config.caps.memory_bytes = 64 * 1024 * 1024,
                "source" => config.source_identity_sha256 = "0".repeat(64),
                "old-schema" => {
                    config.schema = borsuk::fine_sq8_groups::sq4_diagnostic::CONFIG_SCHEMA.into()
                }
                _ => {}
            }
            let output = tmp.path().join(format!("histogram-{case}.json"));
            let result = histogram::diagnose(&config, &"b".repeat(64), &output);
            let body = fs::read(&output).unwrap();
            let report: Value = serde_json::from_slice(&body).unwrap();
            if case == "valid" {
                result.unwrap();
                assert_eq!(report["complete"], true);
                assert_eq!(report["queries"], 128);
            } else {
                assert!(result.is_err());
                assert_eq!(report["status"], "INVALID");
            }
            let frozen = output.with_extension("histogram-sq4-freeze.json");
            if ["valid", "missing", "tampered", "late-invalid"].contains(&case) {
                let freeze: Value = serde_json::from_slice(&fs::read(&frozen).unwrap()).unwrap();
                assert_eq!(freeze["results"].as_array().unwrap().len(), 128);
                assert_eq!(freeze["truth_opened"], false);
                assert_eq!(report["queries"], 128);
                for (i, payload) in freeze["payloads"].as_array().unwrap().iter().enumerate() {
                    let generation = &payload["histogram_generation"];
                    let book_pin: Artifact =
                        serde_json::from_value(generation["book"].clone()).unwrap();
                    let generation_pin: Artifact =
                        serde_json::from_value(generation["root"].clone()).unwrap();
                    for pin in [&book_pin, &generation_pin] {
                        assert_eq!(hash(&fs::read(&pin.path).unwrap()), pin.sha256);
                    }
                    let root: Value =
                        serde_json::from_slice(&fs::read(&generation_pin.path).unwrap()).unwrap();
                    assert_eq!(root["schema"], histogram::ROOT_SCHEMA);
                    assert_eq!(root["original_root"]["sha256"], roots[i].sha256);
                    assert_eq!(
                        root["original_records"]["sha256"],
                        manifests[i]["records"]["sha256"]
                    );
                    assert!(book_pin.bytes <= histogram::BOOK_CAP);
                    assert_eq!(generation["retained_book_capacity_bytes"], 130);
                }
                for (index, item) in freeze["results"].as_array().unwrap().iter().enumerate() {
                    let pin: Artifact = serde_json::from_value(item.clone()).unwrap();
                    let bytes = fs::read(&pin.path).unwrap();
                    assert_eq!(hash(&bytes), pin.sha256);
                    let result: Value = serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(result["truth_opened"], false);
                    let ranges: Vec<std::ops::Range<usize>> =
                        serde_json::from_value(result["plan"]["row_ranges"].clone()).unwrap();
                    let expected = ranges
                        .iter()
                        .flat_map(|r| r.clone())
                        .map(|row| {
                            i64::from_le_bytes(
                                sources[index / 64][row * 14..row * 14 + 8]
                                    .try_into()
                                    .unwrap(),
                            )
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(result["sq4"]["fetched_ids"], json!(expected));
                    assert_eq!(
                        result["sq4"]["fetched_ids"],
                        result["sq8_reference"]["fetched_ids"]
                    );
                    assert_eq!(result["sq4"]["ranked"].as_array().unwrap().len(), 100);
                    assert_eq!(result["sq4"]["expanded_capacity_peak"], 0);
                    assert!(result["nominees_retained"].as_bool().unwrap());
                    assert_eq!(
                        result["sq4"]["histogram_metrics"]["total_cold_get_claim"],
                        false
                    );
                }
            } else {
                assert!(!frozen.exists());
            }
            if case == "query" {
                assert!(
                    output
                        .with_extension("histogram-sq4-payloads.json")
                        .exists()
                );
            }
            if ["output-cap", "memory", "source", "old-schema"].contains(&case) {
                assert!(!output.with_extension("histogram-sq4-0.bin").exists());
            }
            assert!(histogram::diagnose(&config, &"b".repeat(64), &output).is_err());
            assert_eq!(fs::read(&output).unwrap(), body);
            fs::write(&truth.path, &truth_bytes).unwrap();
            fs::write(&request.path, requests.as_bytes()).unwrap();
        }
        let pin = probe_artifact(
            &tmp.path().join("histogram-config.json"),
            &serde_json::to_vec(&cfg).unwrap(),
        );
        assert!(
            histogram::check_fine_histogram_sq4(
                &pin.path,
                &pin.sha256,
                &tmp.path().join("strict.json")
            )
            .is_err()
        );
    }
    #[test]
    fn fine_sq4_strict_cli_dispatch() {
        let temp = tempfile::tempdir().unwrap();
        let pin = probe_artifact(
            &temp.path().join("config.json"),
            br#"{"schema":"borsuk-fixed-sq4-config-v1","unknown":true}"#,
        );
        let output = temp.path().join("result.json");
        let args = vec![
            "hierarchical_semantic_cells".into(),
            "check-fine-sq4".into(),
            pin.path.display().to_string(),
            pin.sha256,
            output.display().to_string(),
        ];
        assert!(execute_args(&args[..4]).is_err());
        assert!(!output.exists());
        assert!(execute_args(&args).is_err());
        let body = fs::read(&output).unwrap();
        let report: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(report["schema"], "borsuk-fixed-sq4-report-v1");
        assert_eq!(report["status"], "INVALID");
        assert!(execute_args(&args).is_err());
        assert_eq!(fs::read(&output).unwrap(), body);

        use borsuk::fine_sq8_groups::sq4_diagnostic as sq4;
        let absent = Artifact {
            path: temp.path().join("must-not-open"),
            bytes: 1,
            sha256: "1".repeat(64),
        };
        let truth_sha = [
            "3ad233f399ba5172051e747f24dbde402bdd357a69460c349ec5ed5872ee5a5c",
            "f6630d0edf06539752c3fbf129ae01e58d3a3cf7b6aefa4decaa9c979e8ba355",
        ];
        let config = sq4::Config {
            schema: sq4::CONFIG_SCHEMA.into(),
            source_identity_sha256: sq4::source_identity(),
            panels: std::array::from_fn(|i| sq4::Panel {
                dataset: ["relaion", "cohere"][i].into(),
                root: absent.clone(),
                requests: absent.clone(),
                truth: Artifact {
                    path: temp.path().join(format!("relocated-truth-{i}")),
                    bytes: 25_600,
                    sha256: truth_sha[i].into(),
                },
                truth_width: 100,
            }),
            original_seal: absent.clone(),
            prefix: absent,
            caps: sq4::Caps {
                memory_bytes: 1024 * 1024 * 1024,
                output_bytes: 128 * 1024 * 1024,
                deadline_seconds: 600,
                operations: 20_000_000_000,
                cpu_threads: 1,
                swap_bytes: 0,
            },
        };
        for panel in 0..2 {
            let mut alternate = config.clone();
            alternate.panels[panel].truth.sha256 = "0".repeat(64);
            let pin = probe_artifact(
                &temp.path().join(format!("alternate-{panel}.json")),
                &serde_json::to_vec(&alternate).unwrap(),
            );
            let output = temp.path().join(format!("rejected-{panel}.json"));
            let args = vec![
                "hierarchical_semantic_cells".into(),
                "check-fine-sq4".into(),
                pin.path.display().to_string(),
                pin.sha256,
                output.display().to_string(),
            ];
            let error = execute_args(&args).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("archived truth descriptor binding")
            );
            let report: Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
            assert_eq!(report["status"], "INVALID");
            assert!(!output.with_extension("sq4-0.bin").exists());
        }
    }

    #[test]
    fn fine_sq4_real_native_pipeline_128_seal_before_truth() {
        use borsuk::fine_sq8_groups::{
            FineBuildConfig, FineSq8Index, ResidentLimits, sq4_diagnostic as sq4,
        };
        let temp = tempfile::tempdir().unwrap();
        let primary = tiny_fine_primary(temp.path());
        let build = FineBuildConfig {
            schema: borsuk::fine_sq8_groups::BUILD_SCHEMA.into(),
            primary_root: primary,
            max_build_payload_bytes: 128 * 1024 * 1024,
            max_output_bytes: 16 * 1024 * 1024,
        };
        let roots = ["relaion", "cohere"]
            .map(|name| FineSq8Index::build(&build, &temp.path().join(name)).unwrap());
        let request = probe_artifact(
            &temp.path().join("requests"),
            (0..64)
                .map(|ordinal| format!("{}\n", json!({"ordinal":ordinal,"query":[3.125,0.875]})))
                .collect::<String>()
                .as_bytes(),
        );
        let truth = probe_artifact(
            &temp.path().join("truth"),
            &(0..64)
                .flat_map(|_| (0..100u32).flat_map(u32::to_le_bytes))
                .collect::<Vec<_>>(),
        );
        let original = temp.path().join("original.jsonl");
        paired_fine(
            FinePairedConfig {
                schema: "borsuk-fine-sq8-paired-v1".into(),
                panels: [
                    FinePanel {
                        dataset: "relaion".into(),
                        root: roots[0].clone(),
                        requests: request.clone(),
                        truth: truth.clone(),
                        truth_width: 100,
                    },
                    FinePanel {
                        dataset: "cohere".into(),
                        root: roots[1].clone(),
                        requests: request.clone(),
                        truth: truth.clone(),
                        truth_width: 100,
                    },
                ],
                source_identity_sha256: fine_source_identity(),
                limits: ResidentLimits {
                    max_peak_payload_bytes: 256 * 1024 * 1024,
                    pinned_generation_bytes: 0,
                    active_queries: 1,
                    delta_bytes: 0,
                    maintenance_bytes: 0,
                    runtime_bytes: 0,
                },
                max_evaluator_payload_bytes: 128 * 1024 * 1024,
                max_result_bytes: 16 * 1024 * 1024,
                test_geometry: Some((135, 2)),
                fail_sync_at: None,
            },
            &"a".repeat(64),
            &original,
        )
        .unwrap();
        let seal_path = original.with_extension("fine-seal.json");
        let seal_body = fs::read(&seal_path).unwrap();
        let seal: Value = serde_json::from_slice(&seal_body).unwrap();
        let cfg = sq4::Config {
            schema: sq4::CONFIG_SCHEMA.into(),
            source_identity_sha256: sq4::source_identity(),
            panels: ["relaion", "cohere"].map(|name| sq4::Panel {
                dataset: name.into(),
                root: roots[usize::from(name == "cohere")].clone(),
                requests: request.clone(),
                truth: truth.clone(),
                truth_width: 100,
            }),
            original_seal: Artifact {
                path: seal_path,
                bytes: seal_body.len(),
                sha256: hash(&seal_body),
            },
            prefix: Artifact {
                path: original,
                bytes: seal["prefix_bytes"].as_u64().unwrap() as usize,
                sha256: seal["prefix_sha256"].as_str().unwrap().into(),
            },
            caps: sq4::Caps {
                memory_bytes: 256 * 1024 * 1024,
                output_bytes: 128 * 1024 * 1024,
                deadline_seconds: 600,
                operations: 20_000_000_000,
                cpu_threads: 1,
                swap_bytes: 0,
            },
        };
        for case in [
            "valid", "missing", "tampered", "query", "binding", "cap", "pair-cap", "memory",
            "source",
        ] {
            let mut config = cfg.clone();
            if case == "missing" {
                config.panels[1].truth.path = temp.path().join("missing");
            }
            if case == "tampered" {
                fs::write(&truth.path, vec![0; truth.bytes]).unwrap();
            }
            if case == "query" {
                fs::write(&request.path, b"tampered").unwrap();
            }
            if case == "binding" {
                config.panels[0].requests.sha256 = "0".repeat(64);
            }
            if case == "cap" {
                config.caps.output_bytes = 8192;
            }
            if case == "pair-cap" {
                config.caps.output_bytes = 135 * 13 + 8192;
            }
            if case == "memory" {
                config.caps.memory_bytes = 64 * 1024 * 1024;
            }
            if case == "source" {
                config.source_identity_sha256 = "0".repeat(64);
            }
            let output = temp.path().join(format!("sq4-{case}.json"));
            let result = sq4::diagnose(&config, &"b".repeat(64), &output);
            let body = fs::read(&output).unwrap();
            let report: Value = serde_json::from_slice(&body).unwrap();
            if case == "valid" {
                result.unwrap();
                assert_eq!(report["complete"], true);
                assert_eq!(report["queries"], 128);
                let receipt = sq4::SupervisorReceipt {
                    run_id: "tiny".into(),
                    config_sha256: "b".repeat(64),
                    report_sha256: hash(&body),
                    process_exit_code: 0,
                    resource_limits_observed: true,
                    drain_complete: true,
                    cleanup_complete: true,
                };
                assert!(sq4::admit_survival(&body, "tiny", &receipt).is_err());
            } else {
                assert!(result.is_err());
                assert_eq!(report["status"], "INVALID");
            }
            let frozen = output.with_extension("sq4-freeze.json");
            if ["valid", "missing", "tampered"].contains(&case) {
                let freeze: Value = serde_json::from_slice(&fs::read(&frozen).unwrap()).unwrap();
                assert_eq!(freeze["results"].as_array().unwrap().len(), 128);
                assert_eq!(freeze["truth_opened"], false);
                assert_eq!(freeze["payloads"].as_array().unwrap().len(), 2);
                for item in freeze["results"].as_array().unwrap() {
                    let artifact: Artifact = serde_json::from_value(item.clone()).unwrap();
                    let bytes = fs::read(&artifact.path).unwrap();
                    assert_eq!(hash(&bytes), artifact.sha256);
                    let scored: Value = serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(scored["nominees_retained"], true);
                    assert_eq!(scored["original_cover_contained"], true);
                    assert_eq!(scored["sq8_reference_serving_eligible"], false);
                    assert_eq!(scored["sq4"]["ranked"].as_array().unwrap().len(), 100);
                }
            } else {
                assert!(!frozen.exists());
            }
            if case == "query" {
                assert!(output.with_extension("sq4-payloads.json").exists());
            }
            if ["cap", "pair-cap"].contains(&case) {
                assert!(!output.with_extension("sq4-0.bin").exists());
                assert!(!output.with_extension("sq4-1.bin").exists());
            }
            assert!(sq4::diagnose(&config, &"b".repeat(64), &output).is_err());
            assert_eq!(fs::read(&output).unwrap(), body);
            if case == "tampered" {
                fs::write(
                    &truth.path,
                    (0..64)
                        .flat_map(|_| (0..100u32).flat_map(u32::to_le_bytes))
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            }
            if case == "query" {
                fs::write(
                    &request.path,
                    (0..64)
                        .map(|ordinal| {
                            format!("{}\n", json!({"ordinal":ordinal,"query":[3.125,0.875]}))
                        })
                        .collect::<String>(),
                )
                .unwrap();
            }
        }
        let config = probe_artifact(
            &temp.path().join("sq4-config.json"),
            &serde_json::to_vec(&cfg).unwrap(),
        );
        // Tiny geometry is available only through the explicit fixture API; the real CLI is fixed.
        assert!(
            sq4::check_fine_sq4(
                &config.path,
                &config.sha256,
                &temp.path().join("strict.json")
            )
            .is_err()
        );

        // Real builders supply the records/router/PQ. Controlled, separately
        // sealed fixture nominations force 34 separated group16 extents, so
        // this pipeline must bridge two gaps to reach exactly 32 ranges.
        let dispersed = temp.path().join("dispersed");
        fs::create_dir(&dispersed).unwrap();
        let n = 1063usize; // 66 complete groups and a seven-row tail.
        let codec = borsuk::rotated_two_bit::RotatedTwoBitCodec::new(&[0.; 3], 20260923).unwrap();
        let low = [-1_f32; 3];
        let step = [0.01_f32; 3];
        let mut canonical = Vec::new();
        let mut records = Vec::new();
        let mut sq8_body = Vec::new();
        let mut order = Vec::new();
        for id in (0..n).rev() {
            let angle = id as f32 * 0.31;
            let height = ((id * 37) % n) as f32 / n as f32 * 1.8 - 0.9;
            let radius = (1. - height * height).sqrt();
            let vector = [radius * angle.cos(), radius * angle.sin(), height];
            canonical.extend((id as i64).to_le_bytes());
            canonical.extend(vector.into_iter().flat_map(f32::to_le_bytes));
            records.extend(codec.encode(&vector).unwrap());
            let codes = vector.map(|v| ((v + 1.) / 0.01).round() as u8);
            let mut norm = 0_f32;
            for axis in 0..3 {
                let value = low[axis] + f32::from(codes[axis]) * step[axis];
                norm += value * value;
            }
            sq8_body.extend((id as i64).to_le_bytes());
            sq8_body.extend(norm.to_le_bytes());
            sq8_body.extend(codes);
            order.extend((id as u64).to_le_bytes());
        }
        let canonical = probe_artifact(&dispersed.join("canonical"), &canonical);
        let records = probe_artifact(&dispersed.join("codes"), &records);
        let sq8_source = probe_artifact(&dispersed.join("sq8"), &sq8_body);
        let order = probe_artifact(&dispersed.join("order"), &order);
        let mean = probe_artifact(&dispersed.join("mean"), &[0; 12]);
        let plane = probe_artifact(
            &dispersed.join("plane"),
            &serde_json::to_vec(&borsuk::two_bit_source::SourcePlaneReceipt {
                schema: "borsuk-two-bit-plane-v3".into(),
                rows: n,
                dimensions: 3,
                seed: 20260923,
                record_bytes: codec.record_bytes(),
                source_sha256: "0".repeat(64),
                sq8_sha256: sq8_source.sha256.clone(),
                source_order_sha256: order.sha256.clone(),
                mean_sha256: mean.sha256.clone(),
                records_sha256: records.sha256.clone(),
                page_rows: 32,
                page_digest_sha256: "0".repeat(64),
                query_or_truth_used: false,
            })
            .unwrap(),
        );
        let generation = probe_artifact(&dispersed.join("generation"), &serde_json::to_vec(&json!({
            "schema":"borsuk-two-bit-generation-v8","generation":1,"base_epoch":0,
            "plane_manifest_sha256":plane.sha256,"page_manifest_sha256":"0".repeat(64),
            "discovery":{"mode":"graph","centroids_sha256":"0".repeat(64),"graph_sha256":"0".repeat(64),
                "graph_resident_bytes":1,"diverse_graph_sha256":"0".repeat(64),"diverse_graph_resident_bytes":1},
            "sq8_object_sha256":sq8_source.sha256,"sq8_object_key":format!("objects/{}",sq8_source.sha256),"sq8_etag":"fixture",
            "canonical":{"rows":n,"dimensions":3,"bytes":canonical.bytes,"sha256":canonical.sha256,
                "object_key":format!("objects/{}",canonical.sha256)},"low":low,"step":step
        })).unwrap());
        let primary_path = dispersed.join("primary");
        let receipt = borsuk::hierarchical_semantic_cells::build(
            &BuildConfig {
                schema: borsuk::hierarchical_semantic_cells::BUILD_SCHEMA.into(),
                generation,
                plane,
                canonical,
                order,
                records,
                mean,
                sq8: sq8_source,
                cell_rows: 32,
                sample_rows: 32,
                max_depth: 24,
                max_build_payload_bytes: 64 * 1024 * 1024,
                max_output_bytes: 16 * 1024 * 1024,
            },
            &primary_path,
        )
        .unwrap();
        let primary_root = Artifact {
            path: primary_path.join("manifest.json"),
            bytes: fs::metadata(primary_path.join("manifest.json"))
                .unwrap()
                .len() as usize,
            sha256: receipt.root_sha256,
        };
        let roots = ["relaion", "cohere"].map(|name| {
            FineSq8Index::build(
                &FineBuildConfig {
                    schema: borsuk::fine_sq8_groups::BUILD_SCHEMA.into(),
                    primary_root: primary_root.clone(),
                    max_build_payload_bytes: 128 * 1024 * 1024,
                    max_output_bytes: 16 * 1024 * 1024,
                },
                &dispersed.join(name),
            )
            .unwrap()
        });
        let manifests = roots
            .each_ref()
            .map(|root| serde_json::from_slice::<Value>(&fs::read(&root.path).unwrap()).unwrap());
        let sources = manifests
            .each_ref()
            .map(|m| fs::read(m["records"]["path"].as_str().unwrap()).unwrap());
        let queries = (0..64)
            .map(|ordinal| {
                std::array::from_fn::<_, 3, _>(|axis| {
                    let value = low[axis] + f32::from(sources[0][16 * 15 + 12 + axis]) * step[axis];
                    value * (2. + ordinal as f32 * 0.01)
                        + if axis == 1 {
                            ordinal as f32 * 0.001
                        } else {
                            0.
                        }
                })
            })
            .collect::<Vec<_>>();
        let request = probe_artifact(
            &dispersed.join("requests"),
            (0..64)
                .map(|ordinal| format!("{}\n", json!({"ordinal":ordinal,"query":queries[ordinal]})))
                .collect::<String>()
                .as_bytes(),
        );
        let truth = probe_artifact(
            &dispersed.join("truth"),
            &(0..64)
                .flat_map(|_| (0..100u32).flat_map(u32::to_le_bytes))
                .collect::<Vec<_>>(),
        );
        let original_ranges = (0..=66usize)
            .step_by(2)
            .map(|g| g * 16..((g + 1) * 16).min(n))
            .collect::<Vec<_>>();
        let nominees = original_ranges
            .iter()
            .map(|r| r.end - 1)
            .collect::<Vec<_>>();
        // Equal-size gaps break ties on their left positions: merge groups0,2,4.
        let expected_ranges = std::iter::once(0..80)
            .chain(original_ranges.iter().skip(3).cloned())
            .collect::<Vec<_>>();
        let candidate_rows = expected_ranges
            .iter()
            .flat_map(|r| r.clone())
            .collect::<Vec<_>>();
        let baseline_rows = original_ranges
            .iter()
            .flat_map(|r| r.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            (
                original_ranges.len(),
                expected_ranges.len(),
                candidate_rows.len(),
                baseline_rows.len()
            ),
            (34, 32, 567, 535)
        );
        let mut prefix = String::new();
        for panel in 0..2 {
            prefix += &format!(
                "{}\n",
                json!({"phase":"startup","dataset":(["relaion","cohere"][panel]),
                "root":roots[panel],"resources":borsuk::fine_sq8_groups::ResourceReceipt::default(),
                "build":manifests[panel]["build"],"truth_opened":false,"wall_ns":0,"process_cpu_ns":0})
            );
        }
        for panel in 0..2 {
            for (ordinal, query) in queries.iter().enumerate() {
                let query_sha = hash(
                    &query
                        .iter()
                        .flat_map(|v| v.to_le_bytes())
                        .collect::<Vec<_>>(),
                );
                prefix += &format!(
                    "{}\n",
                    json!({"phase":"fine_plan","dataset":(["relaion","cohere"][panel]),
                    "ordinal":ordinal,"truth_opened":false,"wall_ns":0,"process_cpu_ns":0,"plan":{
                    "root_sha256":roots[panel].sha256,"query_sha256":query_sha,"revision":0,"mutation_sha256":"",
                    "nominees":nominees,"ranges":original_ranges.iter().map(|r|r.start*15..r.end*15).collect::<Vec<_>>(),
                    "planned_bytes":535*15,"feasible":true,"exhausted":false,"converged":true,
                    "actual_shortlist_rows":34,"evaluations":n,"base_visits":n,"old_page_gets":1,"old_page_bytes":n*15}})
                );
            }
        }
        let prefix = probe_artifact(&dispersed.join("prefix"), prefix.as_bytes());
        let seal = probe_artifact(&dispersed.join("seal"), &serde_json::to_vec(&json!({
            "schema":"borsuk-fine-sq8-seal-v1","config_sha256":hash(b"controlled dispersed fixture"),
            "source_identity_sha256":fine_source_identity(),"prefix_bytes":prefix.bytes,"prefix_sha256":prefix.sha256,
            "plans_per_panel":64,"truth_opened":false,"panels":(0..2).map(|i|json!({
                "dataset":(["relaion","cohere"][i]),"root":roots[i],"requests":request})).collect::<Vec<_>>()
        })).unwrap());
        let config = sq4::Config {
            original_seal: seal,
            prefix,
            panels: std::array::from_fn(|i| sq4::Panel {
                dataset: ["relaion", "cohere"][i].into(),
                root: roots[i].clone(),
                requests: request.clone(),
                truth: truth.clone(),
                truth_width: 100,
            }),
            ..cfg
        };
        let output = dispersed.join("report.json");
        sq4::diagnose(&config, &"c".repeat(64), &output).unwrap();
        let freeze: Value =
            serde_json::from_slice(&fs::read(output.with_extension("sq4-freeze.json")).unwrap())
                .unwrap();
        assert_eq!(freeze["results"].as_array().unwrap().len(), 128);
        assert_eq!(freeze["truth_opened"], false);
        for panel in 0..2 {
            let packed = fs::read(output.with_extension(format!("sq4-{panel}.bin"))).unwrap();
            assert_eq!(packed.len(), n * 14);
            assert!(packed.chunks_exact(14).all(|row| row[13] & 0xf0 == 0));
            let codes = packed
                .chunks_exact(14)
                .flat_map(|row| [row[12] & 15, row[12] >> 4, row[13]])
                .collect::<std::collections::BTreeSet<_>>();
            assert!(codes.len() > 4);
        }
        let mut incidental_ranked = false;
        for (index, pin) in freeze["results"].as_array().unwrap().iter().enumerate() {
            let artifact: Artifact = serde_json::from_value(pin.clone()).unwrap();
            let body = fs::read(&artifact.path).unwrap();
            assert_eq!(hash(&body), artifact.sha256);
            let result: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(result["truth_opened"], false);
            assert_eq!(result["plan"]["row_ranges"], json!(expected_ranges));
            assert_eq!(
                result["plan"]["original_row_ranges"],
                json!(original_ranges)
            );
            assert_eq!(result["plan"]["candidate_bytes"], 567 * 14);
            let query = queries[index % 64];
            let length = query
                .iter()
                .map(|&v| f64::from(v) * f64::from(v))
                .sum::<f64>()
                .sqrt();
            let query = query.map(|v| (f64::from(v) / length) as f32);
            let mut shift = 0_f32;
            let mut qnorm = 0_f32;
            for axis in 0..3 {
                shift += query[axis] * low[axis];
                qnorm += query[axis] * query[axis];
            }
            shift -= qnorm / 2.;
            for (key, rows, quantized) in [
                ("sq4", &candidate_rows, true),
                ("sq8_reference", &candidate_rows, false),
                ("original256_baseline", &baseline_rows, false),
            ] {
                let mut expected = Vec::new();
                let mut fetched = Vec::new();
                for &physical in rows {
                    let row = &sources[index / 64][physical * 15..(physical + 1) * 15];
                    let id = i64::from_le_bytes(row[..8].try_into().unwrap());
                    let mut norm = 0_f32;
                    let mut inner = 0_f32;
                    for axis in 0..3 {
                        let original = row[12 + axis];
                        let code = if quantized {
                            (0..=15u8)
                                .min_by_key(|&n| i32::from(original).abs_diff(17 * i32::from(n)))
                                .unwrap()
                                * 17
                        } else {
                            original
                        };
                        let value = low[axis] + f32::from(code) * step[axis];
                        norm += value * value;
                        let weight = query[axis] * step[axis];
                        inner += f32::from(code) * weight;
                    }
                    if !quantized {
                        assert_eq!(
                            norm.to_bits(),
                            u32::from_le_bytes(row[8..12].try_into().unwrap())
                        );
                    }
                    expected.push((norm - 2. * (inner + shift), id, physical));
                    fetched.push(id);
                }
                expected.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                assert_eq!(result[key]["ranked"].as_array().unwrap().len(), 100);
                assert_eq!(result[key]["fetched_ids"], json!(fetched));
                assert_eq!(
                    result[key]["range_reads"],
                    if key == "original256_baseline" {
                        34
                    } else {
                        32
                    }
                );
                assert_eq!(
                    result[key]["verified_bytes"],
                    rows.len() * if quantized { 14 } else { 15 }
                );
                for (actual, expected) in result[key]["ranked"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .zip(expected.iter().take(100))
                {
                    assert_eq!(actual["id"], expected.1);
                    assert_eq!(actual["ordinal"], expected.2);
                    assert_eq!(actual["score_bits"], expected.0.to_bits());
                    incidental_ranked |= key == "sq4"
                        && ((16..32).contains(&expected.2) || (48..64).contains(&expected.2));
                }
            }
            assert_eq!(
                result["sq4"]["fetched_ids"],
                result["sq8_reference"]["fetched_ids"]
            );
            assert_eq!(result["sq8_reference_serving_eligible"], false);
        }
        assert!(incidental_ranked);
        let mut missing = config.clone();
        for panel in &mut missing.panels {
            panel.truth.path = dispersed.join("unopened-missing-truth");
        }
        let output = dispersed.join("missing.json");
        assert!(sq4::diagnose(&missing, &"d".repeat(64), &output).is_err());
        let invalid: Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
        assert_eq!(invalid["status"], "INVALID");
        assert_eq!(invalid["queries"], 128);
        assert_eq!(invalid["details"]["truth_opened"], false);
        let freeze: Value =
            serde_json::from_slice(&fs::read(output.with_extension("sq4-freeze.json")).unwrap())
                .unwrap();
        assert_eq!(freeze["results"].as_array().unwrap().len(), 128);
        assert_eq!(freeze["truth_opened"], false);
    }

    #[test]
    fn fine_pack_strict_cli_real_tiny_pipeline() {
        use borsuk::fine_sq8_groups::{
            FineBuildConfig, FineSq8Index, ResidentLimits, pack_diagnostic as pack,
        };
        fn tiny_action(path: &Path, sha: &str, output: &Path) -> Result<()> {
            let a = Artifact {
                path: path.into(),
                bytes: fs::metadata(path)?.len() as usize,
                sha256: sha.into(),
            };
            let config: pack::Config =
                serde_json::from_slice(&read_source_probe_artifact(&a, CONFIG_CAP)?)?;
            pack::diagnose(&config, sha, output)
        }
        let temp = tempfile::tempdir().unwrap();
        let primary = tiny_fine_primary(temp.path());
        let build_config = FineBuildConfig {
            schema: borsuk::fine_sq8_groups::BUILD_SCHEMA.into(),
            primary_root: primary,
            max_build_payload_bytes: 128 * 1024 * 1024,
            max_output_bytes: 16 * 1024 * 1024,
        };
        let limits = ResidentLimits {
            max_peak_payload_bytes: 256 * 1024 * 1024,
            pinned_generation_bytes: 0,
            active_queries: 1,
            delta_bytes: 0,
            maintenance_bytes: 0,
            runtime_bytes: 0,
        };
        let roots = ["relaion", "cohere"]
            .map(|name| FineSq8Index::build(&build_config, &temp.path().join(name)).unwrap());
        let indexes = roots
            .iter()
            .map(|root| FineSq8Index::open(root, &limits).unwrap())
            .collect::<Vec<_>>();
        let mut prefix = Vec::new();
        for (i, index) in indexes.iter().enumerate() {
            prefix.extend(serde_json::to_vec(&json!({"phase":"startup","dataset":(["relaion","cohere"][i]),"root":roots[i],
                "resources":index.resources,"build":index.build_receipt(),"truth_opened":false,"wall_ns":1,"process_cpu_ns":1})).unwrap());
            prefix.push(b'\n');
        }
        for (i, index) in indexes.iter().enumerate() {
            let mut workspace = index.new_workspace().unwrap();
            for ordinal in 0..64 {
                let query = [1.0, ordinal as f32 / 64.0];
                let plan = index.plan(&query, &mut workspace).unwrap();
                prefix.extend(serde_json::to_vec(&json!({"phase":"fine_plan","dataset":(["relaion","cohere"][i]),"ordinal":ordinal,
                    "plan":plan,"truth_opened":false,"wall_ns":1,"process_cpu_ns":1})).unwrap());
                prefix.push(b'\n');
            }
        }
        drop(indexes);
        let prefix_pin = probe_artifact(&temp.path().join("prefix.jsonl"), &prefix);
        OpenOptions::new()
            .append(true)
            .open(&prefix_pin.path)
            .unwrap()
            .write_all(b"UNOPENED REMAINING TRACE")
            .unwrap();
        let requests = Artifact {
            path: temp.path().join("nonexistent-requests"),
            bytes: 1,
            sha256: hash(b"unopened"),
        };
        let seal=probe_artifact(&temp.path().join("seal.json"),&serde_json::to_vec(&json!({
            "schema":"borsuk-fine-sq8-seal-v1","config_sha256":hash(b"paired config"),"source_identity_sha256":hash(b"paired source"),
            "prefix_bytes":prefix_pin.bytes,"prefix_sha256":prefix_pin.sha256,"plans_per_panel":64,"truth_opened":false,
            "panels":roots.iter().enumerate().map(|(i,root)|json!({"dataset":(["relaion","cohere"][i]),"root":root,"requests":requests})).collect::<Vec<_>>() })).unwrap());
        let panels=roots.iter().enumerate().map(|(i,root)|{
            let manifest:Value=serde_json::from_slice(&fs::read(&root.path).unwrap()).unwrap();
            // Actual built objects are deliberately absent during the diagnostic.
            for field in ["pq","records","groups","order"] {fs::remove_file(manifest[field]["path"].as_str().unwrap()).unwrap();}
            json!({"dataset":(["relaion","cohere"][i]),"root":root,"graph":manifest["graph"],"identity":manifest["identity"]})
        }).collect::<Vec<_>>();
        let config = json!({"schema":pack::CONFIG_SCHEMA,"panels":panels,"original_seal":seal,"prefix":prefix_pin,
            "caps":{"memory_bytes":268435456,"output_bytes":2097152,"deadline_seconds":300,"operations":500000000,"cpu_threads":1,"swap_bytes":0}});
        let pin = probe_artifact(
            &temp.path().join("pack-config.json"),
            &serde_json::to_vec(&config).unwrap(),
        );
        let out = temp.path().join("pack.json");
        let mut args = vec![
            "hierarchical_semantic_cells".into(),
            "check-fine-pack".into(),
            pin.path.display().to_string(),
            pin.sha256.clone(),
            out.display().to_string(),
        ];
        // Same strict argument dispatcher, using only a private action seam for
        // tiny geometry. The production action rejects this unfrozen fixture.
        execute_args_with_pack(&args, tiny_action).unwrap();
        let result: Value = serde_json::from_slice(&fs::read(&out).unwrap()).unwrap();
        assert_eq!(result["status"], "SURVIVED_NECESSARY_LOCALITY");
        assert_eq!(result["queries"], 128);
        assert_eq!(
            result["diagnostic_source_sha256"]["bin/hierarchical_semantic_cells.rs"],
            hash(include_bytes!("hierarchical_semantic_cells.rs"))
        );
        assert_eq!(
            result["details"]["original_trace_source_identity_sha256"],
            hash(b"paired source")
        );
        assert_eq!(
            fs::read(out.with_extension("prefix.jsonl")).unwrap(),
            prefix
        );
        assert!(execute_args_with_pack(&args, tiny_action).is_err());
        args[4] = temp
            .path()
            .join("production-reject.json")
            .display()
            .to_string();
        assert!(execute_args(&args).is_err());
        args[4] = temp.path().join("bad-sha.json").display().to_string();
        args[3] = "0".repeat(64);
        assert!(execute_args_with_pack(&args, tiny_action).is_err());
        for forbidden in [
            "truth",
            "requests",
            "query",
            "test_geometry",
            "groups_per_pack",
        ] {
            let mut bad = config.clone();
            bad[forbidden] = json!("forbidden");
            let pin = probe_artifact(
                &temp.path().join(format!("config-{forbidden}")),
                &serde_json::to_vec(&bad).unwrap(),
            );
            args[2] = pin.path.display().to_string();
            args[3] = pin.sha256;
            args[4] = temp
                .path()
                .join(format!("out-{forbidden}"))
                .display()
                .to_string();
            assert!(execute_args_with_pack(&args, tiny_action).is_err());
        }
        assert!(execute_args_with_pack(&args[..4], tiny_action).is_err());
    }
    fn tiny_fine_primary(dir: &Path) -> Artifact {
        let codec = borsuk::rotated_two_bit::RotatedTwoBitCodec::new(&[0., 0.], 20260923).unwrap();
        let mut canonical = Vec::new(); let mut records = Vec::new(); let mut sq8 = Vec::new(); let mut order = Vec::new();
        for id in (0..135_i64).rev() {
            let vector = if id % 2 == 0 { [1_f32, 0.] } else { [0_f32, 1.] };
            canonical.extend_from_slice(&id.to_le_bytes());
            for value in vector { canonical.extend_from_slice(&value.to_le_bytes()); }
            records.extend_from_slice(&codec.encode(&vector).unwrap());
            sq8.extend_from_slice(&id.to_le_bytes()); sq8.extend_from_slice(&1_f32.to_le_bytes());
            sq8.extend(vector.map(|value| value as u8)); order.extend_from_slice(&(id as u64).to_le_bytes());
        }
        let canonical = probe_artifact(&dir.join("pair-canonical"), &canonical);
        let records = probe_artifact(&dir.join("pair-codes"), &records);
        let sq8 = probe_artifact(&dir.join("pair-sq8"), &sq8);
        let order = probe_artifact(&dir.join("pair-order"), &order);
        let mean = probe_artifact(&dir.join("pair-mean"), &[0; 8]);
        let plane = probe_artifact(&dir.join("pair-plane.json"), &serde_json::to_vec(&borsuk::two_bit_source::SourcePlaneReceipt {
            schema:"borsuk-two-bit-plane-v3".into(), rows:135, dimensions:2, seed:20260923, record_bytes:codec.record_bytes(),
            source_sha256:"0".repeat(64), sq8_sha256:sq8.sha256.clone(), source_order_sha256:order.sha256.clone(),
            mean_sha256:mean.sha256.clone(), records_sha256:records.sha256.clone(), page_rows:32,
            page_digest_sha256:"0".repeat(64), query_or_truth_used:false,
        }).unwrap());
        let generation = probe_artifact(&dir.join("pair-generation.json"), &serde_json::to_vec(&json!({
            "schema":"borsuk-two-bit-generation-v8","generation":1,"base_epoch":0,
            "plane_manifest_sha256":plane.sha256,"page_manifest_sha256":"0".repeat(64),
            "discovery":{"mode":"graph","centroids_sha256":"0".repeat(64),"graph_sha256":"0".repeat(64),
                "graph_resident_bytes":1,"diverse_graph_sha256":"0".repeat(64),"diverse_graph_resident_bytes":1},
            "sq8_object_sha256":sq8.sha256,"sq8_object_key":format!("objects/{}",sq8.sha256),"sq8_etag":"fixture",
            "canonical":{"rows":135,"dimensions":2,"bytes":canonical.bytes,"sha256":canonical.sha256,
                "object_key":format!("objects/{}",canonical.sha256)},"low":[0.,0.],"step":[1.,1.]
        })).unwrap());
        let original = BuildConfig { schema:borsuk::hierarchical_semantic_cells::BUILD_SCHEMA.into(),
            generation, plane, canonical, order, records, mean, sq8, cell_rows:32, sample_rows:32, max_depth:24,
            max_build_payload_bytes:64 * 1024 * 1024, max_output_bytes:16 * 1024 * 1024 };
        let primary = dir.join("pair-primary");
        let receipt = build(&original, &primary).unwrap();
        let root = Artifact { path:primary.join("manifest.json"), bytes:fs::metadata(primary.join("manifest.json")).unwrap().len() as usize,
            sha256:receipt.root_sha256 };
        root
    }
    #[test]
    fn fine_real_source_pipeline_seals_both_panels_before_gt_and_is_durable() {
        use borsuk::fine_sq8_groups::{FineSq8Index,FineBuildConfig,ResidentLimits};
        let temp=tempfile::tempdir().unwrap(); let primary=tiny_fine_primary(temp.path());
        let cfg=FineBuildConfig {schema:borsuk::fine_sq8_groups::BUILD_SCHEMA.into(),primary_root:primary,
            max_build_payload_bytes:128*1024*1024,max_output_bytes:16*1024*1024};
        let a=FineSq8Index::build(&cfg,&temp.path().join("fine-a")).unwrap();
        let b=FineSq8Index::build(&cfg,&temp.path().join("fine-b")).unwrap();
        assert!(FineSq8Index::build(&cfg,&temp.path().join("fine-a")).is_err());
        let primary_manifest:Value=serde_json::from_slice(&fs::read(&cfg.primary_root.path).unwrap()).unwrap();
        let retained=Prototype::open_for_source_probes(&cfg.primary_root,128*1024*1024).unwrap();
        let mut expected=Vec::new();
        for cell in 0..primary_manifest["build"]["cells"].as_u64().unwrap() as usize {expected.extend(retained.primary_cell_sq8(cell).unwrap());}
        assert_eq!(fs::read(a.path.parent().unwrap().join("records.bin")).unwrap(),expected);
        drop(retained);
        for field in ["generation","plane","canonical","order","records","mean","sq8"] {
            fs::remove_file(primary_manifest["input"][field]["path"].as_str().unwrap()).unwrap();
        }
        fs::remove_dir_all(cfg.primary_root.path.parent().unwrap()).unwrap();
        let assert_fresh = |fresh: std::process::Output| {
            let stdout=String::from_utf8_lossy(&fresh.stdout);
            assert!(fresh.status.success()
                && stdout.lines().filter(|line| line.starts_with("test ") && line.ends_with(" ... ok"))
                    .eq(std::iter::once("test tests::fine_fresh_process_plane_free_open ... ok"))
                && stdout.lines().any(|line| line.starts_with("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;")),
                "fresh child status={}\nstdout:\n{}\nstderr:\n{}",fresh.status,stdout,String::from_utf8_lossy(&fresh.stderr));
        };
        let fresh=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","tests::fine_fresh_process_plane_free_open","--color","never"])
            .env("BORSUK_FINE_TEST_ROOT",serde_json::to_string(&a).unwrap()).output().unwrap();
        assert_fresh(fresh);
        let requests=(0..64).map(|ordinal| format!("{}\n",json!({"ordinal":ordinal,"query":[3.125,0.875]}))).collect::<String>();
        let requests=probe_artifact(&temp.path().join("fine-requests"),requests.as_bytes());
        let truth_body=(0..64).flat_map(|_|(0..100u32).flat_map(u32::to_le_bytes)).collect::<Vec<_>>();
        let valid=probe_artifact(&temp.path().join("fine-truth"),&truth_body);
        let missing=Artifact {path:temp.path().join("absent-truth"),..valid.clone()};
        let config=|truth:Artifact| FinePairedConfig {schema:"borsuk-fine-sq8-paired-v1".into(),
            panels:[FinePanel {dataset:"relaion".into(),root:a.clone(),requests:requests.clone(),truth:truth.clone(),truth_width:100},
                FinePanel {dataset:"cohere".into(),root:b.clone(),requests:requests.clone(),truth,truth_width:100}],
            source_identity_sha256:fine_source_identity(), limits:ResidentLimits {max_peak_payload_bytes:256*1024*1024,
                pinned_generation_bytes:0,active_queries:1,delta_bytes:0,maintenance_bytes:0,runtime_bytes:0},
            max_evaluator_payload_bytes:128*1024*1024,max_result_bytes:16*1024*1024,test_geometry:Some((135,2)),fail_sync_at:None};
        for (case,truth) in [("missing",missing.clone()),("valid",valid.clone())] {
            let out=temp.path().join(format!("fine-{case}.jsonl"));
            let result=paired_fine(config(truth),&"a".repeat(64),&out);
            if case=="valid" { result.unwrap(); } else { assert!(result.is_err()); }
            let body=fs::read(&out).unwrap();
            let rows=std::str::from_utf8(&body).unwrap().lines().map(|line|serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
            let seal=rows.iter().position(|r|r["phase"]=="fine_seal").unwrap();
            assert_eq!(rows[..seal].iter().filter(|r|r["phase"]=="fine_plan").count(),128);
            assert!(rows[..=seal].iter().all(|r|r["truth_opened"]==false));
            let frozen:Value=serde_json::from_slice(&fs::read(out.with_extension("fine-seal.json")).unwrap()).unwrap();
            assert_eq!(frozen["prefix_sha256"],hash(&body[..frozen["prefix_bytes"].as_u64().unwrap() as usize]));
            assert_eq!(rows.iter().filter(|r|r["phase"]=="fine_scored").count(),128);
            if case=="missing" { assert_eq!(rows.last().unwrap()["status"],"INVALID"); }
            else { assert_eq!(rows.last().unwrap()["complete"],true); }
            assert!(paired_fine(config(valid.clone()),&"a".repeat(64),&out).is_err());
            assert_eq!(fs::read(&out).unwrap(),body);
        }
        for case in ["cap","sync","geometry","aggregate","unknown-fields"] {
            let mut cfg=config(missing.clone()); let out=temp.path().join(format!("fine-{case}.jsonl"));
            match case {
                "cap"=>cfg.max_result_bytes=TERMINAL_CAP+100,
                "sync"=>cfg.fail_sync_at=Some(1),
                "geometry"=>cfg.test_geometry=None,
                "aggregate"=>cfg.limits.max_peak_payload_bytes=1024,
                _=> { let body=br#"{"schema":"borsuk-fine-sq8-paired-v1","extra":true}"#;
                    assert!(probe_config::<FinePairedConfig>(body,&"a".repeat(64),&out).is_err());
                    assert_eq!(probe_terminal(&out)["status"],"INVALID"); continue; }
            }
            assert!(paired_fine(cfg,&"a".repeat(64),&out).is_err());
            assert_eq!(probe_terminal(&out)["status"],"INVALID");
            assert!(!out.with_extension("fine-seal.json").exists());
        }
        fs::remove_file(a.path.parent().unwrap().join("records.bin")).unwrap();
        let fresh=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","tests::fine_fresh_process_plane_free_open","--color","never"])
            .env("BORSUK_FINE_TEST_ROOT",serde_json::to_string(&a).unwrap())
            .env("BORSUK_FINE_TEST_REMOTE","1").output().unwrap();
        assert_fresh(fresh);
    }
    #[test]
    fn fine_fresh_process_plane_free_open() {
        let Ok(encoded)=std::env::var("BORSUK_FINE_TEST_ROOT") else {return;};
        let root:Artifact=serde_json::from_str(&encoded).unwrap();
        let limits=borsuk::fine_sq8_groups::ResidentLimits {max_peak_payload_bytes:256*1024*1024,
            pinned_generation_bytes:0,active_queries:1,delta_bytes:0,maintenance_bytes:0,runtime_bytes:0};
        if std::env::var_os("BORSUK_FINE_TEST_REMOTE").is_some() {
            use borsuk::fine_sq8_groups::{FineSq8Index,FineSq8Snapshot};
            let manifest:Value=serde_json::from_slice(&fs::read(&root.path).unwrap()).unwrap();
            assert!(!Path::new(manifest["primary_root"]["path"].as_str().unwrap()).exists());
            for field in ["generation","plane","canonical","order","records","mean","sq8"] {
                assert!(!Path::new(manifest["original"][field]["path"].as_str().unwrap()).exists());
            }
            assert!(!root.path.parent().unwrap().join("records.bin").exists());
            assert!(FineSq8Index::open(&root,&limits).is_err());
            let index=std::sync::Arc::new(FineSq8Index::open_remote(&root,&limits).unwrap());
            let snapshot=FineSq8Snapshot::new(index.clone());
            let query=[3.125,0.875]; let plan=snapshot.plan(&query,&mut index.new_workspace().unwrap()).unwrap();
            assert!(plan.feasible() && !plan.nominees().is_empty() && !plan.ranges().is_empty());
            let failure=snapshot.search(&plan,&query,100).unwrap_err();
            assert_eq!(failure.error,"fine local record provider unavailable");
            assert_eq!(failure.accounting.attempted_gets,0);
            assert_eq!(failure.accounting.attempted_bytes,Some(0));
            assert_eq!(failure.accounting.read_bytes,Some(0));
            assert_eq!(failure.accounting.verified_bytes,0);
            return;
        }
        let index=borsuk::fine_sq8_groups::FineSq8Index::open(&root,&limits).unwrap();
        let query=[3.125,0.875]; let plan=index.plan(&query,&mut index.new_workspace().unwrap()).unwrap();
        assert_eq!(index.search(&plan,&query,100).unwrap().ranked.len(),100);
    }

    #[test]
    fn overlap_pair_seal_precedes_truth() {
        let dir = tempfile::tempdir().unwrap();
        let (control_root, candidate_root) = tiny_overlap_pair(dir.path());
        let requests = (0..64).map(|ordinal| format!("{}\n", json!({"ordinal":ordinal,"query":[3.125,0.875]}))).collect::<String>();
        let request = probe_artifact(&dir.path().join("pair-requests"), requests.as_bytes());
        let truth_body = (0..64).flat_map(|_| (0..100_u32).flat_map(u32::to_le_bytes)).collect::<Vec<_>>();
        let valid_truth = probe_artifact(&dir.path().join("valid-truth"), &truth_body);
        let truth = probe_artifact(&dir.path().join("tampered-truth"), &truth_body);
        let mut corrupt_truth = truth_body; corrupt_truth[0] ^= 1;
        fs::write(&truth.path, corrupt_truth).unwrap();
        let absent = Artifact { path: dir.path().join("absent-truth"), bytes: truth.bytes, sha256: truth.sha256.clone() };
        let config = |request: Artifact, truth: Artifact| OverlapPairedConfig {
            schema:"borsuk-cell-overlap-paired-v1".into(), dataset:"relaion".into(),
            control_root:control_root.clone(), candidate_root:candidate_root.clone(), requests:request,
            first:0, count:64, truth, truth_width:100, source_identity_sha256:overlap_source_identity(),
            max_resident_payload_bytes:128 * 1024 * 1024, max_evaluator_payload_bytes:256 * 1024 * 1024,
            max_result_bytes:16 * 1024 * 1024,
            resources:OverlapResources { build_workers:1, build_memory_bytes:8 * 1024 * 1024 * 1024,
                build_swap_bytes:0, build_scratch_bytes:8 * 1024 * 1024 * 1024, build_timeout_seconds:1200,
                query_workers:1, query_memory_bytes:512 * 1024 * 1024, query_swap_bytes:0, query_timeout_seconds:300 },
            test_seam:Some(OverlapPairTestSeam { rows:128, dimensions:2, evaluator_roster_bytes:128 * 32 * 640 * 64,
                query_scratch_bytes:3 * overlap::MAX_BYTES + 128 * (2 + 12 + 512) + 8 * 32 * (2 * 4 + 4096),
                selection_mismatch_at:None, fail_sync_at:None, corrupt_frame_arm:None }),
        };
        for (case, gt) in [("missing-gt", absent.clone()), ("tampered-gt", truth), ("valid-gt", valid_truth)] {
            let output = dir.path().join(format!("{case}.jsonl"));
            let cfg = config(request.clone(), gt);
            assert!(cfg.max_evaluator_payload_bytes > 128 * 1024 * 1024);
            assert!(cfg.test_seam.unwrap().evaluator_roster_bytes > 128 * 1024 * 1024);
            assert_eq!(cfg.truth.bytes, 25600);
            let result = paired_overlap(cfg, &"b".repeat(64), &output);
            if case == "valid-gt" { result.unwrap(); } else {
                let error = result.unwrap_err();
                if case == "tampered-gt" { assert!(error.to_string().contains("probe artifact SHA256/EOF")); }
            }
            let body = fs::read(&output).unwrap();
            let rows = std::str::from_utf8(&body).unwrap().lines().map(|line| serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
            let selections = rows.iter().enumerate().filter(|(_, row)| row["phase"] == "paired_selection").collect::<Vec<_>>();
            let rosters = rows.iter().enumerate().filter(|(_, row)| row["phase"] == "paired_scored_roster").collect::<Vec<_>>();
            assert_eq!(selections.len(), 64); assert_eq!(rosters.len(), 64);
            assert!(selections[63].0 < rosters[0].0);
            for (ordinal, (_, row)) in selections.iter().enumerate() {
                assert_eq!(row["ordinal"], ordinal);
                assert!(!row["control"]["selected"].as_array().unwrap().is_empty());
                assert_eq!(row["control"]["selected"], row["candidate"]["selected"]);
            }
            for (ordinal, (_, row)) in rosters.iter().enumerate() {
                assert_eq!(row["ordinal"], ordinal);
                for arm in ["control", "candidate"] {
                    assert_eq!(row[arm]["base_ids"].as_array().unwrap().len(), 128);
                    assert_eq!(row[arm]["ranked"].as_array().unwrap().len(), 128);
                    assert!(row[arm]["accounting"]["payload"]["verified_bytes"].as_u64().unwrap() > 0);
                }
            }
            let seal_index = rows.iter().position(|row| row["phase"] == "paired_seal").unwrap();
            assert!(rosters[63].0 < seal_index); assert_eq!(rows[seal_index]["truth_opened"], false);
            let seal_path = output.with_extension("paired-seal.json");
            let sealed = fs::read(&seal_path).unwrap();
            let identity: Value = serde_json::from_slice(&sealed).unwrap();
            assert_eq!(identity["selections_per_arm"], 64);
            assert_eq!(identity["complete_unique_scored_rosters_per_arm"], 64);
            let prefix = identity["prefix_bytes"].as_u64().unwrap() as usize;
            assert_eq!(identity["prefix_sha256"], hash(&body[..prefix]));
            let prefix_rows = std::str::from_utf8(&body[..prefix]).unwrap().lines().map(|line| serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
            assert_eq!(prefix_rows.iter().filter(|row| row["phase"] == "paired_scored_roster").count(), 64);
            if case == "valid-gt" {
                assert_eq!(rows.last().unwrap()["complete"], true);
                assert!([json!("FAIL"), json!("PANEL_PASS")].contains(&rows.last().unwrap()["status"]));
                assert_eq!(rows.iter().filter(|row| row["phase"] == "paired_metrics").count(), 64);
            } else { assert_eq!(rows.last().unwrap()["status"], "INVALID"); }
            assert!(paired_overlap(config(request.clone(), absent.clone()), &"b".repeat(64), &output).is_err());
            assert_eq!(fs::read(&output).unwrap(), body); assert_eq!(fs::read(&seal_path).unwrap(), sealed);
        }
        let short_body = requests.lines().take(63).map(|line| format!("{line}\n")).collect::<String>();
        let short = probe_artifact(&dir.path().join("short-requests"), short_body.as_bytes());
        let control_frames = fs::read(control_root.path.parent().unwrap().join("sq8-cells.bin")).unwrap();
        let candidate_frames = fs::read(candidate_root.path.parent().unwrap().join("sq8-cells.bin")).unwrap();
        for case in ["incomplete", "incomplete-rosters", "selection-mismatch", "cap", "sync", "runtime-geometry", "count", "first", "resources", "candidate-frame", "control-frame"] {
            let mut cfg = config(if case == "incomplete" { short.clone() } else { request.clone() }, absent.clone());
            match case {
                "selection-mismatch" => cfg.test_seam.as_mut().unwrap().selection_mismatch_at = Some(31),
                "incomplete-rosters" => cfg.max_result_bytes = 512 * 1024,
                "cap" => cfg.max_result_bytes = TERMINAL_CAP + 4096,
                "sync" => cfg.test_seam.as_mut().unwrap().fail_sync_at = Some(3),
                "runtime-geometry" => { cfg.test_seam = None; cfg.max_evaluator_payload_bytes = 512 * 1024 * 1024; },
                "count" => cfg.count = 63,
                "first" => cfg.first = 1,
                "resources" => cfg.resources.query_memory_bytes = 128 * 1024 * 1024,
                "candidate-frame" => cfg.test_seam.as_mut().unwrap().corrupt_frame_arm = Some("candidate"),
                "control-frame" => cfg.test_seam.as_mut().unwrap().corrupt_frame_arm = Some("control"),
                _ => {},
            }
            let output = dir.path().join(format!("{case}.jsonl"));
            let error = paired_overlap(cfg, &"b".repeat(64), &output).unwrap_err();
            let body = fs::read(&output).unwrap();
            let rows = std::str::from_utf8(&body).unwrap().lines().map(|line| serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
            assert_eq!(rows.last().unwrap()["status"], "INVALID");
            assert!(!output.with_extension("paired-seal.json").exists());
            if case == "selection-mismatch" {
                assert!(error.to_string().contains("cell IDs/primary flags/distance bits mismatch"));
                assert!(!rows.iter().any(|row| row["phase"] == "paired_scored_roster"));
            }
            if case == "incomplete-rosters" {
                assert!(error.to_string().contains("diagnostic output byte cap"));
                assert_eq!(rows.iter().filter(|row| row["phase"] == "paired_selection").count(), 64);
                let scored = rows.iter().filter(|row| row["phase"] == "paired_scored_roster").count();
                assert!(scored > 0 && scored < 64);
            }
            if case == "runtime-geometry" { assert!(error.to_string().contains("matched retained capacity-v4 layouts")); }
            if case.ends_with("-frame") {
                let failure = rows.iter().position(|row| row["phase"] == "paired_arm_failure").unwrap();
                assert!(failure < rows.len() - 1);
                let receipt = &rows[failure]["receipt"];
                assert_eq!(receipt, &rows.last().unwrap()["failed_query_receipt"]);
                assert_eq!(receipt["ordinal"], 0);
                let arm = if case == "candidate-frame" { "candidate" } else { "control" };
                assert_eq!(receipt["failed_arm"], arm);
                assert_eq!(receipt[arm]["status"], "failed");
                assert_eq!(receipt[arm]["accounting"]["payload"]["submitted_gets"], 1);
                assert_eq!(receipt[arm]["accounting"]["payload"]["failed_gets"], 1);
                assert_eq!(receipt[arm]["accounting"]["payload"]["verified_bytes"], 0);
                assert!(receipt[arm]["accounting"]["payload"]["requested_bytes"].as_u64().unwrap() > 0);
                if arm == "candidate" {
                    assert_eq!(receipt["control"]["status"], "completed");
                    assert!(receipt["control"]["accounting"]["payload"]["verified_bytes"].as_u64().unwrap() > 0);
                    assert_eq!(receipt["control"]["accounting"]["payload"]["failed_gets"], 0);
                    assert_eq!(receipt["control"]["accounting"]["visible_unique_rows"], 128);
                } else {
                    assert_eq!(receipt["candidate"]["status"], "not_attempted");
                    assert!(receipt["candidate"].get("accounting").is_none());
                }
                fs::write(control_root.path.parent().unwrap().join("sq8-cells.bin"), &control_frames).unwrap();
                fs::write(candidate_root.path.parent().unwrap().join("sq8-cells.bin"), &candidate_frames).unwrap();
            }
            if case == "sync" {
                assert!(error.to_string().contains("paired fixture sync failure"));
                assert_eq!(rows.iter().filter(|row| row["phase"] == "paired_selection").count(), 64);
                assert_eq!(rows.iter().filter(|row| row["phase"] == "paired_scored_roster").count(), 64);
            }
            if ["incomplete-rosters", "sync"].contains(&case) {
                let scored = rows.iter().filter(|row| row["phase"] == "paired_scored_roster").count();
                let ordinal = if case == "sync" { 63 } else { scored };
                let receipt = &rows.last().unwrap()["failed_query_receipt"];
                assert_eq!(receipt["ordinal"], ordinal);
                assert!(receipt.get("failed_arm").is_none());
                for arm in ["control", "candidate"] {
                    assert_eq!(receipt[arm]["status"], "completed");
                    let account = &receipt[arm]["accounting"];
                    assert!(account["payload"]["submitted_gets"].as_u64().unwrap() > 0);
                    assert!(account["payload"]["requested_bytes"].as_u64().unwrap() > 0);
                    assert!(account["payload"]["verified_bytes"].as_u64().unwrap() > 0);
                    assert_eq!(account["payload"]["failed_gets"], 0);
                    assert_eq!(account["visible_unique_rows"], 128);
                }
                if case == "incomplete-rosters" {
                    assert!(!rows.iter().any(|row| row["phase"] == "paired_scored_roster" && row["ordinal"] == ordinal));
                }
            }
            assert!(paired_overlap(config(request.clone(), absent.clone()), &"b".repeat(64), &output).is_err());
            assert_eq!(fs::read(&output).unwrap(), body);
        }
        let output = dir.path().join("existing-seal.jsonl");
        let seal = output.with_extension("paired-seal.json");
        fs::write(&seal, b"existing immutable seal").unwrap();
        assert!(paired_overlap(config(request, absent), &"b".repeat(64), &output).is_err());
        assert_eq!(fs::read(seal).unwrap(), b"existing immutable seal");
        // Both fixed denominator and the exact p05 index remain observable on
        // underfill. Sorting index2 or dividing by returned length changes this.
        let mut hits = vec![(100, 100); 64];
        hits[..4].copy_from_slice(&[(0, 0), (10, 10), (20, 20), (30, 30)]);
        let summary = overlap_summary(&hits);
        assert_eq!(summary["coverage_p05_hits"], 30);
        assert_eq!(summary["recall_mean"], 6060_f64 / 6400.);
    }
    fn tiny_overlap_pair(dir: &Path) -> (Artifact, Artifact) {
        let codec = borsuk::rotated_two_bit::RotatedTwoBitCodec::new(&[0., 0.], 20260923).unwrap();
        let mut canonical = Vec::new(); let mut records = Vec::new(); let mut sq8 = Vec::new(); let mut order = Vec::new();
        for id in (0..128_i64).rev() {
            let vector = if id % 2 == 0 { [1_f32, 0.] } else { [0_f32, 1.] };
            canonical.extend_from_slice(&id.to_le_bytes());
            for value in vector { canonical.extend_from_slice(&value.to_le_bytes()); }
            records.extend_from_slice(&codec.encode(&vector).unwrap());
            sq8.extend_from_slice(&id.to_le_bytes()); sq8.extend_from_slice(&1_f32.to_le_bytes());
            sq8.extend(vector.map(|value| value as u8)); order.extend_from_slice(&(id as u64).to_le_bytes());
        }
        let canonical = probe_artifact(&dir.join("pair-canonical"), &canonical);
        let records = probe_artifact(&dir.join("pair-codes"), &records);
        let sq8 = probe_artifact(&dir.join("pair-sq8"), &sq8);
        let order = probe_artifact(&dir.join("pair-order"), &order);
        let mean = probe_artifact(&dir.join("pair-mean"), &[0; 8]);
        let plane = probe_artifact(&dir.join("pair-plane.json"), &serde_json::to_vec(&borsuk::two_bit_source::SourcePlaneReceipt {
            schema:"borsuk-two-bit-plane-v3".into(), rows:128, dimensions:2, seed:20260923, record_bytes:codec.record_bytes(),
            source_sha256:"0".repeat(64), sq8_sha256:sq8.sha256.clone(), source_order_sha256:order.sha256.clone(),
            mean_sha256:mean.sha256.clone(), records_sha256:records.sha256.clone(), page_rows:32,
            page_digest_sha256:"0".repeat(64), query_or_truth_used:false,
        }).unwrap());
        let generation = probe_artifact(&dir.join("pair-generation.json"), &serde_json::to_vec(&json!({
            "schema":"borsuk-two-bit-generation-v8","generation":1,"base_epoch":0,
            "plane_manifest_sha256":plane.sha256,"page_manifest_sha256":"0".repeat(64),
            "discovery":{"mode":"graph","centroids_sha256":"0".repeat(64),"graph_sha256":"0".repeat(64),
                "graph_resident_bytes":1,"diverse_graph_sha256":"0".repeat(64),"diverse_graph_resident_bytes":1},
            "sq8_object_sha256":sq8.sha256,"sq8_object_key":format!("objects/{}",sq8.sha256),"sq8_etag":"fixture",
            "canonical":{"rows":128,"dimensions":2,"bytes":canonical.bytes,"sha256":canonical.sha256,
                "object_key":format!("objects/{}",canonical.sha256)},"low":[0.,0.],"step":[1.,1.]
        })).unwrap());
        let original = BuildConfig { schema:borsuk::hierarchical_semantic_cells::BUILD_SCHEMA.into(),
            generation, plane, canonical, order, records, mean, sq8, cell_rows:32, sample_rows:32, max_depth:24,
            max_build_payload_bytes:64 * 1024 * 1024, max_output_bytes:16 * 1024 * 1024 };
        let primary = dir.join("pair-primary");
        let receipt = build(&original, &primary).unwrap();
        let root = Artifact { path:primary.join("manifest.json"), bytes:fs::metadata(primary.join("manifest.json")).unwrap().len() as usize,
            sha256:receipt.root_sha256 };
        let arm = |name: &str, enabled| {
            let output = dir.join(name);
            let config = OverlapBuildConfig { schema:overlap::BUILD_SCHEMA.into(), retained_root:root.clone(), original:original.clone(),
                overlap:enabled, max_resident_payload_bytes:128 * 1024 * 1024,
                max_build_payload_bytes:128 * 1024 * 1024, max_output_bytes:16 * 1024 * 1024 };
            let receipt = overlap::build_overlap(&config, &output).unwrap();
            if enabled { assert!(receipt.admitted > 0); }
            Artifact { path:output.join("manifest.json"), bytes:fs::metadata(output.join("manifest.json")).unwrap().len() as usize,
                sha256:receipt.root_sha256 }
        };
        (arm("pair-control", false), arm("pair-candidate", true))
    }
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
