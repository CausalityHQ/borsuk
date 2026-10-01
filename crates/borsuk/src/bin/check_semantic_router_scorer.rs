//! Frozen local/logical-I/O scale scorer through the current production generation.
use borsuk::{
    semantic_unit_router::SemanticProfile,
    two_bit_generation::{
        DiscoveryMode, TwoBitGeneration, TwoBitGenerationLimits, TwoBitPlanTrace,
        TwoBitSearchResult,
    },
};
use object_store::{local::LocalFileSystem, path::Path as ObjectPath};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    error::Error,
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::Instant,
};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const CONFIG_CAP: usize = 65536;
const PANEL: usize = 64;
const EVALUATOR_CHARGE: u64 = 32 * 1024 * 1024;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    path: PathBuf,
    bytes: usize,
    sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    dataset: String,
    profile: SemanticProfile,
    rows: usize,
    dimensions: usize,
    first: usize,
    count: usize,
    store_root: PathBuf,
    generation_prefix: String,
    generation_root_sha256: String,
    scratch_parent: PathBuf,
    requests: Artifact,
    truth: Artifact,
    order: Artifact,
    max_memory_bytes: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    ordinal: usize,
    query: Vec<f32>,
}
fn require(ok: bool, message: &str) -> Result<()> {
    if !ok {
        return Err(message.into());
    }
    Ok(())
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
fn regular(path: &Path, bytes: usize, cap: usize) -> Result<File> {
    require(bytes > 0 && bytes <= cap, "artifact byte cap")?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::NOFOLLOW).bits() as i32)
        .open(path)?;
    let metadata = file.metadata()?;
    require(
        metadata.is_file() && metadata.len() == bytes as u64,
        "regular artifact/exact length",
    )?;
    Ok(file)
}
fn checked(a: &Artifact, cap: usize) -> Result<Vec<u8>> {
    require(valid_sha(&a.sha256), "artifact SHA")?;
    let mut file = regular(&a.path, a.bytes, cap)?;
    let mut body = vec![0; a.bytes];
    file.read_exact(&mut body)?;
    require(
        file.read(&mut [0])? == 0 && hash(&body) == a.sha256,
        "artifact EOF/identity",
    )?;
    Ok(body)
}
fn validate_config(c: &Config) -> Result<()> {
    require(
        c.schema == "borsuk-semantic-router-scorer-config-v2"
            && matches!(c.dataset.as_str(), "ReLAION" | "CoHere")
            && c.profile.valid_geometry(c.rows, c.dimensions)
            && c.count == PANEL
            && c.first.checked_add(c.count).is_some()
            && c.store_root.is_absolute()
            && c.scratch_parent.is_absolute()
            && valid_sha(&c.generation_root_sha256)
            && c.max_memory_bytes == 512 * 1024 * 1024,
        "frozen scale config/profile/panel",
    )?;
    require(
        !c.generation_prefix.is_empty()
            && c.generation_prefix.split('/').all(|part| {
                !part.is_empty()
                    && part != "."
                    && part != ".."
                    && part
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
            }),
        "generation prefix",
    )?;
    let mut paths = BTreeSet::new();
    for (a, cap) in [
        (&c.requests, 2 * 1024 * 1024),
        (&c.truth, PANEL * 100 * 8),
        (&c.order, c.rows * 8),
    ] {
        require(
            a.path.is_absolute()
                && paths.insert(&a.path)
                && a.bytes > 0
                && a.bytes <= cap
                && valid_sha(&a.sha256),
            "artifact descriptor",
        )?;
    }
    require(
        c.truth.bytes == PANEL * 100 * 8 && c.order.bytes == c.rows * 8,
        "truth/order exact geometry",
    )
}
fn config(path: &Path, sha: &str) -> Result<Config> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32)
        .open(path)?;
    let length = usize::try_from(file.metadata()?.len())?;
    let c: Config = serde_json::from_slice(&checked(
        &Artifact {
            path: path.into(),
            bytes: length,
            sha256: sha.into(),
        },
        CONFIG_CAP,
    )?)?;
    validate_config(&c)?;
    Ok(c)
}
fn inverse_order(body: &[u8], rows: usize) -> Result<Vec<usize>> {
    require(body.len() == rows * 8, "order length")?;
    let mut inverse = vec![usize::MAX; rows];
    for (physical, word) in body.chunks_exact(8).enumerate() {
        let logical = usize::try_from(u64::from_le_bytes(word.try_into()?))?;
        require(
            logical < rows && inverse[logical] == usize::MAX,
            "order permutation",
        )?;
        inverse[logical] = physical;
    }
    Ok(inverse)
}
fn emit(file: &mut File, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *file, value)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}
fn cpu_ns() -> i128 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    i128::from(t.tv_sec) * 1_000_000_000 + i128::from(t.tv_nsec)
}
fn observed_peak_bytes() -> Option<u64> {
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
fn executable_sha() -> Result<String> {
    let mut file = File::open(std::env::current_exe()?)?;
    let mut digest = Sha256::new();
    let mut block = [0; 65536];
    loop {
        let n = file.read(&mut block)?;
        if n == 0 {
            break;
        }
        digest.update(&block[..n]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
fn coverage(gold: &[i64], inverse: &[usize], includes: impl Fn(usize) -> bool) -> Value {
    let hits10 = gold[..10]
        .iter()
        .filter(|&&id| includes(inverse[id as usize]))
        .count();
    let hits100 = gold
        .iter()
        .filter(|&&id| includes(inverse[id as usize]))
        .count();
    json!({"hits10":hits10,"hits100":hits100})
}
async fn run(c: &Config, events: &mut File) -> Result<Value> {
    let store = LocalFileSystem::new_with_prefix(&c.store_root)?;
    let prefix = ObjectPath::from(c.generation_prefix.clone());
    let limits = TwoBitGenerationLimits {
        max_memory_bytes: c.max_memory_bytes,
        max_active_queries: 1,
        max_query_bytes: 16773120,
        max_query_gets: 32,
        max_parallel_gets: 16,
        max_source_bytes: 64 * 1024 * 1024,
        max_source_gets: 128,
        max_parallel_source_gets: 16,
        max_query_scratch_bytes: 400000,
        already_pinned_bytes: EVALUATOR_CHARGE,
    };
    let generation = TwoBitGeneration::open_remote(
        &store,
        &prefix,
        &c.generation_root_sha256,
        limits,
        &c.scratch_parent,
    )
    .await?;
    require(
        generation.rows() == c.rows
            && generation.discovery_mode() == DiscoveryMode::Semantic
            && generation.semantic_profile() == Some(c.profile),
        "native generation/profile agreement",
    )?;
    // The native opener authenticated this receipt. Bind the independently retained
    // evaluator mapping to the same source order; it never enters nomination.
    let root_path = c
        .store_root
        .join(&c.generation_prefix)
        .join("manifest.json");
    let root_body = checked(
        &Artifact {
            bytes: usize::try_from(root_path.metadata()?.len())?,
            path: root_path,
            sha256: c.generation_root_sha256.clone(),
        },
        CONFIG_CAP,
    )?;
    let root: Value = serde_json::from_slice(&root_body)?;
    let plane_path = c
        .store_root
        .join(&c.generation_prefix)
        .join("plane/manifest.json");
    let plane_body = checked(
        &Artifact {
            bytes: usize::try_from(plane_path.metadata()?.len())?,
            path: plane_path,
            sha256: root["plane_manifest_sha256"]
                .as_str()
                .ok_or("plane digest")?
                .into(),
        },
        CONFIG_CAP,
    )?;
    let plane: Value = serde_json::from_slice(&plane_body)?;
    require(
        plane["source_order_sha256"] == c.order.sha256,
        "evaluator source order binding",
    )?;
    let inverse = inverse_order(&checked(&c.order, c.rows * 8)?, c.rows)?;
    let request_body = checked(&c.requests, 2 * 1024 * 1024)?;
    let mut requests = Vec::with_capacity(PANEL);
    for (index, line) in std::str::from_utf8(&request_body)?.lines().enumerate() {
        require(index < PANEL, "panel length")?;
        let request: Request = serde_json::from_str(line)?;
        require(
            request.ordinal == c.first + index
                && request.query.len() == c.dimensions
                && request.query.iter().all(|v| v.is_finite()),
            "request ordinal/geometry",
        )?;
        requests.push(request);
    }
    require(requests.len() == PANEL, "complete panel")?;
    emit(
        events,
        &json!({"phase":"startup","truth_opened":false,"profile":c.profile,
        "metadata":generation.remote_open_stats(),"evaluator_payload_charge":EVALUATOR_CHARGE}),
    )?;
    let mut frozen: Vec<(TwoBitSearchResult, TwoBitPlanTrace)> = Vec::with_capacity(PANEL);
    for request in &requests {
        let wall = Instant::now();
        let cpu = cpu_ns();
        let (result, trace) = match generation
            .diagnostic_search_with_store(&store, &request.query, 100)
            .await
        {
            Ok(result) => result,
            Err(error) => {
                let (source, sq8) = error.read_stats();
                let leaves = error.router_stats();
                emit(
                    events,
                    &json!({"phase":"query_failure","ordinal":request.ordinal,"error":error.to_string(),
                    "leaf_gets":leaves.submitted_gets,"leaf_bytes":leaves.verified_bytes,
                    "source_gets":source.submitted_gets,"source_bytes":source.verified_bytes,
                    "sq8_gets":sq8.submitted_gets,"sq8_bytes":sq8.verified_bytes,"stages":error.stages()}),
                )?;
                return Err(error.into());
            }
        };
        for candidate in &result.ranked.candidates {
            require(
                candidate.id >= 0
                    && (candidate.id as usize) < c.rows
                    && inverse[candidate.id as usize] == candidate.ordinal,
                "returned ID/order binding",
            )?;
        }
        emit(
            events,
            &json!({"phase":"frozen_query","ordinal":request.ordinal,"truth_opened":false,
            "trace":trace,"selected_pages":result.plan.selected_pages,"ranges":result.plan.ranges,
            "returned_ids":result.ranked.candidates.iter().map(|r|r.id).collect::<Vec<_>>(),
            "leaf_gets":result.router_stats.submitted_gets,"leaf_bytes":result.router_stats.verified_bytes,
            "source_gets":result.source_stats.submitted_gets,"source_bytes":result.source_stats.verified_bytes,
            "sq8_gets":result.ranked.stats.submitted_gets,"sq8_bytes":result.ranked.stats.verified_bytes,
            "stages":result.stages,"query_wall_ns":wall.elapsed().as_nanos(),"query_process_cpu_ns":cpu_ns()-cpu}),
        )?;
        frozen.push((result, trace));
    }
    emit(
        events,
        &json!({"phase":"all_queries_frozen","count":PANEL,"truth_opened":false}),
    )?;
    let truth = checked(&c.truth, PANEL * 100 * 8)?;
    let mut hits10 = 0;
    let mut hits100 = 0;
    for (index, ((result, trace), truth)) in
        frozen.iter().zip(truth.chunks_exact(100 * 8)).enumerate()
    {
        let gold = truth
            .chunks_exact(8)
            .map(|word| i64::from_le_bytes(word.try_into().unwrap()))
            .collect::<Vec<_>>();
        require(
            gold.iter().all(|&id| id >= 0 && (id as usize) < c.rows)
                && gold.iter().collect::<BTreeSet<_>>().len() == 100,
            "truth logical IDs/uniqueness",
        )?;
        let nominated: BTreeSet<_> = trace.semantic_units.iter().copied().collect();
        let closure: BTreeSet<_> = nominated.iter().map(|u| u / 8).collect();
        let scored: BTreeSet<_> = trace.nomination_evaluated_units.iter().copied().collect();
        let source_pages: BTreeSet<_> = trace.ranked_candidate_pages.iter().copied().collect();
        let ids = result
            .ranked
            .candidates
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>();
        let h10 = ids[..10]
            .iter()
            .filter(|id| gold[..10].contains(id))
            .count();
        let h100 = ids.iter().filter(|id| gold.contains(id)).count();
        hits10 += h10;
        hits100 += h100;
        let row_bytes = c.dimensions + 12;
        emit(
            events,
            &json!({"phase":"evaluation","ordinal":c.first+index,
            "nominated_units":coverage(&gold,&inverse,|row|nominated.contains(&(row/32))),
            "page_closure":coverage(&gold,&inverse,|row|closure.contains(&(row/256))),
            "source_scored_units":coverage(&gold,&inverse,|row|scored.contains(&(row/32))),
            "source_ranked_pages":coverage(&gold,&inverse,|row|source_pages.contains(&(row/256))),
            "sq8_admitted_ranges":coverage(&gold,&inverse,|row|result.plan.ranges.iter().any(|r|r.contains(&(row*row_bytes)))),
            "returned_hits10":h10,"returned_hits100":h100}),
        )?;
    }
    Ok(
        json!({"status":if hits10 >= 608 {"PASS"} else {"FAIL"},"complete":true,"queries":PANEL,
        "hits10":hits10,"hits100":hits100,"mean_returned_r10":hits10 as f64/640.,"mean_returned_r100":hits100 as f64/6400.,
        "observed_process_peak_bytes":observed_peak_bytes(),"physical_s3_measured":false}),
    )
}
fn execute() -> Result<bool> {
    let args = std::env::args().collect::<Vec<_>>();
    require(
        args.len() == 4,
        "usage: check_semantic_router_scorer CONFIG CONFIG_SHA NEW_RESULT_JSONL",
    )?;
    let mut events = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[3])?;
    emit(
        &mut events,
        &json!({"schema":"borsuk-semantic-router-scorer-result-v2","phase":"identity",
        "config_sha256":args[2],"binary_sha256":executable_sha()?,"scorer_source_sha256":hash(include_bytes!("check_semantic_router_scorer.rs")),
        "router_source_sha256":hash(include_bytes!("../semantic_unit_router.rs")),"physical_s3_measured":false}),
    )?;
    let result = (|| -> Result<Value> {
        let c = config(Path::new(&args[1]), &args[2])?;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run(&c, &mut events))
    })();
    let terminal = match result {
        Ok(summary) => summary,
        Err(error) => json!({"status":"FAIL","complete":false,"error":error.to_string()}),
    };
    emit(&mut events, &json!({"phase":"terminal","summary":terminal}))?;
    events.sync_all()?;
    println!("{terminal}");
    Ok(terminal["status"] == "PASS")
}
fn main() {
    match execute() {
        Ok(true) => (),
        Ok(false) => std::process::exit(2),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inverse_requires_an_exact_permutation_and_maps_physical_coverage() {
        let order = [2_u64, 0, 1]
            .into_iter()
            .flat_map(u64::to_le_bytes)
            .collect::<Vec<_>>();
        assert_eq!(inverse_order(&order, 3).unwrap(), [1, 2, 0]);
        assert!(inverse_order(&order[..23], 3).is_err());
        let duplicate = [0_u64, 0, 1]
            .into_iter()
            .flat_map(u64::to_le_bytes)
            .collect::<Vec<_>>();
        assert!(inverse_order(&duplicate, 3).is_err());
    }
    #[test]
    fn authenticated_config_and_artifacts_reject_unknown_fields_and_hash_or_length_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("artifact");
        std::fs::write(&path, b"abc").unwrap();
        let a = Artifact {
            path: path.clone(),
            bytes: 3,
            sha256: hash(b"abc"),
        };
        assert_eq!(checked(&a, 3).unwrap(), b"abc");
        assert!(checked(&a, 2).is_err());
        std::fs::write(&path, b"abd").unwrap();
        assert!(checked(&a, 3).is_err());
        std::fs::write(&path, b"ab").unwrap();
        assert!(checked(&a, 3).is_err());
        assert!(
            serde_json::from_str::<Request>(r#"{"ordinal":1,"query":[1.0],"nominees":[1]}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<SemanticProfile>(r#""fresh_2m""#).is_err());
    }
}
