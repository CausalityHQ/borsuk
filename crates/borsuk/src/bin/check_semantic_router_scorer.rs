//! Truth-free frozen local/logical-I/O diagnostics through the production generation.
use borsuk::{
    semantic_unit_router::SemanticProfile,
    two_bit_generation::{
        DiscoveryMode, TwoBitGeneration, TwoBitGenerationLimits, TwoBitPlanTrace,
    },
};
use object_store::{chunked::ChunkedStore, local::LocalFileSystem, path::Path as ObjectPath};
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
    sync::Arc,
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
        c.schema == "borsuk-semantic-router-scorer-config-v3"
            && matches!(c.dataset.as_str(), "ReLAION" | "CoHere")
            && c.profile.valid_geometry(c.rows, c.dimensions)
            && c.count == PANEL
            && c.first == 0
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
    for (a, cap) in [(&c.requests, 2 * 1024 * 1024), (&c.order, c.rows * 8)] {
        require(
            a.path.is_absolute()
                && paths.insert(&a.path)
                && a.bytes > 0
                && a.bytes <= cap
                && valid_sha(&a.sha256),
            "artifact descriptor",
        )?;
    }
    require(c.order.bytes == c.rows * 8, "order exact geometry")
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
fn diagnostic_scratch_bytes(rows: usize) -> Result<usize> {
    // Keep the production codec allowance; admission also charges the trace.
    400_000_usize
        .checked_add(TwoBitPlanTrace::scratch_bytes(rows))
        .ok_or_else(|| "diagnostic scratch overflow".into())
}
fn scorer_store(root: &Path) -> object_store::Result<ChunkedStore> {
    // Keep local range payloads bounded while using the production stream verifier.
    Ok(ChunkedStore::new(
        Arc::new(LocalFileSystem::new_with_prefix(root)?),
        8192,
    ))
}
fn freeze(c: &Config, events: &mut File, identity: &Value, completed: usize) -> Result<Value> {
    require(completed == PANEL, "complete frozen panel required")?;
    // All diagnostics must be durable before claiming the measurement is frozen.
    events.sync_all()?;
    let summary = json!({"status":"FROZEN","complete":true,"queries":completed,"first":c.first,
        "dataset":c.dataset,"profile":c.profile,"rows":c.rows,"dimensions":c.dimensions,
        "config_sha256":identity["config_sha256"],"binary_sha256":identity["binary_sha256"],
        "scorer_source_sha256":identity["scorer_source_sha256"],"router_source_sha256":identity["router_source_sha256"],
        "generation_prefix":c.generation_prefix,"generation_root_sha256":c.generation_root_sha256,
        "requests_sha256":c.requests.sha256,"requests_bytes":c.requests.bytes,
        "order_sha256":c.order.sha256,"order_bytes":c.order.bytes,
        "truth_opened":false,"observed_process_peak_bytes":observed_peak_bytes(),"physical_s3_measured":false});
    emit(
        events,
        &json!({"phase":"all_queries_frozen","count":completed,"status":"FROZEN",
        "complete":true,"truth_opened":false,"summary":summary}),
    )?;
    events.sync_all()?;
    Ok(summary)
}
async fn run(c: &Config, events: &mut File, identity: &Value) -> Result<Value> {
    let store = scorer_store(&c.store_root)?;
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
        max_query_scratch_bytes: diagnostic_scratch_bytes(c.rows)?,
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
    // offline mapping to the same source order; it never enters nomination.
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
    let mut completed = 0;
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
        require(
            result.ranked.candidates.len() >= 10,
            "fewer than ten returned IDs",
        )?;
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
        completed += 1;
    }
    freeze(c, events, identity, completed)
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
    let identity = json!({"schema":"borsuk-semantic-router-scorer-result-v3","phase":"identity",
        "config_sha256":args[2],"binary_sha256":executable_sha()?,"scorer_source_sha256":hash(include_bytes!("check_semantic_router_scorer.rs")),
        "router_source_sha256":hash(include_bytes!("../semantic_unit_router.rs")),"physical_s3_measured":false});
    emit(&mut events, &identity)?;
    let result = (|| -> Result<Value> {
        let c = config(Path::new(&args[1]), &args[2])?;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run(&c, &mut events, &identity))
    })();
    let terminal = match result {
        Ok(summary) => summary,
        Err(error) => json!({"status":"FAIL","complete":false,"error":error.to_string()}),
    };
    emit(&mut events, &json!({"phase":"terminal","summary":terminal}))?;
    events.sync_all()?;
    println!("{terminal}");
    Ok(terminal["status"] == "FROZEN")
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
    fn config_value(dir: &Path) -> Value {
        json!({"schema":"borsuk-semantic-router-scorer-config-v3",
        "dataset":"CoHere","profile":"fresh1m","rows":1_000_000,"dimensions":768,
        "first":0,"count":64,"store_root":dir.join("store"),"generation_prefix":"semantic/index",
        "generation_root_sha256":"a".repeat(64),"scratch_parent":dir.join("scratch"),
        "requests":{"path":dir.join("requests.jsonl"),"bytes":1024,"sha256":"b".repeat(64)},
        "order":{"path":dir.join("order.u64"),"bytes":8_000_000,"sha256":"c".repeat(64)},
        "max_memory_bytes":536_870_912})
    }
    #[test]
    fn truth_free_v3_config_rejects_truth_unknown_fields_and_old_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let value = config_value(dir.path());
        let body = serde_json::to_vec(&value).unwrap();
        std::fs::write(&path, &body).unwrap();
        config(&path, &hash(&body)).unwrap();
        for field in [
            "truth",
            "truth_path",
            "truth_sha256",
            "truth_body",
            "unknown",
        ] {
            let mut invalid = value.clone();
            invalid[field] =
                json!({"path":"/unreadable/truth","bytes":51200,"sha256":"d".repeat(64)});
            assert!(serde_json::from_value::<Config>(invalid).is_err());
        }
        for (field, rejected) in [
            ("schema", json!("borsuk-semantic-router-scorer-config-v2")),
            ("first", json!(1)),
            ("count", json!(63)),
        ] {
            let mut invalid = value.clone();
            invalid[field] = rejected;
            let body = serde_json::to_vec(&invalid).unwrap();
            std::fs::write(&path, &body).unwrap();
            assert!(config(&path, &hash(&body)).is_err());
        }
    }
    #[test]
    fn frozen_marker_requires_complete64_and_binds_measurement_identity() {
        let dir = tempfile::tempdir().unwrap();
        let c: Config = serde_json::from_value(config_value(dir.path())).unwrap();
        let identity = json!({"config_sha256":"d".repeat(64),"binary_sha256":"e".repeat(64),
            "scorer_source_sha256":"f".repeat(64),"router_source_sha256":"0".repeat(64)});
        let path = dir.path().join("records.jsonl");
        let mut events = File::create(&path).unwrap();
        assert!(freeze(&c, &mut events, &identity, 0).is_err());
        assert_eq!(events.metadata().unwrap().len(), 0);
        for ordinal in 0..63 {
            emit(
                &mut events,
                &json!({"phase":"frozen_query","ordinal":ordinal}),
            )
            .unwrap();
        }
        let partial_length = events.metadata().unwrap().len();
        for count in [63, 65] {
            assert!(freeze(&c, &mut events, &identity, count).is_err());
            assert_eq!(events.metadata().unwrap().len(), partial_length);
        }
        emit(&mut events, &json!({"phase":"frozen_query","ordinal":63})).unwrap();
        let summary = freeze(&c, &mut events, &identity, 64).unwrap();
        assert_eq!(summary["status"], "FROZEN");
        assert_eq!(summary["complete"], true);
        assert_eq!(summary["queries"], 64);
        assert_eq!(summary["first"], 0);
        for field in [
            "config_sha256",
            "binary_sha256",
            "scorer_source_sha256",
            "router_source_sha256",
        ] {
            assert_eq!(summary[field], identity[field]);
        }
        assert_eq!(summary["generation_root_sha256"], "a".repeat(64));
        assert_eq!(summary["requests_sha256"], "b".repeat(64));
        assert_eq!(summary["order_sha256"], "c".repeat(64));
        assert!(summary.get("hits10").is_none());
        let records = std::fs::read_to_string(&path).unwrap();
        let records = records
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 65);
        assert_eq!(records[64]["phase"], "all_queries_frozen");
        assert_eq!(records[64]["count"], 64);
        assert_eq!(records[64]["summary"], summary);
    }
    #[tokio::test]
    async fn real_file_source_and_sq8_preserve_production_ranking_and_fail_closed() {
        use borsuk::{
            sq8_s3_range::Sq8ReadStats, two_bit_build::TwoBitGenerationBuilder,
            two_bit_source::TwoBitSource, two_bit_store::publish_two_bit_generation,
        };
        use object_store::{GetResultPayload, ObjectStoreExt};
        let rows = 257;
        let dimensions = 768;
        let temp = tempfile::tempdir().unwrap();
        let store_root = temp.path().join("store");
        std::fs::create_dir(&store_root).unwrap();
        let direct = LocalFileSystem::new_with_prefix(&store_root).unwrap();
        let store = scorer_store(&store_root).unwrap();
        let mut raw = Vec::new();
        let mut sq8 = Vec::new();
        for row in 0..rows {
            let value = (1 + row % 7) as u8;
            for _ in 0..dimensions {
                raw.extend_from_slice(&(value as f32).to_le_bytes());
            }
            sq8.extend_from_slice(&((rows - row + 1000) as i64).to_le_bytes());
            sq8.extend_from_slice(&(dimensions as f32 * (value as f32).powi(2)).to_le_bytes());
            sq8.extend(std::iter::repeat_n(value, dimensions));
        }
        let raw_path = temp.path().join("raw");
        let sq8_path = temp.path().join("sq8");
        std::fs::write(&raw_path, &raw).unwrap();
        std::fs::write(&sq8_path, &sq8).unwrap();
        let sq8_sha = hash(&sq8);
        let key = ObjectPath::from(format!("semantic/objects/{sq8_sha}"));
        direct.put(&key, sq8.into()).await.unwrap();
        assert!(matches!(
            direct.get(&key).await.unwrap().payload,
            GetResultPayload::File(..)
        ));
        let etag = direct.head(&key).await.unwrap().e_tag.unwrap();
        let root = temp.path().join("generation");
        let order = (0..rows as u64).collect::<Vec<_>>();
        let root_sha = TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &hash(&raw),
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions,
            },
            base_epoch: 0,
            generation: 1,
            low: &[0.; 768],
            step: &[1.; 768],
            sq8_object_key: key.as_ref(),
            sq8_etag: &etag,
        }
        .build_with_discovery(Some(&order), DiscoveryMode::Semantic, &root, 128_000_000)
        .unwrap();
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 512 * 1024 * 1024,
            max_active_queries: 1,
            max_query_bytes: 16_773_120,
            max_query_gets: 32,
            max_parallel_gets: 16,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 16,
            max_query_scratch_bytes: diagnostic_scratch_bytes(rows).unwrap(),
            already_pinned_bytes: EVALUATOR_CHARGE,
        };
        let head = publish_two_bit_generation(
            &direct,
            &ObjectPath::from("semantic/index"),
            &root,
            &root_sha,
            limits,
            None,
        )
        .await
        .unwrap();
        let prefix = head.metadata_prefix();
        let generation =
            TwoBitGeneration::open_remote(&store, &prefix, &root_sha, limits, temp.path())
                .await
                .unwrap();
        let query = [1.; 768];
        let error = generation
            .diagnostic_search_with_store(&direct, &query, 100)
            .await
            .err()
            .unwrap();
        assert!(
            format!("{error:?}").contains("UnexpectedMetadata"),
            "{error:?}"
        );
        assert!(error.router_stats().verified_bytes > 0);
        assert!(error.read_stats().0.failed_gets > 0);
        assert_eq!(error.read_stats().1, Sq8ReadStats::default());
        let expected = generation
            .search_with_store(&store, &query, 100, None)
            .await
            .unwrap();
        let (actual, trace) = generation
            .diagnostic_search_with_store(&store, &query, 100)
            .await
            .unwrap();
        assert_eq!(actual.ranked.candidates.len(), 100);
        assert_eq!(actual.ranked.candidates, expected.ranked.candidates);
        assert_eq!(actual.plan, expected.plan);
        assert_eq!(actual.source_stats, expected.source_stats);
        assert_eq!(actual.router_stats, expected.router_stats);
        assert_eq!(actual.ranked.stats, expected.ranked.stats);
        assert!(actual.source_stats.verified_bytes > 8192);
        assert!(actual.ranked.stats.verified_bytes > 8192);
        assert!(!trace.nomination_evaluated_units.is_empty());
        let source_key = ObjectPath::from(format!("{prefix}/plane/records.bin"));
        let mut changed = std::fs::read(root.join("plane/records.bin")).unwrap();
        changed[0] ^= 1;
        // Replacing the file changes its ETag, so the already-open generation rejects it.
        store.put(&source_key, changed.into()).await.unwrap();
        let error = generation
            .diagnostic_search_with_store(&store, &query, 100)
            .await
            .err()
            .unwrap();
        assert!(format!("{error:?}").contains("Precondition"), "{error:?}");
        assert!(error.read_stats().0.failed_gets > 0);
        assert_eq!(error.read_stats().0.verified_bytes, 0);
        assert_eq!(error.read_stats().1, Sq8ReadStats::default());
        // Refreshing HEAD accepts the new ETag, but the frozen page SHA still rejects it.
        let reopened =
            TwoBitGeneration::open_remote(&store, &prefix, &root_sha, limits, temp.path())
                .await
                .unwrap();
        let error = reopened
            .search_with_store(&store, &query, 100, None)
            .await
            .err()
            .unwrap();
        assert!(format!("{error:?}").contains("Page("), "{error:?}");
        assert!(error.read_stats().0.failed_gets > 0);
        assert_eq!(error.read_stats().0.verified_bytes, 0);
        assert_eq!(error.read_stats().1, Sq8ReadStats::default());
    }
    #[test]
    fn diagnostic_budget_admits_d768_without_spending_the_trace_charge() {
        use borsuk::rotated_two_bit::{RotatedTwoBitCodec, TwoBitError};
        let codec = RotatedTwoBitCodec::new(&[0.; 768], 0).unwrap();
        for rows in [100_000, 1_000_000] {
            let trace_bytes = TwoBitPlanTrace::scratch_bytes(rows);
            assert!(matches!(
                codec.prepare_query(&[1.; 768], 400_000 - trace_bytes),
                Err(TwoBitError::MemoryBudget)
            ));
            let scratch = diagnostic_scratch_bytes(rows).unwrap() - trace_bytes;
            assert_eq!(scratch, 400_000);
            codec.prepare_query(&[1.; 768], scratch).unwrap();
        }
    }
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
