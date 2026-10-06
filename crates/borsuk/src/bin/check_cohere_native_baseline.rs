//! Authenticated local-file Cohere quality baseline; external scientific gates required.
use borsuk::{
    exact_sq8_nominee::ScoredNominee,
    semantic_unit_router::SemanticProfile,
    sq8_s3_range::Sq8ReadStats,
    two_bit_generation::{
        DiscoveryMode, TwoBitGeneration, TwoBitGenerationLimits, TwoBitPlanTrace,
    },
    two_bit_source::SourcePlaneReceipt,
};
use object_store::{chunked::ChunkedStore, local::LocalFileSystem, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
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
const MEMORY_CAP: u64 = 512 * 1024 * 1024;
const BLOCK: usize = 65_536;
const DATASET: &str = "CohereLabs/wikipedia-2023-11-embed-multilingual-v3";
const REVISION: &str = "ade45fb52bd549f5e8c065636fe4160a43c2af36";
const CONFIG_SCHEMA: &str = "borsuk-cohere-native-baseline-config-v1";
const RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v1";

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
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    dataset: String,
    revision: String,
    metric: String,
    tie_rule: String,
    corpus_source_first: usize,
    query_source_first: usize,
    rows: usize,
    dimensions: usize,
    count: usize,
    k: usize,
    profile: SemanticProfile,
    store_root: PathBuf,
    generation_prefix: String,
    generation_root_sha256: String,
    scratch_parent: PathBuf,
    requests: Artifact,
    truth: Artifact,
    native_source: NativeSource,
    max_memory_bytes: u64,
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
    let mut file = regular(&a.path, Some(a.bytes), cap)?;
    let before = file_identity(&file)?;
    let mut body = vec![0; a.bytes];
    file.read_exact(&mut body)?;
    require(file.read(&mut [0])? == 0, "artifact EOF")?;
    require(
        file_identity(&file)? == before && hash(&body) == a.sha256,
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
fn validate_config(c: &Config, shape: Shape) -> Result<()> {
    require(
        c.schema == CONFIG_SCHEMA
            && c.dataset == DATASET
            && c.revision == REVISION
            && c.metric == "cosine"
            && c.tie_rule == "corpus_ordinal_ascending"
            && c.corpus_source_first == 0
            && c.query_source_first == shape.rows
            && c.rows == shape.rows
            && c.dimensions == D
            && c.count == shape.count
            && c.k == K
            && c.profile == SemanticProfile::Native100k
            && c.max_memory_bytes == MEMORY_CAP,
        "fixed Cohere corpus/query/D1024/k10/profile/cap",
    )?;
    for path in [
        &c.store_root,
        &c.scratch_parent,
        &c.requests.path,
        &c.truth.path,
    ] {
        require(
            path.is_absolute() && path.as_os_str().len() <= 4096,
            "absolute bounded path",
        )?;
    }
    require(
        c.requests.path != c.truth.path
            && c.requests.bytes == shape.count * D * 4
            && c.truth.bytes == shape.count * K * 8,
        "request/truth exact geometry/distinct paths",
    )?;
    require(
        !c.generation_prefix.is_empty()
            && c.generation_prefix.len() <= 512
            && c.generation_prefix.split('/').all(|p| {
                !p.is_empty()
                    && p != "."
                    && p != ".."
                    && p.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
            }),
        "generation prefix",
    )?;
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
    Ok(())
}
fn config(path: &Path, sha: &str, shape: Shape) -> Result<Config> {
    // Metadata comes from a nonblocking, no-follow regular descriptor, even for CONFIG.
    let file = regular(path, None, CONFIG_CAP)?;
    let c: Config = serde_json::from_slice(
        &checked(
            &Artifact {
                path: path.into(),
                bytes: usize::try_from(file.metadata()?.len())?,
                sha256: sha.into(),
            },
            CONFIG_CAP,
        )?
        .0,
    )?;
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
fn bind_source(c: &Config) -> Result<()> {
    #[derive(Deserialize)]
    struct Root {
        plane_manifest_sha256: String,
        sq8_object_sha256: String,
    }
    let root_path = c
        .store_root
        .join(&c.generation_prefix)
        .join("manifest.json");
    let root_file = regular(&root_path, None, CONFIG_CAP)?;
    let root: Root = serde_json::from_slice(
        &checked(
            &Artifact {
                path: root_path,
                bytes: usize::try_from(root_file.metadata()?.len())?,
                sha256: c.generation_root_sha256.clone(),
            },
            CONFIG_CAP,
        )?
        .0,
    )?;
    let plane_path = c
        .store_root
        .join(&c.generation_prefix)
        .join("plane/manifest.json");
    let plane_file = regular(&plane_path, None, CONFIG_CAP)?;
    let plane: SourcePlaneReceipt = serde_json::from_slice(
        &checked(
            &Artifact {
                path: plane_path,
                bytes: usize::try_from(plane_file.metadata()?.len())?,
                sha256: root.plane_manifest_sha256,
            },
            CONFIG_CAP,
        )?
        .0,
    )?;
    require(
        plane.rows == c.rows
            && plane.dimensions == D
            && !plane.query_or_truth_used
            && plane.source_sha256 == c.native_source.source_sha256
            && plane.sq8_sha256 == c.native_source.sq8_sha256
            && plane.source_order_sha256 == c.native_source.source_order_sha256
            && root.sq8_object_sha256 == c.native_source.sq8_sha256,
        "native source receipt binding",
    )
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
fn caller_bytes(c: &Config) -> usize {
    let descriptors = size_of::<Config>()
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
        ]
        .iter()
        .map(|s| s.capacity())
        .sum::<usize>()
        + [
            &c.store_root,
            &c.scratch_parent,
            &c.requests.path,
            &c.truth.path,
        ]
        .iter()
        .map(|p| p.capacity())
        .sum::<usize>();
    // Returned trace/result remain caller-owned while streaming, then drop before the next query.
    // Vec growth is bounded by twice the entire page roster; ranking retains only k entries.
    let returned = TwoBitPlanTrace::scratch_bytes(c.rows)
        + 2 * c.rows.div_ceil(256) * (size_of::<usize>() + size_of::<std::ops::Range<usize>>())
        + K * (size_of::<ScoredNominee>() + size_of::<Hit>());
    let query = c.requests.bytes + size_of::<[f32; D]>() + returned;
    // Reduction drops generation/requests first: one capped JSONL line, one truth body,
    // 8KiB reader, k hits. Authentication uses one 64KiB stack block.
    let reduction = LINE_CAP + 1 + c.truth.bytes + 8192 + 2 * K * size_of::<Hit>();
    // Four capped receipt/config bodies cover sequential authentication/decoding; identity,
    // descriptors duplicated by store/path/runtime setup fit another CONFIG_CAP.
    descriptors + 5 * CONFIG_CAP + BLOCK + query.max(reduction)
}
fn limits(c: &Config) -> Result<TwoBitGenerationLimits> {
    Ok(TwoBitGenerationLimits {
        max_memory_bytes: MEMORY_CAP,
        max_active_queries: 1,
        max_query_bytes: 16_773_120,
        max_query_gets: 32,
        max_parallel_gets: 16,
        max_source_bytes: 64 * 1024 * 1024,
        max_source_gets: 128,
        max_parallel_source_gets: 16,
        max_query_scratch_bytes: scratch_bytes(c.rows)?,
        already_pinned_bytes: caller_bytes(c) as u64,
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

struct Output {
    file: File,
    directory: File,
    path: PathBuf,
    digest: Sha256,
    bytes: u64,
    line_bytes: usize,
    cap: u64,
    #[cfg(test)]
    directory_synced: bool,
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
            || bytes.len() > LINE_CAP.saturating_sub(self.line_bytes)
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
            cap: OUTPUT_CAP - TERMINAL_RESERVE,
            #[cfg(test)]
            directory_synced: false,
        })
    }
    fn emit(&mut self, value: &impl Serialize) -> Result<()> {
        self.line_bytes = 0;
        serde_json::to_writer(&mut *self, value)?;
        self.write_all(b"\n")?;
        Ok(())
    }
    fn sha(&self) -> String {
        format!("{:x}", self.digest.clone().finalize())
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
    router: Charge,
    source: Charge,
    sq8: Charge,
}
impl Charges {
    fn add(&mut self, router: Sq8ReadStats, source: Sq8ReadStats, sq8: Sq8ReadStats) -> Result<()> {
        self.router.add(router)?;
        self.source.add(source)?;
        self.sq8.add(sq8)
    }
    fn sum(&self) -> Charge {
        Charge {
            submitted_gets: self.router.submitted_gets
                + self.source.submitted_gets
                + self.sq8.submitted_gets,
            verified_bytes: self.router.verified_bytes
                + self.source.verified_bytes
                + self.sq8.verified_bytes,
            failed_gets: self.router.failed_gets + self.source.failed_gets + self.sq8.failed_gets,
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Hit {
    id: u64,
    score_bits: u32,
}
fn validate_hits(hits: &[Hit], rows: usize) -> Result<()> {
    require(hits.len() <= K, "returned count exceeds k")?;
    for (i, h) in hits.iter().enumerate() {
        require(
            h.id < rows as u64
                && f32::from_bits(h.score_bits).is_finite()
                && !hits[..i].iter().any(|old| old.id == h.id),
            "returned IDs/scores",
        )?;
    }
    require(
        hits.windows(2).all(|h| {
            f32::from_bits(h[0].score_bits)
                .total_cmp(&f32::from_bits(h[1].score_bits))
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
    query_wall_ns: u128,
    query_cpu_ns: i128,
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
            query_wall_ns: 0,
            query_cpu_ns: 0,
        }
    }
}
fn seal(c: &Config, out: &mut Output, p: &mut Progress, requests: FileIdentity) -> Result<Seal> {
    require(p.completed == c.count, "cannot seal incomplete query panel")?;
    p.stage = "seal";
    out.file.sync_all()?;
    let prefix_bytes = out.bytes;
    let prefix_sha256 = out.sha();
    out.emit(
        &json!({"phase":"all_queries_sealed","count":p.completed,"truth_opened":false,
        "prefix_bytes":prefix_bytes,"prefix_sha256":prefix_sha256,
        "requests_sha256":c.requests.sha256,"generation_root_sha256":c.generation_root_sha256,
        "requires_successful_sync":true,"requires_successful_directory_sync":true}),
    )?;
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
async fn query_and_seal(
    c: &Config,
    shape: Shape,
    out: &mut Output,
    p: &mut Progress,
) -> Result<Seal> {
    validate_config(c, shape)?;
    p.stage = "requests";
    let (requests, request_identity) = checked(&c.requests, c.count * D * 4)?;
    // Authenticate and validate ALL rows before issuing even the first query.
    for ordinal in 0..c.count {
        request_row(&requests, ordinal).map_err(|e| format!("request {ordinal}: {e}"))?;
    }
    p.stage = "native_source";
    bind_source(c)?;
    let admission = limits(c)?;
    let store = ChunkedStore::new(
        Arc::new(LocalFileSystem::new_with_prefix(&c.store_root)?),
        8192,
    );
    p.stage = "generation_open";
    let generation = TwoBitGeneration::open_remote(
        &store,
        &ObjectPath::from(c.generation_prefix.clone()),
        &c.generation_root_sha256,
        admission,
        &c.scratch_parent,
    )
    .await?;
    require(
        generation.rows() == c.rows
            && generation.discovery_mode() == DiscoveryMode::Semantic
            && generation.semantic_profile() == Some(SemanticProfile::Native100k),
        "generation geometry/profile",
    )?;
    out.emit(
        &json!({"phase":"startup","metadata":generation.remote_open_stats(),
        "library_cap_bytes":MEMORY_CAP,"caller_pinned_bytes":admission.already_pinned_bytes,
        "codec_scratch_bytes":532480,"trace_scratch_bytes":TwoBitPlanTrace::scratch_bytes(c.rows),
        "query_scratch_bytes":admission.max_query_scratch_bytes,"truth_opened":false}),
    )?;
    for ordinal in 0..c.count {
        p.stage = "query";
        let query = request_row(&requests, ordinal)?;
        let wall = Instant::now();
        let cpu = cpu_ns();
        let result = generation
            .diagnostic_search_with_store(&store, &query, K)
            .await;
        let wall_ns = wall.elapsed().as_nanos();
        let cpu_ns = cpu_ns() - cpu;
        p.query_wall_ns += wall_ns;
        p.query_cpu_ns += cpu_ns;
        let (result, trace) = match result {
            Ok(result) => result,
            Err(e) => {
                let (source, sq8) = e.read_stats();
                p.charges.add(e.router_stats(), source, sq8)?;
                out.emit(
                    &json!({"phase":"query_failure","ordinal":ordinal,"error":e.to_string(),
                    "charges_so_far":p.charges,"sum_so_far":p.charges.sum(),"stages":e.stages(),
                    "query_wall_ns":wall_ns,"query_process_cpu_ns":cpu_ns,"truth_opened":false}),
                )?;
                return Err(format!("native query {ordinal}: {e}").into());
            }
        };
        p.charges.add(
            result.router_stats,
            result.source_stats,
            result.ranked.stats,
        )?;
        require(
            result.ranked.candidates.len() <= K,
            "native returned count exceeds k",
        )?;
        let mut returned = Vec::with_capacity(K);
        for hit in &result.ranked.candidates {
            require(
                hit.id >= 0 && hit.ordinal < c.rows,
                "native returned identity",
            )?;
            returned.push(Hit {
                id: hit.id as u64,
                score_bits: hit.score.to_bits(),
            });
        }
        #[cfg(test)]
        returned.truncate(shape.returned_limit);
        validate_hits(&returned, c.rows)?;
        let underfill = returned.len() < K;
        let mut charges = Charges::default();
        charges.add(
            result.router_stats,
            result.source_stats,
            result.ranked.stats,
        )?;
        #[derive(Serialize)]
        struct Record<'a> {
            phase: &'static str,
            ordinal: usize,
            truth_opened: bool,
            returned: &'a [Hit],
            returned_count: usize,
            underfill: bool,
            charges: &'a Charges,
            sum: Charge,
            stages: &'a borsuk::two_bit_generation::QueryStages,
            query_wall_ns: u128,
            query_process_cpu_ns: i128,
            trace: &'a TwoBitPlanTrace,
        }
        out.emit(&Record {
            phase: "query",
            ordinal,
            truth_opened: false,
            returned_count: returned.len(),
            underfill,
            returned: &returned,
            charges: &charges,
            sum: charges.sum(),
            stages: &result.stages,
            query_wall_ns: wall_ns,
            query_process_cpu_ns: cpu_ns,
            trace: &trace,
        })?;
        p.completed += 1;
        p.underfilled += usize::from(underfill);
        // result, trace and decoded row drop here; no panel-sized retained traces.
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
    require(
        p.sealed && p.completed == c.count,
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
    let mut line = Vec::with_capacity(LINE_CAP + 1);
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
        let n = Read::take(&mut reader, (LINE_CAP + 1) as u64).read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        require(
            n <= LINE_CAP && line.last() == Some(&b'\n'),
            "sealed row cap/EOF",
        )?;
        consumed += n as u64;
        digest.update(&line);
        // Ignored trace fields deserialize without constructing a JSON tree.
        let row: Row = serde_json::from_slice(&line)?;
        if row.phase != "query" {
            continue;
        }
        require(
            queries < c.count && row.ordinal == Some(queries) && row.truth_opened == Some(false),
            "sealed query sequence/truth boundary",
        )?;
        let returned = row.returned.ok_or("sealed returned IDs missing")?;
        validate_hits(&returned, c.rows)?;
        require(
            row.returned_count == Some(returned.len()) && row.underfill == Some(returned.len() < K),
            "sealed returned count/underfill",
        )?;
        let truth_ids = truth_row(&truth, queries, c.rows)?;
        let hits = returned
            .iter()
            .filter(|h| truth_ids.contains(&h.id))
            .count();
        total_hits += hits;
        out.emit(&json!({"phase":"recall","ordinal":queries,"hits10":hits,
            "recall10":hits as f64 / K as f64,"returned_count":returned.len(),
            "underfill":returned.len() < K}))?;
        queries += 1;
    }
    require(
        consumed == seal.prefix_bytes
            && format!("{:x}", digest.finalize()) == seal.prefix_sha256
            && queries == c.count,
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
    Ok(
        json!({"status":"MEASURED","complete":true,"queries":queries,"k":K,
        "total_hits10":total_hits,"recall_numerator":total_hits,"recall_denominator":c.count * K,
        "mean_recall10":total_hits as f64 / (c.count * K) as f64,"underfilled_queries":p.underfilled,
        "all_queries_sealed":true,"prefix_bytes":seal.prefix_bytes,"prefix_sha256":seal.prefix_sha256,
        "sealed_bytes":seal.sealed_bytes,"sealed_sha256":seal.sealed_sha256,
        "requests_sha256":c.requests.sha256,"truth_sha256":c.truth.sha256,
        "generation_root_sha256":c.generation_root_sha256,"charges":p.charges,"sum":p.charges.sum(),
        "query_wall_ns":p.query_wall_ns,"query_process_cpu_ns":p.query_cpu_ns,
        "physical_s3_measured":false,"external_gate_required":true}),
    )
}
fn execute_paths(
    config_path: &Path,
    config_sha: &str,
    output_path: &Path,
    shape: Shape,
) -> Result<bool> {
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
        out.emit(&json!({"schema":RESULT_SCHEMA,"phase":"identity","config_sha256":config_sha,
            "binary_sha256":executable_sha()?,"runner_source_sha256":hash(include_bytes!("check_cohere_native_baseline.rs")),
            "generation_source_sha256":hash(include_bytes!("../two_bit_generation.rs")),
            "router_source_sha256":hash(include_bytes!("../semantic_unit_router.rs")),
            "codec_source_sha256":hash(include_bytes!("../rotated_two_bit.rs")),
            "source_plane_source_sha256":hash(include_bytes!("../two_bit_source.rs")),
            "scope":"LOCAL_AUTHENTICATED_FILE_QUALITY_CORRECTNESS","physical_s3_measured":false,
            "io_measurement":"logical_object_store_GETs_and_authenticated_payload_bytes",
            "external_gate_required":true,"truth_opened":false}))?;
        let c = config(config_path, config_sha, shape)?;
        out.emit(&json!({"phase":"bound_inputs","dataset":c.dataset,"revision":c.revision,
            "metric":c.metric,"tie_rule":c.tie_rule,"rows":c.rows,"dimensions":D,"count":c.count,"k":K,
            "corpus_source_first":c.corpus_source_first,"query_source_first":c.query_source_first,
            "profile":c.profile,"store_root":c.store_root,"generation_prefix":c.generation_prefix,
            "generation_root_sha256":c.generation_root_sha256,
            "requests_bytes":c.requests.bytes,"requests_sha256":c.requests.sha256,
            "truth_bytes":c.truth.bytes,"truth_sha256":c.truth.sha256,
            "native_source_sha256":c.native_source.source_sha256,"native_sq8_sha256":c.native_source.sq8_sha256,
            "native_order_sha256":c.native_source.source_order_sha256,"truth_opened":false}))?;
        let seal = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(query_and_seal(&c, shape, &mut out, &mut p))?;
        let summary = reduce(&c, &mut out, &seal, &mut p)?;
        p.stage = "reduction_sync";
        out.file.sync_all()?;
        Ok(summary)
    })();
    let mut summary = match result {
        Ok(summary) => summary,
        Err(e) => json!({"status":"INVALID","complete":false,"stage":p.stage,
            "error":e.to_string().chars().take(512).collect::<String>(),"completed_queries":p.completed,
            "all_queries_sealed":p.sealed,"truth_opened":p.truth_opened,"charges":p.charges,"sum":p.charges.sum(),
            "physical_s3_measured":false,"external_gate_required":true}),
    };
    summary["process_wall_ns"] = json!(started.elapsed().as_nanos());
    summary["process_cpu_ns"] = json!(cpu_ns() - cpu);
    summary["observed_process_peak_bytes"] = json!(peak_bytes());
    out.cap = OUTPUT_CAP;
    // A cap failure may have left a partial record; start the INVALID terminal on a new line.
    if summary["status"] == "INVALID" {
        out.line_bytes = 0;
        out.write_all(b"\n")?;
    }
    out.emit(&json!({"phase":"terminal","summary":summary}))?;
    if let Err(e) = out.file.sync_all() {
        // Never print a successful measurement when its final durable write failed.
        out.emit(
            &json!({"phase":"terminal","summary":{"status":"INVALID","complete":false,
            "stage":"terminal_sync","error":e.to_string(),"all_queries_sealed":p.sealed,
            "external_gate_required":true}}),
        )?;
        let _ = out.file.sync_all();
        return Err(format!("terminal sync: {e}").into());
    }
    println!("{summary}");
    Ok(summary["status"] == "MEASURED")
}
fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let result = if args.len() == 4 {
        execute_paths(
            Path::new(&args[1]),
            &args[2],
            Path::new(&args[3]),
            Shape::PRODUCTION,
        )
    } else {
        Err("usage: check_cohere_native_baseline CONFIG CONFIG_SHA NEW_OUTPUT".into())
    };
    match result {
        Ok(true) => (),
        Ok(false) => std::process::exit(2),
        Err(e) => {
            eprintln!("INVALID: {e}; external gate required");
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use borsuk::{
        two_bit_build::TwoBitGenerationBuilder, two_bit_source::TwoBitSource,
        two_bit_store::publish_two_bit_generation,
    };
    use object_store::ObjectStoreExt;

    fn artifact(path: &Path, bytes: &[u8]) -> Value {
        std::fs::write(path, bytes).unwrap();
        json!({"path":path,"bytes":bytes.len(),"sha256":hash(bytes)})
    }
    fn config_value(dir: &Path, shape: Shape) -> Value {
        json!({"schema":CONFIG_SCHEMA,"dataset":DATASET,"revision":REVISION,
            "metric":"cosine","tie_rule":"corpus_ordinal_ascending",
            "corpus_source_first":0,"query_source_first":shape.rows,"rows":shape.rows,
            "dimensions":D,"count":shape.count,"k":K,"profile":"native100k",
            "store_root":dir.join("store"),"generation_prefix":"semantic/index",
            "generation_root_sha256":"a".repeat(64),"scratch_parent":dir.join("scratch"),
            "requests":{"path":dir.join("requests.f32"),"bytes":shape.count*D*4,"sha256":"b".repeat(64)},
            "truth":{"path":dir.join("truth.u64"),"bytes":shape.count*K*8,"sha256":"c".repeat(64)},
            "native_source":{"source_sha256":"d".repeat(64),"sq8_sha256":"e".repeat(64),
                "source_order_sha256":"f".repeat(64)},"max_memory_bytes":MEMORY_CAP})
    }
    fn write_config(dir: &Path, value: &Value) -> (PathBuf, String) {
        let path = dir.join("config.json");
        let body = serde_json::to_vec(value).unwrap();
        std::fs::write(&path, &body).unwrap();
        (path, hash(&body))
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
            _ => v[200 + id] = 1.,
        }
        v
    }
    async fn fixture(shape: Shape) -> (tempfile::TempDir, Value) {
        assert!(shape.rows >= 32);
        let dir = tempfile::tempdir().unwrap();
        let mut c = config_value(dir.path(), shape);
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
        .build_with_discovery(Some(&order), DiscoveryMode::Semantic, &root, 128_000_000)
        .unwrap();
        let native: SourcePlaneReceipt =
            serde_json::from_slice(&std::fs::read(root.join("plane/manifest.json")).unwrap())
                .unwrap();
        c["generation_root_sha256"] = json!(root_sha);
        c["native_source"] = json!({"source_sha256":raw_sha,"sq8_sha256":sq8_sha,
            "source_order_sha256":native.source_order_sha256});
        let mut requests = Vec::new();
        for (axis, scale) in [(0, 3_f32), (1, 5_f32)] {
            for d in 0..D {
                requests.extend_from_slice(&(if d == axis { scale } else { 0. }).to_le_bytes());
            }
        }
        c["requests"] = artifact(&dir.path().join("requests.f32"), &requests);
        // Cosine ignores raw norms: axis hit, eight .75 hits, then ID8 wins the
        // .5 tie over ID9 at the k10 boundary, independently for each query.
        let truth = TRUTH_IDS
            .into_iter()
            .flatten()
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
        (dir, c)
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
            ("max_memory_bytes", json!(MEMORY_CAP - 1)),
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
    async fn actual_native_queries_seal_and_reduce_against_literal_oracles() {
        let shape = Shape::tiny(257);
        let (dir, c) = fixture(shape).await;
        let (path, sha) = write_config(dir.path(), &c);
        let output = dir.path().join("results");
        // execute_paths owns a runtime; do not nest it inside this fixture runtime.
        let c = config(&path, &sha, shape).unwrap();
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
        let seal = query_and_seal(&c, shape, &mut out, &mut p).await.unwrap();
        assert!(!p.truth_opened && p.sealed && out.directory_synced);
        let rows = records(&output);
        let queries = rows
            .iter()
            .filter(|r| r["phase"] == "query")
            .collect::<Vec<_>>();
        assert_eq!(queries.len(), 2);
        let sq8 = std::fs::read(dir.path().join("sq8")).unwrap();
        let literal_bits = [
            0x3d80_0000_u32,
            0x3f00_0000,
            0x3f00_0000,
            0x3f00_0000,
            0x3f00_0000,
            0x3f00_0000,
            0x3f00_0000,
            0x3f00_0000,
            0x3f00_0000,
            0x3f80_0000,
        ];
        for (i, query) in queries.iter().enumerate() {
            // Independently normalize the specified 3e0/5e1 queries to e0/e1.
            // Decode one scalar coordinate from stored SQ8 code with low=-1,
            // step=1/16: squared distance = stored_norm + 1 - 2*coordinate.
            // No production normalization, scoring, or returned IDs feed this oracle.
            let mut oracle = sq8
                .chunks_exact(D + 12)
                .enumerate()
                .map(|(physical, record)| {
                    let id = u64::from_le_bytes(record[..8].try_into().unwrap());
                    let norm = f32::from_le_bytes(record[8..12].try_into().unwrap());
                    let coordinate = -1. + f32::from(record[12 + i]) / 16.;
                    (physical, id, (norm + 1. - 2. * coordinate).to_bits())
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
            query_and_seal(&c, shape, &mut failed, &mut p)
                .await
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
            let c: Config = serde_json::from_value(invalid).unwrap();
            let mut out =
                Output::create(&dir.path().join(format!("early-invalid-{index}"))).unwrap();
            let mut p = Progress::default();
            assert!(query_and_seal(&c, shape, &mut out, &mut p).await.is_err());
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
}
