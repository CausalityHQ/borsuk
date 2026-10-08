//! Authenticated Local/S3 Cohere quality baseline; external scientific gates required.
use borsuk::{
    exact_sq8_nominee::ScoredNominee,
    semantic_unit_router::SemanticProfile,
    sq8_s3_range::{NativeTransportStats, OneAttemptS3, Sq8ReadStats},
    two_bit_generation::{
        DirectClosureLimits, DirectClosureMemory, DirectClosureRejection, DiscoveryMode,
        TwoBitGeneration, TwoBitGenerationLimits, TwoBitPlanTrace,
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
// v3 adds the required explicit `serving` mode; v2 and older configs refuse.
const CONFIG_SCHEMA: &str = "borsuk-cohere-native-baseline-config-v3";
const RESULT_SCHEMA: &str = "borsuk-cohere-native-baseline-result-v3";
// One 256-row SQ8 page at the fixed D1024 width, for reporting covered/bridge pages.
const SQ8_PAGE_BYTES: usize = 256 * (D + 12);

/// Which serving path answers each query. Required in every config: there is no default.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum Serving {
    /// Historical SOURCE nomination, then SQ8 under the unchanged baseline caps.
    Baseline {},
    /// Opt-in direct closure: no SOURCE I/O. These caps are explicit and independent of
    /// the baseline `max_query_*` limits.
    DirectClosure {
        max_sq8_bytes: usize,
        max_sq8_gets: usize,
    },
}
impl Serving {
    fn direct(self) -> Option<DirectClosureLimits> {
        match self {
            Self::Baseline {} => None,
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
    #[cfg(test)]
    Stub {
        store: Arc<dyn ObjectStore>,
        stats: Arc<std::sync::Mutex<NativeTransportStats>>,
    },
}
impl Reader {
    fn new(backend: &Backend) -> Result<Self> {
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
            } => Self::S3(
                OneAttemptS3::new_with_prefix(
                    bucket,
                    region,
                    ObjectPath::from(physical_prefix.as_str()),
                )
                .map_err(|e| format!("S3 reader: {e:?}"))?,
            ),
        })
    }
    fn store(&self) -> &dyn ObjectStore {
        match self {
            Self::Local(store) => store,
            Self::S3(reader) => reader.store(),
            #[cfg(test)]
            Self::Stub { store, .. } => store.as_ref(),
        }
    }
    fn snapshot(&self) -> Option<NativeTransportStats> {
        match self {
            Self::Local(_) => None,
            Self::S3(reader) => Some(reader.transport_stats()),
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
    backend: Backend,
    generation_prefix: String,
    generation_root_sha256: String,
    scratch_parent: PathBuf,
    requests: Artifact,
    truth: Artifact,
    native_source: NativeSource,
    max_memory_bytes: u64,
    #[serde(default = "default_fetch_parallelism")]
    fetch_parallelism: usize,
    serving: Serving,
}

fn default_fetch_parallelism() -> usize {
    16
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
fn validate_config(c: &Config, shape: Shape) -> Result<()> {
    require(
        matches!(c.fetch_parallelism, 16 | 32),
        "fetch_parallelism must be 16 or 32",
    )?;
    require(
        c.schema == CONFIG_SCHEMA,
        "config format marker must be borsuk-cohere-native-baseline-config-v3; older formats refuse",
    )?;
    if let Some(direct) = c.serving.direct() {
        // Sanity bounds only; the library admits the real envelope (modeled memory and
        // the planned cover) before any query allocation or I/O.
        require(
            (1..=MEMORY_CAP as usize).contains(&direct.max_sq8_bytes)
                && (1..=128).contains(&direct.max_sq8_gets),
            "direct closure caps must be explicit, positive and bounded",
        )?;
    }
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
    for path in [&c.scratch_parent, &c.requests.path, &c.truth.path] {
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
#[cfg(test)]
thread_local! {
    // Runs between the guarded metadata open and its read, without a race or sleep.
    static LOCAL_METADATA_OPEN_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce(&Path)>>> =
        std::cell::RefCell::new(None);
}
async fn metadata<T: serde::de::DeserializeOwned>(
    c: &Config,
    store: &dyn ObjectStore,
    name: &str,
    sha: &str,
    charge: &mut Charge,
) -> Result<T> {
    require(valid_sha(sha), "metadata SHA descriptor")?;
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
        return Ok(serde_json::from_slice(
            &checked_file(file, None, CONFIG_CAP, sha)?.0,
        )?);
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
    // Neither root nor plane body survives this call.
    Ok(serde_json::from_slice(&result?)?)
}
async fn bind_source(c: &Config, store: &dyn ObjectStore, charge: &mut Charge) -> Result<()> {
    #[derive(Deserialize)]
    struct Root {
        plane_manifest_sha256: String,
        sq8_object_sha256: String,
        sq8_object_key: String,
        sq8_etag: String,
    }
    let root: Root = metadata(c, store, "manifest.json", &c.generation_root_sha256, charge).await?;
    require(
        root.sq8_object_sha256 == c.native_source.sq8_sha256,
        "root SQ8 source binding",
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
    drop(root);
    let plane: SourcePlaneReceipt =
        metadata(c, store, "plane/manifest.json", &plane_sha, charge).await?;
    require(
        plane.rows == c.rows
            && plane.dimensions == D
            && !plane.query_or_truth_used
            && plane.source_sha256 == c.native_source.source_sha256
            && plane.sq8_sha256 == c.native_source.sq8_sha256
            && plane.source_order_sha256 == c.native_source.source_order_sha256,
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
        + [&c.scratch_parent, &c.requests.path, &c.truth.path]
            .iter()
            .map(|p| p.capacity())
            .sum::<usize>()
        + 2 * backend
        + size_of::<Reader>();
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
    // Native histogram has 900 possible codes; allow doubled Vec capacity and a
    // transient snapshot alongside the retained before/after pair. The fixed process
    // histogram is also charged. SDK/TLS RSS still requires an external cgroup gate.
    let transport = if matches!(&c.backend, Backend::S3 { .. }) {
        3 * (size_of::<NativeTransportStats>() + 2 * 900 * size_of::<(u16, u64)>())
            + 900 * size_of::<u64>()
    } else {
        0
    };
    descriptors + transport + size_of::<Progress>() + 5 * CONFIG_CAP + BLOCK + query.max(reduction)
}
fn limits(c: &Config) -> Result<TwoBitGenerationLimits> {
    Ok(TwoBitGenerationLimits {
        max_memory_bytes: MEMORY_CAP,
        max_active_queries: 1,
        max_query_bytes: 16_773_120,
        max_query_gets: 32,
        max_parallel_gets: c.fetch_parallelism,
        max_source_bytes: 64 * 1024 * 1024,
        max_source_gets: 128,
        max_parallel_source_gets: c.fetch_parallelism,
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
            #[cfg(test)]
            fail_query_output: false,
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
    binding_charge: Charge,
    query_wall_ns: u128,
    query_cpu_ns: i128,
    transport: TransportSpan,
    direct_memory: Option<DirectClosureMemory>,
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
    query_and_seal_with(c, shape, out, p, || Reader::new(&c.backend)).await
}
async fn query_and_seal_with(
    c: &Config,
    shape: Shape,
    out: &mut Output,
    p: &mut Progress,
    reader: impl FnOnce() -> Result<Reader>,
) -> Result<Seal> {
    validate_config(c, shape)?;
    p.stage = "requests";
    let (requests, request_identity) = checked(&c.requests, c.count * D * 4)?;
    // Authenticate and validate ALL rows before issuing even the first query.
    for ordinal in 0..c.count {
        request_row(&requests, ordinal).map_err(|e| format!("request {ordinal}: {e}"))?;
    }
    let admission = limits(c)?;
    p.stage = "backend";
    let reader = reader()?;
    let store = reader.store();
    p.stage = "native_source";
    p.transport.begin(&reader, p.stage, None);
    let binding = bind_source(c, store, &mut p.binding_charge).await;
    p.transport.finish(&reader);
    out.emit(
        &json!({"phase":"source_binding","transport":p.transport,"charges":p.binding_charge,"success":binding.is_ok(),
        "truth_opened":false}),
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
    require(
        generation.rows() == c.rows
            && generation.discovery_mode() == DiscoveryMode::Semantic
            && generation.semantic_profile() == Some(SemanticProfile::Native100k),
        "generation geometry/profile",
    )?;
    out.emit(
        &json!({"phase":"startup","metadata":generation.remote_open_stats(),
        "serving":c.serving,
        "library_cap_bytes":MEMORY_CAP,"caller_pinned_bytes":admission.already_pinned_bytes,
        "codec_scratch_bytes":532480,"trace_scratch_bytes":TwoBitPlanTrace::scratch_bytes(c.rows),
        "query_scratch_bytes":admission.max_query_scratch_bytes,"truth_opened":false}),
    )?;
    if let Some(direct) = c.serving.direct() {
        // Exact modeled memory must fit before any direct query: the existing model is kept
        // whole (no SOURCE credit) and a full direct query budget is added.
        p.stage = "direct_admission";
        let memory = generation.direct_closure_memory(direct)?;
        let admitted = memory.total_bytes <= memory.cap_bytes;
        out.emit(
            &json!({"phase":"direct_admission","limits":c.serving,"memory":memory,
            "admitted":admitted,"truth_opened":false}),
        )?;
        p.direct_memory = Some(memory);
        if !admitted {
            return Err(ResourceReject {
                reason: DirectClosureRejection::ModeledMemory,
                message: format!(
                    "modeled {} bytes exceed the {} byte cap before any query",
                    memory.total_bytes, memory.cap_bytes
                ),
            }
            .into());
        }
    }
    for ordinal in 0..c.count {
        p.stage = "query";
        let query = request_row(&requests, ordinal)?;
        p.transport.begin(&reader, p.stage, Some(ordinal));
        let wall = Instant::now();
        let cpu = cpu_ns();
        let result = match c.serving.direct() {
            None => {
                generation
                    .diagnostic_search_with_store(store, &query, K)
                    .await
            }
            Some(direct) => {
                generation
                    .diagnostic_direct_closure_search_with_store(store, &query, K, direct)
                    .await
            }
        };
        let wall_ns = wall.elapsed().as_nanos();
        let cpu_ns = cpu_ns() - cpu;
        p.transport.finish(&reader);
        p.query_wall_ns += wall_ns;
        p.query_cpu_ns += cpu_ns;
        let (result, trace) = match result {
            Ok(result) => result,
            Err(e) => {
                let (source, sq8) = e.read_stats();
                p.charges.add(e.router_stats(), source, sq8)?;
                let rejection = e.direct_resource_rejection();
                out.emit(
                    &json!({"phase":"query_failure","ordinal":ordinal,"error":e.to_string(),
                    "resource_rejection":rejection,
                    "charges_so_far":p.charges,"sum_so_far":p.charges.sum(),"stages":e.stages(),
                    "query_wall_ns":wall_ns,"query_process_cpu_ns":cpu_ns,"truth_opened":false,
                    "transport":p.transport}),
                )?;
                let message = format!("native query {ordinal}: {e}");
                return Err(match rejection {
                    Some(reason) => ResourceReject { reason, message }.into(),
                    None => message.into(),
                });
            }
        };
        if c.serving.direct().is_some() {
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
        // Full ordered range identity, not just totals: `selected_pages` is the required
        // closure (direct) or the admitted pages (baseline); bridges live only in `ranges`.
        #[derive(Serialize)]
        struct PlanRecord<'a> {
            selected_pages: &'a [usize],
            ranges: Vec<[usize; 2]>,
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
            .map(|r| r.len().div_ceil(SQ8_PAGE_BYTES))
            .sum::<usize>();
        let plan = PlanRecord {
            selected_pages: &result.plan.selected_pages,
            ranges: result
                .plan
                .ranges
                .iter()
                .map(|r| [r.start, r.end])
                .collect(),
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
            source_nomination_skipped: bool,
            // What `stages.planning` measures: baseline = SOURCE nomination plus cover;
            // direct = the cover computation alone (SOURCE nomination is skipped).
            planning_scope: &'static str,
            returned: &'a [Hit],
            returned_count: usize,
            underfill: bool,
            charges: &'a Charges,
            sum: Charge,
            plan: PlanRecord<'a>,
            stages: &'a borsuk::two_bit_generation::QueryStages,
            query_wall_ns: u128,
            query_process_cpu_ns: i128,
            trace: &'a TwoBitPlanTrace,
            transport: &'a TransportSpan,
        }
        #[cfg(test)]
        if out.fail_query_output {
            out.cap = out.bytes + 17; // Leave an actual partial query record.
        }
        out.emit(&Record {
            phase: "query",
            ordinal,
            truth_opened: false,
            serving: c.serving,
            source_nomination_skipped: result.source_nomination_skipped,
            planning_scope: if result.source_nomination_skipped {
                "direct_cover_only"
            } else {
                "source_nomination_and_cover"
            },
            returned_count: returned.len(),
            underfill,
            returned: &returned,
            charges: &charges,
            sum: charges.sum(),
            plan,
            stages: &result.stages,
            query_wall_ns: wall_ns,
            query_process_cpu_ns: cpu_ns,
            trace: &trace,
            transport: &p.transport,
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
        "binding_charge":p.binding_charge,
        "serving":c.serving,"direct_memory":p.direct_memory,
        "query_wall_ns":p.query_wall_ns,"query_process_cpu_ns":p.query_cpu_ns,
        "physical_s3_measured":false,"external_gate_required":true}),
    )
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
        |c, shape, out, p| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(query_and_seal(c, shape, out, p))
        },
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
        out.emit(&json!({"schema":RESULT_SCHEMA,"phase":"identity","config_sha256":config_sha,
            "fetch_parallelism":configured.as_ref().ok().map(|c| c.fetch_parallelism),
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
            "external_gate_required":true,"truth_opened":false}))?;
        let c = configured?;
        out.emit(&json!({"phase":"bound_inputs","dataset":c.dataset,"revision":c.revision,
            "fetch_parallelism":c.fetch_parallelism,"source_cache":"off","serving":c.serving,
            "metric":c.metric,"tie_rule":c.tie_rule,"rows":c.rows,"dimensions":D,"count":c.count,"k":K,
            "corpus_source_first":c.corpus_source_first,"query_source_first":c.query_source_first,
            "profile":c.profile,"backend":c.backend,"generation_prefix":c.generation_prefix,
            "credential_source":matches!(&c.backend, Backend::S3 { .. }).then_some("imds_instance_role_only"),
            "generation_root_sha256":c.generation_root_sha256,
            "requests_bytes":c.requests.bytes,"requests_sha256":c.requests.sha256,
            "truth_bytes":c.truth.bytes,"truth_sha256":c.truth.sha256,
            "native_source_sha256":c.native_source.source_sha256,"native_sq8_sha256":c.native_source.sq8_sha256,
            "native_order_sha256":c.native_source.source_order_sha256,"truth_opened":false}))?;
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
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?
                    .block_on(query_and_seal(c, shape, out, p))
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
        json!({"schema":CONFIG_SCHEMA,"dataset":DATASET,"revision":REVISION,
            "metric":"cosine","tie_rule":"corpus_ordinal_ascending",
            "corpus_source_first":0,"query_source_first":shape.rows,"rows":shape.rows,
            "dimensions":D,"count":shape.count,"k":K,"profile":"native100k",
            "backend":{"kind":"local","store_root":dir.join("store")},"generation_prefix":"semantic/index",
            "generation_root_sha256":"a".repeat(64),"scratch_parent":dir.join("scratch"),
            "requests":{"path":dir.join("requests.f32"),"bytes":shape.count*D*4,"sha256":"b".repeat(64)},
            "truth":{"path":dir.join("truth.u64"),"bytes":shape.count*K*8,"sha256":"c".repeat(64)},
            "native_source":{"source_sha256":"d".repeat(64),"sq8_sha256":"e".repeat(64),
                "source_order_sha256":"f".repeat(64)},"max_memory_bytes":MEMORY_CAP,
            "serving":{"mode":"baseline"}})
    }
    fn write_config(dir: &Path, value: &Value) -> (PathBuf, String) {
        let path = dir.join("config.json");
        let body = serde_json::to_vec(value).unwrap();
        std::fs::write(&path, &body).unwrap();
        (path, hash(&body))
    }
    #[test]
    fn cold_fetch_parallelism_selector_is_explicit_and_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let shape = Shape::tiny(32);
        let mut value = config_value(dir.path(), shape);
        let default: Config = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(default.fetch_parallelism, 16);
        for parallelism in [0, 1, 16, 17, 32, 33, usize::MAX] {
            value["fetch_parallelism"] = json!(parallelism);
            let config: Config = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(
                validate_config(&config, shape).is_ok(),
                matches!(parallelism, 16 | 32)
            );
            if matches!(parallelism, 16 | 32) {
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
        let body = br#"{"guarded":true}"#;
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
        let value: Value = tokio::runtime::Builder::new_current_thread()
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
        assert_eq!(value, json!({"guarded":true}));
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
                        0x3d80_0000_u32
                    } else if rank < 9 {
                        0x3f00_0000
                    } else {
                        0x3f80_0000
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
                    if fault == "output" { 2 } else { 1 },
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
                assert_eq!(rows[0]["charges"]["submitted_gets"], 2);
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
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(query_and_seal_with(c, shape, out, p, || {
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
                        }))
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

    // ===== Direct-closure serving mode: explicit format, controls, resource outcomes =====
    fn with_direct(mut value: Value, bytes: usize, gets: usize) -> Value {
        value["serving"] =
            json!({"mode":"direct_closure","max_sq8_bytes":bytes,"max_sq8_gets":gets});
        value
    }
    fn run_status(dir: &Path, name: &str, value: &Value, shape: Shape) -> (Status, Vec<Value>) {
        let (path, sha) = write_config(dir, value);
        let output = dir.join(name);
        let status = execute_paths_status(&path, &sha, &output, shape, |c, shape, out, p| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(query_and_seal(c, shape, out, p))
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
        let planner =
            1_048_576 + TwoBitPlanTrace::scratch_bytes(257) as u64 + 4 * D as u64 + 512 * 2;
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
            assert_eq!(query["returned_count"], 10);
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
}
