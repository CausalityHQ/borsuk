//! Offline reduction of four completed native A1/B1/B2/A2 result files.
//! This does not qualify resources, cost, cache state, or a vendor comparison.

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
    returned: [Hit; K],
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
    returned: [Hit; K],
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
    require(valid_sha(sha), "result lowercase SHA256")?;
    let mut rows = Rows::open(path)?;
    let identity: Identity = rows.row("identity")?;
    identity.validate()?;
    let inputs: Inputs = rows.row("bound_inputs")?;
    inputs.validate()?;
    #[derive(Deserialize)]
    struct Startup {
        truth_opened: bool,
    }
    let startup: Startup = rows.row("startup")?;
    require(!startup.truth_opened, "startup truth boundary")?;
    let mut samples = Vec::with_capacity(COUNT);
    let mut charges = Charges::default();
    let mut stages = StageTotals::default();
    let (mut wall, mut cpu) = (0, 0);
    for ordinal in 0..COUNT {
        let q: Query = rows.row("query")?;
        let trace_sha256 = trace_fingerprint(&rows.line)?;
        require(
            q.ordinal == ordinal && !q.truth_opened && q.returned_count == K && !q.underfill,
            "query order/truth/count/underfill",
        )?;
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
        require(q.charges.sum()? == q.sum, "query charge total")?;
        require(
            q.query_wall_ns > 0 && q.query_wall_ns <= i64::MAX as u64,
            "positive bounded query wall",
        )?;
        charges.add(q.charges)?;
        stages.add(&q.stages, q.query_wall_ns)?;
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
                && recall.hits10 <= K as u64
                && recall.recall10 == recall.hits10 as f64 / K as f64
                && recall.returned_count == K
                && !recall.underfill,
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
    require(
        t.status == "MEASURED"
            && t.complete
            && t.queries == COUNT
            && t.k == K
            && t.total_hits10 == hits
            && t.recall_numerator == hits
            && t.recall_denominator == (COUNT * K) as u64
            && t.mean_recall10 == hits as f64 / (COUNT * K) as f64
            && t.underfilled_queries == 0
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
    let file_identity = rows.finish(path, sha)?;
    Ok(Run {
        identity,
        inputs,
        samples,
        stages,
        terminal: t,
        file_identity,
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
    let total = samples.iter().try_fold(0, |sum, &n| plus(sum, n))?;
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    // Same integer nearest-rank rule as native_ann_100k_qualify.rs.
    let percentile = |p: usize| sorted[(sorted.len() * p).div_ceil(100).saturating_sub(1)];
    Ok(Statistics {
        count: samples.len(),
        query_wall_ns: total,
        sequential_qps: samples.len() as f64 * 1e9 / total as f64,
        p50_ms: percentile(50) as f64 / 1e6,
        p90_ms: percentile(90) as f64 / 1e6,
        p95_ms: percentile(95) as f64 / 1e6,
        p99_ms: percentile(99) as f64 / 1e6,
        p90_ns: percentile(90),
        p95_ns: percentile(95),
    })
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
    let report = match compare(paths) {
        Ok(report) => report,
        Err(e) => {
            json!({"schema":"borsuk-compare-native-replay-v1","status":"INVALID","complete":false,
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
    use std::os::unix::fs::symlink;

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
                serde_json::to_writer(&mut bytes, &prefix).unwrap();
                assert_eq!(bytes.pop(), Some(b'}'));
                bytes.extend_from_slice(b",\"trace\":");
                serde_json::to_writer(&mut bytes, &trace).unwrap();
                bytes.push(b'}');
            } else {
                serde_json::to_writer(&mut bytes, row).unwrap();
            }
            bytes.push(b'\n');
        }
        bytes
    }

    fn authenticate(rows: &mut [Value]) {
        let prefix = encode(&rows[..1003]);
        rows[1003]["prefix_bytes"] = json!(prefix.len());
        rows[1003]["prefix_sha256"] = json!(sha(&prefix));
        let sealed = encode(&rows[..1004]);
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
