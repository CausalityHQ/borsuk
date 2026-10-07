//! Once-only S3 Vectors reference. Backend cache state is unobservable.
//!
//! CLI: check_s3_vectors_baseline <admit|publish|measure|reduce|cleanup> CONFIG SHA256
//! Receipts are create-only; calls.jsonl is append-only under CONFIG.state_dir.
//! Cleanup has at most eight numbered attempts; measurement never resumes.
//! Keep this directory across processes: it is the no-resume/ownership ledger.
//! Admission opens truth; measurement never does. Reduction requires a complete
//! file-and-directory-fsynced response seal before it can open truth.
//! Partial creates with ownership receipts but missing creation identities fail
//! cleanup closed; they require explicit operator reconciliation, never guessed deletion.
//! Reconcile create-intent, returned ARN/request ID and CloudTrail/current resource
//! identity manually; do not synthesize identity receipts from a later Get call.
//! Completed cleanup is historical: another invocation succeeds without API calls.
//! Native qualification and Cargo.lock resolution belong to the remote build gate.
use aws_sdk_s3vectors::{
    Client,
    config::{Region, retry::RetryConfig, timeout::TimeoutConfig},
    types::{DataType, DistanceMetric, PutInputVector, VectorData},
};
use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        interceptors::{
            Intercept,
            context::{
                AfterDeserializationInterceptorContextRef,
                BeforeDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextRef,
            },
        },
        runtime_components::RuntimeComponents,
    },
};
use aws_smithy_types::config_bag::ConfigBag;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    error::Error,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const D: usize = 1024;
const K: usize = 10;
const CONFIG_CAP: u64 = 65_536;
const LINE_CAP: usize = 128 * 1024;
const OUTPUT_CAP: u64 = 32 * 1024 * 1024;
// Retained evidence only: the SDK buffers before this check. Native execution
// requires an external 512 MiB memory limit, swap disabled (recorded by root).
const RESPONSE_CAP: usize = 4 * 1024 * 1024;
const CALL_CAP: usize = 4096;
const LIST_PAGE_CAP: usize = 1024;
const CLEANUP_ATTEMPT_CAP: usize = 8;
const LEDGER_CAP: u64 = 4 * 1024 * 1024;
const CONFIG_SCHEMA: &str = "borsuk-s3-vectors-config-v1";
const RECEIPT_SCHEMA: &str = "borsuk-s3-vectors-receipt-v1";
const DATASET: &str = "CohereLabs/wikipedia-2023-11-embed-multilingual-v3";
const REVISION: &str = "ade45fb52bd549f5e8c065636fe4160a43c2af36";
const CORPUS_SHA: &str = "3c95fa49a7d3f9d4bf6178f5ac2493e700a30fbcfe91da97a5fcf16a1f5fc09c";
const QUERY_SHA: &str = "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e";
const TRUTH_SHA: &str = "479064239b698a2b8094c7838b1bb01af6eff5ea6eee4736692971849fdcfb2c";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    path: PathBuf,
    bytes: u64,
    sha256: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    mode: String,
    dataset: String,
    revision: String,
    rows: usize,
    count: usize,
    dimensions: usize,
    k: usize,
    metric: String,
    corpus: Artifact,
    queries: Artifact,
    truth: Artifact,
    state_dir: PathBuf,
    run_id: String,
    profile: String,
    region: String,
    account_id: String,
    bucket: String,
    index: String,
    idle_seconds: u64,
    query_interval_ms: u64,
    host_identity: String,
}
struct Context {
    c: Config,
    sha: String,
}
impl Context {
    fn path(&self, name: &str) -> PathBuf {
        self.c.state_dir.join(name)
    }
    fn save(&self, name: &str, data: Value) -> Result<()> {
        atomic_json(
            &self.path(name),
            &json!({"schema":RECEIPT_SCHEMA,"config_sha256":self.sha,"kind":name,"data":data}),
        )
    }
    fn receipt(&self, name: &str) -> Result<Value> {
        let bytes = read_small(&self.path(name), CONFIG_CAP)?;
        let value: Value = serde_json::from_slice(&bytes)?;
        require(
            value["schema"] == RECEIPT_SCHEMA
                && value["config_sha256"] == self.sha
                && value["kind"] == name,
            "receipt schema/config/kind",
        )?;
        Ok(value["data"].clone())
    }
}
fn require(ok: bool, message: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(message.into()) }
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn valid_sha(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn now_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}
fn elapsed_ns(start: Instant) -> Result<u64> {
    Ok(u64::try_from(start.elapsed().as_nanos())?)
}
fn flags() -> i32 {
    (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}
fn identity(file: &File) -> Result<Identity> {
    let m = file.metadata()?;
    require(m.is_file(), "regular file required")?;
    Ok(Identity {
        dev: m.dev(),
        ino: m.ino(),
        len: m.len(),
        mtime: (m.mtime(), m.mtime_nsec()),
        ctime: (m.ctime(), m.ctime_nsec()),
    })
}
fn regular(path: &Path, cap: u64) -> Result<File> {
    #[cfg(test)]
    WATCH_TRUTH.with(|watch| {
        if watch.borrow().as_deref() == Some(path) {
            TRUTH_OPENS.with(|n| n.set(n.get() + 1));
        }
    });
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(flags())
        .open(path)?;
    require(
        identity(&file)?.len > 0 && identity(&file)?.len <= cap,
        "file length/cap",
    )?;
    Ok(file)
}
fn directory(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(flags() | rustix::fs::OFlags::DIRECTORY.bits() as i32)
        .open(path)?)
}
fn read_small(path: &Path, cap: u64) -> Result<Vec<u8>> {
    let mut f = regular(path, cap)?;
    let before = identity(&f)?;
    let mut bytes = vec![0; usize::try_from(before.len)?];
    f.read_exact(&mut bytes)?;
    require(
        f.read(&mut [0])? == 0 && identity(&f)? == before,
        "input changed/EOF",
    )?;
    Ok(bytes)
}
fn atomic_json(path: &Path, value: &Value) -> Result<()> {
    let parent = path.parent().ok_or("receipt parent")?;
    let dir = directory(parent)?;
    let bytes = serde_json::to_vec(value)?;
    require(bytes.len() < CONFIG_CAP as usize, "receipt cap")?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    tmp.write_all(&bytes)?;
    tmp.write_all(b"\n")?;
    tmp.as_file().sync_all()?;
    // persist_noclobber never overwrites another attempt's receipt.
    let file = tmp.persist_noclobber(path).map_err(|e| e.error)?;
    file.sync_all()?;
    let current = directory(parent)?.metadata()?;
    let original = dir.metadata()?;
    require(
        current.dev() == original.dev() && current.ino() == original.ino(),
        "receipt directory changed",
    )?;
    dir.sync_all()?;
    Ok(())
}
fn validate_config(c: &Config) -> Result<()> {
    let full = c.mode == "measurement";
    require(full || c.mode == "canary", "mode measurement/canary")?;
    require(
        c.schema == CONFIG_SCHEMA
            && c.dataset == DATASET
            && c.revision == REVISION
            && c.dimensions == D
            && c.k == K
            && c.metric == "cosine",
        "schema/dataset/metric/D/k",
    )?;
    require(
        (c.rows, c.count) == if full { (100_000, 1000) } else { (257, 2) },
        "frozen geometry",
    )?;
    require(
        c.profile == "causality" && c.region == "eu-central-1",
        "explicit profile/region",
    )?;
    require(
        c.run_id.len() == 32
            && c.run_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "run_id lowercase hex32",
    )?;
    require(
        c.bucket == format!("borsuk-s3v-{}", c.run_id)
            && c.index == format!("baseline-{}", c.run_id),
        "fresh dedicated names",
    )?;
    require(
        c.account_id.len() == 12 && c.account_id.bytes().all(|b| b.is_ascii_digit()),
        "account_id",
    )?;
    require(
        c.idle_seconds == if full { 3600 } else { 0 } && c.query_interval_ms == 0,
        "frozen idle/cadence",
    )?;
    require(
        !c.host_identity.is_empty() && c.host_identity.len() <= 1024,
        "host/instance identity",
    )?;
    for path in [&c.state_dir, &c.corpus.path, &c.queries.path, &c.truth.path] {
        require(
            path.is_absolute()
                && path.as_os_str().len() <= 4096
                && !path
                    .components()
                    .any(|p| matches!(p, std::path::Component::ParentDir)),
            "absolute bounded path without parent traversal",
        )?;
    }
    require(
        c.corpus.path != c.queries.path
            && c.corpus.path != c.truth.path
            && c.queries.path != c.truth.path,
        "distinct artifact paths",
    )?;
    for (a, n) in [
        (&c.corpus, c.rows * D * 4),
        (&c.queries, c.count * D * 4),
        (&c.truth, c.count * K * 8),
    ] {
        require(
            a.bytes == n as u64 && valid_sha(&a.sha256),
            "artifact SHA/length/geometry",
        )?;
    }
    if full {
        require(
            c.corpus.sha256 == CORPUS_SHA
                && c.queries.sha256 == QUERY_SHA
                && c.truth.sha256 == TRUTH_SHA,
            "frozen original raw corpus/query/truth SHA",
        )?;
    }
    directory(&c.state_dir)?;
    Ok(())
}
fn load(path: &Path, sha: &str) -> Result<Context> {
    require(valid_sha(sha), "config SHA")?;
    let bytes = read_small(path, CONFIG_CAP)?;
    require(hash(&bytes) == sha, "config SHA mismatch")?;
    let c: Config = serde_json::from_slice(&bytes)?;
    validate_config(&c)?;
    Ok(Context { c, sha: sha.into() })
}
fn open_artifact(a: &Artifact) -> Result<File> {
    let f = regular(&a.path, a.bytes)?;
    require(identity(&f)?.len == a.bytes, "exact artifact length")?;
    Ok(f)
}
fn vector(bytes: &[u8]) -> Result<Vec<f32>> {
    require(bytes.len() == D * 4, "vector dimension")?;
    let values: Vec<f32> = bytes
        .chunks_exact(4)
        .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
        .collect();
    require(
        values.iter().all(|v| v.is_finite())
            && values.iter().map(|v| f64::from(*v).powi(2)).sum::<f64>() > 0.0,
        "finite nonzero vector",
    )?;
    Ok(values)
}
fn read_vector(f: &mut File) -> Result<Vec<f32>> {
    let mut bytes = [0; D * 4];
    f.read_exact(&mut bytes)?;
    vector(&bytes)
}
fn scan(a: &Artifact, truth: bool, rows: usize) -> Result<Identity> {
    let mut f = open_artifact(a)?;
    let before = identity(&f)?;
    let width = if truth { K * 8 } else { D * 4 };
    let mut row = vec![0; width];
    let mut digest = Sha256::new();
    require(a.bytes % width as u64 == 0, "row alignment")?;
    for _ in 0..a.bytes / width as u64 {
        f.read_exact(&mut row)?;
        digest.update(&row);
        if truth {
            truth_row(&row, rows)?;
        } else {
            vector(&row)?;
        }
    }
    require(
        f.read(&mut [0])? == 0
            && identity(&f)? == before
            && format!("{:x}", digest.finalize()) == a.sha256,
        "artifact SHA/EOF/identity",
    )?;
    Ok(before)
}
fn truth_row(bytes: &[u8], rows: usize) -> Result<Vec<u64>> {
    require(bytes.len() == K * 8, "truth row width")?;
    let ids: Vec<u64> = bytes
        .chunks_exact(8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .collect();
    require(
        ids.iter().all(|&id| id < rows as u64) && ids.iter().collect::<BTreeSet<_>>().len() == K,
        "truth range/duplicates",
    )?;
    Ok(ids)
}
fn input_identities(c: &Config, include_truth: bool) -> Result<Value> {
    let corpus = scan(&c.corpus, false, c.rows)?;
    let queries = scan(&c.queries, false, c.rows)?;
    require(
        (corpus.dev, corpus.ino) != (queries.dev, queries.ino),
        "corpus/query alias",
    )?;
    let truth = if include_truth {
        Some(scan(&c.truth, true, c.rows)?)
    } else {
        None
    };
    if let Some(ref t) = truth {
        require(
            (t.dev, t.ino) != (corpus.dev, corpus.ino)
                && (t.dev, t.ino) != (queries.dev, queries.ino),
            "truth aliases vectors",
        )?;
    }
    Ok(json!({"corpus":corpus,"queries":queries,"truth":truth}))
}
fn admit(ctx: &Context) -> Result<()> {
    validate_config(&ctx.c)?;
    ctx.save("admission.json",json!({"inputs":input_identities(&ctx.c,true)?,"corpus_sha256":ctx.c.corpus.sha256,"queries_sha256":ctx.c.queries.sha256,"truth_sha256":ctx.c.truth.sha256,"truth_validated":true,"ordinal_keys":"six_digit_zero_based","at_ms":now_ms()?}))
}
fn admitted(ctx: &Context, truth: bool) -> Result<()> {
    validate_config(&ctx.c)?;
    let a = ctx.receipt("admission.json")?;
    require(
        a["truth_validated"] == true
            && a["corpus_sha256"] == ctx.c.corpus.sha256
            && a["queries_sha256"] == ctx.c.queries.sha256
            && a["truth_sha256"] == ctx.c.truth.sha256,
        "admission pins",
    )?;
    let current = input_identities(&ctx.c, truth)?;
    for key in if truth {
        &["corpus", "queries", "truth"][..]
    } else {
        &["corpus", "queries"][..]
    } {
        // A separate measurement host has different inode/device identities.
        // Both scans authenticate all bytes; identities guard each local read.
        require(
            current[key]["len"] == a["inputs"][key]["len"],
            "admitted input length changed",
        )?;
    }
    Ok(())
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Capture {
    attempts: u32,
    request_bytes: usize,
    status: Option<u16>,
    request_id: Option<String>,
    body_bytes: usize,
    body_sha256: Option<String>,
    body: Option<String>,
    truncated: bool,
}
#[derive(Debug, Default, Clone)]
struct Tap(Arc<Mutex<Capture>>);
impl Intercept for Tap {
    fn name(&self) -> &'static str {
        "s3-vectors-receipt"
    }
    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> std::result::Result<(), BoxError> {
        let bytes = context
            .request()
            .body()
            .bytes()
            .ok_or("non-buffered request")?;
        require(
            bytes.len() <= 20 * 1024 * 1024,
            "serialized request exceeds 20 MiB",
        )?;
        let mut capture = self.0.lock().map_err(|_| "capture lock poisoned")?;
        require(capture.attempts == 0, "second transmit attempt forbidden")?;
        capture.attempts += 1;
        capture.request_bytes = bytes.len();
        Ok(())
    }
    fn read_before_deserialization(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> std::result::Result<(), BoxError> {
        let response = context.response();
        let mut c = self.0.lock().map_err(|_| "capture lock poisoned")?;
        c.status = Some(response.status().as_u16());
        c.request_id = response
            .headers()
            .get("x-amzn-requestid")
            .or_else(|| response.headers().get("x-amz-request-id"))
            .map(str::to_owned);
        Ok(())
    }
    fn read_after_deserialization(
        &self,
        context: &AfterDeserializationInterceptorContextRef<'_>,
        _runtime: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> std::result::Result<(), BoxError> {
        let response = context.response();
        let mut c = self.0.lock().map_err(|_| "capture lock poisoned")?;
        if let Some(bytes) = response.body().bytes() {
            c.body_bytes = bytes.len();
            c.body_sha256 = Some(hash(bytes));
            c.truncated = bytes.len() > RESPONSE_CAP;
            c.body =
                Some(String::from_utf8_lossy(&bytes[..bytes.len().min(RESPONSE_CAP)]).into_owned());
        }
        Ok(())
    }
}
fn client_builder(c: &Config, tap: Tap) -> aws_sdk_s3vectors::config::Builder {
    aws_sdk_s3vectors::config::Builder::new()
        .behavior_version_latest()
        .region(Region::new(c.region.clone()))
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(5))
                .read_timeout(Duration::from_secs(30))
                .operation_attempt_timeout(Duration::from_secs(30))
                .operation_timeout(Duration::from_secs(30))
                .build(),
        )
        .interceptor(tap)
}
fn sts_builder(c: &Config, tap: Tap) -> aws_sdk_sts::config::Builder {
    aws_sdk_sts::config::Builder::new()
        .behavior_version_latest()
        .region(Region::new(c.region.clone()))
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(5))
                .read_timeout(Duration::from_secs(30))
                .operation_attempt_timeout(Duration::from_secs(30))
                .operation_timeout(Duration::from_secs(30))
                .build(),
        )
        .interceptor(tap)
}
struct Service {
    client: Client,
    sts: aws_sdk_sts::Client,
    tap: Tap,
}
impl Service {
    fn new(c: &Config) -> Result<Self> {
        // A named profile is the only credential source; environment credentials
        // and endpoint overrides cannot silently change the registered identity.
        for (key, _) in std::env::vars_os() {
            require(
                !key.to_string_lossy().starts_with("AWS_ENDPOINT_URL"),
                "endpoint environment override forbidden",
            )?;
        }
        let credentials = aws_sdk_s3vectors::config::SharedCredentialsProvider::new(
            aws_config::profile::ProfileFileCredentialsProvider::builder()
                .profile_name(&c.profile)
                .build(),
        );
        let tap = Tap::default();
        let client = Client::from_conf(
            client_builder(c, tap.clone())
                .credentials_provider(credentials.clone())
                .build(),
        );
        let sts = aws_sdk_sts::Client::from_conf(
            sts_builder(c, tap.clone())
                .credentials_provider(credentials)
                .build(),
        );
        Ok(Self { client, sts, tap })
    }
    fn begin(&self) -> Result<()> {
        *self.tap.0.lock().map_err(|_| "capture lock poisoned")? = Capture::default();
        Ok(())
    }
    fn capture(&self) -> Result<Capture> {
        Ok(self
            .tap
            .0
            .lock()
            .map_err(|_| "capture lock poisoned")?
            .clone())
    }
    fn body(&self) -> Result<Value> {
        let c = self.capture()?;
        require(
            !c.truncated && c.attempts == 1 && c.status == Some(200),
            "service response/attempt bound",
        )?;
        Ok(serde_json::from_str(
            c.body.as_deref().ok_or("missing response body")?,
        )?)
    }
}
// This small append-only ledger is shared by all command receipts. A durable
// reservation precedes dispatch; an interrupted request consumes one allowance
// without inventing an observed attempt. The descriptor lock serializes commands.
struct Calls {
    file: File,
    reserved: usize,
    observed: usize,
    pending: BTreeSet<usize>,
}
impl Calls {
    fn open(ctx: &Context) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .mode(0o600)
            .custom_flags(flags())
            .open(ctx.path("calls.jsonl"))?;
        file.try_lock()
            .map_err(|e| format!("call ledger is locked: {e}"))?;
        require(identity(&file)?.len <= LEDGER_CAP, "call ledger byte cap")?;
        let mut ledger = Self {
            file,
            reserved: 0,
            observed: 0,
            pending: BTreeSet::new(),
        };
        let header = json!({"schema":"borsuk-s3-vectors-calls-v1","config_sha256":ctx.sha,"max_reserved_calls":CALL_CAP});
        if identity(&ledger.file)?.len == 0 {
            ledger.append(&header)?;
            directory(&ctx.c.state_dir)?.sync_all()?;
        } else {
            let mut reader = BufReader::new(ledger.file.try_clone()?);
            require(
                line(&mut reader)? == Some(header),
                "call ledger config/schema/cap",
            )?;
            while let Some(row) = line(&mut reader)? {
                let id = usize::try_from(row["call_id"].as_u64().ok_or("ledger call ID")?)?;
                match row["phase"].as_str() {
                    Some("reserved") => {
                        require(
                            id == ledger.reserved + 1 && id <= CALL_CAP,
                            "ledger reservation sequence/cap",
                        )?;
                        ledger.reserved = id;
                        ledger.pending.insert(id);
                    }
                    Some("observed") => {
                        let attempts =
                            usize::try_from(row["attempts"].as_u64().ok_or("ledger attempts")?)?;
                        require(
                            attempts <= 1 && ledger.pending.remove(&id),
                            "ledger completion/attempts",
                        )?;
                        ledger.observed += attempts;
                    }
                    _ => return Err("call ledger phase".into()),
                }
            }
        }
        Ok(ledger)
    }
    fn append(&mut self, row: &Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(row)?;
        bytes.push(b'\n');
        require(
            identity(&self.file)?.len + bytes.len() as u64 <= LEDGER_CAP,
            "call ledger byte cap",
        )?;
        self.file.write_all(&bytes)?;
        self.file.sync_all()?;
        Ok(())
    }
    fn reserve(&mut self, command: &str, operation: &str) -> Result<usize> {
        require(
            self.reserved < CALL_CAP,
            "aggregate service reservation cap",
        )?;
        let id = self.reserved + 1;
        self.append(
            &json!({"phase":"reserved","call_id":id,"command":command,"operation":operation}),
        )?;
        self.reserved = id;
        self.pending.insert(id);
        Ok(id)
    }
    fn finish(&mut self, id: usize, capture: &Capture) -> Result<()> {
        require(
            capture.attempts <= 1 && self.pending.contains(&id),
            "call completion/attempt bound",
        )?;
        self.append(&json!({"phase":"observed","call_id":id,"attempts":capture.attempts,"status":capture.status}))?;
        self.pending.remove(&id);
        self.observed += capture.attempts as usize;
        Ok(())
    }
    fn summary(&self) -> Value {
        json!({"scope":"all commands for this config","max_reserved_calls":CALL_CAP,"reserved_calls":self.reserved,
            "observed_transmit_attempts":self.observed,"unconfirmed_reservations":self.pending.len(),
            "accounting":"all reservations consume allowance; only captured transmits count as observed; credentials excluded"})
    }
}
struct Output {
    file: File,
    dir: File,
    path: PathBuf,
    bytes: u64,
    digest: Sha256,
    calls: Calls,
}
impl Output {
    fn create(ctx: &Context, name: &str) -> Result<Self> {
        Self::with_calls(&ctx.path(name), Calls::open(ctx)?)
    }
    fn with_calls(path: &Path, calls: Calls) -> Result<Self> {
        let dir = directory(path.parent().ok_or("output parent")?)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(flags())
            .open(path)?;
        Ok(Self {
            file,
            dir,
            path: path.into(),
            bytes: 0,
            digest: Sha256::new(),
            calls,
        })
    }
    fn begin_call(&mut self, s: &Service, operation: &str) -> Result<usize> {
        s.begin()?;
        let name = self
            .path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or("command log name")?
            .to_owned();
        self.calls.reserve(&name, operation)
    }
    fn emit(&mut self, v: &Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(v)?;
        bytes.push(b'\n');
        require(
            bytes.len() <= LINE_CAP && self.bytes + bytes.len() as u64 <= OUTPUT_CAP,
            "output line/total cap",
        )?;
        self.file.write_all(&bytes)?;
        self.digest.update(&bytes);
        self.bytes += bytes.len() as u64;
        Ok(())
    }
    fn sync(&self) -> Result<()> {
        self.file.sync_all()?;
        let m = directory(self.path.parent().ok_or("output parent")?)?.metadata()?;
        let old = self.dir.metadata()?;
        require(
            (m.dev(), m.ino()) == (old.dev(), old.ino()),
            "output directory identity changed",
        )?;
        self.dir.sync_all()?;
        Ok(())
    }
    fn terminal(&mut self, status: &str, error: Option<String>) -> Result<()> {
        // Publication can fail before or after its truth scan; do not assert a
        // false unopened state. The measurement path's guarantee stays explicit.
        let truth = if self.path.file_name().is_some_and(|p| p == "publish.jsonl") {
            Value::Null
        } else {
            json!(false)
        };
        self.emit(&json!({"schema":RECEIPT_SCHEMA,"phase":"terminal","status":status,"error":error,"truth_opened":truth,"calls":self.calls.summary()}))?;
        self.sync()
    }
}
fn error_text(e: &impl std::fmt::Debug) -> String {
    format!("{e:?}").chars().take(2048).collect()
}
// Each call is recorded before returning an SDK failure. No operation is retried
// in application code; the SDK interceptor records actual transmit attempts.
macro_rules! call {
    ($s:expr,$out:expr,$name:expr,$request:expr) => {{
        let call_id=$out.begin_call($s,$name)?; let start=Instant::now(); let result=$request.send().await;
        let mut capture=$s.capture()?; capture.body=None;
        let elapsed=elapsed_ns(start)?; $out.calls.finish(call_id,&capture)?;
        $out.emit(&json!({"phase":"api","call_id":call_id,"operation":$name,"elapsed_ns":elapsed,"capture":capture,"calls":$out.calls.summary(),"error":result.as_ref().err().map(error_text)}))?;
        let value=result.map_err(|e|error_text(&e))?;
        require(capture.attempts==1 && !capture.truncated && capture.status==Some(200),"one-attempt successful response required")?;
        value
    }};
}
// Preserve returned create evidence before interpreting it or making a Get.
// Neither this evidence nor a random name authorizes deletion without identity.
macro_rules! create {
    ($ctx:expr,$s:expr,$out:expr,$name:expr,$receipt:expr,$request:expr) => {{
        let call_id=$out.begin_call($s,$name)?; let start=Instant::now(); let result=$request.send().await;
        let mut capture=$s.capture()?; let elapsed=elapsed_ns(start)?;
        $out.calls.finish(call_id,&capture)?;
        if let Some(body)=capture.body.as_mut() {if body.len()>8192 {let mut end=8192; while !body.is_char_boundary(end) {end-=1;} body.truncate(end); capture.truncated=true;}}
        $ctx.save($receipt,json!({"call_id":call_id,"operation":$name,"capture":capture,"elapsed_ns":elapsed,"sdk_error":result.as_ref().err().map(error_text)}))?;
        capture.body=None;
        $out.emit(&json!({"phase":"api","call_id":call_id,"operation":$name,"capture":capture,"elapsed_ns":elapsed,"calls":$out.calls.summary(),"error":result.as_ref().err().map(error_text)}))?;
        result.map_err(|e|error_text(&e))?;
        require(capture.attempts==1 && capture.status==Some(200) && !capture.truncated,"create response/attempt bound")?;
    }};
}
async fn authenticate_account(ctx: &Context, s: &Service, out: &mut Output) -> Result<()> {
    let caller = call!(s, out, "STS.GetCallerIdentity", s.sts.get_caller_identity());
    out.emit(&json!({"phase":"caller_identity","expected_account":ctx.c.account_id,"account":caller.account(),"arn":caller.arn(),"user_id":caller.user_id()}))?;
    out.sync()?;
    require(
        caller.account() == Some(ctx.c.account_id.as_str()),
        "authenticated AWS account differs from config",
    )
}
fn bucket_arn(c: &Config) -> String {
    format!(
        "arn:aws:s3vectors:{}:{}:bucket/{}",
        c.region, c.account_id, c.bucket
    )
}
fn index_arn(c: &Config) -> String {
    format!("{}/index/{}", bucket_arn(c), c.index)
}
fn key(id: usize) -> String {
    format!("{id:06}")
}
fn ordinal(key: &str, rows: usize) -> Result<usize> {
    require(
        key.len() == 6 && key.bytes().all(|b| b.is_ascii_digit()),
        "six-digit ordinal key",
    )?;
    let id = key.parse::<usize>()?;
    require(id < rows, "ordinal key range")?;
    Ok(id)
}

fn check_bucket(c: &Config, v: &Value) -> Result<()> {
    require(
        v["vectorBucketArn"] == bucket_arn(c)
            && v["vectorBucketName"] == c.bucket
            && v["creationTime"]
                .as_f64()
                .is_some_and(|x| x.is_finite() && x > 0.0),
        "bucket ARN/name/creation identity",
    )
}
fn check_index(c: &Config, v: &Value) -> Result<()> {
    require(
        v["indexArn"] == index_arn(c)
            && v["indexName"] == c.index
            && v["vectorBucketName"] == c.bucket
            && v["dimension"] == D
            && v["dataType"] == "float32"
            && v["distanceMetric"] == "cosine"
            && v["creationTime"]
                .as_f64()
                .is_some_and(|x| x.is_finite() && x > 0.0),
        "index ARN/name/creation/shape",
    )
}
async fn publish(ctx: &Context, s: &Service) -> Result<()> {
    let mut out = Output::create(ctx, "publish.jsonl")?;
    let result = publish_inner(ctx, s, &mut out).await;
    out.terminal(
        if result.is_ok() {
            "COMPLETE"
        } else {
            "INVALID"
        },
        result.as_ref().err().map(error_text),
    )?;
    result
}
async fn publish_inner(ctx: &Context, s: &Service, out: &mut Output) -> Result<()> {
    admitted(ctx, true)?;
    out.emit(&json!({"phase":"admission","truth_opened":true}))?;
    authenticate_account(ctx, s, out).await?;
    let c = &ctx.c;
    ctx.save("bucket-create-intent.json",json!({"expected_arn":bucket_arn(c),"name":c.bucket,"at_ms":now_ms()?,"account_authenticated":true}))?;
    create!(
        ctx,
        s,
        out,
        "CreateVectorBucket",
        "bucket-create-result.json",
        s.client
            .create_vector_bucket()
            .vector_bucket_name(&c.bucket)
    );
    let actual_bucket = s.body()?["vectorBucketArn"].clone();
    ctx.save(
        "owned-bucket.json",
        json!({"arn":actual_bucket,"create_request_id":s.capture()?.request_id,"at_ms":now_ms()?}),
    )?;
    require(actual_bucket == bucket_arn(c), "created bucket ARN")?;
    call!(
        s,
        out,
        "GetVectorBucket",
        s.client.get_vector_bucket().vector_bucket_name(&c.bucket)
    );
    let bucket = s.body()?["vectorBucket"].clone();
    check_bucket(c, &bucket)?;
    ctx.save("bucket-identity.json", bucket)?;
    ctx.save("index-create-intent.json",json!({"expected_arn":index_arn(c),"name":c.index,"bucket_arn":bucket_arn(c),"at_ms":now_ms()?,"account_authenticated":true}))?;
    create!(
        ctx,
        s,
        out,
        "CreateIndex",
        "index-create-result.json",
        s.client
            .create_index()
            .vector_bucket_name(&c.bucket)
            .index_name(&c.index)
            .data_type(DataType::Float32)
            .dimension(D as i32)
            .distance_metric(DistanceMetric::Cosine)
    );
    let actual_index = s.body()?["indexArn"].clone();
    ctx.save(
        "owned-index.json",
        json!({"arn":actual_index,"create_request_id":s.capture()?.request_id,"at_ms":now_ms()?}),
    )?;
    require(actual_index == index_arn(c), "created index ARN")?;
    // GetIndex has attributes, not an ACTIVE readiness state. Never query it to warm up.
    call!(
        s,
        out,
        "GetIndex",
        s.client.get_index().index_arn(index_arn(c))
    );
    let index = s.body()?["index"].clone();
    check_index(c, &index)?;
    ctx.save("index-identity.json", index)?;
    let mut corpus = open_artifact(&c.corpus)?;
    let bound = identity(&corpus)?;
    let mut last_put: Option<Instant> = None;
    let mut put_calls = 0;
    for first in (0..c.rows).step_by(500) {
        let end = (first + 500).min(c.rows);
        let mut batch = Vec::with_capacity(end - first);
        for id in first..end {
            batch.push(
                PutInputVector::builder()
                    .key(key(id))
                    .data(VectorData::Float32(read_vector(&mut corpus)?))
                    .build()?,
            );
        }
        require(identity(&corpus)? == bound, "corpus changed during publish")?;
        if c.mode == "measurement" {
            if let Some(last) = last_put {
                tokio::time::sleep(Duration::from_secs(1).saturating_sub(last.elapsed())).await;
            }
        }
        last_put = Some(Instant::now());
        // 500*1024 finite f32 JSON numbers plus keys stay below the 20 MiB API cap.
        call!(
            s,
            out,
            "PutVectors",
            s.client
                .put_vectors()
                .index_arn(index_arn(c))
                .set_vectors(Some(batch))
        );
        put_calls += 1;
    }
    require(corpus.read(&mut [0])? == 0, "corpus upload EOF")?;
    let mut seen = vec![false; c.rows];
    let mut token: Option<String> = None;
    let mut tokens = BTreeSet::new();
    let mut list_calls = 0;
    loop {
        // maxResults is a ceiling: S3 Vectors may stop after 1 MiB processed.
        require(list_calls < LIST_PAGE_CAP, "enumeration page cap")?;
        call!(
            s,
            out,
            "ListVectors",
            s.client
                .list_vectors()
                .index_arn(index_arn(c))
                .max_results(500)
                .return_data(false)
                .return_metadata(false)
                .set_next_token(token.take())
        );
        list_calls += 1;
        let response = s.body()?;
        let vectors = response["vectors"].as_array().ok_or("list vectors shape")?;
        require(vectors.len() <= 500, "list page bound")?;
        for v in vectors {
            let id = ordinal(v["key"].as_str().ok_or("list key")?, c.rows)?;
            require(!seen[id], "duplicate enumeration key")?;
            seen[id] = true;
        }
        match response.get("nextToken") {
            None | Some(Value::Null) => break,
            Some(Value::String(t)) => {
                require(
                    !t.is_empty() && t.len() <= 2048 && tokens.insert(t.clone()),
                    "pagination token/progress",
                )?;
                token = Some(t.clone());
            }
            _ => return Err("pagination token shape".into()),
        }
    }
    require(seen.iter().all(|v| *v), "incomplete enumeration")?;
    let mut get_calls = 0;
    for first in (0..c.rows).step_by(100) {
        let end = (first + 100).min(c.rows);
        let keys = (first..end).map(key).collect();
        call!(
            s,
            out,
            "GetVectors",
            s.client
                .get_vectors()
                .index_arn(index_arn(c))
                .set_keys(Some(keys))
                .return_data(true)
                .return_metadata(false)
        );
        get_calls += 1;
        let response = s.body()?;
        let vectors = response["vectors"].as_array().ok_or("readback shape")?;
        require(
            vectors.len() == end - first && response.get("nextToken").is_none(),
            "readback count/pagination",
        )?;
        let mut batch_seen = BTreeSet::new();
        for v in vectors {
            let id = ordinal(v["key"].as_str().ok_or("readback key")?, c.rows)?;
            require(
                (first..end).contains(&id) && batch_seen.insert(id),
                "readback wrong/duplicate key",
            )?;
            let values: Vec<f32> = serde_json::from_value(v["data"]["float32"].clone())?;
            require(values.len() == D, "readback dimension")?;
            corpus.seek(SeekFrom::Start((id * D * 4) as u64))?;
            let original = read_vector(&mut corpus)?;
            require(
                values
                    .iter()
                    .zip(original)
                    .all(|(a, b)| a.to_bits() == b.to_bits()),
                "readback f32 bit drift",
            )?;
        }
    }
    require(
        identity(&corpus)? == bound && scan(&c.corpus, false, c.rows)? == bound,
        "post-readback corpus binding",
    )?;
    require(
        put_calls == c.rows.div_ceil(500) && get_calls == c.rows.div_ceil(100),
        "complete publication call counts",
    )?;
    out.sync()?;
    ctx.save("published.json",json!({"index_arn":index_arn(c),"bucket_arn":bucket_arn(c),"rows":c.rows,
        "put_calls":put_calls,"list_calls":list_calls,"list_page_cap":LIST_PAGE_CAP,"get_calls":get_calls,"calls":out.calls.summary(),
        "enumerated_keys":c.rows,"bit_exact_readback_rows":c.rows,"verified_at_ms":now_ms()?,
        "corpus_sha256":c.corpus.sha256,"queries_sha256":c.queries.sha256,"truth_sha256":c.truth.sha256,
        "index_identity":ctx.receipt("index-identity.json")?,"bucket_identity":ctx.receipt("bucket-identity.json")?}))
}
fn published(ctx: &Context) -> Result<Value> {
    let p = ctx.receipt("published.json")?;
    let c = &ctx.c;
    require(
        p["index_arn"] == index_arn(c)
            && p["bucket_arn"] == bucket_arn(c)
            && p["rows"] == c.rows
            && p["enumerated_keys"] == c.rows
            && p["bit_exact_readback_rows"] == c.rows
            && p["put_calls"] == c.rows.div_ceil(500)
            && p["get_calls"] == c.rows.div_ceil(100)
            && p["list_page_cap"] == LIST_PAGE_CAP
            && p["list_calls"]
                .as_u64()
                .is_some_and(|n| n > 0 && n <= LIST_PAGE_CAP as u64)
            && p["calls"]["max_reserved_calls"] == CALL_CAP
            && p["corpus_sha256"] == c.corpus.sha256
            && p["queries_sha256"] == c.queries.sha256
            && p["truth_sha256"] == c.truth.sha256,
        "complete publication binding",
    )?;
    require(
        ctx.receipt("owned-index.json")?["arn"] == index_arn(c)
            && ctx.receipt("owned-bucket.json")?["arn"] == bucket_arn(c),
        "owned resources",
    )?;
    require(
        p["index_identity"] == ctx.receipt("index-identity.json")?
            && p["bucket_identity"] == ctx.receipt("bucket-identity.json")?,
        "published resource identities",
    )?;
    check_index(c, &p["index_identity"])?;
    check_bucket(c, &p["bucket_identity"])?;
    Ok(p)
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Hit {
    key: String,
    distance_bits: u32,
}
fn parse_hits(raw: &str, rows: usize) -> Result<Vec<Hit>> {
    require(raw.len() <= 32 * 1024, "query response body cap")?;
    let v: Value = serde_json::from_str(raw)?;
    let object = v.as_object().ok_or("query response object")?;
    require(
        object
            .keys()
            .all(|k| k == "vectors" || k == "distanceMetric"),
        "query unexpected field/pagination",
    )?;
    if let Some(metric) = object.get("distanceMetric") {
        require(metric == "cosine", "query distance metric")?;
    }
    let vectors = v["vectors"].as_array().ok_or("query vectors missing")?;
    require(vectors.len() <= K, "query returned count exceeds k")?;
    let mut seen = BTreeSet::new();
    let mut hits = Vec::with_capacity(vectors.len());
    let mut previous = f32::NEG_INFINITY;
    for v in vectors {
        require(
            v.as_object().is_some_and(|o| {
                o.len() == 2 && o.contains_key("key") && o.contains_key("distance")
            }),
            "query vector shape",
        )?;
        let key = v["key"].as_str().ok_or("query key")?;
        let id = ordinal(key, rows)?;
        let distance = v["distance"].as_f64().ok_or("query distance missing")? as f32;
        require(
            distance.is_finite() && distance >= previous && seen.insert(id),
            "query finite distance/order/duplicates",
        )?;
        previous = distance;
        hits.push(Hit {
            key: key.into(),
            distance_bits: distance.to_bits(),
        });
    }
    Ok(hits)
}
#[cfg(test)]
thread_local! {
    static FAIL_SYNC:std::cell::Cell<u8>=const {std::cell::Cell::new(0)};
    static WATCH_TRUTH:std::cell::RefCell<Option<PathBuf>>=const {std::cell::RefCell::new(None)};
    static TRUTH_OPENS:std::cell::Cell<usize>=const {std::cell::Cell::new(0)};
}
fn seal_responses(ctx: &Context, out: &mut Output, count: usize, wall_ns: u64) -> Result<()> {
    require(count == ctx.c.count, "cannot seal partial query panel")?;
    out.emit(&json!({"phase":"all_queries_complete","count":count,"truth_opened":false,"wall_ns":wall_ns}))?;
    #[cfg(test)]
    require(
        FAIL_SYNC.with(|v| v.get()) != 1,
        "injected file sync failure",
    )?;
    out.file.sync_all()?;
    #[cfg(test)]
    require(
        FAIL_SYNC.with(|v| v.get()) != 2,
        "injected directory sync failure",
    )?;
    out.sync()?;
    ctx.save("response-seal.json",json!({"count":count,"bytes":out.bytes,"sha256":format!("{:x}",out.digest.clone().finalize()),
        "file":identity(&out.file)?,"queries_sha256":ctx.c.queries.sha256,"index_arn":index_arn(&ctx.c),
        "wall_ns":wall_ns,"truth_opened":false,"file_synced":true,"directory_synced":true,"calls":out.calls.summary()}))
}
async fn measure(ctx: &Context, s: &Service) -> Result<()> {
    // No truth descriptor is opened anywhere in this process path.
    admitted(ctx, false)?;
    let publication = published(ctx)?;
    let verified = publication["verified_at_ms"]
        .as_u64()
        .ok_or("verified timestamp")?;
    let idle_ms = now_ms()?
        .checked_sub(verified)
        .ok_or("clock before verification")?;
    require(
        idle_ms >= ctx.c.idle_seconds * 1000,
        "preregistered idle interval not elapsed; exit and release compute",
    )?;
    let query_bytes = read_small(&ctx.c.queries.path, ctx.c.queries.bytes)?;
    require(
        hash(&query_bytes) == ctx.c.queries.sha256,
        "query reauthentication",
    )?;
    // The create-only marker burns this index's attempt before the first request.
    // A partial stream can never be resumed by invoking measure again.
    let mut out = Output::create(ctx, "responses.jsonl")?;
    ctx.save("measure-started.json",json!({"index_arn":index_arn(&ctx.c),"at_ms":now_ms()?,"observed_inactivity_ms":idle_ms,"resume_allowed":false}))?;
    out.emit(&json!({"schema":RECEIPT_SCHEMA,"phase":"measurement","config_sha256":ctx.sha,"index_arn":index_arn(&ctx.c),
        "query_count":ctx.c.count,"queries_sha256":ctx.c.queries.sha256,"observed_inactivity_ms":idle_ms,"host_identity":ctx.c.host_identity,
        "mode":ctx.c.mode,"cache_conditions":"cache-uncontrolled first pass; inactivity is not proof of eviction",
        "inactivity_basis":"elapsed wall time since this run's full readback; external index activity is unobservable",
        "client_reuse":true,"concurrency":1,"query_interval_ms":0,"max_attempts":1,"operation_timeout_seconds":30,"pass_timeout_seconds":1800,
        "response_retention_cap_bytes":RESPONSE_CAP,"transport_response_byte_limit":null,
        "required_external_memory_max_bytes":536870912_u64,"required_external_swap_max_bytes":0,
        "credential_setup":"explicit named profile; lazy SDK resolution included in first request",
        "latency_boundary":"request construction, signing, transport, deserialization and validation; excludes input staging and receipt writes",
        "truth_opened":false}))?;
    let result = measure_inner(ctx, s, &query_bytes, &mut out).await;
    if let Err(ref error) = result {
        out.terminal("INVALID", Some(error_text(error)))?;
    }
    result
}
async fn measure_inner(ctx: &Context, s: &Service, queries: &[u8], out: &mut Output) -> Result<()> {
    let pass = Instant::now();
    let deadline = Duration::from_secs(1800);
    let mut completed = 0;
    for id in 0..ctx.c.count {
        let remaining = deadline
            .checked_sub(pass.elapsed())
            .ok_or("measurement deadline")?;
        let bytes = &queries[id * D * 4..(id + 1) * D * 4];
        let values = vector(bytes)?;
        let call_id = out.begin_call(s, "QueryVectors")?;
        let start = Instant::now();
        let request = s
            .client
            .query_vectors()
            .index_arn(index_arn(&ctx.c))
            .top_k(K as i32)
            .query_vector(VectorData::Float32(values))
            .return_distance(true)
            .return_metadata(false);
        let response = tokio::time::timeout(remaining, request.send()).await;
        let mut capture = s.capture()?;
        // Preserve bounded original responses even when the service violates shape.
        if let Some(body) = capture.body.as_mut() {
            if body.len() > 32 * 1024 {
                let mut end = 32 * 1024;
                while !body.is_char_boundary(end) {
                    end -= 1;
                }
                body.truncate(end);
                capture.truncated = true;
            }
        }
        let sdk_error = match &response {
            Err(e) => Some(error_text(e)),
            Ok(Err(e)) => Some(error_text(e)),
            Ok(Ok(_)) => None,
        };
        let valid = (|| -> Result<Vec<Hit>> {
            require(sdk_error.is_none(), "SDK query failure")?;
            require(
                capture.attempts == 1
                    && capture.status == Some(200)
                    && !capture.truncated
                    && capture
                        .request_id
                        .as_ref()
                        .is_some_and(|s| !s.is_empty() && s.len() <= 1024),
                "query status/attempt/request ID",
            )?;
            parse_hits(
                capture.body.as_deref().ok_or("missing query body")?,
                ctx.c.rows,
            )
        })();
        let ns = elapsed_ns(start)?;
        out.calls.finish(call_id, &capture)?;
        out.emit(&json!({"phase":"query","call_id":call_id,"ordinal":id,"query_sha256":hash(bytes),"elapsed_ns":ns,"capture":capture,"calls":out.calls.summary(),
            "hits":valid.as_ref().ok(),"sdk_error":sdk_error,"validation_error":valid.as_ref().err().map(error_text),"truth_opened":false}))?;
        valid?;
        completed += 1;
        require(pass.elapsed() <= deadline, "measurement deadline")?;
    }
    seal_responses(ctx, out, completed, elapsed_ns(pass)?)
}

fn quantiles(samples: &[u64]) -> Result<(u64, [u64; 4])> {
    require(
        !samples.is_empty() && samples.len() <= 1000,
        "quantile population",
    )?;
    let sum = samples
        .iter()
        .try_fold(0_u64, |s, &n| s.checked_add(n).ok_or("latency overflow"))?;
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    Ok((
        sum,
        [50, 90, 95, 99].map(|p| sorted[(sorted.len() * p).div_ceil(100) - 1]),
    ))
}
fn authenticate_output(ctx: &Context, seal: &Value) -> Result<File> {
    require(
        seal["count"] == ctx.c.count
            && seal["truth_opened"] == false
            && seal["file_synced"] == true
            && seal["directory_synced"] == true
            && seal["queries_sha256"] == ctx.c.queries.sha256
            && seal["index_arn"] == index_arn(&ctx.c),
        "complete durable seal required",
    )?;
    let bytes = seal["bytes"].as_u64().ok_or("seal length")?;
    let sha = seal["sha256"].as_str().ok_or("seal SHA")?;
    require(valid_sha(sha) && bytes <= OUTPUT_CAP, "seal bounds")?;
    let bound: Identity = serde_json::from_value(seal["file"].clone())?;
    let mut file = regular(&ctx.path("responses.jsonl"), OUTPUT_CAP)?;
    let current = identity(&file)?;
    require(
        current.len == bytes && bound.len == bytes,
        "sealed file length",
    )?;
    let mut remaining = bytes;
    let mut block = [0; 65_536];
    let mut digest = Sha256::new();
    while remaining > 0 {
        let n = usize::try_from(remaining.min(block.len() as u64))?;
        file.read_exact(&mut block[..n])?;
        digest.update(&block[..n]);
        remaining -= n as u64;
    }
    require(
        file.read(&mut [0])? == 0
            && identity(&file)? == current
            && format!("{:x}", digest.finalize()) == sha,
        "sealed SHA/EOF",
    )?;
    // Reduction re-establishes durability before touching truth after restart.
    file.sync_all()?;
    directory(&ctx.c.state_dir)?.sync_all()?;
    file.rewind()?;
    Ok(file)
}
fn line(reader: &mut BufReader<File>) -> Result<Option<Value>> {
    let mut bytes = Vec::new();
    let n = Read::by_ref(reader)
        .take((LINE_CAP + 1) as u64)
        .read_until(b'\n', &mut bytes)?;
    if n == 0 {
        return Ok(None);
    }
    require(
        n <= LINE_CAP && bytes.last() == Some(&b'\n'),
        "response line cap/framing",
    )?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}
fn reduce(ctx: &Context) -> Result<Value> {
    validate_config(&ctx.c)?;
    published(ctx)?;
    let seal = ctx.receipt("response-seal.json")?;
    let file = authenticate_output(ctx, &seal)?;
    let bound = identity(&file)?;
    let mut reader = BufReader::with_capacity(8192, file);
    let head = line(&mut reader)?.ok_or("response header missing")?;
    require(
        head["phase"] == "measurement"
            && head["schema"] == RECEIPT_SCHEMA
            && head["config_sha256"] == ctx.sha
            && head["index_arn"] == index_arn(&ctx.c)
            && head["query_count"] == ctx.c.count
            && head["queries_sha256"] == ctx.c.queries.sha256
            && head["truth_opened"] == false,
        "sealed header binding",
    )?;
    let queries = read_small(&ctx.c.queries.path, ctx.c.queries.bytes)?;
    require(
        hash(&queries) == ctx.c.queries.sha256,
        "reduction query SHA",
    )?;
    let mut samples = Vec::with_capacity(ctx.c.count);
    let mut returned = Vec::with_capacity(ctx.c.count);
    let mut underfills = 0;
    for id in 0..ctx.c.count {
        let row = line(&mut reader)?.ok_or("incomplete query panel")?;
        require(
            row["phase"] == "query"
                && row["ordinal"] == id
                && row["truth_opened"] == false
                && row["sdk_error"].is_null()
                && row["validation_error"].is_null()
                && row["query_sha256"] == hash(&queries[id * D * 4..(id + 1) * D * 4]),
            "sealed query binding/status",
        )?;
        let capture: Capture = serde_json::from_value(row["capture"].clone())?;
        require(
            capture.attempts == 1
                && capture.status == Some(200)
                && !capture.truncated
                && capture
                    .request_id
                    .as_ref()
                    .is_some_and(|id| !id.is_empty() && id.len() <= 1024),
            "sealed response status/attempt/ID",
        )?;
        let raw = capture
            .body
            .as_deref()
            .ok_or("sealed raw response missing")?;
        require(
            capture.body_bytes == raw.len()
                && capture.body_sha256.as_deref() == Some(hash(raw.as_bytes()).as_str()),
            "sealed raw response SHA/length",
        )?;
        let hits: Vec<Hit> = serde_json::from_value(row["hits"].clone())?;
        require(
            hits == parse_hits(raw, ctx.c.rows)?,
            "sealed original responses differ from hits",
        )?;
        let elapsed = row["elapsed_ns"].as_u64().ok_or("sealed elapsed")?;
        require(elapsed > 0, "nonzero query latency")?;
        samples.push(elapsed);
        underfills += usize::from(hits.len() < K);
        returned.push(
            hits.iter()
                .map(|h| ordinal(&h.key, ctx.c.rows).map(|x| x as u64))
                .collect::<Result<Vec<_>>>()?,
        );
    }
    let tail = line(&mut reader)?.ok_or("complete marker missing")?;
    require(
        tail["phase"] == "all_queries_complete"
            && tail["count"] == ctx.c.count
            && tail["truth_opened"] == false
            && tail["wall_ns"] == seal["wall_ns"]
            && line(&mut reader)?.is_none()
            && identity(reader.get_ref())? == bound,
        "complete marker/EOF/identity",
    )?;
    let wall_ns = seal["wall_ns"].as_u64().ok_or("pass wall time")?;
    let (sum, [p50, p90, p95, p99]) = quantiles(&samples)?;
    require(sum > 0 && wall_ns >= sum, "pass time covers queries")?;
    // Only here can truth be opened: the original responses and entire panel
    // have already passed the complete durable seal and framing checks.
    scan(&ctx.c.truth, true, ctx.c.rows)?;
    let truth = read_small(&ctx.c.truth.path, ctx.c.truth.bytes)?;
    require(hash(&truth) == ctx.c.truth.sha256, "truth reauthentication")?;
    let mut hits = 0;
    for (id, found) in returned.iter().enumerate() {
        let expected = truth_row(&truth[id * K * 8..(id + 1) * K * 8], ctx.c.rows)?;
        hits += found.iter().filter(|id| expected.contains(id)).count();
    }
    let summary = json!({"schema":"borsuk-s3-vectors-reduction-v1","status":"VALID","mode":ctx.c.mode,"config_sha256":ctx.sha,
        "index_arn":index_arn(&ctx.c),"response_sha256":seal["sha256"],"query_count":ctx.c.count,
        "hits":hits,"recall_denominator":ctx.c.count*K,"recall_at_10":hits as f64/(ctx.c.count*K) as f64,"underfilled_queries":underfills,
        "first_query_ns":samples[0],"pooled_condition":"cache-uncontrolled first pass; backend cache state unknown",
        "percentile_method":"integer nearest rank","pooled_p50_ns":p50,"pooled_p90_ns":p90,"pooled_p95_ns":p95,"pooled_p99_ns":p99,
        "serial_reciprocal_qps":ctx.c.count as f64*1e9/sum as f64,"whole_pass_qps":ctx.c.count as f64*1e9/wall_ns as f64,
        "sum_query_ns":sum,"whole_pass_ns":wall_ns,"concurrent_service_capacity_claim":false,"guaranteed_cold_claim":false,
        "all_queries_sealed":true,"truth_opened_after_seal":true,"observed_inactivity_ms":head["observed_inactivity_ms"],
        "calls_at_measurement_seal":seal["calls"],"billed_requests":null,"billed_bytes":null,"cost_usd":null});
    ctx.save("reduction.json", summary.clone())?;
    Ok(summary)
}
// Only an explicit single-attempt HTTP 404 proves absence. A timeout/503 or
// missing local identity is never permission to infer that a resource is gone.
macro_rules! probe {
    ($s:expr,$out:expr,$name:expr,$request:expr) => {{
        let call_id=$out.begin_call($s,$name)?; let start=Instant::now(); let result=$request.send().await;
        let mut capture=$s.capture()?; capture.body=None;
        let elapsed=elapsed_ns(start)?; $out.calls.finish(call_id,&capture)?;
        $out.emit(&json!({"phase":"api","call_id":call_id,"operation":$name,"elapsed_ns":elapsed,"capture":capture,"calls":$out.calls.summary(),"error":result.as_ref().err().map(error_text)}))?;
        require(capture.attempts==1 && !capture.truncated,"probe attempts/body bound")?;
        if result.is_err() && capture.status==Some(404) {None} else {
            result.map_err(|e|error_text(&e))?;
            Some($s.body()?)
        }
    }};
}
async fn probe_index(ctx: &Context, s: &Service, out: &mut Output) -> Result<Option<Value>> {
    Ok(probe!(
        s,
        out,
        "GetIndex",
        s.client.get_index().index_arn(index_arn(&ctx.c))
    )
    .map(|v| v["index"].clone()))
}
async fn probe_bucket(ctx: &Context, s: &Service, out: &mut Output) -> Result<Option<Value>> {
    Ok(probe!(
        s,
        out,
        "GetVectorBucket",
        s.client
            .get_vector_bucket()
            .vector_bucket_name(&ctx.c.bucket)
    )
    .map(|v| v["vectorBucket"].clone()))
}
async fn cleanup(ctx: &Context, s: &Service) -> Result<()> {
    if ctx.path("cleanup.json").try_exists()? {
        let receipt = ctx.receipt("cleanup.json")?;
        require(
            receipt["bucket_arn"] == bucket_arn(&ctx.c)
                && receipt["index_arn"] == index_arn(&ctx.c)
                && receipt["bucket_absence_verified"] == true
                && (receipt["owned_index_absent"] == false
                    || receipt["index_absence_verified"] == true),
            "completed cleanup receipt binding",
        )?;
        return Ok(()); // Idempotent historical completion: no new service calls.
    }
    let calls = Calls::open(ctx)?;
    let path = (1..=CLEANUP_ATTEMPT_CAP)
        .map(|i| ctx.path(&format!("cleanup-{i:03}.jsonl")))
        .find(|p| !p.exists())
        .ok_or("cleanup attempt cap; explicit reconciliation required")?;
    let mut out = Output::with_calls(&path, calls)?;
    let result = cleanup_inner(ctx, s, &mut out).await;
    out.terminal(
        if result.is_ok() {
            "COMPLETE"
        } else {
            "INVALID"
        },
        result.as_ref().err().map(error_text),
    )?;
    result
}
async fn cleanup_inner(ctx: &Context, s: &Service, out: &mut Output) -> Result<()> {
    let c = &ctx.c;
    let owned_bucket = ctx.receipt("owned-bucket.json")?;
    let bound_bucket = ctx.receipt("bucket-identity.json")?;
    require(
        owned_bucket["arn"] == bucket_arn(c),
        "cleanup bucket ownership",
    )?;
    check_bucket(c, &bound_bucket)?;
    // Missing ownership evidence fails closed, even after a crashed create.
    let index_owned = ctx.path("owned-index.json").try_exists()?;
    let bound_index = if index_owned {
        require(
            ctx.receipt("owned-index.json")?["arn"] == index_arn(c),
            "cleanup index ownership",
        )?;
        let i = ctx.receipt("index-identity.json")?;
        check_index(c, &i)?;
        Some(i)
    } else {
        None
    };
    let mut index_deleted = false;
    let mut bucket_deleted = false;
    authenticate_account(ctx, s, out).await?;
    if let Some(bucket) = probe_bucket(ctx, s, out).await? {
        require(bucket == bound_bucket, "cleanup bucket replaced/changed")?;
        call!(
            s,
            out,
            "ListIndexes",
            s.client
                .list_indexes()
                .vector_bucket_name(&c.bucket)
                .max_results(2)
        );
        let response = s.body()?;
        let indexes = response["indexes"].as_array().ok_or("cleanup index list")?;
        require(
            response.get("nextToken").is_none_or(Value::is_null),
            "foreign index pagination",
        )?;
        require(
            indexes.len() <= usize::from(index_owned)
                && indexes
                    .iter()
                    .all(|i| i["indexArn"] == index_arn(c) && i["indexName"] == c.index),
            "foreign index prevents cleanup",
        )?;
        if let Some(bound) = bound_index {
            match probe_index(ctx, s, out).await? {
                Some(index) => {
                    require(
                        index == bound && indexes.len() == 1,
                        "cleanup index replaced/changed",
                    )?;
                    call!(
                        s,
                        out,
                        "DeleteIndex",
                        s.client.delete_index().index_arn(index_arn(c))
                    );
                    index_deleted = true;
                    require(
                        probe_index(ctx, s, out).await?.is_none(),
                        "deleted index absence not verified",
                    )?;
                }
                None => require(indexes.is_empty(), "index absence/list inconsistency")?,
            }
        }
        call!(
            s,
            out,
            "DeleteVectorBucket",
            s.client
                .delete_vector_bucket()
                .vector_bucket_name(&c.bucket)
        );
        bucket_deleted = true;
        require(
            probe_bucket(ctx, s, out).await?.is_none(),
            "deleted bucket absence not verified",
        )?;
    } else if index_owned {
        require(
            probe_index(ctx, s, out).await?.is_none(),
            "bucket/index absence inconsistency",
        )?;
    }
    out.sync()?;
    ctx.save(
        "cleanup.json",
        json!({"bucket_arn":bucket_arn(c),"index_arn":index_arn(c),"owned_index_absent":index_owned,
        "index_deleted_this_attempt":index_deleted,"bucket_deleted_this_attempt":bucket_deleted,
        "bucket_absence_verified":true,"index_absence_verified":index_owned,"at_ms":now_ms()?,
        "attempt_log":out.path.file_name().and_then(|s|s.to_str()),"calls":out.calls.summary()}),
    )
}
fn run() -> Result<()> {
    let args: Vec<_> = std::env::args_os().collect();
    require(
        args.len() == 4,
        "usage: check_s3_vectors_baseline <admit|publish|measure|reduce|cleanup> CONFIG CONFIG_SHA256",
    )?;
    let command = args[1].to_str().ok_or("command UTF-8")?;
    require(
        matches!(
            command,
            "admit" | "publish" | "measure" | "reduce" | "cleanup"
        ),
        "unknown command",
    )?;
    let ctx = load(Path::new(&args[2]), args[3].to_str().ok_or("SHA UTF-8")?)?;
    match command {
        "admit" => admit(&ctx),
        "reduce" => {
            println!("{}", reduce(&ctx)?);
            Ok(())
        }
        _ => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .max_blocking_threads(1)
                .build()?;
            runtime.block_on(async {
                let service = Service::new(&ctx.c)?;
                match command {
                    "publish" => publish(&ctx, &service).await,
                    "measure" => measure(&ctx, &service).await,
                    "cleanup" => cleanup(&ctx, &service).await,
                    _ => unreachable!(),
                }
            })
        }
    }
}
fn main() {
    if let Err(error) = run() {
        eprintln!("INVALID: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::post};
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct Mock {
        calls: AtomicUsize,
        queries: AtomicUsize,
        corpus: Mutex<BTreeMap<String, Vec<f32>>>,
        underfill: bool,
        fail_query: bool,
        oversized_response: bool,
        wrong_account: bool,
        wrong_create_arn: bool,
        create_calls: AtomicUsize,
        fail_bucket_identity_once: AtomicBool,
        fail_index_identity_once: AtomicBool,
        drift: bool,
        pagination_fault: &'static str,
        list_calls: AtomicUsize,
        foreign_index: bool,
        replace_index: AtomicBool,
        replace_bucket: AtomicBool,
        delete_calls: AtomicUsize,
        fail_cleanup_once: AtomicBool,
        fail_index_absence_once: AtomicBool,
        fail_bucket_absence_once: AtomicBool,
        index_deleted: AtomicBool,
        bucket_deleted: AtomicBool,
    }
    async fn sts_endpoint(State(s): State<Arc<Mock>>, body: String) -> impl IntoResponse {
        s.calls.fetch_add(1, Ordering::SeqCst);
        assert!(body.contains("Action=GetCallerIdentity"));
        let account = if s.wrong_account {
            "999999999999"
        } else {
            "123456789012"
        };
        (
            StatusCode::OK,
            [
                ("content-type", "text/xml"),
                ("x-amzn-requestid", "sts-fixture-request-id"),
            ],
            format!(
                "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><GetCallerIdentityResult><Arn>arn:aws:iam::{account}:user/fixture</Arn><UserId>fixture-user</UserId><Account>{account}</Account></GetCallerIdentityResult><ResponseMetadata><RequestId>sts-fixture-request-id</RequestId></ResponseMetadata></GetCallerIdentityResponse>"
            ),
        )
    }
    async fn endpoint(
        State(s): State<Arc<Mock>>,
        uri: axum::http::Uri,
        axum::Json(body): axum::Json<Value>,
    ) -> impl IntoResponse {
        s.calls.fetch_add(1, Ordering::SeqCst);
        let mut status = StatusCode::OK;
        let response = match uri.path() {
            "/GetVectorBucket" if s.fail_bucket_identity_once.swap(false, Ordering::SeqCst) => {
                status = StatusCode::SERVICE_UNAVAILABLE;
                json!({"__type":"ServiceUnavailableException","message":"bucket identity unavailable"})
            }
            "/GetIndex" if s.fail_index_identity_once.swap(false, Ordering::SeqCst) => {
                status = StatusCode::SERVICE_UNAVAILABLE;
                json!({"__type":"ServiceUnavailableException","message":"index identity unavailable"})
            }
            "/GetVectorBucket"
                if s.fail_cleanup_once.swap(false, Ordering::SeqCst)
                    || (s.bucket_deleted.load(Ordering::SeqCst)
                        && s.fail_bucket_absence_once.swap(false, Ordering::SeqCst)) =>
            {
                status = StatusCode::SERVICE_UNAVAILABLE;
                json!({"__type":"ServiceUnavailableException","message":"cleanup transient"})
            }
            "/GetIndex"
                if s.index_deleted.load(Ordering::SeqCst)
                    && s.fail_index_absence_once.swap(false, Ordering::SeqCst) =>
            {
                status = StatusCode::SERVICE_UNAVAILABLE;
                json!({"__type":"ServiceUnavailableException","message":"absence transient"})
            }
            "/GetVectorBucket" if s.bucket_deleted.load(Ordering::SeqCst) => {
                status = StatusCode::NOT_FOUND;
                json!({"__type":"NotFoundException","message":"bucket deleted"})
            }
            "/GetIndex" if s.index_deleted.load(Ordering::SeqCst) => {
                status = StatusCode::NOT_FOUND;
                json!({"__type":"NotFoundException","message":"index deleted"})
            }
            "/CreateVectorBucket" => {
                s.create_calls.fetch_add(1, Ordering::SeqCst);
                json!({"vectorBucketArn":if s.wrong_create_arn {"unexpected-returned-arn".into()} else {bucket_arn(&fixture_config(Path::new("/tmp")))}})
            }
            "/GetVectorBucket" => json!({"vectorBucket":{
                "vectorBucketName":fixture_config(Path::new("/tmp")).bucket,
                "vectorBucketArn":bucket_arn(&fixture_config(Path::new("/tmp"))),
                "creationTime":if s.replace_bucket.load(Ordering::SeqCst) {1700000100.0} else {1700000000.0}}}),
            "/CreateIndex" => {
                s.create_calls.fetch_add(1, Ordering::SeqCst);
                json!({"indexArn":index_arn(&fixture_config(Path::new("/tmp")))})
            }
            "/GetIndex" => json!({"index":{
                "indexName":fixture_config(Path::new("/tmp")).index,
                "indexArn":index_arn(&fixture_config(Path::new("/tmp"))),
                "vectorBucketName":fixture_config(Path::new("/tmp")).bucket,
                "creationTime":if s.replace_index.load(Ordering::SeqCst) {1700000101.0} else {1700000001.0},
                "dataType":"float32","dimension":D,"distanceMetric":"cosine"}}),
            "/ListIndexes" => {
                let mut indexes = if s.index_deleted.load(Ordering::SeqCst) {
                    vec![]
                } else {
                    vec![
                        json!({"indexArn":index_arn(&fixture_config(Path::new("/tmp"))),"indexName":fixture_config(Path::new("/tmp")).index,"creationTime":1700000001.0}),
                    ]
                };
                if s.foreign_index {
                    indexes.push(json!({"indexArn":"arn:aws:s3vectors:eu-central-1:123456789012:bucket/foreign/index/foreign","indexName":"foreign","creationTime":1700000002.0}));
                }
                json!({"indexes":indexes})
            }
            "/DeleteIndex" => {
                s.delete_calls.fetch_add(1, Ordering::SeqCst);
                s.index_deleted.store(true, Ordering::SeqCst);
                json!({})
            }
            "/DeleteVectorBucket" => {
                s.delete_calls.fetch_add(1, Ordering::SeqCst);
                s.bucket_deleted.store(true, Ordering::SeqCst);
                json!({})
            }
            "/PutVectors" => {
                let mut rows = s.corpus.lock().unwrap();
                for v in body["vectors"].as_array().unwrap() {
                    let values: Vec<f32> =
                        serde_json::from_value(v["data"]["float32"].clone()).unwrap();
                    assert_eq!(values.len(), D);
                    assert!(
                        rows.insert(v["key"].as_str().unwrap().into(), values)
                            .is_none()
                    );
                }
                json!({})
            }
            "/ListVectors" => {
                s.list_calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(body["maxResults"], 500);
                // One empty page with a fresh token precedes the four short pages.
                if body["nextToken"].is_null() {
                    return (
                        status,
                        [("x-amzn-requestid", "fixture-request-id")],
                        axum::Json(json!({"vectors":[],"nextToken":"0"})),
                    );
                }
                let start = body["nextToken"]
                    .as_str()
                    .unwrap_or("0")
                    .parse::<usize>()
                    .unwrap();
                let rows = s.corpus.lock().unwrap();
                let end = (start + 73).min(rows.len());
                let mut keys: Vec<_> = rows.keys().skip(start).take(end - start).cloned().collect();
                if s.pagination_fault == "duplicate" && start > 0 {
                    keys[0] = key(0);
                }
                if s.pagination_fault == "missing" && end == rows.len() {
                    keys.pop();
                }
                if s.pagination_fault == "overflow" {
                    keys = (0..501).map(|id| key(id % 257)).collect();
                }
                let mut response =
                    json!({"vectors":keys.iter().map(|k|json!({"key":k})).collect::<Vec<_>>()});
                if end < rows.len() {
                    response["nextToken"] = json!(if s.pagination_fault == "loop" && start > 0 {
                        start.to_string()
                    } else {
                        end.to_string()
                    });
                }
                if s.pagination_fault == "token_overflow" {
                    response["nextToken"] = json!("x".repeat(2049));
                }
                response
            }
            "/GetVectors" => {
                let rows = s.corpus.lock().unwrap();
                json!({"vectors":body["keys"].as_array().unwrap().iter().map(|k| {
                    let mut values = rows[k.as_str().unwrap()].clone();
                    if s.drift {values[0] = f32::from_bits(values[0].to_bits() ^ 1);}
                    json!({"key":k,"data":{"float32":values}})
                }).collect::<Vec<_>>()})
            }
            "/QueryVectors" => {
                let ordinal = s.queries.fetch_add(1, Ordering::SeqCst);
                assert_eq!(body["topK"], K);
                assert_eq!(body["returnDistance"], true);
                assert_eq!(body["returnMetadata"], false);
                assert!(body.get("filter").is_none());
                if s.oversized_response {
                    json!({"vectors":[],"padding":"x".repeat(RESPONSE_CAP+1)})
                } else if s.fail_query && ordinal == 1 {
                    status = StatusCode::SERVICE_UNAVAILABLE;
                    json!({"__type":"ServiceUnavailableException","message":"single attempt fixture"})
                } else {
                    let query: Vec<f32> =
                        serde_json::from_value(body["queryVector"]["float32"].clone()).unwrap();
                    let mut scores: Vec<_> = s
                        .corpus
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|(key, row)| {
                            let dot: f64 = row
                                .iter()
                                .zip(&query)
                                .map(|(a, b)| f64::from(*a) * f64::from(*b))
                                .sum();
                            let norm = |v: &[f32]| {
                                v.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt()
                            };
                            (key.clone(), (1.0 - dot / (norm(row) * norm(&query))) as f32)
                        })
                        .collect();
                    scores.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
                    scores.truncate(if s.underfill && ordinal == 1 { 7 } else { K });
                    json!({"vectors":scores.iter().map(|(k,d)|json!({"key":k,"distance":d})).collect::<Vec<_>>(),"distanceMetric":"cosine"})
                }
            }
            path => panic!("unexpected service operation {path}"),
        };
        (
            status,
            [("x-amzn-requestid", "fixture-request-id")],
            axum::Json(response),
        )
    }
    async fn mock_service(mock: Arc<Mock>) -> (Service, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = Router::new()
            .route("/", post(sts_endpoint))
            .route("/{operation}", post(endpoint))
            .with_state(mock);
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let tap = Tap::default();
        let credentials =
            aws_sdk_s3vectors::config::Credentials::new("test", "test", None, None, "fixture");
        let client = Client::from_conf(
            client_builder(&fixture_config(Path::new("/tmp")), tap.clone())
                .credentials_provider(credentials.clone())
                .endpoint_url(format!("http://{address}"))
                .build(),
        );
        let sts = aws_sdk_sts::Client::from_conf(
            sts_builder(&fixture_config(Path::new("/tmp")), tap.clone())
                .credentials_provider(credentials)
                .endpoint_url(format!("http://{address}"))
                .build(),
        );
        (Service { client, sts, tap }, task)
    }
    fn fixture_config(root: &Path) -> Config {
        let artifact = |name: &str| Artifact {
            path: root.join(name),
            bytes: 1,
            sha256: "0".repeat(64),
        };
        let run_id = "0123456789abcdef0123456789abcdef".to_string();
        Config {
            schema: CONFIG_SCHEMA.into(),
            mode: "canary".into(),
            dataset: DATASET.into(),
            revision: REVISION.into(),
            rows: 257,
            count: 2,
            dimensions: D,
            k: K,
            metric: "cosine".into(),
            corpus: artifact("corpus.f32"),
            queries: artifact("queries.f32"),
            truth: artifact("truth.u64"),
            state_dir: root.join("state"),
            bucket: format!("borsuk-s3v-{run_id}"),
            index: format!("baseline-{run_id}"),
            run_id,
            profile: "causality".into(),
            region: "eu-central-1".into(),
            account_id: "123456789012".into(),
            idle_seconds: 0,
            query_interval_ms: 0,
            host_identity: "native-mock-http".into(),
        }
    }
    fn fixture() -> (tempfile::TempDir, Context) {
        let dir = tempfile::tempdir().unwrap();
        let mut c = fixture_config(dir.path());
        std::fs::create_dir(&c.state_dir).unwrap();
        let mut corpus = Vec::new();
        for id in 0..c.rows {
            for dim in 0..D {
                let value = if dim == id {
                    1.0 + id as f32 / 1024.0
                } else if dim == D - 1 {
                    -0.0
                } else {
                    0.0
                };
                corpus.extend_from_slice(&value.to_le_bytes());
            }
        }
        let queries = [&corpus[..D * 4], &corpus[256 * D * 4..257 * D * 4]].concat();
        let truth: Vec<u8> = [
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            vec![256, 0, 1, 2, 3, 4, 5, 6, 7, 8],
        ]
        .concat()
        .iter()
        .flat_map(|id| (*id as u64).to_le_bytes())
        .collect();
        for (a, body) in [
            (&mut c.corpus, corpus),
            (&mut c.queries, queries),
            (&mut c.truth, truth),
        ] {
            a.bytes = body.len() as u64;
            a.sha256 = hash(&body);
            std::fs::write(&a.path, body).unwrap();
        }
        let bytes = serde_json::to_vec(&c).unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, &bytes).unwrap();
        let ctx = load(&path, &hash(&bytes)).unwrap();
        (dir, ctx)
    }
    fn watch_truth(ctx: &Context) {
        WATCH_TRUTH.with(|v| *v.borrow_mut() = Some(ctx.c.truth.path.clone()));
        TRUTH_OPENS.with(|n| n.set(0));
    }
    #[tokio::test]
    async fn sdk_fixture_identity_recall_and_underfill() {
        for (underfill, expected) in [(false, 20), (true, 17)] {
            let (_dir, ctx) = fixture();
            let mock = Arc::new(Mock {
                underfill,
                ..Mock::default()
            });
            let (service, server) = mock_service(mock.clone()).await;
            admit(&ctx).unwrap();
            publish(&ctx, &service).await.unwrap();
            assert_eq!(mock.list_calls.load(Ordering::SeqCst), 5);
            assert_eq!(ctx.receipt("published.json").unwrap()["list_calls"], 5);
            let mut original = open_artifact(&ctx.c.corpus).unwrap();
            for row in mock.corpus.lock().unwrap().values() {
                assert_eq!(
                    read_vector(&mut original)
                        .unwrap()
                        .iter()
                        .map(|v| v.to_bits())
                        .collect::<Vec<_>>(),
                    row.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
                );
            }
            assert_eq!(mock.queries.load(Ordering::SeqCst), 0);
            watch_truth(&ctx);
            let truth_hidden = ctx.c.truth.path.with_extension("hidden");
            std::fs::rename(&ctx.c.truth.path, &truth_hidden).unwrap();
            measure(&ctx, &service).await.unwrap();
            assert_eq!(TRUTH_OPENS.with(|n| n.get()), 0);
            assert_eq!(mock.queries.load(Ordering::SeqCst), 2);
            std::fs::rename(truth_hidden, &ctx.c.truth.path).unwrap();
            let summary = reduce(&ctx).unwrap();
            assert!(TRUTH_OPENS.with(|n| n.get()) > 0);
            assert_eq!(summary["hits"], expected);
            assert_eq!(summary["recall_denominator"], 20);
            assert_eq!(summary["recall_at_10"], expected as f64 / 20.0);
            assert!(measure(&ctx, &service).await.is_err());
            assert_eq!(mock.queries.load(Ordering::SeqCst), 2);
            cleanup(&ctx, &service).await.unwrap();
            assert!(mock.index_deleted.load(Ordering::SeqCst));
            assert!(mock.bucket_deleted.load(Ordering::SeqCst));
            assert_eq!(mock.delete_calls.load(Ordering::SeqCst), 2);
            let receipt = ctx.receipt("cleanup.json").unwrap();
            assert_eq!(receipt["index_absence_verified"], true);
            assert_eq!(receipt["bucket_absence_verified"], true);
            assert_eq!(
                receipt["calls"]["observed_transmit_attempts"],
                mock.calls.load(Ordering::SeqCst)
            );
            assert_eq!(receipt["calls"]["unconfirmed_reservations"], 0);
            let calls = mock.calls.load(Ordering::SeqCst);
            cleanup(&ctx, &service).await.unwrap();
            assert_eq!(mock.calls.load(Ordering::SeqCst), calls);
            server.abort();
        }
    }
    #[tokio::test]
    async fn corrupt_inputs_make_zero_api_calls() {
        let mock = Arc::new(Mock::default());
        let (service, server) = mock_service(mock.clone()).await;
        for kind in [
            "corpus",
            "queries",
            "truth",
            "trailing",
            "nan",
            "zero",
            "duplicate_truth",
        ] {
            let (_dir, mut ctx) = fixture();
            let a = match kind {
                "queries" => &mut ctx.c.queries,
                "truth" | "duplicate_truth" => &mut ctx.c.truth,
                _ => &mut ctx.c.corpus,
            };
            let mut bytes = std::fs::read(&a.path).unwrap();
            match kind {
                "trailing" => bytes.push(0),
                "nan" => bytes[..4].copy_from_slice(&f32::NAN.to_le_bytes()),
                "zero" => bytes[..D * 4].fill(0),
                "duplicate_truth" => bytes[8..16].fill(0),
                _ => bytes[0] ^= 1,
            }
            if matches!(kind, "nan" | "zero" | "duplicate_truth") {
                a.sha256 = hash(&bytes);
            }
            std::fs::write(&a.path, bytes).unwrap();
            assert!(admit(&ctx).is_err(), "{kind}");
            assert!(publish(&ctx, &service).await.is_err(), "{kind}");
        }
        let (dir, ctx) = fixture();
        let config_path = dir.path().join("config.json");
        assert!(load(&config_path, &"0".repeat(64)).is_err());
        let mut config_bytes = std::fs::read(&config_path).unwrap();
        config_bytes.push(b' ');
        std::fs::write(&config_path, config_bytes).unwrap();
        assert!(load(&config_path, &ctx.sha).is_err());
        admit(&ctx).unwrap();
        let mut changed = std::fs::read(&ctx.c.corpus.path).unwrap();
        changed[0] ^= 1;
        std::fs::write(&ctx.c.corpus.path, changed).unwrap();
        assert!(publish(&ctx, &service).await.is_err());
        assert_eq!(mock.calls.load(Ordering::SeqCst), 0);
        server.abort();
    }
    #[tokio::test]
    async fn sdk_503_is_one_attempt_and_truth_stays_unopened() {
        let (_dir, ctx) = fixture();
        let mock = Arc::new(Mock {
            fail_query: true,
            ..Mock::default()
        });
        let (service, server) = mock_service(mock.clone()).await;
        admit(&ctx).unwrap();
        publish(&ctx, &service).await.unwrap();
        watch_truth(&ctx);
        std::fs::remove_file(&ctx.c.truth.path).unwrap();
        assert!(measure(&ctx, &service).await.is_err());
        assert_eq!(mock.queries.load(Ordering::SeqCst), 2);
        assert!(!ctx.path("response-seal.json").exists());
        let raw = std::fs::read_to_string(ctx.path("responses.jsonl")).unwrap();
        assert!(raw.contains("single attempt fixture") && raw.contains("INVALID"));
        let rows: Vec<Value> = raw
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        assert!(
            rows.iter()
                .any(|r| r["capture"]["status"] == 503 && r["capture"]["attempts"] == 1)
        );
        assert!(reduce(&ctx).is_err());
        assert_eq!(TRUTH_OPENS.with(|n| n.get()), 0);
        assert!(measure(&ctx, &service).await.is_err());
        assert_eq!(mock.queries.load(Ordering::SeqCst), 2);
        server.abort();
        let (_dir, ctx) = fixture();
        let mock = Arc::new(Mock {
            oversized_response: true,
            ..Mock::default()
        });
        let (service, server) = mock_service(mock.clone()).await;
        admit(&ctx).unwrap();
        publish(&ctx, &service).await.unwrap();
        watch_truth(&ctx);
        assert!(measure(&ctx, &service).await.is_err());
        let raw = std::fs::read_to_string(ctx.path("responses.jsonl")).unwrap();
        let rows: Vec<Value> = raw
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let query = rows.iter().find(|v| v["phase"] == "query").unwrap();
        assert_eq!(query["capture"]["truncated"], true);
        assert!(query["capture"]["body_bytes"].as_u64().unwrap() > RESPONSE_CAP as u64);
        assert!(query["capture"]["body"].as_str().unwrap().len() <= 32 * 1024);
        assert_eq!(query["capture"]["attempts"], 1);
        assert_eq!(mock.queries.load(Ordering::SeqCst), 1);
        assert!(!ctx.path("response-seal.json").exists());
        assert_eq!(TRUTH_OPENS.with(|n| n.get()), 0);
        server.abort();
    }
    #[tokio::test]
    async fn sdk_readback_drift_blocks_measurement() {
        for fault in [
            "drift",
            "loop",
            "duplicate",
            "missing",
            "overflow",
            "token_overflow",
        ] {
            let (_dir, ctx) = fixture();
            let mock = Arc::new(Mock {
                drift: fault == "drift",
                pagination_fault: fault,
                ..Mock::default()
            });
            let (service, server) = mock_service(mock.clone()).await;
            admit(&ctx).unwrap();
            assert!(publish(&ctx, &service).await.is_err(), "{fault}");
            assert!(!ctx.path("published.json").exists());
            assert!(measure(&ctx, &service).await.is_err());
            assert_eq!(mock.queries.load(Ordering::SeqCst), 0);
            server.abort();
        }
    }
    #[tokio::test]
    async fn seal_sync_tamper_and_partial_refused() {
        for fault in ["sync", "directory_sync", "missing", "tamper", "partial"] {
            let (_dir, ctx) = fixture();
            let mock = Arc::new(Mock::default());
            let (service, server) = mock_service(mock.clone()).await;
            admit(&ctx).unwrap();
            publish(&ctx, &service).await.unwrap();
            watch_truth(&ctx);
            if matches!(fault, "sync" | "directory_sync") {
                FAIL_SYNC.with(|v| v.set(if fault == "sync" { 1 } else { 2 }));
                assert!(measure(&ctx, &service).await.is_err());
                FAIL_SYNC.with(|v| v.set(0));
                assert!(!ctx.path("response-seal.json").exists());
            } else {
                measure(&ctx, &service).await.unwrap();
                match fault {
                    "missing" => std::fs::remove_file(ctx.path("response-seal.json")).unwrap(),
                    "tamper" => {
                        let mut f = OpenOptions::new()
                            .append(true)
                            .open(ctx.path("responses.jsonl"))
                            .unwrap();
                        f.write_all(b"{}\n").unwrap();
                    }
                    "partial" => {
                        let p = ctx.path("response-seal.json");
                        let mut v: Value =
                            serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
                        v["data"]["count"] = json!(1);
                        std::fs::write(p, serde_json::to_vec(&v).unwrap()).unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            std::fs::remove_file(&ctx.c.truth.path).unwrap();
            assert!(reduce(&ctx).is_err());
            assert_eq!(TRUTH_OPENS.with(|n| n.get()), 0);
            assert!(!ctx.path("reduction.json").exists());
            server.abort();
        }
    }
    #[tokio::test]
    async fn sdk_cleanup_checks_ownership_and_foreign_indexes() {
        for fault in [
            "wrong_account",
            "bucket_identity",
            "index_identity",
            "create_arn",
        ] {
            let (_dir, ctx) = fixture();
            let mock = Arc::new(Mock {
                wrong_account: fault == "wrong_account",
                wrong_create_arn: fault == "create_arn",
                ..Mock::default()
            });
            mock.fail_bucket_identity_once
                .store(fault == "bucket_identity", Ordering::SeqCst);
            mock.fail_index_identity_once
                .store(fault == "index_identity", Ordering::SeqCst);
            let (service, server) = mock_service(mock.clone()).await;
            admit(&ctx).unwrap();
            assert!(publish(&ctx, &service).await.is_err(), "{fault}");
            if fault == "wrong_account" {
                assert_eq!(mock.create_calls.load(Ordering::SeqCst), 0);
                assert!(!ctx.path("bucket-create-intent.json").exists());
            } else {
                let kind = if fault == "index_identity" {
                    "index"
                } else {
                    "bucket"
                };
                assert!(ctx.path(&format!("{kind}-create-intent.json")).exists());
                let result = ctx.receipt(&format!("{kind}-create-result.json")).unwrap();
                assert_eq!(result["capture"]["status"], 200);
                assert_eq!(result["capture"]["attempts"], 1);
                assert!(
                    result["capture"]["body"]
                        .as_str()
                        .is_some_and(|s| !s.is_empty())
                );
                assert!(!ctx.path(&format!("{kind}-identity.json")).exists());
            }
            let before = mock.calls.load(Ordering::SeqCst);
            assert!(cleanup(&ctx, &service).await.is_err());
            assert_eq!(mock.calls.load(Ordering::SeqCst), before);
            assert_eq!(mock.delete_calls.load(Ordering::SeqCst), 0);
            let publish = std::fs::read_to_string(ctx.path("publish.jsonl")).unwrap();
            let terminal: Value = serde_json::from_str(publish.lines().last().unwrap()).unwrap();
            assert!(terminal["truth_opened"].is_null());
            server.abort();
        }
        for fault in [
            "foreign_index",
            "replaced_index",
            "replaced_bucket",
            "missing_index_identity",
            "missing_bucket_identity",
        ] {
            let (_dir, ctx) = fixture();
            let mock = Arc::new(Mock {
                foreign_index: fault == "foreign_index",
                ..Mock::default()
            });
            let (service, server) = mock_service(mock.clone()).await;
            admit(&ctx).unwrap();
            publish(&ctx, &service).await.unwrap();
            match fault {
                "replaced_index" => mock.replace_index.store(true, Ordering::SeqCst),
                "replaced_bucket" => mock.replace_bucket.store(true, Ordering::SeqCst),
                "missing_index_identity" => {
                    std::fs::remove_file(ctx.path("index-identity.json")).unwrap()
                }
                "missing_bucket_identity" => {
                    std::fs::remove_file(ctx.path("bucket-identity.json")).unwrap()
                }
                _ => {}
            }
            let before = mock.calls.load(Ordering::SeqCst);
            assert!(cleanup(&ctx, &service).await.is_err(), "{fault}");
            assert_eq!(mock.delete_calls.load(Ordering::SeqCst), 0, "{fault}");
            assert!(!mock.index_deleted.load(Ordering::SeqCst));
            assert!(!mock.bucket_deleted.load(Ordering::SeqCst));
            assert!(!ctx.path("cleanup.json").exists());
            if fault.starts_with("missing_") {
                assert_eq!(mock.calls.load(Ordering::SeqCst), before, "{fault}");
            }
            server.abort();
        }
        for fault in [
            "before_delete",
            "index_absence",
            "bucket_absence",
            "already_absent",
            "unconfirmed",
            "budget_exhausted",
        ] {
            let (_dir, ctx) = fixture();
            let mock = Arc::new(Mock::default());
            let (service, server) = mock_service(mock.clone()).await;
            admit(&ctx).unwrap();
            publish(&ctx, &service).await.unwrap();
            match fault {
                "before_delete" => mock.fail_cleanup_once.store(true, Ordering::SeqCst),
                "index_absence" => mock.fail_index_absence_once.store(true, Ordering::SeqCst),
                "bucket_absence" => mock.fail_bucket_absence_once.store(true, Ordering::SeqCst),
                "already_absent" => {
                    mock.index_deleted.store(true, Ordering::SeqCst);
                    mock.bucket_deleted.store(true, Ordering::SeqCst);
                }
                "unconfirmed" => {
                    let mut ledger = Calls::open(&ctx).unwrap();
                    ledger
                        .reserve("interrupted-cleanup", "GetVectorBucket")
                        .unwrap();
                }
                "budget_exhausted" => {
                    // Simulate durable reservations left by interrupted commands;
                    // these are explicitly not invented observed HTTP attempts.
                    let mut ledger = Calls::open(&ctx).unwrap();
                    let mut records = Vec::new();
                    for id in ledger.reserved + 1..=CALL_CAP {
                        serde_json::to_writer(&mut records,&json!({"phase":"reserved","call_id":id,"command":"interrupted","operation":"GetVectorBucket"})).unwrap();
                        records.push(b'\n');
                    }
                    ledger.file.write_all(&records).unwrap();
                    ledger.file.sync_all().unwrap();
                }
                _ => unreachable!(),
            }
            let before = mock.calls.load(Ordering::SeqCst);
            if fault == "budget_exhausted" {
                assert!(cleanup(&ctx, &service).await.is_err());
                assert_eq!(mock.calls.load(Ordering::SeqCst), before);
                assert_eq!(mock.delete_calls.load(Ordering::SeqCst), 0);
                server.abort();
                continue;
            }
            if matches!(fault, "before_delete" | "index_absence" | "bucket_absence") {
                assert!(cleanup(&ctx, &service).await.is_err(), "{fault}");
                let original = std::fs::read(ctx.path("cleanup-001.jsonl")).unwrap();
                assert!(String::from_utf8_lossy(&original).contains("INVALID"));
                assert!(!ctx.path("cleanup.json").exists());
                if fault == "before_delete" {
                    assert_eq!(mock.delete_calls.load(Ordering::SeqCst), 0);
                }
                if fault == "index_absence" {
                    assert!(mock.index_deleted.load(Ordering::SeqCst));
                    assert!(!mock.bucket_deleted.load(Ordering::SeqCst));
                }
                cleanup(&ctx, &service).await.unwrap();
                assert_eq!(
                    std::fs::read(ctx.path("cleanup-001.jsonl")).unwrap(),
                    original
                );
                assert!(ctx.path("cleanup-002.jsonl").exists());
            } else {
                cleanup(&ctx, &service).await.unwrap();
            }
            assert!(mock.index_deleted.load(Ordering::SeqCst));
            assert!(mock.bucket_deleted.load(Ordering::SeqCst));
            assert_eq!(
                mock.delete_calls.load(Ordering::SeqCst),
                if fault == "already_absent" { 0 } else { 2 }
            );
            let final_receipt = ctx.receipt("cleanup.json").unwrap();
            assert_eq!(final_receipt["index_absence_verified"], true);
            assert_eq!(final_receipt["bucket_absence_verified"], true);
            assert_eq!(
                final_receipt["calls"]["observed_transmit_attempts"],
                mock.calls.load(Ordering::SeqCst)
            );
            assert_eq!(
                final_receipt["calls"]["unconfirmed_reservations"],
                usize::from(fault == "unconfirmed")
            );
            let before = mock.calls.load(Ordering::SeqCst);
            cleanup(&ctx, &service).await.unwrap();
            assert_eq!(mock.calls.load(Ordering::SeqCst), before);
            assert_eq!(mock.queries.load(Ordering::SeqCst), 0);
            server.abort();
        }
    }
    #[test]
    fn response_shape_and_nearest_rank() {
        let good = json!({"vectors":[{"key":"000001","distance":0.25}]});
        assert_eq!(
            parse_hits(&serde_json::to_string(&good).unwrap(), 257)
                .unwrap()
                .len(),
            1
        );
        for bad in [
            json!({"vectors":[{"key":"1","distance":0.25}]}),
            json!({"vectors":[{"key":"000257","distance":0.25}]}),
            json!({"vectors":[{"key":"000001","distance":0.25},{"key":"000001","distance":0.25}]}),
            json!({"vectors":[],"nextToken":"unexpected"}),
        ] {
            assert!(parse_hits(&bad.to_string(), 257).is_err());
        }
        assert_eq!(
            quantiles(&(1..=20).collect::<Vec<u64>>()).unwrap(),
            (210, [10, 18, 19, 20])
        );
    }
}
