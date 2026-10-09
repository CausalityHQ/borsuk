//! Offline, explicitly configured Cohere cohort extraction and independent exhaustive cosine truth.
//! A complete receipt seals data identity only; it makes no ANN quality claim.

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, ListArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    basic::{Compression, Type as PhysicalType},
    file::metadata::{ColumnChunkMetaData, ParquetMetaDataReader},
};
use rustix::fs::{AtFlags, Mode, OFlags, RenameFlags, mkdirat, openat, renameat_with, unlinkat};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap},
    error::Error,
    fs::{File, Metadata},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Instant,
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;
const CONFIG_CAP: u64 = 65536;
const CONFIG_SCHEMA: &str = "borsuk-cohere-native-cohort-config-v3";
const DATASET: &str = "CohereLabs/wikipedia-2023-11-embed-multilingual-v3";
const REVISION: &str = "ade45fb52bd549f5e8c065636fe4160a43c2af36";
const PAGE_HEADER_BYTES: u64 = 65536;
const PAGE_HEADER_FIELDS: usize = 64;
const PAGE_HEADER_DEPTH: usize = 4;
const COLUMN_PAGES: usize = 65536;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Shard {
    publisher_path: String,
    path: PathBuf,
    bytes: u64,
    sha256: String,
    rows: u64,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OutputParent {
    path: PathBuf,
    device: u64,
    inode: u64,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Resources {
    cpu_limit: u64,
    actual_memory_bytes: u64,
    modeled_memory_bytes: u64,
    swap_bytes: u64,
    scratch_bytes: u64,
    timeout_seconds: u64,
    max_footer_bytes: u64,
    max_row_group_rows: u64,
    max_row_group_compressed_bytes: u64,
    max_row_group_uncompressed_bytes: u64,
    batch_rows: usize,
    max_batch_bytes: u64,
    max_id_bytes: usize,
    truth_block_rows: usize,
    caller_memory_bytes: u64,
    caller_scratch_bytes: u64,
    temporary_reserve_bytes: u64,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    dataset: String,
    revision: String,
    embedding_column: String,
    document_id_column: String,
    geometry: Geometry,
    corpus_intervals: Vec<SourceInterval>,
    reserved_query_interval: SourceInterval,
    shards: Vec<Shard>,
    output_parent: OutputParent,
    resources: Resources,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Geometry {
    corpus: usize,
    queries: usize,
    dimensions: usize,
    k: usize,
}

impl Geometry {
    fn rows(self) -> Result<usize> {
        self.corpus
            .checked_add(self.queries)
            .ok_or_else(|| "population addition overflow".into())
    }
    fn vector_bytes(self, rows: usize) -> Result<u64> {
        times(rows as u64, times(self.dimensions as u64, 4)?)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SourceInterval {
    start: usize,
    end: usize,
}
impl Config {
    fn source_end(&self) -> Result<usize> {
        Ok(self
            .corpus_intervals
            .last()
            .ok_or("corpus intervals required")?
            .end
            .max(self.reserved_query_interval.end))
    }
    fn corpus_ordinal(&self, source: usize) -> Option<usize> {
        let mut ordinal = 0;
        for interval in &self.corpus_intervals {
            if (interval.start..interval.end).contains(&source) {
                return Some(ordinal + source - interval.start);
            }
            ordinal += interval.end - interval.start;
        }
        None
    }
}
fn validate_geometry(c: &Config, g: Geometry) -> Result<()> {
    require(
        c.geometry == g
            && g.corpus > 0
            && g.corpus <= 1_000_000
            && g.queries > 0
            && g.queries <= 1000
            && g.dimensions > 0
            && g.dimensions <= 1024
            && g.k > 0
            && g.k <= g.corpus
            && g.k <= 10,
        "explicit bounded geometry",
    )?;
    let reserved = c.reserved_query_interval;
    let query_end = reserved
        .start
        .checked_add(g.queries)
        .ok_or("query interval overflow")?;
    require(
        reserved.start < reserved.end
            && reserved.end - reserved.start <= 1000
            && query_end <= reserved.end,
        "selected query prefix inside explicit reserved interval",
    )?;
    require(
        !c.corpus_intervals.is_empty() && c.corpus_intervals.len() <= 2,
        "bounded corpus intervals",
    )?;
    let mut count = 0_usize;
    let mut previous = 0;
    for interval in &c.corpus_intervals {
        require(
            interval.start < interval.end
                && interval.start >= previous
                && (interval.end <= reserved.start || interval.start >= reserved.end),
            "nonoverlapping ordered corpus/query intervals",
        )?;
        count = count
            .checked_add(interval.end - interval.start)
            .ok_or("corpus count overflow")?;
        previous = interval.end;
    }
    require(
        count == g.corpus && c.source_end()? <= 1_001_000,
        "exact bounded source population",
    )?;
    if !cfg!(test) {
        let expected = if g.corpus == 100_000 {
            vec![SourceInterval {
                start: 0,
                end: 100_000,
            }]
        } else {
            vec![
                SourceInterval {
                    start: 0,
                    end: 100_000,
                },
                SourceInterval {
                    start: 101_000,
                    end: g
                        .corpus
                        .checked_add(1000)
                        .ok_or("source population overflow")?,
                },
            ]
        };
        require(
            g.corpus >= 100_000
                && g.dimensions == 1024
                && g.k == 10
                && reserved
                    == (SourceInterval {
                        start: 100_000,
                        end: 101_000,
                    })
                && c.corpus_intervals == expected,
            "historical query exclusion and explicit scale corpus",
        )?;
    }
    times(g.corpus as u64, times(g.dimensions as u64, 4)?)?;
    times(g.queries as u64, times(g.k as u64, 8)?)?;
    Ok(())
}

fn require(ok: bool, message: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(message.into()) }
}

fn plus(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b)
        .ok_or_else(|| "resource addition overflow".into())
}
fn times(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b)
        .ok_or_else(|| "resource multiplication overflow".into())
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn valid_sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

// Resolve every directory component through pinned descriptors, rejecting links
// in ancestors as well as at the leaf. NONBLOCK makes unopened FIFOs rejectable.
fn directory(path: &Path) -> Result<File> {
    require(path.is_absolute(), "absolute directory required")?;
    let flags =
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
    let mut dir = File::from(rustix::fs::open("/", flags, Mode::empty())?);
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => dir = File::from(openat(&dir, name, flags, Mode::empty())?),
            _ => return Err("directory traversal component".into()),
        }
    }
    Ok(dir)
}

fn open_regular(path: &Path) -> Result<File> {
    let dir = directory(path.parent().ok_or("input parent")?)?;
    let name = path.file_name().ok_or("input filename")?;
    let file = File::from(openat(
        &dir,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    require(file.metadata()?.is_file(), "regular input required")?;
    Ok(file)
}

fn regular(path: &Path, bytes: u64) -> Result<File> {
    require(bytes > 0, "positive input length required")?;
    let file = open_regular(path)?;
    require(
        file.metadata()?.len() == bytes,
        "exact regular input length",
    )?;
    Ok(file)
}

fn read_config(path: &Path, sha: &str) -> Result<(Config, u64)> {
    require(valid_sha(sha), "config lowercase SHA256")?;
    let mut file = open_regular(path)?;
    let bytes = file.metadata()?.len();
    require(bytes > 0 && bytes <= CONFIG_CAP, "config byte cap")?;
    let mut body = vec![0; bytes as usize];
    file.read_exact(&mut body)?;
    require(
        file.read(&mut [0])? == 0 && hash(&body) == sha,
        "config exact EOF/SHA256",
    )?;
    Ok((serde_json::from_slice(&body)?, bytes))
}

fn validate_config(c: &Config, g: Geometry) -> Result<()> {
    validate_geometry(c, g)?;
    require(
        c.schema == CONFIG_SCHEMA && c.dataset == DATASET && c.revision == REVISION,
        "pinned schema/dataset/revision",
    )?;
    require(
        c.embedding_column == "emb"
            && !c.document_id_column.is_empty()
            && c.document_id_column != "emb"
            && c.document_id_column.len() <= 256,
        "explicit embedding/document-ID columns",
    )?;
    require(
        (1..=32).contains(&c.shards.len()) && c.output_parent.path.is_absolute(),
        "bounded shards/output parent",
    )?;
    let mut paths = BTreeSet::new();
    let mut declared_rows = 0;
    for (i, shard) in c.shards.iter().enumerate() {
        require(
            shard.publisher_path == format!("en/{i:04}.parquet")
                && shard.path.is_absolute()
                && paths.insert(&shard.path)
                && shard.bytes > 12
                && shard.bytes <= 256 * MIB
                && valid_sha(&shard.sha256)
                && shard.rows > 0
                && shard.rows <= 1_001_000,
            "ordered whole-shard descriptors with explicit expected rows",
        )?;
        declared_rows = plus(declared_rows, shard.rows)?;
    }
    require(
        declared_rows >= c.source_end()? as u64,
        "insufficient declared source rows",
    )?;
    let r = &c.resources;
    require(
        (1..=4).contains(&r.cpu_limit)
            && (1..=8 * GIB).contains(&r.actual_memory_bytes)
            && r.swap_bytes == 0
            && r.scratch_bytes > 0
            && (1..=2400).contains(&r.timeout_seconds)
            && (1..=4096).contains(&r.truth_block_rows),
        "explicit execution/resource limits",
    )?;
    require(
        r.modeled_memory_bytes > 0
            && r.modeled_memory_bytes <= r.actual_memory_bytes
            && (1..=MIB).contains(&r.max_footer_bytes)
            && (1..=100_000).contains(&r.max_row_group_rows)
            && (1..=256 * MIB).contains(&r.max_row_group_compressed_bytes)
            && (1..=GIB).contains(&r.max_row_group_uncompressed_bytes)
            && (1..=1024).contains(&r.batch_rows)
            && (1..=GIB).contains(&r.max_batch_bytes)
            && (1..=1024).contains(&r.max_id_bytes),
        "explicit decode/allocation caps",
    )
}

fn small_text(path: &Path) -> Result<String> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(CONFIG_CAP + 1)
        .read_to_end(&mut bytes)?;
    require(bytes.len() as u64 <= CONFIG_CAP, "system metadata byte cap")?;
    Ok(String::from_utf8(bytes)?)
}

struct Monitor {
    started: Instant,
    actual_cap: u64,
    timeout: u64,
}
impl Monitor {
    fn new(actual_cap: u64, timeout: u64) -> Self {
        Self {
            started: Instant::now(),
            actual_cap,
            timeout,
        }
    }
    fn time(&self) -> Result<()> {
        require(
            self.started.elapsed().as_secs() < self.timeout,
            "wall-clock cap",
        )
    }
    fn check(&self) -> Result<u64> {
        self.time()?;
        let status = small_text(Path::new("/proc/self/status"))?;
        let peak = status
            .lines()
            .find_map(|line| line.strip_prefix("VmHWM:"))
            .ok_or("missing actual peak RSS")?
            .split_whitespace()
            .next()
            .ok_or("peak RSS value")?
            .parse::<u64>()?;
        let peak = times(peak, 1024)?;
        require(peak <= self.actual_cap, "actual peak RSS cap")?;
        Ok(peak)
    }
}

// Parent owns launch limits; refuse a production invocation lacking hard cgroup
// CPU/memory/no-swap ceilings. Ancestor limits count, never silently tune them.
fn runtime_limits(r: &Resources) -> Result<serde_json::Value> {
    let membership = small_text(Path::new("/proc/self/cgroup"))?;
    let relative = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or("cgroup v2 required")?;
    let root = Path::new("/sys/fs/cgroup");
    let mut path = root.join(relative.trim_start_matches('/'));
    require(
        !Path::new(relative)
            .components()
            .any(|c| matches!(c, Component::ParentDir)),
        "cgroup path",
    )?;
    let mut memory = u64::MAX;
    let mut swap = u64::MAX;
    let mut cpus = f64::INFINITY;
    loop {
        for (name, cap) in [("memory.max", &mut memory), ("memory.swap.max", &mut swap)] {
            match small_text(&path.join(name)) {
                Ok(text) if text.trim() != "max" => *cap = (*cap).min(text.trim().parse::<u64>()?),
                Ok(_) => {}
                Err(error)
                    if path == root
                        && error
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        match small_text(&path.join("cpu.max")) {
            Ok(text) => {
                let parts = text.split_whitespace().collect::<Vec<_>>();
                require(parts.len() == 2, "CPU quota shape")?;
                if parts[0] != "max" {
                    let quota = parts[0].parse::<u64>()?;
                    let period = parts[1].parse::<u64>()?;
                    require(period > 0, "CPU quota period")?;
                    cpus = cpus.min(quota as f64 / period as f64);
                }
            }
            Err(error)
                if path == root
                    && error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) => {}
            Err(error) => return Err(error),
        }
        if path == root {
            break;
        }
        require(path.pop() && path.starts_with(root), "cgroup ancestry")?;
    }
    require(
        memory <= r.actual_memory_bytes && swap == 0 && cpus <= r.cpu_limit as f64,
        "parent hard CPU/memory/noSwap limits required",
    )?;
    Ok(
        serde_json::json!({"enforcement":"cgroup-v2", "memory_max_bytes":memory, "swap_max_bytes":swap, "cpu_quota_cores":cpus}),
    )
}

fn unchanged(file: &File, before: &Metadata) -> Result<()> {
    let after = file.metadata()?;
    require(
        after.is_file()
            && after.dev() == before.dev()
            && after.ino() == before.ino()
            && after.len() == before.len()
            && after.mtime() == before.mtime()
            && after.mtime_nsec() == before.mtime_nsec()
            && after.ctime() == before.ctime()
            && after.ctime_nsec() == before.ctime_nsec(),
        "authenticated file descriptor identity/timestamps changed",
    )
}

fn authenticate(file: &mut File, bytes: u64, sha: &str, monitor: &Monitor) -> Result<()> {
    let before = file.metadata()?;
    require(
        valid_sha(sha) && before.is_file() && before.len() == bytes,
        "whole-stream descriptor",
    )?;
    file.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut remaining = bytes;
    let mut buffer = [0; 65536];
    while remaining > 0 {
        monitor.time()?;
        let n = usize::try_from(remaining.min(buffer.len() as u64))?;
        file.read_exact(&mut buffer[..n])?;
        digest.update(&buffer[..n]);
        remaining -= n as u64;
    }
    require(
        file.read(&mut [0])? == 0 && format!("{:x}", digest.finalize()) == sha,
        "whole-stream exact EOF/SHA256",
    )?;
    unchanged(file, &before)
}

fn resident_charge(c: &Config, g: Geometry) -> Result<u64> {
    let r = &c.resources;
    // Charge extraction and truth coexistence conservatively; decoder admission adds its peak.
    let ids = times(c.source_end()? as u64, plus(r.max_id_bytes as u64, 256)?)?;
    let queries = g.vector_bytes(g.queries)?;
    let block = g.vector_bytes(r.truth_block_rows)?;
    let top = times(
        times(g.queries as u64, g.k as u64)?,
        std::mem::size_of::<Candidate>() as u64,
    )?;
    let query_norms = times(g.queries as u64, std::mem::size_of::<f64>() as u64)?;
    let heap_headers = times(
        g.queries as u64,
        std::mem::size_of::<BinaryHeap<Candidate>>() as u64,
    )?;
    let sorted_output = times(g.k as u64, std::mem::size_of::<Candidate>() as u64)?;
    let metadata = times(c.shards.len() as u64, times(128, r.max_footer_bytes)?)?;
    [
        ids,
        queries,
        block,
        top,
        query_norms,
        heap_headers,
        sorted_output,
        metadata,
        g.vector_bytes(2)?,
        64 * MIB,
        r.caller_memory_bytes,
    ]
    .into_iter()
    .try_fold(0, plus)
}

fn admit(charge: u64, resources: &Resources) -> Result<()> {
    require(
        charge <= resources.modeled_memory_bytes,
        "modeled memory cap",
    )
}

struct GroupPlan {
    index: usize,
    take: usize,
    first_row: u64,
    charge: u64,
}
struct PinnedShard {
    file: File,
    metadata: ArrowReaderMetadata,
    plans: Vec<GroupPlan>,
    rows: u64,
    footer_bytes: u64,
    device: u64,
    inode: u64,
    stamp: Metadata,
}

// Locked Parquet 58.4 keeps its header reader private. This scanner recognizes
// only page-header scalar/struct fields: no collections, payload reads, or heap
// allocation. Statistics binaries are skipped within the same header byte cap.
struct PageHeaderScan<'a> {
    file: &'a mut File,
    remaining: u64,
    used: u64,
    fields: usize,
    integers: [[Option<i64>; 9]; 4],
    present: [bool; 4],
}

impl PageHeaderScan<'_> {
    fn byte(&mut self) -> Result<u8> {
        require(
            self.used < PAGE_HEADER_BYTES && self.used < self.remaining,
            "page header byte/span cap",
        )?;
        let mut byte = [0];
        self.file.read_exact(&mut byte)?;
        self.used += 1;
        Ok(byte[0])
    }

    fn skip(&mut self, bytes: u64) -> Result<()> {
        let used = plus(self.used, bytes)?;
        require(
            used <= PAGE_HEADER_BYTES && used <= self.remaining,
            "page header byte/span cap",
        )?;
        self.file.seek(SeekFrom::Current(i64::try_from(bytes)?))?;
        self.used = used;
        Ok(())
    }

    fn unsigned(&mut self, bits: usize) -> Result<u64> {
        let mut value = 0;
        for shift in (0..bits).step_by(7) {
            let byte = self.byte()?;
            let low = u64::from(byte & 0x7f);
            require(
                low < (1_u64 << (bits - shift).min(7)),
                "page header integer bound",
            )?;
            value |= low << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err("page header integer bound".into())
    }

    fn signed(&mut self, bits: usize) -> Result<i64> {
        let value = self.unsigned(bits)?;
        Ok((value >> 1) as i64 ^ -((value & 1) as i64))
    }

    fn structure(&mut self, part: Option<usize>, depth: usize) -> Result<()> {
        require(depth <= PAGE_HEADER_DEPTH, "page header depth cap")?;
        let mut last = 0_i64;
        let mut seen = 0_u16;
        loop {
            let tag = self.byte()?;
            if tag == 0 {
                return Ok(());
            }
            self.fields += 1;
            require(self.fields <= PAGE_HEADER_FIELDS, "page header field cap")?;
            let id = if tag >> 4 == 0 {
                self.signed(16)?
            } else {
                last + i64::from(tag >> 4)
            };
            require((1..=15).contains(&id), "page header field ID")?;
            let mask = 1_u16 << id;
            require(seen & mask == 0, "duplicate page header field")?;
            seen |= mask;
            last = id;
            let id = id as usize;
            match tag & 0x0f {
                1 | 2 => {
                    if part == Some(3) && id == 7 {
                        self.integers[3][7] = Some(if tag & 0x0f == 1 { 1 } else { 0 });
                    }
                }
                3 => self.skip(1)?,
                4 => {
                    self.signed(16)?;
                }
                5 => {
                    let value = self.signed(32)?;
                    if let Some(part) = part {
                        if id < 9 {
                            self.integers[part][id] = Some(value);
                        }
                    }
                }
                6 => {
                    self.signed(64)?;
                }
                7 => self.skip(8)?,
                8 => {
                    let bytes = self.unsigned(32)?;
                    self.skip(bytes)?;
                }
                12 => {
                    let child = if part == Some(0) {
                        match id {
                            5 => Some(1),
                            7 => Some(2),
                            8 => Some(3),
                            _ => None,
                        }
                    } else {
                        None
                    };
                    if let Some(child) = child {
                        self.present[child] = true;
                    }
                    self.structure(child, depth + 1)?;
                }
                _ => return Err("unsupported page header compact type/collection".into()),
            }
        }
    }

    fn number(&self, part: usize, field: usize) -> Result<u64> {
        let value = self.integers[part][field].ok_or("required page header i32 field")?;
        require(value >= 0, "negative page header size/count/encoding")?;
        Ok(value as u64)
    }
}

fn scan_column_pages(
    file: &mut File,
    column: &ColumnChunkMetaData,
    group_rows: u64,
    stamp: &Metadata,
    monitor: &Monitor,
) -> Result<()> {
    unchanged(file, stamp)?;
    let start = u64::try_from(
        column
            .dictionary_page_offset()
            .unwrap_or(column.data_page_offset()),
    )?;
    let end = plus(start, u64::try_from(column.compressed_size())?)?;
    let uncompressed = u64::try_from(column.uncompressed_size())?;
    let values = u64::try_from(column.num_values())?;
    let mut at = start;
    let mut decoded_bytes = 0;
    let mut data_values = 0;
    let mut pages = 0;
    let mut dictionary = false;
    let mut first_data = true;
    while at < end {
        monitor.time()?;
        pages += 1;
        require(pages <= COLUMN_PAGES, "column page-count cap")?;
        file.seek(SeekFrom::Start(at))?;
        let mut header = PageHeaderScan {
            file: &mut *file,
            remaining: end - at,
            used: 0,
            fields: 0,
            integers: [[None; 9]; 4],
            present: [false; 4],
        };
        header.structure(Some(0), 0)?;
        let kind = header.number(0, 1)?;
        let raw = header.number(0, 2)?;
        let compressed = header.number(0, 3)?;
        require(
            raw > 0 && raw <= uncompressed,
            "page uncompressed-size admission",
        )?;
        let next = plus(plus(at, header.used)?, compressed)?;
        require(
            compressed > 0 && next <= end,
            "page compressed-size/span admission",
        )?;
        decoded_bytes = plus(decoded_bytes, plus(header.used, raw)?)?;
        require(
            decoded_bytes <= uncompressed,
            "column uncompressed page-total admission",
        )?;
        let part = match kind {
            0 => 1,
            2 => 2,
            3 => 3,
            _ => return Err("unsupported selected-column page type".into()),
        };
        require(
            header.present[part] && header.present.iter().filter(|&&p| p).count() == 1,
            "page header type/structure mismatch",
        )?;
        let count = header.number(part, 1)?;
        require(count > 0 && count <= values, "page value-count admission")?;
        if kind == 2 {
            require(
                !dictionary && pages == 1 && column.dictionary_page_offset() == Some(at as i64),
                "dictionary page position/count",
            )?;
            dictionary = true;
            require(
                header.number(2, 2)? == 0,
                "plain dictionary encoding required",
            )?;
            let width = if column.column_type() == PhysicalType::INT64 {
                8
            } else {
                4
            };
            require(
                times(count, width)? <= raw,
                "dictionary value/byte admission",
            )?;
        } else {
            if first_data {
                require(
                    at == u64::try_from(column.data_page_offset())?,
                    "first data page offset",
                )?;
                first_data = false;
            }
            data_values = plus(data_values, count)?;
            require(data_values <= values, "column data value-total admission")?;
            if kind == 0 {
                for field in 2..=4 {
                    header.number(1, field)?;
                }
            } else {
                let nulls = header.number(3, 2)?;
                let rows = header.number(3, 3)?;
                header.number(3, 4)?;
                let levels = plus(header.number(3, 5)?, header.number(3, 6)?)?;
                require(
                    nulls <= count && rows <= group_rows && levels <= raw && levels <= compressed,
                    "v2 page count/level admission",
                )?;
            }
        }
        if column.compression() == Compression::UNCOMPRESSED
            || (kind == 3 && header.integers[3][7] == Some(0))
        {
            require(raw == compressed, "uncompressed page byte equality")?;
        }
        // Skip the exact encoded payload span; never enter a decompressor.
        at = next;
    }
    require(
        at == end
            && decoded_bytes == uncompressed
            && data_values == values
            && dictionary == column.dictionary_page_offset().is_some()
            && !first_data,
        "column exact page spans/byte totals/value totals",
    )?;
    unchanged(file, stamp)?;
    monitor.check()?;
    Ok(())
}

fn pin_shard(
    shard: &Shard,
    c: &Config,
    g: Geometry,
    remaining: usize,
    resident: u64,
    monitor: &Monitor,
) -> Result<PinnedShard> {
    let r = &c.resources;
    let mut file = regular(&shard.path, shard.bytes)?;
    let stat = file.metadata()?;
    authenticate(&mut file, shard.bytes, &shard.sha256, monitor)?;
    unchanged(&file, &stat)?;
    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0; 4];
    file.read_exact(&mut magic)?;
    file.seek(SeekFrom::End(-8))?;
    let mut tail = [0; 8];
    file.read_exact(&mut tail)?;
    require(
        &magic == b"PAR1" && &tail[4..] == b"PAR1",
        "ordinary Parquet magic",
    )?;
    let footer_bytes = u64::from(u32::from_le_bytes(tail[..4].try_into()?));
    require(
        footer_bytes > 0
            && footer_bytes <= r.max_footer_bytes
            && plus(footer_bytes, 12)? <= shard.bytes,
        "Parquet footer admission",
    )?;
    file.seek(SeekFrom::Start(shard.bytes - footer_bytes - 8))?;
    let mut footer = vec![0; usize::try_from(footer_bytes)?];
    file.read_exact(&mut footer)?;
    let metadata = ParquetMetaDataReader::decode_metadata(&footer)?;
    require(
        metadata.num_row_groups() > 0
            && metadata.num_row_groups() <= 4096
            && metadata.file_metadata().schema_descr().num_columns() <= 64
            && metadata.file_metadata().schema().get_fields().len() <= 64,
        "metadata topology cap",
    )?;
    let metadata = ArrowReaderMetadata::try_new(Arc::new(metadata), ArrowReaderOptions::default())?;
    let schema = metadata.schema();
    require(
        schema
            .fields()
            .iter()
            .filter(|f| f.name() == &c.embedding_column)
            .count()
            == 1
            && schema
                .fields()
                .iter()
                .filter(|f| f.name() == &c.document_id_column)
                .count()
                == 1,
        "unique embedding/document-ID fields",
    )?;
    let embedding = schema.field_with_name(&c.embedding_column)?.data_type();
    match embedding {
        DataType::List(field) => require(
            field.data_type() == &DataType::Float32,
            "Float32 list required",
        )?,
        DataType::FixedSizeList(field, d) => require(
            field.data_type() == &DataType::Float32 && *d == g.dimensions as i32,
            "Float32 fixed-list dimension",
        )?,
        _ => return Err("Float32 list/fixed-list embedding schema required".into()),
    }
    require(
        matches!(
            schema.field_with_name(&c.document_id_column)?.data_type(),
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        ),
        "string/integer document-ID schema required",
    )?;
    let rows = u64::try_from(metadata.metadata().file_metadata().num_rows())?;
    require(
        rows == shard.rows,
        "authenticated footer matches explicit shard rows",
    )?;
    let mut first_row = 0;
    let mut left = remaining;
    let mut plans = Vec::new();
    for (index, group) in metadata.metadata().row_groups().iter().enumerate() {
        let group_rows = u64::try_from(group.num_rows())?;
        require(group_rows > 0, "nonempty row groups required")?;
        if left > 0 {
            require(
                group_rows <= r.max_row_group_rows,
                "row-group row admission",
            )?;
            let mut compressed = 0;
            let mut uncompressed = 0;
            let mut values = 0;
            let mut id_uncompressed = 0;
            let mut selected_leaves = 0;
            for column in group.columns() {
                let root_name = column.column_path().parts().first().ok_or("column path")?;
                if root_name != &c.embedding_column && root_name != &c.document_id_column {
                    continue;
                }
                selected_leaves += 1;
                require(
                    column.file_path().is_none(),
                    "external Parquet column paths forbidden",
                )?;
                let size = u64::try_from(column.compressed_size())?;
                let decoded = u64::try_from(column.uncompressed_size())?;
                let count = u64::try_from(column.num_values())?;
                let start = u64::try_from(
                    column
                        .dictionary_page_offset()
                        .unwrap_or(column.data_page_offset()),
                )?;
                require(
                    start >= 4
                        && size > 0
                        && decoded > 0
                        && plus(start, size)? <= shard.bytes - footer_bytes - 8,
                    "column byte range",
                )?;
                compressed = plus(compressed, size)?;
                uncompressed = plus(uncompressed, decoded)?;
                values = plus(values, count)?;
                if root_name == &c.embedding_column {
                    require(
                        column.column_type() == PhysicalType::FLOAT
                            && count <= times(group_rows, g.dimensions as u64 + 1)?,
                        "embedding leaf/value admission",
                    )?;
                } else {
                    require(count == group_rows, "document-ID value count")?;
                    id_uncompressed = decoded;
                }
            }
            require(
                selected_leaves == 2
                    && compressed <= r.max_row_group_compressed_bytes
                    && uncompressed <= r.max_row_group_uncompressed_bytes
                    && u64::try_from(group.total_byte_size())? >= uncompressed,
                "projected row-group byte admission",
            )?;
            // A dictionary string can expand once per batch row. Admit that
            // expansion and its offsets before constructing any Arrow reader.
            let id_expansion = plus(
                times(r.batch_rows as u64, id_uncompressed)?,
                times(8, plus(r.batch_rows as u64, 1)?)?,
            )?;
            // Charge the entire projected group for arbitrary variable-list
            // lengths, plus expanded string IDs rather than dictionary bytes.
            let worst_batch = plus(
                plus(times(values, 8)?, times(group_rows, 32)?)?,
                plus(id_expansion, MIB)?,
            )?;
            let nominal_batch = times(
                r.batch_rows as u64,
                plus(g.dimensions as u64 * 4, r.max_id_bytes as u64 + 128)?,
            )?;
            require(
                worst_batch <= r.max_batch_bytes && nominal_batch <= r.max_batch_bytes,
                "metadata batch allocation admission",
            )?;
            let charge = plus(
                plus(
                    plus(
                        plus(resident, times(4, plus(compressed, uncompressed)?)?)?,
                        times(24, values)?,
                    )?,
                    times(32, group_rows)?,
                )?,
                // Both expanded string construction/storage and the emitted
                // Arrow batch may coexist with dictionary/page decoder buffers.
                times(2, r.max_batch_bytes)?,
            )?;
            admit(charge, r)?;
            for column in group.columns() {
                let name = column.column_path().parts().first().ok_or("column path")?;
                if name == &c.embedding_column || name == &c.document_id_column {
                    scan_column_pages(&mut file, column, group_rows, &stat, monitor)?;
                }
            }
            let take = left.min(usize::try_from(group_rows)?);
            plans.push(GroupPlan {
                index,
                take,
                first_row,
                charge,
            });
            left -= take;
        }
        first_row = plus(first_row, group_rows)?;
    }
    require(first_row == rows, "Parquet footer/row-group row agreement")?;
    unchanged(&file, &stat)?;
    monitor.check()?;
    Ok(PinnedShard {
        file,
        metadata,
        plans,
        rows,
        footer_bytes,
        device: stat.dev(),
        inode: stat.ino(),
        stamp: stat,
    })
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "lowercase")]
enum PublisherId {
    String(String),
    Integer(String),
}

fn document_id(array: &dyn Array, row: usize, cap: usize) -> Result<PublisherId> {
    require(!array.is_null(row), "null document ID")?;
    let string = |value: &str| -> Result<PublisherId> {
        require(
            !value.is_empty() && value.len() <= cap,
            "missing/oversized document ID",
        )?;
        Ok(PublisherId::String(value.to_owned()))
    };
    macro_rules! integer {
        ($kind:ty) => {
            PublisherId::Integer(
                array
                    .as_any()
                    .downcast_ref::<$kind>()
                    .ok_or("integer array type")?
                    .value(row)
                    .to_string(),
            )
        };
    }
    let id = match array.data_type() {
        DataType::Utf8 => string(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("string array type")?
                .value(row),
        )?,
        DataType::LargeUtf8 => string(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or("large-string array type")?
                .value(row),
        )?,
        DataType::Int8 => integer!(Int8Array),
        DataType::Int16 => integer!(Int16Array),
        DataType::Int32 => integer!(Int32Array),
        DataType::Int64 => integer!(Int64Array),
        DataType::UInt8 => integer!(UInt8Array),
        DataType::UInt16 => integer!(UInt16Array),
        DataType::UInt32 => integer!(UInt32Array),
        DataType::UInt64 => integer!(UInt64Array),
        _ => return Err("document ID type".into()),
    };
    let value = match &id {
        PublisherId::String(s) | PublisherId::Integer(s) => s,
    };
    require(
        !value.is_empty() && value.len() <= cap,
        "missing/oversized document ID",
    )?;
    Ok(id)
}

fn embedding(array: &dyn Array, row: usize, dimensions: usize) -> Result<Float32Array> {
    require(!array.is_null(row), "null embedding")?;
    let values = if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        list.value(row)
    } else if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        list.value(row)
    } else {
        return Err("embedding array type".into());
    };
    let values = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or("original Float32 values required")?;
    require(
        values.len() == dimensions && values.null_count() == 0,
        "embedding dimension/null element",
    )?;
    Ok(values.clone())
}

fn squared_norm(values: &[f32]) -> Result<f64> {
    let mut norm = 0.0;
    for &v in values {
        require(v.is_finite(), "nonfinite embedding")?;
        let x = f64::from(v);
        norm += x * x;
    }
    require(norm.is_finite() && norm > 0.0, "zero/nonfinite norm")?;
    Ok(norm)
}

#[derive(Serialize)]
struct Identity<'a> {
    service_id: u64,
    source_ordinal: u64,
    corpus_ordinal: Option<u64>,
    query_ordinal: Option<u64>,
    publisher_id: &'a PublisherId,
    shard: &'a str,
    shard_row: u64,
}

#[derive(Serialize)]
struct Seal {
    name: String,
    bytes: u64,
    sha256: String,
}
struct OutputFile {
    name: &'static str,
    writer: BufWriter<File>,
    digest: Sha256,
    bytes: u64,
    cap: u64,
}
impl OutputFile {
    fn new(dir: &File, name: &'static str, cap: u64) -> Result<Self> {
        let file = File::from(openat(
            dir,
            name,
            OFlags::WRONLY
                | OFlags::CREATE
                | OFlags::EXCL
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?);
        Ok(Self {
            name,
            writer: BufWriter::with_capacity(65536, file),
            digest: Sha256::new(),
            bytes: 0,
            cap,
        })
    }
    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        let count = plus(self.bytes, bytes.len() as u64)?;
        require(count <= self.cap, "output byte cap")?;
        self.writer.write_all(bytes)?;
        self.digest.update(bytes);
        self.bytes = count;
        Ok(())
    }
    fn finish(mut self, dir: &File, monitor: &Monitor) -> Result<Seal> {
        self.writer.flush()?;
        sync(self.writer.get_ref(), "artifact")?;
        let sha256 = format!("{:x}", self.digest.finalize());
        let mut file = File::from(openat(
            dir,
            self.name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        let written = self.writer.get_ref().metadata()?;
        let read = file.metadata()?;
        require(
            written.dev() == read.dev() && written.ino() == read.ino(),
            "output descriptor identity",
        )?;
        authenticate(&mut file, self.bytes, &sha256, monitor)?;
        Ok(Seal {
            name: self.name.into(),
            bytes: self.bytes,
            sha256,
        })
    }
}

#[cfg(test)]
thread_local! { static SYNC_FAILURE: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) }; }
#[cfg(test)]
thread_local! {
    static SOURCE_MUTATION: std::cell::RefCell<Option<(PathBuf, Vec<u8>, Vec<u8>)>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn mutate_source(restore: bool) -> Result<()> {
    SOURCE_MUTATION.with(|state| {
        let mut state = state.borrow_mut();
        if restore {
            if let Some((path, original, _)) = state.take() {
                std::fs::write(path, original)?;
            }
        } else if let Some((path, _, changed)) = state.as_ref() {
            std::fs::write(path, changed)?;
            // Make the actual mutation deterministic even on coarse mtime clocks.
            File::open(path)?
                .set_times(std::fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH))?;
        }
        Ok(())
    })
}
fn sync(file: &File, _point: &'static str) -> Result<()> {
    #[cfg(test)]
    if SYNC_FAILURE.with(|f| f.get() == Some(_point)) {
        return Err("injected synchronization failure".into());
    }
    file.sync_all()?;
    Ok(())
}

#[derive(Clone, Copy)]
struct Candidate {
    distance: f64,
    ordinal: u64,
}
impl PartialEq for Candidate {
    fn eq(&self, rhs: &Self) -> bool {
        self.distance == rhs.distance && self.ordinal == rhs.ordinal
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        Some(self.cmp(rhs))
    }
}
impl Ord for Candidate {
    fn cmp(&self, rhs: &Self) -> Ordering {
        self.distance
            .partial_cmp(&rhs.distance)
            .expect("finite admitted cosine distance")
            .then(self.ordinal.cmp(&rhs.ordinal))
    }
}

fn exact_truth(
    corpus: &mut File,
    queries: &[f32],
    g: Geometry,
    block_rows: usize,
    output: &mut OutputFile,
    monitor: &Monitor,
) -> Result<()> {
    require(
        (1..=4096).contains(&block_rows)
            && (1..=1024).contains(&g.dimensions)
            && (1..=1000).contains(&g.queries)
            && (1..=10).contains(&g.k)
            && g.corpus <= 1_000_000
            && g.k <= g.corpus
            && queries.len()
                == g.queries
                    .checked_mul(g.dimensions)
                    .ok_or("query coordinates overflow")?
            && corpus.metadata()?.len() == g.vector_bytes(g.corpus)?,
        "truth exact input geometry",
    )?;
    let stamp = corpus.metadata()?;
    let block_bytes = usize::try_from(g.vector_bytes(block_rows)?)?;
    let mut block = Vec::new();
    block.try_reserve_exact(block_bytes)?;
    block.resize(block_bytes, 0_u8);
    let mut vector = Vec::new();
    vector.try_reserve_exact(g.dimensions)?;
    vector.resize(g.dimensions, 0_f32);
    let mut query_norms = Vec::new();
    query_norms.try_reserve_exact(g.queries)?;
    let mut tops: Vec<BinaryHeap<Candidate>> = Vec::new();
    tops.try_reserve_exact(g.queries)?;
    for query in queries.chunks_exact(g.dimensions) {
        query_norms.push(squared_norm(query)?.sqrt());
        let mut heap = BinaryHeap::new();
        heap.try_reserve_exact(g.k)?;
        tops.push(heap);
    }
    corpus.seek(SeekFrom::Start(0))?;
    let row_bytes = usize::try_from(g.vector_bytes(1)?)?;
    let mut ordinal = 0;
    // One corpus scan; each query's dot keeps the original sequential f64 coordinate order.
    while ordinal < g.corpus {
        monitor.check()?;
        let bytes = usize::try_from(g.vector_bytes(block_rows.min(g.corpus - ordinal))?)?;
        corpus.read_exact(&mut block[..bytes])?;
        for encoded in block[..bytes].chunks_exact(row_bytes) {
            monitor.time()?;
            for (value, bits) in vector.iter_mut().zip(encoded.chunks_exact(4)) {
                *value = f32::from_le_bytes(bits.try_into()?);
            }
            let norm = squared_norm(&vector)?.sqrt();
            for ((query, &query_norm), top) in queries
                .chunks_exact(g.dimensions)
                .zip(&query_norms)
                .zip(&mut tops)
            {
                let mut dot = 0.0_f64;
                for (&x, &y) in vector.iter().zip(query) {
                    dot += f64::from(x) * f64::from(y);
                }
                let distance = 1.0 - dot / (norm * query_norm);
                require(distance.is_finite(), "nonfinite cosine distance")?;
                let candidate = Candidate {
                    distance,
                    ordinal: ordinal as u64,
                };
                if top.len() < g.k {
                    top.push(candidate);
                } else if candidate < *top.peek().ok_or("top-k heap")? {
                    *top.peek_mut().ok_or("top-k heap")? = candidate;
                }
            }
            ordinal += 1;
        }
        unchanged(corpus, &stamp)?;
    }
    require(
        corpus.read(&mut [0])? == 0 && ordinal == g.corpus,
        "truth exact corpus EOF/count",
    )?;
    for top in tops {
        require(top.len() == g.k, "exhaustive per-query top-k cardinality")?;
        // Consumes the heap allocation; only this query's sorted output exists at a time.
        for candidate in top.into_sorted_vec() {
            output.write(&candidate.ordinal.to_le_bytes())?;
        }
    }
    monitor.check()?;
    unchanged(corpus, &stamp)
}

fn prepare(config_path: &Path, config_sha: &str, output: &Path) -> Result<serde_json::Value> {
    let (c, config_bytes) = read_config(config_path, config_sha)?;
    let g = c.geometry;
    validate_config(&c, g)?;
    let r = &c.resources;
    let monitor = Monitor::new(r.actual_memory_bytes, r.timeout_seconds);
    let enforcement = if !cfg!(test) {
        runtime_limits(r)?
    } else {
        serde_json::json!({"enforcement":"private-test-geometry"})
    };
    monitor.check()?;
    let resident = resident_charge(&c, g)?;
    admit(resident, r)?;
    let id_cap = times(
        c.source_end()? as u64,
        plus(times(6, r.max_id_bytes as u64)?, 1024)?,
    )?;
    let source_bytes = c
        .shards
        .iter()
        .try_fold(0, |n, shard| plus(n, shard.bytes))?;
    let output_cap = plus(
        plus(
            g.vector_bytes(g.rows()?)?,
            times((g.queries * g.k) as u64, 8)?,
        )?,
        plus(id_cap, CONFIG_CAP)?,
    )?;
    require(
        plus(
            plus(source_bytes, output_cap)?,
            plus(r.caller_scratch_bytes, r.temporary_reserve_bytes)?,
        )? <= r.scratch_bytes,
        "source/output scratch admission",
    )?;
    let mut pinned = Vec::new();
    let mut remaining = c.source_end()?;
    for shard in &c.shards {
        require(
            remaining > 0,
            "every declared shard must intersect the admitted source prefix",
        )?;
        let source = pin_shard(shard, &c, g, remaining, resident, &monitor)?;
        remaining -= source.plans.iter().map(|p| p.take).sum::<usize>();
        pinned.push(source);
    }
    require(remaining == 0, "missing declared cohort rows")?;
    let mut inodes = BTreeSet::new();
    for source in &pinned {
        require(
            inodes.insert((source.device, source.inode)),
            "distinct shard inodes",
        )?;
        require(
            source
                .metadata
                .schema()
                .field_with_name(&c.document_id_column)?
                .data_type()
                == pinned[0]
                    .metadata
                    .schema()
                    .field_with_name(&c.document_id_column)?
                    .data_type(),
            "consistent publisher ID type across shards",
        )?;
    }
    let parent = directory(&c.output_parent.path)?;
    let parent_stat = parent.metadata()?;
    require(
        parent_stat.dev() == c.output_parent.device
            && parent_stat.ino() == c.output_parent.inode
            && output.parent() == Some(c.output_parent.path.as_path()),
        "pinned output parent",
    )?;
    let name = output.file_name().ok_or("output directory name")?;
    require(
        output.is_absolute()
            && matches!(output.components().next_back(), Some(Component::Normal(_))),
        "new absolute output directory",
    )?;
    mkdirat(&parent, name, Mode::RUSR | Mode::WUSR | Mode::XUSR)?;
    let dir = File::from(openat(
        &parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    let result = (|| -> Result<serde_json::Value> {
        let mut queries = Vec::new();
        queries.try_reserve_exact(g.queries * g.dimensions)?;
        let mut corpus_rows = 0;
        let mut ids = BTreeSet::new();
        let mut reserved_query_digest = Sha256::new();
        let mut reserved_query_rows = 0_usize;
        let mut corpus_output = OutputFile::new(&dir, "corpus.f32", g.vector_bytes(g.corpus)?)?;
        let mut query_output = OutputFile::new(&dir, "queries.f32", g.vector_bytes(g.queries)?)?;
        let mut corpus_ids = OutputFile::new(&dir, "corpus.ids.jsonl", id_cap)?;
        let mut query_ids = OutputFile::new(&dir, "queries.ids.jsonl", id_cap)?;
        let mut source_ordinal = 0;
        let mut modeled_peak = resident;
        let mut encoded_vector = vec![0; g.dimensions * 4];
        #[cfg(test)]
        mutate_source(false)?;
        for (shard, source) in c.shards.iter().zip(&pinned) {
            unchanged(&source.file, &source.stamp)?;
            let fields = source.metadata.parquet_schema().root_schema().get_fields();
            let emb_root = fields
                .iter()
                .position(|f| f.name() == c.embedding_column)
                .ok_or("embedding root")?;
            let id_root = fields
                .iter()
                .position(|f| f.name() == c.document_id_column)
                .ok_or("document ID root")?;
            for plan in &source.plans {
                modeled_peak = modeled_peak.max(plan.charge);
                monitor.check()?;
                unchanged(&source.file, &source.stamp)?;
                let projection =
                    ProjectionMask::roots(source.metadata.parquet_schema(), [emb_root, id_root]);
                let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                    source.file.try_clone()?,
                    source.metadata.clone(),
                )
                .with_projection(projection)
                .with_row_groups(vec![plan.index])
                .with_batch_size(r.batch_rows)
                .with_limit(plan.take)
                .build()?;
                unchanged(&source.file, &source.stamp)?;
                let mut row_in_group = 0;
                for batch in reader {
                    let batch = batch?;
                    unchanged(&source.file, &source.stamp)?;
                    require(
                        batch.num_rows() > 0
                            && batch.num_rows() <= r.batch_rows
                            && batch.get_array_memory_size() as u64 <= r.max_batch_bytes,
                        "actual Arrow batch admission",
                    )?;
                    monitor.check()?;
                    let embeddings = batch
                        .column_by_name(&c.embedding_column)
                        .ok_or("embedding batch field")?;
                    let documents = batch
                        .column_by_name(&c.document_id_column)
                        .ok_or("document ID batch field")?;
                    for row in 0..batch.num_rows() {
                        require(
                            row_in_group < plan.take && source_ordinal < c.source_end()?,
                            "declared source interval only",
                        )?;
                        let values = embedding(embeddings.as_ref(), row, g.dimensions)?;
                        squared_norm(values.values())?;
                        let id = document_id(documents.as_ref(), row, r.max_id_bytes)?;
                        require(!ids.contains(&id), "duplicate publisher document ID")?;
                        for (j, value) in values.values().iter().enumerate() {
                            encoded_vector[j * 4..j * 4 + 4]
                                .copy_from_slice(&value.to_bits().to_le_bytes());
                        }
                        if (c.reserved_query_interval.start..c.reserved_query_interval.end)
                            .contains(&source_ordinal)
                        {
                            reserved_query_digest.update(&encoded_vector);
                            reserved_query_rows = reserved_query_rows
                                .checked_add(1)
                                .ok_or("reserved query row overflow")?;
                        }
                        let corpus_ordinal = c.corpus_ordinal(source_ordinal);
                        let query_ordinal = (c.reserved_query_interval.start
                            ..c.reserved_query_interval.start + g.queries)
                            .contains(&source_ordinal)
                            .then(|| source_ordinal - c.reserved_query_interval.start);
                        let identity = Identity {
                            service_id: corpus_ordinal.unwrap_or(source_ordinal) as u64,
                            source_ordinal: source_ordinal as u64,
                            corpus_ordinal: corpus_ordinal.map(|n| n as u64),
                            query_ordinal: query_ordinal.map(|n| n as u64),
                            publisher_id: &id,
                            shard: &shard.publisher_path,
                            shard_row: plan.first_row + row_in_group as u64,
                        };
                        let mut encoded_id = serde_json::to_vec(&identity)?;
                        encoded_id.push(b'\n');
                        require(
                            encoded_id.len() as u64 <= r.max_id_bytes as u64 * 6 + 1024,
                            "identity row byte cap",
                        )?;
                        if let Some(ordinal) = corpus_ordinal {
                            require(ordinal == corpus_rows, "ascending corpus ordinal")?;
                            corpus_rows += 1;
                            corpus_output.write(&encoded_vector)?;
                            if !cfg!(test) && corpus_rows == 100_000 {
                                require(
                                    format!("{:x}", corpus_output.digest.clone().finalize())
                                        == "3c95fa49a7d3f9d4bf6178f5ac2493e700a30fbcfe91da97a5fcf16a1f5fc09c",
                                    "historical original corpus prefix SHA256",
                                )?;
                            }
                            corpus_ids.write(&encoded_id)?;
                        } else if query_ordinal.is_some() {
                            queries.extend_from_slice(values.values());
                            query_output.write(&encoded_vector)?;
                            query_ids.write(&encoded_id)?;
                        }
                        ids.insert(id);
                        source_ordinal += 1;
                        row_in_group += 1;
                    }
                    unchanged(&source.file, &source.stamp)?;
                }
                require(row_in_group == plan.take, "row-group exact decoded count")?;
                unchanged(&source.file, &source.stamp)?;
            }
        }
        require(
            source_ordinal == c.source_end()?
                && corpus_rows == g.corpus
                && queries.len() == g.queries * g.dimensions
                && ids.len() == c.source_end()?,
            "fixed original cohort cardinality",
        )?;
        require(
            reserved_query_rows == c.reserved_query_interval.end - c.reserved_query_interval.start,
            "all reserved query rows validated, including unselected suffix",
        )?;
        let reserved_queries_sha256 = format!("{:x}", reserved_query_digest.finalize());
        if !cfg!(test) {
            require(
                reserved_queries_sha256
                    == "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e",
                "historical full reserved query bytes SHA256",
            )?;
        }
        let mut seals = Vec::new();
        require(
            corpus_output.bytes == g.vector_bytes(g.corpus)?
                && query_output.bytes == g.vector_bytes(g.queries)?,
            "exact original vector byte counts",
        )?;
        for file in [corpus_output, query_output, corpus_ids, query_ids] {
            seals.push(file.finish(&dir, &monitor)?);
        }
        drop(ids);
        let mut truth = OutputFile::new(&dir, "truth.u64", (g.queries * g.k * 8) as u64)?;
        let mut corpus = File::from(openat(
            &dir,
            "corpus.f32",
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        let corpus_seal = &seals[0];
        authenticate(
            &mut corpus,
            corpus_seal.bytes,
            &corpus_seal.sha256,
            &monitor,
        )?;
        exact_truth(
            &mut corpus,
            &queries,
            g,
            r.truth_block_rows,
            &mut truth,
            &monitor,
        )?;
        authenticate(
            &mut corpus,
            corpus_seal.bytes,
            &corpus_seal.sha256,
            &monitor,
        )?;
        require(
            truth.bytes == (g.queries * g.k * 8) as u64,
            "exact ground-truth byte count",
        )?;
        seals.push(truth.finish(&dir, &monitor)?);
        #[cfg(test)]
        mutate_source(true)?;
        for (source, shard) in pinned.iter().zip(&c.shards) {
            unchanged(&source.file, &source.stamp)?;
            authenticate(
                &mut source.file.try_clone()?,
                shard.bytes,
                &shard.sha256,
                &monitor,
            )?;
            unchanged(&source.file, &source.stamp)?;
        }
        for seal in &seals {
            let mut file = File::from(openat(
                &dir,
                seal.name.as_str(),
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )?);
            authenticate(&mut file, seal.bytes, &seal.sha256, &monitor)?;
        }
        sync(&dir, "before_marker")?;
        sync(&parent, "parent_before_marker")?;
        let peak = monitor.check()?;
        let sources = pinned.iter().zip(&c.shards).map(|(s, a)| serde_json::json!({"publisher_path":a.publisher_path,"path":a.path,"bytes":a.bytes,"sha256":a.sha256,"rows":s.rows,"footer_bytes":s.footer_bytes,"device":s.device,"inode":s.inode})).collect::<Vec<_>>();
        let resource_accounting = serde_json::json!({
            "id_state_bytes":times(c.source_end()? as u64, plus(r.max_id_bytes as u64, 256)?)?,
            "retained_query_vector_bytes":g.vector_bytes(g.queries)?,
            "truth_vector_block_bytes":g.vector_bytes(r.truth_block_rows)?,
            "truth_all_query_top_k_bytes":times(times(g.queries as u64, g.k as u64)?, std::mem::size_of::<Candidate>() as u64)?,
            "truth_query_norm_bytes":times(g.queries as u64, std::mem::size_of::<f64>() as u64)?,
            "truth_heap_header_bytes":times(g.queries as u64, std::mem::size_of::<BinaryHeap<Candidate>>() as u64)?,
            "truth_single_sorted_output_reserve_bytes":times(g.k as u64, std::mem::size_of::<Candidate>() as u64)?,
            "retained_shard_metadata_bytes":times(c.shards.len() as u64, times(128, r.max_footer_bytes)?)?,
            "encoded_and_decoded_row_bytes":g.vector_bytes(2)?,
            "control_output_and_failure_buffers_bytes":64 * MIB,
            "caller_memory_bytes":r.caller_memory_bytes,"resident_peak_bytes":resident,
            "admitted_decoder_peak_bytes":modeled_peak.checked_sub(resident).ok_or("decoder peak accounting")?,
            "source_bytes":source_bytes,"output_cap_bytes":output_cap,
            "caller_scratch_bytes":r.caller_scratch_bytes,"temporary_and_failure_reserve_bytes":r.temporary_reserve_bytes,
            "failure_outputs_retained_within_output_cap":true,"process_rss_is_separate":true
        });
        let receipt = serde_json::json!({
            "schema":"borsuk-cohere-native-cohort-receipt-v3", "status":"COMPLETE",
            "dataset":DATASET, "revision":REVISION,
            "columns":{"embedding":c.embedding_column,"document_id":c.document_id_column},
            "config":{"path":config_path,"bytes":config_bytes,"sha256":config_sha},
            "sources":sources,"outputs":seals,"output_parent":c.output_parent,
            "reserved_queries_sha256":reserved_queries_sha256,
            "geometry":{"corpus_rows":g.corpus,"query_rows":g.queries,"dimensions":g.dimensions,"k":g.k,"corpus_intervals":c.corpus_intervals,"reserved_query_interval":c.reserved_query_interval,"query_source_ordinals":[c.reserved_query_interval.start,c.reserved_query_interval.start+g.queries]},
            "values":"original publisher Float32 bits, little endian, unnormalized",
            "service_ids":"stable corpus ordinal as unsigned integer; query source ordinals belong to a separate query metadata namespace and are not corpus service keys",
            "metric":"cosine", "truth_arithmetic":"sequential f64 dot and squared-norm sums over original f32; 1-dot/(sqrt(cnorm2)*sqrt(qnorm2))",
            "truth_ties":"ascending corpus ordinal", "truth_method":"independent single corpus block-major exhaustive scan with bounded per-query top-k heaps; no ANN inputs",
            "resources":r,"runtime_limits":enforcement,
            "resource_accounting":resource_accounting,"modeled_peak_bytes":modeled_peak,
            "observed_peak_rss_at_receipt_bytes":peak,"elapsed_seconds_at_receipt":monitor.started.elapsed().as_secs_f64(),
            "readiness":"data identity only"
        });
        let body = serde_json::to_vec(&receipt)?;
        require(body.len() as u64 <= CONFIG_CAP, "receipt byte cap")?;
        let mut marker = OutputFile::new(&dir, "complete.json.tmp", CONFIG_CAP)?;
        marker.write(&body)?;
        marker.writer.flush()?;
        sync(marker.writer.get_ref(), "marker")?;
        let mut marker_read = File::from(openat(
            &dir,
            "complete.json.tmp",
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        let written = marker.writer.get_ref().metadata()?;
        let read = marker_read.metadata()?;
        require(
            written.dev() == read.dev() && written.ino() == read.ino(),
            "receipt descriptor identity",
        )?;
        authenticate(&mut marker_read, body.len() as u64, &hash(&body), &monitor)?;
        monitor.check()?;
        for source in &pinned {
            unchanged(&source.file, &source.stamp)?;
        }
        renameat_with(
            &dir,
            "complete.json.tmp",
            &dir,
            "complete.json",
            RenameFlags::NOREPLACE,
        )?;
        sync(&dir, "after_marker")?;
        sync(&parent, "parent")?;
        monitor.check()?;
        for source in &pinned {
            unchanged(&source.file, &source.stamp)?;
        }
        Ok(receipt)
    })();
    if result.is_err() {
        #[cfg(test)]
        let _ = mutate_source(true);
        // Failed output remains inspectable and INVALID. The final marker is
        // removed even when a post-rename directory/parent synchronization fails.
        for marker in ["complete.json", "complete.json.tmp"] {
            let _ = unlinkat(&dir, marker, AtFlags::empty());
        }
        let _ = dir.sync_all();
        let _ = parent.sync_all();
    }
    result
}

fn main() {
    let result = (|| -> Result<()> {
        let args = std::env::args_os().collect::<Vec<_>>();
        require(
            args.len() == 4,
            "usage: prepare_cohere_native_cohort CONFIG CONFIG_SHA NEW_OUTPUT_DIR",
        )?;
        let sha = args[2].to_str().ok_or("config SHA UTF8")?;
        let receipt = prepare(Path::new(&args[1]), sha, Path::new(&args[3]))?;
        println!(
            "{}",
            serde_json::json!({"status":"COMPLETE", "receipt_sha256":hash(&serde_json::to_vec(&receipt)?)})
        );
        Ok(())
    })();
    if let Err(error) = result {
        eprintln!("INVALID: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        ArrayRef, Int64Array, RecordBatch,
        builder::{FixedSizeListBuilder, Float32Builder, ListBuilder},
    };
    use arrow_schema::{Field, Schema};
    use parquet::{
        arrow::ArrowWriter,
        file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
    };
    use std::{fs, os::unix::fs::symlink};

    const TINY: Geometry = Geometry {
        corpus: 5,
        queries: 2,
        dimensions: 2,
        k: 3,
    };

    fn lists(rows: &[Option<Vec<Option<f32>>>]) -> ArrayRef {
        let mut b = ListBuilder::new(Float32Builder::new());
        for row in rows {
            if let Some(values) = row {
                for value in values {
                    b.values().append_option(*value);
                }
                b.append(true);
            } else {
                b.append(false);
            }
        }
        Arc::new(b.finish())
    }

    fn vectors<const D: usize>(values: &[[f32; D]]) -> ArrayRef {
        lists(
            &values
                .iter()
                .map(|v| Some(v.iter().copied().map(Some).collect()))
                .collect::<Vec<_>>(),
        )
    }

    fn parquet(path: &Path, emb: ArrayRef, ids: ArrayRef) -> Shard {
        let schema = Arc::new(Schema::new(vec![
            Field::new("emb", emb.data_type().clone(), true),
            Field::new("doc_id", ids.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![emb, ids]).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let body = fs::read(path).unwrap();
        Shard {
            publisher_path: String::new(),
            path: path.into(),
            bytes: body.len() as u64,
            sha256: hash(&body),
            rows: batch.num_rows() as u64,
        }
    }

    fn fixture(parent: &Path) -> Config {
        let mut first = parquet(
            &parent.join("0.parquet"),
            vectors(&[[2.0, 0.0], [0.0, 3.0], [-4.0, 0.0], [1.0, 1.0]]),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        );
        let mut second = parquet(
            &parent.join("1.parquet"),
            vectors(&[[6.0, -0.0], [1.0, 0.0], [0.0, 2.0]]),
            Arc::new(StringArray::from(vec!["e", "q0", "q1"])),
        );
        first.publisher_path = "en/0000.parquet".into();
        second.publisher_path = "en/0001.parquet".into();
        let meta = fs::metadata(parent).unwrap();
        Config {
            schema: CONFIG_SCHEMA.into(),
            dataset: DATASET.into(),
            revision: REVISION.into(),
            embedding_column: "emb".into(),
            document_id_column: "doc_id".into(),
            geometry: TINY,
            corpus_intervals: vec![SourceInterval { start: 0, end: 5 }],
            reserved_query_interval: SourceInterval { start: 5, end: 7 },
            shards: vec![first, second],
            output_parent: OutputParent {
                path: parent.into(),
                device: meta.dev(),
                inode: meta.ino(),
            },
            resources: Resources {
                cpu_limit: 4,
                actual_memory_bytes: 8 * GIB,
                modeled_memory_bytes: 512 * MIB,
                swap_bytes: 0,
                scratch_bytes: 8 * GIB,
                timeout_seconds: 2400,
                max_footer_bytes: MIB,
                max_row_group_rows: 100_000,
                max_row_group_compressed_bytes: 256 * MIB,
                max_row_group_uncompressed_bytes: GIB,
                batch_rows: 2,
                max_batch_bytes: 16 * MIB,
                max_id_bytes: 1024,
                truth_block_rows: 2,
                caller_memory_bytes: 0,
                caller_scratch_bytes: 0,
                temporary_reserve_bytes: CONFIG_CAP,
            },
        }
    }

    fn run(c: &Config, output: &Path) -> Result<serde_json::Value> {
        let body = serde_json::to_vec(c)?;
        let config_path = c.output_parent.path.join("config.json");
        fs::write(&config_path, &body)?;
        prepare(&config_path, &hash(&body), output)
    }

    fn scalar_oracle(corpus: &[f32], queries: &[f32]) -> Vec<u64> {
        // Independent full-sort scalar reference, with no production norm/dot/heap helper.
        let mut answer = Vec::new();
        for query in queries.chunks_exact(2) {
            let qnorm = (f64::from(query[0]).powi(2) + f64::from(query[1]).powi(2)).sqrt();
            let mut all = Vec::new();
            for i in 0..5 {
                let x = f64::from(corpus[2 * i]);
                let y = f64::from(corpus[2 * i + 1]);
                let distance = 1.0
                    - (x * f64::from(query[0]) + y * f64::from(query[1]))
                        / ((x * x + y * y).sqrt() * qnorm);
                all.push((distance, i as u64));
            }
            all.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));
            answer.extend(all[..3].iter().map(|v| v.1));
        }
        answer
    }

    #[test]
    fn two_shard_original_bits_scalar_truth_and_locators() {
        let t = tempfile::tempdir().unwrap();
        let c = fixture(t.path());
        let out = t.path().join("output");
        let receipt = run(&c, &out).unwrap();
        let corpus = fs::read(out.join("corpus.f32")).unwrap();
        let queries = fs::read(out.join("queries.f32")).unwrap();
        let expected = [2.0_f32, 0.0, 0.0, 3.0, -4.0, 0.0, 1.0, 1.0, 6.0, -0.0];
        assert_eq!(
            corpus,
            expected
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            queries,
            [1.0_f32, 0.0, 0.0, 2.0]
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>()
        );
        let truth = fs::read(out.join("truth.u64"))
            .unwrap()
            .chunks_exact(8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(truth, [0, 4, 3, 1, 3, 0]);
        assert_eq!(truth, scalar_oracle(&expected, &[1.0, 0.0, 0.0, 2.0]));
        let ids = fs::read_to_string(out.join("queries.ids.jsonl"))
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids[0]["source_ordinal"], 5);
        assert_eq!(ids[0]["shard"], "en/0001.parquet");
        assert_eq!(ids[0]["shard_row"], 1);
        assert_eq!(ids[0]["publisher_id"]["value"], "q0");
        assert_eq!(receipt["status"], "COMPLETE");
        for file in receipt["outputs"].as_array().unwrap() {
            let body = fs::read(out.join(file["name"].as_str().unwrap())).unwrap();
            assert_eq!(file["sha256"], hash(&body));
            assert_eq!(file["bytes"], body.len() as u64);
        }
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &fs::read(out.join("complete.json")).unwrap()
            )
            .unwrap(),
            receipt
        );
    }

    #[test]
    fn excluded_middle_three_shards_nonidentity_ids_and_truth_tail() {
        let t = tempfile::tempdir().unwrap();
        let mut c = fixture(t.path());
        c.reserved_query_interval = SourceInterval { start: 2, end: 4 };
        c.corpus_intervals = vec![
            SourceInterval { start: 0, end: 2 },
            SourceInterval { start: 4, end: 7 },
        ];
        let rows = [
            vec![[2.0, 0.0], [0.0, 3.0]],
            vec![[3.0, 0.0], [0.0, 5.0], [-4.0, 0.0]],
            vec![[1.0, 1.0], [6.0, -0.0]],
        ];
        let names = [vec!["a", "b"], vec!["q0", "q1", "c"], vec!["d", "e"]];
        c.shards = rows
            .iter()
            .zip(&names)
            .enumerate()
            .map(|(i, (rows, ids))| {
                let mut shard = parquet(
                    &t.path().join(format!("{i}.parquet")),
                    vectors(rows),
                    Arc::new(StringArray::from(ids.clone())),
                );
                shard.publisher_path = format!("en/{i:04}.parquet");
                shard
            })
            .collect();
        let expected = [2.0_f32, 0.0, 0.0, 3.0, -4.0, 0.0, 1.0, 1.0, 6.0, -0.0];
        let queries = [3.0_f32, 0.0, 0.0, 5.0];
        for block_rows in [1, 2, 3, 5, 8] {
            c.resources.truth_block_rows = block_rows;
            let out = t.path().join(format!("middle-{block_rows}"));
            let receipt = run(&c, &out).unwrap();
            assert_eq!(
                fs::read(out.join("corpus.f32")).unwrap(),
                expected
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                fs::read(out.join("queries.f32")).unwrap(),
                queries
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>()
            );
            let truth = fs::read(out.join("truth.u64"))
                .unwrap()
                .chunks_exact(8)
                .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                .collect::<Vec<_>>();
            assert_eq!(
                truth,
                scalar_oracle(&expected, &queries),
                "block_rows={block_rows}"
            );
            assert_eq!(
                receipt["resource_accounting"]["truth_all_query_top_k_bytes"],
                2 * 3 * std::mem::size_of::<Candidate>()
            );
            assert_eq!(
                receipt["resource_accounting"]["truth_query_norm_bytes"],
                2 * std::mem::size_of::<f64>()
            );
        }
        let out = t.path().join("middle-1");
        let ids = fs::read_to_string(out.join("corpus.ids.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids[2]["service_id"], 2);
        assert_eq!(ids[2]["source_ordinal"], 4);
        assert_eq!(ids[4]["publisher_id"]["value"], "e");
        assert_eq!(ids[4]["shard_row"], 1);
        for case in [
            "overlap",
            "query-overlap",
            "overflow",
            "rows",
            "block",
            "caller",
            "scratch",
            "duplicate",
        ] {
            let mut bad = c.clone();
            match case {
                "overlap" => bad.corpus_intervals[1].start = 1,
                "query-overlap" => bad.corpus_intervals[0].end = 3,
                "overflow" => bad.reserved_query_interval.start = usize::MAX,
                "rows" => bad.shards[2].rows += 1,
                "block" => bad.resources.truth_block_rows = 0,
                "caller" => bad.resources.caller_memory_bytes = u64::MAX,
                "scratch" => bad.resources.caller_scratch_bytes = u64::MAX,
                "duplicate" => {
                    let mut shard = parquet(
                        &t.path().join("duplicate.parquet"),
                        vectors(&rows[2]),
                        Arc::new(StringArray::from(vec!["d", "q0"])),
                    );
                    shard.publisher_path = "en/0002.parquet".into();
                    bad.shards[2] = shard;
                }
                _ => unreachable!(),
            }
            let out = t.path().join(case);
            assert!(run(&bad, &out).is_err(), "{case}");
            assert!(!out.join("complete.json").exists());
        }
    }

    #[test]
    fn three_coordinate_rounding_near_tie_requires_sequential_dot_order() {
        let t = tempfile::tempdir().unwrap();
        let mut c = fixture(t.path());
        c.geometry = Geometry {
            corpus: 2,
            queries: 1,
            dimensions: 3,
            k: 2,
        };
        c.corpus_intervals = vec![SourceInterval { start: 0, end: 2 }];
        c.reserved_query_interval = SourceInterval { start: 2, end: 3 };
        let large = 67_108_864_f32; // 2^26: each half is lost after the 2^52 dot term.
        let corpus = [[large, 0.0, 0.0], [large, 0.5, 0.5]];
        let query = [large, 1.0, 1.0];
        c.shards = vec![
            parquet(
                &t.path().join("0.parquet"),
                vectors(&corpus),
                Arc::new(StringArray::from(vec!["large", "halves"])),
            ),
            parquet(
                &t.path().join("1.parquet"),
                vectors(&[query]),
                Arc::new(StringArray::from(vec!["query"])),
            ),
        ];
        for (i, shard) in c.shards.iter_mut().enumerate() {
            shard.publisher_path = format!("en/{i:04}.parquet");
        }
        // Independent three-coordinate norm and full-sort oracle; reverse dots expose rounding.
        let norm = |v: [f32; 3]| {
            let [x, y, z] = v.map(f64::from);
            ((x * x + y * y) + z * z).sqrt()
        };
        let scalar_rank = |order: [usize; 3]| {
            let mut distances = corpus
                .iter()
                .enumerate()
                .map(|(ordinal, &vector)| {
                    let mut dot = 0.0_f64;
                    for j in order {
                        dot += f64::from(vector[j]) * f64::from(query[j]);
                    }
                    (1.0 - dot / (norm(vector) * norm(query)), ordinal as u64)
                })
                .collect::<Vec<_>>();
            distances.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));
            distances
                .into_iter()
                .map(|(_, ordinal)| ordinal)
                .collect::<Vec<_>>()
        };
        let expected = scalar_rank([0, 1, 2]);
        assert_eq!(expected, [0, 1]);
        assert_eq!(scalar_rank([2, 1, 0]), [1, 0]);
        for block_rows in [1, 2] {
            c.resources.truth_block_rows = block_rows;
            let out = t.path().join(format!("rounding-{block_rows}"));
            run(&c, &out).unwrap();
            let truth = fs::read(out.join("truth.u64"))
                .unwrap()
                .chunks_exact(8)
                .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                .collect::<Vec<_>>();
            assert_eq!(truth, expected, "block_rows={block_rows}");
        }
    }

    #[test]
    fn selected_prefix_validates_and_excludes_entire_reserved_suffix() {
        let t = tempfile::tempdir().unwrap();
        let mut c = fixture(t.path());
        c.geometry.queries = 1;
        let out = t.path().join("prefix");
        let receipt = run(&c, &out).unwrap();
        let expected_query = [1.0_f32, 0.0]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<_>>();
        let reserved = [1.0_f32, 0.0, 0.0, 2.0]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<_>>();
        assert_eq!(fs::read(out.join("queries.f32")).unwrap(), expected_query);
        assert_eq!(receipt["reserved_queries_sha256"], hash(&reserved));
        assert_eq!(
            receipt["geometry"]["reserved_query_interval"],
            serde_json::json!({"start":5,"end":7})
        );
        assert_eq!(
            fs::read_to_string(out.join("queries.ids.jsonl"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let corpus = fs::read(out.join("corpus.f32"))
            .unwrap()
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        let truth = fs::read(out.join("truth.u64"))
            .unwrap()
            .chunks_exact(8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(truth, scalar_oracle(&corpus, &[1.0, 0.0]));
        for case in [
            "duplicate-suffix",
            "invalid-suffix",
            "reserved-overlap",
            "prefix-outside",
        ] {
            let mut bad = c.clone();
            match case {
                "duplicate-suffix" | "invalid-suffix" => {
                    let tail = if case == "invalid-suffix" {
                        [0.0, 0.0]
                    } else {
                        [0.0, 2.0]
                    };
                    let id = if case == "duplicate-suffix" {
                        "a"
                    } else {
                        "q1"
                    };
                    let mut shard = parquet(
                        &t.path().join(format!("{case}.parquet")),
                        vectors(&[[6.0, -0.0], [1.0, 0.0], tail]),
                        Arc::new(StringArray::from(vec!["e", "q0", id])),
                    );
                    shard.publisher_path = "en/0001.parquet".into();
                    bad.shards[1] = shard;
                }
                "reserved-overlap" => bad.reserved_query_interval.start = 4,
                "prefix-outside" => bad.geometry.queries = 3,
                _ => unreachable!(),
            }
            let out = t.path().join(case);
            assert!(run(&bad, &out).is_err(), "{case}");
            assert!(!out.join("complete.json").exists());
        }
    }

    #[test]
    fn fixed_list_and_integer_ids() {
        let t = tempfile::tempdir().unwrap();
        let mut c = fixture(t.path());
        for (index, values) in [
            vec![[2.0, 0.0], [0.0, 3.0], [-4.0, 0.0], [1.0, 1.0]],
            vec![[6.0, -0.0], [1.0, 0.0], [0.0, 2.0]],
        ]
        .iter()
        .enumerate()
        {
            let mut b = FixedSizeListBuilder::new(Float32Builder::new(), 2);
            for v in values {
                b.values().append_slice(v);
                b.append(true);
            }
            let start = if index == 0 { 0 } else { 4 };
            let ids = Arc::new(Int64Array::from_iter_values(
                (start..start + values.len()).map(|i| i as i64 - 2),
            ));
            let mut shard = parquet(&c.shards[index].path, Arc::new(b.finish()), ids);
            shard.publisher_path = c.shards[index].publisher_path.clone();
            c.shards[index] = shard;
        }
        let out = t.path().join("fixed");
        run(&c, &out).unwrap();
        let ids = fs::read_to_string(out.join("corpus.ids.jsonl")).unwrap();
        let row: serde_json::Value = serde_json::from_str(ids.lines().next().unwrap()).unwrap();
        assert_eq!(
            row["publisher_id"],
            serde_json::json!({"kind":"integer","value":"-2"})
        );
        assert_eq!(row["service_id"], 0);
    }

    #[test]
    fn invalid_embeddings_and_duplicate_ids() {
        let cases = vec![
            None,
            Some(vec![Some(1.0)]),
            Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
            Some(vec![None, Some(1.0)]),
            Some(vec![Some(f32::NAN), Some(1.0)]),
            Some(vec![Some(f32::INFINITY), Some(1.0)]),
            Some(vec![Some(0.0), Some(-0.0)]),
        ];
        for (i, bad) in cases.into_iter().enumerate() {
            let t = tempfile::tempdir().unwrap();
            let mut c = fixture(t.path());
            let mut shard = parquet(
                &c.shards[1].path,
                lists(&[
                    Some(vec![Some(6.0), Some(-0.0)]),
                    bad,
                    Some(vec![Some(0.0), Some(2.0)]),
                ]),
                Arc::new(StringArray::from(vec!["e", "q0", "q1"])),
            );
            shard.publisher_path = c.shards[1].publisher_path.clone();
            c.shards[1] = shard;
            let out = t.path().join(format!("bad-{i}"));
            assert!(run(&c, &out).is_err());
            assert!(!out.join("complete.json").exists());
        }
        for ids in [
            vec![Some("e"), Some("a"), Some("q1")],
            vec![Some("e"), None, Some("q1")],
            vec![Some("e"), Some(""), Some("q1")],
        ] {
            let t = tempfile::tempdir().unwrap();
            let mut c = fixture(t.path());
            let mut shard = parquet(
                &c.shards[1].path,
                vectors(&[[6.0, 0.0], [1.0, 0.0], [0.0, 2.0]]),
                Arc::new(StringArray::from(ids)),
            );
            shard.publisher_path = c.shards[1].publisher_path.clone();
            c.shards[1] = shard;
            let out = t.path().join("bad-id");
            assert!(run(&c, &out).is_err());
            assert!(!out.join("complete.json").exists());
        }
    }

    #[test]
    fn strict_config_and_missing_columns() {
        let t = tempfile::tempdir().unwrap();
        let c = fixture(t.path());
        let mut v = serde_json::to_value(&c).unwrap();
        v["unexpected"] = true.into();
        assert!(serde_json::from_value::<Config>(v).is_err());
        let mut v = serde_json::to_value(&c).unwrap();
        v["resources"]["unexpected"] = true.into();
        assert!(serde_json::from_value::<Config>(v).is_err());
        for column in ["absent", "emb"] {
            let mut changed = c.clone();
            changed.document_id_column = column.into();
            assert!(run(&changed, &t.path().join(column)).is_err());
        }
        let mut changed = c.clone();
        changed.revision = "not-the-pinned-revision".into();
        assert!(run(&changed, &t.path().join("revision")).is_err());
        let mut b = ListBuilder::new(arrow_array::builder::Float64Builder::new());
        for _ in 0..4 {
            b.values().append_slice(&[1.0, 0.0]);
            b.append(true);
        }
        let mut changed = c.clone();
        let mut shard = parquet(
            &changed.shards[0].path,
            Arc::new(b.finish()),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        );
        shard.publisher_path = changed.shards[0].publisher_path.clone();
        changed.shards[0] = shard;
        assert!(run(&changed, &t.path().join("f64")).is_err());
    }

    #[test]
    fn secure_hash_eof_link_fifo_inputs() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("body");
        fs::write(&path, b"abc").unwrap();
        let mut file = regular(&path, 3).unwrap();
        let monitor = Monitor::new(8 * GIB, 2400);
        assert!(authenticate(&mut file, 3, &hash(b"abc"), &monitor).is_ok());
        assert!(authenticate(&mut file, 3, &hash(b"abd"), &monitor).is_err());
        fs::write(&path, b"abcd").unwrap();
        assert!(authenticate(&mut file, 3, &hash(b"abc"), &monitor).is_err());
        fs::write(&path, b"ab").unwrap();
        assert!(authenticate(&mut file, 3, &hash(b"abc"), &monitor).is_err());
        let link = t.path().join("link");
        symlink(&path, &link).unwrap();
        assert!(regular(&link, 2).is_err());
        let dir_link = t.path().join("dir-link");
        symlink(t.path(), &dir_link).unwrap();
        assert!(regular(&dir_link.join("body"), 2).is_err());
        let fifo = t.path().join("fifo");
        rustix::fs::mkfifoat(rustix::fs::CWD, &fifo, Mode::RUSR | Mode::WUSR).unwrap();
        assert!(regular(&fifo, 1).is_err());
        assert!(regular(t.path(), 1).is_err());

        let c = fixture(t.path());
        let original = fs::read(&c.shards[0].path).unwrap();
        let changed_path = t.path().join("changed.parquet");
        parquet(
            &changed_path,
            vectors(&[[8.0, 0.0], [0.0, 3.0], [-4.0, 0.0], [1.0, 1.0]]),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        );
        let changed = fs::read(&changed_path).unwrap();
        assert_eq!(changed.len(), original.len());
        assert_ne!(hash(&changed), hash(&original));
        SOURCE_MUTATION.with(|state| {
            *state.borrow_mut() = Some((c.shards[0].path.clone(), original.clone(), changed));
        });
        let out = t.path().join("mutate-and-restore");
        let error = run(&c, &out).unwrap_err().to_string();
        assert!(
            error.contains("descriptor identity/timestamps changed"),
            "{error}"
        );
        assert_eq!(fs::read(&c.shards[0].path).unwrap(), original);
        assert!(!out.join("complete.json").exists());

        // Restoring both bytes and mtime must still fail the retained ctime pin.
        let mut file = regular(&c.shards[0].path, c.shards[0].bytes).unwrap();
        let stamp = file.metadata().unwrap();
        authenticate(&mut file, c.shards[0].bytes, &c.shards[0].sha256, &monitor).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        fs::write(&c.shards[0].path, b"changed").unwrap();
        fs::write(&c.shards[0].path, &original).unwrap();
        file.set_times(std::fs::FileTimes::new().set_modified(stamp.modified().unwrap()))
            .unwrap();
        assert_eq!(file.metadata().unwrap().mtime(), stamp.mtime());
        assert_eq!(file.metadata().unwrap().mtime_nsec(), stamp.mtime_nsec());
        authenticate(&mut file, c.shards[0].bytes, &c.shards[0].sha256, &monitor).unwrap();
        assert!(unchanged(&file, &stamp).is_err());
    }

    #[test]
    #[allow(deprecated)] // Locked 58.4 public Thrift output API, used only to forge the fixture.
    fn metadata_and_batch_memory_caps() {
        for field in [
            "modeled",
            "footer",
            "rows",
            "compressed",
            "uncompressed",
            "batch",
            "id",
        ] {
            let t = tempfile::tempdir().unwrap();
            let mut c = fixture(t.path());
            match field {
                "modeled" => c.resources.modeled_memory_bytes = 1,
                "footer" => c.resources.max_footer_bytes = 1,
                "rows" => c.resources.max_row_group_rows = 1,
                "compressed" => c.resources.max_row_group_compressed_bytes = 1,
                "uncompressed" => c.resources.max_row_group_uncompressed_bytes = 1,
                "batch" => c.resources.max_batch_bytes = 1,
                "id" => c.resources.max_id_bytes = 1,
                _ => unreachable!(),
            }
            let out = t.path().join(field);
            assert!(run(&c, &out).is_err(), "{field}");
            assert!(!out.join("complete.json").exists());
        }
        assert!(Monitor::new(1, 2400).check().is_err());
        assert!(Monitor::new(8 * GIB, 0).check().is_err());

        let t = tempfile::tempdir().unwrap();
        let mut c = fixture(t.path());
        let emb = vectors(&[[2.0, 0.0], [0.0, 3.0], [-4.0, 0.0], [1.0, 1.0]]);
        let oversized = "x".repeat(768 * 1024);
        let ids: ArrayRef = Arc::new(StringArray::from(vec![oversized.as_str(); 4]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("emb", emb.data_type().clone(), false),
            Field::new("doc_id", DataType::Utf8, false),
        ]));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .set_dictionary_enabled(true)
            .set_dictionary_page_size_limit(1024 * 1024)
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let mut writer = ArrowWriter::try_new(
            File::create(&c.shards[0].path).unwrap(),
            schema.clone(),
            Some(props),
        )
        .unwrap();
        writer
            .write(&RecordBatch::try_new(schema, vec![emb, ids]).unwrap())
            .unwrap();
        writer.close().unwrap();
        let body = fs::read(&c.shards[0].path).unwrap();
        c.shards[0].bytes = body.len() as u64;
        c.shards[0].sha256 = hash(&body);
        c.resources.batch_rows = 4;
        c.resources.max_batch_bytes = 2 * MIB;
        let metadata = ArrowReaderMetadata::load(
            &File::open(&c.shards[0].path).unwrap(),
            ArrowReaderOptions::default(),
        )
        .unwrap();
        assert!(
            metadata
                .metadata()
                .row_group(0)
                .column(1)
                .dictionary_page_offset()
                .is_some()
        );
        let out = t.path().join("dictionary-expansion");
        let error = run(&c, &out).unwrap_err().to_string();
        // Require refusal from metadata, before any Arrow reader/body decode.
        assert!(
            error.contains("metadata batch allocation admission"),
            "{error}"
        );
        assert!(!out.exists());

        // Authenticated bytes with a contradictory compressed-page allocation
        // request: preserve all footer spans by shortening its compressed payload.
        use parquet::{
            format::{DataPageHeader, Encoding, PageHeader, PageType},
            thrift::{TCompactOutputProtocol, TSerializable},
        };
        let t = tempfile::tempdir().unwrap();
        let mut c = fixture(t.path());
        let emb = vectors(&[[2.0, 0.0], [0.0, 3.0], [-4.0, 0.0], [1.0, 1.0]]);
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("emb", emb.data_type().clone(), false),
            Field::new("doc_id", DataType::Utf8, false),
        ]));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(
            File::create(&c.shards[0].path).unwrap(),
            schema.clone(),
            Some(props),
        )
        .unwrap();
        writer
            .write(&RecordBatch::try_new(schema, vec![emb, ids]).unwrap())
            .unwrap();
        writer.close().unwrap();
        let mut body = fs::read(&c.shards[0].path).unwrap();
        let metadata = ArrowReaderMetadata::load(
            &File::open(&c.shards[0].path).unwrap(),
            ArrowReaderOptions::default(),
        )
        .unwrap();
        let column = metadata.metadata().row_group(0).column(0);
        let mut source = File::open(&c.shards[0].path).unwrap();
        let stamp = source.metadata().unwrap();
        scan_column_pages(&mut source, column, 2, &stamp, &Monitor::new(8 * GIB, 2400)).unwrap();
        let start = usize::try_from(column.data_page_offset()).unwrap();
        assert_eq!(&body[start..start + 3], &[0x15, 0, 0x15]);
        assert_eq!(body[start + 4], 0x15);
        assert_eq!(body[start + 3] & 0x81, 0);
        assert_eq!(body[start + 5] & 0x81, 0);
        let raw = i32::from(body[start + 3] >> 1);
        let compressed = i32::from(body[start + 5] >> 1);
        let mut page = PageHeader::new(
            PageType::DATA_PAGE,
            raw,
            compressed,
            None,
            Some(DataPageHeader::new(
                i32::try_from(column.num_values()).unwrap(),
                Encoding::PLAIN,
                Encoding::RLE,
                Encoding::RLE,
                None,
            )),
            None,
            None,
            None,
        );
        let encode = |p: &PageHeader| {
            let mut bytes = Vec::new();
            p.write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut bytes))
                .unwrap();
            bytes
        };
        let original_header = encode(&page);
        assert_eq!(&body[start..start + original_header.len()], original_header);
        assert_eq!(
            column.compressed_size(),
            original_header.len() as i64 + i64::from(compressed)
        );
        page.uncompressed_page_size = i32::MAX;
        let extra = encode(&page).len() - original_header.len();
        page.compressed_page_size -= i32::try_from(extra).unwrap();
        assert!(page.compressed_page_size > 0);
        let mut replacement = encode(&page);
        replacement.extend_from_slice(
            &body[start + original_header.len()
                ..start + original_header.len() + page.compressed_page_size as usize],
        );
        assert_eq!(replacement.len(), column.compressed_size() as usize);
        body[start..start + replacement.len()].copy_from_slice(&replacement);
        fs::write(&c.shards[0].path, &body).unwrap();
        c.shards[0].bytes = body.len() as u64;
        c.shards[0].sha256 = hash(&body);
        let out = t.path().join("contradictory-compressed-page");
        let error = run(&c, &out).unwrap_err().to_string();
        assert!(
            error.contains("page uncompressed-size admission"),
            "{error}"
        );
        assert!(!out.exists());
    }

    #[test]
    fn missing_rows_and_occupied_output() {
        let t = tempfile::tempdir().unwrap();
        let mut c = fixture(t.path());
        let occupied = t.path().join("occupied");
        fs::create_dir(&occupied).unwrap();
        fs::write(occupied.join("keep"), b"keep").unwrap();
        assert!(run(&c, &occupied).is_err());
        assert_eq!(fs::read(occupied.join("keep")).unwrap(), b"keep");
        let linked = t.path().join("linked");
        symlink(&occupied, &linked).unwrap();
        assert!(run(&c, &linked).is_err());
        let mut shard = parquet(
            &c.shards[1].path,
            vectors(&[[6.0, 0.0], [1.0, 0.0]]),
            Arc::new(StringArray::from(vec!["e", "q0"])),
        );
        shard.publisher_path = c.shards[1].publisher_path.clone();
        c.shards[1] = shard;
        let out = t.path().join("short");
        assert!(run(&c, &out).is_err());
        assert!(!out.join("complete.json").exists());
        c.output_parent.inode += 1;
        assert!(run(&c, &t.path().join("wrong-parent")).is_err());
    }

    #[test]
    fn sync_failure_removes_complete_marker() {
        for point in [
            "artifact",
            "before_marker",
            "parent_before_marker",
            "marker",
            "after_marker",
            "parent",
        ] {
            let t = tempfile::tempdir().unwrap();
            let c = fixture(t.path());
            let out = t.path().join("sync-failure");
            SYNC_FAILURE.with(|f| f.set(Some(point)));
            let result = run(&c, &out);
            SYNC_FAILURE.with(|f| f.set(None));
            assert!(result.is_err(), "{point}");
            assert!(!out.join("complete.json").exists(), "{point}");
        }
    }
}
