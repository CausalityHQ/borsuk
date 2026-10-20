//! Streaming exhaustive f64 cosine diagnostic for an authenticated logical-order corpus.
//! Usage: exhaustive_f64_cosine CONFIG CONFIG_SHA256 QUERY_LIMIT NEW_OUTPUT_JSON
//! CONFIG supplies rows, count, dimensions, k, metric="cosine", and corpus/queries/truth
//! {path, bytes, sha256} pins (the canonical baseline config is accepted unchanged).
//! Inputs are headerless LE f32 vectors and LE u64 truth IDs. Corpus row i is logical ID i.
//! All input bytes and rows are checked, including requests/truth outside the prefix.
//! This consumes previously admitted logical-order artifacts; it does not rerun source admission.
//! Only the selected prefix is compared; success never establishes full-request equivalence.
//! Output is create-only and valid only with the original process's zero exit status.
//! Explicit working data is conservatively capped at 256 MiB; enforce RSS externally.
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap},
    error::Error,
    fs::{File, Metadata, OpenOptions},
    io::{BufReader, Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    time::Instant,
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const CONFIG_CAP: u64 = 65_536;
const WORK_CAP: u64 = 256 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Artifact {
    path: PathBuf,
    bytes: u64,
    sha256: String,
}

// Extra baseline fields describe the previous admission, not this diagnostic.
#[derive(Clone, Deserialize)]
struct Config {
    rows: u64,
    count: usize,
    dimensions: usize,
    k: usize,
    metric: String,
    corpus: Artifact,
    queries: Artifact,
    truth: Artifact,
    canonical_input: Option<LogicalExport>,
}

#[derive(Clone, Deserialize)]
struct LogicalExport {
    logical_export: Artifact,
}

fn require(ok: bool, message: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(message.into()) }
}

fn valid_sha(sha: &str) -> bool {
    sha.len() == 64
        && sha
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

impl Config {
    fn validate(&self, limit: usize) -> Result<()> {
        require(
            self.rows > 0
                && self.count > 0
                && self.dimensions > 0
                && self.k > 0
                && self.k as u64 <= self.rows
                && limit > 0
                && limit <= self.count
                && self.metric == "cosine",
            "positive geometry, k <= rows, 0 < limit <= count, and cosine required",
        )?;
        let product = |a: u64, b: u64, c: u64| -> Result<u64> {
            a.checked_mul(b)
                .and_then(|n| n.checked_mul(c))
                .ok_or("geometry overflow".into())
        };
        for (artifact, expected) in [
            (&self.corpus, product(self.rows, self.dimensions as u64, 4)?),
            (
                &self.queries,
                product(self.count as u64, self.dimensions as u64, 4)?,
            ),
            (&self.truth, product(self.count as u64, self.k as u64, 8)?),
        ] {
            require(
                artifact.bytes == expected
                    && expected <= i64::MAX as u64
                    && valid_sha(&artifact.sha256)
                    && artifact.path.is_absolute(),
                "artifact geometry, SHA256, or absolute path invalid",
            )?;
        }
        if let Some(proof) = &self.canonical_input {
            require(
                proof.logical_export == self.corpus,
                "corpus differs from admitted logical export",
            )?;
        }
        // Covers row buffers, retained queries, heaps, truth, and JSON representation.
        let working = product(self.dimensions as u64, limit as u64 + 2, 16)?
            .checked_add(product(self.k as u64, limit as u64 + 1, 2048)?)
            .and_then(|n| n.checked_add(1024 * 1024))
            .ok_or("working memory overflow")?;
        require(
            working <= WORK_CAP,
            "geometry exceeds 256 MiB working-data cap",
        )
    }
}

fn identity(m: &Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        m.dev(),
        m.ino(),
        m.len(),
        m.mtime(),
        m.mtime_nsec(),
        m.ctime(),
        m.ctime_nsec(),
    )
}

fn regular(path: &Path) -> Result<File> {
    let f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    require(f.metadata()?.is_file(), "regular input file required")?;
    Ok(f)
}

struct Scan<'a> {
    artifact: &'a Artifact,
    reader: BufReader<File>,
    before: Metadata,
    digest: Sha256,
}

impl<'a> Scan<'a> {
    fn open(artifact: &'a Artifact) -> Result<Self> {
        let file = regular(&artifact.path)?;
        let before = file.metadata()?;
        require(before.len() == artifact.bytes, "artifact length mismatch")?;
        Ok(Self {
            artifact,
            reader: BufReader::with_capacity(65_536, file),
            before,
            digest: Sha256::new(),
        })
    }

    fn row(&mut self, bytes: &mut [u8]) -> Result<()> {
        self.reader.read_exact(bytes)?;
        self.digest.update(bytes);
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        require(self.reader.read(&mut [0])? == 0, "trailing artifact bytes")?;
        require(
            format!("{:x}", self.digest.finalize()) == self.artifact.sha256,
            "artifact SHA256 mismatch",
        )?;
        require(
            identity(&self.before) == identity(&self.reader.get_ref().metadata()?)
                && identity(&self.before) == identity(&regular(&self.artifact.path)?.metadata()?),
            "artifact changed during scan",
        )
    }
}

fn vector(bytes: &[u8], values: &mut [f64]) -> Result<f64> {
    require(
        bytes.len() == values.len().checked_mul(4).ok_or("vector width overflow")?,
        "vector width mismatch",
    )?;
    let mut squared_norm = 0.0;
    for (chunk, value) in bytes.chunks_exact(4).zip(values) {
        *value = f64::from(f32::from_le_bytes(chunk.try_into()?));
        require(value.is_finite(), "nonfinite vector")?;
        squared_norm += *value * *value;
    }
    require(
        squared_norm.is_finite() && squared_norm > 0.0,
        "nonfinite or zero vector norm",
    )?;
    Ok(squared_norm.sqrt())
}

#[derive(Clone, Copy, Debug, Serialize)]
struct Neighbor {
    id: u64,
    cosine: f64,
}

impl PartialEq for Neighbor {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.cosine == other.cosine
    }
}
impl Eq for Neighbor {}
impl PartialOrd for Neighbor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Neighbor {
    // Heap maximum is the worst retained candidate. Signed zero is a score tie.
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .cosine
            .partial_cmp(&self.cosine)
            .expect("finite cosine")
            .then_with(|| self.id.cmp(&other.id))
    }
}

fn retain(heap: &mut BinaryHeap<Neighbor>, candidate: Neighbor, k: usize) {
    if heap.len() < k {
        heap.push(candidate);
    } else if candidate < *heap.peek().expect("nonempty heap") {
        *heap.peek_mut().expect("nonempty heap") = candidate;
    }
}

fn truth_ids(bytes: &[u8], rows: u64) -> Result<Vec<u64>> {
    let ids: Vec<u64> = bytes
        .chunks_exact(8)
        .map(|b| u64::from_le_bytes(b.try_into().expect("eight bytes")))
        .collect();
    require(
        bytes.len() % 8 == 0
            && ids.iter().all(|id| *id < rows)
            && ids.iter().collect::<BTreeSet<_>>().len() == ids.len(),
        "truth ID range/duplicate/width",
    )?;
    Ok(ids)
}

fn diagnose(c: &Config, limit: usize) -> Result<serde_json::Value> {
    c.validate(limit)?;
    let started = Instant::now();
    let mut bytes = vec![0; c.dimensions * 4];
    let mut values = vec![0.0; c.dimensions];
    let mut queries = Vec::with_capacity(limit);
    let mut scan = Scan::open(&c.queries)?;
    for i in 0..c.count {
        scan.row(&mut bytes)?;
        let norm = vector(&bytes, &mut values)?;
        if i < limit {
            queries.push((values.clone(), norm));
        }
    }
    scan.finish()?;
    let mut truth = Vec::with_capacity(limit);
    let mut truth_bytes = vec![0; c.k * 8];
    let mut scan = Scan::open(&c.truth)?;
    for i in 0..c.count {
        scan.row(&mut truth_bytes)?;
        let ids = truth_ids(&truth_bytes, c.rows)?;
        if i < limit {
            truth.push(ids);
        }
    }
    scan.finish()?;
    let mut heaps: Vec<_> = (0..limit).map(|_| BinaryHeap::with_capacity(c.k)).collect();
    let mut scan = Scan::open(&c.corpus)?;
    for id in 0..c.rows {
        scan.row(&mut bytes)?;
        let norm = vector(&bytes, &mut values)?;
        for ((query, query_norm), heap) in queries.iter().zip(&mut heaps) {
            let dot: f64 = query.iter().zip(&values).map(|(q, v)| q * v).sum();
            let denominator = norm * query_norm;
            let cosine = dot / denominator;
            require(
                dot.is_finite()
                    && denominator.is_finite()
                    && denominator > 0.0
                    && cosine.is_finite(),
                "cosine overflow/nonfinite",
            )?;
            retain(heap, Neighbor { id, cosine }, c.k);
        }
    }
    scan.finish()?;
    let elapsed = started.elapsed().as_secs_f64();
    let reports: Vec<_> = heaps.into_iter().zip(truth).enumerate().map(|(query_index, (heap, pinned))| {
        let topk = heap.into_sorted_vec();
        let ids: Vec<_> = topk.iter().map(|n| n.id).collect();
        let overlap = ids.iter().filter(|id| pinned.contains(id)).count();
        let position_differences: Vec<_> = ids.iter().zip(&pinned).enumerate()
            .filter_map(|(rank, (actual, expected))| (actual != expected).then_some(rank)).collect();
        json!({"query_index":query_index, "topk":topk, "pinned_truth":pinned,
            "overlap":overlap, "overlap_fraction":overlap as f64 / c.k as f64,
            "same_order":position_differences.is_empty(), "position_differences":position_differences})
    }).collect();
    Ok(
        json!({"schema":"borsuk-exhaustive-f64-cosine-diagnostic-v1",
        "metric":"f64 cosine", "tie_break":"smaller logical ID", "logical_id":"zero-based corpus row",
        "rows":c.rows, "dimensions":c.dimensions, "k":c.k, "total_requests":c.count,
        "query_limit":limit, "full_request_equivalence_established":false,
        "limitation":"prefix diagnostic only; never establishes full-request truth equivalence",
        "all_input_bytes_authenticated":true, "elapsed_seconds":elapsed,
        "inputs":{"corpus":c.corpus,"queries":c.queries,"truth":c.truth}, "queries":reports}),
    )
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    require(
        args.len() == 4,
        "usage: exhaustive_f64_cosine CONFIG CONFIG_SHA256 QUERY_LIMIT NEW_OUTPUT_JSON",
    )?;
    let config_path = Path::new(&args[0]);
    let sha = args[1].to_str().ok_or("non-UTF8 config SHA")?;
    require(valid_sha(sha), "invalid config SHA256")?;
    let mut file = regular(config_path)?;
    let before = file.metadata()?;
    require(before.len() <= CONFIG_CAP, "config exceeds 64 KiB")?;
    let mut bytes = Vec::new();
    (&mut file).take(CONFIG_CAP + 1).read_to_end(&mut bytes)?;
    require(
        bytes.len() as u64 == before.len()
            && hash(&bytes) == sha
            && identity(&before) == identity(&file.metadata()?),
        "config SHA/length/identity mismatch",
    )?;
    let c: Config = serde_json::from_slice(&bytes)?;
    let limit: usize = args[2].to_str().ok_or("non-UTF8 query limit")?.parse()?;
    c.validate(limit)?;
    let output = Path::new(&args[3]);
    require(output.is_absolute(), "absolute output path required")?;
    match output.symlink_metadata() {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
        Ok(_) => return Err("output already exists".into()),
    }
    let mut report = diagnose(&c, limit)?;
    report["config_sha256"] = json!(sha);
    let mut out = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    serde_json::to_writer_pretty(&mut out, &report)?;
    out.write_all(b"\n")?;
    out.sync_all()?;
    File::open(output.parent().ok_or("output parent")?)?.sync_all()?;
    println!(
        "diagnostic written to {}; prefix only, not full-request equivalence",
        output.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn artifact(dir: &Path, name: &str, bytes: &[u8]) -> Artifact {
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        Artifact {
            path,
            bytes: bytes.len() as u64,
            sha256: hash(bytes),
        }
    }

    fn fixture() -> (TempDir, Config) {
        let dir = tempfile::tempdir().unwrap();
        let floats = |v: &[f32]| v.iter().flat_map(|x| x.to_le_bytes()).collect::<Vec<_>>();
        // Dot-only would prefer ID 0; cosine selects tied IDs 1,2 in logical order.
        let corpus = artifact(
            dir.path(),
            "corpus",
            &floats(&[100.0, 100.0, 2.0, 0.0, 3.0, 0.0, -1.0, 0.0]),
        );
        let queries = artifact(dir.path(), "queries", &floats(&[4.0, 0.0, 0.0, 2.0]));
        let truth = artifact(
            dir.path(),
            "truth",
            &[2u64, 1, 0, 1]
                .iter()
                .flat_map(|x| x.to_le_bytes())
                .collect::<Vec<_>>(),
        );
        (
            dir,
            Config {
                rows: 4,
                count: 2,
                dimensions: 2,
                k: 2,
                metric: "cosine".into(),
                corpus,
                queries,
                truth,
                canonical_input: None,
            },
        )
    }

    #[test]
    fn nonunit_cosine_and_logical_ties_and_comparison() {
        let (_dir, c) = fixture();
        let report = diagnose(&c, 1).unwrap();
        let q = &report["queries"][0];
        assert_eq!(q["topk"][0]["id"], 1);
        assert_eq!(q["topk"][1]["id"], 2);
        assert_eq!(q["topk"][0]["cosine"], 1.0);
        assert_eq!(q["overlap"], 2);
        assert_eq!(q["same_order"], false);
        assert_eq!(q["position_differences"], json!([0, 1]));
        assert_eq!(report["full_request_equivalence_established"], false);
        let mut heap = BinaryHeap::new();
        for id in [7, 2, 5, 1] {
            retain(&mut heap, Neighbor { id, cosine: 1.0 }, 2);
        }
        assert_eq!(
            heap.into_sorted_vec()
                .iter()
                .map(|n| n.id)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[test]
    fn rejects_bad_geometry_hash_length_and_truth_ids() {
        let (_dir, mut c) = fixture();
        assert!(c.validate(0).is_err());
        assert!(c.validate(3).is_err());
        let original = c.clone();
        c.rows = u64::MAX;
        assert!(c.validate(1).is_err());
        c = original.clone();
        c.dimensions = usize::MAX;
        assert!(c.validate(1).is_err());
        c = original.clone();
        c.corpus.sha256 = "0".repeat(64);
        assert!(diagnose(&c, 1).unwrap_err().to_string().contains("SHA256"));
        c = original.clone();
        std::fs::write(&c.corpus.path, [0; 4]).unwrap();
        assert!(diagnose(&c, 1).unwrap_err().to_string().contains("length"));
        assert!(truth_ids(&[0; 16], 4).is_err());
        assert!(truth_ids(&4u64.to_le_bytes(), 4).is_err());
        assert!(truth_ids(&[0; 9], 4).is_err());
    }

    #[test]
    fn rejects_nonfinite_zero_and_bad_unselected_requests() {
        for v in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, 0.0] {
            let bytes = v.to_le_bytes();
            assert!(vector(&bytes, &mut [0.0]).is_err());
        }
        let (_dir, mut c) = fixture();
        let bytes = [1.0f32, 0.0, f32::NAN, 1.0]
            .iter()
            .flat_map(|x| x.to_le_bytes())
            .collect::<Vec<_>>();
        std::fs::write(&c.queries.path, &bytes).unwrap();
        c.queries.sha256 = hash(&bytes);
        assert!(
            diagnose(&c, 1)
                .unwrap_err()
                .to_string()
                .contains("nonfinite")
        );
    }
}
