//! Report-only lossless SOURCE frame falsifier; never opens requests or truth.
//! Usage: check_source_frame --mode MODE --config PATH --report NEW_PATH
//! synthetic-canary instead requires only --report NEW_PATH and uses disposable files.
use borsuk::{
    source_frame::{self, FRAME_ROWS, HEADER_BYTES, Limits},
    sq8_page_authority::PageAuthority,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, Metadata},
    io::{self, Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::Instant,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
fn fail(s: &str) -> Box<dyn std::error::Error> {
    io::Error::new(io::ErrorKind::InvalidData, s).into()
}
fn hash(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}
fn valid_hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    path: PathBuf,
    bytes: u64,
    sha256: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    generation: Artifact,
    plane: Artifact,
    digests: Artifact,
    records: Artifact,
    rows: usize,
    dimensions: usize,
    max_source_bytes: u64,
    max_metadata_bytes: usize,
    max_frame_bytes: usize,
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
struct Pinned {
    file: File,
    path: PathBuf,
    before: Metadata,
}
impl Pinned {
    fn open(a: &Artifact, cap: u64) -> Result<Self> {
        if !a.path.is_absolute()
            || a.path.as_os_str().len() > 4096
            || !valid_hash(&a.sha256)
            || a.bytes == 0
            || a.bytes > cap
        {
            return Err(fail("artifact contract/cap"));
        }
        let before = fs::symlink_metadata(&a.path)?;
        if !before.is_file() || before.len() != a.bytes || fs::canonicalize(&a.path)? != a.path {
            return Err(fail("artifact path/type/length"));
        }
        let file = File::open(&a.path)?;
        if identity(&before) != identity(&file.metadata()?) {
            return Err(fail("artifact changed at open"));
        }
        Ok(Self {
            file,
            path: a.path.clone(),
            before,
        })
    }
    fn check(&self) -> Result<()> {
        if identity(&self.before) != identity(&self.file.metadata()?)
            || identity(&self.before) != identity(&fs::symlink_metadata(&self.path)?)
        {
            return Err(fail("artifact changed"));
        }
        Ok(())
    }
}
fn read_artifact(a: &Artifact, cap: usize) -> Result<Vec<u8>> {
    let mut p = Pinned::open(a, cap as u64)?;
    let mut bytes = Vec::with_capacity(a.bytes as usize);
    (&mut p.file).take(a.bytes + 1).read_to_end(&mut bytes)?;
    p.check()?;
    if bytes.len() as u64 != a.bytes || hash(&bytes) != a.sha256 {
        return Err(fail("artifact hash/length"));
    }
    Ok(bytes)
}
fn read_config(path: &Path) -> Result<Config> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() > 65536 {
        return Err(fail("config cap"));
    }
    let bytes = fs::read(path)?;
    if bytes.len() > 65536 {
        return Err(fail("config cap"));
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn cpu_ns() -> u128 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    (t.tv_sec as u128) * 1_000_000_000 + t.tv_nsec as u128
}
fn resource_snapshot() -> Value {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let status: BTreeMap<_, _> = status
        .lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(k, _)| ["VmHWM", "VmRSS", "VmSwap", "Threads"].contains(k))
        .map(|(k, v)| (k.to_owned(), v.trim().to_owned()))
        .collect();
    let cgroup = fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    let relative = cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .unwrap_or("");
    let base = Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/'));
    let counters: BTreeMap<_, _> = [
        "memory.max",
        "memory.peak",
        "memory.swap.max",
        "memory.events",
        "cpu.max",
        "cpu.stat",
        "cpuset.cpus.effective",
        "pids.max",
        "pids.peak",
    ]
    .into_iter()
    .filter_map(|name| fs::read_to_string(base.join(name)).ok().map(|v| (name, v)))
    .collect();
    json!({"process_status":status,"cgroup":cgroup,"cgroup_counters":counters})
}
fn execute(c: &Config, measure: bool) -> Result<Value> {
    let start = Instant::now();
    let cpu_start = cpu_ns();
    if c.schema != "source-frame-config-v1"
        || c.rows == 0
        || c.dimensions == 0
        || c.max_metadata_bytes == 0
        || c.max_metadata_bytes > 64 * 1024 * 1024
        || c.max_frame_bytes == 0
        || c.max_frame_bytes > 1 << 20
        || c.max_source_bytes == 0
        || c.max_source_bytes > 1 << 40
    {
        return Err(fail("configuration bounds"));
    }
    let nframes = c.rows.div_ceil(FRAME_ROWS);
    let size_bytes = nframes
        .checked_mul(std::mem::size_of::<usize>())
        .ok_or_else(|| fail("frame count overflow"))?;
    if nframes > 1_000_000 || size_bytes > c.max_metadata_bytes {
        return Err(fail("frame distribution cap"));
    }
    let metadata_total = c
        .generation
        .bytes
        .checked_add(c.plane.bytes)
        .and_then(|n| n.checked_add(c.digests.bytes))
        .and_then(|n| n.checked_add(size_bytes as u64))
        .ok_or_else(|| fail("metadata overflow"))?;
    if metadata_total > c.max_metadata_bytes as u64 {
        return Err(fail("metadata total cap"));
    }
    let generation_bytes = read_artifact(&c.generation, c.max_metadata_bytes)?;
    let plane_bytes = read_artifact(&c.plane, c.max_metadata_bytes)?;
    let generation: Value = serde_json::from_slice(&generation_bytes)?;
    let plane: Value = serde_json::from_slice(&plane_bytes)?;
    if generation["schema"] != "borsuk-two-bit-generation-v11"
        || generation["source_layout"] != json!({"layout":"flat_records"})
        || generation["plane_manifest_sha256"] != c.plane.sha256
        || generation["discovery"]["input_root_sha256"] != c.plane.sha256
        || generation["discovery"]["records_sha256"] != c.records.sha256
        || plane["records_sha256"] != c.records.sha256
        || plane["page_digest_sha256"] != c.digests.sha256
    {
        return Err(fail("generation/plane/records authority chain"));
    }
    let sidecar = read_artifact(&c.digests, c.max_metadata_bytes)?;
    let authority = PageAuthority::load_two_bit(
        &plane_bytes,
        &c.plane.sha256,
        generation["generation"]
            .as_u64()
            .ok_or_else(|| fail("generation number"))?,
        &sidecar,
    )
    .map_err(|e| fail(&format!("authority {e:?}")))?;
    if authority.rows() != c.rows
        || authority.dimensions() != c.dimensions
        || authority.object_bytes() as u64 != c.records.bytes
        || c.records.bytes > c.max_source_bytes
    {
        return Err(fail("authority geometry/source cap"));
    }
    let width = plane["record_bytes"]
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| fail("record width"))?;
    if FRAME_ROWS
        .checked_mul(width)
        .is_none_or(|n| n > c.max_frame_bytes)
    {
        return Err(fail("frame capacity"));
    }
    let limits = Limits {
        max_decoded_bytes: c.max_frame_bytes,
    };
    let mut records = Pinned::open(&c.records, c.max_source_bytes)?;
    let mut full_hash = Sha256::new();
    let mut input_hash = Sha256::new();
    let mut decoded_hash = Sha256::new();
    let mut sizes = Vec::with_capacity(if measure { nframes } else { 0 });
    let mut fallback = 0usize;
    let mut payload_bytes = 0u64;
    let mut encode_wall = 0u128;
    let mut decode_wall = 0u128;
    let mut encode_cpu = 0u128;
    let mut decode_cpu = 0u128;
    for first in (0..c.rows).step_by(FRAME_ROWS) {
        let rows = FRAME_ROWS.min(c.rows - first);
        let mut original = vec![0u8; rows * width];
        records.file.read_exact(&mut original)?;
        authority
            .verify_payload(first / 32, (first + rows - 1) / 32, &original)
            .map_err(|e| fail(&format!("original unit {e:?}")))?;
        full_hash.update(&original);
        if measure {
            input_hash.update(&original);
            let before = Instant::now();
            let cpu = cpu_ns();
            let frame = source_frame::encode(rows, width, &original, limits)?;
            encode_cpu += cpu_ns() - cpu;
            encode_wall += before.elapsed().as_nanos();
            let before = Instant::now();
            let cpu = cpu_ns();
            let decoded = source_frame::decode(&frame, rows, width, limits)?;
            decode_cpu += cpu_ns() - cpu;
            decode_wall += before.elapsed().as_nanos();
            if decoded != original {
                return Err(fail("lossless mismatch"));
            }
            authority
                .verify_payload(first / 32, (first + rows - 1) / 32, &decoded)
                .map_err(|e| fail(&format!("decoded unit {e:?}")))?;
            decoded_hash.update(&decoded);
            fallback += usize::from(frame[4] == 0);
            payload_bytes = payload_bytes
                .checked_add((frame.len() - HEADER_BYTES) as u64)
                .ok_or_else(|| fail("payload overflow"))?;
            sizes.push(frame.len());
        }
    }
    let mut extra = [0u8; 1];
    if records.file.read(&mut extra)? != 0 {
        return Err(fail("records trailing data"));
    }
    records.check()?;
    let records_hash = format!("{:x}", full_hash.finalize());
    if records_hash != c.records.sha256 {
        return Err(fail("full original records hash"));
    }
    // Reauthenticate metadata paths after streaming before publishing a result.
    for a in [&c.generation, &c.plane, &c.digests] {
        read_artifact(a, c.max_metadata_bytes)?;
    }
    let mut report = json!({"schema":"source-frame-report-v1","mode":if measure {"measure"}else{"admit-only"},"config":c,"original_bytes":c.records.bytes,"original_sha256":records_hash,"authenticated_original_units":c.rows.div_ceil(32),"frame_rows":FRAME_ROWS,"frame_count":nframes,"query_or_truth_opened":false,"limits":{"frame_decoded_bytes":c.max_frame_bytes,"zstd_max_window_bytes":1<<20,"metadata_admission_bytes":metadata_total},"resources":resource_snapshot(),"limitations":"Whole-plane lossless compression only. No query-range transfer, GET, billed-byte, ANN recall, latency, QPS or vendor claim. Buffers/windows are bounded; zstd context/allocator/process overhead is represented by observed process/cgroup RSS, not an analytic total workspace cap."});
    if measure {
        sizes.sort_unstable();
        let header_bytes = (HEADER_BYTES as u64)
            .checked_mul(nframes as u64)
            .ok_or_else(|| fail("header overflow"))?;
        let directory_bytes = 64u64
            .checked_add(
                24u64
                    .checked_mul(nframes as u64)
                    .ok_or_else(|| fail("directory overflow"))?,
            )
            .ok_or_else(|| fail("directory overflow"))?;
        let framed_bytes = payload_bytes
            .checked_add(header_bytes)
            .ok_or_else(|| fail("framed overflow"))?;
        report["measurement"] = json!({"codec_input_sha256":format!("{:x}",input_hash.finalize()),"decoded_sha256":format!("{:x}",decoded_hash.finalize()),"payload_bytes":payload_bytes,"actual_frame_header_bytes":header_bytes,"candidate_framed_bytes":framed_bytes,"counterfactual_directory_bytes":directory_bytes,"candidate_framed_plus_counterfactual_directory_bytes":framed_bytes.checked_add(directory_bytes).ok_or_else(||fail("total overflow"))?,"raw_fallback_frames":fallback,"raw_fallback_header_expansion_bytes":fallback*HEADER_BYTES,"frame_size_bytes_including_header":{"min":sizes[0],"p50":sizes[(sizes.len()-1)/2],"p95":sizes[(sizes.len()-1)*95/100],"max":sizes[sizes.len()-1]},"encode_wall_ns":encode_wall,"decode_wall_ns":decode_wall,"encode_process_cpu_ns":encode_cpu,"decode_process_cpu_ns":decode_cpu,"zstd_level":1,"dictionary":false,"directory_is_adopted":false});
    }
    report["total_wall_ns"] = json!(start.elapsed().as_nanos());
    report["total_process_cpu_ns"] = json!(cpu_ns() - cpu_start);
    Ok(report)
}
fn publish(path: &Path, report: &Value) -> Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, report)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    temporary.persist_noclobber(path).map_err(|e| e.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
fn artifact(path: PathBuf, bytes: &[u8]) -> Result<Artifact> {
    fs::write(&path, bytes)?;
    Ok(Artifact {
        path,
        bytes: bytes.len() as u64,
        sha256: hash(bytes),
    })
}
fn fixture(dir: &Path) -> Result<Config> {
    let rows = 289usize;
    let dimensions = 3;
    let width = 9;
    let records: Vec<u8> = (0..rows * width).map(|i| (i / width) as u8).collect();
    let mut digests = Vec::new();
    for chunk in records.chunks(32 * width) {
        digests.extend_from_slice(&Sha256::digest(chunk));
    }
    let records = artifact(dir.join("records.bin"), &records)?;
    let digests = artifact(dir.join("page_digests.bin"), &digests)?;
    let plane = json!({"schema":"borsuk-two-bit-plane-v3","rows":rows,"dimensions":dimensions,"seed":20260923,"record_bytes":width,"source_sha256":"0".repeat(64),"sq8_sha256":"1".repeat(64),"source_order_sha256":"2".repeat(64),"mean_sha256":"3".repeat(64),"records_sha256":records.sha256,"page_rows":32,"page_digest_sha256":digests.sha256,"query_or_truth_used":false});
    let plane = artifact(
        dir.join("plane-manifest.json"),
        &serde_json::to_vec(&plane)?,
    )?;
    let generation = json!({"schema":"borsuk-two-bit-generation-v11","generation":1,"source_layout":{"layout":"flat_records"},"plane_manifest_sha256":plane.sha256,"discovery":{"input_root_sha256":plane.sha256,"records_sha256":records.sha256}});
    let generation = artifact(
        dir.join("generation-manifest.json"),
        &serde_json::to_vec(&generation)?,
    )?;
    Ok(Config {
        schema: "source-frame-config-v1".into(),
        generation,
        plane,
        digests,
        records,
        rows,
        dimensions,
        max_source_bytes: 10000,
        max_metadata_bytes: 65536,
        max_frame_bytes: 65536,
    })
}
fn canary() -> Result<Value> {
    let dir = tempfile::tempdir()?;
    let config = fixture(dir.path())?;
    execute(&config, false)?;
    execute(&config, true)?;
    let mut bad = config.clone();
    bad.records.sha256 = "f".repeat(64);
    if execute(&bad, true).is_ok() {
        return Err(fail("wrong hash accepted"));
    }
    let mut bad = config.clone();
    bad.records.path = dir.path().join("missing");
    if execute(&bad, true).is_ok() {
        return Err(fail("missing source accepted"));
    }
    let report = dir.path().join("report.json");
    publish(&report, &json!({"canary":true}))?;
    if publish(&report, &json!({"canary":false})).is_ok() {
        return Err(fail("report overwritten"));
    }
    let mut unknown = serde_json::to_value(&config)?;
    unknown["unknown"] = json!(1);
    if serde_json::from_value::<Config>(unknown).is_ok() {
        return Err(fail("unknown config accepted"));
    }
    let mut corrupt = fs::read(&config.records.path)?;
    corrupt[0] ^= 1;
    fs::write(&config.records.path, corrupt)?;
    if execute(&config, true).is_ok() {
        return Err(fail("corrupt authority accepted"));
    }
    dir.close()?;
    Ok(
        json!({"schema":"source-frame-report-v1","mode":"synthetic-canary","passed":true,"real_operations":["disposable SOURCE authority files","codec","streaming authentication","create-only publication","cleanup"],"mocked_operations":["generation fixture"],"scientific_performance_evidence":false,"query_or_truth_opened":false}),
    )
}
fn args(args: impl Iterator<Item = String>) -> Result<(String, Option<PathBuf>, PathBuf)> {
    let mut values = BTreeMap::new();
    let mut args = args;
    while let Some(key) = args.next() {
        if !["--mode", "--config", "--report"].contains(&key.as_str()) || values.contains_key(&key)
        {
            return Err(fail("unknown/duplicate argument"));
        }
        values.insert(
            key,
            args.next().ok_or_else(|| fail("missing argument value"))?,
        );
    }
    let mode = values
        .remove("--mode")
        .ok_or_else(|| fail("missing mode"))?;
    let config = values.remove("--config").map(PathBuf::from);
    let report = PathBuf::from(
        values
            .remove("--report")
            .ok_or_else(|| fail("missing report"))?,
    );
    if !["admit-only", "synthetic-canary", "measure"].contains(&mode.as_str())
        || (mode == "synthetic-canary") == config.is_some()
    {
        return Err(fail("mode/config contract"));
    }
    if report.exists() {
        return Err(fail("report already exists"));
    }
    Ok((mode, config, report))
}
fn main() {
    let result = (|| -> Result<()> {
        let (mode, config, report) = args(std::env::args().skip(1))?;
        let value = if mode == "synthetic-canary" {
            canary()?
        } else {
            execute(&read_config(config.as_deref().unwrap())?, mode == "measure")?
        };
        publish(&report, &value)?;
        println!("REPORT_SYNCED {}", report.display());
        Ok(())
    })();
    if let Err(e) = result {
        eprintln!("SOURCE_FRAME_REFUSED {e}");
        std::process::exit(1);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_frame_cli_canary() {
        canary().unwrap();
    }
    #[test]
    fn source_frame_cli_strictness() {
        for args_ in [
            vec!["--mode", "measure", "--report", "new"],
            vec![
                "--mode",
                "synthetic-canary",
                "--config",
                "x",
                "--report",
                "new",
            ],
            vec!["--mode", "other", "--report", "new"],
            vec![
                "--mode",
                "synthetic-canary",
                "--report",
                "new",
                "--truth",
                "x",
            ],
            vec![
                "--mode",
                "synthetic-canary",
                "--mode",
                "measure",
                "--report",
                "new",
            ],
        ] {
            assert!(args(args_.into_iter().map(str::to_owned)).is_err());
        }
    }
    #[test]
    fn source_frame_cli_authority_and_caps() {
        let dir = tempfile::tempdir().unwrap();
        let c = fixture(dir.path()).unwrap();
        let mut bad = c.clone();
        bad.max_frame_bytes = 1;
        assert!(execute(&bad, true).is_err());
        let mut bad = c.clone();
        bad.max_metadata_bytes = 1;
        assert!(execute(&bad, true).is_err());
        let mut bad = c.clone();
        bad.rows += 1;
        assert!(execute(&bad, true).is_err());
        let mut bytes = fs::read(&c.digests.path).unwrap();
        bytes[0] ^= 1;
        fs::write(&c.digests.path, bytes).unwrap();
        assert!(execute(&c, true).is_err());
    }
}
