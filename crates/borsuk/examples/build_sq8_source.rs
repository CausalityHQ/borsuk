//! Create SQ8 with the public Rust API from sealed source/order snapshots.
use borsuk::sq8_source::{build_sq8_source, normalize_source};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::File,
    io::{BufReader, Read, Write},
    path::Path,
};

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).is_some_and(|arg| arg == "--derive") {
        if args.len() != 5 {
            return Err("usage: build_sq8_source --derive CONFIG CONFIG_SHA NEW_OUTPUT_DIR".into());
        }
        return derive::run(Path::new(&args[2]), &args[3], Path::new(&args[4]));
    }
    if args.len() != 8 {
        return Err("usage: build_sq8_source SOURCE SOURCE_SHA DIMENSIONS ORDER_LE_U64 ORDER_SHA MAX_PAYLOAD_BYTES NEW_OUTPUT (or normalize SOURCE SOURCE_SHA ROWS DIMENSIONS MAX_PAYLOAD_BYTES NEW_OUTPUT; fit/hier-fit have the same arguments as normalize)".into());
    }
    if args[1] == "fit" || args[1] == "hier-fit" {
        let output = Path::new(&args[7]);
        if output.exists() {
            return Err("output already exists".into());
        }
        let (order, extents) = if args[1] == "hier-fit" {
            let layout = borsuk::source_order::fit_hierarchical_source_layout(
                Path::new(&args[2]),
                &args[3],
                args[4].parse()?,
                args[5].parse()?,
                args[6].parse()?,
            )?;
            (layout.order, Some(layout.extents))
        } else {
            (
                borsuk::source_order::fit_source_order(
                    Path::new(&args[2]),
                    &args[3],
                    args[4].parse()?,
                    args[5].parse()?,
                    args[6].parse()?,
                )?,
                None,
            )
        };
        let parent = output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut pending = tempfile::NamedTempFile::new_in(parent)?;
        let mut digest = Sha256::new();
        for ordinal in &order {
            let bytes = ordinal.to_le_bytes();
            pending.write_all(&bytes)?;
            digest.update(bytes);
        }
        pending.as_file().sync_all()?;
        pending.persist_noclobber(output).map_err(|e| e.error)?;
        File::open(parent)?.sync_all()?;
        let mut receipt = serde_json::json!({"order_sha256":format!("{:x}",digest.finalize()),
            "rows":order.len(), "recipe":"borsuk-semantic-order-chacha8-f32-v1", "query_or_truth_used":false});
        if let Some(extents) = extents {
            receipt["recipe"] = "borsuk-hierarchical-extents-chacha8-v3".into();
            receipt["source_cell_order"] =
                "nearest-unvisited-layer0-entry-ordinal-fallback-v1".into();
            receipt["source_cell_target_rows"] = 256.into();
            receipt["sampling_cell_target_rows"] = 1024.into();
            receipt["samples_per_sampling_cell"] = 64.into();
            receipt["extent_row_cap"] = 1024.into();
            receipt["extents"] =
                serde_json::json!(extents.iter().map(|r| [r.start, r.end]).collect::<Vec<_>>());
        }
        println!("{receipt}");
        return Ok(());
    }
    if args[1] == "normalize" {
        let sha = normalize_source(
            Path::new(&args[2]),
            &args[3],
            args[4].parse()?,
            args[5].parse()?,
            Path::new(&args[7]),
            args[6].parse()?,
        )?;
        println!(
            "{}",
            serde_json::json!({"normalized_sha256":sha,"query_or_truth_used":false})
        );
        return Ok(());
    }
    let cap: usize = args[6].parse()?;
    let file = File::open(&args[4])?;
    let length = usize::try_from(file.metadata()?.len())?;
    // Order is caller-owned payload charged again by the library's admission.
    if length == 0 || length % 8 != 0 || length.checked_add(65536).is_none_or(|n| n > cap) {
        return Err("order geometry or payload cap".into());
    }
    let mut input = BufReader::with_capacity(65536, file);
    let mut digest = Sha256::new();
    let mut order = Vec::with_capacity(length / 8);
    for _ in 0..length / 8 {
        let mut bytes = [0_u8; 8];
        input.read_exact(&mut bytes)?;
        digest.update(bytes);
        order.push(u64::from_le_bytes(bytes));
    }
    if input.read(&mut [0])? != 0 || format!("{:x}", digest.finalize()) != args[5] {
        return Err("order identity".into());
    }
    drop(input);
    let receipt = build_sq8_source(
        Path::new(&args[1]),
        &args[2],
        args[3].parse()?,
        &order,
        Path::new(&args[7]),
        cap,
    )?;
    println!(
        "{}",
        serde_json::json!({"low":receipt.low,"step":receipt.step,
        "sq8_sha256":receipt.sha256,"rows":order.len(),"query_or_truth_used":false})
    );
    Ok(())
}

// This wrapper admits payload coexistence, not RSS. Native qualification and the
// binding from executable bytes to reviewed source remain external root gates.
pub(crate) mod derive {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::{
        fs::{self, Metadata, OpenOptions},
        io::{Seek, SeekFrom},
        os::unix::fs::{MetadataExt, OpenOptionsExt},
        path::PathBuf,
    };

    type Result<T> = std::result::Result<T, Box<dyn Error>>;
    const CONFIG_CAP: usize = 65_536;
    const RECEIPT_CAP: usize = 65_536;
    const CONTROL_RESERVE: usize = 4 * 1024 * 1024;
    const MEMORY_CEILING: usize = 8 * 1024 * 1024 * 1024;
    const CONFIG_SCHEMA: &str = "borsuk-native-scale-derivation-config-v1";
    const RECEIPT_SCHEMA: &str = "borsuk-native-scale-derivation-receipt-v2";

    #[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct Authority {
        source_commit: String,
        executable_sha256: String,
        producer_source_sha256: String,
        sq8_source_sha256: String,
        source_order_source_sha256: String,
    }
    #[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct Seal {
        bytes: usize,
        sha256: String,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Source {
        path: PathBuf,
        bytes: usize,
        sha256: String,
    }
    #[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct Interval {
        start: usize,
        end: usize,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Resources {
        max_payload_bytes: usize,
        max_aggregate_scratch_bytes: usize,
        caller_payload_bytes: usize,
        caller_scratch_bytes: usize,
        temporary_reserve_bytes: usize,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Config {
        schema: String,
        original_corpus: Source,
        rows: usize,
        dimensions: usize,
        corpus_intervals: Vec<Interval>,
        producer_authority: Authority,
        resources: Resources,
    }
    #[derive(Serialize)]
    struct Outputs {
        normalized: Seal,
        source_order: Seal,
        sq8: Seal,
    }
    #[derive(Serialize)]
    struct Admission {
        wrapper_payload_bytes: usize,
        normalization_api_payload_bytes: usize,
        flat_fit_api_payload_upper_bound_bytes: usize,
        sq8_api_payload_bytes: usize,
        peak_payload_upper_bound_bytes: usize,
        aggregate_scratch_upper_bound_bytes: usize,
    }
    #[derive(Serialize)]
    struct Receipt<'a> {
        schema: &'static str,
        producer_config_sha256: &'a str,
        status: &'static str,
        recipe: &'static str,
        query_or_truth_used: bool,
        original_corpus: Seal,
        rows: usize,
        dimensions: usize,
        corpus_intervals: &'a [Interval],
        outputs: Outputs,
        low_f32_bits: Vec<u32>,
        step_f32_bits: Vec<u32>,
        producer_authority: &'a Authority,
        source_identity_qualification: &'static str,
        admission: &'a Admission,
    }

    fn require(ok: bool, message: &'static str) -> Result<()> {
        if ok { Ok(()) } else { Err(message.into()) }
    }
    fn hex(value: &str, length: usize) -> bool {
        value.len() == length
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
    fn sha(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
    fn add(a: usize, b: usize) -> Result<usize> {
        a.checked_add(b)
            .ok_or_else(|| "derivation size overflow".into())
    }
    fn mul(a: usize, b: usize) -> Result<usize> {
        a.checked_mul(b)
            .ok_or_else(|| "derivation size overflow".into())
    }
    fn sum(values: &[usize]) -> Result<usize> {
        values.iter().try_fold(0, |a, &b| add(a, b))
    }

    #[derive(PartialEq, Eq)]
    struct Stamp {
        dev: u64,
        ino: u64,
        len: u64,
        mtime: (i64, i64),
        ctime: (i64, i64),
    }
    fn stamp(meta: Metadata) -> Result<Stamp> {
        require(meta.nlink() == 1, "single-link regular source/output")?;
        regular_stamp(meta)
    }
    fn regular_stamp(meta: Metadata) -> Result<Stamp> {
        require(
            meta.is_file(),
            "regular executable/source/output descriptor",
        )?;
        Ok(Stamp {
            dev: meta.dev(),
            ino: meta.ino(),
            len: meta.len(),
            mtime: (meta.mtime(), meta.mtime_nsec()),
            ctime: (meta.ctime(), meta.ctime_nsec()),
        })
    }
    struct Snapshot {
        path: PathBuf,
        file: File,
        before: Stamp,
    }
    impl Snapshot {
        fn open(path: &Path) -> Result<Self> {
            require(
                path.is_absolute() && fs::canonicalize(path)? == path,
                "canonical absolute source/output path",
            )?;
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(
                    (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
                )
                .open(path)?;
            let before = stamp(file.metadata()?)?;
            let snapshot = Self {
                path: path.to_owned(),
                file,
                before,
            };
            snapshot.check()?;
            Ok(snapshot)
        }
        fn check(&self) -> Result<()> {
            require(
                stamp(self.file.metadata()?)? == self.before
                    && stamp(fs::symlink_metadata(&self.path)?)? == self.before,
                "immutable source/output stamp",
            )
        }
        fn seal(&mut self) -> Result<Seal> {
            self.check()?;
            self.file.seek(SeekFrom::Start(0))?;
            let mut digest = Sha256::new();
            let mut buffer = [0_u8; 65_536];
            let mut bytes = 0;
            loop {
                let n = self.file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                bytes = add(bytes, n)?;
                digest.update(&buffer[..n]);
            }
            self.check()?;
            require(
                u64::try_from(bytes)? == self.before.len,
                "snapshot exact EOF",
            )?;
            Ok(Seal {
                bytes,
                sha256: format!("{:x}", digest.finalize()),
            })
        }
    }
    // /proc/self/exe opens the executable mapped into this process, even if its
    // original pathname has been replaced. It is deliberately not a config path.
    fn executable_sha() -> Result<String> {
        hash_executable_file(File::open("/proc/self/exe")?)
    }
    fn hash_executable_file(mut file: File) -> Result<String> {
        // Cargo may legitimately hardlink the uplifted example binary. Only
        // source/output snapshots have the separate single-link admission.
        let before = regular_stamp(file.metadata()?)?;
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 65_536];
        let mut bytes = 0_u64;
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            bytes = bytes
                .checked_add(u64::try_from(n)?)
                .ok_or("executable size overflow")?;
            digest.update(&buffer[..n]);
        }
        require(
            before == regular_stamp(file.metadata()?)? && bytes == before.len,
            "immutable running executable",
        )?;
        Ok(format!("{:x}", digest.finalize()))
    }
    fn compiled_authority(source_commit: String) -> Result<Authority> {
        Ok(Authority {
            source_commit,
            executable_sha256: executable_sha()?,
            producer_source_sha256: sha(include_bytes!("build_sq8_source.rs")),
            sq8_source_sha256: sha(include_bytes!("../src/sq8_source.rs")),
            source_order_source_sha256: sha(include_bytes!("../src/source_order.rs")),
        })
    }
    fn validate(c: &Config) -> Result<Admission> {
        require(
            c.schema == CONFIG_SCHEMA && usize::BITS == 64,
            "strict derivation config/64-bit target",
        )?;
        require(
            c.rows > 0 && c.rows <= 1_000_000 && c.dimensions > 0 && c.dimensions <= 1024,
            "derivation geometry",
        )?;
        require(
            hex(&c.original_corpus.sha256, 64) && hex(&c.producer_authority.source_commit, 40),
            "source/authority SHA",
        )?;
        for pin in [
            &c.producer_authority.executable_sha256,
            &c.producer_authority.producer_source_sha256,
            &c.producer_authority.sq8_source_sha256,
            &c.producer_authority.source_order_source_sha256,
        ] {
            require(hex(pin, 64), "producer authority SHA")?;
        }
        require(
            !c.corpus_intervals.is_empty() && c.corpus_intervals.len() <= 2,
            "source selection intervals",
        )?;
        let mut rows = 0;
        let mut end = 0;
        for interval in &c.corpus_intervals {
            require(
                interval.start >= end && interval.start < interval.end,
                "ordered source selection",
            )?;
            rows = add(rows, interval.end - interval.start)?;
            end = interval.end;
        }
        let source = mul(mul(c.rows, c.dimensions)?, 4)?;
        require(
            rows == c.rows && c.original_corpus.bytes == source,
            "source selection/byte geometry",
        )?;
        let r = &c.resources;
        require(
            r.max_payload_bytes > 0 && r.max_payload_bytes <= MEMORY_CEILING,
            "derivation payload ceiling",
        )?;
        let wrapper = sum(&[
            CONTROL_RESERVE,
            mul(32, c.dimensions)?,
            r.caller_payload_bytes,
            include_bytes!("build_sq8_source.rs").len(),
            include_bytes!("../src/sq8_source.rs").len(),
            include_bytes!("../src/source_order.rs").len(),
        ])?;
        let normalization = add(mul(16, c.dimensions)?, 135_168)?;
        let cells = add(c.rows, 255)? / 256;
        let samples = c.rows.min(mul(cells, 64)?);
        let batch = c.rows.min(256);
        // Current private RankedRow is 24 bytes on the required 64-bit target.
        // Charge 32 plus its coexisting 8-byte returned order conservatively;
        // all other terms match ordinary fit_source_order's pinned admission.
        let fitting = sum(&[
            327_680,
            mul(c.rows, 40)?,
            mul(samples, 8)?,
            mul(mul(samples, c.dimensions)?, 4)?,
            mul(mul(cells, c.dimensions)?, 16)?,
            mul(mul(batch, c.dimensions)?, 12)?,
            mul(mul(cells, batch)?, 8)?,
            mul(cells, 128)?,
            mul(c.dimensions, 16)?,
        ])?;
        let order = mul(c.rows, 8)?;
        let sq8 = sum(&[order, add(c.rows, 7)? / 8, mul(c.dimensions, 32)?, 131_072])?;
        // Charge caller-owned order again outside the SQ8 API's admission.
        let peak = add(wrapper, normalization.max(fitting).max(add(order, sq8)?))?;
        require(
            peak <= r.max_payload_bytes,
            "cumulative derivation payload cap",
        )?;
        let sq8_bytes = mul(c.rows, add(c.dimensions, 12)?)?;
        // Includes original input, completed and possible pending normalized/order
        // copies, partial SQ8, two bounded receipt copies and failure reserves.
        let scratch = sum(&[
            source,
            mul(source, 2)?,
            mul(order, 2)?,
            sq8_bytes,
            RECEIPT_CAP * 2,
            r.caller_scratch_bytes,
            r.temporary_reserve_bytes,
        ])?;
        require(
            scratch <= r.max_aggregate_scratch_bytes,
            "aggregate derivation scratch cap",
        )?;
        Ok(Admission {
            wrapper_payload_bytes: wrapper,
            normalization_api_payload_bytes: normalization,
            flat_fit_api_payload_upper_bound_bytes: fitting,
            sq8_api_payload_bytes: sq8,
            peak_payload_upper_bound_bytes: peak,
            aggregate_scratch_upper_bound_bytes: scratch,
        })
    }
    struct Directory {
        path: PathBuf,
        file: File,
        dev: u64,
        ino: u64,
    }
    impl Directory {
        fn open(path: &Path) -> Result<Self> {
            require(
                path.is_absolute() && fs::canonicalize(path)? == path,
                "canonical output parent",
            )?;
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(
                    (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::DIRECTORY).bits() as i32,
                )
                .open(path)?;
            let meta = file.metadata()?;
            Ok(Self {
                path: path.to_owned(),
                file,
                dev: meta.dev(),
                ino: meta.ino(),
            })
        }
        fn check(&self) -> Result<()> {
            let meta = fs::symlink_metadata(&self.path)?;
            require(
                meta.is_dir() && meta.dev() == self.dev && meta.ino() == self.ino,
                "output directory identity",
            )
        }
        fn sync(&self) -> Result<()> {
            self.check()?;
            self.file.sync_all()?;
            self.check()
        }
    }
    fn bits(values: &[f32]) -> Result<Vec<u32>> {
        let mut output = Vec::new();
        output.try_reserve_exact(values.len())?;
        output.extend(values.iter().map(|v| v.to_bits()));
        Ok(output)
    }
    #[derive(Debug)]
    struct PublicationFailure {
        original: Box<dyn Error>,
        unlink: String,
        directory_sync: String,
        parent_sync: String,
    }
    impl std::fmt::Display for PublicationFailure {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "{}; pinned receipt revocation unlink={}; directory_sync={}; parent_sync={}; revocation durability uncertain; COMPLETE may survive failure; acceptance requires ORIGINAL process exit 0",
                self.original, self.unlink, self.directory_sync, self.parent_sync
            )
        }
    }
    impl Error for PublicationFailure {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(self.original.as_ref())
        }
    }
    fn outcome(result: Result<()>) -> String {
        match result {
            Ok(()) => "ok".to_owned(),
            Err(error) => format!("error: {error}"),
        }
    }
    fn revoke_pinned(dir: &Directory, installed: &File) -> Result<()> {
        let expected = regular_stamp(installed.metadata()?)?;
        let entry = rustix::fs::statat(
            &dir.file,
            "derivation.json",
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?;
        require(
            entry.st_dev == expected.dev && entry.st_ino == expected.ino,
            "refuse replaced receipt entry in pinned directory",
        )?;
        // Relative to the retained original directory fd: a renamed/replaced
        // namespace never redirects cleanup into an unrelated replacement dir.
        rustix::fs::unlinkat(&dir.file, "derivation.json", rustix::fs::AtFlags::empty())?;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn fixture_config(
        source: &Path,
        rows: usize,
        dimensions: usize,
    ) -> Result<serde_json::Value> {
        let mut original = Snapshot::open(source)?;
        let seal = original.seal()?;
        // The test's source_commit is a declared fixture authority, not a build
        // provenance assertion. Executable and embedded partial hashes are real.
        Ok(
            serde_json::json!({"schema":CONFIG_SCHEMA,"original_corpus":{"path":source,"bytes":seal.bytes,"sha256":seal.sha256},
            "rows":rows,"dimensions":dimensions,"corpus_intervals":[{"start":0,"end":rows}],
            "producer_authority":compiled_authority("1".repeat(40))?,
            "resources":{"max_payload_bytes":8*1024*1024,"max_aggregate_scratch_bytes":2*1024*1024,
                "caller_payload_bytes":0,"caller_scratch_bytes":0,"temporary_reserve_bytes":4096}}),
        )
    }
    pub(crate) fn run(config: &Path, pin: &str, output: &Path) -> Result<()> {
        run_with(config, pin, output, |_, _| Ok(()))
    }
    fn run_with(
        config: &Path,
        pin: &str,
        output: &Path,
        mut boundary: impl FnMut(&str, &Path) -> Result<()>,
    ) -> Result<()> {
        require(hex(pin, 64), "config SHA")?;
        let mut config_snapshot = Snapshot::open(config)?;
        require(
            config_snapshot.before.len > 0 && config_snapshot.before.len <= CONFIG_CAP as u64,
            "config admission",
        )?;
        require(config_snapshot.seal()?.sha256 == pin, "config SHA mismatch")?;
        config_snapshot.file.seek(SeekFrom::Start(0))?;
        let mut body = Vec::new();
        body.try_reserve_exact(usize::try_from(config_snapshot.before.len)?)?;
        body.resize(usize::try_from(config_snapshot.before.len)?, 0);
        config_snapshot.file.read_exact(&mut body)?;
        require(
            config_snapshot.file.read(&mut [0])? == 0,
            "config exact EOF",
        )?;
        config_snapshot.check()?;
        require(sha(&body) == pin, "config SHA mismatch")?;
        let c: Config = serde_json::from_slice(&body)?;
        let admission = validate(&c)?;
        require(
            c.producer_authority == compiled_authority(c.producer_authority.source_commit.clone())?,
            "running producer/root-frozen authority mismatch",
        )?;
        let mut original = Snapshot::open(&c.original_corpus.path)?;
        require(
            original.before.len == u64::try_from(c.original_corpus.bytes)?,
            "original corpus byte geometry",
        )?;
        let original_seal = original.seal()?;
        require(
            original_seal.bytes == c.original_corpus.bytes
                && original_seal.sha256 == c.original_corpus.sha256,
            "original corpus seal mismatch",
        )?;
        require(
            output.is_absolute() && output.file_name().is_some(),
            "absolute new output directory",
        )?;
        let parent = Directory::open(output.parent().ok_or("output parent")?)?;
        require(
            !output.starts_with(&c.original_corpus.path) && !output.starts_with(config),
            "source/output alias",
        )?;
        parent.check()?;
        fs::create_dir(output)?; // Exclusive: occupied paths (including dangling links) refuse.
        let dir = Directory::open(output)?;
        parent.sync()?;
        let normalized_path = output.join("normalized.f32");
        let order_path = output.join("order.u64");
        let sq8_path = output.join("sq8.bin");
        let receipt_path = output.join("derivation.json");
        let budget = c.resources.max_payload_bytes - admission.wrapper_payload_bytes;
        let normalized_sha = normalize_source(
            &original.path,
            &original_seal.sha256,
            c.rows,
            c.dimensions,
            &normalized_path,
            budget,
        )?;
        boundary("normalized", output)?;
        original.check()?;
        config_snapshot.check()?;
        dir.check()?;
        let mut normalized = Snapshot::open(&normalized_path)?;
        let normalized_seal = normalized.seal()?;
        require(
            normalized_seal.bytes == original_seal.bytes
                && normalized_seal.sha256 == normalized_sha,
            "normalized output seal mismatch",
        )?;
        let order = borsuk::source_order::fit_source_order(
            &normalized.path,
            &normalized_seal.sha256,
            c.rows,
            c.dimensions,
            budget,
        )?;
        require(order.len() == c.rows, "fitted order geometry")?;
        normalized.check()?;
        dir.check()?;
        let mut pending = tempfile::NamedTempFile::new_in(output)?;
        let mut digest = Sha256::new();
        for ordinal in &order {
            let bytes = ordinal.to_le_bytes();
            pending.write_all(&bytes)?;
            digest.update(bytes);
        }
        pending.as_file().sync_all()?;
        pending
            .persist_noclobber(&order_path)
            .map_err(|e| e.error)?;
        dir.sync()?;
        let mut ordered = Snapshot::open(&order_path)?;
        let order_seal = ordered.seal()?;
        require(
            order_seal.bytes == mul(c.rows, 8)?
                && order_seal.sha256 == format!("{:x}", digest.finalize()),
            "source order independent seal mismatch",
        )?;
        boundary("ordered", output)?;
        original.check()?;
        config_snapshot.check()?;
        normalized.check()?;
        ordered.check()?;
        dir.check()?;
        let sq8 = build_sq8_source(
            &normalized.path,
            &normalized_seal.sha256,
            c.dimensions,
            &order,
            &sq8_path,
            budget - mul(c.rows, 8)?,
        )?;
        boundary("sq8", output)?;
        let mut quantized = Snapshot::open(&sq8_path)?;
        let sq8_seal = quantized.seal()?;
        require(
            sq8_seal.bytes == mul(c.rows, add(c.dimensions, 12)?)? && sq8_seal.sha256 == sq8.sha256,
            "SQ8 independent seal mismatch",
        )?;
        let receipt = Receipt {
            schema: RECEIPT_SCHEMA,
            producer_config_sha256: pin,
            status: "COMPLETE",
            recipe: "normalize_then_flat_fit_then_sq8",
            query_or_truth_used: false,
            original_corpus: original_seal.clone(),
            rows: c.rows,
            dimensions: c.dimensions,
            corpus_intervals: &c.corpus_intervals,
            outputs: Outputs {
                normalized: normalized_seal.clone(),
                source_order: order_seal.clone(),
                sq8: sq8_seal.clone(),
            },
            low_f32_bits: bits(&sq8.low)?,
            step_f32_bits: bits(&sq8.step)?,
            producer_authority: &c.producer_authority,
            source_identity_qualification: "external_frozen_prerequisite_not_self_certified",
            admission: &admission,
        };
        let mut encoded = Vec::new();
        encoded.try_reserve_exact(RECEIPT_CAP)?;
        // The writer refuses growth beyond the charged receipt cap.
        struct Limited<'a>(&'a mut Vec<u8>);
        impl Write for Limited<'_> {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                if b.len() > RECEIPT_CAP.saturating_sub(self.0.len()) {
                    return Err(std::io::Error::other("receipt cap"));
                }
                self.0.extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        serde_json::to_writer(Limited(&mut encoded), &receipt)?;
        boundary("before_publish", output)?;
        // Full pre/post content authentication and stamp checks, not declarations.
        require(
            original.seal()? == original_seal
                && normalized.seal()? == normalized_seal
                && ordered.seal()? == order_seal
                && quantized.seal()? == sq8_seal,
            "derivation final content seals",
        )?;
        config_snapshot.check()?;
        for snapshot in [&original, &normalized, &ordered, &quantized] {
            snapshot.check()?;
        }
        for snapshot in [&normalized, &ordered, &quantized] {
            snapshot.file.sync_all()?;
        }
        dir.sync()?;
        parent.sync()?;
        let mut pending = tempfile::NamedTempFile::new_in(output)?;
        pending.write_all(&encoded)?;
        pending.as_file().sync_all()?;
        // Receipt installation is not the success boundary. Killing this process
        // or failed/uncertain revocation may leave COMPLETE bytes on disk. Root
        // must admit these seals only with the ORIGINAL derivation process exit 0.
        let installed = pending
            .persist_noclobber(&receipt_path)
            .map_err(|e| e.error)?;
        let publication = installed
            .metadata()
            .map_err(Into::into)
            .and_then(stamp)
            .map(|_| ())
            .and_then(|()| boundary("published", output))
            .and_then(|()| boundary("publish_directory_sync", output))
            .and_then(|()| dir.sync())
            .and_then(|()| boundary("publish_parent_sync", output))
            .and_then(|()| parent.sync());
        if let Err(original) = publication {
            // Attempt every cleanup step, retaining the original failure even
            // if unlink or either revocation synchronization also fails. A later
            // successful fsync is not proof of durability after the first error.
            let unlink = outcome(
                boundary("revoke_unlink", output).and_then(|()| revoke_pinned(&dir, &installed)),
            );
            let directory_sync = outcome(
                boundary("revoke_directory_sync", output)
                    .and_then(|()| dir.file.sync_all().map_err(Into::into)),
            );
            let parent_sync = outcome(
                boundary("revoke_parent_sync", output)
                    .and_then(|()| parent.file.sync_all().map_err(Into::into)),
            );
            return Err(Box::new(PublicationFailure {
                original,
                unlink,
                directory_sync,
                parent_sync,
            }));
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use serde_json::{Value, json};
        fn fixture(dir: &Path) -> (PathBuf, String, Value) {
            let source = dir.join("original.f32");
            let bytes: Vec<u8> = [[2.0_f32, 0.0], [0.0, 3.0], [-4.0, 0.0], [3.0, 4.0]]
                .into_iter()
                .flatten()
                .flat_map(f32::to_le_bytes)
                .collect();
            fs::write(&source, &bytes).unwrap();
            let config = json!({"schema":CONFIG_SCHEMA,"original_corpus":{"path":source,"bytes":bytes.len(),"sha256":sha(&bytes)},
                "rows":4,"dimensions":2,"corpus_intervals":[{"start":0,"end":4}],
                "producer_authority":compiled_authority("1".repeat(40)).unwrap(),
                "resources":{"max_payload_bytes":8*1024*1024,"max_aggregate_scratch_bytes":1024*1024,
                    "caller_payload_bytes":0,"caller_scratch_bytes":0,"temporary_reserve_bytes":4096}});
            let (path, pin) = write(dir, &config);
            (path, pin, config)
        }
        fn write(dir: &Path, value: &Value) -> (PathBuf, String) {
            let body = serde_json::to_vec(value).unwrap();
            let path = dir.join("derive-config.json");
            fs::write(&path, &body).unwrap();
            (path, sha(&body))
        }
        #[test]
        fn executable_hash_allows_hardlinks_without_broadening_source_admission() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("executable");
            let bytes = b"synthetic regular executable bytes";
            fs::write(&path, bytes).unwrap();
            fs::hard_link(&path, dir.path().join("uplifted")).unwrap();
            assert_eq!(fs::metadata(&path).unwrap().nlink(), 2);
            assert_eq!(
                hash_executable_file(File::open(&path).unwrap()).unwrap(),
                sha(bytes)
            );
            assert_eq!(
                Snapshot::open(&path).err().unwrap().to_string(),
                "single-link regular source/output"
            );
        }
        #[test]
        fn post_install_failures_retain_original_error_and_all_revocation_outcomes() {
            for (sync, cleanup) in [
                ("publish_directory_sync", "none"),
                ("publish_parent_sync", "none"),
                ("publish_directory_sync", "revoke_unlink"),
                ("publish_parent_sync", "revoke_directory_sync"),
                ("publish_directory_sync", "revoke_parent_sync"),
            ] {
                let dir = tempfile::tempdir().unwrap();
                let (path, pin, _) = fixture(dir.path());
                let output = dir.path().join("derived");
                let error = run_with(&path, &pin, &output, |phase, out| {
                    if phase == "published" {
                        assert!(out.join("derivation.json").is_file());
                    }
                    if phase == sync {
                        return Err("original publication sync fault".into());
                    }
                    if phase == cleanup {
                        return Err("secondary cleanup fault".into());
                    }
                    Ok(())
                })
                .unwrap_err();
                let failure = error.downcast_ref::<PublicationFailure>().unwrap();
                assert_eq!(
                    failure.original.to_string(),
                    "original publication sync fault"
                );
                assert_eq!(
                    error.source().unwrap().to_string(),
                    "original publication sync fault"
                );
                let message = error.to_string();
                assert!(message.starts_with("original publication sync fault;"));
                assert!(message.contains("revocation durability uncertain"));
                assert!(message.contains("acceptance requires ORIGINAL process exit 0"));
                assert_eq!(
                    failure.unlink,
                    if cleanup == "revoke_unlink" {
                        "error: secondary cleanup fault"
                    } else {
                        "ok"
                    }
                );
                assert_eq!(
                    failure.directory_sync,
                    if cleanup == "revoke_directory_sync" {
                        "error: secondary cleanup fault"
                    } else {
                        "ok"
                    }
                );
                assert_eq!(
                    failure.parent_sync,
                    if cleanup == "revoke_parent_sync" {
                        "error: secondary cleanup fault"
                    } else {
                        "ok"
                    }
                );
                assert_eq!(
                    output.join("derivation.json").exists(),
                    cleanup == "revoke_unlink"
                );
                if cleanup == "revoke_unlink" {
                    let receipt: Value =
                        serde_json::from_slice(&fs::read(output.join("derivation.json")).unwrap())
                            .unwrap();
                    assert_eq!(receipt["status"], "COMPLETE"); // Nonzero run must never be admitted.
                }
            }
        }
        #[test]
        fn post_install_namespace_drift_never_unlinks_replacement_directory_receipt() {
            let dir = tempfile::tempdir().unwrap();
            let (path, pin, _) = fixture(dir.path());
            let output = dir.path().join("derived");
            let moved = dir.path().join("original-derived");
            let error = run_with(&path, &pin, &output, |phase, out| {
                if phase == "published" {
                    fs::rename(out, &moved)?;
                    fs::create_dir(out)?;
                    fs::write(
                        out.join("derivation.json"),
                        b"unrelated replacement receipt",
                    )?;
                }
                Ok(())
            })
            .unwrap_err();
            assert_eq!(
                error.source().unwrap().to_string(),
                "output directory identity"
            );
            assert_eq!(
                fs::read(output.join("derivation.json")).unwrap(),
                b"unrelated replacement receipt"
            );
            assert!(!moved.join("derivation.json").exists());
            assert!(error.to_string().contains("unlink=ok"));
        }
        #[test]
        fn derivation_actual_chain_matches_independent_public_api_outputs() {
            let dir = tempfile::tempdir().unwrap();
            let (path, pin, value) = fixture(dir.path());
            let output = dir.path().join("derived");
            run(&path, &pin, &output).unwrap();
            let receipt: Value =
                serde_json::from_slice(&fs::read(output.join("derivation.json")).unwrap()).unwrap();
            let original = Path::new(value["original_corpus"]["path"].as_str().unwrap());
            let normalized = dir.path().join("independent.f32");
            let normalized_sha = normalize_source(
                original,
                value["original_corpus"]["sha256"].as_str().unwrap(),
                4,
                2,
                &normalized,
                8 * 1024 * 1024,
            )
            .unwrap();
            let order = borsuk::source_order::fit_source_order(
                &normalized,
                &normalized_sha,
                4,
                2,
                8 * 1024 * 1024,
            )
            .unwrap();
            let independent = dir.path().join("independent.sq8");
            let sq8 = build_sq8_source(
                &normalized,
                &normalized_sha,
                2,
                &order,
                &independent,
                8 * 1024 * 1024,
            )
            .unwrap();
            assert_eq!(
                fs::read(output.join("normalized.f32")).unwrap(),
                fs::read(&normalized).unwrap()
            );
            assert_eq!(
                fs::read(output.join("order.u64")).unwrap(),
                order
                    .iter()
                    .flat_map(|o| o.to_le_bytes())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                fs::read(output.join("sq8.bin")).unwrap(),
                fs::read(&independent).unwrap()
            );
            assert_eq!(receipt["outputs"]["normalized"]["sha256"], normalized_sha);
            assert_eq!(receipt["outputs"]["sq8"]["sha256"], sq8.sha256);
            assert_eq!(receipt["low_f32_bits"], json!(bits(&sq8.low).unwrap()));
            assert_eq!(receipt["step_f32_bits"], json!(bits(&sq8.step).unwrap()));
            assert_eq!(
                receipt["original_corpus"],
                json!({"bytes":value["original_corpus"]["bytes"],"sha256":value["original_corpus"]["sha256"]})
            );
            assert_eq!(receipt["producer_authority"], value["producer_authority"]);
            assert!(receipt.get("cohort_receipt_sha256").is_none());
            assert_eq!(receipt["producer_config_sha256"], pin);
            assert_eq!(receipt["status"], "COMPLETE");
        }
        #[test]
        fn derivation_refuses_config_geometry_authority_caps_and_occupied_outputs() {
            for fault in [
                "sha",
                "rows",
                "dimensions",
                "source",
                "authority",
                "compiled-source",
                "payload",
                "scratch",
                "overflow",
                "queries",
                "truth",
            ] {
                let dir = tempfile::tempdir().unwrap();
                let (_, _, mut value) = fixture(dir.path());
                let expected = match fault {
                    "sha" => "config SHA mismatch",
                    "rows" => {
                        value["rows"] = json!(5);
                        "source selection/byte geometry"
                    }
                    "dimensions" => {
                        value["dimensions"] = json!(3);
                        "source selection/byte geometry"
                    }
                    "source" => {
                        value["original_corpus"]["sha256"] = json!("0".repeat(64));
                        "original corpus seal mismatch"
                    }
                    "authority" => {
                        value["producer_authority"]["executable_sha256"] = json!("0".repeat(64));
                        "running producer/root-frozen authority mismatch"
                    }
                    "compiled-source" => {
                        value["producer_authority"]["producer_source_sha256"] =
                            json!("0".repeat(64));
                        "running producer/root-frozen authority mismatch"
                    }
                    "payload" => {
                        value["resources"]["max_payload_bytes"] = json!(1);
                        "cumulative derivation payload cap"
                    }
                    "scratch" => {
                        value["resources"]["max_aggregate_scratch_bytes"] = json!(1);
                        "aggregate derivation scratch cap"
                    }
                    "overflow" => {
                        value["resources"]["caller_payload_bytes"] = json!(usize::MAX);
                        "derivation size overflow"
                    }
                    "queries" => {
                        value["queries"] = json!({"path":"forbidden"});
                        "unknown field `queries`"
                    }
                    "truth" => {
                        value["truth"] = json!({"path":"forbidden"});
                        "unknown field `truth`"
                    }
                    _ => unreachable!(),
                };
                let (path, mut pin) = write(dir.path(), &value);
                if fault == "sha" {
                    pin = "0".repeat(64);
                }
                let output = dir.path().join("derived");
                let error = run(&path, &pin, &output).unwrap_err().to_string();
                assert!(error.starts_with(expected), "{fault}: {error}");
                assert!(!output.exists());
            }
            let dir = tempfile::tempdir().unwrap();
            let (path, pin, value) = fixture(dir.path());
            let occupied = dir.path().join("occupied");
            fs::create_dir(&occupied).unwrap();
            assert!(run(&path, &pin, &occupied).is_err());
            assert!(!occupied.join("derivation.json").exists());
            // An output alias cannot overwrite either original source or config.
            let source = Path::new(value["original_corpus"]["path"].as_str().unwrap());
            assert_eq!(
                run(&path, &pin, source).unwrap_err().to_string(),
                "source/output alias"
            );
            assert_eq!(
                run(&path, &pin, &path).unwrap_err().to_string(),
                "source/output alias"
            );
        }
        #[test]
        fn derivation_strict_json_and_symlink_sources_refuse_before_output_creation() {
            let dir = tempfile::tempdir().unwrap();
            let (path, _, mut value) = fixture(dir.path());
            let body = serde_json::to_vec(&value).unwrap();
            let mut duplicate =
                b"{\"schema\":\"borsuk-native-scale-derivation-config-v1\",".to_vec();
            duplicate.extend_from_slice(&body[1..]);
            fs::write(&path, &duplicate).unwrap();
            let output = dir.path().join("duplicate");
            let error = run(&path, &sha(&duplicate), &output)
                .unwrap_err()
                .to_string();
            assert!(error.starts_with("duplicate field `schema`"), "{error}");
            assert!(!output.exists());
            let source = PathBuf::from(value["original_corpus"]["path"].as_str().unwrap());
            let alias = dir.path().join("source-alias");
            std::os::unix::fs::symlink(&source, &alias).unwrap();
            value["original_corpus"]["path"] = json!(alias);
            let (path, pin) = write(dir.path(), &value);
            let output = dir.path().join("aliased");
            assert_eq!(
                run(&path, &pin, &output).unwrap_err().to_string(),
                "canonical absolute source/output path"
            );
            assert!(!output.exists());
        }
        #[test]
        fn derivation_actual_publication_conflict_refuses_completed_receipt() {
            let dir = tempfile::tempdir().unwrap();
            let (path, pin, _) = fixture(dir.path());
            let output = dir.path().join("derived");
            let result = run_with(&path, &pin, &output, |phase, out| {
                if phase == "before_publish" {
                    fs::create_dir(out.join("derivation.json"))?;
                }
                Ok(())
            });
            assert!(result.is_err());
            assert!(output.join("sq8.bin").is_file());
            assert!(output.join("derivation.json").is_dir());
            assert!(!output.join("derivation.json").is_file());
        }
        #[test]
        fn derivation_mutations_and_partial_publication_never_leave_completed_receipt() {
            for stage in ["normalized", "ordered", "sq8", "config", "before_publish"] {
                let dir = tempfile::tempdir().unwrap();
                let (path, pin, value) = fixture(dir.path());
                let output = dir.path().join("derived");
                let result = run_with(&path, &pin, &output, |phase, out| {
                    if phase == stage || (stage == "config" && phase == "ordered") {
                        let target = match stage {
                            "normalized" => {
                                PathBuf::from(value["original_corpus"]["path"].as_str().unwrap())
                            }
                            "ordered" => out.join("normalized.f32"),
                            "sq8" => out.join("sq8.bin"),
                            "config" => path.clone(),
                            _ => return Err("fixture publication failure".into()),
                        };
                        let mut bytes = fs::read(&target)?;
                        bytes[0] ^= 1;
                        fs::write(target, bytes)?;
                    }
                    Ok(())
                });
                assert!(result.is_err(), "{stage}");
                assert!(!output.join("derivation.json").exists(), "{stage}");
                assert!(output.join("normalized.f32").exists());
                if stage == "before_publish" {
                    assert_eq!(
                        result.unwrap_err().to_string(),
                        "fixture publication failure"
                    );
                }
            }
        }
    }
}
