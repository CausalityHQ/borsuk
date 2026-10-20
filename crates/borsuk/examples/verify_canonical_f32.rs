//! Restore original-ordinal raw f32 bytes from LE i64 ordinal + D LE f32 records.
//! Opt-in --recover-order extracts physical-order LE u64 IDs, requiring the pinned order SHA.
//! Linux only. See usage in main; paths must be absolute and contain no symlinks.
//! The memory budget admits explicit buffers/control, not RSS or filesystem cache.
//! A .pending receipt is never valid. The final receipt is provisional until the
//! original process exits zero after syncing the final receipt directory.
//! On I/O failure cleanup is best effort; the marker alone never proves success.

#[cfg(target_os = "linux")]
mod linux {
    use rustix::fs::{AtFlags, CWD, Mode, OFlags, RenameFlags, openat, renameat_with, statat};
    use serde::Serialize;
    use sha2::{Digest, Sha256};
    use std::{
        error::Error,
        ffi::OsString,
        fs::{File, Metadata},
        io::{self, Cursor, Read, Seek, SeekFrom, Write},
        os::unix::fs::{FileExt, MetadataExt},
        path::{Component, Path, PathBuf},
    };

    type Result<T> = std::result::Result<T, Box<dyn Error>>;
    const BUFFER_BYTES: usize = 65_536;
    const CONTROL_BYTES: u64 = 65_536;
    const RECEIPT_BYTES: usize = 4096;
    const USAGE: &str = "usage: verify_canonical_f32 CANONICAL_PATH CANONICAL_BYTES CANONICAL_SHA256 N D ORIGINAL_RAW_SHA256 NEW_RAW_PATH NEW_RECEIPT_PATH MEMORY_BUDGET_BYTES";

    struct Config {
        canonical: PathBuf,
        canonical_bytes: u64,
        canonical_sha256: [u8; 32],
        n: u64,
        d: u64,
        original_raw_sha256: [u8; 32],
        raw: PathBuf,
        receipt: PathBuf,
        memory_budget_bytes: u64,
    }

    struct Geometry {
        row_bytes: u64,
        raw_bytes: u64,
        bitmap_bytes: usize,
        admitted_bytes: u64,
    }

    impl Config {
        fn geometry(&self) -> Result<Geometry> {
            if self.n == 0 || self.n > i64::MAX as u64 || self.d == 0 {
                return Err("N and D must be positive; N must fit i64".into());
            }
            let row_bytes = self.d.checked_mul(4).ok_or("row size overflow")?;
            let record_bytes = row_bytes.checked_add(8).ok_or("record size overflow")?;
            let canonical_bytes = self
                .n
                .checked_mul(record_bytes)
                .ok_or("input size overflow")?;
            let raw_bytes = self.n.checked_mul(row_bytes).ok_or("raw size overflow")?;
            if canonical_bytes != self.canonical_bytes || canonical_bytes > i64::MAX as u64 {
                return Err("input geometry/length exceeds supported file offsets".into());
            }
            let bitmap_bytes = self.n.checked_add(7).ok_or("bitmap overflow")? / 8;
            let admitted_bytes = bitmap_bytes
                .checked_add(BUFFER_BYTES as u64)
                .and_then(|n| n.checked_add(CONTROL_BYTES))
                .ok_or("memory admission overflow")?;
            if admitted_bytes > self.memory_budget_bytes {
                return Err("explicit memory budget is insufficient".into());
            }
            let bitmap_bytes = usize::try_from(bitmap_bytes)?;
            if bitmap_bytes > isize::MAX as usize {
                return Err("bitmap exceeds allocation limit".into());
            }
            Ok(Geometry {
                row_bytes,
                raw_bytes,
                bitmap_bytes,
                admitted_bytes,
            })
        }
    }

    fn sha_arg(value: &str) -> Result<[u8; 32]> {
        if value.len() != 64 || !value.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err("SHA256 must contain exactly 64 hexadecimal digits".into());
        }
        let mut result = [0; 32];
        for (i, byte) in result.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16)?;
        }
        Ok(result)
    }

    pub fn main() -> Result<()> {
        let mut args = std::env::args_os().skip(1);
        let first = args.next().ok_or(USAGE)?;
        if first.len() > 4096 {
            return Err("argument exceeds 4096 bytes".into());
        }
        let order_mode = first == "--recover-order";
        let usage = if order_mode { ORDER_USAGE } else { USAGE };
        let mut next = || -> Result<OsString> {
            let value = args.next().ok_or(usage)?;
            if value.len() > 4096 {
                return Err("argument exceeds 4096 bytes".into());
            }
            Ok(value)
        };
        let canonical = PathBuf::from(if order_mode { next()? } else { first });
        let canonical_bytes = next()?.to_str().ok_or(usage)?.parse()?;
        let canonical_sha256 = sha_arg(next()?.to_str().ok_or(usage)?)?;
        let n = next()?.to_str().ok_or(usage)?.parse()?;
        let d = next()?.to_str().ok_or(usage)?.parse()?;
        let original_raw_sha256 = sha_arg(next()?.to_str().ok_or(usage)?)?;
        let raw = PathBuf::from(next()?);
        let receipt = PathBuf::from(next()?);
        let memory_budget_bytes = next()?.to_str().ok_or(usage)?.parse()?;
        if args.next().is_some() {
            return Err(usage.into());
        }
        let config = Config {
            canonical,
            canonical_bytes,
            canonical_sha256,
            n,
            d,
            original_raw_sha256,
            raw,
            receipt,
            memory_budget_bytes,
        };
        if order_mode {
            recover_order(&config)
        } else {
            verify(&config)
        }
    }

    // Anchor operations to nofollow directory descriptors; re-open the path to detect replacement.
    struct Entry {
        path: PathBuf,
        parent: File,
        name: OsString,
    }

    impl Entry {
        fn new(path: &Path) -> Result<Self> {
            if !path.is_absolute() || path.as_os_str().len() > 4096 {
                return Err("file paths must be absolute and at most 4096 bytes".into());
            }
            let name = path.file_name().ok_or("missing filename")?.to_owned();
            let mut parent: File = openat(
                CWD,
                "/",
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
            )?
            .into();
            for component in path.parent().ok_or("missing parent")?.components() {
                match component {
                    Component::RootDir => {}
                    Component::Normal(part) => {
                        parent = openat(
                            &parent,
                            part,
                            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                            Mode::empty(),
                        )?
                        .into();
                    }
                    _ => {
                        return Err(
                            "only absolute paths without parent components are allowed".into()
                        );
                    }
                }
            }
            Ok(Self {
                path: path.to_owned(),
                parent,
                name,
            })
        }

        fn open(&self, create: bool) -> Result<File> {
            let flags = OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC
                | if create {
                    OFlags::RDWR | OFlags::CREATE | OFlags::EXCL
                } else {
                    OFlags::RDONLY
                };
            let file: File =
                openat(&self.parent, &self.name, flags, Mode::RUSR | Mode::WUSR)?.into();
            regular(&file.metadata()?)?;
            Ok(file)
        }

        fn absent(&self) -> Result<()> {
            match statat(&self.parent, &self.name, AtFlags::SYMLINK_NOFOLLOW) {
                Err(rustix::io::Errno::NOENT) => Ok(()),
                Err(e) => Err(e.into()),
                Ok(_) => Err("output/receipt path already occupied".into()),
            }
        }

        fn check(&self, file: &File, expected: &Metadata) -> Result<()> {
            let current = file.metadata()?;
            regular(&current)?;
            let reopened = Self::new(&self.path)?;
            let parent = self.parent.metadata()?;
            let current_parent = reopened.parent.metadata()?;
            if (parent.dev(), parent.ino()) != (current_parent.dev(), current_parent.ino()) {
                return Err("parent directory identity changed".into());
            }
            let named = reopened.open(false)?.metadata()?;
            if !same_metadata(expected, &current) || !same_metadata(&current, &named) {
                return Err("file path/descriptor identity, size or timestamps changed".into());
            }
            Ok(())
        }
    }

    fn regular(meta: &Metadata) -> Result<()> {
        if !meta.is_file() || meta.nlink() != 1 {
            return Err("expected regular single-link file".into());
        }
        Ok(())
    }

    fn same_metadata(a: &Metadata, b: &Metadata) -> bool {
        (
            a.dev(),
            a.ino(),
            a.len(),
            a.nlink(),
            a.mtime(),
            a.mtime_nsec(),
            a.ctime(),
            a.ctime_nsec(),
        ) == (
            b.dev(),
            b.ino(),
            b.len(),
            b.nlink(),
            b.mtime(),
            b.mtime_nsec(),
            b.ctime(),
            b.ctime_nsec(),
        )
    }

    fn hash_file(file: &mut File, bytes: u64, buffer: &mut [u8]) -> Result<[u8; 32]> {
        file.seek(SeekFrom::Start(0))?;
        let mut left = bytes;
        let mut hash = Sha256::new();
        while left != 0 {
            let take = left.min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..take])?;
            hash.update(&buffer[..take]);
            left -= take as u64;
        }
        if file.read(&mut [0])? != 0 {
            return Err("extra bytes after expected EOF".into());
        }
        Ok(hash.finalize().into())
    }

    #[derive(Serialize)]
    struct Receipt {
        schema: &'static str,
        status: &'static str,
        success_requires: &'static str,
        source_sha256: String,
        executable_sha256: String,
        canonical_bytes: u64,
        canonical_sha256: String,
        raw_bytes: u64,
        raw_sha256: String,
        n: u64,
        d: u64,
        id_bijection: bool,
        raw_bit_identity: bool,
        memory_budget_bytes: u64,
        admitted_bytes: u64,
    }

    fn hex(hash: &[u8; 32]) -> String {
        use std::fmt::Write;
        let mut text = String::with_capacity(64);
        for byte in hash {
            write!(&mut text, "{byte:02x}").expect("writing to String");
        }
        text
    }

    fn verify(config: &Config) -> Result<()> {
        let geometry = config.geometry()?;
        let input_entry = Entry::new(&config.canonical)?;
        let raw_entry = Entry::new(&config.raw)?;
        let receipt_entry = Entry::new(&config.receipt)?;
        raw_entry.absent()?;
        receipt_entry.absent()?;
        let mut input = input_entry.open(false)?;
        let input_meta = input.metadata()?;
        if input_meta.len() != config.canonical_bytes {
            return Err("canonical length mismatch".into());
        }
        input_entry.check(&input, &input_meta)?;

        let mut buffer = Vec::new();
        buffer.try_reserve_exact(BUFFER_BYTES)?;
        buffer.resize(BUFFER_BYTES, 0_u8);
        let executable_entry = Entry::new(&std::env::current_exe()?)?;
        let mut executable = executable_entry.open(false)?;
        let executable_meta = executable.metadata()?;
        let executable_sha256 = hash_file(&mut executable, executable_meta.len(), &mut buffer)?;
        executable_entry.check(&executable, &executable_meta)?;
        let mut seen = Vec::new();
        seen.try_reserve_exact(geometry.bitmap_bytes)?;
        seen.resize(geometry.bitmap_bytes, 0_u8);
        let mut raw = raw_entry.open(true)?;
        let mut input_hash = Sha256::new();
        let mut seen_count = 0_u64;
        for _ in 0..config.n {
            let mut ordinal = [0; 8];
            input.read_exact(&mut ordinal)?;
            input_hash.update(ordinal);
            let id = u64::try_from(i64::from_le_bytes(ordinal)).map_err(|_| "negative ordinal")?;
            if id >= config.n {
                return Err("ordinal out of range".into());
            }
            let index = usize::try_from(id / 8)?;
            let mask = 1_u8 << (id % 8);
            if seen[index] & mask != 0 {
                return Err("duplicate ordinal (therefore missing ordinal)".into());
            }
            seen[index] |= mask;
            seen_count += 1;
            let mut offset = id
                .checked_mul(geometry.row_bytes)
                .ok_or("output offset overflow")?;
            let mut left = geometry.row_bytes;
            while left != 0 {
                let take = left.min(BUFFER_BYTES as u64) as usize;
                input.read_exact(&mut buffer[..take])?;
                input_hash.update(&buffer[..take]);
                for value in buffer[..take].chunks_exact(4) {
                    let bytes = [value[0], value[1], value[2], value[3]];
                    if !f32::from_le_bytes(bytes).is_finite() {
                        return Err("nonfinite f32 payload".into());
                    }
                }
                raw.write_all_at(&buffer[..take], offset)?;
                offset = offset
                    .checked_add(take as u64)
                    .ok_or("output offset overflow")?;
                left -= take as u64;
            }
        }
        if seen_count != config.n
            || input.read(&mut [0])? != 0
            || <[u8; 32]>::from(input_hash.finalize()) != config.canonical_sha256
        {
            return Err("canonical hash/EOF/ordinal coverage mismatch".into());
        }
        input_entry.check(&input, &input_meta)?;
        let raw_meta = raw.metadata()?;
        if raw_meta.len() != geometry.raw_bytes {
            return Err("raw length mismatch".into());
        }
        raw_entry.check(&raw, &raw_meta)?;
        let raw_sha256 = hash_file(&mut raw, geometry.raw_bytes, &mut buffer)?;
        if raw_sha256 != config.original_raw_sha256 {
            return Err("original-ordinal raw bit identity mismatch".into());
        }
        raw.sync_all()?;
        raw_entry.parent.sync_all()?;
        raw_entry.check(&raw, &raw_meta)?;
        // Re-read after unpermutation, so a changing input cannot seal a stale digest.
        if hash_file(&mut input, config.canonical_bytes, &mut buffer)? != config.canonical_sha256 {
            return Err("canonical post-processing hash mismatch".into());
        }
        input_entry.check(&input, &input_meta)?;
        executable_entry.check(&executable, &executable_meta)?;
        raw_entry.check(&raw, &raw_meta)?;

        let receipt = Receipt {
            schema: "borsuk-canonical-f32-provenance-v1",
            status: "provisional",
            success_requires: "original_exit_zero_after_final_receipt_directory_sync",
            source_sha256: hex(&Sha256::digest(include_bytes!("verify_canonical_f32.rs")).into()),
            executable_sha256: hex(&executable_sha256),
            canonical_bytes: config.canonical_bytes,
            canonical_sha256: hex(&config.canonical_sha256),
            raw_bytes: geometry.raw_bytes,
            raw_sha256: hex(&raw_sha256),
            n: config.n,
            d: config.d,
            id_bijection: true,
            raw_bit_identity: true,
            memory_budget_bytes: config.memory_budget_bytes,
            admitted_bytes: geometry.admitted_bytes,
        };
        let mut receipt_buffer = [0; RECEIPT_BYTES];
        let mut cursor = Cursor::new(receipt_buffer.as_mut_slice());
        serde_json::to_writer(&mut cursor, &receipt)?;
        cursor.write_all(b"\n")?;
        let length = usize::try_from(cursor.position())?;
        publish_receipt(&receipt_entry, &receipt_buffer[..length], || {
            input_entry.check(&input, &input_meta)?;
            raw_entry.check(&raw, &raw_meta)?;
            executable_entry.check(&executable, &executable_meta)
        })
    }

    // 10 bounded argv values, 24 live path/name copies, receipt, fixed caller/hash scratch.
    const ORDER_CONTROL_BYTES: u64 = (10 + 24) * 4096 + RECEIPT_BYTES as u64 + 16_384;
    const ORDER_WRITER_BYTES: u64 = 8; // Direct File writes have no buffered-writer allocation.
    const ORDER_USAGE: &str = "usage: verify_canonical_f32 --recover-order CANONICAL_PATH CANONICAL_BYTES CANONICAL_SHA256 N D EXPECTED_ORDER_SHA256 NEW_ORDER_PATH NEW_RECEIPT_PATH MEMORY_BUDGET_BYTES";

    impl Config {
        fn order_geometry(&self) -> Result<Geometry> {
            let mut geometry = self.geometry()?;
            geometry.raw_bytes = self.n.checked_mul(8).ok_or("order size overflow")?;
            geometry.admitted_bytes = (geometry.bitmap_bytes as u64)
                .checked_add(BUFFER_BYTES as u64)
                .and_then(|n| n.checked_add(ORDER_WRITER_BYTES))
                .and_then(|n| n.checked_add(ORDER_CONTROL_BYTES))
                .ok_or("order memory admission overflow")?;
            if geometry.admitted_bytes > self.memory_budget_bytes {
                return Err("explicit order memory budget is insufficient".into());
            }
            Ok(geometry)
        }
    }

    #[derive(Serialize)]
    struct OrderReceipt {
        schema: &'static str,
        status: &'static str,
        success_requires: &'static str,
        source_sha256: String,
        executable_sha256: String,
        canonical_bytes: u64,
        canonical_sha256: String,
        order_bytes: u64,
        order_sha256: String,
        n: u64,
        d: u64,
        id_bijection: bool,
        order_bit_identity: bool,
        memory_budget_bytes: u64,
        admitted_bytes: u64,
    }

    // The legacy CLI output path/hash slots name order.u64 in this opt-in mode.
    fn recover_order(config: &Config) -> Result<()> {
        let geometry = config.order_geometry()?;
        let input_entry = Entry::new(&config.canonical)?;
        let order_entry = Entry::new(&config.raw)?;
        let receipt_entry = Entry::new(&config.receipt)?;
        order_entry.absent()?;
        receipt_entry.absent()?;
        let mut input = input_entry.open(false)?;
        let input_meta = input.metadata()?;
        if input_meta.len() != config.canonical_bytes {
            return Err("canonical length mismatch".into());
        }
        input_entry.check(&input, &input_meta)?;

        let mut buffer = Vec::new();
        buffer.try_reserve_exact(BUFFER_BYTES)?;
        buffer.resize(BUFFER_BYTES, 0_u8);
        let executable_entry = Entry::new(&std::env::current_exe()?)?;
        let mut executable = executable_entry.open(false)?;
        let executable_meta = executable.metadata()?;
        let executable_sha256 = hash_file(&mut executable, executable_meta.len(), &mut buffer)?;
        executable_entry.check(&executable, &executable_meta)?;
        let mut seen = Vec::new();
        seen.try_reserve_exact(geometry.bitmap_bytes)?;
        seen.resize(geometry.bitmap_bytes, 0_u8);
        let mut order = order_entry.open(true)?;
        let mut input_hash = Sha256::new();
        let mut order_hash = Sha256::new();
        for _ in 0..config.n {
            let mut ordinal = [0; 8];
            input.read_exact(&mut ordinal)?;
            input_hash.update(ordinal);
            let id = u64::from_le_bytes(ordinal);
            if id >= config.n {
                return Err("ordinal out of range".into());
            }
            let index = usize::try_from(id / 8)?;
            let mask = 1_u8 << (id % 8);
            if seen[index] & mask != 0 {
                return Err("duplicate ordinal (therefore missing ordinal)".into());
            }
            seen[index] |= mask;
            order.write_all(&ordinal)?;
            order_hash.update(ordinal);
            let mut left = geometry.row_bytes;
            while left != 0 {
                let take = left.min(BUFFER_BYTES as u64) as usize;
                input.read_exact(&mut buffer[..take])?;
                input_hash.update(&buffer[..take]);
                left -= take as u64;
            }
        }
        // N distinct in-range IDs prove the complete 0..N permutation.
        if input.read(&mut [0])? != 0
            || <[u8; 32]>::from(input_hash.finalize()) != config.canonical_sha256
        {
            return Err("canonical hash/EOF mismatch".into());
        }
        input_entry.check(&input, &input_meta)?;
        let order_sha256: [u8; 32] = order_hash.finalize().into();
        if order_sha256 != config.original_raw_sha256 {
            return Err("expected source order hash mismatch".into());
        }
        let order_meta = order.metadata()?;
        if order_meta.len() != geometry.raw_bytes {
            return Err("order length mismatch".into());
        }
        order_entry.check(&order, &order_meta)?;
        if hash_file(&mut order, geometry.raw_bytes, &mut buffer)? != order_sha256 {
            return Err("order readback hash mismatch".into());
        }
        order.sync_all()?;
        order_entry.parent.sync_all()?;
        order_entry.check(&order, &order_meta)?;
        if hash_file(&mut input, config.canonical_bytes, &mut buffer)? != config.canonical_sha256 {
            return Err("canonical post-processing hash mismatch".into());
        }
        input_entry.check(&input, &input_meta)?;
        executable_entry.check(&executable, &executable_meta)?;
        order_entry.check(&order, &order_meta)?;

        let receipt = OrderReceipt {
            schema: "borsuk-canonical-order-provenance-v1",
            status: "provisional",
            success_requires: "original_exit_zero_after_final_receipt_directory_sync",
            source_sha256: hex(&Sha256::digest(include_bytes!("verify_canonical_f32.rs")).into()),
            executable_sha256: hex(&executable_sha256),
            canonical_bytes: config.canonical_bytes,
            canonical_sha256: hex(&config.canonical_sha256),
            order_bytes: geometry.raw_bytes,
            order_sha256: hex(&order_sha256),
            n: config.n,
            d: config.d,
            id_bijection: true,
            order_bit_identity: true,
            memory_budget_bytes: config.memory_budget_bytes,
            admitted_bytes: geometry.admitted_bytes,
        };
        let mut receipt_buffer = [0; RECEIPT_BYTES];
        let mut cursor = Cursor::new(receipt_buffer.as_mut_slice());
        serde_json::to_writer(&mut cursor, &receipt)?;
        cursor.write_all(b"\n")?;
        let length = usize::try_from(cursor.position())?;
        publish_receipt(&receipt_entry, &receipt_buffer[..length], || {
            input_entry.check(&input, &input_meta)?;
            order_entry.check(&order, &order_meta)?;
            executable_entry.check(&executable, &executable_meta)
        })
    }

    fn publish_receipt(
        entry: &Entry,
        bytes: &[u8],
        final_check: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let file = stage_receipt(entry, bytes, final_check)?;
        finish_publication(entry, &file, bytes, entry.parent.sync_all())
    }

    fn stage_receipt(
        entry: &Entry,
        bytes: &[u8],
        final_check: impl FnOnce() -> Result<()>,
    ) -> Result<File> {
        let mut pending_path = entry.path.clone().into_os_string();
        pending_path.push(".pending");
        let pending = Entry::new(Path::new(&pending_path))?;
        let mut file = pending.open(true)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        pending.check(&file, &file.metadata()?)?;
        // Verify the final parent still names the directory opened before processing.
        let current = Entry::new(&entry.path)?;
        if (
            current.parent.metadata()?.dev(),
            current.parent.metadata()?.ino(),
        ) != (
            entry.parent.metadata()?.dev(),
            entry.parent.metadata()?.ino(),
        ) {
            return Err("receipt parent changed".into());
        }
        final_check()?;
        renameat_with(
            &pending.parent,
            &pending.name,
            &entry.parent,
            &entry.name,
            RenameFlags::NOREPLACE,
        )?;
        Ok(file)
    }

    fn finish_publication(
        entry: &Entry,
        file: &File,
        bytes: &[u8],
        synced: io::Result<()>,
    ) -> Result<()> {
        // Keep provisional evidence; rollback by pathname could delete another writer's file.
        synced.map_err(|error| format!("receipt directory sync failed: {error}; receipt remains provisional; original exit zero is required"))?;
        let meta = file.metadata()?;
        entry.check(file, &meta)?;
        if bytes.len() > RECEIPT_BYTES || meta.len() != bytes.len() as u64 {
            return Err("published receipt length mismatch".into());
        }
        let mut actual = [0; RECEIPT_BYTES];
        file.read_exact_at(&mut actual[..bytes.len()], 0)?;
        if &actual[..bytes.len()] != bytes {
            return Err("published receipt bytes changed".into());
        }
        entry.check(file, &meta)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::{fs, os::unix::fs::symlink};
        use tempfile::{TempDir, tempdir};

        struct Fixture {
            _dir: TempDir,
            config: Config,
            raw: Vec<u8>,
        }

        fn fixture() -> Fixture {
            let dir = tempdir().unwrap();
            let raw = [
                0x8000_0000_u32,
                0x0000_0000, // -0, +0
                0x3f80_0000,
                0xbf80_0000, // +1, -1
                0x0000_0001,
                0x7f7f_ffff, // subnormal, largest finite
            ]
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
            let mut canonical = Vec::new();
            for id in [2_i64, 0, 1] {
                canonical.extend_from_slice(&id.to_le_bytes());
                canonical.extend_from_slice(&raw[id as usize * 8..id as usize * 8 + 8]);
            }
            let config = Config {
                canonical: dir.path().join("canonical"),
                canonical_bytes: canonical.len() as u64,
                canonical_sha256: Sha256::digest(&canonical).into(),
                n: 3,
                d: 2,
                original_raw_sha256: Sha256::digest(&raw).into(),
                raw: dir.path().join("raw"),
                receipt: dir.path().join("receipt.json"),
                memory_budget_bytes: 131_073,
            };
            fs::write(&config.canonical, canonical).unwrap();
            Fixture {
                _dir: dir,
                config,
                raw,
            }
        }

        fn order_fixture() -> Fixture {
            let mut fixture = fixture();
            fixture.config.original_raw_sha256 =
                sha_arg("53afea624a503a0bf39e469e8979f67fcb1890ea0392adf9e155124f5ede9ebb")
                    .unwrap();
            fixture.config.memory_budget_bytes =
                1 + BUFFER_BYTES as u64 + ORDER_WRITER_BYTES + ORDER_CONTROL_BYTES;
            fixture
        }

        fn order_rejected(fixture: &Fixture, reason: &str) {
            let error = recover_order(&fixture.config).unwrap_err().to_string();
            assert!(error.contains(reason), "expected {reason:?}, got {error:?}");
            assert!(!fixture.config.receipt.exists());
        }

        #[test]
        fn recover_order_exact_permutation_bytes_and_sha_golden() {
            let fixture = order_fixture();
            recover_order(&fixture.config).unwrap();
            let expected = [
                2_u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
            ];
            assert_eq!(fs::read(&fixture.config.raw).unwrap(), expected);
            assert_eq!(
                hex(&Sha256::digest(expected).into()),
                "53afea624a503a0bf39e469e8979f67fcb1890ea0392adf9e155124f5ede9ebb"
            );
            let receipt: serde_json::Value =
                serde_json::from_slice(&fs::read(&fixture.config.receipt).unwrap()).unwrap();
            assert_eq!(receipt["schema"], "borsuk-canonical-order-provenance-v1");
            assert_eq!(receipt["status"], "provisional");
            assert_eq!(
                receipt["success_requires"],
                "original_exit_zero_after_final_receipt_directory_sync"
            );
            assert_eq!(receipt["order_bytes"], 24);
            assert_eq!(
                receipt["order_sha256"],
                "53afea624a503a0bf39e469e8979f67fcb1890ea0392adf9e155124f5ede9ebb"
            );
            assert_eq!(
                receipt["canonical_sha256"],
                hex(&fixture.config.canonical_sha256)
            );
            assert_eq!(receipt["id_bijection"], true);
            assert_eq!(receipt["order_bit_identity"], true);
            assert_eq!(
                receipt["admitted_bytes"],
                fixture.config.memory_budget_bytes
            );
        }

        #[test]
        fn recover_order_rejects_duplicate_range_truncate_trailing_and_hashes() {
            for id in [0_u64, 3, u64::MAX] {
                let mut fixture = order_fixture();
                let mut bytes = fs::read(&fixture.config.canonical).unwrap();
                bytes[..8].copy_from_slice(&id.to_le_bytes());
                replace_input(&mut fixture, &bytes);
                order_rejected(
                    &fixture,
                    if id == 0 {
                        "duplicate ordinal"
                    } else {
                        "ordinal out of range"
                    },
                );
            }
            for size in [0, 7, 47, 49] {
                let mut fixture = order_fixture();
                let mut bytes = fs::read(&fixture.config.canonical).unwrap();
                bytes.resize(size, 0);
                replace_input(&mut fixture, &bytes);
                order_rejected(&fixture, "canonical length mismatch");
                assert!(!fixture.config.raw.exists());
            }
            let mut fixture = order_fixture();
            fixture.config.canonical_sha256[0] ^= 1;
            order_rejected(&fixture, "canonical hash");
            let mut fixture = order_fixture();
            fixture.config.original_raw_sha256[0] ^= 1;
            order_rejected(&fixture, "source order hash");
        }

        #[test]
        fn recover_order_occupied_paths_and_budget_fail_closed() {
            for receipt_path in [false, true] {
                let fixture = order_fixture();
                let path = if receipt_path {
                    &fixture.config.receipt
                } else {
                    &fixture.config.raw
                };
                fs::write(path, b"foreign").unwrap();
                assert!(recover_order(&fixture.config).is_err());
                assert_eq!(fs::read(path).unwrap(), b"foreign");
                if receipt_path {
                    assert!(!fixture.config.raw.exists());
                } else {
                    assert!(!fixture.config.receipt.exists());
                }
            }
            let fixture = order_fixture();
            let pending = fixture._dir.path().join("receipt.json.pending");
            fs::write(&pending, b"foreign pending").unwrap();
            order_rejected(&fixture, "File exists");
            assert_eq!(fs::read(&pending).unwrap(), b"foreign pending");

            let mut fixture = order_fixture();
            fixture.config.memory_budget_bytes -= 1;
            order_rejected(&fixture, "order memory budget");
            assert!(!fixture.config.raw.exists());
        }

        #[test]
        fn recover_order_streams_rows_larger_than_buffer() {
            let mut fixture = order_fixture();
            fixture.config.d = (BUFFER_BYTES / 4 + 1) as u64;
            let mut bytes = Vec::new();
            for id in [2_u64, 0, 1] {
                bytes.extend_from_slice(&id.to_le_bytes());
                bytes.resize(bytes.len() + fixture.config.d as usize * 4, 0);
            }
            fixture.config.canonical_bytes = bytes.len() as u64;
            replace_input(&mut fixture, &bytes);
            recover_order(&fixture.config).unwrap();
            assert_eq!(fs::metadata(&fixture.config.raw).unwrap().len(), 24);
            assert_eq!(
                hex(&Sha256::digest(fs::read(&fixture.config.raw).unwrap()).into()),
                "53afea624a503a0bf39e469e8979f67fcb1890ea0392adf9e155124f5ede9ebb"
            );
        }

        fn replace_input(fixture: &mut Fixture, bytes: &[u8]) {
            fs::write(&fixture.config.canonical, bytes).unwrap();
            fixture.config.canonical_sha256 = Sha256::digest(bytes).into();
        }

        fn rejected(fixture: &Fixture, reason: &str) {
            let error = verify(&fixture.config).unwrap_err().to_string();
            assert!(error.contains(reason), "expected {reason:?}, got {error:?}");
            assert!(!fixture.config.receipt.exists());
        }

        #[test]
        fn exact_permutation_preserves_signed_zero_bits() {
            let fixture = fixture();
            verify(&fixture.config).unwrap();
            assert_eq!(fs::read(&fixture.config.raw).unwrap(), fixture.raw);
            let receipt: serde_json::Value =
                serde_json::from_slice(&fs::read(&fixture.config.receipt).unwrap()).unwrap();
            assert_eq!(receipt["schema"], "borsuk-canonical-f32-provenance-v1");
            assert_eq!(receipt["status"], "provisional");
            assert_eq!(
                receipt["success_requires"],
                "original_exit_zero_after_final_receipt_directory_sync"
            );
            assert_eq!(receipt["n"], 3);
            assert_eq!(receipt["d"], 2);
            assert_eq!(receipt["canonical_bytes"], 48);
            assert_eq!(receipt["raw_bytes"], 24);
            assert_eq!(receipt["id_bijection"], true);
            assert_eq!(receipt["raw_bit_identity"], true);
            assert_eq!(receipt["admitted_bytes"], 131_073);
            assert_eq!(
                receipt["raw_sha256"],
                hex(&fixture.config.original_raw_sha256)
            );
            assert_eq!(
                receipt["source_sha256"],
                hex(&Sha256::digest(include_bytes!("verify_canonical_f32.rs")).into())
            );
            assert_eq!(receipt["executable_sha256"].as_str().unwrap().len(), 64);
        }

        #[test]
        fn rows_larger_than_buffer_stream_without_reencoding() {
            let mut fixture = fixture();
            fixture.config.n = 2;
            fixture.config.d = (BUFFER_BYTES / 4 + 1) as u64;
            let row_bytes = fixture.config.d as usize * 4;
            let mut raw = vec![0_u8; row_bytes * 2];
            raw[..4].copy_from_slice(&0x8000_0000_u32.to_le_bytes());
            raw[row_bytes..row_bytes + 4].copy_from_slice(&1_f32.to_le_bytes());
            raw[row_bytes * 2 - 4..].copy_from_slice(&0x0000_0001_u32.to_le_bytes());
            let mut canonical = Vec::new();
            for id in [1_i64, 0] {
                canonical.extend_from_slice(&id.to_le_bytes());
                canonical.extend_from_slice(
                    &raw[id as usize * row_bytes..(id as usize + 1) * row_bytes],
                );
            }
            fixture.config.canonical_bytes = canonical.len() as u64;
            fixture.config.original_raw_sha256 = Sha256::digest(&raw).into();
            replace_input(&mut fixture, &canonical);
            verify(&fixture.config).unwrap();
            assert_eq!(fs::read(&fixture.config.raw).unwrap(), raw);
        }

        #[test]
        fn hash_reader_rejects_truncation_and_extra_eof() {
            let fixture = fixture();
            let mut input = File::open(&fixture.config.canonical).unwrap();
            let mut buffer = [0_u8; 7];
            assert!(hash_file(&mut input, 49, &mut buffer).is_err());
            assert!(hash_file(&mut input, 47, &mut buffer).is_err());
            assert_eq!(
                hash_file(&mut input, 48, &mut buffer).unwrap(),
                fixture.config.canonical_sha256
            );
        }

        #[test]
        fn duplicate_and_missing_ids_rejected() {
            let mut fixture = fixture();
            let mut bytes = fs::read(&fixture.config.canonical).unwrap();
            bytes[32..40].copy_from_slice(&0_i64.to_le_bytes());
            replace_input(&mut fixture, &bytes);
            rejected(&fixture, "duplicate ordinal");
            assert!(fixture.config.raw.exists());

            let mut fixture = self::fixture();
            let bytes = fs::read(&fixture.config.canonical).unwrap();
            replace_input(&mut fixture, &bytes[..32]);
            rejected(&fixture, "canonical length mismatch");
        }

        #[test]
        fn out_of_range_ids_rejected() {
            for id in [-1_i64, 3, i64::MAX] {
                let mut fixture = fixture();
                let mut bytes = fs::read(&fixture.config.canonical).unwrap();
                bytes[..8].copy_from_slice(&id.to_le_bytes());
                replace_input(&mut fixture, &bytes);
                rejected(
                    &fixture,
                    if id < 0 {
                        "negative ordinal"
                    } else {
                        "ordinal out of range"
                    },
                );
            }
        }

        #[test]
        fn truncated_extra_and_wrong_hash_rejected() {
            for size in [0, 7, 47, 49] {
                let mut fixture = fixture();
                let mut bytes = fs::read(&fixture.config.canonical).unwrap();
                bytes.resize(size, 0);
                replace_input(&mut fixture, &bytes);
                rejected(&fixture, "canonical length mismatch");
            }
            let mut fixture = fixture();
            fixture.config.canonical_sha256[0] ^= 1;
            rejected(&fixture, "canonical hash");
            let mut fixture = self::fixture();
            fixture.config.original_raw_sha256[0] ^= 1;
            rejected(&fixture, "raw bit identity");
        }

        #[test]
        fn nonfinite_payload_rejected() {
            for bits in [0x7f80_0000_u32, 0xff80_0000, 0x7fc0_0001] {
                let mut fixture = fixture();
                let mut bytes = fs::read(&fixture.config.canonical).unwrap();
                bytes[8..12].copy_from_slice(&bits.to_le_bytes());
                replace_input(&mut fixture, &bytes);
                rejected(&fixture, "nonfinite");
            }
        }

        #[test]
        fn occupied_paths_are_preserved() {
            let fixture = fixture();
            fs::write(&fixture.config.raw, b"keep raw").unwrap();
            rejected(&fixture, "occupied");
            assert_eq!(fs::read(&fixture.config.raw).unwrap(), b"keep raw");
            let fixture = self::fixture();
            fs::write(&fixture.config.receipt, b"keep receipt").unwrap();
            assert!(verify(&fixture.config).is_err());
            assert_eq!(fs::read(&fixture.config.receipt).unwrap(), b"keep receipt");
            assert!(!fixture.config.raw.exists());
        }

        #[test]
        fn symlinks_and_hardlinks_rejected() {
            for hardlink in [false, true] {
                let fixture = fixture();
                let other = fixture._dir.path().join("other");
                if hardlink {
                    fs::hard_link(&fixture.config.canonical, &other).unwrap();
                } else {
                    fs::rename(&fixture.config.canonical, &other).unwrap();
                    symlink(&other, &fixture.config.canonical).unwrap();
                }
                assert!(verify(&fixture.config).is_err());
                assert!(!fixture.config.raw.exists());
                assert!(!fixture.config.receipt.exists());
            }
            for receipt_path in [false, true] {
                for hardlink in [false, true] {
                    let fixture = fixture();
                    let target = fixture._dir.path().join("target");
                    fs::write(&target, b"keep").unwrap();
                    let path = if receipt_path {
                        &fixture.config.receipt
                    } else {
                        &fixture.config.raw
                    };
                    if hardlink {
                        fs::hard_link(&target, path).unwrap();
                    } else {
                        symlink(&target, path).unwrap();
                    }
                    assert!(verify(&fixture.config).is_err());
                    assert_eq!(fs::read(&target).unwrap(), b"keep");
                    if !receipt_path {
                        assert!(!fixture.config.receipt.exists());
                    }
                }
            }
            let mut fixture = fixture();
            let link = fixture._dir.path().join("linked-parent");
            symlink(fixture._dir.path(), &link).unwrap();
            fixture.config.raw = link.join("raw");
            assert!(verify(&fixture.config).is_err());
            assert!(!fixture.config.receipt.exists());
        }

        #[test]
        fn receipt_failure_has_no_success_receipt() {
            let fixture = fixture();
            let pending = fixture._dir.path().join("receipt.json.pending");
            fs::write(&pending, b"keep pending").unwrap();
            assert!(verify(&fixture.config).is_err());
            assert_eq!(fs::read(&fixture.config.raw).unwrap(), fixture.raw);
            assert_eq!(fs::read(pending).unwrap(), b"keep pending");
            assert!(!fixture.config.receipt.exists());

            let fixture = self::fixture();
            let entry = Entry::new(&fixture.config.receipt).unwrap();
            let mut file = entry.open(true).unwrap();
            file.write_all(b"{\"status\":\"provisional\"}").unwrap();
            let failure = io::Error::other("injected parent sync failure");
            assert!(
                finish_publication(&entry, &file, b"{\"status\":\"provisional\"}", Err(failure))
                    .is_err()
            );
            assert_eq!(
                fs::read(&fixture.config.receipt).unwrap(),
                b"{\"status\":\"provisional\"}"
            );
            assert!(file.metadata().unwrap().len() > 0);
        }

        #[test]
        fn receipt_pending_substitution_cannot_attest_zero() {
            let fixture = fixture();
            let entry = Entry::new(&fixture.config.receipt).unwrap();
            let pending = fixture.config.receipt.with_extension("json.pending");
            let foreign = fixture._dir.path().join("foreign");
            fs::write(&foreign, b"foreign receipt").unwrap();
            let result = publish_receipt(&entry, b"expected receipt", || {
                fs::rename(&foreign, &pending)?;
                Ok(())
            });
            assert!(result.is_err());
            assert_eq!(
                fs::read(&fixture.config.receipt).unwrap(),
                b"foreign receipt"
            );
        }
        #[test]
        fn receipt_sync_failure_preserves_foreign_replacement() {
            let fixture = fixture();
            let entry = Entry::new(&fixture.config.receipt).unwrap();
            let mut file = entry.open(true).unwrap();
            file.write_all(b"original provisional").unwrap();
            let foreign = fixture._dir.path().join("foreign");
            fs::write(&foreign, b"foreign receipt").unwrap();
            fs::rename(&foreign, &fixture.config.receipt).unwrap();
            assert!(
                finish_publication(
                    &entry,
                    &file,
                    b"original provisional",
                    Err(io::Error::other("injected sync failure"))
                )
                .is_err()
            );
            assert_eq!(
                fs::read(&fixture.config.receipt).unwrap(),
                b"foreign receipt"
            );
            assert_eq!(file.metadata().unwrap().len(), 20);
        }
        #[test]
        fn receipt_final_check_and_publish_race_fail_closed() {
            let fixture = fixture();
            let entry = Entry::new(&fixture.config.receipt).unwrap();
            assert!(publish_receipt(&entry, b"staged", || Err("changed input".into())).is_err());
            assert!(!fixture.config.receipt.exists());

            let fixture = self::fixture();
            let entry = Entry::new(&fixture.config.receipt).unwrap();
            assert!(
                publish_receipt(&entry, b"staged", || {
                    fs::write(&fixture.config.receipt, b"concurrent owner")?;
                    Ok(())
                })
                .is_err()
            );
            assert_eq!(
                fs::read(&fixture.config.receipt).unwrap(),
                b"concurrent owner"
            );
        }

        #[test]
        fn geometry_and_memory_checked_before_creation() {
            for (n, d, bytes, budget) in [
                (0, 2, 48, 131_073),
                (3, 0, 48, 131_073),
                (3, 2, 47, 131_073),
                (3, u64::MAX, 48, u64::MAX),
                (u64::MAX, 2, 48, u64::MAX),
                (3, 2, 48, 131_072),
            ] {
                let mut fixture = fixture();
                fixture.config.n = n;
                fixture.config.d = d;
                fixture.config.canonical_bytes = bytes;
                fixture.config.memory_budget_bytes = budget;
                assert!(verify(&fixture.config).is_err());
                assert!(!fixture.config.raw.exists());
                assert!(!fixture.config.receipt.exists());
            }
        }

        #[test]
        fn input_replacement_and_mutation_rejected() {
            for replacement in [false, true] {
                let fixture = fixture();
                let entry = Entry::new(&fixture.config.canonical).unwrap();
                let file = entry.open(false).unwrap();
                let before = file.metadata().unwrap();
                if replacement {
                    fs::rename(&fixture.config.canonical, fixture._dir.path().join("old")).unwrap();
                    fs::write(
                        &fixture.config.canonical,
                        fs::read(fixture._dir.path().join("old")).unwrap(),
                    )
                    .unwrap();
                } else {
                    fs::write(&fixture.config.canonical, b"mutated").unwrap();
                }
                assert!(entry.check(&file, &before).is_err());
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    linux::main()
}

#[cfg(not(target_os = "linux"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("verify_canonical_f32 requires Linux nofollow/create-only filesystem operations".into())
}
