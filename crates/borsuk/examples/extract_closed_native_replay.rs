//! Authenticated experiment artifact extraction. Inventory acceptance is not
//! native closure, ANN correctness, recall, or performance acceptance.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, Read};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Pin {
    path: String,
    bytes: u64,
    sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Authority {
    schema: String,
    arm: String,
    archive: Pin,
    manifest: Pin,
}

#[derive(PartialEq, Eq, Debug, Clone)]
struct Stamp(u64, u64, u32, u64, u64, i64, i64, i64, i64);
fn stamp(m: &Metadata) -> Stamp {
    Stamp(
        m.dev(),
        m.ino(),
        m.mode(),
        m.nlink(),
        m.len(),
        m.mtime(),
        m.mtime_nsec(),
        m.ctime(),
        m.ctime_nsec(),
    )
}
fn canonical(path: &Path) -> Result<()> {
    require(
        path.is_absolute() && std::fs::canonicalize(path)?.as_os_str() == path.as_os_str(),
        "noncanonical/symlink path",
    )
}
struct Input {
    pin: Pin,
    file: File,
    before: Stamp,
}
impl Input {
    fn open(pin: Pin, cap: u64) -> Result<Self> {
        require(
            pin.bytes > 0 && pin.bytes <= cap && sha_valid(&pin.sha256),
            "input pin",
        )?;
        canonical(Path::new(&pin.path))?;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&pin.path)?;
        let m = file.metadata()?;
        require(
            m.is_file() && m.nlink() == 1 && m.len() == pin.bytes,
            "input regular/link/size",
        )?;
        let out = Self {
            pin,
            file,
            before: stamp(&m),
        };
        out.check()?;
        Ok(out)
    }
    fn check(&self) -> Result<()> {
        canonical(Path::new(&self.pin.path))?;
        require(
            self.before == stamp(&self.file.metadata()?)
                && self.before == stamp(&std::fs::symlink_metadata(&self.pin.path)?),
            "input identity drift",
        )
    }
    fn authenticate(&mut self, capture: bool) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut h = Sha256::new();
        let mut bytes = 0u64;
        let mut kept = Vec::new();
        let mut buffer = [0u8; 65536];
        loop {
            let n = self.file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            bytes = bytes.checked_add(n as u64).ok_or("input overflow")?;
            require(bytes <= self.pin.bytes, "input growth")?;
            h.update(&buffer[..n]);
            if capture {
                kept.extend_from_slice(&buffer[..n]);
            }
        }
        require(
            bytes == self.pin.bytes && format!("{:x}", h.finalize()) == self.pin.sha256,
            "input digest/EOF",
        )?;
        self.check()?;
        Ok(kept)
    }
}

fn open_at(parent: &File, name: &str, flags: i32, mode: u32) -> Result<File> {
    let name = std::ffi::CString::new(name)?;
    // Owned directory descriptor and single validated path component; returned
    // descriptor is transferred exactly once to File.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            mode,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}
fn directory_at(parent: &File, name: &str) -> Result<File> {
    require(
        safe_name(name) && !name.contains('/'),
        "directory component",
    )?;
    let c = std::ffi::CString::new(name)?;
    if unsafe { libc::mkdirat(parent.as_raw_fd(), c.as_ptr(), 0o700) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    let file = open_at(parent, name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
    let m = file.metadata()?;
    require(
        m.mode() & 0o777 == 0o700 && m.uid() == unsafe { libc::geteuid() },
        "private directory",
    )?;
    Ok(file)
}
struct Output {
    file: File,
    before: Stamp,
    pin: Pin,
}
struct Destination {
    path: PathBuf,
    parent_path: PathBuf,
    parent: File,
    directories: BTreeMap<String, (File, u64, u64)>,
    files: BTreeMap<String, Output>,
}
impl Destination {
    fn new(path: &Path) -> Result<Self> {
        require(
            path.is_absolute()
                && path.components().all(|c| {
                    !matches!(
                        c,
                        std::path::Component::CurDir | std::path::Component::ParentDir
                    )
                }),
            "destination path",
        )?;
        let parent_path = path.parent().ok_or("destination parent")?.to_owned();
        canonical(&parent_path)?;
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or("destination name")?;
        require(
            parent_path.join(name).as_os_str() == path.as_os_str(),
            "destination lexical path",
        )?;
        let parent = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&parent_path)?;
        let root = directory_at(&parent, name)?;
        let m = root.metadata()?;
        let mut directories = BTreeMap::new();
        directories.insert(String::new(), (root, m.dev(), m.ino()));
        Ok(Self {
            path: path.to_owned(),
            parent_path,
            parent,
            directories,
            files: BTreeMap::new(),
        })
    }
    fn write(&mut self, name: &str, body: &mut dyn Read) -> Result<()> {
        require(safe_name(name), "output name")?;
        let parts: Vec<_> = name.split('/').collect();
        let mut parent = String::new();
        for part in &parts[..parts.len() - 1] {
            let next = if parent.is_empty() {
                part.to_string()
            } else {
                format!("{parent}/{part}")
            };
            if !self.directories.contains_key(&next) {
                let file = directory_at(&self.directories[&parent].0, part)?;
                let m = file.metadata()?;
                self.directories
                    .insert(next.clone(), (file, m.dev(), m.ino()));
            }
            parent = next;
        }
        let mut file = open_at(
            &self.directories[&parent].0,
            parts[parts.len() - 1],
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )?;
        let mut hash = Sha256::new();
        let mut n = 0u64;
        let mut buf = [0u8; 65536];
        loop {
            let count = body.read(&mut buf)?;
            if count == 0 {
                break;
            }
            n = n.checked_add(count as u64).ok_or("output overflow")?;
            require(n <= MAX_MEMBER, "output cap")?;
            file.write_all(&buf[..count])?;
            hash.update(&buf[..count]);
        }
        file.sync_all()?;
        let m = file.metadata()?;
        require(
            m.is_file() && m.nlink() == 1 && m.len() == n,
            "output identity",
        )?;
        let pin = Pin {
            path: self
                .path
                .join(name)
                .to_str()
                .ok_or("output utf8")?
                .to_owned(),
            bytes: n,
            sha256: format!("{:x}", hash.finalize()),
        };
        require(
            self.files
                .insert(
                    name.to_owned(),
                    Output {
                        file,
                        before: stamp(&m),
                        pin,
                    },
                )
                .is_none(),
            "duplicate output",
        )
    }
    fn check_files(&mut self) -> Result<()> {
        for out in self.files.values_mut() {
            canonical(Path::new(&out.pin.path))?;
            require(
                stamp(&out.file.metadata()?) == out.before
                    && stamp(&std::fs::symlink_metadata(&out.pin.path)?) == out.before,
                "output drift",
            )?;
            out.file.seek(SeekFrom::Start(0))?;
            let mut h = Sha256::new();
            let mut buf = [0u8; 65536];
            let mut n = 0u64;
            loop {
                let count = out.file.read(&mut buf)?;
                if count == 0 {
                    break;
                }
                n += count as u64;
                require(n <= out.pin.bytes, "output growth")?;
                h.update(&buf[..count]);
            }
            require(
                n == out.pin.bytes
                    && format!("{:x}", h.finalize()) == out.pin.sha256
                    && stamp(&out.file.metadata()?) == out.before
                    && stamp(&std::fs::symlink_metadata(&out.pin.path)?) == out.before,
                "output digest/identity",
            )?;
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        self.check_files()?;
        for (relative, (file, dev, ino)) in self.directories.iter().rev() {
            let path = self.path.join(relative);
            canonical(&path)?;
            let m = file.metadata()?;
            require(
                m.dev() == *dev
                    && m.ino() == *ino
                    && stamp(&m) == stamp(&std::fs::symlink_metadata(&path)?),
                "directory drift",
            )?;
            file.sync_all()?;
            require(
                stamp(&m) == stamp(&file.metadata()?)
                    && stamp(&m) == stamp(&std::fs::symlink_metadata(&path)?),
                "directory sync drift",
            )?;
        }
        canonical(&self.parent_path)?;
        let before = stamp(&self.parent.metadata()?);
        require(
            before == stamp(&std::fs::symlink_metadata(&self.parent_path)?),
            "parent identity",
        )?;
        self.parent.sync_all()?;
        require(
            before == stamp(&self.parent.metadata()?)
                && before == stamp(&std::fs::symlink_metadata(&self.parent_path)?),
            "parent sync drift",
        )?;
        self.check_files()?;
        for (relative, (file, dev, ino)) in &self.directories {
            let path = self.path.join(relative);
            canonical(&path)?;
            let m = file.metadata()?;
            require(
                m.dev() == *dev
                    && m.ino() == *ino
                    && stamp(&m) == stamp(&std::fs::symlink_metadata(&path)?),
                "final directory drift",
            )?;
        }
        canonical(&self.parent_path)?;
        require(
            before == stamp(&self.parent.metadata()?)
                && before == stamp(&std::fs::symlink_metadata(&self.parent_path)?),
            "final parent drift",
        )
    }
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const MAX_RAW: u64 = 256 * 1024 * 1024;
const MAX_MEMBER: u64 = 64 * 1024 * 1024;
const MAX_HEADERS: usize = 4096;

fn require(ok: bool, reason: &str) -> Result<()> {
    if !ok {
        return Err(reason.into());
    }
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sha_valid(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn safe_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 255
        && s.split('/').all(|part| {
            !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._@+=,-".contains(&b))
        })
}

fn parse_manifest(body: &[u8]) -> Result<BTreeMap<String, String>> {
    require(
        body.len() <= 2 * 1024 * 1024 && body.ends_with(b"\n"),
        "manifest size/newline",
    )?;
    let text = std::str::from_utf8(body)?;
    let mut entries = BTreeMap::new();
    let mut prior = "";
    for line in text[..text.len() - 1].split('\n') {
        let (sha, name) = line.split_once("  ./").ok_or("manifest syntax")?;
        require(
            sha_valid(sha) && safe_name(name) && name > prior,
            "manifest hash/path/order",
        )?;
        require(entries.len() < MAX_HEADERS, "manifest count")?;
        let mut ancestor = name;
        while let Some((parent, _)) = ancestor.rsplit_once('/') {
            require(!entries.contains_key(parent), "manifest file ancestor")?;
            ancestor = parent;
        }
        entries.insert(name.to_owned(), sha.to_owned());
        prior = name;
    }
    require(!entries.is_empty(), "empty manifest")?;
    Ok(entries)
}

fn text_field(bytes: &[u8]) -> Result<&str> {
    let end = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
    require(
        bytes[end..].iter().all(|b| *b == 0),
        "nonzero tar text suffix",
    )?;
    Ok(std::str::from_utf8(&bytes[..end])?)
}

fn octal(bytes: &[u8]) -> Result<u64> {
    require(
        !bytes.is_empty() && bytes[0] & 128 == 0,
        "tar base256 refused",
    )?;
    let text = std::str::from_utf8(bytes)?.trim_matches([' ', '\0']);
    require(
        !text.is_empty() && text.bytes().all(|b| (b'0'..=b'7').contains(&b)),
        "tar octal",
    )?;
    Ok(u64::from_str_radix(text, 8)?)
}

struct HashedBody<R> {
    inner: R,
    hash: Sha256,
    bytes: u64,
}
impl<R: Read> Read for HashedBody<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(out)?;
        self.hash.update(&out[..n]);
        self.bytes += n as u64;
        Ok(n)
    }
}

// Deliberately limited to ordinary USTAR/GNU regular and directory headers.
// PAX, long-name, sparse, links, devices, base256 and concatenated tar are refused.
fn scan_tar<R: Read>(
    input: &mut R,
    manifest: &BTreeMap<String, String>,
    mut consume: impl FnMut(&str, &mut dyn Read) -> Result<()>,
) -> Result<()> {
    let mut raw = input.take(MAX_RAW + 1);
    let mut headers = BTreeMap::<String, bool>::new();
    let mut files = BTreeSet::new();
    loop {
        let mut h = [0u8; 512];
        raw.read_exact(&mut h)?;
        if h == [0; 512] {
            let mut second = [0u8; 512];
            raw.read_exact(&mut second)?;
            require(second == [0; 512], "tar end blocks")?;
            let mut tail = [0u8; 65536];
            loop {
                let n = raw.read(&mut tail)?;
                if n == 0 {
                    break;
                }
                require(tail[..n].iter().all(|b| *b == 0), "nonzero tar tail")?;
            }
            let used = MAX_RAW + 1 - raw.limit();
            require(
                used <= MAX_RAW && used % 512 == 0,
                "tar expanded cap/alignment",
            )?;
            break;
        }
        require(headers.len() < MAX_HEADERS, "tar header count")?;
        let checksum = octal(&h[148..156])?;
        let sum: u64 = h
            .iter()
            .enumerate()
            .map(|(i, b)| {
                if (148..156).contains(&i) {
                    32
                } else {
                    u64::from(*b)
                }
            })
            .sum();
        require(sum == checksum, "tar checksum")?;
        let ustar = &h[257..265] == b"ustar\000";
        let gnu = &h[257..265] == b"ustar  \0";
        require(ustar || gnu, "tar format")?;
        require(h[157..257].iter().all(|b| *b == 0), "tar link name")?;
        require(
            matches!(h[156], 0 | b'0' | b'5'),
            "tar extended/link/sparse/type",
        )?;
        // GNU uses this region for sparse/offset metadata, never a USTAR prefix.
        if gnu {
            require(h[345..512].iter().all(|b| *b == 0), "GNU extension refused")?;
        } else {
            require(h[500..512].iter().all(|b| *b == 0), "USTAR reserved bytes")?;
        }
        let base = text_field(&h[..100])?;
        let prefix = if ustar { text_field(&h[345..500])? } else { "" };
        let name = if prefix.is_empty() {
            base.to_owned()
        } else {
            format!("{prefix}/{base}")
        };
        let directory = h[156] == b'5';
        let name = if directory {
            name.strip_suffix('/').unwrap_or(&name)
        } else {
            &name
        };
        let relative = if name == "." {
            ""
        } else {
            name.strip_prefix("./").ok_or("tar root")?
        };
        require(
            (relative.is_empty() && directory) || safe_name(relative),
            "tar path",
        )?;
        require(
            headers.insert(relative.to_owned(), directory).is_none(),
            "duplicate tar header",
        )?;
        let size = octal(&h[124..136])?;
        require(
            size <= MAX_MEMBER && (!directory || size == 0),
            "tar member bounds",
        )?;
        if directory {
            continue;
        }
        let expected = manifest.get(relative).ok_or("unlisted tar file")?;
        let mut body = HashedBody {
            inner: (&mut raw).take(size),
            hash: Sha256::new(),
            bytes: 0,
        };
        consume(relative, &mut body)?;
        io::copy(&mut body, &mut io::sink())?;
        require(
            body.bytes == size && format!("{:x}", body.hash.finalize()) == *expected,
            "tar member digest/length",
        )?;
        files.insert(relative.to_owned());
        let padding = (512 - size % 512) % 512;
        let mut pad = [0u8; 512];
        raw.read_exact(&mut pad[..padding as usize])?;
        require(
            pad[..padding as usize].iter().all(|b| *b == 0),
            "tar padding",
        )?;
    }
    require(
        files.len() == manifest.len() && files.iter().eq(manifest.keys()),
        "tar inventory",
    )?;
    for name in headers.keys() {
        let mut ancestor = name.as_str();
        while let Some((parent, _)) = ancestor.rsplit_once('/') {
            require(
                headers.get(parent).copied().unwrap_or(true),
                "tar file ancestor",
            )?;
            ancestor = parent;
        }
    }
    Ok(())
}

fn selected(arm: &str) -> Result<BTreeSet<String>> {
    let result = match arm {
        "historical" => "prepared-parent_query_baseline-result.jsonl",
        "local" => "baseline-result.jsonl",
        _ => return Err("unknown replay arm".into()),
    };
    Ok([
        result,
        "evidence-chain/configs/baseline.json",
        "prepared-parent_cohort_complete.json",
        "prepared-parent_cohort_truth.u64",
        "prepared-parent_derived_derivation.json",
        "prepared-parent_generation_manifest.json",
        "prepared-parent_publication-receipt.json",
        "evidence-chain/terminal.json",
        "chain-outer/outer-closure.json",
        "chain-outer/manager.show",
        "chain-outer/drain.proof.json",
        "chain-actual.exit",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect())
}

fn gzip_archive(
    archive: &mut Input,
    manifest: &BTreeMap<String, String>,
    consume: impl FnMut(&str, &mut dyn Read) -> Result<()>,
) -> Result<()> {
    archive.file.seek(SeekFrom::Start(0))?;
    let buffered = BufReader::with_capacity(65536, &mut archive.file);
    // bufread decoder leaves bytes following the first gzip member untouched.
    let mut decoder = flate2::bufread::GzDecoder::new(buffered);
    scan_tar(&mut decoder, manifest, consume)?;
    let mut rest = decoder.into_inner();
    require(
        rest.fill_buf()?.is_empty(),
        "gzip trailing/concatenated member",
    )?;
    drop(rest);
    archive.check()?;
    // Authenticate the compressed stream again after decode; timestamps alone
    // are not a substitute for content authentication.
    archive.authenticate(false)?;
    Ok(())
}

fn extract(authority_path: &str, authority_sha: &str, destination: &Path) -> Result<Value> {
    require(sha_valid(authority_sha), "authority SHA")?;
    let m = std::fs::symlink_metadata(authority_path)?;
    let mut source = Input::open(
        Pin {
            path: authority_path.to_owned(),
            bytes: m.len(),
            sha256: authority_sha.to_owned(),
        },
        65536,
    )?;
    let body = source.authenticate(true)?;
    let UniqueJson(value) = serde_json::from_slice(&body)?;
    let authority: Authority = serde_json::from_value(value)?;
    require(
        authority.schema == "borsuk-closed-native-replay-extraction-authority-v1",
        "authority schema",
    )?;
    let chosen = selected(&authority.arm)?;
    let mut manifest_input = Input::open(authority.manifest.clone(), 2 * 1024 * 1024)?;
    let manifest = parse_manifest(&manifest_input.authenticate(true)?)?;
    require(
        chosen.iter().all(|n| manifest.contains_key(n)),
        "missing selected member",
    )?;
    let mut archive = Input::open(authority.archive.clone(), MAX_RAW)?;
    let identities = [&source, &manifest_input, &archive].map(|i| (i.before.0, i.before.1));
    require(
        identities.into_iter().collect::<BTreeSet<_>>().len() == 3,
        "aliased inputs",
    )?;
    archive.authenticate(false)?;
    source.check()?;
    manifest_input.check()?;
    let mut out = Destination::new(destination)?;
    gzip_archive(&mut archive, &manifest, |name, body| {
        if chosen.contains(name) {
            out.write(name, body)?;
        }
        Ok(())
    })?;
    require(out.files.keys().eq(chosen.iter()), "selected output roster")?;
    out.finish()?;
    source.authenticate(false)?;
    manifest_input.authenticate(false)?;
    archive.check()?;
    out.check_files()?;
    let pins: BTreeMap<_, _> = out
        .files
        .iter()
        .map(|(n, f)| (n.clone(), f.pin.clone()))
        .collect();
    Ok(serde_json::json!({
        "schema":"borsuk-closed-native-replay-extraction-v1",
        "status":"ARCHIVE_INVENTORY_EXTRACTED", "arm":authority.arm,
        "authority_sha256":authority_sha, "archive":authority.archive,
        "manifest":authority.manifest, "inventory_files":manifest.len(),
        "destination":destination, "extracted":pins,
        "claims_native_closure":false, "claims_quality":false, "claims_performance":false
    }))
}

fn main() {
    let result = (|| -> Result<()> {
        let args: Vec<_> = std::env::args().collect();
        require(args.len() == 4, "usage: AUTHORITY AUTHORITY_SHA NEW_DEST")?;
        let value = extract(&args[1], &args[2], Path::new(&args[3]))?;
        let stdout = io::stdout();
        let mut lock = stdout.lock();
        serde_json::to_writer(&mut lock, &value)?;
        lock.write_all(b"\n")?;
        lock.flush()?;
        Ok(())
    })();
    if let Err(error) = result {
        eprintln!("INVALID archive inventory extraction: {error}");
        std::process::exit(98);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Independent POSIX tar fixture; no production header serializer is reused.
    fn member(name: &str, body: &[u8], kind: u8) -> Vec<u8> {
        let mut h = [0u8; 512];
        h[..name.len()].copy_from_slice(name.as_bytes());
        h[100..108].copy_from_slice(b"0000600\0");
        h[108..116].copy_from_slice(b"0000000\0");
        h[116..124].copy_from_slice(b"0000000\0");
        let size = format!("{:011o}\0", body.len());
        h[124..136].copy_from_slice(size.as_bytes());
        h[136..148].copy_from_slice(b"00000000000\0");
        h[148..156].fill(b' ');
        h[156] = kind;
        h[257..265].copy_from_slice(b"ustar\000");
        let sum: u32 = h.iter().map(|b| u32::from(*b)).sum();
        h[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        let mut out = h.to_vec();
        out.extend_from_slice(body);
        out.resize(out.len().div_ceil(512) * 512, 0);
        out
    }

    fn archive(entries: &[(&str, &[u8], u8)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (name, body, kind) in entries {
            out.extend(member(name, body, *kind));
        }
        out.resize(out.len() + 1024, 0);
        out
    }

    #[test]
    fn archive_authenticates_selected_and_unselected_members() {
        let raw = archive(&[("./a", b"selected", b'0'), ("./b", b"unselected", b'0')]);
        let manifest = BTreeMap::from([
            ("a".to_owned(), digest(b"selected")),
            ("b".to_owned(), digest(b"unselected")),
        ]);
        let mut seen = Vec::new();
        scan_tar(&mut &raw[..], &manifest, |name, body| {
            let mut bytes = Vec::new();
            body.read_to_end(&mut bytes)?;
            seen.push((name.to_owned(), bytes));
            Ok(())
        })
        .unwrap();
        assert_eq!(
            seen,
            vec![
                ("a".into(), b"selected".to_vec()),
                ("b".into(), b"unselected".to_vec())
            ]
        );
        let mut corrupt = raw.clone();
        corrupt[1536] ^= 1;
        assert!(scan_tar(&mut &corrupt[..], &manifest, |_, _| Ok(())).is_err());
        for n in [0, 511, 512, raw.len() - 1] {
            assert!(scan_tar(&mut &raw[..n], &manifest, |_, _| Ok(())).is_err());
        }
    }

    #[test]
    fn archive_rejects_duplicates_links_paths_and_hidden_tail() {
        let m = BTreeMap::from([("a".to_owned(), digest(b"x"))]);
        for name in ["/a", "./../a", "./a/../b", "./a//b"] {
            let raw = archive(&[(name, b"x", b'0')]);
            assert!(scan_tar(&mut &raw[..], &m, |_, _| Ok(())).is_err());
        }
        for kind in [b'1', b'2', b'x', b'g', b'L', b'S'] {
            let raw = archive(&[("./a", b"x", kind)]);
            assert!(scan_tar(&mut &raw[..], &m, |_, _| Ok(())).is_err());
        }
        let duplicate = archive(&[("./a", b"x", b'0'), ("./a", b"x", b'0')]);
        assert!(scan_tar(&mut &duplicate[..], &m, |_, _| Ok(())).is_err());
        let mut tail = archive(&[("./a", b"x", b'0')]);
        tail.extend(member("./hidden", b"x", b'0'));
        assert!(scan_tar(&mut &tail[..], &m, |_, _| Ok(())).is_err());
        let mut header = archive(&[("./a", b"x", b'0')]);
        header[10] ^= 1;
        assert!(scan_tar(&mut &header[..], &m, |_, _| Ok(())).is_err());
    }

    #[test]
    fn manifest_refuses_unsafe_ambiguous_and_unsorted_names() {
        let sha = digest(b"x");
        assert_eq!(
            parse_manifest(format!("{sha}  ./a\n").as_bytes())
                .unwrap()
                .len(),
            1
        );
        for names in [
            vec!["b", "a"],
            vec!["a", "a"],
            vec!["a", "a/b"],
            vec!["../a"],
            vec!["a//b"],
        ] {
            let text: String = names.iter().map(|n| format!("{sha}  ./{n}\n")).collect();
            assert!(parse_manifest(text.as_bytes()).is_err());
        }
        assert!(parse_manifest(format!("{sha}  ./a").as_bytes()).is_err());
    }

    #[test]
    fn archive_member_bounds_and_authority_types_are_strict() {
        let mut raw = archive(&[("./a", b"x", b'0')]);
        raw[124..136].copy_from_slice(format!("{:011o}\0", MAX_MEMBER + 1).as_bytes());
        raw[148..156].fill(b' ');
        let sum: u32 = raw[..512].iter().map(|b| u32::from(*b)).sum();
        raw[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        let manifest = BTreeMap::from([("a".to_owned(), digest(b"x"))]);
        assert!(scan_tar(&mut &raw[..], &manifest, |_, _| Ok(())).is_err());
        let valid = serde_json::json!({
            "schema":"borsuk-closed-native-replay-extraction-authority-v1", "arm":"local",
            "archive":{"path":"/a", "bytes":1, "sha256":"0".repeat(64)},
            "manifest":{"path":"/b", "bytes":1, "sha256":"0".repeat(64)}
        });
        for bad in [
            Value::Bool(true),
            Value::from(-1),
            Value::from(1.5),
            Value::String("1".into()),
            Value::Null,
        ] {
            let mut value = valid.clone();
            value["archive"]["bytes"] = bad;
            assert!(serde_json::from_value::<Authority>(value).is_err());
        }
        let mut unknown = valid;
        unknown["archive"]["extra"] = Value::Bool(true);
        assert!(serde_json::from_value::<Authority>(unknown).is_err());
    }

    fn pin_file(path: &Path, body: &[u8]) -> Pin {
        std::fs::write(path, body).unwrap();
        Pin {
            path: path.to_str().unwrap().to_owned(),
            bytes: body.len() as u64,
            sha256: digest(body),
        }
    }
    fn gzip(raw: &[u8]) -> Vec<u8> {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(raw).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn gzip_refuses_concatenation_trailing_crc_and_truncation() {
        let t = tempfile::tempdir().unwrap();
        let dir = std::fs::canonicalize(t.path()).unwrap();
        let raw = archive(&[("./a", b"x", b'0')]);
        let good = gzip(&raw);
        let manifest = BTreeMap::from([("a".to_owned(), digest(b"x"))]);
        let pin = pin_file(&dir.join("good"), &good);
        let mut input = Input::open(pin, MAX_RAW).unwrap();
        gzip_archive(&mut input, &manifest, |_, _| Ok(())).unwrap();
        let mut joined = good.clone();
        joined.extend_from_slice(&good);
        let mut trailing = good.clone();
        trailing.push(0);
        let mut crc = good.clone();
        let last = crc.len() - 8;
        crc[last] ^= 1;
        for (i, bad) in [joined, trailing, crc, good[..good.len() - 1].to_vec()]
            .into_iter()
            .enumerate()
        {
            let pin = pin_file(&dir.join(format!("bad{i}")), &bad);
            let mut input = Input::open(pin, MAX_RAW).unwrap();
            assert!(gzip_archive(&mut input, &manifest, |_, _| Ok(())).is_err());
        }
    }

    #[test]
    fn extraction_preserves_exact_roles_and_authenticates_before_creation() {
        let t = tempfile::tempdir().unwrap();
        let dir = std::fs::canonicalize(t.path()).unwrap();
        let names = selected("local").unwrap();
        let mut raw = Vec::new();
        let mut lines = String::new();
        for name in &names {
            raw.extend(member(&format!("./{name}"), b"fixture", b'0'));
            lines.push_str(&format!("{}  ./{name}\n", digest(b"fixture")));
        }
        raw.resize(raw.len() + 1024, 0);
        let ap = pin_file(&dir.join("archive.gz"), &gzip(&raw));
        let mp = pin_file(&dir.join("manifest"), lines.as_bytes());
        let auth = serde_json::json!({"schema":"borsuk-closed-native-replay-extraction-authority-v1", "arm":"local", "archive":ap, "manifest":mp});
        let body = serde_json::to_vec(&auth).unwrap();
        let authority = pin_file(&dir.join("authority.json"), &body);
        let out = dir.join("extracted");
        let receipt = extract(&authority.path, &authority.sha256, &out).unwrap();
        assert_eq!(receipt["status"], "ARCHIVE_INVENTORY_EXTRACTED");
        assert_eq!(receipt["extracted"].as_object().unwrap().len(), 12);
        for name in names {
            assert_eq!(std::fs::read(out.join(name)).unwrap(), b"fixture");
        }
        assert!(extract(&authority.path, &authority.sha256, &out).is_err());
        let mut wrong = auth.clone();
        wrong["archive"]["sha256"] = Value::String("0".repeat(64));
        let wrong_pin = pin_file(
            &dir.join("wrong.json"),
            &serde_json::to_vec(&wrong).unwrap(),
        );
        let refused = dir.join("refused");
        assert!(extract(&wrong_pin.path, &wrong_pin.sha256, &refused).is_err());
        assert!(!refused.exists());
        let duplicate = format!("{{\"schema\":\"x\",\"schema\":\"y\"}}");
        let dupe = pin_file(&dir.join("duplicate.json"), duplicate.as_bytes());
        assert!(extract(&dupe.path, &dupe.sha256, &refused).is_err());
        assert!(!refused.exists());
    }

    #[test]
    fn descriptors_and_publication_refuse_replacement_links_and_failed_sync() {
        let t = tempfile::tempdir().unwrap();
        let dir = std::fs::canonicalize(t.path()).unwrap();
        let p = pin_file(&dir.join("input"), b"original");
        let input = Input::open(p.clone(), 65536).unwrap();
        std::fs::rename(&p.path, dir.join("old")).unwrap();
        std::fs::write(&p.path, b"original").unwrap();
        assert!(input.check().is_err());
        std::fs::hard_link(&p.path, dir.join("hard")).unwrap();
        assert!(Input::open(p.clone(), 65536).is_err());
        std::os::unix::fs::symlink(dir.join("old"), dir.join("link")).unwrap();
        let mut link = p.clone();
        link.path = dir.join("link").to_str().unwrap().to_owned();
        assert!(Input::open(link, 65536).is_err());
        let mut out = Destination::new(&dir.join("output")).unwrap();
        out.write("a", &mut &b"output"[..]).unwrap();
        std::fs::rename(out.path.join("a"), out.path.join("old-a")).unwrap();
        std::fs::write(out.path.join("a"), b"output").unwrap();
        assert!(out.finish().is_err());
        let mut sync = Destination::new(&dir.join("sync-fail")).unwrap();
        sync.write("a", &mut &b"output"[..]).unwrap();
        // Same real directory identity but an O_PATH descriptor cannot fsync.
        let parent = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_PATH | libc::O_DIRECTORY)
            .open(&dir)
            .unwrap();
        sync.parent = parent;
        assert!(sync.finish().is_err());
    }
}
struct UniqueJson(Value);
impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueJson;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("JSON with unique object keys")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut map = serde_json::Map::new();
                while let Some((key, UniqueJson(value))) = a.next_entry::<String, UniqueJson>()? {
                    if map.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate JSON key"));
                    }
                }
                Ok(UniqueJson(Value::Object(map)))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(UniqueJson(v)) = a.next_element()? {
                    values.push(v);
                }
                Ok(UniqueJson(Value::Array(values)))
            }
            fn visit_bool<E: serde::de::Error>(
                self,
                v: bool,
            ) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(v.into()))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(v.into()))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(v.into()))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> std::result::Result<Self::Value, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| UniqueJson(Value::Number(n)))
                    .ok_or_else(|| E::custom("finite number"))
            }
            fn visit_str<E: serde::de::Error>(
                self,
                v: &str,
            ) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(v.into()))
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(Value::Null))
            }
        }
        d.deserialize_any(Visitor)
    }
}
