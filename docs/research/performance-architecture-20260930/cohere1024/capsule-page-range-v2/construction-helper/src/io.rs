use crate::{Result, ensure};
use rustix::fs::{Mode, OFlags, RenameFlags, mkdirat, openat, renameat_with};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, Metadata, Permissions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Component, Path},
};

pub const BUFFER: usize = 65536;
// Covers bounded JSON parse trees, descriptors, hashes, paths and streaming scratch.
// This is a payload-capacity model; the parent supplies the process/RSS limit.
pub const CONTROL: usize = 8 * 1024 * 1024;

pub fn admit(cap: usize, terms: &[usize]) -> Result<usize> {
    let sum = terms
        .iter()
        .try_fold(0usize, |a, b| a.checked_add(*b))
        .ok_or("memory overflow")?;
    ensure(sum <= cap, "construction payload cap")?;
    Ok(sum)
}

pub fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(|| "size overflow".into())
}
pub fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or_else(|| "size overflow".into())
}

pub fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn valid_sha(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub fn digest(s: &str) -> Result<[u8; 32]> {
    ensure(valid_sha(s), "lowercase SHA256 required")?;
    let mut out = [0; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16)?;
    }
    Ok(out)
}

// Open every component relative to a retained directory FD: no symlink ancestor
// can redirect an input read or a create-only write.
pub fn directory(path: &Path) -> Result<File> {
    ensure(path.is_absolute(), "absolute path required")?;
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let mut dir = File::from(openat(rustix::fs::CWD, "/", flags, Mode::empty())?);
    for c in path.components() {
        match c {
            Component::RootDir => {}
            Component::Normal(name) => dir = File::from(openat(&dir, name, flags, Mode::empty())?),
            _ => return Err("noncanonical path component".into()),
        }
    }
    Ok(dir)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stamp {
    dev: u64,
    ino: u64,
    len: u64,
    mode: u32,
    mtime: i64,
    mtime_ns: i64,
    ctime: i64,
    ctime_ns: i64,
}
impl Stamp {
    fn of(m: &Metadata) -> Self {
        Self {
            dev: m.dev(),
            ino: m.ino(),
            len: m.len(),
            mode: m.mode(),
            mtime: m.mtime(),
            mtime_ns: m.mtime_nsec(),
            ctime: m.ctime(),
            ctime_ns: m.ctime_nsec(),
        }
    }
}
pub struct Input {
    pub file: File,
    stamp: Stamp,
}
impl Input {
    pub fn open(path: &Path, bytes: u64) -> Result<Self> {
        let parent = directory(path.parent().ok_or("file parent")?)?;
        let fd = openat(
            &parent,
            path.file_name().ok_or("file name")?,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let file = File::from(fd);
        let m = file.metadata()?;
        ensure(
            m.is_file() && m.len() == bytes && bytes > 0,
            "regular file/exact length",
        )?;
        Ok(Self {
            file,
            stamp: Stamp::of(&m),
        })
    }
    pub fn unchanged(&self, path: &Path) -> Result<()> {
        ensure(
            Stamp::of(&self.file.metadata()?) == self.stamp,
            "input changed while open",
        )?;
        let now = Self::open(path, self.stamp.len)?;
        ensure(now.stamp == self.stamp, "input pathname changed")
    }
    pub fn end(&mut self, path: &Path) -> Result<()> {
        ensure(self.file.read(&mut [0])? == 0, "input grew/trailing bytes")?;
        self.unchanged(path)
    }
}

pub fn length(path: &Path, max: usize) -> Result<usize> {
    let parent = directory(path.parent().ok_or("file parent")?)?;
    let file = File::from(openat(
        &parent,
        path.file_name().ok_or("file name")?,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    let m = file.metadata()?;
    ensure(
        m.is_file() && m.len() > 0 && m.len() <= max as u64,
        "bounded regular file",
    )?;
    Ok(usize::try_from(m.len())?)
}

pub fn vector(bytes: usize) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.try_reserve_exact(bytes)?;
    ensure(out.capacity() == bytes, "unexpected allocation capacity")?;
    out.resize(bytes, 0);
    Ok(out)
}
pub fn read(path: &Path, bytes: usize, expected: &str) -> Result<Vec<u8>> {
    ensure(valid_sha(expected), "input SHA256")?;
    let mut input = Input::open(path, bytes as u64)?;
    let mut out = vector(bytes)?;
    input.file.read_exact(&mut out)?;
    input.end(path)?;
    ensure(sha(&out) == expected, "input SHA256 mismatch")?;
    Ok(out)
}
pub fn create(path: &Path) -> Result<File> {
    let parent = directory(path.parent().ok_or("output parent")?)?;
    Ok(File::from(openat(
        &parent,
        path.file_name().ok_or("output name")?,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?))
}
pub fn new_directory(path: &Path) -> Result<()> {
    let parent = directory(path.parent().ok_or("output parent")?)?;
    mkdirat(
        &parent,
        path.file_name().ok_or("output name")?,
        Mode::RUSR | Mode::WUSR | Mode::XUSR,
    )?;
    directory(path)?.sync_all()?;
    parent.sync_all()?;
    Ok(())
}
pub fn absent(path: &Path) -> Result<()> {
    directory(path.parent().ok_or("output parent")?)?;
    match path.symlink_metadata() {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => Err("output exists or is inaccessible".into()),
    }
}
pub fn stream(path: &Path, bytes: u64, expected: &str, output: Option<&Path>) -> Result<()> {
    ensure(valid_sha(expected), "stream SHA256")?;
    let mut input = Input::open(path, bytes)?;
    let mut out = output.map(create).transpose()?;
    let mut remaining = bytes;
    let mut h = Sha256::new();
    let mut buffer = [0u8; BUFFER];
    while remaining > 0 {
        let n = usize::try_from(remaining.min(BUFFER as u64))?;
        input.file.read_exact(&mut buffer[..n])?;
        h.update(&buffer[..n]);
        if let Some(out) = &mut out {
            out.write_all(&buffer[..n])?;
        }
        remaining -= n as u64;
    }
    input.end(path)?;
    ensure(
        format!("{:x}", h.finalize()) == expected,
        "stream SHA256 mismatch",
    )?;
    if let Some(out) = out {
        out.set_permissions(Permissions::from_mode(0o400))?;
        out.sync_all()?;
    }
    Ok(())
}

pub fn seal(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = directory(path.parent().ok_or("seal parent")?)?;
    let name = path.file_name().ok_or("seal name")?;
    let mut pending_name = name.to_os_string();
    pending_name.push(".pending");
    let pending = path.with_file_name(&pending_name);
    let mut f = create(&pending)?;
    f.write_all(bytes)?;
    f.set_permissions(Permissions::from_mode(0o400))?;
    f.sync_all()?;
    renameat_with(
        &parent,
        &pending_name,
        &parent,
        name,
        RenameFlags::NOREPLACE,
    )?;
    parent.sync_all()?;
    directory(
        path.parent()
            .and_then(Path::parent)
            .ok_or("seal grandparent")?,
    )?
    .sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hashes_and_overflow_are_strict() {
        assert!(valid_sha(&sha(b"")));
        assert!(!valid_sha(&"A".repeat(64)));
        assert!(admit(10, &[usize::MAX, 1]).is_err());
        assert!(mul(usize::MAX, 2).is_err());
        assert!(directory(Path::new("relative")).is_err());
    }
}
