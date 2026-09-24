//! Whole-artifact authenticated mapping from relaid SQ8 rows to old PQ rows.

use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    os::unix::fs::FileExt,
    path::Path,
};

use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;

use crate::native_source_tier::decoded_sha256;

const MAGIC: [u8; 16] = *b"BORSUK-ROWMAP-V1";
const HEADER_BYTES: u64 = 160;
const ENTRY_BYTES: u64 = 4;
const BLOCK_BYTES: usize = 64 * 1024;

/// Identity of both physical orders and their common source generation.
#[derive(Clone, Copy)]
pub struct RowMapBinding<'a> {
    /// Immutable generation identifier shared by all mapped artifacts.
    pub generation: u64,
    /// Number of rows in both physical orders.
    pub rows: u64,
    /// Digest of the common exact source artifact.
    pub source_sha256: &'a str,
    /// Digest of the authenticated original router manifest.
    pub router_manifest_sha256: &'a str,
    /// Digest of the SQ8 object named by the original router.
    pub old_sq8_sha256: &'a str,
    /// Digest of the relaid SQ8 object used by the page authority.
    pub new_sq8_sha256: &'a str,
}

/// Failure to create or authenticate a physical row map.
#[derive(Debug, Error)]
pub enum RowMapError {
    /// File read, write or durability operation failed.
    #[error("row-map I/O failed: {0}")]
    Io(#[from] io::Error),
    /// The supplied shape, identity or permutation violates the format.
    #[error("row-map contract differs: {0}")]
    Invalid(&'static str),
    /// The complete file differs from the trusted artifact digest.
    #[error("row-map whole-artifact SHA-256 differs")]
    HashMismatch,
    /// The file was published but syncing its parent failed; reopen by digest.
    #[error("row-map {sha256} was persisted but parent sync failed: {source}")]
    PersistedButUnsynced {
        /// Digest of the published artifact for recovery.
        sha256: String,
        /// Directory sync error.
        source: io::Error,
    },
}

/// Both directions are resident; payload is exactly eight bytes per row.
#[derive(Debug)]
pub struct PhysicalRowPermutation {
    artifact_sha256: [u8; 32],
    generation: u64,
    source_sha256: [u8; 32],
    router_manifest_sha256: [u8; 32],
    old_sq8_sha256: [u8; 32],
    new_sq8_sha256: [u8; 32],
    new_to_old: Vec<u32>,
    old_to_new: Vec<u32>,
}

fn expected_len(rows: u64) -> Result<u64, RowMapError> {
    if rows == 0 || rows > u64::from(u32::MAX) {
        return Err(RowMapError::Invalid("row count exceeds u32 format"));
    }
    rows.checked_mul(ENTRY_BYTES)
        .and_then(|body| body.checked_add(HEADER_BYTES))
        .ok_or(RowMapError::Invalid("row-map byte length overflow"))
}

fn decode_hash(value: &str) -> Result<[u8; 32], RowMapError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RowMapError::Invalid("SHA-256 identity"));
    }
    decoded_sha256(value).map_err(|_| RowMapError::Invalid("SHA-256 identity"))
}

fn header(binding: RowMapBinding<'_>) -> Result<[u8; 160], RowMapError> {
    expected_len(binding.rows)?;
    if binding.generation == 0 {
        return Err(RowMapError::Invalid("zero generation"));
    }
    let mut bytes = [0_u8; 160];
    bytes[..16].copy_from_slice(&MAGIC);
    bytes[16..24].copy_from_slice(&binding.generation.to_le_bytes());
    bytes[24..32].copy_from_slice(&binding.rows.to_le_bytes());
    for (slot, value) in [
        binding.source_sha256,
        binding.router_manifest_sha256,
        binding.old_sq8_sha256,
        binding.new_sq8_sha256,
    ]
    .into_iter()
    .enumerate()
    {
        bytes[32 + slot * 32..64 + slot * 32].copy_from_slice(&decode_hash(value)?);
    }
    Ok(bytes)
}

/// Atomically write one non-replaceable v1 artifact in new SQ8 row order.
/// Production callers must use `write_verified_row_permutation` instead.
pub(crate) fn write_row_permutation(
    path: &Path,
    binding: RowMapBinding<'_>,
    new_to_old: &[u32],
) -> Result<String, RowMapError> {
    let head = header(binding)?;
    let count = usize::try_from(binding.rows)
        .map_err(|_| RowMapError::Invalid("row count exceeds address space"))?;
    if new_to_old.len() != count {
        return Err(RowMapError::Invalid("row-map payload length"));
    }
    let mut seen = Vec::new();
    seen.try_reserve_exact(count)
        .map_err(|_| RowMapError::Invalid("row-map validation allocation"))?;
    seen.resize(count, false);
    for &old in new_to_old {
        let old = old as usize;
        if old >= count || seen[old] {
            return Err(RowMapError::Invalid("row-map is not a permutation"));
        }
        seen[old] = true;
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temporary = NamedTempFile::new_in(parent)?;
    let mut digest = Sha256::new();
    {
        let mut output = BufWriter::new(temporary.as_file_mut());
        output.write_all(&head)?;
        digest.update(head);
        let mut block = [0_u8; BLOCK_BYTES];
        for rows in new_to_old.chunks(BLOCK_BYTES / 4) {
            let bytes = &mut block[..rows.len() * 4];
            for (slot, &old) in bytes.chunks_exact_mut(4).zip(rows) {
                slot.copy_from_slice(&old.to_le_bytes());
            }
            output.write_all(bytes)?;
            digest.update(bytes);
        }
        output.flush()?;
    }
    temporary.as_file().sync_all()?;
    let sha256 = format!("{:x}", digest.finalize());
    temporary
        .persist_noclobber(path)
        .map_err(|error| RowMapError::Io(error.error))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| RowMapError::PersistedButUnsynced {
            sha256: sha256.clone(),
            source,
        })?;
    Ok(sha256)
}

fn verify_sq8_file(
    file: &File,
    expected_sha256: &str,
    expected_bytes: u64,
) -> Result<(), RowMapError> {
    let expected = decode_hash(expected_sha256)?;
    if file.metadata()?.len() != expected_bytes {
        return Err(RowMapError::Invalid("SQ8 object length"));
    }
    let mut digest = Sha256::new();
    let mut block = [0_u8; BLOCK_BYTES];
    let mut offset = 0_u64;
    while offset < expected_bytes {
        let want = (expected_bytes - offset).min(BLOCK_BYTES as u64) as usize;
        let mut read = 0;
        while read < want {
            let got = file.read_at(&mut block[read..want], offset + read as u64)?;
            if got == 0 {
                return Err(RowMapError::Invalid("SQ8 object ended early"));
            }
            read += got;
        }
        digest.update(&block[..want]);
        offset += want as u64;
    }
    if digest.finalize()[..] != expected {
        return Err(RowMapError::HashMismatch);
    }
    Ok(())
}

/// Check both authenticated SQ8 objects and every mapped record before
/// publishing a row map. The object files must remain immutable during build.
/// Scratch memory is O(rows + row width) for bijection checking and row
/// comparison; the old object is read by mapped offset.
pub fn write_verified_row_permutation(
    path: &Path,
    old_sq8_path: &Path,
    new_sq8_path: &Path,
    binding: RowMapBinding<'_>,
    dimensions: usize,
    new_to_old: &[u32],
) -> Result<String, RowMapError> {
    let rows = usize::try_from(binding.rows)
        .map_err(|_| RowMapError::Invalid("row count exceeds address space"))?;
    if new_to_old.len() != rows || dimensions == 0 {
        return Err(RowMapError::Invalid("SQ8 map geometry"));
    }
    let row_bytes = dimensions
        .checked_add(12)
        .ok_or(RowMapError::Invalid("SQ8 row width"))?;
    let total_bytes = binding
        .rows
        .checked_mul(row_bytes as u64)
        .ok_or(RowMapError::Invalid("SQ8 object length overflow"))?;
    let mut seen = Vec::new();
    seen.try_reserve_exact(rows)
        .map_err(|_| RowMapError::Invalid("row-map validation allocation"))?;
    seen.resize(rows, false);
    for &old in new_to_old {
        let index = old as usize;
        if index >= rows || seen[index] {
            return Err(RowMapError::Invalid("row-map is not a permutation"));
        }
        seen[index] = true;
    }
    drop(seen);
    let old_file = File::open(old_sq8_path)?;
    verify_sq8_file(&old_file, binding.old_sq8_sha256, total_bytes)?;
    let new_file = File::open(new_sq8_path)?;
    if new_file.metadata()?.len() != total_bytes {
        return Err(RowMapError::Invalid("SQ8 object length"));
    }
    let mut new_input = BufReader::new(new_file);
    let expected_new_digest = decode_hash(binding.new_sq8_sha256)?;
    let mut new_digest = Sha256::new();
    let mut old_row = Vec::new();
    old_row
        .try_reserve_exact(row_bytes)
        .map_err(|_| RowMapError::Invalid("SQ8 row buffer allocation"))?;
    old_row.resize(row_bytes, 0);
    let rows_per_block = (BLOCK_BYTES / row_bytes).max(1);
    let block_bytes = rows_per_block
        .checked_mul(row_bytes)
        .ok_or(RowMapError::Invalid("SQ8 row block overflow"))?;
    let mut new_block = Vec::new();
    new_block
        .try_reserve_exact(block_bytes)
        .map_err(|_| RowMapError::Invalid("SQ8 row buffer allocation"))?;
    new_block.resize(block_bytes, 0);
    for mapped_rows in new_to_old.chunks(rows_per_block) {
        let bytes = &mut new_block[..mapped_rows.len() * row_bytes];
        new_input.read_exact(bytes)?;
        new_digest.update(&*bytes);
        for (&old, new_row) in mapped_rows.iter().zip(bytes.chunks_exact(row_bytes)) {
            let offset = u64::from(old) * row_bytes as u64;
            let mut read = 0;
            while read < row_bytes {
                let got = old_file.read_at(&mut old_row[read..], offset + read as u64)?;
                if got == 0 {
                    return Err(RowMapError::Invalid("old SQ8 row ended early"));
                }
                read += got;
            }
            if old_row != new_row {
                return Err(RowMapError::Invalid("row-map direction or SQ8 row body"));
            }
        }
    }
    let mut extra = [0_u8; 1];
    if new_input.read(&mut extra)? != 0 {
        return Err(RowMapError::Invalid("new SQ8 trailing bytes"));
    }
    if new_digest.finalize()[..] != expected_new_digest {
        return Err(RowMapError::HashMismatch);
    }
    verify_sq8_file(&old_file, binding.old_sq8_sha256, total_bytes)?;
    write_row_permutation(path, binding, new_to_old)
}

impl PhysicalRowPermutation {
    /// Verify the complete artifact and bijection before exposing either map.
    pub fn open_authenticated(
        path: &Path,
        expected_artifact_sha256: &str,
        binding: RowMapBinding<'_>,
    ) -> Result<Self, RowMapError> {
        let expected_digest = decode_hash(expected_artifact_sha256)?;
        let expected_head = header(binding)?;
        let count = usize::try_from(binding.rows)
            .map_err(|_| RowMapError::Invalid("row count exceeds address space"))?;
        let file = File::open(path)?;
        if file.metadata()?.len() != expected_len(binding.rows)? {
            return Err(RowMapError::Invalid("row-map file length"));
        }
        let mut input = BufReader::new(file);
        let mut actual_head = [0_u8; 160];
        input.read_exact(&mut actual_head)?;
        if actual_head != expected_head {
            return Err(RowMapError::Invalid("row-map header identity"));
        }
        let mut digest = Sha256::new();
        digest.update(actual_head);
        let mut new_to_old = Vec::new();
        new_to_old
            .try_reserve_exact(count)
            .map_err(|_| RowMapError::Invalid("forward-map allocation"))?;
        let mut old_to_new = Vec::new();
        old_to_new
            .try_reserve_exact(count)
            .map_err(|_| RowMapError::Invalid("inverse-map allocation"))?;
        old_to_new.resize(count, u32::MAX);
        let mut block = [0_u8; BLOCK_BYTES];
        for start in (0..count).step_by(BLOCK_BYTES / 4) {
            let rows = (count - start).min(BLOCK_BYTES / 4);
            let bytes = &mut block[..rows * 4];
            input.read_exact(bytes)?;
            digest.update(&*bytes);
            for (offset, slot) in bytes.chunks_exact(4).enumerate() {
                let old = u32::from_le_bytes(slot.try_into().unwrap());
                let old_index = old as usize;
                if old_index >= count || old_to_new[old_index] != u32::MAX {
                    return Err(RowMapError::Invalid("row-map is not a permutation"));
                }
                old_to_new[old_index] = (start + offset) as u32;
                new_to_old.push(old);
            }
        }
        let mut extra = [0_u8; 1];
        if input.read(&mut extra)? != 0 {
            return Err(RowMapError::Invalid("row-map trailing bytes"));
        }
        if digest.finalize()[..] != expected_digest {
            return Err(RowMapError::HashMismatch);
        }
        Ok(Self {
            artifact_sha256: expected_digest,
            generation: binding.generation,
            source_sha256: decode_hash(binding.source_sha256)?,
            router_manifest_sha256: decode_hash(binding.router_manifest_sha256)?,
            old_sq8_sha256: decode_hash(binding.old_sq8_sha256)?,
            new_sq8_sha256: decode_hash(binding.new_sq8_sha256)?,
            new_to_old,
            old_to_new,
        })
    }

    /// Number of rows in either physical order.
    pub fn rows(&self) -> usize {
        self.new_to_old.len()
    }
    /// Trusted whole-artifact digest used to authenticate this map.
    pub fn artifact_sha256(&self) -> [u8; 32] {
        self.artifact_sha256
    }
    /// Immutable generation identifier.
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// Digest of the common exact source artifact.
    pub fn source_sha256(&self) -> [u8; 32] {
        self.source_sha256
    }
    /// Digest of the authenticated router manifest.
    pub fn router_manifest_sha256(&self) -> [u8; 32] {
        self.router_manifest_sha256
    }
    /// Digest of the original router's SQ8 object.
    pub fn old_sq8_sha256(&self) -> [u8; 32] {
        self.old_sq8_sha256
    }
    /// Digest of the relaid SQ8 object.
    pub fn new_sq8_sha256(&self) -> [u8; 32] {
        self.new_sq8_sha256
    }
    /// Map a relaid SQ8 row to its original router row.
    pub fn new_to_old(&self, new_row: u32) -> Option<u32> {
        self.new_to_old.get(new_row as usize).copied()
    }
    /// Map an original router row to its relaid SQ8 row.
    pub fn old_to_new(&self, old_row: u32) -> Option<u32> {
        self.old_to_new.get(old_row as usize).copied()
    }
    /// Bytes in the two resident `u32` maps, excluding allocator overhead.
    pub fn resident_payload_bytes(&self) -> usize {
        (self.new_to_old.len() + self.old_to_new.len()) * 4
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    const E: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

    fn binding() -> RowMapBinding<'static> {
        RowMapBinding {
            generation: 7,
            rows: 4,
            source_sha256: A,
            router_manifest_sha256: B,
            old_sq8_sha256: C,
            new_sq8_sha256: D,
        }
    }

    #[test]
    fn nonidentity_round_trip_and_wrong_binding_fail() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("rows.bin");
        let expected = write_row_permutation(&path, binding(), &[2, 0, 3, 1]).unwrap();
        let map = PhysicalRowPermutation::open_authenticated(&path, &expected, binding()).unwrap();
        assert_eq!(map.artifact_sha256(), decode_hash(&expected).unwrap());
        assert_eq!(map.rows(), 4);
        assert_eq!(map.resident_payload_bytes(), 32);
        assert_eq!(map.generation(), 7);
        assert_eq!(
            (0..4)
                .map(|row| map.new_to_old(row).unwrap())
                .collect::<Vec<_>>(),
            vec![2, 0, 3, 1]
        );
        assert_eq!(
            (0..4)
                .map(|row| map.old_to_new(row).unwrap())
                .collect::<Vec<_>>(),
            vec![1, 3, 0, 2]
        );
        assert_eq!(map.new_to_old(4), None);
        let mut wrong = binding();
        wrong.new_sq8_sha256 = E;
        assert!(PhysicalRowPermutation::open_authenticated(&path, &expected, wrong).is_err());
        assert!(write_row_permutation(&path, binding(), &[2, 0, 3, 1]).is_err());
    }

    #[test]
    fn rejects_nonbijection_and_tampering() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("rows.bin");
        assert!(write_row_permutation(&path, binding(), &[0, 1, 1, 3]).is_err());
        assert!(write_row_permutation(&path, binding(), &[0, 1, 2, 4]).is_err());
        let expected = write_row_permutation(&path, binding(), &[3, 2, 1, 0]).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes[160] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            PhysicalRowPermutation::open_authenticated(&path, &expected, binding()),
            Err(RowMapError::Invalid("row-map is not a permutation"))
        ));
    }

    #[test]
    fn rejects_a_bijective_payload_change_and_a_wrong_pin_by_hash() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("rows.bin");
        let expected = write_row_permutation(&path, binding(), &[3, 2, 1, 0]).unwrap();
        assert!(matches!(
            PhysicalRowPermutation::open_authenticated(&path, E, binding()),
            Err(RowMapError::HashMismatch)
        ));
        let mut bytes = fs::read(&path).unwrap();
        bytes[160..164].copy_from_slice(&2u32.to_le_bytes());
        bytes[164..168].copy_from_slice(&3u32.to_le_bytes());
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            PhysicalRowPermutation::open_authenticated(&path, &expected, binding()),
            Err(RowMapError::HashMismatch)
        ));
    }

    #[test]
    fn bare_filename_is_durable_and_canonical_hashes_are_required() {
        let filename = format!(
            "borsuk-rowmap-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let path = Path::new(&filename);
        let hash = write_row_permutation(path, binding(), &[0, 1, 2, 3]).unwrap();
        let opened = PhysicalRowPermutation::open_authenticated(path, &hash, binding()).unwrap();
        fs::remove_file(path).unwrap();
        assert_eq!(opened.new_to_old(3), Some(3));
        assert!(matches!(
            PhysicalRowPermutation::open_authenticated(path, &hash.to_uppercase(), binding()),
            Err(RowMapError::Invalid("SHA-256 identity"))
        ));
    }

    #[test]
    fn checked_writer_rejects_inverse_direction_and_authenticates_sq8_bodies() {
        fn row(id: i64, code: u8) -> Vec<u8> {
            let mut bytes = id.to_le_bytes().to_vec();
            bytes.extend(0.0f32.to_le_bytes());
            bytes.push(code);
            bytes
        }
        let root = tempfile::tempdir().unwrap();
        let old_path = root.path().join("old.sq8");
        let new_path = root.path().join("new.sq8");
        let map_path = root.path().join("rows.bin");
        let old = [row(11, 1), row(22, 2), row(33, 3)].concat();
        let new = [row(33, 3), row(11, 1), row(22, 2)].concat();
        fs::write(&old_path, &old).unwrap();
        fs::write(&new_path, &new).unwrap();
        let old_sha = format!("{:x}", Sha256::digest(&old));
        let new_sha = format!("{:x}", Sha256::digest(&new));
        let binding = RowMapBinding {
            generation: 7,
            rows: 3,
            source_sha256: A,
            router_manifest_sha256: B,
            old_sq8_sha256: &old_sha,
            new_sq8_sha256: &new_sha,
        };
        assert!(matches!(
            write_verified_row_permutation(&map_path, &old_path, &new_path, binding, 1, &[1, 2, 0]),
            Err(RowMapError::Invalid("row-map direction or SQ8 row body"))
        ));
        assert!(!map_path.exists());
        let sha =
            write_verified_row_permutation(&map_path, &old_path, &new_path, binding, 1, &[2, 0, 1])
                .unwrap();
        let map = PhysicalRowPermutation::open_authenticated(&map_path, &sha, binding).unwrap();
        assert_eq!(map.new_to_old(0), Some(2));
        fs::write(&old_path, [row(11, 1), row(22, 2), row(33, 4)].concat()).unwrap();
        assert!(matches!(
            write_verified_row_permutation(
                &root.path().join("tampered.bin"),
                &old_path,
                &new_path,
                binding,
                1,
                &[2, 0, 1]
            ),
            Err(RowMapError::HashMismatch)
        ));
    }
}
