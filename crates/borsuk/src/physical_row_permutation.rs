//! Whole-artifact authenticated mapping from relaid SQ8 rows to old PQ rows.

use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
};

use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;

use crate::native_source_tier::decoded_sha256;

const MAGIC: [u8; 16] = *b"BORSUK-ROWMAP-V1";
const HEADER_BYTES: u64 = 160;
const ENTRY_BYTES: u64 = 4;

/// Identity of both physical orders and their common source generation.
#[derive(Clone, Copy)]
pub struct RowMapBinding<'a> {
    pub generation: u64,
    pub rows: u64,
    pub source_sha256: &'a str,
    pub router_manifest_sha256: &'a str,
    pub old_sq8_sha256: &'a str,
    pub new_sq8_sha256: &'a str,
}

#[derive(Debug, Error)]
pub enum RowMapError {
    #[error("row-map I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("row-map contract differs: {0}")]
    Invalid(&'static str),
    #[error("row-map whole-artifact SHA-256 differs")]
    HashMismatch,
}

/// Both directions are resident; payload is exactly eight bytes per row.
#[derive(Debug)]
pub struct PhysicalRowPermutation {
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
pub fn write_row_permutation(
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
        .ok_or(RowMapError::Invalid("row-map path parent"))?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    let mut digest = Sha256::new();
    {
        let mut output = BufWriter::new(temporary.as_file_mut());
        output.write_all(&head)?;
        digest.update(head);
        for &old in new_to_old {
            let bytes = old.to_le_bytes();
            output.write_all(&bytes)?;
            digest.update(bytes);
        }
        output.flush()?;
    }
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| RowMapError::Io(error.error))?;
    File::open(parent)?.sync_all()?;
    Ok(format!("{:x}", digest.finalize()))
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
        for new in 0..count {
            let mut bytes = [0_u8; 4];
            input.read_exact(&mut bytes)?;
            digest.update(bytes);
            let old = u32::from_le_bytes(bytes);
            let old_index = old as usize;
            if old_index >= count || old_to_new[old_index] != u32::MAX {
                return Err(RowMapError::Invalid("row-map is not a permutation"));
            }
            old_to_new[old_index] = new as u32;
            new_to_old.push(old);
        }
        if digest.finalize()[..] != expected_digest {
            return Err(RowMapError::HashMismatch);
        }
        Ok(Self {
            generation: binding.generation,
            source_sha256: decode_hash(binding.source_sha256)?,
            router_manifest_sha256: decode_hash(binding.router_manifest_sha256)?,
            old_sq8_sha256: decode_hash(binding.old_sq8_sha256)?,
            new_sq8_sha256: decode_hash(binding.new_sq8_sha256)?,
            new_to_old,
            old_to_new,
        })
    }

    pub fn rows(&self) -> usize {
        self.new_to_old.len()
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn source_sha256(&self) -> [u8; 32] {
        self.source_sha256
    }
    pub fn router_manifest_sha256(&self) -> [u8; 32] {
        self.router_manifest_sha256
    }
    pub fn old_sq8_sha256(&self) -> [u8; 32] {
        self.old_sq8_sha256
    }
    pub fn new_sq8_sha256(&self) -> [u8; 32] {
        self.new_sq8_sha256
    }
    pub fn new_to_old(&self, new_row: u32) -> Option<u32> {
        self.new_to_old.get(new_row as usize).copied()
    }
    pub fn old_to_new(&self, old_row: u32) -> Option<u32> {
        self.old_to_new.get(old_row as usize).copied()
    }
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
        assert!(PhysicalRowPermutation::open_authenticated(&path, &expected, binding()).is_err());
    }
}
